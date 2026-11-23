"""F5 (17 §6): потребитель очереди — единственный путь конверсии.

Что проверяется (по пункту «Чего нельзя потерять при удалении inline»):

1. **Повтор воспроизводит корпус, а не удваивает его.** Второе исполнение по тому же
   сеансу берёт retained-байты (краулер не вызывается) и даёт ту же идентичность
   партиции (``product_token``/``pb``/``index_build_id``) и тот же набор строк
   ``corpus_docs``/``extract_queue``. Строку ``done`` потребитель ингестит заново
   намеренно: Pass B собирает партицию из ``ProductIngest`` **этого** исполнения.
2. **Отказ конверсии виден.** Строка получает ``state=failed`` и **класс** сбоя
   (``error_kind``), причина попадает в ``errors`` фазы, документа в корпусе нет —
   вместо ``pending``, который держал бы барьер вечно, и вместо тихого пропуска.
3. **Порядок записи — порядок содержимого, а не расписание.** Доводка окна FIFO:
   при обратном порядке завершения заданий в пуле документы и чанки пишутся
   каноническим порядком ``(product_id, url, chunk_no)``, и два прогона с разным
   расписанием дают одну и ту же последовательность строк.

Нейтральные данные (§1.3): ``p1``, ``docs.test``. Сеть не используется, только
``tmp_path`` для retention.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import threading
import time
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    AnchorRelation,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    DocumentLevel,
    ExtractQueueState,
    PdfPipeline,
    RunConfig,
    RunMode,
    WorkClass,
)
from core.pipeline import RunPlan, extract_consume
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id
from core.pipeline.extraction_runtime import ExtractionPool
from core.pipeline.resources import (
    BUDGET_UNACCOUNTED,
    DOCLING_PEAK_MB,
    MODEL_FREE_PEAK_MB,
    PARENT_FLOOR_MB,
    ResourcesObservation,
    workers_for_class,
)
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-extract-consumer"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
ROOT = "https://docs.test/"
LINKS = ("a", "b", "c", "d")
URLS = tuple(f"{ROOT}{name}" for name in LINKS)


# --------------------------------------------------------------------------- #
# Нейтральные входы и заглушки портов
# --------------------------------------------------------------------------- #


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=[],
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


def _make_run(store: SqliteStore) -> None:
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


def _pages() -> dict[str, bytes]:
    links = "".join(f'<a href="{url}">{name}</a>' for url, name in zip(URLS, LINKS, strict=True))
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    for url, name in zip(URLS, LINKS, strict=True):
        pages[url] = f"<html>body-{name}</html>".encode()
    return pages


def _page_bytes(page: RawPage) -> bytes:
    """Байты страницы: тело либо retained-файл (потребитель не держит тело в RAM)."""
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return Path(path).read_bytes() if path else b""


class _Crawler:
    version = "crawler-1"
    engine = "http"
    supports_browser = False

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            content_type="text/html",
            lang="en",
            status_code=200,
            engine=self.engine,
        )


class _BombCrawler(_Crawler):
    """Краулер, обязанный НЕ вызываться: retained-байты уже есть (resume)."""

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        raise AssertionError("краулер вызван при наличии retained-байтов (resume)")


class _Extractor:
    """Экстрактор-заглушка; ``fail_names`` — документы, на которых он отказывает."""

    version = "extractor-1"
    supports_source_path = True

    def __init__(self, *, fail_names: tuple[str, ...] = ()) -> None:
        self.fail_names = tuple(fail_names)
        self.calls: list[str] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls.append(page.url)
        data = _page_bytes(page)
        text = data.decode("utf-8", errors="replace")
        if any(f"body-{name}" in text for name in self.fail_names):
            raise RuntimeError("extractor refused this document")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(data),
            extractor_version=self.version,
        )


class _Chunker:
    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="h1",
                context_prefix="ctx",
                content_hash=doc.content_hash,
            )
        ]


# --------------------------------------------------------------------------- #
# Пул с настоящей асинхронностью: порядок завершения ≠ порядок submit
# --------------------------------------------------------------------------- #


class _Future:
    def __init__(self) -> None:
        self._done = threading.Event()
        self._value: Any = None
        self._error: BaseException | None = None

    def result(self, timeout: float | None = None) -> Any:
        if not self._done.wait(timeout):
            raise TimeoutError()
        if self._error is not None:
            raise self._error
        return self._value


class _ThreadPool:
    def __init__(self, size: int, *, delays: tuple[float, ...] = ()) -> None:
        self.size = size
        self._delays = list(delays)
        self.submitted: list[str] = []
        self.completed: list[str] = []
        self.max_inflight = 0
        self.max_tasks_per_child: int | None = None
        self._open = 0

    def submit(self, fn: Any, request: Any) -> _Future:
        self.submitted.append(request.url)
        delay = self._delays.pop(0) if self._delays else 0.0
        self._open += 1
        self.max_inflight = max(self.max_inflight, self._open)
        future = _Future()

        def _run() -> None:
            try:
                if delay:
                    time.sleep(delay)
                future._value = fn(request)
            except BaseException as exc:  # noqa: BLE001 — эмуляция отказа воркера
                future._error = exc
            finally:
                self._open -= 1
                self.completed.append(request.url)
                future._done.set()

        threading.Thread(target=_run, daemon=True).start()
        return future

    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        pass


def _file_worker(request: Any) -> ExtractedDoc:
    """Воркер, эквивалентный ``_Extractor``: читает retained-файл (I19)."""
    data = Path(request.raw_path).read_bytes()
    return ExtractedDoc(
        text=data.decode("utf-8", errors="replace"),
        content_hash=content_hash(data),
        extractor_version="extractor-1",
    )


def _pool_factory(created: list[_ThreadPool], **kw: Any) -> Any:
    def factory(
        n: int,
        level: Any = None,
        *,
        pdf_pipeline: Any = None,
        max_tasks_per_child: int | None = None,
    ) -> Any:
        pool = _ThreadPool(n, **kw)
        pool.max_tasks_per_child = max_tasks_per_child
        created.append(pool)
        return pool

    return factory


def _pool(created: list[_ThreadPool], **kw: Any) -> ExtractionPool:
    return ExtractionPool(pool_factory=_pool_factory(created, **kw), worker=_file_worker)


def _failing_pool(created: list[_ThreadPool], names: tuple[str, ...] = ("h0",)) -> ExtractionPool:
    """Пул, чей воркер отказывает на перечисленных документах.

    Отказ нужен **в воркере**, а не в ``extractor``: при объявленном пуле конверсию ведёт
    он (:class:`ExtractionPool`), и подменённый экстрактор до строки не доходит. Так тест
    проверяет возврат резерва на пути ``failed``, а не на пути, где отказа не было.
    """

    def _worker(request: Any) -> ExtractedDoc:
        data = Path(request.raw_path).read_bytes()
        if any(f"body-{name}".encode() in data for name in names):
            raise RuntimeError("extractor refused this document")
        return ExtractedDoc(
            text=data.decode("utf-8", errors="replace"),
            content_hash=content_hash(data),
            extractor_version="extractor-1",
        )

    return ExtractionPool(pool_factory=_pool_factory(created), worker=_worker)


def _build(
    tmp_path: Path,
    store: SqliteStore,
    *,
    crawler: _Crawler | None = None,
    extractor: Any = None,
    pages: dict[str, bytes] | None = None,
    **kwargs: Any,
):
    _make_run(store)
    crawl_defaults = kwargs.pop("crawl_defaults", None) or CrawlRunConfig(max_depth=1)
    return build_corpus(
        _plan(),
        crawler if crawler is not None else _Crawler(pages or _pages()),
        extractor if extractor is not None else _Extractor(),
        _Chunker(),
        store,
        urls={"p1": [ROOT]},
        raw_root=tmp_path,
        run_id=RUN_ID,
        crawl_defaults=crawl_defaults,
        **kwargs,
    )


def _build_of(result: Any) -> IndexBuild:
    builds = [entry for entry in result.log_entries if isinstance(entry, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _queue(store: SqliteStore) -> list[Any]:
    return list(store.iter_extract_queue(SESSION_ID))


def _doc_url_sequence(store: SqliteStore) -> list[str]:
    return [
        str(row["url"])
        for row in store.conn.execute("SELECT url FROM corpus_docs ORDER BY rowid")
    ]


def _chunk_sequence(store: SqliteStore) -> list[tuple[str, int]]:
    return [
        (str(row["url"]), int(row["chunk_no"]))
        for row in store.conn.execute(
            "SELECT url, chunk_no FROM corpus_chunks ORDER BY rowid"
        )
    ]


# --------------------------------------------------------------------------- #
# (1) Повтор исполнения воспроизводит корпус, а не удваивает его
# --------------------------------------------------------------------------- #


def test_second_execution_reproduces_the_same_corpus(tmp_path: Path) -> None:
    """Повторное исполнение по тому же сеансу воспроизводит корпус, не удваивая его.

    Потребитель **намеренно** заново ингестит строку, закрытую прошлым исполнением:
    Pass B собирает партицию из ``ProductIngest`` этого исполнения, и «пропустить
    ``done``» дало бы пустую партицию. Идемпотентность здесь — свойство **исхода**:
    те же документы (тот же ``content_hash``, тот же ``doc_id``), та же идентичность
    партиции и те же строки очереди, а не отсутствие работы.

    Retained-байты берутся из ``corpus_raw`` (краулер ниже обязан не вызываться):
    повтор не ходит в сеть и не переписывает наблюдённый факт.
    """
    store = SqliteStore(":memory:")
    try:
        first = _build(tmp_path, store)
        first_build = _build_of(first)
        assert first.errors == []
        rows_first = _queue(store)
        docs_first = sorted(
            (doc.url, doc.content_hash) for doc in store.iter_corpus_docs(RUN_ID)
        )
        assert {row.state for row in rows_first} == {ExtractQueueState.DONE}
        assert len(docs_first) == len(rows_first)

        created: list[_ThreadPool] = []
        second = _build(
            tmp_path,
            store,
            crawler=_BombCrawler({}),
            extractor=_Extractor(),
            extraction_pool=_pool(created),
            crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=2),
        )
        second_build = _build_of(second)

        assert second.errors == []
        # Повтор шёл из retained-байтов (краулер не вызван), через тот же пул.
        assert sum(len(pool.submitted) for pool in created) > 0
        # Идентичность партиции — функция содержимого, а не числа конверсий.
        assert second_build.index_build_id == first_build.index_build_id
        assert [
            (entry.product_id, entry.partition_build_id) for entry in second_build.entries
        ] == [
            (entry.product_id, entry.partition_build_id) for entry in first_build.entries
        ]
        # Корпус не удвоен и не переписан: тот же набор документов.
        docs_second = sorted(
            (doc.url, doc.content_hash) for doc in store.iter_corpus_docs(RUN_ID)
        )
        assert docs_second == docs_first
        # Очередь не переписана и не задвоена: те же строки, те же состояния.
        rows_second = _queue(store)
        assert [(row.product_id, row.url) for row in rows_second] == [
            (row.product_id, row.url) for row in rows_first
        ]
        assert {row.state for row in rows_second} == {ExtractQueueState.DONE}
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (2) Отказ конверсии виден: строка failed + error_kind, документа нет
# --------------------------------------------------------------------------- #


def test_failed_conversion_is_visible_with_kind_on_the_queue_row(tmp_path: Path) -> None:
    """Отказ экстрактора закрывает строку ``failed`` с классом сбоя, а не молчит.

    Раньше (до F5) отказ был строкой ``errors`` при живой inline-конверсии. Теперь
    отказ обязан остаться **на строке очереди**: ``pending`` держал бы барьер вечно,
    а ``done`` объявил бы корпус полным без документа. Проверяются оба факта
    одновременно: состояние строки и отсутствие документа в корпусе.
    """
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path,
            store,
            extractor=_Extractor(fail_names=("b",)),
        )
        rows = {(row.product_id, row.url): row for row in _queue(store)}

        broken = rows[("p1", URLS[1])]
        assert broken.state is ExtractQueueState.FAILED
        assert broken.error_kind, broken
        assert broken.error, broken
        # Отказ назван в канале видимости фазы, с продуктом и адресом.
        assert any(URLS[1] in line for line in result.errors), result.errors

        docs = {(doc.url, doc.content_hash) for doc in store.iter_corpus_docs(RUN_ID)}
        assert URLS[1] not in {url for url, _hash in docs}
        # Остальные строки доведены: один отказ не отменяет соседей.
        others = [row for key, row in rows.items() if key[1] != URLS[1]]
        assert {row.state for row in others} == {ExtractQueueState.DONE}
        for row in others:
            assert (row.url, row.content_hash) in docs
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (3) Порядок записи — канонический, а не порядок завершения заданий
# --------------------------------------------------------------------------- #


def test_document_write_order_follows_the_queue_not_completion(tmp_path: Path) -> None:
    """Обратный порядок завершения заданий не меняет порядок строк корпуса.

    Доводка окна FIFO: ``future.result`` вызывается в порядке строк очереди, поэтому
    завершение «с конца» не сдвигает запись. Проверка нужна потому, что
    ``_content_digest`` склеивает чанки **в порядке вставки**: запись по расписанию
    дала бы при том же ``partition_build_id`` другой физический дайджест.
    """
    sequences: list[tuple[list[str], list[tuple[str, int]]]] = []
    completions: list[list[str]] = []
    submissions: list[list[str]] = []
    for index, delays in enumerate(
        ((0.0, 0.12, 0.08, 0.04, 0.0), (0.0, 0.0, 0.04, 0.08, 0.12))
    ):
        store = SqliteStore(":memory:")
        created: list[_ThreadPool] = []
        try:
            result = _build(
                tmp_path / f"schedule-{index}",
                store,
                extraction_pool=_pool(created, delays=delays),
                crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=3),
            )
            assert result.errors == []
            queue_urls = [row.url for row in _queue(store)]
            assert all(row.state is ExtractQueueState.DONE for row in _queue(store))
            # Порядок записи документов = порядок очереди ``(product_id, url)``.
            assert _doc_url_sequence(store) == queue_urls
            # Чанки — тот же порядок плюс номер чанка внутри документа.
            assert _chunk_sequence(store) == sorted(
                _chunk_sequence(store), key=lambda pair: (pair[0], pair[1])
            )
            sequences.append((_doc_url_sequence(store), _chunk_sequence(store)))
            completions.append(
                [request for pool in created for request in pool.completed]
            )
            submissions.append(
                [request for pool in created for request in pool.submitted]
            )
        finally:
            store.close()

    # Первое расписание действительно завершило задания не в порядке submit —
    # иначе тест не отличал бы FIFO от случайного совпадения.
    assert completions[0] != submissions[0]
    # Два разных расписания дают одну и ту же последовательность строк.
    assert sequences[0] == sequences[1]

# --------------------------------------------------------------------------- #
# (4) Ширина полосы не входит в идентичность корпуса (F0)
# --------------------------------------------------------------------------- #


def _corpus_identity(store: SqliteStore) -> tuple[list[Any], list[Any]]:
    """Идентичность корпуса: ``doc_id``/``chunk_id``/``content_hash`` и текст чанков.

    Читается из БД, а не из накопителя фазы: предмет пина — то, что записано, а не то,
    что фаза держала в памяти.
    """
    docs = [
        (row["doc_id"], row["url"], row["content_hash"], row["extractor_version"])
        for row in store.conn.execute(
            "SELECT doc_id, url, content_hash, extractor_version "
            "FROM corpus_docs ORDER BY url"
        )
    ]
    chunks = [
        (
            row["chunk_id"],
            row["doc_id"],
            row["url"],
            int(row["chunk_no"]),
            row["content_hash"],
            row["text"],
            row["context_prefix"],
        )
        for row in store.conn.execute(
            "SELECT chunk_id, doc_id, url, chunk_no, content_hash, text, "
            "context_prefix FROM corpus_chunks ORDER BY url, chunk_no"
        )
    ]
    return docs, chunks


def test_window_one_and_window_n_give_the_same_corpus_identity(tmp_path: Path) -> None:
    """Пин F0: окно полосы — расписание, а не содержимое.

    Сравниваются два расписания **одной** работы:

    * окно 1 — пул с рециклингом воркеров (``extract_max_tasks_per_child``), то есть
      прежнее поведение «одно задание в полёте, воркер пересоздаётся»;
    * окно N — объявленная ось ``crawl.extract_workers=3``.

    Требование: ``doc_id``/``chunk_id``/``content_hash``, текст чанков, порядок записи
    документов и идентичность партиции совпадают до последнего символа. Если бы
    ``content_hash`` или порядок вставки зависели от ширины окна, ``partition_build_id``
    описывал бы расписание, а не корпус, и один ``run_id`` давал бы разные партиции.
    """
    runs: list[tuple[Any, dict[str, int], list[int]]] = []
    for index, (defaults, recycle) in enumerate(
        (
            (CrawlRunConfig(max_depth=1, extract_max_tasks_per_child=2), True),
            (CrawlRunConfig(max_depth=1, extract_workers=3), False),
        )
    ):
        store = SqliteStore(":memory:")
        created: list[_ThreadPool] = []
        try:
            pool = ExtractionPool(
                pool_factory=_pool_factory(created),
                worker=_file_worker,
                supports_worker_recycling=recycle,
            )
            result = _build(
                tmp_path / f"window-{index}",
                store,
                extraction_pool=pool,
                crawl_defaults=defaults,
            )
            assert result.errors == []
            build = _build_of(result)
            observation = next(
                e for e in result.log_entries if isinstance(e, ResourcesObservation)
            )
            runs.append(
                (
                    (
                        _corpus_identity(store),
                        _doc_url_sequence(store),
                        build.index_build_id,
                        [
                            (entry.product_id, entry.partition_build_id)
                            for entry in build.entries
                        ],
                    ),
                    dict(observation.extract_workers_by_class),
                    [p.max_inflight for p in created],
                )
            )
        finally:
            store.close()

    # Расписания действительно разные — иначе тест не отличал бы окно 1 от окна N.
    # Сверяется **опубликованное** окно полосы, а не наблюдённая одновременность: потоки
    # успевают завершиться между подачами, и ``max_inflight`` — факт планировщика ОС,
    # а не решение потребителя (на этом он и падал под нагрузкой).
    assert runs[0][1] == {"model_free": 1}, runs[0][1]
    assert runs[1][1] == {"model_free": 3}, runs[1][1]
    # Наблюдённая одновременность не превышает объявленного окна — верхняя граница.
    assert max(runs[0][2]) <= 1, runs[0][2]
    assert max(runs[1][2]) <= 3, runs[1][2]
    assert runs[0][0] == runs[1][0]


class _MixedCrawler(_Crawler):
    """Краулер с двумя классами работы: ``/doc.pdf`` отдаётся как PDF, остальное — HTML."""

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        page = super().fetch(url, plan)
        if url.endswith(".pdf"):
            return dataclasses.replace(page, content_type="application/pdf")
        return page


def test_each_work_class_gets_its_own_window_from_the_budget(tmp_path: Path) -> None:
    """Полосы классов: HTML и PDF получают **разные** окна из одного бюджета.

    Бюджет взят так, что три HTML-страницы помещаются (317 MB каждая), а docling
    ``full`` (4833 MB) — ни одной. Одно общее окно в этой точке либо морило HTML в
    очереди за PDF (окно 0/1), либо обещало память, которой нет (окно 3 для PDF).
    Проверяется фактическая ширина полос — по пулам, а не по отчёту: пул адресуется
    ключом ``(level, pdf_pipeline, window)``, поэтому разные окна дают разные пулы.
    """
    budget = 500 + 3 * MODEL_FREE_PEAK_MB
    pages: dict[str, bytes] = {
        ROOT: b'<html><a href="/a">a</a><a href="/doc.pdf">pdf</a></html>',
        f"{ROOT}a": b"<html>body-a</html>",
        f"{ROOT}doc.pdf": b"%PDF-1.4 body-pdf",
    }
    store = SqliteStore(":memory:")
    created: list[_ThreadPool] = []
    try:
        result = _build(
            tmp_path,
            store,
            crawler=_MixedCrawler(pages),
            extraction_pool=_pool(created),
            crawl_defaults=CrawlRunConfig(max_depth=1),
            derive_extract_workers=True,
            mem_budget_mb=budget,
        )
        # Непоместившийся класс назван, а не помолчал: окно 1 без причины выглядело бы
        # замером. Это единственное сообщение прогона — провалов нет.
        assert [line for line in result.errors if "не помещается в бюджет" not in line] == []
        assert any("pdf_structured" in line for line in result.errors), result.errors
        assert all(row.state is ExtractQueueState.DONE for row in _queue(store))

        # Две полосы с разной шириной: HTML — 3 (по 317 MB), PDF — 1 (пик не помещается,
        # поэтому окно 1, а не выдуманное число).
        by_size = {pool.size: pool for pool in created}
        assert sorted(by_size) == [1, 3], [pool.size for pool in created]
        pdf_urls = [url for url in by_size[1].submitted if url.endswith(".pdf")]
        assert pdf_urls == [f"{ROOT}doc.pdf"], by_size[1].submitted
        # HTML-документы не попали в полосу PDF и наоборот.
        assert not [url for url in by_size[3].submitted if url.endswith(".pdf")]
        # Оба класса доведены до корпуса: разное окно не потеряло документ.
        docs = {row["url"] for row in store.conn.execute("SELECT url FROM corpus_docs")}
        assert docs == set(pages)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) Учёт памяти «в полёте»: сумма пиков, а не независимые окна (F0)
# --------------------------------------------------------------------------- #


class _TypedCrawler(_Crawler):
    """Краулер с объявленными типами: класс работы строки берётся из ``Content-Type``."""

    def __init__(self, pages: dict[str, bytes], types: dict[str, str]) -> None:
        super().__init__(pages)
        self.types = dict(types)

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        page = super().fetch(url, plan)
        return dataclasses.replace(page, content_type=self.types.get(url, "text/html"))


def _observation(result: Any) -> ResourcesObservation:
    return next(
        entry for entry in result.log_entries if isinstance(entry, ResourcesObservation)
    )


def _mixed_pages(
    html_count: int, pdf_count: int
) -> tuple[dict[str, bytes], dict[str, str]]:
    """Корень со ссылками на ``html_count`` HTML-страниц и ``pdf_count`` PDF."""
    html_urls = [f"{ROOT}h{index}" for index in range(html_count)]
    pdf_urls = [f"{ROOT}d{index}.pdf" for index in range(pdf_count)]
    links = "".join(
        f'<a href="{url}">{index}</a>' for index, url in enumerate(html_urls + pdf_urls)
    )
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    for index, url in enumerate(html_urls):
        # Тело несёт короткое имя: ``_Extractor(fail_names=…)`` ищет ``body-h0``.
        pages[url] = f"<html>body-h{index}</html>".encode()
    for index, url in enumerate(pdf_urls):
        pages[url] = f"%PDF-1.4 body-d{index}".encode()
    return pages, {url: "application/pdf" for url in pdf_urls}


def test_sum_of_windows_may_exceed_budget_but_starts_are_throttled(
    tmp_path: Path,
) -> None:
    """Сумма окон **может** превысить бюджет, но старты держатся учётом памяти.

    Бюджет 8000 MB (доступно 7500): окно HTML — ``7500 // 317`` (сверху CPU), окно
    ``layout_tables`` — ``7500 // 3679 = 2``. Каждое окно по отдельности помещается, а их
    сумма — нет (это и есть предусловие теста, считается из тех же функций, а не вписано).
    Учёт обязан удержать **сумму**: наблюдаемый пик не больше доступного бюджета, а часть
    стартов отложена — иначе 2 × 3679 + 12 × 317 = 11162 MB против 7500 на 6.9 GB хосте.
    """
    budget = 8000
    room = budget - PARENT_FLOOR_MB
    level = DocumentLevel.LAYOUT_TABLES
    pipeline = PdfPipeline.STRUCTURED
    html_window, _ = workers_for_class(WorkClass.MODEL_FREE, level, pipeline, budget)
    pdf_window, _ = workers_for_class(WorkClass.PDF_STRUCTURED, level, pipeline, budget)
    pdf_peak = DOCLING_PEAK_MB[level]
    # Предусловие: по отдельности помещается, вместе — нет. Если оно перестанет
    # выполняться, тест обязан упасть, а не проверять вакуум.
    assert html_window * MODEL_FREE_PEAK_MB <= room
    assert pdf_window * pdf_peak <= room
    assert html_window * MODEL_FREE_PEAK_MB + pdf_window * pdf_peak > room

    pages, types = _mixed_pages(html_window + 3, pdf_window)
    store = SqliteStore(":memory:")
    created: list[_ThreadPool] = []
    try:
        result = _build(
            tmp_path,
            store,
            crawler=_TypedCrawler(pages, types),
            extraction_pool=_pool(created),
            crawl_defaults=CrawlRunConfig(max_depth=1, document_level=level),
            derive_extract_workers=True,
            mem_budget_mb=budget,
        )
        assert result.errors == [], result.errors
        assert all(row.state is ExtractQueueState.DONE for row in _queue(store))

        observation = _observation(result)
        assert observation.budget_accounting == "accounted"
        assert observation.extract_workers_by_class == {
            "model_free": html_window,
            "pdf_structured": pdf_window,
        }
        # Сумма пиков «в полёте» не превысила доступного бюджета ни в один момент.
        assert observation.in_flight_mb_peak is not None
        assert observation.in_flight_mb_peak <= room, observation.in_flight_mb_peak
        # И это достигнуто откладыванием стартов, а не тем, что окна были малы.
        assert observation.class_skipped_for_budget, observation.class_skipped_for_budget
        assert sum(observation.class_skipped_for_budget.values()) > 0
    finally:
        store.close()


def test_in_flight_released_on_settle(tmp_path: Path) -> None:
    """Резерв возвращается на **каждом** пути закрытия строки: успех, отказ, мёртвый пул.

    Проверка устроена так, что утечка сразу видна: бюджет 4000 MB (доступно 3500) даёт окно
    ``3500 // 317 = 11`` для HTML и 12 строк в очереди. Двенадцатая строка стартует только
    если одиннадцать предыдущих **вернули** свои резервы; забытый возврат оставляет счётчик
    на 3487 MB, двенадцатая не помещается (3487 + 317 > 3500), стартовать нечему — и проход
    встаёт с ``pending = 1`` вместо ``pending = 0``.

    Второй проход того же теста — мёртвый пул (``BrokenProcessPool``): остаток полосы
    закрывается терминально (:func:`_abort_pending`), и его резервы обязаны вернуться в
    общий счётчик, иначе двенадцатая строка не стартует уже из-за них.
    """
    budget = 4000
    room = budget - PARENT_FLOOR_MB
    level = DocumentLevel.FULL
    window, _ = workers_for_class(WorkClass.MODEL_FREE, level, PdfPipeline.STRUCTURED, budget)
    assert window * MODEL_FREE_PEAK_MB <= room < (window + 1) * MODEL_FREE_PEAK_MB
    pages, _types = _mixed_pages(window + 1, 0)

    # (а) успех и отказ конверсии: один документ валит экстрактор.
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path / "failed",
            store,
            crawler=_Crawler(pages),
            extraction_pool=_failing_pool([]),
            crawl_defaults=CrawlRunConfig(max_depth=1),
            derive_extract_workers=True,
            mem_budget_mb=budget,
        )
        rows = _queue(store)
        assert all(
            row.state in (ExtractQueueState.DONE, ExtractQueueState.FAILED) for row in rows
        )
        assert sum(row.state is ExtractQueueState.FAILED for row in rows) == 1, rows
        observation = _observation(result)
        assert observation.in_flight_mb_peak is not None
        assert observation.in_flight_mb_peak <= room
        # Все строки закрыты: ни одна не осталась ждать память, которую никто не вернул.
        assert all(row.state is not ExtractQueueState.PENDING for row in rows)
    finally:
        store.close()

    # (б) мёртвый пул: первое задание валит воркера, остаток полосы — терминально.
    store = SqliteStore(":memory:")
    try:
        created: list[_ThreadPool] = []

        def _dying_worker(request: Any) -> Any:
            raise BrokenProcessPool("воркер умер (эмуляция OOM)")

        pool = ExtractionPool(
            pool_factory=_pool_factory(created), worker=_dying_worker
        )
        result = _build(
            tmp_path / "dead-pool",
            store,
            crawler=_Crawler(pages),
            extraction_pool=pool,
            crawl_defaults=CrawlRunConfig(max_depth=1),
            derive_extract_workers=True,
            mem_budget_mb=budget,
        )
        rows = _queue(store)
        assert not [row for row in rows if row.state is ExtractQueueState.PENDING], rows
        assert sum(row.state is ExtractQueueState.FAILED for row in rows) >= 1
        observation = _observation(result)
        assert observation.in_flight_mb_peak is not None
        assert observation.in_flight_mb_peak <= room
        assert observation.budget_accounting == "accounted"
    finally:
        store.close()


def test_budget_none_keeps_previous_behaviour_and_is_visible(tmp_path: Path) -> None:
    """Без бюджета окна классов работают как прежде, но «не считали» видно токеном.

    Учёт требует объявленного бюджета: без него сумма пиков не с чем сравнивать. Молчать
    об этом нельзя — отсутствие учёта не имеет права читаться как «всё поместилось»,
    поэтому публикуется ``class_budget_unaccounted``, а пик остаётся ``None`` (не ``0``:
    ноль был бы утверждением «ничего не занимали», которого сделать нельзя).
    """
    store = SqliteStore(":memory:")
    created: list[_ThreadPool] = []
    try:
        result = _build(
            tmp_path,
            store,
            extraction_pool=_pool(created),
            crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=3),
        )
        assert result.errors == []
        assert all(row.state is ExtractQueueState.DONE for row in _queue(store))
        observation = _observation(result)
        # Поведение прежнее: окно — объявленная ось, по классам оно то же самое.
        assert observation.extract_workers_by_class == {"model_free": 3}
        # Учёт не вёлся — и это названо.
        assert observation.budget_accounting == BUDGET_UNACCOUNTED
        assert observation.in_flight_mb_peak is None
        assert observation.class_skipped_for_budget == {}
    finally:
        store.close()


def test_peak_unmeasured_class_runs_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Класс с неизмеренным пиком идёт один на узел — и не делит узел с docling.

    Пик ``office`` не измерен (``work_class_peak_unmeasured``), подставлять пик PDF-уровня
    запрещено (A3): число было бы чужим замером. Поэтому такой класс держит не мегабайты,
    а право быть единственным. Проверяется в точке решения (:meth:`_BandRun._admit`):
    каждый допущенный старт ``office`` видит пустой узел, а ``model_free`` не стартует,
    пока ``office`` в полёте, — иначе неизмеренный класс складывался бы с измеренным
    «бесплатно», то есть бюджет обходился бы через класс без числа.
    """
    budget = 4000
    level = DocumentLevel.FULL
    window, _ = workers_for_class(WorkClass.MODEL_FREE, level, PdfPipeline.STRUCTURED, budget)
    assert window > 1  # иначе «рядом» неотличимо от «по одному»

    pages, _types = _mixed_pages(window + 2, 0)
    office_url = f"{ROOT}manual.docx"
    pages[ROOT] = pages[ROOT].replace(
        b"</html>", f'<a href="{office_url}">doc</a></html>'.encode()
    )
    pages[office_url] = b"PK\x03\x04 not-a-real-docx"
    types = {
        office_url: (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )
    }

    admitted: list[tuple[str, int, int]] = []
    original = extract_consume._BandRun._admit

    def _spy(
        self: Any, band: Any, lvl: Any, pipeline: Any
    ) -> tuple[bool, int | None, bool]:
        verdict = original(self, band, lvl, pipeline)
        if verdict[0]:
            admitted.append((band.work_class.value, self.in_flight_mb, self.unmeasured_in_flight))
        return verdict

    monkeypatch.setattr(extract_consume._BandRun, "_admit", _spy)

    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path,
            store,
            crawler=_TypedCrawler(pages, types),
            extraction_pool=_pool([]),
            crawl_defaults=CrawlRunConfig(max_depth=1),
            derive_extract_workers=True,
            mem_budget_mb=budget,
        )
        # Пик office не измерен — это **названный** факт (окно 1), а не провал прогона.
        assert [
            line for line in result.errors if "work_class_peak_unmeasured" not in line
        ] == [], result.errors
        assert any("work_class_peak_unmeasured" in line for line in result.errors)
        assert all(row.state is ExtractQueueState.DONE for row in _queue(store))

        office_starts = [entry for entry in admitted if entry[0] == WorkClass.OFFICE.value]
        assert office_starts, "office-строка не стартовала вовсе"
        for _cls, in_flight_mb, unmeasured in office_starts:
            assert (in_flight_mb, unmeasured) == (0, 0), (in_flight_mb, unmeasured)
        # И обратное направление: пока неизмеренный класс в полёте, HTML не стартует.
        for _cls, _in_flight_mb, unmeasured in [
            entry for entry in admitted if entry[0] == WorkClass.MODEL_FREE.value
        ]:
            assert unmeasured == 0
    finally:
        store.close()
