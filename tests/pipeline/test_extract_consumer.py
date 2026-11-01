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

import datetime as _dt
import threading
import time
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    ExtractQueueState,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id
from core.pipeline.extraction_runtime import ExtractionPool
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