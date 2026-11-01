"""Тесты F2: ``crawl_only`` — краул без конверсии (17 §4).

Проверяется граница подсистемы: в режиме ``crawl_only`` краул **добывает** (frontier,
фетч, retention, элемент сеанса ``FETCHED``, расширение ссылок) и **не собирает**
(``corpus_docs``/``corpus_chunks`` пусты, экстрактор и чанкер не вызываются), а каждый
добытый документ становится строкой durable-очереди ``extract_queue``. Контрольный
прогон с ``crawl_only=False`` собирает корпус — то есть режим меняет именно конверсию,
а не краул.

Строка очереди проверяется как **контракт передачи условий измерения**: провенанс
получения (``fetched_at``/``fetch_mode``/``extract_route``/``content_type``/``engine``)
берётся из наблюдённого ответа, а повторная запись того же URL не перезаписывает строку.

Фейки локальные и нейтральные (§1.3): ``p1``, ``docs.test``. Сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
import json
import pathlib
from pathlib import Path
from typing import Any

import pytest

from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlRunConfig,
    CrawlSessionStatus,
    ExtractQueueState,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-crawl-only"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"
ROOT = "https://docs.test/"
HOSTS = ("h0.docs.test", "h1.docs.test")


# --------------------------------------------------------------------------- #
# Обвязка (нейтральные входы + фейковые порты)
# --------------------------------------------------------------------------- #


def _plan() -> RunPlan:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    catalog = [CatalogRow.model_validate(row) for row in raw["catalog"]]
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=catalog,
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


def _two_host_pages() -> dict[str, bytes]:
    """Корень со ссылками на два объявленных host'а (волна ссылок = второй уровень)."""
    links = "".join(f'<a href="https://{host}/doc">{host}</a>' for host in HOSTS)
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    for host in HOSTS:
        pages[f"https://{host}/doc"] = f"<html>body-{host}</html>".encode()
    return pages


def _profiles() -> dict[str, DomainProfile]:
    """Объявление поддоменов: scope корня — только **объявленные** хосты (C/§13)."""
    return {host: DomainProfile() for host in HOSTS}


class _Crawler:
    """Краулер ``url -> bytes``; запоминает вызовы (доказательство обхода ссылок)."""

    version = "crawler-1"
    engine = "http"
    supports_browser = False

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append(url)
        # Наблюдённый ответ: провенанс получения (тип/статус/движок) — то, что несёт
        # ``_ExtractRequest`` документа и что обязана повторить строка очереди.
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
            status_code=200,
            content_type="text/html",
            engine=self.engine,
        )


class _Extractor:
    """Экстрактор, который **не должен** вызываться в режиме ``crawl_only``."""

    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        return ExtractedDoc(
            text=_page_bytes(page).decode("utf-8", errors="replace"),
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    version = "chunker-1"

    def __init__(self) -> None:
        self.calls = 0

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        self.calls += 1
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


def _build(
    tmp_path: Path,
    store: SqliteStore,
    crawler: _Crawler,
    extractor: _Extractor,
    chunker: _Chunker,
    *,
    crawl_only: bool,
    **kwargs: Any,
):
    _make_run(store)
    return build_corpus(
        _plan(),
        crawler,
        extractor,
        chunker,
        store,
        urls={"p1": [ROOT]},
        raw_root=tmp_path,
        run_id=RUN_ID,
        domain_profiles=_profiles(),
        crawl_defaults=CrawlRunConfig(max_depth=1),
        crawl_only=crawl_only,
        **kwargs,
    )


def _build_of(result: Any) -> IndexBuild:
    builds = [entry for entry in result.log_entries if isinstance(entry, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _chunk_rows(store: SqliteStore) -> int:
    row = store.conn.execute("SELECT COUNT(*) AS n FROM corpus_chunks").fetchone()
    return int(row["n"])


# --------------------------------------------------------------------------- #
# (1) crawl_only: краул добывает, корпус не собирается
# --------------------------------------------------------------------------- #


def test_crawl_only_fetches_but_does_not_build_corpus(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    crawler = _Crawler(_two_host_pages())
    extractor = _Extractor()
    chunker = _Chunker()
    try:
        result = _build(
            tmp_path,
            store,
            crawler,
            extractor,
            chunker,
            crawl_only=True,
        )
        build = _build_of(result)

        # Краул полон: сеанс терминален, обход расширился по ссылкам (2 host'а + корень).
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.COMPLETED
        assert len(crawler.calls) == 1 + len(HOSTS)
        assert len(crawler.calls) > 1  # ссылки в frontier расширялись
        assert session.fetched == len(crawler.calls)
        assert result.errors == []

        # Конверсии нет: ни документов, ни чанков, ни вызовов портов извлечения.
        assert list(store.iter_corpus_docs(RUN_ID)) == []
        assert _chunk_rows(store) == 0
        assert extractor.calls == 0
        assert chunker.calls == 0
        assert build.doc_count == 0
        assert build.chunk_count == 0

        # Durable-факт: очередь = число ``FETCHED``-элементов, файлы существуют.
        queue = list(store.iter_extract_queue(SESSION_ID))
        assert len(queue) == session.fetched
        assert {item.state for item in queue} == {ExtractQueueState.PENDING}
        assert store.count_extract_queue(SESSION_ID) == {"pending": session.fetched}
        assert {item.url for item in queue} == set(crawler.calls)
        for item in queue:
            retained = Path(item.raw_path)
            assert retained.is_file(), f"нет retained-файла для {item.url}"
            assert item.content_hash == content_hash(retained.read_bytes())
            # Условия измерения корня — часть контракта очереди.
            assert item.level == "full"
            assert item.pdf_pipeline == "structured"
        # Элементы сеанса и очередь говорят об одном и том же наборе URL.
        fetched = {
            row.url
            for row in store.iter_crawl_session_items(SESSION_ID)
            if row.status is CrawlItemStatus.FETCHED
        }
        assert fetched == {item.url for item in queue}
    finally:
        store.close()


def test_crawl_only_queue_carries_observed_fetch_provenance(tmp_path: Path) -> None:
    """Producer заполняет провенанс получения из наблюдённого ответа (F6, I19).

    Потребитель конверсии строит ``_ExtractRequest`` **из строки**, а не из конфига:
    ``fetch_mode``/``content_type``/``lang`` участвуют в выборе маршрута, ``engine`` —
    в провенансе получения. Пустая колонка, читающаяся как ``NULL``, означала бы, что
    потребитель выведет условия сам и получит другой ``extractor_version`` — то есть
    другой корпус при тех же объявленных входах (A3).

    Здесь же проверяется, что повторный прогон по тем же URL **не перезаписывает**
    строку (``ON CONFLICT DO NOTHING``): состояние и адрес факта принадлежат потребителю.
    """
    store = SqliteStore(":memory:")
    crawler = _Crawler(_two_host_pages())
    try:
        _build(tmp_path, store, crawler, _Extractor(), _Chunker(), crawl_only=True)

        queue = {item.url: item for item in store.iter_extract_queue(SESSION_ID)}
        assert set(queue) == set(crawler.calls)
        for item in queue.values():
            # Наблюдённые значения ответа — ровно те, что ушли бы в заявку экстракции.
            assert item.fetched_at == TS  # момент получения, а не момент коммита
            assert item.fetch_mode == FetchMode.HTTP.value
            assert item.extract_route == ExtractionRoute.AUTO.value
            assert item.content_type == "text/html"
            assert item.status_code == 200
            assert item.engine == "http"
            assert item.lang == "en"

        # Потребитель забрал один документ: состояние строки принадлежит ему.
        taken_url = sorted(queue)[0]
        store.conn.execute(
            "UPDATE extract_queue SET state = ?, enqueued_at = ?"
            " WHERE session_id = ? AND url = ?",
            (
                ExtractQueueState.DONE.value,
                "2026-01-01T00:00:09+00:00",
                SESSION_ID,
                taken_url,
            ),
        )

        # Повторный прогон: те же URL уже ``FETCHED`` в сеансе ⇒ resume из retained-байтов.
        second = _Crawler(_two_host_pages())
        _build(tmp_path, store, second, _Extractor(), _Chunker(), crawl_only=True)
        assert second.calls == []  # живого фетча не было — проверяется именно запись строки

        again = {item.url: item for item in store.iter_extract_queue(SESSION_ID)}
        assert set(again) == set(queue)
        assert again[taken_url].state is ExtractQueueState.DONE
        assert again[taken_url].enqueued_at == "2026-01-01T00:00:09+00:00"
        for url, item in queue.items():
            assert again[url].fetched_at == item.fetched_at
            assert again[url].raw_path == item.raw_path
            assert again[url].content_hash == item.content_hash
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (2) контроль: тот же вход без crawl_only собирает корпус
# --------------------------------------------------------------------------- #


def test_full_run_still_builds_corpus(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    crawler = _Crawler(_two_host_pages())
    extractor = _Extractor()
    chunker = _Chunker()
    try:
        result = _build(
            tmp_path,
            store,
            crawler,
            extractor,
            chunker,
            crawl_only=False,
        )
        build = _build_of(result)

        assert len(list(store.iter_corpus_docs(RUN_ID))) > 0
        assert build.doc_count > 0
        assert extractor.calls > 0
        # F5: очередь — не побочный эффект, а **единственный** путь конверсии, поэтому
        # после полного прогона она существует и пуста по работе: каждая строка
        # терминальна (``done``), и её число — число добытых документов.
        counts = store.count_extract_queue(SESSION_ID)
        assert set(counts) == {"done"}, counts
        assert counts["done"] == len(list(store.iter_crawl_session_items(SESSION_ID)))
        rows = list(store.iter_extract_queue(SESSION_ID))
        docs = {doc.url for doc in store.iter_corpus_docs(RUN_ID)}
        # Документ корпуса есть по каждому адресу строки: «done» не врёт (F5, §4.1).
        assert {row.url for row in rows} <= docs
        assert all(row.content_hash and row.raw_path for row in rows)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (3) инварианты режима: «без конверсии» несовместимо с пулом извлечения
# --------------------------------------------------------------------------- #


def test_crawl_only_rejects_extraction_pool(tmp_path: Path) -> None:
    """Пул извлечения в режиме «без извлечения» — противоречие, а не удобство."""
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        with pytest.raises(ValueError, match="extraction_pool"):
            build_corpus(
                _plan(),
                _Crawler(_two_host_pages()),
                _Extractor(),
                _Chunker(),
                store,
                urls={"p1": [ROOT]},
                raw_root=tmp_path,
                run_id=RUN_ID,
                domain_profiles=_profiles(),
                crawl_defaults=CrawlRunConfig(max_depth=1),
                crawl_only=True,
                # Заглушка: до исполнения дело не доходит — отказ на входе.
                extraction_pool=object(),  # type: ignore[arg-type]
            )
    finally:
        store.close()


def test_crawl_only_requires_session(tmp_path: Path) -> None:
    """Без сеанса очередь не адресуема: молчаливый краул без публикации запрещён."""
    store = SqliteStore(":memory:")
    try:
        with pytest.raises(ValueError, match="сеанса"):
            build_corpus(
                _plan(),
                _Crawler(_two_host_pages()),
                _Extractor(),
                _Chunker(),
                None,  # без store → без сеанса
                urls={"p1": [ROOT]},
                raw_root=tmp_path,
                domain_profiles=_profiles(),
                crawl_defaults=CrawlRunConfig(max_depth=1),
                crawl_only=True,
            )
    finally:
        store.close()


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело или retained-файл (потребитель очереди не держит тело в RAM).

    До F5 конверсия шла внутри краула, и ``RawPage.content`` страницы был непуст. Теперь
    документ конвертируется из строки очереди: ``content`` пуст, тело лежит по
    ``source_path`` (I19, ``corpus_raw/{hash}.bin``) — ровно так его получает воркер пула.
    Фейковый экстрактор обязан читать тот же носитель, что и адаптер, иначе он проверял бы
    не контракт порта, а прежний способ доставки байтов.
    """
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""


# --------------------------------------------------------------------------- #
# (5) T6 (17 §7.1): crawl-only и полный прогон дают один и тот же краул
# --------------------------------------------------------------------------- #


def test_crawl_only_equals_full_run_crawl_part(tmp_path: Path) -> None:
    """Краул-часть полного прогона совпадает с ``crawl_only`` — по содержимому.

    F5 сделал очередь **единственным** hand-off'ом: ``crawl_only`` — это тот же путь
    без вызова потребителя. Поэтому сравниваются наблюдаемые факты краула: элементы
    сеанса, строки очереди и retained-байты (плюс состав корпуса, который из них
    следует), а не физическая раскладка партиции — её порядок задаёт расписание
    потребителя, и он проверяется отдельно (``test_extract_consumer.py``).

    Идентичность сборки (``index_build_id``/``partition_build_id``) — функция этого же
    краул-набора: второй полный прогон на тех же страницах обязан дать её же.
    """
    stores = {"crawl": SqliteStore(":memory:"), "full": SqliteStore(":memory:"), "again": SqliteStore(":memory:")}
    try:
        crawl_result = _build(
            tmp_path / "crawl",
            stores["crawl"],
            _Crawler(_two_host_pages()),
            _Extractor(),
            _Chunker(),
            crawl_only=True,
        )
        full_result = _build(
            tmp_path / "full",
            stores["full"],
            _Crawler(_two_host_pages()),
            _Extractor(),
            _Chunker(),
            crawl_only=False,
        )
        again_result = _build(
            tmp_path / "again",
            stores["again"],
            _Crawler(_two_host_pages()),
            _Extractor(),
            _Chunker(),
            crawl_only=False,
        )

        def crawl_part(store: SqliteStore) -> dict[str, Any]:
            # ``doc_hash`` намеренно **не** входит: это носитель ключа дедупа, который
            # пишет конверсия (``ingest_one``), а не краул. T6 сравнивает краул-часть
            # ``(url, content_hash, raw_path)``; ключ дедупа — свойство корпуса и
            # проверяется ниже, в сравнении ``docs``/``queued``.
            items = [
                (
                    item.product_id,
                    item.url,
                    item.status,
                    item.content_hash,
                    pathlib.Path(item.raw_path or "").name,
                    item.fetch_mode,
                    item.extract_route,
                    item.content_type,
                    item.engine,
                )
                for item in store.iter_crawl_session_items(SESSION_ID)
            ]
            rows = [
                (
                    row.product_id,
                    row.url,
                    row.content_hash,
                    pathlib.Path(row.raw_path or "").name,
                    row.fetch_mode,
                    row.extract_route,
                    row.content_type,
                    row.engine,
                    row.level,
                    row.pdf_pipeline,
                )
                for row in store.iter_extract_queue(SESSION_ID)
            ]
            return {"items": items, "rows": rows}

        crawl_only_part = crawl_part(stores["crawl"])
        full_part = crawl_part(stores["full"])
        # Краул-часть одна и та же: очередь — передача, а не второй краул.
        assert crawl_only_part["items"] == full_part["items"]
        assert crawl_only_part["rows"] == full_part["rows"]
        # В crawl_only очередь не терминальна (конверсии не было), в полном — терминальна.
        assert {row.state for row in stores["crawl"].iter_extract_queue(SESSION_ID)} == {
            ExtractQueueState.PENDING
        }
        assert {row.state for row in stores["full"].iter_extract_queue(SESSION_ID)} == {
            ExtractQueueState.DONE
        }
        assert crawl_result.errors == [] and full_result.errors == []

        # Содержимое корпуса = содержимое очереди: done не врёт.
        docs = {
            (doc.url, doc.content_hash)
            for doc in stores["full"].iter_corpus_docs(RUN_ID)
        }
        queued = {(url, content_hash) for _p, url, content_hash, *_rest in full_part["rows"]}
        assert docs == queued

        # Идентичность сборки зависит от этого краул-набора, а не от прогона.
        first_build = _build_of(full_result)
        again_build = _build_of(again_result)
        assert again_build.index_build_id == first_build.index_build_id
        assert [
            (entry.product_id, entry.partition_build_id) for entry in again_build.entries
        ] == [
            (entry.product_id, entry.partition_build_id) for entry in first_build.entries
        ]
        assert again_build.doc_count == first_build.doc_count
        assert again_build.chunk_count == first_build.chunk_count
    finally:
        for store in stores.values():
            store.close()
