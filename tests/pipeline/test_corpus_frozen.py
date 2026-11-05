"""``--refresh frozen``: корпус из ``corpus_raw`` текущим стеком, **ноль** сети (план 19 §P3).

Проверяется семантика ``frozen_source=bytes``:

1. ноль сетевых вызовов — краулер и внешний поиск падают на любом вызове;
2. два прогона подряд дают одинаковые ``doc_set_digest``, ``corpus_docs.url`` и текст
   чанков — канонический адрес выбирается ``min(url)`` по durable-группам, а не порядком
   готовности и не ``host_concurrency``;
3. промах кэша, дающий **другой** ``content_hash``, назван ``frozen_reextract_mismatch``,
   а не «контент изменился»: нового документа с новым ``doc_id`` не появляется;
4. отсутствие сохранённых байтов у известного URL — видимая причина
   ``stored_bytes_missing:{n}``, а не ``failed`` (I4);
5. запись снятия durable: ``coverage_complete``/``coverage_digest``/``doc_set_digest`` и
   группы дедупа переживают прогон (``frozen`` не пишет ``crawl_session_items``).

Субстрат наполняется **реальным** путём (первый прогон ``auto``), а не собран руками.
"""

from __future__ import annotations

import datetime as _dt
import pathlib
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CorpusDoc,
    CrawlRunConfig,
    DocumentLevel,
    PdfPipeline,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    COVERAGE_FROZEN_NO_OBSERVATION,
    IndexBuild,
    build_corpus,
)
from core.pipeline.corpus_ingest import (
    FROZEN_REEXTRACT_MISMATCH,
    FrozenBytesReport,
)
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash

RUN_ID = "run-frozen"
TS = "2026-01-01T00:00:00+00:00"

ROOT = "https://docs.test/p1/index"
PAGE = "https://docs.test/p1/print"
BODY = b"page one text"


class _NoNetworkCrawler:
    """Краулер, падающий на **любом** вызове: доказательство отсутствия сети."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        self.calls.append((url, plan))
        raise AssertionError(f"frozen: сетевой вызов краулера по {url!r}")


class _NoSearch:
    """Внешний поиск — тоже сеть: ``frozen`` его не исполняет."""

    def __init__(self) -> None:
        self.calls = 0

    def search(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        raise AssertionError("frozen: вызов внешнего поиска (SERP-контур)")


class _FrozenExtractor:
    """Текущий экстрактор: текст = декодированные байты, хеш = хеш текста.

    Одинаков с наполняющим прогоном — иначе ``frozen_reextract_mismatch`` был бы
    ожидаемым, и тест не доказывал бы воспроизводимость.
    """

    version = "extractor-1"

    def __init__(self, *, hash_override: str | None = None) -> None:
        self.calls = 0
        self.hash_override = hash_override

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        text = _page_bytes(page).decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=self.hash_override or content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _FrozenChunker:
    """Текущий чанкер: один чанк на документ."""

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


class _FirstRunCrawler:
    """Краулер наполняющего прогона: отдаёт заранее заданные байты."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        self.calls.append((url, plan))
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
            status_code=200,
        )


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: ``content`` либо retained-файл (путь — входной, комиссии нет)."""
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=[
            CatalogRow(
                param_id="param_a",
                title="Param A",
                module="mod_a",
                group="grp_1",
                importance="Кр",
                param_type="fact",
                rule="presence of mechanism a",
                absence_rule="registry_absence",
                role_binding="role_a",
            )
        ],
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


def _seed_substrate(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    pages: dict[str, bytes],
    roots: list[str],
    crawl_defaults: CrawlRunConfig | None = None,
) -> None:
    """Наполняет ``state.db`` штатным путём: прогон ``auto`` с живым краулером."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _FirstRunCrawler(pages)
    try:
        build_corpus(
            _plan(),
            crawler,
            _FrozenExtractor(),
            _FrozenChunker(),
            store,
            urls={"p1": list(roots)},
            raw_root=tmp_path / "raw",
            run_id=RUN_ID,
            state=state,
            product_modes={"p1": "incremental"},
            refresh_policy="auto",
            crawl_defaults=crawl_defaults,
        )
    finally:
        store.close()
    assert crawler.calls, "наполняющий прогон не фетчил — субстрат не создан"


def _frozen_run(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    roots: list[str],
    extractor: _FrozenExtractor | None = None,
    host_concurrency: int | None = 2,
    crawl_defaults: CrawlRunConfig | None = None,
    **kwargs: Any,
) -> tuple[Any, SqliteStore, _NoNetworkCrawler]:
    """``frozen``-прогон с краулером, падающим на любом вызове."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _NoNetworkCrawler()
    result = build_corpus(
        _plan(),
        crawler,
        extractor or _FrozenExtractor(),
        _FrozenChunker(),
        store,
        urls={"p1": list(roots)},
        raw_root=tmp_path / "raw",
        run_id=RUN_ID,
        state=state,
        product_modes={"p1": "incremental"},
        refresh_policy="frozen",
        crawl_defaults=(
            crawl_defaults
            if crawl_defaults is not None
            else (
                None
                if host_concurrency is None
                else CrawlRunConfig(host_concurrency=host_concurrency)
            )
        ),
        **kwargs,
    )
    return result, store, crawler


def _docs(store: SqliteStore) -> list[CorpusDoc]:
    return list(store.iter_corpus_docs(RUN_ID))


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _chunk_texts(store: SqliteStore, result: Any) -> list[str]:
    build = _build_of(result)
    (entry,) = build.entries
    return [chunk.text for chunk in store.iter_chunks(entry.index_build_id)]


def test_frozen_is_zero_network_and_reproducible_at_host_concurrency_two(
    tmp_path: Path,
) -> None:
    """Два ``frozen``-прогона при ``host_concurrency=2`` неотличимы и не ходят в сеть."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY, PAGE: BODY}, roots=[ROOT, PAGE])

        first, store1, crawler1 = _frozen_run(tmp_path, state, roots=[ROOT, PAGE])
        try:
            assert crawler1.calls == [], "frozen обязан не трогать сеть"
            docs1 = _docs(store1)
            assert len(docs1) == 1, "два URL одного контента — один документ (дедуп)"
            assert docs1[0].url == ROOT, "канонический адрес — min(url), а не порядок"
            assert docs1[0].reuse_kind is not None
            assert docs1[0].reuse_kind.value == "reused"
            assert docs1[0].fetched_at is None and docs1[0].http_status is None
            texts1 = _chunk_texts(store1, first)
            capture1 = state.last_corpus_capture("p1")
            assert capture1 is not None
            assert capture1.source == "bytes"
            assert capture1.doc_set_digest
            # Группа дедупа durable: оба URL названы одним doc_hash.
            groups: dict[str, list[str]] = {}
            for doc in capture1.docs:
                groups.setdefault(doc.doc_hash, []).append(doc.url)
            assert groups == {content_hash(BODY): [ROOT, PAGE]}
        finally:
            store1.close()

        # Второй прогон — из того же состояния и тех же сохранённых байтов.
        second, store2, crawler2 = _frozen_run(tmp_path, state, roots=[ROOT, PAGE])
        try:
            assert crawler2.calls == []
            docs2 = _docs(store2)
            assert [doc.url for doc in docs2] == [doc.url for doc in docs1]
            assert _chunk_texts(store2, second) == texts1
            capture2 = state.last_corpus_capture("p1")
            assert capture2 is not None
            assert capture2.doc_set_digest == capture1.doc_set_digest
        finally:
            store2.close()


def test_live_run_records_observation_and_capture(tmp_path: Path) -> None:
    """План 20 §3: живой обход записывает наблюдение и снятие корня.

    ``observed_at`` — время ответа сервера (``fetched_at``), а не «сейчас»;
    ``observed_how='live'``; ``capture_id`` ссылается на снятие этого корня, а дайджесты
    снятия догоняются на границе продукта (не считаются второй раз).
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])

        row = state.get_url_state("p1", ROOT)
        assert row is not None
        assert row.observed_at == TS
        assert row.observed_how == "live"
        assert row.observed_actor is None
        assert row.capture_id is not None

        capture = state.get_capture(row.capture_id)
        assert capture is not None
        assert capture.root_url == ROOT
        assert capture.observed_how == "live"
        assert capture.complete is True
        assert capture.run_id == RUN_ID
        assert capture.doc_set_digest is not None


def test_frozen_keeps_live_observation(tmp_path: Path) -> None:
    """Проход без сети не обнуляет наблюдение живого прогона (план 20 §3).

    ``frozen`` не пишет ``url_state`` и не передаёт наблюдение; прежние
    ``observed_at``/``observed_how``/``capture_id`` обязаны сохраниться.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        before = state.get_url_state("p1", ROOT)
        assert before is not None and before.observed_at == TS

        _result, store, crawler = _frozen_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == []
            assert _docs(store), "frozen собрал корпус из сохранённых байтов"
        finally:
            store.close()

        after = state.get_url_state("p1", ROOT)
        assert after is not None
        assert after.observed_at == before.observed_at
        assert after.observed_how == before.observed_how
        assert after.capture_id == before.capture_id


def test_frozen_reextract_drift_is_a_named_token_not_a_new_document(
    tmp_path: Path,
) -> None:
    """Промах кэша с другим ``content_hash`` — ``frozen_reextract_mismatch``, не новый doc."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])

        result, store, crawler = _frozen_run(
            tmp_path,
            state,
            roots=[ROOT],
            extractor=_FrozenExtractor(hash_override="0" * 64),
        )
        try:
            assert crawler.calls == []
            assert _docs(store) == [], "дрейф не публикует документ под ложным doc_id"
            assert any(
                FROZEN_REEXTRACT_MISMATCH in error and ROOT in error
                for error in result.errors
            ), f"нет видимого токена дрейфа: {result.errors}"
            events = [
                entry
                for entry in result.log_entries
                if isinstance(entry, dict) and entry.get("event") == "product_end"
            ]
            assert events[0]["frozen"]["reextract_mismatch"] == [ROOT]
        finally:
            store.close()


def test_frozen_missing_stored_bytes_is_visible_with_count(tmp_path: Path) -> None:
    """Нет файла сохранённых байтов — ``stored_bytes_missing:{n}``, а не ``failed``."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        # Удаляем retained-файл: состояние знает URL, а байтов нет.
        prior = state.get_url_state("p1", ROOT)
        assert prior is not None and prior.doc_version_id is not None
        version = state.get_document_version(prior.doc_version_id)
        assert version is not None and version.raw_path
        Path(version.raw_path).unlink()

        result, store, crawler = _frozen_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == []
            assert _docs(store) == []
            assert any(
                "stored_bytes_missing:1" in error for error in result.errors
            ), f"нет видимой причины с числом: {result.errors}"
            events = [
                entry
                for entry in result.log_entries
                if isinstance(entry, dict) and entry.get("event") == "product_end"
            ]
            assert events[0]["frozen"]["stored_bytes_missing"] == [ROOT]
        finally:
            store.close()


def test_frozen_coverage_is_complete_by_capture_and_names_its_own_reason(
    tmp_path: Path,
) -> None:
    """Покрытие ``frozen`` полно **по снятию** и названо ``frozen_no_network_observation``.

    F3 (план 20 §11.2): полноту решает датированное снятие корня, а не элементы сеанса.
    Это и есть замеренный дефект ``demo-absence``: ``frozen`` терял все ``❌``, потому что
    ``observed_sources == 0`` читалось как «нет наблюдений». ``reason`` остаётся
    провенансом прогона (сеть не трогалась), а ``complete`` — факт корпуса.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        result, store, crawler = _frozen_run(tmp_path, state, roots=[ROOT], search=_NoSearch(), serp_rounds=1, serp_source="declared")
        try:
            assert crawler.calls == []
            coverage = store.read_corpus_coverage(RUN_ID, "p1")
            assert coverage is not None
            assert coverage.observed_sources == 0
            assert coverage.complete is True
            assert [root.complete for root in coverage.roots] == [True]
            assert coverage.reason == COVERAGE_FROZEN_NO_OBSERVATION
            events = [
                entry
                for entry in result.log_entries
                if isinstance(entry, dict) and entry.get("event") == "product_end"
            ]
            # ``reason`` строки продукта — только zero-source; при непустом корпусе
            # причина живёт в покрытии и в отчёте ``frozen``.
            assert events[0]["frozen"]["docs"] == 1
            assert events[0]["frozen"]["label"] == "frozen"
        finally:
            store.close()


def test_frozen_report_note_names_the_source_and_the_price() -> None:
    """Строка продукта называет свой источник, число причин и отсутствие наблюдений."""
    report = FrozenBytesReport(
        docs=2,
        chunks=9,
        reextracted=1,
        cache_hits=1,
        unknown_roots=("https://docs.test/a",),
        stored_bytes_missing=("https://docs.test/b",),
    )
    note = report.note("p1")
    assert "frozen p1" in note
    assert "документов 2" in note
    assert "чанков 9" in note
    assert "нет байтов 1" in note
    assert "корней вне состояния 1" in note
    # F3: проход без сети больше не отменяет `❌` — он публикуется только с датой носителя
    # (I2d). Строка обязана называть это, а не прежнее «❌ не публикуется».
    assert "наблюдений этого прогона нет" in note
    assert "только с датой носителя, I2d" in note
    assert "❌ не публикуется" not in note
    assert report.stored_bytes_missing_token == "stored_bytes_missing:1"


def test_frozen_bytes_writes_no_session_items(tmp_path: Path) -> None:
    """Элементы сеанса не пишутся: ``frozen`` — не фетч, а пере-извлечение байтов."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        _result, store, _crawler = _frozen_run(tmp_path, state, roots=[ROOT])
        try:
            assert list(store.iter_crawl_session_items("crawl:" + RUN_ID)) == []
        finally:
            store.close()


class _LevelExtractor:
    """Экстрактор, который **называет** условие, которым его построили.

    Штамп — тот же текст байтов, поэтому ``content_hash`` совпадает с записанным: тест
    проверяет выбор условия, а не дрейф текста.
    """

    def __init__(self, version: str) -> None:
        self.version = version
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        body = _page_bytes(page)
        return ExtractedDoc(
            text=body.decode("utf-8", errors="replace"),
            content_hash=content_hash(body),
            extractor_version=self.version,
            page_count=1,
        )


def test_frozen_extracts_at_the_declared_level_not_the_composition_one(
    tmp_path: Path,
) -> None:
    """``frozen`` выбирает экстрактор по объявленному уровню (зеркало живого пути).

    Без этого пере-извлечение идёт уровнем композиции и штампует ``extractor_version``
    условия, которого в корпусе не было: на стенде ``ru-pg-dbms`` это дало
    ``frozen_reextract_mismatch`` у 4 из 5 PDF и второй ``doc_version_id`` у выжившего.
    """
    calls: list[tuple[DocumentLevel, PdfPipeline]] = []

    def factory(level: DocumentLevel, pipeline: PdfPipeline) -> _LevelExtractor:
        calls.append((level, pipeline))
        return _LevelExtractor(f"docling-fake-{level.value}/{pipeline.value}")

    defaults = CrawlRunConfig(document_level=DocumentLevel.LAYOUT_TABLES)
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(
            tmp_path,
            state,
            pages={ROOT: BODY},
            roots=[ROOT],
            crawl_defaults=defaults,
        )
        _result, store, crawler = _frozen_run(
            tmp_path,
            state,
            roots=[ROOT],
            crawl_defaults=defaults,
            extractor_factory=factory,
        )
        try:
            assert crawler.calls == [], "frozen обязан не трогать сеть"
            assert calls == [(DocumentLevel.LAYOUT_TABLES, PdfPipeline.STRUCTURED)]
            docs = _docs(store)
            assert len(docs) == 1
            assert docs[0].extractor_version == (
                "docling-fake-layout_tables/structured"
            ), "условие замера — объявленный уровень, а не уровень композиции"
        finally:
            store.close()
