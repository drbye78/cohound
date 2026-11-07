"""Фолбэк переиспользования: chunk-set отвергнут → пересборка из сохранённых байтов.

Дефект, ради которого это плечо существует (замер 07.11.2026): политика краула дала
двум продуктам ``reuse``, у обоих материал был весь (``url_state`` с датами, активные
``document_version``, байты ``corpus_raw``, нарезка в ``chunk_meta``), но версия чанкера
в записи была чужая — и прогон отверг **каждую** строку, оставив продукт с нулём ячеек.
``reuse_plan`` умел только «взять готовый chunk-set», а пути «байты → текущий конвейер»
из политики краула не было.

Здесь проверяется ровно то, что дефект закрывает:

1. чужой ``chunker_version`` + байты на месте → документ **пересобран** текущим
   чанкером, вошёл в корпус, ``rebuilt=1``, сеть не тронута;
2. байтов нет → документа нет, причина названа отдельно (``no_bytes``), это не
   «переиспользовано 0»;
3. повторный проход идемпотентен и **не платит конверсию дважды** (ключ
   ``extract_cache`` — хеш сырых байтов);
4. провенанс I2d: строка ``corpus_artifact`` пересобранного документа несёт дату
   durable-наблюдения (``adopted``), а не ``NULL`` — иначе ``❌`` понизился бы до ``❓``;
5. ``run_id`` не меняется: пересборка не трогает объявленные входы, а чанкер в корпусе —
   **объявленный** текущий, не версия из чужой записи.

Субстрат наполняется **реальным** путём (первый прогон ``auto``), а не собран руками:
иначе тест доказывал бы согласие заглушки с заглушкой. Нейтральные фикстуры: ``p1``,
``docs.test``.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CorpusDoc,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.corpus_ingest import REUSE_BYTES_REEXTRACT, OfflineReuseReport
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.normalize import load_inputs
from core.pipeline.plan import derive_run_id
from core.pipeline.report import declared_identity_digests
from core.pipeline.run_identity import build_execution_contract
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

RUN_ID = "run-reuse-rebuild"
TS = "2026-01-01T00:00:00+00:00"
#: Дата durable-наблюдения, под которой документ вошёл в состояние (живой случай:
#: ``adopted``, 2026-11-06). Пересборка не наблюдает — дата обязана прийти отсюда.
ADOPTED_TS = "2026-11-06T09:15:00+00:00"

ROOT = "https://docs.test/p1/index"
PAGE = "https://docs.test/p1/print"
BODY = b"page one text"

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"


# --------------------------------------------------------------------------- #
# Порты: всё, что ходит в сеть, падает на первом вызове
# --------------------------------------------------------------------------- #


class _NoNetworkCrawler:
    """Краулер, падающий на любом вызове: доказательство отсутствия сети."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - порт
        self.calls.append((url, plan))
        raise AssertionError(f"reuse: сетевой вызов краулера по {url!r}")


class _SubstrateCrawler:
    """Краулер наполняющего прогона: отдаёт заранее заданные байты."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - порт
        self.calls.append((url, plan))
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
            status_code=200,
        )


class _SubstrateExtractor:
    """Экстрактор первого прогона: текст = декодированные байты."""

    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        text = _page_bytes(page).decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _SubstrateChunker:
    """Чанкер первого прогона: один чанк на документ."""

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


class _CurrentExtractor:
    """Текущий экстрактор пересборки: тот же текст, **другая** версия движка.

    Версия отличается намеренно: пересобранный документ обязан нести ``extractor_version``
    фактического движка (провенанс), а ``content_hash`` — совпасть, иначе это уже
    ``content_change`` (проверяется отдельно).
    """

    version = "extractor-2"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        text = _page_bytes(page).decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _CurrentChunker:
    """Текущий чанкер: **два** чанка — по ним видно, что нарезка не переиспользована."""

    version = "chunker-2"

    def __init__(self) -> None:
        self.calls = 0

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        self.calls += 1
        return [
            Chunk(
                chunk_id=f"{document_id}:{no}",
                chunk_no=no,
                text=f"{doc.text} #{no}",
                heading_path=f"h{no + 1}",
                context_prefix="ctx",
                content_hash=content_hash(f"{doc.text} #{no}"),
            )
            for no in range(2)
        ]


# --------------------------------------------------------------------------- #
# План/стор/хелперы
# --------------------------------------------------------------------------- #


def _page_bytes(page: RawPage) -> bytes:
    """Байты страницы: ``content`` либо файл ``source_path`` (пересборка идёт из файла)."""
    if page.content:
        return page.content
    if page.source_path:
        return Path(page.source_path).read_bytes()
    raise AssertionError("RawPage без байтов: пересборке нечего читать")


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
    """Строка прогона: нужна для FK ``crawl_sessions.run_id`` (L1a)."""
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
) -> None:
    """Наполняет ``state.db`` **реальным** путём: прогон ``auto`` с живым краулером."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _SubstrateCrawler(pages)
    try:
        build_corpus(
            _plan(),
            crawler,
            _SubstrateExtractor(),
            _SubstrateChunker(),
            store,
            urls={"p1": list(roots)},
            raw_root=tmp_path / "raw-first",
            run_id=RUN_ID,
            state=state,
            product_modes={"p1": "incremental"},
            refresh_policy="auto",
            chunker_version="chunker-1",
        )
    finally:
        store.close()
    assert crawler.calls, "наполняющий прогон не фетчил — субстрат не создан"


def _reuse_run(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    roots: list[str],
    store: SqliteStore | None = None,
    extractor: Any = None,
    chunker: Any = None,
    extract_cache: ExtractionCache | None = None,
    chunker_version: str = "chunker-2",
) -> tuple[Any, SqliteStore, _NoNetworkCrawler]:
    """Проход переиспользования: сеть запрещена, chunk-set заведомо чужой версии."""
    own_store = store is None
    store = store or SqliteStore(":memory:")
    if own_store:
        _make_run(store)
    crawler = _NoNetworkCrawler()
    result = build_corpus(
        _plan(),
        crawler,
        extractor or _CurrentExtractor(),
        chunker or _CurrentChunker(),
        store,
        urls={"p1": list(roots)},
        raw_root=tmp_path / "raw-reuse",
        run_id=RUN_ID,
        state=state,
        product_modes={"p1": "incremental"},
        refresh_policy="offline",
        chunker_version=chunker_version,
        extract_cache=extract_cache,
    )
    return result, store, crawler


def _docs(store: SqliteStore) -> list[CorpusDoc]:
    return list(store.iter_corpus_docs(RUN_ID))


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _offline_events(result: Any) -> list[dict[str, Any]]:
    return [
        entry
        for entry in result.log_entries
        if isinstance(entry, dict) and entry.get("event") == "product_end"
    ]


def _notes(result: Any) -> list[str]:
    return [entry for entry in result.log_entries if isinstance(entry, str)]


# --------------------------------------------------------------------------- #
# 1. Пересборка: чужой chunk-set + наличные байты
# --------------------------------------------------------------------------- #


def test_foreign_chunker_version_is_rebuilt_from_stored_bytes(
    tmp_path: Path,
) -> None:
    """Документ входит в корпус, чанки — **текущего** чанкера, ``rebuilt=1``.

    Chunk-set чужой версии по-прежнему не годится как есть (I19 не ослаблен): его
    версия не совпала, и вместо подмены версии документ пересобирается из тех же байтов
    текущим конвейером. Пересборка — не ошибка: ``errors`` пуст, сеть не тронута.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        version_before = state.get_url_state("p1", ROOT).doc_version_id
        extractor, chunker = _CurrentExtractor(), _CurrentChunker()
        result, store, crawler = _reuse_run(
            tmp_path, state, roots=[ROOT], extractor=extractor, chunker=chunker
        )
        try:
            assert crawler.calls == [], "пересборка не трогает сеть"
            assert extractor.calls == 1, "экстрактор вызван из сохранённых байтов"
            assert chunker.calls == 1, "нарезка — текущим чанкером"

            docs = _docs(store)
            assert len(docs) == 1, "документ вошёл в корпус"
            assert docs[0].content_hash == content_hash(BODY)
            assert docs[0].reuse_kind is not None
            assert docs[0].reuse_kind.value == "reused", "сеть не трогалась"
            assert docs[0].http_status is None, "сервер не отвечал — статус NULL"
            assert docs[0].extractor_version == "extractor-2", (
                "провенанс — фактический движок пересборки"
            )

            build = _build_of(result)
            assert build.chunk_count == 2, "чанки текущего чанкера, а не чужой нарезки"
            # Версия нарезки в сборке — **объявленная** текущая, не версия из записи.
            assert build.entries[0].chunker_version == "chunker-2"
            assert build.doc_counters == {"p1": {"total": 1, "new": 0, "reused": 1}}

            # Пересборка — факт, а не ошибка: причина отказа ``reuse_plan`` снята,
            # ``errors`` пуст.
            assert result.errors == [], result.errors

            events = _offline_events(result)
            offline = events[0]["offline"]
            assert offline["reused_docs"] == 0
            assert offline["rebuilt_docs"] == 1
            assert offline["rebuilt_chunks"] == 2
            assert offline["no_bytes"] == []
            assert offline["rebuild_s"] >= 0.0

            # Операторская строка несёт числа и машинный токен пересборки.
            assert any("rebuilt=1" in note for note in _notes(result)), _notes(result)
            assert any("reuse_bytes_rebuild:1" in note for note in _notes(result)), (
                _notes(result)
            )

            # ``url_state`` не переписан: наблюдения в этом прогоне не было.
            assert state.get_url_state("p1", ROOT).doc_version_id == version_before
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 2. Байтов нет: пересобрать нечем
# --------------------------------------------------------------------------- #


def test_missing_bytes_is_a_visible_outcome_not_a_silent_skip(
    tmp_path: Path,
) -> None:
    """Нет файла байтов: документа нет, причина названа ``no_bytes``, не «reused 0»."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        prior = state.get_url_state("p1", ROOT)
        version = state.get_document_version(prior.doc_version_id)
        assert version is not None and version.raw_path
        Path(version.raw_path).unlink()

        result, store, crawler = _reuse_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == []
            assert _docs(store) == [], "пересобрать нечем — документа нет"
            assert any("reuse_bytes_missing" in error for error in result.errors), (
                result.errors
            )
            events = _offline_events(result)
            offline = events[0]["offline"]
            assert offline["no_bytes"] == [ROOT]
            assert offline["rebuilt_docs"] == 0
            assert offline["reused_docs"] == 0
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 3. Идемпотентность и цена: второй проход не платит конверсию
# --------------------------------------------------------------------------- #


def test_repeated_rebuild_is_idempotent_and_hits_the_extract_cache(
    tmp_path: Path,
) -> None:
    """Повторный проход даёт тот же набор чанков и **не** зовёт экстрактор.

    Ключ ``extract_cache`` — хеш сырых байтов (``raw_path.stem``), а не версия чанкера:
    иначе каждая смена нарезки заставляла бы платить конверсию заново. Строки корпуса
    не плодятся: ``doc_id``/``chunk_id`` детерминированы, а запись — upsert.
    """
    cache = ExtractionCache(tmp_path / "extract-cache")
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        store = SqliteStore(":memory:")
        _make_run(store)
        extractor, chunker = _CurrentExtractor(), _CurrentChunker()
        try:
            first, _store, _crawler = _reuse_run(
                tmp_path,
                state,
                roots=[ROOT],
                store=store,
                extractor=extractor,
                chunker=chunker,
                extract_cache=cache,
            )
            first_build = _build_of(first)
            first_chunks = [
                (chunk.chunk_no, chunk.text)
                for chunk in store.iter_chunks(first_build.index_build_id)
            ]
            assert extractor.calls == 1, "первый проход платит конверсию один раз"

            second, _store2, _crawler2 = _reuse_run(
                tmp_path,
                state,
                roots=[ROOT],
                store=store,
                extractor=extractor,
                chunker=chunker,
                extract_cache=cache,
            )
            second_build = _build_of(second)
            second_chunks = [
                (chunk.chunk_no, chunk.text)
                for chunk in store.iter_chunks(second_build.index_build_id)
            ]
            assert extractor.calls == 1, "второй проход обязан попасть в кэш"
            assert first_build.index_build_id == second_build.index_build_id
            assert first_chunks == second_chunks, "тот же набор чанков"
            assert len(_docs(store)) == 1, "строки не плодятся"
            assert _offline_events(second)[0]["offline"]["rebuilt_docs"] == 1
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 4. Провенанс I2d: дата — durable-наблюдение, а не NULL
# --------------------------------------------------------------------------- #


def test_rebuilt_document_carries_the_durable_observation_date(
    tmp_path: Path,
) -> None:
    """``corpus_artifact.observed_at`` пересобранного документа — дата наблюдения.

    Пересборка не наблюдала сервер. Датированный носитель ``❌`` (I2d) — durable-запись
    ``url_state.observed_at`` (в живом случае ``adopted``); ``NULL`` понизил бы ``❌`` до
    ``❓``, и пересборка дала бы худший результат, чем отказ.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        prior = state.get_url_state("p1", ROOT)
        state.upsert_url_state(
            dataclasses.replace(
                prior,
                observed_at=ADOPTED_TS,
                observed_how="adopted",
                observed_actor="owner",
            )
        )
        _result, store, crawler = _reuse_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == []
            rows = list(store.iter_corpus_artifacts(RUN_ID, "p1"))
            assert len(rows) == 1
            row = rows[0]
            assert row.doc_version_id, "строка несёт адрес версии корпуса"
            assert row.observed_at == ADOPTED_TS, (
                "дата — durable-наблюдение, а не NULL и не время прогона"
            )
            assert row.observed_how == "adopted"
        finally:
            store.close()


def test_reextract_drift_is_named_but_the_document_stays(
    tmp_path: Path,
) -> None:
    """Тот же файл, другой текст: ``content_change`` назван, документ остаётся.

    Экстрактор — провенанс, а не ось контракта, поэтому смена текста при том же файле
    не повод выбросить документ (иначе смена движка снова обнулила бы продукт). Факт
    называется отдельным токеном — молчаливой подмены провенанса нет.
    """
    class _DriftingExtractor(_CurrentExtractor):
        def extract(
            self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
        ) -> ExtractedDoc:
            extracted = super().extract(page, route)
            drifted = extracted.text + " (drift)"
            return dataclasses.replace(
                extracted, text=drifted, content_hash=content_hash(drifted)
            )

    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        result, store, crawler = _reuse_run(
            tmp_path, state, roots=[ROOT], extractor=_DriftingExtractor()
        )
        try:
            assert crawler.calls == []
            assert len(_docs(store)) == 1, "документ остаётся в корпусе"
            assert any(REUSE_BYTES_REEXTRACT in error for error in result.errors), (
                result.errors
            )
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 5. Тождество прогона: run_id не меняется
# --------------------------------------------------------------------------- #


def test_rebuild_does_not_change_run_identity(tmp_path: Path) -> None:
    """Пересборка не трогает объявленные входы: ``run_id`` — та же функция.

    ``extractor_version`` в контракт **не** входит (провенанс/``content_change``), а
    ``chunker_version`` входит — но прогон объявил текущую, и пересборка нарезает именно
    ею. Значит пересборка совпадает с объявленным условием, а не меняет его.
    """
    inputs = load_inputs(RUN_YAML, FIXTURES)
    cohort, seeds = declared_identity_digests(inputs, package_dir=FIXTURES)
    contract_before = build_execution_contract(
        inputs, cohort_digest=cohort, seeds_digest=seeds
    )
    run_id_before = derive_run_id(
        inputs,
        cycle_id=inputs.run.snapshot_date.isoformat(),
        cohort_digest=cohort,
        seeds_digest=seeds,
        execution_contract_digest=contract_before,
        refresh_policy="offline",
    )

    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        result, store, crawler = _reuse_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == []
            assert _offline_events(result)[0]["offline"]["rebuilt_docs"] == 1
            # Нарезка корпуса — объявленным текущим чанкером, не версией из записи.
            assert _build_of(result).entries[0].chunker_version == "chunker-2"
        finally:
            store.close()

    contract_after = build_execution_contract(
        inputs, cohort_digest=cohort, seeds_digest=seeds
    )
    run_id_after = derive_run_id(
        inputs,
        cycle_id=inputs.run.snapshot_date.isoformat(),
        cohort_digest=cohort,
        seeds_digest=seeds,
        execution_contract_digest=contract_after,
        refresh_policy="offline",
    )
    assert contract_after == contract_before
    assert run_id_after == run_id_before
    # Почему это верно по построению — пин на состав контракта.
    assert "extractor_version" not in EXECUTION_CONTRACT_FIELDS
    assert "chunker_version" in EXECUTION_CONTRACT_FIELDS


# --------------------------------------------------------------------------- #
# Единица: отчёт и токен
# --------------------------------------------------------------------------- #


def test_report_note_separates_the_three_outcomes() -> None:
    """Строка продукта различает «переиспользовано», «пересобрано», «пересобрать нечем»."""
    report = OfflineReuseReport(
        reused=2,
        chunks=5,
        rebuilt=3,
        rebuilt_chunks=7,
        rebuilt_seconds=1.25,
        no_bytes=("https://docs.test/a",),
    )
    note = report.note("p1")
    assert "reused=2" in note
    assert "rebuilt=3" in note
    assert "chunks=12" in note, "чанки — сумма обоих плеч"
    assert "no_bytes=1" in note
    assert "reuse_bytes_rebuild:3" in note
    assert "1.2 с" in note


def test_report_without_rebuild_has_no_rebuild_token() -> None:
    """Ноль пересобранных документов не публикует токен: нулевой факт — не факт."""
    report = OfflineReuseReport(reused=1, chunks=2)
    note = report.note("p1")
    assert "reuse_bytes_rebuild" not in note
    assert "rebuilt=0" in note
