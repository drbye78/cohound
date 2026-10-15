"""Тесты ``SqliteStore`` (W3/T3.1): schema, round-trip домена, триггеры I18.

Все тесты работают на ``:memory:``-БД. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
from collections.abc import Iterator

import pytest

from core.domain import (
    AbsenceRuleType,
    AdapterKind,
    Cell,
    CellEvidence,
    CellState,
    CorpusChunk,
    CorpusDoc,
    CrawlProvenance,
    CrawlSessionProvenance,
    EndpointKind,
    EvidenceType,
    Focus,
    Gate,
    GateDecision,
    IndexManifestEntry,
    LLMScenario,
    ProtocolExhaustion,
    RankingPolicy,
    RegistryFact,
    RunConfig,
    RunManifest,
    RunMode,
    ScenarioBinding,
    SearchKind,
    SearchLogEntry,
    SearchPhase,
    SourceKind,
    SourceScope,
    StaleReason,
    Status,
    TerminationReason,
)
from core.ports.store import GateStore, StorePort
from core.ports.types import Chunk
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-1"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        yield s


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(modules=["m1", "m2"], params=["param.a"]),
        languages=["ru", "en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _cell(param_id: str, **overrides: object) -> Cell:
    base: dict[str, object] = {
        "run_id": RUN_ID,
        "product_id": "p1",
        "param_id": param_id,
        "state": CellState.PENDING,
    }
    base.update(overrides)
    return Cell(**base)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# schema / protocols
# --------------------------------------------------------------------------- #


def test_schema_opens(store: SqliteStore) -> None:
    ver = store.conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    assert ver["value"] == "3.4.0"
    tables = {
        r["name"] for r in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    for t in (
        "products",
        "runs",
        "llm_scenarios",
        "corpus_docs",
        "corpus_chunks",
        "index_manifest",
        "cells",
        "cell_evidence",
        "gate_decisions",
        "search_log",
        "search_exhaustion",
        "registry_facts",
        "run_inputs",
        "crawl_sessions",
        "crawl_session_items",
    ):
        assert t in tables
    # L1b (schema 3.1.0): legacy crawl_attempts удалена (замещена сеансом краула).
    assert "crawl_attempts" not in tables
    # Phase 2 (schema 3.2.0): провенанс краула в манифесте прогона.
    runs_cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(runs)")}
    assert "crawl_provenance_json" in runs_cols
    # Backlog B (schema 3.3.0): операционные оси краула — границы ``execution_change`` (§13).
    assert "crawl_strategy" in runs_cols
    assert "crawl_rate_limit" in runs_cols
    # P1 (schema 3.4.0): деплой-провенанс манифеста (07 §7) и порог ранжирования (§6/B-1).
    assert {"deploy_profile", "edge_schema_version", "edge_capabilities_json",
            "ranking_epsilon"} <= runs_cols
    # P1: снимок плановых входов прогона — реальная таблица.
    inputs_cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(run_inputs)")}
    assert {"run_id", "snapshot_version", "payload_json", "payload_sha256",
            "created_at"} <= inputs_cols
    # P2 (schema 2.2.0): build-id witness — версии/отпечаток контента.
    chunk_cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(corpus_chunks)")}
    assert "extractor_version" in chunk_cols
    manifest_cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(index_manifest)")}
    assert "extractor_version" in manifest_cols
    assert "content_digest" in manifest_cols


def test_store_implements_protocols(store: SqliteStore) -> None:
    assert isinstance(store, StorePort)
    assert isinstance(store, GateStore)


# --------------------------------------------------------------------------- #
# products / run / manifest
# --------------------------------------------------------------------------- #


def test_upsert_products_accepts_objects_and_dicts(store: SqliteStore) -> None:
    from core.ports.types import ProductRef

    store.upsert_products(
        [
            ProductRef(product_id="p1", canonical_name="One", vendor="V1", jurisdiction="RU"),
            {"product_id": "p2", "canonical_name": "Two", "vendor": "V2"},
        ]
    )
    rows = {
        r["product_id"]: r
        for r in store.conn.execute("SELECT * FROM products ORDER BY product_id")
    }
    assert rows["p1"]["canonical_name"] == "One"
    assert rows["p1"]["vendor"] == "V1"
    assert rows["p1"]["jurisdiction"] == "RU"
    assert rows["p2"]["canonical_name"] == "Two"
    assert rows["p1"]["first_seen"] is not None
    assert rows["p1"]["last_seen"] is not None


def test_create_run_then_read_manifest_round_trip(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    manifest = store.read_manifest(RUN_ID)
    assert manifest.run_id == RUN_ID
    assert manifest.case_id == "case-1"
    assert manifest.mode is RunMode.STRICT
    assert manifest.anchor_product_id == "p1"
    assert manifest.cohort_ref == "cohort.yaml"
    assert manifest.focus_modules == ["m1", "m2"]
    assert manifest.snapshot_date == "2026-01-01"
    assert manifest.requested_ranking is RankingPolicy.CAPABILITY_RANKED
    assert manifest.created_at  # ISO now


def test_set_phase_state_convention(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    store.set_phase_state(RUN_ID, "W6", "evaluated")
    state = store.conn.execute(
        "SELECT state FROM runs WHERE run_id = ?", (RUN_ID,)
    ).fetchone()["state"]
    # Durable-носитель resume (02 §A.1): JSON-карта фаз в единственной колонке runs.state.
    assert json.loads(state) == {"phase_state": {"W6": "evaluated"}}


def test_read_manifest_missing_run_raises(store: SqliteStore) -> None:
    with pytest.raises(ValueError):
        store.read_manifest("does-not-exist")


# --------------------------------------------------------------------------- #
# cells
# --------------------------------------------------------------------------- #


def test_save_cell_and_iter_cells_round_trip(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    from core.domain import LLMEvidence

    cell = _cell(
        "param.a",
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.DOCUMENTATION,
        evidence=[LLMEvidence(source_id="src-1", quote="q")],
        reviewed_by="human-1",
        index_build_id="build-1",
        retrieval_profile="R1",
        retrieval_params_json='{"rrf_k": 60}',
        stale_reason=StaleReason.NEW_BUILD,
    )
    store.save_cell(cell)

    got = list(store.iter_cells(RUN_ID))
    assert len(got) == 1
    c = got[0]
    # enum-поля, персистируемые моделью Cell
    assert c.state is CellState.EVALUATED
    assert c.status is Status.OK
    assert c.source_kind is SourceKind.DOCUMENTATION
    assert c.stale_reason is StaleReason.NEW_BUILD
    # прочие поля
    assert c.reviewed_by == "human-1"
    assert c.index_build_id == "build-1"
    assert c.retrieval_profile == "R1"
    assert c.retrieval_params_json == '{"rrf_k": 60}'
    # evidence не хранится в cells — восстанавливается пустым
    assert c.evidence == []
    assert c.absence_evidence == []
    # absence_rule не имеет колонки в cells — дефолт модели
    assert c.absence_rule is AbsenceRuleType.EXPLICIT


def test_save_cell_failed_and_preliminary_null_status(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    store.save_cell(_cell("p.failed", state=CellState.FAILED, status=None))
    store.save_cell(_cell("p.prelim", state=CellState.PRELIMINARY, status=None))
    statuses = {
        r["param_id"]: r["status"]
        for r in store.conn.execute("SELECT param_id, status FROM cells")
    }
    assert statuses == {"p.failed": None, "p.prelim": None}


def test_save_cell_na_requires_reviewer(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    # Валидатор Cell не даёт собрать N/A без ревьюера, поэтому обходим его, чтобы
    # проверить именно SQL-триггер I6/I7.
    bad = Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id="p.na",
        state=CellState.REVIEWED,
        status=Status.NOT_APPLICABLE,
        source_kind=None,
        absence_rule=AbsenceRuleType.EXPLICIT,
        evidence=[],
        absence_evidence=[],
        reviewed_by=None,
    )
    with pytest.raises(sqlite3.IntegrityError):
        store.save_cell(bad)

    ok = _cell(
        "p.na", state=CellState.REVIEWED, status=Status.NOT_APPLICABLE, reviewed_by="human-1"
    )
    store.save_cell(ok)
    got = list(store.iter_cells(RUN_ID))
    assert got[0].status is Status.NOT_APPLICABLE
    assert got[0].reviewed_by == "human-1"


def test_save_cells_batch_two_chunks(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    cells = [_cell(f"param.{i}") for i in range(10)]
    store.save_cells_batch(cells, chunk_size=5)  # 10 ячеек -> 2 транзакции
    assert len(list(store.iter_cells(RUN_ID))) == 10


def test_iter_cells_state_filter(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    store.save_cells_batch(
        [
            _cell("p.pending"),
            _cell("p.evaluated", state=CellState.EVALUATED, status=Status.ABSENT,
                  absence_evidence=[{"source_scope": "partial"}]),
        ]
    )
    only_eval = list(store.iter_cells(RUN_ID, state="evaluated"))
    assert [c.param_id for c in only_eval] == ["p.evaluated"]


# --------------------------------------------------------------------------- #
# evidence
# --------------------------------------------------------------------------- #


def test_add_evidence_round_trip(store: SqliteStore) -> None:
    ev = CellEvidence(
        evidence_id="ev-1",
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        source_kind=SourceKind.MANUAL,
        evidence_type=EvidenceType.DIRECT,
        source_id="src-1",
        source_url="https://example.test/doc",
        citation_quote="quote",
        source_scope=SourceScope.PARTIAL,
        scope_attested_by="human-1",
        index_build_id="build-1",
        retrieved_at="2026-01-01T00:00:00Z",
    )
    store.add_evidence(ev)
    row = store.conn.execute(
        "SELECT * FROM cell_evidence WHERE evidence_id = ?", ("ev-1",)
    ).fetchone()
    assert row["source_kind"] == "manual"
    assert row["evidence_type"] == "direct"
    assert row["source_scope"] == "partial"
    assert row["source_url"] == "https://example.test/doc"
    assert row["scope_attested_by"] == "human-1"


def test_add_evidence_batch(store: SqliteStore) -> None:
    evs = [
        CellEvidence(
            evidence_id=f"ev-{i}",
            run_id=RUN_ID,
            product_id="p1",
            param_id="param.a",
            source_kind=SourceKind.MANUAL,
            evidence_type=EvidenceType.DERIVED,
            source_id=f"src-{i}",
        )
        for i in range(3)
    ]
    store.add_evidence_batch(evs)
    count = store.conn.execute("SELECT COUNT(*) AS n FROM cell_evidence").fetchone()["n"]
    assert count == 3


# --------------------------------------------------------------------------- #
# search log / exhaustion / registry
# --------------------------------------------------------------------------- #


def test_log_search(store: SqliteStore) -> None:
    entry = SearchLogEntry(
        run_id=RUN_ID,
        phase=SearchPhase.SERP,
        kind=SearchKind.EXTERNAL,
        provider="provider-1",
        adapter_kind=AdapterKind.SERP,
        language="en",
        query="some query",
        results_count=10,
        selected=3,
        termination_reason=TerminationReason.RESULTS_EXHAUSTED,
        ts="2026-01-01T00:00:00Z",
    )
    store.log_search(entry)
    row = store.conn.execute("SELECT * FROM search_log").fetchone()
    assert row["phase"] == "serp"
    assert row["kind"] == "external"
    assert row["adapter_kind"] == "serp"
    assert row["termination_reason"] == "results_exhausted"
    assert row["selected"] == 3
    assert row["log_id"] is not None


def test_save_exhaustion(store: SqliteStore) -> None:
    pe = ProtocolExhaustion(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        contours=["registry", "corpus", "serp"],
        queries=["q1", "q2"],
        rounds=2,
        budget={"serp": 5},
        termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        exhausted_at="2026-01-01T00:00:00Z",
        notes="note",
    )
    store.save_exhaustion(pe)
    row = store.conn.execute(
        "SELECT protocol_json FROM search_exhaustion WHERE run_id = ?", (RUN_ID,)
    ).fetchone()
    payload = json.loads(row["protocol_json"])
    assert payload["contours"] == ["registry", "corpus", "serp"]
    assert payload["rounds"] == 2
    assert payload["budget"] == {"serp": 5}
    assert payload["termination_reason"] == "budget_exhausted"


def test_save_registry_fact(store: SqliteStore) -> None:
    rf = RegistryFact(
        run_id=RUN_ID,
        product_id="p1",
        registry_id="reg-1",
        fact_json='{"status": "active"}',
        source_url="https://registry.test/p1",
        fetched_at="2026-01-01T00:00:00Z",
    )
    store.save_registry_fact(rf)
    row = store.conn.execute(
        "SELECT * FROM registry_facts WHERE registry_id = ?", ("reg-1",)
    ).fetchone()
    assert row["source_url"] == "https://registry.test/p1"
    assert json.loads(row["fact_json"]) == {"status": "active"}


# --------------------------------------------------------------------------- #
# gates
# --------------------------------------------------------------------------- #


def test_save_gate_decision_and_iter(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    store.save_gate_decision(
        GateDecision(
            decision_id="gd-1",
            run_id=RUN_ID,
            gate=Gate.G2,
            target="role:management_console",
            actor="human-1",
            decided_at="2026-01-01T00:00:00Z",
            notes="accepted",
        )
    )
    decisions = list(store.iter_gate_decisions(RUN_ID))
    assert len(decisions) == 1
    d = decisions[0]
    assert d.decision_id == "gd-1"
    assert d.gate is Gate.G2
    assert d.actor == "human-1"
    assert d.notes == "accepted"


# --------------------------------------------------------------------------- #
# llm scenarios / manifest / trigger I18
# --------------------------------------------------------------------------- #


def test_save_llm_scenarios_and_manifest_includes_them(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    store.save_llm_scenarios(
        [
            ScenarioBinding(
                run_id=RUN_ID,
                scenario=LLMScenario.EVALUATOR,
                model_id="model-eval",
                endpoint_kind=EndpointKind.SELF_HOST,
                web_tool=False,
                temperature=0.0,
            ),
            ScenarioBinding(
                run_id=RUN_ID,
                scenario=LLMScenario.SEARCH_ADAPTER,
                model_id="model-search",
                endpoint_kind=EndpointKind.API,
                web_tool=True,
            ),
        ]
    )
    manifest = store.read_manifest(RUN_ID)
    assert len(manifest.llm_scenarios) == 2
    by_scenario = {s.scenario: s for s in manifest.llm_scenarios}
    assert by_scenario[LLMScenario.EVALUATOR].web_tool is False
    assert by_scenario[LLMScenario.SEARCH_ADAPTER].web_tool is True
    assert by_scenario[LLMScenario.EVALUATOR].endpoint_kind is EndpointKind.SELF_HOST


def test_trigger_i18_rejects_evaluator_with_web_tool(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    # Валидатор ScenarioBinding не даёт собрать такое значение, поэтому обходим его,
    # чтобы проверить именно SQL-триггер I18.
    bad = ScenarioBinding.model_construct(
        run_id=RUN_ID,
        scenario=LLMScenario.EVALUATOR,
        model_id="model-eval",
        model_version=None,
        endpoint_kind=EndpointKind.SELF_HOST,
        web_tool=True,
        decoder=None,
        seed=None,
        temperature=None,
        constrained_decoding=None,
    )
    with pytest.raises(sqlite3.IntegrityError):
        store.save_llm_scenarios([bad])
    assert store.conn.execute("SELECT COUNT(*) AS n FROM llm_scenarios").fetchone()["n"] == 0


# --------------------------------------------------------------------------- #
# corpus chunk helpers
# --------------------------------------------------------------------------- #


def test_get_chunk_and_resolve_source_url(store: SqliteStore) -> None:
    with store.conn:
        store.conn.execute(
            "INSERT INTO corpus_docs (doc_id, product_id, url) VALUES ('doc-1', 'p1',"
            " 'https://example.test/page')"
        )
        store.conn.execute(
            "INSERT INTO corpus_chunks"
            " (chunk_id, doc_id, product_id, url, heading_path, chunk_no, text,"
            "  context_prefix, content_hash)"
            " VALUES ('chunk-1', 'doc-1', 'p1', 'https://example.test/page#a',"
            " 'H1 > H2', 0, 'body text', 'prefix', 'hash-1')"
        )
    chunk = store.get_chunk("chunk-1")
    assert chunk is not None
    assert chunk.chunk_id == "chunk-1"
    assert chunk.chunk_no == 0
    assert chunk.text == "body text"
    assert chunk.heading_path == "H1 > H2"
    assert chunk.context_prefix == "prefix"
    assert chunk.content_hash == "hash-1"
    assert store.get_chunk("missing") is None
    assert store.resolve_source_url("chunk-1") == "https://example.test/page#a"
    assert store.resolve_source_url("missing") is None


# --------------------------------------------------------------------------- #
# iter_chunks (resume/rebuild индексной сборки)
# --------------------------------------------------------------------------- #


def _doc(doc_id: str, product_id: str) -> CorpusDoc:
    """Нейтральный ``corpus_docs``-документ для FK чанков."""
    return CorpusDoc(
        doc_id=doc_id,
        product_id=product_id,
        url=f"https://example.test/{doc_id}",
        fetched_at="2026-01-01T00:00:00Z",
        content_hash=f"doc-hash-{doc_id}",
    )


def _chunk(
    chunk_id: str,
    *,
    doc_id: str = "doc-1",
    product_id: str = "p1",
    chunk_no: int = 0,
    build_id: str = "build-1",
    text: str = "body text",
) -> CorpusChunk:
    """Нейтральный ``corpus_chunks``-чанк для тестов итерации."""
    return CorpusChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        product_id=product_id,
        url=f"https://example.test/{doc_id}#{chunk_id}",
        heading_path="H1 > H2",
        lang="en",
        chunk_no=chunk_no,
        text=text,
        context_prefix="prefix",
        content_hash=f"hash-{chunk_id}",
        build_id=build_id,
    )


def test_iter_chunks_deterministic_order_and_port_type(store: SqliteStore) -> None:
    store.upsert_corpus_doc(_doc("doc-1", "p1"))
    store.upsert_corpus_doc(_doc("doc-2", "p2"))
    # Вставка в «перемешанном» порядке: итерация обязана вернуть детерминированный
    # порядок (product_id, chunk_no, chunk_id), а не порядок вставки.
    store.add_chunks_batch(
        [
            _chunk("p1-b", chunk_no=1),
            _chunk("p2-a", doc_id="doc-2", product_id="p2", chunk_no=0),
            _chunk("p1-c", chunk_no=0),
            _chunk("p1-a", chunk_no=0),
        ]
    )
    got = list(store.iter_chunks("build-1"))
    assert [c.chunk_id for c in got] == ["p1-a", "p1-c", "p1-b", "p2-a"]
    assert all(isinstance(c, Chunk) for c in got)
    first = got[0]
    assert first.chunk_no == 0
    assert first.text == "body text"
    assert first.heading_path == "H1 > H2"
    assert first.context_prefix == "prefix"
    assert first.content_hash == "hash-p1-a"
    # ``url`` намеренно не переносится: у порт-типа ``Chunk`` нет поля ``url``.
    assert not hasattr(first, "url")


def test_iter_chunks_product_filter(store: SqliteStore) -> None:
    store.upsert_corpus_doc(_doc("doc-1", "p1"))
    store.upsert_corpus_doc(_doc("doc-2", "p2"))
    store.add_chunks_batch(
        [
            _chunk("p1-a", chunk_no=0),
            _chunk("p1-b", chunk_no=1),
            _chunk("p2-a", doc_id="doc-2", product_id="p2", chunk_no=0),
        ]
    )
    only_p2 = list(store.iter_chunks("build-1", product_id="p2"))
    assert [c.chunk_id for c in only_p2] == ["p2-a"]
    all_chunks = list(store.iter_chunks("build-1", product_id=None))
    assert [c.chunk_id for c in all_chunks] == ["p1-a", "p1-b", "p2-a"]


def test_iter_chunks_unknown_build_is_empty(store: SqliteStore) -> None:
    store.upsert_corpus_doc(_doc("doc-1", "p1"))
    store.add_chunks_batch([_chunk("p1-a", build_id="build-1")])
    assert list(store.iter_chunks("build-unknown")) == []
    assert list(store.iter_chunks("build-1", product_id="p9")) == []


def test_p2_extractor_version_and_content_digest_roundtrip(store: SqliteStore) -> None:
    """P2: ``extractor_version`` чанка и ``extractor_version``/``content_digest`` манифеста."""
    store.upsert_corpus_doc(_doc("doc-1", "p1"))
    chunk = _chunk("p1-a").model_copy(update={"extractor_version": "extractor-x"})
    store.add_chunks_batch([chunk])
    row = store.conn.execute(
        "SELECT extractor_version FROM corpus_chunks WHERE chunk_id = 'p1-a'"
    ).fetchone()
    assert row["extractor_version"] == "extractor-x"

    entry = IndexManifestEntry(
        index_build_id="ib_x",
        product_id="p1",
        package_id="pkg",
        chunker_version="chunker-1",
        extractor_version="extractor-x",
        lexical_engine_version="lex-1",
        vector_store="npy",
        content_digest="digest-x",
    )
    store.save_index_manifest(entry)
    mrow = store.conn.execute(
        "SELECT extractor_version, content_digest FROM index_manifest"
        " WHERE index_build_id = 'ib_x' AND product_id = 'p1'"
    ).fetchone()
    assert mrow["extractor_version"] == "extractor-x"
    assert mrow["content_digest"] == "digest-x"


# --------------------------------------------------------------------------- #
# deploy provenance on the manifest (R7/§13, 07 §7)
# --------------------------------------------------------------------------- #


def test_read_manifest_deploy_fields_default_none(store: SqliteStore) -> None:
    """Колонки деплой-провенанса реальны, но не записаны → ``read_manifest`` даёт ``None``."""
    store.create_run(RUN_ID, _cfg())
    manifest = store.read_manifest(RUN_ID)
    assert manifest.deploy_profile is None
    assert manifest.edge_schema_version is None
    assert manifest.edge_capabilities == {}


def test_read_manifest_reads_deploy_fields_from_columns(store: SqliteStore) -> None:
    """Поля деплой-провенанса читаются из реальных колонок ``runs`` (P1, 07 §7)."""
    store.create_run(RUN_ID, _cfg())
    with store.conn:
        store.conn.execute(
            "UPDATE runs SET deploy_profile = ?, edge_schema_version = ?,"
            " edge_capabilities_json = ? WHERE run_id = ?",
            ("split", "1.0.0", '{"gateway": "0.4.2"}', RUN_ID),
        )
    manifest = store.read_manifest(RUN_ID)
    assert manifest.deploy_profile == "split"
    assert manifest.edge_schema_version == "1.0.0"
    assert manifest.edge_capabilities == {"gateway": "0.4.2"}


def test_persist_manifest_roundtrips_deploy_fields(store: SqliteStore) -> None:
    """``persist_manifest``→``read_manifest`` несут деплой-провенанс (P1, 07 §7)."""
    store.create_run(RUN_ID, _cfg())
    # До записи манифеста деплой-провенанс пуст.
    assert store.read_manifest(RUN_ID).deploy_profile is None

    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case-1",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="2.0.0",
        package_version="pkg-1",
        catalog_version="cat-1",
        status_dict_version="sd-1",
        scoring_policy_version="sp-1",
        anchor_product_id="p1",
        deploy_profile="local",
        edge_schema_version="1",
        edge_capabilities={"gateway": "0.4.2", "edge": "1"},
    )
    store.persist_manifest(RUN_ID, manifest)
    read = store.read_manifest(RUN_ID)
    assert read.deploy_profile == "local"
    assert read.edge_schema_version == "1"
    assert read.edge_capabilities == {"gateway": "0.4.2", "edge": "1"}


# --------------------------------------------------------------------------- #
# P1: снимок плановых входов прогона (run_inputs)
# --------------------------------------------------------------------------- #


def test_save_read_plan_inputs_roundtrip(store: SqliteStore) -> None:
    """``save_plan_inputs``→``read_plan_inputs`` сохраняет payload и стабильный sha256."""
    import hashlib

    payload = {
        "snapshot_version": 1,
        "run_id": RUN_ID,
        "params": [{"param_id": "p1"}],
        "cohort": {"case_id": "case-1", "frozen_at": "2026-01-01", "entries": []},
        "module_weights": {"m": 1.0},
    }
    sha = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()

    assert store.read_plan_inputs(RUN_ID) is None  # ещё не записано

    store.save_plan_inputs(RUN_ID, payload=payload, payload_sha256=sha)
    assert store.read_plan_inputs(RUN_ID) == payload

    # Повторная запись идентичного снимка: sha256 стабилен, payload тот же.
    store.save_plan_inputs(RUN_ID, payload=payload, payload_sha256=sha)
    row = store.conn.execute(
        "SELECT snapshot_version, payload_sha256 FROM run_inputs WHERE run_id = ?",
        (RUN_ID,),
    ).fetchone()
    assert row["snapshot_version"] == 1
    assert row["payload_sha256"] == sha
    assert store.read_plan_inputs(RUN_ID) == payload
    # Чужой run_id — нет записи.
    assert store.read_plan_inputs("other") is None


# --------------------------------------------------------------------------- #
# v2.1: settings_provenance (A3), persist_manifest (R7/§13), iter_exhaustions (§5)
# --------------------------------------------------------------------------- #


def test_settings_provenance_roundtrip(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    # Новый прогон: пустой провенанс по умолчанию.
    assert store.read_manifest(RUN_ID).settings_provenance == {}

    provenance = {
        "ranking_epsilon": {"source": "measured", "experiment_id": "exp-1"},
        "partial_score": {"source": "default"},
    }
    with store.conn:
        store.conn.execute(
            "UPDATE runs SET settings_provenance_json = ? WHERE run_id = ?",
            (json.dumps(provenance, ensure_ascii=False), RUN_ID),
        )
    manifest = store.read_manifest(RUN_ID)
    assert manifest.settings_provenance == provenance


def test_persist_manifest_roundtrip(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case-1",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="2.1.0",
        package_version="pkg-1.2.3",
        catalog_version="cat-1",
        status_dict_version="sd-1",
        scoring_policy_version="sp-1",
        anchor_product_id="p1",
        index_build_id="ib_x",
        retrieval_profile="R1",
        retrieval_cutoff_K=20,
        requested_ranking=RankingPolicy.CAPABILITY_RANKED,
        computed_ranking=RankingPolicy.COVERAGE_FALLBACK,
        settings_provenance={"ranking_epsilon": {"source": "measured", "experiment_id": "e1"}},
    )
    store.persist_manifest(RUN_ID, manifest)
    read = store.read_manifest(RUN_ID)
    assert read.package_version == "pkg-1.2.3"
    assert read.catalog_version == "cat-1"
    assert read.methodology_version == "2.1.0"
    assert read.index_build_id == "ib_x"
    assert read.retrieval_profile == "R1"
    assert read.retrieval_cutoff_K == 20
    assert read.requested_ranking is RankingPolicy.CAPABILITY_RANKED
    assert read.computed_ranking is RankingPolicy.COVERAGE_FALLBACK
    assert read.settings_provenance == {
        "ranking_epsilon": {"source": "measured", "experiment_id": "e1"}
    }


def test_crawl_provenance_roundtrip(store: SqliteStore) -> None:
    """§4.4/Phase 2: ``persist_manifest``/``read_manifest`` несут провенанс краула."""
    store.create_run(RUN_ID, _cfg())
    # Новый прогон без провенанса → пустой (не ``None``).
    assert store.read_manifest(RUN_ID).crawl_provenance == CrawlProvenance()

    provenance = CrawlProvenance(
        sessions=[
            CrawlSessionProvenance(
                session_id="crawl:run-1",
                profile="local",
                status="partial",
                engines=["http", "scrapy"],
                kinds=["web"],
                total=3,
                fetched=2,
                failed=1,
            )
        ],
        engines=["http", "scrapy"],
        kinds=["web"],
        crawl_sources_digest="digest-1",
    )
    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case-1",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="2.1.0",
        package_version="pkg-1",
        catalog_version="cat-1",
        status_dict_version="sd-1",
        scoring_policy_version="sp-1",
        anchor_product_id="p1",
        crawl_provenance=provenance,
    )
    store.persist_manifest(RUN_ID, manifest)
    read = store.read_manifest(RUN_ID)
    assert read.crawl_provenance == provenance
    assert read.crawl_provenance.sessions[0].engines == ["http", "scrapy"]
    assert read.crawl_provenance.crawl_sources_digest == "digest-1"


def test_read_manifest_crawl_provenance_tolerates_null(store: SqliteStore) -> None:
    """NULL в ``crawl_provenance_json`` → пустой ``CrawlProvenance`` (forward-compat)."""
    store.create_run(RUN_ID, _cfg())
    with store.conn:
        store.conn.execute(
            "UPDATE runs SET crawl_provenance_json = NULL WHERE run_id = ?", (RUN_ID,)
        )
    assert store.read_manifest(RUN_ID).crawl_provenance == CrawlProvenance()


def test_crawl_axes_roundtrip(store: SqliteStore) -> None:
    """Backlog B (§13): ``persist_manifest``/``read_manifest`` несут оси краула."""
    store.create_run(RUN_ID, _cfg())
    # Новый прогон без секции ``crawl`` → ``None`` (граница не заполнена).
    assert store.read_manifest(RUN_ID).crawl_strategy is None
    assert store.read_manifest(RUN_ID).crawl_rate_limit is None

    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case-1",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="2.0.0",
        package_version="pkg-1",
        catalog_version="cat-1",
        status_dict_version="sd-1",
        scoring_policy_version="sp-1",
        anchor_product_id="p1",
        crawl_strategy="hybrid",
        crawl_rate_limit="uniform(min=0.5,max=2.0)",
    )
    store.persist_manifest(RUN_ID, manifest)
    read = store.read_manifest(RUN_ID)
    assert read.crawl_strategy == "hybrid"
    assert read.crawl_rate_limit == "uniform(min=0.5,max=2.0)"


def test_iter_exhaustions(store: SqliteStore) -> None:
    store.create_run(RUN_ID, _cfg())
    pe = ProtocolExhaustion(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        contours=["corpus", "serp", "registry", "inheritance"],
        queries=["q1"],
        rounds=2,
        budget={"serp": 5},
        termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        exhausted_at="2026-01-01T00:00:00Z",
        notes="note",
    )
    store.save_exhaustion(pe)
    # Повреждённая строка (невалидный protocol_json) не должна ронять итерацию.
    with store.conn:
        store.conn.execute(
            "INSERT INTO search_exhaustion(run_id, product_id, param_id, protocol_json)"
            " VALUES (?, 'p1', 'zz', 'not-json')",
            (RUN_ID,),
        )
    items = list(store.iter_exhaustions(RUN_ID))
    assert [e.param_id for e in items] == ["param.a"]
    assert items[0].contours == ["corpus", "serp", "registry", "inheritance"]
    assert items[0].termination_reason is TerminationReason.BUDGET_EXHAUSTED
    assert items[0].budget == {"serp": 5}
    assert items[0].rounds == 2
    # Неизвестный прогон → пустая итерация.
    assert list(store.iter_exhaustions("does-not-exist")) == []
