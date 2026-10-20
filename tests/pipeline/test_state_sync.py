"""Тесты writer-стороны durable state (09 §4.4/§5.1): готовность, снимки, lineage.

Проверяют, что ``state.db`` не инертен: ``sync_product_state`` материализует готовность
продукта и не затирает рубежи, ``record_run_outcome`` пишет снимок/lineage по реальным
данным прогона, ``_coverage_digest`` стабилен относительно ``computed_at``.

Офлайн: ``:memory:``-state + ``:memory:``-store прогона, без сети.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain.enums import (
    CellState,
    OnboardingState,
    ProductMode,
    ProductScope,
    SeedRole,
    SeedSource,
    Status,
)
from core.domain.models import (
    Cell,
    CorpusCoverage,
    Focus,
    ProductPlan,
    ProductSeeds,
    RankingPolicy,
    RunConfig,
    RunMode,
    SeedEntry,
    SeedsConfig,
)
from core.pipeline.state_sync import (
    _coverage_digest,
    record_run_outcome,
    sync_product_state,
)
from core.ports.state import DocumentVersionRow
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore

RUN_ID = "run-1"
PACKAGE_ID = "case_x"
CYCLE_ID = "2026-06-01"
STARTED = "2026-06-01T10:00:00Z"
COMPLETED = "2026-06-01T10:30:00Z"


@pytest.fixture()
def state() -> Iterator[SqliteStateStore]:
    with SqliteStateStore(":memory:") as store:
        yield store


@pytest.fixture()
def run_store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as store:
        store.create_run(RUN_ID, _run_config())
        yield store


def _run_config() -> RunConfig:
    """Минимальная конфигурация прогона для строки ``runs`` (FK ячеек)."""
    return RunConfig(
        case_id="case_x",
        anchor="p1",
        cohort="cohort_case_x",
        products=["p1"],
        package=PACKAGE_ID,
        focus=Focus(modules=["mod_a"], params=["param.a"]),
        languages=["ru", "en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _seed(product_id: str = "p1") -> SeedEntry:
    host = f"{product_id}.test"
    return SeedEntry(
        url=f"https://{host}/",
        normalized_url=f"https://{host}/",
        role=SeedRole.PRIMARY,
        source=SeedSource.OPERATOR,
        profile_key=host,
        added_at=_dt.date(2026, 1, 1),
        added_by="operator",
    )


def _seeds(*product_ids: str) -> SeedsConfig:
    return SeedsConfig(
        products={
            product_id: ProductSeeds(
                state=OnboardingState.READY, seeds=[_seed(product_id)]
            )
            for product_id in product_ids
        }
    )


def _plan(
    product_id: str = "p1",
    *,
    onboarding_state: OnboardingState = OnboardingState.READY,
    reasons: list[str] | None = None,
) -> ProductPlan:
    return ProductPlan(
        product_id=product_id,
        mode=ProductMode.NEW,
        onboarding_state=onboarding_state,
        seed_count=1 if onboarding_state is OnboardingState.READY else 0,
        reasons=list(reasons or []),
    )


def _coverage(
    product_id: str = "p1",
    *,
    complete: bool = True,
    computed_at: str = "2026-06-01T10:05:00Z",
    reason: str | None = None,
) -> CorpusCoverage:
    return CorpusCoverage(
        product_id=product_id,
        complete=complete,
        expected_sources=2,
        observed_sources=2 if complete else 1,
        truncated=not complete,
        computed_at=computed_at,
        reason=reason,
    )


def _save_cells(store: SqliteStore, cells: list[Cell]) -> None:
    """Пишет ячейки прогона по одной (``save_cell`` — upsert по PK)."""
    for cell in cells:
        store.save_cell(cell)


def _cell(product_id: str, param_id: str, state_: CellState) -> Cell:
    """Ячейка без evidence: ``evaluated`` без статуса допустим (❌ требует absence)."""
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=state_,
        status=Status.ABSENT if state_ is CellState.EVALUATED else None,
        absence_evidence=(
            [{"source_scope": "exhaustive"}] if state_ is CellState.EVALUATED else []
        ),
    )


def _run_state(
    state: SqliteStateStore,
    run_store: SqliteStore,
    *,
    products: tuple[str, ...] = ("p1",),
    status: str = "ok",
    evaluate_ok: bool = True,
    scope: str = ProductScope.FULL.value,
    completed_at: str | None = COMPLETED,
) -> None:
    record_run_outcome(
        state,
        run_id=RUN_ID,
        package_id=PACKAGE_ID,
        cycle_id=CYCLE_ID,
        products=products,
        store=run_store,
        status=status,
        started_at=STARTED,
        completed_at=completed_at,
        scope=scope,
        evaluate_ok=evaluate_ok,
    )


# ---------------------------------------------------------------------------
# sync_product_state
# ---------------------------------------------------------------------------


def test_sync_creates_ready_row(state: SqliteStateStore) -> None:
    sync_product_state(
        state,
        products=["p1"],
        onboarding_plans=[_plan()],
        seeds=_seeds("p1"),
        today=_dt.date(2026, 6, 1),
    )
    row = state.get_product_state("p1")
    assert row is not None
    assert row.onboarding_state is OnboardingState.READY
    assert row.canonical_name == "p1"
    assert row.added_at == "2026-06-01"
    assert row.added_by == "run"
    assert row.onboarding_reason is None


def test_sync_preserves_run_milestones_on_second_call(
    state: SqliteStateStore,
) -> None:
    sync_product_state(
        state, products=["p1"], onboarding_plans=[_plan()], seeds=_seeds("p1")
    )
    row = state.get_product_state("p1")
    assert row is not None
    state.upsert_product_state(
        type(row)(
            **{
                **row.__dict__,
                "last_corpus_run_id": "run-9",
                "last_evaluated_run_id": "run-9",
                "last_completed_run_id": "run-9",
                "added_at": "2025-01-01",
            }
        )
    )

    sync_product_state(
        state, products=["p1"], onboarding_plans=[_plan()], seeds=_seeds("p1")
    )
    again = state.get_product_state("p1")
    assert again is not None
    assert again.last_corpus_run_id == "run-9"
    assert again.last_evaluated_run_id == "run-9"
    assert again.last_completed_run_id == "run-9"
    assert again.added_at == "2025-01-01"  # провенанс первого появления не перезаписан


def test_sync_blocked_product_writes_pending_and_reason(
    state: SqliteStateStore,
) -> None:
    sync_product_state(
        state,
        products=["p2"],
        onboarding_plans=[
            _plan(
                "p2",
                onboarding_state=OnboardingState.PENDING_SEEDS,
                reasons=["no_seeds", "profile_unbound:https://p2.test/"],
            )
        ],
        seeds=_seeds(),
    )
    row = state.get_product_state("p2")
    assert row is not None
    assert row.onboarding_state is OnboardingState.PENDING_SEEDS
    assert row.onboarding_reason == "no_seeds"


def test_sync_blocked_reason_falls_back_to_no_seeds(
    state: SqliteStateStore,
) -> None:
    sync_product_state(
        state,
        products=["p3"],
        onboarding_plans=[_plan("p3", onboarding_state=OnboardingState.PENDING_SEEDS)],
        seeds=_seeds(),
    )
    row = state.get_product_state("p3")
    assert row is not None and row.onboarding_reason == "no_seeds"


def test_sync_is_idempotent(state: SqliteStateStore) -> None:
    for _ in range(3):
        sync_product_state(
            state, products=["p1"], onboarding_plans=[_plan()], seeds=_seeds("p1")
        )
    assert (
        state.conn.execute("SELECT COUNT(*) AS n FROM product_state").fetchone()["n"]
        == 1
    )


def test_sync_ignores_products_outside_plans(state: SqliteStateStore) -> None:
    sync_product_state(
        state, products=["p1", "ghost"], onboarding_plans=[_plan()], seeds=_seeds("p1")
    )
    assert state.get_product_state("ghost") is None


# ---------------------------------------------------------------------------
# record_run_outcome: снимки
# ---------------------------------------------------------------------------


def _snapshot_status(state: SqliteStateStore, product_id: str = "p1") -> str:
    snap = state.last_snapshot(product_id, scope=ProductScope.FULL.value)
    assert snap is not None
    return snap.status


def test_snapshot_complete_when_coverage_complete(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _run_state(state, run_store)
    snap = state.last_snapshot("p1")
    assert snap is not None
    assert snap.status == "complete"
    assert snap.coverage_complete is True
    assert snap.docs_total == 2
    assert snap.coverage_digest == _coverage_digest(_coverage())


def test_snapshot_partial_when_coverage_incomplete(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage(complete=False))
    _run_state(state, run_store)
    snap = state.last_snapshot("p1")
    assert snap is not None
    assert snap.status == "partial"
    assert snap.coverage_complete is False


def test_snapshot_failed_without_coverage(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    _run_state(state, run_store)
    snap = state.last_snapshot("p1")
    assert snap is not None
    assert snap.status == "failed"
    assert snap.coverage_digest is None
    assert snap.docs_total == 0


def test_snapshot_counters_from_cells(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _save_cells(
        run_store,
        [
            _cell("p1", "param.a", CellState.EVALUATED),
            _cell("p1", "param.b", CellState.PENDING),
        ],
    )
    _run_state(state, run_store)
    snap = state.last_snapshot("p1")
    assert snap is not None
    assert (snap.cells_total, snap.cells_measured, snap.cells_pending) == (2, 1, 1)
    assert snap.cells_carried == 0  # carry-forward не реализован — не выдумываем


# ---------------------------------------------------------------------------
# record_run_outcome: рубежи product_state
# ---------------------------------------------------------------------------


def test_milestones_corpus_evaluated_completed(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _save_cells(
        run_store,
        [
            _cell("p1", "param.a", CellState.EVALUATED),
            _cell("p1", "param.b", CellState.REVIEWED),
        ],
    )
    _run_state(state, run_store)
    row = state.get_product_state("p1")
    assert row is not None
    assert row.last_corpus_run_id == RUN_ID
    assert row.last_evaluated_run_id == RUN_ID
    assert row.last_completed_run_id == RUN_ID
    assert row.last_completed_cycle_id == CYCLE_ID
    assert row.last_attempt_status == "ok"


def test_evaluated_without_pending_free_cells_is_not_completed(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _save_cells(
        run_store,
        [
            _cell("p1", "param.a", CellState.EVALUATED),
            _cell("p1", "param.b", CellState.PENDING),
        ],
    )
    _run_state(state, run_store)
    row = state.get_product_state("p1")
    assert row is not None
    assert row.last_evaluated_run_id == RUN_ID
    assert row.last_completed_run_id is None


def test_no_cells_means_no_completed_milestone(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _run_state(state, run_store)
    row = state.get_product_state("p1")
    assert row is not None
    assert row.last_corpus_run_id == RUN_ID
    assert row.last_completed_run_id is None


def test_evaluate_not_ok_leaves_evaluated_milestone(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _save_cells(run_store, [_cell("p1", "param.a", CellState.EVALUATED)])
    _run_state(state, run_store, evaluate_ok=False, status="blocked")
    row = state.get_product_state("p1")
    assert row is not None
    assert row.last_corpus_run_id == RUN_ID
    assert row.last_evaluated_run_id is None
    assert row.last_completed_run_id is None


def test_product_only_scope_does_not_move_completed(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _save_cells(run_store, [_cell("p1", "param.a", CellState.EVALUATED)])
    _run_state(state, run_store, scope=ProductScope.PRODUCT_ONLY.value)
    row = state.get_product_state("p1")
    assert row is not None
    assert row.last_evaluated_run_id == RUN_ID
    assert row.last_completed_run_id is None
    snap = state.last_snapshot("p1", scope=ProductScope.PRODUCT_ONLY.value)
    assert snap is not None and snap.baseline_complete is False


def test_completed_row_makes_product_incremental(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    """Сквозная проверка смысла: после полного прогона продукт не ``new``."""
    from core.pipeline.onboarding import resolve_product_modes

    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _save_cells(run_store, [_cell("p1", "param.a", CellState.EVALUATED)])
    _run_state(state, run_store)
    plan = resolve_product_modes(
        ["p1"], _seeds("p1"), profiles={"p1.test": {}}, state=state
    ).plan_set.plan_for("p1")
    assert plan is not None
    assert plan.mode is ProductMode.INCREMENTAL


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------


def test_lineage_written_once_and_previous_run(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    assert state.previous_run(PACKAGE_ID) is None
    _run_state(state, run_store)

    rows = state.conn.execute("SELECT * FROM run_lineage").fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "complete"
    assert rows[0]["completed_at"] == COMPLETED
    assert rows[0]["previous_run_id"] is None
    assert state.previous_run(PACKAGE_ID) == RUN_ID

    # повторная запись того же прогона не дублирует строку
    _run_state(state, run_store)
    assert (
        state.conn.execute("SELECT COUNT(*) AS n FROM run_lineage").fetchone()["n"] == 1
    )


def test_lineage_status_mapping(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    _run_state(state, run_store, status="blocked")
    row = state.conn.execute("SELECT status FROM run_lineage").fetchone()
    assert row["status"] == "partial"


def test_previous_run_is_chained_explicitly(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    record_run_outcome(
        state,
        run_id="run-0",
        package_id=PACKAGE_ID,
        cycle_id=CYCLE_ID,
        products=["p1"],
        store=run_store,
        status="ok",
        started_at="2026-05-01T00:00:00Z",
        completed_at="2026-05-01T01:00:00Z",
    )
    record_run_outcome(
        state,
        run_id=RUN_ID,
        package_id=PACKAGE_ID,
        cycle_id=CYCLE_ID,
        products=["p1"],
        store=run_store,
        status="ok",
        started_at=STARTED,
        completed_at=COMPLETED,
        previous_run_id="run-0",
    )
    row = state.conn.execute(
        "SELECT previous_run_id FROM run_lineage WHERE run_id = ?", (RUN_ID,)
    ).fetchone()
    assert row["previous_run_id"] == "run-0"
    assert state.previous_run(PACKAGE_ID) == RUN_ID


# ---------------------------------------------------------------------------
# _coverage_digest
# ---------------------------------------------------------------------------


def test_coverage_digest_ignores_computed_at() -> None:
    first = _coverage(computed_at="2026-06-01T10:00:00Z")
    second = _coverage(computed_at="2027-01-01T00:00:00Z")
    assert _coverage_digest(first) == _coverage_digest(second)


def test_coverage_digest_tracks_semantics() -> None:
    complete = _coverage()
    partial = _coverage(complete=False)
    assert _coverage_digest(complete) != _coverage_digest(partial)
    assert _coverage_digest(complete) != _coverage_digest(
        _coverage(reason="coverage_missing")
    )
    assert _coverage_digest(None) is None


def test_coverage_digest_is_stable_string() -> None:
    digest = _coverage_digest(_coverage())
    assert isinstance(digest, str)
    assert len(digest) == 24


# ---------------------------------------------------------------------------
# record_run_outcome: счётчики документов из durable doc-set (09 §6.3)
# ---------------------------------------------------------------------------


def _doc_version(
    dv_id: str,
    *,
    content_hash: str,
    first_seen_run_id: str,
    last_seen_run_id: str,
    status: str = "active",
) -> DocumentVersionRow:
    """Строка ``document_version`` для проверки счётчиков (ключ §4.2 — уникален)."""
    return DocumentVersionRow(
        doc_version_id=dv_id,
        doc_id=f"p1:{content_hash}",
        product_id="p1",
        url=f"https://p1.test/{content_hash}",
        content_hash=content_hash,
        extractor_version="ext-1",
        level="full",
        first_seen_run_id=first_seen_run_id,
        last_seen_run_id=last_seen_run_id,
        first_seen_at=STARTED,
        last_seen_at=COMPLETED,
        status=status,
    )


def test_snapshot_doc_counters_from_durable_doc_set(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    """``docs_total`` — размер active doc-set; new/reused — по run-рубежам версии."""
    state.upsert_document_version(
        _doc_version("dv_new", content_hash="h-new", first_seen_run_id=RUN_ID,
                     last_seen_run_id=RUN_ID)
    )
    state.upsert_document_version(
        _doc_version("dv_reused", content_hash="h-old", first_seen_run_id="run-0",
                     last_seen_run_id=RUN_ID)
    )
    # Не наблюдался этим прогоном — в doc-set входит, в new/reused нет.
    state.upsert_document_version(
        _doc_version("dv_idle", content_hash="h-idle", first_seen_run_id="run-0",
                     last_seen_run_id="run-0")
    )
    # Вытесненная версия — вне doc-set (§4.2: только ``active``).
    state.upsert_document_version(
        _doc_version("dv_gone", content_hash="h-gone", first_seen_run_id=RUN_ID,
                     last_seen_run_id=RUN_ID, status="superseded")
    )
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _run_state(state, run_store)

    snap = state.last_snapshot("p1")
    assert snap is not None
    assert (snap.docs_total, snap.docs_new, snap.docs_reused) == (3, 1, 1)
    # Счётчик покрытия — другой смысл (наблюдённые источники), он не подменяется.
    assert _coverage().observed_sources == 2


def test_snapshot_doc_counters_legacy_without_substrate(
    state: SqliteStateStore, run_store: SqliteStore
) -> None:
    """Без doc-set счётчики не фабрикуются: ``docs_total = observed_sources``, new/reused = 0."""
    run_store.save_corpus_coverage(RUN_ID, _coverage())
    _run_state(state, run_store)
    snap = state.last_snapshot("p1")
    assert snap is not None
    assert (snap.docs_total, snap.docs_new, snap.docs_reused) == (2, 0, 0)
