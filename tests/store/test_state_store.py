"""Тесты ``SqliteStateStore`` (09 §4.2/§5.3): идемпотентные upsert, seed-набор, lineage.

Офлайн: ``:memory:``-БД, без сети.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain.enums import OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import SeedEntry
from core.ports.state import ProductStateRow, RunLineageRow, SnapshotRow
from core.store.state_store import SqliteStateStore


@pytest.fixture()
def store() -> Iterator[SqliteStateStore]:
    with SqliteStateStore(":memory:") as state:
        yield state


def _product(product_id: str = "p1", **overrides: object) -> ProductStateRow:
    base: dict[str, object] = {
        "product_id": product_id,
        "canonical_name": "Product One",
        "onboarding_state": OnboardingState.READY,
        "added_at": "2026-01-01T00:00:00Z",
        "added_by": "test",
    }
    base.update(overrides)
    return ProductStateRow(**base)  # type: ignore[arg-type]


def _snapshot(
    product_id: str = "p1",
    *,
    snapshot_id: str | None = None,
    run_id: str = "run-1",
    scope: str = "full",
    status: str = "complete",
    created_at: str = "2026-01-01T00:00:00Z",
) -> SnapshotRow:
    return SnapshotRow(
        snapshot_id=snapshot_id or f"snap-{run_id}-{scope}",
        product_id=product_id,
        cycle_id="cycle-1",
        run_id=run_id,
        status=status,
        scope=scope,
        coverage_complete=status == "complete",
        created_at=created_at,
    )


def _seed(url: str = "https://p1.test/", **overrides: object) -> SeedEntry:
    base: dict[str, object] = {
        "url": url,
        "normalized_url": url,
        "role": SeedRole.PRIMARY,
        "source": SeedSource.OPERATOR,
        "profile_key": "p1.test",
        "added_at": _dt.date(2026, 1, 1),
        "added_by": "operator",
    }
    base.update(overrides)
    return SeedEntry(**base)  # type: ignore[arg-type]


def _lineage(run_id: str, *, started_at: str, status: str = "running") -> RunLineageRow:
    return RunLineageRow(
        run_id=run_id,
        package_id="case_x",
        cycle_id="cycle-1",
        cohort_digest="cohort",
        seeds_digest="seeds",
        execution_contract_digest="contract",
        change_class="execution_change",
        status=status,
        started_at=started_at,
    )


# ---------------------------------------------------------------------------
# product_state
# ---------------------------------------------------------------------------


def test_product_state_upsert_is_idempotent(store: SqliteStateStore) -> None:
    store.upsert_product_state(_product(last_corpus_run_id="run-1"))
    store.upsert_product_state(_product(last_corpus_run_id="run-1"))
    count = store.conn.execute("SELECT COUNT(*) AS n FROM product_state").fetchone()["n"]
    assert count == 1

    store.upsert_product_state(_product(last_corpus_run_id="run-2"))
    row = store.get_product_state("p1")
    assert row is not None
    assert row.last_corpus_run_id == "run-2"
    assert row.onboarding_state is OnboardingState.READY


def test_get_missing_product_state_is_none(store: SqliteStateStore) -> None:
    assert store.get_product_state("absent") is None


# ---------------------------------------------------------------------------
# seeds
# ---------------------------------------------------------------------------


def test_reconcile_seed_set_upserts(store: SqliteStateStore) -> None:
    store.reconcile_seed_set("p1", [_seed(), _seed("https://docs.p1.test/")])
    rows = store.seed_rows("p1")
    assert {row.normalized_url for row in rows} == {
        "https://p1.test/",
        "https://docs.p1.test/",
    }
    assert all(row.state is SeedState.ENABLED for row in rows)

    # повторная синхронизация не дублирует строки
    store.reconcile_seed_set("p1", [_seed()])
    assert len(store.seed_rows("p1")) == 2


def test_reconcile_seed_set_disables_removed_without_deleting(
    store: SqliteStateStore,
) -> None:
    store.reconcile_seed_set("p1", [_seed(), _seed("https://docs.p1.test/")])
    store.reconcile_seed_set("p1", [_seed()])

    rows = {row.normalized_url: row for row in store.seed_rows("p1")}
    assert set(rows) == {"https://p1.test/", "https://docs.p1.test/"}
    removed = rows["https://docs.p1.test/"]
    assert removed.state is SeedState.DISABLED
    assert removed.retired_at is not None
    assert rows["https://p1.test/"].state is SeedState.ENABLED


def test_reconcile_seed_set_revives_returned_seed(store: SqliteStateStore) -> None:
    store.reconcile_seed_set("p1", [_seed(), _seed("https://docs.p1.test/")])
    store.reconcile_seed_set("p1", [_seed()])
    store.reconcile_seed_set("p1", [_seed(), _seed("https://docs.p1.test/")])

    rows = {row.normalized_url: row for row in store.seed_rows("p1")}
    revived = rows["https://docs.p1.test/"]
    assert revived.state is SeedState.ENABLED
    assert revived.retired_at is None


def test_reconcile_seed_set_empty_disables_all(store: SqliteStateStore) -> None:
    store.reconcile_seed_set("p1", [_seed()])
    store.reconcile_seed_set("p1", [])
    rows = store.seed_rows("p1")
    assert len(rows) == 1
    assert rows[0].state is SeedState.DISABLED


def test_seed_rows_are_product_scoped(store: SqliteStateStore) -> None:
    store.reconcile_seed_set("p1", [_seed()])
    store.reconcile_seed_set("p2", [_seed("https://p2.test/")])
    assert [row.normalized_url for row in store.seed_rows("p2")] == [
        "https://p2.test/"
    ]


# ---------------------------------------------------------------------------
# snapshots
# ---------------------------------------------------------------------------


def test_last_snapshot_scope_full_ignores_product_only(
    store: SqliteStateStore,
) -> None:
    store.record_snapshot(
        _snapshot(run_id="run-full", created_at="2026-01-01T00:00:00Z")
    )
    store.record_snapshot(
        _snapshot(
            snapshot_id="snap-only",
            run_id="run-only",
            scope="product_only",
            created_at="2026-02-01T00:00:00Z",
        )
    )

    full = store.last_snapshot("p1", scope="full")
    assert full is not None and full.run_id == "run-full"
    only = store.last_snapshot("p1", scope="product_only")
    assert only is not None and only.run_id == "run-only"


def test_last_snapshot_is_latest_and_idempotent(store: SqliteStateStore) -> None:
    store.record_snapshot(_snapshot(run_id="run-1", created_at="2026-01-01T00:00:00Z"))
    store.record_snapshot(_snapshot(run_id="run-2", created_at="2026-02-01T00:00:00Z"))
    store.record_snapshot(_snapshot(run_id="run-2", created_at="2026-02-01T00:00:00Z"))
    latest = store.last_snapshot("p1")
    assert latest is not None and latest.run_id == "run-2"
    count = store.conn.execute(
        "SELECT COUNT(*) AS n FROM product_snapshot"
    ).fetchone()["n"]
    assert count == 2


def test_last_snapshot_missing_is_none(store: SqliteStateStore) -> None:
    assert store.last_snapshot("absent") is None


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------


def test_previous_run_returns_latest_for_package(store: SqliteStateStore) -> None:
    assert store.previous_run("case_x") is None
    store.record_run_lineage(
        _lineage("run-1", started_at="2026-01-01T00:00:00Z", status="complete")
    )
    store.record_run_lineage(_lineage("run-2", started_at="2026-02-01T00:00:00Z"))
    assert store.previous_run("case_x") == "run-2"
    assert store.previous_run("other_case") is None


def test_record_run_lineage_upserts(store: SqliteStateStore) -> None:
    store.record_run_lineage(_lineage("run-1", started_at="2026-01-01T00:00:00Z"))
    store.record_run_lineage(
        _lineage("run-1", started_at="2026-01-01T00:00:00Z", status="complete")
    )
    row = store.conn.execute(
        "SELECT status FROM run_lineage WHERE run_id = 'run-1'"
    ).fetchone()
    assert row["status"] == "complete"
    assert (
        store.conn.execute("SELECT COUNT(*) AS n FROM run_lineage").fetchone()["n"] == 1
    )
