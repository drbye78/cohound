"""Тесты резолва режима продукта по durable-состоянию (09 §5.2/§5.3).

Матрица §5.2: новый продукт; corpus без eval → ``full``; eval + полный complete-снимок →
``incremental``; неполный снимок → ``full``; re-add после ``retired`` → ``new``;
отсутствие state-строки при наличии seed'ов → ``new`` (и **не** ``no_seeds``);
заблокированный продукт сохраняет причины и ``mode=new``.

Офлайн: ``:memory:``-state, без сети.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain.enums import (
    OnboardingState,
    ProductMode,
    RefreshPolicy,
    SeedRole,
    SeedSource,
)
from core.domain.models import ProductSeeds, SeedEntry, SeedsConfig
from core.pipeline.onboarding import resolve_product_modes
from core.ports.state import ProductStateRow, SnapshotRow
from core.store.state_store import SqliteStateStore

PROFILES = {"p1.test": {}, "p2.test": {}}


@pytest.fixture()
def state() -> Iterator[SqliteStateStore]:
    with SqliteStateStore(":memory:") as store:
        yield store


def _seed(product_id: str) -> SeedEntry:
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
    status: str = "complete",
    scope: str = "full",
    run_id: str = "run-1",
) -> SnapshotRow:
    return SnapshotRow(
        snapshot_id=f"snap-{product_id}-{run_id}-{scope}",
        product_id=product_id,
        cycle_id="cycle-1",
        run_id=run_id,
        status=status,
        scope=scope,
        coverage_complete=status == "complete",
        created_at="2026-01-01T00:00:00Z",
    )


def _plan(state: SqliteStateStore | None, product_id: str = "p1"):
    result = resolve_product_modes(
        [product_id],
        _seeds(product_id),
        profiles=PROFILES,
        state=state,
    )
    plan = result.plan_set.plan_for(product_id)
    assert plan is not None
    return plan


def test_new_product_without_state_row(state: SqliteStateStore) -> None:
    plan = _plan(state)
    assert plan.mode is ProductMode.NEW
    assert plan.reasons == []
    assert plan.onboarding_state is OnboardingState.READY
    assert plan.refresh_policy is RefreshPolicy.REFRESH
    assert plan.last_corpus_run_id is None


def test_missing_state_row_is_not_no_seeds(state: SqliteStateStore) -> None:
    """Отсутствие state-строки — норма первого прогона, а не отсутствие seed'ов."""
    plan = _plan(state)
    assert "no_seeds" not in plan.reasons
    assert plan.seed_count == 1


def test_state_none_keeps_mvp1_behaviour() -> None:
    plan = _plan(None)
    assert plan.mode is ProductMode.NEW
    assert plan.refresh_policy is RefreshPolicy.REFRESH


def test_corpus_without_eval_is_full(state: SqliteStateStore) -> None:
    state.upsert_product_state(_product(last_corpus_run_id="run-1"))
    plan = _plan(state)
    assert plan.mode is ProductMode.FULL
    assert plan.refresh_policy is RefreshPolicy.REFRESH
    assert plan.last_corpus_run_id == "run-1"
    assert plan.last_evaluated_run_id is None


def test_evaluated_with_complete_snapshot_is_incremental(
    state: SqliteStateStore,
) -> None:
    state.upsert_product_state(
        _product(last_corpus_run_id="run-1", last_evaluated_run_id="run-1")
    )
    state.record_snapshot(_snapshot())
    plan = _plan(state)
    assert plan.mode is ProductMode.INCREMENTAL
    assert plan.refresh_policy is RefreshPolicy.REVALIDATE
    assert plan.last_evaluated_run_id == "run-1"


def test_incomplete_snapshot_is_full(state: SqliteStateStore) -> None:
    state.upsert_product_state(
        _product(last_corpus_run_id="run-1", last_evaluated_run_id="run-1")
    )
    state.record_snapshot(_snapshot(status="partial"))
    assert _plan(state).mode is ProductMode.FULL


def test_missing_snapshot_is_full(state: SqliteStateStore) -> None:
    state.upsert_product_state(
        _product(last_corpus_run_id="run-1", last_evaluated_run_id="run-1")
    )
    assert _plan(state).mode is ProductMode.FULL


def test_product_only_snapshot_does_not_make_incremental(
    state: SqliteStateStore,
) -> None:
    """Режим определяется **полным** baseline, а не scoped-снимком (§5.1)."""
    state.upsert_product_state(
        _product(last_corpus_run_id="run-1", last_evaluated_run_id="run-1")
    )
    state.record_snapshot(_snapshot(scope="product_only"))
    assert _plan(state).mode is ProductMode.FULL


def test_readded_after_retired_is_new(state: SqliteStateStore) -> None:
    state.upsert_product_state(
        _product(
            last_corpus_run_id="run-1",
            last_evaluated_run_id="run-1",
            re_added_at="2026-03-01T00:00:00Z",
            retired_at="2026-02-01T00:00:00Z",
        )
    )
    state.record_snapshot(_snapshot())
    plan = _plan(state)
    assert plan.mode is ProductMode.NEW
    assert plan.refresh_policy is RefreshPolicy.REFRESH


def test_blocked_product_keeps_reasons_and_new_mode(
    state: SqliteStateStore,
) -> None:
    config = SeedsConfig(
        products={
            "p1": ProductSeeds(state=OnboardingState.READY, seeds=[_seed("p1")]),
            "p2": ProductSeeds(state=OnboardingState.PENDING_SEEDS, seeds=[]),
        }
    )
    state.upsert_product_state(
        _product("p2", last_corpus_run_id="run-1", last_evaluated_run_id="run-1")
    )
    result = resolve_product_modes(
        ["p1", "p2"], config, profiles=PROFILES, state=state
    )
    assert result.blocked == ["p2"]
    plan = result.plan_set.plan_for("p2")
    assert plan is not None
    assert plan.mode is ProductMode.NEW
    assert plan.reasons == ["no_seeds"]
    assert plan.refresh_policy is RefreshPolicy.REVALIDATE
    assert result.effective_urls["p2"] == []


def test_cycle_and_lineage_metadata_are_carried(state: SqliteStateStore) -> None:
    result = resolve_product_modes(
        ["p1"],
        _seeds("p1"),
        profiles=PROFILES,
        cycle_id="cycle-1",
        previous_run_id="run-0",
        change_class="execution_change",
        state=state,
    )
    assert result.cycle_id == "cycle-1"
    assert result.previous_run_id == "run-0"
    assert result.change_class == "execution_change"
    plan_set = result.plan_set
    assert plan_set.cycle_id == "cycle-1"
    assert plan_set.previous_run_id == "run-0"
