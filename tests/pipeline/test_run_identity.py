"""Тесты идентичности прогона (09 §2.4/§6.2/§12.3): цикл, scope, refresh, change_class.

Офлайн: только доменные модели и чистые функции ``core.pipeline.run_identity``.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import replace

import pytest

from core.domain.enums import (
    AnchorRelation,
    Archetype,
    ChangeClass,
    RefreshPolicy,
    RunMode,
    RunScope,
)
from core.domain.models import (
    Cohort,
    CohortEntry,
    ModuleDef,
    PackageManifest,
    RunConfig,
)
from core.pipeline.normalize import RunInputs
from core.pipeline.plan import derive_run_id
from core.pipeline.run_identity import (
    RunIdentity,
    build_execution_contract,
    build_run_identity,
    classify_change,
    golden_required_for,
    resolve_cycle_id,
    resolve_refresh_policy,
    resolve_run_scope,
)


def _run_config(**overrides: object) -> RunConfig:
    base: dict[str, object] = {
        "case_id": "case_x",
        "anchor": "p1",
        "cohort": "cohort_case_x",
        "products": ["p1"],
        "package": "case_x",
        "snapshot_date": _dt.date(2026, 6, 1),
        "mode": RunMode.STRICT,
    }
    base.update(overrides)
    return RunConfig(**base)  # type: ignore[arg-type]


def _inputs() -> RunInputs:
    cohort = Cohort(
        case_id="case_x",
        frozen_at=_dt.date(2026, 6, 1),
        entries=[
            CohortEntry(
                product_id="p1",
                role_overlap=1.0,
                selection_reason="якорь",
                anchor_relation=AnchorRelation.ANCHOR,
            )
        ],
    )
    package = PackageManifest(
        case_id="case_x",
        package_version="1.2.3",
        archetype=Archetype.A,
        modules=[ModuleDef(module_id="mod_a", weight=100.0)],
        roles=[],
    )
    return RunInputs(
        run=_run_config(),
        package=package,
        catalog=[],
        baselines=[],
        cohort=cohort,
        golden=[],
    )


# ---------------------------------------------------------------------------
# cycle_id
# ---------------------------------------------------------------------------


def test_cycle_without_update_prefers_snapshot_date() -> None:
    config = _run_config()
    assert (
        resolve_cycle_id(
            cli_cycle_id=None,
            run_config=config,
            update=False,
            today=_dt.date(2026, 10, 20),
        )
        == "2026-06-01"
    )


def test_cycle_without_update_prefers_run_yaml_cycle_id_and_cli() -> None:
    config = _run_config(cycle_id="2026-W40")
    assert (
        resolve_cycle_id(
            cli_cycle_id=None,
            run_config=config,
            update=False,
            today=_dt.date(2026, 10, 20),
        )
        == "2026-W40"
    )
    assert (
        resolve_cycle_id(
            cli_cycle_id="2026-W41",
            run_config=config,
            update=False,
            today=_dt.date(2026, 10, 20),
        )
        == "2026-W41"
    )


def test_update_with_explicit_new_cycle_id() -> None:
    assert (
        resolve_cycle_id(
            cli_cycle_id="2026-W43",
            run_config=_run_config(),
            update=True,
            today=_dt.date(2026, 10, 20),
            previous_cycle_id="2026-06-01",
        )
        == "2026-W43"
    )


def test_update_without_explicit_cycle_uses_today() -> None:
    assert (
        resolve_cycle_id(
            cli_cycle_id=None,
            run_config=_run_config(),
            update=True,
            today=_dt.date(2026, 10, 20),
            previous_cycle_id="2026-06-01",
        )
        == "2026-10-20"
    )


def test_update_rejects_previous_cycle_id() -> None:
    with pytest.raises(ValueError, match="--update требует нового cycle_id"):
        resolve_cycle_id(
            cli_cycle_id="2026-06-01",
            run_config=_run_config(),
            update=True,
            today=_dt.date(2026, 10, 20),
            previous_cycle_id="2026-06-01",
        )


def test_update_rejects_today_when_it_equals_previous() -> None:
    with pytest.raises(ValueError, match="--update требует нового cycle_id"):
        resolve_cycle_id(
            cli_cycle_id=None,
            run_config=_run_config(),
            update=True,
            today=_dt.date(2026, 10, 20),
            previous_cycle_id="2026-10-20",
        )


# ---------------------------------------------------------------------------
# run_scope / refresh_policy
# ---------------------------------------------------------------------------


def test_run_scope_matrix() -> None:
    assert resolve_run_scope(only_failed=False, only_product=None) == RunScope.FULL.value
    assert (
        resolve_run_scope(only_failed=True, only_product=None)
        == RunScope.ONLY_FAILED.value
    )
    assert resolve_run_scope(only_failed=False, only_product="p1") == "product:p1"


def test_run_scope_rejects_conflicting_filters() -> None:
    with pytest.raises(ValueError, match="взаимоисключающие"):
        resolve_run_scope(only_failed=True, only_product="p1")


def test_refresh_policy_precedence() -> None:
    config = _run_config()
    assert resolve_refresh_policy(cli=None, run_config=config) is RefreshPolicy.AUTO
    assert (
        resolve_refresh_policy(cli="offline", run_config=config)
        is RefreshPolicy.OFFLINE
    )
    configured = _run_config(refresh_policy=RefreshPolicy.REVALIDATE)
    assert (
        resolve_refresh_policy(cli=None, run_config=configured)
        is RefreshPolicy.REVALIDATE
    )
    assert (
        resolve_refresh_policy(cli="refresh", run_config=configured)
        is RefreshPolicy.REFRESH
    )


# ---------------------------------------------------------------------------
# execution contract
# ---------------------------------------------------------------------------


def test_execution_contract_uses_available_values_and_none_for_later_wiring() -> None:
    inputs = _inputs()
    first = build_execution_contract(
        inputs,
        cohort_digest="cohort",
        seeds_digest="seeds",
        retrieval_profile="R2",
        cutoff_K=10,
    )
    second = build_execution_contract(
        inputs,
        cohort_digest="cohort",
        seeds_digest="seeds",
        retrieval_profile="R2",
        cutoff_K=10,
    )
    assert first == second
    assert len(first) == 24

    # Смена честно доступного поля меняет контракт.
    changed = build_execution_contract(
        inputs,
        cohort_digest="cohort",
        seeds_digest="seeds",
        retrieval_profile="R1",
        cutoff_K=10,
    )
    assert changed != first


# ---------------------------------------------------------------------------
# change_class / RunIdentity
# ---------------------------------------------------------------------------


def test_change_class_matrix() -> None:
    assert (
        classify_change(
            previous_run_id=None,
            previous_products=["p1"],
            current_products=["p1", "p2"],
            previous_execution_contract_digest="old",
            execution_contract_digest="new",
        )
        is ChangeClass.NONE
    )
    assert (
        classify_change(
            previous_run_id="run-1",
            previous_products=["p1"],
            current_products=["p1", "p2"],
            previous_execution_contract_digest="old",
            execution_contract_digest="new",
        )
        is ChangeClass.COHORT_EXPANSION
    )
    assert (
        classify_change(
            previous_run_id="run-1",
            previous_products=["p1", "p2"],
            current_products=["p1"],
            previous_execution_contract_digest="old",
            execution_contract_digest="new",
        )
        is ChangeClass.COHORT_REDUCTION
    )
    assert (
        classify_change(
            previous_run_id="run-1",
            previous_products=["p1"],
            current_products=["p1"],
            previous_execution_contract_digest="old",
            execution_contract_digest="new",
        )
        is ChangeClass.EXECUTION_CHANGE
    )
    assert (
        classify_change(
            previous_run_id="run-1",
            previous_products=["p1"],
            current_products=["p1"],
            previous_execution_contract_digest="same",
            execution_contract_digest="same",
        )
        is ChangeClass.NONE
    )
    # Legacy-строка без products_json: состав неизвестен, сравнение пропускается.
    assert (
        classify_change(
            previous_run_id="run-1",
            previous_products=None,
            current_products=["p1"],
            previous_execution_contract_digest="same",
            execution_contract_digest="same",
        )
        is ChangeClass.NONE
    )


def test_golden_required_is_conservative_for_execution_change() -> None:
    assert golden_required_for(ChangeClass.NONE) == "none"
    assert golden_required_for(ChangeClass.CONTENT_CHANGE) == "none"
    assert golden_required_for(ChangeClass.COHORT_EXPANSION) == "full"
    assert golden_required_for(ChangeClass.COHORT_REDUCTION) == "full"
    assert golden_required_for(ChangeClass.EXECUTION_CHANGE) == "full"
    assert golden_required_for(ChangeClass.METHODOLOGY_CHANGE) == "full"


def test_derive_run_id_excludes_products_and_snapshot_date() -> None:
    """Материал §6.2: ``products`` и ``snapshot_date`` больше не входят в run_id."""
    inputs = _inputs()
    kwargs = {
        "cycle_id": "2026-06-01",
        "cohort_digest": "cohort",
        "seeds_digest": "seeds",
        "execution_contract_digest": "contract",
    }
    base = derive_run_id(inputs, **kwargs)
    assert base.startswith("case_x-")

    # Список products и дата снимка меняются — идентификатор тот же (состав когорты
    # уже зафиксирован cohort_digest, цикл — cycle_id).
    changed = replace(
        inputs,
        run=inputs.run.model_copy(
            update={
                "products": ["p1", "p2"],
                "snapshot_date": _dt.date(2026, 6, 2),
            }
        ),
    )
    assert derive_run_id(changed, **kwargs) == base

    # А значимые поля материала — меняют.
    assert derive_run_id(inputs, **{**kwargs, "cycle_id": "2026-W43"}) != base
    assert derive_run_id(inputs, **{**kwargs, "run_scope": "only_failed"}) != base
    assert derive_run_id(inputs, **{**kwargs, "refresh_policy": "offline"}) != base
    assert derive_run_id(inputs, **{**kwargs, "cohort_digest": "other"}) != base


def test_build_run_identity_without_previous_run() -> None:
    identity = build_run_identity(
        run_id="case_x-abc",
        cycle_id="2026-06-01",
        previous_run_id=None,
        previous_products=None,
        previous_execution_contract_digest=None,
        cohort_digest="cohort",
        seeds_digest="seeds",
        execution_contract_digest="contract",
        run_scope="full",
        refresh_policy=RefreshPolicy.AUTO,
        current_products=["p1"],
    )
    assert isinstance(identity, RunIdentity)
    assert identity.change_class is ChangeClass.NONE
    assert identity.golden_required == "none"
    assert identity.previous_run_id is None


def test_build_run_identity_with_previous_execution_change() -> None:
    identity = build_run_identity(
        run_id="case_x-abc",
        cycle_id="2026-W43",
        previous_run_id="case_x-prev",
        previous_products=["p1"],
        previous_execution_contract_digest="old",
        cohort_digest="cohort",
        seeds_digest="seeds",
        execution_contract_digest="new",
        run_scope="full",
        refresh_policy=RefreshPolicy.REVALIDATE,
        current_products=["p1"],
    )
    assert identity.previous_run_id == "case_x-prev"
    assert identity.change_class is ChangeClass.EXECUTION_CHANGE
    assert identity.golden_required == "full"
    assert identity.refresh_policy is RefreshPolicy.REVALIDATE
