"""Тесты нормализации и плана прогона (W5/T5.4).

``load_inputs`` собирает артефакты ``tests/config/fixtures/`` в :class:`RunInputs`;
``plan`` строит воспроизводимый :class:`RunPlan` (scope, роли, когорта,
перенормировка весов). План и линтер компонуются через ``load_plan``. Фикстуры
категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from core.domain import FetchMode, Focus
from core.pipeline.normalize import RunInputs, cross_check, load_inputs
from core.pipeline.plan import load_plan, plan

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"

_PACKAGE_FILES = (
    "package.yaml",
    "catalog.csv",
    "baselines.csv",
    "cohort.yaml",
    "golden.csv",
    "domain_profiles.yaml",
)


def _inputs() -> RunInputs:
    """Нормализованные входы на фикстурах."""
    return load_inputs(RUN_YAML, FIXTURES)


def _clean() -> RunInputs:
    """Входы с настроенной процедурой аттестации (линт-чистые, §4.2/I2b)."""
    return replace(
        _inputs(),
        scope_attestation_procedure="ручная аттестация полноты источника (тест)",
    )


def _with_focus(modules: list[str], params: list[str]) -> RunInputs:
    """Копия чистых входов с подменённым ``run.focus`` (без ревалидации)."""
    inputs = _clean()
    run = inputs.run.model_copy(update={"focus": Focus(modules=modules, params=params)})
    return replace(inputs, run=run)


# ---------------------------------------------------------------------------
# normalize: load_inputs / cross_check
# ---------------------------------------------------------------------------


def test_load_inputs_happy() -> None:
    inputs = _inputs()
    assert inputs.run.case_id == "case_x"
    assert inputs.package.case_id == "case_x"
    assert [row.param_id for row in inputs.catalog] == ["param_a", "param_b"]
    assert len(inputs.baselines) == 2
    assert {entry.product_id for entry in inputs.cohort.entries} == {"p1", "p2"}
    assert len(inputs.golden) == 2
    assert inputs.domain_profiles["case_x"].fetch is FetchMode.HTTP
    assert inputs.scope_attestation_procedure is None


def test_load_inputs_reads_optional_attestation(tmp_path: Path) -> None:
    for name in _PACKAGE_FILES:
        (tmp_path / name).symlink_to(FIXTURES / name)
    (tmp_path / "scope_attestation.yaml").write_text(
        "procedure: ручная аттестация полноты источника\n", encoding="utf-8"
    )
    inputs = load_inputs(RUN_YAML, tmp_path)
    assert inputs.scope_attestation_procedure == "ручная аттестация полноты источника"


def test_cross_check_ok() -> None:
    assert cross_check(_clean()) == []


def test_cross_check_reports_s11_mismatch() -> None:
    inputs = _clean()
    run = inputs.run.model_copy(update={"products": ["p1", "p2", "p3"]})
    violations = cross_check(replace(inputs, run=run))
    assert violations and "S11" in violations[0] and "p3" in violations[0]


def test_cross_check_reports_anchor_mismatch() -> None:
    inputs = _clean()
    run = inputs.run.model_copy(update={"anchor": "p3"})
    violations = cross_check(replace(inputs, run=run))
    assert any("anchor" in violation and "p3" in violation for violation in violations)


# ---------------------------------------------------------------------------
# plan: базовые поля, фокус, роли, golden
# ---------------------------------------------------------------------------


def test_plan_happy_fields() -> None:
    result = plan(_clean())
    assert result.run_id.startswith("case_x-")
    assert result.products == ["p1", "p2"]
    assert result.anchor == "p1"
    assert result.modules == ["mod_a"]
    assert result.roles == ["role_a"]
    assert [row.param_id for row in result.params] == ["param_a"]
    assert result.module_weights == {"mod_a": 60.0}
    assert result.renormalized_weights == {"mod_a": 1.0}
    assert result.cohort.case_id == "case_x"
    assert [(row.product_id, row.param_id, row.tier.value) for row in result.golden] == [
        ("p1", "param_a", "fast"),
        ("p2", "param_b", "full"),
    ]


def test_plan_explicit_run_id() -> None:
    assert plan(_clean(), run_id="run-x").run_id == "run-x"


def test_plan_focus_by_module() -> None:
    result = plan(_with_focus(modules=["mod_b"], params=[]))
    assert result.modules == ["mod_b"]
    assert [row.param_id for row in result.params] == ["param_b"]
    assert result.module_weights == {"mod_b": 40.0}
    assert result.renormalized_weights == {"mod_b": 1.0}
    assert result.roles == ["role_a"]


def test_plan_focus_by_param_keeps_all_modules() -> None:
    result = plan(_with_focus(modules=[], params=["param_b"]))
    assert result.modules == ["mod_a", "mod_b"]
    assert [row.param_id for row in result.params] == ["param_b"]
    assert result.module_weights == {"mod_a": 60.0, "mod_b": 40.0}
    # Применим только mod_b (его параметр в scope).
    assert result.renormalized_weights == {"mod_b": 1.0}


def test_plan_unknown_focus_module_warns() -> None:
    result = plan(_with_focus(modules=["mod_x"], params=[]))
    assert result.modules == ["mod_x"]
    assert any("mod_x" in warning for warning in result.warnings)


# ---------------------------------------------------------------------------
# plan: перенормировка и детерминизм
# ---------------------------------------------------------------------------


def test_renormalization_sums_to_one_over_applicable() -> None:
    result = plan(_with_focus(modules=[], params=[]))
    assert set(result.renormalized_weights) == {"mod_a", "mod_b"}
    assert result.renormalized_weights["mod_a"] == pytest.approx(0.6)
    assert result.renormalized_weights["mod_b"] == pytest.approx(0.4)
    assert sum(result.renormalized_weights.values()) == pytest.approx(1.0)


def test_plan_is_deterministic() -> None:
    first = plan(_clean())
    second = plan(_clean())
    assert first == second
    assert first.run_id == second.run_id
    # Порядок не зависит от порядка входа: reversed-когорта даёт тот же план.
    inputs = _clean()
    reversed_products = list(reversed(inputs.run.products))
    run = inputs.run.model_copy(update={"products": reversed_products})
    assert plan(replace(inputs, run=run)).products == first.products


# ---------------------------------------------------------------------------
# композиция load_inputs → lint → plan
# ---------------------------------------------------------------------------


def test_load_plan_blocks_without_attestation() -> None:
    with pytest.raises(ValueError, match="I2b"):
        load_plan(str(RUN_YAML), str(FIXTURES))


def test_load_plan_succeeds_with_attestation(tmp_path: Path) -> None:
    for name in _PACKAGE_FILES:
        (tmp_path / name).symlink_to(FIXTURES / name)
    (tmp_path / "scope_attestation.yaml").write_text(
        "procedure: ручная аттестация полноты источника\n", encoding="utf-8"
    )
    result = load_plan(str(RUN_YAML), str(tmp_path))
    assert result.run_id.startswith("case_x-")
    assert [row.param_id for row in result.params] == ["param_a"]
