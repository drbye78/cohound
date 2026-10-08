"""Тесты линтера входов прогона (W5/T5.2).

Проверяют, что каждое правило :data:`RULES` срабатывает и что чистая конфигурация
проходит. Проверки вызывают отдельные предикаты там, где нужна изоляция, и
:func:`lint_run_inputs` — для интеграции уровней blocking/warning. Фикстуры
категорийно-нейтральны (``tests/config/fixtures/``, §1.3).
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from core.domain import Archetype, GoldenTier, ModuleDef, Status
from core.pipeline.lint import (
    RULES,
    LintReport,
    check_absence_attestation,
    check_anchor_in_products,
    check_archetype_c_baselines,
    check_empty_concept_terms,
    check_golden_full_absent_stratum,
    check_golden_presence,
    check_products_match_cohort,
    check_showcase_metric,
    check_slug_lowercase,
    check_weights_sum_to_100,
    lint_run_inputs,
)
from core.pipeline.normalize import RunInputs, load_inputs

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"


def _inputs() -> RunInputs:
    """Нормализованные входы на фикстурах (без настроенной процедуры аттестации)."""
    return load_inputs(RUN_YAML, FIXTURES)


def _clean() -> RunInputs:
    """Входы с настроенной процедурой аттестации → линт-чистые (§4.2/I2b)."""
    return replace(
        _inputs(),
        scope_attestation_procedure="ручная аттестация полноты источника (тест)",
    )


# ---------------------------------------------------------------------------
# LintReport / реестр правил
# ---------------------------------------------------------------------------


def test_lint_report_ok_and_raise() -> None:
    report = LintReport(blocking=[], warnings=["w"])
    assert report.ok is True
    assert report.warnings == ["w"]

    report = LintReport(blocking=["a", "b"])
    assert report.ok is False
    with pytest.raises(ValueError) as excinfo:
        report.raise_if_blocking()
    assert "a" in str(excinfo.value) and "b" in str(excinfo.value)


def test_rules_registry_is_complete() -> None:
    codes = {rule.code for rule in RULES}
    expected = {
        "weights-sum-100",
        "archetype-c-baselines",
        "absence-attestation",
        "golden-full-absent-stratum",
        "showcase-metric",
        "slug-lowercase",
        "anchor-in-products",
        "products-match-cohort",
        "empty-concept-terms",
        "golden-presence",
    }
    assert expected <= codes


def test_clean_fixture_passes() -> None:
    report = lint_run_inputs(_clean())
    assert report.ok is True
    assert report.blocking == []
    assert report.warnings == []


# ---------------------------------------------------------------------------
# weights-sum-100
# ---------------------------------------------------------------------------


def test_weights_sum_100_passes() -> None:
    assert check_weights_sum_to_100(_clean()) == []


def test_weights_sum_not_100_blocks() -> None:
    package = _clean().package.model_copy(
        update={
            "modules": [
                ModuleDef(module_id="mod_a", weight=60),
                ModuleDef(module_id="mod_b", weight=30),
            ]
        }
    )
    inputs = replace(_clean(), package=package)
    messages = check_weights_sum_to_100(inputs)
    assert messages and "100" in messages[0]
    assert any("100" in msg for msg in lint_run_inputs(inputs).blocking)


# ---------------------------------------------------------------------------
# archetype-c-baselines
# ---------------------------------------------------------------------------


def test_archetype_c_with_baselines_blocks() -> None:
    package = _clean().package.model_copy(
        update={"archetype": Archetype.C, "baseline_participant": None}
    )
    inputs = replace(_clean(), package=package)
    messages = check_archetype_c_baselines(inputs)
    assert messages and "архетип C" in messages[0]
    assert any("архетип C" in msg for msg in lint_run_inputs(inputs).blocking)


def test_archetype_c_without_baselines_passes() -> None:
    package = _clean().package.model_copy(
        update={"archetype": Archetype.C, "baseline_participant": None, "baselines": []}
    )
    inputs = replace(_clean(), package=package, baselines=[])
    assert check_archetype_c_baselines(inputs) == []


def test_archetype_b_with_baselines_passes() -> None:
    assert check_archetype_c_baselines(_clean()) == []


# ---------------------------------------------------------------------------
# absence-attestation
# ---------------------------------------------------------------------------


def test_absence_without_procedure_blocks() -> None:
    # param_b в фикстуре использует distribution_absence, процедура не настроена.
    inputs = _inputs()
    messages = check_absence_attestation(inputs)
    assert messages and "I2b" in messages[0] and "param_b" in messages[0]
    assert any("I2b" in msg for msg in lint_run_inputs(inputs).blocking)


def test_absence_with_procedure_passes() -> None:
    assert check_absence_attestation(_clean()) == []


# ---------------------------------------------------------------------------
# empty-concept-terms (warning)
# ---------------------------------------------------------------------------


def test_empty_concept_terms_warns() -> None:
    catalog = list(_clean().catalog)
    catalog[0] = catalog[0].model_copy(update={"concept_terms": {}})
    inputs = replace(_clean(), catalog=catalog)

    messages = check_empty_concept_terms(inputs)
    assert messages and "param_a" in messages[0]
    report = lint_run_inputs(inputs)
    assert report.ok is True  # warning, не блок
    assert any("concept_terms" in msg for msg in report.warnings)


def test_non_empty_concept_terms_passes() -> None:
    assert check_empty_concept_terms(_clean()) == []


# ---------------------------------------------------------------------------
# golden-full-absent-stratum (blocking) + golden-presence (warning)
# ---------------------------------------------------------------------------


def test_golden_full_with_absent_stratum_passes() -> None:
    assert check_golden_full_absent_stratum(_clean()) == []


def test_golden_full_without_absent_stratum_blocks() -> None:
    golden = [
        row.model_copy(update={"tier": GoldenTier.FULL, "expected_status": Status.OK})
        for row in _clean().golden
    ]
    inputs = replace(_clean(), golden=golden)
    messages = check_golden_full_absent_stratum(inputs)
    assert messages and "❌" in messages[0]
    assert any("❌" in msg for msg in lint_run_inputs(inputs).blocking)


def test_golden_missing_fast_warns() -> None:
    golden = [row for row in _clean().golden if row.tier is GoldenTier.FULL]
    inputs = replace(_clean(), golden=golden)
    assert any("golden-fast" in msg for msg in check_golden_presence(inputs))
    report = lint_run_inputs(inputs)
    assert report.ok is True
    assert any("golden-fast" in msg for msg in report.warnings)


def test_golden_missing_full_warns() -> None:
    golden = [row for row in _clean().golden if row.tier is GoldenTier.FAST]
    inputs = replace(_clean(), golden=golden)
    assert any("golden-full" in msg for msg in check_golden_presence(inputs))
    assert lint_run_inputs(inputs).ok is True


def test_empty_golden_warns() -> None:
    inputs = replace(_clean(), golden=[])
    messages = check_golden_presence(inputs)
    assert messages and "пуст" in messages[0]


# ---------------------------------------------------------------------------
# showcase-metric
# ---------------------------------------------------------------------------


def test_showcase_with_metric_passes() -> None:
    assert check_showcase_metric(_clean()) == []


def test_showcase_without_metric_blocks() -> None:
    catalog = list(_clean().catalog)
    # param_b — showcase; снимаем метрику, обходя pydantic-валидатор домена.
    catalog[1] = catalog[1].model_copy(update={"rule": "описание без метрики"})
    inputs = replace(_clean(), catalog=catalog)

    messages = check_showcase_metric(inputs)
    assert messages and "param_b" in messages[0]
    assert any("showcase" in msg for msg in lint_run_inputs(inputs).blocking)


# ---------------------------------------------------------------------------
# slug-lowercase
# ---------------------------------------------------------------------------


def test_slug_lowercase_passes() -> None:
    assert check_slug_lowercase(_clean()) == []


def test_slug_uppercase_blocks() -> None:
    catalog = list(_clean().catalog)
    catalog[0] = catalog[0].model_copy(update={"param_id": "Param_Z"})
    inputs = replace(_clean(), catalog=catalog)

    messages = check_slug_lowercase(inputs)
    assert messages and "Param_Z" in messages[0]
    assert any("slug" in msg for msg in lint_run_inputs(inputs).blocking)


# ---------------------------------------------------------------------------
# anchor-in-products / products-match-cohort (S11)
# ---------------------------------------------------------------------------


def test_products_match_cohort_passes() -> None:
    assert check_products_match_cohort(_clean()) == []
    assert check_anchor_in_products(_clean()) == []


def test_products_match_cohort_mismatch_blocks() -> None:
    run = _clean().run.model_copy(update={"products": ["p1", "p2", "p3"]})
    inputs = replace(_clean(), run=run)
    messages = check_products_match_cohort(inputs)
    assert messages and "S11" in messages[0] and "p3" in messages[0]
    assert any("S11" in msg for msg in lint_run_inputs(inputs).blocking)


def test_anchor_not_in_products_blocks() -> None:
    run = _clean().run.model_copy(update={"anchor": "p3"})
    inputs = replace(_clean(), run=run)
    messages = check_anchor_in_products(inputs)
    assert messages and "p3" in messages[0]
    assert any("anchor" in msg for msg in lint_run_inputs(inputs).blocking)
