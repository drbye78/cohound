"""Тесты линтера входов прогона (W5/T5.2).

Проверяют, что каждое правило :data:`RULES` срабатывает и что чистая конфигурация
проходит. Проверки вызывают отдельные предикаты там, где нужна изоляция, и
:func:`lint_run_inputs` — для интеграции уровней blocking/warning. Фикстуры
категорийно-нейтральны (``tests/config/fixtures/``, §1.3).
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    Archetype,
    GoldenTier,
    ModuleDef,
    ReferenceProducts,
    Status,
)
from core.pipeline.golden_status import GoldenStatus
from core.pipeline.lint import (
    RULES,
    LintReport,
    check_absence_attestation,
    check_anchor_in_products,
    check_archetype_c_baselines,
    check_baseline_reference_product,
    check_empty_concept_terms,
    check_golden_full_absent_stratum,
    check_golden_presence,
    check_passport_upstreams,
    check_products_match_cohort,
    check_reference_product_not_in_cohort,
    check_reference_versions,
    check_showcase_metric,
    check_slug_lowercase,
    check_upstream_presence,
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
        # План 27 §10: связь «строка baselines.csv → reference-продукт» и форма предков.
        "baseline-without-reference-product",
        "reference-product-in-cohort",
        "baseline-version-numeric",
        "passport-upstreams",
        "upstream-presence",
    }
    assert expected <= codes
    # Новые правила — блокирующие, кроме `upstream-presence` (видимое состояние, не блок).
    blocking = {rule.code for rule in RULES if rule.severity == "blocking"}
    assert {
        "baseline-without-reference-product",
        "reference-product-in-cohort",
        "baseline-version-numeric",
        "passport-upstreams",
    } <= blocking
    assert "upstream-presence" not in blocking


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
        update={"archetype": Archetype.C, "baseline_participants": []}
    )
    inputs = replace(_clean(), package=package)
    messages = check_archetype_c_baselines(inputs)
    assert messages and "архетип C" in messages[0]
    assert any("архетип C" in msg for msg in lint_run_inputs(inputs).blocking)


def test_archetype_c_without_baselines_passes() -> None:
    package = _clean().package.model_copy(
        update={"archetype": Archetype.C, "baseline_participants": [], "baselines": []}
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


# Объявленное состояние эталона: «сверять не с чем» — режим, а не недочёт.
# Необъявленное отсутствие остаётся warning'ом: молчание не должно выглядеть решением.


def _status(state: str, *, declared: bool, file_present: bool, reason: str | None = None):
    """Объявленное состояние эталона для линтер-тестов (нейтральные значения)."""
    return GoldenStatus(
        state=state,  # type: ignore[arg-type] — значения заданы тестом
        declared=declared,
        file_present=file_present,
        reason=reason,
    )


def test_golden_absence_declared_is_not_warning() -> None:
    inputs = replace(
        _clean(),
        golden=[],
        golden_status=_status("absent", declared=True, file_present=False),
    )
    assert check_golden_presence(inputs) == []
    assert lint_run_inputs(inputs).warnings == []


def test_golden_declined_is_not_warning() -> None:
    inputs = replace(
        _clean(),
        golden=[],
        golden_status=_status(
            "declined",
            declared=True,
            file_present=True,
            reason="разметка непригодна",
        ),
    )
    assert check_golden_presence(inputs) == []
    assert lint_run_inputs(inputs).warnings == []


def test_golden_absence_undeclared_still_warns() -> None:
    inputs = replace(
        _clean(),
        golden=[],
        golden_status=_status("absent", declared=False, file_present=False),
    )
    messages = check_golden_presence(inputs)
    assert messages and "golden.csv отсутствует" in messages[0]
    assert any("golden" in warning for warning in lint_run_inputs(inputs).warnings)


def test_golden_present_but_empty_still_warns() -> None:
    """Объявленный ``present`` с пустым набором — недочёт, а не режим."""
    inputs = replace(
        _clean(),
        golden=[],
        golden_status=_status("present", declared=True, file_present=True),
    )
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


# ---------------------------------------------------------------------------
# План 27 §10: связь baselines.csv ↔ reference-продукты и форма предков
# ---------------------------------------------------------------------------


def _passport(product_id: str, *, upstreams: list[tuple[str, str]]) -> Any:
    """Паспорт с объявленными линиями ``[(baseline_id, version), …]`` (порядок = приоритет)."""
    from core.domain import Citation, Passport, Upstream

    return Passport(
        product_id=product_id,
        canonical_name=f"Product {product_id}",
        vendor="Vendor",
        jurisdiction="XX",
        snapshot_date=_dt.date(2026, 1, 1),
        upstreams=[
            Upstream(
                baseline_id=baseline_id,
                version=version,
                evidence=Citation(
                    url=f"https://{baseline_id}.test/doc", quote="ancestor quote"
                ),
            )
            for baseline_id, version in upstreams
        ],
    )


def _declared(*baseline_ids: str) -> ReferenceProducts:
    """Минимальная декларация reference-продуктов (schema ``reference_products/1``)."""
    return ReferenceProducts.model_validate(
        {
            "schema": "reference_products/1",
            "products": [
                {
                    "baseline_id": baseline_id,
                    "product_id": f"ref_{baseline_id}",
                    "canonical_name": f"Reference {baseline_id}",
                    "current_version": "1",
                    "versions": [
                        {"version": "1", "docs_roots": [f"https://{baseline_id}.test/docs"]}
                    ],
                }
                for baseline_id in baseline_ids
            ],
        }
    )


def test_baselines_without_reference_product_blocks() -> None:
    # baselines.csv непуст, а декларации нет вовсе: строке некуда наследовать.
    inputs = replace(_clean(), reference_products=None)
    messages = check_baseline_reference_product(inputs)
    assert messages and "baseline-without-reference-product" in messages[0]
    assert "reference_products.yaml отсутствует" in messages[0]
    assert any(
        "baseline-without-reference-product" in msg
        for msg in lint_run_inputs(inputs).blocking
    )


def test_baselines_with_undeclared_ancestor_blocks() -> None:
    # Декларация есть, но объявляет другую линию: (baseline_b, param_b) остаётся сиротой.
    inputs = replace(_clean(), reference_products=_declared("baseline_a"))
    messages = check_baseline_reference_product(inputs)
    assert messages and "baseline_b" in messages[0] and "param_b" in messages[0]
    assert lint_run_inputs(inputs).ok is False


def test_empty_baselines_need_no_reference_product() -> None:
    # Архетип C и пилот без строк: правило не задевает пустой baselines.csv.
    inputs = replace(_clean(), reference_products=None, baselines=[])
    assert check_baseline_reference_product(inputs) == []


def test_reference_product_in_cohort_blocks() -> None:
    declared = _declared("baseline_a")
    # product_id reference-продукта делит namespace с когортой — пересечение блокирует.
    colliding = declared.model_copy(
        update={
            "products": [
                declared.products[0].model_copy(update={"product_id": "p1"})
            ]
        }
    )
    inputs = replace(_clean(), reference_products=colliding)
    messages = check_reference_product_not_in_cohort(inputs)
    assert messages and "reference-product-in-cohort" in messages[0]
    assert "'p1'" in messages[0]
    assert any(
        "reference-product-in-cohort" in msg for msg in lint_run_inputs(inputs).blocking
    )


def test_reference_product_outside_cohort_passes() -> None:
    assert check_reference_product_not_in_cohort(_clean()) == []


def test_reference_product_as_comparison_base_passes() -> None:
    """Точка отсчёта — не участник сравнения (§27.10, решение 18.11.2026).

    ``postgresql`` у ``ru-pg-dbms`` объявлен и предком, и ``comparison_base: true``:
    измеряется, чтобы дать Δ, а не чтобы занять место в матрице. Оба поля обязаны
    разрешать одно и то же — иначе «разрешено в когорте, запрещено в ``run.products``»
    дало бы то же падение через соседнее поле.
    """
    declared = _declared("baseline_a")
    colliding = declared.model_copy(
        update={
            "products": [
                declared.products[0].model_copy(update={"product_id": "p1"})
            ]
        }
    )
    base = _clean().cohort.entries[0].model_copy(
        update={"product_id": "p1", "comparison_base": True}
    )
    inputs = replace(
        _clean(),
        reference_products=colliding,
        cohort=_clean().cohort.model_copy(update={"entries": [base]}),
        run=_clean().run.model_copy(update={"products": ["p1"]}),
    )
    assert check_reference_product_not_in_cohort(inputs) == []
    assert "reference-product-in-cohort" not in " ".join(lint_run_inputs(inputs).blocking)


def test_reference_product_in_cohort_without_base_flag_blocks() -> None:
    """Тот же id, но без ``comparison_base`` — по-прежнему блокирует."""
    declared = _declared("baseline_a")
    colliding = declared.model_copy(
        update={
            "products": [
                declared.products[0].model_copy(update={"product_id": "p1"})
            ]
        }
    )
    base = _clean().cohort.entries[0].model_copy(
        update={"product_id": "p1", "comparison_base": False}
    )
    inputs = replace(
        _clean(),
        reference_products=colliding,
        cohort=_clean().cohort.model_copy(update={"entries": [base]}),
        run=_clean().run.model_copy(update={"products": ["p1"]}),
    )
    assert check_reference_product_not_in_cohort(inputs) != []


def test_archetype_a_with_two_upstreams_blocks() -> None:
    package = _clean().package.model_copy(update={"archetype": Archetype.A})
    inputs = replace(
        _clean(),
        package=package,
        passports={
            "p1": _passport("p1", upstreams=[("baseline_a", "1"), ("baseline_b", "1")]),
            "p2": _passport("p2", upstreams=[("baseline_a", "1")]),
        },
    )
    messages = check_passport_upstreams(inputs)
    assert any("архетип A" in msg and "p1" in msg for msg in messages)
    assert any("upstreams-by-archetype" in msg for msg in lint_run_inputs(inputs).blocking)


def test_archetype_c_with_upstreams_blocks() -> None:
    package = _clean().package.model_copy(
        update={"archetype": Archetype.C, "baselines": [], "baseline_participants": []}
    )
    inputs = replace(
        _clean(),
        package=package,
        baselines=[],
        passports={"p1": _passport("p1", upstreams=[("baseline_a", "1")])},
    )
    messages = check_passport_upstreams(inputs)
    assert any("архетип C" in msg and "p1" in msg for msg in messages)
    assert any("upstreams-by-archetype" in msg for msg in lint_run_inputs(inputs).blocking)


def test_undeclared_passport_upstream_blocks() -> None:
    # Продукт объявляет линию, о которой профиль ничего не знает: наследовать не по чему.
    inputs = replace(
        _clean(),
        passports={"p1": _passport("p1", upstreams=[("baseline_zzz", "1")])},
    )
    messages = check_passport_upstreams(inputs)
    assert any("baseline_zzz" in msg for msg in messages)
    assert any("passport-upstream-undeclared" in msg for msg in lint_run_inputs(inputs).blocking)


def test_archetype_a_without_any_upstream_blocks() -> None:
    # A-пакет описывает линию: без единого объявленного предка линия не существует.
    package = _clean().package.model_copy(update={"archetype": Archetype.A})
    inputs = replace(_clean(), package=package, passports={"p1": _passport("p1", upstreams=[])})
    messages = check_passport_upstreams(inputs)
    assert any("не объявляет ни одного предка" in msg for msg in messages)
    assert lint_run_inputs(inputs).ok is False


def test_archetype_b_without_any_upstream_does_not_block() -> None:
    """B-пакет без единой объявленной линии — **не** блок: это штатная форма B.

    Регламент §1.5: базлайн-участник в когорту включается «если он существует», а
    кросс-линейные параметры измеряются только корпусом. Поэтому «ни у одного продукта
    нет предка» в B — состояние профиля, а не противоречие его методу; блокировать его
    значило бы заставить автора выдумать линию (A3).

    Цена обратного правила измерена: канонический валидный пакет
    (``tests/config/fixtures/package.yaml``) — архетип B с ``baselines: [baseline_a]`` и
    продуктами без предков, и пакетный блок сделал его невалидным.
    """
    assert _clean().package.archetype is Archetype.B
    inputs = replace(
        _clean(),
        passports={
            "p1": _passport("p1", upstreams=[]),
            "p2": _passport("p2", upstreams=[]),
        },
    )
    assert check_passport_upstreams(inputs) == []
    report = lint_run_inputs(inputs)
    assert report.ok is True
    # Наследование недоступно — это названо предупреждением, а не скрыто.
    assert any("p1" in warning and "предок не объявлен" in warning for warning in report.warnings)


def test_archetype_b_with_one_declared_line_passes_package_rule() -> None:
    """Хотя бы одна объявленная линия снимает пакетный блок (метод профиля есть)."""
    inputs = replace(
        _clean(),
        passports={
            "p1": _passport("p1", upstreams=[("baseline_a", "1")]),
            "p2": _passport("p2", upstreams=[]),
        },
    )
    assert check_passport_upstreams(inputs) == []


def test_archetype_b_product_without_upstream_warns_not_blocks() -> None:
    """Продукт B-пакета без предка — предупреждение ``upstream-presence``, не блок.

    Наследование для него недоступно, и это названо; блокировать нечего — «вендор не
    называет версию» остаётся честным состоянием (A3, precision-first).
    """
    inputs = replace(
        _clean(),
        passports={
            "p1": _passport("p1", upstreams=[("baseline_a", "1")]),
            "p2": _passport("p2", upstreams=[]),
        },
    )
    messages = check_upstream_presence(inputs)
    assert any("p2" in msg and "архетип B" in msg for msg in messages)
    assert not any("p1" in msg for msg in messages)
    report = lint_run_inputs(inputs)
    assert report.ok is True
    assert any("p2" in warning and "предок не объявлен" in warning for warning in report.warnings)


def test_archetype_c_product_without_upstream_is_silent() -> None:
    """Архетип C предупреждения не получает: предка там нет по определению (§1.5)."""
    package = _clean().package.model_copy(
        update={"archetype": Archetype.C, "baselines": [], "baseline_participants": []}
    )
    inputs = replace(
        _clean(),
        package=package,
        baselines=[],
        passports={"p1": _passport("p1", upstreams=[])},
    )
    assert check_upstream_presence(inputs) == []
    assert check_passport_upstreams(inputs) == []


def test_archetype_a_product_without_upstream_warns_not_blocks() -> None:
    # «Вендор не называет версию» — честное состояние: предупреждение, не блок (§3.2).
    package = _clean().package.model_copy(update={"archetype": Archetype.A})
    inputs = replace(
        _clean(),
        package=package,
        passports={
            "p1": _passport("p1", upstreams=[("baseline_a", "1")]),
            "p2": _passport("p2", upstreams=[]),
        },
    )
    report = lint_run_inputs(inputs)
    assert report.ok is True
    assert any("p2" in warning and "предок не объявлен" in warning for warning in report.warnings)


def test_non_numeric_min_version_blocks() -> None:
    baselines = [
        row.model_copy(update={"min_version": "abc"}) if row.param_id == "param_a" else row
        for row in _clean().baselines
    ]
    inputs = replace(_clean(), baselines=baselines)
    messages = check_reference_versions(inputs)
    assert any("baseline-version-numeric" in msg and "abc" in msg for msg in messages)
    assert any(
        "baseline-version-numeric" in msg for msg in lint_run_inputs(inputs).blocking
    )


def test_non_numeric_declared_version_blocks() -> None:
    declared = _declared("baseline_a")
    broken = declared.model_copy(
        update={
            "products": [
                declared.products[0].model_copy(
                    update={
                        "current_version": "abc",
                        "versions": [
                            declared.products[0].versions[0].model_copy(
                                update={"version": "abc"}
                            )
                        ],
                    }
                )
            ]
        }
    )
    inputs = replace(_clean(), reference_products=broken)
    messages = check_reference_versions(inputs)
    assert any("baseline_a" in msg and "abc" in msg for msg in messages)
    assert lint_run_inputs(inputs).ok is False


def test_numeric_versions_pass() -> None:
    assert check_reference_versions(_clean()) == []
