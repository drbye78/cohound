"""coHound — линтер входов прогона (W5/T5.2, ``03`` §D.3): блокирует прогон.

Проверяет правила §4.2 плюс новые, которые **нельзя** выразить одиночными
pydantic-валидаторами домена (нужно сопоставление нескольких артефактов):

- сумма весов модулей = 100 (defensive: домен уже проверяет при загрузке);
- архетип C при непустом ``baselines`` (manifest и/или ``baselines.csv``) → **блок**
  (§1.5/§4.2);
- ``absence_rule ∈ {parameter_reference, distribution_absence}`` без настроенной
  процедуры аттестации полноты источника ``scope_attested_by`` → **блок** (§4.2/I2b);
- пустые ``concept_terms`` у параметра → warning (§4.2; S13);
- ``golden-full`` без страты ожидаемых ❌ → **блок** (§4.2/§11.6); наличие тиров
  ``fast``/``full`` в golden-наборе → warning;
- ``showcase`` с ``rule`` без публичной величины/метрики → **блок** (зеркало домена);
- ``param_id`` не в нижнем регистре → **блок** (зеркало домена);
- ``products`` — проекция когорты (S11) и ``anchor ∈ products`` → **блок** (§4.5).

Каждое правило — отдельный маленький предикат из :data:`RULES`, поэтому оно
тестируется изолированно и адресуется кодом (``code``). Нейтрален к предметной
категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from core.config.run_config import cross_check_products
from core.domain.enums import AbsenceRuleType, Archetype, GoldenTier, ParamType, Status
from core.pipeline.normalize import RunInputs

__all__ = [
    "LintReport",
    "LintRule",
    "Severity",
    "RULES",
    "lint_run_inputs",
    "assert_lint",
    # правила (по одному предикату; тестируются изолированно)
    "check_weights_sum_to_100",
    "check_archetype_c_baselines",
    "check_absence_attestation",
    "check_empty_concept_terms",
    "check_golden_full_absent_stratum",
    "check_golden_presence",
    "check_showcase_metric",
    "check_slug_lowercase",
    "check_anchor_in_products",
    "check_products_match_cohort",
]

#: Уровень правила: блокирует прогон или только предупреждает.
Severity = Literal["blocking", "warning"]

#: absence-правила, требующие действующей процедуры аттестации ``source_scope`` (I2b).
_ATTESTED_ABSENCE_RULES: frozenset[AbsenceRuleType] = frozenset(
    {AbsenceRuleType.PARAMETER_REFERENCE, AbsenceRuleType.DISTRIBUTION_ABSENCE}
)

#: Токены публичной величины/метрики — зеркало ``CatalogRow._check`` (домен §4.2).
#: Дублируется намеренно: линтер обязан re-check'нуть правило даже для объекта,
#: собранного в обход валидатора (``model_construct``/``model_copy``).
_SHOWCASE_TOKENS: tuple[str, ...] = ("величин", "metric", "велич", "%", "числ", "значени")


@dataclass
class LintReport:
    """Отчёт линтера: блокирующие нарушения и предупреждения.

    ``ok`` = нет блокирующих. :meth:`raise_if_blocking` превращает блокирующие
    сообщения в :class:`ValueError` (сводки для CLI, ``03`` §E.2).
    """

    blocking: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Прогон разрешён: нет ни одного блокирующего нарушения."""
        return not self.blocking

    def raise_if_blocking(self) -> None:
        """Поднимает :class:`ValueError` со склейкой блокирующих сообщений."""
        if self.blocking:
            raise ValueError("; ".join(self.blocking))


@dataclass(frozen=True)
class LintRule:
    """Именованное правило линтера: код, уровень и проверка ``RunInputs → [msg]``."""

    code: str
    severity: Severity
    check: Callable[[RunInputs], list[str]]


# ---------------------------------------------------------------------------
# Правила (по одному предикату)
# ---------------------------------------------------------------------------


def check_weights_sum_to_100(inputs: RunInputs) -> list[str]:
    """§4.2: сумма весов модулей package.yaml обязана быть 100 (defensive re-check)."""
    total = round(sum(module.weight for module in inputs.package.modules), 6)
    if total != 100:
        return [
            f"§4.2: сумма весов модулей package.yaml должна быть 100, получено {total}"
        ]
    return []


def check_archetype_c_baselines(inputs: RunInputs) -> list[str]:
    """§1.5/§4.2: архетип C не заполняет baselines — непустой набор есть ошибка.

    Проверяются оба носителя: ``package.yaml: baselines`` (реестр в манифесте) и
    ``baselines.csv`` (строки наследования).
    """
    if inputs.package.archetype is not Archetype.C:
        return []
    manifest_ids = set(inputs.package.baselines)
    csv_ids = {row.baseline_id for row in inputs.baselines}
    found = sorted(manifest_ids | csv_ids)
    if not found:
        return []
    return [
        "§4.2/§1.5: архетип C не заполняет baselines "
        f"(найдено: {', '.join(found)})"
    ]


def check_absence_attestation(inputs: RunInputs) -> list[str]:
    """§4.2/I2b: ``parameter_reference``/``distribution_absence`` требуют процедуры.

    Процедура аттестации полноты источника — пакетная настройка
    ``RunInputs.scope_attestation_procedure`` (загружается из опционального
    ``scope_attestation.yaml`` в каталоге пакета, см. ``normalize``). Если она не
    задана, а каталог содержит такие absence-правила — прогон блокируется: без
    механизма ``scope_attested_by`` ❌ по этим правилам недоказуем (I2b).
    """
    if (inputs.scope_attestation_procedure or "").strip():
        return []
    offenders = sorted(
        row.param_id
        for row in inputs.catalog
        if row.absence_rule in _ATTESTED_ABSENCE_RULES
    )
    if not offenders:
        return []
    return [
        "§4.2/I2b: absence_rule parameter_reference/distribution_absence требует "
        "действующей процедуры аттестации source_scope "
        f"(scope_attestation_procedure не настроена); затронуты: {', '.join(offenders)}"
    ]


def check_empty_concept_terms(inputs: RunInputs) -> list[str]:
    """§4.2/S13: пустые ``concept_terms`` → warning (немой поиск по синонимам)."""
    missing = sorted(
        row.param_id for row in inputs.catalog if not _has_terms(row.concept_terms)
    )
    if not missing:
        return []
    return [f"§4.2: пустые concept_terms у параметров: {', '.join(missing)}"]


def check_golden_full_absent_stratum(inputs: RunInputs) -> list[str]:
    """§4.2/§11.6: golden-full обязан включать страту ожидаемых ❌.

    Проверяется состав **тира full**: если full-строки есть, среди них обязана
    быть хотя бы одна с ``expected_status = ❌`` (через ``registry_absence`` /
    ``distribution_absence``). Иначе критический класс false-❌ нечем тренировать.
    """
    full_rows = [row for row in inputs.golden if row.tier is GoldenTier.FULL]
    if not full_rows:
        return []
    if any(row.expected_status is Status.ABSENT for row in full_rows):
        return []
    return [
        "§4.2/§11.6: golden-full обязан включать страту ожидаемых ❌ "
        "(registry_absence/distribution_absence)"
    ]


def check_golden_presence(inputs: RunInputs) -> list[str]:
    """§11.6: presence-предупреждения golden-набора (тиры fast/full)."""
    if not inputs.golden:
        return ["§11.6: golden.csv пуст — golden-сеть отсутствует"]
    messages: list[str] = []
    if not any(row.tier is GoldenTier.FAST for row in inputs.golden):
        messages.append("§11.6: отсутствует тир golden-fast")
    if not any(row.tier is GoldenTier.FULL for row in inputs.golden):
        messages.append("§11.6: отсутствует тир golden-full")
    return messages


def check_showcase_metric(inputs: RunInputs) -> list[str]:
    """§4.2: showcase → ``rule`` обязан упоминать публичную величину/метрику."""
    offenders = sorted(
        row.param_id
        for row in inputs.catalog
        if row.param_type is ParamType.SHOWCASE
        and not any(token in row.rule.lower() for token in _SHOWCASE_TOKENS)
    )
    if not offenders:
        return []
    return [
        "§4.2: param_type=showcase требует публичную величину/метрику в rule; "
        f"нарушают: {', '.join(offenders)}"
    ]


def check_slug_lowercase(inputs: RunInputs) -> list[str]:
    """§4.2: ``param_id`` — slug в нижнем регистре (зеркало ``CatalogRow``)."""
    offenders = sorted(
        row.param_id
        for row in inputs.catalog
        if not row.param_id or row.param_id != row.param_id.lower()
    )
    if not offenders:
        return []
    return [f"§4.2: param_id — slug в нижнем регистре; нарушают: {', '.join(offenders)}"]


def check_anchor_in_products(inputs: RunInputs) -> list[str]:
    """§4.5: ``anchor`` обязан входить в ``products`` (defensive re-check)."""
    if inputs.run.anchor not in inputs.run.products:
        return [
            f"§4.5: anchor '{inputs.run.anchor}' обязан входить в products"
        ]
    return []


def check_products_match_cohort(inputs: RunInputs) -> list[str]:
    """S11 (§4.5): ``products`` — проекция состава когорты (один источник)."""
    return sorted(cross_check_products(inputs.run, inputs.cohort))


#: Все правила линтера в детерминированном порядке.
RULES: tuple[LintRule, ...] = (
    LintRule("weights-sum-100", "blocking", check_weights_sum_to_100),
    LintRule("archetype-c-baselines", "blocking", check_archetype_c_baselines),
    LintRule("absence-attestation", "blocking", check_absence_attestation),
    LintRule("golden-full-absent-stratum", "blocking", check_golden_full_absent_stratum),
    LintRule("showcase-metric", "blocking", check_showcase_metric),
    LintRule("slug-lowercase", "blocking", check_slug_lowercase),
    LintRule("anchor-in-products", "blocking", check_anchor_in_products),
    LintRule("products-match-cohort", "blocking", check_products_match_cohort),
    LintRule("empty-concept-terms", "warning", check_empty_concept_terms),
    LintRule("golden-presence", "warning", check_golden_presence),
)


# ---------------------------------------------------------------------------
# Публичный вход
# ---------------------------------------------------------------------------


def _has_terms(terms: dict[str, list[str]] | None) -> bool:
    """Есть ли хотя бы один непустой термин в ``concept_terms``."""
    if not terms:
        return False
    return any(entry for entry in terms.values())


def lint_run_inputs(inputs: RunInputs) -> LintReport:
    """Прогоняет все :data:`RULES` и собирает :class:`LintReport`.

    Правила не зависят друг от друга; порядок сообщений детерминирован (порядок
    ``RULES`` и сортировка внутри предикатов). Не блокирует сама по себе — решение
    принимает вызывающий (``raise_if_blocking`` в CLI-композиции).
    """
    report = LintReport()
    for rule in RULES:
        messages = rule.check(inputs)
        if rule.severity == "blocking":
            report.blocking.extend(messages)
        else:
            report.warnings.extend(messages)
    return report


def assert_lint(inputs: RunInputs) -> LintReport:
    """Линтует и поднимает :class:`ValueError`, если есть блокирующие нарушения."""
    report = lint_run_inputs(inputs)
    report.raise_if_blocking()
    return report
