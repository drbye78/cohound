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
  ``fast``/``full`` в golden-наборе → warning; **объявленное** отсутствие эталона
  (``state: absent``/``declined`` в ``sources/golden_status.yaml``) — не недочёт:
  сверка не выполняется, и это решение, а не пропуск;
- ``showcase`` с ``rule`` без публичной величины/метрики → **блок** (зеркало домена);
- ``param_id`` не в нижнем регистре → **блок** (зеркало домена);
- ``products`` — проекция когорты (S11) и ``anchor ∈ products`` → **блок** (§4.5);
- авторские поля, не заполненные человеком (пустые ``definition`` роли и
  ``selection_reason`` записи когорты) → **incomplete** (A3): не блокирует создание/
  правку пакета, но запрещает прогон (иначе неполный пакет молча выдал бы
  evaluated-ячейки). Обязательные непустые CSV-поля каталога (``title``/``rule``) уже
  отклоняются загрузчиком (``_req``), поэтому отдельного уровня не требуют;
- продукт когорты без единой golden-строки и без объявленного waiver'а → **incomplete**
  (§11.6): «эталон есть» не значило «эталон покрывает когорту», и продукт без строк
  раньше не проверялся вовсе — неотличимо от «строки потеряли». Лечится строками
  ``golden.csv`` либо объявлением ``exempt_products`` в ``sources/golden_status.yaml``;
- waiver для продукта вне когорты или для продукта, у которого строки есть → **warning**:
  устаревшее объявление называет решение, которого больше нет (сверка при этом идёт).

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
from core.pipeline.golden_status import STATE_ABSENT, STATE_DECLINED
from core.pipeline.normalize import RunInputs

#: Порядок сгруппирован по назначению (типы → правила → предикаты), а не по алфавиту:
#: комментарий-разделитель — часть документации публичной поверхности модуля.
__all__ = [  # noqa: RUF022
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
    "check_golden_coverage",
    "check_golden_exempt_sync",
    "check_showcase_metric",
    "check_slug_lowercase",
    "check_anchor_in_products",
    "check_products_match_cohort",
    "check_unfilled_authoring",
]

#: Уровень правила: блокирует прогон, только предупреждает или помечает неполноту (A3).
#: ``incomplete`` — авторские поля ещё не заполнены человеком: не блокирует создание и
#: правку пакета, но запрещает прогон (см. :attr:`LintReport.ready`).
Severity = Literal["blocking", "warning", "incomplete"]

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
    """Отчёт линтера: блокирующие нарушения, предупреждения и неполнота (A3).

    ``ok`` = нет блокирующих. :meth:`raise_if_blocking` превращает блокирующие
    сообщения в :class:`ValueError` (сводки для CLI, ``03`` §E.2). ``incomplete`` —
    авторские поля, ещё не заполненные человеком: отдельный уровень, не блокирующий
    создание/правку пакета, но блокирующий прогон (:attr:`ready`).
    """

    blocking: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Прогон разрешён: нет ни одного блокирующего нарушения."""
        return not self.blocking

    @property
    def complete(self) -> bool:
        """Все обязательные авторские поля заполнены человеком (A3)."""
        return not self.incomplete

    @property
    def ready(self) -> bool:
        """Пакет готов к прогону: нет блокирующих нарушений И нет незаполненных полей."""
        return not self.blocking and not self.incomplete

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
    """§11.6: presence-предупреждения golden-набора (тиры fast/full).

    Учитывает **объявленное** состояние эталона (:mod:`core.pipeline.golden_status`):

    * ``present`` — прежнее поведение (пустой набор и недостающие тиры — warning);
    * ``absent``/``declined`` **с объявлением** — не warning: это законный режим
      («сверять не с чем» / «эталон объявлен непригодным»), а не недочёт пакета;
    * ``absent`` **без объявления** (и ``golden_status is None``, т.е. входы собраны в
      обход :func:`~core.pipeline.normalize.load_inputs`) — warning остаётся: молчаливое
      отсутствие обязано быть видимым.
    """
    status = inputs.golden_status
    if status is not None:
        if status.state == STATE_DECLINED:
            return []
        if status.state == STATE_ABSENT and status.declared:
            return []
    if not inputs.golden:
        if status is not None and not status.file_present:
            return [
                "§11.6: golden.csv отсутствует и эталон не объявлен — golden-сеть "
                "отсутствует (объявите state: absent в sources/golden_status.yaml, "
                "если сверка намеренно не выполняется)"
            ]
        return ["§11.6: golden.csv пуст — golden-сеть отсутствует"]
    messages: list[str] = []
    if not any(row.tier is GoldenTier.FAST for row in inputs.golden):
        messages.append("§11.6: отсутствует тир golden-fast")
    if not any(row.tier is GoldenTier.FULL for row in inputs.golden):
        messages.append("§11.6: отсутствует тир golden-full")
    return messages


def check_golden_coverage(inputs: RunInputs) -> list[str]:
    """§11.6: у каждого продукта когорты есть golden-строки **или** объявленный waiver.

    Правило исполняет требование, которое до сих пор было надписью: «эталон есть» ничего
    не говорило о том, покрывает ли он когорту. Продукт без единой golden-строки раньше
    просто не проверялся — и это было неотличимо от «строки потеряли».

    Применимость: только когда сверка реально идёт, то есть golden-строки есть
    (``state: present`` либо выведенное ``present``). При ``absent``/``declined``
    строк нет по объявлению, сверки нет — и требовать покрытие не по чему.

    Уровень — **``warning``, не блокирующий** (решение владельца 29.10.2026). Причина —
    статус самого инструмента: ``golden.csv`` не входит в ``execution_contract_digest``,
    не влияет на вердикты и на карту (§11.3), то есть не является условием измерения.
    Гейт, который останавливает прогон, объявлял бы проверочный инструмент условием
    замера — и делал бы невозможным первый замер продукта: эталон аннотируют **по**
    результату первого прогона, ответного ключа до него не существует (так и появился
    пятый продукт когорты — прогон, затем аннотация). Поэтому неполнота здесь
    **видима**, но не запрещает измерять: предупреждение печатают ``plan``/``run``
    (в т.ч. в ``--json``), а объявленный waiver дополнительно виден в отчёте
    (``golden_exempt_product``).

    Рассинхронизация объявления с данными (waiver для продукта вне когорты или для
    продукта, у которого строки есть) — отдельное правило
    :func:`check_golden_exempt_sync`: это уже не неполнота, а устаревшее объявление.
    """
    sets = _golden_coverage_sets(inputs)
    if sets is None:
        return []
    products, covered, exempt = sets
    return [
        f"§11.6: у продукта '{product_id}' когорты нет ни одной строки golden.csv; "
        "объявите эталон для него либо waiver в sources/golden_status.yaml "
        "(exempt_products: product_id, reason, declared_by)"
        for product_id in products
        if product_id not in covered and product_id not in exempt
    ]


def check_golden_exempt_sync(inputs: RunInputs) -> list[str]:
    """§11.6: waiver'ы по продуктам обязаны быть согласованы с когортой и golden.csv.

    Waiver — **объявление**, и устаревшее объявление хуже отсутствующего: оно называет
    решение, которого больше нет. Два случая, оба ``warning`` (не блокируют прогон —
    сверка идёт, просто объявление врёт):

    * waiver для продукта, которого нет в когорте (продукт убрали, объявление осталось);
    * waiver для продукта, у которого golden-строки **есть** (эталон появился, waiver
      не сняли).

    Правило не срабатывает без объявления (``golden_status is None`` — входы собраны в
    обход :func:`~core.pipeline.normalize.load_inputs`).
    """
    status = inputs.golden_status
    if status is None or not status.exempt_products:
        return []
    products = {entry.product_id for entry in inputs.cohort.entries}
    covered = {row.product_id for row in inputs.golden}
    messages = [
        f"§11.6: waiver продукта '{product_id}' объявлен в "
        "sources/golden_status.yaml (exempt_products), но продукта нет в когорте — "
        "объявление рассинхронизировано с составом когорты"
        for product_id in sorted(status.exempt_ids - products)
    ]
    messages.extend(
        f"§11.6: waiver продукта '{product_id}' объявлен, но golden-строки для него "
        "есть — уберите waiver либо строки: объявление рассинхронизировано с golden.csv"
        for product_id in sorted(status.exempt_ids & covered)
    )
    return messages


def _golden_coverage_sets(
    inputs: RunInputs,
) -> tuple[list[str], set[str], set[str]] | None:
    """Материал правил покрытия: ``(продукты когорты, покрытые, waiver'ы)`` или ``None``.

    ``None`` — сверка не идёт (строк эталона нет: ``absent``/``declined`` либо набор
    пуст): тогда требовать покрытие не по чему.
    """
    status = inputs.golden_status
    if status is not None and not status.comparison_available:
        return None
    if not inputs.golden:
        return None
    products = sorted({entry.product_id for entry in inputs.cohort.entries})
    covered = {row.product_id for row in inputs.golden}
    exempt = set(status.exempt_ids) if status is not None else set()
    return products, covered, exempt


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


def check_unfilled_authoring(inputs: RunInputs) -> list[str]:
    """A3: обязательные авторские поля, ещё не заполненные человеком.

    Незаполненность выражается явно (``None`` у опциональных полей роли/когорты) и не
    подменяется правдоподобным текстом-заглушкой.
    Возвращает детерминированный список сообщений ``A3:`` — прогон такого пакета
    запрещён (:attr:`LintReport.ready`), но создание/правка разрешены.

    Покрытые поля: ``RoleDef.definition`` (пусто/``None``) и
    ``CohortEntry.selection_reason``. Обязательные непустые CSV-поля каталога
    (``title``/``rule``) отклоняются загрузчиком (``run_config._req``) до линтера.
    """
    messages: list[str] = []
    for role in inputs.package.roles:
        if not (role.definition or "").strip():
            messages.append(
                f"A3: роль '{role.role_id}' не заполнена автором (definition)"
            )
    for entry in inputs.cohort.entries:
        if not (entry.selection_reason or "").strip():
            messages.append(
                f"A3: продукт '{entry.product_id}' когорты: обоснование выбора не "
                "заполнено автором (selection_reason)"
            )
    return sorted(messages)


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
    LintRule("unfilled-authoring", "incomplete", check_unfilled_authoring),
    LintRule("golden-coverage", "warning", check_golden_coverage),
    LintRule("empty-concept-terms", "warning", check_empty_concept_terms),
    LintRule("golden-presence", "warning", check_golden_presence),
    LintRule("golden-exempt-sync", "warning", check_golden_exempt_sync),
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
        elif rule.severity == "incomplete":
            report.incomplete.extend(messages)
        else:
            report.warnings.extend(messages)
    return report


def assert_lint(inputs: RunInputs) -> LintReport:
    """Линтует и поднимает :class:`ValueError`, если есть блокирующие нарушения."""
    report = lint_run_inputs(inputs)
    report.raise_if_blocking()
    return report
