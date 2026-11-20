"""Достижимость эталонных ``❌`` при объявленном каталоге и методологии (I2b/I2d).

Вопрос. ``golden.csv`` размечен как истина о продуктах («этого у продукта нет»), а
``agreement`` считает совпадением только точное равенство статусов
(:func:`core.pipeline.golden._classify`). После пере-квалификации каталога 18.11.2026
(``explicit`` 181 → 0) отсутствие доказуемо только исчерпанием объявленного пространства
(класс ``bounded``, I2b) с датированным снятием **каждого** объявленного корня (I2d) —
либо реестром. Значит для части эталонных ``❌`` методология **не позволяет** вердикт при
любом качестве понимания: лучший возможный исход там — ``❓``. Этот модуль отвечает,
сколько таких ячеек и каков из-за них потолок согласия.

Что он **не** делает. Не пересчитывает вердикты и не правит эталон: опубликованные ячейки
прогона читаются как **факт артефакта** («что этот прогон сказал»), эталон — как
объявленная истина, каталог — как объявление правила. Три источника не смешиваются.

Достижимость ``❌`` считается **доменной функцией**, а не копией правила:
:func:`core.domain.invariants.check_absence_dated` — то же, чем проверяет I2d в конвейере.
Копия разошлась бы с правилом при первой правке, и потолок описывал бы методологию,
которой нет.

Три исхода на ячейку эталона:

* ``reachable`` — ``❌`` достижим: правило ``registry_absence`` (носитель не нужен) либо
  все объявленные корни продукта удостоверены снятием;
* ``unreachable`` — ``❌`` недостижим: носителя нет (причина называется отдельно —
  ``policy_skipped``, когда корпус пришёл по объявленной политике без обхода, или
  ``carrier_missing``, когда обход был, но не исчерпал корни);
* ``unknown`` — правило ``explicit``: доказательство зависит от цитаты конкретной ячейки,
  до вердикта невычислимо. Такие ячейки из потолка **исключаются** и называются числом,
  а не молча приравниваются к достижимым.

Потолок — **наблюдение**, а не порог: :func:`core.pipeline.golden.check_thresholds` и
``GoldenReport.passes`` он не трогает. Публикуется рядом с метрикой согласия и решает
читатель.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from core.domain.enums import AbsenceRuleType, DenialStrength, Status
from core.domain.invariants import check_absence_dated
from core.domain.models import (
    CatalogRow,
    CorpusCoverage,
    CorpusRootCoverage,
    GoldenRow,
)

__all__ = [
    "BOUNDED_RULES",
    "CARRIER_MISSING",
    "POLICY_SKIPPED",
    "REACHABLE",
    "UNKNOWN",
    "UNREACHABLE",
    "CellVerdict",
    "ProductCarrier",
    "Reachability",
    "agreement_with_ceiling",
    "carriers_from_coverage",
    "classify_reachability",
]

#: Правила, чей ``❌`` держится на исчерпании объявленного пространства (I2b).
BOUNDED_RULES = frozenset(
    {AbsenceRuleType.PARAMETER_REFERENCE, AbsenceRuleType.DISTRIBUTION_ABSENCE}
)

REACHABLE = "reachable"
UNREACHABLE = "unreachable"
UNKNOWN = "unknown"

#: Причина недостижимости, когда корпус пришёл по объявленной политике без обхода.
POLICY_SKIPPED = "policy_skipped"
#: Причина недостижимости, когда обход был, но корни не исчерпаны.
CARRIER_MISSING = "carrier_missing"

#: Причина «корпус пришёл по объявленной политике без обхода» в ``CorpusCoverage.reason``.
_POLICY_SKIPPED_REASON = "crawl_policy_skipped"


@dataclass
class ProductCarrier:
    """Носитель ``bounded`` у продукта: снятия объявленных корней.

    :param complete: ``corpus_coverage.complete`` как его записал прогон (сводка).
    :param roots: снятия корней — то, чем на самом деле проверяет I2d.
    :param skipped_by_policy: корпус получен по объявленной политике без обхода
        (``reason='crawl_policy_skipped'``) — «не смотрели по решению», а не «смотрели и
        не нашли».
    """

    complete: bool
    roots: tuple[CorpusRootCoverage, ...]
    skipped_by_policy: bool

    def bounded_ok(self) -> bool:
        """Удостоверяет ли носитель ``bounded``-отсутствие — **правилом I2d**."""
        ok, _, _ = check_absence_dated(
            denial_strength=DenialStrength.BOUNDED,
            cited_observations=[],
            roots=self.roots,
        )
        return ok


@dataclass
class CellVerdict:
    """Что прогон сказал об ячейке (факт артефакта, не пересчёт)."""

    status: Status | None
    quotes: tuple[str, ...] = ()


@dataclass
class Reachability:
    """Раскладка ячеек эталона по достижимости ``❌``.

    Счётчики ``absent_*`` считают **только эталонные** ``❌``: у ``✅``/``⚠️`` вопроса о
    носителе нет, и попадание их в те же поля читалось бы как «достижим» без проверки.
    ``cells`` — все строки эталона, потому что потолок согласия считается по всему набору.
    """

    cells: int = 0
    absent_reachable: int = 0
    absent_unknown: int = 0
    absent_unreachable_by_reason: dict[str, int] = field(default_factory=dict)
    #: ``(absence_rule, product_id, param_id)`` недостижимых — причина называется адресно.
    unreachable_cells: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def absent_unreachable(self) -> int:
        """Сколько эталонных ``❌`` недостижимо при текущем носителе."""
        return sum(self.absent_unreachable_by_reason.values())

    @property
    def absent_total(self) -> int:
        """Сколько всего эталонных ``❌`` (достижимые + неизвестные + недостижимые)."""
        return self.absent_reachable + self.absent_unknown + self.absent_unreachable

    @property
    def ceiling(self) -> float | None:
        """Потолок согласия: доля ячеек, где вердикт вообще выразим.

        Недостижимый ``❌`` даёт ``❓`` при любом качестве понимания, поэтому он вычитается
        из числителя. ``unknown`` (правило ``explicit``) вычитается и из знаменателя: там
        достижимость решает цитата конкретной ячейки, и приравнять её к нулю значило бы
        объявить недостижимым то, что не измерено (A3).

        ``None``, когда измеримого знаменателя нет: опубликовать ``0.0`` значило бы
        сказать «ничего не достижимо», хотя не измерено ничего.
        """
        denom = self.cells - self.absent_unknown
        if denom <= 0:
            return None
        return (denom - self.absent_unreachable) / denom


def classify_reachability(
    golden: list[GoldenRow],
    catalog: dict[str, CatalogRow],
    carriers: dict[str, ProductCarrier],
) -> Reachability:
    """Раскладывает **эталонные** ``❌`` по достижимости (остальные статусы не считаются).

    Достижимость считается только для ``❌``: у ``✅``/``⚠️`` вопроса о носителе нет, и
    включение их в знаменатель потолка разбавило бы число, ничего не измерив.
    """
    out = Reachability(cells=len(golden))
    for row in golden:
        if row.expected_status is not Status.ABSENT:
            continue
        param = catalog.get(row.param_id)
        rule = param.absence_rule if param is not None else AbsenceRuleType.EXPLICIT
        if rule is AbsenceRuleType.REGISTRY_ABSENCE:
            # I2b: реестр — авторитетная энумерация, снятие и подпись не нужны.
            out.absent_reachable += 1
            continue
        if rule not in BOUNDED_RULES:
            out.absent_unknown += 1
            continue
        carrier = carriers.get(row.product_id)
        if carrier is not None and carrier.bounded_ok():
            out.absent_reachable += 1
            continue
        reason = (
            POLICY_SKIPPED
            if carrier is not None and carrier.skipped_by_policy
            else CARRIER_MISSING
        )
        by_reason = out.absent_unreachable_by_reason
        by_reason[reason] = by_reason.get(reason, 0) + 1
        out.unreachable_cells.append((rule.value, row.product_id, row.param_id))
    return out


def carriers_from_coverage(
    coverages: Iterable[CorpusCoverage],
) -> dict[str, ProductCarrier]:
    """Носители ``bounded`` из записей покрытия прогона (``product_id -> носитель``).

    ``skipped_by_policy`` — не догадка: причина записана самим прогоном
    (``CorpusCoverage.reason``), поэтому «не смотрели по решению» отличается от «смотрели
    и не исчерпали» без повторного вывода.
    """
    out: dict[str, ProductCarrier] = {}
    for coverage in coverages:
        out[coverage.product_id] = ProductCarrier(
            complete=coverage.complete,
            roots=tuple(coverage.roots),
            skipped_by_policy=(coverage.reason or "") == _POLICY_SKIPPED_REASON,
        )
    return out


def agreement_with_ceiling(
    golden: list[GoldenRow],
    cells: dict[tuple[str, str], CellVerdict],
    reach: Reachability,
    catalog: dict[str, CatalogRow],
) -> dict[str, Any]:
    """Согласие эталона с прогоном — как есть и без недостижимых ``❓``.

    Разделение существенно: ``❌`` эталона, недостижимый по носителю, даёт ``❓`` при
    **любом** качестве понимания. Считать это ошибкой системы значит мерить методологию
    качеством продукта. Но и обратное неверно: если прогон на такой ячейке сказал ``✅``/
    ``⚠️``, это не «методология виновата» — он утверждает **наличие** там, где эталон
    говорит об отсутствии, и это остаётся ошибкой (или ошибкой эталона — их различает
    человек, не прибор).

    ``catalog`` принимается для симметрии с :func:`classify_reachability` и для будущих
    разрезов: согласие считается по опубликованным статусам, а не по правилу.
    """
    unreachable = {(product, param) for _, product, param in reach.unreachable_cells}
    matches = 0
    not_attributable = 0
    claims_presence = 0
    for row in golden:
        key = (row.product_id, row.param_id)
        actual = cells.get(key)
        status = actual.status if actual is not None else None
        if status is row.expected_status:
            matches += 1
            continue
        if key in unreachable and status is Status.UNKNOWN:
            # ❓ там, где ❌ недостижим, — исход методологии, а не промах прибора.
            not_attributable += 1
        elif key in unreachable and status in (Status.OK, Status.PARTIAL):
            # Утверждение наличия там, где эталон говорит об отсутствии: методология тут
            # ни при чём — вердикт не тот, который она запрещает.
            claims_presence += 1
    denom = len(golden) - not_attributable
    return {
        "golden": len(golden),
        "matches": matches,
        "agreement": matches / len(golden) if golden else 0.0,
        "not_attributable": not_attributable,
        "claims_presence_where_golden_absent": claims_presence,
        "attestable_denominator": denom,
        "attestable_agreement": matches / denom if denom else 0.0,
        "ceiling": reach.ceiling,
        # Доля от потолка не определена, когда потолок не измерен (``None``) или равен
        # нулю: делить наблюдённое согласие не на что.
        "share_of_ceiling": (
            (matches / len(golden)) / reach.ceiling if reach.ceiling else None
        ),
    }
