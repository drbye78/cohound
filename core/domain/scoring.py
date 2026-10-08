"""coHound — расчёт показателей §6 (L0): чистые функции над доменными объектами.

Никакого доступа к БД и I/O: загрузка ячеек/каталога — в pipeline (`pipeline/compute.py`).
Реализует Resolved Capability, Evidence Coverage, failure_rate, Comparability,
sensitivity-анализ и модульные баллы. Нейтрален к предметной категории (§1.3).

Инварианты:
- I4/I5: `failed` не является статусом и вне числителя Capability, но **входит** в
  знаменатель Coverage как неразрешённое измерение (сбои не завышают покрытие).
- I5/I13: `❓` (unknown) — не отрицательное свидетельство; вне числителя и знаменателя
  Capability (при политике ``excluded``); `preliminary` вне всех расчётов.
- I7: `N/A` — только человеческое решение; исключается из обеих метрик.
- K6/A3: ранжирование демотируется по sensitivity независимо от запрошенного режима.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from core.domain.enums import (
    CapabilityPolicy,
    CellState,
    ComparabilityLevel,
    Importance,
    RankingPolicy,
    Status,
    UnknownPolicy,
)
from core.domain.models import AlignmentReport, CatalogRow, Cell, Cohort

__all__ = [
    "Score",
    "Coverage",
    "ModuleScore",
    "Comparability",
    "SensitivityReport",
    "resolved_capability",
    "evidence_coverage",
    "failure_rate",
    "comparability",
    "sensitivity",
    "module_scores",
]

_EPS = 1e-9

#: Числовой вес важности параметра — **provisional** (в регламенте §6 значения не
#: зафиксированы). Используется как «объявленный» вес по умолчанию, когда для
#: параметра/модуля не передан явный числовой вес. Порядок ординален: Кр > Вж > Баз > Опц.
IMPORTANCE_WEIGHTS: dict[Importance, float] = {
    Importance.CRITICAL: 4.0,
    Importance.IMPORTANT: 3.0,
    Importance.BASIC: 2.0,
    Importance.OPTIONAL: 1.0,
}

#: Статусы, дающие вклад в Resolved Capability (нумеруемый набор).
_RESOLVED_STATUSES: frozenset[Status] = frozenset({Status.OK, Status.PARTIAL, Status.ABSENT})
#: Применимые статусы для Coverage — включая `❓`.
_APPLICABLE_STATUSES: frozenset[Status] = _RESOLVED_STATUSES | {Status.UNKNOWN}
#: Состояния ячейки, считающиеся разрешённым результатом измерения.
_MEASURED_STATES: frozenset[CellState] = frozenset({CellState.EVALUATED, CellState.REVIEWED})

_DEFAULT_PARTIALS: tuple[float, ...] = (0.25, 0.5, 0.75)
_DEFAULT_UNKNOWN: tuple[UnknownPolicy, ...] = (
    UnknownPolicy.EXCLUDED,
    UnknownPolicy.UNKNOWN_AS_ZERO,
)
_DEFAULT_WEIGHT_MODES: tuple[str, ...] = ("declared", "uniform")


# ---------------------------------------------------------------------------
# Результаты (frozen dataclass)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Score:
    """Resolved Capability: ``value`` ∈ [0, 1] вместе с числителем/знаменателем и n_used."""

    value: float
    numerator: float
    denominator: float
    n_used: int


@dataclass(frozen=True)
class Coverage:
    """Evidence Coverage: ``value`` ∈ [0, 1] (полнота прохождения протокола)."""

    value: float
    numerator: float
    denominator: float
    n_used: int


@dataclass(frozen=True)
class ModuleScore:
    """Модульный балл и покрытие; ``applicable_weight`` — вес, перенормированный на применимые."""

    module: str
    score: float
    coverage: float
    weight: float
    applicable_weight: float


@dataclass(frozen=True)
class Comparability:
    """Кросс-продуктовая сопоставимость: уровень по min Coverage + флаг cross-cycle."""

    level: str
    min_coverage: float
    cross_cycle: bool
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SensitivityReport:
    """Пост-хок sensitivity-анализ по осям §6."""

    stable: bool
    per_axis: dict[str, Any]
    unstable_pairs: list[tuple[str, str]]
    requested_ranking: str
    computed_ranking: str


# ---------------------------------------------------------------------------
# Внутренние помощники
# ---------------------------------------------------------------------------


def _cell_weight(
    cell: Cell,
    params: Mapping[str, CatalogRow] | None,
    weights: Mapping[str, float] | None,
    *,
    uniform: bool = False,
) -> float:
    """Вес ячейки: per-param override → per-module → важность из каталога → 1.0.

    ``uniform=True`` игнорирует и объявленные веса, и важность (все ячейки равны) —
    это ось «uniform» sensitivity-анализа.
    """
    if uniform:
        return 1.0
    if weights:
        if cell.param_id in weights:
            return float(weights[cell.param_id])
    row = params.get(cell.param_id) if params else None
    if row is not None and weights and row.module in weights:
        return float(weights[row.module])
    if row is not None:
        return float(IMPORTANCE_WEIGHTS[row.importance])
    return 1.0


def _status_value(status: Status, partial_score: float) -> float:
    """Технический коэффициент агрегирования порядкового статуса (§6)."""
    if status is Status.OK:
        return 1.0
    if status is Status.PARTIAL:
        return float(partial_score)
    # ABSENT (0.0) и UNKNOWN при unknown_as_zero (0.0).
    return 0.0


def _is_measured(cell: Cell) -> bool:
    return cell.state in _MEASURED_STATES


def _resolved_capability(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow] | None,
    weights: Mapping[str, float] | None,
    *,
    partial_score: float = 0.5,
    unknown_policy: UnknownPolicy = UnknownPolicy.EXCLUDED,
    uniform: bool = False,
) -> Score:
    """Ядро Resolved Capability с явными partial/unknown-политиками (для sensitivity)."""
    include_unknown = unknown_policy is UnknownPolicy.UNKNOWN_AS_ZERO
    allowed = _APPLICABLE_STATUSES if include_unknown else _RESOLVED_STATUSES
    numerator = 0.0
    denominator = 0.0
    n_used = 0
    for cell in cells:
        status = cell.status
        if not _is_measured(cell) or status is None or status not in allowed:
            continue
        w = _cell_weight(cell, params, weights, uniform=uniform)
        numerator += w * _status_value(status, partial_score)
        denominator += w
        n_used += 1
    value = numerator / denominator if denominator > 0 else 0.0
    return Score(value=value, numerator=numerator, denominator=denominator, n_used=n_used)


def _normalize_axes(axes: Mapping[str, Iterable[Any]] | None) -> dict[str, list[Any]]:
    """Нормализует оси sensitivity: алиасы ключей + значения по умолчанию."""
    alias = {
        "warn": "partial_score",
        "warn_coefficient": "partial_score",
        "partial": "partial_score",
        "unknown": "unknown_policy",
        "weight": "weights",
        "weight_mode": "weights",
    }
    if axes is None:
        return {
            "partial_score": list(_DEFAULT_PARTIALS),
            "unknown_policy": list(_DEFAULT_UNKNOWN),
            "weights": list(_DEFAULT_WEIGHT_MODES),
        }
    out: dict[str, list[Any]] = {}
    for key, values in axes.items():
        out[alias.get(key, key)] = list(values)
    return out


def _cohort_product_ids(cohort: Any) -> list[str]:
    """Извлекает product_id из когорты (или итерируемого набора идентификаторов)."""
    if cohort is None:
        return []
    entries = getattr(cohort, "entries", None)
    if entries is not None:
        return [e.product_id if hasattr(e, "product_id") else str(e) for e in entries]
    return [e.product_id if hasattr(e, "product_id") else str(e) for e in cohort]


def _sign(value: float) -> int:
    if value > _EPS:
        return 1
    if value < -_EPS:
        return -1
    return 0


def _norm_coverage(value: float) -> float:
    """Принимает долю 0..1 либо проценты (>1 → /100); возвращает долю, зажатую в [0, 1]."""
    x = float(value)
    if x > 1.0:
        x = x / 100.0
    return max(0.0, min(1.0, x))


def _level_for(min_coverage: float) -> ComparabilityLevel:
    if min_coverage >= 0.90:
        return ComparabilityLevel.HIGH
    if min_coverage >= 0.75:
        return ComparabilityLevel.MEDIUM
    if min_coverage >= 0.50:
        return ComparabilityLevel.LOW
    return ComparabilityLevel.INSUFFICIENT


def _downgrade(level: ComparabilityLevel) -> ComparabilityLevel:
    """Понижение на один уровень: HIGH→MEDIUM→LOW→INSUFFICIENT (INSUFFICIENT остаётся)."""
    order = {
        ComparabilityLevel.HIGH: ComparabilityLevel.MEDIUM,
        ComparabilityLevel.MEDIUM: ComparabilityLevel.LOW,
        ComparabilityLevel.LOW: ComparabilityLevel.INSUFFICIENT,
        ComparabilityLevel.INSUFFICIENT: ComparabilityLevel.INSUFFICIENT,
    }
    return order[level]


# ---------------------------------------------------------------------------
# Публичные функции §6
# ---------------------------------------------------------------------------


def resolved_capability(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
    policy: str = CapabilityPolicy.RESOLVED_ONLY_V1.value,
) -> Score:
    """Resolved Capability (§6): «из исследованного подтверждено N%».

    Числитель — взвешенная сумма ✅=1, ⚠️=partial_score, ❌=0; знаменатель — сумма весов
    тех же разрешённых ячеек. `❓` отсутствует и в числителе, и в знаменателе (не
    отрицательное свидетельство); `N/A` и `failed` исключены (I4/I5/I7). Пустой вход → 0.0.
    """
    policy_value = policy.value if isinstance(policy, CapabilityPolicy) else str(policy)
    if policy_value != CapabilityPolicy.RESOLVED_ONLY_V1.value:
        raise ValueError(f"неизвестная capability_policy: {policy!r}")
    return _resolved_capability(cells, params, weights)


def evidence_coverage(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
) -> Coverage:
    """Evidence Coverage (§6, I5, R13): полнота прохождения протокола, а не истинность.

    Числитель — разрешённые ячейки {✅,⚠️,❌}; знаменатель — применимые {✅,⚠️,❌,❓}
    **плюс** `failed`. `N/A` вне знаменателя. Пустой вход → 0.0.
    """
    numerator = 0.0
    denominator = 0.0
    n_used = 0
    for cell in cells:
        if _is_measured(cell) and cell.status in _APPLICABLE_STATUSES:
            w = _cell_weight(cell, params, weights)
            denominator += w
            if cell.status in _RESOLVED_STATUSES:
                numerator += w
            n_used += 1
        elif cell.state is CellState.FAILED:
            # I5: сбой — неразрешённое измерение; входит в знаменатель.
            denominator += _cell_weight(cell, params, weights)
            n_used += 1
    value = numerator / denominator if denominator > 0 else 0.0
    return Coverage(value=value, numerator=numerator, denominator=denominator, n_used=n_used)


def failure_rate(cells: Iterable[Cell]) -> float:
    """failure_rate = failed / (evaluated ∪ failed) — диагностика надёжности (§6, R13)."""
    failed = 0
    evaluated = 0
    for cell in cells:
        if cell.state is CellState.FAILED:
            failed += 1
        elif cell.state in _MEASURED_STATES:
            evaluated += 1
    total = evaluated + failed
    return failed / total if total > 0 else 0.0


def comparability(
    cohort: Cohort,
    per_product_coverage: Mapping[str, float],
    alignment: AlignmentReport | None = None,
) -> Comparability:
    """Кросс-продуктовая сопоставимость (§6).

    Уровень по **минимальному** Coverage среди продуктов когорты: HIGH ≥90 · MEDIUM 75–89.9 ·
    LOW 50–74.9 · INSUFFICIENT <50 (принимается доля 0..1 либо проценты >1). При ``alignment``,
    равном None, при непустых ``violations`` либо рассинхронизации edition/jurisdiction —
    ``cross_cycle=True`` и понижение уровня на один шаг с явной причиной.
    """
    product_ids = _cohort_product_ids(cohort)
    if product_ids:
        coverages = [_norm_coverage(per_product_coverage.get(pid, 0.0)) for pid in product_ids]
    else:
        coverages = [_norm_coverage(v) for v in per_product_coverage.values()]
    min_coverage = min(coverages) if coverages else 0.0

    level = _level_for(min_coverage)
    reasons: list[str] = []
    cross_cycle = False
    if alignment is None:
        cross_cycle = True
        reasons.append("alignment_missing")
    else:
        reasons.extend(alignment.violations)
        if not alignment.editions_consistent:
            reasons.append("editions_inconsistent")
        if not alignment.jurisdictions_consistent:
            reasons.append("jurisdictions_inconsistent")
        cross_cycle = bool(reasons)

    if cross_cycle:
        level = _downgrade(level)
    return Comparability(
        level=level.value,
        min_coverage=min_coverage,
        cross_cycle=cross_cycle,
        reasons=reasons,
    )


def _product_scores(
    by_product: Mapping[str, list[Cell]],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
    *,
    partial_score: float,
    unknown_policy: UnknownPolicy,
    weight_mode: str,
) -> dict[str, float]:
    uniform = weight_mode == "uniform"
    return {
        product: _resolved_capability(
            product_cells,
            params,
            weights,
            partial_score=partial_score,
            unknown_policy=unknown_policy,
            uniform=uniform,
        ).value
        for product, product_cells in by_product.items()
    }


def _pair_signs(scores: Mapping[str, float]) -> dict[tuple[str, str], int]:
    products = sorted(scores)
    return {
        (a, b): _sign(scores[a] - scores[b])
        for a, b in itertools.combinations(products, 2)
    }


def _is_order_stable(rankings: list[Mapping[str, float]]) -> tuple[bool, list[tuple[str, str]]]:
    combined: dict[tuple[str, str], set[int]] = {}
    for scores in rankings:
        for pair, sign in _pair_signs(scores).items():
            combined.setdefault(pair, set()).add(sign)
    unstable = sorted(pair for pair, signs in combined.items() if len(signs) > 1)
    return (not unstable), unstable


def sensitivity(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
    axes: Mapping[str, Iterable[Any]] | None = None,
) -> SensitivityReport:
    """Sensitivity-анализ §6 по осям {partial_score, unknown_policy, веса}.

    Пересчитывает Capability каждого продукта для всех комбинаций осей. Ранжирование
    **устойчиво** тогда и только тогда, когда относительный порядок каждой пары продуктов
    одинаков во всех комбинациях. Неустойчивость → `computed_ranking = coverage_fallback`
    (демотирование; запрошенный режим его не переопределяет — K6/A3).
    """
    cell_list = list(cells)
    spec = _normalize_axes(axes)
    products = sorted({c.product_id for c in cell_list})
    by_product = {p: [c for c in cell_list if c.product_id == p] for p in products}

    # Полный факториал по всем осям.
    full_rankings: list[Mapping[str, float]] = []
    for partial, unknown, weight_mode in itertools.product(
        spec.get("partial_score", [0.5]),
        spec.get("unknown_policy", [UnknownPolicy.EXCLUDED]),
        spec.get("weights", ["declared"]),
    ):
        full_rankings.append(
            _product_scores(
                by_product,
                params,
                weights,
                partial_score=float(partial),
                unknown_policy=unknown if isinstance(unknown, UnknownPolicy) else UnknownPolicy(str(unknown)),
                weight_mode=str(weight_mode),
            )
        )

    stable, unstable_pairs = _is_order_stable(full_rankings)

    # Детализация по каждой оси (прочие оси — в дефолте).
    per_axis: dict[str, Any] = {}
    default_combo = {
        "partial_score": 0.5,
        "unknown_policy": UnknownPolicy.EXCLUDED,
        "weights": "declared",
    }
    for axis, values in spec.items():
        axis_rankings: list[Mapping[str, float]] = []
        rankings_by_value: dict[str, list[str]] = {}
        for value in values:
            combo = dict(default_combo)
            combo[axis] = value
            scores = _product_scores(
                by_product,
                params,
                weights,
                partial_score=float(combo["partial_score"]),
                unknown_policy=combo["unknown_policy"]
                if isinstance(combo["unknown_policy"], UnknownPolicy)
                else UnknownPolicy(str(combo["unknown_policy"])),
                weight_mode=str(combo["weights"]),
            )
            axis_rankings.append(scores)
            rankings_by_value[str(value)] = sorted(scores, key=lambda p: (-scores[p], p))
        axis_stable, _ = _is_order_stable(axis_rankings)
        per_axis[axis] = {
            "values": [str(v) for v in values],
            "rankings": rankings_by_value,
            "stable": axis_stable,
        }

    return SensitivityReport(
        stable=stable,
        per_axis=per_axis,
        unstable_pairs=unstable_pairs,
        requested_ranking=RankingPolicy.CAPABILITY_RANKED.value,
        computed_ranking=(
            RankingPolicy.CAPABILITY_RANKED.value
            if stable
            else RankingPolicy.COVERAGE_FALLBACK.value
        ),
    )


def module_scores(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
) -> list[ModuleScore]:
    """Модульные баллы (§6): Σ importance×score / Σ importance(resolved) по модулю.

    Веса модулей перенормируются на **применимые** модули (у которых знаменатель Coverage > 0);
    у неприменимого модуля ``applicable_weight`` = 0. Пустой вход → пустой список.
    """
    by_module: dict[str, list[Cell]] = {}
    for cell in cells:
        row = params.get(cell.param_id) if params else None
        if row is None:
            continue
        by_module.setdefault(row.module, []).append(cell)

    records: list[tuple[str, float, Score, Coverage]] = []
    for module, module_cells in by_module.items():
        capability = _resolved_capability(module_cells, params, weights)
        coverage = evidence_coverage(module_cells, params, weights)
        weight = float(weights[module]) if weights and module in weights else 1.0
        records.append((module, weight, capability, coverage))

    applicable_total = sum(
        weight for _module, weight, _cap, cov in records if cov.n_used > 0
    )
    result: list[ModuleScore] = []
    for module, weight, capability, coverage in records:
        if coverage.n_used > 0 and applicable_total > 0:
            applicable_weight = weight / applicable_total
        else:
            applicable_weight = 0.0
        result.append(
            ModuleScore(
                module=module,
                score=capability.value,
                coverage=coverage.value,
                weight=weight,
                applicable_weight=applicable_weight,
            )
        )
    result.sort(key=lambda item: item.module)
    return result
