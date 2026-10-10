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
from collections.abc import Iterable, Mapping, Sequence
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
    """Evidence Coverage: полнота прохождения протокола по вселенной scope (§6/B-3a).

    ``value`` ∈ [0, 1] либо ``None`` ⇔ ``UNDEFINED`` (в universe-режиме при
    ``universe = 0``: 0.0 читалось бы как «ничего не покрыто»). ``denominator`` и
    ``universe`` несут вес вселенной scope; ``completed`` — взвешенный числитель
    завершённых измерений (``evaluated/reviewed`` без N/A + ``failed``), т.е. доля
    завершённости есть ``completed / universe``. ``defined`` различает вырожденную
    вселенную от валидного покрытия. В legacy-режиме (``universe is None``) поля
    ``universe``/``completed`` равны 0.0, ``defined`` — True, ``value`` — float.
    """

    value: float | None
    numerator: float
    denominator: float
    n_used: int
    universe: float = 0.0
    completed: float = 0.0
    defined: bool = True


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
    ranking_epsilon: float = 0.0


# ---------------------------------------------------------------------------
# Внутренние помощники
# ---------------------------------------------------------------------------


def _param_weight(
    param_id: str,
    params: Mapping[str, CatalogRow] | None,
    weights: Mapping[str, float] | None,
) -> float:
    """Вес параметра: per-param override → per-module → важность каталога → 1.0.

    Используется для синтетических пар ``(product_id, param_id)`` universe-режима,
    у которых нет ячейки (``pending``/отсутствующая), но которым нужен тот же вес,
    что и у реальной ячейки (см. :func:`_cell_weight`).
    """
    if weights and param_id in weights:
        return float(weights[param_id])
    row = params.get(param_id) if params else None
    if row is not None and weights and row.module in weights:
        return float(weights[row.module])
    if row is not None:
        return float(IMPORTANCE_WEIGHTS[row.importance])
    return 1.0


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
    return _param_weight(cell.param_id, params, weights)


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
    *,
    universe: Iterable[tuple[str, str]] | None = None,
) -> Coverage:
    """Evidence Coverage (§6, I5, R13): полнота прохождения протокола, а не истинность.

    **Legacy-режим** (``universe is None``): числитель — разрешённые ячейки {✅,⚠️,❌};
    знаменатель — применимые {✅,⚠️,❌,❓} **плюс** `failed`; `N/A` вне знаменателя;
    `preliminary` игнорируется. Пустой вход → ``value = 0.0``.

    **Universe-режим** (§6/B-3a, v2.1, ``universe`` — множество пар ``(product_id,
    param_id)`` scope): знаменатель — **вся вселенная scope**, а не только существующие
    ячейки. По каждой паре:

    * ``N/A`` — исключена (человеческое решение о неприменимости, I7);
    * ``preliminary`` — исключена (I13: вне всех расчётов, включая Coverage);
    * ``evaluated/reviewed`` с применимым статусом {✅,⚠️,❌,❓} → ``+w`` в universe,
      ``+w`` в числитель при разрешённом {✅,⚠️,❌};
    * ``failed`` → ``+w`` в universe (неразрешённое измерение, I5);
    * ``pending`` или отсутствующая ячейка → ``+w`` в universe (неразрешённая).

    ``completed`` — взвешенный числитель завершённых измерений (``evaluated/reviewed``
    без N/A + ``failed``); опубликованная доля завершённости — ``completed / universe``.
    ``universe = 0`` → ``value = None`` (``UNDEFINED``), ``defined = False``.
    """
    cell_list = list(cells)
    if universe is None:
        numerator = 0.0
        denominator = 0.0
        n_used = 0
        for cell in cell_list:
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
        return Coverage(
            value=value,
            numerator=numerator,
            denominator=denominator,
            n_used=n_used,
        )

    by_key: dict[tuple[str, str], Cell] = {
        (cell.product_id, cell.param_id): cell for cell in cell_list
    }
    numerator = 0.0
    universe_weight = 0.0
    completed = 0.0
    n_used = 0
    for product_id, param_id in universe:
        w = _param_weight(param_id, params, weights)
        cell = by_key.get((product_id, param_id))
        if cell is None:
            # Отсутствующая в прогоне ячейка scope — неразрешённая.
            universe_weight += w
            continue
        if cell.status is Status.NOT_APPLICABLE:
            continue  # N/A вне знаменателя (I7)
        if cell.state is CellState.PRELIMINARY:
            continue  # I13: preliminary вне всех расчётов
        if cell.state in _MEASURED_STATES and cell.status in _APPLICABLE_STATUSES:
            universe_weight += w
            completed += w
            n_used += 1
            if cell.status in _RESOLVED_STATUSES:
                numerator += w
        elif cell.state is CellState.FAILED:
            universe_weight += w
            completed += w
            n_used += 1
        else:
            # pending (или иное неразрешённое) — в знаменателе как неразрешённая.
            universe_weight += w

    defined = universe_weight > 0
    if defined:
        value: float | None = numerator / universe_weight
    else:
        value = None
        completed = 0.0
    return Coverage(
        value=value,
        numerator=numerator,
        denominator=universe_weight,
        n_used=n_used,
        universe=universe_weight,
        completed=completed,
        defined=defined,
    )


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


#: Порядок ранжирования для join ``requested × computed`` (K6): итог — ``min``.
_RANKING_ORDER: dict[str, int] = {
    RankingPolicy.UNRANKED.value: 0,
    RankingPolicy.COVERAGE_FALLBACK.value: 1,
    RankingPolicy.CAPABILITY_RANKED.value: 2,
}


def _join_ranking(requested: str, stability: RankingPolicy) -> str:
    """Итоговый допустимый ранг = ``min(requested, computed)`` по порядку K6.

    Демотирование sensitivity (``stability``) не может быть повышено запрошенным
    режимом; запрос более слабого ранга (``unranked``) сохраняется. Неизвестный
    запрошенный режим трактуется как ``capability_ranked``.
    """
    req_value = _RANKING_ORDER.get(
        requested, _RANKING_ORDER[RankingPolicy.CAPABILITY_RANKED.value]
    )
    joined = min(req_value, _RANKING_ORDER[stability.value])
    for ranking, order in _RANKING_ORDER.items():
        if order == joined:
            return ranking
    return RankingPolicy.CAPABILITY_RANKED.value


def _is_order_stable(
    rankings: Sequence[Mapping[str, float]], epsilon: float = 0.0
) -> tuple[bool, list[tuple[str, str]]]:
    """Устойчив ли относительный порядок пар во всех комбинациях осей (§6/B-1).

    Для каждой пары продуктов и каждой комбинации осей ``tol = epsilon * (max - min
    числового ряда этой комбинации)``. Если на **любой** комбинации ``|a - b| <= tol +
    _EPS``, пара **неразличима** и в подсчёт не входит (порог ``ranking_epsilon``,
    дефолт 0 — точное равенство). Иначе собираются знаки разностей; неустойчивость ⟺
    более одного знака у пары.
    """
    combined: dict[tuple[str, str], set[int]] = {}
    skipped: set[tuple[str, str]] = set()
    for scores in rankings:
        values = list(scores.values())
        span = (max(values) - min(values)) if values else 0.0
        tol = epsilon * span
        for pair in itertools.combinations(sorted(scores), 2):
            difference = scores[pair[0]] - scores[pair[1]]
            if abs(difference) <= tol + _EPS:
                skipped.add(pair)
                continue
            combined.setdefault(pair, set()).add(_sign(difference))
    unstable = sorted(
        pair
        for pair, signs in combined.items()
        if pair not in skipped and len(signs) > 1
    )
    return (not unstable), unstable


def sensitivity(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
    axes: Mapping[str, Iterable[Any]] | None = None,
    *,
    requested_ranking: RankingPolicy | str = RankingPolicy.CAPABILITY_RANKED,
    ranking_epsilon: float = 0.0,
) -> SensitivityReport:
    """Sensitivity-анализ §6 по осям {partial_score, unknown_policy, веса}.

    Пересчитывает Capability каждого продукта для всех комбинаций осей. Ранжирование
    **устойчиво** тогда и только тогда, когда относительный порядок каждой пары продуктов
    одинаков во всех комбинациях (пары в пределах порога неразличимости
    ``ranking_epsilon`` — доли диапазона агрегата — в подсчёт не входят, §6/B-1).
    Неустойчивость демотирует ``computed_ranking``; запрошенный режим не повышает
    демотированный ранг: итог — ``min(requested, computed)`` (K6). ``requested_ranking``
    сохраняется как передан (в ``.value``).
    """
    requested_value = (
        requested_ranking.value
        if isinstance(requested_ranking, RankingPolicy)
        else str(requested_ranking)
    )
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

    stable, unstable_pairs = _is_order_stable(full_rankings, ranking_epsilon)

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
        axis_stable, _ = _is_order_stable(axis_rankings, ranking_epsilon)
        per_axis[axis] = {
            "values": [str(v) for v in values],
            "rankings": rankings_by_value,
            "stable": axis_stable,
        }

    stability = (
        RankingPolicy.CAPABILITY_RANKED if stable else RankingPolicy.COVERAGE_FALLBACK
    )
    return SensitivityReport(
        stable=stable,
        per_axis=per_axis,
        unstable_pairs=unstable_pairs,
        requested_ranking=requested_value,
        computed_ranking=_join_ranking(requested_value, stability),
        ranking_epsilon=ranking_epsilon,
    )


def module_scores(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
    *,
    universe: Iterable[tuple[str, str]] | None = None,
) -> list[ModuleScore]:
    """Модульные баллы (§6): Σ importance×score / Σ importance(resolved) по модулю.

    Веса модулей перенормируются на **применимые** модули (у которых знаменатель Coverage > 0);
    у неприменимого модуля ``applicable_weight`` = 0. Пустой вход → пустой список.

    В universe-режиме (§6/B-3a) множество модулей — объединение модулей каталога,
    затронутых ``universe``, и наблюдаемых модулей; знаменатель Coverage каждого модуля
    считается по парам его параметров, а не по существующим ячейкам. ``ModuleScore.coverage``
    — ``float``; ``UNDEFINED`` (пустая вселенная модуля) публикуется как ``0.0``.
    """
    cell_list = list(cells)
    by_module: dict[str, list[Cell]] = {}
    for cell in cell_list:
        row = params.get(cell.param_id) if params else None
        if row is None:
            continue
        by_module.setdefault(row.module, []).append(cell)

    pairs_by_module: dict[str, list[tuple[str, str]]] = {}
    if universe is None:
        modules = list(by_module)
    else:
        universe_list = list(universe)
        for product_id, param_id in universe_list:
            row = params.get(param_id) if params else None
            if row is None:
                continue
            pairs_by_module.setdefault(row.module, []).append((product_id, param_id))
        modules = sorted(set(by_module) | set(pairs_by_module))

    records: list[tuple[str, float, Score, Coverage]] = []
    for module in modules:
        module_cells = by_module.get(module, [])
        capability = _resolved_capability(module_cells, params, weights)
        coverage = evidence_coverage(
            module_cells,
            params,
            weights,
            universe=pairs_by_module.get(module) if universe is not None else None,
        )
        weight = float(weights[module]) if weights and module in weights else 1.0
        records.append((module, weight, capability, coverage))

    def _applicable(cov: Coverage) -> bool:
        return cov.universe > 0 if universe is not None else cov.n_used > 0

    applicable_total = sum(
        weight for _module, weight, _cap, cov in records if _applicable(cov)
    )
    result: list[ModuleScore] = []
    for module, weight, capability, coverage in records:
        if _applicable(coverage) and applicable_total > 0:
            applicable_weight = weight / applicable_total
        else:
            applicable_weight = 0.0
        coverage_value = coverage.value if coverage.value is not None else 0.0
        result.append(
            ModuleScore(
                module=module,
                score=capability.value,
                coverage=coverage_value,
                weight=weight,
                applicable_weight=applicable_weight,
            )
        )
    result.sort(key=lambda item: item.module)
    return result
