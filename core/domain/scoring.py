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
from dataclasses import asdict, dataclass, field
from typing import Any

from core.domain.enums import (
    CapabilityPolicy,
    CellState,
    ComparabilityLevel,
    DenialStrength,
    ExcludedCellClass,
    Importance,
    PublishabilityClass,
    RankingPolicy,
    Status,
    UnknownCause,
    UnknownPolicy,
)
from core.domain.models import AlignmentReport, CatalogRow, Cell, Cohort

__all__ = [
    "DEFAULT_COVERAGE_FLOOR",
    "DEFAULT_PARTIAL_SCORE",
    "DEFAULT_UNKNOWN_POLICY",
    "DEFAULT_UNKNOWN_SHARE_THRESHOLD",
    "DEFAULT_WEAK_DENIAL_SHARE_THRESHOLD",
    "Bucket",
    "Comparability",
    "Coverage",
    "ModuleScore",
    "ProductPublishability",
    "PublishabilityProfile",
    "Score",
    "SensitivityReport",
    "comparability",
    "endpoint_host_notices",
    "evidence_coverage",
    "failure_rate",
    "module_scores",
    "publishability_profile",
    "resolved_capability",
    "scoring_policy_spec",
    "sensitivity",
]

_EPS = 1e-9

#: Объявленная политика расчёта §6 — **единственное** место, где заданы значения,
#: с которыми исполняется формула: ``capability_policy`` (формула Resolved Capability),
#: ``unknown_policy`` (участвует ли ``❓`` в знаменателе) и ``partial_score`` (вклад
#: ``⚠️``). Эти же значения попадают в ключ межпрогонного кэша вердиктов (09 §10.5):
#: смена правил обязана инвалидировать кэш, иначе он вернул бы вердикт, посчитанный по
#: старым правилам.
#:
#: :class:`~core.domain.models.RunManifest` объявляет те же значения как **схему
#: записи** (домен не может импортировать себя через ``scoring`` — направление
#: зависимости обратное). Совпадение не проверяется комментарием: тест
#: ``tests/pipeline/test_eval_cache.py::test_manifest_scoring_defaults_match_declared_policy``
#: сверяет дефолты манифеста с :func:`scoring_policy_spec`.
DEFAULT_PARTIAL_SCORE: float = 0.5
DEFAULT_UNKNOWN_POLICY: UnknownPolicy = UnknownPolicy.EXCLUDED


def scoring_policy_spec() -> dict[str, Any]:
    """Объявленная политика расчёта §6 — значения, с которыми исполняется формула.

    Возвращает ``{capability_policy, unknown_policy, partial_score}`` в JSON-примитивах
    (строки/число): это же представление попадает в манифест прогона и в ключ
    межпрогонного кэша вердиктов (09 §10.5) — смена правил обязана менять оба, иначе
    кэш вернул бы вердикт, посчитанный по старым правилам.

    Функция — **декларация**, а не резолвер: объявленных уровней (``run.yaml``/``deploy``)
    у этой политики сегодня нет, поэтому значения — константы кода. Если уровень
    появится, он входит здесь, а не в вызывающих: вторая формула разошлась бы с первой.
    """
    return {
        "capability_policy": CapabilityPolicy.RESOLVED_ONLY_V1.value,
        "unknown_policy": DEFAULT_UNKNOWN_POLICY.value,
        "partial_score": DEFAULT_PARTIAL_SCORE,
    }

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
    partial_score: float = DEFAULT_PARTIAL_SCORE,
    unknown_policy: UnknownPolicy = DEFAULT_UNKNOWN_POLICY,
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


def _axis_partial_score(raw: object) -> float:
    """Значение оси ``partial_score`` как ``float``; нечисловая форма — видимая ошибка.

    Ось приходит извне домена (sensitivity-спека §6), поэтому приводится **с проверкой**,
    а не вслепую: ``0.5`` и ``"0.5"`` проходят, ``None`` даёт ``TypeError`` с именем оси.
    """
    if isinstance(raw, (int, float, str)):
        try:
            return float(raw)
        except ValueError as exc:
            raise ValueError(f"partial_score: не число ({raw!r})") from exc
    raise TypeError(f"partial_score: ожидалось число, получено {type(raw).__name__}")


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
    # Порядок обхода **сортирован**, а не «как пришло». Вселенная приходит множеством
    # (``scope_universe``), а сложение float неассоциативно: порядок обхода set зависит
    # от хеш-сида процесса, поэтому одна и та же вселенная давала ``211.88000000000096``
    # в одном процессе и ``211.8800000000009`` в другом. Следствие было не косметическим:
    # ``subject_digest`` гейта G4 менялся между прогонами, и уже принятое решение
    # отвергалось как «устаревшее» — гейт становился непроходимым на resume.
    for product_id, param_id in sorted(universe):
        w = _param_weight(param_id, params, weights)
        # Имя `scope_cell`, а не `cell`: в первой ветке (universe is None) `cell` уже
        # связана как `Cell`, и повторное присваивание `Cell | None` сузило бы её тип
        # вопреки проверкам ниже — mypy ловил это как not-assignable.
        scope_cell = by_key.get((product_id, param_id))
        if scope_cell is None:
            # Отсутствующая в прогоне ячейка scope — неразрешённая.
            universe_weight += w
            continue
        if scope_cell.status is Status.NOT_APPLICABLE:
            continue  # N/A вне знаменателя (I7)
        if scope_cell.state is CellState.PRELIMINARY:
            continue  # I13: preliminary вне всех расчётов
        if (
            scope_cell.state in _MEASURED_STATES
            and scope_cell.status in _APPLICABLE_STATUSES
        ):
            universe_weight += w
            completed += w
            n_used += 1
            if scope_cell.status in _RESOLVED_STATUSES:
                numerator += w
        elif scope_cell.state is CellState.FAILED:
            universe_weight += w
            completed += w
            n_used += 1
        else:
            # pending (или иное неразрешённое) — в знаменателе как неразрешённая.
            universe_weight += w

    defined = universe_weight > 0
    # `coverage_value`, а не `value`: в ветке `universe is None` имя `value` уже занято
    # скаляром `float`, а здесь тип шире (`None` означает «Coverage не определён», I11).
    if defined:
        coverage_value: float | None = numerator / universe_weight
    else:
        coverage_value = None
        completed = 0.0
    return Coverage(
        value=coverage_value,
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
                partial_score=_axis_partial_score(combo["partial_score"]),
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


# ---------------------------------------------------------------------------
# Профиль публикуемости (A1, §6): разложение неизвестности по причинам
# ---------------------------------------------------------------------------

#: Доля неизвестных ячеек, выше которой карта **не** публикуется как сравнение (A1).
#:
#: Это **объявленный дефолт, а не измеренная константа** (аксиома A3: «дефолты — это
#: assumption/candidate; замена дефолта происходит по измерению, не по предвидению»).
#: Происхождение: D-19 формулировал A1 как «>30 % ``❓``/``❌`` без absence-evidence ⇒
#: карта непригодна». Сегодня ``❌`` без absence-evidence недостижим по конструкции
#: (I2: ``❌`` несёт силу отрицания), поэтому доля считается по ``❓`` + ``failed`` +
#: ``pending``, а слабые «нет» (``bounded``) публикуются **отдельным** числом и в долю не
#: подмешиваются. Достигнутая доля всегда публикуется рядом с порогом — порог без
#: достигнутого значения ничего не измеряет.
DEFAULT_UNKNOWN_SHARE_THRESHOLD: float = 0.30

#: Доля «нет» на слабом основании (``denial_strength=bounded``) среди **опубликованных**
#: ``❌``, после которой появляется оговорка ``weak_denials_above_threshold``.
#:
#: Тоже assumption/candidate (A3), не измерение: ``bounded`` — законное ``❌``, но оно
#: держится на принятии риска человеком, а не на полном охвате. Порог не запрещает
#: публикацию — он делает основание видимым.
DEFAULT_WEAK_DENIAL_SHARE_THRESHOLD: float = 0.10

#: Нижний уровень Coverage, ниже которого карта не публикуется как сравнение (A1).
#:
#: Согласован с границей Comparability ``INSUFFICIENT`` (``_level_for``: <0.50): ниже
#: половины протокола «сравнимость» описывает не продукты, а пробелы измерения.
DEFAULT_COVERAGE_FLOOR: float = 0.50


@dataclass(frozen=True)
class Bucket:
    """Один класс разложения: сколько ячеек и какая доля веса знаменателя.

    ``share`` — доля **своего** знаменателя; какой именно, называет владелец бакета
    (:class:`ProductPublishability.denominator` для причин неизвестности,
    ``denials_weight`` для слабых «нет»). ``missing`` — пары вселенной **без строки**
    ячейки (только ``pending``): «не смотрели» отличается от «строка есть, но пустая».
    """

    cells: int = 0
    weight: float = 0.0
    share: float = 0.0
    missing: int = 0


@dataclass(frozen=True)
class ProductPublishability:
    """Разложение неизвестности одного продукта (§6/A1) — часть профиля публикуемости.

    ``denominator`` совпадает с весом знаменателя Coverage этого продукта в том же режиме
    (universe/legacy): разложение объясняет **тот же** знаменатель, а не свой собственный.
    ``causes`` — только внутри знаменателя (``UnknownCause``), ``excluded`` — вне его
    (``ExcludedCellClass``, I7/I13): смешать их значило бы выдать ``N/A`` за незнание.
    """

    product_id: str
    denominator: float
    measured_cells: int
    measured_weight: float
    unknown_cells: int
    unknown_weight: float
    unknown_share: float
    causes: dict[str, Bucket]
    excluded: dict[str, Bucket]
    #: вес опубликованных ``❌`` (знаменатель доли слабых «нет»)
    denials_weight: float
    weak_denials: Bucket
    #: контекст покрытия корпуса (``complete``/``observed``/``expected``/``blocked``).
    #: Это **не** причина (причина живёт в ячейке, ``EvidenceScope``), а объяснение, почему
    #: материал мог не наблюдаться; ``None`` — записи покрытия нет.
    corpus: dict[str, Any] | None = None


@dataclass(frozen=True)
class PublishabilityProfile:
    """Профиль публикуемости карты (§6/A1): класс + числа, из которых он выведен.

    :param klass: класс :class:`~core.domain.enums.PublishabilityClass` — **результат**.
    :param unknown_share: достигнутая доля неизвестного веса (знаменатель Coverage).
    :param unknown_share_threshold: объявленный порог (:data:`DEFAULT_UNKNOWN_SHARE_THRESHOLD`).
    :param coverage: агрегатный Coverage (``None`` ⇔ ``UNDEFINED``).
    :param coverage_floor: объявленный уровень Coverage (:data:`DEFAULT_COVERAGE_FLOOR`).
    :param weak_denial_share: доля слабых «нет» среди опубликованных ``❌``.
    :param dominant_cause: класс причины с наибольшим весом (``None`` — неизвестных нет).
    :param causes / excluded: агрегат по всем продуктам.
    :param products: разложение по продуктам (детерминированный порядок ключей).
    :param measured_products: сколько продуктов имеют хотя бы одну измеренную ячейку.
    :param caveats: названные оговорки (коды); пустой список — оговорок нет.
    :param reasons: условия, определившие класс (коды).
    :param banner: готовая строка для шапки отчёта — класс и доминирующая причина.
    :param rnd_endpoint: карта получена со стенда, endpoint которого **объявлен вне**
        корпоративного периметра (``deploy.llm.rnd_endpoint: true``). Это условие
        стенда, а не качество измерения, поэтому на **класс публикуемости оно не
        влияет**: демотировать карту за место исполнения значило бы назвать измерение
        плохим из-за адреса шлюза (A1/A3). Видимость даётся полем профиля, строкой
        ``publishability_notes`` и шапкой battle card: «карта не является
        производственной публикацией» — утверждение о провенансе, а не о качестве.
    :param llm_hosts: хосты endpoint'ов из объявленных ``llm_scenarios`` (без схемы/пути,
        отсортированы и дедуплицированы) — факт «куда ушёл корпус». Пусто — прогон не
        объявлял endpoint (оценщик не поднимался).
    :param llm_host_notices: машиночитаемые замечания о хостах
        (:func:`endpoint_host_notices`): ``{"code", "level", "hosts", "message"}``.
        Строятся **только** из объявленных входов (флаг + хосты) — окружение сюда не
        читается, иначе провенанс публикации зависел бы от env хоста.
    """

    klass: str
    unknown_share: float
    unknown_share_threshold: float
    coverage: float | None
    coverage_floor: float
    weak_denial_share: float
    weak_denial_share_threshold: float
    dominant_cause: str | None
    causes: dict[str, Bucket]
    excluded: dict[str, Bucket]
    weak_denials: Bucket
    products: dict[str, ProductPublishability]
    measured_products: int
    caveats: list[str]
    reasons: list[str]
    banner: str
    #: Стенд объявлен вне корпоративного периметра (``deploy.llm.rnd_endpoint: true``).
    rnd_endpoint: bool = False
    #: Объявлена ли принадлежность периметру вообще (``rnd_endpoint`` не ``None``).
    #: Отдельное поле, потому что ``rnd_endpoint: false`` — это **декларация** «внутри
    #: периметра», а отсутствие декларации — другое состояние (см. замечания).
    rnd_endpoint_declared: bool = False
    #: Объявленные хосты endpoint'ов (``llm_scenarios.endpoint_host``), детерминированно.
    llm_hosts: list[str] = field(default_factory=list)
    #: Замечания о хостах/периметре (коды + текст), детерминированный порядок.
    llm_host_notices: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """JSON/YAML-совместимый словарь (манифест, CLI ``--json``, артефакт ``outputs/``).

        Один сериализатор на все выходы: манифест, battle card и
        ``outputs/publishability.yaml`` обязаны показывать **одни и те же** числа.
        """
        return asdict(self)


def endpoint_host_notices(
    *, rnd_endpoint: bool | None, hosts: Sequence[str]
) -> list[dict[str, Any]]:
    """Машиночитаемые замечания о хостах endpoint'а оценщика (§10, исключение стенда RnD).

    Источник — **только объявленные** входы: флаг ``deploy.llm.rnd_endpoint`` и хосты из
    ``llm_scenarios.endpoint_host``. Имя хоста не классифицируется: «внутренний» и
    «внешний» — это то, что заявлено, а не то, что угадано по строке (корпоративный
    endpoint тоже может быть публичным FQDN, а разрешение DNS ради провенанса зависело бы
    от сети и не было бы детерминированным).

    Коды:

    * ``rnd_endpoint_not_declared`` (``warning``) — endpoint объявлен, а принадлежность
      периметру — нет (``rnd_endpoint: null``). Внешний хост в этом состоянии неотличим от
      ошибки конфигурации: объявите ``true`` (стенд) или ``false`` (внутри периметра);
    * ``rnd_endpoint_declared`` (``warning``) — объявлен стенд вне периметра: карта
      получена на внешнем endpoint и **не является производственной публикацией**;
    * ``rnd_endpoint_without_endpoint`` (``notice``) — исключение объявлено, а endpoint не
      объявлен вовсе (``llm_scenarios`` пуста): признак лишний, хоста для него нет.

    Порядок детерминирован (порядок проверок ниже), хосты в замечании — отсортированы.
    ``rnd_endpoint: false`` (явно объявлено «внутри периметра») замечаний не даёт: это
    декларация оператора, а не наблюдение, и спорить с ней коду нечем.
    """
    declared_hosts = sorted(
        {str(host).strip() for host in hosts if str(host).strip()}
    )
    notices: list[dict[str, Any]] = []
    if rnd_endpoint is True and not declared_hosts:
        notices.append(
            {
                "code": "rnd_endpoint_without_endpoint",
                "level": "notice",
                "hosts": [],
                "message": (
                    "llm.rnd_endpoint объявлен, но endpoint не объявлен ни одним слотом "
                    "(llm_scenarios пуста): исключение стенда не к чему отнести — "
                    "оценщик на этом прогоне не поднимался"
                ),
            }
        )
    elif rnd_endpoint is True:
        notices.append(
            {
                "code": "rnd_endpoint_declared",
                "level": "warning",
                "hosts": declared_hosts,
                "message": (
                    "стенд RnD: endpoint "
                    + ", ".join(declared_hosts)
                    + " объявлен вне корпоративного периметра (llm.rnd_endpoint: true) — "
                    "карта получена на стенде и не является производственной публикацией"
                ),
            }
        )
    elif rnd_endpoint is None and declared_hosts:
        notices.append(
            {
                "code": "rnd_endpoint_not_declared",
                "level": "warning",
                "hosts": declared_hosts,
                "message": (
                    "endpoint "
                    + ", ".join(declared_hosts)
                    + " объявлен в llm_scenarios, но принадлежность корпоративному "
                    "периметру не объявлена (llm.rnd_endpoint: null): внешний хост здесь "
                    "неотличим от ошибки конфигурации — объявите true (стенд RnD) или "
                    "false (внутри периметра)"
                ),
            }
        )
    return notices


def _unknown_cause(cell: Cell) -> str:
    """Причина неизвестности ячейки из **записанных** данных (не догадка)."""
    scope = getattr(cell, "evidence_scope", None)
    strength = getattr(scope, "denial_strength", None)
    if strength is DenialStrength.NOT_STATED:
        return UnknownCause.NOT_STATED.value
    if strength is DenialStrength.UNOBSERVED:
        return UnknownCause.UNOBSERVED_SCOPE.value
    if strength is DenialStrength.INSTRUMENT_FAILED:
        return UnknownCause.INSTRUMENT_FAILED.value
    # Класс не записан (историческая строка без ``evidence_scope``) либо записан
    # несогласованно со статусом ``❓`` (аномалия данных) — причина не выводится, и
    # подставлять вместо неё «вероятную» значило бы выдумать измерение.
    return UnknownCause.UNSPECIFIED.value


def _cell_class(cell: Cell | None, *, legacy: bool) -> tuple[str, str]:
    """Классифицирует строку разложения: ``(вид, класс)``.

    Вид: ``resolved`` (ячейка дала вердикт), ``unknown`` (внутри знаменателя, причина
    названа) либо ``excluded`` (вне знаменателя, I7/I13/аномалия статуса). Правила
    повторяют **построчно** цикл :func:`evidence_coverage`: разложение обязано объяснять
    тот же знаменатель, иначе это второе определение Coverage.
    """
    if cell is None:
        return "unknown", UnknownCause.PENDING.value
    if cell.status is Status.NOT_APPLICABLE:
        return "excluded", ExcludedCellClass.NOT_APPLICABLE.value
    if cell.state is CellState.PRELIMINARY:
        return "excluded", ExcludedCellClass.PRELIMINARY.value
    if cell.state in _MEASURED_STATES and cell.status in _APPLICABLE_STATUSES:
        if cell.status in _RESOLVED_STATUSES:
            return "resolved", ""
        return "unknown", _unknown_cause(cell)
    if cell.state is CellState.FAILED:
        return "unknown", UnknownCause.INSTRUMENT_FAILED.value
    if cell.status is None and cell.state in _MEASURED_STATES:
        # Ячейка «измерена», но статуса нет: в universe-режиме Coverage считает её
        # неразрешённой, в legacy — не засчитывает вовсе. Повторяем оба правила.
        return (
            ("unknown", UnknownCause.PENDING.value)
            if not legacy
            else ("excluded", ExcludedCellClass.UNRESOLVED_STATE.value)
        )
    return "unknown", UnknownCause.PENDING.value


def _partition(
    cells: list[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
    universe: Iterable[tuple[str, str]] | None,
) -> Iterable[tuple[str, Cell | None, float]]:
    """Обход строк разложения с **теми же** весами, что у Coverage.

    Universe-режим идёт по ``sorted(universe)`` (детерминизм суммы float — тот же приём,
    что в :func:`evidence_coverage`: порядок ``set`` зависит от хеш-сида процесса).
    """
    if universe is None:
        for cell in cells:
            yield cell.product_id, cell, _cell_weight(cell, params, weights)
        return
    by_key = {(cell.product_id, cell.param_id): cell for cell in cells}
    for product_id, param_id in sorted(universe):
        yield (
            product_id,
            by_key.get((product_id, param_id)),
            _param_weight(param_id, params, weights),
        )


def _corpus_context(entry: Any) -> dict[str, Any] | None:
    """Контекст покрытия корпуса продукта из ``CorpusCoverage`` (объект или mapping)."""
    if entry is None:
        return None

    def _get(name: str, default: Any = None) -> Any:
        if isinstance(entry, Mapping):
            return entry.get(name, default)
        return getattr(entry, name, default)

    context = {
        "complete": _get("complete"),
        "expected_sources": _get("expected_sources", 0),
        "observed_sources": _get("observed_sources", 0),
        "blocked": dict(_get("blocked", None) or {}),
        "reason": _get("reason"),
    }
    return context


def publishability_profile(
    cells: Iterable[Cell],
    params: Mapping[str, CatalogRow],
    weights: Mapping[str, float],
    *,
    coverage: Coverage,
    universe: Iterable[tuple[str, str]] | None = None,
    comparability_level: str | None = None,
    comparability_reasons: Sequence[str] = (),
    cross_cycle: bool = False,
    corpus_coverage: Mapping[str, Any] | None = None,
    unknown_share_threshold: float = DEFAULT_UNKNOWN_SHARE_THRESHOLD,
    weak_denial_share_threshold: float = DEFAULT_WEAK_DENIAL_SHARE_THRESHOLD,
    coverage_floor: float = DEFAULT_COVERAGE_FLOOR,
    rnd_endpoint: bool | None = None,
    llm_hosts: Sequence[str] = (),
) -> PublishabilityProfile:
    """Строит профиль публикуемости: разложение неизвестности + класс (A1, §6).

    **Разложение.** Каждая строка знаменателя Coverage попадает ровно в один бакет:
    ``resolved`` (✅/⚠️/❌) либо одна из причин :class:`UnknownCause`. Строки вне
    знаменателя (``N/A`` — I7, ``preliminary`` — I13) идут в ``excluded`` и знаменатель
    **не** объясняют. Поэтому ``Σ causes + resolved_weight == coverage.denominator`` —
    это инвариант, а не совпадение (проверяется тестом).

    **Класс** (порядок проверок фиксирован — первый сработавший побеждает):

    1. ``NOT_COMPARABLE`` — ``coverage.defined is False`` (``UNDEFINED``) либо ни у одного
       продукта нет измеренных ячеек: сравнивать структурно нечего;
    2. ``ANALYTICAL_ONLY`` — Capability вычислим, но ``unknown_share > порога`` либо
       ``coverage < coverage_floor``: карта как сравнение не публикуется;
    3. ``PUBLISHABLE_WITH_CAVEAT`` — публикуется, но оговорки непусты;
    4. ``PUBLISHABLE`` — оговорок нет.

    :param cells: ячейки прогона (**без** устаревших: ``stale`` исключены в
        :mod:`core.pipeline.compute` — разложение считает тот же вход, что Capability).
    :param coverage: уже посчитанный Coverage того же входа: знаменатель разложения берётся
        из него, второй формулы нет.
    :param universe: вселенная scope (§6/B-3a); ``None`` — legacy-режим.
    :param comparability_level/reasons/cross_cycle: аннотация Comparability прогона —
        источник оговорок (сама по себе в класс не входит: ``INSUFFICIENT`` покрыт
        проверкой ``coverage_floor``, а как оговорка публикуется отдельно).
    :param corpus_coverage: ``{product_id: CorpusCoverage}`` — контекст неполноты корпуса.
        В причины **не** превращается: причина выводится из ``EvidenceScope`` ячейки.
    :param rnd_endpoint: объявленное исключение стенда RnD (§10): ``None`` — не объявлено,
        ``True`` — endpoint вне корпоративного периметра. В класс публикуемости **не
        входит** (условие стенда, не качество измерения): попадает в профиль полем,
        строкой ``publishability_notes`` и шапкой battle card.
    :param llm_hosts: объявленные хосты endpoint'ов (``llm_scenarios.endpoint_host``).
        Сортируются и дедуплицируются здесь, чтобы артефакт был детерминирован независимо
        от порядка строк в БД.
    """
    declared_hosts = sorted(
        {str(host).strip() for host in llm_hosts if str(host).strip()}
    )
    host_notices = endpoint_host_notices(
        rnd_endpoint=rnd_endpoint, hosts=declared_hosts
    )
    cell_list = list(cells)
    corpus_map = dict(corpus_coverage or {})
    # Материализуем вселенную один раз: она нужна и обходу, и составу продуктов
    # (генератор был бы исчерпан первым проходом).
    universe_list: list[tuple[str, str]] | None = (
        None if universe is None else sorted(set(universe))
    )

    # Аккумуляторы на продукт; порядок ключей — отсортированный (детерминизм артефакта).
    denominator: dict[str, float] = {}
    measured_cells: dict[str, int] = {}
    measured_weight: dict[str, float] = {}
    denials_weight: dict[str, float] = {}
    weak_cells: dict[str, int] = {}
    weak_weight: dict[str, float] = {}
    causes: dict[str, dict[str, dict[str, float]]] = {}
    excluded: dict[str, dict[str, dict[str, float]]] = {}

    def _bump(
        store: dict[str, dict[str, dict[str, float]]],
        product: str,
        key: str,
        weight: float,
        *,
        missing: bool,
    ) -> None:
        bucket = store.setdefault(product, {}).setdefault(
            key, {"cells": 0, "weight": 0.0, "missing": 0}
        )
        bucket["cells"] += 1
        bucket["weight"] += weight
        if missing:
            bucket["missing"] += 1

    for product_id, cell, weight in _partition(
        cell_list, params, weights, universe_list
    ):
        product_id = str(product_id)
        kind, key = _cell_class(cell, legacy=universe_list is None)
        if kind == "excluded":
            _bump(excluded, product_id, key, weight, missing=False)
            continue
        denominator[product_id] = denominator.get(product_id, 0.0) + weight
        if kind == "resolved":
            measured_cells[product_id] = measured_cells.get(product_id, 0) + 1
            measured_weight[product_id] = measured_weight.get(product_id, 0.0) + weight
            scope = getattr(cell, "evidence_scope", None)
            if cell is not None and cell.status is Status.ABSENT:
                denials_weight[product_id] = denials_weight.get(product_id, 0.0) + weight
                if getattr(scope, "denial_strength", None) is DenialStrength.BOUNDED:
                    weak_cells[product_id] = weak_cells.get(product_id, 0) + 1
                    weak_weight[product_id] = weak_weight.get(product_id, 0.0) + weight
            continue
        _bump(causes, product_id, key, weight, missing=cell is None)

    all_products = sorted(
        set(denominator)
        | set(excluded)
        | ({str(pair[0]) for pair in universe_list} if universe_list else set())
        | {str(cell.product_id) for cell in cell_list}
    )

    def _bucket(
        raw: Mapping[str, Mapping[str, float]] | None, denom: float
    ) -> dict[str, Bucket]:
        if not raw:
            return {}
        result: dict[str, Bucket] = {}
        for key in sorted(raw):
            cells_count = int(raw[key].get("cells", 0))
            weight = float(raw[key].get("weight", 0.0))
            result[key] = Bucket(
                cells=cells_count,
                weight=weight,
                share=(weight / denom) if denom > 0 else 0.0,
                missing=int(raw[key].get("missing", 0)),
            )
        return result

    products: dict[str, ProductPublishability] = {}
    for product_id in all_products:
        denom = denominator.get(product_id, 0.0)
        resolved_w = measured_weight.get(product_id, 0.0)
        denials_w = denials_weight.get(product_id, 0.0)
        weak_w = weak_weight.get(product_id, 0.0)
        product_causes = _bucket(causes.get(product_id), denom)
        products[product_id] = ProductPublishability(
            product_id=product_id,
            denominator=denom,
            measured_cells=measured_cells.get(product_id, 0),
            measured_weight=resolved_w,
            unknown_cells=sum(bucket.cells for bucket in product_causes.values()),
            # Неизвестный вес = знаменатель минус разрешённый: разложение и Coverage
            # считают один вход, поэтому величина не может «разойтись» с ним.
            unknown_weight=max(denom - resolved_w, 0.0),
            unknown_share=(1.0 - resolved_w / denom) if denom > 0 else 0.0,
            causes=product_causes,
            excluded=_bucket(excluded.get(product_id), denom),
            denials_weight=denials_w,
            weak_denials=Bucket(
                cells=weak_cells.get(product_id, 0),
                weight=weak_w,
                share=(weak_w / denials_w) if denials_w > 0 else 0.0,
            ),
            corpus=_corpus_context(corpus_map.get(product_id)),
        )

    total_denominator = sum(product.denominator for product in products.values())
    total_resolved = sum(product.measured_weight for product in products.values())
    unknown_share = (
        (total_denominator - total_resolved) / total_denominator
        if total_denominator > 0
        else 0.0
    )
    total_denials = sum(product.denials_weight for product in products.values())
    total_weak = sum(product.weak_denials.weight for product in products.values())
    weak_share = (total_weak / total_denials) if total_denials > 0 else 0.0

    aggregate_causes = _aggregate_buckets(products, "causes", total_denominator)
    aggregate_excluded = _aggregate_buckets(products, "excluded", total_denominator)
    dominant_cause: str | None = None
    if aggregate_causes:
        # Максимум веса; ничья решается именем класса — детерминизм артефакта.
        dominant_cause = sorted(
            aggregate_causes.items(), key=lambda item: (-item[1].weight, item[0])
        )[0][0]

    measured_products = sum(1 for product in products.values() if product.measured_cells > 0)
    klass, reasons, caveats = _publishability_class(
        coverage=coverage,
        unknown_share=unknown_share,
        weak_denial_share=weak_share,
        measured_products=measured_products,
        products=products,
        comparability_level=comparability_level,
        cross_cycle=cross_cycle,
        unknown_share_threshold=unknown_share_threshold,
        weak_denial_share_threshold=weak_denial_share_threshold,
        coverage_floor=coverage_floor,
    )
    banner = _publishability_banner(
        klass=klass,
        unknown_share=unknown_share,
        unknown_share_threshold=unknown_share_threshold,
        coverage=coverage.value,
        coverage_floor=coverage_floor,
        dominant_cause=dominant_cause,
        causes=aggregate_causes,
        caveats=caveats,
        reasons=reasons,
        comparability_reasons=comparability_reasons,
    )
    return PublishabilityProfile(
        klass=klass,
        unknown_share=unknown_share,
        unknown_share_threshold=unknown_share_threshold,
        coverage=coverage.value,
        coverage_floor=coverage_floor,
        weak_denial_share=weak_share,
        weak_denial_share_threshold=weak_denial_share_threshold,
        dominant_cause=dominant_cause,
        causes=aggregate_causes,
        excluded=aggregate_excluded,
        weak_denials=Bucket(
            cells=sum(product.weak_denials.cells for product in products.values()),
            weight=total_weak,
            share=weak_share,
        ),
        products=products,
        measured_products=measured_products,
        caveats=caveats,
        reasons=reasons,
        banner=banner,
        # §10: исключение стенда RnD — видимое, но не влияющее на класс (без демотирования).
        rnd_endpoint=rnd_endpoint is True,
        rnd_endpoint_declared=rnd_endpoint is not None,
        llm_hosts=declared_hosts,
        llm_host_notices=host_notices,
    )


def _aggregate_buckets(
    products: Mapping[str, ProductPublishability], attribute: str, denominator: float
) -> dict[str, Bucket]:
    """Суммирует одноимённые бакеты продуктов (детерминированный порядок ключей)."""
    totals: dict[str, dict[str, float]] = {}
    for product in products.values():
        buckets: Mapping[str, Bucket] = getattr(product, attribute)
        for key, bucket in buckets.items():
            slot = totals.setdefault(key, {"cells": 0.0, "weight": 0.0, "missing": 0.0})
            slot["cells"] += bucket.cells
            slot["weight"] += bucket.weight
            slot["missing"] += bucket.missing
    return {
        key: Bucket(
            cells=int(totals[key]["cells"]),
            weight=totals[key]["weight"],
            share=(totals[key]["weight"] / denominator) if denominator > 0 else 0.0,
            missing=int(totals[key]["missing"]),
        )
        for key in sorted(totals)
    }


def _publishability_class(
    *,
    coverage: Coverage,
    unknown_share: float,
    weak_denial_share: float,
    measured_products: int,
    products: Mapping[str, ProductPublishability],
    comparability_level: str | None,
    cross_cycle: bool,
    unknown_share_threshold: float,
    weak_denial_share_threshold: float,
    coverage_floor: float,
) -> tuple[str, list[str], list[str]]:
    """Выводит класс публикуемости из чисел: ``(класс, причины класса, оговорки)``.

    Порог — не приговор, а **измеряемая граница**: достигнутая доля всегда публикуется
    рядом, поэтому читатель видит, насколько карта отстоит от класса.
    """
    reasons: list[str] = []
    caveats: list[str] = []

    if not coverage.defined:
        reasons.append("coverage_undefined")
    if measured_products == 0:
        reasons.append("no_measured_cells")
    if reasons:
        return PublishabilityClass.NOT_COMPARABLE.value, reasons, []

    below_floor = (coverage.value or 0.0) < coverage_floor
    above_threshold = unknown_share > unknown_share_threshold
    if above_threshold:
        reasons.append("unknown_share_above_threshold")
    if below_floor:
        reasons.append("coverage_below_floor")
    if reasons:
        return PublishabilityClass.ANALYTICAL_ONLY.value, reasons, []

    if measured_products < 2:
        caveats.append("single_product_cohort")
    if len([p for p in products.values() if p.denominator > 0]) >= 2 and any(
        product.measured_cells == 0 for product in products.values()
    ):
        caveats.append("product_without_measurements")
    if comparability_level == ComparabilityLevel.INSUFFICIENT.value:
        caveats.append("comparability_insufficient")
    if cross_cycle:
        caveats.append("comparability_cross_cycle")
    if weak_denial_share > weak_denial_share_threshold:
        caveats.append("weak_denials_above_threshold")
    if caveats:
        return PublishabilityClass.PUBLISHABLE_WITH_CAVEAT.value, reasons, caveats
    return PublishabilityClass.PUBLISHABLE.value, reasons, []


def _publishability_banner(
    *,
    klass: str,
    unknown_share: float,
    unknown_share_threshold: float,
    coverage: float | None,
    coverage_floor: float,
    dominant_cause: str | None,
    causes: Mapping[str, Bucket],
    caveats: Sequence[str],
    reasons: Sequence[str],
    comparability_reasons: Sequence[str],
) -> str:
    """Строка для шапки отчёта: класс **и** причина, по которой класс такой.

    Баннер обязан называть доминирующую причину, а не только «coverage
    INSUFFICIENT»: иначе читатель видит отказ без основания и не знает, что чинить
    (наблюдать материал, спрашивать вендора или перезапускать сбойное измерение).
    """
    def pct(value: float) -> str:
        return f"{value * 100:.1f}%"

    if klass == PublishabilityClass.NOT_COMPARABLE.value:
        detail = ", ".join(reasons)
        parts = [f"{klass}: сравнивать нечего ({detail})"]
        if coverage is None:
            parts.append("Coverage UNDEFINED")
        elif reasons == ["no_measured_cells"]:
            parts.append("нет ни одной измеренной ячейки")
        return "; ".join(parts)

    parts = [f"{klass}: {pct(unknown_share)} ячеек неизвестны"]
    parts.append(f"порог {pct(unknown_share_threshold)}")
    if dominant_cause is not None:
        cause_share = causes[dominant_cause].share if dominant_cause in causes else 0.0
        parts.append(f"доминирует {dominant_cause} ({pct(cause_share)})")
    if coverage is not None:
        suffix = f" < уровня {coverage_floor:.4f}" if "coverage_below_floor" in reasons else ""
        parts.append(f"coverage {coverage:.4f}{suffix}")
    else:
        parts.append("coverage UNDEFINED")
    if caveats:
        parts.append(f"оговорки: {', '.join(caveats)}")
    if not caveats and klass == PublishabilityClass.PUBLISHABLE.value and comparability_reasons:
        parts.append(f"comparability: {', '.join(comparability_reasons)}")
    return "; ".join(parts)

