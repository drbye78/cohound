"""coHound — расчёт показателей прогона (W10/T10.1, Ш8, §6, 02 §E.1).

Загружает ячейки из хранилища и собирает :class:`ScoreBundle` из **готовых** доменных
функций :mod:`core.domain.scoring` (формулы здесь не дублируются):

* Capability/Evidence Coverage/failure_rate (§6);
* модульные баллы (``module_scores``);
* Comparability (min Coverage по когорте + alignment-условия);
* sensitivity-анализ и допустимый ранг.

Инварианты:

* **I5** — ``failed`` вне числителя Capability, но входит в знаменатель Coverage; модуль
  видит это через ``evidence_coverage`` (не переопределяем).
* **I11** — Capability не публикуется без Coverage: :func:`assert_publishable` выдаёт
  предупреждение при отсутствии покрытия (жёсткий гейт — в ``pipeline/report.py``);
  Coverage из бандла никогда не «теряется» молча.
* **I13** — ``preliminary``-ячейки исключаются из **всех** расчётов. Доменные функции их и
  так игнорируют; здесь они фильтруются явно, чтобы модульные баллы не содержали фантомных
  модулей, и выполняется проверка эквивалентности.
* **K6/A3** — ``requested_ranking`` не переопределяет демотирование: ``computed_ranking``
  берётся из sensitivity и при неустойчивости становится ``coverage_fallback`` даже при
  запрошенном ``capability_ranked``.

Модуль категорийно нейтрален (§1.3), детерминирован и не добавляет зависимостей:
единственный I/O — чтение ячеек и манифеста через ``StorePort``.
"""

from __future__ import annotations

import json
import warnings
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import Any

from core.domain.enums import (
    AnchorRelation,
    CellState,
    ComparabilityLevel,
    RankingPolicy,
)
from core.domain.models import (
    AlignmentReport,
    CatalogRow,
    Cell,
    Cohort,
    CohortEntry,
)
from core.domain.scoring import (
    Comparability,
    Coverage,
    ModuleScore,
    Score,
    SensitivityReport,
    comparability,
    evidence_coverage,
    failure_rate,
    module_scores,
    resolved_capability,
    sensitivity,
)
from core.ports.store import StorePort

__all__ = [
    "ScoreBundle",
    "compute",
    "per_product_bundle",
    "assert_publishable",
    "publish_warnings",
]

#: Запрошенный режим ранжирования по умолчанию (§6/K6), если ни аргумент, ни манифест
#: его не задают.
DEFAULT_REQUESTED_RANKING: str = RankingPolicy.CAPABILITY_RANKED.value


# ---------------------------------------------------------------------------
# Результат расчёта
# ---------------------------------------------------------------------------


@dataclass
class ScoreBundle:
    """Сводка расчёта прогона (Ш8, 02 §E.1).

    :param capability: Resolved Capability (§6) — «из исследованного подтверждено N%».
    :param coverage: Evidence Coverage (§6, I5) — полнота прохождения протокола.
    :param failure_rate: ``failed / (evaluated ∪ failed)`` (R13).
    :param modules: модульные баллы и перенормированные веса.
    :param comparability: кросс-продуктовая аннотация (min Coverage + alignment).
    :param sensitivity: пост-хок sensitivity-анализ и допустимый ранг.
    :param requested_ranking: запрошенный режим отображения (K6).
    :param computed_ranking: вычисленный допустимый ранг (демотируется sensitivity, K6).
    """

    capability: Score
    coverage: Coverage
    failure_rate: float
    modules: list[ModuleScore]
    comparability: Comparability
    sensitivity: SensitivityReport
    requested_ranking: str
    computed_ranking: str

    def to_dict(self) -> dict[str, Any]:
        """Словарь бандла для отчёта/манифеста (JSON-совместимый, вложенный)."""
        return _jsonable(self)

    def to_manifest_json(self) -> str:
        """Детерминированный JSON бандла (``sort_keys``) для манифеста/отчёта."""
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)


# ---------------------------------------------------------------------------
# Внутренние помощники
# ---------------------------------------------------------------------------


def _jsonable(value: Any) -> Any:
    """Рекурсивно приводит значение к JSON-совместимому виду (dataclass/enum/tuple → dict/list)."""
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: _jsonable(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return value


def _ranking_value(value: RankingPolicy | str) -> str:
    """Нормализует ранжирование к ``.value``; неизвестное значение → :class:`ValueError`."""
    if isinstance(value, RankingPolicy):
        return value.value
    return RankingPolicy(str(value)).value


def _resolve_requested_ranking(
    store: StorePort, run_id: str, requested_ranking: RankingPolicy | str | None
) -> str:
    """Определяет запрошенный режим: аргумент → манифест → :data:`DEFAULT_REQUESTED_RANKING`.

    Манифест может отсутствовать (промежуточный/пустой прогон) — тогда действует дефолт.
    """
    if requested_ranking is not None:
        return _ranking_value(requested_ranking)
    try:
        manifest = store.read_manifest(run_id)
    except ValueError:
        manifest = None
    if manifest is not None and manifest.requested_ranking is not None:
        return _ranking_value(manifest.requested_ranking)
    return DEFAULT_REQUESTED_RANKING


def _per_product_coverage(
    cells: list[Cell], catalog: Mapping[str, CatalogRow], module_weights: Mapping[str, float]
) -> dict[str, float]:
    """Coverage по каждому продукту (для Comparability) через доменную функцию."""
    grouped: dict[str, list[Cell]] = {}
    for cell in cells:
        grouped.setdefault(cell.product_id, []).append(cell)
    return {
        product_id: evidence_coverage(group, catalog, module_weights).value
        for product_id, group in grouped.items()
    }


def _comparability(
    cohort: Cohort | None,
    per_product_coverage: Mapping[str, float],
    alignment: AlignmentReport | None,
) -> Comparability:
    """Comparability по когорте; без когорты кросс-продуктовая аннотация неопределена.

    ``domain.scoring.comparability`` требует :class:`Cohort` (состав и якорь). Если когорта
    не передана, честный результат — ``INSUFFICIENT`` с причиной ``cohort_missing`` (не
    выдумываем состав), а ``cross_cycle`` фиксирует отсутствие alignment-условий.
    """
    if cohort is not None:
        return comparability(cohort, per_product_coverage, alignment)
    return Comparability(
        level=ComparabilityLevel.INSUFFICIENT.value,
        min_coverage=0.0,
        cross_cycle=True,
        reasons=["cohort_missing"],
    )


def _compute_from_cells(
    cells: list[Cell],
    *,
    catalog: Mapping[str, CatalogRow],
    module_weights: Mapping[str, float],
    cohort: Cohort | None,
    alignment: AlignmentReport | None,
    requested_ranking: str,
) -> ScoreBundle:
    """Считает :class:`ScoreBundle` по уже загруженным ячейкам (без I/O)."""
    # I13: preliminary вне всех расчётов. Фильтруем явно (модульные баллы иначе содержали бы
    # фантомные модули с нулевым покрытием) и проверяем эквивалентность метрик.
    measured = [cell for cell in cells if cell.state is not CellState.PRELIMINARY]
    assert resolved_capability(cells, catalog, module_weights) == resolved_capability(
        measured, catalog, module_weights
    ), "I13: preliminary не должны влиять на Capability"
    assert evidence_coverage(cells, catalog, module_weights) == evidence_coverage(
        measured, catalog, module_weights
    ), "I13: preliminary не должны влиять на Coverage"

    report = sensitivity(measured, catalog, module_weights)
    per_product = _per_product_coverage(measured, catalog, module_weights)
    return ScoreBundle(
        capability=resolved_capability(measured, catalog, module_weights),
        coverage=evidence_coverage(measured, catalog, module_weights),
        failure_rate=failure_rate(measured),
        modules=module_scores(measured, catalog, module_weights),
        comparability=_comparability(cohort, per_product, alignment),
        sensitivity=report,
        requested_ranking=requested_ranking,
        # K6/A3: демотирование по sensitivity не переопределяется запрошенным режимом.
        computed_ranking=report.computed_ranking,
    )


# ---------------------------------------------------------------------------
# Публичный интерфейс
# ---------------------------------------------------------------------------


def compute(
    *,
    store: StorePort,
    run_id: str,
    catalog: Mapping[str, CatalogRow],
    module_weights: Mapping[str, float],
    cohort: Cohort | None = None,
    alignment: AlignmentReport | None = None,
    requested_ranking: RankingPolicy | str | None = None,
) -> ScoreBundle:
    """Собирает :class:`ScoreBundle` прогона из store (Ш8, §6, 02 §E.1).

    :param store: хранилище (``StorePort``): ячейки и (опц.) манифест запрошенного ранга.
    :param run_id: идентификатор прогона.
    :param catalog: ``{param_id: CatalogRow}`` — модуль и важность параметров.
    :param module_weights: ``{module_id: weight}`` — веса модулей (объявленные/перенормированные).
    :param cohort: когорта для Comparability; ``None`` → аннотация ``INSUFFICIENT``.
    :param alignment: alignment-условия Comparability (§6); ``None`` → ``cross_cycle``.
    :param requested_ranking: запрошенный режим (K6); ``None`` → манифест → дефолт.

    Формулы не дублируются: вызовы делегированы :mod:`core.domain.scoring`. ``computed_ranking``
    всегда берётся из sensitivity (K6). Пустой прогон обрабатывается детерминированно.
    """
    cells = list(store.iter_cells(run_id))
    requested = _resolve_requested_ranking(store, run_id, requested_ranking)
    return _compute_from_cells(
        cells,
        catalog=catalog,
        module_weights=module_weights,
        cohort=cohort,
        alignment=alignment,
        requested_ranking=requested,
    )


def per_product_bundle(
    store: StorePort,
    run_id: str,
    *,
    catalog: Mapping[str, CatalogRow],
    module_weights: Mapping[str, float],
    cohort: Cohort | None = None,
    alignment: AlignmentReport | None = None,
    requested_ranking: RankingPolicy | str | None = None,
) -> dict[str, ScoreBundle]:
    """Бандл на каждый продукт (для battle card): ``{product_id: ScoreBundle}``.

    Набор продуктов — когорта, если задана, иначе продукты, найденные в ячейках (сортировка
    для детерминизма). Comparability считается на **однопродуктовой** когорте продукта, чтобы
    уровень отражал его собственное покрытие (кросс-продуктовый уровень — у агрегатного
    :func:`compute`). Продукты когорты без ячеек дают пустой бандл.
    """
    cells = list(store.iter_cells(run_id))
    requested = _resolve_requested_ranking(store, run_id, requested_ranking)

    by_product: dict[str, list[Cell]] = {}
    for cell in cells:
        by_product.setdefault(cell.product_id, []).append(cell)

    if cohort is not None:
        product_ids = [entry.product_id for entry in cohort.entries]
    else:
        product_ids = sorted(by_product)

    result: dict[str, ScoreBundle] = {}
    for product_id in sorted(set(product_ids)):
        result[product_id] = _compute_from_cells(
            by_product.get(product_id, []),
            catalog=catalog,
            module_weights=module_weights,
            cohort=_single_product_cohort(cohort, product_id),
            alignment=alignment,
            requested_ranking=requested,
        )
    return result


def _single_product_cohort(cohort: Cohort | None, product_id: str) -> Cohort | None:
    """Однопродуктовая когорта для per-product Comparability (``None``, если когорты нет)."""
    if cohort is None:
        return None
    return Cohort(
        case_id=cohort.case_id,
        frozen_at=cohort.frozen_at,
        entries=[
            CohortEntry(
                product_id=product_id,
                role_overlap=1.0,
                selection_reason="per-product bundle",
                anchor_relation=AnchorRelation.ANCHOR,
            )
        ],
    )


def publish_warnings(bundle: ScoreBundle) -> list[str]:
    """Причины, по которым бандл непубликуем (I11: Capability без Coverage).

    Возвращает список предупреждений; пустой — публикация допустима (при ``INSUFFICIENT``
    покрытие приглушается, но не блокирует).
    """
    if bundle.coverage.n_used <= 0 or bundle.coverage.denominator <= 0:
        return [
            "I11: Resolved Capability без Coverage не публикуется "
            "(нет применимых/failed ячеек в знаменателе Coverage)"
        ]
    return []


def assert_publishable(bundle: ScoreBundle) -> None:
    """G4/I11 guard: предупреждает, если бандл публиковал бы Capability без Coverage.

    Мягкая проверка (``RuntimeWarning``): жёсткий блокирующий гейт G4 реализует
    ``pipeline/report.py`` (I11). Coverage из бандла не удаляется и не подменяется молча.
    """
    problems = publish_warnings(bundle)
    if problems:
        warnings.warn("; ".join(problems), RuntimeWarning, stacklevel=2)
