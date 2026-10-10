"""Тесты расчёта прогона (W10/T10.1, Ш8): ``pipeline/compute.py`` → ``domain/scoring``.

Покрывают: Capability/Coverage на смешанном наборе (I5 — ``failed`` вне Capability, но в
знаменателе Coverage; ``❓`` и ``N/A`` исключены), I13 (``preliminary`` вне всех расчётов),
``failure_rate``, модульные баллы, Comparability (уровни + cross-cycle), демотирование
sensitivity (K6: ``computed_ranking = coverage_fallback`` даже при запрошенном
``capability_ranked``), источник ``requested_ranking`` из манифеста, round-trip
``to_manifest_json``, пустой прогон и G4/I11-предупреждение без Coverage.

Работают на ``SqliteStore(":memory:")``; фикстуры категорийно нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import warnings
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain.enums import (
    AnchorRelation,
    CellState,
    Importance,
    RankingPolicy,
    RunMode,
    Status,
)
from core.domain.models import (
    AlignmentReport,
    CatalogRow,
    Cell,
    Cohort,
    CohortEntry,
    Focus,
    LLMEvidence,
    RunConfig,
)
from core.pipeline.compute import (
    ScoreBundle,
    assert_publishable,
    compute,
    per_product_bundle,
    publish_warnings,
    scope_universe,
    score_universe,
)
from core.store.sqlite_store import SqliteStore

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "compute_catalog.json"
RUN_ID = "run-compute"
PRODUCT_A = "prod.a"
PRODUCT_B = "prod.b"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _catalog() -> dict[str, CatalogRow]:
    """Нейтральный каталог из JSON-фикстуры."""
    rows = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return {row["param_id"]: CatalogRow(**row) for row in rows}


def _cfg(ranking: RankingPolicy = RankingPolicy.CAPABILITY_RANKED) -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort.yaml",
        products=[PRODUCT_A, PRODUCT_B],
        package="pkg-1",
        focus=Focus(),
        languages=["en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=ranking,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    """Хранилище с созданным прогоном (FK cells → runs включён)."""
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        yield s


def _cell(
    product_id: str,
    param_id: str,
    status: Status | None,
    state: CellState = CellState.EVALUATED,
) -> Cell:
    """Собирает валидную ячейку под инварианты I1/I2/I4/I6/I7/I13."""
    if state in {CellState.FAILED, CellState.PRELIMINARY}:
        return Cell(
            run_id=RUN_ID, product_id=product_id, param_id=param_id, state=state
        )
    if status in {Status.OK, Status.PARTIAL}:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            evidence=[LLMEvidence(source_id="src.1", quote="цитата")],
        )
    if status is Status.ABSENT:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            absence_evidence=[{"source_scope": "partial"}],
        )
    if status is Status.NOT_APPLICABLE:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            reviewed_by="human",
        )
    return Cell(
        run_id=RUN_ID, product_id=product_id, param_id=param_id, state=state, status=status
    )


def _save(store: SqliteStore, cells: list[Cell]) -> None:
    for cell in cells:
        store.save_cell(cell)


def _cohort(*product_ids: str) -> Cohort:
    entries = [
        CohortEntry(
            product_id=product_ids[0],
            role_overlap=1.0,
            selection_reason="anchor",
            anchor_relation=AnchorRelation.ANCHOR,
        )
    ]
    entries.extend(
        CohortEntry(
            product_id=pid,
            role_overlap=0.5,
            selection_reason="analog",
            anchor_relation=AnchorRelation.ANALOG,
        )
        for pid in product_ids[1:]
    )
    return Cohort(case_id="case-1", frozen_at=_dt.date(2026, 1, 1), entries=entries)


def _alignment(**overrides: object) -> AlignmentReport:
    data: dict[str, object] = {
        "snapshot_date": "2026-01-01",
        "retrieval_profile": "p1",
        "retrieval_cutoff_K": 10,
        "index_build_id": "build.1",
        "package_version": "1.0.0",
        "catalog_version": "1.0.0",
    }
    data.update(overrides)
    return AlignmentReport(**data)  # type: ignore[arg-type]


def _mixed_cells() -> list[Cell]:
    """Смешанный набор для ``PRODUCT_A``: ✅/⚠️/❌/❓/failed/N-A."""
    return [
        _cell(PRODUCT_A, "param.ok", Status.OK, CellState.EVALUATED),
        _cell(PRODUCT_A, "param.partial", Status.PARTIAL, CellState.EVALUATED),
        _cell(PRODUCT_A, "param.absent", Status.ABSENT, CellState.EVALUATED),
        _cell(PRODUCT_A, "param.unknown", Status.UNKNOWN, CellState.EVALUATED),
        _cell(PRODUCT_A, "param.na", Status.NOT_APPLICABLE, CellState.EVALUATED),
        _cell(PRODUCT_A, "param.fail", None, CellState.FAILED),
    ]


# ---------------------------------------------------------------------------
# Capability / Coverage / failure_rate / модули
# ---------------------------------------------------------------------------


def test_mixed_metrics_i5_and_exclusions(store: SqliteStore) -> None:
    _save(store, _mixed_cells())
    bundle = compute(
        store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={}
    )

    # Capability: только ✅(4)+⚠️(3*0.5)+❌(0) → 5.5/9; ❓/N/A/failed вне.
    assert bundle.capability.value == pytest.approx(5.5 / 9.0)
    assert bundle.capability.numerator == 5.5
    assert bundle.capability.denominator == 9.0
    assert bundle.capability.n_used == 3

    # Coverage: (✅⚠️❌)=9 / (9 ∪ ❓3 ∪ failed3)=15; N/A вне знаменателя.
    assert bundle.coverage.numerator == 9.0
    assert bundle.coverage.denominator == 15.0
    assert bundle.coverage.value == pytest.approx(0.6)
    assert bundle.coverage.n_used == 5

    # failure_rate = 1 / (5 evaluated + 1 failed).
    assert bundle.failure_rate == pytest.approx(1.0 / 6.0)

    modules = {m.module: m for m in bundle.modules}
    assert set(modules) == {"mod.one", "mod.two"}
    assert modules["mod.one"].score == pytest.approx(5.5 / 9.0)
    assert modules["mod.one"].coverage == pytest.approx(1.0)
    # mod.two: только ❓/N-A/failed → неприменим для Capability, Coverage=0.
    assert modules["mod.two"].score == pytest.approx(0.0)
    assert modules["mod.two"].coverage == pytest.approx(0.0)
    assert modules["mod.one"].applicable_weight == pytest.approx(0.5)
    assert modules["mod.two"].applicable_weight == pytest.approx(0.5)


def test_failed_in_coverage_denominator_only(store: SqliteStore) -> None:
    """I5: ``failed`` не входит в Capability, но добавляет вес в знаменатель Coverage."""
    catalog = _catalog()
    _save(store, _mixed_cells())
    with_failed = compute(store=store, run_id=RUN_ID, catalog=catalog, module_weights={})

    second = SqliteStore(":memory:")
    second.create_run(RUN_ID, _cfg())
    _save(second, [c for c in _mixed_cells() if c.state is not CellState.FAILED])
    without_failed = compute(
        store=second, run_id=RUN_ID, catalog=catalog, module_weights={}
    )
    second.close()

    # Capability не зависит от failed (вне числителя и знаменателя).
    assert with_failed.capability == without_failed.capability
    # failed — только знаменатель Coverage: числитель тот же, знаменатель больше, значение ниже.
    assert with_failed.coverage.numerator == without_failed.coverage.numerator
    assert with_failed.coverage.denominator > without_failed.coverage.denominator
    assert with_failed.coverage.value < without_failed.coverage.value
    assert with_failed.failure_rate > 0.0


def test_failure_rate(store: SqliteStore) -> None:
    _save(
        store,
        [
            _cell(PRODUCT_A, "param.ok", Status.OK),
            _cell(PRODUCT_A, "param.partial", Status.PARTIAL),
            _cell(PRODUCT_A, "param.fail", None, CellState.FAILED),
            _cell(PRODUCT_A, "param.absent", None, CellState.FAILED),
        ],
    )
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    assert bundle.failure_rate == pytest.approx(2.0 / 4.0)


def test_preliminary_excluded_from_everything(store: SqliteStore) -> None:
    """I13: preliminary не влияет ни на одну метрику и не создаёт фантомный модуль."""
    catalog = _catalog()
    _save(store, _mixed_cells())
    baseline = compute(store=store, run_id=RUN_ID, catalog=catalog, module_weights={})

    _save(store, [_cell(PRODUCT_A, "param.preliminary", None, CellState.PRELIMINARY)])
    with_prelim = compute(store=store, run_id=RUN_ID, catalog=catalog, module_weights={})

    assert with_prelim.capability == baseline.capability
    assert with_prelim.coverage == baseline.coverage
    assert with_prelim.failure_rate == baseline.failure_rate
    assert with_prelim.modules == baseline.modules
    assert "mod.three" not in {m.module for m in with_prelim.modules}


# ---------------------------------------------------------------------------
# Comparability
# ---------------------------------------------------------------------------


def test_comparability_levels_and_cross_cycle(store: SqliteStore) -> None:
    _save(store, _mixed_cells())  # prod.a → coverage 0.6
    _save(
        store,
        [
            _cell(PRODUCT_B, "param.ok", Status.OK),
            _cell(PRODUCT_B, "param.partial", Status.OK),
            _cell(PRODUCT_B, "param.absent", Status.OK),
        ],
    )  # prod.b → coverage 1.0
    catalog = _catalog()
    cohort = _cohort(PRODUCT_A, PRODUCT_B)

    aligned = _alignment()
    bundle = compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        cohort=cohort,
        alignment=aligned,
    )
    assert bundle.comparability.min_coverage == pytest.approx(0.6)
    assert bundle.comparability.level == "LOW"
    assert bundle.comparability.cross_cycle is False

    violated = _alignment(violations=["snapshot mismatch"])
    bundle_cc = compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        cohort=cohort,
        alignment=violated,
    )
    assert bundle_cc.comparability.cross_cycle is True
    assert bundle_cc.comparability.level == "INSUFFICIENT"  # LOW понижен на шаг
    assert "snapshot mismatch" in bundle_cc.comparability.reasons


def test_comparability_without_cohort_is_insufficient(store: SqliteStore) -> None:
    _save(store, _mixed_cells())
    bundle = compute(
        store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={}
    )
    assert bundle.comparability.level == "INSUFFICIENT"
    assert "cohort_missing" in bundle.comparability.reasons
    assert bundle.comparability.cross_cycle is True


# ---------------------------------------------------------------------------
# Sensitivity / K6
# ---------------------------------------------------------------------------


def test_sensitivity_demotion_k6_requested_never_overrides(store: SqliteStore) -> None:
    """K6: неустойчивый порядок демотирует computed_ranking даже при requested=capability."""
    _save(
        store,
        [
            _cell(PRODUCT_A, "param.p1", Status.OK),
            _cell(PRODUCT_A, "param.p2", Status.UNKNOWN),
            _cell(PRODUCT_B, "param.p1", Status.OK),
            _cell(PRODUCT_B, "param.p2", Status.PARTIAL),
        ],
    )
    catalog = _catalog()

    # requested приходит из манифеста (созданный прогон: capability_ranked).
    bundle = compute(store=store, run_id=RUN_ID, catalog=catalog, module_weights={})
    assert bundle.sensitivity.stable is False
    assert bundle.requested_ranking == RankingPolicy.CAPABILITY_RANKED.value
    assert bundle.computed_ranking == RankingPolicy.COVERAGE_FALLBACK.value

    # Запрошенный режим не повышает демотированный ранг (K6 join = min): при
    # requested=unranked итог остаётся unranked.
    forced = compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        requested_ranking=RankingPolicy.UNRANKED,
    )
    assert forced.requested_ranking == RankingPolicy.UNRANKED.value
    assert forced.computed_ranking == RankingPolicy.UNRANKED.value

    # requested=coverage_fallback при неустойчивости остаётся coverage_fallback.
    fallback = compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        requested_ranking=RankingPolicy.COVERAGE_FALLBACK,
    )
    assert fallback.computed_ranking == RankingPolicy.COVERAGE_FALLBACK.value


def test_requested_ranking_sourced_from_manifest() -> None:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg(ranking=RankingPolicy.UNRANKED))
        s.save_cell(_cell(PRODUCT_A, "param.ok", Status.OK))
        bundle = compute(
            store=s, run_id=RUN_ID, catalog=_catalog(), module_weights={}
        )
        assert bundle.requested_ranking == RankingPolicy.UNRANKED.value


# ---------------------------------------------------------------------------
# Манифест / пустой прогон / G4-I11
# ---------------------------------------------------------------------------


def test_to_manifest_json_round_trip(store: SqliteStore) -> None:
    _save(store, _mixed_cells())
    bundle = compute(
        store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={}
    )
    raw = bundle.to_manifest_json()
    assert raw == bundle.to_manifest_json()  # детерминирован
    assert json.loads(raw) == bundle.to_dict()


def test_empty_run_handled(store: SqliteStore) -> None:
    bundle = compute(
        store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={}
    )
    assert bundle.capability.value == 0.0
    assert bundle.coverage.value == 0.0
    assert bundle.coverage.n_used == 0
    assert bundle.failure_rate == 0.0
    assert bundle.modules == []
    assert bundle.computed_ranking == RankingPolicy.CAPABILITY_RANKED.value
    assert publish_warnings(bundle)  # нет Coverage → непубликуем


def test_unknown_run_id_uses_default_requested_ranking() -> None:
    with SqliteStore(":memory:") as s:
        bundle = compute(
            store=s, run_id="missing-run", catalog=_catalog(), module_weights={}
        )
        assert bundle.requested_ranking == RankingPolicy.CAPABILITY_RANKED.value
        assert bundle.capability.n_used == 0


def test_assert_publishable_warns_without_coverage(store: SqliteStore) -> None:
    # Только N/A → Coverage-знаменатель пуст (I11: нечего публиковать).
    _save(store, [_cell(PRODUCT_A, "param.na", Status.NOT_APPLICABLE)])
    empty_cov = compute(
        store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={}
    )
    with pytest.warns(RuntimeWarning):
        assert_publishable(empty_cov)

    # ✅ даёт Coverage → предупреждения нет, покрытие не теряется.
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.OK)])
    publishable = compute(
        store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={}
    )
    assert publishable.coverage.n_used > 0
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert_publishable(publishable)
    assert not [w for w in caught if issubclass(w.category, RuntimeWarning)]


# ---------------------------------------------------------------------------
# per_product_bundle
# ---------------------------------------------------------------------------


def test_per_product_bundle_groups_by_product(store: SqliteStore) -> None:
    _save(store, _mixed_cells())  # prod.a
    _save(
        store,
        [
            _cell(PRODUCT_B, "param.ok", Status.OK),
            _cell(PRODUCT_B, "param.partial", Status.OK),
            _cell(PRODUCT_B, "param.absent", Status.OK),
        ],
    )  # prod.b
    bundles = per_product_bundle(
        store,
        RUN_ID,
        catalog=_catalog(),
        module_weights={},
        cohort=_cohort(PRODUCT_A, PRODUCT_B),
        alignment=_alignment(),
    )
    assert set(bundles) == {PRODUCT_A, PRODUCT_B}
    assert isinstance(bundles[PRODUCT_A], ScoreBundle)
    assert bundles[PRODUCT_A].coverage.value == pytest.approx(0.6)
    assert bundles[PRODUCT_B].coverage.value == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# §6/B-3a: universe-scope в pipeline/compute
# ---------------------------------------------------------------------------


def test_scope_universe_cartesian_product_and_alias() -> None:
    catalog = _catalog()
    universe = scope_universe(catalog, [PRODUCT_A, PRODUCT_B])
    assert universe == {
        (product, param) for product in (PRODUCT_A, PRODUCT_B) for param in catalog
    }
    assert score_universe is scope_universe


def test_compute_universe_counts_missing_params(store: SqliteStore) -> None:
    """Отсутствующие параметры scope входят в знаменатель Coverage (§6/B-3a)."""
    catalog = _catalog()
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.OK)])  # Кр → вес 4
    universe = {
        (PRODUCT_A, "param.ok"),  # 4
        (PRODUCT_A, "param.partial"),  # Вж → 3
        (PRODUCT_A, "param.absent"),  # Баз → 2
    }
    bundle = compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        universe=universe,
    )
    assert bundle.coverage.numerator == 4.0
    assert bundle.coverage.universe == 9.0
    assert bundle.coverage.value == pytest.approx(4.0 / 9.0)
    assert bundle.coverage.completed == 4.0
    assert bundle.coverage.defined is True


def test_compute_universe_empty_is_undefined_and_unpublishable(
    store: SqliteStore,
) -> None:
    catalog = _catalog()
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.OK)])
    bundle = compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        universe=set(),
    )
    assert bundle.coverage.value is None
    assert bundle.coverage.defined is False
    assert publish_warnings(bundle)  # UNDEFINED → непубликуем (I11)


def test_compute_universe_preliminary_and_na_excluded(store: SqliteStore) -> None:
    """I13 + I7: preliminary и N/A исключены из знаменателя universe-покрытия."""
    catalog = _catalog()
    _save(
        store,
        [
            _cell(PRODUCT_A, "param.ok", Status.OK),
            _cell(PRODUCT_A, "param.preliminary", None, CellState.PRELIMINARY),
            _cell(PRODUCT_A, "param.na", Status.NOT_APPLICABLE),
        ],
    )
    universe = {
        (PRODUCT_A, "param.ok"),
        (PRODUCT_A, "param.preliminary"),
        (PRODUCT_A, "param.na"),
    }
    bundle = compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        universe=universe,
    )
    # Только param.ok (Кр=4) в знаменателе; preliminary/N-A вне.
    assert bundle.coverage.universe == 4.0
    assert bundle.coverage.value == 1.0
    assert bundle.coverage.completed == 4.0
    assert bundle.coverage.n_used == 1


def test_compute_ranking_epsilon_propagates(store: SqliteStore) -> None:
    _save(store, _mixed_cells())
    bundle = compute(
        store=store,
        run_id=RUN_ID,
        catalog=_catalog(),
        module_weights={},
        ranking_epsilon=0.5,
    )
    assert bundle.ranking_epsilon == 0.5
    assert bundle.sensitivity.ranking_epsilon == 0.5


def test_compute_requested_unranked_stable_joins_to_unranked(
    store: SqliteStore,
) -> None:
    _save(
        store,
        [
            _cell(PRODUCT_A, "param.ok", Status.OK),
            _cell(PRODUCT_B, "param.ok", Status.ABSENT),
        ],
    )
    bundle = compute(
        store=store,
        run_id=RUN_ID,
        catalog=_catalog(),
        module_weights={},
        requested_ranking=RankingPolicy.UNRANKED,
    )
    assert bundle.sensitivity.stable is True
    assert bundle.requested_ranking == RankingPolicy.UNRANKED.value
    assert bundle.computed_ranking == RankingPolicy.UNRANKED.value
