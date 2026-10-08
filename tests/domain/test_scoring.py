"""Юнит- и property-тесты расчёта §6 (W2/T2.3).

Покрывают: взвешивание ✅/⚠️/❌, исключение ❓ из Capability, ключевой инвариант I5
(`failed` вне Capability, но **в** знаменателе Coverage), исключение N/A, failure_rate,
пороги и cross-cycle-понижение Comparability, устойчивость/неустойчивость sensitivity
(демотирование до coverage_fallback), перенормировку модулей и пустой вход.
"""

from __future__ import annotations

import datetime as dt

from hypothesis import given
from hypothesis import strategies as st

from core.domain.enums import (
    AnchorRelation,
    CellState,
    ComparabilityLevel,
    Importance,
    ParamType,
    RankingPolicy,
    Status,
    UnknownPolicy,
)
from core.domain.models import (
    AlignmentReport,
    CatalogRow,
    Cell,
    Cohort,
    CohortEntry,
    LLMEvidence,
)
from core.domain.scoring import (
    Coverage,
    ModuleScore,
    Score,
    comparability,
    evidence_coverage,
    failure_rate,
    module_scores,
    resolved_capability,
    sensitivity,
)

# ---------------------------------------------------------------------------
# Фикстуры-помощники
# ---------------------------------------------------------------------------


def _param(
    param_id: str,
    *,
    module: str = "m1",
    importance: Importance = Importance.BASIC,
) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=param_id,
        module=module,
        group="g",
        importance=importance,
        param_type=ParamType.FACT,
        rule="правило",
    )


def _cell(
    product_id: str,
    param_id: str,
    status: Status | None,
    state: CellState = CellState.EVALUATED,
) -> Cell:
    """Собирает валидную ячейку под требования инвариантов I1/I2/I4/I7."""
    if state is CellState.FAILED:
        return Cell(run_id="r1", product_id=product_id, param_id=param_id, state=state)  # I4
    if status in {Status.OK, Status.PARTIAL}:
        return Cell(
            run_id="r1",
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            evidence=[LLMEvidence(source_id="s1", quote="q")],
        )
    if status is Status.ABSENT:
        return Cell(
            run_id="r1",
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            absence_evidence=[{"source_scope": "partial"}],
        )
    if status is Status.NOT_APPLICABLE:
        return Cell(
            run_id="r1",
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            reviewed_by="human",
        )
    return Cell(
        run_id="r1", product_id=product_id, param_id=param_id, state=state, status=status
    )


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
    return Cohort(case_id="c1", frozen_at=dt.date(2026, 1, 1), entries=entries)


def _alignment(**overrides: object) -> AlignmentReport:
    data: dict[str, object] = {
        "snapshot_date": "2026-01-01",
        "retrieval_profile": "p1",
        "retrieval_cutoff_K": 10,
        "index_build_id": "b1",
        "package_version": "1.0.0",
        "catalog_version": "1.0.0",
    }
    data.update(overrides)
    return AlignmentReport(**data)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Resolved Capability — взвешивание и исключения
# ---------------------------------------------------------------------------


def test_resolved_capability_weights_by_importance() -> None:
    params = {
        "p_ok": _param("p_ok", importance=Importance.CRITICAL),
        "p_part": _param("p_part", importance=Importance.IMPORTANT),
        "p_abs": _param("p_abs", importance=Importance.BASIC),
    }
    cells = [
        _cell("a", "p_ok", Status.OK),
        _cell("a", "p_part", Status.PARTIAL),
        _cell("a", "p_abs", Status.ABSENT),
    ]
    score = resolved_capability(cells, params, {})
    # (4*1 + 3*0.5 + 2*0) / (4+3+2) = 5.5/9
    assert score == Score(value=5.5 / 9.0, numerator=5.5, denominator=9.0, n_used=3)
    assert 0.0 <= score.value <= 1.0


def test_resolved_capability_per_param_weight_override() -> None:
    params = {"p1": _param("p1", importance=Importance.CRITICAL), "p2": _param("p2")}
    cells = [_cell("a", "p1", Status.OK), _cell("a", "p2", Status.ABSENT)]
    # Явный per-param вес переопределяет важность: p1=1, p2=1 → 1/2.
    score = resolved_capability(cells, params, {"p1": 1.0, "p2": 1.0})
    assert score.value == 0.5
    assert score.denominator == 2.0


def test_unknown_excluded_from_capability() -> None:
    params = {
        "p_ok": _param("p_ok", importance=Importance.CRITICAL),
        "p_unk": _param("p_unk", importance=Importance.IMPORTANT),
    }
    cells = [_cell("a", "p_ok", Status.OK), _cell("a", "p_unk", Status.UNKNOWN)]
    score = resolved_capability(cells, params, {})
    # ❓ нет ни в числителе, ни в знаменателе: 4*1/4.
    assert score.value == 1.0
    assert score.numerator == 4.0
    assert score.denominator == 4.0
    assert score.n_used == 1


def test_failed_excluded_capability_but_in_coverage_denominator() -> None:
    """Ключевой инвариант I5: failed вне Capability, но в знаменателе Coverage."""
    params = {
        "p_ok": _param("p_ok", importance=Importance.CRITICAL),
        "p_fail": _param("p_fail", importance=Importance.BASIC),
    }
    cells = [
        _cell("a", "p_ok", Status.OK),
        _cell("a", "p_fail", None, CellState.FAILED),
    ]
    capability = resolved_capability(cells, params, {})
    coverage = evidence_coverage(cells, params, {})
    # Capability: только ✅ (вес 4) → 1.0, знаменатель 4.
    assert capability.value == 1.0
    assert capability.denominator == 4.0
    # Coverage: numerator=4 (✅), denominator=4(✅)+2(failed)=6.
    assert coverage.numerator == 4.0
    assert coverage.denominator == 6.0
    assert coverage.value == 4.0 / 6.0
    assert coverage.n_used == 2


def test_na_excluded_from_both_metrics() -> None:
    params = {
        "p_ok": _param("p_ok", importance=Importance.CRITICAL),
        "p_na": _param("p_na", importance=Importance.IMPORTANT),
    }
    cells = [
        _cell("a", "p_ok", Status.OK),
        _cell("a", "p_na", Status.NOT_APPLICABLE),
    ]
    capability = resolved_capability(cells, params, {})
    coverage = evidence_coverage(cells, params, {})
    assert capability.value == 1.0
    assert capability.denominator == 4.0
    assert coverage.value == 1.0
    assert coverage.denominator == 4.0
    assert coverage.n_used == 1


def test_unknown_counts_in_coverage_denominator() -> None:
    params = {
        "p_ok": _param("p_ok", importance=Importance.CRITICAL),
        "p_unk": _param("p_unk", importance=Importance.BASIC),
    }
    cells = [_cell("a", "p_ok", Status.OK), _cell("a", "p_unk", Status.UNKNOWN)]
    coverage = evidence_coverage(cells, params, {})
    assert coverage.denominator == 6.0  # ❓ применим → в знаменателе
    assert coverage.numerator == 4.0
    assert coverage.value == 4.0 / 6.0


# ---------------------------------------------------------------------------
# failure_rate
# ---------------------------------------------------------------------------


def test_failure_rate() -> None:
    cells = [
        _cell("a", "p1", Status.OK),
        _cell("a", "p2", Status.PARTIAL),
        _cell("a", "p3", Status.ABSENT),
        _cell("a", "p4", None, CellState.FAILED),
        _cell("a", "p5", None, CellState.FAILED),
    ]
    assert failure_rate(cells) == 2.0 / 5.0


def test_failure_rate_empty_is_zero() -> None:
    assert failure_rate([]) == 0.0


# ---------------------------------------------------------------------------
# Comparability
# ---------------------------------------------------------------------------


def test_comparability_thresholds() -> None:
    cohort = _cohort("a", "b")
    assert comparability(cohort, {"a": 0.95, "b": 0.92}, _alignment()).level == "HIGH"
    assert comparability(cohort, {"a": 0.80, "b": 0.90}, _alignment()).level == "MEDIUM"
    assert comparability(cohort, {"a": 0.60, "b": 0.90}, _alignment()).level == "LOW"
    assert comparability(cohort, {"a": 0.30, "b": 0.90}, _alignment()).level == "INSUFFICIENT"


def test_comparability_min_is_over_products_and_accepts_percent() -> None:
    cohort = _cohort("a", "b")
    result = comparability(cohort, {"a": 95, "b": 60}, _alignment())
    assert result.min_coverage == 0.60
    assert result.level == "LOW"


def test_comparability_missing_product_is_zero() -> None:
    cohort = _cohort("a", "b")
    result = comparability(cohort, {"a": 0.95}, _alignment())
    assert result.min_coverage == 0.0
    assert result.level == "INSUFFICIENT"


def test_comparability_cross_cycle_downgrade_on_violations() -> None:
    cohort = _cohort("a", "b")
    report = comparability(
        cohort, {"a": 0.95, "b": 0.95}, _alignment(violations=["snapshot mismatch"])
    )
    assert report.cross_cycle is True
    assert report.level == "MEDIUM"  # HIGH понижен на один шаг
    assert "snapshot mismatch" in report.reasons


def test_comparability_cross_cycle_on_inconsistent_editions() -> None:
    cohort = _cohort("a")
    report = comparability(
        cohort, {"a": 0.60}, _alignment(editions_consistent=False)
    )
    assert report.cross_cycle is True
    assert report.level == "INSUFFICIENT"  # LOW понижен
    assert "editions_inconsistent" in report.reasons


def test_comparability_cross_cycle_missing_alignment() -> None:
    cohort = _cohort("a")
    report = comparability(cohort, {"a": 0.95}, None)
    assert report.cross_cycle is True
    assert report.level == "MEDIUM"
    assert report.reasons


# ---------------------------------------------------------------------------
# Sensitivity
# ---------------------------------------------------------------------------


def test_sensitivity_stable_ranking() -> None:
    params = {
        "p1": _param("p1", importance=Importance.CRITICAL),
        "p2": _param("p2", importance=Importance.IMPORTANT),
    }
    cells = [
        _cell("a", "p1", Status.OK),
        _cell("a", "p2", Status.OK),
        _cell("b", "p1", Status.ABSENT),
        _cell("b", "p2", Status.ABSENT),
    ]
    report = sensitivity(cells, params, {})
    assert report.stable is True
    assert report.unstable_pairs == []
    assert report.computed_ranking == RankingPolicy.CAPABILITY_RANKED.value
    assert set(report.per_axis) == {"partial_score", "unknown_policy", "weights"}
    assert report.per_axis["unknown_policy"]["stable"] is True


def test_sensitivity_unstable_unknown_policy_axis_demotes() -> None:
    """Смена политики ❓ меняет порядок пары → демотирование до coverage_fallback (K6)."""
    params = {
        "p1": _param("p1", importance=Importance.CRITICAL),
        "p2": _param("p2", importance=Importance.IMPORTANT),
    }
    cells = [
        _cell("a", "p1", Status.OK),
        _cell("a", "p2", Status.UNKNOWN),
        _cell("b", "p1", Status.OK),
        _cell("b", "p2", Status.PARTIAL),
    ]
    report = sensitivity(cells, params, {})
    assert report.stable is False
    assert ("a", "b") in report.unstable_pairs
    assert report.computed_ranking == RankingPolicy.COVERAGE_FALLBACK.value
    assert report.requested_ranking == RankingPolicy.CAPABILITY_RANKED.value
    assert report.per_axis["unknown_policy"]["stable"] is False


def test_sensitivity_unknown_as_zero_policy_increases_denominator() -> None:
    params = {
        "p1": _param("p1", importance=Importance.CRITICAL),
        "p2": _param("p2", importance=Importance.IMPORTANT),
    }
    # Оба продукта с ❓: при unknown_as_zero знаменатель включает ❓.
    cells = [
        _cell("a", "p1", Status.OK),
        _cell("a", "p2", Status.UNKNOWN),
    ]
    from core.domain.scoring import _resolved_capability  # локальный вызов ядра

    excluded = _resolved_capability(cells, params, {}, unknown_policy=UnknownPolicy.EXCLUDED)
    as_zero = _resolved_capability(cells, params, {}, unknown_policy=UnknownPolicy.UNKNOWN_AS_ZERO)
    assert excluded.value == 1.0 and excluded.denominator == 4.0
    assert as_zero.value == 4.0 / 7.0 and as_zero.denominator == 7.0


# ---------------------------------------------------------------------------
# module_scores — перенормировка на применимые
# ---------------------------------------------------------------------------


def test_module_scores_renormalization_on_applicable() -> None:
    params = {
        "p1": _param("p1", module="m1"),
        "p2": _param("p2", module="m2"),
        "p3": _param("p3", module="m3"),
    }
    cells = [
        _cell("a", "p1", Status.OK),
        _cell("a", "p2", Status.ABSENT),
        _cell("a", "p3", Status.NOT_APPLICABLE),
    ]
    weights = {"m1": 60.0, "m2": 40.0, "m3": 20.0}
    scores = {m.module: m for m in module_scores(cells, params, weights)}
    assert set(scores) == {"m1", "m2", "m3"}
    assert scores["m1"].score == 1.0
    assert scores["m2"].score == 0.0
    # m3 — только N/A → неприменим, вес не участвует.
    assert scores["m3"].applicable_weight == 0.0
    assert scores["m3"].coverage == 0.0
    # Перенормировка только на применимые m1+m2 (60+40).
    assert scores["m1"].applicable_weight == 0.6
    assert scores["m2"].applicable_weight == 0.4
    assert abs(scores["m1"].applicable_weight + scores["m2"].applicable_weight - 1.0) < 1e-12


def test_module_scores_importance_weighting_within_module() -> None:
    params = {
        "p_ok": _param("p_ok", module="m1", importance=Importance.CRITICAL),
        "p_abs": _param("p_abs", module="m1", importance=Importance.BASIC),
    }
    cells = [_cell("a", "p_ok", Status.OK), _cell("a", "p_abs", Status.ABSENT)]
    scores = module_scores(cells, params, {})
    # (4*1 + 2*0) / (4+2) = 4/6
    assert scores[0].module == "m1"
    assert scores[0].score == 4.0 / 6.0
    assert scores[0].coverage == 1.0


# ---------------------------------------------------------------------------
# Пустой вход
# ---------------------------------------------------------------------------


def test_empty_inputs() -> None:
    cap = resolved_capability([], {}, {})
    assert isinstance(cap, Score)
    assert (cap.value, cap.numerator, cap.denominator, cap.n_used) == (0.0, 0.0, 0.0, 0)
    cov = evidence_coverage([], {}, {})
    assert isinstance(cov, Coverage)
    assert (cov.value, cov.numerator, cov.denominator, cov.n_used) == (0.0, 0.0, 0.0, 0)
    assert failure_rate([]) == 0.0
    assert module_scores([], {}, {}) == []
    report = sensitivity([], {}, {})
    assert report.stable is True
    assert report.unstable_pairs == []


def test_empty_comparability_is_insufficient() -> None:
    cohort = _cohort("a")
    result = comparability(cohort, {}, _alignment())
    assert result.level == ComparabilityLevel.INSUFFICIENT.value
    assert result.min_coverage == 0.0


# ---------------------------------------------------------------------------
# Property-тесты (hypothesis)
# ---------------------------------------------------------------------------

_statuses = st.sampled_from(list(Status))
_states = st.sampled_from([CellState.EVALUATED, CellState.FAILED])


@st.composite
def _datasets(draw: st.DrawFn) -> tuple[list[Cell], dict[str, CatalogRow]]:
    n = draw(st.integers(min_value=0, max_value=8))
    cells: list[Cell] = []
    params: dict[str, CatalogRow] = {}
    for i in range(n):
        pid = f"p{i}"
        importance = draw(st.sampled_from(list(Importance)))
        module = draw(st.sampled_from(["m1", "m2"]))
        params[pid] = _param(pid, module=module, importance=importance)
        state = draw(_states)
        status = draw(_statuses)
        cells.append(_cell("a", pid, status, state))
    return cells, params


@given(_datasets())
def test_property_metrics_within_bounds(data: tuple[list[Cell], dict[str, CatalogRow]]) -> None:
    cells, params = data
    cap = resolved_capability(cells, params, {})
    cov = evidence_coverage(cells, params, {})
    assert 0.0 <= cap.value <= 1.0
    assert 0.0 <= cov.value <= 1.0
    # Числитель не превышает знаменатель (взвешенные неотрицательные вклады).
    assert cap.denominator == 0.0 or cap.numerator <= cap.denominator + 1e-9
    assert cov.denominator == 0.0 or cov.numerator <= cov.denominator + 1e-9
    assert 0.0 <= failure_rate(cells) <= 1.0


@given(_datasets())
def test_property_failed_never_in_capability(
    data: tuple[list[Cell], dict[str, CatalogRow]],
) -> None:
    """I5: ячейки failed никогда не попадают в числитель Capability."""
    cells, params = data
    failed = [c for c in cells if c.state is CellState.FAILED]
    cap_with = resolved_capability(cells, params, {})
    cap_without = resolved_capability(
        [c for c in cells if c.state is not CellState.FAILED], params, {}
    )
    assert failed or cap_with == cap_without
    assert cap_with.numerator == cap_without.numerator


@given(_datasets())
def test_property_coverage_monotone_on_removing_failed(
    data: tuple[list[Cell], dict[str, CatalogRow]],
) -> None:
    """I5: удаление failed не уменьшает Coverage (они лишь в знаменателе)."""
    cells, params = data
    with_failed = evidence_coverage(cells, params, {})
    without_failed = evidence_coverage(
        [c for c in cells if c.state is not CellState.FAILED], params, {}
    )
    assert with_failed.value <= without_failed.value + 1e-9
