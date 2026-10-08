"""Тесты ``core/pipeline/metrics.py`` (W12/T12.1, 04 §B.2).

Покрывают: счётчики по состояниям, ``failure_rate``/``coverage_aggregate`` из
``ScoreBundle`` и из ячеек, ``search_calls_by_phase`` по duck-typed логу,
``to_manifest_json`` round-trip, пустые входы, детерминированный порядок ключей,
инжектируемые длительности и источник ячеек ``store.iter_cells``.

Фикстуры категорийно-нейтральны (§1.3): ``prod_a``/``param_x``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from core.domain.enums import CellState, SearchPhase
from core.domain.scoring import Comparability, Coverage, Score, SensitivityReport
from core.pipeline.compute import ScoreBundle
from core.pipeline.metrics import RunMetrics, collect_phase_durations, compute_metrics


@dataclass
class _Cell:
    """Минимальная ячейка: счётчикам нужен только ``state``."""

    state: CellState
    product_id: str = "prod_a"
    param_id: str = "param_x"


def _bundle(failure: float = 0.25, coverage: float = 0.8) -> ScoreBundle:
    """Реальный ``ScoreBundle`` с заданными ``failure_rate``/Coverage."""
    return ScoreBundle(
        capability=Score(value=0.5, numerator=1.0, denominator=2.0, n_used=2),
        coverage=Coverage(value=coverage, numerator=2.0, denominator=2.5, n_used=2),
        failure_rate=failure,
        modules=[],
        comparability=Comparability(
            level="insufficient", min_coverage=0.0, cross_cycle=False, reasons=[]
        ),
        sensitivity=SensitivityReport(
            stable=True,
            per_axis={},
            unstable_pairs=[],
            requested_ranking="capability_ranked",
            computed_ranking="capability_ranked",
        ),
        requested_ranking="capability_ranked",
        computed_ranking="capability_ranked",
    )


# --- cells_by_state -----------------------------------------------------------


def test_cells_by_state_counts() -> None:
    cells = [
        _Cell(CellState.EVALUATED),
        _Cell(CellState.EVALUATED),
        _Cell(CellState.FAILED),
        _Cell(CellState.PENDING),
    ]
    metrics = compute_metrics(cells=cells)
    assert metrics.cells_by_state == {
        "evaluated": 2,
        "failed": 1,
        "pending": 1,
        "preliminary": 0,
        "reviewed": 0,
    }


def test_cells_by_state_keys_are_sorted() -> None:
    metrics = compute_metrics(cells=[])
    keys = list(metrics.cells_by_state)
    assert keys == sorted(keys)
    assert set(keys) == {state.value for state in CellState}


# --- failure_rate / coverage --------------------------------------------------


def test_failure_rate_and_coverage_from_score_bundle() -> None:
    metrics = compute_metrics(cells=[], score_bundle=_bundle(failure=0.4, coverage=0.9))
    assert metrics.failure_rate == pytest.approx(0.4)
    assert metrics.coverage_aggregate == pytest.approx(0.9)


def test_failure_rate_derived_from_cells_without_bundle() -> None:
    cells = [
        _Cell(CellState.EVALUATED),
        _Cell(CellState.REVIEWED),
        _Cell(CellState.FAILED),
        _Cell(CellState.FAILED),
    ]
    metrics = compute_metrics(cells=cells)
    assert metrics.failure_rate == pytest.approx(2 / 4)
    assert metrics.coverage_aggregate == 0.0


# --- search_calls_by_phase ----------------------------------------------------


def test_search_calls_by_phase_duck_typed() -> None:
    log = [
        SimpleNamespace(phase="discovery"),
        SimpleNamespace(phase="discovery"),
        SimpleNamespace(phase=SearchPhase.CORPUS),
        SimpleNamespace(phase=None),  # без фазы — пропускается
    ]
    metrics = compute_metrics(search_log=log)
    assert metrics.search_calls_by_phase == {"corpus": 1, "discovery": 2}


# --- llm_calls / tokens -------------------------------------------------------


def test_llm_calls_and_tokens_passthrough() -> None:
    metrics = compute_metrics(llm_calls=7, tokens={"prompt": 10, "completion": 5})
    assert metrics.llm_calls == 7
    assert metrics.tokens == {"completion": 5, "prompt": 10}


# --- детерминизм и JSON -------------------------------------------------------


def test_deterministic_ordering() -> None:
    metrics = compute_metrics(
        tokens={"z": 1, "a": 2},
        phase_durations={"report": 3.0, "evaluate": 1.0},
        search_log=[SimpleNamespace(phase="scout"), SimpleNamespace(phase="corpus")],
    )
    assert list(metrics.tokens) == ["a", "z"]
    assert list(metrics.duration_by_phase) == ["evaluate", "report"]
    assert list(metrics.search_calls_by_phase) == ["corpus", "scout"]


def test_to_manifest_json_round_trip() -> None:
    metrics = compute_metrics(
        cells=[_Cell(CellState.EVALUATED)],
        score_bundle=_bundle(failure=0.1, coverage=0.75),
        search_log=[SimpleNamespace(phase="corpus")],
        phase_durations={"evaluate": 2.5},
        llm_calls=3,
        tokens={"prompt": 4},
    )
    assert json.loads(metrics.to_manifest_json()) == metrics.to_dict()
    # Детерминированность: повторная сериализация идентична.
    assert metrics.to_manifest_json() == metrics.to_manifest_json()


def test_empty_inputs() -> None:
    metrics = compute_metrics()
    assert isinstance(metrics, RunMetrics)
    assert metrics.cells_by_state == {
        "evaluated": 0,
        "failed": 0,
        "pending": 0,
        "preliminary": 0,
        "reviewed": 0,
    }
    assert metrics.failure_rate == 0.0
    assert metrics.coverage_aggregate == 0.0
    assert metrics.search_calls_by_phase == {}
    assert metrics.llm_calls == 0
    assert metrics.tokens == {}
    assert metrics.duration_by_phase == {}


# --- источник ячеек -----------------------------------------------------------


def test_cells_read_from_store() -> None:
    stored = [_Cell(CellState.EVALUATED), _Cell(CellState.FAILED)]
    store = SimpleNamespace(iter_cells=lambda run_id: iter(stored))
    metrics = compute_metrics(store=store, run_id="run_1")
    assert metrics.cells_by_state["evaluated"] == 1
    assert metrics.cells_by_state["failed"] == 1


def test_explicit_cells_take_priority_over_store() -> None:
    stored = [_Cell(CellState.EVALUATED), _Cell(CellState.EVALUATED)]
    store = SimpleNamespace(iter_cells=lambda run_id: iter(stored))
    metrics = compute_metrics(store=store, run_id="run_1", cells=[_Cell(CellState.PENDING)])
    assert metrics.cells_by_state["pending"] == 1
    assert metrics.cells_by_state["evaluated"] == 0


# --- collect_phase_durations --------------------------------------------------


def test_collect_phase_durations_from_mapping() -> None:
    assert collect_phase_durations(None, {"report": 2.0, "evaluate": 1.0}) == {
        "evaluate": 1.0,
        "report": 2.0,
    }


def test_collect_phase_durations_from_report_wins() -> None:
    report = SimpleNamespace(
        results=[SimpleNamespace(phase="evaluate", duration=3.5)]
    )
    result = collect_phase_durations(report, {"evaluate": 1.0, "report": 9.0})
    assert result == {"evaluate": 3.5, "report": 9.0}


def test_collect_phase_durations_without_timing_falls_back_to_mapping() -> None:
    report = SimpleNamespace(results=[SimpleNamespace(phase="evaluate")])
    assert collect_phase_durations(report, {"evaluate": 4.0}) == {"evaluate": 4.0}


def test_phase_durations_flow_into_metrics() -> None:
    durations = collect_phase_durations(None, {"report": 5.0, "corpus": 1.0})
    metrics = compute_metrics(phase_durations=durations)
    assert metrics.duration_by_phase == {"corpus": 1.0, "report": 5.0}
