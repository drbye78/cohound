"""Тесты :mod:`core.pipeline.observability` (W12/T12.1, 04 §B.1/§B.2).

Покрывают: неизменность исходного ``PhaseResult``, одну JSON-lines-запись-файл на фазу с
полями §B.1, ``WARN`` на не-``ok`` и ``ERROR`` на исключении, захват длительностей,
детерминированный ``ts`` по инжектируемым часам, ``write_run_metrics`` (ключи §B.2,
детерминизм/``sort_keys``, источник ячеек), ``component``-override и отсутствие импорта
``core.adapters``/``core.edge`` (AST, матрица 01 §1).

Фикстуры категорийно-нейтральны (§1.3): ``run_x``/``normalize``/``param_x``.
"""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core.pipeline.metrics import compute_metrics
from core.pipeline.observability import (
    InstrumentedPhases,
    instrument_phases,
    write_run_metrics,
)
from core.domain.enums import SkipReason
from core.pipeline.runner import PhaseResult

#: Поля записи §B.1 (04-reliability-adr).
B1_FIELDS = {"ts", "run_id", "phase", "component", "level", "message", "ctx", "invariant"}

#: Ключи метрик §B.2 (+ Backlog A ``crawl``).
B2_KEYS = {
    "cells_by_state",
    "failure_rate",
    "coverage_aggregate",
    "search_calls_by_phase",
    "llm_calls",
    "tokens",
    "duration_by_phase",
    "crawl",
}

STAMP = "2026-10-09T00:00:00+00:00"


@dataclass
class _Clock:
    """Детерминированные часы (``now_iso`` — контракт ``runner.Clock``)."""

    stamp: str = STAMP

    def now_iso(self) -> str:
        return self.stamp


def _ctx() -> Any:
    """Минимальный контекст фазы: обёртки лишь транслируют его в оригинал."""
    return SimpleNamespace(run_id="run_x", store=None, ports=None, clock=None)


def _events(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


# ---------------------------------------------------------------------------
# Неизменность результата
# ---------------------------------------------------------------------------


def test_wrapped_phase_returns_original_result_unchanged(tmp_path: Path) -> None:
    original = PhaseResult(status="blocked", phase="g1", errors=["гейт не пройден"])

    def fn(ctx: Any) -> PhaseResult:
        return original

    instrumented = instrument_phases(
        {"g1": fn}, run_id="run_x", logs_root=tmp_path, clock=_Clock()
    )
    result = instrumented.phases["g1"](_ctx())
    # Возвращается ТОТ ЖЕ объект: status/cells/errors не переписываются.
    assert result is original
    assert result.status == "blocked"
    assert result.errors == ["гейт не пройден"]


def test_wrapped_phase_passes_context_through(tmp_path: Path) -> None:
    seen: list[Any] = []

    def fn(ctx: Any) -> PhaseResult:
        seen.append(ctx)
        return PhaseResult(status="ok", phase="normalize")

    ctx = _ctx()
    instrumented = instrument_phases(
        {"normalize": fn}, run_id="run_x", logs_root=tmp_path, clock=_Clock()
    )
    instrumented.phases["normalize"](ctx)
    assert seen == [ctx]


# ---------------------------------------------------------------------------
# Логи §B.1
# ---------------------------------------------------------------------------


def test_each_phase_writes_one_jsonl_file_with_b1_fields(tmp_path: Path) -> None:
    def ok(ctx: Any) -> PhaseResult:
        return PhaseResult(status="ok", phase="")

    instrumented = instrument_phases(
        {"normalize": ok, "lint": ok},
        run_id="run_x",
        logs_root=tmp_path,
        clock=_Clock(),
    )
    for phase in ("normalize", "lint"):
        instrumented.phases[phase](_ctx())

    for phase in ("normalize", "lint"):
        path = tmp_path / "run_x" / "logs" / f"{phase}.jsonl"
        assert path.is_file()
        events = _events(path)
        assert len(events) == 2  # ровно старт + конец
        assert [event["level"] for event in events] == ["INFO", "INFO"]
        for event in events:
            assert set(event) == B1_FIELDS
            assert event["ts"] == STAMP
            assert event["run_id"] == "run_x"
            assert event["phase"] == phase
            assert event["component"] == phase
            assert event["invariant"] is None
        assert events[0]["ctx"] == {"event": "start"}
        assert events[1]["ctx"] == {
            "event": "end",
            "status": "ok",
            "cells": 0,
            "errors": 0,
            "skip_reason": None,
            "pending": 0,
            "pending_items": [],
        }


def test_non_ok_end_is_warn_and_carries_errors_count(tmp_path: Path) -> None:
    def blocked(ctx: Any) -> PhaseResult:
        return PhaseResult(status="blocked", phase="g1", errors=["нет решения"])

    instrumented = instrument_phases(
        {"g1": blocked}, run_id="run_x", logs_root=tmp_path, clock=_Clock()
    )
    instrumented.phases["g1"](_ctx())
    events = _events(tmp_path / "run_x" / "logs" / "g1.jsonl")
    assert [event["level"] for event in events] == ["INFO", "WARN"]
    assert events[1]["ctx"]["status"] == "blocked"
    assert events[1]["ctx"]["errors"] == 1


def test_skipped_end_surfaces_skip_reason_and_pending(tmp_path: Path) -> None:
    """P2b: ``skipped`` логируется ``INFO`` и несёт машинную причину + advisory pending."""
    reason = SkipReason.NO_QUERIES
    result = PhaseResult(
        status="skipped",
        phase="discovery",
        skip_reason=reason,
        pending=["нужен человек: задать запросы разведки"],
    )

    def fn(ctx: Any) -> PhaseResult:
        return result

    instrumented = instrument_phases(
        {"discovery": fn}, run_id="run_x", logs_root=tmp_path, clock=_Clock()
    )
    instrumented.phases["discovery"](_ctx())
    events = _events(tmp_path / "run_x" / "logs" / "discovery.jsonl")

    assert [event["level"] for event in events] == ["INFO", "INFO"]  # не WARN
    ctx = events[1]["ctx"]
    assert ctx["status"] == "skipped"
    assert ctx["skip_reason"] == reason.value
    assert ctx["pending"] == 1
    assert ctx["pending_items"] == ["нужен человек: задать запросы разведки"]


def test_phase_exception_logs_error_and_reraises(tmp_path: Path) -> None:
    def boom(ctx: Any) -> PhaseResult:
        raise RuntimeError("сбой фазы")

    instrumented = instrument_phases(
        {"corpus": boom}, run_id="run_x", logs_root=tmp_path, clock=_Clock()
    )
    with pytest.raises(RuntimeError, match="сбой фазы"):
        instrumented.phases["corpus"](_ctx())
    events = _events(tmp_path / "run_x" / "logs" / "corpus.jsonl")
    assert [event["level"] for event in events] == ["INFO", "ERROR"]
    assert "corpus" in instrumented.durations  # тайминг зафиксирован даже при исключении


# ---------------------------------------------------------------------------
# Тайминг и детерминизм
# ---------------------------------------------------------------------------


def test_durations_captured_per_phase(tmp_path: Path) -> None:
    def ok(ctx: Any) -> PhaseResult:
        return PhaseResult(status="ok", phase="")

    instrumented = instrument_phases(
        {"plan": ok, "compute": ok}, run_id="run_x", logs_root=tmp_path, clock=_Clock()
    )
    for phase in ("plan", "compute"):
        instrumented.phases[phase](_ctx())
    assert set(instrumented.durations) == {"plan", "compute"}
    assert all(
        isinstance(seconds, float) and seconds >= 0.0
        for seconds in instrumented.durations.values()
    )


def test_logs_are_deterministic_with_injected_clock(tmp_path: Path) -> None:
    def ok(ctx: Any) -> PhaseResult:
        return PhaseResult(status="ok", phase="evaluate")

    first = instrument_phases(
        {"evaluate": ok}, run_id="run_x", logs_root=tmp_path / "a", clock=_Clock()
    )
    second = instrument_phases(
        {"evaluate": ok}, run_id="run_x", logs_root=tmp_path / "b", clock=_Clock()
    )
    first.phases["evaluate"](_ctx())
    second.phases["evaluate"](_ctx())
    path_a = tmp_path / "a" / "run_x" / "logs" / "evaluate.jsonl"
    path_b = tmp_path / "b" / "run_x" / "logs" / "evaluate.jsonl"
    assert path_a.read_text(encoding="utf-8") == path_b.read_text(encoding="utf-8")


def test_component_override_and_logger_access(tmp_path: Path) -> None:
    def ok(ctx: Any) -> PhaseResult:
        return PhaseResult(status="ok", phase="evaluate")

    instrumented = instrument_phases(
        {"evaluate": ok},
        run_id="run_x",
        logs_root=tmp_path,
        clock=_Clock(),
        component="llm",
    )
    assert instrumented.logger_for("evaluate").component == "llm"
    instrumented.phases["evaluate"](_ctx())
    event = _events(tmp_path / "run_x" / "logs" / "evaluate.jsonl")[0]
    assert event["component"] == "llm"


def test_does_not_write_logs_before_phase_runs(tmp_path: Path) -> None:
    def ok(ctx: Any) -> PhaseResult:
        return PhaseResult(status="ok", phase="normalize")

    instrument_phases({"normalize": ok}, run_id="run_x", logs_root=tmp_path, clock=_Clock())
    assert not (tmp_path / "run_x" / "logs").exists()


# ---------------------------------------------------------------------------
# write_run_metrics
# ---------------------------------------------------------------------------


def test_write_run_metrics_writes_expected_keys(tmp_path: Path) -> None:
    path = write_run_metrics(
        run_id="run_y",
        out_dir=tmp_path,
        durations={"evaluate": 1.5, "report": 0.25},
        llm_calls=4,
        tokens={"completion": 5, "prompt": 10},
    )
    assert path == tmp_path / "run_y" / "metrics.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert set(data) == B2_KEYS
    assert data["duration_by_phase"] == {"evaluate": 1.5, "report": 0.25}
    assert data["llm_calls"] == 4
    assert data["tokens"] == {"completion": 5, "prompt": 10}
    assert data["cells_by_state"] == {
        "evaluated": 0,
        "failed": 0,
        "pending": 0,
        "preliminary": 0,
        "reviewed": 0,
    }
    # Backlog A: без переданных счётчиков краула поле — пустая карта (стабильная форма).
    assert data["crawl"] == {}


def test_write_run_metrics_includes_crawl_counts(tmp_path: Path) -> None:
    """Backlog A: ``crawl_counts`` протягиваются в ``metrics.json`` (сортированные ключи)."""
    path = write_run_metrics(
        run_id="run_y",
        out_dir=tmp_path,
        crawl_counts={"fetched": 3, "failed": 1},
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["crawl"] == {"failed": 1, "fetched": 3}
    assert list(data["crawl"]) == ["failed", "fetched"]


def test_write_run_metrics_is_deterministic_and_sort_keys(tmp_path: Path) -> None:
    kwargs: dict[str, Any] = {
        "run_id": "run_y",
        "durations": {"z": 1.0, "a": 2.0},
        "tokens": {"z": 1, "a": 2},
        "llm_calls": 1,
    }
    first = write_run_metrics(out_dir=tmp_path / "one", **kwargs)
    second = write_run_metrics(out_dir=tmp_path / "two", **kwargs)
    assert first.read_text(encoding="utf-8") == second.read_text(encoding="utf-8")
    # sort_keys=True: ключи верхнего уровня и вложенных карт отсортированы.
    data = json.loads(first.read_text(encoding="utf-8"))
    assert list(data) == sorted(data)
    assert list(data["duration_by_phase"]) == ["a", "z"]
    assert list(data["tokens"]) == ["a", "z"]


def test_write_run_metrics_injected_durations_match_metrics_module(tmp_path: Path) -> None:
    durations = {"plan": 0.1, "evaluate": 2.0}
    path = write_run_metrics(run_id="run_y", out_dir=tmp_path, durations=durations)
    expected = compute_metrics(phase_durations=durations).to_manifest_json()
    assert path.read_text(encoding="utf-8") == expected


def test_write_run_metrics_reads_cells_from_store(tmp_path: Path) -> None:
    @dataclass
    class _Cell:
        state: str

    store = SimpleNamespace(iter_cells=lambda run_id: iter([_Cell("evaluated")]))
    path = write_run_metrics(run_id="run_y", out_dir=tmp_path, store=store)
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["cells_by_state"]["evaluated"] == 1


def test_instrumentation_errors_start_empty(tmp_path: Path) -> None:
    def ok(ctx: Any) -> PhaseResult:
        return PhaseResult(status="ok", phase="normalize")

    instrumented = instrument_phases(
        {"normalize": ok}, run_id="run_x", logs_root=tmp_path, clock=_Clock()
    )
    assert isinstance(instrumented, InstrumentedPhases)
    assert instrumented.instrumentation_errors == []


# ---------------------------------------------------------------------------
# Слой/нейтральность (матрица 01 §1)
# ---------------------------------------------------------------------------


def test_observability_does_not_import_adapters_or_edge() -> None:
    import core.pipeline.observability as module

    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    forbidden = {
        name for name in imported if name.startswith(("core.adapters", "core.edge"))
    }
    assert not forbidden, f"observability не должен импортировать {forbidden}"
