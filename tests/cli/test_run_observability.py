"""Наблюдаемость CLI ``run``: логи §B.1 и метрики §B.2 под ``runs/`` (W12/T12.1).

Проверяют сквозную проводку :mod:`core.pipeline.observability` в :func:`core.cli.main.cmd_run`
на **реальном** реестре фаз из e2e-фикстуры (``tests/fixtures/e2e``):

* остановка на ``g1`` всё равно оставляет ``runs/{run_id}/logs/{normalize,lint,plan,g1}.jsonl``
  и ``runs/{run_id}/metrics.json`` (метрики отражают то, что реально отработало);
* ``--dry-run`` не пишет ни логов, ни метрик.

Гейт ``g1`` остаётся человеческим (S9/I6/I7): без решения прогон ``blocked``. Внешние
системы не используются (до ``g1`` сеть/LLM не задействованы).
"""

from __future__ import annotations

import json
from pathlib import Path

from core.cli.main import EXIT_BLOCKED, EXIT_OK, main

#: Корень e2e-фикстур (``tests/fixtures/e2e``).
E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"

#: Ожидаемые ключи метрик §B.2.
B2_KEYS = {
    "cells_by_state",
    "failure_rate",
    "coverage_aggregate",
    "search_calls_by_phase",
    "llm_calls",
    "tokens",
    "duration_by_phase",
}


def _argv(runs_dir: Path, run_id: str, *, dry_run: bool = False) -> list[str]:
    argv = [
        "run",
        "--run-yaml",
        str(E2E_FIXTURES / "run.yaml"),
        "--package",
        str(E2E_FIXTURES),
        "--runs-dir",
        str(runs_dir),
        "--run-id",
        run_id,
        "--json",
    ]
    if dry_run:
        argv.append("--dry-run")
    return argv


def test_run_blocked_at_g1_still_writes_phase_logs_and_metrics(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "obs-g1"

    code = main(_argv(runs_dir, run_id))
    assert code == EXIT_BLOCKED

    logs = runs_dir / run_id / "logs"
    for phase in ("normalize", "lint", "plan", "g1"):
        path = logs / f"{phase}.jsonl"
        assert path.is_file(), f"нет лога фазы {phase}"
        events = [
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        ]
        assert events, f"лог фазы {phase} пуст"
        for event in events:
            assert event["run_id"] == run_id
            assert event["phase"] == phase
            assert event["level"] in {"INFO", "WARN", "ERROR"}

    # g1 остановил прогон: его конец — WARN (blocked).
    g1_events = [
        json.loads(line)
        for line in (logs / "g1.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert g1_events[-1]["level"] == "WARN"

    metrics_path = runs_dir / run_id / "metrics.json"
    assert metrics_path.is_file()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert set(metrics) == B2_KEYS
    # Метрики записаны ДО остановки и содержат тайминги реально отработавших фаз.
    assert {"normalize", "lint", "plan", "g1"} <= set(metrics["duration_by_phase"])
    # До compute/report не дошли — в знаменателе нет ячеек.
    assert metrics["cells_by_state"]["evaluated"] == 0


def test_run_dry_run_writes_no_logs_or_metrics(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "obs-dry"

    code = main(_argv(runs_dir, run_id, dry_run=True))
    assert code == EXIT_OK

    assert not (runs_dir / run_id / "logs").exists()
    assert not (runs_dir / run_id / "metrics.json").exists()
