"""Наблюдаемость CLI ``run``: логи §B.1 и метрики §B.2 под ``runs/`` (W12/T12.1).

Проверяют сквозную проводку :mod:`core.pipeline.observability` в :func:`core.cli.main.cmd_run`
на **реальном** реестре фаз из e2e-фикстуры (``tests/fixtures/e2e``):

* остановка на публикации (``g4``) всё равно оставляет ``runs/{run_id}/logs/{phase}.jsonl``
  для каждой отработавшей фазы и ``runs/{run_id}/metrics.json`` (метрики отражают то, что
  реально отработало);
* ``--dry-run`` не пишет ни логов, ни метрик.

Публикация (``g4``) остаётся человеческим/внешним решением (S9/I6/I7): без него прогон
``blocked``. G1/G2/G3 прогон не блокируют (решение владельца 26.10.2026), поэтому до
публикации он доходит. Внешние системы не используются; домен фикстуры недоступен, поэтому
корпус пуст и сетевые отказы видны как изолированные ошибки фаз.
"""

from __future__ import annotations

import json
from pathlib import Path

from core.cli.main import EXIT_BLOCKED, EXIT_OK, main

#: Корень e2e-фикстур (``tests/fixtures/e2e``).
E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"

#: Ожидаемые ключи метрик §B.2 (+ Backlog A ``crawl``).
B2_KEYS = {
    "cells_by_state",
    "failure_rate",
    "coverage_aggregate",
    "search_calls_by_phase",
    "llm_calls",
    "tokens",
    # M1/§15: накопленная стоимость LLM по заходам + вклад этого процесса.
    "llm_usage",
    "duration_by_phase",
    "crawl",
    "crawl_truncation",
    # F1: жизненный цикл сеанса краула (``interrupted`` виден без разбора БД).
    "crawl_session",
    # F0: блок ресурсов (§4 F0) — присутствует всегда, даже если нечего измерять.
    "resources",
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


def test_run_blocked_at_publication_still_writes_phase_logs_and_metrics(
    tmp_path: Path,
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "obs-g1"

    code = main(_argv(runs_dir, run_id))
    # Блокирует публикацию (G4): G1/G2/G3 пропускаются, но прогон идёт дальше.
    assert code == EXIT_BLOCKED

    logs = runs_dir / run_id / "logs"
    for phase in ("normalize", "lint", "plan", "g1", "g4"):
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

    # Неблокирующий g1 прошёл без остановки — его конец INFO, а не WARN.
    g1_events = [
        json.loads(line)
        for line in (logs / "g1.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert g1_events[-1]["level"] == "INFO"
    # Остановил прогон G4 — его конец WARN (blocked).
    g4_events = [
        json.loads(line)
        for line in (logs / "g4.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert g4_events[-1]["level"] == "WARN"

    metrics_path = runs_dir / run_id / "metrics.json"
    assert metrics_path.is_file()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert set(metrics) == B2_KEYS
    # Метрики записаны ДО остановки и содержат тайминги реально отработавших фаз.
    assert {"normalize", "lint", "plan", "g1", "corpus", "g4"} <= set(
        metrics["duration_by_phase"]
    )
    # До compute/report не дошли — в знаменателе нет ячеек.
    assert metrics["cells_by_state"]["evaluated"] == 0
    # Backlog A: счётчики краула присутствуют; Ш5 исполнялась (корпус пуст — домен
    # фикстуры недоступен), поэтому ``failed`` не ноль, а остальные счётчики — ноль.
    assert set(metrics["crawl"]) == {
        "pending",
        "running",
        "fetched",
        "failed",
        "cancelled",
    }
    assert metrics["crawl"]["failed"] >= 1
    for status_name in ("pending", "running", "fetched", "cancelled"):
        assert metrics["crawl"][status_name] == 0

    # F0: блок ``resources`` собран CLI из трёх источников — пики RSS по фазам
    # (наблюдаемость), наблюдение фазы корпуса и провенанс бюджета памяти.
    resources = metrics["resources"]
    assert {"normalize", "lint", "plan", "g1"} <= set(resources["phases"])
    assert resources["phases"]["plan"]["peak_rss_mb"] is not None
    assert resources["phases"]["plan"]["wall_s"] is not None
    assert resources["budget"]["source"] in {"env", "available", "unknown"}
    assert resources["budget"]["parent_floor_mb"] > 0
    # Ш5 исполнялась (гейты G1–G3 больше не режут конвейер), поэтому источник расписания —
    # наблюдённый (миграция «declared» осталась только для прогонов без корпуса).
    assert resources["scheduling"]["source"] in {"declared", "derived"}
    # Корпус пуст (домен фикстуры недоступен): нули — факт, а не выдумка.
    assert resources["documents"] == 0
    assert resources["chunks"] == 0


def test_run_llm_usage_accumulates_across_invocations_of_same_run(
    tmp_path: Path,
) -> None:
    """Продолжение прогона другим процессом суммирует стоимость, а не перезаписывает её.

    Два захода ``run`` по одному ``run_id`` — это ровно сценарий resume: каждый процесс
    видит только свой ``usage_summary()``, а ``metrics.json`` обязан описывать прогон.
    """
    runs_dir = tmp_path / "runs"
    run_id = "obs-usage"

    assert main(_argv(runs_dir, run_id)) in {EXIT_OK, EXIT_BLOCKED}
    first = json.loads(
        (runs_dir / run_id / "metrics.json").read_text(encoding="utf-8")
    )
    assert first["llm_usage"]["processes"] == 1
    assert first["llm_usage"]["calls_cumulative"] == first["llm_calls"]

    assert main(_argv(runs_dir, run_id)) in {EXIT_OK, EXIT_BLOCKED}
    second = json.loads(
        (runs_dir / run_id / "metrics.json").read_text(encoding="utf-8")
    )
    usage = second["llm_usage"]
    assert usage["processes"] == 2
    # Накопленное = первый заход + второй; поля не перезаписаны «ценой последнего».
    assert usage["calls_cumulative"] == (
        first["llm_usage"]["calls_cumulative"] + usage["calls_this_process"]
    )
    assert usage["previous_source"] == "ok"
    assert second["llm_calls"] == usage["calls_cumulative"]
    assert usage["accounting"] == "processes_that_wrote"
    # Source вклада назван: формат оценивался адаптером (в e2e-фикстуре — фейк с методом).
    assert usage["current_source"] in {"adapter", "absent"}


def test_run_dry_run_writes_no_logs_or_metrics(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "obs-dry"

    code = main(_argv(runs_dir, run_id, dry_run=True))
    assert code == EXIT_OK

    assert not (runs_dir / run_id / "logs").exists()
    assert not (runs_dir / run_id / "metrics.json").exists()


def test_run_parser_accepts_phase4_flags() -> None:
    """Phase 4: ``run`` принимает флаги кэша/фильтров/потолка/follow (парсер-контракт)."""
    from core.cli.main import build_parser

    args = build_parser().parse_args(
        [
            "run",
            "--run-yaml",
            "run.yaml",
            "--package",
            "pkg",
            "--from-scratch",
            "--only-failed",
            "--only-product",
            "p1",
            "--max-attempts",
            "3",
            "--follow",
            "--interval",
            "0.5",
        ]
    )
    assert args.from_scratch is True
    assert args.only_failed is True
    assert args.only_product == "p1"
    assert args.max_attempts == 3
    assert args.follow is True
    assert args.interval == 0.5


def test_run_parser_watch_is_follow_alias() -> None:
    """Phase 4: ``--watch`` — алиас ``--follow``."""
    from core.cli.main import build_parser

    args = build_parser().parse_args(
        ["run", "--run-yaml", "run.yaml", "--package", "pkg", "--watch"]
    )
    assert args.follow is True
