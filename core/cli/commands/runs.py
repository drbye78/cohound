"""``runs``: список прогонов каталога (read-only) — вход в любую команду с ``run_id``.

До этой команды идентификатор прогона требовали 16 команд, а обнаружить его можно было
только ``ls runs/`` (``12-cli-architecture-review.md`` §2.1). Идентификатор, который нельзя
обнаружить командой, копируют руками — и однажды копируют не тот.

Чтение строго read-only (:func:`core.store.sqlite.connect_read_only`): каталог/файл/схема
не создаются, прогон живого писателя читается через WAL. БД чужой ``schema_version``
**пропускается с видимой пометкой** (``skipped``), а не роняет список: в ``runs/`` уже
лежат прогоны схем 3.9.0/3.10.0.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from core.cli.context import _add_common, _print_json
from core.cli.errors import EXIT_OK
from core.cli.run_overview import RunSummary, effective_runs_dir, scan_runs


def _print_runs(payload: dict[str, Any]) -> None:
    """Человекочитаемая таблица прогонов (по одной строке на прогон).

    Пропущенные БД печатаются отдельной строкой ``skipped: <причина>``: список прогонов,
    который молча теряет каталоги, врёт о содержимом ``runs/``.
    """
    runs = payload["runs"]
    sys.stdout.write(f"runs_dir: {payload['runs_dir']}\n")
    if not runs:
        sys.stdout.write(
            "runs: 0 — прогонов нет (каталог пуст либо не создан)\n"
        )
        return
    header = ("RUN_ID", "CREATED_AT", "PACKAGE", "PHASE", "STATE", "CELLS", "DOCS")
    rows: list[tuple[str, ...]] = []
    for run in runs:
        cells = sum(run["counts"]["cells"].values())
        rows.append(
            (
                run["run_id"],
                (run["created_at"] or "—")[:19],
                run["package_id"] or "—",
                run["last_phase"] or "—",
                run["state"],
                str(cells),
                str(run["counts"]["corpus_docs"]),
            )
        )
    widths = [
        max(len(header[index]), max(len(row[index]) for row in rows))
        for index in range(len(header))
    ]
    sys.stdout.write(
        "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(header)).rstrip() + "\n"
    )
    for row in rows:
        sys.stdout.write(
            "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip() + "\n"
        )
    for run in runs:
        if run["skipped"]:
            sys.stdout.write(f"skipped: {run['run_id']} ({run['db']}): {run['skipped']}\n")
    sys.stdout.write(
        f"runs: {sum(1 for run in runs if not run['skipped'])}"
        f" (+{sum(1 for run in runs if run['skipped'])} skipped)\n"
    )


def run(args: argparse.Namespace) -> int:
    """``runs``: перечисляет прогоны каталога (read-only, ``EXIT_OK`` и на пустом)."""
    runs_dir = effective_runs_dir(args)
    summaries: list[RunSummary] = scan_runs(runs_dir)
    payload = {
        "ok": True,
        "runs_dir": runs_dir,
        "count": len(summaries),
        "runs": [summary.as_dict() for summary in summaries],
    }
    if args.json:
        _print_json(payload)
    else:
        _print_runs(payload)
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``runs``."""
    p_runs = sub.add_parser(
        "runs",
        help="список прогонов каталога: run_id, пакет, фаза, состояние (read-only)",
    )
    p_runs.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (дефолт — раскладка: config/paths.yaml/COHOUND_RUNS_DIR/"
        "COHOUND_HOME, иначе runs)",
    )
    _add_common(p_runs)
    p_runs.set_defaults(handler=run)
