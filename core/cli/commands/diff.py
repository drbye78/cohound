"""``diff``: межцикловый diff ``run_a`` → ``run_b`` (§9/§13).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse
import sys
from pathlib import Path

from core.cli.context import (
    _add_common,
    _concise,
    _emit_error,
    _print_json,
    _read_cycle,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, extract_code
from core.pipeline.report import CrossCycleDiffError, render_diff


def run(args: argparse.Namespace) -> int:
    """``diff``: межцикловый diff ``run_a`` → ``run_b`` (§9/§13).

    Резолв хранилищ (контракт выбора):

    * ``--runs-dir`` (дефолт ``runs``) + два позиционных ``run_id`` →
      ``runs/{run_id}/cohound.db`` через :func:`core.cli.wiring.run_db_path`;
    * единый явный ``--db`` — когда оба цикла лежат в **одной** БД; он переопределяет
      ``--runs-dir``.

    Оба цикла читаются как реальные prev/curr (без заглушки): ``read_manifest`` +
    ``iter_cells``. :func:`core.pipeline.report.render_diff` пишет ``diff.md``; при
    нарушении версионной границы §13 (major ``methodology_version`` либо расхождение
    cohort/profile+K/index_build/retrieval_params) :class:`CrossCycleDiffError` →
    :data:`EXIT_ERROR` с причиной ``§13: ...``.

    Дефолт ``--out`` — ``runs/{run_b}/outputs/diff.md`` (родительские каталоги создаются).
    """
    out_path = (
        Path(args.out)
        if args.out is not None
        else Path(args.runs_dir) / args.run_b / "outputs" / "diff.md"
    )
    prev_manifest, prev_cells = _read_cycle(args, args.run_a)
    curr_manifest, curr_cells = _read_cycle(args, args.run_b)
    try:
        written = render_diff(
            prev_manifest, curr_manifest, prev_cells, curr_cells, out_path
        )
    except CrossCycleDiffError as exc:
        message = _concise(exc)
        code = extract_code(message)
        prefix = f"{code}:"
        if message.startswith(prefix):
            message = message[len(prefix) :].strip()
        _emit_error(args, code, message)
        return EXIT_ERROR

    payload = {
        "ok": True,
        "out": str(written),
        "run_a": args.run_a,
        "run_b": args.run_b,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(f"{written}\n")
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``diff``."""
    p_diff = sub.add_parser(
        "diff",
        help="межцикловый diff run_a → run_b (запрещён через major/границу, §13)",
    )
    p_diff.add_argument("run_a", help="предыдущий прогон (run_id)")
    p_diff.add_argument("run_b", help="текущий прогон (run_id)")
    p_diff.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов: runs/{run_id}/cohound.db (дефолт — раскладка: "
        "config/paths.yaml/COHOUND_RUNS_DIR/COHOUND_HOME, иначе runs); --db переопределяет",
    )
    p_diff.add_argument(
        "--db",
        default=None,
        help="один общий cohound.db для обоих прогонов (оба run_id в одной БД)",
    )
    p_diff.add_argument(
        "--out",
        default=None,
        help="путь diff.md (дефолт runs/{run_b}/outputs/diff.md)",
    )
    _add_common(p_diff)
    p_diff.set_defaults(handler=run)
