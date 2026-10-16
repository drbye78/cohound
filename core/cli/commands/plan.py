"""``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse

from core.cli.context import _add_common, _plan_payload, _print_json, _print_plan
from core.cli.errors import EXIT_OK, LintFailure
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.plan import plan as build_plan


def run(args: argparse.Namespace) -> int:
    """``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

    План чисто вычислим и **ничего не пишет**; флаг ``--dry-run`` фиксирует это явно
    (запись черновика ``run.yaml``/``cohort.yaml`` — будущий шаг W10/T10.4). Блокирующий
    линт прерывает построение плана с кодом правила.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    if not report.ready:
        raise LintFailure([*report.blocking, *report.incomplete], report.warnings)

    result = build_plan(inputs, run_id=args.run_id)
    payload = _plan_payload(result)
    payload["dry_run"] = bool(args.dry_run)
    if args.json:
        _print_json({"ok": True, **payload})
    else:
        _print_plan(payload)
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``plan``."""
    p_plan = sub.add_parser(
        "plan", help="нормализация входов + линт + план прогона (без записи)"
    )
    p_plan.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    p_plan.add_argument("--package", required=True, help="каталог пакета категории")
    p_plan.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    p_plan.add_argument(
        "--dry-run",
        action="store_true",
        help="линт + план без записи (состояние W5: запись черновика — W10/T10.4)",
    )
    _add_common(p_plan)
    p_plan.set_defaults(handler=run)
