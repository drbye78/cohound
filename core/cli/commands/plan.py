"""``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import datetime as _dt
from typing import Any

from core.cli.context import _add_common, _plan_payload, _print_json, _print_plan
from core.cli.errors import EXIT_OK, LintFailure
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.plan import plan as build_plan
from core.pipeline.run_identity import build_execution_contract, resolve_cycle_id


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

    if args.run_id is not None:
        result = build_plan(inputs, run_id=args.run_id)
    else:
        # Та же идентичность, что у ``run`` с дефолтами (без --update/--refresh):
        # иначе ``plan`` показывал бы иной run_id, чем реальный прогон.
        profiles = getattr(inputs, "domain_profiles", None) or {}
        seed_config, _ = load_effective_seeds(
            args.package, inputs.run.products, profiles=profiles
        )
        cohort = cohort_digest(
            inputs.cohort,
            case_id=inputs.run.case_id,
            cohort_ref=getattr(inputs.run, "cohort_ref", None),
        )
        seeds = seeds_digest(seed_config)
        contract = build_execution_contract(
            inputs, cohort_digest=cohort, seeds_digest=seeds
        )
        result = build_plan(
            inputs,
            cycle_id=resolve_cycle_id(
                cli_cycle_id=None,
                run_config=inputs.run,
                update=False,
                today=_dt.date.today(),
            ),
            cohort_digest=cohort,
            seeds_digest=seeds,
            execution_contract_digest=contract,
        )
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
