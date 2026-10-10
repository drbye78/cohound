"""``sensitivity``: пост-хок анализ устойчивости по осям (§6, K6).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse
from pathlib import Path

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _print_json,
    _print_sensitivity,
    _resolve_db_path,
    _restore_plan_inputs,
)
from core.cli.errors import EXIT_OK
from core.cli.wiring import build_store
from core.pipeline.compute import compute


def run(args: argparse.Namespace) -> int:
    """``sensitivity``: пост-хок анализ устойчивости по осям (§6, K6).

    Хранилище резолвится одним из двух способов: явный ``--db`` (``run_id`` берётся из
    ``--run-id`` либо выводится, если в БД ровно один прогон) **или**
    ``--runs-dir`` + ``--run-id`` → ``runs/{run_id}/cohound.db``.

    Бандл пересобирается :func:`core.pipeline.compute.compute` из ячеек прогона. Источник
    каталога/весов/когорты (приоритет): явные ``--run-yaml`` + ``--package``, иначе снимок
    прогона ``runs/{run_id}/plan_inputs.json`` (``cmd_run`` пишет его — прогон
    самодостаточен, ``03`` §A.2). Лишь при отсутствии обоих источников расчёт идёт с
    **пустым** каталогом (вес ячейки = 1.0) и без когорты, а ограничение явно печатается
    в поле ``notes`` (значения не выдумываются).

    Печатает оси ``per_axis``, флаг ``stable``, ``unstable_pairs`` и
    ``requested_ranking``/``computed_ranking`` (человеку или JSON).
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    bundle = None
    notes: list[str] = []
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        catalog, module_weights, cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="sensitivity", run_dir=Path(db_path).parent
        )
        bundle = compute(
            store=store,
            run_id=run_id,
            catalog=catalog,
            module_weights=module_weights,
            cohort=cohort,
        )

    assert bundle is not None  # compute выполняется безусловно (см. выше)
    report = bundle.sensitivity
    payload = {
        "ok": True,
        "run_id": run_id,
        "stable": report.stable,
        "per_axis": report.per_axis,
        "unstable_pairs": [list(pair) for pair in report.unstable_pairs],
        "requested_ranking": report.requested_ranking,
        "computed_ranking": report.computed_ranking,
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        _print_sensitivity(payload)
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``sensitivity``."""
    p_sens = sub.add_parser(
        "sensitivity", help="пост-хок sensitivity по осям (§6, K6)"
    )
    sens_source = p_sens.add_mutually_exclusive_group(required=True)
    sens_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    sens_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_sens.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_sens.add_argument(
        "--run-yaml",
        default=None,
        help="run.yaml для восстановления каталога/весов/когорты (вместе с --package)",
    )
    p_sens.add_argument(
        "--package",
        default=None,
        help="каталог пакета для восстановления каталога/весов/когорты (вместе с --run-yaml)",
    )
    _add_common(p_sens)
    p_sens.set_defaults(handler=run)
