"""``report``: пересборка артефактов прогона из БД (Ш9, §9, ``03`` §D.6).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _artifacts_payload,
    _infer_run_id,
    _manifest_partial,
    _print_json,
    _resolve_db_path,
    _restore_plan_inputs,
)
from core.cli.errors import EXIT_OK
from core.cli.wiring import build_store
from core.pipeline.compute import compute, scope_universe
from core.pipeline.report import write_reports


def run(args: argparse.Namespace) -> int:
    """``report``: пересборка артефактов прогона из БД (Ш9, §9, ``03`` §D.6).

    Хранилище резолвится как в ``sensitivity`` (``--db`` либо ``--runs-dir`` +
    ``--run-id``). Бандл пересобирается :func:`core.pipeline.compute.compute`, ячейки
    читаются :meth:`~core.pipeline.runner.StorePort.iter_cells`, манифест —
    ``read_manifest``. Артефакты пишутся :func:`core.pipeline.report.write_reports` в
    ``{out}/{run_id}/outputs/`` (``--out``; дефолт — ``--runs-dir`` либо ``runs``).

    Каталог/веса/когорта восстанавливаются из явных ``--run-yaml`` + ``--package`` либо из
    снимка прогона в БД (``run_inputs``; ``cmd_run`` пишет его — прогон самодостаточен);
    лишь без обоих источников расчёт идёт с пустым каталогом и явной заметкой-ограничением.
    Если ``read_manifest`` вернул незаполненный манифест (финальная фаза ``report`` не
    завершалась), это также сообщается в ``notes`` — артефакты строятся из текущего
    состояния ячеек, а не выдумываются версии.

    :returns: :data:`EXIT_OK` при записанных артефактах.
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    notes: list[str] = []
    manifest = None
    bundle = None
    catalog: dict[str, Any] = {}
    cells: list[Any] = []
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        manifest = store.read_manifest(run_id)
        catalog, module_weights, cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="report", store=store
        )
        # §6/B-3a: при наличии когорты вселенная scope задаёт знаменатель Coverage;
        # без когорты — legacy-режим (по существующим ячейкам) + заметка-ограничение.
        universe = (
            scope_universe(catalog, [entry.product_id for entry in cohort.entries])
            if cohort is not None
            else None
        )
        bundle = compute(
            store=store,
            run_id=run_id,
            catalog=catalog,
            module_weights=module_weights,
            cohort=cohort,
            universe=universe,
        )
        cells = list(store.iter_cells(run_id))

    assert run_id is not None and bundle is not None and manifest is not None
    # Базовый каталог артефактов: явный --out > --runs-dir > resolved paths.runs > runs.
    out_dir = args.out
    if out_dir is None:
        out_dir = getattr(args, "runs_dir", None)
    if out_dir is None:
        _paths = getattr(args, "paths", None)
        out_dir = str(_paths.runs) if _paths is not None else "runs"
    artifacts = write_reports(
        bundle=bundle,
        cells=cells,
        catalog=catalog,
        manifest=manifest,
        out_dir=out_dir,
        run_id=run_id,
    )
    partial = _manifest_partial(manifest)
    if partial:
        notes.append(
            "манифест не заполнен (фаза report не завершалась): версии — заглушки; "
            "артефакты построены из текущего состояния ячеек"
        )
    payload = {
        "ok": True,
        "run_id": run_id,
        "out": str(Path(out_dir) / run_id / "outputs"),
        "manifest_partial": partial,
        "artifacts": _artifacts_payload(artifacts),
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        for name, path in payload["artifacts"].items():
            sys.stdout.write(f"{name}: {path}\n")
        for note in notes:
            sys.stdout.write(f"note: {note}\n")
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``report``."""
    p_report = sub.add_parser(
        "report", help="пересборка артефактов прогона из БД (Ш9, §9)"
    )
    report_source = p_report.add_mutually_exclusive_group(required=True)
    report_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    report_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_report.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_report.add_argument(
        "--run-yaml",
        default=None,
        help="run.yaml для восстановления каталога/весов/когорты (вместе с --package)",
    )
    p_report.add_argument(
        "--package",
        default=None,
        help="каталог пакета для восстановления каталога/весов/когорты (вместе с --run-yaml)",
    )
    p_report.add_argument(
        "--out",
        default=None,
        help="базовый каталог артефактов (дефолт --runs-dir либо runs); пишет {out}/{run_id}/outputs/",
    )
    _add_common(p_report)
    p_report.set_defaults(handler=run)
