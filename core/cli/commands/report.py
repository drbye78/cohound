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
from core.pipeline.gates import gate_trail
from core.pipeline.partial_docs import human_line as partial_docs_human_line
from core.pipeline.report import write_reports
from core.pipeline.support_quality import support_quality_line


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
    # Базовый каталог артефактов: явный --out > --runs-dir > resolved paths.runs > runs.
    out_dir = args.out
    if out_dir is None:
        out_dir = getattr(args, "runs_dir", None)
    if out_dir is None:
        _paths = getattr(args, "paths", None)
        out_dir = str(_paths.runs) if _paths is not None else "runs"
    with build_store(db_path, profile=args.profile, read_only=True) as store:
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
        # §13: след гейтов не персистится в строке ``runs`` (там нет такой колонки), поэтому
        # пост-хок пересборка артефактов восстановила бы манифест **без** него. След
        # восстанавливается из того же источника, что у фазы отчёта, — ``gate_decisions``
        # (:func:`core.pipeline.gates.gate_trail`), иначе ``run_manifest.yaml``,
        # перезаписанный командой ``report``, молчал бы о делегировании/пропуске гейта.
        # Читается внутри ``with``: после закрытия store соединения уже нет.
        if not _manifest_partial(manifest):
            manifest = manifest.model_copy(
                update={"gate_waivers": gate_trail(store, run_id)}
            )
        # Артефакты пишутся **внутри** ``with``: ``write_reports`` пересобирает
        # ``artifacts_gap.yaml`` из ``crawl_session_items`` и без живого соединения получал
        # закрытый store. Раньше ошибка чтения глушилась внутри сбора пробелов, и файл
        # писался пустым (``total: 0``) — то есть отчёт «недоступного нет» на прогоне, где
        # таких URL 17. Живой факт: ``hound report`` давал ``gaps: []`` при 17 отказах
        # ``robots_disallow`` в БД.
        artifacts = write_reports(
            bundle=bundle,
            cells=cells,
            catalog=catalog,
            manifest=manifest,
            out_dir=out_dir,
            run_id=run_id,
            store=store,
        )

    # Сужение типа для mypy: ``run_id``/``bundle``/``manifest`` присваиваются безусловно
    # внутри ``with`` выше. ``None`` недостижим, поэтому ``raise`` завёл бы путь ошибки для
    # несуществующего состояния; проверка остаётся инвариантом, а не отказом.
    assert run_id is not None and bundle is not None and manifest is not None  # noqa: S101 — сужение типа для недостижимого None
    # I2/I2c: частичное извлечение — строкой, а не только файлом, который надо открыть.
    # Оператор обязан узнать «корпус неполон из-за частичного чтения» из вывода команды.
    partial_line = partial_docs_human_line(
        getattr(artifacts, "partial_docs_summary", None)
    )
    if partial_line:
        notes.append(partial_line)
    # A3: качество поддержки карты — строкой, как и частичность. Три замера
    # (поддержка не о предмете, концентрация) не меняют вердиктов, но именно они
    # объясняют, почему карта «зелёная» при почти пустом доказательстве.
    support_line = support_quality_line(
        getattr(artifacts, "support_quality_summary", None) or {}
    )
    if support_line:
        notes.append(support_line)
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
        # A1/§6: класс публикуемости и числа, из которых он выведен — в машинном
        # выводе, а не только в шапке battle card. Отсутствие профиля не выдумывается
        # (``None``): синтетический бандл без профиля честно молчит.
        "publishability": (
            bundle.publishability.to_dict()
            if getattr(bundle, "publishability", None) is not None
            else None
        ),
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
