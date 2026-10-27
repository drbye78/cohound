"""``revalidate``: пере-оценка **только** устаревших ячеек существующего прогона (§13/K2).

Команда исполняет селектор :func:`core.pipeline.evaluate.select_stale_cells` на уже
существующем прогоне: ячейки, помеченные ``stale_reason`` (их описывает прошедшую
индексную сборку), пере-оцениваются тем же конвейером Ш6 (ретривал → промпт → оценщик →
валидация), после чего пометка снимается — но **только** при полученном вердикте по
текущей сборке. Незатронутые ячейки не пересчитываются: смысл команды в том, чтобы не
платить за незатронутое.

**Почему отдельная команда, а не флаг ``hound run``.**

* флаг прогона, сужающий набор оцениваемых ячеек, меняет **состав измерения**, а значит
  обязан входить в ``execution_contract_digest`` и ``run_id`` (иначе разный набор ячеек
  живёт под одним идентификатором — тот же дефект, что чинился для ``--max-depth``);
* но пере-оценка не меняет условий измерения: те же объявленные входы, тот же корпус, та
  же модель оценщика. Меняется только то, что работа Ш6, недоделанная прошлым прогоном,
  доводится до конца. Новый ``run_id`` здесь был бы ложью: измерение не новое;
* поэтому команда работает на существующем прогоне, ``run_id`` не меняет и не
  притворяется новым измерением. Она же — точка, где видно «сколько было / пере-оценено /
  осталось и почему».

**Модель оценщика не переопределяется.** Она берётся из объявленных ``llm_scenarios``
прогона (``runs.llm_scenarios``, читается через манифест), а не из флага и не из env:
вердикт, полученный другой моделью, уже не принадлежал бы этому ``run_id`` (§7.4/I21).
Endpoint/ключ/таймаут остаются операционными (env/deploy) — это данные хоста, а не условие
измерения.

**Условия ячейки повторяются, а не назначаются.** Профиль ретривала и ``cutoff_K`` берутся
из самой ячейки (``retrieval_profile``/``retrieval_params_json``, R7/I16/I20); принятие
риска (``scope_acceptance``) — из её ``evidence_scope``: пере-оценка корпуса не отменяет
человеческое решение о принятии риска (``bounded``-отрицание не понижается до ``❓`` из-за
«забытой» подписи).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _print_json,
    _resolve_db_path,
    _restore_plan_inputs,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK
from core.cli.wiring import build_adapters, build_evaluator_llm, build_store
from core.config.deploy import load_deploy_config
from core.domain import LLMScenario
from core.pipeline import Ports
from core.pipeline.evaluate import (
    REVALIDATE_NOTHING_TO_DO,
    REVALIDATE_STATUS_NOOP,
    RevalidationReport,
    reevaluate_stale,
    select_stale_cells,
)

__all__ = ["configure", "run"]

#: Статус отчёта ``--dry-run``: только селектор, оценщик не вызывался (локальный статус
#: команды: конвейер о режиме только-чтения не знает).
_STATUS_DRY_RUN = "dry_run"


def _default_runs_dir(args: argparse.Namespace) -> str:
    """Каталог прогонов: ``--runs-dir`` → resolved-раскладка → ``runs``.

    ``revalidate`` не входит в ``core.cli.main._RUNS_DIR_COMMANDS`` (тот файл — чужая
    полоса правок), поэтому дефолт раскладки подставляется здесь, как у ``crawl``: иначе
    ``--runs-dir`` без значения резолвился бы в относительный ``runs`` мимо
    ``COHOUND_HOME``/``paths.yaml``.
    """
    paths = getattr(args, "paths", None)
    if paths is not None:
        return str(paths.runs)
    return "runs"


def _declared_evaluator_model(manifest: Any) -> str | None:
    """Модель оценщика, **объявленная** прогоном (``llm_scenarios``), если она есть."""
    for binding in getattr(manifest, "llm_scenarios", None) or ():
        if getattr(binding, "scenario", None) is LLMScenario.EVALUATOR:
            model_id = getattr(binding, "model_id", None)
            if model_id:
                return str(model_id)
    return None


def _package_id(args: argparse.Namespace, store: Any, run_id: str, manifest: Any) -> str | None:
    """Идентичность пакета для сборки портов: ``--package`` → снимок входов → манифест."""
    if getattr(args, "package", None):
        return Path(str(args.package)).name
    try:
        raw = store.read_plan_inputs(run_id)
    except Exception:  # noqa: BLE001 — провенанс не обязателен для чтения ячеек
        raw = None
    if isinstance(raw, Mapping):
        package_id = raw.get("package_id")
        if isinstance(package_id, str) and package_id:
            return package_id
    return getattr(manifest, "case_id", None)


def _build_ports(
    args: argparse.Namespace,
    *,
    store: Any,
    package: str | None,
    model_id: str | None,
    index_build_id: str | None,
) -> Ports:
    """Порты пере-оценки: инъекция (``args.ports``) либо сборка профиля (A1/03 §E.3).

    Оценщик собирается с **объявленной** моделью прогона: иначе вердикт пришёл бы от
    модели, которой нет в ``llm_scenarios``, и ``run_id`` перестал бы её описывать.
    """
    injected = getattr(args, "ports", None)
    if injected is not None:
        return injected
    deploy = load_deploy_config(None)
    evaluator = build_evaluator_llm(
        deploy_llm=getattr(deploy, "llm", None),
        model_id=model_id,
    )
    paths = getattr(args, "paths", None)
    return build_adapters(
        profile=args.profile,
        package=package,
        index_root=str(paths.index_store) if paths is not None else "index_store",
        store=store,
        chunk_lookup=store.get_chunk,
        index_build_id=index_build_id,
        evaluator=evaluator,
    )


def _print_revalidate(payload: dict[str, Any]) -> None:
    """Человекочитаемая сводка: счётчики, сводка и построчно — что сделано/осталось."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"status: {payload['status']}",
        f"index_build_id: {payload.get('index_build_id') or '—'}",
        f"stale_total: {payload['stale_total']}",
        f"reevaluated: {payload['reevaluated']}",
        f"remaining: {payload['remaining']}",
        f"skipped: {payload['skipped']}",
        f"message: {payload['message']}",
    ]
    for row in payload.get("reevaluated_cells", ()):
        lines.append(
            f"  пере-оценено {row['product_id']}/{row['param_id']}: "
            f"{row.get('status') or '—'} (сборка {row.get('index_build_id') or '—'})"
        )
    for row in payload.get("remaining_cells", ()):
        lines.append(
            f"  осталось stale {row['product_id']}/{row['param_id']}: {row['reason']}"
        )
    for row in payload.get("skipped_cells", ()):
        lines.append(
            f"  вне пере-оценки {row['product_id']}/{row['param_id']}: {row['reason']}"
        )
    for note in payload.get("notes", ()):
        lines.append(f"note: {note}")
    for error in payload.get("errors", ()):
        lines.append(f"error: {error}")
    sys.stdout.write("\n".join(lines) + "\n")


def run(args: argparse.Namespace) -> int:
    """``revalidate``: пере-оценить stale-ячейки прогона и снять пометку при вердикте.

    :returns: :data:`EXIT_OK` — работа состоялась либо пере-оценивать нечего;
        :data:`EXIT_ERROR` — пере-оценка невозможна/ни одна попытка не дала вердикта
        (оставшиеся stale-ячейки названы в ``remaining_cells``).
    """
    if args.db is None and not args.runs_dir:
        args.runs_dir = _default_runs_dir(args)
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    notes: list[str] = []
    payload: dict[str, Any]
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        manifest = store.read_manifest(run_id)  # валидация: прогон существует
        catalog, _weights, _cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="revalidate", store=store
        )
        product_ids = list(args.product or [])
        param_ids = list(args.param or [])

        # Селектор считается **до** сборки портов: no-op не должен платить за индексы и
        # оценщика (идемпотентный повтор — это «нечего делать», а не «сделаем вид»).
        selection = select_stale_cells(
            store,
            run_id,
            product_ids=product_ids or None,
            param_ids=param_ids or None,
        )
        if args.dry_run or selection.total == 0:
            report = RevalidationReport(
                run_id=str(run_id),
                index_build_id=_current_build_id(store, run_id),
                status=REVALIDATE_STATUS_NOOP,
                message=(
                    REVALIDATE_NOTHING_TO_DO
                    if selection.total == 0
                    else f"stale-ячеек {selection.total}, "
                    f"пере-оцениваемых {len(selection.reevaluable_cells)}"
                ),
                stale_total=selection.total,
                skipped=[dict(row) for row in selection.skipped],
            )
            payload = {
                "ok": True,
                "status": _STATUS_DRY_RUN if args.dry_run else report.status,
                "run_id": report.run_id,
                "index_build_id": report.index_build_id,
                "message": report.message,
                "stale_total": report.stale_total,
                "reevaluated": 0,
                "remaining": 0,
                "skipped": len(report.skipped),
                "reevaluated_cells": [],
                "remaining_cells": [],
                "skipped_cells": list(report.skipped),
                "selection": selection.to_dict(),
                "notes": notes,
                "errors": [],
            }
        else:
            model_id = _declared_evaluator_model(manifest)
            if model_id is None:
                notes.append(
                    "llm_scenarios прогона не объявляют модель оценщика: пере-оценка "
                    "требует объявленной модели (env COHOUND_LLM_MODEL оценщиком не читается)"
                )
            current_build = _current_build_id(store, run_id)
            ports = _build_ports(
                args,
                store=store,
                package=_package_id(args, store, run_id, manifest),
                model_id=model_id,
                index_build_id=current_build,
            )
            report = reevaluate_stale(
                store=store,
                run_id=run_id,
                params=catalog,
                ports=ports,
                product_ids=product_ids or None,
                param_ids=param_ids or None,
                current_build_id=current_build,
                # V7: аудит промптов пере-оценки — тот же каталог прогона.
                prompt_dir=Path(db_path).parent / "prompts",
            )
            payload = {
                "ok": report.ok,
                **report.to_dict(),
                "notes": notes,
            }

    if args.json:
        _print_json(payload)
    else:
        _print_revalidate(payload)
    # Пустой stale — валидный no-op (EXIT_OK); отказ и «попытки без вердикта» — нет.
    return EXIT_OK if payload["status"] in {
        _STATUS_DRY_RUN,
        REVALIDATE_STATUS_NOOP,
    } or payload["reevaluated"] else EXIT_ERROR


def _current_build_id(store: Any, run_id: str) -> str | None:
    """Текущая сборка прогона (``runs.index_build_id``) из хранилища."""
    try:
        ref = store.read_run_index_ref(run_id)
    except Exception:  # noqa: BLE001 — отсутствие провенанса видно ниже (отказ)
        return None
    return getattr(ref, "index_build_id", None) or None


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``revalidate``."""
    p_reval = sub.add_parser(
        "revalidate",
        help="пере-оценить устаревшие (stale) ячейки прогона и снять пометку при вердикте",
    )
    reval_source = p_reval.add_mutually_exclusive_group(required=True)
    reval_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    reval_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_reval.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_reval.add_argument(
        "--run-yaml",
        default=None,
        help="run.yaml для восстановления каталога (вместе с --package; иначе — снимок "
        "входов из БД прогона)",
    )
    p_reval.add_argument(
        "--package",
        default=None,
        help="каталог пакета для восстановления каталога (вместе с --run-yaml)",
    )
    p_reval.add_argument(
        "--product",
        action="append",
        default=None,
        metavar="PRODUCT_ID",
        help="ограничить пере-оценку этими продуктами (можно повторять)",
    )
    p_reval.add_argument(
        "--param",
        action="append",
        default=None,
        metavar="PARAM_ID",
        help="ограничить пере-оценку этими параметрами (можно повторять)",
    )
    p_reval.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="только селектор: показать stale-ячейки и счётчики, оценщик не вызывать",
    )
    _add_common(p_reval)
    p_reval.set_defaults(handler=run)
