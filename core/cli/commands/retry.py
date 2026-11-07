"""``retry``: повтор оценки ячеек **без вердикта** на существующем прогоне.

Команда исполняет селектор :func:`core.pipeline.evaluate.select_failed_cells` на уже
существующем ``run_id``: ``failed``-ячейки с повторяемым классом сбоя (отказ прибора,
непрошедший эшелон §10 ответ, сбой ретривала, ``no_sources`` при уже собранном корпусе)
проходят тот же конвейер Ш6, что и в фазе, после чего получают вердикт либо снова
остаются без него — с обновлённым классом и номером попытки в заметках.

**Почему отдельная команда, а не флаг ``hound run``.** Флаг прогона, сужающий набор
оцениваемых ячеек, меняет **состав измерения**, а значит обязан входить в
``execution_contract_digest`` и ``run_id``. Но доведение недоделанной Ш6 до конца
условий измерения не меняет: те же объявленные входы, тот же корпус, та же модель
оценщика. Новый ``run_id`` здесь был бы ложью: измерение не новое. Поэтому команда
работает на существующем прогоне, ``run_id`` не меняет и не пишет в ``runs``.

**Почему не ``revalidate``.** У ``revalidate`` правило сохранения — «снять
``stale_reason`` только при вердикте по текущей сборке». У ``failed``-ячейки пометки нет,
а есть класс сбоя; смешать два правила в одном отчёте значит потерять различие
«устарело» / «не измерено».

**Модель оценщика не переопределяется.** Она берётся из объявленных ``llm_scenarios``
прогона (``runs.llm_scenarios``), а не из флага и не из env: вердикт, полученный другой
моделью, уже не принадлежал бы этому ``run_id`` (§7.4/I21). Endpoint/ключ/таймаут
остаются операционными (env/deploy) — это данные хоста, а не условие измерения.

**Условия ячейки повторяются, а не назначаются.** Профиль ретривала и ``cutoff_K``
берутся из самой ячейки (``retrieval_profile``/``retrieval_params_json``, R7/I16/I20);
принятие риска (``scope_acceptance``) — из её ``evidence_scope``.

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
    _add_llm_concurrency_flag,
    _infer_run_id,
    _print_json,
    _resolve_db_path,
    _restore_plan_inputs,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK
from core.cli.wiring import build_adapters, build_evaluator_llm, build_store
from core.config.deploy import load_deploy_config
from core.domain import LLMScenario, RunMode
from core.pipeline import Ports
from core.pipeline.evaluate import (
    RETRY_NOTHING_TO_DO,
    RETRY_STATUS_NOOP,
    RETRY_STATUS_REFUSED,
    RetryReport,
    retry_failed_cells,
    select_failed_cells,
)
from core.pipeline.evaluation_policy import EVALUATION_MAX_RETRIES

__all__ = ["configure", "run"]

#: Статус отчёта ``--dry-run``: только селектор, оценщик не вызывался (локальный статус
#: команды: конвейер о режиме только-чтения не знает).
_STATUS_DRY_RUN = "dry_run"


def _default_runs_dir(args: argparse.Namespace) -> str:
    """Каталог прогонов: ``--runs-dir`` → resolved-раскладка → ``runs``.

    ``retry`` не входит в ``core.cli.main._RUNS_DIR_COMMANDS``, поэтому дефолт
    раскладки подставляется здесь, как у ``revalidate``/``crawl``: иначе ``--runs-dir``
    без значения резолвился бы в относительный ``runs`` мимо
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


def _package_id(
    args: argparse.Namespace, store: Any, run_id: str, manifest: Any
) -> str | None:
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
    """Порты повтора: инъекция (``args.ports``) либо сборка профиля (A1/03 §E.3).

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


def _current_build_id(store: Any, run_id: str) -> str | None:
    """Текущая сборка прогона (``runs.index_build_id``) из хранилища."""
    try:
        ref = store.read_run_index_ref(run_id)
    except Exception:  # noqa: BLE001 — отсутствие провенанса видно в отчёте
        return None
    return getattr(ref, "index_build_id", None) or None


def _retries(raw: str) -> int:
    """``argparse``-тип ``--max-retries``: целое ``≥ 0`` (0 — без повторов вызова)."""
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"ожидалось целое, получено {raw!r}") from exc
    if value < 0:
        raise argparse.ArgumentTypeError(
            f"число повторов не может быть отрицательным, получено {raw!r}"
        )
    return value


def _print_retry(payload: dict[str, Any]) -> None:
    """Человекочитаемая сводка: счётчики, сводка и построчно — что стало с ячейками."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"status: {payload['status']}",
        f"index_build_id: {payload.get('index_build_id') or '—'}",
        f"failed_total: {payload['failed_total']}",
        f"retryable: {payload['retryable']}",
        f"retried: {payload['retried']}",
        f"recovered: {payload['recovered']}",
        f"still_failed: {payload['still_failed']}",
        f"skipped: {payload['skipped']}",
        f"message: {payload['message']}",
    ]
    for row in payload.get("cells", ()):
        lines.append(
            f"  {row['outcome']} {row['product_id']}/{row['param_id']}: "
            f"попытка {row.get('attempt')}, "
            f"класс {row.get('failure_kind') or row.get('previous_failure_kind') or '—'}"
        )
    for row in payload.get("skipped_cells", ()):
        lines.append(
            f"  вне повтора {row['product_id']}/{row['param_id']}: {row['reason']}"
        )
    for note in payload.get("notes", ()):
        lines.append(f"note: {note}")
    for error in payload.get("errors", ()):
        lines.append(f"error: {error}")
    sys.stdout.write("\n".join(lines) + "\n")


def run(args: argparse.Namespace) -> int:
    """``retry``: повторить оценку ячеек без вердикта; ``run_id`` не меняется.

    :returns: :data:`EXIT_OK` — все отобранные получили вердикт, либо отбирать было
        нечего (в том числе частичный успех: остаток оператор видит и повторит);
        :data:`EXIT_ERROR` — повтор невозможен либо попытки были, а вердикта нет ни
        у одной («не прошло» — валидный исход, но не успех).
    """
    if args.db is None and not args.runs_dir:
        args.runs_dir = _default_runs_dir(args)
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    notes: list[str] = []
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        manifest = store.read_manifest(run_id)  # валидация: прогон существует
        catalog, _weights, _cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="retry", store=store
        )
        product_ids = list(args.product or [])
        param_ids = list(args.param or [])

        # Селектор считается **до** сборки портов: no-op не должен платить за индексы и
        # оценщика (идемпотентный повтор — это «нечего делать», а не «сделаем вид»).
        selection = select_failed_cells(
            store,
            run_id,
            product_ids=product_ids or None,
            param_ids=param_ids or None,
        )
        payload: dict[str, Any] | None = None
        model_id: str | None = None
        if args.dry_run or not selection.retryable_cells:
            report = RetryReport(
                run_id=str(run_id),
                index_build_id=_current_build_id(store, run_id),
                status=RETRY_STATUS_NOOP,
                message=(
                    RETRY_NOTHING_TO_DO
                    if selection.failed_total == 0
                    else f"без вердикта {selection.failed_total}, "
                    f"повторяемо {len(selection.retryable_cells)}"
                ),
                failed_total=selection.failed_total,
                retryable=len(selection.retryable_cells),
                skipped=[dict(row) for row in selection.skipped],
            )
            payload = {
                "ok": True,
                "status": _STATUS_DRY_RUN if args.dry_run else report.status,
                "run_id": report.run_id,
                "index_build_id": report.index_build_id,
                "message": report.message,
                "failed_total": report.failed_total,
                "retryable": report.retryable,
                "retried": 0,
                "recovered": 0,
                "still_failed": 0,
                "skipped": len(report.skipped),
                "skipped_cells": list(report.skipped),
                "cells": [],
                "selection": selection.to_dict(),
                "notes": notes,
                "errors": [],
            }
        else:
            model_id = _declared_evaluator_model(manifest)
            if model_id is None:
                # Отказ, а не догадка: модель не объявлена — вердикт не принадлежал бы
                # прогону. Env оценщиком не читается (§7.4/I21).
                payload = {
                    "ok": False,
                    "status": RETRY_STATUS_REFUSED,
                    "run_id": str(run_id),
                    "index_build_id": _current_build_id(store, run_id),
                    "message": (
                        "повтор невозможен: llm_scenarios прогона не объявляют модель "
                        "оценщика (env COHOUND_LLM_MODEL оценщиком не читается)"
                    ),
                    "failed_total": selection.failed_total,
                    "retryable": len(selection.retryable_cells),
                    "retried": 0,
                    "recovered": 0,
                    "still_failed": 0,
                    "skipped": len(selection.skipped),
                    "skipped_cells": list(selection.skipped),
                    "cells": [],
                    "selection": selection.to_dict(),
                    "notes": notes,
                    "errors": [],
                }
        if payload is None:
            current_build = _current_build_id(store, run_id)
            ports = _build_ports(
                args,
                store=store,
                package=_package_id(args, store, run_id, manifest),
                model_id=model_id,
                index_build_id=current_build,
            )
            report = retry_failed_cells(
                store=store,
                run_id=run_id,
                params=catalog,
                ports=ports,
                # Режим прогона — объявленный вход: preliminary-ячейка не становится
                # ``failed`` только потому, что её чинят (I13).
                strict=getattr(manifest, "mode", None) is RunMode.STRICT,
                max_retries=args.max_retries,
                product_ids=product_ids or None,
                param_ids=param_ids or None,
                current_build_id=current_build,
                # Промпты — в тот же каталог прогона, журналы — в те же файлы, что у Ш6:
                # путь отказа повтора обязан разбираться тем же запросом.
                prompt_dir=Path(db_path).parent / "prompts",
                llm_journal=Path(db_path).parent / "logs" / "llm_calls.jsonl",
                retry_journal=Path(db_path).parent / "logs" / "retry.jsonl",
                llm_concurrency=args.llm_concurrency or 1,
            )
            payload = {
                "ok": report.ok,
                **report.to_dict(),
                "notes": notes,
            }

    if payload is None:  # недостижимо: обе ветви выше заполняют payload
        return EXIT_ERROR
    if args.json:
        _print_json(payload)
    else:
        _print_retry(payload)
    if payload["status"] in {_STATUS_DRY_RUN, RETRY_STATUS_NOOP}:
        return EXIT_OK
    if payload["status"] == RETRY_STATUS_REFUSED:
        return EXIT_ERROR
    # Попытки были: частичный успех — EXIT_OK (оператор видит остаток), полный провал
    # попыток — EXIT_ERROR.
    return EXIT_OK if payload.get("recovered") else EXIT_ERROR


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``retry``."""
    p_retry = sub.add_parser(
        "retry",
        help="повторить оценку ячеек без вердикта на существующем прогоне (run_id не меняется)",
    )
    retry_source = p_retry.add_mutually_exclusive_group(required=True)
    retry_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    retry_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_retry.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_retry.add_argument(
        "--run-yaml",
        default=None,
        help="run.yaml для восстановления каталога (вместе с --package; иначе — снимок "
        "входов из БД прогона)",
    )
    p_retry.add_argument(
        "--package",
        default=None,
        help="каталог пакета для восстановления каталога (вместе с --run-yaml)",
    )
    p_retry.add_argument(
        "--product",
        action="append",
        default=None,
        metavar="PRODUCT_ID",
        help="ограничить повтор этими продуктами (можно повторять)",
    )
    p_retry.add_argument(
        "--param",
        action="append",
        default=None,
        metavar="PARAM_ID",
        help="ограничить повтор этими параметрами (можно повторять)",
    )
    p_retry.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="только селектор: показать повторяемые/пропущенные ячейки и счётчики, "
        "оценщик и индексы не собирать",
    )
    p_retry.add_argument(
        "--max-retries",
        dest="max_retries",
        type=_retries,
        default=EVALUATION_MAX_RETRIES,
        metavar="N",
        help="повторы вызова оценщика внутри попытки (дефолт — объявленная политика "
        f"прогона: {EVALUATION_MAX_RETRIES})",
    )
    _add_llm_concurrency_flag(p_retry)
    _add_common(p_retry)
    p_retry.set_defaults(handler=run)
