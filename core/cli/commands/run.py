"""``run``: собрать прогон и провести ``Runner`` по реальным фазам Ш1–Ш9.

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _concise,
    _ensure_run,
    _load_render_modes,
    _phases_until,
    _positive_int,
    _print_json,
    _print_run_dry,
    _print_run_report,
    _resolve_plan,
    _resolve_ports,
    _resolve_phases,
    _run_report_payload,
    _snapshot_run_inputs,
)
from core.cli.errors import EXIT_BLOCKED, EXIT_ERROR, EXIT_OK, LintFailure
from core.cli.wiring import (
    SPLIT_PROFILE,
    WiringError,
    build_runner,
    build_store,
    manifest_deploy_fields,
    run_db_path,
)
from core.config.deploy import is_split, load_deploy_config
from core.pipeline import GATE_PHASES, PHASES
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.observability import instrument_phases, write_run_metrics
from core.pipeline.plan import derive_run_id
from core.pipeline.runner import PhaseFn, _gate_phase


def run(args: argparse.Namespace) -> int:
    """``run``: собрать прогон и провести ``Runner`` по реальным фазам Ш1–Ш9.

    Composition root строит план и реестр фаз (:func:`build_phases`) и связывает их с
    портами профиля (:func:`_resolve_ports`). Гейты G1–G4 намеренно **не** автоматизируются
    (I6/I7): без строки в ``gate_decisions`` прогон останавливается ``blocked`` на ``g1``.
    Resume honour'ится: строка прогона **не пересоздаётся**, если она уже есть, поэтому
    durable-карта фаз в ``runs.state`` сохраняется между процессами (``02`` §A.1).

    ``--dry-run`` показывает план фаз/линт без записи; ``--reset`` форсирует полный
    перезапуск в обход durable-состояния.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    run_id = args.run_id or derive_run_id(inputs)
    db_path = Path(args.db) if args.db is not None else run_db_path(args.runs_dir, run_id)
    # Валидация per-URL режимов рендера (Ш5) — до dry-run, чтобы ошибка входа была
    # видна и в сухом прогоне (I19: никакого молчаливого фетча/подмены).
    render_modes = _load_render_modes(getattr(args, "render_modes", None))

    if args.dry_run:
        if not report.ok:
            raise LintFailure(report.blocking, report.warnings)
        payload = {
            "ok": True,
            "dry_run": True,
            "run_id": run_id,
            "db": str(db_path),
            "until": args.until,
            "phases": _phases_until(args.until),
            "render_modes": {url: mode.value for url, mode in render_modes.items()},
            "warnings": list(report.warnings),
        }
        if args.json:
            _print_json(payload)
        else:
            _print_run_dry(payload)
        return EXIT_OK

    if not report.ok:
        raise LintFailure(report.blocking, report.warnings)

    # R7 (07 §7): сведения о профиле зон для манифеста прогона. ГЭП: CLI W5 ещё не
    # собирает run_manifest.yaml (это W10/T10.4), поэтому поля добавляются в отчёт
    # прогона (`deploy`) и готовы для edge_capabilities через manifest_deploy_fields.
    # Конфиг загружается всегда: секция ``crawl`` (03 §D.5/D.6) — операционная политика
    # HTTP-краулинга корпуса и в профиле ``local``; для ``split`` дополнительно
    # валидируется шов и собираются deploy-поля манифеста.
    deploy_fields: dict[str, Any] = {}
    deploy_config = load_deploy_config(None)
    if args.profile == SPLIT_PROFILE:
        if not is_split(deploy_config):
            raise WiringError(
                "профиль split требует deploy-конфиг с profile split/split-hardened "
                "(07 §6/§7)"
            )
        deploy_fields = manifest_deploy_fields(deploy_config)

    # План строится ОДИН раз здесь и переиспользуется фазами: повторный ``plan()`` внутри
    # фазы дал бы иной ``run_id``/двойной линт (см. core/pipeline/phases.py). ``run_id``
    # переиспользуется тот же, что выведен выше, — второго derive нет.
    run_plan = _resolve_plan(args, inputs, run_id)
    ports = _resolve_ports(args, deploy_config, inputs=inputs)
    phases = _resolve_phases(
        args,
        inputs=inputs,
        plan=run_plan,
        ports=ports,
        paths=getattr(args, "paths", None),
        render_modes=render_modes,
    )
    # Наблюдаемость (W12/T12.1, 04 §B.1/§B.2): оборачиваем фазы логом+таймингом.
    # Гейты — дефолтные замыкания Runner; оборачиваем и их, чтобы §B.1-лог покрывал
    # точку остановки (g1 без человеческого решения тоже оставляет logs/g1.jsonl).
    clock = getattr(args, "clock", None)
    instrumentable: dict[str, PhaseFn] = dict(phases)
    for gate_name, gate in GATE_PHASES.items():
        instrumentable.setdefault(gate_name, _gate_phase(gate_name, gate))
    instrumentation = instrument_phases(
        instrumentable,
        run_id=run_id,
        logs_root=args.runs_dir,
        clock=clock,
    )

    instrumentation_errors: list[str] = []
    store = build_store(db_path, profile=args.profile)
    try:
        _ensure_run(store, run_id, inputs)
        # 03 §A.2: снимок входов прогона (иммутабельная копия run.yaml + plan_inputs.json)
        # записывается ДО исполнения фаз, чтобы последующие команды были самодостаточны.
        # Сбой снимка не роняет прогон: фиксируется в instrumentation_errors и не
        # подменяет код возврата.
        try:
            instrumentation_errors.extend(
                _snapshot_run_inputs(args, run_id, inputs=inputs, run_plan=run_plan)
            )
        except Exception as exc:  # noqa: BLE001 — самодостаточность не роняет прогон
            instrumentation_errors.append(
                f"snapshot: {type(exc).__name__}: {_concise(exc)}"
            )
        runner = build_runner(
            store,
            profile=args.profile,
            ports=ports,
            phases=instrumentation.phases,
            clock=clock,
        )
        if args.reset:
            runner.reset(run_id)
            # D3: сброс durable-краула — очищаем crawl_attempts, чтобы resume не подхватил
            # старые retained-записи. Глобальный R7-кэш может всё ещё дедуплицировать
            # идентичные байты: --reset форсирует повторную попытку, но не обязательно живой
            # сетевой фетч (явный --no-cache — документированный follow-up, вне рамок D3).
            try:
                store.clear_crawl_attempts(run_id)
            except Exception as exc:  # noqa: BLE001 — сброс best-effort, не роняет прогон
                instrumentation_errors.append(
                    f"reset crawl_attempts: {type(exc).__name__}: {_concise(exc)}"
                )
        run_report = runner.run(run_id, until=args.until)
        # Метрики (04 §B.2) пишутся ДО закрытия store (источник ячеек) и независимо от
        # остановки на гейте — отражают реально отработавшие фазы. Сбой наблюдаемости
        # фиксируется, но не подменяет код возврата прогона.
        try:
            write_run_metrics(
                run_id=run_id,
                out_dir=args.runs_dir,
                durations=instrumentation.durations,
                store=store,
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость не роняет прогон
            instrumentation_errors.append(
                f"metrics: {type(exc).__name__}: {_concise(exc)}"
            )
    finally:
        store.close()
        # Best-effort остановка краулера: только если он есть и умеет ``close`` (после
        # разделения реактора — ScrapyCrawler; у HttpCrawler/None ``close`` нет). Проверка
        # существования делает вызов безопасным для обоих краулеров.
        close = getattr(ports, "crawler", None)
        close = getattr(close, "close", None)
        if callable(close):
            close()

    payload = _run_report_payload(run_id, run_report, db_path)
    if instrumentation_errors:
        payload["instrumentation_errors"] = instrumentation_errors
    if deploy_fields:
        payload["deploy"] = deploy_fields
    if args.json:
        _print_json(payload)
    else:
        _print_run_report(payload)
    if run_report.stopped_at is None:
        return EXIT_OK
    if run_report.status == "blocked":
        return EXIT_BLOCKED
    return EXIT_ERROR


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``run``."""
    p_run = sub.add_parser(
        "run", help="минимальный прогон: создать прогон и провести Runner по фазам"
    )
    p_run.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    p_run.add_argument("--package", required=True, help="каталог пакета категории")
    p_run.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    p_run.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db); дефолт — раскладка "
        "(config/paths.yaml/COHOUND_RUNS_DIR/COHOUND_HOME), иначе runs",
    )
    p_run.add_argument("--db", default=None, help="путь к БД прогона (иначе runs/{run_id}/...)")
    p_run.add_argument(
        "--until", choices=PHASES, default=None, help="последняя выполняемая фаза (включительно)"
    )
    p_run.add_argument(
        "--dry-run", action="store_true", help="показать план фаз без записи и исполнения"
    )
    p_run.add_argument(
        "--reset", action="store_true", help="форсировать полный перезапуск (обход resume)"
    )
    p_run.add_argument(
        "--passports-dir",
        default=None,
        help="каталог паспортов passports/{product_id}/{YYYY-MM-DD}.yaml (дефолт — "
        "раскладка: config/paths.yaml/COHOUND_PASSPORTS_DIR/COHOUND_HOME, иначе passports; "
        "отсутствие каталога → без паспортов)",
    )
    p_run.add_argument(
        "--urls",
        default=None,
        help="опциональный JSON/YAML {product_id: [url, ...]} — операторский корпусный вход "
        "Ш5; без него корпус URL пуст (doc_domains паспортов не фетчатся молча)",
    )
    p_run.add_argument(
        "--render-modes",
        dest="render_modes",
        default=None,
        metavar="FILE",
        help="опциональный JSON/YAML {url: \"http\"|\"browser\"} — per-URL режим рендера Ш5 "
        "(дефолт: HTTP/по домен-профилю)",
    )
    p_run.add_argument(
        "--cutoff-k",
        dest="cutoff_k",
        type=_positive_int,
        default=None,
        metavar="K",
        help="retrieval_cutoff_K (I20) выреза ретривала для фазы оценки; дефолт — "
        "DEFAULT_RETRIEVAL_CUTOFF_K (10)",
    )
    p_run.add_argument(
        "--llm-slot",
        dest="llm_slot",
        choices=["cheap", "flagship"],
        default=None,
        help="активный LLM-слот оценщика (override COHOUND_LLM_SLOT/deploy.llm.slot)",
    )
    _add_common(p_run)
    p_run.set_defaults(handler=run)
