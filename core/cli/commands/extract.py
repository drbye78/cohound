"""``extract``: вход потребителя очереди конверсии (17 §6, F6).

Команда — **не второй потребитель**: она разрешает прогон и запускает ту же фазу
корпуса Ш5 (:func:`core.pipeline.corpus.build_corpus`) в режиме ``consume_only``, где
сетевые шаги (дренаж корней, ревалидация, SERP) не исполняются, а ``extract_consume.
consume`` читает durable-очередь ``extract_queue`` уже завершённого сеанса краула.
Логика конверсии живёт в одном месте — :func:`core.pipeline.extract_consume.consume`, и
второй её реализации здесь нет: команда только печатает **её** отчёт.

Зачем отдельный вход. ``crawl run --crawl-only`` наполняет очередь и не конвертирует;
``hound extract consume`` доводит её до корпуса другой командой (другой процесс, другое
время). Это и есть смысл разделения краула и конверсии: «краул выполнен» ≠ «корпус
собран», и сумма времени двух команд — честная стоимость, а не одна цифра фазы.

Печатается **наблюдённое** (``done/failed/skipped/claimed/reclaimed/heartbeats/pending``
и ``backlog_bytes``/``free_bytes``), а не оценка. Частичный исход (строки остались
незакрытыми или закрылись ``failed``) — :data:`EXIT_ERROR`, а не «ok»: тихий успех на
незакрытой очереди выдал бы неполный корпус за собранный.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _ensure_run,
    _resolve_db_path,
    _resolve_phases,
    _resolve_plan,
    _resolve_ports,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, EXIT_USAGE, CliError, LintFailure
from core.cli.execution_axes import (
    declared_status_dict_version,
    resolve_declared_axes,
)
from core.cli.wiring import build_store, resolve_extraction_budget
from core.config.deploy import load_deploy_config
from core.pipeline.crawl_session import default_session_id
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.runner import RunContext


def _effective_runs_dir(args: argparse.Namespace) -> str:
    """Каталог прогонов: ``--runs-dir`` > resolved раскладка > ``runs``."""
    runs_dir = getattr(args, "runs_dir", None)
    if runs_dir is None:
        paths = getattr(args, "paths", None)
        runs_dir = str(paths.runs) if paths is not None else "runs"
    return str(runs_dir)


def _db_path(args: argparse.Namespace) -> Path:
    """БД прогона: ``--db`` приоритетнее ``--runs-dir`` + ``--run-id``."""
    args.runs_dir = _effective_runs_dir(args)
    return Path(_resolve_db_path(args))


def _session_and_run_id(args: argparse.Namespace) -> tuple[str, str]:
    """``(session_id, run_id)``: сеанс адресует прогон, а не наоборот.

    ``session_id`` канонический (``crawl:{run_id}``), поэтому ``run_id`` выводится из него
    обратно: команда приходит **после** краула, и второго вычисления ``run_id`` (со своим
    набором объявленных осей) здесь быть не должно — оно разошлось бы с прогоном, и
    конверсия писала бы в чужую строку.
    """
    session = getattr(args, "session", None)
    run_id = getattr(args, "run_id", None)
    if session:
        if run_id and default_session_id(str(run_id)) != str(session):
            raise CliError(
                f"--session {session} не соответствует --run-id {run_id} "
                f"(канонический session_id — f\"crawl:{{run_id}}\")",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        run_id = str(session).removeprefix("crawl:")
    if not run_id:
        raise CliError(
            "нужен --session (канонический f\"crawl:{run_id}\") либо --run-id",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return default_session_id(str(run_id)), str(run_id)


def _consumer_records(result: Any) -> list[dict[str, Any]]:
    """Отчёты потребителя из ``log_entries`` фазы (структурные записи ``extract_consume``)."""
    records = [
        entry
        for entry in getattr(result, "log_entries", None) or ()
        if isinstance(entry, dict) and entry.get("event") == "extract_consume"
    ]
    return [dict(record) for record in records]


def _consumer_payload(
    records: list[dict[str, Any]], *, run_id: str, session_id: str, db: Path
) -> dict[str, Any]:
    """Сводка прохода: суммы по продуктам + backlog последней записи (числа, не оценка).

    ``backlog`` берётся из записи, а не считается здесь: ``backlog_bytes`` — сумма
    размеров retained-файлов строк ``pending``+``claimed``, и её знает только сам проход
    по очереди (второй расчёт разошёлся бы с первым при удалении файла).
    """
    totals = {
        key: sum(int(record.get(key) or 0) for record in records)
        for key in (
            "done",
            "failed",
            "already",
            "lost",
            "claimed",
            "reclaimed",
            "heartbeats",
            "pending",
            "docs",
        )
    }
    backlog = dict(records[-1].get("backlog") or {}) if records else {}
    state_counts = dict(backlog.get("queue_rows") or {})
    return {
        "ok": totals["pending"] == 0 and totals["failed"] == 0,
        "run_id": run_id,
        "session_id": session_id,
        "db": str(db),
        # ``skipped`` — терминальные строки, которые потребитель не конвертирует
        # (краул закрыл их как ``skipped``); число берётся из состояния очереди, а не
        # выводится как «already»: это разные факты.
        "skipped": int(state_counts.get("skipped", 0)),
        "queue_rows": state_counts,
        "totals": totals,
        "backlog_bytes": int(backlog.get("backlog_bytes") or 0),
        "backlog_files": int(backlog.get("backlog_files") or 0),
        "backlog_missing": int(backlog.get("backlog_missing") or 0),
        "backlog_limit_bytes": backlog.get("backlog_limit_bytes"),
        "free_bytes": backlog.get("free_bytes"),
        "products": records,
    }


def run(args: argparse.Namespace) -> int:
    """``extract consume``: довести durable-очередь сеанса до корпуса.

    Сеанса нет или очередь пуста → :data:`EXIT_OK` с честной строкой: «конвертировать
    нечего» — валидный исход, а не сбой. Частичный проход (остались ``pending``/
    ``claimed`` либо закрылись ``failed``) → :data:`EXIT_ERROR`: незакрытая очередь
    означает неполный корпус, и притворяться успехом здесь дороже всего (I2/I2b).
    """
    session_id, run_id = _session_and_run_id(args)
    args.run_id = run_id
    db_path = _db_path(args)
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    if not report.ready:
        raise LintFailure([*report.blocking, *report.incomplete], report.warnings)

    store = build_store(db_path, profile=args.profile)
    ports: Any = None
    result: Any = None
    try:
        if store.get_crawl_session(session_id) is None:
            # Нет сеанса — нет очереди: конвертировать нечего. Это не ошибка входа:
            # оператор мог ошибиться адресом, и честный текст полезнее исключения.
            payload: dict[str, Any] = {
                "ok": True,
                "run_id": run_id,
                "session_id": session_id,
                "db": str(db_path),
                "session_found": False,
                "note": "сеанс не найден — конвертировать нечего",
                "totals": {},
                "backlog_bytes": 0,
                "products": [],
            }
            if args.json:
                sys.stdout.write(_json(payload) + "\n")
            else:
                sys.stdout.write(
                    f"session_id: {session_id}\n(db: {db_path})\n"
                    "сеанс не найден — конвертировать нечего\n"
                )
            return EXIT_OK
        _ensure_run(store, run_id, inputs)
        deploy_config = load_deploy_config(None)
        declared = resolve_declared_axes(args=args, inputs=inputs, deploy=deploy_config)
        status_dict_version = declared_status_dict_version()
        #: Продукты — из объявленного объёма прогона: очередь ключуется ``product_id``, и
        #: конверсия без продукта оставила бы его строки вне корпуса молча.
        declared_products = sorted(str(pid) for pid in inputs.run.products)
        only_product = getattr(args, "only_product", None)
        if only_product is not None and str(only_product) not in declared_products:
            raise CliError(
                f"продукт {only_product} не объявлен в run.yaml "
                f"({', '.join(declared_products) or '—'})",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        products = [str(only_product)] if only_product else declared_products
        #: Пустые корни: краул не исполняется ни для одного продукта (F6). Список URL —
        #: носитель объёма: пустой список = «продукт в прогоне есть, корней нет».
        urls: dict[str, list[str]] = {pid: [] for pid in products}
        run_plan = _resolve_plan(args, inputs, run_id)
        mem_budget_mb, derive_extract_workers = resolve_extraction_budget(
            deploy_config.profile,
            injected_ports=getattr(args, "ports", None) is not None,
        )
        args.runs_dir = _effective_runs_dir(args)
        ports = _resolve_ports(
            args, deploy_config, inputs=inputs, no_cache=False, run_id=run_id
        )
        phases = _resolve_phases(
            args,
            inputs=inputs,
            plan=run_plan,
            ports=ports,
            deploy_config=deploy_config,
            paths=getattr(args, "paths", None),
            render_modes=None,
            urls=urls,
            mem_budget_mb=mem_budget_mb,
            derive_extract_workers=derive_extract_workers,
            axes=declared,
            declared_status_dict_version=status_dict_version,
            consume_only=True,
        )
        ctx = RunContext(
            run_id=run_id, store=store, ports=ports, clock=getattr(args, "clock", None)
        )
        result = phases["corpus"](ctx)
    finally:
        store.close()
        close = getattr(getattr(ports, "crawler", None), "close", None)
        if callable(close):
            close()

    records = _consumer_records(result)
    payload = _consumer_payload(
        records, run_id=run_id, session_id=session_id, db=db_path
    )
    payload["phase_status"] = getattr(result, "status", None)
    payload["errors"] = sorted(getattr(result, "errors", None) or ())
    if args.json:
        sys.stdout.write(_json(payload) + "\n")
    else:
        sys.stdout.write(_format_report(payload) + "\n")
    # F6: «частичный исход» — не успех. Корпус, чья очередь не закрылась, неполон; код
    # возврата называет это снаружи, а не только строкой в выводе.
    if payload["totals"].get("pending") or payload["totals"].get("failed"):
        return EXIT_ERROR
    if getattr(result, "status", None) == "failed":
        return EXIT_ERROR
    return EXIT_OK


def _json(payload: dict[str, Any]) -> str:
    """JSON-представление полезной нагрузки (``sort_keys`` — стабильная форма)."""
    import json

    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _format_report(payload: dict[str, Any]) -> str:
    """Человекочитаемая форма: числа прохода, backlog, восстановленные аренды."""
    totals = payload.get("totals") or {}
    lines = [
        f"session_id: {payload['session_id']}",
        f"db: {payload['db']}",
        "converted: "
        + ", ".join(
            f"{key}={totals.get(key, 0)}"
            for key in ("done", "failed", "already", "lost", "claimed", "reclaimed")
        ),
        f"pending: {totals.get('pending', 0)}"
        f" skipped: {payload.get('skipped', 0)}"
        f" docs: {totals.get('docs', 0)}"
        f" heartbeats: {totals.get('heartbeats', 0)}",
        "backlog: "
        f"backlog_bytes={payload.get('backlog_bytes', 0)}"
        f" backlog_files={payload.get('backlog_files', 0)}"
        f" free_bytes={payload.get('free_bytes')}"
        f" backlog_limit_bytes={payload.get('backlog_limit_bytes')}",
    ]
    if payload.get("backlog_missing"):
        lines.append(
            f"backlog_missing: {payload['backlog_missing']} (строка обещала байты — I19)"
        )
    for record in payload.get("products") or ():
        lines.append(
            f"  {record['product_id']}: done={record['done']} failed={record['failed']} "
            f"already={record['already']} pending={record['pending']} "
            f"claimed={record['claimed']} reclaimed={record['reclaimed']}"
        )
        for item in record.get("reclaim_records") or ():
            lines.append(
                f"    reclaimed {item['product_id']}/{item['url']}: "
                f"attempts={item['attempts']} last_owner={item['claimed_by'] or '—'} "
                f"exhausted={bool(item['exhausted'])}"
            )
    for error in payload.get("errors") or ():
        lines.append(f"error: {error}")
    if payload["totals"].get("pending"):
        lines.append(
            "неполный проход: очередь не закрыта — корпус продукта не полон"
        )
    return "\n".join(lines)


def configure(sub: Any) -> None:
    """Регистрирует ``extract`` с подкомандой ``consume``."""
    p_extract = sub.add_parser(
        "extract", help="потребитель очереди конверсии: consume (F6)"
    )
    extract = p_extract.add_subparsers(dest="extract_command", metavar="ДЕЙСТВИЕ")

    p_consume = extract.add_parser(
        "consume",
        help="довести durable-очередь сеанса до корпуса (тот же consumer, что в фазе)",
    )
    p_consume.add_argument(
        "--run-yaml", required=True, help="путь к run.yaml (§4.5)"
    )
    p_consume.add_argument("--package", required=True, help="каталог пакета категории")
    p_consume.add_argument(
        "--session",
        default=None,
        help="идентификатор сеанса краула (канонический f\"crawl:{run_id}\")",
    )
    p_consume.add_argument(
        "--run-id",
        default=None,
        help="идентификатор прогона (нужен, если не задан --session)",
    )
    p_consume.add_argument(
        "--product",
        dest="only_product",
        default=None,
        metavar="ID",
        help="только указанный продукт (по умолчанию — все объявленные)",
    )
    p_consume.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db); дефолт — resolved раскладка",
    )
    p_consume.add_argument("--db", default=None, help="путь к БД прогона")
    _add_common(p_consume)
    p_consume.set_defaults(handler=run)

    p_extract.set_defaults(handler=_no_subcommand)


def _no_subcommand(args: argparse.Namespace) -> int:
    """``hound extract`` без подкоманды: понятная ошибка использования."""
    raise CliError(
        "не указана подкоманда extract (consume)", code="usage", exit_code=EXIT_USAGE
    )