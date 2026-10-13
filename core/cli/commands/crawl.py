"""``crawl``: управление сеансами краула (local) — run/status/list/resume/restart/cancel.

Подпакет ``hound crawl`` (``03`` §E.1) — тонкая CLI-поверхность над durable-сеансом
краула (Lane B/L1a): :func:`core.pipeline.corpus.build_corpus` сам открывает/восстанавливает
и финализирует сеанс, поэтому команда только резолвит прогон/порты/фазы (как ``run``) и
вызывает фазу корпуса Ш5 — логика краула здесь **не** дублируется.

Живого worker'а в foreground-профиле ``local`` нет: ``cancel`` переводит сеанс в
терминальный ``cancelled`` (намерение оператора), а не прерывает работающий процесс.

Семантика подкоманд:

* ``run``     — запустить сеанс краула (фаза корпуса); ``--reset`` очищает сеанс до старта.
* ``status``  — сеанс и счётчики (``--per-url`` — построчно). Сеанса нет → ``EXIT_OK`` с
  ``status: null`` (пустой, но валидный машинночитаемый ответ).
* ``list``    — перечислить сеансы в БД прогона.
* ``resume``  — продолжить прерванный сеанс: та же ``session_id``, уже ``FETCHED``-элементы
  не пере-фетчатся (recovery-on-open + retained raw); идемпотентно.
* ``restart`` — очистить сеанс и запустить заново; ``--from-scratch`` документирует обход
  R7-кэша (Phase 4 — флаг принимается, но кэш пока **не** обходится).
* ``cancel``  — перевести сеанс в ``cancelled`` (best-effort; foreground без worker'а).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import sys
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _ensure_run,
    _load_render_modes,
    _phases_until,
    _print_json,
    _resolve_db_path,
    _resolve_plan,
    _resolve_ports,
    _resolve_phases,
)
from core.cli.errors import (
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    CliError,
    LintFailure,
)
from core.cli.wiring import build_store
from core.config.deploy import load_deploy_config
from core.domain import CrawlItemStatus, CrawlSessionStatus
from core.pipeline import PHASES
from core.pipeline.crawl_session import default_session_id
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.plan import derive_run_id
from core.pipeline.runner import RunContext

__all__ = [
    "configure",
    "run",
    "status",
    "list_sessions",
    "resume",
    "restart",
    "cancel",
]


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC) — метка финала сеанса (``cancel``)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _effective_runs_dir(args: argparse.Namespace) -> str:
    """Эффективный ``runs``-каталог: флаг > resolved-раскладка > прежний литерал ``runs``.

    ``crawl`` не входит в ``main._RUNS_DIR_COMMANDS``, поэтому дефолт раскладки
    подставляется здесь (composition root уже положил ``args.paths``).
    """
    runs_dir = getattr(args, "runs_dir", None)
    if runs_dir is None:
        paths = getattr(args, "paths", None)
        runs_dir = str(paths.runs) if paths is not None else "runs"
    return str(runs_dir)


def _crawl_db_path(args: argparse.Namespace) -> Path:
    """Резолвит БД сеанса: ``--db`` приоритетнее ``--runs-dir`` + ``--run-id``.

    :raises CliError: ``--runs-dir`` без ``--run-id`` (:data:`EXIT_USAGE`).
    """
    args.runs_dir = _effective_runs_dir(args)
    return Path(_resolve_db_path(args))


def _set_runs_dir(args: argparse.Namespace) -> None:
    """Проставляет эффективный ``runs_dir`` для ``_resolve_phases`` (out_dir Ш9)."""
    if getattr(args, "runs_dir", None) is None:
        args.runs_dir = _effective_runs_dir(args)


def _session_id_for(args: argparse.Namespace) -> tuple[str, str]:
    """``(run_id, session_id)`` для адресации сеанса; ``--run-id`` обязателен.

    :raises CliError: ``--run-id`` не задан (:data:`EXIT_USAGE`).
    """
    run_id = getattr(args, "run_id", None)
    if not run_id:
        raise CliError(
            "нужен --run-id (канонический session_id — f\"crawl:{run_id}\")",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return str(run_id), default_session_id(str(run_id))


def _crawl_counts(result: Any) -> dict[str, int]:
    """Счётчики краула из ``IndexBuild`` фазы корпуса (пусто, если фаза не запускалась)."""
    for entry in getattr(result, "log_entries", None) or ():
        if hasattr(entry, "crawl_pending"):
            return {
                "pending": int(entry.crawl_pending),
                "fetched": int(entry.crawl_fetched),
                "done": int(entry.crawl_done),
                "failed": int(entry.crawl_failed),
            }
    return {}


def _item_row(item: Any) -> dict[str, Any]:
    """JSON-совместимая строка элемента сеанса (``crawl status --per-url``)."""
    status = item.status
    return {
        "product_id": item.product_id,
        "url": item.url,
        "status": status.value if isinstance(status, CrawlItemStatus) else str(status),
        "attempts": item.attempts,
        "engine": item.engine,
        "kind": item.kind,
        "http_status": item.http_status,
        "content_type": item.content_type,
        "lang": item.lang,
        "error_kind": item.error_kind,
        "error": item.error,
    }


def _session_fields(session: Any) -> dict[str, Any]:
    """Поля жизненного цикла сеанса для payload (``None`` при отсутствии сеанса)."""
    if session is None:
        return {
            "status": None,
            "created_at": None,
            "started_at": None,
            "finished_at": None,
            "total": 0,
            "error": None,
        }
    return {
        "status": session.status.value,
        "created_at": session.created_at,
        "started_at": session.started_at,
        "finished_at": session.finished_at,
        "total": session.total,
        "error": session.error,
    }


def _print_crawl_run(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``crawl run``/``resume``/``restart``."""
    lines = [
        f"mode: {payload['mode']}",
        f"run_id: {payload['run_id']}",
        f"session_id: {payload['session_id']}",
        f"db: {payload['db']}",
        f"status: {payload['status']}",
    ]
    if payload.get("cleared"):
        lines.append("session: очищен перед запуском")
    counts = payload.get("crawl") or {}
    if counts:
        lines.append(
            "crawl: " + ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
        )
    for note in payload.get("notes", ()):
        lines.append(f"note: {note}")
    for error in payload.get("errors", ()):
        lines.append(f"error: {error}")
    import sys

    sys.stdout.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# run / resume / restart
# ---------------------------------------------------------------------------


def _execute_crawl(
    args: argparse.Namespace, *, mode: str, clear_first: bool, from_scratch: bool = False
) -> int:
    """Общий путь ``run``/``resume``/``restart``: резолв прогона → фаза корпуса Ш5.

    Сеанс открывается/восстанавливается/финализируется самим ``build_corpus``; здесь
    только DI: план/порты/фазы строятся тем же composition root'ом, что и ``run``.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    if not report.ok:
        raise LintFailure(report.blocking, report.warnings)

    run_id = args.run_id or derive_run_id(inputs)
    _set_runs_dir(args)
    db_path = _crawl_db_path(args)
    render_modes = _load_render_modes(getattr(args, "render_modes", None))
    deploy_config = load_deploy_config(None)

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

    session_id = default_session_id(run_id)
    until = getattr(args, "until", None) or "corpus"
    notes: list[str] = []
    if from_scratch:
        notes.append(
            "from-scratch: обход R7-кэша — Phase 4 (флаг принят, кэш пока не обходится)"
        )

    store = build_store(db_path, profile=args.profile)
    result: Any = None
    cleared = False
    try:
        _ensure_run(store, run_id, inputs)
        if clear_first:
            store.clear_crawl_session(session_id)
            cleared = True
        if "corpus" in _phases_until(until):
            ctx = RunContext(
                run_id=run_id,
                store=store,
                ports=ports,
                clock=getattr(args, "clock", None),
            )
            result = phases["corpus"](ctx)
        else:
            notes.append(f"корпус отсечён --until={until}")
    finally:
        store.close()
        close = getattr(getattr(ports, "crawler", None), "close", None)
        if callable(close):
            close()

    phase_status = result.status if result is not None else "skipped"
    payload = {
        "ok": phase_status == "ok",
        "mode": mode,
        "run_id": run_id,
        "session_id": session_id,
        "db": str(db_path),
        "status": phase_status,
        "cleared": cleared,
        "crawl": _crawl_counts(result),
        "errors": list(getattr(result, "errors", []) or []),
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        _print_crawl_run(payload)
    if phase_status == "ok":
        return EXIT_OK
    if phase_status == "skipped":
        return EXIT_OK
    return EXIT_ERROR


def run(args: argparse.Namespace) -> int:
    """``crawl run``: запустить сеанс краула (``--reset`` очищает сеанс до старта)."""
    return _execute_crawl(
        args, mode="run", clear_first=bool(getattr(args, "reset", False))
    )


def resume(args: argparse.Namespace) -> int:
    """``crawl resume``: продолжить сеанс (идемпотентно; ``FETCHED`` не пере-фетчатся)."""
    return _execute_crawl(args, mode="resume", clear_first=False)


def restart(args: argparse.Namespace) -> int:
    """``crawl restart``: очистить сеанс и запустить заново (``--from-scratch`` — Phase 4)."""
    return _execute_crawl(
        args,
        mode="restart",
        clear_first=True,
        from_scratch=bool(getattr(args, "from_scratch", False)),
    )


# ---------------------------------------------------------------------------
# status / list / cancel
# ---------------------------------------------------------------------------


def status(args: argparse.Namespace) -> int:
    """``crawl status``: сеанс и счётчики; отсутствие сеанса → ``status: null`` (``EXIT_OK``)."""
    db_path = _crawl_db_path(args)
    run_id, session_id = _session_id_for(args)
    per_url = bool(getattr(args, "per_url", False))
    with build_store(db_path, profile=args.profile) as store:
        session = store.get_crawl_session(session_id)
        counts = store.crawl_session_counts(session_id) if session is not None else {}
        items = (
            list(store.iter_crawl_session_items(session_id))
            if session is not None and per_url
            else []
        )
        payload: dict[str, Any] = {
            "ok": True,
            "run_id": run_id,
            "session_id": session_id,
            "db": str(db_path),
            "counts": counts,
            **_session_fields(session),
        }
        if per_url:
            payload["items"] = [_item_row(item) for item in items]
        if args.json:
            _print_json(payload)
        elif session is None:
            sys.stdout.write(f"session_id: {session_id}\nstatus: —\n(сеанс не найден)\n")
        else:
            lines = [
                f"session_id: {session_id}",
                f"status: {session.status.value}",
                f"total: {session.total}",
                "counts: " + ", ".join(f"{key}={counts[key]}" for key in sorted(counts)),
            ]
            for row in payload.get("items", ()):
                lines.append(f"  {row['product_id']} {row['status']} {row['url']}")
            sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


def list_sessions(args: argparse.Namespace) -> int:
    """``crawl list``: перечислить сеансы в БД прогона (read-only)."""
    db_path = _crawl_db_path(args)
    with build_store(db_path, profile=args.profile) as store:
        rows = store.conn.execute(
            "SELECT session_id, run_id, profile, status, created_at, started_at,"
            " finished_at, total, pending, running, fetched, failed, cancelled"
            " FROM crawl_sessions ORDER BY session_id"
        ).fetchall()
        sessions = [{key: row[key] for key in row.keys()} for row in rows]
        if args.json:
            _print_json(
                {
                    "ok": True,
                    "db": str(db_path),
                    "count": len(sessions),
                    "sessions": sessions,
                }
            )
        else:
            lines = [f"db: {db_path}", f"sessions: {len(sessions)}"]
            for session in sessions:
                lines.append(
                    f"  {session['session_id']} [{session['status']}] "
                    f"total={session['total']} fetched={session['fetched']} "
                    f"failed={session['failed']}"
                )
            sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


def cancel(args: argparse.Namespace) -> int:
    """``crawl cancel``: перевести сеанс в ``cancelled`` (best-effort, foreground без worker'а).

    Отсутствие сеанса → ``EXIT_NOT_FOUND`` с понятным сообщением (отменять нечего).
    """
    db_path = _crawl_db_path(args)
    run_id, session_id = _session_id_for(args)
    store = build_store(db_path, profile=args.profile)
    try:
        if store.get_crawl_session(session_id) is None:
            if args.json:
                _print_json(
                    {
                        "ok": False,
                        "run_id": run_id,
                        "session_id": session_id,
                        "db": str(db_path),
                        "error": "not_found",
                        "status": None,
                    }
                )
            else:
                sys.stdout.write(
                    f"session_id: {session_id}\n(сеанс не найден — отменять нечего)\n"
                )
            return EXIT_NOT_FOUND
        store.update_crawl_session_status(
            session_id, CrawlSessionStatus.CANCELLED, finished_at=_now_iso()
        )
        session = store.get_crawl_session(session_id)
        payload = {
            "ok": True,
            "run_id": run_id,
            "session_id": session_id,
            "db": str(db_path),
            "notes": [
                "local: живого worker'а нет — статус переведён в cancelled (намерение оператора)"
            ],
            **_session_fields(session),
        }
        if args.json:
            _print_json(payload)
        else:
            sys.stdout.write(f"session_id: {session_id}\nstatus: {payload['status']}\n")
        return EXIT_OK
    finally:
        store.close()


def _no_subcommand(args: argparse.Namespace) -> int:
    """``hound crawl`` без подкоманды: понятная ошибка использования (``EXIT_USAGE``)."""
    raise CliError(
        "не указана подкоманда crawl (run|status|list|resume|restart|cancel)",
        code="usage",
        exit_code=EXIT_USAGE,
    )


# ---------------------------------------------------------------------------
# Регистрация
# ---------------------------------------------------------------------------


def _add_source_flags(parser: argparse.ArgumentParser) -> None:
    """Источники корпуса Ш5: ``--urls`` и ``--render-modes`` (как у ``run``)."""
    parser.add_argument(
        "--urls",
        default=None,
        help="опциональный JSON/YAML {product_id: [url, ...]} — операторский корпусный вход Ш5",
    )
    parser.add_argument(
        "--render-modes",
        dest="render_modes",
        default=None,
        metavar="FILE",
        help="опциональный JSON/YAML {url: \"http\"|\"browser\"} — per-URL режим рендера Ш5",
    )


def _add_run_target_flags(parser: argparse.ArgumentParser) -> None:
    """Общие флаги резолва прогона (как у ``run``): run-yaml/package/run-id/runs-dir/db."""
    parser.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    parser.add_argument("--package", required=True, help="каталог пакета категории")
    parser.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    parser.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db); дефолт — resolved раскладка, иначе runs",
    )
    parser.add_argument("--db", default=None, help="путь к БД прогона (иначе runs/{run_id}/...)")


def _add_session_target_flags(
    parser: argparse.ArgumentParser, *, require_run_id: bool
) -> None:
    """Флаги адресации сеанса: ``--run-id`` (``session_id = f"crawl:{run_id}"``),
    ``--runs-dir``, ``--db``."""
    parser.add_argument(
        "--run-id",
        required=require_run_id,
        default=None,
        help="идентификатор прогона (канонический session_id — f\"crawl:{run_id}\")",
    )
    parser.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db); дефолт — resolved раскладка, иначе runs",
    )
    parser.add_argument("--db", default=None, help="путь к БД прогона (приоритетнее --runs-dir)")


def configure(sub: Any) -> None:
    """Регистрирует группу ``crawl`` с подкомандами run/status/list/resume/restart/cancel."""
    p_crawl = sub.add_parser(
        "crawl", help="сеансы краула (local): run/status/list/resume/restart/cancel"
    )
    crawl = p_crawl.add_subparsers(dest="crawl_command", metavar="ДЕЙСТВИЕ")

    p_run = crawl.add_parser("run", help="запустить сеанс краула (фаза корпуса Ш5)")
    _add_run_target_flags(p_run)
    _add_source_flags(p_run)
    p_run.add_argument(
        "--until",
        choices=PHASES,
        default="corpus",
        help="фаза-потолок (дефолт corpus); если corpus отсечён — краул не запускается",
    )
    p_run.add_argument(
        "--reset", action="store_true", help="очистить сеанс краула перед запуском"
    )
    _add_common(p_run)
    p_run.set_defaults(handler=run)

    p_status = crawl.add_parser("status", help="статус сеанса и счётчики (read-only)")
    _add_session_target_flags(p_status, require_run_id=True)
    p_status.add_argument(
        "--per-url", dest="per_url", action="store_true", help="построчно: элементы сеанса"
    )
    _add_common(p_status)
    p_status.set_defaults(handler=status)

    p_list = crawl.add_parser("list", help="перечислить сеансы в БД прогона (read-only)")
    _add_session_target_flags(p_list, require_run_id=False)
    _add_common(p_list)
    p_list.set_defaults(handler=list_sessions)

    p_resume = crawl.add_parser("resume", help="продолжить прерванный сеанс (идемпотентно)")
    _add_run_target_flags(p_resume)
    _add_source_flags(p_resume)
    _add_common(p_resume)
    p_resume.set_defaults(handler=resume)

    p_restart = crawl.add_parser("restart", help="очистить сеанс и запустить заново")
    _add_run_target_flags(p_restart)
    _add_source_flags(p_restart)
    p_restart.add_argument(
        "--from-scratch",
        dest="from_scratch",
        action="store_true",
        help="документирует обход R7-кэша (Phase 4; сейчас кэш не обходится)",
    )
    _add_common(p_restart)
    p_restart.set_defaults(handler=restart)

    p_cancel = crawl.add_parser("cancel", help="перевести сеанс в терминальный cancelled")
    _add_session_target_flags(p_cancel, require_run_id=True)
    _add_common(p_cancel)
    p_cancel.set_defaults(handler=cancel)

    p_crawl.set_defaults(handler=_no_subcommand)
