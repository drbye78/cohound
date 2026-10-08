"""coHound — CLI-ядро (composition root Z0, ручной DI, A1; ``03`` §E).

Точка входа :func:`main` реализует команды W5/T5.3: ``resolve``, ``plan`` и минимальный
``run`` (реальный конвейер Ш1–Ш9 подключается в W7–W11, T10.4). Только stdlib
(``argparse``/``json``/``sys``) — без фреймворков (A1).

**Принципы UX (``03`` §E.2).**

- гейты — явные человеческие решения: ``run`` останавливается и печатает, что требуется;
- идемпотентность/resume: повторный ``run`` продолжает с прерванной фазы;
- ``--dry-run`` — показать план/бюджеты без исполнения и без записи;
- ``--json`` — машинночитаемый вывод для CI и интеграций;
- ошибки несут **код инварианта/правила**: ``I2b: ...``, ``S11: ...`` (ссылка на регламент).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from core.cli.wiring import (
    LOCAL_PROFILE,
    SUPPORTED_PROFILES,
    WiringError,
    build_runner,
    build_store,
    run_db_path,
)
from core.pipeline import PHASES, RunReport
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import RunInputs, load_inputs
from core.pipeline.plan import RunPlan, derive_run_id, plan as build_plan

__all__ = [
    "EXIT_OK",
    "EXIT_ERROR",
    "EXIT_USAGE",
    "EXIT_BLOCKED",
    "EXIT_NOT_FOUND",
    "CliError",
    "LintFailure",
    "main",
    "build_parser",
]

# -- Коды возврата ---------------------------------------------------------- #
#: Успех.
EXIT_OK = 0
#: Непредвиденная ошибка исполнения.
EXIT_ERROR = 1
#: Ошибка входа/конфигурации: блокирующий линт, валидация, неразобранные аргументы.
EXIT_USAGE = 2
#: Прогон остановлен на гейте (нужно человеческое решение, ``03`` §E.2).
EXIT_BLOCKED = 3
#: Запрошенный объект не найден (например, ``source_id`` не резолвится).
EXIT_NOT_FOUND = 4

#: Код по умолчанию для ошибок без распознанного инварианта/правила.
_FALLBACK_CODE = "E"

#: ``CODE: message`` — сообщения линтера/инвариантов начинаются с кода правила.
_CODE_RE = re.compile(r"^\s*([^\s:]+):")
#: Код инварианта внутри текста (например, ``I2b``).
_INVARIANT_RE = re.compile(r"\bI\d+[a-z]?\b")
#: Ссылка на раздел регламента (например, ``§4.2``/``§4.2/I2b``).
_SECTION_RE = re.compile(r"§[\d.]+(?:/[^\s:,;]+)?")


class CliError(Exception):
    """Ошибка CLI с кодом инварианта/правила и кодом возврата.

    Сообщение печатается как ``CODE: message`` в stderr (или в JSON при ``--json``).
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        exit_code: int = EXIT_ERROR,
    ) -> None:
        super().__init__(message)
        self.code = code or extract_code(message)
        self.exit_code = exit_code


class LintFailure(CliError):
    """Блокирующие нарушения линтера входов (``03`` §D.3/§E.2).

    ``blocking``/``warnings`` сохраняются раздельно: CLI печатает блокирующие в stderr
    (каждое сообщение уже несёт код правила), предупреждения — отдельной строкой.
    """

    def __init__(self, blocking: Sequence[str], warnings: Sequence[str] = ()) -> None:
        super().__init__(
            "блокирующие нарушения линтера", code="lint", exit_code=EXIT_USAGE
        )
        self.blocking = list(blocking)
        self.warnings = list(warnings)


# ---------------------------------------------------------------------------
# Разбор кодов и вывод
# ---------------------------------------------------------------------------


def extract_code(message: str) -> str:
    """Извлекает код инварианта/правила из сообщения (для ``CODE: message``).

    Порядок попыток: ведущий ``CODE:`` (сообщения линтера), затем инвариант вида
    ``I2b`` в тексте (сообщения домена), затем ссылка ``§4.2``. Если ничего не найдено —
    :data:`_FALLBACK_CODE`.
    """
    text = (message or "").strip()
    if not text:
        return _FALLBACK_CODE
    match = _CODE_RE.match(text)
    if match and len(match.group(1)) <= 32:
        return match.group(1)
    for pattern in (_INVARIANT_RE, _SECTION_RE):
        found = pattern.search(text)
        if found:
            return found.group(0)
    return _FALLBACK_CODE


def _concise(exc: BaseException) -> str:
    """Однострочное представление исключения (без простыней traceback)."""
    return " ".join(str(exc).split()) or type(exc).__name__


def _print_json(payload: dict[str, Any]) -> None:
    """Печатает JSON-объект в stdout (машинночитаемый вывод, ``--json``)."""
    sys.stdout.write(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    )


def _emit_lint_failure(args: argparse.Namespace, exc: LintFailure) -> None:
    """Печатает блокирующий линт: каждое сообщение уже несёт код правила."""
    if getattr(args, "json", False):
        _print_json(
            {
                "ok": False,
                "error": "lint",
                "blocking": exc.blocking,
                "warnings": exc.warnings,
            }
        )
        return
    for message in exc.blocking:
        sys.stderr.write(f"{message}\n")
    for message in exc.warnings:
        sys.stderr.write(f"{_FALLBACK_CODE}: предупреждение: {message}\n")


def _emit_error(args: argparse.Namespace, code: str, message: str) -> None:
    """Печатает ошибку как ``CODE: message`` (stderr) или JSON (stdout)."""
    if getattr(args, "json", False):
        _print_json({"ok": False, "error": code, "message": message})
        return
    sys.stderr.write(f"{code}: {message}\n")


# ---------------------------------------------------------------------------
# Команды
# ---------------------------------------------------------------------------


def cmd_resolve(args: argparse.Namespace) -> int:
    """``resolve``: детерминированный резолв ``source_id`` → URL (I10/I15).

    Резолв идёт по операционной БД прогона (``core.store.sqlite.SqliteStore``), без сети.
    Используются оба метода хранилища: :meth:`resolve` (метаданные: hash/build) и
    :meth:`resolve_source_url` (URL). Промах — не исключение, а код возврата
    :data:`EXIT_NOT_FOUND` с кодом ``I10/I15``.
    """
    resolved = None
    url = None
    with build_store(args.db, profile=args.profile) as store:
        resolved = store.resolve(args.source_id)
        url = store.resolve_source_url(args.source_id)

    if resolved is None or url is None:
        message = f"source_id не резолвится: {args.source_id}"
        if args.json:
            _print_json(
                {
                    "ok": False,
                    "found": False,
                    "source_id": args.source_id,
                    "error": "I10/I15",
                    "message": message,
                }
            )
        else:
            sys.stderr.write(f"I10/I15: {message}\n")
        return EXIT_NOT_FOUND

    payload = {
        "ok": True,
        "found": True,
        "source_id": args.source_id,
        "source_url": url,
        "content_hash": resolved.content_hash,
        "build_id": resolved.build_id,
        "chunk_id": resolved.chunk_id,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(f"{url}\n")
    return EXIT_OK


def cmd_plan(args: argparse.Namespace) -> int:
    """``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

    План чисто вычислим и **ничего не пишет**; флаг ``--dry-run`` фиксирует это явно
    (запись черновика ``run.yaml``/``cohort.yaml`` — будущий шаг W10/T10.4). Блокирующий
    линт прерывает построение плана с кодом правила.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    if not report.ok:
        raise LintFailure(report.blocking, report.warnings)

    result = build_plan(inputs, run_id=args.run_id)
    payload = _plan_payload(result)
    payload["dry_run"] = bool(args.dry_run)
    if args.json:
        _print_json({"ok": True, **payload})
    else:
        _print_plan(payload)
    return EXIT_OK


def cmd_run(args: argparse.Namespace) -> int:
    """``run`` (минимальный): создать прогон и провести ``Runner`` по доступным фазам.

    Реальный конвейер Ш1–Ш9 появится в W7–W11 (T10.4); сейчас фазы — заглушки, а гейты
    G1–G4 останавливают прогон без человеческого решения. Resume honour'ится: строка
    прогона **не пересоздаётся**, если она уже есть, поэтому durable-карта фаз в
    ``runs.state`` сохраняется между процессами (``02`` §A.1).

    ``--dry-run`` показывает план фаз/линт без записи; ``--reset`` форсирует полный
    перезапуск в обход durable-состояния.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    run_id = args.run_id or derive_run_id(inputs)
    db_path = Path(args.db) if args.db is not None else run_db_path(args.runs_dir, run_id)

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
            "warnings": list(report.warnings),
        }
        if args.json:
            _print_json(payload)
        else:
            _print_run_dry(payload)
        return EXIT_OK

    if not report.ok:
        raise LintFailure(report.blocking, report.warnings)

    store = build_store(db_path, profile=args.profile)
    try:
        _ensure_run(store, run_id, inputs)
        runner = build_runner(store, profile=args.profile)
        if args.reset:
            runner.reset(run_id)
        run_report = runner.run(run_id, until=args.until)
    finally:
        store.close()

    payload = _run_report_payload(run_id, run_report, db_path)
    if args.json:
        _print_json(payload)
    else:
        _print_run_report(payload)
    if run_report.stopped_at is None:
        return EXIT_OK
    if run_report.status == "blocked":
        return EXIT_BLOCKED
    return EXIT_ERROR


# ---------------------------------------------------------------------------
# Помощники команд
# ---------------------------------------------------------------------------


def _phases_until(until: str | None) -> list[str]:
    """Список фаз от начала до ``until`` включительно (все — при ``None``)."""
    if until is None:
        return list(PHASES)
    return list(PHASES[: PHASES.index(until) + 1])


def _ensure_run(store: Any, run_id: str, inputs: RunInputs) -> None:
    """Создаёт строку прогона, если её ещё нет (сохраняет durable-состояние resume).

    ``SqliteStore.create_run`` использует ``INSERT OR REPLACE`` и обнулил бы ``runs.state``;
    поэтому вызов обёрнут проверкой существования строки — повторный ``run`` не теряет
    прогресс (P4/``02`` §A.1).
    """
    row = store.conn.execute(
        "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if row is None:
        store.create_run(run_id, inputs.run)


def _plan_payload(result: RunPlan) -> dict[str, Any]:
    """Сериализует :class:`RunPlan` в JSON-совместимый словарь."""
    return {
        "run_id": result.run_id,
        "case_id": result.cohort.case_id,
        "anchor": result.anchor,
        "products": list(result.products),
        "modules": list(result.modules),
        "roles": list(result.roles),
        "params": [row.param_id for row in result.params],
        "module_weights": dict(result.module_weights),
        "renormalized_weights": dict(result.renormalized_weights),
        "golden_count": len(result.golden),
        "warnings": list(result.warnings),
    }


def _run_report_payload(run_id: str, report: RunReport, db_path: Path) -> dict[str, Any]:
    """Сериализует :class:`RunReport` в JSON-совместимый словарь."""
    return {
        "ok": report.ok,
        "run_id": run_id,
        "status": report.status,
        "stopped_at": report.stopped_at,
        "db": str(db_path),
        "skipped": list(report.skipped),
        "errors": list(report.errors),
        "results": [
            {
                "phase": item.phase,
                "status": item.status,
                "errors": list(item.errors),
            }
            for item in report.results
        ],
    }


def _print_plan(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод плана."""
    weights = ", ".join(
        f"{module}={weight}" for module, weight in sorted(payload["renormalized_weights"].items())
    )
    lines = [
        f"run_id: {payload['run_id']}",
        f"case_id: {payload['case_id']}",
        f"anchor: {payload['anchor']}",
        f"products: {', '.join(payload['products'])}",
        f"modules: {', '.join(payload['modules'])}",
        f"params: {', '.join(payload['params'])}",
        f"roles: {', '.join(payload['roles'])}",
        f"weights: {weights or '—'}",
        f"golden: {payload['golden_count']}",
        f"dry-run: {'yes' if payload.get('dry_run') else 'no'}",
    ]
    for warning in payload.get("warnings", ()):
        lines.append(f"warning: {warning}")
    sys.stdout.write("\n".join(lines) + "\n")


def _print_run_dry(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``run --dry-run`` (план фаз без исполнения)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"db: {payload['db']}",
        f"until: {payload['until'] or '—'}",
        f"phases: {', '.join(payload['phases'])}",
        "dry-run: yes (без записи)",
    ]
    for warning in payload.get("warnings", ()):
        lines.append(f"warning: {warning}")
    sys.stdout.write("\n".join(lines) + "\n")


def _print_run_report(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``run`` (итог и точка остановки)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"db: {payload['db']}",
        f"status: {payload['status']}",
        f"stopped_at: {payload['stopped_at'] or '—'}",
    ]
    for item in payload["results"]:
        lines.append(f"phase {item['phase']}: {item['status']}")
        for error in item["errors"]:
            lines.append(f"  error: {error}")
    if payload["skipped"]:
        lines.append(f"skipped: {', '.join(payload['skipped'])}")
    if payload["stopped_at"] is not None:
        lines.append(
            "примечание: гейт требует явного человеческого решения (03 §E.2); "
            "продолжите после записи решения в gate_decisions"
        )
    sys.stdout.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Парсер и точка входа
# ---------------------------------------------------------------------------


def _add_common(parser: argparse.ArgumentParser) -> None:
    """Общие флаги команд: ``--json`` и ``--profile``."""
    parser.add_argument(
        "--json", action="store_true", help="машинночитаемый вывод (JSON) в stdout"
    )
    parser.add_argument(
        "--profile",
        choices=SUPPORTED_PROFILES,
        default=LOCAL_PROFILE,
        help="профиль развёртывания (07): local (дефолт) | split (W13, пока недоступен)",
    )


def build_parser() -> argparse.ArgumentParser:
    """Собирает argparse-парсер CLI (stdlib, без фреймворков, A1)."""
    parser = argparse.ArgumentParser(
        prog="hound",
        description=(
            "coHound — сравнительная техническая карта продуктов по публично "
            "доказуемой функциональности. Composition root Z0 (ручной DI, A1)."
        ),
    )
    sub = parser.add_subparsers(dest="command", metavar="КОМАНДА")

    # resolve ------------------------------------------------------------- #
    p_resolve = sub.add_parser(
        "resolve", help="резолв source_id → URL по БД прогона (I10/I15)"
    )
    p_resolve.add_argument("source_id", help="идентификатор источника (chunk_id/source_id)")
    p_resolve.add_argument(
        "--db", required=True, help="путь к БД прогона (runs/{run_id}/cohound.db)"
    )
    _add_common(p_resolve)
    p_resolve.set_defaults(handler=cmd_resolve)

    # plan ---------------------------------------------------------------- #
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
    p_plan.set_defaults(handler=cmd_plan)

    # run ----------------------------------------------------------------- #
    p_run = sub.add_parser(
        "run", help="минимальный прогон: создать прогон и провести Runner по фазам"
    )
    p_run.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    p_run.add_argument("--package", required=True, help="каталог пакета категории")
    p_run.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    p_run.add_argument(
        "--runs-dir", default="runs", help="каталог прогонов (runs/{run_id}/cohound.db)"
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
    _add_common(p_run)
    p_run.set_defaults(handler=cmd_run)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа CLI. Возвращает код выхода (``0`` — успех).

    ``argparse`` сам печатает ``--help`` и usage-ошибки; здесь его ``SystemExit``
    превращается в обычный код возврата, поэтому :func:`main` тестируем без процесса.

    Ошибки домена/линта печатаются как ``CODE: message`` (код инварианта/правила).
    Непредвиденные исключения не проглатываются traceback'ом: печатается сжатое
    сообщение и возвращается ненулевой код.
    """
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return _system_exit_code(exc)

    if getattr(args, "command", None) is None:
        parser.print_help()
        return EXIT_USAGE

    try:
        return int(args.handler(args))
    except LintFailure as exc:
        _emit_lint_failure(args, exc)
        return exc.exit_code
    except CliError as exc:
        _emit_error(args, exc.code, _concise(exc))
        return exc.exit_code
    except WiringError as exc:
        # Профиль/сборка: это конфигурационная ошибка (split до W13).
        _emit_error(args, "profile", _concise(exc))
        return EXIT_USAGE
    except ValueError as exc:  # включает pydantic.ValidationError (I-инварианты домена)
        _emit_error(args, extract_code(str(exc)), _concise(exc))
        return EXIT_USAGE
    except OSError as exc:
        _emit_error(args, _FALLBACK_CODE, _concise(exc))
        return EXIT_ERROR
    except Exception as exc:  # noqa: BLE001 — консолидированный ненулевой выход
        _emit_error(args, _FALLBACK_CODE, f"{type(exc).__name__}: {exc}")
        return EXIT_ERROR


def _system_exit_code(exc: SystemExit) -> int:
    """Нормализует код ``SystemExit`` argparse к целому (``--help`` → 0)."""
    if exc.code is None:
        return EXIT_OK
    if isinstance(exc.code, int):
        return exc.code
    return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover — точка входа модуля
    raise SystemExit(main())
