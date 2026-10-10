"""coHound — вывод CLI: JSON, ошибки, линт-фейлы (composition root Z0; ``03`` §E.2).

Инфраструктурный модуль вывода, вынесен из ``core.cli.main``, чтобы ``package_commands``
печатал через те же функции без импорта из ``main`` (разрыв циклической связки).
Поведение сохранено 1:1: ``--json`` → stdout (сортированные ключи), ``CODE: message`` →
stderr.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from core.cli.errors import FALLBACK_CODE, LintFailure

__all__ = [
    "concise",
    "print_json",
    "emit_lint_failure",
    "emit_error",
]


def concise(exc: BaseException) -> str:
    """Однострочное представление исключения (без простыней traceback)."""
    return " ".join(str(exc).split()) or type(exc).__name__


def print_json(payload: dict[str, Any]) -> None:
    """Печатает JSON-объект в stdout (машинночитаемый вывод, ``--json``)."""
    sys.stdout.write(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    )


def emit_lint_failure(args: argparse.Namespace, exc: LintFailure) -> None:
    """Печатает блокирующий линт: каждое сообщение уже несёт код правила."""
    if getattr(args, "json", False):
        print_json(
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
        sys.stderr.write(f"{FALLBACK_CODE}: предупреждение: {message}\n")


def emit_error(args: argparse.Namespace, code: str, message: str) -> None:
    """Печатает ошибку как ``CODE: message`` (stderr) или JSON (stdout)."""
    if getattr(args, "json", False):
        print_json({"ok": False, "error": code, "message": message})
        return
    sys.stderr.write(f"{code}: {message}\n")
