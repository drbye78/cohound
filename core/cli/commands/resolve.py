"""``resolve``: детерминированный резолв ``source_id`` → URL (I10/I15).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse
import sys

from core.cli.context import _add_common, _print_json
from core.cli.errors import EXIT_NOT_FOUND, EXIT_OK
from core.cli.wiring import build_store


def run(args: argparse.Namespace) -> int:
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


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``resolve``."""
    p_resolve = sub.add_parser(
        "resolve", help="резолв source_id → URL по БД прогона (I10/I15)"
    )
    p_resolve.add_argument("source_id", help="идентификатор источника (chunk_id/source_id)")
    p_resolve.add_argument(
        "--db", required=True, help="путь к БД прогона (runs/{run_id}/cohound.db)"
    )
    _add_common(p_resolve)
    p_resolve.set_defaults(handler=run)
