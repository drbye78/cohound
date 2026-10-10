"""``promote``: человеческое повышение ``preliminary`` → ``evaluated`` (I6/I7/I14).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
from typing import Any

from core.cli.context import (
    _PROMOTE_STATUSES,
    _add_common,
    _concise,
    _emit_error,
    _infer_run_id,
    _print_json,
    _print_promote,
    _resolve_db_path,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK
from core.cli.wiring import build_store
from core.domain.enums import Status
from core.pipeline import promote_batch, review_queue
from core.pipeline.review import ReviewError


def run(args: argparse.Namespace) -> int:
    """``promote``: человеческое повышение ``preliminary`` → ``evaluated`` (I6/I7/I14).

    Резолвит хранилище как ``review``, берёт :func:`core.pipeline.review_queue` и
    повышает :func:`core.pipeline.promote_batch`:

    * ``--param`` задан — повышаются только пары ``(product_id, param_id)`` очереди с
      указанными ``param_id``;
    * иначе — **все** элементы очереди.

    ``--actor`` обязателен и непуст (I6/I7). Вердикт переаттестации — явный ``--status``;
    **без** ``--status`` он берётся из ``cell.status``. При ``--validated-only`` (дефолт)
    отсутствие вердикта у ``preliminary``-ячейки → отказ ``I14`` (вердикт **не
    выдумывается**); ``--no-validated-only`` допускает ``evaluated`` без вердикта (первый
    шаг цикла §1.4). Отказы попадают в ``skipped`` с причиной; если не повышено **ни
    одной** ячейки — :data:`EXIT_ERROR`.

    :returns: :data:`EXIT_OK`, если повышена хотя бы одна ячейка; иначе :data:`EXIT_ERROR`.
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    target_status = Status(args.status) if args.status else None
    wanted_params = set(args.param or [])
    result: PromotionResult | None = None
    target_count = 0
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        queue = review_queue(store, run_id)
        if wanted_params:
            targets = [
                (item.product_id, item.param_id)
                for item in queue
                if item.param_id in wanted_params
            ]
        else:
            targets = [(item.product_id, item.param_id) for item in queue]
        target_count = len(targets)
        statuses = (
            {pair: target_status for pair in targets}
            if target_status is not None
            else {}
        )
        try:
            result = promote_batch(
                store=store,
                run_id=run_id,
                items=targets,
                actor=args.actor,
                notes=args.notes,
                validated_only=args.validated_only,
                statuses=statuses,
            )
        except ReviewError as exc:
            _emit_error(args, exc.code or "I14", _concise(exc))
            return EXIT_ERROR

    assert result is not None  # promote_batch выполняется безусловно (см. выше)
    payload: dict[str, Any] = {
        "ok": bool(result.promoted),
        "run_id": run_id,
        "actor": args.actor,
        "status": target_status.value if target_status is not None else None,
        "validated_only": args.validated_only,
        "requested": target_count,
        "promoted": len(result.promoted),
        "promoted_cells": [
            {
                "product_id": cell.product_id,
                "param_id": cell.param_id,
                "status": cell.status.value if cell.status is not None else None,
            }
            for cell in result.promoted
        ],
        "skipped": [
            {"product_id": product_id, "param_id": param_id, "reason": reason}
            for product_id, param_id, reason in result.skipped
        ],
        "errors": list(result.errors),
    }
    if args.json:
        _print_json(payload)
    else:
        _print_promote(payload)
    return EXIT_OK if result.promoted else EXIT_ERROR


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``promote``."""
    p_promote = sub.add_parser(
        "promote",
        help="человеческое повышение preliminary → evaluated (I6/I7/I14)",
    )
    promote_source = p_promote.add_mutually_exclusive_group(required=True)
    promote_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    promote_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_promote.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_promote.add_argument(
        "--actor",
        required=True,
        help="человек, выполняющий промоушен (I6/I7; обязателен и непуст)",
    )
    p_promote.add_argument(
        "--param",
        action="append",
        default=None,
        metavar="PARAM_ID",
        help="ограничить повышение этими param_id (можно повторять; иначе — вся очередь)",
    )
    p_promote.add_argument(
        "--status",
        choices=list(_PROMOTE_STATUSES),
        default=None,
        help="явный вердикт переаттестации; без него берётся из ячейки (а при "
        "--validated-only отсутствие вердикта → отказ I14)",
    )
    p_promote.add_argument(
        "--validated-only",
        dest="validated_only",
        action="store_true",
        default=True,
        help="дефолт: без валидированного вердикта повышение запрещено (I14)",
    )
    p_promote.add_argument(
        "--no-validated-only",
        dest="validated_only",
        action="store_false",
        help="разрешить evaluated без вердикта (первый шаг цикла §1.4, set-status позднее)",
    )
    p_promote.add_argument(
        "--notes", default=None, help="заметка к решению (аудит-след)"
    )
    _add_common(p_promote)
    p_promote.set_defaults(handler=run)
