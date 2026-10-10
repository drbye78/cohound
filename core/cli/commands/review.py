"""``review``: очередь ревью — preliminary-контур + ``❓``-кандидаты (Ш7/G3).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _print_json,
    _print_review,
    _resolve_db_path,
    _review_item_payload,
)
from core.cli.errors import EXIT_OK
from core.cli.wiring import build_store
from core.pipeline import review_queue
from core.pipeline.review import ReviewItem


def run(args: argparse.Namespace) -> int:
    """``review``: очередь ревью — preliminary-контур + ``❓``-кандидаты (Ш7/G3, ``03`` §D.6).

    Резолвит хранилище как ``sensitivity`` (``--db`` приоритетнее ``--runs-dir`` +
    ``--run-id``; ``run_id`` выводится, если в БД ровно один прогон) и печатает
    :func:`core.pipeline.review_queue` — детерминированный (по ``product_id``/``param_id``)
    список ячеек, требующих человеческого внимания. Пустая очередь — валидный результат
    :data:`EXIT_OK` с явной пометкой «queue empty».
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    items: list[ReviewItem] = []
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        items = review_queue(store, run_id)

    payload = {
        "ok": True,
        "run_id": run_id,
        "count": len(items),
        "items": [_review_item_payload(item) for item in items],
    }
    if args.json:
        _print_json(payload)
    else:
        _print_review(payload)
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``review``."""
    p_review = sub.add_parser(
        "review", help="очередь ревью: preliminary-контур + ❓-кандидаты (Ш7/G3)"
    )
    review_source = p_review.add_mutually_exclusive_group(required=True)
    review_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    review_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_review.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    _add_common(p_review)
    p_review.set_defaults(handler=run)
