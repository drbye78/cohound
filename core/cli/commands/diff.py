"""``diff``: межцикловый diff ``run_a`` → ``run_b`` (§9/§13).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse
import sys
from pathlib import Path

from core.cli.context import (
    _add_common,
    _concise,
    _emit_error,
    _print_json,
    _read_cycle,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, extract_code
from core.pipeline.report import (
    CrossCycleDiffError,
    render_diff,
    resolve_diff_change_class,
)


def _doc_delta(prev_docs: Any, curr_docs: Any) -> dict[str, dict[str, int]]:
    """Per-product doc-delta двух прогонов по наборам ``content_hash`` (09 §12.5).

    ``new = curr − prev``, ``removed = prev − curr``, ``reused = prev ∩ curr``.
    Детерминирован (продукты сортируются); отсутствующий ``content_hash`` заменяется
    ``doc_id`` (контент-addressed ``{product}:{hash}``) — не выдумываем совпадение.

    Ограничение раскладки: ``corpus_docs`` не несёт ``run_id``, поэтому в общей ``--db``
    таблица — объединение корпусов и delta вырождается в ``reused``; в штатной
    раскладке (``runs/{run_id}/cohound.db``) выборка — корпус прогона.
    """

    def _by_product(docs: Any) -> dict[str, set[str]]:
        grouped: dict[str, set[str]] = {}
        for doc in docs or ():
            product = str(doc.product_id)
            digest = str(doc.content_hash or doc.doc_id)
            grouped.setdefault(product, set()).add(digest)
        return grouped

    previous = _by_product(prev_docs)
    current = _by_product(curr_docs)
    delta: dict[str, dict[str, int]] = {}
    for product in sorted(set(previous) | set(current)):
        prev_set = previous.get(product, set())
        curr_set = current.get(product, set())
        delta[product] = {
            "new": len(curr_set - prev_set),
            "reused": len(prev_set & curr_set),
            "removed": len(prev_set - curr_set),
        }
    return delta


def _products(cells: Any) -> set[str] | None:
    """Набор ``product_id`` цикла из ячеек; ``None`` — данных нет (не выдумываем)."""
    products = {str(cell.product_id) for cell in cells or ()}
    return products or None


def run(args: argparse.Namespace) -> int:
    """``diff``: межцикловый diff ``run_a`` → ``run_b`` (§9/§13).

    Резолв хранилищ (контракт выбора):

    * ``--runs-dir`` (дефолт ``runs``) + два позиционных ``run_id`` →
      ``runs/{run_id}/cohound.db`` через :func:`core.cli.wiring.run_db_path`;
    * единый явный ``--db`` — когда оба цикла лежат в **одной** БД; он переопределяет
      ``--runs-dir``.

    Оба цикла читаются как реальные prev/curr (без заглушки): ``read_manifest`` +
    ``iter_cells`` + ``iter_corpus_docs``. :func:`core.pipeline.report.render_diff`
    пишет ``diff.md``; при нарушении границы §13 (major ``methodology_version``,
    методологическое поле либо расхождение ``execution_contract_digest``) —
    :class:`CrossCycleDiffError` → :data:`EXIT_ERROR` с причиной ``§13: ...``.

    **Сравнимость (09 §12.5).** Граница — ``execution_contract_digest``, а не
    ``index_build_id``: новая композиция при тех же условиях измерения даёт
    ``content_change`` — разрешённый diff с баннером и per-product doc-delta
    (``new``/``reused``/``removed``, считается здесь по ``corpus_docs`` обоих прогонов).
    Изменение состава когорты называется ``cohort_expansion``/``cohort_reduction``.
    Legacy-пара без дайджеста сохраняет прежнее поведение (включая отказ по
    ``index_build_id``).

    Дефолт ``--out`` — ``runs/{run_b}/outputs/diff.md`` (родительские каталоги создаются).
    """
    out_path = (
        Path(args.out)
        if args.out is not None
        else Path(args.runs_dir) / args.run_b / "outputs" / "diff.md"
    )
    prev_manifest, prev_cells, prev_docs = _read_cycle(args, args.run_a)
    curr_manifest, curr_cells, curr_docs = _read_cycle(args, args.run_b)
    prev_products = _products(prev_cells)
    curr_products = _products(curr_cells)
    doc_delta = _doc_delta(prev_docs, curr_docs)
    try:
        written = render_diff(
            prev_manifest,
            curr_manifest,
            prev_cells,
            curr_cells,
            out_path,
            doc_delta=doc_delta,
            prev_products=prev_products,
            curr_products=curr_products,
        )
    except CrossCycleDiffError as exc:
        message = _concise(exc)
        code = extract_code(message)
        prefix = f"{code}:"
        if message.startswith(prefix):
            message = message[len(prefix) :].strip()
        _emit_error(args, code, message)
        return EXIT_ERROR

    change_class = resolve_diff_change_class(
        prev_manifest,
        curr_manifest,
        prev_products=prev_products,
        curr_products=curr_products,
    )
    payload = {
        "ok": True,
        "out": str(written),
        "run_a": args.run_a,
        "run_b": args.run_b,
        "change_class": change_class.value,
        "doc_delta": doc_delta,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(f"{written}\n")
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``diff``."""
    p_diff = sub.add_parser(
        "diff",
        help="межцикловый diff run_a → run_b (запрещён через major/границу, §13)",
    )
    p_diff.add_argument("run_a", help="предыдущий прогон (run_id)")
    p_diff.add_argument("run_b", help="текущий прогон (run_id)")
    p_diff.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов: runs/{run_id}/cohound.db (дефолт — раскладка: "
        "config/paths.yaml/COHOUND_RUNS_DIR/COHOUND_HOME, иначе runs); --db переопределяет",
    )
    p_diff.add_argument(
        "--db",
        default=None,
        help="один общий cohound.db для обоих прогонов (оба run_id в одной БД)",
    )
    p_diff.add_argument(
        "--out",
        default=None,
        help="путь diff.md (дефолт runs/{run_b}/outputs/diff.md)",
    )
    _add_common(p_diff)
    p_diff.set_defaults(handler=run)
