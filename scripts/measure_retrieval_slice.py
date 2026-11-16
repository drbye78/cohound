"""Диагностика среза: что стоит срез по чанкам против среза по документам (O13, план 26).

**Вопрос.** `document_ids_for` режет **чанки** до дедупа (``chunk_ids[:top_k]``), поэтому
в набор документов top-10 попадает меньше десяти документов: документ, стоящий 11-м по
чанкам, теряется, хотя по документам он был бы в первой десятке. Сколько `hit@10` это
стоит — вот что мерит прибор.

**Это диагностика, а не метрика.** Объявленный срез — K **чанков** (I20), и
`scripts/measure_retrieval_docs.py` считает именно по нему. Публиковать число при срезе по
документам как метрику значило бы подменить вопрос замера и потерять сравнимость с O14/O15:
«поиск нашёл документ 11-м чанком» и «поиск нашёл документ в первой десятке документов» —
разные утверждения. Поэтому прибор печатает три числа рядом, и ни одно из них не заменяет
опубликованное.

**Почему нужна политика `per_query`.** У `max`/`rrf` срез до `cutoff_k` делает само слияние
(`_fuse_query_results`), поэтому выдача короче K не бывает и «срез по документам» от «среза
по чанкам» не отличается **ни на одном корпусе**. Только `per_query` отдаёт union без
обрезки, и только на нём вопрос имеет смысл. Это же — причина, по которой `per_query` не
является политикой по `hit@K`: её префикс K тождествен `max`, а весь её смысл — в хвосте,
который объявленная метрика не читает.

**Три числа в строке** (для каждой глубины запроса):

``hit@K чанки``
    Объявленная метрика: документы первых K **чанков**. Это число публикует
    `measure_retrieval_docs.py`, и оно же — то, с чем сравнимы O14/O15.
``hit@K K док``
    Та же выдача, но срез по **K документам**: сколько теряется на срезе по чанкам.
``док в union``
    Верхняя граница: ожидаемый документ нашёлся где угодно в union (глубина запроса
    ограничивает и это). Показывает, сколько остаётся «за срезом» вообще.

**Что прибор НЕ отвечает.** Стало бы лучше, если бы потребитель резал по документам, —
вопрос методологии: `cutoff_K` объявлен в контракте (I20), и его смена меняет `run_id` и
несравнимость с опубликованными числами. Прибор даёт цену решения, а не решение.

**Подготовка корпуса и индекса — не своя копия, а тот же путь, что у метрики.** Записи
лексического индекса, карта ``chunk_id → doc_id``, гидртация чанка и чтение прогона берутся
из `scripts/measure_retrieval_docs.py` (импортом, как это делает
`scripts/prepare_answer_sets.py`). Иначе прибор мерил бы свою копию подготовки: две копии
расходятся молча, и разница попала бы в число.

Запуск::

    .venv/bin/python scripts/measure_retrieval_slice.py \\
      --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \\
      --golden packages/ru-pg-dbms/sources/retrieval_golden_docs.json \\
      --index-root ~/.cohound/stands/ru-pg-dbms/index_store \\
      --depths 10,20,40 --out-json /tmp/opencode/o13-slice.json

Нейтрален (§1.3): продукт и эталон — аргументы, а не константы.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Корень репозитория на sys.path: скрипт запускается как `python scripts/…py`, и тогда
# sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
# Каталог скриптов — тоже: подготовка корпуса импортируется из прибора метрики по имени
# модуля, а не копируется (две копии расходятся молча).
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))


@dataclass(frozen=True)
class SliceHits:
    """Три ответа на одну ячейку при одной глубине: что считается каким срезом."""

    by_chunks: bool
    by_documents: bool
    anywhere: bool


def slice_hits(
    chunk_ids: Sequence[str],
    chunk_doc: dict[str, str],
    expected: set[str],
    *,
    k: int,
) -> SliceHits:
    """Три вопроса к одной выдаче: срез по чанкам, срез по документам, весь union.

    :param chunk_ids: выдача поиска в порядке ранжирования (у ``per_query`` — без обрезки).
    :param chunk_doc: карта ``chunk_id → doc_id``; чанк без записи пропускается, как и в
        :func:`core.pipeline.retrieval_docs.document_ids_for` — иначе прибор считал бы по
        другому правилу, чем метрика, и его числа были бы несравнимы с ней.
    :param expected: документы эталона для ячейки (набор: у ячейки может быть несколько).
    :param k: объявленный срез (I20) — он же «K документов» во втором числе.
    """
    from core.pipeline.retrieval_docs import document_ids_for

    if not chunk_ids:
        # Пустая выдача — законный случай (ячейка без хитов), а не сбой: `document_ids_for`
        # отказывает на `top_k=0`, и без этой ветки прибор падал бы на пустой ячейке
        # вместо того, чтобы посчитать её промахом.
        return SliceHits(by_chunks=False, by_documents=False, anywhere=False)
    by_chunks = set(document_ids_for(chunk_ids, chunk_doc, top_k=k))
    all_docs = document_ids_for(chunk_ids, chunk_doc, top_k=len(chunk_ids))
    return SliceHits(
        by_chunks=bool(expected & by_chunks),
        by_documents=bool(expected & set(all_docs[:k])),
        anywhere=bool(expected & set(all_docs)),
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "цена среза по чанкам против среза по документам: диагностика к O13, "
            "не метрика (объявленный срез — K чанков, I20)"
        )
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--golden", required=True, help="retrieval_golden_docs.json")
    parser.add_argument("--index-root", required=True, help="корень index_store")
    parser.add_argument(
        "--depths",
        default="10,20,40",
        help="глубины запроса через запятую (K = объявленный срез, то есть без запаса)",
    )
    parser.add_argument("--profile", default="R0", help="профиль ретривала (нужен R0/R2)")
    parser.add_argument(
        "--cutoff-k", type=int, default=0, help="K эталона; 0 — из runs.retrieval_cutoff_K"
    )
    parser.add_argument("--deploy", default="config/deploy.yaml", help="deploy-конфиг")
    parser.add_argument("--out-json", metavar="PATH", help="машинный отчёт")
    return parser.parse_args(argv)


def _resolve_depths(raw: str) -> tuple[int, ...]:
    """Глубины из списка: положительные, без дублей, порядок как задан."""
    depths: list[int] = []
    for item in (part.strip() for part in raw.split(",")):
        if not item:
            continue
        try:
            value = int(item)
        except ValueError:
            raise SystemExit(f"slice: глубина {item!r} — не целое число") from None
        if value <= 0:
            raise SystemExit(f"slice: глубина обязана быть ≥ 1, получено {value}")
        if value not in depths:
            depths.append(value)
    if not depths:
        raise SystemExit("slice: --depths пуст")
    return tuple(depths)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"slice: БД не найдена: {db_path}", file=sys.stderr)
        return 2
    depths = _resolve_depths(args.depths)

    import measure_retrieval_docs as metrics

    from core.adapters.lexical_tantivy import TantivyLexicalIndex
    from core.config.deploy import load_deploy_config
    from core.pipeline.composition import read_composition
    from core.pipeline.evaluate import retrieve_for_cell
    from core.pipeline.retrieval_docs import load_doc_golden

    _case_id, _golden_run_id, entries = load_doc_golden(args.golden)
    conn = metrics._open_readonly(db_path)
    try:
        meta = metrics._read_run(conn, None)
        cutoff_k = args.cutoff_k or meta.cutoff_k or 10
        params = {p.param_id: p for p in metrics._load_params(db_path, meta.package_id, None)}
        chunk_doc = metrics._chunk_doc_map(conn)
        lookup = metrics._chunk_lookup(conn)

        deploy = load_deploy_config(args.deploy)
        index_root = Path(args.index_root).expanduser()
        view = read_composition(index_root, meta.package_id, meta.build_id)
        if view is None:
            print(
                f"slice: композиция {meta.build_id!r} не найдена под {index_root}",
                file=sys.stderr,
            )
            return 2
        lexical = TantivyLexicalIndex(
            profile=deploy.lexical.profile,
            hydrate=lambda ids: {
                chunk_id: chunk
                for chunk_id in ids
                if (chunk := lookup(chunk_id)) is not None
            },
            view=view,
        )
        started = time.perf_counter()
        # Записи идут потоком из того же соединения: список 253k чанков в память не берётся,
        # и соединение закрывается **после** индексации — гидртация читает из него.
        lexical.index(
            metrics._iter_lexical_records(conn, meta.build_id), build_id=meta.build_id
        )
        index_seconds = time.perf_counter() - started

        rows: list[dict[str, Any]] = []
        for depth in depths:
            totals = {"by_chunks": 0, "by_documents": 0, "anywhere": 0}
            for entry in entries:
                param = params.get(entry.param_id)
                if param is None:
                    raise SystemExit(
                        f"slice: ячейка {entry.cell}: param_id {entry.param_id!r} "
                        "отсутствует в каталоге прогона — запрос не рендерится"
                    )
                retrieval = retrieve_for_cell(
                    param,
                    product_id=entry.product_id,
                    profile=args.profile,
                    cutoff_k=cutoff_k,
                    lexical_index=lexical,
                    cross_query_fusion="per_query",
                    query_depth=depth,
                )
                hits = slice_hits(
                    [chunk.chunk_id for chunk in retrieval.chunks],
                    chunk_doc,
                    set(entry.expected_doc_ids),
                    k=cutoff_k,
                )
                totals["by_chunks"] += hits.by_chunks
                totals["by_documents"] += hits.by_documents
                totals["anywhere"] += hits.anywhere
            total = len(entries)
            rows.append(
                {
                    "depth": depth,
                    "cells": total,
                    "hit_at_k_by_chunks": totals["by_chunks"] / total,
                    "hit_at_k_by_documents": totals["by_documents"] / total,
                    "expected_document_in_union": totals["anywhere"] / total,
                }
            )
    finally:
        conn.close()

    payload = {
        "instrument": "measure_retrieval_slice",
        "measured_at": time.strftime("%Y-%m-%d"),
        "db": str(db_path),
        "package_id": meta.package_id,
        "index_build_id": meta.build_id,
        "schema_version": meta.schema_version,
        "profile": args.profile,
        "fusion": "per_query",
        "cutoff_k": cutoff_k,
        "cells": len(entries),
        "index_seconds": round(index_seconds, 1),
        "rows": rows,
        "note": (
            "диагностика, не метрика: объявленный срез — K чанков (I20), и число "
            "'hit_at_k_by_documents' не заменяет опубликованное"
        ),
    }
    if args.out_json:
        path = Path(args.out_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    print(
        f"прогон {meta.run_id} (схема файла {meta.schema_version}) · сборка "
        f"{meta.build_id} · ячеек {len(entries)} · K={cutoff_k} · профиль {args.profile} · "
        "политика per_query"
    )
    print(f"{'глубина':>8}{'hit@K чанки':>15}{'hit@K K док':>15}{'док в union':>14}")
    for row in rows:
        print(
            f"{row['depth']:>8}{row['hit_at_k_by_chunks']:>15.4f}"
            f"{row['hit_at_k_by_documents']:>15.4f}"
            f"{row['expected_document_in_union']:>14.4f}"
        )
    print(f"цена: лексический индекс {index_seconds:.1f} с")
    print(
        "Диагностика, а не метрика: объявленный срез — K чанков (I20); "
        "«K документов» — другой вопрос замера."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
