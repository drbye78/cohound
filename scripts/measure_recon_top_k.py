#!/usr/bin/env python3
"""Сколько кандидатов BM25 достаточно разведке: замер K замены линейного прохода (план 27 §8.1).

**Вопрос.** ``hound baseline recon`` раньше на каждый параметр каталога читал **все** чанки
корпуса версии и брал первый подходящий (порядок ``doc_id, chunk_no`` — порядок документа,
а не релевантность). Замена — лексический индекс (``Bm25Index`` за портом
``LexicalSearcherPort``) и ``top_k`` кандидатов на параметр. Сколько их нужно, чтобы разведка
находила **то же**, что линейный проход, — вопрос замера, а не вкуса: слишком малое ``K``
потеряет цитаты, слишком большое платит временем на каждый параметр.

**Что сравнивается.** Не «тот же чанк» (индекс ранжирует иначе — это и есть его смысл), а
**исход отбора**: находится ли дословная цитата (`select_quote`) среди первых ``K``
кандидатов. Факт ``present`` появляется ровно тогда, когда цитата нашлась, поэтому совпадение
исхода и есть то, что видит читатель фактов.

**Почему K=0 — это линейный проход.** Базовая линия считается тем же ``select_quote`` по тем
же чанкам в порядке корпуса: разница между строками отчёта — цена замены, а не цена правил.

**Границы.** Замер идёт по ``--max-chunks`` чанкам в порядке корпуса (по умолчанию — весь
корпус): срез — это срез, и на нём «цитата не нашлась» означает «не нашлась в срезе». Число
чанков, на котором сделан вывод, публикуется. Стемминг лексики и разбор ``parameter_terms``
режут текст по-разному, поэтому кандидат без терминов правила — нормальный исход, а не
дефект: именно его и покрывает запас ``K``.

Запуск::

    .venv/bin/python scripts/measure_recon_top_k.py \
      --db ~/.cohound/stands/ru-pg-dbms/runs/{run_id}/cohound.db \
      --catalog packages/ru-pg-dbms/catalog.csv \
      --ks 1,2,3,5,10,20,50 [--max-chunks 6000] [--out-json /tmp/recon_top_k.json]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from core.adapters.lexical_bm25 import Bm25Index, LexicalDoc
from core.config.run_config import load_catalog
from core.pipeline.reference_corpus import ChunkText, parameter_terms, select_quote

#: Значения ``K``, которые печатаются по умолчанию: от «самого дешёвого» до «с запасом».
DEFAULT_KS = (1, 2, 3, 5, 10, 20, 50)


def read_chunks(db_path: Path, max_chunks: int) -> list[ChunkText]:
    """Чанки корпуса **одним проходом** (порядок ``doc_id, chunk_no`` — как у линейного).

    Один проход, а не запрос на документ: индекса по ``corpus_chunks.doc_id`` в схеме нет,
    и ``WHERE doc_id=?`` на каждый документ — полный скан (замер 17.11.2026).
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        sql = "SELECT chunk_id, doc_id, url, text FROM corpus_chunks ORDER BY doc_id, chunk_no, chunk_id"
        params: tuple[Any, ...] = ()
        if max_chunks > 0:
            sql += " LIMIT ?"
            params = (max_chunks,)
        return [
            ChunkText(
                chunk_id=str(chunk_id),
                doc_id=str(doc_id),
                url=str(url) if url else None,
                text=str(text or ""),
            )
            for chunk_id, doc_id, url, text in conn.execute(sql, params)
        ]
    finally:
        conn.close()


def measure(
    chunks: list[ChunkText], catalog: list[Any], ks: tuple[int, ...]
) -> dict[str, Any]:
    """Исход отбора цитаты для линейного прохода и для каждого ``K``."""
    index = Bm25Index()
    index.index(
        [LexicalDoc(chunk_id=c.chunk_id, product_id="p", text=c.text) for c in chunks]
    )
    by_id = {c.chunk_id: c for c in chunks}
    stats: dict[int, dict[str, int]] = {k: {"found": 0, "same_chunk": 0} for k in ks}
    linear_found = 0
    no_hit_at_all = 0
    params = 0
    for param in catalog:
        terms = parameter_terms(param)
        if not terms:
            continue
        params += 1
        linear: str | None = None
        for chunk in chunks:
            if select_quote(chunk.text, terms) is not None:
                linear = chunk.chunk_id
                break
        if linear is not None:
            linear_found += 1
        hits = index.search_scores(" ".join(terms), top_k=max(ks))
        if not hits:
            no_hit_at_all += 1
        for k in ks:
            picked: str | None = None
            for chunk_id, _score in hits[:k]:
                if select_quote(by_id[chunk_id].text, terms) is not None:
                    picked = chunk_id
                    break
            if picked is not None:
                stats[k]["found"] += 1
                if picked == linear:
                    stats[k]["same_chunk"] += 1
    return {
        "chunks": len(chunks),
        "params": params,
        "engine": index.engine_version,
        "linear_found": linear_found,
        "no_term_hit": no_hit_at_all,
        "by_k": {str(k): stats[k] for k in ks},
    }


def _parse_ks(raw: str) -> tuple[int, ...]:
    """Список ``K`` из ``1,2,5`` (положительные целые, без повторов, по возрастанию)."""
    values = sorted({int(part) for part in raw.split(",") if part.strip()})
    if not values or values[0] < 1:
        raise argparse.ArgumentTypeError(f"ожидались положительные целые, получено {raw!r}")
    return tuple(values)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True, type=Path, help="БД корпуса (run или reference)")
    parser.add_argument("--catalog", required=True, type=Path, help="catalog.csv профиля")
    parser.add_argument(
        "--ks",
        type=_parse_ks,
        default=DEFAULT_KS,
        help="значения K через запятую (по умолчанию 1,2,3,5,10,20,50)",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        default=0,
        help="срез корпуса в порядке doc_id, chunk_no (0 — весь корпус)",
    )
    parser.add_argument("--out-json", type=Path, default=None, help="куда писать результат")
    args = parser.parse_args(argv)

    chunks = read_chunks(args.db, args.max_chunks)
    if not chunks:
        print(f"чанков нет: {args.db}", file=sys.stderr)
        return 1
    result = measure(chunks, load_catalog(args.catalog), tuple(args.ks))
    result["db"] = str(args.db)
    result["catalog"] = str(args.catalog)

    print(f"срез: {result['chunks']} чанков; параметров: {result['params']}; движок {result['engine']}")
    print(f"линейный проход нашёл цитату: {result['linear_found']}")
    print(f"ни одного совпадения термина: {result['no_term_hit']}")
    for key, stats in result["by_k"].items():
        print(
            f"K={key:<3} цитата найдена: {stats['found']:<4} "
            f"тот же чанк: {stats['same_chunk']}"
        )
    if args.out_json is not None:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
