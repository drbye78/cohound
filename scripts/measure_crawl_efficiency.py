"""Сколько краул тратит на дубликаты и где он усечён: замер по прогонной БД.

Отвечает на три вопроса, которые нельзя решить чтением кода:

1. **Дубликаты URL.** ``Frontier._seen`` дедуплицирует по точной строке
   (``core/pipeline/crawl_frontier.py:1050``), поэтому ``/page`` и ``/page/`` — два
   элемента очереди. Замер показывает, какую долю ``crawl_session_items`` это даёт и
   во что обходится бюджет ``max_documents``.
2. **Форма дерева.** Гистограмма по ``depth`` и число уникальных документов на глубину:
   «потолок ниже измерения» виден как рост глубины за объявленный предел.
3. **Куда ушли отказы.** ``error_kind`` по продуктам: ``robots_disallow`` и
   ``redirect_out_of_scope`` — это не «сайт недоступен», а объявленный scope.

Запуск (БД читается **только на чтение**, прогон можно не останавливать):

    python3 scripts/measure_crawl_efficiency.py runs/{run_id}/cohound.db \
        [--out docs/reports/measurements/{date}-crawl-efficiency.jsonl]

Без ``--out`` печатает JSONL в stdout. Одна строка на продукт + итоговая строка
``scope=total``. Формат — «сырьё» к отчёту: числа не пересчитываются вручную.
"""

from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
import urllib.parse
from pathlib import Path

#: Продукты, которых нет в БД, не выдумываются: список берётся из самих данных.


def _ro(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{Path(path).expanduser()}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    return conn


def _slash_normalized(url: str) -> str:
    """URL без хвостового ``/`` в пути (query/fragment не трогаем)."""
    parts = urllib.parse.urlsplit(url)
    path = parts.path.rstrip("/")
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, path, parts.query, parts.fragment)
    )


def _items(conn: sqlite3.Connection, product_id: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT url, status, depth, error_kind, http_status, content_hash"
            " FROM crawl_session_items WHERE product_id = ?",
            (product_id,),
        )
    )


def measure_product(conn: sqlite3.Connection, product_id: str) -> dict[str, object]:
    rows = _items(conn, product_id)
    by_status = collections.Counter(str(r["status"]) for r in rows)
    by_error = collections.Counter(
        str(r["error_kind"]) for r in rows if r["status"] == "failed"
    )
    fetched = [r for r in rows if r["status"] == "fetched"]

    exact = {str(r["url"]) for r in fetched}
    normalized = collections.Counter(_slash_normalized(str(r["url"])) for r in fetched)
    dup_items = sum(v - 1 for v in normalized.values() if v > 1)
    dup_pairs = sum(1 for v in normalized.values() if v > 1)

    hashes = collections.Counter(str(r["content_hash"]) for r in fetched if r["content_hash"])
    hash_dup_items = sum(v - 1 for v in hashes.values() if v > 1)

    depth_hist = collections.Counter(int(r["depth"] or 0) for r in rows)
    depth_fetched = collections.Counter(int(r["depth"] or 0) for r in fetched)
    depth_unique_docs: dict[str, int] = {}
    for depth in sorted(depth_fetched):
        depth_unique_docs[str(depth)] = len(
            {
                _slash_normalized(str(r["url"]))
                for r in fetched
                if int(r["depth"] or 0) == depth
            }
        )

    docs = conn.execute(
        "SELECT COUNT(*) FROM corpus_docs WHERE product_id = ?", (product_id,)
    ).fetchone()[0]
    chunks = conn.execute(
        "SELECT COUNT(*) FROM corpus_chunks WHERE product_id = ?", (product_id,)
    ).fetchone()[0]
    # http_status: сколько элементов вообще получили ответ сервера в этом цикле
    status_hist = collections.Counter(
        str(r["http_status"]) for r in fetched if r["http_status"] is not None
    )

    return {
        "scope": "product",
        "product_id": product_id,
        "items_total": len(rows),
        "items_by_status": dict(by_status),
        "items_by_error_kind": dict(by_error),
        "fetched_items": len(fetched),
        "unique_urls_exact": len(exact),
        "unique_urls_slash_normalized": len(normalized),
        "slash_duplicate_items": dup_items,
        "slash_duplicate_pairs": dup_pairs,
        "slash_duplicate_share": round(dup_items / len(fetched), 4) if fetched else None,
        "unique_content_hashes": len(hashes),
        "content_hash_duplicate_items": hash_dup_items,
        "corpus_docs": docs,
        "corpus_chunks": chunks,
        "fetched_http_status": dict(sorted(status_hist.items())),
        "depth_items": {str(k): v for k, v in sorted(depth_hist.items())},
        "depth_fetched": {str(k): v for k, v in sorted(depth_fetched.items())},
        "depth_unique_urls": depth_unique_docs,
        "max_depth_seen": max(depth_hist) if depth_hist else None,
    }


def measure(conn: sqlite3.Connection) -> list[dict[str, object]]:
    products = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT product_id FROM crawl_session_items ORDER BY product_id"
        )
    ]
    out = [measure_product(conn, p) for p in products]
    totals: dict[str, object] = {
        "scope": "total",
        "products": products,
        "items_total": sum(int(r["items_total"]) for r in out),  # type: ignore[arg-type]
        "fetched_items": sum(int(r["fetched_items"]) for r in out),  # type: ignore[arg-type]
        "slash_duplicate_items": sum(
            int(r["slash_duplicate_items"]) for r in out  # type: ignore[arg-type]
        ),
        "corpus_docs": sum(int(r["corpus_docs"]) for r in out),  # type: ignore[arg-type]
        "corpus_chunks": sum(int(r["corpus_chunks"]) for r in out),  # type: ignore[arg-type]
    }
    out.append(totals)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db", help="runs/{run_id}/cohound.db")
    parser.add_argument("--out", default=None, help="JSONL-файл (по умолчанию stdout)")
    args = parser.parse_args(argv)

    conn = _ro(args.db)
    try:
        records = measure(conn)
    finally:
        conn.close()

    text = "\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in records)
    if args.out:
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
        print(f"{len(records)} записей → {path}", file=sys.stderr)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
