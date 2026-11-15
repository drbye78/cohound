"""Замер O16: идентификаторы в лексике — профиль ``stemmed`` против ``identifier``.

Вопрос плана 26 §O16: анализатор ``stemmed`` режет ``pg_stat_statements`` на термы, и
профиль ``identifier`` (``whitespace`` без lowercase и стемминга) существует, но в
прогоне не вызывается. Нужно **число**, а не мнение: если ``stemmed`` проигрывает,
маршрутизация идентификаторных запросов в ``identifier`` становится объявленной осью.

Как меряется. Оба профиля строятся на **одних и тех же** чанках одной сборки; запрос
один и тот же; различается только анализатор. Эталон — не разметка человека, а
**буквальное вхождение** идентификатора в текст чанка (объявленный критерий: чанк, где
строки нет, не может быть свидетельством про этот идентификатор). Отсюда:

* ``P@K`` — доля выдачи, где идентификатор есть буквально (точность);
* ``hit@K`` — есть ли в выдаче хотя бы один такой чанк (нашлось ли вообще);
* ``первый`` — ранг первого буквального попадания (``—`` если его нет).

**Markdown-экранирование нормализуется.** Документация в корпусе пишет
``pg\\_basebackup`` (underscore экранирован, иначе markdown его съест): замер
15.11.2026 показал, что сырое «буквальное вхождение» находит такую форму **не** как
идентификатор, и `pg_basebackup` выходил промахом при 930 чанках с экранированной
формой против 810 с чистой. Поэтому обе стороны сравнения (текст чанка и запрос)
приводятся к одной форме: ``lower()`` + снятие ``\\`` перед ``_``. Число чанков-эталона
в таблице — уже после нормализации.

Ограничение названо и не прячется: буквальное вхождение — необходимое, но не
достаточное условие релевантности (чанк с идентификатором в списке литературы не
отвечает на вопрос). Поэтому сравнение профилей честно, а абсолютные числа — нет.
Второе ограничение метрики: ``identifier`` — термовый запрос без стемминга, чанк без
точного токена в выдачу не попадает **по построению**, поэтому ``P@K = 1.00`` у него
почти тавтологична, пока чанков с токеном ≥ K. Сравнение опирается на ``hit@K`` и на
ранг первого попадания, а не на ``P@K``.

Использование::

    python scripts/measure_lexical_identifiers.py --db runs/{run_id}/cohound.db
    python scripts/measure_lexical_identifiers.py --db … --json-out /tmp/o16.json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Идентификаторы-запросы: составные (с ``_``/``-``), потому что вопрос именно о них.
#: Односложные (``pg_dump``) добавлены как контроль: там стеммер почти безвреден, и
#: профиль обязан выигрывать не «вообще», а на составных.
DEFAULT_QUERIES: tuple[str, ...] = (
    "pg_stat_statements",
    "pg_stat_activity",
    "pg_stat_progress_vacuum",
    "pg_stat_user_tables",
    "max_wal_size",
    "min_wal_size",
    "autovacuum_naptime",
    "default_transaction_isolation",
    "synchronous_commit",
    "shared_buffers",
    "pg_basebackup",
    "pg_upgrade",
    "pg_dump",
    "pg_profile",
    "wal_level",
)

#: Профили в порядке вывода. ``identifier`` — псевдоним ``raw``, отдельно не меряется.
PROFILES: tuple[str, ...] = ("stemmed", "identifier")


def _normalized(text: str) -> str:
    """Одна форма для обеих сторон сравнения: нижний регистр + снятое ``\\_``.

    Экранирование значимо только для критерия: индекс видит ``pg\\_basebackup`` как
    терм ``pg\\_basebackup`` (whitespace) или как ``pg``+``basebackup`` (simple), но
    буквальная подстрока ``pg_basebackup`` в таком чанке отсутствует, и без нормализации
    чанк, целиком посвящённый идентификатору, считался бы промахом.
    """
    return text.lower().replace("\\_", "_")


@dataclass
class ProfileRun:
    """Итог одного профиля: по строке на запрос плюс агрегат."""

    profile: str
    build_seconds: float
    rows: list[dict[str, Any]]

    def mean(self, field: str) -> float:
        values = [row[field] for row in self.rows if row[field] is not None]
        return sum(values) / len(values) if values else 0.0


def _chunks(db: Path) -> Iterator[dict[str, str]]:
    """Чанки сборки потоком: текст не материализуется списком (A1)."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        cursor = con.execute(
            "select chunk_id, product_id, text, heading_path, content_hash "
            "from corpus_chunks"
        )
        for row in cursor:
            yield {
                "chunk_id": str(row["chunk_id"]),
                "product_id": str(row["product_id"]),
                "text": str(row["text"] or ""),
                "heading_path": str(row["heading_path"] or ""),
                "content_hash": str(row["content_hash"] or ""),
            }
    finally:
        con.close()


def _products(db: Path) -> list[str]:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return [
            str(row[0])
            for row in con.execute(
                "select distinct product_id from corpus_chunks order by 1"
            )
        ]
    finally:
        con.close()


def _hydrate(db: Path) -> Any:
    """Провайдер гидртации: ``chunk_id → запись``, читается по поисковому окну.

    Индекс тела не хранит (A1/Д-3), поэтому без гидртации ``search`` отказывает, а не
    отдаёт пустой текст. Здесь достаточно вернуть идентификатор и текст: метрика
    считается по тексту, провенанс цитаты не нужен.
    """
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)

    def hydrate(chunk_ids: Sequence[str]) -> dict[str, dict[str, str]]:
        ids = list(chunk_ids)
        if not ids:
            return {}
        # Подстановка — только знаки ``?``; значения идут параметрами, поэтому
        # конкатенации пользовательских данных в SQL здесь нет.
        marks = ",".join("?" * len(ids))
        rows = con.execute(
            "select chunk_id, product_id, text, heading_path, content_hash "  # noqa: S608
            f"from corpus_chunks where chunk_id in ({marks})",
            ids,
        )
        return {
            str(row[0]): {
                "chunk_id": str(row[0]),
                "product_id": str(row[1]),
                "text": str(row[2] or ""),
                "heading_path": str(row[3] or ""),
                "content_hash": str(row[4] or ""),
            }
            for row in rows
        }

    return hydrate


def _truths(db: Path, queries: Sequence[str]) -> dict[str, int]:
    """Сколько чанков содержит идентификатор буквально — одним проходом по корпусу.

    Нормализация — ``_normalized``; проход один, потому что сравнение идёт по тексту,
    а не по индексу (``instr`` в SQL не умеет снимать экранирование).
    """
    needles = {query: _normalized(query) for query in queries}
    counts = dict.fromkeys(queries, 0)
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        for (text,) in con.execute("select text from corpus_chunks"):
            normalized = _normalized(str(text or ""))
            for query, needle in needles.items():
                if needle in normalized:
                    counts[query] += 1
    finally:
        con.close()
    return counts


def _run_profile(
    db: Path,
    profile: str,
    queries: Sequence[str],
    truths: dict[str, int],
    *,
    top_k: int,
    products: Sequence[str],
) -> ProfileRun:
    from core.adapters.lexical_tantivy import LexicalChunk, TantivyLexicalIndex

    tmp = tempfile.TemporaryDirectory(prefix=f"o16-{profile}-")
    try:
        index = TantivyLexicalIndex(
            Path(tmp.name) / "index", profile=profile, hydrate=_hydrate(db)
        )
        started = time.perf_counter()
        indexed = index.index(
            LexicalChunk(
                chunk_id=chunk["chunk_id"],
                product_id=chunk["product_id"],
                text=chunk["text"],
                heading_path=chunk["heading_path"],
                content_hash=chunk["content_hash"],
            )
            for chunk in _chunks(db)
        )
        build_seconds = time.perf_counter() - started
        print(
            f"[{profile}] индекс: {indexed} чанков за {build_seconds:.1f} с",
            file=sys.stderr,
        )
        rows: list[dict[str, Any]] = []
        for query in queries:
            needle = _normalized(query)
            hits = index.search(query, list(products), top_k)
            texts = [_normalized(str(getattr(hit, "text", "") or "")) for hit in hits]
            flags = [needle in text for text in texts]
            first = next((i + 1 for i, flag in enumerate(flags) if flag), None)
            rows.append(
                {
                    "query": query,
                    "truth": truths[query],
                    "returned": len(hits),
                    "exact": sum(flags),
                    "precision": (sum(flags) / len(flags)) if flags else 0.0,
                    "hit": 1 if any(flags) else 0,
                    "first": first,
                }
            )
        return ProfileRun(profile=profile, build_seconds=build_seconds, rows=rows)
    finally:
        tmp.cleanup()


def _print_table(runs: Sequence[ProfileRun], top_k: int) -> None:
    print(f"\nточность выдачи top-{top_k} по профилям (эталон — буквальное вхождение)")
    head = f"| {'запрос':30} | {'чанков':>7} |"
    for run in runs:
        head += f" {run.profile + ' P@K':>14} | {run.profile + ' hit':>13} |"
    print(head)
    print(
        "|"
        + "-" * 32
        + "|"
        + "-" * 9
        + "|"
        + ("-" * 16 + "|" + "-" * 15 + "|") * len(runs)
    )
    for i, row in enumerate(runs[0].rows):
        line = f"| {row['query']:30} | {row['truth']:7} |"
        for run in runs:
            other = run.rows[i]
            first = f" (#{other['first']})" if other["first"] else ""
            line += f" {other['precision']:14.2f} | {other['hit']:>10}{first:>3} |"
        print(line)
    print(
        "|"
        + "-" * 32
        + "|"
        + "-" * 9
        + "|"
        + ("-" * 16 + "|" + "-" * 15 + "|") * len(runs)
    )
    line = f"| {'СРЕДНЕЕ':30} | {'':7} |"
    for run in runs:
        line += f" {run.mean('precision'):14.2f} | {run.mean('hit'):>13.2f} |"
    print(line)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="cohound.db прогона")
    parser.add_argument(
        "--queries",
        default=None,
        help="файл со списком идентификаторов (по одному в строке); иначе набор по умолчанию",
    )
    parser.add_argument("--top-k", type=int, default=10, dest="top_k")
    parser.add_argument("--json-out", default=None, dest="json_out")
    args = parser.parse_args()

    db = Path(args.db)
    if not db.is_file():
        print(f"нет файла БД: {db}", file=sys.stderr)
        return 2
    queries = (
        tuple(
            line.strip()
            for line in Path(args.queries).read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        )
        if args.queries
        else DEFAULT_QUERIES
    )
    products = _products(db)
    print(f"БД: {db}\nпродуктов: {len(products)} ({', '.join(products)})")
    print(f"запросов: {len(queries)}, top_k={args.top_k}")
    print("эталон: чанков с идентификатором (нормализация `\\_` → `_`)", file=sys.stderr)
    truths = _truths(db, queries)
    print(
        "  " + ", ".join(f"{query}={truths[query]}" for query in queries),
        file=sys.stderr,
    )

    runs = [
        _run_profile(db, profile, queries, truths, top_k=args.top_k, products=products)
        for profile in PROFILES
    ]
    _print_table(runs, args.top_k)

    payload = {
        "db": str(db),
        "top_k": args.top_k,
        "products": products,
        "criterion": "буквальное вхождение идентификатора в текст чанка (нормализация `\\_` → `_`)",
        "runs": [
            {
                "profile": run.profile,
                "build_seconds": round(run.build_seconds, 2),
                "mean_precision": round(run.mean("precision"), 4),
                "mean_hit": round(run.mean("hit"), 4),
                "rows": run.rows,
            }
            for run in runs
        ],
    }
    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"\nJSON: {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
