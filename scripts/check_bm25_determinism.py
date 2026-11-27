"""Прибор: воспроизводится ли балл BM25 между двумя сборками индекса (O41 плана 26).

**Что измеряется.** Два независимых процесса строят лексический индекс по одному и тому же
корпусу и отвечают на одни и те же запросы. Прибор пишет **отпечаток выдачи** — по каждой
ячейке эталона пары «балл (``repr``, полная точность) + ``chunk_id``» — и сравнивает два
отпечатка.

**Почему это дефект, а не косметика.** ``prompt_hash`` ячейки считается по ``chunk_ids``,
поэтому расхождение выдачи означает, что два прогона с одним ``run_id`` отдают оценщику
**разные чанки** при неизменных объявленных входах. Прибор нужен, чтобы цена исправления
(``--single-segment``) и остаточное расхождение были числами, а не мнением.

**Почему два процесса, а не два индекса в одном.** Причина расхождения — раскладка на
сегменты, которую tantivy выбирает по бюджету памяти writer'а (128 MB по умолчанию) и по
числу потоков хоста. Внутри одного процесса оба индекса получают одинаковую раскладку чаще,
чем в разных, и дефект прячется. Отпечаток поэтому пишется в файл и сравнивается отдельным
запуском.

**Режимы.**

``--mode default``
    Индекс собирается так, как его собирает прогон: раскладка сегментов выбирается
    библиотекой.
``--mode single``
    Writer объявляет бюджет (``--heap-mb``) и **один поток** — то, что делает рабочий код
    (``WRITER_THREADS``). Ожидаемый исход — воспроизводимый балл.
``--mode threads``
    Writer получает ``num_threads=0`` («взять с хоста») — **контр-опыт**: так дефект
    воспроизводим и после исправления, иначе «до» проверить нечем.
``--compare A B``
    Сравнивает два отпечатка: по каждой ячейке — совпал ли **состав** top-K, **порядок** и
    **баллы**, плюс максимальное ``|Δ балла|``. Выход 1 при любом расхождении (гейт после
    исправления), 0 — при полном совпадении.

Нейтрален (§1.3): ни продуктов, ни домена — пакет и эталон приходят аргументами.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any


def _open_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _iter_records(conn: sqlite3.Connection, build_id: str) -> Iterator[Any]:
    """Записи индекса потоком: список 253k чанков в память не берётся (A1)."""
    from core.adapters.lexical_tantivy import LexicalChunk

    for row in conn.execute(
        "SELECT chunk_id, product_id, text, heading_path, content_hash FROM corpus_chunks"
    ):
        yield LexicalChunk(
            chunk_id=str(row["chunk_id"]),
            product_id=str(row["product_id"]),
            text=str(row["text"] or ""),
            heading_path=str(row["heading_path"] or ""),
            content_hash=str(row["content_hash"] or ""),
            build_id=build_id,
        )


def _chunk_lookup(conn: sqlite3.Connection) -> Any:
    """Гидртация top-K: точка-чтение чанка по ``chunk_id``."""
    from core.ports.types import Chunk
    from core.store.corpus_reader import _CHUNK_FIELDS
    from core.store.sqlite_rows import _row_to_chunk

    # Проекция берётся из единственного объявления, а не пишется здесь руками: рукописный
    # SELECT отстал от схемы, когда у чанка появился `table_ref` (3.26.0), и прибор падал
    # на `IndexError` вместо ответа (найдено 27.11.2026).
    projection = ", ".join(_CHUNK_FIELDS)

    def lookup(chunk_id: str) -> Chunk | None:
        row = conn.execute(
            f"SELECT {projection} FROM corpus_chunks WHERE chunk_id = ?",  # noqa: S608 - проекция из объявления, chunk_id — bind
            (chunk_id,),
        ).fetchone()
        return None if row is None else _row_to_chunk(row)

    return lookup


class _DeclaredWriter:
    """Прокси индекса: ``writer()`` объявляет бюджет и число потоков, остальное — делегат.

    Наследоваться от ``tantivy.Index`` нельзя (Rust-тип закрыт для подклассов), поэтому
    подменяется **имя**, которое адаптер вызывает: ``tantivy.Index(...)`` становится
    фабрикой. Прокси нужен ровно для одного — воспроизвести раскладку, которую прогон
    получил бы при объявленных значениях.
    """

    def __init__(self, index: Any, heap_bytes: int, threads: int) -> None:
        self._index = index
        self._heap = heap_bytes
        self._threads = threads

    def writer(self, heap_size: int | None = None, num_threads: int | None = None) -> Any:
        """Объявленные значения сильнее переданных: цель прокси — назвать раскладку.

        Иначе рабочий код (он передаёт ``num_threads=WRITER_THREADS``) перебивал бы
        контрольный режим, и дефект стал бы невоспроизводим после исправления.
        """
        del heap_size, num_threads
        return self._index.writer(self._heap, self._threads)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._index, name)


def _declare_writer(heap_mb: int, threads: int) -> None:
    """Подменяет ``tantivy.Index`` фабрикой с объявленным writer'ом (см. ``_DeclaredWriter``)."""
    import tantivy

    real_index = tantivy.Index

    def factory(*args: Any, **kwargs: Any) -> _DeclaredWriter:
        return _DeclaredWriter(real_index(*args, **kwargs), heap_mb * 1024 * 1024, threads)

    tantivy.Index = factory  # type: ignore[misc]


def _fingerprint(
    *,
    db: Path,
    golden: Path,
    index_root: Path,
    package: str,
    build_id: str,
    profile: str,
    lexical_profile: str,
    cutoff_k: int,
    cells: int,
    mode: str,
    heap_mb: int,
) -> dict[str, Any]:
    """Строит индекс и пишет отпечаток выдачи по первым ``cells`` ячейкам эталона."""
    from core.adapters.lexical_tantivy import TantivyLexicalIndex
    from core.domain.models import CatalogRow
    from core.pipeline.evaluate import retrieve_for_cell
    from core.pipeline.retrieval_docs import load_doc_golden

    _case, _run, entries = load_doc_golden(golden)
    if cells > 0:
        entries = entries[:cells]

    snapshot = db.parent / "plan_inputs.json"
    if not snapshot.is_file():
        raise SystemExit(f"проба: нет снимка входов прогона: {snapshot}")
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    params = {
        str(item["param_id"]): CatalogRow.model_validate(item)
        for item in payload.get("params", [])
    }

    conn = _open_readonly(db)
    chunks_conn = _open_readonly(db)
    try:
        lookup = _chunk_lookup(chunks_conn)
        index = TantivyLexicalIndex(
            profile=lexical_profile,
            hydrate=lambda ids: {
                cid: chunk for cid in ids if (chunk := lookup(cid)) is not None
            },
        )
        started = time.perf_counter()
        written = index.index(_iter_records(conn, build_id), build_id=build_id)
        seconds = time.perf_counter() - started
        segments = int(index._searcher.num_segments)  # noqa: SLF001 — предмет измерения

        rows: list[dict[str, Any]] = []
        for entry in entries:
            param = params.get(entry.param_id)
            if param is None:
                raise SystemExit(
                    f"проба: ячейка {entry.cell}: param_id {entry.param_id!r} отсутствует "
                    "в снимке каталога прогона"
                )
            result = retrieve_for_cell(
                param,
                product_id=entry.product_id,
                profile=profile,
                cutoff_k=cutoff_k,
                lexical_index=index,
            )
            rows.append(
                {
                    "cell": entry.cell,
                    "hits": [[repr(chunk.score), chunk.chunk_id] for chunk in result.chunks],
                }
            )
    finally:
        chunks_conn.close()
        conn.close()

    digest = hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return {
        "mode": mode,
        "profile": profile,
        "cutoff_k": cutoff_k,
        "docs": written,
        "segments": segments,
        "seconds": round(seconds, 2),
        "heap_mb": heap_mb if mode == "single" else None,
        "cells": len(rows),
        "fingerprint": digest,
        "rows": rows,
    }


def _compare(left: dict[str, Any], right: dict[str, Any]) -> int:
    """Сравнивает два отпечатка; 1 — расхождение (гейт), 0 — совпадение."""
    a = {row["cell"]: row["hits"] for row in left["rows"]}
    b = {row["cell"]: row["hits"] for row in right["rows"]}
    common = sorted(set(a) & set(b))
    set_diff = [c for c in common if {x[1] for x in a[c]} != {x[1] for x in b[c]}]
    order_diff = [c for c in common if [x[1] for x in a[c]] != [x[1] for x in b[c]]]
    score_diff = [c for c in common if [x[0] for x in a[c]] != [x[0] for x in b[c]]]
    max_delta = 0.0
    for cell in common:
        left_scores = {cid: float(score) for score, cid in a[cell]}
        for score, cid in b[cell]:
            if cid in left_scores:
                max_delta = max(max_delta, abs(float(score) - left_scores[cid]))
    print(
        f"сравнение: ячеек {len(common)}; иной состав {len(set_diff)}, "
        f"иной порядок {len(order_diff)}, иные баллы {len(score_diff)}; "
        f"max |Δ балла| = {max_delta:.3e}"
    )
    print(
        f"сегментов: {left['segments']} против {right['segments']}; "
        f"сборка {left['seconds']} с против {right['seconds']} с"
    )
    for cell in set_diff:
        print(f"  состав: {cell}")
    return 1 if (set_diff or order_diff or score_diff) else 0


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="воспроизводимость балла BM25 между сборками индекса (O27/O41)"
    )
    parser.add_argument("--compare", nargs=2, metavar=("A", "B"), help="два отпечатка")
    parser.add_argument("--db", help="cohound.db прогона (только чтение)")
    parser.add_argument("--golden", help="retrieval_golden_docs.json")
    parser.add_argument("--index-root", default="index_store")
    parser.add_argument("--package", default=None)
    parser.add_argument("--build-id", default=None, help="сборка (по умолчанию из runs)")
    parser.add_argument(
        "--retrieval-profile", default="R0", help="профиль ретривала (R0/R1/R2)"
    )
    parser.add_argument(
        "--lexical-profile",
        default="stemmed",
        help="профиль анализатора BM25 (stemmed|identifier|ngram)",
    )
    parser.add_argument("--cutoff-k", type=int, default=10)
    parser.add_argument("--cells", type=int, default=0, help="первые N ячеек (0 — все)")
    parser.add_argument("--mode", choices=("default", "single", "threads"), default="default")
    parser.add_argument("--heap-mb", type=int, default=1024, help="бюджет writer'а (single)")
    parser.add_argument("--out", help="куда записать отпечаток (JSON)")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.compare:
        left = json.loads(Path(args.compare[0]).read_text(encoding="utf-8"))
        right = json.loads(Path(args.compare[1]).read_text(encoding="utf-8"))
        return _compare(left, right)
    if not args.db or not args.golden:
        print("проба: нужны --db и --golden (либо --compare A B)", file=sys.stderr)
        return 2

    db = Path(args.db).expanduser()
    if not db.is_file():
        print(f"проба: БД не найдена: {db}", file=sys.stderr)
        return 2

    build_id = args.build_id
    if build_id is None:
        conn = _open_readonly(db)
        try:
            row = conn.execute(
                "SELECT index_build_id FROM runs LIMIT 2"
            ).fetchall()
        finally:
            conn.close()
        if len(row) != 1 or not row[0]["index_build_id"]:
            print(
                "проба: в файле не один прогон или не записан index_build_id — "
                "назовите --build-id",
                file=sys.stderr,
            )
            return 2
        build_id = str(row[0]["index_build_id"])

    if args.mode == "single":
        _declare_writer(args.heap_mb, 1)
    elif args.mode == "threads":
        _declare_writer(args.heap_mb, 0)

    payload = _fingerprint(
        db=db,
        golden=Path(args.golden).expanduser(),
        index_root=Path(args.index_root).expanduser(),
        package=args.package or "",
        build_id=build_id,
        profile=args.retrieval_profile,
        lexical_profile=args.lexical_profile,
        cutoff_k=args.cutoff_k,
        cells=args.cells,
        mode=args.mode,
        heap_mb=args.heap_mb,
    )
    print(
        f"режим {payload['mode']}: документов {payload['docs']}, сегментов "
        f"{payload['segments']}, сборка {payload['seconds']} с, ячеек {payload['cells']}"
    )
    print(f"отпечаток: {payload['fingerprint']}")
    if args.out:
        out = Path(args.out).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
