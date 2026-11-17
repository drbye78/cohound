#!/usr/bin/env python3
"""Эквивалентность набора кусков при смене версии чанкера (план ``26``, O39 фаза 2).

**Вопрос, на который отвечает прибор.** ``policy-6`` объявляет новую версию правил нарезки,
и это меняет ``run_id`` (``chunker_version`` ∈ ``EXECUTION_CONTRACT_FIELDS``) и
``partition_build_id``. Меняется ли при этом **сам набор кусков** — ``text``,
``content_hash``, смещения, ``heading_path``, ``context_prefix``?

**Почему это измеримо без пере-экстракции и без переиндексации.** В run-DB чанк — точный
срез текста документа (``text == document.text[char_start:char_end]``, P1 у обоих штатных
чанкеров), а смещения покрывают документ непрерывно. Значит текст документа восстанавливается
конкатенацией кусков по ``chunk_no``, и нарезку можно повторить **тем же** чанкером текущего
дерева. Прибор проверяет непрерывность прежде, чем что-либо сравнивать: документ с дырой или
перекрытием в смещениях пропускается и называется числом, а не сравнивается «как получится».

**Семейство чанкера восстанавливается, а не угадывается.** Прод выбирает реализацию по
``ExtractedDoc.family`` (``core/adapters/chunker_family.py``), а в run-DB этого поля нет.
Правило прибора повторяет единственный производитель ``family`` для текстового читателя
(``core/adapters/extract_text.py``: ``textual_family(url, content_type) or FAMILY_TEXT``):
документ, извлечённый ``text-plain-*``, идёт абзацным чанкером, если его URL не выглядит
markdown-ом. Документы с markdown-суффиксом при ``text-plain-*`` прибор **называет
отдельно** (``markdown_suffix_in_text_reader``), потому что там правило зависит от
``Content-Type``, которого в БД нет, — догадка вместо факта недопустима.

**Что прибор НЕ делает.** Он не строит индекс и не считает retrieval-метрики: выдача — предмет
отдельного замера, и она зависит от ``text``/``chunk_id``, а не от версии чанкера
(``chunk_id`` = ``product:doc_version:chunk_no``, ``chunker_version`` в него не входит —
``core/pipeline/corpus_index.py``).

Запуск::

    .venv/bin/python scripts/measure_chunker_equivalence.py \
        --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \
        --out-json /tmp/opencode/o39/equivalence.json

Код возврата: 0 — расхождений нет, 1 — есть (с числом), 2 — прибор не смог прочитать вход.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.adapters.chunker_markdown import MarkdownChunker  # noqa: E402
from core.adapters.chunker_text import TextChunker  # noqa: E402
from core.util.document_formats import (  # noqa: E402
    FAMILY_MARKDOWN,
    FAMILY_TEXT,
    textual_family,
)
from core.util.hashing import content_hash  # noqa: E402

#: Бюджет куска. Дефолт чанкера (``MarkdownChunker(chunk_size=1500)``); прогон 14.11.2026
#: объявленного ``chunk_size`` в ``run.yaml`` не имел, то есть шёл на дефолте. Прибор держит
#: его **константой**, а не флагом: ось, которую меряют, не должна меняться от опечатки.
CHUNK_SIZE = 1500

#: Профиль грамматики: в рантайме всегда ``gfm`` (``18-markdown-chunker-plan.md`` §5).
PROFILE = "gfm"

#: Префикс ``extractor_version`` текстового читателя (``core/adapters/extract_text.py``).
TEXT_EXTRACTOR_PREFIX = "text-plain"

#: Сколько примеров расхождений держать в отчёте (остальное — числом).
EXAMPLES_LIMIT = 5


@dataclass
class DocOutcome:
    """Исход одного документа: чем кончилось и сколько кусков сравнено."""

    doc_id: str
    product_id: str
    extractor_version: str
    family: str
    chunks_old: int = 0
    chunks_new: int = 0
    compared: int = 0
    mismatches: Counter = field(default_factory=Counter)
    examples: list[dict[str, Any]] = field(default_factory=list)
    skipped: str | None = None
    seconds: float = 0.0


def _connect(db_path: Path) -> sqlite3.Connection:
    """Открывает run-DB **только на чтение**: прибор не имеет права менять прогон."""
    if not db_path.is_file():
        raise FileNotFoundError(f"run-DB не найден: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _family_for(extractor_version: str, url: str) -> tuple[str, str | None]:
    """Семейство чанкера для документа: ``(family, подозрение)``.

    Повторяет единственный производитель ``family`` у текстового читателя
    (``extract_text.py``: ``textual_family(url, content_type) or FAMILY_TEXT``). Для
    документных читателей (docling, HTML, EPUB) семейство всегда ``markdown``.

    ``textual_family`` зависит и от суффикса, и от ``Content-Type``; ``Content-Type`` в
    run-DB не хранится. Поэтому markdown-суффикс при текстовом читателе возвращается
    вторым значением как **подозрение**: правило там не восстановимо из БД, и молча
    выбрать сторону значило бы выдать догадку за факт.
    """
    if not extractor_version.startswith(TEXT_EXTRACTOR_PREFIX):
        return FAMILY_MARKDOWN, None
    by_url = textual_family(url, None)
    if by_url == FAMILY_MARKDOWN:
        # Без ``Content-Type`` нельзя отличить «.md, прочитанный текстовым читателем» от
        # «.md, отданного как text/plain». Чанкер выбирается по family, поэтому сторона
        # здесь — предположение; документ называется отдельно, а не сравнивается молча.
        return FAMILY_MARKDOWN, "markdown_suffix_in_text_reader"
    return FAMILY_TEXT, None


def _restore_text(rows: list[sqlite3.Row]) -> tuple[str | None, str | None]:
    """Текст документа из кусков: ``(text, причина отказа)``.

    Куски — точные срезы, смещения покрывают документ без дыр (это проверяется, а не
    предполагается). Любая дыра, перекрытие или несовпадение суммы длин с концом документа
    означает, что конкатенация даст **не тот** текст, и сравнивать было бы нечего.
    """
    if not rows:
        return None, "no_chunks"
    parts: list[str] = []
    position = 0
    for row in rows:
        start, end = row["char_start"], row["char_end"]
        if start is None or end is None:
            return None, "no_offsets"
        if start != position:
            return None, "gap_or_overlap"
        if len(row["text"]) != end - start:
            return None, "length_mismatch"
        parts.append(row["text"])
        position = end
    return "".join(parts), None


def _compare(
    doc: sqlite3.Row,
    old: list[sqlite3.Row],
    new_chunks: list[Any],
) -> DocOutcome:
    """Сравнивает старый набор кусков с перечанкованным по ``chunk_no``."""
    family, suspicion = _family_for(doc["extractor_version"], doc["url"])
    outcome = DocOutcome(
        doc_id=doc["doc_id"],
        product_id=doc["product_id"],
        extractor_version=doc["extractor_version"],
        family=family,
    )
    outcome.chunks_old = len(old)
    outcome.chunks_new = len(new_chunks)
    if suspicion:
        outcome.mismatches[suspicion] += 1
    if len(old) != len(new_chunks):
        outcome.mismatches["chunk_count"] += 1
        if len(outcome.examples) < EXAMPLES_LIMIT:
            outcome.examples.append(
                {
                    "kind": "chunk_count",
                    "old": len(old),
                    "new": len(new_chunks),
                }
            )
    # Сравниваются только совпавшие позиции: при разном числе кусков «тот же chunk_no» уже
    # не тот же кусок, и выдавать совпадение текста за совпадение набора нельзя.
    for index, (before, after) in enumerate(zip(old, new_chunks, strict=False)):
        outcome.compared += 1
        for field_name, old_value, new_value in (
            ("text", before["text"], after.text),
            # Хеш сравнивается **авторитетный** (``core.util.hashing.content_hash`` от текста,
            # 64 hex), а не ``Chunk.content_hash`` чанкера: последний — короткий
            # ``_sha256_16`` (16 hex) внутреннего пространства, и прод пересчитывает хеш сам
            # (``corpus_ingest._accept_chunk_candidates``). Сравнение двух разных пространств
            # дало бы «расхождение» на каждом куске — именно это и было поймано первым
            # прогоном (1702 из 1702).
            ("content_hash", before["content_hash"], content_hash(after.text)),
            ("char_start", before["char_start"], after.char_start),
            ("char_end", before["char_end"], after.char_end),
            ("heading_path", before["heading_path"], after.heading_path),
            ("context_prefix", before["context_prefix"], after.context_prefix),
        ):
            if old_value == new_value:
                continue
            outcome.mismatches[field_name] += 1
            if len(outcome.examples) < EXAMPLES_LIMIT:
                outcome.examples.append(
                    {
                        "kind": field_name,
                        "chunk_no": index,
                        "old": (
                            old_value[:120] + "…"
                            if isinstance(old_value, str) and len(old_value) > 120
                            else old_value
                        ),
                        "new": (
                            new_value[:120] + "…"
                            if isinstance(new_value, str) and len(new_value) > 120
                            else new_value
                        ),
                    }
                )
    return outcome


def _chunk_document(
    text: str, family: str, document_id: str
) -> list[Any]:
    """Режет документ **тем же** чанкером, что выбрал бы прод для этого семейства."""
    if family == FAMILY_TEXT:
        return TextChunker(chunk_size=CHUNK_SIZE).chunk_text(document_id, text)
    return MarkdownChunker(chunk_size=CHUNK_SIZE, profile=PROFILE).chunk_text(
        document_id, text
    )


def _iter_documents(
    conn: sqlite3.Connection, limit: int
) -> list[sqlite3.Row]:
    """Документы прогона (порядок — стабильный, по ``doc_id``).

    **Без ``EXISTS`` по ``corpus_chunks``**: индекса по ``doc_id`` в run-DB нет, и такой
    подзапрос превращается в полный скан на каждый документ (19 324 × 253 306 строк —
    прибор мерил бы отсутствие индекса). Наличие кусков проверяется по факту: документ без
    кусков просто не встретится в потоке ``_chunks_by_document`` и попадёт в отчёт как
    пропуск, а не исчезнет молча.
    """
    sql = """
        SELECT doc_id, product_id, url, extractor_version
        FROM corpus_docs
        ORDER BY doc_id
    """
    rows = conn.execute(sql).fetchall()
    return rows[:limit] if limit else rows


#: Один проход по ``corpus_chunks`` вместо запроса на документ.
#:
#: В run-DB **нет индекса по ``doc_id``** (есть ``idx_chunks_product``/``idx_chunks_build``),
#: поэтому запрос «чанки одного документа» — полный скан 253 306 строк. При 19 324
#: документах это 4.9 млрд прочитанных строк: прибор мерил бы не чанкер, а отсутствие
#: индекса (измерено: 40 документов не уложились в 120 с). Порядок ``doc_id, chunk_no``
#: позволяет группировать потоково и держать в памяти один документ.
_CHUNKS_SQL = """
    SELECT doc_id, chunk_no, text, content_hash, char_start, char_end,
           heading_path, context_prefix
    FROM corpus_chunks
    ORDER BY doc_id, chunk_no
"""


def _chunks_by_document(
    conn: sqlite3.Connection, keep: set[str]
) -> Iterator[tuple[str, list[sqlite3.Row]]]:
    """Потоково отдаёт ``(doc_id, чанки по порядку)`` — один скан таблицы."""
    current: str | None = None
    bucket: list[sqlite3.Row] = []
    for row in conn.execute(_CHUNKS_SQL):
        doc_id = row["doc_id"]
        if doc_id != current:
            if current is not None and (not keep or current in keep):
                yield current, bucket
            current = doc_id
            bucket = []
        bucket.append(row)
    if current is not None and (not keep or current in keep):
        yield current, bucket


def run(db_path: Path, limit: int = 0, progress_every: int = 2000) -> dict[str, Any]:
    """Прогоняет сравнение по всем документам и возвращает отчёт."""
    conn = _connect(db_path)
    try:
        documents = _iter_documents(conn, limit)
        by_id = {row["doc_id"]: row for row in documents}
        keep = set(by_id) if limit else set()
        outcomes: list[DocOutcome] = []
        started = time.perf_counter()
        for number, (doc_id, old) in enumerate(
            _chunks_by_document(conn, keep), start=1
        ):
            doc = by_id.get(doc_id)
            if doc is None:
                continue
            chunk_started = time.perf_counter()
            text, skip = _restore_text(old)
            if text is None:
                family, suspicion = _family_for(doc["extractor_version"], doc["url"])
                outcome = DocOutcome(
                    doc_id=doc["doc_id"],
                    product_id=doc["product_id"],
                    extractor_version=doc["extractor_version"],
                    family=family,
                    chunks_old=len(old),
                    skipped=skip,
                )
                if suspicion:
                    outcome.mismatches[suspicion] += 1
                outcomes.append(outcome)
                continue
            family, _ = _family_for(doc["extractor_version"], doc["url"])
            new_chunks = _chunk_document(text, family, doc["doc_id"])
            outcome = _compare(doc, old, new_chunks)
            outcome.seconds = round(time.perf_counter() - chunk_started, 3)
            outcomes.append(outcome)
            if progress_every and number % progress_every == 0:
                print(
                    f"  [{number}/{len(documents)}] документов,"
                    f" {time.perf_counter() - started:.0f} с",
                    file=sys.stderr,
                    flush=True,
                )
    finally:
        conn.close()
    return _summarize(outcomes, documents, db_path, time.perf_counter() - started)


def _summarize(
    outcomes: list[DocOutcome],
    documents: list[sqlite3.Row],
    db_path: Path,
    wall_seconds: float,
) -> dict[str, Any]:
    """Сводка: сколько документов/кусков сравнено и какие поля разошлись."""
    compared = [item for item in outcomes if item.skipped is None]
    skipped = [item for item in outcomes if item.skipped is not None]
    mismatch_totals: Counter = Counter()
    for item in compared:
        for kind, count in item.mismatches.items():
            mismatch_totals[kind] += count
    by_product: dict[str, dict[str, int]] = {}
    for item in outcomes:
        bucket = by_product.setdefault(
            item.product_id, {"documents": 0, "compared": 0, "mismatched": 0}
        )
        bucket["documents"] += 1
        bucket["compared"] += item.compared
        if any(item.mismatches.values()):
            bucket["mismatched"] += 1
    examples = [item.examples for item in compared if item.examples][:5]
    return {
        "instrument": "scripts/measure_chunker_equivalence.py",
        "conditions": {
            "db": str(db_path),
            "chunk_size": CHUNK_SIZE,
            "profile": PROFILE,
            "documents_in_run": len(documents),
            "family_rule": (
                "text-plain-* → text (если URL не markdown), иначе markdown;"
                " повторяет core/adapters/extract_text.py:family"
            ),
        },
        "summary": {
            "documents": len(outcomes),
            "compared": len(compared),
            "skipped": len(skipped),
            "skipped_reasons": dict(
                Counter(item.skipped for item in skipped if item.skipped)
            ),
            # Документ без кусков в поток не попадает вовсе, и без этого счётчика он
            # исчез бы из отчёта молча — а «не сравнивали» читается как «совпало».
            "documents_without_chunks": len(documents) - len(outcomes),
            "chunks_compared": sum(item.compared for item in compared),
            "chunks_old_total": sum(item.chunks_old for item in outcomes),
            "documents_with_mismatch": sum(
                1 for item in compared if any(item.mismatches.values())
            ),
            "mismatches": dict(mismatch_totals),
            "families": dict(Counter(item.family for item in outcomes)),
            "by_product": by_product,
            "wall_seconds": round(wall_seconds, 1),
        },
        "documents": [
            {
                "doc_id": item.doc_id,
                "product_id": item.product_id,
                "extractor_version": item.extractor_version,
                "family": item.family,
                "chunks_old": item.chunks_old,
                "chunks_new": item.chunks_new,
                "compared": item.compared,
                "mismatches": dict(item.mismatches),
                "skipped": item.skipped,
                "seconds": item.seconds,
            }
            for item in outcomes
            if item.skipped is not None or any(item.mismatches.values())
        ],
        "examples": examples,
    }


def _print_line(report: dict[str, Any]) -> None:
    """Строка оператора: числа, а не прилагательные."""
    summary = report["summary"]
    print(
        f"документов {summary['documents']}"
        f" (сравнено {summary['compared']}, пропущено {summary['skipped']}),"
        f" кусков сравнено {summary['chunks_compared']} из {summary['chunks_old_total']}"
    )
    if summary["mismatches"]:
        print(f"РАСХОЖДЕНИЯ: {summary['mismatches']}")
        print(f"документов с расхождением: {summary['documents_with_mismatch']}")
    else:
        print("расхождений нет: набор кусков совпал поле в поле")
    if summary["skipped_reasons"]:
        print(f"пропуски: {summary['skipped_reasons']}")
    if summary["documents_without_chunks"]:
        print(f"документов без кусков: {summary['documents_without_chunks']}")
    print(f"стена: {summary['wall_seconds']} с")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path, help="run-DB (mode=ro)")
    parser.add_argument(
        "--limit", type=int, default=0, help="документов (0 — все); для проб"
    )
    parser.add_argument("--out-json", type=Path, default=None, help="машинный отчёт")
    args = parser.parse_args(argv)

    try:
        report = run(args.db, limit=args.limit)
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 2

    _print_line(report)
    if args.out_json is not None:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(
            json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"отчёт: {args.out_json}")
    return 1 if report["summary"]["mismatches"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
