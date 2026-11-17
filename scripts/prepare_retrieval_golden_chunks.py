#!/usr/bin/env python3
"""Чанковый retrieval-эталон: пересборка из документного (остаток O14 плана 26).

**Зачем.** Прежний чанковый эталон (`sources/retrieval_golden.json`, 18 пар) привязан к
`chunk_id` сборки 16.10 и не резолвится в текущем корпусе ни одним id: формат id сменился
(`3736fae`, 29.10 — второй сегмент стал `doc_version_id` вида `dv_…`). `hound experiment
retrieval` на нём исполняется, но ожидания не существуют, и `recall@K = 0` неотличим от
провала поиска (O4). Прибор пересобирает чанковый эталон **из** документного
(`retrieval_golden_docs.json`, формат `cohound-retrieval-golden-docs/1`), чтобы у него
снова были живые адреса.

**Что утверждает запись.** «Для ячейки `{product_id}/{param_id}` чанк, из которого взята
цитата её доказательства, должен попасть в top-K». Это **вторичная** проверка: документный
эталон адресует документ (устойчив к смене чанкера), чанковый — единицу доказательства
(I19: `corpus_raw → extract → chunk → content_hash → index`), и он ломается при каждой
пере-нарезке. Поэтому он и пересобирается скриптом, а не поддерживается руками.

**Запрос — тот же, что у фазы Ш6.** Запрос строится не здесь: вызывается построитель фазы
(`core.pipeline.evaluate.run_queries_for_cell` → `core.pipeline.prompt.render_queries`,
`evaluate.py:1062`), а параметр берётся из снимка входов прогона (`plan_inputs.json` рядом с
run-DB — ровно те `CatalogRow`, что рендерила фаза). Свой шаблон запроса сделал бы эталон
про другой предмет: сравнимость с замером Ш6 держится на тождестве запроса, а не на
похожести.

**Ожидаемые чанки — из доказательства, а не из «всех чанков документа».** Источник —
`cell_evidence`: `citation_quote` + `doc_version_id`. Документ цитаты резолвится в
`doc_id` (`corpus_doc_version`), текст документа собирается из его чанков в порядке
`chunk_no` (чанк — точный срез текста, `char_start`/`char_end`), цитата ищется в
нормализованном тексте (пробельные серии → один пробел, `\\_` → `_` — экранированный
underscore документации, замер O16), найденный отрезок пересекается с отрезками чанков.
Цитата, пересекающая границу чанков, даёт **все** пересечённые чанки и считается отдельным
числом. Ненайденная цитата **отбрасывает запись с названной причиной** — пустого
`expected_ids` не бывает никогда (`load_retrieval_golden` такой файл отверг бы, и это
правильно: «поиск не нашёл» и «ожидание не существует» — разные факты).

**Записи.** Одна запись на пару (ячейка, запрос): фаза Ш6 задаёт до `MAX_QUERIES` запросов
и сливает их, а формат `load_retrieval_golden` выражает один запрос. Выбор «взять первый»
был бы выбором прибора, а не фазы, поэтому публикуются все отрендеренные запросы ячейки;
`query_index`/`queries_total` называют место записи, `cell` — ячейку. Ограничение названо:
recall по записям — это recall **каждого** запроса, а не слияния, и он не сравнивается
напрямую с `hit@K` документного эталона (там одна запись на ячейку).

**Почему скрипт, а не `core/`.** Подготовка входа — не оценка; формат уже принимается
пайплайном (`load_retrieval_golden`), поэтому здесь только чтение run-DB и запись JSON.
Схема run-DB читается **сырым `sqlite3` в `mode=ro`**: прогон стенда бывает старой схемы
(3.20.0 против кода 3.26.0), и `core.store` отверг бы файл гейтом версии — прибору нужны
две таблицы, а не весь DDL.

Подкоманды:

``build`` (по умолчанию)
    Собирает JSON-эталон из документного + run-DB; печатает сводку (``--json`` — машиной).

``validate``
    Проверяет готовый JSON: форма (``load_retrieval_golden`` без правок), непустые
    ``query``/``expected_ids``, резолв каждого id в указанной БД и его продукт-партицию
    (I17). Любой нерезолвленный id — отказ (код 1) с числом и примерами.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

# Корень репозитория на sys.path: скрипт запускается как `python scripts/…py`, и тогда
# sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.domain.models import CatalogRow  # noqa: E402 — после правки sys.path
from core.pipeline.evaluate import run_queries_for_cell  # noqa: E402
from core.pipeline.experiments import load_retrieval_golden  # noqa: E402
from core.pipeline.quote_text import normalize  # noqa: E402 — одна нормализация на два прибора
from core.pipeline.retrieval_docs import DocGoldenEntry, load_doc_golden  # noqa: E402

#: Путь документного эталона кейса по умолчанию (вход пересборки).
DEFAULT_DOC_GOLDEN = _ROOT / "packages/ru-pg-dbms/sources/retrieval_golden_docs.json"
#: Путь чанкового эталона кейса по умолчанию (выход пересборки).
DEFAULT_OUT = _ROOT / "packages/ru-pg-dbms/sources/retrieval_golden_chunks.json"

#: Причины отбрасывания записи. Машинные токены: их читает сводка и отчёт.
DROP_NO_EVIDENCE = "no_evidence"          # у ячейки нет доказательства с непустой цитатой
DROP_NO_ADDRESS = "quote_without_address"  # цитата есть, `doc_version_id` не записан (O5)
DROP_NO_DOC = "doc_not_in_corpus"         # документ цитаты не резолвится в корпусе прогона
DROP_FOREIGN_DOC = "foreign_doc"          # документ цитаты принадлежит другому продукту (I17)
DROP_NO_CHUNKS = "no_chunks"              # документ в корпусе есть, чанков у него нет
DROP_QUOTE_NOT_FOUND = "quote_not_found"  # цитата не найдена ни в одном чанке документа

#: Порядок причин при подсчёте «ячейка отброшена по причине X» (первая применимая).
_DROP_ORDER = (
    DROP_NO_EVIDENCE,
    DROP_NO_ADDRESS,
    DROP_NO_DOC,
    DROP_FOREIGN_DOC,
    DROP_NO_CHUNKS,
    DROP_QUOTE_NOT_FOUND,
)


@dataclass(frozen=True)
class EvidenceQuote:
    """Цитата доказательства ячейки и адрес её документа (O5: ``doc_version_id``)."""

    product_id: str
    param_id: str
    doc_version_id: str
    quote: str


@dataclass(frozen=True)
class ChunkSpan:
    """Чанк документа: адрес, порядок и отрезок в тексте документа (полуоткрытый)."""

    chunk_id: str
    chunk_no: int
    char_start: int
    char_end: int


@dataclass
class DocText:
    """Текст документа, собранный из чанков, и карта «смещение → чанк»."""

    spans: list[ChunkSpan]
    text: str
    #: Сколько чанков документа имели ``char_start``/``char_end``, не совпавшие с
    #: накопленной длиной текста (``0`` — срез согласован; иначе отрезки считаны по длине).
    span_mismatch: int = 0


@dataclass
class BuildSummary:
    """Сводка пересборки: числа, которые публикуются в отчёте и в ``--json``."""

    cells: int = 0
    records: int = 0
    cells_with_chunks: int = 0
    dropped_cells: dict[str, int] = field(default_factory=lambda: dict.fromkeys(_DROP_ORDER, 0))
    quotes_total: int = 0
    quotes_found: int = 0
    quotes_not_found: int = 0
    quotes_no_address: int = 0
    quotes_no_doc: int = 0
    quotes_no_chunks: int = 0
    quotes_foreign_doc: int = 0
    quotes_on_boundary: int = 0
    cells_on_boundary: int = 0
    cells_evidence_doc_is_expected: int = 0
    cells_evidence_doc_differs: int = 0
    chunks_per_cell: dict[str, float | int | None] = field(default_factory=dict)
    expected_ids_total: int = 0
    chunks_span_mismatch_docs: int = 0
    queries_total: int = 0

    def to_dict(self) -> dict[str, object]:
        """Машинная форма сводки (детерминированный порядок ключей — ``sort_keys``)."""
        return {
            "cells": self.cells,
            "records": self.records,
            "cells_with_chunks": self.cells_with_chunks,
            "dropped_cells": dict(self.dropped_cells),
            "quotes": {
                "total": self.quotes_total,
                "found": self.quotes_found,
                "not_found": self.quotes_not_found,
                "without_address": self.quotes_no_address,
                "doc_not_in_corpus": self.quotes_no_doc,
                "doc_without_chunks": self.quotes_no_chunks,
                "foreign_doc": self.quotes_foreign_doc,
                "on_boundary": self.quotes_on_boundary,
            },
            "cells_with_boundary_quote": self.cells_on_boundary,
            "cells_evidence_doc_is_expected": self.cells_evidence_doc_is_expected,
            "cells_evidence_doc_differs": self.cells_evidence_doc_differs,
            "chunks_per_cell": dict(self.chunks_per_cell),
            "expected_ids_total": self.expected_ids_total,
            "chunks_span_mismatch_docs": self.chunks_span_mismatch_docs,
            "queries_total": self.queries_total,
        }


# ---------------------------------------------------------------------------
# Чтение входов
# ---------------------------------------------------------------------------
def _connect_ro(db_path: Path) -> sqlite3.Connection:
    """Run-DB **только на чтение**: прибор не имеет права менять прогон."""
    if not db_path.is_file():
        raise SystemExit(f"build: run-DB не найден: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_params(inputs_path: Path, *, run_id: str) -> dict[str, CatalogRow]:
    """Каталог прогона из снимка входов — те же ``CatalogRow``, что рендерила фаза.

    Снимок (`plan_inputs.json` рядом с run-DB) — единственный носитель **того** каталога,
    который был входом прогона: `catalog.csv` в рабочем дереве мог измениться после него,
    и запрос, собранный по нему, был бы про другой предмет (сравнимость с Ш6 теряется).
    """
    if not inputs_path.is_file():
        raise SystemExit(
            f"build: нет снимка входов {inputs_path} — запросы обязаны быть теми же, "
            "что рендерила фаза (укажите --inputs)"
        )
    payload = json.loads(inputs_path.read_text(encoding="utf-8"))
    snapshot_run = payload.get("run_id")
    if snapshot_run and snapshot_run != run_id:
        raise SystemExit(
            f"build: {inputs_path} — снимок прогона {snapshot_run!r}, а БД принадлежит "
            f"{run_id!r}: каталог другого прогона дал бы другие запросы"
        )
    params = payload.get("params")
    if not isinstance(params, list) or not params:
        raise SystemExit(f"build: {inputs_path}: params пуст или не список")
    return {str(item["param_id"]): CatalogRow.model_validate(item) for item in params}


def read_evidence(conn: sqlite3.Connection) -> dict[tuple[str, str], list[EvidenceQuote]]:
    """Цитаты доказательств по ячейкам: ``(product, param) → [(doc_version, quote)]``.

    Берутся только строки с непустой цитатой: цитата — то, чем адресуется чанк (I22 для
    ``direct``), и без неё искать нечего (причина ``no_evidence`` видна в сводке).
    Строка с цитатой, но без ``doc_version_id`` (адрес версии не записан — O5) не
    пропускается молча: она попадёт в сводку отдельной причиной ``quote_without_address``,
    иначе «адреса нет» выглядело бы как «доказательства нет».
    """
    quotes: dict[tuple[str, str], list[EvidenceQuote]] = defaultdict(list)
    for row in conn.execute(
        "SELECT product_id, param_id, doc_version_id, citation_quote FROM cell_evidence"
        " WHERE citation_quote IS NOT NULL AND trim(citation_quote) <> ''"
    ):
        quotes[(row["product_id"], row["param_id"])].append(
            EvidenceQuote(
                product_id=row["product_id"],
                param_id=row["param_id"],
                doc_version_id=str(row["doc_version_id"] or ""),
                quote=str(row["citation_quote"]),
            )
        )
    return dict(quotes)


def read_version_map(
    conn: sqlite3.Connection, doc_version_ids: set[str]
) -> dict[str, tuple[str, str]]:
    """``doc_version_id → (doc_id, product_id)`` для запрошенных версий."""
    if not doc_version_ids:
        return {}
    mapping: dict[str, tuple[str, str]] = {}
    ordered = sorted(doc_version_ids)
    for start in range(0, len(ordered), 500):
        batch = ordered[start : start + 500]
        placeholders = ",".join("?" for _ in batch)
        sql = (
            "SELECT doc_version_id, doc_id, product_id FROM corpus_doc_version"  # noqa: S608 — только '?'-плейсхолдеры
            f" WHERE doc_version_id IN ({placeholders})"
        )
        for row in conn.execute(sql, batch):
            mapping[str(row["doc_version_id"])] = (str(row["doc_id"]), str(row["product_id"]))
    return mapping


def read_chunks(conn: sqlite3.Connection, doc_ids: set[str]) -> dict[str, list[sqlite3.Row]]:
    """Чанки запрошенных документов: ``doc_id → строки в порядке chunk_no``.

    Один проход по таблице с фильтром по ``doc_id`` — индекс по ``corpus_chunks.doc_id``
    в run-DB отсутствует, и запрос «чанки одного документа» на каждый документ стоил бы
    полного скана 253k строк (замер 17.11.2026: 40 документов не уложились в 120 с).
    """
    chunks: dict[str, list[sqlite3.Row]] = defaultdict(list)
    if not doc_ids:
        return {}
    ordered = sorted(doc_ids)
    for start in range(0, len(ordered), 500):
        batch = ordered[start : start + 500]
        placeholders = ",".join("?" for _ in batch)
        sql = (
            "SELECT chunk_id, doc_id, chunk_no, text, char_start, char_end FROM corpus_chunks"  # noqa: S608 — только '?'-плейсхолдеры
            f" WHERE doc_id IN ({placeholders})"
        )
        for row in conn.execute(sql, batch):
            chunks[str(row["doc_id"])].append(row)
    for rows in chunks.values():
        rows.sort(key=lambda row: int(row["chunk_no"] or 0))
    return dict(chunks)


def build_doc_text(rows: list[sqlite3.Row]) -> DocText:
    """Собирает текст документа из чанков и проверяет согласованность срезов.

    Чанк — точный срез текста документа (``chunk.text`` без ``context_prefix``), поэтому
    конкатенация в порядке ``chunk_no`` восстанавливает текст. ``char_start``/``char_end``
    сверяются с накопленной длиной: расхождение считается числом (``span_mismatch``), а
    отрезки берутся по длине — иначе цитата искалась бы в одном пространстве смещений, а
    пересекалась с чанками в другом.
    """
    spans: list[ChunkSpan] = []
    parts: list[str] = []
    position = 0
    mismatch = 0
    for row in rows:
        text = str(row["text"] or "")
        char_start = row["char_start"]
        char_end = row["char_end"]
        if char_start is not None and int(char_start) != position:
            mismatch += 1
        if char_end is not None and int(char_end) != position + len(text):
            mismatch += 1
        spans.append(
            ChunkSpan(
                chunk_id=str(row["chunk_id"]),
                chunk_no=int(row["chunk_no"] or 0),
                char_start=position,
                char_end=position + len(text),
            )
        )
        parts.append(text)
        position += len(text)
    return DocText(spans=spans, text="".join(parts), span_mismatch=mismatch)


# ---------------------------------------------------------------------------
# Нормализация и поиск цитаты
# ---------------------------------------------------------------------------
#: Нормализация — **не здесь**: она общая с документным прибором (правило A) и живёт в
#: :mod:`core.pipeline.quote_text`. Две копии разошлись бы молча, и один прибор находил бы
#: цитату там, где другой её не видит. Имя оставлено импортом выше: на него опираются
#: `locate_quote` и замер O16.
def locate_quote(doc: DocText, quote: str) -> list[tuple[int, int, list[str]]]:
    """Все вхождения цитаты в тексте документа: ``[(начало, конец, [chunk_id…])]``.

    Смещения — в исходном тексте документа; чанки — все, чей отрезок пересекается с
    вхождением (цитата на границе даёт два и больше). Пустой список — цитаты нет.
    """
    haystack, index = normalize(doc.text)
    needle, _ = normalize(quote)
    if not needle:
        return []
    hits: list[tuple[int, int, list[str]]] = []
    position = haystack.find(needle)
    while position >= 0:
        start = index[position]
        end = index[position + len(needle) - 1] + 1
        chunks = [
            span.chunk_id
            for span in doc.spans
            if span.char_start < end and start < span.char_end
        ]
        hits.append((start, end, chunks))
        position = haystack.find(needle, position + 1)
    return hits


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
def _percentile(values: list[int], q: float) -> float:
    """Перцентиль без numpy: линейная интерполяция между соседними порядковыми."""
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    weight = position - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def build_records(
    doc_entries: Sequence[DocGoldenEntry],
    *,
    params: dict[str, CatalogRow],
    evidence: dict[tuple[str, str], list[EvidenceQuote]],
    versions: dict[str, tuple[str, str]],
    chunks: dict[str, list[sqlite3.Row]],
    doc_urls: dict[str, str],
    max_queries: int,
) -> tuple[list[dict[str, object]], BuildSummary]:
    """Собирает записи эталона и сводку: цитата → чанки → запросы ячейки.

    Ни одна запись не выходит с пустым ``expected_ids``: ячейка без найденной цитаты
    отбрасывается с причиной (``BuildSummary.dropped_cells``).
    """
    summary = BuildSummary(cells=len(doc_entries))
    records: list[dict[str, object]] = []
    per_cell_sizes: list[int] = []

    for entry in doc_entries:
        cell = entry.cell
        product_id = entry.product_id
        param_id = entry.param_id
        expected_docs = set(entry.expected_doc_ids)
        quotes = evidence.get((product_id, param_id), [])
        if not quotes:
            summary.dropped_cells[DROP_NO_EVIDENCE] += 1
            continue

        found_ids: set[str] = set()
        source_urls: list[str] = []
        source_docs: list[str] = []
        boundary_cell = False
        reason: str | None = None
        resolved_any_doc = False
        foreign_any = False

        for item in quotes:
            summary.quotes_total += 1
            if not item.doc_version_id:
                # O5: цитата есть, адрес версии не записан — искать не в чем, но это
                # отдельный факт, а не «цитата не найдена».
                summary.quotes_no_address += 1
                reason = reason or DROP_NO_ADDRESS
                continue
            target = versions.get(item.doc_version_id)
            if target is None:
                summary.quotes_no_doc += 1
                reason = reason or DROP_NO_DOC
                continue
            doc_id, doc_product = target
            if doc_product != product_id:
                # I17: ретривал продукт-скоупед — чанк чужой партиции недостижим по
                # построению, и «эталон не нашёлся» означало бы не то, что есть.
                summary.quotes_foreign_doc += 1
                foreign_any = True
                reason = reason or DROP_FOREIGN_DOC
                continue
            rows = chunks.get(doc_id, [])
            if not rows:
                summary.quotes_no_chunks += 1
                reason = reason or DROP_NO_CHUNKS
                continue
            resolved_any_doc = True
            doc = build_doc_text(rows)
            if doc.span_mismatch:
                summary.chunks_span_mismatch_docs += 1
            hits = locate_quote(doc, item.quote)
            if not hits:
                summary.quotes_not_found += 1
                reason = reason or DROP_QUOTE_NOT_FOUND
                continue
            summary.quotes_found += 1
            for _, _, chunk_ids in hits:
                if len(chunk_ids) > 1:
                    summary.quotes_on_boundary += 1
                    boundary_cell = True
                found_ids.update(chunk_ids)
            source_docs.append(doc_id)
            url = doc_urls.get(doc_id)
            if url:
                source_urls.append(url)

        if not found_ids:
            if resolved_any_doc:
                reason = DROP_QUOTE_NOT_FOUND
            elif foreign_any:
                reason = DROP_FOREIGN_DOC
            else:
                reason = reason or DROP_NO_DOC
            summary.dropped_cells[reason] += 1
            continue

        summary.cells_with_chunks += 1
        if boundary_cell:
            summary.cells_on_boundary += 1
        if set(source_docs) & expected_docs:
            summary.cells_evidence_doc_is_expected += 1
        else:
            summary.cells_evidence_doc_differs += 1
        per_cell_sizes.append(len(found_ids))

        param = params.get(param_id)
        if param is None:
            raise SystemExit(
                f"build: параметр {param_id!r} ячейки {cell!r} отсутствует в снимке входов "
                "прогона — запрос Ш6 не воспроизвести"
            )
        queries = run_queries_for_cell(param, max_queries=max_queries)
        if not queries:
            raise SystemExit(
                f"build: построитель Ш6 вернул пустой список запросов для {cell!r} — "
                "эталон без запроса невалиден"
            )
        summary.queries_total += len(queries)
        lang = param.search_langs[0] if param.search_langs else "ru"
        expected_sorted = sorted(found_ids)
        summary.expected_ids_total += len(expected_sorted)
        for query_index, query in enumerate(queries):
            records.append(
                {
                    "query": query,
                    "expected_ids": expected_sorted,
                    "lang": lang,
                    "cell": cell,
                    "source": source_urls[0] if source_urls else "",
                    # Лишние ключи загрузчик игнорирует; они называют, откуда запись взята.
                    "product_id": product_id,
                    "param_id": param_id,
                    "query_index": query_index,
                    "queries_total": len(queries),
                    "doc_ids": sorted(set(source_docs)),
                }
            )

    summary.records = len(records)
    if per_cell_sizes:
        summary.chunks_per_cell = {
            "mean": round(sum(per_cell_sizes) / len(per_cell_sizes), 4),
            "min": min(per_cell_sizes),
            "max": max(per_cell_sizes),
            "p50": round(_percentile(per_cell_sizes, 0.5), 4),
            "denominator": len(per_cell_sizes),
        }
    else:
        summary.chunks_per_cell = {"mean": None, "min": None, "max": None, "p50": None, "denominator": 0}
    return records, summary


def cmd_build(args: argparse.Namespace) -> int:
    """Пересобирает чанковый эталон из документного эталона и run-DB."""
    started = time.monotonic()
    db_path = Path(args.db).expanduser()
    run_id = db_path.parent.name
    inputs_path = Path(args.inputs).expanduser() if args.inputs else db_path.parent / "plan_inputs.json"
    doc_path = Path(args.golden).expanduser() if args.golden else DEFAULT_DOC_GOLDEN
    out_path = Path(args.out).expanduser() if args.out else DEFAULT_OUT

    case_id, corpus_run_id, doc_entries = load_doc_golden(doc_path)
    if corpus_run_id and corpus_run_id != run_id:
        raise SystemExit(
            f"build: документный эталон собран на прогоне {corpus_run_id!r}, а БД принадлежит "
            f"{run_id!r}: цитаты адресуют другой корпус"
        )
    params = load_params(inputs_path, run_id=run_id)

    conn = _connect_ro(db_path)
    try:
        evidence = read_evidence(conn)
        versions = read_version_map(
            conn, {item.doc_version_id for items in evidence.values() for item in items}
        )
        doc_ids = {doc_id for doc_id, _ in versions.values()}
        chunks = read_chunks(conn, doc_ids)
        doc_urls = {}
        ordered = sorted(doc_ids)
        for start in range(0, len(ordered), 500):
            batch = ordered[start : start + 500]
            placeholders = ",".join("?" for _ in batch)
            sql = (
                "SELECT doc_id, url FROM corpus_docs"  # noqa: S608 — только '?'-плейсхолдеры
                f" WHERE doc_id IN ({placeholders})"
            )
            for row in conn.execute(sql, batch):
                doc_urls[str(row["doc_id"])] = str(row["url"] or "")
    finally:
        conn.close()

    records, summary = build_records(
        doc_entries,
        params=params,
        evidence=evidence,
        versions=versions,
        chunks=chunks,
        doc_urls=doc_urls,
        max_queries=args.max_queries,
    )

    # Запись атомарна по смыслу: файл либо старый, либо полный новый (tmp + replace).
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = out_path.with_suffix(out_path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(out_path)

    payload = {
        "case_id": case_id,
        "run_id": run_id,
        "corpus_run_id": corpus_run_id,
        "golden_docs": str(doc_path),
        "golden_chunks": str(out_path),
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "elapsed_s": round(time.monotonic() - started, 3),
        "summary": summary.to_dict(),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        _print_build_summary(payload, summary)
    return 0


def _print_build_summary(payload: dict[str, object], summary: BuildSummary) -> None:
    """Человекочитаемая сводка пересборки (числа с единицами и адресом замера)."""
    dropped = {key: value for key, value in summary.dropped_cells.items() if value}
    print(f"эталон: {payload['golden_chunks']}")
    print(f"прогон: {payload['run_id']} (документный эталон: {payload['corpus_run_id']})")
    print(f"ячеек в документном эталоне: {summary.cells}")
    print(f"записей (ячейка × запрос Ш6): {summary.records}")
    print(f"ячеек с чанками: {summary.cells_with_chunks}")
    print(f"отброшено ячеек: {summary.cells - summary.cells_with_chunks} — {dropped}")
    quotes = summary.to_dict()["quotes"]
    print(f"цитат: {quotes}")
    sizes = summary.chunks_per_cell
    print(
        f"чанков на ячейку: среднее {sizes.get('mean')}, p50 {sizes.get('p50')}, "
        f"min {sizes.get('min')}, max {sizes.get('max')} (n={sizes.get('denominator')})"
    )
    print(
        f"ячеек с цитатой на границе чанков: {summary.cells_on_boundary}; "
        f"цитата источника = ожидаемый документ: {summary.cells_evidence_doc_is_expected}, "
        f"другой документ: {summary.cells_evidence_doc_differs}"
    )
    print(f"всего ожидаемых id: {summary.expected_ids_total}; время сборки: {payload['elapsed_s']} с")


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------
def cmd_validate(args: argparse.Namespace) -> int:
    """Проверяет эталон: форма, непустые поля, резолв id и продукт-партиция (I17)."""
    golden_path = Path(args.golden).expanduser()
    try:
        golden = load_retrieval_golden(golden_path)
    except (ValueError, FileNotFoundError) as exc:
        print(f"validate: отказ — {exc}", file=sys.stderr)
        return 1

    raw = json.loads(golden_path.read_text(encoding="utf-8"))
    # ``load_retrieval_golden`` уже отверг пустой query/expected_ids; здесь — адресные
    # поля, которые он игнорирует (продукт записи нужен для проверки партиции, I17).
    product_by_id: dict[str, str] = {}
    empty_query = 0
    empty_expected = 0
    for item in raw:
        query = item.get("query")
        expected = item.get("expected_ids")
        if not isinstance(query, str) or not query.strip():
            empty_query += 1
        if not isinstance(expected, list) or not expected:
            empty_expected += 1
            continue
        product = item.get("product_id")
        if isinstance(product, str) and product:
            for chunk_id in expected:
                product_by_id.setdefault(str(chunk_id), product)

    expected_ids = sorted({cid for item in golden for cid in item.expected_ids})
    db_path = Path(args.db).expanduser()
    conn = _connect_ro(db_path)
    try:
        resolved: dict[str, str] = {}
        for start in range(0, len(expected_ids), 500):
            batch = expected_ids[start : start + 500]
            placeholders = ",".join("?" for _ in batch)
            sql = (
                "SELECT chunk_id, product_id FROM corpus_chunks"  # noqa: S608 — только '?'-плейсхолдеры
                f" WHERE chunk_id IN ({placeholders})"
            )
            for row in conn.execute(sql, batch):
                resolved[str(row["chunk_id"])] = str(row["product_id"])
    finally:
        conn.close()

    unresolved = [cid for cid in expected_ids if cid not in resolved]
    partition_mismatch = [
        cid
        for cid, product in product_by_id.items()
        if cid in resolved and resolved[cid] != product
    ]
    payload = {
        "golden": str(golden_path),
        "db": str(db_path),
        "records": len(golden),
        "expected_ids_total": len(expected_ids),
        "unresolved": len(unresolved),
        "unresolved_examples": unresolved[:5],
        "empty_query": empty_query,
        "empty_expected_ids": empty_expected,
        "partition_mismatch": len(partition_mismatch),
        "partition_mismatch_examples": partition_mismatch[:5],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(
            f"эталон: {golden_path} — записей {len(golden)}, ожидаемых id {len(expected_ids)}, "
            f"unresolved = {len(unresolved)}"
        )
        print(
            f"пустой query: {empty_query}; пустой expected_ids: {empty_expected}; "
            f"id чужой партиции: {len(partition_mismatch)}"
        )
        for cid in unresolved[:5]:
            print(f"  не резолвится: {cid}")
        for cid in partition_mismatch[:5]:
            print(f"  чужая партиция: {cid}")

    if empty_query or empty_expected:
        print(
            "validate: отказ — запись с пустым query/expected_ids не является эталоном "
            "(«поиск не нашёл» и «ожидание не существует» — разные факты)",
            file=sys.stderr,
        )
        return 1
    if unresolved or partition_mismatch:
        print(
            f"validate: отказ — {len(unresolved)} из {len(expected_ids)} ожидаемых id "
            f"не резолвятся в {db_path}; id чужой партиции: {len(partition_mismatch)}",
            file=sys.stderr,
        )
        return 1
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    """Парсер прибора: ``build`` (по умолчанию) и ``validate``."""
    parser = argparse.ArgumentParser(
        description="Чанковый retrieval-эталон: пересборка из документного (O14 плана 26)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="собрать чанковый эталон из документного и run-DB")
    build.add_argument("--db", required=True, help="run-DB прогона (открывается только на чтение)")
    build.add_argument("--golden", default=None, help="документный эталон (вход)")
    build.add_argument("--out", default=None, help="чанковый эталон (выход)")
    build.add_argument(
        "--inputs",
        default=None,
        help="снимок входов прогона plan_inputs.json (по умолчанию рядом с run-DB)",
    )
    build.add_argument(
        "--max-queries",
        type=int,
        default=3,
        help="сколько запросов Ш6 брать на ячейку (тот же бюджет, что у фазы)",
    )
    build.add_argument("--json", action="store_true", help="сводка машинным JSON")
    build.set_defaults(handler=cmd_build)

    validate = sub.add_parser("validate", help="проверить чанковый эталон против run-DB")
    validate.add_argument("--golden", required=True, help="чанковый эталон (JSON)")
    validate.add_argument("--db", required=True, help="run-DB прогона (открывается только на чтение)")
    validate.add_argument("--json", action="store_true", help="сводка машинным JSON")
    validate.set_defaults(handler=cmd_validate)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Точка входа: ``build`` по умолчанию, если подкоманда не названа."""
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] not in {"build", "validate", "-h", "--help"}:
        args = ["build", *args]
    parser = build_parser()
    namespace = parser.parse_args(args)
    return int(namespace.handler(namespace))


if __name__ == "__main__":
    raise SystemExit(main())
