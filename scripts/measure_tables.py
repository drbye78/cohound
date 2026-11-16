#!/usr/bin/env python3
"""Таблицы корпуса: сколько их, какого размера и что из них делает нынешняя нарезка (O39 фаза 2).

**Зачем прибор до кода.** Фаза 2 плана 26 меняет **состав чанков** — чанк обязан нести
``table_id`` и покрытые столбцы, а нарезка — идти от объекта таблицы, а не от строк
markdown. Такая правка ``execution_change``: другой ``text``, ``chunk_no``, ``content_hash``,
``index_build_id`` и ``run_id``, то есть переиндексация. Платить это имеет смысл только по
измеренной нужде, поэтому сначала снимается нужда, и только потом пишется чанкер (A3).

**Три числа, которые решают форму чанка.** Первое — сколько таблиц в корпусе и какого они
размера (если таблица целиком влезает в ``chunk_size``, «чанк на таблицу» возможен; если нет
— нужна нарезка внутри таблицы). Второе — совпадает ли число таблиц-объектов (docling,
``ExtractedDoc.tables``) с числом табличных блоков markdown: по нему видно, можно ли
сопоставить объект с местом в тексте **по порядку**, или сопоставление придётся доказывать
содержимым. Третье — сколько чанков даёт таблица **сегодня**: строка на чанк с шапкой в
``context_prefix``.

**Почему нарезка считается тем же парсером, что в проде.** Табличные спаны берутся из
``parse_markdown(text, profile)`` — того же разбора, на котором работает чанкер, — а чанк
считается «табличным», если его ``[char_start, char_end)`` пересекает спан табличного блока.
Поиск по тексту (``"|" in chunk.text``) дал бы и таблицы, и код, и цитаты с вертикальной
чертой; пересечение спанов — факт о нарезке, а не догадка о содержимом.

**Каждый документ — в своём процессе.** ``VmHWM`` монотонен, а docling на 6.9 GiB узле
упирается в память (замер 16.11.2026: прогон убит OOM на 118-страничном документе). Поэтому
результат пишется построчно в журнал ``<out>.jsonl`` сразу после замера, и сводка
пересчитывается из журнала (``--from-journal``): данные переживают прибор.

**Границы.** Уровень ``layout_tables`` — тот, что объявлен в ``run.yaml`` профиля
``ru-pg-dbms``; ``full`` меняет таблицы только через OCR и здесь не мерится. Механизм
``text_layer`` таблиц не даёт вовсе (``None``), поэтому в выборку не входит: предмет замера —
то, что даёт **структурный** читатель. Числа принадлежат этому корпусу и этому хосту.

Запуск::

    .venv/bin/python scripts/measure_tables.py \\
      --from-state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db \\
      --sample 30 --level layout_tables --device cuda \\
      --out-json /tmp/tables.json
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]

#: Префикс строки дочернего замера в stdout.
_CHILD_PREFIX = "CHILD_JSON:"

#: Страты по числу страниц: цена и состав таблиц зависят от размера документа.
STRATA: tuple[tuple[str, int, int | None], ...] = (
    ("short", 1, 11),
    ("medium", 11, 41),
    ("long", 41, 101),
    ("huge", 101, None),
)

#: Размер куска, объявленный прогоном профиля (``run.yaml`` не переопределяет дефолт).
CHUNK_SIZE = 1500

#: Профиль грамматики для страниц и офисных документов (``md_ast.PROFILES``).
PROFILE = "gfm"


def _is_pdf(path: Path) -> bool:
    """PDF ли это — по сигнатуре: retained-файл называется ``{sha256}.bin``."""
    try:
        with path.open("rb") as handle:
            return handle.read(5).startswith(b"%PDF")
    except OSError:
        return False


def _candidates(db: str) -> list[dict[str, Any]]:
    """Документы с retained-байтами из ``state.db`` (только чтение)."""
    if not Path(db).is_file():
        raise SystemExit(f"measure: нет файла state.db: {db}")
    uri = f"file:{Path(db).expanduser().resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        rows = connection.execute(
            "select distinct extract_page_count, content_hash, raw_path"
            " from document_version where raw_path is not null"
        ).fetchall()
    except sqlite3.Error as exc:
        raise SystemExit(f"measure: state.db не читается: {exc}") from exc
    finally:
        connection.close()
    picked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page_count, content_hash, raw_path in rows:
        path = Path(raw_path)
        if not path.is_file() or not _is_pdf(path):
            continue
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        picked.append(
            {
                "path": key,
                "page_count": int(page_count or 0),
                "content_hash": content_hash,
            }
        )
    return picked


def _stratified(
    candidates: list[dict[str, Any]], sample: int, seed: int
) -> list[dict[str, Any]]:
    """Стратифицированная выборка: поровну на страту, порядок детерминирован ``seed``.

    Перемешивание внутри страты — до отбора: иначе «первые N длинных» были бы первыми N по
    порядку в базе, то есть одним продуктом, и выборка описывала бы продукт, а не корпус.
    """
    generator = random.Random(seed)  # noqa: S311 — воспроизводимость по seed (A3)
    per_stratum = max(1, sample // len(STRATA))
    buckets: list[tuple[str, list[dict[str, Any]]]] = []
    for label, low, high in STRATA:
        bucket = [
            item
            for item in candidates
            if item["page_count"] >= low and (high is None or item["page_count"] < high)
        ]
        bucket.sort(key=lambda item: item["path"])
        generator.shuffle(bucket)
        buckets.append((label, bucket))
    chosen: list[dict[str, Any]] = []
    taken: dict[str, int] = {}
    for label, bucket in buckets:
        for item in bucket[:per_stratum]:
            chosen.append({**item, "stratum": label})
            taken[label] = taken.get(label, 0) + 1
    leftover = [(label, bucket[taken.get(label, 0) :]) for label, bucket in buckets]
    while len(chosen) < sample and any(items for _, items in leftover):
        for index, (label, items) in enumerate(leftover):
            if len(chosen) >= sample:
                break
            if not items:
                continue
            chosen.append({**items.pop(0), "stratum": label})
            leftover[index] = (label, items)
    return chosen


def _hwm_mb() -> float | None:
    """Пик RSS этого процесса, МиБ (``/proc/self/status:VmHWM``)."""
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmHWM:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        return None
    return None


def _document_levels() -> tuple[str, ...]:
    """Объявленные уровни ``structured`` — закрытым множеством, а не свободной строкой.

    Опечатка в ``--level`` иначе доезжает до дочернего процесса и падает там ``ValueError``
    («ребёнок не дал замера»): условие замера обязано проверяться на входе, тем более что
    множество уже объявлено кодом (:class:`core.domain.enums.DocumentLevel`).
    """
    from core.domain.enums import DocumentLevel

    return tuple(level.value for level in DocumentLevel)


def _table_row(table: Any) -> dict[str, Any]:
    """Одна таблица-объект: размер, шапка, подпись и то, что ломает pipe-представление.

    ``cells_with_pipe``/``cells_with_newline`` считаются не для красоты: markdown-таблица
    записывает ячейку строкой между ``|``, поэтому ячейка с ``|`` или переводом строки
    рвёт представление, и сопоставление «объект ↔ блок markdown» по числу строк перестаёт
    быть верным.

    **Ячейки шапки считаются вместе с ячейками данных**, и это исправление дефекта, а не
    удобство: шапка печатается той же строкой ``| … |`` и попадает в ``text`` куска, а
    первая версия прибора её не считала. Цена ошибки — не косметика: ``cells_chars`` шёл в
    ``fits_one_chunk`` («влезает ли таблица в один кусок»), то есть главный ответ замера был
    смещён в сторону «влезает», а ``cells_with_pipe`` недосчитывал разрывы именно там, где
    они и возникают — в именах столбцов. Найдено тестом, а не на глаз.
    """
    header = list(table.header or ())
    cells = [cell for cell in header] + [cell for row in table.rows for cell in row]
    widths = {len(header)} if header else set()
    widths.update(len(row) for row in table.rows)
    return {
        "table_id": table.table_id,
        "page_no": table.page_no,
        "rows": len(table.rows),
        "cols": len(header) if header else (len(table.rows[0]) if table.rows else 0),
        "has_header": bool(header),
        "caption_chars": len(table.caption or ""),
        "cells_chars": sum(len(cell) for cell in cells),
        "max_cell_chars": max((len(cell) for cell in cells), default=0),
        "cells_with_pipe": sum(1 for cell in cells if "|" in cell),
        "cells_with_newline": sum(1 for cell in cells if "\n" in cell),
        "ragged_widths": sorted(widths),
    }


def _table_blocks(text: str) -> list[dict[str, Any]]:
    """Табличные блоки markdown: спан, строки, шапка — тем же парсером, что у чанкера."""
    from core.adapters.md_ast import parse_markdown

    document = parse_markdown(text, PROFILE)
    blocks: list[dict[str, Any]] = []
    for block in document.blocks:
        for item in block.walk():
            if item.kind != "table":
                continue
            rows = [child for child in item.walk() if child.kind == "table_row"]
            blocks.append(
                {
                    "start": item.span.start_char,
                    "end": item.span.end_char,
                    "rows": len(rows),
                    "header_rows": int(item.info.get("header_rows") or 0),
                    "dialect": str(item.info.get("dialect") or "pipe"),
                    "chars": item.span.end_char - item.span.start_char,
                    # Самая длинная строка блока: если она больше ``chunk_size``, нарезка
                    # режет её **внутри** (жёсткий разрез), и кусок перестаёт быть строкой.
                    "max_row_chars": max(
                        (row.span.end_char - row.span.start_char for row in rows), default=0
                    ),
                }
            )
    return blocks


def _chunk_facts(
    text: str,
    table_spans: list[tuple[int, int]],
    tables: Any = None,
) -> dict[str, Any]:
    """Что нарезка делает с таблицами: чанки, их число, размер **и адрес**.

    Чанк «табличный», если его диапазон пересекает спан табличного блока. Пересечение — факт
    о нарезке; ``"|" in text`` дал бы ещё код и цитаты с вертикальной чертой.

    Адрес (``table_ref``) считается тем же чанкером, что в проде, и с теми же таблицами
    читателя: прибор обязан мерить **то, что будет записано**, а не пересказывать код.
    Три исхода различаются числом, потому что цена у них разная: адрес со строками (кусок
    можно процитировать построчно), адрес без строк (таблица названа, строки — нет) и
    отсутствие адреса (кусок табличный, но связать его не с чем).
    """
    from core.adapters.chunker_markdown import MarkdownChunker, _table_bindings
    from core.adapters.md_ast import parse_markdown

    chunker = MarkdownChunker(chunk_size=CHUNK_SIZE, profile=PROFILE)
    chunks = chunker.chunk_text("measure", text, tables)
    verified = [
        binding.rows_verified
        for binding in _table_bindings(parse_markdown(text, PROFILE), tables)
    ]
    table_chunks: list[dict[str, Any]] = []
    for chunk in chunks:
        start, end = chunk.char_start, chunk.char_end
        if start is None or end is None:
            continue
        inside = [
            index
            for index, (low, high) in enumerate(table_spans)
            if start < high and end > low
        ]
        if not inside:
            continue
        table_chunks.append(
            {
                "chunk_no": chunk.chunk_no,
                "chars": len(chunk.text),
                "tables": inside,
                "context_chars": len(chunk.context_prefix),
                "table_ref": (
                    None
                    if chunk.table_ref is None
                    else {
                        "table_id": chunk.table_ref.table_id,
                        "row_start": chunk.table_ref.row_start,
                        "row_end": chunk.table_ref.row_end,
                    }
                ),
            }
        )
    return {
        "chunks": len(chunks),
        "tables_row_verified": sum(1 for flag in verified if flag),
        "tables_row_unverified": sum(1 for flag in verified if not flag),
        "table_chunks_bound": sum(1 for item in table_chunks if item["table_ref"]),
        "table_chunks_with_rows": sum(
            1
            for item in table_chunks
            if item["table_ref"] and item["table_ref"]["row_start"] is not None
        ),
        "table_chunks_unbound": sum(
            1 for item in table_chunks if not item["table_ref"]
        ),
        "table_chunks": len(table_chunks),
        "table_chunk_chars": sum(item["chars"] for item in table_chunks),
        "table_chunk_chars_median": (
            statistics.median([item["chars"] for item in table_chunks])
            if table_chunks
            else None
        ),
        "table_chunk_context_median": (
            statistics.median([item["context_chars"] for item in table_chunks])
            if table_chunks
            else None
        ),
        "chunks_per_table": sorted(
            sum(1 for item in table_chunks if index in item["tables"])
            for index in range(len(table_spans))
        ),
    }


def _run_child(args: argparse.Namespace) -> int:
    """Дочерний замер: извлечение, таблицы-объекты, табличные блоки, нарезка."""
    from core.adapters.extract_document import DoclingDocumentExtractor
    from core.domain.enums import DocumentLevel, PdfDevice

    extractor = DoclingDocumentExtractor(
        DocumentLevel(args.level), pdf_device=PdfDevice(args.device)
    )
    # ``--pdf`` объявлен ``action="append"``: в дочернем процессе путь — последний элемент
    # списка, а не список (иначе в адаптер уезжает ``str(list)`` и он честно отвечает
    # «retained-файл не найден», указывая на несуществующий файл с квадратными скобками).
    source = Path(args.pdf[-1] if isinstance(args.pdf, list) else args.pdf)
    started = time.perf_counter()
    try:
        document = extractor.extract_path(str(source), filename_hint="document.pdf")
    except Exception as exc:  # noqa: BLE001 — цена отказа тоже факт
        print(
            _CHILD_PREFIX
            + json.dumps(
                {"error": f"{type(exc).__name__}: {exc}", "peak_rss_mb": _hwm_mb()},
                ensure_ascii=False,
            )
        )
        return 0
    seconds = time.perf_counter() - started
    text = getattr(document, "text", None) or ""
    tables = getattr(document, "tables", None)
    payload: dict[str, Any] = {
        "seconds": round(seconds, 3),
        "chars": len(text),
        "pages": getattr(document, "page_count", None),
        "error": getattr(document, "error", None),
        "peak_rss_mb": _hwm_mb(),
        "tables_state": (
            "unmeasured" if tables is None else ("empty" if not tables else "present")
        ),
        "tables": [_table_row(table) for table in (tables or ())],
    }
    if text:
        blocks = _table_blocks(text)
        payload["table_blocks"] = blocks
        payload["block_chars"] = sum(item["chars"] for item in blocks)
        payload.update(
            _chunk_facts(
                text,
                [(item["start"], item["end"]) for item in blocks],
                tables,
            )
        )
        payload["object_vs_block"] = {
            "tables": len(payload["tables"]),
            "blocks": len(blocks),
            "equal": len(payload["tables"]) == len(blocks),
        }
    print(_CHILD_PREFIX + json.dumps(payload, ensure_ascii=False))
    return 0


def _measure_one(
    item: dict[str, Any], *, level: str, device: str, timeout: float
) -> dict[str, Any]:
    """Один документ в дочернем процессе: убийство ребёнка не уносит весь замер."""
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--pdf",
        item["path"],
        "--level",
        level,
        "--device",
        device,
    ]
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, cwd=str(REPO)
        )
    except subprocess.TimeoutExpired:
        return {**item, "error": f"timeout {timeout:.0f} s", "wall_seconds": timeout}
    wall = round(time.perf_counter() - started, 3)
    payload: dict[str, Any] | None = None
    for line in completed.stdout.splitlines():
        if line.startswith(_CHILD_PREFIX):
            try:
                payload = json.loads(line[len(_CHILD_PREFIX) :])
            except ValueError:
                payload = None
    if payload is None:
        return {
            **item,
            "wall_seconds": wall,
            "error": f"ребёнок не дал замера (exit {completed.returncode})",
            "stderr_tail": completed.stderr[-500:],
        }
    return {**item, **payload, "wall_seconds": wall}


class _Journal:
    """Дописываемый журнал: строка на документ, сразу после замера.

    Прибор, пишущий результат только в конце, теряет всё при убийстве процесса — и это не
    гипотеза: прогон O30 16.11.2026 погиб на 15-м документе из 20 и не оставил чисел.
    """

    def __init__(self, path: Path | None) -> None:
        self.path = path
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")

    def write(self, row: dict[str, Any]) -> None:
        if self.path is None:
            return
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            handle.flush()


def _read_journal(path: Path) -> list[dict[str, Any]]:
    """Строки журнала, которые прибор успел записать (убитый прогон тоже годится)."""
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError as exc:
            raise SystemExit(f"measure: журнал повреждён: {line[:80]!r}") from exc
    return rows


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Сводка: таблицы, их размеры, совпадение объекта с блоком, чанки из таблиц."""
    ok = [row for row in rows if not row.get("error") and row.get("chars")]
    tables = [table for row in ok for table in row.get("tables") or ()]
    blocks = [block for row in ok for block in row.get("table_blocks") or ()]
    table_chunks = [row["table_chunks"] for row in ok if "table_chunks" in row]
    chunk_chars = [row["table_chunk_chars"] for row in ok if "table_chunk_chars" in row]
    block_chars = [row.get("block_chars", 0) for row in ok]
    sizes = [table["cells_chars"] + table["caption_chars"] for table in tables]
    chunk_sizes = [
        size
        for row in ok
        for size in ([row["table_chunk_chars_median"]] if row.get("table_chunk_chars_median") else [])
    ]
    return {
        "documents": len(rows),
        "measured": len(ok),
        "failed": sum(1 for row in rows if row.get("error")),
        # Документ без ошибки и без текста не входит ни в ``measured``, ни в ``failed`` —
        # без отдельного числа он исчезает из отчёта молча («документов 24, измерено 22,
        # отказов 2» сходилось только по случайности: 22 + 2 = 24).
        "empty_text": sum(
            1 for row in rows if not row.get("error") and not row.get("chars")
        ),
        "tables_state": {
            state: sum(1 for row in rows if row.get("tables_state") == state)
            for state in ("present", "empty", "unmeasured")
        },
        "documents_with_tables": sum(1 for row in ok if row.get("tables")),
        "tables": len(tables),
        "tables_with_header": sum(1 for table in tables if table["has_header"]),
        "tables_with_caption": sum(1 for table in tables if table["caption_chars"]),
        "rows_total": sum(table["rows"] for table in tables),
        "rows_median": statistics.median([t["rows"] for t in tables]) if tables else None,
        "cols_median": statistics.median([t["cols"] for t in tables]) if tables else None,
        "cols_max": max((t["cols"] for t in tables), default=None),
        "cells_chars_total": sum(t["cells_chars"] for t in tables),
        "cells_with_pipe": sum(t["cells_with_pipe"] for t in tables),
        "cells_with_newline": sum(t["cells_with_newline"] for t in tables),
        "cells_chars_median": statistics.median(sizes) if sizes else None,
        "cells_chars_max": max(sizes, default=None),
        "fits_one_chunk": sum(1 for size in sizes if size <= CHUNK_SIZE),
        "objects_vs_blocks_equal": sum(
            1 for row in ok if (row.get("object_vs_block") or {}).get("equal")
        ),
        "objects_vs_blocks_differ": sum(
            1
            for row in ok
            if row.get("object_vs_block") and not row["object_vs_block"]["equal"]
        ),
        "table_blocks": len(blocks),
        "blocks_with_row_over_chunk": sum(
            1 for block in blocks if block["max_row_chars"] > CHUNK_SIZE
        ),
        "max_row_chars": max((block["max_row_chars"] for block in blocks), default=None),
        "blocks_fitting_one_chunk": sum(
            1 for block in blocks if block["chars"] <= CHUNK_SIZE
        ),
        "table_block_chars": sum(block_chars),
        "chars_total": sum(row["chars"] for row in ok),
        "table_share_of_text": (
            round(sum(block_chars) / sum(row["chars"] for row in ok), 5)
            if ok and sum(row["chars"] for row in ok)
            else None
        ),
        "chunks_total": sum(row.get("chunks", 0) for row in ok),
        "table_chunks": sum(table_chunks),
        "tables_row_verified": sum(row.get("tables_row_verified", 0) for row in ok),
        "tables_row_unverified": sum(row.get("tables_row_unverified", 0) for row in ok),
        "table_chunks_bound": sum(row.get("table_chunks_bound", 0) for row in ok),
        "table_chunks_with_rows": sum(
            row.get("table_chunks_with_rows", 0) for row in ok
        ),
        "table_chunks_unbound": sum(row.get("table_chunks_unbound", 0) for row in ok),
        "table_chunk_share": (
            round(sum(table_chunks) / sum(row.get("chunks", 0) for row in ok), 5)
            if ok and sum(row.get("chunks", 0) for row in ok)
            else None
        ),
        "table_chunk_chars_total": sum(chunk_chars),
        "table_chunk_chars_median": statistics.median(chunk_sizes) if chunk_sizes else None,
        "chunks_per_table_median": (
            statistics.median(
                [value for row in ok for value in row.get("chunks_per_table") or []]
            )
            if any(row.get("chunks_per_table") for row in ok)
            else None
        ),
        "chunks_per_table_max": max(
            (value for row in ok for value in row.get("chunks_per_table") or []),
            default=None,
        ),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Таблицы корпуса и то, что из них делает нарезка (O39 фаза 2)"
    )
    parser.add_argument("--db", default="", help="state.db оператора (только чтение)")
    parser.add_argument("--from-state-db", dest="db_path", help="то же, длинным именем")
    parser.add_argument("--pdf", action="append", default=[], help="PDF (можно несколько)")
    parser.add_argument(
        "--level",
        choices=_document_levels(),
        default="layout_tables",
        help="уровень извлечения structured: off|text|layout|layout_tables|full",
    )
    parser.add_argument("--device", default="cuda", choices=("cpu", "cuda"))
    parser.add_argument(
        "--sample",
        type=int,
        default=30,
        help=(
            "документов в выборке; квота страты = max(1, sample // 4), поэтому при "
            "sample < 4 выборка всё равно даст 4 документа (по одному на страту)"
        ),
    )
    parser.add_argument("--seed", type=int, default=0, help="seed выборки")
    parser.add_argument(
        "--top", type=int, default=0, help="ограничить число документов (0 — все)"
    )
    parser.add_argument(
        "--timeout", type=float, default=1800.0, help="таймаут одного документа, с"
    )
    parser.add_argument("--out-json", metavar="PATH", help="машинный отчёт")
    parser.add_argument(
        "--from-journal",
        metavar="PATH",
        help="пересчитать сводку из журнала (документы не извлекаются)",
    )
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.child:
        return _run_child(args)

    if args.from_journal:
        journal_path = Path(args.from_journal)
        if not journal_path.is_file():
            # Тот же класс, что отсутствующая ``state.db``: прибор отказывает строкой, а не
            # трейсбеком. Журнал — durable артефакт, и «файла нет» здесь означает «пересчёт
            # не с чего», а не дефект прибора.
            print(
                f"measure: журнала нет: {journal_path}",
                file=sys.stderr,
            )
            return 2
        rows = _read_journal(journal_path)
        summary = _summarize(rows)
        payload = {
            "instrument": "scripts/measure_tables.py",
            "conditions": {
                "source": "journal",
                "journal": str(journal_path),
                "level": args.level,
                "device": args.device,
                "chunk_size": CHUNK_SIZE,
                "profile": PROFILE,
                "measured": summary["measured"],
            },
            "summary": summary,
            "documents": rows,
        }
        print(json.dumps(summary, ensure_ascii=False, indent=1))
        if args.out_json:
            Path(args.out_json).write_text(
                json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
            )
            print(f"отчёт: {args.out_json}")
        return 0 if summary["measured"] else 1

    documents = [{"path": str(Path(raw).expanduser().resolve())} for raw in args.pdf]
    db = args.db_path or args.db
    if db:
        documents.extend(_candidates(db))
    documents = [item for item in documents if Path(item["path"]).is_file()]
    if args.top:
        documents = documents[: args.top]
    if not documents:
        print("measure: нет входных данных", file=sys.stderr)
        return 2
    planned = len(documents)
    if not args.pdf:
        documents = _stratified(documents, args.sample, args.seed)

    journal_path = Path(f"{args.out_json}.jsonl") if args.out_json else None
    journal = _Journal(journal_path)
    rows: list[dict[str, Any]] = []
    for position, item in enumerate(documents, start=1):
        print(
            f"[{position}/{len(documents)}] {Path(item['path']).name[:16]}"
            f"  {item.get('page_count', '?')} стр.",
            flush=True,
        )
        row = _measure_one(
            item, level=args.level, device=args.device, timeout=args.timeout
        )
        rows.append(row)
        journal.write(row)
        if row.get("error"):
            print(f"    отказ: {row['error']}", flush=True)
            continue
        print(
            f"    таблиц {len(row.get('tables') or ())}"
            f" ({row.get('tables_state')}), блоков {len(row.get('table_blocks') or ())},"
            f" чанков {row.get('chunks')}, из них табличных {row.get('table_chunks')}"
            f" (с адресом {row.get('table_chunks_bound')},"
            f" со строками {row.get('table_chunks_with_rows')}),"
            f" {row.get('seconds')} с, пик {row.get('peak_rss_mb')} MiB",
            flush=True,
        )

    summary = _summarize(rows)
    payload = {
        "instrument": "scripts/measure_tables.py",
        "conditions": {
            "source": "measured",
            "journal": str(journal_path) if journal_path else None,
            "planned": planned,
            "measured": len(rows),
            "level": args.level,
            "device": args.device,
            "chunk_size": CHUNK_SIZE,
            "profile": PROFILE,
            "sample": args.sample,
            "seed": args.seed,
            "db": db or None,
        },
        "summary": summary,
        "documents": rows,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    if args.out_json:
        Path(args.out_json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        print(f"отчёт: {args.out_json}")
    return 0 if summary["measured"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
