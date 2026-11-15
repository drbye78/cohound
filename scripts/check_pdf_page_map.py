#!/usr/bin/env python3
"""Проверка карты страниц PDF на реальном корпусе (O36, план 26).

Прибор отвечает на один вопрос: **можно ли верить ``ExtractedDoc.page_map``** — то есть
покрывают ли её сегменты ровно тот текст, который опубликован в ``doc.text``, без дыр,
пересечений и выходов за границу. Это не тест на фикстурах (он есть —
``tests/adapters/test_extract_pdf_text.py``), а проверка на корпусе: фикстура проверяет
форму, корпус — что форма держится на настоящих документах (2 763 страницы, 6.5 M символов
встречаются).

Проверяемые инварианты (по каждому документу):

* ``page_map is None`` при непустом тексте — **находка**, а не норма: карта либо точна,
  либо её нет по названной причине (посегментная финализация разошлась со склейкой);
* сегменты отсортированы по номеру страницы, не пересекаются, ``0 <= start <= end <=
  len(text)``;
* между соседними сегментами — только ``""`` или ``_PAGE_SEPARATOR``: разделитель страниц
  это **разрыв** карты, а не её часть;
* обратная сборка: ``_PAGE_SEPARATOR.join(text[s:e] for ...).strip() == text.strip()``.

Запуск::

    .venv/bin/python scripts/check_pdf_page_map.py --root ~/.cohound/data/corpus_raw
    .venv/bin/python scripts/check_pdf_page_map.py --root … --limit 50 --json

Код выхода: 0 — дефектов нет, 1 — есть (печатаются адреса), 2 — корень не найден.
Нейтрален к предметной категории (§1.3): читает retained-файлы и не знает, о чём они.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

# Запуск из корня репозитория без установки пакета (как ``hound.py``).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.adapters.extract_pdf_text import (  # noqa: E402 — после правки sys.path
    _PAGE_SEPARATOR,
    PdfiumDocumentExtractor,
)

#: Сигнатура PDF: смещение 0, ровно так начинается файл.
_PDF_MAGIC = b"%PDF-"


def _is_pdf(path: Path) -> bool:
    """Файл — PDF по магии байтов (retained-файл суффикса не несёт)."""
    try:
        with path.open("rb") as handle:
            return handle.read(len(_PDF_MAGIC)) == _PDF_MAGIC
    except OSError:
        return False


def _collect(root: Path, limit: int | None) -> list[Path]:
    """PDF-файлы корпуса в устойчивом порядке (обход каталога — не порядок)."""
    found: list[Path] = []
    for path in sorted(root.rglob("*.bin")):
        if _is_pdf(path):
            found.append(path)
            if limit is not None and len(found) >= limit:
                break
    return found


def _check_one(doc: Any) -> str | None:
    """``None`` — карта верна; иначе короткая причина дефекта."""
    page_map = doc.page_map
    if page_map is None:
        return "map_none" if doc.text else None
    text = doc.text
    previous_end = 0
    pieces: list[str] = []
    for page_no, start, end in page_map:
        if not (0 <= start <= end <= len(text)):
            return f"range page={page_no} {start}:{end} len={len(text)}"
        if start < previous_end:
            return f"overlap page={page_no} start={start} prev_end={previous_end}"
        gap = text[previous_end:start]
        if gap not in ("", _PAGE_SEPARATOR):
            return f"gap page={page_no} {previous_end}:{start} {gap!r}"
        pieces.append(text[start:end])
        previous_end = end
    if _PAGE_SEPARATOR.join(pieces).strip() != text.strip():
        return "rebuild"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.home() / ".cohound" / "data" / "corpus_raw",
        help="корень retained-файлов корпуса",
    )
    parser.add_argument("--limit", type=int, default=None, help="проверить не больше N файлов")
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    args = parser.parse_args(argv)

    if not args.root.is_dir():
        print(f"корень не найден: {args.root}", file=sys.stderr)
        return 2

    started = time.time()
    files = _collect(args.root, args.limit)
    extractor = PdfiumDocumentExtractor()
    counts: dict[str, int] = {
        "files": len(files),
        "with_text": 0,
        "pages": 0,
        "max_pages": 0,
        "max_chars": 0,
        "map_none": 0,
        "defects": 0,
    }
    defects: list[dict[str, str]] = []
    for path in files:
        try:
            doc = extractor.extract_path(path, filename_hint="document.pdf")
        except Exception as exc:  # noqa: BLE001 — прибор не падает на одном файле
            defects.append({"file": str(path), "defect": f"exception: {exc}"})
            continue
        if doc.text:
            counts["with_text"] += 1
        counts["pages"] += doc.page_count
        counts["max_pages"] = max(counts["max_pages"], doc.page_count)
        counts["max_chars"] = max(counts["max_chars"], len(doc.text))
        defect = _check_one(doc)
        if defect is not None:
            if defect == "map_none":
                counts["map_none"] += 1
            defects.append({"file": str(path), "defect": defect})
    counts["defects"] = len(defects)
    counts["seconds"] = round(time.time() - started, 1)

    if args.json:
        print(json.dumps({"counts": counts, "defects": defects[:50]}, ensure_ascii=False))
    else:
        print(
            f"файлов: {counts['files']}, с текстом: {counts['with_text']}, "
            f"страниц: {counts['pages']} (max {counts['max_pages']}), "
            f"max символов: {counts['max_chars']}, "
            f"карты нет при тексте: {counts['map_none']}, "
            f"дефектов: {counts['defects']}, {counts['seconds']} c"
        )
        for item in defects[:20]:
            print(f"  {item['defect']}: {item['file']}")
    return 0 if not defects else 1


if __name__ == "__main__":
    raise SystemExit(main())
