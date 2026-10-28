"""Скан корпуса: какая доля **документов** могла бы уйти в pdfium без потери структуры.

Считать по страницам мало: гибрид решается документом (``extracted_doc.family`` — одно
значение на документ, и ``heading_path`` живёт в чанке, а не в странице). Поэтому единица
здесь — документ: он пригоден, если **ни одна** его страница не помечена консервативным
детектором (таблица возможна **или** есть заголовки). Порог консервативный намеренно:
пропущенная таблица — потерянный текст, а лишняя страница в docling — только время.

Результат — JSONL по документу (resumable) и сводка: сколько документов/страниц пригодно.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pypdfium2 as pdfium

#: Консервативный порог: столько различных колонок в строках уже считается риском таблицы.
#: Обоснование — замер на 48 настоящих таблицах: при ``cols >= 5`` recall 1.000, при
#: ``cols >= 8`` — 0.979 (теряется таблица, то есть текст).
COLS_RISK = 5

#: Разрыв между прогонами внутри строки, пункты (табличная ячейка отстоит на 8–17 pt).
GAP_PT = 8.0


def page_risk(page: object) -> tuple[bool, int, int]:
    """(риск, символов, строк-заголовков) для страницы."""
    tp = page.get_textpage()
    try:
        total = tp.count_chars()
        if total <= 0:
            return False, 0, 0
        boxes = []
        for index in range(total):
            try:
                left, bottom, right, top = tp.get_charbox(index)
            except Exception:  # noqa: BLE001
                continue
            if right > left and top > bottom:
                boxes.append((left, bottom, right, top))
    finally:
        tp.close()
    if not boxes:
        return False, 0, 0

    sizes: dict[float, int] = {}
    for box in boxes:
        key = round(box[3] - box[1], 1)
        sizes[key] = sizes.get(key, 0) + 1
    body = max(sizes.items(), key=lambda item: item[1])[0] or 1.0
    tolerance = max(1.0, body * 0.5)

    boxes.sort(key=lambda b: -(b[1] + b[3]) / 2)
    rows: list[list[tuple[float, float, float, float]]] = []
    current: list[tuple[float, float, float, float]] = []
    anchor = None
    for box in boxes:
        centre = (box[1] + box[3]) / 2
        if anchor is None or abs(centre - anchor) <= tolerance:
            current.append(box)
            anchor = centre if anchor is None else (anchor + centre) / 2
        else:
            rows.append(current)
            current = [box]
            anchor = centre
    if current:
        rows.append(current)

    starts: set[float] = set()
    heading_rows = 0
    for row in rows:
        row.sort(key=lambda b: b[0])
        previous_right = None
        for box in row:
            if previous_right is None or box[0] - previous_right > GAP_PT:
                # Начало прогона: колонка-кандидат.
                starts.add(round(box[0]))
            previous_right = max(previous_right, box[2]) if previous_right is not None else box[2]
        big = sum(1 for box in row if (box[3] - box[1]) >= body * 1.15)
        if big >= 0.8 * len(row):
            heading_rows += 1

    return (len(starts) >= COLS_RISK or heading_rows > 0), len(boxes), heading_rows


def is_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            return fh.read(5) == b"%PDF-"
    except OSError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=300, help="сколько документов сканировать")
    ap.add_argument("--root", default="corpus_raw/postgrespro")
    args = ap.parse_args()

    out = Path(args.out)
    seen: set[str] = set()
    if out.exists():
        for line in out.read_text(encoding="utf-8").splitlines():
            try:
                seen.add(json.loads(line)["doc_id"])
            except Exception:  # noqa: BLE001
                pass

    root = Path(args.root)
    names = sorted(p for p in root.glob("*.bin"))
    scanned = 0
    started = time.time()
    for path in names:
        if scanned >= args.limit:
            break
        doc_id = f"{root.name}/{path.name}"
        if doc_id in seen:
            continue
        if not is_pdf(path):
            continue
        record: dict = {"doc_id": doc_id, "bytes": path.stat().st_size}
        try:
            doc = pdfium.PdfDocument(str(path), autoclose=True)
            try:
                pages = len(doc)
                risk_pages = 0
                chars = 0
                heading_pages = 0
                for index in range(pages):
                    page = doc[index]
                    risky, page_chars, heading_rows = page_risk(page)
                    page.close()
                    chars += page_chars
                    if heading_rows:
                        heading_pages += 1
                    if risky:
                        risk_pages += 1
            finally:
                doc.close()
            record.update(
                {
                    "pages": pages,
                    "chars": chars,
                    "risk_pages": risk_pages,
                    "heading_pages": heading_pages,
                    "eligible": risk_pages == 0 and chars > 0,
                    "ok": True,
                }
            )
        except Exception as exc:  # noqa: BLE001 — битый PDF: фиксируем, не падаем
            record.update({"ok": False, "error": repr(exc), "eligible": False})
        with out.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        scanned += 1
        if scanned % 25 == 0:
            print(f"{scanned} docs, {time.time() - started:.0f}s", flush=True)

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    good = [r for r in rows if r.get("ok")]
    eligible = [r for r in good if r.get("eligible")]
    pages = sum(r["pages"] for r in good)
    eligible_pages = sum(r["pages"] for r in eligible)
    risky_pages = sum(r["risk_pages"] for r in good)
    print(f"документов: {len(good)} (ошибок {len(rows) - len(good)})")
    print(f"страниц: {pages}, с риском: {risky_pages} ({risky_pages / pages:.1%})")
    print(f"пригодных документов (ни одной рискованной страницы): {len(eligible)}"
          f" ({len(eligible) / len(good):.1%})")
    print(f"их страниц: {eligible_pages} ({eligible_pages / pages:.1%} всего)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
