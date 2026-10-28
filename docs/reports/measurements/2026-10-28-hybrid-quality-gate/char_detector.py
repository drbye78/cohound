"""Детектор «страница с таблицей» и «страница с заголовками» по геометрии символов pdfium.

Почему не объектами: объектная геометрия pdfium бесполезна как признак. Замер на двух
документах корпуса показал, что число векторных путей не отличает таблицу от прозы (recall
0 на 3 страницах из 181; на 665-страничном документе ``paths > 0`` помечает 663 страницы из
665 при 48 настоящих таблицах — precision 0.07). Причина механическая: таблицы в этих PDF
нарисованы **без линий**, а текстовый объект в pdfium — это абзац, а не ячейка.

Символьная геометрия — другой сигнал и другая цена: ``FPDFText_GetCharBox``/``GetFontSize``
не запускают моделей. Таблица видна как **сетка**: в строке несколько текстовых прогонов,
разделённых пробелами-разрывами, и эти разрывы повторяются от строки к строке (колонки).
Заголовок виден как **кегль больше основного** — docling выводит иерархию именно из этого.

Признаки:

* ``grid_rows`` — число строк, в которых ≥ ``min_cells`` прогонов, разделённых разрывом
  ≥ ``gap_pt``;
* ``grid_cols`` — число колонок: кластеры начал прогонов по x, встреченные в ≥ 2 строках;
* ``table_like`` — ``grid_rows >= 3 and grid_cols >= 3``;
* ``heading_lines`` — строк, где ≥ 80 % символов имеют кегль ≥ ``1.15 × body_size``;
* ``numeric_cells`` — доля прогонов, состоящих только из цифр/пунктуации: таблица с числами
  отличается от двухколоночной прозы именно этим.

Проверка — против docling как арбитра: ``tables > 0`` в его постраничном выводе.
Метрика не «средняя точность», а **recall** (пропущенная таблица = потерянный текст) и
**precision** (иначе страницы уходят в docling и экономии нет).

Запуск::

    python3 char_detector.py --docling pages.json --pdf doc.pdf --out rows.jsonl
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from pathlib import Path

import pypdfium2 as pdfium

#: Разрыв между прогонами внутри строки, пункты. Больше пробела, меньше обычного
#: межколоночного интервала: 8 pt отделяет ячейку от ячейки, но не рвёт фразу.
GAP_PT = 8.0

#: Допуск объединения символов в строку по вертикали — половина кегля; символы одной
#: строки дрожат на доли пункта, строки разнесены на кегль.
ROW_TOLERANCE_RATIO = 0.5

#: Кегль «больше основного»: заголовок. 1.15 — на измеренных документах docling проводит
#: границу иерархии именно в этом диапазоне (14 pt против 12 pt основного).
HEADING_SIZE_RATIO = 1.15


def _empty_geometry() -> dict:
    """Страница без символов: все признаки нулевые и **названы**, а не отсутствуют.

    Ключи обязаны присутствовать всегда: потребитель (отчёт, гейт) не должен различать
    «признак ноль» и «признака нет» — первое это измерение, второе ошибка формы данных.
    """
    return {
        "chars": 0,
        "body_size": 0.0,
        "rows": 0,
        "runs": 0,
        "grid_rows": 0,
        "grid_cols": 0,
        "table_like": False,
        "heading_lines": 0,
        "numeric_runs": 0,
    }


def page_geometry(page: object) -> dict:
    """Символьная геометрия страницы: строки, прогоны, кегли."""
    tp = page.get_textpage()
    try:
        total = tp.count_chars()
        if total <= 0:
            return _empty_geometry()
        boxes: list[tuple[float, float, float, float]] = []
        sizes: list[float] = []
        for index in range(total):
            try:
                box = tp.get_charbox(index)
            except Exception:  # noqa: BLE001 — символ без бокса (артефакт PDF)
                continue
            left, bottom, right, top = box
            if right <= left or top <= bottom:
                continue
            boxes.append((left, bottom, right, top))
            sizes.append(round(top - bottom, 1))
    finally:
        tp.close()
    if not boxes:
        return _empty_geometry()

    body_size = Counter(sizes).most_common(1)[0][0] or 1.0
    row_tolerance = max(1.0, body_size * ROW_TOLERANCE_RATIO)

    # Строки: сортировка по y центра, группировка с допуском.
    order = sorted(range(len(boxes)), key=lambda i: -(boxes[i][1] + boxes[i][3]) / 2)
    rows: list[list[int]] = []
    current: list[int] = []
    anchor = None
    for index in order:
        centre = (boxes[index][1] + boxes[index][3]) / 2
        if anchor is None or abs(centre - anchor) <= row_tolerance:
            current.append(index)
            anchor = centre if anchor is None else (anchor + centre) / 2
        else:
            rows.append(current)
            current = [index]
            anchor = centre
    if current:
        rows.append(current)

    grid_rows = 0
    numeric_runs = 0
    total_runs = 0
    column_hits: Counter = Counter()
    heading_lines = 0
    for row in rows:
        row.sort(key=lambda i: boxes[i][0])
        runs: list[list[int]] = []
        run: list[int] = []
        previous_right = None
        for index in row:
            left, _bottom, right, _top = boxes[index]
            if previous_right is not None and left - previous_right > GAP_PT:
                runs.append(run)
                run = []
            run.append(index)
            previous_right = max(previous_right, right) if previous_right is not None else right
        if run:
            runs.append(run)
        total_runs += len(runs)
        if len(runs) >= 3:
            grid_rows += 1
        for run_indices in runs:
            start = round(boxes[run_indices[0]][0], 0)
            column_hits[start] += 1
            text_chars = []
            for index in run_indices:
                try:
                    text_chars.append(tp.char_at(index) if hasattr(tp, "char_at") else "")
                except Exception:  # noqa: BLE001
                    text_chars.append("")
        headings_chars = sum(
            1 for index in row if (boxes[index][3] - boxes[index][1]) >= body_size * HEADING_SIZE_RATIO
        )
        if headings_chars >= 0.8 * len(row):
            heading_lines += 1

    # Колонки: x-позиции начал, встреченные минимум в двух строках (сетка повторяется).
    clustered: list[float] = []
    for start in sorted(column_hits):
        if clustered and start - clustered[-1] <= GAP_PT:
            continue
        clustered.append(start)
    grid_cols = len(clustered)

    return {
        "chars": len(boxes),
        "body_size": body_size,
        "rows": len(rows),
        "runs": total_runs,
        "grid_rows": grid_rows,
        "grid_cols": grid_cols,
        "table_like": grid_rows >= 3 and grid_cols >= 3,
        "heading_lines": heading_lines,
        "numeric_runs": numeric_runs,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--docling", required=True)
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    docling = json.loads(Path(args.docling).read_text(encoding="utf-8"))
    doc = pdfium.PdfDocument(args.pdf, autoclose=True)
    output: list[dict] = []
    try:
        for index, doc_page in enumerate(docling):
            page = doc[index]
            geo = page_geometry(page)
            page.close()
            markdown = doc_page.get("md") or ""
            geo["page_no"] = index + 1
            geo["docling_tables"] = doc_page.get("tables") or 0
            geo["docling_headings"] = sum(
                1 for line in markdown.splitlines() if line.startswith("#") and line.strip("# ")
            )
            geo["docling_chars"] = doc_page.get("md_chars") or 0
            output.append(geo)
    finally:
        doc.close()
    Path(args.out).write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in output) + "\n", encoding="utf-8"
    )

    tables = [r for r in output if r["docling_tables"] > 0]
    headings = [r for r in output if r["docling_headings"] > 0]
    print(f"страниц: {len(output)}, с таблицей: {len(tables)}, с заголовками: {len(headings)}")

    def report(name: str, predicate, cohort: list[dict]) -> None:
        caught = sum(1 for r in cohort if predicate(r))
        flagged = sum(1 for r in output if predicate(r))
        recall = caught / len(cohort) if cohort else 0.0
        precision = caught / flagged if flagged else 0.0
        print(
            f"{name:16s} recall {recall:.3f} ({caught}/{len(cohort)})"
            f"  помечено {flagged}/{len(output)}  precision {precision:.3f}"
        )

    report("grid_rows>=3", lambda r: r["grid_rows"] >= 3, tables)
    report("table_like", lambda r: r["table_like"], tables)
    report("grid_cols>=3", lambda r: r["grid_cols"] >= 3, tables)
    report("heading>=1", lambda r: r["heading_lines"] >= 1, headings)

    # Порог по числу строк сетки: подбирается как компромисс recall/precision.
    print("\nпорог grid_rows → recall/precision на таблицах:")
    for threshold in (3, 4, 6, 8, 10, 14, 20):
        caught = sum(1 for r in tables if r["grid_rows"] >= threshold)
        flagged = sum(1 for r in output if r["grid_rows"] >= threshold)
        print(
            f"  >={threshold:2d}: recall {caught / len(tables):.3f}"
            f"  precision {caught / flagged if flagged else 0.0:.3f}"
            f"  (помечено {flagged})"
        )
    if tables:
        sizes = [r["grid_rows"] for r in tables]
        print(f"\ngrid_rows у таблиц: медиана {statistics.median(sizes)}, "
              f"мин {min(sizes)}, макс {max(sizes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
