"""Поиск дешёвого признака «на странице есть таблица» по данным pdfium (без моделей).

Зачем: гибридный проход отдаёт страницу в pdfium только там, где docling не нужен, значит
решение принимается **до** запуска моделей. Первый кандидат (число векторных путей) провалил
гейт: recall 0 на трёх страницах с таблицей из 181. Этот скрипт проверяет остальные дешёвые
признаки, которые pdfium отдаёт без рендера:

* ``texts`` — число текстовых объектов;
* ``images`` — число изображений;
* ``cols`` — число различимых колонок: сколько разных x-координат начала текстовых
  объектов, сгруппированных с допуском. Таблица — это сетка, то есть много текстовых
  объектов, начинающихся в ограниченном наборе x;
* ``rows`` — то же по y: таблица даёт много строк с регулярным шагом;
* ``grid`` — произведение нормированных ``cols × rows`` относительно числа текстовых
  объектов: у таблицы текстовые объекты лежат в узлах сетки, у прозы — в одном столбце.

Признак годится только при **recall 1.0** на страницах, где docling нашёл таблицу: пропуск =
потерянный текст, и тогда экономия времени не имеет цены, которая бы её оправдала.

Запуск::

    python3 probe_detector.py --docling pages.json --pdf doc.pdf --out detector.jsonl
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as raw

#: Допуск группировки координат, пункты (1 pt = 1/72 дюйма). 2 pt — меньше кегля: два
#: текстовых объекта с началами в пределах 2 pt — это одна колонка, а не две.
TOLERANCE_PT = 2.0


def _clusters(values: list[float], tolerance: float) -> int:
    """Сколько различимых кластеров в наборе координат (жадно, по сортировке)."""
    if not values:
        return 0
    ordered = sorted(values)
    count = 1
    anchor = ordered[0]
    for value in ordered[1:]:
        if value - anchor > tolerance:
            count += 1
            anchor = value
    return count


def page_features(page: object) -> dict:
    kinds: Counter = Counter()
    x_starts: list[float] = []
    y_starts: list[float] = []
    try:
        for obj in page.get_objects():
            kinds[int(obj.type)] += 1
            if obj.type == raw.FPDF_PAGEOBJ_TEXT:
                try:
                    left, bottom, right, top = obj.get_bounds()
                except Exception:  # noqa: BLE001 — объект без позиции
                    continue
                x_starts.append(round(left, 1))
                y_starts.append(round(bottom, 1))
    except Exception:  # noqa: BLE001 — битая страница
        return {"ok": False}
    cols = _clusters(x_starts, TOLERANCE_PT)
    rows = _clusters(y_starts, TOLERANCE_PT)
    texts = kinds[int(raw.FPDF_PAGEOBJ_TEXT)]
    return {
        "ok": True,
        "texts": texts,
        "images": kinds[int(raw.FPDF_PAGEOBJ_IMAGE)],
        "paths": kinds[int(raw.FPDF_PAGEOBJ_PATH)],
        "shadings": kinds[int(raw.FPDF_PAGEOBJ_SHADING)],
        "forms": kinds[int(raw.FPDF_PAGEOBJ_FORM)],
        "cols": cols,
        "rows": rows,
        # Узлы сетки: у таблицы число текстовых объектов близко к cols × rows.
        "grid_ratio": round(texts / (cols * rows), 3) if cols and rows else 0.0,
        "cols_gt1": cols > 1,
        "grid_dense": bool(cols > 1 and rows > 3 and texts > 20),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--docling", required=True)
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    docling = json.loads(Path(args.docling).read_text(encoding="utf-8"))
    doc = pdfium.PdfDocument(args.pdf, autoclose=True)
    rows_out: list[dict] = []
    try:
        for index, doc_page in enumerate(docling):
            page = doc[index]
            feats = page_features(page)
            page.close()
            feats["page_no"] = index + 1
            feats["docling_tables"] = doc_page.get("tables") or 0
            feats["docling_texts"] = doc_page.get("texts") or 0
            feats["docling_chars"] = doc_page.get("md_chars") or 0
            rows_out.append(feats)
    finally:
        doc.close()

    Path(args.out).write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows_out) + "\n",
        encoding="utf-8",
    )

    table_pages = [r for r in rows_out if r.get("docling_tables", 0) > 0]
    print(f"страниц: {len(rows_out)}, с таблицей у docling: {len(table_pages)}")
    candidates = {
        "paths>0": lambda r: r.get("paths", 0) > 0,
        "cols_gt1": lambda r: bool(r.get("cols_gt1")),
        "grid_dense": lambda r: bool(r.get("grid_dense")),
        "images>0": lambda r: r.get("images", 0) > 0,
        "cols>=3": lambda r: r.get("cols", 0) >= 3,
    }
    for name, predicate in candidates.items():
        caught = sum(1 for r in table_pages if predicate(r))
        flagged = sum(1 for r in rows_out if predicate(r))
        recall = caught / len(table_pages) if table_pages else 0.0
        precision = caught / flagged if flagged else 0.0
        print(
            f"{name:12s} recall {recall:.3f} ({caught}/{len(table_pages)})"
            f"  помечено {flagged}/{len(rows_out)}  precision {precision:.3f}"
        )
    print("\nстраницы с таблицей:")
    for row in table_pages:
        print("  ", {k: row.get(k) for k in
                    ("page_no", "texts", "cols", "rows", "grid_ratio", "paths", "images")})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
