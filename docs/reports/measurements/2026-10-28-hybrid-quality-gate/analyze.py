"""Гейт качества гибридного прохода: pdfium на простых страницах, docling на страницах с графикой.

Вопрос замера — не «сколько экономит», а **не теряет ли текст**. Поэтому здесь считается
то, что теряется при уходе страницы в pdfium: таблицы, идентификаторы, числа, заголовки.
Средние не считаются: считаются худшая страница и низкий процентиль.

Стороны сравнения берутся из разных источников, и это важно помнить:

* **docling** — постраничный разбор из ``pages_*.json``, снятый прогоном ``layout_tables``
  (``tables`` — число найденных таблиц, ``texts`` — текстовых блоков, ``md`` — markdown
  этой страницы). Это «эталон качества»: то, что сегодня публикуется в карту.
* **pdfium** — то, что даёт нативный текстовый слой: число объектов-путей (векторная
  графика — признак, по которому страницу нельзя отдавать pdfium-у) и текст.

Детектор — число объектов ``FPDF_PAGEOBJ_PATH`` на странице. Проверяется его **recall**:
страницы, где docling нашёл таблицу, обязаны быть помечены. Пропущенная таблица — это
пропущенный текст, и тогда все прочие числа замера ничего не значат.

Запуск::

    python3 docs/reports/measurements/2026-10-28-hybrid-quality-gate/analyze.py \\
        --docling /tmp/.../pages_medium_tables.json --pdf <pdf> --out результат.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path

import pypdfium2 as pdfium
import pypdfium2.raw as raw

#: Идентификатор: `snake_case`, `camelCase`, `UPPER_SNAKE`, имя параметра GUC/функции.
#: Именно по ним читатель отчёта отличает «текст похож» от «текст тот же»: прозаический
#: абзац, переставленный иначе, всё ещё проза, а потерянный идентификатор — потерянная
#: ссылка на реальную возможность продукта.
_RE_IDENT = re.compile(r"\b(?:[a-z][a-z0-9]*(?:_[a-z0-9]+)+|[a-z]+[A-Z][A-Za-z0-9]*|[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)\b")

#: Числовой токен с необязательной единицей измерения: `1 024`, `8.0`, `55 W`, `3.5 GiB`.
_RE_NUMBER = re.compile(r"\b\d+(?:[.,]\d+)?\b")

#: Заголовок markdown — единственное место, где pdfium не даёт ничего эквивалентного.
_RE_HEADING = re.compile(r"^#{1,6}\s+\S", re.MULTILINE)


def pdfium_pages(pdf_path: Path) -> list[dict]:
    """Постранично: объекты-пути (детектор), прочие объекты, число символов, текст."""
    doc = pdfium.PdfDocument(str(pdf_path), autoclose=True)
    rows: list[dict] = []
    try:
        for index in range(len(doc)):
            page = doc[index]
            paths = 0
            other = 0
            try:
                for obj in page.get_objects():
                    if obj.type == raw.FPDF_PAGEOBJ_PATH:
                        paths += 1
                    else:
                        other += 1
            except Exception:  # noqa: BLE001 — битая страница: считаем неизвестным
                paths = -1
            text = ""
            try:
                tp = page.get_textpage()
                text = tp.get_text_range() or ""
                chars = tp.count_chars()
                tp.close()
            except Exception:  # noqa: BLE001
                chars = -1
            page.close()
            rows.append(
                {
                    "page_no": index + 1,
                    "paths": paths,
                    "other": other,
                    "chars": chars,
                    "text": text,
                }
            )
    finally:
        doc.close()
    return rows


def tokens(text: str) -> tuple[set[str], set[str], int]:
    """Множество идентификаторов, множество чисел и число заголовков в тексте."""
    return (
        set(_RE_IDENT.findall(text)),
        set(_RE_NUMBER.findall(text)),
        len(_RE_HEADING.findall(text)),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--docling", required=True, help="pages_*.json прогона layout_tables")
    ap.add_argument("--pdf", required=True, help="тот же документ, исходный PDF")
    ap.add_argument("--out", required=True, help="куда писать постраничный jsonl")
    ap.add_argument(
        "--threshold",
        type=int,
        default=1,
        help="порог детектора: paths >= порога ⇒ страница идёт в docling (дефолт 1)",
    )
    args = ap.parse_args()

    docling = json.loads(Path(args.docling).read_text(encoding="utf-8"))
    pdf_pages = pdfium_pages(Path(args.pdf))
    if len(docling) != len(pdf_pages):
        raise SystemExit(
            f"число страниц не совпало: docling {len(docling)}, pdfium {len(pdf_pages)}"
        )

    rows: list[dict] = []
    for doc_page, pdf_page in zip(docling, pdf_pages, strict=True):
        d_idents, d_numbers, d_headings = tokens(doc_page.get("md") or "")
        p_idents, p_numbers, _ = tokens(pdf_page["text"])
        rows.append(
            {
                "page_no": doc_page["page_no"],
                "paths": pdf_page["paths"],
                "other": pdf_page["other"],
                "pdfium_chars": pdf_page["chars"],
                "docling_chars": doc_page.get("md_chars") or 0,
                "docling_tables": doc_page.get("tables") or 0,
                "docling_texts": doc_page.get("texts") or 0,
                "docling_headings": d_headings,
                "idents_docling": len(d_idents),
                "idents_pdfium": len(p_idents),
                "idents_lost": len(d_idents - p_idents),
                "numbers_docling": len(d_numbers),
                "numbers_pdfium": len(p_numbers),
                "numbers_lost": len(d_numbers - p_numbers),
                "routed": "docling" if pdf_page["paths"] >= args.threshold else "pdfium",
            }
        )

    out = Path(args.out)
    out.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )

    table_pages = [r for r in rows if r["docling_tables"] > 0]
    caught = [r for r in table_pages if r["routed"] == "docling"]
    pdfium_routed = [r for r in rows if r["routed"] == "pdfium"]
    print(f"страниц: {len(rows)}")
    print(f"страниц с таблицей у docling: {len(table_pages)}")
    print(
        f"детектор пометил из них: {len(caught)}"
        f" (recall {len(caught) / len(table_pages):.3f})" if table_pages else "нет таблиц"
    )
    print(f"ушло в pdfium: {len(pdfium_routed)}")
    if pdfium_routed:
        lost_idents = sorted(r["idents_lost"] for r in pdfium_routed)
        lost_numbers = sorted(r["numbers_lost"] for r in pdfium_routed)
        print(f"потеряно идентификаторов: медиана {statistics.median(lost_idents)}, "
              f"p90 {lost_idents[int(len(lost_idents) * 0.9)]}, макс {lost_idents[-1]}")
        print(f"потеряно чисел: медиана {statistics.median(lost_numbers)}, "
              f"p90 {lost_numbers[int(len(lost_numbers) * 0.9)]}, макс {lost_numbers[-1]}")
        worst = sorted(pdfium_routed, key=lambda r: -r["idents_lost"])[:5]
        print("худшие страницы по идентификаторам:")
        for row in worst:
            print(
                f"  стр. {row['page_no']}: идентификаторов {row['idents_docling']}"
                f"→{row['idents_pdfium']} (потеряно {row['idents_lost']}),"
                f" чисел {row['numbers_docling']}→{row['numbers_pdfium']},"
                f" заголовков {row['docling_headings']}, paths {row['paths']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
