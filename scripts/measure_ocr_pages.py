"""Стоит ли OCR для страниц без текстового слоя: рендер pdfium + OCR, замер и выход.

Проверяет три вещи сразу:
1. что физически на странице (доля «чернил» — скан с текстом, рисунок или пусто);
2. даёт ли OCR на ней текст (символы, время, память);
3. сколько стоит рендер страницы pdfium (цена входа в OCR).

Один движок OCR — один процесс: ``ru_maxrss`` монотонен, мешать движки нельзя.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time

sys.path.insert(0, "/home/roger/src/cohound")

import pypdfium2 as pdfium  # noqa: E402


def peak_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def ink_ratio(page: pdfium.PdfPage, scale: float = 1.0) -> tuple[float, tuple[int, int]]:
    """Доля непустых (не белых) пикселей и размер изображения."""
    bitmap = page.render(scale=scale)
    image = bitmap.to_pil().convert("L")
    pixels = image.getdata()
    total = len(pixels)
    # пиксель считается «чернилами», если темнее 240 (серый фон/антиалиасинг не в счёт)
    dark = sum(1 for value in pixels if value < 240)
    return round(dark / total, 4), image.size


def empty_pages_of(path: str, threshold: int = 40) -> list[int]:
    pdf = pdfium.PdfDocument(path)
    pages = []
    for index in range(len(pdf)):
        textpage = pdf[index].get_textpage()
        text = textpage.get_text_bounded()
        textpage.close()
        if len(text.strip()) < threshold:
            pages.append(index + 1)
    pdf.close()
    return pages


def ocr_rapidocr(image) -> str:
    """OCR через rapidocr (тот же движок, что использует docling в уровне full)."""
    from rapidocr import RapidOCR

    engine = RapidOCR()
    result = engine(image)
    # rapidocr 3.x: объект с .txts / .boxes; 1.x: кортеж (boxes, text, scores)
    texts = getattr(result, "txts", None)
    if texts is None and isinstance(result, (tuple, list)) and len(result) >= 2:
        texts = result[1]
    if not texts:
        return ""
    return "\n".join(str(t) for t in texts)


def ocr_tesseract(image) -> str:
    """OCR через системный tesseract (rus+eng)."""
    import subprocess
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as handle:
        image.save(handle.name)
        tmp = handle.name
    try:
        out = subprocess.run(
            ["tesseract", tmp, "stdout", "-l", "rus+eng", "--psm", "3"],
            capture_output=True, text=True, timeout=300,
        )
        return out.stdout
    finally:
        os.unlink(tmp)


ENGINES = {"rapidocr": ocr_rapidocr, "tesseract": ocr_tesseract}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--engine", required=True, choices=tuple(ENGINES))
    parser.add_argument("--pages", default="", help="номера страниц через запятую; пусто = все пустые")
    parser.add_argument("--scale", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--json-out")
    args = parser.parse_args()

    pages = ([int(x) for x in args.pages.split(",")] if args.pages
             else empty_pages_of(args.path)[: args.limit])

    pdf = pdfium.PdfDocument(args.path)
    render_total = ocr_total = 0.0
    rows = []
    for number in pages:
        page = pdf[number - 1]
        started = time.time()
        ratio, size = ink_ratio(page, args.scale)
        render_s = time.time() - started
        render_total += render_s

        bitmap = page.render(scale=args.scale)
        image = bitmap.to_pil()
        started = time.time()
        try:
            text = ENGINES[args.engine](image)
            error = None
        except Exception as exc:  # noqa: BLE001 - замер обязан пережить отказ движка
            text, error = "", f"{type(exc).__name__}: {exc}"
        ocr_s = time.time() - started
        ocr_total += ocr_s

        rows.append({
            "page": number, "ink_ratio": ratio, "image": list(size),
            "render_s": round(render_s, 3), "ocr_s": round(ocr_s, 2),
            "chars": len(text.strip()), "error": error,
            "head": " ".join(text.split())[:160],
        })
        print(f"стр.{number:5} чернил={ratio:.3f} {size[0]}x{size[1]} "
              f"рендер={render_s:.2f}s OCR={ocr_s:6.2f}s символов={len(text.strip()):5} "
              f"{'ОШИБКА: ' + error if error else repr(rows[-1]['head'][:70])}", flush=True)
        page.close()
    pdf.close()

    summary = {
        "engine": args.engine, "pages": len(rows), "scale": args.scale,
        "render_s_total": round(render_total, 2),
        "ocr_s_total": round(ocr_total, 2),
        "chars_total": sum(r["chars"] for r in rows),
        "pages_with_text": sum(1 for r in rows if r["chars"] >= 40),
        "pages_low_ink": sum(1 for r in rows if r["ink_ratio"] < 0.01),
        "peak_rss_mb": peak_mb(),
        "rows": rows,
    }
    print("\n=== ИТОГО ===")
    for key in ("engine", "pages", "render_s_total", "ocr_s_total", "chars_total",
                "pages_with_text", "pages_low_ink", "peak_rss_mb"):
        print(f"  {key:18} {summary[key]}")
    if args.json_out:
        with open(args.json_out, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(summary, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
