#!/usr/bin/env python3
"""Замер PDF-бэкендов: память, время, качество текста на одном и том же документе.

Один бэкенд — один процесс: сравнивать пик RSS двух бэкендов в одном процессе нельзя,
``ru_maxrss`` монотонен по построению (см. ``docs/reports/measurements/
2026-10-25-docling-memory-and-streaming.md``, §4 — там эта ошибка разобрана).

Режимы:

``memory``   — пик RSS и время конвертации документа целиком;
``blocks``   — нарезка ``page_range`` по блокам, блок = отдельный процесс;
``quality``  — постраничное сравнение текста с эталоном (docling) по токенам и числам;
``page``     — печать одной страницы из каждого бэкенда для глазами-проверки.

Примеры::

    python3 scripts/measure_pdf_backends.py memory  DOC --backend pypdfium2
    python3 scripts/measure_pdf_backends.py memory  DOC --backend docling --level layout_tables
    python3 scripts/measure_pdf_backends.py blocks  DOC --backend docling --block 50 --start 1 --end 50
    python3 scripts/measure_pdf_backends.py quality DOC --pages 40,120,544 --reference docling
    python3 scripts/measure_pdf_backends.py page    DOC --page 544 --backend pypdfium2

Дата — 2026-10-25, повод — выбор оси ``pdf_backend`` (см. отчёт о замерах).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import resource
import subprocess
import sys
import time

REPO = "/home/roger/src/cohound"

#: Управляющие символы, которые PDF-парсеры вставляют в местах переноса.
#: ``\x02`` (STX) в docling-parse/pypdfium2 стоит **внутри слова** как мягкий перенос:
#: ``Post\x02greSQL``. Удаление даёт ``PostgreSQL``. Проверено контекстами на стр. 3/18/19
#: 665-страничного документа (269 вхождений на 60 страниц, всегда без пробела вокруг).
_CTRL = re.compile(r"[\x00-\x08\x0b\x0e-\x1f\x7f]")

_TOKEN = re.compile(r"[\w\u0400-\u04FF]+")
_HEAD_NUM = re.compile(r"^\s*\d+(?:\.\d+)*\.?\s+\S")
_MD_HEAD = re.compile(r"^#{1,6} \S")
_SQL = re.compile(r"\b(SELECT|INSERT|UPDATE|WHERE|FROM|CREATE|ALTER)\b")

BACKENDS = ("docling", "pypdfium2", "pypdf", "pdfplumber")


def peak_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def clean(text: str) -> str:
    """Убирает управляющие символы PDF (мягкие переносы внутри слов)."""
    return _CTRL.sub("", text)


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", clean(text)).strip()


def tokens(text: str) -> list[str]:
    return _TOKEN.findall(clean(text))


def page_count(path: str) -> int:
    try:
        out = subprocess.run(["pdfinfo", path], capture_output=True, text=True, timeout=120).stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return 0
    for line in out.splitlines():
        if line.startswith("Pages:"):
            return int(line.split()[-1])
    return 0


# --- docling -----------------------------------------------------------------


def docling_extractor(level: str, timeout: float | None):
    sys.path.insert(0, REPO)
    from core.adapters.extract_document import DoclingDocumentExtractor, DocumentLevel

    return DoclingDocumentExtractor(DocumentLevel(level), document_timeout=timeout)


def docling_convert(extractor, path: str, page_range=None):
    converter = extractor._converter or extractor._build_converter(pdf=True)  # noqa: SLF001
    extractor._converter = converter  # noqa: SLF001
    kwargs: dict[str, object] = {"raises_on_error": False}
    if page_range is not None:
        kwargs["page_range"] = page_range
    result = converter.convert(path, **kwargs)
    status = getattr(result.status, "name", str(result.status))
    md = result.document.export_to_markdown() if status in ("SUCCESS", "PARTIAL_SUCCESS") else ""
    return status, md


# --- безмодельные бэкенды ----------------------------------------------------


def pypdfium2_text(path: str, page_range=None) -> str:
    import pypdfium2 as pdfium

    pdf = pdfium.PdfDocument(path)
    total = len(pdf)
    first, last = (1, total) if page_range is None else page_range
    parts = []
    for idx in range(first - 1, min(last, total)):
        page = pdf[idx]
        textpage = page.get_textpage()
        parts.append(textpage.get_text_bounded())
        textpage.close()
        page.close()
    pdf.close()
    return "\n\n".join(parts)


def pypdf_text(path: str, page_range=None) -> str:
    from pypdf import PdfReader

    reader = PdfReader(path)
    first, last = (1, len(reader.pages)) if page_range is None else page_range
    return "\n\n".join(
        reader.pages[i].extract_text() or "" for i in range(first - 1, min(last, len(reader.pages)))
    )


def pdfplumber_text(path: str, page_range=None) -> str:
    import pdfplumber

    parts = []
    with pdfplumber.open(path) as pdf:
        first, last = (1, len(pdf.pages)) if page_range is None else page_range
        for page in pdf.pages[first - 1:min(last, len(pdf.pages))]:
            parts.append(page.extract_text() or "")
            for table in page.extract_tables() or []:
                parts.append("\n".join(" | ".join(c or "" for c in row) for row in table))
            page.flush_cache()
    return "\n\n".join(parts)


FAST = {"pypdfium2": pypdfium2_text, "pypdf": pypdf_text, "pdfplumber": pdfplumber_text}


def extract(backend: str, path: str, page_range=None, level: str = "layout_tables",
            timeout: float | None = None):
    """Возвращает (status, text). Единая точка входа для всех режимов."""
    if backend == "docling":
        return docling_convert(docling_extractor(level, timeout), path, page_range)
    try:
        return "SUCCESS", FAST[backend](path, page_range)
    except BaseException as exc:  # noqa: BLE001 - замер обязан пережить отказ бэкенда
        return f"ERROR:{type(exc).__name__}", ""


# --- режимы ------------------------------------------------------------------


def mode_memory(args) -> dict:
    started = time.time()
    status, text = extract(args.backend, args.path, None, args.level, args.timeout_s)
    return {
        "mode": "memory", "backend": args.backend, "level": args.level,
        "pages": page_count(args.path), "status": status,
        "chars": len(text), "md_headings": len(_MD_HEAD.findall(text)),
        "num_headings": sum(1 for line in text.splitlines() if _HEAD_NUM.match(line)),
        "sql": len(_SQL.findall(text)),
        "elapsed_s": round(time.time() - started, 1), "peak_rss_mb": peak_mb(),
        "sha16": hashlib.sha256(norm(text).encode()).hexdigest()[:16],
    }


def mode_blocks(args) -> dict:
    started = time.time()
    status, text = extract(args.backend, args.path, (args.start, args.end), args.level,
                           args.timeout_s)
    return {
        "mode": "blocks", "backend": args.backend, "level": args.level,
        "range": [args.start, args.end], "status": status, "chars": len(text),
        "elapsed_s": round(time.time() - started, 1), "peak_rss_mb": peak_mb(),
        "sha16": hashlib.sha256(norm(text).encode()).hexdigest()[:16],
    }


def mode_page(args) -> dict:
    status, text = extract(args.backend, args.path, (args.page, args.page), args.level,
                           args.timeout_s)
    print(f"===== {args.backend} стр.{args.page}: {len(text)} символов, status={status}")
    print(clean(text)[: args.max_chars])
    return {"mode": "page", "backend": args.backend, "page": args.page,
            "chars": len(text), "status": status}


def _compare(reference: str, candidate: str, page: int) -> dict:
    ref_tokens, cand_tokens = tokens(reference), tokens(candidate)
    ref_set, cand_set = set(ref_tokens), set(cand_tokens)
    ref_nums = {t for t in ref_tokens if any(c.isdigit() for c in t)}
    cand_nums = {t for t in cand_tokens if any(c.isdigit() for c in t)}
    return {
        "page": page,
        "reference": {"chars": len(reference), "tokens": len(ref_tokens),
                      "md_headings": len(_MD_HEAD.findall(reference)),
                      "num_headings": sum(1 for line in reference.splitlines()
                                          if _HEAD_NUM.match(line)),
                      "sql": len(_SQL.findall(reference))},
        "candidate": {"chars": len(candidate), "tokens": len(cand_tokens),
                      "md_headings": len(_MD_HEAD.findall(candidate)),
                      "num_headings": sum(1 for line in candidate.splitlines()
                                          if _HEAD_NUM.match(line)),
                      "sql": len(_SQL.findall(candidate))},
        # доля токенов эталона, найденных у кандидата: 1.0 = ничего не потеряно
        "token_coverage": round(len(ref_set & cand_set) / max(len(ref_set), 1), 4),
        "numbers_lost": len(ref_nums - cand_nums),
        "numbers_extra": len(cand_nums - ref_nums),
        "only_reference": sorted(ref_set - cand_set)[:10],
        "only_candidate": sorted(cand_set - ref_set)[:10],
        "identical": norm(reference) == norm(candidate),
    }


def mode_quality(args) -> dict:
    pages = [int(x) for x in args.pages.split(",")]
    rows = []
    for page in pages:
        ref_status, reference = extract(args.reference, args.path, (page, page),
                                        args.reference_level, args.timeout_s)
        cand_status, candidate = extract(args.backend, args.path, (page, page),
                                         args.level, args.timeout_s)
        row = _compare(reference, candidate, page)
        row["status"] = {"reference": ref_status, "candidate": cand_status}
        rows.append(row)
        print(f"стр.{page:5} coverage={row['token_coverage']:.3f} "
              f"{args.reference}={row['reference']['chars']:6}ch "
              f"{args.backend}={row['candidate']['chars']:6}ch "
              f"чисел потеряно={row['numbers_lost']:3} лишних={row['numbers_extra']:3} "
              f"идентично={row['identical']}", flush=True)
    total = {
        "mode": "quality", "backend": args.backend, "reference": args.reference,
        "pages": len(rows),
        "mean_coverage": round(sum(r["token_coverage"] for r in rows) / max(len(rows), 1), 4),
        "numbers_lost": sum(r["numbers_lost"] for r in rows),
        "numbers_extra": sum(r["numbers_extra"] for r in rows),
        "identical_pages": sum(1 for r in rows if r["identical"]),
        "reference_chars": sum(r["reference"]["chars"] for r in rows),
        "candidate_chars": sum(r["candidate"]["chars"] for r in rows),
        "reference_md_headings": sum(r["reference"]["md_headings"] for r in rows),
        "candidate_num_headings": sum(r["candidate"]["num_headings"] for r in rows),
    }
    print("\n=== ИТОГО ===")
    for key, value in total.items():
        print(f"  {key:24} {value}")
    return {"total": total, "pages": rows}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("memory", "blocks", "quality", "page"))
    parser.add_argument("path")
    parser.add_argument("--backend", default="pypdfium2", choices=BACKENDS)
    parser.add_argument("--level", default="layout_tables")
    parser.add_argument("--timeout-s", type=float, default=None)
    parser.add_argument("--block", type=int, default=50)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=50)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--pages", default="40,120,544")
    parser.add_argument("--reference", default="docling", choices=BACKENDS)
    parser.add_argument("--reference-level", default="layout_tables")
    parser.add_argument("--max-chars", type=int, default=2500)
    parser.add_argument("--json-out")
    args = parser.parse_args()

    handler = {"memory": mode_memory, "blocks": mode_blocks,
               "quality": mode_quality, "page": mode_page}[args.mode]
    result = handler(args)
    result["_process"] = {"peak_rss_mb": peak_mb()}
    if args.mode in ("memory", "blocks"):
        print(json.dumps(result, ensure_ascii=False))
    if args.json_out:
        with open(args.json_out, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
