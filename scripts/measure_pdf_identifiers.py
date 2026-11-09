#!/usr/bin/env python3
"""Воспроизводимый замер потери идентификаторов при извлечении PDF.

Повод — план ``26`` (O34): числа «6.7 % = 147 из 2 204» (28.10.2026) и
«14.48 % (84)» (01.11.2026) жили в скриптах ``/tmp`` и умерли вместе с ними.
Этот файл возвращает замер в репозиторий и **не выдумывает** недостающие данные:
он либо считает заново на сохранённых байтах, либо честно называет, чего в
артефактах нет.

Что считается
-------------
Для каждого PDF:

* **pdfium** (механизм ``text_layer``) — текст через прод-адаптер
  :class:`core.adapters.extract_pdf_text.PdfiumDocumentExtractor`, то есть тем же
  путём, которым его читает прогон;
* **docling** (механизм ``structured``) — либо из ``extract_cache`` (сохранённый
  прод-разбор, бесплатно), либо новым прогоном адаптера (дорого, ``--docling run``).

Идентификатор — токен, который в карте становится доказательством. Два прибора,
потому что два опубликованных числа считались **разными** регексами:

* ``broad`` — ``[A-Za-z_][A-Za-z0-9_]{2,}`` (замер 01.11.2026, отчёт
  ``2026-11-01-pdf-identifier-loss.md``);
* ``strict`` — ``snake_case`` / ``camelCase`` / ``UPPER_SNAKE`` из
  ``docs/reports/measurements/2026-10-28-hybrid-quality-gate/analyze.py:40``
  (замер 28.10.2026, отчёт ``2026-10-28-hybrid-quality-gate.md``).

Классы потерь
-------------
* ``markup_escape`` — идентификатор возвращается, если снять экранирование
  ``\\_`` → ``_``. Это артефакт **нашей** сериализации markdown, а не потеря
  текста; после ``998863f`` (``DOCLING_MARKDOWN_POLICY = md-plain-underscore``)
  он в свежем тексте не возникает.
* ``separator_split`` — части токена есть, но разделены пробелом/дефисом/
  подчёркиванием (перенос строки, разрыв колонки). Проверяется шаблоном
  ``часть[\\s\\-_]+часть``.
* ``absent`` — токена нет в тексте docling вовсе. Историческая диагностика
  (01.11.2026) — текст нарисован внутри картинок/схем; **разметка областей
  docling при этом не проверялась**, поэтому класс называется «отсутствует»,
  а не «картинка» без оговорки.

Дорогие режимы (по умолчанию выключены)
---------------------------------------
* ``--legacy-escape`` — прогон docling с ``escape_underscores=True`` (поведение
  до 02.11.2026): воспроизводит класс ``markup_escape`` и число 26 на
  ``opisanie.pdf``. Стоит ~100 с и ~1.3–1.6 GB RSS на 81 странице.
* ``--ocr`` — прогон docling уровня ``full`` (OCR): проверяет, сколько
  отсутствующих идентификаторов OCR возвращает. Дороже ``layout_tables``.

Сеть не используется. Документы читаются по одному: пик RSS ограничен одним
прогоном docling.

Примеры::

    # Бесплатно: pdfium + сохранённый docling-разбор из extract_cache
    .venv/bin/python scripts/measure_pdf_identifiers.py \\
        ~/.cohound/data/corpus_raw/jatoba/98aa0699....bin --json-out /tmp/x.json

    # Воспроизвести класс экранирования (нужен docling; CPU, ~2 мин на 81 стр.)
    .venv/bin/python scripts/measure_pdf_identifiers.py DOC \\
        --legacy-escape --docling-device cpu

    # Проверить арифметику опубликованных 2 204 / 147 из сохранённых строк замера
    .venv/bin/python scripts/measure_pdf_identifiers.py --verify-published \\
        docs/reports/measurements/2026-10-28-hybrid-quality-gate/pages-large.jsonl

Дата: 2026-11-09. Разбор чисел и границы прибора — в отчёте
``docs/reports/measurements/2026-11-09-pdf-identifier-loss-repro.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import resource
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

#: Идентификатор в замере 01.11.2026: латиница, цифры, ``_``, длина ≥ 3.
_BROAD = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")

#: Идентификатор в замере 28.10.2026 (analyze.py:40): snake_case / camelCase / UPPER_SNAKE.
_STRICT = re.compile(
    r"\b(?:[a-z][a-z0-9]*(?:_[a-z0-9]+)+|[a-z]+[A-Z][A-Za-z0-9]*|[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)\b"
)

_REGEXES = {"broad": _BROAD, "strict": _STRICT}

#: Кэш экстракции: файл называется ``{sha256 байтов}.json``, внутри — конверт
#: с ``doc_content_hash``, ``extractor_version``, ``level`` и ``text``.
_DEFAULT_CACHE = Path.home() / ".cohound" / "data" / "extract_cache"


def peak_rss_mb() -> float:
    """Пик RSS процесса в MB (``ru_maxrss`` в KiB на Linux)."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def pdfium_text(path: Path) -> tuple[str, str, str | None]:
    """Текст механизмом ``text_layer`` через **прод-адаптер**, не через свой pdfium-цикл."""
    from core.adapters.extract_pdf_text import PdfiumDocumentExtractor

    extractor = PdfiumDocumentExtractor()
    doc = extractor.extract_path(path, filename_hint="document.pdf")
    return doc.text, doc.extractor_version, doc.error


def cached_docling_text(
    raw_sha256: str, cache_root: Path, level: str
) -> tuple[str, str, int | None] | None:
    """Сохранённый docling-разбор из ``extract_cache`` или ``None``.

    Ключ файла — ``sha256`` **сырых байтов**; уровень и версия проверяются по
    конверту. Версия не сверяется с текущим кодом намеренно: замер обязан
    назвать, **чем** получен текст, а не подогнать его под сегодняшний штамп.
    """
    entry = cache_root / raw_sha256[:2] / f"{raw_sha256}.json"
    try:
        envelope = json.loads(entry.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(envelope, dict):
        return None
    if envelope.get("content_hash") != raw_sha256:
        return None
    if envelope.get("level") != level:
        return None
    version = str(envelope.get("extractor_version") or "")
    if not version.startswith("docling-doc-3:"):
        return None
    pages = envelope.get("page_count")
    return str(envelope.get("text") or ""), version, pages if isinstance(pages, int) else None


def run_docling(
    path: Path,
    *,
    level: str,
    device: str,
    escape_underscores: bool,
) -> tuple[str, str, str | None]:
    """Прогон docling **прод-адаптером**, с явно выбранной политикой экспорта.

    ``escape_underscores=False`` — текущее поведение (``md-plain-underscore``).
    ``True`` — поведение до ``998863f``, нужное только для ``--legacy-escape``.
    """
    from core.adapters.extract_document import DoclingDocumentExtractor, DocumentLevel
    from core.util.sanitize import finalize_extracted_text

    extractor = DoclingDocumentExtractor(
        DocumentLevel(level), pdf_device=device
    )
    if escape_underscores:
        converter = extractor._converter or extractor._build_converter(pdf=True)  # noqa: SLF001
        extractor._converter = converter  # noqa: SLF001
        result = converter.convert(str(path))
        status = getattr(result.status, "name", str(result.status))
        if status not in ("SUCCESS", "PARTIAL_SUCCESS"):
            return "", extractor.version, f"docling: статус конвертации {status}"
        text = finalize_extracted_text(
            result.document.export_to_markdown(escape_underscores=True)
        )
        return text, extractor.version, None
    doc = extractor.extract_path(path, filename_hint="document.pdf")
    return doc.text, doc.extractor_version, doc.error


def _parts(token: str) -> list[str]:
    return [part for part in re.split(r"[_\-]", token) if len(part) >= 2]


def classify(
    pdfium_ids: set[str],
    docling_ids: set[str],
    docling_text: str,
) -> dict[str, object]:
    """Разность множеств и разбивка потерь на классы (без догадок о разметке)."""
    lost = pdfium_ids - docling_ids
    extra = docling_ids - pdfium_ids

    escaped = docling_text.replace("\\_", "_")
    if escaped != docling_text:
        after_unescape = set(_REGEXES[_ACTIVE_REGEX].findall(escaped))
        markup_escape = lost & after_unescape
    else:
        markup_escape = set()

    remaining = lost - markup_escape
    separator_split: set[str] = set()
    for token in remaining:
        parts = _parts(token)
        if len(parts) < 2:
            continue
        pattern = r"[\s\-_]+".join(re.escape(part) for part in parts)
        if re.search(pattern, docling_text):
            separator_split.add(token)

    absent = remaining - separator_split
    return {
        "lost_total": len(lost),
        "lost_pct_of_pdfium": round(100.0 * len(lost) / max(len(pdfium_ids), 1), 2),
        "markup_escape": sorted(markup_escape),
        "separator_split": sorted(separator_split),
        "absent": sorted(absent),
        "extra_in_docling": sorted(extra),
    }


def measure_document(
    path: Path,
    *,
    cache_root: Path,
    level: str,
    docling_source: str,
    device: str,
    legacy_escape: bool,
    ocr: bool,
) -> dict[str, object]:
    raw_sha = sha256_file(path)
    pdf_text, pdf_version, pdf_error = pdfium_text(path)

    docling_text = ""
    docling_version = ""
    docling_pages: int | None = None
    docling_source_used = "none"

    if docling_source == "cache":
        cached = cached_docling_text(raw_sha, cache_root, level)
        if cached is not None:
            docling_text, docling_version, docling_pages = cached
            docling_source_used = "extract_cache"
    if not docling_text and docling_source == "run":
        # ``cache`` без записи не падает: честно сообщает, что docling-стороны нет.
        docling_text, docling_version, error = run_docling(
            path, level=level, device=device, escape_underscores=False
        )
        docling_source_used = "docling_run"
        if error:
            return {
                "path": str(path),
                "raw_sha256": raw_sha,
                "error": error,
                "pdfium": {"identifiers": len(_REGEXES[_ACTIVE_REGEX].findall(pdf_text))},
            }

    if not docling_text:
        return {
            "path": str(path),
            "raw_sha256": raw_sha,
            "error": "docling-текст недоступен (нет в extract_cache; --docling run)",
            "pdfium": {
                "chars": len(pdf_text),
                "identifiers": len(_REGEXES[_ACTIVE_REGEX].findall(pdf_text)),
                "extractor_version": pdf_version,
                "error": pdf_error,
            },
        }

    pdfium_ids = set(_REGEXES[_ACTIVE_REGEX].findall(pdf_text))
    docling_ids = set(_REGEXES[_ACTIVE_REGEX].findall(docling_text))
    row: dict[str, object] = {
        "path": str(path),
        "raw_sha256": raw_sha,
        "regex": _ACTIVE_REGEX,
        "pdfium": {
            "chars": len(pdf_text),
            "identifiers": len(pdfium_ids),
            "extractor_version": pdf_version,
            "error": pdf_error,
        },
        "docling": {
            "source": docling_source_used,
            "level": level,
            "chars": len(docling_text),
            "identifiers": len(docling_ids),
            "pages": docling_pages,
            "extractor_version": docling_version,
        },
        "loss": classify(pdfium_ids, docling_ids, docling_text),
    }

    if legacy_escape:
        legacy_text, legacy_version, error = run_docling(
            path, level=level, device=device, escape_underscores=True
        )
        if error:
            row["legacy_escape"] = {"error": error}
        else:
            legacy_ids = set(_REGEXES[_ACTIVE_REGEX].findall(legacy_text))
            after = set(_REGEXES[_ACTIVE_REGEX].findall(legacy_text.replace("\\_", "_")))
            recovered = (pdfium_ids - legacy_ids) & after
            row["legacy_escape"] = {
                "docling_identifiers": len(legacy_ids),
                "backslash_underscore_occurrences": legacy_text.count("\\_"),
                "lost_before_unescape": len(pdfium_ids - legacy_ids),
                "lost_after_unescape": len(pdfium_ids - after),
                "recovered": sorted(recovered),
                "extractor_version": legacy_version,
                "peak_rss_mb": peak_rss_mb(),
            }

    if ocr:
        ocr_text, ocr_version, error = run_docling(
            path, level="full", device=device, escape_underscores=False
        )
        if error:
            row["ocr"] = {"error": error}
        else:
            ocr_ids = set(_REGEXES[_ACTIVE_REGEX].findall(ocr_text))
            absent = set(row["loss"]["absent"])  # type: ignore[index]
            row["ocr"] = {
                "level": "full",
                "identifiers": len(ocr_ids),
                "recovered": sorted(absent & ocr_ids),
                "still_absent": sorted(absent - ocr_ids),
                "extractor_version": ocr_version,
                "peak_rss_mb": peak_rss_mb(),
            }
    return row


def pdfium_per_page(path: Path) -> dict[str, object]:
    """Pdfium-сторона **методом замера 28.10.2026**: постранично, без нормализации.

    ``analyze.py`` того замера считал ``len(set(_RE_IDENT.findall(text)))`` на каждой
    странице отдельно и ``count_chars()`` для объёма. Здесь повторено ровно это, чтобы
    опубликованные 2 662 / 945 778 можно было пересчитать на сохранённых байтах.
    """
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument(str(path))
    identifiers = 0
    chars = 0
    pages = 0
    try:
        for index in range(len(doc)):
            page = doc[index]
            try:
                textpage = page.get_textpage()
                try:
                    text = textpage.get_text_range() or ""
                    chars += textpage.count_chars()
                finally:
                    textpage.close()
            finally:
                page.close()
            identifiers += len(set(_STRICT.findall(text)))
            pages += 1
    finally:
        doc.close()
    return {"pages": pages, "idents_strict_sum": identifiers, "chars_count_chars": chars}


def verify_published(jsonl_path: Path) -> dict[str, object]:
    """Пересчитать опубликованные 2 204 / 2 662 / 147 из сохранённых постраничных строк.

    Это **не** повторное извлечение: ``pages-large.jsonl`` — сохранённый результат
    замера 28.10.2026. Проверяется арифметика и то, что число — сумма по страницам,
    а не разность множеств по документу.
    """
    rows = [json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines()]
    return {
        "source": str(jsonl_path),
        "pages": len(rows),
        "idents_docling_sum": sum(int(r["idents_docling"]) for r in rows),
        "idents_pdfium_sum": sum(int(r["idents_pdfium"]) for r in rows),
        "idents_lost_sum": sum(int(r["idents_lost"]) for r in rows),
        "note": (
            "сумма постраничных множеств; идентификатор, повторённый на двух страницах, "
            "считается дважды, поэтому это не мощность множества по документу"
        ),
    }


_ACTIVE_REGEX = "broad"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdfs", nargs="*", type=Path, help="PDF-файлы (retained-байты)")
    parser.add_argument(
        "--regex", choices=tuple(_REGEXES), default="broad",
        help="прибор идентификаторов: broad (01.11) или strict (28.10)",
    )
    parser.add_argument(
        "--cache-root", type=Path, default=_DEFAULT_CACHE,
        help="корень extract_cache (по умолчанию ~/.cohound/data/extract_cache)",
    )
    parser.add_argument(
        "--docling", choices=("cache", "run"), default="cache",
        help="откуда брать docling-текст: cache (сохранённый) или run (новый прогон)",
    )
    parser.add_argument("--level", default="layout_tables", help="уровень docling")
    parser.add_argument(
        "--docling-device", choices=("cpu", "cuda"), default="cpu",
        help="устройство docling для дорогих режимов (--legacy-escape/--ocr)",
    )
    parser.add_argument(
        "--legacy-escape", action="store_true",
        help="прогон docling с escape_underscores=True (поведение до 02.11.2026)",
    )
    parser.add_argument(
        "--ocr", action="store_true",
        help="прогон docling уровня full и подсчёт возвращённых OCR идентификаторов",
    )
    parser.add_argument("--json-out", type=Path, help="куда записать JSON")
    parser.add_argument(
        "--verify-published", type=Path, metavar="JSONL",
        help="пересчитать 2204/2662/147 из pages-large.jsonl (без извлечения)",
    )
    parser.add_argument(
        "--pdfium-per-page", action="store_true",
        help="pdfium методом 28.10: постраничные суммы strict-идентификаторов и count_chars()",
    )
    args = parser.parse_args()

    global _ACTIVE_REGEX
    _ACTIVE_REGEX = args.regex

    if args.verify_published:
        payload: dict[str, object] = {
            "script": "scripts/measure_pdf_identifiers.py",
            "date": "2026-11-09",
            "verify_published": verify_published(args.verify_published),
        }
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        print(text)
        if args.json_out:
            args.json_out.write_text(text + "\n", encoding="utf-8")
        return 0

    if not args.pdfs:
        parser.error("нужен хотя бы один PDF, --verify-published или --pdfium-per-page")

    if args.pdfium_per_page:
        rows = [
            {"path": str(pdf), **pdfium_per_page(pdf)}
            for pdf in args.pdfs
            if pdf.is_file()
        ]
        payload = {
            "script": "scripts/measure_pdf_identifiers.py",
            "date": "2026-11-09",
            "method": "pdfium per page, analyze.py 28.10.2026",
            "documents": rows,
            "peak_rss_mb": peak_rss_mb(),
        }
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        print(text)
        if args.json_out:
            args.json_out.write_text(text + "\n", encoding="utf-8")
        return 0

    documents = []
    for pdf in args.pdfs:
        if not pdf.is_file():
            print(f"нет файла: {pdf}", file=sys.stderr)
            continue
        print(f"== {pdf.name} ({pdf.stat().st_size} байт)", flush=True)
        row = measure_document(
            pdf,
            cache_root=args.cache_root,
            level=args.level,
            docling_source=args.docling,
            device=args.docling_device,
            legacy_escape=args.legacy_escape,
            ocr=args.ocr,
        )
        documents.append(row)
        if "loss" in row:
            loss = row["loss"]
            print(
                f"   pdfium={row['pdfium']['identifiers']} "  # type: ignore[index]
                f"docling={row['docling']['identifiers']} "  # type: ignore[index]
                f"потеряно={loss['lost_total']} ({loss['lost_pct_of_pdfium']} %) "
                f"escape={len(loss['markup_escape'])} "
                f"split={len(loss['separator_split'])} "
                f"absent={len(loss['absent'])} "
                f"лишних={len(loss['extra_in_docling'])}",
                flush=True,
            )
        else:
            print(f"   {row.get('error')}", flush=True)

    payload = {
        "script": "scripts/measure_pdf_identifiers.py",
        "date": "2026-11-09",
        "regex": args.regex,
        "docling_source": args.docling,
        "level": args.level,
        "documents": documents,
        "peak_rss_mb": peak_rss_mb(),
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.json_out:
        args.json_out.write_text(text + "\n", encoding="utf-8")
        print(f"JSON: {args.json_out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
