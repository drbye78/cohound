#!/usr/bin/env python3
"""Цена OCR **одной** страницы и сохранение её номера при частичной конвертации (O35, план 26).

**Два вопроса, на которые прибор отвечает, и почему они вместе.** Остаток O35 — это два
факта, без которых page-level OCR не проектируется:

1. **Сколько стоит одна страница.** Не «сколько стоит документ» (это O30), а именно цена
   страницы: по ней решается, можно ли читать сканы постранично и сколько воркеров держать.
2. **Сохраняется ли номер страницы**, если отдать docling **только** эту страницу. Если
   ``page_range=(N, N)`` возвращает провенанс с номером ``1``, то постраничное чтение теряет
   адрес доказательства — и тогда механизм «прочитать одну страницу» непригоден, каким бы
   дешёвым он ни был.

**Почему отдельный процесс на замер.** ``ru_maxrss`` монотонен внутри процесса: второй замер
в том же процессе дал бы **сумму** с первым, и «цена страницы» перестала бы быть ценой
страницы. Поэтому каждая страница меряется дочерним процессом (тот же приём, что в
``measure_extract_peak.py``), а прогрев (загрузка весов docling) меряется отдельно и в цену
страницы **не** входит: в проде воркер прогревается один раз.

**Чего прибор не мерит.** Качество текста страницы (O38/O40) и долю страниц без слоя по
корпусу (это O26 и замер 09.11.2026). Здесь только цена и адрес.

Запуск::

    .venv/bin/python scripts/measure_page_ocr.py --pdf /path/scan.pdf --pages 3,40 \
      --level full --device cuda --out-json /tmp/page_ocr.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
_CHILD_PREFIX = "CHILD_JSON:"


def _rss_mb() -> float | None:
    """Текущий RSS процесса, МиБ (``/proc/self/status:VmRSS``)."""
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        return None
    return None


def _hwm_mb() -> float | None:
    """Пик RSS процесса с начала жизни, МиБ (``VmHWM``) — потолок, а не текущее."""
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        return None
    return None


def _build_extractor(level: str, device: str) -> Any:
    """Экстрактор того же класса, что в проде, — иначе мерилась бы другая цена."""
    from core.adapters.extract_document import DoclingDocumentExtractor
    from core.domain.enums import DocumentLevel, PdfDevice

    return DoclingDocumentExtractor(DocumentLevel(level), pdf_device=PdfDevice(device))


def _prov_pages(document: Any) -> list[int]:
    """Номера страниц, которые docling приписал **содержимому** (1-based, как в PDF).

    Источник — провенанс элементов документа, а не ``document.pages``: ``pages`` — это
    «сколько страниц сконвертировано», и при ``page_range=(3, 3)`` он может содержать одну
    запись с ключом ``1``. Вопрос O35 ровно в том, что видит **элемент**: если страница
    текста помечена номером 1 вместо 3, адрес доказательства потерян, даже когда конвертация
    удалась.
    """
    pages: set[int] = set()
    for item in document.iterate_items():
        node = item[0] if isinstance(item, tuple) else item
        for prov in getattr(node, "prov", None) or ():
            pages.add(int(prov.page_no))
    return sorted(pages)


def _run_child(args: argparse.Namespace) -> dict[str, Any]:
    """Один дочерний замер: прогрев, затем конвертация объявленной страницы."""
    extractor = _build_extractor(args.level, args.device)
    floor_mb = _rss_mb()
    # Конвертер строится **до** замера: загрузка весов — цена воркера, а не страницы.
    converter = extractor._build_converter(pdf=True)  # noqa: SLF001 — иного входа нет

    warmup_started = time.perf_counter()
    converter.convert(str(args.pdf), page_range=(1, 1))
    warmup_seconds = round(time.perf_counter() - warmup_started, 3)
    warm_floor_mb = _rss_mb()

    started = time.perf_counter()
    result = converter.convert(str(args.pdf), page_range=(args.page, args.page))
    elapsed = round(time.perf_counter() - started, 3)
    document = result.document
    text = document.export_to_markdown()
    return {
        "pdf": str(args.pdf),
        "page": args.page,
        "level": args.level,
        "device": args.device,
        "seconds": elapsed,
        "warmup_seconds": warmup_seconds,
        "floor_rss_mb": floor_mb,
        "warm_floor_rss_mb": warm_floor_mb,
        "peak_rss_mb": _hwm_mb(),
        "chars": len(text),
        "tables": len(document.tables or ()),
        "prov_pages": _prov_pages(document),
        "page_number_preserved": _prov_pages(document) == [args.page],
        "pages_in_result": len(result.pages),
        "error": None,
    }


def _measure_page(
    pdf: Path, page: int, *, level: str, device: str, timeout: float
) -> dict[str, Any]:
    """Дочерний процесс на страницу: ``ru_maxrss`` ребёнка — цена именно этой страницы."""
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--pdf",
        str(pdf),
        "--page",
        str(page),
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
        return {"pdf": str(pdf), "page": page, "error": f"timeout {timeout} s"}
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
            "pdf": str(pdf),
            "page": page,
            "error": f"дочерний процесс не дал замера (exit {completed.returncode})",
            "stderr_tail": completed.stderr[-2000:],
        }
    payload["wall_seconds"] = wall
    return payload


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Цена OCR одной страницы и сохранение номера (O35)"
    )
    parser.add_argument("--pdf", required=True, help="PDF с retained-байтами")
    parser.add_argument(
        "--pages",
        default="1",
        help="номера страниц через запятую (1-based); каждая меряется своим процессом",
    )
    parser.add_argument("--level", default="full", help="уровень docling")
    parser.add_argument("--device", default="cpu", choices=("cpu", "cuda"))
    parser.add_argument(
        "--timeout", type=float, default=1800.0, help="таймаут одного замера, с"
    )
    parser.add_argument("--out-json", metavar="PATH", help="полный отчёт")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--page", type=int, default=1, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def _pages(raw: str) -> list[int]:
    """Номера страниц через запятую; ``0`` и мусор — отказ, а не тихий пропуск."""
    values: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError as exc:
            raise SystemExit(f"measure: --pages: {item!r} — не число") from exc
        if value < 1:
            raise SystemExit(f"measure: --pages: {value} < 1 (страницы 1-based)")
        if value not in values:
            values.append(value)
    if not values:
        raise SystemExit("measure: --pages пуст")
    return values


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.child:
        result = _run_child(args)
        print(_CHILD_PREFIX + json.dumps(result, ensure_ascii=False))
        return 0

    pdf = Path(args.pdf).expanduser().resolve()
    if not pdf.is_file():
        print(f"measure: файла нет: {pdf}", file=sys.stderr)
        return 2
    pages = _pages(args.pages)
    rows: list[dict[str, Any]] = []
    for page in pages:
        print(f"страница {page} …", flush=True)
        row = _measure_page(
            pdf, page, level=args.level, device=args.device, timeout=args.timeout
        )
        rows.append(row)
        if row.get("error"):
            print(f"  ОШИБКА: {row['error']}", flush=True)
            continue
        preserved = "номер сохранён" if row["page_number_preserved"] else (
            f"НОМЕР НЕ СОХРАНЁН (prov={row['prov_pages']})"
        )
        print(
            f"  {row['seconds']:.2f} с, {row['chars']} симв, пик {row['peak_rss_mb']:.0f} MiB, "
            f"таблиц {row['tables']}, {preserved}",
            flush=True,
        )
    ok = [row for row in rows if not row.get("error")]
    payload = {
        "instrument": "scripts/measure_page_ocr.py",
        "conditions": {
            "pdf": str(pdf),
            "level": args.level,
            "device": args.device,
            "pages": pages,
        },
        "summary": {
            "measured": len(ok),
            "failed": len(rows) - len(ok),
            "page_number_preserved": all(row["page_number_preserved"] for row in ok)
            if ok
            else None,
            "seconds": [row["seconds"] for row in ok],
            "peak_rss_mb": [row["peak_rss_mb"] for row in ok],
        },
        "pages": rows,
    }
    if args.out_json:
        Path(args.out_json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        print(f"отчёт: {args.out_json}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
