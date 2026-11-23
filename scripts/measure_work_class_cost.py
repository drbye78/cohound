#!/usr/bin/env python3
"""Цена **класса работы** `model_free`: пик RSS, время и масштабирование по процессам.

**Вопрос, на который прибор отвечает.** Планировщик извлечения (`plan 24`) выдаёт воркеру
окно по цене **корня** — объявленному уровню документации (`workers_from_budget`,
`DOCLING_PEAK_MB`). HTML-страница внутри корня с `document_level: layout_tables` оценивается
в 3 679 MB, хотя моделей не грузит вовсе. План `24` §2.2 называет цену класса
`model_free` как «45 MB / 116 мс», но файла `2026-11-07-work-class-peaks.md` в дереве
**нет** — числа не опубликованы. Прибор снимает их и публикует вместе с условиями.

**Почему дочерний процесс на документ.** `ru_maxrss`/`VmHWM` монотонны в пределах
процесса: второй документ в том же процессе дал бы **сумму** с первым, и «пик на документ»
перестал бы быть пиком на документ (та же причина, что у `measure_extract_peak.py`).

**Почему важна не только величина пика.** Планировщику нужна **приростная** цена воркера,
а не полный RSS процесса: интерпретатор, `trafilatura` и `lxml` уже загружены в родителе и
разделяются копированием страниц при `fork`/`forkserver`. Поэтому прибор печатает и
`peak` (полный пик процесса), и `delta` (пик минус база **сразу после старта** процесса,
то есть цена самого документа). Разница между ними — то, что реально умножается на ширину
окна.

**Почему масштабирование меряется процессами, а не потоками.** Сборка CPython на этом
хосте обычная (`Py_GIL_DISABLED=0`), поэтому потоки для CPU-bound разбора бесполезны;
`--workers` строит `ProcessPoolExecutor` на `forkserver` с инициализатором (ленивая сборка
адаптеров в рабочем процессе даёт `BrokenProcessPool`, проверено).

**Другие классы — другие приборы, и это сказано, а не спрятано.** `pdf_structured` и
`office` требуют сборки docling через composition root (уровень, устройство, политика) —
их цена снимается `measure_extract_peak.py` (пик с разложением) и
`measure_extract_duration.py` (распределение времени, стратифицированная выборка).
Класс `office` **не измерен вовсе** — это названная дыра, а не ноль.

Запуск::

    # пик и время на 30 самых крупных HTML-документах durable-корпуса
    .venv/bin/python scripts/measure_work_class_cost.py \\
        --state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db --limit 30

    # плюс масштабирование по 1/2/4/6 процессам на 600 документах
    .venv/bin/python scripts/measure_work_class_cost.py --state-db … \\
        --limit 30 --scale-limit 600 --workers 1,2,4,6 --json

Код выхода: 0 — замер снят, 2 — нет входных данных (или класс не обслуживается прибором).
Нейтрален к предметной категории (§1.3): читает строки `document_version` и файлы, не зная,
о чём документы.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

#: Классы работы, цену которых снимает **этот** прибор, и указатель на прибор для прочих.
OWN_CLASSES = ("model_free",)
OTHER_CLASSES = {
    "pdf_structured": "scripts/measure_extract_peak.py / scripts/measure_extract_duration.py",
    "office": "не измерен: нужен экстрактор docling через composition root (см. те же приборы)",
}

#: Код дочернего замера: один документ — один процесс, чтобы `ru_maxrss` был пиком документа.
_CHILD = r'''
import json, resource, sys, time
from pathlib import Path

from core.adapters.chunker_family import FamilyChunker, declared_token_limiter
from core.adapters.extract_html import HtmlExtractor
from core.ports.types import RawPage

url, raw = sys.argv[1], sys.argv[2]
base_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
extractor = HtmlExtractor()
chunker = FamilyChunker(token_limiter=declared_token_limiter())
data = Path(raw).read_bytes()
started = time.perf_counter()
doc = extractor.extract(
    RawPage(
        url=url,
        content=data,
        fetched_at="2026-11-23T00:00:00+00:00",
        status_code=200,
        content_type="text/html",
    )
)
chunks = len(chunker.chunk(doc, document_id=url)) if getattr(doc, "text", None) else 0
elapsed_ms = (time.perf_counter() - started) * 1000.0
print(
    json.dumps(
        {
            "peak_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "base_kb": base_kb,
            "ms": elapsed_ms,
            "chunks": chunks,
            "bytes": len(data),
            "error": getattr(doc, "error", None),
        }
    )
)
'''

_POOL_EX = None


def _pool_init() -> None:
    """Сборка адаптеров **в рабочем процессе**: ленивая сборка ломает пул."""
    global _POOL_EX
    from core.adapters.chunker_family import FamilyChunker, declared_token_limiter
    from core.adapters.extract_html import HtmlExtractor

    _POOL_EX = (HtmlExtractor(), FamilyChunker(token_limiter=declared_token_limiter()))


def _pool_work(item: tuple[str, str]) -> tuple[float, int]:
    from core.ports.types import RawPage

    assert _POOL_EX is not None, "пул запущен без инициализатора"
    extractor, chunker = _POOL_EX
    url, raw = item
    started = time.perf_counter()
    doc = extractor.extract(
        RawPage(
            url=url,
            content=Path(raw).read_bytes(),
            fetched_at="2026-11-23T00:00:00+00:00",
            status_code=200,
            content_type="text/html",
        )
    )
    chunks = len(chunker.chunk(doc, document_id=url)) if getattr(doc, "text", None) else 0
    return time.perf_counter() - started, chunks


def _rows(state_db: Path, limit: int) -> list[tuple[str, str]]:
    """HTML-документы durable-корпуса: самые крупные первыми (по числу страниц/строк)."""
    conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            select url, raw_path from document_version
             where extractor_version like 'trafilatura%' and raw_path is not null
             order by coalesce(extract_page_count, 0) desc, rowid desc
             limit ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [(url, raw) for url, raw in rows if Path(raw).is_file()]


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def _measure(state_db: Path, limit: int) -> dict[str, Any]:
    rows = _rows(state_db, limit)
    if not rows:
        return {"documents": 0}
    results: list[dict[str, Any]] = []
    for url, raw in rows:
        proc = subprocess.run(
            [sys.executable, "-c", _CHILD, url, raw], capture_output=True, text=True
        )
        if proc.returncode != 0:
            continue
        results.append(json.loads(proc.stdout.strip().splitlines()[-1]))
    if not results:
        return {"documents": 0}
    peaks = [r["peak_kb"] / 1024.0 for r in results]
    deltas = [(r["peak_kb"] - r["base_kb"]) / 1024.0 for r in results]
    times = [r["ms"] for r in results]
    return {
        "documents": len(results),
        "peak_mb": {
            "median": round(statistics.median(peaks), 1),
            "max": round(max(peaks), 1),
        },
        "delta_mb": {
            "median": round(statistics.median(deltas), 1),
            "max": round(max(deltas), 1),
        },
        "ms": {
            "median": round(statistics.median(times), 1),
            "p90": round(_percentile(times, 0.9), 1),
            "max": round(max(times), 1),
        },
        "chunks_median": statistics.median(r["chunks"] for r in results),
        "bytes_max": max(r["bytes"] for r in results),
        "errors": sum(1 for r in results if r["error"]),
    }


def _scale(state_db: Path, limit: int, workers: list[int]) -> list[dict[str, Any]]:
    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor

    rows = _rows(state_db, limit)
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    baseline: float | None = None
    context = mp.get_context("forkserver")
    for width in workers:
        started = time.perf_counter()
        with ProcessPoolExecutor(
            max_workers=width, mp_context=context, initializer=_pool_init
        ) as pool:
            results = list(pool.map(_pool_work, rows, chunksize=8))
        elapsed = time.perf_counter() - started
        rate = len(rows) / elapsed if elapsed > 0 else 0.0
        if baseline is None:
            baseline = rate
        out.append(
            {
                "workers": width,
                "seconds": round(elapsed, 1),
                "docs_per_s": round(rate, 1),
                "speedup": round(rate / baseline, 2) if baseline else None,
                "median_ms": round(statistics.median(r[0] for r in results) * 1000.0, 1),
            }
        )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state-db", required=True, help="операторский state.db (только чтение)")
    parser.add_argument(
        "--class",
        dest="work_class",
        default="model_free",
        help=f"класс работы: {', '.join(OWN_CLASSES)}",
    )
    parser.add_argument("--limit", type=int, default=30, help="документов для пика/времени")
    parser.add_argument("--scale-limit", type=int, default=0, help="документов для масштабирования")
    parser.add_argument("--workers", default="1,2,4,6", help="ширины пула через запятую")
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    args = parser.parse_args(argv)

    if args.work_class not in OWN_CLASSES:
        pointer = OTHER_CLASSES.get(args.work_class, "неизвестный класс")
        print(
            f"класс {args.work_class!r} не обслуживается этим прибором: {pointer}",
            file=sys.stderr,
        )
        return 2

    state_db = Path(args.state_db).expanduser()
    if not state_db.is_file():
        print(f"нет файла state.db: {state_db}", file=sys.stderr)
        return 2

    payload: dict[str, Any] = {
        "class": args.work_class,
        "state_db": str(state_db),
        "single": _measure(state_db, args.limit),
    }
    if args.scale_limit > 0:
        widths = [int(w) for w in args.workers.split(",") if w.strip()]
        payload["scale"] = _scale(state_db, args.scale_limit, widths)

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    single = payload["single"]
    if not single.get("documents"):
        print("нет подходящих документов: замер не снят", file=sys.stderr)
        return 2
    print(f"класс {payload['class']}: {single['documents']} документов")
    print(
        f"  пик RSS:      медиана {single['peak_mb']['median']} MB, max {single['peak_mb']['max']} MB"
    )
    print(
        f"  прирост:      медиана {single['delta_mb']['median']} MB, max {single['delta_mb']['max']} MB"
    )
    print(
        f"  время:        медиана {single['ms']['median']} ms, p90 {single['ms']['p90']}, "
        f"max {single['ms']['max']}"
    )
    print(
        f"  чанков:       медиана {single['chunks_median']:.0f}; "
        f"крупнейший файл {single['bytes_max'] // 1024} KB; ошибок {single['errors']}"
    )
    for row in payload.get("scale") or []:
        print(
            f"  workers={row['workers']}: {row['docs_per_s']} док/с "
            f"(x{row['speedup']}), медиана {row['median_ms']} ms"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
