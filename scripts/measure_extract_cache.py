#!/usr/bin/env python3
"""Цена и полезность ``extract_cache``: сканирование, вытеснение, hit-rate (O32, план 26).

Три вопроса, на которые прибор отвечает числами, а не оценками:

1. **Сколько стоит полный обход** кэша (``rglob("*.json")`` + ``stat`` + сортировка) —
   ровно то, что ``ExtractionCache._evict_if_needed`` делает **на каждом** ``put`` сверх
   бюджета. Индекса нет, поэтому цена линейна по числу файлов.
2. **Сколько стоит ``put`` при включённом бюджете** против ``budget=None`` (сегодняшний
   дефолт: вытеснение не исполняется никогда). Меряется на песочнице, чтобы не трогать
   рабочий кэш: удаление записей — необратимая операция, и мерить её на стенде значило бы
   платить кэшем за число.
3. **Каков hit-rate сейчас** — сколько документов корпуса обслужит кэш при текущих штампах
   экстракторов. Считается по ``state.db:document_version`` (``content_hash`` +
   ``extractor_version`` + ``level`` — тот же ключ, что у кэша), а не по прогону: прогон
   на этом хосте — решение владельца, а вопрос «есть ли работа» от него не зависит.

Запуск::

    .venv/bin/python scripts/measure_extract_cache.py
    .venv/bin/python scripts/measure_extract_cache.py --bench-files 1000 10000 --repeats 3
    .venv/bin/python scripts/measure_extract_cache.py --state-db ~/.cohound/.../state.db

Код выхода: 0 — замер снят, 2 — не найден корень кэша. Нейтрален к предметной категории
(§1.3): читает файлы кэша и строки ``document_version``, не зная, о чём документы.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.pipeline.extract_cache import ExtractionCache  # noqa: E402


def _percentile(values: list[int], q: float) -> int:
    """Квантиль по ближайшему рангу (для «p95 размера записи» точности хватает)."""
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
    return ordered[index]


def _inventory(root: Path) -> dict[str, Any]:
    """Файлы кэша: число, суммарный размер, распределение размеров."""
    sizes: list[int] = []
    started = time.perf_counter()
    for path in root.rglob("*.json"):
        try:
            sizes.append(path.stat().st_size)
        except OSError:
            continue
    return {
        "files": len(sizes),
        "bytes": sum(sizes),
        "p50_bytes": _percentile(sizes, 0.50),
        "p95_bytes": _percentile(sizes, 0.95),
        "p99_bytes": _percentile(sizes, 0.99),
        "max_bytes": max(sizes) if sizes else 0,
        "inventory_seconds": round(time.perf_counter() - started, 3),
    }


def _scan_cost(root: Path, repeats: int) -> dict[str, Any]:
    """Цена полного обхода — без удаления (меряется только чтение метаданных)."""
    samples: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        entries: list[tuple[int, str, int]] = []
        for path in root.rglob("*.json"):
            try:
                stat = path.stat()
            except OSError:
                continue
            entries.append((stat.st_mtime_ns, str(path), stat.st_size))
        entries.sort(key=lambda row: (row[0], row[1]))
        samples.append(time.perf_counter() - started)
    return {
        "repeats": repeats,
        "median_seconds": round(statistics.median(samples), 4),
        "min_seconds": round(min(samples), 4),
        "max_seconds": round(max(samples), 4),
    }


def _sandbox(count: int, entry_bytes: int = 5169) -> Path:
    """Песочница с ``count`` записями вида ``<hash>.json`` (структура как у кэша).

    ``entry_bytes`` — медианный размер записи реального кэша (замер 15.11.2026:
    p50 = 5 169 байт). Размер важен: порог проверки бюджета считается **в байтах**,
    поэтому песочница из 120-байтных записей мерила бы не тот режим.
    """
    root = Path(tempfile.mkdtemp(prefix=f"extract-cache-bench-{count}-"))
    payload = json.dumps(
        {"format": 1, "text": "x" * max(1, entry_bytes - 40)}, separators=(",", ":")
    )
    for index in range(count):
        name = f"{index:064x}"
        target = root / name[:2] / f"{name}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
    return root


def _put_cost(root: Path, budget: int | None, repeats: int) -> dict[str, Any]:
    """Цена ``put`` на песочнице: с бюджетом (проверка + вытеснение) и без него.

    Пишутся записи **медианного** размера реального кэша, поэтому порог проверки
    (1/16 бюджета) закрывается за столько же записей, за сколько на настоящем кэше.
    Возвращаются и медиана записи, и полное время прогона: амортизация видна не в
    медиане (она почти всегда «без обхода»), а в числе обходов ``scans``.
    """
    from core.ports.types import ExtractedDoc  # локально: прибор не тянет порт зря

    cache = ExtractionCache(root, max_bytes=budget)
    samples: list[float] = []
    started_all = time.perf_counter()
    for index in range(repeats):
        doc = ExtractedDoc(
            text=f"текст замера {index} " + "x" * 5000,
            content_hash=f"{index:064x}",
            extractor_version="bench-1",
            page_count=1,
            family="text",
        )
        started = time.perf_counter()
        cache.put(doc, level="layout_tables")
        samples.append(time.perf_counter() - started)
    total = time.perf_counter() - started_all
    stats = cache.stats()
    return {
        "budget": budget,
        "repeats": repeats,
        "median_seconds": round(statistics.median(samples), 4),
        "max_seconds": round(max(samples), 4),
        "total_seconds": round(total, 3),
        "evicted": stats["evicted"],
        "writes": stats["writes"],
        "scans": stats["scans"],
    }


def _cache_index(root: Path) -> dict[tuple[str, str, str], str]:
    """``(doc_content_hash, extractor_version, level) -> путь`` по конвертам кэша.

    Ключ кэша — хеш **байтов** (``content_hash``), а ``document_version.content_hash`` —
    хеш **чистого текста** (I19, ``doc_id = product_id:content_hash``). Сопоставлять их
    напрямую нельзя: это разные величины, и первый замер по ошибке дал 0 попаданий на
    17 874 документах. Поэтому индекс строится по ``doc_content_hash`` — тому полю
    конверта, которое и есть хеш извлечённого текста.
    """
    index: dict[tuple[str, str, str], str] = {}
    for path in root.rglob("*.json"):
        try:
            with path.open(encoding="utf-8") as handle:
                envelope = json.load(handle)
        except (OSError, ValueError):
            continue
        if not isinstance(envelope, dict):
            continue
        key = (
            str(envelope.get("doc_content_hash") or ""),
            str(envelope.get("extractor_version") or ""),
            str(envelope.get("level") or ""),
        )
        if all(key):
            index[key] = str(path)
    return index


def _hit_rate(state_db: Path, cache_index: dict[tuple[str, str, str], str]) -> dict[str, Any]:
    """Сколько записей ``document_version`` обслужит кэш при текущих штампах."""
    connection = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT extractor_version, level, content_hash FROM document_version"
        ).fetchall()
    finally:
        connection.close()
    total = len(rows)
    hits = 0
    by_stamp: dict[str, list[int]] = {}
    for extractor_version, level, content_hash in rows:
        key = (str(content_hash), str(extractor_version), str(level))
        hit = key in cache_index
        hits += 1 if hit else 0
        bucket = by_stamp.setdefault(str(extractor_version), [0, 0])
        bucket[0] += 1
        bucket[1] += 1 if hit else 0
    return {
        "documents": total,
        "hits": hits,
        "hit_rate": round(hits / total, 4) if total else None,
        "by_stamp": {
            stamp: {"documents": counts[0], "hits": counts[1]}
            for stamp, counts in sorted(by_stamp.items(), key=lambda kv: -kv[1][0])
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.home() / ".cohound" / "data" / "extract_cache",
        help="корень рабочего кэша экстракции",
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=Path.home() / ".cohound" / "stands" / "ru-pg-dbms" / "state" / "ru-pg-dbms" / "state.db",
        help="state.db пакета (источник document_version для hit-rate); нет файла — пропуск",
    )
    parser.add_argument("--repeats", type=int, default=5, help="повторов на замер скана")
    parser.add_argument(
        "--bench-files",
        type=int,
        nargs="*",
        default=[1000, 10000],
        help="размеры песочниц для замера put (0 — пропустить)",
    )
    parser.add_argument(
        "--cost",
        action="append",
        default=[],
        metavar="FILES:BUDGET_MB:WRITES",
        help=(
            "прямой замер цены бюджета: песочница из FILES записей медианного размера, "
            "бюджет BUDGET_MB, WRITES записей подряд. Нужен, чтобы цена обхода мерилась "
            "при числе файлов реального кэша, а не экстраполировалась"
        ),
    )
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    args = parser.parse_args(argv)

    root: Path = args.root
    if not root.is_dir():
        print(f"корень кэша не найден: {root}", file=sys.stderr)
        return 2

    report: dict[str, Any] = {"root": str(root)}
    report["inventory"] = _inventory(root)
    report["scan"] = _scan_cost(root, max(1, args.repeats))

    actual_files = report["inventory"]["files"]
    benches: list[dict[str, Any]] = []
    for count in args.bench_files:
        if count <= 0:
            continue
        sandbox = _sandbox(count)
        try:
            total = sum(p.stat().st_size for p in sandbox.rglob("*.json"))
            # Бюджет ниже суммы — вытеснение действительно исполняется (иначе путь
            # «total <= budget» вернулся бы до сортировки и замер был бы пустым).
            benches.append(_put_cost(sandbox, int(total * 0.9), max(2, args.repeats)))
            benches.append(_put_cost(sandbox, None, max(2, args.repeats)))
        finally:
            shutil.rmtree(sandbox, ignore_errors=True)
    report["put"] = benches
    report["put_note"] = (
        "первый put в паре — с бюджетом 90 % суммы песочницы (проверка + вытеснение), "
        "второй — с budget=None (дефолт D8). Порог проверки — 1/16 бюджета, поэтому "
        "при repeats < порога обходов может не быть вовсе: scans это показывает."
    )

    costs: list[dict[str, Any]] = []
    for spec in args.cost:
        try:
            files_text, budget_text, writes_text = spec.split(":")
            files, budget_mb, writes = int(files_text), float(budget_text), int(writes_text)
        except ValueError:
            print(f"--cost: ожидается FILES:BUDGET_MB:WRITES, получено {spec!r}", file=sys.stderr)
            return 2
        sandbox = _sandbox(files)
        try:
            row = _put_cost(sandbox, int(budget_mb * 1_000_000), max(1, writes))
            row["sandbox_files"] = files
            row["scan_seconds_on_sandbox"] = _scan_cost(sandbox, 1)["median_seconds"]
            row["seconds_per_scan_on_real_cache"] = report["scan"]["median_seconds"]
            row["projected_seconds_on_real_cache"] = round(
                row["scans"] * report["scan"]["median_seconds"], 2
            )
            costs.append(row)
        finally:
            shutil.rmtree(sandbox, ignore_errors=True)
    report["cost"] = costs

    if args.state_db.is_file():
        report["cache_index"] = {"entries": len(_cache_index(root))}
        report["hit_rate"] = _hit_rate(args.state_db, _cache_index(root))
    else:
        report["hit_rate"] = {"skipped": f"нет {args.state_db}"}
    report["workload_note"] = (
        f"прогон на {actual_files} записях кэша с включённым бюджетом заплатит "
        f"{report['scan']['median_seconds']} с сканирования на каждый put сверх бюджета"
    )

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        inv, scan = report["inventory"], report["scan"]
        print(f"кэш: {inv['files']} файлов, {inv['bytes'] / 1e6:.1f} MB "
              f"(p50 {inv['p50_bytes']} / p95 {inv['p95_bytes']} / p99 {inv['p99_bytes']} / "
              f"max {inv['max_bytes']} байт)")
        print(f"полный обход (rglob+stat+sort): медиана {scan['median_seconds']} с "
              f"[{scan['min_seconds']}; {scan['max_seconds']}] на {args.repeats} повторах")
        for row in benches:
            budget = "None" if row["budget"] is None else f"{row['budget'] / 1e6:.1f} MB"
            print(f"put (budget={budget}, {row['repeats']} записей): медиана "
                  f"{row['median_seconds']} с, всего {row['total_seconds']} с, "
                  f"обходов {row['scans']}, вытеснено {row['evicted']}")
        hit = report["hit_rate"]
        if "documents" in hit:
            print(f"hit-rate по state.db: {hit['hits']} / {hit['documents']} = {hit['hit_rate']}")
            for stamp, counts in list(hit["by_stamp"].items())[:6]:
                print(f"   {stamp}: {counts['hits']} / {counts['documents']}")
        else:
            print(f"hit-rate: {hit}")
        for row in costs:
            print(
                f"цена бюджета: {row['sandbox_files']} файлов, бюджет "
                f"{row['budget'] / 1e6:.0f} MB, {row['writes']} записей → обходов "
                f"{row['scans']} (по {row['scan_seconds_on_sandbox']} с на песочнице; "
                f"на реальном кэше {row['projected_seconds_on_real_cache']} с), "
                f"всего {row['total_seconds']} с, вытеснено {row['evicted']}"
            )
        print(report["workload_note"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
