#!/usr/bin/env python3
"""Цена и полезность ``extract_cache``: сканирование, вытеснение, hit-rate (O32, план 26).

Четыре вопроса, на которые прибор отвечает числами, а не оценками:

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
4. **Сколько из ``put`` стоит ``fsync``** — разложение записи на «запись без ``fsync``»,
   «``fsync`` файла» и «``fsync`` каталога» (последний рабочий код **не** делает) с
   контролем: сумма частей обязана сходиться с настоящим ``ExtractionCache.put``. Без
   этого разложения цена записи известна только суммой (0.0021 с на запись), и «материален
   ли ``fsync``» остаётся мнением.

Запуск::

    .venv/bin/python scripts/measure_extract_cache.py
    .venv/bin/python scripts/measure_extract_cache.py --bench-files 1000 10000 --repeats 3
    .venv/bin/python scripts/measure_extract_cache.py --state-db ~/.cohound/.../state.db
    .venv/bin/python scripts/measure_extract_cache.py --fsync-repeats 100 --fsync-warmup 20

Код выхода: 0 — замер снят, 2 — не найден корень кэша. Нейтрален к предметной категории
(§1.3): читает файлы кэша и строки ``document_version``, не зная, о чём документы.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
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


def _percentile_float(values: list[float], q: float) -> float | None:
    """Квантиль по ближайшему рангу для длительностей.

    Пустой список — ``None``, а не ``0``: «не мерили» и «стоит ноль» — разные факты, и
    подмена первого вторым как раз и делает отчёт неотличимым от выдуманного.
    """
    if not values:
        return None
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


def _payload_of_size(entry_bytes: int) -> str:
    """JSON-конверт ровно ``entry_bytes`` байт (ASCII, поэтому ``len`` = число байт).

    Размер — условие замера, а не удобство: ``fsync`` синхронизирует грязные страницы, и
    запись на 120 байт меряет не ту цену, что запись медианного размера реального кэша
    (5 169 байт, замер 15.11.2026). Форма конверта взята у ``ExtractionCache.put``
    (``json.dumps`` с ``separators``), чтобы длина была честной, а не декоративной.
    """
    if entry_bytes < 2:
        raise ValueError("entry_bytes должен быть положительным")
    envelope: dict[str, Any] = {"format": 1, "text": ""}
    base = len(json.dumps(envelope, separators=(",", ":"), ensure_ascii=False))
    envelope["text"] = "x" * max(0, entry_bytes - base)
    payload = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
    # Текст — только ASCII, поэтому правка длины текста на d меняет длину на d байт.
    delta = entry_bytes - len(payload)
    if delta:
        envelope["text"] = "x" * max(0, len(envelope["text"]) + delta)
        payload = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
    return payload


def _write_once(parent: Path, name: str, payload: str) -> dict[str, float]:
    """Запись без единого ``fsync`` по образцу ``ExtractionCache._write_atomic``.

    Порядок фаз повторяет рабочий код (``mkdir`` → ``mkstemp`` → ``write`` → ``flush`` →
    ``close`` → ``os.replace``) с одним отличием: ``os.fsync`` не зовётся. Это и есть
    «запись без ``fsync``» — цена содержимого на диске до того, как оно стало durable.
    """
    target = parent / name[:2] / f"{name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    timings = {"write": 0.0, "replace": 0.0, "total": 0.0}
    started = time.perf_counter()
    descriptor, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=f".{name}.", suffix=".tmp")
    handle = os.fdopen(descriptor, "w", encoding="utf-8")
    handle.write(payload)
    handle.flush()
    written = time.perf_counter()
    timings["write"] = written - started
    handle.close()
    os.replace(tmp_name, target)
    replaced = time.perf_counter()
    timings["replace"] = replaced - written
    timings["total"] = time.perf_counter() - started
    return timings


class _FsyncProbe:
    """Считает цену ``fsync`` **внутри настоящего** ``put``, не подменяя путь записи.

    Причина, по которой это отдельный класс, а не второй цикл записи: два независимых цикла
    меряют два разных состояния ФС. Проверено 17.11.2026 — `fsync` в отдельном цикле дал
    1.469 мс, а в следующем прогоне 1.840 мс, и сумма частей превысила полный `put`
    (остаток **−10 %**): сравнивать было нечего. Замер внутри `put` делает разложение
    аддитивным по построению — фаза и полное время берутся из **одной** итерации.

    ``sync_file=False`` — ``fsync`` файла не зовётся вовсе (цена записи без него);
    ``sync_dir=True`` — после ``os.replace`` синхронизируется ещё и каталог записи, то есть
    меряется цена того, чего рабочий код **не делает** (``extract_cache.py:596``).
    """

    def __init__(self, *, sync_file: bool, sync_dir: bool) -> None:
        self._sync_file = sync_file
        self._sync_dir = sync_dir
        self.file_samples: list[float] = []
        self.dir_samples: list[float] = []

    def __enter__(self) -> _FsyncProbe:
        from core.pipeline import extract_cache as cache_module

        probe = self
        real_os = cache_module.os

        class _Proxy:
            """Подменяет только ``fsync``/``replace``; остальное — настоящий ``os``."""

            def __getattr__(self, name: str) -> Any:
                return getattr(real_os, name)

            def fsync(self, fd: int) -> None:
                if not probe._sync_file:
                    return
                started = time.perf_counter()
                real_os.fsync(fd)
                probe.file_samples.append(time.perf_counter() - started)

            def replace(self, src: Any, dst: Any) -> None:
                real_os.replace(src, dst)
                if not probe._sync_dir:
                    return
                started = time.perf_counter()
                descriptor = real_os.open(Path(dst).parent, real_os.O_RDONLY)
                try:
                    real_os.fsync(descriptor)
                finally:
                    real_os.close(descriptor)
                probe.dir_samples.append(time.perf_counter() - started)

        self._module = cache_module
        self._real_os = real_os
        cache_module.os = _Proxy()  # type: ignore[assignment]
        return self

    def __exit__(self, *exc_info: object) -> bool:
        self._module.os = self._real_os  # type: ignore[assignment]
        return False


def _aggregate(samples: list[float]) -> dict[str, Any]:
    """Медиана/p95/min/max по списку длительностей (пусто → ``None``, не ноль)."""
    if not samples:
        return {"repeats": 0, "median_seconds": None, "p95_seconds": None}
    return {
        "repeats": len(samples),
        "median_seconds": round(statistics.median(samples), 6),
        "p95_seconds": round(_percentile_float(samples, 0.95) or 0.0, 6),
        "min_seconds": round(min(samples), 6),
        "max_seconds": round(max(samples), 6),
    }


def _bench_writes(
    parent: Path,
    payload: str,
    repeats: int,
    warmup: int,
) -> dict[str, Any]:
    """``repeats`` записей без ``fsync`` после ``warmup`` отброшенных.

    Прогрев отбрасывается явно: первый ``fsync`` на новом каталоге/ФС несёт создание
    inode, расширение журнала и заполнение страничного кэша ОС — это цена первого касания,
    а не записи. Сколько именно отброшено, публикуется (``warmup``), иначе число
    невоспроизводимо.
    """
    rows: list[dict[str, float]] = []
    for index in range(warmup + repeats):
        row = _write_once(parent, f"{index:064x}", payload)
        if index >= warmup:
            rows.append(row)
    result: dict[str, Any] = {"warmup": warmup}
    for phase in ("write", "replace", "total"):
        result[phase] = _aggregate([row[phase] for row in rows])
    return result


def _bench_put(
    root: Path,
    entry_bytes: int,
    repeats: int,
    warmup: int,
    *,
    sync_dir: bool = False,
) -> dict[str, Any]:
    """Настоящий ``ExtractionCache.put`` с ценой ``fsync``, снятой **внутри** него.

    Возвращает и полное время ``put``, и время самого ``fsync`` из той же итерации, поэтому
    «запись без ``fsync``» получается вычитанием в паре (``total - fsync``), а не вторым
    независимым циклом: два цикла меряют два состояния ФС и складываться не обязаны
    (проверено 17.11.2026 — остаток ушёл в −10 %).

    ``sync_dir=True`` добавляет синхронизацию каталога после ``os.replace``: это цена того,
    чего рабочий код не делает, и она тоже снимается внутри ``put``.
    """
    from core.ports.types import ExtractedDoc  # локально: прибор не тянет порт зря

    cache = ExtractionCache(root, max_bytes=None)

    def _doc(index: int, text_len: int) -> ExtractedDoc:
        return ExtractedDoc(
            text="x" * max(1, text_len),
            content_hash=f"{index:064x}",
            extractor_version="bench-1",
            page_count=1,
            family="text",
        )

    # Подгонка размера конверта: накладные расходы конверта неизвестны заранее, поэтому
    # они измеряются одной пробной записью, а не угадываются.
    probe = _doc(0, entry_bytes)
    cache.put(probe, level="layout_tables")
    probe_path = root / f"{0:064x}"[:2] / f"{0:064x}.json"
    try:
        overhead = probe_path.stat().st_size - len(probe.text)
    except OSError:
        overhead = 0
    if overhead >= entry_bytes:
        # Запись меньше собственного конверта недостижима: мерить пришлось бы другой
        # размер, а «сумма частей сходится с put» тогда было бы совпадением не того пути.
        raise ValueError(
            f"конверт занимает {overhead} байт — запись {entry_bytes} байт недостижима"
        )
    text_len = entry_bytes - overhead
    totals: list[float] = []
    without_fsync: list[float] = []
    file_samples: list[float] = []
    dir_samples: list[float] = []
    written = None
    with _FsyncProbe(sync_file=True, sync_dir=sync_dir) as probe_fsync:
        for index in range(1, warmup + repeats + 1):
            doc = _doc(index, text_len)
            before_file = len(probe_fsync.file_samples)
            before_dir = len(probe_fsync.dir_samples)
            started = time.perf_counter()
            cache.put(doc, level="layout_tables")
            elapsed = time.perf_counter() - started
            if index > warmup:
                totals.append(elapsed)
                inside = probe_fsync.file_samples[before_file:]
                file_samples.extend(inside)
                dir_samples.extend(probe_fsync.dir_samples[before_dir:])
                # Парное вычитание: обе величины из одной итерации, поэтому аддитивны.
                without_fsync.append(elapsed - sum(inside))
            entry = root / f"{index:064x}"[:2] / f"{index:064x}.json"
            try:
                written = entry.stat().st_size
            except OSError:
                written = None
    result = _aggregate(totals)
    result["without_fsync"] = _aggregate(without_fsync)
    result["fsync_file"] = _aggregate(file_samples)
    result["fsync_dir"] = _aggregate(dir_samples)
    result["writes"] = cache.stats()["writes"]
    result["entry_bytes_written"] = written
    result["_totals"] = totals
    return result


def _filesystem_type(path: Path) -> str | None:
    """Тип ФС по ``/proc/mounts`` (самая длинная подходящая точка монтирования).

    Носитель — условие замера, а не оговорка: ``fsync`` на ext4 внутри VHDX (WSL2) и на
    голом NVMe стоят по-разному, и число без носителя не переносимо.
    """
    try:
        resolved = Path(os.path.realpath(path))
        best_len = -1
        best: str | None = None
        with open("/proc/mounts", encoding="utf-8") as handle:
            for line in handle:
                fields = line.split()
                if len(fields) < 3:
                    continue
                mount_point = fields[1].replace("\\040", " ")
                matches = str(resolved) == mount_point or str(resolved).startswith(
                    mount_point.rstrip("/") + "/"
                )
                if matches and len(mount_point) > best_len:
                    best_len = len(mount_point)
                    best = fields[2]
        return best
    except OSError:
        return None


def _write_breakdown(
    *,
    entry_bytes: int,
    repeats: int,
    warmup: int,
    sandbox_parent: Path | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Раскладывает ``put`` на составляющие: запись, ``fsync`` файла, ``fsync`` каталога.

    Три величины, и все три — из **настоящего** ``put``, кроме записи без ``fsync``:
    ``fsync_file`` снимается внутри ``put`` (обёртка вокруг ``os.fsync``), ``full_put`` — его
    полное время, а ``write_only`` меряется копией ``_write_atomic`` без единого ``fsync``
    (второй цикл нужен только чтобы увидеть цену записи **без** синхронизации; аддитивность
    обеспечивает пара ``full_put − fsync_file``, а не этот цикл).
    ``fsync_dir`` — ``fsync`` каталога после ``os.replace``, которого рабочий код **не
    делает** (``core/pipeline/extract_cache.py:595`` синхронизирует файл, метаданные
    каталога остаются несинхронизированными).

    Публикуется и сходимость (``residual``): ``full_put − (write_only + fsync_file)`` —
    это накладные расходы ``put`` вне ``_write_atomic`` (``json.dumps`` конверта, ``stat``,
    путь). Если остаток велик или отрицателен, прибор мерил не тот путь, и разложение
    публиковать нельзя.
    """
    parent = Path(tempfile.mkdtemp(prefix="extract-cache-fsync-", dir=sandbox_parent))
    try:
        sandbox_fs = _filesystem_type(parent)
        payload = _payload_of_size(entry_bytes)
        # Порядок важен: сначала циклы с `fsync` (чистое состояние ФС), потом запись без
        # него — она оставляет грязные страницы в кэше ОС, и после неё `fsync` дороже.
        full_put = _bench_put(parent / "put", entry_bytes, repeats, warmup)
        with_dir = _bench_put(parent / "put_dir", entry_bytes, repeats, warmup, sync_dir=True)
        write_only = _bench_writes(parent / "write_only", payload, repeats, warmup)

        # Парная разница «put + fsync каталога» минус «put»: обе половины — настоящий
        # `put`, поэтому разброс хоста сокращается, а не складывается.
        paired_dir = [
            later - earlier
            for earlier, later in zip(full_put["_totals"], with_dir["_totals"], strict=False)
        ]
        parts_sum = (write_only["total"]["median_seconds"] or 0.0) + (
            full_put["fsync_file"]["median_seconds"] or 0.0
        )
        full_median = full_put["median_seconds"]
        residual = None if full_median is None else round(full_median - parts_sum, 6)
        paired = _aggregate(paired_dir)
    finally:
        shutil.rmtree(parent, ignore_errors=True)

    return {
        "entry_bytes_requested": entry_bytes,
        "entry_bytes_written": full_put["entry_bytes_written"],
        "warmup": warmup,
        "repeats": repeats,
        "sandbox_filesystem": sandbox_fs,
        "root_filesystem": None if root is None else _filesystem_type(root),
        "kernel": platform.release(),
        "components": {
            "write_only": write_only["total"],
            "fsync_file": full_put["fsync_file"],
            "fsync_dir": with_dir["fsync_dir"],
            "full_put": {key: value for key, value in full_put.items() if not key.startswith("_")},
            "put_without_fsync": full_put["without_fsync"],
        },
        "phases_inside_write_only": {
            "write": write_only["write"],
            "replace": write_only["replace"],
        },
        "parts_sum_seconds": round(parts_sum, 6),
        "residual_seconds": residual,
        "residual_share": (
            None if residual is None or not full_median else round(residual / full_median, 4)
        ),
        "durable_extra_seconds": paired["median_seconds"],
        "durable_extra_p95_seconds": paired["p95_seconds"],
        "dir_fsync_in_code": False,
        "code_reference": "core/pipeline/extract_cache.py:595",
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


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Аргументы прибора. Дефолты — это условия опубликованного числа."""
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
    parser.add_argument(
        "--fsync-repeats",
        type=int,
        default=50,
        help="повторов на каждую составляющую записи (fsync-разложение); <1 — ошибка",
    )
    parser.add_argument(
        "--fsync-warmup",
        type=int,
        default=20,
        help="сколько первых записей каждой составляющей отбросить (первый fsync на ФС "
        "несёт создание inode/журнала и меряет не запись)",
    )
    parser.add_argument(
        "--entry-bytes",
        type=int,
        default=5169,
        help="размер записи в замере fsync: p50 реального кэша 5 169 байт (замер 15.11.2026)",
    )
    parser.add_argument(
        "--sandbox-parent",
        type=Path,
        default=None,
        help="каталог для песочницы fsync (по умолчанию системный temp); рабочий кэш не трогается",
    )
    parser.add_argument(
        "--skip-fsync",
        action="store_true",
        help="пропустить разложение записи на fsync (быстрый прогон)",
    )
    return parser.parse_args(argv)


def _validate(args: argparse.Namespace) -> str | None:
    """Проверки условий замера: `repeats=0` дал бы `None` под видом медианы."""
    if args.skip_fsync:
        return None
    if args.fsync_repeats < 1:
        return "--fsync-repeats: нужно ≥1, иначе «медиана» не из чего"
    if args.fsync_warmup < 0:
        return "--fsync-warmup: не может быть отрицательным"
    if args.entry_bytes < 2:
        return "--entry-bytes: запись меньше 2 байт — не конверт"
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    problem = _validate(args)
    if problem is not None:
        print(problem, file=sys.stderr)
        return 2

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

    if not args.skip_fsync:
        try:
            report["write_breakdown"] = _write_breakdown(
                entry_bytes=args.entry_bytes,
                repeats=args.fsync_repeats,
                warmup=args.fsync_warmup,
                sandbox_parent=args.sandbox_parent,
                root=root,
            )
        except ValueError as error:
            print(f"--entry-bytes: {error}", file=sys.stderr)
            return 2
    else:
        report["write_breakdown"] = {"skipped": "--skip-fsync"}

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
        breakdown = report["write_breakdown"]
        if "components" in breakdown:
            print(
                f"разложение записи: {breakdown['entry_bytes_requested']} байт "
                f"(конверт полного put {breakdown['entry_bytes_written']} байт), "
                f"прогрев {breakdown['warmup']} отброшен, повторов {breakdown['repeats']}, "
                f"ФС песочницы {breakdown['sandbox_filesystem']}, "
                f"ФС кэша {breakdown['root_filesystem']}, ядро {breakdown['kernel']}"
            )
            for name, label in (
                ("write_only", "запись без fsync (write+close+replace)"),
                ("put_without_fsync", "put без fsync (парно, та же итерация)"),
                ("fsync_file", "fsync файла внутри put"),
                ("fsync_dir", "fsync каталога внутри put (код НЕ делает)"),
                ("full_put", "полный put (ExtractionCache.put)"),
            ):
                row = breakdown["components"][name]
                print(
                    f"   {label}: медиана {row['median_seconds']} с, "
                    f"p95 {row['p95_seconds']} с, n={row['repeats']}"
                )
            print(
                f"   сумма частей {breakdown['parts_sum_seconds']} с против "
                f"full put {breakdown['components']['full_put']['median_seconds']} с → "
                f"остаток {breakdown['residual_seconds']} с "
                f"({breakdown['residual_share']}); цена каталога сверх текущего put "
                f"{breakdown['durable_extra_seconds']} с (p95 "
                f"{breakdown['durable_extra_p95_seconds']} с)"
            )
        else:
            print(f"разложение записи: {breakdown}")
        print(report["workload_note"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
