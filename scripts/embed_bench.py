"""Замер эмбеддинга: точность вариантов счёта и параллелизм CPU.

Инструмент отвечает двумя числами, которых нет в юнит-тестах: **сколько стоит** вектор
под каждым вариантом точности (fp32/tf32/bf16/fp16) и **сколько ядер** реально работает
при разных ``torch.set_num_threads`` × ``batch_size``. Вход — сохранённая БД прогона
(``cohound.db``), читается **только на чтение** (``mode=ro``): тексты берутся из
``corpus_chunks`` по объявленному продукту и детерминированному шагу выборки (не
``random``), поэтому прогон воспроизводим по одному и тому же срезу корпуса.

**Метрики** (определения зафиксированы):

``chunks_per_s``
    Чанков в секунду на замере **после прогрева** (прогрев = один батч, в замер не входит).
``peak_vram_bytes``
    ``torch.cuda.max_memory_allocated()`` за вариант, включая веса модели (сброс пика —
    перед загрузкой варианта).
``peak_rss_bytes``
    Пик RSS **процесса** в окне замера (монитор ``/proc/self/status``, шаг 20 мс), а не
    ``ru_maxrss``: последний монотонен по процессу и включал бы загрузку всех вариантов.
``agreement.cos_gap``
    Распределение ``1 − cos`` вектора варианта против вектора ``fp32`` на том же тексте:
    median / p95 / max. Векторы перед сравнением ренормируются (иначе bf16-норма
    отклонялась бы от fp32 дважды — в косинусе и в норме).
``agreement.top10_overlap``
    Средняя доля перекрытия top-10 соседей по самому сэмплу (сэмпл = набор запросов,
    сам сосед исключён): сравнение ранжирования, а не только близости вектора.
``determinism.max_abs_delta``
    ``max |V_pass1 − V_pass0|`` по компонентам: два замера одного варианта в одном
    процессе. Ненулевое значение — недетерминизм самого варианта, а не шума измерения.
``cpu_ratio``
    ``Δtime.process_time() / Δwall`` за окно замера: сколько ядер реально занято
    (1.0 — одно ядро, 12.0 — все потоки).

**Чего метрики НЕ измеряют:** качество ретривала (согласие векторов — не качество
поиска), κ оценщика, влияние точности на вердикты. Здесь — скорость, память, согласие и
детерминизм на одном срезе корпуса.

Нейтрален по терминам (§1.3): идентификатор модели — данные, категорий продуктов нет.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import sqlite3
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Идентификатор модели по умолчанию (данные, не категория): пишется в отчёт.
DEFAULT_MODEL = "BAAI/bge-m3"
#: Размер батча вариантов точности: как у продакшн-пути (``DEFAULT_BATCH_SIZE``).
DEFAULT_VARIANT_BATCH = 32
#: Батчи CPU-развёртки: те же, что объявлены в задании замера.
DEFAULT_BATCH_SIZES = (32, 64, 128)
#: Потоки CPU-развёртки.
DEFAULT_THREADS = (1, 4, 8, 12)


# ---------------------------------------------------------------------------
# Память процесса
# ---------------------------------------------------------------------------


def _rss_bytes() -> int:
    """Текущий RSS процесса из ``/proc/self/status`` (без psutil)."""
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return 0
    return 0


class RssMonitor:
    """Пик RSS за окно замера: поток опрашивает процесс, а не монотонный ``ru_maxrss``."""

    def __init__(self, interval_s: float = 0.02) -> None:
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._peak = 0

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._peak = max(self._peak, _rss_bytes())
            self._stop.wait(self._interval)

    def start(self) -> None:
        self._peak = _rss_bytes()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> int:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self._peak = max(self._peak, _rss_bytes())
        return self._peak


# ---------------------------------------------------------------------------
# Выборка текстов
# ---------------------------------------------------------------------------


def _percentile(values: Sequence[float], q: float) -> float:
    """Перцентиль без numpy-зависимости (линейная интерполяция, как ``numpy.percentile``)."""
    ordered = sorted(values)
    if not ordered:
        return 0.0
    pos = (len(ordered) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo))


@dataclass(frozen=True)
class Sample:
    """Детерминированный срез корпуса: конкретные строки и их адреса."""

    product_id: str
    rows_total: int
    chunk_ids: list[str]
    texts: list[str]

    @property
    def n(self) -> int:
        return len(self.texts)

    def describe(self) -> dict[str, Any]:
        lengths = [len(t) for t in self.texts]
        return {
            "product_id": self.product_id,
            "rows_total": self.rows_total,
            "chunks": self.n,
            "mean_chars": round(sum(lengths) / len(lengths), 1) if lengths else 0.0,
            "max_chars": max(lengths) if lengths else 0,
            "stride": round(self.rows_total / self.n, 3) if self.n else 0.0,
            "first_chunk_id": self.chunk_ids[0] if self.chunk_ids else None,
            "last_chunk_id": self.chunk_ids[-1] if self.chunk_ids else None,
        }


def load_sample(db: Path, product_id: str, sample: int) -> Sample:
    """Читает детерминированную выборку: каждый ``stride``-й чанк в порядке ``chunk_id``.

    ``random`` не используется: повторный прогон на той же БД обязан дать тот же срез,
    иначе числа скорости и согласия не сравнить между запусками.
    """
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        total = int(
            conn.execute(
                "SELECT count(*) FROM corpus_chunks WHERE product_id = ?", (product_id,)
            ).fetchone()[0]
        )
        if total == 0:
            raise SystemExit(f"corpus_chunks пуст для product_id={product_id!r}")
        n = min(sample, total)
        # Равномерный шаг по упорядоченным строкам: (i * total) // n, без повторов.
        offsets = sorted({min(total - 1, (i * total) // n) for i in range(n)})
        marks = ",".join("?" for _ in offsets)
        sql = (
            "WITH ordered AS ("  # noqa: S608 — подставляются только «?», значения bind'ом
            "  SELECT chunk_id, text, row_number() OVER (ORDER BY chunk_id) - 1 AS rn"
            "  FROM corpus_chunks WHERE product_id = ?"
            ") "
            f"SELECT chunk_id, text FROM ordered WHERE rn IN ({marks}) ORDER BY rn"
        )
        rows = conn.execute(sql, (product_id, *offsets)).fetchall()
    finally:
        conn.close()
    chunk_ids = [str(r[0]) for r in rows]
    texts = [str(r[1]) for r in rows]
    return Sample(product_id=product_id, rows_total=total, chunk_ids=chunk_ids, texts=texts)


# ---------------------------------------------------------------------------
# Варианты точности
# ---------------------------------------------------------------------------


def _force_offline() -> None:
    """Запрещает любые обращения к HF Hub: веса обязаны быть в локальном кэше.

    ``local_files_only=True`` у ``SentenceTransformer`` недостаточно: библиотека всё
    равно пробует уточнить метаданные через hub и печатает предупреждение об
    анонимном запросе. Оффлайн-флаги окружения ставятся **до** импорта стека, иначе
    torch/transformers успевают прочитать окружение.
    """
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")


def _require_torch() -> Any:
    try:
        import torch
    except ImportError as exc:  # pragma: no cover - окружение без torch
        raise SystemExit(f"torch не установлен: {exc}") from exc
    return torch


@contextmanager
def precision_variant(variant: str, device: str) -> Iterator[None]:
    """Выставляет объявленный вариант счёта и возвращает прежние флаги после него.

    ``tf32`` — флаг ``allow_tf32`` + ``matmul_precision="high"``; ``bf16``/``fp16`` —
    ``torch.autocast`` вокруг вызова ``encode``; ``fp32`` — явный возврат к «как сейчас»
    (``allow_tf32=False``, ``matmul_precision="highest"``), чтобы вариант не наследовал
    настройки предыдущего.
    """
    torch = _require_torch()
    if variant == "fp32":
        prev_tf32 = torch.backends.cuda.matmul.allow_tf32
        prev_prec = torch.get_float32_matmul_precision()
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.set_float32_matmul_precision("highest")
        try:
            yield
        finally:
            torch.backends.cuda.matmul.allow_tf32 = prev_tf32
            torch.set_float32_matmul_precision(prev_prec)
    elif variant == "tf32":
        if device != "cuda":
            raise SystemExit("вариант tf32 существует только на cuda")
        prev_tf32 = torch.backends.cuda.matmul.allow_tf32
        prev_prec = torch.get_float32_matmul_precision()
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.set_float32_matmul_precision("high")
        try:
            yield
        finally:
            torch.backends.cuda.matmul.allow_tf32 = prev_tf32
            torch.set_float32_matmul_precision(prev_prec)
    elif variant in {"bf16", "fp16"}:
        if device != "cuda":
            raise SystemExit(f"вариант {variant} существует только на cuda")
        dtype = torch.bfloat16 if variant == "bf16" else torch.float16
        with torch.autocast(device_type="cuda", dtype=dtype):
            yield
    else:  # pragma: no cover - argparse ограничивает значения
        raise SystemExit(f"неизвестный вариант точности: {variant!r}")


def _encode_batch(
    model: Any, batch: list[str], *, variant: str, device: str
) -> Any:
    """Один вызов ``encode`` тем же способом, что продакшн-путь (нормировка + numpy)."""
    torch = _require_torch()
    with torch.inference_mode(), precision_variant(variant, device):
        matrix = model.encode(
            batch,
            batch_size=len(batch),
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
    return matrix.astype("float32", copy=False)


def _encode_pass(
    model: Any,
    texts: Sequence[str],
    *,
    batch_size: int,
    variant: str,
    device: str,
    limit_chunks: int | None = None,
) -> tuple[Any, float, float, int]:
    """Один замер: батчи подряд, без прогрева. Возвращает матрицу, wall, cpu, пик RSS."""
    torch = _require_torch()
    total = len(texts) if limit_chunks is None else min(limit_chunks, len(texts))
    chunks: list[Any] = []
    monitor = RssMonitor()
    if device == "cuda":
        torch.cuda.synchronize()
    monitor.start()
    wall0 = time.perf_counter()
    cpu0 = time.process_time()
    for start in range(0, total, batch_size):
        batch = [str(t) for t in texts[start : start + batch_size]]
        if not batch:
            break
        chunks.append(_encode_batch(model, batch, variant=variant, device=device))
    if device == "cuda":
        torch.cuda.synchronize()
    wall = time.perf_counter() - wall0
    cpu = time.process_time() - cpu0
    peak_rss = monitor.stop()
    if not chunks:  # pragma: no cover - total == 0 отсекается на выборке
        raise SystemExit("пустой замер: нет текстов")
    import numpy as np

    return np.vstack(chunks), wall, cpu, peak_rss


def _warmup(model: Any, texts: Sequence[str], *, batch_size: int, variant: str, device: str) -> None:
    """Прогрев: ровно один батч (ленивая инициализация ядер/кусков модели)."""
    batch = [str(t) for t in texts[:batch_size]]
    if len(batch) < batch_size:
        batch = ([str(t) for t in texts] * (batch_size // max(1, len(texts)) + 1))[:batch_size]
    _encode_batch(model, batch, variant=variant, device=device)


# ---------------------------------------------------------------------------
# Согласие и детерминизм
# ---------------------------------------------------------------------------


def _renormalized(matrix: Any) -> Any:
    """Строки к единичной норме: bf16/fp16 дают норму, отличную от fp32."""
    import numpy as np

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return matrix / norms


def _top10_overlap(reference: Any, candidate: Any, k: int = 10) -> float:
    """Средняя доля перекрытия top-k соседей по самому сэмплу (сам сосед исключён)."""
    import numpy as np

    ref = _renormalized(reference)
    cand = _renormalized(candidate)
    sims_ref = ref @ ref.T
    sims_cand = cand @ cand.T
    np.fill_diagonal(sims_ref, -np.inf)
    np.fill_diagonal(sims_cand, -np.inf)
    k = min(k, ref.shape[0] - 1)
    if k <= 0:
        return 1.0
    ref_top = np.argpartition(-sims_ref, k, axis=1)[:, :k]
    cand_top = np.argpartition(-sims_cand, k, axis=1)[:, :k]
    overlaps = [
        len(set(ref_top[i].tolist()) & set(cand_top[i].tolist())) / k
        for i in range(ref.shape[0])
    ]
    return float(sum(overlaps) / len(overlaps))


def _agreement(reference: Any, candidate: Any) -> dict[str, Any]:
    """``1 − cos`` по тем же текстам + перекрытие top-10."""
    import numpy as np

    ref = _renormalized(reference)
    cand = _renormalized(candidate)
    cos = np.sum(ref * cand, axis=1)
    gap = (1.0 - cos).tolist()
    return {
        "cos_gap": {
            "median": round(_percentile(gap, 0.5), 9),
            "p95": round(_percentile(gap, 0.95), 9),
            "max": round(max(gap), 9),
        },
        "top10_overlap": round(_top10_overlap(reference, candidate), 6),
    }


# ---------------------------------------------------------------------------
# Варианты точности: замер
# ---------------------------------------------------------------------------


def _cuda_device_name() -> str:
    torch = _require_torch()
    if not torch.cuda.is_available():
        return "none"
    return str(torch.cuda.get_device_name(0))


def run_precision_variants(
    sample: Sample,
    *,
    variants: Sequence[str],
    batch_size: int,
    repeats: int,
    model_id: str,
    dump_vectors: Path | None,
) -> list[dict[str, Any]]:
    """Гоняет варианты точности на cuda; ``fp32`` обязан быть первым (эталон согласия)."""
    import numpy as np
    from sentence_transformers import SentenceTransformer

    torch = _require_torch()
    if variants and variants[0] != "fp32":
        raise SystemExit("эталон согласия — fp32: он обязан идти первым в --variants")
    results: list[dict[str, Any]] = []
    baseline: Any = None
    for variant in variants:
        print(f"[precision] {variant}: загрузка модели", file=sys.stderr, flush=True)
        if variant not in {"fp32", "tf32", "bf16", "fp16"}:
            raise SystemExit(f"неизвестный вариант: {variant!r}")
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        model = None
        try:
            model = SentenceTransformer(model_id, device="cuda", local_files_only=True)
            rss_after_load = _rss_bytes()
            with precision_variant(variant, "cuda"):
                _warmup(model, sample.texts, batch_size=batch_size, variant=variant, device="cuda")
            passes: list[Any] = []
            speeds: list[float] = []
            peaks_rss: list[int] = []
            cpu_ratios: list[float] = []
            for repeat in range(repeats):
                matrix, wall, cpu_time, peak_rss = _encode_pass(
                    model, sample.texts, batch_size=batch_size, variant=variant, device="cuda"
                )
                passes.append(matrix)
                speeds.append(sample.n / wall if wall > 0 else 0.0)
                peaks_rss.append(peak_rss)
                cpu_ratios.append(cpu_time / wall if wall > 0 else 0.0)
                print(
                    f"[precision] {variant}: прогон {repeat + 1}/{repeats} — "
                    f"{speeds[-1]:.2f} чанк/с, RSS {peak_rss / 2**20:.0f} MiB",
                    file=sys.stderr,
                    flush=True,
                )
            vram_peak = int(torch.cuda.max_memory_allocated())
            vram_reserved = int(torch.cuda.max_memory_reserved())
            determinism = (
                float(np.max(np.abs(passes[1] - passes[0]))) if len(passes) > 1 else None
            )
            if baseline is None:
                baseline = passes[0]
            entry: dict[str, Any] = {
                "variant": variant,
                "device": "cuda",
                "batch_size": batch_size,
                "chunks": sample.n,
                "chunks_per_s": round(speeds[0], 4),
                "chunks_per_s_runs": [round(s, 4) for s in speeds],
                "wall_s": None,
                "peak_vram_bytes": vram_peak,
                "peak_vram_reserved_bytes": vram_reserved,
                "rss_after_load_bytes": rss_after_load,
                "peak_rss_bytes": max(peaks_rss),
                "cpu_ratio": round(max(cpu_ratios), 3),
                "determinism_max_abs_delta": (
                    round(determinism, 9) if determinism is not None else None
                ),
                "agreement": _agreement(baseline, passes[0]) if variant != "fp32" else {
                    "cos_gap": {"median": 0.0, "p95": 0.0, "max": 0.0},
                    "top10_overlap": 1.0,
                },
            }
            # wall первого прогона — для проверки «чанков/с × wall = chunks».
            entry["wall_s"] = round(sample.n / speeds[0], 3) if speeds[0] else None
            if dump_vectors is not None:
                dump_vectors.mkdir(parents=True, exist_ok=True)
                np.save(dump_vectors / f"{variant}.npy", passes[0])
                for idx, matrix in enumerate(passes[1:], start=2):
                    np.save(dump_vectors / f"{variant}.pass{idx}.npy", matrix)
            results.append(entry)
        except Exception as exc:  # noqa: BLE001 - отказ варианта публикуется, а не глотается
            results.append(
                {
                    "variant": variant,
                    "device": "cuda",
                    "batch_size": batch_size,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            print(f"[precision] {variant}: ОТКАЗ — {exc}", file=sys.stderr, flush=True)
        finally:
            del model
            _release_cuda()
    return results


def _release_cuda() -> None:
    """Освобождает VRAM между вариантами (иначе пик следующего включает мусор прошлого)."""
    import gc

    torch = _require_torch()
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


# ---------------------------------------------------------------------------
# Параллелизм CPU
# ---------------------------------------------------------------------------


def run_cpu_sweep(
    sample: Sample,
    *,
    threads: Sequence[int],
    batch_sizes: Sequence[int],
    variant: str,
    model_id: str,
    cpu_sample: int,
) -> dict[str, Any]:
    """Потоки × батч на CPU: чанков/с и ``process_time/wall`` (сколько ядер занято)."""
    from sentence_transformers import SentenceTransformer

    torch = _require_torch()
    texts = list(sample.texts[: min(cpu_sample, sample.n)])
    print(
        f"[cpu] загрузка модели, срез {len(texts)} чанков, вариант {variant}",
        file=sys.stderr,
        flush=True,
    )
    model = SentenceTransformer(model_id, device="cpu", local_files_only=True)
    cells: list[dict[str, Any]] = []
    base_rss = _rss_bytes()
    for nthreads in threads:
        torch.set_num_threads(int(nthreads))
        for batch_size in batch_sizes:
            batch_size = int(batch_size)
            n_batches = max(1, len(texts) // batch_size)
            window = texts[: n_batches * batch_size]
            _warmup(model, window, batch_size=batch_size, variant=variant, device="cpu")
            matrix, wall, cpu_time, peak_rss = _encode_pass(
                model, window, batch_size=batch_size, variant=variant, device="cpu"
            )
            del matrix
            cell = {
                "threads": int(nthreads),
                "torch_num_threads": int(torch.get_num_threads()),
                "batch_size": batch_size,
                "chunks": len(window),
                "chunks_per_s": round(len(window) / wall, 4) if wall > 0 else 0.0,
                "cpu_ratio": round(cpu_time / wall, 3) if wall > 0 else 0.0,
                "peak_rss_bytes": peak_rss,
            }
            cells.append(cell)
            print(
                f"[cpu] threads={nthreads} batch={batch_size}: "
                f"{cell['chunks_per_s']:.3f} чанк/с, ядер {cell['cpu_ratio']:.2f}",
                file=sys.stderr,
                flush=True,
            )
    del model
    return {
        "device": "cpu",
        "variant": variant,
        "chunks": len(texts),
        "batch_sizes": [int(b) for b in batch_sizes],
        "threads": [int(t) for t in threads],
        "rss_before_bytes": base_rss,
        "cells": cells,
    }


# ---------------------------------------------------------------------------
# Вывод
# ---------------------------------------------------------------------------


def _print_table(report: dict[str, Any]) -> None:
    """Человеческая таблица в stderr; stdout остаётся машиночитаемым JSON."""
    out = sys.stderr
    sample = report["sample"]
    print(
        f"\nвыборка: product={sample['product_id']} чанков={sample['chunks']} "
        f"из {sample['rows_total']} (шаг {sample['stride']}), "
        f"средняя длина {sample['mean_chars']} символов\n",
        file=out,
    )
    precision = report.get("precision", [])
    if precision:
        print("точность (cuda):", file=out)
    header = f"{'вариант':>8} {'чанк/с':>9} {'VRAM MiB':>10} {'RSS MiB':>9} {'1-cos med':>11} {'p95':>9} {'max':>9} {'top10':>7} {'det':>10}"
    print(header, file=out)
    for item in precision:
        if "error" in item:
            print(f"{item['variant']:>8} ОТКАЗ: {item['error']}", file=out)
            continue
        gap = item["agreement"]["cos_gap"]
        det = item["determinism_max_abs_delta"]
        print(
            f"{item['variant']:>8} {item['chunks_per_s']:>9.3f} "
            f"{item['peak_vram_bytes'] / 2**20:>10.0f} {item['peak_rss_bytes'] / 2**20:>9.0f} "
            f"{gap['median']:>11.2e} {gap['p95']:>9.2e} {gap['max']:>9.2e} "
            f"{item['agreement']['top10_overlap']:>7.4f} "
            f"{('—' if det is None else f'{det:.2e}'):>10}",
            file=out,
        )
    cpu = report.get("cpu")
    if cpu:
        print(f"\nCPU (вариант {cpu['variant']}, срез {cpu['chunks']} чанков):", file=out)
        print(f"{'потоки':>7} {'батч':>5} {'чанк/с':>9} {'ядер':>7}", file=out)
        for cell in cpu["cells"]:
            print(
                f"{cell['threads']:>7} {cell['batch_size']:>5} "
                f"{cell['chunks_per_s']:>9.3f} {cell['cpu_ratio']:>7.2f}",
                file=out,
            )


def _parse_ints(raw: str) -> list[int]:
    return [int(part) for part in raw.replace(" ", "").split(",") if part]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Замер точности эмбеддинга и параллелизма CPU на срезе корпуса"
    )
    parser.add_argument("--db", type=Path, required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--product", required=True, help="product_id из corpus_chunks")
    parser.add_argument("--sample", type=int, default=512, help="чанков в срезе точности")
    parser.add_argument(
        "--variants",
        default="fp32,tf32,bf16,fp16",
        help="варианты точности cuda через запятую (fp32 обязан быть первым)",
    )
    parser.add_argument(
        "--batch-sizes", default="32,64,128", help="батчи CPU-развёртки через запятую"
    )
    parser.add_argument("--threads", default="1,4,8,12", help="потоки CPU-развёртки через запятую")
    parser.add_argument("--repeats", type=int, default=2, help="прогонов на вариант (детерминизм)")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="идентификатор модели в HF-кэше")
    parser.add_argument(
        "--variant-batch", type=int, default=DEFAULT_VARIANT_BATCH, help="батч вариантов точности"
    )
    parser.add_argument(
        "--cpu-sample", type=int, default=256, help="чанков в срезе CPU-развёртки"
    )
    parser.add_argument(
        "--mode",
        choices=("all", "precision", "cpu"),
        default="all",
        help="что мерить: варианты точности, развёртку CPU или всё",
    )
    parser.add_argument(
        "--dump-vectors",
        type=Path,
        default=None,
        help="каталог для .npy матриц (сверка двух запусков по компонентам)",
    )
    parser.add_argument(
        "--json", action="store_true", help="не печатать человеческую таблицу в stderr"
    )
    args = parser.parse_args(argv)

    if not args.db.exists():
        raise SystemExit(f"БД не найдена: {args.db}")
    if args.repeats < 1:
        raise SystemExit("--repeats обязан быть >= 1")
    variants = [v.strip() for v in args.variants.split(",") if v.strip()]
    threads = _parse_ints(args.threads)
    batch_sizes = _parse_ints(args.batch_sizes)

    _force_offline()
    torch = _require_torch()
    with contextlib.suppress(RuntimeError):  # уже инициализировано — не ошибка замера
        torch.set_num_interop_threads(1)

    t_start = time.time()
    sample = load_sample(args.db, args.product, args.sample)
    report: dict[str, Any] = {
        "python": sys.version.split()[0],
        "torch": torch.__version__,
        "model": args.model,
        "cuda_available": bool(torch.cuda.is_available()),
        "cuda_device": _cuda_device_name(),
        "cpu_count": torch.get_num_threads(),
        "float32_matmul_precision": torch.get_float32_matmul_precision(),
        "allow_tf32": bool(torch.backends.cuda.matmul.allow_tf32),
        "sample": sample.describe(),
    }

    if args.mode in {"all", "precision"}:
        if not torch.cuda.is_available():
            raise SystemExit("варианты точности требуют cuda; карта не найдена")
        report["precision"] = run_precision_variants(
            sample,
            variants=variants,
            batch_size=args.variant_batch,
            repeats=args.repeats,
            model_id=args.model,
            dump_vectors=args.dump_vectors,
        )
    if args.mode in {"all", "cpu"}:
        # На CPU объявленный вариант один: fp32. tf32/bf16/fp16 — механизмы cuda
        # (autocast), а аппаратного bf16 у этого хоста нет (нет avx512_bf16).
        report["cpu"] = run_cpu_sweep(
            sample,
            threads=threads,
            batch_sizes=batch_sizes,
            variant="fp32",
            model_id=args.model,
            cpu_sample=args.cpu_sample,
        )

    report["elapsed_s"] = round(time.time() - t_start, 1)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not args.json:
        _print_table(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())