"""Замер оси ``EMBED_INPUT_FIELDS``: меняет ли заголовок в входе эмбеддера ранжирование.

**Вопрос.** ``chunking_spec.embed_input`` объявлен контрактом (``run_id``), но его значение
``("text",)`` — допущение, а не измерение (O19 плана 26). Включение заголовка стоит
пере-эмбеддинга почти всего корпуса: непустой ``heading_path`` у **238 001** из **253 306**
чанков (94.0 %, замер 16.11.2026 по ``corpus_chunks`` стенда). Платить это имеет смысл только
если ранжирование меняется — и вот это «меняется ли» и мерит прибор.

**Базовый вариант читается из сборки, а не пересчитывается.** Векторы, лежащие в партиции
``index_store``, посчитаны **ровно** с составом ``("text",)`` — это и есть исполненное
условие прогона. Пересчитывать их заново значило бы сравнивать не два состава входа, а два
пути счёта (мой и продакшен-путь сборки), и разница вариантов включала бы разницу ядер. Поэтому
вариант, чьи поля равны ``("text",)``, берётся из партиции, а сравниваются только варианты,
которые в сборке не лежат. Побочная выгода — встроенный контроль: число базового варианта
обязано совпасть с числом прибора O14 на том же профиле, иначе прибор мерит что-то не то.

**Что именно измеряется.** Профиль ``R1`` (только dense). Это не упрощение, а изоляция:
``R0`` — лексика, на неё ось не влияет вовсе (BM25 смотрит на текст чанка, а не на вектор);
``R2`` смешивает лексику и dense, поэтому эффект оси в нём ограничен эффектом в ``R1`` и
отдельного числа не даёт. Запросы рендерятся и эмбеддятся **продакшен-путём**
(:func:`core.pipeline.evaluate.precompute_query_vectors`), ретривал — тоже
(:func:`core.pipeline.evaluate.retrieve_for_cell`), метрики — функциями документного эталона
(``core.pipeline.retrieval_docs``). Своего только одно: матрица документов вместо чтения
партиции на каждый запрос.

**Цена публикуется рядом с числом.** Сколько чанков попало в кэш, сколько пересчитано, сколько
секунд, пик RSS/VRAM. Без этого «стало лучше на N пунктов» не отвечает на вопрос «стоит ли
платить».

**Что НЕ измеряется.** ``R2``/``R0`` (см. выше), качество вердиктов (ось меняет адреса
документов, а не доказательства — это предмет O15/O18), поведение на других продуктах
(``--product`` называет один; число принадлежит ему).

**Режим питания — объявленный вход, а не вывод из признаков.** Скорость на этом хосте
зависит от питания (USB-C против комплектного БП), а программно отличить одно от другого
нельзя: ``power.limit`` у мобильной карты отдаётся как ``[N/A]``, ``cpufreq`` в WSL2 не
проброшен, ``BAT1 power_now`` = 0. Поэтому режим **объявляет оператор** (``--power-mode``) и
он попадает в отчёт **как есть**: прибор не нормализует значение и не угадывает режим по
признакам. Наблюдённое (``ac_online``, ``battery_status``, частота/потребление/температура
карты, счётчик ``SW Power Capping`` в микросекундах) публикуется рядом как **контроль**, а
не как источник режима: ``battery_status == "Discharging"`` не переписывает объявленное
значение и не трогает числа — расхождение объявленного с наблюдённым читает человек.
Счётчик троттлинга публикуется **парой «до/после»** и не сворачивается в разницу: разница
теряет факт «до тоже был не ноль», то есть что карта была ограничена ещё до замера.
``unknown`` (дефолт) печатается явно: отсутствие факта должно быть видно, а не выглядеть
как «всё в порядке».

Запуск::

    .venv/bin/python scripts/measure_embed_input_variant.py \
      --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \
      --golden packages/ru-pg-dbms/sources/retrieval_golden_docs.json \
      --product postgrespro --variants text,heading --power-mode ac-barrel \
      --out-json /tmp/opencode/o19.json

Нейтрален (§1.3): продукт и эталон — аргументы, а не константы.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Корень репозитория на sys.path: скрипт запускается как `python scripts/…py`, и тогда
# sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

#: Объявленные варианты состава входа. Имя — то, что попадает в отчёт; порядок полей —
#: тот же, что у ``EMBED_INPUT_FIELDS`` (объявленный порядок, а не «как удобно склеить»).
VARIANTS: dict[str, tuple[str, ...]] = {
    "text": ("text",),
    "heading": ("heading_path", "text"),
    "heading+context": ("heading_path", "context_prefix", "text"),
}

#: Режим питания по умолчанию: «оператор не объявил». Печатается явно — отсутствие факта
#: не должно выглядеть как «всё в порядке» (см. docstring прибора).
DEFAULT_POWER_MODE = "unknown"

#: Прокси-признаки питания: то, что на этом хосте вообще наблюдаемо (WSL2, мобильная карта).
#: Объявленный режим отсюда **не выводится** — это контроль, а не источник.
AC_ONLINE_PATH = Path("/sys/class/power_supply/AC1/online")
BATTERY_STATUS_PATH = Path("/sys/class/power_supply/BAT1/status")
NVIDIA_SMI = "nvidia-smi"

#: Поля снимка карты: ``None`` — не измерено (не «ноль»).
GPU_FIELDS: tuple[str, ...] = (
    "sm_clock_mhz",
    "power_draw_w",
    "temperature_c",
    "sw_power_cap_us",
)


@dataclass(frozen=True)
class _Row:
    """Чанк продукта из ``corpus_chunks``: минимум для входа эмбеддера и ранжирования."""

    chunk_id: str
    doc_id: str
    product_id: str
    text: str
    heading_path: str
    context_prefix: str
    content_hash: str


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "замер оси EMBED_INPUT_FIELDS на документном эталоне: меняет ли заголовок "
            "в входе эмбеддера ранжирование (O19 плана 26)"
        )
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--golden", required=True, help="retrieval_golden_docs.json")
    parser.add_argument(
        "--product",
        required=True,
        help="product_id, на котором мерить (матрица держится в памяти — один продукт)",
    )
    parser.add_argument(
        "--variants",
        default="text,heading",
        help=f"варианты через запятую из {', '.join(VARIANTS)}; первый — базовый",
    )
    parser.add_argument(
        "--profile", default="R1", help="профиль ретривала; ось влияет только на R1"
    )
    parser.add_argument(
        "--cutoff-k", type=int, default=0, help="K эталона; 0 — из runs.retrieval_cutoff_K"
    )
    parser.add_argument(
        "--index-root",
        default="",
        help="корень index_store (пусто — $COHOUND_HOME/index_store)",
    )
    parser.add_argument(
        "--recompute-baseline",
        action="store_true",
        help=(
            "пересчитать и вариант с составом (text,), а не брать векторы сборки: нужно "
            "только чтобы проверить сам прибор — числа перестают быть сравнимыми с O14"
        ),
    )
    parser.add_argument(
        "--limit-chunks",
        type=int,
        default=0,
        help="N чанков равномерным шагом (0 — все); срез делает прогон дымовым",
    )
    parser.add_argument(
        "--batch-size", type=int, default=0, help="батч эмбеддинга (0 — дефолт исполнения)"
    )
    parser.add_argument(
        "--deploy", default="config/deploy.yaml", help="deploy-конфиг (оси эмбеддера)"
    )
    parser.add_argument(
        "--cache-dir",
        default="",
        help="каталог embed_cache (пусто — COHOUND_HOME/embed_cache, если он есть)",
    )
    parser.add_argument(
        "--power-mode",
        default=DEFAULT_POWER_MODE,
        help=(
            "объявленный оператором режим питания (ac-barrel/ac-usbc/battery/…): попадает "
            "в отчёт как есть, прибор его не выводит из признаков"
        ),
    )
    parser.add_argument("--out-json", metavar="PATH", help="машинный отчёт")
    parser.add_argument(
        "--out-csv",
        metavar="PATH",
        help="по-ячеечно: ранг под каждым вариантом (дифф ранжирования читается глазами)",
    )
    return parser.parse_args(argv)


def _resolve_variants(raw: str) -> list[str]:
    """Имена вариантов из списка; неизвестное имя — отказ со списком допустимых."""
    names = [item.strip() for item in raw.split(",") if item.strip()]
    if not names:
        raise SystemExit("measure: --variants пуст")
    unknown = [name for name in names if name not in VARIANTS]
    if unknown:
        raise SystemExit(
            f"measure: неизвестный вариант {', '.join(unknown)} "
            f"(допустимо: {', '.join(VARIANTS)})"
        )
    if len(set(names)) != len(names):
        raise SystemExit("measure: вариант указан дважды — сравнение было бы с самим собой")
    return names


def _load_rows(
    conn: sqlite3.Connection, product: str
) -> tuple[dict[str, _Row], list[str]]:
    """Чанки продукта: карта ``chunk_id → строка`` и порядок ``chunk_id`` по возрастанию.

    Порядок — ``chunk_id``, а не вставки: матрица и отчёт обязаны быть воспроизводимыми
    по содержимому БД, а не по истории записи. Порядок партиции (если базовый вариант
    читается из сборки) сверяется с этой картой отдельно.
    """
    rows: dict[str, _Row] = {}
    for row in conn.execute(
        "select chunk_id, doc_id, product_id, text, heading_path, context_prefix, "
        "content_hash from corpus_chunks where product_id = ? order by chunk_id",
        (product,),
    ):
        rows[str(row[0])] = _Row(
            chunk_id=str(row[0]),
            doc_id=str(row[1]),
            product_id=str(row[2]),
            text=str(row[3] or ""),
            heading_path=str(row[4] or ""),
            context_prefix=str(row[5] or ""),
            content_hash=str(row[6] or ""),
        )
    if not rows:
        raise SystemExit(f"measure: у продукта {product!r} нет чанков в корпусе")
    return rows, sorted(rows)


def _subset(order: Sequence[str], limit: int) -> list[str]:
    """Срез равномерным шагом, а не первыми N — и это не педантизм.

    Замер 16.11.2026: первые 2000 чанков ``postgrespro`` покрывают **168** документов из
    **15 188** (1.1 %), потому что ``chunk_id`` упорядочен по документу, а корпус
    начинается с одного раздела. Такой срез даёт ``hit@K = 1.000`` на всех вариантах: в
    матрице просто нет чужих документов, которые могли бы отобрать место.
    """
    if limit <= 0 or limit >= len(order):
        return list(order)
    step = len(order) / limit
    return [order[int(index * step)] for index in range(limit)]


def _is_smoke_only(order: Sequence[str], full_order: Sequence[str]) -> bool:
    """Прогон дымовой: измерено меньше чанков, чем в корпусе.

    Отдельной функцией, а не строкой в :func:`main`, — чтобы свойство отчёта
    проверялось без БД, эмбеддера и GPU. Потерянный флаг публикует число среза как
    число оси: в матрице меньше документов, и место в top-K делят другие соседи.
    """
    return len(order) < len(full_order)


class _MatrixIndex:
    """``VectorIndexPort`` поверх матрицы в памяти: тот же контракт, что у партиции.

    Ранжирование повторяет правило адаптера (``core/adapters/vector_npy.py``): косинус по
    нормализованным строкам, tie-break по ``chunk_id``, кандидаты — не ниже ``limit``-го
    балла. Это не «похожий» порядок, а тот же: иначе сравнение вариантов мерило бы
    разницу ядер, а не разницу входа.
    """

    def __init__(self, rows: Sequence[_Row], matrix: Any, *, build_id: str, dim: int):
        import numpy as np

        self.build_id = build_id
        self.dim = dim
        self._np = np
        self._rows = list(rows)
        self._chunk_ids = [row.chunk_id for row in rows]
        # Приведение к float32 и нормализация — один раз на вариант, а не на запрос:
        # косинус не зависит от длины вектора, и нормировать 207k строк на каждый запрос
        # значило бы платить за это K раз.
        dense = np.array(matrix, dtype=np.float32)
        norms = np.linalg.norm(dense, axis=1, keepdims=True)
        norms[norms == 0.0] = 1.0
        # Деление **на месте**: ``dense / norms`` дало бы вторую матрицу на 207k×1024
        # (847 MB) рядом с первой, и пик RSS вырос бы на её размер без пользы.
        dense /= norms
        self._matrix = dense

    def shard_ids(self, product_id: str) -> list[str]:
        return [product_id]

    def search(self, q: Any) -> list[Any]:
        return self.search_many([q])[0]

    def search_many(self, queries: Sequence[Any]) -> list[list[Any]]:
        np = self._np
        out: list[list[Any]] = []
        for query in queries:
            vector = getattr(query, "vector", None)
            if vector is None:
                raise ValueError("запрос без вектора: эмбеддинг — дело вызывающего")
            q = np.asarray(vector, dtype=np.float32)
            norm = float(np.linalg.norm(q)) or 1.0
            scores = self._matrix @ (q / norm)
            out.append(self._top(scores, int(getattr(query, "top_k", 0))))
        return out

    def _top(self, scores: Any, limit: int) -> list[Any]:
        """Top-``limit`` тем же ключом, что у адаптера: ``(-балл, chunk_id)``."""
        from core.ports.vector_index import RetrievedChunk

        np = self._np
        total = int(scores.shape[0])
        if limit >= total or not bool(np.all(np.isfinite(scores))):
            candidates = list(range(total))
        else:
            threshold = np.partition(scores, total - limit)[total - limit]
            candidates = np.flatnonzero(scores >= threshold).tolist()
        candidates.sort(key=lambda row: (-float(scores[row]), self._chunk_ids[row]))
        hits: list[Any] = []
        for row in candidates[:limit]:
            chunk = self._rows[row]
            hits.append(
                RetrievedChunk(
                    chunk_id=chunk.chunk_id,
                    product_id=chunk.product_id,
                    text=chunk.text,
                    content_hash=chunk.content_hash,
                    heading_path=chunk.heading_path,
                    build_id=self.build_id,
                    score=float(scores[row]),
                    context_prefix=chunk.context_prefix,
                )
            )
        return hits


def _variant_matrix(
    rows: Sequence[_Row],
    fields: tuple[str, ...],
    *,
    embedder: Any,
    cache: Any,
    embed_model_id: str,
    embed_device: str,
    embed_precision: str,
    batch_size: int,
) -> tuple[Any, dict[str, Any]]:
    """Матрица варианта через **продакшен-путь** эмбеддинга (дедуп + кэш + батчи).

    Состав входа подменяется на уровне объявления (:data:`core.util.embed_input.
    EMBED_INPUT_FIELDS`), а не передачей аргумента: исполнение читает ровно это
    объявление, поэтому замер идёт через тот же код, что и прогон. Иначе прибор мерил бы
    свою копию логики склейки, а не ось.
    """
    from core.pipeline.corpus_index import _embed_batched
    from core.util import embed_input

    declared = embed_input.EMBED_INPUT_FIELDS
    embed_input.EMBED_INPUT_FIELDS = fields
    try:
        # Строки передаются как есть, а не пересобираются в ``CorpusChunk``: контракт
        # ``embed_input_text`` — ``getattr`` по объявленному имени поля, и :class:`_Row`
        # несёт ровно эти поля. Пydantic-валидация 207k моделей стоила бы памяти и
        # времени, ничего не проверяя: тексты уже лежат в БД, а инварианты ``CorpusChunk``
        # проверены на записи.
        chunks = rows
        started = time.perf_counter()
        matrix = _embed_batched(
            embedder,
            chunks,
            batch_size=batch_size,
            cache=cache,
            embed_model_id=embed_model_id,
            embed_device=embed_device,
            embed_precision=embed_precision,
        )
        seconds = time.perf_counter() - started
    finally:
        # Объявление возвращается в исходное состояние даже при падении: прибор не имеет
        # права оставить процесс в состоянии, где ось включена.
        embed_input.EMBED_INPUT_FIELDS = declared
    stats = {
        "source": "recomputed",
        "chunks": len(chunks),
        "seconds": round(seconds, 3),
        "chunks_per_s": round(len(chunks) / seconds, 2) if seconds > 0 else None,
        "cache": cache.stats() if cache is not None else None,
    }
    return matrix, stats


def _partition_matrix(
    index_root: Path, package_id: str, build_id: str, product: str
) -> tuple[Any, list[str], dict[str, Any]]:
    """Векторы продукта **из сборки** — исполненное условие ``("text",)``.

    Это не оптимизация: партиция посчитана продакшен-путём, и брать её значит сравнивать
    два состава входа, а не два пути счёта. Побочно — встроенный контроль: число базового
    варианта обязано совпасть с числом прибора O14 на том же профиле.

    ``chunks`` здесь — размер партиции, то есть измеренное число только пока среза нет:
    ``--limit-chunks`` режет матрицу ниже, и тогда ``chunks`` правит
    :func:`_apply_partition_slice`, а размер партиции остаётся в ``partition_chunks``.
    """
    from core.adapters.index_format import read_partition
    from core.pipeline.composition import read_composition

    view = read_composition(index_root, package_id, build_id)
    if view is None:
        raise SystemExit(
            f"measure: композиция {build_id!r} не найдена под {index_root}: "
            "сборка индекса не разрешается"
        )
    partition_build_id = view.build_id_for(product)
    data = read_partition(index_root, package_id, partition_build_id, product)
    return (
        data.vectors,
        list(data.chunk_ids),
        {
            "source": "partition",
            "partition_build_id": partition_build_id,
            "composition_id": view.composition_id,
            "chunks": len(data.chunk_ids),
            "partition_chunks": len(data.chunk_ids),
            "seconds": 0.0,
            "chunks_per_s": None,
            "cache": None,
        },
    )


def _apply_partition_slice(
    order: Sequence[str],
    partition_ids: Sequence[str],
    matrix: Any,
    stats: dict[str, Any],
) -> tuple[list[str], Any]:
    """Срез базовой матрицы по позициям партиции: порядок и строки режутся **вместе**.

    Позиция ``i`` в ``partition_ids`` — строка ``i`` матрицы, поэтому фильтр применяется к
    обоим спискам одним и тем же ``positions``: разойтись они не могут. Публикуется при
    этом число **измеренных** чанков, а не размер партиции — иначе строка базового варианта
    утверждает 206 931 чанк там, где измерено 3 000 (замер 16.11.2026, ``--limit-chunks
    3000``), и читатель сравнивает дымовое число с O14 как число оси.
    """
    keep = set(order)
    positions = [index for index, cid in enumerate(partition_ids) if cid in keep]
    sliced_order = [partition_ids[index] for index in positions]
    stats["chunks"] = len(sliced_order)
    return sliced_order, matrix[positions]


def _evaluate(
    *,
    entries: Sequence[Any],
    params: dict[str, Any],
    profile: str,
    cutoff_k: int,
    index: Any,
    query_vectors: dict[str, list[float]],
    chunk_doc: dict[str, str],
    doc_ids: set[str],
) -> tuple[Any, list[dict[str, Any]]]:
    """Метрики варианта: продакшен-ретривал + метрики документного эталона."""
    from core.pipeline.evaluate import retrieve_for_cell
    from core.pipeline.retrieval_docs import (
        DocQueryResult,
        document_ids_for,
        evaluate_documents,
    )

    results: list[DocQueryResult] = []
    per_cell: list[dict[str, Any]] = []
    for entry in entries:
        param = params.get(entry.param_id)
        if param is None:
            raise SystemExit(
                f"measure: ячейка {entry.cell}: param_id {entry.param_id!r} отсутствует "
                "в каталоге прогона — запрос не рендерится"
            )
        retrieval = retrieve_for_cell(
            param,
            product_id=entry.product_id,
            profile=profile,
            cutoff_k=cutoff_k,
            lexical_index=None,
            vector_index=index,
            query_vectors=query_vectors,
        )
        retrieved = document_ids_for(
            [chunk.chunk_id for chunk in retrieval.chunks], chunk_doc, top_k=cutoff_k
        )
        rank = next(
            (
                position
                for position, doc_id in enumerate(retrieved, start=1)
                if doc_id in entry.expected_doc_ids
            ),
            None,
        )
        results.append(
            DocQueryResult(
                cell=entry.cell,
                product_id=entry.product_id,
                param_id=entry.param_id,
                expected_doc_ids=tuple(entry.expected_doc_ids),
                retrieved_doc_ids=retrieved,
                rank=rank,
                unresolved=not any(doc in doc_ids for doc in entry.expected_doc_ids),
            )
        )
        per_cell.append({"cell": entry.cell, "rank": rank})
    report = evaluate_documents(results, profile=profile, k=cutoff_k, bootstrap=1, seed=0)
    return report, per_cell


def _peak_rss_bytes() -> int:
    """Пик RSS процесса: ``ru_maxrss`` в KiB на Linux, байты в отчёте."""
    import resource

    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


def _load_average() -> list[float] | None:
    """Средняя нагрузка хоста (1/5/15 мин) на момент замера.

    Нужна по той же причине, что в приборе реранкера: ``chunks_per_s`` на общем хосте без
    неё невоспроизводимо. Замер 16.11.2026 шёл при занятом хосте (рядом работали pytest и
    другой прибор), и без этого поля читатель не отличил бы цену оси от цены конкуренции.
    ``None`` — счётчика нет (не Linux): отсутствие факта не факт отсутствия.
    """
    import os

    try:
        return [round(value, 2) for value in os.getloadavg()]
    except OSError:
        return None


def _read_text(path: Path) -> str | None:
    """Содержимое sysfs-файла; ``None`` — файла нет или он не читается.

    Не исключение и не ``0``: «счётчика нет» и «счётчик показал ноль» — разные факты, а
    прибор обязан различать их так же, как ``None`` и ``0`` в остальных полях отчёта.
    """
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _ac_online() -> bool | None:
    """``AC1/online``: питание от сети. ``None`` — файла нет или значение не 0/1."""
    raw = _read_text(AC_ONLINE_PATH)
    if raw in {"0", "1"}:
        return raw == "1"
    return None


def _battery_status() -> str | None:
    """``BAT1/status`` как есть (``Charging``/``Discharging``/…); ``None`` — не читается."""
    return _read_text(BATTERY_STATUS_PATH) or None


def _nvidia_smi(args: Sequence[str], *, timeout_s: float = 5.0) -> str | None:
    """Вывод ``nvidia-smi``; ``None`` — команды нет, карты нет, отказ или таймаут.

    Отказ инструмента — это не факт о железе: ``None``, а не исключение и не ``0``. Таймаут
    короткий намеренно: опрос карты не имеет права задерживать замер.
    """
    try:
        completed = subprocess.run(
            [NVIDIA_SMI, *args],
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_s,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def _as_int(raw: str) -> int | None:
    """Целое из поля вывода; ``[N/A]``/пусто/мусор — ``None``, а не ``0``."""
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _as_float(raw: str) -> float | None:
    """Дробное из поля вывода; ``[N/A]``/пусто/мусор — ``None``, а не ``0.0``."""
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _gpu_fields_none() -> dict[str, Any]:
    """Снимок карты «не измерено»: ключи обязаны быть в отчёте и тогда, когда их нет.

    Поле, которое «иногда есть», иначе не отличить от «не измеряли» при разборе старых
    артефактов.
    """
    return dict.fromkeys(GPU_FIELDS)


def _parse_gpu_metrics(text: str) -> dict[str, Any]:
    """Строка ``--query-gpu=clocks.sm,power.draw,temperature.gpu`` → поля снимка.

    ``[N/A]`` у мобильной карты даёт ``None``, а не ``0``: «карта не отдаёт потребление» —
    это не «потребление ноль».
    """
    metrics = _gpu_fields_none()
    line = next((item for item in text.splitlines() if item.strip()), "")
    parts = [part.strip() for part in line.split(",")]
    if len(parts) != 3:
        return metrics
    metrics["sm_clock_mhz"] = _as_int(parts[0])
    metrics["power_draw_w"] = _as_float(parts[1])
    metrics["temperature_c"] = _as_int(parts[2])
    return metrics


def _parse_sw_power_cap_us(text: str) -> int | None:
    """Счётчик ``SW Power Capping`` из ``nvidia-smi -q -d PERFORMANCE``, микросекунды.

    Берётся именно **счётчик** (секция ``Clocks Event Reasons Counters``), а не флаг
    ``SW Power Cap: Active``: флаг говорит «ограничена сейчас», счётчик — «за время замера
    карта была ограничена по питанию столько-то микросекунд». Строки нет — ``None``;
    ``0 us`` — измеренный ноль, и он остаётся нулём.
    """
    for line in text.splitlines():
        label, sep, value = line.partition(":")
        if not sep or not label.strip().lower().startswith("sw power cap"):
            continue
        value = value.strip()
        if not value.endswith("us"):
            continue  # «Active»/«Not Active» — флаг, а не счётчик
        return _as_int(value[: -len("us")].strip())
    return None


def _power_snapshot(*, with_gpu: bool) -> dict[str, Any]:
    """Прокси-признаки питания на момент вызова: **контроль**, а не источник режима.

    Снимается в окне замера, а не «на момент старта»: карта и питание меняются по ходу
    прогона, и признак, взятый один раз в начале, описывал бы другое окно. ``with_gpu``
    включается только объявленным ``cuda`` — при ``cpu`` карта не опрашивается вовсе, и
    поля остаются ``None`` (не измерено), а не нулями.

    Режим питания из этих признаков **не выводится**: объявленное (``--power-mode``) и
    наблюдённое живут раздельно, расхождение между ними — повод для человека.
    """
    snapshot: dict[str, Any] = {
        "ac_online": _ac_online(),
        "battery_status": _battery_status(),
    }
    if not with_gpu:
        snapshot.update(_gpu_fields_none())
        return snapshot
    metrics = _nvidia_smi(
        ["--query-gpu=clocks.sm,power.draw,temperature.gpu", "--format=csv,noheader,nounits"]
    )
    snapshot.update(_parse_gpu_metrics(metrics) if metrics is not None else _gpu_fields_none())
    counters = _nvidia_smi(["-q", "-d", "PERFORMANCE"])
    snapshot["sw_power_cap_us"] = (
        _parse_sw_power_cap_us(counters) if counters is not None else None
    )
    return snapshot


def _declared_power_mode(args: argparse.Namespace) -> str:
    """Объявленный режим питания — **только** из аргумента оператора.

    Отдельная функция ради проверяемости: режим нельзя вывести из наблюдённых признаков
    (``_ac_online``/``_battery_status``/снимок карты), потому что USB-C и комплектный БП
    на этом хосте неразличимы, а «угаданный» режим в отчёте — это ложь о условии замера.
    Признаки публикуются рядом как контроль; расхождение читает человек.
    """
    return str(args.power_mode)


def _power_window(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Окно варианта: снимки «до» и «после» рядом, **без производных**.

    Разница ``after − before`` здесь намеренно не публикуется: она теряет факт «до тоже
    был не ноль», то есть что карта была ограничена по питанию ещё до замера. Читатель
    видит оба абсолютных значения и решает сам.
    """
    return {"before": dict(before), "after": dict(after)}


def _peak_vram_bytes() -> int | None:
    """Пик VRAM в окне замера; ``None`` — счёт невозможен (нет cuda/torch)."""
    try:
        import torch
    except Exception:  # noqa: BLE001 — счёт невозможен, а не «ноль»
        return None
    if not torch.cuda.is_available():
        return None
    return int(torch.cuda.max_memory_allocated())


def _condition_fields(stats: dict[str, Any]) -> dict[str, Any]:
    """Условия замера для строки варианта: измеренное число и размер партиции — раздельно.

    ``chunks`` — сколько чанков реально попало в матрицу (после среза). ``partition_chunks``
    — размер сборки, из которой взят базовый вариант; у пересчитанного варианта его нет
    (``None``): он партицию не читал, и выдумывать ему это поле нельзя.
    """
    return {
        "source": stats.get("source"),
        "partition_build_id": stats.get("partition_build_id"),
        "chunks": stats.get("chunks"),
        "partition_chunks": stats.get("partition_chunks"),
        "seconds": stats.get("seconds"),
        "chunks_per_s": stats.get("chunks_per_s"),
        "cache": stats.get("cache"),
    }


def _require_same_chunks(reports: Sequence[dict[str, Any]]) -> None:
    """Инвариант сравнимости: у всех вариантов отчёта одно и то же число чанков.

    Варианты, померившиеся на разных матрицах, дают числа, в разницу которых входит разница
    корпусов, — публиковать их рядом как «эффект оси» нельзя. Сегодня срез режется в одном
    месте (:func:`_apply_partition_slice`); проверка стоит здесь, чтобы второй путь среза не
    прошёл молча.
    """
    measured = [(str(report.get("variant")), report.get("chunks")) for report in reports]
    if len({chunks for _, chunks in measured}) > 1:
        detail = ", ".join(f"{name}={chunks}" for name, chunks in measured)
        raise SystemExit(
            "measure: варианты померились на разном числе чанков — числа несравнимы "
            f"({detail})"
        )


def _build_payload(
    reports: Sequence[dict[str, Any]],
    base: dict[str, Any],
    *,
    peak_rss_bytes: int | None,
    peak_vram_bytes: int | None,
    load_average: list[float] | None = None,
) -> dict[str, Any]:
    """Собирает отчёт и **на этом же пути** проверяет сравнимость вариантов.

    Инвариант стоит здесь, а не в :func:`main`, по той же причине, по которой страница
    чанка выводится функцией: путь публикации один, и проверка неотделима от него.
    Вызов в ``main`` можно удалить (или обойти вторым путём сборки) — и тогда отчёт с
    несравнимыми строками выйдет молча; здесь он выйти не может.
    """
    _require_same_chunks(reports)
    return {
        **base,
        "peak_rss_bytes": peak_rss_bytes,
        "peak_vram_bytes": peak_vram_bytes,
        "load_average": load_average,
        "variants": list(reports),
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"measure: БД не найдена: {db_path}", file=sys.stderr)
        return 2
    variants = _resolve_variants(args.variants)
    index_root = _resolve_index_root(args.index_root)

    from core.pipeline.retrieval_docs import load_doc_golden

    case_id, golden_run_id, entries = load_doc_golden(args.golden)
    selected = [entry for entry in entries if entry.product_id == args.product]
    if not selected:
        print(
            f"measure: в эталоне {args.golden} нет ячеек продукта {args.product!r} "
            f"(есть: {', '.join(sorted({e.product_id for e in entries}))})",
            file=sys.stderr,
        )
        return 2

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        run_row = conn.execute(
            "select run_id, package_id, index_build_id, retrieval_cutoff_K from runs limit 1"
        ).fetchone()
        if run_row is None:
            print("measure: в БД нет записи прогона", file=sys.stderr)
            return 2
        run_id = str(run_row["run_id"])
        if golden_run_id and golden_run_id != run_id:
            print(
                f"measure: эталон скомпилирован на прогоне {golden_run_id!r}, а БД — "
                f"{run_id!r}: doc_id разных корпусов несравнимы",
                file=sys.stderr,
            )
            return 2
        package_id = str(run_row["package_id"] or case_id)
        build_id = str(run_row["index_build_id"] or "")
        cutoff_k = args.cutoff_k or int(run_row["retrieval_cutoff_K"] or 0) or 10
        rows_by_id, full_order = _load_rows(conn, args.product)
        params = _load_params(db_path)
    finally:
        conn.close()

    # Порядок строк задаётся **один раз** и общий для всех вариантов: матрица и строки
    # обязаны соответствовать друг другу, иначе вектор попадёт чужому чанку.
    baseline_fields = VARIANTS[variants[0]]
    order = _subset(full_order, args.limit_chunks)
    baseline_stats: dict[str, Any] | None = None
    baseline_matrix = None
    use_partition = (
        not args.recompute_baseline
        and baseline_fields == ("text",)
        and build_id
        and index_root is not None
    )
    if use_partition:
        baseline_matrix, partition_ids, baseline_stats = _partition_matrix(
            index_root, package_id, build_id, args.product
        )
        if len(partition_ids) != len(full_order) or set(partition_ids) != set(full_order):
            print(
                "measure: состав партиции и корпуса не совпадает "
                f"({len(partition_ids)} чанков в сборке, {len(full_order)} в corpus_chunks) — "
                "базовый вариант нельзя взять из сборки: это были бы разные корпуса",
                file=sys.stderr,
            )
            return 2
        # Срез идёт по позициям партиции: её порядок и есть порядок матрицы.
        if len(order) != len(partition_ids):
            order, baseline_matrix = _apply_partition_slice(
                order, partition_ids, baseline_matrix, baseline_stats
            )
        else:
            order = partition_ids

    rows = [rows_by_id[chunk_id] for chunk_id in order]
    chunk_doc = {row.chunk_id: row.doc_id for row in rows}
    doc_ids = {row.doc_id for row in rows}
    smoke_only = _is_smoke_only(order, full_order)

    from core.cli.wiring import _resolve_embedder
    from core.config.deploy import load_deploy_config

    deploy = load_deploy_config(args.deploy)
    notices: list[str] = []
    embedder = _resolve_embedder(
        deploy.embed.device,
        precision=deploy.embed.precision,
        model_id=deploy.embed.model_id,
        revision=deploy.embed.revision,
        notices=notices,
    )
    if embedder is None:
        print(
            "measure: embed.model_id не объявлен в "
            f"{args.deploy} — вектор нечем считать, а ось — про вектор",
            file=sys.stderr,
        )
        return 2
    embed_model_id = str(getattr(embedder, "model_id", "")) or str(deploy.embed.model_id)

    cache = _open_cache(args.cache_dir)
    from core.pipeline.corpus_index import EMBED_BATCH_CHUNKS

    batch_size = args.batch_size or EMBED_BATCH_CHUNKS

    # Карта опрашивается только при объявленном ``cuda``: при ``cpu`` признаки карты к
    # числу не относятся, и ``None`` там честнее нулей. Объявленный режим питания
    # (``args.power_mode``) сюда не входит — он публикуется как есть, без вывода.
    with_gpu = str(deploy.embed.device) == "cuda"

    # Векторы запросов считаются **один раз** и не зависят от варианта: ось меняет вход
    # документов, а не запросов. Иначе разница включала бы разницу в запросе.
    from core.pipeline.evaluate import precompute_query_vectors

    query_vectors = precompute_query_vectors(
        [params[entry.param_id] for entry in selected],
        profile=args.profile,
        embedder=embedder,
    )
    if not query_vectors:
        print(
            f"measure: профиль {args.profile} не даёт векторов запросов — ось мерить не на чем",
            file=sys.stderr,
        )
        return 2

    reports: list[dict[str, Any]] = []
    cells_by_variant: dict[str, list[dict[str, Any]]] = {}
    # Снимок «до» первого варианта; дальше «после» предыдущего = «до» следующего, поэтому
    # окна стыкуются и ни одно не остаётся без границы.
    power_before = _power_snapshot(with_gpu=with_gpu)
    for name in variants:
        fields = VARIANTS[name]
        if name == variants[0] and baseline_matrix is not None:
            matrix, stats = baseline_matrix, baseline_stats or {}
        else:
            matrix, stats = _variant_matrix(
                rows,
                fields,
                embedder=embedder,
                cache=cache,
                embed_model_id=embed_model_id,
                embed_device=str(deploy.embed.device),
                embed_precision=str(deploy.embed.precision),
                batch_size=batch_size,
            )
        index = _MatrixIndex(
            rows,
            matrix,
            build_id=f"variant:{name}",
            dim=int(getattr(embedder, "dim", 0) or matrix.shape[1]),
        )
        # fp16-матрица больше не нужна: индекс держит float32-копию. Без этого на корпусе
        # 207k чанков рядом лежали бы 424 MB + 847 MB, хотя работать нужно со второй.
        del matrix
        report, per_cell = _evaluate(
            entries=selected,
            params=params,
            profile=args.profile,
            cutoff_k=cutoff_k,
            index=index,
            query_vectors=query_vectors,
            chunk_doc=chunk_doc,
            doc_ids=doc_ids,
        )
        power_after = _power_snapshot(with_gpu=with_gpu)
        reports.append(
            {
                "variant": name,
                "fields": list(fields),
                **_condition_fields(stats),
                "power": _power_window(power_before, power_after),
                "metrics": {
                    "hit_at_k": report.hit_at_k,
                    "recall_at_k": report.recall_at_k,
                    "mrr": report.mrr,
                    "ndcg_at_k": getattr(report, "ndcg_at_k", None),
                    "hit_ci": list(report.hit_ci),
                    "scored": report.scored,
                    "unresolved_cells": report.unresolved_cells,
                },
            }
        )
        power_before = power_after
        cells_by_variant[name] = per_cell
        # Индекс отпускается до следующего варианта: две float32-матрицы рядом — это
        # второй пик RSS (847 MB на корпусе 207k×1024) там, где нужен один.
        del index
        if name == variants[0]:
            baseline_matrix = None

    payload = _build_payload(
        reports,
        {
            "instrument": "measure_embed_input_variant",
            "measured_at": time.strftime("%Y-%m-%d"),
            "smoke_only": smoke_only,
            "run_id": run_id,
            "case_id": case_id,
            "package_id": package_id,
            "index_build_id": build_id,
            "product": args.product,
            "profile": args.profile,
            "cutoff_k": cutoff_k,
            "golden": str(args.golden),
            "golden_cells": len(entries),
            "cells_in_scope": len(selected),
            "chunks_total": len(full_order),
            "chunks_measured": len(order),
            "embed_model_id": embed_model_id,
            "embed_device": str(deploy.embed.device),
            "embed_precision": str(deploy.embed.precision),
            "batch_size": batch_size,
            # Объявленный режим питания — как есть, без разбора и нормализации: прибор не
            # угадывает режим по признакам (см. docstring модуля).
            "power_mode": _declared_power_mode(args),
            "notices": notices,
        },
        peak_rss_bytes=_peak_rss_bytes(),
        peak_vram_bytes=_peak_vram_bytes(),
        load_average=_load_average(),
    )
    if args.out_json:
        path = Path(args.out_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    if args.out_csv:
        _write_cells_csv(Path(args.out_csv), variants, cells_by_variant)

    _print_summary(payload)
    return 0


def _resolve_index_root(raw: str) -> Path | None:
    """Корень ``index_store``: аргумент, иначе ``$COHOUND_HOME/index_store``."""
    import os

    if raw:
        return Path(raw).expanduser()
    home = os.environ.get("COHOUND_HOME")
    if not home:
        return None
    candidate = Path(home) / "index_store"
    return candidate if candidate.is_dir() else None


def _open_cache(raw: str) -> Any:
    """Кэш векторов, если каталог назван или существует по раскладке хоста.

    Кэш здесь — не удобство: он делает вариант ``text`` почти бесплатным (прогон уже
    эмбеддил эти чанки) и публикует, сколько векторов варианта пришли готовыми. Без
    каталога прибор работает без кэша и честно это печатает.
    """
    from core.pipeline.embed_cache import EmbedCache

    if raw:
        return EmbedCache(Path(raw).expanduser())
    import os

    home = os.environ.get("COHOUND_HOME")
    if not home:
        return None
    candidate = Path(home) / "embed_cache"
    return EmbedCache(candidate) if candidate.is_dir() else None


def _load_params(db_path: Path) -> dict[str, Any]:
    """Каталог прогона из ``plan_inputs.json`` рядом с БД (снимок, не пакет)."""
    from core.domain.models import CatalogRow

    snapshot = db_path.parent / "plan_inputs.json"
    if not snapshot.is_file():
        raise SystemExit(
            f"measure: нет снимка входов {snapshot} — запросы обязаны быть теми же, "
            "что рендерила фаза"
        )
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    params = payload.get("params")
    if not isinstance(params, list) or not params:
        raise SystemExit(f"measure: {snapshot}: params пуст или не список")
    return {str(item["param_id"]): CatalogRow.model_validate(item) for item in params}


def _write_cells_csv(
    path: Path, variants: Sequence[str], cells: dict[str, list[dict[str, Any]]]
) -> None:
    """По-ячеечно: ранг под каждым вариантом — дифф ранжирования читается глазами."""
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    order = [item["cell"] for item in cells[variants[0]]]
    ranks = {name: {item["cell"]: item["rank"] for item in cells[name]} for name in variants}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["cell", *variants, "delta"])
        for cell in order:
            values = [ranks[name].get(cell) for name in variants]
            base, other = values[0], values[1] if len(values) > 1 else None
            delta = "" if base is None or other is None else base - other
            writer.writerow(
                [cell, *["" if value is None else value for value in values], delta]
            )


def _measured(value: Any, unit: str = "", *, digits: int | None = None) -> str:
    """Значение с единицей; ``None`` печатается как «не измерено», а не как ноль."""
    if value is None:
        return "не измерено"
    text = f"{value:.{digits}f}" if digits is not None else str(value)
    return f"{text}{unit}"


def _tri_state(value: bool | None) -> str:
    """Тройное состояние признака: ``None`` — не измерено, а не «нет»."""
    if value is None:
        return "не измерено"
    return "да" if value else "нет"


def _counter_pair(before: Any, after: Any) -> str:
    """Счётчик троттлинга парой «до→после»: разница потеряла бы «до тоже был не ноль»."""
    if before is None and after is None:
        return "SW Power Capping не измерено"
    left = "не измерено" if before is None else str(before)
    right = "не измерено" if after is None else str(after)
    return f"SW Power Capping {left}→{right} us"


def _power_line(payload: dict[str, Any]) -> str:
    """Режим питания и признаки одной строкой: объявленное и наблюдённое — раздельно.

    Режим печатается **объявленный**, признаки — наблюдённые, и рядом, а не вместо: если
    оператор объявил ``ac-barrel``, а батарея при этом ``Discharging``, строка покажет оба
    факта. Строка ничего не решает — расхождение читает человек.
    """
    mode = str(payload.get("power_mode") or DEFAULT_POWER_MODE)
    declared = "не объявлен" if mode == DEFAULT_POWER_MODE else mode
    variants = payload.get("variants") or []
    power = (variants[0].get("power") or {}) if variants else {}
    before = power.get("before") or {}
    after = power.get("after") or {}
    parts = [
        f"питание: режим {declared}",
        f"AC online {_tri_state(before.get('ac_online'))}",
        f"батарея {before.get('battery_status') or 'не измерено'}",
        f"SM {_measured(before.get('sm_clock_mhz'), ' МГц')}",
        f"потребление {_measured(before.get('power_draw_w'), ' Вт', digits=1)}",
        f"температура {_measured(before.get('temperature_c'), ' °C')}",
        _counter_pair(before.get("sw_power_cap_us"), after.get("sw_power_cap_us")),
    ]
    return " · ".join(parts)


def _print_power_conditions(payload: dict[str, Any]) -> None:
    """Режим питания и признаки; при ``unknown`` — предупреждение.

    Молчать при необъявленном режиме нельзя: числа скорости тогда не привязаны к условию
    питания, и сравнивать их между прогонами (USB-C против комплектного БП) не с чем.
    Отсутствие факта печатается, а не выглядит как «всё в порядке».
    """
    print(_power_line(payload))
    mode = str(payload.get("power_mode") or DEFAULT_POWER_MODE)
    if mode == DEFAULT_POWER_MODE:
        print(
            "ВНИМАНИЕ: режим питания не объявлен (--power-mode unknown) — числа скорости "
            "не привязаны к режиму питания и между прогонами не сравниваются."
        )


def _print_summary(payload: dict[str, Any]) -> None:
    """Сводка: метрики вариантов рядом, цена каждого — тоже рядом."""
    print(
        f"run {payload['run_id']} · продукт {payload['product']} · профиль "
        f"{payload['profile']} · K={payload['cutoff_k']} · ячеек "
        f"{payload['cells_in_scope']}/{payload['golden_cells']} · чанков "
        f"{payload['chunks_measured']}/{payload['chunks_total']}"
    )
    if payload.get("smoke_only"):
        print(
            "ДЫМОВОЙ ПРОГОН (срез корпуса): метрики НЕ сопоставимы с прогоном на полном "
            "корпусе — в матрице меньше документов, и место в top-K делят другие соседи. "
            "Числа ниже проверяют, что прибор работает, а не силу оси."
        )
    header = (
        f"{'вариант':<20}{'hit@K':>8}{'recall':>8}{'MRR':>8}{'nDCG':>8}"
        f"{'сек':>9}{'чанк/с':>9}{'источник':>12}"
    )
    print(header)
    for report in payload["variants"]:
        metrics = report["metrics"]
        ndcg = metrics["ndcg_at_k"]
        print(
            f"{report['variant']:<20}{metrics['hit_at_k']:>8.3f}"
            f"{metrics['recall_at_k']:>8.3f}{metrics['mrr']:>8.3f}"
            f"{(0.0 if ndcg is None else ndcg):>8.3f}"
            f"{(report['seconds'] or 0.0):>9.1f}"
            f"{(report['chunks_per_s'] or 0.0):>9.2f}"
            f"{report['source']!s:>12}"
        )
    peak_vram = payload["peak_vram_bytes"]
    print(
        f"пик RSS {payload['peak_rss_bytes'] / 2**20:.0f} MiB · "
        f"пик VRAM {'не измерен' if peak_vram is None else f'{peak_vram / 2**20:.0f} MiB'}"
    )
    load = payload.get("load_average")
    if load:
        # Нагрузка хоста — условие числа, а не украшение: на общем хосте цена оси и цена
        # конкуренции за него неотличимы, если напечатано только «чанк/с».
        print(f"нагрузка хоста (1/5/15 мин): {load[0]:.2f} / {load[1]:.2f} / {load[2]:.2f}")
    # Режим питания и его прокси — одной строкой: скорость на этом хосте от него зависит,
    # а отличить USB-C от комплектного БП программно нельзя (см. docstring модуля).
    _print_power_conditions(payload)


if __name__ == "__main__":
    raise SystemExit(main())
