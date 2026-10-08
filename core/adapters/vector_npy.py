"""Векторный индекс — brute-force cosine по продукт-партициям (W8/T8.4, §3.2/02 §B.2).

Роль в coHound: плотное (dense) плечо ретривала (профиль **R1**, 02 §B.3). Реализация
намеренно проста — brute-force косинус по ``float16``-матрице партиции; ANN (faiss/Qdrant)
подключается позже через тот же ``VectorIndexPort`` (§15).

**Раскладка — единый формат.** Модуль не читает ``.npy`` напрямую: он делегирует
``adapters/index_format.read_partition`` (единственный носитель раскладки, K1) и потому
не может разойтись с писателем ``index_npy.py``. Векторы хранятся как ``float16`` и
приводятся к ``float32`` для скалярного произведения (точность косинуса).

**Продукт-скоупед (I17).** Перебираются **только** партиции ``query.product_ids``;
оценка продукта не видит чужие партиции. Пустой список даёт пустой результат.

**Провенанс (R15).** ``index_format`` хранит лишь векторы и ``chunk_ids``, а
``RetrievedChunk`` требует ``text``/``heading_path``/``content_hash``. Поэтому адаптер
принимает **резолвер метаданных чанка** (``chunk_lookup``): ``Mapping[str, Chunk]`` либо
callable ``chunk_id -> Chunk | None``. Резолвер предоставляет корпус/store (composition
root); это шов между индексом и хранилищем чанков. Чанки, отсутствующие в резолвере,
пропускаются (метаданные — обязательная часть провенанса).

**Вектор запроса обязателен.** Эмбеддинг текста — дело вызывающего (Ш5); если пришёл
``VectorQuery`` без ``vector`` (только ``text``), поднимается ``ValueError``.

**Детерминизм (R7).** Ранжирование по ``(-score, chunk_id)``; при равных косинусах
порядок фиксирован ``chunk_id``.

**Ленивый импорт.** ``numpy`` импортируется только в реальном пути; модуль импортируется
и конструируется без него. Нейтрален (§1.3).
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from core.adapters.index_format import read_partition
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk, VectorQuery

__all__ = [
    "VectorIndexUnavailable",
    "NpyVectorIndex",
    "numpy_available",
]

#: Резолвер метаданных чанка: ``Mapping`` или callable по ``chunk_id``.
ChunkLookup = Mapping[str, Chunk] | Callable[[str], Chunk | None]


def numpy_available() -> bool:
    """Установлен ли ``numpy`` (проверка без импорта)."""
    return importlib.util.find_spec("numpy") is not None


class VectorIndexUnavailable(RuntimeError):
    """Векторный индекс недоступен: нет ``numpy``."""


class NpyVectorIndex:
    """Brute-force cosine по ``.npy`` продукт-партициям — ``VectorIndexPort`` (§3.2, I17).

    :param index_root: корень ``index_store/``.
    :param package: пакет категории (``packages/{case_id}`` — ядро не знает имён).
    :param build_id: идентификатор иммутабельной сборки (K1/K2).
    :param dim: размерность вектора (из эмбеддера/манифеста; часть версии, §13).
    :param chunk_lookup: резолвер метаданных чанка (``Mapping``/callable), предоставляется
        корпусом/store. Без него ``RetrievedChunk`` не собрать (нужны text/heading/hash).

    Атрибуты ``build_id``/``dim`` — контракт порта, доступны сразу после конструирования.
    """

    def __init__(
        self,
        index_root: str | Path,
        package: str,
        build_id: str,
        dim: int,
        chunk_lookup: ChunkLookup,
    ) -> None:
        if dim <= 0:
            raise ValueError(f"dim обязан быть положительным, получено {dim!r}")
        if not build_id:
            raise ValueError("build_id обязан быть непустым")
        self.index_root = Path(index_root)
        self.package = package
        self.build_id = build_id
        self.dim = dim
        self._lookup = _normalize_lookup(chunk_lookup)

    @property
    def available(self) -> bool:
        """Готов ли адаптер (установлен ли ``numpy``)."""
        return numpy_available()

    # -- VectorIndexPort.shape ------------------------------------------ #

    def shard_ids(self, product_id: str) -> list[str]:
        """Идентификатор(ы) партиции продукта.

        Текущая раскладка — одна партиция на ``product_id`` (сателлиты индексируются
        в партицию продукта-владельца), поэтому отображение 1:1.
        """
        return [product_id]

    def search(self, q: VectorQuery) -> list[RetrievedChunk]:
        """Brute-force cosine, продукт-скоупед (I17), tie-break по ``chunk_id`` (R7).

        :raises VectorIndexUnavailable: если ``numpy`` не установлен.
        :raises ValueError: если ``vector`` не задан или его размерность ≠ ``dim``.
        :raises FileNotFoundError: если партиция запрошенного продукта не зафиксирована.
        """
        if q.vector is None:
            raise ValueError(
                "NpyVectorIndex: VectorQuery.vector обязателен — эмбеддинг текста "
                "выполняет вызывающий (Ш5); поиск по одному text не определён."
            )
        if q.top_k <= 0 or not q.product_ids:
            return []
        np = _require_numpy()
        query_vector = _unit_vector(np, q.vector, self.dim)

        scored: list[tuple[float, str, str, Chunk]] = []
        for product_id in dict.fromkeys(q.product_ids):
            partition = read_partition(
                self.index_root, self.package, self.build_id, product_id
            )
            matrix = np.asarray(partition.vectors, dtype=np.float32)
            if matrix.ndim != 2 or matrix.shape[1] != self.dim:
                raise ValueError(
                    f"партиция {product_id!r}: форма {matrix.shape} несовместима "
                    f"с dim={self.dim}"
                )
            if len(partition.chunk_ids) != matrix.shape[0]:
                raise ValueError(
                    f"партиция {product_id!r}: {len(partition.chunk_ids)} chunk_id "
                    f"на {matrix.shape[0]} векторов"
                )
            similarities = _cosine_scores(np, matrix, query_vector)
            for chunk_id, score in zip(partition.chunk_ids, similarities):
                meta = self._lookup(chunk_id)
                if meta is None:
                    continue
                scored.append((float(score), chunk_id, product_id, meta))

        scored.sort(key=lambda item: (-item[0], item[1]))
        return [
            RetrievedChunk(
                chunk_id=chunk_id,
                product_id=product_id,
                text=meta.text,
                content_hash=meta.content_hash,
                heading_path=meta.heading_path,
                build_id=self.build_id,
                score=score,
            )
            for score, chunk_id, product_id, meta in scored[: q.top_k]
        ]


def _normalize_lookup(chunk_lookup: ChunkLookup) -> Callable[[str], Chunk | None]:
    """Приводит ``Mapping``/callable к callable-резолверу ``chunk_id -> Chunk | None``."""
    if isinstance(chunk_lookup, Mapping):
        mapping = chunk_lookup
        return lambda chunk_id: mapping.get(chunk_id)
    if callable(chunk_lookup):
        return chunk_lookup
    raise ValueError(
        "chunk_lookup обязан быть Mapping или callable (chunk_id -> Chunk | None)"
    )


def _unit_vector(np: Any, vector: Sequence[float], dim: int) -> Any:
    """Нормализует вектор запроса к единичной длине (нулевой остаётся нулевым)."""
    values = np.asarray(list(vector), dtype=np.float32)
    if values.ndim != 1 or values.shape[0] != dim:
        raise ValueError(
            f"вектор запроса: размерность {values.shape} не совпадает с dim={dim}"
        )
    norm = float(np.linalg.norm(values))
    if norm == 0.0:
        return values
    return values / norm


def _cosine_scores(np: Any, matrix: Any, query_vector: Any) -> Any:
    """Косинусы строк ``matrix`` к единичному ``query_vector`` (0 для нулевых строк)."""
    norms = np.linalg.norm(matrix, axis=1)
    denominators = norms * float(np.linalg.norm(query_vector))
    dots = matrix @ query_vector
    with np.errstate(divide="ignore", invalid="ignore"):
        scores = np.where(denominators > 0.0, dots / denominators, 0.0)
    return scores


def _require_numpy() -> Any:
    """Возвращает модуль ``numpy`` или поднимает ``VectorIndexUnavailable``."""
    if not numpy_available():
        raise VectorIndexUnavailable(
            "numpy не установлен: векторный индекс недоступен. "
            "Установите зависимость 'retrieval' (05 §2.3)."
        )
    import numpy

    return numpy
