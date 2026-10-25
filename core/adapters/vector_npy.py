"""Векторный индекс — brute-force cosine по продукт-партициям (W8/T8.4, §3.2/02 §B.2).

Роль в coHound: плотное (dense) плечо ретривала (профиль **R1**, 02 §B.3). Реализация
намеренно проста — brute-force косинус по ``float16``-матрице партиции; ANN (faiss/Qdrant)
подключается позже через тот же ``VectorIndexPort`` (§14).

**Раскладка — единый формат.** Модуль не читает ``.npy`` напрямую: он делегирует
``adapters/index_format.read_partition`` (единственный носитель раскладки, K1) и потому
не может разойтись с писателем ``index_npy.py``. Векторы хранятся как ``float16`` и
приводятся к ``float32`` для скалярного произведения (точность косинуса).

**Продукт-скоупед (I17).** Перебираются **только** партиции ``query.product_ids``;
оценка продукта не видит чужие партиции. Пустой список даёт пустой результат.

**Композиция (09 §8.4).** При ``view`` (``CompositionViewPort``) каждый продукт читается
из своей ``partition_build_id``, а ``RetrievedChunk.build_id`` несёт id этой партиции;
``self.build_id``/``self.composition_id`` — id композиции (провенанс сборки). Без ``view``
поведение прежнее: один ``build_id`` на все продукты.

**Провенанс (R15).** ``index_format`` хранит лишь векторы и ``chunk_ids``, а
``RetrievedChunk`` требует ``text``/``heading_path``/``content_hash``. Поэтому адаптер
принимает **резолвер метаданных чанка** (``chunk_lookup``): ``Mapping[str, Chunk]`` либо
callable ``chunk_id -> Chunk | None``. Резолвер предоставляет корпус/store (composition
root); это шов между индексом и хранилищем чанков. Чанки, отсутствующие в резолвере,
пропускаются (метаданные — обязательная часть провенанса).

**Вектор запроса обязателен.** Эмбеддинг текста — дело вызывающего (Ш5); если пришёл
``VectorQuery`` без ``vector`` (только ``text``), поднимается ``ValueError``.

**Метаданные резолвятся только для кандидатов.** Баллы считаются для всей партиции
(иначе косинус не получить), но ``chunk_lookup`` вызывается лишь для тех строк, которые
могут войти в ``top_k`` — и в порядке ``(-score, chunk_id)``, чтобы «пропустить
отсутствующий чанк» (метаданных нет — провенанса нет) сохранялось дословно. Резолв
всех чанков партиции был не оптимизацией, а её отсутствием: при composition root
``chunk_lookup=store.get_chunk`` это один point-lookup в SQLite на каждый чанк партиции
(11 400 запросов в БД на поиск с ``top_k=10``).

**Детерминизм (R7).** Ранжирование по ``(-score, chunk_id)``; при равных косинусах
порядок фиксирован ``chunk_id``. Отбор кандидатов порогом не меняет порядок: на пороге
берутся **все** строки, а сортировка кандидатов идёт тем же ключом, что и раньше.

**Ленивый импорт.** ``numpy`` импортируется только в реальном пути; модуль импортируется
и конструируется без него. Нейтрален (§1.3).
"""

from __future__ import annotations

import importlib.util
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping, Sequence
from itertools import islice
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.adapters.index_format import read_partition
from core.ports.types import Chunk
from core.ports.vector_index import (
    CompositionViewPort,
    RetrievedChunk,
    VectorQuery,
)

__all__ = [
    "DEFAULT_PARTITION_CACHE_BYTES",
    "PartitionCache",
    "VectorIndexUnavailable",
    "NpyVectorIndex",
    "numpy_available",
]

#: Резолвер метаданных чанка: ``Mapping`` или callable по ``chunk_id``.
ChunkLookup = Mapping[str, Chunk] | Callable[[str], Chunk | None]

#: Ключ кэша партиции: ``(index_root, package, partition_build_id, product_id)``.
PartitionKey = tuple[str, str, str, str]

#: Бюджет кэша fp32-матриц партиций по умолчанию — 256 MiB.
#:
#: Число обосновано замером, а не «сколько влезет»: партиция наблюдаемого масштаба
#: (11 400 чанков × 1024 dim × 4 байта) — 46.7 MB, крупнейшая из измеренных (32 647)
#: — 133.7 MB. Рабочее множество одного батч-прохода — продукт плюс его сателлиты,
#: то есть 1–5 партиций; 256 MiB покрывает пять партиций наблюдаемого масштаба
#: (233 MB) и одну крупнейшую с запасом, оставаясь ниже резерва памяти воркера
#: (``core/util/resources.py``: ``MEM_RESERVE_MB``=512, ``PARENT_FLOOR_MB``=500).
#: Заявленный порог роста партиции (200 000 чанков, doc 14 §4) требует 800 MB: такая
#: партиция **не** кэшируется (запись больше бюджета не вытесняет кэш), и путь остаётся
#: корректным — цена в повторной конверсии fp32, а не в OOM. Бюджет — параметр
#: конструктора, а не константа поведения.
DEFAULT_PARTITION_CACHE_BYTES = 256 * 1024 * 1024


def numpy_available() -> bool:
    """Установлен ли ``numpy`` (проверка без импорта)."""
    return importlib.util.find_spec("numpy") is not None


class VectorIndexUnavailable(RuntimeError):
    """Векторный индекс недоступен: нет ``numpy``."""


@dataclass(frozen=True)
class _Partition:
    """Партиция, приведённая к виду, в котором считается косинус.

    :param matrix: ``float32``-матрица ``(N, dim)`` — конверсия из ``float16`` сделана
        **один раз** (на партицию, а не на запрос).
    :param norms: нормы строк ``(N,)`` — от запроса не зависят, считаются вместе с матрицей.
    :param chunk_ids: ``chunk_id`` строк в порядке строк матрицы.
    :param nbytes: размер матрицы в байтах — учёт бюджета кэша.
    """

    matrix: Any
    norms: Any
    chunk_ids: tuple[str, ...]
    nbytes: int


class PartitionCache:
    """LRU-кэш партиций в памяти процесса; ограничен бюджетом и очищаем.

    Партиции **иммутабельны** (K2/§13): содержимое ``index_store/{package}/{build}/{product}``
    не меняется на месте, поэтому кэш не может отдать устаревшие векторы — новый раунд
    даёт новую сборку, а с ней и новый ключ. Ключ включает ``partition_build_id`` именно
    поэтому, а не из осторожности.

    Вытеснение — по давности использования. Запись больше бюджета **не** кэшируется:
    иначе она вытеснила бы всё и вернулась бы на следующем запросе, забрав с собой
    остальное (трешинг вместо кэша).

    :param budget_bytes: бюджет в байтах; ``0`` — кэш выключен (прежнее поведение по
        стоимости: конверсия на каждый запрос).
    :raises ValueError: если ``budget_bytes`` отрицателен.
    """

    def __init__(self, budget_bytes: int = DEFAULT_PARTITION_CACHE_BYTES) -> None:
        if budget_bytes < 0:
            raise ValueError(f"бюджет кэша обязан быть ≥ 0, получено {budget_bytes!r}")
        self.budget_bytes = budget_bytes
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self._items: OrderedDict[PartitionKey, _Partition] = OrderedDict()
        self._bytes = 0

    @property
    def bytes_used(self) -> int:
        """Сколько байт матриц сейчас в кэше."""
        return self._bytes

    def __len__(self) -> int:
        return len(self._items)

    def get(self, key: PartitionKey) -> _Partition | None:
        """Возвращает партицию по ключу и обновляет давность использования."""
        item = self._items.get(key)
        if item is None:
            self.misses += 1
            return None
        self._items.move_to_end(key)
        self.hits += 1
        return item

    def put(self, key: PartitionKey, partition: _Partition) -> None:
        """Кладёт партицию, вытесняя старейшие записи до бюджета."""
        if self.budget_bytes == 0 or partition.nbytes > self.budget_bytes:
            return
        previous = self._items.pop(key, None)
        if previous is not None:
            self._bytes -= previous.nbytes
        self._items[key] = partition
        self._bytes += partition.nbytes
        while self._bytes > self.budget_bytes and self._items:
            _, evicted = self._items.popitem(last=False)
            self._bytes -= evicted.nbytes
            self.evictions += 1

    def clear(self) -> None:
        """Полностью очищает кэш (память возвращается куче)."""
        self._items.clear()
        self._bytes = 0


class NpyVectorIndex:
    """Brute-force cosine по ``.npy`` продукт-партициям — ``VectorIndexPort`` (§3.2, I17).

    :param index_root: корень ``index_store/``.
    :param package: пакет категории (``packages/{case_id}`` — ядро не знает имён).
    :param build_id: идентификатор иммутабельной сборки (K1/K2). При заданном ``view``
        обязан совпадать с ``view.composition_id`` либо быть ``None``.
    :param dim: размерность вектора (из эмбеддера/манифеста; часть версии, §13).
    :param chunk_lookup: резолвер метаданных чанка (``Mapping``/callable), предоставляется
        корпусом/store. Без него ``RetrievedChunk`` не собрать (нужны text/heading/hash).
    :param view: вид композиции партиций (09 §8.4, ``core.pipeline.composition``).
        ``None`` — прежняя раскладка: один ``build_id`` на все продукты (поведение
        байт-идентично до-композиционному).
    :param cache_budget_bytes: бюджет кэша партиций в байтах (см. :class:`PartitionCache`
        и :data:`DEFAULT_PARTITION_CACHE_BYTES`); ``0`` — без кэша.

    Атрибуты ``build_id``/``dim`` — контракт порта, доступны сразу после конструирования.
    При ``view`` ``build_id`` — это ``composition_id`` (провенанс сборки), а каждая
    продукт-партиция читается по своему ``partition_build_id``.
    """

    def __init__(
        self,
        index_root: str | Path,
        package: str,
        build_id: str | None,
        dim: int,
        chunk_lookup: ChunkLookup,
        *,
        view: CompositionViewPort | None = None,
        cache_budget_bytes: int = DEFAULT_PARTITION_CACHE_BYTES,
    ) -> None:
        if dim <= 0:
            raise ValueError(f"dim обязан быть положительным, получено {dim!r}")
        if view is not None:
            if not view.composition_id:
                raise ValueError("view.composition_id обязан быть непустым")
            if build_id is not None and build_id != view.composition_id:
                raise ValueError(
                    f"build_id {build_id!r} не совпадает с composition_id "
                    f"{view.composition_id!r}: двусмысленный провенанс сборки"
                )
            resolved_build_id = view.composition_id
        else:
            if not build_id:
                raise ValueError("build_id обязан быть непустым")
            resolved_build_id = build_id
        self.index_root = Path(index_root)
        self.package = package
        self.build_id = resolved_build_id
        self.composition_id = view.composition_id if view is not None else None
        self.dim = dim
        self._view = view
        self._lookup = _normalize_lookup(chunk_lookup)
        self.partition_cache = PartitionCache(cache_budget_bytes)

    @property
    def available(self) -> bool:
        """Готов ли адаптер (установлен ли ``numpy``)."""
        return numpy_available()

    # -- VectorIndexPort.shape ------------------------------------------ #

    def shard_ids(self, product_id: str) -> list[str]:
        """Идентификатор(ы) партиции продукта.

        Раскладка — одна партиция на ``product_id`` (сателлиты индексируются в партицию
        продукта-владельца), поэтому отображение 1:1. При ``view`` продукт вне композиции
        не имеет партиций: пустой список, а не чужая/несуществующая сборка.
        """
        if self._view is not None and product_id not in self._view.members:
            return []
        return [product_id]

    def search(self, q: VectorQuery) -> list[RetrievedChunk]:
        """Brute-force cosine, продукт-скоупед (I17), tie-break по ``chunk_id`` (R7).

        Одиночный поиск — это батч из одного запроса (:meth:`search_many`): так у
        скалярного произведения **одно** ядро (см. оговорку о точности в
        :meth:`search_many`), и выдача не зависит от того, как вызывающий сгруппировал
        запросы.

        :raises VectorIndexUnavailable: если ``numpy`` не установлен.
        :raises ValueError: если ``vector`` не задан или его размерность ≠ ``dim``.
        :raises FileNotFoundError: если партиция запрошенного продукта не зафиксирована.
        """
        return self.search_many([q])[0]

    def search_many(self, queries: Sequence[VectorQuery]) -> list[list[RetrievedChunk]]:
        """Батч-поиск: один BLAS-вызов на партицию вместо одного на запрос.

        Смысл не в «красивее», а в замере (doc 14 §3): косинус по партиции
        11 400 × 1024 — memory-bound, и матрица, прочитанная **один** раз на 600
        запросов, даёт 0.15 мс на запрос против 4.5 мс при отдельном вызове на запрос.
        Поэтому запросы с одинаковым набором ``product_ids`` считаются вместе
        (``matrix @ Qᵀ`` одним gemm), а дальше каждый запрос получает свой top-k.

        **Оговорка о точности.** Ядро выбирается размером батча (см.
        :func:`_cosine_scores`): один запрос считается gemv — тем же ядром, что у
        прежнего кода, поэтому одиночная выдача побитово прежняя (замер на партиции
        11 400 × 1024, 600 запросов: расхождений баллов 0, |Δ| = 0.0). Батч считается
        gemm, и его блочность зависит от размера батча: балл того же запроса отличается
        в последних битах (замер: ≤ 8.2e-08, порядок top-10 совпал 600/600). Порядок и
        tie-break по ``chunk_id`` (R7) от этого не меняются — ключ сортировки тот же.
        Выбор ядра детерминирован размером батча и не зависит от того, кто и как
        сгруппировал запросы: фаза оценки всегда идёт батчем по продукту.

        :raises VectorIndexUnavailable: если ``numpy`` не установлен.
        :raises ValueError: если у какого-то запроса нет ``vector`` или его
            размерность ≠ ``dim``.
        :raises FileNotFoundError: если партиция запрошенного продукта не зафиксирована.
        """
        results: list[list[RetrievedChunk]] = [[] for _ in queries]
        if not queries:
            return results
        np = _require_numpy()
        # Вектор обязателен у каждого запроса: эмбеддинг текста — дело вызывающего (Ш5).
        units: list[Any] = []
        groups: dict[tuple[str, ...], list[int]] = {}
        for position, q in enumerate(queries):
            if q.vector is None:
                raise ValueError(
                    "NpyVectorIndex: VectorQuery.vector обязателен — эмбеддинг текста "
                    "выполняет вызывающий (Ш5); поиск по одному text не определён."
                )
            units.append(_unit_vector(np, q.vector, self.dim))
            if q.top_k <= 0 or not q.product_ids:
                continue
            groups.setdefault(tuple(dict.fromkeys(q.product_ids)), []).append(position)

        scored: list[list[tuple[float, str, str, Chunk, str]]] = [
            [] for _ in queries
        ]
        for products, positions in groups.items():
            stacked = np.stack([units[position] for position in positions])
            for product_id in products:
                # При ``view`` — своя партиция продукта; без view — прежний единый
                # build_id (байт-идентичный до-композиционный путь).
                partition_build_id = (
                    self.build_id
                    if self._view is None
                    else self._view.build_id_for(product_id)
                )
                partition = self._partition(np, partition_build_id, product_id)
                similarities = _cosine_scores(
                    np, partition.matrix, stacked, row_norms=partition.norms
                )
                for column, position in enumerate(positions):
                    scored[position].extend(
                        self._resolve_top(
                            np,
                            similarities[:, column],
                            partition,
                            product_id,
                            partition_build_id,
                            queries[position].top_k,
                        )
                    )

        return [
            _to_chunks(items, q.top_k) for items, q in zip(scored, queries)
        ]

    def _resolve_top(
        self,
        np: Any,
        scores: Any,
        partition: _Partition,
        product_id: str,
        partition_build_id: str,
        limit: int,
    ) -> list[tuple[float, str, str, Chunk, str]]:
        """Резолвит метаданные в порядке ранжирования, пока не набрано ``limit``.

        Кандидатов обычно ``limit`` (плюс равные на границе балла) — столько и стоит
        point-lookup'ов. Если часть чанков резолвером не найдена, порядок **добирается**
        следующим по рангу: пропуск не сужает выдачу (прежняя семантика «отсутствующие
        пропускаются»), но и не заставляет резолвить все чанки партиции заранее.

        «Хвост» (сортировка остатка) считается только при пропусках — поэтому обычный
        поиск не платит за гарантию полноты.
        """
        ordered = _ranked_rows(np, scores, partition.chunk_ids, limit)
        resolved: list[tuple[float, str, str, Chunk, str]] = []
        for row in islice(ordered, limit):  # кандидаты: без полного перебора
            chunk_id = partition.chunk_ids[row]
            meta = self._lookup(chunk_id)
            if meta is None:
                continue
            resolved.append(
                (float(scores[row]), chunk_id, product_id, meta, partition_build_id)
            )
        if len(resolved) < limit:  # пропуски: добираем следующими по рангу
            for row in ordered:
                chunk_id = partition.chunk_ids[row]
                meta = self._lookup(chunk_id)
                if meta is None:
                    continue
                resolved.append(
                    (float(scores[row]), chunk_id, product_id, meta, partition_build_id)
                )
                if len(resolved) >= limit:
                    break
        return resolved

    def _partition(self, np: Any, partition_build_id: str, product_id: str) -> _Partition:
        """Партиция в виде для скалярного произведения: из кэша либо с диска.

        **Конверсия fp16 → fp32 — один раз на партицию.** ``mmap`` отдаёт ``float16``
        с диска без чтения в кучу, но скалярное произведение считается в ``float32``
        (точность косинуса), и ``np.asarray(..., dtype=float32)`` каждый раз аллоцировал
        бы 46.7 MB на партицию 11 400 × 1024. Поэтому fp32-матрица и нормы строк
        считаются при первом обращении и живут в кэше; повторные запросы читают файл
        только через уже отображённые страницы.

        Валидация формы/счётчиков — здесь же: она свойство партиции, а не запроса,
        и повторять её на каждый поиск значит платить за то, что уже проверено.

        :raises FileNotFoundError: партиция не зафиксирована.
        :raises KeyError: продукт вне композиции (``view``).
        """
        key: PartitionKey = (
            str(self.index_root),
            self.package,
            partition_build_id,
            product_id,
        )
        cached = self.partition_cache.get(key)
        if cached is not None:
            return cached
        data = read_partition(
            self.index_root, self.package, partition_build_id, product_id
        )
        matrix = np.ascontiguousarray(np.asarray(data.vectors, dtype=np.float32))
        if matrix.ndim != 2 or matrix.shape[1] != self.dim:
            raise ValueError(
                f"партиция {product_id!r}: форма {matrix.shape} несовместима "
                f"с dim={self.dim}"
            )
        if len(data.chunk_ids) != matrix.shape[0]:
            raise ValueError(
                f"партиция {product_id!r}: {len(data.chunk_ids)} chunk_id "
                f"на {matrix.shape[0]} векторов"
            )
        partition = _Partition(
            matrix=matrix,
            norms=np.linalg.norm(matrix, axis=1),
            chunk_ids=tuple(data.chunk_ids),
            nbytes=int(matrix.nbytes),
        )
        self.partition_cache.put(key, partition)
        return partition


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


def _cosine_scores(
    np: Any, matrix: Any, queries: Any, *, row_norms: Any | None = None
) -> Any:
    """Косинусы строк ``matrix`` к каждому единичному вектору ``queries`` — ``(N, Q)``.

    **Одно ядро — gemm — независимо от размера батча.** Это требование детерминизма
    (R7/A3), а не оптимизация: разные ядра дают разные последние биты, и тогда
    ``hound experiment retrieval`` (мелкий батч) и фаза ``evaluate`` (крупный батч)
    ранжировали бы один и тот же запрос по-разному у близких соседей. Замер на
    партиции 11 400 × 1024: gemv против gemm давал **1 расхождение top-10 из 600**,
    а gemm при размерах батча 1 / 3 / 600 — **0 из 600**; шум между размерами батча
    ≤ 5.4e-07 при минимальном зазоре 10-го и 11-го 4.7e-06 (запас ~9×).
    Стоимость единого ядра нулевая: ``gemm(N, 1)`` на этом хосте даже быстрее
    ``gemv`` (1.44 мс против 1.59 мс) и побитово совпадает с ним.

    Кэш норм строк приходит из партиции: нормы не зависят от запроса, и повторный
    проход по матрице ради них стоил бы столько же, сколько сам поиск.

    :param queries: единичные векторы запроса: один ``(dim,)`` или матрица ``(Q, dim)``.
    :param row_norms: заранее посчитанные нормы строк ``matrix``; ``None`` — считаются здесь.
    """
    stacked = np.atleast_2d(queries)
    norms = np.linalg.norm(matrix, axis=1) if row_norms is None else row_norms
    query_norms = np.linalg.norm(stacked, axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        denominators = norms[:, None] * query_norms[None, :]
        dots = matrix @ stacked.T
        return np.where(denominators > 0.0, dots / denominators, 0.0)


def _to_chunks(
    items: Sequence[tuple[float, str, str, Chunk, str]], top_k: int
) -> list[RetrievedChunk]:
    """Готовит ``RetrievedChunk``: порядок ``(-score, chunk_id)`` (R7), срез до ``top_k``."""
    ordered = sorted(items, key=lambda item: (-item[0], item[1]))
    return [
        RetrievedChunk(
            chunk_id=chunk_id,
            product_id=product_id,
            text=meta.text,
            content_hash=meta.content_hash,
            heading_path=meta.heading_path,
            build_id=partition_build_id,
            score=score,
        )
        for score, chunk_id, product_id, meta, partition_build_id in ordered[:top_k]
    ]


def _ranked_rows(
    np: Any, scores: Any, chunk_ids: Sequence[str], limit: int
) -> Iterator[int]:
    """Индексы строк в порядке ``(-score, chunk_id)``, лениво и без полного перебора.

    Сначала отдаются строки, которые могут войти в top-``limit``: все с баллом не ниже
    ``limit``-го по убыванию, в порядке ``(-score, chunk_id)``. Поэтому равные баллы на
    границе не теряются, а tie-break остаётся прежним (R7).

    **Добор (зачем это генератор).** Резолвер метаданных может не знать чанк — тогда
    он пропускается, и место занимает следующая строка по порядку. Поэтому, если
    кандидатов не хватило, генератор по требованию продолжает перебор: «хвост»
    сортируется тем же ключом. Плата — сортировка всей партиции, но она берётся
    **только** когда в выдаче есть пропуски; обычный поиск ограничен кандидатами.

    Нечисловые баллы (``nan``/``inf`` во входных векторах) — не повод менять
    семантику: кандидатами становятся все строки, порядок решает та же сортировка.
    """
    total = int(scores.shape[0])
    if limit >= total or not bool(np.all(np.isfinite(scores))):
        candidates = list(range(total))
    else:
        threshold = np.partition(scores, total - limit)[total - limit]
        candidates = np.flatnonzero(scores >= threshold).tolist()
    candidates.sort(key=lambda row: (-float(scores[row]), chunk_ids[row]))
    yield from candidates
    if len(candidates) == total:
        return
    chosen = set(candidates)
    rest = [row for row in range(total) if row not in chosen]
    rest.sort(key=lambda row: (-float(scores[row]), chunk_ids[row]))
    yield from rest


def _require_numpy() -> Any:
    """Возвращает модуль ``numpy`` или поднимает ``VectorIndexUnavailable``."""
    if not numpy_available():
        raise VectorIndexUnavailable(
            "numpy не установлен: векторный индекс недоступен. "
            "Установите зависимость 'retrieval' (05 §2.3)."
        )
    import numpy

    return numpy
