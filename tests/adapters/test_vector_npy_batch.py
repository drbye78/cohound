"""Тесты дефектов ретривала, измеренных на production-масштабе (doc 14 §3/§6).

Проверяется ровно то, что было дефектом, а не «стало быстрее на глаз»:

* (а) выдача совпадает с прежним полным перебором — порядок ``(-score, chunk_id)``
  побитово, включая равные баллы (tie-break R7);
* (б) ``chunk_lookup`` вызывается порядка ``top_k``, а не размера партиции;
* (в) файл партиции читается один раз на N запросов (счётчик ``numpy.load``);
* (г) батч ``search_many`` даёт те же результаты, что поштучный ``search``;
* бюджет кэша партиций — явное число, переполнение вытесняет старейшее, ``0`` выключает.

Фикстуры категорийно нейтральны (§1.3); партиции пишутся единым писателем
``index_format.write_partition`` в ``tmp_path``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from core.adapters import index_format
from core.adapters.vector_npy import (
    DEFAULT_PARTITION_CACHE_BYTES,
    NpyVectorIndex,
    PartitionCache,
    numpy_available,
)
from core.ports.types import Chunk
from core.ports.vector_index import VectorQuery, batched_search

pytestmark = pytest.mark.skipif(not numpy_available(), reason="numpy не установлен")

PACKAGE = "pkg"
BUILD = "build-1"
PRODUCT = "p-alpha"
DIM = 8


# ---------------------------------------------------------------------------
# Фикстуры
# ---------------------------------------------------------------------------


def _chunk(chunk_id: str) -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        chunk_no=0,
        text=f"text-{chunk_id}",
        heading_path=f"H-{chunk_id}",
        context_prefix="",
        content_hash=f"hash-{chunk_id}",
    )


def _write(root: Path, vectors: Any, chunk_ids: list[str], product: str = PRODUCT) -> None:
    index_format.write_partition(
        root, PACKAGE, BUILD, product, vectors, chunk_ids, {"dim": DIM, "embed_model_id": "m"}
    )


def _random_vectors(count: int, seed: int = 0) -> Any:
    import numpy as np

    rng = np.random.default_rng(seed)
    return rng.standard_normal((count, DIM)).astype(np.float16)


class _CountingLookup:
    """Резолвер метаданных со счётчиком обращений и множеством «отсутствующих»."""

    def __init__(self, chunk_ids: list[str], missing: set[str] | None = None) -> None:
        self.calls: list[str] = []
        self.missing = set(missing or ())

    def __call__(self, chunk_id: str) -> Chunk | None:
        self.calls.append(chunk_id)
        return None if chunk_id in self.missing else _chunk(chunk_id)


def _legacy_search(
    root: Path, index: NpyVectorIndex, query: VectorQuery, chunk_ids: list[str]
) -> list:
    """Прежний путь: полный перебор, резолв метаданных для **каждого** чанка.

    Воспроизведён здесь локально, потому что сравнивать надо с поведением **до**
    правки, а не с текущей реализацией. Считает так же, как считал прежний код:
    косинус по всей партиции (``matrix @ vector``), затем сортировка ``(-score, id)``.
    """
    import numpy as np

    partition = index_format.read_partition(root, PACKAGE, BUILD, query.product_ids[0])
    matrix = np.asarray(partition.vectors, dtype=np.float32)
    vector = np.asarray(list(query.vector or []), dtype=np.float32)
    norm = float(np.linalg.norm(vector))
    if norm != 0.0:
        vector = vector / norm
    norms = np.linalg.norm(matrix, axis=1)
    denominators = norms * float(np.linalg.norm(vector))
    dots = matrix @ vector
    with np.errstate(divide="ignore", invalid="ignore"):
        scores = np.where(denominators > 0.0, dots / denominators, 0.0)
    scored = []
    for chunk_id, score in zip(chunk_ids, scores):
        meta = index._lookup(chunk_id)
        if meta is None:
            continue
        scored.append((float(score), chunk_id, meta))
    scored.sort(key=lambda item: (-item[0], item[1]))
    return scored[: query.top_k]


# ---------------------------------------------------------------------------
# (а) выдача = прежний полный перебор
# ---------------------------------------------------------------------------


def test_ranking_matches_legacy_full_scan(tmp_path: Path) -> None:
    """Порядок совпадает с прежним полным перебором; балл — с точностью до ядра BLAS.

    Гарантия здесь — **порядок и tie-break**, а не последние биты балла. Прежний код
    считал одиночный запрос через gemv, теперь всё идёт через gemm: это цена единого
    ядра (см. :func:`core.adapters.vector_npy._cosine_scores`), и она осознанна —
    два ядра давали расхождение порядка у близких соседей (замер: 1 из 600 запросов
    на партиции 11 400 × 1024), а один gemm при любом размере батча — 0 из 600.

    Допуск на балл — ``1e-6``: на два порядка выше наблюдаемого шума между размерами
    батча (≤ 5.4e-07) и на порядок ниже минимального наблюдаемого зазора между 10-м и
    11-м результатом (4.7e-06), то есть перестановок он не допускает.
    """
    ids = [f"c{i:03d}" for i in range(40)]
    _write(tmp_path, _random_vectors(len(ids), seed=1), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))

    import numpy as np

    rng = np.random.default_rng(2)
    for _ in range(5):
        vector = rng.standard_normal(DIM).astype(np.float32).tolist()
        query = VectorQuery(product_ids=[PRODUCT], top_k=5, vector=vector)
        legacy = _legacy_search(tmp_path, index, query, ids)
        hits = index.search(query)
        assert [c.chunk_id for c in hits] == [item[1] for item in legacy]
        for hit, (score, _, meta) in zip(hits, legacy):
            assert abs(hit.score - score) <= 1e-6
            assert hit.text == meta.text
            assert hit.content_hash == meta.content_hash
            assert hit.heading_path == meta.heading_path


def test_single_and_batched_search_agree(tmp_path: Path) -> None:
    """``search`` (батч из одного) и ``search_many`` дают **один и тот же** порядок.

    Это требование провенанса, а не удобства: ``hound experiment retrieval`` считает
    баллы мелкими батчами, фаза ``evaluate`` — крупным. Два разных ядра (gemv против
    gemm) ранжировали бы один запрос по-разному у близких соседей, и «чувствительность»
    показала бы разницу там, где изменился только размер батча.
    """
    ids = [f"c{i:03d}" for i in range(60)]
    _write(tmp_path, _random_vectors(len(ids), seed=3), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))

    import numpy as np

    rng = np.random.default_rng(4)
    vectors = [rng.standard_normal(DIM).astype(np.float32).tolist() for _ in range(24)]

    batched = index.search_many(
        [VectorQuery(product_ids=[PRODUCT], top_k=5, vector=v) for v in vectors]
    )
    for vector, batch_hits in zip(vectors, batched):
        single = index.search(VectorQuery(product_ids=[PRODUCT], top_k=5, vector=vector))
        assert [c.chunk_id for c in single] == [c.chunk_id for c in batch_hits]
        for one, many in zip(single, batch_hits):
            # Балл несёт шум блокировки BLAS (≤ 1e-6) между размерами батча; порядок — нет.
            assert abs(one.score - many.score) <= 1e-6


def test_equal_scores_tie_break_is_exact(tmp_path: Path) -> None:
    """Равные баллы разрешаются по ``chunk_id`` — побитово, как в прежнем пути (R7)."""
    ids = ["t3", "t1", "t2"]
    vector = [1.0] + [0.0] * (DIM - 1)
    _write(tmp_path, [vector, vector, vector], ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))
    hits = index.search(VectorQuery(product_ids=[PRODUCT], top_k=3, vector=vector))
    assert [c.chunk_id for c in hits] == ["t1", "t2", "t3"]
    assert all(c.score == hits[0].score for c in hits)


def test_tie_at_top_k_boundary_is_not_lost(tmp_path: Path) -> None:
    """Равные баллы **на границе** ``top_k`` не теряются: берётся tie-break, а не порядок строк."""
    # Четыре первых вектора одинаковы, ещё один — слабее.
    best = [1.0] + [0.0] * (DIM - 1)
    worse = [0.0, 1.0] + [0.0] * (DIM - 2)
    ids = ["d4", "d2", "d1", "d3", "weak"]
    _write(tmp_path, [best, best, best, best, worse], ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))
    hits = index.search(VectorQuery(product_ids=[PRODUCT], top_k=2, vector=best))
    assert [c.chunk_id for c in hits] == ["d1", "d2"]


def test_missing_chunks_are_skipped_and_backfilled(tmp_path: Path) -> None:
    """Отсутствующие чанки пропускаются, и место добирается следующим по порядку."""
    ids = [f"c{i:02d}" for i in range(12)]
    _write(tmp_path, _random_vectors(len(ids), seed=3), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))
    import numpy as np

    vector = np.random.default_rng(4).standard_normal(DIM).astype(np.float32).tolist()
    full = index.search(VectorQuery(product_ids=[PRODUCT], top_k=5, vector=vector))
    ghosts = {c.chunk_id for c in full[:2]}
    ghost_index = NpyVectorIndex(
        tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids, missing=ghosts)
    )
    hits = ghost_index.search(VectorQuery(product_ids=[PRODUCT], top_k=5, vector=vector))
    assert len(hits) == 5
    assert not ghosts & {c.chunk_id for c in hits}
    # Добор идёт по тому же порядку: остаток прежней выдачи, затем следующая строка.
    assert [c.chunk_id for c in hits[:3]] == [c.chunk_id for c in full[2:5]]


# ---------------------------------------------------------------------------
# (б) число обращений к резолверу метаданных
# ---------------------------------------------------------------------------


def test_lookup_calls_bounded_by_top_k(tmp_path: Path) -> None:
    """``chunk_lookup`` зовётся порядка ``top_k``, а не размера партиции."""
    ids = [f"c{i:03d}" for i in range(200)]
    _write(tmp_path, _random_vectors(len(ids), seed=5), ids)
    lookup = _CountingLookup(ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, lookup)
    import numpy as np

    vector = np.random.default_rng(6).standard_normal(DIM).astype(np.float32).tolist()
    index.search(VectorQuery(product_ids=[PRODUCT], top_k=10, vector=vector))
    assert len(lookup.calls) == 10


def test_lookup_calls_bounded_by_top_k_plus_misses(tmp_path: Path) -> None:
    """Пропуски метаданных стоят ровно столько резолвов, сколько их нужно добрать."""
    ids = [f"c{i:03d}" for i in range(200)]
    _write(tmp_path, _random_vectors(len(ids), seed=7), ids)
    lookup = _CountingLookup(ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, lookup)
    import numpy as np

    vector = np.random.default_rng(8).standard_normal(DIM).astype(np.float32).tolist()
    top = index.search(VectorQuery(product_ids=[PRODUCT], top_k=10, vector=vector))
    ghosts = {c.chunk_id for c in top[:3]}
    lookup.missing = ghosts
    lookup.calls.clear()
    hits = index.search(VectorQuery(product_ids=[PRODUCT], top_k=10, vector=vector))
    assert len(hits) == 10
    assert len(lookup.calls) == 13  # 10 нужных + 3 отсутствующих


# ---------------------------------------------------------------------------
# (в) файл партиции читается один раз
# ---------------------------------------------------------------------------


def test_partition_file_read_once_for_many_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``np.load`` партиции — один раз на N запросов (кэш fp32-матрицы)."""
    ids = [f"c{i:02d}" for i in range(30)]
    _write(tmp_path, _random_vectors(len(ids), seed=9), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))

    import numpy as np

    calls = {"n": 0}
    original = np.load

    def counting_load(*args: Any, **kwargs: Any):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(np, "load", counting_load)
    rng = np.random.default_rng(10)
    for _ in range(5):
        index.search(
            VectorQuery(
                product_ids=[PRODUCT], top_k=3, vector=rng.standard_normal(DIM).tolist()
            )
        )
    assert calls["n"] == 1
    assert index.partition_cache.hits == 4
    assert index.partition_cache.misses == 1


def test_cache_disabled_reads_every_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``cache_budget_bytes=0`` — прежняя стоимость: чтение на каждый запрос."""
    ids = [f"c{i:02d}" for i in range(10)]
    _write(tmp_path, _random_vectors(len(ids), seed=11), ids)
    index = NpyVectorIndex(
        tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids), cache_budget_bytes=0
    )
    import numpy as np

    calls = {"n": 0}
    original = np.load

    def counting_load(*args: Any, **kwargs: Any):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(np, "load", counting_load)
    vector = np.ones(DIM).astype(np.float32).tolist()
    for _ in range(3):
        index.search(VectorQuery(product_ids=[PRODUCT], top_k=2, vector=vector))
    assert calls["n"] == 3


def test_read_partition_uses_mmap_by_default(tmp_path: Path) -> None:
    """Векторы читаются отображением (fp16 на диске), а не копией в кучу."""
    import numpy as np

    ids = ["a", "b"]
    _write(tmp_path, _random_vectors(2, seed=12), ids)
    mapped = index_format.read_partition(tmp_path, PACKAGE, BUILD, PRODUCT)
    assert isinstance(mapped.vectors, np.memmap)
    assert mapped.vectors.dtype.name == "float16"
    copied = index_format.read_partition(tmp_path, PACKAGE, BUILD, PRODUCT, mmap=False)
    assert not isinstance(copied.vectors, np.memmap)
    assert np.array_equal(np.asarray(mapped.vectors), np.asarray(copied.vectors))


def test_read_partition_falls_back_without_mmap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``numpy`` без mmap — прежний путь (полное чтение), без падения."""
    import numpy as np

    ids = ["a", "b"]
    _write(tmp_path, _random_vectors(2, seed=13), ids)
    original = np.load

    def refusing_load(*args: Any, **kwargs: Any):
        if "mmap_mode" in kwargs:
            raise TypeError("mmap не поддерживается")
        return original(*args, **kwargs)

    monkeypatch.setattr(np, "load", refusing_load)
    data = index_format.read_partition(tmp_path, PACKAGE, BUILD, PRODUCT)
    assert not isinstance(data.vectors, np.memmap)
    assert data.vectors.shape == (2, DIM)


# ---------------------------------------------------------------------------
# (г) батч = поштучно
# ---------------------------------------------------------------------------


def test_search_many_matches_single_search(tmp_path: Path) -> None:
    """``search_many`` возвращает то же, что поштучный ``search`` (порядок — побитово)."""
    ids = [f"c{i:02d}" for i in range(50)]
    _write(tmp_path, _random_vectors(len(ids), seed=14), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))

    import numpy as np

    rng = np.random.default_rng(15)
    queries = [
        VectorQuery(product_ids=[PRODUCT], top_k=4, vector=rng.standard_normal(DIM).tolist())
        for _ in range(6)
    ]
    batched = index.search_many(queries)
    assert len(batched) == len(queries)
    for query, hits in zip(queries, batched):
        single = index.search(query)
        assert [c.chunk_id for c in hits] == [c.chunk_id for c in single]
        for one, two in zip(single, hits):
            # Блочность BLAS зависит от размера батча: баллы сверяются с допуском,
            # порядок и tie-break — побитово.
            assert two.score == pytest.approx(one.score, abs=1e-6)


def test_search_many_reads_partition_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Батч читает партицию один раз и считает все запросы одним вызовом."""
    ids = [f"c{i:02d}" for i in range(40)]
    _write(tmp_path, _random_vectors(len(ids), seed=16), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))
    import numpy as np

    calls = {"n": 0}
    original = np.load

    def counting_load(*args: Any, **kwargs: Any):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(np, "load", counting_load)
    rng = np.random.default_rng(17)
    queries = [
        VectorQuery(product_ids=[PRODUCT], top_k=3, vector=rng.standard_normal(DIM).tolist())
        for _ in range(10)
    ]
    index.search_many(queries)
    assert calls["n"] == 1


def test_batched_search_helper_falls_back_to_single(tmp_path: Path) -> None:
    """Порт без ``search_many`` обслуживается циклом ``search`` (контракт не ломается)."""
    ids = ["a", "b", "c"]
    _write(tmp_path, _random_vectors(3, seed=18), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))

    class _SearchOnly:
        """Реализация порта без батча — как любой сторонний адаптер до правки."""

        build_id = BUILD
        dim = DIM

        def __init__(self, inner: NpyVectorIndex) -> None:
            self.inner = inner
            self.calls = 0

        def search(self, q):  # noqa: ANN001
            self.calls += 1
            return self.inner.search(q)

        def shard_ids(self, product_id: str) -> list[str]:
            return [product_id]

    port = _SearchOnly(index)
    vector = [1.0] + [0.0] * (DIM - 1)
    queries = [VectorQuery(product_ids=[PRODUCT], top_k=1, vector=vector) for _ in range(3)]
    results = batched_search(port, queries)  # type: ignore[arg-type]
    assert port.calls == 3
    assert [hits[0].chunk_id for hits in results] == [
        index.search(q)[0].chunk_id for q in queries
    ]


def test_search_many_empty_and_degenerate(tmp_path: Path) -> None:
    """Пустой батч, ``top_k=0`` и пустой ``product_ids`` — пустая выдача, без ошибок."""
    ids = ["a", "b"]
    _write(tmp_path, _random_vectors(2, seed=19), ids)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, DIM, _CountingLookup(ids))
    vector = [1.0] + [0.0] * (DIM - 1)
    assert index.search_many([]) == []
    assert index.search_many(
        [
            VectorQuery(product_ids=[PRODUCT], top_k=0, vector=vector),
            VectorQuery(product_ids=[], top_k=5, vector=vector),
        ]
    ) == [[], []]


# ---------------------------------------------------------------------------
# Бюджет кэша партиций
# ---------------------------------------------------------------------------


def test_partition_cache_budget_evicts_oldest() -> None:
    """Переполнение бюджета вытесняет старейшую запись, а не всё сразу."""
    import numpy as np

    cache = PartitionCache(budget_bytes=3 * 4 * 8)
    for i in range(4):
        cache.put(
            ("root", PACKAGE, BUILD, f"p{i}"),
            _partition_stub(np, bytes_=4 * 8),
        )
    assert len(cache) == 3
    assert cache.get(("root", PACKAGE, BUILD, "p0")) is None
    assert cache.get(("root", PACKAGE, BUILD, "p3")) is not None
    assert cache.evictions == 1


def test_partition_cache_skips_oversized_partition() -> None:
    """Запись больше бюджета не кэшируется: иначе она вытесняла бы всё (трешинг)."""
    import numpy as np

    cache = PartitionCache(budget_bytes=64)
    cache.put(("root", PACKAGE, BUILD, "big"), _partition_stub(np, bytes_=1024))
    assert len(cache) == 0
    cache.put(("root", PACKAGE, BUILD, "small"), _partition_stub(np, bytes_=32))
    assert len(cache) == 1


def test_partition_cache_clear_and_zero_budget() -> None:
    """``clear`` освобождает память; бюджет ``0`` выключает кэш."""
    import numpy as np

    cache = PartitionCache(budget_bytes=1024)
    cache.put(("root", PACKAGE, BUILD, "p"), _partition_stub(np, bytes_=32))
    assert cache.bytes_used == 32
    cache.clear()
    assert cache.bytes_used == 0 and len(cache) == 0

    off = PartitionCache(budget_bytes=0)
    off.put(("root", PACKAGE, BUILD, "p"), _partition_stub(np, bytes_=32))
    assert len(off) == 0


def _partition_stub(np: Any, *, bytes_: int):  # noqa: ANN202
    from core.adapters.vector_npy import _Partition

    return _Partition(
        matrix=np.zeros((4, 8), dtype=np.float32),
        norms=np.zeros(4, dtype=np.float32),
        chunk_ids=("a", "b", "c", "d"),
        nbytes=bytes_,
    )


def test_default_cache_budget_is_a_number_not_a_hope() -> None:
    """Бюджет по умолчанию — явное число байт и делается разумным из него."""
    assert DEFAULT_PARTITION_CACHE_BYTES == 256 * 1024 * 1024
    # 11 400 × 1024 × 4 = 46.7 MB — партиция наблюдаемого масштаба; в бюджет входит ≥ 5.
    partition_bytes = 11_400 * 1024 * 4
    assert DEFAULT_PARTITION_CACHE_BYTES // partition_bytes >= 5
