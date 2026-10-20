"""Тесты ``NpyVectorIndex`` (W8/T8.4) — реализация ``VectorIndexPort``.

Проверяются: brute-force cosine-ранжирование по ``float16``-партициям, продукт-скоупед
(I17), ``top_k``, детерминированный tie-break (R7), провенанс ``RetrievedChunk`` через
инъекцию резолвера метаданных, атрибуты/контракт порта, вход через ``Mapping`` и callable,
а также ясные ошибки (нет вектора, несовпадение ``dim``). Раскладка пишется через
``index_format.write_partition`` (единый формат) в ``tmp_path``. Фикстуры категорийно
нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.adapters import index_format, vector_npy
from core.adapters.vector_npy import NpyVectorIndex, VectorIndexUnavailable, numpy_available
from core.pipeline.composition import CompositionView
from core.ports.types import Chunk
from core.ports.vector_index import VectorIndexPort, VectorQuery

pytestmark = pytest.mark.skipif(not numpy_available(), reason="numpy не установлен")

PACKAGE = "pkg"
BUILD = "build-1"
PRODUCT_A = "p-alpha"
PRODUCT_B = "p-beta"


def _chunk(chunk_id: str, text: str = "", heading_path: str = "") -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        chunk_no=0,
        text=text or f"text-{chunk_id}",
        heading_path=heading_path,
        context_prefix="",
        content_hash=f"hash-{chunk_id}",
    )


def _write(root: Path, product_id: str, vectors, chunk_ids) -> None:
    index_format.write_partition(
        root,
        PACKAGE,
        BUILD,
        product_id,
        vectors,
        chunk_ids,
        {"dim": len(vectors[0]), "embed_model_id": "test-model"},
    )


def _lookup(chunk_ids) -> dict[str, Chunk]:
    return {chunk_id: _chunk(chunk_id, heading_path=f"H-{chunk_id}") for chunk_id in chunk_ids}


# ---------------------------------------------------------------------------
# Атрибуты и контракт порта
# ---------------------------------------------------------------------------


def test_constructor_rejects_invalid_dim_and_build(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        NpyVectorIndex(tmp_path, PACKAGE, BUILD, 0, _lookup(["a"]))
    with pytest.raises(ValueError):
        NpyVectorIndex(tmp_path, PACKAGE, "", 3, _lookup(["a"]))
    with pytest.raises(ValueError):
        NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, object())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Cosine-ранжирование
# ---------------------------------------------------------------------------


def test_cosine_ranking_order(tmp_path: Path) -> None:
    _write(
        tmp_path,
        PRODUCT_A,
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        ["a", "b", "c"],
    )
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a", "b", "c"]))
    hits = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=3, vector=[1.0, 0.0, 0.0]))
    assert [c.chunk_id for c in hits] == ["a", "b", "c"]
    assert hits[0].score == pytest.approx(1.0, abs=1e-4)
    assert hits[1].score == pytest.approx(0.0, abs=1e-4)


def test_scaled_vectors_give_cosine_not_dot(tmp_path: Path) -> None:
    # Направление совпадает с запросом, но норма иная: косинус = 1, не 5.
    _write(tmp_path, PRODUCT_A, [[5.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    hits = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=1, vector=[1.0, 0.0, 0.0]))
    assert hits[0].score == pytest.approx(1.0, abs=1e-4)


def test_provenance_fields(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    top = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=1, vector=[1.0, 0.0, 0.0]))[0]
    assert top.product_id == PRODUCT_A
    assert top.text == "text-a"
    assert top.heading_path == "H-a"
    assert top.content_hash == "hash-a"
    assert top.build_id == BUILD


# ---------------------------------------------------------------------------
# Продукт-скоуп (I17)
# ---------------------------------------------------------------------------


def test_product_scope_only_requested_partitions(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0]], ["a"])
    _write(tmp_path, PRODUCT_B, [[1.0, 0.0, 0.0]], ["z"])
    lookup = _lookup(["a", "z"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, lookup)
    only_a = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=10, vector=[1.0, 0.0, 0.0]))
    only_b = index.search(VectorQuery(product_ids=[PRODUCT_B], top_k=10, vector=[1.0, 0.0, 0.0]))
    assert [c.chunk_id for c in only_a] == ["a"]
    assert [c.chunk_id for c in only_b] == ["z"]


def test_empty_product_ids_return_nothing(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    assert index.search(VectorQuery(product_ids=[], top_k=5, vector=[1.0, 0.0, 0.0])) == []


# ---------------------------------------------------------------------------
# top_k и tie-break (R7)
# ---------------------------------------------------------------------------


def test_top_k_limits_results(tmp_path: Path) -> None:
    _write(
        tmp_path,
        PRODUCT_A,
        [[1.0, 0.0, 0.0], [0.9, 0.1, 0.0], [0.8, 0.2, 0.0]],
        ["a", "b", "c"],
    )
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a", "b", "c"]))
    hits = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=2, vector=[1.0, 0.0, 0.0]))
    assert [c.chunk_id for c in hits] == ["a", "b"]


def test_equal_scores_tie_break_by_chunk_id(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0], [1.0, 0.0, 0.0]], ["t2", "t1"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["t1", "t2"]))
    hits = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0, 0.0]))
    assert [c.chunk_id for c in hits] == ["t1", "t2"]


def test_zero_query_vector_yields_zero_scores_deterministically(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]], ["b", "a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a", "b"]))
    hits = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[0.0, 0.0, 0.0]))
    assert [c.chunk_id for c in hits] == ["a", "b"]
    assert all(c.score == 0.0 for c in hits)


# ---------------------------------------------------------------------------
# Резолвер метаданных
# ---------------------------------------------------------------------------


def test_callable_lookup_is_supported(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(
        tmp_path, PACKAGE, BUILD, 3, lambda cid: _chunk(cid) if cid == "a" else None
    )
    hits = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0, 0.0]))
    assert [c.chunk_id for c in hits] == ["a"]


def test_unresolved_chunk_is_skipped(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0], [1.0, 0.0, 0.0]], ["a", "ghost"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, {"a": _chunk("a")})
    hits = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0, 0.0]))
    assert [c.chunk_id for c in hits] == ["a"]


# ---------------------------------------------------------------------------
# Ошибочные входы
# ---------------------------------------------------------------------------


def test_missing_vector_raises(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    with pytest.raises(ValueError):
        index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, text="only text"))


def test_dimension_mismatch_raises(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    with pytest.raises(ValueError):
        index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0, 0.0]))


def test_query_vector_dimension_mismatch_raises(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    with pytest.raises(ValueError):
        index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0]))


def test_missing_partition_raises(tmp_path: Path) -> None:
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    with pytest.raises(FileNotFoundError):
        index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0, 0.0]))


def test_missing_numpy_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vector_npy, "numpy_available", lambda: False)
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    assert index.available is False
    with pytest.raises(VectorIndexUnavailable):
        index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0, 0.0]))


# ---------------------------------------------------------------------------
# Композиция партиций (09 §8.4): view=
# ---------------------------------------------------------------------------


def _write_partition(root: Path, partition_id: str, product_id: str, chunk_ids) -> None:
    index_format.write_partition(
        root,
        PACKAGE,
        partition_id,
        product_id,
        [[1.0, 0.0, 0.0] for _ in chunk_ids],
        list(chunk_ids),
        {"dim": 3, "embed_model_id": "test-model"},
    )


def _two_partition_view() -> CompositionView:
    return CompositionView(
        composition_id="cb_composite",
        members={PRODUCT_A: "pb_alpha", PRODUCT_B: "pb_beta"},
    )


def test_view_reads_per_product_partitions_and_carries_partition_build(tmp_path: Path) -> None:
    _write_partition(tmp_path, "pb_alpha", PRODUCT_A, ["a"])
    _write_partition(tmp_path, "pb_beta", PRODUCT_B, ["b"])
    view = _two_partition_view()
    index = NpyVectorIndex(
        tmp_path, PACKAGE, None, 3, _lookup(["a", "b"]), view=view
    )

    assert index.build_id == "cb_composite"  # контракт порта: id композиции
    assert index.composition_id == "cb_composite"

    hits = index.search(
        VectorQuery(product_ids=[PRODUCT_A, PRODUCT_B], top_k=5, vector=[1.0, 0.0, 0.0])
    )
    by_product = {hit.product_id: hit for hit in hits}
    assert by_product[PRODUCT_A].build_id == "pb_alpha"
    assert by_product[PRODUCT_B].build_id == "pb_beta"


def test_view_shard_ids_only_for_members(tmp_path: Path) -> None:
    view = _two_partition_view()
    index = NpyVectorIndex(tmp_path, PACKAGE, None, 3, _lookup([]), view=view)
    assert index.shard_ids(PRODUCT_A) == [PRODUCT_A]
    assert index.shard_ids("stranger") == []


def test_view_search_of_non_member_raises(tmp_path: Path) -> None:
    _write_partition(tmp_path, "pb_alpha", PRODUCT_A, ["a"])
    index = NpyVectorIndex(
        tmp_path, PACKAGE, None, 3, _lookup(["a"]), view=_two_partition_view()
    )
    with pytest.raises(KeyError, match="stranger"):
        index.search(VectorQuery(product_ids=["stranger"], top_k=5, vector=[1.0, 0.0, 0.0]))


def test_view_build_id_must_match_composition(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="composition_id"):
        NpyVectorIndex(
            tmp_path,
            PACKAGE,
            "other-build",
            3,
            _lookup(["a"]),
            view=_two_partition_view(),
        )


def test_without_view_behaviour_is_unchanged(tmp_path: Path) -> None:
    _write(tmp_path, PRODUCT_A, [[1.0, 0.0, 0.0]], ["a"])
    index = NpyVectorIndex(tmp_path, PACKAGE, BUILD, 3, _lookup(["a"]))
    assert index.build_id == BUILD
    assert index.composition_id is None
    hit = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=1, vector=[1.0, 0.0, 0.0]))[0]
    assert hit.build_id == BUILD  # прежний единый build_id, не partition id
