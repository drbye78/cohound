"""Тесты ``NpyIndexBuilder`` (W8/T8.2) — реализация ``IndexBuilderPort``.

Проверяются: запись трёх артефактов партиции (``vectors.fp16.npy``, ``chunk_ids.jsonl``,
``manifest.json``) через ``index_format``; round-trip float16 и порядок ``chunk_ids``;
детерминированный ``build_id`` (одинаковые входы → тот же id; смена версий/снимка → другой);
приоритет явного ``meta.extra["partition_build_id"]``; продукт-партиции (I17); иммутабельность
(повторная сборка того же ключа с другим содержимым → ``ImmutableBuildError``); провенанс
манифеста; структурное соответствие порту; путь без ``numpy`` (guard). Нейтральность (§1.3).
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from core.adapters import index_format
from core.adapters.index_npy import (
    ImmutableBuildError,
    NpyIndexBuilder,
    derive_build_id,
    numpy_available,
)
from core.ports.index_builder import IndexBuilderPort, IndexMeta
from core.ports.types import Chunk
from core.util.hashing import content_hash as chash

try:  # необязательная зависимость (extra ``retrieval``)
    import numpy as np  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - зависит от окружения
    np = None  # type: ignore[assignment]

requires_numpy = pytest.mark.skipif(np is None, reason="numpy не установлен (extra retrieval)")

PKG = "pkg-1"
SNAP = "2026-10-08"


# --------------------------------------------------------------------------- #
# Фикстуры-хелперы
# --------------------------------------------------------------------------- #


def _chunk(i: int, product: str = "p1") -> Chunk:
    text = f"text-{product}-{i}"
    return Chunk(
        chunk_id=f"{product}:doc-{i}:0",
        chunk_no=i,
        text=text,
        heading_path="h",
        context_prefix="",
        content_hash=chash(text),
    )


def _chunks(product: str = "p1", n: int = 3) -> list[Chunk]:
    return [_chunk(i, product) for i in range(n)]


def _vectors(n: int = 3, dim: int = 3) -> list[list[float]]:
    return [[0.1 * (i + 1) + 0.01 * j for j in range(dim)] for i in range(n)]


def _meta(product_id: str = "p1", **kw) -> IndexMeta:  # noqa: ANN003
    base: dict = {
        "package": PKG,
        "chunker_version": "ck-1",
        "embed_model_id": "emb-1",
        "snapshot_date": SNAP,
        "product_id": product_id,
        "extra": {},
    }
    base.update(kw)
    return IndexMeta(**base)


def _derived(chunks: list[Chunk]) -> str:
    return derive_build_id(
        PKG,
        "ck-1",
        "emb-1",
        [c.chunk_id for c in chunks],
        [c.content_hash for c in chunks],
    )


# --------------------------------------------------------------------------- #
# Артефакты и round-trip
# --------------------------------------------------------------------------- #


@requires_numpy
def test_build_writes_three_artifacts(tmp_path: Path) -> None:
    builder = NpyIndexBuilder(tmp_path)
    bid = builder.build(_chunks(), _vectors(), _meta())
    assert bid.startswith("pb_") and len(bid) == len("pb_") + 24
    assert index_format.is_committed(tmp_path, PKG, bid, "p1")
    root = index_format.build_root(tmp_path, PKG, bid, "p1")
    assert (root / "vectors.fp16.npy").is_file()
    assert (root / "chunk_ids.jsonl").is_file()
    assert (root / "manifest.json").is_file()


@requires_numpy
def test_read_partition_roundtrips_float16_and_order(tmp_path: Path) -> None:
    chunks = _chunks()
    vectors = [[1.0, 0.0, 0.0], [0.0, 2.0, 0.0], [0.0, 0.0, 3.0]]
    bid = NpyIndexBuilder(tmp_path).build(chunks, vectors, _meta())
    data = index_format.read_partition(tmp_path, PKG, bid, "p1")
    assert data.product_id == "p1"
    assert data.chunk_ids == [c.chunk_id for c in chunks]
    assert data.vectors.dtype.name == "float16"
    assert data.vectors.shape == (3, 3)
    # Порядок строк матрицы = порядок chunk_ids.
    assert float(data.vectors[0][0]) == 1.0
    assert float(data.vectors[2][2]) == 3.0


@requires_numpy
def test_chunk_ids_jsonl_format(tmp_path: Path) -> None:
    chunks = _chunks("p1", 2)
    bid = NpyIndexBuilder(tmp_path).build(chunks, _vectors(2), _meta())
    path = index_format.build_root(tmp_path, PKG, bid, "p1") / "chunk_ids.jsonl"
    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 2
    assert [json.loads(ln)["chunk_id"] for ln in lines] == [c.chunk_id for c in chunks]


def test_list_partitions_deterministic_and_absent(tmp_path: Path) -> None:
    """``list_partitions`` — единый носитель раскладки: отсортировано, пусто без сборки."""
    assert index_format.list_partitions(tmp_path, PKG, "ib_missing") == []
    if np is None:
        return
    for product in ("pB", "pA"):
        index_format.write_partition(
            tmp_path, PKG, "ib_multi", product, [[1.0, 0.0]], [f"c-{product}"], {"dim": 1}
        )
    assert index_format.list_partitions(tmp_path, PKG, "ib_multi") == ["pA", "pB"]


# --------------------------------------------------------------------------- #
# build_id: детерминизм, чувствительность, приоритет
# --------------------------------------------------------------------------- #


@requires_numpy
def test_build_id_deterministic_for_same_inputs(tmp_path: Path) -> None:
    chunks, vectors, meta = _chunks(), _vectors(), _meta()
    expected = _derived(chunks)

    b1 = NpyIndexBuilder(tmp_path)
    id1 = b1.build(chunks, vectors, meta)
    assert id1 == expected
    # Идемпотентный повтор того же содержимого — тот же id.
    assert b1.build(chunks, vectors, meta) == id1
    # Новый инстанс (resume) — тоже тот же id.
    assert NpyIndexBuilder(tmp_path).build(chunks, vectors, meta) == id1


@requires_numpy
def test_build_id_sensitive_to_versions_and_snapshot_is_provenance(tmp_path: Path) -> None:
    """09 §8.1: версии входят в fallback-id, ``snapshot_date`` — нет (провенанс)."""
    chunks, vectors = _chunks(), _vectors()
    builder = NpyIndexBuilder(tmp_path)
    base = builder.build(chunks, vectors, _meta())
    assert builder.build(chunks, vectors, _meta(chunker_version="ck-2")) != base
    assert builder.build(chunks, vectors, _meta(embed_model_id="emb-2")) != base
    # Снимок не меняет физический id: тот же контент/версии → тот же pb.
    assert builder.build(chunks, vectors, _meta(snapshot_date="2027-01-01")) == base


@requires_numpy
def test_build_id_is_order_sensitive() -> None:
    fwd = _chunks("p1", 3)
    rev = list(reversed(fwd))
    assert _derived(fwd) != _derived(rev)


@requires_numpy
def test_explicit_build_id_takes_precedence(tmp_path: Path) -> None:
    chunks, vectors = _chunks(), _vectors()
    derived = _derived(chunks)
    builder = NpyIndexBuilder(tmp_path)
    bid = builder.build(
        chunks, vectors, _meta(extra={"partition_build_id": "pb_explicit"})
    )
    assert bid == "pb_explicit"
    assert bid != derived
    assert index_format.is_committed(tmp_path, PKG, "pb_explicit", "p1")
    # Пустой явный id не считается заданным — берётся производный.
    assert builder.build(
        chunks, vectors, _meta(extra={"partition_build_id": ""})
    ) == derived


# --------------------------------------------------------------------------- #
# Продукт-партиции (I17)
# --------------------------------------------------------------------------- #


@requires_numpy
def test_explicit_partition_ids_give_separate_product_partitions(tmp_path: Path) -> None:
    """Явные per-product ``partition_build_id`` (как у Ш5) → N партиций (I17)."""
    builder = NpyIndexBuilder(tmp_path)
    shared = "pb_round-42"
    ida = builder.build(
        _chunks("p1"), _vectors(), _meta("p1", extra={"partition_build_id": shared})
    )
    idb = builder.build(
        _chunks("p2"), _vectors(), _meta("p2", extra={"partition_build_id": shared})
    )
    assert ida == idb == shared
    assert index_format.is_committed(tmp_path, PKG, shared, "p1")
    assert index_format.is_committed(tmp_path, PKG, shared, "p2")
    build_dir = index_format.build_root(tmp_path, PKG, shared, "p1").parent
    partitions = {p.name for p in build_dir.iterdir() if p.is_dir()}
    assert {"p1", "p2"} <= partitions
    # Партиции изолированы: чужие chunk_id не видны (I17).
    a_ids = set(index_format.read_partition(tmp_path, PKG, shared, "p1").chunk_ids)
    b_ids = set(index_format.read_partition(tmp_path, PKG, shared, "p2").chunk_ids)
    assert a_ids and b_ids and a_ids.isdisjoint(b_ids)


@requires_numpy
def test_each_product_gets_own_partition(tmp_path: Path) -> None:
    builder = NpyIndexBuilder(tmp_path)
    ida = builder.build(_chunks("p1"), _vectors(), _meta("p1"))
    idb = builder.build(_chunks("p2"), _vectors(), _meta("p2"))
    assert index_format.is_committed(tmp_path, PKG, ida, "p1")
    assert index_format.is_committed(tmp_path, PKG, idb, "p2")


# --------------------------------------------------------------------------- #
# Иммутабельность (§13/K2)
# --------------------------------------------------------------------------- #


@requires_numpy
def test_second_build_same_key_different_content_raises(tmp_path: Path) -> None:
    builder = NpyIndexBuilder(tmp_path)
    meta = _meta(extra={"partition_build_id": "pb_fixed"})
    builder.build(_chunks("p1", 3), _vectors(3), meta)
    with pytest.raises(ImmutableBuildError):
        builder.build(_chunks("p1", 2), _vectors(2), meta)


@requires_numpy
def test_build_after_commit_with_different_vectors_raises(tmp_path: Path) -> None:
    builder = NpyIndexBuilder(tmp_path)
    meta = _meta(extra={"partition_build_id": "pb_fixed"})
    builder.build(_chunks("p1", 3), _vectors(3), meta)
    builder.commit()
    other_vectors = [[9.0, 9.0, 9.0], [8.0, 8.0, 8.0], [7.0, 7.0, 7.0]]
    with pytest.raises(ImmutableBuildError):
        builder.build(_chunks("p1", 3), other_vectors, meta)


@requires_numpy
def test_rebuild_same_content_after_commit_is_allowed(tmp_path: Path) -> None:
    builder = NpyIndexBuilder(tmp_path)
    meta = _meta(extra={"partition_build_id": "pb_fixed"})
    chunks, vectors = _chunks("p1"), _vectors()
    assert builder.build(chunks, vectors, meta) == "pb_fixed"
    builder.commit()
    assert builder.build(chunks, vectors, meta) == "pb_fixed"


@requires_numpy
def test_new_builder_refuses_to_mutate_committed_partition(tmp_path: Path) -> None:
    meta = _meta(extra={"partition_build_id": "pb_fixed"})
    NpyIndexBuilder(tmp_path).build(_chunks("p1", 3), _vectors(3), meta)
    with pytest.raises(ImmutableBuildError):
        NpyIndexBuilder(tmp_path).build(_chunks("p1", 2), _vectors(2), meta)


# --------------------------------------------------------------------------- #
# Манифест и провенанс
# --------------------------------------------------------------------------- #


@requires_numpy
def test_manifest_carries_provenance(tmp_path: Path) -> None:
    chunks = _chunks("p1", 4)
    bid = NpyIndexBuilder(tmp_path).build(chunks, _vectors(4, 5), _meta())
    manifest = index_format.read_manifest(tmp_path, PKG, bid, "p1")
    assert manifest["build_id"] == bid
    assert manifest["package"] == PKG
    assert manifest["product_id"] == "p1"
    assert manifest["embed_model_id"] == "emb-1"
    assert manifest["chunker_version"] == "ck-1"
    assert manifest["snapshot_date"] == SNAP
    assert manifest["dim"] == 5
    assert manifest["stats"]["chunks"] == 4
    assert manifest["extra"] == {}
    assert manifest["content_digest"]


@requires_numpy
def test_manifest_extra_is_persisted(tmp_path: Path) -> None:
    meta = _meta(extra={"partition_build_id": "pb_x", "round": "R1-serp"})
    bid = NpyIndexBuilder(tmp_path).build(_chunks(), _vectors(), meta)
    manifest = index_format.read_manifest(tmp_path, PKG, bid, "p1")
    assert manifest["extra"]["round"] == "R1-serp"


@requires_numpy
def test_lexical_engine_version_published_and_in_manifest(tmp_path: Path) -> None:
    """``lexical_engine_version`` — часть контракта сборки: атрибут + манифест (§13:676)."""
    builder = NpyIndexBuilder(tmp_path, lexical_engine_version="tantivy-0.26.2")
    assert builder.lexical_engine_version == "tantivy-0.26.2"
    bid = builder.build(_chunks(), _vectors(), _meta())
    manifest = index_format.read_manifest(tmp_path, PKG, bid, "p1")
    assert manifest["lexical_engine_version"] == "tantivy-0.26.2"


@requires_numpy
def test_explicit_provenance_wins_over_builder_engine(tmp_path: Path) -> None:
    """Явный ``meta.extra`` (входы ``pb`` от Ш5) приоритетнее метки сборщика."""
    builder = NpyIndexBuilder(tmp_path, lexical_engine_version="bm25-py-1")
    bid = builder.build(
        _chunks(),
        _vectors(),
        _meta(extra={"lexical_engine_version": "tantivy-0.26.2"}),
    )
    manifest = index_format.read_manifest(tmp_path, PKG, bid, "p1")
    assert manifest["lexical_engine_version"] == "tantivy-0.26.2"


def test_builder_satisfies_port_and_vector_store_unchanged(tmp_path: Path) -> None:
    """Регресс: ``vector_store`` — прежний ``npy-fp16``; порт реализован структурно."""
    builder = NpyIndexBuilder(tmp_path, lexical_engine_version="tantivy-0.26.2")
    assert builder.vector_store == "npy-fp16"
    assert builder.lexical_engine_version == "tantivy-0.26.2"
    assert isinstance(builder, IndexBuilderPort)


# --------------------------------------------------------------------------- #
# Пакет: приоритет meta над конструктором
# --------------------------------------------------------------------------- #


@requires_numpy
def test_constructor_package_used_when_meta_package_empty(tmp_path: Path) -> None:
    builder = NpyIndexBuilder(tmp_path, package="pkg-ctor")
    bid = builder.build(_chunks(), _vectors(), _meta(package=""))
    assert index_format.is_committed(tmp_path, "pkg-ctor", bid, "p1")


@requires_numpy
def test_meta_package_wins_over_constructor(tmp_path: Path) -> None:
    builder = NpyIndexBuilder(tmp_path, package="pkg-ctor")
    bid = builder.build(_chunks(), _vectors(), _meta())
    assert index_format.is_committed(tmp_path, PKG, bid, "p1")
    assert not index_format.is_committed(tmp_path, "pkg-ctor", bid, "p1")


# --------------------------------------------------------------------------- #
# Ошибки валидации
# --------------------------------------------------------------------------- #


@requires_numpy
def test_length_mismatch_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        NpyIndexBuilder(tmp_path).build(_chunks("p1", 3), _vectors(2), _meta())


@requires_numpy
def test_empty_partition_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        NpyIndexBuilder(tmp_path).build([], [], _meta())


@requires_numpy
def test_missing_product_id_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        NpyIndexBuilder(tmp_path).build(_chunks(), _vectors(), _meta(product_id=""))


# --------------------------------------------------------------------------- #
# commit / порт
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# numpy — необязательная зависимость
# --------------------------------------------------------------------------- #


def test_build_without_numpy_raises_clear_error(tmp_path: Path, monkeypatch) -> None:
    """Guard: при отсутствии numpy ``build`` поднимает понятную ``RuntimeError``."""
    import core.adapters.index_npy as module

    def _boom():  # noqa: ANN202
        raise RuntimeError("NpyIndexBuilder: требуется numpy")

    monkeypatch.setattr(module, "_import_numpy", _boom)
    with pytest.raises(RuntimeError, match="numpy"):
        module.NpyIndexBuilder(tmp_path).build(_chunks(), _vectors(), _meta())


# --------------------------------------------------------------------------- #
# Нейтральность (§1.3)
# --------------------------------------------------------------------------- #


