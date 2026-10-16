"""Тесты P4a: замкнутый dense/lexical read-loop через composition root (I17/I20/N3).

Проверяют, что ``build_adapters("local")``/``build_vector_index``/``seed_lexical_index``
собирают **реальные** порты ретривала из зафиксированной ``.npy``-сборки (product-scoped,
I17) и персистентных чанков store, а при отсутствии предпосылки честно оставляют ``None``
(явное отсутствие, N3) — не выдуманный индекс. Здесь же — доказательство, что с
инъецированными виртуальными портами ``run_retrieval_experiment`` исполняет dense-плечо
R1/R2, а без них — пропускает без падения. Синтетика нейтральна (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.adapters import index_format
from core.adapters.index_npy import numpy_available
from core.adapters.lexical_tantivy import tantivy_available
from core.adapters.vector_npy import NpyVectorIndex
from core.cli.wiring import build_adapters, build_vector_index, seed_lexical_index
from core.domain import CorpusChunk, CorpusDoc
from core.pipeline.experiments import run_retrieval_experiment
from core.pipeline.retrieve import RetrievalProfile
from core.ports.vector_index import VectorQuery
from core.store.sqlite_store import SqliteStore
from core.util.retrieval_eval import GoldenQuery

PKG = "pkg-x"
BUILD = "ib_synth"
PRODUCT_A = "p-alpha"
PRODUCT_B = "p-beta"

#: Синтетические чанки: текст содержит токены запросов (для лексического плеча).
TEXTS = {
    "a0": ("alpha document text", PRODUCT_A),
    "a1": ("beta-only document", PRODUCT_A),
    "b0": ("alpha beta cross", PRODUCT_B),
}


def _write_build(root: Path) -> None:
    """Пишет зафиксированную сборку (2 продукт-партиции, dim=2) через единый формат."""
    index_format.write_partition(
        root, PKG, BUILD, PRODUCT_A,
        [[1.0, 0.0], [0.0, 1.0]], ["a0", "a1"], {"dim": 2},
    )
    index_format.write_partition(
        root, PKG, BUILD, PRODUCT_B,
        [[1.0, 0.0]], ["b0"], {"dim": 2},
    )


def _store_with_chunks() -> SqliteStore:
    """Store с персистентными чанками сборки (``corpus_chunks`` + ``corpus_docs`` FK)."""
    store = SqliteStore(":memory:")
    for pid in dict.fromkeys(pid for _, pid in TEXTS.values()):
        store.upsert_corpus_doc(
            CorpusDoc(
                doc_id=f"{pid}:doc",
                product_id=pid,
                url=f"https://example.test/{pid}",
                content_hash=f"doc-{pid}",
            )
        )
    store.add_chunks_batch(
        [
            CorpusChunk(
                chunk_id=cid,
                doc_id=f"{pid}:doc",
                product_id=pid,
                text=text,
                heading_path="H",
                content_hash=f"hash-{cid}",
                build_id=BUILD,
            )
            for cid, (text, pid) in TEXTS.items()
        ]
    )
    return store


requires_numpy = pytest.mark.skipif(not numpy_available(), reason="numpy не установлен")
requires_tantivy = pytest.mark.skipif(not tantivy_available(), reason="tantivy не установлен")


# ---------------------------------------------------------------------------
# build_vector_index / NpyVectorIndex через wired path
# ---------------------------------------------------------------------------


@requires_numpy
def test_build_vector_index_reads_persisted_build(tmp_path: Path) -> None:
    _write_build(tmp_path)
    with _store_with_chunks() as store:
        index = build_vector_index(
            index_root=tmp_path, package=PKG, chunk_lookup=store.get_chunk
        )
    assert isinstance(index, NpyVectorIndex)
    assert index.build_id == BUILD
    assert index.dim == 2


@requires_numpy
def test_vector_index_is_product_scoped(tmp_path: Path) -> None:
    """I17: запрос продукта A не читает партицию продукта B."""
    _write_build(tmp_path)
    with _store_with_chunks() as store:
        index = build_vector_index(
            index_root=tmp_path, package=PKG, chunk_lookup=store.get_chunk
        )
        assert index is not None
        hits_a = index.search(VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0]))
        hits_b = index.search(VectorQuery(product_ids=[PRODUCT_B], top_k=5, vector=[1.0, 0.0]))
    assert [h.chunk_id for h in hits_a] == ["a0", "a1"]
    assert [h.chunk_id for h in hits_b] == ["b0"]
    assert all(h.product_id == PRODUCT_A for h in hits_a)
    assert all(h.product_id == PRODUCT_B for h in hits_b)


@requires_numpy
def test_build_vector_index_wired_through_build_adapters(tmp_path: Path) -> None:
    _write_build(tmp_path)
    with _store_with_chunks() as store:
        ports = build_adapters(
            profile="local",
            package=PKG,
            index_root=tmp_path,
            chunk_lookup=store.get_chunk,
            store=store,
        )
        assert isinstance(ports.vector_index, NpyVectorIndex)
        # Ретривал через поднятый порт отдаёт провенанс продукта (I17).
        hits = ports.vector_index.search(
            VectorQuery(product_ids=[PRODUCT_A], top_k=2, vector=[1.0, 0.0])
        )
    assert hits[0].chunk_id == "a0"
    assert hits[0].content_hash == "hash-a0"


@requires_numpy
def test_vector_index_absent_without_chunk_lookup(tmp_path: Path) -> None:
    _write_build(tmp_path)
    # Без резолвера метаданных чанка ``RetrievedChunk`` не собрать → явный ``None``.
    assert build_vector_index(index_root=tmp_path, package=PKG, chunk_lookup=None) is None


@requires_numpy
def test_vector_index_absent_without_persisted_build(tmp_path: Path) -> None:
    assert (
        build_vector_index(index_root=tmp_path, package=PKG, chunk_lookup=lambda _cid: None)
        is None
    )


@requires_numpy
def test_vector_index_absent_when_build_ambiguous(tmp_path: Path) -> None:
    _write_build(tmp_path)
    # Вторая сборка пакета → выбор неоднозначен → явный ``None`` (не гадаем, A3).
    index_format.write_partition(
        tmp_path, PKG, "ib_second", PRODUCT_A, [[1.0, 0.0]], ["a0"], {"dim": 2}
    )
    assert (
        build_vector_index(index_root=tmp_path, package=PKG, chunk_lookup=lambda _cid: None)
        is None
    )
    # Явный build_id снимает неоднозначность.
    explicit = build_vector_index(
        index_root=tmp_path, package=PKG, chunk_lookup=lambda _cid: None, build_id=BUILD
    )
    assert isinstance(explicit, NpyVectorIndex)


# ---------------------------------------------------------------------------
# seed_lexical_index (R0/R2) — симметричное закрытие лексического плеча
# ---------------------------------------------------------------------------


@requires_tantivy
def test_seed_lexical_index_from_store(tmp_path: Path) -> None:
    _write_build(tmp_path)
    with _store_with_chunks() as store:
        index = seed_lexical_index(index_root=tmp_path, package=PKG, store=store)
        assert index is not None
        assert index.build_id == BUILD
        hits = index.search("alpha", [PRODUCT_A], top_k=5)
    assert [h.chunk_id for h in hits] == ["a0"]
    assert all(h.product_id == PRODUCT_A for h in hits)


def test_seed_lexical_index_absent_without_store(tmp_path: Path) -> None:
    _write_build(tmp_path)
    assert seed_lexical_index(index_root=tmp_path, package=PKG, store=None) is None


# ---------------------------------------------------------------------------
# Отсутствие порта — явное (N3): эксперимент пропускает, а не падает
# ---------------------------------------------------------------------------


def test_run_experiment_all_ports_absent_raises_cleanly() -> None:
    """A3-регрессия: без портов эксперимент не падает «сырой» ошибкой и не выдумывает вектор."""
    golden = [GoldenQuery(query="alpha", expected_ids=frozenset({"a0"}))]
    with pytest.raises(ValueError, match="ни один профиль"):
        run_retrieval_experiment(
            golden,
            product_ids=[PRODUCT_A],
            cutoff_k=3,
            lexical_index=None,
            vector_index=None,
            embed=None,
        )


@requires_numpy
@requires_tantivy
def test_run_experiment_skips_dense_but_runs_lexical(tmp_path: Path) -> None:
    """Dense отсутствует → R1/R2 помечены ``skipped`` (не падение, не подделка), R0 исполняется."""
    _write_build(tmp_path)
    with _store_with_chunks() as store:
        ports = build_adapters(
            profile="local",
            package=PKG,
            index_root=tmp_path,
            chunk_lookup=store.get_chunk,
            store=store,
        )
        assert ports.lexical_index is not None
        golden = [GoldenQuery(query="alpha", expected_ids=frozenset({"a0"}))]
        report = run_retrieval_experiment(
            golden,
            product_ids=[PRODUCT_A],
            cutoff_k=3,
            lexical_index=ports.lexical_index,
            vector_index=None,  # dense недоступен
            embed=None,
        )
    assert set(report.by_profile) == {"R0"}
    assert set(report.skipped) == {"R1", "R2"}


@requires_numpy
def test_run_experiment_executes_dense_arm_with_wired_ports(tmp_path: Path) -> None:
    """P4a: с wired-портами dense-плечо R1/R2 реально исполняется (не ``skipped``)."""
    _write_build(tmp_path)
    with _store_with_chunks() as store:
        ports = build_adapters(
            profile="local",
            package=PKG,
            index_root=tmp_path,
            chunk_lookup=store.get_chunk,
            store=store,
        )
        assert ports.vector_index is not None

        golden = [
            GoldenQuery(query="alpha", expected_ids=frozenset({"a0"})),
        ]

        def embed(query: str) -> list[float]:
            return [1.0, 0.0]

        report = run_retrieval_experiment(
            golden,
            product_ids=[PRODUCT_A],
            cutoff_k=3,
            profiles=(RetrievalProfile.R1,),
            vector_index=ports.vector_index,
            embed=embed,
        )
    assert set(report.by_profile) == {"R1"}
    assert "R1" not in report.skipped
    assert report.by_profile["R1"].recall_at_k == pytest.approx(1.0)


@requires_numpy
@requires_tantivy
def test_run_experiment_executes_hybrid_arm_with_wired_ports(tmp_path: Path) -> None:
    """R2 (RRF-гибрид) исполняется, когда wired и lexical, и vector."""
    _write_build(tmp_path)
    with _store_with_chunks() as store:
        ports = build_adapters(
            profile="local",
            package=PKG,
            index_root=tmp_path,
            chunk_lookup=store.get_chunk,
            store=store,
        )
        assert ports.lexical_index is not None and ports.vector_index is not None
        golden = [GoldenQuery(query="alpha", expected_ids=frozenset({"a0"}))]
        report = run_retrieval_experiment(
            golden,
            product_ids=[PRODUCT_A],
            cutoff_k=3,
            profiles=(RetrievalProfile.R2,),
            lexical_index=ports.lexical_index,
            vector_index=ports.vector_index,
            embed=lambda _q: [1.0, 0.0],
        )
    assert set(report.by_profile) == {"R2"}
    assert report.by_profile["R2"].recall_at_k == pytest.approx(1.0)
