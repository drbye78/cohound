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
from core.pipeline.composition import CompositionView, write_composition
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


def _store_with_chunks(build_by_product: dict[str, str] | None = None) -> SqliteStore:
    """Store с персистентными чанками сборки (``corpus_chunks`` + ``corpus_docs`` FK).

    ``build_by_product`` — id сборки/партиции на продукт (по умолчанию единый ``BUILD``).
    """
    builds = build_by_product or {}
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
                build_id=builds.get(pid, BUILD),
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
    assert index.composition_id == BUILD  # legacy-view: одна сборка = одна композиция
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
# Композиция партиций (09 §8.2/§8.4)
# ---------------------------------------------------------------------------

COMPOSITION_ID = "cb_composed"
PARTITION_A = "pb_alpha"
PARTITION_B = "pb_beta"


def _write_composed_layout(root: Path) -> CompositionView:
    """Партиции с per-product id + манифест композиции (shim: id композиции ≠ партиции)."""
    index_format.write_partition(
        root, PKG, PARTITION_A, PRODUCT_A,
        [[1.0, 0.0], [0.0, 1.0]], ["a0", "a1"], {"dim": 2},
    )
    index_format.write_partition(
        root, PKG, PARTITION_B, PRODUCT_B,
        [[1.0, 0.0]], ["b0"], {"dim": 2},
    )
    view = CompositionView(
        composition_id=COMPOSITION_ID,
        members={PRODUCT_A: PARTITION_A, PRODUCT_B: PARTITION_B},
    )
    write_composition(root, PKG, view, created_at="2026-10-20T00:00:00Z")
    return view


@requires_numpy
def test_vector_index_uses_composition_file_when_present(tmp_path: Path) -> None:
    _write_composed_layout(tmp_path)
    with _store_with_chunks({PRODUCT_A: PARTITION_A, PRODUCT_B: PARTITION_B}) as store:
        index = build_vector_index(
            index_root=tmp_path,
            package=PKG,
            chunk_lookup=store.get_chunk,
            build_id=COMPOSITION_ID,
        )
        assert isinstance(index, NpyVectorIndex)
        assert index.build_id == COMPOSITION_ID
        assert index.composition_id == COMPOSITION_ID
        hits_a = index.search(
            VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0])
        )
        hits_b = index.search(
            VectorQuery(product_ids=[PRODUCT_B], top_k=5, vector=[1.0, 0.0])
        )
    assert [h.chunk_id for h in hits_a] == ["a0", "a1"]
    assert [h.chunk_id for h in hits_b] == ["b0"]
    # Провенанс — партиция продукта, не id композиции (09 §8.4).
    assert {h.build_id for h in hits_a} == {PARTITION_A}
    assert {h.build_id for h in hits_b} == {PARTITION_B}


@requires_tantivy
def test_seed_lexical_index_uses_composition_file(tmp_path: Path) -> None:
    _write_composed_layout(tmp_path)
    with _store_with_chunks({PRODUCT_A: PARTITION_A, PRODUCT_B: PARTITION_B}) as store:
        index = seed_lexical_index(
            index_root=tmp_path,
            package=PKG,
            store=store,
            build_id=COMPOSITION_ID,
        )
        assert index is not None
        assert index.build_id == COMPOSITION_ID
        hits = index.search("alpha", [PRODUCT_A], top_k=5)
    assert [h.chunk_id for h in hits] == ["a0"]
    # Запись несёт партицию продукта, а не id композиции (09 §8.4).
    assert {h.build_id for h in hits} == {PARTITION_A}


@requires_tantivy
def test_seed_lexical_index_records_carry_their_product_partition(
    tmp_path: Path,
) -> None:
    """Две партиции в одной композиции: каждый R0-хит несёт **свой** ``pb_``.

    Общий ``build_id`` записи прошёл бы на одном продукте случайно; здесь запрос
    возвращает хиты обоих продуктов, поэтому id композиции или id чужой партиции
    немедленно ломает соответствие продукт → партиция.
    """
    _write_composed_layout(tmp_path)
    with _store_with_chunks({PRODUCT_A: PARTITION_A, PRODUCT_B: PARTITION_B}) as store:
        index = seed_lexical_index(
            index_root=tmp_path,
            package=PKG,
            store=store,
            build_id=COMPOSITION_ID,
        )
        assert index is not None
        assert index.build_id == COMPOSITION_ID
        hits = index.search("alpha", [PRODUCT_A, PRODUCT_B], top_k=5)
    by_product = {hit.product_id: hit.build_id for hit in hits}
    assert by_product == {PRODUCT_A: PARTITION_A, PRODUCT_B: PARTITION_B}
    assert {hit.chunk_id for hit in hits} == {"a0", "b0"}
    assert COMPOSITION_ID not in {hit.build_id for hit in hits}


@requires_numpy
def test_empty_compositions_dir_keeps_legacy_build_resolvable(tmp_path: Path) -> None:
    """Пустой служебный ``_compositions`` не делает legacy-сборку неоднозначной."""
    _write_build(tmp_path)
    index_format.compositions_dir(tmp_path, PKG).mkdir(parents=True)
    assert index_format.list_build_ids(tmp_path, PKG) == [BUILD]
    with _store_with_chunks() as store:
        index = build_vector_index(
            index_root=tmp_path, package=PKG, chunk_lookup=store.get_chunk
        )
    assert isinstance(index, NpyVectorIndex)
    assert index.build_id == BUILD


@requires_numpy
def test_single_composition_wins_over_legacy_dirs(tmp_path: Path) -> None:
    """09 §8.1: N per-product каталогов — норма; единственная композиция разрешает сборку.

    Старое правило «>1 верхнеуровневых каталогов → ``None``» молча выключило бы dense
    после перехода на per-product партиции.
    """
    _write_composed_layout(tmp_path)
    # Рядом лежит и legacy-сборка: композиция всё равно главнее (у неё явный состав).
    index_format.write_partition(
        tmp_path, PKG, "ib_legacy", PRODUCT_A, [[1.0, 0.0]], ["a0"], {"dim": 2}
    )
    assert len(index_format.list_build_ids(tmp_path, PKG)) > 1
    with _store_with_chunks({PRODUCT_A: PARTITION_A, PRODUCT_B: PARTITION_B}) as store:
        index = build_vector_index(
            index_root=tmp_path, package=PKG, chunk_lookup=store.get_chunk
        )
        assert isinstance(index, NpyVectorIndex)
        assert index.build_id == COMPOSITION_ID
        hits = index.search(
            VectorQuery(product_ids=[PRODUCT_A], top_k=5, vector=[1.0, 0.0])
        )
    assert [h.chunk_id for h in hits] == ["a0", "a1"]


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
