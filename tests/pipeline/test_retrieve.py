"""Тесты ретривала Ш6: профили R0/R1/R2, RRF-фьюжн, провенанс (T8.5, I16/I17/I20, R7).

Используют лёгкие in-memory фейки портов (без tantivy/numpy): проверяют логику
профилей и детерминизм фьюжна независимо от адаптеров. Нейтральны (§1.3).
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from core.pipeline.retrieve import (
    PROFILE_DESCRIPTIONS,
    RetrievalBuildMismatch,
    RetrievalProfile,
    retrieve,
    rrf_fuse_chunks,
)
from core.ports.vector_index import RetrievedChunk


def _chunk(cid: str, product: str = "p1", score: float = 1.0, build: str = "b1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=cid,
        product_id=product,
        text=f"text-{cid}",
        content_hash=f"h-{cid}",
        heading_path=f"H-{cid}",
        build_id=build,
        score=score,
    )


#: Одна сборка на оба плеча R2: штатно лексический и dense-индексы строятся из одного
#: ``composition_id`` (wiring.build_vector_index/seed_lexical_index), поэтому успешные
#: R2-фикстуры подают согласованные ``build_id``, а расхождение проверяется отдельно.
_BOTH_ARMS_BUILD = "build-both"


class _FakeLexical:
    engine_version = "fake-bm25-1"
    build_id = "build-lex-1"

    def __init__(self, hits: list[RetrievedChunk], build_id: str | None = None) -> None:
        self._hits = hits
        self.calls: list[tuple] = []
        if build_id is not None:
            self.build_id = build_id

    def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001
        self.calls.append((query, list(product_ids), top_k, lang))
        allowed = set(product_ids)
        return [h for h in self._hits if h.product_id in allowed][:top_k]


class _FakeVector:
    build_id = "build-x"
    dim = 3

    def __init__(self, hits: list[RetrievedChunk], build_id: str | None = None) -> None:
        self._hits = hits
        self.calls: list = []
        if build_id is not None:
            self.build_id = build_id

    def search(self, q):  # noqa: ANN001
        self.calls.append(q)
        allowed = set(q.product_ids)
        return [h for h in self._hits if h.product_id in allowed][: q.top_k]

    def shard_ids(self, product_id):  # noqa: ANN001
        return [product_id]


# -- профиль --------------------------------------------------------------- #


def test_profile_is_required_and_validated() -> None:
    with pytest.raises(ValueError):
        retrieve(
            query_text="q",
            query_vector=None,
            product_ids=["p1"],
            profile="R9",
            cutoff_k=5,
        )


def test_r0_uses_lexical_only() -> None:
    lex = _FakeLexical([_chunk("c1"), _chunk("c2")])
    result = retrieve(
        query_text="q",
        query_vector=None,
        product_ids=["p1"],
        profile=RetrievalProfile.R0,
        cutoff_k=5,
        lexical_index=lex,
    )
    assert result.profile == "R0"
    assert [c.chunk_id for c in result.chunks] == ["c1", "c2"]
    assert result.build_id == "build-lex-1"


def test_r0_build_id_is_build_not_engine_version() -> None:
    """Регрессия: R0-провенанс — сборка, а не ``engine_version`` движка (§5/R15)."""
    lex = _FakeLexical([_chunk("c1")])
    result = retrieve(
        query_text="q",
        query_vector=None,
        product_ids=["p1"],
        profile=RetrievalProfile.R0,
        cutoff_k=5,
        lexical_index=lex,
    )
    assert result.build_id == lex.build_id
    assert result.build_id != lex.engine_version


def test_r0_without_build_id_attribute_yields_empty_provenance() -> None:
    """Порт без ``build_id`` — честное «неизвестно», не подмена версией движка."""

    class _NoBuild:
        engine_version = "engine-only"

        def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001
            return []

    result = retrieve(
        query_text="q",
        query_vector=None,
        product_ids=["p1"],
        profile="R0",
        cutoff_k=5,
        lexical_index=_NoBuild(),
    )
    assert result.build_id == ""


def test_r1_requires_vector() -> None:
    vec = _FakeVector([_chunk("c1")])
    with pytest.raises(ValueError):
        retrieve(
            query_text="q",
            query_vector=None,
            product_ids=["p1"],
            profile="R1",
            cutoff_k=5,
            vector_index=vec,
        )
    result = retrieve(
        query_text="q",
        query_vector=[1.0, 0.0, 0.0],
        product_ids=["p1"],
        profile="R1",
        cutoff_k=5,
        vector_index=vec,
    )
    assert result.profile == "R1" and result.build_id == "build-x"


def test_r2_requires_both_ports() -> None:
    with pytest.raises(ValueError):
        retrieve(
            query_text="q",
            query_vector=[1.0],
            product_ids=["p1"],
            profile="R2",
            cutoff_k=5,
            lexical_index=_FakeLexical([]),
        )


# -- продукт-скоуп (I17) --------------------------------------------------- #


def test_retrieval_is_product_scoped() -> None:
    lex = _FakeLexical([_chunk("c1", "p1"), _chunk("c9", "p2")])
    result = retrieve(
        query_text="q",
        query_vector=None,
        product_ids=["p1"],
        profile="R0",
        cutoff_k=5,
        lexical_index=lex,
    )
    assert [c.chunk_id for c in result.chunks] == ["c1"]
    assert lex.calls[0][1] == ["p1"]  # product_ids переданы в порт


# -- RRF-фьюжн ------------------------------------------------------------- #


def test_rrf_fuses_both_arms_and_dedups() -> None:
    lexical = [_chunk("c1"), _chunk("c2")]
    dense = [_chunk("c2"), _chunk("c3")]
    fused = rrf_fuse_chunks(lexical, dense, rrf_k=60)
    ids = [c.chunk_id for c in fused]
    # c2 есть в обоих плечах → стоит выше c1/c3 (сумма двух вкладов).
    assert ids[0] == "c2"
    assert set(ids) == {"c1", "c2", "c3"}


def test_rrf_deterministic_tie_break() -> None:
    lexical = [_chunk("b"), _chunk("a")]
    dense = [_chunk("a"), _chunk("b")]
    first = rrf_fuse_chunks(lexical, dense, rrf_k=60)
    second = rrf_fuse_chunks(lexical, dense, rrf_k=60)
    assert [c.chunk_id for c in first] == [c.chunk_id for c in second]
    # Равные score (обе позиции симметричны) → tie-break по chunk_id.
    assert [c.chunk_id for c in first] == ["a", "b"]


def test_rrf_top_k_truncates() -> None:
    fused = rrf_fuse_chunks([_chunk(f"c{i}") for i in range(10)], [], rrf_k=60, top_k=3)
    assert len(fused) == 3


def test_rrf_preserves_provenance() -> None:
    fused = rrf_fuse_chunks([_chunk("c1", build="b-lex")], [_chunk("c1", build="b-dense")])
    assert fused[0].content_hash == "h-c1"
    assert fused[0].heading_path == "H-c1"
    assert fused[0].build_id == "b-dense"  # dense-провенанс приоритетен


def test_r2_cutoff_k_applied() -> None:
    # Обе фикстуры — одна сборка: штатно плечи строятся из одного ``composition_id``.
    lex = _FakeLexical(
        [_chunk("c1"), _chunk("c2"), _chunk("c3")], build_id=_BOTH_ARMS_BUILD
    )
    vec = _FakeVector([_chunk("c3"), _chunk("c4")], build_id=_BOTH_ARMS_BUILD)
    result = retrieve(
        query_text="q",
        query_vector=[1.0, 0.0, 0.0],
        product_ids=["p1"],
        profile="R2",
        cutoff_k=2,
        lexical_index=lex,
        vector_index=vec,
    )
    assert len(result.chunks) <= 2
    assert result.cutoff_K == 2
    assert result.build_id == _BOTH_ARMS_BUILD


# -- сборки плеч R2: отказ вместо молчаливого выбора (A3/S5) ---------------- #


def test_r2_rejects_different_known_builds() -> None:
    """Два известных ``build_id`` ⇒ отказ: иначе провенанс одной сборки выдаётся за обе."""
    lex = _FakeLexical([_chunk("c1")], build_id="build-lex-2")
    vec = _FakeVector([_chunk("c1")], build_id="build-dense-2")
    with pytest.raises(RetrievalBuildMismatch) as excinfo:
        retrieve(
            query_text="q",
            query_vector=[1.0, 0.0, 0.0],
            product_ids=["p1"],
            profile="R2",
            cutoff_k=5,
            lexical_index=lex,
            vector_index=vec,
        )
    message = str(excinfo.value)
    assert "build-lex-2" in message and "build-dense-2" in message


def test_r2_unknown_lexical_build_is_not_refused() -> None:
    """Неизвестная сборка плеча — «не удостоверено», а не отказ: сравнивать нечего (A3)."""

    class _LexicalWithoutBuild:
        engine_version = "engine-only"

        def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001
            return [_chunk("c1")]

    for lexical in (_LexicalWithoutBuild(), _FakeLexical([_chunk("c1")], build_id="")):
        result = retrieve(
            query_text="q",
            query_vector=[1.0, 0.0, 0.0],
            product_ids=["p1"],
            profile="R2",
            cutoff_k=5,
            lexical_index=lexical,
            vector_index=_FakeVector([_chunk("c1")], build_id="build-x"),
        )
        # Провенанс результата — dense-плечо (прежнее поведение), сборка не выдумана.
        assert result.build_id == "build-x"


def _fused_one(lexical: RetrievedChunk, dense: RetrievedChunk) -> list[RetrievedChunk]:
    return rrf_fuse_chunks([lexical], [dense])


def test_rrf_rejects_divergent_content_hash() -> None:
    """Один ``chunk_id``, разные ``content_hash`` ⇒ разные единицы доказательства."""
    lexical = _chunk("c1", build="b-lex")
    dense = replace(_chunk("c1", build="b-dense"), content_hash="h-other")
    with pytest.raises(RetrievalBuildMismatch) as excinfo:
        _fused_one(lexical, dense)
    message = str(excinfo.value)
    assert "c1" in message and "content_hash" in message
    assert "'h-c1'" in message and "'h-other'" in message


def test_rrf_rejects_divergent_product_id() -> None:
    lexical = _chunk("c1", product="p1", build="b-lex")
    dense = _chunk("c1", product="p2", build="b-dense")
    with pytest.raises(RetrievalBuildMismatch) as excinfo:
        _fused_one(lexical, dense)
    message = str(excinfo.value)
    assert "c1" in message and "product_id" in message
    assert "'p1'" in message and "'p2'" in message


def test_profile_descriptions_cover_all() -> None:
    assert set(PROFILE_DESCRIPTIONS) == {
        RetrievalProfile.R0,
        RetrievalProfile.R1,
        RetrievalProfile.R2,
    }
