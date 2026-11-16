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
from core.ports.vector_index import RetrievalResult, RetrievedChunk


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


# -- пул плеч R2 (``pool_multiplier``) ------------------------------------- #


def _r2_call(
    lexical: _FakeLexical,
    vector: _FakeVector,
    *,
    cutoff_k: int,
    pool_multiplier: int | None = None,
) -> RetrievalResult:
    """R2-вызов с явным множителем пула (``None`` — дефолт модуля, не передан)."""
    kwargs: dict[str, int] = {}
    if pool_multiplier is not None:
        kwargs["pool_multiplier"] = pool_multiplier
    return retrieve(
        query_text="q",
        query_vector=[1.0, 0.0, 0.0],
        product_ids=["p1"],
        profile="R2",
        cutoff_k=cutoff_k,
        lexical_index=lexical,
        vector_index=vector,
        **kwargs,
    )


def test_pool_multiplier_default_is_previous_behaviour() -> None:
    """``pool_multiplier=1`` — тот же срез и те же score, что без аргумента (дефолт)."""
    hits = [_chunk(f"c{i}") for i in range(1, 6)]
    default = _r2_call(
        _FakeLexical(list(hits), build_id=_BOTH_ARMS_BUILD),
        _FakeVector(list(hits), build_id=_BOTH_ARMS_BUILD),
        cutoff_k=3,
    )
    explicit = _r2_call(
        _FakeLexical(list(hits), build_id=_BOTH_ARMS_BUILD),
        _FakeVector(list(hits), build_id=_BOTH_ARMS_BUILD),
        cutoff_k=3,
        pool_multiplier=1,
    )
    assert [(c.chunk_id, c.score, c.build_id) for c in explicit.chunks] == [
        (c.chunk_id, c.score, c.build_id) for c in default.chunks
    ]
    assert explicit.cutoff_K == default.cutoff_K == 3


def test_pool_multiplier_one_queries_arms_with_cutoff_k() -> None:
    """Множитель 1 — окно плеч равно K: прежние ``top_k`` у обоих портов."""
    lex = _FakeLexical([_chunk("c1")], build_id=_BOTH_ARMS_BUILD)
    vec = _FakeVector([_chunk("c1")], build_id=_BOTH_ARMS_BUILD)
    _r2_call(lex, vec, cutoff_k=3, pool_multiplier=1)
    assert lex.calls[0][2] == 3
    assert vec.calls[0].top_k == 3


def test_pool_multiplier_widens_pool_and_can_promote_chunk() -> None:
    """Чанк, стоящий 11-м в обоих плечах, при множителе 4 входит в RRF и поднимается.

    При пуле=K (множитель 1) его не видит ни одно плечо; при пуле=4K сумма двух
    вкладов (2/(rrf_k+11)) выше вклада первого места одного плеча (1/(rrf_k+1)).
    """
    lexical_hits = [_chunk(f"l{i}") for i in range(1, 11)] + [_chunk("x")]
    dense_hits = [_chunk(f"d{i}") for i in range(1, 11)] + [_chunk("x")]

    narrow = _r2_call(
        _FakeLexical(list(lexical_hits), build_id=_BOTH_ARMS_BUILD),
        _FakeVector(list(dense_hits), build_id=_BOTH_ARMS_BUILD),
        cutoff_k=5,
        pool_multiplier=1,
    )
    assert "x" not in [c.chunk_id for c in narrow.chunks]

    lex = _FakeLexical(list(lexical_hits), build_id=_BOTH_ARMS_BUILD)
    vec = _FakeVector(list(dense_hits), build_id=_BOTH_ARMS_BUILD)
    wide = _r2_call(lex, vec, cutoff_k=5, pool_multiplier=4)
    assert lex.calls[0][2] == 20  # пул = cutoff_k * pool_multiplier
    assert vec.calls[0].top_k == 20
    assert next(c.chunk_id for c in wide.chunks) == "x"  # 2/(60+11) > 1/(60+1)
    assert len(wide.chunks) == 5  # срез остаётся K (I20)
    assert wide.cutoff_K == 5


@pytest.mark.parametrize("bad", [0, -1])
def test_pool_multiplier_must_be_positive(bad: int) -> None:
    """``pool_multiplier <= 0`` — отказ с числом, а не пустое/отрицательное окно."""
    with pytest.raises(ValueError, match="pool_multiplier") as excinfo:
        _r2_call(
            _FakeLexical([_chunk("c1")], build_id=_BOTH_ARMS_BUILD),
            _FakeVector([_chunk("c1")], build_id=_BOTH_ARMS_BUILD),
            cutoff_k=5,
            pool_multiplier=bad,
        )
    assert str(bad) in str(excinfo.value)


def test_ready_dense_hits_are_not_requeried_or_truncated() -> None:
    """Готовое dense-плечо (батч) не перезапрашивается и не обрезается до K здесь.

    ``X`` стоит 15-м в переданном плече. Если бы ``retrieve`` срезал готовые хиты до
    ``cutoff_k`` (2), вклад dense у ``X`` пропал бы, и на первом месте оказался бы ``A``
    (1/(60+1) > 1/(60+2)); с полным плечом ``X`` побеждает суммой двух вкладов.
    """
    lexical_hits = [_chunk("A"), _chunk("X")]
    dense_hits = [_chunk(f"D{i}") for i in range(1, 15)] + [_chunk("X")]
    vec = _FakeVector([_chunk("never-queried")], build_id=_BOTH_ARMS_BUILD)
    result = retrieve(
        query_text="q",
        query_vector=[1.0, 0.0, 0.0],
        product_ids=["p1"],
        profile="R2",
        cutoff_k=2,
        lexical_index=_FakeLexical(lexical_hits, build_id=_BOTH_ARMS_BUILD),
        vector_index=vec,
        dense_hits=dense_hits,
        pool_multiplier=2,
    )
    assert vec.calls == []  # готовое плечо не перезапрашивается
    assert [c.chunk_id for c in result.chunks] == ["X", "A"]
    assert result.chunks[0].score == pytest.approx(1 / (60 + 2) + 1 / (60 + 15))


# -- глубина поиска одного запроса (``query_depth``) ------------------------ #
#
# Зачем аргумент: политики кросс-запросного слияния различают запросы только тем, что
# каждый из них вернул. При глубине, равной ``cutoff_k``, каждый запрос отдаёт не больше
# K чанков, и ``per_query`` (union без обрезки) не может отличаться от ``max`` (union,
# обрезанный до K) — сравнивались бы две записи одного списка.


def test_query_depth_none_is_previous_behaviour() -> None:
    """``query_depth=None`` — те же чанки, те же score, то же окно портов."""
    hits = [_chunk(f"c{i}") for i in range(1, 6)]
    default = _r2_call(
        _FakeLexical(list(hits), build_id=_BOTH_ARMS_BUILD),
        _FakeVector(list(hits), build_id=_BOTH_ARMS_BUILD),
        cutoff_k=3,
    )
    lex = _FakeLexical(list(hits), build_id=_BOTH_ARMS_BUILD)
    vec = _FakeVector(list(hits), build_id=_BOTH_ARMS_BUILD)
    explicit = retrieve(
        query_text="q",
        query_vector=[1.0, 0.0, 0.0],
        product_ids=["p1"],
        profile="R2",
        cutoff_k=3,
        lexical_index=lex,
        vector_index=vec,
        query_depth=None,
    )
    assert [(c.chunk_id, c.score) for c in explicit.chunks] == [
        (c.chunk_id, c.score) for c in default.chunks
    ]
    assert explicit.cutoff_K == default.cutoff_K == 3
    assert lex.calls[0][2] == 3 and vec.calls[0].top_k == 3


def test_query_depth_widens_the_window_but_cutoff_k_stays_the_cutoff() -> None:
    """Глубина ``2K`` отдаёт до ``2K`` чанков до слияния; ``cutoff_K`` не меняется (I20)."""
    hits = [_chunk(f"c{i}") for i in range(1, 7)]
    lex = _FakeLexical(hits)
    result = retrieve(
        query_text="q",
        query_vector=None,
        product_ids=["p1"],
        profile="R0",
        cutoff_k=2,
        lexical_index=lex,
        query_depth=4,
    )
    assert lex.calls[0][2] == 4  # окно запроса — объявленная глубина
    assert [c.chunk_id for c in result.chunks] == ["c1", "c2", "c3", "c4"]
    assert result.cutoff_K == 2  # объявленный cut-off не поехал


def test_query_depth_below_cutoff_k_does_not_shrink_the_window() -> None:
    """Глубина ниже K выдачу не укорачивает: иначе cut-off стал бы недостижим молча."""
    hits = [_chunk(f"c{i}") for i in range(1, 6)]
    lex = _FakeLexical(hits)
    result = retrieve(
        query_text="q",
        query_vector=None,
        product_ids=["p1"],
        profile="R0",
        cutoff_k=4,
        lexical_index=lex,
        query_depth=2,
    )
    assert lex.calls[0][2] == 4
    assert len(result.chunks) == 4


@pytest.mark.parametrize("bad", [0, -1])
def test_query_depth_must_be_positive(bad: int) -> None:
    """``query_depth <= 0`` — отказ с числом, а не тихий возврат к K (A3)."""
    with pytest.raises(ValueError, match="query_depth") as excinfo:
        retrieve(
            query_text="q",
            query_vector=None,
            product_ids=["p1"],
            profile="R0",
            cutoff_k=5,
            lexical_index=_FakeLexical([]),
            query_depth=bad,
        )
    assert str(bad) in str(excinfo.value)


def test_query_depth_widens_r2_pool_and_result() -> None:
    """У R2 окно — максимум из пула и глубины: иначе глубина не дошла бы до слияния."""
    hits = [_chunk(f"c{i}") for i in range(1, 9)]
    lex = _FakeLexical(list(hits), build_id=_BOTH_ARMS_BUILD)
    vec = _FakeVector(list(hits), build_id=_BOTH_ARMS_BUILD)
    result = _r2_call(lex, vec, cutoff_k=2, pool_multiplier=2)
    assert lex.calls[0][2] == 4  # пул = 2 * 2
    assert vec.calls[0].top_k == 4
    assert len(result.chunks) == 2  # срез K

    lex = _FakeLexical(list(hits), build_id=_BOTH_ARMS_BUILD)
    vec = _FakeVector(list(hits), build_id=_BOTH_ARMS_BUILD)
    deep = retrieve(
        query_text="q",
        query_vector=[1.0, 0.0, 0.0],
        product_ids=["p1"],
        profile="R2",
        cutoff_k=2,
        lexical_index=lex,
        vector_index=vec,
        pool_multiplier=2,
        query_depth=6,
    )
    assert lex.calls[0][2] == 6  # max(2*2, 6)
    assert vec.calls[0].top_k == 6
    assert len(deep.chunks) == 6
    assert deep.cutoff_K == 2
