"""Тесты батч-пути фазы оценки Ш6 (п.3/п.4 doc 14 §6).

Проверяется то, что было дефектом:

* (д) мемоизация: один и тот же текст запроса эмбеддится **один** раз в прогоне, а не
  на каждый продукт когорты (карта ``text → vector`` + ``memo``);
* (е) пре-компьют: ``evaluate_phase`` считает векторы **до** цикла оценки и передаёт их
  через ``query_vectors``, поэтому эмбеддер внутри цикла не вызывается вовсе;
* батч: dense-плечо продукта считается **одним** вызовом порта на все запросы пакета
  (``search_many``), а не поштучно (``search``);
* ``retrieve_for_cell`` с готовым dense-плечом даёт тот же ``RetrievalResult``, что и
  поштучный путь (склейка запросов ячейки не изменилась).

Фикстуры категорийно нейтральны (§1.3); сеть/диск не используются.
"""

from __future__ import annotations

import datetime as _dt

import pytest

from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    CatalogRow,
    CellState,
    Cohort,
    CohortEntry,
    CorpusCoverage,
    DenialStrength,
    FailureKind,
    Importance,
    LLMCellOutput,
    ParamType,
    RankingPolicy,
    RunConfig,
    RunMode,
    Status,
)
from core.pipeline.evaluate import (
    evaluate_phase,
    precompute_dense_hits,
    precompute_query_vectors,
    retrieve_for_cell,
    unique_queries,
)
from core.pipeline.plan import RunPlan
from core.pipeline.runner import Ports
from core.ports.vector_index import RetrievedChunk, VectorQuery, batched_search
from core.store.sqlite_store import SqliteStore
from tests.fakes.ports import FakeEvaluator

RUN_ID = "run-batch"
DIM = 4
FIXED_TS = "2026-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Фикстуры (нейтральные)
# ---------------------------------------------------------------------------


def _param(param_id: str, title: str, ru: str, en: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="mod_a",
        group="grp_1",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule=f"Подтвердить {ru}.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="role_a",
        concept_terms={"ru": [ru], "en": [en]},
        search_langs=["ru", "en"],
    )


def _params() -> list[CatalogRow]:
    return [
        _param("param_a", "Параметр Альфа", "свойство альфа", "alpha feature"),
        _param("param_b", "Параметр Бета", "свойство бета", "beta feature"),
    ]


def _plan() -> RunPlan:
    cohort = Cohort(
        case_id="case-1",
        frozen_at=_dt.date(2026, 1, 1),
        entries=[
            CohortEntry(
                product_id="p1",
                role_overlap=1.0,
                selection_reason="anchor",
                anchor_relation=AnchorRelation.ANCHOR,
            ),
            CohortEntry(
                product_id="p2",
                role_overlap=0.5,
                selection_reason="analog",
                anchor_relation=AnchorRelation.ANALOG,
            ),
        ],
    )
    return RunPlan(
        run_id=RUN_ID,
        products=["p1", "p2"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=_params(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=cohort,
        golden=[],
        warnings=[],
    )


def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case-1",
            anchor="p1",
            cohort="cohort.yaml",
            products=["p1", "p2"],
            package="pkg-1",
            languages=["ru"],
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    for product_id in ("p1", "p2"):
        store.save_corpus_coverage(
            RUN_ID,
            CorpusCoverage(
                product_id=product_id,
                complete=True,
                expected_sources=1,
                observed_sources=1,
                computed_at=FIXED_TS,
            ),
        )
    return store


def _ok_output(chunk_id: str) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство подтверждено документацией.",
        evidence=[{"source_id": chunk_id, "quote": f"текст {chunk_id}"}],
    )


class _CountingEmbedder:
    """Детерминированный эмбеддер, считающий **вызовы** (тексты батчами)."""

    def __init__(self, dim: int = DIM) -> None:
        self.dim = dim
        self.model_id = "fake-embedder"
        self.calls: list[list[str]] = []

    def embed(self, texts):  # noqa: ANN001, ANN202
        batch = [str(text) for text in texts]
        self.calls.append(batch)
        return [
            [float((len(text) + i) % 7 + 1) for i in range(self.dim)] for text in batch
        ]

    @property
    def embedded_texts(self) -> int:
        return sum(len(batch) for batch in self.calls)


class _RecordingVectorIndex:
    """``VectorIndexPort`` с раздельным учётом поштучных и батч-вызовов (I17)."""

    build_id = "build-vec"
    dim = DIM

    def __init__(self) -> None:
        self.search_calls: list[VectorQuery] = []
        # Батч-вызовы: ``(размер батча, продукт-скоуп каждого запроса)``. Текста в
        # ``VectorQuery`` нет намеренно — батч-путь формирует запрос так же, как
        # поштучный (см. ``precompute_dense_hits``), поэтому учёт идёт по структуре.
        self.many_calls: list[tuple[int, list[list[str]]]] = []

    def _hits(self, q: VectorQuery) -> list[RetrievedChunk]:
        hits = []
        for product_id in q.product_ids:
            chunk_id = f"chunk-{product_id}"
            hits.append(
                RetrievedChunk(
                    chunk_id=chunk_id,
                    product_id=product_id,
                    text=f"текст {chunk_id}",
                    content_hash=f"hash-{chunk_id}",
                    heading_path=f"H-{chunk_id}",
                    build_id=self.build_id,
                    score=0.5 + 0.1 * len(q.text or ""),
                )
            )
        return hits[: q.top_k]

    def search(self, q: VectorQuery) -> list[RetrievedChunk]:
        self.search_calls.append(q)
        return self._hits(q)

    def search_many(self, queries) -> list[list[RetrievedChunk]]:  # noqa: ANN001
        self.many_calls.append((len(queries), [list(q.product_ids) for q in queries]))
        return [self._hits(q) for q in queries]

    def shard_ids(self, product_id: str) -> list[str]:
        return [product_id]


# ---------------------------------------------------------------------------
# (д) мемоизация вектора запроса по тексту
# ---------------------------------------------------------------------------


def test_precompute_is_memoised_across_calls() -> None:
    """Повторный пре-компьют с той же ``memo`` не эмбеддит ничего."""
    embedder = _CountingEmbedder()
    memo: dict[str, list[float]] = {}
    first = precompute_query_vectors(
        _params(), profile="R1", embedder=embedder, memo=memo
    )
    assert embedder.embedded_texts == len(unique_queries(_params()))
    second = precompute_query_vectors(
        _params(), profile="R1", embedder=embedder, memo=memo
    )
    assert second == first
    assert len(embedder.calls) == 1  # второго вызова эмбеддера не было


def test_precompute_covers_every_unique_query_text() -> None:
    """Карта покрывает все тексты ``render_queries`` — одиночного пути в цикле нет."""
    embedder = _CountingEmbedder()
    texts = unique_queries(_params())
    vectors = precompute_query_vectors(_params(), profile="R1", embedder=embedder)
    assert vectors is not None
    assert set(vectors) == set(texts)
    # Тексты батча — ровно уникальные, без повторов (≤3 на параметр).
    assert sorted(text for batch in embedder.calls for text in batch) == sorted(texts)


def test_precompute_skipped_for_lexical_profile() -> None:
    """R0 не нуждается в dense-векторах: эмбеддер не вызывается вовсе."""
    embedder = _CountingEmbedder()
    assert precompute_query_vectors(_params(), profile="R0", embedder=embedder) is None
    assert embedder.calls == []


# ---------------------------------------------------------------------------
# (е) пре-компьют до цикла оценки + батч по продукту
# ---------------------------------------------------------------------------


def test_evaluate_phase_never_embeds_inside_cell_loop() -> None:
    """Векторы считаются до цикла: эмбеддер вызван один раз на пакет, поиск — батчем."""
    store = _store()
    plan = _plan()
    embedder = _CountingEmbedder()
    vector_index = _RecordingVectorIndex()
    evaluator = FakeEvaluator(default=_ok_output("chunk-p1"))
    ports = Ports(
        evaluator=evaluator,
        embedder=embedder,
        vector_index=vector_index,
    )
    try:
        result = evaluate_phase(
            plan=plan, ports=ports, store=store, run_id=RUN_ID, profile="R1", cutoff_k=3
        )
    finally:
        store.close()

    assert result.status == "ok"
    texts = unique_queries(plan.params)
    # Один вызов эмбеддера на весь пакет (и на все продукты): мемоизация по тексту.
    assert len(embedder.calls) == 1
    assert sorted(embedder.calls[0]) == sorted(texts)
    # dense-плечо — один батч на продукт, поштучных поисков нет.
    assert vector_index.search_calls == []
    assert len(vector_index.many_calls) == 2
    # Батч покрывает все уникальные запросы пакета, в одном вызове на продукт.
    assert all(size == len(texts) for size, _ in vector_index.many_calls)
    assert result.errors == []


def test_evaluate_phase_dense_path_keeps_product_scope() -> None:
    """Батч не расширяет scope: каждый продукт считает только свою партицию (I17)."""
    store = _store()
    plan = _plan()
    seen: list[list[str]] = []
    vector_index = _RecordingVectorIndex()
    original = vector_index.search_many

    def recording(queries):  # noqa: ANN001, ANN202
        seen.extend([list(q.product_ids) for q in queries])
        return original(queries)

    vector_index.search_many = recording  # type: ignore[method-assign]
    evaluator = FakeEvaluator(default=_ok_output("chunk-p1"))
    ports = Ports(evaluator=evaluator, embedder=_CountingEmbedder(), vector_index=vector_index)
    try:
        result = evaluate_phase(
            plan=plan, ports=ports, store=store, run_id=RUN_ID, profile="R1", cutoff_k=3
        )
    finally:
        store.close()
    assert result.status == "ok"
    # В каждом батч-вызове — ровно один продукт, и это продукт прохода.
    assert {tuple(scope) for scope in seen} == {("p1",), ("p2",)}


def test_batched_dense_hits_equal_per_query_path() -> None:
    """Готовое dense-плечо даёт тот же ``RetrievalResult``, что и поштучный поиск."""
    param = _params()[0]
    embedder = _CountingEmbedder()
    vectors = precompute_query_vectors([param], profile="R1", embedder=embedder)
    assert vectors is not None
    index = _RecordingVectorIndex()

    batched = precompute_dense_hits(
        [param],
        product_ids=["p1"],
        profile="R1",
        cutoff_k=3,
        vector_index=index,
        query_vectors=vectors,
    )
    assert batched is not None
    assert len(index.many_calls) == 1
    assert set(batched) == set(unique_queries([param]))  # карта покрывает все уникальные тексты

    with_batch = retrieve_for_cell(
        param,
        product_id="p1",
        profile="R1",
        cutoff_k=3,
        vector_index=index,
        query_vectors=vectors,
        dense_by_query=batched,
    )
    assert index.search_calls == []  # поштучного пути не было
    per_query = retrieve_for_cell(
        param,
        product_id="p1",
        profile="R1",
        cutoff_k=3,
        vector_index=index,
        query_vectors=vectors,
    )
    assert [c.chunk_id for c in with_batch.chunks] == [c.chunk_id for c in per_query.chunks]
    assert [(c.score, c.build_id) for c in with_batch.chunks] == [
        (c.score, c.build_id) for c in per_query.chunks
    ]
    assert with_batch.build_id == per_query.build_id
    assert with_batch.profile == per_query.profile
    assert with_batch.cutoff_K == per_query.cutoff_K
    assert len(index.search_calls) == len(unique_queries([param]))  # второй путь — поштучный


def test_batched_search_helper_uses_port_batch() -> None:
    """``batched_search`` предпочитает ``search_many`` порта (одна точка вызова)."""
    index = _RecordingVectorIndex()
    vector = [0.1] * DIM
    queries = [
        VectorQuery(product_ids=["p1"], top_k=2, text=f"q{i}", vector=vector)
        for i in range(3)
    ]
    results = batched_search(index, queries)
    assert index.many_calls == [(3, [["p1"], ["p1"], ["p1"]])]
    assert index.search_calls == []
    assert [[c.chunk_id for c in hits] for hits in results] == [
        [c.chunk_id for c in index._hits(q)] for q in queries
    ]


def test_precompute_dense_hits_without_vectors_is_absent() -> None:
    """Нет векторов (или порта) — батч не подменяется нулями: возвращается ``None``."""
    index = _RecordingVectorIndex()
    assert (
        precompute_dense_hits(
            _params(),
            product_ids=["p1"],
            profile="R1",
            cutoff_k=3,
            vector_index=index,
            query_vectors=None,
        )
        is None
    )
    assert (
        precompute_dense_hits(
            _params(),
            product_ids=["p1"],
            profile="R1",
            cutoff_k=3,
            vector_index=None,
            query_vectors={"x": [0.0] * DIM},
        )
        is None
    )


def test_precompute_vectors_validates_embedder_output() -> None:
    """Неполный батч — ошибка, а не «сколько пришло» (иначе индекс короче запросов)."""

    class _Short:
        dim = DIM
        model_id = "short"

        def embed(self, texts):  # noqa: ANN001, ANN202
            return []

    with pytest.raises(ValueError, match="векторов"):
        precompute_query_vectors(_params(), profile="R1", embedder=_Short())


# ---------------------------------------------------------------------------
# R2: батч-плечо входит в RRF-слияние без изменения результата
# ---------------------------------------------------------------------------


class _Lexical:
    """BM25-фейк: один хит на запрос, продукт-скоупед (I17)."""

    engine_version = "lex-fake-1"
    build_id = "build-lex"

    def __init__(self, chunk_id: str) -> None:
        self.chunk_id = chunk_id
        self.calls: list[tuple] = []

    def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001
        self.calls.append((query, list(product_ids), top_k))
        return [
            RetrievedChunk(
                chunk_id=self.chunk_id,
                product_id=product_ids[0],
                text=f"текст {self.chunk_id}",
                content_hash=f"hash-{self.chunk_id}",
                heading_path="H",
                build_id=self.build_id,
                score=0.9,
            )
        ][:top_k]


def test_hybrid_profile_fuses_batched_dense_leg() -> None:
    """R2 с готовым dense-плечом даёт тот же срез, что и поштучный путь (I16/I17/I20)."""
    param = _params()[0]
    embedder = _CountingEmbedder()
    vectors = precompute_query_vectors([param], profile="R2", embedder=embedder)
    assert vectors is not None
    index = _RecordingVectorIndex()
    batched = precompute_dense_hits(
        [param],
        product_ids=["p1"],
        profile="R2",
        cutoff_k=5,
        vector_index=index,
        query_vectors=vectors,
    )
    assert batched is not None

    fused = retrieve_for_cell(
        param,
        product_id="p1",
        profile="R2",
        cutoff_k=5,
        lexical_index=_Lexical("chunk-lex"),
        vector_index=index,
        query_vectors=vectors,
        dense_by_query=batched,
    )
    per_query = retrieve_for_cell(
        param,
        product_id="p1",
        profile="R2",
        cutoff_k=5,
        lexical_index=_Lexical("chunk-lex"),
        vector_index=index,
        query_vectors=vectors,
    )
    assert [c.chunk_id for c in fused.chunks] == [c.chunk_id for c in per_query.chunks]
    assert [round(c.score, 12) for c in fused.chunks] == [
        round(c.score, 12) for c in per_query.chunks
    ]
    assert fused.build_id == per_query.build_id == index.build_id
    assert fused.cutoff_K == 5  # I20


def test_dense_batch_failure_is_visible_and_falls_back() -> None:
    """Сбой батча виден в ``errors`` и повторяется поштучно — не тише, чем до правки."""

    class _BrokenBatch(_RecordingVectorIndex):
        def search_many(self, queries):  # noqa: ANN001, ANN202
            raise RuntimeError("batch boom")

    store = _store()
    plan = _plan()
    vector_index = _BrokenBatch()
    evaluator = FakeEvaluator(default=_ok_output("chunk-p1"))
    ports = Ports(
        evaluator=evaluator, embedder=_CountingEmbedder(), vector_index=vector_index
    )
    try:
        result = evaluate_phase(
            plan=plan, ports=ports, store=store, run_id=RUN_ID, profile="R1", cutoff_k=3
        )
    finally:
        store.close()
    assert result.status == "ok"
    assert any("dense batch" in error for error in result.errors)
    assert vector_index.search_calls  # поштучный путь отработал
    assert result.cells  # ячейки построены, а не потеряны

# ---------------------------------------------------------------------------
# (ж) сбой ретривала — failed (I4), а не измеренное «❓»
# ---------------------------------------------------------------------------

class _BrokenLexicalIndex:
    """Лексический индекс, который **падает**: прибор отказал, а не «ничего не нашёл»."""

    engine_version = "fake-bm25-1"
    build_id = "build-lex"

    def __init__(self) -> None:
        self.calls = 0

    def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001, ANN202
        self.calls += 1
        raise RuntimeError("индекс недоступен")

def _store_without_protected_cells() -> SqliteStore:
    return _store()

def test_retrieval_failure_is_failed_not_unknown() -> None:
    """Упавший ретривал даёт ``failed`` с ``instrument_failed``, а не ``❓ no_fragments``.

    Регрессия, которую ловит тест: пустой срез перестал тратить вызов модели, и если
    отказ прибора приходит в оценку как «нет ключа в карте чанков» → ``()``, то ячейка
    публикуется как измеренное «вендор не заявил». I4 запрещает статус при отказе
    инструмента: ``failed`` — единственный честный исход, и он виден в ``errors``.
    """
    store = _store_without_protected_cells()
    plan = _plan()
    index = _BrokenLexicalIndex()
    evaluator = FakeEvaluator(default=_ok_output("chunk-p1"))
    ports = Ports(evaluator=evaluator, lexical_index=index)
    try:
        result = evaluate_phase(
            plan=plan, ports=ports, store=store, run_id=RUN_ID, profile="R0", cutoff_k=3
        )
        persisted = list(store.iter_cells(RUN_ID))
    finally:
        store.close()

    assert result.status == "ok"
    # Индекс реально вызывался и реально падал — сценарий не вырожденный.
    assert index.calls >= 1
    assert any("retrieval: RuntimeError" in error for error in result.errors)
    # Модель не вызывалась: оценивать было нечего, и это не «пустой срез».
    assert evaluator.calls == []
    assert persisted
    for cell in persisted:
        assert cell.state is CellState.FAILED
        assert cell.status is None  # I4: failed никогда не статус
        scope = cell.evidence_scope
        assert scope is not None
        assert scope.denial_strength is DenialStrength.INSTRUMENT_FAILED
        assert scope.termination == FailureKind.SEARCH_FAILED.value
        assert "no_fragments" not in (cell.validation_notes or [])
