"""Тесты эксперимента ретривала R0/R1/R2 (T8.6, R9/R10, I20, A3).

Используют лёгкие in-memory фейки портов (без tantivy/numpy) и нейтральный
retrieval-golden-фикстур. Проверяют: загрузку golden, прогон профилей, единый cut-off K
(I20), детерминированный выбор профиля (recall → MRR → порядок R2>R1>R0), сохранение
консервативного R0 при ``min_delta``, пропуск dense-профилей без ``embed`` и
``to_manifest_json``. Нейтральны (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.pipeline.experiments import (
    DEFAULT_PROFILES,
    ExperimentReport,
    ProfileExperiment,
    load_retrieval_golden,
    run_retrieval_experiment,
)
from core.pipeline.retrieve import RetrievalProfile
from core.ports.vector_index import RetrievedChunk
from core.util.retrieval_eval import GoldenQuery

FIXTURES = Path(__file__).resolve().parent / "fixtures"
GOLDEN_PATH = FIXTURES / "retrieval_golden.json"


def _chunk(cid: str, score: float, product: str = "p1", build: str = "b1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=cid,
        product_id=product,
        text=f"text-{cid}",
        content_hash=f"hash-{cid}",
        heading_path=f"H-{cid}",
        build_id=build,
        score=score,
    )


def _ranked(ids: list[str]) -> list[RetrievedChunk]:
    """Позиционный порядок → убывающий score (чтобы ``retrieve`` не переупорядочил по id)."""
    return [_chunk(cid, score=float(len(ids) - i)) for i, cid in enumerate(ids)]


class FakeLexical:
    """Фиксированный BM25-порт: ``query -> ordered chunk_id`` (нейтральный фейк)."""

    engine_version = "fake-bm25-1"

    def __init__(self, by_query: dict[str, list[str]]) -> None:
        self._by_query = by_query
        self.calls: list[tuple] = []

    def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001
        self.calls.append((query, list(product_ids), top_k, lang))
        return _ranked(self._by_query.get(query, [])[:top_k])


class FakeVector:
    """Фиксированный dense-порт: ``tuple(vector) -> ordered chunk_id`` (нейтральный фейк)."""

    build_id = "build-dense-1"
    dim = 2

    def __init__(self, by_vector: dict[tuple, list[str]]) -> None:
        self._by_vector = by_vector
        self.calls: list = []

    def search(self, q):  # noqa: ANN001
        self.calls.append(q)
        key = tuple(q.vector or [])
        return _ranked(self._by_vector.get(key, [])[: q.top_k])

    def shard_ids(self, product_id):  # noqa: ANN001
        return [product_id]


def _embedder(mapping: dict[str, list[float]]):
    return lambda text: mapping[text]


def _hybrid_setup():
    """Q1/Q2 решаются одним плечом, Q3 — только гибридом (R2 recall 1.0 > R0=R1=2/3)."""
    lexical = FakeLexical(
        {
            "механизм альфа": ["c1"],
            "beta mechanism": ["c2"],
            "gamma component": ["c9"],  # miss в lexical
        }
    )
    vectors = {"механизм альфа": [1.0, 0.0], "beta mechanism": [2.0, 0.0], "gamma component": [3.0, 0.0]}
    vector = FakeVector(
        {
            (1.0, 0.0): ["c8"],  # miss в dense
            (2.0, 0.0): ["c2"],
            (3.0, 0.0): ["c3"],
        }
    )
    return lexical, vector, _embedder(vectors)


# -- load_retrieval_golden ------------------------------------------------- #


def test_load_golden_fixture() -> None:
    golden = load_retrieval_golden(GOLDEN_PATH)
    assert len(golden) == 3
    assert golden[0].query == "механизм альфа"
    assert golden[0].expected_ids == frozenset({"c1"})
    assert golden[0].lang == "ru"
    assert golden[1].lang == "en"


@pytest.mark.parametrize(
    "payload",
    [
        "{}",  # не список
        "[1]",  # элемент не объект
        '[{"expected_ids": ["c1"]}]',  # нет query
        '[{"query": "", "expected_ids": ["c1"]}]',  # пустой query
        '[{"query": "q", "expected_ids": []}]',  # пустой expected_ids
        '[{"query": "q", "expected_ids": [1]}]',  # id не строка
        '[{"query": "q", "expected_ids": ["c1"], "lang": 1}]',  # lang не строка
        "not json",  # не JSON
    ],
)
def test_load_golden_rejects_bad_payload(tmp_path: Path, payload: str) -> None:
    path = tmp_path / "bad.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(ValueError):
        load_retrieval_golden(path)


def test_load_golden_lang_defaults_to_unknown(tmp_path: Path) -> None:
    path = tmp_path / "g.json"
    path.write_text('[{"query": "q", "expected_ids": ["c1"]}]', encoding="utf-8")
    (item,) = load_retrieval_golden(path)
    assert item.lang == "unknown"


# -- прогон профилей и выбор ----------------------------------------------- #


def test_runs_all_requested_profiles() -> None:
    golden = load_retrieval_golden(GOLDEN_PATH)
    lexical, vector, embed = _hybrid_setup()
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=3,
        lexical_index=lexical,
        vector_index=vector,
        embed=embed,
    )
    assert isinstance(report, ExperimentReport)
    assert set(report.by_profile) == {"R0", "R1", "R2"}
    assert set(report.per_query) == {"R0", "R1", "R2"}
    assert report.skipped == {}


def test_higher_recall_wins() -> None:
    golden = load_retrieval_golden(GOLDEN_PATH)
    lexical, vector, embed = _hybrid_setup()
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=3,
        lexical_index=lexical,
        vector_index=vector,
        embed=embed,
    )
    assert report.by_profile["R0"].recall_at_k == pytest.approx(2 / 3)
    assert report.by_profile["R1"].recall_at_k == pytest.approx(2 / 3)
    assert report.by_profile["R2"].recall_at_k == pytest.approx(1.0)
    assert report.selected_profile == "R2"
    assert "R2" in report.selection_basis


def test_mrr_tie_break() -> None:
    # R0 и R1 совпадают по recall@K, но R1 ставит ожидаемый чанк выше → выигрывает по MRR.
    golden = [
        GoldenQuery(query="q1", expected_ids=frozenset({"c1"})),
        GoldenQuery(query="q2", expected_ids=frozenset({"c2"})),
    ]
    lexical = FakeLexical({"q1": ["cX", "c1"], "q2": ["cY", "c2"]})
    vectors = {"q1": [1.0, 0.0], "q2": [2.0, 0.0]}
    vector = FakeVector({(1.0, 0.0): ["c1", "cX"], (2.0, 0.0): ["c2", "cY"]})
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=2,
        profiles=(RetrievalProfile.R0, RetrievalProfile.R1),
        lexical_index=lexical,
        vector_index=vector,
        embed=_embedder(vectors),
    )
    assert report.by_profile["R0"].recall_at_k == report.by_profile["R1"].recall_at_k
    assert report.by_profile["R1"].mrr > report.by_profile["R0"].mrr
    assert report.selected_profile == "R1"


def test_profile_order_tie_break_prefers_hybrid() -> None:
    # Все три профиля дают одинаковый ранг → побеждает лучший гибрид R2 (порядок R2>R1>R0).
    golden = load_retrieval_golden(GOLDEN_PATH)
    lexical = FakeLexical(
        {"механизм альфа": ["c1"], "beta mechanism": ["c2"], "gamma component": ["c3"]}
    )
    vectors = {"механизм альфа": [1.0, 0.0], "beta mechanism": [2.0, 0.0], "gamma component": [3.0, 0.0]}
    vector = FakeVector(
        {(1.0, 0.0): ["c1"], (2.0, 0.0): ["c2"], (3.0, 0.0): ["c3"]}
    )
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=3,
        lexical_index=lexical,
        vector_index=vector,
        embed=_embedder(vectors),
    )
    assert report.by_profile["R0"].mrr == report.by_profile["R1"].mrr == report.by_profile["R2"].mrr
    assert report.selected_profile == "R2"


def test_min_delta_keeps_r0_baseline() -> None:
    golden = load_retrieval_golden(GOLDEN_PATH)
    lexical, vector, embed = _hybrid_setup()
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=3,
        lexical_index=lexical,
        vector_index=vector,
        embed=embed,
        min_delta=0.5,  # улучшение 1/3 < 0.5 → консервативный R0 (A3)
    )
    assert report.selected_profile == "R0"
    assert "min_delta" in report.selection_basis


# -- cut-off K (I20) ------------------------------------------------------- #


def test_cutoff_k_propagated_to_retrieval_and_eval() -> None:
    golden = load_retrieval_golden(GOLDEN_PATH)
    lexical, vector, embed = _hybrid_setup()
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=2,
        lexical_index=lexical,
        vector_index=vector,
        embed=embed,
    )
    assert report.k == 2
    assert all(profile_report.k == 2 for profile_report in report.by_profile.values())
    # Порт BM25 получил top_k == cutoff_K на каждом вызове.
    assert lexical.calls and all(call[2] == 2 for call in lexical.calls)
    # Порт dense получил top_k == cutoff_K (VectorQuery.top_k).
    assert vector.calls and all(q.top_k == 2 for q in vector.calls)


def test_invalid_cutoff_k_rejected() -> None:
    with pytest.raises(ValueError):
        run_retrieval_experiment(
            [],
            product_ids=["p1"],
            cutoff_k=0,
            lexical_index=FakeLexical({}),
        )


# -- пропуск dense-профилей ------------------------------------------------ #


def test_dense_profiles_skipped_without_embed() -> None:
    golden = load_retrieval_golden(GOLDEN_PATH)
    lexical = FakeLexical({"механизм альфа": ["c1"], "beta mechanism": ["c2"], "gamma component": ["c3"]})
    vector = FakeVector({})
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=3,
        lexical_index=lexical,
        vector_index=vector,
        embed=None,  # dense-вектор запроса недоступен
    )
    assert set(report.by_profile) == {"R0"}
    assert set(report.skipped) == {"R1", "R2"}
    assert all("embed" in reason for reason in report.skipped.values())
    assert report.selected_profile == "R0"


def test_all_profiles_skipped_raises() -> None:
    with pytest.raises(ValueError):
        run_retrieval_experiment(
            load_retrieval_golden(GOLDEN_PATH),
            product_ids=["p1"],
            cutoff_k=3,
        )


def test_profile_dependencies_missing_skips() -> None:
    # R0 требует lexical_index; R2 — оба порта; отсутствие портов фиксируется как skip.
    lexical = FakeLexical({"q": ["c1"]})
    report = run_retrieval_experiment(
        [GoldenQuery(query="q", expected_ids=frozenset({"c1"}))],
        product_ids=["p1"],
        cutoff_k=3,
        lexical_index=lexical,
        vector_index=None,
        embed=None,
    )
    assert set(report.by_profile) == {"R0"}
    assert set(report.skipped) == {"R1", "R2"}


# -- манифест и пустой golden ---------------------------------------------- #


def test_to_manifest_json_round_trips() -> None:
    golden = load_retrieval_golden(GOLDEN_PATH)
    lexical, vector, embed = _hybrid_setup()
    report = run_retrieval_experiment(
        golden,
        product_ids=["p1"],
        cutoff_k=3,
        lexical_index=lexical,
        vector_index=vector,
        embed=embed,
        rrf_k=42,
    )
    raw = report.to_manifest_json()
    assert raw == report.to_manifest_json()  # детерминирован
    data = json.loads(raw)
    assert data["selected_profile"] == report.selected_profile
    assert data["k"] == 3
    assert data["rrf_k"] == 42
    assert "selection_basis" in data
    assert set(data["metrics"]) == {"R0", "R1", "R2"}
    assert data["metrics"]["R2"]["recall_at_k"] == pytest.approx(1.0)
    assert data["skipped"] == {}


def test_empty_golden_handled_deterministically() -> None:
    lexical = FakeLexical({})
    vector = FakeVector({})
    embed = _embedder({})
    first = run_retrieval_experiment(
        [], product_ids=["p1"], cutoff_k=5, lexical_index=lexical, vector_index=vector, embed=embed
    )
    second = run_retrieval_experiment(
        [], product_ids=["p1"], cutoff_k=5, lexical_index=lexical, vector_index=vector, embed=embed
    )
    assert first.k == 5
    assert set(first.by_profile) == {"R0", "R1", "R2"}
    assert all(r.queries == 0 and r.recall_at_k == 0.0 for r in first.by_profile.values())
    assert first.selected_profile in first.by_profile
    assert first.selected_profile == second.selected_profile


# -- контракт данных ------------------------------------------------------- #


def test_default_profiles_and_dataclasses() -> None:
    assert DEFAULT_PROFILES == (
        RetrievalProfile.R0,
        RetrievalProfile.R1,
        RetrievalProfile.R2,
    )
    from core.util.retrieval_eval import RetrievalReport

    experiment = ProfileExperiment(
        profile="R0",
        report=RetrievalReport(k=3, queries=1, hit_at_k=1.0, recall_at_k=1.0, mrr=1.0),
        cutoff_k=3,
    )
    assert experiment.cutoff_k == 3 and experiment.profile == "R0"
