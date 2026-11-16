"""Тесты документного retrieval-эталона (O14 плана 26).

Фикстуры — JSON в ``tmp_path``; без сети и без БД. Проверяют загрузку и валидацию
эталона, дедуп документов top-K чанков, метрики на уровне документа, исключение
``unresolved`` ячеек, детерминизм бутстрэпа и порядок ``per_cell``. Нейтральны (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core.pipeline.retrieval_docs import (
    DOC_GOLDEN_FORMAT,
    DocQueryResult,
    document_ids_for,
    evaluate_documents,
    first_hit_rank,
    load_doc_golden,
)


def _entry(cell: str = "p1/param1", *, expected: list[str] | None = None) -> dict[str, Any]:
    """Запись эталона в формате файла; ``cell`` задаёт product/param по умолчанию."""
    product_id, param_id = cell.split("/", 1)
    return {
        "cell": cell,
        "product_id": product_id,
        "param_id": param_id,
        "source_url": f"https://example.test/{product_id}",
        "expected_doc_ids": ["p1:doc1"] if expected is None else expected,
        "match": "canonical",
    }


def _write_golden(path: Path, entries: list[dict[str, Any]], **extra: Any) -> Path:
    payload: dict[str, Any] = {
        "format": DOC_GOLDEN_FORMAT,
        "case_id": "case-x",
        "corpus_run_id": "case-x-run",
        "entries": entries,
    }
    payload.update(extra)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _result(
    cell: str = "p1/param1",
    *,
    expected: tuple[str, ...] = ("p1:doc1",),
    retrieved: tuple[str, ...] = ("p1:doc1",),
    rank: int | None = 1,
    unresolved: bool = False,
) -> DocQueryResult:
    product_id, param_id = cell.split("/", 1)
    return DocQueryResult(
        cell=cell,
        product_id=product_id,
        param_id=param_id,
        expected_doc_ids=expected,
        retrieved_doc_ids=retrieved,
        rank=rank,
        unresolved=unresolved,
    )


# ---------------------------------------------------------------------------
# load_doc_golden
# ---------------------------------------------------------------------------


def test_load_doc_golden_normal(tmp_path: Path) -> None:
    path = _write_golden(tmp_path / "g.json", [_entry("p1/param1")])
    case_id, corpus_run_id, entries = load_doc_golden(path)
    assert case_id == "case-x"
    assert corpus_run_id == "case-x-run"
    assert len(entries) == 1
    entry = entries[0]
    assert entry.cell == "p1/param1"
    assert entry.product_id == "p1"
    assert entry.param_id == "param1"
    assert entry.source_url == "https://example.test/p1"
    assert entry.expected_doc_ids == ("p1:doc1",)
    assert entry.match == "canonical"


def test_load_doc_golden_wrong_format(tmp_path: Path) -> None:
    path = _write_golden(tmp_path / "g.json", [_entry()], format="other/9")
    with pytest.raises(ValueError, match="format"):
        load_doc_golden(path)


def test_load_doc_golden_empty_expected(tmp_path: Path) -> None:
    path = _write_golden(tmp_path / "g.json", [_entry(expected=[])])
    with pytest.raises(ValueError, match="expected_doc_ids"):
        load_doc_golden(path)


def test_load_doc_golden_duplicate_cell(tmp_path: Path) -> None:
    path = _write_golden(tmp_path / "g.json", [_entry(), _entry()])
    with pytest.raises(ValueError) as excinfo:
        load_doc_golden(path)
    message = str(excinfo.value)
    assert "entries[0]" in message
    assert "entries[1]" in message


def test_load_doc_golden_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_doc_golden(tmp_path / "absent.json")


def test_load_doc_golden_empty_case_and_run_allowed(tmp_path: Path) -> None:
    path = _write_golden(tmp_path / "g.json", [_entry()], case_id="", corpus_run_id="")
    case_id, corpus_run_id, entries = load_doc_golden(path)
    assert case_id == ""
    assert corpus_run_id == ""
    assert len(entries) == 1


def test_load_doc_golden_missing_case_fields_default_empty(tmp_path: Path) -> None:
    payload = {
        "format": DOC_GOLDEN_FORMAT,
        "entries": [_entry()],
    }
    path = tmp_path / "g.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    case_id, corpus_run_id, _ = load_doc_golden(path)
    assert case_id == ""
    assert corpus_run_id == ""


def test_load_doc_golden_entries_not_list(tmp_path: Path) -> None:
    path = _write_golden(tmp_path / "g.json", [])
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["entries"] = {"cell": "p1/param1"}
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="entries"):
        load_doc_golden(path)


# ---------------------------------------------------------------------------
# document_ids_for
# ---------------------------------------------------------------------------


def test_document_ids_for_dedup_preserves_order() -> None:
    chunk_doc = {"c1": "d1", "c2": "d2", "c3": "d1", "c4": "d3"}
    assert document_ids_for(["c1", "c2", "c3", "c4"], chunk_doc, top_k=10) == (
        "d1",
        "d2",
        "d3",
    )


def test_document_ids_for_truncates_by_top_k() -> None:
    chunk_doc = {"c1": "d1", "c2": "d2", "c3": "d3"}
    assert document_ids_for(["c1", "c2", "c3"], chunk_doc, top_k=2) == ("d1", "d2")


def test_document_ids_for_skips_chunk_without_doc() -> None:
    chunk_doc = {"c1": "d1", "c3": "d3"}
    assert document_ids_for(["c1", "c2", "c3"], chunk_doc, top_k=3) == ("d1", "d3")


def test_document_ids_for_rejects_non_positive_top_k() -> None:
    with pytest.raises(ValueError, match="top_k"):
        document_ids_for(["c1"], {"c1": "d1"}, top_k=0)


# ---------------------------------------------------------------------------
# evaluate_documents
# ---------------------------------------------------------------------------


def test_evaluate_all_hits() -> None:
    results = [_result("p1/a"), _result("p1/b")]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=50, seed=1)
    assert report.scored == 2
    assert report.unresolved_cells == 0
    assert report.hit_at_k == 1.0
    assert report.recall_at_k == 1.0
    assert report.mrr == 1.0
    assert report.ndcg_at_k == 1.0


def test_evaluate_ndcg_perfect_order_is_one() -> None:
    """Единственный ожидаемый документ на первом месте — идеальный порядок."""
    result = _result("p1/a", retrieved=("p1:doc1", "p1:x"), rank=1)
    report = evaluate_documents([result], profile="R0", k=5, bootstrap=1, seed=0)
    assert report.ndcg_at_k == pytest.approx(1.0)


def test_evaluate_ndcg_expected_at_third_position_between_zero_and_one() -> None:
    """nDCG учитывает позицию: тот же найденный документ на 3-м месте дешевле первого."""
    result = _result("p1/a", retrieved=("p1:x", "p1:y", "p1:doc1"), rank=3)
    report = evaluate_documents([result], profile="R0", k=5, bootstrap=1, seed=0)
    assert report.hit_at_k == 1.0  # найден — hit не различает позицию
    assert 0.0 < report.ndcg_at_k < 1.0


def test_evaluate_ndcg_not_found_is_zero() -> None:
    result = _result("p1/a", retrieved=("p1:x", "p1:y"), rank=None)
    report = evaluate_documents([result], profile="R0", k=5, bootstrap=1, seed=0)
    assert report.ndcg_at_k == 0.0


def test_evaluate_ndcg_excludes_unresolved_cells() -> None:
    """``unresolved`` не входит в среднее nDCG: документа нет в корпусе — не провал поиска."""
    results = [
        _result("p1/a"),  # идеальный порядок → 1.0
        _result("p1/b", retrieved=("p1:x",), rank=None, unresolved=True),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    assert report.scored == 1
    assert report.unresolved_cells == 1
    assert report.ndcg_at_k == pytest.approx(1.0)


def test_evaluate_no_hits() -> None:
    results = [
        _result("p1/a", retrieved=("p1:x",), rank=None),
        _result("p1/b", retrieved=("p1:y",), rank=None),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=50, seed=1)
    assert report.hit_at_k == 0.0
    assert report.recall_at_k == 0.0
    assert report.mrr == 0.0


def test_evaluate_partial_with_multiple_expectations() -> None:
    results = [
        _result(
            "p1/a",
            expected=("p1:doc1", "p1:doc2"),
            retrieved=("p1:doc1", "p1:x"),
            rank=1,
        )
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=50, seed=1)
    assert report.hit_at_k == 1.0
    assert report.recall_at_k == 0.5
    assert report.mrr == 1.0


def test_evaluate_rank_is_first_expected_position() -> None:
    result = _result(
        "p1/a",
        expected=("p1:doc1", "p1:doc2"),
        retrieved=("p1:x", "p1:doc2", "p1:doc1"),
        rank=2,
    )
    report = evaluate_documents([result], profile="R0", k=5, bootstrap=50, seed=1)
    assert report.per_cell[0].rank == 2
    assert report.mrr == 0.5


def test_evaluate_unresolved_excluded_and_counted() -> None:
    results = [
        _result("p1/a"),
        _result("p1/b", retrieved=("p1:x",), rank=None, unresolved=True),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=50, seed=1)
    assert report.cells == 2
    assert report.scored == 1
    assert report.unresolved_cells == 1
    assert report.hit_at_k == 1.0
    assert report.recall_at_k == 1.0


def test_evaluate_all_unresolved_gives_honest_zero() -> None:
    results = [
        _result("p1/a", retrieved=(), rank=None, unresolved=True),
        _result("p1/b", retrieved=(), rank=None, unresolved=True),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=50, seed=1)
    assert report.scored == 0
    assert report.unresolved_cells == 2
    assert report.hit_at_k == 0.0
    assert report.recall_at_k == 0.0
    assert report.mrr == 0.0
    assert report.ndcg_at_k == 0.0
    assert report.hit_ci == (0.0, 0.0)
    assert report.recall_ci == (0.0, 0.0)


def test_evaluate_rejects_zero_bootstrap() -> None:
    with pytest.raises(ValueError, match="bootstrap"):
        evaluate_documents([_result()], profile="R0", k=5, bootstrap=0)


def test_evaluate_deterministic_for_same_seed() -> None:
    results = [
        _result("p1/a"),
        _result("p1/b", retrieved=("p1:x",), rank=None),
        _result("p1/c"),
        _result("p1/d", retrieved=("p1:y",), rank=None),
    ]
    first = evaluate_documents(results, profile="R0", k=5, bootstrap=200, seed=7)
    second = evaluate_documents(results, profile="R0", k=5, bootstrap=200, seed=7)
    assert first == second


def test_evaluate_bootstrap_one_gives_point_interval() -> None:
    results = [
        _result("p1/a"),
        _result("p1/b", retrieved=("p1:x",), rank=None),
        _result("p1/c"),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    assert report.hit_ci == (report.hit_at_k, report.hit_at_k)
    assert report.recall_ci == (report.recall_at_k, report.recall_at_k)


def test_evaluate_interval_contains_point_estimate() -> None:
    results = [
        _result("p1/a"),
        _result("p1/b", retrieved=("p1:x",), rank=None),
        _result("p1/c"),
        _result("p1/d", retrieved=("p1:y",), rank=None),
        _result("p1/e"),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=300, seed=3)
    low, high = report.hit_ci
    assert low <= report.hit_at_k <= high
    low, high = report.recall_ci
    assert low <= report.recall_at_k <= high


def test_evaluate_per_cell_preserves_input_order() -> None:
    results = [_result("p1/z"), _result("p1/a"), _result("p1/m")]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=50, seed=1)
    assert [cell.cell for cell in report.per_cell] == ["p1/z", "p1/a", "p1/m"]


# ---------------------------------------------------------------------------
# Правило знаменателя при наборе > 1 (answer-семейство)
# ---------------------------------------------------------------------------


def _answer_result(
    cell: str,
    *,
    expected: tuple[str, ...],
    resolved: tuple[str, ...],
    retrieved: tuple[str, ...],
) -> DocQueryResult:
    """Ячейка answer-семейства: ``unresolved`` — только при полном выпадении набора."""
    product_id, param_id = cell.split("/", 1)
    return DocQueryResult(
        cell=cell,
        product_id=product_id,
        param_id=param_id,
        expected_doc_ids=expected,
        retrieved_doc_ids=retrieved,
        rank=None,
        unresolved=not resolved,
        resolved_doc_ids=resolved,
    )


def test_recall_denominator_counts_only_documents_present_in_corpus() -> None:
    """Набор из 3, два документа исчезли из корпуса: recall — по одному присутствующему.

    Без правила знаменателя (деление на ``len(expected)``) ячейка навсегда упиралась бы в
    ``1/3``, и отсутствие документа в корпусе читалось бы как провал поиска.
    """
    results = [
        _answer_result(
            "p1/a",
            expected=("p1:doc1", "p1:gone1", "p1:gone2"),
            resolved=("p1:doc1",),
            retrieved=("p1:doc1", "p1:x"),
        )
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    assert report.scored == 1
    assert report.unresolved_cells == 0
    assert report.recall_at_k == 1.0  # 1 из 1 присутствующего, а не 1 из 3
    assert report.hit_at_k == 1.0
    assert report.unresolved_docs == 2


def test_cell_is_unresolved_only_when_no_member_is_in_corpus() -> None:
    results = [
        _answer_result(
            "p1/a",
            expected=("p1:gone1", "p1:gone2"),
            resolved=(),
            retrieved=("p1:x",),
        ),
        _answer_result("p1/b", expected=("p1:doc1",), resolved=("p1:doc1",), retrieved=("p1:doc1",)),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    assert report.unresolved_cells == 1
    assert report.scored == 1
    assert report.unresolved_docs == 2


def test_format_v1_recall_is_unchanged() -> None:
    """Формат 1: набор — один документ, ``resolved_doc_ids`` не объявлен.

    Разрешённая ячейка даёт ``1/1``, неразрешённая исключается — ровно как до правки;
    знаменатель «по разрешённым» не должен превращать исключённую ячейку в честный ноль.
    """
    results = [
        _result("p1/a"),  # ожидание есть в корпусе, найдено
        _result("p1/b", retrieved=("p1:x",), rank=None, unresolved=True),
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    assert report.scored == 1
    assert report.recall_at_k == 1.0
    assert report.unresolved_docs == 0  # поле не объявлено — не выдумывается


def test_empty_denominator_is_zero_not_skipped() -> None:
    """Ячейка, чьи ответы целиком выпали, даёт 0.0 и остаётся в знаменателе.

    ``unresolved=False`` здесь намеренно: так прибор считает чувствительность (остаток
    после вычитания ``retrieved`` пуст — это ответ «ноль», а не «ячейку не спрашивали»).
    """
    result = DocQueryResult(
        cell="p1/a",
        product_id="p1",
        param_id="a",
        expected_doc_ids=("p1:gone",),
        retrieved_doc_ids=(),
        rank=None,
        unresolved=False,
        resolved_doc_ids=(),
    )
    report = evaluate_documents([result], profile="R0", k=5, bootstrap=1, seed=0)
    assert report.scored == 1
    assert report.hit_at_k == 0.0
    assert report.recall_at_k == 0.0
    assert report.unresolved_docs == 1


# ---------------------------------------------------------------------------
# Ранг — вывод, а не вход
# ---------------------------------------------------------------------------


def test_first_hit_rank_is_the_first_expected_position() -> None:
    assert first_hit_rank(("p1:a", "p1:b"), ("p1:x", "p1:b", "p1:a")) == 2
    assert first_hit_rank(("p1:a",), ("p1:x",)) is None
    assert first_hit_rank(("p1:a",), ("p1:a",)) == 1


def test_inconsistent_rank_does_not_fake_a_hit() -> None:
    """``rank=1`` при пустой выдаче не делает ``hit=1``: ранг пересчитывается из выдачи.

    Иначе ``hit``/``MRR`` (от ``rank``) разошлись бы с ``recall``/``nDCG`` (от выдачи) в
    одном отчёте, и заметить это было бы нечем.
    """
    result = _result("p1/a", retrieved=(), rank=1)
    report = evaluate_documents([result], profile="R0", k=5, bootstrap=1, seed=0)
    assert report.hit_at_k == 0.0
    assert report.mrr == 0.0
    assert report.recall_at_k == 0.0
    assert report.ndcg_at_k == 0.0


def test_missing_rank_is_recovered_from_retrieval() -> None:
    """Обратный случай: ``rank=None`` при найденном документе — ``hit`` всё равно 1."""
    result = _result("p1/a", retrieved=("p1:x", "p1:doc1"), rank=None)
    report = evaluate_documents([result], profile="R0", k=5, bootstrap=1, seed=0)
    assert report.hit_at_k == 1.0
    assert report.mrr == 0.5
