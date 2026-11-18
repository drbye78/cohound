"""Пины разведки по корпусу (``core.pipeline.corpus_search``), без сети и без прогона.

Модуль держит два правила, которые нельзя переизобрести на стороне команды, поэтому
проверяются именно они, а не форма вывода:

* **слияние запросов — ``max``**, как объявлено осью ``cross_query_fusion`` в Ш6: иначе
  ``hound search --param`` показывал бы не то, что увидит оценщик;
* **ответ разведки отделён от вердикта по типу**: у :class:`SearchAnswer` нет ни
  ``status``, ни ``citation_quote``, и его нельзя передать туда, где ждут
  ``LLMCellOutput`` (I1/I22).
"""

from __future__ import annotations

import pytest

from core.domain import LLMCellOutput
from core.pipeline.corpus_search import (
    Hit,
    SearchAnswer,
    build_answer_prompt,
    merge_hits,
    render_hits,
)
from core.ports.vector_index import RetrievedChunk


def _chunk(chunk_id: str, score: float, *, product: str = "p1", text: str = "текст") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        product_id=product,
        text=text,
        content_hash="h" * 16,
        heading_path="раздел",
        build_id="pb_1",
        score=score,
    )


def test_merge_keeps_the_best_score_and_remembers_every_query() -> None:
    """Тот же чанк из двух запросов — один хит с максимальным счётом и двумя номерами."""
    merged = merge_hits([[_chunk("c1", 1.5), _chunk("c2", 0.5)], [_chunk("c1", 2.5)]])
    assert [hit.chunk_id for hit in merged] == ["c1", "c2"]
    assert merged[0].score == 2.5
    assert merged[0].queries == (0, 1)
    assert merged[1].queries == (0,)


def test_merge_is_deterministic_on_equal_scores() -> None:
    """Равные счёты разрешаются ``chunk_id``: две одинаковые команды дают один список."""
    merged = merge_hits([[_chunk("b", 1.0), _chunk("a", 1.0)]])
    assert [hit.chunk_id for hit in merged] == ["a", "b"]
    assert merge_hits([[]]) == []


def test_merge_does_not_lose_the_chunk_body() -> None:
    merged = merge_hits([[_chunk("c1", 1.0, text="тело куска")]])
    assert merged[0].text == "тело куска"
    assert merged[0].build_id == "pb_1"


def test_answer_prompt_numbers_fragments_and_marks_truncation() -> None:
    hits = [
        Hit("c1", "p1", "а" * 50, "раздел", "", "h", "pb_1", 1.0),
        Hit("c2", "p2", "короткий", "", "", "h", "pb_1", 0.5),
    ]
    prompt = build_answer_prompt("вопрос?", hits, max_chars=10)
    assert prompt.count("[1]") == 1 and prompt.count("[2]") == 1
    assert "…[обрезано]" in prompt
    assert "короткий" in prompt
    assert "вопрос?" in prompt
    # Обрезка названа, а не сделана молча (A3).
    assert "а" * 50 not in prompt


def test_answer_prompt_refuses_to_build_without_fragments() -> None:
    with pytest.raises(ValueError, match="без фрагментов"):
        build_answer_prompt("вопрос?", [])


def test_search_answer_is_not_a_verdict() -> None:
    """У ответа разведки нет полей вердикта — по типу, а не по соглашению."""
    assert set(SearchAnswer.model_fields) == {"answer", "citations", "insufficient"}
    assert not set(SearchAnswer.model_fields) & set(LLMCellOutput.model_fields)
    with pytest.raises(ValueError):
        SearchAnswer.model_validate({"answer": ""})
    with pytest.raises(ValueError):
        SearchAnswer.model_validate({"answer": "ок", "status": "✅"})


def test_render_hits_shows_url_only_when_known() -> None:
    hits = [Hit("c1", "p1", "тело", "раздел", "", "h", "pb_1", 1.0)]
    with_url = render_hits(hits, urls={"c1": "https://example.org/x"})
    assert "https://example.org/x" in with_url
    assert "c1" in with_url
    without = render_hits(hits)
    assert "https://" not in without
