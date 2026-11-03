"""Смещения куска в домене: инвариант ``CorpusChunk.char_start``/``char_end`` (L1b-2b).

Смещения — **провенанс**, а не ось нарезки: они говорят, где кусок лежит в
нормализованном тексте документа, и потому обязаны быть либо полной парой, либо
отсутствовать. Проверяются три ложные записи, каждая из которых сделала бы провенанс
бесполезным (по ней нельзя ни проверить цитату, ни пере-срезать кусок):

* задана только одна граница (диапазон без конца — не диапазон);
* пустой/перевёрнутый диапазон (``start >= end``);
* длина диапазона не равна длине ``text`` (смещения указывают не на этот текст).

Плюс граница, объявленная явно: ``None``/``None`` — законное состояние (абзацный чанкер
точным срезом не является), и краевые пробелы куска не срезаются моделью, иначе длина
диапазона перестала бы совпадать с длиной текста. Тесты нейтральны (§1.3).
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.domain import CorpusChunk


def _chunk(**overrides: object) -> CorpusChunk:
    base: dict[str, object] = {
        "chunk_id": "p1:dv#0",
        "doc_id": "p1:doc",
        "product_id": "p1",
        "text": "кусок",
    }
    base.update(overrides)
    return CorpusChunk(**base)  # type: ignore[arg-type]


def test_span_defaults_to_none_for_both_ends() -> None:
    """Без смещений кусок валиден: «не срез» — объявленное состояние, а не ошибка."""
    chunk = _chunk()

    assert chunk.char_start is None
    assert chunk.char_end is None


def test_span_pair_is_accepted() -> None:
    """Полная пара с длиной, равной ``len(text)``, — валидная провенанс-запись."""
    chunk = _chunk(char_start=10, char_end=15)

    assert (chunk.char_start, chunk.char_end) == (10, 15)
    assert chunk.char_end - chunk.char_start == len(chunk.text)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"char_start": 10}, id="только-начало"),
        pytest.param({"char_end": 15}, id="только-конец"),
    ],
)
def test_half_of_the_pair_is_rejected(overrides: dict[str, object]) -> None:
    """Одна граница без второй — не провенанс: диапазон задаётся парой."""
    with pytest.raises(ValidationError, match="только одна граница"):
        _chunk(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"char_start": 7, "char_end": 7}, id="пустой"),
        pytest.param({"char_start": 9, "char_end": 4}, id="перевёрнутый"),
        pytest.param({"char_start": -1, "char_end": 4}, id="отрицательный"),
    ],
)
def test_empty_or_inverted_span_is_rejected(overrides: dict[str, object]) -> None:
    """``0 <= start < end`` — единственная законная форма диапазона."""
    with pytest.raises(ValidationError, match="диапазон смещений"):
        _chunk(**overrides)


def test_span_length_must_equal_text_length() -> None:
    """Диапазон длиной не в ``text`` — смещения от другого куска (ложный провенанс)."""
    with pytest.raises(ValidationError, match="длина диапазона"):
        _chunk(text="кусок", char_start=0, char_end=4)


def test_text_edges_are_not_stripped() -> None:
    """Краевые переводы строки — часть среза: стрип сломал бы длину диапазона.

    Кусок markdown-чанкера кончается ``"\\n\\n"`` (P1: конкатенация равна входу), и обрезка
    сделала бы ``len(text)`` меньше ``char_end - char_start``.
    """
    chunk = _chunk(text="кусок\n\n", char_start=0, char_end=7)

    assert chunk.text == "кусок\n\n"
    assert chunk.char_end - chunk.char_start == len(chunk.text)