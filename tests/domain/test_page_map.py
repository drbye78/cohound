"""Карта страниц: правило вывода номера страницы и носитель в БД (O36, план 26).

Проверяется ровно то, от чего зависит номер страницы цитаты:

* **правило** (:func:`page_number_for`) — попадание внутрь отрезка, разрыв между
  страницами (уходит следующей), выход за карту в обе стороны, пустая страница
  (``start == end``), отсутствие карты и отсутствие смещения;
* **инвариант** (:func:`check_page_map`) — тот же, что у ``ExtractedDoc``: проверка живёт
  в домене одна, и её сообщения обязаны называть носитель;
* **носитель в БД** (:func:`page_map_to_json`/:func:`page_map_from_json`) — порча колонки
  читается как «карты нет», а не как падение отчёта.

Тесты нейтральны (§1.3) и не трогают диск.
"""

from __future__ import annotations

import json

import pytest

from core.domain.page_map import (
    check_page_map,
    page_map_from_json,
    page_map_to_json,
    page_number_for,
)

#: Карта из трёх страниц с пустой второй (как у скана: страница прочитана, текста нет).
#: ``text = "первая\\n\\n\\n\\nтретья"``: 0..5 — страница 1, 6..7 — разрыв, 8 — пустая
#: страница 2, 9 — разрыв, 10..15 — страница 3.
THREE_PAGES = ((1, 0, 6), (2, 8, 8), (3, 10, 16))


# ---------------------------------------------------------------------------
# Правило: какой странице принадлежит смещение
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("char_start", "expected"),
    [
        (0, 1),  # начало первой страницы
        (5, 1),  # последний символ первой страницы
        (6, 2),  # разрыв между 1 и пустой 2 — следующая страница
        (7, 2),  # тот же разрыв
        (8, 3),  # позиция пустой страницы — уходит следующей (правило пустого отрезка)
        (9, 3),  # разрыв после пустой страницы
        (10, 3),  # начало третьей
        (15, 3),  # последний символ третьей
        (16, 3),  # за последним отрезком — последняя страница
        (999, 3),
        (-5, 1),  # до первого отрезка — первая страница
    ],
)
def test_page_number_follows_the_gap_rule(char_start: int, expected: int) -> None:
    """Разрыв принадлежит следующей странице; вне карты — первая/последняя."""
    assert page_number_for(char_start, THREE_PAGES) == expected


def test_page_number_of_plain_tiling() -> None:
    """Без пустых страниц и без разрывов — обычное попадание в отрезок."""
    page_map = ((1, 0, 10), (2, 10, 20), (3, 20, 30))
    assert [page_number_for(offset, page_map) for offset in (0, 9, 10, 19, 20, 29, 30)] == [
        1,
        1,
        2,
        2,
        3,
        3,
        3,
    ]


def test_page_number_is_none_without_a_map_or_an_offset() -> None:
    """``None`` на любом входе — «назвать нельзя», а не «страница 1»."""
    assert page_number_for(0, None) is None
    assert page_number_for(None, THREE_PAGES) is None
    assert page_number_for(None, None) is None


def test_page_number_of_empty_map_is_none() -> None:
    """Пустая карта читается как отсутствующая: «ноль страниц» — не утверждение."""
    assert page_number_for(0, ()) is None


def test_empty_first_segment_hands_the_offset_to_the_next_page() -> None:
    """Смещение на позиции пустой **первой** страницы уходит второй (следующей нет — не 0)."""
    page_map = ((1, 0, 0), (2, 2, 6))
    assert page_number_for(0, page_map) == 2
    assert page_number_for(1, page_map) == 2


def test_gap_before_an_empty_last_page_belongs_to_that_page() -> None:
    """Разрыв перед пустой последней страницей — её страница; её позиция — тоже она."""
    page_map = ((1, 0, 6), (2, 8, 8))
    # Смещение 6..7 — разрыв: следующая страница (пустая вторая) его получает.
    assert page_number_for(6, page_map) == 2
    assert page_number_for(7, page_map) == 2
    # Смещение на позиции пустой страницы и за картой — последняя страница.
    assert page_number_for(8, page_map) == 2


# ---------------------------------------------------------------------------
# Инвариант карты
# ---------------------------------------------------------------------------


def test_valid_map_passes() -> None:
    """Корректная карта проходит обе формы проверки (с длиной текста и без)."""
    check_page_map(THREE_PAGES, 16)
    check_page_map(THREE_PAGES, None)


@pytest.mark.parametrize(
    "entry",
    [
        (1, 0),  # не тройка
        (1, 0, 1, 2),  # четвёрка
        ("1", 0, 1),  # номер страницы не int
        (1, True, 1),  # bool вместо int
        (0, 0, 1),  # нумерация 1-based
        (1, -1, 1),  # отрицательное начало
        (1, 5, 4),  # вывернутый интервал
    ],
)
def test_malformed_entry_is_rejected(entry: object) -> None:
    """Ложная карта отвергается: по ней номер страницы был бы догадкой."""
    with pytest.raises(ValueError):
        check_page_map((entry,), None)  # type: ignore[arg-type]


def test_out_of_text_and_unsorted_maps_are_rejected() -> None:
    """Выход за текст, обратный порядок и пересечение — нарушения, а не «странности»."""
    with pytest.raises(ValueError, match="выходит за текст"):
        check_page_map(((1, 0, 10),), 9)
    with pytest.raises(ValueError, match="не отсортирована или сегменты пересекаются"):
        check_page_map(((2, 0, 5), (1, 5, 9)), None)
    with pytest.raises(ValueError, match="не отсортирована или сегменты пересекаются"):
        check_page_map(((1, 0, 6), (2, 5, 9)), None)


def test_message_names_the_carrier() -> None:
    """Один валидатор на два носителя: сообщение обязано называть нарушивший.

    ``ExtractedDoc`` — прежнее имя (сообщения не менялись), ``corpus_docs`` — строка
    корпуса, у которой текста нет и граница берётся из ``extracted_chars``.
    """
    with pytest.raises(ValueError, match=r"^ExtractedDoc:"):
        check_page_map(((0, 0, 1),), None)
    with pytest.raises(ValueError, match=r"^corpus_docs:"):
        check_page_map(((0, 0, 1),), None, label="corpus_docs")


# ---------------------------------------------------------------------------
# Носитель в БД: JSON-колонка
# ---------------------------------------------------------------------------


def test_json_roundtrip() -> None:
    """Карта переживает запись и чтение без потерь (и без лишних пробелов)."""
    raw = page_map_to_json(THREE_PAGES)
    assert raw == "[[1,0,6],[2,8,8],[3,10,16]]"
    assert page_map_from_json(raw) == THREE_PAGES


def test_absent_map_is_null_not_an_empty_string() -> None:
    """Отсутствие карты — ``NULL`` в колонке: ``''`` читалось бы как «карта есть, пуста»."""
    assert page_map_to_json(None) is None
    assert page_map_to_json(()) is None
    assert page_map_from_json(None) is None
    assert page_map_from_json("") is None


@pytest.mark.parametrize(
    "raw",
    [
        "{not valid json",  # не JSON
        "[]",  # пустой список
        "null",  # JSON-null
        '{"page_map": [[1, 0, 6]]}',  # не список
        "[[1, 0]]",  # не тройка
        "[[1, 0, 6, 7]]",  # четвёрка
        '[["1", 0, 6]]',  # строка вместо int
        "[[1, true, 6]]",  # bool вместо int
        "[[0, 0, 6]]",  # нумерация 1-based
        "[[1, 6, 0]]",  # вывернутый интервал
        "[[2, 0, 6], [1, 6, 9]]",  # обратный порядок
        "[[1, 0, 6], [2, 5, 9]]",  # пересечение
        b"[[1,0,6]]",  # байты, а не текст
        42,  # не строка и не None
    ],
)
def test_corrupt_column_reads_as_no_map(raw: object) -> None:
    """Порча колонки — «карты нет», а не исключение: отчёт строится и на битой БД."""
    assert page_map_from_json(raw) is None


def test_json_of_a_corrupt_map_is_not_hidden_by_the_reader() -> None:
    """Граница различается явно: пишем что дали, читаем как «нет» — но не молча портим."""
    corrupt = json.dumps([[1, 5, 4]])
    assert page_map_from_json(corrupt) is None
