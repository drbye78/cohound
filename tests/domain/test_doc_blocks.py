"""Структурные блоки документа: правило адреса и носитель в БД (O36 фаза 2, план 26).

Проверяется ровно то, от чего зависит адрес блока у цитаты:

* **правило** (:func:`block_id_for`) — попадание внутрь отрезка, разрыв между блоками
  (уходит следующему), выход за список в обе стороны, пустой блок (``start == end``),
  отсутствие блоков и отсутствие смещения. Правило обязано совпадать с
  :func:`core.domain.page_map.page_number_for` — это один и тот же вопрос, заданный дважды;
* **инвариант** (:func:`check_blocks`) — тот же, что у ``ExtractedDoc``: проверка живёт в
  домене одна, и её сообщения обязаны называть носитель;
* **носитель в БД** (:func:`blocks_to_json`/:func:`blocks_from_json`) — ``None`` (читатель
  не даёт) и ``()`` (смотрел и не дал) различимы, порча колонки читается как «блоков нет»,
  а не как падение отчёта.

Тесты нейтральны (§1.3) и не трогают диск.
"""

from __future__ import annotations

import pytest

from core.domain.doc_blocks import (
    BLOCK_KIND_HEADING,
    BLOCK_KIND_PAGE,
    BLOCK_KIND_PARAGRAPH,
    TEXT_SOURCE_READ,
    TEXT_SOURCE_RECONSTRUCTED,
    ExtractedBlock,
    block_id_for,
    blocks_from_json,
    blocks_to_json,
    check_blocks,
)
from core.domain.page_map import page_number_for

#: Три страницы с пустой второй — тот же случай, что в ``test_page_map``: ``text`` длиной
#: 16, страница 1 — 0..6, разрыв 6..8, пустая страница 2 — 8, разрыв, страница 3 — 10..16.
THREE_PAGES = (
    ExtractedBlock(
        block_id="b0",
        kind=BLOCK_KIND_PAGE,
        char_start=0,
        char_end=6,
        order=0,
        text_source=TEXT_SOURCE_READ,
        page_no=1,
    ),
    ExtractedBlock(
        block_id="b1",
        kind=BLOCK_KIND_PAGE,
        char_start=8,
        char_end=8,
        order=1,
        text_source=TEXT_SOURCE_READ,
        page_no=2,
    ),
    ExtractedBlock(
        block_id="b2",
        kind=BLOCK_KIND_PAGE,
        char_start=10,
        char_end=16,
        order=2,
        text_source=TEXT_SOURCE_READ,
        page_no=3,
    ),
)


def _block(index: int, start: int, end: int, **overrides: object) -> ExtractedBlock:
    """Блок с адресом по позиции; ``overrides`` — точечная порча одного поля."""
    fields: dict[str, object] = {
        "block_id": f"b{index}",
        "kind": BLOCK_KIND_PAGE,
        "char_start": start,
        "char_end": end,
        "order": index,
        "text_source": TEXT_SOURCE_READ,
        "page_no": index + 1,
    }
    fields.update(overrides)
    return ExtractedBlock(**fields)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Правило: какому блоку принадлежит смещение
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("char_start", "expected"),
    [
        (0, "b0"),  # начало первого блока
        (5, "b0"),  # последний символ первого
        (6, "b1"),  # разрыв между 1 и пустой 2 — следующий блок
        (7, "b1"),  # тот же разрыв
        (8, "b2"),  # позиция пустого блока — уходит следующему
        (9, "b2"),  # разрыв после пустого блока
        (10, "b2"),  # начало третьего
        (15, "b2"),  # последний символ третьего
        (16, "b2"),  # за последним блоком — последний
        (999, "b2"),
        (-5, "b0"),  # до первого блока — первый
    ],
)
def test_block_follows_the_gap_rule(char_start: int, expected: str) -> None:
    """Разрыв принадлежит следующему блоку; вне списка — первый/последний."""
    assert block_id_for(char_start, THREE_PAGES) == expected


def test_block_rule_matches_the_page_rule() -> None:
    """Правило блока — **то же**, что правило страницы: один вопрос, один ответ.

    Сравнение идёт по всем смещениям вокруг и внутри отрезков: две реализации одного
    правила, разошедшиеся на границе, дали бы цитате страницу и блок из разных мест.
    """
    page_map = ((1, 0, 6), (2, 8, 8), (3, 10, 16))
    for offset in range(-2, 19):
        page = page_number_for(offset, page_map)
        block = block_id_for(offset, THREE_PAGES)
        assert (page, block) == (
            (None, None) if page is None else (page, f"b{page - 1}")
        ), f"смещение {offset}: страница {page}, блок {block}"


def test_block_is_none_without_blocks_or_an_offset() -> None:
    """``None`` на любом входе — «назвать нельзя», а не «первый блок»."""
    assert block_id_for(0, None) is None
    assert block_id_for(None, THREE_PAGES) is None
    assert block_id_for(None, None) is None


def test_block_of_empty_list_is_none() -> None:
    """Пустой список читается как отсутствующий: «блоков не названо» — не «ноль блоков»."""
    assert block_id_for(0, ()) is None


def test_nested_blocks_name_the_outer_one() -> None:
    """Вложенность разрешается порядком чтения: смещение называет **родителя**.

    Родитель стоит раньше ребёнка (инвариант), поэтому первое попадание в отрезок —
    самый общий блок, который смещение содержит. Это детерминированно и не зависит от
    того, насколько глубоко вложен фрагмент.
    """
    blocks = (
        _block(0, 0, 20, kind=BLOCK_KIND_PAGE, page_no=1),
        _block(1, 0, 8, kind=BLOCK_KIND_HEADING, page_no=1, parent_id="b0"),
        _block(2, 8, 20, kind=BLOCK_KIND_PARAGRAPH, page_no=1, parent_id="b0"),
    )
    check_blocks(blocks, 20)
    assert block_id_for(0, blocks) == "b0"
    assert block_id_for(19, blocks) == "b0"


# ---------------------------------------------------------------------------
# Инвариант списка блоков
# ---------------------------------------------------------------------------


def test_valid_blocks_pass() -> None:
    """Корректный список проходит обе формы проверки (с длиной текста и без)."""
    check_blocks(THREE_PAGES, 16)
    check_blocks(THREE_PAGES, None)
    check_blocks((), 0)


@pytest.mark.parametrize(
    ("block", "message"),
    [
        # Адрес обязан совпадать с позицией: иначе «b3» перестаёт значить «четвёртый».
        (_block(0, 0, 5, block_id="b1"), "не совпадает с позицией"),
        (_block(0, 0, 5, kind="section"), "неизвестный вид блока"),
        (_block(0, 0, 5, text_source="guessed"), "неизвестное происхождение текста"),
        (_block(0, 0, 5, page_no=0), "page_no обязан быть >= 1"),
        (_block(0, -1, 5), "отрицательным или вывернутым"),
        (_block(0, 6, 5), "отрицательным или вывернутым"),
        (_block(0, 0, 5, order=-1), "order отрицателен"),
    ],
)
def test_malformed_block_is_rejected(block: ExtractedBlock, message: str) -> None:
    """Ложная запись отвергается: по ней цитата получила бы адрес, которого не было."""
    with pytest.raises(ValueError, match=message):
        check_blocks((block,), None)


def test_out_of_text_block_is_rejected() -> None:
    """Отрезок за текстом — нарушение, а не «странность»."""
    with pytest.raises(ValueError, match="выходит за текст"):
        check_blocks((_block(0, 0, 10),), 9)


def test_duplicate_address_is_rejected() -> None:
    """Дубль адреса невозможен по построению, но проверяется отдельно: список — данные."""
    with pytest.raises(ValueError, match="не совпадает с позицией"):
        check_blocks((_block(0, 0, 5), _block(0, 5, 9)), None)


def test_order_must_grow() -> None:
    """Порядок чтения строго возрастает: равный и обратный — нарушения."""
    with pytest.raises(ValueError, match="order не возрастает"):
        check_blocks((_block(0, 0, 5, order=1), _block(1, 5, 9, order=1)), None)
    with pytest.raises(ValueError, match="order не возрастает"):
        check_blocks((_block(0, 0, 5, order=2), _block(1, 5, 9, order=1)), None)


def test_dangling_or_late_parent_is_rejected() -> None:
    """Родитель обязан существовать и идти раньше ребёнка."""
    with pytest.raises(ValueError, match="parent_id"):
        check_blocks((_block(0, 0, 5, parent_id="b9"),), None)
    with pytest.raises(ValueError, match="parent_id"):
        check_blocks(
            (
                _block(0, 0, 5, parent_id="b1"),
                _block(1, 0, 9, kind=BLOCK_KIND_HEADING),
            ),
            None,
        )


def test_bbox_is_either_absent_or_a_finite_quadruple() -> None:
    """Рамка — ``None`` или четвёрка конечных чисел: NaN не позиция."""
    check_blocks((_block(0, 0, 5, bbox=(0.0, 1.0, 2.0, 3.0)),), None)
    with pytest.raises(ValueError, match="bbox"):
        check_blocks((_block(0, 0, 5, bbox=(0.0, 1.0, 2.0)),), None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="bbox"):
        check_blocks((_block(0, 0, 5, bbox=(0.0, float("nan"), 2.0, 3.0)),), None)


def test_message_names_the_carrier() -> None:
    """Один валидатор на два носителя: сообщение обязано называть нарушивший."""
    with pytest.raises(ValueError, match=r"^ExtractedDoc:"):
        check_blocks((_block(0, 0, 5, kind="section"),), None)
    with pytest.raises(ValueError, match=r"^corpus_docs:"):
        check_blocks((_block(0, 0, 5, kind="section"),), None, label="corpus_docs")


# ---------------------------------------------------------------------------
# Носитель в БД: JSON-колонка
# ---------------------------------------------------------------------------


def test_json_roundtrip() -> None:
    """Блоки переживают запись и чтение без потерь — включая рамку и родителя."""
    blocks = (
        _block(0, 0, 20, kind=BLOCK_KIND_PAGE, bbox=(0.0, 1.5, 2.0, 3.5)),
        _block(
            1,
            0,
            8,
            kind=BLOCK_KIND_HEADING,
            text_source=TEXT_SOURCE_RECONSTRUCTED,
            parent_id="b0",
        ),
    )
    raw = blocks_to_json(blocks)
    assert raw is not None
    assert blocks_from_json(raw) == blocks


def test_absent_blocks_are_null_and_empty_are_not() -> None:
    """``None`` («не даёт») — ``NULL``, ``()`` («смотрел и не дал») — ``[]``: разные факты."""
    assert blocks_to_json(None) is None
    assert blocks_to_json(()) == "[]"
    assert blocks_from_json(None) is None
    assert blocks_from_json("") is None
    assert blocks_from_json("[]") == ()


@pytest.mark.parametrize(
    "raw",
    [
        "{not valid json",  # не JSON
        "null",  # JSON-null
        '{"blocks": []}',  # не список
        "[1, 2]",  # элементы — не объекты
        '[{"block_id": "b0"}]',  # нет обязательных полей
        '[{"block_id": "b0", "kind": "page", "char_start": 0, "char_end": 5,'
        ' "order": 0, "text_source": "read", "bbox": [1, 2]}]',  # рамка не четвёрка
        '[{"block_id": "b0", "kind": "page", "char_start": 5, "char_end": 0,'
        ' "order": 0, "text_source": "read"}]',  # вывернутый отрезок
        '[{"block_id": "b1", "kind": "page", "char_start": 0, "char_end": 5,'
        ' "order": 0, "text_source": "read"}]',  # адрес не совпадает с позицией
        '[{"block_id": "b0", "kind": "page", "char_start": 0, "char_end": 5,'
        ' "order": 0, "text_source": "guessed"}]',  # неизвестное происхождение
        b'[{"block_id": "b0"}]',  # байты, а не текст
        42,  # не строка и не None
    ],
)
def test_corrupt_column_reads_as_no_blocks(raw: object) -> None:
    """Порча колонки — «блоков нет», а не исключение: отчёт строится и на битой БД."""
    assert blocks_from_json(raw) is None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
