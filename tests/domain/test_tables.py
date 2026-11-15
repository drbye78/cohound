"""Таблица как объект документа: инвариант формы и JSON-кодек (O39 фаза 1 плана ``26``).

Проверяется то, от чего зависит пригодность факта, а не удобство чтения:

* **рваная строка — отказ модели**, а не тихая обрезка: обрезанная строка объявила бы
  таблицу, которой читатель не видел, и число в отчёте было бы ложью;
* **``None`` и ``()`` — разные факты**: «читатель таблиц не даёт» против «искал и не
  нашёл». Свести их к одному значило бы публиковать «таблиц нет» там, где их никто не
  искал (A3);
* **страница 1-based либо ``None``**: ``0`` не бывает — «страница 0» это выдумка;
* **round-trip** через текст колонки ``corpus_docs.tables`` и терпимость к порче:
  ``report`` обязан строиться и на повреждённой БД.
"""

from __future__ import annotations

import pytest

from core.domain.doc_tables import (
    ExtractedTable,
    tables_from_json,
    tables_to_json,
)


def _table(**overrides: object) -> ExtractedTable:
    payload: dict[str, object] = {
        "table_id": "t0",
        "page_no": 3,
        "caption": "Параметры",
        "header": ("имя", "значение"),
        "rows": (("work_mem", "64MB"), ("shared_buffers", "1GB")),
    }
    payload.update(overrides)
    return ExtractedTable(**payload)  # type: ignore[arg-type]


def test_ragged_row_is_rejected() -> None:
    """Строка уже шапки — отказ модели (а не обрезка/дополнение)."""
    with pytest.raises(ValueError, match="ширина строки"):
        _table(rows=(("work_mem",),))


def test_ragged_row_without_header_is_rejected_against_first_row() -> None:
    """Без шапки ширину задаёт первая строка: расхождение — тот же отказ."""
    with pytest.raises(ValueError, match="первой строки"):
        _table(header=(), rows=(("a", "b"), ("c",)))


def test_page_zero_is_rejected() -> None:
    """Страница 0 не бывает: нумерация 1-based, отсутствие — ``None``."""
    with pytest.raises(ValueError, match="page_no"):
        _table(page_no=0)


def test_missing_page_and_header_are_legal() -> None:
    """Страница неизвестна, шапки нет — законная таблица, а не ошибка."""
    table = ExtractedTable(
        table_id="t1", page_no=None, header=(), rows=(("a",), ("b",))
    )
    assert table.page_no is None
    assert table.header == ()


def test_round_trip_keeps_none_and_empty_distinct() -> None:
    """Кодек сохраняет различие: ``None`` → NULL, ``()`` → ``[]``."""
    table = _table()
    assert tables_to_json(None) is None
    assert tables_to_json(()) == "[]"
    assert tables_from_json(None) is None
    assert tables_from_json("[]") == ()
    assert tables_from_json(tables_to_json((table,))) == (table,)


def test_corrupted_column_reads_as_not_measured() -> None:
    """Порча колонки читается как «не даёт» (``None``), а не как исключение."""
    assert tables_from_json("") is None
    assert tables_from_json("не json") is None
    assert tables_from_json('{"table_id": "t0"}') is None
    # Рваная строка внутри записи — та же порча: инвариант держит конструктор.
    broken = '[{"table_id":"t0","header":["a","b"],"rows":[["1"]]}]'
    assert tables_from_json(broken) is None
