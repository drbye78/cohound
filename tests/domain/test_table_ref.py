"""Адрес куска внутри таблицы: инвариант ``TableRef`` и JSON-кодек (O39 фаза 2 плана ``26``).

Проверяется то, от чего зависит пригодность адреса, а не удобство чтения:

* **адрес обязан называть таблицу**: пустой ``table_id`` — не адрес, а пустое место, и
  чанк с ним читался бы как «из таблицы без имени»;
* **диапазон строк — пара и по порядку**: одна граница из двух, ноль, отрицательное
  число, ``bool`` или перевёрнутая пара — не диапазон. ``row_start``/``row_end`` —
  **1-based номера строк данных** (шапка не считается), поэтому ``0`` не бывает;
* **``None``/``None`` — законное состояние**: кусок таблицы содержит шапку и ни одной
  строки данных (короткая таблица, чья шапка ушла отдельным куском);
* **round-trip** через текст колонки ``corpus_chunks.table_ref`` и терпимость к порче:
  ``report`` обязан строиться и на повреждённой БД, поэтому любая непонятная запись
  читается как «адрес неизвестен» (``None``), а не как исключение (A3).

Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from core.domain.doc_tables import TableRef, table_ref_from_json, table_ref_to_json

# --------------------------------------------------------------------------- #
# Инвариант адреса
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("table_id", ["", " ", "\t\n"])
def test_empty_table_id_is_rejected(table_id: str) -> None:
    """Пустой или пробельный ``table_id`` — не адрес: кусок обязан называть таблицу."""
    with pytest.raises(ValueError, match="пустой table_id"):
        TableRef(table_id)


def test_single_bound_is_rejected() -> None:
    """Одна граница из двух — половина диапазона, а не диапазон."""
    with pytest.raises(ValueError, match="только одна граница"):
        TableRef("t0", row_start=1)
    with pytest.raises(ValueError, match="только одна граница"):
        TableRef("t0", row_end=2)


@pytest.mark.parametrize("start", [0, -1, True, "1", 1.5])
def test_start_below_one_or_not_int_is_rejected(start: object) -> None:
    """Строки данных нумеруются с единицы: ``0``/отрицательное/``bool``/не-``int`` — отказ."""
    with pytest.raises(ValueError, match="обязан быть >= 1"):
        TableRef("t0", row_start=start, row_end=2)  # type: ignore[arg-type]


@pytest.mark.parametrize("end", [0, -2, False, "2", 2.5])
def test_end_below_one_or_not_int_is_rejected(end: object) -> None:
    """Та же проверка для второй границы: ``bool`` — не число строк, хотя ``isinstance(int)``."""
    with pytest.raises(ValueError, match="обязан быть >= 1"):
        TableRef("t0", row_start=1, row_end=end)  # type: ignore[arg-type]


def test_reversed_range_is_rejected() -> None:
    """Перевёрнутый диапазон — не пустой диапазон, а ложь о том, какие строки в куске."""
    with pytest.raises(ValueError, match="перевёрнутый диапазон"):
        TableRef("t0", row_start=3, row_end=1)


def test_legal_forms() -> None:
    """Законные формы: только таблица; пара границ; диапазон из одной строки."""
    assert TableRef("t0") == TableRef(table_id="t0", row_start=None, row_end=None)
    assert (TableRef("t0", 1, 5).row_start, TableRef("t0", 1, 5).row_end) == (1, 5)
    # Одна строка данных — это диапазон [2; 2], а не «нет диапазона».
    single = TableRef("t0", 2, 2)
    assert (single.row_start, single.row_end) == (2, 2)


def test_frozen() -> None:
    """Адрес неизменяем: он часть записи о чанке, а не рабочая переменная."""
    ref = TableRef("t0", 1, 2)
    with pytest.raises(FrozenInstanceError):
        ref.row_start = 3  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# JSON-кодек колонки
# --------------------------------------------------------------------------- #


def test_none_stays_none() -> None:
    """«Адрес неизвестен» пишется как ``NULL``, а не как пустой объект.

    Пустой объект объявил бы кусок обычным текстом, а ``NULL`` честно говорит «не знаем»:
    это то же различие, что ``None``/``()`` у таблиц документа.
    """
    assert table_ref_to_json(None) is None
    assert table_ref_from_json(None) is None


@pytest.mark.parametrize(
    "ref",
    [TableRef("t0"), TableRef("t0", 1, 3), TableRef("таблица-1", 7, 7)],
)
def test_round_trip(ref: TableRef) -> None:
    """Round-trip через текст колонки: адрес читается обратно тем же объектом."""
    raw = table_ref_to_json(ref)
    assert isinstance(raw, str)
    assert table_ref_from_json(raw) == ref


def test_range_absent_survives_round_trip_as_absent() -> None:
    """«Таблица названа, строки — нет» переживает запись: ``null`` не превращается в ``0``."""
    raw = table_ref_to_json(TableRef("t0"))
    assert raw is not None and "null" in raw
    restored = table_ref_from_json(raw)
    assert restored == TableRef("t0")
    assert restored is not None and restored.row_start is None and restored.row_end is None


@pytest.mark.parametrize(
    "raw",
    [
        "",  # пустая колонка
        "не json",  # мусор
        "{",  # незакрытый объект
        "[]",  # не объект
        '"t0"',  # не объект
        "null",  # JSON-«ничего»
        "0",  # не объект
    ],
)
def test_corrupted_column_reads_as_unknown(raw: str) -> None:
    """Порча колонки — «адрес неизвестен», а не исключение: отчёт строится и на битой БД."""
    assert table_ref_from_json(raw) is None


@pytest.mark.parametrize("raw", [b'{"table_id":"t0"}', 42, None, {"table_id": "t0"}])
def test_non_text_column_reads_as_unknown(raw: object) -> None:
    """Не-``str`` на входе — та же порча: кодек не приводит типы и не гадает."""
    assert table_ref_from_json(raw) is None


def test_missing_table_id_reads_as_unknown() -> None:
    """Объект без ``table_id`` — не адрес: назвать таблицу нечем."""
    assert table_ref_from_json('{"row_start":1,"row_end":2}') is None
    assert table_ref_from_json('{"table_id":"","row_start":1,"row_end":2}') is None


def test_single_bound_in_column_reads_as_unknown() -> None:
    """Половина диапазона в колонке — не диапазон: тот же инвариант, что у конструктора."""
    assert table_ref_from_json('{"table_id":"t0","row_start":1}') is None
    assert table_ref_from_json('{"table_id":"t0","row_end":2}') is None


def test_reversed_range_in_column_reads_as_unknown() -> None:
    """Перевёрнутый диапазон из БД отвергается конструктором и читается как «неизвестно»."""
    assert table_ref_from_json('{"table_id":"t0","row_start":3,"row_end":1}') is None


def test_non_numeric_bounds_in_column_read_as_unknown() -> None:
    """``bool``/строка/дробь на месте границы — порча записи, а не «единица»."""
    assert table_ref_from_json('{"table_id":"t0","row_start":true,"row_end":true}') is None
    assert table_ref_from_json('{"table_id":"t0","row_start":"1","row_end":"2"}') is None
    assert table_ref_from_json('{"table_id":"t0","row_start":1.5,"row_end":2}') is None
    assert table_ref_from_json('{"table_id":"t0","row_start":0,"row_end":2}') is None


def test_non_text_table_id_is_coerced_to_text() -> None:
    """Числовой ``table_id`` кодек приводит к тексту, а не отвергает.

    Так же ведёт себя :func:`core.domain.doc_tables.tables_from_json`: значение приходит от
    чужого писателя, а адрес — это текст. Отвергается только **пустое** значение (см.
    ``test_missing_table_id_reads_as_unknown``): у пустого адреса нет предмета.
    """
    restored = table_ref_from_json('{"table_id":7,"row_start":1,"row_end":2}')
    assert restored == TableRef("7", 1, 2)
