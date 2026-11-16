"""Адрес куска внутри таблицы: ``Chunk.table_ref`` у markdown-чанкера (O39 фаза 2, ``policy-6``).

Проверяется **адрес**, а не текст: нарезка от привязки не зависит, и это главный инвариант
фазы (см. ``test_address_does_not_change_text_hash_or_numbering``). Всё остальное здесь —
про то, какие строки таблицы кусок действительно содержит:

* **правило диапазона строк.** Pipe-таблица печатает строку-шапку **всегда**, даже когда у
  таблицы-объекта шапки нет (тогда в неё попадает первая строка данных). Поэтому «строк в
  тексте на одну больше, чем в объекте» — это шапка, «столько же» — шапки в тексте нет.
  Догадка здесь стоит сдвига на единицу у 75 из 181 таблиц (замер 16.11.2026), поэтому
  ловушка «шапка без шапки» проверяется отдельно;
* **пересечение, а не вложенность.** Кусок начинается с заголовка секции и кончается на
  границе строки, поэтому «целиком внутри таблицы» почти никогда не выполняется, а адрес
  нужен именно такому куску;
* **сопоставление объект↔блок — по порядку и только при равенстве числа.** «Сдвинулось на
  одну» и «другая таблица» неразличимы, и привязать кусок к чужой таблице хуже, чем ни к
  какой;
* **кусок, пересекающий две таблицы, адреса не получает**: «t0 и t3» — перечисление, а не
  адрес.

Таблицы задаются объектами домена, без docling: тест проверяет правило чанкера, а не
библиотеку. Нейтрален (§1.3).
"""

from __future__ import annotations

import itertools

from core.adapters.chunker_markdown import MarkdownChunker, MarkdownChunkerPort
from core.domain.doc_tables import ExtractedTable, TableRef
from core.ports.types import ExtractedDoc


def _chunks(
    md: str,
    tables: tuple[ExtractedTable, ...] | None = None,
    **kwargs: int,
):
    """Куски документа; порог фрагмента снят явно — тест про адрес, а не про размер."""
    kwargs.setdefault("min_chunk_size", 1)
    return MarkdownChunker(**kwargs).chunk_text("doc", md, tables)


def _pipe(header: tuple[str, ...], rows: tuple[tuple[str, ...], ...]) -> str:
    """Pipe-таблица так, как её печатает docling: строка шапки, разделитель, строки данных."""
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join("---" for _ in header) + " |"]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Правило диапазона строк
# --------------------------------------------------------------------------- #


def test_table_with_header_gives_data_row_range() -> None:
    """Шапка в тексте есть: первая строка — шапка, значит данные начинаются со строки 1."""
    rows = (("work_mem", "64MB"), ("shared_buffers", "1GB"), ("max_conn", "100"))
    table = ExtractedTable(table_id="t0", header=("имя", "значение"), rows=rows)
    md = "# Секция\n\n" + _pipe(("имя", "значение"), rows)

    chunks = _chunks(md, (table,))

    assert len(chunks) == 1
    assert chunks[0].table_ref == TableRef("t0", 1, 3)


def test_table_without_header_is_not_shifted_by_one() -> None:
    """У таблицы без шапки строк в тексте столько же, сколько в объекте — сдвига быть не может.

    Ловушка: «первая строка всегда шапка» дало бы здесь ``[2; 12]`` вместо ``[1; 12]``,
    то есть адрес уехал бы на единицу — ровно у тех 75 из 181 таблиц, у которых шапки нет.
    Проверка идёт по границе куска: первый кусок обязан начинаться со строки **1**.
    """
    rows = tuple((f"param_{i}", f"value_{i}_with_some_length") for i in range(1, 13))
    table = ExtractedTable(table_id="t0", header=(), rows=rows)
    # Первая строка данных стоит на месте шапки (так печатает docling), остальные — ниже.
    md = "# Таблица\n\n" + _pipe(rows[0], rows[1:])

    chunks = _chunks(md, (table,), chunk_size=300)

    assert len(chunks) > 1, "таблица обязана разрезаться, иначе сдвиг не виден"
    assert chunks[0].table_ref == TableRef("t0", 1, 7)
    assert chunks[-1].table_ref is not None
    assert chunks[-1].table_ref.row_end == 12


def test_header_rows_marked_by_dialect_are_dropped() -> None:
    """У grid-таблицы шапка помечена разметкой: она отбрасывается, сверка — по остатку.

    Здесь правило «на одну больше» не работает вовсе (grid печатает шапку внутри блока, а
    не отдельной pipe-строкой), поэтому единственный признак — ``row.info["header"]``.
    """
    grid = (
        "+------+-------+\n"
        "| имя  | знач  |\n"
        "+======+=======+\n"
        "| a    | 1     |\n"
        "+------+-------+\n"
        "| b    | 2     |\n"
        "+------+-------+\n"
    )
    table = ExtractedTable(table_id="t0", header=("имя", "знач"), rows=(("a", "1"), ("b", "2")))

    chunks = _chunks(grid, (table,), profile="pandoc")

    assert len(chunks) == 1
    assert chunks[0].table_ref == TableRef("t0", 1, 2)


def test_row_count_mismatch_names_table_without_range() -> None:
    """Число строк не сошлось — таблица названа, диапазона нет: догадка неотличима от факта."""
    table = ExtractedTable(table_id="t0", header=("имя", "значение"), rows=(("только", "одна"),))
    md = "# Секция\n\n" + _pipe(
        ("имя", "значение"),
        (("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")),
    )

    chunks = _chunks(md, (table,))

    assert len(chunks) == 1
    assert chunks[0].table_ref == TableRef("t0")
    assert chunks[0].table_ref is not None
    assert chunks[0].table_ref.row_start is None and chunks[0].table_ref.row_end is None


def test_header_only_chunk_names_table_without_rows() -> None:
    """Кусок из одной шапки: таблица названа, строк данных в нём нет."""
    table = ExtractedTable(table_id="t0", header=("имя", "значение"), rows=())
    md = "# Секция\n\n" + _pipe(("имя", "значение"), ())

    chunks = _chunks(md, (table,))

    assert len(chunks) == 1
    assert chunks[0].table_ref == TableRef("t0")


# --------------------------------------------------------------------------- #
# Сопоставление объект ↔ блок
# --------------------------------------------------------------------------- #


def test_tables_none_gives_no_address() -> None:
    """``None`` — читатель таблиц не даёт (HTML, text_layer, шов): адреса нет ни у кого."""
    md = "# Секция\n\n" + _pipe(("a", "b"), (("1", "2"),))

    chunks = _chunks(md, None)

    assert [chunk.table_ref for chunk in chunks] == [None]


def test_fewer_objects_than_blocks_gives_no_address_at_all() -> None:
    """Один объект на два блока — сопоставление по порядку было бы догадкой."""
    first = (("a", "b"), (("1", "2"),))
    second = (("c", "d"), (("3", "4"),))
    md = "# Секция\n\n" + _pipe(*first) + "\n" + _pipe(*second)
    table = ExtractedTable(table_id="t0", header=first[0], rows=first[1])

    chunks = _chunks(md, (table,))

    assert [chunk.table_ref for chunk in chunks] == [None]


def test_more_objects_than_blocks_gives_no_address_at_all() -> None:
    """Обратное расхождение — тот же отказ: адрес не привязывается ни к какой таблице."""
    rows = (("1", "2"),)
    md = "# Секция\n\n" + _pipe(("a", "b"), rows)
    tables = (
        ExtractedTable(table_id="t0", header=("a", "b"), rows=rows),
        ExtractedTable(table_id="t1", header=("c", "d"), rows=rows),
    )

    chunks = _chunks(md, tables)

    assert [chunk.table_ref for chunk in chunks] == [None]


# --------------------------------------------------------------------------- #
# Пересечение куска с таблицей
# --------------------------------------------------------------------------- #


def test_chunk_outside_table_has_no_address() -> None:
    """Кусок вне таблицы адреса не получает — иначе «неизвестно» и «не таблица» слились бы."""
    rows = (("1", "2"),)
    table = ExtractedTable(table_id="t0", header=("a", "b"), rows=rows)
    md = (
        "# Первая\n\nТекст без таблицы, достаточно длинный.\n\n"
        "# Вторая\n\n" + _pipe(("a", "b"), rows)
    )

    chunks = _chunks(md, (table,))

    assert [chunk.table_ref for chunk in chunks] == [None, TableRef("t0", 1, 1)]


def test_chunk_starting_before_table_keeps_address() -> None:
    """Кусок начинается с заголовка секции — адрес всё равно есть: привязка по пересечению.

    Вложенности здесь нет и быть не может: кусок открывается заголовком, а кончается на
    границе строки таблицы. Именно такому куску адрес и нужен — в нём шапка таблицы.
    """
    rows = (("1", "2"), ("3", "4"))
    table = ExtractedTable(table_id="t0", header=("a", "b"), rows=rows)
    md = "# Раздел\n\n" + _pipe(("a", "b"), rows)

    chunks = _chunks(md, (table,))

    assert len(chunks) == 1
    assert chunks[0].text.startswith("# Раздел")
    assert "| a | b |" in chunks[0].text
    assert chunks[0].table_ref == TableRef("t0", 1, 2)


def test_chunk_spanning_two_tables_has_no_address() -> None:
    """Кусок, пересекающий две таблицы, адреса не получает: перечисление — не адрес."""
    first = (("a", "b"), (("1", "2"),))
    second = (("c", "d"), (("3", "4"),))
    md = "# Секция\n\n" + _pipe(*first) + "\n" + _pipe(*second)
    tables = (
        ExtractedTable(table_id="t0", header=first[0], rows=first[1]),
        ExtractedTable(table_id="t1", header=second[0], rows=second[1]),
    )

    chunks = _chunks(md, tables)

    assert len(chunks) == 1, "обе таблицы обязаны попасть в один кусок"
    assert chunks[0].table_ref is None


def test_split_table_gives_disjoint_ranges_covering_all_rows() -> None:
    """Таблица, разрезанная на куски: у каждого свой диапазон, они не пересекаются и полны."""
    rows = tuple((f"param_{i}", f"value_{i}_with_some_length") for i in range(1, 13))
    table = ExtractedTable(table_id="t0", header=("параметр", "значение"), rows=rows)
    md = "# Таблица\n\n" + _pipe(("параметр", "значение"), rows)

    chunks = _chunks(md, (table,), chunk_size=300)

    refs = [chunk.table_ref for chunk in chunks]
    assert len(chunks) > 1, "таблица обязана разрезаться"
    assert all(ref is not None for ref in refs)
    assert all(ref.table_id == "t0" for ref in refs if ref is not None)
    assert refs[0] == TableRef("t0", 1, 6)
    assert refs[-1] == TableRef("t0", 7, 12)
    # Диапазоны идут по порядку, не пересекаются и покрывают все строки объекта.
    spans = [(ref.row_start, ref.row_end) for ref in refs if ref is not None]
    for (_, previous_end), (next_start, _) in itertools.pairwise(spans):
        assert previous_end is not None and next_start is not None
        assert previous_end + 1 == next_start
    assert spans[0][0] == 1
    assert spans[-1][1] == len(rows)


def test_escaped_pipe_in_cell_does_not_break_binding() -> None:
    """``&#124;`` (так docling печатает ``|`` внутри ячейки) не добавляет строк и не рвёт адрес."""
    rows = (("`a || b`", "логическое &#124; или"), ("`x`", "второй ряд"))
    table = ExtractedTable(
        table_id="t0",
        header=("оператор", "смысл"),
        rows=(("`a || b`", "логическое | или"), ("`x`", "второй ряд")),
    )
    md = "# Секция\n\n" + _pipe(("оператор", "смысл"), rows)

    chunks = _chunks(md, (table,))

    assert len(chunks) == 1
    assert chunks[0].table_ref == TableRef("t0", 1, 2)


# --------------------------------------------------------------------------- #
# Адрес не меняет текст
# --------------------------------------------------------------------------- #


def _textual(chunk) -> tuple[object, ...]:
    """Всё, что описывает **текст** куска, без адреса в таблице."""
    return (
        chunk.chunk_id,
        chunk.chunk_no,
        chunk.text,
        chunk.heading_path,
        chunk.context_prefix,
        chunk.content_hash,
        chunk.char_start,
        chunk.char_end,
    )


def test_address_does_not_change_text_hash_or_numbering() -> None:
    """Смысл фазы 2 в её измеренной форме: адрес добавляется, текст не пере-генерируется.

    ``chunk_text(id, text)`` и ``chunk_text(id, text, tables)`` обязаны дать побайтно те же
    ``text``, ``content_hash``, ``chunk_no``, ``char_start``, ``char_end`` — иначе это была
    бы не привязка, а новая нарезка (другой корпус, другой ``run_id``).
    """
    rows = tuple((f"param_{i}", f"value_{i}_with_some_length") for i in range(1, 13))
    table = ExtractedTable(table_id="t0", header=("параметр", "значение"), rows=rows)
    md = (
        "# Раздел\n\n"
        + ("Абзац до таблицы без всякой таблицы, но достаточно длинный. " * 6)
        + "\n\n"
        + _pipe(("параметр", "значение"), rows)
        + "\nАбзац после таблицы.\n"
    )

    without = _chunks(md, None, chunk_size=300)
    with_tables = _chunks(md, (table,), chunk_size=300)

    assert len(without) == len(with_tables) > 1
    assert [_textual(chunk) for chunk in without] == [
        _textual(chunk) for chunk in with_tables
    ]
    # Единственное отличие — адрес, и он появился хотя бы у одного куска.
    assert all(chunk.table_ref is None for chunk in without)
    assert any(chunk.table_ref is not None for chunk in with_tables)


def test_port_passes_document_tables() -> None:
    """Порт-путь передаёт ``doc.tables``: адрес берётся от документа, а не из воздуха."""
    rows = (("1", "2"),)
    table = ExtractedTable(table_id="t0", header=("a", "b"), rows=rows)
    md = "# Секция\n\n" + _pipe(("a", "b"), rows)
    doc = ExtractedDoc(
        text=md,
        content_hash="hash-1",
        extractor_version="fake-1",
        tables=(table,),
    )

    chunks = MarkdownChunkerPort(min_chunk_size=1).chunk(doc, "doc")

    assert [chunk.table_ref for chunk in chunks] == [TableRef("t0", 1, 1)]
