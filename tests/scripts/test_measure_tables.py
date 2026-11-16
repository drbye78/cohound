"""Тесты прибора таблиц (`scripts/measure_tables.py`, O39 фаза 2).

Прибор отвечает на три вопроса, и все три ломаются тихо — число получается правдоподобным:

* **сколько таблиц и какого размера** — от этого зависит, возможен ли «чанк на таблицу» или
  нужна нарезка внутри таблицы;
* **совпадают ли объекты читателя с блоками markdown** — по этому числу решается, можно ли
  сопоставлять объект с местом в тексте по порядку;
* **что нарезка делает с таблицей сегодня** — и здесь ошибка дешевле всего: чанк считается
  табличным по **пересечению спанов**, а не по ``"|" in text``, иначе код и цитаты с
  вертикальной чертой попадают в «табличные чанки», и доля таблиц в корпусе завышается.

Отдельно проверяется **адрес** куска (``table_chunks_bound``/``..._with_rows``/``..._unbound``):
три исхода стоят по-разному, и свести их в один счётчик значило бы объявить «адрес есть» там,
где связать кусок не с чем.

Docling, PDF, GPU и БД не нужны: проверяются чистые функции, журнал и разбор аргументов.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from core.domain.doc_tables import ExtractedTable

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_tables.py"
    spec = importlib.util.spec_from_file_location("measure_tables", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_tables"] = module
    spec.loader.exec_module(module)
    return module


measure = _load_module()


def _doc(page_count: int, name: str = "") -> dict:
    """Кандидат выборки: путь и число страниц (страта считается по нему)."""
    return {"path": f"/x/{name or page_count}.bin", "page_count": page_count}


#: Таблица с шапкой: 3 строки markdown (шапка + две данных), 2 столбца.
PIPE_TABLE = "| Имя | Значение |\n| --- | --- |\n| a | 1 |\n| b | 2 |\n"


# ---------------------------------------------------------------- стратификация


def test_stratified_sample_covers_every_stratum_evenly() -> None:
    """Иначе выборка описывала бы один размер документа под именем корпуса."""
    candidates = [_doc(n, f"d{n}") for n in (1, 5, 9, 20, 30, 50, 70, 120, 200, 400)]
    chosen = measure._stratified(candidates, 8, 0)
    counts: dict[str, int] = {}
    for item in chosen:
        counts[item["stratum"]] = counts.get(item["stratum"], 0) + 1
    assert set(counts) == {"short", "medium", "long", "huge"}
    assert set(counts.values()) == {2}


def test_stratified_sample_is_deterministic_by_seed() -> None:
    """Один seed — одна выборка: иначе опубликованное число не воспроизводится."""
    candidates = [_doc(n, f"d{n}") for n in range(1, 200)]
    first = [item["path"] for item in measure._stratified(candidates, 12, 7)]
    second = [item["path"] for item in measure._stratified(candidates, 12, 7)]
    other = [item["path"] for item in measure._stratified(candidates, 12, 8)]
    assert first == second
    assert first != other


def test_stratified_sample_has_no_duplicates() -> None:
    """Дубль в выборке — тот же документ, посчитанный дважды в сводке."""
    candidates = [_doc(n % 200 + 1, f"d{n}") for n in range(60)]
    chosen = measure._stratified(candidates, 20, 3)
    assert len({item["path"] for item in chosen}) == len(chosen)


def test_stratified_sample_never_exceeds_the_candidates() -> None:
    """Просили больше, чем есть, — берём сколько есть, без отказа и без дублей."""
    chosen = measure._stratified([_doc(5, "a"), _doc(60, "b")], 20, 0)
    assert len(chosen) == 2
    assert {item["stratum"] for item in chosen} == {"short", "long"}


def test_stratified_sample_keeps_the_declared_boundaries() -> None:
    """Границы страт объявлены в модуле: 10 — ещё short, 11 — уже medium.

    Сдвиг границы на единицу меняет состав выборки молча, поэтому границы проверяются
    ровно там, где они заданы (``STRATA``), а не «где-то около».
    """
    edges = [_doc(n, f"e{n}") for n in (10, 11, 40, 41, 100, 101)]
    chosen = {item["path"]: item["stratum"] for item in measure._stratified(edges, 8, 0)}
    assert chosen["/x/e10.bin"] == "short"
    assert chosen["/x/e11.bin"] == "medium"
    assert chosen["/x/e40.bin"] == "medium"
    assert chosen["/x/e41.bin"] == "long"
    assert chosen["/x/e100.bin"] == "long"
    assert chosen["/x/e101.bin"] == "huge"


def test_stratified_sample_is_not_the_first_documents_of_the_database() -> None:
    """Порядок в базе — это продукт, а не корпус: «первые N» мерили бы один продукт."""
    candidates = [_doc(200, f"a{i}") for i in range(50)]
    chosen = [item["path"] for item in measure._stratified(candidates, 4, 0)]
    assert len(chosen) == 4
    assert chosen != [f"/x/a{i}.bin" for i in range(4)]


def test_stratified_sample_fills_the_quota_from_a_skewed_corpus() -> None:
    """Пустые страты не съедают квоту: на стенде почти все документы — long/huge."""
    candidates = [_doc(200, f"d{i}") for i in range(30)]
    chosen = measure._stratified(candidates, 20, 0)
    assert len(chosen) == 20
    assert {item["stratum"] for item in chosen} == {"huge"}


# ---------------------------------------------------------------- таблица-объект


def test_table_row_reports_shape_and_header() -> None:
    """``rows``/``cols``/``has_header`` — вход решения «влезает ли таблица в кусок»."""
    table = ExtractedTable(
        table_id="t0",
        page_no=3,
        caption="Таблица 1",
        header=("Имя", "Значение"),
        rows=(("a", "1"), ("b", "2")),
    )
    row = measure._table_row(table)
    assert row["table_id"] == "t0"
    assert row["page_no"] == 3
    assert row["rows"] == 2
    assert row["cols"] == 2
    assert row["has_header"] is True


def test_table_row_without_header_takes_width_from_the_first_row() -> None:
    """Шапки нет — ширину задаёт первая строка данных, иначе ``cols`` был бы нулём."""
    table = ExtractedTable(table_id="t0", rows=(("1", "2", "3"), ("4", "5", "6")))
    row = measure._table_row(table)
    assert row["has_header"] is False
    assert row["cols"] == 3
    assert row["rows"] == 2


def test_table_row_of_empty_table_is_not_an_error() -> None:
    """Пустая таблица — факт читателя, а не повод падать: нули, а не исключение."""
    row = measure._table_row(ExtractedTable(table_id="t0"))
    assert (row["rows"], row["cols"], row["has_header"]) == (0, 0, False)
    assert row["max_cell_chars"] == 0
    assert row["ragged_widths"] == []


def test_table_row_counts_breaking_cells_per_cell_not_per_table() -> None:
    """Ячейка с ``|``/переводом строки рвёт pipe-представление — считать надо ячейки.

    Счёт «в таблице есть вертикальная черта» дал бы 1 на любую таблицу с такой ячейкой и
    скрыл бы, что сломаны две.
    """
    table = ExtractedTable(
        table_id="t0",
        header=("Имя", "Значение"),
        rows=(("x|y", "z"), ("a\nb", "|")),
    )
    row = measure._table_row(table)
    assert row["cells_with_pipe"] == 2
    assert row["cells_with_newline"] == 1


def test_table_row_counts_header_cells_too() -> None:
    """Шапка входит в счётчики — иначе замер занижает сам себя.

    Первая версия прибора собирала ячейки только из ``table.rows``, и ячейка шапки с ``|``
    не считалась разрывом, хотя печатается той же строкой ``| … |`` и попадает в ``text``
    куска. Тот же пропуск занижал ``cells_chars``, из которого считается ``fits_one_chunk``,
    — главный ответ замера («влезает ли таблица в один кусок») был смещён в сторону
    «влезает». Тест держит исправление: шапка считается наравне с данными.
    """
    table = ExtractedTable(table_id="t0", header=("a|b", "c"), rows=(("x", "y"),))
    row = measure._table_row(table)
    assert row["cells_with_pipe"] == 1  # «|» в шапке — тоже разрыв представления
    assert row["cells_chars"] == 6  # «a|b» + «c» + «x» + «y»
    assert row["max_cell_chars"] == 3  # «a|b»


def test_table_row_of_header_only_table_counts_its_cells() -> None:
    """Таблица из одной шапки имеет размер: шапка — её единственное содержимое."""
    row = measure._table_row(ExtractedTable(table_id="t0", header=("очень длинное имя",)))
    assert (row["cols"], row["rows"]) == (1, 0)
    assert (row["cells_chars"], row["max_cell_chars"]) == (17, 17)


def test_table_row_of_a_clean_table_has_no_breaking_cells() -> None:
    """Нормальная таблица не имеет ни одного разрыва: иначе число перестало бы значить."""
    table = ExtractedTable(
        table_id="t0", header=("Имя", "Значение"), rows=(("a", "1"), ("b", "2"))
    )
    row = measure._table_row(table)
    assert row["cells_with_pipe"] == 0
    assert row["cells_with_newline"] == 0


def test_table_row_sums_cell_and_caption_characters() -> None:
    """``cells_chars + caption_chars`` — размер таблицы для вопроса «влезает в кусок»."""
    table = ExtractedTable(
        table_id="t0",
        caption="Таблица 1",
        header=("a", "b"),
        rows=(("de", "f"), ("gh", "i")),
    )
    row = measure._table_row(table)
    assert row["cells_chars"] == 8  # шапка (a + b) + данные (de + f + gh + i)
    assert row["max_cell_chars"] == 2
    assert row["caption_chars"] == len("Таблица 1")


def test_table_row_ragged_widths_of_a_valid_table_is_one_width() -> None:
    """У допустимой таблицы ширина одна: ``ragged_widths`` — контроль, а не гипотеза."""
    table = ExtractedTable(table_id="t0", header=("a", "b"), rows=(("1", "2"),))
    assert measure._table_row(table)["ragged_widths"] == [2]


def test_ragged_table_is_refused_by_the_carrier() -> None:
    """Рваной таблицы не существует: ``ExtractedTable`` отказывает, а не обрезает.

    Прибор считает ``ragged_widths`` как множество ширин, но проверить на нём две ширины
    нельзя — инвариант носителя (:meth:`ExtractedTable.__post_init__`) отвергает строку
    другой длины. Поэтому здесь фиксируется отказ носителя, а не выдуманное поведение
    прибора: строка, обрезанная молча, объявила бы таблицу, которой читатель не видел.
    """
    with pytest.raises(ValueError, match="ширина строки"):
        ExtractedTable(table_id="t0", header=("a", "b"), rows=(("1",),))
    with pytest.raises(ValueError, match="ширина строки"):
        ExtractedTable(table_id="t0", rows=(("1", "2"), ("3",)))


# ---------------------------------------------------------------- блоки markdown


def test_table_blocks_finds_one_pipe_table() -> None:
    """Один pipe-блок: спан, строки, шапка, диалект."""
    blocks = measure._table_blocks(PIPE_TABLE)
    assert len(blocks) == 1
    block = blocks[0]
    assert block["dialect"] == "pipe"
    assert block["header_rows"] == 1
    assert block["rows"] == 3  # шапка + две строки данных
    assert block["chars"] == block["end"] - block["start"]


def test_table_blocks_spans_are_real_and_ordered() -> None:
    """Спан обязан быть адресом: срез текста по нему — сама таблица, а не её часть."""
    text = PIPE_TABLE + "\nТекст между таблицами.\n\n" + PIPE_TABLE
    blocks = measure._table_blocks(text)
    assert len(blocks) == 2
    for block in blocks:
        assert text[block["start"] : block["end"]].startswith("|")
        assert block["chars"] == block["end"] - block["start"]
    assert blocks[0]["end"] <= blocks[1]["start"]


def test_table_blocks_do_not_count_the_separator_row() -> None:
    """Строка ``| --- |`` — разметка, а не строка таблицы: иначе строк было бы на одну больше."""
    text = "| a | b |\n| --- | --- |\n| 1 | 2 |\n"
    block = measure._table_blocks(text)[0]
    assert block["rows"] == 2  # шапка + одна данных, разделитель не в счёт
    assert block["header_rows"] == 1


def test_table_blocks_max_row_chars_is_the_longest_row() -> None:
    """Строка длиннее ``chunk_size`` режется внутри: это отдельный факт о таблице."""
    text = "| a | b |\n| --- | --- |\n| " + "x" * 40 + " | 1 |\n| c | d |\n"
    block = measure._table_blocks(text)[0]
    lengths = [
        len(line) + 1
        for line in text.splitlines()
        if not line.startswith("| ---")
    ]
    assert block["max_row_chars"] == max(lengths)


def test_table_blocks_of_a_paragraph_with_pipes_is_empty() -> None:
    """Вертикальная черта в прозе — не таблица: иначе блоков было бы больше, чем таблиц."""
    assert measure._table_blocks("Абзац с чертой | и ещё одной | внутри текста.\n") == []


def test_table_blocks_of_a_code_block_with_pipes_is_empty() -> None:
    """Код с ``|`` — тоже не таблица, и это главная причина считать пересечение спанов."""
    assert measure._table_blocks("```\nselect a | b from t\n```\n") == []


def test_table_blocks_of_a_pipe_block_without_separator_is_empty() -> None:
    """Без строки-разделителя pipe-таблицы в GFM нет: блоки не выдумываются."""
    assert measure._table_blocks("| a | b |\n| 1 | 2 |\n") == []


# ---------------------------------------------------------------- нарезка


def _table_chunk_facts(text: str, tables: tuple[ExtractedTable, ...] | None):
    """Факты нарезки по блокам, найденным самим прибором (как в дочернем замере)."""
    blocks = measure._table_blocks(text)
    return measure._chunk_facts(
        text, [(block["start"], block["end"]) for block in blocks], tables
    )


def test_chunk_with_pipe_in_prose_is_not_a_table_chunk() -> None:
    """Чанк табличный по **пересечению спанов**, а не по наличию ``|`` в тексте.

    Первый чанк целиком из прозы, и вертикальная черта в нём есть — по ``"|" in text`` он
    был бы табличным, и доля табличных чанков завысилась бы на всём корпусе.
    """
    prose = ("Предложение с вертикальной чертой | внутри прозы. " * 60).strip()
    text = prose + "\n\n" + PIPE_TABLE
    table = ExtractedTable(table_id="t0", header=("Имя", "Значение"), rows=(("a", "1"), ("b", "2")))
    facts = _table_chunk_facts(text, (table,))
    assert "|" in text[: measure.CHUNK_SIZE]  # первый чанк — проза с чертой
    assert measure._table_blocks(text)[0]["start"] > measure.CHUNK_SIZE
    assert facts["chunks"] >= 3
    assert facts["table_chunks"] == 1


def test_table_with_header_is_bound_with_rows() -> None:
    """Шапка в тексте + объект с шапкой: кусок получает адрес **и** номера строк."""
    table = ExtractedTable(
        table_id="t0", header=("Имя", "Значение"), rows=(("a", "1"), ("b", "2"))
    )
    facts = _table_chunk_facts(PIPE_TABLE, (table,))
    assert facts["tables_row_verified"] == 1
    assert facts["tables_row_unverified"] == 0
    assert facts["table_chunks_bound"] == 1
    assert facts["table_chunks_with_rows"] == 1
    assert facts["table_chunks_unbound"] == 0


def test_table_without_header_is_bound_with_rows() -> None:
    """У таблицы без шапки число строк в тексте равно числу строк объекта.

    Без этой сверки адрес уехал бы на единицу ровно у таблиц без шапки (75 из 181 на
    стенде): строка-шапка в pipe-представлении есть всегда, даже когда её нет в объекте.
    """
    text = "| a | b |\n| --- | --- |\n| 1 | 2 |\n| 3 | 4 |\n"
    table = ExtractedTable(table_id="t0", rows=(("a", "b"), ("1", "2"), ("3", "4")))
    facts = _table_chunk_facts(text, (table,))
    assert facts["tables_row_verified"] == 1
    assert facts["table_chunks_with_rows"] == 1


def test_mismatched_object_count_leaves_every_chunk_unbound() -> None:
    """Два объекта на один блок — сопоставление по порядку не выполняется вовсе.

    «Сдвинулось на одну» и «другая таблица» неразличимы, и привязать кусок к чужой
    таблице хуже, чем не привязать ни к какой.
    """
    text = "| a | b |\n| --- | --- |\n| 1 | 2 |\n"
    tables = (
        ExtractedTable(table_id="t0", header=("a", "b"), rows=(("1", "2"),)),
        ExtractedTable(table_id="t1", header=("c", "d"), rows=(("3", "4"),)),
    )
    facts = _table_chunk_facts(text, tables)
    assert facts["table_chunks"] == 1
    assert facts["table_chunks_bound"] == 0
    assert facts["table_chunks_with_rows"] == 0
    assert facts["table_chunks_unbound"] == 1
    assert (facts["tables_row_verified"], facts["tables_row_unverified"]) == (0, 0)


def test_missing_tables_leave_the_chunk_unbound() -> None:
    """``tables=None`` — «читатель таблиц не даёт»: кусок табличный, но связать не с чем."""
    facts = _table_chunk_facts(PIPE_TABLE, None)
    assert facts["table_chunks"] == 1
    assert facts["table_chunks_unbound"] == 1
    assert facts["tables_row_verified"] == 0


def test_row_verification_separates_verified_from_unverified() -> None:
    """Адрес без строк — отдельный исход: таблица названа, процитировать построчно нельзя."""
    text = "| a | b |\n| --- | --- |\n| 1 | 2 |\n"
    table = ExtractedTable(
        table_id="t0", header=("a", "b"), rows=(("1", "2"), ("3", "4"), ("5", "6"))
    )
    facts = _table_chunk_facts(text, (table,))
    assert facts["tables_row_verified"] == 0
    assert facts["tables_row_unverified"] == 1
    assert facts["table_chunks_bound"] == 1
    assert facts["table_chunks_with_rows"] == 0


def test_chunks_per_table_has_one_entry_per_span() -> None:
    """Длина списка — число табличных спанов: иначе сводка усредняла бы разное."""
    text = PIPE_TABLE + "\nТекст.\n\n" + PIPE_TABLE
    tables = (
        ExtractedTable(table_id="t0", header=("Имя", "Значение"), rows=(("a", "1"), ("b", "2"))),
        ExtractedTable(table_id="t1", header=("Имя", "Значение"), rows=(("c", "3"), ("d", "4"))),
    )
    facts = _table_chunk_facts(text, tables)
    assert len(facts["chunks_per_table"]) == len(measure._table_blocks(text))


def test_chunks_per_table_sum_equals_table_chunks() -> None:
    """Таблица, разрезанная на два куска, даёт ``[2]`` — и оба куска табличные."""
    rows = "\n".join(f"| row{i} | {'x' * 60} |" for i in range(40))
    text = "| a | b |\n| --- | --- |\n" + rows + "\n"
    table = ExtractedTable(
        table_id="t0", header=("a", "b"), rows=tuple((f"row{i}", "x" * 60) for i in range(40))
    )
    facts = _table_chunk_facts(text, (table,))
    assert facts["chunks_per_table"] == [2]
    assert sum(facts["chunks_per_table"]) == facts["table_chunks"]


def test_chunk_touching_two_tables_gets_no_address() -> None:
    """Кусок, пересекающий две таблицы, адреса не получает: перечисление — не адрес."""
    text = PIPE_TABLE + "\nТекст.\n\n" + PIPE_TABLE
    tables = (
        ExtractedTable(table_id="t0", header=("Имя", "Значение"), rows=(("a", "1"), ("b", "2"))),
        ExtractedTable(table_id="t1", header=("Имя", "Значение"), rows=(("c", "3"), ("d", "4"))),
    )
    facts = _table_chunk_facts(text, tables)
    assert facts["table_chunks"] == 1  # обе таблицы в одном куске
    assert facts["table_chunks_unbound"] == 1
    assert sum(facts["chunks_per_table"]) == 2  # по строке на каждую пересечённую таблицу


def test_no_tables_gives_no_table_chunks() -> None:
    """Документ без таблиц: нули, а не ``None`` — чанки посчитаны, табличных среди них нет."""
    facts = _table_chunk_facts("Обычный текст без таблиц.\n\nЕщё абзац.\n", None)
    assert facts["chunks"] == 1
    assert facts["table_chunks"] == 0
    assert facts["chunks_per_table"] == []
    assert facts["table_chunk_chars_median"] is None


# ---------------------------------------------------------------- сводка


def _row(**overrides: object) -> dict:
    """Строка журнала в том составе, который пишет дочерний замер."""
    row: dict = {
        "path": "/x/a.bin",
        "chars": 100,
        "tables_state": "present",
        "tables": [],
        "table_blocks": [],
        "block_chars": 0,
        "chunks": 4,
        "table_chunks": 0,
        "table_chunk_chars": 0,
        "tables_row_verified": 0,
        "tables_row_unverified": 0,
        "table_chunks_bound": 0,
        "table_chunks_with_rows": 0,
        "table_chunks_unbound": 0,
        "chunks_per_table": [],
        "object_vs_block": {"tables": 0, "blocks": 0, "equal": True},
    }
    row.update(overrides)
    return row


def _table_stat(
    *,
    rows: int = 2,
    cols: int = 2,
    has_header: bool = True,
    caption_chars: int = 0,
    cells_chars: int = 10,
) -> dict:
    """Строка ``_table_row`` без обращения к носителю: сводка работает с числами."""
    return {
        "table_id": "t0",
        "page_no": 1,
        "rows": rows,
        "cols": cols,
        "has_header": has_header,
        "caption_chars": caption_chars,
        "cells_chars": cells_chars,
        "max_cell_chars": 4,
        "cells_with_pipe": 0,
        "cells_with_newline": 0,
        "ragged_widths": [cols],
    }


def test_summary_skips_failed_and_empty_documents() -> None:
    """Отказ и пустой текст не голосуют за медианы: иначе «таблиц нет» стало бы фактом."""
    rows = [
        _row(chars=100, tables=[_table_stat()]),
        _row(chars=0),
        _row(chars=50, error="timeout 1800 s"),
    ]
    summary = measure._summarize(rows)
    assert summary["documents"] == 3
    assert summary["measured"] == 1
    assert summary["failed"] == 1
    assert summary["tables"] == 1
    assert summary["chars_total"] == 100


def test_summary_keeps_none_and_zero_apart() -> None:
    """«Нечего считать» и «посчитали ноль» — разные факты (A3), и в сводке они разные."""
    summary = measure._summarize([_row(chars=100, chunks=2)])
    assert summary["cols_max"] is None
    assert summary["cells_chars_max"] is None
    assert summary["max_row_chars"] is None
    assert summary["table_chunk_share"] == 0.0  # чанки есть, табличных среди них ноль
    assert summary["table_share_of_text"] == 0.0

    empty = measure._summarize([])
    assert empty["cols_max"] is None
    assert empty["table_chunk_share"] is None  # чанков нет вовсе
    assert empty["table_share_of_text"] is None
    assert empty["chunks_per_table_median"] is None


def test_summary_sums_the_address_counters() -> None:
    """Три исхода адреса складываются раздельно: «названа» и «со строками» — разная цена."""
    rows = [
        _row(table_chunks=3, tables_row_verified=2, tables_row_unverified=1,
             table_chunks_bound=2, table_chunks_with_rows=1, table_chunks_unbound=1),
        _row(table_chunks=1, tables_row_verified=0, tables_row_unverified=1,
             table_chunks_bound=1, table_chunks_with_rows=1, table_chunks_unbound=0),
    ]
    summary = measure._summarize(rows)
    assert summary["table_chunks"] == 4
    assert summary["tables_row_verified"] == 2
    assert summary["tables_row_unverified"] == 2
    assert summary["table_chunks_bound"] == 3
    assert summary["table_chunks_with_rows"] == 2
    assert summary["table_chunks_unbound"] == 1


def test_summary_publishes_shares_not_absolute_numbers() -> None:
    """``table_share_of_text`` — доля: абсолютный объём зависит от размера выборки."""
    rows = [
        _row(chars=150, block_chars=50, chunks=8, table_chunks=2),
        _row(chars=50, block_chars=10, chunks=2, table_chunks=1),
    ]
    summary = measure._summarize(rows)
    assert summary["chars_total"] == 200
    assert summary["table_block_chars"] == 60
    assert summary["table_share_of_text"] == 0.3
    assert summary["chunks_total"] == 10
    assert summary["table_chunk_share"] == 0.3


def test_summary_counts_objects_against_blocks() -> None:
    """Совпадение объекта с блоком — основание сопоставлять по порядку, и оно числом."""
    without_text = _row()
    without_text.pop("object_vs_block")  # текста не было — сопоставления тоже
    rows = [
        _row(object_vs_block={"tables": 2, "blocks": 2, "equal": True}),
        _row(object_vs_block={"tables": 2, "blocks": 1, "equal": False}),
        without_text,
    ]
    summary = measure._summarize(rows)
    assert summary["objects_vs_blocks_equal"] == 1
    assert summary["objects_vs_blocks_differ"] == 1


def test_summary_counts_a_table_that_does_not_fit_one_chunk() -> None:
    """Таблица шире куска требует нарезки внутри таблицы — это и есть вопрос прибора."""
    wide = measure.CHUNK_SIZE + 7
    rows = [
        _row(
            tables=[_table_stat(cells_chars=wide)],
            table_blocks=[{"chars": wide, "max_row_chars": wide}],
            block_chars=wide,
        ),
        _row(
            tables=[_table_stat(cells_chars=20)],
            table_blocks=[{"chars": 20, "max_row_chars": 20}],
            block_chars=20,
        ),
    ]
    summary = measure._summarize(rows)
    assert summary["fits_one_chunk"] == 1
    assert summary["blocks_fitting_one_chunk"] == 1
    assert summary["blocks_with_row_over_chunk"] == 1
    assert summary["max_row_chars"] == wide


def test_summary_median_of_tables_is_not_the_total() -> None:
    """Медиана по таблицам, а не по документам: одна гигантская не сдвигает «типичную»."""
    rows = [
        _row(tables=[_table_stat(cells_chars=10), _table_stat(cells_chars=10)]),
        _row(tables=[_table_stat(cells_chars=1000)]),
    ]
    summary = measure._summarize(rows)
    assert summary["cells_chars_median"] == 10
    assert summary["cells_chars_max"] == 1000
    assert summary["rows_median"] == 2


def test_summary_counts_tables_states_over_every_document() -> None:
    """``None`` (не даёт) и ``()`` (искал и не нашёл) считаются раздельно, включая отказы."""
    rows = [
        _row(tables_state="present", tables=[_table_stat()]),
        _row(tables_state="empty"),
        _row(tables_state="unmeasured"),
        _row(tables_state="unmeasured", error="timeout 5 s"),
    ]
    summary = measure._summarize(rows)
    assert summary["tables_state"] == {"present": 1, "empty": 1, "unmeasured": 2}


# ---------------------------------------------------------------- журнал


def test_journal_writes_a_row_immediately(tmp_path: Path) -> None:
    """Строка на диске сразу после замера: убитый прогон обязан оставить данные.

    16.11.2026 замер O30 погиб на 15-м документе из 20 и не оставил ни одного числа —
    ровно потому, что результат писался в конце.
    """
    path = tmp_path / "o.jsonl"
    journal = measure._Journal(path)
    journal.write({"path": "/x/a.bin", "chars": 10})
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
    journal.write({"path": "/x/b.bin", "chars": 20})
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_journal_round_trips_rows_unchanged(tmp_path: Path) -> None:
    """Читается обратно тот же состав: сводка считается из журнала, а не из памяти."""
    path = tmp_path / "o.jsonl"
    journal = measure._Journal(path)
    rows = [
        {"path": "/x/a.bin", "chars": 10, "tables": [], "error": None},
        {"path": "/x/b.bin", "error": "timeout 5 s"},
    ]
    for row in rows:
        journal.write(row)
    assert measure._read_journal(path) == rows


def test_journal_of_a_new_run_does_not_continue_the_old_one(tmp_path: Path) -> None:
    """Новый прогон пишет с нуля: склейка с прошлым посчитала бы документы дважды."""
    path = tmp_path / "o.jsonl"
    path.write_text('{"path": "/x/old.bin"}\n', encoding="utf-8")
    measure._Journal(path).write({"path": "/x/new.bin"})
    assert [row["path"] for row in measure._read_journal(path)] == ["/x/new.bin"]


def test_journal_without_a_path_writes_nothing() -> None:
    """Без ``--out-json`` журнала нет — и это не ошибка: ``write`` просто ничего не делает."""
    journal = measure._Journal(None)
    journal.write({"path": "/x/a.bin"})
    assert journal.path is None


def test_corrupt_journal_line_is_a_usage_error(tmp_path: Path) -> None:
    """Порча журнала — отказ с адресом строки, а не половина сводки.

    Молча пропущенная строка уменьшила бы знаменатель, и «доля табличных чанков»
    посчиталась бы по другому набору документов.
    """
    path = tmp_path / "bad.jsonl"
    path.write_text('{"path": "/x/a.bin"}\n{сломано\n', encoding="utf-8")
    with pytest.raises(SystemExit) as exc:
        measure._read_journal(path)
    assert "журнал повреждён" in str(exc.value)


def test_empty_journal_is_an_empty_list(tmp_path: Path) -> None:
    """Пустой журнал — «ничего не успели», а не отказ чтения."""
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    assert measure._read_journal(path) == []


# ---------------------------------------------------------------- пересчёт из журнала


def test_from_journal_rebuilds_the_summary_without_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Путь пересчёта не измеряет ничего: данные уже есть, docling не нужен."""
    journal = tmp_path / "o.jsonl"
    writer = measure._Journal(journal)
    writer.write(
        {
            "path": "/x/a.bin",
            "chars": 100,
            "chunks": 4,
            "table_chunks": 1,
            "tables": [_table_stat()],
            "tables_state": "present",
            "table_blocks": [],
            "block_chars": 0,
        }
    )
    writer.write({"path": "/x/b.bin", "error": "timeout 5 s"})

    def _forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("--from-journal не должен ничего извлекать")

    monkeypatch.setattr(measure, "_measure_one", _forbidden)
    out = tmp_path / "out.json"
    code = measure.main(["--from-journal", str(journal), "--out-json", str(out)])
    assert code == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["conditions"]["source"] == "journal"
    assert payload["conditions"]["journal"] == str(journal)
    assert payload["summary"]["documents"] == 2
    assert payload["summary"]["measured"] == 1
    assert payload["summary"]["failed"] == 1
    assert len(payload["documents"]) == 2


def test_from_journal_without_a_single_measurement_returns_one(tmp_path: Path) -> None:
    """Ни одного измерения — код 1: отчёт записан, но публиковать нечего."""
    journal = tmp_path / "o.jsonl"
    measure._Journal(journal).write({"path": "/x/a.bin", "error": "timeout 5 s"})
    out = tmp_path / "out.json"
    code = measure.main(["--from-journal", str(journal), "--out-json", str(out)])
    assert code == 1
    assert json.loads(out.read_text(encoding="utf-8"))["summary"]["measured"] == 0


def test_from_journal_does_not_import_the_extractor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Извлечение живёт в дочернем процессе и импортируется лениво.

    Проверка не «модуль не в ``sys.modules``» (его мог подтянуть соседний тест), а
    «путь работает при недоступном модуле»: ``None`` в ``sys.modules`` делает импорт
    невозможным, и любой ленивый импорт на этом пути упал бы.
    """
    journal = tmp_path / "o.jsonl"
    measure._Journal(journal).write({"path": "/x/a.bin", "chars": 100})
    monkeypatch.setitem(sys.modules, "core.adapters.extract_document", None)
    assert measure.main(["--from-journal", str(journal)]) == 0


# ---------------------------------------------------------------- аргументы


def test_defaults_declare_level_device_and_sample() -> None:
    """Условие замера объявлено умолчаниями: уровень профиля, устройство, выборка."""
    args = measure._parse_args([])
    assert args.level == "layout_tables"
    assert args.device == "cuda"
    assert args.sample == 30
    assert args.seed == 0
    assert args.top == 0
    assert args.pdf == []
    assert args.child is False


def test_sample_seed_and_top_are_parsed() -> None:
    """Выборка воспроизводима только вместе с seed: оба обязаны доехать до отбора."""
    args = measure._parse_args(["--sample", "12", "--seed", "7", "--top", "5"])
    assert (args.sample, args.seed, args.top) == (12, 7, 5)


def test_device_is_a_closed_set_without_auto() -> None:
    """``auto`` нет: место вычисления — объявленное условие (R5)."""
    with pytest.raises(SystemExit):
        measure._parse_args(["--device", "auto"])


def test_level_is_passed_through_to_the_child() -> None:
    """Уровень не проверяется argparse — его проверяет ``DocumentLevel`` в ребёнке."""
    assert measure._parse_args(["--level", "text"]).level == "text"
    assert measure._parse_args(["--level", "full"]).level == "full"


def test_db_and_from_state_db_are_the_same_input() -> None:
    """Два имени одного входа: короткое и длинное, чтобы команду нельзя было прочитать неверно."""
    assert measure._parse_args(["--db", "s.db"]).db == "s.db"
    assert measure._parse_args(["--from-state-db", "s.db"]).db_path == "s.db"


def test_child_flag_is_hidden_from_help(capsys: pytest.CaptureFixture[str]) -> None:
    """``--child`` — внутренний протокол прибора, а не ручка оператора."""
    with pytest.raises(SystemExit):
        measure._parse_args(["--help"])
    assert "--child" not in capsys.readouterr().out


def test_child_flag_is_parsed() -> None:
    """Скрытый флаг всё равно обязан разбираться: им запускается дочерний замер."""
    assert measure._parse_args(["--child"]).child is True


def test_no_input_is_a_usage_error() -> None:
    """Ни PDF, ни базы — отказ с кодом 2, а не пустая сводка."""
    assert measure.main([]) == 2
