"""Читатель docling: таблица становится объектом (O39 фаза 1 плана ``26``).

Предмет — не «таблица попала в markdown» (это было и раньше), а **факт существования
таблицы**: шапка, строки, подпись и страница, извлечённые из ``DoclingDocument``.

Три состояния различаются намеренно, и тест обязан их различать:

* таблица с ``prov`` → ``page_no`` и строки;
* документ без таблиц → **пустой кортеж** («читатель искал и не нашёл»);
* ``text_layer`` (pdfium) → ``None`` («читатель таблиц не даёт вовсе»).

Реальный docling здесь не нужен: предмет — арифметика сетки и провенанса, а модели
загружались бы минутами. Форма ``DoclingDocument``/``TableItem`` проверена по библиотеке
2.135.0: ``tables: list[TableItem]``, ``table.data.grid`` через ``table_cells``
(``start_row_offset_idx``/``start_col_offset_idx``/``text``/``column_header``),
``table.prov[0].page_no`` (1-based), подпись — ссылка ``captions``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from core.adapters.extract_document import DoclingDocumentExtractor
from core.domain.enums import DocumentLevel
from core.ports.types import ExtractedDoc


class _FakeConverter:
    """Конвертер-фейк: отдаёт заранее собранный ``ConversionResult``."""

    def __init__(self, document: object, *, status: str = "SUCCESS") -> None:
        self._result = SimpleNamespace(
            status=SimpleNamespace(name=status),
            document=document,
            pages=[object()],
            input=SimpleNamespace(page_count=1),
        )

    def convert(self, path: str) -> object:  # noqa: ARG002 - путь не важен
        return self._result


def _cell(row: int, column: int, text: str, *, header: bool = False) -> Any:
    """Ячейка таблицы в форме ``TableCell`` docling (только нужные поля)."""
    return SimpleNamespace(
        start_row_offset_idx=row,
        end_row_offset_idx=row + 1,
        start_col_offset_idx=column,
        end_col_offset_idx=column + 1,
        text=text,
        column_header=header,
        row_header=False,
    )


def _table_item(
    cells: list[Any],
    *,
    num_rows: int,
    num_cols: int,
    page_no: int | None = 1,
    captions: list[Any] | None = None,
) -> Any:
    """``TableItem`` в форме docling 2.135 (сетка + провенанс + подпись-ссылка)."""
    prov = (
        []
        if page_no is None
        else [SimpleNamespace(page_no=page_no, bbox=None, charspan=(0, 0))]
    )
    return SimpleNamespace(
        data=SimpleNamespace(
            table_cells=cells,
            num_rows=num_rows,
            num_cols=num_cols,
        ),
        prov=prov,
        captions=captions or [],
    )


def _document(
    *, tables: list[Any] | None = None, markdown: str = "| a | b |\n|---|---|\n| 1 | 2 |"
) -> SimpleNamespace:
    """Документ-фейк: markdown и (опционально) список таблиц.

    ``tables=None`` — у документа **нет** поля ``tables`` вовсе: так выглядит читатель,
    который таблиц не отдаёт (шов, другая версия библиотеки).
    """
    fields: dict[str, object] = {"export_to_markdown": lambda **_kw: markdown}
    if tables is not None:
        fields["tables"] = tables
    return SimpleNamespace(**fields)


def _extract(document: object, *, level: DocumentLevel = DocumentLevel.LAYOUT_TABLES) -> ExtractedDoc:
    """``_convert`` на подставленном документе — без docling, моделей и сети."""
    extractor = DoclingDocumentExtractor(level)
    return extractor._convert(_FakeConverter(document), "doc.pdf", version="v")  # noqa: SLF001 — предмет теста


def _caption(text: str) -> Any:
    """Ссылка на подпись: ``RefItem.resolve(doc)`` → элемент с ``text``."""
    return SimpleNamespace(resolve=lambda _doc, _text=text: SimpleNamespace(text=_text))


def test_table_with_provenance_becomes_object() -> None:
    """Таблица с ``prov`` → объект: страница, шапка, строки данных."""
    table = _table_item(
        [
            _cell(0, 0, "имя", header=True),
            _cell(0, 1, "значение", header=True),
            _cell(1, 0, "work_mem"),
            _cell(1, 1, "64MB"),
            _cell(2, 0, "shared_buffers"),
            _cell(2, 1, "1GB"),
        ],
        num_rows=3,
        num_cols=2,
        page_no=7,
    )

    doc = _extract(_document(tables=[table]))

    assert doc.error is None
    assert doc.tables is not None and len(doc.tables) == 1
    extracted = doc.tables[0]
    assert extracted.table_id == "t0"
    assert extracted.page_no == 7, "страница берётся из prov[0].page_no (1-based)"
    assert extracted.header == ("имя", "значение")
    assert extracted.rows == (("work_mem", "64MB"), ("shared_buffers", "1GB"))


def test_table_caption_is_resolved_from_reference() -> None:
    """Подпись читается разрешением ссылки, а не выдумывается из порядка."""
    table = _table_item(
        [_cell(0, 0, "x")],
        num_rows=1,
        num_cols=1,
        captions=[_caption("Параметры сервера")],
    )

    doc = _extract(_document(tables=[table]))

    assert doc.tables is not None
    assert doc.tables[0].caption == "Параметры сервера"
    # Шапки нет: ячейка не помечена ``column_header`` — значит и первой строки в шапку нет.
    assert doc.tables[0].header == ()
    assert doc.tables[0].rows == (("x",),)


def test_table_without_provenance_has_no_page() -> None:
    """``prov`` пуст → ``page_no=None``; ноль страницей не подменяется."""
    table = _table_item([_cell(0, 0, "x")], num_rows=1, num_cols=1, page_no=None)

    doc = _extract(_document(tables=[table]))

    assert doc.tables is not None
    assert doc.tables[0].page_no is None


def test_multi_row_header_is_not_merged() -> None:
    """Две ведущие строки-шапки не сводятся в одну: шапки нет, строки остаются как есть."""
    table = _table_item(
        [
            _cell(0, 0, "группа", header=True),
            _cell(0, 1, "группа", header=True),
            _cell(1, 0, "имя", header=True),
            _cell(1, 1, "значение", header=True),
            _cell(2, 0, "work_mem"),
            _cell(2, 1, "64MB"),
        ],
        num_rows=3,
        num_cols=2,
    )

    doc = _extract(_document(tables=[table]))

    assert doc.tables is not None
    assert doc.tables[0].header == (), "склейка двух строк шапки была бы догадкой"
    assert doc.tables[0].rows == (
        ("группа", "группа"),
        ("имя", "значение"),
        ("work_mem", "64MB"),
    )


def test_merged_cell_text_stays_at_its_start() -> None:
    """Объединённая ячейка не размножает текст: остальные позиции — пустые строки."""
    table = _table_item(
        [
            _cell(0, 0, "параметр", header=True),
            _cell(0, 1, "значение", header=True),
            # Ячейка-объединение по двум столбцам: текст только в начале.
            SimpleNamespace(
                start_row_offset_idx=1,
                end_row_offset_idx=2,
                start_col_offset_idx=0,
                end_col_offset_idx=2,
                text="work_mem = 64MB",
                column_header=False,
                row_header=False,
            ),
        ],
        num_rows=2,
        num_cols=2,
    )

    doc = _extract(_document(tables=[table]))

    assert doc.tables is not None
    assert doc.tables[0].rows == (("work_mem = 64MB", ""),)


def test_document_without_tables_gives_empty_tuple() -> None:
    """Документ прочитан, таблиц нет → **пустой кортеж**, а не ``None``."""
    doc = _extract(_document(tables=[]))

    assert doc.error is None
    assert doc.tables == ()


def test_reader_without_tables_field_gives_none() -> None:
    """У читателя поля нет вовсе → ``None`` («таблиц не даёт»), а не пустой кортеж."""
    doc = _extract(_document(tables=None))

    assert doc.tables is None


def test_office_path_does_not_measure_tables() -> None:
    """Офисный путь объявленно не измеряет таблицы: ``prov`` там — лист/слайд, не страница."""
    table = _table_item([_cell(0, 0, "x")], num_rows=1, num_cols=1, page_no=1)
    extractor = DoclingDocumentExtractor(DocumentLevel.TEXT)
    result = extractor._convert(  # noqa: SLF001 — предмет теста
        _FakeConverter(_document(tables=[table])),
        "doc.docx",
        version="office",
        office=True,
    )

    assert result.tables is None


def test_partial_document_keeps_tables() -> None:
    """Частичный документ тоже называет прочитанные таблицы (тот же прибор)."""
    table = _table_item(
        [_cell(0, 0, "a"), _cell(0, 1, "b")], num_rows=1, num_cols=2, page_no=2
    )
    document = _document(tables=[table], markdown="| a | b |")
    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)

    doc = extractor._convert(  # noqa: SLF001 — предмет теста
        _FakeConverter(document, status="PARTIAL_SUCCESS"), "doc.pdf", version="v"
    )

    assert doc.partial is True
    assert doc.tables is not None and len(doc.tables) == 1
    assert doc.tables[0].page_no == 2
    assert doc.tables[0].header == ()
    assert doc.tables[0].rows == (("a", "b"),)


def test_text_layer_reader_gives_no_tables() -> None:
    """``text_layer`` (pdfium) таблиц не распознаёт вовсе → ``None``, не ``()``."""
    pdfium = pytest.importorskip("pypdfium2")
    assert pdfium is not None
    from core.adapters.extract_pdf_text import PdfiumDocumentExtractor
    from tests.adapters.test_extract_pdf_text import _make_pdf

    extractor = PdfiumDocumentExtractor()
    if not extractor.available:
        pytest.skip("pdfium недоступен")

    doc = extractor.extract_bytes(_make_pdf(["PostgreSQL 17"]), "doc.pdf")

    assert doc.error is None
    assert doc.tables is None, (
        "pdfium таблиц не распознаёт: это «не даёт вовсе», а не «искал и не нашёл»"
    )
