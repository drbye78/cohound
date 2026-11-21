"""Таблица как объект документа: шапка, строки, подпись, страница (O39, план ``26``).

Зачем отдельный носитель. До этой правки таблица существовала только как **текст**:
``export_to_markdown`` docling печатает её строками ``| … |``, шапка и подпись уезжают в
``context_prefix`` чанка, и привязки «это ячейка такой-то таблицы» нет ни у чанка, ни у
документа. Обратная сторона той же потери — отчёт: «сколько в корпусе таблиц» нельзя
назвать ничем, потому что таблицы как объекта не было ни в контракте читателя, ни в
схеме БД. Поэтому объект заводится **до** генерации чанков из него: чанкер здесь не
меняется вовсе, и состав чанков/``content_hash``/``run_id`` от этой правки не зависит.

Что такое ``None`` и что такое пустой кортеж — **два разных факта**, и смешивать их
нельзя (A3):

* ``None`` — читатель таблиц **не даёт вовсе**: ``text_layer`` (pdfium не распознаёт
  таблицы), HTML, EPUB, офисный путь docling, шов ``split``;
* ``()`` — читатель таблицы **искал** и не нашёл ни одной.

Из этого различия считается отчёт (``documents_unmeasured`` — это ровно ``None``), и
именно оно делает честной фразу «таблиц нет» вместо «мы их не искали».

Почему ``table_id`` — нумерация, а не хеш. ``t0``/``t1``/… по порядку в документе
детерминированы при пере-извлечении того же текста тем же механизмом, а хеш содержимого
менялся бы от любой правки ячейки — то есть адрес объекта «переезжал» бы вместе с
данными, и «таблица t3 в прошлом цикле» перестала бы значить что-либо. Идентичность
здесь — место в документе, а не содержимое.

Почему нет ``n_rows``/``n_cols``. Это ``len()``. Копия разошлась бы с содержимым ровно
так же, как разошлась бы любая другая денормализация: писатель обязан помнить про неё на
каждом пути, и один забытый путь даёт молча неверное число.

Инвариант (:meth:`ExtractedTable.__post_init__`): строка данных обязана иметь ту же
ширину, что шапка (если шапка есть) либо первую строку. Нарушение — отказ модели, а не
тихая обрезка: обрезанная строка объявила бы таблицу, которой читатель не видел.

Нейтрален (§1.3): здесь нет предметных терминов — только структура и числа.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

__all__ = [
    "ExtractedTable",
    "TableKey",
    "TableList",
    "TableRef",
    "table_ref_from_json",
    "table_ref_to_json",
    "tables_from_json",
    "tables_to_json",
]

#: Ключ карты таблиц для шапки фрагмента: ``(документ, table_id)``.
#:
#: **Первый компонент — адрес документа, выведенный из ``chunk_id``**
#: (:func:`core.pipeline.prompt._document_id`: числовой хвост отрезан, ``{product}:{doc_version_id}``),
#: а **не** ``corpus_docs.doc_id`` (``{product}:{content_hash}``). Это две разные строки, и
#: склейка ключа обязана быть вычислима из того, что есть у фрагмента в промпте: промпт
#: печатает ``document:`` из ``chunk_id`` и ищет таблицу по тому же значению. Ключ из
#: ``doc_id`` не совпал бы ни на одном чанке, и потеря была бы тихой — «адрес известен,
#: объект не передан» читается как «таблиц нет». Настоящий ``doc_id`` остаётся внутренним
#: адресом чтения ``corpus_docs.tables``.
#:
#: Один ``table_id`` недостаточен, и это не теория: ``table_id`` **локален для документа**
#: (``t0``/``t1``/… по порядку таблиц в документе), а срез одной ячейки регулярно содержит
#: фрагменты **разных** документов. Карта по одному ``table_id`` тогда молча подставила бы
#: таблицу одного документа к фрагменту другого — ровно тот класс ошибки, ради которого
#: табличная шапка и печатается.
#:
#: Объявлен здесь, а не в промпте, потому что ключ используют и порты, и хранилище
#: (``core/ports/tools.py``, ``core/store/``): ``ports``/``store`` не имеют права
#: импортировать ``pipeline`` (§4.1, гейт ``tests/selfcheck.py::_import_graph``).
TableKey = tuple[str, str]


@dataclass(frozen=True)
class ExtractedTable:
    """Таблица документа так, как её отдал читатель (O39 фаза 1 плана ``26``).

    ``page_no`` — страница из провенанса документа (``prov[0].page_no`` у docling,
    нумерация **1-based**, проверено по коду библиотеки 15.11.2026: ``Page(page_no=i + 1)``
    и ``PyPdfiumPageBackend.page_no = self._page_no + 1``). ``None`` — читатель страницу не
    дал; ``0`` не бывает: «страница 0» — не факт, а выдумка.

    ``caption`` — подпись, **если читатель её отдаёт**; ``None`` — подписи нет либо
    читатель её не сообщает. Пустая строка не пишется: пустая подпись — это отсутствие
    подписи, а не подпись из нуля символов.

    ``header`` — шапка (имена столбцов); пустой кортеж — шапки нет. ``rows`` — строки
    **данных**; шапка в них не повторяется. Ширина строки задаётся шапкой, а если её нет
    — первой строкой: инвариант держится в :meth:`__post_init__`.

    Объединённые ячейки (``row_span``/``col_span``) заполняются по **началу**: текст стоит
    в той ячейке, где его отдал читатель, остальные позиции объединения — пустые строки.
    Размножение текста по объединению было бы догадкой о том, что там написано.
    """

    table_id: str
    page_no: int | None = None
    caption: str | None = None
    header: tuple[str, ...] = ()
    rows: tuple[tuple[str, ...], ...] = ()

    def __post_init__(self) -> None:
        """Держит инвариант формы: ширина строк одна и та же, страница 1-based.

        Проверка живёт здесь, а не в адаптере, потому что объект пересекает границы:
        читатель → ``extract_cache`` → строка корпуса → отчёт. Таблица с рваной строкой —
        ложная запись о документе, независимо от того, кто её собрал, и отказ модели
        дешевле, чем число в CSV, которому нельзя верить.
        """
        if not str(self.table_id).strip():
            raise ValueError(
                "ExtractedTable: пустой table_id — таблица обязана иметь адрес в документе"
            )
        if self.page_no is not None and (
            isinstance(self.page_no, bool)
            or not isinstance(self.page_no, int)
            or self.page_no < 1
        ):
            raise ValueError(
                "ExtractedTable: page_no обязан быть >= 1 или None — нумерация "
                f"страниц 1-based, получено {self.page_no!r}"
            )
        width = len(self.header) if self.header else None
        for index, row in enumerate(self.rows):
            if not isinstance(row, tuple):
                raise ValueError(
                    "ExtractedTable: строка таблицы обязана быть кортежем, "
                    f"получено {type(row).__name__} (строка {index})"
                )
            if any(not isinstance(cell, str) for cell in row):
                raise ValueError(
                    f"ExtractedTable: в строке {index} есть не строка — ячейка таблицы "
                    "обязана быть текстом"
                )
            if width is None:
                width = len(row)
                continue
            if len(row) != width:
                source = "шапки" if self.header else "первой строки"
                raise ValueError(
                    "ExtractedTable: ширина строки не равна ширине "
                    f"{source} — строка {index} содержит {len(row)} ячеек при {width}"
                )


#: Список таблиц документа: ``()`` — читатель искал и не нашёл, ``None`` — не даёт вовсе.
#: Контракт различия — в :attr:`core.ports.types.ExtractedDoc.tables`.
TableList = tuple[ExtractedTable, ...]


@dataclass(frozen=True)
class TableRef:
    """Адрес куска **внутри** таблицы: какая таблица и какие её строки (O39 фаза 2).

    Зачем отдельный носитель у чанка. Фаза 1 завела таблицу как объект, но связать с ней
    чанк было нечем: чанк таблицы нёс шапку в ``context_prefix`` и больше ничего, поэтому
    «эта цитата из таблицы t3» сказать было нельзя, и строка ``outputs/tables.csv`` не
    сходилась ни с одним чанком. :class:`TableRef` — это и есть связь: ``table_id``
    называет таблицу в ``corpus_docs.tables``, ``row_start``/``row_end`` — какие её строки
    данных попали в кусок.

    **Почему номер строки, а не номер столбца.** Нарезка режет таблицу только по границам
    строк (``_table_cuts``), поэтому кусок таблицы покрывает **все** её столбцы, и «номер
    столбца» у чанка был бы константой, а не сведением. Измерено 16.11.2026 на 22
    документах / 181 таблице: ни одна строка не превышает бюджета куска
    (``blocks_with_row_over_chunk = 0``), то есть разреза **внутри** строки не бывает.
    Если он появится (таблица с очень широкими ячейками), ``row_start``/``row_end``
    перестанут описывать кусок, и тогда — и только тогда — понадобится номер столбца;
    условие названо, а не спрятано.

    ``row_start``/``row_end`` — 1-based **номера строк данных** (шапка не считается:
    она не строка данных, а её имена) и **обе границы включительны**: кусок
    ``row_start=4, row_end=7`` покрывает четыре строки, то есть срез
    ``rows[row_start-1:row_end]``. Так их и пишет чанкер (``covered[0]+1`` …
    ``covered[-1]+1``), и так их читает шапка промпта; полуоткрытая трактовка теряла бы
    последнюю строку куска. Оба ``None`` — кусок таблицы содержит шапку, но ни
    одной строки данных (так бывает у короткой таблицы, чья шапка ушла отдельным куском);
    одна пара вместо двух половин — по тому же правилу, что ``char_start``/``char_end``.

    ``None`` вместо объекта у чанка — **не** «не таблица», а «неизвестно», и причин две:
    читатель таблиц их не даёт (``text_layer``, HTML, EPUB, офис, шов) либо число
    таблиц-объектов не совпало с числом табличных блоков текста, и сопоставление по порядку
    было бы догадкой. Первая причина видна на уровне документа без чтения чанков:
    ``corpus_docs.tables IS NULL`` против непустого списка (``[]`` — искал и не нашёл).
    Вторая на уровне отчёта **не различается** — и это названный пробел, а не забытое поле:
    отдельного счётчика «сопоставление не выполнялось» у прогона нет, потому что его
    пришлось бы считать полным обходом ``corpus_chunks`` (253 306 строк на стенде) ради
    одного числа; мерит его прибор (``scripts/measure_tables.py``:
    ``objects_vs_blocks_differ``, ``table_chunks_unbound``), и там же опубликовано, что на
    этом корпусе несовпадений не было (22 из 22 документов).
    """

    table_id: str
    row_start: int | None = None
    row_end: int | None = None

    def __post_init__(self) -> None:
        """Держит инвариант адреса: таблица названа, диапазон строк — пара и по порядку."""
        if not str(self.table_id).strip():
            raise ValueError(
                "TableRef: пустой table_id — адрес куска обязан называть таблицу"
            )
        if (self.row_start is None) != (self.row_end is None):
            raise ValueError(
                "TableRef: задана только одна граница строк "
                f"(row_start={self.row_start!r}, row_end={self.row_end!r}) — диапазон "
                "задаётся парой"
            )
        row_start, row_end = self.row_start, self.row_end
        if row_start is None or row_end is None:
            return
        for name, value in (("row_start", row_start), ("row_end", row_end)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(
                    f"TableRef: {name} обязан быть >= 1 — строки данных нумеруются с "
                    f"единицы, получено {value!r}"
                )
        if row_start > row_end:
            raise ValueError(
                "TableRef: перевёрнутый диапазон строк "
                f"[{row_start}; {row_end}] — нужен row_start <= row_end"
            )


def table_ref_to_json(ref: TableRef | None) -> str | None:
    """Адрес куска → текст колонки ``corpus_chunks.table_ref``; «неизвестно» → ``None``.

    ``None`` пишется как ``NULL`` — ровно как у таблиц документа: «не знаем» и «не таблица»
    разные факты, и свести их к пустому объекту значило бы объявить кусок обычным текстом.
    """
    if ref is None:
        return None
    return json.dumps(
        {
            "table_id": ref.table_id,
            "row_start": ref.row_start,
            "row_end": ref.row_end,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def table_ref_from_json(raw: Any) -> TableRef | None:
    """Текст колонки → адрес куска; всё непонятное → ``None`` (как :func:`tables_from_json`).

    Порча колонки читается как «адрес неизвестен», а не как исключение: отчёт обязан
    строиться и на повреждённой БД, а адрес, собранный из испорченной записи, был бы
    догадкой (A3).
    """
    if not isinstance(raw, str) or not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return TableRef(
            table_id=str(payload.get("table_id") or ""),
            row_start=payload.get("row_start"),
            row_end=payload.get("row_end"),
        )
    except (TypeError, ValueError):
        return None


def tables_to_json(tables: TableList | None) -> str | None:
    """Таблицы → текст колонки ``corpus_docs.tables``; «читатель не даёт» → ``None``.

    ``None`` пишется как ``NULL``, пустой список — как ``[]``: это **разные** факты
    («не искали» против «искали и не нашли»), и свести их к одному значило бы потерять
    ровно тот ответ, ради которого колонка заведена. Формат — JSON-список объектов: он
    читается глазами в ``litecli`` и не требует схемы.
    """
    if tables is None:
        return None
    return json.dumps(
        [
            {
                "table_id": table.table_id,
                "page_no": table.page_no,
                "caption": table.caption,
                "header": list(table.header),
                "rows": [list(row) for row in table.rows],
            }
            for table in tables
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def tables_from_json(raw: Any) -> TableList | None:
    """Текст колонки ``corpus_docs.tables`` → таблицы; всё непонятное → ``None``.

    Порча колонки читается как «читатель таблиц не даёт», а **не** как исключение:
    ``report`` обязан строиться и на повреждённой БД, а таблица, собранная из
    испорченной записи, была бы догадкой (A3). Поэтому здесь нет ни одного ``raise``:
    не список, не объект, нарушенная ширина строки (её ловит
    :meth:`ExtractedTable.__post_init__`) — всё ``None``.

    Пустой список — **не** порча: ``[]`` означает «читатель искал и не нашёл», и это
    возвращается пустым кортежем, а не ``None``.
    """
    if not isinstance(raw, str) or not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, list):
        return None
    tables: list[ExtractedTable] = []
    for item in payload:
        if not isinstance(item, dict):
            return None
        header = item.get("header") or []
        rows = item.get("rows") or []
        caption = item.get("caption")
        if not isinstance(header, list) or not isinstance(rows, list):
            return None
        if caption is not None and not isinstance(caption, str):
            return None
        if any(not isinstance(row, list) for row in rows):
            return None
        try:
            tables.append(
                ExtractedTable(
                    table_id=str(item.get("table_id") or ""),
                    page_no=item.get("page_no"),
                    caption=caption,
                    header=tuple(str(cell) for cell in header),
                    rows=tuple(tuple(str(cell) for cell in row) for row in rows),
                )
            )
        except (TypeError, ValueError):
            return None
    return tuple(tables)
