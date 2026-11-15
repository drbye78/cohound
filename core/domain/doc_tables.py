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
    "TableList",
    "tables_from_json",
    "tables_to_json",
]


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
