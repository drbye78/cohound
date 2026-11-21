"""Корпус прогона для локальных инструментов оценщика: узкое окно, только чтение (T1/T4).

Зачем отдельная реализация, если есть ``StorePort``. Инструмент не должен уметь больше,
чем «прочитать уже собранный материал одного документа». Полный ``StorePort`` умеет
писать ячейки, менять состояние прогона и читать **любой** продукт — то есть нарушал бы
I17 не по решению, а по возможности. Здесь окно сужено до четырёх чтений
(:class:`core.ports.tools.ToolCorpusPort`) и до **одного** продукта: чужой адрес даёт
пустой ответ, а не данные соседа по когорте.

Что этот модуль **не** делает. Не ретривит, не ранжирует, не создаёт фрагментов и не
пишет ничего. Возвращённый текст — уже собранный корпус, а не доказательство: цитата
по-прежнему обязана быть подстрокой текста фрагмента (I22), и инструмент этого правила
не ослабляет.

Два формата адреса документа, и почему их два. ``corpus_chunks.chunk_id`` — это
``{product_id}:{doc_version_id}:{chunk_no}`` (``corpus_index._chunk_id``), а
``corpus_chunks.doc_id`` — ``{product_id}:{content_hash}``: версия из хеша не выводится
(``short_hash`` необратим), отображение живёт в производной таблице
``corpus_doc_version``. Поэтому документ адресуется так:

1. ``product_id:doc_version_id`` — тот же способ, каким ``core.pipeline.prompt._document_id``
   читает документ из ``chunk_id`` (не второй парсер: модуль **переиспользует** его);
2. ``doc_id`` из ``corpus_doc_version`` — если у хранилища есть соединение, по которому
   это отображение читается.

Спрашиваются оба по порядку, и берётся первый непустой ответ: разные реализации
``StorePort`` принимают разные формы адреса, а «пусто» обязано означать «документа нет»,
а не «мы спросили не в той форме». Порядок зафиксирован, поэтому ответ детерминирован.

**Граница полосы (честно).** Читателя таблиц документа в ``StorePort`` нет, и добавлять
его отсюда нельзя (это чужая полоса). Поэтому :meth:`StoreToolCorpus.document_tables`
читает ``corpus_docs.tables`` через ``store.conn`` **только на чтение** — тот же приём,
что уже применён в ядре для версий документов (``evaluate.py`` читает
``iter_corpus_doc_versions`` через ``getattr``). Хранилище без соединения (фейк в тестах,
иная реализация порта) даёт ``None`` — «читатель таблиц не даёт», что и объявлено в
``core/domain/doc_tables.py``.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from typing import Any

from core.domain.doc_tables import (
    ExtractedTable,
    TableKey,
    TableRef,
    table_ref_from_json,
    tables_from_json,
)
from core.pipeline.prompt import _document_id
from core.ports.store import StorePort
from core.ports.types import Chunk

__all__ = ["HEADING_PATH_SEP", "StoreToolCorpus"]

#: Разделитель пути заголовков в ``heading_path``.
#:
#: Значение — то, чем его строит чанкер (``core/adapters/chunker_markdown.py:418``,
#: ``" > ".join(...)``); своего разделителя модуль не вводит, иначе «граница пути» здесь
#: и в корпусе означали бы разное. Импортировать константу из адаптера нельзя: матрица
#: слоёв (01 §1) запрещает ``pipeline → adapters``. Поэтому — объявленная копия с адресом
#: источника; при смене разделителя в чанкере это место обязано меняться вместе с ним.
HEADING_PATH_SEP = " > "


def _column(row: Any, name: str, index: int) -> Any:
    """Значение колонки из строки SQLite (``sqlite3.Row``) или кортежа (фейк в тестах)."""
    try:
        return row[name]
    except (IndexError, KeyError, TypeError):
        try:
            return row[index]
        except (IndexError, KeyError, TypeError):
            return None


class StoreToolCorpus:
    """``ToolCorpusPort`` поверх ``StorePort``: чтение корпуса **одного** продукта (I17).

    :param store: хранилище прогона; модуль пользуется только чтением.
    :param product_id: продукт, к которому привязано окно. Чанк другого продукта не
        отдаётся ни одним методом: адрес проверяется по префиксу ``chunk_id`` (форма
        ``product:doc_version:chunk_no``), а не по содержимому чанка.
    """

    def __init__(self, store: StorePort, product_id: str) -> None:
        if not str(product_id).strip():
            raise ValueError(
                "StoreToolCorpus: пустой product_id — окно обязано быть продуктовым (I17)"
            )
        self._store = store
        self._product_id = str(product_id)

    # -- публичный контракт порта ---------------------------------------- #

    def chunk(self, chunk_id: str) -> Chunk | None:
        """Чанк по адресу; ``None`` — адрес чужой или его нет в корпусе прогона."""
        if not self._owns(chunk_id):
            return None
        chunk = self._store.get_chunk(chunk_id)
        if chunk is None or not self._owns(chunk.chunk_id):
            return None
        return chunk

    def document_chunks(self, chunk_id: str) -> Sequence[Chunk]:
        """Чанки документа адресованного чанка, по ``chunk_no`` (пусто — адреса нет).

        Формы адреса документа перебираются в объявленном порядке (см. модульный
        докстринг); берётся первый непустой ответ. Чужие чанки отфильтровываются, даже
        если реализация ``iter_chunks_by_doc`` вернула больше, чем спросили.
        """
        if self.chunk(chunk_id) is None:
            return ()
        for address in self._document_addresses(chunk_id):
            found = [
                candidate
                for candidate in self._store.iter_chunks_by_doc(address)
                if self._owns(candidate.chunk_id)
            ]
            if found:
                return tuple(sorted(found, key=lambda c: (c.chunk_no, c.chunk_id)))
        return ()

    def section_chunks(self, chunk_id: str) -> Sequence[Chunk]:
        """Чанки **поддерева** раздела адресованного чанка (пусто — адреса нет).

        Граница задаётся **структурой документа**, а не окрестностью по ``chunk_no``:
        «соседние фрагменты» и «фрагменты того же раздела» — разные множества, и именно
        разница между ними снимает класс ошибки T1 (оговорка стоит в конце раздела, а
        цитата вырвана из середины).

        Раздел — **поддерево**, а не одна ветка: в него входят чанки с тем же
        ``heading_path``, с родительским (общая оговорка раздела) и с дочерним
        (конкретика подраздела: «Ограничения» → «Ограничения > Память»). Почему не только
        родитель: адрес часто попадает в родительский заголовок, а ограничение живёт в
        подразделе — то есть инструмент, видящий лишь родителя, не решал бы тот класс,
        который сам же объявляет. Цена поддерева — объём; она объявлена бюджетом
        инструмента и видимой обрезкой (``meta["truncated"]``), а не тишиной.

        Родство — по **границе** пути (:data:`HEADING_PATH_SEP`), а не по символу:
        «A > B» не является ни родителем, ни потомком «A > BC». Пустой ``heading_path``
        (куски до первого заголовка) — пролог документа, а не всякий раздел: он даёт
        только чанки с пустым путём.

        Порядок — ``chunk_no``, то есть порядок документа.
        """
        anchor = self.chunk(chunk_id)
        if anchor is None:
            return ()
        heading = (anchor.heading_path or "").strip()
        return tuple(
            candidate
            for candidate in self.document_chunks(chunk_id)
            if _in_section((candidate.heading_path or "").strip(), heading)
        )

    def document_tables(self, chunk_id: str) -> tuple[ExtractedTable, ...] | None:
        """Таблицы документа; ``None`` — читатель таблиц их не даёт (или адреса нет).

        Различие ``None``/``()`` — из ``core/domain/doc_tables.py``: ``None`` = «не
        искали / читатель не умеет», ``()`` = «искали и не нашли». Порча JSON читается
        как ``None`` (``tables_from_json``), а не как исключение: ответ инструмента не
        должен зависеть от целостности колонки.
        """
        if self.chunk(chunk_id) is None:
            return None
        conn = getattr(self._store, "conn", None)
        if conn is None:
            return None
        for address in self._document_addresses(chunk_id):
            try:
                row = conn.execute(
                    "SELECT tables FROM corpus_docs WHERE doc_id = ?", (address,)
                ).fetchone()
            except sqlite3.Error:
                # Колонки нет (файл прошлой схемы) или таблица не читается: «читатель
                # таблиц не даёт» — объявленный ответ, а не сбой инструмента.
                return None
            if row is None:
                continue
            return tables_from_json(_column(row, "tables", 0))
        return None

    def table_context(
        self, chunk_ids: Sequence[str]
    ) -> tuple[dict[str, TableRef | None], dict[TableKey, ExtractedTable]]:
        """Адреса таблиц пачкой — один запрос на фрагменты, один на документы.

        Цена. Срез ячейки — до ``cutoff_K`` фрагментов (10 на стенде), поэтому поштучные
        чтения умножали бы два запроса на число фрагментов на каждой из 905 ячеек. Здесь
        ровно два запроса на ячейку: ``corpus_chunks`` по первичному ключу (``IN (…)``) и
        ``corpus_docs`` по ``doc_id`` (индекс ``idx_chunks_doc`` — на обратную сторону,
        документ→чанки; сам ``corpus_docs.doc_id`` — первичный ключ).

        **Ключ карты — адрес документа из ``chunk_id``, а не ``corpus_chunks.doc_id``.**
        Это две разные строки: :func:`core.pipeline.prompt._document_id` обрезает числовой
        хвост ``chunk_id`` и даёт ``{product}:{doc_version_id}``, а ``corpus_chunks.doc_id``
        — ``{product}:{content_hash}``. Промпт ищет таблицу ровно по первому
        (:func:`core.pipeline.prompt._table_header_lines`), поэтому ключ из второго не
        совпал бы **ни на одном** чанке — и деградация была бы тихой: печаталось бы «адрес
        известен, объект не передан», что читается как «таблиц нет». Настоящий ``doc_id``
        нужен только как **промежуточное** значение для чтения ``corpus_docs.tables``.

        Что **не** попадает в первый словарь: чужой продукт (I17 — проверка по префиксу
        адреса, как у прочих методов окна) и адрес, которого в корпусе прогона нет. Это
        разные факты, и оба отличимы от «адрес неизвестен» (``None``).

        Второй словарь строится **только** по документам, где адрес реально встретился:
        читать таблицы всех документов среза значило бы платить за материал, который в
        промпт не пойдёт. Порча JSON и отсутствие колонки читаются как «неизвестно»
        (:func:`table_ref_from_json`/:func:`tables_from_json` возвращают ``None``), а не
        исключением: промпт не должен зависеть от целостности колонки.
        """
        conn = getattr(self._store, "conn", None)
        owned = self._owned_ids(chunk_ids)
        if conn is None or not owned:
            # Хранилище без соединения (фейк в тестах, иная реализация порта) адресов не
            # отдаёт: «неизвестно» — объявленный ответ, а не сбой.
            return {}, {}
        try:
            rows = conn.execute(
                "SELECT chunk_id, doc_id, table_ref FROM corpus_chunks"  # noqa: S608 - плейсхолдеры по числу адресов, значения — bind
                f" WHERE chunk_id IN ({', '.join('?' for _ in owned)})",
                tuple(owned),
            ).fetchall()
        except sqlite3.Error:
            # Колонки ``table_ref`` нет (файл прошлой схемы) или таблица не читается.
            return {}, {}
        addresses: dict[str, TableRef | None] = {}
        #: ``corpus_chunks.doc_id`` → адрес документа из ``chunk_id`` (первый компонент ключа).
        #: Один документ может быть адресован несколькими ``chunk_id``: адрес из ``chunk_id``
        #: у них один и тот же, поэтому первая встреченная форма и запоминается.
        key_of_doc: dict[str, str] = {}
        documents: list[str] = []
        for row in rows:
            chunk_id = _column(row, "chunk_id", 0)
            if not self._owns(chunk_id):
                continue
            ref = table_ref_from_json(_column(row, "table_ref", 2))
            addresses[str(chunk_id)] = ref
            doc_id = _column(row, "doc_id", 1)
            if ref is None or not doc_id:
                continue
            key_of_doc.setdefault(str(doc_id), _document_id(str(chunk_id)))
            if str(doc_id) not in documents:
                documents.append(str(doc_id))
        if not documents:
            return addresses, {}
        try:
            doc_rows = conn.execute(
                "SELECT doc_id, tables FROM corpus_docs"  # noqa: S608 - плейсхолдеры по числу адресов, значения — bind
                f" WHERE doc_id IN ({', '.join('?' for _ in documents)})",
                tuple(documents),
            ).fetchall()
        except sqlite3.Error:
            return addresses, {}
        tables: dict[TableKey, ExtractedTable] = {}
        for row in doc_rows:
            doc_id = _column(row, "doc_id", 0)
            if not doc_id or str(doc_id) not in documents:
                continue
            document = key_of_doc.get(str(doc_id)) or ""
            if not document:
                # ``chunk_id`` не разобрался в документ — ключ был бы выдуман (промпт ищет
                # по ``_document_id``), поэтому таблицы этого документа не кладутся вовсе.
                continue
            for table in tables_from_json(_column(row, "tables", 1)) or ():
                tables[(document, table.table_id)] = table
        return addresses, tables

    # -- внутреннее ------------------------------------------------------- #

    def _owned_ids(self, chunk_ids: Sequence[str]) -> list[str]:
        """Адреса этого продукта без повторов, в порядке входа (для ``IN (…)``)."""
        owned: list[str] = []
        for chunk_id in chunk_ids:
            if self._owns(chunk_id) and chunk_id not in owned:
                owned.append(chunk_id)
        return owned

    def _owns(self, chunk_id: str) -> bool:
        """Принадлежит ли адрес этому продукту (``chunk_id = product:doc_version:no``).

        Проверка по префиксу, а не по полю чанка: поля «продукт» у
        :class:`core.ports.types.Chunk` нет, а слаг продукта двоеточий не содержит —
        значит форма адреса различает продукты однозначно.
        """
        return isinstance(chunk_id, str) and chunk_id.startswith(f"{self._product_id}:")

    def _document_addresses(self, chunk_id: str) -> tuple[str, ...]:
        """Формы адреса документа для ``chunk_id``, в объявленном порядке запроса."""
        address = _document_id(chunk_id)
        if not address:
            return ()
        mapped = self._doc_id_by_version(address)
        if mapped and mapped != address:
            return (mapped, address)
        return (address,)

    def _doc_id_by_version(self, address: str) -> str | None:
        """``doc_id`` по адресу ``product:doc_version_id``; ``None`` — отображения нет.

        Отображение — производная таблица ``corpus_doc_version`` (16 §6). Её нет в
        read-only файле прошлой версии и нет у хранилища без соединения — тогда ``None``,
        и вызывающий остаётся с формой версии (объявленный фолбэк, не догадка).
        """
        conn = getattr(self._store, "conn", None)
        if conn is None:
            return None
        prefix = f"{self._product_id}:"
        if not address.startswith(prefix):
            return None
        version = address[len(prefix) :]
        if not version:
            return None
        try:
            row = conn.execute(
                "SELECT doc_id FROM corpus_doc_version"
                " WHERE product_id = ? AND doc_version_id = ?",
                (self._product_id, version),
            ).fetchone()
        except sqlite3.Error:
            return None
        if row is None:
            return None
        value = _column(row, "doc_id", 0)
        return str(value) if value else None


def _in_section(candidate: str, heading: str) -> bool:
    """Лежит ли ``candidate`` в поддереве раздела ``heading`` (сам, родитель, потомок).

    Родство — по границе пути: «A > B» не родитель и не потомок «A > BC». Пустой
    ``heading`` — пролог документа: он даёт только чанки с пустым путём (иначе пролог
    превратился бы в «весь документ», а это не раздел).
    """
    if candidate == heading:
        return True
    if not heading or not candidate:
        return False
    # родитель адресованного раздела: «Ограничения» для «Ограничения > Память»
    if heading.startswith(candidate + HEADING_PATH_SEP):
        return True
    # дочерний подраздел адресованного: «Ограничения > Память» для «Ограничения»
    return candidate.startswith(heading + HEADING_PATH_SEP)
