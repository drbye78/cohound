"""Страница у **цитаты**: доказательство называет страницу, а не только документ (O36).

Зачем это отдельно от :mod:`core.pipeline.page_provenance`. Тот модуль публикует **наличие
карты** у документа (у кого она есть и чьи цитаты страницу назвать не могут), и его форма
отвечает на вопрос «где провенанс потерян». Здесь — другой предмет: **сколько цитат
страницу назвали** и по какой причине не назвали остальные. Одно число не заменяет другое:
карта у документа может быть, а чанк — не быть точным срезом, и тогда страницы у цитаты
нет при полной карте.

Что публикуется:

* ``outputs/evidence_pages.csv`` — строка на evidence: продукт, параметр, вид источника и
  тип доказательства, документ, чанк, смещения, ``page``/``page_end``, ``page_status``,
  ``block``/``block_status``, ``table``/``table_rows``/``table_status``;
* блок ``page_provenance`` манифеста (:func:`evidence_pages_summary`) — те же числа,
  свёрнутые до прогона и продукта, чтобы факт был виден без разбора CSV;
* строка для оператора (:func:`human_line`) — в ``notes`` CLI-отчёта;
* карта ``chunk_id → (page, page_end)`` для battle card (:func:`pages_by_chunk`): цитата
  в ячейке матрицы называет страницу тем же расчётом, что и файл.

**Почему пара страниц, а не одно число.** Чанк может пересекать границу страниц; одно число
объявило бы «цитата на стр. 12», когда половина её на 13. Равенство ``page == page_end`` —
обычный случай одной страницы, и файл не выдумывает диапазон там, где его нет.

**Почему страница — функция.** ``corpus_chunks.page_number`` не существует и не заводится
(см. :mod:`core.domain.page_map`): копию пришлось бы поддерживать на каждом пути записи
чанка. Здесь она считается из ``(page_map документа, char_start чанка)`` — из тех же двух
чисел, что уже лежат в БД.

**Блок (O36 фаза 2) — та же функция, а не колонка.** ``corpus_chunks.block_id`` не
существует по той же причине: адрес считается из ``(blocks документа, char_start чанка)``
(:func:`core.domain.doc_blocks.block_id_for`). Правило границы у него **то же**, что у
страницы (разрыв уходит следующему блоку, вне списка — первый/последний), потому что это
один и тот же вопрос «какому отрезку принадлежит смещение».

``page_status`` — машинный токен, ровно один из пяти, и каждый называет **свой** носитель
отсутствия, а не «неизвестно вообще»:

* ``mapped`` — страница известна;
* ``no_page_map`` — документ есть, карты нет (pdfium вне ``text_layer``, docling, HTML,
  EPUB, Office, ``split``): читатель карту не дал;
* ``no_char_span`` — чанк есть, но ``char_start is None`` (абзацный чанкер: кусок не
  является точным срезом текста документа);
* ``no_chunk`` — ``source_id`` не найден в ``corpus_chunks``: реестровый, ручной или
  унаследованный evidence — это **не** корпусная цитата, и страницы у неё нет по природе;
* ``no_doc`` — чанк есть, документа нет (битая ссылка): факт, а не тишина.

``block_status`` — те же пять исходов для **блока**, и каждый тоже называет свой носитель
отсутствия:

* ``mapped`` — блок известен;
* ``no_blocks`` — у документа блоков нет: читатель блоков их не даёт (HTML, текст, EPUB,
  Office, docling, ``split``) либо смотрел и не дал (``()``) — в обоих случаях назвать
  нечего, а различие двух фактов видно на уровне документа
  (``runs.blocks_json.documents_unmeasured``);
* ``no_char_span`` — чанк есть, но ``char_start is None``;
* ``no_chunk``/``no_doc`` — как у страницы.

**Таблица (O39 фаза 2) — третий адрес, и здесь он колонка, а не функция.** Страница и блок
считаются из носителя документа (карты страниц / списка блоков) по ``char_start`` чанка. У
таблицы такого источника нет: объект таблицы (``corpus_docs.tables``) смещений в тексте
**не несёт** — docling их не сообщает, — поэтому «какой кусок покрывает эти строки таблицы»
знает только нарезка, и она пишет ответ в ``corpus_chunks.table_ref``
(:class:`core.domain.doc_tables.TableRef`). Считать адрес таблицы из смещений здесь было бы
догадкой (A3), поэтому он **читается** из колонки чанка. Текст чанка от этой правки не
менялся (измерено 16.11.2026: ``chunk_text`` с таблицами и без — байт в байт).

``table_status`` — пять исходов, и каждый называет свой носитель отсутствия:

* ``mapped`` — таблица названа и диапазон строк есть;
* ``table_without_rows`` — таблица названа, диапазона нет: шапка ушла отдельным куском либо
  число строк объекта и табличного блока не сошлось;
* ``no_table_ref`` — адреса нет: читатель таблиц их не даёт (``text_layer``/pdfium, HTML,
  EPUB, Office, ``split``) либо сопоставление не выполнялось. На уровне документа видно
  первое различие (``runs.tables_json.documents_unmeasured`` — «читатель не даёт» против
  «таблицы прочитаны»); отдельного счётчика «сопоставление не выполнялось» (``documents_unbound``,
  назван в :class:`core.domain.doc_tables.TableRef`) у отчёта пока **нет производителя**, и
  выдавать отсутствие счётчика за ноль нельзя (A3);
* ``no_chunk``/``no_doc`` — как у страницы.

``table_ref is None`` у чанка — **не** «кусок не из таблицы», а «адрес неизвестен».

Пустая ячейка ``page``/``page_end``/``block``/``table``/``table_rows`` — «не известно», не
ноль.

Источник — **персистентные** данные (``corpus_docs``/``corpus_chunks``/``cell_evidence``), а
не память прогона: на resume фазы корпуса короткозамкнуты, а отчёт обязан называть тот же
факт. Сбой чтения не роняет отчёт (он вторичен к фазе): строк тогда нет, и это честно —
«не прочитали», а не «страниц нет».

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from core.domain.doc_blocks import BlockList, block_id_for, blocks_from_json
from core.domain.page_map import PageMap, page_map_from_json, page_number_for
from core.util.ids import DOC_VERSION_PREFIX, parse_source_id

__all__ = [
    "BLOCK_STATUSES",
    "BLOCK_STATUS_MAPPED",
    "BLOCK_STATUS_NO_BLOCKS",
    "BLOCK_STATUS_NO_CHAR_SPAN",
    "BLOCK_STATUS_NO_CHUNK",
    "BLOCK_STATUS_NO_DOC",
    "EVIDENCE_CSV_COLUMNS",
    "PAGE_STATUS_MAPPED",
    "PAGE_STATUS_NO_CHAR_SPAN",
    "PAGE_STATUS_NO_CHUNK",
    "PAGE_STATUS_NO_DOC",
    "PAGE_STATUS_NO_PAGE_MAP",
    "TABLE_STATUSES",
    "TABLE_STATUS_MAPPED",
    "TABLE_STATUS_NO_CHUNK",
    "TABLE_STATUS_NO_DOC",
    "TABLE_STATUS_NO_TABLE_REF",
    "TABLE_STATUS_WITHOUT_ROWS",
    "collect_evidence_pages",
    "evidence_pages_summary",
    "human_line",
    "pages_by_chunk",
    "summary_with_products",
    "write_evidence_pages_artifact",
]

#: Колонки ``outputs/evidence_pages.csv`` (порядок стабилен: файл читают глазами и скриптом).
EVIDENCE_CSV_COLUMNS: tuple[str, ...] = (
    "product_id",
    "param_id",
    "source_kind",
    "evidence_type",
    "doc_id",
    "chunk_id",
    "char_start",
    "char_end",
    "page",
    "page_end",
    "page_status",
    "block",
    "block_status",
    "table",
    "table_rows",
    "table_status",
)

#: Страница известна.
PAGE_STATUS_MAPPED = "mapped"
#: Документ есть, карты страниц нет (читатель её не дал).
PAGE_STATUS_NO_PAGE_MAP = "no_page_map"
#: Чанк есть, но смещений у него нет — кусок не является точным срезом текста.
PAGE_STATUS_NO_CHAR_SPAN = "no_char_span"
#: ``source_id`` не найден в ``corpus_chunks`` — это не корпусная цитата.
PAGE_STATUS_NO_CHUNK = "no_chunk"
#: Чанк есть, документа нет — битая ссылка.
PAGE_STATUS_NO_DOC = "no_doc"

#: Блок известен.
BLOCK_STATUS_MAPPED = "mapped"
#: У документа блоков нет: читатель их не даёт либо смотрел и не дал.
BLOCK_STATUS_NO_BLOCKS = "no_blocks"
#: Чанк есть, но смещений у него нет — кусок не является точным срезом текста.
BLOCK_STATUS_NO_CHAR_SPAN = "no_char_span"
#: ``source_id`` не найден в ``corpus_chunks`` — это не корпусная цитата.
BLOCK_STATUS_NO_CHUNK = "no_chunk"
#: Чанк есть, документа нет — битая ссылка.
BLOCK_STATUS_NO_DOC = "no_doc"

#: Все исходы для блока — в порядке объявления (сводка печатает их всегда, включая нули).
BLOCK_STATUSES: tuple[str, ...] = (
    BLOCK_STATUS_MAPPED,
    BLOCK_STATUS_NO_BLOCKS,
    BLOCK_STATUS_NO_CHAR_SPAN,
    BLOCK_STATUS_NO_CHUNK,
    BLOCK_STATUS_NO_DOC,
)

#: Все исходы — в порядке объявления: сводка печатает их всегда, включая нули (ноль здесь
#: честен: «такого класса у этого прогона не было»).
PAGE_STATUSES: tuple[str, ...] = (
    PAGE_STATUS_MAPPED,
    PAGE_STATUS_NO_PAGE_MAP,
    PAGE_STATUS_NO_CHAR_SPAN,
    PAGE_STATUS_NO_CHUNK,
    PAGE_STATUS_NO_DOC,
)

#: Таблица названа и диапазон строк есть.
TABLE_STATUS_MAPPED = "mapped"
#: Таблица названа, диапазона строк нет (шапка ушла отдельным куском либо число строк
#: объекта и табличного блока не сошлось).
TABLE_STATUS_WITHOUT_ROWS = "table_without_rows"
#: Адреса нет: читатель таблиц их не даёт либо сопоставление не выполнялось (второго
#: счётчика у отчёта нет — ``documents_unbound`` назван в ``TableRef``, но не публикуется).
TABLE_STATUS_NO_TABLE_REF = "no_table_ref"
#: ``source_id`` не найден в ``corpus_chunks`` — это не корпусная цитата.
TABLE_STATUS_NO_CHUNK = "no_chunk"
#: Чанк есть, документа нет — битая ссылка.
TABLE_STATUS_NO_DOC = "no_doc"

#: Все исходы для таблицы — в порядке объявления (сводка печатает их всегда, включая нули).
TABLE_STATUSES: tuple[str, ...] = (
    TABLE_STATUS_MAPPED,
    TABLE_STATUS_WITHOUT_ROWS,
    TABLE_STATUS_NO_TABLE_REF,
    TABLE_STATUS_NO_CHUNK,
    TABLE_STATUS_NO_DOC,
)


def _text(value: Any) -> str:
    """Строка из значения (``StrEnum`` — своим ``value``, отсутствие — пустая строка)."""
    if value is None:
        return ""
    return str(getattr(value, "value", value))


def _corpus_docs(store: Any, run_id: str | None) -> dict[str, Any]:
    """Документы корпуса прогона по ``doc_id``; сбои чтения → пустая карта.

    ``run_id`` обязателен: ``iter_corpus_docs`` проверяет присутствие прогона в БД и не
    выдаёт документы чужого прогона. Отсутствие аргумента — «источника нет», а не повод
    прочитать весь корпус.
    """
    if store is None or not run_id:
        return {}
    reader = getattr(store, "iter_corpus_docs", None)
    if not callable(reader):
        return {}
    try:
        return {str(doc.doc_id): doc for doc in reader(str(run_id))}
    except Exception:  # noqa: BLE001 — отчёт вторичен к фазе
        return {}


def _version_map(store: Any, run_id: str, docs: Mapping[str, Any]) -> dict[tuple[str, str], str]:
    """``(product_id, doc_version_id) → doc_id`` — адрес версии в адрес документа (16 §6).

    Нужен потому, что ``chunk_id`` несёт **версию** документа (``dv_...``), а карта страниц
    живёт у документа (``product_id:content_hash``); обратной функции у ``short_hash`` нет.
    Носитель — производная таблица ``corpus_doc_version``; читается по продукту, потому что
    её читатель устроен так же, как ``iter_corpus_docs`` (область — корпус прогона).
    Сбой чтения продукта → его версии не отображены: страница тогда не названа, но и отчёт
    не упал.
    """
    reader = getattr(store, "iter_corpus_doc_versions", None)
    if not callable(reader):
        return {}
    mapping: dict[tuple[str, str], str] = {}
    for product_id in sorted({str(getattr(doc, "product_id", "")) for doc in docs.values()}):
        for version, doc_id in _versions_of_product(reader, str(run_id), product_id).items():
            mapping[(product_id, str(version))] = str(doc_id)
    return mapping


def _versions_of_product(reader: Any, run_id: str, product_id: str) -> dict[str, str]:
    """Отображение версий одного продукта; сбой чтения → пусто («не прочитали»)."""
    try:
        return dict(reader(run_id, product_id))
    except Exception:  # noqa: BLE001 — отчёт вторичен к фазе
        return {}


def _evidence(store: Any, run_id: str | None) -> list[Any]:
    """Все evidence прогона (одним чтением, в порядке вставки); сбои чтения → ``[]``.

    Читатель — ``iter_evidence`` (один запрос на прогон): вопрос задаётся ко **всем**
    цитатам, а построчный читатель дал бы запрос на ячейку. Отсутствие читателя у стора —
    «источника нет», а не повод выдумывать строки.
    """
    if store is None or not run_id:
        return []
    reader = getattr(store, "iter_evidence", None)
    if not callable(reader):
        return []
    try:
        return list(reader(str(run_id)))
    except Exception:  # noqa: BLE001 — отчёт вторичен к фазе
        return []


def _page_map_of(doc: Any) -> PageMap | None:
    """Карта страниц документа (``None`` — читатель её не дал)."""
    raw = getattr(doc, "page_map", None)
    if raw is None:
        return None
    return raw if isinstance(raw, tuple) else page_map_from_json(raw)


def _blocks_of(doc: Any) -> BlockList | None:
    """Блоки документа (``None`` — читатель их не даёт, ``()`` — смотрел и не дал)."""
    raw = getattr(doc, "blocks", None)
    if raw is None:
        return None
    return raw if isinstance(raw, tuple) else blocks_from_json(raw)


def _doc_id_of(source_id: str, versions: Mapping[tuple[str, str], str]) -> str | None:
    """``doc_id`` по адресу чанка; ``None`` — адрес не разбирается либо версия не отображена.

    Адрес ``product:version:chunk_no`` (16 §6) переводится в документ через
    ``corpus_doc_version``; legacy-адрес ``product:content_hash:chunk_no`` несёт хеш текста,
    и ``doc_id`` корпуса — ровно ``product:hash``, поэтому он берётся напрямую.
    """
    try:
        ref = parse_source_id(source_id)
    except ValueError:
        return None
    if ref.doc_id.startswith(DOC_VERSION_PREFIX):
        mapped = versions.get((ref.product_id, ref.doc_id))
        return str(mapped) if mapped is not None else None
    return f"{ref.product_id}:{ref.doc_id}"


def _row(
    evidence: Any,
    *,
    chunk: Any | None,
    doc_id: str | None,
    page_map: PageMap | None,
    blocks: BlockList | None = None,
) -> dict[str, Any]:
    """Строка артефакта по одному evidence — включая **причину** отсутствия страницы и блока.

    Порядок проверок у блока тот же, что у страницы (чанк → документ → носитель → смещение):
    причина обязана называть **свой** носитель отсутствия, иначе «нет блока» читается как
    «неизвестно вообще». ``blocks = ()`` (читатель смотрел и не дал) даёт ``no_blocks``:
    назвать нечего — тот же факт для цитаты, а различие двух состояний видно на уровне
    документа (``runs.blocks_json``).
    """
    char_start = getattr(chunk, "char_start", None) if chunk is not None else None
    char_end = getattr(chunk, "char_end", None) if chunk is not None else None
    page: int | None = None
    page_end: int | None = None
    if chunk is None:
        status = PAGE_STATUS_NO_CHUNK
    elif doc_id is None:
        status = PAGE_STATUS_NO_DOC
    elif page_map is None:
        status = PAGE_STATUS_NO_PAGE_MAP
    elif char_start is None:
        status = PAGE_STATUS_NO_CHAR_SPAN
    else:
        status = PAGE_STATUS_MAPPED
        page = page_number_for(char_start, page_map)
        page_end = page
        # Конец куска — исключающий: последний символ цитаты — ``char_end - 1``. Диапазон
        # называется только тогда, когда кусок действительно пересекает границу страниц.
        if isinstance(char_end, int) and char_end > char_start:
            page_end = page_number_for(char_end - 1, page_map)
    block: str | None = None
    if chunk is None:
        block_status = BLOCK_STATUS_NO_CHUNK
    elif doc_id is None:
        block_status = BLOCK_STATUS_NO_DOC
    elif not blocks:
        # ``None`` («читатель не даёт») и ``()`` («смотрел и не дал») для цитаты — один
        # факт: назвать нечего. Различие состояний публикуется на уровне документа.
        block_status = BLOCK_STATUS_NO_BLOCKS
    elif char_start is None:
        block_status = BLOCK_STATUS_NO_CHAR_SPAN
    else:
        block_status = BLOCK_STATUS_MAPPED
        block = block_id_for(char_start, blocks)
        if block is None:
            # Точный срез есть, но список блоков его не покрывает (порча либо блоки
            # другого текста): адрес не выдумывается (A3), и причина — «блоков нет».
            block_status = BLOCK_STATUS_NO_BLOCKS
    # Таблица — третий адрес (O39 фаза 2). Порядок проверок тот же (чанк → документ →
    # носитель → строки), но носитель здесь **колонка** ``corpus_chunks.table_ref``: объект
    # таблицы смещений в тексте не несёт, и посчитать адрес из ``char_start`` было бы
    # догадкой. ``table_ref is None`` — «неизвестно», а не «кусок не из таблицы».
    table_ref = getattr(chunk, "table_ref", None) if chunk is not None else None
    table: str | None = None
    table_rows: str | None = None
    if chunk is None:
        table_status = TABLE_STATUS_NO_CHUNK
    elif doc_id is None:
        table_status = TABLE_STATUS_NO_DOC
    elif table_ref is None:
        table_status = TABLE_STATUS_NO_TABLE_REF
    elif table_ref.row_start is None:
        # Таблица названа, строк данных в куске нет: шапка ушла отдельным куском либо
        # число строк объекта и табличного блока не сошлось — адрес строк не выдумывается.
        table_status = TABLE_STATUS_WITHOUT_ROWS
        table = table_ref.table_id
    else:
        table_status = TABLE_STATUS_MAPPED
        table = table_ref.table_id
        # Половины диапазона не бывает (инвариант :class:`TableRef`), поэтому ``None`` во
        # второй границе здесь означал бы нарушенный контракт, а не «строк нет».
        row_start = int(table_ref.row_start)
        row_end = table_ref.row_end or row_start
        # Пара печатается одним полем: «4-7», а равенство границ — одним числом. Это не
        # диапазон страниц: строка таблицы не режется, поэтому пара всегда осмысленна.
        table_rows = f"{row_start}-{row_end}" if row_start != row_end else str(row_start)
    return {
        "product_id": _text(getattr(evidence, "product_id", "")),
        "param_id": _text(getattr(evidence, "param_id", "")),
        "source_kind": _text(getattr(evidence, "source_kind", "")),
        "evidence_type": _text(getattr(evidence, "evidence_type", "")),
        "doc_id": doc_id or "",
        "chunk_id": _text(getattr(evidence, "source_id", "")),
        "char_start": char_start if isinstance(char_start, int) else None,
        "char_end": char_end if isinstance(char_end, int) else None,
        "page": page,
        "page_end": page_end,
        "page_status": status,
        "block": block,
        "block_status": block_status,
        "table": table,
        "table_rows": table_rows,
        "table_status": table_status,
    }


def collect_evidence_pages(
    store: Any, run_id: str | None
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Строки артефакта и сводка — **один** расчёт (файл и манифест не могут разойтись).

    Порядок строк детерминирован — ``(product_id, param_id, source_id)``: файл читают
    глазами и сравнивают между прогонами. Evidence без корпусной цитаты тоже получает
    строку: именно она называет, что цитата страницу назвать не может.

    Сводка — блок манифеста (:func:`evidence_pages_summary` без продуктов плана): продукты
    без evidence в неё не попадают, потому что плана здесь нет — он известен вызывающему.
    Сбой чтения не бросает: ``([], {})`` — «не прочитали», а не «страниц нет».
    """
    docs = _corpus_docs(store, run_id)
    if not docs:
        return [], {}
    # Прогон присутствует в БД (иначе ``_corpus_docs`` пуст) — дальше адрес фиксирован.
    versions = _version_map(store, str(run_id), docs)
    evidence = _evidence(store, run_id)
    if not evidence:
        return [], {}
    source_ids = sorted(
        {
            str(getattr(item, "source_id", ""))
            for item in evidence
            if getattr(item, "source_id", None)
        }
    )
    chunks: dict[str, Any] = {}
    getter = getattr(store, "get_chunks", None)
    if callable(getter) and source_ids:
        try:
            chunks = dict(getter(source_ids))
        except Exception:  # noqa: BLE001 — отчёт вторичен к фазе
            chunks = {}
    rows: list[dict[str, Any]] = []
    for item in evidence:
        chunk_id = _text(getattr(item, "source_id", ""))
        chunk = chunks.get(chunk_id)
        doc_id = _doc_id_of(chunk_id, versions) if chunk is not None else None
        doc = docs.get(doc_id) if doc_id is not None else None
        rows.append(
            _row(
                item,
                chunk=chunk,
                doc_id=doc_id if doc is not None else None,
                page_map=_page_map_of(doc) if doc is not None else None,
                blocks=_blocks_of(doc) if doc is not None else None,
            )
        )
    rows.sort(key=lambda row: (row["product_id"], row["param_id"], row["chunk_id"]))
    return rows, _summary(rows)


def _summary(
    rows: Sequence[dict[str, Any]], products: Iterable[str] = ()
) -> dict[str, Any]:
    """Сводка по строкам: всего/со страницей/с блоком/с таблицей, по исходам и продуктам.

    ``by_product`` содержит **нули** для всех продуктов плана (симметрия с
    ``partial_docs_summary``): «у этого продукта цитат нет» — тоже факт. Продукты, которых
    нет в плане, не выдумываются.

    Блок (O36 фаза 2) и таблица (O39 фаза 2) считаются **теми же строками**, что и страница:
    второй расчёт разошёлся бы с файлом. ``with_block``/``by_block_status`` и
    ``with_table``/``with_table_rows``/``by_table_status`` — не новые сущности, а те же
    числа для второго и третьего адреса цитаты. ``with_table`` — сколько цитат назвали
    таблицу (диапазон строк при этом может отсутствовать), ``with_table_rows`` — сколько
    назвали и таблицу, и строки.
    """
    by_status = {status: 0 for status in PAGE_STATUSES}
    by_block_status = {status: 0 for status in BLOCK_STATUSES}
    by_table_status = {status: 0 for status in TABLE_STATUSES}
    by_product: dict[str, dict[str, int]] = {
        str(product): {"evidence": 0, "with_page": 0, "with_block": 0, "with_table": 0}
        for product in products
    }
    with_page = 0
    with_block = 0
    with_table = 0
    with_table_rows = 0
    for row in rows:
        status = str(row.get("page_status") or "")
        if status in by_status:
            by_status[status] += 1
        block_status = str(row.get("block_status") or "")
        if block_status in by_block_status:
            by_block_status[block_status] += 1
        table_status = str(row.get("table_status") or "")
        if table_status in by_table_status:
            by_table_status[table_status] += 1
        bucket = by_product.setdefault(
            str(row.get("product_id") or ""),
            {"evidence": 0, "with_page": 0, "with_block": 0, "with_table": 0},
        )
        bucket["evidence"] += 1
        if row.get("page") is not None:
            with_page += 1
            bucket["with_page"] += 1
        if row.get("block") is not None:
            with_block += 1
            bucket["with_block"] += 1
        if row.get("table") is not None:
            with_table += 1
            bucket["with_table"] += 1
        if row.get("table_rows") is not None:
            with_table_rows += 1
    return {
        "evidence": len(rows),
        "with_page": with_page,
        "with_block": with_block,
        "with_table": with_table,
        "with_table_rows": with_table_rows,
        "by_status": by_status,
        "by_block_status": by_block_status,
        "by_table_status": by_table_status,
        "by_product": dict(sorted(by_product.items())),
    }


def summary_with_products(
    summary: Mapping[str, Any], products: Iterable[str]
) -> dict[str, Any]:
    """Добавляет к сводке **нули** продуктов плана (второй проход по БД не нужен).

    Симметрия с ``partial_docs_summary``: «у этого продукта цитат нет» — тоже факт, и он
    обязан быть виден ключом, а не выводиться из отсутствия ключа. Строки уже посчитаны,
    поэтому нули добавляются к ним, а не пересчётом: второй расчёт разошёлся бы с файлом.
    """
    by_product: dict[str, dict[str, int]] = {
        str(product_id): dict(bucket)
        for product_id, bucket in (summary.get("by_product") or {}).items()
    }
    for product in products:
        by_product.setdefault(
            str(product),
            {"evidence": 0, "with_page": 0, "with_block": 0, "with_table": 0},
        )
    updated = dict(summary)
    updated["by_product"] = dict(sorted(by_product.items()))
    return updated


def evidence_pages_summary(
    store: Any, run_id: str | None, products: Iterable[str] | None = None
) -> dict[str, Any] | None:
    """Блок ``page_provenance`` манифеста: сколько цитат называют страницу и почему нет.

    ``None`` — evidence нет **вовсе**: это не ноль. «Цитаты есть, ни одна страницу не
    назвала» — наоборот, факт, и он публикуется числами (``with_page = 0``).

    ``products`` — план прогона: его продукты попадают в ``by_product`` с нулями, чтобы
    отсутствие цитат у продукта было видно, а не выводилось из отсутствия ключа.

    Сводка несёт **три адреса** цитаты: страницу (``by_status``), блок (``by_block_status``)
    и таблицу (``by_table_status``) — у каждого свой носитель отсутствия, поэтому и свой
    набор исходов. Таблица добавлена без изменения формы остальных полей: блок манифеста
    читается тем же кодом, что и раньше.
    """
    rows, summary = collect_evidence_pages(store, run_id)
    if not rows:
        return None
    return summary_with_products(summary, products or ())


def pages_by_chunk(rows: Iterable[Mapping[str, Any]]) -> dict[str, tuple[int, int]]:
    """Карта ``chunk_id → (page, page_end)`` для battle card — из тех же строк, что в CSV.

    Второй расчёт здесь запрещён: цитата в матрице и строка файла обязаны называть одну
    страницу. Чанки без известной страницы в карту не попадают — «не названо» не
    превращается в число.
    """
    pages: dict[str, tuple[int, int]] = {}
    for row in rows:
        chunk_id = str(row.get("chunk_id") or "")
        page = row.get("page")
        if not chunk_id or not isinstance(page, int):
            continue
        page_end = row.get("page_end")
        pages[chunk_id] = (page, page_end if isinstance(page_end, int) else page)
    return pages


def write_evidence_pages_artifact(
    store: Any,
    run_id: str | None,
    path: str | Path,
    *,
    rows: Sequence[Mapping[str, Any]] | None = None,
) -> Path | None:
    """Пишет ``outputs/evidence_pages.csv``; evidence нет → ``None``.

    Пустой файл — не артефакт: он выглядел бы как «посмотрели и ничего не нашли» там, где
    цитат не было вовсе. Evidence есть, а страницы нет ни у одной — это, наоборот, факт:
    файл пишется, и в нём стоит причина по каждой строке.

    ``rows`` — уже собранные строки: ``write_reports`` считает их один раз и отдаёт тому же
    расчёту battle card, поэтому второй проход по БД не нужен.
    """
    materialized = list(rows) if rows is not None else collect_evidence_pages(store, run_id)[0]
    if not materialized:
        return None
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(EVIDENCE_CSV_COLUMNS),
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in materialized:
            writer.writerow(
                {key: ("" if value is None else value) for key, value in row.items()}
            )
    return target


def human_line(summary: Mapping[str, Any] | None) -> str | None:
    """Одна строка для оператора: ``None`` — сказать нечего (цитат нет вовсе).

    Строка называет **число и причину по классам**, без «прочих»: «120 из 500» без причины
    читается как дефект прибора, хотя за ним стоит документ без карты (pdfium вне
    ``text_layer``/HTML/Office/``split``) — то есть известное ограничение механизма.
    Нулевые классы в строку не попадают: перечисление нулей врало бы о значимости.

    Второй адрес цитаты — **блок** (O36 фаза 2) — печатается тем же правилом: число и
    причины по классам. Причина у него своя (``no_blocks`` вместо ``no_page_map``), потому
    что носитель отсутствия другой: документ может нести карту страниц и не нести блоков.

    Третий адрес — **таблица** (O39 фаза 2) — печатается только когда он непустой: у
    большинства корпусов таблиц нет, и строка «таблицы: 0 цитат» была бы шумом, а не
    фактом. Причина отсутствия таблицы названа в файле (``no_table_ref``), а не здесь: у
    цитаты она почти всегда одна и та же («читатель таблиц их не даёт»), и перечисление её
    в каждой строке оператора не добавляло бы сведения.
    """
    if not summary:
        return None
    evidence = int(summary.get("evidence") or 0)
    if evidence <= 0:
        return None
    with_page = int(summary.get("with_page") or 0)
    with_block = int(summary.get("with_block") or 0)
    with_table = int(summary.get("with_table") or 0)
    with_table_rows = int(summary.get("with_table_rows") or 0)
    # Сводка прежней схемы блока не несёт вовсе. Молчать о нём — единственный честный
    # вариант: «0 из N называют блок» было бы утверждением о корпусе, которого никто не
    # делал (A3). Живая сводка всегда несёт ``by_block_status``.
    has_block_fact = "by_block_status" in summary or "with_block" in summary
    parts: list[str] = []
    if with_page >= evidence:
        parts.append(f"цитаты: все {evidence} называют страницу")
    else:
        by_status = summary.get("by_status") or {}
        reasons: list[str] = []
        for status, label in (
            (PAGE_STATUS_NO_PAGE_MAP, "без карты документа"),
            (PAGE_STATUS_NO_CHAR_SPAN, "без смещений чанка"),
            (PAGE_STATUS_NO_CHUNK, "вне корпуса"),
        ):
            count = int(by_status.get(status) or 0)
            if count:
                reasons.append(f"{label} {count}")
        tail = f"; {', '.join(reasons)}" if reasons else ""
        # Битая ссылка — отдельный хвост: это не ограничение механизма, а дефект данных, и
        # смешивать его с «карты нет» значило бы прятать поломку в списке причин.
        broken = int(by_status.get(PAGE_STATUS_NO_DOC) or 0)
        broken_tail = f", битая ссылка {broken}" if broken else ""
        parts.append(
            f"цитаты: {with_page} из {evidence} называют страницу{tail}{broken_tail}"
        )
    if has_block_fact and with_block >= evidence:
        parts.append(f"блок: все {evidence} называют блок")
    elif has_block_fact:
        by_block_status = summary.get("by_block_status") or {}
        block_reasons: list[str] = []
        for status, label in (
            (BLOCK_STATUS_NO_BLOCKS, "без разметки блоков"),
            (BLOCK_STATUS_NO_CHAR_SPAN, "без смещений чанка"),
            (BLOCK_STATUS_NO_CHUNK, "вне корпуса"),
        ):
            count = int(by_block_status.get(status) or 0)
            if count:
                block_reasons.append(f"{label} {count}")
        block_tail = f"; {', '.join(block_reasons)}" if block_reasons else ""
        block_broken = int(by_block_status.get(BLOCK_STATUS_NO_DOC) or 0)
        block_broken_tail = f", битая ссылка {block_broken}" if block_broken else ""
        parts.append(
            f"блок: {with_block} из {evidence} называют блок"
            f"{block_tail}{block_broken_tail}"
        )
    # Третий адрес — таблица. Печатается только когда он непустой: «таблицы: 0 цитат» у
    # корпуса без таблиц было бы шумом, а не фактом (нули не печатаются — как у блока,
    # который молчит целиком при пустой сводке). Хвост про строки — тоже только при
    # ненулевом числе: «строки у 0» читалось бы как утверждение о таблицах.
    if with_table:
        rows_tail = f" (строки у {with_table_rows})" if with_table_rows else ""
        parts.append(f"таблицы: {with_table} цитат{rows_tail}")
    return "; ".join(parts)
