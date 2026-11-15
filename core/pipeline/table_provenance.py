"""Таблицы документа — объект, а не строка в ``context_prefix`` (O39 фаза 1 плана ``26``).

Зачем отдельный артефакт. Таблица жила в корпусе только как текст: ``export_to_markdown``
печатает её строками ``| … |``, шапка и подпись уезжают в ``context_prefix`` чанка, и
привязки «это ячейка такой-то таблицы» нет ни у чанка, ни у документа. Пока таблица не
публикуется **числом**, её наличие в корпусе невидимо: «в документах нет таблиц» и «таблицы
никто не искал» выглядят одинаково — ровно тот класс тихой потери, ради видимости которого
проект существует.

Что публикуется:

* ``outputs/tables.csv`` — строка **на таблицу** (не на документ): продукт, документ,
  ``table_id``, страница, число строк/столбцов, признак шапки. Документов без таблиц в
  файле нет: файл про таблицы, а «таблиц нет» видно числом ``documents_unmeasured`` в
  сводке — иначе пустая строка на каждый документ корпуса выдавала бы отсутствие за факт;
* блок ``tables`` манифеста (:func:`tables_summary`) — те же числа, свёрнутые до прогона и
  продукта, чтобы факт был виден без разбора CSV;
* строка для оператора (:func:`human_line`) — в ``notes`` CLI-отчёта.

**Два числа, которые нельзя складывать.** ``documents_with_tables`` (документы, где
таблицы прочитаны) и ``documents_unmeasured`` (документы, где читатель таблиц **не даёт**:
``text_layer``/pdfium, HTML, EPUB, офисный путь docling, шов ``split``) — разные факты:
первое отвечает «сколько таблиц в корпусе», второе — «на какой части корпуса этот вопрос
вообще не задавался». Свести их к одному значило бы публиковать «таблиц нет» там, где их
никто не искал (A3).

Источник — **персистентный** корпус (``corpus_docs``), а не память прогона: на resume фаза
корпуса короткозамкнута, а отчёт обязан называть тот же факт. Сбой чтения не роняет отчёт
(он вторичен к фазе): строк тогда нет, и это честно — «не прочитали», а не «таблиц нет».

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

__all__ = [
    "CSV_COLUMNS",
    "collect_tables",
    "human_line",
    "tables_summary",
    "write_tables_artifact",
]

#: Колонки ``outputs/tables.csv`` (порядок стабилен: файл читают глазами и скриптом).
CSV_COLUMNS: tuple[str, ...] = (
    "product_id",
    "doc_id",
    "table_id",
    "page_no",
    "n_rows",
    "n_cols",
    "has_header",
)


def _corpus_docs(store: Any, run_id: str | None) -> list[Any]:
    """Документы корпуса прогона (детерминированно); сбои чтения → ``[]``.

    ``run_id`` обязателен: ``iter_corpus_docs`` проверяет его присутствие в БД и не выдаёт
    документы чужого прогона. Отсутствие аргумента — «источника нет», а не повод прочитать
    весь корпус.
    """
    if store is None or not run_id:
        return []
    reader = getattr(store, "iter_corpus_docs", None)
    if not callable(reader):
        return []
    try:
        docs = list(reader(str(run_id)))
    except Exception:  # noqa: BLE001 — отчёт вторичен к фазе
        return []
    return sorted(docs, key=lambda doc: (str(doc.product_id), str(doc.doc_id)))


def _tables_of(doc: Any) -> Sequence[Any] | None:
    """Таблицы документа: ``None`` — читатель их не даёт, ``()`` — искал и не нашёл."""
    return getattr(doc, "tables", None)


def _width(table: Any) -> int:
    """Ширина таблицы: шапка, а если её нет — первая строка данных.

    Не колонка и не поле объекта: ``n_cols`` — это ``len()``, и копия разошлась бы с
    содержимым при первой же правке строки (см. :class:`core.domain.doc_tables.ExtractedTable`).
    """
    header = tuple(getattr(table, "header", ()) or ())
    if header:
        return len(header)
    rows = tuple(getattr(table, "rows", ()) or ())
    return len(rows[0]) if rows else 0


def collect_tables(store: Any, run_id: str | None) -> list[dict[str, Any]]:
    """Строки артефакта: одна на таблицу, документы и таблицы — в порядке документа.

    Порядок детерминирован (продукт, документ, порядок таблиц в документе): файл читают
    глазами и сравнивают между прогонами. ``page_no`` пустая ячейка — «читатель страницу
    не дал», а не ноль. Документ без таблиц строк не даёт вовсе.
    """
    rows: list[dict[str, Any]] = []
    for doc in _corpus_docs(store, run_id):
        tables = _tables_of(doc)
        if not tables:
            continue
        product_id = str(getattr(doc, "product_id", ""))
        doc_id = str(getattr(doc, "doc_id", ""))
        for table in tables:
            header = tuple(getattr(table, "header", ()) or ())
            rows.append(
                {
                    "product_id": product_id,
                    "doc_id": doc_id,
                    "table_id": str(getattr(table, "table_id", "")),
                    "page_no": getattr(table, "page_no", None),
                    "n_rows": len(tuple(getattr(table, "rows", ()) or ())),
                    "n_cols": _width(table),
                    # ``1``/``0``, а не ``True``/``False``: так же объявлены булевы факты
                    # в схеме (``partial``/``complete``), и файл читают скриптом.
                    "has_header": 1 if header else 0,
                }
            )
    return rows


def _empty_bucket() -> dict[str, int]:
    """Пустой счётчик продукта: нули — факт («у продукта таблиц нет»), а не отсутствие."""
    return {"documents_with_tables": 0, "tables": 0, "documents_unmeasured": 0}


def _aggregate(docs: Iterable[Any]) -> dict[str, Any]:
    """Счётчики по набору документов (продукт или прогон).

    Документ попадает **ровно в одну** из двух категорий: у него либо есть прочитанные
    таблицы (``documents_with_tables``), либо читатель таблиц их не даёт
    (``documents_unmeasured``). Документ, где читатель искал и не нашёл (``()``), — ни в
    той, ни в другой: это «измерено, таблиц нет», и выдумывать ему категорию нельзя.
    """
    bucket = _empty_bucket()
    for doc in docs:
        tables = _tables_of(doc)
        if tables is None:
            bucket["documents_unmeasured"] += 1
            continue
        if not tables:
            continue
        bucket["documents_with_tables"] += 1
        bucket["tables"] += len(tuple(tables))
    return bucket


def tables_summary(
    store: Any,
    run_id: str | None,
    *,
    products: Iterable[str] = (),
) -> dict[str, Any] | None:
    """Блок ``tables`` манифеста: сколько таблиц, в скольких документах и где не измерено.

    ``None`` — документов корпуса нет **вовсе**: это не ноль. «Документы есть, таблиц нет»
    — наоборот, факт, и он публикуется числом (``tables = 0`` при
    ``documents_with_tables = 0``).

    ``by_product`` содержит **нули** для всех продуктов плана (симметрия с
    ``partial_docs``): «у этого продукта таблиц нет» — тоже факт. Продукты, которых нет ни
    в плане, ни в корпусе, не выдумываются.
    """
    docs = _corpus_docs(store, run_id)
    if not docs:
        return None
    by_product: dict[str, list[Any]] = {}
    for doc in docs:
        by_product.setdefault(str(getattr(doc, "product_id", "")), []).append(doc)
    summary = _aggregate(docs)
    per_product = {
        product_id: _aggregate(bucket)
        for product_id, bucket in by_product.items()
    }
    for product in products:
        per_product.setdefault(str(product), _empty_bucket())
    summary["by_product"] = dict(sorted(per_product.items()))
    return summary


def write_tables_artifact(
    store: Any, run_id: str | None, path: str | Path
) -> Path | None:
    """Пишет ``outputs/tables.csv``; ни одной таблицы → ``None``.

    Пустой файл — не артефакт: он выглядел бы как «посмотрели и ничего не нашли» там, где
    таблиц не было вовсе либо их никто не искал. Различить эти два случая внутри пустого
    файла нечем, поэтому отсутствие таблиц публикуется **сводкой**
    (``documents_unmeasured`` против ``documents_with_tables``), а не пустым CSV.
    """
    rows = collect_tables(store, run_id)
    if not rows:
        return None
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(CSV_COLUMNS),
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {key: ("" if value is None else value) for key, value in row.items()}
            )
    return target


def human_line(summary: Mapping[str, Any] | None) -> str | None:
    """Одна строка для оператора: ``None`` — сказать нечего (документов нет).

    Форма — «таблицы: N в M документах; без распознавания таблиц K». Нулевые части не
    печатаются: перечисление нулей врало бы о значимости, а строка нужна затем, чтобы
    оператор увидел оба факта — «сколько таблиц» и «на какой части корпуса вопрос не
    задавался», — не открывая CSV.
    """
    if not summary:
        return None
    tables = int(summary.get("tables") or 0)
    with_tables = int(summary.get("documents_with_tables") or 0)
    unmeasured = int(summary.get("documents_unmeasured") or 0)
    parts: list[str] = []
    if tables or with_tables:
        parts.append(f"таблицы: {tables} в {with_tables} документах")
    if unmeasured:
        parts.append(f"без распознавания таблиц {unmeasured}")
    if not parts:
        return None
    return "; ".join(parts)
