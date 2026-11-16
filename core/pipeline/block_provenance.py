"""Структурные блоки документа — **числом** (O36 фаза 2 плана ``26``).

Зачем отдельная сводка. Сами блоки живут у документа (``corpus_docs.blocks``): вид
фрагмента, отрезок, страница, рамка, родитель, порядок чтения и происхождение текста. По
одной этой колонке нельзя ответить на вопрос «на какой части корпуса структура вообще
измерена»: документ без блоков и документ, где читатель блоков **не даёт**, выглядят
одинаково — пусто. Пока это не опубликовано числом, «структуры в корпусе нет» и «структуру
никто не читал» неотличимы, и цитата без адреса блока читается как дефект прибора, хотя за
ней стоит известное ограничение механизма.

Что публикуется (``runs.blocks_json``):

* ``documents_with_blocks`` — документы, где блоки прочитаны;
* ``blocks`` — сколько блоков всего;
* ``documents_unmeasured`` — документы, где читатель блоков их **не даёт** (``blocks is
  None``: HTML, текст/markdown, EPUB, Office, docling, шов ``split``);
* ``by_kind`` — распределение по виду фрагмента (``page``/``heading``/…);
* ``by_text_source`` — распределение по происхождению текста (``read``/``reconstructed``);
* ``by_product`` — те же числа по продукту, с нулями для продуктов плана.

**Два числа, которые нельзя складывать.** ``documents_with_blocks`` и
``documents_unmeasured`` — разные факты: первое отвечает «сколько структуры в корпусе»,
второе — «на какой части корпуса этот вопрос вообще не задавался». Свести их к одному
значило бы публиковать «блоков нет» там, где их никто не искал (A3).

**Признак ``read|reconstructed`` публикуется, а не сглаживается.** В отличие от исхода
OCR, здесь нет «деградации», которую надо называть худшей: это два честных источника
текста. Смешать их в одно число значило бы потерять ровно тот ответ, ради которого признак
заведён («реконструкция» неотличима от факта).

Источник — **персистентный** корпус (``corpus_docs``), а не память прогона: на resume фаза
корпуса короткозамкнута, а отчёт обязан называть тот же факт. Сбой чтения не роняет отчёт
(он вторичен к фазе): строк тогда нет, и это честно — «не прочитали», а не «блоков нет».

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from core.domain.doc_blocks import BLOCK_KINDS, TEXT_SOURCES

__all__ = ["blocks_summary", "human_line"]


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


def _blocks_of(doc: Any) -> tuple[Any, ...] | None:
    """Блоки документа: ``None`` — читатель их не даёт, ``()`` — смотрел и не дал."""
    return getattr(doc, "blocks", None)


def _empty_bucket() -> dict[str, Any]:
    """Пустой счётчик продукта: нули — факт («у продукта блоков нет»), а не отсутствие."""
    return {
        "documents_with_blocks": 0,
        "blocks": 0,
        "documents_unmeasured": 0,
        "by_kind": {kind: 0 for kind in BLOCK_KINDS},
        "by_text_source": {source: 0 for source in TEXT_SOURCES},
    }


def _aggregate(docs: Iterable[Any]) -> dict[str, Any]:
    """Счётчики по набору документов (продукт или прогон).

    Документ попадает **ровно в одну** из двух категорий: у него либо есть прочитанные
    блоки (``documents_with_blocks``), либо читатель блоков их не даёт
    (``documents_unmeasured``). Документ, где читатель смотрел и не дал (``()``), — ни в
    той, ни в другой: это «измерено, блоков нет», и выдумывать ему категорию нельзя.
    """
    bucket = _empty_bucket()
    by_kind: dict[str, int] = bucket["by_kind"]
    by_source: dict[str, int] = bucket["by_text_source"]
    for doc in docs:
        blocks = _blocks_of(doc)
        if blocks is None:
            bucket["documents_unmeasured"] += 1
            continue
        if not blocks:
            continue
        bucket["documents_with_blocks"] += 1
        bucket["blocks"] += len(blocks)
        for block in blocks:
            kind = str(getattr(block, "kind", ""))
            if kind in by_kind:
                by_kind[kind] += 1
            source = str(getattr(block, "text_source", ""))
            if source in by_source:
                by_source[source] += 1
    return bucket


def blocks_summary(
    store: Any,
    run_id: str | None,
    *,
    products: Iterable[str] = (),
) -> dict[str, Any] | None:
    """Блок ``blocks`` манифеста: сколько структуры прочитано и где она не измерена.

    ``None`` — документов корпуса нет **вовсе**: это не ноль. «Документы есть, блоков нет»
    — наоборот, факт, и он публикуется числом (``blocks = 0`` при
    ``documents_with_blocks = 0``).

    ``by_product`` содержит **нули** для всех продуктов плана (симметрия с
    ``partial_docs``): «у этого продукта блоков нет» — тоже факт. Продукты, которых нет ни
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
        product_id: _aggregate(bucket) for product_id, bucket in by_product.items()
    }
    for product in products:
        per_product.setdefault(str(product), _empty_bucket())
    summary["by_product"] = dict(sorted(per_product.items()))
    return summary


def human_line(summary: Mapping[str, Any] | None) -> str | None:
    """Одна строка для оператора: ``None`` — сказать нечего (документов нет).

    Форма — «блоки: N в M документах (прочитано X, реконструировано Y); без разметки
    блоков K». Нулевые части не печатаются: перечисление нулей врало бы о значимости, а
    строка нужна затем, чтобы оператор увидел **оба** факта — сколько структуры и на какой
    части корпуса вопрос не задавался, — не открывая БД.
    """
    if not summary:
        return None
    blocks = int(summary.get("blocks") or 0)
    with_blocks = int(summary.get("documents_with_blocks") or 0)
    unmeasured = int(summary.get("documents_unmeasured") or 0)
    by_source = summary.get("by_text_source") or {}
    read = int(by_source.get("read") or 0)
    reconstructed = int(by_source.get("reconstructed") or 0)
    parts: list[str] = []
    if blocks or with_blocks:
        sources: list[str] = []
        if read:
            sources.append(f"прочитано {read}")
        if reconstructed:
            sources.append(f"реконструировано {reconstructed}")
        tail = f" ({', '.join(sources)})" if sources else ""
        parts.append(f"блоки: {blocks} в {with_blocks} документах{tail}")
    if unmeasured:
        parts.append(f"без разметки блоков {unmeasured}")
    if not parts:
        return None
    return "; ".join(parts)
