"""Карта страниц в отчёте: у кого она есть и чьи цитаты страницу назвать не могут (O36).

Зачем отдельный артефакт. Карта страниц (``corpus_docs.page_map``) — единственный носитель
номера страницы: по ней :func:`core.domain.page_map.page_number_for` выводит страницу
чанка. Пока её наличие не публикуется, отсутствие карты невидимо — а отсутствие номера
страницы означает, что цитата из документа на 300 страниц не может назвать страницу
вообще. Это не «мелочь оформления»: читатель карты не отличает «страница не названа,
потому что её нет» от «её не назвали».

Что публикуется (``outputs/page_provenance.csv``, один файл — две формы строки, они
различаются колонкой ``scope``):

* ``scope=product`` — агрегаты по продукту: сколько документов, у скольких есть карта,
  сколько страниц отображено, у скольких карты нет;
* ``scope=doc`` — строка на документ, у которого карты **нет**, хотя страниц в нём
  **больше одной** (``page_count > 1``): именно такие документы не могут назвать страницу
  цитаты. Документ с ``page_count`` неизвестным сюда **не** попадает: «не сообщено» не
  даёт права утверждать «страниц больше одной» (A3).

Источник — **персистентный** корпус (``corpus_docs``), а не память прогона: на resume фаза
корпуса короткозамкнута, а отчёт обязан называть тот же факт. Сбой чтения не роняет отчёт
(он вторичен к фазе): строк тогда нет, и это честно — «не прочитали», а не «карт нет».

Пустой файл — не артефакт: нет документов → ``None`` (как ``partial_docs.csv``).

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

__all__ = [
    "CSV_COLUMNS",
    "collect_page_provenance",
    "write_page_provenance_artifact",
]

#: Колонки ``outputs/page_provenance.csv`` (порядок стабилен: файл читают глазами и скриптом).
#:
#: ``scope`` различает две формы строки: агрегат продукта и документ без карты. Пустая
#: ячейка означает «не относится к этой форме», а не ноль.
CSV_COLUMNS: tuple[str, ...] = (
    "scope",
    "product_id",
    "doc_id",
    "url",
    "page_count",
    "docs",
    "docs_with_page_map",
    "pages_mapped",
    "docs_without_page_map",
)

#: Значение ``scope`` для агрегата продукта и для документа без карты.
SCOPE_PRODUCT = "product"
SCOPE_DOC = "doc"


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


def _has_page_map(doc: Any) -> bool:
    """Есть ли у документа **непустая** карта (пустая читается как отсутствующая)."""
    page_map = getattr(doc, "page_map", None)
    return bool(page_map)


def collect_page_provenance(docs: Any) -> list[dict[str, Any]]:
    """Строки артефакта: сначала агрегаты продуктов, затем документы без карты.

    Порядок детерминирован (продукт, затем документ): файл читают глазами и сравнивают
    между прогонами. Числа считаются по факту, а не по ожиданию: ``pages_mapped`` —
    сумма сегментов записанных карт (ноль здесь честен: карт нет — страниц не отображено).
    """
    by_product: dict[str, list[Any]] = {}
    for doc in docs:
        by_product.setdefault(str(getattr(doc, "product_id", "")), []).append(doc)
    rows: list[dict[str, Any]] = []
    for product_id in sorted(by_product):
        product_docs = by_product[product_id]
        with_map = [doc for doc in product_docs if _has_page_map(doc)]
        pages_mapped = sum(len(doc.page_map or ()) for doc in with_map)
        rows.append(
            {
                "scope": SCOPE_PRODUCT,
                "product_id": product_id,
                "doc_id": "",
                "url": "",
                "page_count": "",
                "docs": len(product_docs),
                "docs_with_page_map": len(with_map),
                "pages_mapped": pages_mapped,
                "docs_without_page_map": len(product_docs) - len(with_map),
            }
        )
        for doc in product_docs:
            page_count = getattr(doc, "page_count", None)
            if _has_page_map(doc):
                continue
            if not isinstance(page_count, int) or isinstance(page_count, bool):
                # «Число страниц не сообщено» — не «страниц больше одной».
                continue
            if page_count <= 1:
                # Одна страница (или ноль): номер страницы цитаты не нужен, чтобы
                # назвать страницу, — такой документ не в очереди дефекта.
                continue
            rows.append(
                {
                    "scope": SCOPE_DOC,
                    "product_id": product_id,
                    "doc_id": str(getattr(doc, "doc_id", "")),
                    "url": str(getattr(doc, "url", "") or ""),
                    "page_count": page_count,
                    "docs": "",
                    "docs_with_page_map": "",
                    "pages_mapped": "",
                    "docs_without_page_map": "",
                }
            )
    return rows


def write_page_provenance_artifact(
    store: Any, run_id: str | None, path: str | Path
) -> Path | None:
    """Пишет ``outputs/page_provenance.csv``; документов корпуса нет → ``None``.

    Пустой файл — не артефакт: он выглядел бы как «посмотрели и ничего не нашли» там, где
    вопроса не было вовсе (корпус пуст). Документы есть, а карт нет — это, наоборот,
    факт: файл пишется, и в нём стоят нули.
    """
    docs = _corpus_docs(store, run_id)
    if not docs:
        return None
    rows = collect_page_provenance(docs)
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
            writer.writerow(row)
    return target
