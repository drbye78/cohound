"""Исходы OCR в отчёте: сколько страниц прошло распознавание и где проба не смогла (O38a/O25).

Зачем отдельный артефакт. Признак «страница прочитана распознаванием» родился у адаптера
(``ExtractedDoc.pages_ocr``) и до этого модуля умирал на границе корпуса: «скан прошёл OCR»
и «документ прочитан из текстового слоя» были в отчёте неотличимы, а провал пробы
(``ocr_probe=unavailable``) не публиковался вовсе — он жил только строкой журнала фазы.
Последствие не косметическое: распознавание — условие измерения, и прогон, где оно не
работало, обязан это назвать, а не выглядеть как обычный.

Что публикуется (``outputs/ocr_provenance.csv``; одна форма строки на продукт плюс итог
прогона, различаются колонкой ``scope``):

* ``docs`` — документов продукта в корпусе; ``docs_with_ocr_pages`` — у скольких
  ``pages_ocr > 0``, то есть распознавание действительно дало текст хотя бы на странице;
* ``pages_ocr`` / ``pages_failed`` — суммы по документам, **где значение измерено**;
  ``None`` (пустая ячейка) — ни одного измерения, а не ноль (A3);
* распределение по исходу пробы: ``ocr_probe_ok``/``..._unavailable``/``..._unsupported``/
  ``..._unknown``/``..._unmeasured`` (последнее — ``None``: проба к документу не применима);
* ``worst_ocr_probe`` — худший исход по продукту/прогону
  (:func:`core.domain.ocr_probe.worst_ocr_probe`).

Источник — **персистентный** корпус (``corpus_docs``), а не память прогона: на resume фаза
корпуса короткозамкнута, а отчёт обязан называть тот же факт. Сбой чтения не роняет отчёт
(он вторичен к фазе): строк тогда нет, и это честно — «не прочитали», а не «OCR не
применялся».

Чего здесь **нет**: признака «текст этой страницы получен OCR-ом» (``TextCell.from_ocr``) —
docling отдаёт его только при ``generate_parsed_pages=True``, а пайплайн обнуляет
``page.parsed_page``; это отдельная ось с ценой по RSS и отдельным замером (A3). И «OCR
смотрел и текста не дал» библиотека не сообщает вовсе (только warning в лог) — публиковать
это как ноль было бы выдумкой.

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from core.domain.ocr_probe import (
    OCR_PROBE_OK,
    OCR_PROBE_UNAVAILABLE,
    OCR_PROBE_UNKNOWN,
    OCR_PROBE_UNSUPPORTED,
    worst_ocr_probe,
)

__all__ = [
    "CSV_COLUMNS",
    "SCOPE_PRODUCT",
    "SCOPE_RUN",
    "collect_ocr_provenance",
    "ocr_probe_by_product",
    "run_ocr_probe",
    "write_ocr_provenance_artifact",
]

#: Колонки ``outputs/ocr_provenance.csv`` (порядок стабилен: файл читают глазами и скриптом).
CSV_COLUMNS: tuple[str, ...] = (
    "scope",
    "product_id",
    "docs",
    "docs_with_ocr_pages",
    "pages_ocr",
    "pages_failed",
    "ocr_probe_ok",
    "ocr_probe_unavailable",
    "ocr_probe_unsupported",
    "ocr_probe_unknown",
    "ocr_probe_unmeasured",
    "worst_ocr_probe",
)

#: Значение ``scope`` для агрегата продукта и для итога прогона.
SCOPE_PRODUCT = "product"
SCOPE_RUN = "run"


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


def _count(value: Any) -> int | None:
    """Измеренное число либо ``None`` (``bool`` — не число: ``True`` не станет «1 страницей»)."""
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


def _aggregate(docs: Sequence[Any]) -> dict[str, Any]:
    """Один блок чисел по набору документов (продукт или прогон).

    Суммы складываются только по **измеренным** значениям: ``None`` у документа не
    превращается в ноль, иначе «не измерено» выглядело бы как «ни одной страницы». Нет ни
    одного измерения — ``None``, а не ноль.
    """
    pages_ocr: int | None = None
    pages_failed: int | None = None
    with_ocr_pages = 0
    distribution = {
        OCR_PROBE_OK: 0,
        OCR_PROBE_UNAVAILABLE: 0,
        OCR_PROBE_UNSUPPORTED: 0,
        OCR_PROBE_UNKNOWN: 0,
    }
    unmeasured = 0
    for doc in docs:
        ocr_pages = _count(getattr(doc, "pages_ocr", None))
        if ocr_pages is not None:
            pages_ocr = ocr_pages if pages_ocr is None else pages_ocr + ocr_pages
            if ocr_pages > 0:
                with_ocr_pages += 1
        failed_pages = _count(getattr(doc, "pages_failed", None))
        if failed_pages is not None:
            pages_failed = (
                failed_pages if pages_failed is None else pages_failed + failed_pages
            )
        probe = getattr(doc, "ocr_probe", None)
        if probe in distribution:
            distribution[probe] += 1
        else:
            unmeasured += 1
    return {
        "docs": len(docs),
        "docs_with_ocr_pages": with_ocr_pages,
        "pages_ocr": pages_ocr,
        "pages_failed": pages_failed,
        "ocr_probe_ok": distribution[OCR_PROBE_OK],
        "ocr_probe_unavailable": distribution[OCR_PROBE_UNAVAILABLE],
        "ocr_probe_unsupported": distribution[OCR_PROBE_UNSUPPORTED],
        "ocr_probe_unknown": distribution[OCR_PROBE_UNKNOWN],
        "ocr_probe_unmeasured": unmeasured,
        "worst_ocr_probe": worst_ocr_probe(
            getattr(doc, "ocr_probe", None) for doc in docs
        ),
    }


def collect_ocr_provenance(docs: Iterable[Any]) -> list[dict[str, Any]]:
    """Строки артефакта: агрегат на продукт (по возрастанию), затем итог прогона.

    Порядок детерминирован: файл читают глазами и сравнивают между прогонами. Итог
    прогона — **последняя** строка (``scope=run``): он складывается из тех же документов,
    а не пересчитывается второй формулой.
    """
    materialized = list(docs)
    by_product: dict[str, list[Any]] = {}
    for doc in materialized:
        by_product.setdefault(str(getattr(doc, "product_id", "")), []).append(doc)
    rows: list[dict[str, Any]] = []
    for product_id in sorted(by_product):
        rows.append(
            {"scope": SCOPE_PRODUCT, "product_id": product_id,
             **_aggregate(by_product[product_id])}
        )
    rows.append({"scope": SCOPE_RUN, "product_id": "", **_aggregate(materialized)})
    return rows


def ocr_probe_by_product(store: Any, run_id: str | None) -> dict[str, str | None]:
    """Худший исход пробы по каждому продукту корпуса (для ``index_manifest``).

    Продукт без единого исхода получает ``None`` — «нечего публиковать», а не ``ok``:
    продуктов, которых нет в корпусе, в словаре нет вовсе (значения не выдумываются).
    """
    by_product: dict[str, list[Any]] = {}
    for doc in _corpus_docs(store, run_id):
        by_product.setdefault(str(getattr(doc, "product_id", "")), []).append(doc)
    return {
        product_id: worst_ocr_probe(
            getattr(doc, "ocr_probe", None) for doc in docs
        )
        for product_id, docs in by_product.items()
    }


def run_ocr_probe(store: Any, run_id: str | None) -> str | None:
    """Худший исход пробы по корпусу прогона (для ``runs.ocr_probe``/манифеста).

    ``None`` — ни один документ исхода не сообщил. Отдельная функция, а не «взять первый
    продукт»: прогон без продуктов и прогон, где проба не применима ни к одному документу,
    обязаны давать одно и то же «нечего публиковать».
    """
    return worst_ocr_probe(
        getattr(doc, "ocr_probe", None) for doc in _corpus_docs(store, run_id)
    )


def write_ocr_provenance_artifact(
    store: Any, run_id: str | None, path: str | Path
) -> Path | None:
    """Пишет ``outputs/ocr_provenance.csv``; документов корпуса нет → ``None``.

    Пустой файл — не артефакт: он выглядел бы как «посмотрели и ничего не нашли» там, где
    вопроса не было вовсе (корпус пуст). Документы есть, а исходов нет — это, наоборот,
    факт: файл пишется, и в нём стоит ``ocr_probe_unmeasured``.
    """
    docs = _corpus_docs(store, run_id)
    if not docs:
        return None
    rows = collect_ocr_provenance(docs)
    if not rows:  # pragma: no cover — ``docs`` непуст ⇒ итог прогона всегда есть
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
                {
                    key: ("" if value is None else value)
                    for key, value in row.items()
                }
            )
    return target
