"""Частичное извлечение — **числом**, а не прилагательным (I2/I2c).

Зачем отдельный артефакт. Признак ``partial`` рождается у экстрактора
(``ExtractedDoc.partial``: docling ``PARTIAL_SUCCESS``, битая страница pdfium) и до
этого модуля умирал на границе корпуса: документ, прочитанный на 7 страницах из 2763,
был в корпусе и в отчёте неотличим от прочитанного полностью. Последствие не
косметическое — молчание непрочитанных страниц читалось как «вендор не заявил», а
затем публиковалось как ``❌`` (I2), хотя половина документа не наблюдалась.

Что публикуется:

* ``outputs/partial_docs.csv`` — строка на документ: продукт, документ, URL, причина,
  прочитано страниц из заявленных, символов. Пустое значение означает «экстрактор
  числа не сообщил» — не ноль (A3: выдумывать нельзя);
* блок ``partial_docs`` манифеста (:func:`partial_docs_summary`) — те же числа,
  свёрнутые до продукта, чтобы факт был виден без разбора CSV;
* строка для оператора (:func:`human_line`) — в ``notes`` CLI-отчёта.

Источник — **персистентный** корпус (``corpus_docs``), а не память прогона: на resume
фаза корпуса короткозамкнута, а отчёт обязан называть тот же факт. Сбой чтения не
роняет отчёт (он вторичен к фазе): блок тогда пуст, и это честно — «не прочитали», а
не «частичных не было».

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

__all__ = [
    "partial_docs_summary",
    "write_partial_docs_artifact",
    "human_line",
]

#: Колонки ``outputs/partial_docs.csv`` (порядок стабилен: файл читают глазами и скриптом).
CSV_COLUMNS: tuple[str, ...] = (
    "product_id",
    "doc_id",
    "url",
    "partial_reason",
    "pages_converted",
    "pages_declared",
    "chars",
)


def _partial_docs(store: Any, run_id: str | None) -> list[Any]:
    """Документы корпуса с ``partial=True`` (детерминированно), сбои чтения → ``[]``.

    ``run_id`` обязателен: ``iter_corpus_docs`` проверяет его присутствие в БД и не
    выдаёт документы чужого прогона. Отсутствие аргумента — «источника нет», а не
    повод прочитать весь корпус.
    """
    if store is None or not run_id:
        return []
    reader = getattr(store, "iter_corpus_docs", None)
    if not callable(reader):
        return []
    try:
        docs = [doc for doc in reader(str(run_id)) if bool(getattr(doc, "partial", False))]
    except Exception:  # noqa: BLE001 — отчёт вторичен к фазе (и к покрытию)
        return []
    return sorted(docs, key=lambda doc: (str(doc.product_id), str(doc.doc_id)))


def partial_docs_summary(
    store: Any,
    run_id: str | None,
    *,
    products: Iterable[str] = (),
) -> dict[str, Any]:
    """Блок ``partial_docs`` манифеста: документы/страницы/символы по продуктам.

    Числа, а не потолок: сколько документов прочитано не целиком, сколько страниц
    реально получено из заявленных и сколько символов из них извлечено. Страницы и
    символы складываются **только по измеренным** значениям: ``None`` у документа не
    превращается в ноль, иначе «не измерено» выглядело бы как «ничего не прочитано».

    ``by_product`` содержит **нули** для всех продуктов плана (симметрия с
    ``manual_artifacts``): «у этого продукта частичных нет» — тоже факт. Продукты,
    которых нет в плане, не выдумываются.
    """
    planned = [str(product) for product in products]
    empty: dict[str, Any] = {
        "documents": 0,
        "by_product": {product: 0 for product in planned},
        "pages_converted": None,
        "pages_declared": None,
        "chars": None,
    }
    docs = _partial_docs(store, run_id)
    if not docs:
        return empty
    by_product: dict[str, int] = {product: 0 for product in planned}
    pages_converted = 0
    pages_declared = 0
    chars = 0
    for doc in docs:
        product_id = str(doc.product_id)
        by_product[product_id] = by_product.get(product_id, 0) + 1
        converted = getattr(doc, "pages_converted", None)
        declared = getattr(doc, "page_count", None)
        text_chars = getattr(doc, "extracted_chars", None)
        if isinstance(converted, int):
            pages_converted += converted
        if isinstance(declared, int):
            pages_declared += declared
        if isinstance(text_chars, int):
            chars += text_chars
    # ``None`` — «ни одного измеренного значения»; частая ошибка — выдать ноль за
    # измерение (страницы 0 из 0 читались бы как «пустой документ»).
    has_converted = _any_measured(docs, "pages_converted")
    has_declared = _any_measured(docs, "page_count")
    has_chars = _any_measured(docs, "extracted_chars")
    return {
        "documents": len(docs),
        "by_product": dict(sorted(by_product.items())),
        "pages_converted": pages_converted if has_converted else None,
        "pages_declared": pages_declared if has_declared else None,
        "chars": chars if has_chars else None,
    }


def _any_measured(docs: Iterable[Any], field: str) -> bool:
    """Сообщил ли хотя бы один документ значение поля (отличает ``0`` от «не измерено»)."""
    return any(isinstance(getattr(doc, field, None), int) for doc in docs)


def write_partial_docs_artifact(
    store: Any, run_id: str | None, path: str | Path
) -> Path | None:
    """Пишет ``outputs/partial_docs.csv``; частичных документов нет → ``None``.

    Пустой файл — не артефакт: он выглядел бы как «нашёл и не нашёл ничего» там, где
    вопроса не было вовсе. Поэтому отсутствие строк возвращает ``None`` (как
    ``stale_cells.csv``/``cell_diagnostics.csv``), а не пустую таблицу.

    Числа печатаются пустой строкой, если экстрактор их не сообщил: ``0`` страниц и
    «число неизвестно» — разные факты.
    """
    docs = _partial_docs(store, run_id)
    if not docs:
        return None
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(CSV_COLUMNS), extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        for doc in docs:
            writer.writerow(
                {
                    "product_id": str(doc.product_id),
                    "doc_id": str(doc.doc_id),
                    "url": str(doc.url or ""),
                    "partial_reason": str(getattr(doc, "partial_reason", None) or ""),
                    "pages_converted": _number(getattr(doc, "pages_converted", None)),
                    "pages_declared": _number(getattr(doc, "page_count", None)),
                    "chars": _number(getattr(doc, "extracted_chars", None)),
                }
            )
    return target


def _number(value: Any) -> str:
    """Число в CSV либо пустая строка («не измерено» ≠ ``0``)."""
    return str(int(value)) if isinstance(value, int) and not isinstance(value, bool) else ""


def human_line(summary: Mapping[str, Any] | None) -> str | None:
    """Строка для оператора (``notes`` CLI) или ``None``, если частичных нет.

    Называет документы и — если экстрактор их сообщил — страницы и символы. Факт
    «охват неполон из-за частичного извлечения» оператор обязан увидеть в выводе
    команды, а не только в файле, который надо открыть.
    """
    if not summary:
        return None
    documents = int(summary.get("documents") or 0)
    if documents <= 0:
        return None
    details: list[str] = []
    converted = summary.get("pages_converted")
    declared = summary.get("pages_declared")
    if isinstance(converted, int) and isinstance(declared, int) and declared > 0:
        details.append(f"страниц прочитано {converted} из {declared}")
    elif isinstance(converted, int):
        details.append(f"страниц прочитано {converted}")
    chars = summary.get("chars")
    if isinstance(chars, int):
        details.append(f"символов {chars}")
    tail = f" ({', '.join(details)})" if details else ""
    return (
        f"частичное извлечение: документов прочитано не целиком {documents}{tail}; "
        "охват корпуса неполон (partial_extraction): ❌ без свидетельства вендора или "
        "принятия риска не публикуется (I2/I2c)"
    )
