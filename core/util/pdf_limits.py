"""Лимиты PDF-документа — объявленная ось, по умолчанию **отсутствующая** (план 26, D9).

**Почему умолчание — «лимита нет».** Замер O31 (09.11.2026, 1 721 PDF / 53 689 страниц,
``docs/reports/measurements/2026-11-09-pdf-corpus-profile.md``): страницы p50 24 / p95 40 /
p99 212.2 / max 2 763; символы p50 26 864 / p99 231 158.6 / max 7 726 772. Любой кандидат
потолка режет материал: 1 000 страниц — 2 документа / 9.2 % страниц, 250 страниц — 13 /
20.0 %, 1 M символов — 9 / 29.1 %. Поэтому ``None`` здесь — **измеренное** состояние
корпуса, а не «безопасный потолок» (A3): потолок объявляет оператор, и объявленный лимит
входит в ``crawl_config_digest``/``run_id`` (условие измерения, ``execution_change`` §13).

**Что делает каждый лимит.**

* ``max_pages`` — читаются **первые N страниц** документа; применяется читателем **до**
  конвертации, поэтому экономит и время (docling получает ``page_range``, pdfium не
  открывает страницы за пределом), и объём;
* ``max_extracted_chars`` — текст обрезается до N символов **после** чтения: цена
  конвертации платится целиком, экономия — в объёме корпуса и в нарезке.

Оба исхода **не тихие**: документ помечается ``partial=True``, получает причину с
машинным токеном :data:`PDF_LIMITS_EXHAUSTED_REASON` и числами («прочитано страниц N из
M», «символов отброшено K из L»). Причина переживает ингест
(``document_version.extract_partial``/``extract_partial_reason``,
``outputs/partial_docs.csv``), поэтому «почему в корпусе нет конца документа» отвечается
из БД, а не из журнала.

**Носитель — контекст вызова, а не конструктор экстрактора.** Экстрактор общий на ключ
``(уровень, механизм PDF)``: два корня одного продукта с разными лимитами разделили бы
один экземпляр, и лимит одного молча применился бы к документам другого. :class:`ContextVar`
делает лимит свойством **задания** извлечения: воркер пула ставит его на время вызова,
потоковый (``split``) и inline-путь — так же; вложенные вызовы не видят чужое значение.

**Кэш.** Текст, обрезанный лимитом, — частичный, а ``ExtractionCache.put`` частичное не
сохраняет, поэтому отравления кэша «обрезанным документом под полным ключом» не
происходит. Обратное направление (хит кэша, снятого **без** лимита, обслужил бы прогон
**с** лимитом полным текстом) закрывает вызывающий: при активных лимитах он кэш не
спрашивает. Цена названа явно: документы с объявленным лимитом пере-извлекаются каждый
прогон.

Слой ``util``: без ``ports``/``adapters`` (матрица 01 §1), нейтрален (§1.3).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import dataclass

__all__ = [
    "PDF_LIMITS_EXHAUSTED_REASON",
    "PdfLimits",
    "char_limit_note",
    "current_pdf_limits",
    "merge_partial_reason",
    "page_limit_note",
    "pdf_limits",
    "truncate_text",
    "use_pdf_limits",
]

#: Машинный токен причины: документ прочитан **не целиком** из-за объявленного лимита.
#:
#: Имя следует стилю существующих причин (``no_text_layer``, ``pages_without_text_layer``):
#: строчные слова через ``_``, класс факта — в первом сегменте. Собственный токен, а не
#: общий «частичный документ»: «страниц не прочитано N из M» (отказ pdfium) и «прочитано
#: первые N из M по лимиту оператора» — разные факты, и смешивать их значило бы прятать
#: решение оператора в отказе прибора.
PDF_LIMITS_EXHAUSTED_REASON = "pdf_limits_exhausted"


@dataclass(frozen=True)
class PdfLimits:
    """Объявленные лимиты одного PDF-документа (``None`` — лимита по этой оси нет).

    Поля — ровно те оси, которые объявляет пакет/прогон/корень
    (``pdf_max_pages``/``pdf_max_extracted_chars``). Объект неизменяемый: он едет в payload
    воркера (примитивы, picklable) и не должен меняться по дороге.
    """

    max_pages: int | None = None
    max_extracted_chars: int | None = None

    def __post_init__(self) -> None:
        """Ноль/отрицательное значение — ошибка объявления, а не «выключить лимит».

        «Ноль страниц» и «ноль символов» не означают «лимита нет»: ``None`` уже означает
        это. Принять ``0`` значило бы получить пустой корпус при объявленной оси и искать
        причину в другом месте.
        """
        for name, value in (
            ("max_pages", self.max_pages),
            ("max_extracted_chars", self.max_extracted_chars),
        ):
            if value is not None and (isinstance(value, bool) or int(value) < 1):
                raise ValueError(f"PdfLimits.{name} обязан быть ≥ 1 либо None: {value!r}")

    @property
    def active(self) -> bool:
        """Хотя бы одна ось объявлена — лимит вообще применяется."""
        return self.max_pages is not None or self.max_extracted_chars is not None

    def as_dict(self) -> dict[str, int]:
        """Только объявленные оси (для провенанса/записи причины)."""
        payload: dict[str, int] = {}
        if self.max_pages is not None:
            payload["pdf_max_pages"] = int(self.max_pages)
        if self.max_extracted_chars is not None:
            payload["pdf_max_extracted_chars"] = int(self.max_extracted_chars)
        return payload


def pdf_limits(
    max_pages: int | None = None, max_extracted_chars: int | None = None
) -> PdfLimits | None:
    """Лимиты из разрешённых осей; ``None`` — ни одна ось не объявлена.

    Фабрика, а не конструктор на месте вызова: у прогона без осей в payload/контекст
    обязан уходить ``None``, а не «объект с двумя ``None``» — иначе проверки вида
    ``if limits is not None`` в читателях стали бы ложными, и лимит «присутствовал» бы
    там, где его никто не объявлял.
    """
    if max_pages is None and max_extracted_chars is None:
        return None
    return PdfLimits(
        max_pages=None if max_pages is None else int(max_pages),
        max_extracted_chars=(
            None if max_extracted_chars is None else int(max_extracted_chars)
        ),
    )


#: Лимиты **текущего** извлечения. ``None`` — ось не объявлена (поведение прежнее).
_CURRENT: ContextVar[PdfLimits | None] = ContextVar("cohound_pdf_limits", default=None)


def current_pdf_limits() -> PdfLimits | None:
    """Лимиты задания, в контексте которого исполняется извлечение (``None`` — нет)."""
    return _CURRENT.get()


@contextlib.contextmanager
def use_pdf_limits(limits: PdfLimits | None) -> Iterator[PdfLimits | None]:
    """Ставит лимиты на время вызова экстрактора и возвращает прежнее значение.

    ``finally``-восстановление обязательно: исключение читателя не должно оставлять лимит
    следующему документу того же воркера — иначе один упавший PDF молча обрезал бы все
    последующие.
    """
    token = _CURRENT.set(limits)
    try:
        yield limits
    finally:
        _CURRENT.reset(token)


def truncate_text(text: str, max_chars: int | None) -> tuple[str, int]:
    """Обрезает текст до ``max_chars``; возвращает ``(текст, сколько символов отброшено)``.

    Обрез **точный** (по границе символа, без «умного» поиска слова): лимит объявлен
    числом, и результат обязан быть функцией этого числа, а не эвристики о словах —
    иначе два прогона с одной осью дали бы разный корпус. Хвостовое слово может быть
    разрезано; чанкер режет по границам сам, а причина частичности называет объём
    отброшенного, поэтому «конец документа отсутствует» виден, а не предполагается.
    """
    if max_chars is None or len(text) <= max_chars:
        return text, 0
    return text[:max_chars], len(text) - max_chars


def page_limit_note(read_pages: int, total_pages: int, limit: int) -> str:
    """Причина частичности по оси страниц: сколько прочитано из скольких и при каком лимите."""
    return (
        f"{PDF_LIMITS_EXHAUSTED_REASON}: прочитано страниц {read_pages} из {total_pages} "
        f"(pdf_max_pages={limit})"
    )


def char_limit_note(dropped: int, total_chars: int, limit: int) -> str:
    """Причина частичности по оси символов: сколько отброшено из скольких при каком лимите."""
    return (
        f"{PDF_LIMITS_EXHAUSTED_REASON}: отброшено символов {dropped} из {total_chars} "
        f"(pdf_max_extracted_chars={limit})"
    )


def merge_partial_reason(existing: str | None, extra: str) -> str:
    """Склеивает причины частичности, не теряя ни одной.

    Читатель может назвать **свой** факт (битая страница, скан без текстового слоя), а
    лимит — свой. Перезапись причины спрятала бы отказ прибора за решением оператора (и
    наоборот), поэтому факты идут через ``; `` в порядке появления.
    """
    current = (existing or "").strip()
    if not current:
        return extra
    if extra in current:
        return current
    return f"{current}; {extra}"
