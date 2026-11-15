"""Исходы OCR по страницам в docling-пути (O38a): счётчики и их границы.

Проверяется то, чего нельзя увидеть на уровне модели: **откуда** берутся числа.
Единственный постраничный сигнал распознавания, переживающий дефолтные опции docling, —
``result.confidence.pages[p].ocr_score`` (заполняется только там, где были ячейки с
``from_ocr``). Признак ``TextCell.from_ocr`` без ``generate_parsed_pages=True`` недоступен
(пайплайн обнуляет ``page.parsed_page``), поэтому «текст страницы получен OCR-ом» здесь не
публикуется вовсе — вместо него считается «страница дала хотя бы одну OCR-ячейку».

Docling подменяется фейками: тест не грузит модели и не зависит от ``extract-pdf``.
"""

from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from core.adapters import extract_document
from core.adapters.extract_document import DoclingDocumentExtractor
from core.domain.enums import DocumentLevel
from core.util.hashing import content_hash


def _patch_docling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Подменяет docling-классы фейками (модели не загружаются)."""
    monkeypatch.setattr(extract_document, "DocumentConverter", lambda **_kw: None)
    monkeypatch.setattr(extract_document, "InputFormat", SimpleNamespace(PDF="pdf"))
    monkeypatch.setattr(
        extract_document,
        "PdfFormatOption",
        lambda pipeline_options: ("pdf", pipeline_options),
    )
    monkeypatch.setattr(
        extract_document,
        "NativePdfFormatOption",
        lambda pipeline_options: ("native-pdf", pipeline_options),
    )
    monkeypatch.setattr(DoclingDocumentExtractor, "_pdf_options", lambda self: object())


class _Converter:
    """Конвертер-фейк: отдаёт заранее собранный результат."""

    def __init__(self, result: object) -> None:
        self._result = result

    def convert(self, path: str) -> object:  # noqa: ARG002 - путь не важен
        return self._result


def _page(ocr_score: object) -> SimpleNamespace:
    """Оценка страницы: ``ocr_score`` может быть ``nan``/``None``/числом."""
    return SimpleNamespace(ocr_score=ocr_score)


def _result(
    *,
    scores: dict[int, object],
    errors: list[object] | None = None,
    total_pages: int = 3,
    status: str = "SUCCESS",
    text: str = "# текст",
) -> SimpleNamespace:
    """``ConversionResult``-фейк: страницы, оценки и ошибки."""
    return SimpleNamespace(
        status=SimpleNamespace(name=status),
        document=SimpleNamespace(export_to_markdown=lambda **_kw: text),
        pages=[object() for _ in range(total_pages)],
        input=SimpleNamespace(page_count=total_pages),
        confidence=SimpleNamespace(pages=scores),
        errors=list(errors or []),
    )


def _extract(
    monkeypatch: pytest.MonkeyPatch,
    result: object,
    *,
    level: DocumentLevel = DocumentLevel.FULL,
) -> object:
    """Прогоняет фейковый результат через настоящий ``_convert`` адаптера."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    extractor = DoclingDocumentExtractor(level)
    extractor._converter = _Converter(result)
    return extractor.extract_bytes(b"%PDF-1.7", "doc.pdf")


# ---------------------------------------------------------------------------
# pages_ocr: страница с OCR-ячейками
# ---------------------------------------------------------------------------


def test_pages_with_ocr_cells_are_counted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Страница с числовым ``ocr_score`` — OCR-страница; ``nan`` — нет.

    Ровно это различает «распознавание дало текст на странице» и «страница прочитана из
    текстового слоя»: ``ocr_score`` заполняет только модель OCR и только при наличии
    ячеек с ``from_ocr``.
    """
    result = _result(scores={1: _page(0.91), 2: _page(float("nan")), 3: _page(0.55)})
    doc = _extract(monkeypatch, result)

    assert doc.error is None
    assert doc.pages_ocr == 2


def test_page_without_ocr_score_is_not_counted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Страница с ``None`` вместо оценки не считается OCR-страницей.

    ``ConfidenceReport.pages`` — ``defaultdict``: индексация создала бы запись и «измерила»
    несуществующую страницу, поэтому читатель обязан спрашивать ``.get``/``values``.
    """
    result = _result(scores={1: _page(None), 2: _page(math.nan)})
    doc = _extract(monkeypatch, result)

    assert doc.pages_ocr == 0


def test_ocr_not_applied_on_level_without_ocr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Уровень без распознавания: ``pages_ocr`` — ``None``, а не ноль.

    ``layout_tables`` объявляет ``do_ocr=False``; ноль здесь означал бы «применили и ни одна
    страница не дала текста», то есть объявил бы измерение, которого не было.
    """
    result = _result(scores={1: _page(float("nan")), 2: _page(float("nan"))})
    doc = _extract(monkeypatch, result, level=DocumentLevel.LAYOUT_TABLES)

    assert doc.pages_ocr is None


def test_office_document_has_no_ocr_counters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Офисный документ: распознавания нет вовсе — оба счётчика ``None``."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    result = _result(scores={1: _page(0.9)}, total_pages=1)
    extractor = DoclingDocumentExtractor(DocumentLevel.FULL)
    extractor._converter = _Converter(result)

    doc = extractor.extract_bytes(b"PK\x03\x04", "doc.docx")

    assert doc.error is None
    assert (doc.pages_ocr, doc.pages_failed) == (None, None)


# ---------------------------------------------------------------------------
# pages_failed: страницы с ошибкой
# ---------------------------------------------------------------------------


def test_pages_with_errors_are_counted_once_per_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Считаются **страницы**, а не ошибки; ошибка уровня документа не входит.

    Две ошибки на одной странице — одна упавшая страница. ``page_no=None`` — ошибка
    документа, и приписывать её странице значило бы выдумать номер.
    """
    result = _result(
        scores={},
        errors=[
            SimpleNamespace(page_no=2),
            SimpleNamespace(page_no=2),
            SimpleNamespace(page_no=3),
            SimpleNamespace(page_no=None),
        ],
    )
    doc = _extract(monkeypatch, result)

    assert doc.pages_failed == 2


def test_no_errors_means_zero_failed_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ошибок нет — ноль (измерение), а не «не измерено»."""
    result = _result(scores={1: _page(0.5)}, errors=[])
    doc = _extract(monkeypatch, result)

    assert doc.pages_failed == 0


def test_counters_do_not_require_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    """Полный по статусу документ несёт счётчики: они не производные от частичности."""
    result = _result(scores={1: _page(0.5)}, errors=[SimpleNamespace(page_no=1)])
    doc = _extract(monkeypatch, result)

    assert doc.error is None
    assert doc.partial is False
    assert (doc.pages_ocr, doc.pages_failed) == (1, 1)


def test_partial_success_also_reports_counters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Частичный успех называет те же числа: прибор один на оба исхода."""
    result = _result(
        scores={1: _page(0.7), 2: _page(float("nan"))},
        errors=[SimpleNamespace(page_no=2)],
        status="PARTIAL_SUCCESS",
        text="# часть",
    )
    doc = _extract(monkeypatch, result)

    assert doc.partial is True
    assert (doc.pages_ocr, doc.pages_failed) == (1, 1)


def test_contradiction_with_page_count_is_not_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Противоречие с размером документа не убивает документ: число не публикуется.

    Инвариант ``ExtractedDoc`` отверг бы такую пару, а ``_convert`` ловит любое исключение
    и отдаёт ``error`` — то есть прочитанный текст пропал бы из-за провенанса (тот же класс
    отказа, что D4). Поэтому адаптер сам называет «не измерено».
    """
    result = _result(scores={1: _page(0.5), 2: _page(0.6), 3: _page(0.7), 4: _page(0.8)}, total_pages=2)
    # ``total_pages=2`` задаёт и ``input.page_count``, но страниц с OCR четыре.
    result.input = SimpleNamespace(page_count=2)
    result.pages = [object(), object()]

    doc = _extract(monkeypatch, result)

    assert doc.error is None
    assert doc.pages_ocr is None


def test_text_and_hash_unchanged_by_counters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Счётчики — провенанс: текст и его хеш от них не меняются."""
    result = _result(scores={1: _page(0.5)}, text="# тот же текст")
    doc = _extract(monkeypatch, result)

    assert doc.text == "# тот же текст"
    assert doc.content_hash == content_hash("# тот же текст")
