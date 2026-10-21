"""Тесты fail-fast OCR в ``DoclingDocumentExtractor`` (дешёвые, без весов docling).

Баг: ``document_level=full`` обещает OCR, но при несовместимой установке ``rapidocr``
docling отвергает все OCR-кандидаты и молча извлекает текст **без** OCR, помечая его
``full``. Здесь закреплено правило: ``full`` без рабочего OCR-движка — явная ошибка
(:class:`OcrEngineUnavailableError`), а уровни, где OCR выключен осознанно
(``layout``/``layout_tables``/``text``), проверку не выполняют и строятся как прежде.

Docling подменяется фейками: тест не грузит модели и не зависит от ``extract-pdf``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.adapters import extract_document
from core.adapters.extract_document import (
    DoclingDocumentExtractor,
    OcrEngineUnavailableError,
    _OcrProbeUnsupported,
    _page_count,
    _probe_ocr_engine,
)
from core.domain.enums import DocumentLevel


class _FakeConverter:
    """Минимальная замена ``DocumentConverter`` (только хранение опций)."""

    def __init__(self, format_options: object = None) -> None:
        self.format_options = format_options


def _patch_docling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Подменяет docling-классы фейками (модели не загружаются)."""
    monkeypatch.setattr(extract_document, "DocumentConverter", _FakeConverter)
    monkeypatch.setattr(extract_document, "InputFormat", SimpleNamespace(PDF="pdf"))
    monkeypatch.setattr(
        extract_document, "PdfFormatOption", lambda pipeline_options: ("pdf", pipeline_options)
    )
    monkeypatch.setattr(
        extract_document,
        "NativePdfFormatOption",
        lambda pipeline_options: ("native-pdf", pipeline_options),
    )
    monkeypatch.setattr(DoclingDocumentExtractor, "_pdf_options", lambda self: object())


# --------------------------------------------------------------------------- #
# Fail-fast: full без OCR-движка — ошибка, а не «текст как full»
# --------------------------------------------------------------------------- #


def test_full_without_ocr_engine_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_probe_ocr_engine", lambda pdf_options: None)

    extractor = DoclingDocumentExtractor(DocumentLevel.FULL)
    with pytest.raises(OcrEngineUnavailableError, match="full"):
        extractor._build_converter()


def test_layout_tables_does_not_probe_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться: уровень выключает OCR")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    converter = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)._build_converter()
    assert isinstance(converter, _FakeConverter)


def test_layout_and_text_do_not_probe_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться: уровень выключает OCR")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    for level in (DocumentLevel.LAYOUT, DocumentLevel.TEXT):
        converter = DoclingDocumentExtractor(level)._build_converter()
        assert isinstance(converter, _FakeConverter), level


def test_full_with_ocr_engine_builds(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_docling(monkeypatch)
    engine = object()
    monkeypatch.setattr(
        extract_document, "_probe_ocr_engine", lambda pdf_options: engine
    )

    converter = DoclingDocumentExtractor(DocumentLevel.FULL)._build_converter()
    assert isinstance(converter, _FakeConverter)


def test_full_office_build_does_not_probe_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Офисный документ OCR не использует: окружение ``extract`` без OCR работает."""
    _patch_docling(monkeypatch)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться для офисного документа")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    converter = DoclingDocumentExtractor(DocumentLevel.FULL)._build_converter(pdf=False)
    assert isinstance(converter, _FakeConverter)


def test_full_office_extract_does_not_require_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сквозной офисный путь ``full`` не упирается в отсутствие OCR."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)

    def _must_not_probe(pdf_options: object) -> object:
        raise AssertionError("OCR-проверка не должна вызываться для .docx")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _must_not_probe)

    doc = DoclingDocumentExtractor(DocumentLevel.FULL).extract_bytes(b"office", "spec.docx")
    # Фейковый конвертер не умеет convert — это ожидаемая грациозная деградация,
    # важно, что путь не потребовал OCR.
    assert doc.error is not None


def test_probe_unsupported_degrades_to_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Переименование внутренностей docling — warning, а не падение извлечения."""
    _patch_docling(monkeypatch)

    def _renamed(pdf_options: object) -> object:
        raise _OcrProbeUnsupported("внутренний API переименован")

    monkeypatch.setattr(extract_document, "_probe_ocr_engine", _renamed)

    converter = DoclingDocumentExtractor(DocumentLevel.FULL)._build_converter()
    assert isinstance(converter, _FakeConverter)
    assert any("проверка пропущена" in record.message for record in caplog.records)


# --------------------------------------------------------------------------- #
# Сам детектор: деградация проверки — не отказ
# --------------------------------------------------------------------------- #


def test_probe_without_ocr_options_is_unsupported() -> None:
    """Опции без ``ocr_options`` (смена внутреннего API) → деградация, не отказ."""
    with pytest.raises(_OcrProbeUnsupported):
        _probe_ocr_engine(object())


# --------------------------------------------------------------------------- #
# queue_max_size — замеренная ручка памяти (execution_change, не methodology)
# --------------------------------------------------------------------------- #


def test_pdf_options_set_queue_max_size_for_pdf_pipelines() -> None:
    """``layout``/``layout_tables``/``full`` несут замеренное ``queue_max_size=1``.

    Тест **вызывает** ``_pdf_options`` (а не мокает её): проверяется реальное поле
    ``PdfPipelineOptions``, которое увидит docling. Дефолт docling 2.135 — 100 страниц
    в полёте (≈3.2 GB peak RSS на 665-стр. документе); 1 даёт −44% при идентичном markdown.
    """
    for level in (
        DocumentLevel.LAYOUT,
        DocumentLevel.LAYOUT_TABLES,
        DocumentLevel.FULL,
    ):
        options = DoclingDocumentExtractor(level)._pdf_options()
        assert options.queue_max_size == 1, level


def test_text_level_has_no_queue_max_size_knob() -> None:
    """Нативный пайплайн (``text``) ручки не имеет — её и не выставляем."""
    options = DoclingDocumentExtractor(DocumentLevel.TEXT)._pdf_options()
    assert not hasattr(options, "queue_max_size")


def test_options_provenance_is_level_aware() -> None:
    """Провенанс ручки: значение для PDF-уровней, ``None`` для ``text``."""
    provenance = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES).options_provenance
    assert provenance["layout"] == 1
    assert provenance["layout_tables"] == 1
    assert provenance["full"] == 1
    assert provenance["text"] is None


# --------------------------------------------------------------------------- #
# page_count — счётчик документа, а не конвертированных страниц
# --------------------------------------------------------------------------- #


class _FakeConvertResult:
    """Минимальный ``ConversionResult``: статус, документ и ``input.page_count``."""

    def __init__(self, *, total_pages: int | None, converted_pages: int) -> None:
        self.status = SimpleNamespace(name="SUCCESS")
        self.document = SimpleNamespace(export_to_markdown=lambda: "# текст")
        self.pages = [object() for _ in range(converted_pages)]
        if total_pages is None:
            self.input = None
        else:
            self.input = SimpleNamespace(page_count=total_pages)


class _ConvertOnlyConverter:
    """Конвертер-фейк: возвращает заранее заданный результат (без docling)."""

    def __init__(self, result: object) -> None:
        self._result = result

    def convert(self, path: str) -> object:  # noqa: ARG002 - путь не важен
        return self._result


def test_page_count_prefers_input_page_count() -> None:
    """Полный счётчик берётся из ``result.input`` (не из числа конвертированных страниц).

    Это защита от будущего ``page_range``: ``len(result.pages)`` вернул бы 2, а документ —
    665 страниц, и ``page_count`` перестал бы описывать документ.
    """
    result = _FakeConvertResult(total_pages=665, converted_pages=2)
    assert _page_count(result) == 665


def test_page_count_falls_back_to_len_pages_without_input() -> None:
    """Нет ``result.input`` → fallback на ``len(result.pages)``."""
    result = _FakeConvertResult(total_pages=None, converted_pages=3)
    assert _page_count(result) == 3


def test_page_count_falls_back_when_input_count_unset() -> None:
    """``input.page_count == 0`` — незаполненный дефолт ``InputDocument``, не счётчик."""
    result = _FakeConvertResult(total_pages=0, converted_pages=4)
    assert _page_count(result) == 4


def test_extract_bytes_uses_document_page_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сквозной путь ``extract_bytes`` отдаёт счётчик документа, а не число страниц."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    result = _FakeConvertResult(total_pages=665, converted_pages=1)
    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)
    extractor._converter = _ConvertOnlyConverter(result)

    doc = extractor.extract_bytes(b"%PDF-1.7", "doc.pdf")

    assert doc.error is None
    assert doc.page_count == 665
