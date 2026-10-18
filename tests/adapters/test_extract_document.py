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
