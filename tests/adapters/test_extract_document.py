"""Тесты fail-fast OCR в ``DoclingDocumentExtractor`` (дешёвые, без весов docling).

Баг: ``document_level=full`` обещает OCR, но при несовместимой установке ``rapidocr``
docling отвергает все OCR-кандидаты и молча извлекает текст **без** OCR, помечая его
``full``. Здесь закреплено правило: ``full`` без рабочего OCR-движка — явная ошибка
(:class:`OcrEngineUnavailableError`), а уровни, где OCR выключен осознанно
(``layout``/``layout_tables``/``text``), проверку не выполняют и строятся как прежде.

Docling подменяется фейками: тест не грузит модели и не зависит от ``extract-pdf``.
"""

from __future__ import annotations

import errno
from pathlib import Path
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
from core.util.hashing import content_hash


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


# --------------------------------------------------------------------------- #
# Путь к retained-файлу: ``extract_path`` без копии тела vs ``extract_bytes``
# --------------------------------------------------------------------------- #

class _PathReadingConverter:
    """Фейк-конвертер: читает файл по пути и строит markdown из его байтов.

    Оба входа (байты/путь) проверяются на **одних и тех же** байтах: если путь подставлен
    неверно или тело скопировано с потерей, markdown разойдётся. Заодно фиксируются
    свойства подставленного файла — суффикс, симлинк/жёсткая ссылка, inode.
    """

    def __init__(self) -> None:
        self.seen: list[dict[str, object]] = []

    def convert(self, path: str) -> object:
        staged = Path(path)
        data = staged.read_bytes()
        self.seen.append(
            {
                "path": staged,
                "suffix": staged.suffix,
                "is_symlink": staged.is_symlink(),
                "inode": staged.stat().st_ino,
                "data": data,
            }
        )
        markdown = f"# документ\n\n{len(data)} байт: {data[:16].decode('utf-8', 'replace')}"
        return SimpleNamespace(
            status=SimpleNamespace(name="SUCCESS"),
            document=SimpleNamespace(export_to_markdown=lambda: markdown),
            pages=[object()],
            input=SimpleNamespace(page_count=7),
        )

def test_extract_path_matches_extract_bytes_byte_for_byte(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``extract_path`` даёт **тот же** markdown, что ``extract_bytes`` на тех же байтах."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    data = b"PK\x03\x04office-document-bytes"
    source = tmp_path / f"{content_hash(data)}.bin"  # раскладка retention (I19)
    source.write_bytes(data)

    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)
    converter = _PathReadingConverter()
    extractor._converter = converter

    by_bytes = extractor.extract_bytes(data, "spec.docx")
    by_path = extractor.extract_path(source, filename_hint="spec.docx")

    assert by_bytes.error is None and by_path.error is None
    assert by_path.text == by_bytes.text
    assert by_path.text.encode("utf-8") == by_bytes.text.encode("utf-8")
    assert by_path.content_hash == by_bytes.content_hash
    assert by_path.page_count == by_bytes.page_count == 7
    assert by_path.extractor_version == by_bytes.extractor_version
    assert by_path.extractor_version == "docling-doc-3:layout_tables"
    # Оба пути отдали конвертеру одни и те же байты.
    assert [entry["data"] for entry in converter.seen] == [data, data]

def test_extract_path_stages_by_reference_with_right_suffix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Формат — из ``filename_hint`` (retained-файл ``.bin``), тело — по ссылке, не копией."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_DOCLING_AVAILABLE", True)
    data = b"%PDF-1.7 retained-body"
    source = tmp_path / f"{content_hash(data)}.bin"
    source.write_bytes(data)

    extractor = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES)
    converter = _PathReadingConverter()
    extractor._converter = converter

    doc = extractor.extract_path(source, filename_hint="report.pdf")

    assert doc.error is None
    staged = converter.seen[0]
    assert staged["suffix"] == ".pdf"
    if staged["is_symlink"]:
        # tmp и corpus_raw на разных ФС — симлинк: байты всё равно не копировались.
        assert staged["path"].resolve() == source.resolve()
    else:
        # Жёсткая ссылка: тот же inode, ноль скопированных байт.
        assert staged["inode"] == source.stat().st_ino
    # Временный каталог убран: ссылка (и копия, если была) не остаётся на диске.
    assert not staged["path"].exists()

def test_extract_path_without_hint_rejects_bin(tmp_path: Path) -> None:
    """Без ``filename_hint`` retained-файл ``.bin`` — честная ошибка формата, не догадка."""
    source = tmp_path / "abc.bin"
    source.write_bytes(b"%PDF-1.7")

    doc = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES).extract_path(source)

    assert doc.error == "формат не поддерживается: .bin"
    assert doc.text == ""

def test_extract_path_missing_file_is_graceful(tmp_path: Path) -> None:
    """Пропавший retained-файл — ``ExtractedDoc.error``, а не исключение."""
    missing = tmp_path / "gone.bin"

    doc = DoclingDocumentExtractor(DocumentLevel.LAYOUT_TABLES).extract_path(
        missing, filename_hint="spec.docx"
    )

    assert doc.error == f"retained-файл не найден: {missing}"
    assert doc.text == ""
    assert doc.extractor_version == "docling-doc-3:layout_tables"

def test_extract_path_full_pdf_keeps_ocr_guard(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Та же OCR-проверка, что у ``extract_bytes``: ``full`` без движка — отказ, не текст."""
    _patch_docling(monkeypatch)
    monkeypatch.setattr(extract_document, "_probe_ocr_engine", lambda pdf_options: None)
    source = tmp_path / "scan.bin"
    source.write_bytes(b"%PDF-1.7")

    with pytest.raises(OcrEngineUnavailableError, match="full"):
        DoclingDocumentExtractor(DocumentLevel.FULL).extract_path(
            source, filename_hint="scan.pdf"
        )

def test_stage_by_reference_falls_back_link_symlink_copy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Цепочка подстановки: hard link → symlink → копия (порядок зафиксирован)."""
    source = tmp_path / "src.bin"
    source.write_bytes(b"body")
    target = tmp_path / "out" / "document.docx"
    target.parent.mkdir()

    # 1. Штатный путь: жёсткая ссылка (байты не копируются).
    extract_document._stage_by_reference(source, target)
    assert not target.is_symlink()
    assert target.stat().st_ino == source.stat().st_ino
    target.unlink()

    # 2. Разные ФС (EXDEV) → симлинк: тоже без копирования байтов.
    def _no_hardlink(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EXDEV, "cross-device link")

    monkeypatch.setattr(extract_document.os, "link", _no_hardlink)
    extract_document._stage_by_reference(source, target)
    assert target.is_symlink()
    assert target.read_bytes() == b"body"
    target.unlink()

    # 3. ФС без ссылок → потоковая копия (последний резерв).
    def _no_symlink(*args: object, **kwargs: object) -> None:
        raise OSError(errno.EPERM, "symlink not permitted")

    monkeypatch.setattr(extract_document.os, "symlink", _no_symlink)
    extract_document._stage_by_reference(source, target)
    assert not target.is_symlink()
    assert target.stat().st_ino != source.stat().st_ino
    assert target.read_bytes() == b"body"
