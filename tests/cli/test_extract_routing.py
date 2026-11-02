"""Маршрутизация экстракции в composition root CLI: ``source_path`` → ``extract_path``.

Контракт ``RawPage.source_path`` (аддитивное поле порта): если страница несёт путь к
retained-файлу (I19), документный путь извлечения идёт по файлу — тело документа не
копируется в RAM и не пишется во второй временный файл. Без ``source_path`` поведение
байт-в-байт прежнее: ``extract_bytes(page.content, filename)``.

Адаптеры подменены фейками: docling/trafilatura не импортируются, веса не грузятся.
"""

from __future__ import annotations

import pytest

from core.cli.wiring import WiringError, _LocalExtractor
from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage


class _FakeHtml:
    version = "html-1"

    def __init__(self) -> None:
        self.calls: list[tuple[RawPage, ExtractionRoute]] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls.append((page, route))
        return ExtractedDoc(text="html", content_hash="h", extractor_version=self.version)

class _FakeDocuments:
    version = "doc-1"

    def __init__(self) -> None:
        self.byte_calls: list[tuple[bytes, str]] = []
        self.path_calls: list[tuple[str, str | None]] = []

    def extract_bytes(self, content: bytes, filename: str = "document.pdf") -> ExtractedDoc:
        self.byte_calls.append((content, filename))
        return ExtractedDoc(text="bytes", content_hash="b", extractor_version=self.version)

    def extract_path(
        self, path: str, *, filename_hint: str | None = None
    ) -> ExtractedDoc:
        self.path_calls.append((path, filename_hint))
        return ExtractedDoc(text="path", content_hash="p", extractor_version=self.version)

def _page(
    *,
    url: str = "https://docs.example.test/report.pdf",
    content: bytes = b"",
    source_path: str | None = None,
    content_type: str | None = "application/pdf",
) -> RawPage:
    return RawPage(
        url=url,
        content=content,
        fetched_at="2026-10-22T00:00:00+00:00",
        content_type=content_type,
        source_path=source_path,
    )

def _composite() -> tuple[_LocalExtractor, _FakeHtml, _FakeDocuments]:
    html = _FakeHtml()
    documents = _FakeDocuments()
    return _LocalExtractor(html, documents), html, documents  # type: ignore[arg-type]

def test_source_path_routes_documents_to_extract_path() -> None:
    composite, html, documents = _composite()

    doc = composite.extract(_page(source_path="/data/corpus_raw/p1/abc.bin"))

    assert doc.text == "path"
    assert documents.path_calls == [("/data/corpus_raw/p1/abc.bin", "document.pdf")]
    assert documents.byte_calls == []
    assert html.calls == []

def test_source_path_autodetect_uses_url_suffix_as_hint() -> None:
    composite, _, documents = _composite()

    composite.extract(
        _page(
            url="https://docs.example.test/files/spec.docx",
            content_type="application/octet-stream",
            source_path="/data/corpus_raw/p1/def.bin",
        )
    )

    assert documents.path_calls == [("/data/corpus_raw/p1/def.bin", "document.docx")]

def test_without_source_path_bytes_are_used_as_before() -> None:
    composite, _, documents = _composite()

    doc = composite.extract(_page(content=b"%PDF-1.7 bytes"))

    assert doc.text == "bytes"
    assert documents.byte_calls == [(b"%PDF-1.7 bytes", "document.pdf")]
    assert documents.path_calls == []

def test_html_route_ignores_source_path_routing() -> None:
    """``HTML`` — принудительный HTML-путь: документный адаптер не зовётся вообще."""
    composite, html, documents = _composite()

    composite.extract(_page(source_path="/data/corpus_raw/p1/abc.bin"), ExtractionRoute.HTML)

    assert documents.path_calls == [] and documents.byte_calls == []
    assert len(html.calls) == 1

def test_auto_route_without_document_signal_uses_html() -> None:
    composite, html, documents = _composite()

    composite.extract(
        _page(
            url="https://docs.example.test/article",
            content=b"<html>x</html>",
            content_type="text/html",
            source_path="/data/corpus_raw/p1/abc.bin",
        )
    )

    assert documents.path_calls == [] and documents.byte_calls == []
    assert len(html.calls) == 1

def test_without_docling_documents_go_to_html() -> None:
    html = _FakeHtml()
    composite = _LocalExtractor(html, None)  # type: ignore[arg-type]

    composite.extract(_page(content=b"%PDF-1.7"))

    assert len(html.calls) == 1

def test_version_composition_is_unchanged() -> None:
    composite, _, _ = _composite()

    assert composite.version == "html-1+doc-1"
    # Провенанс без ручек у фейка — пустой словарь (значения не выдумываются, A3).
    assert composite.options_provenance == {}


# ---------------------------------------------------------------------------
# Версия для ключа extract_cache = версия фактического движка (Д-8, побочный дефект)
# ---------------------------------------------------------------------------

def test_extractor_version_for_document_is_document_version() -> None:
    """Документный маршрут: ключ кэша — версия docling, а не композитная строка.

    Пока ключ пробы брался из ``composite.version`` (``html-1+doc-1``), а запись шла с
    ``ExtractedDoc.extractor_version`` (``doc-1``), ключи не совпадали **никогда**: кэш
    был write-only. Совпадение обязано следовать из одного диспетчера маршрута, а не из
    дисциплины вызывающего.
    """
    composite, html, _ = _composite()
    page = _page()

    assert composite.extractor_version_for(page, ExtractionRoute.AUTO) == "doc-1"
    assert composite.extractor_version_for(page, ExtractionRoute.DOCUMENT) == "doc-1"
    assert composite.version == "html-1+doc-1", "композитная версия остаётся провенансом"
    assert html.calls == []


def test_extractor_version_for_matches_stamped_version() -> None:
    """Ключ пробы равен штампу результата — на каждом маршруте, а не только на документном."""
    composite, _, _ = _composite()
    cases = [
        (_page(url="https://e.test/x", content_type="text/html"), ExtractionRoute.AUTO),
        (_page(url="https://e.test/x.css", content_type="text/css"), ExtractionRoute.HTML),
        (_page(url="https://e.test/a.md", content_type=None), ExtractionRoute.AUTO),
        (_page(), ExtractionRoute.AUTO),
    ]
    for page, route in cases:
        predicted = composite.extractor_version_for(page, route)
        stamped = composite.extract(page, route).extractor_version
        assert predicted == stamped, (page.url, route)


def test_extractor_version_for_without_docling_falls_back_to_html() -> None:
    """Без docling документный URL обслуживает HTML-путь — и ключ обязан это отражать."""
    composite = _LocalExtractor(_FakeHtml(), None)  # type: ignore[arg-type]

    assert composite.extractor_version_for(_page()) == "html-1"
    assert composite.extract(_page()).extractor_version == "html-1"


# ---------------------------------------------------------------------------
# Текстовые форматы и EPUB: третий и четвёртый маршруты композита
# ---------------------------------------------------------------------------


class _FakeText:
    version = "text-1"

    def __init__(self) -> None:
        self.calls: list[RawPage] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls.append(page)
        return ExtractedDoc(
            text="text", content_hash="t", extractor_version=self.version, family="text"
        )


class _FakeEpub:
    version = "epub-1"

    def __init__(self) -> None:
        self.calls: list[RawPage] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls.append(page)
        return ExtractedDoc(
            text="epub", content_hash="e", extractor_version=self.version, family="epub"
        )


def _composite_with_text() -> tuple[_LocalExtractor, _FakeHtml, _FakeDocuments, _FakeText, _FakeEpub]:
    html = _FakeHtml()
    documents = _FakeDocuments()
    text = _FakeText()
    epub = _FakeEpub()
    composite = _LocalExtractor(
        html, documents, text=text, epub=epub  # type: ignore[arg-type]
    )
    return composite, html, documents, text, epub


def test_txt_url_reaches_text_reader_and_never_trafilatura() -> None:
    """Регресс на 72 × ``.txt`` в ``parser_failed`` (22.10): текст не идёт в HTML-путь."""
    composite, html, documents, text, epub = _composite_with_text()

    doc = composite.extract(
        _page(
            url="https://edu.example.test/files/dba1_03_install_lab.txt",
            content=b"lab instructions",
            content_type="text/plain; charset=utf-8",
        )
    )

    assert doc.text == "text"
    assert len(text.calls) == 1
    assert html.calls == []
    assert documents.byte_calls == [] and documents.path_calls == []
    assert epub.calls == []


def test_txt_without_docling_still_reaches_text_reader() -> None:
    """Без docling композит остаётся композитом: текст не откатывается в HTML."""
    html = _FakeHtml()
    composite = _LocalExtractor(html, None)  # type: ignore[arg-type]

    doc = composite.extract(
        _page(url="https://x.test/notes.txt", content=b"plain", content_type="text/plain")
    )

    assert html.calls == []
    assert doc.text == "plain"
    assert doc.family == "text"


def test_textual_content_type_routes_suffixless_url() -> None:
    composite, html, _, text, _ = _composite_with_text()

    composite.extract(
        _page(url="https://x.test/download?id=7", content=b"x", content_type="text/plain")
    )

    assert len(text.calls) == 1
    assert html.calls == []


def test_markdown_url_reaches_text_reader_with_markdown_family() -> None:
    """``.md`` читается текстовым читателем и помечается семейством ``markdown``.

    Семейство — вход выбора чанкера: разметку режет ``MarkdownChunker``, а не абзацный.
    """
    html = _FakeHtml()
    composite = _LocalExtractor(html, None)  # type: ignore[arg-type]

    doc = composite.extract(
        _page(url="https://x.test/guide.md", content=b"# Title", content_type="text/markdown")
    )

    assert html.calls == []
    assert doc.text == "# Title"
    assert doc.family == "markdown"


def test_epub_url_reaches_epub_reader() -> None:
    composite, html, documents, text, epub = _composite_with_text()

    doc = composite.extract(
        _page(
            url="https://repo.example.test/books/guide.epub",
            content=b"PK\x03\x04",
            content_type="application/epub+zip",
        )
    )

    assert doc.text == "epub"
    assert len(epub.calls) == 1
    assert text.calls == [] and html.calls == []
    assert documents.byte_calls == [] and documents.path_calls == []


def test_epub_by_content_type_routes_suffixless_url() -> None:
    composite, _, _, _, epub = _composite_with_text()

    composite.extract(
        _page(url="https://x.test/download/9", content=b"PK", content_type="application/epub+zip")
    )

    assert len(epub.calls) == 1


def test_suffixless_pdf_still_routes_to_documents() -> None:
    composite, html, documents, text, epub = _composite_with_text()

    composite.extract(
        _page(url="https://x.test/download?id=7", content=b"%PDF-1.7", content_type="application/pdf")
    )

    assert documents.byte_calls == [(b"%PDF-1.7", "document.pdf")]
    assert text.calls == [] and epub.calls == [] and html.calls == []


def test_document_suffix_beats_textual_content_type() -> None:
    """``report.pdf`` как ``text/plain`` — документ, а не текст (реестр решает)."""
    composite, _html, documents, text, epub = _composite_with_text()

    composite.extract(
        _page(
            url="https://x.test/files/report.pdf",
            content=b"%PDF-1.7",
            content_type="text/plain",
        )
    )

    assert documents.byte_calls == [(b"%PDF-1.7", "document.pdf")]
    assert text.calls == [] and epub.calls == []


def test_explicit_html_route_beats_textual_detection() -> None:
    composite, html, _, text, _ = _composite_with_text()

    composite.extract(
        _page(url="https://x.test/notes.txt", content=b"plain"), ExtractionRoute.HTML
    )

    assert len(html.calls) == 1
    assert text.calls == []


def test_explicit_document_route_beats_textual_detection() -> None:
    composite, html, documents, text, _ = _composite_with_text()

    composite.extract(
        _page(url="https://x.test/notes.txt", content=b"plain"), ExtractionRoute.DOCUMENT
    )

    assert documents.byte_calls == [(b"plain", "document.pdf")]
    assert text.calls == [] and html.calls == []


def test_html_page_still_goes_to_trafilatura() -> None:
    composite, html, _documents, text, epub = _composite_with_text()

    composite.extract(
        _page(url="https://x.test/article", content=b"<html>x</html>", content_type="text/html")
    )

    assert len(html.calls) == 1
    assert text.calls == [] and epub.calls == []


# ---------------------------------------------------------------------------
# P7: ось crawl.pdf_pipeline — PDF обслуживает выбранный механизм
# ---------------------------------------------------------------------------


class _FakePdf(_FakeDocuments):
    """Фейк второго PDF-механизма (``text_layer``): свой штамп версии и свой текст."""

    version = "pdfx-1"

    def extract_bytes(self, content: bytes, filename: str = "document.pdf") -> ExtractedDoc:
        self.byte_calls.append((content, filename))
        return ExtractedDoc(text="textlayer", content_hash="t", extractor_version=self.version)

    def extract_path(
        self, path: str, *, filename_hint: str | None = None
    ) -> ExtractedDoc:
        self.path_calls.append((path, filename_hint))
        return ExtractedDoc(text="textlayer", content_hash="t", extractor_version=self.version)


def _composite_with_pdf(
    *, pdf_required: bool = True
) -> tuple[_LocalExtractor, _FakeHtml, _FakeDocuments, _FakePdf]:
    html = _FakeHtml()
    documents = _FakeDocuments()
    pdf = _FakePdf()
    composite = _LocalExtractor(
        html,
        documents,
        pdf=pdf,  # type: ignore[arg-type]
        pdf_required=pdf_required,
    )
    return composite, html, documents, pdf


def test_text_layer_routes_pdf_to_pdf_backend() -> None:
    """``text_layer``: PDF уходит в свой механизм, docling не трогается вовсе."""
    composite, html, documents, pdf = _composite_with_pdf()

    doc = composite.extract(_page(url="https://x.test/files/spec.pdf"))

    assert doc.text == "textlayer"
    assert len(pdf.byte_calls) == 1
    assert documents.byte_calls == [] and documents.path_calls == []
    assert html.calls == []


def test_text_layer_reads_retained_path_not_bytes() -> None:
    """I19: retained-файл читает адаптер (``extract_path``), тело не копируется в RAM."""
    composite, _, documents, pdf = _composite_with_pdf()

    composite.extract(_page(source_path="/data/corpus_raw/p1/abc.bin"))

    assert pdf.path_calls == [("/data/corpus_raw/p1/abc.bin", "document.pdf")]
    assert pdf.byte_calls == []
    assert documents.path_calls == []


def test_text_layer_keeps_office_in_docling() -> None:
    """Механизм ``text_layer`` говорит только про PDF: Office остаётся docling'у."""
    composite, _, documents, pdf = _composite_with_pdf()

    composite.extract(
        _page(
            url="https://x.test/files/report.docx",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            content=b"PK\x03\x04",
        )
    )

    assert documents.byte_calls == [(b"PK\x03\x04", "document.docx")]
    assert pdf.byte_calls == [] and pdf.path_calls == []


def test_structured_pdf_goes_to_docling() -> None:
    """``structured`` (pdf не передан) — прежний docling, байт-в-байт."""
    composite, _, documents = _composite()

    composite.extract(_page())

    assert documents.byte_calls == [(b"", "document.pdf")]


def test_document_route_picks_pdf_backend_without_suffix() -> None:
    """Принудительный документный маршрут без суффикса — ``document.pdf``: механизм PDF."""
    composite, html, documents, pdf = _composite_with_pdf()

    composite.extract(
        _page(url="https://x.test/download?id=7", content_type=None, content=b"%PDF-1.7"),
        ExtractionRoute.DOCUMENT,
    )

    assert pdf.byte_calls == [(b"%PDF-1.7", "document.pdf")]
    assert documents.byte_calls == [] and html.calls == []


def test_version_for_follows_selected_pdf_backend() -> None:
    """Ключ ``extract_cache`` — версия фактического механизма (не композита)."""
    composite, _, documents, _ = _composite_with_pdf()

    assert composite.extractor_version_for(_page()) == "pdfx-1"
    assert composite.extractor_version_for(
        _page(
            url="https://x.test/files/report.docx", content_type="application/msword"
        )
    ) == "doc-1"
    # Композит называет оба движка — это провенанс, а не ключ кэша.
    assert composite.version == "html-1+doc-1+pdfx-1"
    assert documents.byte_calls == []


def test_required_pdf_backend_missing_is_visible_not_silent_html() -> None:
    """Явно выбранный механизм без адаптера — отказ, а не тихий переход в HTML."""
    composite = _LocalExtractor(_FakeHtml(), None, pdf_required=True)  # type: ignore[arg-type]

    with pytest.raises(WiringError):
        composite.extract(_page())
    with pytest.raises(WiringError):
        composite.extractor_version_for(_page())


def test_docling_fallback_without_axis_is_unchanged() -> None:
    """Без оси (``pdf_required=False``) отсутствие docling по-прежнему уводит в HTML."""
    composite = _LocalExtractor(_FakeHtml(), None)  # type: ignore[arg-type]

    assert composite.extract(_page()).extractor_version == "html-1"


# ---------------------------------------------------------------------------
# P7: сборка воркер-экстрактора по оси
# ---------------------------------------------------------------------------


def test_worker_extractor_selects_pdfium_for_text_layer(monkeypatch: pytest.MonkeyPatch) -> None:
    """``build_worker_extractor(..., text_layer)``: PDF — pdfium, версия — фактическая."""
    from core.adapters import extract_pdf_text
    from core.cli.wiring import build_worker_extractor
    from core.domain.enums import DocumentLevel, PdfPipeline

    if not extract_pdf_text.pdfium_installed():
        pytest.skip("pypdfium2 не установлен")

    composite = build_worker_extractor(DocumentLevel.FULL, PdfPipeline.TEXT_LAYER)

    assert composite.extractor_version_for(_page()) == "pdfium-text-2"
    assert composite.version.endswith("+pdfium-text-2")
    # Дефолтная сборка — прежний docling (проверка в тестах версии композита).
    assert build_worker_extractor().extractor_version_for(_page()) != "pdfium-text-2"


def test_worker_extractor_fails_visibly_without_pdfium(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Явно выбранный ``text_layer`` без ``pypdfium2`` — ``WiringError`` (EXIT_USAGE)."""
    from core.adapters import extract_pdf_text
    from core.cli.wiring import build_worker_extractor
    from core.domain.enums import DocumentLevel, PdfPipeline

    monkeypatch.setattr(extract_pdf_text, "pdfium_installed", lambda: False)

    with pytest.raises(WiringError, match="pypdfium2"):
        build_worker_extractor(DocumentLevel.FULL, PdfPipeline.TEXT_LAYER)


def test_build_adapters_honours_explicit_pdf_pipeline() -> None:
    """Порты видят тот же механизм, что и Ш5: иначе диагностика мерила бы другой движок."""
    from core.adapters import extract_pdf_text
    from core.cli.wiring import build_adapters
    from core.domain.enums import PdfPipeline

    if not extract_pdf_text.pdfium_installed():
        pytest.skip("pypdfium2 не установлен")

    ports = build_adapters(profile="local", pdf_pipeline=PdfPipeline.TEXT_LAYER)

    assert ports.extractor is not None
    assert ports.extractor.extractor_version_for(_page()) == "pdfium-text-2"
    # Без явной оси — прежний docling (дефолт ``structured``).
    default = build_adapters(profile="local")
    assert default.extractor is not None
    assert default.extractor.extractor_version_for(_page()) != "pdfium-text-2"
