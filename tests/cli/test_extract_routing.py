"""Маршрутизация экстракции в composition root CLI: ``source_path`` → ``extract_path``.

Контракт ``RawPage.source_path`` (аддитивное поле порта): если страница несёт путь к
retained-файлу (I19), документный путь извлечения идёт по файлу — тело документа не
копируется в RAM и не пишется во второй временный файл. Без ``source_path`` поведение
байт-в-байт прежнее: ``extract_bytes(page.content, filename)``.

Адаптеры подменены фейками: docling/trafilatura не импортируются, веса не грузятся.
"""

from __future__ import annotations

from core.cli.wiring import _LocalExtractor
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
