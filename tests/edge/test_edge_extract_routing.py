"""Маршрутизация экстракции в composition root edge (DMZ): ``source_path`` → ``extract_path``.

Тот же контракт, что у CLI-композита (``tests/cli/test_extract_routing.py``): страница с
``source_path`` (retained-файл, I19) извлекается по пути, без копии тела в RAM; без поля —
прежний ``extract_bytes``. Реальные адаптеры подменяются после сборки композита, поэтому
docling-веса и trafilatura не нужны.
"""

from __future__ import annotations

import core.edge.__main__ as edge_main
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

def _composite() -> tuple[edge_main._EdgeExtractor, _FakeHtml, _FakeDocuments]:
    composite = edge_main._EdgeExtractor()
    html = _FakeHtml()
    documents = _FakeDocuments()
    composite._html = html  # type: ignore[assignment]
    composite._documents = documents  # type: ignore[assignment]
    return composite, html, documents

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

def test_source_path_routes_documents_to_extract_path() -> None:
    composite, html, documents = _composite()

    doc = composite.extract(_page(source_path="/data/corpus_raw/p1/abc.bin"))

    assert doc.text == "path"
    assert documents.path_calls == [("/data/corpus_raw/p1/abc.bin", "document.pdf")]
    assert documents.byte_calls == []
    assert html.calls == []

def test_without_source_path_bytes_are_used_as_before() -> None:
    composite, _, documents = _composite()

    composite.extract(_page(content=b"%PDF-1.7 bytes"))

    assert documents.byte_calls == [(b"%PDF-1.7 bytes", "document.pdf")]
    assert documents.path_calls == []

def test_html_route_ignores_source_path_routing() -> None:
    composite, html, documents = _composite()

    composite.extract(_page(source_path="/data/corpus_raw/p1/abc.bin"), ExtractionRoute.HTML)

    assert documents.path_calls == [] and documents.byte_calls == []
    assert len(html.calls) == 1
