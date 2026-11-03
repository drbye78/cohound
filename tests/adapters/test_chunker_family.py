"""Маршрутизация композита чанкеров по семейству формата.

``FamilyChunker`` — **один** ``ChunkerPort`` для всего корпуса: семейство ``text`` уходит
в абзацный :class:`~core.adapters.chunker_text.TextChunker`, markdown/HTML/EPUB — в
:class:`~core.adapters.chunker_markdown.MarkdownChunker`. Тест фиксирует именно выбор:
если маршрут поедет, точные срезы ``text-para-4`` перестанут попадать в корпус незаметно
(тип чанка один и тот же, отличается только форма). Поэтому различие проверяется по
свойствам результата: у текстового семейства ``heading_path == ""`` и смещения — точные
срезы, у markdown — путь заголовка и своя версия.

Нейтрален (§1.3).
"""

from __future__ import annotations

from core.adapters.chunker_family import (
    FamilyChunker,
    composite_chunker_version,
    composite_chunking_spec,
)
from core.adapters.chunker_markdown import MarkdownChunker
from core.adapters.chunker_text import TextChunker
from core.ports.types import ExtractedDoc

_TEXT = "# not a heading\n\nПервый абзац достаточно длинный.\n\nВторой абзац тоже длинный."
_MARKDOWN = "# Раздел\n\n## Подраздел\n\nТекст раздела достаточно длинный."


def _doc(text: str, family: str | None) -> ExtractedDoc:
    return ExtractedDoc(text=text, content_hash="h", extractor_version="v", family=family)


def test_text_family_goes_to_text_chunker() -> None:
    chunks = FamilyChunker().chunk(_doc(_TEXT, "text"), "doc")

    assert chunks
    assert all(chunk.heading_path == "" for chunk in chunks)
    assert all(chunk.char_start is not None and chunk.char_end is not None for chunk in chunks)
    # Семейство ``text`` не разбирает markdown: решётки остаются текстом первого чанка.
    assert chunks[0].text.startswith("# not a heading")


def test_markdown_family_goes_to_markdown_chunker() -> None:
    chunks = FamilyChunker().chunk(_doc(_MARKDOWN, "markdown"), "doc")

    assert chunks
    assert any(chunk.heading_path for chunk in chunks)
    assert any("# Раздел" in chunk.text for chunk in chunks)


def test_epub_and_none_families_go_to_markdown_chunker() -> None:
    for family in ("epub", None):
        chunks = FamilyChunker().chunk(_doc(_MARKDOWN, family), "doc")

        assert any(chunk.heading_path for chunk in chunks), family


def test_markdown_family_does_not_use_text_chunker() -> None:
    """Точные смещения есть у обоих, поэтому маршрут различается структурой, не полем."""
    text_doc = FamilyChunker().chunk(_doc(_TEXT, "text"), "doc")
    md_doc = FamilyChunker().chunk(_doc(_TEXT, "markdown"), "doc")

    assert all(chunk.heading_path == "" for chunk in text_doc)
    assert any("# not a heading" in chunk.text for chunk in text_doc)
    assert any(chunk.heading_path for chunk in md_doc)


def test_version_is_composite_of_both_chunkers() -> None:
    assert composite_chunker_version() == f"{MarkdownChunker.version}+{TextChunker.version}"
    assert FamilyChunker().version == composite_chunker_version()
    assert TextChunker.version in FamilyChunker().version


def test_chunking_spec_declares_text_geometry() -> None:
    spec = composite_chunking_spec()

    assert spec["text"] == {
        "chunk_size": TextChunker().chunk_size,
        "min_chunk_size": TextChunker().min_chunk_size,
    }
    assert spec["embed_input"] == ["text"]