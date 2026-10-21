"""Тесты распознавания заголовков markdown-чанкера (``md-heading-2``).

Проверяются два исправленных дефекта ``_extract_sections``:

* **fence-aware** — заголовки внутри ``` ```/~~~ ``` блоков кода не распознаются и
  не обрезают стек заголовков; незакрытый fence не теряет текст;
* **setext** — ``Title`` + ``=====``/``-----`` даёт непустой ``heading_path``,
  а ``---`` как тематический разрыв заголовком не становится.

Плюс регрессия детерминизма (одинаковый вход → одинаковые ``chunk_id``/``content_hash``)
и фиксация версии чанкера. Нейтрален (§1.3).
"""

from __future__ import annotations

from core.adapters.chunker_markdown import MarkdownChunker


def _chunk(md: str, **kwargs: int):
    return MarkdownChunker(**kwargs).chunk("doc", md)


def _chunk_with(chunks, needle: str):
    """Первый чанк, содержащий ``needle`` (для проверки heading_path после fence)."""
    for chunk in chunks:
        if needle in chunk.text:
            return chunk
    raise AssertionError(f"чанк с {needle!r} не найден: {[c.text for c in chunks]}")


# ---------------------------------------------------------------------------
# fence-aware: заголовки внутри блока кода
# ---------------------------------------------------------------------------


def test_hash_comment_inside_bash_fence_is_not_heading() -> None:
    md = "# H1\n\nтекст\n\n```bash\n# shell comment\necho hi\n```\n\nпосле\n"
    chunks = _chunk(md)
    assert [c.heading_path for c in chunks] == ["H1"]
    assert "# shell comment" in chunks[0].text
    assert "после" in chunks[0].text


def test_heading_inside_fence_does_not_truncate_stack() -> None:
    md = (
        "# H1\n\n## H2\n\nтекст\n\n```md\n#### fake heading\nкод\n```\n\nпосле\n"
    )
    chunks = _chunk(md)
    assert _chunk_with(chunks, "после").heading_path == "H1 > H2"
    assert _chunk_with(chunks, "#### fake heading").heading_path == "H1 > H2"


def test_tilde_fence_behaves_like_backtick() -> None:
    md = "# H1\n\n~~~bash\n# comment\n~~~\n\nпосле\n"
    chunks = _chunk(md)
    assert all(c.heading_path == "H1" for c in chunks)
    assert _chunk_with(chunks, "после").heading_path == "H1"


def test_fence_with_info_string() -> None:
    md = "# H1\n\n```python\n# not a heading\ndef f():\n    pass\n```\n\nпосле\n"
    chunks = _chunk(md)
    assert _chunk_with(chunks, "def f").heading_path == "H1"
    assert _chunk_with(chunks, "после").heading_path == "H1"
    # Info-string не теряется из текста чанка.
    assert "```python" in _chunk_with(chunks, "def f").text


def test_fence_closed_by_shorter_or_other_marker_stays_code() -> None:
    # Открыт ```` (4), закрытие ``` (3) и ~~~ не закрывают; `# x` остаётся кодом.
    md = "# H1\n\n````md\n```\n# x\n~~~\n````\n\nпосле\n"
    chunks = _chunk(md)
    assert _chunk_with(chunks, "после").heading_path == "H1"
    assert _chunk_with(chunks, "# x").heading_path == "H1"


def test_unclosed_fence_keeps_text() -> None:
    md = "# H1\n\n```\nкод\n# не заголовок\n"
    chunks = _chunk(md)
    assert all(c.heading_path == "H1" for c in chunks)
    assert "# не заголовок" in chunks[0].text
    assert "код" in chunks[0].text


def test_fence_closing_with_info_string_does_not_close() -> None:
    md = "# H1\n\n```\nкод\n``` tail\n# x\n```\n\nпосле\n"
    chunks = _chunk(md)
    assert _chunk_with(chunks, "после").heading_path == "H1"
    assert _chunk_with(chunks, "# x").heading_path == "H1"


# ---------------------------------------------------------------------------
# setext-заголовки
# ---------------------------------------------------------------------------


def test_setext_h1_and_h2() -> None:
    md = "Title\n=====\n\nтело\n\nSub\n-----\n\nтело2\n"
    chunks = _chunk(md)
    assert chunks[0].heading_path == "Title"
    assert _chunk_with(chunks, "тело2").heading_path == "Title > Sub"


def test_setext_heading_text_not_in_content() -> None:
    md = "Title\n=====\n\nтело\n"
    chunks = _chunk(md)
    assert chunks[0].text == "тело"
    assert "Title" not in chunks[0].text


def test_thematic_break_is_not_heading() -> None:
    md = "# H1\n\nтекст\n\n---\n\nещё\n"
    chunks = _chunk(md)
    assert all(c.heading_path == "H1" for c in chunks)


def test_thematic_break_at_document_start_is_not_heading() -> None:
    md = "---\n\nтекст\n"
    chunks = _chunk(md)
    assert all(c.heading_path == "" for c in chunks)


def test_setext_after_list_item_is_not_heading() -> None:
    md = "# H1\n\n- пункт\n---\n\nещё\n"
    chunks = _chunk(md)
    assert all(c.heading_path == "H1" for c in chunks)


def test_setext_after_fence_line_is_not_heading() -> None:
    md = "# H1\n\n```\nкод\n```\n---\n\nещё\n"
    chunks = _chunk(md)
    assert all(c.heading_path == "H1" for c in chunks)


def test_setext_after_atx_heading_is_not_heading() -> None:
    md = "# H1\n===\n\nтело\n"
    chunks = _chunk(md)
    assert chunks[0].heading_path == "H1"


# ---------------------------------------------------------------------------
# YAML-front-matter (консервативно: только в начале документа и с телом-отображением)
# ---------------------------------------------------------------------------


def test_yaml_front_matter_is_not_heading() -> None:
    md = "---\ntitle: x\n# yaml comment\n---\n\n# H1\n\nтело\n"
    chunks = _chunk(md)
    # Front-matter остаётся текстом секции без заголовка; H1 — единственный заголовок.
    assert chunks[0].heading_path == ""
    assert "title: x" in chunks[0].text
    assert chunks[-1].heading_path == "H1"
    assert "title: x" not in chunks[-1].text


def test_dashes_without_front_matter_body_keep_commonmark_behaviour() -> None:
    # Нет YAML-тела (нет `:`) → front-matter не распознаётся, setext работает.
    md = "---\nSome text\n---\n\nтело\n"
    chunks = _chunk(md)
    assert chunks[-1].heading_path == "Some text"
    assert chunks[-1].text == "тело"


# ---------------------------------------------------------------------------
# версия и детерминизм
# ---------------------------------------------------------------------------


def test_version_is_md_heading_2() -> None:
    assert MarkdownChunker.version == "md-heading-2"


def test_chunking_is_deterministic() -> None:
    md = (
        "# H1\n\n```bash\n# comment\n```\n\nTitle\n=====\n\n"
        "## H2\n\nтекст " * 20
    )
    first = _chunk(md, chunk_size=120, chunk_overlap=20)
    second = _chunk(md, chunk_size=120, chunk_overlap=20)
    assert [(c.chunk_id, c.content_hash) for c in first] == [
        (c.chunk_id, c.content_hash) for c in second
    ]
    assert [c.text for c in first] == [c.text for c in second]
