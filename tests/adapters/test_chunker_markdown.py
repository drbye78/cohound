"""Тесты распознавания заголовков markdown-чанкера (``md-heading-4``).

Проверяются исправленные дефекты ``_extract_sections``:

* **fence-aware** — заголовки внутри ``` ```/~~~ ``` блоков кода не распознаются и
  не обрезают стек заголовков; незакрытый fence не теряет текст;
* **setext** — ``Title`` + ``=====``/``-----`` даёт непустой ``heading_path``,
  а ``---`` как тематический разрыв заголовком не становится.
* **относительные уровни** — ATX-уровни считаются от самого мелкого уровня документа,
  поэтому документ без ``#`` (trafilatura, docling для Office) не получает ложную
  вложенность из двух соседних ``##``.

Плюс регрессия детерминизма (одинаковый вход → одинаковые ``chunk_id``/``content_hash``)
и фиксация версии чанкера.

Кроме распознавания заголовков проверяются свойства самой нарезки:

* **границы куска (C4)** — кусок не начинается и не кончается обрывком слова, конец
  предпочитает перевод строки, фактическое перекрытие ≤ ``chunk_overlap``;
* **порог фрагмента (C6)** — обломок разметки (``|``, ``Листов 81``) в индекс не попадает.

Нейтрален (§1.3).
"""

from __future__ import annotations

import itertools

import pytest

from core.adapters.chunker_markdown import (
    MIN_CHUNK_SIZE,
    MarkdownChunker,
    MarkdownChunkerPort,
)
from core.ports.chunker import ChunkerPort
from core.ports.types import ExtractedDoc


def _chunk(md: str, **kwargs: int):
    """Чанкер для тестов распознавания заголовков — с отключённым порогом фрагмента.

    Тела секций здесь намеренно короткие (``тело``), а ``min_chunk_size`` по умолчанию
    отбрасывает куски короче :data:`MIN_CHUNK_SIZE`. Тесты ниже — про заголовки, а не про
    размер, поэтому порог снят явно; сам порог проверяется отдельно
    (``test_min_chunk_size_*``), а не «попутно».
    """
    kwargs.setdefault("min_chunk_size", 1)
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
# относительные уровни: документ без `#` не получает вложенность из ничего
# ---------------------------------------------------------------------------


def test_adjacent_h2_without_h1_are_siblings() -> None:
    """Два соседних ``##`` — два независимых раздела, а не родитель и ребёнок.

    Так выглядят заголовки из trafilatura (первый заголовок страницы — h2) и из
    docling для DOCX/XLSX: ``#`` в тексте отсутствует. ``heading_path`` — привязка
    цитаты к разделу, поэтому «РАЗДЕЛ 1 > РАЗДЕЛ 2» была ложью в провенансе.
    """
    md = "## РАЗДЕЛ 1\n\nтело1\n\n## РАЗДЕЛ 2\n\nтело2\n"
    chunks = _chunk(md)
    assert _chunk_with(chunks, "тело1").heading_path == "РАЗДЕЛ 1"
    assert _chunk_with(chunks, "тело2").heading_path == "РАЗДЕЛ 2"


def test_h3_nests_under_h2_when_h1_is_absent() -> None:
    """Относительность не отменяет иерархию: ``###`` под ``##`` — ребёнок."""
    md = (
        "## РАЗДЕЛ 1\n\nтело1\n\n### ПОДРАЗДЕЛ\n\nтело2\n\n"
        "## РАЗДЕЛ 2\n\nтело3\n"
    )
    chunks = _chunk(md)
    assert _chunk_with(chunks, "тело1").heading_path == "РАЗДЕЛ 1"
    assert _chunk_with(chunks, "тело2").heading_path == "РАЗДЕЛ 1 > ПОДРАЗДЕЛ"
    assert _chunk_with(chunks, "тело3").heading_path == "РАЗДЕЛ 2"


def test_document_starting_with_h1_keeps_absolute_structure() -> None:
    """Документ с ``#`` режется как прежде: корень = 1, пути абсолютны."""
    md = (
        "# РАЗДЕЛ 1\n\nтело1\n\n## ПОДРАЗДЕЛ\n\nтело2\n\n"
        "## РАЗДЕЛ 2\n\nтело3\n"
    )
    chunks = _chunk(md)
    assert _chunk_with(chunks, "тело1").heading_path == "РАЗДЕЛ 1"
    assert _chunk_with(chunks, "тело2").heading_path == "РАЗДЕЛ 1 > ПОДРАЗДЕЛ"
    assert _chunk_with(chunks, "тело3").heading_path == "РАЗДЕЛ 1 > РАЗДЕЛ 2"


def test_shallower_heading_lowers_root_for_following_headings() -> None:
    """`#` после `##` понижает корень; уже выданные чанки не переписываются."""
    md = "## ЧАСТЬ\n\nтело1\n\n# ГЛАВА\n\nтело2\n\n## РАЗДЕЛ\n\nтело3\n"
    chunks = _chunk(md)
    assert _chunk_with(chunks, "тело1").heading_path == "ЧАСТЬ"
    assert _chunk_with(chunks, "тело2").heading_path == "ГЛАВА"
    assert _chunk_with(chunks, "тело3").heading_path == "ГЛАВА > РАЗДЕЛ"


def test_setext_h1_keeps_nesting_for_following_h2() -> None:
    """setext вносит свой уровень (1/2) в корень: ``Title`` + ``##`` остаётся вложением."""
    md = "Title\n=====\n\nтело\n\n## Sub\n\nтело2\n"
    chunks = _chunk(md)
    assert _chunk_with(chunks, "тело").heading_path == "Title"
    assert _chunk_with(chunks, "тело2").heading_path == "Title > Sub"


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


def test_version_is_md_heading_4() -> None:
    assert MarkdownChunker.version == "md-heading-4"


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


# ---------------------------------------------------------------------------
# границы куска: слово и перекрытие (C4)
# ---------------------------------------------------------------------------


def _words_text(n: int = 400) -> str:
    """Длинный текст без переводов строк: единственная граница — пробел."""
    return " ".join(f"слово{index}" for index in range(n))


def _common_overlap(left: str, right: str) -> int:
    """Длина фактического перекрытия: суффикс левого = префикс правого."""
    for size in range(min(len(left), len(right)), 0, -1):
        if left.endswith(right[:size]):
            return size
    return 0


def test_split_does_not_break_words() -> None:
    """Куски режутся по пробелам: ни один кусок не содержит обрывка слова (C4)."""
    text = _words_text()
    words = set(text.split())
    chunks = MarkdownChunker(chunk_size=120, chunk_overlap=30).chunk("doc", text)

    assert len(chunks) > 10
    for chunk in chunks:
        assert chunk.text.split()
        assert set(chunk.text.split()) <= words, chunk.text
        assert len(chunk.text) <= 120


def test_overlap_never_exceeds_chunk_overlap() -> None:
    """Фактическое перекрытие ≤ ``chunk_overlap``: 30 — потолок, а не обещание (C4)."""
    text = _words_text()
    chunks = MarkdownChunker(chunk_size=120, chunk_overlap=30).chunk("doc", text)

    overlaps = [
        _common_overlap(previous.text, current.text)
        for previous, current in itertools.pairwise(chunks)
    ]
    assert overlaps and all(0 < overlap <= 30 for overlap in overlaps)


def test_cut_prefers_line_boundary() -> None:
    """Строки без пробелов: конец куска и начало следующего — на переводе строки."""
    lines = [f"строка{index:03d}" for index in range(200)]
    text = "\n".join(lines)
    chunks = MarkdownChunker(chunk_size=50, chunk_overlap=10).chunk("doc", text)

    assert len(chunks) > 10
    for chunk in chunks:
        assert set(chunk.text.split("\n")) <= set(lines), chunk.text
        assert len(chunk.text) <= 50


def test_split_is_deterministic_for_long_text() -> None:
    """Снап к границам слова не вносит недетерминизма (R7)."""
    first = MarkdownChunker(chunk_size=120, chunk_overlap=30).chunk("doc", _words_text())
    second = MarkdownChunker(chunk_size=120, chunk_overlap=30).chunk("doc", _words_text())
    assert [(c.chunk_id, c.content_hash) for c in first] == [
        (c.chunk_id, c.content_hash) for c in second
    ]


# ---------------------------------------------------------------------------
# порог фрагмента (C6)
# ---------------------------------------------------------------------------


def test_min_chunk_size_default_is_declared() -> None:
    """Порог объявлен константой и действует по умолчанию (C6)."""
    assert MIN_CHUNK_SIZE == 20
    assert MarkdownChunker().min_chunk_size == MIN_CHUNK_SIZE


def test_fragment_below_min_chunk_size_is_not_indexed() -> None:
    """Обломок разметки не попадает в индекс: он не может быть цитатой (C6/I22)."""
    assert MarkdownChunker().chunk("doc", "# H1\n|") == []
    assert MarkdownChunker().chunk("doc", "# H1\nЛистов 81") == []
    # Осмысленная строка порог проходит (и становится единственным чанком секции).
    chunks = MarkdownChunker().chunk("doc", "# H1\nВерсия для работы в Docker")
    assert [c.text for c in chunks] == ["Версия для работы в Docker"]


def test_min_chunk_size_must_fit_chunk_size() -> None:
    with pytest.raises(ValueError):
        MarkdownChunker(chunk_size=40, min_chunk_size=40)
    with pytest.raises(ValueError):
        MarkdownChunker(chunk_overlap=-1)


# ---------------------------------------------------------------------------
# порт-контракт (T7.3)
# ---------------------------------------------------------------------------


def test_markdown_chunker_port_conformance() -> None:
    """``MarkdownChunkerPort`` структурно реализует ``ChunkerPort`` (T7.3).

    Порт принимает ``ExtractedDoc`` + ``document_id`` и обязан отдавать ту же нарезку,
    что и базовый ``MarkdownChunker``: нумерация ``chunk_no`` с нуля, ``chunk_id`` из
    ``document_id`` и ``heading_path`` по заголовку секции.
    """
    chunker = MarkdownChunkerPort()
    assert isinstance(chunker, ChunkerPort)

    chunks = chunker.chunk(
        ExtractedDoc(
            text="# H1\nтело первого раздела документа",
            content_hash="h",
            extractor_version="v",
        ),
        "doc1",
    )

    assert [chunk.chunk_id for chunk in chunks] == ["doc1#0"]
    assert [chunk.chunk_no for chunk in chunks] == [0]
    assert chunks[0].heading_path == "H1"
