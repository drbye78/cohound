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


def test_setext_heading_line_is_part_of_section_text() -> None:
    """Строка заголовка входит в ``text`` своей секции (P1).

    ``md-heading-4`` её выбрасывала: заголовок жил только в ``heading_path``, и
    конкатенация чанков не воспроизводила вход. Теперь ``text`` — точный срез.
    """
    md = "Title\n=====\n\nтело\n"
    chunks = _chunk(md)
    assert chunks[0].heading_path == "Title"
    assert chunks[0].text == "Title\n=====\n\nтело\n"


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
    # Строка заголовка теперь в тексте (P1): секция — точный срез источника.
    assert chunks[-1].text == "Some text\n---\n\nтело\n"


# ---------------------------------------------------------------------------
# версия и детерминизм
# ---------------------------------------------------------------------------


def test_version_is_md_heading_5() -> None:
    """Нарезка по AST — другая версия правил (``execution_versions``, §13)."""
    assert MarkdownChunker.version == "md-heading-5"
    assert MarkdownChunker().chunk_overlap == 0


def test_chunking_is_deterministic() -> None:
    md = (
        "# H1\n\n```bash\n# comment\n```\n\nTitle\n=====\n\n"
        "## H2\n\nтекст " * 20
    )
    first = _chunk(md, chunk_size=120)
    second = _chunk(md, chunk_size=120)
    assert [(c.chunk_id, c.content_hash) for c in first] == [
        (c.chunk_id, c.content_hash) for c in second
    ]
    assert [c.text for c in first] == [c.text for c in second]


# ---------------------------------------------------------------------------
# P1: дословность и отсутствие перекрытия
# ---------------------------------------------------------------------------


def _words_text(n: int = 400) -> str:
    """Длинный текст без переводов строк: единственная граница — пробел."""
    return " ".join(f"слово{index}" for index in range(n))


def _assert_exact_join(text: str, chunks) -> None:
    """Куски покрывают нормализованный вход байт-в-байт (P1)."""
    from core.adapters.md_ast import parse_markdown

    assert "".join(chunk.text for chunk in chunks) == parse_markdown(text).text


def test_chunks_join_into_the_whole_document() -> None:
    """Конкатенация текстов всех чанков равна входу — ни символ не потерян (P1)."""
    md = (
        "# Раздел\n\nАбзац с `кодом` и [ссылкой](http://example.com).\n\n"
        "| a | b |\n|---|---|\n| 1 | 2 |\n\n"
        "```sql\nSELECT 1;\n```\n\n- пункт\n- второй\n\n"
        "> цитата\n\n---\n\n<ta"
        "ble><tr><td>x</td></tr></table>\n\n[ref]: http://example.com\n"
    )
    _assert_exact_join(md, MarkdownChunker(chunk_size=80).chunk("doc", md))


def test_adjacent_chunks_do_not_overlap() -> None:
    """Перекрытия нет: соседние чанки не делят текст (P1 против ``chunk_overlap``)."""
    text = _words_text()
    chunks = MarkdownChunker(chunk_size=120).chunk("doc", text)

    assert len(chunks) > 10
    for previous, current in itertools.pairwise(chunks):
        assert not previous.text.endswith(current.text[:1])


def test_split_does_not_break_words() -> None:
    """Куски режутся по пробелам: ни один кусок не содержит обрывка слова (C4)."""
    text = _words_text()
    words = set(text.split())
    chunks = MarkdownChunker(chunk_size=120).chunk("doc", text)

    assert len(chunks) > 10
    for chunk in chunks:
        assert chunk.text.split()
        assert set(chunk.text.split()) <= words, chunk.text


def test_cut_prefers_line_boundary() -> None:
    """Строки без пробелов: границы кусков — на переводах строк."""
    lines = [f"строка{index:03d}" for index in range(200)]
    text = "\n".join(lines)
    chunks = MarkdownChunker(chunk_size=50).chunk("doc", text)

    assert len(chunks) > 10
    for chunk in chunks:
        body = chunk.text.strip("\n")
        assert set(body.split("\n")) <= set(lines), chunk.text


def test_split_is_deterministic_for_long_text() -> None:
    """Границы кусков не вносят недетерминизма (R7)."""
    first = MarkdownChunker(chunk_size=120).chunk("doc", _words_text())
    second = MarkdownChunker(chunk_size=120).chunk("doc", _words_text())
    assert [(c.chunk_id, c.content_hash) for c in first] == [
        (c.chunk_id, c.content_hash) for c in second
    ]


def test_cjk_paragraph_is_cut_at_sentence_end_not_mid_phrase() -> None:
    """Иероглифы без пробелов: рез — по ``。``, а не по номеру символа (китайский)."""
    sentence = "数据库配置管理工具提供了备份功能。"
    text = "# 配置\n\n" + sentence * 40
    chunks = MarkdownChunker(chunk_size=200).chunk("doc", text)

    assert len(chunks) > 3
    for chunk in chunks[1:]:
        assert chunk.text.startswith("数据库") or chunk.text.startswith("配置"), chunk.text
    for chunk in chunks[:-1]:
        assert chunk.text.rstrip().endswith("。"), chunk.text


# ---------------------------------------------------------------------------
# атомарность конструкций (P2/P9/P10)
# ---------------------------------------------------------------------------


def test_table_continuation_carries_header_and_alignment() -> None:
    """Чанк, начавшийся внутри таблицы, несёт шапку с разделителем (P9)."""
    rows = "".join(f"| {index} | значение-{index} плюс текст |\n" for index in range(60))
    md = "# Таблица\n\n| id | смысл |\n|:--|--:|\n" + rows
    chunks = MarkdownChunker(chunk_size=200).chunk("doc", md)

    assert len(chunks) > 3
    for chunk in chunks[1:]:
        assert "| id | смысл |" in chunk.context_prefix
        assert "|:--|--:|" in chunk.context_prefix
        assert chunk.heading_path == "Таблица"
        assert chunk.text.startswith("| ")


def test_fence_continuation_carries_info_string() -> None:
    """Продолжение fence несёт маркер и info string (P10)."""
    md = "# Код\n\n```sql\n" + "SELECT 1;\n" * 60 + "```\n"
    chunks = MarkdownChunker(chunk_size=100).chunk("doc", md)

    assert len(chunks) > 3
    for chunk in chunks[1:]:
        assert chunk.context_prefix == "Код\n```sql"


def test_list_item_is_not_split_when_it_fits() -> None:
    """Элемент списка — минимальная единица: рез только между элементами (P2)."""
    items = "".join(f"- пункт {index} с текстом и хвостом\n" for index in range(40))
    chunks = MarkdownChunker(chunk_size=150).chunk("doc", "# Список\n\n" + items)

    assert len(chunks) > 2
    for chunk in chunks:
        for line in chunk.text.splitlines():
            assert line.startswith(("# Список", "- ", "\n")) or not line, chunk.text


def test_heading_is_not_orphaned_from_its_first_paragraph() -> None:
    """Заголовок не отрывается от первой фразы: кусок короче половины бюджета — потеря."""
    md = "# Заголовок\n\n" + "слово " * 300
    chunks = MarkdownChunker(chunk_size=200).chunk("doc", md)

    assert chunks[0].text.startswith("# Заголовок\n\nслово")


def test_blockquote_continuation_carries_marker() -> None:
    """Продолжение цитаты несёт ``> ``: одиночный абзац не читается как обычный текст."""
    md = "# Цитата\n\n> начало\n>\n" + "> строка текста\n" * 60
    chunks = MarkdownChunker(chunk_size=150).chunk("doc", md)

    assert len(chunks) > 2
    assert all(chunk.context_prefix == "Цитата\n> " for chunk in chunks[1:])


# ---------------------------------------------------------------------------
# порог фрагмента (C6)
# ---------------------------------------------------------------------------


def test_min_chunk_size_default_is_declared() -> None:
    """Порог объявлен константой и действует по умолчанию (C6)."""
    assert MIN_CHUNK_SIZE == 20
    assert MarkdownChunker().min_chunk_size == MIN_CHUNK_SIZE


def test_short_tail_is_absorbed_into_previous_chunk() -> None:
    """Хвост короче порога приклеивается к предыдущему куску той же секции (P1/C6).

    В индекс он не попадает отдельным ``source_id`` цитаты, но и не выбрасывается:
    иначе конкатенация перестала бы равняться входу.
    """
    rows = "".join(f"| {index} | значение-{index} |\n" for index in range(30))
    md = "| id | смысл |\n|---|---|\n" + rows
    chunks = MarkdownChunker(chunk_size=200).chunk("doc", md)

    assert all(len(chunk.text.strip()) >= MIN_CHUNK_SIZE for chunk in chunks)
    _assert_exact_join(md, chunks)


def test_min_chunk_size_must_fit_chunk_size() -> None:
    with pytest.raises(ValueError):
        MarkdownChunker(chunk_size=40, min_chunk_size=40)
    with pytest.raises(ValueError):
        MarkdownChunker(min_chunk_size=-1)


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
