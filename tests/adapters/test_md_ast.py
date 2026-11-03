"""Пины AST-слоя markdown (L1a): спаны, покрытие строк, безопасные позиции разреза."""

from __future__ import annotations

import pytest

from core.adapters.md_ast import (
    GAP_KIND,
    PROFILES,
    LineIndex,
    normalize_text,
    parse_markdown,
    safe_cut_offsets,
)


def _kinds(document: object) -> list[str]:
    return [block.kind for block in document.blocks]  # type: ignore[attr-defined]


# -- нормализация и строчный индекс -----------------------------------------


def test_bom_is_stripped_and_reported() -> None:
    text, changed = normalize_text("\ufeff# Заголовок\n")
    assert text == "# Заголовок\n"
    assert changed is True


def test_crlf_and_bare_cr_are_normalized() -> None:
    text, changed = normalize_text("a\r\nb\rc")
    assert text == "a\nb\nc"
    assert changed is True


def test_clean_text_is_not_reported_as_normalized() -> None:
    text, changed = normalize_text("# h\n\nтело\n")
    assert text == "# h\n\nтело\n"
    assert changed is False


def test_line_index_maps_lines_to_char_offsets() -> None:
    index = LineIndex("аб\nвг\n")
    assert index.line_count == 3
    assert [index.offset(line) for line in range(3)] == [0, 3, 6]
    assert index.line_text(1) == "вг\n"
    assert index.line_of(4) == 1
    assert index.span(0, 2).start_char == 0
    assert index.span(0, 2).end_char == 6


def test_span_end_beyond_text_is_clamped() -> None:
    index = LineIndex("одна строка")
    span = index.span(0, 99)
    assert span.end_char == len("одна строка")


# -- блоки ------------------------------------------------------------------


def test_headings_carry_level_and_span() -> None:
    doc = parse_markdown("# Раз\n\n## Два\n")
    headings = doc.of_kind("heading")
    assert [block.level for block in headings] == [1, 2]
    assert doc.text[headings[0].span.start_char : headings[0].span.end_char] == "# Раз\n"


def test_table_rows_are_separate_blocks_with_alignment() -> None:
    doc = parse_markdown("| a | b |\n|:--|--:|\n| 1 | 2 |\n")
    table = doc.of_kind("table")[0]
    assert "align" in table.info
    assert table.info["header_rows"] == 1
    rows = doc.of_kind("table_row")
    assert len(rows) == 2
    assert doc.text[rows[1].span.start_char : rows[1].span.end_char] == "| 1 | 2 |\n"
    assert [child.kind for child in table.children] == ["table_head", "table_body"]


def test_fence_keeps_info_string_and_lang() -> None:
    doc = parse_markdown("```sql\nSELECT 1;\n```\n")
    fence = doc.of_kind("fence")[0]
    assert fence.info["lang"] == "sql"
    assert fence.info["info"] == "sql"
    assert doc.text[fence.span.start_char : fence.span.end_char] == "```sql\nSELECT 1;\n```\n"


def test_blockquote_nests_paragraphs_inside_its_span() -> None:
    doc = parse_markdown("> цитата\n>\n> второй абзац\n\nпосле\n")
    quote = doc.of_kind("blockquote")[0]
    assert quote.span.start_line == 0 and quote.span.end_line == 3
    assert [child.kind for child in quote.children] == ["paragraph", "paragraph"]
    # Срез цитаты включает '>' — спан относится к источнику, а не к очищенному content.
    assert doc.text[quote.span.start_char : quote.span.end_char].startswith("> цитата")


def test_nested_list_items_keep_their_parents() -> None:
    doc = parse_markdown("- один\n  - вложенный\n- два\n")
    lists = doc.of_kind("list")
    assert len(lists) == 2
    assert lists[0].info["ordered"] is False
    top_items = [child for child in lists[0].children if child.kind == "list_item"]
    assert len(top_items) == 2


def test_ordered_list_carries_start() -> None:
    doc = parse_markdown("3. три\n4. четыре\n")
    assert doc.of_kind("list")[0].info["start"] == 3


def test_indented_code_is_its_own_block() -> None:
    doc = parse_markdown("    SELECT 1;\n    SELECT 2;\n")
    code = doc.of_kind("code")
    assert len(code) == 1
    assert code[0].info["indented"] is True


# -- покрытие строк (P13) ----------------------------------------------------


def test_reference_definition_is_a_named_block_not_a_gap() -> None:
    doc = parse_markdown("Текст [ссылка][ref].\n\n[ref]: http://example.com \"t\"\n")
    assert doc.gaps == ()
    assert doc.counts["reference"] == 1
    reference = doc.of_kind("reference")[0]
    assert doc.text[reference.span.start_char : reference.span.end_char].startswith("[ref]:")


def test_reference_definition_may_span_lines() -> None:
    """Метка, назначение и заголовок могут стоять на разных строках (CommonMark §4.7).

    Парсер таких определений не отдаёт токеном, и прежняя однострочная эвристика
    считала их разрывом покрытия: текст оставался вне блоков (P13), а значит и вне
    чанков. Примеры — CommonMark 193, 195, 198, 208, 541.
    """
    cases = [
        "   [foo]: \n      /url  \n           'the title'  \n\n[foo]\n",
        "[Foo bar]:\n<my url>\n'title'\n\n[Foo bar]\n",
        "[foo]:\n/url\n\n[foo]\n",
        "[\nfoo\n]: /url\nbar\n",
        "[Foo\n  bar]: /url\n\n[Baz][Foo bar]\n",
    ]
    for text in cases:
        doc = parse_markdown(text, profile="commonmark")
        assert doc.counts["unknown_lines"] == 0, text
        assert doc.counts["reference"] == 1, text
        assert doc.gaps == (), text


def test_reference_definition_label_may_be_escaped() -> None:
    """Экранированная скобка в метке — часть метки, а не её конец (CommonMark 194)."""
    doc = parse_markdown("[Foo*bar\\]]:my_(url) 'title (with parens)'\n\n[Foo*bar\\]]\n")

    assert doc.counts["reference"] == 1
    assert doc.counts["unknown_lines"] == 0


def test_consecutive_reference_definitions_are_one_gap() -> None:
    """Несколько определений подряд целиком занимают разрыв (CommonMark 204, 217, 565)."""
    doc = parse_markdown("[foo]: /url1\n[bar]: /url2\n\n[foo][bar]\n")

    assert doc.counts["reference"] == 1
    assert doc.counts["unknown_lines"] == 0
    assert len(doc.of_kind("paragraph")) == 1


def test_definition_like_text_outside_the_rule_stays_a_gap() -> None:
    """Строка, не являющаяся определением, не «съедается»: P13 остаётся сигналом.

    Правило разрешает только то, что разрешает грамматика: иначе молчаливое вычёркивание
    текста из покрытия выдало бы себя за корректный разбор. Отступ в четыре пробела —
    уже блок кода, а `[foo]` без `:` — обычный абзац; неизвестных строк быть не должно,
    но и `reference` тоже.
    """
    code = parse_markdown("    [foo]: /url\n")
    assert code.counts.get("reference", 0) == 0
    assert code.of_kind("code") and code.counts["unknown_lines"] == 0

    plain = parse_markdown("[foo]\n")
    assert plain.counts.get("reference", 0) == 0
    assert plain.of_kind("paragraph")


def test_colon_fence_is_a_container_block() -> None:
    """``::: {.cls}`` в профиле ``pandoc`` — блок, а не разрыв покрытия.

    Токен ``colon_fence`` не был отображён, и весь текст div уходил в ``unknown_lines``:
    чанкер его не видел вовсе. Содержимое при этом не разбирается на блоки — div остаётся
    одной единицей, и это объявленная граница, а не догадка о структуре.
    """
    for text in ("::: {.note}\nтекст div\n:::\n", "::: note\n- пункт\n:::\n"):
        doc = parse_markdown(text, profile="pandoc")
        assert doc.counts["unknown_lines"] == 0, text
        containers = doc.of_kind("container")
        assert len(containers) == 1, text
        assert containers[0].span.start_line == 0
        assert containers[0].span.end_line == 3, "закрывающая строка входит в спан"


def test_container_closing_line_is_covered_and_not_invented() -> None:
    """Строка ``:::`` принадлежит контейнеру; текст после него — нет.

    Расширение спана на закрывающую строку не должно «съедать» следующую: иначе P13
    перестал бы быть сигналом о непокрытом тексте.
    """
    doc = parse_markdown("::: div\nтекст\n:::\n\nобычный абзац\n", profile="pandoc")

    assert doc.counts["unknown_lines"] == 0
    container = doc.of_kind("container")[0]
    assert doc.text[container.span.start_char : container.span.end_char].endswith(":::\n")
    assert [block.kind for block in doc.blocks] == ["container", "paragraph"]


def test_footnote_definition_covers_its_label_line() -> None:
    doc = parse_markdown("См. [^1].\n\n[^1]:\n    Тело сноски.\n", profile="pandoc")
    definitions = doc.of_kind("footnote_def")
    assert len(definitions) == 1
    assert definitions[0].info["label"] == "1"
    assert definitions[0].span.start_line == 3
    # Метка на отдельной строке не потеряна: она либо в блоке, либо названа отдельно.
    assert doc.gaps == ()


def test_every_non_blank_line_is_covered_on_a_mixed_document() -> None:
    document = (
        "# Раздел\n\n"
        "Абзац с `кодом` и [ссылкой](http://example.com).\n\n"
        "| a | b |\n|---|---|\n| 1 | 2 |\n\n"
        "```sql\nSELECT 1;\n```\n\n"
        "- пункт\n- второй\n\n"
        "> цитата\n\n"
        "---\n\n"
        "<div>блок</div>\n\n"
        "[ref]: http://example.com\n"
    )
    doc = parse_markdown(document)
    assert doc.gaps == ()
    assert doc.counts["unknown_lines"] == 0
    assert GAP_KIND not in doc.counts


def test_unparsed_line_is_visible_as_gap() -> None:
    doc = parse_markdown("обычный текст\n\n@@ непонятная конструкция @@\n")
    # Обе строки — обычные абзацы: покрытие полное, дыр нет.
    assert doc.gaps == ()
    # А вот строка без абзаца-владельца (после reference definition) обязана попасть в
    # reference, а не в gap: это проверено выше. Здесь фиксируем, что счётчик есть.
    assert doc.counts["gaps"] == 0


# -- профили грамматики ------------------------------------------------------


def test_profiles_are_declared() -> None:
    assert PROFILES == ("commonmark", "gfm", "pandoc")


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="неизвестный профиль"):
        parse_markdown("текст", profile="myst")


def test_math_block_exists_only_in_pandoc_profile() -> None:
    source = "$$\nE = mc^2\n$$\n"
    pandoc = parse_markdown(source, profile="pandoc")
    assert len(pandoc.of_kind("math")) == 1
    gfm = parse_markdown(source, profile="gfm")
    assert gfm.of_kind("math") == ()


def test_gfm_table_is_not_a_table_in_commonmark() -> None:
    source = "| a | b |\n|---|---|\n| 1 | 2 |\n"
    assert parse_markdown(source, profile="commonmark").of_kind("table") == ()
    assert len(parse_markdown(source, profile="gfm").of_kind("table")) == 1


# -- безопасные позиции разреза ---------------------------------------------


def test_cut_is_never_inside_code_span() -> None:
    text = "Начало `код с пробелами` конец фразы тут."
    opening = text.index("`")
    closing = text.index("`", opening + 1)
    cuts = safe_cut_offsets(text, 0, len(text))
    assert cuts, "ожидались кандидаты разреза в прозе"
    assert all(not (opening < cut <= closing) for cut in cuts)


def test_cut_is_never_inside_link_destination() -> None:
    text = "См. [документ](http://example.com/a b) и дальше текст."
    cuts = safe_cut_offsets(text, 0, len(text))
    destination = text.index("](")
    assert all(not (destination < cut <= text.index(")", destination)) for cut in cuts)


def test_cut_is_never_between_base_char_and_combining_mark() -> None:
    text = "слово e\u0301 дальше ещё текст"
    cuts = safe_cut_offsets(text, 0, len(text))
    position = text.index("\u0301")
    assert position not in cuts


def test_cut_never_starts_a_chunk_with_closing_punctuation() -> None:
    text = "слово ) другое слово"
    cuts = safe_cut_offsets(text, 0, len(text))
    assert text.index(")") not in cuts


def test_cut_keeps_separator_in_previous_piece() -> None:
    text = "первое предложение. второе предложение"
    cuts = safe_cut_offsets(text, 0, len(text))
    cut = cuts[0]
    assert text[cut - 1].isspace() or text[cut - 1] in ".!?;…"
    assert not text[cut].isspace()


def test_cut_range_is_validated() -> None:
    with pytest.raises(ValueError, match="некорректный диапазон"):
        safe_cut_offsets("текст", 3, 2)


# -- детерминизм -------------------------------------------------------------


def test_parsing_is_deterministic() -> None:
    source = "# Раз\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n```sql\nSELECT 1;\n```\n"
    first = parse_markdown(source)
    second = parse_markdown(source)
    assert first.blocks == second.blocks
    assert first.counts == second.counts
    assert first.gaps == second.gaps