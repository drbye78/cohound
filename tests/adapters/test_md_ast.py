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


# -- профиль pandoc: правила блоков (L3, первая половина) --------------------


def _layout(document: object) -> list[tuple[str, int, int]]:
    """(вид, начало, конец) блоков верхнего уровня — компактный пин структуры."""
    return [
        (block.kind, block.span.start_line, block.span.end_line)
        for block in document.blocks  # type: ignore[attr-defined]
    ]


def test_pandoc_list_does_not_interrupt_a_paragraph() -> None:
    """В Pandoc список сразу после строки абзаца — продолжение абзаца (§L3).

    Измерено оракулом (``pandoc 2.17.1.1``): ``текст\\n- a\\n- b`` — один ``Para``,
    потому что ``lists_without_preceding_blankline`` выключено. CommonMark/GFM начинают
    список; профильная развилка и есть предмет теста.
    """
    source = "вступление:\n- пункт\n- второй\n"

    assert _layout(parse_markdown(source, profile="pandoc")) == [("paragraph", 0, 3)]
    for profile in ("commonmark", "gfm"):
        assert _layout(parse_markdown(source, profile=profile)) == [
            ("paragraph", 0, 1),
            ("list", 1, 3),
        ], profile


def test_pandoc_blockquote_does_not_interrupt_a_paragraph() -> None:
    """Цитата без пустой строки перед ней — тоже продолжение абзаца.

    ``blank_before_blockquote`` в Pandoc markdown включено: ``text\\n> q`` — один
    ``Para`` (измерено), а не абзац и цитата.
    """
    source = "текст\n> цитата\n"

    assert _layout(parse_markdown(source, profile="pandoc")) == [("paragraph", 0, 2)]
    for profile in ("commonmark", "gfm"):
        assert _layout(parse_markdown(source, profile=profile)) == [
            ("paragraph", 0, 1),
            ("blockquote", 1, 2),
        ], profile


def test_pandoc_paragraph_keeps_absorbing_until_blank_line() -> None:
    """Абзац тянется до пустой строки: заголовок и разрыв его не прерывают.

    Измерено: ``text\\n- a\\n# H`` и ``text\\n- a\\n---`` — по одному ``Para``. Fenced
    code, наоборот, прерывает — он в :data:`ABSORBABLE` не входит.
    """
    plain = "текст\n- пункт\n# Заголовок\n---\n"
    assert _layout(parse_markdown(plain, profile="pandoc")) == [("paragraph", 0, 4)]

    fenced = "текст\n- пункт\n```\nкод\n```\n"
    assert _layout(parse_markdown(fenced, profile="pandoc")) == [
        ("paragraph", 0, 2),
        ("fence", 2, 5),
    ]


def test_pandoc_line_block_is_one_block_of_lines() -> None:
    """``| строка`` — line block: блок со спаном на все строки и ребёнком на каждую.

    Единица нарезки объявлена структурой (§5), а не соглашением чанкера.
    """
    source = "| Первая строка\n| Вторая строка\n|    Третья с отступом\n"
    doc = parse_markdown(source, profile="pandoc")

    blocks = doc.blocks
    assert [block.kind for block in blocks] == ["line_block"]
    block = blocks[0]
    assert (block.span.start_line, block.span.end_line) == (0, 3)
    assert [child.kind for child in block.children] == ["line", "line", "line"]
    assert [doc.text[child.span.start_char : child.span.end_char] for child in block.children] == [
        "| Первая строка\n",
        "| Вторая строка\n",
        "|    Третья с отступом\n",
    ]
    assert doc.gaps == ()
    assert doc.counts["unknown_lines"] == 0


def test_line_block_requires_a_block_start_and_a_space() -> None:
    """Не всякая строка с ``|`` — line block: измеренные границы оракула.

    ``|a`` (без пробела) — текст; отступ на верхнем уровне — текст; строка после
    абзаца без пустой строки — тоже текст (Pandoc не прерывает абзац line block'ом).
    """
    cases = ("|a\n|b\n", "  | отступ\n  | второй\n", "текст\n| строка\n")
    for source in cases:
        doc = parse_markdown(source, profile="pandoc")
        assert doc.of_kind("line_block") == (), source
        assert doc.counts["unknown_lines"] == 0, source


def test_line_block_tail_becomes_a_paragraph() -> None:
    """Блок кончается на первой строке без ``| ``; остаток — обычный абзац."""
    source = "| стих\nобычный текст\n| снова стих\n"
    doc = parse_markdown(source, profile="pandoc")

    assert [(block.kind, block.span.start_line, block.span.end_line) for block in doc.blocks] == [
        ("line_block", 0, 1),
        ("paragraph", 1, 3),
    ]
    assert doc.gaps == ()


def test_pandoc_fancy_and_example_lists_are_lists() -> None:
    """``a)``/``iv.``/``#.``/``(@)`` — списки: маркер, пункты, точные спаны.

    В CommonMark/GFM эти строки — абзац (запиновано ниже в
    ``test_commonmark_and_gfm_structure_is_unchanged``).
    """
    cases = (
        "a) первый\nb) второй\n",
        "A) первый\nB) второй\n",
        "i. первый\nii. второй\n",
        "iv. четвёртый\nv. пятый\n",
        "#. первый\n#. второй\n",
        "(@) первый\n(@label) второй\n",
    )
    for source in cases:
        doc = parse_markdown(source, profile="pandoc")
        lists = doc.of_kind("list")
        assert len(lists) == 1, source
        assert lists[0].info["ordered"] is True, source
        assert lists[0].info["dialect"] == "fancy", source
        assert [child.kind for child in lists[0].children] == ["list_item", "list_item"], source
        assert doc.gaps == (), source
        assert doc.counts["unknown_lines"] == 0, source


def test_uppercase_single_letter_with_period_is_not_a_marker() -> None:
    """Измерено: ``A. текст`` — не список (оракул отдаёт ``Para``), ``A)`` — список."""
    assert _layout(parse_markdown("A. текст\n", profile="pandoc")) == [("paragraph", 0, 1)]
    assert _layout(parse_markdown("A) текст\n", profile="pandoc")) == [("list", 0, 1)]


def test_fancy_lists_merge_across_a_blank_line_only_when_style_fits() -> None:
    """Оракул сливает ``a.``/``b.`` через пустую строку, но не ``a)``/``iv.``."""
    same = "a) первый\n\nb) второй\n"
    assert _layout(parse_markdown(same, profile="pandoc")) == [("list", 0, 3)]

    different = "a) первый\n\niv. второй\n"
    assert _layout(parse_markdown(different, profile="pandoc")) == [
        ("list", 0, 1),
        ("list", 2, 3),
    ]


def test_pandoc_grid_table_has_real_rows_and_pipe_header() -> None:
    """Grid-таблица: рамка, шапка ``+===+``, строки-дети и шапка в pipe-виде.

    Строка — объявленная единица нарезки (§5); шапка синтезируется для
    ``context_prefix``, потому что в источнике она не pipe-таблица.
    """
    source = "+---+---+\n| a | b |\n+===+===+\n| 1 | 2 |\n+---+---+\n| 3 | 4 |\n+---+---+\n"
    doc = parse_markdown(source, profile="pandoc")

    tables = doc.of_kind("table")
    assert len(tables) == 1
    table = tables[0]
    assert table.info["dialect"] == "grid"
    assert table.info["header_rows"] == 1
    assert table.info["columns"] == 2
    assert table.info["header_pipe"] == "| a | b |\n|---|---|"
    rows = [child for child in table.children if child.kind == "table_row"]
    assert [row.info["header"] for row in rows] == [True, False, False]
    assert doc.text[rows[1].span.start_char : rows[1].span.end_char] == "| 1 | 2 |\n"
    assert doc.gaps == ()
    assert doc.counts["unknown_lines"] == 0


def test_pandoc_grid_alignment_comes_from_the_header_separator() -> None:
    """Выравнивание grid-таблицы — двоеточия ``+===+``; двоеточия верхней рамки не в счёт.

    Измерено оракулом: ``+:--+--:+`` даёт ``AlignDefault``, а ``+:==+==:+`` — левое и
    правое; поэтому правило читает разделитель шапки, а не верхнюю рамку.
    """
    source = "+---+---+\n| a | b |\n+:==+==:+\n| 1 | 2 |\n+---+---+\n"
    table = parse_markdown(source, profile="pandoc").of_kind("table")[0]
    assert table.info["align"] == ["left", "right"]
    assert table.info["header_pipe"] == "| a | b |\n|:---|---:|"


def test_pandoc_multiline_and_simple_tables_are_tables_with_captions() -> None:
    """Multiline/simple: строки-дети, подпись в метаданных и в pipe-шапке."""
    multiline = (
        "-----------\n A    B\n---- ----\n 1    2\n 3    4\n-----------\n\nTable: cap\n"
    )
    document = parse_markdown(multiline, profile="pandoc")
    table = document.of_kind("table")[0]
    assert table.info["dialect"] == "multiline"
    assert table.info["header_pipe"] == "| A | B |\n|---|---|"
    assert table.info["caption"] == "cap"
    assert table.info["caption_text"] == "Table: cap"
    assert document.text[table.span.start_char : table.span.end_char].endswith("Table: cap\n")
    assert document.gaps == ()

    simple = "  A     B\n----- -----\n  1     2\n\n: cap\n"
    document = parse_markdown(simple, profile="pandoc")
    table = document.of_kind("table")[0]
    assert table.info["dialect"] == "simple"
    assert table.info["header_pipe"] == "| A | B |\n|---|---|"
    assert table.info["caption"] == "cap"
    assert table.info["caption_text"] == ": cap"
    assert document.counts["unknown_lines"] == 0


def test_gfm_and_commonmark_do_not_see_pandoc_tables() -> None:
    """Развилка профиля: в ``gfm``/``commonmark`` grid-таблица — обычный абзац."""
    source = "+---+---+\n| a | b |\n+===+===+\n| 1 | 2 |\n+---+---+\n"
    for profile in ("commonmark", "gfm"):
        doc = parse_markdown(source, profile=profile)
        assert doc.of_kind("table") == (), profile
        assert [block.kind for block in doc.blocks] == ["paragraph"], profile


def test_pandoc_pipe_table_stops_where_pandoc_stops() -> None:
    """GFM-ячейка не переносится на строку без ``|``: Pandoc кончает таблицу раньше."""
    source = "| a | b |\n|---|---|\n| 1 | 2 |\ncontinuation\n| 3 | 4 |\n\nпосле\n"
    doc = parse_markdown(source, profile="pandoc")

    assert [block.kind for block in doc.blocks] == ["table", "paragraph", "paragraph"]
    assert doc.blocks[0].span.end_line == 3
    assert doc.text[doc.blocks[1].span.start_char : doc.blocks[1].span.end_char] == (
        "continuation\n| 3 | 4 |\n"
    )
    assert doc.counts["unknown_lines"] == 0


def test_pandoc_raw_tex_block_and_its_measured_boundaries() -> None:
    """Raw TeX: окружение и команда — блок ``raw``; границы взяты из оракула.

    ``\\alpha beta`` — уже не raw (хвост той же строки), ``\\alpha\\nbeta`` — raw + абзац,
    ``\\%`` — экранированная пунктуация, математическое окружение — абзац.
    """
    document = parse_markdown(
        "\\begin{quote}\ntext\n\\end{quote}\n\nпосле\n", profile="pandoc"
    )
    blocks = document.blocks
    assert [block.kind for block in blocks] == ["raw", "paragraph"]
    raw = blocks[0]
    assert raw.info["raw_format"] == "tex"
    assert raw.info["marker"] == "\\begin{quote}"
    assert document.text[raw.span.start_char : raw.span.end_char] == (
        "\\begin{quote}\ntext\n\\end{quote}\n"
    )
    assert document.counts["unknown_lines"] == 0

    assert _layout(parse_markdown("\\alpha beta\n", profile="pandoc")) == [
        ("paragraph", 0, 1)
    ]
    assert _layout(parse_markdown("\\alpha\nbeta\n", profile="pandoc")) == [
        ("raw", 0, 1),
        ("paragraph", 1, 2),
    ]
    assert _layout(parse_markdown("\\% 50\n", profile="pandoc")) == [("paragraph", 0, 1)]
    assert _layout(parse_markdown("\\begin{align}\nx\n\\end{align}\n", profile="pandoc")) == [
        ("paragraph", 0, 3)
    ]


def test_pandoc_native_div_is_a_container_with_parsed_children() -> None:
    """Нативный ``<div>`` — контейнер; дети разобраны из внутреннего среза со сдвигом."""
    source = '<div class="x">\n\nhi\n\n</div>\n\nпосле\n'
    doc = parse_markdown(source, profile="pandoc")

    assert [block.kind for block in doc.blocks] == ["container", "paragraph"]
    container = doc.blocks[0]
    assert container.info["name"] == "div"
    assert doc.text[container.span.start_char : container.span.end_char] == (
        '<div class="x">\n\nhi\n\n</div>\n'
    )
    assert [child.kind for child in container.children] == ["paragraph"]
    child = container.children[0]
    assert doc.text[child.span.start_char : child.span.end_char] == "hi\n"
    assert child.span.start_char == source.index("hi")
    assert doc.counts["unknown_lines"] == 0


def test_pandoc_div_children_keep_markdown_structure() -> None:
    """Внутренний срез разбирается профилем: список внутри div остаётся списком."""
    doc = parse_markdown('<div class="x">\n\n- a\n- b\n\n</div>\n', profile="pandoc")

    assert doc.gaps == ()
    container = doc.of_kind("container")[0]
    assert [child.kind for child in container.children] == ["list"]
    assert doc.text[container.children[0].span.start_char : container.children[0].span.end_char].startswith(
        "- a"
    )


def test_commonmark_and_gfm_structure_is_unchanged() -> None:
    """Пин: в ``commonmark``/``gfm`` правила Pandoc не исполняются вовсе.

    Ожидания — дословно то, что отдавал разбор до L3 (абзац/список/цитата по
    CommonMark). Тест существует потому, что «мы не трогали другой профиль» — обещание,
    а структура блоков — проверяемый факт.
    """
    expected = {
        "вступление:\n- пункт\n- второй\n": [("paragraph", 0, 1), ("list", 1, 3)],
        "текст\n> цитата\n": [("paragraph", 0, 1), ("blockquote", 1, 2)],
        "a) первый\nb) второй\n": [("paragraph", 0, 2)],
        "| строка\n| вторая\n": [("paragraph", 0, 2)],
        "(@) первый\n(@label) второй\n": [("paragraph", 0, 2)],
    }
    for source, layout in expected.items():
        for profile in ("commonmark", "gfm"):
            assert _layout(parse_markdown(source, profile=profile)) == layout, (profile, source)


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