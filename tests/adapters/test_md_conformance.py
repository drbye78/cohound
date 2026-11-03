"""Пины conformance-харнесса markdown (``scripts/conformance_md.py``).

Сеть в тестах не используется: наборы подменяются inline-фикстурами, а отсутствие кэша
проверяется через ``--offline``. Пины фиксируют три вещи — разбор форматов наборов,
классификатор ожидаемого HTML в блочные виды и честный отказ (код 2) вместо «0 примеров»,
когда данных нет.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from core.adapters.md_ast import parse_markdown

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "conformance_md.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_conformance_md", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CONFORMANCE = _load_harness()


# -- разбор наборов ----------------------------------------------------------


def test_load_spec_json_reads_official_layout(tmp_path: Path) -> None:
    path = tmp_path / "spec.json"
    path.write_text(
        '[{"markdown": "# h\\n", "html": "<h1>h</h1>\\n", "example": 7, '
        '"start_line": 1, "end_line": 3, "section": "ATX headings"}]',
        encoding="utf-8",
    )
    examples = CONFORMANCE.load_spec_json(path)  # type: ignore[attr-defined]
    assert len(examples) == 1
    assert examples[0].example == 7
    assert examples[0].markdown == "# h\n"
    assert examples[0].html == "<h1>h</h1>\n"
    assert examples[0].section == "ATX headings"


def test_load_spec_json_accepts_examples_object(tmp_path: Path) -> None:
    path = tmp_path / "spec.json"
    path.write_text(
        '{"examples": [{"markdown": "x\\n", "html": "<p>x</p>\\n"}]}',
        encoding="utf-8",
    )
    examples = CONFORMANCE.load_spec_json(path)  # type: ignore[attr-defined]
    assert len(examples) == 1
    assert examples[0].example == 1


def test_load_spec_json_rejects_items_without_markdown(tmp_path: Path) -> None:
    path = tmp_path / "broken.json"
    path.write_text('[{"html": "<p>x</p>\\n"}]', encoding="utf-8")
    with pytest.raises(ValueError):
        CONFORMANCE.load_spec_json(path)  # type: ignore[attr-defined]


def test_parse_spec_txt_reads_cmark_format_and_arrow_tabs() -> None:
    fence = "`" * 32
    text = (
        "# Section A\n"
        "\n"
        f"{fence} example\n"
        "→foo\n"
        ".\n"
        "<pre><code>foo\n</code></pre>\n"
        f"{fence}\n"
        "\n"
        f"{fence} example disabled\n"
        "x\n"
        ".\n"
        "<p>x</p>\n"
        f"{fence}\n"
        "\n"
        "## Sub\n"
        f"{fence} example table\n"
        "| a |\n"
        "| - |\n"
        ".\n"
        "<table></table>\n"
        f"{fence}\n"
    )
    examples = CONFORMANCE.parse_spec_txt(text)  # type: ignore[attr-defined]
    assert len(examples) == 2  # пример с `disabled` не возвращается
    assert examples[0].markdown == "\tfoo\n"
    assert examples[0].html == "<pre><code>foo\n</code></pre>\n"
    assert examples[0].section == "Section A"
    assert examples[1].example == 3  # отключённый пример тоже занимает номер
    assert examples[1].section == "Sub"
    assert examples[1].extensions == ("table",)


def test_load_gfm_examples_counts_disabled(tmp_path: Path) -> None:
    fence = "`" * 32
    path = tmp_path / "spec.txt"
    path.write_text(
        f"{fence} example\nx\n.\n<p>x</p>\n{fence}\n\n"
        f"{fence} example disabled\nx\n.\n<p>x</p>\n{fence}\n",
        encoding="utf-8",
    )
    examples, disabled = CONFORMANCE.load_gfm_examples(path)  # type: ignore[attr-defined]
    assert len(examples) == 1
    assert disabled == 1


# -- классификатор HTML → блочные виды --------------------------------------


def test_classify_generated_structure() -> None:
    html = (
        "<h1>t</h1>\n"
        "<p>p</p>\n"
        "<blockquote>\n<p>q</p>\n</blockquote>\n"
        "<ul>\n<li>x</li>\n</ul>\n"
        "<table>\n<thead><tr><th>a</th></tr></thead>"
        "<tbody><tr><td>1</td></tr></tbody>\n</table>\n"
        '<pre><code class="language-sql">x\n</code></pre>\n'
        "<hr />\n"
    )
    kinds, reasons = CONFORMANCE.classify_html_blocks(html)  # type: ignore[attr-defined]
    assert reasons == ()
    assert kinds == (
        "heading",
        "paragraph",
        "blockquote",
        "list",
        "table",
        "fence",
        "thematic_break",
    )


def test_classify_raw_html_is_opaque_single_block() -> None:
    kinds, reasons = CONFORMANCE.classify_html_blocks("<div>text</div>\n")  # type: ignore[attr-defined]
    assert (kinds, reasons) == (("html",), ())

    # Сырая таблица без thead/th — не GFM-таблица, а raw HTML-блок.
    kinds, reasons = CONFORMANCE.classify_html_blocks(  # type: ignore[attr-defined]
        "<table><tr><td>x</td></tr></table>\n"
    )
    assert (kinds, reasons) == (("html",), ())


def test_classify_excludes_raw_container_with_markdown_inside() -> None:
    kinds, reasons = CONFORMANCE.classify_html_blocks(  # type: ignore[attr-defined]
        "<div>\n<p>x</p>\n</div>\n"
    )
    assert kinds == ()
    assert reasons == ("html_raw_container",)


def test_classify_excludes_top_level_text() -> None:
    kinds, reasons = CONFORMANCE.classify_html_blocks("&lt;xmp&gt;\nfoo\n")  # type: ignore[attr-defined]
    assert kinds == ()
    assert reasons == ("html_top_level_text",)


def test_classify_reports_unparsed_html_instead_of_empty_silence() -> None:
    kinds, reasons = CONFORMANCE.classify_html_blocks('<div id="x"\nfoo\n')  # type: ignore[attr-defined]
    assert kinds == ()
    assert reasons == ("html_unparsed",)


def test_classify_empty_html_is_comparable_and_empty() -> None:
    assert CONFORMANCE.classify_html_blocks("") == ((), ())  # type: ignore[attr-defined]


# -- виды блоков разбора -----------------------------------------------------


def test_document_block_kinds_maps_code_and_drops_references() -> None:
    doc = parse_markdown("# h\n\npara\n\n[x]: http://y\n", "commonmark")
    kinds, reasons = CONFORMANCE.document_block_kinds(doc)  # type: ignore[attr-defined]
    assert kinds == ("heading", "paragraph")
    assert reasons == ()

    doc = parse_markdown("    code\n", "commonmark")
    kinds, reasons = CONFORMANCE.document_block_kinds(doc)  # type: ignore[attr-defined]
    assert (kinds, reasons) == (("fence",), ())


# -- прогон и свойства P13/P1 ------------------------------------------------


def test_run_corpus_matches_simple_structure() -> None:
    example = CONFORMANCE.SpecExample(  # type: ignore[attr-defined]
        example=1, markdown="# h\n\npara\n", html="<h1>h</h1>\n<p>para</p>\n"
    )
    report = CONFORMANCE.run_corpus(  # type: ignore[attr-defined]
        "inline", "commonmark", "inline", [example]
    )
    outcome = report.outcomes[0]
    assert outcome.status == "match"
    assert outcome.p13_ok is True
    assert outcome.p1_ok is True


def test_multiline_reference_definition_is_covered() -> None:
    """``[foo]:`` с URL на следующей строке — определение, а не разрыв покрытия.

    Пин переставлен по факту: харнесс нашёл дефект ``_REFERENCE_RE`` (URL только в той же
    строке), дефект исправлен сканером по §4.7 CommonMark, и теперь проверяется
    **исправленное** поведение, а не наблюдение о поломке. Иначе тест охранял бы баг.
    """
    example = CONFORMANCE.SpecExample(  # type: ignore[attr-defined]
        example=1,
        markdown="[foo]:\n/url\n\n[foo]\n",
        html='<p><a href="/url">foo</a></p>\n',
    )
    report = CONFORMANCE.run_corpus(  # type: ignore[attr-defined]
        "inline", "commonmark", "inline", [example]
    )
    outcome = report.outcomes[0]
    assert outcome.parsed is True
    assert outcome.p13_ok is True
    assert outcome.p1_ok is True


# -- честный отказ без данных ------------------------------------------------


def test_missing_cache_returns_two_with_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("COHOUND_CONFORMANCE_DIR", str(tmp_path))
    code = CONFORMANCE.main(["--offline"])  # type: ignore[attr-defined]
    captured = capsys.readouterr()
    assert code == 2
    assert "данных нет" in captured.err
    assert "нет кэша" in captured.err
    assert "0 примеров" not in captured.out


def test_explicit_missing_spec_returns_two(tmp_path: Path) -> None:
    missing = tmp_path / "absent.json"
    code = CONFORMANCE.main(["--spec", str(missing), "--offline"])  # type: ignore[attr-defined]
    assert code == 2