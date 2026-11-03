"""Пины дифференциального оракула Pandoc (``scripts/conformance_pandoc.py``).

Тесты делятся на две части. **Чистые** не запускают ``pandoc``: AST оракула задаётся
литералом, а вторая сторона — настоящий ``core.adapters.md_ast.parse_markdown``; так
проверяется объявленная нормализация классов, а не поведение бинаря. **Интеграционные**
помечены ``skipif`` и требуют установленного ``pandoc`` (в рантайме проекта его нет, в CI
он — оракул). Отдельный тест фиксирует видимый пропуск: без ``pandoc`` инструмент обязан
вернуть 2 и сказать почему, а не «0 расхождений».
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "conformance_pandoc.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_conformance_pandoc", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CONFORMANCE = _load_harness()


def _pandoc_doc(blocks: list[dict[str, Any]], meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Минимальный JSON-документ Pandoc: канонизации достаточно поля ``t``."""
    return {"pandoc-api-version": [1, 22, 2, 1], "meta": meta or {}, "blocks": blocks}


def _compare(text: str, blocks: list[dict[str, Any]], meta: dict[str, Any] | None = None) -> Any:
    return CONFORMANCE.compare_document(  # type: ignore[attr-defined]
        text, _pandoc_doc(blocks, meta), label="test", path="test.md"
    )


# -- совпадение блочных видов -------------------------------------------------


def test_commonmark_shapes_match_at_block_level() -> None:
    text = "# H\n\npara\n\n- a\n- b\n\n```py\nx=1\n```\n"
    blocks = [
        {"t": "Header", "c": [1, ["h", [], []], []]},
        {"t": "Para", "c": []},
        {"t": "BulletList", "c": []},
        {"t": "CodeBlock", "c": [["", ["py"], []], "x=1"]},
    ]
    outcome = _compare(text, blocks)
    assert outcome.diffs == []


def test_pipe_table_matches() -> None:
    text = "| a | b |\n|---|---|\n| 1 | 2 |\n"
    outcome = _compare(text, [{"t": "Table", "c": []}])
    assert outcome.diffs == []


def test_definition_list_matches() -> None:
    text = "Term\n\n:   Definition\n"
    outcome = _compare(text, [{"t": "DefinitionList", "c": []}])
    assert outcome.diffs == []


# -- нормализации, объявленные в докстринге ----------------------------------


def test_display_math_para_normalises_to_math_block() -> None:
    text = "$$\nE = mc^2\n$$\n"
    blocks = [{"t": "Para", "c": [{"t": "Math", "c": [{"t": "DisplayMath"}, "E = mc^2"]}]}]
    outcome = _compare(text, blocks)
    assert outcome.diffs == []


def test_yaml_metadata_is_not_compared() -> None:
    text = "---\ntitle: T\n---\n\nBody.\n"
    outcome = _compare(text, [{"t": "Para", "c": []}], meta={"title": {"t": "MetaString", "c": "T"}})
    assert outcome.diffs == []
    assert outcome.not_compared["metadata"] == 1


def test_footnote_definition_is_inline_in_pandoc() -> None:
    text = "Text.[^1]\n\n[^1]: Note body.\n"
    outcome = _compare(text, [{"t": "Para", "c": []}])
    assert outcome.diffs == []
    assert outcome.not_compared["footnote_inline"] == 1


# -- расхождения: конструкция названа, а не «совпала» ------------------------


def test_grid_table_is_unparsed_construct() -> None:
    text = (
        "+---+---+\n"
        "| a | b |\n"
        "+===+===+\n"
        "| 1 | 2 |\n"
        "+---+---+\n"
    )
    outcome = _compare(text, [{"t": "Table", "c": []}])
    assert [(diff.cls, diff.construct) for diff in outcome.diffs] == [
        ("unparsed_construct", "grid_table")
    ]


def test_fancy_list_is_unparsed_construct() -> None:
    text = "a)  First\nb)  Second\n"
    outcome = _compare(text, [{"t": "OrderedList", "c": []}])
    assert [(diff.cls, diff.construct) for diff in outcome.diffs] == [
        ("unparsed_construct", "fancy_list")
    ]


def test_list_without_blank_line_is_named() -> None:
    # Pandoc по умолчанию требует пустую строку перед списком, markdown-it — нет.
    text = "intro text:\n- a\n- b\n"
    outcome = _compare(text, [{"t": "Para", "c": []}])
    assert [(diff.cls, diff.construct) for diff in outcome.diffs] == [
        ("sequence_length", "mdast_only:list_without_blank_line")
    ]


def test_unparsed_construct_never_counts_as_match() -> None:
    # Таблица-сетка даёт совпадающий канонический класс только после нормализации,
    # и всё равно обязана быть расхождением, а не «совпало».
    text = "+---+\n| a |\n+---+\n"
    outcome = _compare(text, [{"t": "Table", "c": []}])
    assert outcome.ok is False


# -- видимый пропуск и сквозной прогон ---------------------------------------


def test_missing_pandoc_returns_two(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(CONFORMANCE.shutil, "which", lambda _name: None)  # type: ignore[attr-defined]
    code = CONFORMANCE.main(["--no-generated"])  # type: ignore[attr-defined]
    captured = capsys.readouterr()
    assert code == 2
    assert "SKIP" in captured.err
    assert "pandoc" in captured.err


@pytest.mark.skipif(shutil.which("pandoc") is None, reason="pandoc — оракул только на хосте замера/в CI")
def test_end_to_end_clean_document_exits_zero(tmp_path: Path) -> None:
    path = tmp_path / "clean.md"
    path.write_text("# H\n\ntext\n\n- a\n- b\n", encoding="utf-8")
    code = CONFORMANCE.main([str(path)])  # type: ignore[attr-defined]
    assert code == 0


@pytest.mark.skipif(shutil.which("pandoc") is None, reason="pandoc — оракул только на хосте замера/в CI")
def test_end_to_end_grid_table_exits_one(tmp_path: Path) -> None:
    path = tmp_path / "grid.md"
    path.write_text("+---+\n| a |\n+---+\n", encoding="utf-8")
    code = CONFORMANCE.main([str(path)])  # type: ignore[attr-defined]
    assert code == 1