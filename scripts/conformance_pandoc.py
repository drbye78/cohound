"""Дифференциальная приёмка markdown против Pandoc как внешнего оракула.

**Что меряется.** Берётся markdown-документ, ``pandoc -f markdown -t json`` отдаёт
собственное AST Pandoc, а ``core.adapters.md_ast.parse_markdown(text, "pandoc")`` —
блочное дерево проекта. Сравнивается **последовательность видов блоков верхнего
уровня**. Это единственное, что можно проверить честно: в Pandoc 2.17.1.1 JSON не несёт
позиций источника (см. ниже), поэтому паритет по спанам этим инструментом не
устанавливается и не заявляется.

**Режим сравнения — ``types-only``.** Измерено на этой машине: ``pandoc -t json``
отдаёт блоки как ``{"t": "Header", "c": …}``; ключа ``sourcepos`` нет ни у одного блока,
``--sourcepos`` в 2.17.1.1 неизвестен (``Unknown option``). Диагностический ``--trace``
печатает по одной строке на блок верхнего уровня, но это **не спаны**: заголовок из
строки 1 репортится как ``at line 3``, список из строк 5–7 — как ``line 9`` (номер
текущей строки после поглощения блока), а само содержимое обрезано до 80 символов.
Поэтому сравнение — последовательность видов; точные границы строк им не проверяются.
Имя ридера тоже измеренный факт: формата ``pandoc`` у 2.17.1.1 нет (``Unknown input
format pandoc``), Pandoc markdown называется ``markdown``; используется он.

**Нормализация сравнения (объявлена здесь, а не «на глаз»).** Каждый блок обеих сторон
сводится к каноническому классу:

============================  ==========================  ==========================
Pandoc ``t``                  канонический класс           ``md_ast`` ``kind``
============================  ==========================  ==========================
``Header``                    ``heading``                  ``heading``
``Para`` / ``Plain``          ``paragraph``                ``paragraph``
``BulletList`` / ``OrderedList``  ``list``                 ``list``
``CodeBlock``                 ``code_block``               ``fence``, ``code``
``BlockQuote``                ``blockquote``               ``blockquote``
``Table``                     ``table``                    ``table``
``DefinitionList``            ``deflist``                  ``definition_list``
``Div``                       ``container``                ``container``
``RawBlock``                  ``raw``                      ``html``
``LineBlock``                 ``line_block``               — (не разбирается)
``HorizontalRule``            ``thematic_break``           ``thematic_break``
``Figure``                    ``figure``                   — (в 2.17 его нет в JSON)
============================  ==========================  ==========================

Дополнительно объявлены три нормализации, без которых сравнение ловило бы разницу
моделей, а не разбора:

* **Display math.** ``$$…$$`` у Pandoc — ``Para`` с единственным инлайном
  ``Math DisplayMath``, у ``md_ast`` — блок ``math``. Такой ``Para`` сводится к ``math``.
  Инлайн-``$x$`` внутри абзаца остаётся ``paragraph`` (и не проверяется — см. границы).
* **YAML-метаданные.** Pandoc забирает их в ``meta`` и в ``blocks`` не отдаёт; блок
  ``front_matter`` из ``md_ast`` снимается с сравнения и попадает в счётчик
  ``metadata`` (если ``meta`` непустой). Если Pandoc метаданных не увидел, а ``md_ast``
  увидел — это расхождение, а не «снято».
* **Подряд идущие ``RawBlock``** схлопываются в один ``raw``: Pandoc режет один
  HTML-фрагмент на несколько блоков, и без схлопывания число блоков сравнивало бы
  нарезку, а не структуру (тот же приём, что в ``scripts/conformance_md.py``).
* **Сноски.** У Pandoc определение сноски — инлайновый ``Note``, отдельного блока нет;
  ``footnote_def`` из ``md_ast`` снимается с блочного сравнения и считается как
  ``footnote_inline``. Инлайновая структура сносок этим инструментом не проверяется.

**Классы расхождений** (счётчики ведутся по документам, блокам и конструкциям):

* ``unparsed_construct`` — Pandoc распознал конструкцию, для которой у ``md_ast`` нет
  правила (grid/multiline/simple table, line block, fancy/example list, raw TeX,
  нативный HTML-``Div``, подпись таблицы). Это **не** «совпало»: конструкция названа и
  посчитана отдельно;
* ``type_mismatch`` — обе стороны дали блок, но классы разные;
* ``sequence_length`` — лишний/недостающий блок при выравнивании;
* ``oracle_error`` — Pandoc не смог разобрать документ (видимый отказ, не «ноль»).

**Источники.** (а) Детерминированная выборка **отслеживаемых git** markdown-файлов
дерева (``git ls-files``: ``docs/**/*.md`` и ``*.md`` в корне), отсортированных по пути,
затем равномерный шаг ``len(paths) // max_docs`` (не «первые N», иначе выборка была бы
одним каталогом; неотслеживаемые файлы других полос не попадают в замер);
(б) сгенерированный набор конструкций, по которым Pandoc отличается от
CommonMark/GFM, — формы взяты из руководства Pandoc (grid/multiline/simple table,
line block, fenced div, definition list, сноска, example/fancy list, citation,
bracketed span, TOML, YAML front matter, implicit figure, sub/superscript, raw TeX,
подпись таблицы, display math).

**Коды возврата.** 0 — расхождений нет; 1 — расхождения есть; 2 — Pandoc недоступен
(видимый пропуск, как в ``tests/smoke_scrapy_playwright.py``), нет документов или оракул
не отвечает. Пропуск никогда не печатается как «0 расхождений».

**Границы.** Не проверяются: инлайновая структура (акценты, цитаты, спаны, сноски,
подписи ссылок), атрибуты блоков, точные границы строк и вложенность блоков внутри
контейнеров (``Div``/``BlockQuote``/список у ``md_ast`` — один блок с детьми, у Pandoc
дерево). ``types-only`` означает именно последовательность видов верхнего уровня.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Final

_ROOT: Final = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.adapters.md_ast import MdBlock, MdDocument, parse_markdown  # noqa: E402

__all__ = [
    "DISCREPANCY_CLASSES",
    "GENERATED_CASES",
    "BlockView",
    "ClassStat",
    "Diff",
    "DocOutcome",
    "GeneratedCase",
    "canonical_mdast_blocks",
    "canonical_pandoc_blocks",
    "compare_document",
    "compare_views",
    "discover_documents",
    "main",
    "run_corpus",
]

#: Профиль разбора: дифференциальная приёмка идёт против Pandoc markdown.
PROFILE: Final = "pandoc"

#: Классы, которые считаются расхождением и дают код возврата 1.
DISCREPANCY_CLASSES: Final[tuple[str, ...]] = (
    "unparsed_construct",
    "type_mismatch",
    "sequence_length",
    "oracle_error",
)

#: Причины, по которым блок снят со сравнения (не «совпало» и не расхождение).
NOT_COMPARED_REASONS: Final[dict[str, str]] = {
    "metadata": "YAML/TOML-метаданные: Pandoc держит их в meta, не в blocks",
    "footnote_inline": "определение сноски у Pandoc — инлайновый Note",
}

#: Pandoc ``t`` → канонический класс.
_PANDOC_KINDS: Final[dict[str, str]] = {
    "Header": "heading",
    "Para": "paragraph",
    "Plain": "paragraph",
    "BulletList": "list",
    "OrderedList": "list",
    "CodeBlock": "code_block",
    "BlockQuote": "blockquote",
    "Table": "table",
    "DefinitionList": "deflist",
    "Div": "container",
    "RawBlock": "raw",
    "LineBlock": "line_block",
    "HorizontalRule": "thematic_break",
    "Figure": "figure",
}

#: ``md_ast`` kind → канонический класс (``front_matter``/``footnote_def`` — особые).
_MDAST_KINDS: Final[dict[str, str]] = {
    "heading": "heading",
    "paragraph": "paragraph",
    "list": "list",
    "fence": "code_block",
    "code": "code_block",
    "blockquote": "blockquote",
    "table": "table",
    "definition_list": "deflist",
    "container": "container",
    "html": "raw",
    "math": "math",
    "thematic_break": "thematic_break",
}


@dataclass(frozen=True, slots=True)
class BlockView:
    """Блок в каноническом виде: класс, исходный тип, строка и подсказка по источнику."""

    canonical: str
    raw: str
    line: int | None = None
    detail: str = ""
    hint: str = ""
    #: Конструкция, которую ``md_ast`` узнал по источнику, но не разобрал правилом.
    unparsed: str = ""


@dataclass(frozen=True, slots=True)
class Diff:
    """Одно расхождение: класс, конструкция, обе стороны, место."""

    cls: str
    construct: str
    pandoc_type: str
    mdast_kind: str
    line: int | None
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "class": self.cls,
            "construct": self.construct,
            "pandoc_type": self.pandoc_type,
            "mdast_kind": self.mdast_kind,
            "line": self.line,
            "detail": self.detail,
        }


@dataclass(slots=True)
class DocOutcome:
    """Результат одного документа."""

    label: str
    path: str
    pandoc_types: list[str] = field(default_factory=list)
    mdast_kinds: list[str] = field(default_factory=list)
    diffs: list[Diff] = field(default_factory=list)
    not_compared: Counter[str] = field(default_factory=Counter)

    @property
    def ok(self) -> bool:
        return not self.diffs

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "path": self.path,
            "pandoc_types": self.pandoc_types,
            "mdast_kinds": self.mdast_kinds,
            "diffs": [diff.as_dict() for diff in self.diffs],
            "not_compared": dict(sorted(self.not_compared.items())),
        }


@dataclass(slots=True)
class ClassStat:
    """Свод по классу расхождений: документы, блоки, конструкции."""

    documents: set[str] = field(default_factory=set)
    blocks: int = 0
    constructs: Counter[str] = field(default_factory=Counter)

    def add(self, document: str, construct: str) -> None:
        self.documents.add(document)
        self.blocks += 1
        self.constructs[construct] += 1


@dataclass(frozen=True, slots=True)
class GeneratedCase:
    """Сгенерированный документ: метка, конструкция и текст."""

    name: str
    construct: str
    text: str


# ---------------------------------------------------------------------------
# Сгенерированный набор: формы из руководства Pandoc
# ---------------------------------------------------------------------------

#: Набор конструкций, где Pandoc отличается от CommonMark/GFM. Тексты повторяют
#: примеры руководства Pandoc (Pandoc User's Guide, «Pandoc's Markdown»), а не
#: выдуманы: иначе замер проверял бы фантазию автора теста.
GENERATED_CASES: Final[tuple[GeneratedCase, ...]] = (
    GeneratedCase(
        "grid-table",
        "grid_table",
        "+---------------+---------------+--------------------+\n"
        "| Fruit         | Price         | Advantages         |\n"
        "+===============+===============+====================+\n"
        "| Bananas       | $1.34         | - built-in wrapper |\n"
        "|               |               | - bright color     |\n"
        "+---------------+---------------+--------------------+\n"
        "| Oranges       | $2.10         | - cures scurvy     |\n"
        "|               |               | - tasty            |\n"
        "+---------------+---------------+--------------------+\n",
    ),
    GeneratedCase(
        "multiline-table",
        "multiline_table",
        "-------------------------------------------------------------\n"
        " Centered   Default           Right Left\n"
        "  Header    Aligned         Aligned Aligned\n"
        "----------- ------- --------------- -------------------------\n"
        "   First    row                12.0 Example of a row that\n"
        "                                    spans multiple lines.\n"
        "\n"
        "  Second    row                 5.0 Here's another one. Note\n"
        "                                    the blank line between\n"
        "                                    rows.\n"
        "-------------------------------------------------------------\n"
        "\n"
        "Table: Here's the caption. It, too, may span\n"
        "multiple lines.\n",
    ),
    GeneratedCase(
        "simple-table",
        "simple_table",
        "  Right     Left     Center     Default\n"
        "-------     ------ ----------   -------\n"
        "     12     12        12            12\n"
        "    123     123       123          123\n"
        "\n"
        "Table:  Demonstration of simple table syntax.\n",
    ),
    GeneratedCase(
        "pipe-table-caption",
        "table_caption",
        "| a | b |\n"
        "|---|---|\n"
        "| 1 | 2 |\n"
        "\n"
        ": Demonstration of pipe table syntax.\n",
    ),
    GeneratedCase(
        "line-block",
        "line_block",
        "| The limerick packs laughs anatomical\n"
        "| In space that is quite economical.\n"
        "|    But the good ones I've seen\n"
        "|    So seldom are clean\n"
        "| And the clean ones so seldom are comical\n",
    ),
    GeneratedCase(
        "fenced-div",
        "fenced_div",
        "::::: {#special .sidebar}\n"
        "Here is a paragraph.\n"
        "\n"
        "And another.\n"
        ":::::\n",
    ),
    GeneratedCase(
        "definition-list",
        "definition_list",
        "Term 1\n"
        "\n"
        ":   Definition 1\n"
        "\n"
        "Term 2 with *inline markup*\n"
        "\n"
        ":   Definition 2\n",
    ),
    GeneratedCase(
        "footnotes",
        "footnote",
        "Here is a footnote reference,[^1] and another.[^longnote]\n"
        "\n"
        "[^1]: Here is the footnote.\n"
        "\n"
        "[^longnote]: Here's one with multiple blocks.\n"
        "\n"
        "    Subsequent paragraphs are indented.\n",
    ),
    GeneratedCase(
        "example-list",
        "example_list",
        "(@)  My first example will be numbered (1).\n"
        "(@)  My second example will be numbered (2).\n"
        "\n"
        "Explanation of examples.\n"
        "\n"
        "(@)  My third example will be numbered (3).\n",
    ),
    GeneratedCase(
        "fancy-list",
        "fancy_list",
        "a)  First item\n"
        "b)  Second item\n"
        "\n"
        "iv. Roman four\n"
        "v.  Roman five\n",
    ),
    GeneratedCase(
        "citations",
        "citation",
        "Blah blah [@doe99; @smith2000; @smith2004].\n"
        "\n"
        "Blah blah [see @doe99, pp. 33-35; also @smith2004, chap. 1].\n",
    ),
    GeneratedCase(
        "bracketed-span",
        "bracketed_span",
        "[This is a span]{.smallcaps}\n",
    ),
    GeneratedCase(
        "toml-metadata",
        "toml_metadata",
        '+++\ntitle = "TOML Example"\n+++\n\nBody text.\n',
    ),
    GeneratedCase(
        "yaml-front-matter",
        "yaml_front_matter",
        "---\ntitle: YAML Example\nauthor: Someone\n---\n\nBody text.\n",
    ),
    GeneratedCase(
        "implicit-figure",
        "implicit_figure",
        "![Caption text](image.png)\n",
    ),
    GeneratedCase(
        "subscript-superscript",
        "subscript_superscript",
        "H~2~O and 2^10^\n",
    ),
    GeneratedCase(
        "raw-tex",
        "raw_tex",
        "\\newcommand{\\tuple}[1]{\\langle #1 \\rangle}\n",
    ),
    GeneratedCase(
        "display-math",
        "display_math",
        "$$\nE = mc^2\n$$\n",
    ),
    GeneratedCase(
        "native-html-div",
        "native_html_div",
        '<div class="x">\n\nhi\n\n</div>\n',
    ),
)


# ---------------------------------------------------------------------------
# Канонизация
# ---------------------------------------------------------------------------


def _first_nonempty_line(text: str, block: MdBlock) -> str:
    """Первая непустая строка спана блока — для подсказки о конструкции."""
    piece = text[block.span.start_char : block.span.end_char]
    for line in piece.splitlines():
        if line.strip():
            return line
    return ""


def _preceded_by_text(text: str, block: MdBlock) -> bool:
    """Есть ли непосредственно перед блоком непустая строка (без пустой разделительной)."""
    start = block.span.start_char
    if start == 0:
        return False
    prefix = text[:start]
    if not prefix.endswith("\n"):
        return False
    previous = prefix[:-1].rsplit("\n", 1)
    return bool(previous and previous[-1].strip())


def _mdast_hint(block: MdBlock, line: str) -> str:
    """Подсказка по источнику: какую именно конструкцию не разобрал ``md_ast``."""
    stripped = line.lstrip(" ")
    if stripped.startswith("+") and any(ch in stripped for ch in "-="):
        return "grid_table"
    if stripped.startswith(("-", "=")) and set(stripped) <= {"-", "=", " "}:
        return "multiline_table"
    if stripped.startswith("(@)"):
        return "example_list"
    if stripped.startswith(":") and block.kind == "paragraph":
        return "table_caption"
    if block.kind == "html" or stripped.startswith("<div"):
        return "html_div"
    if stripped.startswith("\\"):
        return "raw_tex"
    if _FANCY_MARKER.match(stripped):
        return "fancy_list"
    return ""


_FANCY_MARKER: Final = re.compile(r"^(?:[A-Za-z]{1,4}|[ivxlcdmIVXLCDM]{1,6})[.)]\s")


def canonical_pandoc_blocks(blocks: list[dict[str, Any]]) -> list[BlockView]:
    """Pandoc JSON ``blocks`` → канонические виды (``Null`` пропускается, raw схлопывается)."""
    views: list[BlockView] = []
    for block in blocks:
        raw = str(block.get("t", "?"))
        if raw == "Null":
            continue
        canonical = _PANDOC_KINDS.get(raw, f"pandoc:{raw}")
        detail = ""
        if raw == "Para" and _is_display_math(block):
            canonical = "math"
            detail = "display_math"
        elif raw == "RawBlock":
            content = block.get("c") or []
            if content and isinstance(content[0], dict):
                detail = str(content[0].get("t", ""))
        if canonical == "raw" and views and views[-1].canonical == "raw":
            # Pandoc режет один HTML-фрагмент на несколько RawBlock — это нарезка,
            # а не структура; схлопываем подряд идущие.
            continue
        views.append(BlockView(canonical=canonical, raw=raw, detail=detail))
    return views


def _is_display_math(block: dict[str, Any]) -> bool:
    """``Para`` с единственным инлайном ``Math DisplayMath`` — это блочная формула."""
    inlines = block.get("c") or []
    if not isinstance(inlines, list) or len(inlines) != 1 or not isinstance(inlines[0], dict):
        return False
    if inlines[0].get("t") != "Math":
        return False
    payload = inlines[0].get("c") or []
    return bool(payload) and isinstance(payload[0], dict) and payload[0].get("t") == "DisplayMath"


def canonical_mdast_blocks(
    document: MdDocument, *, pandoc_meta: bool
) -> tuple[list[BlockView], Counter[str]]:
    """``md_ast`` → канонические виды плюс счётчик снятых со сравнения блоков."""
    views: list[BlockView] = []
    not_compared: Counter[str] = Counter()
    for block in document.blocks:
        if block.kind == "footnote_def":
            not_compared["footnote_inline"] += 1
            continue
        if block.kind == "front_matter":
            if pandoc_meta:
                not_compared["metadata"] += 1
                continue
            views.append(BlockView(canonical="metadata", raw="front_matter", line=block.span.start_line + 1))
            continue
        canonical = _MDAST_KINDS.get(block.kind, f"mdast:{block.kind}")
        line = _first_nonempty_line(document.text, block)
        hint = _mdast_hint(block, line)
        if block.kind == "list" and _preceded_by_text(document.text, block):
            # Pandoc по умолчанию требует пустую строку перед списком
            # (``lists_without_preceding_blankline`` выключено), markdown-it — нет.
            # Это расхождение семантики разбора, а не «лишний блок» без причины.
            hint = "list_without_blank_line"
        elif block.kind == "blockquote" and _preceded_by_text(document.text, block):
            # То же для цитаты: ``blank_before_blockquote`` у Pandoc включено, у
            # CommonMark прерывание абзаца через ``>`` законно.
            hint = "blockquote_without_blank_line"
        unparsed = ""
        if canonical == "paragraph" and hint in {"fancy_list", "example_list"}:
            # Абзац, начинающийся с маркера fancy/example-списка, — это список, который
            # наш парсер не разобрал. Класс списка нужен для выравнивания, а ``unparsed``
            # не даёт паре «совпасть»: она обязана попасть в ``unparsed_construct``.
            canonical = "list"
            unparsed = hint
        views.append(
            BlockView(
                canonical=canonical,
                raw=block.kind,
                line=block.span.start_line + 1,
                hint=hint,
                unparsed=unparsed,
            )
        )
    return views, not_compared


# ---------------------------------------------------------------------------
# Сравнение
# ---------------------------------------------------------------------------


def _classify_pair(pandoc: BlockView, mdast: BlockView) -> Diff | None:
    """Пара выровненных блоков → расхождение либо ``None``."""
    if pandoc.canonical == mdast.canonical:
        if mdast.unparsed:
            return Diff(
                "unparsed_construct", mdast.unparsed, pandoc.raw, mdast.raw, mdast.line
            )
        return None
    line = mdast.line
    if pandoc.canonical == "line_block":
        return Diff("unparsed_construct", "line_block", pandoc.raw, mdast.raw, line)
    if pandoc.canonical == "figure":
        return Diff("unparsed_construct", "figure", pandoc.raw, mdast.raw, line)
    if pandoc.canonical == "table" and mdast.canonical != "table":
        construct = mdast.hint if mdast.hint in {"grid_table", "multiline_table"} else "table"
        return Diff("unparsed_construct", construct, pandoc.raw, mdast.raw, line)
    if pandoc.canonical == "container" and mdast.canonical != "container":
        construct = "html_div" if mdast.hint == "html_div" else "div"
        return Diff("unparsed_construct", construct, pandoc.raw, mdast.raw, line)
    if pandoc.canonical == "raw" and mdast.canonical != "raw":
        construct = "raw_tex" if mdast.hint == "raw_tex" else "raw_block"
        return Diff("unparsed_construct", construct, pandoc.raw, mdast.raw, line)
    if pandoc.canonical == "list" and mdast.canonical == "paragraph" and mdast.hint:
        return Diff("unparsed_construct", mdast.hint, pandoc.raw, mdast.raw, line)
    return Diff(
        "type_mismatch",
        f"{pandoc.canonical}->{mdast.canonical}",
        pandoc.raw,
        mdast.raw,
        line,
    )


def compare_views(pandoc: list[BlockView], mdast: list[BlockView]) -> list[Diff]:
    """Выравнивает две последовательности видов и классифицирует расхождения.

    Выравнивание — ``difflib.SequenceMatcher`` по каноническим классам: так одна
    вставка не порождает каскад «все следующие блоки сдвинулись». Внутри ``replace``
    пары берутся по позиции; лишние блоки — ``sequence_length``, кроме подписи
    таблицы (абзац ``: caption`` сразу после таблицы) — у неё свой класс.
    """
    diffs: list[Diff] = []
    matcher = SequenceMatcher(
        None,
        [view.canonical for view in pandoc],
        [view.canonical for view in mdast],
        autojunk=False,
    )
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            # Классы совпали, но конструкция могла быть узнана и не разобрана
            # (``unparsed``): такая пара не «совпала», она обязана попасть в счётчик.
            for offset in range(i2 - i1):
                diff = _classify_pair(pandoc[i1 + offset], mdast[j1 + offset])
                if diff is not None:
                    diffs.append(diff)
            continue
        pairs = min(i2 - i1, j2 - j1)
        for offset in range(pairs):
            diff = _classify_pair(pandoc[i1 + offset], mdast[j1 + offset])
            if diff is not None:
                diffs.append(diff)
        for index in range(i1 + pairs, i2):
            diffs.append(
                Diff(
                    "sequence_length",
                    f"pandoc_only:{pandoc[index].canonical}",
                    pandoc[index].raw,
                    "",
                    None,
                )
            )
        for index in range(j1 + pairs, j2):
            block = mdast[index]
            if (
                block.canonical == "paragraph"
                and block.hint == "table_caption"
                and i1 > 0
                and pandoc[i1 - 1].canonical == "table"
            ):
                diffs.append(
                    Diff("unparsed_construct", "table_caption", "Table", block.raw, block.line)
                )
                continue
            diffs.append(
                Diff(
                    "sequence_length",
                    f"mdast_only:{block.hint or block.raw}",
                    "",
                    block.raw,
                    block.line,
                )
            )
    return diffs


def compare_document(
    text: str,
    pandoc_document: dict[str, Any],
    *,
    label: str,
    path: str,
) -> DocOutcome:
    """Один документ: канонизация обеих сторон + сравнение."""
    pandoc_views = canonical_pandoc_blocks(list(pandoc_document.get("blocks") or []))
    mdast_document = parse_markdown(text, PROFILE)
    mdast_views, not_compared = canonical_mdast_blocks(
        mdast_document, pandoc_meta=bool(pandoc_document.get("meta"))
    )
    outcome = DocOutcome(
        label=label,
        path=path,
        pandoc_types=[view.raw for view in pandoc_views],
        mdast_kinds=[view.raw for view in mdast_views],
        not_compared=not_compared,
    )
    outcome.diffs = compare_views(pandoc_views, mdast_views)
    return outcome


# ---------------------------------------------------------------------------
# Источники документов
# ---------------------------------------------------------------------------


def _tracked_markdown() -> list[Path] | None:
    """Markdown, **отслеживаемый git** (``docs/**/*.md`` + ``*.md`` в корне).

    Рабочее дерево может содержать неотслеживаемые файлы других полос; замер на них
    менял бы числа от прогона к прогону. ``git ls-files`` делает выборку привязанной к
    коммиту. Нет git — возвращается ``None``, вызывающий падает на обход дерева.
    """
    try:
        completed = subprocess.run(
            ["git", "-C", str(_ROOT), "ls-files", "-z"],
            capture_output=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    paths: list[Path] = []
    for name in completed.stdout.decode("utf-8", "replace").split("\0"):
        if not name.endswith(".md"):
            continue
        if name.startswith("docs/") or "/" not in name:
            paths.append(_ROOT / name)
    return sorted(paths)


def discover_documents(max_docs: int) -> tuple[list[Path], int]:
    """Детерминированная выборка markdown дерева: ``docs/**/*.md`` + ``*.md`` в корне.

    Возвращает ``(выборка, всего)``. Сортировка по пути, затем равномерный шаг —
    «первые N» дали бы только первый каталог. Источник — ``git ls-files`` (см.
    :func:`_tracked_markdown`); без git — обход файловой системы.
    """
    tracked = _tracked_markdown()
    if tracked is None:
        paths = sorted((_ROOT / "docs").rglob("*.md")) + sorted(_ROOT.glob("*.md"))
    else:
        paths = tracked
    total = len(paths)
    if total <= max_docs:
        return paths, total
    step = total / max_docs
    # int(i * step) строго возрастает при total > max_docs.
    return [paths[int(i * step)] for i in range(max_docs)], total


# ---------------------------------------------------------------------------
# Прогон и вывод
# ---------------------------------------------------------------------------


def _pandoc_version(pandoc: str) -> str | None:
    try:
        completed = subprocess.run(
            [pandoc, "--version"], capture_output=True, text=True, timeout=60, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.splitlines()[0].strip() if completed.stdout else "unknown"


def _run_pandoc(pandoc: str, text: str, timeout: float) -> tuple[dict[str, Any] | None, str]:
    """``pandoc -f markdown -t json`` на одном документе; возвращает (AST, ошибка)."""
    try:
        completed = subprocess.run(
            [pandoc, "-f", "markdown", "-t", "json"],
            input=text,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, f"pandoc: timeout {timeout:g}s"
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f"pandoc: {exc}"
    if completed.returncode != 0:
        message = (completed.stderr or "").strip().splitlines()
        return None, f"pandoc exit {completed.returncode}: {message[0] if message else ''}"
    try:
        return json.loads(completed.stdout), ""
    except json.JSONDecodeError as exc:
        return None, f"pandoc json: {exc}"


def _format_location(outcome: DocOutcome, diff: Diff) -> str:
    if diff.line is None:
        return f"{outcome.path}:? (pandoc block)"
    return f"{outcome.path}:{diff.line}"


def run_corpus(
    documents: list[tuple[str, str, str]],
    *,
    pandoc: str,
    timeout: float,
    out_json: str | None,
) -> int:
    """Прогоняет документы, печатает отчёт и возвращает код (0/1/2)."""
    version = _pandoc_version(pandoc)
    if version is None:
        print(f"SKIP: pandoc не отвечает на `{pandoc} --version`", file=sys.stderr)
        return 2

    started = time.perf_counter()
    outcomes: list[DocOutcome] = []
    stats: dict[str, ClassStat] = {name: ClassStat() for name in DISCREPANCY_CLASSES}
    not_compared: Counter[str] = Counter()
    not_compared_docs: dict[str, set[str]] = {name: set() for name in NOT_COMPARED_REASONS}

    for label, path, text in documents:
        pandoc_document, error = _run_pandoc(pandoc, text, timeout)
        if pandoc_document is None:
            outcome = DocOutcome(
                label=label,
                path=path,
                diffs=[Diff("oracle_error", "oracle", "", "", None, error)],
            )
        else:
            try:
                outcome = compare_document(text, pandoc_document, label=label, path=path)
            except ValueError as exc:  # неизвестный профиль/битый вход — видимый отказ
                outcome = DocOutcome(
                    label=label,
                    path=path,
                    diffs=[Diff("oracle_error", "parse", "", "", None, str(exc))],
                )
        outcomes.append(outcome)
        for diff in outcome.diffs:
            if diff.cls in stats:
                stats[diff.cls].add(outcome.label, diff.construct)
        for reason, count in outcome.not_compared.items():
            not_compared[reason] += count
            not_compared_docs.setdefault(reason, set()).add(outcome.label)

    elapsed = time.perf_counter() - started
    total_diffs = sum(stat.blocks for stat in stats.values())
    compared = sum(len(outcome.pandoc_types) for outcome in outcomes)

    print(f"pandoc: {version}")
    print("режим: types-only (в JSON 2.17.1.1 нет sourcepos; см. докстринг)")
    print(f"документов: {len(outcomes)}; блоков Pandoc: {compared}; время: {elapsed:.2f} с")
    print()
    if total_diffs == 0:
        print("расхождений блочных видов: 0")
    else:
        print("классы расхождений:")
        for name in DISCREPANCY_CLASSES:
            stat = stats[name]
            if not stat.blocks:
                continue
            constructs = ", ".join(
                f"{construct} {count}" for construct, count in stat.constructs.most_common()
            )
            print(
                f"  {name:20} документов {len(stat.documents):3}  блоков {stat.blocks:4}"
                f"  [{constructs}]"
            )
        print()
        print("первые примеры:")
        limit = _example_limit
        shown: Counter[str] = Counter()
        for outcome in sorted(outcomes, key=lambda item: item.label):
            for diff in outcome.diffs:
                if shown[diff.cls] >= limit:
                    continue
                shown[diff.cls] += 1
                print(
                    f"  [{diff.cls}:{diff.construct}] {_format_location(outcome, diff)}"
                    f"  pandoc={diff.pandoc_type or '-'} mdast={diff.mdast_kind or '-'}"
                    f"  {diff.detail}".rstrip()
                )
    if not_compared:
        print()
        print("снято со сравнения (не «совпало»):")
        for reason, count in sorted(not_compared.items()):
            docs = len(not_compared_docs.get(reason, ()))
            print(f"  {reason:18} блоков {count:4}  документов {docs:3}  ({NOT_COMPARED_REASONS.get(reason, '')})")

    if out_json:
        payload = {
            "mode": "types-only",
            "pandoc": version,
            "profile": PROFILE,
            "documents": len(outcomes),
            "compared_blocks": compared,
            "elapsed_s": round(elapsed, 3),
            "classes": {
                name: {
                    "documents": len(stats[name].documents),
                    "blocks": stats[name].blocks,
                    "constructs": dict(stats[name].constructs.most_common()),
                }
                for name in DISCREPANCY_CLASSES
                if stats[name].blocks
            },
            "not_compared": {
                reason: {"blocks": count, "documents": len(not_compared_docs.get(reason, ()))}
                for reason, count in sorted(not_compared.items())
            },
            "outcomes": [outcome.as_dict() for outcome in outcomes],
        }
        Path(out_json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"\njson: {out_json}")

    return 0 if total_diffs == 0 else 1


#: Сколько примеров на класс печатать (переопределяется ``--show``).
_example_limit: int = 8


def _load_documents(
    paths: list[str], *, max_docs: int, generated: bool, repo_docs: bool
) -> tuple[list[tuple[str, str, str]], list[str]]:
    """Собирает документы: явные пути заменяют и выборку дерева, и сгенерированный набор."""
    documents: list[tuple[str, str, str]] = []
    warnings: list[str] = []
    if paths:
        for raw in paths:
            path = Path(raw)
            candidates = [path] if path.is_file() else sorted(path.rglob("*.md"))
            for candidate in candidates:
                try:
                    text = candidate.read_text(encoding="utf-8")
                except (OSError, UnicodeDecodeError) as exc:
                    warnings.append(f"{candidate}: {exc}")
                    continue
                label = (
                    str(candidate.relative_to(_ROOT))
                    if candidate.is_absolute() and _ROOT in candidate.parents
                    else str(candidate)
                )
                documents.append((label, str(candidate), text))
    elif repo_docs:
        sample, total = discover_documents(max_docs)
        for path in sample:
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                warnings.append(f"{path}: {exc}")
                continue
            documents.append((str(path.relative_to(_ROOT)), str(path), text))
        if total > len(sample):
            warnings.append(f"выборка дерева: {len(sample)} из {total} (равномерный шаг)")
    if generated and not paths:
        for case in GENERATED_CASES:
            documents.append((f"generated:{case.name}", f"generated:{case.construct}", case.text))
    return documents, warnings


def main(argv: list[str] | None = None) -> int:
    """CLI. Коды: 0 — чисто, 1 — расхождения, 2 — оракула/документов нет."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "paths",
        nargs="*",
        help="файлы/каталоги; если заданы — сравниваются только они (без выборки дерева и набора)",
    )
    parser.add_argument("--pandoc", default="pandoc", help="путь к oracle-бинарю pandoc")
    parser.add_argument("--max-docs", type=int, default=40, help="размер выборки docs/**/*.md")
    parser.add_argument("--show", type=int, default=8, help="примеров на класс расхождений")
    parser.add_argument("--out-json", default=None, help="записать машинный отчёт")
    parser.add_argument("--timeout", type=float, default=120.0, help="таймаут pandoc на документ, с")
    parser.add_argument("--no-generated", action="store_true", help="без сгенерированного набора")
    parser.add_argument("--no-docs", action="store_true", help="без выборки docs/**/*.md")
    args = parser.parse_args(argv)

    global _example_limit
    _example_limit = max(0, args.show)

    executable = shutil.which(args.pandoc)
    if executable is None:
        print(
            f"SKIP: pandoc не найден (`{args.pandoc}`); оракул доступен только на хосте "
            "замера/в CI. Сравнение не выполнено — это не «0 расхождений».",
            file=sys.stderr,
        )
        return 2

    documents, warnings = _load_documents(
        args.paths,
        max_docs=max(1, args.max_docs),
        generated=not args.no_generated,
        repo_docs=not args.no_docs,
    )
    for warning in warnings:
        print(f"notice: {warning}", file=sys.stderr)
    if not documents:
        print("SKIP: нет документов для сравнения", file=sys.stderr)
        return 2
    return run_corpus(
        documents,
        pandoc=executable,
        timeout=args.timeout,
        out_json=args.out_json,
    )


if __name__ == "__main__":
    raise SystemExit(main())