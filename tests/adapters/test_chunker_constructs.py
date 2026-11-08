"""Мутационная приёмка нарезки по конструкциям (план 18 §5/§7, P1/P2/P8–P10).

Зачем отдельный модуль, если есть ``test_chunker_markdown.py``. Тот файл пинит
**конкретные** случаи: один список, одна таблица, одна цитата. Здесь проверяется
**свойство**, которое обязано держаться при любой форме конструкции: чанкер не имеет
права терять текст (P1), начинать кусок закрывающей пунктуацией (P8), отрывать шапку
таблицы от её строк (P9), отрывать info string от тела fence (P10) и оставлять
продолжение пункта списка без маркера (P2).

Плюс **атомарность единицы** (§7): при бюджете заведомо больше конструкции её первый
и последний непробельные символы обязаны попасть в **один** чанк, а если документ
состоит из одной конструкции — чанк ровно один. При малом бюджете конструкция режется,
и продолжение несёт объявленный контекст (шапку таблицы, info string, маркер пункта,
``> `` цитаты, ``\\begin{…}`` raw-блока). Обе ветки проверяются на **одних и тех же**
формах, поэтому «влезает целиком» и «режется с контекстом» не расходятся.

Инварианты считаются по **тексту и его геометрии**, без повторного разбора в AST:
проверка не должна опираться на тот же разбор, который использует чанкер, иначе она
подтверждает его ошибку вместо того, чтобы её поймать.

Страж ``test_atomicity_forms_do_not_silently_vanish`` падает, если форма перестала
попадать в проверку атомарности (неизвестный ``kind``, пропавшая конструкция §5,
документ длиннее объявленного бюджета). «Зелено, потому что ничего не проверялось» —
запрещённый исход.

Нейтрален (§1.3).
"""

from __future__ import annotations

import itertools
import re
from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from core.adapters.chunker_markdown import MarkdownChunker
from core.util.text_utils import CLOSING_CHARS

#: Бюджет куска намеренно мал: конструкция гарантированно режется, и «продолжение»
#: проверяется, а не остаётся недостижимым.
CHUNK_SIZE = 220

#: Бюджет заведомо больше любой формы: здесь проверяется, что конструкция **влезает**
#: целиком и не разрезается внутренней границей. Страж ниже требует, чтобы это число
#: действительно превышало самый длинный документ реестра.
ATOMIC_BUDGET = 20_000

#: Сколько строк внутрь конструкции, чтобы она переросла ``CHUNK_SIZE``.
FILLER_ROWS = 24

#: Слово-наполнитель: резы внутри абзаца/пункта должны иметь безопасные позиции.
_WORD = "слово"


def _words(count: int = 18) -> str:
    """Строка из ``count`` различных слов: переполняет бюджет и даёт пробелы для реза."""
    return " ".join(f"{_WORD}{index}" for index in range(count))


def _filler() -> list[str]:
    return [f"строка {n} с достаточно длинным содержимым для переполнения бюджета" for n in range(FILLER_ROWS)]


#: Вид формы — чтобы проверка знала, какого контекста требовать от продолжения.
#:
#: ``LIST``/``TABLE``/``FENCE`` — уже объявленные проверки P2/P9/P10.
#: ``PANDOC_TABLE`` — grid/multiline/simple/подпись: шапка синтезируется в pipe-виде.
#: ``CODE`` — indented code: режется по строкам, своего маркера у него нет.
#: ``HTML``/``RAW`` — raw-блоки: HTML и TeX (у TeX есть маркер ``\\begin{…}``).
#: ``QUOTE``/``DIV``/``COLON_DIV`` — контейнеры: цитата, нативный ``<div>``, ``:::``.
#: ``LINE_BLOCK`` — строки ``| …`` профиля ``pandoc``.
#: ``DEFLIST``/``MATH``/``FOOTNOTE``/``META``/``REFERENCE`` — атомарные блоки.
LIST, TABLE, PANDOC_TABLE, FENCE, CODE = "list", "table", "pandoc_table", "fence", "code"
RAW, HTML, PLAIN, QUOTE, DIV, COLON_DIV = "raw", "html", "plain", "quote", "div", "colon_div"
LINE_BLOCK, DEFLIST, MATH, FOOTNOTE, META, REFERENCE = (
    "line_block",
    "deflist",
    "math",
    "footnote",
    "meta",
    "reference",
)

ATOMIC_KINDS: frozenset[str] = frozenset(
    {
        LIST,
        TABLE,
        PANDOC_TABLE,
        FENCE,
        CODE,
        RAW,
        HTML,
        PLAIN,
        QUOTE,
        DIV,
        COLON_DIV,
        LINE_BLOCK,
        DEFLIST,
        MATH,
        FOOTNOTE,
        META,
        REFERENCE,
    }
)

#: Виды, для которых проверка продолжения **есть** (контекст объявлен и держится).
CONTINUATION_CHECKED: frozenset[str] = frozenset(
    {LIST, TABLE, PANDOC_TABLE, FENCE, CODE, RAW, PLAIN, QUOTE, DIV, LINE_BLOCK, META, REFERENCE}
)

#: Виды, для которых контекст продолжения §5 сегодня **не** держится или не объявлен;
#: причина названа, чтобы остаток был виден, а не исчезал как «зелёный skip».
CONTINUATION_UNCHECKED: dict[str, str] = {
    DEFLIST: "чанкер режет между термином и его определением и не несёт терм в контексте",
    COLON_DIV: "`colon_fence` не разбирает содержимое на блоки: жёсткий рез без маркера `:::`",
    MATH: "при переполнении продолжение не несёт `$$`",
    FOOTNOTE: "при переполнении продолжение не несёт id определения",
    HTML: "продолжение raw-HTML не несёт маркер raw-блока",
}

#: Виды, где §5 разрешает резать только между строками: продолжение обязано начинаться
#: с начала строки, иначе разрезана атомарная единица (строка таблицы / строка кода).
_LINE_UNIT_KINDS: frozenset[str] = frozenset(
    {TABLE, PANDOC_TABLE, FENCE, CODE, RAW, LINE_BLOCK}
)

#: Конструкции §5, для которых единица нарезки объявлена. Каждая обязана иметь форму;
#: инлайновые токены (citation/footnote ref/bracketed span) сюда не входят: это не
#: блочная единица, и геометрия чанка к ним неприменима (названо в §7 плана).
REQUIRED_CONSTRUCTS: frozenset[str] = frozenset(
    {
        "atx_heading",
        "setext_heading",
        "paragraph",
        "fence",
        "indented_code",
        "raw_html",
        "raw_tex",
        "pipe_table",
        "grid_table",
        "multiline_table",
        "simple_table",
        "table_caption",
        "list",
        "task_list",
        "definition_list",
        "blockquote",
        "fenced_div",
        "line_block",
        "footnote_definition",
        "math_block",
        "yaml_metadata",
        "reference_definition",
        "figure",
        "fancy_list",
        "example_list",
    }
)

#: Маркер пункта списка в начале строки (CommonMark, задачи и fancy/example Pandoc).
_ITEM_START = re.compile(
    r"^[ \t]*(?:[-+*]|\d{1,9}[.)]|[A-Za-z][.)]|[ivxlcdmIVXLCDM]+[.)]|#\.|\(@[^)\n]*\))[ \t]"
)


@dataclass(frozen=True, slots=True)
class Form:
    """Одна проверяемая форма конструкции.

    ``document`` — документ, состоящий **из одной** конструкции (возможно, с
    наполнителем внутри). Один и тот же документ проверяется двумя бюджетами:
    ``ATOMIC_BUDGET`` (должен влезть целиком) и ``CHUNK_SIZE`` (должен резаться с
    объявленным контекстом). Это и есть §7: обе ветки на одних данных.
    """

    name: str
    construct: str
    kind: str
    document: str
    profile: str = "gfm"


# ---------------------------------------------------------------------------
# Генераторы форм: одна конструкция §5 — одна форма
# ---------------------------------------------------------------------------


def _list_forms() -> Iterator[Form]:
    """Списки: маркер, начало нумерации, вложенность, отметка задачи."""
    for marker in ("-", "*", "+"):
        body = "\n".join(f"{marker} пункт {n} {_words()}" for n in range(FILLER_ROWS))
        yield Form(f"список {marker!r}", "list", LIST, body + "\n")
    for start in (1, 3, 10):
        body = "\n".join(
            f"{start + n}. пункт {n} {_words()}" for n in range(FILLER_ROWS)
        )
        yield Form(f"нумерованный с {start}", "list", LIST, body + "\n")
    body = "\n".join(
        f"- пункт {n} {_words()}\n    - вложенный {n} {_words()}" for n in range(FILLER_ROWS)
    )
    yield Form("вложенный", "list", LIST, body + "\n")
    for mark in ("[ ]", "[x]"):
        body = "\n".join(f"- {mark} пункт {n} {_words()}" for n in range(FILLER_ROWS))
        yield Form(f"task list {mark}", "task_list", LIST, body + "\n")


def _fancy_list_forms() -> Iterator[Form]:
    """Fancy/example-списки профиля ``pandoc``: маркер и метка — часть контекста."""
    yield Form(
        "fancy a)",
        "fancy_list",
        LIST,
        "".join(f"{chr(ord('a') + n)}) пункт {n} {_words()}\n" for n in range(20)),
        "pandoc",
    )
    yield Form(
        "fancy i.",
        "fancy_list",
        LIST,
        "".join(f"{'i' * (n + 1)}. пункт {n} {_words()}\n" for n in range(10)),
        "pandoc",
    )
    yield Form(
        "example (@)",
        "example_list",
        LIST,
        "".join(f"(@item{n}) пример {n} {_words()}\n" for n in range(20)),
        "pandoc",
    )


def _table_forms() -> Iterator[Form]:
    """Pipe-таблицы: число колонок и выравнивание (P9 — шапка с разделителем)."""
    for columns, align in itertools.product((2, 3), ("---", ":--", "--:", ":-:")):
        header = "| " + " | ".join(f"колонка {i}" for i in range(columns)) + " |"
        separator = "|" + "|".join(align for _ in range(columns)) + "|"
        rows = [
            "| " + " | ".join(f"значение {n}-{i}" for i in range(columns)) + " |"
            for n in range(FILLER_ROWS)
        ]
        yield Form(
            f"таблица {columns}×{align}",
            "pipe_table",
            TABLE,
            "\n".join([header, separator, *rows]) + "\n",
        )


def _fence_forms() -> Iterator[Form]:
    """Fence: info string, вид забора, тильды против обратных кавычек (P10)."""
    for info, fence in itertools.product(("sql", "python title=app.py", ""), ("```", "~~~~")):
        body = [f"SELECT {n} FROM таблица;" for n in range(FILLER_ROWS)]
        yield Form(
            f"fence {fence!r} info={info!r}",
            "fence",
            FENCE,
            "\n".join([f"{fence}{info}", *body, fence]) + "\n",
        )


def _code_forms() -> Iterator[Form]:
    """Indented code: блок кода без info string и без маркера забора."""
    body = "".join(f"    кодовая строка {n} {_words(6)}\n" for n in range(FILLER_ROWS))
    yield Form("indented code", "indented_code", CODE, body)


def _raw_forms() -> Iterator[Form]:
    """Raw-блоки: HTML (маркер не объявлен) и TeX (маркер ``\\begin{…}``)."""
    html_body = (
        "<div>\n"
        + "".join(f"<p>строка {n} {_words(5)}</p>\n" for n in range(FILLER_ROWS))
        + "</div>\n"
    )
    yield Form("raw html", "raw_html", HTML, html_body)
    tex_body = (
        "\\begin{verbatim}\n"
        + "".join(f"строка {n} {_words(5)}\n" for n in range(FILLER_ROWS))
        + "\\end{verbatim}\n"
    )
    yield Form("raw TeX", "raw_tex", RAW, tex_body, "pandoc")


def _plain_forms() -> Iterator[Form]:
    """Заголовки, абзац и figure: у этих единиц контекст не синтезируется."""
    for level in (1, 2, 3):
        title = f"Раздел {level} " + " ".join(f"{_WORD}{n}" for n in range(40))
        yield Form(
            f"ATX заголовок {level} уровня",
            "atx_heading",
            PLAIN,
            f"{'#' * level} {title}\n",
        )
    yield Form(
        "setext-заголовок",
        "setext_heading",
        PLAIN,
        "Раздел документа\n=================\n",
    )
    paragraph = " ".join(
        f"Предложение номер {n} с достаточно длинным содержимым." for n in range(20)
    )
    yield Form("абзац", "paragraph", PLAIN, paragraph + "\n")
    yield Form("figure", "figure", PLAIN, "![Подпись рисунка](image.png)\n")


def _container_forms() -> Iterator[Form]:
    """Цитата, нативный div и ``:::``-div: границы стоят между дочерними блоками."""
    yield Form("цитата", "blockquote", QUOTE, "".join(f"> {line}\n>\n" for line in _filler()))

    paragraphs = "\n\n".join(f"абзац {n} {_words(6)}" for n in range(FILLER_ROWS))
    yield Form(
        "нативный div",
        "fenced_div",
        DIV,
        f'<div class="x">\n\n{paragraphs}\n\n</div>\n',
        "pandoc",
    )
    yield Form(
        "fenced div :::",
        "fenced_div",
        COLON_DIV,
        f"::: {{.note}}\n\n{paragraphs}\n\n:::\n",
        "pandoc",
    )


def _pandoc_table_forms() -> Iterator[Form]:
    """Grid/multiline/simple-таблицы и подпись: шапка синтезируется в pipe-виде."""
    yield Form("grid-таблица", "grid_table", PANDOC_TABLE, _grid_table(), "pandoc")
    yield Form("multiline-таблица", "multiline_table", PANDOC_TABLE, _multiline_table(), "pandoc")
    simple = _simple_table()
    yield Form("simple-таблица", "simple_table", PANDOC_TABLE, simple, "pandoc")
    yield Form(
        "подпись таблицы",
        "table_caption",
        PANDOC_TABLE,
        simple + "\n: Подпись таблицы\n",
        "pandoc",
    )


def _grid_table(rows: int = FILLER_ROWS) -> str:
    """Grid-таблица с рамкой ровно по позициям ``+`` (иначе строки не распознаются)."""
    border = "+" + "-" * 7 + "+" + "-" * 7 + "+\n"
    separator = "+" + "=" * 7 + "+" + "=" * 7 + "+\n"

    def row(left: str, right: str) -> str:
        return "| " + f"{left:<5}" + " | " + f"{right:<5}" + " |\n"

    body = border + row("a", "b") + separator
    return body + "".join(row(f"r{n}", f"v{n}") + border for n in range(rows))


def _dash_row(cells: tuple[str, str]) -> str:
    """Строка dash-таблицы: ячейки стоят на колонках разделителя ``------- -------``."""
    return " " + "".join(f"{cell:<7}" for cell in cells) + "\n"


def _multiline_table(rows: int = FILLER_ROWS) -> str:
    """Multiline-таблица: ряды разделены пустой строкой, иначе это один ряд."""
    header = _dash_row(("A", "B"))
    separator = "------- -------\n"
    body = "\n\n".join(_dash_row((f"r{n}", f"v{n}")) for n in range(rows))
    return "-" * 15 + "\n" + header + separator + body + "\n" + "-" * 15 + "\n"


def _simple_table(rows: int = FILLER_ROWS) -> str:
    """Simple-таблица: одна строка шапки, разделитель, по строке на ряд."""
    header = _dash_row(("A", "B"))
    separator = "------- -------\n"
    return header + separator + "".join(
        _dash_row((f"r{n}", f"v{n}")) for n in range(rows)
    )


def _atomic_block_forms() -> Iterator[Form]:
    """Атомарные блоки §5: definition list, line block, сноска, формула, YAML, ссылка."""
    definition = "".join(
        f"Термин {n}\n:   Определение {n} {_words(6)}\n\n" for n in range(FILLER_ROWS)
    )
    yield Form("definition list", "definition_list", DEFLIST, definition, "pandoc")

    lines = "".join(f"| строка {n} {_words(6)}\n" for n in range(FILLER_ROWS))
    yield Form("line block", "line_block", LINE_BLOCK, lines, "pandoc")

    footnote = "См. [^1].\n\n[^1]:\n" + "".join(
        f"    строка {n} {_words(6)}\n" for n in range(FILLER_ROWS)
    )
    yield Form("footnote definition", "footnote_definition", FOOTNOTE, footnote, "pandoc")

    math = "$$\n" + "".join(
        f"x_{n} + y_{n} = z_{n} + {_words(4)}\n" for n in range(FILLER_ROWS)
    ) + "$$\n"
    yield Form("math block", "math_block", MATH, math, "pandoc")

    yaml = "---\n" + "".join(
        f"key{n}: значение {n} {_words(4)}\n" for n in range(FILLER_ROWS)
    ) + "---\n"
    yield Form("YAML metadata", "yaml_metadata", META, yaml)

    reference = f'[ref]: http://example.com/{"a" * 300} "title"\n'
    yield Form("reference definition", "reference_definition", REFERENCE, reference)


def _nested_forms() -> Iterator[Form]:
    """Конструкция внутри цитаты/пункта: контейнерный префикс не должен двоиться."""
    yield Form(
        "таблица в цитате",
        "pipe_table",
        TABLE,
        "> | a | b |\n> |---|---|\n"
        + "".join(f"> | {n} | значение {n} |\n" for n in range(FILLER_ROWS)),
    )
    yield Form(
        "fence в цитате",
        "fence",
        FENCE,
        "> ```sql\n" + "".join(f"> SELECT {n};\n" for n in range(FILLER_ROWS)) + "> ```\n",
    )
    yield Form(
        "список в цитате",
        "list",
        LIST,
        "".join(f"> - {line}\n" for line in _filler()),
    )
    # Вложенность «пункт внутри цитаты» — тот случай, где контейнерный префикс и маркер
    # пункта встречаются: сырая строка уже несёт `> `, а маркер `> - ` добавляется отдельно.
    yield Form(
        "таблица в пункте в цитате",
        "pipe_table",
        TABLE,
        "# T\n\n> - пункт:\n>\n>   | a | b |\n>   |---|---|\n"
        + "".join(f">   | {n} | значение {n} |\n" for n in range(FILLER_ROWS)),
    )
    yield Form(
        "fence в пункте в цитате",
        "fence",
        FENCE,
        "# F\n\n> - пункт:\n>\n>   ```sql\n"
        + "".join(f">   SELECT {n};\n" for n in range(FILLER_ROWS))
        + ">   ```\n",
    )
    yield Form(
        "fence в пункте",
        "fence",
        FENCE,
        "- пункт\n\n  ```sql\n"
        + "".join(f"  SELECT {n};\n" for n in range(FILLER_ROWS))
        + "  ```\n",
    )


FORMS: list[Form] = [
    *_list_forms(),
    *_fancy_list_forms(),
    *_table_forms(),
    *_fence_forms(),
    *_code_forms(),
    *_raw_forms(),
    *_plain_forms(),
    *_container_forms(),
    *_pandoc_table_forms(),
    *_atomic_block_forms(),
    *_nested_forms(),
]

def _ids(forms: list[Form]) -> list[str]:
    return [form.name for form in forms]


def _applicable(*kinds: str, need_cut: bool = False) -> list[Form]:
    """Формы только тех видов, к которым проверка **применима**.

    ``need_cut`` — проверка продолжения: документ обязан перерасти ``CHUNK_SIZE``, иначе
    резать нечего и пара просто не попадает в параметризацию. Пропуск здесь запрещён:
    объявленное-но-невызываемое — это ``⬜``, а не зелёный skip.
    """
    forms = [
        form
        for form in FORMS
        if form.kind in kinds and (not need_cut or len(form.document) > CHUNK_SIZE)
    ]
    assert forms, f"нет применимых форм для видов {sorted(kinds)} (need_cut={need_cut})"
    return forms


#: Применимые пары «проверка × форма». Каждый список — только те формы, для которых
#: проверка что-то утверждает; неприменимые пары не собираются вовсе (а не пропускаются).
PIPE_TABLE_FORMS = _applicable(TABLE, need_cut=True)
PANDOC_TABLE_FORMS = _applicable(PANDOC_TABLE, need_cut=True)
FENCE_FORMS = _applicable(FENCE, need_cut=True)
LIST_FORMS = _applicable(LIST, need_cut=True)
LINE_UNIT_FORMS = _applicable(*_LINE_UNIT_KINDS, need_cut=True)
LINE_BLOCK_FORMS = _applicable(LINE_BLOCK, need_cut=True)
RAW_FORMS = _applicable(RAW, need_cut=True)
CONTAINER_FORMS = _applicable(QUOTE, DIV, need_cut=True)

ALL_PARAMS = pytest.mark.parametrize("form", FORMS, ids=_ids(FORMS))
PIPE_TABLE_PARAMS = pytest.mark.parametrize(
    "form", PIPE_TABLE_FORMS, ids=_ids(PIPE_TABLE_FORMS)
)
PANDOC_TABLE_PARAMS = pytest.mark.parametrize(
    "form", PANDOC_TABLE_FORMS, ids=_ids(PANDOC_TABLE_FORMS)
)
FENCE_PARAMS = pytest.mark.parametrize("form", FENCE_FORMS, ids=_ids(FENCE_FORMS))
LIST_PARAMS = pytest.mark.parametrize("form", LIST_FORMS, ids=_ids(LIST_FORMS))
LINE_UNIT_PARAMS = pytest.mark.parametrize(
    "form", LINE_UNIT_FORMS, ids=_ids(LINE_UNIT_FORMS)
)
LINE_BLOCK_PARAMS = pytest.mark.parametrize(
    "form", LINE_BLOCK_FORMS, ids=_ids(LINE_BLOCK_FORMS)
)
RAW_PARAMS = pytest.mark.parametrize("form", RAW_FORMS, ids=_ids(RAW_FORMS))
CONTAINER_PARAMS = pytest.mark.parametrize(
    "form", CONTAINER_FORMS, ids=_ids(CONTAINER_FORMS)
)


def _chunks(form: Form, budget: int = CHUNK_SIZE):
    return MarkdownChunker(chunk_size=budget, profile=form.profile).chunk_text("doc", form.document)


def _continuation_chunks(form: Form):
    """Куски после первого; форма в параметризации продолжения обязана резаться.

    Если форма перестала резаться, это падение, а не skip: иначе проверка молча
    выродилась бы в «ничего не проверялось».
    """
    chunks = _chunks(form)
    assert len(chunks) > 1, f"{form.name}: форма попала в проверку продолжения, но не режется"
    return chunks[1:]


def _first_line(chunk_text: str) -> str:
    """Первая строка куска без префикса цитаты: с ней сравниваются маркеры."""
    return chunk_text.split("\n", 1)[0].strip().lstrip(">").strip()


def _is_table_row(line: str) -> bool:
    return line.startswith("|") and line.count("|") >= 2


def _table_columns(line: str) -> int:
    return len(line.strip("|").split("|"))


def _is_delimiter_row(line: str) -> bool:
    """Строка-разделитель шапки: только трубы, дефисы, двоеточия и пробелы.

    Проверять наличие трёх дефисов нельзя: GFM разрешает и ``|:-|-:|`` — разделитель
    узнаётся по **составу** символов, а не по их числу.
    """
    return _is_table_row(line) and set(line) <= set("|-: ")


#: Строка-маркер: контейнер/пункт без содержимого (``> - ``, ``- ``, ``> ``). Такие
#: строки чанкер синтезирует сам — их в документе нет и быть не должно.
_MARKER_ONLY = re.compile(
    r"^(?:[ \t]*>[ \t]?)*(?:[-+*]|\d{1,9}[.)]|[A-Za-z][.)]|[ivxlcdmIVXLCDM]+[.)]|#\.|\(@[^)\n]*\))?[ \t]*$"
)


def _is_marker_only(line: str) -> bool:
    return bool(_MARKER_ONLY.match(line))


def _source_lines(document: str) -> set[str]:
    """Строки документа как есть: сырая строка контекста обязана среди них найтись.

    Это и есть проверка «контекст не выдуман»: доклеенный префикс даёт строку
    (``> - >   ```sql``), которой в документе нет, и свойство её ловит.
    """
    return {line.rstrip("\r") for line in document.split("\n")}


def _context_lines(chunk) -> list[str]:
    """Строки контекста **как есть**: префикс цитаты не снимается — он часть провенанса."""
    return [line.rstrip() for line in chunk.context_prefix.split("\n") if line.strip()]


def _bare(line: str) -> str:
    """Строка без префикса цитаты — для распознавания вида строки."""
    return line.lstrip(">").strip()


def _visible_bounds(document: str) -> tuple[int, int]:
    """Первый и последний **непробельные** символы документа (по геометрии текста)."""
    start = len(document) - len(document.lstrip())
    end = len(document.rstrip())
    return start, end


# ---------------------------------------------------------------------------
# Инварианты
# ---------------------------------------------------------------------------


@ALL_PARAMS
def test_p1_text_is_an_exact_concatenation(form: Form) -> None:
    """P1: чанки — точные срезы, их конкатенация возвращает вход."""
    chunks = _chunks(form)

    assert chunks, f"{form.name}: документ не дал ни одного чанка"
    assert "".join(chunk.text for chunk in chunks) == form.document


@ALL_PARAMS
def test_p8_no_chunk_starts_with_closing_punctuation(form: Form) -> None:
    """P8: кусок не открывается закрывающей пунктуацией без основания в источнике.

    Если первый символ куска — закрывающий, кусок обязан начинаться там же, где
    начинается **строка документа** (``:   Определение`` в definition list — законный
    старт строки). Иначе это след реза, а не свойство документа.
    """
    document = form.document
    for chunk in _chunks(form):
        stripped = chunk.text.lstrip()
        if not stripped:
            continue
        first = stripped[0]
        if first not in CLOSING_CHARS:
            continue
        offset = chunk.char_start + (len(chunk.text) - len(stripped))
        at_line_start = offset == 0 or document[offset - 1] == "\n"
        assert at_line_start, (
            f"{form.name}: чанк начинается с {first!r} посреди строки: {chunk.text[:40]!r}"
        )


@PIPE_TABLE_PARAMS
def test_p9_table_continuation_carries_header(form: Form) -> None:
    """P9: кусок, начинающийся строкой pipe-таблицы, несёт шапку с разделителем.

    Проверяется форма контекста, а не его наличие: шапка обязана содержать столько же
    колонок, сколько строка, иначе «таблица с шапкой» — таблица с чужой шапкой.
    """
    for chunk in _continuation_chunks(form):
        line = _first_line(chunk.text)
        if not _is_table_row(line) or _is_delimiter_row(line):
            continue  # разделитель шапки — не продолжение строки данных
        header = [candidate for candidate in _context_lines(chunk) if _is_table_row(_bare(candidate))]
        assert header, f"{form.name}: у строки {line[:30]!r} нет шапки в контексте"
        assert any(_is_delimiter_row(_bare(candidate)) for candidate in header), (
            f"{form.name}: в шапке нет разделителя"
        )
        source_lines = _source_lines(form.document)
        for candidate in header:
            assert candidate in source_lines, (
                f"{form.name}: строка шапки в контексте не совпадает со строкой документа: "
                f"{candidate!r} — префикс доклеен"
            )
        assert _table_columns(_bare(header[-1])) == _table_columns(line), (
            f"{form.name}: колонок в шапке {_table_columns(_bare(header[-1]))}, "
            f"в строке {_table_columns(line)}"
        )


@PANDOC_TABLE_PARAMS
def test_pandoc_table_continuation_carries_pipe_header(form: Form) -> None:
    """P9 для grid/multiline/simple: шапка синтезирована в pipe-виде и не теряется.

    В источнике шапки нет как pipe-строки, поэтому проверяется **форма** контекста:
    строка шапки с тем же числом колонок, что и разделитель.
    """
    for chunk in _continuation_chunks(form):
        context = _context_lines(chunk)
        rows = [line for line in context if _is_table_row(_bare(line))]
        assert rows, f"{form.name}: продолжение таблицы без pipe-шапки: {chunk.text[:40]!r}"
        assert any(_is_delimiter_row(_bare(line)) for line in rows), (
            f"{form.name}: в pipe-шапке нет разделителя"
        )
        header = [line for line in rows if not _is_delimiter_row(_bare(line))]
        assert header, f"{form.name}: в контексте только разделитель, нет имён колонок"
        assert _table_columns(_bare(header[-1])) == _table_columns(_bare(rows[-1])), (
            f"{form.name}: колонок в шапке {_table_columns(_bare(header[-1]))}, "
            f"в разделителе {_table_columns(_bare(rows[-1]))}"
        )


@FENCE_PARAMS
def test_p10_fence_continuation_carries_info_string(form: Form) -> None:
    """P10: продолжение fence несёт открывающую строку вместе с info string."""
    for chunk in _continuation_chunks(form):
        context = _context_lines(chunk)
        assert context, f"{form.name}: продолжение fence без контекста"
        assert _bare(context[-1]).startswith(("```", "~~~")), (
            f"{form.name}: в контексте нет открывающей строки fence: {context[-1]!r}"
        )
        assert context[-1] in _source_lines(form.document), (
            f"{form.name}: открывающая строка в контексте не совпадает со строкой документа: "
            f"{context[-1]!r} — префикс доклеен"
        )


@LIST_PARAMS
def test_p2_list_continuation_keeps_marker(form: Form) -> None:
    """P2: продолжение пункта списка несёт маркер и отступ в контексте.

    Пункт — минимальная единица: если кусок начинается не с маркера, значит он начался
    внутри пункта, и без маркера в контексте он читается как обычный абзац (а нумерованный
    теряет номер).
    """
    for chunk in _continuation_chunks(form):
        line = _first_line(chunk.text)
        if not line or _ITEM_START.match(line):
            continue  # кусок начинается с маркера пункта — он сам себе контекст
        context = chunk.context_prefix.split("\n")
        assert any(_ITEM_START.match(candidate) for candidate in context), (
            f"{form.name}: продолжение пункта без маркера в контексте: {chunk.text[:40]!r}"
        )


@LINE_UNIT_PARAMS
def test_line_units_are_cut_at_line_boundaries(form: Form) -> None:
    """Рез внутри строки таблицы/кода — разрыв атомарной единицы, а не продолжение."""
    for chunk in _chunks(form):
        if chunk.char_start == 0:
            continue
        assert form.document[chunk.char_start - 1] == "\n", (
            f"{form.name}: кусок начинается посреди строки: {chunk.text[:40]!r}"
        )


@LINE_BLOCK_PARAMS
def test_line_block_continuation_carries_marker(form: Form) -> None:
    """Продолжение line block несёт ``| `` — иначе строка читается как абзац."""
    for chunk in _continuation_chunks(form):
        assert chunk.text.startswith("| "), f"{form.name}: строка без маркера: {chunk.text[:40]!r}"
        assert any(_bare(line) == "|" for line in _context_lines(chunk)), (
            f"{form.name}: контекст без маркера line block"
        )


@RAW_PARAMS
def test_raw_tex_continuation_carries_marker(form: Form) -> None:
    """Продолжение raw TeX несёт ``\\begin{…}``: без него блок перестаёт быть блоком."""
    opening = form.document.split("\n", 1)[0]
    for chunk in _continuation_chunks(form):
        context = _context_lines(chunk)
        assert context and context[-1] == opening, (
            f"{form.name}: продолжение без маркера raw-блока: {context[-1:]!r}"
        )


@CONTAINER_PARAMS
def test_container_continuation_carries_marker(form: Form) -> None:
    """Продолжение цитаты и нативного div несёт маркер контейнера."""
    for chunk in _continuation_chunks(form):
        context = _context_lines(chunk)
        if form.kind == QUOTE:
            assert any(line.lstrip().startswith(">") for line in context), (
                f"{form.name}: продолжение без маркера цитаты"
            )
        else:
            assert context and context[-1] == '<div class="x">', (
                f"{form.name}: продолжение без открывающего тега div: {context[-1:]!r}"
            )


@ALL_PARAMS
def test_context_is_built_from_document_lines(form: Form) -> None:
    """Контекст продолжения не выдумывает строк: либо строка документа, либо маркер.

    Это ловит склейку «префикс + сырая строка»: ``> - >   ```sql`` — не строка документа
    и не маркер, то есть контекст описывает вложенность, которой в тексте нет.
    """
    # Сравнение без префикса контейнера: ``heading_path`` печатает текст заголовка, а в
    # документе строка идёт с ``#``; это тот же текст, а не выдуманная строка.
    source_lines = {_bare(line) for line in _source_lines(form.document)}
    # ``heading_path`` печатает текст заголовка без ``#``: это тот же текст документа.
    source_lines |= {line.lstrip("#").strip() for line in source_lines}
    for chunk in _chunks(form):
        for line in _context_lines(chunk):
            bare = _bare(line)
            if bare in source_lines or _is_marker_only(line) or bare == "|":
                continue
            # Pipe-шапка Pandoc-таблицы синтезирована из ячеек: в источнике её нет.
            if form.kind == PANDOC_TABLE and _is_table_row(bare):
                continue
            pytest.fail(f"{form.name}: строка контекста {line!r} не из документа и не маркер")


@ALL_PARAMS
def test_no_doubled_container_prefix(form: Form) -> None:
    """Контейнерный префикс не доклеивается дважды (``> > | a | b |``)."""
    for chunk in _chunks(form):
        if not chunk.text.startswith(">"):
            assert "> > " not in chunk.context_prefix, f"{form.name}: удвоенный префикс цитаты в контексте"
        assert "> > " not in chunk.text[:80], f"{form.name}: удвоенный префикс цитаты в тексте"


# ---------------------------------------------------------------------------
# Атомарность единицы (план 18 §7)
# ---------------------------------------------------------------------------


@ALL_PARAMS
def test_atomic_construct_fits_in_one_chunk(form: Form) -> None:
    """Бюджет больше конструкции ⇒ первый и последний её символы в одном чанке.

    Документ формы — ровно одна конструкция, поэтому чанк обязан быть один: второй
    означал бы внутреннюю границу, которой у атомарной единицы быть не может.
    """
    chunks = _chunks(form, ATOMIC_BUDGET)
    assert len(chunks) == 1, (
        f"{form.name}: конструкция разрезана при бюджете {ATOMIC_BUDGET}: "
        f"{len(chunks)} чанков"
    )
    start, end = _visible_bounds(form.document)
    chunk = chunks[0]
    assert chunk.char_start <= start and chunk.char_end >= end, (
        f"{form.name}: границы чанка [{chunk.char_start}, {chunk.char_end}) "
        f"не покрывают конструкцию [{start}, {end})"
    )


def test_atomicity_forms_do_not_silently_vanish() -> None:
    """Страж: каждая конструкция §5 имеет форму, вид известен, бюджет её покрывает.

    Тест падает, если форма «исчезла» из проверки атомарности: ``kind`` стал неизвестным,
    конструкция пропала из реестра или её документ перерос ``ATOMIC_BUDGET``. Иначе
    зелёный прогон означал бы «ничего не проверялось».
    """
    assert FORMS, "реестр форм пуст"

    names = [form.name for form in FORMS]
    assert len(names) == len(set(names)), "имена форм дублируются"

    unknown = sorted({form.kind for form in FORMS} - ATOMIC_KINDS)
    assert not unknown, f"вид формы не участвует в проверке атомарности: {unknown}"

    covered = {form.construct for form in FORMS}
    assert covered == REQUIRED_CONSTRUCTS, (
        "реестр форм разошёлся с §5: "
        f"нет {sorted(REQUIRED_CONSTRUCTS - covered)}, лишние {sorted(covered - REQUIRED_CONSTRUCTS)}"
    )

    longest = max(len(form.document) for form in FORMS)
    assert ATOMIC_BUDGET > longest, (
        f"ATOMIC_BUDGET={ATOMIC_BUDGET} не больше самой длинной формы ({longest} символов): "
        "проверка «влезает целиком» перестала быть атомарной"
    )


def test_continuation_kinds_are_classified() -> None:
    """У каждого вида продолжение либо проверяется, либо названо остатком с причиной."""
    unchecked = set(CONTINUATION_UNCHECKED)
    assert CONTINUATION_CHECKED | unchecked == ATOMIC_KINDS, (
        "вид формы не классифицирован: "
        f"{sorted(ATOMIC_KINDS - CONTINUATION_CHECKED - unchecked)}"
    )
    assert not (CONTINUATION_CHECKED & unchecked), "вид одновременно проверяется и назван остатком"
    for kind, reason in CONTINUATION_UNCHECKED.items():
        assert reason.strip(), f"{kind}: остаток без причины"