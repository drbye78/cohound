"""Мутационная приёмка нарезки по конструкциям (план 18 §7, P1/P2/P8–P10).

Зачем отдельный модуль, если есть ``test_chunker_markdown.py``. Тот файл пинит
**конкретные** случаи: один список, одна таблица, одна цитата. Здесь проверяется
**свойство**, которое обязано держаться при любой форме конструкции: чанкер не имеет
права терять текст (P1), начинать кусок закрывающей пунктуацией (P8), отрывать шапку
таблицы от её строк (P9), отрывать info string от тела fence (P10) и оставлять
продолжение пункта списка без маркера (P2).

Мутация — это перебор форм одной конструкции: маркер списка, начало нумерации, глубина
вложенности, число колонок и выравнивание таблицы, info string и вид забора, уровень
заголовка, обёртка цитатой. Каждая форма — документ, на котором проверяются все
инварианты сразу; поэтому отчёт читается как «на скольких формах держится», а не «этот
пример работает».

Инварианты считаются по **тексту и его геометрии**, без повторного разбора в AST:
проверка не должна опираться на тот же разбор, который использует чанкер, иначе она
подтверждает его ошибку вместо того, чтобы её поймать.

Нейтрален (§1.3).
"""

from __future__ import annotations

import itertools
import re
from collections.abc import Iterator

import pytest

from core.adapters.chunker_markdown import MarkdownChunker
from core.util.text_utils import CLOSING_CHARS

#: Бюджет куска намеренно мал: конструкция гарантированно режется, и «продолжение»
#: проверяется, а не остаётся недостижимым.
CHUNK_SIZE = 220

#: Сколько строк внутрь конструкции, чтобы она переросла бюджет.
FILLER_ROWS = 24

ROW_TEXT = "строка {n} с достаточно длинным содержимым для переполнения бюджета"

#: Вид формы — чтобы проверка знала, какого контекста требовать от продолжения.
LIST, TABLE, FENCE, PLAIN = "list", "table", "fence", "plain"

#: Маркер пункта списка в начале строки (включая отметку задачи).
_ITEM_START = re.compile(r"^[ \t]*(?:[-+*]|\d{1,9}[.)])[ \t]")


def _filler() -> list[str]:
    return [ROW_TEXT.format(n=n) for n in range(FILLER_ROWS)]


# ---------------------------------------------------------------------------
# Генераторы форм: (имя, вид, документ)
# ---------------------------------------------------------------------------


def _list_forms() -> Iterator[tuple[str, str, str]]:
    """Списки: маркер, начало нумерации, вложенность, отметка задачи."""
    body = _filler()
    for marker in ("-", "*", "+"):
        yield f"список {marker!r}", LIST, "\n".join(f"{marker} {line}" for line in body) + "\n"
    for start in (1, 3, 10):
        yield (
            f"нумерованный с {start}",
            LIST,
            "\n".join(f"{start + i}. {line}" for i, line in enumerate(body)) + "\n",
        )
    yield (
        "вложенный",
        LIST,
        "\n".join(f"- внешний {n}\n    - {line}" for n, line in enumerate(body)) + "\n",
    )
    for mark in ("[ ]", "[x]"):
        yield f"task list {mark}", LIST, "\n".join(f"- {mark} {line}" for line in body) + "\n"


def _table_forms() -> Iterator[tuple[str, str, str]]:
    """Таблицы: число колонок и выравнивание (P9 — шапка с разделителем)."""
    for columns, align in itertools.product((2, 3), ("---", ":--", "--:", ":-:")):
        header = "| " + " | ".join(f"колонка {i}" for i in range(columns)) + " |"
        separator = "|" + "|".join(align for _ in range(columns)) + "|"
        rows = [
            "| " + " | ".join(f"значение {n}-{i}" for i in range(columns)) + " |"
            for n in range(FILLER_ROWS)
        ]
        yield f"таблица {columns}×{align}", TABLE, "\n".join([header, separator, *rows]) + "\n"


def _fence_forms() -> Iterator[tuple[str, str, str]]:
    """Fence: info string, вид забора, тильды против обратных кавычек (P10)."""
    for info, fence in itertools.product(("sql", "python title=app.py", ""), ("```", "~~~~")):
        body = [f"SELECT {n} FROM таблица;" for n in range(FILLER_ROWS)]
        yield (
            f"fence {fence!r} info={info!r}",
            FENCE,
            "\n".join([f"{fence}{info}", *body, fence]) + "\n",
        )


def _plain_forms() -> Iterator[tuple[str, str, str]]:
    """Цитаты и заголовки: обёртка цитатой, уровни заголовков, абзацы."""
    yield "цитата", PLAIN, "".join(f"> {line}\n>\n" for line in _filler())
    yield ("абзацы", PLAIN, "\n\n".join(_filler()) + "\n")
    for level in (1, 2, 3):
        heading = "#" * level
        yield f"заголовок {level} уровня", PLAIN, f"{heading} Раздел\n\n" + "\n\n".join(_filler()) + "\n"


def _nested_forms() -> Iterator[tuple[str, str, str]]:
    """Конструкция внутри цитаты: контейнерный префикс не должен двоиться."""
    yield (
        "таблица в цитате",
        TABLE,
        "> | a | b |\n> |---|---|\n"
        + "".join(f"> | {n} | значение {n} |\n" for n in range(FILLER_ROWS)),
    )
    yield (
        "fence в цитате",
        FENCE,
        "> ```sql\n" + "".join(f"> SELECT {n};\n" for n in range(FILLER_ROWS)) + "> ```\n",
    )
    yield (
        "список в цитате",
        LIST,
        "".join(f"> - {line}\n" for line in _filler()),
    )
    # Вложенность «пункт внутри цитаты» — тот случай, где контейнерный префикс и маркер
    # пункта встречаются: сырая строка уже несёт `> `, а маркер `> - ` добавляется отдельно.
    yield (
        "таблица в пункте в цитате",
        TABLE,
        "# T\n\n> - пункт:\n>\n>   | a | b |\n>   |---|---|\n"
        + "".join(f">   | {n} | значение {n} |\n" for n in range(FILLER_ROWS)),
    )
    yield (
        "fence в пункте в цитате",
        FENCE,
        "# F\n\n> - пункт:\n>\n>   ```sql\n"
        + "".join(f">   SELECT {n};\n" for n in range(FILLER_ROWS))
        + ">   ```\n",
    )
    yield (
        "fence в пункте",
        FENCE,
        "- пункт\n\n  ```sql\n"
        + "".join(f"  SELECT {n};\n" for n in range(FILLER_ROWS))
        + "  ```\n",
    )


FORMS: list[tuple[str, str, str]] = [
    *_list_forms(),
    *_table_forms(),
    *_fence_forms(),
    *_plain_forms(),
    *_nested_forms(),
]

IDS = [name for name, _, _ in FORMS]
PARAMS = pytest.mark.parametrize(("name", "kind", "document"), FORMS, ids=IDS)


def _chunks(document: str):
    return MarkdownChunker(chunk_size=CHUNK_SIZE).chunk("doc", document)


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
_MARKER_ONLY = re.compile(r"^(?:[ \t]*>[ \t]?)*(?:[-+*]|\d{1,9}[.)])?[ \t]*$")


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


# ---------------------------------------------------------------------------
# Инварианты
# ---------------------------------------------------------------------------


@PARAMS
def test_p1_text_is_an_exact_concatenation(name: str, kind: str, document: str) -> None:
    """P1: чанки — точные срезы, их конкатенация возвращает вход."""
    chunks = _chunks(document)

    assert chunks, f"{name}: документ не дал ни одного чанка"
    assert "".join(chunk.text for chunk in chunks) == document


@PARAMS
def test_p8_no_chunk_starts_with_closing_punctuation(name: str, kind: str, document: str) -> None:
    """P8: кусок не открывается закрывающей пунктуацией.

    В этих формах ни один блок источника не начинается со знака, поэтому любой такой
    кусок — след реза, а не свойство документа.
    """
    for chunk in _chunks(document):
        first = _first_line(chunk.text)[:1]
        assert first not in CLOSING_CHARS, f"{name}: чанк начинается с {first!r}: {chunk.text[:40]!r}"


@PARAMS
def test_p9_table_continuation_carries_header(name: str, kind: str, document: str) -> None:
    """P9: кусок, начинающийся строкой таблицы, несёт шапку с разделителем.

    Проверяется форма контекста, а не его наличие: шапка обязана содержать столько же
    колонок, сколько строка, иначе «таблица с шапкой» — таблица с чужой шапкой.
    """
    if kind != TABLE:
        pytest.skip("форма без таблицы")
    for chunk in _chunks(document)[1:]:
        line = _first_line(chunk.text)
        if not _is_table_row(line) or _is_delimiter_row(line):
            continue  # разделитель шапки — не продолжение строки данных
        header = [candidate for candidate in _context_lines(chunk) if _is_table_row(_bare(candidate))]
        assert header, f"{name}: у строки {line[:30]!r} нет шапки в контексте"
        assert any(_is_delimiter_row(_bare(candidate)) for candidate in header), (
            f"{name}: в шапке нет разделителя"
        )
        source_lines = _source_lines(document)
        for candidate in header:
            assert candidate in source_lines, (
                f"{name}: строка шапки в контексте не совпадает со строкой документа: "
                f"{candidate!r} — префикс доклеен"
            )
        assert _table_columns(_bare(header[-1])) == _table_columns(line), (
            f"{name}: колонок в шапке {_table_columns(_bare(header[-1]))}, "
            f"в строке {_table_columns(line)}"
        )


@PARAMS
def test_p10_fence_continuation_carries_info_string(name: str, kind: str, document: str) -> None:
    """P10: продолжение fence несёт открывающую строку вместе с info string."""
    if kind != FENCE:
        pytest.skip("форма без fence")
    for chunk in _chunks(document)[1:]:
        line = _first_line(chunk.text)
        if not line.startswith(("```", "~~~")):
            continue
        context = _context_lines(chunk)
        assert context, f"{name}: продолжение fence без контекста"
        assert _bare(context[-1]).startswith(("```", "~~~")), (
            f"{name}: в контексте нет открывающей строки fence: {context[-1]!r}"
        )
        assert context[-1] in _source_lines(document), (
            f"{name}: открывающая строка в контексте не совпадает со строкой документа: "
            f"{context[-1]!r} — префикс доклеен"
        )


@PARAMS
def test_p2_list_continuation_keeps_marker(name: str, kind: str, document: str) -> None:
    """P2: продолжение пункта списка несёт маркер и отступ в контексте.

    Пункт — минимальная единица: если кусок начинается не с маркера, значит он начался
    внутри пункта, и без маркера в контексте он читается как обычный абзац (а нумерованный
    теряет номер).
    """
    if kind != LIST:
        pytest.skip("форма без списка")
    for chunk in _chunks(document)[1:]:
        line = _first_line(chunk.text)
        if not line or _ITEM_START.match(line):
            continue  # кусок начинается с маркера пункта — он сам себе контекст
        context = chunk.context_prefix.split("\n")
        assert any(_ITEM_START.match(candidate) for candidate in context), (
            f"{name}: продолжение пункта без маркера в контексте: {chunk.text[:40]!r}"
        )


@PARAMS
def test_context_is_built_from_document_lines(name: str, kind: str, document: str) -> None:
    """Контекст продолжения не выдумывает строк: либо строка документа, либо маркер.

    Это ловит склейку «префикс + сырая строка»: ``> - >   ```sql`` — не строка документа
    и не маркер, то есть контекст описывает вложенность, которой в тексте нет.
    """
    # Сравнение без префикса контейнера: ``heading_path`` печатает текст заголовка, а в
    # документе строка идёт с ``#``; это тот же текст, а не выдуманная строка.
    source_lines = {_bare(line) for line in _source_lines(document)}
    # ``heading_path`` печатает текст заголовка без ``#``: это тот же текст документа.
    source_lines |= {line.lstrip("#").strip() for line in source_lines}
    for chunk in _chunks(document):
        for line in _context_lines(chunk):
            assert _bare(line) in source_lines or _is_marker_only(line), (
                f"{name}: строка контекста {line!r} не из документа и не маркер"
            )


@PARAMS
def test_no_doubled_container_prefix(name: str, kind: str, document: str) -> None:
    """Контейнерный префикс не доклеивается дважды (``> > | a | b |``)."""
    for chunk in _chunks(document):
        if not chunk.text.startswith(">"):
            assert "> > " not in chunk.context_prefix, f"{name}: удвоенный префикс цитаты в контексте"
        assert "> > " not in chunk.text[:80], f"{name}: удвоенный префикс цитаты в тексте"