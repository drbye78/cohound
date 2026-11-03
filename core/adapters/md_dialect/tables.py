"""Таблицы Pandoc: grid, multiline, simple и подпись таблицы (вторая половина L3).

**Измеренное поведение оракула** (``pandoc -f markdown -t json``, 2.17.1.1). Pandoc
разбирает три вида таблиц, которых нет ни в CommonMark, ни в GFM:

* **grid** — рамка ``+---+---+``; строка шапки отделяется линией ``+===+===+``; ячейка
  может занимать несколько физических строк; выравнивание задаётся двоеточиями **в
  разделителе шапки** (``+:==+==:+``), а не в верхней рамке — измерено: двоеточия верхней
  рамки оракул игнорирует;
* **multiline** — верхняя линия дефисов, строки шапки (любое число), разделитель из групп
  дефисов, строки тела через пустую строку, нижняя линия дефисов;
* **simple** — ровно одна строка шапки, разделитель из групп дефисов, по строке на ряд;
  таблица кончается первой пустой строкой.

**Что здесь важнее типа блока.** Нарезка (``chunker_markdown``) режет таблицу **только
между строками** (§5 плана), поэтому правило обязано отдать не только вид ``table``, но и
реальные спаны строк: ``table_row`` с флагом ``header``. Шапка и выравнивание уходят в
``info["header_pipe"]`` в **pipe-виде** — это представление контекста продолжения, а не
текст куска: сам текст остаётся точным срезом источника (P1/P4), а ``context_prefix``
показывает читателю модели имена столбцов.

**Подпись таблицы.** ``Table: текст`` перед таблицей (обязательна пустая строка между
подписью и таблицей — измерено: без неё оракул отдаёт один ``Para``) и ``Table:``/``:``
после неё (пустая строка не обязательна). Подпись **входит в спан таблицы**, иначе она
осталась бы отдельным блоком верхнего уровня, а оракул её блоком не считает; чанкер
поэтому физически не может оторвать подпись от строк.

**Текст не переписывается.** Разбор идёт по исходным строкам, спаны — отрезки этих строк.
Разрывов покрытия правило не создаёт: спан таблицы покрывает рамки, разделители, пустые
строки тела и подпись, а строки-дети лежат внутри.

**Границы (объявлены).** Распознаётся только верхний уровень документа: таблица внутри
цитаты/div не переразбирается (вложенность в ``blockquote`` обрабатывается другими
правилами). Одноколоночные dash-таблицы не распознаются: их разделитель неотличим от
тематического разрыва, а ``pandoc`` для них отдельного синтаксиса не даёт. Ячейка
разбирается как текст: инлайновая структура (акценты, ссылки) внутри неё не проверяется.
Нейтрален (§1.3): грамматика markdown — не предметная область.
"""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass, field, replace
from typing import Final

from core.adapters.md_ast import LineIndex, MdBlock, MdSpan, parse_markdown

__all__ = ["attach_captions", "recognize_tables", "trim_pipe_tables"]

#: Разделитель шапки grid-таблицы: ``+===+===+`` (двоеточия — выравнивание).
_GRID_TOP_RE: Final = re.compile(r"^ {0,3}\+(?:[-=:]+\+)+\s*$")
#: Группа дефисов/двоеточий — колонка dash-таблицы.
_DASH_GROUP_RE: Final = re.compile(r"[-:]+")
#: Разделитель dash-таблицы: две и более групп, разделённых пробелами.
_DASH_SEPARATOR_RE: Final = re.compile(r"^ {0,3}[-:]+(?: +[-:]+)+ *$")
#: Верхняя/нижняя линия multiline-таблицы: один непрерывный прогон дефисов.
_DASH_BORDER_RE: Final = re.compile(r"^ {0,3}[-:]+ *$")
#: Подпись таблицы: ``Table: …`` или ``: …`` в начале абзаца.
_CAPTION_RE: Final = re.compile(r"^(?:Table:|:)[ \t]")


@dataclass(frozen=True, slots=True)
class _Row:
    """Строка таблицы: диапазон физических строк и её ячейки."""

    start_line: int
    end_line: int
    header: bool
    cells: tuple[str, ...]


@dataclass(slots=True)
class _Match:
    """Распознанная таблица: спан, строки, шапка и выравнивание."""

    start_line: int
    end_line: int
    dialect: str
    rows: list[_Row] = field(default_factory=list)
    header_rows: int = 0
    align: tuple[str, ...] = ()
    columns: int = 0


def _line(index: LineIndex, number: int) -> str:
    return index.line_text(number).rstrip("\n")


def _blank(index: LineIndex, number: int) -> bool:
    return not _line(index, number).strip()


def _alignment(group: str) -> str:
    """Выравнивание колонки по двоеточиям группы дефисов (``left``/``right``/``center``)."""
    left = group.startswith(":")
    right = group.endswith(":")
    if left and right:
        return "center"
    if left:
        return "left"
    if right:
        return "right"
    return ""


def _align_token(align: str) -> str:
    return {"left": ":---", "right": "---:", "center": ":---:"}.get(align, "---")


def _pipe_text(cell: str) -> str:
    """Текст ячейки для pipe-шапки: трубы внутри экранируются, переводы строк — пробел."""
    return " ".join(cell.split()).replace("|", "\\|")


def _header_pipe(header: tuple[tuple[str, ...], ...], align: tuple[str, ...]) -> str:
    """Синтезирует pipe-шапку из разобранной шапки.

    Это **представление контекста**, а не текст куска: ``chunk.text`` остаётся точным
    срезом источника (P1/P4), а ``context_prefix`` получает имена столбцов и
    выравнивание в том виде, в котором их читает человек и модель.
    """
    if not align:
        return ""
    separator = "|" + "|".join(_align_token(item) for item in align) + "|"
    if not header:
        return separator
    lines = []
    for row in header:
        if len(row) < len(align):
            row = row + ("",) * (len(align) - len(row))
        lines.append("| " + " | ".join(_pipe_text(item) for item in row[: len(align)]) + " |")
    return "\n".join([*lines, separator])


# ---------------------------------------------------------------------------
# Grid table
# ---------------------------------------------------------------------------


def _grid_plus(text: str) -> tuple[int, ...] | None:
    """Позиции ``+`` в рамке; рамка обязана состоять только из ``+ - = :``."""
    stripped = text.rstrip()
    if _GRID_TOP_RE.match(stripped) is None:
        return None
    plus = tuple(position for position, char in enumerate(stripped) if char == "+")
    if len(plus) < 2:
        return None
    for char in stripped:
        if char not in "+-=: ":
            return None
    return plus


def _grid_border(text: str, plus: tuple[int, ...]) -> bool:
    """Рамка grid-таблицы с теми же позициями ``+`` (и допусками ``=``/``:``)."""
    if len(text.rstrip()) <= plus[-1]:
        return False
    for position in plus:
        if text[position] != "+":
            return False
    for start, end in itertools.pairwise(plus):
        if set(text[start + 1 : end]) - set("-=:"):
            return False
    return True


def _grid_cells(group: list[int], plus: tuple[int, ...], index: LineIndex) -> tuple[str, ...]:
    """Ячейки строки: физические строки склеиваются с сохранением текста."""
    cells = []
    for start, end in itertools.pairwise(plus):
        parts = []
        for line in group:
            piece = _line(index, line)[start + 1 : end].strip()
            if piece:
                parts.append(piece)
        cells.append(" ".join(parts))
    return tuple(cells)


def _grid_match(index: LineIndex, start: int) -> _Match | None:
    plus = _grid_plus(_line(index, start))
    if plus is None:
        return None
    groups: list[list[int]] = []
    current: list[int] = []
    header_sep: str | None = None
    header_sep_line = -1
    number = start + 1
    while number < index.line_count:
        text = _line(index, number)
        if not text.strip():
            if current:
                groups.append(current)
                current = []
            break
        if _grid_border(text, plus):
            if current:
                groups.append(current)
                current = []
            if "=" in text and header_sep is None:
                # Разделитель ``+===+``: всё выше него — строки шапки.
                header_sep = text
                header_sep_line = number
            number += 1
            continue
        # Содержимая строка обязана стоять на тех же колонках: иначе таблица кончилась.
        if len(text) <= plus[-1] or any(text[position] != "|" for position in plus):
            break
        current.append(number)
        number += 1
    if current:
        groups.append(current)
    if not groups:
        return None
    if header_sep is None:
        header_groups: list[list[int]] = []
        body_groups = groups
    else:
        header_groups = [group for group in groups if group[-1] < header_sep_line]
        body_groups = [group for group in groups if group[0] > header_sep_line]
    if not body_groups and not header_groups:
        return None
    align = _grid_align(header_sep, plus)
    rows = [
        _Row(group[0], group[-1] + 1, True, _grid_cells(group, plus, index))
        for group in header_groups
    ] + [
        _Row(group[0], group[-1] + 1, False, _grid_cells(group, plus, index))
        for group in body_groups
    ]
    return _Match(
        start_line=start,
        end_line=number,
        dialect="grid",
        rows=rows,
        header_rows=len(header_groups),
        align=align,
        columns=len(plus) - 1,
    )


def _grid_align(text: str | None, plus: tuple[int, ...]) -> tuple[str, ...]:
    if text is None:
        return tuple("" for _ in plus[1:])
    return tuple(
        _alignment(text[start + 1 : end]) for start, end in itertools.pairwise(plus)
    )


# ---------------------------------------------------------------------------
# Multiline / simple table
# ---------------------------------------------------------------------------


def _dash_groups(text: str) -> list[tuple[int, int]]:
    """Группы дефисов/двоеточий разделителя как диапазоны колонок."""
    return [(match.start(), match.end()) for match in _DASH_GROUP_RE.finditer(text)]


def _dash_cells(
    group: list[int], columns: list[tuple[int, int]], index: LineIndex
) -> tuple[str, ...]:
    cells = []
    for start, end in columns:
        parts = []
        for line in group:
            piece = _line(index, line)[start:end].strip()
            if piece:
                parts.append(piece)
        cells.append(" ".join(parts))
    return tuple(cells)


def _simple_match(index: LineIndex, start: int) -> _Match | None:
    """Simple table: одна строка шапки, разделитель, по строке на ряд."""
    if start + 1 >= index.line_count:
        return None
    header_text = _line(index, start)
    stripped = header_text.lstrip()
    if not stripped or stripped[0] in "-=+|":
        # Строка из дефисов — тематический разрыв, а не шапка. Без этой проверки
        # вырожденный случай (код, в котором ``markdown-it`` не увидел забор) даёт
        # ложную simple-таблицу: измерено на корпусе — блок SQL-сессии.
        return None
    if _DASH_BORDER_RE.match(header_text):
        return None
    separator = _line(index, start + 1)
    if _DASH_SEPARATOR_RE.match(separator) is None:
        return None
    columns = _dash_groups(separator)
    if len(columns) < 2:
        return None
    body: list[int] = []
    number = start + 2
    while number < index.line_count:
        text = _line(index, number)
        if not text.strip():
            break
        body.append(number)
        number += 1
    if not body:
        return None
    align = tuple(_alignment(separator[start_col:end_col]) for start_col, end_col in columns)
    header = (_Row(start, start + 1, True, _dash_cells([start], columns, index)),)
    rows = list(header) + [
        _Row(line, line + 1, False, _dash_cells([line], columns, index)) for line in body
    ]
    return _Match(
        start_line=start,
        end_line=number,
        dialect="simple",
        rows=rows,
        header_rows=1,
        align=align,
        columns=len(columns),
    )


def _multiline_match(index: LineIndex, start: int) -> _Match | None:
    """Multiline table: верхняя линия, шапка (1+ строк), разделитель, тело, нижняя линия."""
    top = _line(index, start)
    if _DASH_BORDER_RE.match(top) is None:
        return None
    # Ищем разделитель из групп дефисов: между верхней линией и ним — строки шапки.
    separator_line: int | None = None
    number = start + 1
    while number < index.line_count:
        text = _line(index, number)
        if not text.strip():
            return None
        if _DASH_SEPARATOR_RE.match(text) is not None:
            separator_line = number
            break
        if number - start > 40:
            return None
        number += 1
    if separator_line is None or separator_line == start + 1:
        return None
    separator = _line(index, separator_line)
    columns = _dash_groups(separator)
    if len(columns) < 2:
        return None
    header_lines = list(range(start + 1, separator_line))
    body: list[list[int]] = []
    current: list[int] = []
    number = separator_line + 1
    bottom: int | None = None
    while number < index.line_count:
        text = _line(index, number)
        if not text.strip():
            if current:
                body.append(current)
                current = []
            number += 1
            continue
        if _DASH_BORDER_RE.match(text) is not None:
            if current:
                body.append(current)
                current = []
            bottom = number
            number += 1
            break
        current.append(number)
        number += 1
    if current:
        body.append(current)
    if bottom is None or not body:
        return None
    align = tuple(_alignment(separator[start_col:end_col]) for start_col, end_col in columns)
    header = _Row(
        start + 1,
        separator_line,
        True,
        _dash_cells(header_lines, columns, index),
    )
    rows = [header] + [
        _Row(group[0], group[-1] + 1, False, _dash_cells(group, columns, index))
        for group in body
    ]
    return _Match(
        start_line=start,
        end_line=number,
        dialect="multiline",
        rows=rows,
        header_rows=1,
        align=align,
        columns=len(columns),
    )


# ---------------------------------------------------------------------------
# Подпись таблицы
# ---------------------------------------------------------------------------


def _caption_before(block: MdBlock, start: int, index: LineIndex) -> bool:
    """Подпись перед таблицей: абзац с ``Table:``/``:`` и пустая строка после него."""
    if block.kind != "paragraph" or block.span.end_line >= start:
        return False
    if not _CAPTION_RE.match(index.slice(block.span).lstrip()):
        return False
    return all(_blank(index, line) for line in range(block.span.end_line, start))


def _caption_after(block: MdBlock, end: int, index: LineIndex) -> bool:
    """Подпись после таблицы: абзац с ``Table:``/``:`` (пустая строка не обязательна)."""
    if block.kind != "paragraph" or block.span.start_line < end:
        return False
    if not _CAPTION_RE.match(index.slice(block.span).lstrip()):
        return False
    return all(_blank(index, line) for line in range(end, block.span.start_line))


def _caption_info(text: str) -> tuple[str, str]:
    """``(подпись без маркера, исходный текст подписи)`` для метаданных и контекста."""
    raw = text.strip()
    return _CAPTION_RE.sub("", raw, count=1).strip(), raw


def attach_captions(blocks: list[MdBlock], index: LineIndex) -> list[MdBlock]:
    """Прикрепляет ``Table:``/``:`` к таблице: расширяет спан и пишет метаданные.

    Подпись не остаётся отдельным блоком верхнего уровня (оракул её блоком не считает),
    поэтому её строки входят в спан таблицы. Это же гарантирует, что чанкер не оторвёт
    подпись от строк: разрез ставится только между строками тела.
    """
    result: list[MdBlock] = []
    position = 0
    while position < len(blocks):
        block = blocks[position]
        if block.kind != "table":
            result.append(block)
            position += 1
            continue
        start = block.span.start_line
        info = dict(block.info)
        info.setdefault("header_start_line", start)
        if result and _caption_before(result[-1], start, index):
            caption = result.pop()
            caption_text = index.slice(caption.span)
            info["caption"], info["caption_text"] = _caption_info(caption_text)
            start = caption.span.start_line
        end = block.span.end_line
        if position + 1 < len(blocks) and _caption_after(
            blocks[position + 1], end, index
        ):
            caption = blocks[position + 1]
            caption_text = index.slice(caption.span)
            info["caption"], info["caption_text"] = _caption_info(caption_text)
            end = caption.span.end_line
            position += 1
        result.append(
            replace(block, span=index.span(start, end), info=info)
        )
        position += 1
    return result


# ---------------------------------------------------------------------------
# Публичное правило
# ---------------------------------------------------------------------------


def _match_at(index: LineIndex, start: int) -> _Match | None:
    for matcher in (_grid_match, _multiline_match, _simple_match):
        match = matcher(index, start)
        if match is not None:
            return match
    return None


def _table_block(match: _Match, index: LineIndex) -> MdBlock:
    header = tuple(row.cells for row in match.rows if row.header)
    rows = tuple(
        MdBlock(
            kind="table_row",
            span=index.span(row.start_line, row.end_line),
            info={"header": row.header, "cells": row.cells},
        )
        for row in match.rows
    )
    return MdBlock(
        kind="table",
        span=index.span(match.start_line, match.end_line),
        info={
            "dialect": match.dialect,
            "header_rows": match.header_rows,
            "align": list(match.align),
            "columns": match.columns,
            "header_pipe": _header_pipe(header, match.align),
        },
        children=rows,
    )


def recognize_tables(blocks: list[MdBlock], index: LineIndex, *, quoted: bool = False) -> list[MdBlock]:
    """Заменяет блоки, покрывающие grid/multiline/simple таблицу, одним ``table``.

    Правило работает по **исходным строкам**: ``markdown-it`` дробит такие таблицы на
    произвольные блоки (тематический разрыв, абзацы, заголовок), и опираться на его
    границы нельзя. Все блоки, чьи строки целиком лежат внутри распознанного диапазона,
    заменяются одним блоком; текст не двигается, поэтому P1/P4/P13 сохраняются.
    """
    if quoted:
        return blocks
    result: list[MdBlock] = []
    position = 0
    while position < len(blocks):
        block = blocks[position]
        match = _match_at(index, block.span.start_line)
        if match is None:
            result.append(block)
            position += 1
            continue
        end = match.end_line
        following = position
        tail: list[MdBlock] = []
        while following < len(blocks) and blocks[following].span.start_line < end:
            block_after = blocks[following]
            if block_after.span.end_line > end:
                # Блок, начавшийся внутри таблицы (например, definition list, который
                # ``markdown-it`` слепил из строки тела и подписи ``: …``), продолжается
                # за её концом. Хвост переразбирается, чтобы не потерять текст.
                tail_start = index.offset(end)
                remainder = index.text[tail_start : block_after.span.end_char]
                document = parse_markdown(remainder, "pandoc")
                tail.extend(
                    _shift(child, tail_start, end) for child in document.blocks
                )
            following += 1
        result.append(_table_block(match, index))
        result.extend(tail)
        position = max(following, position + 1)
    return result

# ---------------------------------------------------------------------------
# Pipe-таблица: граница Pandoc (ячейка GFM не переносится на следующую строку)
# ---------------------------------------------------------------------------


def _clamp(block: MdBlock, cut: int, index: LineIndex) -> MdBlock | None:
    """Обрезает спан блока по строке ``cut``; блоки после неё выбрасываются."""
    if block.span.start_line >= cut:
        return None
    children = tuple(
        clamped
        for child in block.children
        if (clamped := _clamp(child, cut, index)) is not None
    )
    end = min(block.span.end_line, cut)
    return replace(block, span=index.span(block.span.start_line, end), children=children)


def _shift(block: MdBlock, char_base: int, line_base: int) -> MdBlock:
    span = MdSpan(
        start_line=block.span.start_line + line_base,
        end_line=block.span.end_line + line_base,
        start_char=block.span.start_char + char_base,
        end_char=block.span.end_char + char_base,
    )
    return replace(
        block,
        span=span,
        children=tuple(_shift(child, char_base, line_base) for child in block.children),
    )


def trim_pipe_tables(
    blocks: list[MdBlock], index: LineIndex, *, quoted: bool = False
) -> list[MdBlock]:
    """Обрезает pipe-таблицу там, где Pandoc её заканчивает.

    GFM разрешает продолжать ячейку на следующей строке; Pandoc — нет: таблица
    кончается **перед** первой строкой без ``|``, а остаток строк разбирается как
    обычный текст (измерено: ``| 1 | 2 |`` + строка без ``|`` + ``| 3 | 4 |`` →
    ``Table`` + один ``Para``). Без этого правила на дереве репозитория остаётся ровно
    одно расхождение — многострочная ячейка в отчёте замера.

    Остаток переразбирается тем же профилем и сдвигается в координаты документа, чтобы
    покрытие (P13) не зависело от догадки о его содержимом.
    """
    if quoted:
        return blocks
    result: list[MdBlock] = []
    for block in blocks:
        if block.kind != "table" or block.info.get("dialect"):
            result.append(block)
            continue
        cut = next(
            (
                line
                for line in range(block.span.start_line, block.span.end_line)
                if "|" not in _line(index, line)
            ),
            None,
        )
        if cut is None:
            result.append(block)
            continue
        clamped = _clamp(block, cut, index)
        if clamped is not None:
            result.append(clamped)
        remainder = index.text[index.offset(cut) : block.span.end_char]
        document = parse_markdown(remainder, "pandoc")
        result.extend(_shift(child, index.offset(cut), cut) for child in document.blocks)
    return result
