"""Нативный HTML ``Div`` в профиле ``pandoc``: контейнер с разобранным содержимым.

**Измеренное поведение оракула** (``pandoc -f markdown -t json``, 2.17.1.1)::

    <div class="x">

    hi

    </div>

— один ``Div`` с абзацем ``hi`` внутри, а не три блока верхнего уровня. ``markdown-it``
же отдаёт ``html`` (открывающий тег), ``paragraph`` (содержимое) и ``html`` (закрывающий
тег), то есть та же разметка выглядит как лишние блоки в последовательности.

**Модель.** ``MdBlock(kind="container")`` со спаном на **весь** элемент (открывающий тег
до закрывающего включительно) и детьми, разобранными из **внутреннего среза** —
текста между ``>`` открывающего тега и ``</div>``. Смещения детей сдвигаются на начало
среза: ``char_start``/``char_end`` остаются координатами исходного документа, ``text`` —
точным срезом (P1/P4). Внутренний разбор идёт тем же профилем ``pandoc``, поэтому
вложенные конструкции (абзацы, списки, таблицы) получают свои правила.

**Если внутренний разбор даёт разрывы** (``gaps``), контейнер не строится: блоки
остаются ``raw``-представлением (``html`` + содержимое + ``html``), то есть видимым
расхождением, а не тихой потерей текста. Это условие проверяется по ``MdDocument.gaps``
внутреннего разбора.

**Границы (объявлены).** Вложенность глубже одного уровня не требуется и не обещается:
закрывающий тег ищется как первый ``</div>``, поэтому вложенный ``div`` закроет внешний
раньше времени. Атрибуты тега не интерпретируются и в структуру не входят. Хвост после
закрывающего тега на той же строке остаётся в спане контейнера, но отдельным блоком не
выделяется. Контейнер внутри цитаты не обрабатывается (``quoted=True``) — та же
объявленная граница, что у таблиц и raw TeX.

Нейтрален (§1.3): грамматика markdown — не предметная область.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Final

from core.adapters.md_ast import LineIndex, MdBlock, MdSpan, parse_markdown

__all__ = ["split_html_divs"]

_OPEN_RE: Final = re.compile(r"<div\b[^>]*>", re.IGNORECASE)
_CLOSE: Final = "</div>"


def _shift_span(span: MdSpan, char_base: int, line_base: int) -> MdSpan:
    """Сдвиг спана внутреннего разбора в координаты документа."""
    return MdSpan(
        start_line=span.start_line + line_base,
        end_line=span.end_line + line_base,
        start_char=span.start_char + char_base,
        end_char=span.end_char + char_base,
    )


def _shift(block: MdBlock, char_base: int, line_base: int) -> MdBlock:
    return replace(
        block,
        span=_shift_span(block.span, char_base, line_base),
        children=tuple(_shift(child, char_base, line_base) for child in block.children),
    )


def _opening(text: str) -> re.Match[str] | None:
    """Открывающий ``<div …>``, если блок начинается с него (допускается отступ)."""
    stripped = text.lstrip(" \t")
    offset = len(text) - len(stripped)
    match = _OPEN_RE.match(stripped)
    if match is None:
        return None
    return _OPEN_RE.match(text, offset)


def _closing_from(blocks: list[MdBlock], start: int, index: LineIndex) -> int | None:
    """Индекс блока, в котором лежит ``</div>``, начиная с ``start``."""
    for position in range(start, len(blocks)):
        block = blocks[position]
        if block.kind == "html" and _CLOSE in index.slice(block.span).lower():
            return position
    return None


def split_html_divs(blocks: list[MdBlock], index: LineIndex, *, quoted: bool = False) -> list[MdBlock]:
    """Заменяет последовательность блоков вокруг ``<div>…</div>`` одним контейнером."""
    if quoted:
        return blocks
    result: list[MdBlock] = []
    position = 0
    while position < len(blocks):
        block = blocks[position]
        if block.kind != "html":
            result.append(block)
            position += 1
            continue
        piece = index.slice(block.span)
        opening = _opening(piece)
        if opening is None:
            result.append(block)
            position += 1
            continue
        closing_block = _closing_from(blocks, position, index)
        if closing_block is None:
            result.append(block)
            position += 1
            continue
        closing_piece = index.slice(blocks[closing_block].span)
        closed_at = closing_piece.lower().find(_CLOSE)
        if closed_at < 0:  # pragma: no cover - _closing_from уже проверил
            result.append(block)
            position += 1
            continue
        inner_start = block.span.start_char + opening.end()
        inner_end = blocks[closing_block].span.start_char + closed_at
        if inner_end < inner_start:  # pragma: no cover - защита от неверной пары
            result.append(block)
            position += 1
            continue
        inner = index.text[inner_start:inner_end]
        document = parse_markdown(inner, "pandoc")
        if document.gaps:
            # Внутренний разбор неполон: оставляем raw-представление и не теряем текст.
            result.extend(blocks[position : closing_block + 1])
            position = closing_block + 1
            continue
        line_base = block.span.start_line
        children = tuple(_shift(child, inner_start, line_base) for child in document.blocks)
        result.append(
            MdBlock(
                kind="container",
                span=index.span(block.span.start_line, blocks[closing_block].span.end_line),
                info={
                    "name": "div",
                    "native": True,
                    "opening": piece.rstrip("\n").strip(),
                },
                children=children,
            )
        )
        position = closing_block + 1
    return result