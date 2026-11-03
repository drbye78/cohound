"""Семантика пустой строки Pandoc: список и цитата не прерывают абзац.

**Измеренное поведение оракула** (``pandoc -f markdown -t json``, 2.17.1.1). В Pandoc
markdown расширения ``lists_without_preceding_blankline`` и ``blank_before_blockquote``
по умолчанию выключены, поэтому строка ``- пункт`` или ``> цитата`` сразу после строки
абзаца — не новый блок, а продолжение абзаца::

    текст
    - пункт
    > цитата

Pandoc отдаёт **один** ``Para`` на все три строки, ``markdown-it`` — ``paragraph``,
``list`` и ``blockquote``. Тот же замер показывает, где абзац кончается: fenced code
(и HTML-блок) его прерывают, а заголовок, тематический разрыв, pipe-таблица и
definition list — нет (``text\\n- a\\n# H`` → один ``Para``; ``text\\n- a\\n---`` → один
``Para``; ``text\\n- a\\n| x | y |`` → один ``Para``). Поэтому множество
:data:`ABSORBABLE` объявлено по замеру, а не по догадке.

**Почему склейка, а не «список с флагом».** Если оставить блок ``list`` и пометить его
«на самом деле это абзац», то каждый потребитель (чанкер, ``heading_path``, счётчики
конструкций) обязан помнить про флаг. Оракул говорит «абзац» — значит и в дереве это
абзац; текст при этом не двигается: спан абзаца просто продлевается до конца
поглощённого блока (объединение спанов уровня не меняется, P13 сохраняется).
"""

from __future__ import annotations

from dataclasses import replace

from core.adapters.md_ast import LineIndex, MdBlock

__all__ = ["ABSORBABLE", "TRIGGERS", "merge_paragraph_interruptions"]

#: Блоки, которые **начинают** склейку: только у них Pandoc требует пустую строку.
TRIGGERS = frozenset({"list", "blockquote"})

#: Блоки, которые поглощаются уже начатым продолжением абзаца (измерено оракулом).
ABSORBABLE = frozenset(
    {
        "paragraph",
        "list",
        "blockquote",
        "heading",
        "thematic_break",
        "table",
        "definition_list",
    }
)


def _ends_with_content(block: MdBlock, index: LineIndex) -> bool:
    """Кончается ли спан блока содержательной строкой.

    ``markdown-it`` включает в спан списка и цитаты **замыкающую пустую строку**
    (``- a\\n\\n---``: у списка ``map=[0,2]``, а ``---`` начинается со строки 2). Без
    этой проверки «соседство по номеру строки» склеило бы абзац с блоком через пустую
    строку — то есть ровно там, где Pandoc её и требует.
    """
    last = block.span.end_line - 1
    return last >= block.span.start_line and bool(index.line_text(last).strip())


def merge_paragraph_interruptions(blocks: list[MdBlock], index: LineIndex) -> list[MdBlock]:
    """Склеивает абзац с непосредственно следующими за ним блоками без пустой строки.

    Условие соседства — **строки**: следующий блок начинается ровно на той строке, где
    кончился предыдущий, и предыдущий кончается содержательной строкой (см.
    :func:`_ends_with_content`). Пустая строка между блоками разрывает соседство, поэтому
    обычные списки и цитаты (с пустой строкой перед ними) не затрагиваются.

    Склейка продолжается, пока соседние блоки принадлежат :data:`ABSORBABLE`: абзац
    Pandoc тянется до пустой строки. Первый поглощённый блок обязан быть списком или
    цитатой (:data:`TRIGGERS`) — иначе правило не включается вовсе.
    """
    result: list[MdBlock] = []
    absorbing = False
    for block in blocks:
        if (
            result
            and result[-1].kind == "paragraph"
            and block.span.start_line == result[-1].span.end_line
            and _ends_with_content(result[-1], index)
        ):
            previous = result[-1]
            if block.kind in ABSORBABLE and (absorbing or block.kind in TRIGGERS):
                result[-1] = replace(
                    previous,
                    span=index.span(previous.span.start_line, block.span.end_line),
                )
                absorbing = True
                continue
        result.append(block)
        absorbing = False
    return result