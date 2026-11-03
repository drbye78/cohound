"""Line block Pandoc (``| строка``): блок из строк, единица нарезки — строка (§5).

**Измеренное поведение оракула** (``pandoc -f markdown -t json``, 2.17.1.1):

* блок начинается строкой, у которой в начале содержимого стоит ``|`` и **пробел**
  (``| строка``); ``|a`` — не line block, а обычный текст;
* каждая следующая строка блока тоже начинается с ``| ``; первая строка без ``| ``
  заканчивает блок, остаток становится обычным абзацем;
* блок требует начала блока: ``текст\\n| a`` — это абзац (Pandoc не прерывает абзац
  line block'ом), ``текст\\n\\n| a`` — line block;
* отступ в начале строки **ломает** блок на верхнем уровне (``  | a`` → абзац), но
  префикс контейнера не считается отступом: ``> | a`` — line block внутри цитаты.

**Модель.** ``MdBlock(kind="line_block")`` со спаном на весь блок и дочерними блоками
``kind="line"`` по строке каждый: единица нарезки объявлена структурой, а не соглашением
чанкера. Текст не переписывается — срез включает маркер ``| ``, как он стоит в источнике
(P4: чанк — точная подстрока входа).
"""

from __future__ import annotations

from dataclasses import replace

from core.adapters.md_ast import LineIndex, MdBlock

from ._content import content

__all__ = ["LINE_START", "split_line_blocks"]

#: Начало строки line block в содержимом (после префикса контейнера).
LINE_START = "| "


def _split_one(block: MdBlock, index: LineIndex, *, quoted: bool) -> list[MdBlock] | None:
    """Абзац → ``[line_block, остаток-абзац?]``; ``None`` — блок не line block."""
    if block.kind != "paragraph":
        return None
    first = block.span.start_line
    end = block.span.end_line
    run = 0
    for line in range(first, end):
        if not content(index.line_text(line), quoted=quoted).startswith(LINE_START):
            break
        run += 1
    if run == 0:
        return None
    lines = tuple(
        MdBlock(kind="line", span=index.span(line, line + 1), info={"number": line - first + 1})
        for line in range(first, first + run)
    )
    line_block = MdBlock(
        kind="line_block",
        span=index.span(first, first + run),
        info={"lines": run},
        children=lines,
    )
    if first + run == end:
        return [line_block]
    # Хвост после line block — обычный абзац: оракул именно так и решает
    # (``| a\\nplain\\n| b`` → LineBlock + Para, а не один блок).
    return [line_block, replace(block, span=index.span(first + run, end))]


def split_line_blocks(blocks: list[MdBlock], index: LineIndex, *, quoted: bool) -> list[MdBlock]:
    """Заменяет абзацы, начинающиеся с ``| ``, на блоки ``line_block``."""
    result: list[MdBlock] = []
    for block in blocks:
        converted = _split_one(block, index, quoted=quoted)
        if converted is None:
            result.append(block)
        else:
            result.extend(converted)
    return result