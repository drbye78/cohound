"""Raw TeX блок Pandoc: параграф, начинающийся с TeX-команды (вторая половина L3).

**Измеренное поведение оракула** (``pandoc -f markdown -t json``, 2.17.1.1). Параграф,
начинающийся с обратной косой черты и **буквенной** команды, становится ``RawBlock`` с
``raw_format = "tex"``::

    \\newcommand{\\tuple}[1]{...}   →  RawBlock tex
    \\begin{quote} … \\end{quote}   →  RawBlock tex
    \\alpha                          →  RawBlock tex

Границы измерены, а не предположены:

* команда с аргументами ``{…}``/``[…]`` может занимать несколько строк — блок кончается
  там, где закрылись скобки (``\\newcommand{\\x}{\\n y\\n}`` — один ``RawBlock``);
* если **на той же строке** после конструкции остаётся текст, это уже не raw:
  ``\\alpha beta`` → ``Para``; для ``\\begin{env}`` хвост становится отдельным ``Para``
  (``\\begin{quote}x\\end{quote} trailing`` → ``RawBlock`` + ``Para``). Здесь реализован
  только первый случай (остаток строки пуст) — второй объявлен границей: иначе правило
  резало бы абзац посередине строки;
* ``\\alpha\\nbeta`` → ``RawBlock`` + ``Para``: следующая строка не входит в raw-блок;
* экранированная пунктуация (``\\%``, ``\\$``, ``\\\\``) — не команда: ``Para``;
* TeX-окружения с математикой (``align``, ``equation``, ``gather``, ``multline``,
  ``displaymath``, ``math``) оракул **не** считает raw-блоком — он отдаёт ``Para``.
  Исключение объявлено множеством :data:`MATH_ENVIRONMENTS`, а не выведено из имени.

**Модель.** ``MdBlock(kind="raw")`` с ``info["raw_format"] = "tex"`` и ``marker`` —
командой/``\\begin{env}`` для ``context_prefix`` продолжения. Текст не переписывается:
спан — отрезок исходных строк (P4), поэтому рез по строкам при переполнении остаётся
точным срезом.

Нейтрален (§1.3): грамматика markdown — не предметная область.
"""

from __future__ import annotations

import re
from typing import Final

from core.adapters.md_ast import LineIndex, MdBlock

__all__ = ["MATH_ENVIRONMENTS", "RAW_FORMAT", "split_raw_tex"]

#: Формат raw-блока в терминах Pandoc (``RawBlock`` ``c[0]``).
RAW_FORMAT: Final = "tex"

#: Окружения, которые оракул разбирает как математику, а не как raw TeX (измерено).
MATH_ENVIRONMENTS: Final = frozenset(
    {
        "align",
        "align*",
        "alignat",
        "alignat*",
        "displaymath",
        "eqnarray",
        "eqnarray*",
        "equation",
        "equation*",
        "gather",
        "gather*",
        "math",
        "multline",
        "multline*",
        "split",
    }
)

_COMMAND_RE: Final = re.compile(r"\\([A-Za-z]+)")
_ENV_RE: Final = re.compile(r"\{([^{}\n]*)\}")


def _skip_spaces(text: str, position: int) -> int:
    while position < len(text) and text[position] in " \t":
        position += 1
    return position


def _balanced(text: str, position: int, opener: str, closer: str) -> int | None:
    """Конец сбалансированной группы от ``opener`` (учитывает вложенность)."""
    if position >= len(text) or text[position] != opener:
        return None
    depth = 0
    cursor = position
    while cursor < len(text):
        char = text[cursor]
        if char == "\\":
            cursor += 2
            continue
        if char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return cursor + 1
        cursor += 1
    return None


def _command_end(text: str, command_end: int) -> int | None:
    """Конец конструкции «команда + её аргументы» (``{…}``/``[…]`` подряд)."""
    position = command_end
    while True:
        position = _skip_spaces(text, position)
        if position >= len(text) or text[position] not in "{[":
            return position
        closer = "}" if text[position] == "{" else "]"
        end = _balanced(text, position, text[position], closer)
        if end is None:
            return None
        position = end


def _tex_end(text: str) -> tuple[int, str] | None:
    """Конец TeX-конструкции в начале ``text`` и маркер для контекста."""
    match = _COMMAND_RE.match(text)
    if match is None:
        return None
    command = match.group(1)
    if command == "begin":
        environment = _ENV_RE.match(text, match.end())
        if environment is None:
            return None
        name = environment.group(1).strip()
        if name in MATH_ENVIRONMENTS:
            return None
        closing = f"\\end{{{name}}}"
        found = text.find(closing, environment.end())
        if found < 0:
            return None
        return found + len(closing), f"\\begin{{{name}}}"
    command_end = _command_end(text, match.end())
    if command_end is None:
        return None
    return command_end, f"\\{command}"


def _split_paragraph(block: MdBlock, index: LineIndex) -> list[MdBlock] | None:
    """Параграф → ``[raw, остаток?]``; ``None`` — это не raw TeX."""
    if block.kind != "paragraph":
        return None
    piece = index.slice(block.span)
    started = _skip_spaces(piece, 0)
    if started > 3 or not piece.startswith("\\", started):
        return None
    result = _tex_end(piece[started:])
    if result is None:
        return None
    relative_end, marker = result
    end = started + relative_end
    line_end = piece.find("\n", end)
    if line_end < 0:
        line_end = len(piece)
    # Хвост той же строки делает конструкцию текстом (измерено), а не raw-блоком.
    if piece[end:line_end].strip():
        return None
    absolute_end = block.span.start_char + end
    raw_end_line = index.line_of(max(absolute_end - 1, block.span.start_char)) + 1
    if absolute_end <= block.span.start_char:
        return None
    raw = MdBlock(
        kind="raw",
        span=index.span(block.span.start_line, raw_end_line),
        info={"raw_format": RAW_FORMAT, "marker": marker},
    )
    if raw_end_line >= block.span.end_line:
        return [raw]
    # Следующие строки того же абзаца — обычный текст (измерено: ``\alpha\nbeta``).
    tail = MdBlock(kind="paragraph", span=index.span(raw_end_line, block.span.end_line))
    return [raw, tail]


def split_raw_tex(blocks: list[MdBlock], index: LineIndex, *, quoted: bool = False) -> list[MdBlock]:
    """Заменяет параграфы, начинающиеся с TeX-команды, блоками ``raw``.

    ``quoted=True`` (содержимое цитаты) не обрабатывается: там пришлось бы снимать
    префикс ``> `` построчно и пересчитывать смещения — это объявленная граница, а не
    забытая ветка.
    """
    if quoted:
        return blocks
    result: list[MdBlock] = []
    for block in blocks:
        converted = _split_paragraph(block, index)
        if converted is None:
            result.append(block)
        else:
            result.extend(converted)
    return result