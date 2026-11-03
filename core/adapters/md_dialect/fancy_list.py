"""Fancy lists и example lists Pandoc: маркеры, которых нет в CommonMark.

**Измеренное поведение оракула** (``pandoc -f markdown -t json``, 2.17.1.1). Pandoc
разбирает как ``OrderedList``:

* ``a)``/``b)`` — нижний альфавит со скобкой, ``A)``/``B)`` — верхний;
* ``i.``/``ii.``/``iv.`` — римские (``i`` как первый маркер читается римским, ``v``,
  ``x``, ``l``, ``c``, ``d``, ``m`` — альфой: измерено, ``v. one`` → ``LowerAlpha``
  со значением 22, а ``v. two`` после ``iv.`` — продолжение римского списка);
* ``#.`` — список с автонумерацией (``DefaultStyle``/``DefaultDelim``);
* ``(@)``/``(@label)`` — example list (``Example``/``TwoParens``), метка необязательна;
* одна заглавная буква с точкой (``A.``, ``I.``, ``X.``) списком **не** считается —
  измерено: оракул отдаёт ``Para``; с скобкой (``A)``, ``I)``) — считается.

**Модель.** Один абзац-«список» превращается в ``list`` с пунктами ``list_item``;
граница пункта — строка с маркером, продолжение пункта — строки без маркера (как у
Pandoc: ``a) one\\n   continued`` — один пункт из двух строк). Списки, разделённые
только пустыми строками, оракул **сливает** в один, если первый маркер второго
подходит к стилю первого (``a.``/``b.`` через пустую строку — один список;
``a)``/``iv.`` — два). Правило :func:`_fits` повторяет это измерение.

**Границы.** Вложенность внутри пункта (``a) x\\n   i. nested``) не разбирается: маркер
на любой строке начинает следующий пункт того же уровня. Это видно в чанкере как более
грубые границы пунктов, но не меняет ни блочный тип, ни текст: на дифференциальное
сравнение верхнего уровня граница не влияет, а текст остаётся точным срезом (P1/P4).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import Final

from core.adapters.md_ast import LineIndex, MdBlock

from ._content import content

__all__ = ["Marker", "split_fancy_lists"]

#: Example list: ``(@)`` или ``(@label)`` (метка — буквы/цифры/``_``/``-``).
_EXAMPLE_RE: Final = re.compile(r"^\(@(?P<label>[A-Za-z0-9_-]*)\)[ \t]+")
#: Автонумерация Pandoc: ``#.`` / ``#)``.
_HASH_RE: Final = re.compile(r"^#(?P<delim>[.)])[ \t]+")
#: Обычная нумерация как продолжение пункта (первый маркер такого вида разбирает
#: ``markdown-it``, поэтому здесь она встречается только внутри абзаца-списка).
_DECIMAL_RE: Final = re.compile(r"^(?P<number>\d{1,9})(?P<delim>[.)])[ \t]+")
#: Одна буква: ``a)``, ``a.``, ``A)``.
_ALPHA_RE: Final = re.compile(r"^(?P<letter>[A-Za-z])(?P<delim>[.)])[ \t]+")
#: Многосимвольный римский маркер: ``iv.``, ``xii)``, ``III.``.
_ROMAN_MULTI_RE: Final = re.compile(r"^(?P<marker>[ivxlcdmIVXLCDM]{2,})(?P<delim>[.)])[ \t]+")
#: Проверка, что многосимвольный прогон — действительно римское число.
_ROMAN_VALID_RE: Final = re.compile(
    r"^M{0,4}(?:CM|CD|D?C{0,3})(?:XC|XL|L?X{0,3})(?:IX|IV|V?I{0,3})$"
)

_ROMAN_VALUES: Final[dict[str, int]] = {
    "i": 1,
    "v": 5,
    "x": 10,
    "l": 50,
    "c": 100,
    "d": 500,
    "m": 1000,
}


@dataclass(frozen=True, slots=True)
class Marker:
    """Разобранный маркер списка и его объявленная стилевая принадлежность.

    ``family`` — как маркер читает Pandoc (``example``/``default``/``decimal``/
    ``alpha``/``roman``); ``roman`` — значение, если маркер **вообще** читается как
    римское число (нужно для продолжения римского списка маркером ``v.``, который сам
    по себе был бы альфой).
    """

    text: str
    family: str
    case: str
    delim: str
    single: bool
    roman: int | None
    label: str = ""

    @property
    def style(self) -> tuple[str, str, str]:
        """Стиль списка: то, что обязано совпасть при слиянии через пустую строку."""
        if self.family == "example":
            return ("example", "", "")
        if self.family in ("default", "decimal"):
            return (self.family, "", self.delim)
        return (self.family, self.case, self.delim)


def _roman_value(text: str) -> int | None:
    """Значение римского числа или ``None``, если это не римское число."""
    upper = text.upper()
    if not upper or _ROMAN_VALID_RE.match(upper) is None:
        return None
    total = 0
    previous = 0
    for char in reversed(upper):
        value = _ROMAN_VALUES[char.lower()]
        if value < previous:
            total -= value
        else:
            total += value
            previous = value
    return total


def _marker_at(text: str) -> Marker | None:
    """Маркер fancy/example-списка в начале строки содержимого (или ``None``)."""
    example = _EXAMPLE_RE.match(text)
    if example is not None:
        return Marker(
            text=f"(@{example.group('label')})",
            family="example",
            case="",
            delim="()",
            single=False,
            roman=None,
            label=example.group("label"),
        )
    hashed = _HASH_RE.match(text)
    if hashed is not None:
        return Marker(
            text=f"#{hashed.group('delim')}",
            family="default",
            case="",
            delim=hashed.group("delim"),
            single=False,
            roman=None,
        )
    decimal = _DECIMAL_RE.match(text)
    if decimal is not None:
        return Marker(
            text=f"{decimal.group('number')}{decimal.group('delim')}",
            family="decimal",
            case="",
            delim=decimal.group("delim"),
            single=False,
            roman=None,
        )
    letter = _ALPHA_RE.match(text)
    if letter is not None:
        symbol = letter.group("letter")
        delim = letter.group("delim")
        # Измерено: одна заглавная буква с точкой — не маркер (оракул даёт Para).
        if symbol.isupper() and delim == ".":
            return None
        lowered = symbol.lower()
        value = _ROMAN_VALUES.get(lowered)
        # Измерено: ``i`` как первый маркер читается римским, прочие одиночные буквы —
        # альфой (``v.`` → LowerAlpha 22), хотя все они — допустимые римские цифры.
        family = "roman" if lowered == "i" else "alpha"
        return Marker(
            text=f"{symbol}{delim}",
            family=family,
            case="upper" if symbol.isupper() else "lower",
            delim=delim,
            single=True,
            roman=value,
        )
    roman = _ROMAN_MULTI_RE.match(text)
    if roman is not None:
        marker = roman.group("marker")
        value = _roman_value(marker)
        if value is None:
            return None
        return Marker(
            text=f"{marker}{roman.group('delim')}",
            family="roman",
            case="upper" if marker.isupper() else "lower",
            delim=roman.group("delim"),
            single=False,
            roman=value,
        )
    return None


def _start_value(marker: Marker) -> int:
    """Начальный номер списка для ``info["start"]`` (для контекста, не для нарезки)."""
    if marker.family == "decimal":
        return int(marker.text[:-1])
    if marker.family == "roman" and marker.roman is not None:
        return marker.roman
    if marker.family == "alpha" and marker.single:
        return ord(marker.text[0].lower()) - ord("a") + 1
    return 1


def _fits(marker: Marker, style: tuple[str, str, str]) -> bool:
    """Подходит ли первый маркер нового блока к стилю предыдущего списка.

    Это и есть измеренное правило слияния через пустую строку: ``a.``/``b.`` — один
    список, ``a)``/``iv.`` — два, ``iv.``/``v.`` — один (``v`` читается римским).
    """
    family, case, delim = style
    if family == "example":
        return marker.family == "example"
    if family == "default":
        return marker.family == "default" and marker.delim == delim
    if family == "decimal":
        return marker.family == "decimal" and marker.delim == delim
    if family == "alpha":
        return marker.single and marker.case == case and marker.delim == delim
    if family == "roman":
        return marker.roman is not None and marker.case == case and marker.delim == delim
    return False


def _item(index: LineIndex, start: int, stop: int) -> MdBlock:
    """Пункт списка: спан на строки пункта и абзац-содержимое того же спана.

    Абзац внутри пункта нужен чанкеру: по нему считаются безопасные позиции разреза
    внутри длинного пункта, а маркер пункта печатается в ``context_prefix``. Текст
    остаётся срезом источника — абзац не переписывается.
    """
    span = index.span(start, stop)
    return MdBlock(kind="list_item", span=span, children=(MdBlock(kind="paragraph", span=span),))


def _convert(block: MdBlock, index: LineIndex, *, quoted: bool) -> list[MdBlock] | None:
    """Абзац-список → ``[list]``; ``None`` — первая строка не маркер."""
    if block.kind != "paragraph":
        return None
    first = block.span.start_line
    end = block.span.end_line
    first_marker = _marker_at(content(index.line_text(first), quoted=quoted))
    if first_marker is None:
        return None
    starts = [first]
    for line in range(first + 1, end):
        if _marker_at(content(index.line_text(line), quoted=quoted)) is not None:
            starts.append(line)
    items = tuple(
        _item(index, start, starts[position + 1] if position + 1 < len(starts) else end)
        for position, start in enumerate(starts)
    )
    info = {
        "ordered": True,
        "marker": first_marker.text,
        "start": _start_value(first_marker),
        "dialect": "fancy",
        "list_style": first_marker.style,
        "example": first_marker.family == "example",
    }
    return [
        MdBlock(kind="list", span=index.span(first, end), info=info, children=items)
    ]


def _only_blank_between(
    end_line: int, start_line: int, index: LineIndex, *, quoted: bool
) -> bool:
    """Между блоками только пустые строки (с учётом префикса цитаты)."""
    return all(
        not content(index.line_text(line), quoted=quoted).strip()
        for line in range(end_line, start_line)
    )


def _merge_adjacent(
    blocks: list[MdBlock], index: LineIndex, *, quoted: bool
) -> list[MdBlock]:
    """Сливает соседние fancy-списки, разделённые только пустыми строками.

    Слияние повторяет оракул; чужие списки (``markdown-it``) не трогаются: у них нет
    ``info["dialect"]``, и стиль им не приписывается.
    """
    result: list[MdBlock] = []
    for block in blocks:
        previous = result[-1] if result else None
        if (
            previous is not None
            and previous.kind == "list"
            and block.kind == "list"
            and previous.info.get("dialect") == "fancy"
            and block.info.get("dialect") == "fancy"
            and _only_blank_between(previous.span.end_line, block.span.start_line, index, quoted=quoted)
        ):
            marker = _marker_at(content(index.line_text(block.span.start_line), quoted=quoted))
            style = previous.info.get("list_style")
            if marker is not None and isinstance(style, tuple) and _fits(marker, style):
                result[-1] = replace(
                    previous,
                    span=index.span(previous.span.start_line, block.span.end_line),
                    children=previous.children + block.children,
                )
                continue
        result.append(block)
    return result


def split_fancy_lists(blocks: list[MdBlock], index: LineIndex, *, quoted: bool) -> list[MdBlock]:
    """Заменяет абзацы с fancy/example-маркером на списки и сливает смежные."""
    result: list[MdBlock] = []
    for block in blocks:
        converted = _convert(block, index, quoted=quoted)
        if converted is None:
            result.append(block)
        else:
            result.extend(converted)
    return _merge_adjacent(result, index, quoted=quoted)