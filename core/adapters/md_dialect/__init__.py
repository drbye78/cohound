"""Правила блочного разбора Pandoc markdown, которых нет в CommonMark/GFM (L3).

**Зачем отдельные правила.** ``markdown-it-py`` реализует CommonMark и GFM; Pandoc
markdown — надмножество с собственной семантикой блоков. Три расхождения измерены
дифференциальным оракулом (``scripts/conformance_pandoc.py``, оракул ``pandoc 2.17.1.1``):

1. **Пустая строка.** Pandoc не начинает список и цитату сразу после строки абзаца
   (``lists_without_preceding_blankline`` выключено, ``blank_before_blockquote``
   включено), CommonMark/GFM — начинает. Правило: такой список/цитата — продолжение
   абзаца, то есть **абзац**, а не ``list``/``blockquote``.
2. **Line block** (``| строка``) — блок из строк, минимальная единица — строка.
3. **Fancy lists** (``a)``, ``A)``, ``i.``, ``iv.``, ``#.``) и **example lists**
   (``(@)``, ``(@label)``) — списки с маркером, которого нет в CommonMark.

**Порядок правил объявлен явно** (:data:`RULES`), потому что он наблюдаем: сначала
склеиваются «прерванные» абзацы, затем уже готовые абзацы делятся на line block и
списки. Обратный порядок дал бы другой блок на том же входе.

**Почему это безопасно для P1/P4/P13.** Правила не двигают текст и не меняют разметку:
они только перекладывают уже разобранные блоки в другую форму, а спаны остаются
отрезками тех же строк источника. Объединение спанов уровня не меняется, поэтому
покрытие (P13) и точность срезов (P1/P4) сохраняются по построению, а не по обещанию.

**Что здесь не реализовано и остаётся видимым в счётчике оракула:** grid/multiline/
simple tables и подпись таблицы, raw TeX, нативный HTML ``Div`` — вторая половина L3.

Нейтрален (§1.3): грамматика markdown — не предметная область.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Final

from core.adapters.md_ast import LineIndex, MdBlock

from .blank_lines import merge_paragraph_interruptions
from .fancy_list import split_fancy_lists
from .line_block import split_line_blocks

__all__ = ["RULES", "apply_rules"]

#: Порядок правил Pandoc-профиля. Список — не украшение: он и есть precedence.
RULES: Final[tuple[str, ...]] = ("blank_lines", "line_block", "fancy_list")


def _apply_level(blocks: list[MdBlock], index: LineIndex, *, quoted: bool) -> list[MdBlock]:
    """Правила на одном уровне вложенности, в объявленном порядке."""
    merged = merge_paragraph_interruptions(blocks, index)
    line_blocks = split_line_blocks(merged, index, quoted=quoted)
    return split_fancy_lists(line_blocks, index, quoted=quoted)


def _with_children(block: MdBlock, index: LineIndex) -> MdBlock:
    """Рекурсия в ``blockquote``: его содержимое — блоки того же грамматического уровня.

    Внутрь пунктов списка рекурсия не идёт: там нужен отступ содержимого, который
    считается по маркеру пункта, а не по контейнеру. Это объявленная граница, а не
    забытая ветка: на блочное сравнение с оракулом она не влияет (верхний уровень —
    ``list``), а чанкер внутри пункта режет по маркеру, как и раньше.
    """
    if block.kind != "blockquote" or not block.children:
        return block
    return replace(block, children=tuple(_apply_level(list(block.children), index, quoted=True)))


def apply_rules(blocks: list[MdBlock], index: LineIndex) -> list[MdBlock]:
    """Применяет правила профиля ``pandoc`` к дереву блоков.

    Вызывается только для профиля ``pandoc``: в ``commonmark``/``gfm`` структура блоков
    обязана остаться байт-в-байт прежней, поэтому там этот модуль не исполняется вовсе.
    Ни одно правило не добавляет и не убирает строк, поэтому спаны остаются теми, что
    отдал разборщик, а объединение спанов уровня не меняется.
    """
    prepared = [_with_children(block, index) for block in blocks]
    return _apply_level(prepared, index, quoted=False)