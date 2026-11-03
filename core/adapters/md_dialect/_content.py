"""Содержимое строки без префикса контейнера (цитата).

Правила Pandoc-профиля смотрят на **то, что видит грамматика**: внутри ``blockquote``
строка ``> | строка`` — это line block, а не абзац, начинающийся с ``>``. Поэтому
маркеры ищутся в содержимом строки, а не в сыром тексте. Отступ внутри пункта списка
сюда не входит: он считается по маркеру пункта (объявленная граница, см. ``__init__``).
"""

from __future__ import annotations

import re
from typing import Final

#: Цепочка маркеров цитаты в начале строки (``> ``/``>``, допускается до 3 пробелов).
QUOTE_PREFIX_RE: Final = re.compile(r"^(?: {0,3}> ?)+")


def content(line_text: str, *, quoted: bool) -> str:
    """Строка без префикса цитаты (``quoted=False`` — как есть)."""
    if not quoted:
        return line_text
    match = QUOTE_PREFIX_RE.match(line_text)
    return line_text[match.end() :] if match is not None else line_text