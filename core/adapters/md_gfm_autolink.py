"""GFM-автоссылки: копия плагина ``mdit-py-plugins`` с исправленной silent-веткой.

**Что это.** Три инлайновых правила GFM-расширения автоссылок (``www.``, ``http(s)://``/
``mailto:``/``xmpp:``, голый e-mail) — построчная копия
``mdit_py_plugins/gfm_autolink/index.py`` из ``mdit-py-plugins`` 0.6.1 (MIT, © Chris Sewell,
https://github.com/executablebooks/mdit-py-plugins). Хелперы сопоставления
(:func:`check_prev`, :func:`match_www`, :func:`match_http`, :func:`match_any_email`) **не
копируются**, а импортируются из ``mdit_py_plugins.gfm_autolink._match``: это ~250 строк
портированной Rust-логики, которые пришлось бы синхронизировать вручную; версия пакета
запинена в ``uv.lock``, а наличие символов проверяется на импорте — при исчезновении
модуль падает громко, а не молча теряет автоссылки.

**Какой дефект исправлен.** Апстрим (0.6.1 — последняя версия на PyPI, проверено
07.11.2026) в silent-режиме возвращает ``True``, **не сдвигая** ``state.pos``: сдвиг
(``state.pos += length`` / ``_create_autolink``) стоит только в не-silent ветке. Молчаливый
успех без сдвига ломает инвариант вызывающего:

* ``markdown_it/parser_inline.py:132`` — :meth:`ParserInline.skipToken` при успехе правила
  кеширует ``cache[pos] = state.pos`` (строка 172); при отсутствии сдвига в кеш попадает
  **исходная** позиция;
* ``markdown_it/helpers/parse_link_label.py:20`` — ``parseLinkLabel`` крутится в
  ``while state.pos < state.posMax`` и вызывает ``skipToken`` (строка 29), полагаясь на то,
  что позиция продвинулась. Позиция не двигается ⇒ **бесконечный цикл** без исключения и
  без таймаута.

Достаточно, чтобы внутри ``[`` … ``]`` встретилось ``www.`` (или протокол/e-mail), а
предыдущий символ проходил :func:`check_prev` (пробел, ``(``, ``*``, ``_``, ``~``, таб,
перевод строки, начало строки). Пример из корпуса::

    [буке: www.facebook.com/groups/postgresql.](https://www.facebook.com/groups/postgresql)

Тот же инвариант нарушают и другие сканеры плагинов, которые ищут закрывающий символ через
``skipToken``: ``mdit_py_plugins/superscript/index.py:79`` (профиль ``pandoc``) не
возвращался на ``^см. www.example.com^`` по той же причине. Правка здесь чинит оба контура
сразу — позиция начинает двигаться в самом правиле.

**Что именно изменено.** Длина совпадения вычисляется **до** ветки ``silent``, и в
silent-ветке позиция сдвигается на неё же (``total_len - bscan_len`` для протокола и
e-mail — ровно то, на что сдвигает ``_create_autolink``). ``state.pending`` в silent-ветке
**не трогается**: валидационный проход не создаёт токенов и не имеет права менять
накопленный текст (в не-silent ветке ``_create_autolink`` обрезает из ``pending``
обратно-просканированный префикс).

**Почему версия чанкера не поднимается.** ``chunker_version`` входит в
``EXECUTION_CONTRACT_FIELDS`` (``core/pipeline/digests.py:108``), то есть её смена даёт
новый ``run_id`` и обнуляет текущий прогон. Дефектная ветка при этом **никогда не
возвращала результат**: документ, попадавший в неё, не завершался вовсе, и ни одного
артефакта под ней не существует. Значит эффективное отображение «текст → чанки» для всех
завершившихся документов не меняется, и ``policy-5``/``plugins-0.6.1`` остаются верными.

Нейтрален (§1.3): грамматика markdown — не предметная область.
"""

from __future__ import annotations

import re

from markdown_it import MarkdownIt
from markdown_it.rules_inline import StateInline
from mdit_py_plugins.gfm_autolink import _match as _gfm_match

__all__ = ["gfm_autolink_plugin"]

#: Символы сопоставления, которые обязаны существовать в пакете. Проверяются на импорте:
#: тихо потерять автоссылки при обновлении зависимости хуже, чем не собраться.
_REQUIRED_MATCH_SYMBOLS = ("check_prev", "match_any_email", "match_http", "match_www")

_missing = [name for name in _REQUIRED_MATCH_SYMBOLS if not hasattr(_gfm_match, name)]
if _missing:
    raise ImportError(
        "mdit_py_plugins.gfm_autolink._match не содержит "
        f"{', '.join(_missing)}: внутренний API mdit-py-plugins изменился. "
        "Сверьте версию пакета (uv.lock) с копией правил в "
        "core/adapters/md_gfm_autolink.py — правку нельзя оставить незамеченной."
    )

check_prev = _gfm_match.check_prev
match_any_email = _gfm_match.match_any_email
match_http = _gfm_match.match_http
match_www = _gfm_match.match_www

# Регулярка обратного сканирования ``pending`` на имя протокола, оканчивающееся текущей
# позицией (символ ``:``).
_PROTO_RE = re.compile(r"(?:^|.)(https?|mailto|xmpp)$", re.DOTALL)


def gfm_autolink_plugin(md: MarkdownIt) -> None:
    """Включает GFM-расширение автоссылок.

    Распознаёт голые ``www.``-адреса, ``http(s)://``, ``mailto:``/``xmpp:`` и голые
    e-mail. Требует ``markdown-it-py`` ≥ 4.1.0 (``add_terminator_char``).
    """
    if not hasattr(md.inline, "add_terminator_char"):
        raise RuntimeError("gfm_autolink_plugin требует markdown-it-py >= 4.1.0")

    md.inline.add_terminator_char("w")
    md.inline.ruler.push("gfm_autolink_www", _www_inline_rule)
    md.inline.ruler.push("gfm_autolink_protocol", _protocol_rule)
    md.inline.ruler.push("gfm_autolink_email", _email_rule)


# ---------------------------------------------------------------------------
# Вспомогательные функции инлайновых правил
# ---------------------------------------------------------------------------


def _preceding_ok(state: StateInline, bscan_len: int) -> bool:
    """Разрешён ли автосимвол по символу перед обратно-просканированной частью."""
    abs_pos = state.pos - bscan_len
    if abs_pos <= 0:
        return True
    preceding = state.src[abs_pos - 1]
    return check_prev(preceding)


def _create_autolink(
    state: StateInline,
    bscan_len: int,
    total_len: int,
    url: str,
    text: str,
) -> bool:
    """Выдаёт ``link_open`` / ``text`` / ``link_close`` и двигает позицию.

    Из конца ``state.pending`` срезается *bscan_len* символов (обратно-просканированный
    протокол или локальная часть адреса); позиция сдвигается на ``total_len - bscan_len``.
    """
    if bscan_len:
        state.pending = state.pending[:-bscan_len]

    full_url = state.md.normalizeLink(url)
    if not state.md.validateLink(full_url):
        return False

    token = state.push("link_open", "a", 1)
    token.attrs = {"href": full_url}
    token.markup = "autolink"
    token.info = "auto"

    token = state.push("text", "", 0)
    token.content = state.md.normalizeLinkText(text)

    token = state.push("link_close", "a", -1)
    token.markup = "autolink"
    token.info = "auto"

    state.pos += total_len - bscan_len
    return True


# ---------------------------------------------------------------------------
# Правило ``www.``  (нужен add_terminator_char("w") — markdown-it-py >= 4.1.0)
# ---------------------------------------------------------------------------


def _www_inline_rule(state: StateInline, silent: bool) -> bool:
    """Сопоставляет автоссылку ``www.`` (триггерный символ: ``w``)."""
    if state.linkLevel > 0:
        return False

    pos = state.pos
    src = state.src

    # Быстрая проверка: символ ``w`` и последовательность «www.».
    if src[pos] != "w":
        return False
    if pos + 4 > state.posMax or src[pos : pos + 4] != "www.":
        return False

    # Проверка предыдущего символа (из pending или начала строки).
    if state.pending:
        preceding = state.pending[-1]
        if not check_prev(preceding):
            return False
    elif pos > 0:
        preceding = src[pos - 1]
        if not check_prev(preceding):
            return False

    result = match_www(src[pos : state.posMax])
    if result is None:
        return False

    url, length = result
    label = src[pos : pos + length]

    if silent:
        # ИСПРАВЛЕНИЕ: валидационный проход обязан продвинуть позицию. Без этого
        # ``skipToken`` кеширует неизменённую позицию, а ``parseLinkLabel`` зацикливается
        # (см. докстроку модуля). ``pending`` здесь не меняется.
        state.pos += length
        return True

    full_url = state.md.normalizeLink(url)
    if not state.md.validateLink(full_url):
        return False

    token = state.push("link_open", "a", 1)
    token.attrs = {"href": full_url}
    token.markup = "autolink"
    token.info = "auto"

    token = state.push("text", "", 0)
    token.content = state.md.normalizeLinkText(label)

    token = state.push("link_close", "a", -1)
    token.markup = "autolink"
    token.info = "auto"

    state.pos += length
    return True


# ---------------------------------------------------------------------------
# Сканер протокола  (триггерный символ: ':')
# ---------------------------------------------------------------------------


def _protocol_rule(state: StateInline, silent: bool) -> bool:
    if state.linkLevel > 0:
        return False

    pos = state.pos
    remaining = state.src[pos : state.posMax]

    # Должно начинаться с ':' и иметь минимум 3 символа после него.
    if len(remaining) < 4 or remaining[0] != ":":
        return False

    # Обратное сканирование pending на известное имя протокола.
    m = _PROTO_RE.search(state.pending)
    if m is None:
        return False

    proto = m.group(1)
    bscan_len = len(proto)

    if not _preceding_ok(state, bscan_len):
        return False

    # Обратно-просканированный протокол склеивается с остатком текста.
    combined = proto + remaining

    if proto in ("mailto", "xmpp"):
        result = match_any_email(combined, bscan_len + 1, proto)
    else:
        result = match_http(combined)

    if result is None:
        return False

    full_url, total_len = result
    label = combined[:total_len]

    if silent:
        # ИСПРАВЛЕНИЕ: та же длина, на которую сдвигает ``_create_autolink``; ``pending``
        # в валидационном проходе не обрезается.
        state.pos += total_len - bscan_len
        return True
    return _create_autolink(state, bscan_len, total_len, full_url, label)


# ---------------------------------------------------------------------------
# Сканер голого e-mail  (триггерный символ: '@')
# ---------------------------------------------------------------------------


def _email_rule(state: StateInline, silent: bool) -> bool:
    if state.linkLevel > 0:
        return False

    pos = state.pos
    if pos >= state.posMax or state.src[pos] != "@":
        return False
    # Нужен хотя бы один символ после '@'.
    if pos + 1 >= state.posMax:
        return False

    # Обратное сканирование pending на локальную часть адреса.
    local_rev: list[str] = []
    for ch in reversed(state.pending):
        if ch.isascii() and (ch.isalnum() or ch in ".+-_"):
            local_rev.append(ch)
        else:
            break

    if not local_rev:
        return False

    local_len = len(local_rev)
    if not _preceding_ok(state, local_len):
        return False

    # Прямое сканирование доменной части.
    after_at = state.src[pos + 1 : state.posMax]
    domain_len = 0
    num_period = 0
    for i, ch in enumerate(after_at):
        if ch.isascii() and ch.isalnum():
            pass
        elif ch == "@":
            return False
        elif (
            ch == "."
            and i + 1 < len(after_at)
            and after_at[i + 1].isascii()
            and after_at[i + 1].isalnum()
        ):
            num_period += 1
        elif ch != "-" and ch != "_":
            break
        domain_len += 1

    if domain_len == 0 or num_period == 0:
        return False

    last_ch = after_at[domain_len - 1]
    if not (last_ch.isascii() and last_ch.isalnum()) and last_ch != ".":
        return False

    local_part = "".join(reversed(local_rev))
    email_text = local_part + state.src[pos : pos + 1 + domain_len]
    total_len = local_len + 1 + domain_len
    url = "mailto:" + email_text

    if silent:
        # ИСПРАВЛЕНИЕ: длина — ``total_len - local_len``, как в ``_create_autolink``.
        state.pos += total_len - local_len
        return True
    return _create_autolink(state, local_len, total_len, url, email_text)
