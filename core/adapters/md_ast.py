"""AST-слой markdown: блоки со спанами строк и безопасные позиции разреза (L1a).

**Зачем отдельный слой.** Чанкер markdown сегодня — строчный конечный автомат на
регулярках: он не знает таблиц, вложенных списков, blockquote, indented code, HTML-блоков
и сносок. Эвристика ошибается молча (замер 03.11.2026: строчный regex «indented code»
насчитал 412 документов, настоящий парсер нашёл 64 блока). Здесь — разбор настоящей
грамматикой: CommonMark 0.31.2, GFM и (по профилю) Pandoc. Каждый блок несёт спан
**строк** и **символьных смещений** в исходном тексте, поэтому нарезка по блокам остаётся
дословной (I22: цитата — подстрока `text`).

**Профиль диалекта — свойство источника, а не прогона.** `trafilatura` (HTML) и `docling`
(PDF/Office) не пишут Pandoc-разметку. Включать им `dollarmath` нельзя: `$$` в
документации — это, как правило, dollar-quoting SQL (замер: 838 вхождений в 91 документе),
а не формула. Поэтому профили объявлены явно (:data:`PROFILES`) и входят в контракт
через `chunker_version`.

**Покрытие строк (P13).** Каждая непустая строка обязана попасть ровно в один спан
верхнего уровня. Разрыв — это либо распознанный блок, который парсер не отдаёт токеном
(reference definitions вырезаются молча), либо :data:`GAP_KIND` — видимый сигнал
«непознанная конструкция», а не тихое «плоский текст».

**Почему безопасные позиции считает сканер, а не AST.** Инлайновые токены markdown-it
**не несут** `map` (проверено: `text`, `code_inline`, `link_open` — все `map=None`), а их
`content` внутри blockquote уже без `>`-префиксов. Взять смещения из AST значило бы
строить карту «content → источник», которую проще обойти: сканер работает по **сырому
срезу** исходника и потому точен по построению.

**Профиль ``pandoc`` — не только плагины, но и правила блоков.** ``markdown-it-py`` не
знает семантики пустой строки Pandoc (список/цитата не прерывают абзац), line block'ов и
fancy/example-списков. Эти правила живут в :mod:`core.adapters.md_dialect` и применяются
**только** к профилю ``pandoc``; в ``commonmark``/``gfm`` структура блоков остаётся
байт-в-байт прежней (это запиновано тестами). Правила не двигают текст: спаны остаются
отрезками тех же строк, поэтому P1/P4/P13 сохраняются.

Слой **аддитивен**: он не меняет поведение чанкера, пока чанкер не переведён на него
(L1b). Нейтрален (§1.3): грамматика markdown — не предметная область.
"""

from __future__ import annotations

import re
from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Any, Final

from markdown_it import MarkdownIt
from markdown_it.token import Token
from mdit_py_plugins.attrs import attrs_plugin
from mdit_py_plugins.colon_fence import colon_fence_plugin
from mdit_py_plugins.container import container_plugin
from mdit_py_plugins.deflist import deflist_plugin
from mdit_py_plugins.dollarmath import dollarmath_plugin
from mdit_py_plugins.footnote import footnote_plugin
from mdit_py_plugins.front_matter import front_matter_plugin
from mdit_py_plugins.gfm_autolink import gfm_autolink_plugin
from mdit_py_plugins.superscript import superscript_plugin
from mdit_py_plugins.tasklists import tasklists_plugin

from core.util.text_utils import SENTENCE_END_CHARS, is_cut_boundary

__all__ = [
    "GAP_KIND",
    "PROFILES",
    "LineIndex",
    "MdBlock",
    "MdDocument",
    "MdSpan",
    "is_cut_boundary",
    "normalize_text",
    "parse_markdown",
    "safe_cut_offsets",
]

#: Метка блока, который не отдал ни один токен: видимый сигнал P12/P13.
GAP_KIND: Final = "unknown_construct"

#: Объявленные профили грамматики. Порядок — от узкого к широкому.
PROFILES: Final[tuple[str, ...]] = ("commonmark", "gfm", "pandoc")

_BOM: Final = "\ufeff"

#: Предел длины метки определения ссылки (CommonMark §4.7: не более 999 символов).
_LABEL_MAX: Final = 999

#: Открывающие кавычки заголовка определения и их закрывающие пары (§4.7).
_TITLE_CLOSERS: Final = {'"': '"', "'": "'", "(": ")"}

#: Метка тела сноски в начале строки (``[^1]:``).
_FOOTNOTE_LABEL_RE: Final = re.compile(r"^ {0,3}\[\^([^\]\n]+)\]:\s*$")

_CONTAINER_OPEN: Final[dict[str, str]] = {
    "heading_open": "heading",
    "paragraph_open": "paragraph",
    "table_open": "table",
    "thead_open": "table_head",
    "tbody_open": "table_body",
    "tr_open": "table_row",
    "blockquote_open": "blockquote",
    "bullet_list_open": "list",
    "ordered_list_open": "list",
    "list_item_open": "list_item",
    "dl_open": "definition_list",
    "dt_open": "definition_term",
    "dd_open": "definition",
    "footnote_open": "footnote_def",
}

_LEAF_BLOCKS: Final[dict[str, str]] = {
    "fence": "fence",
    # ``::: {.cls} … :::`` — div в профиле ``pandoc``. Токен ``colon_fence`` несёт
    # ``map`` на весь блок, включая закрывающую строку, и **не** разбирает содержимое
    # на блоки: без этого отображения весь текст div уходил в разрыв покрытия (P13), а
    # чанкер его вовсе не видел.
    "colon_fence": "container",
    "code_block": "code",
    "hr": "thematic_break",
    "html_block": "html",
    "front_matter": "front_matter",
    "math_block": "math",
}


# ---------------------------------------------------------------------------
# Нормализация и строчный индекс
# ---------------------------------------------------------------------------


def normalize_text(text: str) -> tuple[str, bool]:
    """Текст, пригодный для разбора: без BOM, с ``\\n`` в качестве перевода строки.

    Возвращает ``(текст, изменился_ли)``. Изменение не скрывается: спан, посчитанный по
    нормализованному тексту, относится к нему же, поэтому вызывающий обязан использовать
    возвращённый текст как источник срезов (иначе P1/P4 поедут).

    **Зачем здесь, если финализация уже есть.** ``strip_control_chars``
    (:func:`core.util.sanitize.strip_control_chars`) удаляет ``\\r`` целиком, а не
    нормализует: строка ``'a\\rb'`` превращается в ``'ab'`` — CR-only markdown теряет
    строчную структуру **до** чанкера. BOM же сохраняется, и первый заголовок документа
    с BOM не распознаётся. Оба случая — дефекты входа, а не разбора; здесь они
    нейтрализуются и видны флагом ``normalized``.
    """
    normalized = False
    if text.startswith(_BOM):
        text = text[len(_BOM) :]
        normalized = True
    if "\r" in text:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        normalized = True
    return text, normalized


@dataclass(frozen=True, slots=True)
class MdSpan:
    """Диапазон: строки (0-based, конец исключительно) и символьные смещения."""

    start_line: int
    end_line: int
    start_char: int
    end_char: int

    @property
    def lines(self) -> int:
        return self.end_line - self.start_line

    @property
    def chars(self) -> int:
        return self.end_char - self.start_char


class LineIndex:
    """Карта «номер строки → символьное смещение» для нормализованного текста.

    Спаны блоков markdown-it выровнены по строкам, поэтому срез по строкам точен
    байт-в-байт: перевод строки принадлежит разрезу, а не блоку.
    """

    __slots__ = ("line_count", "starts", "text")

    def __init__(self, text: str) -> None:
        self.text = text
        starts = [0]
        for position, char in enumerate(text):
            if char == "\n":
                starts.append(position + 1)
        self.starts = starts
        self.line_count = len(starts)

    def offset(self, line: int) -> int:
        """Смещение начала строки (``line >= line_count`` → длина текста)."""
        if line < 0:
            raise ValueError(f"отрицательный номер строки: {line}")
        if line >= self.line_count:
            return len(self.text)
        return self.starts[line]

    def line_of(self, char: int) -> int:
        """Номер строки, содержащей символ (для обратного перевода offset → line)."""
        return max(0, bisect_right(self.starts, char) - 1)

    def span(self, start_line: int, end_line: int) -> MdSpan:
        """Спан по диапазону строк; конец строки в блок не входит."""
        if start_line < 0:
            raise ValueError(f"отрицательное начало спана: {start_line}")
        end_line = max(start_line, min(end_line, self.line_count))
        return MdSpan(
            start_line=start_line,
            end_line=end_line,
            start_char=self.offset(start_line),
            end_char=self.offset(end_line),
        )

    def slice(self, span: MdSpan) -> str:
        return self.text[span.start_char : span.end_char]

    def line_text(self, line: int) -> str:
        return self.text[self.offset(line) : self.offset(line + 1)]


# ---------------------------------------------------------------------------
# Блоки
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MdBlock:
    """Блок разметки со спаном и (для контейнеров) детьми."""

    kind: str
    span: MdSpan
    level: int = 0
    info: dict[str, Any] = field(default_factory=dict)
    children: tuple[MdBlock, ...] = ()

    def walk(self) -> list[MdBlock]:
        """Блок и все его потомки в порядке документа."""
        result = [self]
        for child in self.children:
            result.extend(child.walk())
        return result


@dataclass(frozen=True, slots=True)
class MdDocument:
    """Результат разбора: блоки, разрывы покрытия, счётчики конструкций."""

    text: str
    profile: str
    blocks: tuple[MdBlock, ...]
    gaps: tuple[MdSpan, ...]
    counts: dict[str, int]
    normalized: bool = False

    def of_kind(self, kind: str) -> tuple[MdBlock, ...]:
        """Все блоки указанного вида, включая вложенные."""
        return tuple(block for top in self.blocks for block in top.walk() if block.kind == kind)


@dataclass
class _Frame:
    """Открытый контейнер при обходе потока токенов.

    ``close_type`` обязателен: ``th_close``/``td_close`` тоже кончаются на ``_close``, и
    без сверки типа ячейка закрывала бы строку таблицы (найдено пином L1a).
    """

    kind: str
    lines: tuple[int, int] | None
    close_type: str
    level: int = 0
    info: dict[str, Any] = field(default_factory=dict)
    children: list[MdBlock] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Парсеры по профилям
# ---------------------------------------------------------------------------

_PARSERS: dict[tuple[str, bool], MarkdownIt] = {}


def _parser(profile: str, *, front_matter: bool) -> MarkdownIt:
    """Разборщик объявленного профиля (кэш на профиль, разбор детерминирован).

    ``front_matter=False`` — вариант без YAML-метаданных: нужен, когда блок ``---…---``
    в начале документа не является YAML-отображением, и по CommonMark это setext-заголовок
    или тематический разрыв (см. :func:`parse_markdown`).
    """
    if profile not in PROFILES:
        raise ValueError(f"неизвестный профиль markdown: {profile!r}; объявлены {PROFILES}")
    key = (profile, front_matter)
    cached = _PARSERS.get(key)
    if cached is not None:
        return cached
    md = MarkdownIt("commonmark")
    if profile in ("gfm", "pandoc"):
        md = md.enable("table").enable("strikethrough")
        md = md.use(gfm_autolink_plugin).use(tasklists_plugin)
    if front_matter and profile != "commonmark":
        md = md.use(front_matter_plugin)
    if profile == "pandoc":
        # ``dollarmath`` — только здесь: в профилях страниц ``$$`` это SQL, а не формула.
        md = (
            md.use(footnote_plugin)
            .use(deflist_plugin)
            .use(attrs_plugin)
            .use(container_plugin, name="div")
            .use(colon_fence_plugin)
            .use(superscript_plugin)
            .use(dollarmath_plugin)
        )
    _PARSERS[key] = md
    return md


def _token_lines(token: Token) -> tuple[int, int] | None:
    """Диапазон строк токена; ``None``, если позиций нет (инлайновые и часть плагинов)."""
    span = token.map
    if not span or len(span) != 2:
        return None
    return int(span[0]), int(span[1])


def _heading_level(token: Token) -> int:
    tag = token.tag or ""
    return int(tag[1]) if len(tag) > 1 and tag[0] == "h" and tag[1].isdigit() else 0


def _align_of(token: Token) -> str | None:
    for name, value in (token.attrs or {}).items():
        if name == "style" and "text-align" in str(value):
            return str(value).split("text-align:", 1)[-1].strip().rstrip(";")
    return None


def _leaf_block(token: Token, kind: str, index: LineIndex) -> MdBlock | None:
    lines = _token_lines(token)
    if lines is None:
        return None
    info: dict[str, Any] = {}
    if kind == "fence":
        info["info"] = (token.info or "").strip()
        info["lang"] = info["info"].split(" ", 1)[0] if info["info"] else ""
        info["marker"] = token.markup
    elif kind == "code":
        info["indented"] = True
    elif kind == "html":
        info["raw"] = True
    elif kind == "math":
        info["delimiter"] = token.markup or "$$"
    elif kind == "front_matter":
        info["content"] = token.content or ""
    return MdBlock(kind=kind, span=index.span(*lines), info=info)


def _container_span(lines: tuple[int, int], index: LineIndex) -> tuple[int, int]:
    """Спан контейнера вместе с закрывающей строкой (``:::``).

    ``map`` токена открытия оканчивается **перед** закрывающей строкой: у ``container``
    закрытие не отдаётся токеном с картой, и строка ``:::`` оставалась разрывом покрытия.
    Расширяем спан на строку закрытия, только если она действительно маркер (``:`` в
    начале строки, отступ ≤ 3): иначе разрыв — не закрытие, и он обязан остаться видимым.
    """
    start, end = lines
    if end >= index.line_count:
        return lines
    closing = index.line_text(end).lstrip(" ")
    if len(index.line_text(end)) - len(closing) > 3 or not closing.startswith(":"):
        return lines
    return start, end + 1


def _finalize(frame: _Frame, index: LineIndex, label: Any = None) -> MdBlock | None:
    """Закрывает кадр: спан из токена открытия либо из детей (плагины без ``map``)."""
    lines = frame.lines
    if lines is None:
        if not frame.children:
            return None
        lines = (
            min(child.span.start_line for child in frame.children),
            max(child.span.end_line for child in frame.children),
        )
    info = dict(frame.info)
    if label is not None:
        info.setdefault("label", label)
    return MdBlock(
        kind=frame.kind,
        span=index.span(*lines),
        level=frame.level,
        info=info,
        children=tuple(frame.children),
    )


def _push(block: MdBlock | None, stack: list[_Frame], top: list[MdBlock]) -> None:
    if block is None:
        return
    if stack:
        stack[-1].children.append(block)
    else:
        top.append(block)


def _build_blocks(tokens: list[Token], index: LineIndex) -> list[MdBlock]:
    """Поток токенов → дерево блоков. Спаны — только из ``map``, без догадок."""
    top: list[MdBlock] = []
    stack: list[_Frame] = []
    footnote_labels: dict[int, Any] = {}

    for token in tokens:
        kind = token.type
        lines = _token_lines(token)

        if kind == "footnote_open":
            label = (token.meta or {}).get("label")
            frame = _Frame(
                kind="footnote_def", lines=None, close_type="footnote_close",
                info={"label": label},
            )
            footnote_labels[id(frame)] = label
            stack.append(frame)
            continue

        if kind.startswith("container_") and kind.endswith("_open"):
            name = kind[len("container_") : -len("_open")]
            stack.append(
                _Frame(
                    kind="container",
                    lines=lines,
                    close_type=kind[:-len("_open")] + "_close",
                    info={"name": name},
                )
            )
            continue

        if kind in _CONTAINER_OPEN:
            frame = _Frame(
                kind=_CONTAINER_OPEN[kind],
                lines=lines,
                close_type=kind[: -len("_open")] + "_close",
            )
            if frame.kind == "heading":
                frame.level = _heading_level(token)
            elif frame.kind == "list":
                frame.info["ordered"] = kind == "ordered_list_open"
                frame.info["marker"] = token.markup
                start = (token.attrs or {}).get("start")
                if start is not None:
                    frame.info["start"] = int(str(start))
            stack.append(frame)
            continue

        if kind in ("th_open", "td_open"):
            align = _align_of(token)
            for frame in reversed(stack):
                if frame.kind == "table":
                    if align is not None:
                        frame.info.setdefault("align", []).append(align)
                    elif "align" in frame.info:
                        frame.info["align"].append("")
                    break
            continue

        if kind == "inline":
            # Заголовку нужен его текст: ``heading_open`` его не несёт, а ``heading_path``
            # строится именно по нему (P11). Инлайновый токен идёт сразу за открытием.
            for frame in reversed(stack):
                if frame.kind != "heading":
                    break
                if "text" not in frame.info:
                    frame.info["text"] = token.content
                    frame.info["attrs"] = dict(token.attrs or {})
                    break
            continue

        if kind in _LEAF_BLOCKS:
            _push(_leaf_block(token, _LEAF_BLOCKS[kind], index), stack, top)
            continue

        if kind.endswith("_close"):
            if not stack or stack[-1].close_type != kind:
                # ``th_close``/``td_close`` и прочие закрытия, не открывавшие контейнер,
                # не трогают стек: иначе ячейка таблицы закрывала бы строку.
                continue
            frame = stack.pop()
            if frame.kind == "container" and frame.lines is not None:
                frame.lines = _container_span(frame.lines, index)
            label = footnote_labels.pop(id(frame), None)
            block = _finalize(frame, index, label)
            if block is not None and block.kind == "table_head":
                rows = sum(1 for child in block.children if child.kind == "table_row")
                for parent in reversed(stack):
                    if parent.kind == "table":
                        parent.info["header_rows"] = rows
                        break
            _push(block, stack, top)

    while stack:
        frame = stack.pop()
        _push(_finalize(frame, index, footnote_labels.pop(id(frame), None)), stack, top)

    return _sort_blocks(top)


def _sort_blocks(blocks: list[MdBlock]) -> list[MdBlock]:
    """Порядок документа по началу спана (стабильный, по строке и символу)."""
    return sorted(blocks, key=lambda block: (block.span.start_line, block.span.start_char))


# ---------------------------------------------------------------------------
# Покрытие строк (P13) и блоки, которые парсер не отдаёт токеном
# ---------------------------------------------------------------------------


def _covered_lines(blocks: list[MdBlock]) -> set[int]:
    covered: set[int] = set()
    for block in blocks:
        covered.update(range(block.span.start_line, block.span.end_line))
    return covered


def _skip_spaces(text: str, position: int) -> int:
    """Пропускает пробелы и табы (перевод строки — уже граница)."""
    while position < len(text) and text[position] in " \t":
        position += 1
    return position


def _at_line_end(text: str, position: int) -> bool:
    return position >= len(text) or text[position] == "\n"


def _reference_definition_end(text: str, position: int) -> int | None:
    """Конец одного определения ссылки от ``position`` или ``None`` (§4.7 CommonMark).

    Парсер таких определений **не отдаёт токеном** (они уходят в ``env.references``), и
    их приходится распознавать по исходнику. Одной регулярки здесь мало: метка может
    переноситься на следующие строки (``[Foo\n  bar]: /url``) и содержать экранированную
    скобку (``[Foo*bar\\]]: /url``), назначение и заголовок — тоже стоять на отдельных
    строках. Не распознанные определения не исчезают молча: они остаются разрывом
    покрытия (P13), поэтому «съесть» текст можно только по правилу, а не по догадке.
    """
    length = len(text)
    index = position
    while index < length and text[index] == " " and index - position < 3:
        index += 1
    if index < length and text[index] == " ":
        return None  # отступ ≥ 4 пробелов — блок кода, а не определение
    if index >= length or text[index] != "[":
        return None
    label_start = index + 1
    index = label_start
    label_length = 0
    while index < length:
        char = text[index]
        if char == "\\" and index + 1 < length:
            index += 2
            label_length += 2
            continue
        if char == "[":
            return None  # незакрытая квадратная скобка внутри метки
        if char == "]":
            break
        # Пустая строка внутри метки — метка закончилась, определения нет.
        if char == "\n" and _at_line_end(text, _skip_spaces(text, index + 1)):
            return None
        index += 1
        label_length += 1
        if label_length > _LABEL_MAX:
            return None
    if index >= length or text[index] != "]" or label_length == 0:
        return None
    if not text[label_start:index].strip():
        return None
    if index + 1 >= length or text[index + 1] != ":":
        return None
    index = _skip_spaces(text, index + 2)

    # Назначение: на этой строке либо на следующей (но не через пустую строку).
    if _at_line_end(text, index):
        index += 1
        index = _skip_spaces(text, index)
        if _at_line_end(text, index):
            return None
    if index >= length:
        return None
    if text[index] == "<":
        close = text.find(">", index + 1)
        if close < 0 or "\n" in text[index + 1 : close]:
            return None
        index = close + 1
    else:
        start = index
        while index < length and not text[index].isspace():
            index += 1
        if index == start:
            return None
    index = _skip_spaces(text, index)

    # Заголовок: необязателен, может начинаться на следующей строке.
    title_index = index
    if _at_line_end(text, title_index) and title_index < length:
        title_index = _skip_spaces(text, title_index + 1)
    if title_index < length and text[title_index] in _TITLE_CLOSERS:
        opener = text[title_index]
        closer = _TITLE_CLOSERS[opener]
        cursor = title_index + 1
        while cursor < length:
            char = text[cursor]
            if char == "\\" and cursor + 1 < length:
                cursor += 2
                continue
            if char == closer:
                break
            cursor += 1
        if cursor >= length:
            return None
        index = _skip_spaces(text, cursor + 1)
    # Определение занимает целые строки: мусор после назначения/заголовка — уже не
    # определение, и «съесть» эту строку значило бы потерять текст молча (P13).
    if not _at_line_end(text, index):
        return None
    return index + 1 if index < length else index


def _is_reference_gap(texts: list[str]) -> bool:
    """Состоит ли разрыв целиком из определений ссылок (возможно, нескольких).

    Частичное совпадение определением **не** считается: иначе правило съело бы строку,
    которую парсер считает содержанием, и P13 перестал бы быть сигналом.
    """
    # ``LineIndex.line_text`` отдаёт строку вместе с переводом строки: без снятия хвоста
    # ``join`` дал бы пустую строку между определениями, а она по правилу рвёт метку.
    text = "\n".join(line.rstrip("\n") for line in texts)
    position = 0
    found = 0
    while position < len(text):
        end = _reference_definition_end(text, position)
        if end is None or end <= position:
            return False
        position = end
        found += 1
    return found > 0


def _gap_blocks(
    index: LineIndex, gaps: list[tuple[int, int]]
) -> tuple[list[MdBlock], list[MdSpan], int]:
    """Классифицирует разрывы: определения ссылок и метки сносок — блоки, прочее — дыры.

    Reference definitions парсер вырезает молча (проверено: на ``[x]: http://y`` токена
    нет), поэтому без этой проверки P13 падала бы на любом документе со ссылками.
    Метка сноски на отдельной строке (``[^1]:`` без текста) — тот же случай: у
    ``footnote_open`` нет ``map``, и строка метки не попадает в спан детей.
    """
    extras: list[MdBlock] = []
    unknown: list[MdSpan] = []
    unknown_lines = 0
    for start, end in gaps:
        texts = [index.line_text(line) for line in range(start, end)]
        is_reference = _is_reference_gap(texts)
        is_label = len(texts) == 1 and _FOOTNOTE_LABEL_RE.match(texts[0]) is not None
        if is_reference or is_label:
            kind = "reference" if is_reference else "footnote_label"
            extras.append(MdBlock(kind=kind, span=index.span(start, end), info={"lines": texts}))
        else:
            unknown.append(index.span(start, end))
            unknown_lines += len(texts)
    return extras, unknown, unknown_lines


def _find_gaps(blocks: list[MdBlock], index: LineIndex) -> list[tuple[int, int]]:
    """Непрерывные непустые отрезки строк, не покрытые ни одним блоком верхнего уровня."""
    covered = _covered_lines(blocks)
    gaps: list[tuple[int, int]] = []
    start: int | None = None
    for line in range(index.line_count):
        blank = not index.line_text(line).strip()
        if line in covered or blank:
            if start is not None:
                gaps.append((start, line))
                start = None
            continue
        if start is None:
            start = line
    if start is not None:
        gaps.append((start, index.line_count))
    return gaps


# ---------------------------------------------------------------------------
# Публичный разбор
# ---------------------------------------------------------------------------


def _is_mapping(content: str) -> bool:
    """Есть ли в теле YAML-метаданных запись-отображение (``ключ: значение``)."""
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" in stripped:
            return True
    return False


def parse_markdown(text: str, profile: str = "gfm") -> MdDocument:
    """Разбирает markdown объявленного профиля в блоки со спанами.

    ``MdDocument.text`` — **нормализованный** текст (без BOM, ``\\n``), и все спаны
    относятся к нему. Если нормализация что-то изменила, ``normalized=True``: срез
    обязан браться из ``MdDocument.text``, а не из переданной строки.

    **YAML-метаданные — только когда это отображение.** Блок ``--- … ---`` в начале
    документа по Pandoc — метаданные, по CommonMark/GFM — setext-заголовок или разрыв.
    Профиль решает не всё: страница, начинающаяся с горизонтальной черты и закрывающей
    черты ниже, иначе теряла бы заголовок, а её текст уезжал бы в «front matter». Поэтому
    блок принимается только при наличии записи-отображения (строка с ``:``) — ровно то
    правило, что раньше стояло в строчной эвристике чанкера. Если отображения нет,
    документ разбирается без плагина, то есть по CommonMark.

    **Правила профиля ``pandoc``** (семантика пустой строки, line block, fancy/example
    списки) применяются после разбора и до поиска разрывов покрытия: они меняют вид
    блоков, но не текст и не объединение спанов, поэтому P13 считается по ним же.
    Импорт ленивый — иначе ``md_dialect`` и ``md_ast`` образовали бы цикл.
    """
    normalized_text, changed = normalize_text(text)
    index = LineIndex(normalized_text)
    blocks = _build_blocks(
        _parser(profile, front_matter=True).parse(normalized_text), index
    )
    if blocks and blocks[0].kind == "front_matter" and not _is_mapping(
        blocks[0].info.get("content", "")
    ):
        blocks = _build_blocks(
            _parser(profile, front_matter=False).parse(normalized_text), index
        )
    if profile == "pandoc":
        from core.adapters.md_dialect import apply_rules  # noqa: PLC0415

        blocks = apply_rules(blocks, index)

    extras, unknown, unknown_lines = _gap_blocks(index, _find_gaps(blocks, index))
    if extras:
        blocks = _sort_blocks([*blocks, *extras])

    counts: dict[str, int] = {
        "lines": index.line_count,
        "chars": len(normalized_text),
        "blocks": len(blocks),
        "gaps": len(unknown),
        "unknown_lines": unknown_lines,
    }
    for block in blocks:
        for item in block.walk():
            counts[item.kind] = counts.get(item.kind, 0) + 1

    return MdDocument(
        text=normalized_text,
        profile=profile,
        blocks=tuple(blocks),
        gaps=tuple(unknown),
        counts=counts,
        normalized=changed,
    )


# ---------------------------------------------------------------------------
# Безопасные позиции разреза
# ---------------------------------------------------------------------------


def _protected_ranges(text: str, start: int, end: int, *, math: bool) -> list[tuple[int, int]]:
    """Диапазоны, внутри которых разрез запрещён: код, ссылки, теги, формулы, сноски.

    Сканер идёт по **сырому срезу** источника, поэтому позиции точны и не зависят от
    того, что markdown-it срезал ``>``-префиксы контейнеров перед инлайн-разбором.
    """
    ranges: list[tuple[int, int]] = []
    position = start
    while position < end:
        char = text[position]
        if char == "\\":
            position += 2
            continue
        if char == "`":
            run = 0
            while position + run < end and text[position + run] == "`":
                run += 1
            closing = text.find("`" * run, position + run, end)
            if closing == -1:
                position += run
                continue
            ranges.append((position, closing + run))
            position = closing + run
            continue
        if char == "<":
            match = re.match(r"<[A-Za-z/!?][^>\n]*>", text[position:end])
            if match:
                ranges.append((position, position + match.end()))
                position += match.end()
                continue
        if char == "]" and position + 1 < end and text[position + 1] == "(":
            depth = 0
            cursor = position + 1
            while cursor < end:
                if text[cursor] == "(":
                    depth += 1
                elif text[cursor] == ")":
                    depth -= 1
                    if depth == 0:
                        ranges.append((position, cursor + 1))
                        cursor += 1
                        break
                cursor += 1
            position = max(cursor, position + 1)
            continue
        if math and char == "$":
            delimiter = "$$" if text.startswith("$$", position) else "$"
            closing = text.find(delimiter, position + len(delimiter), end)
            if closing != -1:
                ranges.append((position, closing + len(delimiter)))
                position = closing + len(delimiter)
                continue
        if char == "[" and position + 1 < end and text[position + 1] in "^@":
            closing = text.find("]", position + 2, end)
            if closing != -1:
                ranges.append((position, closing + 1))
                position = closing + 1
                continue
        if char == "{" and position > start and text[position - 1] in "] ":
            closing = text.find("}", position + 1, end)
            if closing != -1 and "\n" not in text[position:closing]:
                ranges.append((position, closing + 1))
                position = closing + 1
                continue
        position += 1
    return ranges


def safe_cut_offsets(text: str, start: int, end: int, *, math: bool = False) -> tuple[int, ...]:
    """Позиции, где разрез не рвёт конструкцию и не встаёт посреди слова/кластера.

    Кандидаты — пробельные позиции вне защищённых диапазонов (код-спаны, ссылки,
    inline-html, формулы, сноски/цитаты, атрибутные блоки). Концы предложений —
    приоритетные кандидаты, но правило то же: они не внутри защищённого диапазона.

    Функция **не** решает, где резать: она отдаёт множество допустимых позиций, а политика
    (L1b) выбирает из них по бюджету. Детерминирована, без состояния.
    """
    if not 0 <= start < end <= len(text):
        raise ValueError(f"некорректный диапазон: {start}..{end} при длине {len(text)}")
    protected = _protected_ranges(text, start, end, math=math)

    def inside(position: int) -> bool:
        return any(lower < position < upper for lower, upper in protected)

    cuts: list[int] = []
    for position in range(start + 1, end):
        left, right = text[position - 1], text[position]
        # Граница — после пробела или после конца предложения; сам разделитель остаётся
        # в предыдущем куске, поэтому ``join`` кусков равен входу без потери символов.
        if not (left.isspace() or left in SENTENCE_END_CHARS):
            continue
        if right.isspace() or right in SENTENCE_END_CHARS:
            continue
        if inside(position) or not is_cut_boundary(text, position):
            continue
        cuts.append(position)
    return tuple(cuts)