"""Генеративная приёмка markdown-чанкера: свойства на случайных документах (план 18 §3/§7).

Зачем третий файл после ``test_chunker_markdown.py`` (пины политики) и
``test_chunker_constructs.py`` (34 фиксированные формы): пины проверяют формы, которые
кто-то придумал, а приёмка обязана проверить формы, которые никто не придумывал. Здесь
документ собирает hypothesis из грамматики markdown — контейнеры, блоки, текст, переводы
строк, BOM и zero-width — и на каждом документе проверяются свойства:

* **P1** — ``"".join(chunk.text)`` равен **нормализованному** входу, точное равенство без
  ``strip()``: ни один символ не потерян и не придуман;
* **P4** — каждый ``chunk.text`` непуст, не состоит из одних пробелов и является
  подстрокой нормализованного входа;
* **P13** — ``unknown_lines == 0``: каждая непустая строка попала в спан какого-то блока;
* **тотальность** — вызов завершается (``deadline=None``) и число кусков не больше числа
  символов;
* **контекст не выдумывает строк** — каждая строка ``context_prefix`` либо строка
  документа, либо маркер контейнера/пункта без содержимого, либо ``heading_path``;
* **heading_path** — последний компонент встречается в документе как текст заголовка.

Инварианты считаются по тексту и его геометрии, а не по повторному разбору AST: иначе
проверка подтверждала бы ошибку чанкера вместо того, чтобы её поймать (тот же принцип,
что в ``test_chunker_constructs.py``).

**Детерминизм.** ``derandomize=True`` обязателен: набор примеров фиксирован, иначе CI
падал бы «иногда» на другом наборе — падение, которое нельзя воспроизвести, не сигнал,
а шум. Seed выводится из самой тестовой функции (``hypothesis.core.get_random_for_wrapped_test``
→ ``function_digest``), поэтому у каждого свойства **свой** фиксированный корпус, а не
общий: прогон воспроизводим побайтно, но два свойства не проверяют одни и те же
документы. Стратегии построены один раз на модуль (словари профилей): генерация не должна
стоить дороже проверяемого кода.

**Размеры.** Целевой размер документа объявлен (:data:`_SIZE_TARGETS`): от 10–50
символов до 12 тыс., чтобы бюджет реально переполнялся, а не «когда сгенерируется».
Документ строится **под бюджет** — ``chunk_size`` известен до сборки, поэтому последняя
секция получает хвост, на котором срабатывает ``_absorb_tail`` (:func:`_tail_pair`).
Верхний класс длины — детерминированный пример :func:`_build_big_document` на ≈42 тыс.
символов, он приходит в каждое свойство через ``@example``.

**Профили.** Основной набор — ``gfm`` (страницы и офисные документы). Отдельный меньший
набор — ``pandoc``: там включается ``dollarmath``, поэтому в генератор добавлены
`$x$`/`$$…$$` (в ``gfm`` ``$$`` — это SQL-доллар-квотинг, а не формула, и математика в
основной набор не входит).

Нейтрален (§1.3).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Final

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from core.adapters.chunker_markdown import MarkdownChunker
from core.adapters.md_ast import normalize_text, parse_markdown
from core.ports.types import Chunk

#: Бюджеты куска: от «конструкция гарантированно длиннее бюджета» до «почти любой
#: документ влезает целиком».
CHUNK_SIZES: Final = (200, 400, 800, 1500, 3000, 8000)

#: Пороги хвоста. Все меньше минимального бюджета (200), поэтому конструктор не откажет.
MIN_CHUNK_SIZES: Final = (0, 20, 60)

#: Сколько примеров на свойство. У P1 корпус шире: потеря или добавление символа — тот
#: дефект, контрпример к которому ищется труднее всего (он тонет в шуме структуры).
#: Остальные свойства проверяются на меньшем, но всё равно детерминированном корпусе;
#: корпус ``pandoc`` меньше основного втрое — там проверяется сам факт работы профиля.
GFM_EXAMPLES: Final = 30
GFM_NARROW_EXAMPLES: Final = 16
PANDOC_EXAMPLES: Final = 10
PANDOC_NARROW_EXAMPLES: Final = 8


def _make_settings(examples: int) -> settings:
    """Настройки набора примеров: дедлайн снят — нарезка 40-килобайтного документа не
    «медленный тест», а его содержание; размер и время примеров объявлены заранее и не
    лечатся урезанием. ``derandomize`` обязателен: набор примеров фиксирован, иначе CI
    падал бы «иногда» на другом наборе примеров.
    """
    return settings(
        max_examples=examples,
        deadline=None,
        derandomize=True,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
    )


_SETTINGS: Final = _make_settings(GFM_EXAMPLES)
_SETTINGS_NARROW: Final = _make_settings(GFM_NARROW_EXAMPLES)
_SETTINGS_PANDOC: Final = _make_settings(PANDOC_EXAMPLES)
_SETTINGS_PANDOC_NARROW: Final = _make_settings(PANDOC_NARROW_EXAMPLES)

# ---------------------------------------------------------------------------
# Текст: пулы строк
# ---------------------------------------------------------------------------

_CYRILLIC_WORDS: Final = (
    "значение",
    "строка",
    "заголовок",
    "пункт",
    "цитата",
    "таблица",
    "раздел",
    "описание",
    "проверка",
    "свойство",
    "документ",
    "чанкер",
    "граница",
    "контекст",
    "переполнение",
    "бюджет",
)

_LATIN_WORDS: Final = (
    "value",
    "chunk",
    "heading",
    "table",
    "fence",
    "provider",
    "note",
    "row",
    "section",
    "budget",
    "offset",
    "stream",
    "policy",
    "source",
)

_CJK_WORDS: Final = ("漢字", "仮名", "中文", "測試", "日本語", "交じり文")

#: Пунктуация внутри токена: точка/запятая/двоеточие в том числе CJK-набором.
_PUNCTUATED: Final = (
    "слово,",
    "фраза.",
    "вопрос?",
    "восклицание!",
    "перечисление;",
    "двоеточие:",
    "многоточие…",
    "句号。",
    "顿号、",
    "冒号：",
)

#: Длинные слова без пробелов: внутри них обязан найтись жёсткий разрез.
_LONG_WORDS: Final = (
    "сверхдлинноесловобезпробеловсверхдлинноесловобезпробелов",
    "identifier_without_any_spaces_but_long_enough_to_overflow_the_budget",
    "аааааааааааааааааааааааааааааааааааааааааааааааааааааааааа",
    "字字字字字字字字字字字字字字字字字字字字字字字字字字字字字字",
)

#: Zero-width символы: не пробелы для ``str.strip``, но и не текст для читателя.
_ZERO_WIDTH_WORDS: Final = ("сло\u200bво", "текст\u200dтекст", "\u200b", "а\u2060б", "字\u200c文")

#: Инлайн-разметка: код-спаны, ссылки, HTML, зачёркивание — защищённые диапазоны сканера.
_INLINE_MARKUP: Final = (
    "**жирный**",
    "_курсив_",
    "`код`",
    "~~зачёркнуто~~",
    "[ссылка](https://example.com/a)",
    "[док](https://пример.рф/x)",
    "<b>тег</b>",
    "<code>x=1</code>",
)

#: Формулы — только ``pandoc``: в ``gfm`` ``$$`` это доллар-квотинг SQL.
_MATH_TOKENS: Final = ("$x$", "$a_1 + b^2$", "$$x^2$$", "$$\\frac{a}{b}$$")


def _build_line_pool(*, math: bool) -> tuple[str, ...]:
    """Пул готовых строк текста: смесь алфавитов, табы, long-word, zero-width, разметка.

    Строка — единица рисования (одно решение hypothesis), а не восемь токенов: генерация
    не должна быть дороже проверяемого разбора. Разнообразие обеспечивает не число
    draw-вызовов, а ширина пула: 200+ строк, собранных из всех алфавитов и форм.
    """
    lines: list[str] = []
    for cyrillic in _CYRILLIC_WORDS[:8]:
        for latin in _LATIN_WORDS[:5]:
            lines.append(f"{cyrillic} {latin}")
            lines.append(f"{cyrillic}, {latin}.")
            lines.append(f"{cyrillic}\t{latin}")
            lines.append(f"{cyrillic}{latin}")
            lines.append(f"{cyrillic} {latin}: {_CYRILLIC_WORDS[1]}?")
    for cjk in _CJK_WORDS:
        lines.append(f"{cjk}{_CYRILLIC_WORDS[0]}")
        lines.append(f"{cjk}。{_LATIN_WORDS[0]}、")
    for punct in _PUNCTUATED:
        lines.append(f"{_CYRILLIC_WORDS[1]} {punct}")
        lines.append(punct)
    for long_word in _LONG_WORDS:
        lines.append(long_word)
        lines.append(f"{_CYRILLIC_WORDS[2]} {long_word} {_LATIN_WORDS[1]}")
    lines.extend(_ZERO_WIDTH_WORDS)
    lines.extend(_INLINE_MARKUP)
    lines.append(" ".join(_CYRILLIC_WORDS))
    lines.append("\t".join(_LATIN_WORDS[:4]))
    if math:
        lines.extend(_MATH_TOKENS)
        lines.append(f"формула {_MATH_TOKENS[0]} и {_MATH_TOKENS[1]}")
    return tuple(lines)


_LINE_POOL: Final = _build_line_pool(math=False)
_LINE_POOL_MATH: Final = _build_line_pool(math=True)
_LINE = st.sampled_from(_LINE_POOL)
_LINE_MATH = st.sampled_from(_LINE_POOL_MATH)

#: Тело кода: без обратных кавычек и тильд (иначе тело закрыло бы свой fence) и без
#: переводов строк. Две строки длиннее 200 символов — они заставляют жёсткий разрез.
_CODE_POOL: Final = (
    "SELECT 1;",
    "select * from t where x = 1;",
    "python -m pytest -q",
    "x = {'a': 1}",
    "def f(x): return x + 1",
    "if x: y()",
    "// комментарий",
    "# хеш-комментарий",
    "    отступ внутри тела",
    "\tтаб в коде",
    "длинная_строка_кода_без_пробелов_которая_переполняет_бюджет_чанкера_значения",
    "0123456789" * 22,
)

_CODE_LINE = st.sampled_from(_CODE_POOL)

#: Ячейка таблицы: без ``|`` и переводов строк — иначе таблица развалится.
_CELL = st.sampled_from((*_CYRILLIC_WORDS, *_LATIN_WORDS, *_CJK_WORDS, *_PUNCTUATED[:4], "\u200b", "1", "2"))


def _text_line(*, math: bool) -> st.SearchStrategy[str]:
    """Строка текста из пула: смесь алфавитов, табы, длинные слова, zero-width."""
    return _LINE_MATH if math else _LINE


# ---------------------------------------------------------------------------
# Блоки
# ---------------------------------------------------------------------------


@st.composite
def _heading(draw: st.DrawFn, *, math: bool) -> str:
    """ATX-заголовок 1–6 уровня, иногда с закрывающими решётками."""
    level = draw(st.integers(min_value=1, max_value=6))
    title = draw(_text_line(math=math))
    closing = draw(st.sampled_from(("", "", "", f" {'#' * level}")))
    return f"{'#' * level} {title}{closing}"


@st.composite
def _paragraph(draw: st.DrawFn, *, math: bool, long: bool) -> str:
    """Абзац с переносами строк внутри (soft break)."""
    rows = draw(st.integers(min_value=1, max_value=14 if long else 4))
    return "\n".join(draw(_text_line(math=math)) for _ in range(rows))


@st.composite
def _table(draw: st.DrawFn, *, long: bool) -> str:
    """GFM pipe-таблица: 1–4 колонки, любое выравнивание (в т.ч. ``|:-|-:|``)."""
    columns = draw(st.integers(min_value=1, max_value=4))
    align = st.sampled_from((":--", "--:", ":-:", "---", ":-", "-"))
    aligns = [draw(align) for _ in range(columns)]
    header = [draw(_CELL) for _ in range(columns)]
    rows = [
        [draw(_CELL) for _ in range(columns)]
        for _ in range(draw(st.integers(min_value=1, max_value=30 if long else 3)))
    ]

    def row(cells: list[str]) -> str:
        return "| " + " | ".join(cells) + " |"

    return "\n".join([row(header), "|" + "|".join(aligns) + "|", *(row(cells) for cells in rows)])


@st.composite
def _fence(draw: st.DrawFn, *, long: bool) -> str:
    """Fence ``` или ~~~ с info string."""
    marker = draw(st.sampled_from(("```", "```", "~~~", "~~~~")))
    info = draw(st.sampled_from(("", "", "sql", "python title=app.py", "text", "json", "c++")))
    rows = draw(st.integers(min_value=1, max_value=30 if long else 3))
    body = [draw(_CODE_LINE) for _ in range(rows)]
    return "\n".join([f"{marker}{info}", *body, marker])


@st.composite
def _indented_code(draw: st.DrawFn, *, long: bool) -> str:
    """Indented code: четыре пробела и тело."""
    rows = draw(st.integers(min_value=1, max_value=30 if long else 3))
    return "\n".join(f"    {draw(_CODE_LINE)}" for _ in range(rows))


@st.composite
def _html_block(draw: st.DrawFn, *, math: bool) -> str:
    """HTML-блок: div, комментарий, details, таблица, span."""
    inner = draw(_text_line(math=math))
    template = draw(
        st.sampled_from(
            (
                '<div class="note">\n{inner}\n</div>',
                "<!-- {inner} -->",
                "<details><summary>{inner}</summary>\n{inner}\n</details>",
                "<table>\n<tr><td>{inner}</td></tr>\n</table>",
                '<span class="x">{inner}</span>',
            )
        )
    )
    return template.replace("{inner}", inner)


@st.composite
def _reference(draw: st.DrawFn) -> str:
    """Определение ссылки: парсер вырезает его молча — P13 обязан это пережить."""
    label = draw(st.sampled_from(_LATIN_WORDS))
    tail = draw(st.sampled_from(_CYRILLIC_WORDS))
    title = draw(st.sampled_from(("", "", ' "Заголовок"', " 'Title'", " (в скобках)")))
    host = draw(st.sampled_from(("https://example.com", "https://пример.рф", "http://docs.example.org")))
    return f"[{label}]: {host}/{tail}{title}"


_MARKER = st.one_of(
    st.sampled_from(("- ", "* ", "+ ")),
    st.builds(
        lambda number, dot: f"{number}{dot} ",
        st.integers(min_value=1, max_value=12),
        st.sampled_from((".", ")")),
    ),
    st.builds(
        lambda bullet, box: f"{bullet} {box} ",
        st.sampled_from(("-", "*")),
        st.sampled_from(("[ ]", "[x]", "[X]")),
    ),
)


@st.composite
def _list_tree(draw: st.DrawFn, *, math: bool, nesting: int) -> tuple[str, str, tuple]:
    """Пункт списка: маркер, текст и вложенные пункты не глубже ``nesting``."""
    marker = draw(_MARKER)
    text = draw(_text_line(math=math))
    children: list[tuple[str, str, tuple]] = []
    if nesting > 0 and draw(st.booleans()):
        for _ in range(draw(st.integers(min_value=1, max_value=3))):
            children.append(draw(_list_tree(math=math, nesting=nesting - 1)))
    return marker, text, tuple(children)


def _render_list(tree: tuple[str, str, tuple], indent: int = 0) -> list[str]:
    """Рендер пункта: дети сдвинуты на ширину маркера родителя (вложенность видна)."""
    marker, text, children = tree
    lines = [" " * indent + marker + text]
    for child in children:
        lines.extend(_render_list(child, indent + len(marker)))
    return lines


@st.composite
def _list_block(draw: st.DrawFn, *, math: bool, long: bool) -> str:
    """Список: 1–8 пунктов, вложенность 0–2, task list через маркер."""
    nesting = draw(st.integers(min_value=0, max_value=2))
    items = draw(st.integers(min_value=1, max_value=8 if long else 4))
    trees = [draw(_list_tree(math=math, nesting=nesting)) for _ in range(items)]
    lines: list[str] = []
    for tree in trees:
        lines.extend(_render_list(tree))
    return "\n".join(lines)


def _quote_lines(text: str, depth: int) -> str:
    """Префикс цитаты на каждой строке; пустая строка цитаты — сам маркер."""
    prefix = "> " * depth
    marker = ">" * depth
    return "\n".join(prefix + line if line.strip() else marker for line in text.split("\n"))


@st.composite
def _quote(draw: st.DrawFn, *, math: bool, long: bool) -> str:
    """Цитата глубины 1–3 с абзацем, списком, таблицей или fence внутри."""
    depth = draw(st.integers(min_value=1, max_value=3))
    inner = draw(
        st.one_of(
            _paragraph(math=math, long=long),
            _list_block(math=math, long=long),
            _table(long=long),
            _fence(long=long),
        )
    )
    return _quote_lines(inner, depth)


@st.composite
def _list_with_embedded_block(draw: st.DrawFn, *, long: bool) -> str:
    """Пункт списка, внутри которого таблица или fence.

    Именно здесь ``_with_prefix`` получает и маркер пункта, и сырую строку таблицы:
    продолжение такого куска обязано нести шапку таблицы (``> `` уже в сырой строке),
    не склеиваясь с маркером. Без этой формы свойство контекста проверяло бы только
    цитаты, где сырая строка уже начинается с префикса.
    """
    marker = draw(st.sampled_from(("- ", "* ", "1. ", "> - ")))
    inner = draw(st.one_of(_table(long=long), _fence(long=long)))
    body = "\n".join(("  " + line if line.strip() else "") for line in inner.split("\n"))
    return f"{marker}пункт с вложенной конструкцией:\n\n{body}"


_MATH_BLOCK = st.sampled_from(("$$\nx + y\n$$", "$$\\sum_{i=0}^{n} i$$", "$$\n\\frac{a}{b}\n$$"))


def _block_catalogue(profile: str, *, long: bool) -> st.SearchStrategy[str]:
    """Блок документа. ``paragraph`` повторён: он и есть основной носитель текста."""
    math = profile == "pandoc"
    catalogue = [
        _heading(math=math),
        _paragraph(math=math, long=long),
        _paragraph(math=math, long=long),
        _table(long=long),
        _fence(long=long),
        _indented_code(long=long),
        st.sampled_from(("---", "***", "___", "- - -", "* * *")),
        _html_block(math=math),
        _reference(),
        _list_block(math=math, long=long),
        _list_with_embedded_block(long=long),
        _quote(math=math, long=long),
    ]
    if math:
        catalogue.append(_MATH_BLOCK)
    return st.one_of(*catalogue)


#: Короткий документ: 10–50 символов, чтобы класс «совсем мелких» был реально покрыт.
_TINY_TEXT = st.builds(
    lambda words: " ".join(words) + ".",
    st.lists(st.sampled_from((*_CYRILLIC_WORDS[:6], *_LATIN_WORDS[:6])), min_size=2, max_size=4),
)


#: Короткий блок: текст, заголовок или однострочный контейнер (одинаково для профилей:
#: короткому документу профильная разметка не нужна — её проверяют средние и длинные).
_SHORT_BLOCKS: Final = st.one_of(
    _TINY_TEXT,
    st.builds(lambda title: f"# {title}", _TINY_TEXT),
    st.builds(lambda marker, text: f"{marker}{text}", st.sampled_from(("- ", "1. ", "> ")), _TINY_TEXT),
)

#: Стратегии строились бы заново на каждом draw — это дороже самого разбора документа,
#: поэтому каталоги собраны один раз на модуль.
_BLOCKS: Final = {
    (profile, long): _block_catalogue(profile, long=long)
    for profile in ("gfm", "pandoc")
    for long in (False, True)
}


def _crlf(text: str, mode: str, seed: int) -> str:
    """Переводы строк: только LF, только CRLF или вперемешку."""
    if mode == "lf":
        return text
    if mode == "crlf":
        return text.replace("\n", "\r\n")
    parts = text.split("\n")
    out = [parts[0]]
    for number, part in enumerate(parts[1:]):
        out.append("\r\n" if (number + seed) % 3 == 0 else "\n")
        out.append(part)
    return "".join(out)


#: Целевые размеры документа (символы): от 10–50 символов до 12 тыс. Размер **объявлен**,
#: а не «как получится»: иначе бюджет ``chunk_size`` переполняется только на удачных
#: примерах. Повторы в начале списка — чтобы мелкие документы оставались частыми (они
#: дешевле и лучше шринкуются). Класс ≈42 тыс. сюда не входит сознательно: он стоит
#: ~0.1 с на документ, а его доля в случайной выборке делала время файла
#: непредсказуемым (замер: 6–9 с при одном и том же коде). Экстремальная длина
#: проверяется пин-примером ``_build_big_document`` в каждом свойстве.
_SIZE_TARGETS: Final = (30, 30, 300, 300, 2000, 2000, 12000, 12000)

#: Потолок числа блоков: страховка от петли «target не достигнут», а не бюджет.
_MAX_BLOCKS: Final = 200

#: До какого бюджета случайный документ получает подогнанный хвост: цена хвоста —
#: ``chunk_size`` символов на документ, поэтому крупные бюджеты отданы пин-тесту.
_TAIL_MAX_BUDGET: Final = 1500

#: Наполнитель хвостового абзаца: длина подгоняется под бюджет (см. :func:`_tail_pair`).
_TAIL_FILL: Final = "значение строки и границы текста "


def _tail_pair(chunk_size: int, tail: str = "хвост") -> str:
    """Хвост документа: абзац впритык к бюджету плюс короткая единица после него.

    Зачем подгонка под бюджет, а не случайный последний блок. ``_absorb_tail``
    приклеивает хвост короче ``min_chunk_size`` к предыдущему куску — это единственный
    путь, где бюджет **сознательно** превышается, и потеря символа в нём невидима для
    всех остальных проверок. На случайных документах этот путь почти не встречается —
    замер **до** этой подгонки: корпус 36 документов, 72 вызова, 0 склеек, потому что
    последний кусок обычно длиннее порога. Здесь абзац длиной ``chunk_size - 3`` гарантирует, что
    ближайшая граница — начало короткой единицы ровно на краю бюджета, то есть последний
    кусок секции равен ``tail`` (5 символов < любого порога из ``MIN_CHUNK_SIZES``).
    """
    fill = (_TAIL_FILL * (chunk_size // len(_TAIL_FILL) + 1))[: chunk_size - 3]
    return f"{fill}\n\n{tail}"


@st.composite
def _document(draw: st.DrawFn, *, profile: str, chunk_size: int) -> str:
    """Документ целиком: целевой размер, блоки, пустые строки, BOM и CRLF.

    Документ строится **под бюджет**: ``chunk_size`` известен до сборки, поэтому
    последняя секция получает хвост, на котором срабатывает ``_absorb_tail``.
    """
    target = draw(st.sampled_from(_SIZE_TARGETS))
    if target <= 50:
        blocks = [draw(_SHORT_BLOCKS) for _ in range(draw(st.integers(min_value=1, max_value=2)))]
    else:
        catalogue = _BLOCKS[(profile, target >= 2000)]
        blocks = []
        length = 0
        while length < target and len(blocks) < _MAX_BLOCKS:
            block = draw(catalogue)
            blocks.append(block)
            length += len(block) + 2  # +2 — разделитель блоков ниже
        if chunk_size <= _TAIL_MAX_BUDGET and target > chunk_size:
            # Хвост имеет смысл только там, где документ длиннее бюджета: секция короче
            # бюджета и так режется одним куском, склеивать нечего. Условие заодно не
            # раздувает короткие документы на chunk_size символов. Крупные бюджеты в
            # случайный корпус не входят: хвост там стоил бы 3–8 тыс. символов на
            # документ, а достижимость склейки для них проверяет отдельный
            # test_tail_merge_path_is_reachable_and_lossless.
            blocks.append(_tail_pair(chunk_size))

    parts: list[str] = []
    for number, block in enumerate(blocks):
        if number:
            # Пустые строки пачками: от одной до пяти.
            parts.append("\n" * draw(st.integers(min_value=2, max_value=6)))
        parts.append(block)
    text = "".join(parts)
    if draw(st.sampled_from((False, False, False, True))):
        text = "\ufeff" + text
    mode = draw(st.sampled_from(("lf", "lf", "crlf", "mixed")))
    return _crlf(text, mode, draw(st.integers(min_value=0, max_value=2)))


@st.composite
def _case(draw: st.DrawFn, *, profile: str) -> tuple[str, int, int]:
    """Пример: бюджет нарезки и документ, собранный под этот бюджет.

    Порядок важен: ``chunk_size`` рисуется **до** документа, иначе генератор не может
    подогнать хвост под бюджет. ``min_chunk_size < chunk_size`` выполняется всегда:
    максимальный порог (60) меньше минимального бюджета (200).
    """
    chunk_size = draw(st.sampled_from(CHUNK_SIZES))
    min_chunk_size = draw(st.sampled_from(MIN_CHUNK_SIZES))
    document = draw(_document(profile=profile, chunk_size=chunk_size))
    return document, chunk_size, min_chunk_size


# ---------------------------------------------------------------------------
# Детерминированный длинный документ (≈40 тыс. символов)
# ---------------------------------------------------------------------------


def _build_big_document(*, math: bool) -> str:
    """Документ из всех семейств конструкций, заведомо длиннее любого бюджета.

    Это не «повезло сгенерировать»: класс длины 40 тыс. обязан проверяться в каждом
    прогоне, поэтому он собран явно и передаётся через ``@example``. Содержимое —
    те же конструкции, что и в стратегиях: цитаты с вложенными списками, таблицы с
    однодефисным разделителем, fence с info string, indented code, HTML-блок,
    определения ссылок, заголовки 1–6, длинные слова без пробелов, CJK и zero-width.
    """
    long_word = _LONG_WORDS[1]
    paragraph = "\n".join(f"Строка {row}: значение {row}, вопрос {row}? {long_word[:40]}." for row in range(20))
    table = "\n".join(
        ["| колонка | вторая | третья | четвёртая |", "|:-|-:|:-:|---|"]
        + [f"| значение {row} | 值 {row} | note {row} | строка {row} |" for row in range(30)]
    )
    fence_body = "\n".join(f"SELECT {row} FROM таблица WHERE id = {row};" for row in range(30))
    indented = "\n".join(f"    print({row})  # комментарий к строке {row}" for row in range(20))
    items = "\n".join(f"- [x] пункт {row} со значением {row}" for row in range(12))
    nested = "- внешний пункт\n  - вложенный пункт\n    1. третий уровень\n    2. ещё один"
    quoted = _quote_lines(paragraph, 2)
    html = '<div class="note">\n<b>внимание</b>: значение\n</div>'
    references = "[docs]: https://example.com/docs \"Документация\"\n[api]: https://пример.рф/api"
    section = "\n\n".join(
        ["# Раздел", paragraph, "## Подраздел", table, f"```sql\n{fence_body}\n```", indented, items, nested, quoted, html, references, "---", paragraph]
    )
    parts = [section for _ in range(5)]
    if math:
        parts.append("$$\n\\sum_{i=0}^{n} i = \\frac{n(n+1)}{2}\n$$\n\nЦена $x$ за штуку.")
    return "\n\n".join(parts)


_BIG_GFM: Final = _build_big_document(math=False)
_BIG_PANDOC: Final = _build_big_document(math=True)

# ---------------------------------------------------------------------------
# Нарезка: один разбор на документ
# ---------------------------------------------------------------------------


def _normalized(document: str) -> str:
    """Нормализованный вход: P13 нужен только текст, нарезка ему не нужна."""
    return normalize_text(document)[0]


def _chunked(document: str, chunk_size: int, min_chunk_size: int, profile: str) -> tuple[str, tuple]:
    """Нормализованный вход чанкера и куски — ровно один разбор документа."""
    normalized, _ = normalize_text(document)
    chunker = MarkdownChunker(chunk_size=chunk_size, min_chunk_size=min_chunk_size, profile=profile)
    return normalized, tuple(chunker.chunk("doc", document))


# ---------------------------------------------------------------------------
# Инварианты: текст и геометрия, без повторного разбора
# ---------------------------------------------------------------------------

#: Строка-маркер: контейнер и/или пункт списка без содержимого (``>``, ``- ``, ``3. ``,
#: ``> - [x] ``). Такие строки чанкер синтезирует сам — в документе их нет и быть не может.
_MARKER_ONLY = re.compile(
    r"^(?:[ \t]*>[ \t]?)*[ \t]*(?:[-+*]|\d{1,9}[.)])?(?:[ \t]+\[[ xX]\])?[ \t]*$"
)


def _is_marker_only(line: str) -> bool:
    return bool(_MARKER_ONLY.match(line))


def _bare(line: str) -> str:
    """Строка без префикса цитаты: маркер ``>`` — часть контейнера, а не текста."""
    return line.lstrip(">").strip()


def _source_lines(normalized: str) -> set[str]:
    """Строки документа без хвостовых пробелов — то, что контекст имеет право цитировать."""
    return {line.rstrip() for line in normalized.split("\n")}


def _context_lines(chunk: Chunk) -> list[str]:
    return [line.rstrip() for line in chunk.context_prefix.split("\n") if line.strip()]


def _bare_source(normalized: str) -> set[str]:
    """Строки документа в двух видах: как есть (без ``>``) и без решёток заголовка.

    ``heading_path`` печатает текст заголовка без ``#`` — это тот же текст документа,
    а не выдуманная строка.
    """
    lines = {_bare(line) for line in _source_lines(normalized)}
    return lines | {line.lstrip("#").strip() for line in lines}


def _case_id(case: tuple[str, int, int]) -> str:
    document, chunk_size, min_chunk_size = case
    return f"chunk_size={chunk_size} min_chunk_size={min_chunk_size} len={len(document)}"


# ---------------------------------------------------------------------------
# Свойства: каждая проверка — именованная функция
# ---------------------------------------------------------------------------

#: Сокращение для проверок: документ и бюджет примера.
_Case = tuple[str, int, int]


def _first_difference(left: str, right: str) -> str:
    """Где именно разошлись строки: диагностика важнее самого «не равно»."""
    for position, (a, b) in enumerate(zip(left, right, strict=False)):
        if a != b:
            return (
                f"позиция {position}: {left[max(0, position - 20) : position + 20]!r} "
                f"vs {right[max(0, position - 20) : position + 20]!r}"
            )
    return f"длины {len(left)} и {len(right)}"


def _check_p1(case: _Case, normalized: str, chunks: Sequence[Chunk]) -> None:
    """P1: конкатенация текстов кусков равна нормализованному входу байт-в-байт.

    Точное равенство, без ``strip()`` и без нормализации на стороне проверки: кусок —
    срез входа, поэтому ни один символ не может быть потерян или добавлен.
    """
    assert chunks, f"P1: {_case_id(case)}: документ с текстом дал ноль кусков"
    joined = "".join(chunk.text for chunk in chunks)
    assert joined == normalized, (
        f"P1: {_case_id(case)}: конкатенация кусков не равна входу — {_first_difference(joined, normalized)}"
    )


def _check_p4(case: _Case, normalized: str, chunks: Sequence[Chunk]) -> None:
    """P4: кусок непуст, не из одних пробелов и является подстрокой входа."""
    for chunk in chunks:
        assert chunk.text, f"P4: {_case_id(case)}: пустой кусок #{chunk.chunk_no}"
        assert chunk.text.strip(), f"P4: {_case_id(case)}: кусок #{chunk.chunk_no} из одних пробелов"
        assert chunk.text in normalized, (
            f"P4: {_case_id(case)}: кусок #{chunk.chunk_no} не подстрока входа: {chunk.text[:40]!r}"
        )


def _check_p13(case: _Case, normalized: str, profile: str) -> None:
    """P13: каждая непустая строка документа попала в спан какого-то блока."""
    parsed = parse_markdown(normalized, profile)
    assert parsed.counts["unknown_lines"] == 0, (
        f"P13: {_case_id(case)}: {parsed.counts['unknown_lines']} строк вне блоков, "
        f"разрывы {[(span.start_line, span.end_line) for span in parsed.gaps]}"
    )


def _check_total(case: _Case, normalized: str, chunks: Sequence[Chunk]) -> None:
    """Тотальность: кусок есть, и кусков не больше, чем символов.

    Верхняя граница не «жёсткий бюджет»: каждый кусок непуст, поэтому кусков физически
    не может быть больше символов — это и проверяется. Отдельно важен сам факт возврата:
    вызов завершился, значит ``_pieces`` не зациклился на коротком остатке.
    """
    assert len(chunks) >= 1, f"тотальность: {_case_id(case)}: ноль кусков"
    assert len(chunks) <= max(1, len(normalized)), (
        f"тотальность: {_case_id(case)}: {len(chunks)} кусков на {len(normalized)} символов"
    )


def _check_context(case: _Case, normalized: str, chunks: Sequence[Chunk]) -> None:
    """Контекст продолжения собран из строк документа, маркеров и heading_path.

    Строка контекста либо дословно есть в документе (с точностью до префикса цитаты),
    либо это маркер контейнера/пункта без содержимого (``>``, ``- ``, ``3. ``,
    ``> - [x] ``), который чанкер синтезирует сам. ``heading_path`` проверяется
    отдельно (:func:`_check_heading_path`): он печатает текст заголовка без ``#``.
    """
    source = _bare_source(normalized)
    for chunk in chunks:
        for line in _context_lines(chunk):
            if line == chunk.heading_path:
                continue
            assert _bare(line) in source or _is_marker_only(line), (
                f"контекст: {_case_id(case)}: строка {line!r} не из документа и не маркер "
                f"(кусок #{chunk.chunk_no})"
            )


def _check_heading_path(
    case: _Case, normalized: str, chunks: Sequence[Chunk], *, every_component: bool = False
) -> None:
    """``heading_path`` называет реальный заголовок документа.

    Проверяется последний компонент (или все компоненты): путь заголовков — провенанс
    цитаты, и выдуманный компонент был бы ложью о том, из какого раздела кусок.
    """
    for chunk in chunks:
        if not chunk.heading_path:
            continue
        components = chunk.heading_path.split(" > ") if every_component else [chunk.heading_path.rsplit(" > ", 1)[-1]]
        for component in components:
            if not component:
                continue
            assert component in normalized, (
                f"heading_path: {_case_id(case)}: компонент {component!r} не найден в документе "
                f"(кусок #{chunk.chunk_no})"
            )


# ---------------------------------------------------------------------------
# Свойства: gfm (основной набор)
# ---------------------------------------------------------------------------

_GFM_CASE: Final = _case(profile="gfm")


@_SETTINGS
@given(case=_GFM_CASE)
@example(case=(_BIG_GFM, 200, 0))
def test_p1_chunks_are_an_exact_concatenation(case: _Case) -> None:
    """P1: конкатенация текстов кусков равна нормализованному входу байт-в-байт."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "gfm")
    _check_p1(case, normalized, chunks)


@_SETTINGS_NARROW
@given(case=_GFM_CASE)
@example(case=(_BIG_GFM, 200, 0))
def test_p4_chunks_are_nonblank_slices(case: _Case) -> None:
    """P4: кусок непуст, не из одних пробелов и является подстрокой входа."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "gfm")
    _check_p4(case, normalized, chunks)


@_SETTINGS_NARROW
@given(case=_GFM_CASE)
@example(case=(_BIG_GFM, 1500, 20))
def test_p13_no_line_falls_outside_blocks(case: _Case) -> None:
    """P13: каждая непустая строка документа попала в спан какого-то блока."""
    document, _, _ = case
    _check_p13(case, _normalized(document), "gfm")


@_SETTINGS_NARROW
@given(case=_GFM_CASE)
@example(case=(_BIG_GFM, 200, 60))
def test_chunking_is_total(case: _Case) -> None:
    """Тотальность: вызов завершается и кусков не больше, чем символов."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "gfm")
    _check_total(case, normalized, chunks)


@_SETTINGS_NARROW
@given(case=_GFM_CASE)
@example(case=(_BIG_GFM, 200, 0))
def test_context_prefix_does_not_invent_text(case: _Case) -> None:
    """Контекст продолжения собран из строк документа, маркеров и heading_path."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "gfm")
    _check_context(case, normalized, chunks)


@_SETTINGS_NARROW
@given(case=_GFM_CASE)
@example(case=(_BIG_GFM, 800, 20))
def test_heading_path_names_a_real_heading(case: _Case) -> None:
    """Последний компонент ``heading_path`` встречается в документе как текст заголовка."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "gfm")
    _check_heading_path(case, normalized, chunks)


def test_tail_merge_path_is_reachable_and_lossless(monkeypatch) -> None:
    """Путь ``_absorb_tail`` достижим на документах генератора и не теряет символов.

    Это проверка **покрытия генератора**, а не чанкера. Замер до появления
    :func:`_tail_pair`: на корпусе из 36 документов ``_absorb_tail`` вызывался 72 раза и
    не склеил ни одного хвоста — значит, единственное место, где бюджет сознательно превышается, свойство P1 не
    проверяло вовсе. Здесь фиксируется, что хвост из :func:`_tail_pair` действительно
    склеивается для каждого бюджета, и что склейка дословна.
    """
    merges = 0
    original = MarkdownChunker._absorb_tail

    def counting(
        self, source: str, pieces: list[tuple[int, int, str | None]]
    ) -> list[tuple[int, int, str | None]]:
        nonlocal merges
        result = original(self, source, pieces)
        if len(result) != len(pieces):
            merges += 1
        return result

    monkeypatch.setattr(MarkdownChunker, "_absorb_tail", counting)

    for chunk_size in CHUNK_SIZES:
        document = _tail_pair(chunk_size)
        chunks = MarkdownChunker(chunk_size=chunk_size, min_chunk_size=60).chunk("doc", document)
        assert "".join(chunk.text for chunk in chunks) == normalize_text(document)[0], (
            f"склейка хвоста потеряла символы при chunk_size={chunk_size}"
        )
    assert merges == len(CHUNK_SIZES), f"хвост склеился {merges} раз из {len(CHUNK_SIZES)}"


# ---------------------------------------------------------------------------
# Свойства: pandoc (меньший набор, включён dollarmath)
# ---------------------------------------------------------------------------

_PANDOC_CASE: Final = _case(profile="pandoc")


@_SETTINGS_PANDOC
@given(case=_PANDOC_CASE)
@example(case=(_BIG_PANDOC, 400, 0))
def test_pandoc_p1_chunks_are_an_exact_concatenation(case: _Case) -> None:
    """P1 в профиле ``pandoc``: формулы не съедают и не добавляют символов."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "pandoc")
    _check_p1(case, normalized, chunks)


@_SETTINGS_PANDOC_NARROW
@given(case=_PANDOC_CASE)
@example(case=(_BIG_PANDOC, 3000, 60))
def test_pandoc_p4_chunks_are_nonblank_slices(case: _Case) -> None:
    """P4 в профиле ``pandoc``."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "pandoc")
    _check_p4(case, normalized, chunks)


@_SETTINGS_PANDOC_NARROW
@given(case=_PANDOC_CASE)
@example(case=(_BIG_PANDOC, 200, 20))
def test_pandoc_p13_no_line_falls_outside_blocks(case: _Case) -> None:
    """P13 в профиле ``pandoc``: формулы и определения попали в блоки."""
    document, _, _ = case
    _check_p13(case, _normalized(document), "pandoc")


@_SETTINGS_PANDOC_NARROW
@given(case=_PANDOC_CASE)
@example(case=(_BIG_PANDOC, 200, 0))
def test_pandoc_context_prefix_does_not_invent_text(case: _Case) -> None:
    """Контекст в ``pandoc``: те же правила, что в ``gfm``."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "pandoc")
    _check_context(case, normalized, chunks)


@_SETTINGS_PANDOC_NARROW
@given(case=_PANDOC_CASE)
@example(case=(_BIG_PANDOC, 800, 20))
def test_pandoc_heading_path_names_a_real_heading(case: _Case) -> None:
    """``heading_path`` в ``pandoc``: все компоненты пути — реальный текст заголовков."""
    document, chunk_size, min_chunk_size = case
    normalized, chunks = _chunked(document, chunk_size, min_chunk_size, "pandoc")
    _check_heading_path(case, normalized, chunks, every_component=True)
