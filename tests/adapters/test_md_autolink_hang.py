"""Пины дефекта «GFM-автоссылка в подписи ссылки вешает разбор навсегда».

Дефект: silent-ветка трёх правил ``mdit_py_plugins.gfm_autolink`` возвращала ``True``,
не сдвигая ``state.pos``; ``ParserInline.skipToken`` кешировал неизменённую позицию
(``markdown_it/parser_inline.py:172``), а ``parseLinkLabel`` крутится в
``while state.pos < state.posMax`` (``markdown_it/helpers/parse_link_label.py:20``) ⇒
бесконечный цикл без исключения и без таймаута. Исправление — копия правил в
:mod:`core.adapters.md_gfm_autolink`; здесь — доказательство, что цикл не возвращается,
и что эффективное отображение «текст → блоки» для остальных документов не изменилось.

Каждый кейс с разбором идёт под жёстким пределом времени (:func:`_deadline`): регресс
обязан падать, а не вешать CI.
"""

from __future__ import annotations

import hashlib
import signal
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from markdown_it import MarkdownIt
from markdown_it.rules_inline import StateInline
from markdown_it.token import Token
from mdit_py_plugins.gfm_autolink import gfm_autolink_plugin as upstream_plugin
from mdit_py_plugins.gfm_autolink.index import (
    _www_inline_rule as upstream_www_rule,
)

from core.adapters.chunker_markdown import MarkdownChunker
from core.adapters.md_ast import MdDocument, parse_markdown
from core.adapters.md_gfm_autolink import (
    _email_rule,
    _protocol_rule,
    _www_inline_rule,
    gfm_autolink_plugin,
)

#: Предел на один разбор. Дефект не имеет таймаута: без этого сторожа регресс повесил бы
#: весь набор (и CI) навсегда, а не упал.
DEADLINE_S = 10.0

#: Строки, на которых апстрим не возвращался никогда (см. замер
#: ``docs/reports/measurements/2026-11-07-md-autolink-hang.md``).
EVIL_INPUTS = (
    "[см. www.example.com](https://example.com)",
    "[буке: www.facebook.com/groups/postgresql.]"
    "(https://www.facebook.com/groups/postgresql)",
    "[https://www.example.com](https://example.com)",
    "[http://example.com](https://example.com)",
    "[пиши на a@b.com](mailto:x@y.z)",
    "[2 www.postgresql.org/message-id/flat/CAPpHfdvJhO1qutziOp]"
    "(https://www.postgresql.org/message-id/flat/CAPpHfdvJhO1qutziOp)",
    "[ Сайт книги: www.postgrespro.ru/education/books/advancedsql. ]"
    "(https://postgrespro.ru/education/books/advancedsql)",
)


@contextmanager
def _deadline(seconds: float = DEADLINE_S) -> Iterator[None]:
    """Жёсткий предел времени: ``TimeoutError`` вместо зависания.

    ``signal.setitimer`` срабатывает и внутри чистого Python-цикла ``parseLinkLabel``,
    поэтому именно он ловит класс дефекта «позиция не двигается».
    """

    def _raise(signum: int, frame: object) -> None:
        raise TimeoutError(
            f"разбор не завершился за {seconds:.0f} с — позиция не двигается"
        )

    previous = signal.signal(signal.SIGALRM, _raise)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _md() -> MarkdownIt:
    return MarkdownIt("commonmark").use(gfm_autolink_plugin)


def _inline_tokens(md: MarkdownIt, src: str) -> list[Token]:
    """Инлайновые токены единственного абзаца."""
    tokens = md.parse(src)
    inline = [token for token in tokens if token.type == "inline"]
    assert len(inline) == 1, f"ожидался один инлайновый токен, получено {len(inline)}"
    return list(inline[0].children or ())


def _links(md: MarkdownIt, src: str) -> list[tuple[str, str]]:
    """Пары ``(href, текст)`` всех ссылок в строке, в порядке появления."""
    result: list[tuple[str, str]] = []
    href: str | None = None
    text = ""
    for token in _inline_tokens(md, src):
        if token.type == "link_open":
            href = str((token.attrs or {}).get("href", ""))
            text = ""
        elif token.type == "text" and href is not None:
            text += token.content
        elif token.type == "link_close" and href is not None:
            result.append((href, text))
            href = None
    return result


def _digest(document: MdDocument) -> str:
    """Отпечаток ``MdDocument``: вид, строки и смещения каждого блока (все уровни)."""
    rows = [
        (
            block.kind,
            block.span.start_line,
            block.span.end_line,
            block.span.start_char,
            block.span.end_char,
        )
        for top in document.blocks
        for block in top.walk()
    ]
    return hashlib.sha256(repr(rows).encode()).hexdigest()


# -- 1–2. разбор завершается и ссылка распознана ----------------------------


def test_www_in_link_label_parses_within_deadline() -> None:
    """``[см. www.example.com](…)``: разбор возвращается, ссылка распознана, текст цел.

    Внешняя markdown-ссылка при этом **не** сохраняется: сопоставитель ``www.`` апстрима
    тянет совпадение до конца «непробельного» текста и не отсекает ``]``, поэтому ``]``
    оказывается внутри автоссылки. Это поведение самого плагина (не дефект цикла) и здесь
    не правится: тест фиксирует то, что важно для чанкера, — разбор завершается, ссылка
    есть, и ни один символ строки не потерян.
    """
    src = "[см. www.example.com](https://example.com)\n"
    with _deadline():
        document = parse_markdown(src, profile="gfm")
    assert [block.kind for block in document.blocks] == ["paragraph"]

    tokens = _inline_tokens(_md(), src.strip())
    links = _links(_md(), src.strip())
    assert links, "автоссылка в подписи не распознана"
    assert links[0][0].startswith("http://www.example.com")
    # Текст не потерян: склейка всех текстовых токенов — исходная строка целиком.
    assert (
        "".join(token.content for token in tokens if token.type == "text")
        == src.strip()
    )


def test_real_corpus_fragment_parses_within_deadline() -> None:
    """Реальный фрагмент корпуса (facebook в подписи) не вешает разбор."""
    src = (
        "[буке: www.facebook.com/groups/postgresql.]"
        "(https://www.facebook.com/groups/postgresql)\n"
    )
    with _deadline():
        document = parse_markdown(src, profile="gfm")
    assert document.counts["paragraph"] == 1
    assert document.text == src


# -- 3. протокол и e-mail в подписи ----------------------------------------


def test_protocol_and_email_in_link_label_parse_as_markdown_links() -> None:
    """Подпись с ``https://``/``http://``/адресом не ломает внешнюю ссылку.

    Здесь предыдущий символ — ``[``, который ``check_prev`` не разрешает, поэтому
    автоссылка внутри подписи не срабатывает и ссылка разбирается как обычная markdown.
    """
    md = _md()
    cases = (
        (
            "[https://www.example.com](https://example.com)",
            "https://example.com",
            "https://www.example.com",
        ),
        (
            "[http://example.com](https://example.com)",
            "https://example.com",
            "http://example.com",
        ),
        ("[пиши на a@b.com](mailto:x@y.z)", "mailto:x@y.z", "пиши на a@b.com"),
    )
    for src, href, text in cases:
        with _deadline():
            links = _links(md, src)
        assert links == [(href, text)], f"{src!r} → {links!r}"


# -- 4. вне подписи всё как раньше -----------------------------------------


def test_bare_www_and_plain_link_are_unchanged() -> None:
    """``www.`` вне подписи — автоссылка; обычная ссылка — без автоссылки."""
    md = _md()
    with _deadline():
        assert _links(md, "см. www.example.com") == [
            ("http://www.example.com", "www.example.com")
        ]
        assert _links(md, "обычная [ссылка](https://example.com) без автоссылки") == [
            ("https://example.com", "ссылка")
        ]
        # Внутри существующей ссылки автоссылка не срабатывает (``linkLevel``).
        assert _links(md, "[www.example.com](https://example.com)") == [
            ("https://example.com", "www.example.com")
        ]


def test_pandoc_superscript_scan_terminates() -> None:
    """Второй контур того же дефекта: сканер ``superscript`` зовёт ``skipToken`` в цикле.

    ``mdit_py_plugins/superscript/index.py:79`` ищет закрывающий ``^`` тем же способом, что
    ``parseLinkLabel`` ищет ``]``. Замерено до правки: ``^см. www.example.com^`` в профиле
    ``pandoc`` не возвращался, стек стоял в ``superscript``.
    """
    src = "^см. www.example.com^\n"
    with _deadline():
        document = parse_markdown(src, profile="pandoc")
    assert document.counts["paragraph"] == 1
    assert document.text == src


# -- 5. свойство, ловящее класс дефекта ------------------------------------


def test_skip_token_always_advances() -> None:
    """``skipToken`` обязан двигать позицию: на этом стоит цикл ``parseLinkLabel``."""
    md = _md()
    for src in EVIL_INPUTS:
        with _deadline():
            state = StateInline(src, md, {}, [])
            steps = 0
            while state.pos < state.posMax:
                before = state.pos
                md.inline.skipToken(state)
                assert state.pos > before, f"позиция встала на {before} в {src!r}"
                steps += 1
                assert steps <= len(src) + 1, f"позиция ходит кругами в {src!r}"


def test_silent_success_always_advances_position() -> None:
    """Прямая проверка правила: ``True`` в silent-режиме ⇒ ``pos`` вырос, ``pending`` цел."""
    md = _md()
    rules = (_www_inline_rule, _protocol_rule, _email_rule)
    checked = 0
    for src in EVIL_INPUTS:
        for start in range(len(src)):
            for rule in rules:
                state = StateInline(src, md, {}, [])
                state.pos = start
                state.posMax = len(src)
                # ``pending`` — текст до позиции: так правило видит тот же префикс, что и
                # при настоящем разборе, и проверяется на каждом символе строки.
                state.pending = src[:start]
                pending_before = state.pending
                if rule(state, True):
                    checked += 1
                    assert state.pos > start, (
                        f"{rule.__name__} вернул True без сдвига в {src!r}"
                    )
                    assert state.pending == pending_before, (
                        "silent-режим изменил pending"
                    )
    assert checked > 0, "ни одно правило не сработало — проверка ничего не доказала"


def test_upstream_plugin_still_has_the_defect() -> None:
    """Сторож: апстрим 0.6.1 не двигает позицию. Упадёт — копию можно удалять."""
    md = MarkdownIt("commonmark").use(upstream_plugin)
    state = StateInline("см. www.example.com", md, {}, [])
    state.pos = 4
    assert state.src[state.pos : state.pos + 4] == "www."
    assert upstream_www_rule(state, True) is True
    assert state.pos == 4, "апстрим исправлен: удалите core/adapters/md_gfm_autolink.py"


# -- 6. неизменность для документов без опасной конструкции ----------------


def test_plain_document_blocks_and_spans_are_unchanged() -> None:
    """Эталон снят с кода **до** правки на реальном документе из ``extract_cache``.

    Документ (8 002 символа, ``trafilatura-html-6``: 22 заголовка, 17 fence-блоков,
    2 таблицы, 5 списков) не содержит ``www.`` в подписи ссылки. Отпечаток снят с версии
    ``core/adapters/md_ast.py`` из ``HEAD`` до подключения исправленного плагина; совпадение
    доказывает, что эффективное отображение «текст → блоки» не изменилось, а значит
    ``chunker_version`` поднимать не за что.
    """
    text = (Path(__file__).parent / "fixtures" / "md_regression_plain.md").read_text(
        encoding="utf-8"
    )
    with _deadline(30.0):
        gfm = parse_markdown(text, profile="gfm")
    assert gfm.counts["blocks"] == 85
    assert gfm.counts["table"] == 2
    assert gfm.counts["fence"] == 17
    assert (
        _digest(gfm)
        == "11a98a0767dc1989aa55656ed4d9a9078fb49411c9e5e78f2ab04c1e58f5503d"
    )
    assert _digest(parse_markdown(text, profile="commonmark")) == (
        "4dd6447a76d295188eef71cd0c31d8bb90eb384479dbb2f7fce6410b203062c4"
    )


@pytest.mark.parametrize("src", EVIL_INPUTS)
def test_evil_inputs_are_stable_across_repeats(src: str) -> None:
    """Разбор детерминирован: два прогона дают один отпечаток (кеш правил не «залипает»)."""
    with _deadline():
        first = _digest(parse_markdown(src + "\n", profile="gfm"))
        second = _digest(parse_markdown(src + "\n", profile="gfm"))
    assert first == second


@pytest.mark.parametrize("src", EVIL_INPUTS)
def test_chunker_slices_evil_input_verbatim(src: str) -> None:
    """Продуктовая гарантия P1 на «злой» строке: чанки — дословные срезы, ничего не теряется."""
    text = src + "\n"
    with _deadline():
        chunks = MarkdownChunker().chunk("doc", text)
    assert chunks, "документ дал ноль чанков"
    for chunk in chunks:
        assert text[chunk.char_start : chunk.char_end] == chunk.text
    assert "".join(chunk.text for chunk in chunks) == parse_markdown(text).text
