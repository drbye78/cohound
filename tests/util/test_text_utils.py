"""Тесты ``core/util/text_utils.py``: язык, нормализация, нарезка, лексические токены.

Соседний ``test_text_utils_transliterate.py`` проверяет только транслитерацию; здесь —
остальной публичный контракт модуля, на который опираются лексический контур (§3.2) и
BM25-baseline. Эталон семантики — ``tests/selfcheck.py::_util_text`` и
``::_util_ru_tokenize``.
"""

from __future__ import annotations

from core.util.cjk_lexical import segment_cjk
from core.util.text_utils import (
    CLOSING_CHARS,
    SENTENCE_END_CHARS,
    TextLanguage,
    detect_language,
    hard_cut,
    is_cut_boundary,
    normalize_cyrillic,
    normalize_ru_token,
    sentence_terminated,
    split_sentences,
    tokenize_lexical,
)

#: Служебные слова RU/EN, которые токенизатор обязан выбросить.
_STOP_WORDS = {"и", "в", "на", "the", "of"}


def test_detect_language_ru_en() -> None:
    """RU/EN различаются; строка без букв языка не имеет — ``UNKNOWN``."""
    assert detect_language("Это предложение на русском языке") is TextLanguage.RUSSIAN
    assert detect_language("This is an english sentence") is TextLanguage.ENGLISH
    # Пустое/цифровое: букв нет, эвристика по алфавиту не находит ни кириллицы, ни латиницы.
    assert detect_language("") is TextLanguage.UNKNOWN
    assert detect_language("12345") is TextLanguage.UNKNOWN


def test_normalize_cyrillic_yo_to_e() -> None:
    """ё→е (и Ё→Е): иначе «ёж» и «еж» не сходятся в поиске."""
    assert normalize_cyrillic("ёж") == "еж"
    assert normalize_cyrillic("ЁЖИК") == "ЕЖИК"


def test_split_sentences_keeps_punctuation() -> None:
    """Границы предложений по ``.!?``; знаки остаются в своих предложениях."""
    assert split_sentences("Привет. Как дела? Хорошо!") == ["Привет.", "Как дела?", "Хорошо!"]
    assert split_sentences("") == []


def test_normalize_ru_token_stems_inflections() -> None:
    """Грубый стеммер сближает флективные формы одного слова."""
    assert normalize_ru_token("перехвата") == normalize_ru_token("перехват")


def test_tokenize_lexical_lowercases_and_drops_stopwords() -> None:
    """Токены в нижнем регистре, служебные слова выброшены."""
    tokens = tokenize_lexical("Инспекция трафика и в сети, the of data")

    assert "data" in tokens
    assert all(token == token.lower() for token in tokens)
    assert not _STOP_WORDS.intersection(tokens)


# ---------------------------------------------------------------------------
# CJK-лексика (план 18, «Сопутствующая работа»): биграммы прогонов и границы
# ---------------------------------------------------------------------------


def test_tokenize_lexical_without_cjk_is_byte_identical_to_legacy() -> None:
    """Прямой пин: для EN/RU-текста список токенов — **ровно** прежний, по порядку.

    Ожидаемые списки сняты с реализации до CJK-правки
    (``git show HEAD:core/util/text_utils.py``), а не записаны «по памяти»: сравнение
    поштучное, поэтому любое изменение порядка, нормализации или фильтра падает.
    """
    cases = {
        "Инспекция трафика и в сети, the of data": ["инспекци", "трафик", "сети", "data"],
        "PostgreSQL 15 экспортирует данные в файл; API-2024_X — endpoint.": [
            "postgresql",
            "15",
            "экспортирует",
            "данные",
            "файл",
            "api",
            "2024",
            "endpoint",
        ],
        (
            "Экспорт данных во внешний файл. Перехват сетевого трафика, "
            "резервное копирование."
        ): [
            "экспорт",
            "данных",
            "внешн",
            "файл",
            "перехват",
            "сетев",
            "трафик",
            "резервно",
            "копирован",
        ],
        "инспекция инспекции инспекцией a ab abc 42": [
            "инспекци",
            "инспекци",
            "инспекцие",
            "ab",
            "abc",
            "42",
        ],
    }
    for text, expected in cases.items():
        assert tokenize_lexical(text) == expected


def test_segment_cjk_is_identity_for_text_without_cjk() -> None:
    """Текст без CJK не переписывается: тот же объект строки, а не эквивалент.

    На этом держится неизменность EN/RU-токенизации: путь обёртки — «верни вход».
    """
    text = "Экспорт данных во внешний файл; endpoint API-2024_X."
    assert segment_cjk(text) is text
    assert segment_cjk("") is not None and segment_cjk("") == ""


def test_tokenize_lexical_cjk_run_is_overlapping_bigrams() -> None:
    """Прогон без пробелов режется на перекрывающиеся биграммы (длина 1 — символ)."""
    assert tokenize_lexical("配置管理") == ["配置", "置管", "管理"]
    assert tokenize_lexical("数据库配置管理工具提供了备份功能。") == [
        "数据", "据库", "库配", "配置", "置管", "管理", "理工", "工具",
        "具提", "提供", "供了", "了备", "备份", "份功", "功能",
    ]
    assert tokenize_lexical("配") == ["配"]
    assert tokenize_lexical("配。") == ["配"]


def test_tokenize_lexical_cjk_punctuation_and_fullwidth_are_boundaries() -> None:
    """``。！？；，、：``, ``・`` и полноширинные формы — границы прогонов, не текст."""
    assert tokenize_lexical("配置，管理") == ["配置", "管理"]
    assert tokenize_lexical("配！置？管；理") == ["配", "置", "管", "理"]
    assert tokenize_lexical("データ・ベース") == ["デー", "ータ", "ベー", "ース"]
    # Полноширинная латиница/цифры не становятся CJK-термами и не склеиваются с ними.
    assert tokenize_lexical("配Ａ置") == ["配", "置"]
    assert tokenize_lexical("配置　管理") == ["配置", "管理"]  # U+3000 ideographic space


def test_tokenize_lexical_mixed_script_keeps_both_parts() -> None:
    """«PostgreSQL 配置管理» находит и латиницу, и обе части CJK-запроса."""
    assert tokenize_lexical("PostgreSQL 配置管理") == [
        "postgresql",
        "配置",
        "置管",
        "管理",
    ]
    assert tokenize_lexical("配置管理PostgreSQL") == [
        "配置",
        "置管",
        "管理",
        "postgresql",
    ]


def test_tokenize_lexical_cjk_is_deterministic() -> None:
    """Один и тот же текст → один и тот же список токенов (два вызова, два процесса не тут)."""
    text = "データベース 配置管理 데이터베이스"
    assert tokenize_lexical(text) == tokenize_lexical(text)
    assert tokenize_lexical("配置管理") == ["配置", "置管", "管理"]


# ---------------------------------------------------------------------------
# Границы разреза (общие для markdown- и абзацного чанкера)
# ---------------------------------------------------------------------------


def test_boundary_rejects_leading_closing_punctuation() -> None:
    """Кусок не начинается с закрывающего знака: иначе он открывается чужим хвостом."""
    assert is_cut_boundary("фраза. Другая", 7) is True
    assert is_cut_boundary("фраза, другая", 5) is False  # начинается с «,»
    assert is_cut_boundary("(сноска) текст", 8) is True
    assert is_cut_boundary("итог」дальше", 4) is False
    assert is_cut_boundary("абв", 0) is False
    assert is_cut_boundary("абв", 3) is False


def test_boundary_keeps_grapheme_cluster_together() -> None:
    """Комбинирующий знак и эмодзи-модификатор не отрываются от своего предка."""
    text = "и\u0301тог"  # «и» + комбинирующее ударение
    assert is_cut_boundary(text, 1) is False
    # ZWJ стоит на позиции 5, эмодзи — на 6: разрезать между ними нельзя.
    assert is_cut_boundary("семья\u200d👨 дальше", 6) is False
    assert is_cut_boundary("семья\u200d👨 дальше", 8) is True


def test_hard_cut_does_not_start_a_piece_with_closing_punctuation() -> None:
    """Жёсткий разрез отступает назад, но не делает кусок пустым."""
    text = "слово, другое"
    assert text[hard_cut(text, 0, 6)] != ","
    assert hard_cut(text, 0, 1) == 1


def test_sentence_terminated_accepts_trailing_closing_quote() -> None:
    """``«цитата.»`` — конец предложения, а не обрывок: кавычка не отменяет терминатор."""
    assert sentence_terminated("Привет.")
    assert sentence_terminated("Привет.  ")
    assert sentence_terminated("他说「你好。」")
    assert not sentence_terminated("обрывок")
    assert not sentence_terminated("список，")  # запятая — не терминатор
    assert all(char in SENTENCE_END_CHARS for char in ".!?;…。！？；")
    assert "」" in CLOSING_CHARS
