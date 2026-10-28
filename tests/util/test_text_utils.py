"""Тесты ``core/util/text_utils.py``: язык, нормализация, нарезка, лексические токены.

Соседний ``test_text_utils_transliterate.py`` проверяет только транслитерацию; здесь —
остальной публичный контракт модуля, на который опираются лексический контур (§3.2) и
BM25-baseline. Эталон семантики — ``tests/selfcheck.py::_util_text`` и
``::_util_ru_tokenize``.
"""

from __future__ import annotations

from core.util.text_utils import (
    TextLanguage,
    detect_language,
    normalize_cyrillic,
    normalize_ru_token,
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
