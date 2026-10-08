"""Многоязычные текстовые утилиты (RU/EN) для корпуса coHound.

Используется при подготовке корпуса (§3.2: языки, tantivy RU/EN), нормализации
для лексического контура и детекции языка документов/чанков.

Происхождение: адаптировано из проекта gag5 (`core/text_utils.py`, MIT License,
Copyright (c) 2024) — см. THIRD_PARTY_NOTICES.md. Изменения: нейтрализация под
категорийную нейтральность coHound (§1.3), удаление доменных/LLM-специфичных
веток, упрощение API.

Категорийная нейтральность: код не содержит предметных терминов.
"""

from __future__ import annotations

import re
import unicodedata
from enum import StrEnum
from functools import lru_cache

try:  # необязательная зависимость: при отсутствии — эвристика по алфавиту
    from langdetect import LangDetectException, detect as _langdetect  # type: ignore[import-not-found]

    _LANGDETECT_AVAILABLE = True
except ImportError:  # pragma: no cover - зависит от окружения
    _LANGDETECT_AVAILABLE = False
    _langdetect = None
    LangDetectException = Exception  # type: ignore[assignment,misc]


class TextLanguage(StrEnum):
    RUSSIAN = "ru"
    ENGLISH = "en"
    UNKNOWN = "unknown"


_CYRILLIC_BLOCKS = ((0x0400, 0x04FF), (0x0500, 0x052F), (0x2DE0, 0x2DFF), (0xA640, 0xA69F))

_RUSSIAN_EQUIVALENTS = {"ё": "е", "Ё": "Е"}

RUSSIAN_STOP_WORDS: frozenset[str] = frozenset(
    {
        "и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как", "а", "то",
        "все", "она", "так", "его", "но", "да", "ты", "у", "же", "вы", "за", "бы",
        "по", "только", "ее", "мне", "было", "вот", "от", "меня", "еще", "нет", "о",
        "из", "ему", "теперь", "когда", "уже", "вам", "ни", "быть", "был", "него",
        "до", "нас", "для",
    }
)

ENGLISH_STOP_WORDS: frozenset[str] = frozenset(
    {
        "the", "is", "at", "which", "on", "and", "a", "an", "to", "in", "of", "for",
        "that", "by", "with", "from", "as", "it", "be", "are", "was", "or", "have",
        "has", "had", "were", "been", "being", "this", "these", "those", "can", "will",
        "just", "should", "would", "could",
    }
)


def _detect_by_script(text: str) -> TextLanguage:
    """Эвристика языка по преобладающему алфавиту (fallback)."""
    cyrillic = 0
    latin = 0
    for char in text:
        code = ord(char)
        if any(lo <= code <= hi for lo, hi in _CYRILLIC_BLOCKS):
            cyrillic += 1
        elif char.isalpha() and ("a" <= char.lower() <= "z"):
            latin += 1
    if cyrillic > latin * 2:
        return TextLanguage.RUSSIAN
    if latin > cyrillic * 2:
        return TextLanguage.ENGLISH
    return TextLanguage.UNKNOWN


@lru_cache(maxsize=4096)
def detect_language(text: str) -> TextLanguage:
    """Определяет язык текста (RU/EN/UNKNOWN)."""
    if not text or not text.strip():
        return TextLanguage.UNKNOWN
    if not _LANGDETECT_AVAILABLE:
        return _detect_by_script(text)
    try:
        lang = _langdetect(text)  # type: ignore[misc]
    except LangDetectException:
        return _detect_by_script(text)
    if lang == "ru":
        return TextLanguage.RUSSIAN
    if lang == "en":
        return TextLanguage.ENGLISH
    return TextLanguage.UNKNOWN


def normalize_cyrillic(text: str) -> str:
    """Приводит ё→е (и Ё→Е) для единообразного поиска."""
    for old, new in _RUSSIAN_EQUIVALENTS.items():
        text = text.replace(old, new)
    return text


def remove_diacritics(text: str) -> str:
    """Удаляет диакритику через разложение NFD."""
    nfd = unicodedata.normalize("NFD", text)
    result = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return unicodedata.normalize("NFC", result)


def normalize_text(
    text: str,
    language: TextLanguage | None = None,
    lowercase: bool = True,
    strip_accents: bool = True,
) -> str:
    """Нормализует текст для индексации/сравнения (NFKC, пробелы, регистр)."""
    if not text:
        return ""
    result = text
    if lowercase:
        result = result.lower()
    if language == TextLanguage.RUSSIAN:
        result = normalize_cyrillic(result)
    if strip_accents:
        result = remove_diacritics(result)
    result = unicodedata.normalize("NFKC", result)
    result = re.sub(r"\s+", " ", result)
    return result.strip()


_SENTENCE_ENDINGS = re.compile(r"(?<=[.!?])\s+")


def split_sentences(text: str, language: TextLanguage | None = None) -> list[str]:
    """Разбивает текст на предложения (язык влияет на эвристику конца предложения)."""
    if not text:
        return []
    return [s.strip() for s in _SENTENCE_ENDINGS.split(text) if s.strip()]


def is_stop_word(word: str, language: TextLanguage) -> bool:
    word_lower = word.lower()
    if language == TextLanguage.RUSSIAN:
        return word_lower in RUSSIAN_STOP_WORDS
    return word_lower in ENGLISH_STOP_WORDS


def clean_whitespace(text: str) -> str:
    """Схлопывает табы/переводы строк и повторные пробелы."""
    text = re.sub(r"[\t\n\r]+", " ", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def is_cyrillic(text: str) -> bool:
    return any("\u0400" <= char <= "\u04ff" for char in text)


def is_latin(text: str) -> bool:
    return any(char.isalpha() and "a" <= char.lower() <= "z" for char in text)
