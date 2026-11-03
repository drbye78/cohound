"""Многоязычные текстовые утилиты (RU/EN/CJK) для корпуса coHound.

Используется при подготовке корпуса (§3.2: языки, tantivy RU/EN), нормализации
для лексического контура и детекции языка документов/чанков. CJK-сегментация для
лексического контура живёт в ``core.util.cjk_lexical`` (словарей и ICU нет:
биграммы прогонов — стандартный приём BM25).

Происхождение: адаптировано из проекта gag5 (`core/text_utils.py`, MIT License,
Copyright (c) 2024). Изменения: нейтрализация под
категорийную нейтральность coHound (§1.3), удаление доменных/LLM-специфичных
веток, упрощение API.

Категорийная нейтральность: код не содержит предметных терминов.
"""

from __future__ import annotations

import re
import unicodedata
from enum import StrEnum
from functools import lru_cache

from core.util.cjk_lexical import cjk_terms, iter_cjk_runs

try:  # необязательная зависимость: при отсутствии — эвристика по алфавиту
    from langdetect import LangDetectException  # type: ignore[import-not-found]
    from langdetect import detect as _langdetect

    _LANGDETECT_AVAILABLE = True
except ImportError:  # pragma: no cover - зависит от окружения
    _LANGDETECT_AVAILABLE = False
    _langdetect = None
    LangDetectException = Exception  # type: ignore[assignment,misc]


__all__ = [
    "CLOSING_CHARS",
    "ENGLISH_STOP_WORDS",
    "RUSSIAN_STOP_WORDS",
    "SENTENCE_END_CHARS",
    "TextLanguage",
    "clean_whitespace",
    "detect_language",
    "hard_cut",
    "is_cut_boundary",
    "is_cyrillic",
    "is_latin",
    "is_stop_word",
    "normalize_cyrillic",
    "normalize_ru_token",
    "normalize_text",
    "remove_diacritics",
    "sentence_terminated",
    "split_sentences",
    "tokenize_lexical",
    "transliterate",
]


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


#: ISO-9/GOST-подобная транслитерация русского алфавита (латиница проходит без изменений).
_TRANSLITERATION_LOWER: dict[str, str] = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "yo",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "kh",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "shch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}

_TRANSLITERATION = {
    **_TRANSLITERATION_LOWER,
    **{char.upper(): value.capitalize() for char, value in _TRANSLITERATION_LOWER.items()},
}
_TRANSLITERATION_TABLE = str.maketrans(_TRANSLITERATION)


def transliterate(text: str) -> str:
    """Транслитерирует кириллицу в латиницу (ISO-9/GOST-подобно).

    Нужна для стабильных slug-идентификаторов из русских имён (``product_id`` и т.п.):
    ``"Квант-Гибрид"`` → ``"Kvant-Gibrid"``. Латиница и прочие символы не меняются,
    поэтому смешанные строки (``"Alpha Квант"``) безопасны.
    """
    return str(text or "").translate(_TRANSLITERATION_TABLE)


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


#: Знаки конца предложения: западные и **полноширинные**. Вторые обязательны: в
#: китайском/японском тексте между предложениями пробела нет, и правило «терминатор +
#: пробел» не находит там ни одной границы — абзац режется по номеру символа.
SENTENCE_END_CHARS: frozenset[str] = frozenset(".!?;…。！？；")

#: Символы, с которых фрагмент начинаться не может: закрывающая пунктуация, знаки конца
#: предложения и закрывающие скобки. Иначе кусок открывается знаком, который относится к
#: предыдущей фразе, и цитата читается как обрывок. Западные ``,.!?;:`` здесь наравне с
#: полноширинными: жёсткий разрез иначе начинал бы чанк с запятой (замерено на
#: ``is_cut_boundary("фраза, другая", 5)`` — свободная позиция ровно перед запятой).
#: Кавычки **не** входят: ``«``/``"`` законно открывают цитату с начала фрагмента.
CLOSING_CHARS: frozenset[str] = frozenset(")]}»”」』>，、：；。！？,.!?;:")

#: Соединители и модификаторы, разрывать перед которыми нельзя (графемный кластер).
_JOINERS: frozenset[str] = frozenset("\u200d\ufe0e\ufe0f\u200b\u0301")


def is_cut_boundary(text: str, position: int) -> bool:
    """Можно ли начинать фрагмент с ``position``: не висячая пунктуация, не кластер.

    Проверяется символ справа: фрагмент не начинается с закрывающего знака (иначе он
    открывается чужим хвостом) и не начинается с комбинирующего знака или вариационного
    селектора (иначе первый символ приклеится к невидимому предку).
    """
    if position <= 0 or position >= len(text):
        return False
    if text[position] in CLOSING_CHARS:
        return False
    if text[position - 1] in _JOINERS or text[position] in _JOINERS:
        return False
    return not (
        unicodedata.combining(text[position]) or unicodedata.combining(text[position - 1])
    )


def hard_cut(text: str, start: int, limit: int) -> int:
    """Конец жёсткого разреза, когда безопасных границ нет: не рвёт графемный кластер.

    ``start`` — начало куска (дальше него не отступаем: кусок не должен стать пустым).
    """
    end = min(limit, len(text))
    while end > start + 1 and not is_cut_boundary(text, end):
        end -= 1
    return end


def sentence_terminated(text: str) -> bool:
    """Кончается ли фрагмент концом предложения.

    Пробелы справа не считаются, как и закрывающая кавычка/скобка сразу после
    терминатора: ``«цитата.»`` — конец предложения, а не обрывок.
    """
    stripped = text.rstrip()
    while stripped and stripped[-1] in CLOSING_CHARS - SENTENCE_END_CHARS:
        stripped = stripped[:-1].rstrip()
    return bool(stripped) and stripped[-1] in SENTENCE_END_CHARS


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


# --------------------------------------------------------------------------- #
# Лёгкая лексическая токенизация (для BM25-контура и сопоставления concept_terms)
# Происхождение: адаптировано из проекта gtm (`scripts/kb_poc.py`, собственная
# разработка).
# --------------------------------------------------------------------------- #

_TOKEN_RE = re.compile(r"[а-яёa-z0-9]+", re.IGNORECASE)

# Частые русские флективные окончания — обрезаются для грубого «стемминга».
_RU_SUFFIXES = (
    "иями", "ями", "ами", "ого", "ему", "ому", "ыми", "ими", "ая", "яя", "ою", "ею",
    "ий", "ый", "ой", "ем", "ам", "ах", "ям", "ев", "ов", "ие", "ье", "её",
    "и", "ы", "а", "я", "о", "е", "у", "ю", "ь", "й",
)

_ALL_STOP_WORDS = RUSSIAN_STOP_WORDS | ENGLISH_STOP_WORDS


def normalize_ru_token(token: str) -> str:
    """Грубая нормализация RU-токена: обрезка частого флективного хвоста.

    Не заменяет настоящий стеммер (tantivy RU/EN, §3.2), но даёт дешёвое
    сближение форм («перехвата» ≈ «перехват») на лексическом baseline.
    """
    t = token.lower()
    if len(t) > 6:
        for suffix in _RU_SUFFIXES:
            if t.endswith(suffix) and len(t) - len(suffix) >= 4:
                return t[: -len(suffix)]
    return t


def _latin_lexical_tokens(chunk: str) -> list[str]:
    """Прежняя токенизация латиницы/кириллицы — ровно та же, что до CJK-сегментации.

    Вынесена отдельно, чтобы CJK-ветка не могла изменить EN/RU-поведение: для текста
    без CJK этот помощник вызывается один раз на весь вход и даёт тот же список, что
    давала прежняя реализация ``tokenize_lexical`` (пин —
    ``tests/util/test_text_utils.py::test_tokenize_lexical_without_cjk_is_byte_identical_to_legacy``).
    """
    out: list[str] = []
    for match in _TOKEN_RE.findall(chunk):
        token = match.lower()
        if token in _ALL_STOP_WORDS or len(token) < 2:
            continue
        out.append(normalize_ru_token(token))
    return out


def tokenize_lexical(text: str) -> list[str]:
    """Токенизация для лексического поиска: регистр, нормализация, стоп-слова, CJK.

    Латиница и кириллица режутся как раньше (``[а-яёa-z0-9]+``, нижний регистр,
    эвристический RU-стемминг, стоп-слова). Добавлена только CJK-ветка: прогоны Han,
    хираганы, катаканы и хангыля дают **перекрывающиеся биграммы** (для прогона длиной 1 —
    сам символ), потому что иначе вся серия без пробелов становится одним термом и
    запрос-подстрока внутри неё не находит ничего (``core.util.cjk_lexical``).

    Порядок токенов — порядок текста: латинские/кириллические фрагменты между CJK-прогонами
    обрабатываются прежней функцией, поэтому для текста без CJK результат **не меняется**
    ни на один токен. Одиночный CJK-символ не отбрасывается фильтром ``len(token) < 2``:
    это терм, а не однобуквенный шум, и он должен находиться (и индексироваться).
    """
    text = text or ""
    out: list[str] = []
    position = 0
    for start, end in iter_cjk_runs(text):
        out.extend(_latin_lexical_tokens(text[position:start]))
        out.extend(cjk_terms(text[start:end]))
        position = end
    out.extend(_latin_lexical_tokens(text[position:]))
    return out
