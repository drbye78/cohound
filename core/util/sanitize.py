"""Санитизация контента на границе экстракции (T7.5, §10 / 02 §C.2).

Три независимые чистые функции:

- :func:`sanitize_html` — удаляет активный/опасный HTML (``<script>``/``<style>``,
  event-handler'ы ``on*``, опасные URL-схемы ``javascript:``/``vbscript:``/``data:text/html``,
  комментарии). Основной путь — ``nh3`` (Rust/ammonia, MIT); при отсутствии ``nh3`` (или
  сбое его конфигурации) работает **stdlib-fallback** на регулярных выражениях. Он удаляет
  только опасные фрагменты, оставляя остальную разметку и **все текстовые узлы**
  байт-в-байт неизменными.
- :func:`strip_control_chars` — удаляет управляющие символы (C0/C1, DEL), кроме ``\\n`` и
  ``\\t``, которые сохраняются: они значимы для markdown/логов и не ломают промпт.
- :func:`neutralize_prompt_injection_marks` — помечает/вырезает **каркас** промпт-инъекций
  (спец-токены чат-шаблонов, псевдо-роли в начале строки, фразы-переопределения,
  tool-call-подобные блоки), не переписывая содержательную прозу.

**Инвариант «санитайзинг не мутирует текст» (05 §2.6, 02 §C.2).** Проверка цитаты в §10
строгая: ``quote`` обязан быть подстрокой чистого ``text`` чанка. Поэтому:

1. Санитайзинг применяется **одной границей** — до чанкинга/хеширования; чанк и цитата
   оцениваются по уже санитизированному тексту, подстрочное свойство сохраняется.
2. Функции не нормализуют прозу: не сворачивают пробелы, не меняют регистр, не
   переписывают предложения — изменяются только распознанные фрагменты каркаса.

Все функции детерминированы, без побочных эффектов и **без обязательных зависимостей**:
``nh3`` и ``defusedxml`` — опциональны. Нейтрален (§1.3).
"""

from __future__ import annotations

import re

__all__ = [
    "sanitize_html",
    "strip_control_chars",
    "neutralize_prompt_injection_marks",
]

# ---------------------------------------------------------------------------
# nh3 — основной путь (опциональная зависимость)
# ---------------------------------------------------------------------------

try:  # pragma: no cover - зависит от окружения
    import nh3  # type: ignore[import-not-found]

    _NH3_AVAILABLE = True
except ImportError:  # pragma: no cover
    nh3 = None  # type: ignore[assignment]
    _NH3_AVAILABLE = False

# Разрешённый поднабор тегов: только структура/разметка, без активного контента.
_NH3_TAGS: set[str] = {
    "a", "abbr", "b", "blockquote", "br", "caption", "code", "col", "colgroup",
    "dd", "div", "dl", "dt", "em", "figcaption", "figure", "h1", "h2", "h3",
    "h4", "h5", "h6", "hr", "i", "img", "li", "ol", "p", "pre", "q", "s",
    "small", "span", "strong", "sub", "sup", "table", "tbody", "td", "tfoot",
    "th", "thead", "tr", "u", "ul",
}

_NH3_ATTRIBUTES: dict[str, set[str]] = {
    "a": {"href", "title"},
    "img": {"src", "alt", "title", "width", "height"},
    "th": {"scope", "colspan", "rowspan"},
    "td": {"colspan", "rowspan"},
    "col": {"span"},
    "colgroup": {"span"},
}

# Только безопасные схемы; остальное (javascript:, data:, vbscript:) отбрасывается.
_NH3_URL_SCHEMES: set[str] = {"http", "https", "mailto"}


# ---------------------------------------------------------------------------
# stdlib-fallback: консервативная регулярная чистка
# ---------------------------------------------------------------------------

# Теги активного контента: вырезаются вместе с содержимым (это не проза-evidence).
_DANGEROUS_BLOCK_TAGS: tuple[str, ...] = (
    "script",
    "style",
    "iframe",
    "object",
    "embed",
    "template",
    "noscript",
)

_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
# on-атрибуты: `\s+onxxx=` + значение в кавычках или без.
_EVENT_HANDLER_RE = re.compile(
    r"""\s+on[a-z]+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+)""", re.IGNORECASE
)
# Опасные URL-схемы внутри атрибутов (заменяются на инертный фрагмент).
_DANGEROUS_URL_RE = re.compile(
    r"""(?i)\b(?:javascript|vbscript)\s*:|data\s*:\s*text/html""",
)


def _stdlib_sanitize_html(html: str) -> str:
    """Консервативная регулярная чистка без внешних зависимостей.

    Удаляются только распознанные опасные фрагменты; вся остальная разметка и
    текстовые узлы остаются без изменений.
    """
    cleaned = _COMMENT_RE.sub("", html)
    for tag in _DANGEROUS_BLOCK_TAGS:
        # Парный блок с содержимым, затем возможные одиночные теги.
        cleaned = re.sub(
            rf"<{tag}\b[^>]*>.*?</{tag}\s*>", "", cleaned, flags=re.IGNORECASE | re.DOTALL
        )
        cleaned = re.sub(rf"</?{tag}\b[^>]*>", "", cleaned, flags=re.IGNORECASE)
    cleaned = _EVENT_HANDLER_RE.sub("", cleaned)
    cleaned = _DANGEROUS_URL_RE.sub("#", cleaned)
    return cleaned


def sanitize_html(html: str) -> str:
    """Удаляет активный/опасный HTML, сохраняя текстовые узлы.

    Сначала пробуется ``nh3.clean`` с явным allow-list тегов/атрибутов и схем URL.
    Если ``nh3`` не установлен или его вызов завершился ошибкой — используется
    :func:`_stdlib_sanitize_html`. Пустая строка возвращается как есть.

    Функция **не мутирует** обычный текст: удаляется только опасная структура.
    """
    if not html:
        return html
    if _NH3_AVAILABLE and nh3 is not None:
        try:
            return nh3.clean(
                html,
                tags=_NH3_TAGS,
                attributes=_NH3_ATTRIBUTES,
                url_schemes=_NH3_URL_SCHEMES,
                strip_comments=True,
            )
        except Exception:  # noqa: BLE001 - деградация к stdlib-fallback
            pass
    return _stdlib_sanitize_html(html)


# ---------------------------------------------------------------------------
# Управляющие символы
# ---------------------------------------------------------------------------

# Сохраняем \t (0x09) и \n (0x0a); удаляем прочие C0, DEL и C1.
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f\x80-\x9f]")


def strip_control_chars(text: str) -> str:
    """Удаляет управляющие символы, сохраняя ``\\n`` и ``\\t``.

    Удаляются C0 (кроме таба и перевода строки), DEL и C1 — они могут ломать
    логи/промпты и не несут смысла в текстовом evidence. Пробелы и проза не трогаются.
    """
    return _CONTROL_CHARS_RE.sub("", text)


# ---------------------------------------------------------------------------
# Нейтрализация каркаса промпт-инъекций
# ---------------------------------------------------------------------------

# Нейтральная метка вместо распознанного каркаса.
_INJECTION_MARKER = "[injection-scaffold-removed]"

_INJECTION_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Спец-токены чат-шаблонов.
    re.compile(
        r"<\|(?:system|user|assistant|tool|function|im_start|im_end|endoftext)\|>",
        re.IGNORECASE,
    ),
    re.compile(r"\[/?INST\]", re.IGNORECASE),
    re.compile(r"<<\s*/?\s*SYS\s*>>", re.IGNORECASE),
    # Tool-call-подобные блоки (структурный каркас, не проза).
    re.compile(r"<tool_call>.*?</tool_call>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<function_call>.*?</function_call>", re.IGNORECASE | re.DOTALL),
    # Фразы-переопределения (сам приказ; остальной текст строки сохраняется).
    re.compile(
        r"(?:ignore|disregard)\s+(?:all\s+)?(?:previous|prior|above)\s+instructions?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:игнорируй|забудь|не\s+учитывай)\s+(?:все\s+)?"
        r"(?:предыдущие|прежние|выше)\s+инструкции",
        re.IGNORECASE,
    ),
)

# Псевдо-роль в начале строки: заменяется только маркер, остаток строки сохраняется.
_FAKE_ROLE_RE = re.compile(
    r"(?im)^[ \t]*(?:system|assistant|developer)[ \t]*[:>][ \t]*"
)


def neutralize_prompt_injection_marks(text: str) -> str:
    """Помечает/вырезает каркас промпт-инъекций, не переписывая прозу.

    Распознаются только узкие структурные шаблоны: спец-токены чат-шаблонов
    (``<|system|>``, ``[INST]``, ``<<SYS>>``), псевдо-роли в начале строки
    (``System:``, ``Assistant>``), фразы-переопределения («ignore previous
    instructions») и tool-call-подобные блоки. Каждый распознанный фрагмент
    заменяется на :data:`_INJECTION_MARKER`; остальной текст, включая хвост строки
    после псевдо-роли, остаётся **байт-в-байт неизменным**.

    Важно для §10/C.2: функция не нормализует и не переписывает содержательную
    прозу, поэтому цитата обычного предложения остаётся подстрокой очищенного
    чанка. Применяйте функцию **до** чанкинга/хеширования, чтобы чанк и цитата
    оценивались по одному и тому же тексту. Если genuine-цитата сама содержит
    распознанный каркас (например, документ цитирует инъекцию), она будет
    изменена — это осознанный компромисс precision-first, а шаблоны намеренно узкие.
    """
    if not text:
        return text
    result = _FAKE_ROLE_RE.sub(_INJECTION_MARKER + " ", text)
    for pattern in _INJECTION_PATTERNS:
        result = pattern.sub(_INJECTION_MARKER, result)
    return result
