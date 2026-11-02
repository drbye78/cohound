"""Санитизация контента на границе экстракции (T7.5, §10 / 02 §C.2).

Четыре независимые чистые функции:

- :func:`strip_active_html` — **пре-клин структуры** для HTML-экстрактора (решение
  владельца D1, 02.11.2026): удаляет активный контент (``script``/``style``/``noscript``/
  ``iframe``/``frame``/``frameset``/``object``/``embed``/``applet``/``svg``/``template``)
  вместе с содержимым, event-handler'ы ``on*`` и опасные URL-схемы, **сохраняя**
  ``class``/``id``/``role`` и оболочку документа. Это вход ``trafilatura``: эвристики
  main-content различают статью и chrome именно по классам/ролям, а allow-list
  :func:`sanitize_html` их снимал — замер 02.11.2026: cookie-баннер попадал в текст у
  **400 из 400** страниц, без санитайза — у 99 из 400, объём ×1.12 медиана / ×2.63 p90.
- :func:`sanitize_html` — удаляет активный/опасный HTML (``<script>``/``<style>``,
  event-handler'ы ``on*``, опасные URL-схемы ``javascript:``/``vbscript:``/``data:text/html``,
  комментарии). Основной путь — ``nh3`` (Rust/ammonia, MIT); при отсутствии ``nh3`` (или
  сбое его конфигурации) работает **stdlib-fallback** на регулярных выражениях. Он удаляет
  только опасные фрагменты, оставляя остальную разметку и **все текстовые узлы**
  байт-в-байт неизменными. **Оболочка документа** (``html``/``head``/``title``/``body``)
  при этом сохраняется в allow-list: ``trafilatura`` на следующем шаге ищет main-content
  внутри документа, и без сохранившегося ``<title>`` извлекает пустой текст
  (см. :data:`_NH3_DOCUMENT_SHELL`).
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
``nh3``, ``lxml`` и ``defusedxml`` — опциональны (``lxml`` приходит жёсткой
зависимостью ``trafilatura``, своей не вводится). Нейтрален (§1.3).
"""

from __future__ import annotations

import re
import warnings
from typing import Any

__all__ = [
    "finalize_extracted_text",
    "neutralize_prompt_injection_marks",
    "sanitize_html",
    "strip_active_html",
    "strip_control_chars",
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

# Оболочка документа — НЕ активный контент, но обязательна для экстракции.
# `nh3.clean` сериализует body-фрагмент, поэтому literal `<html>`/`<body>` он всё равно
# не выводит; реально сохраняемым и решающим маркером документа является `<title>`
# (его текст уже сохранялся и раньше, а вот тег выбрасывался). Без него `trafilatura`
# (следующий шаг, `extract_html.py::_default_extract`) не распознаёт документ и извлекает
# пустой текст — это была реальная регрессия (T7.5/Ш5). Расширяем allow-list ровно на
# оболочку `html`/`head`/`title`/`body`; активные (`script`/`style`/`iframe`/`object`/
# `embed`/`template`) остаются запрещёнными. Та же оболочка literal-тегами сохраняется
# stdlib-fallback'ом — теперь оба пути согласованы.
_NH3_DOCUMENT_SHELL: frozenset[str] = frozenset({"html", "head", "title", "body"})

# Разрешённый поднабор тегов: структура/разметка + оболочка документа,
# без активного контента.
_NH3_TAGS: set[str] = {
    "a", "abbr", "b", "blockquote", "br", "caption", "code", "col", "colgroup",
    "dd", "div", "dl", "dt", "em", "figcaption", "figure", "h1", "h2", "h3",
    "h4", "h5", "h6", "hr", "i", "img", "li", "ol", "p", "pre", "q", "s",
    "small", "span", "strong", "sub", "sup", "table", "tbody", "td", "tfoot",
    "th", "thead", "tr", "u", "ul",
} | _NH3_DOCUMENT_SHELL

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
    :func:`_stdlib_sanitize_html`; сбой ``nh3`` при этом **не молчит**: он поднимает
    ``RuntimeWarning`` (деградация к более слабому regex-пути должна быть видна).
    Пустая строка возвращается как есть.

    **Оболочка документа сохраняется намеренно** (``html``/``head``/``title``/``body``).
    ``nh3`` сериализует body-фрагмент, поэтому literal ``<html>``/``<body>`` не выводит,
    но тег ``<title>`` — решающий маркер документа для ``trafilatura`` — теперь
    сохраняется. Без него ``trafilatura`` — следующий шаг извлечения
    (``core/adapters/extract_html.py``) — не находит главный контент и выдаёт пустой
    текст. Это была реальная регрессия: ``extract_html`` санитайзит HTML **до** вызова
    ``trafilatura``, поэтому выброшенный ``<title>`` обнулял извлечение на обычных
    документах. stdlib-fallback оболочку и так не трогал — теперь оба пути согласованы.

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
        except Exception as exc:  # noqa: BLE001 - деградация к stdlib-fallback
            # Молчаливая деградация недопустима: stdlib-фолбэк — регулярные выражения, он
            # слабее allow-list ``nh3``, и «санитайзинг прошёл» перестало бы значить одно и
            # то же для двух путей. Отказ **видим** (``RuntimeWarning``), но не смертелен:
            # сломанный ``nh3`` не должен валить извлечение документа — фолбэк по-прежнему
            # снимает активный HTML, поэтому падать вызывающему коду не за что.
            warnings.warn(
                "nh3.clean не смог обработать HTML "
                f"({type(exc).__name__}: {exc}); "
                "санитизация деградировала к stdlib-regex-фолбэку",
                RuntimeWarning,
                stacklevel=2,
            )
    return _stdlib_sanitize_html(html)


# ---------------------------------------------------------------------------
# Пре-клин активного HTML с сохранением структуры (D1, решение владельца 02.11.2026)
# ---------------------------------------------------------------------------

try:  # pragma: no cover - зависит от окружения (приходит зависимостью trafilatura)
    from lxml import etree as _lxml_etree
    from lxml import html as _lxml_html
except ImportError:  # pragma: no cover
    _lxml_etree = None  # type: ignore[assignment]
    _lxml_html = None  # type: ignore[assignment]

# Активный контент: удаляется **вместе с содержимым** — это не проза-evidence,
# а исполняемый код либо встроенный документ.
_STRIP_TAGS: frozenset[str] = frozenset(
    {
        "applet",
        "embed",
        "frame",
        "frameset",
        "iframe",
        "noscript",
        "object",
        "script",
        "style",
        "svg",
        "template",
    }
)

# Атрибуты-URL: опасное значение снимается (атрибут остаётся инертным).
_URL_ATTRS: frozenset[str] = frozenset({"action", "formaction", "href", "src"})

# Опасная схема = исполняемый код (``javascript:``/``vbscript:``) либо активный документ
# (``data:text/html``). Прочие схемы и относительные URL остаются как есть.
_DANGEROUS_URL_PREFIXES: tuple[str, ...] = ("javascript:", "vbscript:", "data:text/html")

# Браузер игнорирует управляющие символы и пробелы внутри схемы, поэтому
# ``java&#9;script:`` — тот же ``javascript:``. Нормализация повторяет это правило.
_URL_NOISE_RE = re.compile(r"[\x00-\x20\x7f]")


def _local_name(name: str) -> str:
    """Локальное имя тега/атрибута в нижнем регистре (``{ns}script`` → ``script``)."""
    return name.rpartition("}")[2].lower()


def _is_dangerous_url(value: str) -> bool:
    """True, если значение атрибута-URL начинается с опасной схемы."""
    return _URL_NOISE_RE.sub("", value).lower().startswith(_DANGEROUS_URL_PREFIXES)


def _strip_active_tree(root: Any) -> None:
    """Вырезает активные элементы и атрибуты из разобранного дерева (in place).

    Удаление идёт по списку, снятому до мутации: обход живого дерева при удалении
    узлов пропускал бы соседей. Хвостовой текст удаляемого блока сохраняется
    (``drop_tree``), чтобы проза вокруг ``<script>`` не склеивалась.
    """
    for element in list(root.iter()):
        tag = element.tag
        if not isinstance(tag, str):  # комментарии и processing instructions — не теги
            continue
        if _local_name(tag) in _STRIP_TAGS:
            drop_tree = getattr(element, "drop_tree", None)
            if drop_tree is not None:
                drop_tree()
            else:  # pragma: no cover - все элементы lxml.html умеют drop_tree
                parent = element.getparent()
                if parent is not None:
                    parent.remove(element)
            continue
        for name in list(element.attrib):
            local = _local_name(name)
            if local.startswith("on"):
                del element.attrib[name]
            elif local in _URL_ATTRS and _is_dangerous_url(element.attrib[name]):
                element.attrib[name] = ""


def _serialize_html(root: Any) -> str:
    """Сериализует разобранное дерево обратно в юникод-HTML.

    Основной путь — XML-сериализатор: он **закрывает каждый тег**, поэтому результат
    остаётся неподвижной точкой. HTML-сериализатор опускает необязательные закрывающие
    теги: пустой ``<li>`` при повторном проходе получал ``</li>``, и второй вызов
    возвращал уже другой текст (замер 02.11.2026: 1 страница из 300 на sbertech).
    Текст извлечения от выбора сериализатора не зависит — 300/300 страниц дали
    одинаковый результат ``trafilatura`` обоими путями.

    На имени тега, недопустимом для XML, — HTML-сериализатор (дерево то же, поэтому
    это не смена качества пре-клина).
    """
    try:
        return _lxml_etree.tostring(root, encoding="unicode", method="xml")
    except (ValueError, TypeError):  # pragma: no cover - редкие имена тегов
        return _lxml_etree.tostring(root, encoding="unicode", method="html")


def strip_active_html(html: str) -> str:
    """Удаляет активный HTML, **сохраняя структуру документа** (пре-клин для trafilatura).

    **Зачем не** :func:`sanitize_html`. Allow-list ``nh3`` снимает ``class``/``id``/``role``
    целиком, а ``trafilatura`` (следующий шаг, ``core/adapters/extract_html.py``) отличает
    статью от chrome именно по ним. Замер 02.11.2026 на 400 страницах: после allow-list
    cookie-баннер попадал в извлечённый текст у **400 из 400** страниц, без чистки —
    у 99 из 400; объём текста ×1.12 медиана / ×2.63 p90 относительно режима без чистки.
    Структура — вход эвристики, поэтому её нельзя снимать «на всякий случай»: цена
    измерима и она уже заплачена.

    Удаляется **вместе с содержимым**: ``script``, ``style``, ``noscript``, ``iframe``,
    ``frame``, ``frameset``, ``object``, ``embed``, ``applet``, ``svg``, ``template``.
    Снимаются: event-handler'ы ``on*`` и значения ``href``/``action``/``formaction``/
    ``src`` с опасными схемами (``javascript:``/``vbscript:``/``data:text/html``, включая
    замаскированные управляющими символами). Сохраняются: ``class``, ``id``, ``role``,
    все текстовые узлы и оболочка документа ``html``/``head``/``title``/``body`` —
    снятый ``<title>`` уже был замеренной регрессией, и повторять её нельзя.

    Разбор — ``lxml.html`` (жёсткая зависимость ``trafilatura``; новых зависимостей не
    вводится), сериализация обратно в юникод (:func:`_serialize_html` — XML-метод, чтобы
    закрытые теги делали результат неподвижной точкой). Функция **идемпотентна**, пустая
    строка возвращается как есть.

    **Откат видим.** Без ``lxml`` (или при ошибке разбора) функция возвращает результат
    :func:`sanitize_html` и поднимает ``RuntimeWarning``: два пути дают **разное качество
    извлечения** (allow-list теряет структуру и тянет chrome в текст), поэтому молчаливая
    подмена запрещена — деградация обязана быть видна в логе прогона.
    """
    if not html.strip():
        return html
    if _lxml_html is None or _lxml_etree is None:
        warnings.warn(
            "lxml недоступен: пре-клин HTML деградировал к allow-list sanitize_html "
            "(структура class/id/role будет потеряна, chrome может попасть в текст)",
            RuntimeWarning,
            stacklevel=2,
        )
        return sanitize_html(html)
    try:
        root = _lxml_html.document_fromstring(html)
        _strip_active_tree(root)
        return _serialize_html(root)
    except Exception as exc:  # noqa: BLE001 - деградация к allow-list, но видимая
        warnings.warn(
            "lxml не смог разобрать HTML "
            f"({type(exc).__name__}: {exc}); "
            "пре-клин деградировал к allow-list sanitize_html "
            "(структура class/id/role будет потеряна)",
            RuntimeWarning,
            stacklevel=2,
        )
        return sanitize_html(html)


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


def finalize_extracted_text(text: str) -> str:
    """Единая граница очистки текста экстракции — **обязательна для каждого читателя**.

    **Зачем одна функция, а не «каждый адаптер сам».** До 02.11.2026 очистка жила в двух
    местах (``extract_html._postprocess`` и ``extract_text``), а документный путь
    (docling: PDF и Office) и механизм ``text_layer`` не вызывали её вовсе. Разница была
    невидимой: HTML-страница с фразой-переопределением доходила до оценщика помеченной,
    а тот же текст в DOCX — дословно (замер 02.11.2026: ``.txt`` →
    ``[injection-scaffold-removed]``, ``.docx`` → фраза целиком). Граница одна, вызов —
    в каждом читателе: адаптер возвращает текст, который уже нельзя «забыть очистить».

    **Порядок фиксирован.** Сначала :func:`strip_control_chars` — управляющие символы
    рвут и лексический индекс, и сверку цитаты с чанком (I22). Затем
    :func:`neutralize_prompt_injection_marks` — узкие структурные шаблоны; прозу она не
    переписывает. Обратный порядок оставил бы управляющие символы внутри уже помеченного
    текста и дал бы другой ``content_hash`` при том же смысле.

    **Идемпотентна**: у метки замены нет шаблонов, повторный вызов текст не меняет.

    **Вызывается до** ``content_hash``: цитата и чанк обязаны считаться по одному и тому
    же тексту. Текст, изменённый здесь, обязан изменить версию читателя — версии
    читателей входят в ``extractor_stack`` (объявленный вход ``run_id``, §9).
    """
    return neutralize_prompt_injection_marks(strip_control_chars(text))
