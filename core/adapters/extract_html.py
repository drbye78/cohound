"""Экстрактор HTML → markdown/text (trafilatura) — реализация ``ExtractorPort`` (W7/T7.2).

Отдельный от документного контура (``extract_document.py``, docling) HTML-адаптер:
формат выводится из ``RawPage`` (url/content-type), реестр поддерживаемых форматов
остаётся единым в ``extract_document.SUPPORTED_FORMATS`` (архитектура 01 §3.5, дрейф
контракта). Санитайзинг — на границе экстракции (T7.5, 02 §C.2): опасная HTML-структура
вырезается до trafilatura, а извлечённый текст освобождается от управляющих символов и
каркаса промпт-инъекций.

Грациозная деградация (как у ``DoclingDocumentExtractor``): отсутствие ``trafilatura``
или ошибка разбора возвращают ``ExtractedDoc`` с пустым ``text`` и заполненным ``error``,
а не исключение. ``trafilatura`` импортируется лениво и является необязательной
зависимостью; для тестов можно внедрить собственный ``extract_fn``. Нейтрален (§1.3).
"""

from __future__ import annotations

import re
from collections.abc import Callable

from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage
from core.util.hashing import content_hash
from core.util.sanitize import (
    neutralize_prompt_injection_marks,
    sanitize_html,
    strip_control_chars,
)

try:  # необязательная зависимость
    import trafilatura  # type: ignore[import-not-found]

    _TRAFILATURA_AVAILABLE = True
except ImportError:  # pragma: no cover - зависит от окружения
    trafilatura = None  # type: ignore[assignment]
    _TRAFILATURA_AVAILABLE = False

# ``extract_fn``: HTML (строка) → markdown/text или ``None``, если извлечение не удалось.
ExtractFn = Callable[[str], "str | None"]

_CHARSET_RE = re.compile(r"""charset\s*=\s*["']?([A-Za-z0-9._:-]+)""", re.IGNORECASE)


def _charset_from_content_type(content_type: str | None) -> str | None:
    """Извлекает ``charset`` из заголовка Content-Type (или ``None``)."""
    if not content_type:
        return None
    match = _CHARSET_RE.search(content_type)
    return match.group(1) if match else None


def _decode_page(page: RawPage) -> str:
    """Декодирует байты страницы: charset из content-type, иначе UTF-8 replace."""
    charset = _charset_from_content_type(page.content_type)
    if charset:
        try:
            text = page.content.decode(charset, errors="replace")
        except (LookupError, UnicodeError):
            text = page.content.decode("utf-8", errors="replace")
    else:
        text = page.content.decode("utf-8", errors="replace")
    # Снимаем возможный UTF-8 BOM, оставшийся от исходного потока.
    return text.removeprefix("\ufeff")


def _default_extract(html: str) -> str | None:
    """Основной путь: trafilatura main-content → markdown."""
    if trafilatura is None:  # pragma: no cover - защита от рассинхрона флага
        raise RuntimeError("trafilatura не установлен")
    return trafilatura.extract(
        html,
        output_format="markdown",
        include_comments=False,
        include_tables=True,
    )


class HtmlExtractor:
    """HTML → markdown (trafilatura). Реализация ``ExtractorPort`` (Ш5)."""

    version = "trafilatura-html-1"

    def __init__(self, extract_fn: ExtractFn | None = None, *, sanitize: bool = True) -> None:
        """``extract_fn`` — внедряемая функция извлечения (для тестов/AB).

        Если не задана — используется обёртка над ``trafilatura`` (ленивая
        необязательная зависимость). ``sanitize=False`` отключает чистку на границе.
        """
        self._extract_fn: ExtractFn = extract_fn or _default_extract
        self._sanitize = sanitize

    @property
    def available(self) -> bool:
        """Доступен ли экстрактор (внедрённая функция или установленная trafilatura)."""
        return self._extract_fn is not _default_extract or _TRAFILATURA_AVAILABLE

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """``RawPage`` → ``ExtractedDoc``. Никогда не бросает: сбой → ``error``.

        ``route`` здесь не влияет на путь: HTML-экстрактор всегда трактует страницу как
        HTML (маршрут ``document``/``auto`` разрешается композитом в composition root).
        """
        try:
            html = _decode_page(page)
        except Exception as exc:  # noqa: BLE001 - грациозная деградация адаптера
            return self._error(f"декодирование: {exc}")

        if self._sanitize:
            try:
                html = sanitize_html(html)
            except Exception as exc:  # noqa: BLE001
                return self._error(f"санитизация: {exc}")

        try:
            extracted = self._extract_fn(html)
        except Exception as exc:  # noqa: BLE001 - любой сбой парсера → error
            return self._error(f"trafilatura: {exc}")

        if extracted is None:
            return self._error("trafilatura: извлечение не дало текста")

        text = self._postprocess(extracted)
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
        )

    @staticmethod
    def _postprocess(text: str) -> str:
        """Чистит управляющие символы и каркас инъекций, не трогая прозу (T7.5)."""
        return neutralize_prompt_injection_marks(strip_control_chars(text))

    def _error(self, message: str) -> ExtractedDoc:
        return ExtractedDoc(
            text="",
            content_hash=content_hash(""),
            extractor_version=self.version,
            error=message,
        )


# Обратная совместимость по стилю именования с документным адаптером.
TrafilaturaHtmlExtractor = HtmlExtractor
