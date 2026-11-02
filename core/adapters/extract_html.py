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

import codecs
import re
from collections.abc import Callable
from pathlib import Path

from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage
from core.util.hashing import content_hash
from core.util.sanitize import finalize_extracted_text, sanitize_html

try:  # необязательная зависимость
    import trafilatura  # type: ignore[import-not-found]

    _TRAFILATURA_AVAILABLE = True
except ImportError:  # pragma: no cover - зависит от окружения
    trafilatura = None  # type: ignore[assignment]
    _TRAFILATURA_AVAILABLE = False

# ``extract_fn``: HTML (строка) → markdown/text или ``None``, если извлечение не удалось.
ExtractFn = Callable[[str], "str | None"]

_CHARSET_RE = re.compile(r"""charset\s*=\s*["']?([A-Za-z0-9._:-]+)""", re.IGNORECASE)

#: Мета-тег целиком: charset ищется **внутри** тега, иначе строка вида
#: ``<p>…charset=…</p>`` в прозе давала бы ложное объявление.
_META_TAG_RE = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)

#: Сниффинг мета-объявления ограничен началом файла: HTML5 требует объявлять
#: кодировку в первых 1024 байтах, здесь — с запасом 4096.
_META_SNIFF_BYTES = 4096

#: BOM → кодек. Порядок важен: UTF-8 проверяется первым (его BOM не является
#: префиксом UTF-16-ных), UTF-16 LE/BE — как требует §10 (кодировка объявлена самим файлом).
_BOM_CODECS: tuple[tuple[bytes, str], ...] = (
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF16_LE, "utf-16-le"),
    (codecs.BOM_UTF16_BE, "utf-16-be"),
)


def _charset_from_content_type(content_type: str | None) -> str | None:
    """Извлекает ``charset`` из заголовка Content-Type (или ``None``)."""
    if not content_type:
        return None
    match = _CHARSET_RE.search(content_type)
    return match.group(1) if match else None


def _bom_charset(content: bytes) -> str | None:
    """Кодек по BOM файла (UTF-8 / UTF-16 LE / UTF-16 BE) или ``None``.

    BOM — самое сильное объявление: он часть самих байтов, а не утверждение
    сервера или разметки. Никакого угадывания: без BOM возвращается ``None``.
    """
    for bom, charset in _BOM_CODECS:
        if content.startswith(bom):
            return charset
    return None


def _charset_from_meta(content: bytes) -> str | None:
    """``charset`` из первых 4096 байт: ``<meta charset=…>`` или ``<meta http-equiv=…>``.

    Сниффинг идёт по **байтам**: начало файла разбирается как latin-1 (однобайтовое
    отображение, объявление кодировки читается и в однобайтовых, и в UTF-8/UTF-16
    документах), затем из найденных мета-тегов извлекается ``charset`` тем же
    :data:`_CHARSET_RE`. Оба объявления (HTML5 ``charset`` и legacy ``http-equiv``)
    содержат подстроку ``charset=``, поэтому разбор один. Если объявления нет —
    ``None`` (кодировка не угадывается).
    """
    head = content[:_META_SNIFF_BYTES].decode("latin-1")
    for tag in _META_TAG_RE.findall(head):
        match = _CHARSET_RE.search(tag)
        if match:
            return match.group(1)
    return None


def _decode_bytes(content: bytes, charset: str | None) -> str:
    """Декодирует байты объявленным кодеком; неизвестный/битый — UTF-8 replace.

    Неизвестное имя кодека (``LookupError``) и невозможность декодировать
    (``UnicodeError``, напр. ``utf-16`` без BOM) — **честный откат**: текст с
    ``errors="replace"``, где потеря видна как U+FFFD, а не исключение и не
    подмена кодировки. BOM, если он был в байтах, снимается с результата.
    """
    if charset:
        try:
            text = content.decode(charset, errors="replace")
        except (LookupError, UnicodeError):
            text = content.decode("utf-8", errors="replace")
    else:
        text = content.decode("utf-8", errors="replace")
    return text.removeprefix("\ufeff")


def _decode_page(page: RawPage) -> str:
    """Декодирует байты страницы: BOM → Content-Type → ``<meta>`` → UTF-8 replace.

    Порядок — от самого сильного объявления к самому слабому: BOM живёт в самих
    байтах, ``Content-Type`` — утверждение сервера, ``<meta charset>`` — утверждение
    самой разметки. Только при отсутствии **всех** объявлений включается UTF-8 с
    ``errors="replace"`` (замена видна в тексте): угадывания кодировки нет.

    Байты берутся из ``page.content``; если их там нет, а задан ``page.source_path``
    (страница несёт только путь к retained-файлу, I19) — файл дочитывается здесь.
    HTML-экстрактор работает по байтам, поэтому это единственное место, где
    retained-файл поднимается в RAM: документный путь читает docling сам
    (``DoclingDocumentExtractor.extract_path``), а HTML-страницы на порядки меньше.
    """
    content = page.content
    if not content and page.source_path:
        content = Path(page.source_path).read_bytes()
    charset = (
        _bom_charset(content)
        or _charset_from_content_type(page.content_type)
        or _charset_from_meta(content)
    )
    return _decode_bytes(content, charset)


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

    version = "trafilatura-html-3"

    #: B3: адаптер читает байты **локального** файла (``page.source_path``), поэтому годится
    #: для членов контейнеров (спуск). Шов ``split`` этого не умеет — см. ``RemoteExtractor``.
    supports_source_path = True

    def __init__(self, extract_fn: ExtractFn | None = None, *, sanitize: bool = True) -> None:
        """``extract_fn`` — внедряемая функция извлечения (для тестов/AB).

        Если не задана — используется обёртка над ``trafilatura`` (ленивая
        необязательная зависимость). ``sanitize=False`` отключает только чистку
        **HTML-структуры** (``sanitize_html``) — финальная очистка текста
        (:func:`core.util.sanitize.finalize_extracted_text`) выполняется всегда:
        это граница evidence, а не опция удобства.
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
        """Чистит управляющие символы и каркас инъекций, не трогая прозу (T7.5).

        Единая граница очистки текста
        (:func:`core.util.sanitize.finalize_extracted_text`) — та же, что у текстового
        и документного читателей: текст, который вернул адаптер, уже нельзя «забыть
        очистить». Вызывается **до** ``content_hash``.
        """
        return finalize_extracted_text(text)

    def _error(self, message: str) -> ExtractedDoc:
        return ExtractedDoc(
            text="",
            content_hash=content_hash(""),
            extractor_version=self.version,
            error=message,
        )


# Обратная совместимость по стилю именования с документным адаптером.
TrafilaturaHtmlExtractor = HtmlExtractor
