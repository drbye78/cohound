"""Экстрактор plain text/markdown → ``ExtractedDoc`` — без trafilatura (текстовый путь).

Plain text и markdown — **первый класс** наравне с HTML и PDF (решение владельца): у них
свой читатель, а не «HTML-путь по остаточному принципу». Прежний маршрут отдавал
``.txt``/``.rtf`` трафилатуре, та возвращала ``None`` на не-HTML, и документ умирал в
``parser_failed`` — блокирующем классе отказа, из-за которого
``corpus_coverage.complete`` не мог стать ``True`` (измерено на 22.10: 72 × ``.txt``).

Что делает читатель (и только это — разметку он **не** интерпретирует):

* декодирует байты: BOM (UTF-8/UTF-16 LE/BE) → charset из ``Content-Type`` →
  UTF-8 с ``errors="replace"`` (у плоского текста нет разметки, поэтому ``<meta charset>``
  не читается; кодировка не угадывается — битые байты не роняют документ, но и не
  прячутся: замена видна в тексте);
* снимает BOM, приводит CRLF/CR к LF;
* вычищает NUL/управляющие символы, сохраняя ``\\t``, и каркас промпт-инъекций — единой
  границей evidence :func:`core.util.sanitize.finalize_extracted_text` (та же, что на
  HTML-границе, T7.5);
* схлопывает серии пустых строк (>2 подряд) до одной — plain text из CMS часто несёт
  хвосты форматирования, а чанкер абзацев считает пустую строку границей блока;
* пустой/пробельный результат — **видимая** ошибка ``"текст пуст"``, а не пустой
  документ: «прочитали и там пусто» не должно выглядеть как успешная экстракция.

``page_count`` — ``None``: у плоского текста нет страниц, выдумывать их нельзя (A3).
Нейтрален (§1.3): ни одного предметного термина.
"""

from __future__ import annotations

import re
from pathlib import Path

from core.adapters.extract_html import (
    _bom_charset,
    _charset_from_content_type,
    _decode_bytes,
)
from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage
from core.util.document_formats import FAMILY_TEXT, textual_family
from core.util.hashing import content_hash
from core.util.sanitize import finalize_extracted_text

__all__ = ["TEXT_EXTRACTOR_VERSION", "TextExtractor", "normalize_text"]

#: Стабильная версия читателя (провенанс/``execution_contract``): меняется только при
#: изменении нормализации текста — это меняет chunk-set и обязано быть видно (§13).
TEXT_EXTRACTOR_VERSION = "text-plain-2"

#: Серия пустых (в т.ч. из одних пробелов) строк длиннее одной → ровно одна пустая.
_BLANK_RUN_RE = re.compile(r"\n[ \t]*\n(?:[ \t]*\n)+")


def normalize_text(text: str) -> str:
    """Нормализует плоский текст: BOM/переводы строк/управляющие символы/пустые строки.

    Порядок важен: сначала переводы строк (CRLF/CR → LF — до схлопывания пустых строк,
    иначе ``\\r\\n\\r\\n`` не считалось бы пустой строкой), затем единая граница очистки
    текста (:func:`core.util.sanitize.finalize_extracted_text`: управляющие символы, затем
    каркас промпт-инъекций — тот же порядок, что у остальных читателей), затем пустые
    строки и обрезка краёв.
    """
    text = text.removeprefix("\ufeff")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = finalize_extracted_text(text)
    text = _BLANK_RUN_RE.sub("\n\n", text)
    return text.strip()


def _decode(content: bytes, content_type: str | None) -> str:
    """Декодирует байты: BOM → ``Content-Type`` → UTF-8 с заменой.

    У плоского текста **нет** разметки, поэтому ``<meta charset>`` не существует: читатель
    не выдумывает объявление, которого в формате не бывает, и не угадывает кодировку.
    BOM — сильнее заголовка (он часть байтов), заголовок — сильнее догадки.

    Неизвестный/битый charset — fallback на UTF-8 (не исключение): заголовок сервера не
    повод потерять документ, а замена символов честно видна в тексте.
    """
    charset = _bom_charset(content) or _charset_from_content_type(content_type)
    return _decode_bytes(content, charset)


class TextExtractor:
    """Plain text/markdown → ``ExtractedDoc``. Реализация ``ExtractorPort`` (текстовый путь).

    Никогда не бросает: сбой чтения/пустой текст → ``ExtractedDoc.error``. ``route`` не
    влияет на путь (маршрут разрешает композит в composition root) — как у
    :class:`core.adapters.extract_html.HtmlExtractor`.
    """

    version = TEXT_EXTRACTOR_VERSION

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """``RawPage`` → ``ExtractedDoc`` (декодирование + нормализация, без разметки)."""
        # Байты берутся из страницы; если их там нет, а задан ``source_path`` (страница
        # несёт только путь к retained-файлу, I19) — файл дочитывается здесь. Размер уже
        # ограничен осью ``crawl.max_response_bytes``: текстовый формат идёт страничным
        # путём, а не артефактным, поэтому собственный потолок здесь не нужен.
        try:
            content = page.content
            if not content and page.source_path:
                content = Path(page.source_path).read_bytes()
        except OSError as exc:
            return self._error(f"текст: {exc}")

        text = normalize_text(_decode(content, page.content_type))
        if not text.strip():
            return self._error("текст пуст")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
            page_count=None,
            family=textual_family(page.url, page.content_type) or FAMILY_TEXT,
        )

    def _error(self, message: str) -> ExtractedDoc:
        return ExtractedDoc(
            text="",
            content_hash=content_hash(""),
            extractor_version=self.version,
            page_count=None,
            error=message,
            family=FAMILY_TEXT,
        )
