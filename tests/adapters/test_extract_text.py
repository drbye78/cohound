"""Тесты текстового читателя ``TextExtractor`` (``text-plain-1``).

Читатель plain text/markdown не использует trafilatura: он декодирует, нормализует и
честно сообщает о пустом результате. Проверяются ровно те дефекты, из-за которых
``.txt`` умирал в ``parser_failed`` (22.10: 72 файла):

* BOM/CRLF/управляющие символы не портят текст (и не остаются в evidence);
* charset из ``Content-Type`` уважается (иначе кириллица превращалась бы в U+FFFD);
* пустой/пробельный документ — **видимая** ошибка ``"текст пуст"``, не пустой успех;
* версия стабильна (она входит в ``extractor_version`` документа).

Сеть не нужна: страницы собираются в памяти. Нейтрален (§1.3).
"""

from __future__ import annotations

from pathlib import Path

from core.adapters.extract_text import (
    TEXT_EXTRACTOR_VERSION,
    TextExtractor,
    normalize_text,
)
from core.ports.extractor import ExtractorPort
from core.ports.types import ExtractedDoc, RawPage
from core.util.hashing import content_hash


def _page(
    content: bytes,
    *,
    url: str = "https://docs.example.test/files/lab.txt",
    content_type: str | None = "text/plain; charset=utf-8",
    source_path: str | None = None,
) -> RawPage:
    return RawPage(
        url=url,
        content=content,
        fetched_at="2026-10-22T00:00:00Z",
        content_type=content_type,
        source_path=source_path,
    )


# ---------------------------------------------------------------------------
# Базовый контракт
# ---------------------------------------------------------------------------


def test_extractor_implements_port() -> None:
    extractor = TextExtractor()
    assert isinstance(extractor, ExtractorPort)
    assert extractor.version == TEXT_EXTRACTOR_VERSION == "text-plain-1"


def test_plain_text_roundtrip() -> None:
    text = "Установка PostgreSQL\n\nШаг 1: распакуйте архив."
    doc = TextExtractor().extract(_page(text.encode("utf-8")))

    assert isinstance(doc, ExtractedDoc)
    assert doc.text == text
    assert doc.content_hash == content_hash(text)
    assert doc.extractor_version == TEXT_EXTRACTOR_VERSION
    assert doc.page_count is None
    assert doc.error is None
    # Семейство — вход выбора чанкера (абзацный, не заголовочный).
    assert doc.family == "text"


def test_markdown_family_is_reported_for_md() -> None:
    doc = TextExtractor().extract(
        _page(b"# Title\n\nBody", url="https://x.test/guide.md", content_type="text/markdown")
    )

    assert doc.error is None
    assert doc.family == "markdown"
    # Разметка не интерпретируется читателем: это дело чанкера.
    assert doc.text == "# Title\n\nBody"


# ---------------------------------------------------------------------------
# Нормализация
# ---------------------------------------------------------------------------


def test_utf8_bom_is_stripped() -> None:
    doc = TextExtractor().extract(_page("\ufeffтекст".encode()))

    assert doc.text == "текст"


def test_crlf_and_cr_are_normalized_to_lf() -> None:
    doc = TextExtractor().extract(_page(b"a\r\nb\rc\nd"))

    assert doc.text == "a\nb\nc\nd"


def test_nul_and_control_chars_are_stripped_but_tab_kept() -> None:
    raw = b"a\x00b\x07c\td\x1be"
    doc = TextExtractor().extract(_page(raw))

    assert doc.text == "abc\tde"
    assert "\x00" not in doc.text
    assert "\t" in doc.text


def test_more_than_two_blank_lines_collapse_to_one() -> None:
    doc = TextExtractor().extract(_page(b"a\n\n\n\n\nb\n \n \n \nc"))

    assert doc.text == "a\n\nb\n\nc"


def test_single_blank_line_is_preserved() -> None:
    """Абзацная граница (одна пустая строка) — смысловая единица чанкера."""
    doc = TextExtractor().extract(_page(b"a\n\nb"))

    assert doc.text == "a\n\nb"


def test_normalize_text_is_idempotent() -> None:
    once = normalize_text("\ufeffa\r\n\r\n\r\nb\x00")
    assert normalize_text(once) == once


# ---------------------------------------------------------------------------
# Кодировка
# ---------------------------------------------------------------------------


def test_charset_from_content_type_is_used() -> None:
    doc = TextExtractor().extract(
        _page("привет".encode("koi8-r"), content_type="text/plain; charset=koi8-r")
    )

    assert doc.text == "привет"


def test_unknown_charset_falls_back_to_utf8() -> None:
    doc = TextExtractor().extract(
        _page("привет".encode(), content_type="text/plain; charset=no-such-charset")
    )

    assert doc.text == "привет"


def test_invalid_bytes_do_not_crash() -> None:
    doc = TextExtractor().extract(_page(b"\xff\xfe broken", content_type="text/plain"))

    assert doc.error is None
    assert "broken" in doc.text


# ---------------------------------------------------------------------------
# Пустой результат — видимая ошибка
# ---------------------------------------------------------------------------


def test_empty_content_is_a_visible_error() -> None:
    doc = TextExtractor().extract(_page(b""))

    assert doc.text == ""
    assert doc.error == "текст пуст"
    assert doc.content_hash == content_hash("")


def test_whitespace_only_content_is_a_visible_error() -> None:
    doc = TextExtractor().extract(_page(b" \n\t\r\n\x00  "))

    assert doc.error == "текст пуст"


# ---------------------------------------------------------------------------
# Retained-файл (I19): тело может лежать на диске
# ---------------------------------------------------------------------------


def test_source_path_is_read_when_content_is_empty(tmp_path: Path) -> None:
    retained = tmp_path / "abc.bin"
    retained.write_bytes("из файла".encode())

    doc = TextExtractor().extract(_page(b"", source_path=str(retained)))

    assert doc.text == "из файла"
    assert doc.error is None


def test_missing_source_path_is_a_visible_error(tmp_path: Path) -> None:
    doc = TextExtractor().extract(_page(b"", source_path=str(tmp_path / "absent.bin")))

    assert doc.text == ""
    assert doc.error is not None and doc.error.startswith("текст:")
