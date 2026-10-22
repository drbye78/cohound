"""Класс «терминальный артефакт»: ``core/util/document_formats.py`` (P6b).

Проверяется единый предикат допуска :func:`is_terminal_artifact` и его половинки:
**документ** (``pdf``/``docx``/``pptx``/``xlsx`` — совпадает с реестром экстрактора) **или**
**заявленный контейнер** (``zip``/``7z``/``rar``). Ключевые границы класса:

* незаявленный контейнер (``ova``/``iso``/``gz``/``tar``) остаётся обычным бинарным
  артефактом — сужение ``attachment_formats`` не расширяет допуск;
* OOXML (``.docx``) — **документ**, а не контейнер (различаем по маркеру
  ``[Content_Types].xml``), поэтому он терминален и без заявленных контейнеров;
* ``.rtf`` намеренно **не** допускается: у реестра экстрактора нет читателя RTF.

Данные нейтральны (§1.3): только суффиксы, MIME-типы и магия байтов.
"""

from __future__ import annotations

import pytest

from core.util.document_formats import (
    DOCUMENT_SUFFIXES,
    MARKDOWN_SUFFIXES,
    PLAIN_TEXT_SUFFIXES,
    TEXTUAL_SUFFIXES,
    classify_archive_bytes,
    is_epub,
    is_terminal_artifact,
    is_terminal_artifact_content_type,
    is_terminal_artifact_url,
    textual_content_type,
    textual_family,
    textual_suffix,
)

_ZIP_LOCAL = b"PK\x03\x04"
_OOXML = b"[Content_Types].xml"


# ---------------------------------------------------------------------------
# По URL-суффиксу (pre-fetch)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "https://x.test/files/manual.pdf",
        "https://x.test/files/manual.PDF",
        "https://x.test/files/spec.docx",
        "https://x.test/files/deck.pptx",
        "https://x.test/files/sheet.xlsx",
        "https://x.test/download/report.pdf?token=1#page=2",
        "https://x.test/files/bundle.zip",
        "https://x.test/files/bundle.7z",
        "https://x.test/files/bundle.rar",
    ],
)
def test_terminal_artifact_url_admits_documents_and_declared_containers(url: str) -> None:
    assert is_terminal_artifact_url(url) is True
    assert is_terminal_artifact(url) is True


@pytest.mark.parametrize(
    "url",
    [
        "https://x.test/docs/page.html",
        "https://x.test/docs/page",
        "https://x.test/",
        # Незаявленные контейнеры — не артефакты допуска (остаются бинарными).
        "https://x.test/files/disk.ova",
        "https://x.test/files/disk.iso",
        "https://x.test/files/archive.tar.gz",
        # RTF ждёт своей полосы (striprtf): читателя в реестре нет.
        "https://x.test/files/memo.rtf",
    ],
)
def test_terminal_artifact_url_rejects_pages_and_undeclared_binaries(url: str) -> None:
    assert is_terminal_artifact_url(url) is False
    assert is_terminal_artifact(url) is False


def test_undeclared_container_stays_out_under_narrowed_set() -> None:
    """Позитивная вселенная контейнеров сужает класс, но не расширяет его."""
    assert is_terminal_artifact_url("https://x.test/a.7z", ["zip"]) is False
    assert is_terminal_artifact_url("https://x.test/a.zip", []) is False
    assert is_terminal_artifact_url("https://x.test/a.zip", ["zip"]) is True
    # Документная часть от конфига не зависит: pdf терминален и при пустом наборе.
    assert is_terminal_artifact_url("https://x.test/a.pdf", []) is True


# ---------------------------------------------------------------------------
# По Content-Type (post-fetch, суффикс-less URL)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "content_type",
    [
        "application/pdf",
        "application/pdf; charset=binary",
        "application/x-pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/zip",
        "application/x-7z-compressed",
        "application/vnd.rar",
    ],
)
def test_terminal_artifact_content_type_admits_documents_and_containers(
    content_type: str,
) -> None:
    assert is_terminal_artifact_content_type(content_type) is True
    assert is_terminal_artifact(content_type=content_type) is True


@pytest.mark.parametrize(
    "content_type",
    [
        None,
        "",
        "text/html; charset=utf-8",
        "text/plain",
        # Общий бинарный тип не доказывает ни документ, ни контейнер.
        "application/octet-stream",
        # RTF: читателя нет.
        "application/rtf",
        "text/rtf",
    ],
)
def test_terminal_artifact_content_type_rejects_pages_and_opaque_binaries(
    content_type: str | None,
) -> None:
    assert is_terminal_artifact_content_type(content_type) is False
    assert is_terminal_artifact(content_type=content_type) is False


def test_suffixless_download_is_terminal_by_content_type_only() -> None:
    """``/download?id=7``: суффикса нет, класс даёт только ``Content-Type``."""
    url = "https://x.test/download?id=7"
    assert is_terminal_artifact(url) is False
    assert is_terminal_artifact(url, "application/pdf") is True
    assert is_terminal_artifact(url, "application/zip") is True
    assert is_terminal_artifact(url, "text/html") is False


# ---------------------------------------------------------------------------
# Документ vs контейнер: OOXML — документ, а не контейнер
# ---------------------------------------------------------------------------


def test_ooxml_is_a_document_not_a_container() -> None:
    """``.docx`` терминален как **документ**; по магии — не контейнер для спуска."""
    assert is_terminal_artifact_url("https://x.test/spec.docx") is True
    # Контейнерный класс (спуск) OOXML не ловит: маркер ``[Content_Types].xml`` в голове.
    assert classify_archive_bytes(_ZIP_LOCAL + b"\x14\x00\x00\x00" + _OOXML) is None
    # Настоящий zip — контейнер (и терминален как заявленный контейнер).
    assert classify_archive_bytes(_ZIP_LOCAL + bytes(64)) == "zip"
    assert is_terminal_artifact_url("https://x.test/bundle.zip") is True


def test_attachment_formats_do_not_leak_into_document_axis() -> None:
    """Контейнерный MIME при незаявленном наборе — не терминален (сужение работает)."""
    assert is_terminal_artifact(content_type="application/zip", attachment_formats=[]) is False
    assert (
        is_terminal_artifact(content_type="application/zip", attachment_formats=["7z"])
        is False
    )
    assert (
        is_terminal_artifact(content_type="application/zip", attachment_formats=["zip"])
        is True
    )


# ---------------------------------------------------------------------------
# Текстовые форматы (plain text/markdown) — первый класс
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("suffix", MARKDOWN_SUFFIXES)
def test_markdown_suffix_maps_to_markdown_family(suffix: str) -> None:
    url = f"https://x.test/docs/guide{suffix.upper()}?v=1#top"
    assert textual_suffix(url) == suffix
    assert textual_family(url) == "markdown"


@pytest.mark.parametrize("suffix", PLAIN_TEXT_SUFFIXES)
def test_plain_text_suffix_maps_to_text_family(suffix: str) -> None:
    url = f"https://x.test/docs/notes{suffix}"
    assert textual_suffix(url) == suffix
    assert textual_family(url) == "text"


def test_textual_registry_covers_every_variant() -> None:
    """Реестр — единый: ``TEXTUAL_SUFFIXES`` = markdown + plain text, без потерь."""
    assert set(TEXTUAL_SUFFIXES) == set(MARKDOWN_SUFFIXES) | set(PLAIN_TEXT_SUFFIXES)
    for suffix in TEXTUAL_SUFFIXES:
        assert textual_family(f"https://x.test/a{suffix}") in {"markdown", "text"}
    # Наблюдаемые в корпусе варианты (22.10): .txt, .log, .sql.
    for suffix in (".txt", ".text", ".log", ".sql", ".rst"):
        assert suffix in TEXTUAL_SUFFIXES


def test_percent_encoded_textual_name_is_decoded() -> None:
    assert textual_family("https://x.test/files/%D0%BE%D1%82%D1%87%D1%91%D1%82.txt") == "text"
    assert textual_suffix("https://x.test/files/%D0%BE%D1%82%D1%87%D1%91%D1%82.md") == ".md"


def test_plain_filename_without_scheme_is_supported() -> None:
    """Реестр принимает и имя файла (не только URL): post-fetch-путь без схемы."""
    assert textual_family("dba1_03_install_lab.txt") == "text"
    assert textual_family("README.markdown") == "markdown"


@pytest.mark.parametrize(
    "content_type",
    [
        "text/plain",
        "text/plain; charset=utf-8",
        "TEXT/PLAIN;CHARSET=KOI8-R",
    ],
)
def test_plain_content_type_maps_to_text_family(content_type: str) -> None:
    assert textual_content_type(content_type) == "text"
    assert textual_family(None, content_type) == "text"


@pytest.mark.parametrize(
    "content_type",
    ["text/markdown", "text/x-markdown", "application/x-markdown; charset=utf-8"],
)
def test_markdown_content_type_maps_to_markdown_family(content_type: str) -> None:
    assert textual_content_type(content_type) == "markdown"


@pytest.mark.parametrize(
    "content_type",
    [None, "", "text/html; charset=utf-8", "application/pdf", "application/epub+zip"],
)
def test_non_textual_content_types_are_not_textual(content_type: str | None) -> None:
    assert textual_content_type(content_type) is None


@pytest.mark.parametrize(
    "url",
    [
        "https://x.test/files/manual.pdf",
        "https://x.test/files/spec.docx",
        "https://x.test/docs/page.html",
        "https://x.test/download?id=7",
        "https://x.test/",
    ],
)
def test_non_textual_urls_have_no_textual_family(url: str) -> None:
    assert textual_suffix(url) is None
    assert textual_family(url) is None


def test_document_suffix_beats_textual_content_type() -> None:
    """``report.pdf`` как ``text/plain`` остаётся документом (иначе docling не увидит PDF)."""
    assert textual_family("https://x.test/report.pdf", "text/plain") is None
    # Суффикс-less download-эндпоинт — решает Content-Type.
    assert textual_family("https://x.test/download?id=7", "text/plain") == "text"


# ---------------------------------------------------------------------------
# EPUB — отдельный читатель, а не текстовый формат
# ---------------------------------------------------------------------------


def test_epub_is_not_textual_and_not_docling_document() -> None:
    url = "https://x.test/books/guide.epub"
    assert textual_suffix(url) is None
    assert textual_family(url) is None
    # Реестр docling — вход конвертера; EPUB разбирает свой читатель (stdlib).
    assert ".epub" not in DOCUMENT_SUFFIXES
    assert is_epub(url) is True
    assert is_epub(content_type="application/epub+zip; charset=binary") is True


def test_epub_by_suffix_or_content_type_only() -> None:
    assert is_epub("https://x.test/book") is False
    assert is_epub("https://x.test/book", "application/epub+zip") is True
    assert is_epub("https://x.test/book", "application/octet-stream") is False
    # Документный суффикс сильнее EPUB-MIME: класс не переворачивается заголовком.
    assert is_epub("https://x.test/report.pdf", "application/epub+zip") is False


def test_epub_does_not_become_terminal_artifact() -> None:
    """Допуск артефактов не расширен: EPUB идёт страничным путём (RAM-потолок)."""
    assert is_terminal_artifact_url("https://x.test/book.epub") is False
    assert is_terminal_artifact(content_type="application/epub+zip") is False
