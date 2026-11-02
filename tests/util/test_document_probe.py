"""Зонд артефакта: класс по байтам файла и число страниц PDF (B3, F4/F8).

Проверяется ровно то, что было дефектом:

* **F8.** OOXML узнаётся по **центральному каталогу** zip, а не по первым 8 KiB: маркер
  ``[Content_Types].xml`` на смещениях 78/4078/8258/9078/20078 даёт класс ``document``.
  Прежняя форма видела только голову, поэтому маркер за 8 KiB давал класс ``container`` —
  и спуск принимал у ``.docx`` ноль членов с пустым ``errors`` (документ терялся молча);
* **F4.** ``pdf_page_count`` читает ограниченные окна (шапка + хвост), а не файл целиком:
  объём чтения не зависит от размера файла, а неполный пересчёт объектов не выдаётся за
  число страниц (честный ``None`` вместо выдуманного числа).

Данные нейтральны (§1.3).
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pytest

from core.util.document_formats import OOXML_MARKER_NAME, classify_archive_bytes
from core.util.document_probe import (
    CLASS_BINARY,
    CLASS_CONTAINER,
    CLASS_DOCUMENT,
    CLASS_PAGE,
    classify_artifact,
    ooxml_content_types,
    pdf_page_count,
)

MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# F8: OOXML по центральному каталогу
# ---------------------------------------------------------------------------


def _ooxml_zip(marker_offset: int) -> bytes:
    """Настоящий zip, в котором имя записи ``[Content_Types].xml`` лежит на смещении.

    Смещение задаётся размером **предыдущего** члена (``ZIP_STORED``): каждый байт его
    тела сдвигает следующий локальный заголовок ровно на один байт.
    """

    def build(pad: int) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("pad.bin", b"\x00" * pad)
            archive.writestr(OOXML_MARKER_NAME, b"<Types/>")
            archive.writestr("word/document.xml", b"<w:document/>")
        return buffer.getvalue()

    base = build(0)
    delta = marker_offset - base.index(OOXML_MARKER_NAME.encode("ascii"))
    assert delta >= 0, "нужен положительный отступ: маркер и так лежит не позже"
    data = build(delta)
    assert data.index(OOXML_MARKER_NAME.encode("ascii")) == marker_offset
    return data


@pytest.mark.parametrize("offset", [78, 4078, 8258, 9078, 20078])
def test_ooxml_marker_at_any_offset_is_a_document(
    tmp_path: Path, offset: int
) -> None:
    """Маркер OOXML найден по центральному каталогу → ``document`` (маршрут экстрактора)."""
    payload = _ooxml_zip(offset)
    path = tmp_path / f"artifact-{offset}.bin"
    path.write_bytes(payload)

    probe = classify_artifact(path, url="https://x.test/download?id=7")

    assert probe.kind == CLASS_DOCUMENT
    assert probe.format == "ooxml"
    assert probe.is_container is False
    # Голова (8 KiB) маркер за 8 KiB не видит — именно поэтому класс решает каталог.
    if offset > 8192:
        assert classify_archive_bytes(payload[:8192]) == "zip"


def test_ooxml_content_types_by_central_directory(tmp_path: Path) -> None:
    """Прямая проверка предиката: ``True`` — OOXML, ``False`` — обычный zip, ``None`` — не zip."""
    ooxml = tmp_path / "book.bin"
    ooxml.write_bytes(_ooxml_zip(8258))
    assert ooxml_content_types(ooxml) is True

    plain = tmp_path / "bundle.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr("img/logo.png", b"\x89PNG\r\n\x1a\n" + bytes(16))
    plain.write_bytes(buffer.getvalue())
    assert ooxml_content_types(plain) is False

    broken = tmp_path / "broken.bin"
    broken.write_bytes(b"PK\x03\x04 not a zip at all")
    assert ooxml_content_types(broken) is None


def test_plain_zip_stays_a_container(tmp_path: Path) -> None:
    """Обычный архив (нет записи ``[Content_Types].xml``) остаётся контейнером спуска."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr("docs/manual.pdf", b"%PDF-1.7\n" + bytes(64))
        archive.writestr("notes/readme.md", b"# hello\n")
    path = tmp_path / "bundle.zip"
    path.write_bytes(buffer.getvalue())

    probe = classify_artifact(path, url="https://x.test/bundle.zip")

    assert probe.kind == CLASS_CONTAINER
    assert probe.format == "zip"


@pytest.mark.parametrize(
    "name",
    ["guide.mdx", "report.qmd", "notes.mdc", "analysis.rmd", "manual.rst", "schema.sql", "run.log"],
)
def test_textual_registry_suffix_is_a_page_not_binary(tmp_path: Path, name: str) -> None:
    """Суффикс из реестра чанкеров ⇒ ``page``: наблюдение не лжёт про ``binary``.

    Список текстовых суффиксов здесь больше не свой: он целиком берётся из
    :data:`core.util.document_formats.TEXTUAL_SUFFIXES`. До сведения ``.mdx``/``.qmd``
    попадали в ``binary`` — маршрут оставался верным (там решает ``textual_family``), а
    провенанс артефакта расходился с тем, что делает экстрактор.
    """
    path = tmp_path / name
    path.write_bytes(b"# Title\n\nBody\n")

    probe = classify_artifact(path)

    assert probe.kind == CLASS_PAGE
    assert probe.format == name.rsplit(".", 1)[-1]


def test_asciidoc_stays_binary_by_decision(tmp_path: Path) -> None:
    """AsciiDoc вне реестра ⇒ ``binary`` (видимый отказ), а не мнимая страница."""
    path = tmp_path / "guide.adoc"
    path.write_bytes(b"= Title\n\nBody\n")

    probe = classify_artifact(path)

    assert probe.kind == CLASS_BINARY


# ---------------------------------------------------------------------------
# F4: стоимость и честность счётчика страниц
# ---------------------------------------------------------------------------


def _pdf(pages: int, *, body: bytes = b"") -> bytes:
    """Минимальный PDF: дерево страниц с ``/Count`` в шапке и ``body`` после него."""
    tree = b"1 0 obj\n<< /Type /Pages /Kids [] /Count " + str(pages).encode() + b" >>\nendobj\n"
    return b"%PDF-1.7\n" + tree + body


def test_pdf_page_count_reads_small_file_whole(tmp_path: Path) -> None:
    """Файл меньше окон читается целиком: счётчик точен (декларация и объекты совпали)."""
    path = tmp_path / "small.pdf"
    path.write_bytes(_pdf(3) + b"x" * 1024)

    assert pdf_page_count(path) == 3


class _CountingReader:
    """Обёртка файла, считающая прочитанные байты (стоимость пробы — измеримая)."""

    def __init__(self, handle: Any, counter: dict[str, int]) -> None:
        self._handle = handle
        self._counter = counter

    def read(self, size: int = -1) -> bytes:
        data = self._handle.read(size)
        self._counter["bytes"] += len(data)
        return data

    def seek(self, *args: Any) -> int:
        return int(self._handle.seek(*args))

    def __enter__(self) -> _CountingReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self._handle.close()


def test_pdf_page_count_reads_bounded_windows_not_whole_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """32 MiB PDF: прочитаны только окна (≤ 2 MiB + запас), а не весь файл (F4).

    Прежняя форма читала файл целиком ради ``/Type /Pages``: замер — 1.3/9.1/29.3/176.8 мс
    на 1/8/32/128 MiB (≈1.4 мс/MiB, линейно). Теперь 2.0–3.4 мс независимо от размера.
    """
    size = 32 * MIB
    path = tmp_path / "big.pdf"
    path.write_bytes(_pdf(7, body=b"x" * size))

    counter = {"bytes": 0}
    original_open = Path.open

    def counting_open(self: Path, *args: Any, **kwargs: Any) -> _CountingReader:
        return _CountingReader(original_open(self, *args, **kwargs), counter)

    monkeypatch.setattr(Path, "open", counting_open)

    assert pdf_page_count(path) == 7  # заявленное /Count видно в шапке
    # Окна: 1 MiB шапки + 1 MiB хвоста (+ 1 KiB проба магии). Не 32 MiB.
    assert counter["bytes"] < 2 * MIB + MIB // 64


def test_pdf_page_count_does_not_invent_number_from_partial_scan(tmp_path: Path) -> None:
    """Страницы только в середине файла (за окнами) → ``None``, а не частичное число."""
    filler = b"y" * (4 * MIB)
    pages = b"".join(
        b"%d 0 obj\n<< /Type /Page >>\nendobj\n" % (index + 10) for index in range(500)
    )
    path = tmp_path / "middle.pdf"
    path.write_bytes(b"%PDF-1.7\n" + filler + pages + filler)

    # Дерева страниц (``/Count``) нет вовсе, объекты лежат в середине — вне окон.
    assert pdf_page_count(path) is None


def test_pdf_page_count_non_pdf_is_none(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    path.write_bytes(b"not a pdf at all")

    assert pdf_page_count(path) is None
