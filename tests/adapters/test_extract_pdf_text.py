"""Тесты PDF-механизма ``text_layer`` (``crawl.pdf_pipeline=text_layer``).

Проверяется то, от чего зависит пригодность механизма для корпуса:

* **семейство** ``text`` — вход выбора чанкера (иначе markdown-чанкер молча дал бы ноль
  заголовков, тихая деградация нарезки);
* **нормализация текста** — pdfium отдаёт ``\\r\\n`` (замер: 31 056 в 40 PDF корпуса) и
  служебные управляющие байты (2 на 16 073 страницы); без нормализации абзацная граница
  чанкера ``\\n[ \\t]*\\n`` не совпадает, и документ режется по размеру;
* **пустой текстовый слой** — видимый ``no_text_layer`` (183 из 16 073 страниц, 1.14%), а
  не молчаливый пустой документ;
* **не-PDF** — видимая ошибка, а не попытка разобрать docx как PDF;
* **путь и байты** дают один текст и одну версию (ключ ``extract_cache``);
* **ленивый импорт**: ``available`` не тянет pdfium-бинарь на уровне модуля.

Часть тестов работает на настоящем pdfium (крошечные PDF собираются здесь же), часть —
на подменённом модуле, когда нужен ровно заданный текст: воспроизвести поведение pdfium на
конкретном файле синтетикой нельзя, а проверяемое правило (нормализация, отказ) — наше.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from core.adapters.extract_pdf_text import (
    PDFIUM_ADAPTER_VERSION,
    PdfiumDocumentExtractor,
    strip_control_characters,
)
from core.util.document_formats import FAMILY_TEXT

_HAS_PDFIUM = __import__("importlib").util.find_spec("pypdfium2") is not None

pytestmark = pytest.mark.skipif(not _HAS_PDFIUM, reason="pypdfium2 не установлен")


def _make_pdf(pages: list[str]) -> bytes:
    """Минимальный PDF с текстом на каждой странице (без шрифтов-вложений)."""
    objects: list[str] = []
    kids: list[int] = []
    body: list[tuple[int, str]] = []
    number = 4
    for text in pages:
        page_number = number
        content_number = number + 1
        kids.append(page_number)
        stream = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET"
        body.append(
            (
                page_number,
                "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                "/Resources << /Font << /F1 3 0 R >> >> "
                f"/Contents {content_number} 0 R >>",
            )
        )
        body.append(
            (
                content_number,
                f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream",
            )
        )
        number += 2
    objects.append("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
    refs = " ".join(f"{kid} 0 R" for kid in kids)
    objects.append(
        f"2 0 obj\n<< /Type /Pages /Kids [{refs}] /Count {len(kids)} >>\nendobj\n"
    )
    objects.append(
        "3 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"
    )
    for obj_number, payload in body:
        objects.append(f"{obj_number} 0 obj\n{payload}\nendobj\n")
    header = "%PDF-1.4\n"
    data = header + "".join(objects)
    offsets: list[int] = []
    position = len(header)
    for chunk in objects:
        offsets.append(position)
        position += len(chunk)
    xref = f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n"
    for offset in offsets:
        xref += f"{offset:010d} 00000 n \n"
    trailer = (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{position}\n%%EOF\n"
    )
    return (data + xref + trailer).encode("latin-1")


def _write(tmp_path: Path, name: str, pages: list[str]) -> Path:
    target = tmp_path / name
    target.write_bytes(_make_pdf(pages))
    return target


# ---------------------------------------------------------------------------
# Контракт адаптера
# ---------------------------------------------------------------------------


def test_version_and_support_are_declared() -> None:
    adapter = PdfiumDocumentExtractor()
    assert adapter.version == PDFIUM_ADAPTER_VERSION == "pdfium-text-1"
    # Работает по пути, как docling: retained-файл читается адаптером (I19).
    assert adapter.supports_source_path is True
    assert adapter.supports("report.pdf") is True
    assert adapter.supports("report.PDF") is True
    assert adapter.supports("report.docx") is False
    assert adapter.supports("report") is False


def test_extracted_family_is_text(tmp_path: Path) -> None:
    """``family`` обязателен: иначе чанкер выберет markdown-правило (ноль заголовков)."""
    path = _write(tmp_path, "one.pdf", ["Hello world"])

    doc = PdfiumDocumentExtractor().extract_path(path, filename_hint="one.pdf")

    assert doc.error is None
    assert doc.text == "Hello world"
    assert doc.page_count == 1
    assert doc.family == FAMILY_TEXT
    assert doc.extractor_version == PDFIUM_ADAPTER_VERSION


def test_page_count_is_document_pages(tmp_path: Path) -> None:
    path = _write(tmp_path, "three.pdf", ["a", "b", "c"])

    doc = PdfiumDocumentExtractor().extract_path(path, filename_hint="three.pdf")

    assert doc.error is None
    assert doc.page_count == 3


def test_path_and_bytes_agree(tmp_path: Path) -> None:
    """Оба входа дают один текст и одну версию — иначе ключ ``extract_cache`` разойдётся."""
    path = _write(tmp_path, "same.pdf", ["first paragraph", "second paragraph"])
    payload = path.read_bytes()

    by_path = PdfiumDocumentExtractor().extract_path(path, filename_hint="same.pdf")
    by_bytes = PdfiumDocumentExtractor().extract_bytes(payload, "same.pdf")

    assert by_path.error is None and by_bytes.error is None
    assert by_path.text == by_bytes.text
    assert by_path.content_hash == by_bytes.content_hash
    assert by_path.extractor_version == by_bytes.extractor_version
    assert by_path.page_count == by_bytes.page_count


# ---------------------------------------------------------------------------
# Нормализация текста (замер: 31 056 × CRLF в 40 PDF; 2 управляющих символа / 16 073 стр.)
# ---------------------------------------------------------------------------


def test_strip_control_characters_joins_split_word() -> None:
    """``Post\\x02greSQL`` → ``PostgreSQL``: символ не пробел, а мусор, слово должно остаться целым."""
    assert strip_control_characters("Post\x02greSQL") == "PostgreSQL"


def test_strip_control_characters_keeps_text_structure() -> None:
    """Табуляция/переводы строк/разрыв страницы — структура, а не мусор."""
    assert strip_control_characters("a\tb\nc\fd") == "a\tb\nc\fd"
    assert strip_control_characters("\x00\x07\x1f") == ""


def test_crlf_becomes_paragraph_boundary() -> None:
    """``\\r\\n\\r\\n`` → ``\\n\\n``: иначе абзацная граница чанкера не совпадает.

    Замер корпуса: 31 056 ``\\r\\n`` в 40 PDF. Чанкер ``TextChunker`` делит абзацы по
    ``\\n[ \\t]*\\n``; ``\\r`` в этот класс не входит, поэтому без нормализации документ
    уходит одним блоком и режется по размеру (заголовки/абзацы теряются).
    """
    assert strip_control_characters("Первый\r\n\r\nВторой") == "Первый\n\nВторой"
    assert strip_control_characters("a\rb") == "a\nb"


def test_crlf_is_normalized_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Сквозная проверка на подменённом pdfium: в документ не просачивается ни ``\\r``, ни мусор."""
    monkeypatch.setitem(
        sys.modules, "pypdfium2", _FakePdfium(["Первый\r\n\r\nВторой\rПод\x02пункт"])
    )
    path = _write(tmp_path, "crlf.pdf", ["ignored"])

    doc = PdfiumDocumentExtractor().extract_path(path, filename_hint="crlf.pdf")

    assert doc.error is None
    assert "\r" not in doc.text
    assert "\x02" not in doc.text
    assert doc.text == "Первый\n\nВторой\nПодпункт"


class _FakeTextPage:
    def __init__(self, text: str) -> None:
        self._text = text

    def get_text_range(self) -> str:
        return self._text

    def close(self) -> None:
        return None


class _FakePage:
    def __init__(self, text: str) -> None:
        self._text = text

    def get_textpage(self) -> _FakeTextPage:
        return _FakeTextPage(self._text)

    def close(self) -> None:
        return None


class _FakePdf:
    def __init__(self, pages: list[str]) -> None:
        self._pages = pages

    def __len__(self) -> int:
        return len(self._pages)

    def __getitem__(self, index: int) -> _FakePage:
        return _FakePage(self._pages[index])

    def close(self) -> None:
        return None


class _FakePdfium:
    PdfiumError = RuntimeError

    def __init__(self, pages: list[str]) -> None:
        self._pages = pages

    def PdfDocument(self, path: str) -> _FakePdf:  # noqa: N802 — имя API pdfium
        return _FakePdf(self._pages)


def test_returned_text_has_no_control_characters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Сквозная проверка: мусор pdfium не доходит ни до индекса, ни до цитаты evidence."""
    monkeypatch.setitem(sys.modules, "pypdfium2", _FakePdfium(["Post\x02greSQL 17"]))
    path = _write(tmp_path, "kern.pdf", ["ignored"])

    doc = PdfiumDocumentExtractor().extract_path(path, filename_hint="kern.pdf")

    assert doc.error is None
    assert doc.text == "PostgreSQL 17"
    assert "\x02" not in doc.text


# ---------------------------------------------------------------------------
# Видимые отказы
# ---------------------------------------------------------------------------


def test_empty_text_layer_is_explicit_error(tmp_path: Path) -> None:
    """Страница без текстового слоя (1.14% страниц корпуса) — ``no_text_layer``, не пустой."""
    path = _write(tmp_path, "scan.pdf", [""])

    doc = PdfiumDocumentExtractor().extract_path(path, filename_hint="scan.pdf")

    assert doc.text == ""
    assert doc.error == "no_text_layer"
    # Хеш пустой строки, как у соседних адаптеров: поле не остаётся пустым.
    assert doc.content_hash == hashlib.sha256(b"").hexdigest()
    assert doc.page_count == 1
    assert doc.family == FAMILY_TEXT


def test_non_pdf_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "table.docx"
    path.write_bytes(b"PK\x03\x04")

    doc = PdfiumDocumentExtractor().extract_path(path, filename_hint="table.docx")

    assert doc.text == ""
    assert doc.error is not None and "text_layer" in doc.error
    assert doc.extractor_version == PDFIUM_ADAPTER_VERSION

    by_bytes = PdfiumDocumentExtractor().extract_bytes(b"PK\x03\x04", "table.docx")
    assert by_bytes.error == doc.error


def test_missing_file_is_explicit_error(tmp_path: Path) -> None:
    doc = PdfiumDocumentExtractor().extract_path(
        tmp_path / "gone.pdf", filename_hint="gone.pdf"
    )

    assert doc.error is not None and "не найден" in doc.error
    assert doc.content_hash == hashlib.sha256(b"").hexdigest()


def test_broken_pdf_degrades_to_error(tmp_path: Path) -> None:
    """Исключение pdfium — ``ExtractedDoc.error`` (как docling), а не raise фазы."""
    path = tmp_path / "broken.pdf"
    path.write_bytes(b"%PDF-1.4 not really a pdf")

    doc = PdfiumDocumentExtractor().extract_path(path, filename_hint="broken.pdf")

    assert doc.error is not None and doc.error.startswith("pdfium:")
    assert doc.text == ""


# ---------------------------------------------------------------------------
# Ленивый импорт
# ---------------------------------------------------------------------------

_LAZY_IMPORT_SCRIPT = """
import importlib.util
import json
import sys

import core.adapters.extract_pdf_text as m

print(json.dumps({
    "pdfium_loaded": "pypdfium2" in sys.modules,
    "available": m.PdfiumDocumentExtractor().available,
    "has_pdfium": importlib.util.find_spec("pypdfium2") is not None,
}))
"""


def test_available_does_not_import_pypdfium2() -> None:
    """``available`` спрашивается на старте воркера — импорт бинаря там недопустим."""
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env.setdefault("PYTHONPATH", str(root))
    proc = subprocess.run(
        [sys.executable, "-c", _LAZY_IMPORT_SCRIPT],
        cwd=str(root),
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr
    seen = json.loads(proc.stdout.strip().splitlines()[-1])

    assert seen["pdfium_loaded"] is False, "импорт адаптера подтянул pypdfium2"
    assert seen["available"] is seen["has_pdfium"]
