"""Детерминированный генератор фикстур extraction-golden (O27, план 26 §13).

Зачем скрипт, а не бинарь «как есть». Фикстуры нужны настоящие: прежний PDF-кейс
(77 байт, ожидание подстроки ``Catalog``) проходил на любом мусоре, а не на тексте
документа. Байты собираются здесь по минимуму формата, без сети и без внешних
библиотек: PDF — только синтаксис (объекты + xref), DOCX — ZIP с тремя частями OCF.
Коммитится **результат**, а побайтовая воспроизводимость держится тестом
``test_golden_fixtures_match_generator`` — устаревший бинарь падает, а не живёт молча.

Как пересобрать::

    .venv/bin/python tests/pipeline/fixtures/make_extraction_golden.py

Куда пишет: ``tests/pipeline/fixtures/extraction_golden/``:

* ``document.pdf`` — 3 страницы с известным текстом (страницы 1 и 3 несут подстроки,
  которые проверяет golden; служебных слов PDF в тексте нет);
* ``scan.pdf`` — 2 страницы без текстового слоя (только графика) → ожидаемый отказ
  ``no_text_layer``;
* ``document.docx`` — минимальный валидный DOCX (OCF: Content_Types + rels + document.xml)
  с двумя абзацами.

Детерминизм: у PDF нет меток времени, у DOCX — ``ZIP_STORED`` (без zlib-версии) и
фиксированный ``date_time``; повторная сборка даёт те же байты на любой машине.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

#: Каталог, куда пишутся фикстуры (рядом со скриптом).
OUT_DIR = Path(__file__).resolve().parent / "extraction_golden"

#: Текст страниц ``document.pdf``: фразы уникальны и не являются служебными словами PDF.
DOCUMENT_PAGES: tuple[tuple[str, ...], ...] = (
    (
        "The extraction golden begins with a distinctive alpha sentence.",
        "Only this probe document contains the word zephyrium.",
    ),
    ("The middle page carries an unrelated paragraph about quokkas.",),
    (
        "The final page closes with the omega conclusion.",
        "Reproduce this file with make_extraction_golden.py.",
    ),
)

#: Абзацы ``document.docx``.
DOCX_PARAGRAPHS: tuple[str, ...] = (
    "Docx golden paragraph with a unique word kumquat.",
    "The second docx line mentions a narwhal.",
)


def _pdf(objects: list[bytes], *, root: int = 1) -> bytes:
    """Собирает PDF из готовых тел объектов: заголовок, тела, xref, trailer."""
    out = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets: list[int] = []
    for index, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{index} 0 obj\n".encode("ascii") + body + b"\nendobj\n"
    xref_offset = len(out)
    size = len(objects) + 1
    out += f"xref\n0 {size}\n".encode("ascii")
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode("ascii")
    out += (
        f"trailer\n<< /Size {size} /Root {root} 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n"
    ).encode("ascii")
    return bytes(out)


def _text_stream(lines: tuple[str, ...]) -> bytes:
    """Поток содержимого: строки текста шрифтом Helvetica (стандартный Type1)."""
    parts = ["BT", "/F1 14 Tf", "72 720 Td"]
    for position, line in enumerate(lines):
        if position:
            parts.append("0 -24 Td")
        parts.append(f"({line}) Tj")
    parts.append("ET")
    return ("\n".join(parts) + "\n").encode("ascii")


def build_document_pdf() -> bytes:
    """``document.pdf``: три страницы с текстовым слоем (Helvetica, без встраивания)."""
    page_count = len(DOCUMENT_PAGES)
    font_index = 3 + 2 * page_count
    kids = " ".join(f"{3 + 2 * index} 0 R" for index in range(page_count))
    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        f"<< /Type /Pages /Kids [{kids}] /Count {page_count} >>".encode("ascii"),
    ]
    for index, lines in enumerate(DOCUMENT_PAGES):
        page_index = 3 + 2 * index
        content_index = page_index + 1
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 {font_index} 0 R >> >> "
                f"/Contents {content_index} 0 R >>"
            ).encode("ascii")
        )
        stream = _text_stream(lines)
        objects.append(
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii")
            + stream
            + b"endstream"
        )
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    return _pdf(objects)


def build_scan_pdf() -> bytes:
    """``scan.pdf``: две страницы только с графикой — текстового слоя нет вовсе."""
    page_count = 2
    kids = " ".join(f"{3 + 2 * index} 0 R" for index in range(page_count))
    objects: list[bytes] = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        f"<< /Type /Pages /Kids [{kids}] /Count {page_count} >>".encode("ascii"),
    ]
    # Заливки прямоугольников: содержимое есть, текста (``Tj``/``TJ``) нет.
    stream = b"0.85 0.85 0.85 rg\n72 72 468 648 re f\n0 0 0 rg\n120 300 200 120 re f\n"
    for index in range(page_count):
        page_index = 3 + 2 * index
        content_index = page_index + 1
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Contents {content_index} 0 R >>"
            ).encode("ascii")
        )
        objects.append(
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii")
            + stream
            + b"endstream"
        )
    return _pdf(objects)


_CONTENT_TYPES = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>
"""

_ROOT_RELS = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>
"""


def _document_xml(paragraphs: tuple[str, ...]) -> str:
    """``word/document.xml``: тело из абзацев, каждый — один прогон текста."""
    body = "".join(
        f"<w:p><w:r><w:t>{paragraph}</w:t></w:r></w:p>" for paragraph in paragraphs
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{body}</w:body></w:document>\n"
    )


def build_document_docx() -> bytes:
    """``document.docx``: минимальный валидный OCF-пакет (stdlib ``zipfile``).

    ``ZIP_STORED`` и фиксированный ``date_time`` — ради побайтового детерминизма:
    deflate зависел бы от версии zlib и ломал сверку с закоммиченным файлом.
    """
    buffer = io.BytesIO()
    members = {
        "[Content_Types].xml": _CONTENT_TYPES,
        "_rels/.rels": _ROOT_RELS,
        "word/document.xml": _document_xml(DOCX_PARAGRAPHS),
    }
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        for name, text in members.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_STORED
            archive.writestr(info, text.encode("utf-8"))
    return buffer.getvalue()


def build_all(dest: Path = OUT_DIR) -> dict[str, bytes]:
    """Пишет все фикстуры в ``dest`` и возвращает имя → байты (для сверки в тесте)."""
    dest.mkdir(parents=True, exist_ok=True)
    built = {
        "document.pdf": build_document_pdf(),
        "scan.pdf": build_scan_pdf(),
        "document.docx": build_document_docx(),
    }
    for name, data in built.items():
        (dest / name).write_bytes(data)
    return built


def main() -> None:
    """Точка входа: пересобрать фикстуры в каталоге рядом со скриптом."""
    for name, data in build_all().items():
        print(f"{OUT_DIR / name}: {len(data)} байт")


if __name__ == "__main__":
    main()
