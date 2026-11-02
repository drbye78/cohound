"""Тесты EPUB-читателя ``EpubExtractor`` (``epub-2``) — stdlib, без сети и трафилатуры.

Строится **настоящий** EPUB (``zipfile``): ``mimetype``, ``META-INF/container.xml``,
OPF с перемешанным ``manifest`` и упорядоченным ``spine``, XHTML-главы. Проверяются
ровно те свойства, из-за которых читатель существует:

* порядок чтения — ``spine``, а не ``manifest`` (``manifest`` — инвентарь ресурсов);
* ``href`` разрешается относительно каталога OPF, percent-декодируется, выход за архив
  отвергается (``normalize_member_path``);
* у каждой главы есть заголовок (``<title>`` либо имя файла) — иначе ``heading_path``
  markdown-чанкера был бы пустым;
* заголовок главы — уровень ``#``, внутренние ATX-заголовки главы — на +1, поэтому
  ``heading_path`` читается как «глава → раздел → подраздел»; сдвиг не трогает код в
  fenced-блоках, строки таблиц и тематические разрывы, а ``######`` не переполняется;
* отсутствующая/нечитаемая глава — **видимая заметка**, а не тихий пропуск;
* нет ``container.xml``/OPF/spine или архив не ZIP — видимая ошибка, не исключение.

HTML-экстрактор подменён внедрённой функцией (trafilatura не требуется): главы несут
маркер ``BODY[…]``, по которому видно, что именно прочитано.
"""

from __future__ import annotations

import re
import zipfile
from pathlib import Path

from core.adapters.chunker_markdown import MarkdownChunker
from core.adapters.extract_epub import EPUB_READER_VERSION, EpubExtractor
from core.adapters.extract_html import HtmlExtractor
from core.ports.types import ExtractedDoc, RawPage
from core.util.hashing import content_hash

_XHTML = """<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>{title}</head>
  <body><p>BODY[{body}]</p></body>
</html>
"""

_CONTAINER_XML = """<?xml version="1.0" encoding="utf-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="{opf}" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
"""

_OPF = """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:identifier id="id">urn:uuid:test</dc:identifier>
    <dc:title>Book</dc:title>
  </metadata>
  <manifest>
{manifest}
  </manifest>
  <spine>
{spine}
  </spine>
</package>
"""


def _chapter(body: str, title: str | None = None) -> str:
    head = f"<title>{title}</title>" if title is not None else ""
    return _XHTML.format(title=head, body=body)


def _build_epub(
    path: Path,
    *,
    items: list[tuple[str, str, str]],
    spine: list[str],
    chapters: dict[str, str],
    opf_dir: str = "OEBPS",
    opf_name: str = "content.opf",
    include_container: bool = True,
    include_opf: bool = True,
) -> Path:
    """Собирает настоящий EPUB: ``items`` — ``(id, href, media-type)``, ``chapters`` —
    ``href (от каталога OPF) → XHTML``."""
    manifest = "\n".join(
        f'    <item id="{identifier}" href="{href}" media-type="{media}"/>'
        for identifier, href, media in items
    )
    spine_xml = "\n".join(f'    <itemref idref="{idref}"/>' for idref in spine)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            zipfile.ZipInfo("mimetype"), "application/epub+zip", zipfile.ZIP_STORED
        )
        if include_container:
            archive.writestr(
                "META-INF/container.xml",
                _CONTAINER_XML.format(opf=f"{opf_dir}/{opf_name}"),
            )
        if include_opf:
            archive.writestr(
                f"{opf_dir}/{opf_name}",
                _OPF.format(manifest=manifest, spine=spine_xml),
            )
        for href, content in chapters.items():
            archive.writestr(f"{opf_dir}/{href}", content)
    return path


def _fake_html(*, fail_on: str | None = None) -> HtmlExtractor:
    """HTML-экстрактор с внедрённой функцией: текст главы — содержимое ``BODY[…]``."""

    def fn(html: str) -> str | None:
        if fail_on is not None and fail_on in html:
            return None
        match = re.search(r"BODY\[(.*?)\]", html, re.DOTALL)
        return match.group(0) if match else None

    return HtmlExtractor(extract_fn=fn)


def _markdown_html(markdown: str) -> HtmlExtractor:
    """HTML-экстрактор, отдающий заданный markdown: проверяется пост-обработка глав."""

    return HtmlExtractor(extract_fn=lambda _html: markdown)


def _markdown_book(tmp_path: Path, *, title: str = "Глава 1") -> Path:
    """EPUB из одной главы; её markdown задаёт внедрённый ``_markdown_html``."""
    return _build_epub(
        tmp_path / "book.epub",
        items=[("c1", "text/1.xhtml", "application/xhtml+xml")],
        spine=["c1"],
        chapters={"text/1.xhtml": _chapter("one", title)},
    )


def _page(content: bytes, *, source_path: str | None = None) -> RawPage:
    return RawPage(
        url="https://repo.example.test/books/guide.epub",
        content=content,
        fetched_at="2026-10-22T00:00:00Z",
        content_type="application/epub+zip",
        source_path=source_path,
    )


def _three_chapters(path: Path, *, spine: list[str]) -> Path:
    items = [
        ("c1", "text/1.xhtml", "application/xhtml+xml"),
        ("c2", "text/2.xhtml", "application/xhtml+xml"),
        ("c3", "text/3.xhtml", "application/xhtml+xml"),
    ]
    chapters = {
        "text/1.xhtml": _chapter("one", "Chapter One"),
        "text/2.xhtml": _chapter("two", "Chapter Two"),
        "text/3.xhtml": _chapter("three", "Chapter Three"),
    }
    return _build_epub(path, items=items, spine=spine, chapters=chapters)


# ---------------------------------------------------------------------------
# Порядок чтения и заголовки
# ---------------------------------------------------------------------------


def test_spine_order_wins_over_manifest_order(tmp_path: Path) -> None:
    """``manifest`` — инвентарь; порядок книги задаёт ``spine``."""
    epub = _three_chapters(tmp_path / "book.epub", spine=["c3", "c1", "c2"])

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert doc.text.index("BODY[three]") < doc.text.index("BODY[one]")
    assert doc.text.index("BODY[one]") < doc.text.index("BODY[two]")


def test_every_spine_item_gets_a_heading(tmp_path: Path) -> None:
    epub = _three_chapters(tmp_path / "book.epub", spine=["c1", "c2", "c3"])

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    for title in ("# Chapter One", "# Chapter Two", "# Chapter Three"):
        assert title in doc.text


def test_heading_falls_back_to_filename_without_title(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[("c1", "text/1.xhtml", "application/xhtml+xml")],
        spine=["c1"],
        chapters={"text/1.xhtml": _chapter("body")},
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert "# 1.xhtml" in doc.text
    assert "BODY[body]" in doc.text


def test_result_contract(tmp_path: Path) -> None:
    epub = _three_chapters(tmp_path / "book.epub", spine=["c1"])

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert isinstance(doc, ExtractedDoc)
    assert doc.extractor_version == EPUB_READER_VERSION == "epub-2"
    assert doc.page_count is None
    assert doc.family == "epub"
    assert doc.content_hash == content_hash(doc.text)


def test_retained_file_is_read_instead_of_bytes(tmp_path: Path) -> None:
    """I19: тело может лежать retained-файлом, ``content`` пуст."""
    epub = _three_chapters(tmp_path / "book.epub", spine=["c2"])

    doc = EpubExtractor(html=_fake_html()).extract(
        _page(b"", source_path=str(epub))
    )

    assert doc.error is None
    assert "BODY[two]" in doc.text


# ---------------------------------------------------------------------------
# Иерархия заголовков главы (``epub-2``): глава → раздел → подраздел
# ---------------------------------------------------------------------------


def test_inner_headings_nest_under_the_chapter_heading(tmp_path: Path) -> None:
    """Регресс 02.11: ``##`` главы + ``#`` раздела сбрасывали стек — глава терялась.

    Проверка идёт через **настоящий** ``MarkdownChunker``: утверждение — про
    ``heading_path``, а не про вид строки, поэтому ``doc.text`` для него не годится.
    """
    markdown = (
        "# Настоящий заголовок главы\n\n"
        "Текст раздела книги, достаточно длинный для чанкера.\n\n"
        "## Подраздел\n\n"
        "Текст подраздела книги, тоже достаточно длинный для чанкера.\n"
    )
    epub = _markdown_book(tmp_path)

    doc = EpubExtractor(html=_markdown_html(markdown)).extract(_page(epub.read_bytes()))

    assert doc.error is None
    chunks = MarkdownChunker(chunk_size=500).chunk("doc", doc.text)
    paths = [chunk.heading_path for chunk in chunks]
    assert "Глава 1 > Настоящий заголовок главы" in paths
    assert "Глава 1 > Настоящий заголовок главы > Подраздел" in paths


def test_hash_comment_inside_fences_is_not_shifted(tmp_path: Path) -> None:
    """Код в ```` ``` ````/``~~~`` — не структура: ``# comment`` остаётся как есть."""
    markdown = (
        "# Установка\n\n"
        "```bash\n"
        "# comment\n"
        "apt-get install -y pkg\n"
        "```\n\n"
        "~~~text\n"
        "# тоже комментарий\n"
        "~~~\n"
    )
    epub = _markdown_book(tmp_path)

    doc = EpubExtractor(html=_markdown_html(markdown)).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "## Установка" in doc.text
    assert "```bash\n# comment" in doc.text
    assert "~~~text\n# тоже комментарий" in doc.text
    assert "## comment" not in doc.text
    assert "## тоже комментарий" not in doc.text


def test_deepest_heading_does_not_overflow_the_level(tmp_path: Path) -> None:
    """``######`` остаётся ``######``: седьмого уровня в markdown нет."""
    markdown = "###### Глубочайший раздел\n\nТекст подраздела книги для чанкера.\n"
    epub = _markdown_book(tmp_path)

    doc = EpubExtractor(html=_markdown_html(markdown)).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "###### Глубочайший раздел" in doc.text
    assert "#######" not in doc.text


def test_table_rows_and_thematic_breaks_are_not_headings(tmp_path: Path) -> None:
    """Таблица (в том числе без ведущего ``|``) и разрыв — не заголовки, их не сдвигают."""
    markdown = (
        "# Раздел\n\n"
        "колонка | вторая\n"
        "--- | ---\n"
        "# значение | ячейка\n\n"
        "---\n\n"
        "| # | x |\n"
        "| --- | --- |\n"
    )
    epub = _markdown_book(tmp_path)

    doc = EpubExtractor(html=_markdown_html(markdown)).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "## Раздел" in doc.text
    assert "# значение | ячейка" in doc.text
    assert "## значение | ячейка" not in doc.text
    assert "| # | x |" in doc.text
    assert "\n---\n" in doc.text


def test_visible_note_keeps_chapter_heading_level(tmp_path: Path) -> None:
    """Заметка ``> epub: …`` под главой: заголовок главы ``#``, сама заметка не тронута."""
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[
            ("c1", "text/1.xhtml", "application/xhtml+xml"),
            ("c2", "text/2.xhtml", "application/xhtml+xml"),
        ],
        spine=["c1", "c2"],
        chapters={
            "text/1.xhtml": _chapter("one", "One"),
            "text/2.xhtml": _chapter("broken", "Two"),
        },
    )

    doc = EpubExtractor(html=_fake_html(fail_on="BODY[broken]")).extract(
        _page(epub.read_bytes())
    )

    assert doc.error is None
    assert "BODY[one]" in doc.text
    assert "# Two\n\n> epub: глава «OEBPS/text/2.xhtml» не извлечена" in doc.text
    assert "## Two" not in doc.text


# ---------------------------------------------------------------------------
# href: каталог OPF, percent-кодирование, выход за архив
# ---------------------------------------------------------------------------


def test_href_is_resolved_relative_to_opf_directory(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[("c1", "text/deep/ch.xhtml", "application/xhtml+xml")],
        spine=["c1"],
        chapters={"text/deep/ch.xhtml": _chapter("deep", "Deep")},
        opf_dir="OPS/package",
        opf_name="book.opf",
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "BODY[deep]" in doc.text


def test_percent_encoded_href_is_decoded(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[("c1", "text/%D0%B3%D0%BB%D0%B0%D0%B2%D0%B0.xhtml", "application/xhtml+xml")],
        spine=["c1"],
        chapters={"text/глава.xhtml": _chapter("ru", "Глава")},
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "BODY[ru]" in doc.text


def test_traversal_href_is_rejected_visibly(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[("c1", "../../etc/passwd.xhtml", "application/xhtml+xml")],
        spine=["c1"],
        chapters={},
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    # Ни одна глава не прочитана → видимая ошибка (а не пустой успех и не падение).
    assert doc.text == ""
    assert doc.error is not None
    assert doc.error.startswith("epub:")
    assert "вне архива" in doc.error


def test_traversal_href_note_when_other_chapters_are_readable(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[
            ("c1", "text/1.xhtml", "application/xhtml+xml"),
            ("c2", "../../etc/passwd.xhtml", "application/xhtml+xml"),
        ],
        spine=["c1", "c2"],
        chapters={"text/1.xhtml": _chapter("one", "One")},
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "BODY[one]" in doc.text
    assert "вне архива" in doc.text


# ---------------------------------------------------------------------------
# Видимые отказы: главы и контейнер
# ---------------------------------------------------------------------------


def test_missing_spine_id_becomes_a_visible_note(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[("c1", "text/1.xhtml", "application/xhtml+xml")],
        spine=["c1", "ghost"],
        chapters={"text/1.xhtml": _chapter("one", "One")},
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "BODY[one]" in doc.text
    assert "отсутствует в manifest" in doc.text


def test_missing_chapter_member_becomes_a_visible_note(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[
            ("c1", "text/1.xhtml", "application/xhtml+xml"),
            ("c2", "text/absent.xhtml", "application/xhtml+xml"),
        ],
        spine=["c1", "c2"],
        chapters={"text/1.xhtml": _chapter("one", "One")},
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is None
    assert "не прочитана" in doc.text


def test_unreadable_chapter_is_noted(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[
            ("c1", "text/1.xhtml", "application/xhtml+xml"),
            ("c2", "text/2.xhtml", "application/xhtml+xml"),
        ],
        spine=["c1", "c2"],
        chapters={
            "text/1.xhtml": _chapter("one", "One"),
            "text/2.xhtml": _chapter("broken", "Two"),
        },
    )

    doc = EpubExtractor(html=_fake_html(fail_on="BODY[broken]")).extract(
        _page(epub.read_bytes())
    )

    assert doc.error is None
    assert "BODY[one]" in doc.text
    assert "не извлечена" in doc.text


def test_member_ceiling_is_a_visible_note(tmp_path: Path) -> None:
    """Потолок члена — видимый отказ главы, документ не теряется целиком."""
    big = _chapter("x" * 2000, "Big")
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[
            ("c1", "text/1.xhtml", "application/xhtml+xml"),
            ("c2", "text/2.xhtml", "application/xhtml+xml"),
        ],
        spine=["c1", "c2"],
        chapters={"text/1.xhtml": _chapter("one", "One"), "text/2.xhtml": big},
    )

    doc = EpubExtractor(html=_fake_html(), max_member_bytes=600).extract(
        _page(epub.read_bytes())
    )

    assert doc.error is None
    assert "BODY[one]" in doc.text
    assert "не прочитана" in doc.text
    assert "member_too_large" in doc.text


def test_without_container_xml_returns_visible_error(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[],
        spine=[],
        chapters={},
        include_container=False,
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.text == ""
    assert doc.error is not None
    assert "container.xml" in doc.error


def test_without_opf_returns_visible_error(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[],
        spine=[],
        chapters={},
        include_opf=False,
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is not None
    assert "OPF" in doc.error


def test_empty_spine_returns_visible_error(tmp_path: Path) -> None:
    epub = _build_epub(
        tmp_path / "book.epub",
        items=[("c1", "text/1.xhtml", "application/xhtml+xml")],
        spine=[],
        chapters={"text/1.xhtml": _chapter("one", "One")},
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.error is not None
    assert "spine" in doc.error


def test_non_zip_payload_does_not_crash(tmp_path: Path) -> None:
    doc = EpubExtractor(html=_fake_html()).extract(_page(b"%PDF-1.7 not an epub"))

    assert doc.text == ""
    assert doc.error is not None
    assert doc.error.startswith("epub:")


def test_no_bytes_and_no_source_path_is_a_visible_error() -> None:
    doc = EpubExtractor(html=_fake_html()).extract(_page(b""))

    assert doc.error is not None
    assert "нет ни байтов" in doc.error


# ---------------------------------------------------------------------------
# Недоверенный XML (F5): stdlib-парсер обязан отказать, а не разорвать процесс
# ---------------------------------------------------------------------------

#: ``billion laughs``: экспоненциальное раскрытие сущностей (DoS через память). Шесть
#: уровней — **замеренный** порог CPython 3.14: пять дают 300 KB и разбираются, шесть
#: отвергаются ``ParseError: limit on input amplification factor`` (pyexpat). Меньше
#: брать нельзя (бомба «пройдёт»), больше — уже не нужно.
def _billion_laughs(tag: str, inner: str) -> str:
    """XML-бомба в обёртке ``<tag>`` (``container`` — OCF, ``package`` — OPF)."""
    entities = ['<!ENTITY lol "lol">']
    previous = "lol"
    for level in range(1, 7):
        body = "".join(f"&{previous};" for _ in range(10))
        entities.append(f'<!ENTITY lol{level} "{body}">')
        previous = f"lol{level}"
    dtd = "\n ".join(entities)
    return (
        f'<?xml version="1.0"?>\n<!DOCTYPE {tag} [\n {dtd}\n]>\n'
        f"<{tag}>{inner}<x>&{previous};</x></{tag}>\n"
    )


#: XXE: внешняя сущность (файловая система/SSRF) — не должна ресолвиться.
def _external_entity(tag: str, inner: str) -> str:
    """XML с внешней сущностью ``&xxe;`` (``file://``) в обёртке ``<tag>``."""
    return (
        f'<?xml version="1.0"?>\n'
        f'<!DOCTYPE {tag} [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>\n'
        f"<{tag}>{inner}<x>&xxe;</x></{tag}>\n"
    )

_ROOTFILE = (
    '<rootfile full-path="OEBPS/content.opf" '
    'media-type="application/oebps-package+xml"/>'
)


def _epub_with_raw_container(path: Path, container_xml: str) -> Path:
    """EPUB с **подложенным** ``container.xml`` (валидный OPF на месте)."""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            zipfile.ZipInfo("mimetype"), "application/epub+zip", zipfile.ZIP_STORED
        )
        archive.writestr("META-INF/container.xml", container_xml)
        archive.writestr(
            "OEBPS/content.opf",
            _OPF.format(manifest="", spine='<itemref idref="c1"/>'),
        )
    return path


def _epub_with_raw_opf(path: Path, opf_xml: str) -> Path:
    """EPUB с **подложенным** OPF (валидный ``container.xml`` на месте)."""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            zipfile.ZipInfo("mimetype"), "application/epub+zip", zipfile.ZIP_STORED
        )
        archive.writestr(
            "META-INF/container.xml", _CONTAINER_XML.format(opf="OEBPS/content.opf")
        )
        archive.writestr("OEBPS/content.opf", opf_xml)
    return path


def test_billion_laughs_in_container_xml_is_a_visible_error(tmp_path: Path) -> None:
    """Экспоненциальные сущности: ``ParseError`` внутри, наружу — ``error`` документа."""
    epub = _epub_with_raw_container(
        tmp_path / "bomb.epub",
        _billion_laughs("container", f"<rootfiles>{_ROOTFILE}</rootfiles>"),
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.text == ""
    assert doc.error is not None
    assert doc.error.startswith("epub:")
    assert "container.xml" in doc.error


def test_external_entity_in_container_xml_is_not_resolved(tmp_path: Path) -> None:
    """XXE не ресолвится: документ с ошибкой, содержимое файла не попадает в текст."""
    epub = _epub_with_raw_container(
        tmp_path / "xxe.epub",
        _external_entity("container", f"<rootfiles>{_ROOTFILE}</rootfiles>"),
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.text == ""
    assert doc.error is not None
    assert "root:" not in doc.text  # /etc/passwd не прочитан
    assert "container.xml" in doc.error


def test_billion_laughs_in_opf_is_a_visible_error(tmp_path: Path) -> None:
    """OPF с бомбой сущностей: ``epub: OPF не разобран`` — видимая ошибка, не падение."""
    epub = _epub_with_raw_opf(
        tmp_path / "opf-bomb.epub", _billion_laughs("package", "")
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.text == ""
    assert doc.error is not None
    assert "OPF" in doc.error


def test_external_entity_in_opf_is_a_visible_error(tmp_path: Path) -> None:
    """XXE в OPF: отказ парсера виден как ошибка OPF, текст пуст."""
    epub = _epub_with_raw_opf(
        tmp_path / "opf-xxe.epub", _external_entity("package", "")
    )

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.text == ""
    assert doc.error is not None
    assert "OPF" in doc.error


def test_malformed_opf_is_a_visible_error_not_an_exception(tmp_path: Path) -> None:
    """F5: битый OPF — видимый ``error`` читателя, а не исключение через ``extract``.

    До приведения к одной точке разбора ``_parse_opf`` звал ``ET.fromstring`` напрямую,
    и наружу выходил ``ParseError``. Тест пинит **причину** отказа («не разобран»), а не
    просто «что-то не так»: сообщение обязано приходить из ветки OPF, а не из
    грациозного ``except`` вокруг всего ``extract``.
    """
    epub = _epub_with_raw_opf(tmp_path / "broken-opf.epub", "<package><metadata>")

    doc = EpubExtractor(html=_fake_html()).extract(_page(epub.read_bytes()))

    assert doc.text == ""
    assert doc.error is not None
    assert "OPF" in doc.error
    assert "не разобран" in doc.error


def test_opf_shares_the_single_xml_parse_entry_point() -> None:
    """F5: у OPF тот же путь разбора, что у ``container.xml``: ``None``, а не исключение.

    Это утверждение о **структуре**: второй путь разбора означал бы второй набор правил
    безопасности, и обоснование про pyexpat было бы верно только для одного из них.
    """
    from core.adapters.extract_epub import _parse_opf, _parse_xml

    assert _parse_xml(b"<package><metadata>") is None
    assert _parse_opf(b"<package><metadata>") is None
    # Обе функции разбирают один и тот же well-formed документ одинаково.
    assert _parse_opf(b"<package><manifest/><spine/></package>") is not None
