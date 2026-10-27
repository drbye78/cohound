"""Читатель EPUB → markdown — stdlib (``zipfile`` + ``xml.etree``), без новых зависимостей.

EPUB — это OCF-контейнер (ZIP) с OPF-пакетом и XHTML-главами. Читатель собирает текст в
**порядке чтения spine**, а не в порядке ``manifest``: ``manifest`` — это инвентарь
ресурсов (туда же входят стили/картинки/обложка), порядок задаёт ``spine``.

Почему stdlib, а не библиотека: EPUB-разбор — это чтение двух XML-файлов и ZIP-членов;
``zipfile``/``xml.etree`` уже в stdlib, а сторонние EPUB-парсеры добавляли бы зависимость
ради формата, который разбирается сотней строк. LlamaIndex/LangChain не используются и не
будут (класс ``d``, 05 §3.1).

Переиспользуется уже принятый субстрат:

* :class:`core.adapters.container_zip.ZipContainer` — индекс и потоковое чтение членов
  (член собирается в RAM под потолком ``archive_max_member_bytes``: XML/XHTML парсится
  целиком, но потолок не даёт ему стать неограниченным);
* :func:`core.util.container_policy.normalize_member_path` — путь члена не выходит за
  пространство имён архива (``..``/абсолют/диск/NUL отвергаются);
* :func:`core.util.container_policy.bounded_iter` — жёсткий байтовый потолок на чтении
  (``archive_max_member_bytes`` и суммарный ``archive_max_uncompressed_bytes``);
* :class:`core.adapters.extract_html.HtmlExtractor` — чистка XHTML главой (trafilatura):
  собственный HTML-ридер здесь **не** дублируется.

**Видимость отказов.** Нет ``container.xml``/OPF/списка spine → ``ExtractedDoc.error``
(не исключение). Глава, которой нет в архиве или которую не удалось прочитать, остаётся
на своём месте в порядке чтения как **видимая заметка** с заголовком — тихий пропуск
сделал бы «главу не прочитали» неотличимым от «главы нет в книге».

Нейтрален (§1.3).
"""

from __future__ import annotations

import posixpath
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import unquote

from core.adapters.container_zip import ZipContainer
from core.adapters.extract_html import HtmlExtractor
from core.domain.models import (
    DEFAULT_ARCHIVE_MAX_MEMBER_BYTES,
    DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES,
)
from core.ports.extractor import ExtractorPort
from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage
from core.util.container_policy import (
    MemberTooLarge,
    bounded_iter,
    normalize_member_path,
)
from core.util.document_formats import FAMILY_EPUB
from core.util.hashing import content_hash

__all__ = ["EPUB_READER_VERSION", "EpubExtractor"]

#: Стабильная версия читателя (провенанс/``extractor_version`` документа).
EPUB_READER_VERSION = "epub-1"

#: Путь контейнерного манифеста OCF — фиксирован стандартом (EPUB OCF §2.5.1).
CONTAINER_XML = "META-INF/container.xml"

#: Media-type OPF-пакета (rootfile ``media-type``).
_OPF_MEDIA_TYPE = "application/oebps-package+xml"

#: Уровень заголовка главы в собранном markdown: ``##`` оставляет место заголовку
#: документа и даёт непустой ``heading_path`` в markdown-чанкере.
_CHAPTER_HEADING = "## "


class _MemberUnreadable(RuntimeError):
    """Член архива не прочитан (нет в индексе/битый/потолок) — причина для заметки."""


class _Budget:
    """Суммарный бюджет распаковки на один документ (второй гейт bomb-guard).

    ``remaining()`` — сколько байт ещё можно прочитать суммарно (``None`` — потолка нет);
    ``consume()`` — учёт фактически прочитанного. Бюджет общий на все члены документа
    (container.xml + OPF + главы): суммарный потолок нельзя обойти числом глав.
    """

    def __init__(self, limit: int | None) -> None:
        self._limit = limit
        self._used = 0

    def remaining(self) -> int | None:
        if self._limit is None:
            return None
        return max(self._limit - self._used, 0)

    def consume(self, size: int) -> None:
        self._used += size


def _local(tag: str) -> str:
    """Локальное имя тега без namespace (``{ns}item`` → ``item``)."""
    return tag.rsplit("}", 1)[-1]


def _elements(root: ET.Element, name: str) -> list[ET.Element]:
    """Все элементы с локальным именем ``name`` (namespace-агностично)."""
    return [element for element in root.iter() if _local(element.tag) == name]


def _parse_xml(data: bytes) -> ET.Element | None:
    """**Единственная** точка разбора недоверенного XML модуля; битый вход → ``None``.

    Через неё идут **все** XML-пути читателя: ``META-INF/container.xml``
    (:func:`_opf_path`), OPF-пакет (:func:`_parse_opf`) и заголовок главы
    (:func:`_chapter_title`). Второго пути разбора нет намеренно: два набора правил —
    это и два набора гарантий, и обоснование безопасности, верное для одного из них,
    ложно для другого (``_parse_opf`` раньше звал ``ET.fromstring`` напрямую, и битый
    OPF выходил исключением из контракта «отказ — ``error``, не исключение»). Решение
    по ``None`` принимает вызывающий: у OPF это ``error``, у заголовка — fallback.

    **Почему stdlib, а не ``defusedxml``.** OCF/OPF-манифест — недоверенный вход, но
    ``defusedxml`` живёт только в необязательном extra ``safety`` (05 §2.7.1), а EPUB —
    формат первого класса: читатель обязан работать без этой группы. Риски при этом
    закрыты самим CPython: замерено на 3.14 (пин проекта) — ``ET.fromstring`` отвергает
    billion-laughs (``ParseError: limit on input amplification factor``) и не ресолвит
    внешние сущности (``ParseError: undefined entity``), потому что pyexpat включает
    защиту от амплификации и не тянет внешние DTD. XXE-вектора у stdlib нет. Замер
    закреплён тестами ``tests/adapters/test_extract_epub.py`` (hostile XML по обоим
    путям: контейнер и OPF).
    """
    try:
        return ET.fromstring(data)  # noqa: S314 — обоснование в докстринге (замер 3.14)
    except ET.ParseError:
        return None


def _opf_path(container_xml: bytes) -> str | None:
    """``full-path`` корневого OPF из ``META-INF/container.xml`` либо ``None``.

    Предпочитается rootfile с ``media-type=application/oebps-package+xml``; если такого
    нет — первый rootfile с непустым ``full-path`` (консервативно: лучше попытаться
    прочитать заявленный пакет, чем объявить книгу нечитаемой).
    """
    root = _parse_xml(container_xml)
    if root is None:
        return None
    rootfiles = _elements(root, "rootfile")
    preferred = [
        element
        for element in rootfiles
        if (element.get("media-type") or "").strip().lower() == _OPF_MEDIA_TYPE
    ]
    for element in [*preferred, *rootfiles]:
        path = (element.get("full-path") or "").strip()
        if path:
            return path
    return None


def _parse_opf(opf: bytes) -> tuple[dict[str, tuple[str, str]], list[str]] | None:
    """Разбирает OPF: ``(manifest: id → (href, media-type), spine: [idref])``.

    ``manifest`` — инвентарь (порядок не значим), ``spine`` — **порядок чтения**.
    Элементы без ``id``/``href`` и ``itemref`` без ``idref`` пропускаются: они не
    адресуют контент, и выдумывать им место в книге нельзя.

    ``None`` — OPF не разобран (не well-formed XML). Разбор идёт через
    :func:`_parse_xml`, то есть через **ту же** точку и те же гарантии, что у
    ``container.xml``: битый пакет — видимый ``error`` читателя, а не исключение наружу.
    """
    root = _parse_xml(opf)
    if root is None:
        return None
    manifest: dict[str, tuple[str, str]] = {}
    for item in _elements(root, "item"):
        identifier = (item.get("id") or "").strip()
        href = (item.get("href") or "").strip()
        if not identifier or not href:
            continue
        media_type = (item.get("media-type") or "").strip().lower()
        manifest[identifier] = (href, media_type)
    spine = [
        idref
        for element in _elements(root, "itemref")
        if (idref := (element.get("idref") or "").strip())
    ]
    return manifest, spine


def _resolve_href(base_dir: str, href: str) -> str | None:
    """Путь члена в пространстве имён архива либо ``None`` (путь недопустим).

    ``href`` percent-декодируется (имена в OPF часто закодированы), query/fragment
    отбрасываются, путь разрешается относительно каталога OPF. Выход за корень архива
    (``..`` сверх базы) отвергается **до** нормализации: ``posixpath.normpath`` схлопнул
    бы ``../..`` и превратил обход в безобидный путь. Финальная проверка —
    :func:`normalize_member_path` (NUL/абсолют/диск/обратные слэши).
    """
    raw = unquote(href.split("#", 1)[0].split("?", 1)[0]).strip()
    if not raw:
        return None
    parts: list[str] = [segment for segment in base_dir.split("/") if segment]
    for segment in raw.replace("\\", "/").split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if not parts:
                return None
            parts.pop()
            continue
        parts.append(segment)
    if not parts:
        return None
    return normalize_member_path("/".join(parts))


def _chapter_title(data: bytes, fallback: str) -> str:
    """``<title>`` главы, иначе ``fallback`` (имя файла).

    Разбор — best-effort: XHTML в книге может быть не well-formed XML, и это не повод
    терять главу — заголовком станет имя файла (видимый, хоть и менее удобный).
    """
    root = _parse_xml(data)
    if root is not None:
        for element in _elements(root, "title"):
            title = " ".join((element.text or "").split())
            if title:
                return title
    return fallback


class EpubExtractor:
    """EPUB → markdown. Реализация ``ExtractorPort`` (отдельный читатель, не docling).

    ``html`` — экстрактор XHTML-глав (по умолчанию trafilatura); подменяется в тестах.
    Потолки — оси спуска (``archive_max_member_bytes``/``archive_max_uncompressed_bytes``),
    дефолты берутся из домена: читатель не изобретает собственные лимиты.
    """

    version = EPUB_READER_VERSION

    def __init__(
        self,
        html: ExtractorPort | None = None,
        *,
        max_member_bytes: int | None = DEFAULT_ARCHIVE_MAX_MEMBER_BYTES,
        max_total_uncompressed: int | None = DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES,
    ) -> None:
        self._html = html if html is not None else HtmlExtractor()
        self._max_member_bytes = max_member_bytes
        self._max_total = max_total_uncompressed

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """``RawPage`` → ``ExtractedDoc``; никогда не бросает (сбой → ``error``)."""
        try:
            return self._extract(page, route)
        except Exception as exc:  # noqa: BLE001 — грациозная деградация адаптера
            return self._error(f"epub: {exc}")

    # -- internals ------------------------------------------------------ #

    def _extract(self, page: RawPage, route: ExtractionRoute) -> ExtractedDoc:
        """Открывает архив (retained-файл или байты через временный файл) и читает книгу."""
        if not getattr(self._html, "available", True):
            return self._error(
                "epub: HTML-экстрактор (trafilatura) недоступен — главы не извлечены"
            )
        staging: tempfile.TemporaryDirectory[str] | None = None
        try:
            if page.source_path:
                archive = page.source_path
            else:
                if not page.content:
                    return self._error("epub: нет ни байтов, ни retained-файла")
                staging = tempfile.TemporaryDirectory(prefix="cohound-epub-")
                archive = str(Path(staging.name) / "document.epub")
                Path(archive).write_bytes(page.content)
            return self._read(archive, page, route)
        finally:
            if staging is not None:
                staging.cleanup()

    def _read(self, archive: str, page: RawPage, route: ExtractionRoute) -> ExtractedDoc:
        """Читает OCF-контейнер: container.xml → OPF → spine → главы в порядке spine."""
        container = ZipContainer()
        budget = _Budget(self._max_total)

        try:
            container_bytes = self._member(container, archive, CONTAINER_XML, budget)
        except _MemberUnreadable as exc:
            return self._error(f"epub: {CONTAINER_XML} не прочитан: {exc}")

        opf_raw = _opf_path(container_bytes)
        if opf_raw is None:
            return self._error(
                f"epub: {CONTAINER_XML} без rootfile full-path — это не EPUB (OCF)"
            )
        opf_path = normalize_member_path(unquote(opf_raw))
        if opf_path is None:
            return self._error(f"epub: недопустимый путь OPF «{opf_raw}»")

        try:
            opf_bytes = self._member(container, archive, opf_path, budget)
        except _MemberUnreadable as exc:
            return self._error(f"epub: OPF «{opf_path}» не прочитан: {exc}")

        parsed = _parse_opf(opf_bytes)
        if parsed is None:
            # Один путь разбора (см. ``_parse_xml``): битый пакет — это отказ читателя,
            # а не ``ET.ParseError`` наружу через ``extract``.
            return self._error(f"epub: OPF «{opf_path}» не разобран — не well-formed XML")
        manifest, spine = parsed
        if not spine:
            return self._error("epub: spine пуст — порядок чтения не определён")

        base_dir = posixpath.dirname(opf_path)
        blocks: list[str] = []
        failures: list[str] = []
        read_any = False
        for idref in spine:
            item = manifest.get(idref)
            if item is None:
                # Глава объявлена в spine, но не описана в manifest — видимая заметка.
                failures.append(f"«{idref}» отсутствует в manifest")
                blocks.append(
                    f"{_CHAPTER_HEADING}{idref}\n\n"
                    f"> epub: глава «{idref}» отсутствует в manifest"
                )
                continue
            href, _media_type = item
            member = _resolve_href(base_dir, href)
            fallback = posixpath.basename(href) or idref
            if member is None:
                failures.append(f"путь главы «{href}» вне архива")
                blocks.append(
                    f"{_CHAPTER_HEADING}{fallback}\n\n"
                    f"> epub: путь главы «{href}» вне архива — отклонён"
                )
                continue
            try:
                data = self._member(container, archive, member, budget)
            except _MemberUnreadable as exc:
                failures.append(f"«{member}» не прочитана: {exc}")
                blocks.append(
                    f"{_CHAPTER_HEADING}{fallback}\n\n"
                    f"> epub: глава «{member}» не прочитана: {exc}"
                )
                continue
            chapter = self._html.extract(
                RawPage(
                    url=f"epub:{member}",
                    content=data,
                    fetched_at=page.fetched_at,
                    content_type="application/xhtml+xml",
                ),
                route,
            )
            text = (chapter.text or "").strip()
            if chapter.error or not text:
                reason = chapter.error or "пустой текст главы"
                failures.append(f"«{member}» не извлечена: {reason}")
                blocks.append(
                    f"{_CHAPTER_HEADING}{_chapter_title(data, fallback)}\n\n"
                    f"> epub: глава «{member}» не извлечена: {reason}"
                )
                continue
            read_any = True
            blocks.append(f"{_CHAPTER_HEADING}{_chapter_title(data, fallback)}\n\n{text}")

        if not read_any:
            # Причины первых отказов — в тексте ошибки: иначе «ни одна глава» не отличима
            # от «книга пуста» без второго прогона.
            detail = "; ".join(failures[:3]) or "spine не дал содержимого"
            return self._error(
                f"epub: ни одна глава не прочитана ({len(blocks)} записей spine): {detail}"
            )
        text = "\n\n".join(blocks).strip()
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
            page_count=None,
            family=FAMILY_EPUB,
        )

    def _member(
        self, container: ZipContainer, archive: str, member: str, budget: _Budget
    ) -> bytes:
        """Читает член под двумя потолками (на член и суммарный); отказ — исключение."""
        limit = self._max_member_bytes
        remaining = budget.remaining()
        if remaining is not None:
            limit = remaining if limit is None else min(limit, remaining)
        stream = container.open_member(archive, member)
        if limit is not None:
            stream = bounded_iter(stream, limit)
        try:
            data = b"".join(stream)
        except MemberTooLarge as exc:
            raise _MemberUnreadable(str(exc)) from exc
        except Exception as exc:  # нет члена/битый архив/шифрование
            raise _MemberUnreadable(f"{type(exc).__name__}: {exc}") from exc
        budget.consume(len(data))
        return data

    def _error(self, message: str) -> ExtractedDoc:
        return ExtractedDoc(
            text="",
            content_hash=content_hash(""),
            extractor_version=self.version,
            page_count=None,
            error=message,
            family=FAMILY_EPUB,
        )
