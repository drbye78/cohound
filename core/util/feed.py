"""coHound — чистый парсер фидов/каналов (Phase 5.0).

Разбирает **сырые байты** публичных лент в плоский список записей :class:`FeedEntry`.
Поддержаны три семейства XML-фидов и «канальные» документы (JSON/JSON-lines/список URL).
Модуль **чистый**: без I/O, без сети, без портов — вызывающий сам решает, что делать с
URL (SSRF-перепроверка и постановка в корпус — на границе corpus enqueue, I19).

**Форматы и детекция.** Формат определяется по **корневому элементу и пространству имён**,
а не по ``content_type`` (тот лишь принимается как подсказка и может быть ``None``):

* RSS 2.0 — ``<rss><channel><item>``: ``link``/``guid``/``pubDate``/``title``;
* Atom 1.0 — ``<feed><entry>``: ``<link href=…>`` (приоритет ``rel="alternate"``),
  ``id``, ``published``/``updated``, ``title`` (в т.ч. ``type="html"``/``"xhtml"``);
* RDF/RSS 1.0 — ``<rdf:RDF>`` c ``<item rdf:about=…>``: ``link``/``title``/``dc:date``.

Сопоставление тегов — по **локальному имени** (без учёта префикса), поэтому Atom/RSS/RDF и
произвольные префиксы обрабатываются единообразно. Относительные ссылки резолвятся против
``base_url`` (URL самой ленты), если он передан.

**Безопасность.** Разбор идёт через ``defusedxml`` (XXE / external-entity / billion-laughs
отвергаются; DOCTYPE и объявления сущностей запрещены). Любой сбой/небезопасный ввод →
:class:`FeedParseError`. URL записей возвращаются **как есть** (после strip control/whitespace);
пустые URL пропускаются. Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urljoin

import defusedxml.ElementTree as _DET
from defusedxml.common import DefusedXmlException

from core.util.sanitize import strip_control_chars

__all__ = [
    "FeedEntry",
    "FeedParseError",
    "entry_not_before",
    "parse_feed",
    "parse_channel_document",
    "parse_instant",
]

#: Пространства имён, значимые для детекции (``dc:date`` ловится по локальному имени).
_ATOM_NS = "http://www.w3.org/2005/Atom"
_RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

#: Объявление кодировки в XML-прологе (для декодирования байтов до передачи в парсер).
_XML_DECL_ENCODING_RE = re.compile(
    rb"""<\?xml[^>]*?encoding\s*=\s*["']([^"']+)["']""", re.IGNORECASE
)

#: BOM → кодек (порядок важен: utf-32 до utf-16 — их BOM — префиксы друг друга).
_BOMS: tuple[tuple[bytes, str], ...] = (
    (b"\xef\xbb\xbf", "utf-8-sig"),
    (b"\xff\xfe\x00\x00", "utf-32-le"),
    (b"\x00\x00\xfe\xff", "utf-32-be"),
    (b"\xff\xfe", "utf-16-le"),
    (b"\xfe\xff", "utf-16-be"),
)

#: XML-парсер: жёстко запрещаем DOCTYPE/сущности/external — фиды не нуждаются в них.
_FORBID_DTD = True


class FeedParseError(ValueError):
    """Небезопасный/некорректный вход фида либо нераспознанный формат."""


@dataclass(frozen=True)
class FeedEntry:
    """Запись фида/канала: URL обязателен, остальное — best-effort метаданные.

    ``url`` — целевая ссылка записи (относительная резолвится против ``base_url``,
    если он передан); ``guid`` — стабильный идентификатор (RSS ``guid`` / Atom ``id`` /
    RDF ``rdf:about``); ``published_at`` — дата публикации в исходной строковой форме
    (RFC822/pubDate, ISO-8601 Atom, ``dc:date``) без нормализации; ``title`` — заголовок.
    """

    url: str
    guid: str | None = None
    published_at: str | None = None
    title: str | None = None


# ---------------------------------------------------------------------------
# Метки времени публикации (общая семантика ``since`` для local/edge)
# ---------------------------------------------------------------------------


def parse_instant(value: str) -> datetime | None:
    """Разбирает метку времени (ISO-8601 либо RFC822) в aware ``datetime`` (UTC).

    Общая семантика фильтра ``since`` для локального и edge-поллеров (N1): обе стороны
    обязаны трактовать RFC822/ISO (в т.ч. ``Z``/``+00:00``) единообразно, а не строковым
    сравнением. ``None`` — неразборчивое значение (вызывающий сам решает: ``validate_params``
    отвергает, а фильтр ``since`` — нет, консервативно сохраняя запись).
    """
    text = value.strip()
    if not text:
        return None
    iso = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    instant: datetime | None
    try:
        instant = datetime.fromisoformat(iso)
    except ValueError:
        try:
            instant = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
    if instant is None:
        return None
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return instant


def entry_not_before(entry: FeedEntry, threshold: datetime) -> bool:
    """Оставить ли запись при пороге ``since``: неизвестное время — сохраняем.

    Записи без ``published_at`` или с неразборчивой датой **сохраняются** (нельзя
    доказать, что они старее порога); отбрасываются лишь доказуемо старые. Сравнение —
    по разобранным инстантам (:func:`parse_instant`), а не лексикографически (N1).
    """
    if not entry.published_at:
        return True
    instant = parse_instant(entry.published_at)
    if instant is None:
        return True
    return instant >= threshold


# ---------------------------------------------------------------------------
# Декодирование
# ---------------------------------------------------------------------------


def _decode_xml(content: bytes | str) -> str:
    """Декодирует XML-байты в ``str`` (BOM → пролог → UTF-8 → latin-1).

    Кодировка берётся из BOM, затем из объявления ``<?xml … encoding=…?>``; иначе
    пробуется UTF-8, и как последний фолбэк — latin-1 (фиды бывают не-UTF8 без пролога).
    """
    if isinstance(content, str):
        return content
    data = bytes(content)
    for bom, encoding in _BOMS:
        if data.startswith(bom):
            return data.decode(encoding)
    match = _XML_DECL_ENCODING_RE.match(data[:256])
    if match is not None:
        declared = match.group(1).decode("ascii", "ignore")
        try:
            return data.decode(declared)
        except (LookupError, UnicodeDecodeError):
            pass  # неверное/неподдерживаемое имя кодировки — идём по фолбэкам
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin-1")


def _decode_text(content: bytes | str) -> str:
    """Декодирует произвольный (не-XML) контент: BOM → UTF-8 → latin-1."""
    if isinstance(content, str):
        return content
    data = bytes(content)
    for bom, encoding in _BOMS:
        if data.startswith(bom):
            return data.decode(encoding)
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin-1")


# ---------------------------------------------------------------------------
# Namespace-агностичные хелперы
# ---------------------------------------------------------------------------


def _localname(tag: object) -> str:
    """Локальное имя тега без префикса пространства имён (``{ns}item`` → ``item``)."""
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1]


def _namespace(tag: object) -> str:
    """URI пространства имён тега (``{ns}tag`` → ``ns``); ``""`` если его нет."""
    if isinstance(tag, str) and tag.startswith("{"):
        return tag[1:].split("}", 1)[0]
    return ""


def _children(elem: Any, name: str) -> list[Any]:
    """Прямые дочерние элементы с данным локальным именем (в порядке документа)."""
    return [child for child in list(elem) if _localname(getattr(child, "tag", None)) == name]


def _first(elem: Any, *names: str) -> Any | None:
    """Первый прямой дочерний элемент с любым из локальных имён ``names``."""
    wanted = set(names)
    for child in list(elem):
        if _localname(getattr(child, "tag", None)) in wanted:
            return child
    return None


def _text(elem: Any | None) -> str:
    """Конкатенация текст-узлов элемента (CDATA/вложенная разметка) без control/пробелов."""
    if elem is None:
        return ""
    return strip_control_chars("".join(elem.itertext())).strip()


def _attr_local(elem: Any, name: str) -> str | None:
    """Значение атрибута по локальному имени (``rdf:about`` → ``about``)."""
    attrib = getattr(elem, "attrib", None)
    if not isinstance(attrib, dict):
        return None
    for key, value in attrib.items():
        if _localname(key) == name:
            return str(value) if value is not None else None
    return None


def _clean(value: object) -> str:
    """Убирает управляющие символы и обрамляющие пробелы из строкового значения."""
    if value is None:
        return ""
    return strip_control_chars(str(value)).strip()


def _resolve(url: str, base_url: str | None) -> str:
    """Резолвит относительный URL против ``base_url``; без базы — возвращает как есть."""
    if base_url:
        return urljoin(base_url, url)
    return url


def _entry(
    url: object,
    *,
    base_url: str | None,
    guid: object = None,
    published_at: object = None,
    title: object = None,
) -> FeedEntry | None:
    """Собирает :class:`FeedEntry`; ``None``, если URL пуст после очистки (пропуск)."""
    cleaned_url = _clean(url)
    if not cleaned_url:
        return None
    cleaned_guid = _clean(guid) or None
    cleaned_published = _clean(published_at) or None
    cleaned_title = _clean(title) or None
    return FeedEntry(
        url=_resolve(cleaned_url, base_url),
        guid=cleaned_guid,
        published_at=cleaned_published,
        title=cleaned_title,
    )


# ---------------------------------------------------------------------------
# XML-фиды
# ---------------------------------------------------------------------------


def parse_feed(
    content: bytes,
    *,
    content_type: str | None = None,
    base_url: str | None = None,
) -> list[FeedEntry]:
    """Разбирает XML-фид (RSS 2.0 / Atom 1.0 / RDF-RSS 1.0) в список :class:`FeedEntry`.

    :param content: сырые байты фида. Кодировка выводится из BOM/пролога (см.
        :func:`_decode_xml`).
    :param content_type: MIME-подсказка; формат определяется **корневым элементом**, поэтому
        параметр не используется для детекции (принимается для единообразия вызова).
    :param base_url: URL самой ленты — база для резолва относительных ссылок записей.
    :raises FeedParseError: небезопасный (XXE/DTD/сущности) либо некорректный XML;
        нераспознанный корневой элемент.
    """
    del content_type  # детекция — по корню/пространству имён, не по MIME
    if not isinstance(content, (bytes, bytearray, str)):
        raise FeedParseError(f"ожидались байты фида, получено {type(content).__name__}")
    text = _decode_xml(content)
    if not text.strip():
        raise FeedParseError("пустой фид: нет XML-содержимого")

    try:
        root = _DET.fromstring(text, forbid_dtd=_FORBID_DTD)
    except DefusedXmlException as exc:
        raise FeedParseError(f"небезопасный XML фида: {exc}") from exc
    except _DET.ParseError as exc:
        raise FeedParseError(f"некорректный XML фида: {exc}") from exc
    except ValueError as exc:  # защитно: парсер может бросить ValueError
        raise FeedParseError(f"некорректный XML фида: {exc}") from exc

    kind = _localname(root.tag)
    namespace = _namespace(root.tag)
    if kind == "rss":
        return _parse_rss(root, base_url)
    if kind == "feed" or namespace == _ATOM_NS:
        return _parse_atom(root, base_url)
    if kind == "RDF" or namespace == _RDF_NS:
        return _parse_rdf(root, base_url)
    raise FeedParseError(f"нераспознанный корневой элемент фида: {kind!r}")


def _decode_xml_base_is_blank(content: object) -> bool:
    """Хелпер-заглушка совместимости (не влияет на разбор)."""
    return False


def _parse_rss(root: object, base_url: str | None) -> list[FeedEntry]:
    """RSS 2.0: ``channel/item`` → ``link``/``guid``/``pubDate``/``title``."""
    channel = _first(root, "channel")
    container = channel if channel is not None else root
    entries: list[FeedEntry] = []
    for item in _children(container, "item"):
        entry = _entry(
            _rss_link(item),
            base_url=base_url,
            guid=_text(_first(item, "guid")),
            published_at=_text(_first(item, "pubDate", "published", "date")),
            title=_text(_first(item, "title")),
        )
        if entry is not None:
            entries.append(entry)
    return entries


def _rss_link(item: object) -> str:
    """Ссылка RSS-элемента: текст ``<link>``, иначе ``href`` (atom:link-фолбэк)."""
    for child in _children(item, "link"):
        text = _text(child)
        if text:
            return text
        href = _attr_local(child, "href")
        if href:
            return href
    return ""


def _parse_atom(root: object, base_url: str | None) -> list[FeedEntry]:
    """Atom 1.0: ``feed/entry`` → ``link``/``id``/``published``/``updated``/``title``."""
    entries: list[FeedEntry] = []
    for entry_elem in _children(root, "entry"):
        entry = _entry(
            _atom_link(entry_elem),
            base_url=base_url,
            guid=_text(_first(entry_elem, "id")),
            published_at=_text(_first(entry_elem, "published", "updated")),
            title=_text(_first(entry_elem, "title")),
        )
        if entry is not None:
            entries.append(entry)
    return entries


def _atom_link(entry_elem: object) -> str:
    """Ссылка Atom-записи: ``rel="alternate"`` (или без ``rel``), иначе первая ``href``."""
    links = _children(entry_elem, "link")
    preferred = [link for link in links if (_attr_local(link, "rel") in (None, "alternate"))]
    candidate = preferred[0] if preferred else (links[0] if links else None)
    if candidate is None:
        return ""
    return _attr_local(candidate, "href") or _text(candidate)


def _parse_rdf(root: object, base_url: str | None) -> list[FeedEntry]:
    """RDF/RSS 1.0: ``rdf:RDF/rdf:item`` → ``link``/``title``/``dc:date``/``rdf:about``."""
    entries: list[FeedEntry] = []
    for item in _children(root, "item"):
        about = _attr_local(item, "about")
        entry = _entry(
            _text(_first(item, "link")) or about,
            base_url=base_url,
            guid=about,
            published_at=_text(_first(item, "date", "pubDate", "published")),
            title=_text(_first(item, "title")),
        )
        if entry is not None:
            entries.append(entry)
    return entries


# ---------------------------------------------------------------------------
# Канальные документы (JSON / JSON-lines / список URL)
# ---------------------------------------------------------------------------


def parse_channel_document(
    content: bytes,
    *,
    content_type: str | None = None,
    base_url: str | None = None,
) -> list[FeedEntry]:
    """Разбирает «канальный» документ в список :class:`FeedEntry`.

    Поддержаны (минимальная generic-форма, без messenger-специфичных API):

    * JSON-массив ``[{url|link, title?, id?}, …]`` (или голые строки-URL);
    * JSON-lines — по объекту/строке на строку;
    * plain-text — список URL по одному на строку.

    :param base_url: база для резолва относительных ссылок (обычно URL документа).
    :raises FeedParseError: ``content`` не bytes/str.
    """
    del content_type  # форма определяется содержимым, не MIME
    if not isinstance(content, (bytes, bytearray, str)):
        raise FeedParseError(f"ожидались байты канала, получено {type(content).__name__}")
    text = _decode_text(content)

    array_entries = _try_json_document(text, base_url)
    if array_entries is not None:
        return array_entries
    return _parse_line_document(text, base_url)


def _try_json_document(text: str, base_url: str | None) -> list[FeedEntry] | None:
    """Пробует разобрать весь документ как JSON (массив/объект); ``None`` — не JSON."""
    stripped = text.lstrip()
    if not stripped:
        return None
    try:
        data = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return None
    if isinstance(data, list):
        return _entries_from_json_items(data, base_url)
    if isinstance(data, dict):
        for key in ("items", "entries", "urls"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                return _entries_from_json_items(candidate, base_url)
        return _entries_from_json_items([data], base_url)
    return None


def _entries_from_json_items(items: list[object], base_url: str | None) -> list[FeedEntry]:
    """Строит записи из JSON-элементов (строки-URL либо объекты ``{url|link, …}``)."""
    entries: list[FeedEntry] = []
    for item in items:
        if isinstance(item, str):
            entry = _entry(item, base_url=base_url)
        elif isinstance(item, dict):
            url = item.get("url") or item.get("link")
            entry = _entry(
                url,
                base_url=base_url,
                guid=item.get("id") or item.get("guid"),
                published_at=item.get("published_at") or item.get("published"),
                title=item.get("title"),
            )
        else:
            entry = None
        if entry is not None:
            entries.append(entry)
    return entries


def _parse_line_document(text: str, base_url: str | None) -> list[FeedEntry]:
    """JSON-lines (объект/строка на строку) либо plain-text список URL."""
    lines = [line for line in text.splitlines() if line.strip()]
    json_items: list[object] = []
    all_json = bool(lines)
    for line in lines:
        try:
            json_items.append(json.loads(line))
        except (json.JSONDecodeError, ValueError):
            all_json = False
            break
    if all_json:
        return _entries_from_json_items(json_items, base_url)

    entries: list[FeedEntry] = []
    for line in lines:
        entry = _entry(line, base_url=base_url)
        if entry is not None:
            entries.append(entry)
    return entries
