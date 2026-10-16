"""coHound — чистый парсер ``sitemap.xml`` (D3).

Разбирает **сырые байты** публичного sitemap в плоский список ссылок-кандидатов
:class:`~core.util.feed.FeedEntry` (``url`` = ``<loc>``, ``published_at`` = ``<lastmod>``).
Поддержаны два корневых элемента:

* ``<urlset><url><loc>…</loc></url></urlset>`` — страницы (:data:`ROUTE_URLSET`);
* ``<sitemapindex><sitemap><loc>…</loc></sitemap></sitemapindex>`` — индекс дочерних
  sitemap (:data:`ROUTE_INDEX`); разворачивается вызывающим **ровно на один уровень**
  (:func:`expand_sitemap`).

``parse_feed`` для sitemap **непригоден** (он разбирает только RSS/Atom/RDF и иначе
бросает), поэтому здесь отдельная ветка. Тяжёлый XML разбирается через ``defusedxml``
(XXE/DTD/сущности запрещены — как в :mod:`core.util.feed`); переиспользуются только
**чистые** хелперы декодирования/обхода из ``feed`` (не ``parse_feed``).

**gzip.** Тело ``.xml.gz`` распознаётся по magic-байтам (``\\x1f\\x8b``) и распаковывается
с жёстким потолком (:data:`MAX_DECOMPRESSED_BYTES`) — защита от decompression-бомб.

**Лимиты (D3).** :data:`MAX_CHILD_SITEMAPS` — потолок числа дочерних sitemap одного
``<sitemapindex>``; :data:`MAX_SITEMAP_ENTRIES` — общий потолок числа ``<loc>``, собранных
из urlset(s). Вложенный ``<sitemapindex>`` (глубже одного уровня) отвергается явной
ошибкой :class:`SitemapParseError`.

Модуль **чистый**: без I/O, сети и портов. ``<loc>`` возвращаются как есть; SSRF-проверка
(:func:`core.util.security.validate_url`) и постановка в корпус — на стороне вызывающего
(I19: ссылка — кандидат, не evidence). Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import zlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import defusedxml.ElementTree as _DET
from defusedxml.common import DefusedXmlException

from core.util.feed import (
    FeedEntry,
    _children,
    _clean,
    _decode_xml,
    _first,
    _localname,
    _resolve,
    _text,
)

__all__ = [
    "MAX_CHILD_SITEMAPS",
    "MAX_DECOMPRESSED_BYTES",
    "MAX_SITEMAP_ENTRIES",
    "ROUTE_INDEX",
    "ROUTE_URLSET",
    "SitemapDocument",
    "SitemapParseError",
    "decompress_if_gzip",
    "expand_sitemap",
    "parse_sitemap",
]

#: Корневой элемент ``<urlset>`` — список страниц.
ROUTE_URLSET = "urlset"
#: Корневой элемент ``<sitemapindex>`` — список дочерних sitemap (один уровень развёртки).
ROUTE_INDEX = "sitemapindex"

#: Потолок числа дочерних sitemap, разворачиваемых из одного ``<sitemapindex>`` (D3).
#: Типовые индексы несут единицы-десятки детей; 50 покрывает крупные сайты и одновременно
#: ограничивает fan-out одного опроса (politeness/DoS-защита ядра).
MAX_CHILD_SITEMAPS = 50

#: Общий потолок числа ``<loc>``, собранных из urlset(s) одного опроса (D3). Совпадает со
#: спековым потолком одного sitemap (50 000 URL) — один «полный» sitemap, без раздувания
#: индекса. Вместе с :data:`MAX_CHILD_SITEMAPS` ограничивает суммарный объём развёртки.
MAX_SITEMAP_ENTRIES = 50_000

#: Потолок распакованного размера gzip-sitemap (байты) — защита от decompression-бомб.
#: Публичные sitemap много меньше; 64 МиБ — с запасом и без риска для памяти ядра.
MAX_DECOMPRESSED_BYTES = 64 * 1024 * 1024

#: Magic-байты gzip (RFC 1952) — детекция формата по содержимому, а не по имени/URL.
_GZIP_MAGIC = b"\x1f\x8b"

#: XML-парсер: DOCTYPE/сущности/external запрещены (как в фидах) — sitemap их не нуждается.
_FORBID_DTD = True


class SitemapParseError(ValueError):
    """Небезопасный/некорректный sitemap либо нераспознанный корень/форма."""


@dataclass(frozen=True)
class SitemapDocument:
    """Разобранный sitemap: маршрут (:data:`ROUTE_URLSET`/:data:`ROUTE_INDEX`) и ``<loc>``.

    ``entries`` — ``<loc>``-записи в порядке документа: для urlset это **страницы**
    (``published_at`` = ``<lastmod>``), для sitemapindex — **дочерние sitemap**. Смысл
    задаётся :attr:`route`; вызывающий разворачивает индекс сам (:func:`expand_sitemap`).
    """

    route: str
    entries: tuple[FeedEntry, ...] = ()


def decompress_if_gzip(
    content: bytes, *, max_bytes: int = MAX_DECOMPRESSED_BYTES
) -> bytes:
    """Распаковывает gzip по magic-байтам; иначе возвращает вход как есть.

    Потолок ``max_bytes`` применяется к **распакованному** размеру: превышение (или
    незавершённый поток, обещающий больше) → :class:`SitemapParseError` (защита от бомб).
    """
    data = bytes(content)
    if not data.startswith(_GZIP_MAGIC):
        return data
    try:
        obj = zlib.decompressobj(16 + zlib.MAX_WBITS)
        out = obj.decompress(data, max_bytes + 1)
    except zlib.error as exc:
        raise SitemapParseError(f"некорректный gzip-sitemap: {exc}") from exc
    if len(out) > max_bytes or obj.unconsumed_tail:
        raise SitemapParseError(
            f"gzip-sitemap превысил потолок распаковки {max_bytes} байт"
        )
    return out


def parse_sitemap(
    content: bytes | str,
    *,
    content_type: str | None = None,
    base_url: str | None = None,
) -> SitemapDocument:
    """Разбирает sitemap (urlset/sitemapindex) в :class:`SitemapDocument`.

    :param content: сырые байты sitemap (в т.ч. gzip — распознаётся по magic). Кодировка
        выводится из BOM/пролога (:func:`core.util.feed._decode_xml`).
    :param content_type: MIME-подсказка; **не** используется для детекции (форма — по
        корневому элементу); принимается для единообразия вызова.
    :param base_url: URL самого sitemap — база для резолва относительных ``<loc>``.
    :raises SitemapParseError: небезопасный (XXE/DTD/сущности) либо некорректный XML;
        нераспознанный корневой элемент; превышен потолок распаковки gzip.
    """
    del content_type  # детекция формы — по корню, не по MIME
    if not isinstance(content, (bytes, bytearray, str)):
        raise SitemapParseError(f"ожидались байты sitemap, получено {type(content).__name__}")
    if isinstance(content, str):
        text = _decode_xml(content)
    else:
        text = _decode_xml(decompress_if_gzip(bytes(content)))
    if not text.strip():
        raise SitemapParseError("пустой sitemap: нет XML-содержимого")

    try:
        root = _DET.fromstring(text, forbid_dtd=_FORBID_DTD)
    except DefusedXmlException as exc:
        raise SitemapParseError(f"небезопасный XML sitemap: {exc}") from exc
    except _DET.ParseError as exc:
        raise SitemapParseError(f"некорректный XML sitemap: {exc}") from exc
    except ValueError as exc:  # защитно: парсер может бросить ValueError
        raise SitemapParseError(f"некорректный XML sitemap: {exc}") from exc

    kind = _localname(root.tag)
    if kind == ROUTE_URLSET:
        return SitemapDocument(ROUTE_URLSET, _locs(root, "url", base_url))
    if kind == ROUTE_INDEX:
        return SitemapDocument(ROUTE_INDEX, _locs(root, "sitemap", base_url))
    raise SitemapParseError(f"нераспознанный корневой элемент sitemap: {kind!r}")


def _locs(root: Any, item_tag: str, base_url: str | None) -> tuple[FeedEntry, ...]:
    """Собирает ``<loc>`` из прямых детей ``item_tag`` (``<url>``/``<sitemap>``).

    Пустой ``<loc>`` пропускается; относительный резолвится против ``base_url``. ``lastmod``
    кладётся в ``published_at`` (best-effort метаданные; сортировка — не здесь).
    """
    entries: list[FeedEntry] = []
    for item in _children(root, item_tag):
        loc = _clean(_text(_first(item, "loc")))
        if not loc:
            continue
        entries.append(
            FeedEntry(
                url=_resolve(loc, base_url),
                published_at=_clean(_text(_first(item, "lastmod"))) or None,
            )
        )
    return tuple(entries)


def expand_sitemap(
    root: SitemapDocument,
    fetch: Callable[[str], SitemapDocument],
    *,
    max_child_sitemaps: int = MAX_CHILD_SITEMAPS,
    max_entries: int = MAX_SITEMAP_ENTRIES,
) -> tuple[list[FeedEntry], bool]:
    """Разворачивает sitemap **на один уровень** в плоский список ``<loc>``-записей.

    Для :data:`ROUTE_URLSET` возвращает его записи (усечённые до ``max_entries``). Для
    :data:`ROUTE_INDEX` фетчит до ``max_child_sitemaps`` детей через ``fetch`` (вызывающий
    сам валидирует URL/SSRF и фетчит транспортом) и объединяет их ``<loc>`` до
    ``max_entries``. Дочерний элемент, оказавшийся ``<sitemapindex>``, — **отказ** (глубже
    одного уровня не поддержано).

    :returns: ``(entries, truncated)`` — ``truncated`` истинно при усечении по любому потолку.
    :raises SitemapParseError: дочерний sitemap — вложенный ``<sitemapindex>``.
    """
    if root.route == ROUTE_URLSET:
        truncated = len(root.entries) > max_entries
        return list(root.entries[:max_entries]), truncated

    children = root.entries[:max_child_sitemaps]
    truncated = len(root.entries) > max_child_sitemaps
    entries: list[FeedEntry] = []
    for child in children:
        if len(entries) >= max_entries:
            truncated = True
            break
        document = fetch(child.url)
        if document.route != ROUTE_URLSET:
            raise SitemapParseError(
                "вложенный <sitemapindex> не поддержан: допускается один уровень "
                "развёртки (индекс с индексами)"
            )
        entries.extend(document.entries)
    if len(entries) > max_entries:
        truncated = True
        entries = entries[:max_entries]
    return entries, truncated
