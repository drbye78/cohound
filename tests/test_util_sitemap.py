"""Тесты ``core/util/sitemap.py`` (D3) — чистый парсер sitemap.

Сеть/диск не используются: фикстуры — inline-байты. Проверяются urlset, ``sitemapindex``
(один уровень развёртки), gzip (magic-байты), потолки (``MAX_CHILD_SITEMAPS``/
``MAX_SITEMAP_ENTRIES``), вложенный индекс (отказ), небезопасный XML (XXE/DTD) и
некорректные входы. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import gzip

import pytest

from core.util.sitemap import (
    MAX_CHILD_SITEMAPS,
    MAX_SITEMAP_ENTRIES,
    ROUTE_INDEX,
    ROUTE_URLSET,
    SitemapParseError,
    decompress_if_gzip,
    expand_sitemap,
    parse_sitemap,
)

_URLSET = b"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://e.test/a</loc><lastmod>2026-01-02</lastmod></url>
  <url><loc>https://e.test/b</loc></url>
  <url><loc></loc></url>
</urlset>
"""

_URLSET_RELATIVE = b"""<?xml version="1.0"?>
<urlset><url><loc>/page/2</loc></url></urlset>
"""

_INDEX = b"""<?xml version="1.0"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://e.test/sitemap-1.xml</loc></sitemap>
  <sitemap><loc>https://e.test/sitemap-2.xml</loc></sitemap>
</sitemapindex>
"""

_XXE = b"""<?xml version="1.0"?>
<!DOCTYPE urlset [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<urlset><url><loc>&xxe;</loc></url></urlset>
"""


# ---------------------------------------------------------------------------
# urlset
# ---------------------------------------------------------------------------


def test_urlset_parses_locs_and_lastmod() -> None:
    doc = parse_sitemap(_URLSET)
    assert doc.route == ROUTE_URLSET
    assert [e.url for e in doc.entries] == ["https://e.test/a", "https://e.test/b"]
    assert doc.entries[0].published_at == "2026-01-02"
    assert doc.entries[1].published_at is None


def test_urlset_resolves_relative_loc_against_base() -> None:
    doc = parse_sitemap(_URLSET_RELATIVE, base_url="https://e.test/sitemap.xml")
    assert doc.entries[0].url == "https://e.test/page/2"


def test_urlset_relative_kept_without_base_url() -> None:
    doc = parse_sitemap(_URLSET_RELATIVE)
    assert doc.entries[0].url == "/page/2"


def test_empty_urlset_returns_empty() -> None:
    doc = parse_sitemap(b'<?xml version="1.0"?><urlset></urlset>')
    assert doc.route == ROUTE_URLSET and doc.entries == ()


def test_parse_sitemap_accepts_str_content() -> None:
    doc = parse_sitemap('<urlset><url><loc>https://e.test/a</loc></url></urlset>')
    assert [e.url for e in doc.entries] == ["https://e.test/a"]


# ---------------------------------------------------------------------------
# sitemapindex + expand_sitemap
# ---------------------------------------------------------------------------


def test_index_parses_child_sitemaps() -> None:
    doc = parse_sitemap(_INDEX)
    assert doc.route == ROUTE_INDEX
    assert [e.url for e in doc.entries] == [
        "https://e.test/sitemap-1.xml",
        "https://e.test/sitemap-2.xml",
    ]


def test_expand_urlset_returns_entries() -> None:
    entries, truncated = expand_sitemap(parse_sitemap(_URLSET), _fail_fetch)
    assert [e.url for e in entries] == ["https://e.test/a", "https://e.test/b"]
    assert truncated is False


def test_expand_index_one_level_fetches_children() -> None:
    children = {
        "https://e.test/sitemap-1.xml": parse_sitemap(
            b'<urlset><url><loc>https://e.test/1</loc></url></urlset>'
        ),
        "https://e.test/sitemap-2.xml": parse_sitemap(
            b'<urlset><url><loc>https://e.test/2</loc></url></urlset>'
        ),
    }
    calls: list[str] = []

    def fetch(url: str):
        calls.append(url)
        return children[url]

    entries, truncated = expand_sitemap(parse_sitemap(_INDEX), fetch)
    assert [e.url for e in entries] == ["https://e.test/1", "https://e.test/2"]
    assert calls == list(children)
    assert truncated is False


def test_expand_index_refuses_nested_index() -> None:
    nested = parse_sitemap(_INDEX)

    def fetch(_url: str):
        return nested  # дочерний sitemap сам является индексом

    with pytest.raises(SitemapParseError, match="вложенный"):
        expand_sitemap(parse_sitemap(_INDEX), fetch)


def test_expand_index_caps_children() -> None:
    many = _index_with(MAX_CHILD_SITEMAPS + 5)
    fetched: list[str] = []

    def fetch(url: str):
        fetched.append(url)
        return parse_sitemap(b"<urlset></urlset>")

    _entries, truncated = expand_sitemap(parse_sitemap(many), fetch)
    assert len(fetched) == MAX_CHILD_SITEMAPS
    assert truncated is True


def test_expand_caps_total_entries() -> None:
    urlset = parse_sitemap(
        b"<urlset>"
        + b"".join(
            f"<url><loc>https://e.test/{i}</loc></url>".encode() for i in range(10)
        )
        + b"</urlset>"
    )
    entries, truncated = expand_sitemap(urlset, _fail_fetch, max_entries=3)
    assert len(entries) == 3
    assert truncated is True


def test_max_sitemap_entries_constant_is_spec_bound() -> None:
    # Спековый потолок одного sitemap — 50 000 URL (константа не «на глаз»).
    assert MAX_SITEMAP_ENTRIES == 50_000


def test_expand_index_respects_total_cap_across_children() -> None:
    index = _index_with(3)

    def fetch(_url: str):
        return parse_sitemap(
            b"<urlset><url><loc>https://e.test/x</loc></url>"
            b"<url><loc>https://e.test/y</loc></url></urlset>"
        )

    entries, truncated = expand_sitemap(parse_sitemap(index), fetch, max_entries=2)
    assert len(entries) == 2
    assert truncated is True


# ---------------------------------------------------------------------------
# gzip
# ---------------------------------------------------------------------------


def test_gzip_sitemap_is_decompressed() -> None:
    doc = parse_sitemap(gzip.compress(_URLSET))
    assert [e.url for e in doc.entries] == ["https://e.test/a", "https://e.test/b"]


def test_decompress_if_gzip_passthrough_for_plain() -> None:
    assert decompress_if_gzip(b"<urlset/>") == b"<urlset/>"


def test_decompress_if_gzip_enforces_cap() -> None:
    with pytest.raises(SitemapParseError, match="потолок"):
        decompress_if_gzip(gzip.compress(b"x" * 4096), max_bytes=16)


def test_bad_gzip_raises() -> None:
    with pytest.raises(SitemapParseError, match="gzip"):
        parse_sitemap(b"\x1f\x8bnot-really-gzip")


# ---------------------------------------------------------------------------
# Ошибки/безопасность
# ---------------------------------------------------------------------------


def test_unrecognized_root_raises() -> None:
    with pytest.raises(SitemapParseError, match="корневой"):
        parse_sitemap(b'<?xml version="1.0"?><rss><channel/></rss>')


def test_malformed_xml_raises() -> None:
    with pytest.raises(SitemapParseError):
        parse_sitemap(b"<urlset><url><loc>")


def test_xxe_is_rejected() -> None:
    with pytest.raises(SitemapParseError):
        parse_sitemap(_XXE)


def test_empty_bytes_raise() -> None:
    with pytest.raises(SitemapParseError):
        parse_sitemap(b"   ")


def test_non_bytes_type_raises() -> None:
    with pytest.raises(SitemapParseError):
        parse_sitemap(123)  # type: ignore[arg-type]


def test_sitemap_parse_error_is_value_error() -> None:
    assert issubclass(SitemapParseError, ValueError)


# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------


def _fail_fetch(_url: str):  # noqa: ANN202 — заглушка: fetch не должен вызываться
    raise AssertionError("fetch не должен вызываться для urlset")


def _index_with(count: int) -> bytes:
    children = b"".join(
        f"<sitemap><loc>https://e.test/sm-{i}.xml</loc></sitemap>".encode()
        for i in range(count)
    )
    return b"<sitemapindex>" + children + b"</sitemapindex>"
