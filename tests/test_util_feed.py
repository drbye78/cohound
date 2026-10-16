"""Тесты ``core/util/feed.py`` (Phase 5.0) — чистый парсер фидов/каналов.

Сеть/диск не используются: фикстуры — inline-байты. Проверяются RSS 2.0, Atom 1.0,
RDF/RSS 1.0, резолв относительных ссылок, CDATA-заголовки, даты, небезопасный XML
(XXE/DOCTYPE), не-UTF8 кодировка, пустой фид, а также канальные документы
(JSON-массив, JSON-lines, plain-text список). Данные нейтральны (§1.3).
"""

from __future__ import annotations

import pytest

from core.util.feed import (
    FeedEntry,
    FeedParseError,
    parse_channel_document,
    parse_feed,
)

# ---------------------------------------------------------------------------
# Фикстуры (inline-байты)
# ---------------------------------------------------------------------------

_RSS_ABSOLUTE = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Feed</title>
    <item>
      <title>First</title>
      <link>https://e.test/a</link>
      <guid isPermaLink="false">g-1</guid>
      <pubDate>Mon, 06 Jan 2026 12:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

_RSS_RELATIVE_CDATA = b"""<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title><![CDATA[Linked <b>title</b> & more]]></title>
      <link>/page/2</link>
    </item>
  </channel>
</rss>
"""

_ATOM = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Feed</title>
  <entry>
    <title>Entry</title>
    <link rel="self" href="https://e.test/rel"/>
    <link rel="alternate" href="post/1"/>
    <id>urn:uuid:1</id>
    <published>2026-02-01T09:00:00Z</published>
  </entry>
</feed>
"""

_ATOM_HTML_TITLE = b"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title type="html">&lt;b&gt;Bold&lt;/b&gt;</title>
    <link href="https://e.test/h"/>
    <updated>2026-02-02T00:00:00Z</updated>
  </entry>
</feed>
"""

_RDF_RSS10 = b"""<?xml version="1.0"?>
<rdf:RDF
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns="http://purl.org/rss/1.0/"
    xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel rdf:about="https://e.test/rdf">
    <title>RDF Feed</title>
  </channel>
  <item rdf:about="https://e.test/r1">
    <title>RDF Item</title>
    <link>https://e.test/r1</link>
    <dc:date>2026-03-01</dc:date>
  </item>
</rdf:RDF>
"""

_EMPTY_RSS = b'<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>'

_XXE = b"""<?xml version="1.0"?>
<!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<rss version="2.0"><channel><item><link>&xxe;</link></item></channel></rss>
"""

_DOCTYPE_BOMB = b"""<?xml version="1.0"?>
<!DOCTYPE lolz [
  <!ENTITY lol "lol">
  <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;">
  <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;">
]>
<rss version="2.0"><channel><item><link>&lol2;</link></item></channel></rss>
"""

_MALFORMED = b"<rss><channel><item></channel></rss>"

_LATIN1 = (
    b'<?xml version="1.0" encoding="ISO-8859-1"?>'
    b"<rss version=\"2.0\"><channel><item>"
    b"<title>Caf\xe9</title><link>https://e.test/l1</link>"
    b"</item></channel></rss>"
)


# ---------------------------------------------------------------------------
# RSS 2.0
# ---------------------------------------------------------------------------


def test_rss_absolute_link_parses_all_fields() -> None:
    entries = parse_feed(_RSS_ABSOLUTE)
    assert len(entries) == 1
    entry = entries[0]
    assert entry == FeedEntry(
        url="https://e.test/a",
        guid="g-1",
        published_at="Mon, 06 Jan 2026 12:00:00 GMT",
        title="First",
    )


def test_rss_cdata_title_and_relative_link_resolved() -> None:
    entries = parse_feed(_RSS_RELATIVE_CDATA, base_url="https://feed.test/root.xml")
    assert len(entries) == 1
    assert entries[0].url == "https://feed.test/page/2"
    assert entries[0].title == "Linked <b>title</b> & more"


def test_rss_relative_link_kept_without_base_url() -> None:
    entries = parse_feed(_RSS_RELATIVE_CDATA)
    assert entries[0].url == "/page/2"


def test_rss_absolute_link_unaffected_by_base_url() -> None:
    entries = parse_feed(_RSS_ABSOLUTE, base_url="https://feed.test/root.xml")
    assert entries[0].url == "https://e.test/a"


# ---------------------------------------------------------------------------
# Atom 1.0
# ---------------------------------------------------------------------------


def test_atom_prefers_alternate_link_and_resolves_relative() -> None:
    entries = parse_feed(_ATOM, base_url="https://feed.test/atom.xml")
    assert len(entries) == 1
    entry = entries[0]
    assert entry.url == "https://feed.test/post/1"  # rel="alternate", не rel="self"
    assert entry.guid == "urn:uuid:1"
    assert entry.published_at == "2026-02-01T09:00:00Z"
    assert entry.title == "Entry"


def test_atom_html_title_text_extracted() -> None:
    entries = parse_feed(_ATOM_HTML_TITLE)
    assert entries[0].title == "<b>Bold</b>"
    assert entries[0].published_at == "2026-02-02T00:00:00Z"  # фолбэк updated


def test_atom_without_base_url_keeps_relative() -> None:
    entries = parse_feed(_ATOM)
    assert entries[0].url == "post/1"


# ---------------------------------------------------------------------------
# RDF / RSS 1.0
# ---------------------------------------------------------------------------


def test_rdf_parses_about_link_and_dc_date() -> None:
    entries = parse_feed(_RDF_RSS10)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.url == "https://e.test/r1"
    assert entry.guid == "https://e.test/r1"
    assert entry.published_at == "2026-03-01"
    assert entry.title == "RDF Item"


def test_rdf_about_used_when_link_missing() -> None:
    rdf = b"""<?xml version="1.0"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <item rdf:about="https://e.test/only-about"><title>T</title></item>
    </rdf:RDF>
    """
    entries = parse_feed(rdf)
    assert entries[0].url == "https://e.test/only-about"


# ---------------------------------------------------------------------------
# Пустой фид и ошибки
# ---------------------------------------------------------------------------


def test_empty_feed_returns_empty_list() -> None:
    assert parse_feed(_EMPTY_RSS) == []


def test_malformed_xml_raises_feed_parse_error() -> None:
    with pytest.raises(FeedParseError):
        parse_feed(_MALFORMED)


def test_xxe_entity_is_rejected() -> None:
    with pytest.raises(FeedParseError):
        parse_feed(_XXE)


def test_doctype_bomb_is_rejected() -> None:
    with pytest.raises(FeedParseError):
        parse_feed(_DOCTYPE_BOMB)


def test_unrecognized_root_raises() -> None:
    # ``parse_feed`` разбирает только RSS/Atom/RDF; sitemap — у отдельного парсера
    # (``core.util.sitemap.parse_sitemap``, D3), поэтому здесь корень отвергается.
    with pytest.raises(FeedParseError):
        parse_feed(b'<?xml version="1.0"?><sitemap><url/></sitemap>')


def test_empty_bytes_raises() -> None:
    with pytest.raises(FeedParseError):
        parse_feed(b"   ")


# ---------------------------------------------------------------------------
# Кодировки
# ---------------------------------------------------------------------------


def test_latin1_declared_encoding_decoded() -> None:
    entries = parse_feed(_LATIN1)
    assert entries[0].title == "Café"


def test_non_utf8_without_declaration_falls_back_to_latin1() -> None:
    # Байты в latin-1 без пролога: UTF-8-декодирование не проходит → фолбэк latin-1.
    raw = (
        b"<rss version=\"2.0\"><channel><item>"
        b"<title>caf\xe9</title><link>https://e.test/c</link>"
        b"</item></channel></rss>"
    )
    entries = parse_feed(raw)
    assert entries[0].title == "café"


def test_utf8_bom_is_handled() -> None:
    raw = b"\xef\xbb\xbf" + _RSS_ABSOLUTE
    assert parse_feed(raw)[0].url == "https://e.test/a"


# ---------------------------------------------------------------------------
# Канальные документы
# ---------------------------------------------------------------------------


def test_channel_json_array_of_objects() -> None:
    content = (
        b'[{"url": "https://a.test/1", "title": "A", "id": "i1"},'
        b' {"link": "/rel", "title": "B"}]'
    )
    entries = parse_channel_document(content, base_url="https://doc.test/chan.json")
    assert [e.url for e in entries] == ["https://a.test/1", "https://doc.test/rel"]
    assert entries[0].title == "A" and entries[0].guid == "i1"
    assert entries[1].title == "B"


def test_channel_json_array_of_strings() -> None:
    entries = parse_channel_document(b'["https://a.test/1", "https://a.test/2"]')
    assert [e.url for e in entries] == ["https://a.test/1", "https://a.test/2"]


def test_channel_json_lines() -> None:
    content = (
        b'{"url": "https://a.test/1", "title": "one"}\n'
        b'{"url": "https://a.test/2"}\n'
    )
    entries = parse_channel_document(content)
    assert [e.url for e in entries] == ["https://a.test/1", "https://a.test/2"]
    assert entries[0].title == "one"


def test_channel_plain_text_url_list() -> None:
    content = b"https://a.test/1\nhttps://a.test/2\n\n  https://a.test/3  \n"
    entries = parse_channel_document(content)
    assert [e.url for e in entries] == [
        "https://a.test/1",
        "https://a.test/2",
        "https://a.test/3",
    ]


def test_channel_plain_text_utf8_and_latin1() -> None:
    raw = b"https://a.test/caf\xe9\n"
    entries = parse_channel_document(raw)
    assert entries[0].url == "https://a.test/café"


def test_channel_plain_text_resolves_relative() -> None:
    entries = parse_channel_document(b"/a\n/b\n", base_url="https://doc.test/base/")
    assert [e.url for e in entries] == ["https://doc.test/a", "https://doc.test/b"]


def test_channel_json_dict_with_items_key() -> None:
    entries = parse_channel_document(
        b'{"items": [{"url": "https://a.test/1"}, {"url": "https://a.test/2"}]}'
    )
    assert [e.url for e in entries] == ["https://a.test/1", "https://a.test/2"]


def test_channel_empty_returns_empty() -> None:
    assert parse_channel_document(b"") == []
    assert parse_channel_document(b"   \n  \n") == []


def test_channel_skips_empty_and_control_char_urls() -> None:
    content = (
        b'[{"url": ""}, {"url": "  "}, {"url": "https://a.test/ok"},'
        b' {"title": "no url"}]'
    )
    entries = parse_channel_document(content)
    assert [e.url for e in entries] == ["https://a.test/ok"]


def test_channel_strips_control_chars_from_url() -> None:
    entries = parse_channel_document(b"https://a.test/\x00p\x07ath\n")
    assert entries[0].url == "https://a.test/path"


# ---------------------------------------------------------------------------
# Прочее
# ---------------------------------------------------------------------------


def test_parse_feed_rejects_non_bytes_type() -> None:
    with pytest.raises(FeedParseError):
        parse_feed(123)  # type: ignore[arg-type]


def test_feed_parse_error_is_value_error() -> None:
    assert issubclass(FeedParseError, ValueError)


def test_multiple_rss_items_order_preserved() -> None:
    rss = b"""<?xml version="1.0"?>
    <rss version="2.0"><channel>
      <item><link>https://e.test/1</link></item>
      <item><link>https://e.test/2</link></item>
      <item><link>https://e.test/3</link></item>
    </channel></rss>
    """
    assert [e.url for e in parse_feed(rss)] == [
        "https://e.test/1",
        "https://e.test/2",
        "https://e.test/3",
    ]
