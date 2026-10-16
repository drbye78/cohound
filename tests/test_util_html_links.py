"""Тесты извлечения ссылок (``core.util.html_links``) — D1 границы обхода (§10).

Чистые, без сети: ``extract_links`` извлекает только ``<a href>``, нормализует и
отбрасывает вне-scope по схеме (``mailto:``/``javascript:``/``data:``/``tel:`` через
``validate_url``), фрагменты и дубликаты страницы.
"""

from __future__ import annotations

from core.util.html_links import extract_links

BASE = "https://example.com/dir/page.html"


def test_extracts_and_normalizes_relative_links() -> None:
    html = b'<a href="../other">o</a><a href="/abs?q=1#frag">a</a>'
    assert extract_links(html, BASE) == [
        "https://example.com/other",
        "https://example.com/abs?q=1",
    ]


def test_rejects_non_http_schemes_and_empty_and_fragment() -> None:
    html = (
        b'<a href="mailto:x@y.z">m</a>'
        b'<a href="javascript:alert(1)">j</a>'
        b'<a href="data:text/html,x">d</a>'
        b'<a href="tel:+1">t</a>'
        b'<a href="">empty</a>'
        b'<a href="#frag">f</a>'
        b'<a href="ftp://h/x">ftp</a>'
    )
    assert extract_links(html, BASE) == []


def test_dedupes_in_page_preserving_order() -> None:
    html = b'<a href="/a">1</a><a href="/b">2</a><A HREF="/a">3</A>'
    assert extract_links(html, BASE) == [
        "https://example.com/a",
        "https://example.com/b",
    ]


def test_lowercases_scheme_host_and_drops_default_port_keeps_path_case() -> None:
    html = b'<a href="HTTPS://Example.COM:443/Case/Path?Q=1#z">x</a>'
    assert extract_links(html, BASE) == [
        "https://example.com/Case/Path?Q=1",
    ]


def test_ignores_non_anchor_and_rel_nofollow() -> None:
    html = (
        b'<area href="/area">'
        b'<link href="/link">'
        b'<iframe src="/iframe"></iframe>'
        b'<img src="/img">'
        b'<a href="/keep" rel="nofollow">k</a>'
    )
    assert extract_links(html, BASE) == ["https://example.com/keep"]


def test_private_ip_rejected_by_ssrf_policy() -> None:
    html = b'<a href="http://127.0.0.1/x">p</a><a href="http://169.254.169.254/meta">m</a>'
    assert extract_links(html, BASE) == []


def test_does_not_raise_on_broken_html() -> None:
    assert extract_links(b"<a href=/x", BASE) == []  # незакрытый тег не извлекается
    assert extract_links(b"not html \xff\xfe", BASE) == []  # битая кодировка не роняет
    assert extract_links(b'<a href="/x">no closing', BASE) == [
        "https://example.com/x"
    ]
