"""Юнит-тесты лёгкого HTTP-краулера ``core/adapters/crawl_http.py`` (M1, A1).

Сеть не используется: внедряется шов ``fetcher``/``http_get``, кэш — ``tmp_path``.
Проверяются: ``RawPage`` из шва, отображение ответа ``httpx`` (финальный URL/статус/
тип), SSRF-отказ до фетча, честная ошибка на ``RenderMode.BROWSER``, интеграция с
content-addressed кэшем (промах → фетч → запись → попадание без повторного фетча),
``available``/ленивый ``httpx``, ``version`` и структурное соответствие ``CrawlerPort``.
Данные нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.adapters.crawl_http import CrawlUnavailable, HttpCrawler
from core.adapters.crawl_scrapy import CrawlError, CrawlUrlError
from core.ports.crawler import CrawlerPort
from core.ports.types import RawPage, RenderMode
from core.util.cache import ContentAddressedCache, http_key

URL = "https://example.com/page"
FIXED_TS = "2026-01-01T00:00:00+00:00"
BODY = b"<html><body>hello</body></html>"


def _counting_fetcher(calls: list[str], content: bytes = BODY):
    """Callable-шов ``(url) -> bytes``, пишущий вызовы в ``calls``."""

    def fetch(url: str) -> bytes:
        calls.append(url)
        return content

    return fetch


class _FakeResponse:
    """Минимальный дублёр ответа ``httpx`` для проверки отображения полей."""

    def __init__(
        self,
        url: str = "https://example.com/final",
        content: bytes = BODY,
        status_code: int = 200,
        content_type: str | None = "text/html; charset=utf-8",
    ) -> None:
        self.url = url
        self.content = content
        self.status_code = status_code
        self.headers = {"Content-Type": content_type} if content_type else {}


# ---------------------------------------------------------------------------
# Фетч через шов
# ---------------------------------------------------------------------------


def test_fetch_via_fetcher_returns_raw_page() -> None:
    calls: list[str] = []
    crawler = HttpCrawler(_counting_fetcher(calls))

    page = crawler.fetch(URL)

    assert isinstance(page, RawPage)
    assert page.url == URL
    assert page.content == BODY
    assert page.render_mode is RenderMode.HTTP
    assert page.fetched_at  # UTC ISO-таймстемп
    assert calls == [URL]


def test_http_get_alias_is_accepted() -> None:
    crawler = HttpCrawler(http_get=lambda url: b"alias-body")  # type: ignore[arg-type]
    assert crawler.fetch(URL).content == b"alias-body"


def test_both_seams_rejected() -> None:
    with pytest.raises(ValueError):
        HttpCrawler(lambda url: b"a", http_get=lambda url: b"b")


def test_fetcher_exception_wrapped_in_crawl_error(tmp_path: Path) -> None:
    def boom(url: str) -> bytes:
        raise RuntimeError("network down")

    cache = ContentAddressedCache(tmp_path)
    crawler = HttpCrawler(boom, cache=cache)
    with pytest.raises(CrawlError):
        crawler.fetch(URL)
    # при сбое в кэш ничего не пишется
    assert cache.get_bytes("http", http_key(URL)) is None


def test_page_from_response_maps_final_url_status_and_type() -> None:
    page = HttpCrawler._page_from_response(
        _FakeResponse(
            url="https://example.com/redirected",
            content=b"redirected-body",
            status_code=203,
        )
    )
    assert page.url == "https://example.com/redirected"
    assert page.content == b"redirected-body"
    assert page.status_code == 203
    assert page.content_type == "text/html; charset=utf-8"
    assert page.render_mode is RenderMode.HTTP


def test_page_from_response_without_content_type() -> None:
    page = HttpCrawler._page_from_response(_FakeResponse(content_type=None))
    assert page.content_type is None


# ---------------------------------------------------------------------------
# SSRF / render
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile",
    ["http://127.0.0.1/", "http://169.254.169.254/latest/meta-data/", "ftp://example.com/x"],
)
def test_ssrf_url_rejected_before_fetch(hostile: str) -> None:
    calls: list[str] = []
    crawler = HttpCrawler(_counting_fetcher(calls))
    with pytest.raises(ValueError):
        crawler.fetch(hostile)
    assert calls == []  # фетч не выполнен


def test_ssrf_error_is_crawl_error_too() -> None:
    crawler = HttpCrawler(_counting_fetcher([]))
    with pytest.raises(CrawlError):
        crawler.fetch("http://127.0.0.1/")
    assert issubclass(CrawlUrlError, CrawlError)
    assert issubclass(CrawlUrlError, ValueError)


@pytest.mark.parametrize("render", [RenderMode.BROWSER, "browser"])
def test_browser_render_raises_honest_error(render: object) -> None:
    calls: list[str] = []
    crawler = HttpCrawler(_counting_fetcher(calls))
    with pytest.raises(CrawlError, match="browser"):
        crawler.fetch(URL, render=render)  # type: ignore[arg-type]
    assert calls == []  # ни fetcher, ни httpx не задействованы


# ---------------------------------------------------------------------------
# Кэш (R7, ADR 0016)
# ---------------------------------------------------------------------------


def test_cache_miss_fetches_and_second_call_served_from_cache(tmp_path: Path) -> None:
    calls: list[str] = []
    crawler = HttpCrawler(
        _counting_fetcher(calls, b"fresh-body"),
        cache=ContentAddressedCache(tmp_path),
    )

    first = crawler.fetch(URL)
    assert first.content == b"fresh-body"
    assert calls == [URL]
    # Промах записал сырые байты под http_key(url).
    assert (tmp_path / "http" / f"{http_key(URL)}.bin").read_bytes() == b"fresh-body"

    second = crawler.fetch(URL)
    assert second.content == b"fresh-body"
    assert calls == [URL]  # второй вызов — из кэша, шов не дёргался


def test_cache_hit_skips_fetcher(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", http_key(URL), b"cached-body")
    calls: list[str] = []
    crawler = HttpCrawler(_counting_fetcher(calls, b"live-body"), cache=cache)

    page = crawler.fetch(URL)

    assert page.content == b"cached-body"
    assert calls == []


def test_without_cache_each_call_fetches() -> None:
    calls: list[str] = []
    crawler = HttpCrawler(_counting_fetcher(calls))
    crawler.fetch(URL)
    crawler.fetch(URL)
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# Ленивый httpx / доступность / контракт
# ---------------------------------------------------------------------------


def test_module_importable_without_httpx() -> None:
    import core.adapters.crawl_http as mod

    assert "httpx" not in getattr(mod, "__dict__", {})


def test_available_true_with_injected_fetcher() -> None:
    assert HttpCrawler(_counting_fetcher([])).available is True


def test_available_matches_httpx_environment() -> None:
    import core.adapters.crawl_http as mod

    assert HttpCrawler().available is mod._httpx_available()


def test_available_false_without_httpx_and_without_seam(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import core.adapters.crawl_http as mod

    monkeypatch.setattr(mod, "_httpx_available", lambda: False)
    assert HttpCrawler().available is False


def test_real_path_without_httpx_raises_crawl_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import core.adapters.crawl_http as mod

    monkeypatch.setattr(mod, "_httpx_available", lambda: False)
    crawler = HttpCrawler()
    with pytest.raises(CrawlError):
        crawler.fetch(URL)
    assert issubclass(CrawlUnavailable, CrawlError)


def test_version_and_port_conformance() -> None:
    crawler = HttpCrawler(_counting_fetcher([]))
    assert isinstance(crawler.version, str) and crawler.version
    assert isinstance(crawler, CrawlerPort)
    assert callable(crawler.fetch)
