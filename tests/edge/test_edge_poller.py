"""Тесты edge-сервиса опроса фидов/каналов ``FeedPollService`` (Wave 5.1).

Проверяются: разбор ``feed`` (XML) и ``channel`` (JSON/список); нормализация ответа
``{entries, count, truncated}``; SSRF-фильтр URL фида (жёсткий отказ) и URL записей
(мягкий пропуск); caps ``limit``/``max_entries``/``since``; усечение по ``max_feed_bytes``;
неблокирующий семафор (``poll busy``); грациозная деградация сбоя фетча. Сети нет —
фейковый ``CrawlerPort`` возвращает заранее заданные байты.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any

import pytest

from core.adapters.feed_poller import LocalFeedPoller
from core.edge.poller import FeedPollService
from core.edge.server import EdgeError
from core.ports.types import FetchPlan, RawPage

_RSS = (
    b'<?xml version="1.0" encoding="utf-8"?>'
    b'<rss version="2.0"><channel>'
    b"<item><title>A</title><link>https://feed.test/a</link>"
    b"<guid>g-a</guid><pubDate>2026-01-01</pubDate></item>"
    b"<item><title>B</title><link>https://feed.test/b</link>"
    b"<pubDate>2027-06-01</pubDate></item>"
    b"</channel></rss>"
)


class FakeCrawler:
    """Стенд ``CrawlerPort``: возвращает заданные байты; опц. ошибку/блокировку."""

    version = "fake-crawler-1"
    engine = "http"

    def __init__(
        self,
        content: bytes = b"",
        *,
        content_type: str | None = "application/rss+xml",
        raises: Exception | None = None,
        blocker: threading.Event | None = None,
        entered: threading.Event | None = None,
    ) -> None:
        self._content = content
        self._content_type = content_type
        self._raises = raises
        self._blocker = blocker
        self._entered = entered
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append((url, plan))
        if self._entered is not None:
            self._entered.set()
        if self._blocker is not None:
            self._blocker.wait(timeout=5)
        if self._raises is not None:
            raise self._raises
        return RawPage(
            url=url,
            content=self._content,
            fetched_at="2026-01-01T00:00:00Z",
            content_type=self._content_type,
        )


def _service(crawler: FakeCrawler, **kwargs: Any) -> FeedPollService:
    return FeedPollService(crawler, **kwargs)


# ---------------------------------------------------------------------------
# Разбор feed/channel и форма ответа
# ---------------------------------------------------------------------------


def test_poll_feed_returns_entries_with_metadata() -> None:
    svc = _service(FakeCrawler(_RSS))
    result = svc.poll({"url": "https://feed.test/rss", "kind": "feed"})
    assert result == {
        "entries": [
            {
                "url": "https://feed.test/a",
                "guid": "g-a",
                "published_at": "2026-01-01",
                "title": "A",
            },
            {
                "url": "https://feed.test/b",
                "guid": None,
                "published_at": "2027-06-01",
                "title": "B",
            },
        ],
        "count": 2,
        "truncated": False,
    }


def test_poll_channel_json_array() -> None:
    payload = b'[{"url": "https://ch.test/1", "title": "one"}, {"url": "https://ch.test/2"}]'
    svc = _service(FakeCrawler(payload, content_type="application/json"))
    result = svc.poll({"url": "https://ch.test/list", "kind": "channel"})
    assert [entry["url"] for entry in result["entries"]] == [
        "https://ch.test/1",
        "https://ch.test/2",
    ]
    assert result["count"] == 2
    assert result["truncated"] is False


def test_poll_channel_plain_text_urls() -> None:
    payload = b"https://ch.test/a\nhttps://ch.test/b\n"
    svc = _service(FakeCrawler(payload, content_type="text/plain"))
    result = svc.poll({"url": "https://ch.test/list", "kind": "channel"})
    assert [entry["url"] for entry in result["entries"]] == [
        "https://ch.test/a",
        "https://ch.test/b",
    ]


def test_poll_resolves_relative_entry_urls_against_base() -> None:
    payload = b'[{"url": "/post/1"}]'
    svc = _service(FakeCrawler(payload, content_type="application/json"))
    result = svc.poll({"url": "https://ch.test/feed/index", "kind": "channel"})
    assert result["entries"][0]["url"] == "https://ch.test/post/1"


# ---------------------------------------------------------------------------
# Валидация запроса
# ---------------------------------------------------------------------------


def test_poll_requires_url() -> None:
    svc = _service(FakeCrawler(_RSS))
    with pytest.raises(EdgeError, match="url"):
        svc.poll({"kind": "feed"})


def test_poll_requires_known_kind() -> None:
    svc = _service(FakeCrawler(_RSS))
    with pytest.raises(EdgeError, match="kind"):
        svc.poll({"url": "https://feed.test/rss", "kind": "calendar"})


# ---------------------------------------------------------------------------
# SSRF
# ---------------------------------------------------------------------------


def test_poll_rejects_ssrf_feed_url_before_fetch() -> None:
    crawler = FakeCrawler(_RSS)
    svc = _service(crawler)
    with pytest.raises(EdgeError, match="URL feed"):
        svc.poll({"url": "http://169.254.169.254/latest", "kind": "feed"})
    assert crawler.calls == []  # фетч не выполнялся


def test_poll_skips_ssrf_entry_urls_softly() -> None:
    payload = b'["https://ok.test/a", "http://10.0.0.1/x", "http://127.0.0.1/y"]'
    svc = _service(FakeCrawler(payload, content_type="application/json"))
    result = svc.poll({"url": "https://ch.test/list", "kind": "channel"})
    assert [entry["url"] for entry in result["entries"]] == ["https://ok.test/a"]
    assert result["count"] == 1


# ---------------------------------------------------------------------------
# Caps: limit / max_entries / since / max_feed_bytes
# ---------------------------------------------------------------------------


def test_poll_limit_truncates_and_flags() -> None:
    svc = _service(FakeCrawler(_RSS))
    result = svc.poll(
        {"url": "https://feed.test/rss", "kind": "feed", "params": {"limit": 1}}
    )
    assert result["count"] == 1
    assert result["truncated"] is True


def test_poll_max_entries_hard_cap() -> None:
    payload = b'["https://ch.test/a", "https://ch.test/b", "https://ch.test/c"]'
    svc = _service(FakeCrawler(payload, content_type="application/json"), max_entries=2)
    result = svc.poll(
        {"url": "https://ch.test/list", "kind": "channel", "params": {"limit": 100}}
    )
    assert result["count"] == 2
    assert result["truncated"] is True


def test_poll_since_filters_by_published_at() -> None:
    svc = _service(FakeCrawler(_RSS))
    result = svc.poll(
        {"url": "https://feed.test/rss", "kind": "feed", "params": {"since": "2027-01-01"}}
    )
    assert [entry["url"] for entry in result["entries"]] == ["https://feed.test/b"]


def test_poll_max_feed_bytes_truncation_is_flagged_or_errors() -> None:
    # Канальный plain-text усечён: ответ помечен truncated, первая запись сохранена.
    payload = b"https://ch.test/first\nhttps://ch.test/second\n"
    svc = _service(FakeCrawler(payload, content_type="text/plain"), max_feed_bytes=25)
    result = svc.poll({"url": "https://ch.test/list", "kind": "channel"})
    assert result["truncated"] is True
    assert result["entries"][0]["url"] == "https://ch.test/first"
    # Усечённый хвост не должен дать полный исходный второй URL.
    assert "https://ch.test/second" not in [e["url"] for e in result["entries"]]


def test_poll_max_feed_bytes_breaking_xml_raises_size_error() -> None:
    svc = _service(FakeCrawler(_RSS), max_feed_bytes=30)
    with pytest.raises(EdgeError, match="лимит"):
        svc.poll({"url": "https://feed.test/rss", "kind": "feed"})


# ---------------------------------------------------------------------------
# Параллелизм / деградация
# ---------------------------------------------------------------------------


def test_poll_busy_when_concurrency_exhausted() -> None:
    entered = threading.Event()
    blocker = threading.Event()
    crawler = FakeCrawler(_RSS, blocker=blocker, entered=entered)
    svc = _service(crawler, poll_concurrency=1)

    outcome: list[Any] = []

    def first() -> None:
        try:
            outcome.append(svc.poll({"url": "https://feed.test/rss", "kind": "feed"}))
        except Exception as exc:  # noqa: BLE001
            outcome.append(exc)

    thread = threading.Thread(target=first)
    thread.start()
    try:
        assert entered.wait(timeout=5)
        with pytest.raises(EdgeError, match="poll busy"):
            svc.poll({"url": "https://feed.test/rss", "kind": "feed"})
    finally:
        blocker.set()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert outcome and not isinstance(outcome[0], Exception)


def test_poll_fetch_exception_becomes_edge_error() -> None:
    svc = _service(FakeCrawler(_RSS, raises=RuntimeError("feed down")))
    with pytest.raises(EdgeError, match="feed down"):
        svc.poll({"url": "https://feed.test/rss", "kind": "feed"})


def test_poll_invalid_xml_becomes_edge_error() -> None:
    svc = _service(FakeCrawler(b"<rss><channel><item>", content_type="application/rss+xml"))
    with pytest.raises(EdgeError, match="parse"):
        svc.poll({"url": "https://feed.test/rss", "kind": "feed"})


def test_poll_request_is_read_only_content_not_evidence() -> None:
    """Сервис не пишет в store и возвращает только ссылки (I19): у записей нет байтов."""
    svc = _service(FakeCrawler(_RSS))
    result = svc.poll({"url": "https://feed.test/rss", "kind": "feed"})
    for entry in result["entries"]:
        assert set(entry) == {"url", "guid", "published_at", "title"}


def test_poll_builds_fetch_plan_axes() -> None:
    crawler = FakeCrawler(_RSS)
    svc = _service(crawler)
    svc.poll(
        {
            "url": "https://feed.test/rss",
            "kind": "feed",
            "fetch": "browser",
            "strategy": "hybrid",
        }
    )
    url, plan = crawler.calls[0]
    assert url == "https://feed.test/rss"
    assert plan.fetch.value == "browser"
    assert plan.strategy.value == "hybrid"


def test_poll_request_mapping_absent_optional_fields() -> None:
    """Отсутствие опциональных осей/params не ломает опрос (дефолты)."""
    svc = _service(FakeCrawler(_RSS))
    result = svc.poll({"url": "https://feed.test/rss", "kind": "feed"})
    assert result["count"] == 2


def test_poll_accepts_mapping_subclass() -> None:
    class MyRequest(dict[str, Any]):
        pass

    svc = _service(FakeCrawler(_RSS))
    request: Mapping[str, Any] = MyRequest(url="https://feed.test/rss", kind="feed")
    assert svc.poll(request)["count"] == 2


# ---------------------------------------------------------------------------
# N1: семантика ``since`` общая с локальным поллером (разбор, не строковое сравнение)
# ---------------------------------------------------------------------------

#: RFC822-дата ``10 Oct 2026`` лексикографически МЕНЬШЕ порога ``2026-10-08…``
#: (``'1' < '2'``), но по разобранному времени — новее порога. Лексикографический фильтр
#: ошибочно отбросил бы запись A.
_LEX_RSS = (
    b'<?xml version="1.0" encoding="utf-8"?>'
    b'<rss version="2.0"><channel>'
    b"<item><title>A</title><link>https://feed.test/a</link>"
    b"<pubDate>10 Oct 2026 00:00:00 GMT</pubDate></item>"
    b"<item><title>B</title><link>https://feed.test/b</link>"
    b"<pubDate>01 Jan 2026 00:00:00 GMT</pubDate></item>"
    b"<item><title>C</title><link>https://feed.test/c</link>"
    b"<pubDate>2026-10-20T00:00:00Z</pubDate></item>"
    b"</channel></rss>"
)

_SINCE = "2026-10-08T00:00:00Z"


def test_poll_since_parses_rfc822_not_lexicographic() -> None:
    """Регрессия N1: RFC822 ``10 Oct 2026`` (новее ``since``) сохраняется, старый — нет."""
    svc = _service(FakeCrawler(_LEX_RSS))
    result = svc.poll(
        {"url": "https://feed.test/rss", "kind": "feed", "params": {"since": _SINCE}}
    )
    urls = [entry["url"] for entry in result["entries"]]
    # Лексикографический фильтр отбросил бы A (``'1' < '2'``) — корректный разбор сохраняет.
    assert urls == ["https://feed.test/a", "https://feed.test/c"]


def test_poll_since_matches_local_poller() -> None:
    """N1: edge-сервис и ``LocalFeedPoller`` дают одинаковый набор при одном ``since``."""
    edge_urls = [
        entry["url"]
        for entry in _service(FakeCrawler(_LEX_RSS)).poll(
            {"url": "https://feed.test/rss", "kind": "feed", "params": {"since": _SINCE}}
        )["entries"]
    ]
    local_urls = [
        entry.url
        for entry in LocalFeedPoller(FakeCrawler(_LEX_RSS)).poll(
            "https://feed.test/rss", kind="feed", params={"since": _SINCE}, plan=FetchPlan()
        )
    ]
    assert edge_urls == local_urls == ["https://feed.test/a", "https://feed.test/c"]


# ---------------------------------------------------------------------------
# D3: sitemap (urlset / gzip / sitemapindex / caps / SSRF / вложенный индекс)
# ---------------------------------------------------------------------------

_SITEMAP = (
    b'<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    b"<url><loc>https://e.test/a</loc></url>"
    b"<url><loc>https://e.test/b</loc></url>"
    b"</urlset>"
)

_SITEMAP_INDEX = (
    b'<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    b"<sitemap><loc>https://e.test/sm-1.xml</loc></sitemap>"
    b"</sitemapindex>"
)


class _MapCrawler:
    """``CrawlerPort``-стенд с картой ``url -> bytes`` (для развёртки sitemapindex)."""

    version = "fake-crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes]) -> None:
        self._pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        return RawPage(url=url, content=self._pages[url], fetched_at="2026-01-01T00:00:00Z")


def test_poll_sitemap_urlset_returns_entries() -> None:
    svc = _service(FakeCrawler(_SITEMAP, content_type="application/xml"))
    result = svc.poll({"url": "https://e.test/sitemap.xml", "kind": "sitemap"})
    assert [e["url"] for e in result["entries"]] == ["https://e.test/a", "https://e.test/b"]
    assert result["count"] == 2 and result["truncated"] is False


def test_poll_sitemap_gzip_supported() -> None:
    import gzip as _gzip

    svc = _service(FakeCrawler(_gzip.compress(_SITEMAP), content_type="application/x-gzip"))
    result = svc.poll({"url": "https://e.test/sitemap.xml.gz", "kind": "sitemap"})
    assert [e["url"] for e in result["entries"]] == ["https://e.test/a", "https://e.test/b"]


def test_poll_sitemap_index_expands_children() -> None:
    crawler = _MapCrawler(
        {
            "https://e.test/sitemap.xml": _SITEMAP_INDEX,
            "https://e.test/sm-1.xml": b"<urlset><url><loc>https://e.test/1</loc></url></urlset>",
        }
    )
    svc = _service(crawler)
    result = svc.poll({"url": "https://e.test/sitemap.xml", "kind": "sitemap"})
    assert [e["url"] for e in result["entries"]] == ["https://e.test/1"]
    assert crawler.calls == ["https://e.test/sitemap.xml", "https://e.test/sm-1.xml"]


def test_poll_sitemap_nested_index_errors() -> None:
    crawler = _MapCrawler(
        {"https://e.test/sitemap.xml": _SITEMAP_INDEX,
         "https://e.test/sm-1.xml": _SITEMAP_INDEX}
    )
    svc = _service(crawler)
    with pytest.raises(EdgeError, match="вложенный"):
        svc.poll({"url": "https://e.test/sitemap.xml", "kind": "sitemap"})


def test_poll_sitemap_soft_drops_ssrf_locs() -> None:
    payload = (
        b"<urlset><url><loc>https://ok.test/a</loc></url>"
        b"<url><loc>http://127.0.0.1/x</loc></url></urlset>"
    )
    svc = _service(FakeCrawler(payload, content_type="application/xml"))
    result = svc.poll({"url": "https://e.test/sitemap.xml", "kind": "sitemap"})
    assert [e["url"] for e in result["entries"]] == ["https://ok.test/a"]


def test_poll_sitemap_limit_truncates_and_flags() -> None:
    svc = _service(FakeCrawler(_SITEMAP, content_type="application/xml"))
    result = svc.poll(
        {"url": "https://e.test/sitemap.xml", "kind": "sitemap", "params": {"limit": 1}}
    )
    assert result["count"] == 1 and result["truncated"] is True


def test_poll_sitemap_rejects_ssrf_root_before_fetch() -> None:
    crawler = FakeCrawler(_SITEMAP)
    svc = _service(crawler)
    with pytest.raises(EdgeError, match="URL sitemap"):
        svc.poll({"url": "http://169.254.169.254/sitemap.xml", "kind": "sitemap"})
    assert crawler.calls == []
