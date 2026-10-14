"""Тесты ``LocalFeedPoller``/``RemoteFeedPoller`` (Phase 5.1; §3.2, Ш5).

Сети нет: локальный поллер работает через фейковый ``CrawlerPort`` (заданные байты),
remote — через фейковый ``ContentGateway`` (payload'ы ``crawl/poll``). Проверяются:
разбор RSS/Atom/канала, обрезка (``max_feed_bytes``/``max_entries``), caps ``params``
(``limit``/``since``), SSRF-отбраковка URL ленты и записей, форма ответа шва и точный
payload запроса. Нейтральные фикстуры (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from core.adapters.feed_poller import LocalFeedPoller, RemoteFeedPoller
from core.adapters.remote_gateway import RemoteContentError
from core.ports.types import FetchPlan, RawPage
from core.util.feed import FeedEntry, FeedParseError

RSS = (
    b"<?xml version=\"1.0\"?>"
    b"<rss version=\"2.0\"><channel>"
    b"<item><title>A</title><link>https://docs.test/a</link>"
    b"<guid>g-a</guid><pubDate>2026-10-01T00:00:00Z</pubDate></item>"
    b"<item><title>B</title><link>https://docs.test/b</link>"
    b"<guid>g-b</guid><pubDate>2026-10-10T00:00:00Z</pubDate></item>"
    b"<item><title>C</title><link>https://docs.test/c</link></item>"
    b"</channel></rss>"
)

ATOM = (
    b"<?xml version=\"1.0\" encoding=\"utf-8\"?>"
    b"<feed xmlns=\"http://www.w3.org/2005/Atom\">"
    b"<entry><title>X</title><link rel=\"alternate\" href=\"https://docs.test/x\"/>"
    b"<id>id-x</id><published>2026-10-05T00:00:00Z</published></entry>"
    b"</feed>"
)

CHANNEL_TEXT = b"https://docs.test/1\nhttps://docs.test/2\nhttps://docs.test/3\n"

FEED_URL = "https://docs.test/feed"


class _FakeCrawler:
    """``CrawlerPort``-заглушка: отдаёт заданные байты; журналирует ``fetch``."""

    version = "fake-crawler-1"

    def __init__(self, content: bytes, *, content_type: str | None = None) -> None:
        self._content = content
        self._content_type = content_type
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self._content,
            fetched_at="2026-01-01T00:00:00Z",
            content_type=self._content_type,
        )


class _FakeGateway:
    """``ContentGateway``-заглушка: возвращает заготовленный ответ; журналирует вызовы."""

    schema_version = "1"

    def __init__(self, response: Any) -> None:
        self._response = response
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def call(self, op: str, payload: Mapping[str, Any]) -> Any:
        self.calls.append((op, dict(payload)))
        return self._response

    def capabilities(self) -> Mapping[str, Any]:  # pragma: no cover - не используется
        return {}


def _urls(entries: list[FeedEntry]) -> list[str]:
    return [entry.url for entry in entries]


# ---------------------------------------------------------------------------
# LocalFeedPoller: разбор
# ---------------------------------------------------------------------------


def test_local_poll_rss_returns_entries() -> None:
    crawler = _FakeCrawler(RSS)
    poller = LocalFeedPoller(crawler)

    entries = poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())

    assert _urls(entries) == [
        "https://docs.test/a",
        "https://docs.test/b",
        "https://docs.test/c",
    ]
    assert entries[0] == FeedEntry(
        url="https://docs.test/a",
        guid="g-a",
        published_at="2026-10-01T00:00:00Z",
        title="A",
    )
    assert crawler.calls == [FEED_URL]


def test_local_poll_atom_returns_entries() -> None:
    poller = LocalFeedPoller(_FakeCrawler(ATOM))
    entries = poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())
    assert _urls(entries) == ["https://docs.test/x"]
    assert entries[0].guid == "id-x"


def test_local_poll_channel_text_returns_entries() -> None:
    poller = LocalFeedPoller(_FakeCrawler(CHANNEL_TEXT))
    entries = poller.poll(FEED_URL, kind="channel", params={}, plan=FetchPlan())
    assert _urls(entries) == [
        "https://docs.test/1",
        "https://docs.test/2",
        "https://docs.test/3",
    ]


def test_local_poll_channel_json_returns_entries() -> None:
    content = b'[{"url": "https://docs.test/j", "id": "j1", "title": "J"}]'
    poller = LocalFeedPoller(_FakeCrawler(content))
    entries = poller.poll(FEED_URL, kind="channel", params={}, plan=FetchPlan())
    assert entries == [FeedEntry(url="https://docs.test/j", guid="j1", title="J")]


def test_local_poll_unknown_kind_raises() -> None:
    poller = LocalFeedPoller(_FakeCrawler(RSS))
    with pytest.raises(ValueError, match="kind"):
        poller.poll(FEED_URL, kind="sitemap", params={}, plan=FetchPlan())


# ---------------------------------------------------------------------------
# LocalFeedPoller: caps
# ---------------------------------------------------------------------------


def test_local_poll_max_entries_truncates() -> None:
    poller = LocalFeedPoller(_FakeCrawler(RSS), max_entries=2)
    entries = poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())
    assert _urls(entries) == ["https://docs.test/a", "https://docs.test/b"]


def test_local_poll_limit_is_clamped_to_max_entries() -> None:
    poller = LocalFeedPoller(_FakeCrawler(RSS), max_entries=2)
    # limit=5 (> max_entries) → эффективный потолок 2.
    entries = poller.poll(FEED_URL, kind="feed", params={"limit": 5}, plan=FetchPlan())
    assert len(entries) == 2


def test_local_poll_limit_smaller_than_max_entries() -> None:
    poller = LocalFeedPoller(_FakeCrawler(RSS), max_entries=10)
    entries = poller.poll(FEED_URL, kind="feed", params={"limit": 1}, plan=FetchPlan())
    assert _urls(entries) == ["https://docs.test/a"]


def test_local_poll_max_feed_bytes_truncates_content() -> None:
    # Обрезка байтов канала до первой строки → только одна запись.
    max_bytes = len(b"https://docs.test/1\n")
    poller = LocalFeedPoller(_FakeCrawler(CHANNEL_TEXT), max_feed_bytes=max_bytes)
    entries = poller.poll(FEED_URL, kind="channel", params={}, plan=FetchPlan())
    assert _urls(entries) == ["https://docs.test/1"]


def test_local_poll_since_filters_older_entries() -> None:
    poller = LocalFeedPoller(_FakeCrawler(RSS))
    entries = poller.poll(
        FEED_URL,
        kind="feed",
        params={"since": "2026-10-08T00:00:00Z"},
        plan=FetchPlan(),
    )
    # A (01.10) отброшен; B (10.10) сохранён; C без даты — консервативно сохранён.
    assert _urls(entries) == ["https://docs.test/b", "https://docs.test/c"]


# ---------------------------------------------------------------------------
# LocalFeedPoller: SSRF
# ---------------------------------------------------------------------------


def test_local_poll_rejects_non_public_feed_url() -> None:
    poller = LocalFeedPoller(_FakeCrawler(RSS))
    with pytest.raises(ValueError, match="приватн|SSRF|metadata"):
        poller.poll("http://127.0.0.1/feed", kind="feed", params={}, plan=FetchPlan())


def test_local_poll_rejects_metadata_feed_url() -> None:
    poller = LocalFeedPoller(_FakeCrawler(RSS))
    with pytest.raises(ValueError):
        poller.poll(
            "http://169.254.169.254/latest/meta-data", kind="feed", params={}, plan=FetchPlan()
        )


def test_local_poll_drops_unsafe_entry_urls() -> None:
    rss = (
        b"<?xml version=\"1.0\"?><rss version=\"2.0\"><channel>"
        b"<item><link>https://docs.test/ok</link></item>"
        b"<item><link>http://127.0.0.1/secret</link></item>"
        b"</channel></rss>"
    )
    poller = LocalFeedPoller(_FakeCrawler(rss))
    entries = poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())
    assert _urls(entries) == ["https://docs.test/ok"]


def test_local_poll_bad_feed_raises_parse_error() -> None:
    poller = LocalFeedPoller(_FakeCrawler(b"<not-a-feed/>"))
    with pytest.raises(FeedParseError):
        poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())


def test_local_validate_params_ok() -> None:
    assert LocalFeedPoller(_FakeCrawler(RSS)).validate_params("feed", {"limit": 1}) == []


# ---------------------------------------------------------------------------
# RemoteFeedPoller
# ---------------------------------------------------------------------------


def _poll_response(entries: list[dict[str, Any]], *, count: int | None = None,
                   truncated: bool = False) -> dict[str, Any]:
    return {
        "entries": entries,
        "count": len(entries) if count is None else count,
        "truncated": truncated,
    }


def test_remote_poll_builds_exact_payload_and_parses() -> None:
    gateway = _FakeGateway(
        _poll_response(
            [{"url": "https://docs.test/a", "guid": "g", "published_at": "2026-01-01", "title": "t"}]
        )
    )
    poller = RemoteFeedPoller(gateway)

    entries = poller.poll(
        FEED_URL, kind="feed", params={"limit": 5}, plan=FetchPlan()
    )

    assert entries == [
        FeedEntry(url="https://docs.test/a", guid="g", published_at="2026-01-01", title="t")
    ]
    op, payload = gateway.calls[-1]
    assert op == "crawl/poll"
    assert payload == {
        "url": FEED_URL,
        "kind": "feed",
        "params": {"limit": 5},
        "fetch": "http",
        "extract": "auto",
        "strategy": "auto",
        "wait_for": None,
    }


def test_remote_poll_serializes_plan_axes() -> None:
    from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode

    gateway = _FakeGateway(_poll_response([]))
    poller = RemoteFeedPoller(gateway)

    poller.poll(
        FEED_URL,
        kind="channel",
        params={},
        plan=FetchPlan(
            fetch=FetchMode.BROWSER,
            extract=ExtractionRoute.HTML,
            strategy=CrawlStrategy.HYBRID,
            wait_for="selector",
        ),
    )

    payload = gateway.calls[-1][1]
    assert payload["fetch"] == "browser"
    assert payload["extract"] == "html"
    assert payload["strategy"] == "hybrid"
    assert payload["wait_for"] == "selector"


def test_remote_poll_drops_unsafe_entry_urls() -> None:
    gateway = _FakeGateway(
        _poll_response(
            [
                {"url": "https://docs.test/ok"},
                {"url": "http://127.0.0.1/secret"},
                {"url": "http://169.254.169.254/latest/meta-data"},
            ]
        )
    )
    poller = RemoteFeedPoller(gateway)
    entries = poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())
    assert _urls(entries) == ["https://docs.test/ok"]


def test_remote_poll_max_entries_truncates() -> None:
    gateway = _FakeGateway(
        _poll_response([{"url": f"https://docs.test/{i}"} for i in range(5)])
    )
    poller = RemoteFeedPoller(gateway, max_entries=2)
    entries = poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())
    assert len(entries) == 2


def test_remote_poll_rejects_non_public_feed_url() -> None:
    poller = RemoteFeedPoller(_FakeGateway(_poll_response([])))
    with pytest.raises(RemoteContentError, match="небезопасный url"):
        poller.poll("http://127.0.0.1/feed", kind="feed", params={}, plan=FetchPlan())


def test_remote_poll_non_mapping_response_raises() -> None:
    poller = RemoteFeedPoller(_FakeGateway(["not", "a", "mapping"]))
    with pytest.raises(RemoteContentError):
        poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())


def test_remote_poll_entries_must_be_list() -> None:
    poller = RemoteFeedPoller(_FakeGateway({"entries": "nope", "count": 0, "truncated": False}))
    with pytest.raises(RemoteContentError, match="entries"):
        poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())


def test_remote_poll_count_must_be_int() -> None:
    poller = RemoteFeedPoller(
        _FakeGateway({"entries": [], "count": "many", "truncated": False})
    )
    with pytest.raises(RemoteContentError, match="count"):
        poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())


def test_remote_poll_truncated_must_be_bool() -> None:
    poller = RemoteFeedPoller(
        _FakeGateway({"entries": [], "count": 0, "truncated": "yes"})
    )
    with pytest.raises(RemoteContentError, match="truncated"):
        poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())


def test_remote_poll_malformed_entry_raises() -> None:
    poller = RemoteFeedPoller(_FakeGateway(_poll_response([{"guid": "no-url"}])))
    with pytest.raises(RemoteContentError, match="url"):
        poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())


def test_remote_poll_entry_must_be_mapping() -> None:
    poller = RemoteFeedPoller(_FakeGateway(_poll_response(["just-a-string"])))  # type: ignore[list-item]
    with pytest.raises(RemoteContentError):
        poller.poll(FEED_URL, kind="feed", params={}, plan=FetchPlan())


def test_remote_validate_params_ok() -> None:
    assert RemoteFeedPoller(_FakeGateway(_poll_response([]))).validate_params(
        "channel", {"limit": 1}
    ) == []
