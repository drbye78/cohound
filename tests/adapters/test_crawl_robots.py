"""Тесты robots-энфорса в адаптерах (egress-слой) — D1/§10.

Без сети: политика ``RobotsGate`` инъектируется (текст ленты), бэкенд/фетчер — фейковые.
Проверяются отказ (``CrawlRefused``/``robots_disallow``) и floor ``Crawl-delay`` (явный
``rate_limit`` важнее).
"""

from __future__ import annotations

from typing import Any

import pytest

from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import CrawlRefused
from core.domain.models import RateLimitSpec
from core.ports.types import FetchPlan
from core.util.robots import RobotsGate

DISALLOW = "User-agent: *\nDisallow: /blocked\n"
ALLOW_WITH_DELAY = "User-agent: *\nCrawl-delay: 3\n"


class _Backend:
    """Минимальный фейковый ``BrowserBackend`` (без реального браузера)."""

    def __init__(self) -> None:
        self.opened = 0

    def open(self) -> Any:
        self.opened += 1
        return _Session()

    def close(self) -> None:
        pass


class _Session:
    def navigate(self, url: str, wait_for: str | None = None) -> bytes:
        return b"<html>ok</html>"

    def cookies(self, url: str) -> list[dict[str, Any]]:
        return []

    def close(self) -> None:
        pass


class _Limiter:
    def __init__(self) -> None:
        self.waits: list[tuple[str, RateLimitSpec | None]] = []

    def wait(self, host: str, spec: RateLimitSpec | None) -> None:
        self.waits.append((host, spec))


def test_http_crawler_refuses_robots_disallow_without_fetching() -> None:
    calls: list[str] = []
    crawler = HttpCrawler(
        fetcher=lambda url: calls.append(url) or b"x",
        robots=RobotsGate(lambda _host: DISALLOW),
    )
    with pytest.raises(CrawlRefused) as excinfo:
        crawler.fetch("https://example.com/blocked", FetchPlan(robots=True))
    assert excinfo.value.refusal_reason == "robots_disallow"
    assert calls == []  # отказ до сетевого шага


def test_http_crawler_allows_when_robots_unavailable() -> None:
    crawler = HttpCrawler(
        fetcher=lambda _url: b"ok", robots=RobotsGate(lambda _host: None)
    )
    assert crawler.fetch("https://example.com/ok", FetchPlan(robots=True)).content == b"ok"


def test_http_crawler_skips_robots_when_plan_robots_off() -> None:
    """§6b: политика внедрена, но ``plan.robots`` ложно ⇒ robots не трогается вовсе."""
    gate_calls: list[str] = []
    fetched: list[str] = []

    def _feed(host: str) -> str:
        gate_calls.append(host)
        return DISALLOW

    crawler = HttpCrawler(
        fetcher=lambda url: fetched.append(url) or b"ok",
        robots=RobotsGate(_feed),
    )
    page = crawler.fetch("https://example.com/blocked")  # plan.robots по умолчанию False
    assert page.content == b"ok"
    assert fetched == ["https://example.com/blocked"]
    assert gate_calls == []  # robots.txt даже не запрошен (нет лишнего fetch)


def test_browser_crawler_refuses_robots_disallow() -> None:
    crawler = BrowserCrawler(
        backend=_Backend(), robots=RobotsGate(lambda _host: DISALLOW)
    )
    with pytest.raises(CrawlRefused) as excinfo:
        crawler.fetch("https://example.com/blocked", FetchPlan(robots=True))
    assert excinfo.value.refusal_reason == "robots_disallow"


def test_browser_crawler_skips_robots_when_plan_robots_off() -> None:
    """§6b: ``plan.robots`` ложно ⇒ robots-гейт не применяется (страница рендерится)."""
    backend = _Backend()
    crawler = BrowserCrawler(backend=backend, robots=RobotsGate(lambda _host: DISALLOW))
    page = crawler.fetch("https://example.com/blocked")  # plan.robots по умолчанию False
    assert page.content == b"<html>ok</html>"
    assert backend.opened == 1


def test_browser_crawl_delay_floor_used_without_explicit_rate_limit() -> None:
    limiter = _Limiter()
    crawler = BrowserCrawler(
        backend=_Backend(),
        rate_limiter=limiter,  # type: ignore[arg-type]
        robots=RobotsGate(lambda _host: ALLOW_WITH_DELAY),
    )
    crawler.fetch("https://example.com/page")
    assert limiter.waits
    _, spec = limiter.waits[0]
    assert spec is not None and spec.min_s == 3.0 and spec.max_s == 3.0


def test_browser_explicit_rate_limit_wins_over_crawl_delay() -> None:
    limiter = _Limiter()
    crawler = BrowserCrawler(
        backend=_Backend(),
        rate_limiter=limiter,  # type: ignore[arg-type]
        robots=RobotsGate(lambda _host: ALLOW_WITH_DELAY),
    )
    explicit = RateLimitSpec(min_s=0.5, max_s=0.5)
    crawler.fetch("https://example.com/page", FetchPlan(rate_limit=explicit))
    assert limiter.waits[0][1] == explicit
