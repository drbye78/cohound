"""Тесты robots-энфорса в адаптерах (egress-слой) — D1/§10.

Без сети: политика ``RobotsGate`` инъектируется (текст ленты), бэкенд/фетчер — фейковые.
Проверяются отказ (``CrawlRefused``/``robots_disallow``), отказ по **недоступной** ленте
(``robots_unavailable``: RFC 9309 §2.3.1.4 — origin не обходится, и это отличимо от
«сайт запретил») и floor ``Crawl-delay`` (явный ``rate_limit`` важнее).
"""

from __future__ import annotations

from typing import Any

import pytest

from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.domain.models import RateLimitSpec
from core.ports.crawl_errors import CrawlNotFound, CrawlRefused
from core.ports.types import FetchPlan
from core.util.robots import (
    ROBOTS_UNAVAILABLE_REASON,
    RobotsFeedState,
    RobotsGate,
)

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


def test_http_crawler_refuses_when_robots_unavailable() -> None:
    """Недоступная лента — отказ с кодом ``robots_unavailable``, а не разрешение.

    Класс отказа ``CrawlRefused`` (политика), но причина отличается от ``Disallow``:
    сайт обход не запрещал — мы не смогли установить разрешение (RFC 9309 §2.3.1.4).
    """
    calls: list[str] = []
    crawler = HttpCrawler(
        fetcher=lambda url: calls.append(url) or b"x",
        robots=RobotsGate(lambda _host: None),
    )
    with pytest.raises(CrawlRefused) as excinfo:
        crawler.fetch("https://example.com/ok", FetchPlan(robots=True))
    assert excinfo.value.refusal_reason == ROBOTS_UNAVAILABLE_REASON
    assert "unavailable" in str(excinfo.value)
    assert calls == []  # отказ до сетевого шага: страница не запрашивалась


def test_http_crawler_allows_when_robots_absent() -> None:
    """``404`` ленты — наблюдение «правил нет»: обход разрешён."""
    crawler = HttpCrawler(
        fetcher=lambda _url: b"ok",
        robots=RobotsGate(
            lambda _host: _raise(CrawlNotFound("HTTP 404", http_status=404))
        ),
    )
    assert crawler.fetch("https://example.com/ok", FetchPlan(robots=True)).content == b"ok"


def _raise(exc: BaseException) -> str:
    """Вспомогательный фетчер: поднимает исключение (``not_found`` ленты)."""
    raise exc


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


def test_browser_crawler_refuses_when_robots_unavailable() -> None:
    """Ось действует и на браузерном пути: недоступная лента → отказ ``robots_unavailable``."""
    backend = _Backend()
    crawler = BrowserCrawler(backend=backend, robots=RobotsGate(lambda _host: None))
    with pytest.raises(CrawlRefused) as excinfo:
        crawler.fetch("https://example.com/page", FetchPlan(robots=True))
    assert excinfo.value.refusal_reason == ROBOTS_UNAVAILABLE_REASON
    assert backend.opened == 0  # браузер не поднимался: отказ до рендера


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


def test_fetcher_that_swallows_404_erases_the_observation() -> None:
    """Проглоченный ``404`` превращается в ``unavailable`` — и origin перестаёт обходиться.

    Это контрактный тест на фетч egress-слоя: «ленты нет» (``CrawlNotFound``) обязан
    **доехать** до гейта. Фетчер, который ловит все исключения и возвращает ``None``,
    стирает различие между «origin ответил 404» и «прочитать не удалось», и второй случай
    по RFC 9309 §2.3.1.4 запрещает обход. Поэтому фетчи ``cli/wiring.py``,
    ``edge/crawler_factory.py`` и ``cli/package/commands.py`` обязаны пропускать
    ``CrawlNotFound`` (иначе хост с ``404``-лентой выключается целиком).
    """

    def swallowing(_host: str) -> str | None:
        try:
            raise CrawlNotFound("HTTP 404", http_status=404)
        except Exception:  # noqa: BLE001 — именно так устроены сегодняшние фетчи
            return None

    verdict = RobotsGate(swallowing).verdict("https://example.com/page")

    assert verdict.state is RobotsFeedState.UNAVAILABLE
    assert verdict.allowed is False
