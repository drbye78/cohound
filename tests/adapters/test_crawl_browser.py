"""Юнит-тесты браузерного краулера ``core/adapters/crawl_browser.py`` (P2, Ш5).

Без сети и без реального браузера: внедряются фейковый ``BrowserBackend``/``BrowserSession``
и фейковый ``http_get``; кэш — ``tmp_path``. Проверяются honest/hybrid-семантику,
cookie-мост, родовую детекцию challenge (по статусу), bounded retry + fallback на honest,
запись/недозапись кэша, SSRF-отказ, ``available``/ленивый playwright и идемпотентный
``close``. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

from core.adapters.crawl_browser import (
    CHALLENGE_STATUSES,
    HYBRID_MAX_ATTEMPTS,
    BrowserCrawler,
    CrawlUnavailable,
)
from core.adapters.crawl_scrapy import CrawlError, CrawlUrlError
from core.domain.enums import CrawlStrategy
from core.domain.models import RateLimitSpec
from core.ports.crawler import CrawlerPort
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.cache import ContentAddressedCache, http_key
from core.util.rate_limit import RateLimiter

URL = "https://example.com/page"
#: Пост-редиректный URL, который отдаёт браузер (``page.url`` после ``goto``).
FINAL_URL = "https://example.com/redirected"
COOKIE = {"name": "session", "value": "abc", "domain": "example.com", "path": "/"}


# ---------------------------------------------------------------------------
# Фейковые швы
# ---------------------------------------------------------------------------


class FakeSession:
    """Фейковая ``BrowserSession``: рендер/cookie из конструктора, запись вызовов.

    ``final_url`` моделирует пост-редиректный ``page.url``: ``None`` → финальный URL равен
    запрошенному; заданная строка (в т.ч. пустая) — отдаётся как есть.
    """

    def __init__(
        self,
        *,
        content: bytes = b"<html>rendered</html>",
        cookies: list[dict[str, Any]] | None = None,
        navigate_error: Exception | None = None,
        final_url: str | None = None,
    ) -> None:
        self.content = content
        self.cookie_list = list(cookies or [])
        self.navigate_error = navigate_error
        self.final_url = final_url
        self.navigated: list[tuple[str, str | None]] = []
        self.cookies_asked: list[str] = []
        self.closed = False

    def navigate(self, url: str, wait_for: str | None = None) -> tuple[str, bytes]:
        self.navigated.append((url, wait_for))
        if self.navigate_error is not None:
            raise self.navigate_error
        return (self.final_url if self.final_url is not None else url), self.content

    def cookies(self, url: str) -> list[dict[str, Any]]:
        self.cookies_asked.append(url)
        return list(self.cookie_list)

    def close(self) -> None:
        self.closed = True


class FakeBackend:
    """Фейковый ``BrowserBackend``: выдаёт ``FakeSession``, считает close."""

    def __init__(
        self,
        *,
        content: bytes = b"<html>rendered</html>",
        cookies: list[dict[str, Any]] | None = None,
        navigate_error: Exception | None = None,
        final_url: str | None = None,
    ) -> None:
        self.content = content
        self.cookies = cookies
        self.navigate_error = navigate_error
        self.final_url = final_url
        self.sessions: list[FakeSession] = []
        self.close_calls = 0

    def open(self) -> FakeSession:
        session = FakeSession(
            content=self.content,
            cookies=self.cookies,
            navigate_error=self.navigate_error,
            final_url=self.final_url,
        )
        self.sessions.append(session)
        return session

    def close(self) -> None:
        self.close_calls += 1


class RecordingHttpGet:
    """Фейковый ``http_get`` ``(url, headers, cookies) -> (status, body, headers)``."""

    def __init__(
        self,
        responses: list[tuple[int, bytes, dict[str, str]]] | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.responses = list(responses or [(200, b"http-body", {"Content-Type": "text/html"})])
        self.error = error
        self.calls: list[tuple[str, dict[str, str], list[dict[str, Any]]]] = []

    def __call__(
        self, url: str, headers: dict[str, str], cookies: list[dict[str, Any]]
    ) -> tuple[int, bytes, dict[str, str]]:
        self.calls.append((url, dict(headers), list(cookies)))
        if self.error is not None:
            raise self.error
        index = min(len(self.calls) - 1, len(self.responses) - 1)
        return self.responses[index]


class RecordingLimiter(RateLimiter):
    """Фейковый ``rate_limiter`` (подкласс контракта): пишет ``(host, spec)``, не спит."""

    def __init__(self) -> None:
        super().__init__(sleep=lambda _seconds: None)
        self.calls: list[tuple[str, RateLimitSpec | None]] = []

    def wait(self, host: str, spec: RateLimitSpec | None) -> float:
        self.calls.append((host, spec))
        return 0.0


class _FakeHttpxResponse:
    """Мини-дублёр ответа ``httpx`` для redirect-loop без сети."""

    def __init__(
        self, status_code: int, headers: dict[str, str], content: bytes = b""
    ) -> None:
        self.status_code = status_code
        self.headers = headers
        self.content = content


class _FakeHttpxModule:
    """Мини-дублёр модуля ``httpx``: отдаёт заготовленные ответы по порядку."""

    def __init__(self, responses: list[_FakeHttpxResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> _FakeHttpxResponse:
        self.calls.append(url)
        index = min(len(self.calls) - 1, len(self._responses) - 1)
        return self._responses[index]


def _plan(
    strategy: CrawlStrategy = CrawlStrategy.HONEST,
    fetch: FetchMode = FetchMode.BROWSER,
) -> FetchPlan:
    return FetchPlan(fetch=fetch, strategy=strategy)


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backoff не спит в тестах: ``jittered_backoff`` → 0."""
    import core.adapters.crawl_browser as mod

    monkeypatch.setattr(mod, "jittered_backoff", lambda *args, **kwargs: 0.0)


# ---------------------------------------------------------------------------
# honest
# ---------------------------------------------------------------------------


def test_honest_navigates_and_returns_browser_page(tmp_path: Path) -> None:
    backend = FakeBackend(content=b"<html>hello</html>", final_url=FINAL_URL)
    cache = ContentAddressedCache(tmp_path)
    crawler = BrowserCrawler(backend=backend, cache=cache)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HONEST))

    assert isinstance(page, RawPage)
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"<html>hello</html>"
    assert page.fetched_at
    # A3: в RawPage.url едет ФИНАЛЬНЫЙ (пост-редиректный) URL браузера, не запрошенный.
    assert page.url == FINAL_URL
    assert backend.sessions[0].navigated == [(URL, None)]
    assert backend.sessions[0].closed is True
    # honest-рендер НЕ кэшируется (R7).
    assert cache.get_bytes("http", http_key(URL)) is None


def test_honest_missing_final_url_falls_back_to_requested() -> None:
    # Финальный URL недоступен (пустая строка) ⇒ откат на запрошенный (никогда ``""``).
    backend = FakeBackend(content=b"<html>x</html>", final_url="")
    crawler = BrowserCrawler(backend=backend)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HONEST))

    assert page.url == URL
    assert page.content == b"<html>x</html>"


def test_auto_strategy_dispatches_to_honest() -> None:
    backend = FakeBackend(content=b"auto-bytes")
    crawler = BrowserCrawler(backend=backend)
    page = crawler.fetch(URL, _plan(CrawlStrategy.AUTO))
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"auto-bytes"


def test_default_plan_strategy_is_auto_and_honest() -> None:
    backend = FakeBackend(content=b"default-bytes")
    crawler = BrowserCrawler(backend=backend)
    page = crawler.fetch(URL)  # FetchPlan(): fetch=HTTP, strategy=AUTO
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"default-bytes"


def test_plan_fetch_http_still_renders_browser() -> None:
    # fetch=HTTP адресуется HttpCrawler; для браузерного адаптера это AUTO→honest
    # (см. докстринг модуля) — явная, задокументированная семантика, не подмена.
    backend = FakeBackend(content=b"rendered-despite-fetch-http")
    crawler = BrowserCrawler(backend=backend)
    page = crawler.fetch(URL, _plan(CrawlStrategy.AUTO, fetch=FetchMode.HTTP))
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"rendered-despite-fetch-http"


def test_honest_render_cannot_supply_validators() -> None:
    # Честный Playwright-рендер не отдаёт заголовки ответа: валидаторов нет,
    # not_modified не заявляется (страница всегда пере-фетчивается целиком).
    backend = FakeBackend(content=b"<html>honest</html>")
    crawler = BrowserCrawler(backend=backend)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HONEST))

    assert page.content == b"<html>honest</html>"
    assert page.etag is None
    assert page.last_modified is None
    assert page.not_modified is False


# ---------------------------------------------------------------------------
# hybrid
# ---------------------------------------------------------------------------


def test_hybrid_bridges_cookies_and_returns_http_page(tmp_path: Path) -> None:
    backend = FakeBackend(content=b"<html>", cookies=[COOKIE])
    getter = RecordingHttpGet(
        [
            (
                200,
                b"hybrid-body",
                {
                    "Content-Type": "text/html; charset=utf-8",
                    "ETag": '"v1"',
                    "Last-Modified": "Wed, 21 Oct 2026 07:28:00 GMT",
                },
            )
        ]
    )
    cache = ContentAddressedCache(tmp_path)
    crawler = BrowserCrawler(backend=backend, http_get=getter, cache=cache)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert page.fetch_mode is FetchMode.HTTP
    assert page.content == b"hybrid-body"
    assert page.status_code == 200
    assert page.content_type == "text/html; charset=utf-8"
    # Hybrid-GET видит заголовки ответа ⇒ валидаторы сохраняются (в отличие от honest).
    assert page.etag == '"v1"'
    assert page.last_modified == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert page.not_modified is False
    # cookie-мост: cookie из сессии доехали в http_get.
    (_, headers, cookies) = getter.calls[0]
    assert cookies == [COOKIE]
    assert "User-Agent" in headers
    assert "Accept-Language" in headers
    # успешный hybrid-ответ закэширован по http_key(url); сайдкар несёт валидаторы.
    assert cache.get_bytes("http", http_key(URL)) == b"hybrid-body"
    meta = cache.get_meta("http", http_key(URL))
    assert meta is not None
    assert meta["etag"] == '"v1"'
    assert meta["last_modified"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert meta["status_code"] == 200


def test_hybrid_success_reports_browser_final_url() -> None:
    # A3: успех hybrid — в RawPage.url идёт финальный (пост-редиректный) URL прогрева.
    backend = FakeBackend(content=b"<html>", cookies=[COOKIE], final_url=FINAL_URL)
    getter = RecordingHttpGet([(200, b"http-body", {"Content-Type": "text/html"})])
    crawler = BrowserCrawler(backend=backend, http_get=getter)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert page.fetch_mode is FetchMode.HTTP
    assert page.url == FINAL_URL


def test_hybrid_fallback_reports_browser_final_url() -> None:
    # A3: fallback (challenge не снят) возвращает рендер — URL также финальный.
    backend = FakeBackend(content=b"<html>render</html>", cookies=[COOKIE], final_url=FINAL_URL)
    getter = RecordingHttpGet([(403, b"denied", {})])
    crawler = BrowserCrawler(backend=backend, http_get=getter)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert page.fetch_mode is FetchMode.BROWSER
    assert page.url == FINAL_URL


def test_hybrid_warm_uses_browser_session() -> None:
    backend = FakeBackend(content=b"<html>", cookies=[COOKIE])
    getter = RecordingHttpGet()
    crawler = BrowserCrawler(backend=backend, http_get=getter)
    crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))
    # прогрев: navigate + cookies на сессии, затем закрытие.
    assert backend.sessions[0].navigated == [(URL, None)]
    assert backend.sessions[0].cookies_asked == [URL]
    assert backend.sessions[0].closed is True


def test_hybrid_cache_hit_skips_browser_and_http(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", http_key(URL), b"cached-hybrid")
    backend = FakeBackend()
    getter = RecordingHttpGet()
    crawler = BrowserCrawler(backend=backend, http_get=getter, cache=cache)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert page.content == b"cached-hybrid"
    assert page.fetch_mode is FetchMode.HTTP
    assert getter.calls == []
    assert backend.sessions == []


def test_hybrid_challenge_retries_bounded_then_returns_render() -> None:
    # challenge не снят за bounded-число попыток → возвращаем уже полученный рендер.
    backend = FakeBackend(content=b"<html>challenge-render</html>", cookies=[COOKIE])
    getter = RecordingHttpGet([(403, b"denied", {})])
    crawler = BrowserCrawler(backend=backend, http_get=getter)

    page = crawler._fetch_hybrid(URL, _plan(CrawlStrategy.HYBRID))

    assert len(getter.calls) == HYBRID_MAX_ATTEMPTS  # bounded
    assert 403 in CHALLENGE_STATUSES
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"<html>challenge-render</html>"


def test_hybrid_5xx_not_cached_returns_render(tmp_path: Path) -> None:
    # M1: 5xx — не успех; кэш не пишется, возвращается браузерный рендер.
    backend = FakeBackend(content=b"<html>render-500</html>", cookies=[COOKIE])
    getter = RecordingHttpGet([(500, b"server error", {})])
    cache = ContentAddressedCache(tmp_path)
    crawler = BrowserCrawler(backend=backend, http_get=getter, cache=cache)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert len(getter.calls) == HYBRID_MAX_ATTEMPTS
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"<html>render-500</html>"
    assert cache.get_bytes("http", http_key(URL)) is None  # нет ложного кэш-контента


def test_hybrid_interstitial_not_cached_returns_render(tmp_path: Path) -> None:
    # M2: 200 + мелкое тело против большого рендера ⇒ interstitial ⇒ не кэшируем, рендер.
    big_render = b"<html>" + b"x" * 2000 + b"</html>"
    backend = FakeBackend(content=big_render, cookies=[COOKIE])
    getter = RecordingHttpGet([(200, b"<html>stub</html>", {"Content-Type": "text/html"})])
    cache = ContentAddressedCache(tmp_path)
    crawler = BrowserCrawler(backend=backend, http_get=getter, cache=cache)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert len(getter.calls) == HYBRID_MAX_ATTEMPTS
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == big_render
    assert cache.get_bytes("http", http_key(URL)) is None


def test_hybrid_plain_2xx_cached(tmp_path: Path) -> None:
    # Обычный 2xx: рендер мелкий (< порога) ⇒ гейт не применяется ⇒ HTTP + кэш.
    backend = FakeBackend(content=b"<html>", cookies=[COOKIE])
    getter = RecordingHttpGet([(200, b"valid-http-content", {"Content-Type": "text/html"})])
    cache = ContentAddressedCache(tmp_path)
    crawler = BrowserCrawler(backend=backend, http_get=getter, cache=cache)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert page.fetch_mode is FetchMode.HTTP
    assert page.content == b"valid-http-content"
    assert len(getter.calls) == 1
    assert cache.get_bytes("http", http_key(URL)) == b"valid-http-content"


def test_hybrid_empty_render_raises_after_exhaustion() -> None:
    # Рендер пуст ⇒ нечего возвращать ⇒ CrawlError (fetch уйдёт в honest).
    backend = FakeBackend(content=b"", cookies=[COOKIE])
    getter = RecordingHttpGet([(503, b"", {})])
    crawler = BrowserCrawler(backend=backend, http_get=getter)

    with pytest.raises(CrawlError):
        crawler._fetch_hybrid(URL, _plan(CrawlStrategy.HYBRID))


def test_honest_wait_for_reaches_navigate() -> None:
    backend = FakeBackend(content=b"<html>wf</html>")
    crawler = BrowserCrawler(backend=backend)
    plan = FetchPlan(FetchMode.BROWSER, strategy=CrawlStrategy.HONEST, wait_for="() => true")

    crawler.fetch(URL, plan)

    assert backend.sessions[0].navigated == [(URL, "() => true")]


def test_hybrid_wait_for_reaches_warm_navigate() -> None:
    backend = FakeBackend(content=b"<html>", cookies=[COOKIE])
    getter = RecordingHttpGet([(200, b"body", {"Content-Type": "text/html"})])
    crawler = BrowserCrawler(backend=backend, http_get=getter)
    plan = FetchPlan(FetchMode.BROWSER, strategy=CrawlStrategy.HYBRID, wait_for="() => ready")

    crawler.fetch(URL, plan)

    assert backend.sessions[0].navigated == [(URL, "() => ready")]


def test_hybrid_failure_falls_back_to_honest() -> None:
    backend = FakeBackend(content=b"<html>honest-fallback</html>", cookies=[COOKIE])
    getter = RecordingHttpGet([(403, b"denied", {})])
    crawler = BrowserCrawler(backend=backend, http_get=getter)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    # hybrid не снял challenge → honest-рендер.
    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"<html>honest-fallback</html>"


def test_hybrid_challenge_not_cached(tmp_path: Path) -> None:
    backend = FakeBackend(content=b"<html>", cookies=[COOKIE])
    getter = RecordingHttpGet([(403, b"denied", {})])
    cache = ContentAddressedCache(tmp_path)
    crawler = BrowserCrawler(backend=backend, http_get=getter, cache=cache)

    crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))  # fallback honest

    assert cache.get_bytes("http", http_key(URL)) is None


def test_hybrid_transport_error_falls_back_to_honest() -> None:
    backend = FakeBackend(content=b"<html>fallback-on-error</html>", cookies=[COOKIE])
    getter = RecordingHttpGet(error=CrawlError("transport down"))
    crawler = BrowserCrawler(backend=backend, http_get=getter)

    page = crawler.fetch(URL, _plan(CrawlStrategy.HYBRID))

    assert page.fetch_mode is FetchMode.BROWSER
    assert page.content == b"<html>fallback-on-error</html>"


# ---------------------------------------------------------------------------
# SSRF / доступность / жизненный цикл / контракт
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile",
    ["http://127.0.0.1/", "http://169.254.169.254/latest/meta-data/", "ftp://example.com/x"],
)
def test_ssrf_url_rejected_before_fetch(hostile: str) -> None:
    backend = FakeBackend()
    crawler = BrowserCrawler(backend=backend)
    with pytest.raises(CrawlUrlError):
        crawler.fetch(hostile)
    assert backend.sessions == []
    assert issubclass(CrawlUrlError, CrawlError)
    assert issubclass(CrawlUrlError, ValueError)


def test_default_http_get_rejects_ssrf_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    # SSRF через редирект: Location на приватный хост обязан быть отклонён.
    fake = _FakeHttpxModule(
        [_FakeHttpxResponse(302, {"location": "http://127.0.0.1/internal"})]
    )
    crawler = BrowserCrawler(backend=FakeBackend())
    monkeypatch.setattr(crawler, "_import_http", lambda: fake)

    with pytest.raises(CrawlUrlError):
        crawler._default_http_get(URL, {}, [])
    assert fake.calls == [URL]  # до небезопасного хопа не дошли


def test_default_http_get_follows_safe_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpxModule(
        [
            _FakeHttpxResponse(302, {"location": "https://example.com/final"}),
            _FakeHttpxResponse(200, {"Content-Type": "text/html"}, b"final-body"),
        ]
    )
    crawler = BrowserCrawler(backend=FakeBackend())
    monkeypatch.setattr(crawler, "_import_http", lambda: fake)

    status, body, headers = crawler._default_http_get(URL, {}, [])

    assert status == 200
    assert body == b"final-body"
    assert headers["Content-Type"] == "text/html"
    assert fake.calls == [URL, "https://example.com/final"]


def test_default_http_get_too_many_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpxModule(
        [_FakeHttpxResponse(302, {"location": "https://example.com/loop"})]
    )
    crawler = BrowserCrawler(backend=FakeBackend())
    monkeypatch.setattr(crawler, "_import_http", lambda: fake)

    with pytest.raises(CrawlError, match="редирект"):
        crawler._default_http_get(URL, {}, [])


def test_available_true_with_injected_backend() -> None:
    assert BrowserCrawler(backend=FakeBackend()).available is True


def test_rate_limiter_called_before_fetch() -> None:
    limiter = RecordingLimiter()
    backend = FakeBackend(content=b"x")
    spec = RateLimitSpec(min_s=1.0, max_s=2.0)
    crawler = BrowserCrawler(backend=backend, rate_limiter=limiter)

    crawler.fetch(URL, FetchPlan(fetch=FetchMode.BROWSER, strategy=CrawlStrategy.HONEST, rate_limit=spec))

    assert limiter.calls == [("example.com", spec)]


def test_rate_limiter_none_spec_does_not_sleep() -> None:
    slept: list[float] = []
    limiter = RateLimiter(sleep=slept.append)
    crawler = BrowserCrawler(backend=FakeBackend(content=b"x"), rate_limiter=limiter)

    crawler.fetch(URL, _plan())  # plan.rate_limit is None → без сна

    assert slept == []


def test_close_is_idempotent() -> None:
    backend = FakeBackend()
    crawler = BrowserCrawler(backend=backend)
    crawler.close()
    crawler.close()
    assert backend.close_calls == 1


def test_close_safe_when_never_opened() -> None:
    crawler = BrowserCrawler(backend=FakeBackend())
    crawler.close()  # не должно падать


def test_fetch_after_close_raises_and_does_not_reopen() -> None:
    backend = FakeBackend()
    crawler = BrowserCrawler(backend=backend)
    crawler.close()
    assert backend.close_calls == 1

    with pytest.raises(CrawlError):
        crawler.fetch(URL)
    # Гард жизненного цикла: движок повторно не поднимается.
    assert backend.sessions == []
    assert backend.close_calls == 1


def test_module_importable_without_playwright() -> None:
    import core.adapters.crawl_browser as mod

    # Ленивый playwright: пакет не в пространстве имён модуля после импорта.
    assert "playwright" not in getattr(mod, "__dict__", {})
    assert "sync_playwright" not in getattr(mod, "__dict__", {})


def test_available_matches_playwright_environment() -> None:
    import core.adapters.crawl_browser as mod

    assert BrowserCrawler().available is mod._playwright_available()


def test_real_path_without_playwright_raises_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import core.adapters.crawl_browser as mod

    monkeypatch.setattr(mod, "_playwright_available", lambda: False)
    crawler = BrowserCrawler()  # без backend → строится реальный драйвер
    with pytest.raises(CrawlUnavailable):
        crawler.fetch(URL)
    assert issubclass(CrawlUnavailable, CrawlError)


# ---------------------------------------------------------------------------
# Запуск браузера: automation-сигнал (persistent) vs CDP (P4)
# ---------------------------------------------------------------------------


class _FakeContext:
    """Мини-дублёр persistent-context: нужен только ``close``."""

    def close(self) -> None:
        pass


class _FakeBrowser:
    """Мини-дублёр CDP-браузера: ``contexts``/``new_context``/``close``."""

    def __init__(self) -> None:
        self.contexts: list[Any] = []

    def new_context(self) -> _FakeContext:
        return _FakeContext()

    def close(self) -> None:
        pass


class _FakeChromium:
    """Перехватывает kwargs запуска обоих взаимоисключающих путей backend."""

    def __init__(self) -> None:
        self.persistent_calls: list[dict[str, Any]] = []
        self.cdp_calls: list[str] = []

    def launch_persistent_context(self, **kwargs: Any) -> _FakeContext:
        self.persistent_calls.append(kwargs)
        return _FakeContext()

    def connect_over_cdp(self, url: str) -> _FakeBrowser:
        self.cdp_calls.append(url)
        return _FakeBrowser()


class _FakeEngine:
    def __init__(self, chromium: _FakeChromium) -> None:
        self.chromium = chromium

    def stop(self) -> None:
        pass


class _FakeSyncPlaywright:
    def __init__(self, chromium: _FakeChromium) -> None:
        self._chromium = chromium

    def start(self) -> _FakeEngine:
        return _FakeEngine(self._chromium)


@pytest.fixture()
def fake_chromium(monkeypatch: pytest.MonkeyPatch) -> _FakeChromium:
    """Подменяет ``playwright`` лёгким фейком; возвращает перехватчик kwargs запуска.

    ``_playwright_available`` форсируется в ``True``, а ``playwright``/``playwright.sync_api``
    кладутся в ``sys.modules`` — реальный браузер и сеть не задействуются.
    """
    import core.adapters.crawl_browser as mod

    chromium = _FakeChromium()
    sync_api = types.ModuleType("playwright.sync_api")
    sync_api.sync_playwright = lambda: _FakeSyncPlaywright(chromium)  # type: ignore[attr-defined]
    package = types.ModuleType("playwright")
    package.sync_api = sync_api  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright", package)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)
    monkeypatch.setattr(mod, "_playwright_available", lambda: True)
    return chromium


def test_persistent_launch_neutralizes_automation_flag(fake_chromium: _FakeChromium) -> None:
    """persistent-ветка: ``args`` снимает automation, ``ignore_default_args`` убирает дефолт."""
    import core.adapters.crawl_browser as mod

    backend = mod._PlaywrightBackend(
        cdp_url=None, user_data_dir=None, headless=False, channel=None, timeout_ms=1000,
    )
    try:
        backend._ensure_context()
    finally:
        backend.close()

    assert len(fake_chromium.persistent_calls) == 1
    kwargs = fake_chromium.persistent_calls[0]
    assert kwargs["args"] == ["--disable-blink-features=AutomationControlled"]
    assert kwargs["ignore_default_args"] == ["--enable-automation"]
    # Единый источник: kwargs совпадают с модульными константами.
    assert kwargs["args"] == list(mod._LAUNCH_ARGS)
    assert kwargs["ignore_default_args"] == list(mod._IGNORE_DEFAULT_ARGS)
    # Политика дефолтов не тронута: headed (``headless=False``) пробрасывается как есть.
    assert kwargs["headless"] is False
    assert kwargs["user_data_dir"]  # временный профиль создан


def test_cdp_branch_connects_without_persistent_launch(
    fake_chromium: _FakeChromium,
) -> None:
    """CDP-ветка: ``connect_over_cdp``, persistent-context (и его флаги) не создаётся."""
    import core.adapters.crawl_browser as mod

    backend = mod._PlaywrightBackend(
        cdp_url="http://127.0.0.1:9222", user_data_dir=None, headless=False,
        channel=None, timeout_ms=1000,
    )
    try:
        backend._ensure_context()
    finally:
        backend.close()

    assert fake_chromium.cdp_calls == ["http://127.0.0.1:9222"]
    assert fake_chromium.persistent_calls == []


def test_crawler_real_backend_uses_launch_flags(fake_chromium: _FakeChromium) -> None:
    """Сквозь ``BrowserCrawler`` реальный backend получает те же launch-флаги (P4)."""
    import core.adapters.crawl_browser as mod  # noqa: F401

    crawler = BrowserCrawler()  # без backend → ленивый реальный Playwright
    try:
        backend: Any = crawler._ensure_backend()
        backend._ensure_context()
    finally:
        crawler.close()

    assert len(fake_chromium.persistent_calls) == 1
    kwargs = fake_chromium.persistent_calls[0]
    assert kwargs["args"] == ["--disable-blink-features=AutomationControlled"]
    assert kwargs["ignore_default_args"] == ["--enable-automation"]
