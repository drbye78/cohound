"""Фейковые краулеры/швы для тестов зонда осей краула (``package crawl-probe``).

Без сети и без реального браузера: ``ScriptedHttpCrawler``/``ScriptedBrowserCrawler``
отдают заранее заданные ``RawPage``/исключения по URL (браузерный — по ``plan.strategy``),
а ``FakeBackend``/``FakeSession``/``RecordingHttpGet`` (промотированы из
``tests/adapters/test_crawl_browser.py``) позволяют прогнать **реальный**
:class:`~core.adapters.crawl_browser.BrowserCrawler` без Playwright/httpx.
Данные нейтральны (§1.3).
"""

from __future__ import annotations

from typing import Any

from core.adapters.crawl_scrapy import CrawlError
from core.domain.enums import CrawlStrategy, FetchMode
from core.ports.types import FetchPlan, RawPage

__all__ = [
    "FakeBackend",
    "FakeSession",
    "RecordingHttpGet",
    "ScriptedBrowserCrawler",
    "ScriptedHttpCrawler",
    "html_page",
    "page",
]

#: Достаточный для предиката пригодности текст (≥ ``MIN_USABLE_TEXT``).
USABLE_TEXT = "контент " * 400


def page(
    url: str,
    *,
    fetch_mode: FetchMode = FetchMode.HTTP,
    status_code: int | None = 200,
    content_type: str | None = "text/html",
    content: bytes | None = None,
) -> RawPage:
    """Строит ``RawPage`` (детерминированный ``fetched_at``)."""
    body = content if content is not None else html_page().content
    return RawPage(
        url=url,
        content=body,
        fetched_at="2026-01-01T00:00:00+00:00",
        fetch_mode=fetch_mode,
        status_code=status_code,
        content_type=content_type,
    )


def html_page(text: str = USABLE_TEXT) -> RawPage:
    """HTML-страница с извлекаемым текстом (используется как дефолт :func:`page`)."""
    body = f"<html><body><p>{text}</p></body></html>".encode("utf-8")
    return RawPage(
        url="about:blank", content=body, fetched_at="2026-01-01T00:00:00+00:00"
    )


class ScriptedHttpCrawler:
    """HTTP-краулер: ``RawPage``/исключение по URL; записывает вызовы."""

    version = "fake-http-1"
    engine = "http"
    supports_browser = False

    def __init__(
        self,
        results: dict[str, Any] | None = None,
        *,
        default: Any = None,
    ) -> None:
        self.results = dict(results or {})
        self.default = default
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """Возвращает заданный результат или ``default``; ``None`` ⇒ ``CrawlError``."""
        self.calls.append(url)
        result = self.results.get(url, self.default)
        if result is None:
            raise CrawlError(f"нет страницы: {url}")
        if isinstance(result, BaseException):
            raise result
        return result


class ScriptedBrowserCrawler:
    """Браузерный краулер: результат по ``plan.strategy`` (hybrid/honest); записывает вызовы."""

    version = "fake-browser-1"
    engine = "browser"
    supports_browser = True

    def __init__(
        self,
        hybrid: dict[str, Any] | None = None,
        honest: dict[str, Any] | None = None,
        *,
        default: Any = None,
    ) -> None:
        self.hybrid = dict(hybrid or {})
        self.honest = dict(honest or {})
        self.default = default
        self.calls: list[tuple[str, CrawlStrategy]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """Возвращает заданный результат тира; ``None`` ⇒ ``CrawlError``."""
        self.calls.append((url, plan.strategy))
        table = self.hybrid if plan.strategy is CrawlStrategy.HYBRID else self.honest
        result = table.get(url, self.default)
        if result is None:
            raise CrawlError(f"нет рендера: {url}")
        if isinstance(result, BaseException):
            raise result
        return result


# ---------------------------------------------------------------------------
# Промотированные швы реального ``BrowserCrawler`` (из tests/adapters/test_crawl_browser.py)
# ---------------------------------------------------------------------------


class FakeSession:
    """Фейковая ``BrowserSession``: рендер/cookie из конструктора, запись вызовов."""

    def __init__(
        self,
        *,
        content: bytes = b"<html>rendered</html>",
        cookies: list[dict[str, Any]] | None = None,
        navigate_error: Exception | None = None,
    ) -> None:
        self.content = content
        self.cookie_list = list(cookies or [])
        self.navigate_error = navigate_error
        self.navigated: list[tuple[str, str | None]] = []
        self.cookies_asked: list[str] = []
        self.closed = False

    def navigate(self, url: str, wait_for: str | None = None) -> bytes:
        """Возвращает заданный рендер (или бросает заданную ошибку)."""
        self.navigated.append((url, wait_for))
        if self.navigate_error is not None:
            raise self.navigate_error
        return self.content

    def cookies(self, url: str) -> list[dict[str, Any]]:
        """Возвращает заданные cookie хоста."""
        self.cookies_asked.append(url)
        return list(self.cookie_list)

    def close(self) -> None:
        """Помечает сессию закрытой."""
        self.closed = True


class FakeBackend:
    """Фейковый ``BrowserBackend``: выдаёт ``FakeSession``, считает ``close``."""

    def __init__(
        self,
        *,
        content: bytes = b"<html>rendered</html>",
        cookies: list[dict[str, Any]] | None = None,
        navigate_error: Exception | None = None,
    ) -> None:
        self.content = content
        self.cookies = cookies
        self.navigate_error = navigate_error
        self.sessions: list[FakeSession] = []
        self.close_calls = 0

    def open(self) -> FakeSession:
        """Открывает новую ``FakeSession`` с заданными параметрами."""
        session = FakeSession(
            content=self.content,
            cookies=self.cookies,
            navigate_error=self.navigate_error,
        )
        self.sessions.append(session)
        return session

    def close(self) -> None:
        """Считает закрытия бэкенда."""
        self.close_calls += 1


class RecordingHttpGet:
    """Фейковый ``http_get`` ``(url, headers, cookies) -> (status, body, headers)``."""

    def __init__(
        self,
        responses: list[tuple[int, bytes, dict[str, str]]] | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.responses = list(
            responses or [(200, b"http-body", {"Content-Type": "text/html"})]
        )
        self.error = error
        self.calls: list[tuple[str, dict[str, str], list[dict[str, Any]]]] = []

    def __call__(
        self, url: str, headers: dict[str, str], cookies: list[dict[str, Any]]
    ) -> tuple[int, bytes, dict[str, str]]:
        """Возвращает заданный ответ (или бросает заданную ошибку)."""
        self.calls.append((url, dict(headers), list(cookies)))
        if self.error is not None:
            raise self.error
        index = min(len(self.calls) - 1, len(self.responses) - 1)
        return self.responses[index]
