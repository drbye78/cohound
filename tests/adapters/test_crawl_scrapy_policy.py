"""Политика Scrapy-пути: кеш ленты ``robots.txt`` и учёт фактических HTTP-запросов.

Сеть и Twisted-реактор не используются: middleware — **настоящие** классы Scrapy
(``RobotsTxtMiddleware`` и наш downloader-middleware счётчика), а движок и краулер —
минимальные дублёры. Движок отдаёт заготовленный ответ ленты и считает вызовы, поэтому
«сколько раз сходили за лентой» — наблюдение теста, а не догадка.

Проверяется:

1. два ``fetch`` подряд на один origin читают ленту **один** раз, а правило соблюдается
   на обоих (отказ по ``disallow`` — ``IgnoreRequest("Forbidden by robots.txt")``, который
   pipeline классифицирует как ``robots_disallow``);
2. «ленты нет» (404) — разрешение и кешируется, а **недоступная** лента не кешируется как
   разрешение (следующая попытка спрашивает origin снова);
3. кеш не переезжает между user-agent'ами;
4. счётчик фактических запросов считает **каждый** запрос по его хосту, а настройки
   краула несут счётчик текущей попытки и не смешивают попытки с чужими.

Данные нейтральны (§1.3): ``a.test``/``other.test``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from scrapy.exceptions import IgnoreRequest

from core.adapters.crawl_scrapy import (
    _HTTP_AUDIT_MIDDLEWARE_PRIORITY,
    _HTTP_AUDIT_SETTING,
    ScrapyCrawler,
    _clear_robots_cache,
    _CrawlSpec,
    _http_audit_middleware_class,
    _record_failure,
    _shared_robots_middleware_class,
)
from core.domain.enums import FailureKind
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util import http_audit

URL = "https://a.test/page"
DISALLOW_ALL = b"User-agent: *\nDisallow: /\n"
DISALLOW_BLOCKED = b"User-agent: *\nDisallow: /blocked\n"


class _FakeSettings:
    """Минимум ``Settings`` для middleware: ``getbool``/``get``/``[]``."""

    def __init__(self, values: dict[str, Any]) -> None:
        self._values = dict(values)

    def getbool(self, key: str) -> bool:
        return bool(self._values.get(key))

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._values[key]


class _FakeStats:
    """Минимум ``StatsCollector``: ``inc_value`` (middleware пишет служебные счётчики)."""

    def __init__(self) -> None:
        self.values: dict[str, int] = {}

    def inc_value(self, key: str, count: int = 1, start: int = 0) -> None:
        self.values[key] = self.values.get(key, start) + count


class _FakeSignals:
    """Минимум ``SignalManager``: сигнал ``robots_parsed`` без подписчиков."""

    async def send_catch_log_async(self, **_kwargs: Any) -> list[Any]:
        return []


class _FakeEngine:
    """Движок Scrapy: отдаёт заготовленные ответы ленты и записывает URL запросов."""

    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.urls: list[str] = []

    async def download_async(self, request: Any) -> Any:
        from scrapy.http import Response  # type: ignore[import-not-found]

        self.urls.append(request.url)
        outcome = self.outcomes[min(len(self.urls) - 1, len(self.outcomes) - 1)]
        if isinstance(outcome, BaseException):
            raise outcome
        status, body = outcome
        return Response(request.url, body=body, status=status, request=request)


class _FakeCrawler:
    """Минимум ``Crawler`` для ``RobotsTxtMiddleware``: настройки, статистика, сигналы."""

    def __init__(
        self,
        engine: _FakeEngine,
        *,
        user_agent: str = "cohound-test",
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.engine = engine
        values: dict[str, Any] = {
            "ROBOTSTXT_OBEY": True,
            "USER_AGENT": user_agent,
            "ROBOTSTXT_USER_AGENT": None,
            "ROBOTSTXT_PARSER": "scrapy.robotstxt.ProtegoRobotParser",
        }
        values.update(extra or {})
        self.settings = _FakeSettings(values)
        self.stats = _FakeStats()
        self.signals = _FakeSignals()
        self.spider = None


class _Failure:
    """Минимум Twisted ``Failure``: ``_record_failure`` читает ``value``/``response``."""

    def __init__(self, value: BaseException) -> None:
        self.value = value


class _AuditingWorker:
    """Шов движка: отмечает в счётчике те запросы, что Scrapy «выпустил»."""

    def __init__(self, requests: list[str]) -> None:
        self.requests = requests
        self.specs: list[_CrawlSpec] = []

    def submit(self, settings: Any, spec: _CrawlSpec, fut: Any, timeout_s: float) -> None:
        self.specs.append(spec)
        audit = settings[_HTTP_AUDIT_SETTING]
        for url in self.requests:
            audit.note(url)
        spec.collected.append(
            RawPage(url=spec.url, content=b"body", fetched_at="t", fetch_mode=spec.fetch)
        )
        fut.set_result(True)

    def close(self) -> None:
        """Воркеру закрываться нечему."""


@pytest.fixture(autouse=True)
def _clean_robots_cache() -> Any:
    """Кеш лент — процесс-глобальный: тесты не должны видеть ленты друг друга."""
    _clear_robots_cache()
    yield
    _clear_robots_cache()


def _middleware(engine: _FakeEngine, *, user_agent: str = "cohound-test") -> Any:
    """Настоящий middleware ленты с фейковым краулером (без реактора)."""
    return _shared_robots_middleware_class()(_FakeCrawler(engine, user_agent=user_agent))


def _request(url: str) -> Any:
    from scrapy.http import Request  # type: ignore[import-not-found]

    return Request(url)


# ---------------------------------------------------------------------------
# Кеш ленты: один запрос на origin, политика прежняя
# ---------------------------------------------------------------------------


def test_two_fetches_on_one_origin_read_the_feed_once() -> None:
    """Два ``fetch`` — одна лента; правило применяется к обоим документам.

    Это и есть регресс дефекта 07.11.2026: свежий ``CrawlerRunner`` на документ создавал
    свежий middleware, и лента запрашивалась заново.
    """
    engine = _FakeEngine([(200, DISALLOW_BLOCKED)])
    first = _middleware(engine)
    second = _middleware(engine)

    assert asyncio.run(first.process_request(_request("https://a.test/allowed"))) is None
    with pytest.raises(IgnoreRequest) as excinfo:
        asyncio.run(second.process_request(_request("https://a.test/blocked")))

    assert str(excinfo.value) == "Forbidden by robots.txt"
    assert engine.urls == ["https://a.test/robots.txt"]


def test_disallow_still_maps_to_robots_disallow() -> None:
    """Отказ ленты — класс политики (``robots_disallow``), а не сбой транспорта."""
    engine = _FakeEngine([(200, DISALLOW_ALL)])
    middleware = _middleware(engine)

    with pytest.raises(IgnoreRequest) as excinfo:
        asyncio.run(middleware.process_request(_request(URL)))

    spec = _CrawlSpec(url=URL, fetch=FetchMode.HTTP, wait_for=None)
    _record_failure(spec, _Failure(excinfo.value))
    assert spec.error_kind == FailureKind.ROBOTS_DISALLOW.value


def test_missing_feed_is_a_permission_and_is_cached() -> None:
    """``404`` — наблюдение «ленты нет»: разрешено всё, и это кешируется."""
    engine = _FakeEngine([(404, b"")])
    first = _middleware(engine)
    second = _middleware(engine)

    assert asyncio.run(first.process_request(_request(URL))) is None
    assert asyncio.run(second.process_request(_request(URL))) is None
    assert engine.urls == ["https://a.test/robots.txt"]


def test_unavailable_feed_is_not_cached_as_permission() -> None:
    """Сбой чтения ленты — не разрешение: следующий ``fetch`` спрашивает origin снова.

    Первая попытка (Scrapy-семантика: ошибка загрузки = ``rp is None`` ⇒ пропустить)
    проходит; вторая читает ленту заново и на этот раз отказывает по ``disallow``.
    Если бы ``None`` закешировался, второй отказ не состоялся бы.
    """
    engine = _FakeEngine([RuntimeError("feed down"), (200, DISALLOW_ALL)])
    first = _middleware(engine)
    second = _middleware(engine)

    assert asyncio.run(first.process_request(_request(URL))) is None
    with pytest.raises(IgnoreRequest) as excinfo:
        asyncio.run(second.process_request(_request(URL)))

    assert str(excinfo.value) == "Forbidden by robots.txt"
    assert engine.urls == [
        "https://a.test/robots.txt",
        "https://a.test/robots.txt",
    ]


def test_cache_key_separates_user_agents() -> None:
    """Лента одного user-agent'а не обслуживает другого: два ключа — два запроса."""
    engine = _FakeEngine([(200, DISALLOW_BLOCKED)])
    first = _middleware(engine, user_agent="agent-one")
    second = _middleware(engine, user_agent="agent-two")

    assert asyncio.run(first.process_request(_request(URL))) is None
    assert asyncio.run(second.process_request(_request(URL))) is None
    assert engine.urls == [
        "https://a.test/robots.txt",
        "https://a.test/robots.txt",
    ]


def test_build_settings_replaces_the_stock_robots_middleware() -> None:
    """Наш подкласс занимает приоритет стокового, стоковый выключен.

    Проверяется на реальном слиянии с ``DOWNLOADER_MIDDLEWARES_BASE``: иначе подмена
    осталась бы в объявлении, а лента по-прежнему читалась бы на каждый документ.
    """
    from scrapy.settings import Settings
    from scrapy.utils.conf import build_component_list

    settings = ScrapyCrawler()._build_settings(FetchPlan())
    merged = Settings(
        {"DOWNLOADER_MIDDLEWARES": settings["DOWNLOADER_MIDDLEWARES"]}
    ).getwithbase("DOWNLOADER_MIDDLEWARES")
    names = [
        getattr(item, "__qualname__", str(item)) for item in build_component_list(merged)
    ]

    stock = "scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware"
    assert stock not in names
    assert any(name.endswith("_SharedRobotsTxtMiddleware") for name in names)


# ---------------------------------------------------------------------------
# Учёт фактических HTTP-запросов
# ---------------------------------------------------------------------------


def test_audit_middleware_counts_each_request_by_actual_host() -> None:
    """Middleware считает запросы, прошедшие до загрузчика, по фактическому хосту."""
    audit = http_audit.HttpAudit("a.test")
    crawler = _FakeCrawler(_FakeEngine([]), extra={_HTTP_AUDIT_SETTING: audit})
    middleware = _http_audit_middleware_class().from_crawler(crawler)

    middleware.process_request(_request("https://a.test/page"))
    middleware.process_request(_request("https://other.test/docs/robots.txt"))

    assert audit.snapshot() == {
        "http_requests": 2,
        "foreign_hosts": {"other.test": 1},
    }


def test_audit_middleware_is_disabled_without_an_attempt() -> None:
    """Без счётчика в настройках компонент отключается (``NotConfigured``), не выдумывая."""
    from scrapy.exceptions import NotConfigured

    with pytest.raises(NotConfigured):
        _http_audit_middleware_class().from_crawler(_FakeCrawler(_FakeEngine([])))


def test_settings_carry_the_attempt_audit_and_install_the_counter() -> None:
    """Счётчик текущей попытки доезжает до движка настройкой краулера."""
    with http_audit.attempt("a.test") as audit:
        settings = ScrapyCrawler()._build_settings(FetchPlan())

    assert settings[_HTTP_AUDIT_SETTING] is audit
    middleware = settings["DOWNLOADER_MIDDLEWARES"]
    assert (
        middleware[_http_audit_middleware_class()] == _HTTP_AUDIT_MIDDLEWARE_PRIORITY
    )


def test_settings_copy_keeps_the_same_audit_handle() -> None:
    """``Settings.copy()`` (deepcopy!) не создаёт второй счётчик и не падает на локе."""
    from scrapy import Spider
    from scrapy.crawler import Crawler

    with http_audit.attempt("a.test") as audit:
        settings = ScrapyCrawler()._build_settings(FetchPlan())
        # Ровно тот путь, которым настоящий ``CrawlerRunner`` передаёт настройки краулу.
        crawler = Crawler(Spider, settings)

    assert crawler.settings.get(_HTTP_AUDIT_SETTING) is audit


def test_settings_without_an_attempt_do_not_install_the_counter() -> None:
    """Вне попытки middleware не ставится: «не измеряли» честнее выдуманного нуля."""
    settings = ScrapyCrawler()._build_settings(FetchPlan())

    assert _HTTP_AUDIT_SETTING not in settings
    assert _http_audit_middleware_class() not in (
        settings["DOWNLOADER_MIDDLEWARES"] or {}
    )


def test_scrapy_path_reports_the_requests_the_engine_made() -> None:
    """Число приходит от движка (его же запросы), а не из формы URL."""
    worker = _AuditingWorker(
        [
            "https://a.test/robots.txt",
            "https://docs.other.test/robots.txt",
            "https://a.test/page",
        ]
    )
    with http_audit.attempt("a.test") as audit:
        ScrapyCrawler(worker=worker).fetch(URL)

    assert audit.snapshot() == {
        "http_requests": 3,
        "foreign_hosts": {"docs.other.test": 1},
    }


# ---------------------------------------------------------------------------
# Второй пейсер: его нет
# ---------------------------------------------------------------------------


def test_download_delay_is_zero_and_no_other_pacer_is_added() -> None:
    """Пейсинг задаёт ``RateLimiter``; скрытая задержка Scrapy на документ — дефект."""
    settings = ScrapyCrawler()._build_settings(FetchPlan())

    assert settings["DOWNLOAD_DELAY"] == 0
    assert settings.get("DOWNLOAD_DELAY_JITTER") == 0
    assert not settings.get("AUTOTHROTTLE_ENABLED", False)
    assert not settings.get("RANDOMIZE_DOWNLOAD_DELAY", False)
    # Не потолок ширины краула: per-downloader, а downloader свой на каждый fetch.
    assert settings["CONCURRENT_REQUESTS_PER_DOMAIN"] == 2