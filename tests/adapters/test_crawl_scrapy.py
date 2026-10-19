"""Юнит-тесты краулера ``core/adapters/crawl_scrapy.py`` (W7/T7.1).

Сеть и браузеры не используются: внедряется ``fetcher`` (шов), кэш — ``tmp_path``.
Проверяются: интеграция с content-addressed кэшем (промах → фетч → запись →
попадание без повторного фетча), проброс ``FetchPlan`` (``fetch: browser`` игнорирует
кэш), SSRF-отказ до фетча, устойчивый импорт без Scrapy, ``version`` и структурное
соответствие ``CrawlerPort``. Резолв доменных профилей — задача pipeline
(``core.pipeline.fetch_plan``), см. ``tests/pipeline/test_fetch_plan.py``.
Данные нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.adapters.crawl_scrapy import (
    CrawlError,
    CrawlUrlError,
    ScrapyCrawler,
    _CrawlSpec,
    _raw_page_from_scrapy_response,
)
from core.ports.crawler import CrawlerPort
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.cache import ContentAddressedCache, http_key

FIXTURES = Path(__file__).parent / "fixtures"
FIXED_TS = "2026-01-01T00:00:00+00:00"
URL = "https://example.com/page"

_BROWSER = FetchPlan(fetch=FetchMode.BROWSER)


def _page(
    url: str, content: bytes = b"payload", fetch: FetchMode = FetchMode.HTTP
) -> RawPage:
    return RawPage(url=url, content=content, fetched_at=FIXED_TS, fetch_mode=fetch)


class _RecordingFetcher:
    """Фетчер-объект с методом ``fetch``; записывает ``(url, plan.fetch)`` вызовы."""

    def __init__(self, content: bytes = b"payload") -> None:
        self.calls: list[tuple[str, FetchMode]] = []
        self.content = content

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append((url, plan.fetch))
        return _page(url, self.content, plan.fetch)


def _counting_fetcher(
    calls: list[tuple[str, FetchMode]], content: bytes = b"payload"
):
    """Callable-фетчер ``(url, plan) -> RawPage``, пишущий вызовы в ``calls``."""

    def fetch(url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        calls.append((url, plan.fetch))
        return _page(url, content, plan.fetch)

    return fetch


# ---------------------------------------------------------------------------
# Кэш (R7, ADR 0016)
# ---------------------------------------------------------------------------


def test_cache_miss_fetches_and_second_call_served_from_cache(tmp_path: Path) -> None:
    calls: list[tuple[str, FetchMode]] = []
    crawler = ScrapyCrawler(
        _counting_fetcher(calls, b"fresh-body"),
        cache=ContentAddressedCache(tmp_path),
    )

    first = crawler.fetch(URL)
    assert first.content == b"fresh-body"
    assert len(calls) == 1
    # Промах записал сырые байты под http_key(url).
    assert (tmp_path / "http" / f"{http_key(URL)}.bin").read_bytes() == b"fresh-body"

    second = crawler.fetch(URL)
    assert second.content == b"fresh-body"
    assert len(calls) == 1  # второй вызов — из кэша, фетчер не дёргался


def test_cache_hit_skips_fetcher(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", http_key(URL), b"cached-body")
    fetcher = _RecordingFetcher(b"live-body")

    crawler = ScrapyCrawler(fetcher, cache=cache)
    page = crawler.fetch(URL)

    assert page.content == b"cached-body"
    assert fetcher.calls == []


def test_without_cache_each_call_fetches(tmp_path: Path) -> None:
    calls: list[tuple[str, FetchMode]] = []
    crawler = ScrapyCrawler(_counting_fetcher(calls))
    crawler.fetch(URL)
    crawler.fetch(URL)
    assert len(calls) == 2


def test_cache_write_uses_global_http_layout(tmp_path: Path) -> None:
    crawler = ScrapyCrawler(_RecordingFetcher(b"x"), cache=ContentAddressedCache(tmp_path))
    crawler.fetch(URL)
    assert (tmp_path / "http").is_dir()
    assert (tmp_path / "http" / f"{http_key(URL)}.bin").is_file()


class _SpyCache(ContentAddressedCache):
    """Кэш-шпион: записывает обращения ``get_bytes``/``put_bytes``."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.gets: list[tuple[str, str]] = []
        self.puts: list[tuple[str, str]] = []

    def get_bytes(self, kind: str, key: str) -> bytes | None:
        self.gets.append((kind, key))
        return super().get_bytes(kind, key)

    def put_bytes(self, kind: str, key: str, data: bytes) -> Path:
        self.puts.append((kind, key))
        return super().put_bytes(kind, key, data)


def test_browser_fetch_bypasses_cache(tmp_path: Path) -> None:
    cache = _SpyCache(tmp_path)
    fetcher = _RecordingFetcher(b"live-body")
    crawler = ScrapyCrawler(fetcher, cache=cache)

    page = crawler.fetch(URL, _BROWSER)

    assert page.content == b"live-body"
    assert fetcher.calls == [(URL, FetchMode.BROWSER)]
    # Браузерный рендер не читает и не пишет кэш (нет кросс-контаминации HTTP/browser).
    assert cache.gets == []
    assert cache.puts == []


def test_browser_fetch_always_live_even_with_cache(tmp_path: Path) -> None:
    cache = _SpyCache(tmp_path)
    # Предзаполняем кэш под тем же ключом — браузер обязан его игнорировать.
    cache.put_bytes("http", http_key(URL), b"cached-body")
    cache.gets.clear()
    cache.puts.clear()
    fetcher = _RecordingFetcher(b"live-body")
    crawler = ScrapyCrawler(fetcher, cache=cache)

    first = crawler.fetch(URL, _BROWSER)
    second = crawler.fetch(URL, _BROWSER)

    assert first.content == b"live-body" and second.content == b"live-body"
    assert len(fetcher.calls) == 2
    assert cache.gets == [] and cache.puts == []


def test_http_fetch_uses_cache(tmp_path: Path) -> None:
    cache = _SpyCache(tmp_path)
    crawler = ScrapyCrawler(_RecordingFetcher(b"live-body"), cache=cache)

    crawler.fetch(URL, FetchPlan(fetch=FetchMode.HTTP))

    assert cache.gets and cache.puts
    assert cache.gets[0] == ("http", http_key(URL))
    assert cache.puts[0] == ("http", http_key(URL))


# ---------------------------------------------------------------------------
# План получения (FetchPlan)
# ---------------------------------------------------------------------------


def test_plan_browser_passthrough() -> None:
    fetcher = _RecordingFetcher()
    crawler = ScrapyCrawler(fetcher)
    page = crawler.fetch(URL, _BROWSER)
    assert fetcher.calls == [(URL, FetchMode.BROWSER)]
    assert page.fetch_mode is FetchMode.BROWSER


def test_plan_default_is_http() -> None:
    fetcher = _RecordingFetcher()
    crawler = ScrapyCrawler(fetcher)
    page = crawler.fetch(URL)
    assert fetcher.calls == [(URL, FetchMode.HTTP)]
    assert page.fetch_mode is FetchMode.HTTP


# ---------------------------------------------------------------------------
# SSRF / ошибки
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile",
    ["http://127.0.0.1/", "http://169.254.169.254/latest/meta-data/", "ftp://example.com/x"],
)
def test_ssrf_url_rejected_before_fetch(hostile: str) -> None:
    fetcher = _RecordingFetcher()
    crawler = ScrapyCrawler(fetcher)
    with pytest.raises(ValueError):
        crawler.fetch(hostile)
    assert fetcher.calls == []  # фетч не выполнен


def test_ssrf_error_is_crawl_error_too() -> None:
    crawler = ScrapyCrawler(_RecordingFetcher())
    with pytest.raises(CrawlError):
        crawler.fetch("http://127.0.0.1/")
    assert issubclass(CrawlUrlError, CrawlError)
    assert issubclass(CrawlUrlError, ValueError)


def test_fetcher_exception_wrapped_in_crawl_error(tmp_path: Path) -> None:
    def boom(url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        raise RuntimeError("network down")

    cache = ContentAddressedCache(tmp_path)
    crawler = ScrapyCrawler(boom, cache=cache)
    with pytest.raises(CrawlError):
        crawler.fetch(URL)
    # при сбое в кэш ничего не пишется
    assert cache.get_bytes("http", http_key(URL)) is None


def test_non_callable_fetcher_raises_crawl_error() -> None:
    crawler = ScrapyCrawler(object())  # type: ignore[arg-type]
    with pytest.raises(CrawlError):
        crawler.fetch(URL)


# ---------------------------------------------------------------------------
# Ленивые импорты / контракт
# ---------------------------------------------------------------------------


def test_available_true_with_injected_fetcher() -> None:
    assert ScrapyCrawler(_RecordingFetcher()).available is True
    assert isinstance(ScrapyCrawler().available, bool)


def test_real_path_without_scrapy_raises_crawl_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.adapters.crawl_scrapy as mod

    monkeypatch.setattr(mod, "_scrapy_available", lambda: False)
    crawler = ScrapyCrawler()
    with pytest.raises(CrawlError):
        crawler.fetch(URL)


def test_scrapy_available_matches_environment() -> None:
    import core.adapters.crawl_scrapy as mod

    assert mod._scrapy_available() is _spec_present("scrapy")


def _spec_present(name: str) -> bool:
    import importlib.util

    return importlib.util.find_spec(name) is not None


# ---------------------------------------------------------------------------
# Условный фетч: валидаторы из bytes-заголовков Scrapy и тело 304 из кэша
# ---------------------------------------------------------------------------


class _FakeScrapyResponse:
    """Мини-дублёр ответа Scrapy: ``headers`` — ``bytes``-ключи, ``body``/``status``."""

    def __init__(
        self,
        *,
        url: str = URL,
        body: bytes = b"fresh-body",
        status: int = 200,
        headers: dict[bytes, bytes] | None = None,
    ) -> None:
        self.url = url
        self.body = body
        self.status = status
        self.headers = dict(headers or {})


def test_scrapy_collect_captures_validators_from_bytes_headers() -> None:
    spec = _CrawlSpec(url=URL, fetch=FetchMode.HTTP, wait_for=None)
    response = _FakeScrapyResponse(
        headers={
            b"Content-Type": b"text/html; charset=utf-8",
            b"ETag": b'"v1"',
            b"Last-Modified": b"Wed, 21 Oct 2026 07:28:00 GMT",
        }
    )

    page = _raw_page_from_scrapy_response(spec, response)

    assert page is not None
    assert page.content == b"fresh-body"
    assert page.content_type == "text/html; charset=utf-8"
    assert page.etag == '"v1"'
    assert page.last_modified == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert page.not_modified is False


def test_scrapy_304_returns_cached_bytes() -> None:
    spec = _CrawlSpec(
        url=URL,
        fetch=FetchMode.HTTP,
        wait_for=None,
        cached=b"cached-representation",
    )

    page = _raw_page_from_scrapy_response(
        spec, _FakeScrapyResponse(body=b"", status=304)
    )

    assert page is not None
    assert page.not_modified is True
    assert page.status_code == 304
    assert page.content == b"cached-representation"
    assert spec.error is None


def test_scrapy_304_without_cache_is_explicit_error() -> None:
    spec = _CrawlSpec(url=URL, fetch=FetchMode.HTTP, wait_for=None)

    page = _raw_page_from_scrapy_response(
        spec, _FakeScrapyResponse(body=b"", status=304)
    )

    assert page is None  # пустую страницу не фабрикуем
    assert spec.error is not None and "304" in spec.error


def test_conditional_plan_bypasses_cache_and_does_not_write_on_304(
    tmp_path: Path,
) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", http_key(URL), b"cached-representation")
    seen: list[FetchPlan] = []

    def fetcher(url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        seen.append(plan)
        return RawPage(
            url=url,
            content=b"cached-representation",
            fetched_at=FIXED_TS,
            fetch_mode=FetchMode.HTTP,
            status_code=304,
            not_modified=True,
        )

    crawler = ScrapyCrawler(fetcher, cache=cache)
    page = crawler.fetch(
        URL, FetchPlan(if_none_match='"v1"', if_modified_since="Wed, 21 Oct 2026")
    )

    # Условный запрос дошёл до фетчера (кэш-шорткат не проглотил его).
    assert len(seen) == 1
    assert seen[0].if_none_match == '"v1"'
    assert page.not_modified is True
    # 304 не создаёт новую запись: байты кэша не перезаписаны.
    assert cache.get_bytes("http", http_key(URL)) == b"cached-representation"


# ---------------------------------------------------------------------------
# Ф1.1 для Scrapy: сайдкар меты на 200 и замыкание цикла (план → мета → GET)
# ---------------------------------------------------------------------------


class _FakeWorker:
    """Шов воркера: фиксирует ``_CrawlSpec`` и отдаёт заготовленный ответ Scrapy.

    Позволяет проверить реальный путь ``fetch → _fetch_with_scrapy`` (включая
    фактические заголовки запроса в ``spec.headers``) без Twisted и сети.
    """

    def __init__(self, response: _FakeScrapyResponse) -> None:
        self.response = response
        self.specs: list[_CrawlSpec] = []
        self.closed = False

    def submit(self, settings, spec: _CrawlSpec, fut, timeout_s) -> None:
        self.specs.append(spec)
        page = _raw_page_from_scrapy_response(spec, self.response)
        if page is not None:
            spec.collected.append(page)
        fut.set_result(True)

    def close(self) -> None:
        self.closed = True


def _seed_cache(
    cache: ContentAddressedCache,
    *,
    content: bytes = b"cached-representation",
    etag: str | None = '"v1"',
    last_modified: str | None = "Wed, 21 Oct 2026 07:28:00 GMT",
) -> None:
    """Тело + сайдкар прошлого получения (как после успешного ``200``)."""
    cache.put_bytes("http", http_key(URL), content)
    cache.put_meta(
        "http",
        http_key(URL),
        {
            "etag": etag,
            "last_modified": last_modified,
            "status_code": 200,
            "content_type": "text/html",
            "final_url": URL,
        },
    )


def test_scrapy_writes_meta_sidecar_on_200(tmp_path: Path) -> None:
    # Без сайдкара цикл условных запросов в Scrapy-пути не мог бы начаться.
    cache = ContentAddressedCache(tmp_path)
    worker = _FakeWorker(
        _FakeScrapyResponse(
            body=b"fresh-body",
            status=200,
            headers={
                b"Content-Type": b"text/html; charset=utf-8",
                b"ETag": b'"v1"',
                b"Last-Modified": b"Wed, 21 Oct 2026 07:28:00 GMT",
            },
        )
    )
    crawler = ScrapyCrawler(worker=worker, cache=cache)

    page = crawler.fetch(URL)

    assert page.content == b"fresh-body"
    meta = cache.get_meta("http", http_key(URL))
    assert meta == {
        "etag": '"v1"',
        "last_modified": "Wed, 21 Oct 2026 07:28:00 GMT",
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
        "final_url": URL,
    }
    assert (tmp_path / "http" / f"{http_key(URL)}.meta.json").is_file()


def test_scrapy_cache_meta_validators_produce_conditional_request_and_304(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    _seed_cache(cache)
    worker = _FakeWorker(_FakeScrapyResponse(body=b"", status=304))
    crawler = ScrapyCrawler(worker=worker, cache=cache)

    def _no_write(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("304 не должен писать новую запись кэша")

    monkeypatch.setattr(cache, "put_bytes", _no_write)
    monkeypatch.setattr(cache, "put_meta", _no_write)

    page = crawler.fetch(URL)  # план без валидаторов

    # Заголовки реально ушли в запрос Scrapy (``_conditional_headers`` из меты).
    assert len(worker.specs) == 1
    assert worker.specs[0].headers == {
        "If-None-Match": '"v1"',
        "If-Modified-Since": "Wed, 21 Oct 2026 07:28:00 GMT",
    }
    assert worker.specs[0].cached == b"cached-representation"
    assert page.not_modified is True
    assert page.status_code == 304
    assert page.content == b"cached-representation"


def test_scrapy_meta_fallback_304_without_cached_bytes_is_explicit_error(
    tmp_path: Path,
) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_meta("http", http_key(URL), {"etag": '"v1"'})  # мета без тела
    worker = _FakeWorker(_FakeScrapyResponse(body=b"", status=304))
    crawler = ScrapyCrawler(worker=worker, cache=cache)

    with pytest.raises(CrawlError, match="304"):
        crawler.fetch(URL)

    assert worker.specs[0].headers == {"If-None-Match": '"v1"'}


def test_scrapy_plan_validators_override_cache_meta(tmp_path: Path) -> None:
    cache = ContentAddressedCache(tmp_path)
    _seed_cache(cache)
    seen: list[FetchPlan] = []

    def fetcher(url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        seen.append(plan)
        return _page(url, b"fresh-body")

    crawler = ScrapyCrawler(fetcher, cache=cache)

    page = crawler.fetch(URL, FetchPlan(if_none_match='"plan-version"'))

    assert seen[0].if_none_match == '"plan-version"'  # план, не '"v1"' из меты
    assert seen[0].if_modified_since is None
    assert page.content == b"fresh-body"


def test_scrapy_cache_hit_with_meta_but_without_validators_served_from_cache(
    tmp_path: Path,
) -> None:
    cache = ContentAddressedCache(tmp_path)
    _seed_cache(cache, etag=None, last_modified=None)
    fetcher = _RecordingFetcher(b"live-body")
    crawler = ScrapyCrawler(fetcher, cache=cache)

    page = crawler.fetch(URL)

    assert fetcher.calls == []  # валидаторов нет нигде → прежний кэш-шорткат
    assert page.content == b"cached-representation"
