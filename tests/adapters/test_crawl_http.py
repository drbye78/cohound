"""Юнит-тесты лёгкого HTTP-краулера ``core/adapters/crawl_http.py`` (M1, A1).

Сеть не используется: внедряется шов ``fetcher``/``http_get``, кэш — ``tmp_path``.
Проверяются: ``RawPage`` из шва, отображение ответа ``httpx`` (финальный URL/статус/
тип), SSRF-отказ до фетча, честная ошибка на ``FetchMode.BROWSER``, интеграция с
content-addressed кэшем (промах → фетч → запись → попадание без повторного фетча),
``available``/ленивый ``httpx``, ``version`` и структурное соответствие ``CrawlerPort``.
Данные нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from core.adapters.crawl_http import CrawlUnavailable, HttpCrawler
from core.adapters.crawl_scrapy import (
    CrawlError,
    CrawlNotFound,
    CrawlPayloadTooLarge,
    CrawlUrlError,
)
from core.domain.enums import FailureKind
from core.ports.crawler import CrawlerPort
from core.ports.types import FetchPlan, FetchMode, RawPage
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
        etag: str | None = None,
        last_modified: str | None = None,
        content_length: int | None = None,
    ) -> None:
        self.url = url
        self.content = content
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        if content_type:
            self.headers["Content-Type"] = content_type
        if etag:
            self.headers["ETag"] = etag
        if last_modified:
            self.headers["Last-Modified"] = last_modified
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def raise_for_status(self) -> None:
        """Как ``httpx``: 4xx/5xx — ошибка, ``304`` — не ошибка."""
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_bytes(self):  # noqa: ANN201 - дублёр потокового тела httpx
        """Потоковое тело: один чанк (достаточно для проверки cap)."""
        if self.content:
            yield self.content

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False


class _FakeHttpx:
    """Мини-дублёр ``httpx``: отдаёт заготовленные ответы, пишет заголовки запроса."""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append((url, dict(kwargs.get("headers") or {})))
        index = min(len(self.calls) - 1, len(self._responses) - 1)
        return self._responses[index]

    def stream(self, method: str, url: str, **kwargs: Any) -> _FakeResponse:
        """Потоковый путь: та же запись вызовов, ответ — контекстный менеджер."""
        assert method == "GET"
        self.calls.append((url, dict(kwargs.get("headers") or {})))
        index = min(len(self.calls) - 1, len(self._responses) - 1)
        return self._responses[index]


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
    assert page.fetch_mode is FetchMode.HTTP
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
            etag='"v1"',
            last_modified="Wed, 21 Oct 2026 07:28:00 GMT",
        )
    )
    assert page.url == "https://example.com/redirected"
    assert page.content == b"redirected-body"
    assert page.status_code == 203
    assert page.content_type == "text/html; charset=utf-8"
    assert page.fetch_mode is FetchMode.HTTP
    # Валидаторы ответа сохраняются для следующего условного запроса.
    assert page.etag == '"v1"'
    assert page.last_modified == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert page.not_modified is False


def test_page_from_response_without_content_type() -> None:
    page = HttpCrawler._page_from_response(_FakeResponse(content_type=None))
    assert page.content_type is None
    assert page.etag is None
    assert page.last_modified is None


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


@pytest.mark.parametrize("render", [FetchMode.BROWSER, "browser"])
def test_browser_fetch_raises_honest_error(render: object) -> None:
    calls: list[str] = []
    crawler = HttpCrawler(_counting_fetcher(calls))
    with pytest.raises(CrawlError, match="browser"):
        crawler.fetch(URL, FetchPlan(fetch=FetchMode(render)))  # type: ignore[arg-type]
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


# ---------------------------------------------------------------------------
# Условный повторный фетч (ETag/Last-Modified → 304)
# ---------------------------------------------------------------------------


def _crawler_with_httpx(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[_FakeResponse],
    cache: ContentAddressedCache,
) -> tuple[HttpCrawler, _FakeHttpx]:
    """Краулер на фейковом ``httpx`` (сеть не используется)."""
    fake = _FakeHttpx(responses)
    monkeypatch.setattr(HttpCrawler, "_import_http", staticmethod(lambda: fake))
    return HttpCrawler(cache=cache), fake


def test_first_fetch_sends_no_conditional_headers_and_writes_meta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [
            _FakeResponse(
                content=BODY,
                status_code=200,
                etag='"v1"',
                last_modified="Wed, 21 Oct 2026 07:28:00 GMT",
            )
        ],
        cache,
    )

    page = crawler.fetch(URL)

    headers = fake.calls[0][1]
    assert "If-None-Match" not in headers
    assert "If-Modified-Since" not in headers
    assert page.etag == '"v1"'
    assert page.last_modified == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert page.not_modified is False
    # Метаданные-сайдкар записаны рядом с .bin.
    meta = cache.get_meta("http", http_key(URL))
    assert meta is not None
    assert meta["etag"] == '"v1"'
    assert meta["last_modified"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert meta["status_code"] == 200
    assert meta["final_url"] == "https://example.com/final"
    assert (tmp_path / "http" / f"{http_key(URL)}.meta.json").is_file()


def test_second_fetch_with_validators_304_returns_cached_bytes_without_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", http_key(URL), b"cached-representation")
    cache.put_meta(
        "http",
        http_key(URL),
        {
            "etag": '"v1"',
            "last_modified": "Wed, 21 Oct 2026 07:28:00 GMT",
            "status_code": 200,
            "content_type": "text/html",
            "final_url": "https://example.com/final",
        },
    )
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"", status_code=304)],
        cache,
    )

    def _no_write(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("304 не должен писать новую запись кэша")

    monkeypatch.setattr(cache, "put_bytes", _no_write)
    monkeypatch.setattr(cache, "put_meta", _no_write)

    page = crawler.fetch(
        URL,
        FetchPlan(
            if_none_match='"v1"',
            if_modified_since="Wed, 21 Oct 2026 07:28:00 GMT",
        ),
    )

    headers = fake.calls[0][1]
    assert headers["If-None-Match"] == '"v1"'
    assert headers["If-Modified-Since"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert page.not_modified is True
    assert page.status_code == 304
    assert page.content == b"cached-representation"
    assert page.etag == '"v1"'
    assert page.last_modified == "Wed, 21 Oct 2026 07:28:00 GMT"


def test_304_with_empty_cache_is_explicit_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crawler, _fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"", status_code=304)],
        ContentAddressedCache(tmp_path),
    )

    with pytest.raises(CrawlError, match="304"):
        crawler.fetch(URL, FetchPlan(if_none_match='"v1"'))


def test_legacy_cache_entry_without_meta_behaves_as_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", http_key(URL), b"legacy-body")
    fake = _FakeHttpx([])
    monkeypatch.setattr(HttpCrawler, "_import_http", staticmethod(lambda: fake))
    crawler = HttpCrawler(cache=cache)

    page = crawler.fetch(URL)

    assert fake.calls == []  # кэш-хит без обращения к сети
    assert page.content == b"legacy-body"
    assert page.url == URL
    assert page.status_code is None
    assert page.content_type is None
    assert page.etag is None
    assert page.last_modified is None
    assert page.not_modified is False


# ---------------------------------------------------------------------------
# Ф1.1: адаптер сам замыкает цикл валидаторов (план → мета кэша → безусловный)
# ---------------------------------------------------------------------------


def _seed_cache_with_validators(
    cache: ContentAddressedCache,
    *,
    content: bytes = b"cached-representation",
    etag: str | None = '"v1"',
    last_modified: str | None = "Wed, 21 Oct 2026 07:28:00 GMT",
) -> None:
    """Кладёт в кэш тело и сайдкар прошлого получения (как после живого ``200``)."""
    cache.put_bytes("http", http_key(URL), content)
    cache.put_meta(
        "http",
        http_key(URL),
        {
            "etag": etag,
            "last_modified": last_modified,
            "status_code": 200,
            "content_type": "text/html",
            "final_url": "https://example.com/final",
        },
    )


def test_cache_meta_validators_produce_conditional_request_and_304(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Регрессия Ф1.1: план без валидаторов — адаптер берёт их из меты кэша.

    Без этой ветки недельный цикл всегда отправлял безусловный GET и получал полный
    ``200``: «главный рычаг» экономии не работал. Здесь проверяется, что условные
    заголовки реально доехали до транспорта, а ``304`` отдал кэшированные байты.
    """
    cache = ContentAddressedCache(tmp_path)
    _seed_cache_with_validators(cache)
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"", status_code=304)],
        cache,
    )

    def _no_write(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("304 не должен писать новую запись кэша")

    monkeypatch.setattr(cache, "put_bytes", _no_write)
    monkeypatch.setattr(cache, "put_meta", _no_write)

    plan = FetchPlan()  # валидаторов нет — их обязан подставить адаптер
    page = crawler.fetch(URL, plan)

    # Живой условный запрос: заголовки дошли до транспорта, а не только до плана.
    assert len(fake.calls) == 1
    headers = fake.calls[0][1]
    assert headers["If-None-Match"] == '"v1"'
    assert headers["If-Modified-Since"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    # 304: байты из кэша, not_modified=True, пустого тела нет.
    assert page.not_modified is True
    assert page.status_code == 304
    assert page.content == b"cached-representation"
    assert page.etag == '"v1"'
    assert page.last_modified == "Wed, 21 Oct 2026 07:28:00 GMT"
    # План вызывающего не мутирован.
    assert plan.if_none_match is None
    assert plan.if_modified_since is None


def test_plan_validators_override_cache_meta(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    _seed_cache_with_validators(cache)
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"fresh-body", status_code=200)],
        cache,
    )

    page = crawler.fetch(URL, FetchPlan(if_none_match='"plan-version"'))

    headers = fake.calls[0][1]
    assert headers["If-None-Match"] == '"plan-version"'  # план, не '"v1"' из меты
    assert "If-Modified-Since" not in headers  # план не задавал — мета не подмешана
    assert page.content == b"fresh-body"
    assert page.not_modified is False


def test_cache_hit_with_meta_but_without_validators_is_served_from_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    cache.put_bytes("http", http_key(URL), b"cached-body")
    cache.put_meta(
        "http",
        http_key(URL),
        {
            "etag": None,
            "last_modified": None,
            "status_code": 200,
            "content_type": "text/html",
            "final_url": "https://example.com/final",
        },
    )
    fake = _FakeHttpx([])  # любое обращение к транспорту — ошибка теста
    monkeypatch.setattr(HttpCrawler, "_import_http", staticmethod(lambda: fake))
    crawler = HttpCrawler(cache=cache)

    page = crawler.fetch(URL)

    assert fake.calls == []  # валидаторов нет нигде → прежний кэш-шорткат
    assert page.content == b"cached-body"
    assert page.status_code == 200
    assert page.content_type == "text/html"
    assert page.url == "https://example.com/final"
    assert page.not_modified is False


def test_meta_fallback_304_without_cached_bytes_is_explicit_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Условный запрос возник из-за меты кэша, но тела нет — ошибка, не пустая страница.
    cache = ContentAddressedCache(tmp_path)
    cache.put_meta("http", http_key(URL), {"etag": '"v1"'})
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"", status_code=304)],
        cache,
    )

    with pytest.raises(CrawlError, match="304"):
        crawler.fetch(URL)

    assert fake.calls[0][1]["If-None-Match"] == '"v1"'


# ---------------------------------------------------------------------------
# 404/410 — определённый ответ «ресурса нет» (not_found), не недоступность
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [404, 410])
def test_not_found_status_maps_to_typed_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    """404/410 классифицируются до ``raise_for_status`` и не пишутся в кэш."""
    cache = ContentAddressedCache(tmp_path)
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"missing", status_code=status)],
        cache,
    )

    with pytest.raises(CrawlNotFound) as excinfo:
        crawler.fetch(URL)

    assert excinfo.value.failure_kind == FailureKind.NOT_FOUND.value
    assert excinfo.value.failure_kind == "not_found"
    # Один запрос: определённый ответ не повторяется транспортом.
    assert len(fake.calls) == 1
    # Ответ «нет ресурса» — не представление: в кэш ничего не пишется.
    assert cache.get_bytes("http", http_key(URL)) is None


def test_server_error_stays_generic_source_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """500 — по-прежнему общий ``CrawlError`` (pipeline → ``source_unavailable``)."""
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"boom", status_code=500)],
        ContentAddressedCache(tmp_path),
    )

    with pytest.raises(CrawlError) as excinfo:
        crawler.fetch(URL)

    assert not isinstance(excinfo.value, CrawlNotFound)
    assert getattr(excinfo.value, "failure_kind", None) is None
    assert len(fake.calls) == 1


def test_forbidden_is_unchanged_generic_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """403 не классифицируется как ``not_found`` (прежний путь не меняется)."""
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"denied", status_code=403)],
        ContentAddressedCache(tmp_path),
    )

    with pytest.raises(CrawlError) as excinfo:
        crawler.fetch(URL)

    assert not isinstance(excinfo.value, CrawlNotFound)
    assert getattr(excinfo.value, "failure_kind", None) is None
    assert len(fake.calls) == 1





# ---------------------------------------------------------------------------
# P0/OOM: потолок ответа (axis crawl.max_response_bytes)
# ---------------------------------------------------------------------------


def test_default_max_response_bytes_is_20_mib() -> None:
    from core.adapters.crawl_http import DEFAULT_MAX_RESPONSE_BYTES

    assert DEFAULT_MAX_RESPONSE_BYTES == 20_971_520
    assert HttpCrawler(_counting_fetcher([]))._max_response_bytes == 20_971_520


def test_oversize_by_content_length_is_typed_and_not_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    crawler, fake = _crawler_with_httpx(
        monkeypatch,
        [
            _FakeResponse(
                url=URL,
                content=b"x" * 10,
                status_code=200,
                content_length=1_000_000,
            )
        ],
        cache,
    )

    with pytest.raises(CrawlPayloadTooLarge) as excinfo:
        crawler.fetch(URL, FetchPlan(max_response_bytes=128))

    assert getattr(excinfo.value, "failure_kind", None) == "payload_too_large"
    assert cache.get_bytes("http", http_key(URL)) is None
    assert len(fake.calls) == 1


def test_oversize_without_content_length_aborts_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = ContentAddressedCache(tmp_path)
    crawler, _fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"y" * 4096, status_code=200)],
        cache,
    )

    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(URL, FetchPlan(max_response_bytes=128))

    assert cache.get_bytes("http", http_key(URL)) is None


def test_payload_within_cap_is_returned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crawler, _fake = _crawler_with_httpx(
        monkeypatch,
        [_FakeResponse(url=URL, content=b"ok", status_code=200)],
        ContentAddressedCache(tmp_path),
    )
    assert crawler.fetch(URL, FetchPlan(max_response_bytes=2)).content == b"ok"


def test_injected_fetcher_respects_cap() -> None:
    crawler = HttpCrawler(_counting_fetcher([], b"z" * 100), max_response_bytes=10)
    with pytest.raises(CrawlPayloadTooLarge):
        crawler.fetch(URL)
