"""Юнит-тесты краулера ``core/adapters/crawl_scrapy.py`` (W7/T7.1).

Сеть и браузеры не используются: внедряется ``fetcher`` (шов), кэш — ``tmp_path``.
Проверяются: интеграция с content-addressed кэшем (промах → фетч → запись →
попадание без повторного фетча), per-domain рендер-профили, проброс
``RenderMode.BROWSER``, SSRF-отказ до фетча, устойчивый импорт без Scrapy,
``version`` и структурное соответствие ``CrawlerPort``. Данные нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.adapters.crawl_scrapy import CrawlError, CrawlUrlError, ScrapyCrawler
from core.config.run_config import load_domain_profiles
from core.ports.crawler import CrawlerPort
from core.ports.types import RawPage, RenderMode
from core.util.cache import ContentAddressedCache, http_key

FIXTURES = Path(__file__).parent / "fixtures"
FIXED_TS = "2026-01-01T00:00:00+00:00"
URL = "https://example.com/page"


def _page(url: str, content: bytes = b"payload", render: RenderMode = RenderMode.HTTP) -> RawPage:
    return RawPage(url=url, content=content, fetched_at=FIXED_TS, render_mode=render)


class _RecordingFetcher:
    """Фетчер-объект с методом ``fetch``; записывает ``(url, render)`` вызовы."""

    def __init__(self, content: bytes = b"payload") -> None:
        self.calls: list[tuple[str, RenderMode]] = []
        self.content = content

    def fetch(self, url: str, render: RenderMode = RenderMode.HTTP) -> RawPage:
        self.calls.append((url, render))
        return _page(url, self.content, render)


def _counting_fetcher(
    calls: list[tuple[str, RenderMode]], content: bytes = b"payload"
):
    """Callable-фетчер ``(url, render) -> RawPage``, пишущий вызовы в ``calls``."""

    def fetch(url: str, render: RenderMode = RenderMode.HTTP) -> RawPage:
        calls.append((url, render))
        return _page(url, content, render)

    return fetch


# ---------------------------------------------------------------------------
# Кэш (R7, ADR 0016)
# ---------------------------------------------------------------------------


def test_cache_miss_fetches_and_second_call_served_from_cache(tmp_path: Path) -> None:
    calls: list[tuple[str, RenderMode]] = []
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
    calls: list[tuple[str, RenderMode]] = []
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


def test_browser_render_bypasses_cache(tmp_path: Path) -> None:
    cache = _SpyCache(tmp_path)
    fetcher = _RecordingFetcher(b"live-body")
    crawler = ScrapyCrawler(fetcher, cache=cache)

    page = crawler.fetch(URL, render=RenderMode.BROWSER)

    assert page.content == b"live-body"
    assert fetcher.calls == [(URL, RenderMode.BROWSER)]
    # Браузерный рендер не читает и не пишет кэш (нет кросс-контаминации HTTP/browser).
    assert cache.gets == []
    assert cache.puts == []


def test_browser_render_always_live_even_with_cache(tmp_path: Path) -> None:
    cache = _SpyCache(tmp_path)
    # Предзаполняем кэш под тем же ключом — браузер обязан его игнорировать.
    cache.put_bytes("http", http_key(URL), b"cached-body")
    cache.gets.clear()
    cache.puts.clear()
    fetcher = _RecordingFetcher(b"live-body")
    crawler = ScrapyCrawler(fetcher, cache=cache)

    first = crawler.fetch(URL, render=RenderMode.BROWSER)
    second = crawler.fetch(URL, render=RenderMode.BROWSER)

    assert first.content == b"live-body" and second.content == b"live-body"
    assert len(fetcher.calls) == 2
    assert cache.gets == [] and cache.puts == []


def test_http_render_uses_cache(tmp_path: Path) -> None:
    cache = _SpyCache(tmp_path)
    crawler = ScrapyCrawler(_RecordingFetcher(b"live-body"), cache=cache)

    crawler.fetch(URL, render=RenderMode.HTTP)

    assert cache.gets and cache.puts
    assert cache.gets[0] == ("http", http_key(URL))
    assert cache.puts[0] == ("http", http_key(URL))


# ---------------------------------------------------------------------------
# Рендер-профили по домену
# ---------------------------------------------------------------------------


def test_render_profile_overrides_default_argument() -> None:
    fetcher = _RecordingFetcher()
    crawler = ScrapyCrawler(
        fetcher, domain_profiles={"example.com": {"render": "browser"}}
    )
    crawler.fetch(URL, render=RenderMode.HTTP)
    assert fetcher.calls == [(URL, RenderMode.BROWSER)]


def test_unknown_domain_falls_back_to_argument() -> None:
    fetcher = _RecordingFetcher()
    crawler = ScrapyCrawler(
        fetcher, domain_profiles={"example.com": {"render": "browser"}}
    )
    crawler.fetch("https://other.test/page", render=RenderMode.BROWSER)
    assert fetcher.calls == [("https://other.test/page", RenderMode.BROWSER)]


def test_browser_mode_passthrough_without_profile() -> None:
    fetcher = _RecordingFetcher()
    crawler = ScrapyCrawler(fetcher)
    page = crawler.fetch(URL, render=RenderMode.BROWSER)
    assert fetcher.calls == [(URL, RenderMode.BROWSER)]
    assert page.render_mode is RenderMode.BROWSER


def test_render_for_exact_suffix_nested_and_bare() -> None:
    crawler = ScrapyCrawler(
        _RecordingFetcher(),
        domain_profiles={
            "example.com": {"render": "browser"},
            "nested.example": {"render": {"mode": "http"}},
            "bare.example": "browser",
        },
    )
    assert crawler.render_for("https://example.com/a") is RenderMode.BROWSER
    # суффиксное совпадение: профиль родительского домена применяется к поддомену
    assert crawler.render_for("https://www.example.com/a") is RenderMode.BROWSER
    # вложенная форма render.mode
    assert crawler.render_for("https://nested.example/a") is RenderMode.HTTP
    # bare-строка
    assert crawler.render_for("https://bare.example/a") is RenderMode.BROWSER
    # неизвестный домен → дефолт
    assert crawler.render_for("https://other.test/a") is RenderMode.HTTP


def test_domain_profiles_loaded_from_yaml_fixture() -> None:
    profiles = load_domain_profiles(FIXTURES / "domain_profiles.yaml")
    crawler = ScrapyCrawler(_RecordingFetcher(), domain_profiles=profiles)
    assert crawler.render_for("https://example.com/a") is RenderMode.BROWSER
    assert crawler.render_for("https://static.example/a") is RenderMode.HTTP
    assert crawler.render_for("https://nested.example/a") is RenderMode.BROWSER


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
    def boom(url: str, render: RenderMode = RenderMode.HTTP) -> RawPage:
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


def test_module_importable_without_scrapy() -> None:
    # Импорт самого модуля не тянет тяжёлые пакеты; реальный путь — ленивый.
    import core.adapters.crawl_scrapy as mod

    assert "scrapy" not in getattr(mod, "__dict__", {})


def test_available_true_with_injected_fetcher() -> None:
    assert ScrapyCrawler(_RecordingFetcher()).available is True
    assert isinstance(ScrapyCrawler().available, bool)


def test_real_path_without_scrapy_raises_crawl_error(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.adapters.crawl_scrapy as mod

    monkeypatch.setattr(mod, "_scrapy_available", lambda: False)
    crawler = ScrapyCrawler()
    with pytest.raises(CrawlError):
        crawler.fetch(URL)


def test_version_and_port_conformance() -> None:
    crawler = ScrapyCrawler(_RecordingFetcher())
    assert isinstance(crawler.version, str) and crawler.version
    assert isinstance(crawler, CrawlerPort)
    assert callable(crawler.fetch)


def test_scrapy_available_matches_environment() -> None:
    import core.adapters.crawl_scrapy as mod

    assert mod._scrapy_available() is _spec_present("scrapy")


def _spec_present(name: str) -> bool:
    import importlib.util

    return importlib.util.find_spec(name) is not None
