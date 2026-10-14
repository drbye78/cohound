"""Тесты проводки R7-кэша и домен-профилей в краулер корпуса (composition root Z0).

Проверяется, что операционные входы доходят от CLI до адаптера краулера:

* :func:`build_scout_crawler` — ``cache`` протягивается в оба краулера
  (``HttpCrawler``/``ScrapyCrawler``); ``domain_profiles`` на проводке не блокируют
  ``http`` (исполнимость browser-URL проверяется URL-скоупедированно в pipeline, D2);
* :func:`build_adapters` — оба параметра доходят до ``ports.crawler`` (профиль ``local``),
  а без ``deploy`` краулер остаётся ``None`` (обратная совместимость); профиль ``split``
  **не** держит локальных веб-краулеров (``Ports.crawlers is None``, только remote-шов, B1);
* :func:`_resolve_ports` — строит ``ContentAddressedCache(paths.cache)`` из resolved
  раскладки и передаёт ``inputs.domain_profiles`` в ``build_adapters``; без ``inputs``
  домен-профили не передаются; без ``paths`` кэш не строится; инъекция ``ports``
  по-прежнему шунтирует сборку;
* ``run`` — best-effort закрытие краулера в ``finally`` (если есть ``close``) и
  безопасность, когда ``close`` нет.

Данные категорийно-нейтральны (§1.3): домены-примеры и профили — просто данные.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import core.cli.context as context_mod
import core.cli.wiring as wiring_mod
from core.adapters.crawl_browser import (
    DEFAULT_TIMEOUT_MS as DEFAULT_BROWSER_TIMEOUT_MS,
    DEFAULT_USER_AGENT as DEFAULT_BROWSER_USER_AGENT,
    BrowserCrawler,
)
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import ScrapyCrawler
from core.adapters.feed_poller import LocalFeedPoller, RemoteFeedPoller
from core.adapters.remote_crawl_executor import EdgeCrawlExecutor
from core.adapters.remote_crawler import RemoteCrawler
from core.cli.context import _resolve_ports
from core.cli.main import EXIT_BLOCKED, main
from core.cli.wiring import (
    ENV_SCOUT_CRAWLER,
    LOCAL_PROFILE,
    SPLIT_PROFILE,
    SUPPORTED_SCOUT_CRAWLERS,
    WiringError,
    build_adapters,
    build_scout_crawler,
    build_scout_crawlers,
)
from core.config.crawl_sources import CrawlSource, CrawlSourcesConfig
from core.config.deploy import CrawlBrowserConfig, CrawlConfig, DeployConfig
from core.config.domain_profiles import DomainProfile
from core.config.paths import PathsConfig
from core.domain.enums import ExtractionRoute, FetchMode
from core.pipeline import Ports
from core.pipeline.normalize import RunInputs
from core.util.cache import ContentAddressedCache
from tests.helpers.packages import run_argv, valid_package
from tests.helpers.scout import clear_env


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _cache(tmp_path: Path) -> ContentAddressedCache:
    """Свежий кэш в изолированном каталоге теста (без записи)."""
    return ContentAddressedCache(tmp_path / "cache")


def _profiles() -> dict[str, DomainProfile]:
    """Категорийно-нейтральные доменные профили (данные, §D.2) без browser-оси."""
    return {
        "example.com": DomainProfile(fetch=FetchMode.HTTP, extract=ExtractionRoute.HTML),
        "static.example.org": DomainProfile(fetch=FetchMode.HTTP),
    }


def _browser_profiles() -> dict[str, DomainProfile]:
    """Профили с browser-осью (проводка ``http`` их больше не блокирует, D2)."""
    return {
        "spa.example.com": DomainProfile(fetch=FetchMode.BROWSER),
        "example.com": DomainProfile(fetch=FetchMode.HTTP),
    }


class _ClosableCrawler:
    """Минимальная заглушка ``CrawlerPort`` с учётом вызовов ``close``."""

    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


# ---------------------------------------------------------------------------
# build_scout_crawler: cache + domain_profiles
# ---------------------------------------------------------------------------


def test_build_scout_crawler_scrapy_threads_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``scrapy``: кэш доходит до ``ScrapyCrawler``; профили ему не нужны (резолв в pipeline)."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "scrapy")
    cache = _cache(tmp_path)

    crawler = build_scout_crawler(cache=cache, domain_profiles=_profiles())

    assert isinstance(crawler, ScrapyCrawler)
    assert crawler._cache is cache


def test_build_scout_crawler_http_allows_browser_profile_at_wiring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``http`` + профиль ``fetch: browser`` не падает на проводке: браузер проверяется по URL в pipeline (D2)."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "http")
    cache = _cache(tmp_path)

    crawler = build_scout_crawler(
        deploy=DeployConfig(profile="local"),
        cache=cache,
        domain_profiles=_browser_profiles(),
    )

    assert isinstance(crawler, HttpCrawler)
    assert crawler._cache is cache


def test_build_scout_crawler_unknown_value_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Неизвестное значение ``COHOUND_SCOUT_CRAWLER`` по-прежнему → ``WiringError``."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "nope")

    with pytest.raises(WiringError, match="неизвестный краулер"):
        build_scout_crawler()


def test_build_scout_crawler_http_threads_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``http``: кэш доходит; профили не влияют на выбор краулера (D2)."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "http")
    cache = _cache(tmp_path)

    crawler = build_scout_crawler(
        deploy=DeployConfig(profile="local"),
        cache=cache,
        domain_profiles=_profiles(),
    )

    assert isinstance(crawler, HttpCrawler)
    assert crawler._cache is cache
    assert not hasattr(crawler, "_profiles")


def test_build_scout_crawler_default_is_http_with_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без env (дефолт ``http``): ``HttpCrawler`` с внедрённым кэшем."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])
    cache = _cache(tmp_path)

    crawler = build_scout_crawler(cache=cache)

    assert isinstance(crawler, HttpCrawler)
    assert crawler._cache is cache


def test_build_scout_crawler_no_args_backward_compatible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Вызов без аргументов не изменён: ``HttpCrawler`` без кэша (прежнее поведение)."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    crawler = build_scout_crawler()

    assert isinstance(crawler, HttpCrawler)
    assert crawler._cache is None


# ---------------------------------------------------------------------------
# build_scout_crawler: browser (BrowserCrawler, honest/hybrid)
# ---------------------------------------------------------------------------


def test_supported_scout_crawlers_include_browser() -> None:
    """Реестр краулеров расширен значением ``browser``."""
    assert "browser" in SUPPORTED_SCOUT_CRAWLERS


def test_build_scout_crawler_browser_threads_cache_and_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``browser``: ``BrowserCrawler`` с протянутым кэшем и полями ``deploy.crawl.browser``."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "browser")
    cache = _cache(tmp_path)
    deploy = DeployConfig(
        profile="local",
        crawl=CrawlConfig(
            browser=CrawlBrowserConfig(
                mode="cdp",
                cdp_url="http://127.0.0.1:9222",
                headless=True,
                timeout_ms=12345,
            )
        ),
    )

    crawler = build_scout_crawler(deploy=deploy, cache=cache)

    assert isinstance(crawler, BrowserCrawler)
    assert crawler.supports_browser is True
    assert crawler._cache is cache
    assert crawler._cdp_url == "http://127.0.0.1:9222"
    assert crawler._headless is True
    assert crawler._timeout_ms == 12345


def test_build_scout_crawler_browser_persistent_and_user_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``persistent``-ветка: ``user_data_dir``/``user_agent`` доезжают; ``cdp_url`` пуст."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "browser")
    deploy = DeployConfig(
        profile="local",
        crawl=CrawlConfig(
            user_agent="ua/9",
            browser=CrawlBrowserConfig(mode="persistent", user_data_dir="/tmp/profile-x"),
        ),
    )

    crawler = build_scout_crawler(deploy=deploy)

    assert isinstance(crawler, BrowserCrawler)
    assert crawler._user_data_dir == "/tmp/profile-x"
    assert crawler._user_agent == "ua/9"
    assert crawler._cdp_url is None


def test_build_scout_crawler_browser_empty_config_uses_adapter_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пустой ``crawl.browser`` (``mode=None``) → дефолты адаптера, без падения."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "browser")

    crawler = build_scout_crawler(deploy=DeployConfig(profile="local"))

    assert isinstance(crawler, BrowserCrawler)
    assert crawler._cdp_url is None
    assert crawler._user_data_dir is None
    assert crawler._headless is False
    assert crawler._timeout_ms == DEFAULT_BROWSER_TIMEOUT_MS
    assert crawler._user_agent == DEFAULT_BROWSER_USER_AGENT
    assert crawler._rate_limiter is None  # пейсинг — в corpus, не в wiring


def test_build_scout_crawler_browser_without_deploy_uses_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``deploy=None`` (нет канонического файла) → дефолты адаптера, без падения."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "browser")
    monkeypatch.chdir(tmp_path)  # нет config/deploy.yaml → канон-дефолт profile local

    crawler = build_scout_crawler()

    assert isinstance(crawler, BrowserCrawler)
    assert crawler._cache is None
    assert crawler._cdp_url is None
    assert crawler._timeout_ms == DEFAULT_BROWSER_TIMEOUT_MS


def test_build_scout_crawler_browser_with_browser_profile_not_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Профиль ``fetch: browser`` на проводке не блокируется (исполнимость — по URL, D2)."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "browser")

    crawler = build_scout_crawler(
        deploy=DeployConfig(profile="local"),
        cache=_cache(tmp_path),
        domain_profiles=_browser_profiles(),
    )

    assert isinstance(crawler, BrowserCrawler)


# ---------------------------------------------------------------------------
# build_adapters: проброс в ports.crawler
# ---------------------------------------------------------------------------


def test_build_adapters_threads_cache_and_profiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` + ``scrapy``: кэш доходит до ``ports.crawler`` (профили — fail-fast/резолв)."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "scrapy")
    cache = _cache(tmp_path)

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        deploy=DeployConfig(profile="local"),
        cache=cache,
        domain_profiles=_profiles(),
    )

    assert isinstance(ports.crawler, ScrapyCrawler)
    assert ports.crawler._cache is cache


def test_build_adapters_http_threads_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` + ``http``: кэш доходит до ``ports.crawler``."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "http")
    cache = _cache(tmp_path)

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        deploy=DeployConfig(profile="local"),
        cache=cache,
        domain_profiles=_profiles(),
    )

    assert isinstance(ports.crawler, HttpCrawler)
    assert ports.crawler._cache is cache


def test_build_adapters_without_deploy_keeps_crawler_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без ``deploy`` краулер не инстанцируется (обратная совместимость)."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "scrapy")

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        cache=_cache(tmp_path),
        domain_profiles=_profiles(),
    )

    assert ports.crawler is None


def test_build_adapters_browser_threads_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` + ``browser``: ``BrowserCrawler`` с кэшем доходит до ``ports.crawler``."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "browser")
    cache = _cache(tmp_path)

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        deploy=DeployConfig(profile="local"),
        cache=cache,
        domain_profiles=_browser_profiles(),
    )

    assert isinstance(ports.crawler, BrowserCrawler)
    assert ports.crawler._cache is cache


# ---------------------------------------------------------------------------
# _resolve_ports: ContentAddressedCache + inputs.domain_profiles
# ---------------------------------------------------------------------------


def _ports_args(paths: Any) -> argparse.Namespace:
    """Аргументы ``run``-команды, минимально необходимые :func:`_resolve_ports`."""
    return argparse.Namespace(
        profile="local",
        ports=None,
        gateway_factory=None,
        deploy_transport=None,
        paths=paths,
    )


def test_resolve_ports_builds_cache_and_forwards_profiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Кэш строится от ``paths.cache``; ``inputs.domain_profiles`` уходит в билдер."""
    cache_dir = tmp_path / "cache_root"
    profiles = _profiles()
    inputs = cast(RunInputs, SimpleNamespace(domain_profiles=profiles))
    captured: dict[str, Any] = {}
    sentinel = object()

    def fake_build_adapters(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(context_mod, "build_adapters", fake_build_adapters)

    result = _resolve_ports(
        _ports_args(PathsConfig(cache=cache_dir)),
        deploy_config=None,
        inputs=inputs,
    )  # type: ignore[arg-type]

    assert result is sentinel
    cache = captured["cache"]
    assert isinstance(cache, ContentAddressedCache)
    assert cache.root == cache_dir
    assert captured["domain_profiles"] == profiles


def test_resolve_ports_no_cache_bypasses_r7_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Phase 4 ``--from-scratch``: ``no_cache=True`` → ``build_adapters(cache=None)``."""
    captured: dict[str, Any] = {}

    def fake_build_adapters(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(context_mod, "build_adapters", fake_build_adapters)

    _resolve_ports(
        _ports_args(PathsConfig(cache=tmp_path / "cache")),
        deploy_config=None,
        no_cache=True,
    )

    assert captured["cache"] is None


def test_resolve_ports_without_inputs_forwards_none_profiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без ``inputs`` (дефолт ``None``) домен-профили не передаются — регрессия run-пути."""
    captured: dict[str, Any] = {}

    def fake_build_adapters(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(context_mod, "build_adapters", fake_build_adapters)

    _resolve_ports(
        _ports_args(PathsConfig(cache=tmp_path / "cache")), deploy_config=None
    )

    assert isinstance(captured["cache"], ContentAddressedCache)
    assert captured["domain_profiles"] is None


def test_resolve_ports_without_paths_builds_no_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без resolved-раскладки (``paths=None``) кэш не строится."""
    captured: dict[str, Any] = {}

    def fake_build_adapters(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(context_mod, "build_adapters", fake_build_adapters)

    _resolve_ports(_ports_args(None), deploy_config=None)

    assert captured["cache"] is None
    assert captured["domain_profiles"] is None


def test_resolve_ports_injected_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Инъекция ``ports`` по-прежнему шунтирует ``build_adapters`` (контракт сохранён)."""
    injected = object()

    def fail_build_adapters(**kwargs: Any) -> Any:  # pragma: no cover - не должен зваться
        raise AssertionError("build_adapters не должен вызываться при инъекции ports")

    monkeypatch.setattr(context_mod, "build_adapters", fail_build_adapters)

    assert _resolve_ports(
        argparse.Namespace(ports=injected), deploy_config=None
    ) is injected


# ---------------------------------------------------------------------------
# run: best-effort закрытие краулера в finally
# ---------------------------------------------------------------------------


def test_run_closes_crawler_with_close_in_finally(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``run`` закрывает краулер с ``close`` в ``finally`` (останов на гейте ``g1``)."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    crawler = _ClosableCrawler()
    ports = Ports(crawler=crawler)  # type: ignore[arg-type]

    code = main(
        run_argv(package, run_id="close-crawler", db=tmp_path / "c.db"),
        ports=ports,
    )

    assert code == EXIT_BLOCKED  # g1 без человеческого решения (I6)
    assert crawler.closed == 1


def test_run_tolerates_crawler_without_close_in_finally(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Краулер без ``close`` (напр. ``HttpCrawler``) не ломает ``finally``."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    ports = Ports(crawler=HttpCrawler())  # HTTP-краулер без ``close``

    code = main(
        run_argv(package, run_id="no-close-crawler", db=tmp_path / "c.db"),
        ports=ports,
    )

    assert code == EXIT_BLOCKED


# ---------------------------------------------------------------------------
# build_scout_crawlers: карта engine → CrawlerPort (L3a, per-engine выбор в Ш5)
# ---------------------------------------------------------------------------


def test_build_scout_crawlers_always_includes_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``http`` строится всегда — дефолтный движок (без env/профилей)."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    crawlers = build_scout_crawlers()

    assert set(crawlers) == {"http"}
    assert isinstance(crawlers["http"], HttpCrawler)


def test_build_scout_crawlers_omits_unreferenced_heavy_engines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без упоминания ``scrapy``/``browser`` тяжёлые адаптеры не строятся (A1)."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    crawlers = build_scout_crawlers(
        cache=_cache(tmp_path), domain_profiles=_profiles()
    )

    assert "scrapy" not in crawlers
    assert "browser" not in crawlers


def test_build_scout_crawlers_threads_cache_into_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """R7-кэш доходит до ``http`` краулера карты."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])
    cache = _cache(tmp_path)

    crawlers = build_scout_crawlers(cache=cache)

    assert crawlers["http"]._cache is cache


def test_build_scout_crawlers_browser_referenced_by_domain_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``fetch: browser`` в доменном профиле помечает браузерный движок → он строится (без Playwright)."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])
    cache = _cache(tmp_path)

    crawlers = build_scout_crawlers(
        deploy=DeployConfig(profile="local"),
        cache=cache,
        domain_profiles=_browser_profiles(),
    )

    assert isinstance(crawlers.get("browser"), BrowserCrawler)
    assert crawlers["browser"]._cache is cache


def test_build_scout_crawlers_browser_referenced_by_crawl_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Поле ``engine: browser`` в ``crawl_sources`` помечает браузерный движок."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])
    cfg = CrawlSourcesConfig(defaults=CrawlSource(engine="browser"))

    crawlers = build_scout_crawlers(
        deploy=DeployConfig(profile="local"), crawl_sources=cfg
    )

    assert isinstance(crawlers.get("browser"), BrowserCrawler)


def test_build_scout_crawlers_scrapy_referenced_by_crawl_sources_site_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``engine: scrapy`` в site-слое ``crawl_sources`` помечает scrapy-движок."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])
    cfg = CrawlSourcesConfig(
        sites={"p1": {"example.com": CrawlSource(engine="scrapy")}}
    )

    crawlers = build_scout_crawlers(
        deploy=DeployConfig(profile="local"), crawl_sources=cfg
    )

    assert isinstance(crawlers.get("scrapy"), ScrapyCrawler)


def test_build_scout_crawlers_engine_referenced_by_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Явный env-выбор ``COHOUND_SCOUT_CRAWLER=scrapy`` помечает движок."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "scrapy")

    crawlers = build_scout_crawlers(deploy=DeployConfig(profile="local"))

    assert isinstance(crawlers.get("scrapy"), ScrapyCrawler)


def test_build_scout_crawlers_omits_engine_when_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Недоступность зависимости при сборке движка опускает его, не валя всю сборку."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    class _Boom:
        def __init__(self, **kwargs: Any) -> None:
            raise ImportError("нет playwright")

    monkeypatch.setattr(wiring_mod, "BrowserCrawler", _Boom)

    crawlers = build_scout_crawlers(
        deploy=DeployConfig(profile="local"), domain_profiles=_browser_profiles()
    )

    assert "http" in crawlers  # лёгкий движок жив
    assert "browser" not in crawlers  # тяжёлый опущен


def test_build_scout_crawlers_omits_engine_on_wiring_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``WiringError`` при сборке движка тоже опускает только этот движок."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    class _Boom:
        def __init__(self, **kwargs: Any) -> None:
            raise WiringError("плохой конфиг")

    monkeypatch.setattr(wiring_mod, "BrowserCrawler", _Boom)

    crawlers = build_scout_crawlers(
        deploy=DeployConfig(profile="local"), domain_profiles=_browser_profiles()
    )

    assert "http" in crawlers
    assert "browser" not in crawlers


# ---------------------------------------------------------------------------
# build_adapters / _resolve_ports: Ports.crawlers (L3a)
# ---------------------------------------------------------------------------


def test_build_adapters_local_sets_crawlers_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` заполняет ``Ports.crawlers`` (минимум ``http``)."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        cache=_cache(tmp_path),
        domain_profiles=_profiles(),
    )

    assert ports.crawlers is not None
    assert isinstance(ports.crawlers["http"], HttpCrawler)
    assert "browser" not in ports.crawlers and "scrapy" not in ports.crawlers


def test_build_adapters_local_builds_browser_when_referenced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` + ``engine: browser`` в ``crawl_sources`` → ``Ports.crawlers`` несёт браузер."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])
    cfg = CrawlSourcesConfig(defaults=CrawlSource(engine="browser"))

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        deploy=DeployConfig(profile="local"),
        cache=_cache(tmp_path),
        crawl_sources=cfg,
    )

    assert ports.crawlers is not None
    assert isinstance(ports.crawlers.get("browser"), BrowserCrawler)


def test_build_adapters_split_has_no_local_crawlers() -> None:
    """B1 (I18/07 §4–§5): ``split`` НЕ держит локальные веб-краулеры — только remote-шов.

    Per-engine движок из ``crawl_sources`` не должен превращаться в in-process
    ``HttpCrawler``/``BrowserCrawler`` (прямой веб-эгресс из ядра). ``Ports.crawlers`` —
    ``None``; падение на ``ports.crawler`` — ``RemoteCrawler`` (шов транспорт-агностичен).
    """
    config = DeployConfig.model_validate(
        {"profile": "split", "edge": {"base_url": "https://edge.test"}}
    )
    gateway = SimpleNamespace()  # RemoteCrawler/Search только хранят шов, не вызывают

    ports = build_adapters(profile=SPLIT_PROFILE, deploy=config, gateway=gateway)  # type: ignore[arg-type]

    assert ports.crawlers is None
    assert isinstance(ports.crawler, RemoteCrawler)


def test_build_adapters_split_has_no_local_web_crawlers_explicit() -> None:
    """B1: в ``split`` ни один локальный веб-краулер не достижим через ``Ports``."""
    config = DeployConfig.model_validate(
        {"profile": "split", "edge": {"base_url": "https://edge.test"}}
    )
    gateway = SimpleNamespace()

    ports = build_adapters(profile=SPLIT_PROFILE, deploy=config, gateway=gateway)  # type: ignore[arg-type]

    crawlers = ports.crawlers or {}
    for local_type in (HttpCrawler, BrowserCrawler, ScrapyCrawler):
        assert not isinstance(ports.crawler, local_type)
        assert not any(isinstance(c, local_type) for c in crawlers.values())


# ---------------------------------------------------------------------------
# Phase 3.3: Ports.executor (EdgeCrawlExecutor) — split vs local
# ---------------------------------------------------------------------------


def test_build_adapters_split_sets_executor() -> None:
    """``split`` собирает ``EdgeCrawlExecutor`` поверх шва (``Ports.executor``)."""
    config = DeployConfig.model_validate(
        {"profile": "split", "edge": {"base_url": "https://edge.test"}}
    )
    gateway = SimpleNamespace()

    ports = build_adapters(profile=SPLIT_PROFILE, deploy=config, gateway=gateway)  # type: ignore[arg-type]

    assert isinstance(ports.executor, EdgeCrawlExecutor)
    assert ports.executor._gateway is gateway
    # ``crawler`` оставлен для обратной совместимости (non-executor путь).
    assert isinstance(ports.crawler, RemoteCrawler)


def test_build_adapters_local_executor_is_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` не собирает исполнителя: ``Ports.executor is None`` (прежний путь)."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        cache=_cache(tmp_path),
        domain_profiles=_profiles(),
    )

    assert ports.executor is None


def test_resolve_ports_forwards_crawl_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_resolve_ports`` передаёт ``inputs.crawl_sources`` в ``build_adapters``."""
    cfg = CrawlSourcesConfig(defaults=CrawlSource(engine="browser"))
    inputs = cast(
        RunInputs, SimpleNamespace(domain_profiles={}, crawl_sources=cfg)
    )
    captured: dict[str, Any] = {}

    def fake_build_adapters(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(context_mod, "build_adapters", fake_build_adapters)

    _resolve_ports(
        _ports_args(PathsConfig(cache=tmp_path / "cache")),
        deploy_config=None,
        inputs=inputs,
    )

    assert captured["crawl_sources"] is cfg


def test_resolve_ports_without_inputs_forwards_none_crawl_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без ``inputs`` слой ``crawl_sources`` не передаётся (обратная совместимость)."""
    captured: dict[str, Any] = {}

    def fake_build_adapters(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(context_mod, "build_adapters", fake_build_adapters)

    _resolve_ports(_ports_args(PathsConfig(cache=tmp_path / "cache")), deploy_config=None)

    assert captured["crawl_sources"] is None


def test_resolve_ports_real_build_sets_ports_crawlers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Реальная сборка ``_resolve_ports`` даёт ``Ports.crawlers`` c ``http``."""
    monkeypatch.chdir(tmp_path)  # нет config/deploy.yaml → канон-дефолт profile local
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])
    inputs = cast(
        RunInputs,
        SimpleNamespace(domain_profiles={}, crawl_sources=CrawlSourcesConfig()),
    )

    ports = _resolve_ports(
        _ports_args(PathsConfig(cache=tmp_path / "cache")),
        deploy_config=None,
        inputs=inputs,
    )

    assert ports.crawlers is not None
    assert "http" in ports.crawlers
    assert isinstance(ports.crawlers["http"], HttpCrawler)


# ---------------------------------------------------------------------------
# Phase 5.1: Ports.pollers (LocalFeedPoller / RemoteFeedPoller) — local vs split
# ---------------------------------------------------------------------------


def test_build_adapters_local_sets_pollers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` + ``deploy`` → ``Ports.pollers`` несёт ``LocalFeedPoller`` для feed/channel."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "http")

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        deploy=DeployConfig(profile="local"),
        cache=_cache(tmp_path),
        domain_profiles=_profiles(),
    )

    assert ports.pollers is not None
    assert set(ports.pollers) == {"feed", "channel"}
    assert all(isinstance(poller, LocalFeedPoller) for poller in ports.pollers.values())
    # Поллер использует тот же локальный краулер (транспорт фетча ленты).
    assert ports.pollers["feed"]._crawler is ports.crawler


def test_build_adapters_local_without_deploy_has_no_pollers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без ``deploy`` нет краулера → поллеры не собираются (``None``)."""
    clear_env(monkeypatch, [ENV_SCOUT_CRAWLER])

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        cache=_cache(tmp_path),
        domain_profiles=_profiles(),
    )

    assert ports.crawler is None
    assert ports.pollers is None


def test_build_adapters_split_sets_remote_pollers() -> None:
    """``split`` → ``Ports.pollers`` несёт ``RemoteFeedPoller`` поверх шва."""
    config = DeployConfig.model_validate(
        {"profile": "split", "edge": {"base_url": "https://edge.test"}}
    )
    gateway = SimpleNamespace()

    ports = build_adapters(profile=SPLIT_PROFILE, deploy=config, gateway=gateway)  # type: ignore[arg-type]

    assert ports.pollers is not None
    assert set(ports.pollers) == {"feed", "channel"}
    assert all(isinstance(poller, RemoteFeedPoller) for poller in ports.pollers.values())
    assert ports.pollers["feed"]._gateway is gateway


def test_build_adapters_local_pollers_are_ports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Поллеры удовлетворяют ``PollPort`` (конформность на проводке)."""
    from core.ports.poller import PollPort

    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "http")
    ports = build_adapters(
        profile=LOCAL_PROFILE,
        deploy=DeployConfig(profile="local"),
        cache=_cache(tmp_path),
        domain_profiles=_profiles(),
    )

    assert ports.pollers is not None
    assert all(isinstance(poller, PollPort) for poller in ports.pollers.values())
