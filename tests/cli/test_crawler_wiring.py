"""Тесты проводки R7-кэша и домен-профилей в краулер корпуса (composition root Z0).

Проверяется, что операционные входы доходят от CLI до адаптера краулера:

* :func:`build_scout_crawler` — ``cache`` протягивается в оба краулера
  (``HttpCrawler``/``ScrapyCrawler``), ``domain_profiles`` — **только** в браузерный
  ``ScrapyCrawler`` (HTTP-краулер их не принимает, R7/03 §D.5);
* :func:`build_adapters` — оба параметра доходят до ``ports.crawler`` (профиль ``local``),
  а без ``deploy`` краулер остаётся ``None`` (обратная совместимость);
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
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import ScrapyCrawler
from core.cli.context import _resolve_ports
from core.cli.main import EXIT_BLOCKED, main
from core.cli.wiring import (
    ENV_SCOUT_CRAWLER,
    LOCAL_PROFILE,
    build_adapters,
    build_scout_crawler,
)
from core.config.deploy import DeployConfig
from core.config.paths import PathsConfig
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


def _profiles() -> dict[str, Any]:
    """Категорийно-нейтральные per-domain рендер-профили (данные, §D.2)."""
    return {
        "example.com": {"render": "browser"},
        "static.example.org": {"render": "http"},
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


def test_build_scout_crawler_scrapy_threads_cache_and_profiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``scrapy``: кэш и домен-профили доходят до ``ScrapyCrawler``."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "scrapy")
    cache = _cache(tmp_path)
    profiles = _profiles()

    crawler = build_scout_crawler(cache=cache, domain_profiles=profiles)

    assert isinstance(crawler, ScrapyCrawler)
    assert crawler._cache is cache
    assert crawler._profiles == profiles


def test_build_scout_crawler_http_threads_cache_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``http``: кэш доходит, ``domain_profiles`` не передаются (HTTP-only)."""
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
# build_adapters: проброс в ports.crawler
# ---------------------------------------------------------------------------


def test_build_adapters_threads_cache_and_profiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``local`` + ``scrapy``: оба параметра доходят до ``ports.crawler``."""
    monkeypatch.setenv(ENV_SCOUT_CRAWLER, "scrapy")
    cache = _cache(tmp_path)
    profiles = _profiles()

    ports = build_adapters(
        profile=LOCAL_PROFILE,
        deploy=DeployConfig(profile="local"),
        cache=cache,
        domain_profiles=profiles,
    )

    assert isinstance(ports.crawler, ScrapyCrawler)
    assert ports.crawler._cache is cache
    assert ports.crawler._profiles == profiles


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
