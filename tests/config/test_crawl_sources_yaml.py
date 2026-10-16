"""Тесты загрузчика ``crawl_sources.yaml`` (Lane A: source-routing contracts).

Проверяются: happy-path слоёв ``defaults``/``products``/``sites``, пустой файл → дефолты,
строгая схема (``extra="forbid"``) и отвержение недопустимых enum-значений. Данные
категорийно-нейтральны (§1.3): ``p1``, ``example.com``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from core.config.crawl_sources import (
    CRAWL_SOURCES_FILENAME,
    CrawlSource,
    CrawlSourcesConfig,
    load_crawl_sources,
)
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode


def _write(tmp_path: Path, text: str) -> Path:
    """Пишет ``crawl_sources.yaml`` во временный каталог."""
    path = tmp_path / CRAWL_SOURCES_FILENAME
    path.write_text(text, encoding="utf-8")
    return path


def test_load_happy_all_layers(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "defaults: {kind: web, engine: http}\n"
        "products:\n"
        "  p1: {engine: scrapy, strategy: honest, params: {a: 1}}\n"
        "sites:\n"
        "  p1:\n"
        "    example.com: {kind: feed, fetch: browser, extract: html, params: {b: 2}}\n",
    )
    cfg = load_crawl_sources(path)
    assert isinstance(cfg, CrawlSourcesConfig)
    assert cfg.defaults.kind == "web"
    assert cfg.defaults.engine == "http"
    assert cfg.products["p1"].engine == "scrapy"
    assert cfg.products["p1"].strategy is CrawlStrategy.HONEST
    assert cfg.products["p1"].params == {"a": 1}
    site = cfg.sites["p1"]["example.com"]
    assert site.kind == "feed"
    assert site.fetch is FetchMode.BROWSER
    assert site.extract is ExtractionRoute.HTML
    assert site.params == {"b": 2}


def test_empty_file_yields_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, "")
    cfg = load_crawl_sources(path)
    assert cfg == CrawlSourcesConfig()
    assert cfg.defaults == CrawlSource()
    assert cfg.products == {}
    assert cfg.sites == {}


def test_comment_only_file_yields_defaults(tmp_path: Path) -> None:
    path = _write(tmp_path, "\n# только комментарий\n")
    assert load_crawl_sources(path) == CrawlSourcesConfig()


@pytest.mark.parametrize(
    "body",
    [
        "defaults: {}\nbogus: 1\n",  # неизвестное поле верхнего уровня
        "defaults: {render: true}\n",  # неизвестное поле CrawlSource
        "products:\n  p1: {index: true}\n",  # неизвестное поле в products
    ],
)
def test_rejects_unknown_keys(tmp_path: Path, body: str) -> None:
    with pytest.raises(ValueError, match="схема"):
        load_crawl_sources(_write(tmp_path, body))


@pytest.mark.parametrize(
    "body",
    [
        "defaults: {fetch: teleport}\n",  # недопустимый FetchMode
        "defaults: {extract: pdf}\n",  # недопустимый ExtractionRoute
        "defaults: {strategy: stealth}\n",  # недопустимый CrawlStrategy
    ],
)
def test_rejects_invalid_enum(tmp_path: Path, body: str) -> None:
    with pytest.raises(ValueError, match="схема"):
        load_crawl_sources(_write(tmp_path, body))


def test_rejects_non_mapping_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="словарь"):
        load_crawl_sources(_write(tmp_path, "- a\n- b\n"))


def test_params_accepts_arbitrary_keys(tmp_path: Path) -> None:
    """C2: ``params`` — свободный per-engine ``dict[str, Any]`` (строгая схема — Phase 2/3)."""
    path = _write(
        tmp_path,
        "products:\n"
        "  p1: {engine: scrapy, params: {any_key: 1, nested: [a, b], flag: true}}\n"
        "sites:\n"
        "  p1:\n"
        "    example.com: {engine: browser, params: {unknown_engine_knob: x}}\n",
    )
    cfg = load_crawl_sources(path)
    assert cfg.products["p1"].params == {"any_key": 1, "nested": ["a", "b"], "flag": True}
    assert cfg.sites["p1"]["example.com"].params == {"unknown_engine_knob": "x"}


def test_rejects_url_form_site_key(tmp_path: Path) -> None:
    """C4: site-ключ обязан быть ``host[+path]``, а не URL со схемой."""
    with pytest.raises(ValueError, match="host"):
        load_crawl_sources(
            _write(tmp_path, "sites:\n  p1:\n    https://example.com/x: {engine: http}\n")
        )


def test_rejects_empty_site_key(tmp_path: Path) -> None:
    """C4: пустой site-ключ отвергается."""
    with pytest.raises(ValueError, match="пустой"):
        load_crawl_sources(_write(tmp_path, 'sites:\n  p1:\n    "": {engine: http}\n'))


def test_accepts_host_and_host_path_site_keys(tmp_path: Path) -> None:
    """C4: корректные формы ``host`` и ``host/path`` принимаются."""
    cfg = load_crawl_sources(
        _write(
            tmp_path,
            "sites:\n  p1:\n    example.com: {engine: http}\n"
            "    example.com/docs: {engine: http}\n",
        )
    )
    assert set(cfg.sites["p1"]) == {"example.com", "example.com/docs"}
