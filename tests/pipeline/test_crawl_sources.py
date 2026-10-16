"""Тесты резолвера цели краула ``resolve_crawl_target`` (Lane A).

Проверяются: матрица приоритетов слоёв (site > product > domain_profiles > run.yaml >
crawl_sources.defaults > адаптер), абсолютный приоритет ``explicit`` по оси ``fetch``,
разрешение ``engine``/``kind``, слияние ``params`` и нормализация ``HTTP`` ⇒ ``strategy=AUTO``,
а также семантика сопоставления site-ключей (длиннейший путь/subdomain/без ложного матча).
Данные категорийно-нейтральны (§1.3): ``p1``, ``example.com``.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from core.config.crawl_sources import CrawlSource, CrawlSourcesConfig
from core.config.domain_profiles import DomainProfile
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec
from core.pipeline.crawl_sources import (
    CrawlTarget,
    crawl_sources_digest,
    resolve_crawl_target,
)
from core.pipeline.fetch_plan import resolve_fetch_plan, select_site_source
from core.ports.types import FetchPlan

URL = "https://example.com/documents/x"


def _resolve(
    url: str = URL,
    *,
    sources: CrawlSourcesConfig,
    profiles: Mapping[str, DomainProfile] | None = None,
    explicit: Mapping[str, FetchMode] | None = None,
    run_strategy: CrawlStrategy = CrawlStrategy.AUTO,
    run_rate: RateLimitSpec | None = None,
    product_id: str = "p1",
) -> CrawlTarget:
    """Тонкая обёртка над ``resolve_crawl_target`` с дефолтным ``product_id``/URL."""
    return resolve_crawl_target(
        product_id,
        url,
        sources=sources,
        profiles=profiles or {},
        explicit=explicit,
        run_default_strategy=run_strategy,
        run_default_rate_limit=run_rate,
    )


def _profiles() -> dict[str, DomainProfile]:
    """Профиль домена со всеми осями, отличными от прочих слоёв."""
    return {
        "example.com": DomainProfile(
            fetch=FetchMode.BROWSER,
            extract=ExtractionRoute.DOCUMENT,
            strategy=CrawlStrategy.HYBRID,
            wait_for="prof",
            rate_limit=RateLimitSpec(min_s=5, max_s=6),
        )
    }


def _defaults_source() -> CrawlSource:
    """Дефолтный источник пакета (низший конфигурируемый слой)."""
    return CrawlSource(
        kind="web",
        engine="http",
        fetch=FetchMode.HTTP,
        extract=ExtractionRoute.AUTO,
        strategy=CrawlStrategy.AUTO,
        wait_for="def",
        rate_limit=RateLimitSpec(min_s=10, max_s=20),
        params={"x": 0},
    )


def _product_source() -> CrawlSource:
    """Источник продукта — отличен от defaults/профиля."""
    return CrawlSource(
        kind="product",
        engine="scrapy",
        fetch=FetchMode.BROWSER,
        extract=ExtractionRoute.AUTO,
        strategy=CrawlStrategy.HONEST,
        wait_for="prod",
        rate_limit=RateLimitSpec(min_s=1, max_s=2),
        params={"x": 1, "p": 1},
    )


def _site_source() -> CrawlSource:
    """Site-источник — отличен от product/профиля."""
    return CrawlSource(
        kind="feed",
        engine="browser",
        fetch=FetchMode.BROWSER,
        extract=ExtractionRoute.DOCUMENT,
        strategy=CrawlStrategy.HYBRID,
        wait_for="site",
        rate_limit=RateLimitSpec(min_s=3, max_s=4),
        params={"x": 2, "s": 2},
    )


# ---------------------------------------------------------------------------
# Матрица приоритетов
# ---------------------------------------------------------------------------


def test_site_layer_wins_over_all() -> None:
    cfg = CrawlSourcesConfig(
        defaults=_defaults_source(),
        products={"p1": _product_source()},
        sites={"p1": {"example.com": _site_source()}},
    )
    t = _resolve(
        sources=cfg,
        profiles=_profiles(),
        run_strategy=CrawlStrategy.HONEST,
        run_rate=RateLimitSpec(min_s=7, max_s=8),
    )
    assert t.plan.fetch is FetchMode.BROWSER
    assert t.plan.extract is ExtractionRoute.DOCUMENT
    assert t.plan.strategy is CrawlStrategy.HYBRID
    assert t.plan.wait_for == "site"
    assert t.plan.rate_limit == RateLimitSpec(min_s=3, max_s=4)
    assert t.engine == "browser"
    assert t.kind == "feed"
    assert dict(t.params) == {"x": 2, "p": 1, "s": 2}


def test_product_wins_over_domain_run_and_defaults() -> None:
    cfg = CrawlSourcesConfig(
        defaults=_defaults_source(),
        products={"p1": _product_source()},
    )
    t = _resolve(
        sources=cfg,
        profiles=_profiles(),
        run_strategy=CrawlStrategy.AUTO,
        run_rate=RateLimitSpec(min_s=7, max_s=8),
    )
    assert t.plan.fetch is FetchMode.BROWSER
    assert t.plan.extract is ExtractionRoute.AUTO
    assert t.plan.strategy is CrawlStrategy.HONEST
    assert t.plan.wait_for == "prod"
    assert t.plan.rate_limit == RateLimitSpec(min_s=1, max_s=2)
    assert t.engine == "scrapy"
    assert t.kind == "product"


def test_domain_profile_wins_over_run_and_defaults() -> None:
    cfg = CrawlSourcesConfig(defaults=_defaults_source())
    t = _resolve(
        sources=cfg,
        profiles=_profiles(),
        run_strategy=CrawlStrategy.HONEST,
        run_rate=RateLimitSpec(min_s=7, max_s=8),
        product_id="p_unknown",
    )
    assert t.plan.fetch is FetchMode.BROWSER
    assert t.plan.extract is ExtractionRoute.DOCUMENT
    assert t.plan.strategy is CrawlStrategy.HYBRID
    assert t.plan.wait_for == "prof"
    assert t.plan.rate_limit == RateLimitSpec(min_s=5, max_s=6)
    assert t.engine == "http"  # у профилей доменов engine нет → defaults


def test_run_defaults_win_over_crawl_sources_defaults() -> None:
    cfg = CrawlSourcesConfig(
        defaults=CrawlSource(
            fetch=FetchMode.BROWSER,
            extract=ExtractionRoute.HTML,
            strategy=CrawlStrategy.AUTO,
            wait_for="def",
            rate_limit=RateLimitSpec(min_s=10, max_s=20),
        )
    )
    t = _resolve(
        sources=cfg,
        run_strategy=CrawlStrategy.HONEST,
        run_rate=RateLimitSpec(min_s=7, max_s=8),
        product_id="p_unknown",
    )
    assert t.plan.fetch is FetchMode.BROWSER
    assert t.plan.extract is ExtractionRoute.HTML
    assert t.plan.strategy is CrawlStrategy.HONEST
    assert t.plan.wait_for == "def"
    assert t.plan.rate_limit == RateLimitSpec(min_s=7, max_s=8)


def test_crawl_sources_defaults_win_over_adapter() -> None:
    cfg = CrawlSourcesConfig(
        defaults=CrawlSource(
            extract=ExtractionRoute.DOCUMENT,
            wait_for="def",
            rate_limit=RateLimitSpec(min_s=10, max_s=20),
            engine="http",
            kind="web",
            params={"a": 1},
        )
    )
    t = _resolve(sources=cfg, product_id="p_unknown")
    assert t.plan.fetch is FetchMode.HTTP
    assert t.plan.extract is ExtractionRoute.DOCUMENT
    assert t.plan.wait_for == "def"
    assert t.plan.rate_limit == RateLimitSpec(min_s=10, max_s=20)
    assert t.engine == "http"
    assert t.kind == "web"
    assert dict(t.params) == {"a": 1}


def test_adapter_defaults_when_no_layers() -> None:
    t = _resolve(sources=CrawlSourcesConfig(), product_id="p_unknown")
    assert t.plan == FetchPlan()
    assert t.engine is None
    assert t.kind is None
    assert dict(t.params) == {}


def test_site_layer_scoped_by_product() -> None:
    cfg = CrawlSourcesConfig(
        defaults=CrawlSource(engine="http"),
        sites={"p2": {"example.com": CrawlSource(engine="other")}},
    )
    assert _resolve(sources=cfg, product_id="p1").engine == "http"


# ---------------------------------------------------------------------------
# explicit (--render-modes): только ось fetch, абсолютный приоритет
# ---------------------------------------------------------------------------


def test_explicit_fetch_wins_over_site_and_normalizes() -> None:
    cfg = CrawlSourcesConfig(
        defaults=_defaults_source(),
        products={"p1": _product_source()},
        sites={"p1": {"example.com": _site_source()}},
    )
    t = _resolve(
        sources=cfg,
        profiles=_profiles(),
        explicit={URL: FetchMode.HTTP},
    )
    assert t.plan.fetch is FetchMode.HTTP
    assert t.plan.strategy is CrawlStrategy.AUTO  # HTTP ⇒ AUTO
    assert t.plan.extract is ExtractionRoute.DOCUMENT  # прочие оси — из site
    assert t.plan.wait_for == "site"
    assert t.plan.rate_limit == RateLimitSpec(min_s=3, max_s=4)


def test_explicit_browser_over_http_site_keeps_strategy() -> None:
    cfg = CrawlSourcesConfig(
        sites={
            "p1": {
                "example.com": CrawlSource(
                    fetch=FetchMode.HTTP, strategy=CrawlStrategy.HYBRID
                )
            }
        }
    )
    t = _resolve(sources=cfg, explicit={URL: FetchMode.BROWSER})
    assert t.plan.fetch is FetchMode.BROWSER
    assert t.plan.strategy is CrawlStrategy.HYBRID


def test_explicit_ignored_for_other_url() -> None:
    cfg = CrawlSourcesConfig(
        defaults=_defaults_source(),
        products={"p1": _product_source()},
        sites={"p1": {"example.com": _site_source()}},
    )
    t = _resolve(sources=cfg, explicit={"https://example.com/other": FetchMode.HTTP})
    assert t.plan.fetch is FetchMode.BROWSER  # site остаётся


# ---------------------------------------------------------------------------
# engine / kind: site > product > defaults
# ---------------------------------------------------------------------------


def test_engine_resolution_site_product_defaults() -> None:
    full = CrawlSourcesConfig(
        defaults=CrawlSource(engine="http"),
        products={"p1": CrawlSource(engine="scrapy")},
        sites={"p1": {"example.com": CrawlSource(engine="browser")}},
    )
    assert _resolve(sources=full).engine == "browser"

    no_site = CrawlSourcesConfig(
        defaults=CrawlSource(engine="http"),
        products={"p1": CrawlSource(engine="scrapy")},
    )
    assert _resolve(sources=no_site).engine == "scrapy"

    only_defaults = CrawlSourcesConfig(defaults=CrawlSource(engine="http"))
    assert _resolve(sources=only_defaults, product_id="p_unknown").engine == "http"

    nothing = CrawlSourcesConfig()
    assert _resolve(sources=nothing, product_id="p_unknown").engine is None


def test_kind_resolution_site_product_defaults() -> None:
    cfg = CrawlSourcesConfig(
        defaults=CrawlSource(kind="web"),
        products={"p1": CrawlSource(kind="product")},
        sites={"p1": {"example.com": CrawlSource(kind="feed")}},
    )
    assert _resolve(sources=cfg).kind == "feed"
    cfg_ps = CrawlSourcesConfig(
        defaults=CrawlSource(kind="web"),
        products={"p1": CrawlSource(kind="product")},
    )
    assert _resolve(sources=cfg_ps).kind == "product"
    cfg_d = CrawlSourcesConfig(defaults=CrawlSource(kind="web"))
    assert _resolve(sources=cfg_d, product_id="p_unknown").kind == "web"


# ---------------------------------------------------------------------------
# params: слияние defaults → product → site, read-only
# ---------------------------------------------------------------------------


def test_params_merge_order_and_readonly() -> None:
    cfg = CrawlSourcesConfig(
        defaults=CrawlSource(params={"a": 1, "shared": "defaults"}),
        products={"p1": CrawlSource(params={"b": 2, "shared": "product"})},
        sites={"p1": {"example.com": CrawlSource(params={"c": 3, "shared": "site"})}},
    )
    t = _resolve(sources=cfg)
    assert dict(t.params) == {"a": 1, "b": 2, "c": 3, "shared": "site"}
    with pytest.raises(TypeError):
        t.params["x"] = 1  # type: ignore[index]  # read-only mapping


# ---------------------------------------------------------------------------
# Нормализация осей: HTTP ⇒ strategy=AUTO
# ---------------------------------------------------------------------------


def test_http_fetch_normalizes_strategy_to_auto() -> None:
    cfg = CrawlSourcesConfig(
        sites={
            "p1": {
                "example.com": CrawlSource(
                    fetch=FetchMode.HTTP, strategy=CrawlStrategy.HYBRID
                )
            }
        }
    )
    t = _resolve(sources=cfg)
    assert t.plan.fetch is FetchMode.HTTP
    assert t.plan.strategy is CrawlStrategy.AUTO


def test_browser_fetch_keeps_non_auto_strategy() -> None:
    cfg = CrawlSourcesConfig(
        sites={
            "p1": {
                "example.com": CrawlSource(
                    fetch=FetchMode.BROWSER, strategy=CrawlStrategy.HYBRID
                )
            }
        }
    )
    assert _resolve(sources=cfg).plan.strategy is CrawlStrategy.HYBRID


def test_domain_profile_http_normalizes_strategy() -> None:
    profiles = {
        "example.com": DomainProfile(
            fetch=FetchMode.HTTP, strategy=CrawlStrategy.HYBRID
        )
    }
    t = _resolve(sources=CrawlSourcesConfig(), profiles=profiles, product_id="p_unknown")
    assert t.plan.fetch is FetchMode.HTTP
    assert t.plan.strategy is CrawlStrategy.AUTO


# ---------------------------------------------------------------------------
# Сопоставление site-ключей (host[+path])
# ---------------------------------------------------------------------------


def test_site_key_matching_longest_path() -> None:
    cfg = CrawlSourcesConfig(
        sites={
            "p1": {
                "example.com": CrawlSource(engine="a"),
                "example.com/documents": CrawlSource(engine="b"),
                "example.com/documents/public": CrawlSource(engine="c"),
            }
        }
    )
    assert _resolve("https://example.com/documents/public/x", sources=cfg).engine == "c"
    assert _resolve("https://example.com/documents/x", sources=cfg).engine == "b"
    assert _resolve("https://example.com/x", sources=cfg).engine == "a"


def test_site_key_matching_subdomain_and_longer_host() -> None:
    cfg = CrawlSourcesConfig(
        sites={
            "p1": {
                "example.com": CrawlSource(engine="base"),
                "nested.example.com": CrawlSource(engine="nested"),
            }
        }
    )
    assert _resolve("https://www.example.com/a", sources=cfg).engine == "base"
    assert _resolve("https://nested.example.com/a", sources=cfg).engine == "nested"


def test_site_key_no_false_positive_on_longer_segment() -> None:
    cfg = CrawlSourcesConfig(
        defaults=CrawlSource(engine="http"),
        sites={"p1": {"example.com/documents": CrawlSource(engine="docs")}},
    )
    # /documents-archive — иной сегмент: site не матчится, работает defaults.
    assert _resolve("https://example.com/documents-archive", sources=cfg).engine == "http"
    # Сегментная граница: /documents и /documents/x матчатся.
    assert _resolve("https://example.com/documents", sources=cfg).engine == "docs"
    assert _resolve("https://example.com/documents/x", sources=cfg).engine == "docs"


def test_select_site_source_none_when_no_match() -> None:
    assert select_site_source("https://none.test/a", {"example.com": CrawlSource()}) is None


# ---------------------------------------------------------------------------
# Регрессия: поведение resolve_fetch_plan не изменилось
# ---------------------------------------------------------------------------


def test_resolve_fetch_plan_behavior_unchanged() -> None:
    profiles = {
        "example.org": DomainProfile(
            fetch=FetchMode.BROWSER,
            strategy=CrawlStrategy.HYBRID,
            wait_for="w",
        )
    }
    plan = resolve_fetch_plan(
        "https://example.org/a", profiles, default_strategy=CrawlStrategy.HONEST
    )
    assert plan == FetchPlan(
        FetchMode.BROWSER,
        extract=ExtractionRoute.AUTO,
        wait_for="w",
        strategy=CrawlStrategy.HYBRID,
    )
    assert plan.engine is None

    explicit = {"https://example.org/a": FetchMode.HTTP}
    plan2 = resolve_fetch_plan(
        "https://example.org/a", profiles, explicit, default_strategy=CrawlStrategy.HONEST
    )
    assert plan2.fetch is FetchMode.HTTP
    assert plan2.strategy is CrawlStrategy.AUTO
    assert plan2.wait_for == "w"

    assert resolve_fetch_plan("https://none.test/a", {}) == FetchPlan()


# ---------------------------------------------------------------------------
# Phase 2: дайджест конфигурации источников краула
# ---------------------------------------------------------------------------


def test_crawl_sources_digest_deterministic() -> None:
    """Одинаковая конфигурация → один отпечаток (порядко-независимо)."""
    cfg = CrawlSourcesConfig(
        defaults=CrawlSource(kind="web", engine="http"),
    )
    assert crawl_sources_digest(cfg) == crawl_sources_digest(cfg)
    assert isinstance(crawl_sources_digest(cfg), str)


def test_crawl_sources_digest_changes_on_semantic_change() -> None:
    """Смена движка/оси меняет отпечаток (провенанс содержателен, а не константа)."""
    base = CrawlSourcesConfig(defaults=CrawlSource(kind="web", engine="http"))
    other_engine = CrawlSourcesConfig(defaults=CrawlSource(kind="web", engine="scrapy"))
    other_kind = CrawlSourcesConfig(defaults=CrawlSource(kind="feed", engine="http"))
    assert crawl_sources_digest(base) != crawl_sources_digest(other_engine)
    assert crawl_sources_digest(base) != crawl_sources_digest(other_kind)
