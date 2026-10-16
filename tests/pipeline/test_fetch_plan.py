"""Тесты резолвера ``FetchPlan`` и загрузчика ``domain_profiles.yaml`` (D2; 03 §D.2).

Проверяются: матчинг профиля по host/поддомену/пути, приоритет (длиннейший path → host),
явное переопределение ``fetch``, дефолтный план и строгая схема ``DomainProfile``.
Данные категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.config.domain_profiles import DomainProfile, load_domain_profiles
from core.domain.enums import ExtractionRoute, FetchMode
from core.ports.types import CrawlStrategy, FetchPlan, RateLimitSpec
from core.pipeline.fetch_plan import resolve_fetch_plan


def _profiles() -> dict[str, DomainProfile]:
    return {
        "example.com": DomainProfile(fetch=FetchMode.HTTP, extract=ExtractionRoute.HTML),
        "nested.example.com": DomainProfile(fetch=FetchMode.HTTP),
        "example.org": DomainProfile(fetch=FetchMode.BROWSER, wait_for="() => true"),
        "docs.example.org": DomainProfile(fetch=FetchMode.HTTP, extract=ExtractionRoute.HTML),
        "example.net/documents": DomainProfile(
            fetch=FetchMode.HTTP, extract=ExtractionRoute.DOCUMENT
        ),
        "example.net": DomainProfile(fetch=FetchMode.BROWSER),
    }


# ---------------------------------------------------------------------------
# resolve_fetch_plan
# ---------------------------------------------------------------------------


def test_default_plan_without_profiles() -> None:
    assert resolve_fetch_plan("https://unknown.test/a", {}) == FetchPlan()


def test_exact_host_match() -> None:
    plan = resolve_fetch_plan("https://example.org/a", _profiles())
    assert plan.fetch is FetchMode.BROWSER
    assert plan.wait_for == "() => true"


def test_parent_domain_suffix_match() -> None:
    # Профиль example.com применяется к поддомену www.example.com.
    plan = resolve_fetch_plan("https://www.example.com/a", _profiles())
    assert plan.extract is ExtractionRoute.HTML


def test_longer_host_wins() -> None:
    # nested.example.com приоритетнее example.com (длиннее host_key).
    plan = resolve_fetch_plan("https://nested.example.com/a", _profiles())
    assert plan.fetch is FetchMode.HTTP  # у nested нет extract → дефолт AUTO
    assert plan.extract is ExtractionRoute.AUTO


def test_path_key_matches() -> None:
    # Регрессия «мёртвого» ключа: example.net/documents обязан матчиться.
    plan = resolve_fetch_plan("https://example.net/documents/report", _profiles())
    assert plan.fetch is FetchMode.HTTP
    assert plan.extract is ExtractionRoute.DOCUMENT


def test_path_not_matching_falls_back_to_host_profile() -> None:
    plan = resolve_fetch_plan("https://example.net/other", _profiles())
    assert plan.fetch is FetchMode.BROWSER
    assert plan.extract is ExtractionRoute.AUTO


def test_longest_path_precedence_over_host() -> None:
    profiles = {
        "example.net": DomainProfile(fetch=FetchMode.BROWSER),
        "example.net/documents": DomainProfile(fetch=FetchMode.HTTP),
        "example.net/documents/public": DomainProfile(fetch=FetchMode.BROWSER),
    }
    assert resolve_fetch_plan("https://example.net/documents/public/x", profiles).fetch is FetchMode.BROWSER
    assert resolve_fetch_plan("https://example.net/documents/x", profiles).fetch is FetchMode.HTTP
    assert resolve_fetch_plan("https://example.net/x", profiles).fetch is FetchMode.BROWSER


def test_path_key_matches_on_segment_boundary() -> None:
    # ``sbertech.ru/documents`` (отгруженный ключ) матчит ``/documents`` и ``/documents/x``.
    profiles = {
        "example.org/documents": DomainProfile(
            fetch=FetchMode.HTTP, extract=ExtractionRoute.DOCUMENT
        )
    }
    assert (
        resolve_fetch_plan("https://example.org/documents", profiles).extract
        is ExtractionRoute.DOCUMENT
    )
    assert (
        resolve_fetch_plan("https://example.org/documents/x", profiles).extract
        is ExtractionRoute.DOCUMENT
    )


def test_path_key_no_false_positive_on_longer_segment() -> None:
    # ``/documents-archive`` — иной сегмент, а не префикс ``/documents``.
    profiles = {
        "example.org/documents": DomainProfile(
            fetch=FetchMode.HTTP, extract=ExtractionRoute.DOCUMENT
        )
    }
    assert resolve_fetch_plan("https://example.org/documents-archive", profiles) == FetchPlan()


def test_path_key_doc_does_not_match_docs() -> None:
    # ``example.org/doc`` не должен матчить ``/docs`` (нет границы сегмента).
    profiles = {"example.org/doc": DomainProfile(fetch=FetchMode.BROWSER)}
    assert resolve_fetch_plan("https://example.org/doc", profiles).fetch is FetchMode.BROWSER
    assert resolve_fetch_plan("https://example.org/doc/x", profiles).fetch is FetchMode.BROWSER
    assert resolve_fetch_plan("https://example.org/docs", profiles) == FetchPlan()


def test_explicit_override_wins_over_profile_fetch_only() -> None:
    explicit = {"https://example.org/a": FetchMode.HTTP}
    plan = resolve_fetch_plan("https://example.org/a", _profiles(), explicit)
    # fetch переопределён явно, extract/wait_for — из профиля.
    assert plan.fetch is FetchMode.HTTP
    assert plan.wait_for == "() => true"


def test_explicit_override_ignored_for_other_urls() -> None:
    explicit = {"https://example.org/a": FetchMode.HTTP}
    plan = resolve_fetch_plan("https://example.org/b", _profiles(), explicit)
    assert plan.fetch is FetchMode.BROWSER


# ---------------------------------------------------------------------------
# DomainProfile: строгая схема и загрузчик
# ---------------------------------------------------------------------------


def test_domain_profile_defaults() -> None:
    profile = DomainProfile()
    assert profile.fetch is FetchMode.HTTP
    assert profile.extract is ExtractionRoute.AUTO
    assert profile.wait_for is None


def test_load_domain_profiles_happy(tmp_path: Path) -> None:
    path = tmp_path / "domain_profiles.yaml"
    path.write_text(
        "example.com: {fetch: browser, extract: html}\n"
        "example.org/documents: {extract: document}\n",
        encoding="utf-8",
    )
    profiles = load_domain_profiles(path)
    assert profiles["example.com"].fetch is FetchMode.BROWSER
    assert profiles["example.com"].extract is ExtractionRoute.HTML
    assert profiles["example.org/documents"].extract is ExtractionRoute.DOCUMENT
    assert profiles["example.org/documents"].fetch is FetchMode.HTTP


def test_load_domain_profiles_with_sitemap(tmp_path: Path) -> None:
    """D3: per-site ``sitemap`` грузится (scalar URL) и доступен на профиле."""
    path = tmp_path / "domain_profiles.yaml"
    path.write_text(
        "example.org: {fetch: http, sitemap: 'https://example.org/sitemap.xml'}\n",
        encoding="utf-8",
    )
    profiles = load_domain_profiles(path)
    assert profiles["example.org"].sitemap == "https://example.org/sitemap.xml"
    assert profiles["example.org"].fetch is FetchMode.HTTP


def test_load_domain_profiles_rejects_non_http_sitemap(tmp_path: Path) -> None:
    """D3: sitemap без схемы http(s) отвергается на загрузке (ранняя ошибка конфигурации)."""
    path = tmp_path / "domain_profiles.yaml"
    path.write_text(
        "example.org: {sitemap: 'example.org/sitemap.xml'}\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="профиль"):
        load_domain_profiles(path)


@pytest.mark.parametrize("field", ["spa", "index", "render"])
def test_load_domain_profiles_rejects_unknown_field(tmp_path: Path, field: str) -> None:
    path = tmp_path / "domain_profiles.yaml"
    extra = "true" if field == "spa" else "{mode: text}"
    path.write_text(f"example.com:\n  {field}: {extra}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="профиль"):
        load_domain_profiles(path)


def test_load_domain_profiles_rejects_non_mapping_root(tmp_path: Path) -> None:
    path = tmp_path / "domain_profiles.yaml"
    path.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ValueError, match="словарь"):
        load_domain_profiles(path)


def test_load_domain_profiles_rejects_non_mapping_entry(tmp_path: Path) -> None:
    path = tmp_path / "domain_profiles.yaml"
    path.write_text("example.com: not_a_mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match="словарь"):
        load_domain_profiles(path)


# ---------------------------------------------------------------------------
# D4: ось стратегии краула + per-host rate_limit
# ---------------------------------------------------------------------------


def test_default_strategy_and_rate_propagate_to_browser() -> None:
    rate = RateLimitSpec(min_s=2.0, max_s=5.0)
    profiles = {"example.org": DomainProfile(fetch=FetchMode.BROWSER)}
    plan = resolve_fetch_plan(
        "https://example.org/a",
        profiles,
        default_strategy=CrawlStrategy.HONEST,
        default_rate_limit=rate,
    )
    assert plan.strategy is CrawlStrategy.HONEST
    assert plan.rate_limit == rate


def test_default_strategy_normalized_to_auto_on_http() -> None:
    # HTTP-фетч: стратегия браузера бессмысленна -> AUTO (инвариант осей).
    plan = resolve_fetch_plan(
        "https://unknown.test/a",
        {},
        default_strategy=CrawlStrategy.HONEST,
    )
    assert plan.fetch is FetchMode.HTTP
    assert plan.strategy is CrawlStrategy.AUTO


def test_default_plan_strategy_is_auto_without_rate() -> None:
    plan = resolve_fetch_plan("https://unknown.test/a", {})
    assert plan.strategy is CrawlStrategy.AUTO
    assert plan.rate_limit is None


def test_domain_profile_strategy_and_rate_override_defaults() -> None:
    profiles = {
        "example.org": DomainProfile(
            fetch=FetchMode.BROWSER,
            strategy=CrawlStrategy.HYBRID,
            rate_limit=RateLimitSpec(min_s=1.0, max_s=2.0),
        )
    }
    plan = resolve_fetch_plan(
        "https://example.org/a",
        profiles,
        default_strategy=CrawlStrategy.HONEST,
        default_rate_limit=RateLimitSpec(min_s=5.0, max_s=9.0),
    )
    assert plan.strategy is CrawlStrategy.HYBRID
    assert plan.rate_limit == RateLimitSpec(min_s=1.0, max_s=2.0)


def test_domain_profile_without_strategy_inherits_default() -> None:
    profiles = {"example.org": DomainProfile(fetch=FetchMode.BROWSER)}
    rate = RateLimitSpec(min_s=3.0, max_s=4.0)
    plan = resolve_fetch_plan(
        "https://example.org/a",
        profiles,
        default_strategy=CrawlStrategy.HONEST,
        default_rate_limit=rate,
    )
    assert plan.strategy is CrawlStrategy.HONEST
    assert plan.rate_limit == rate


def test_profile_strategy_with_http_fetch_normalizes_to_auto() -> None:
    profiles = {
        "example.org": DomainProfile(
            fetch=FetchMode.HTTP,
            strategy=CrawlStrategy.HYBRID,
        )
    }
    plan = resolve_fetch_plan("https://example.org/a", profiles)
    assert plan.fetch is FetchMode.HTTP
    assert plan.strategy is CrawlStrategy.AUTO


def test_profile_browser_fetch_keeps_strategy() -> None:
    profiles = {
        "example.org": DomainProfile(
            fetch=FetchMode.BROWSER,
            strategy=CrawlStrategy.HYBRID,
        )
    }
    plan = resolve_fetch_plan("https://example.org/a", profiles)
    assert plan.fetch is FetchMode.BROWSER
    assert plan.strategy is CrawlStrategy.HYBRID


def test_explicit_override_to_http_normalizes_strategy_to_auto() -> None:
    profiles = {
        "example.org": DomainProfile(
            fetch=FetchMode.BROWSER,
            strategy=CrawlStrategy.HYBRID,
        )
    }
    explicit = {"https://example.org/a": FetchMode.HTTP}
    plan = resolve_fetch_plan("https://example.org/a", profiles, explicit)
    assert plan.fetch is FetchMode.HTTP
    assert plan.strategy is CrawlStrategy.AUTO


def test_explicit_override_to_browser_keeps_strategy() -> None:
    profiles = {"example.org": DomainProfile(fetch=FetchMode.HTTP)}
    explicit = {"https://example.org/a": FetchMode.BROWSER}
    plan = resolve_fetch_plan(
        "https://example.org/a",
        profiles,
        explicit,
        default_strategy=CrawlStrategy.HONEST,
    )
    assert plan.fetch is FetchMode.BROWSER
    assert plan.strategy is CrawlStrategy.HONEST


def test_domain_profile_accepts_strategy_and_rate_limit() -> None:
    profile = DomainProfile(
        strategy=CrawlStrategy.HONEST,
        rate_limit=RateLimitSpec(min_s=0.5, max_s=1.5),
    )
    assert profile.strategy is CrawlStrategy.HONEST
    assert profile.rate_limit == RateLimitSpec(min_s=0.5, max_s=1.5)


def test_domain_profile_strategy_defaults_to_none() -> None:
    profile = DomainProfile()
    assert profile.strategy is None
    assert profile.rate_limit is None


def test_load_domain_profiles_with_strategy(tmp_path: Path) -> None:
    path = tmp_path / "domain_profiles.yaml"
    path.write_text(
        "example.org: {fetch: browser, strategy: hybrid, "
        "rate_limit: {min_s: 3, max_s: 10}}\n",
        encoding="utf-8",
    )
    profile = load_domain_profiles(path)["example.org"]
    assert profile.strategy is CrawlStrategy.HYBRID
    assert profile.rate_limit == RateLimitSpec(min_s=3.0, max_s=10.0)
