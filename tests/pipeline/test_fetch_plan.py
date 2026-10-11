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
from core.ports.types import FetchPlan
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


def test_unknown_host_falls_back_to_default() -> None:
    assert resolve_fetch_plan("https://other.test/a", _profiles()) == FetchPlan()


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
