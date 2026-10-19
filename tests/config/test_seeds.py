"""Тесты ``core.config.seeds`` (09 §7): нормализация, дедуп, roundtrip, миграция.

Офлайн: только ``tmp_path`` и YAML; сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest
import yaml

from core.config.domain_profiles import load_domain_profiles
from core.config.seeds import (
    CORPUS_URLS_RELPATH,
    SEEDS_RELPATH,
    ensure_seeds_file,
    load_corpus_urls,
    load_seeds,
    merge_seeds,
    normalize_seed_url,
    resolve_profile_key,
    save_seeds,
    seed_id,
)
from core.domain.enums import OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import ProductSeeds, SeedEntry, SeedsConfig

PROFILES = {
    "acme.test": {"fetch": "http"},
    "acme.test/products/db": {"fetch": "browser"},
    "docs.acme.test": {"fetch": "http"},
    "example.test/docs": {"fetch": "http"},
}


def _entry(
    url: str,
    *,
    normalized: str | None = None,
    role: SeedRole = SeedRole.PRIMARY,
    source: SeedSource = SeedSource.ADD_PRODUCT,
    profile_key: str | None = "acme.test",
    enabled: bool = True,
    state: SeedState = SeedState.ENABLED,
) -> SeedEntry:
    return SeedEntry(
        url=url,
        normalized_url=normalized or url,
        role=role,
        source=source,
        profile_key=profile_key,
        added_at=_dt.date(2026, 10, 19),
        added_by="tester",
        enabled=enabled,
        state=state,
    )


# ---------------------------------------------------------------------------
# Нормализация (§7.2)
# ---------------------------------------------------------------------------


def test_normalize_lowercases_host_strips_default_port_and_fragment() -> None:
    assert (
        normalize_seed_url("HTTPS://ACME.Test:443/Products/DB/#frag")
        == "https://acme.test/Products/DB/"
    )


def test_normalize_preserves_path_case_and_trailing_slash() -> None:
    """Path case-sensitive, ``/p`` и ``/p/`` — разные URL (дедуп — точный)."""
    assert normalize_seed_url("https://acme.test/P") == "https://acme.test/P"
    assert normalize_seed_url("https://acme.test/p") != normalize_seed_url(
        "https://acme.test/p/"
    )


def test_normalize_keeps_non_default_port_and_query_with_warning() -> None:
    warnings: list[str] = []
    normalized = normalize_seed_url(
        "https://acme.test:8443/docs?page=2", warnings=warnings
    )
    assert normalized == "https://acme.test:8443/docs?page=2"
    assert any(warning.startswith("seed_query:") for warning in warnings)


@pytest.mark.parametrize(
    "url",
    [
        "ftp://acme.test/",
        "file:///etc/passwd",
        "acme.test/no-scheme",
        "https:///no-host",
        "https://user:pass@acme.test/",
        "",
    ],
)
def test_normalize_rejects_invalid_urls(url: str) -> None:
    with pytest.raises(ValueError):
        normalize_seed_url(url)


def test_normalize_rejects_private_ip_through_ssrf_policy() -> None:
    from core.config.seeds import validate_seed_url

    with pytest.raises(ValueError):
        validate_seed_url("http://127.0.0.1/")


def test_seed_id_is_deterministic_and_field_separated() -> None:
    first = seed_id("acme-db", "https://acme.test/")
    assert first == seed_id("acme-db", "https://acme.test/")
    assert len(first) == 24
    assert first != seed_id("acme-db-2", "https://acme.test/")
    # Разделитель полей исключает склейку «a» + «bc» == «ab» + «c».
    assert seed_id("ab", "c") != seed_id("a", "bc")


# ---------------------------------------------------------------------------
# profile_key (§7.4)
# ---------------------------------------------------------------------------


def test_resolve_profile_key_prefers_longest_path() -> None:
    assert (
        resolve_profile_key("https://acme.test/products/db/docs", PROFILES)
        == "acme.test/products/db"
    )
    assert resolve_profile_key("https://acme.test/", PROFILES) == "acme.test"


def test_resolve_profile_key_matches_subdomain_and_segment_boundary() -> None:
    assert resolve_profile_key("https://docs.acme.test/", PROFILES) == "docs.acme.test"
    # ``/docs-archive`` не матчит ``/docs`` (граница сегмента) → совпадения нет.
    assert resolve_profile_key("https://example.test/docs-archive", PROFILES) is None
    assert resolve_profile_key("https://example.test/docs/a", PROFILES) == "example.test/docs"


def test_resolve_profile_key_returns_none_without_match() -> None:
    assert resolve_profile_key("https://other.test/", PROFILES) is None
    assert resolve_profile_key("https://acme.test/", {}) is None


# ---------------------------------------------------------------------------
# Roundtrip / load / save
# ---------------------------------------------------------------------------


def test_save_and_load_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / SEEDS_RELPATH
    config = SeedsConfig(
        products={
            "acme-db": ProductSeeds(
                state=OnboardingState.READY,
                seeds=[_entry("https://acme.test/")],
            ),
            "acme-analytics": ProductSeeds(
                state=OnboardingState.PENDING_SEEDS, seeds=[]
            ),
        }
    )
    save_seeds(path, config)
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert raw["schema"] == "seeds/1"
    assert raw["products"]["acme-analytics"]["seeds"] == []

    loaded = load_seeds(path)
    assert loaded.enabled_for("acme-db")[0].normalized_url == "https://acme.test/"
    assert loaded.all_for("acme-analytics") == []
    assert loaded.products["acme-analytics"].state is OnboardingState.PENDING_SEEDS


def test_load_missing_file_is_empty_config(tmp_path: Path) -> None:
    config = load_seeds(tmp_path / "absent.yaml")
    assert config.products == {}
    assert config.enabled_for("any") == []


def test_load_rejects_ready_without_enabled_seed(tmp_path: Path) -> None:
    """``ready`` + пустой набор — противоречие O1, файл не принимается."""
    path = tmp_path / "seeds.yaml"
    path.write_text(
        "schema: seeds/1\nproducts:\n  p:\n    state: ready\n    seeds: []\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_seeds(path)


def test_disabled_seed_must_not_be_enabled() -> None:
    with pytest.raises(ValueError):
        _entry("https://acme.test/", enabled=True, state=SeedState.DISABLED)


# ---------------------------------------------------------------------------
# merge / миграция
# ---------------------------------------------------------------------------


def test_merge_keeps_existing_and_appends_new_dedup_by_normalized_url() -> None:
    existing = SeedsConfig(
        products={
            "acme-db": ProductSeeds(
                state=OnboardingState.READY,
                seeds=[_entry("https://acme.test/", profile_key="acme.test")],
            )
        }
    )
    merged = merge_seeds(
        existing,
        "acme-db",
        [
            _entry("https://acme.test/", profile_key="other.key"),  # дубль — не перетирает
            _entry("https://docs.acme.test/", profile_key="docs.acme.test"),
        ],
    )
    seeds = merged.enabled_for("acme-db")
    assert [seed.normalized_url for seed in seeds] == [
        "https://acme.test/",
        "https://docs.acme.test/",
    ]
    # Существующая запись побеждает (провенанс не затирается).
    assert seeds[0].profile_key == "acme.test"


def test_merge_zero_seeds_marks_pending() -> None:
    merged = merge_seeds(SeedsConfig(), "acme-db", [])
    product = merged.products["acme-db"]
    assert product.state is OnboardingState.PENDING_SEEDS
    assert product.seeds == []


def test_ensure_seeds_file_migrates_legacy_corpus_urls(tmp_path: Path) -> None:
    pkg = tmp_path / "package"
    (pkg / "sources").mkdir(parents=True)
    (pkg / CORPUS_URLS_RELPATH).write_text(
        "acme-db:\n- https://acme.test/products/db/\n- https://docs.acme.test/\n"
        "other:\n- https://other.test/\n",
        encoding="utf-8",
    )
    (pkg / "domain_profiles.yaml").write_text(
        "acme.test/products/db:\n  fetch: http\ndocs.acme.test:\n  fetch: http\n",
        encoding="utf-8",
    )

    config = ensure_seeds_file(pkg, "migrator", today="2026-10-19")
    seeds = config.enabled_for("acme-db")
    assert [seed.url for seed in seeds] == [
        "https://acme.test/products/db/",
        "https://docs.acme.test/",
    ]
    assert all(seed.role is SeedRole.PRIMARY for seed in seeds)
    assert all(seed.source is SeedSource.MIGRATION for seed in seeds)
    assert all(seed.enabled for seed in seeds)
    assert seeds[0].profile_key == "acme.test/products/db"
    assert seeds[1].profile_key == "docs.acme.test"
    # Продукт без профиля остаётся в файле, но профиль не выдумывается.
    assert config.enabled_for("other")[0].profile_key is None
    assert (pkg / SEEDS_RELPATH).is_file()


def test_ensure_seeds_file_does_not_overwrite_existing(tmp_path: Path) -> None:
    pkg = tmp_path / "package"
    (pkg / "sources").mkdir(parents=True)
    (pkg / CORPUS_URLS_RELPATH).write_text(
        "acme-db:\n- https://acme.test/\n", encoding="utf-8"
    )
    existing = SeedsConfig(
        products={
            "kept": ProductSeeds(
                state=OnboardingState.READY, seeds=[_entry("https://kept.test/", profile_key=None)]
            )
        }
    )
    save_seeds(pkg / SEEDS_RELPATH, existing)
    before = (pkg / SEEDS_RELPATH).read_bytes()

    loaded = ensure_seeds_file(pkg, "migrator")

    assert (pkg / SEEDS_RELPATH).read_bytes() == before
    assert set(loaded.products) == {"kept"}


def test_load_corpus_urls_is_strict(tmp_path: Path) -> None:
    path = tmp_path / "corpus_urls.yaml"
    path.write_text("p1: https://p1.test/\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_corpus_urls(path)


def test_migrated_file_loads_with_domain_profiles(tmp_path: Path) -> None:
    pkg = tmp_path / "package"
    (pkg / "sources").mkdir(parents=True)
    (pkg / CORPUS_URLS_RELPATH).write_text(
        "p1:\n- https://p1.test/\n", encoding="utf-8"
    )
    (pkg / "domain_profiles.yaml").write_text(
        "p1.test:\n  fetch: http\n", encoding="utf-8"
    )
    ensure_seeds_file(pkg, "migrator")
    load_domain_profiles(pkg / "domain_profiles.yaml")  # не бросает
    assert load_seeds(pkg / SEEDS_RELPATH).enabled_for("p1")
