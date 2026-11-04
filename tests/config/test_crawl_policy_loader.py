"""Загрузчик ``crawl_policy.yaml`` (план 19 §4): строгая схема и три слоя."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.config.crawl_policy import (
    CrawlPolicyConfig,
    load_crawl_policy,
    parse_site_key,
)
from core.domain.enums import CrawlPolicyMode, CrawlPolicyScopeKind


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "crawl_policy.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_loads_three_layers(tmp_path: Path) -> None:
    config = load_crawl_policy(
        _write(
            tmp_path,
            "defaults: {mode: auto}\n"
            "products:\n"
            "  p1: {mode: on_request, note: discovery}\n"
            "sites:\n"
            "  p1:\n"
            "    docs.test: {mode: network_denied}\n"
            "    docs.test/api: {mode: scheduled, interval_hours: 24}\n"
            "    docs.test/api/v1: {mode: auto, scope_kind: path}\n",
        )
    )
    assert config.defaults is not None and config.defaults.mode is CrawlPolicyMode.AUTO
    assert config.products["p1"].mode is CrawlPolicyMode.ON_REQUEST
    assert config.sites["p1"]["docs.test/api"].interval_hours == 24
    assert (
        config.sites["p1"]["docs.test/api/v1"].scope_kind
        is CrawlPolicyScopeKind.PATH
    )


def test_empty_file_is_empty_config(tmp_path: Path) -> None:
    assert load_crawl_policy(_write(tmp_path, "")).model_dump() == (
        CrawlPolicyConfig().model_dump()
    )


def test_unknown_field_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"extra_forbidden|некорректная схема"):
        load_crawl_policy(_write(tmp_path, "defaults: {mode: auto, typo: 1}\n"))


def test_scheduled_requires_positive_interval(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        load_crawl_policy(_write(tmp_path, "defaults: {mode: scheduled}\n"))
    with pytest.raises(ValueError):
        load_crawl_policy(
            _write(tmp_path, "defaults: {mode: scheduled, interval_hours: 0}\n")
        )


def test_interval_only_for_scheduled(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="interval_hours"):
        load_crawl_policy(_write(tmp_path, "defaults: {mode: auto, interval_hours: 5}\n"))


def test_site_key_with_scheme_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="схему URL"):
        load_crawl_policy(
            _write(
                tmp_path,
                "sites:\n  p1:\n    https://docs.test/docs: {mode: auto}\n",
            )
        )


def test_scope_kind_not_allowed_on_product_layers(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="scope_kind"):
        load_crawl_policy(
            _write(
                tmp_path,
                "products:\n  p1: {mode: auto, scope_kind: host}\n",
            )
        )


def test_site_scope_kind_product_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="product"):
        load_crawl_policy(
            _write(
                tmp_path,
                "sites:\n  p1:\n    docs.test: {mode: auto, scope_kind: product}\n",
            )
        )


def test_parse_site_key_normalizes_path() -> None:
    assert parse_site_key("Docs.Test/docs/") == ("docs.test", "/docs")
    assert parse_site_key("docs.test") == ("docs.test", "")
    assert parse_site_key("docs.test/") == ("docs.test", "")