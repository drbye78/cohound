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

# ---------------------------------------------------------------------------
# D9 (план 26): ``product_axes`` — оси лимитов PDF на продукт
# ---------------------------------------------------------------------------


def test_loads_product_axes_section(tmp_path: Path) -> None:
    """Секция ``product_axes`` читается тем же файлом: лимиты — данные пакета.

    Политика (``defaults``/``products``/``sites``) отвечает на «когда обновлять», оси —
    на «что измеряется»; они не смешиваются в одной записи, но живут в одном артефакте,
    чтобы не заводить вторую поверхность «продукт → оси».
    """
    config = load_crawl_policy(
        _write(
            tmp_path,
            "products:\n"
            "  p1: {mode: auto}\n"
            "product_axes:\n"
            "  p1: {pdf_max_pages: 250, pdf_max_extracted_chars: 1000000}\n",
        )
    )
    axes = config.axes_for("p1")
    assert axes is not None
    assert axes.pdf_max_pages == 250
    assert axes.pdf_max_extracted_chars == 1_000_000
    # Продукт без осей — не «оси по умолчанию», а их отсутствие (A3).
    assert config.axes_for("p2") is None


def test_product_axes_reject_unknown_and_non_positive(tmp_path: Path) -> None:
    """Строгая схема: неизвестная ось и ``0`` отвергаются, а не игнорируются молча."""
    with pytest.raises(ValueError):
        load_crawl_policy(
            _write(tmp_path, "product_axes:\n  p1: {pdf_max_page: 10}\n")
        )
    with pytest.raises(ValueError):
        load_crawl_policy(
            _write(tmp_path, "product_axes:\n  p1: {pdf_max_pages: 0}\n")
        )
