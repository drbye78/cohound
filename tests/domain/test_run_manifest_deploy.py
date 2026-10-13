"""Тесты деплой-провенанса на ``RunManifest`` (R7/§13, 07 §7).

Проверяют, что поля ``deploy_profile``/``edge_schema_version`` опциональны и
backward-compatible (дефолт ``None``), корректно сериализуются и что режим
``extra="forbid"`` доменных моделей сохранён. Нейтральны (§1.3).
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from core.domain import (
    CrawlProvenance,
    CrawlSessionProvenance,
    RunManifest,
    RunMode,
)


def _manifest(**overrides: Any) -> RunManifest:
    """Минимальный валидный ``RunManifest`` с нейтральными значениями."""
    data: dict[str, Any] = {
        "run_id": "run-1",
        "case_id": "case-1",
        "created_at": "2026-01-01T00:00:00+00:00",
        "mode": RunMode.STRICT,
        "methodology_version": "2.0.0",
        "package_version": "1.0.0",
        "catalog_version": "1.0.0",
        "status_dict_version": "1.0.0",
        "scoring_policy_version": "1.0.0",
        "anchor_product_id": "p1",
    }
    data.update(overrides)
    return RunManifest(**data)


def test_deploy_fields_default_none() -> None:
    manifest = _manifest()
    assert manifest.deploy_profile is None
    assert manifest.edge_schema_version is None


def test_deploy_fields_round_trip_through_dump() -> None:
    manifest = _manifest(deploy_profile="split", edge_schema_version="1.2.3")
    dumped = manifest.model_dump(mode="json")
    assert dumped["deploy_profile"] == "split"
    assert dumped["edge_schema_version"] == "1.2.3"
    restored = RunManifest(**dumped)
    assert restored == manifest
    assert restored.deploy_profile == "split"
    assert restored.edge_schema_version == "1.2.3"


def test_run_manifest_extra_forbid_preserved() -> None:
    with pytest.raises(ValidationError):
        _manifest(unknown_deploy_field="x")


# --- Phase 2: провенанс краула --------------------------------------------- #


def test_crawl_provenance_defaults_empty() -> None:
    """§4.4/Phase 2: без краула провенанс — пустой, не ``None`` (устойчиво к чтению)."""
    manifest = _manifest()
    assert manifest.crawl_provenance == CrawlProvenance()
    assert manifest.crawl_provenance.sessions == []
    assert manifest.crawl_provenance.engines == []
    assert manifest.crawl_provenance.kinds == []
    assert manifest.crawl_provenance.crawl_sources_digest is None


def test_crawl_provenance_round_trip_through_dump() -> None:
    provenance = CrawlProvenance(
        sessions=[
            CrawlSessionProvenance(
                session_id="crawl:run-1",
                profile="local",
                status="completed",
                engines=["http", "scrapy"],
                kinds=["web"],
                total=3,
                fetched=2,
                failed=1,
            )
        ],
        engines=["http", "scrapy"],
        kinds=["web"],
        crawl_sources_digest="abc123",
    )
    manifest = _manifest(crawl_provenance=provenance)
    dumped = manifest.model_dump(mode="json")
    assert dumped["crawl_provenance"]["crawl_sources_digest"] == "abc123"
    restored = RunManifest(**dumped)
    assert restored == manifest
    assert restored.crawl_provenance.engines == ["http", "scrapy"]


def test_crawl_provenance_extra_forbid() -> None:
    with pytest.raises(ValidationError):
        CrawlProvenance(unknown_field=1)
    with pytest.raises(ValidationError):
        CrawlSessionProvenance(
            session_id="s", profile="local", status="completed", unknown_field=1
        )
