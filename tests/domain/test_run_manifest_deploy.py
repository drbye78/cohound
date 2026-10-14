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


# --- Backlog B: операционные оси краула (§13) ------------------------------- #


def test_crawl_axes_default_none() -> None:
    """Backlog B: без секции ``crawl`` оси краула — ``None`` (граница не заполнена)."""
    manifest = _manifest()
    assert manifest.crawl_strategy is None
    assert manifest.crawl_rate_limit is None


def test_crawl_axes_round_trip_through_dump() -> None:
    """Backlog B: оси краула сериализуются как строки и переживают round-trip."""
    manifest = _manifest(
        crawl_strategy="hybrid", crawl_rate_limit="uniform(min=0.5,max=2.0)"
    )
    dumped = manifest.model_dump(mode="json")
    assert dumped["crawl_strategy"] == "hybrid"
    assert dumped["crawl_rate_limit"] == "uniform(min=0.5,max=2.0)"
    restored = RunManifest(**dumped)
    assert restored == manifest
    assert restored.crawl_strategy == "hybrid"
    assert restored.crawl_rate_limit == "uniform(min=0.5,max=2.0)"


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


def test_crawl_provenance_canonicalizes_engines_and_heterogeneous() -> None:
    """Phase 2.1/S3: ``engines`` sorted/dedup, ``heterogeneous`` выводится по конструкции."""
    hetero = CrawlProvenance(engines=["b", "a", "a"])
    assert hetero.engines == ["a", "b"]  # sorted + dedup
    assert hetero.heterogeneous is True

    single = CrawlProvenance(engines=["a"])
    assert single.engines == ["a"]
    assert single.heterogeneous is False

    empty = CrawlProvenance()
    assert empty.engines == []
    assert empty.heterogeneous is False

    dedup_single = CrawlProvenance(engines=["a", "a"])
    assert dedup_single.engines == ["a"]
    assert dedup_single.heterogeneous is False


def test_crawl_provenance_heterogeneous_ignores_stale_input() -> None:
    """Флаг — производный: противоречивое входное значение перекрывается валидатором."""
    assert CrawlProvenance(engines=["a"], heterogeneous=True).heterogeneous is False
    assert CrawlProvenance(engines=["a", "b"], heterogeneous=False).heterogeneous is True


def test_crawl_provenance_heterogeneous_survives_dump_validate() -> None:
    """Phase 2.1/S3: ``heterogeneous`` переживает round-trip (persist→read) манифеста."""
    provenance = CrawlProvenance(engines=["scrapy", "http"], kinds=["web", "feed"])
    manifest = _manifest(crawl_provenance=provenance)
    dumped = manifest.model_dump(mode="json")
    restored = RunManifest(**dumped)
    assert restored.crawl_provenance.heterogeneous is True
    assert restored.crawl_provenance == provenance
    # ``read_manifest`` валидирует JSON-текст так же — флаг не теряется под extra="forbid".
    from_json = CrawlProvenance.model_validate(dumped["crawl_provenance"])
    assert from_json.heterogeneous is True
