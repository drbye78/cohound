"""Тесты деплой-провенанса на ``RunManifest`` (R7/§13, 07 §7).

Проверяют, что поля ``deploy_profile``/``edge_schema_version`` опциональны и
backward-compatible (дефолт ``None``), корректно сериализуются. Нейтральны (§1.3).
"""

from __future__ import annotations

from typing import Any

import pytest

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


def _sample_provenance() -> CrawlProvenance:
    """Эталонный провенанс краула для round-trip-проверки сериализации."""
    return CrawlProvenance(
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


def _dig(node: Any, path: tuple[str, ...]) -> Any:
    """Достаёт значение по пути из dict-дампа или pydantic-модели."""
    for key in path:
        node = node[key] if isinstance(node, dict) else getattr(node, key)
    return node


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param(
            {"deploy_profile": "split", "edge_schema_version": "1.2.3"},
            [(("deploy_profile",), "split"), (("edge_schema_version",), "1.2.3")],
            id="deploy-fields",
        ),
        pytest.param(
            {"crawl_strategy": "hybrid", "crawl_rate_limit": "uniform(min=0.5,max=2.0)"},
            [
                (("crawl_strategy",), "hybrid"),
                (("crawl_rate_limit",), "uniform(min=0.5,max=2.0)"),
            ],
            id="crawl-axes",
        ),
        pytest.param(
            {"crawl_provenance": _sample_provenance()},
            [
                (("crawl_provenance", "crawl_sources_digest"), "abc123"),
                (("crawl_provenance", "engines"), ["http", "scrapy"]),
            ],
            id="crawl-provenance",
        ),
    ],
)
def test_provenance_fields_round_trip_through_dump(
    overrides: dict[str, Any], expected: list[tuple[tuple[str, ...], Any]]
) -> None:
    """Поля провенанса (деплой/оси краула/провенанс краула) переживают dump→validate (§13/R7)."""
    manifest = _manifest(**overrides)
    dumped = manifest.model_dump(mode="json")
    restored = RunManifest(**dumped)
    assert restored == manifest
    for path, value in expected:
        assert _dig(dumped, path) == value
        assert _dig(restored, path) == value


# --- Backlog B: операционные оси краула (§13) ------------------------------- #


def test_crawl_axes_default_none() -> None:
    """Backlog B: без секции ``crawl`` оси краула — ``None`` (граница не заполнена)."""
    manifest = _manifest()
    assert manifest.crawl_strategy is None
    assert manifest.crawl_rate_limit is None


# --- Phase 2: провенанс краула --------------------------------------------- #


def test_crawl_provenance_defaults_empty() -> None:
    """§4.4/Phase 2: без краула провенанс — пустой, не ``None`` (устойчиво к чтению)."""
    manifest = _manifest()
    assert manifest.crawl_provenance == CrawlProvenance()
    assert manifest.crawl_provenance.sessions == []
    assert manifest.crawl_provenance.engines == []
    assert manifest.crawl_provenance.kinds == []
    assert manifest.crawl_provenance.crawl_sources_digest is None


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
