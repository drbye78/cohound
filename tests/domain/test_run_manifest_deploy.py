"""Тесты деплой-провенанса на ``RunManifest`` (R7/§13, 07 §7).

Проверяют, что поля ``deploy_profile``/``edge_schema_version`` опциональны и
backward-compatible (дефолт ``None``), корректно сериализуются и что режим
``extra="forbid"`` доменных моделей сохранён. Нейтральны (§1.3).
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from core.domain import RunManifest, RunMode


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
