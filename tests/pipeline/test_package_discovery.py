"""Тесты базового разведочного инференса (итерация I1, ``package_discovery``).

Эвристика архетипа §1.5 и черновая сборка наследования; ничего не пишут (A3).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.domain.enums import Archetype, Importance, InheritanceMode, ParamType
from core.domain.models import CatalogRow
from core.pipeline.package_discovery import compose_baselines, infer_archetype

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "package_admin" / "profile.json"


def _catalog() -> list[CatalogRow]:
    return [
        CatalogRow(
            param_id="param.a",
            title="A",
            module="m1",
            group="g",
            importance=Importance.BASIC,
            param_type=ParamType.FACT,
            rule="r",
        ),
        CatalogRow(
            param_id="param.b",
            title="B",
            module="m2",
            group="g",
            importance=Importance.BASIC,
            param_type=ParamType.FACT,
            rule="r",
        ),
    ]


# ---------------------------------------------------------------------------
# infer_archetype
# ---------------------------------------------------------------------------


def test_infer_none_without_data() -> None:
    result = infer_archetype({})
    assert result.archetype is None
    assert result.confidence == 0.0
    assert result.needs_human is True


def test_infer_a_on_upstream() -> None:
    result = infer_archetype({"upstream_name": "shared", "upstream_shared": True})
    assert result.archetype is Archetype.A
    assert result.needs_human is False


def test_infer_b_on_alternatives_and_derivatives() -> None:
    result = infer_archetype({"alternatives": ["x", "y"], "commercial_derivatives": ["z"]})
    assert result.archetype is Archetype.B
    assert result.needs_human is True


def test_infer_b_on_alternatives_and_cross_line() -> None:
    assert infer_archetype({"alternatives": ["x"], "cross_line": True}).archetype is Archetype.B


def test_infer_c_without_leader() -> None:
    result = infer_archetype({"alternatives": ["x", "y"]})
    assert result.archetype is Archetype.C


def test_infer_c_on_sparse_data() -> None:
    assert infer_archetype({"note": "only note"}).archetype is Archetype.C


# ---------------------------------------------------------------------------
# compose_baselines
# ---------------------------------------------------------------------------


def test_compose_skip_unreferenced() -> None:
    proposal = compose_baselines({"upstream_name": "Up"}, _catalog())
    assert proposal.baseline_id == "up"
    assert proposal.rows == []
    assert any("пропущен" in note for note in proposal.notes)
    assert proposal.needs_human is True


def test_compose_direct_and_conditional() -> None:
    direct = compose_baselines({"upstream_name": "Up"}, _catalog(), unreferenced="direct")
    assert {row.inheritance_mode for row in direct.rows} == {InheritanceMode.DIRECT}
    cond = compose_baselines({"upstream_name": "Up"}, _catalog(), unreferenced="conditional")
    assert {row.inheritance_mode for row in cond.rows} == {InheritanceMode.CONDITIONAL}


def test_compose_uses_hints() -> None:
    profile = {
        "upstream_name": "Up",
        "upstream_id": "product_anchor",
        "params": {"param.a": {"inheritance_mode": "conditional", "min_version": "2.0.0", "note": "n"}},
    }
    proposal = compose_baselines(profile, _catalog())
    row = proposal.rows[0]
    assert row.param_id == "param.a"
    assert row.inheritance_mode is InheritanceMode.CONDITIONAL
    assert row.min_version == "2.0.0"
    assert proposal.baseline_participant == "product_anchor"


def test_compose_scope_all_vs_module() -> None:
    profile = {"upstream_name": "Up"}
    all_rows = compose_baselines(profile, _catalog(), unreferenced="direct")
    assert len(all_rows.rows) == 2
    scoped = compose_baselines(profile, _catalog(), scope="m1", unreferenced="direct")
    assert [row.param_id for row in scoped.rows] == ["param.a"]


def test_compose_no_upstream() -> None:
    proposal = compose_baselines({}, _catalog())
    assert proposal.baseline_id == ""
    assert proposal.rows == []
    assert proposal.notes


def test_compose_invalid_unreferenced() -> None:
    with pytest.raises(ValueError):
        compose_baselines({"upstream_name": "Up"}, _catalog(), unreferenced="nope")


def test_compose_bad_hint_mode() -> None:
    profile = {"upstream_name": "Up", "params": {"param.a": {"inheritance_mode": "bad"}}}
    with pytest.raises(ValueError):
        compose_baselines(profile, _catalog())


def test_fixture_profile_loads() -> None:
    import json

    profile = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert infer_archetype(profile).archetype is Archetype.A
    proposal = compose_baselines(profile, _catalog())
    assert proposal.baseline_id == "shared_upstream"
