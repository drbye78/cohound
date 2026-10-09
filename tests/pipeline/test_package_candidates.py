"""Тесты кандидатов/apply и остального CRUD (итерация I2, ``package_candidates``).

Ключевое: ``plan_candidates``/``suggest_*`` ничего не пишут; единственная точка записи —
``apply_candidate`` — требует явного ``actor`` (человеческий шлюз I6/I7); заблокированный
кандидат не меняет файлы; батч fail-fast и не атомарен.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.pipeline import package_admin as pa
from core.pipeline import package_candidates as pc
from core.pipeline.package_candidates import (
    CANDIDATE_KINDS,
    Candidate,
    apply_candidate,
    apply_candidates,
    dumps_candidates,
    loads_candidates,
    plan_candidates,
)
from core.pipeline.package_discovery import (
    suggest_cohort_members,
    suggest_modules,
    suggest_roles,
)
from core.pipeline.package_io import load_artifacts, save_artifact


def _make(tmp_path: Path, case: str = "case_x", archetype: str = "B") -> Path:
    pa.create_package(case, archetype, dir=tmp_path)
    return tmp_path / case


def _snapshot(package: Path) -> dict[str, bytes]:
    return {
        path.name: path.read_bytes()
        for path in package.iterdir()
        if path.is_file()
    }


# ---------------------------------------------------------------------------
# Candidate: валидация / сериализация
# ---------------------------------------------------------------------------


def test_candidate_accepts_all_kinds() -> None:
    for kind in CANDIDATE_KINDS:
        assert Candidate(kind=kind, payload={}).kind == kind


def test_candidate_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError):
        Candidate(kind="nope", payload={})


def test_candidate_rejects_bad_confidence() -> None:
    for bad in (1.5, -0.1, "x"):
        with pytest.raises(ValueError):
            Candidate(kind="module", payload={}, confidence=bad)


def test_candidate_rejects_non_dict_payload() -> None:
    with pytest.raises(ValueError):
        Candidate(kind="module", payload=["x"])  # type: ignore[arg-type]


def test_candidate_key_is_order_insensitive() -> None:
    a = Candidate(kind="module", payload={"module_id": "m", "weight": 1})
    b = Candidate(kind="module", payload={"weight": 1, "module_id": "m"})
    assert a.key() == b.key()


def test_candidate_round_trip() -> None:
    original = Candidate(
        kind="role", payload={"role_id": "r"}, confidence=0.4, rationale="x", source="infer"
    )
    restored = Candidate.from_dict(original.as_dict())
    assert restored.as_dict() == original.as_dict()


def test_candidate_from_dict_rejects_bad_kind() -> None:
    with pytest.raises(ValueError):
        Candidate.from_dict({"kind": "nope", "payload": {}})


def test_dumps_loads_round_trip() -> None:
    candidates = [
        Candidate(kind="module", payload={"module_id": "m", "weight": 5}),
        Candidate(kind="baseline", payload={"baseline_id": "u", "param_id": "p"}),
    ]
    text = dumps_candidates(candidates)
    assert loads_candidates(text) == candidates
    assert text.endswith("\n")


def test_loads_rejects_bad_input() -> None:
    for bad in ("{not json", "{}", '[{"kind": "nope"}]'):
        with pytest.raises(ValueError):
            loads_candidates(bad)


# ---------------------------------------------------------------------------
# suggest-* (чистые эвристики)
# ---------------------------------------------------------------------------


def test_suggest_modules_forms() -> None:
    assert suggest_modules({"modules": {"m1": 30}})[0].payload == {"module_id": "m1", "weight": 30.0}
    assert suggest_modules({"modules": ["m1"]})[0].payload["module_id"] == "m1"
    assert (
        suggest_modules({"modules": [{"module_id": "m1", "weight": 40}]})[0].payload["weight"] == 40.0
    )
    assert suggest_modules({"modules": {"m1": {"weight": 7}}})[0].payload["weight"] == 7.0
    assert suggest_modules({}) == []


def test_suggest_roles_and_cohort() -> None:
    roles = suggest_roles({"roles": [{"role_id": "r1", "definition": "d"}]})
    assert roles[0].kind == "role" and roles[0].payload["role_id"] == "r1"
    cohort = suggest_cohort_members({"products": ["p1"]})
    assert cohort[0].kind == "cohort_member"
    assert cohort[0].payload["anchor_relation"] == "analog"
    assert cohort[0].needs_human is True


def test_suggest_deduplicates_ids() -> None:
    assert len(suggest_modules({"modules": ["m1", "m1"]})) == 1


# ---------------------------------------------------------------------------
# plan_candidates
# ---------------------------------------------------------------------------


def test_plan_empty_profile_writes_nothing(tmp_path: Path) -> None:
    package = _make(tmp_path)
    before = _snapshot(package)
    assert plan_candidates(package) == []
    assert _snapshot(package) == before


def test_plan_aggregates_and_dedups(tmp_path: Path) -> None:
    package = _make(tmp_path)
    profile = {
        "modules": {"module_core": 50, "module_new": 10},  # module_core уже есть
        "roles": [{"role_id": "role_primary"}, {"role_id": "role_new"}],  # role_primary есть
        "cohort_members": ["product_anchor", "product_new"],  # product_anchor есть
    }
    kinds = {}
    for candidate in plan_candidates(package, profile=profile):
        kinds.setdefault(candidate.kind, []).append(candidate)

    assert [c.payload["module_id"] for c in kinds["module"]] == ["module_new"]
    assert [c.payload["role_id"] for c in kinds["role"]] == ["role_new"]
    assert [c.payload["product_id"] for c in kinds["cohort_member"]] == ["product_new"]


def test_plan_baseline_candidate(tmp_path: Path) -> None:
    package = _make(tmp_path)
    profile = {
        "upstream_name": "shared_upstream",
        "upstream_id": "product_anchor",
        "params": {"param.alpha_capability": {"inheritance_mode": "conditional"}},
    }
    baselines = [c for c in plan_candidates(package, profile=profile) if c.kind == "baseline"]
    assert len(baselines) == 1
    assert baselines[0].payload["baseline_id"] == "shared_upstream"
    assert baselines[0].source == "compose"


def test_plan_order_is_deterministic(tmp_path: Path) -> None:
    package = _make(tmp_path)
    profile = {"modules": ["mv"], "roles": [{"role_id": "rv"}], "products": ["pv"]}
    first = [c.key() for c in plan_candidates(package, profile=profile)]
    second = [c.key() for c in plan_candidates(package, profile=profile)]
    assert first == second


def test_plan_param_candidates_explicit(tmp_path: Path) -> None:
    package = _make(tmp_path)
    profile = {
        "param_candidates": [
            {"param_id": "param.new", "title": "T", "rule": "r", "module": "module_core", "group": "g", "importance": "Баз"}
        ]
    }
    params = [c for c in plan_candidates(package, profile=profile) if c.kind == "param"]
    assert params and params[0].payload["param_id"] == "param.new"


# ---------------------------------------------------------------------------
# apply_candidate: шлюз actor
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("actor", ["", "   ", None])
def test_apply_requires_actor(tmp_path: Path, actor: object) -> None:
    package = _make(tmp_path)
    candidate = Candidate(kind="module", payload={"module_id": "m", "weight": 5})
    with pytest.raises(pa.PackageAdminError) as exc:
        apply_candidate(package, candidate, actor=actor)  # type: ignore[arg-type]
    assert exc.value.code == "actor_required"
    assert exc.value.kind == "usage"
    assert load_artifacts(package).manifest is not None  # ничего не сломано


def test_apply_unknown_kind(tmp_path: Path) -> None:
    package = _make(tmp_path)
    candidate = Candidate(kind="module", payload={"module_id": "m", "weight": 5})
    candidate.kind = "nope"  # обходим конструктор — маршрутизация обязана отвергнуть
    with pytest.raises(pa.PackageAdminError) as exc:
        apply_candidate(package, candidate, actor="tester")
    assert exc.value.code == "candidate"


# ---------------------------------------------------------------------------
# apply_candidate: маршрутизация по видам
# ---------------------------------------------------------------------------


def test_apply_module_candidate(tmp_path: Path) -> None:
    package = _make(tmp_path)
    result = apply_candidate(
        package,
        Candidate(kind="module", payload={"module_id": "module_new", "weight": 10}),
        actor="tester",
    )
    assert result.changed == ["package"]
    assert "module_new" in {m.module_id for m in load_artifacts(package).manifest.modules}


def test_apply_role_candidate(tmp_path: Path) -> None:
    package = _make(tmp_path)
    result = apply_candidate(
        package,
        Candidate(kind="role", payload={"role_id": "role_new", "definition": "d"}),
        actor="tester",
    )
    assert result.changed == ["package"]
    pa.add_param(package, "T", "r", module="module_core", group="g", importance="Баз", role_binding="role_new")


def test_apply_param_candidate(tmp_path: Path) -> None:
    package = _make(tmp_path)
    result = apply_candidate(
        package,
        Candidate(
            kind="param",
            payload={
                "title": "New",
                "rule": "граница",
                "module": "module_core",
                "group": "g",
                "importance": "Баз",
                "param_id": "param.new",
            },
        ),
        actor="tester",
    )
    assert result.created_id == "param.new"


def test_apply_baseline_candidate(tmp_path: Path) -> None:
    package = _make(tmp_path)
    result = apply_candidate(
        package,
        Candidate(
            kind="baseline",
            payload={
                "baseline_id": "up",
                "param_id": "param.alpha_capability",
                "inheritance_mode": "conditional",
            },
        ),
        actor="tester",
    )
    assert result.changed == ["baselines"]
    assert pa.list_baselines(package)[0].inheritance_mode.value == "conditional"


def test_apply_cohort_member_candidate(tmp_path: Path) -> None:
    package = _make(tmp_path)
    result = apply_candidate(
        package,
        Candidate(
            kind="cohort_member",
            payload={"product_id": "product_new", "role_overlap": 0.3, "selection_reason": "x"},
        ),
        actor="tester",
    )
    assert "cohort" in result.changed
    assert any(e.product_id == "product_new" for e in load_artifacts(package).cohort.entries)


def test_apply_cohort_member_syncs_run_products(tmp_path: Path) -> None:
    package = _make(tmp_path)
    apply_candidate(
        package,
        Candidate(kind="cohort_member", payload={"product_id": "product_new"}),
        actor="tester",
    )
    assert "product_new" in load_artifacts(package).run.products


# ---------------------------------------------------------------------------
# Блокировка и dry-run
# ---------------------------------------------------------------------------


def test_apply_blocked_candidate_leaves_files(tmp_path: Path) -> None:
    package = _make(tmp_path)
    (package / "scope_attestation.yaml").unlink()
    before = _snapshot(package)
    blocked = Candidate(
        kind="param",
        payload={
            "title": "Blocked",
            "rule": "r",
            "module": "module_core",
            "group": "g",
            "importance": "Баз",
            "absence_rule": "parameter_reference",
        },
    )
    with pytest.raises(pa.PackageAdminError) as exc:
        apply_candidate(package, blocked, actor="tester")
    assert exc.value.kind == "blocked"
    assert _snapshot(package) == before


def test_apply_dry_run_writes_nothing(tmp_path: Path) -> None:
    package = _make(tmp_path)
    before = _snapshot(package)
    result = apply_candidate(
        package,
        Candidate(kind="module", payload={"module_id": "module_dry", "weight": 5}),
        actor="tester",
        dry_run=True,
    )
    assert result.dry_run is True and result.written == []
    assert _snapshot(package) == before


def test_scope_set_unblocks_parameter_reference(tmp_path: Path) -> None:
    package = _make(tmp_path)
    (package / "scope_attestation.yaml").unlink()
    blocked = Candidate(
        kind="param",
        payload={
            "title": "Ref",
            "rule": "r",
            "module": "module_core",
            "group": "g",
            "importance": "Баз",
            "absence_rule": "parameter_reference",
            "param_id": "param.ref",
        },
    )
    with pytest.raises(pa.PackageAdminError):
        apply_candidate(package, blocked, actor="tester")
    pa.set_scope_attestation(package, "ручная полная проверка", actor="tester")
    result = apply_candidate(package, blocked, actor="tester")
    assert result.created_id == "param.ref"


# ---------------------------------------------------------------------------
# apply_candidates: fail-fast, не-атомарность
# ---------------------------------------------------------------------------


def test_apply_candidates_returns_results(tmp_path: Path) -> None:
    package = _make(tmp_path)
    candidates = [
        Candidate(kind="module", payload={"module_id": "m1", "weight": 5}),
        Candidate(kind="role", payload={"role_id": "r1", "definition": "d"}),
    ]
    results = apply_candidates(package, candidates, actor="tester")
    assert len(results) == 2


def test_apply_candidates_fail_fast_keeps_applied(tmp_path: Path) -> None:
    package = _make(tmp_path)
    (package / "scope_attestation.yaml").unlink()
    candidates = [
        Candidate(kind="module", payload={"module_id": "module_ok", "weight": 5}),
        Candidate(
            kind="param",
            payload={
                "title": "Blocked",
                "rule": "r",
                "module": "module_core",
                "group": "g",
                "importance": "Баз",
                "absence_rule": "parameter_reference",
            },
        ),
    ]
    with pytest.raises(pa.PackageAdminError):
        apply_candidates(package, candidates, actor="tester")
    # Первый кандидат применён и остался (батч не атомарен).
    assert "module_ok" in {m.module_id for m in load_artifacts(package).manifest.modules}


# ---------------------------------------------------------------------------
# Остальной CRUD (baselines / cohort / scope)
# ---------------------------------------------------------------------------


def test_baseline_crud(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_baseline(package, "up", "param.alpha_capability", min_version="1.0", actor="t")
    pa.add_baseline(package, "up", "param.beta_capability", actor="t")
    assert len(pa.list_baselines(package)) == 2
    pa.update_baseline(package, "up", "param.alpha_capability", inheritance_mode="conditional")
    assert pa.list_baselines(package)[0].inheritance_mode.value == "conditional"
    pa.remove_baseline(package, "up", "param.beta_capability")
    assert {row.param_id for row in pa.list_baselines(package)} == {"param.alpha_capability"}


def test_baseline_add_requires_existing_param(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError):
        pa.add_baseline(package, "up", "param.none")


def test_baseline_duplicate_refuses(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_baseline(package, "up", "param.alpha_capability")
    with pytest.raises(pa.PackageAdminError):
        pa.add_baseline(package, "up", "param.alpha_capability")


def test_update_baseline_missing_refuses(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError):
        pa.update_baseline(package, "up", "param.alpha_capability", note="x")


def test_cohort_update_warns_and_syncs_run(tmp_path: Path) -> None:
    package = _make(tmp_path)
    entries = [
        {"product_id": "product_anchor", "role_overlap": 1.0, "selection_reason": "a", "anchor_relation": "anchor"},
        {"product_id": "product_new", "role_overlap": 0.4, "selection_reason": "b", "anchor_relation": "analog"},
    ]
    result = pa.update_cohort(package, {"entries": entries}, actor="t")
    assert "run" in result.changed
    assert any("§13" in warning for warning in result.warnings)
    assert load_artifacts(package).run.products == ["product_anchor", "product_new"]


def test_cohort_update_immutable_field_refuses(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError):
        pa.update_cohort(package, {"case_id": "other"})


def test_cohort_two_anchors_refuses(tmp_path: Path) -> None:
    package = _make(tmp_path)
    entries = [
        {"product_id": "a1", "role_overlap": 1.0, "selection_reason": "a", "anchor_relation": "anchor"},
        {"product_id": "a2", "role_overlap": 1.0, "selection_reason": "b", "anchor_relation": "anchor"},
    ]
    with pytest.raises(pa.PackageAdminError):
        pa.update_cohort(package, {"entries": entries})


def test_set_cohort_replaces_entries(tmp_path: Path) -> None:
    package = _make(tmp_path)
    entries = [
        {"product_id": "product_anchor", "role_overlap": 1.0, "selection_reason": "a", "anchor_relation": "anchor"},
    ]
    pa.set_cohort(package, entries, frozen_at="2030-01-01", actor="t")
    cohort = load_artifacts(package).cohort
    assert len(cohort.entries) == 1
    assert str(cohort.frozen_at) == "2030-01-01"


def test_scope_set_and_clear(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.set_scope_attestation(package, "процедура", actor="t")
    assert load_artifacts(package).scope_attestation == "процедура"
    pa.clear_scope_attestation(package, actor="t")
    assert load_artifacts(package).scope_attestation is None
    assert not (package / "scope_attestation.yaml").exists()


def test_scope_set_empty_refuses(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError):
        pa.set_scope_attestation(package, "   ")


def test_scope_clear_blocked_when_required(tmp_path: Path) -> None:
    package = _make(tmp_path)
    save_artifact(
        package,
        "catalog",
        [
            row.model_copy(update={"absence_rule": pa.AbsenceRuleType.PARAMETER_REFERENCE})
            if row.param_id == "param.alpha_capability"
            else row
            for row in load_artifacts(package).catalog
        ],
    )
    before = (package / "scope_attestation.yaml").read_bytes()
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.clear_scope_attestation(package)
    assert exc.value.kind == "blocked"
    assert (package / "scope_attestation.yaml").read_bytes() == before
