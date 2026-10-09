"""Тесты низкоуровневого I/O артефактов пакета (итерация I1, ``package_io``).

Проверяют: атомарную детерминированную запись (YAML/CSV), path-конфайнмент,
round-trip чтения/записи, условные колонки ``deprecated``/``replaced_by`` (§13),
проспективный линт, предикат новых blocking и append-only журнал мутаций.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.config.run_config import load_catalog
from core.domain.models import CatalogRow
from core.domain.enums import Importance, ParamType
from core.pipeline.lint import LintReport
from core.pipeline.package_io import (
    ARTIFACT_FILES,
    PackageArtifacts,
    append_apply_log,
    assert_no_new_blocking,
    atomic_write_csv,
    atomic_write_text,
    build_run_inputs,
    load_artifacts,
    prospective_lint,
    save_artifact,
    validate_case_id,
)

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "packages" / "_template"


def _row(**overrides: object) -> CatalogRow:
    base: dict[str, object] = {
        "param_id": "param.x",
        "title": "X",
        "module": "module_core",
        "group": "core",
        "importance": Importance.BASIC,
        "param_type": ParamType.FACT,
        "rule": "route",
    }
    base.update(overrides)
    return CatalogRow.model_validate(base)


# ---------------------------------------------------------------------------
# Атомарная запись / детерминизм
# ---------------------------------------------------------------------------


def test_atomic_write_text_is_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "out.txt"
    atomic_write_text(path, "строка\n")
    atomic_write_text(path, "строка\n")
    assert path.read_text(encoding="utf-8") == "строка\n"
    assert not list(tmp_path.glob(".*.tmp")), "временные файлы не должны оставаться"


def test_atomic_write_csv_uses_lf_and_creates_parent(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "out.csv"
    atomic_write_csv(path, ["a", "b"], [["1", "2"], ["3", None]])
    raw = path.read_bytes()
    assert b"\r\n" not in raw
    assert raw == "a,b\n1,2\n3,\n".encode("utf-8")


def test_validate_case_id_accepts_slug_and_rejects_bad() -> None:
    assert validate_case_id("demo_pkg") == "demo_pkg"
    for bad in ("", "Demo", "a-b", "../x", "a/b", "a b"):
        with pytest.raises(ValueError):
            validate_case_id(bad)


# ---------------------------------------------------------------------------
# Round-trip артефактов
# ---------------------------------------------------------------------------


def test_load_artifacts_reads_template(tmp_path: Path) -> None:
    artifacts = load_artifacts(TEMPLATE)
    assert artifacts.manifest is not None
    assert artifacts.cohort is not None
    assert artifacts.run is not None
    assert artifacts.catalog
    assert artifacts.scope_attestation


def test_load_artifacts_missing_dir_returns_empty(tmp_path: Path) -> None:
    artifacts = load_artifacts(tmp_path / "nope")
    assert artifacts == PackageArtifacts()
    assert artifacts.manifest is None
    assert artifacts.catalog == []


def test_save_and_reload_round_trip(tmp_path: Path) -> None:
    source = load_artifacts(TEMPLATE)
    for name in ("package", "catalog", "baselines", "cohort", "golden", "run"):
        save_artifact(tmp_path, name, {
            "package": source.manifest,
            "catalog": source.catalog,
            "baselines": source.baselines,
            "cohort": source.cohort,
            "golden": source.golden,
            "run": source.run,
        }[name])
    reloaded = load_artifacts(tmp_path)
    assert reloaded.manifest == source.manifest
    assert reloaded.cohort == source.cohort
    assert reloaded.run == source.run
    assert reloaded.catalog == sorted(source.catalog, key=lambda r: r.param_id)
    assert reloaded.golden == sorted(
        source.golden, key=lambda r: (r.product_id, r.param_id, r.tier.value, r.case_class)
    )


def test_optional_monitoring_columns_only_when_needed(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    rows = [_row()]
    save_artifact(plain, "catalog", rows)
    header = (plain / "catalog.csv").read_text(encoding="utf-8").splitlines()[0]
    assert "deprecated" not in header and "replaced_by" not in header

    flagged = tmp_path / "flagged"
    save_artifact(flagged, "catalog", [_row(deprecated=True, replaced_by="param.y")])
    reloaded = load_catalog(flagged / "catalog.csv")
    assert reloaded[0].deprecated is True
    assert reloaded[0].replaced_by == "param.y"


def test_csv_without_optional_columns_is_backward_compatible(tmp_path: Path) -> None:
    path = tmp_path / "catalog.csv"
    path.write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        "param.a,T,module_core,g,Баз,fact,rule,explicit,,\"{\"\"ru\"\": [\"\"a\"\"]}\",ru\n",
        encoding="utf-8",
    )
    rows = load_catalog(path)
    assert rows[0].deprecated is False
    assert rows[0].replaced_by is None


def test_save_artifact_unknown_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        save_artifact(tmp_path, "nope", {})


def test_save_artifact_wrong_type(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        save_artifact(tmp_path, "package", {"not": "a manifest"})


def test_save_artifact_scope_and_profiles(tmp_path: Path) -> None:
    save_artifact(tmp_path, "scope_attestation", "полная проверка")
    save_artifact(tmp_path, "domain_profiles", {"dom": {"render": {"mode": "text"}}})
    reloaded = load_artifacts(tmp_path)
    assert reloaded.scope_attestation == "полная проверка"
    assert reloaded.domain_profiles == {"dom": {"render": {"mode": "text"}}}


# ---------------------------------------------------------------------------
# Проспективный линт
# ---------------------------------------------------------------------------


def test_prospective_lint_on_template_is_clean() -> None:
    report = prospective_lint(load_artifacts(TEMPLATE))
    assert report.blocking == []
    assert report.warnings == []


def test_prospective_lint_synthesizes_partial_state() -> None:
    report = prospective_lint(PackageArtifacts())
    assert report.blocking == []
    assert any("golden" in warning for warning in report.warnings)


def test_build_run_inputs_synthesizes_missing_run() -> None:
    inputs = build_run_inputs(PackageArtifacts())
    assert inputs.run.products == ["anchor"]
    assert inputs.run.anchor == "anchor"
    assert inputs.package.case_id == "package"


def test_assert_no_new_blocking_detects_delta() -> None:
    before = LintReport(blocking=["a"], warnings=[])
    after = LintReport(blocking=["a", "b"], warnings=[])
    assert assert_no_new_blocking(before, after) == ["b"]
    assert assert_no_new_blocking(after, before) == []


def test_build_run_inputs_uses_manifest_case_id() -> None:
    artifacts = load_artifacts(TEMPLATE)
    inputs = build_run_inputs(artifacts)
    assert inputs.package.case_id == "template"


def test_artifact_files_cover_all_names() -> None:
    assert set(ARTIFACT_FILES) == {
        "package",
        "catalog",
        "baselines",
        "cohort",
        "golden",
        "domain_profiles",
        "scope_attestation",
        "run",
    }


# ---------------------------------------------------------------------------
# Журнал авторских мутаций
# ---------------------------------------------------------------------------


def test_append_apply_log_is_append_only_and_sorted(tmp_path: Path) -> None:
    log = append_apply_log(tmp_path, {"op": "param-add", "actor": "a1", "changed": ["catalog"]})
    append_apply_log(tmp_path, {"op": "module-add", "actor": "a2", "changed": ["package"]})
    assert log == tmp_path / ".authoring" / "apply_log.jsonl"
    lines = log.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["op"] == "param-add"
    assert json.loads(lines[1])["actor"] == "a2"
    # Ключи отсортированы (детерминированный JSON).
    assert lines[0].startswith('{"actor"')
