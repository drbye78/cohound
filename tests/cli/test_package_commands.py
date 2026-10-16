"""Тесты CLI ``hound package …`` (итерация I1, ``package_commands``).

Сквозные сценарии через :func:`core.cli.main.main`: new → module/role/param CRUD →
validate → infer/compose, включая коды выхода и ``--dry-run``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.cli.main import EXIT_BLOCKED, EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, main
from core.domain.enums import AbsenceRuleType
from core.pipeline.package_io import load_artifacts, save_artifact


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _new(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> Path:
    code, out, _ = _run(
        capsys,
        ["package", "new", "--case-id", "demo_pkg", "--archetype", "B", "--dir", str(tmp_path), "--json"],
    )
    assert code == EXIT_OK, out
    return tmp_path / "demo_pkg"


# ---------------------------------------------------------------------------
# new / show / validate
# ---------------------------------------------------------------------------


def test_cli_new_json_creates_files(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    assert (package / "package.yaml").is_file()
    assert (package / "catalog.csv").is_file()


def test_cli_new_dry_run_no_files(capsys, tmp_path: Path) -> None:
    code, out, _ = _run(
        capsys,
        ["package", "new", "--case-id", "dry", "--archetype", "C", "--dir", str(tmp_path), "--dry-run", "--json"],
    )
    assert code == EXIT_OK
    assert json.loads(out)["dry_run"] is True
    assert not (tmp_path / "dry").exists()


def test_cli_show_and_validate(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "show", "--package", str(package), "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    # Свежий пакет линт-чист (нет блокирующих), но неполон: авторские поля не заполнены.
    assert payload["ok"] is True
    assert payload["complete"] is False
    assert payload["incomplete"]

    code, out, _ = _run(capsys, ["package", "validate", "--package", str(package), "--json"])
    # Неполнота не проходит молча: validate завершается EXIT_BLOCKED с «incomplete:».
    assert code == EXIT_BLOCKED
    payload = json.loads(out)
    assert payload["ok"] is False
    assert payload["complete"] is False
    assert payload["incomplete"]


def test_cli_validate_ok_when_authoring_filled(capsys, tmp_path: Path) -> None:
    """Заполненные авторские поля (definition/selection_reason) → validate EXIT_OK."""
    package = _new(capsys, tmp_path)
    for role_id in ("role_primary", "role_secondary"):
        code, _, _ = _run(
            capsys,
            ["package", "role", "update", "--package", str(package),
             "--role-id", role_id, "--definition", f"роль {role_id}"],
        )
        assert code == EXIT_OK
    entries = _write(
        tmp_path / "entries.yaml",
        "- product_id: product_anchor\n  role_overlap: 1.0\n  selection_reason: якорь\n  anchor_relation: anchor\n"
        "- product_id: product_analog_a\n  role_overlap: 0.8\n  selection_reason: аналог A\n  anchor_relation: analog\n"
        "- product_id: product_analog_b\n  role_overlap: 0.6\n  selection_reason: аналог B\n  anchor_relation: analog\n",
    )
    code, _, _ = _run(
        capsys,
        ["package", "cohort", "set", "--package", str(package), "--entries", str(entries), "--actor", "t", "--json"],
    )
    assert code == EXIT_OK
    code, out, _ = _run(capsys, ["package", "validate", "--package", str(package), "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["ok"] is True


def test_cli_validate_blocked_exit_code(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    (package / "scope_attestation.yaml").unlink()
    # Пишем блокирующее состояние в обход CRUD (save_artifact не линтует) — так проверяем
    # именно CLI-путь validate, а не отказ мутации.
    artifacts = load_artifacts(package)
    rows = [
        row.model_copy(update={"absence_rule": AbsenceRuleType.PARAMETER_REFERENCE})
        if row.param_id == "param.alpha_capability"
        else row
        for row in artifacts.catalog
    ]
    save_artifact(package, "catalog", rows)
    code, out, _ = _run(capsys, ["package", "validate", "--package", str(package), "--json"])
    assert code == EXIT_BLOCKED
    assert json.loads(out)["ok"] is False


# ---------------------------------------------------------------------------
# module / role / param
# ---------------------------------------------------------------------------


def test_cli_module_role_param_flow(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, _, _ = _run(capsys, ["package", "module", "add", "--package", str(package), "--module-id", "module_extra", "--weight", "10"])
    assert code == EXIT_OK
    code, _, _ = _run(capsys, ["package", "module", "renormalize", "--package", str(package)])
    assert code == EXIT_OK
    code, _, _ = _run(capsys, ["package", "role", "add", "--package", str(package), "--role-id", "role_extra", "--definition", "доп"])
    assert code == EXIT_OK
    code, out, _ = _run(
        capsys,
        [
            "package", "param", "add", "--package", str(package), "--title", "Latency",
            "--rule", "публичная величина latency p95 (metric)", "--module", "module_extra",
            "--group", "ecosystem", "--importance", "Баз", "--param-type", "showcase",
            "--role-binding", "role_extra", "--concept-term", "en=latency", "--lang", "en",
            "--param-id", "param.latency", "--json",
        ],
    )
    assert code == EXIT_OK, out
    assert json.loads(out)["created_id"] == "param.latency"

    code, out, _ = _run(capsys, ["package", "param", "get", "--package", str(package), "--param-id", "param.latency", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["param"]["title"] == "Latency"

    code, out, _ = _run(capsys, ["package", "param", "update", "--package", str(package), "--param-id", "param.latency", "--title", "Latency2", "--json"])
    assert code == EXIT_OK

    code, out, _ = _run(capsys, ["package", "param", "list", "--package", str(package), "--json"])
    assert code == EXIT_OK
    assert any(row["param_id"] == "param.latency" for row in json.loads(out)["params"])

    code, out, _ = _run(capsys, ["package", "param", "remove", "--package", str(package), "--param-id", "param.latency", "--json"])
    assert code == EXIT_OK
    code, out, _ = _run(capsys, ["package", "param", "list", "--package", str(package), "--exclude-deprecated", "--json"])
    assert all(row["param_id"] != "param.latency" for row in json.loads(out)["params"])


def test_cli_role_list(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "role", "list", "--package", str(package), "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["roles"]


def test_cli_param_get_missing(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "param", "get", "--package", str(package), "--param-id", "param.none", "--json"])
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False


# ---------------------------------------------------------------------------
# Ошибки/блокировки
# ---------------------------------------------------------------------------


def test_cli_bad_module_is_usage(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, _, err = _run(
        capsys,
        ["package", "param", "add", "--package", str(package), "--title", "T", "--rule", "r", "--module", "nope", "--group", "g", "--importance", "Баз"],
    )
    assert code == EXIT_USAGE
    assert "nope" in err


def test_cli_new_module_blocked_exit(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, _, err = _run(capsys, ["package", "module", "remove", "--package", str(package), "--module-id", "module_core"])
    assert code == EXIT_BLOCKED
    assert "module-in-use" in err


def test_cli_package_no_subcommand(capsys) -> None:
    code, out, _ = _run(capsys, ["package"])
    assert code == EXIT_USAGE
    assert "module" in out


# ---------------------------------------------------------------------------
# infer / compose
# ---------------------------------------------------------------------------


def test_cli_infer_archetype(capsys) -> None:
    code, out, _ = _run(capsys, ["package", "infer-archetype", "--upstream-name", "u", "--upstream-shared", "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["archetype"] == "A"
    assert payload["needs_human"] is False


def test_cli_compose_baselines(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(
        capsys,
        ["package", "compose-baselines", "--package", str(package), "--upstream-name", "u", "--upstream-id", "product_anchor", "--unreferenced", "direct", "--json"],
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["baseline_id"] == "u"
    assert payload["needs_human"] is True
    assert payload["rows"]


def test_cli_compose_bad_profile_file(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    bad = tmp_path / "bad.yaml"
    bad.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    code, _, err = _run(
        capsys,
        ["package", "compose-baselines", "--package", str(package), "--profile", str(bad)],
    )
    assert code == EXIT_USAGE
    assert "словар" in err


# ---------------------------------------------------------------------------
# I2: baselines / cohort / scope
# ---------------------------------------------------------------------------


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_cli_baseline_crud(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, _, _ = _run(
        capsys,
        ["package", "baseline", "add", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.alpha_capability", "--min-version", "1.0", "--actor", "t", "--json"],
    )
    assert code == EXIT_OK
    code, out, _ = _run(capsys, ["package", "baseline", "list", "--package", str(package), "--json"])
    assert json.loads(out)["baselines"][0]["baseline_id"] == "up"
    code, _, _ = _run(
        capsys,
        ["package", "baseline", "update", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.alpha_capability", "--inheritance-mode", "conditional", "--json"],
    )
    assert code == EXIT_OK
    code, _, _ = _run(
        capsys,
        ["package", "baseline", "remove", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.alpha_capability", "--json"],
    )
    assert code == EXIT_OK
    code, out, _ = _run(capsys, ["package", "baseline", "list", "--package", str(package), "--json"])
    assert json.loads(out)["baselines"] == []


def test_cli_baseline_add_unknown_param_usage(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, _, err = _run(
        capsys,
        ["package", "baseline", "add", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.none"],
    )
    assert code == EXIT_USAGE
    assert "param.none" in err


def test_cli_cohort_update_warns(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    fields = _write(
        tmp_path / "cohort.yaml",
        "entries:\n"
        "  - product_id: product_anchor\n    role_overlap: 1.0\n    selection_reason: a\n    anchor_relation: anchor\n"
        "  - product_id: product_new\n    role_overlap: 0.5\n    selection_reason: b\n    anchor_relation: analog\n",
    )
    code, out, _ = _run(
        capsys,
        ["package", "cohort", "update", "--package", str(package), "--fields", str(fields),
         "--actor", "t", "--json"],
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert "run" in payload["changed"]
    assert any("§13" in warning for warning in payload["warnings"])


def test_cli_cohort_set(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    entries = _write(
        tmp_path / "entries.yaml",
        "- product_id: product_anchor\n  role_overlap: 1.0\n  selection_reason: a\n  anchor_relation: anchor\n",
    )
    code, _, _ = _run(
        capsys,
        ["package", "cohort", "set", "--package", str(package), "--entries", str(entries),
         "--frozen-at", "2030-01-01", "--actor", "t", "--json"],
    )
    assert code == EXIT_OK
    assert len(load_artifacts(package).cohort.entries) == 1


def test_cli_scope_set_clear(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, _, _ = _run(
        capsys,
        ["package", "scope", "set", "--package", str(package), "--procedure", "полная проверка",
         "--actor", "t", "--json"],
    )
    assert code == EXIT_OK
    assert load_artifacts(package).scope_attestation == "полная проверка"
    code, _, _ = _run(capsys, ["package", "scope", "clear", "--package", str(package), "--actor", "t", "--json"])
    assert code == EXIT_OK
    assert not (package / "scope_attestation.yaml").exists()


def test_cli_scope_clear_blocked(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    artifacts = load_artifacts(package)
    rows = [
        row.model_copy(update={"absence_rule": AbsenceRuleType.PARAMETER_REFERENCE})
        if row.param_id == "param.alpha_capability"
        else row
        for row in artifacts.catalog
    ]
    save_artifact(package, "catalog", rows)
    code, _, _ = _run(capsys, ["package", "scope", "clear", "--package", str(package), "--json"])
    assert code == EXIT_BLOCKED
    assert (package / "scope_attestation.yaml").is_file()


# ---------------------------------------------------------------------------
# I2: suggest / candidates / apply
# ---------------------------------------------------------------------------


def test_cli_suggest_modules(capsys, tmp_path: Path) -> None:
    profile = _write(tmp_path / "profile.yaml", "modules:\n  module_new: 12\n")
    code, out, _ = _run(capsys, ["package", "suggest", "modules", "--profile", str(profile), "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["candidates"][0]["payload"]["module_id"] == "module_new"


def test_cli_candidates_and_write(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    profile = _write(
        tmp_path / "profile.yaml",
        "modules:\n  module_new: 12\ncohort_members:\n  - product_id: product_new\n",
    )
    out_file = tmp_path / "candidates.json"
    code, out, _ = _run(
        capsys,
        ["package", "candidates", "--package", str(package), "--profile", str(profile),
         "--write", str(out_file), "--json"],
    )
    assert code == EXIT_OK
    assert json.loads(out)["count"] == 2
    assert out_file.is_file()


def test_cli_apply_from_file(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    candidates = _write(
        tmp_path / "candidates.json",
        json.dumps(
            [
                {"kind": "module", "payload": {"module_id": "module_new", "weight": 10}},
                {"kind": "role", "payload": {"role_id": "role_new", "definition": "d"}},
            ]
        ),
    )
    code, out, _ = _run(
        capsys,
        ["package", "apply", "--package", str(package), "--from", str(candidates),
         "--actor", "tester", "--json"],
    )
    assert code == EXIT_OK
    assert json.loads(out)["applied"] == 2
    assert "module_new" in {m.module_id for m in load_artifacts(package).manifest.modules}
    assert (package / ".authoring" / "apply_log.jsonl").is_file()


def test_cli_apply_requires_actor(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    candidates = _write(tmp_path / "c.json", "[]")
    code, _, err = _run(
        capsys,
        ["package", "apply", "--package", str(package), "--from", str(candidates)],
    )
    assert code == EXIT_USAGE
    assert "--actor" in err


def test_cli_apply_blocked_candidate(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    (package / "scope_attestation.yaml").unlink()
    before = (package / "catalog.csv").read_bytes()
    candidates = _write(
        tmp_path / "blocked.json",
        json.dumps(
            [
                {
                    "kind": "param",
                    "payload": {
                        "title": "Blocked",
                        "rule": "r",
                        "module": "module_core",
                        "group": "g",
                        "importance": "Баз",
                        "absence_rule": "parameter_reference",
                    },
                }
            ]
        ),
    )
    code, out, _ = _run(
        capsys,
        ["package", "apply", "--package", str(package), "--from", str(candidates),
         "--actor", "tester", "--json"],
    )
    assert code == EXIT_BLOCKED
    assert json.loads(out)["ok"] is False
    assert (package / "catalog.csv").read_bytes() == before


def test_cli_apply_stdin(capsys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _new(capsys, tmp_path)
    import io
    import sys

    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps([{"kind": "module", "payload": {"module_id": "module_s", "weight": 5}}])))
    code, out, _ = _run(
        capsys,
        ["package", "apply", "--package", str(package), "--stdin", "--actor", "tester", "--json"],
    )
    assert code == EXIT_OK
    assert json.loads(out)["applied"] == 1


# ---------------------------------------------------------------------------
# I3: обзор пакетов / модулей / когорты + поиск в list-командах
# ---------------------------------------------------------------------------


def test_cli_package_list_json_and_search(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)  # demo_pkg в tmp_path
    code, out, _ = _run(capsys, ["package", "list", "--dir", str(tmp_path), "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["count"] == 1
    assert payload["packages"][0]["case_id"] == "demo_pkg"
    assert payload["packages"][0]["ok"] is True

    code, out, _ = _run(capsys, ["package", "list", "--dir", str(tmp_path), "--search", "DEMO", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["count"] == 1

    code, out, _ = _run(capsys, ["package", "list", "--dir", str(tmp_path), "--search", "nomatch", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["count"] == 0

    # Пустой каталог — валидный EXIT_OK с count 0.
    code, out, _ = _run(capsys, ["package", "list", "--dir", str(tmp_path / "empty"), "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["count"] == 0


def test_cli_package_list_includes_template(capsys, tmp_path: Path) -> None:
    (tmp_path / "_template").mkdir()
    (tmp_path / "_template" / "package.yaml").write_text(
        "case_id: template\npackage_version: '0.0.0'\narchetype: B\n"
        "modules:\n  - module_id: core\n    weight: 100\n"
        "roles: []\nbaselines: []\nbaseline_participant: null\n",
        encoding="utf-8",
    )
    code, out, _ = _run(capsys, ["package", "list", "--dir", str(tmp_path), "--json"])
    assert json.loads(out)["count"] == 0
    code, out, _ = _run(
        capsys, ["package", "list", "--dir", str(tmp_path), "--include-template", "--json"]
    )
    assert json.loads(out)["count"] == 1


def test_cli_module_list(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "module", "list", "--package", str(package), "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["count"] == 2
    assert payload["weight_total"] == 100.0
    assert {m["module_id"] for m in payload["modules"]} == {"module_core", "module_ecosystem"}


def test_cli_cohort_show_and_not_found(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "cohort", "show", "--package", str(package), "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["found"] is True
    assert payload["cohort"]["case_id"] == "demo_pkg"
    assert len(payload["cohort"]["entries"]) == 3

    code, out, _ = _run(
        capsys, ["package", "cohort", "show", "--package", str(tmp_path / "nope"), "--json"]
    )
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False


def test_cli_role_list_search(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(
        capsys, ["package", "role", "list", "--package", str(package), "--search", "PRIMARY", "--json"]
    )
    assert code == EXIT_OK
    assert [r["role_id"] for r in json.loads(out)["roles"]] == ["role_primary"]
    code, out, _ = _run(
        capsys, ["package", "role", "list", "--package", str(package), "--search", "nomatch", "--json"]
    )
    assert json.loads(out)["count"] == 0


def test_cli_baseline_list_search_and_filter(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        ["package", "baseline", "add", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.alpha_capability", "--note", "upstream", "--actor", "t", "--json"],
    )
    _run(
        capsys,
        ["package", "baseline", "add", "--package", str(package), "--baseline-id", "down",
         "--param-id", "param.alpha_capability", "--actor", "t", "--json"],
    )
    code, out, _ = _run(
        capsys, ["package", "baseline", "list", "--package", str(package), "--search", "UPSTREAM", "--json"]
    )
    assert code == EXIT_OK
    assert [r["baseline_id"] for r in json.loads(out)["baselines"]] == ["up"]
    code, out, _ = _run(
        capsys, ["package", "baseline", "list", "--package", str(package), "--baseline", "down", "--json"]
    )
    assert [r["baseline_id"] for r in json.loads(out)["baselines"]] == ["down"]


def test_cli_param_list_filters_and_search(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        ["package", "param", "add", "--package", str(package), "--title", "Latency",
         "--rule", "публичная величина latency p95 (metric)", "--module", "module_ecosystem",
         "--group", "ecosystem", "--importance", "Баз", "--param-type", "showcase",
         "--concept-term", "en=latency", "--param-id", "param.latency", "--json"],
    )
    code, out, _ = _run(
        capsys, ["package", "param", "list", "--package", str(package), "--search", "LATENCY", "--json"]
    )
    assert code == EXIT_OK
    assert [p["param_id"] for p in json.loads(out)["params"]] == ["param.latency"]

    code, out, _ = _run(
        capsys, ["package", "param", "list", "--package", str(package), "--module", "module_ecosystem", "--json"]
    )
    assert all(p["module"] == "module_ecosystem" for p in json.loads(out)["params"])

    code, out, _ = _run(
        capsys, ["package", "param", "list", "--package", str(package), "--importance", "Кр", "--json"]
    )
    assert all(p["importance"] == "Кр" for p in json.loads(out)["params"])

    code, out, _ = _run(
        capsys, ["package", "param", "list", "--package", str(package), "--search", "nomatch", "--json"]
    )
    assert json.loads(out)["count"] == 0


# ---------------------------------------------------------------------------
# I4: read-плоскость — get/list по модулям/ролям/параметрам/baselines/scope/golden/run/artifact
# ---------------------------------------------------------------------------


def test_cli_module_get_and_list_search(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "module", "get", "--package", str(package), "--module-id", "module_core", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["module"]["module_id"] == "module_core"

    code, out, _ = _run(capsys, ["package", "module", "get", "--package", str(package), "--module-id", "nope", "--json"])
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False

    code, out, _ = _run(capsys, ["package", "module", "list", "--package", str(package), "--search", "CORE", "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["count"] == 1
    assert [m["module_id"] for m in payload["modules"]] == ["module_core"]

    code, out, _ = _run(capsys, ["package", "module", "list", "--package", str(package), "--search", "nomatch", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["count"] == 0


def test_cli_role_get_found_and_missing(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "role", "get", "--package", str(package), "--role-id", "role_primary", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["role"]["role_id"] == "role_primary"

    code, out, _ = _run(capsys, ["package", "role", "get", "--package", str(package), "--role-id", "nope", "--json"])
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False


def test_cli_param_get_exact_missing_is_not_found(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "param", "get", "--package", str(package), "--param-id", "param.none", "--json"])
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False


def test_cli_param_get_search_single_and_ambiguous(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    # Уникальная подстрока → ровно одно совпадение.
    code, out, _ = _run(capsys, ["package", "param", "get", "--package", str(package), "--search", "delta", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["param"]["param_id"] == "param.delta_integration"

    # «capability» входит в три параметра → неоднозначность → EXIT_USAGE со списком.
    code, out, _ = _run(capsys, ["package", "param", "get", "--package", str(package), "--search", "capability", "--json"])
    assert code == EXIT_USAGE
    payload = json.loads(out)
    assert payload["ambiguous"] is True
    assert payload["count"] == 3
    assert {c["param_id"] for c in payload["candidates"]} == {
        "param.alpha_capability",
        "param.beta_capability",
        "param.gamma_capability",
    }


def test_cli_param_get_requires_a_source(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, _, err = _run(capsys, ["package", "param", "get", "--package", str(package)])
    assert code == EXIT_USAGE
    assert "--param-id" in err or "--search" in err


def test_cli_baseline_get_exact_and_ambiguous(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    _run(
        capsys,
        ["package", "baseline", "add", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.alpha_capability", "--note", "x", "--actor", "t", "--json"],
    )
    _run(
        capsys,
        ["package", "baseline", "add", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.beta_capability", "--actor", "t", "--json"],
    )
    # Точный ключ → вывод.
    code, out, _ = _run(
        capsys,
        ["package", "baseline", "get", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.alpha_capability", "--json"],
    )
    assert code == EXIT_OK
    assert json.loads(out)["baseline"]["param_id"] == "param.alpha_capability"

    # Точный ключ, которого нет → EXIT_NOT_FOUND.
    code, out, _ = _run(
        capsys,
        ["package", "baseline", "get", "--package", str(package), "--baseline-id", "up",
         "--param-id", "param.none", "--json"],
    )
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False

    # Только baseline_id, несколько совпадений → неоднозначность → EXIT_USAGE.
    code, out, _ = _run(
        capsys, ["package", "baseline", "get", "--package", str(package), "--baseline-id", "up", "--json"]
    )
    assert code == EXIT_USAGE
    payload = json.loads(out)
    assert payload["ambiguous"] is True
    assert payload["count"] == 2


def test_cli_scope_show_present_and_missing(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "scope", "show", "--package", str(package), "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["procedure"]
    (package / "scope_attestation.yaml").unlink()
    code, out, _ = _run(capsys, ["package", "scope", "show", "--package", str(package), "--json"])
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False


def test_cli_golden_list_search_and_filters(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "golden", "list", "--package", str(package), "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["count"] == 3

    code, out, _ = _run(capsys, ["package", "golden", "list", "--package", str(package), "--search", "class_ok", "--json"])
    assert [g["param_id"] for g in json.loads(out)["golden"]] == ["param.alpha_capability"]

    code, out, _ = _run(
        capsys, ["package", "golden", "list", "--package", str(package), "--product", "product_analog_b", "--json"]
    )
    assert {g["product_id"] for g in json.loads(out)["golden"]} == {"product_analog_b"}

    code, out, _ = _run(capsys, ["package", "golden", "list", "--package", str(package), "--search", "nomatch", "--json"])
    assert code == EXIT_OK and json.loads(out)["count"] == 0


def test_cli_run_show_present_and_missing(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "run", "show", "--package", str(package), "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["found"] is True
    assert payload["run"]["case_id"] == "demo_pkg"

    (package / "run.yaml").unlink()
    code, out, _ = _run(capsys, ["package", "run", "show", "--package", str(package), "--json"])
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False


def test_cli_artifact_list(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    code, out, _ = _run(capsys, ["package", "artifact", "list", "--package", str(package), "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["count"] == 9
    names = {item["name"] for item in payload["artifacts"]}
    assert names == {
        "package", "catalog", "baselines", "cohort",
        "golden", "domain_profiles", "scope_attestation", "run", "crawl_sources",
    }


def test_cli_artifact_get_structured_and_raw(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    # golden — структурный список строк.
    code, out, _ = _run(capsys, ["package", "artifact", "get", "--package", str(package), "--name", "golden", "--json"])
    assert code == EXIT_OK
    payload = json.loads(out)
    assert isinstance(payload["data"], list) and len(payload["data"]) == 3

    # run — структура (не сырой текст).
    code, out, _ = _run(capsys, ["package", "artifact", "get", "--package", str(package), "--name", "run", "--json"])
    assert code == EXIT_OK
    assert json.loads(out)["data"]["case_id"] == "demo_pkg"

    # domain_profiles — dict.
    code, out, _ = _run(capsys, ["package", "artifact", "get", "--package", str(package), "--name", "domain_profiles", "--json"])
    assert code == EXIT_OK and isinstance(json.loads(out)["data"], dict)

    # scope_attestation --raw — сырой текст YAML.
    code, out, _ = _run(capsys, ["package", "artifact", "get", "--package", str(package), "--name", "scope_attestation", "--raw"])
    assert code == EXIT_OK
    assert "procedure:" in out


def test_cli_artifact_get_absent_and_raw_json_conflict(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    # Отсутствующий артефакт → not found.
    (package / "run.yaml").unlink()
    code, out, _ = _run(capsys, ["package", "artifact", "get", "--package", str(package), "--name", "run", "--json"])
    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["found"] is False

    # --raw и --json несовместимы → usage (при --json ошибка печатается в stdout).
    code, out, _ = _run(capsys, ["package", "artifact", "get", "--package", str(package), "--name", "cohort", "--raw", "--json"])
    assert code == EXIT_USAGE
    assert "raw" in out.lower() or "json" in out.lower()

    # Неизвестное имя отсекается argparse (choices) → usage.
    code, _, _ = _run(capsys, ["package", "artifact", "get", "--package", str(package), "--name", "nope"])
    assert code == EXIT_USAGE


def test_cli_json_output_is_deterministic(capsys, tmp_path: Path) -> None:
    package = _new(capsys, tmp_path)
    _, first, _ = _run(capsys, ["package", "artifact", "list", "--package", str(package), "--json"])
    _, second, _ = _run(capsys, ["package", "artifact", "list", "--package", str(package), "--json"])
    assert first == second
    _, g1, _ = _run(capsys, ["package", "golden", "list", "--package", str(package), "--json"])
    _, g2, _ = _run(capsys, ["package", "golden", "list", "--package", str(package), "--json"])
    assert g1 == g2


# ---------------------------------------------------------------------------
# ``--package``: резолв case_id (путь ИЛИ case_id) + fail-closed на несуществующем пакете
# ---------------------------------------------------------------------------


def test_cli_package_bare_case_id_resolves_via_dir(capsys, tmp_path: Path) -> None:
    """``--package <case_id> --dir <packages>`` резолвится в каталог пакета."""
    _new(capsys, tmp_path)  # demo_pkg лежит в tmp_path
    code, out, _ = _run(
        capsys, ["package", "show", "--package", "demo_pkg", "--dir", str(tmp_path), "--json"]
    )
    assert code == EXIT_OK
    assert json.loads(out)["case_id"] == "demo_pkg"

    code, out, _ = _run(
        capsys, ["package", "run", "show", "--package", "demo_pkg", "--dir", str(tmp_path), "--json"]
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["found"] is True
    assert payload["run"]["case_id"] == "demo_pkg"


def test_cli_package_missing_case_id_fail_closed(capsys, tmp_path: Path) -> None:
    """Несуществующий ``case_id`` — ``EXIT_NOT_FOUND`` во всех read-командах (не ложный успех)."""
    for argv in (
        ["package", "show", "--package", "no_such_pkg", "--dir", str(tmp_path)],
        ["package", "validate", "--package", "no_such_pkg", "--dir", str(tmp_path)],
        ["package", "role", "list", "--package", "no_such_pkg", "--dir", str(tmp_path)],
        ["package", "golden", "list", "--package", "no_such_pkg", "--dir", str(tmp_path)],
        ["package", "artifact", "list", "--package", "no_such_pkg", "--dir", str(tmp_path)],
    ):
        code, _, _ = _run(capsys, argv)
        assert code == EXIT_NOT_FOUND, argv


def test_cli_validate_missing_path_is_not_found(capsys, tmp_path: Path) -> None:
    """``validate`` на несуществующем пути — ``EXIT_NOT_FOUND`` (не ``ok: yes``)."""
    code, out, _ = _run(
        capsys, ["package", "validate", "--package", str(tmp_path / "nope"), "--json"]
    )
    assert code == EXIT_NOT_FOUND
    payload = json.loads(out)
    assert payload["found"] is False
    assert payload["ok"] is False


def test_cli_show_missing_manifest_is_not_found(capsys, tmp_path: Path) -> None:
    """Существующий каталог без ``package.yaml`` — ``EXIT_NOT_FOUND`` для ``show``."""
    empty = tmp_path / "empty_pkg"
    empty.mkdir()
    code, out, _ = _run(capsys, ["package", "show", "--package", str(empty), "--json"])
    assert code == EXIT_NOT_FOUND
    payload = json.loads(out)
    assert payload["found"] is False
    assert payload["reason"] == "package.yaml отсутствует"


def test_cli_validate_existing_dir_without_manifest_is_blocked(capsys, tmp_path: Path) -> None:
    """Существующий каталог без ``package.yaml`` — не not-found, но fail-closed:
    ``EXIT_BLOCKED`` с блокирующим сообщением (не ложный ``ok: yes``)."""
    empty = tmp_path / "bare_dir"
    empty.mkdir()
    code, out, _ = _run(capsys, ["package", "validate", "--package", str(empty), "--json"])
    assert code == EXIT_BLOCKED
    payload = json.loads(out)
    assert payload["ok"] is False
    assert payload["lint"]["blocking"] == ["package.yaml отсутствует"]


