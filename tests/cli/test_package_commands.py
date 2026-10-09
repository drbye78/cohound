"""Тесты CLI ``hound package …`` (итерация I1, ``package_commands``).

Сквозные сценарии через :func:`core.cli.main.main`: new → module/role/param CRUD →
validate → infer/compose, включая коды выхода и ``--dry-run``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.cli.main import EXIT_BLOCKED, EXIT_OK, EXIT_USAGE, main
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
    assert json.loads(out)["ok"] is True

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
    assert code == EXIT_USAGE
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
