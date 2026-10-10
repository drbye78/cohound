"""Самодостаточность каталога прогона: снимок входов (``03`` §A.2).

``cmd_run`` записывает в ``runs/{run_id}/`` иммутабельную копию ``run.yaml`` и снимок
входов плана ``plan_inputs.json`` (каталог/веса/когорта). Последующие команды
(``sensitivity``/``report``/``golden``) восстанавливают из них каталог **без** исходного
пакета. Явные ``--run-yaml``/``--package`` сохраняют приоритет; при отсутствии обоих
источников сохраняется прежний ГЭП-путь.

Фикстуры категорийно нейтральны (§1.3): ``tests/fixtures/e2e`` + ``tests/config/fixtures``.
Сеть не используется: прогон доходит лишь до человеческого гейта ``g1``.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import (
    EXIT_BLOCKED,
    EXIT_OK,
    PLAN_INPUTS_SNAPSHOT_VERSION,
    main,
)
from tests.helpers.output import json_out
from tests.helpers.packages import run_argv

#: Корень e2e-фикстур (``tests/fixtures/e2e``) — валидный пакет для ``run``.
E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"
#: Фикстуры пакета (``tests/config/fixtures``) — второй (отличный) каталог для приоритета.
CONFIG_FIXTURES = Path(__file__).resolve().parents[1] / "config" / "fixtures"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _config_package(tmp_path: Path) -> Path:
    """Копия ``tests/config/fixtures`` — отличный от e2e каталог (param_a)."""
    package = tmp_path / "config_package"
    package.mkdir()
    for item in CONFIG_FIXTURES.iterdir():
        if item.is_file():
            shutil.copy(item, package / item.name)
    return package


def _notes(payload: dict[str, Any]) -> str:
    return " ".join(payload.get("notes") or [])


# ---------------------------------------------------------------------------
# cmd_run: запись снимка
# ---------------------------------------------------------------------------


def test_run_writes_immutable_run_yaml_and_plan_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-run"

    code = main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir))
    assert code == EXIT_BLOCKED  # g1 — человеческое решение (I6/I7)
    capsys.readouterr()

    run_dir = runs_dir / run_id
    # run.yaml — байт-в-байт копия входа (иммутабельный вход, 03 §A.2).
    assert (run_dir / "run.yaml").read_bytes() == (E2E_FIXTURES / "run.yaml").read_bytes()

    snapshot_path = run_dir / "plan_inputs.json"
    assert snapshot_path.is_file()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    assert snapshot["snapshot_version"] == PLAN_INPUTS_SNAPSHOT_VERSION
    assert snapshot["run_id"] == run_id
    assert snapshot["package_id"] == "e2e"
    assert snapshot["package_version"] == "1.0.0"
    assert snapshot["case_id"] == "e2e"
    # Каталог, веса и когорта присутствуют (достаточно для пересборки без пакета).
    assert {row["param_id"] for row in snapshot["params"]} == {"feature.alpha"}
    assert snapshot["renormalized_weights"] == {"m1": 1.0}
    assert snapshot["cohort"]["case_id"] == "e2e"
    assert {entry["product_id"] for entry in snapshot["cohort"]["entries"]} == {"p1"}
    # Детерминированный JSON: ключи отсортированы.
    assert list(snapshot) == sorted(snapshot)


def test_run_dry_run_writes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-dry"

    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir, dry_run=True)) == EXIT_OK
    assert json_out(capsys)["dry_run"] is True
    assert not (runs_dir / run_id).exists()
    assert not (runs_dir / run_id / "run.yaml").exists()
    assert not (runs_dir / run_id / "plan_inputs.json").exists()


def test_second_run_does_not_overwrite_differing_run_yaml(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-immutable"

    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    target = runs_dir / run_id / "run.yaml"
    tampered = b"case_id: tampered\n"
    target.write_bytes(tampered)

    # Повторный прогон (resume) не должен мутировать завершённый вход.
    code = main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir))
    assert code == EXIT_BLOCKED
    assert target.read_bytes() == tampered


# ---------------------------------------------------------------------------
# Пост-хок команды: восстановление из снимка
# ---------------------------------------------------------------------------


def test_sensitivity_restores_from_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-sens"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    # Ни --run-yaml, ни --package: единственный источник — снимок прогона.
    code = main(
        ["sensitivity", "--runs-dir", str(runs_dir), "--run-id", run_id, "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["run_id"] == run_id
    notes = _notes(payload)
    assert "plan_inputs.json" in notes
    assert "ГЭП" not in notes


def test_sensitivity_restores_from_snapshot_via_db_parent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--db runs/{run_id}/cohound.db`` выводит каталог прогона из родителя БД."""
    runs_dir = tmp_path / "runs"
    run_id = "sc-db-parent"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    code = main(
        [
            "sensitivity",
            "--db",
            str(runs_dir / run_id / "cohound.db"),
            "--run-id",
            run_id,
            "--json",
        ]
    )
    assert code == EXIT_OK
    assert "plan_inputs.json" in _notes(json_out(capsys))


def test_report_restores_from_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-report"
    out = tmp_path / "out"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    code = main(
        [
            "report",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            run_id,
            "--out",
            str(out),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    notes = _notes(payload)
    assert "plan_inputs.json" in notes
    assert "ГЭП" not in notes
    assert Path(payload["artifacts"]["manifest"]).exists()


def test_golden_restores_from_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-golden"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    # Код возврата зависит от вердиктов (ячеек нет → приёмка не пройдена); проверяем
    # именно источник каталога: снимок использован, ГЭП-заметки нет.
    main(
        [
            "golden",
            "--golden",
            str(E2E_FIXTURES / "golden.csv"),
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            run_id,
            "--json",
        ]
    )
    payload = json_out(capsys)
    notes = _notes(payload)
    assert "plan_inputs.json" in notes
    assert "ГЭП" not in notes


def test_explicit_inputs_take_priority_over_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-explicit"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    package = _config_package(tmp_path)  # отличный каталог (param_a), не снимок e2e
    code = main(
        [
            "sensitivity",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            run_id,
            "--run-yaml",
            str(package / "run.yaml"),
            "--package",
            str(package),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    # Явный источник: ни restore-заметки, ни ГЭП (снимок не читался).
    assert payload["notes"] == []


def test_missing_snapshot_keeps_gap_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без снимка и без явных входов сохраняется прежний ГЭП-путь (не выдумываем)."""
    from core.cli.wiring import run_db_path  # локально: helper раскладки

    runs_dir = tmp_path / "runs"
    run_id = "sc-gap"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    (runs_dir / run_id / "plan_inputs.json").unlink()
    assert run_db_path(runs_dir, run_id).exists()

    code = main(
        ["sensitivity", "--runs-dir", str(runs_dir), "--run-id", run_id, "--json"]
    )
    assert code == EXIT_OK
    assert "ГЭП" in _notes(json_out(capsys))
