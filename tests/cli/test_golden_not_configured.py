"""Тесты CLI: эталон объявлен неприменимым (``golden_status/1``) — не молчание и не провал.

Предмет — поведение снаружи: пакет без ``golden.csv`` **планируется** (``plan`` → 0),
состояние эталона видно в ``plan --json``, а ``hound golden`` при ``absent``/``declined``
отвечает «неприменимо» кодом :data:`~core.cli.errors.EXIT_USAGE` с
``error: golden_not_configured`` и причиной из объявления. Это **не** ``EXIT_OK``
(«не выполнилось» не должно выглядеть зелёным) и **не** ``EXIT_ERROR`` (пороги не
считались — сравнивать было не с чем).

Противоречивое объявление — видимая ошибка пакета с кодом ``golden_status.yaml``, а не
общий ``error: E``.

Данные категорийно-нейтральны (§1.3); ``tmp_path`` + ``SqliteStore``.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from core.cli.main import EXIT_ERROR, EXIT_OK, EXIT_USAGE, main
from core.domain.enums import RankingPolicy, RunMode, Status
from core.domain.models import Focus, RunConfig
from core.store.sqlite_store import SqliteStore
from tests.helpers.domain import cell
from tests.helpers.output import json_out
from tests.helpers.packages import valid_package

PRODUCT_A = "p1"
PRODUCT_B = "p2"
PARAM_A = "param_a"
PARAM_B = "param_b"

_RUN_ID = "run.g"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor=PRODUCT_A,
        cohort="cohort_case_x",
        products=[PRODUCT_A, PRODUCT_B],
        package="case_x",
        focus=Focus(modules=["mod_a", "mod_b"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _seed_run(db_path: Path, run_id: str = _RUN_ID) -> None:
    """Прогон с двумя ячейками: эталон может полностью совпасть."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db_path) as store:
        store.create_run(run_id, _cfg())
        store.save_cell(cell(run_id, PRODUCT_A, PARAM_A, Status.OK))
        store.save_cell(cell(run_id, PRODUCT_B, PARAM_B, Status.ABSENT))
        store.conn.commit()


def _write_golden(path: Path) -> Path:
    """Эталон, совпадающий с ячейками ``_seed_run`` (fast + full + страта ❌)."""
    header = "product_id,param_id,expected_status,expected_evidence_kind,case_class,tier"
    rows = [
        f"{PRODUCT_A},{PARAM_A},✅,documentation,class_ok,fast",
        f"{PRODUCT_B},{PARAM_B},❌,registry,class_absent,full",
    ]
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


def _declare(package: Path, text: str) -> None:
    """Пишет объявление состояния эталона в ``{package}/sources/golden_status.yaml``."""
    path = package / "sources" / "golden_status.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _declined(reason: str = "разметка непригодна: не те продукты") -> str:
    return (
        "schema: golden_status/1\n"
        "state: declined\n"
        f'reason: "{reason}"\n'
        "declined_by: analyst\n"
        "declined_at: '2026-01-31'\n"
    )


def _plan_argv(package: Path) -> list[str]:
    return [
        "plan",
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--json",
    ]


# ---------------------------------------------------------------------------
# plan: отсутствие эталона — режим, а не падение
# ---------------------------------------------------------------------------


def test_plan_without_golden_file_is_ok(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Пакет без ``golden.csv`` планируется: сверка не выполняется, состояние объявлено."""
    package = valid_package(tmp_path)
    (package / "golden.csv").unlink()

    assert main(_plan_argv(package)) == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["golden_count"] == 0
    assert payload["golden_state"] == "absent"
    assert payload["golden_declared"] is False
    assert payload["golden_file_present"] is False
    # Шаблонного lint-warning'а про отсутствие здесь не ждём: `plan` подменяет
    # `warnings` env-заметками, когда они есть (пре-существующее поведение
    # `core/cli/commands/plan.py`) — поэтому предупреждение проверяется на уровне
    # линтера (`tests/pipeline/test_lint.py`), а состояние — отдельными полями выше.


def test_plan_declared_absent_shows_state_without_warning(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``state: absent`` — решение автора, поэтому шаблонного warning'а нет."""
    package = valid_package(tmp_path)
    (package / "golden.csv").unlink()
    _declare(package, "schema: golden_status/1\nstate: absent\n")

    assert main(_plan_argv(package)) == EXIT_OK
    payload = json_out(capsys)
    assert payload["golden_state"] == "absent"
    assert payload["golden_declared"] is True
    assert not any("golden" in warning for warning in payload["warnings"])


def test_plan_declined_shows_state_and_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``declined``: файл остаётся, причина и автор видны ещё до прогона."""
    package = valid_package(tmp_path)
    _declare(package, _declined())

    assert main(_plan_argv(package)) == EXIT_OK
    payload = json_out(capsys)
    assert payload["golden_count"] == 0
    assert payload["golden_state"] == "declined"
    assert payload["golden_declared"] is True
    assert payload["golden_file_present"] is True
    assert "не те продукты" in payload["golden_reason"]
    assert payload["golden_declined_by"] == "analyst"
    assert not any("golden" in warning for warning in payload["warnings"])
    # Объявление не удаляет данные: файл на месте.
    assert (package / "golden.csv").is_file()


def test_plan_declined_without_reason_is_visible_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Причина обязательна: иначе объявление было бы тихим waiver'ом."""
    package = valid_package(tmp_path)
    _declare(package, "schema: golden_status/1\nstate: declined\n")

    assert main(_plan_argv(package)) == EXIT_USAGE
    payload = json_out(capsys)
    assert payload["error"] == "golden_status.yaml"
    assert "declined" in payload["message"]


def test_plan_present_without_file_is_visible_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    (package / "golden.csv").unlink()
    _declare(package, "schema: golden_status/1\nstate: present\n")

    assert main(_plan_argv(package)) == EXIT_USAGE
    payload = json_out(capsys)
    assert payload["error"] == "golden_status.yaml"
    assert "golden.csv отсутствует" in payload["message"]


# ---------------------------------------------------------------------------
# golden: «неприменимо» — отдельный исход
# ---------------------------------------------------------------------------


def test_golden_declined_is_not_configured(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db)
    package = valid_package(tmp_path)
    _write_golden(package / "golden.csv")
    _declare(package, _declined("разметка непригодна: не те продукты"))

    code = main(
        [
            "golden",
            "--db",
            str(db),
            "--run-id",
            _RUN_ID,
            "--package",
            str(package),
            "--json",
        ]
    )

    assert code == EXIT_USAGE, "неприменимо — не успех и не провал порога"
    payload = json_out(capsys)
    assert payload["error"] == "golden_not_configured"
    assert payload["golden_state"] == "declined"
    assert "не те продукты" in payload["golden_reason"]
    assert "не те продукты" in payload["message"]


def test_golden_absent_is_not_configured(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db)
    package = valid_package(tmp_path)
    (package / "golden.csv").unlink()

    code = main(
        ["golden", "--db", str(db), "--run-id", _RUN_ID, "--package", str(package), "--json"]
    )

    assert code == EXIT_USAGE
    payload = json_out(capsys)
    assert payload["error"] == "golden_not_configured"
    assert payload["golden_state"] == "absent"
    assert payload["golden_file_present"] is False


def test_golden_present_compares_and_passes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``present`` — прежнее поведение: сверка идёт, пороги считаются."""
    db = tmp_path / "cohound.db"
    _seed_run(db)
    package = valid_package(tmp_path)
    _write_golden(package / "golden.csv")
    _declare(package, "schema: golden_status/1\nstate: present\n")

    code = main(
        ["golden", "--db", str(db), "--run-id", _RUN_ID, "--package", str(package), "--json"]
    )

    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["total"] == 2
    assert payload["golden_source"] == "package"
    assert payload["golden_state"] == "present"


def test_golden_absent_fails_thresholds_when_given_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Явный ``--golden`` работает как раньше: провал порога — ``EXIT_ERROR``."""
    db = tmp_path / "cohound.db"
    _seed_run(db)
    golden = _write_golden(tmp_path / "golden.csv")
    golden.write_text(
        "product_id,param_id,expected_status,expected_evidence_kind,case_class,tier\n"
        f"{PRODUCT_A},{PARAM_A},❌,documentation,class_ok,full\n",
        encoding="utf-8",
    )

    code = main(["golden", "--golden", str(golden), "--db", str(db), "--run-id", _RUN_ID, "--json"])

    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["golden_source"] == "cli"
    assert payload["golden_state"] is None, "объявление не читали — не выдаём его за прочитанное"


def test_golden_explicit_file_against_declined_package_is_conflict(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Молча выбирать сторону нельзя: противоречие «явный файл ↔ declined» видимо."""
    db = tmp_path / "cohound.db"
    _seed_run(db)
    package = valid_package(tmp_path)
    golden = _write_golden(package / "golden.csv")
    _declare(package, _declined())

    code = main(
        [
            "golden",
            "--golden",
            str(golden),
            "--db",
            str(db),
            "--run-id",
            _RUN_ID,
            "--package",
            str(package),
            "--json",
        ]
    )

    assert code == EXIT_USAGE
    payload = json_out(capsys)
    assert payload["error"] == "golden_status_conflict"
