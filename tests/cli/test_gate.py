"""Тесты CLI-команды ``gate`` — запись человеческого решения гейта G1–G4 (S9/I6/I7).

Покрывают:

* утверждение гейта через ``--db`` и через ``--runs-dir`` + ``--run-id``;
* вывод ``run_id`` из единственного прогона в БД (без ``--run-id``);
* обязательность ``--actor`` (argparse) и пустой ``actor`` (``ValueError`` → ``EXIT_USAGE``);
* несуществующий прогон (``read_manifest`` → ``EXIT_USAGE``);
* идемпотентность: повторная подача даёт тот же ``decision_id`` и ровно одну строку;
* человекочитаемый вывод.

Данные категорийно-нейтральны (§1.3): ``p1``/``p2``, ``case_x``, ``mod_a``/``mod_b``.
БД — ``tmp_path`` + :class:`SqliteStore`; сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.pipeline.gates import gate_decision_for, is_gate_passed
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

PRODUCT_A = "p1"
PRODUCT_B = "p2"
RUN_ID = "run.r"


# ---------------------------------------------------------------------------
# Помощники (категорийно нейтрально)
# ---------------------------------------------------------------------------


def _cfg() -> RunConfig:
    """Конфиг прогона с двумя продуктами и фокусом на два модуля."""
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


def _seed_run(db_path: Path, *, run_id: str = RUN_ID) -> None:
    """Создаёт прогон (строка ``runs``) для валидации ``read_manifest``."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db_path) as store:
        store.create_run(run_id, _cfg())
        store.conn.commit()


# ---------------------------------------------------------------------------
# gate
# ---------------------------------------------------------------------------


def test_gate_approve_g1_via_db(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        [
            "gate",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--gate",
            "G1",
            "--actor",
            "tester",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["gate"] == "G1"
    assert payload["run_id"] == RUN_ID
    assert payload["actor"] == "tester"
    assert payload["decision_id"]
    assert payload["decided_at"]

    with SqliteStore(db) as store:
        assert is_gate_passed(store, RUN_ID, "G1") is True
        assert gate_decision_for(store, RUN_ID, "G1") is not None


def test_gate_approve_via_runs_dir(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir / RUN_ID / "cohound.db")

    code = main(
        [
            "gate",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            RUN_ID,
            "--gate",
            "G2",
            "--actor",
            "tester",
            "--target",
            "p1/role_a",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["gate"] == "G2"
    assert payload["target"] == "p1/role_a"


def test_gate_infers_run_id_for_single_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        ["gate", "--db", str(db), "--gate", "G1", "--actor", "tester", "--json"]
    )
    assert code == EXIT_OK
    assert json_out(capsys)["run_id"] == RUN_ID


def test_gate_missing_actor_is_usage_error() -> None:
    assert (
        main(["gate", "--db", "x.db", "--run-id", RUN_ID, "--gate", "G1"])
        == EXIT_USAGE
    )


def test_gate_empty_actor_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Пустой ``actor`` → ``ValueError`` (I6/I7) → ``EXIT_USAGE``."""
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        [
            "gate",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--gate",
            "G1",
            "--actor",
            "   ",
        ]
    )
    assert code == EXIT_USAGE
    assert "I6/I7" in capsys.readouterr().err


def test_gate_nonexistent_run_is_usage_error(tmp_path: Path) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        [
            "gate",
            "--db",
            str(db),
            "--run-id",
            "run.missing",
            "--gate",
            "G1",
            "--actor",
            "tester",
        ]
    )
    assert code == EXIT_USAGE


def test_gate_idempotent_same_decision_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db)
    argv = [
        "gate",
        "--db",
        str(db),
        "--run-id",
        RUN_ID,
        "--gate",
        "G1",
        "--actor",
        "tester",
        "--json",
    ]

    assert main(argv) == EXIT_OK
    first_id = json_out(capsys)["decision_id"]
    assert main(argv) == EXIT_OK
    second_id = json_out(capsys)["decision_id"]
    assert first_id == second_id

    with SqliteStore(db) as store:
        rows = [d for d in store.iter_gate_decisions(RUN_ID) if d.gate.value == "G1"]
        assert len(rows) == 1


def test_gate_human_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        [
            "gate",
            "--db",
            str(db),
            "--run-id",
            RUN_ID,
            "--gate",
            "G1",
            "--actor",
            "tester",
        ]
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "gate: G1" in out
    assert "actor: tester" in out
    assert "решение записано" in out
