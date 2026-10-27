"""Тесты CLI-команды ``gate`` — запись человеческого решения гейта G1/G2 (S9/I6/I7).

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
import json
from pathlib import Path

import pytest

from core.cli.main import EXIT_BLOCKED, EXIT_OK, EXIT_USAGE, main
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
            "--subject-digest",
            "d_test",
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
        assert is_gate_passed(store, RUN_ID, "G1", "d_test") is True
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
            "--subject-digest",
            "d_test",
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
        [
            "gate",
            "--db",
            str(db),
            "--gate",
            "G1",
            "--actor",
            "tester",
            "--subject-digest",
            "d_test",
            "--json",
        ]
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
            "--subject-digest",
            "d_test",
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
        "--subject-digest",
        "d_test",
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
            "--subject-digest",
            "d_test",
        ]
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "gate: G1" in out
    assert "actor: tester" in out
    assert "решение записано" in out

# ---------------------------------------------------------------------------
# Точки интеграции G3/G4: источник, внешняя ссылка, вердикт, отпечаток предмета
# ---------------------------------------------------------------------------

def _gate_argv(db: Path, gate: str = "G3", digest: str = "d_req") -> list[str]:
    """Базовый argv решения гейта с привязкой к предмету."""
    return [
        "gate",
        "--db",
        str(db),
        "--run-id",
        RUN_ID,
        "--gate",
        gate,
        "--actor",
        "review-system",
        "--subject-digest",
        digest,
    ]

def test_gate_external_requires_ref_and_notes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Внешнее решение без ссылки или причины — ``EXIT_USAGE``, а не молчаливая запись."""
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main([*_gate_argv(db), "--source", "external", "--notes", "ревью"])
    assert code == EXIT_USAGE
    assert "external-ref" in capsys.readouterr().err

    code = main([*_gate_argv(db), "--source", "external", "--external-ref", "REV-1"])
    assert code == EXIT_USAGE
    assert "--notes" in capsys.readouterr().err

    with SqliteStore(db) as store:
        assert list(store.iter_gate_decisions(RUN_ID)) == []

def test_gate_external_approval_is_recorded(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Внешнее одобрение записывается с источником, ссылкой и отпечатком предмета."""
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        [
            *_gate_argv(db),
            "--source",
            "external",
            "--external-ref",
            "REV-17",
            "--notes",
            "ревью пройдено",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["source"] == "external"
    assert payload["external_ref"] == "REV-17"
    assert payload["subject_digest"] == "d_req"
    assert payload["verdict"] == "approved"

    with SqliteStore(db) as store:
        assert is_gate_passed(store, RUN_ID, "G3", "d_req") is True
        # Отпечаток обязателен: решение не проходит другой предмет.
        assert is_gate_passed(store, RUN_ID, "G3", "d_other") is False

def test_gate_rejected_verdict_is_recorded_and_blocks(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--verdict rejected`` — явная запись отказа: гейт не проходит, вывод честен.

    Код возврата — ``EXIT_BLOCKED`` (3), а не ``EXIT_OK``: прогон заблокирован отказом, и
    скрипт, автоматизирующий гейты, обязан увидеть это тем же кодом, каким ``run``
    сообщает об остановке на гейте (``12-cli-architecture-review.md`` §4.7).
    """
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        [
            *_gate_argv(db),
            "--source",
            "external",
            "--external-ref",
            "REV-18",
            "--notes",
            "две ячейки не подтверждены",
            "--verdict",
            "rejected",
            "--json",
        ]
    )
    assert code == EXIT_BLOCKED
    assert json_out(capsys)["verdict"] == "rejected"

    with SqliteStore(db) as store:
        assert is_gate_passed(store, RUN_ID, "G3", "d_req") is False

    code = main(
        [
            *_gate_argv(db),
            "--source",
            "external",
            "--external-ref",
            "REV-18",
            "--notes",
            "две ячейки не подтверждены",
            "--verdict",
            "rejected",
        ]
    )
    assert code == EXIT_BLOCKED
    text = capsys.readouterr().out
    assert "ОТКЛОНЁН" in text
    # Отказ называет, кем он принят, и что прогон заблокирован (а не «записано, всё хорошо»).
    assert "review-system" in text
    assert "заблокирован" in text

def test_gate_takes_digest_from_request_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без ``--subject-digest`` отпечаток берётся из ``gates/{gate}-request.json``."""
    runs_dir = tmp_path / "runs"
    db = runs_dir / RUN_ID / "cohound.db"
    _seed_run(db)
    gates_dir = runs_dir / RUN_ID / "gates"
    gates_dir.mkdir(parents=True)
    (gates_dir / "G3-request.json").write_text(
        json.dumps({"run_id": RUN_ID, "gate": "G3", "subject_digest": "d_from_request"}),
        encoding="utf-8",
    )

    code = main(
        [
            "gate",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            RUN_ID,
            "--gate",
            "G3",
            "--actor",
            "review-system",
            "--source",
            "external",
            "--external-ref",
            "REV-20",
            "--notes",
            "по запросу",
            "--json",
        ]
    )
    assert code == EXIT_OK
    assert json_out(capsys)["subject_digest"] == "d_from_request"

def test_gate_without_digest_or_request_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет ни ``--subject-digest``, ни файла запроса → ``EXIT_USAGE`` с объяснением."""
    db = tmp_path / "cohound.db"
    _seed_run(db)

    code = main(
        ["gate", "--db", str(db), "--run-id", RUN_ID, "--gate", "G1", "--actor", "h"]
    )
    assert code == EXIT_USAGE
    err = capsys.readouterr().err
    assert "subject-digest" in err or "G1-request.json" in err
    with SqliteStore(db) as store:
        assert list(store.iter_gate_decisions(RUN_ID)) == []
