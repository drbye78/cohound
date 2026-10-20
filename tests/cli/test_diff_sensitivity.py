"""Тесты CLI-команд ``diff`` и ``sensitivity`` (03 §D.6, §9/§13, §6/K6).

Покрывают:

* ``diff`` — успешный межцикловый diff двух прогонов в одной БД (изменение статуса
  ячейки) → ``diff.md``; отказ ``§13`` при расхождении ``index_build_id``
  (:class:`CrossCycleDiffError`, :data:`EXIT_ERROR`);
* ``sensitivity`` — человекочитаемые оси/устойчивость и JSON-форма
  (``per_axis``/``stable``/``unstable_pairs``/``requested_ranking``/``computed_ranking``);
  заметка-ограничение при отсутствии ``--run-yaml``/``--package`` и её отсутствие при передаче;
* argparse-отказ при отсутствующих обязательных аргументах.

Работают на ``tmp_path`` и ``SqliteStore``; фикстуры категорийно нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest

from core.cli.main import (
    EXIT_ERROR,
    EXIT_OK,
    main,
)
from core.domain.enums import (
    RankingPolicy,
    RunMode,
    Status,
)
from core.domain.models import Focus, RunConfig
from core.store.sqlite_store import SqliteStore
from tests.helpers.domain import cell
from tests.helpers.output import json_out
from tests.helpers.packages import valid_package

PRODUCT_A = "p1"
PRODUCT_B = "p2"
PARAM_A = "param_a"


# ---------------------------------------------------------------------------
# Помощники (категорийно нейтрально)
# ---------------------------------------------------------------------------


def _cfg() -> RunConfig:
    """Общий конфиг прогона для обоих циклов (одинаковая когорта/case_id)."""
    return RunConfig(
        case_id="case_x",
        anchor=PRODUCT_A,
        cohort="cohort_case_x",
        products=[PRODUCT_A, PRODUCT_B],
        package="case_x",
        focus=Focus(modules=["mod_a"], params=[PARAM_A]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _seed_cycle(
    db_path: Path,
    run_id: str,
    *,
    cells: list[tuple[str, str, Status]],
    methodology_version: str = "2.0.0",
    index_build_id: str = "build.1",
    catalog_version: str = "1.0.0",
) -> None:
    """Создаёт прогон с ячейками и версионным контуром (для §13-границы)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db_path) as store:
        store.create_run(run_id, _cfg())
        store.conn.execute(
            "UPDATE runs SET methodology_version = ?, package_version = ?,"
            " catalog_version = ?, index_build_id = ?, retrieval_profile = ?,"
            " retrieval_cutoff_K = ? WHERE run_id = ?",
            (
                methodology_version,
                "1.0.0",
                catalog_version,
                index_build_id,
                "R1",
                10,
                run_id,
            ),
        )
        for product_id, param_id, status in cells:
            store.save_cell(cell(run_id, product_id, param_id, status))
        store.conn.commit()


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------


def test_diff_happy_path_writes_diff_md(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_cycle(db, "run.a", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    _seed_cycle(db, "run.b", cells=[(PRODUCT_A, PARAM_A, Status.ABSENT)])

    runs_dir = tmp_path / "runs"
    code = main(
        [
            "diff",
            "run.a",
            "run.b",
            "--db",
            str(db),
            "--runs-dir",
            str(runs_dir),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload == {
        "ok": True,
        "out": str(runs_dir / "run.b" / "outputs" / "diff.md"),
        "run_a": "run.a",
        "run_b": "run.b",
    }
    out = Path(payload["out"])
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "# diff: run.a → run.b" in text
    assert "✅" in text and "❌" in text


def test_diff_human_prints_written_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_cycle(db, "run.a", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    _seed_cycle(db, "run.b", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    out_path = tmp_path / "custom" / "diff.md"

    code = main(
        ["diff", "run.a", "run.b", "--db", str(db), "--out", str(out_path)]
    )
    assert code == EXIT_OK
    assert str(out_path) in capsys.readouterr().out
    assert out_path.exists()


def test_diff_across_boundary_returns_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_cycle(db, "run.a", cells=[(PRODUCT_A, PARAM_A, Status.OK)], index_build_id="build.1")
    _seed_cycle(db, "run.b", cells=[(PRODUCT_A, PARAM_A, Status.OK)], index_build_id="build.2")

    code = main(["diff", "run.a", "run.b", "--db", str(db), "--runs-dir", str(tmp_path)])
    assert code == EXIT_ERROR
    err = capsys.readouterr().err
    assert "§13" in err
    assert "index_build_id" in err


def test_diff_refuses_catalog_change_as_execution_change(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """§13:670: смена ``catalog_version`` — граница ``execution_change``.

    После удаления ``params_token`` из формулы id (09 §8.1) это единственный
    diff-видимый сигнал смены каталога; без него diff сравнил бы несопоставимые
    условия измерения молча.
    """
    db = tmp_path / "cohound.db"
    _seed_cycle(
        db,
        "run.a",
        cells=[(PRODUCT_A, PARAM_A, Status.OK)],
        catalog_version="1.0.0",
    )
    _seed_cycle(
        db,
        "run.b",
        cells=[(PRODUCT_A, PARAM_A, Status.OK)],
        catalog_version="2.0.0",
    )

    code = main(["diff", "run.a", "run.b", "--db", str(db), "--json"])
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert payload["error"] == "§13"
    assert "execution_change" in payload["message"]
    assert "catalog_version" in payload["message"]


def test_diff_across_boundary_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_cycle(
        db, "run.a", cells=[(PRODUCT_A, PARAM_A, Status.OK)], methodology_version="2.0.0"
    )
    _seed_cycle(
        db, "run.b", cells=[(PRODUCT_A, PARAM_A, Status.OK)], methodology_version="3.0.0"
    )

    code = main(["diff", "run.a", "run.b", "--db", str(db), "--json"])
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert payload["error"] == "§13"
    assert "methodology_version major" in payload["message"]


def test_diff_runs_dir_resolution(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без ``--db`` каждый цикл резолвится как ``runs/{run_id}/cohound.db``."""
    runs_dir = tmp_path / "runs"
    _seed_cycle(runs_dir / "run.a" / "cohound.db", "run.a", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    _seed_cycle(runs_dir / "run.b" / "cohound.db", "run.b", cells=[(PRODUCT_A, PARAM_A, Status.ABSENT)])

    code = main(["diff", "run.a", "run.b", "--runs-dir", str(runs_dir), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert Path(payload["out"]).exists()


# ---------------------------------------------------------------------------
# sensitivity
# ---------------------------------------------------------------------------


def _seed_sensitivity_run(db: Path) -> None:
    _seed_cycle(
        db,
        "run.s",
        cells=[
            (PRODUCT_A, PARAM_A, Status.OK),
            (PRODUCT_B, PARAM_A, Status.ABSENT),
        ],
    )


def test_sensitivity_json_shape(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_sensitivity_run(db)

    code = main(
        ["sensitivity", "--db", str(db), "--run-id", "run.s", "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["run_id"] == "run.s"
    assert payload["stable"] in (True, False)
    assert set(payload["per_axis"]) == {"partial_score", "unknown_policy", "weights"}
    assert isinstance(payload["unstable_pairs"], list)
    assert payload["requested_ranking"] == "capability_ranked"
    assert payload["computed_ranking"] in ("capability_ranked", "coverage_fallback")
    # Прогон посеян напрямую (без записи ``run_inputs``): каталог пуст → заметка обязательна.
    assert payload["notes"]


def test_sensitivity_human_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_sensitivity_run(db)

    code = main(["sensitivity", "--db", str(db), "--run-id", "run.s"])
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "run_id: run.s" in out
    assert "stable:" in out
    assert "axis partial_score:" in out
    assert "note:" in out


def test_sensitivity_runs_dir_resolution(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    _seed_sensitivity_run(runs_dir / "run.s" / "cohound.db")

    code = main(
        ["sensitivity", "--runs-dir", str(runs_dir), "--run-id", "run.s", "--json"]
    )
    assert code == EXIT_OK
    assert json_out(capsys)["run_id"] == "run.s"


def test_sensitivity_infers_single_run_from_db(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_sensitivity_run(db)

    code = main(["sensitivity", "--db", str(db), "--json"])
    assert code == EXIT_OK
    assert json_out(capsys)["run_id"] == "run.s"


def test_sensitivity_with_package_has_no_gap_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_sensitivity_run(db)
    package = valid_package(tmp_path, scope_attestation=False)

    code = main(
        [
            "sensitivity",
            "--db",
            str(db),
            "--run-id",
            "run.s",
            "--run-yaml",
            str(package / "run.yaml"),
            "--package",
            str(package),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["notes"] == []
    # Каталог из пакета применён: ось весов присутствует в форме плана.
    assert set(payload["per_axis"]["weights"]["rankings"]) == {"declared", "uniform"}
