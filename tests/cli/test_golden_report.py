"""Тесты CLI-команд ``golden`` и ``report`` (03 §D.6, §9/§11.3, R6/R12/Ш9).

Покрывают:

* ``golden`` — проход (agreement + страта ❌) → ``EXIT_OK``; нарушение (false ✅ /
  отсутствие страты ❌) → ``EXIT_ERROR``; ``--json``-форма (поля отчёта +
  ``violations``/``notes``); фильтр ``--tier`` до оценки; argparse-обязательные
  аргументы;
* ``report`` — пересборка артефактов из БД в ``{out}/{run_id}/outputs/`` (``--json``
  отдаёт пути), вывод заметок-ограничений без ``--run-yaml``/``--package``, пометка
  незаполненного манифеста, резолв ``--runs-dir`` и вывод ``run_id`` по единственному
  прогону.

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


# ---------------------------------------------------------------------------
# Помощники (категорийно нейтрально)
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


def _seed_run(
    db_path: Path,
    run_id: str,
    *,
    cells: list[tuple[str, str, Status]],
    methodology_version: str = "2.0.0",
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db_path) as store:
        store.create_run(run_id, _cfg())
        store.conn.execute(
            "UPDATE runs SET methodology_version = ? WHERE run_id = ?",
            (methodology_version, run_id),
        )
        for product_id, param_id, status in cells:
            store.save_cell(cell(run_id, product_id, param_id, status))
        store.conn.commit()


def _write_golden(path: Path, rows: list[str]) -> Path:
    header = "product_id,param_id,expected_status,expected_evidence_kind,case_class,tier"
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# golden
# ---------------------------------------------------------------------------


def _golden_argv(db: Path, golden: Path, run_id: str = "run.g") -> list[str]:
    return ["golden", "--golden", str(golden), "--db", str(db), "--run-id", run_id]


def test_golden_pass_returns_ok(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(
        db,
        "run.g",
        cells=[(PRODUCT_A, PARAM_A, Status.OK), (PRODUCT_B, PARAM_B, Status.ABSENT)],
    )
    golden = _write_golden(
        tmp_path / "golden.csv",
        [f"{PRODUCT_A},{PARAM_A},✅,documentation,class_a,fast",
         f"{PRODUCT_B},{PARAM_B},❌,registry,class_b,full"],
    )

    code = main([*_golden_argv(db, golden), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["run_id"] == "run.g"
    assert payload["agreement"] == pytest.approx(1.0)
    assert payload["total"] == 2
    assert payload["has_absent_stratum"] is True
    assert payload["violations"] == []
    # Нет --run-yaml/--package и нет записи run_inputs → явная заметка-ограничение.
    assert payload["notes"]


def test_golden_false_ok_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(
        db,
        "run.g",
        cells=[
            (PRODUCT_A, PARAM_A, Status.OK),
            (PRODUCT_B, PARAM_B, Status.ABSENT),
            (PRODUCT_A, PARAM_B, Status.OK),  # ожидается ❌ → false ✅
        ],
    )
    golden = _write_golden(
        tmp_path / "golden.csv",
        [f"{PRODUCT_A},{PARAM_A},✅,documentation,class_a,fast",
         f"{PRODUCT_B},{PARAM_B},❌,registry,class_b,full",
         f"{PRODUCT_A},{PARAM_B},❌,registry,class_b,full"],
    )

    code = main([*_golden_argv(db, golden), "--json"])
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert payload["false_ok"] == 1
    assert any("false ✅" in v for v in payload["violations"])


def test_golden_missing_absent_stratum_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """R12/§4.2: даже при agreement 1.0 нет страты ❌ → отказ."""
    db = tmp_path / "cohound.db"
    _seed_run(db, "run.g", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    golden = _write_golden(
        tmp_path / "golden.csv",
        [f"{PRODUCT_A},{PARAM_A},✅,documentation,class_a,full"],
    )

    code = main([*_golden_argv(db, golden), "--json"])
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["has_absent_stratum"] is False
    assert any("страты" in v for v in payload["violations"])


def test_golden_tier_filter_applied_before_evaluation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(
        db,
        "run.g",
        cells=[
            (PRODUCT_A, PARAM_A, Status.OK),
            (PRODUCT_B, PARAM_B, Status.ABSENT),
            (PRODUCT_A, PARAM_B, Status.OK),  # false ✅ только в full
        ],
    )
    golden = _write_golden(
        tmp_path / "golden.csv",
        [f"{PRODUCT_A},{PARAM_A},✅,documentation,class_a,fast",
         f"{PRODUCT_B},{PARAM_B},❌,registry,class_b,fast",
         f"{PRODUCT_A},{PARAM_B},❌,registry,class_b,full"],
    )

    # Без фильтра — full-строка ломает приёмку.
    assert main([*_golden_argv(db, golden), "--json"]) == EXIT_ERROR
    capsys.readouterr()

    code = main([*_golden_argv(db, golden), "--tier", "fast", "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["tier"] == "fast"
    assert payload["total"] == 2
    assert payload["ok"] is True


def test_golden_with_package_catalog_has_no_gap_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(
        db,
        "run.g",
        cells=[(PRODUCT_A, PARAM_A, Status.OK), (PRODUCT_B, PARAM_B, Status.ABSENT)],
    )
    golden = _write_golden(
        tmp_path / "golden.csv",
        [f"{PRODUCT_A},{PARAM_A},✅,documentation,class_a,full",
         f"{PRODUCT_B},{PARAM_B},❌,registry,class_b,full"],
    )
    package = valid_package(tmp_path, scope_attestation=False)

    code = main(
        [
            *_golden_argv(db, golden),
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


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def _report_argv(db: Path, out: Path, run_id: str = "run.r") -> list[str]:
    return [
        "report",
        "--db",
        str(db),
        "--run-id",
        run_id,
        "--out",
        str(out),
        "--json",
    ]


def test_report_writes_artifacts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(
        db,
        "run.r",
        cells=[(PRODUCT_A, PARAM_A, Status.OK), (PRODUCT_B, PARAM_B, Status.ABSENT)],
    )
    package = valid_package(tmp_path, scope_attestation=False)
    out = tmp_path / "runs"

    code = main(
        [
            *_report_argv(db, out),
            "--run-yaml",
            str(package / "run.yaml"),
            "--package",
            str(package),
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert Path(payload["out"]) == out / "run.r" / "outputs"
    assert payload["manifest_partial"] is False
    assert payload["notes"] == []

    artifacts = payload["artifacts"]
    assert Path(artifacts["manifest"]).exists()
    assert Path(artifacts["nda_questions"]).exists()
    assert Path(artifacts["diff"]).exists()
    assert Path(artifacts["preliminary_findings"]).exists()


def test_report_without_run_inputs_notes_limit_and_still_writes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Прогон без записи ``run_inputs`` → пустой каталог и заметка-ограничение."""
    db = tmp_path / "cohound.db"
    _seed_run(db, "run.r", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    out = tmp_path / "runs"

    code = main(_report_argv(db, out))
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["notes"]
    assert any("отсутствуют в БД прогона" in note for note in payload["notes"])
    assert Path(payload["artifacts"]["manifest"]).exists()


def test_report_infers_single_run_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, "run.r", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    out = tmp_path / "runs"

    code = main(
        ["report", "--db", str(db), "--out", str(out), "--json"]
    )
    assert code == EXIT_OK
    assert json_out(capsys)["run_id"] == "run.r"


def test_report_runs_dir_resolution(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir / "run.r" / "cohound.db", "run.r", cells=[(PRODUCT_A, PARAM_A, Status.OK)])

    code = main(
        [
            "report",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            "run.r",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    # --out не задан → артефакты рядом с БД прогона.
    assert Path(payload["out"]) == runs_dir / "run.r" / "outputs"


def test_report_partial_manifest_is_flagged(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Прогон без финальной фазы: ``read_manifest`` отдаёт заглушки → ``manifest_partial``."""
    db = tmp_path / "cohound.db"
    with SqliteStore(db) as store:
        store.create_run("run.p", _cfg())
    out = tmp_path / "runs"

    code = main(
        ["report", "--db", str(db), "--run-id", "run.p", "--out", str(out), "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["manifest_partial"] is True
    assert any("манифест не заполнен" in note for note in payload["notes"])


def test_report_human_prints_paths(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "cohound.db"
    _seed_run(db, "run.r", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    out = tmp_path / "runs"

    code = main(
        ["report", "--db", str(db), "--run-id", "run.r", "--out", str(out)]
    )
    assert code == EXIT_OK
    text = capsys.readouterr().out
    assert "manifest:" in text
    assert "nda_questions:" in text




def test_report_lists_urls_blocked_by_robots(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """``hound report`` обязан ПОКАЗАТЬ недоступное, а не написать «недоступного нет».

    Регресс на живой дефект: команда собирала артефакты уже после выхода из
    ``with build_store(...)``, поэтому ``write_reports`` получал **закрытый** store;
    ``read_run_items`` глушил ``ProgrammingError``, и ``artifacts_gap.yaml`` писался с
    ``total: 0`` рядом с 17 отказами ``robots_disallow`` в БД. Отчёт «всё скачалось» на
    прогоне, где часть документов запрещена robots, — худший вид ошибки: он снимает
    вопрос вместо того, чтобы его задать.
    """
    from core.domain.enums import CrawlItemStatus, FailureKind
    from core.domain.models import CrawlSessionItem
    from core.pipeline.crawl_session import default_session_id, open_session

    db = tmp_path / "cohound.db"
    _seed_run(db, "run.r", cells=[(PRODUCT_A, PARAM_A, Status.OK)])
    blocked = "https://p1.example/files/blocked.pdf"
    with SqliteStore(db) as store:
        open_session(store, session_id=default_session_id("run.r"), run_id="run.r")
        store.upsert_crawl_session_item(
            CrawlSessionItem(
                session_id=default_session_id("run.r"),
                product_id=PRODUCT_A,
                url=blocked,
                status=CrawlItemStatus.FAILED,
                attempts=1,
                error_kind=FailureKind.ROBOTS_DISALLOW.value,
                last_attempt="2026-06-01T00:00:00+00:00",
            )
        )

    package = valid_package(tmp_path, scope_attestation=False)
    out = tmp_path / "runs"
    code = main(
        [
            *_report_argv(db, out),
            "--run-yaml",
            str(package / "run.yaml"),
            "--package",
            str(package),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    gap_path = Path(payload["artifacts"]["artifacts_gap"])
    assert gap_path.exists()

    import yaml

    document = yaml.safe_load(gap_path.read_text(encoding="utf-8"))
    assert document["summary"]["total"] == 1
    assert document["summary"]["by_error_kind"] == {"robots_disallow": 1}
    assert [gap["url"] for gap in document["gaps"]] == [blocked]
    # Заготовка входа заполняется человеком, машина её не выдумывает.
    assert document["gaps"][0]["provide"]["path"] is None
    assert document["gaps"][0]["provide"]["provided_by"] is None
