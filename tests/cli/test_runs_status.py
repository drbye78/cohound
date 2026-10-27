"""Тесты команд ``hound runs`` и ``hound status`` (этап 0 рефакторинга CLI).

Обоснование — ``12-cli-architecture-review.md`` §2.1/§3: ``run_id`` требовали 16 команд,
а обнаружить его можно было только ``ls runs/``; на вопрос «где прогон и что дальше» не
отвечала ни одна команда.

Проверяется:

* ``runs`` перечисляет прогоны (``run_id``/пакет/фаза/состояние) и **пропускает** БД чужой
  ``schema_version`` с видимой пометкой, не падая (в ``runs/`` такие лежат);
* отсутствующий каталог прогонов → пустой список и ``EXIT_OK``;
* ``runs``/``status`` ничего не создают и не меняют (read-only);
* ``status`` на отсутствующем прогоне → ``EXIT_NOT_FOUND`` (4);
* ``status`` печатает готовую следующую команду: ``gate`` на гейте, ``run`` на
  незавершённом прогоне, ``report`` на дошедшем до ``report``.

Данные категорийно нейтральны (§1.3): ``p1``/``case_x``/``mod_a``; сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from core.cli.main import EXIT_NOT_FOUND, EXIT_OK, main
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.1"
RUN_ID_B = "run.2"


def _cfg(package: str = "case_x") -> RunConfig:
    """Минимальный конфиг прогона (категорийно нейтральный)."""
    return RunConfig(
        case_id=package,
        anchor="p1",
        cohort="cohort_case_x",
        products=["p1"],
        package=package,
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _seed_run(
    runs_dir: Path,
    run_id: str = RUN_ID,
    *,
    package: str = "case_x",
    phases: dict[str, str] | None = None,
) -> Path:
    """Создаёт ``runs/{run_id}/cohound.db`` со строкой прогона и картой фаз."""
    db = runs_dir / run_id / "cohound.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db) as store:
        store.create_run(run_id, _cfg(package))
        for phase, state in (phases or {}).items():
            store.set_phase_state(run_id, phase, state)
    return db


def _legacy_db(runs_dir: Path, run_id: str, version: str) -> Path:
    """БД «прошлой версии»: только ``schema_meta`` с чужим номером схемы."""
    import sqlite3

    db = runs_dir / run_id / "cohound.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db))
    try:
        conn.executescript(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        )
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)", (version,)
        )
        conn.commit()
    finally:
        conn.close()
    return db


# ---------------------------------------------------------------------------
# hound runs
# ---------------------------------------------------------------------------


def test_runs_missing_dir_is_empty_ok(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет каталога прогонов — пустой список и ``EXIT_OK`` (это не ошибка)."""
    runs_dir = tmp_path / "no-runs"

    assert main(["runs", "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["count"] == 0
    assert payload["runs"] == []
    assert not runs_dir.exists()


def test_runs_lists_runs_with_phase_and_state(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Список несёт ``run_id``, пакет, дату, последнюю фазу и состояние."""
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir, RUN_ID, package="case_x", phases={"normalize": "ok", "plan": "ok"})
    _seed_run(
        runs_dir,
        RUN_ID_B,
        package="case_y",
        phases={"normalize": "ok", "g1": "blocked"},
    )

    assert main(["runs", "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["count"] == 2
    by_id = {run["run_id"]: run for run in payload["runs"]}
    assert by_id[RUN_ID]["package_id"] == "case_x"
    assert by_id[RUN_ID]["last_phase"] == "plan"
    assert by_id[RUN_ID]["state"] == "running"
    assert by_id[RUN_ID]["created_at"]
    assert by_id[RUN_ID_B]["state"] == "blocked"
    assert by_id[RUN_ID_B]["gate"] == "G1"
    assert by_id[RUN_ID_B]["blocked_at"] == "g1"


def test_runs_skips_foreign_schema_with_visible_mark(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """БД чужой схемы не роняет список и не исчезает: она помечена ``skipped``."""
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir, RUN_ID, phases={"normalize": "ok"})
    _legacy_db(runs_dir, "legacy.1", "3.10.0")

    assert main(["runs", "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    by_id = {run["run_id"]: run for run in payload["runs"]}
    assert by_id["legacy.1"]["skipped"]
    assert "schema_version" in by_id["legacy.1"]["skipped"]
    assert "3.10.0" in by_id["legacy.1"]["skipped"]
    # Живой прогон от пропуска соседа не пострадал.
    assert by_id[RUN_ID]["skipped"] is None
    assert by_id[RUN_ID]["state"] == "running"


def test_runs_human_output_marks_skipped(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Человекочитаемый вывод называет пропущенную БД, а не теряет её."""
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir, RUN_ID, phases={"normalize": "ok"})
    _legacy_db(runs_dir, "legacy.1", "3.9.0")

    assert main(["runs", "--runs-dir", str(runs_dir)]) == EXIT_OK
    out = capsys.readouterr().out
    assert RUN_ID in out
    assert "skipped: legacy.1" in out
    assert "3.9.0" in out


def test_runs_does_not_touch_the_database(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``runs`` — read-only: файл БД и его версия схемы не меняются."""
    runs_dir = tmp_path / "runs"
    db = _seed_run(runs_dir, RUN_ID, phases={"normalize": "ok"})
    before = db.read_bytes()

    assert main(["runs", "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    capsys.readouterr()
    assert db.read_bytes() == before


# ---------------------------------------------------------------------------
# hound status
# ---------------------------------------------------------------------------


def test_status_missing_run_is_not_found(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Несуществующий прогон — ``EXIT_NOT_FOUND`` (4), а не «неправильно вызвал» (2)."""
    runs_dir = tmp_path / "runs"

    assert main(["status", "nope", "--runs-dir", str(runs_dir)]) == EXIT_NOT_FOUND
    captured = capsys.readouterr()
    assert "not_found" in captured.err
    assert "nope" in captured.err
    # Read-only: команда не создала ни каталога, ни файла.
    assert not runs_dir.exists()


def test_status_json_on_missing_run_is_not_found(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--json`` на отсутствующем прогоне: код 4 и машинная ошибка, не пустой успех."""
    code = main(["status", "nope", "--runs-dir", str(tmp_path / "runs"), "--json"])
    assert code == EXIT_NOT_FOUND
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is False
    assert payload["error"] == "not_found"


def test_status_blocked_on_gate_prints_gate_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Стоит на G1 — следующая команда ``hound gate … --gate G1`` и запрос назван."""
    runs_dir = tmp_path / "runs"
    db = _seed_run(
        runs_dir, RUN_ID, phases={"normalize": "ok", "lint": "ok", "plan": "ok", "g1": "blocked"}
    )
    (db.parent / "gates").mkdir()
    (db.parent / "gates" / "G1-request.json").write_text(
        json.dumps({"run_id": RUN_ID, "gate": "G1", "subject_digest": "d1"}), encoding="utf-8"
    )

    assert main(["status", RUN_ID, "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["state"] == "blocked"
    assert payload["blocked_at"] == "g1"
    assert payload["gate"] == "G1"
    command = payload["next"]["command"]
    assert command.startswith("hound gate ")
    assert "--gate G1" in command
    assert f"--run-id {RUN_ID}" in command
    assert f"--runs-dir {runs_dir}" in command
    assert "--actor" in command
    assert payload["gate_decision"] is None  # решения ещё нет — это не «отклонён»


def test_status_blocked_reports_rejection_by_whom(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Отказ гейта виден в ``status``: кто и почему (а не только «blocked»)."""
    runs_dir = tmp_path / "runs"
    db = _seed_run(runs_dir, RUN_ID, phases={"normalize": "ok", "g1": "blocked"})
    assert (
        main(
            [
                "gate",
                "--db",
                str(db),
                "--run-id",
                RUN_ID,
                "--gate",
                "G1",
                "--actor",
                "alice",
                "--subject-digest",
                "d1",
                "--verdict",
                "rejected",
                "--notes",
                "профиль не тот",
            ]
        )
        == 3  # EXIT_BLOCKED: запись состоялась, прогон заблокирован отказом
    )
    capsys.readouterr()

    assert main(["status", RUN_ID, "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["gate_decision"] == {
        "verdict": "rejected",
        "actor": "alice",
        "notes": "профиль не тот",
    }
    assert any("ОТКЛОНЁН" in note for note in payload["notes"])


def test_status_running_prints_resume_command(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Незавершённый прогон — ``hound run`` с восстановленными ``run.yaml``/пакетом/URL."""
    monkeypatch.chdir(tmp_path)
    runs_dir = tmp_path / "runs"
    db = _seed_run(runs_dir, RUN_ID, phases={"normalize": "ok", "inheritance": "ok"})
    (db.parent / "run.yaml").write_text("case_id: case_x\n", encoding="utf-8")
    package_dir = tmp_path / "packages" / "case_x"
    (package_dir / "sources").mkdir(parents=True)
    (package_dir / "sources" / "corpus_urls.yaml").write_text("p1: []\n", encoding="utf-8")

    assert main(["status", RUN_ID, "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["state"] == "running"
    assert payload["last_phase"] == "inheritance"
    command = payload["next"]["command"]
    assert command.startswith("hound run ")
    assert "--run-yaml" in command and "run.yaml" in command
    assert "--package" in command
    assert "packages/case_x" in command
    assert "packages/case_x/sources/corpus_urls.yaml" in command
    assert payload["notes"] == []


def test_status_running_marks_unrecoverable_paths(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Чего восстановить нельзя — заглушка ``<…>`` и явная заметка, а не выдуманный путь."""
    monkeypatch.chdir(tmp_path)
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir, RUN_ID, phases={"normalize": "ok"})

    assert main(["status", RUN_ID, "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    command = payload["next"]["command"]
    assert "<run.yaml>" in command
    assert "<каталог пакета case_x>" in command
    assert payload["notes"]


def test_status_done_prints_report_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Прогон дошёл до ``report`` — отдельное состояние, дальше только артефакты."""
    runs_dir = tmp_path / "runs"
    _seed_run(
        runs_dir,
        RUN_ID,
        phases={"normalize": "ok", "evaluate": "ok", "g4": "ok", "report": "ok"},
    )

    assert main(["status", RUN_ID, "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["state"] == "done"
    assert payload["next"]["command"] == (
        f"hound report --runs-dir {runs_dir} --run-id {RUN_ID}"
    )


def test_status_counts_cells_and_corpus(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Счётчики берутся из БД: ячейки по состояниям, документы/чанки корпуса."""
    runs_dir = tmp_path / "runs"
    db = _seed_run(runs_dir, RUN_ID, phases={"evaluate": "ok"})
    with SqliteStore(db) as store:
        store.conn.execute(
            "INSERT INTO corpus_docs(doc_id, product_id, url) VALUES ('d1', 'p1', 'https://x')"
        )
        store.conn.execute(
            "INSERT INTO corpus_chunks(chunk_id, doc_id, product_id, text)"
            " VALUES ('c1', 'd1', 'p1', 'text')"
        )
        for param in ("a", "b"):
            store.conn.execute(
                "INSERT INTO cells(run_id, product_id, param_id, state)"
                " VALUES (?, 'p1', ?, 'evaluated')",
                (RUN_ID, param),
            )
        store.conn.commit()

    assert main(["status", RUN_ID, "--runs-dir", str(runs_dir), "--json"]) == EXIT_OK
    payload = json_out(capsys)
    assert payload["counts"]["cells"] == {"evaluated": 2}
    assert payload["counts"]["corpus_docs"] == 1
    assert payload["counts"]["corpus_chunks"] == 1


def test_status_human_output_shows_next_and_why(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Человекочитаемый вывод отвечает «где прогон» и «что дальше»."""
    runs_dir = tmp_path / "runs"
    _seed_run(runs_dir, RUN_ID, phases={"normalize": "ok", "g2": "blocked"})

    assert main(["status", RUN_ID, "--runs-dir", str(runs_dir)]) == EXIT_OK
    out = capsys.readouterr().out
    assert f"run_id: {RUN_ID}" in out
    assert "state: blocked" in out
    assert "blocked_at: g2 (гейт G2)" in out
    assert "next: hound gate " in out
    # Файла запроса в этом прогоне нет — команда сразу называет --subject-digest,
    # а не отправляет оператора к несуществующему файлу.
    assert "--subject-digest" in out
    assert "why: " in out


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
