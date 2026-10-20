"""Тесты CLI ``hound package index-gc`` (09 §8.6; офлайн, ``tmp_path``).

Проверяются: dry-run по умолчанию (``applied: false``, файлы на месте), ``--apply`` без
``--actor`` — usage-ошибка, ``--apply --actor`` удаляет неиспользуемые композиции и
сообщает результат. Живое множество — ``runs.index_build_id`` run DB пакета.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from tests.helpers.packages import valid_package


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _write_composition(index_root: Path, package: str, composition_id: str) -> Path:
    """Пишет минимальный манифест композиции ``composition/1``."""
    directory = index_root / package / "_compositions"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{composition_id}.json"
    path.write_text(
        json.dumps(
            {
                "schema": "composition/1",
                "composition_id": composition_id,
                "package": package,
                "created_at": "2026-10-20T00:00:00Z",
                "members": [
                    {
                        "product_id": "p1",
                        "partition_build_id": "pb_1",
                        "content_digest": "d1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def _make_run_db(runs_dir: Path, run_id: str, *, package: str, index_build_id: str) -> None:
    """Создаёт ``runs/{run_id}/cohound.db`` с одной строкой ``runs``."""
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(run_dir / "cohound.db")
    try:
        conn.execute(
            "CREATE TABLE runs ("
            " run_id TEXT PRIMARY KEY, package_id TEXT, index_build_id TEXT)"
        )
        conn.execute(
            "INSERT INTO runs (run_id, package_id, index_build_id) VALUES (?, ?, ?)",
            (run_id, package, index_build_id),
        )
        conn.commit()
    finally:
        conn.close()


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    """Пакет + ``index_store/case_x/_compositions`` + run DB; cwd = ``tmp_path``."""
    package = valid_package(tmp_path)
    monkeypatch.chdir(tmp_path)  # resolved paths по умолчанию относительны cwd
    index_root = tmp_path / "index_store"
    runs_dir = tmp_path / "runs"
    _write_composition(index_root, "case_x", "cb_live")
    _write_composition(index_root, "case_x", "cb_old")
    _make_run_db(runs_dir, "run-1", package="case_x", index_build_id="cb_live")
    return package, index_root, runs_dir


def test_cli_index_gc_dry_run_by_default(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "index-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--json",
        ],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["package"] == "case_x"
    assert payload["applied"] is False
    assert payload["pruned"] == ["cb_old"]
    assert payload["kept"] == ["cb_live"]
    assert payload["live"] == ["cb_live"]
    assert payload["partitions_pruned"] == []
    # Dry-run ничего не удалил.
    assert (index_root / "case_x" / "_compositions" / "cb_old.json").is_file()


def test_cli_index_gc_apply_requires_actor(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, _, err = _run(
        capsys,
        [
            "package",
            "index-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--apply",
        ],
    )

    assert code == EXIT_USAGE
    assert "actor" in err
    assert (index_root / "case_x" / "_compositions" / "cb_old.json").is_file()


def test_cli_index_gc_apply_deletes_and_reports(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "index-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--apply",
            "--actor",
            "tester",
            "--json",
        ],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["applied"] is True
    assert payload["pruned"] == ["cb_old"]
    assert not (index_root / "case_x" / "_compositions" / "cb_old.json").exists()
    assert (index_root / "case_x" / "_compositions" / "cb_live.json").is_file()


def test_cli_index_gc_human_output(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, _index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "index-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
        ],
    )

    assert code == EXIT_OK
    assert "pruned: cb_old" in out
    assert "summary: pruned=1 kept=1 live=1" in out
    assert "applied: no" in out


def test_cli_index_gc_sequence_after_old_run_dir_deleted(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """09 §8.6: два прогона → удалить старый run dir → ``--apply`` прунит ровно его композицию.

    Run DB — единственная заявка на композицию: после ретенции старого каталога ``runs/``
    ``index-gc --apply`` удаляет ``cb_old``, оставляет ``cb_new``, а партиции обеих
    остаются на диске (их собирает отдельный ``gc_index_builds``).
    """
    package = valid_package(tmp_path)
    monkeypatch.chdir(tmp_path)
    index_root = tmp_path / "index_store"
    runs_dir = tmp_path / "runs"
    for composition_id, partition in (("cb_new", "pb_new"), ("cb_old", "pb_old")):
        _write_composition(index_root, "case_x", composition_id)
        (index_root / "case_x" / partition / "p1").mkdir(parents=True)
    _make_run_db(runs_dir, "run-old", package="case_x", index_build_id="cb_old")
    _make_run_db(runs_dir, "run-new", package="case_x", index_build_id="cb_new")

    # (1) ретенция прогонов — вне области GC: каталог старого прогона удаляется целиком.
    shutil.rmtree(runs_dir / "run-old")

    # (2) прунинг композиций.
    code, out, _ = _run(
        capsys,
        [
            "package",
            "index-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--apply",
            "--actor",
            "tester",
            "--json",
        ],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["applied"] is True
    assert payload["pruned"] == ["cb_old"]
    assert payload["kept"] == ["cb_new"]
    assert not (index_root / "case_x" / "_compositions" / "cb_old.json").exists()
    assert (index_root / "case_x" / "_compositions" / "cb_new.json").is_file()

    # (3) партиции не тронуты — «новейшая» композиция остаётся читаемой.
    assert (index_root / "case_x" / "pb_new" / "p1").is_dir()
    assert (index_root / "case_x" / "pb_old" / "p1").is_dir()


def test_cli_index_gc_refusal_is_state_error(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Недоказуемые ссылки (нет run DB) — отказ ``state``, а не молчаливый прунинг."""
    package = valid_package(tmp_path)
    monkeypatch.chdir(tmp_path)
    _write_composition(tmp_path / "index_store", "case_x", "cb_old")
    (tmp_path / "runs").mkdir()

    code, _, err = _run(
        capsys,
        [
            "package",
            "index-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(tmp_path / "runs"),
        ],
    )

    assert code == EXIT_USAGE
    assert "state" in err
    assert (tmp_path / "index_store" / "case_x" / "_compositions" / "cb_old.json").is_file()
