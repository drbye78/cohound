"""Тесты CLI ``hound package builds-gc`` (09 §8.6; офлайн, ``tmp_path``).

Проверяются: dry-run по умолчанию, ``--apply`` без ``--actor`` (usage-ошибка),
``--apply --actor`` удаляет ровно unreferenced-сборки, legacy-сборка из
``runs.index_build_id`` без манифеста композиции защищается, отказ ``state`` при
недоказуемых ссылках, явный ``--protect`` и неприкосновенность композиций
(отдельная операция ``index-gc``).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from core.cli.main import EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, main
from tests.helpers.packages import valid_package


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _make_build(index_root: Path, package: str, build_id: str) -> Path:
    """Создаёт верхнеуровневую сборку с одной продукт-партицией."""
    build_dir = index_root / package / build_id
    (build_dir / "p1").mkdir(parents=True)
    return build_dir


def _write_composition(
    index_root: Path, package: str, composition_id: str, partition: str = "pb_1"
) -> Path:
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
                        "partition_build_id": partition,
                        "content_digest": "d1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def _make_run_db(
    runs_dir: Path, run_id: str, *, package: str, index_build_id: str
) -> None:
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


def _setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, Path]:
    """Пакет + сборки ``pb_live``/``ib_garbage`` + композиция + run DB.

    ``pb_live`` удерживается композицией ``cb_live``, на которую ссылается прогон;
    ``ib_garbage`` не упомянут нигде и является кандидатом на удаление.
    """
    package = valid_package(tmp_path)
    monkeypatch.chdir(tmp_path)
    index_root = tmp_path / "index_store"
    runs_dir = tmp_path / "runs"
    _make_build(index_root, "case_x", "pb_live")
    _make_build(index_root, "case_x", "ib_garbage")
    _write_composition(index_root, "case_x", "cb_live", "pb_live")
    _make_run_db(runs_dir, "run-1", package="case_x", index_build_id="cb_live")
    return package, index_root, runs_dir


def test_builds_gc_dry_run_by_default(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "builds-gc",
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
    assert payload["pruned"] == ["ib_garbage"]
    assert payload["protected"] == []
    assert payload["live"] == ["cb_live"]
    # Dry-run ничего не удалил.
    assert (index_root / "case_x" / "ib_garbage").is_dir()
    assert (index_root / "case_x" / "pb_live").is_dir()


def test_builds_gc_apply_requires_actor(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, _, err = _run(
        capsys,
        [
            "package",
            "builds-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--apply",
        ],
    )

    assert code == EXIT_USAGE
    assert "actor" in err
    assert (index_root / "case_x" / "ib_garbage").is_dir()


def test_builds_gc_apply_deletes_only_unreferenced(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "builds-gc",
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
    assert payload["pruned"] == ["ib_garbage"]
    assert not (index_root / "case_x" / "ib_garbage").exists()
    assert (index_root / "case_x" / "pb_live").is_dir()


def test_builds_gc_legacy_build_from_run_db_is_protected(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy-сборка названа ``runs.index_build_id``, но манифеста композиции у неё нет.

    Композиционная защита её не видит, поэтому CLI обязан добавить её в ``protect`` —
    иначе ``gc_index_builds`` удалил бы живой артефакт.
    """
    package = valid_package(tmp_path)
    monkeypatch.chdir(tmp_path)
    index_root = tmp_path / "index_store"
    runs_dir = tmp_path / "runs"
    _make_build(index_root, "case_x", "ib_legacy")
    _make_build(index_root, "case_x", "ib_garbage")
    _make_build(index_root, "case_x", "pb_live")
    _write_composition(index_root, "case_x", "cb_live", "pb_live")
    _make_run_db(runs_dir, "run-legacy", package="case_x", index_build_id="ib_legacy")

    code, out, _ = _run(
        capsys,
        [
            "package",
            "builds-gc",
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
    assert payload["pruned"] == ["ib_garbage"]
    assert payload["protected"] == ["ib_legacy"]
    assert payload["live"] == ["ib_legacy"]
    assert (index_root / "case_x" / "ib_legacy").is_dir()
    assert (index_root / "case_x" / "pb_live").is_dir()
    assert not (index_root / "case_x" / "ib_garbage").exists()


def test_builds_gc_refusal_is_state_error_when_no_run_db(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Недоказуемые ссылки (нет run DB) — отказ ``state``, а не молчаливый прунинг."""
    package = valid_package(tmp_path)
    monkeypatch.chdir(tmp_path)
    index_root = tmp_path / "index_store"
    _make_build(index_root, "case_x", "ib_garbage")
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()

    code, _, err = _run(
        capsys,
        [
            "package",
            "builds-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
        ],
    )

    assert code == EXIT_USAGE
    assert "state" in err
    assert (index_root / "case_x" / "ib_garbage").is_dir()


def test_builds_gc_protect_flag_keeps_build(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "builds-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--protect",
            "ib_garbage",
            "--apply",
            "--actor",
            "tester",
            "--json",
        ],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["pruned"] == []
    assert payload["protected"] == ["ib_garbage"]
    assert (index_root / "case_x" / "ib_garbage").is_dir()


def test_builds_gc_does_not_touch_compositions(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Разделение операций: ``builds-gc`` чистит сборки, композиции — ``index-gc``."""
    package, index_root, runs_dir = _setup(tmp_path, monkeypatch)
    old_manifest = _write_composition(index_root, "case_x", "cb_old", "pb_old")

    code, _, _ = _run(
        capsys,
        [
            "package",
            "builds-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--apply",
            "--actor",
            "tester",
        ],
    )

    assert code == EXIT_OK
    # Обе композиции на диске: даже та, на которую не ссылается ни один прогон.
    assert old_manifest.is_file()
    assert (index_root / "case_x" / "_compositions" / "cb_live.json").is_file()


def test_builds_gc_human_output(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package, _index_root, runs_dir = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "builds-gc",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
        ],
    )

    assert code == EXIT_OK
    assert "package: case_x" in out
    assert f"runs_dir: {runs_dir}" in out
    assert "index_root: index_store" in out
    assert "applied: no" in out
    assert "pruned: ib_garbage" in out
    assert "summary: pruned=1 protected=0" in out
    assert "compositions: untouched" in out


def test_builds_gc_missing_package_is_not_found(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    code, out, _ = _run(
        capsys,
        [
            "package",
            "builds-gc",
            "--package",
            str(tmp_path / "missing"),
            "--runs-dir",
            str(tmp_path / "runs"),
            "--json",
        ],
    )

    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["ok"] is False
