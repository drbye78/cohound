"""Тесты backup/restore прогона и GC индексных сборок (W12/T12.2).

Проверяются: консистентный снапшот ``cohound.db`` через ``sqlite3.Connection.backup``,
детерминированность и состав бандла, защита от path traversal при восстановлении,
детерминированный порядок ``list_builds`` и безопасность политики GC (§13).
Нейтральны (§1.3), используют ``tmp_path``.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sqlite3
import zipfile
from pathlib import Path

import pytest

from core.domain import (
    Cell,
    CellState,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.store.backup import backup_run, gc_index_builds, list_builds, restore_run
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-1"


def _cfg() -> RunConfig:
    """Минимальный конфиг прогона для фикстуры."""
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=["param.a"]),
        languages=["ru", "en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _cell(param_id: str) -> Cell:
    """Ячейка прогона в состоянии pending."""
    return Cell(run_id=RUN_ID, product_id="p1", param_id=param_id, state=CellState.PENDING)


def _build_run_dir(root: Path) -> Path:
    """Создаёт пакет прогона ``runs/{run_id}/`` с БД и файлами (§A.2)."""
    run_dir = root / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    with SqliteStore(run_dir / "cohound.db") as store:
        store.create_run(RUN_ID, _cfg())
        store.save_cell(_cell("param.a"))
    (run_dir / "run.yaml").write_text("case_id: case-1\n", encoding="utf-8")
    (run_dir / "run_manifest.yaml").write_text("run_id: run-1\n", encoding="utf-8")
    (run_dir / "logs").mkdir()
    (run_dir / "logs" / "W6.jsonl").write_text('{"phase": "W6"}\n', encoding="utf-8")
    (run_dir / "prompts").mkdir()
    (run_dir / "prompts" / "abc123.json").write_text('{"prompt": "x"}', encoding="utf-8")
    (run_dir / "outputs").mkdir()
    (run_dir / "outputs" / "diff.md").write_text("# diff\n", encoding="utf-8")
    return run_dir


def _make_build(index_root: Path, package: str, build_id: str, partitions: int, mtime: float) -> Path:
    """Создаёт сборку ``index_store/{package}/{build_id}/{product}`` с заданным mtime."""
    build_dir = index_root / package / build_id
    build_dir.mkdir(parents=True, exist_ok=True)
    for i in range(partitions):
        (build_dir / f"p{i}").mkdir(parents=True, exist_ok=True)
    os.utime(build_dir, (mtime, mtime))
    return build_dir


# --------------------------------------------------------------------------- #
# backup_run: состав и детерминированность
# --------------------------------------------------------------------------- #


def test_backup_run_expected_members(tmp_path: Path) -> None:
    run_dir = _build_run_dir(tmp_path)
    # Секреты и sidecar-файлы не должны попасть в бандл.
    (run_dir / ".env").write_text("API_KEY=secret\n", encoding="utf-8")
    (run_dir / "secrets.yaml").write_text("token: secret\n", encoding="utf-8")
    (run_dir / "server.pem").write_text("-----BEGIN KEY-----\n", encoding="utf-8")
    (run_dir / "cohound.db-wal").write_bytes(b"wal")

    archive = backup_run(run_dir)

    assert archive == tmp_path / "runs" / f"{RUN_ID}.zip"
    assert archive.is_file()
    with zipfile.ZipFile(archive) as zf:
        names = zf.namelist()
    for expected in (
        "cohound.db",
        "run.yaml",
        "run_manifest.yaml",
        "logs/W6.jsonl",
        "prompts/abc123.json",
        "outputs/diff.md",
    ):
        assert expected in names
    # Детерминированный порядок членов.
    assert names == sorted(names)
    # Секреты/sidecar исключены.
    assert ".env" not in names
    assert "secrets.yaml" not in names
    assert "server.pem" not in names
    assert "cohound.db-wal" not in names


def test_backup_run_deterministic(tmp_path: Path) -> None:
    run_dir = _build_run_dir(tmp_path)
    first = backup_run(run_dir, tmp_path / "first.zip")
    second = backup_run(run_dir, tmp_path / "second.zip")

    with zipfile.ZipFile(first) as a, zipfile.ZipFile(second) as b:
        assert a.namelist() == b.namelist()
        for name in a.namelist():
            assert a.getinfo(name).CRC == b.getinfo(name).CRC


# --------------------------------------------------------------------------- #
# снапшот БД консистентен и восстановим
# --------------------------------------------------------------------------- #


def test_archived_db_is_queryable(tmp_path: Path) -> None:
    run_dir = _build_run_dir(tmp_path)
    archive = backup_run(run_dir, tmp_path / "bundle.zip")

    with zipfile.ZipFile(archive) as zf:
        db_bytes = zf.read("cohound.db")
    extracted = tmp_path / "extracted.db"
    extracted.write_bytes(db_bytes)

    conn = sqlite3.connect(extracted)
    try:
        assert conn.execute("SELECT run_id FROM runs").fetchone()[0] == RUN_ID
        assert conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0] == 1
    finally:
        conn.close()


def test_restore_run_round_trip(tmp_path: Path) -> None:
    run_dir = _build_run_dir(tmp_path)
    archive = backup_run(run_dir, tmp_path / "bundle.zip")

    restored = restore_run(archive, tmp_path / "restored")

    assert restored == tmp_path / "restored"
    assert (restored / "run.yaml").is_file()
    assert (restored / "run_manifest.yaml").is_file()
    assert (restored / "logs" / "W6.jsonl").is_file()
    assert (restored / "outputs" / "diff.md").read_text(encoding="utf-8") == "# diff\n"
    conn = sqlite3.connect(restored / "cohound.db")
    try:
        assert conn.execute("SELECT COUNT(*) FROM cells").fetchone()[0] == 1
    finally:
        conn.close()


def test_restore_rejects_path_traversal(tmp_path: Path) -> None:
    for name in ("../evil.txt", "/abs-eviltxt", "a/../../evil.txt", "C:/evil.txt"):
        bad = tmp_path / "bad.zip"
        with zipfile.ZipFile(bad, "w") as zf:
            zf.writestr(name, "x")
        with pytest.raises(ValueError):
            restore_run(bad, tmp_path / "out")
    assert not (tmp_path / "evil.txt").exists()


# --------------------------------------------------------------------------- #
# list_builds
# --------------------------------------------------------------------------- #


def test_list_builds_deterministic_ordering(tmp_path: Path) -> None:
    _make_build(tmp_path, "pkg", "b2", partitions=2, mtime=200.0)
    _make_build(tmp_path, "pkg", "b1", partitions=1, mtime=100.0)
    _make_build(tmp_path, "pkg", "b3", partitions=0, mtime=300.0)

    builds = list_builds(tmp_path, "pkg")

    assert [b[0] for b in builds] == ["b1", "b2", "b3"]
    assert [b[1] for b in builds] == [1, 2, 0]
    assert builds[0][2] == pytest.approx(100.0)
    assert builds[2][2] == pytest.approx(300.0)
    # Детерминированность: повторный вызов даёт тот же результат.
    assert list_builds(tmp_path, "pkg") == builds
    # Отсутствующий пакет — пустой список.
    assert list_builds(tmp_path, "missing") == []


# --------------------------------------------------------------------------- #
# gc_index_builds
# --------------------------------------------------------------------------- #


def _seed_six_builds(tmp_path: Path) -> None:
    for i in range(1, 7):
        _make_build(tmp_path, "pkg", f"b{i}", partitions=1, mtime=float(i))


def _write_composition(
    index_root: Path, package: str, composition_id: str, partitions: list[str]
) -> Path:
    """Пишет манифест композиции, ссылающийся на заданные ``partition_build_id``."""
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
                        "product_id": f"p{i}",
                        "partition_build_id": partition,
                        "content_digest": f"d{i}",
                    }
                    for i, partition in enumerate(partitions)
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_gc_dry_run_default_does_not_delete(tmp_path: Path) -> None:
    _seed_six_builds(tmp_path)

    # Reference-aware: композиций нет ⇒ ни одна партиция не защищена ссылкой.
    would_delete = gc_index_builds(tmp_path, "pkg", keep=2)

    assert would_delete == ["b1", "b2", "b3", "b4", "b5", "b6"]
    for build_id in ("b1", "b2", "b3", "b4", "b5", "b6"):
        assert (tmp_path / "pkg" / build_id).is_dir()


def test_gc_protect_respected(tmp_path: Path) -> None:
    _seed_six_builds(tmp_path)

    result = gc_index_builds(tmp_path, "pkg", keep=2, protect={"b1"})

    assert result == ["b2", "b3", "b4", "b5", "b6"]
    assert (tmp_path / "pkg" / "b1").is_dir()


def test_compositions_dir_is_not_a_build_and_survives_gc(tmp_path: Path) -> None:
    """09 §8.2/§8.6: ``_compositions`` не сборка — не листается и не удаляется GC."""
    _seed_six_builds(tmp_path)
    manifest = _write_composition(tmp_path, "pkg", "cb_x", ["b6"])

    assert "_compositions" not in [b[0] for b in list_builds(tmp_path, "pkg")]
    deleted = gc_index_builds(tmp_path, "pkg", keep=1, dry_run=False)

    assert "_compositions" not in deleted
    assert manifest.is_file()
    # Ссылка композиции удерживает b6; остальные не упомянуты — удалены.
    assert [b[0] for b in list_builds(tmp_path, "pkg")] == ["b6"]


def test_gc_referenced_live_partition_survives_old_keep_count(tmp_path: Path) -> None:
    """09 §8.6: живая партиция композиции не удаляется счётчиком ``keep``.

    ``b1`` — самая старая сборка, которую старая keep-политика удалила бы при ``keep=1``.
    Но на неё ссылается композиция ⇒ она обязана выжить.
    """
    _seed_six_builds(tmp_path)
    _write_composition(tmp_path, "pkg", "cb_live", ["b1"])

    deleted = gc_index_builds(tmp_path, "pkg", keep=1, dry_run=False)

    assert "b1" not in deleted
    assert (tmp_path / "pkg" / "b1").is_dir()
    assert [b[0] for b in list_builds(tmp_path, "pkg")] == ["b1"]


def test_gc_unreadable_composition_refuses_to_delete(tmp_path: Path) -> None:
    """Битый манифест ⇒ ссылки неизвестны ⇒ GC отказывается (не удаляет живое)."""
    _seed_six_builds(tmp_path)
    directory = tmp_path / "pkg" / "_compositions"
    directory.mkdir()
    (directory / "cb_bad.json").write_text("{not json", encoding="utf-8")

    with pytest.raises(ValueError):
        gc_index_builds(tmp_path, "pkg", dry_run=False)
    for build_id in ("b1", "b2", "b3", "b4", "b5", "b6"):
        assert (tmp_path / "pkg" / build_id).is_dir()


def test_gc_keep_lt_1_refused(tmp_path: Path) -> None:
    _seed_six_builds(tmp_path)
    with pytest.raises(ValueError):
        gc_index_builds(tmp_path, "pkg", keep=0)
    with pytest.raises(ValueError):
        gc_index_builds(tmp_path, "pkg", keep=-1, dry_run=False)


def test_gc_real_deletes_only_unreferenced(tmp_path: Path) -> None:
    _seed_six_builds(tmp_path)
    _write_composition(tmp_path, "pkg", "cb_keep", ["b5"])

    deleted = gc_index_builds(
        tmp_path, "pkg", keep=2, protect={"b1"}, dry_run=False
    )

    # b1 защищён явно, b5 удерживается композицией; остальные — мусор.
    assert deleted == ["b2", "b3", "b4", "b6"]
    for build_id in ("b2", "b3", "b4", "b6"):
        assert not (tmp_path / "pkg" / build_id).exists()
    for build_id in ("b1", "b5"):
        assert (tmp_path / "pkg" / build_id).is_dir()
