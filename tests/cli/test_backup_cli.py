"""CLI ``hound backup`` / ``hound restore``: бандл прогона и durable-состояния (09 §14.4).

Проверяется не только «команда есть», а три вещи, которые нельзя доказать чтением кода:

* **состояние входит согласованным снимком** — строки, лежащие в ``state.db-wal``
  (живое соединение не сделало checkpoint), попадают в архив и читаются после
  восстановления; копия файла их бы потеряла;
* **отсутствие состояния видимо** — при ``--include-state`` без ``state.db`` команда
  не молчит, а печатает строку (``--json`` и человекочитаемо);
* **опасное — с подтверждением** — восстановление поверх непустого каталога и поверх
  живой ``state.db`` отказывает с именем пути, а не перезаписывает молча.

Данные категорийно нейтральны (§1.3); сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
import zipfile
from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.store.backup import STATE_MEMBER
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.b1"
PACKAGE_ID = "pkg_b"
PROBE_ROWS = 7


def _cfg() -> RunConfig:
    """Минимальный конфиг прогона (категорийно нейтральный)."""
    return RunConfig(
        case_id="case_b",
        anchor="p1",
        cohort="cohort_case_b",
        products=["p1"],
        package=PACKAGE_ID,
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _make_run_dir(runs_dir: Path) -> Path:
    """Каталог прогона ``runs/{run_id}/`` с настоящей БД, логами и выходами."""
    run_dir = runs_dir / RUN_ID
    run_dir.mkdir(parents=True)
    with SqliteStore(run_dir / "cohound.db") as store:
        store.create_run(RUN_ID, _cfg())
    (run_dir / "run.yaml").write_text("case_id: case_b\n", encoding="utf-8")
    (run_dir / "logs").mkdir()
    (run_dir / "logs" / "W6.jsonl").write_text('{"phase": "W6"}\n', encoding="utf-8")
    (run_dir / "outputs").mkdir()
    (run_dir / "outputs" / "battle_card.md").write_text("# card\n", encoding="utf-8")
    return run_dir


def _make_state_db(state_root: Path) -> tuple[Path, sqlite3.Connection]:
    """``state/{package_id}/state.db`` с WAL: часть строк живёт только в ``-wal``.

    Соединение **возвращается** открытым: закрытие делает checkpoint и убирает
    ``-wal``, а предмет проверки — что снапшот видит непрочекпойнченные строки.
    """
    package_dir = state_root / PACKAGE_ID
    package_dir.mkdir(parents=True)
    db_path = package_dir / "state.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE probe (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
    conn.executemany(
        "INSERT INTO probe (k, v) VALUES (?, ?)",
        [(f"k{i}", f"v{i}") for i in range(PROBE_ROWS)],
    )
    conn.commit()
    return db_path, conn


def _count(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        conn.close()


def _package_dir(tmp_path: Path) -> Path:
    """Каталог пакета: имя каталога = ``package_id`` (конвенция ``state/{package_id}``)."""
    package = tmp_path / "packages" / PACKAGE_ID
    package.mkdir(parents=True, exist_ok=True)
    manifest = package / "package.yaml"
    if not manifest.exists():
        manifest.write_text("package_id: pkg_b\n", encoding="utf-8")
    return package


# --------------------------------------------------------------------------- #
# backup
# --------------------------------------------------------------------------- #


def test_backup_without_state_has_no_state_member(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """По умолчанию бандл — только прогон: состояния в архиве нет."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    _state_db, state_conn = _make_state_db(isolated_state_dir)

    code = main(
        [
            "backup",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            RUN_ID,
            "--package",
            str(_package_dir(tmp_path)),
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["ok"] is True
    assert payload["state_included"] is False
    assert payload["state_db"] is None
    # Дефолт ``--dest`` — рядом с прогоном (как у библиотечного ``backup_run``).
    assert payload["archive"] == str(runs_dir / f"{RUN_ID}.zip")
    with zipfile.ZipFile(payload["archive"]) as zf:
        names = zf.namelist()
    assert "cohound.db" in names
    assert "logs/W6.jsonl" in names
    assert STATE_MEMBER not in names
    state_conn.close()


def test_backup_include_state_snapshot_restores_rows(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Состояние входит снапшотом (видит WAL) и восстанавливается в чистый каталог."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    state_db, state_conn = _make_state_db(isolated_state_dir)
    # Доказательство, что тест не пустой: строки ещё в ``-wal``, копии файла мало.
    assert state_db.with_name("state.db-wal").stat().st_size > 0

    code = main(
        [
            "backup",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            RUN_ID,
            "--package",
            str(_package_dir(tmp_path)),
            "--include-state",
            "--dest",
            str(tmp_path / "bundle.zip"),
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["state_included"] is True
    assert payload["state_db"] == str(state_db)
    assert payload["notes"] == []
    state_conn.close()
    archive = tmp_path / "bundle.zip"
    with zipfile.ZipFile(archive) as zf:
        assert STATE_MEMBER in zf.namelist()

    restored_run = tmp_path / "restored" / RUN_ID
    restored_state = tmp_path / "restored_state" / PACKAGE_ID
    code = main(
        [
            "restore",
            "--archive",
            str(archive),
            "--dest-dir",
            str(restored_run),
            "--state-into",
            str(restored_state),
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["restored_dir"] == str(restored_run)
    assert payload["state_restored"] is True
    assert payload["state_db"] == str(restored_state / "state.db")
    # БД прогона открывается и число строк совпадает.
    assert _count(restored_run / "cohound.db", "runs") == 1
    # БД состояния открывается и число строк совпадает — включая те, что были в WAL.
    assert _count(restored_state / "state.db", "probe") == PROBE_ROWS
    # Файлы прогона восстановлены, состояние — не в каталоге прогона.
    assert (restored_run / "outputs" / "battle_card.md").read_text(encoding="utf-8") == "# card\n"
    assert not (restored_run / "state").exists()


def test_backup_include_state_missing_is_visible(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """``--include-state`` без ``state.db`` — не ошибка, но видимая строка, не тишина."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    missing = isolated_state_dir / PACKAGE_ID / "state.db"

    code = main(
        [
            "backup",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            RUN_ID,
            "--package",
            str(_package_dir(tmp_path)),
            "--include-state",
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["state_included"] is False
    assert payload["include_state"] is True
    assert len(payload["notes"]) == 1
    assert "состояние не найдено" in payload["notes"][0]
    assert str(missing) in payload["notes"][0]
    with zipfile.ZipFile(payload["archive"]) as zf:
        assert STATE_MEMBER not in zf.namelist()

    # Человекочитаемый режим говорит то же самое, а не молчит.
    code = main(
        [
            "backup",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            RUN_ID,
            "--package",
            str(_package_dir(tmp_path)),
            "--include-state",
        ]
    )
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert f"state: не найден ({missing}) — архив без state" in out


def test_backup_include_state_requires_package(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Состояние привязано к пакету: без ``--package`` команда отказывает, а не угадывает."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)

    code = main(
        [
            "backup",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            RUN_ID,
            "--include-state",
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert "--include-state требует --package" in payload["message"]


def test_backup_unknown_run_is_usage_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Несуществующий прогон — ошибка входа с именем каталога."""
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()

    code = main(
        ["backup", "--runs-dir", str(runs_dir), "--run-id", "nope", "--json"]
    )
    payload = json_out(capsys)

    assert code == EXIT_USAGE
    assert str(runs_dir / "nope") in payload["message"]


# --------------------------------------------------------------------------- #
# restore
# --------------------------------------------------------------------------- #


def _bundle(tmp_path: Path, runs_dir: Path, *, with_state: bool) -> Path:
    """Готовит архив через CLI (тот же путь, что у оператора)."""
    argv = [
        "backup",
        "--runs-dir",
        str(runs_dir),
        "--run-id",
        RUN_ID,
        "--package",
        str(_package_dir(tmp_path)),
        "--dest",
        str(tmp_path / "bundle.zip"),
    ]
    if with_state:
        argv.append("--include-state")
    assert main(argv) == EXIT_OK
    return tmp_path / "bundle.zip"


def test_restore_refuses_nonempty_dest(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Восстановление не перезаписывает существующий прогон: отказ с именем каталога."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    archive = _bundle(tmp_path, runs_dir, with_state=False)
    capsys.readouterr()

    busy = tmp_path / "busy"
    busy.mkdir()
    (busy / "keep.txt").write_text("важное\n", encoding="utf-8")

    code = main(["restore", "--archive", str(archive), "--dest-dir", str(busy), "--json"])
    payload = json_out(capsys)

    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert f"каталог восстановления не пуст: {busy}" in payload["message"]
    # Ничего не тронуто: ни файл, ни новые члены.
    assert sorted(p.name for p in busy.iterdir()) == ["keep.txt"]


def test_restore_state_refuses_live_db_without_force(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Перезапись живой ``state.db`` — только по явному ``--force``."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    state_db, state_conn = _make_state_db(isolated_state_dir)
    archive = _bundle(tmp_path, runs_dir, with_state=True)
    capsys.readouterr()
    # Живое соединение закрыто: дальше проверяется отказ перезаписи существующей БД,
    # а не согласованность WAL (она проверена отдельным тестом выше).
    state_conn.close()
    live_dir = state_db.parent

    code = main(
        ["restore", "--archive", str(archive), "--state-into", str(live_dir), "--json"]
    )
    payload = json_out(capsys)

    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert str(state_db) in payload["message"]
    assert "--force" in payload["message"]
    assert _count(state_db, "probe") == PROBE_ROWS

    # С --force перезапись разрешена явно и состояние читается.
    code = main(
        [
            "restore",
            "--archive",
            str(archive),
            "--state-into",
            str(live_dir),
            "--force",
            "--json",
        ]
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["state_restored"] is True
    assert _count(state_db, "probe") == PROBE_ROWS


def test_restore_state_from_archive_without_state_refused(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Архив без состояния не выдаётся за архив с состоянием."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    archive = _bundle(tmp_path, runs_dir, with_state=False)
    capsys.readouterr()

    target = tmp_path / "state_target"
    code = main(
        ["restore", "--archive", str(archive), "--state-into", str(target), "--json"]
    )
    payload = json_out(capsys)

    assert code == EXIT_USAGE
    assert "в архиве нет состояния" in payload["message"]
    assert not (target / "state.db").exists()


def test_restore_run_leaves_state_alone_and_says_so(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Состояние в архиве есть, но восстановление прогона его не трогает — и говорит об этом."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    _state_db, state_conn = _make_state_db(isolated_state_dir)
    archive = _bundle(tmp_path, runs_dir, with_state=True)
    capsys.readouterr()
    state_conn.close()

    dest = tmp_path / "restored" / RUN_ID
    code = main(["restore", "--archive", str(archive), "--dest-dir", str(dest), "--json"])
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["archive_has_state"] is True
    assert payload["state_restored"] is False
    assert payload["state_db"] is None
    assert len(payload["notes"]) == 1
    assert "НЕ восстановлено" in payload["notes"][0]
    assert not (dest / "state").exists()


def test_restore_requires_a_destination(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    isolated_state_dir: Path,
) -> None:
    """Без ``--dest-dir``/``--state-into`` восстанавливать некуда — ошибка входа."""
    runs_dir = tmp_path / "runs"
    _make_run_dir(runs_dir)
    archive = _bundle(tmp_path, runs_dir, with_state=False)
    capsys.readouterr()

    code = main(["restore", "--archive", str(archive), "--json"])
    payload = json_out(capsys)

    assert code == EXIT_USAGE
    assert "--dest-dir" in payload["message"]


def test_both_commands_are_registered(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Обе команды отвечают на ``--help``: регистрация в парсере — часть интерфейса."""
    assert main(["backup", "--help"]) == EXIT_OK
    assert "--include-state" in capsys.readouterr().out
    assert main(["restore", "--help"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "--state-into" in out
    assert "--force" in out
