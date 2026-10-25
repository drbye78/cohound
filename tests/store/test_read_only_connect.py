"""Read-only открытие БД прогона (``core.store.sqlite.connect_read_only``).

Read-only команды (``review``/``sensitivity``/``golden``/``report``/``diff``/``runs``/
``status``) не имеют права оставлять на диске артефакт: раньше ``build_store`` создавал
каталог и гонял полный ``DDL``, и ``hound review --db /tmp/ghost/x.db`` производил
``x.db`` на 233 472 байта (``12-cli-architecture-review.md`` §4.7). Тогда опечатка в пути
неотличима от «прогон есть, он пустой».

Проверяется:

* отсутствующий файл → :class:`StoreNotInitialized`, ни файла, ни каталога;
* **живой WAL**: читатель видит закоммиченные данные писателя (оба соединения в одном
  процессе), при этом БД не мигрируется;
* запись через read-only соединение отвергается SQLite;
* гейт версии схемы действует и в read-only режиме.

Данные категорийно нейтральны (§1.3); сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from pathlib import Path

import pytest

from core.cli.wiring import build_store
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.store.sqlite import SchemaVersionMismatch, StoreNotInitialized
from core.store.sqlite_store import SqliteStore


def _cfg(case_id: str = "case_x") -> RunConfig:
    """Минимальный конфиг прогона (категорийно нейтральный)."""
    return RunConfig(
        case_id=case_id,
        anchor="p1",
        cohort="cohort_case_x",
        products=["p1"],
        package="case_x",
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def test_missing_db_is_not_created(tmp_path: Path) -> None:
    """Отсутствующий файл — отказ, а не пустая схема; каталог тоже не создаётся."""
    db = tmp_path / "ghost" / "x.db"

    with pytest.raises(StoreNotInitialized) as excinfo:
        build_store(db, read_only=True)

    assert not db.exists()
    assert not db.parent.exists()
    assert "не найдена" in str(excinfo.value)
    assert excinfo.value.code == "not_found"


def test_read_only_sees_live_writer_wal(tmp_path: Path) -> None:
    """Читатель видит закоммиченные данные живого писателя (WAL), не мигрируя БД."""
    db = tmp_path / "cohound.db"
    writer = SqliteStore(db)
    try:
        writer.create_run("run.1", _cfg())
        writer.conn.commit()
        # Писатель держит WAL: рядом с БД появились -wal/-shm (режим журнала из DDL).
        assert (tmp_path / "cohound.db-wal").exists()

        reader = build_store(db, read_only=True)
        try:
            assert reader.read_manifest("run.1").run_id == "run.1"
            # Второй прогон, записанный писателем уже после открытия читателя, тоже виден.
            writer.create_run("run.2", _cfg(case_id="case_y"))
            writer.conn.commit()
            assert reader.read_manifest("run.2").case_id == "case_y"
        finally:
            reader.close()
    finally:
        writer.close()


def test_read_only_rejects_writes(tmp_path: Path) -> None:
    """Запись через read-only соединение — ошибка SQLite, а не тихий DDL."""
    db = tmp_path / "cohound.db"
    with SqliteStore(db) as store:
        store.create_run("run.1", _cfg())
        store.conn.commit()

    reader = build_store(db, read_only=True)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader.conn.execute(
                "INSERT INTO products(product_id, canonical_name, vendor)"
                " VALUES ('p9', 'P9', 'V')"
            )
    finally:
        reader.close()


def test_read_only_keeps_schema_gate(tmp_path: Path) -> None:
    """Чужой ``schema_version`` отвергается и при read-only открытии (``11`` §2.6)."""
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.executescript(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        )
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', '3.9.0')"
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(SchemaVersionMismatch):
        build_store(db, read_only=True)


def test_read_only_does_not_change_schema_version(tmp_path: Path) -> None:
    """Читатель не перезаписывает ``schema_meta`` (это делает только писатель)."""
    db = tmp_path / "cohound.db"
    with SqliteStore(db) as store:
        store.create_run("run.1", _cfg())
        store.conn.commit()

    before = db.stat().st_size
    with build_store(db, read_only=True) as store:
        assert store.read_manifest("run.1").run_id == "run.1"
    assert db.stat().st_size == before


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
