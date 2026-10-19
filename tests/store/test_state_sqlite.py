"""Тесты схемы ``state.db`` (09 §4.1/§4.2): DDL, версия, PK, идемпотентность открытия.

Все тесты офлайновые: ``:memory:`` или временный каталог, без сети.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.store.state_sqlite import (
    STATE_SCHEMA_VERSION,
    connect_state,
    state_db_path,
)

#: Таблицы §4.2 (10 предметных + ``schema_meta``).
EXPECTED_TABLES = {
    "schema_meta",
    "product_state",
    "seed_set",
    "url_state",
    "document_version",
    "chunk_store",
    "chunk_meta",
    "product_snapshot",
    "cell_measurement",
    "cycle",
    "run_lineage",
    "index_partition",
    "index_composition",
    "attestation",
}


@pytest.fixture()
def conn() -> sqlite3.Connection:
    connection = connect_state(":memory:")
    yield connection
    connection.close()


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _pk(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = [
        row
        for row in conn.execute(f"PRAGMA table_info({table})")
        if int(row["pk"]) > 0
    ]
    return [row["name"] for row in sorted(rows, key=lambda row: int(row["pk"]))]


def test_all_tables_exist(conn: sqlite3.Connection) -> None:
    assert EXPECTED_TABLES <= _tables(conn)


def test_schema_version(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    assert row is not None
    assert row["value"] == STATE_SCHEMA_VERSION == "3"


@pytest.mark.parametrize(
    ("table", "primary_key"),
    [
        ("product_state", ["product_id"]),
        ("seed_set", ["seed_id"]),
        ("url_state", ["product_id", "normalized_url"]),
        ("product_snapshot", ["snapshot_id"]),
        ("cell_measurement", ["run_id", "product_id", "param_id"]),
        ("run_lineage", ["run_id"]),
    ],
)
def test_primary_keys(
    conn: sqlite3.Connection, table: str, primary_key: list[str]
) -> None:
    assert _pk(conn, table) == primary_key


def test_indexes_exist(conn: sqlite3.Connection) -> None:
    names = {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
    }
    assert {
        "idx_seed_product",
        "idx_doc_product",
        "idx_snapshot_product",
        "idx_cell_measure_product",
        "idx_partition_product",
    } <= names


def test_row_factory_is_row(conn: sqlite3.Connection) -> None:
    assert conn.row_factory is sqlite3.Row


def test_reconnect_is_idempotent(tmp_path: Path) -> None:
    """Повторное открытие той же БД не падает и сохраняет данные."""
    db_path = tmp_path / "state.db"
    first = connect_state(db_path)
    first.execute(
        "INSERT INTO product_state (product_id, canonical_name, onboarding_state, "
        "added_at, added_by) VALUES ('p1', 'Product 1', 'ready', '2026-01-01', 'test')"
    )
    first.commit()
    first.close()

    second = connect_state(db_path)
    try:
        row = second.execute(
            "SELECT canonical_name FROM product_state WHERE product_id = 'p1'"
        ).fetchone()
        assert row is not None and row["canonical_name"] == "Product 1"
        assert (
            second.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()["value"]
            == STATE_SCHEMA_VERSION
        )
    finally:
        second.close()


def test_additive_migration_adds_lineage_columns(tmp_path: Path) -> None:
    """Старая ``run_lineage`` без ``products_json``/``refresh_policy`` досоздаётся."""
    db_path = tmp_path / "state.db"
    legacy = sqlite3.connect(db_path)
    legacy.execute(
        "CREATE TABLE run_lineage (run_id TEXT PRIMARY KEY, package_id TEXT NOT NULL)"
    )
    legacy.commit()
    legacy.close()

    conn = connect_state(db_path)
    try:
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(run_lineage)")
        }
    finally:
        conn.close()

    assert {"products_json", "refresh_policy"} <= columns


def test_state_db_path(tmp_path: Path) -> None:
    assert state_db_path(tmp_path, "case_x") == tmp_path / "case_x" / "state.db"
