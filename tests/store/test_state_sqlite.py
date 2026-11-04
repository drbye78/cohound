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

#: Таблицы §4.2 (11 предметных + ``schema_meta``).
EXPECTED_TABLES = {
    "schema_meta",
    "product_state",
    "seed_set",
    "url_state",
    "container_state",
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
    "capture",
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
    assert _tables(conn) >= EXPECTED_TABLES


def test_schema_version(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    assert row is not None
    assert row["value"] == STATE_SCHEMA_VERSION == "11"


@pytest.mark.parametrize(
    ("table", "primary_key"),
    [
        ("product_state", ["product_id"]),
        ("seed_set", ["seed_id"]),
        ("url_state", ["product_id", "normalized_url"]),
        ("product_snapshot", ["snapshot_id"]),
        ("cell_measurement", ["run_id", "product_id", "param_id"]),
        ("run_lineage", ["run_id"]),
        ("capture", ["capture_id"]),
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


def test_url_state_observation_columns_declared(conn: sqlite3.Connection) -> None:
    """План 20 §3: наблюдение артефакта объявлено в ``url_state`` (STATE 11).

    ``last_checked_at`` для этой роли не переиспользуется: он пишется при любом исходе,
    включая отказ, а отказ наблюдённой версией не является.
    """
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(url_state)")}
    assert {
        "observed_at",
        "observed_how",
        "capture_id",
        "observed_actor",
    } <= columns


def test_capture_table_shape(conn: sqlite3.Connection) -> None:
    """Снятие корня (STATE 11): обязательные колонки и CHECK ``observed_how``."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(capture)")}
    assert {
        "capture_id",
        "product_id",
        "run_id",
        "root_url",
        "started_at",
        "finished_at",
        "complete",
        "coverage_digest",
        "doc_set_digest",
        "policy_digest",
        "observed_how",
        "actor",
        "superseded_by",
    } <= columns
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO capture (capture_id, product_id, root_url, started_at,"
            " complete, observed_how) VALUES ('c1', 'p1', 'https://p1.test/',"
            " '2026-01-01T00:00:00Z', 1, 'invented')"
        )


def test_fresh_db_declares_lineage_columns(conn: sqlite3.Connection) -> None:
    """Колонки ``run_lineage``, прежде досоздаваемые ``ALTER TABLE``, объявлены в DDL.

    Прежде тест строил «старую» таблицу без этих колонок и проверял, что открытие их
    допишет. Шим удалён (решение владельца 01.11.2026), поэтому проверка переехала на
    новый носитель — ``STATE_DDL``: колонки обязаны быть в свежей БД, иначе колонку
    забыли при объявлении. Отказ старому файлу проверяет
    ``tests/store/test_state_schema_gate.py``.
    """
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(run_lineage)")}
    assert {"products_json", "refresh_policy"} <= columns


def test_state_db_path(tmp_path: Path) -> None:
    assert state_db_path(tmp_path, "case_x") == tmp_path / "case_x" / "state.db"
