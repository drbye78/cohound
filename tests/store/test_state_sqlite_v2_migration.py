"""Миграция ``state.db`` v2 → v3 (09 §4.2/§4.4): аддитивно, без потери данных.

БД v2 строится **вручную** (не через текущий ``STATE_DDL``), чтобы тест не подгонял
историю под реализацию. Проверяется: строки сохраняются, версия становится ``"3"``,
поддерживающие индексы v3 появляются, дрейф колонок лечится ``ALTER TABLE``, повторное
открытие идемпотентно.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from core.store.state_sqlite import STATE_SCHEMA_VERSION, connect_state

#: Схема v2 в том виде, в каком она существовала до MVP-3a (ключевые таблицы среза).
V2_DDL = """
CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE product_state (
    product_id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    vendor TEXT,
    onboarding_state TEXT NOT NULL,
    onboarding_reason TEXT,
    added_at TEXT NOT NULL,
    added_by TEXT NOT NULL,
    re_added_at TEXT,
    retired_at TEXT,
    last_corpus_run_id TEXT,
    last_evaluated_run_id TEXT,
    last_completed_run_id TEXT,
    last_completed_cycle_id TEXT,
    last_attempt_run_id TEXT,
    last_attempt_status TEXT,
    snapshot_digest TEXT
);

CREATE TABLE seed_set (
    seed_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    role TEXT NOT NULL,
    source TEXT NOT NULL,
    source_ref TEXT,
    profile_key TEXT,
    state TEXT NOT NULL,
    added_at TEXT NOT NULL,
    added_by TEXT NOT NULL,
    last_verified_at TEXT,
    retired_at TEXT,
    UNIQUE (product_id, normalized_url)
);

CREATE TABLE url_state (
    product_id TEXT NOT NULL,
    url TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    doc_version_id TEXT,
    etag TEXT,
    last_modified TEXT,
    last_http_status INTEGER,
    last_checked_at TEXT,
    last_changed_at TEXT,
    next_check_at TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error_kind TEXT,
    reuse_kind TEXT,
    seed_id TEXT,
    PRIMARY KEY (product_id, normalized_url)
);

CREATE TABLE document_version (
    doc_version_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    url TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    level TEXT NOT NULL,
    raw_path TEXT,
    chunker_version TEXT,
    chunk_ids_digest TEXT,
    first_seen_run_id TEXT NOT NULL,
    last_seen_run_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    status TEXT NOT NULL,
    UNIQUE (product_id, content_hash, extractor_version, level)
);

CREATE TABLE chunk_store (
    content_hash TEXT PRIMARY KEY,
    text TEXT NOT NULL,
    lang TEXT
);

CREATE TABLE chunk_meta (
    chunk_id TEXT PRIMARY KEY,
    doc_version_id TEXT NOT NULL REFERENCES document_version(doc_version_id),
    product_id TEXT NOT NULL,
    chunk_no INTEGER NOT NULL,
    content_hash TEXT NOT NULL REFERENCES chunk_store(content_hash),
    chunker_version TEXT NOT NULL,
    heading_path TEXT,
    context_prefix TEXT,
    lang TEXT
);

CREATE TABLE run_lineage (
    run_id TEXT PRIMARY KEY,
    package_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    previous_run_id TEXT,
    cohort_digest TEXT NOT NULL,
    seeds_digest TEXT NOT NULL,
    execution_contract_digest TEXT NOT NULL,
    change_class TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT,
    products_json TEXT,
    refresh_policy TEXT
);
"""

_V2_ROWS = """
INSERT INTO schema_meta (key, value) VALUES ('schema_version', '2');
INSERT INTO product_state (product_id, canonical_name, onboarding_state, added_at, added_by)
VALUES ('p1', 'Product One', 'ready', '2026-01-01', 'test');
INSERT INTO url_state (product_id, url, normalized_url, doc_version_id, etag, consecutive_failures)
VALUES ('p1', 'https://p1.test/a', 'https://p1.test/a', 'dv_old', '"v1"', 1);
INSERT INTO document_version (
    doc_version_id, doc_id, product_id, url, content_hash, extractor_version, level,
    first_seen_run_id, last_seen_run_id, first_seen_at, last_seen_at, status
) VALUES (
    'dv_old', 'p1:h_old', 'p1', 'https://p1.test/a', 'h_old', 'ext-1', 'html',
    'run-0', 'run-0', '2026-01-01', '2026-01-01', 'active'
);
INSERT INTO chunk_store (content_hash, text) VALUES ('ch_old', 'старый текст');
INSERT INTO chunk_meta (chunk_id, doc_version_id, product_id, chunk_no, content_hash, chunker_version)
VALUES ('c_old', 'dv_old', 'p1', 0, 'ch_old', 'chk-1');
INSERT INTO run_lineage (
    run_id, package_id, cycle_id, cohort_digest, seeds_digest,
    execution_contract_digest, change_class, status, started_at
) VALUES ('run-0', 'pkg', 'cycle-0', 'cd', 'sd', 'ecd', 'none', 'complete', '2026-01-01');
"""


def _build_v2(path: Path, ddl: str = V2_DDL, rows: str = _V2_ROWS) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(ddl)
    conn.executescript(rows)
    conn.commit()
    conn.close()


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _indexes(conn: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
    }


def test_v2_db_opens_under_schema_3_without_data_loss(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    _build_v2(db_path)

    conn = connect_state(db_path)
    try:
        assert (
            conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()["value"]
            == STATE_SCHEMA_VERSION
            == "3"
        )
        # Строки v2 на месте.
        assert (
            conn.execute(
                "SELECT canonical_name FROM product_state WHERE product_id = 'p1'"
            ).fetchone()["canonical_name"]
            == "Product One"
        )
        assert (
            conn.execute(
                "SELECT etag FROM url_state WHERE normalized_url = 'https://p1.test/a'"
            ).fetchone()["etag"]
            == '"v1"'
        )
        assert (
            conn.execute(
                "SELECT text FROM chunk_store WHERE content_hash = 'ch_old'"
            ).fetchone()["text"]
            == "старый текст"
        )
        assert (
            conn.execute(
                "SELECT chunk_id FROM chunk_meta WHERE doc_version_id = 'dv_old'"
            ).fetchone()["chunk_id"]
            == "c_old"
        )
        # v3: поддерживающие индексы.
        assert {"idx_url_state_product", "idx_chunk_meta_doc"} <= _indexes(conn)
        # Колонки durable-субстрата совпадают с текущим DDL (ничего не потеряно).
        assert {
            "doc_version_id",
            "etag",
            "last_modified",
            "last_http_status",
            "reuse_kind",
            "seed_id",
            "consecutive_failures",
            "last_error_kind",
        } <= _columns(conn, "url_state")
        assert {
            "chunk_id",
            "doc_version_id",
            "content_hash",
            "chunker_version",
            "heading_path",
            "context_prefix",
            "lang",
        } <= _columns(conn, "chunk_meta")
    finally:
        conn.close()


def test_migration_heals_column_drift(tmp_path: Path) -> None:
    """Дрейфующая v2-таблица досоздаёт недостающие колонки, строки сохраняются."""
    drifted = V2_DDL.replace("    next_check_at TEXT,\n", "").replace(
        "    reuse_kind TEXT,\n", ""
    ).replace("    context_prefix TEXT,\n", "").replace(
        "    chunker_version TEXT,\n    chunk_ids_digest TEXT,\n", ""
    )
    db_path = tmp_path / "state.db"
    _build_v2(db_path, ddl=drifted)

    conn = connect_state(db_path)
    try:
        assert {"next_check_at", "reuse_kind"} <= _columns(conn, "url_state")
        assert {"context_prefix"} <= _columns(conn, "chunk_meta")
        assert {"chunker_version", "chunk_ids_digest"} <= _columns(
            conn, "document_version"
        )
        row = conn.execute(
            "SELECT next_check_at, reuse_kind, etag FROM url_state "
            "WHERE normalized_url = 'https://p1.test/a'"
        ).fetchone()
        assert row["etag"] == '"v1"'
        assert row["next_check_at"] is None
        assert row["reuse_kind"] is None
        assert (
            conn.execute(
                "SELECT chunk_id FROM chunk_meta WHERE doc_version_id = 'dv_old'"
            ).fetchone()["chunk_id"]
            == "c_old"
        )
    finally:
        conn.close()


def test_reopen_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    _build_v2(db_path)

    first = connect_state(db_path)
    first.close()
    second = connect_state(db_path)
    try:
        assert (
            second.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()["value"]
            == "3"
        )
        assert (
            second.execute(
                "SELECT COUNT(*) AS n FROM url_state WHERE product_id = 'p1'"
            ).fetchone()["n"]
            == 1
        )
    finally:
        second.close()
