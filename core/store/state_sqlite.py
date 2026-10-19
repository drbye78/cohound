"""coHound — durable per-product состояние ``state.db`` (09 §4.1/§4.2): DDL и подключение.

Отдельная SQLite-БД **на пакет**: ``state/{package_id}/state.db``. Она переживает прогоны
(в отличие от ``runs/{run_id}/cohound.db``) и лежит вне ``packages/`` (авторские данные не
смешиваются с операционным состоянием). Схема — зеркало §4.2 дизайна онбординга: продукт и
его рубежи измерения, seed-набор, URL/document-версии, текст чанков, снимки продукта,
индекс измерений ячеек, lineage прогонов/циклов, реестр индексных партиций и человеческие
аттестации.

Модуль только **открывает** БД и применяет идемпотентную схему (``IF NOT EXISTS``); он не
содержит бизнес-логики чтения/записи — это ``core/store/state_store.py``
(``SqliteStateStore``). Нейтрален к предметной категории (§1.3): пакет — данные.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

__all__ = ["STATE_SCHEMA_VERSION", "STATE_DDL", "connect_state", "state_db_path"]

#: Версия схемы ``state.db`` (пишется в ``schema_meta.schema_version``).
STATE_SCHEMA_VERSION = "1"

#: DDL §4.2. Идемпотентен: ``CREATE ... IF NOT EXISTS`` на всех объектах.
STATE_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 1. Продукт: готовность и последние завершённые измерения.
CREATE TABLE IF NOT EXISTS product_state (
    product_id            TEXT PRIMARY KEY,
    canonical_name        TEXT NOT NULL,
    vendor                TEXT,
    onboarding_state      TEXT NOT NULL,        -- pending_seeds|ready|blocked|retired
    onboarding_reason     TEXT,                 -- no_seeds|profile_unbound|...
    added_at              TEXT NOT NULL,
    added_by              TEXT NOT NULL,
    re_added_at           TEXT,
    retired_at            TEXT,
    last_corpus_run_id    TEXT,                 -- corpus завершён (не обязательно eval)
    last_evaluated_run_id TEXT,                 -- eval-фаза прошла (pending-ячейки допустимы)
    last_completed_run_id TEXT,                 -- все ячейки разрешены (evaluated/reviewed/NA)
    last_completed_cycle_id TEXT,
    last_attempt_run_id   TEXT,
    last_attempt_status   TEXT,
    snapshot_digest       TEXT
);

-- 2. Seed-набор (durable; файл seeds.yaml — авторский источник, таблица — операционный).
CREATE TABLE IF NOT EXISTS seed_set (
    seed_id        TEXT PRIMARY KEY,            -- short_hash(product_id, normalized_url)
    product_id     TEXT NOT NULL,
    url            TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    role           TEXT NOT NULL,               -- primary|docs|satellite|host_root
    source         TEXT NOT NULL,               -- add_product|operator|discovery|manual
    source_ref     TEXT,
    profile_key    TEXT,
    state          TEXT NOT NULL,               -- enabled|disabled|invalid
    added_at       TEXT NOT NULL,
    added_by       TEXT NOT NULL,
    last_verified_at TEXT,
    retired_at     TEXT,
    UNIQUE (product_id, normalized_url)
);

-- 3. URL → последняя документ-версия + условная валидация.
CREATE TABLE IF NOT EXISTS url_state (
    product_id          TEXT NOT NULL,
    url                 TEXT NOT NULL,
    normalized_url      TEXT NOT NULL,
    doc_version_id      TEXT,
    etag                TEXT,
    last_modified       TEXT,
    last_http_status    INTEGER,
    last_checked_at     TEXT,
    last_changed_at     TEXT,
    next_check_at       TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error_kind     TEXT,
    reuse_kind          TEXT,                   -- live|revalidated|reused|stale
    seed_id             TEXT,                   -- из какого seed'а обнаружен URL
    PRIMARY KEY (product_id, normalized_url)
);

-- 4. Версии документов (immutable; метаданные — бессрочно).
CREATE TABLE IF NOT EXISTS document_version (
    doc_version_id    TEXT PRIMARY KEY,         -- dv_<hash>
    doc_id            TEXT NOT NULL,            -- product_id:content_hash
    product_id        TEXT NOT NULL,
    url               TEXT NOT NULL,            -- representative URL; aliases — в url_state
    content_hash      TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    level             TEXT NOT NULL,
    raw_path          TEXT,                     -- может быть NULL после retention
    chunker_version   TEXT,
    chunk_ids_digest  TEXT,
    first_seen_run_id TEXT NOT NULL,
    last_seen_run_id  TEXT NOT NULL,
    first_seen_at     TEXT NOT NULL,
    last_seen_at      TEXT NOT NULL,
    status            TEXT NOT NULL,            -- active|superseded|evicted
    UNIQUE (product_id, content_hash, extractor_version, level)
);

-- 5. Текст чанков — durable субстрат evidence (raw может быть вытеснен retention).
CREATE TABLE IF NOT EXISTS chunk_store (
    content_hash TEXT PRIMARY KEY,              -- hash чистого текста (I19)
    text         TEXT NOT NULL,
    lang         TEXT
);

CREATE TABLE IF NOT EXISTS chunk_meta (
    chunk_id         TEXT PRIMARY KEY,          -- product:dv:chunker:no
    doc_version_id   TEXT NOT NULL REFERENCES document_version(doc_version_id),
    product_id       TEXT NOT NULL,
    chunk_no         INTEGER NOT NULL,
    content_hash     TEXT NOT NULL REFERENCES chunk_store(content_hash),
    chunker_version  TEXT NOT NULL,
    heading_path     TEXT,
    context_prefix   TEXT,
    lang             TEXT
);

-- 6. Снимок продукта за прогон (только после eval/coverage).
CREATE TABLE IF NOT EXISTS product_snapshot (
    snapshot_id       TEXT PRIMARY KEY,         -- short_hash(run_id, product_id)
    product_id        TEXT NOT NULL,
    cycle_id          TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    status            TEXT NOT NULL,            -- complete|partial|failed
    scope             TEXT NOT NULL,            -- full|product_only
    coverage_complete INTEGER NOT NULL,
    coverage_digest   TEXT,
    doc_set_digest    TEXT,
    partition_build_id TEXT,
    docs_total        INTEGER NOT NULL DEFAULT 0,
    docs_reused       INTEGER NOT NULL DEFAULT 0,
    docs_new          INTEGER NOT NULL DEFAULT 0,
    cells_total       INTEGER NOT NULL DEFAULT 0,
    cells_carried     INTEGER NOT NULL DEFAULT 0,
    cells_measured    INTEGER NOT NULL DEFAULT 0,
    cells_pending     INTEGER NOT NULL DEFAULT 0,
    baseline_complete INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL
);

-- 7. Индекс измерений ячеек (для carry-forward; сами cells/evidence — в run DB).
CREATE TABLE IF NOT EXISTS cell_measurement (
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    param_id          TEXT NOT NULL,
    state             TEXT NOT NULL,
    status            TEXT,
    prompt_hash       TEXT,
    model_id          TEXT,
    llm_scenario      TEXT,
    index_build_id    TEXT,
    partition_build_id TEXT,
    doc_set_digest    TEXT,
    coverage_digest   TEXT,
    catalog_row_digest TEXT,
    cell_reuse_key    TEXT,
    evidence_digest   TEXT,
    measured_at       TEXT NOT NULL,
    PRIMARY KEY (run_id, product_id, param_id)
);

-- 8. Lineage прогонов и циклов.
CREATE TABLE IF NOT EXISTS cycle (
    cycle_id      TEXT PRIMARY KEY,
    package_id    TEXT NOT NULL,
    label         TEXT,
    started_at    TEXT NOT NULL,
    cohort_digest TEXT,
    methodology_version TEXT,
    execution_contract_digest TEXT
);

CREATE TABLE IF NOT EXISTS run_lineage (
    run_id            TEXT PRIMARY KEY,
    package_id        TEXT NOT NULL,
    cycle_id          TEXT NOT NULL,
    previous_run_id   TEXT,
    cohort_digest     TEXT NOT NULL,
    seeds_digest      TEXT NOT NULL,
    execution_contract_digest TEXT NOT NULL,
    change_class      TEXT NOT NULL,
    status            TEXT NOT NULL,            -- running|complete|partial|failed|aborted
    started_at        TEXT NOT NULL,
    completed_at      TEXT,
    error             TEXT
);

-- 9. Реестр иммутабельных индексных партиций и композиций.
CREATE TABLE IF NOT EXISTS index_partition (
    package_id        TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    partition_build_id TEXT NOT NULL,
    content_digest    TEXT NOT NULL,
    chunker_version   TEXT NOT NULL,
    embed_model_id    TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    lexical_engine_version TEXT NOT NULL,
    vector_store      TEXT NOT NULL,
    store_path        TEXT NOT NULL,
    chunks            INTEGER NOT NULL,
    dim               INTEGER,
    created_at        TEXT NOT NULL,
    last_used_run_id  TEXT,
    PRIMARY KEY (package_id, product_id, partition_build_id)
);

CREATE TABLE IF NOT EXISTS index_composition (
    composition_id  TEXT PRIMARY KEY,           -- cb_<hash>
    package_id      TEXT NOT NULL,
    members_json    TEXT NOT NULL,              -- [{product_id, partition_build_id, content_digest}]
    created_at      TEXT NOT NULL
);

-- 10. Человеческие аттестации (не переносятся молча между циклами).
CREATE TABLE IF NOT EXISTS attestation (
    attestation_id  TEXT PRIMARY KEY,
    product_id      TEXT NOT NULL,
    param_id        TEXT NOT NULL,
    source_scope    TEXT NOT NULL,
    attested_by     TEXT NOT NULL,
    attested_at     TEXT NOT NULL,
    doc_set_digest  TEXT NOT NULL,              -- к какому корпусу относилась
    coverage_digest TEXT NOT NULL,
    superseded_at   TEXT
);

CREATE INDEX IF NOT EXISTS idx_seed_product ON seed_set(product_id, state);
CREATE INDEX IF NOT EXISTS idx_doc_product  ON document_version(product_id, status);
CREATE INDEX IF NOT EXISTS idx_snapshot_product ON product_snapshot(product_id, created_at);
CREATE INDEX IF NOT EXISTS idx_cell_measure_product ON cell_measurement(product_id, run_id);
CREATE INDEX IF NOT EXISTS idx_partition_product ON index_partition(package_id, product_id);
"""


def connect_state(db_path: str | Path = ":memory:") -> sqlite3.Connection:
    """Открывает ``state.db`` и применяет идемпотентную схему §4.2.

    ``row_factory = sqlite3.Row`` — доступ к колонкам по имени (как в
    :func:`core.store.sqlite.connect`). Схема применяется на каждом открытии
    (``IF NOT EXISTS`` — no-op для существующих объектов), версия схемы пишется в
    ``schema_meta``. Отдельного helper'а аддитивных миграций нет: схема этого среза
    совпадает с §4.2 (колонок сверх документа нет).
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(STATE_DDL)
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
        (STATE_SCHEMA_VERSION,),
    )
    conn.commit()
    return conn


def state_db_path(root: str | Path, package_id: str) -> Path:
    """Путь БД состояния пакета: ``root / package_id / "state.db"`` (09 §4.1)."""
    return Path(root) / str(package_id) / "state.db"
