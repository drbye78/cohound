"""coHound — хранилище (store, L3): SQLite DDL и подключение (§4.4).

Соответствует §4.4 регламента. Схема категорийно-нейтральна. Индексы:
``index_store/{package}/{build}/{product_id}/`` — продукт-партиции (I17).

Триггеры обеспечивают инварианты I4/I6/I7/I13/I18 на уровне БД (§2.3). ``StorePort``
(``core/ports/store.py``) — контракт доступа; реализация ``SqliteStore`` появится в W3.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = "2.2.0"

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id    TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    vendor        TEXT NOT NULL,
    jurisdiction  TEXT,
    first_seen    TEXT,
    last_seen     TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    run_id                TEXT PRIMARY KEY,
    created_at            TEXT NOT NULL,
    mode                  TEXT NOT NULL,              -- strict|preliminary
    case_id               TEXT NOT NULL,
    anchor_product_id     TEXT NOT NULL,
    cohort_ref            TEXT,                        -- cohort.yaml
    focus_text            TEXT,
    focus_modules         TEXT,                        -- JSON array
    package_id            TEXT,
    package_version       TEXT,
    catalog_version       TEXT,
    status_dict_version   TEXT,
    scoring_policy_version TEXT,
    methodology_version   TEXT,
    embed_model_id        TEXT,
    index_build_id        TEXT,
    retrieval_profile     TEXT,                        -- R0|R1|R2
    retrieval_params_json TEXT,                        -- fusion-параметры (rrf_k и пр.) — S1
    retrieval_cutoff_K    INTEGER,                     -- I20
    search_effort_budget_json TEXT,                    -- I23
    requested_ranking     TEXT,                        -- запрошенный режим (run.yaml) — K6
    computed_ranking      TEXT,                        -- вычисленный допустимый ранг — K6
    settings_provenance_json TEXT,                     -- A3 (v2.1): {setting: {source, experiment_id?}}
    snapshot_date         TEXT,
    state                 TEXT
);
-- llm_scenario_map_json удалён: канонический носитель — таблица llm_scenarios (S2).

-- §7.4: детерминированная маршрутизация модель->сценарий (I21)
CREATE TABLE IF NOT EXISTS llm_scenarios (
    run_id            TEXT NOT NULL,
    scenario          TEXT NOT NULL,                   -- evaluator|search_adapter|discovery|concept_scout
    model_id          TEXT NOT NULL,
    model_version     TEXT,
    endpoint_kind     TEXT NOT NULL,                   -- self_host|api
    web_tool          INTEGER NOT NULL DEFAULT 0,      -- 0/1; evaluator обязан 0 (I18)
    decoder           TEXT,
    seed              INTEGER,
    temperature       REAL,
    constrained_decoding TEXT,
    PRIMARY KEY (run_id, scenario),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

-- §7.1: evaluator не должен иметь веб-тула (I18) — проверяется триггером ниже.

CREATE TABLE IF NOT EXISTS corpus_docs (
    doc_id           TEXT PRIMARY KEY,
    product_id       TEXT NOT NULL,
    url              TEXT NOT NULL,
    fetched_at       TEXT,
    content_hash     TEXT,
    raw_path         TEXT,
    extractor_version TEXT,
    render_mode      TEXT,
    lang             TEXT
);

CREATE TABLE IF NOT EXISTS corpus_chunks (
    chunk_id        TEXT PRIMARY KEY,
    doc_id          TEXT NOT NULL REFERENCES corpus_docs(doc_id),
    product_id      TEXT NOT NULL,
    url             TEXT,
    heading_path    TEXT,
    lang            TEXT,
    chunk_no        INTEGER,
    text            TEXT NOT NULL,
    context_prefix  TEXT,                              -- НЕ входит в валидацию цитат (§4.4)
    content_hash    TEXT,
    extractor_version TEXT,                            -- P2: версия экстрактора чанка
    build_id        TEXT,                              -- привязка чанка к сборке (S5)
    embed_model_id  TEXT,
    embedded_at     TEXT
);

-- K1: пакетная сборка (index_build_id, иммутабельна) × продукт-партиция (I17).
-- Один index_build_id даёт N партиций → составной PK.
CREATE TABLE IF NOT EXISTS index_manifest (
    index_build_id  TEXT NOT NULL,                       -- пакетная сборка (иммутабельная)
    product_id      TEXT NOT NULL,                       -- продукт-партиция (I17)
    package_id      TEXT,
    partition_key   TEXT,                                -- (index_build_id, product_id)
    snapshot_date   TEXT,
    chunker_version TEXT,
    extractor_version TEXT,                              -- P2: композитная версия экстрактора
    embed_model_id  TEXT,
    embed_dim       INTEGER,
    lexical_engine_version TEXT,
    vector_store    TEXT,
    content_digest  TEXT,                                -- P2: отпечаток содержимого партиции
    store_path      TEXT,
    created_at      TEXT,
    stats_json      TEXT,
    PRIMARY KEY (index_build_id, product_id)
);

CREATE TABLE IF NOT EXISTS cells (
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    param_id          TEXT NOT NULL,
    state             TEXT NOT NULL,                    -- pending|preliminary|evaluated|failed|reviewed
    status            TEXT,                             -- NULL при failed/preliminary (I4/I13)
    preliminary_status TEXT,
    source_kind       TEXT,
    justification     TEXT,
    rule_note         TEXT,
    validation_notes  TEXT,
    failure_kind      TEXT,
    model_id          TEXT,
    llm_scenario      TEXT,                             -- §7.4/I21
    prompt_hash       TEXT,
    index_build_id    TEXT,                             -- provenance ячейки (R7/§13)
    retrieval_profile TEXT,
    retrieval_params_json TEXT,
    stale_reason      TEXT,                             -- инвалидация при смене сборки/prompt
    primary_evidence_id TEXT,
    reviewed_by       TEXT,
    updated_at        TEXT,
    PRIMARY KEY (run_id, product_id, param_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS cell_evidence (
    evidence_id       TEXT PRIMARY KEY,                 -- детерминированный: H(run_id,product,param,source_id,kind,seq) — S8
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    param_id          TEXT NOT NULL,
    source_kind       TEXT,
    evidence_type     TEXT,                             -- direct|derived (K5)
    source_id         TEXT,                             -- I10/I15: резолв в URL
    source_url        TEXT,
    citation_quote    TEXT,
    content_hash      TEXT,
    source_scope      TEXT,                             -- absence-evidence (§4.2)
    scope_attested_by TEXT,                             -- человек для param_ref/distribution
    index_build_id    TEXT,                             -- привязка evidence к сборке (R7)
    retrieved_at      TEXT
);

-- S9: персистентные человеческие решения гейтов G1–G4 (носитель reviewed_by, аудит-след).
CREATE TABLE IF NOT EXISTS gate_decisions (
    decision_id       TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL,
    gate              TEXT NOT NULL,                    -- G1|G2|G3|G4
    target            TEXT,                             -- что подтверждено (cohort/cell/...)
    actor             TEXT NOT NULL,                    -- человек
    decided_at        TEXT,
    notes             TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS search_log (
    log_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL,
    phase             TEXT NOT NULL,                    -- discovery|corpus|serp|scout
    kind              TEXT,                             -- retrieval|external (S12)
    provider          TEXT,
    adapter_kind      TEXT,                             -- serp|llm_grounding
    language          TEXT,
    query             TEXT,
    results_count     INTEGER,
    selected          INTEGER,
    lexical_score     REAL,
    dense_score       REAL,
    fused_rank        INTEGER,
    rerank_score      REAL,
    retrieval_score   REAL,
    termination_reason TEXT,
    ts                TEXT
);

CREATE TABLE IF NOT EXISTS search_exhaustion (
    run_id       TEXT NOT NULL,
    product_id   TEXT NOT NULL,
    param_id     TEXT NOT NULL,
    protocol_json TEXT NOT NULL,
    PRIMARY KEY (run_id, product_id, param_id)
);

CREATE TABLE IF NOT EXISTS registry_facts (
    run_id      TEXT NOT NULL,
    product_id  TEXT NOT NULL,
    registry_id TEXT NOT NULL,
    fact_json   TEXT,
    source_url  TEXT,                                   -- R1: 100% имеют source_url
    fetched_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_cells_run_state  ON cells(run_id, state);
CREATE INDEX IF NOT EXISTS idx_chunks_product   ON corpus_chunks(product_id);
CREATE INDEX IF NOT EXISTS idx_chunks_build     ON corpus_chunks(build_id);
CREATE INDEX IF NOT EXISTS idx_log_run_phase    ON search_log(run_id, phase);
CREATE INDEX IF NOT EXISTS idx_evidence_run_cell ON cell_evidence(run_id, product_id, param_id);
CREATE INDEX IF NOT EXISTS idx_gate_run         ON gate_decisions(run_id, gate);

-- I18: оценщик (evaluator) не может быть зарегистрирован с веб-тулом.
CREATE TRIGGER IF NOT EXISTS trg_evaluator_no_web_tool
BEFORE INSERT ON llm_scenarios
FOR EACH ROW
WHEN NEW.scenario = 'evaluator' AND NEW.web_tool <> 0
BEGIN
    SELECT RAISE(ABORT, 'I18: evaluator не может иметь web_tool');
END;

-- I4: failed → status = null.
CREATE TRIGGER IF NOT EXISTS trg_failed_null_status
BEFORE INSERT ON cells
FOR EACH ROW
WHEN NEW.state = 'failed' AND NEW.status IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'I4: failed → status обязан быть null');
END;

-- I13: preliminary-ячейка не несёт разрешённый status.
CREATE TRIGGER IF NOT EXISTS trg_preliminary_null_status
BEFORE INSERT ON cells
FOR EACH ROW
WHEN NEW.state = 'preliminary' AND NEW.status IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'I13: preliminary → status обязан быть null');
END;

-- I7: N/A невозможен автоматически — оценщик не пишет N/A (защита на уровне БД).
CREATE TRIGGER IF NOT EXISTS trg_no_auto_na
BEFORE INSERT ON cells
FOR EACH ROW
WHEN NEW.status = 'N/A' AND NEW.reviewed_by IS NULL
BEGIN
    SELECT RAISE(ABORT, 'I6/I7: N/A требует reviewed_by (только человек)');
END;

-- I10/I15: source_url не задаётся вручную для корпусного evidence — только резолв.
-- (проверка делается кодом резолвера; триггер фиксирует, что source_id обязателен)
CREATE TRIGGER IF NOT EXISTS trg_evidence_source_id
BEFORE INSERT ON cell_evidence
FOR EACH ROW
WHEN NEW.source_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'I10/I15: evidence обязан иметь source_id для системного резолва');
END;
"""


def _ensure_columns(
    conn: sqlite3.Connection, table: str, columns: dict[str, str]
) -> None:
    """Идемпотентно добавляет недостающие колонки ``table`` (DDL-миграция).

    ``CREATE TABLE IF NOT EXISTS`` не меняет существующую таблицу, поэтому дельты схемы
    добавляются явными ``ALTER TABLE ... ADD COLUMN``. Повторный вызов (в т.ч. на уже
    мигрированной БД) — no-op (проверка через ``PRAGMA table_info``).
    """
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    for name, ddl_type in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl_type}")


def connect(db_path: str | Path = ":memory:") -> sqlite3.Connection:
    """Открывает БД и применяет схему (+ идемпотентные DDL-миграции v2.1/v2.2)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    # DDL-миграция v2.1 (A3): старая БД без settings_provenance_json получает колонку.
    # CREATE TABLE IF NOT EXISTS не меняет существующую таблицу — добавляем явно.
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(runs)")}
    if "settings_provenance_json" not in columns:
        conn.execute("ALTER TABLE runs ADD COLUMN settings_provenance_json TEXT")
    # DDL-миграция v2.2 (P2): content-addressed build-id witness — версия экстрактора у чанка
    # и версия экстрактора + отпечаток содержимого у продукт-партиции манифеста.
    _ensure_columns(conn, "corpus_chunks", {"extractor_version": "TEXT"})
    _ensure_columns(
        conn,
        "index_manifest",
        {"extractor_version": "TEXT", "content_digest": "TEXT"},
    )
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()
    return conn
