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

**Версия схемы — гейт, а не метка.** Штатное открытие читает
``schema_meta.schema_version`` **до** ``STATE_DDL``; файл другой версии (или файл с
таблицами, но без объявленной версии) отвергается
:class:`core.store.sqlite.SchemaVersionMismatch`. Миграций при открытии нет ни одной:
``ALTER TABLE ... ADD COLUMN`` не осталось, все колонки объявлены в :data:`STATE_DDL`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from core.util.schema_gate import (
    SchemaVersionMismatch,
    declared_schema_version,
    schema_mismatch_message,
)

__all__ = ["STATE_DDL", "STATE_SCHEMA_VERSION", "connect_state", "state_db_path"]

#: Версия схемы ``state.db`` (пишется в ``schema_meta.schema_version``).
#: 2 — аддитивно: ``run_lineage.products_json`` (состав когорты прогона) и
#: ``run_lineage.refresh_policy`` (политика наблюдения).
#: 3 — durable-субстрат инкрементального корпуса (09 §4.2/§6.3): ``url_state``,
#: ``document_version``, ``chunk_store``, ``chunk_meta`` объявлены рабочими таблицами
#: (колонки уже были в DDL с v1), добавлены поддерживающие индексы
#: ``idx_url_state_product``/``idx_chunk_meta_doc``; миграция лечила дрейф колонок.
#: 4 — аттестация **профиля** пакета-якоря (``profile_attestation``): человек аттестует
#: содержимое профиля (дайджест) один раз, последующие прогоны с тем же профилем не
#: требуют повторного решения G1/G2.
#: 5 — частичность извлечения как **durable** свойство версии документа
#: (``document_version.extract_partial``/``extract_partial_reason``/
#: ``extract_pages_converted``/``extract_page_count``): переиспользование версии
#: (``304``/тот же ``content_hash``) не знало, прочитан ли документ целиком, и объявляло
#: покрытие полным там, где текст был усечён. Колонки допускают ``NULL``, и ``NULL``
#: («не измерено») — единственное честное значение для строк, записанных до v5.
#: 6 — **гейт версии вместо миграции** (решение владельца 01.11.2026): все колонки, которые
#: прежде досоздавались при открытии (``_ADDITIVE_COLUMNS``), и индексы
#: (``_POST_MIGRATION_DDL``) объявлены прямо в :data:`STATE_DDL`. ``ALTER TABLE ... ADD COLUMN``
#: при штатном открытии не осталось ни одного, а старая БД отвергается видимо: тихий догон
#: схемы объявлял файл «новым», не будучи им. Цена названа прямо — каждый существующий
#: ``state/{package_id}/state.db`` версий ≤ 5 отвергается и пересоздаётся оператором.
#: 7 — ``chunk_meta.char_start``/``char_end``: смещения куска в тексте документа. Без них
#: chunk-set, переиспользованный из ``state.db``, терял провенанс куска (``corpus_chunks``
#: получал ``NULL``), и документ из повторного прогона отличался бы от документа первого.
#: Колонки объявлены в ``CREATE TABLE``; ``ALTER TABLE`` при открытии нет, поэтому файл
#: версии ≤ 6 отвергается ``SchemaVersionMismatch`` (путь файла в ошибке) и пересоздаётся
#: оператором — тихий догон колонок объявил бы старый файл текущим, не будучи им.
#: 8 — политика краула по областям (``crawl_policy``): продукт/хост/корень/маска пути,
#: append-only через ``superseded_by``. Таблица объявлена в :data:`STATE_DDL`, шимов нет;
#: файл версии ≤ 7 отвергается ``SchemaVersionMismatch`` (план 19 §4).
#: 9 — запись снятия корпуса (``corpus_capture``/``corpus_capture_doc``): что именно было
#: зафиксировано при ``frozen``-прогоне (``captured_at``, ``capture_digest``,
#: ``coverage_complete``/``coverage_digest``, ``doc_set_digest``, ``reuse_kind`` документов)
#: и durable-группы дедупа, которые использовались. Без них следующий ``frozen``-прогон
#: выбирал бы канонический адрес наугад: ``crawl_session_items`` он не пишет. Таблицы
#: объявлены в :data:`STATE_DDL`, шимов нет; файл версии ≤ 8 отвергается
#: ``SchemaVersionMismatch`` (план 19 §P3).
#: 10 — состояние **контейнера** (``container_state``): URL → хеш ретейнённых байтов
#: архива, путь, формат и исход спуска. Контейнер не имеет версии документа (членов он не
#: несёт), поэтому путь и хеш его байтов жили только в БД прогона, и проход без сети
#: (``frozen``) воспроизводил лишь верхнеуровневые документы, теряя членов молча (план 19
#: §E.7). Таблица объявлена в :data:`STATE_DDL`, шимов нет; файл версии ≤ 9 отвергается
#: ``SchemaVersionMismatch``.
#: 11 — **наблюдение артефакта** и **снятие корня** (план 20 §3/§4): поля
#: ``url_state.observed_at``/``observed_how``/``capture_id``/``observed_actor`` и таблица
#: ``capture`` (один обход одного корня: даты, ``complete``, дайджесты, ``observed_how``,
#: ``actor``, ``superseded_by``). ``last_checked_at`` роли наблюдения не несёт: он
#: пишется при любом исходе, включая отказ. Шимов нет; файл версии ≤ 10 отвергается
#: ``SchemaVersionMismatch``.
STATE_SCHEMA_VERSION = "11"

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
    -- v11 (план 20 §3): наблюдение артефакта. NULL — наблюдения не было либо писатель
    -- не передал значение (upsert сохраняет прежнее: COALESCE). ``last_checked_at``
    -- для этой роли не годится — он пишется и при отказе.
    observed_at         TEXT,                   -- когда версия наблюдалась
    observed_how        TEXT,                   -- live|revalidated|adopted|scheduled
    capture_id          TEXT,                   -- снятие корня (capture.capture_id)
    observed_actor      TEXT,                   -- кто предоставил (adopted), иначе NULL
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
    -- v5: частичность извлечения — измеренное свойство версии, а не вывод из текста.
    -- NULL — не измерено (строка до v5); читать NULL как «полное» запрещено.
    extract_partial        INTEGER,             -- 1 частично, 0 полное, NULL неизвестно
    extract_partial_reason TEXT,
    extract_pages_converted INTEGER,            -- прочитано страниц (NULL — прибор не сказал)
    extract_page_count      INTEGER,            -- заявлено документом
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
    lang             TEXT,
    -- L1b-2b: смещения куска в тексте документа. NULL — кусок не является точным
    -- срезом (абзацный чанкер); 0 — настоящая позиция, поэтому NULL и 0 не смешиваются.
    char_start       INTEGER,
    char_end         INTEGER
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
    error             TEXT,
    products_json     TEXT,                     -- JSON-массив product_id (NULL = неизвестно)
    refresh_policy    TEXT                      -- auto|revalidate|refresh|offline
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

-- 11. Аттестация профиля пакета-якоря для G1/G2 (v4).
-- Профиль квази-константен (2–5 правок в год), прогоны — еженедельные: человек
-- аттестует содержимое профиля (profile_digest) один раз, и прогоны с тем же
-- профилем не требуют повторного решения. НЕ путать с cell-level ``attestation``
-- выше (та — source_scope аттестация охвата, I2b).
CREATE TABLE IF NOT EXISTS profile_attestation (
    attestation_id  TEXT PRIMARY KEY,
    case_id         TEXT NOT NULL,
    package_id      TEXT NOT NULL,
    anchor          TEXT NOT NULL,
    gate            TEXT NOT NULL,              -- G1 | G2
    profile_digest  TEXT NOT NULL,
    digest_material TEXT NOT NULL,              -- JSON: компоненты дайджеста, для diff при расхождении
    attested_by     TEXT NOT NULL,
    attested_at     TEXT NOT NULL,
    superseded_at   TEXT,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_profile_attest
    ON profile_attestation(case_id, package_id, anchor, gate, profile_digest);

-- 12. Политика краула по областям (план 19 §4, STATE 8). Append-only: смена режима не
-- правит строку, а вставляет новую и проставляет прежней ``superseded_by`` (аудит
-- решений человека сохраняется). ``host``/``path`` — NULL только там, где область их не
-- несёт; ``interval_hours`` обязателен при mode='scheduled' (проверяется писателем и
-- резолвером: SQL CHECK не умеет «обязателен только для одного значения»).
CREATE TABLE IF NOT EXISTS crawl_policy (
    policy_id      INTEGER PRIMARY KEY,
    product_id     TEXT NOT NULL,
    scope_kind     TEXT NOT NULL CHECK (scope_kind IN ('product','host','root','path')),
    host           TEXT,          -- NULL только при scope_kind='product'
    path           TEXT,          -- NULL при 'product'/'host'; маска при 'path'
    mode           TEXT NOT NULL CHECK (mode IN ('auto','on_request','scheduled','network_denied')),
    interval_hours INTEGER,       -- обязателен при mode='scheduled', иначе NULL
    note           TEXT,
    actor          TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    superseded_by  INTEGER REFERENCES crawl_policy(policy_id)
);

CREATE INDEX IF NOT EXISTS crawl_policy_scope
    ON crawl_policy (product_id, scope_kind, host, path);

-- 13. Запись снятия корпуса (план 19 §P3, STATE 9). Append-only: снятие — наблюдение,
-- а не настройка. ``captured_at`` — провенанс (в ``run_id`` не входит, как
-- ``extractor_version``); ``capture_digest`` — отпечаток объявленного входа снятия.
-- ``url_state.last_checked_at`` для этой роли не годится: он обновляется при **любом**
-- исходе, включая отказ, и потому не отвечает на вопрос «какой корпус зафиксирован».
CREATE TABLE IF NOT EXISTS corpus_capture (
    capture_id        INTEGER PRIMARY KEY,
    product_id        TEXT NOT NULL,
    run_id            TEXT NOT NULL,
    captured_at       TEXT NOT NULL,
    capture_digest    TEXT NOT NULL,             -- набор корней + набор URL + seeds_digest
    seeds_digest      TEXT NOT NULL,
    source            TEXT NOT NULL CHECK (source IN ('bytes','chunks')),
    coverage_complete INTEGER NOT NULL,
    coverage_digest   TEXT,
    doc_set_digest    TEXT NOT NULL
);

-- 14. Документы снятия и durable-группы дедупа: ``doc_hash`` → URL'ы одного документа.
-- Группа читается как ``GROUP BY capture_id, doc_hash``; канонический адрес — ``min(url)``,
-- поэтому ``frozen`` не зависит от порядка готовности и не обращается к
-- ``crawl_session_items`` (он их не пишет).
CREATE TABLE IF NOT EXISTS corpus_capture_doc (
    capture_id     INTEGER NOT NULL REFERENCES corpus_capture(capture_id),
    product_id     TEXT NOT NULL,
    doc_hash       TEXT NOT NULL,
    url            TEXT NOT NULL,
    normalized_url TEXT NOT NULL,
    reuse_kind     TEXT NOT NULL,
    PRIMARY KEY (capture_id, url)
);

CREATE INDEX IF NOT EXISTS idx_corpus_capture_product
    ON corpus_capture(product_id, captured_at, capture_id);
CREATE INDEX IF NOT EXISTS idx_corpus_capture_doc_hash
    ON corpus_capture_doc(product_id, doc_hash);

-- 15. Состояние контейнера (план 19 §E.7, STATE 10): URL → последний ретейнённый архив.
-- Контейнер **не** документ: версии документа у него нет (``document_version`` описывает
-- извлечённый текст), поэтому путь и хеш его байтов прежде жили только в БД прогона
-- (``archive_observations``/``corpus_members``), и проход без сети воспроизводил лишь
-- верхнеуровневые документы — члены контейнера выпадали молча. ``archive_content_hash`` —
-- хеш ретейнённых байтов (имя файла retention, I19), не хеш текста. ``status`` —
-- :class:`~core.domain.enums.ArchiveDescentStatus` последнего спуска. Строка —
-- провенанс, а не вход ``run_id``: наблюдение байтов не меняет условий измерения.
CREATE TABLE IF NOT EXISTS container_state (
    product_id           TEXT NOT NULL,
    url                  TEXT NOT NULL,
    normalized_url       TEXT NOT NULL,
    archive_content_hash TEXT NOT NULL,
    raw_path             TEXT NOT NULL,
    format               TEXT,
    status               TEXT NOT NULL CHECK (
        status IN ('planned','descended','unreadable','budget_exhausted')
    ),
    first_seen_run_id    TEXT NOT NULL,
    last_seen_run_id     TEXT NOT NULL,
    first_seen_at        TEXT NOT NULL,
    last_seen_at         TEXT NOT NULL,
    PRIMARY KEY (product_id, normalized_url)
);

CREATE INDEX IF NOT EXISTS idx_container_state_product
    ON container_state(product_id, status);

-- 16. Снятие корня (план 20 §3/§4, STATE 11): один обход одного корня. Отличие от
-- ``corpus_capture`` (13): та фиксирует корпус **без наблюдения сети** (``frozen``), а
-- снятие описывает акт наблюдения — живой обход, ревалидацию, ручное принятие или
-- работу планировщика — и на него ссылается ``url_state.capture_id``. ``capture_id``
-- детерминирован от ``(product_id, root_url, run_id)``: повторный вход в фазу не
-- плодит снятий. Append-only: ``superseded_by`` ставится явным методом, не автоматически.
-- ``complete`` — исчерпан ли корень **на дату снятия** (у adoption всегда 0: принятый
-- артефакт не обход и исчерпания не удостоверяет). Даты/дайджесты — провенанс, в
-- ``run_id`` не входят.
CREATE TABLE IF NOT EXISTS capture (
    capture_id      TEXT PRIMARY KEY,
    product_id      TEXT NOT NULL,
    run_id          TEXT,
    root_url        TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    complete        INTEGER NOT NULL,            -- 1 — обход исчерпан, 0 — нет
    coverage_digest TEXT,
    doc_set_digest  TEXT,
    policy_digest   TEXT,                        -- crawl_policy_digest прогона
    observed_how    TEXT NOT NULL CHECK (
        observed_how IN ('live','revalidated','adopted','scheduled')
    ),
    actor           TEXT,                        -- человек/планировщик (adopted/scheduled)
    superseded_by   TEXT REFERENCES capture(capture_id)
);

CREATE INDEX IF NOT EXISTS idx_capture_product
    ON capture(product_id, started_at, capture_id);

CREATE INDEX IF NOT EXISTS idx_seed_product ON seed_set(product_id, state);
CREATE INDEX IF NOT EXISTS idx_doc_product  ON document_version(product_id, status);
CREATE INDEX IF NOT EXISTS idx_snapshot_product ON product_snapshot(product_id, created_at);
CREATE INDEX IF NOT EXISTS idx_cell_measure_product ON cell_measurement(product_id, run_id);
CREATE INDEX IF NOT EXISTS idx_partition_product ON index_partition(package_id, product_id);
-- v6: индексы durable-субстрата (v3) объявлены прямо здесь. Прежде они выполнялись после
-- ``ALTER TABLE``-миграции колонок — то есть существовали только на пути «догнать старую БД».
CREATE INDEX IF NOT EXISTS idx_url_state_product ON url_state(product_id);
CREATE INDEX IF NOT EXISTS idx_chunk_meta_doc ON chunk_meta(doc_version_id, chunk_no);
"""


# Миграций при открытии нет: ``_ADDITIVE_COLUMNS``/``_POST_MIGRATION_DDL``/``_migrate``
# удалены вместе с ``STATE_SCHEMA_VERSION`` 6. Все их колонки и индексы объявлены в
# :data:`STATE_DDL` выше, а файл другой версии отвергается гейтом (:func:`connect_state`)
# **до** применения схемы. Второй путь записи схемы («догнать старую БД при открытии»)
# означал бы, что старая БД объявляет себя новой, не будучи ею (решение владельца
# 01.11.2026: шимы не тащим, отказ видимый).


def connect_state(db_path: str | Path = ":memory:") -> sqlite3.Connection:
    """Открывает ``state.db`` и применяет идемпотентную схему §4.2 — через гейт версии.

    ``row_factory = sqlite3.Row`` — доступ к колонкам по имени (как в
    :func:`core.store.sqlite.connect`). Порядок важен и он один:

    1. версия схемы читается **до** ``STATE_DDL`` (:func:`core.util.schema_gate.declared_schema_version`);
    2. несовпадение — :class:`~core.store.sqlite.SchemaVersionMismatch` с путём файла и
       указанием, что пересоздавать; соединение закрывается, файл **не меняется**;
    3. только затем применяется ``STATE_DDL`` (``IF NOT EXISTS`` — no-op на текущей БД) и
       пишется ``schema_meta.schema_version``.

    Миграций нет ни одной: все колонки и индексы объявлены в :data:`STATE_DDL`. Старая БД
    (версия ≤ 10, либо файл с таблицами без объявленной версии) отвергается — durable-состояние
    пакета не переносится между версиями схемы, а тихий ``ALTER TABLE`` объявлял бы файл
    новой версии, не будучи им.

    ``":memory:"`` — пустая БД: таблиц нет, сверять нечего, схема создаётся с нуля.

    :raises SchemaVersionMismatch: файл объявляет другую ``schema_version`` (или не объявляет
        её вовсе, уже имея таблицы) — пересоздайте ``state.db`` пакета.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        has_tables, found = declared_schema_version(conn)
        if found != STATE_SCHEMA_VERSION and (found is not None or has_tables):
            raise SchemaVersionMismatch(
                schema_mismatch_message(
                    db_path,
                    found=found,
                    expected=STATE_SCHEMA_VERSION,
                    what="09 §4.1",
                    action=(
                        "durable-состояние пакета не переносится между версиями схемы — "
                        "пересоздайте state.db (например, "
                        "``rm <root>/state/<package_id>/state.db``); историю прошлой схемы "
                        "миграция не догоняет, прогон начнёт её заново."
                    ),
                )
            )
        conn.executescript(STATE_DDL)
    except BaseException:
        conn.close()
        raise
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
        (STATE_SCHEMA_VERSION,),
    )
    conn.commit()
    return conn


def state_db_path(root: str | Path, package_id: str) -> Path:
    """Путь БД состояния пакета: ``root / package_id / "state.db"`` (09 §4.1)."""
    return Path(root) / str(package_id) / "state.db"
