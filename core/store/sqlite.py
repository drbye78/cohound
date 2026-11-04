"""coHound — хранилище (store, L3): SQLite DDL и подключение (§4.4).

Соответствует §4.4 регламента. Схема категорийно-нейтральна. Индексы:
``index_store/{package}/{build}/{product_id}/`` — продукт-партиции (I17).

Триггеры обеспечивают инварианты I4/I6/I7/I13/I18 на уровне БД (§2.3). ``StorePort``
(``core/ports/store.py``) — контракт доступа; реализация ``SqliteStore`` появится в W3.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# Код ошибки, класс отказа и текст сообщения гейта версии схемы живут в
# ``core.util.schema_gate``: тот же отказ поднимает DMZ (``core/edge/scheduler.py``,
# ``edge_jobs.db``), а матрица слоёв (``01`` §1) запрещает ``edge → store``. Реэкспорт
# оставляет поверхность ``core.store.sqlite`` прежней и гарантирует **один** класс на все
# три БД: иначе ``except SchemaVersionMismatch`` в composition root ловил бы половину
# отказов, а вторая редакция сообщения разошлась бы с первой.
from core.util.schema_gate import (  # noqa: F401 - реэкспорт: поверхность модуля сохранена
    SCHEMA_VERSION_CODE,
    SchemaVersionMismatch,
    declared_schema_version,
    schema_mismatch_message,
)

#: Версия схемы ``cohound.db``.
#: 3.14.0 — **все** аддитивные колонки, которые прежде досоздавались при открытии
#: (``_ensure_columns``), объявлены прямо в ``DDL``. Шимы удалены: файл другой версии
#: отвергается гейтом (:class:`SchemaVersionMismatch`) до любых миграций, поэтому
#: ``ALTER TABLE ... ADD COLUMN`` в штатном открытии не осталось ни одного. Цена названа
#: прямо: существующие ``runs/*/cohound.db`` версий ≤ 3.13.0 отвергаются — прогон
#: переносится пересозданием (новый каталог/``run_id``), а не тихим догоном схемы.
#:
#: 3.15.0 (F6 плана ``17``, **один** bump на четыре колонки замера и claim):
#: ``crawl_session_items.fetch_started_at``/``fetch_ms`` (момент начала попытки и её
#: длительность — без второго интервал между запросами одного host'а не восстановить, а
#: это критерий приёмки §7.2 №1) и ``extract_queue.claimed_at``/``heartbeat_at``
#: (claim/lease §4.2 п.3–4). Колонка без производителя не заводится: обе пары пишутся
#: тем же коммитом, что и сам факт (``commit_local``/``set_extract_state``). Цена названа:
#: все ранее записанные ``runs/*/cohound.db`` отвергаются гейтом — это ожидаемо.
#:
#: 3.16.0 (L1b-2b плана ``18``, **один** bump на две колонки провенанса врезки):
#: ``corpus_chunks.char_start``/``char_end`` — смещения куска в нормализованном тексте
#: документа (``ExtractedDoc.text``). Без них нельзя проверить, что цитата (I22) стоит в
#: документе там, где ей место, и нельзя пере-срезать кусок, не пере-нарезая документ.
#: Пара пишется тем же коммитом, что и сам чанк (``add_chunks_batch``), и ``NULL`` в обеих
#: колонках — законное состояние: абзацный чанкер точным срезом не является. Шимов нет
#: (``ALTER TABLE`` при открытии запрещены), поэтому файлы ≤ 3.15.0 отвергаются гейтом —
#: прогон переносится пересозданием, а не тихим догоном схемы.
#:
#: 3.17.0 (P4 плана ``19``, **один** bump на колонку идентичности и блок провенанса):
#: ``runs.crawl_policy_digest`` — отпечаток эффективной поверхности решений политики
#: краула (поле ``EXECUTION_CONTRACT_FIELDS``). Без колонки дайджест был в контракте,
#: но не в манифесте, и ``hound diff`` честно печатал «различающееся поле не восстановимо
#: из манифеста»: ось есть, а назвать её нельзя. ``runs.crawl_policy_json`` — провенанс
#: исполнения политики (``refresh_policy``/``frozen_source``/несовпавшие записи/счётчики
#: пропусков/``captured_at``/оси по корням): он **не** входит в ``run_id``, поэтому лежит
#: отдельным блоком, а не второй копией идентичности. Шимов нет: файлы ≤ 3.16.0
#: отвергаются гейтом версии, прогон переносится пересозданием.
SCHEMA_VERSION = "3.17.0"

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;

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
    cohort_digest         TEXT,                        -- 09 §12.5: дайджест состава когорты (слагаемое run_id, поле контракта)
    seeds_digest          TEXT,                        -- 09 §12.5: дайджест seed-набора (слагаемое run_id, поле контракта)
    crawl_policy_digest   TEXT,                        -- план 19 §6.1: отпечаток эффективной поверхности решений политики краула (поле контракта)
    crawl_policy_json     TEXT,                        -- план 19 §P4: провенанс исполнения политики (провенанс, не идентичность)
    focus_text            TEXT,
    focus_modules         TEXT,                        -- JSON array
    package_id            TEXT,
    package_version       TEXT,
    catalog_version       TEXT,
    status_dict_version   TEXT,
    scoring_policy_version TEXT,
    methodology_version   TEXT,
    embed_model_id        TEXT,
    embed_device          TEXT,                        -- §13/R5: место вычисления векторов (cpu|cuda; auto нет)
    index_build_id        TEXT,
    retrieval_profile     TEXT,                        -- R0|R1|R2
    retrieval_params_json TEXT,                        -- fusion-параметры (rrf_k и пр.) — S1
    retrieval_cutoff_K    INTEGER,                     -- I20
    search_effort_budget_json TEXT,                    -- I23
    requested_ranking     TEXT,                        -- запрошенный режим (run.yaml) — K6
    computed_ranking      TEXT,                        -- вычисленный допустимый ранг — K6
    settings_provenance_json TEXT,                     -- A3 (v2.1): {setting: {source, experiment_id?, skipped?}}
    crawl_provenance_json TEXT,                        -- §4.4/Phase 2: провенанс краула (сеансы/движки/дайджест)
    crawl_strategy        TEXT,                        -- §13/Backlog B: ось стратегии краула (execution_change)
    crawl_rate_limit      TEXT,                        -- §13/Backlog B: канон. форма rate_limit (execution_change)
    crawl_config_digest   TEXT,                        -- §13/N6: дайджест разрешённой конфигурации контроля краула (execution_change)
    registry_spec_digest  TEXT,                        -- §13/E2: дайджест package-scoped спецификаций реестров (execution_change)
    execution_contract_digest TEXT,                    -- §13/09 §12.5: отпечаток условий измерения (methodology_change boundary)
    previous_run_id       TEXT,                        -- 09 §12.3: прогон, против которого считался этот (lineage)
    change_class          TEXT,                        -- §13: класс изменения относительно предыдущего прогона
    cycle_id              TEXT,                        -- 09 §2.4: эффективный цикл прогона (материал run_id)
    -- 16 §3.1: провенанс среза прогона. ``--only-product`` меняет смысл измерения и обязан
    -- быть виден в артефакте (09 §13.6), иначе «полный» и «срезочный» прогоны неотличимы.
    run_scope             TEXT,
    product_plan_json     TEXT,                        -- JSON: плановый состав продуктов среза
    manual_artifacts_digest TEXT,                      -- 13-manual-artifacts §3.2: отпечаток объявленных ручных артефактов (execution_change)
    manual_artifacts_json TEXT,                        -- 13-manual-artifacts §3.4: доля ручного (наблюдение, не потолок)
    partial_docs_json     TEXT,                        -- I2/I2c: частичное извлечение числом (наблюдение, не потолок)
    publishability_json   TEXT,                        -- §6/A1: профиль публикуемости (класс + числа, из которых он выведен)
    ranking_epsilon       REAL,                        -- §6/B-1: порог неразличимости ранжирования
    deploy_profile        TEXT,                        -- 07 §7: профиль доверенных зон (R7)
    edge_schema_version   TEXT,                        -- 07 §7: версия контракта шва /v1/* (execution-class)
    edge_capabilities_json TEXT,                       -- 07 §7: версии edge-компонентов gateway (R7)
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
    endpoint_host     TEXT,                            -- V6: хост endpoint'а (без схемы/пути; не секрет)
    max_tokens        INTEGER,                         -- V5: потолок генерации (NULL = дефолт провайдера)
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
    lang             TEXT,
    -- 13-manual-artifacts §3.3: происхождение **байтов** ('crawl'|'manual'). Отказ краула
    -- (crawl_session_items) не стирается: материал наблюдён другим путём — два факта.
    origin           TEXT NOT NULL DEFAULT 'crawl',
    -- Частичное извлечение (I2/I2c): документ получен, но прочитана лишь часть страниц.
    -- Признак обязан пережить границу корпуса — иначе «7 страниц из 2763» неотличимо от
    -- «прочитано всё», и молчание страниц становится ``❌``.
    partial          INTEGER NOT NULL DEFAULT 0,
    partial_reason   TEXT,
    page_count       INTEGER,           -- заявлено документом; NULL — не сообщено
    pages_converted  INTEGER,           -- реально прочитано; NULL — не измерено
    extracted_chars  INTEGER,           -- длина извлечённого текста
    -- 09 §11.2: происхождение наблюдения. Без них 304-документ неотличим от живого 200.
    -- NULL = «наблюдение не записано» (legacy-строка), а не «наблюдение подтверждено».
    http_status      INTEGER,
    -- live|revalidated|reused|stale
    reuse_kind       TEXT
);

-- 13-manual-artifacts §3.3: аудит предоставления. Строка на объявленную подачу, дошедшую
-- до корпуса; факт отказа по этому URL остаётся в crawl_session_items.
CREATE TABLE IF NOT EXISTS manual_artifacts (
    run_id          TEXT NOT NULL,
    product_id      TEXT NOT NULL,
    url             TEXT NOT NULL,
    sha256          TEXT NOT NULL,
    doc_id          TEXT,
    raw_path        TEXT,
    declared_path   TEXT,
    reason          TEXT,
    provided_by     TEXT,
    provided_at     TEXT,
    byte_size       INTEGER,
    created_at      TEXT,
    PRIMARY KEY (run_id, product_id, url),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

-- 16 §6: версия документа → строка корпуса. Производный индекс: ``dv`` уникален
-- (включает ``content_hash`` + версию экстрактора + уровень), а ``doc_id`` — нет
-- (``product_id:content_hash``, без уровня), поэтому «версия на строку» теряла бы версии
-- при совпадении текста на разных уровнях извлечения (ось ``document_level`` — per-site).
-- Хранится потому, что ни одно из направлений не выводится: ``dv`` → ``doc_id`` не
-- вычисляется (``short_hash`` необратим), ``doc_id`` → ``dv`` неоднозначно (версий у
-- документа бывает несколько). Колонкой строки это не выразить — отсюда таблица.
CREATE TABLE IF NOT EXISTS corpus_doc_version (
    doc_version_id TEXT PRIMARY KEY,
    product_id     TEXT NOT NULL,
    doc_id         TEXT NOT NULL
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
    embedded_at     TEXT,
    char_start      INTEGER,                           -- L1b-2b: начало куска в тексте документа
    char_end        INTEGER                            -- конец (исключающий); NULL+NULL — не срез
);

-- K1: пакетная сборка (index_build_id, иммутабельна) × продукт-партиция (I17).
-- Один index_build_id даёт N партиций → составной PK.
CREATE TABLE IF NOT EXISTS index_manifest (
    index_build_id  TEXT NOT NULL,                       -- композиция когорты (09 §8.1)
    partition_build_id TEXT,                             -- сборка продукт-партиции (09 §8.2)
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
    absence_rule      TEXT,                             -- правило отсутствия параметра (§4.2/I2); NULL = не записано
    justification     TEXT,
    rule_note         TEXT,
    validation_notes  TEXT,                             -- JSON-массив: свидетельство о дефектах оценки (ремонт/отказ)
    failure_kind      TEXT,
    model_id          TEXT,
    llm_scenario      TEXT,                             -- §7.4/I21
    prompt_hash       TEXT,
    index_build_id    TEXT,                             -- provenance ячейки (R7/§13)
    retrieval_profile TEXT,
    retrieval_params_json TEXT,
    stale_reason      TEXT,                             -- инвалидация при смене сборки/prompt
    review_reason     TEXT,                             -- аннотация причины pending (не статус)
    evidence_scope    TEXT,                             -- JSON силы отрицания + вектора охвата (§2.3)
    primary_evidence_id TEXT,
    reviewed_by       TEXT,
    -- 09 §10 (схема cell-reuse-2): происхождение ячейки. ``observed`` — вердикт получен
    -- в этом прогоне, ``carried`` — перенесён из ``carried_from_run_id`` по совпадению
    -- ``cell_reuse_key``. Дефолт ``observed`` семантически верен и для строк, записанных
    -- до появления колонки: они действительно были измерены, поэтому backfill не нужен.
    measurement_origin TEXT NOT NULL DEFAULT 'observed',  -- observed|carried
    carried_from_run_id TEXT,                           -- NULL ⟺ measurement_origin=observed
    cell_reuse_key    TEXT,                             -- ключ переноса (cell-reuse-2); NULL = не вычислялся
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
    source_scope      TEXT,                             -- УСТАРЕЛО: скаляр прежних прогонов
    scope_acceptance  TEXT,                             -- принятие риска человеком (было scope_attested_by)
    index_build_id    TEXT,                             -- привязка evidence к сборке (R7)
    partition_build_id TEXT,                            -- партиция чанка-evidence (09 §8.1)
    doc_version_id    TEXT,                             -- O5: адрес версии документа (dv_...); NULL = не записано
    retrieved_at      TEXT
);

-- S9: персистентные решения гейтов G1–G4 (носитель reviewed_by, аудит-след).
-- `subject_digest` привязывает решение к **предмету**: профилю (G1/G2), очереди ревью
-- (G3), Coverage/Capability/артефактам (G4). Пустая строка — legacy-решение без
-- привязки (записано до появления привязки; продолжает проходить гейт).
-- `source`/`external_ref`/`verdict` — интеграция: внешняя система **присылает**
-- решение (approved|rejected), `external_ref` — ссылка обратно в неё.
CREATE TABLE IF NOT EXISTS gate_decisions (
    decision_id       TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL,
    gate              TEXT NOT NULL,                    -- G1|G2|G3|G4
    target            TEXT,                             -- что подтверждено (cohort/cell/...)
    actor             TEXT NOT NULL,                    -- человек/система, принявшая решение
    decided_at        TEXT,
    notes             TEXT,
    subject_digest    TEXT NOT NULL DEFAULT '',         -- отпечаток предмета решения
    source            TEXT NOT NULL DEFAULT 'internal', -- internal|external
    external_ref      TEXT,                             -- id решения во внешней системе
    verdict           TEXT NOT NULL DEFAULT 'approved', -- approved|rejected
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

-- P1 (self-containment): снимок плановых входов прогона (каталог/веса/когорта) в БД.
-- Источник истины для пост-хок команд (sensitivity/golden/report); side-файл
-- plan_inputs.json — лишь производный экспорт. payload_sha256 — детерминированный
-- дайджест канонической JSON-формы (§13/R7, diff-visible).
CREATE TABLE IF NOT EXISTS run_inputs (
    run_id           TEXT PRIMARY KEY,
    snapshot_version INTEGER NOT NULL,
    payload_json     TEXT NOT NULL,
    payload_sha256   TEXT NOT NULL,
    created_at       TEXT NOT NULL
);

-- Lane B (schema 3.0.0; removal — 3.1.0): сеанс краула — ядро-владеемое durable намерение +
-- агрегированный прогресс. Счётчики сеанса поддерживаются транзакционно при записи элемента
-- (см. SqliteStore). Legacy per-URL таблица попыток удалена в 3.1.0 (заменена сеансом краула).
CREATE TABLE IF NOT EXISTS crawl_sessions (
    session_id    TEXT PRIMARY KEY,
    run_id        TEXT,                    -- soft-link на прогон; NULL = standalone-сеанс
    profile       TEXT NOT NULL,
    status        TEXT NOT NULL,           -- created|running|paused|cancelled|completed|partial|interrupted|failed
    created_at    TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    total         INTEGER NOT NULL DEFAULT 0,
    pending       INTEGER NOT NULL DEFAULT 0,
    running       INTEGER NOT NULL DEFAULT 0,
    fetched       INTEGER NOT NULL DEFAULT 0,
    failed        INTEGER NOT NULL DEFAULT 0,
    cancelled     INTEGER NOT NULL DEFAULT 0,
    error         TEXT,
    options_json  TEXT,
    provenance_json TEXT,
    -- F1 (schema 3.12.0): свидетельство живости, а **не** распределённая блокировка (`10` §Д-6,
    -- К-4). ``owner`` — кто исполняет сеанс (host/pid/старт процесса), ``heartbeat_at`` — когда
    -- этот процесс последний раз подавал признаки жизни. SIGKILL/OOM не перехватываются, поэтому
    -- мёртвый сеанс распознаёт recovery-on-open (``core/pipeline/liveness.py``) и переводит его
    -- в ``interrupted``. Обе колонки — операционный провенанс, в ``run_id`` не входят.
    owner         TEXT,
    heartbeat_at  TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS crawl_session_items (
    session_id    TEXT NOT NULL,
    product_id    TEXT NOT NULL,
    url           TEXT NOT NULL,
    status        TEXT NOT NULL,           -- pending|running|fetched|failed|cancelled
    attempts      INTEGER NOT NULL DEFAULT 0,
    depth         INTEGER,
    engine        TEXT,
    kind          TEXT,
    fetch_mode    TEXT,
    extract_route TEXT,
    strategy      TEXT,
    http_status   INTEGER,
    content_type  TEXT,
    lang          TEXT,
    content_hash  TEXT,
    -- 17 §6 (F3): хеш **извлечённого текста** (ключ дедупа корпуса), а не байтов выше.
    -- Носитель канонического адреса: ``min(url)`` среди равных ``doc_hash`` спрашивается у
    -- этой колонки, поэтому победитель дедупа не зависит от порядка готовности страниц.
    -- NULL = «не записано» (строка, созданная до появления колонки), а не «хеша нет».
    doc_hash      TEXT,
    raw_path      TEXT,
    error_kind    TEXT,                    -- FailureKind (валидируется доменной моделью)
    error         TEXT,
    first_seen    TEXT,
    last_attempt  TEXT,
    -- 17 §6 (F6, schema 3.15.0): замер попытки получения. ``fetch_started_at`` — момент
    -- начала **живого** фетча (не ``first_seen``/``last_attempt``: те операционные метки и
    -- перезаписываются), ``fetch_ms`` — длительность этого фетча. Обе пишет fetch-коммит
    -- (``core.pipeline.corpus_ingest.retain`` → ``mark``); ``NULL`` — живого фетча не было
    -- по построению (resume из retained-байтов, отказ до сети) либо запись прежнего
    -- производителя. Интервал между запросами одного host'а из длительностей не
    -- восстанавливается, поэтому момент начала — отдельная колонка, а не вывод из
    -- ``last_attempt``.
    fetch_started_at TEXT,
    fetch_ms      INTEGER,
    PRIMARY KEY (session_id, product_id, url),
    FOREIGN KEY (session_id) REFERENCES crawl_sessions(session_id)
);

-- 17 §4.1: durable передача добытого документа в конверсию. Краул (``crawl run
-- --crawl-only``) записывает факт «документ добыт и лежит в ``corpus_raw``», потребитель
-- конверсии берёт строку (``claimed``) и объявляет готовность (``done``). Это и есть
-- разделение «краул выполнен ≠ корпус собран»: строки пишет **только** краул, состояние
-- меняет **только** потребитель. ``level``/``pdf_pipeline`` — условия измерения корня
-- (без них ``extractor_version`` документа невоспроизводим).
-- Провенанс **получения** (``fetched_at``…``extract_route``) — вторая половина тех же
-- условий: ось ``extract_route`` выбирается по ``fetch_mode``/``content_type``/``lang``,
-- поэтому потребитель обязан получить их из строки, а не угадать по retained-байтам —
-- иначе тот же документ изъят другим маршрутом, ``extractor_version``/``doc_version_id``
-- другие, и корпус меняется при неизменных объявленных входах (I19, A3).
-- Таблица объявлена в ``DDL`` целиком, поэтому переживает открытие без миграций.
CREATE TABLE IF NOT EXISTS extract_queue (
    session_id      TEXT NOT NULL,
    product_id      TEXT NOT NULL,
    url             TEXT NOT NULL,
    content_hash    TEXT NOT NULL,           -- хеш retained-байтов (I19)
    raw_path        TEXT NOT NULL,           -- файл в corpus_raw; живёт до done
    level           TEXT NOT NULL,           -- document_level корня
    pdf_pipeline    TEXT NOT NULL,           -- pdf_pipeline корня
    -- Провенанс получения: те же величины, что ``crawl_session_items`` несёт у себя
    -- (``http_status`` здесь назван ``status_code`` — по имени поля payload'а
    -- ``_ExtractRequest``, чтобы потребитель не переводил имена). NULL = «не записано»
    -- (строка прежнего производителя), а не «сервер не отвечал».
    fetched_at      TEXT,
    fetch_mode      TEXT,                    -- FetchMode (ось получения)
    lang            TEXT,
    status_code     INTEGER,                 -- HTTP-статус получения
    content_type    TEXT,
    engine          TEXT,                    -- движок фетча (http|browser|…)
    extract_route   TEXT,                    -- ExtractionRoute (ось извлечения)
    state           TEXT NOT NULL DEFAULT 'pending',  -- ExtractQueueState
    attempts        INTEGER NOT NULL DEFAULT 0,
    enqueued_at     TEXT NOT NULL,
    state_changed_at TEXT,
    claimed_by      TEXT,                    -- кто взял (провенанс, не блокировка)
    -- 17 §4.2 п.3–4 / §6 F6 (schema 3.15.0): claim — это **аренда**, а не пометка.
    -- ``claimed_at`` — момент взятия строки воркером, ``heartbeat_at`` — последний признак
    -- жизни этого воркера по этой строке. Живой потребитель обязан обновлять heartbeat,
    -- пока строка в работе: иначе долгий документ был бы признан брошенным и сконвертирован
    -- дважды. Просроченная аренда возвращается в ``pending`` с ``attempts += 1`` и видимой
    -- записью (молчаливое воскрешение запрещено — §4.2 п.3).
    claimed_at      TEXT,
    heartbeat_at    TEXT,
    error_kind      TEXT,
    error           TEXT,
    PRIMARY KEY (session_id, product_id, url)
);

-- Ops: «сколько в очереди по состояниям» спрашивается на каждый ``crawl status``.
CREATE INDEX IF NOT EXISTS idx_extract_queue_state
    ON extract_queue(session_id, state);

-- Ф0 (schema 3.8.0): покрытие корпуса продукта по исходам краула — первоклассный факт.
-- Производно от персистентных ``crawl_session_items`` + записей об усечении; гейтит
-- публикацию ``❌`` (I2c). Аддитивная таблица, идемпотентная через CREATE TABLE IF NOT EXISTS.
-- v3.8.0: ``unreadable_origins_json``/``partial_blocks_json`` — origin-уровневый порог.
-- v3.9.0: ``reason`` — причина неполноты/zero-source покрытия (09 §11.3).
CREATE TABLE IF NOT EXISTS corpus_coverage (
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    complete          INTEGER NOT NULL,
    expected_sources  INTEGER NOT NULL DEFAULT 0,
    observed_sources  INTEGER NOT NULL DEFAULT 0,
    blocked_json      TEXT NOT NULL DEFAULT '{}',   -- {"error_kind": count}
    unreadable_origins_json TEXT NOT NULL DEFAULT '[]',  -- ["origin", ...]
    partial_blocks_json     TEXT NOT NULL DEFAULT '{}',  -- {"origin": {"error_kind": count}}
    truncated         INTEGER NOT NULL DEFAULT 0,
    computed_at       TEXT NOT NULL,
    reason            TEXT,
    -- 13-manual-artifacts §3.3: блокирующие отказы, закрытые объявленным ручным артефактом.
    -- Отказ остаётся в blocked_json (аудит); поле меняет только последствие для complete.
    resolved_manually INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, product_id)
);

-- 16 §3.2: статус продукта **в прогоне** — первоклассная строка, а не снимок в ``state.db``
-- (09 §4.3/§6.5) и не текст в ``results[].notes``. Пишется на границе продукта и
-- переписывается в конце фазы (образец — ``corpus_coverage``), поэтому ``run_product`` —
-- носитель величин ``cells_total``/``cells_pending``/``baseline_complete``, а ``notes``
-- остаётся для оператора. ``cells_carried``/``cells_measured`` **не заводятся**: переноса
-- ячеек нет (09 §10), и колонка, всегда равная нулю, — ложь о существующем механизме.
-- ``coverage_complete`` NULL = покрытие не считалось (не «полное»).
CREATE TABLE IF NOT EXISTS run_product (
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    mode              TEXT NOT NULL,            -- new|full|incremental
    status            TEXT NOT NULL,            -- pending|running|complete|partial|failed|blocked
    scope             TEXT NOT NULL DEFAULT 'full',
    phase             TEXT,
    coverage_complete INTEGER,
    docs_total        INTEGER NOT NULL DEFAULT 0,
    docs_reused       INTEGER NOT NULL DEFAULT 0,
    docs_new          INTEGER NOT NULL DEFAULT 0,
    cells_total       INTEGER NOT NULL DEFAULT 0,
    cells_pending     INTEGER NOT NULL DEFAULT 0,
    baseline_complete INTEGER NOT NULL DEFAULT 0,
    error_kind        TEXT,
    error             TEXT,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (run_id, product_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

-- B3 (schema 3.10.0): члены контейнеров — **производный** вид ретейленного архива (§6.7).
-- Байты членов отдельно не хранятся: архив — сырьё (I19), член ре-выводится из него тем
-- же ``open_member`` по ``archive_path``. Идентичность — ``(archive_content_hash,
-- member_path, product_id)``; ``member_path`` нормализован
-- (container_policy.normalize_member_path). ``run_id`` в ключе не нужен: БД и так per-run
-- (``runs/{run_id}/cohound.db``), а ``product_id`` — **нужное** измерение: один и тот же
-- архив в двух продуктах одного прогона иначе перезаписывал бы ``run_id``/``root_url``/
-- ``doc_id`` (провенанс члена врал бы молча; 3.13.0).
-- ``skip_reason`` заполнен ⟺ член не ингестнут (``doc_id`` NULL) — видимый пропуск.
CREATE TABLE IF NOT EXISTS corpus_members (
    archive_content_hash  TEXT NOT NULL,
    member_path           TEXT NOT NULL,
    product_id            TEXT NOT NULL,
    run_id                TEXT,
    root_url              TEXT,                       -- корень спуска
    archive_url           TEXT,
    archive_path          TEXT,                       -- ретейленный архив (corpus_raw)
    member_url            TEXT,                       -- {archive_url}!/{member_path}
    member_format         TEXT,                       -- суффикс без точки / формат архива
    declared_size         INTEGER,                    -- заявленный размер из индекса
    extracted_content_hash TEXT,                      -- хеш извлечённого текста (не байтов)
    doc_id                TEXT,                       -- corpus_docs.doc_id, если ингестнут
    skip_reason           TEXT,                       -- причина, если не ингестнут
    depth                 INTEGER NOT NULL DEFAULT 0, -- уровень вложенности (0 — корень)
    created_at            TEXT,
    PRIMARY KEY (archive_content_hash, member_path, product_id)
);

CREATE INDEX IF NOT EXISTS idx_corpus_members_product ON corpus_members(product_id);
CREATE INDEX IF NOT EXISTS idx_corpus_members_doc     ON corpus_members(doc_id);
-- 16 §6: отображение версии в документ спрашивается по паре (продукт, документ) — например
-- при резолве ``source_id``, где второй сегмент несёт версию.
CREATE INDEX IF NOT EXISTS idx_corpus_doc_version_doc
    ON corpus_doc_version(product_id, doc_id);

-- B3 (schema 3.10.0): наблюдения спуска — прогноз (до извлечения) + исход.
-- Запись существует для **каждого** контейнера, включая пустой и нечитаемый: «спуск
-- ничего не дал» обязан быть так же виден, как «спуск дал десять документов».
CREATE TABLE IF NOT EXISTS archive_observations (
    observation_id        TEXT PRIMARY KEY,           -- детерминированный (run|product|root|hash|depth)
    run_id                TEXT,
    product_id            TEXT NOT NULL,
    root_url              TEXT NOT NULL,
    archive_url           TEXT NOT NULL,
    archive_content_hash  TEXT NOT NULL,
    archive_path          TEXT,
    depth                 INTEGER NOT NULL DEFAULT 0,
    descent               TEXT NOT NULL,              -- plan|full
    status                TEXT NOT NULL,              -- ArchiveDescentStatus
    forecast_json         TEXT NOT NULL DEFAULT '{}',
    outcome_json          TEXT NOT NULL DEFAULT '{}',
    reason                TEXT,
    created_at            TEXT
);

CREATE INDEX IF NOT EXISTS idx_archive_observations_run ON archive_observations(run_id);

CREATE INDEX IF NOT EXISTS idx_cells_run_state  ON cells(run_id, state);CREATE INDEX IF NOT EXISTS idx_chunks_product   ON corpus_chunks(product_id);
CREATE INDEX IF NOT EXISTS idx_chunks_build     ON corpus_chunks(build_id);
CREATE INDEX IF NOT EXISTS idx_log_run_phase    ON search_log(run_id, phase);
CREATE INDEX IF NOT EXISTS idx_evidence_run_cell ON cell_evidence(run_id, product_id, param_id);
CREATE INDEX IF NOT EXISTS idx_gate_run         ON gate_decisions(run_id, gate);
CREATE INDEX IF NOT EXISTS idx_crawl_sessions_run    ON crawl_sessions(run_id, status);
CREATE INDEX IF NOT EXISTS idx_crawl_items_session   ON crawl_session_items(session_id, status);
CREATE INDEX IF NOT EXISTS idx_crawl_items_product   ON crawl_session_items(session_id, product_id);
CREATE INDEX IF NOT EXISTS idx_corpus_coverage_run   ON corpus_coverage(run_id);

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
-- Исключение — absence-evidence (§4.2): у протокола отсутствия источника нет по
-- построению (цитировать нечего), поэтому его источник — `evidence_type='absence'`, а
-- NULL в source_id честнее выдуманного нерезолвимого id. Триггер пересоздаётся явно:
-- `CREATE TRIGGER IF NOT EXISTS` оставил бы старую (строгую) редакцию в уже
-- существующих БД прогонов, и запись отсутствия падала бы там, где код её допускает.
DROP TRIGGER IF EXISTS trg_evidence_source_id;
CREATE TRIGGER trg_evidence_source_id
BEFORE INSERT ON cell_evidence
FOR EACH ROW
WHEN NEW.source_id IS NULL AND NEW.evidence_type IS NOT 'absence'
BEGIN
    SELECT RAISE(ABORT, 'I10/I15: evidence обязан иметь source_id для системного резолва');
END;
"""


# ``_ensure_columns`` (``ALTER TABLE ... ADD COLUMN`` при открытии) удалён вместе с
# ``_migrate_scope_acceptance`` и ``SCHEMA_VERSION`` 3.14.0: все аддитивные колонки
# объявлены в ``DDL`` выше, а файл другой версии отвергается гейтом (:func:`connect`)
# **до** любых миграций (``11`` §2.6). Держать второй, никем не проверяемый путь записи
# схемы — значит иметь способ тихо «догнать» старую БД и объявить её новой; владелец
# запретил именно это. Историческая подпись ``scope_attested_by`` → ``scope_acceptance``
# больше не копируется: копировать её некуда и не для кого — БД, где она лежит, отвергается
# целиком по версии, а не открывается частично читаемой.


# ``SchemaVersionMismatch``/``schema_mismatch_message``/``SCHEMA_VERSION_CODE`` объявлены
# в ``core.util.schema_gate`` (реэкспорт — в шапке модуля): тот же отказ поднимает DMZ, а
# ``edge → store`` запрещён матрицей слоёв. Один класс и один формат сообщения на три БД.


def _reject_foreign_schema(
    db_path: str | Path, has_tables: bool, found: str | None
) -> None:
    """Гейт версии схемы: несовпадение — отказ с сообщением о действии оператора."""
    if found == SCHEMA_VERSION:
        return
    if found is None and not has_tables:
        return
    raise SchemaVersionMismatch(
        schema_mismatch_message(
            db_path,
            found=found,
            expected=SCHEMA_VERSION,
            what="11 §2.6",
            action=(
                "прогон переносится только пересозданием — запустите прогон в новом "
                "каталоге (новый run_id) или удалите устаревший cohound.db."
            ),
        )
    )


class StoreNotInitialized(FileNotFoundError):
    """Read-only открытие: файла БД нет.

    Читатель не создаёт БД: пустая схема неотличима от прогона, который ничего не нашёл
    (§4.7 ``12-cli-architecture-review.md``). CLI отображает это в ``EXIT_NOT_FOUND`` (4),
    а не в «ошибку вызова».

    ``code`` — код правила для ``CODE: message`` в CLI.
    """

    #: Код ошибки для ``CODE: message``.
    code = "not_found"


def connect_read_only(db_path: str | Path = ":memory:") -> sqlite3.Connection:
    """Открывает **существующую** БД только на чтение: без каталога, DDL и миграций.

    Отличие от :func:`connect`: тот создаёт файл, каталог и применяет ``DDL`` — поэтому
    ``hound review --db /tmp/ghost/x.db`` оставлял на диске полноценную схему на 233 472
    байта и «прогон, который ничего не нашёл» становился неотличим от опечатки в пути.
    Здесь файл только открывается.

    **WAL и ``mode=ro``.** Прогон живёт в ``journal_mode=WAL``, то есть рядом с
    ``cohound.db`` лежат ``-wal`` и ``-shm``. Read-only соединение с WAL-БД читает
    **закоммиченные** данные живого писателя
    (``tests/store/test_read_only_connect.py::test_read_only_sees_live_writer_wal``): SQLite
    подключается к уже существующему ``-shm``, поэтому блокировок на запись не требует.
    Одна оговорка, которую нельзя обойти настройкой: если ``-shm`` нет (писатель
    завершился, оставив ``-wal``), SQLite **создаёт** ``-shm`` при первом открытии —
    значит, каталог прогона должен быть доступен на запись. Это восстановление
    WAL-индекса, а не мутация прогона: ``cohound.db`` и его данные не меняются. Если
    каталог read-only, SQLite вернёт ошибку открытия — она видима, а не проглочена.

    Гейт версии схемы сохраняется: файл чужой версии отвергается
    :class:`SchemaVersionMismatch` до любого чтения данных (``11`` §2.6).

    :param db_path: путь к существующему файлу БД либо ``":memory:"`` (последний
        создаётся в памяти процессом — на диске ничего не появляется).
    :raises StoreNotInitialized: файла нет (читатель его не создаёт).
    :raises SchemaVersionMismatch: файл объявляет другую ``schema_version``.
    """
    if str(db_path) == ":memory:":
        # Файла нет и не будет: закрытая схема в памяти не создаёт артефакта на диске.
        return connect(db_path)
    path = Path(db_path)
    if not path.exists():
        raise StoreNotInitialized(
            f"БД прогона не найдена: {path} — read-only команда файл не создаёт "
            "(проверьте run_id/--runs-dir/--db)"
        )
    conn = sqlite3.connect(f"file:{path.absolute().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        # ``busy_timeout`` — не запись, а ожидание чужой блокировки: живой писатель держит
        # WAL недолго, и без таймаута читатель получал бы мгновенный ``database is locked``.
        conn.execute("PRAGMA busy_timeout = 5000")
        _reject_foreign_schema(path, *declared_schema_version(conn))
    except BaseException:
        conn.close()
        raise
    return conn


def connect(db_path: str | Path = ":memory:") -> sqlite3.Connection:
    """Открывает БД и применяет схему. Миграций колонок при открытии **нет**.

    Создаёт файл и схему (каталог — забота вызывающего): это путь **писателя**. Читателю нужен
    :func:`connect_read_only` — иначе опечатка в пути оставит на диске пустую БД.

    Все колонки объявлены в :data:`DDL` для текущей :data:`SCHEMA_VERSION`; файл другой
    версии отвергается гейтом (:func:`_reject_foreign_schema`) до применения схемы. Поэтому
    «догнать» старую БД при открытии нечем и не нужно: аддитивная дельта, дописанная тихо,
    сделала бы строку прошлой схемы неотличимой от текущей.

    :raises SchemaVersionMismatch: файл объявляет другую ``schema_version`` (гейт ``11`` §2.6).
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _reject_foreign_schema(db_path, *declared_schema_version(conn))
    except SchemaVersionMismatch:
        conn.close()
        raise
    conn.executescript(DDL)
    # Миграций при открытии нет: гейт версии выше отвергает файл другой версии **до**
    # любого чтения (``11`` §2.6), поэтому все колонки обязаны быть объявлены в ``DDL``,
    # а ``ALTER TABLE ... ADD COLUMN`` в штатном открытии не осталось ни одного.
    # Решение владельца 01.11.2026: шимы не тащим, старая БД отвергается видимо.
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()
    return conn
