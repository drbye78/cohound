# coHound — архитектура: Итерация 3 — данные, конфигурация, CLI

**Документ:** `docs/architecture/03-data-config-cli.md`
**Статус:** принято (ревью-гейт @oracle пройден 2026-10-08)
**Опирается на:** `00-overview.md`, `01-modules-and-contracts.md`, `02-subsystems.md`
**Источник истины:** `cohound-spec-v2.md`
**Смежное:** `05-dependency-stack.md` (стек/пины), `06-implementation-strategy.md` (план).

Детальный дизайн: (A) раскладка на диске, (B) операционное хранилище и транзакции,
(C) версионирование, (D) конфигурация и входы, (E) CLI и UX, (F) миграция текущего кода.

---

## A. Раскладка на диске (per run и глобально)

### A.1. Глобальные каталоги

```
repo/
  cohound-spec-v2.md
  pyproject.toml             # ядро + extras (стек/пины — 05-dependency-stack.md)
  uv.lock                    # зафиксированный резолв для воспроизводимости
  core/                      # пакет (см. итерацию 1)
    cli/                     # composition root Z0 (ядро)
    edge/                    # composition root Z1/DMZ (веб-адаптеры за gateway) — 07
  config/
    deploy.yaml              # профиль развёртывания (local|split), endpoints, allowlist (07)
    paths.example.yaml       # пример раскладки операционных каталогов (COHOUND_HOME/COHOUND_*_DIR)
  packages/{case_id}/
    package.yaml             # §4.2
    catalog.csv              # 11 колонок
    baselines.csv
    cohort.yaml              # §1.6
    golden.csv
    adversarial/
    domain_profiles.yaml
    registries/{registry_id}.yaml   # package-scoped generic-конфиг фетчеров (§D.4)
  passports/{product_id}/{YYYY-MM-DD}.yaml
  docs/status-dictionary.yaml
  docs/architecture/*.md
  runs/{run_id}/             # пакет прогона (см. A.2)
  index_store/{package}/{build}/{product_id}/    # иммутабельные сборки (I17)
  corpus_raw/{product_id}/{doc_hash}.bin         # retention сырых байтов
  cache/serp/{query_hash}.json                   # глобальный content-addressed кэш SERP (R7, ADR 0016)
  cache/http/{url_hash}.bin                      # глобальный content-addressed кэш HTTP
  reports/{run_id}/          # (опц.) публикуемые артефакты вне runs/
  tests/
```

### A.2. Пакет прогона `runs/{run_id}/`

```
runs/{run_id}/
  run.yaml                   # вход (копия, иммутабельна)
  run_manifest.yaml          # §9: все версии, модели, index_build, профиль+K, бюджет
  cohound.db                 # SQLite (операционные данные прогона) + WAL
  plan_inputs.json           # производный экспорт снимка плановых входов (источник истины — run_inputs в cohound.db)
  logs/{phase}.jsonl         # структурированные логи по фазам
  prompts/{prompt_hash}.json # скомпилированные промпты (для аудита/воспроизводимости)
  outputs/
    battle_card.xlsx
    preliminary_findings.xlsx
    nda_questions.csv
    diff.md
```

**Правила:** `run.yaml`/`run_manifest.yaml` иммутабельны после завершения; `cohound.db` несёт всё
операционное состояние; артефакты `outputs/` перезаписываемы (регенерация из БД). Кэш внешних
вызовов — **глобальный content-addressed** (`cache/serp/{query_hash}.json`, `cache/http/{url_hash}.bin`
в корне репозитория), а **не** внутри `runs/{run_id}/cache/`: иначе R7 между разными `run_id`
недостижим. Второй прогон читает тот же кэш по хэшу ключа, без живого ре-фетча (K4, ADR 0016).

---

## B. Операционное хранилище и транзакции

### B.1. Контракт `ports/store.py` → `store/sqlite.py`

Схема §4.4 перенесена из бывшего `core/db.py` (удалён в T1.3) в `store/sqlite.py`
(15 таблиц + `schema_meta`, индексы, триггеры I4/I6/I7/I13/I18). Константа `SCHEMA_VERSION`
= **`3.5.0`** (E2: дайджест спецификаций реестров; D3/D4/сеансы/провенанс краула/P1-самодостаточность) — версия **SQLite-схемы**, не регламента; колонки
`runs.settings_provenance_json` (v2.1/A3), `runs.crawl_provenance_json` (v3.2/Phase 2),
`runs.crawl_strategy` / `runs.crawl_rate_limit` (v3.3/Backlog B: операционные оси краула,
границы `execution_change`, §13), `runs.registry_spec_digest` (v3.5/E2: дайджест package-scoped
спецификаций реестров — граница `execution_change`, §13), `runs.ranking_epsilon` / `runs.deploy_profile` /
`runs.edge_schema_version` / `runs.edge_capabilities_json` (v3.4/P1: порог ранжирования §6/B-1
и деплой-провенанс манифеста, 07 §7) и
`corpus_chunks.extractor_version` /
`index_manifest.extractor_version` / `index_manifest.content_digest` (P2) хранят провенанс настроек,
провенанс краула и build-id witness, а таблицы `crawl_sessions`/`crawl_session_items` — первоклассный сеанс краула
(id/статус/прогресс/возобновляемость; legacy `crawl_attempts` удалена в v3.1.0); таблица `run_inputs`
(снимок плановых входов: `catalog`/веса/когорта) делает `runs/{run_id}` самодостаточным — источник
истины пост-хок команд, а `plan_inputs.json` — лишь производный экспорт (P1). Миграция
additive/idempotent (`ALTER TABLE ... ADD COLUMN`/`CREATE TABLE IF NOT EXISTS` при отсутствии).
`index_build_id` — **контент-адресуемый** отпечаток батча (два прохода: ingest → финализация id
→ index; см. §B.1 `02-subsystems.md`). Методы доступа (реализация `StorePort`):

```python
class SqliteStore:
    # запись
    def upsert_products(...); def create_run(cfg, manifest)
    def set_phase_state(run_id, phase, state)
    def save_cell(cell: Cell)                    # upsert по (run_id, product_id, param_id)
    def save_cells_batch(cells: list[Cell])      # одна транзакция на батч
    def add_evidence(ev: CellEvidence); def add_evidence_batch(evs)
    def log_search(entry: SearchLogEntry)
    def save_exhaustion(pe: ProtocolExhaustion)
    def save_gate_decision(d: GateDecision)        # S9: gate_decisions
    def save_registry_fact(rf: RegistryFact)
    def save_llm_scenarios(bindings: list[ScenarioBinding])
    def persist_manifest(run_id, manifest: RunManifest)   # версии/индекс/retrieval + settings_provenance_json
    def save_plan_inputs(run_id, *, payload, payload_sha256)  # run_inputs: снимок плановых входов (P1)
    def upsert_crawl_session(session: CrawlSession)       # сеанс краула: durable намерение + прогресс
    def upsert_crawl_session_item(item: CrawlSessionItem) # per-URL элемент; счётчики сеанса — в той же транзакции
    def update_crawl_session_status(session_id, status, *, ...)  # жизненный цикл сеанса
    def recover_crawl_session(session_id) -> int          # running → pending при рестарте
    def clear_crawl_session(session_id)                   # сброс при --reset
    # чтение
    def iter_cells(run_id, state=None) -> Iterator[Cell]
    def iter_exhaustions(run_id) -> Iterator[ProtocolExhaustion]    # I3/§5: протоколы исчерпания
    def get_chunk(chunk_id) -> Chunk            # для валидации цитат
    def resolve_source_url(source_id) -> str    # I10/I15
    def read_manifest(run_id) -> RunManifest    # forward-compatible: settings_provenance/ranking_epsilon
    def read_plan_inputs(run_id) -> dict | None # снимок плановых входов из run_inputs (P1); None если нет
    def iter_gate_decisions(run_id) -> Iterator[GateDecision]   # S9
    def get_crawl_session(session_id) -> CrawlSession | None
    def iter_crawl_session_items(session_id, *, product_id=None, status=None) -> Iterator[CrawlSessionItem]
    def crawl_session_counts(session_id) -> dict[str, int]
```

### B.2. Транзакции и производительность

- **WAL** включён (уже в DDL); читатели не блокируют писателя.
- **Батчевые записи:** ячейки Ш6 пишутся пачками (`save_cells_batch`), одна транзакция на N
  (по умолчанию 200) ячеек → снижает fsync-нагрузку.
- **Единая БД (S6):** корпус и операционные данные живут в **одной** SQLite-БД прогона
  (`runs/{run_id}/cohound.db`). Вариант «отдельная корпусная БД» **отвергнут**: при разнесении
  таблиц SQLite не может обеспечить FK между `cell_evidence.source_id` и `corpus_chunks.chunk_id`,
  что ломает provenance (I10/I15/I19). Объём корпуса — не основание менять модель данных;
  триггеры масштабирования — §14, а не разделение БД.
- **Индексы:** `idx_cells_run_state`, `idx_chunks_product`, `idx_log_run_phase`,
  `idx_evidence_run_cell` (уже в DDL).

### B.3. Гранулярность транзакции по фазам

| Фаза | Единица транзакции | Идемпотентность |
|---|---|---|
| Ш3/Ш4 | батч ячеек (200) | upsert по PK |
| Ш5 | документ (doc + его чанки) | `content_hash` dedup |
| Ш6 | батч ячеек + evidence | upsert + evidence по `evidence_id` |
| Ш8/Ш9 | одна | перезапись сводки |

---

## C. Версионирование (§13)

### C.1. Версии и где живут

| Версия | Носитель | Влияет на |
|---|---|---|
| `methodology_version` | run_manifest | всё; major запрещает межцикловый diff |
| `package_version` | package.yaml | когорта, роли, веса |
| `catalog_version` | catalog.csv (git) | параметры, rule, absence_rule |
| `status_dictionary_version` | docs/status-dictionary.yaml | перечисления |
| `scoring_policy_version` | run_manifest | расчёт |
| `index_build_id` | index_manifest | ретривал |
| `embed_model_id`, `chunker_version`, `extractor_version`, `lexical_engine_version` | index_manifest | сборка индекса |
| `retrieval_profile`, `retrieval_cutoff_K` | run_manifest | ретривал (I16, I20) |
| `retrieval_params_json` (fusion, вкл. `rrf_k`) | run_manifest | ретривал (S1) |
| `requested_ranking` / `computed_ranking` | run_manifest | ранжирование (K6) |
| `ranking_epsilon` | run_manifest | порог неразличимости ранжирования (§6/B-1, K6) |
| `settings_provenance` | run_manifest | провенанс настроек: `default`\|`measured` (+ `experiment_id`) (A3) |
| `index_build_id`, `retrieval_profile` (per cell) | cells | provenance ячейки (R15) |
| `prompt_hash` (per cell) | cells | воспроизводимость ячейки |
| `stale_reason` (per cell) | cells | инвалидация при смене сборки/prompt-шаблона |

**Разделение версий (§13/B-7, v2.1; уточнено методологией 2.3.0 и 2.4.0).** Межцикловый diff
классифицирует причины изменения манифеста по **классам полей**: `methodology_change` — смена
`methodology_version` (любой, не только major) либо поля `METHODOLOGY_FIELDS`
(`scoring_policy_version`, `status_dict_version`, `capability_policy`,
`unknown_policy`, `partial_score`); `execution_change` — расхождение
**`execution_contract_digest`** (отпечаток условий измерения, `core/pipeline/digests.py`).
`EXECUTION_FIELDS` (`case_id`, `cohort_ref`, `catalog_version`, `index_build_id`, `embed_model_id`,
`chunker_version`, `extractor_version`, `lexical_engine_version`, `vector_store`,
`retrieval_profile`, `retrieval_cutoff_K`, `retrieval_params_json`, `llm_scenarios`,
`crawl_strategy`, `crawl_rate_limit`, `crawl_max_workers`, `crawl_max_depth`,
`crawl_max_documents`, `crawl_exclude_paths`, `crawl_exclude_documents`,
`crawl_config_digest`, `registry_spec_digest`) остаётся перечнем полей для **legacy-пар**
(у одного из манифестов контракта нет) и для аудита «что именно исполнялось»; контракт —
его подмножество **без контент-производных** id (`index_build_id`), потому что контент
меняется каждый цикл и условием измерения не является. Последние —
операционные оси краула (`run.yaml:crawl`, commodity-слой A2): их смена — `execution_change`,
не `methodology_change` (Backlog B, D1/D2).
`crawl_config_digest` (N6) — стабильный хеш **разрешённой** конфигурации контроля краула
(цепочка app < run < per-site, по каждому site-key + `__default__`): ловит смену на
**app-/per-site-слое**, которую индивидуальные `crawl_max_*` (только run-слой) не видят.
`DIFF_BOUNDARY_FIELDS` сохранён алиасом `EXECUTION_FIELDS` (обратная совместимость).

**Состав контракта (методология 2.4.0).** `execution_contract_digest` = отпечаток
**декларированных** осей (`core/cli/execution_axes.py`): `methodology_version`,
`scoring_policy_version`, `status_dict_version`, `catalog_version`, `package_version`,
`cohort_digest`, `seeds_digest`, `llm_scenarios`, ретривал-профиль/`cutoff_K`/
`retrieval_params_json`, `chunker_version`, `extractor_stack`, оси краула
(`crawl_strategy`/`crawl_rate_limit`/`crawl_config_digest`), `registry_spec_digest`,
`deploy_profile`, `edge_schema_version`. Наблюдаемые величины
(`extractor_version`/`embed_model_id`/`lexical_engine_version`/`vector_store`) **в
контракт не входят**: они не выводятся из входа (per-product композит корпуса, диск,
wheel'ы), поэтому хеширование сделало бы `run_id` хост-зависимым (R7 §11.7). Вместо
`extractor_version` объявлен `extractor_stack` (стек извлечения + версия библиотеки
docling). **Принятое ограничение, названное прямо:** смена этих четырёх величин **не
блокирует** межцикловый diff — она видна только через `pb`/`index_build_id` и попадает в
разрешённый класс `content_change`. Альтернатива (контракт = `hash(declared)` + отдельная
блокирующая именованная строка для наблюдаемых) — в §14 регламента с триггером возврата.

**`content_change` (методология 2.3.0).** `index_build_id`/`composition_id` — следствие
контента, а не условие измерения, поэтому в контракт не входит: при равных контрактах его
расхождение — **разрешённый** diff класса `content_change` с баннером и per-product
doc-delta (`new`/`reused`/`removed`). Иначе каждый недельный update запрещал бы ровно тот
diff, ради которого делается инкрементальность. Смена состава когорты остаётся запрещающей
и помечается отдельно (`cohort_expansion`/`cohort_reduction`). Legacy-пары, у которых
контракта нет, сравниваются по перечню `EXECUTION_FIELDS` как раньше — прошлые прогоны не
становятся задним числом сравнимыми. Дифф отказывает при обеих классах причин (§13).

### C.2. Версионные границы (bump + golden-full)

- Смена модели под сценарий (I21) → bump `methodology_version`, обязателен `golden-full`.
- Смена когорты после G1 → новый цикл (межцикловый diff запрещён).
- Смена `rule`/`absence_rule` параметра → bump `catalog_version` + golden.
- Смена контента/входов сборки (`chunker`/`embedder`/`extractor`/`lexical`/`vector_store`/
  `snapshot_date`) → новая контент-адресуемая `index_build_id` + golden-full. Смена
  `embedder`/`extractor`/`lexical`/`vector_store` — **не** граница diff (2.4.0): наблюдаемые
  величины не входят в контракт, diff проходит как `content_change`; `chunker_version` и
  `extractor_stack` — входят и блокируют.
- Смена `retrieval_profile`/K и fusion-параметров (`rrf_k`, `retrieval_params_json`) →
  retrieval-golden и `execution_change` в diff, но **не** меняет `index_build_id`:
  ретривал-конфиг не влияет на чанкинг/векторы (P2).
- Смена операционных осей краула (`crawl.strategy`/`crawl.rate_limit`/`crawl.max_depth`/
  `crawl.max_documents`/`crawl.exclude_paths`/`crawl.exclude_documents`/`crawl.max_workers`/
  `crawl.robots`) → `execution_change` в diff (§13, Backlog B/D1/N6), без bump
  `methodology_version`: commodity-слой exec-условий (A2). Оси ловятся и на app-/per-site-слое
  через `crawl_config_digest` (N6).
- Смена prompt-шаблона → новый `prompt_hash`; ячейки с прошлым hash помечаются `stale_reason='prompt_template'`.
- Смена `index_build_id` → затронутые ячейки помечаются `stale_reason='new_build'`; `stale`-ячейки
  не участвуют в расчёте до переоценки/ревью.

### C.3. Slug-иммутабельность

`param_id`, `product_id`, `role_id`, `module_id` — иммутабельны. Отмена — `deprecated: true` +
`replaced_by: <new_id>`. Линтер отклоняет переиспользование slug.

---

## D. Конфигурация и входы

### D.1. run.yaml (§4.5)

```yaml
case_id: pg-core
anchor: pangolin
cohort: cohort-pg-2026q2
products: [pangolin, alt1, alt2, ...]
package: pg-core
focus: { modules: [storage, backup], params: [pg_backup_pitr] }
languages: [ru, en]
snapshot_date: 2026-06-01
mode: strict
ranking_policy: capability_ranked
ranking_epsilon: 0.0            # §6/B-1: порог неразличимости ранжирования (доля диапазона)
settings_provenance:            # A3: default|measured (+ experiment_id при measured, + skipped)
  partial_score: {source: default}
crawl:                          # §3.2/D4: дефолты оси стратегии краула и politeness-пейсинга
  strategy: auto                # auto | honest | hybrid (как используется браузер)
  rate_limit: {min_s: 1.0, max_s: 3.0}   # опц.: uniform(min_s, max_s) между запросами (per-host)
  max_workers: 4                # опц. (>0): D4-пул потоков на сетевой fetch (None/1 = inline)
  extract_workers: null         # опц. (>0): P4-пул экстракции (local — процессы, split —
                                # потоки; None/1 = inline). Память: W × footprint(level)
  document_level: full          # опц.: P5-уровень извлечения документов
                                # (off|text|layout|layout_tables|full; дефолт full)
  max_depth: 2                  # опц. (>=0): глубина BFS-обхода in-site ссылок (0 — только seeds)
  max_documents: 500            # опц. (>0): потолок добытых страниц на (продукт, seed-root)
  exclude_paths: ["/private/*"] # опц.: glob блэклиста по пути URL (fnmatchcase)
  exclude_documents: ["*.pdf"]  # опц.: glob блэклиста по basename URL
```

Загрузчик `config/run_config.py` → `RunConfig` (домен) с валидацией (anchor ∈ products;
preliminary не совместим с capability_ranked — I13). `ranking_policy` — **запрошенный** режим
отображения (K6); вычисленный ранг определяется sensitivity (join `requested × computed = min`,
02 E.1). `ranking_epsilon` — калибруемый порог неразличимости пар (§6/B-1; `None` → 0.0).
`settings_provenance` фиксирует, какие настройки — дефолт, а какие получены **измерением**
(`source: default|measured`), — это делает «дефолт по предвидению» явным исключением, а не нормой
(A3). `products` — **проекция** `cohort.yaml` (один канонический источник состава);
линтер проверяет совпадение множеств `products` и `cohort.product_id` (S11).

Секция `crawl` — дефолты **оси стратегии краула** `CrawlStrategy` (`auto`|`honest`|`hybrid`:
*как* используется браузер при browser-capable адаптере) и per-host диапазона задержек
`RateLimitSpec` (`uniform(min_s, max_s)`, `min_s <= max_s`). При отсутствии секции поведение
неизменно (`auto`, без пейсинга); резолв — `pipeline/fetch_plan.py` (дефолт прогона →
профиль домена → явный `--render-modes`).

Секция `crawl` несёт также **оси контроля краула** (`max_workers`/`extract_workers`/
`max_depth`/`max_documents`/`exclude_paths`/`exclude_documents`/`robots`), разрешаемые
**отдельной** цепочкой `deploy.yaml:crawl` (application default) < `run.yaml:crawl`
(run profile) < `domain_profiles.yaml` (per-site) —
`pipeline/crawl_frontier.py:resolve_crawl_limits`. Это **не** оси `crawl_sources.yaml` и
**не** `FetchPlan`: отдельный канал, чтобы не сломать приоритет source-routing. Две
**независимые** оси конкурентности: `max_workers` — пул потоков на **сетевой fetch**
(локальный путь; «у одной оси не может быть двух смыслов», A2/P3), а `extract_workers`
(P4) — пул **экстракции** (процессы на `local`, потоки на `split`): «в полёте» не более
`W` экстракций, доводка строго FIFO; байты страниц через границу процессов не пиклятся —
воркер перечитывает retained-`raw_path` (I19). Обе оси — `int | None`: `None`/`1` ⇒ пул
не создаётся, исполнение inline (байт-в-байт сегодняшнее). `max_depth` дефолт `0` (только
seeds); BFS-обход идёт и локальным путём, и через executor/split (A2/R2, с предохранителем
раундов `round_budget` под executor). `max_documents=0` отвергается на загрузке (`>0`).
Блэклисты — glob (`fnmatchcase`, case-sensitive): `exclude_paths` по пути URL (паттерн
принимается и с ведущим `/`, и без — `docs/*` ≡ `/docs/*`, N5), `exclude_documents` по
basename пути (`*.pdf`); применяются и к seeds, и к обнаруженным ссылкам. **Caveat
`exclude_documents`:** имя файла, переданное только в query-строке
(`/download?file=report.pdf`), basename не является и паттерном не ловится.

**`document_level` (P5) — уровень извлечения документов.** Ось `crawl.document_level`
(`off`|`text`|`layout`|`layout_tables`|`full`) едет **той же** цепочкой
`app < run < site` (`resolve_crawl_limits`) и резолвится **per seed-root** (один корень —
один уровень). Применяется только к документным форматам (PDF/Office Open XML); HTML не
затрагивается ни при каком значении. **Дефолт `full`** = сегодняшнее эффективное поведение
(Heron layout + таблицы TableFormer `ACCURATE` + cell-matching + **авто-OCR**;
`images_scale=1.0`). Лестница: `text` — моделе-независимый `NativePdfPipeline` (текст +
координаты; без reading order/заголовков/таблиц); `layout` — только layout-модель (без
таблиц и OCR); `layout_tables` — layout + `TableFormer FAST`, **OCR off**; `full` — OCR on
(OCR включён только на верхнем уровне — distinguishes `full`). Уровень входит в
`extractor_version` (`docling-doc-3:<level>`) и потому в `index_build_id`: смена уровня
**не может** молча дать ту же сборку (сборки иммутабельны — новый уровень = новая сборка).
Класс изменения — `execution_change` (§13), не `methodology_change`; дефолт контент не
меняет (кроме churn `index_build_id`/`crawl_config_digest`).

**`off` — режим «игнорировать документы».** Документы **не извлекаются/не индексируются**,
docling не вызывается:
* **суффикс известен** (`.pdf/.docx/...` по пути URL) ⇒ **pre-fetch-пропуск**: URL не
  фетчится, не ретейнится и **не входит** в `max_documents`; причина — `url_suffix`;
* **суффикс неизвестен**, но `Content-Type` документный ⇒ **post-fetch-пропуск**: страница
  фетчится (иначе тип не узнать) и **ретейнится** (I19/R7), элемент сеанса остаётся
  `FETCHED` (**не** `FAILED` — skip ≠ сбой), **входит** в `max_documents`; причина —
  `content_type`;
* **HTML не затрагивается**: обычные страницы обрабатываются как прежде.

Пропуски **видимы** (никогда не тихо): `IndexBuild.document_skips` +
`truncation[].skipped_documents` (`metrics.json`/payload). Продукт с пропущенными документами
«прорежен» по построению ⇒ попадает в `truncated_products`: absence-evidence
`source_scope: exhaustive` поверх него даёт **аномалию** (`crawl_truncation`, защита от
false ❌ I2/I2b), а сеанс не рапортуется `COMPLETED`. `off` **не экономит фетч**
суффикс-less PDF (content-type узнаётся только после фетча) — экономится лишь **docling**.
Блэклисты (`exclude_documents`) **побеждают** `off` (проверяются раньше и по другому ключу —
basename/path без знания content-type); «полная» экономия фетча — только блэклист/суффикс.

**Память по уровням.** Пик = `W × footprint(level корня)` (`W` = `extract_workers`), где
`footprint(full)` ≈ 2.0 GiB/процесс; у `text` ML-весов нет (заметно ниже), у `layout`/
`layout_tables` — промежуточно (`full` ≈ 2.0 GiB peak, 2.6–7.5 с/стр). Пулы корней
**последовательны** (создаются лениво на корень и `shutdown` в `finally`), поэтому пик —
**максимум** по корням, а **не** сумма: прогон с site A `full` и site B `off` не складывает
память. `off`-корень docling не строит вовсе (0). Операторский escape hatch от «слишком
медленного PDF» — понизить уровень per-site (`domain_profiles.yaml: document_level: layout`),
взять `off` или уменьшить `extract_workers` — **без правки кода**.

**Развёртывание `split`.** Под `split` документную **экстракцию** исполняет **edge** своим
конфигом; поэтому уровни **качества** извлечения (`text`/`layout`/`layout_tables`/`full`) в
ядре **не действуют** — они не распространяются на edge, а распространение требует
аддитивного расширения шва (`/v1/*`) — **отдельное** решение. Но **`off` действует и под
`split`**: skip-решение принимает ядро/pipeline, а не edge. Pre-fetch-пропуск по суффиксу URL
(`_skip_document_prefetch`) исполняется в drain-цикле и в executor-ветке
(`core/pipeline/corpus.py`, `if executor is not None:`), post-fetch-пропуск по `Content-Type` —
в `_commit_outcome`; обе ветки активны под `split` (остаток R-S относится к качеству
извлечения на edge, не к `off`).

`max_documents` — жёсткий потолок **на `(продукт, seed-root)`** (не исключение): в drain-цикле
проверяется ёмкость, поэтому даже пакетно поставленные seeds/ссылки сверх потолка не
фетчатся. На **resume** того же `session_id` бюджет корня инициализируется по durable-элементам
сеанса (retained, в scope корня), поэтому повторный прогон не превышает потолок и не теряет
признак усечения (B2). Усечение (`max_documents`/`max_depth`) честно фиксируется: сеанс краула
не рапортуется `COMPLETED` (→ `PARTIAL`), а absence-evidence `source_scope: exhaustive` поверх
усечённого продукта даёт аномалию для человека — строка `anomaly_kind=crawl_truncation` в
`nda_questions.csv` и поле в JSON-payload артефактов/прогона (§G/D1, B3). Если **единственная**
неполнота прогона — политическое усечение (сбоев фетча нет), команда завершается **успешно**
с явным предупреждением; реальные сбои фетча сохраняют ненулевой код (N4).

**Redirect-scope (B4).** Жёсткая проверка финального URL после редиректов применяется
**только при discovery** (`max_depth > 0`): на глубине 0 оператор явно запросил URL, и
финальная страница ретейнится как прежде (в т.ч. cross-host редирект) — это устраняет
тихую потерю корпуса. При discovery проверка **симметрична** (`www↔apex`/родительский
редирект не теряется), а настоящий уход из host-семейства поднимает явный
`redirect_out_of_scope` (не молчаливый дроп).

**robots (N1, §10).** Принуждение `robots.txt` в адаптерах (egress-слой, `RobotsGate`)
включается **автоматически ровно при discovery** (любой эффективный `max_depth > 0` или
сконфигурированный per-site `sitemap`); прогоны без discovery остаются байт-в-байт прежними
(ни одного лишнего fetch `robots.txt`). Явный `crawl.robots: true|false` (app-слой)
переопределяет автодетект. Отказ `Disallow` несёт отдельную причину `robots_disallow`
(не общий сбой); фетч `robots.txt` ленивый и идёт внутри уже пейсимого `fetch`, поэтому
владелец пейсинга остаётся один (`RateLimiter`, P2-3).

### D.2. Загрузчики пакета (`pipeline/normalize.py`)

- `package.yaml` → `PackageManifest` (веса=100, уникальность id, C → baseline null).
- `catalog.csv` → `list[CatalogRow]` (showcase → rule упоминает величину; slug lowercase).
- `baselines.csv` → `list[BaselineRow]`.
- `cohort.yaml` → `Cohort` (ровно один якорь — §1.6).
- `golden.csv` → `list[GoldenRow]` (тир fast/full; страта ❌ обязательна для full).
- `domain_profiles.yaml` → `dict[str, DomainProfile]` — три независимые оси получения
  контента: `fetch: http|browser`, `extract: auto|html|document` и ось стратегии краула
  `strategy: auto|honest|hybrid` (+ опц. `wait_for` для браузерного рендера и per-host
  `rate_limit`). Ключ — `host` с опциональным `/path`; схема строгая
  (`extra="forbid"`: старые `render`/`index`/`spa` отвергаются). Резолв `FetchPlan` для
  каждого URL — **в pipeline** (`pipeline/fetch_plan.py`), не в адаптерах:

  ```yaml
  docs.example:          {fetch: http,    extract: html}
  app.example:           {fetch: browser, extract: html, strategy: honest, wait_for: "() => document.readyState === 'complete'"}
  example.org/documents: {fetch: http,    extract: document}
  api.example:           {fetch: browser, extract: auto, strategy: hybrid, rate_limit: {min_s: 1.0, max_s: 3.0}}
  ```

  Матчинг: host (или родительский суффикс) + опциональный префикс пути; приоритет —
  длиннейший `path_key`, затем длиннейший `host_key`. Явный `--render-modes` по точному URL
  переопределяет **только** ось `fetch`, оставляя `extract`/`strategy`/`wait_for`/`rate_limit`
  из профиля.

  Профиль домена — **высший** слой цепочки осей контроля краула (D1/D2): per-site
  `max_workers`/`extract_workers`/`max_depth`/`max_documents`/`exclude_paths`/
  `exclude_documents`/`document_level` переопределяют run/app-дефолты
  (`resolve_crawl_limits`). Например,
  `example.org/documents: {fetch: http, max_depth: 1, exclude_documents: ["*.pdf"]}`.
  Схема остаётся строгой (`extra="forbid"`). Per-site `document_level` (P5) — операторский
  escape hatch от «слишком медленного PDF»: `example.org: {document_level: layout}`.

  Профиль домена несёт также per-site URL публичного **sitemap** (D3):
  `example.org: {sitemap: "https://example.org/sitemap.xml"}`. Его `<loc>` добавляются
  **дополнительными** depth-0 seeds того же seed-root (не заменяют seed-URL оператора;
  sitemap-only работа — если seed-URL не задан). Сам sitemap — не страница: не ретейнится
  и не ингестится (не evidence, I19). Опрос идёт через `PollPort` (`kind=sitemap`, §D.2.1);
  `sitemapindex` разворачивается **ровно на один уровень** (потолки `MAX_CHILD_SITEMAPS`/
  `MAX_SITEMAP_ENTRIES` в `core/util/sitemap.py`; вложенный индекс → отказ). `<loc>` проходят
  `validate_url` (SSRF), затем тот же блэклист/`max_documents`/`max_depth`, что и прочие seeds.
  Пример: `example.org: {fetch: http, sitemap: "https://example.org/sitemap.xml"}`.

### D.2.1. Источники краула (`packages/{case_id}/crawl_sources.yaml`) — source routing

Пакетный артефакт `crawl_sources.yaml` (`CRAWL_SOURCES_FILENAME`) задаёт **маршрутизацию
источников** краула: как каждый URL продукта добывается и разбирается. Слои (низкий → высокий
приоритет в резолвере `pipeline/crawl_sources.py::resolve_crawl_target`):

* `defaults` — дефолтный источник пакета (fallback для всех продуктов);
* `products` — источник на продукт (`product_id → CrawlSource`);
* `sites` — источник на сайт продукта (`product_id → {host[+path] → CrawlSource}`), лучший
  `host[+path]`-ключ побеждает.

Каждый `CrawlSource` несёт независимые оси (`fetch`/`extract`/`strategy`/`wait_for`/`rate_limit`),
строковые `kind`/`engine` (семантика источника и идентификатор движка транспорта — свободные
строки-данные, не доменные enum) и свободный per-source `params` (`dict[str, Any]`, сливается по
слоям: поздний ключ побеждает). Схема строгая (`extra="forbid"`).

```yaml
# packages/{case_id}/crawl_sources.yaml
defaults:
  kind: web                 # kind ∈ {web, feed, channel} — строки-данные (sitemap — в D.2)
  engine: http
products:
  p1:
    engine: scrapy          # per-product: движок транспорта (побеждает defaults)
sites:
  p1:
    feed.example.com:       # per-site: каталог-фид
      kind: feed
      params: {limit: 50, since: "2026-01-01"}
    channel.example.com:    # per-site: канал
      kind: channel
      params: {token_env: "CHANNEL_TOKEN"}   # секреты — только по ссылке на env
```

**`kind` и директорные источники.** `kind` — свободная строка-данные; `web` — обычная страница.
`feed`/`channel` — **директорные** виды (`_DIRECTORY_KINDS = {"feed","channel"}`): такой источник
**разворачивается в список ссылок-записей** (`FeedEntry`), а не индексируется сам. Ленты
разбираются чистыми парсерами `core/util/feed.py` (`feed` — XML: RSS/Atom/RDF; `channel` —
JSON/JSON-lines/список URL). Записи получают **провенанс каталога** (`kind` = `feed`/`channel`,
`engine` — собственный движок URL-записи), затем добываются как **обычные страницы** штатным путём:
retention сырых байтов → extract → chunk → index (I19). Развёртка — **ровно один уровень** (записи
не ре-расширяются, даже если их `kind` тоже директорный). Сам **документ каталога не ретейнится и
не ингестится** (не evidence, I19); его элемент сеанса помечается `fetched` **без** `raw_path`.

**`sitemap` — не вид `crawl_sources.yaml` (D3).** Публичный sitemap задаётся **per-site** в
`domain_profiles.yaml` (`DomainProfile.sitemap`, §D.2), а не в `crawl_sources.yaml`, и **не**
входит в `_DIRECTORY_KINDS`. Транспорт опроса — тот же `PollPort`, но с отдельным `kind`
`"sitemap"` (`LocalFeedPoller` ‖ `RemoteFeedPoller` → шов `crawl/poll`); парсер —
`core/util/sitemap.py` (`urlset`; `sitemapindex` — один уровень; gzip — по magic-байтам).

**`params`.** Свободный `dict`; валидируется реализацией `PollPort.validate_params` **до** опроса
(в фазе корпуса). Базовые ключи: `limit` (целое > 0), `since` (ISO-8601/RFC822 — отсекает
доказуемо старые записи; неизвестная дата сохраняется консервативно), `token_env` (имя
env-переменной с токеном канала — **единственный** допустимый способ секрета; инлайн-ключи вида
`token`/`api_key`/`authorization` отвергаются). Проблемы `params` (или отсутствие поллера, или
сбой опроса) → каталог `failed`/`source_unavailable`, сеанс финализируется `partial` (не «тихо
пусто»). Каждое действие опроса проверяется **на входе** (URL ленты) и **на каждой записи**
(SSRF-политика `core/util/security.py::validate_url`); небезопасная запись отбрасывается.

**Паритет `local`/`split` и шов.** Логика развёртки одна; различается транспорт опроса:
в профиле `local` — `LocalFeedPoller` (in-process `CrawlerPort`), в профиле `split` —
`RemoteFeedPoller` через op шва **`crawl/poll`** (scope `crawl`, `schema_version` шва остаётся
`"1"`; `kind` аддитивно расширен до `feed|channel|sitemap`). Для `sitemap` DMZ исполняет и
развёртку `sitemapindex` (один уровень), ядру возвращаются уже плоские ссылки. Шов добывает
**только документ каталога/sitemap**; записи возвращаются как **ссылки**
(`{entries, count, truncated}`), их байты — не через этот опрос, а обычным фетчем (I19).
В `executor`-режиме (`split`, долгоиграющий краул) развёртка каталогов и `<loc>` sitemap
исполняется **отдельной волной** с собственным `idempotency_key` (волна 1 — top-level URL,
волна 2 — записи каталогов и `<loc>` sitemap; локальный путь ставит `<loc>` как
дополнительные depth-0 seeds того же seed-root).

**Версионирование.** Операционные оси краула `run.yaml:crawl.strategy`/`crawl.rate_limit` — это
версионная граница класса `execution_change` (§13, `03 §C.1`/Backlog B), а отпечаток конфигурации
источников `crawl_sources_digest` — **информационный** маркер провенанса (не граница §13).

### D.3. Линтер (`pipeline/lint.py`, блокирует прогон)

Всё из §4.2 + новые: `absence_rule` с аттестацией без процедуры `scope_attested_by` → ошибка;
непустой baselines для архетипа C → **ошибка**; пустые `concept_terms` → warning; golden-full без
страты ❌ → ошибка; незаполненные авторские поля (`definition` роли, `selection_reason` когорты) →
**`incomplete`** (A3; выражается как `None`, а не текст-заглушка). `LintReport(blocking, warnings,
incomplete)`; `ok` = нет блокирующих, `complete` = нет `incomplete`, **`ready` = нет ни блокирующих,
ни `incomplete`** — гейт `run`/`plan`/`crawl` и `package validate`. Авторские команды пакета
(`package …`) ориентируются на `blocking`/`complete` и `incomplete` **не** блокирует создание/правку.

**Усечение запросов (S13).** На ячейку допускается ≤3 запроса, приоритет детерминирован:
(1) `title`; (2) `concept_terms` (сначала ru, затем en, затем порядок в пакете). Линтер не
блокирует при большем числе концептов, но предупреждает, что лишние в запрос не войдут.

### D.4. Реестровый конфиг (`packages/{case_id}/registries/{id}.yaml`)

Спецификации реестров — **package-scoped**: `packages/{case_id}/registries/{registry_id}.yaml`,
перечисленные в `package.yaml` (`PackageManifest.registries`). Загрузка —
`core.adapters.registry_generic.load_package_registry_configs(package_dir, registry_ids)`;
корневого `registries/` нет (шаблоны — `docs/templates/registries/`). Секретные заголовки —
через `headers_env` (`имя_заголовка → env-переменная`), а не значениями в файле.

```yaml
registry_id: rrp
jurisdiction: RU
endpoint: "https://.../{query}"
method: GET
format: json            # json | html
mapping: { name: "$.items[*].name", version: "$.items[*].version" }
# headers_env: { Authorization: RRP_TOKEN }   # значение — из окружения, не из файла
```

`adapters/registry_generic.py` интерпретирует; код ядра не знает названий реестров (§1.3).

### D.5. Конфиг развёртывания (`config/deploy.yaml`, 07)

Отдельно от `run.yaml` (развёртывание ≠ методологический прогон). Выбирает профиль зон и адреса
DMZ-шва:

```yaml
profile: split                 # local | split | split-hardened
edge:
  base_url: "https://edge.internal:8443"
  tls: { ca: certs/ca.pem, client_cert: certs/core.pem, client_key: secrets/core.key }  # без pinning (07 §8)
  auth: mtls                   # mtls | oidc (issuer/audience — IDP заказчика)
  timeout_s: 30
  max_retries: 2
  max_response_bytes: 52428800
  audit: { sink: "file|syslog", target: "…" }   # SIEM-экспорт
extractor_zone: edge           # edge | core | separate
search:
  budget: { ru: 5, en: 5 }      # симметрия I16
  adapter: yandex               # провайдер поиска по умолчанию (Yandex Search API)
  # режим Yandex `async` (дефолт) | `sync` — env YANDEX_SEARCH_MODE (§D.6), не поле YAML
  by_language:                  # переопределение по языку (география-прокси, I16)
    ru: yandex
    en: glm
crawl:                         # операционная политика HTTP-краулинга корпуса (local|split)
  user_agent: "coHound-crawler/0.1"   # None → дефолт HttpCrawler
  timeout_s: 30                # None → дефолт HttpCrawler (30 с)
  follow_redirects: true
  # D1/D2: application-слой осей контроля краула (низший приоритет app < run < site;
  # резолв — pipeline/crawl_frontier.py:resolve_crawl_limits).
  max_workers: 4               # опц. (>0): D4-пул потоков на сетевой fetch (None/1 = inline)
  extract_workers: null        # опц. (>0): P4-пул экстракции (процессы local / потоки split;
                               # None/1 = inline). Память: W × footprint(document_level)
  document_level: full         # P5: уровень извлечения документов (off|text|layout|
                               # layout_tables|full; дефолт full = сегодняшнее поведение)
  max_depth: 0                 # >=0: 0 = только seeds (BFS идёт и под executor/split — A2/R2)
  max_documents: null          # опц. (>0): потолок добытых страниц на (продукт, seed-root)
  exclude_paths: []            # glob блэклиста по пути URL (fnmatchcase; ведущий "/" опционален)
  exclude_documents: []        # glob блэклиста по basename URL, напр. ["*.pdf"]
  robots: null                 # N1: null=auto (robots при discovery); true/false — явно
  browser:                     # браузерный краулер BrowserCrawler (honest/hybrid; 07)
    mode: cdp                  # cdp | persistent (декларативен; взаимоисключающие)
    cdp_url: "http://127.0.0.1:9222"    # mode: cdp — подключение к существующему браузеру
    user_data_dir: null        # mode: persistent — каталог профиля (взаимоисключим с cdp_url)
    headless: false
    channel: null              # канал браузера (напр. установленный дистрибутив)
    timeout_ms: 30000
```

Профиль `local` игнорирует `edge.*`. Секция `crawl` действует в обоих профилях: она
переопределяет операционные параметры лёгкого `HttpCrawler` (`user_agent`/`timeout_s`/
`follow_redirects`); при `None`/отсутствии поля берётся дефолт адаптера — обратная
совместимость профиля `local` без файла.

**`extract_workers` (P4, runbook памяти).** Параллельная экстракция документов:
`local` — пул процессов (`ProcessPoolExecutor`/forkserver, docling-экстрактор строится на
воркер), `split` — пул потоков поверх `RemoteExtractor` (шов `/v1/*` не меняется). Окно
`W` = `extract_workers`: «в полёте» не более `W` экстракций, доводка строго FIFO (порядок
коммита = порядок отправки). Дефолт `None` (выключено) ⇒ поведение **байт-в-байт** сегодняшнее.
**Бюджет памяти: `W × footprint(document_level)` RSS на процесс** (у `full` измерено `≈2.0 GiB`
peak: `import docling` + первый `convert`; см. `05` §V4) — планируйте `W` против доступной RAM
хоста; авто-детект не вводится (A1: не угадывать). `footprint` зависит от уровня (P5):
`full` ≈ 2.0 GiB/процесс (ML-веса layout+TableFormer+OCR), `text` — заметно ниже (ML-весов
нет), `layout`/`layout_tables` — промежуточно; **экстраполировать 2.0 GiB на `text/layout`
нельзя — мерьте** (`full` 2.6–7.5 с/стр). Пулы создаются **per seed-root** и `shutdown` в
`finally`, корни обходятся **последовательно**, поэтому пик = `W × footprint(level корня)` —
**максимум** по корням, а не сумма: прогон с site A `full` и site B `off` не складывает
память (`off`-корень docling не строит вовсе). Один медленный PDF в голове FIFO задерживает
доводку окна — следствие FIFO-детерминизма, не регресс. Отказ воркера (краш/OOM/таймаут) →
явный `FAILED` + сеанс `PARTIAL`, пул пересобирается (bounded) либо остаток идёт inline;
авто-ретрая нет (восстановление — повторный прогон того же `session_id` по retained raw без
ре-фетча).

Вложенная секция `crawl.browser` (`CrawlBrowserConfig`) конфигурирует браузерный краулер
`BrowserCrawler` (`crawl_browser.py`, прямой Playwright/CDP, honest/hybrid). Режимы
взаимоисключающи: `mode: cdp` требует непустой `cdp_url` и запрещает `user_data_dir`;
`mode: persistent` требует непустой `user_data_dir` и запрещает `cdp_url`. `mode`
**декларативен** и не является источником истины рантайма — адаптер ветвится по заданным
данным (`cdp_url`/`user_data_dir`). Требуется установленный браузер Playwright
(`playwright install chromium`); в закрытом контуре — зеркало CDN
(`PLAYWRIGHT_DOWNLOAD_HOST`/`PLAYWRIGHT_BROWSERS_PATH`).

**Провайдер поиска (`search.adapter` / `search.by_language`).** Провайдер выбирается
детерминированно (`core.config.deploy.resolve_search_provider`) по приоритету:
явный CLI-флаг (`--search-adapter`) > `search.by_language[lang]` > `search.adapter` >
дефолт `yandex`. Маршрут — **функция от языка**, а не от продукта: один и тот же язык
даёт один и тот же провайдер для всех продуктов цикла, поэтому симметричный протокол
**I16** сохраняется, а дифференциация по географии реализуется через каноничную ось
`lang` (`SearchQuery.lang`, `search_log.language`, бюджет по языкам §7.2). Решение
вычисляется по правилу, а не LLM (**дух I21**). Имена провайдеров — свободные строки-данные:
config-слой не импортирует `cli.wiring`; валидация против `SUPPORTED_SEARCH_ADAPTERS` —
в composition root, неизвестный провайдер отвергается с понятной ошибкой (I19: кандидаты
не выдумываются). Выбранный провайдер фиксируется в провенансе/`search_log` (**I23**).
Смена таблицы провайдеров меняет протокол поиска и потому влияет на сопоставимость
циклов — это **версионная граница**: фиксируется в манифесте/провенансе (§13).
Режим Yandex Search API — `async` (дефолт; переопределяется env `YANDEX_SEARCH_MODE`).

Загрузчик `config/deploy.py` → `DeployConfig`
(домен-нейтрален). `deploy_profile`, версии edge-компонентов (`/v1/capabilities`) и
`edge_schema_version` пишутся в `run_manifest.yaml` (R7). `edge/` (composition root Z1)
читает тот же файл для bind-адреса, аутентификации входящих (IDP) и экспорта аудита
(SIEM). Полные требования к прозрачности шва для IDP/DLP/IDS — `07` §8.

### D.6. Переменные окружения и внешние ключи

Единая точка правды по env-переменным конвейера. Секреты **не коммитятся**; шаблон без
значений — `.env.example` в корне. Ключи передаются через среду процесса (в `split` —
только в контейнере edge, см. ниже).

**Контуры (I18/07).** Эгрессионные ключи внешнего поиска (`YANDEX_*`, `GLM_SEARCH_API_KEY`)
в профиле `split` живут **только в
edge** (composition root Z1, `07` §4/§7); ядро ходит за кандидатами через `/v1/*`-шов и
этих переменных не читает. **Оценщик (Ш6, I18) веб-тула не имеет** и никакие поисковые
ключи не читает — он работает только по переданным чанкам корпуса. В профиле `local`
поиск/краул исполняются in-process, поэтому те же переменные доступны процессу ядра.

| Переменная | Назначение | Обязательность | Fallback | Потребитель |
|---|---|---|---|---|
| `COHOUND_LLM_BASE_URL` | базовый URL [OI]-совместимого LLM-API корпоративного облака | опц. | — (обязателен: внутренний endpoint) | `wiring.build_discovery_llm` (LLM-разведка `passport-scout`, Ш2) |
| `COHOUND_LLM_API_KEY` | ключ LLM-разведки | **обязателен для вызова** | — (обязателен для вызова) | `wiring.build_discovery_llm` |
| `COHOUND_LLM_SLOT` | активный LLM-слот: `cheap`\|`flagship` (см. `deploy.yaml.llm`) | опц. | `cheap` | `wiring` (выбор модели) |
| `COHOUND_LLM_MODEL` | `model_id` LLM-разведки (переопределяет активный слот) | опц. | `deepseek-v4.1-flash:cxbc` | `wiring.build_discovery_llm` |
| `COHOUND_LLM_DECODER` | механизм декодирования (`none`/`json_object`/`json_schema`) | опц. | `json_object` | `wiring.build_discovery_llm` |
| `COHOUND_LLM_TIMEOUT` | таймаут LLM-запроса, с (>0) | опц. | `120.0` | `wiring.build_discovery_llm` |
| `COHOUND_LLM_MAX_TOKENS` | максимум токенов ответа (>0) | опц. | `4096` | `wiring.build_discovery_llm` |
| `COHOUND_SCOUT_CRAWLER` | краулер `passport-scout`: `http`\|`scrapy`\|`browser` | опц. | `http` | `wiring.build_scout_crawler` |
| `COHOUND_NO_REEXEC` | отключить автоперезапуск `hound.py` в проектной `.venv` | опц. | — | `hound.py` |
| `COHOUND_REEXEC` | предохранитель от зацикливания перезапуска (ставит `hound.py`) | служебная | — | `hound.py` |
| `YANDEX_SEARCH_API_KEY` | ключ Yandex Search API | **обязателен для адаптера** | — | `adapters.search_yandex` |
| `YANDEX_FOLDER_ID` | folder-id Yandex Cloud | **обязателен для адаптера** | — | `adapters.search_yandex` |
| `YANDEX_SEARCH_MODE` | режим поиска Yandex: `async` (дефолт) \| `sync` | опц. | `async` | `adapters.search_yandex` |
| `GLM_SEARCH_API_KEY` | ключ GLM web-search | **обязателен для адаптера** | — | `adapters.search_glm` |

Примечания:

- **Обязательность** ключей поиска проявляется в `hound discover`: без пары
  (`api_key`, `folder_id`) адаптер считается недоступным, и кандидаты **не
  выдумываются** (I19) — команда возвращает `EXIT_ERROR` с явным сообщением.
- Флаги CLI переопределяют env: `passport-scout --llm-base-url/--llm-model/--llm-decoder`,
  `discover --search-adapter`.
- **Две слотовые модели (`deploy.yaml.llm`).** Секция `llm:` в `config/deploy.yaml` задаёт два
  именованных слота — `cheap` и `flagship`; **оба по умолчанию** указывают на
  `deepseek-v4.1-flash:cxbc`. Активный слот выбирается детерминированно по приоритету:
  `--llm-slot` > env `COHOUND_LLM_SLOT` > `run.yaml: llm_slot` > `deploy.llm.slot` > `cheap`.
  `api_key` берётся из env (`COHOUND_LLM_API_KEY`); endpoint — [OI]-совместимый
  API корпоративного облака. Форма секции — `07` §7.

**Runbook живого прогона корпуса.** Минимальный операторский запуск (профиль `local`,
in-process, без Правки кода):

```bash
hound run \
  --run-yaml packages/{case_id}/run.yaml \
  --package packages/{case_id} \
  --passports-dir passports \
  --urls <urls>.json \
  --until corpus \
  [--render-modes <render>.json]
```

- `<urls>.json` — `{product_id: [url, ...]}` (явный корпусный вход, Ш5; `doc_domains`
  паспортов молча не фетчатся, I19).
- `[--render-modes <render>.json]` — `{url: "http"|"browser"}`: явное переопределение оси
  `fetch` для SPA/JS-страниц (значение не из `http`/`browser` → `EXIT_USAGE`). **Явное
  переопределение побеждает доменный профиль** по оси `fetch`; `extract`/`strategy`/`wait_for`/
  `rate_limit` при этом берутся из `domain_profiles.yaml` (D.2).
- `--cutoff-k K` (I20) и `--render-modes` влияют на фазы Ш5/Ш6; `--cutoff-k` имеет смысл
  при прогоне до `evaluate`.

Что появится на диске (раскладка A.1/A.2):

- `corpus_raw/{product_id}/{hash}.bin` — retention сырых байтов (пишется до экстракции, I19);
- `index_store/{package}/{build_id}/{product_id}/` — иммутабельная индексная сборка
  (chunks + `index_manifest`; при подключённых эмбеддере/билдере — `.npy`-шарды);
- `runs/{run_id}/cohound.db` — чанки/документы/манифест индекса прогона;
- `cache/http/{url_hash}.bin` — глобальный content-addressed кэш (R7; повторный прогон
  читает кэш без живого ре-фетча).

Тяжёлые опциональные зависимости (extras `pyproject.toml`, `05-dependency-stack.md`):

- `docling` (extra `extract`/`extract-pdf`) — извлечение PDF/DOCX/PPTX/XLSX; без него
  документные форматы деградируют, HTML обрабатывается `trafilatura`;
- `sentence_transformers` (extra `embed`) — плотные эмбеддинги bge-m3 (dense-ретривал);
  без него — лексический индекс (`tantivy`/BM25, extra `retrieval`) и/или sparse;
- `model2vec` (extra `embed-lite`, без `torch`) — лёгкие офлайн-эмбеддинги
  (`adapters/embed_model2vec.py`; дефолт `minishlab/potion-multilingual-128M`, dim=256);
  альтернатива `embed` для контуров без GPU/torch; отсутствие модели/зависимости — честный
  `EmbedderUnavailable`, нулевыми векторами не подменяется;
- `scrapy` + `playwright` (extra `crawl`) — браузерный рендер SPA/JS; `COHOUND_SCOUT_CRAWLER=scrapy`
  или `=browser` для `passport-scout` (прямой `BrowserCrawler`).

---

## E. CLI и UX

### E.1. Команды (из §3.1)

> **Источник истины по CLI — код `core/cli/`**: команды живут в `core/cli/commands/*` (реестр —
> `commands/__init__.py:COMMANDS`), подкоманды `hound package` — в `core/cli/package/*`
> (`add_package_parser`, `cmd_package_*`); `main.py` — тонкая точка входа, `package_commands.py` —
> re-export-шим (обратная совместимость). Таблица ниже — его зеркало; при расхождении верен код.
> Актуальный список команд см. также `./hound.py --help`.

| Команда | Назначение | Пишет |
|---|---|---|
| `hound resolve` | нормализация входов + линт | — (отчёт) |
| `hound plan` | план прогона, когорта | run.yaml (черновик), cohort.yaml |
| `hound discover` | Discovery-агент | passports, run |
| `hound passport-scout` | авто-генерация **черновика** паспорта продукта: краул `--url` (и, при поиске, URL выдачи) → tool-less LLM-разведка (`discovery_llm`) → консервативный черновик. Сателлиты — только `draft`/`not_found`; `relationship_evidence`/`upstream` — лишь при подтверждённом URL и дословной цитате (I10/I15, precision-first). Публикация только при `report.ok`; `--dry-run` не пишет. Кандидаты не выдумываются без LLM-конфигурации | `passports/{product_id}/…yaml` (+ sidecar кандидатов) |
| `hound package scout` | веб-заземлённая разведка пакета категории: pipeline сам делает SERP+краул+extract, tool-less LLM синтезирует гипотезы → консервативный маппинг → кандидаты (`needs_human=True`; auto-approve нет, I18/I19/A3). Провайдер поиска резолвится из `config/deploy.yaml` (`search.adapter`/`search.by_language` по `--lang`) детерминированно; `--search-adapter` переопределяет (I16/I21/I23) | `packages/{case_id}/sources/scout/{date}.candidates.json` (+ `.provenance.json`, в т.ч. `search_provider`) |
| `hound package {module\|role\|param\|baseline\|scope\|golden\|run\|artifact} {get\|list\|show}` | read-плоскость управления пакетом: единичные элементы и наборы по критерию поиска (`--search`/`--product`/`--param`), обзор/чтение всех 8 артефактов (`artifact list`/`artifact get [--raw]`), `--json` | — (read-only) |
| `hound run [--until PHASE]` | конвейер Ш1–Ш9 | БД, артефакты |
| `hound crawl {run\|status\|list\|resume\|restart\|cancel}` | сеансы краула (`local`): durable-запуск фазы корпуса Ш5, статус/счётчики, resume/restart/cancel | `crawl_sessions`, `crawl_session_items` |
| `hound review` | очередь ревью / preliminary-стол | cells (reviewed) |
| `hound report` | сборка артефактов | outputs |
| `hound golden [--tier fast\|full]` | прогон golden | отчёт |
| `hound sensitivity` | пост-хок по осям | отчёт |
| `hound diff <run_a> <run_b>` | межцикловый diff (запрещён через major) | diff.md |
| `hound promote --validated-only` | preliminary → strict (переаттестация) | cells |
| `hound gate --gate G1..G4 --actor <вы>` | запись **человеческого решения** гейта G1–G4 (утверждение); гейт проходит фактом строки в `gate_decisions`, отказ — отсутствие строки (S9/I6/I7) | gate_decisions |
| `hound deploy-check` | диагностика профиля: достижимость DMZ-шва, версии/форматы edge (`/v1/health`, `/v1/capabilities`) + неразрушающий зонд очереди краула (`crawl/status` → строка `crawl`; без подачи задания) | — (отчёт) |
| `hound experiment retrieval` | прогон R0/R1/R2 на retrieval-golden и фиксация измеренного `retrieval_profile`/`retrieval_cutoff_K`/`embed_model_id`/`rrf_k` как `settings_provenance` со `source: measured` (A3, T8.6, I20). Фиксация — **только профиль**, и **только** при полном сравнении; частичное сравнение/`skipped` → `source: default`. Требует зафиксированную индексную сборку в `runs`; рассинхрон «эмбеддер запроса ↔ сборка» → отказ (§13), отсутствие предпосылки → `skipped` (N3) | `runs.settings_provenance_json`, `runs.retrieval_*` |
| `hound doctor [--strict] [--with-selfcheck]` | самодиагностика инсталляции: runtime, зависимости по extras, env/ключи, конфиг, каталоги, CLI-поверхность | — (отчёт) |
| `hound baseline-agreement --annotations <csv>` | согласие независимых аннотаторов §11.5: Cohen's κ по полному набору меток **и отдельно** по бинарному решению N/A-vs-не-N/A (N/A задаёт знаменатель Coverage, I6/I7), + матрица расхождений и счётчики. Непарные ячейки исключаются и перечисляются; нулевая вариация маргиналов → `undefined` (не выдуманное число). Разметку не создаёт: это ручная работа человека | — (отчёт; артефакт разметки — вход) |
| `hound package crawl-probe` | **измерение** оптимальных осей краула для каждого ключа `domain_profiles.yaml` (A3): лестница `http` → `browser/hybrid` → `browser/honest`; решение принимает исход **браузерного** яруса (`RawPage.fetch_mode`), а не текст ошибки — оба яруса провалились ⇒ `unresolved` с причиной (транзиентный сбой не выдаётся за `browser`); причина **диагностична**: сбой транспорта (`both_tiers_failed`) отличается от «транспорт дошёл, но экстракция дала 0 символов» (`extraction_empty` — смотреть экстрактор, не краулер; при расхождении ярусов в отчёте обе причины). Пригодность меряется **выходом реального экстрактора**, не размером байт. `wait_for`/`rate_limit` не выдумываются: только подсказка (`needs_confirmation`) и только при `429`. URL — `--urls` > `corpus_docs` (`--corpus-db`) > синтез `https://{host}{path}/` (`confidence: low`). По умолчанию **только предложение**; `--write` (требует `--actor`) правит `domain_profiles.yaml`, сохраняя шапку байт-в-байт и не тронутые блоки дословно, при отсутствии изменений — no-op. Смена осей краула — граница `execution_change` (§13), не `methodology_version` | `packages/{case_id}/sources/crawl_probe/{date}.proposal.yaml` |

**Флаги контроля краула (`hound run`, D1/D2).** В дополнение к `--max-attempts` команда
принимает оси контроля краула: `--max-depth D` (BFS-обход in-site ссылок; `0` — только seeds,
дефолт), `--max-documents N` (`>0`; потолок добытых страниц на `(продукт, seed-root)`),
`--exclude-path GLOB` и `--exclude-document GLOB` (повторяемые glob-блэклисты по пути URL и
по basename). Флаги уточняют **run-слой** поверх `run.yaml:crawl` (цепочка app < run < site);
усечение (`max_documents`/`max_depth`) фиксируется честно — сеанс краула не рапортуется
`COMPLETED` (→ `PARTIAL`), а absence `source_scope: exhaustive` поверх усечённого продукта
даёт аномалию (в `nda_questions.csv` и JSON-payload). При **resume** бюджет `max_documents`
корня восстанавливается из durable-элементов сеанса — потолок не превышается (B2).
Под `--profile split`/executor `--max-depth > 0` **поддержан** (A2/R2): BFS-раунды ведёт ядро
тем же `_drain_root`, что и локальный путь; предохранитель — `round_budget` (не молчаливая
остановка). Ось `--max-attempts`/`extract_workers` действует на обоих профилях.
Поля `truncated`/`truncated_reason`/`effective_limits`/`pages_fetched` видны в
`crawl`/`run` payload и в `metrics.json` (`crawl_truncation`); при **единственной**
неполноте — политическом усечении без сбоев фетча — код возврата `EXIT_OK` с явным
предупреждением; реальные сбои сохраняют `EXIT_ERROR` (N4).

Подкоманды `hound package …` принимают `--package` как **путь к каталогу пакета**, так и**`case_id`** (резолвится в каталоге пакетов: `--dir`, иначе resolved `paths.packages`);
несуществующий пакет — честный `EXIT_NOT_FOUND` (команды чтения fail-closed, а не ложный успех).
`hound package validate` дополнительно fail-closed, когда каталог есть, но `package.yaml`
отсутствует: `EXIT_BLOCKED` с `blocking: package.yaml отсутствует` (не `EXIT_NOT_FOUND`
и не ложный `ok: yes`; терпимость к частичным пакетам с манифестом сохраняется).
`ok: yes` требует `LintReport.ready` (нет блокирующих **и** нет `incomplete`): незаполненные
авторские поля печатаются `incomplete:` и дают `EXIT_BLOCKED` — неполнота не проходит молча
(при этом `incomplete` **не** блокирует команды авторства, только прогон/валидацию).

`hound doctor` — read-only и по умолчанию **без сети**: проверяет Python/venv, наличие
зависимостей по группам extras (`find_spec`, без импорта тяжёлых), заданность env/ключей
(значения **не** печатаются), файлы `config/deploy.yaml`/`.env.example`/`packages` и
package-scoped реестровые спецификации (`package.yaml: registries` → `packages/{case_id}/
registries/`, при `--package`),
запись в операционные каталоги (`runs/`/`index_store/`/`corpus_raw/`/`cache/`) и полноту
CLI-поверхности. Каждая проверка обёрнута в try/except и даёт запись `ok|warn|fail`;
секреты эгрессии в профиле `split` живут только в edge (07). `--json` — машинный отчёт
(детерминированный: `sort_keys=True`); код возврата ненулевой при `fail`, а с `--strict` —
и при любом `warn`. `--with-selfcheck` дополнительно прогоняет `tests/selfcheck.py`.

### E.2. UX-принципы

- **Гейты — явные действия:** `hound run` останавливается на G1–G4 и печатает, что требуется;
  продолжение — отдельная команда с решением человека. Никаких автоматических «approve».
- **Запись решения гейта:** решение человека фиксируется `hound gate --gate G1..G4 --actor <вы>`
  (плюс `--db`/`--runs-dir` + `--run-id`, `--target`, `--notes`; `--json`). Гейт проходит
  **фактом строки** в `gate_decisions`; «отказ» выражается **отсутствием** строки — прогон
  остаётся `blocked`. Повторная подача того же решения идемпотентна (та же строка).
- **Идемпотентность и resume:** повторный `hound run` продолжает с последней завершённой фазы;
  `--until` даёт пошаговый контроль. В CLI-JSON resume-короткое-замыкание уже завершённых фаз
  сериализуется под ключом `resumed_skipped` — это **отдельно** от `PhaseStatus.skipped` (пропуск
  из-за отсутствия предпосылки, §5); прогон останавливают только `blocked`/`failed`.
- **Сухой прогон:** `--dry-run` показывает план/бюджеты без исполнения.
- **Машинночитаемый вывод:** `--json` для CI и интеграций.
- **Понятные ошибки с кодом инварианта:** например `I2b: parameter_reference требует exhaustive
  и scope_attested_by (param=..., product=...)` — ссылка на регламент.

### E.3. Композиция (wiring)

`cli/wiring.py` (вызывается из тонкого `cli/main.py`) — единственное место сборки **ядра** (Z0): выбирает адаптеры по `run_manifest`/CLI-флагам
и `deploy.yaml`, создаёт порты, инстанцирует `Runner`. При профиле `split` веб-порты подставляются
**remote-прокси** (`RemoteCrawler`/`RemoteSearch`/…), вызывающими DMZ-сервис через `ContentGateway`.
Ручной DI без фреймворка (A1). Второй composition root — `edge/` (Z1): собирает реальные веб-адаптеры
и публикует их через gateway-сервер (07). `pipeline` не знает о профиле: получает порты как аргументы.

---

## F. Миграция текущего кода

| Шаг | Действие | Проверка | Статус |
|---|---|---|---|
| 1 | Расщепить `core/schemas.py` → `domain/{enums,models,invariants,r2,anchors}.py` | `tests/selfcheck.py` зелёный | ✅ T1.2, монолит **удалён** |
| 2 | Расщепить `core/db.py` → `store/sqlite.py` (DDL) + `ports/store.py` (контракт) | DDL-тест + триггеры | ✅ T1.3, монолит **удалён** |
| 3 | Ввести `ports/*` (Protocol) и адаптеры-заглушки | импорт-граф CI (итерация 4) | ✅ |
| 4 | Перенести проверки `Cell._enforce_invariants` в `domain/invariants.py` (вызов из Pydantic) | selfcheck | ✅ |

Ранее использовавшиеся transitional re-export-шимы `core.schemas`/`core.db` **удалены**;
публичный контракт — `core.domain` и `core.store.sqlite` (проверяется
`tests/selfcheck.py::_shims_removed`).

---

## G. Traceability и открытые вопросы

| Регламент | Покрытие |
|---|---|
| §3.1 CLI | E.1 |
| §4.4 БД | B |
| §4.5 run.yaml | D.1 |
| §9 артефакты | A.2 |
| §13 версионирование | C |
| I10/I15 | B.1 (resolve_source_url) |
| Развёртывание/DMZ (07) | A.1 (`edge/`, `config/deploy.yaml`), D.5, E.3 |

**Открытые вопросы к итерации 4:** формат импорт-графа CI; структура логирования; бэкап/
восстановление прогона; как считать `prompt`-шаблоны версиями. Отдельная корпусная БД —
**отвергнута** (S6, B.2; единая БД сохраняет FK).
