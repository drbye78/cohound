# coHound — архитектура: Итерация 3 — данные, конфигурация, CLI

**Документ:** `docs/architecture/03-data-config-cli.md`
**Статус:** черновик проектирования (итерация 3 из 4 + ревью-гейт)
**Опирается на:** `00-overview.md`, `01-modules-and-contracts.md`, `02-subsystems.md`
**Источник истины:** `cohound-spec-v2.md`

Детальный дизайн: (A) раскладка на диске, (B) операционное хранилище и транзакции,
(C) версионирование, (D) конфигурация и входы, (E) CLI и UX, (F) миграция текущего кода.

---

## A. Раскладка на диске (per run и глобально)

### A.1. Глобальные каталоги

```
repo/
  cohound-spec-v2.md
  core/                      # пакет (см. итерацию 1)
  packages/{case_id}/
    package.yaml             # §4.2
    catalog.csv              # 11 колонок
    baselines.csv
    cohort.yaml              # §1.6
    golden.csv
    adversarial/
    domain_profiles.yaml
  registries/{registry_id}.yaml   # generic-конфиг фетчеров (§1.3-совместимо)
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

Текущая схема §4.4 (`core/db.py`) переносится в `store/sqlite.py` без семантических изменений
(11 таблиц + `schema_meta`, индексы, триггеры I4/I6/I7/I13/I18). Добавляются методы доступа
(реализация `StorePort`):

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
    # чтение
    def iter_cells(run_id, state=None) -> Iterator[Cell]
    def get_chunk(chunk_id) -> Chunk            # для валидации цитат
    def resolve_source_url(source_id) -> str    # I10/I15
    def read_manifest(run_id) -> RunManifest
    def iter_gate_decisions(run_id) -> Iterator[GateDecision]   # S9
```

### B.2. Транзакции и производительность

- **WAL** включён (уже в DDL); читатели не блокируют писателя.
- **Батчевые записи:** ячейки Ш6 пишутся пачками (`save_cells_batch`), одна транзакция на N
  (по умолчанию 200) ячеек → снижает fsync-нагрузку.
- **Единая БД (S6):** корпус и операционные данные живут в **одной** SQLite-БД прогона
  (`runs/{run_id}/cohound.db`). Вариант «отдельная корпусная БД» **отвергнут**: при разнесении
  таблиц SQLite не может обеспечить FK между `cell_evidence.source_id` и `corpus_chunks.chunk_id`,
  что ломает provenance (I10/I15/I19). Объём корпуса — не основание менять модель данных;
  триггеры масштабирования — §15, а не разделение БД.
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
| `index_build_id`, `retrieval_profile` (per cell) | cells | provenance ячейки (R15) |
| `prompt_hash` (per cell) | cells | воспроизводимость ячейки |
| `stale_reason` (per cell) | cells | инвалидация при смене сборки/prompt-шаблона |

### C.2. Версионные границы (bump + golden-full)

- Смена модели под сценарий (I21) → bump `methodology_version`, обязателен `golden-full`.
- Смена когорты после G1 → новый цикл (межцикловый diff запрещён).
- Смена `rule`/`absence_rule` параметра → bump `catalog_version` + golden.
- Смена `retrieval_profile`/K → новая `index_build_id` + retrieval-golden.
- Смена fusion-параметров (`rrf_k`) → `retrieval_params_json` + новая `index_build_id` + retrieval-golden (S1).
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
```

Загрузчик `config/run_config.py` → `RunConfig` (домен) с валидацией (anchor ∈ products;
preliminary не совместим с capability_ranked — I13). `ranking_policy` — **запрошенный** режим
отображения (K6); вычисленный ранг определяется sensitivity и может демотироваться независимо от
запрошенного (02 E.1). `products` — **проекция** `cohort.yaml` (один канонический источник состава);
линтер проверяет совпадение множеств `products` и `cohort.product_id` (S11).

### D.2. Загрузчики пакета (`pipeline/normalize.py`)

- `package.yaml` → `PackageManifest` (веса=100, уникальность id, C → baseline null).
- `catalog.csv` → `list[CatalogRow]` (showcase → rule упоминает величину; slug lowercase).
- `baselines.csv` → `list[BaselineRow]`.
- `cohort.yaml` → `Cohort` (ровно один якорь — §1.6).
- `golden.csv` → `list[GoldenRow]` (тир fast/full; страта ❌ обязательна для full).
- `domain_profiles.yaml` → профили рендера per domain.

### D.3. Линтер (`pipeline/lint.py`, блокирует прогон)

Всё из §4.2 + новые: `absence_rule` с аттестацией без процедуры `scope_attested_by` → ошибка;
непустой baselines для архетипа C → **ошибка**; пустые `concept_terms` → warning; golden-full без
страты ❌ → ошибка. `LintReport(blocking, warnings)`.

**Усечение запросов (S13).** На ячейку допускается ≤3 запроса, приоритет детерминирован:
(1) `title`; (2) `concept_terms` (сначала ru, затем en, затем порядок в пакете). Линтер не
блокирует при большем числе концептов, но предупреждает, что лишние в запрос не войдут.

### D.4. Реестровый конфиг (`registries/{id}.yaml`)

```yaml
registry_id: rrp
jurisdiction: RU
endpoint: "https://.../{query}"
method: GET
format: json            # json | html
mapping: { name: "$.items[*].name", version: "$.items[*].version" }
```

`adapters/registry_generic.py` интерпретирует; код ядра не знает названий реестров (§1.3).

---

## E. CLI и UX

### E.1. Команды (из §3.1)

| Команда | Назначение | Пишет |
|---|---|---|
| `hound resolve` | нормализация входов + линт | — (отчёт) |
| `hound plan` | план прогона, когорта | run.yaml (черновик), cohort.yaml |
| `hound discover` | Discovery-агент | passports, run |
| `hound run [--until PHASE]` | конвейер Ш1–Ш9 | БД, артефакты |
| `hound review` | очередь ревью / preliminary-стол | cells (reviewed) |
| `hound report` | сборка артефактов | outputs |
| `hound golden [--tier fast\|full]` | прогон golden | отчёт |
| `hound sensitivity` | пост-хок по осям | отчёт |
| `hound diff <run_a> <run_b>` | межцикловый diff (запрещён через major) | diff.md |
| `hound promote --validated-only` | preliminary → strict (переаттестация) | cells |

### E.2. UX-принципы

- **Гейты — явные действия:** `hound run` останавливается на G1–G4 и печатает, что требуется;
  продолжение — отдельная команда с решением человека. Никаких автоматических «approve».
- **Идемпотентность и resume:** повторный `hound run` продолжает с последней завершённой фазы;
  `--until` даёт пошаговый контроль.
- **Сухой прогон:** `--dry-run` показывает план/бюджеты без исполнения.
- **Машинночитаемый вывод:** `--json` для CI и интеграций.
- **Понятные ошибки с кодом инварианта:** например `I2b: parameter_reference требует exhaustive
  и scope_attested_by (param=..., product=...)` — ссылка на регламент.

### E.3. Композиция (wiring)

`cli/main.py` — единственное место сборки: выбирает адаптеры по `run_manifest`/CLI-флагам,
создаёт порты, инстанцирует `Runner`. Ручной DI без фреймворка (A1).

---

## F. Миграция текущего кода

| Шаг | Действие | Проверка |
|---|---|---|
| 1 | Расщепить `core/schemas.py` → `domain/{enums,models,invariants,r2,anchors}.py` | `tests/selfcheck.py` остаётся зелёным (импорты обновляются) |
| 2 | Расщепить `core/db.py` → `store/sqlite.py` (DDL) + `ports/store.py` (контракт) | DDL-тест + триггеры |
| 3 | Ввести `ports/*` (Protocol) и адаптеры-заглушки | импорт-граф CI (итерация 4) |
| 4 | Перенести проверки `Cell._enforce_invariants` в `domain/invariants.py` (оставить вызов из Pydantic) | selfcheck |

Миграция не ломает существующий публичный контракт `core.schemas`/`core.db` до полного перехода
(переходные re-export-шимы).

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

**Открытые вопросы к итерации 4:** формат импорт-графа CI; структура логирования; бэкап/
восстановление прогона; как считать `prompt`-шаблоны версиями. Отдельная корпусная БД —
**отвергнута** (S6, B.2; единая БД сохраняет FK).
