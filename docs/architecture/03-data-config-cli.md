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

Схема §4.4 перенесена из бывшего `core/db.py` (удалён в T1.3) в `store/sqlite.py`
(11 таблиц + `schema_meta`, индексы, триггеры I4/I6/I7/I13/I18). Константа `SCHEMA_VERSION`
= **`2.1.0`** (v2.1/A3) — версия **SQLite-схемы**, не регламента; колонка
`runs.settings_provenance_json` хранит провенанс настроек, а миграция additive/idempotent
(`ALTER TABLE ... ADD COLUMN` при отсутствии). Методы доступа (реализация `StorePort`):

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
    # чтение
    def iter_cells(run_id, state=None) -> Iterator[Cell]
    def iter_exhaustions(run_id) -> Iterator[ProtocolExhaustion]    # I3/§5: протоколы исчерпания
    def get_chunk(chunk_id) -> Chunk            # для валидации цитат
    def resolve_source_url(source_id) -> str    # I10/I15
    def read_manifest(run_id) -> RunManifest    # forward-compatible: settings_provenance/ranking_epsilon
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

**Разделение версий (§13/B-7, v2.1).** Межцикловый diff классифицирует причины изменения
манифеста по **классам полей**: `methodology_change` — смена major `methodology_version` либо поля
`METHODOLOGY_FIELDS` (`scoring_policy_version`, `status_dict_version`, `capability_policy`,
`unknown_policy`, `partial_score`); `execution_change` — расхождение поля `EXECUTION_FIELDS`
(`case_id`, `cohort_ref`, `index_build_id`, `embed_model_id`, `chunker_version`, `extractor_version`,
`lexical_engine_version`, `vector_store`, `retrieval_profile`, `retrieval_cutoff_K`,
`retrieval_params_json`, `llm_scenarios`). `DIFF_BOUNDARY_FIELDS` сохранён алиасом
`EXECUTION_FIELDS` (обратная совместимость). Дифф отказывает при обеих классах причин (§13).

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
ranking_epsilon: 0.0            # §6/B-1: порог неразличимости ранжирования (доля диапазона)
settings_provenance:            # A3: default|measured (+ experiment_id при measured)
  partial_score: {source: default}
```

Загрузчик `config/run_config.py` → `RunConfig` (домен) с валидацией (anchor ∈ products;
preliminary не совместим с capability_ranked — I13). `ranking_policy` — **запрошенный** режим
отображения (K6); вычисленный ранг определяется sensitivity (join `requested × computed = min`,
02 E.1). `ranking_epsilon` — калибруемый порог неразличимости пар (§6/B-1; `None` → 0.0).
`settings_provenance` фиксирует, какие настройки — дефолт, а какие получены **измерением**
(`source: default|measured`), — это делает «дефолт по предвидению» явным исключением, а не нормой
(A3). `products` — **проекция** `cohort.yaml` (один канонический источник состава);
линтер проверяет совпадение множеств `products` и `cohort.product_id` (S11).

### D.2. Загрузчики пакета (`pipeline/normalize.py`)

- `package.yaml` → `PackageManifest` (веса=100, уникальность id, C → baseline null).
- `catalog.csv` → `list[CatalogRow]` (showcase → rule упоминает величину; slug lowercase).
- `baselines.csv` → `list[BaselineRow]`.
- `cohort.yaml` → `Cohort` (ровно один якорь — §1.6).
- `golden.csv` → `list[GoldenRow]` (тир fast/full; страта ❌ обязательна для full).
- `domain_profiles.yaml` → `dict[str, DomainProfile]` — две независимые оси получения
  контента: `fetch: http|browser` и `extract: auto|html|document` (+ опц. `wait_for` для
  браузерного рендера). Ключ — `host` с опциональным `/path`; схема строгая
  (`extra="forbid"`: старые `render`/`index`/`spa` отвергаются). Резолв `FetchPlan` для
  каждого URL — **в pipeline** (`pipeline/fetch_plan.py`), не в адаптерах:

  ```yaml
  docs.example:          {fetch: http,    extract: html}
  app.example:           {fetch: browser, extract: html, wait_for: "() => document.readyState === 'complete'"}
  example.org/documents: {fetch: http,    extract: document}
  ```

  Матчинг: host (или родительский суффикс) + опциональный префикс пути; приоритет —
  длиннейший `path_key`, затем длиннейший `host_key`. Явный `--render-modes` по точному URL
  переопределяет **только** ось `fetch`, оставляя `extract`/`wait_for` из профиля.

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
  adapter: yandex               # провайдер поиска по умолчанию
  by_language:                  # переопределение по языку (география-прокси, I16)
    ru: yandex
    en: duckduckgo
crawl:                         # операционная политика HTTP-краулинга корпуса (local|split)
  user_agent: "coHound-crawler/0.1"   # None → дефолт HttpCrawler
  timeout_s: 30                # None → дефолт HttpCrawler (30 с)
  follow_redirects: true
```

Профиль `local` игнорирует `edge.*`. Секция `crawl` действует в обоих профилях: она
переопределяет операционные параметры лёгкого `HttpCrawler` (`user_agent`/`timeout_s`/
`follow_redirects`); при `None`/отсутствии поля берётся дефолт адаптера — обратная
совместимость профиля `local` без файла.

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

Загрузчик `config/deploy.py` → `DeployConfig`
(домен-нейтрален). `deploy_profile`, версии edge-компонентов (`/v1/capabilities`) и
`edge_schema_version` пишутся в `run_manifest.yaml` (R7). `edge/` (composition root Z1)
читает тот же файл для bind-адреса, аутентификации входящих (IDP) и экспорта аудита
(SIEM). Полные требования к прозрачности шва для IDP/DLP/IDS — `07` §8.

### D.6. Переменные окружения и внешние ключи

Единая точка правды по env-переменным конвейера. Секреты **не коммитятся**; шаблон без
значений — `.env.example` в корне. Ключи передаются через среду процесса (в `split` —
только в контейнере edge, см. ниже).

**Контуры (I18/07).** Эгрессионные ключи внешнего поиска (`YANDEX_*`, `GLM_SEARCH_API_KEY`,
`GOOGLE_*`, `OPEN_WEBSEARCH_*`, `DUCKDUCKGO_BACKEND`) в профиле `split` живут **только в
edge** (composition root Z1, `07` §4/§7); ядро ходит за кандидатами через `/v1/*`-шов и
этих переменных не читает. **Оценщик (Ш6, I18) веб-тула не имеет** и никакие поисковые
ключи не читает — он работает только по переданным чанкам корпуса. В профиле `local`
поиск/краул исполняются in-process, поэтому те же переменные доступны процессу ядра.

| Переменная | Назначение | Обязательность | Fallback | Потребитель |
|---|---|---|---|---|
| `COHOUND_LLM_BASE_URL` | базовый URL openai-совместимого LLM-сервера | опц. | `https://plusvibeapi.ru/v1` | `wiring.build_discovery_llm` (LLM-разведка `passport-scout`, Ш2) |
| `COHOUND_LLM_API_KEY` | ключ LLM-разведки | **обязателен для вызова** | `PLUSVIBEAPI_API_KEY` | `wiring.build_discovery_llm` |
| `PLUSVIBEAPI_API_KEY` | фолбэк ключа контура | опц. | — | `wiring.build_discovery_llm` |
| `COHOUND_LLM_MODEL` | `model_id` LLM-разведки | опц. | `deepseek-v4.1-flash:cxbc` | `wiring.build_discovery_llm` |
| `COHOUND_LLM_DECODER` | механизм декодирования (`none`/`json_object`/`json_schema`) | опц. | `json_object` | `wiring.build_discovery_llm` |
| `COHOUND_LLM_TIMEOUT` | таймаут LLM-запроса, с (>0) | опц. | `120.0` | `wiring.build_discovery_llm` |
| `COHOUND_LLM_MAX_TOKENS` | максимум токенов ответа (>0) | опц. | `4096` | `wiring.build_discovery_llm` |
| `COHOUND_SCOUT_CRAWLER` | краулер `passport-scout`: `http`\|`scrapy` | опц. | `http` | `wiring.build_scout_crawler` |
| `COHOUND_NO_REEXEC` | отключить автоперезапуск `hound.py` в проектной `.venv` | опц. | — | `hound.py` |
| `COHOUND_REEXEC` | предохранитель от зацикливания перезапуска (ставит `hound.py`) | служебная | — | `hound.py` |
| `YANDEX_SEARCH_API_KEY` | ключ Yandex Search API | **обязателен для адаптера** | — | `adapters.search_yandex` |
| `YANDEX_FOLDER_ID` | folder-id Yandex Cloud | **обязателен для адаптера** | — | `adapters.search_yandex` |
| `YANDEX_SEARCH_MODE` | режим поиска Yandex: `async`/`sync` | опц. | `async` | `adapters.search_yandex` |
| `GLM_SEARCH_API_KEY` | ключ GLM web-search | **обязателен для адаптера** | — | `adapters.search_glm` |
| `GOOGLE_API_KEY` | ключ Google CSE | **обязателен для адаптера** | `GOOGLE_SEARCH_API_KEY` | `adapters.search_google` |
| `GOOGLE_SEARCH_API_KEY` | альтернативное имя ключа Google CSE | опц. | — | `adapters.search_google` |
| `GOOGLE_CSE_ID` | идентификатор поискового движка (cx) | **обязателен для адаптера** | `GOOGLE_SEARCH_ENGINE_ID` | `adapters.search_google` |
| `GOOGLE_SEARCH_ENGINE_ID` | альтернативное имя cx | опц. | — | `adapters.search_google` |
| `DUCKDUCKGO_BACKEND` | движок библиотеки `ddgs` | опц. | `duckduckgo` | `adapters.search_duckduckgo` |
| `OPEN_WEBSEARCH_BIN` | путь/имя утилиты `open-websearch` | опц. | `open-websearch` | `adapters.search_openwebsearch` |
| `OPEN_WEBSEARCH_ENGINE` | движок утилиты | опц. | `duckduckgo` | `adapters.search_openwebsearch` |

Примечания:

- **Обязательность** ключей поиска проявляется в `hound discover`: без пары
  (`api_key`, `folder_id`/`cx`) адаптер считается недоступным, и кандидаты **не
  выдумываются** (I19) — команда возвращает `EXIT_ERROR` с явным сообщением.
- Флаги CLI переопределяют env: `passport-scout --llm-base-url/--llm-model/--llm-decoder`,
  `discover --search-adapter`.

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
  переопределение побеждает доменный профиль** по оси `fetch`; `extract`/`wait_for` при этом
  берутся из `domain_profiles.yaml` (D.2).
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
- `scrapy` + `playwright` (extra `crawl`) — браузерный рендер SPA/JS; `COHOUND_SCOUT_CRAWLER=scrapy`
  для `passport-scout`.

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
| `hound review` | очередь ревью / preliminary-стол | cells (reviewed) |
| `hound report` | сборка артефактов | outputs |
| `hound golden [--tier fast\|full]` | прогон golden | отчёт |
| `hound sensitivity` | пост-хок по осям | отчёт |
| `hound diff <run_a> <run_b>` | межцикловый diff (запрещён через major) | diff.md |
| `hound promote --validated-only` | preliminary → strict (переаттестация) | cells |
| `hound gate --gate G1..G4 --actor <вы>` | запись **человеческого решения** гейта G1–G4 (утверждение); гейт проходит фактом строки в `gate_decisions`, отказ — отсутствие строки (S9/I6/I7) | gate_decisions |
| `hound deploy-check` | диагностика профиля: достижимость DMZ-шва, версии/форматы edge (`/v1/health`, `/v1/capabilities`) | — (отчёт) |
| `hound doctor [--strict] [--with-selfcheck]` | самодиагностика инсталляции: runtime, зависимости по extras, env/ключи, конфиг, каталоги, CLI-поверхность | — (отчёт) |

Подкоманды `hound package …` принимают `--package` как **путь к каталогу пакета**, так и
**`case_id`** (резолвится в каталоге пакетов: `--dir`, иначе resolved `paths.packages`);
несуществующий пакет — честный `EXIT_NOT_FOUND` (команды чтения fail-closed, а не ложный успех).
`hound package validate` дополнительно fail-closed, когда каталог есть, но `package.yaml`
отсутствует: `EXIT_BLOCKED` с `blocking: package.yaml отсутствует` (не `EXIT_NOT_FOUND`
и не ложный `ok: yes`; терпимость к частичным пакетам с манифестом сохраняется).

`hound doctor` — read-only и по умолчанию **без сети**: проверяет Python/venv, наличие
зависимостей по группам extras (`find_spec`, без импорта тяжёлых), заданность env/ключей
(значения **не** печатаются), файлы `config/deploy.yaml`/`registries`/`.env.example`/`packages`,
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
  `--until` даёт пошаговый контроль.
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
