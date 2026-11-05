# coHound — архитектура: Итерация 3 — данные, конфигурация, CLI

> **Статус открытых вопросов на 2026-10-28** (HEAD `f6100c7`): 8 закрыто, 1 открыто.
> У каждого пункта в §F–§G маркер ✅/⬜/➖/❔ с доказательством.

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
    catalog.lock.csv         # зафиксированные авторские величины каталога (после фиксации)
    derived_fields.csv       # вычисленные поля каталога (production-экспорт)
    baselines.csv
    cohort.yaml              # §1.6
    golden.csv
    adversarial/
    domain_profiles.yaml     # оси получения контента + per-site лимиты (§D.2)
    crawl_sources.yaml       # ОПЦ.: source routing feed/channel/engine (§D.2.1) —
                             # сейчас нет ни в одном пакете, включая _template
    run.yaml                 # мандат прогона (§D.1); копия кладётся в runs/{run_id}/
    scope_attestation.yaml   # процедура аттестации полноты источника (I2b)
    registries/{registry_id}.yaml   # ОПЦ.: package-scoped конфиг фетчеров (§D.4);
                             # сейчас нет ни в одном пакете (шаблоны — docs/templates/registries/)
    sources/                 # авторские входы и операторские артефакты
      golden_status.yaml     # ОПЦ.: состояние эталона (`schema: golden_status/1`):
                             # absent | present | declined (+ reason/declined_by) —
                             # объявленное «сверять не с чем» вместо удаления golden.csv;
                             # exempt_products — waiver по продукту (эталона для него нет)
      seeds.yaml             # КАНОНИЧЕСКИЙ seed-набор (`schema: seeds/1`, §A.1.1);
                             # сейчас пакеты ещё на legacy corpus_urls.yaml
      corpus_urls.yaml       # legacy `{product_id: [url, …]}` (миграционный fallback)
      render_modes.yaml      # per-URL ось `fetch` (§D.6, runbook)
      scout/                 # кандидаты разведки пакета (`hound package scout`)
      crawl_probe/           # предложения `hound package crawl-probe`
  passports/{product_id}/{YYYY-MM-DD}.yaml
  docs/status-dictionary.yaml
  docs/architecture/*.md
  runs/{run_id}/             # пакет прогона (см. A.2)           ┐
  index_store/{package}/{build}/{product_id}/    # сборки (I17)  │
  corpus_raw/{product_id}/{doc_hash}.bin         # сырые байты   │ операционные:
  cache/serp/{query_hash}.json                   # кэш SERP (R7) │ по умолчанию живут
  cache/http/{url_hash}.bin                      # кэш HTTP      │ в репозитории, при
  extract_cache/             # кэш экстракции                      │ настроенной раскладке —
  eval_cache/                # кэш оценщика (Ш6)                   │ вне его (§A.1.2)
  embed_cache/{model}/{h2}/{content_hash}.vec  # кэш векторов (Ш5) │
  state/{package_id}/state.db  # durable-состояние продукта (09 §4.1)│
  reports/{run_id}/          # (опц.) публикуемые артефакты        ┘
  tests/
```

Операционные каталоги (`packages/`, `passports/`, `runs/`, `index_store/`, `corpus_raw/`,
`cache/`, `extract_cache/`, `eval_cache/`, `embed_cache/`, `reports/`, `state/`) переопределяются
без правки
кода: `config/paths.yaml` (операторский файл; в репозитории только образец
`config/paths.example.yaml`), env `COHOUND_<FIELD>_DIR`/`COHOUND_HOME` (см. §D.6) или CLI-флаг;
приоритет — CLI > env > `paths.yaml` > дефолт (`core/config/paths.py`).

#### A.1.2. Артефакты вне репозитория (раскладка хоста)

`runs/`, `index_store/`, `corpus_raw/`, `cache/`, `extract_cache/`, `eval_cache/`,
`embed_cache/`, `state/` — **не файлы проекта**: это данные прогонов, кэши и durable-состояние.
Держать их в дереве репозитория не обязательно, и на рабочем хосте они унесены под общий
корень `~/.cohound` (12 GB на 30.10.2026, из них `corpus_raw` 5.3 и `cache` 5.2).

```
~/.cohound/
  paths.yaml                 # раскладка ЭТОГО хоста (вне git: пути машинно-специфичны)
  env.sh                     # артефакты инструментов (mypy/ruff/coverage/hypothesis/pycache)
  data/                      # home: → runs/ index_store/ corpus_raw/ cache/ extract_cache/
                             #   eval_cache/ embed_cache/ state/ reports/
  tool/                      # .mypy_cache .ruff_cache .hypothesis .coverage .pytest_cache
```

Подключение — двумя строками в `./.env` (файл в `.gitignore`, шаблон — `.env.example`):
`COHOUND_HOME=~/.cohound/data` и `COHOUND_PATHS_FILE=~/.cohound/paths.yaml`.

**Ловушка, которую надо знать.** `COHOUND_HOME` сдвигает **все 11** полей
`HARNESS_FIELDS` (`core/config/paths.py:with_home`), включая `packages` и `passports`. Без
явного закрепления CLI искал бы пакеты в `~/.cohound/data/packages` и не находил бы ничего.
Поэтому в `paths.yaml` эти два поля задаются **абсолютными** путями к репозиторию:
абсолютный путь `with_home` не трогает. Это проверяется `hound doctor` (группа `layout`
печатает эффективный путь **и источник** — `file`/`env`/`home`/`default`), а не глазами.

**Тесты раскладку не читают.** `tests/conftest.py` ставит `COHOUND_NO_DOTENV=1`, пути в
`tests/config/test_paths.py` резолвятся с `env={}`: прогон набора не зависит от окружения
хоста. Единственное исключение — проверка на **живом** прогоне
(`tests/pipeline/test_publishability.py`): она берёт каталог из явного
`COHOUND_LIVE_RUNS_DIR` и без него скипается, а не ищет `ROOT/runs` (иначе «живой прогон
проверен» было бы неправдой у всех, кто настроил раскладку).

#### A.1.1. Seed-набор и onboarding (блокирует прогон)

Канонический seed-артефакт пакета — **`sources/seeds.yaml`** (`core/config/seeds.py`,
`schema: seeds/1`): на продукт — записи с `url`/`role`/`profile_key`/`enabled`/`state`.
Legacy-формат `sources/corpus_urls.yaml` (`{product_id: [url, …]}`) читается как fallback;
миграция — `ensure_seeds_file` (не перезаписывает существующий файл, переносит legacy-записи
как `role=primary`, `source=migration`, сохраняя порядок). Сегодня оба пакета ещё живут на
`corpus_urls.yaml`.
Приоритет источника корпуса (`load_effective_seeds`, `core/pipeline/onboarding.py`):

1. `--urls FILE` — **полностью заменяет** пакетный набор для всех продуктов (warning
   `seeds_operator_override`); продукт без записи получает пустой набор и блокируется;
2. `sources/seeds.yaml[product]`;
3. `sources/corpus_urls.yaml[product]` (legacy, warning `seeds_legacy_fallback:<id>`).

**Порядок корней значим — и он входит в идентичность.** Корни продукта дедуплицируются с
сохранением **авторского** порядка (`_ordered_unique`, `core/pipeline/corpus.py`), а не
сортируются: корень, идущий последним, может не выполниться вовсе (живой прогон 22.10:
800-страничный портал съел бюджет, docs-корень не дренировался). Поэтому первым ставьте
корень, который важнее. `seeds_digest` канонизирует записи по `(product_id, order)`
(`core/pipeline/digests.py`): **перестановка корней внутри продукта меняет `run_id`** —
иначе один `run_id` описывал бы два разных корпуса. Порядок **продуктов** в файле, наоборот,
не значим (сортировка по `product_id`).

**Onboarding-префлайт (O1) блокирует прогон** до краула: продукт без активного seed'а или
с несвязанным профилем попадает в `blocked` с причиной `no_seeds` / `profile_unbound` /
`seed_invalid` (`onboarding.preflight`, `core/cli/commands/run.py`; прогон
останавливается на `stopped_at: onboarding`). Заблокированный прогон **виден**: БД прогона и
снимок входов (`run_inputs`) пишутся — «молча ничего не произошло» не бывает; а вот
durable-состояние пакета `state/{package_id}/state.db` **не** создаётся (заблокированный
прогон не открывает запись состояния). `--only-product`
**сужает** префлайт: заблокированный продукт вне области частичного прогона не
останавливает его; полный прогон блокируется любым неготовым продуктом когорты.
Префлайт исполняется между фазами `plan` и `g1` — `--until plan` его **пропускает**.

### A.2. Пакет прогона `runs/{run_id}/`

```
runs/{run_id}/
  run.yaml                   # вход (копия, иммутабельна)
  run_manifest.yaml          # §9: все версии, модели, index_build, профиль+K, бюджет
                             # (выгружается фазой report; источник истины — строка манифеста в cohound.db)
  cohound.db                 # SQLite (операционные данные прогона) + WAL
  plan_inputs.json           # производный экспорт снимка плановых входов (источник истины — run_inputs в cohound.db)
  gates/{gate}-request.json  # G3/G4: детерминированный предмет решения + subject_digest (точка интеграции)
  logs/{phase}.jsonl         # структурированные логи по фазам
  prompts/{prompt_hash}.json # аудит **исполненных** промптов (V7): ровно то, что ушло в модель.
                             # Пишется `PromptArchive` (фаза Ш6 и пере-оценка), один файл на
                             # `prompt_hash` — то есть стоимость пропорциональна числу различных
                             # входов, а не числу ячеек. Сбой записи виден в `PhaseResult.errors`
                             # (`prompt_save_failed`) и не роняет фазу: аудит — след измерения,
                             # а не его условие. Расхождение содержимого при совпавшем хеше —
                             # `prompt_hash_conflict` (формула хеша перестала покрывать вход).
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
(18 предметных таблиц + `schema_meta`, индексы, триггеры I4/I6/I7/I13/I18). Константа `SCHEMA_VERSION`
= **`3.14.0`** (3.14.0 — гейт версии вместо аддитивных шимов — волна S плана 17; 3.13.0 — свидетельство живости сеанса краула — `crawl_sessions.owner`/`heartbeat_at`,
`interrupted` в `crawl_session_status`; B3: спуск в контейнеры — `corpus_members`/`archive_observations`
(PK `(archive_content_hash, member_path, product_id)` с 3.13.0 — провенанс члена не перезаписывается
другим продуктом); E2: дайджест спецификаций реестров; D3/D4/сеансы/провенанс краула/P1-самодостаточность; gates: `gate_decisions` с предметом решения) — версия **SQLite-схемы**, не регламента; колонки
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

**Гейт версии схемы (v3.12.0, `11` §2.6).** Аддитивность не означает «читать чужую схему можно»:
`connect()` сверяет `schema_meta.schema_version` **до** DDL и миграций и при несовпадении
поднимает `SchemaVersionMismatch` (`core/store/sqlite.py`) — CLI печатает
`schema_version: схема БД несовместима: … код ожидает '3.14.0' …` и возвращает `EXIT_USAGE`,
вместо трейсбека или молчаливого апгрейда. Прогон переносится **пересозданием** каталога прогона,
а не чтением старой БД: аддитивные дельты сделали бы «старую» строку неотличимой от «новой».
Поэтому `_ensure_columns` теперь страховка от «полусозданной» БД **в пределах** объявленной
версии, а не путь миграции старой.

**Свидетельство живости (v3.12.0, `10` §Д-6).** `crawl_sessions.owner` (`host/pid/старт-процесса`)
и `crawl_sessions.heartbeat_at` — **не** блокировка и не lease (SQLite — локальный одно-писательный
файл, распределённая аренда на нём невозможна, `10` §9 К-4). Это два сигнала для recovery-on-open
(`core/pipeline/liveness.py`): живой `owner` на **этом** хосте сильнее heartbeat (pid переиспользован
— сверяется момент старта), а при неизвестном владельце (чужой хост/нет метки) применяется порог
`SESSION_STALE_AFTER_S` = 600 s (10 интервалов `HEARTBEAT_INTERVAL_S` = 60 s, ≈4× замеренного
потолка одной экстракции — `10` §9.1). Протухший `running` становится `interrupted` с причиной в
`error` (`owner_dead`/`no_heartbeat`/`bad_heartbeat`/`stale_heartbeat`), `provenance_json` не
пишется. Heartbeat обновляется точечно (старт сеанса, запись исхода URL, каждая граница шага
дренажа) — отдельного потока нет намеренно: признак жизни обязан исчезать вместе с процессом.
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

#### C.1.1. Что входит в `run_id` (форкает ли прогон идентичность)

`run_id` — детерминированная функция **декларированных** входов
(`derive_run_id`, `core/pipeline/plan.py`; материал склеивается `\x1f`, результат
`{case_id}-{short_hash(material, 12)}`). Порядок полей фиксирован:

| Поле материала | Откуда | Что его меняет (CLI) |
|---|---|---|
| `case_id` | `run.yaml:case_id` | правка `run.yaml` |
| `package` | `run.yaml:package` | правка `run.yaml` |
| `anchor` | `run.yaml:anchor` | правка `run.yaml` (смена якоря — новый прогон) |
| `cycle_id` | `--cycle-id` > `run.yaml:cycle_id` > `snapshot_date` | `--cycle-id`, `--update` |
| `cohort_digest` | состав + `cohort_ref` + `frozen_at` когорты (`digests.cohort_digest`) | правка `cohort.yaml`/`cohort_ref` |
| `seeds_digest` | эффективный seed-набор, включая **порядок** корней внутри продукта (`digests.seeds_digest`) | **`--urls FILE`** (полная замена набора), правка `seeds.yaml`/`corpus_urls.yaml`, перестановка корней |
| `execution_contract_digest` | отпечаток осей контракта (`digests.execution_contract_digest`, §C.1 выше) | смена любой оси контракта: `--llm-model`, `--llm-slot`, `--retrieval-profile`, `--cutoff-k`, `--document-level`, `--profile`, `run.yaml:crawl.*` |
| `run_scope` | `full` \| `only_failed` \| `product:<id>` | **`--only-failed`**, **`--only-product ID`** |
| `refresh_policy` | `auto`\|`revalidate`\|`refresh`\|`offline` | **`--refresh MODE`** |
| `focus` | `run.yaml:focus` (модули и параметры, отсортированные) | правка `run.yaml:focus` |

Практические следствия, которые оператор обязан знать:

- **`--urls` форкает прогон** (меняется `seeds_digest`), даже если это «тот же» корпус в
  другом файле: путь файла не входит, а состав записей — входит.
- **`--refresh` форкает прогон**: `auto` и `revalidate` — разные `run_id` одного цикла.
- **`--only-product`/`--only-failed` форкают прогон**: частичный прогон — **другой прогон**,
  а не resume полного (`run_scope`). Догоняющий прогон остатка — отдельный `run_id` со своим
  манифестом и своей историей; межцикловый `diff` между частичным и полным прогонами
  некорректен по построению.
- **URL-фильтр применяется позже вывода `run_id`** (`core/cli/commands/run.py`): `run_id`
  считается до `_filter_corpus_urls`, поэтому идентичность меняет именно `run_scope`
  (объявленный объём), а не фактическая фильтрация записей. `--urls` — исключение: он
  меняет сам вход (`seeds_digest`).
- `snapshot_date` и `sorted(products)` в материал **не входят**: цикл задан `cycle_id`,
  состав когорты — `cohort_digest`.
- Env-переменные в `run_id` не входят вообще (в т.ч. `COHOUND_LLM_*`): два хоста с одним
  `run.yaml` обязаны получить один `run_id`.
- `--run-id` переопределяет вывод целиком (аварийный/миграционный путь): ответственность за
  коллизию идентичностей — на операторе.

**`lexical_engine_version` заполняется фактически.** Composition root передаёт в
`NpyIndexBuilder` версию реально используемого лексического движка (`tantivy-<ver>`; пусто —
лексического плеча в прогоне нет, R0/R2 пропускаются), и она входит в `partition_build_id`
(§13:676) и в `index_manifest`. Ранее поле всегда было пустым (дефект провенанса: читалось у
сборщика, у которого атрибута не было), поэтому `pb` не различал лексические движки. В
контракт `execution_contract_digest` эта величина **не** входит (методология 2.4.0): она
наблюдаемая, её смена видна через `pb`/`index_build_id`, а не через границу diff.

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
`crawl_extract_workers`, `crawl_extract_max_tasks_per_child`, `crawl_max_response_bytes`,
`crawl_document_level`, `crawl_config_digest`, `registry_spec_digest`) остаётся перечнем полей для **legacy-пар**
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

- Смена модели под сценарий (I21) → **`execution_change`** (`execution_versions`, §13): меняется
  `llm_scenarios` ∈ контракта → новый `run_id`; `methodology_version` **не** меняется
  (`core/config/deploy.py`). Обязателен `golden-full` (условие измерения, AGENTS.md §Git-конвенции).
  Значение модели — провизионные данные, финальный выбор даёт
  `hound experiment evaluator-benchmark` (A3).
- Смена когорты после G1 → новый цикл (межцикловый diff запрещён).
- Смена `rule`/`absence_rule` параметра → bump `catalog_version` + golden.
- Смена контента/входов сборки (`chunker`/`embedder`/`extractor`/`lexical`/
  `vector_store`/`snapshot_date`) → новая контент-адресуемая `index_build_id` + golden-full. Смена
  `embedder`/`extractor`/`lexical`/`vector_store` — **не** граница diff (2.4.0): наблюдаемые
  величины не входят в контракт, diff проходит как `content_change`; `chunker_version` и
  `extractor_stack` — входят и блокируют.
- Смена `retrieval_profile`/K и fusion-параметров (`rrf_k`, `retrieval_params_json`) →
  retrieval-golden и `execution_change` в diff, но **не** меняет `index_build_id`:
  ретривал-конфиг не влияет на чанкинг/векторы (P2).
- Смена операционных осей краула → `execution_change` в diff (§13, Backlog B/D1/N6), без bump
  `methodology_version`: commodity-слой exec-условий (A2). Классы осей **различаются**:

  | класс | оси | где ловится |
  |---|---|---|
  | (а) `CrawlLimits` | `max_workers`, `max_depth`, `max_documents`, `exclude_paths`, `exclude_documents`, `document_level`, а также `revalidate_max_documents`, `max_response_bytes`, `max_artifact_bytes`, `attachment_formats`, `archive_max_*`, `archive_descent`, `document_timeout_s` (последние — **только когда заданы явно**) | `crawl_config_digest` — по каждому site-key + `__default__`, т.е. **все слои** (app/run/per-site); плюс индивидуальные `crawl_*` поля run-слоя. Класс — `execution_change` |
  | (а′) расписание экстракции: `extract_workers`, `extract_max_tasks_per_child` | **не** входят в контракт (F0, `11` §2.10): параллелизм извлечения не может изменить состав корпуса — документ уже добыт и ретенирован, чанки — детерминированная функция его текста и уровня. В отличие от `max_workers`, конкурентность fetch взаимодействует с усечением `max_documents` и потому **остаётся** осью контракта | наблюдённый факт: `metrics.json.resources.scheduling` (`source`: `declared`\|`derived`). Резолвнутое значение публикуется, но `run_id` не меняет — иначе выведенное из бюджета хоста окно делало бы `run_id` функцией машины |
  | (б) `crawl.strategy` / `crawl.rate_limit` | только run-слой (`run.yaml:crawl`); per-site `strategy` из `domain_profiles.yaml` идентичность **не** меняет | отдельные поля контракта `crawl_strategy`/`crawl_rate_limit` (`core/cli/execution_axes.py`); класс — `execution_change` |
  | (в) `crawl.robots` | включение/выключение `robots.txt` — **app-слой** (`deploy.yaml:crawl.robots`; в `run.yaml:crawl` поля нет) | входит в `crawl_config_digest` **всегда** (как `max_depth`), класс — `execution_change` (D-5): гейт `robots.txt` решает, какие документы вообще допущены, т.е. это условие измерения, а не рычаг пропускной способности. Пробел закрыт: до правки D-5 ось меняла поведение, но не входила ни в один дайджест |
  | (г) `crawl.user_agent`/`timeout_s`/`follow_redirects`/`crawl.browser.*` | ни идентичности, ни `diff` | — (операционные данные транспорта) |
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
cohort_ref: cohort-pg-2026q2-r2    # опц.: авторская ссылка на состав когорты → входит в cohort_digest
products: [pangolin, alt1, alt2, ...]
package: pg-core
methodology_version: 0.1.0        # опц. (дефолт — пин проекта); входит в контракт, НЕ в run_id-материал напрямую
cycle_id: 2026-Q2                 # опц.: ISO-дата/ISO-неделя; None → snapshot_date. ВХОДИТ в run_id
refresh_policy: auto              # опц.: auto|revalidate|refresh|offline; None → auto. ВХОДИТ в run_id
llm_slot: cheap                   # опц.: cheap|flagship; активный слот оценщика → в llm_scenarios (контракт)
focus: { modules: [storage, backup], params: [pg_backup_pitr] }   # ВХОДИТ в run_id
languages: [ru, en]
snapshot_date: 2026-06-01
mode: strict
ranking_policy: capability_ranked
ranking_epsilon: 0.0            # §6/B-1: порог неразличимости ранжирования (доля диапазона)
settings_provenance:            # A3: default|measured (+ experiment_id при measured, + skipped)
  partial_score: {source: default}
gates:                          # опц.: политика G3/G4; процесс, НЕ условие измерения (не в run_id)
  g3: external                  # external (дефолт) | waived
  g4: external
serp:                           # опц.: SERP-контур (I3/§5); ВХОДИТ в контракт (serp_rounds, serp_mode) ⇒ run_id
  rounds: 0                     # число раундов внешнего поиска до дренажа корпуса (0 — выключен)
  budget: 5                     # обязателен при rounds > 0: запросов на продукт на раунд
  mode: async                   # опц.: sync | async (дефолт async). Режим = какой бэкенд
                                # провайдера отвечает, значит состав находок ⇒ ось контракта.
                                # Env YANDEX_SEARCH_MODE читает ТОЛЬКО разведка, не конвейер.
crawl:                          # §3.2/D4: дефолты оси стратегии краула и politeness-пейсинга
  strategy: auto                # auto | honest | hybrid (как используется браузер)
  rate_limit: {min_s: 1.0, max_s: 3.0}   # опц.: uniform(min_s, max_s) между запросами (per-host)
  max_workers: 4                # опц. (>0): D4-пул потоков на сетевой fetch (None/1 = inline)
  extract_workers: null         # опц. (>0): P4-пул экстракции (local — процессы, split —
                                # потоки; None/1 = inline). Память: W × footprint(level)
  extract_max_tasks_per_child: null   # опц. (>=1): рециклинг воркер-процессов экстракции
  document_level: full          # опц.: P5-уровень извлечения документов
                                # (off|text|layout|layout_tables|full; дефолт full)
  pdf_pipeline: structured      # опц.: P7-механизм извлечения PDF (structured|text_layer)
  pdf_device: cpu               # опц.: P9-устройство моделей PDF (cpu|cuda; дефолт cpu);
                                # cuda без устройства — отказ, а не тихий CPU
  max_depth: 2                  # опц. (>=0): глубина BFS-обхода in-site ссылок (0 — только seeds)
  max_documents: 500            # опц. (>0): потолок добытых страниц на (продукт, seed-root)
  revalidate_max_documents: null      # опц. (>=1): потолок известных URL ревалидации на продукт
  exclude_paths: ["/private/*"] # опц.: glob блэклиста по пути URL (fnmatchcase)
  exclude_documents: ["*.pdf"]  # опц.: glob блэклиста по basename URL
  # --- спуск в контейнеры (P6/B3) — см. §D.1.1 ---
  attachment_formats: [zip, 7z, rar]          # опц.: позитивная вселенная контейнеров
  archive_descent: full                       # опц.: full (спуск исполняется) | plan (только листинг)
  archive_max_member_bytes: 67108864          # опц.: потолок ОДНОГО члена (64 MiB)
  archive_max_uncompressed_bytes: 268435456   # опц.: суммарно распакованное ОДНОГО контейнера (256 MiB)
  archive_max_members: null                   # опц.: число членов контейнера (None — без потолка)
  archive_max_total_uncompressed_bytes: 1073741824   # опц.: КУМУЛЯТИВНО на корень спуска (1 GiB)
```

Загрузчик `core/config/run_config.py` → `RunConfig` (домен) с валидацией (anchor ∈ products;
preliminary не совместим с capability_ranked — I13). `ranking_policy` — **запрошенный** режим
отображения (K6); вычисленный ранг определяется sensitivity (join `requested × computed = min`,
02 E.1). `ranking_epsilon` — калибруемый порог неразличимости пар (§6/B-1; `None` → 0.0).
`settings_provenance` фиксирует, какие настройки — дефолт, а какие получены **измерением**
(`source: default|measured`), — это делает «дефолт по предвидению» явным исключением, а не нормой
(A3). `products` — **проекция** `cohort.yaml` (один канонический источник состава);
линтер проверяет совпадение множеств `products` и `cohort.product_id` (S11).

**Поля `RunConfig`, влияющие на идентичность.** `cycle_id`/`cohort_ref`/`refresh_policy`/
`focus`/`llm_slot` — не «косметика»: они входят в материал `run_id` (§C.1.1) прямо
(`cycle_id`, `refresh_policy`, `focus`, `cohort_digest`) или через контракт (`llm_slot` →
`llm_scenarios`). `methodology_version` входит в контракт, но `run_id`-материал напрямую не
содержит: он свёрнут в `execution_contract_digest`. Секция `gates` — **процесс**, не условие
измерения: `run_id` от неё не зависит (§13).

#### D.1.0. SERP-контур: внешний поиск как источник документов корпуса (I3/§5)

Секция `serp` включает **второй проход по объявленным хостам**: фаза корпуса (Ш5) до
дренажа выпускает запросы «каталог + имя продукта» во внешний поиск, допускает
найденные URL по правилам scope/блэклиста и ставит их depth-0 seed'ами **уже
объявленных** корней. Новых корней и новых продуктов поиск не заводит: иначе
`seeds_declared`, `max_documents` и само число корней описывали бы результат
ранжирования провайдера (A2/I16). Найденный документ проходит обычный путь
`corpus_raw → hash → index` (I19), поэтому отдельной фазы в `PHASES` нет.

**Три уровня переключения** (приоритет сверху вниз, первый задавший побеждает):

| Уровень | Ключ | Провенанс |
|---|---|---|
| CLI | `--serp-rounds N` | `source: cli` |
| окружение | `COHOUND_SERP=N` (или `off`/`0`) | `source: env` |
| вход прогона | `run.yaml: serp.rounds` | `source: run.yaml` |
| профиль | `deploy.yaml: search.serp.rounds` | `source: deploy.yaml` |
| дефолт | `0` (выключен) | `source: default` |

Наблюдённые раунды входят в `EXECUTION_CONTRACT_FIELDS` (`serp_rounds`): раунд меняет
состав корпуса, то есть условие измерения, поэтому его смена — `execution_change`
(новый `run_id`), а не правка текущего прогона. Уровень-источник пишется в провенанс
(`rounds.source`), чтобы «поиск не работал» было отличимо от «поиск не объявлен».

**Режим запроса — отдельная ось, а не настройка хоста.** `serp.mode` (`sync` | `async`)
объявляется во входе прогона и входит в контракт как `serp_mode`. Причина та же, что у
раундов: `sync` — один POST `/v2/web/search`, `async` — запуск операции
`/v2/web/searchAsync` и опрос, то есть **другой бэкенд провайдера**, а значит другой
состав находок и другой корпус. Замер 28.10: `sync` отвечает за 6.9 с медиана (среднее
8.4 с) при 10 кандидатах, `async` — за 180 с при `poll_interval=30` (объявленный минимум
обработки — 5 мин) и отвечает `search API backends are overloaded` там, где `sync`
работает. Env `YANDEX_SEARCH_MODE` конвейером **не читается** — её читает разведка (её
выход — кандидаты, не evidence); попытка вывести условие измерения из окружения дала бы
`run_id`, не описывающий, чем снят корпус (§7.4/I21). Провенанс хранит **пару**
`mode.declared`/`mode.used`: объявленный — условие, фактический — свойство собранного
адаптера, и их расхождение видно, а не выводится догадкой.

**Бюджет — один.** `serp.budget` (запросов на продукт на раунд) объявляется **только** во
входе прогона: он определяет, сколько запросов реально уйдёт, то есть стоимость и состав
корпуса. `rounds > 0` без `budget` — отказ `EXIT_USAGE` (код `serp`), а не молчаливый
безлимит (прежний `search.budget: {ru: …, en: …}` удалён: он ключевался по языку, тогда
как потребитель (`SearchBudget`) ключуется по фазе, и не ограничивал ничего).

**Видимый отказ вместо тихого пропуска.** Если раунды объявлены, а адаптер внешнего поиска
не собран (нет провайдера/ключа) — фаза пишет ошибку `serp: объявлены раунды, но адаптер
внешнего поиска не подключён`, а провенанс контура несёт `unavailable_reason: no_adapter`.
Выключенный контур тоже пишет наблюдение (`enabled: false`, `rounds.source`) — отсутствие
записи было бы неотличимо от «контур был, но ничего не нашёл».

**Что видно в прогоне.** Наблюдение контура (`rounds`/`budget`/`mode`/`queries`/`query_outcomes`/
`candidates`/`accepted`/`rejected`/`errors`/`termination`/`ingest`/`timing`/`yield`) пишется в
провенанс сеанса краула (`crawl_sessions.provenance_json`, ключ `serp`), сводка — в
`runs.search_effort_budget_json` (колонка читалась отчётом, но до этого не писалась).
`ingest` отделяет «нашли» от «вошло в корпус»: принятый кандидат может быть зачёркнут
блэклистом или не поместиться в `max_documents` корня.

**Доходность названа знаменателем, а не числом принятых.** `yield` несёт
`issued`/`served`/`cached`/`with_results`/`no_results`/`errors`/`budget_refused`/`candidates`/
`accepted` и два отношения — `accepted_per_served` и `accepted_per_candidate`. Отказ бюджета
означает «вызова не было» и в знаменатель **не** входит; сбой — отдельная строка, поэтому
прогон со 100 % сбоев адаптера больше не читается как «поиск ничего не нашёл». Отношение при
нулевом знаменателе — `None`, а не доля 0: «нечего делить» и «поделили и получили ноль» —
разные наблюдения.

**`query_outcomes` — исход каждого выпущенного запроса** (`results`/`no_results`/`error`/
`budget_exhausted`), записанный **до** любых `continue`: иначе знаменатель считал бы только
результативные запросы. Текст сбоя (`type: message` с потолком длины) отличает «провайдер
отказал после 5 попыток» от «ничего не нашлось»; заголовки и ключи в него не попадают
(это гарантирует `core/util/retry.py`), но длину всё равно ограничиваем: провенанс не лог.

**Арифметика раундов обязана сходиться:**
`rounds.executed + rounds.not_executed == rounds.declared`. `not_executed_reason` принимает
два токена: `no_query_variation` («спрашивать было бы нечего» — раунды выпустили бы те же
запросы) и `early_termination` («спрашивать было бы не за чем» — выдача/бюджет/сбой уже
терминальны). До правки ранний выход писал `not_executed: 0`, и наблюдение утверждало, что
объявленные раунды исполнились все.

**`timing` — стоимость ожидания как факт, а не как ощущение.** Контур последовательный
(async-провайдер с опросом), поэтому «медленно» — не диагноз. Наблюдение несёт
`{measured, wait_seconds, request_seconds, polls, from_cache}` по продукту:
`wait_seconds` — ожидание внешней системы (пауза опроса), `request_seconds` — сам обмен
(backoff повторов входит **сюда**, а не в ожидание). `measured=False` означает «адаптер не
наблюдал», а не «ноль»: `merged_with` складывает величины конъюнкцией по `measured`, чтобы
неизмеренное не превратилось в ноль. Ноль ожидания — тоже факт и печатается.

#### D.1.1. Спуск в контейнеры (`crawl.archive_*`, P6/B3)

Контейнер (`zip`/`7z`/`rar` из `crawl.attachment_formats`) — терминальный артефакт: его
байты не продолжают навигацию, а служат входом для спуска. Оси резолвятся **той же**
цепочкой `app < run < site`, что и остальные лимиты, и исполняет их
`pipeline/archive_descent.py` за портом `ContainerPort`.

| Ось | Дефолт | Что ограничивает |
|---|---|---|
| `attachment_formats` | `zip`/`7z`/`rar` | позитивная вселенная: сужение делает незаявленный контейнер обычным бинарным артефактом |
| `archive_max_member_bytes` | 64 MiB | распакованный размер **одного члена** |
| `archive_max_uncompressed_bytes` | 256 MiB | суммарно распакованное **одного контейнера** |
| `archive_max_members` | `None` (без потолка) | число членов одного контейнера |
| `archive_max_total_uncompressed_bytes` | **1 GiB** | **кумулятивно** на один **корень спуска** (все уровни вложенности) |
| `archive_descent` | `full` | `full` — члены читаются и ингестятся; `plan` — только листинг по индексу (члены не ингестятся, провенанс усечения виден) |

Потолка **глубины** спуска нет: конечность даёт цикл по хешу содержимого (`archive_cycle`) и
кумулятивный бюджет корня. Исчерпание бюджета — видимая запись `budget_exhausted`
(`archive_observations`, поле `outcome.stopped_reason`), а не тихое завершение; нечитаемый
индекс — `unreadable` с причиной (`backend_unavailable: …`). Запись `archive_observations`
пишется **всегда** — «контейнер без членов» так же виден, как «контейнер с десятью».

Все оси `archive_*` (кроме `archive_max_members`-дефолта) попадают в `crawl_config_digest`
**только когда заданы явно**: прогон без них байт-идентичен прежнему (§13, обратная
совместимость). Класс изменения — `execution_change`.

Секция `crawl` — дефолты **оси стратегии краула** `CrawlStrategy` (`auto`|`honest`|`hybrid`:
*как* используется браузер при browser-capable адаптере) и per-host диапазона задержек
`RateLimitSpec` (`uniform(min_s, max_s)`, `min_s <= max_s`). При отсутствии секции поведение
неизменно (`auto`, без пейсинга); резолв — `pipeline/fetch_plan.py` (дефолт прогона →
профиль домена → явный `--render-modes`).

Секция `crawl` несёт также **оси контроля краула** (`max_workers`/`extract_workers`/
`extract_max_tasks_per_child`/`max_depth`/`max_documents`/`revalidate_max_documents`/
`exclude_paths`/`exclude_documents`/`document_level`/`max_response_bytes`/`max_artifact_bytes`/
`attachment_formats`/`archive_*`), разрешаемые
**отдельной** цепочкой `deploy.yaml:crawl` (application default) < `run.yaml:crawl`
(run profile) < `domain_profiles.yaml` (per-site) —
`pipeline/crawl_frontier.py:resolve_crawl_limits`. `crawl.robots` — **только app-слой**
(`deploy.yaml`): в `run.yaml:crawl` этого поля нет (`extra="forbid"` отвергнет). Это **не** оси `crawl_sources.yaml` и
**не** `FetchPlan`: отдельный канал, чтобы не сломать приоритет source-routing. Две
**независимые** оси конкурентности: `max_workers` — пул потоков на **сетевой fetch**
(локальный путь; «у одной оси не может быть двух смыслов», A2/P3), а `extract_workers`
(P4) — пул **экстракции** (процессы на `local`, потоки на `split`): «в полёте» не более
`W` экстракций, доводка строго FIFO; байты страниц через границу процессов не пиклятся —
воркер перечитывает retained-`raw_path` (I19). Обе оси — `int | None`: `None`/`1` ⇒ пул
не создаётся, исполнение inline (байт-в-байт сегодняшнее). `extract_workers` не входит в
`crawl_config_digest` (F0, `11` §2.10): расписание экстракции не меняет состав корпуса, а
выведенное из бюджета хоста окно сделало бы `run_id` функцией машины; резолвнутое значение
публикуется в `metrics.json.resources.scheduling`. `max_depth` дефолт `0` (только
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
Класс изменения — **`execution_change`** (§13), не `methodology_change`; дефолт контент не
меняет (кроме churn `index_build_id`/`crawl_config_digest`). Идентичность форкается
**раньше** контентного следствия: `document_level` входит в `CrawlLimits.as_dict()`
**безусловно** (дефолт `full` присутствует всегда), поэтому `crawl_config_digest` →
`execution_contract_digest` → новый `run_id`; новая сборка (`partition_build_id`) —
вторичный эффект уже разошедшейся идентичности, а не основание класса.

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

**`pdf_pipeline` (P7) — механизм извлечения PDF.** Ось `crawl.pdf_pipeline`
(`structured` — дефолт, `text_layer`) объявляет **механизм**, а не библиотеку, и едет той же
цепочкой `app < run < site`; операторский вход — `--pdf-pipeline`
(`run`, `crawl run|resume|restart`), он применяется **поверх** разрешённой цепочки и потому
входит в `crawl_config_digest` → `run_id` (**`execution_change`**, §13): другой механизм —
другой текст документа, то есть другой корпус. `structured` = сегодняшнее поведение
(docling, лестница `document_level` действует); `text_layer` — только текстовый слой pdfium
(`core/adapters/extract_pdf_text.py`): без чтения порядка, таблиц и OCR, поэтому **уровней у
него нет** и `document_level` к нему не применяется (объявлять несуществующий уровень =
ложный провенанс). Механизм входит в наблюдаемый `extractor_version`
(`pdfium-text-2`) → новая индексная сборка. Явно выбранный `text_layer` без `pypdfium2`
(extra `extract`) — `EXIT_USAGE`, а **не** тихий переход на docling. Замер на retained-корпусе
(246 PDF, 16 073 страницы): открылись все 246, извлечение — 0.03–0.06 s на документ
(6–12 страниц); **183** страницы (**1.14 %**) без текстового слоя дают видимый отказ
`no_text_layer`, а не пустой документ. Под `split` ось не действует: экстрактор — remote,
конвертеры строит edge (R-S).

**`pdf_device` (P9) — устройство моделей извлечения PDF.** Ось `crawl.pdf_device`
(`cpu` — дефолт, `cuda`) объявляет, **на чём** исполняются layout-модели/таблицы/OCR.
Объявлена на уровне развёртывания (`config/deploy.yaml:crawl.pdf_device`) — устройство
принадлежит **хосту исполнения**, а не измерению продукта; `run.yaml` может переопределить
её для конкретного прогона (`run.yaml:crawl.pdf_device`), per-site слоя нет вовсе.
`auto` не существует намеренно (R5, `10` §1): объявленная ось обязана действовать, а
`auto` = «устройство выбрал движок», то есть тот же класс, что чтение модели оценщика из
env (§7.4). Цепочка — `app < run`, **без** per-site слоя: устройство принадлежит хосту
исполнения, а не сайту. CLI-флага нет по той же причине. Отдельного значения в
`crawl_config_digest` ось **не** получает: материал уже назван `extractor_stack`
(`docling-doc-3/<device>/docling-<build>`) и `extractor_version` документа, а второе поле на
тот же материал не дало бы различить расхождение прогонов; `run_id` меняется через стек —
`execution_change` (§13), один раз. `cuda` при недоступном устройстве — `EXIT_USAGE` с
числами (версия torch, `cuda_available`, объём карты), **не** тихий CPU: иначе провенанс
назвал бы GPU, на котором прогон не исполнялся. Замер (`10` §3.4–3.6): `layout_tables` на
RTX 3050 — 9.94×/11.73×/8.45× на 22/181/665 стр. против одного CPU-воркера; параллель на одной
карте — 1.10×, поэтому правильная конфигурация — один GPU-воркер. Под `split` ось не
действует: конвертеры строит edge своим конфигом (R-S).

**`document_timeout_s` (P8) — таймаут документа.** Ось `crawl.document_timeout_s`
(цепочка `app < run < site`) объявляет потолок ожидания извлечения **одного** документа/
члена контейнера. Дефолт — **прежнее зашитое поведение** (`DEFAULT_DOCUMENT_TIMEOUT_S` =
1800 с, бывший `_EXTRACT_TASK_TIMEOUT_S` в `core/pipeline/corpus.py`), поэтому объявление
оси не меняет ни поведения, ни `run_id` прогона, который её не задавал; ключ попадает в
`CrawlLimits.as_dict()`/`crawl_config_digest` **только при явном задании** (как
`max_response_bytes`/`archive_*`). Задание оси — `execution_change` (§13): потолок решает,
какой медленный документ успел стать корпусом, то есть это условие измерения, а не рычаг
пропускной способности. Механизм — guard от зависания воркера, а не SLA: замер
`layout_tables` на 665-стр. PDF — 866 с (`05` §2).

**Память по уровням.** Пик = `parent_floor + W × peak(level корня)` (`W` = `extract_workers`),
где замеренные пики — `text` 855 MB, `layout` 1563, `layout_tables` **3679**, `full` **4833**
(3679 + синтетическая дельта OCR+ACCURATE 1154; прямой замер `full` на реальном корпусе —
открытая задача, `10` §3), `parent_floor` ≈ 500 MB
(`docs/reports/measurements/2026-10-23-docling-level-peaks.md`,
константы — `core/pipeline/resources.py`). Если ось `extract_workers` **не объявлена** ни на
одном слое, окно выводится как `min(floor((бюджет − parent_floor) / peak(level)), cpu_count)`,
а под `crawl.pdf_device: cuda` — с третьим слагаемым `floor(usable_vram / vram_peak(level))`
(`10` §5.5),
где бюджет — `COHOUND_MEM_BUDGET_MB` либо `MemAvailable − 512 MB`; нулевой результат — отказ
прогона с названными числами (`EXIT_USAGE`), а не «возьмём один» (F0, `11` §3.3). Пулы корней
**последовательны** (создаются лениво на корень и `shutdown` в `finally`), поэтому пик —
**максимум** по корням, а **не** сумма: прогон с site A `full` и site B `off` не складывает
память. `off`-корень docling не строит вовсе (0). Операторский escape hatch от «слишком
медленного PDF» — понизить уровень per-site (`domain_profiles.yaml: document_level: layout`),
взять `off` или объявить `extract_workers` — **без правки кода**. Для `text_layer` пик **не
замерен**: окно считается по пикам уровней docling (консервативно) — подставлять догадку
вместо замера нельзя (A3), а `document_level` у механизма нет.

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

  Матчинг **профиля**: host (или родительский суффикс) + опциональный префикс пути; приоритет —
  длиннейший `path_key`, затем длиннейший `host_key`. Явный `--render-modes` по точному URL
  переопределяет **только** ось `fetch`, оставляя `extract`/`strategy`/`wait_for`/`rate_limit`
  из профиля.

  > ⚠️ **Наследование осей ≠ допуск в обход.** `_host_matches` (`pipeline/fetch_plan.py`)
  > отвечает на вопрос «какие **оси** применить к URL»: поддомен наследует `fetch`/`extract`/
  > лимиты родительского ключа — это верно и нужно. **Scope краула — другой предикат**
  > (`Frontier.host_in_scope`, `pipeline/crawl_frontier.py`): в обход допускаются только
  > **объявленные** хосты — хост корня (включая `www.`-вариант) и поддомен, объявленный
  > **самостоятельно** (собственным seed-корнем продукта или ключом `domain_profiles.yaml`).
  > Необъявленный поддомен **не** получает бюджета корня-предка. Измеренная цена путаницы:
  > корень, объявивший только родительский хост, отдал 563 элемента из 682 учебному
  > поддомену, а собственный раздел документации не получил ни одного.
  >
  > Отказы **видимы**: `out_of_scope_subdomains` (счётчик по хостам + ограниченные примеры) и
  > `accepted_foreign_hosts` (сколько бюджета корня ушло на объявленный чужой хост) в
  > `truncation_records`/провенансе корня. Хост, объявленный ключом `domain_profiles.yaml`,
  > **становится** scope-хостом (`core/pipeline/corpus.py`): объявление профиля — это и есть
  > объявление допуска. `www.example.org` ≡ `example.org` в обе стороны.

  Профиль домена — **высший** слой цепочки осей контроля краула (D1/D2): per-site
  `max_workers`/`extract_workers`/`extract_max_tasks_per_child`/`max_depth`/`max_documents`/
  `revalidate_max_documents`/`exclude_paths`/`exclude_documents`/`document_level`/
  `max_response_bytes`/`max_artifact_bytes`/`attachment_formats`/`archive_*` переопределяют
  run/app-дефолты (`resolve_crawl_limits`). Например,
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
  audit: { sink: file, target: "…" }   # SIEM-экспорт; sink: file | syslog (одно значение)
extractor_zone: edge           # edge | core | separate
search:
  adapter: yandex               # провайдер поиска по умолчанию (Yandex Search API)
                                # режим запроса (sync|async) — НЕ здесь: он объявляется
                                # входом прогона `run.yaml: serp.mode` (ось контракта)
  # режим Yandex `async` (дефолт) | `sync` — env YANDEX_SEARCH_MODE (§D.6), не поле YAML
  by_language:                  # переопределение по языку (география-прокси, I16)
    ru: yandex
    en: glm
  serp:                         # нейтральный дефолт SERP-контура (§D.1.0)
    rounds: 0                   # низший уровень переключения; 0 — контур выключен.
                                # Бюджета здесь НЕТ: он объявляется в run.yaml (serp.budget),
                                # потому что определяет число запросов и состав корпуса.
crawl:                         # операционная политика HTTP-краулинга корпуса (local|split)
  user_agent: "coHound-crawler/0.1"   # None → дефолт HttpCrawler
  timeout_s: 30                # None → дефолт HttpCrawler (30 с)
  follow_redirects: true
  # D1/D2: application-слой осей контроля краула (низший приоритет app < run < site;
  # резолв — pipeline/crawl_frontier.py:resolve_crawl_limits).
  max_workers: 4               # опц. (>0): D4-пул потоков на сетевой fetch (None/1 = inline)
  extract_workers: null        # опц. (>0): P4-пул экстракции (процессы local / потоки split;
                               # None = вывести из бюджета памяти, 1 = inline).
                               # В crawl_config_digest/run_id НЕ входит (расписание, не условие
                               # измерения) — см. metrics.json.resources.scheduling
  extract_max_tasks_per_child: null  # опц. (>=1): рециклинг воркер-процессов экстракции
  document_level: full         # P5: уровень извлечения документов (off|text|layout|
                               # layout_tables|full; дефолт full = сегодняшнее поведение)
  document_timeout_s: null     # P8, опц. (>0): потолок извлечения ОДНОГО документа, с.
                               # null = прежнее зашитое значение 1800 (DEFAULT_DOCUMENT_TIMEOUT_S);
                               # задание входит в crawl_config_digest → run_id (execution_change)
  max_depth: 0                 # >=0: 0 = только seeds (BFS идёт и под executor/split — A2/R2)
  max_documents: null          # опц. (>0): потолок добытых страниц на (продукт, seed-root)
  revalidate_max_documents: null  # опц. (>=1): потолок известных URL ревалидации на продукт
  # P6/P6b: два РАЗНЫХ потолка. max_response_bytes — RAM-потолок СТРАНИЦЫ (дефолт 20 MiB;
  # HTML сверх него → payload_too_large). max_artifact_bytes — потоковый потолок
  # ТЕРМИНАЛЬНОГО АРТЕФАКТА (документ pdf/docx/pptx/xlsx И контейнер zip/7z/rar): тело
  # пишется в staging-файл, не в RAM. Оси независимы; путать их — ошибка конфигурации.
  # ВАЖНО: оба — int > 0, `null` НЕ допустим (ValidationError). Ключ просто опускается →
  # действует дефолт (20 MiB / 250 MiB).
  # max_response_bytes: 20971520    # опц.: in-memory потолок страницы (дефолт 20 MiB)
  # max_artifact_bytes: 262144000   # опц.: потоковый потолок артефакта (дефолт 250 MiB)
  attachment_formats: null     # P6: позитивная вселенная контейнеров (дефолт zip/7z/rar)
  # P6/B3: спуск в контейнеры (§D.1.1). `null` для archive_max_* НЕ допустим — ключ
  # опускается; `archive_max_members: null` допустим (без потолка числа членов).
  # archive_max_member_bytes: 67108864              # 64 MiB на ОДИН член
  # archive_max_uncompressed_bytes: 268435456       # 256 MiB на ОДИН контейнер
  # archive_max_members: 1000                       # число членов контейнера
  # archive_max_total_uncompressed_bytes: 1073741824  # 1 GiB КУМУЛЯТИВНО на корень спуска
  # archive_descent: full                           # full | plan
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

**Терминальный артефакт и два потолка (P6/P6b).** Класс ответа определяется не «размером», а
поведением навигации: **страница** продолжает обход (её RAM-потолок —
`crawl.max_response_bytes`, дефолт 20 MiB; сверх — `payload_too_large`), а **терминальный
артефакт** — нет. Терминальный артефакт = документ (`pdf`/`docx`/`pptx`/`xlsx` — лист для
экстрактора) **или** контейнер (`zip`/`7z`/`rar` из `crawl.attachment_formats` — вход для
спуска); определяется URL-суффиксом либо `Content-Type`
(`core/util/document_formats.is_terminal_artifact`). Он вправе быть больше страничного
потолка: тело пишется потоком в staging-файл (не в RAM) и ограничено
`crawl.max_artifact_bytes` (дефолт 250 MiB). Потолки **независимы**: повышать
`max_response_bytes` «чтобы прошли PDF» — ошибка (это поднимет и RAM-потолок страниц).
`.rtf` намеренно не допускается: у реестра экстрактора нет читателя RTF, допуск дал бы
блокирующий `parser_failed`.

**Обход path-prefix артефактом (P6b).** Артефакт не продолжает навигацию, поэтому
path-prefix seed-root к нему не применяется: `/docs/` может ссылаться на
`/files/report.pdf`, и такой URL ставится в очередь (BFS-ссылки, известные URL ревалидации
и sitemap `<loc>`), а не отбрасывается как «вне раздела». **Host scope** здесь не меняется
и означает ровно одно: в обход допускаются **объявленные** хосты (`Frontier.host_in_scope`,
определение и ловушку `edu.postgrespro.ru` см. §D.2) — поддомен, объявленный только
родительским ключом профиля, бюджета корня не получает. SSRF-валидация,
`..`-traversal-guard и robots — тоже без изменений; блэклист оператора
(`exclude_paths`/`exclude_documents`) побеждает всегда. Обход видим: счётчик + ограниченные
примеры (`artifacts_outside_prefix`) попадают в провенанс корня рядом с
`skipped_blacklisted`/`skipped_documents` — молчаливый обход был бы дефектом.

**`extract_workers` (P4, runbook памяти).** Параллельная экстракция документов:
`local` — пул процессов (`ProcessPoolExecutor`/forkserver, docling-экстрактор строится на
воркер), `split` — пул потоков поверх `RemoteExtractor` (шов `/v1/*` не меняется). Окно
`W` = `extract_workers`: «в полёте» не более `W` экстракций, доводка строго FIFO (порядок
коммита = порядок отправки). `None`/`1` ⇒ пул не создаётся, экстракция inline.
**Окно выводится из замера, а не назначается вручную** (F0/Д-7, `11` §3.3): при оси,
не объявленной ни на одном слое, `W = min(floor((бюджет − parent_floor) / peak(level)),
cpu_count)` — `peak` замерен по уровням (`text` 855 MB, `layout` 1563, `layout_tables` 3679,
`full` 4833), `parent_floor` ≈ 500 MB, бюджет — `COHOUND_MEM_BUDGET_MB` либо
`MemAvailable − 512 MB` (`core/pipeline/resources.py`). Потолок по CPU обязателен: память
отвечает «сколько поместится», но не «сколько успеет», и без него хост с запасом RAM получил
бы сотню процессов docling. Ноль воркеров — отказ прогона на входе (`EXIT_USAGE` с числами),
а не OOM на первом документе. Объявленная ось сильнее бюджета (воля оператора), но
расхождение печатается предупреждением. Пулы создаются **per seed-root** и `shutdown` в
`finally`, корни обходятся **последовательно**, поэтому пик = `parent_floor + W × peak(level
корня)` — **максимум** по корням, а не сумма: прогон с site A `full` и site B `off` не
складывает память (`off`-корень docling не строит вовсе). Один медленный PDF в голове FIFO
задерживает доводку окна — следствие FIFO-детерминизма, не регресс. Отказ воркера
(краш/OOM/таймаут) → явный `FAILED` + сеанс `PARTIAL`, пул пересобирается (bounded) либо
остаток идёт inline; авто-ретрая нет (восстановление — повторный прогон того же `session_id`
по retained raw без ре-фетча).

> **Изоляция прогона от соседей по хосту (F1).** `MemAvailable` — это «сколько можно отдать
> прямо сейчас», поэтому без изоляции бюджет прогона зависит от того, что ещё живёт на
> машине (замер: idle MCP-сервер занимал 5.5 GB и приводил к OOM-kill). Прогон, упавший
> из-за соседа, неотличим от прогона, упавшего из-за собственной конфигурации, — то есть
> проверка «помещается ли» перестаёт быть проверкой. Практика: запускать прогон в
> ограниченном cgroup-скоупе и **объявлять** бюджет явно, чтобы он не зависел от соседей:
>
> ```bash
> systemd-run --user --scope -p MemoryMax=6G -p MemorySwapMax=0 \
>   env COHOUND_MEM_BUDGET_MB=5500 python3 -m core.cli run …
> ```
>
> `COHOUND_MEM_BUDGET_MB` меньше `MemoryMax` — резерв остаётся прогону на не-экстракцию
> (краул, индекс, БД). Без объявленного бюджета число воркеров становится функцией
> занятости хоста: это допустимо (и preflight откажет, если не помещается), но
> воспроизводимость такой прогон не даёт — публикуйте бюджет в `resources.budget`.

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
`lang` (`SearchQuery.lang`, `search_log.language`; бюджет поиска — один, объявляется входом
прогона `serp.budget`, см. §D.1.0). Решение
вычисляется по правилу, а не LLM (**дух I21**). Имена провайдеров — свободные строки-данные:
config-слой не импортирует `cli.wiring`; валидация против `SUPPORTED_SEARCH_ADAPTERS` —
в composition root, неизвестный провайдер отвергается с понятной ошибкой (I19: кандидаты
не выдумываются). Выбранный провайдер фиксируется в провенансе/`search_log` (**I23**).
Смена таблицы провайдеров меняет протокол поиска и потому влияет на сопоставимость
циклов — это **версионная граница**: фиксируется в манифесте/провенансе (§13).
Режим Yandex Search API — **объявленный вход прогона** `run.yaml: serp.mode`
(`sync` | `async`, дефолт `async`), ось контракта `serp_mode`. Env `YANDEX_SEARCH_MODE`
конвейер не читает (её читает разведка, чей выход — кандидаты, а не evidence): `sync` и
`async` — два бэкенда одного API, поэтому режим меняет состав находок, то есть корпус.

Загрузчик `core/config/deploy.py` → `DeployConfig`
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
| `COHOUND_LLM_BASE_URL` | базовый URL [OI]-совместимого LLM-API корпоративного облака | опц. | — (обязателен: внутренний endpoint) | `wiring.build_discovery_llm` (LLM-разведка `passport-scout`, Ш2) **и** `wiring.build_evaluator_llm` (endpoint оценщика; env-значение переопределяет слот и оставляет `llm_scenarios` пустой — см. ниже) |
| `COHOUND_LLM_API_KEY` | ключ LLM-API (единственный источник; внешних фолбэков нет) | **обязателен для вызова** | — (обязателен для вызова) | `wiring.build_discovery_llm` **и** `build_evaluator_llm` |
| `COHOUND_LLM_SLOT` | активный LLM-слот: `cheap`\|`flagship` (см. `deploy.yaml.llm`) | опц. | `cheap` | `wiring` (выбор слота — и оценщиком, и разведкой) |
| `COHOUND_LLM_MODEL` | `model_id` **LLM-разведки** (переопределяет активный слот) | опц. | `deepseek-v4.1-flash:purple` (провизионно) | **только** `wiring.build_discovery_llm`; оценщик её **не читает** (модель оценщика — `deploy.llm.<slot>.model_id` или `--llm-model`) |
| `COHOUND_LLM_DECODER` | механизм декодирования (`none`/`json_object`/`json_schema`) | опц. | `json_object` | **только** `wiring.build_discovery_llm`; оценщик берёт `decoder` из слота |
| `COHOUND_LLM_TIMEOUT` | таймаут LLM-запроса, с (>0) | опц. | `120.0` | `wiring.build_discovery_llm`/`build_evaluator_llm` (операционный параметр хоста) |
| `COHOUND_LLM_MAX_TOKENS` | максимум токенов ответа (>0) | опц. | `4096` | `wiring.build_discovery_llm` |
| `COHOUND_MEM_BUDGET_MB` | явный бюджет памяти прогона для экстракции, MB (>0) | опц. | `MemAvailable − 512 MB` | `core/pipeline/resources.resolve_mem_budget_mb` через composition root (`wiring`); из бюджета выводится окно экстракции `floor((бюджет − 500) / пик уровня)`. **Не входит в `run_id`**: число воркеров — расписание, а не условие измерения (11 §2.10) |
| `COHOUND_SERP` | раунды SERP-контура: целое `N ≥ 0` либо `off`/`0` | опц. | — (уровень ниже: `run.yaml: serp.rounds` → `deploy.search.serp.rounds` → 0) | `cli.execution_axes._serp_axis` (раунды → `serp_rounds` контракта ⇒ `run_id`) и фаза корпуса; расхождение с объявленным входом видно в `warnings` прогона (`wiring.serp_env_notices`), как у `COHOUND_LLM_*` |
| `COHOUND_SCOUT_CRAWLER` | краулер `passport-scout`: `http`\|`scrapy`\|`browser` | опц. | `http` | `wiring.build_scout_crawler` |
| `COHOUND_NO_REEXEC` | отключить автоперезапуск `hound.py` в проектной `.venv` | опц. | — | `hound.py` |
| `COHOUND_REEXEC` | предохранитель от зацикливания перезапуска (ставит `hound.py`) | служебная | — | `hound.py` |
| `YANDEX_SEARCH_API_KEY` | ключ Yandex Search API | **обязателен для адаптера** | — | `adapters.search_yandex` |
| `YANDEX_FOLDER_ID` | folder-id Yandex Cloud | **обязателен для адаптера** | — | `adapters.search_yandex` |
| `YANDEX_SEARCH_MODE` | режим поиска Yandex: `async` (дефолт) \| `sync` | опц. | `async` | `adapters.search_yandex` |
| `GLM_SEARCH_API_KEY` | ключ GLM web-search | **обязателен для адаптера** | — | `adapters.search_glm` |
| `COHOUND_HOME` | единый корень операционной раскладки; относительные пути считаются от него | опц. | CWD | `core/config/paths.py` (все команды) |
| `COHOUND_PATHS_FILE` | путь к `paths.yaml` | опц. | `config/paths.yaml` (отсутствие файла — дефолты) | `core/config/paths.py` |
| `COHOUND_PACKAGES_DIR` / `COHOUND_PASSPORTS_DIR` / `COHOUND_RUNS_DIR` / `COHOUND_INDEX_STORE_DIR` / `COHOUND_CORPUS_RAW_DIR` / `COHOUND_CACHE_DIR` / `COHOUND_EXTRACT_CACHE_DIR` / `COHOUND_EVAL_CACHE_DIR` / `COHOUND_EMBED_CACHE_DIR` / `COHOUND_REPORTS_DIR` / `COHOUND_STATE_DIR` | переопределение отдельного каталога раскладки (§A.1); все 11 полей `HARNESS_FIELDS` | опц. | `paths.yaml`, иначе дефолт модели | `core/config/paths.py`; приоритет CLI > env > `paths.yaml` > дефолт. **Per-field env сильнее `COHOUND_HOME`** — поэтому «увести всё» делается домом, а не одиннадцатью переменными |
| `COHOUND_LIVE_RUNS_DIR` | каталог, где лежат `runs/{run_id}/` **живых** прогонов — только для проверки на реальном прогоне (`tests/pipeline/test_publishability.py`) | опц. | resolved-раскладка | тест, не `core/`; без значения проверка скипается. Отдельная переменная, потому что набор тестов намеренно не читает операторское окружение (`COHOUND_NO_DOTENV=1`) |
| `COHOUND_EDGE_STATE_DIR` | каталог durable-состояния edge (DMZ) | опц. | `state_dir` из edge-конфига | `edge/config.py` (`EdgeRuntimeConfig`) |
| `COHOUND_EMBED_LITE_MODEL` / `COHOUND_EMBED_LITE_REVISION` / `COHOUND_EMBED_LITE_PATH` | модель/пин ревизии (sha/тег)/явный локальный каталог снапшота для `embed-lite` | опц. | `minishlab/potion-multilingual-128M`; ревизия не задана ⇒ `model_id` без `@sha`; путь — HF-кэш | `wiring` (сборка `embed_model2vec`); ревизия входит в `embed_model_id` как `repo@sha` |
| `PLAYWRIGHT_DOWNLOAD_HOST` / `PLAYWRIGHT_BROWSERS_PATH` | зеркало CDN браузеров и каталог их установки (закрытый контур) | опц. | CDN Playwright / штатный кэш | сам `playwright` (внешний механизм; код ядра их **не** читает) |
| `HF_HOME` / `DOCLING_ARTIFACTS_PATH` / `HF_HUB_OFFLINE` | офлайн-веса docling (V4, `05` §7) | опц. | HF-кэш / — | `docling` (внешний механизм) |

> **`COHOUND_REGISTRIES_DIR` — удалённая ось.** Переменной **нет** в шаблоне `.env.example`
> (вычищена 30.10.2026): она не читается нигде в `core/` — раскладка реестров package-scoped
> (§D.4, `registries/` внутри пакета). В операторском `./.env` строка могла остаться от прежней
> редакции шаблона; она безвредна, но мёртвая — удаляйте.
>
> **Артефакты инструментов не читаются `core/` вовсе** (поэтому их нет в этой таблице):
> `MYPY_CACHE_DIR`, `RUFF_CACHE_DIR`, `HYPOTHESIS_STORAGE_DIRECTORY`, `COVERAGE_FILE`,
> `PYTHONPYCACHEPREFIX`, `PYTEST_ADDOPTS` — их читают сами инструменты. `.env` загружается
> **только на входе CLI** (`core/cli/main.py`), значит для `pytest`/`ruff`/`mypy` эти
> переменные нужно экспортировать в оболочку — пример лежит в `~/.cohound/env.sh` (§A.1.2).
>
> **Не env-переменные (хотя похожи):** `PLAYWRIGHT_BROWSER_TYPE` и
> `PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT` — это **настройки Scrapy** (`_PLAYWRIGHT_SETTINGS`
> в `core/adapters/crawl_scrapy.py`, значения `chromium`/`15000`), а не читаемые из
> окружения ключи; менять их — правкой адаптера, не переменной. `HF_HOME`/`HF_HUB_OFFLINE`
> читает `huggingface_hub` (транзитивно, через docling).

Примечания:

- **Обязательность** ключей поиска проявляется в `hound discover`: без пары
  (`api_key`, `folder_id`) адаптер считается недоступным, и кандидаты **не
  выдумываются** (I19) — команда возвращает `EXIT_ERROR` с явным сообщением.
- Флаги CLI переопределяют env: `passport-scout --llm-base-url/--llm-model/--llm-decoder`,
  `discover --search-adapter`. Для **оценщика** переопределение модели — `--llm-model`
  (`run`/`plan`/`crawl run|resume|restart`): env `COHOUND_LLM_MODEL`/`COHOUND_LLM_DECODER`
  на него не действуют (см. ниже).
- **Идентичность модели оценщика — объявленный вход, не env (§7.4/I21).** `model_id`
  оценщика резолвится `--llm-model` > `deploy.llm.<slot>.model_id` > **ошибка** (`CliError`,
  код `llm`, `EXIT_USAGE`): дефолтной модели **нет** — модель, которую оператор не
  объявлял, была бы ложной провенанс-записью. Дефолт есть только у `decoder`
  (`json_object`, `EVALUATOR_DEFAULT_DECODER`).
  `base_url`/`api_key`/`timeout` — операционные и читаются из env как раньше. Причина —
  `llm_scenarios` входят в `execution_contract_digest` (→ `run_id`): манифест обязан
  называть ту модель, которая работала, а env — свойство хоста, не прогона. Активный слот
  без объявленного `model_id` — **ошибка**, а не тихий дефолт.
  Флаг питает **и** runtime-оценщик, **и** `llm_scenarios` — контракт и рантайм не могут
  разойтись. Если `COHOUND_LLM_MODEL`/`COHOUND_LLM_DECODER` заданы, прогон сообщает об их
  игнорировании (`warnings` payload + `logs/wiring.jsonl`, событие `llm_env_note`).
  Endpoint из env (`COHOUND_LLM_BASE_URL`) оставляет `llm_scenarios` пустой (env не входит
  в `run_id`) — и это тоже видно в payload/`wiring.jsonl` как «оценщик вне execution
  contract». Асимметрия с LLM-разведкой намеренная: её выход — кандидаты/гипотезы (G2),
  а не evidence.
- **Условие активности слота (то, что оператору нужно знать раньше всего).** Слот активен
  **тогда и только тогда**, когда у него непустой `base_url`. Пустой `base_url` ⇒
  `build_evaluator_llm` возвращает `None` (оценщик не поднимается) и `llm_scenarios`
  остаётся **пустой** ⇒ `execution_contract_digest` от модели не зависит ⇒ **`run_id` не
  меняется**. Практическое следствие: правка `model_id` в слоте с пустым `base_url`
  безопасна для идентичности, а та же правка на активном слоте форкает прогон
  (`execution_change`). Проверяется вычислением: `_resolve_llm_scenarios` возвращает `()`
  для неактивного слота.
- **Две слотовые модели (`deploy.yaml.llm`).** Секция `llm:` в `config/deploy.yaml` задаёт два
  именованных слота — `cheap` и `flagship`. **Обе слоты по умолчанию пусты**
  (`LLMDeployConfig`: ни `base_url`, ни `model_id` не заданы), т.е. оценщик из коробки
  **не поднимается**. `deepseek-v4.1-flash:purple` — **провизионный дефолт LLM-разведки**
  (`wiring.DEFAULT_LLM_MODEL`), а не слотов; он же прописан в `model_id` обоих слотов
  канонического `config/deploy.yaml` как провизионные данные (финальный выбор даёт
  `hound experiment evaluator-benchmark`). Активный слот выбирается детерминированно по
  приоритету: `--llm-slot` > env `COHOUND_LLM_SLOT` > `run.yaml: llm_slot` > `deploy.llm.slot`
  > `cheap`. `api_key` берётся из env (`COHOUND_LLM_API_KEY`); endpoint — [OI]-совместимый
  API корпоративного облака. Форма секции — `07` §7.

**Автозагрузка `.env` (composition root, `core/config/dotenv.py`).** CLI загружает
dotenv-файл **на входе** (`core/cli/main.py`, после разбора argv и до резолва раскладки/
портов) — библиотечный код `.env` не читает, на импорте побочных эффектов нет. Это
защищает от измеренного класса ошибок: забытый `export` терял
`COHOUND_SCOUT_CRAWLER=scrapy`, и прогон молча деградировал на `http`.

- **Приоритет источника файла:** `--env-file PATH` > env `COHOUND_ENV_FILE` > `./.env`.
  Загружается только существующий файл; отсутствие дефолтного `./.env` — не ошибка
  (тишина). Явный `--env-file` без файла — ошибка входа (`EXIT_USAGE`), значение
  `COHOUND_ENV_FILE` без файла — диагностика в `stderr` без остановки.
- **Окружение процесса приоритетнее файла:** уже заданные переменные **не
  переопределяются** (секрет-хранилище/оператор побеждают `.env`).
- **Opt-out:** `COHOUND_NO_DOTENV=1` (`true`/`yes`/`on`) — автозагрузка отключена.
- **Диагностика:** некорректные строки не проглатываются, а печатаются в `stderr` как
  `env-file: <path>: строка N: …`; при `--json` диагностики подавляются (машинный контракт
  не засоряется). Поддерживаются `KEY=VALUE`, `export KEY=VALUE`, комментарии, кавычки
  (одинарные — литерал; двойные — `\n`/`\t`/`\\`/`\"`), `\r\n` и BOM; UTF-8.
- **Видимость движка краула.** Движок — commodity-слой (A2) и **не входит** в
  `execution_contract_digest`, поэтому при сборке портов (`wiring.build_scout_crawlers`)
  в `runs/{run_id}/logs/wiring.jsonl` пишется одна INFO-строка на движок:
  `движок краула: <engine> (источник: <источник>)`, где источник —
  `env:COHOUND_SCOUT_CRAWLER=<value>`, `crawl_sources.engine[...]`, `profile.fetch[hosts]`
  или `default`. Запрошенный, но несобранный движок (нет зависимости/ошибка конфигурации)
  виден как WARN с причиной — тихая подмена движка больше не возможна.
- **Видимость env-расхождений LLM.** Идентичность модели оценщика — объявленный вход
  (`deploy.llm.<slot>.model_id` / `--llm-model`), поэтому `COHOUND_LLM_MODEL`/
  `COHOUND_LLM_DECODER` в прогоне не действуют. Чтобы смена env не меняла поведение молча,
  `_resolve_ports` пишет по WARN-строке на каждое расхождение в `logs/wiring.jsonl`
  (событие `llm_env_note`), а команда (`run`/`plan`/`crawl`) дублирует их в `warnings`/
  `notes` payload. Отдельная строка — «endpoint из env»: оценщик поднят, но `llm_scenarios`
  пуста, то есть модель оценщика **вне** execution contract (`run_id` её не описывает).

**Runbook живого прогона корпуса.** Минимальный операторский запуск (профиль `local`,
in-process, без Правки кода):

```bash
hound run \
  --run-yaml packages/{case_id}/run.yaml \
  --package packages/{case_id} \
  --passports-dir passports \
  [--urls <urls>.yaml] \
  --until corpus \
  [--refresh auto|revalidate|refresh|offline] \
  [--render-modes <render>.yaml]
```

- `<urls>.yaml` — `{product_id: [url, ...]}` (операторский корпусный вход, Ш5). **Не
  обязателен**: без него корпус берётся из пакетного seed-набора — `sources/seeds.yaml`
  (канонический) либо legacy `sources/corpus_urls.yaml`; приоритет и значимость порядка
  корней — §A.1.1. `doc_domains` паспортов молча не фетчатся (I19).
- `--refresh MODE` — политика наблюдения корпуса (`auto` дефолт, `revalidate` — только
  известные URL, `refresh` — живой ре-фетч, `offline` — без сети). **Входит в `run_id`**
  (§C.1.1): смена политики — новый прогон, а не продолжение.
- `[--render-modes <render>.yaml]` — `{url: "http"|"browser"}`: явное переопределение оси
  `fetch` для SPA/JS-страниц (значение не из `http`/`browser` → `EXIT_USAGE`). **Явное
  переопределение побеждает доменный профиль** по оси `fetch`; `extract`/`strategy`/`wait_for`/
  `rate_limit` при этом берутся из `domain_profiles.yaml` (D.2).
- `--cutoff-k K` (I20) и `--render-modes` влияют на фазы Ш5/Ш6; `--cutoff-k` имеет смысл
  при прогоне до `evaluate`.

**Фазы (`--until`).** Полный список (совпадает с `--help`):
`normalize, lint, plan, g1, discovery, g2, registry, inheritance, corpus, evaluate, review,
g3, compute, g4, report` — 15 фаз. Между `plan` и `g1` исполняется **не-фазовый**
onboarding-префлайт (seed-набор, §A.1.1): `--until plan` его **пропускает** (прогон,
который не собирается краулить, не должен блокироваться seed-префлайтом). Гейты: `g1`/`g2` —
внутренняя аттестация профиля, `g3` — между `review` и `compute`, `g4` — между `compute` и
`report` (точки интеграции, §E.2).

**Гейты per-run.** Свежий прогон встаёт на `g1` и завершается кодом **3** (`EXIT_BLOCKED`) —
это не сбой, а остановка на гейте: гейт не проходится автоматически никогда. Решение
пишется `hound gate --runs-dir runs --run-id {run_id} --gate G1 --actor {кто}
--target cohort --notes "{почему}"` (§E.2). Строка `gate_decisions` от **прошлого** прогона
не действует: новый `run_id` требует новых решений. Продолжить с места остановки —
повторный `run` (resume) или `--until {фаза}`; `--reset` форсирует перезапуск.

Что появится на диске (раскладка A.1/A.2):

- `corpus_raw/{product_id}/{hash}.bin` — retention сырых байтов (пишется до экстракции, I19);
- `corpus_raw/_staging/` — staging-файлы терминальных артефактов до публикации переездом
  (`os.replace`); файл живёт только на время фетча и не является evidence сам по себе.
  Потолки: страница — `crawl.max_response_bytes` (20 MiB, RAM), артефакт —
  `crawl.max_artifact_bytes` (250 MiB, диск); см. §D.5;
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
  или `=browser` для `passport-scout` (прямой `BrowserCrawler`);
- `numpy` + `tantivy` (extra `retrieval`) — лексический индекс (BM25) и `.npy`-шарды; без
  него лексическое плечо не поднимается (R0/R2 пропускаются);
- `nh3` + `defusedxml` (extra `safety`) — санитайзинг внешнего контента (§10); без них
  недоступны соответствующие шаги очистки;
- `py7zr` + `rarfile` (extra `extract-archives`) — читатели контейнеров 7z/rar. **Без этого
  extra спуск в 7z/rar невозможен**: контейнер остаётся нераскрытым с видимой причиной
  (`container_reader_unavailable` / `backend_unavailable: py7zr не установлен`), а не с
  тихим «пустым архивом». `zip` работает всегда (stdlib); AES-zip не поддерживается.
  Установка: `uv pip install -e '.[extract-archives]'`.

Проверка состава extras — `hound doctor` (группы `core`/`retrieval`/`crawl`/`extract`/
`extract-pdf`/`extract-archives`/`safety`/`ops`/`embed`/`embed-lite`/`ann`/`pdf-measure`/`dev`).

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
| `hound runs [--runs-dir DIR]` | список прогонов каталога: `run_id`, пакет, дата, последняя фаза, состояние, счётчики. БД читается **только на чтение** (каталог/файл не создаются); БД чужой `schema_version` пропускается с видимой пометкой `skipped`; пустой/отсутствующий каталог → пустой список (`EXIT_OK`) | — (только чтение) |
| `hound status <run_id> [--runs-dir DIR]` | где прогон и что дальше: последняя исполненная фаза, гейт остановки и записанное решение (кто/почему), счётчики ячеек/корпуса и **готовая следующая команда** (`hound gate …` / `hound run …` / `hound report …`). Отсутствующий прогон → `EXIT_NOT_FOUND` (4) | — (только чтение) |
| `hound resolve <source_id> --db PATH` | резолв `source_id` → URL по БД прогона (I10/I15), без сети | — (только чтение БД) |
| `hound plan` | нормализация входов + линт + план прогона, когорта; принимает **те же оси краула**, что `run` (`--max-depth`/`--max-documents`/`--exclude-path`/`--exclude-document`/`--document-level`/`--pdf-pipeline`), плюс `--serp-rounds` и **контрактные флаги ретривала** `--retrieval-profile`/`--cutoff-k` — и печатает `run_id` вместе со слагаемыми контракта (`execution_contract_digest`, `crawl_config_digest`, `serp_rounds`/`serp_source`, `retrieval_profile`/`cutoff_K`). Инвентарь флагов один (`CONTRACT_FLAGS`/`CRAWL_AXIS_DESTS` в `core/cli/execution_axes.py`), паритет с `run` читают тесты по собранным парсерам | — (отчёт в stdout/`--json`; запись черновика `run.yaml`/`cohort.yaml` — roadmap W10. Флага `--dry-run` нет: `plan` и так ничего не пишет, а флаг обещал «есть и не-dry режим») |
| `hound discover` | Discovery-агент | passports, run |
| `hound passport-scout` | авто-генерация **черновика** паспорта продукта: краул `--url` (и, при поиске, URL выдачи) → tool-less LLM-разведка (`discovery_llm`) → консервативный черновик. Сателлиты — только `draft`/`not_found`; `relationship_evidence`/`upstream` — лишь при подтверждённом URL и дословной цитате (I10/I15, precision-first). Публикация только при `report.ok`; `--dry-run` не пишет. Кандидаты не выдумываются без LLM-конфигурации | `passports/{product_id}/…yaml` (+ sidecar кандидатов) |
| `hound package scout` | веб-заземлённая разведка пакета категории: pipeline сам делает SERP+краул+extract, tool-less LLM синтезирует гипотезы → консервативный маппинг → кандидаты (`needs_human=True`; auto-approve нет, I18/I19/A3). Провайдер поиска резолвится из `config/deploy.yaml` (`search.adapter`/`search.by_language` по `--lang`) детерминированно; `--search-adapter` переопределяет (I16/I21/I23) | `packages/{case_id}/sources/scout/{date}.candidates.json` (+ `.provenance.json`, в т.ч. `search_provider`) |
| `hound package {module\|role\|param\|baseline\|cohort\|scope\|golden\|run\|artifact} <действие>` | read/CRUD-плоскость управления пакетом. **Глаголы у подкоманд разные** (см. матрицу ниже); `--json` | — (read-only, кроме `*: add/update/remove/set/clear`) |
| `hound run [--until PHASE]` | конвейер Ш1–Ш9 (15 фаз) | БД, артефакты |
| `hound crawl {run\|status\|list\|resume\|restart\|cancel}` | сеансы краула (`local`): durable-запуск фазы корпуса Ш5, статус/счётчики, resume/restart/cancel | `crawl_sessions`, `crawl_session_items` |
| `hound review` | очередь ревью / preliminary-стол: `preliminary`-контур + `❓`-кандидаты (Ш7) — **только печать**, ячейки не меняет | — (только чтение БД) |
| `hound report` | сборка артефактов | outputs |
| `hound golden [--golden PATH \| --package DIR] [--tier fast\|full]` | прогон golden; источник эталона — явный `--golden` **или** объявление пакета `sources/golden_status.yaml` (`present` — сверка; `absent`/`declined` — «неприменимо», `EXIT_USAGE` с `error: golden_not_configured`, пороги не считались) | отчёт |
| `hound sensitivity` | пост-хок по осям | отчёт |
| `hound diff <run_a> <run_b>` | межцикловый diff (запрещён через major) | diff.md |
| `hound promote --actor <вы> [--validated-only]` | preliminary → strict (переаттестация); `--actor` **обязателен** (I6/I7) | cells |
| `hound revalidate` | пере-оценка **только** устаревших ячеек существующего прогона (§13/K2): селектор `select_stale_cells` берёт ячейки со `stale_reason` из store (не из `stale_cells.csv`) и **только** корпусного контура (`source_kind=documentation`, состояние `evaluated`/`preliminary`) — реестр/наследование/baselines не переписываются. Условия ячейки повторяются (её `retrieval_profile`/`cutoff_K`, принятие риска из `evidence_scope`), модель оценщика — из объявленных `llm_scenarios` прогона (`--llm-model` нет: другая модель — другой `run_id`). Пометка снимается **только** при вердикте по текущей сборке; `failed`/`preliminary` или чужая сборка → пометка остаётся. `run_id` не меняется (это не новое измерение); `--dry-run` — только селектор; повтор без изменений → `noop` с явным «пере-оценивать нечего» | cells (`stale_reason`/`index_build_id`/`validation_notes`) |
| `hound gate --gate G1..G4 --actor <вы>` | запись решения гейта. G1/G2 — **внутренняя аттестация** профиля; G3/G4 — **точки интеграции** (вердикт приходит извне). `--source internal\|external`, `--verdict approved\|rejected`, `--subject-digest` (по умолчанию — из `runs/{run_id}/gates/{gate}-request.json`), `--external-ref` + `--notes` обязательны при `--source external` (S9/I6/I7) | `gate_decisions` |
| `hound deploy-check` | диагностика профиля: достижимость DMZ-шва, версии/форматы edge (`/v1/health`, `/v1/capabilities`) + неразрушающий зонд очереди краула (`crawl/status` → строка `crawl`; без подачи задания) | — (отчёт) |
| `hound experiment retrieval` | прогон R0/R1/R2 на retrieval-golden и фиксация измеренного `retrieval_profile`/`retrieval_cutoff_K`/`embed_model_id`/`rrf_k` как `settings_provenance` со `source: measured` (A3, T8.6, I20). Фиксация — **только профиль**, и **только** при полном сравнении; частичное сравнение/`skipped` → `source: default`. Требует зафиксированную индексную сборку в `runs`; рассинхрон «эмбеддер запроса ↔ сборка» → отказ (§13), отсутствие предпосылки → `skipped` (N3) | `runs.settings_provenance_json`, `runs.retrieval_*` |
| `hound experiment evaluator-benchmark` | измерение моделей **оценщика** (Ш6) при фиксированных прочих условиях (A3): валидность JSON, Cohen's κ, confusion, abstention, `fabricated_support`, bootstrap CI + проба детерминизма. `--golden` обязателен; `--model` (повторяемый) — **единственный** источник `model_id` (не env); `--llm-slot` — откуда взять `base_url`/`api_key`/`decoder`/`timeout`; `--repeats N` (N≥2 включает пробу); `--limit N` — smoke-подмножество. Пишет **только JSON-артефакт** отчёта (БД не меняет: `persist_manifest` есть лишь в ветке `retrieval`); выбор модели — измерение, а не предрешение | JSON-артефакт (`{runs}/{run_id}/benchmark/evaluator-<ts>.json`), отчёт |
| `hound doctor [--strict] [--with-selfcheck]` | самодиагностика инсталляции: runtime, зависимости по extras, env/ключи, конфиг, каталоги, CLI-поверхность | — (отчёт) |
| `hound baseline-agreement --annotations <csv>` | согласие независимых аннотаторов §11.5: Cohen's κ по полному набору меток **и отдельно** по бинарному решению N/A-vs-не-N/A (N/A задаёт знаменатель Coverage, I6/I7), + матрица расхождений и счётчики. Непарные ячейки исключаются и перечисляются; нулевая вариация маргиналов → `undefined` (не выдуманное число). Разметку не создаёт: это ручная работа человека | — (отчёт; артефакт разметки — вход) |
| `hound package crawl-probe` | **измерение** оптимальных осей краула для каждого ключа `domain_profiles.yaml` (A3): лестница `http` → `browser/hybrid` → `browser/honest`; решение принимает исход **браузерного** яруса (`RawPage.fetch_mode`), а не текст ошибки — оба яруса провалились ⇒ `unresolved` с причиной (транзиентный сбой не выдаётся за `browser`); причина **диагностична**: сбой транспорта (`both_tiers_failed`) отличается от «транспорт дошёл, но экстракция дала 0 символов» (`extraction_empty` — смотреть экстрактор, не краулер; при расхождении ярусов в отчёте обе причины). Пригодность меряется **выходом реального экстрактора**, не размером байт. `wait_for`/`rate_limit` не выдумываются: только подсказка (`needs_confirmation`) и только при `429`. URL — `--urls` > `corpus_docs` (`--corpus-db`) > синтез `https://{host}{path}/` (`confidence: low`). По умолчанию **только предложение**; `--write` (требует `--actor`) правит `domain_profiles.yaml`, сохраняя шапку байт-в-байт и не тронутые блоки дословно, при отсутствии изменений — no-op. Смена осей краула — граница `execution_change` (§13), не `methodology_version` | `packages/{case_id}/sources/crawl_probe/{date}.proposal.yaml` |

**Полный состав `hound package` — 27 подкоманд** (`hound package --help`): `new`, `show`,
`validate`, `list`, `set-archetype`, `bump-version`, `module`, `role`, `param`,
`infer-archetype`, `compose-baselines`, `baseline`, `cohort`, `scope`, `golden`, `seeds`,
`state`, `run`, `artifact`, `suggest`, `candidates`, `apply`, `scout`, `crawl-probe`, `add-product`,
`index-gc`, `builds-gc`.

Матрица «подкоманда → глаголы» (универсальной формулы `{get|list|show}` **нет**):

| Подкоманда | Глаголы |
|---|---|
| `module` | `add` · `update` · `remove` · `renormalize` · `list` · `get` |
| `role` | `add` · `update` · `remove` · `list` · `get` |
| `param` | `add` · `update` · `remove` · `get` · `list` |
| `baseline` | `add` · `update` · `remove` · `list` · `get` |
| `cohort` | `update` · `set` · `show` |
| `scope` | `set` · `clear` · `show` |
| `golden` | `list` |
| `seeds` | `list` · `add` · `disable` · `enable` · `export` |
| `run` | `show` |
| `artifact` | `list` · `get` |

Подкоманды без глаголов (собственные действия): `new` (из `packages/_template`), `show`
(сводка), `validate` (линт без записи), `list` (обзор пакетов каталога), `set-archetype`
(C запрещён при baselines), `bump-version` (semver-инкремент `package_version`),
`infer-archetype` (эвристика, без записи), `compose-baselines` (черновик наследования, без
записи), `suggest`/`candidates` (эвристики/план кандидатов — в пакет не пишут), `apply`
(**применяет** кандидатов; требует `--actor`), `scout`, `crawl-probe`, `add-product`
(добавить продукт в когорту: SERP+scout+ключи доменов; по умолчанию plan-only, `--apply`
пишет), `seeds export` (legacy-экспорт активных seed'ов в `corpus_urls.yaml`, read-only без
`--out`), `index-gc` (прунинг композиций, на которые не ссылается ни один прогон; dry-run,
`--apply` требует `--actor`), `builds-gc` (прунинг индексных сборок вне композиций; то же).

> `hound package apply`, `index-gc`, `builds-gc` с `--apply` — **разрушающие** операции над
> пакетом/индексом: по умолчанию они ничего не пишут и требуют явного `--actor` как
> носителя ответственности.

**Флаги `hound run`.** Полный набор (`hound run --help`) делится на три класса.

* **Меняют идентичность прогона** (`run_id`, §C.1.1): `--urls FILE` (`seeds_digest`),
  `--refresh {auto,revalidate,refresh,offline}` (`refresh_policy`), `--only-failed` /
  `--only-product ID` (`run_scope`), `--cycle-id` / `--update` (`cycle_id`), `--profile
  {local,split}` (`deploy_profile`), а также
  `--llm-slot`, `--llm-model`, `--retrieval-profile`, `--cutoff-k`, `--document-level`,
  `--max-depth`, `--max-documents`, `--exclude-path`, `--exclude-document` (оси контракта).
  Частичный прогон — **другой прогон**, а не resume полного.
* **Меняют объём работы, но не идентичность:** `--reset` (форсировать полный перезапуск,
  обход resume), `--from-scratch` (сброс сеанса + живой ре-фетч в обход R7-кэша),
  `--max-attempts N` (потолок попыток по URL → терминальный `failed`/`attempts_exhausted`),
  `--since-run ID` (явный predecessor для carry-forward/lineage; по умолчанию — последний
  прогон пакета из `state.db`).
* **Операционные:** `--until PHASE`, `--dry-run`, `--follow`/`--watch` + `--interval S`,
  `--run-id` (ручное переопределение), `--runs-dir`/`--db`, `--passports-dir`,
  `--render-modes`, `--env-file`, `--json`.

Оси контроля краула (D1/D2): `--max-depth D` (BFS-обход in-site ссылок; `0` — только seeds,
дефолт), `--max-documents N` (`>0`; потолок добытых страниц на `(продукт, seed-root)`),
`--exclude-path GLOB` и `--exclude-document GLOB` (повторяемые glob-блэклисты по пути URL и
по basename), `--document-level LEVEL`. Флаги уточняют **run-слой** поверх `run.yaml:crawl`
(цепочка app < run < site);
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

У `hound crawl run` — свой набор: `--until` по умолчанию **`corpus`** (если `corpus` отсечён,
краул не запускается); флаги краула те же (`--reset`/`--from-scratch`/`--only-*`/
`--max-attempts`/`--max-depth`/`--max-documents`/`--exclude-*`/`--document-level`/`--follow`),
плюс `--llm-slot`/`--llm-model`.

Подкоманды `hound package …` принимают `--package` как **путь к каталогу пакета**, так и **`case_id`** (резолвится в каталоге пакетов: `--dir`, иначе resolved `paths.packages`);
несуществующий пакет — честный `EXIT_NOT_FOUND` (команды чтения fail-closed, а не ложный успех).
`hound package validate` дополнительно fail-closed, когда каталог есть, но `package.yaml`
отсутствует: `EXIT_BLOCKED` с `blocking: package.yaml отсутствует` (не `EXIT_NOT_FOUND`
и не ложный `ok: yes`; терпимость к частичным пакетам с манифестом сохраняется).
`ok: yes` требует `LintReport.ready` (нет блокирующих **и** нет `incomplete`): незаполненные
авторские поля печатаются `incomplete:` и дают `EXIT_BLOCKED` — неполнота не проходит молча
(при этом `incomplete` **не** блокирует команды авторства, только прогон/валидацию).

`hound doctor` — read-only и по умолчанию **без сети**: проверяет Python/venv, наличие
зависимостей по группам extras (`find_spec`, без импорта тяжёлых — группы `deps:core`,
`deps:retrieval`, `deps:crawl`, `deps:extract`, `deps:safety`, `deps:ops`, `deps:embed`,
`deps:embed-lite`, `deps:dev`), заданность env/ключей
(значения **не** печатаются), файлы `config/deploy.yaml`/`.env.example`/`packages` и
package-scoped реестровые спецификации (`package.yaml: registries` → `packages/{case_id}/
registries/`, при `--package`),
запись в операционные каталоги (`runs/`/`index_store/`/`corpus_raw/`/`cache/`), раскладку
каталогов (`layout.*`) и полноту
CLI-поверхности. Каждая проверка обёрнута в try/except и даёт запись `ok|warn|fail`;
секреты эгрессии в профиле `split` живут только в edge (07). `--json` — машинный отчёт
(детерминированный: `sort_keys=True`); код возврата ненулевой при `fail`, а с `--strict` —
и при любом `warn`. `--with-selfcheck` дополнительно прогоняет `tests/selfcheck.py`.

### E.2. UX-принципы

- **Гейты — явные действия, но два разных класса.** `hound run` останавливается на G1–G4 и
  печатает, что требуется. **G1/G2 — внутренняя аттестация:** человек утверждает содержимое
  профиля пакета-якоря (когорта/роли/объём/паспорта) и матрицу роль×продукт. Второй путь —
  durable-аттестация профиля (`profile_attestation` в `state/{package_id}/state.db`) —
  объявлена и проверяется гейтом, но **производителя в прогоне нет** (design-only: строку
  пишут только тесты), поэтому как работающий механизм её описывать нельзя. Рабочий путь
  сегодня один — per-run строка `gate_decisions` (аудит-след и override), и она
  **приоритетна**. **G3/G4 — точки интеграции:** coHound не решает ревью/публикацию, а
  **получает** решение. На блоке
  пишется `runs/{run_id}/gates/{gate}-request.json` — детерминированный предмет +
  `subject_digest`; вердикт приходит извне и только записывается. Никаких автоматических
  «approve».
- **Запись решения гейта:** `hound gate --gate G1..G4 --actor <вы>` (плюс
  `--db`/`--runs-dir` + `--run-id`, `--source internal|external`, `--verdict approved|rejected`,
  `--subject-digest`, `--target`, `--notes`; `--json`). `--subject-digest` по умолчанию
  берётся из `runs/{run_id}/gates/{gate}-request.json`; **если нет ни флага, ни файла —
  отказ** (`gate_subject`, `EXIT_USAGE`): решение без привязки к предмету проходило бы гейт
  при любом состоянии прогона. `--source external` требует `--external-ref` и `--notes`.
  Гейт проходит **фактом строки** в `gate_decisions`; **устаревшее** одобрение (предмет
  изменился) гейт **не** проходит, и сообщение называет изменившийся компонент. Явный
  `--verdict rejected` блокирует прогон отдельным сообщением — молчание внешней системы не
  выдаётся ни за отказ, ни за одобрение. **Код возврата отказа — `EXIT_BLOCKED` (3)**, как у
  `run` на блокировке (запись решения состоялась, но это не успех). Повторная подача того же
  решения идемпотентна.
- **Делегирование G3/G4:** `run.yaml:gates.{g3,g4}: external|waived` (дефолт `external`).
  `waived` → фаза `skipped` с `skip_reason=gate_delegated_external`, waiver-строка в
  `gate_decisions` и `gate_waivers` в манифесте. Невидимый waiver запрещён. Политика гейтов —
  **процесс**, а не условие измерения: в `EXECUTION_CONTRACT_FIELDS` её нет, `run_id` от неё
  не зависит.
- **Идемпотентность и resume:** повторный `hound run` продолжает с последней завершённой фазы;
  `--until` даёт пошаговый контроль. В CLI-JSON resume-короткое-замыкание уже завершённых фаз
  сериализуется под ключом `resumed_skipped` — это **отдельно** от `PhaseStatus.skipped` (пропуск
  из-за отсутствия предпосылки, §5); прогон останавливают только `blocked`/`failed`.
- **Сухой прогон:** `--dry-run` показывает план/бюджеты без исполнения.
- **Чтение не создаёт состояния.** Команды, которые только читают (`resolve`, `review`,
  `sensitivity`, `golden`, `report`, `diff`, `crawl status|list`, `runs`, `status`),
  открывают БД через `file:…?mode=ro` (`core.store.sqlite.connect_read_only`): каталог,
  файл и схема не создаются, живой прогон читается через WAL без миграции. Нет файла БД →
  `EXIT_NOT_FOUND` (4), а не «создали пустую»; `hound status` тем же кодом отвечает на
  отсутствующий прогон («прогон ещё не создан» отличимо от опечатки); чужой
  `schema_version` по-прежнему `EXIT_USAGE` (гейт `11` §2.6).
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
| 1 | Расщепить `core/schemas.py` → `domain/{enums,models,invariants,r2,anchors}.py` | `tests/selfcheck.py` зелёный | ✅ сделано (T1.2; `core/domain/enums.py:1` … `core/domain/anchors.py:1`) — монолит **удалён**, гейт `tests/selfcheck.py:1123` |
| 2 | Расщепить `core/db.py` → `store/sqlite.py` (DDL) + `ports/store.py` (контракт) | DDL-тест + триггеры | ✅ сделано (T1.3; `core/store/sqlite.py:21`) — монолит **удалён**, тот же гейт `tests/selfcheck.py:1123` |
| 3 | Ввести `ports/*` (Protocol) и адаптеры-заглушки | импорт-граф CI (итерация 4) | ✅ сделано (`core/ports/` — 20 модулей портов, напр. `core/ports/store.py:1`; импорт-граф — `tests/selfcheck.py:948`) |
| 4 | Перенести проверки `Cell._enforce_invariants` в `domain/invariants.py` (вызов из Pydantic) | selfcheck | ⬜ не сделано — правила **продублированы**, а не перенесены: Pydantic-валидатор `core/domain/models.py:1172` держит проверки inline (77 `raise ValueError` в модуле, импорта `core.domain.invariants` нет — он был бы циклическим), а `core/domain/invariants.py:196` (`check_cell`) лишь «зеркалит» их для pipeline/store/selfcheck |

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

**Открытые вопросы к итерации 4:** формат импорт-графа CI — ✅ сделано (`tests/selfcheck.py:982-1024`,
`_import_graph`, вызов `:354`; AST по `core.*`); структура логирования — ✅ сделано
(`core/util/logging.py:1` — JSON-lines `runs/{run_id}/logs/{phase}.jsonl`, поля 04 §B.1); бэкап/
восстановление прогона — ✅ сделано (`core/store/backup.py:173` `backup_run` + `:211` `restore_run` —
zip-бандл + `sqlite3.Connection.backup`, не копирование файла); как считать `prompt`-шаблоны версиями — ✅ сделано
(`core/pipeline/prompt.py:117` — по файлам `templates/{version}.txt`, версия входит в `prompt_hash`).
Отдельная корпусная БД — ➖ снято (S6, B.2; единая БД сохраняет FK).
