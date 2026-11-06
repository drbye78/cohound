# Аудит крупных модулей и необходимости декомпозиции

**Дата:** 30 октября 2026 года  
**Проект:** coHound

## Вывод

Полный рефакторинг кодовой базы или массовое дробление файлов не требуется. Нужна адресная
декомпозиция нескольких узлов, прежде всего `core/pipeline/corpus.py`.

Критерии оценки: независимые причины изменения, coupling через общий lifecycle,
тестируемость, риск изменения порядка persistence и методологической семантики.

| Модуль | Решение | Приоритет |
|---|---|---:|
| `core/pipeline/corpus.py` | Декомпозировать по контрактам | **P0** |
| `core/store/sqlite_store.py` | Внутренние repositories/helpers за фасадом | **P1** |
| `core/pipeline/evaluate.py` | Пока оставить, выделять только по задаче | P2 |
| `core/pipeline/report.py` | Не трогать методологическое ядро | P2/P3 |
| `core/cli/wiring.py` | Сначала устранить extraction coupling | P2 |
| `core/cli/context.py` | Позже разделить runtime-контексты | P2/P3 |
| `core/pipeline/phases.py`, `runner.py` | Не декомпозировать сейчас | P3 |
| `core/domain/models.py`, `scoring.py` | Не трогать сейчас | P3 |

## P0: `core/pipeline/corpus.py`

Файл содержит примерно **10 226 строк**. `_build_corpus()` начинается около
`core/pipeline/corpus.py`, принимает порядка 50–60 параметров и координирует:

- crawl и durable session;
- resume/interrupted semantics;
- retention сырых данных;
- extraction и worker lifecycle;
- archive descent;
- chunking, embedding и index build;
- reuse/incremental state;
- coverage и SERP;
- persistence, observability и финализацию продуктов.

Это не только проблема читаемости: состояние и порядок побочных эффектов связаны общими
замыканиями, поэтому изменение одной оси может затронуть другие стадии.

### Естественные границы

| Область | Ориентиры | Оценка |
|---|---:|---|
| Coverage/status | `624–1000` | Первый кандидат |
| Extraction runtime | `1011–1535` | Первый кандидат |
| Index/embed build | `1536–1619`, `2544–3014` | После extraction |
| Resume/retention | `3014–3294` | Отделять осторожно |
| Archive/member ingestion | `3294–3560` | Локально отделим |
| SERP contour | `3561–3950` | Отдельная граница, но есть cycle |
| Main orchestration | `3968–10196` | Выносить последним |

### Цикл `corpus` ↔ `serp_loop`

Есть двусторонняя зависимость:

- `core/pipeline/serp_loop.py:65` импортирует `IndexBuild` из `corpus`;
- `corpus.py` поздними импортами использует SERP-логику около строк `3699`, `3944`,
  `5512`, `5577`.

Локальные импорты откладывают технический import cycle, но не устраняют архитектурную
связность. До глубокой декомпозиции корпуса нужно вынести `IndexBuild` или нейтральный
build contract в отдельный seam, например `core/pipeline/index_build.py`.

Целевая зависимость:

```text
corpus     → index_build contract
serp_loop  → index_build contract
```

а не `serp_loop → corpus`.

### Рекомендуемый порядок

#### 1. Extraction runtime

Вынести в `core/pipeline/extraction_runtime.py`:

- `_ExtractRequest`;
- `ExtractionPool` и `_ExtractPools`;
- `_extract_worker`, `_run_extract`, `set_worker_extractor`;
- worker RSS/arena helpers;
- extraction cache/version helpers.

У runtime уже есть самостоятельный тестовый контур `tests/pipeline/test_corpus_extract_pool.py`.
Одновременно исчезнет private coupling из `core/cli/wiring.py`, где используются
`ExtractionPool` (`wiring.py:149`), `set_worker_extractor` (`wiring.py:2278`);
`_extract_worker` и `_run_extract` переехали в `core/pipeline/extraction_runtime.py`
(`:441` и `:313` — волна декомпозиции; прежний адрес был в `wiring.py` на строке 2250).

#### 2. Coverage/status

Вынести `derive_corpus_coverage`, `_product_notes`, `_final_product_status` и
`_persist_corpus_coverage` (`corpus.py` до правки L0; после L0 — `corpus.py`). Это позволит получить дешёвые
unit/property tests без запуска crawler или extractor.

#### 3. Index/embed build

Вынести `IndexBuild`, `_embed_batched`, `_embed_and_build`, `_derive_partition_build_id`,
content/extractor tokens и `_partition_reuse_reason` (ориентиры: `corpus.py`,
`2002–2109`, `2544–3014`).

Желательный контракт: `ProductIndexInput → ProductIndexResult`. Не передавать туда весь
`RunContext`, общий store и замыкание `_build_corpus`.

Проверять: детерминизм build identity, новый build при изменении extraction/chunker/embed
axis, immutability сборок, порядок `physical build → index_manifest`, product-scoped retrieval.

#### 4. Archive/member ingestion

Вынести `_member_route`, `_member_extract_request`, `_member_extract_unavailable` и
`_ingest_member_doc` (`corpus.py`). Это соответствует
`tests/pipeline/test_corpus_descent.py`.

#### 5. Crawl/session orchestration

Оставить на последнюю очередь. Здесь пересекаются resume, retention, session item status,
retry/terminal failure, progress, partial extraction, persistence order и product finalization.

## P1: `core/store/sqlite_store.py`

Файл содержит около **3087 строк**, класс `SqliteStore` — более 100 методов. Он одновременно
является run/phase, cell/evidence, gate, corpus, crawl-session, index repository и source
resolver, а также schema compatibility layer и владельцем transaction/batch policy.

Естественные диапазоны:

```text
cells/evidence        472–1055
gates/search/registry 1065–1300
run/manifest          1301–1509
corpus/index          1509–1812, 2149–2922
crawl sessions        1812–2149
source resolution     2922–3078
```

Сохранить публичный фасад `SqliteStore(StorePort)`. Не создавать отдельные подключения и
не менять transaction boundaries. Сначала выделить внутренние модули:

```text
core/store/sqlite_rows.py
core/store/sqlite_json.py
core/store/sqlite_corpus.py
core/store/sqlite_crawl.py
core/store/sqlite_manifest.py
```

Все они должны использовать тот же `sqlite3.Connection`, общий migration path и текущую
политику транзакций.

Порядок: row conversion/legacy parsing → corpus/chunks/index → crawl sessions →
run/manifest → cells/evidence → gates/source resolver.

`cells/evidence` отложить: там пересекаются Pydantic-инварианты, SQL-триггеры, legacy schema
и `_guard_against_downgrade()` (`sqlite_store.py`).

## `evaluate.py`: пока не дробить агрессивно

`evaluate.py` содержит примерно **4052 строки** и объединяет retrieval, evidence,
denial/absence semantics, carry-forward, cache, evaluation и stale revalidation.

Методологически полезно, что правила I2/I2c/I4/I17/I18/I20/I21 находятся рядом с
orchestration оценки. Не следует разбивать `evaluate_cell()` на сервисы, каждый из которых
самостоятельно меняет `Cell`. Порядок должен оставаться:

```text
retrieval
→ validation
→ denial/absence
→ reuse confirmation
→ persistence/provenance
```

Допустимые будущие кандидаты при конкретной волне: `evaluation_retrieval.py`,
`evaluation_revalidation.py` и policy-модуль denial/absence.

## `report.py`: оставить публикационное ядро централизованным

`report.py` содержит около **3222 строк**: serialization helpers (`341–634`), manifest
(`743–1286`), battle card (`1311–1548`), anomalies (`1671–2110`), diff/change-class
(`2164–2670`), artifact writers (`2686–2963`) и `write_reports` (`2963+`).

Это последняя publication boundary. Разбиение может нарушить Coverage=0 blocking,
исключение preliminary, diff boundary classification, различие execution/methodology
changes или публикацию anomalies.

Позже допустимо выделить только `report_diff.py`, `report_anomalies.py`,
`report_renderers.py` и `report_manifest.py`, сохранив фасадные public imports.

## `wiring.py` и `context.py`

`wiring.py` содержит около 3212 строк; для composition root широкий импортный граф ожидаем.
После устранения extraction private coupling срочное механическое дробление не нужно.

`context.py` содержит около 2749 строк и является вторым CLI orchestration root: paths,
package inputs, state/locks, gates, progress, report payloads, port resolution и parser
helpers. После стабилизации corpus API возможны `context_inputs.py`, `context_state.py`,
`context_progress.py` и `context_output.py`.

## Что не нужно трогать сейчас

- `core/pipeline/phases.py` — composition boundary с хорошим покрытием.
- `core/pipeline/runner.py` — единый владелец `PHASES`, gates, phase state и resume.
- `core/pipeline/serp_loop.py` — сначала устранить зависимость от corpus через build contract.
- `core/domain/models.py` — разделение потребует re-export, проверки schema и forward refs.
- `core/domain/scoring.py` — методологически связный модуль, влияющий на golden.

## Безопасный план работ

### Шаг 0: baseline и контракты

```bash
free -m

.venv/bin/python -m py_compile \
  core/pipeline/*.py \
  core/cli/*.py \
  core/store/*.py \
  core/domain/*.py

.venv/bin/python -m pytest -q --collect-only
```

Проверить contract tests для `build_corpus`, `ExtractionPool`, `IndexBuild`, `evaluate_cell`,
`build_manifest`, `write_reports`, `SqliteStore` и старых public import paths.

### Шаг 1: extraction runtime

```bash
.venv/bin/python -m pytest -q \
  tests/pipeline/test_corpus_extract_pool.py \
  tests/pipeline/test_corpus_concurrency.py \
  tests/cli/test_crawler_wiring.py \
  tests/cli/test_extract_routing.py
```

### Шаг 2: coverage/status и index/build

```bash
.venv/bin/python -m pytest -q \
  tests/pipeline/test_corpus.py \
  tests/pipeline/test_corpus_phase_boundary.py \
  tests/pipeline/test_corpus_runs_boundary.py \
  tests/pipeline/test_corpus_parity.py \
  tests/pipeline/test_corpus_embed_progress.py \
  tests/pipeline/test_embed_cache.py
```

### Шаги 3–6

1. Устранить `corpus ↔ serp_loop`, проверив immutable build, build identity, composition
   manifest, SERP termination и exhaustion protocol.
2. Вынести corpus/chunk/index persistence helpers за фасадом `SqliteStore`, не меняя
   `StorePort`, connection, transaction boundaries, schema migration и legacy read path.
3. Перейти к crawl/session orchestration только после выделения extraction и persistence seams.
4. Декомпозировать evaluate/report только при появлении независимой волны изменений.

## Главная рекомендация

Начинать не с уменьшения `corpus.py` целиком, а с трёх проверяемых границ:

1. `extraction_runtime`;
2. `coverage/status`;
3. `index/build`.

Так можно снизить coupling и улучшить тестируемость, сохранив порядок persistence,
методологические инварианты и публичные import paths.
