# 16. Версионная граница `runs`: схема, идентичность, состав

> **Статус: применено 29.10.2026 (as-built).** Всё описанное ниже реализовано в `core/`;
> §8 описывает, как это применялось, а не как это предстоит делать, и §10 — что сверка
> спецификации с кодом вскрыла по ходу (включая три пункта, которых в спецификации не
> было, и один, где реализация оказалась строже спецификации).
>
> **Решение владельца 29.10.2026:** всё, что меняет материал `run_id` или состав прогонной
> БД, делается **одной** границей. Две отдельные границы (`embed_device` отдельно, схема
> отдельно) обнулили бы прогресс дважды и дважды порвали бы межцикловый diff без пользы.
>
> **`METHODOLOGY_VERSION` — `0.2.0`** (класс `methodology_change`, §1). `SCHEMA_VERSION`
> остался `3.13.0`: всё аддитивно. `SEAM_SCHEMA_VERSION` остался `"1"`.
>
> **ОБНОВЛЕНО 2026-11-01 (волна S плана 17).** Решение «без bump, аддитивно» **отменено
> владельцем**: совместимость схемы не сохраняется. `SCHEMA_VERSION = "3.14.0"`,
> `STATE_SCHEMA_VERSION = "6"`, `EDGE_JOBS_SCHEMA_VERSION = "1"`; аддитивные
> `ALTER TABLE ADD COLUMN`-шимы снесены во всех трёх БД, все колонки и индексы объявлены в
> `CREATE TABLE`, файл прежней версии отвергается гейтом (`SchemaVersionMismatch` →
> `EXIT_USAGE`/`schema_version`). Запись ниже сохранена как **история решения на 29.10.2026**:
> она объясняет, почему граница A обошлась без bump'а, и больше не описывает поведение кода.
>
> **ОБНОВЛЕНО 2026-11-05 (планы 19/20, F1–F3).** Действующие значения: `SCHEMA_VERSION =
> "3.20.0"` (3.17.0 — `crawl_policy_digest`, план 19; 3.18.0 — `runs.corpus_snapshot_json`;
> 3.19.0 — `corpus_artifact`; 3.20.0 — `corpus_coverage.roots_json` + `cells.absence_as_of`),
> `STATE_SCHEMA_VERSION = "12"` (8/9 — `crawl_policy`/`corpus_capture`, 10 —
> `container_state`, 11 — `capture`/наблюдения `url_state`, 12 — отпечаток пространства
> снятия), `METHODOLOGY_VERSION = "0.3.0"` (датированное отсутствие I2d, F3). Всё ниже —
> история границы, не описание текущего кода.

**Дата:** 2026-10-29. **Основание:** `09` §14.1 (состав границы), `14` оговорка 5 (замер
устройства эмбеддера).

---

## 1. Почему одна граница

`run_id` — функция объявленных входов (`derive_run_id`, `core/pipeline/plan.py:79`), и
`embed_device` входит в неё через `EXECUTION_CONTRACT_FIELDS`. Значит любая правка,
добавляющая ось в контракт, даёт новый `run_id` **всем** будущим прогонам и превращает
любой `hound diff` против старых прогонов в `execution_change`. Сделать это дважды (сначала
ось, потом схему) — значит дважды объявить «условия измерения сменились» на одном и том же
изменении.

Вторая причина — порядок. Живой прогон `ru-pg-dbms-3d3dc7c3c969` пишет `runs/…/cohound.db`
и импортирует `core/pipeline/corpus.py`/`phases.py`; правка этих файлов на ходу ломает его.

**Третья причина (решение владельца 29.10.2026): в границу входит правило «`❌` требует
`live|revalidated`»** (`09` §11.2; **снято F3 плана 20** — носитель теперь датированный,
I2d, `09` §11.2 врезка). Оно меняет условие публикации `❌`, то есть
absence/evidence-модель, а значит по `AGENTS.md` §13 это `methodology_change` с
обязательными `golden-full` **и** пере-аннотацией golden. Правило неотделимо от
`corpus_docs.reuse_kind`: без колонки оно неисполнимо, а колонка без правила — только
видимость. Разносить их на две границы значило бы платить за `golden-full` дважды.

**Четвёртая причина (решение владельца 29.10.2026): в ту же полосу входят два дефекта
краула**, найденные живым прогоном (`09` §6.3). Оба меняют корпус, то есть по отдельности
дали бы `execution_change` — а `golden_required_for` возвращает `full` **и** для
`execution_change` (`core/pipeline/run_identity.py:372`; `09` §12.1). Отдельная полоса
стоила бы второго `golden-full` и второго нового `run_id` на том же изменении корпуса.
Класс полосы от этого не меняется: `methodology_version` поглощает `execution_change`
(правило `❌` сильнее), поэтому объединение **дешевле**, а не «менее строго».

**Следствие, которое надо назвать прямо: класс всей границы — `methodology_version`, а не
`execution_change`.** Остальные пункты сами по себе дали бы `execution_change` (ось
`embed_device`) или `content_change` (формат `chunk_id`); самый сильный класс поглощает
остальные, и это не бюрократия: если бы правило поехало отдельно, оно всё равно потребовало
бы `golden-full`, то есть второй прогон пере-аннотации на том же изменении.

---

## 2. Что входит и что не входит

| Пункт | Зачем | Класс изменения |
|---|---|---|
| ось `embed_device` (`cpu\|cuda`) в контракте | устройство эмбеддера сегодня выбирает torch, а `run_id` об этом не знает (`14` оговорка 5) | `execution_change` |
| `embed_device` в материале `pb` и в проверке переиспользования партиции | иначе CPU-партиция молча переиспользуется CUDA-прогоном | `content_change` (следствие) |
| `vector_digest` в проверке переиспользования | сборщик его пишет (`core/adapters/index_npy.py:276`), `_guard_immutable` сверяет (`:309`), но при переиспользовании сборка не строится — гард не исполняется | `content_change` |
| `content_hash` в `chunk_ids.jsonl` | **необходимое условие** предыдущего пункта, см. §5 | формат партиции `partition-2` |
| `embed_device` в ключе `embed_cache` | иначе CPU-прогон получит CUDA-векторы | стоимость, не идентичность |
| `runs.cohort_ref` — писать `cfg.cohort_ref`, а не имя файла когорты (**✅ сделано**) | манифест показывает ревизию, которой в `run_id` не было (`09` §12.2) | провенанс, `run_id` **не** меняется |
| `runs.run_scope`, `runs.product_plan_json` | `--only-product` меняет смысл среза и не виден в артефакте (`09` §13.6) | провенанс, `run_id` **не** меняется |
| таблица `run_product` | статус продукта в прогоне живёт только снимком в `state.db` (`09` §4.3/§6.5) | наблюдаемость |
| `corpus_docs.http_status`/`reuse_kind` | `❌` на retained-байтах без серверного подтверждения сегодня невыразим (`09` §11.2); колонка **исполняет** правило `❌` ⇒ `live\|revalidated`, а не только показывает его | **`methodology_change`** (условие публикации `❌` меняется) |
| `chunk_id` с `doc_version_id` вместо `doc_hash` | один и тот же текст на разных уровнях извлечения давал один `chunk_id` (`09` §14.6 п. 11); `chunker_version` в него **не** входит — довод в §6.1 | `content_change` |
| **дефект краула 1**: реконструкция бюджета на resume заряжает `max_documents` первому корню-претенденту, а не владельцу | потолок умножается на число перезапусков: измерено 1610 `fetched` при потолке 800 = `2 × 800` (`09` §6.3) | `execution_change` (корпус) |
| **дефект краула 3**: зарядка бюджета на resume не делает заряженный URL дренируемым (дренаж идёт только через `add_revalidate_seed`, а тот строится лишь для `incremental`) | у `pangolin` корень заряжен в потолок 600/600 ⇒ ноль `fetched` в попытке ⇒ `corpus_chunks` 25 при 509 документах, хотя 4935 чанков лежат в `state.db` (`09` §6.3) | `execution_change` (корпус) |
| **дефект краула 2**: `Frontier._seen` дедуплицирует по точной строке URL | `/page` и `/page/` — два элемента очереди: измерено 1164 из 2402 `fetched` (48.5 %) у `digital_q_database`, 54 из 629 у `pangolin`, 0 у `jatoba` (`docs/reports/measurements/2026-10-29-crawl-url-duplication.md`) | `execution_change` (корпус) |

**Не входит** (остаётся на свои полосы): carry-forward ячеек и `cell_measurement` с
писателем (`09` §10 — волна 2), объявленный backend эмбеддера (`deploy.embed` целиком,
`14` §6 п. 5), межпрогонный `--only-failed` и `offline` (`09` §2.4), `RemoteEmbedder`. Файловый lock на
пакет (`09` §4.4) **сделан отдельно** (`d80a6e4`) — в границу не входит.

---

## 3. Схема прогонной БД: **без** bump `SCHEMA_VERSION` (решение 29.10.2026 — отменено 01.11.2026)

> ⚠️ Раздел описывает **отменённое** решение. Сегодня: версия поднята, шимов нет, старая БД
> отвергается. Ссылки на `_ensure_columns` ниже указывают на удалённый код — они оставлены
> как след принятого тогда решения, а не как описание текущего состояния.

**Решение: `SCHEMA_VERSION` остаётся `3.13.0`** (`core/store/sqlite.py:15`). Все изменения
ниже — аддитивные: колонки добавлял идемпотентный `_ensure_columns`
(удалён в 3.14.0: шимов `ALTER TABLE` больше нет, `core/store/sqlite.py`), таблицу —
`CREATE TABLE IF NOT EXISTS` в `DDL`. Гейт
`_reject_foreign_schema` (`:615-624`, вызовы `:683` и `:701`) существует ради
**несовместимого** чтения, а не ради счётчика ревизий: поднимать версию на аддитивном
изменении значило бы запретить чтение уже завершённых прогонов без причины.

Практическое следствие, ради которого это и выбрано: **границу можно применять сразу после
`report`**, не дожидаясь, пока старые `runs/*/cohound.db` станут нечитаемыми. Ни один
существующий прогон не теряется.

### 3.1. `runs` — новые колонки

```sql
ALTER TABLE runs ADD COLUMN run_scope TEXT;           -- full|only_failed|product:<id>
ALTER TABLE runs ADD COLUMN product_plan_json TEXT;   -- ProductPlanSet: mode/blocked/reasons
ALTER TABLE runs ADD COLUMN embed_device TEXT;        -- cpu|cuda — объявленная ось (наблюдаемо)
```

`embed_device` в строке — не второй носитель идентичности (он уже в
`execution_contract_digest`), а наблюдаемое поле: без него «на чём считались векторы» нельзя
прочитать запросом, только восстановить из контракта.

### 3.2. `run_product` — таблица статуса продукта в прогоне

```sql
CREATE TABLE IF NOT EXISTS run_product (
    run_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    mode              TEXT NOT NULL,            -- new|full|incremental
    status            TEXT NOT NULL,            -- pending|running|complete|partial|failed|blocked
    scope             TEXT NOT NULL DEFAULT 'full',
    phase             TEXT,
    coverage_complete INTEGER,                  -- NULL = покрытие не считалось
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
```

Пишется на границе продукта (там же, где `corpus_docs`, `09` §4.7) и переписывается в конце
фазы — так же, как `corpus_coverage`. Прецедент записи — `store.save_corpus_coverage(run_id,
coverage)` (`core/pipeline/corpus_coverage.py:654`): одна функция на продукт, вызывается на границе,
а не накоплением в памяти. `cells_carried`/`cells_measured` **не заводятся**: переноса
ячеек нет (`09` §10), и колонка, всегда равная нулю, — ложь о том, что механизм
существует.

**Побочный эффект, который надо принять явно:** `cells_total`/`cells_pending`/`baseline_complete`
из `run_product` — те же величины, что `product_end` пишет в `results[].notes` прогона
(`core/pipeline/corpus_coverage.py:276-298`). Два носителя одной величины разойдутся, если один из них обновят, а
второй нет. Решение границы: **`run_product` — носитель, `notes` остаётся для оператора**
(текст), и расхождение между ними — аномалия отчёта, а не допустимое состояние.

### 3.3. `corpus_docs` — происхождение наблюдения

```sql
ALTER TABLE corpus_docs ADD COLUMN http_status INTEGER;  -- NULL = сервер в этом цикле не отвечал
ALTER TABLE corpus_docs ADD COLUMN reuse_kind TEXT;      -- live|revalidated|reused|stale
```

Зачем: `url_state.reuse_kind` уже различает «сервер ответил 304» и «взяли retained-байты»
(`core/store/state_sqlite.py:106`), но в прогонную БД это не доезжает, и 304-документ
выглядит как живой 200 (`09` §11.2). Без этих колонок правило «`❌` нельзя публиковать на
неподтверждённом наблюдении» неисполнимо, сколько бы его ни уточнять текстом.

**Правило записи — монотонное, и это не деталь.** Значение берётся из пути страницы
**этого** прогона: живой fetch — `live`/`revalidated`, чтение retained-байтов
(`_resume_page`, `core/pipeline/corpus.py:828-881`) — `reused`. Resume перечитывает и те документы, которые
в этом же `run_id` уже были получены от сервера (до перезапуска хоста), поэтому строка
этого прогона, несущая `live`/`revalidated`, **не понижается** до `reused`: иначе resume
стирал бы состоявшееся наблюдение. Наследовать `url_state.reuse_kind` **нельзя** — там
последняя классификация, в том числе из прошлого цикла (замер 29.10: 2157 `live` + 2009
`revalidated` + 0 `reused`, при том что 629 элементов `pangolin` перечитаны из
retained-байтов).
`reuse_kind IS NULL` в `corpus_docs` = «наблюдение не записано» (legacy-строки) ⇒ `❌` не
публикуется; писатель границы всегда пишет значение.

**Оба значения уже в руках у единственного писателя** — новой проводки не нужно:
`CorpusDoc(...)` собирается в `core/pipeline/corpus_ingest.py:1693`, рядом живут `reuse`
(`:5197` `reused = active_reuse is not None`) и `page.status_code` (он же уже уходит в
`url_state` как `last_http_status`, `:5217`; второй путь — `:5053`). Точка записи одна — `persist_docs`
(`core/pipeline/corpus_ingest.py:1346`, докстринг сам это фиксирует).

**Что спецификация обязана решить, а не оставить на ходу:** поле `CorpusDoc` или kwarg
`upsert_corpus_doc`. Прецедент в коде — `origin` (`core/pipeline/corpus_index.py:139-141`): это kwarg, а не
поле модели, с доводом «доменная модель не получает смысла, которого у неё нет». Довод
здесь **не переносится**: `origin` пишется и назад не читается, а `http_status`/`reuse_kind`
обязаны читаться — `hound diff` строит дельту документов по `corpus_docs`
(`core/cli/commands/diff.py:105`), а правило `❌`-допустимости (I2c) обязано спрашивать
происхождение наблюдения там же. Решение границы: **поля
`CorpusDoc`** + чтение в `iter_corpus_docs` (`core/store/sqlite_store.py:2195`, явные
kwargs) + `_has_column`-терпимость к необязательным колонкам
(`core/store/sqlite_rows.py:77`). ✅ As-built — так и сделано.

### 3.4. `corpus_doc_version` — отображение версии документа в строку корпуса (**as-built**)

```sql
-- Версия документа → строка корпуса. Производный индекс, не второй носитель документа:
-- dv уникален (включает content_hash, extractor_version и level), doc_id — нет
-- (content_hash без уровня), поэтому отношение 1:N и колонкой не выражается.
CREATE TABLE IF NOT EXISTS corpus_doc_version (
    doc_version_id TEXT PRIMARY KEY,
    product_id     TEXT NOT NULL,
    doc_id         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_corpus_doc_version_doc
    ON corpus_doc_version (product_id, doc_id);
```

**Зачем это вообще нужно.** `chunk_id` после §6 несёт `doc_version_id`, а `source_id` — это
`chunk_id`. Правило «`❌` требует `live|revalidated`» (`09` §11.2) находит документ по
`source_id`, то есть обязано отобразить `dv_...` в строку `corpus_docs`. Обратной функции у
`short_hash` нет, а `doc_id` не выводится из `dv` — значит отображение обязано быть
**записано**. Без него правило снимало бы подтверждённые `❌` как «наблюдение не записано»
(§10 п. 8–9). **Зачем это нужно после F3:** отображение читает I2d, чтобы найти
`corpus_artifact` по `doc_version_id` (дата носителя адресована версией документа); писатель
прежнего правила удалён, таблица осталась носителем адреса.

**Почему таблица, а не колонка `corpus_docs.doc_version_id`.** `doc_id` дедуплицируется по
`content_hash`, который **не включает уровень извлечения**, а `document_level` объявлен
per-site (`core/pipeline/crawl_frontier.py:636-641`) — два уровня одного текста в одном прогоне
законны. Колонка держит одну версию на строку, то есть теряет вторую молча. Воспроизведение
и цена — §10 п. 8.

**Где читается.** `_doc_id_by_version` (точечно, для `resolve`) и
`iter_corpus_doc_versions(run_id, product_id)` (для правила `❌`). Пишется в
`upsert_corpus_doc` — в той же транзакции, что строка корпуса, идемпотентно по PK. В БД
прошлой версии таблицы нет: оба читателя возвращают `None`/пусто, а не падают.

---

## 4. Идентичность: ось `embed_device`

### 4.1. Объявление

- app-слой: `deploy.yaml:embed.device` — секция `EmbedConfig` в `DeployConfig`
  (`core/config/deploy.py`; `extra="forbid"`, поэтому ключ без модели отвергается — секция
  появилась вместе с полем, как и требовалось). ✅ **As-built:** обе модели существуют
  (`EmbedConfig`, `EmbedRunConfig`); `DeployConfig` теперь несёт
  `profile`/`edge`/`extractor_zone`/`search`/`crawl`/`llm`/`embed`, и секция несёт **только**
  оси `device` и `precision` — внешний backend (`backend`/`base_url`/`model_version`) не
  объявлен и остаётся решением `14` §6 п. 5.
- run-слой: `run.yaml:embed.device` (`EmbedRunConfig` рядом с `CrawlRunConfig`,
  `core/domain/models.py:493`), дефолт `None` — по той же причине, что у `pdf_device`
  (`:543-549`): дефолт run-слоя перекрыл бы app-слой в `_pick`.
- **per-site слоя нет**: устройство — свойство хоста исполнения, а не сайта (тот же довод,
  что в `resolve_pdf_device`, `core/pipeline/crawl_frontier.py:459-469`).
- Резолвер: `resolve_embed_device(app, run)` — копия `resolve_pdf_device` без site-слагаемого.
  Дефолт `cpu`, значения только `cpu|cuda`, `auto` **нет** (R5: объявленная ось обязана
  действовать). Enum `EmbedDevice` — в `core/domain/enums.py` рядом с `PdfDevice` (`:531-552`).
- **Порт эмбеддера не меняется.** `core/ports/embedder.py:15-21` знает только `model_id`/`dim`,
  и это правильно: устройство — объявленная ось прогона, а не свойство адаптера, которое
  можно у него спросить (спрашивание и есть дефект `14` оговорки 5). Значение протаскивается
  **явным параметром**: `core/pipeline/phases.py:1249` → `core/pipeline/corpus.py:1496` (объявление оси) и
  `:4336`/`:4370` (передача рядом с `embed_model_id`), там же, где уже идёт `embed_model_id`
  (`core/pipeline/corpus.py:2268` — единственный источник этого значения).

### 4.2. Контракт (**✅ as-built**)

`EXECUTION_CONTRACT_FIELDS` — 28 → **29** полей (`core/pipeline/digests.py:88-205`):
добавляется `embed_device` рядом с `extractor_stack` (та же природа: устройство как условие
измерения). Значение кладёт `build_execution_contract` (`core/pipeline/run_identity.py:241`)
из `DeclaredExecutionAxes` — там же, где `extractor_stack` (`core/cli/execution_axes.py:633-692`).

**Совместимость чтения проверена:** `execution_contract_digest(values)`
(`core/pipeline/digests.py:408-450`) требует точного набора ключей, но вызывается ровно из
одного места — `run_identity.py:325`, внутри `build_execution_contract`, то есть только на
живом прогоне. Пересчёта из сохранённого словаря нет; `hound diff` сравнивает сохранённые
строки (`core/pipeline/report.py:2064-2123`), а отсутствующее на манифесте поле уходит в
`missing`, а не в исключение. Поэтому старые прогоны читаются, а их diff честно говорит
`execution_change: embed_device`.

`RunManifest` (`core/domain/models.py:1541-1753`) получает поле `embed_device`, и этого
достаточно: `_contract_field_attr` (`core/pipeline/report.py:2554-2557`) сопоставляет имя
поля контракта с атрибутом манифеста напрямую, а `_CONTRACT_FIELD_GETTERS` (`:2490`)
заведён только для полей **без** одноимённого атрибута (сейчас там `serp_rounds`/`serp_mode`).
Без поля на манифесте диагностика назовёт ось «недоступной» вместо «изменённой».

Строку манифеста в БД тоже надо дописать: `persist_manifest`
(`core/store/sqlite_store.py:1196`) — **явный список** из 29 колонок, а не `SELECT *`.
Колонка, не вписанная туда, останется `NULL` навсегда: штатного `ALTER TABLE` больше нет
(схема версионируется, старая БД отвергается), поэтому расхождение списка с `DDL` — дефект,
а не «домигрируется». Чтение — `read_manifest` (`:2486-2665`), где необязательные поля идут
через `_has_column` (`core/store/sqlite_rows.py:77`).

### 4.3. Выбор эмбеддера перестаёт быть «что установлено»

`_resolve_embedder` (`core/cli/wiring.py:2871-2954`) сегодня выбирает по факту установки:
`model2vec` → `LocalEmbedder` → `None`. `LiteEmbedder` — NumPy/CPU by design
(`core/adapters/embed_model2vec.py:4`), и `device` он игнорировать обязан.

Правило границы — минимальное, backend остаётся недекларированным (это `14` §6 п. 5):

| Объявлено | Выбор |
|---|---|
| `cpu` | как сегодня (`Lite` → `Local`): обе реализации CPU-честны |
| `cuda` | `Lite` **исключается** с видимой заметкой (он не может исполнить ось), берётся `LocalEmbedder`; недоступен → `EXIT_USAGE` с числами |

Тихого фолбэка нет: прогон, объявивший `cuda`, не должен получить CPU-векторы под тем же
`run_id` — это ровно тот класс, ради которого ось вводится. И наоборот: `cuda` без
устройства — отказ, а не CPU (прецедент `require_pdf_device`).

`LocalEmbedder` получает `device=` в конструктор и передаёт в
`SentenceTransformer(...)` (`core/adapters/embed_local.py:210-214`) — сегодня аргумента нет,
и устройство выбирает torch.

---

## 5. Партиция: формат `partition-3`

`_PARTITION_SCHEMA = "partition-1"` (`core/pipeline/corpus_index.py:48`) → `partition-2`
(граница A, 29.10.2026, ниже) → `partition-3` (06.11.2026: материал `pb` получил
объявленную точность `embed_precision`, ось `embed.precision`; сам формат партиции от
точности не изменился). Ревизия схемы уже даёт новые `pb` (старые партиции остаются на
диске, но не переиспользуются) — это и есть механизм, которым вносятся изменения.

**✅ As-built, и цена оказалась больше, чем «новый `pb`»:** `recompute_vector_digest`
возвращает `None` для **всех десяти** существующих партиций — они `partition-1` и не несут
`content_hash` в `chunk_ids.jsonl`. То есть после границы не переиспользуется **ни одна**
партиция, и первый прогон пере-эмбеддит корпус целиком — независимо от оси устройства.
Измерено и опубликовано: `docs/reports/measurements/2026-10-29-embed-cache-device-migration-cost.md`.

### 5.1. `embed_device` в материале и в манифесте (**✅ as-built**)

Материал `_derive_partition_build_id` (`core/pipeline/corpus_ingest.py:228-241`) получает `embed_device`
рядом с `embed_model_id`; манифест партиции — ключ `embed_device` в том же явном словаре
(`core/adapters/index_npy.py:251-277`, где `extra` сливается на `:270`; второго места нет);
проверка `_partition_reuse_reason` (`core/pipeline/corpus_index.py:609-662`) — в `expected` рядом с шестью
текущими (`:2840-2846`: `content_digest`, `chunker_version`, `embed_model_id`,
`extractor_version`, `lexical_engine_version`, `vector_store`).

`_derive_partition_build_id` вызывается из `core/pipeline/corpus_index.py:86-151`, а в тестах — с явными kwargs
(`tests/pipeline/test_corpus_phase_boundary.py:153-163`,
`tests/pipeline/test_document_levels.py:574-585`): новый **обязательный** аргумент даёт там
`TypeError`, поэтому параметр вводится со значением по умолчанию либо тесты правятся в той
же полосе. Пинованные значения — `PINNED_PARTITION_BUILD_ID`
(`tests/pipeline/test_corpus_stage_timings.py:64`, сверка `:399`) — меняются неизбежно.

Почему **не** в `embed_model_id`: он описывает **веса**, а не место счёта, и классифицируется
как `content_change` (`core/pipeline/digests.py:79-88`). Положив устройство туда, мы
прочитали бы смену прибора как смену содержимого — то, от чего `pdf_device` ушёл через
`extractor_stack`.

### 5.2. `vector_digest` — и почему одного поля в `expected` мало (**✅ as-built**)

`vector_digest` считается как хеш `chunk_ids` + `content_hashes` + байтов матрицы
(`core/adapters/index_npy.py:120-131`) и пишется в манифест (`:275`). Но на диск
`chunk_ids.jsonl` пишет **только** `{"chunk_id": …}` (`core/adapters/index_format.py:124-126`)
— `content_hash` не сохраняется. Значит пересчитать `vector_digest` из партиции **нельзя**,
и проверка «просто добавить его в `expected`» не работает: сверять было бы нечего.

Отсюда два шага, оба в `partition-2`:

1. запись в `chunk_ids.jsonl` несёт `content_hash` (аддитивно; читатель без поля работает
   как раньше — старые партиции остаются читаемыми, но не переиспользуемыми по новому
   гейту, что верно: их `vector_digest` недоказуем);
2. проверка переиспользования пересчитывает `vector_digest` с диска и сверяет с манифестом.

Цена проверки **измерена** (29.10.2026, эта машина, `hashlib.sha256` по fp16-матрице):
**9.4 мс** на 15.0 MiB (7667 × 1024) и **37.7 мс** на 63.8 MiB (32 647 × 1024). То есть на
самой большой партиции пакета это доли процента времени прогона — потолок не нужен, число
публикуется.

### 5.3. `embed_cache` получает третью компоненту ключа (**✅ as-built**)

Ключ тогда — `(content_hash текста, embed_model_id)` (`core/pipeline/embed_cache.py`,
докстринг модуля). Векторы CPU и CUDA численно разные, а ключ их не различает — значит
после введения оси `cuda`-прогон молча взял бы `cpu`-векторы из кэша (и наоборот). Ключ
стал `(content_hash, embed_model_id, embed_device)`.

**06.11.2026 — четвёртая компонента.** Ось `embed.precision` добавила `embed_precision`
(`fp16`- и `fp32`-векторы — тоже разные числа), поэтому сегодня ключ
`(content_hash, embed_model_id, embed_device, embed_precision)`, а формат конверта
поднят до `2`: запись без точности — промах, а не догадка (A3).

**Стоимость названа честно и измерена** (`docs/reports/measurements/2026-10-29-embed-cache-device-migration-cost.md`):
существующие записи кэша созданы без объявленного устройства, поэтому приписать им `cuda`
было бы догадкой; первый прогон после границы **пере-эмбеддит корпус**. Числа: 73 383
чанка durable-субстрата против **22 204** записей кэша (52.8 MB) ⇒ полный пере-эмбеддинг
**24.0 ч** при измеренном темпе 0.85 чанк/с (CPU `sentence-transformers` fp32,
`2026-10-25-embedding-throughput.md`), из них дополнительная цена **именно оси** — **7.3 ч**
разово: остальные 51 179 чанков пересчитывались бы и без неё (`pb` меняется от
`partition-2` и нового `chunk_id`). GPU-путь дешевле, но измеренного числа для него нет —
подставлять замер llama.cpp значило бы назвать чужой прибор своим, поэтому в документах
стоит верхняя граница.


---

## 6. `chunk_id` (run-level)

`_chunk_id` = `f"{product_id}:{doc_hash}:{chunk_no}"` (`core/pipeline/corpus_index.py:181-199`, где
`doc_hash` — `extracted.content_hash`, `:3287`) → **`f"{product_id}:{doc_version_id}:{chunk_no}"`**.
Почему в этой границе: `chunk_id` — часть `LexicalChunk`/`RetrievedChunk` и адрес гидртации
из store, поэтому смена формата и так требует пересборки партиций; делать её отдельным
`content_change` после `partition-2` значило бы пересобирать дважды.

`doc_version_id` = `"dv_" + short_hash(product, content_hash, extractor_version, level)`
(`core/pipeline/incremental.py:279-287`) — **без двоеточий**, поэтому грамматика
`source_id` (`product:doc:chunk_no[:build_id]`, `core/util/ids.py:46-58`;
`parse_source_id` требует ровно 3 или 4 сегмента, `:47-60`) не меняется. Это не
косметика: `doc_hash` не различает уровень извлечения и версию экстрактора, поэтому
`layout`-документ и `text`-документ с одинаковым `content_hash` давали **один и тот же**
`chunk_id` при разном тексте.

### 6.1. `chunker_version` в `chunk_id` **не входит** — и это не упущение

Соблазн добавить его туда есть (`09` §14.6 п. 11 формулирует дефект именно так), но цена
не оправдана, а выигрыш мнимый:

- `chunker_version` — поле `EXECUTION_CONTRACT_FIELDS`, то есть **один чанкер на прогон**;
  внутри прогонной БД `chunk_id` уникален и без него (PK `corpus_chunks.chunk_id`);
- смена чанкера уже меняет `pb` (он в `expected` у `_partition_reuse_reason`,
  `core/pipeline/corpus_index.py:785-795`), то есть даёт новую партицию;
- evidence уже несёт физическую партицию (`cell_evidence.partition_build_id`), поэтому
  `(chunk_id, partition_build_id)` различает то, что «голый» `chunk_id` не различает.

Добавление четвёртого сегмента сломало бы грамматику: четвёртый сегмент `source_id` — это
`build_id` (`core/util/ids.py:46-58`), а `parse_source_id` требует, чтобы третий сегмент был целым
(`_CHUNK_RE`). Двух разных смыслов у одной позиции быть не может.

### 6.2. Durable-пространство не меняется

`chunk_row_id(doc_version_id, chunker_version, chunk_no)` (`core/pipeline/incremental.py:289-304`) уже
содержит и `dv_id`, и `chunker_version`. Два идентификатора остаются разными намеренно
(`09` §14.6 п. 11).

### 6.3. Дефект, вскрытый при сверке: фолбэк `resolve()` мёртв (**✅ исправлен**)

> **As-built. Порядок форм в `resolve` — четыре шага, и порядок не косметический:**
>
> 1. прямой lookup по `corpus_chunks.chunk_id` (как было);
> 2. **точный адрес чанка** `f"{product_id}:{doc_version_id}:{chunk_no}"` (+`build_id`) — это
>    ровно форма `_chunk_id`, то есть адрес строки `corpus_chunks`. Шаг обязателен: у
>    документа бывает несколько версий (`dv` включает уровень извлечения, `doc_id` — нет),
>    и `(doc_id, chunk_no)` у них совпадает, поэтому матч по `doc_id` вернул бы чанк
>    **другой** версии. Поймано тестом на второй версии;
> 3. `f"{product_id}:{ref.doc_id}"` — форма, в которой `doc_id` реально лежит в БД
>    (для legacy-строк с `content_hash` во втором сегменте);
> 4. «голый» `ref.doc_id` — строки, записанные до правила `doc_id = product:hash`;
>    их читаемость не должна зависеть от порядка.
>
> Плюс отображение версии в `doc_id` (`corpus_doc_version`, §10 п. 8) — для строк, чей
> `chunk_id` записан не канонической формой. В БД прошлой версии таблицы нет: шаг не
> исполняется, фолбэк остаётся прежним, а не падает.

`StorePort.resolve` (`core/store/sqlite_store.py:2718-2810`) сначала делает прямой lookup по
`corpus_chunks.chunk_id`, а при промахе разбирает `source_id` через `parse_source_id` и ищет
`WHERE product_id = ? AND doc_id = ? AND chunk_no = ?`. **Этот второй путь не может
сработать**: `parse_source_id` возвращает `doc_id` = второй сегмент = `doc_hash`
(проверено на живой БД: `chunk_id = digital_q_database:c78f…c7d:0`, а `corpus_chunks.doc_id`
= `digital_q_database:c78f…c7d`). То есть фолбэк ищет по голому хешу там, где лежит
`product_id:hash`.

Следствия для границы: (1) грамматика `source_id` сегодня **не** несёт нагрузки — работает
только прямой lookup, поэтому смена `chunk_id` не ломает резолв; (2) фолбэк надо либо
починить (искать по `doc_id = f"{product_id}:{ref.doc_id}"`), либо удалить как мнимую
страховку. Решение границы: **починить** — иначе после смены формата он останется мёртвым
уже молча, а тест на него отсутствует (`09` §14.6 п. 11).

---

## 7. Тесты (обязательные для полосы) — **✅ написаны**

> **As-built.** Пункты 1–10 закрыты; три файла созданы границей
> (`tests/pipeline/test_corpus_runs_boundary.py`, `tests/pipeline/test_evaluate_reuse_confirmed.py`,
> `tests/cli/test_report_provenance_payload.py`), остальные дополнены. Отдельно добавлены
> тесты, которых спецификация не требовала, а дефекты — требовали: две версии одного
> документа (обе подтверждают `❌`, обе резолвятся), `resolve` для **второй** версии,
> read-only БД прошлой версии без таблицы отображения.

Файлы названы по факту существующим (первая редакция ссылалась на пять несуществующих —
`tests/store/test_sqlite.py`, `tests/cli/test_wiring_embedder.py`,
`tests/pipeline/test_corpus_partition_reuse.py`, `tests/pipeline/test_corpus_coverage.py`,
`tests/cli/test_run_report_scope.py`; такого деления в репозитории нет).

1. `tests/store/test_schema_gate.py` + `tests/store/test_sqlite_store.py` — **сегодня**
   проверяют обратное исходному решению: свежая БД объявляет все прежние аддитивные колонки
   и индексы, файл чужой версии отвергается и остаётся неизменным. `persist_manifest` пишет
   новые колонки (иначе они `NULL` навсегда), `read_manifest` терпит необязательные колонки
   без `IndexError`.
2. `tests/pipeline/test_run_identity.py` — `embed_device` в контракте: `cpu`→`cuda` даёт
   другой `execution_contract_digest`; тот же — стабилен; `len(EXECUTION_CONTRACT_FIELDS) == 32`.
3. `tests/config/test_deploy.py` — `deploy.embed.device` объявлен; неизвестный ключ в
   `embed:` отвергается (`extra="forbid"`, там уже есть параметризация `:653-666`);
   `run.yaml:embed.device` резолвится app < run, site-слой отсутствует.
4. `tests/cli/test_execution_axes.py` — `cuda` исключает `Lite` с видимой заметкой; `cuda`
   без `sentence_transformers` — `EXIT_USAGE`; `cpu` сохраняет прежний приоритет;
   `LocalEmbedder` получает `device` (подменённый конструктор).
5. `tests/pipeline/test_corpus_phase_boundary.py` (+ новый файл на переиспользование
   партиции) — `pb` различает `cpu`/`cuda`; партиция с другим `embed_device` в манифесте не
   переиспользуется с видимой причиной; подмена `vectors.fp16.npy` при неизменном манифесте
   отвергается (это и есть проверка `vector_digest`); старая партиция без `content_hash` в
   `chunk_ids.jsonl` — не переиспользуется, но читается.
6. `tests/pipeline/test_embed_cache.py` — ключ различает устройство; старая запись без
   устройства — промах, а не хит.
7. `tests/store/test_corpus_docs_partial.py` — `http_status`/`reuse_kind` доезжают до
   `corpus_docs` и читаются назад; `❌` на `reuse_kind=reused` (retained без ответа) не
   публикуется, на `revalidated` — публикуется при прочих равных.
8. `tests/pipeline/test_report.py` — `run_scope`/`product_plan` в манифесте и в `--json`
   (в payload команды `report` блока манифеста сегодня **нет**, `core/cli/commands/report.py:141-159`
   — поле придётся добавить); `run_product` заполняется по продуктам, `cells_carried` не
   существует.
9. `tests/pipeline/test_report.py` (диагностика) — diff против прогона без `embed_device`
   даёт `execution_change: embed_device`, а не «поле недоступно».
10. `tests/store/test_resolver.py` — фолбэк `resolve()` по `source_id` после §6.3 находит
    чанк (сегодня не находит); `chunk_id` нового формата резолвится прямым lookup.

### 7.1. Тесты, которые граница обязана поправить (сейчас зафиксируют старое)

- `tests/pipeline/test_corpus_stage_timings.py:64` `PINNED_PARTITION_BUILD_ID`, `:65`
  `PINNED_EXECUTION_CONTRACT_DIGEST` (сверки `:399`, `:404`) — оба значения меняются.
- `tests/pipeline/test_corpus_phase_boundary.py:153-163`,
  `tests/pipeline/test_document_levels.py:574-585` — вызывают
  `_derive_partition_build_id(**явные kwargs)`; новый обязательный аргумент → `TypeError`.
- `tests/pipeline/test_corpus.py:934-948` — перечень вариантов чувствительности `pb`
  (подмножество параметров проходит, но перечень вариантов надо расширить).
- `tests/selfcheck.py:793-801` (список обязательных таблиц — `run_product` в него),
  `:801-827` (ассерты по колонкам `runs`/`corpus_docs`), `:1524-1527` (round-trip
  `source_id`).
- `docs/status-dictionary.yaml`: `reuse_kind` (`:651`), `run_scope` (`:670`), `pdf_device`
  (`:437`) есть, `embed_device` **нет**, перечня статусов продукта прогона **нет**.
  Синхронизация обязательна: `core/domain/dictionary.py:80` `assert_dictionary_integrity`,
  `required`-список (`:144` `reuse_kind`), `checks`-таблица (`:162`); словарь грузится
  selfcheck'ом.

---

## 8. Порядок применения (как применялось)

> **As-built: шаг 0 выполнен иначе, и это надо прочитать до остального.** Живой прогон
> `ru-pg-dbms-3d3dc7c3c969` был **остановлен** (SIGTERM, сеанс закрыт как `interrupted`,
> `cancelled_by_operator`), а не доведён до `report`: `kill -INT` до фонового процесса не
> доходит (POSIX ставит `SigIgn`), поэтому Ctrl-C-семантика была недоступна. `report` по
> этому прогону **невозможен** — фаза `corpus` не зафиксирована. Цена эмбеддинга взята не
> из `stage_timings` этого прогона, а из опубликованного замера темпа
> (`2026-10-29-embed-cache-device-migration-cost.md`). Деревья измерены по факту до
> остановки: потолки подняты по замеру, а не по догадке (A3).

0. **`report` текущего прогона — не дождались** (см. выше). Вместо него: остановка,
   публикация замера цены оси устройства и замеров деревьев
   (`2026-10-29-crawl-url-duplication.md`, `2026-10-29-psq-docs-tree-scope.md`).
2. Один коммит, по файлам:
   - **схема**: `core/store/sqlite.py` (DDL `run_product` + `_ensure_columns`);
   - **писатель `run_product`**: `core/store/sqlite_store.py` (`save_run_product` /
     `iter_run_products` по образцу `save_corpus_coverage`), вызов на границе продукта в
     `core/pipeline/corpus.py`;
   - **идентичность**: `core/pipeline/digests.py` (+1 поле), `core/pipeline/run_identity.py`,
     `core/cli/execution_axes.py`, `core/pipeline/crawl_frontier.py`
     (`resolve_embed_device`), `core/config/deploy.py` (`EmbedConfig`), `core/domain/enums.py`
     (`EmbedDevice`), `core/domain/models.py` (`EmbedRunConfig`, `RunManifest.embed_device`,
     `RunManifest.run_scope`/`product_plan`, `CorpusDoc.http_status`/`reuse_kind`);
   - **эмбеддер**: `core/cli/wiring.py`, `core/adapters/embed_local.py`;
   - **партиция и чанк**: `core/pipeline/corpus.py` (`_PARTITION_SCHEMA`, материал `pb`,
     `_partition_reuse_reason`, `_chunk_id`, проводка `embed_device`), `core/adapters/index_format.py`,
     `core/adapters/index_npy.py` (`embed_device` в манифест партиции, пересчёт `vector_digest`),
     `core/pipeline/embed_cache.py`;
   - **чтение и отчёт**: `core/store/sqlite_store.py` (**`persist_manifest` — явный список
     колонок**; `iter_corpus_docs`; `resolve` — §6.3), `core/pipeline/report.py`,
     `core/cli/commands/report.py` (payload `--json`);
   - **словарь**: `core/domain/dictionary.py` + `docs/status-dictionary.yaml`
     (`embed_device`, статусы продукта прогона);
   - **дефекты краула 1 и 3 — одна правка, не две.** Зарядка бюджета на resume
     (`core/pipeline/corpus.py:3286`) и дренаж уже добытого расцеплены: URL
     заряжается в `consumed` и **не ставится в очередь**, а очередь строится только
     через `add_revalidate_seed`, чей work-set гейтится режимом продукта
     (`_revalidation_items`, `:6612-6624`). Правило: **заряженный URL обязан быть и
     дренируемым** — каждый элемент, засчитанный в `consumed`, ставится depth-0 листом
     (`add_revalidate_seed`, вне бюджета discovery) **независимо от режима**; гейт
     `mode == INCREMENTAL` для этого набора неверен, его настоящее предусловие — «есть
     durable-элементы для дренажа». Плюс приписывание владельцу — тем же предикатом, что
     `_group_revalidation_items` (`:6653-6688`: побеждает самый специфичный
     `root_path_prefix`), а не «кто первый» (дефект 1); продукт-wide множество
     `resume_counted` (`:7066`) остаётся — меняется правило приписывания.
     **Почему нельзя сделать только приписывание:** измерено — у `digital_q_database`
     host-корень (prefix `/`, потолок 10) случайно забрал зарядку 2402 элементов, поэтому
     docs-корень получил `consumed = 0` и 2390 URL переингестились из retained-байтов без
     сети. Приписать их docs-корню (потолок 800) без второго полушага значит усечь его и
     потерять всё дерево — ровно судьба `pangolin`. Порядок обязателен: сначала
     «заряженное дренируется», потом «заряжается владельцу»;
   - **дефект краула 2** (дедуп очереди): `core/pipeline/crawl_frontier.py` (`_seen`,
     `:1050`) — ключ дедупа должен схлопывать формы одного URL, различающиеся только
     завершающим слэшем, **не меняя** сохраняемый URL и не отменяя правило `09` §7.2 для
     **seed'ов** (там `p` и `p/` — разные утверждения автора, потому что seed задаёт
     path-prefix корня). Схлопывание **видимо**: счётчик в провенансе корня рядом с
     `skipped_blacklisted`/`skipped_documents`; тихое отбрасывание URL запрещено (тот же
     класс дефекта, что «тихий обход path-prefix», `09` §6.6);
   - тесты §7 + тесты обоих дефектов краула.

   **Чего в этом списке нет и почему:** `core/pipeline/composition.py` — первая редакция
   называла его, но в нём нет ни `embed_model_id`, ни `vector_digest`, ни `embed_device`:
   композиция оперирует только `(product_id, partition_build_id, content_digest)`
   (`:93-107`, `:163`, `:275`). `core/store/state_sqlite.py`/`state_store.py` не трогаются:
   у `state.db` свой механизм (`_ADDITIVE_COLUMNS`, `:307`), и `state_adopt` читает
   `SELECT *` (`core/pipeline/state_adopt.py:626`, `:888`) — новые колонки в `state.db` просто
   не попадут, и это приемлемо: `state.db` — durable-субстрат, а не прогонный провенанс.

3. **Писатель для `run_scope`/`product_plan` надо создать, а не «подключить».**
   Решение владельца 29.10.2026: `run_scope` **персистится** (`runs.run_scope` +
   `RunManifest.run_scope`), политика `--only-product` **не меняется** — scoped-прогон
   по-прежнему обрабатывает только выбранный продукт и не двигает
   `last_completed_run_id`. Закрывается дефект **видимости**: сегодня читатель отчёта не
   может отличить полное измерение от среза, не заглянув в `state.db`. Сегодня
   `run_scope` не хранится нигде, кроме материала `run_id` (`core/pipeline/plan.py:94`;
   прямо зафиксировано в `core/pipeline/state_adopt.py:33`, `:117`), а `ProductPlanSet`
   существует (`core/domain/models.py:2775`) и не персистится ни разу — единственное
   использование `core/pipeline/onboarding.py:95-112`. То есть `runs.run_scope` и
   `runs.product_plan_json` — новые записи, а не переезд существующей.
4. **Правило `❌` (часть той же полосы, `09` §11.2) — СНЯТО F3 плана 20.** Проверка рядом с
   `_enforce_absence_coverage` (`core/pipeline/evaluate.py`): у `explicit`/
   `authoritative` — каждый документ, на который опирается `❌`, обязан иметь
   `reuse_kind ∈ {live, revalidated}`; у корпусных классов — ни один документ корпуса
   продукта не `reused`/`stale`. Нарушение ⇒ ячейка `❓` (`not_stated`, I12) с причиной
   `reuse_unconfirmed` в `evidence_scope.reason` и `cells.validation_notes`,
   `absence_evidence=[]`. Зеркало в отчёте (как у `absence_coverage_unsatisfied`).
   `freshness`/`offline` **не** нужны: правило читает `corpus_docs.reuse_kind` этого прогона.
   **As-built 0.3.0:** вместо этого — `_enforce_dated_absence` (I2d): носитель
   `explicit`/`authoritative` — `corpus_artifact.observed_at` каждого цитируемого документа,
   `bounded` — `corpus_coverage.roots` (`complete=1` + дата); токен `absence_undated`,
   дата `absence_as_of` пишется в `cells.absence_as_of` (SCHEMA 3.20.0).
5. **Версия и golden.** Класс границы — `methodology_version` (§1): bump
   `METHODOLOGY_VERSION`, `methodology_change` в lineage, **полный `golden-full`** и
   **пере-аннотация golden** по `09` §13 — одним прогоном на всю границу, не дважды.
   Новый `run_id` неизбежен (ось `embed_device` в контракте); партиции пересобираются,
   эмбеддинг — по кэшу нового пространства.
6. Прогон-ревизия после этого — обычный `--update`; `hound diff` против прогонов до
   границы честно скажет `methodology_change`, а не «поле недоступно».
7. Документы: `09` §4.3/§6.5/§11.2/§12.2/§13.6/§14.1 — с ⬜ на ✅; `14` оговорка 5 и
   п. 7 §6; **`14:217`/`:236`/`:250` становятся ложными** — там сказано, что `deploy.yaml`
   с секцией `embed:` отвергается `ValidationError`; после границы секция существует, и
   отвергается только неизвестный ключ внутри неё; этот документ — из «спецификации» в
   «as-built» либо в архив.

**Чем шаги 1–7 отличались от записанного (as-built):**

- **п. 1** выполнен целиком, но с одним отступлением: `corpus_docs.doc_version_id` вводился
  как **колонка** и не выдержал проверки — колонка держит одну версию на строку, а
  `doc_id` дедуплицируется по `content_hash` без уровня, поэтому версии двух уровней
  извлечения одного текста терялись. Заменено таблицей `corpus_doc_version` (§10 п. 8).
- **п. 2** — дефект 2 (дедуп форм URL) сделан через `url_form_key` + `duplicate_form_count`
  в провенансе корня; формы схлопываются, сохраняемый URL не меняется, правило `09` §7.2
  для **seed'ов** не тронуто.
- **п. 2, дефекты 1+3** — порядок соблюдён («сначала дренируемость, потом владелец»), и это
  не формальность: обратный порядок усекал бы docs-корень и терял всё дерево.
- **п. 3** — `run_scope` персистится, `ProductPlanSet` персистится; политика
  `--only-product` не менялась.
- **п. 4** — правило реализовано, но документ должен был сказать больше: проверка на уровне
  evidence ищет документ по `source_id`, а `source_id` — это `chunk_id`, второй сегмент
  которого после §6 стал `dv_...`. Без отображения версии в строку корпуса правило
  снимало бы **подтверждённые** `❌` как «наблюдение не записано». Отображение —
  `corpus_doc_version` (§10 п. 8).
- **п. 5** — `METHODOLOGY_VERSION` = `0.2.0`. Под ним ещё **не измерено ни одного прогона**
  (прерванный шёл под `0.1.0`), поэтому волна 2 (перенос ячеек) входит в ту же `0.2.0`:
  номер описывает условия измерения, а измерений под ним нет.
- **п. 7** — `14` оговорка 5 и п. 7 §6 обновлены; `09` §4.3/§6.5/§11.2/§13.6/§14.1
  переведены в ✅.

**Откат:** граница не имеет обратного хода по идентичности (новый `run_id` — необратимо),
но и не разрушает ничего: старые прогоны читаются, старые партиции остаются на диске
иммутабельно, `state.db` не мигрирует.

---

## 9. Чего в этой границе нет

- **carry-forward** и писателя `cell_measurement` (`09` §10) — волна 2, отдельная работа.
- **`deploy.embed` целиком** (`backend`, `base_url`, `model_version`) — `14` §6 п. 5:
  требует решения о появлении инстанса. Здесь вводится только `device`.
- **`--only-failed` межпрогонный** и `offline` — `09` §2.4. Файловый lock на пакет
  (`09` §4.4) сделан отдельным коммитом `d80a6e4` и в границу не входит.
- **`SCHEMA_VERSION` bump** — сознательно не делается (§3).
- **`corpus_docs.doc_id` → `doc_version_id`** (`09` §14.6 п. 12) — **снято** решением
  владельца 29.10.2026. `doc_id` дедуплицируется по `content_hash`, и это настоящее
  требование (один текст — одна строка корпуса); `dv` его не заменяет, он различает то,
  чего `content_hash` не различает. Обе идентичности сосуществуют: строка корпуса —
  по `content_hash`, версия — в отображении `corpus_doc_version` (§3.4).

---

## 10. Что вскрыла сверка спецификации с кодом (29.10.2026)

Первая редакция этого документа была написана по памяти о коде. Сверка с якорями
(`путь:строка`) дала семь расхождений и три вещи, которых в спецификации не было вовсе.
Записаны здесь, чтобы вторая редакция не повторяла ошибок первой.

**Расхождения, исправленные выше:** `_CONTRACT_FIELD_GETTERS` —
`core/pipeline/report.py:2505`, а не `:1970`; `reuse_kind` в `url_state` —
`core/store/state_sqlite.py:106`, а не `:93` (там комментарий); `_PARTITION_SCHEMA` —
`core/pipeline/corpus_index.py:48`; `composition.py` исключён из состава (§8);
«`embed_device` в `extra` **и** в корне» — одно место, а не два (§5.1); «геттер в
`_CONTRACT_FIELD_GETTERS`» — поле на манифесте достаточно (§4.2).

**Чего не было вовсе:**

1. **`EmbedConfig`/`EmbedRunConfig` не существовали** — имена были только в этом документе
   (§4.1). ✅ Созданы границей.
2. **Порт эмбеддера без `device`** — и это правильно; устройство идёт явным параметром
   (§4.1). Иначе граница повторила бы дефект: спрашивать у адаптера, на чём он считает.
3. **Грамматика `source_id`** — `chunk_id` с четырьмя сегментами несовместим с
   `parse_source_id` (§6.1); и фолбэк `resolve()` сегодня мёртв (§6.3).
4. **`persist_manifest` — явный список колонок** (§4.2): колонка, не вписанная туда, не
   пишется никогда, хотя `ALTER TABLE` её создал.
5. **`run_scope`/`product_plan` — не переезд, а новый писатель** (§8 п. 3).
6. **Пять из девяти тестовых файлов не существовали** (§7); плюс список тестов, которые
   сейчас пинуют старое поведение и обязаны быть поправлены (§7.1).
7. **`14:217`/`:236`/`:250` станут ложными** после границы (§8 п. 5).

**Что вскрылось при применении (вторая сверка — уже по факту, 29.10.2026):**

8. **Отображение `dv_...` → строка корпуса не помещается в колонку `corpus_docs`.**
   Спецификация §6 сказала «`chunk_id` несёт `doc_version_id`» и на этом остановилась: как
   `source_id` вернётся в документ — не описано. Первая реализация добавила
   `corpus_docs.doc_version_id`; воспроизведение показало, что она теряет версии:

   ```
   две версии одного текста (text и layout, одинаковый content_hash):
     upsert(doc_id="p1:hashA", doc_version_id="dv_text")
     upsert(doc_id="p1:hashA", doc_version_id="dv_layout")
   → строк: 1, версия в строке: dv_layout
   ```

   Причина не в реализации, а в схеме: `doc_id = product:content_hash` **не различает
   уровень извлечения**, а `dv` различает — и `document_level` объявлен **per-site**
   (`core/pipeline/crawl_frontier.py:636-641`), то есть два уровня в одном прогоне законны.
   Вторая версия терялась бы молча, и evidence, цитирующий её, получал бы «наблюдение не
   записано» — ложное утверждение о прогоне. Носитель — производная таблица
   `corpus_doc_version(doc_version_id PK, product_id, doc_id)`: одна версия → один документ,
   обратное неверно, поэтому это не колонка. `corpus_docs.doc_id` остаётся
   `product:content_hash` (на нём держится дедуп), §14.6 п. 12 «перевести `doc_id` на
   `doc_version_id`» — **снято** решением владельца 29.10.2026.
9. **Правило `❌` без этого отображения не работает — и тест этого не видел.** Тест
   строил `chunk_id` старым форматом, то есть проверял формат, которого больше нет.
   Мораль общая: тест, который сам конструирует идентификатор, обязан строить его той же
   функцией, что и код (`_chunk_id`), иначе он пинует прошлое.
10. **`RunManifest` объявлял три lineage-поля, которых никто не заполнял.** `cycle_id`,
    `previous_run_id`, `change_class` (`core/domain/models.py`) есть в модели и **не
    передавались** в `build_manifest` — в `run_manifest.yaml` и `hound report --json` они
    были `None` всегда. Класс дефекта — «объявлено, но не написано»: читатель артефакта не
    видел, против какого прогона считался этот. Заодно правило «предыдущий прогон»
    существовало **в трёх копиях** (`run.py`, `state_adopt.py`, `state_sync`) — три способа
    разойтись; сведено в один резолвер `previous_run_of`. Источник манифеста — lineage-строка
    **самого** прогона, а не пересчёт: `change_class` вычисляется один раз при записи lineage.
11. **`previous_run_id` не доходил до фазы оценки.** `RunPlan` его не нёс,
    `evaluate_phase` не принимал. Для волны 2 (перенос ячеек) он нужен; сейчас потребитель —
    наблюдаемость (`log_entries` фазы называют, против какого прогона оценивали). В материал
    `run_id` он **не входит**: предшественник — не условие измерения, иначе каждый новый
    прогон менял бы идентичность от собственной истории.
12. **`CorpusReuseObservation` строится один проход на продукт, а не на ячейку.** 181
    параметр × продукты читали бы одну и ту же таблицу сотни раз. Сбой чтения виден в
    `errors` фазы, а не выдан за «наблюдений нет»: иначе правило снималось бы молча.
13. **Счётчик дедупа форм URL считался, но никуда не выводился.** Спецификация (§8 п. 2)
    требовала «схлопывание **видимо**»; реализация завела `Frontier.duplicate_form_count`
    и на этом остановилась — в провенансе корня его не было. То есть дедуп экономил бюджет
    **молча**, ровно тот класс, который запрещён («тихое отбрасывание URL»). Дописано в
    запись корня (`truncation[].duplicate_forms`, ключ появляется только при наличии
    дублей), тест — `tests/pipeline/test_corpus_artifacts.py::test_duplicate_url_forms_are_visible_in_provenance`.
    Мораль: счётчик в объекте — не наблюдаемость; наблюдаемость — это поле в артефакте.
14. **`runs.cohort_ref` хранила имя файла когорты** (`cfg.cohort`), а не авторскую ревизию
    (`cfg.cohort_ref`), — при том что именно ревизия входит в `cohort_digest`. Манифест
    показывал ревизию, которой в `run_id` не было, а настоящая не сохранялась нигде.
    Писатель исправлен в двух местах (`create_run` и `build_manifest` — второй предпочитал
    `run.cohort`), `None` не подменяется именем файла: «ревизия не объявлена» и «ревизия
    равна имени файла» — разные факты. Тест различает два поля на одном входе.

---

*Документ — отчёт о применённой границе, а не задание. Все ссылки `путь:строка` проверены по
коду 29.10.2026; если они разойдутся — прав код, а не текст.*
