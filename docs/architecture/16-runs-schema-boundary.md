# 16. Версионная граница `runs`: схема, идентичность, состав

> **Статус: применено 29.10.2026 (as-built); сверено с кодом 08.11.2026, HEAD `f14f866`.**
> Всё описанное ниже реализовано в `core/`; §8 описывает, как это применялось, а не как это
> предстоит делать, и §10 — что сверка спецификации с кодом вскрыла по ходу.
>
> **Действующие версии на 08.11.2026** — прочитаны в коде, а не в этом тексте:
>
> | величина | значение | носитель |
> |---|---|---|
> | `METHODOLOGY_VERSION` | **`0.3.0`** | `core/domain/models.py:162` |
> | `SCHEMA_VERSION` (`runs/{run_id}/cohound.db`) | **`3.20.0`** | `core/store/sqlite.py:86` |
> | `STATE_SCHEMA_VERSION` (`state/{package_id}/state.db`) | **`"12"`** | `core/store/state_sqlite.py:84` |
> | `SEAM_SCHEMA_VERSION` (контракт `/v1/*`) | **`"1"`** | `core/edge/seam_schema.py:68` |
> | `EDGE_JOBS_SCHEMA_VERSION` (`edge_jobs.db`) | **`"1"`** | `core/edge/scheduler.py:197` |
> | `EXECUTION_CONTRACT_FIELDS` | **32** поля | `core/pipeline/digests.py:96` |
>
> **Решение владельца 29.10.2026:** всё, что меняет материал `run_id` или состав прогонной
> БД, делается **одной** границей. Две отдельные границы (`embed_device` отдельно, схема
> отдельно) обнулили бы прогресс дважды и дважды порвали бы межцикловый diff без пользы.
>
> **Что из решений 29.10.2026 снято позже — и когда:**
>
> - **➖ «`SCHEMA_VERSION` остаётся `3.13.0`, всё аддитивно» — снято 01.11.2026**
>   (волна S плана 17, решение владельца). Совместимость схемы не сохраняется: аддитивные
>   `ALTER TABLE ADD COLUMN`-шимы снесены во всех трёх БД, все колонки и индексы объявлены
>   в `CREATE TABLE`, файл прежней версии отвергается гейтом (`SchemaVersionMismatch` →
>   `EXIT_USAGE`/`schema_version`) **до** DDL. Тогда же `SCHEMA_VERSION = "3.14.0"`,
>   `STATE_SCHEMA_VERSION = "6"`; сегодня — таблица выше.
> - **➖ «`METHODOLOGY_VERSION` — `0.2.0`» — снято 05.11.2026** (F3 планов 19/20):
>   действует `0.3.0` — датированное отсутствие (I2d) вместо правила «`❌` требует
>   `live|revalidated` в этом цикле».
> - **✅ «`SEAM_SCHEMA_VERSION` остался `"1"`»** — не снято: `"1"` действует
>   (`core/edge/seam_schema.py:68`).
> - **✅ «`EDGE_JOBS_SCHEMA_VERSION` остался `"1"`»** — не снято: `"1"` действует
>   (`core/edge/scheduler.py:197`).
>
> **История версий — канонический перечень здесь**, и только здесь (`03` §4.4 даёт ссылку,
> а не копию).
>
> `SCHEMA_VERSION`: **3.13.0** — граница A без bump (29.10.2026, ➖ снято 01.11.2026);
> **3.14.0** — гейт версии вместо аддитивных шимов, все колонки в `DDL` (01.11.2026, волна S
> плана 17); **3.15.0** — `crawl_session_items.fetch_started_at`/`fetch_ms` и
> `extract_queue.claimed_at`/`heartbeat_at` (F6 плана 17); **3.16.0** —
> `corpus_chunks.char_start`/`char_end` (L1b-2b плана 18); **3.17.0** — `runs.crawl_policy_digest`
> + `runs.crawl_policy_json` (P4 плана 19); **3.18.0** — `runs.corpus_snapshot_json` (F1 плана
> 20); **3.19.0** — таблица `corpus_artifact` (F2 плана 20); **3.20.0** —
> `corpus_coverage.roots_json` + `cells.absence_as_of` (F3 плана 20, 05.11.2026). Источник —
> докстринг `SCHEMA_VERSION` (`core/store/sqlite.py:28-86`), не этот документ.
>
> `STATE_SCHEMA_VERSION`: 2 — `run_lineage.products_json`/`refresh_policy`; 3 — durable-субстрат
> (`url_state`/`document_version`/`chunk_store`/`chunk_meta`); 4 — `profile_attestation`;
> 5 — частичность извлечения; **6** — гейт версии вместо миграции (01.11.2026); 7 —
> `chunk_meta.char_start`/`char_end`; 8 — `crawl_policy`; 9 — `corpus_capture`; 10 —
> `container_state`; 11 — `capture` + наблюдения `url_state`; **12** — отпечаток пространства
> снятия. Источник — `core/store/state_sqlite.py:34-84`.
>
> Всё ниже — история границы и её сверка, а не описание текущего кода там, где это названо.

**Дата:** 2026-10-29. **Сверено с кодом:** 2026-11-08 (HEAD `f14f866`). **Основание:** `09`
§14.1 (состав границы), `14` оговорка 5 (замер устройства эмбеддера).

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
`live|revalidated`»** (`09` §11.2; ➖ **снято 05.11.2026** — F3 плана 20, носитель теперь
датированный, I2d, `09` §11.2 врезка). Оно меняет условие публикации `❌`, то есть
absence/evidence-модель, а значит по `AGENTS.md` §9 (классы версий) это `methodology_change`
с обязательными `golden-full` **и** пере-аннотацией golden. Правило неотделимо от
`corpus_docs.reuse_kind`: без колонки оно неисполнимо, а колонка без правила — только
видимость. Разносить их на две границы значило бы платить за `golden-full` дважды.

**Четвёртая причина (решение владельца 29.10.2026): в ту же полосу входят два дефекта
краула**, найденные живым прогоном (`09` §6.3). Оба меняют корпус, то есть по отдельности
дали бы `execution_change` — а `golden_required_for` возвращает `full` **и** для
`execution_change` (`core/pipeline/run_identity.py:421`; `09` §12.1). Отдельная полоса
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
| ось `embed_device` (`cpu\|cuda`) в контракте | до границы устройство эмбеддера выбирал torch, а `run_id` об этом не знал (`14` оговорка 5) | `execution_change` |
| `embed_device` в материале `pb` и в проверке переиспользования партиции | иначе CPU-партиция молча переиспользуется CUDA-прогоном | `content_change` (следствие) |
| `vector_digest` в проверке переиспользования | сборщик его пишет (`core/adapters/index_npy.py:305`), `_guard_immutable` сверяет (`core/adapters/index_npy.py:324`), но при переиспользовании сборка не строится — гард не исполняется | `content_change` |
| `content_hash` в `chunk_ids.jsonl` | **необходимое условие** предыдущего пункта, см. §5 | формат партиции `partition-2` |
| `embed_device` в ключе `embed_cache` | иначе CPU-прогон получит CUDA-векторы | стоимость, не идентичность |
| `runs.cohort_ref` — писать `cfg.cohort_ref`, а не имя файла когорты (**✅ сделано**) | манифест показывает ревизию, которой в `run_id` не было (`09` §12.2) | провенанс, `run_id` **не** меняется |
| `runs.run_scope`, `runs.product_plan_json` | `--only-product` меняет смысл среза и не виден в артефакте (`09` §13.6) | провенанс, `run_id` **не** меняется |
| таблица `run_product` | статус продукта в прогоне живёт только снимком в `state.db` (`09` §4.3/§6.5) | наблюдаемость |
| `corpus_docs.http_status`/`reuse_kind` | `❌` на retained-байтах без серверного подтверждения до границы был невыразим (`09` §11.2); колонка **исполняла** правило `❌` ⇒ `live\|revalidated`, а не только показывала его (правило снято F3 плана 20 — колонки остались носителем происхождения наблюдения) | **`methodology_change`** (условие публикации `❌` меняется) |
| `chunk_id` с `doc_version_id` вместо `doc_hash` | один и тот же текст на разных уровнях извлечения давал один `chunk_id` (`09` §14.6 п. 11); `chunker_version` в него **не** входит — довод в §6.1 | `content_change` |
| **дефект краула 1**: реконструкция бюджета на resume заряжает `max_documents` первому корню-претенденту, а не владельцу | потолок умножается на число перезапусков: измерено 1610 `fetched` при потолке 800 = `2 × 800` (`09` §6.3) | `execution_change` (корпус) |
| **дефект краула 3**: зарядка бюджета на resume не делает заряженный URL дренируемым (дренаж идёт только через `add_revalidate_seed`, а тот строится лишь для `incremental`) | у `pangolin` корень заряжен в потолок 600/600 ⇒ ноль `fetched` в попытке ⇒ `corpus_chunks` 25 при 509 документах, хотя 4935 чанков лежат в `state.db` (`09` §6.3) | `execution_change` (корпус) |
| **дефект краула 2**: `Frontier._seen` дедуплицирует по точной строке URL | `/page` и `/page/` — два элемента очереди: измерено 1164 из 2402 `fetched` (48.5 %) у `digital_q_database`, 54 из 629 у `pangolin`, 0 у `jatoba` (`docs/reports/measurements/2026-10-29-crawl-url-duplication.md`) | `execution_change` (корпус) |

**Не входит** (остаётся на свои полосы; часть уже исполнена — §9): carry-forward ячеек и
`cell_measurement` с писателем (`09` §10 — волна 2), объявленный backend эмбеддера
(`deploy.embed` целиком, `14` §6 п. 5), межпрогонный `--only-failed` и `offline` (`09` §2.4),
`RemoteEmbedder`. Файловый lock на пакет (`09` §4.4) **сделан отдельно** (`d80a6e4`).

---

## 3. Схема прогонной БД: **без** bump `SCHEMA_VERSION` (решение 29.10.2026 — отменено 01.11.2026)

> ⚠️ Раздел описывает **отменённое** решение. Сегодня: версия поднята, шимов нет, старая БД
> отвергается. Ссылки на `_ensure_columns` ниже указывают на удалённый код — они оставлены
> как след принятого тогда решения, а не как описание текущего состояния.

> **➖ снято 01.11.2026.** Решение ниже отменено владельцем вместе с волной S плана 17:
> `_ensure_columns` и `_migrate_scope_acceptance` **удалены** из `core/store/sqlite.py`
> (след — докстринг `SCHEMA_VERSION`, `core/store/sqlite.py:28-34`), версия стала **гейтом**:
> `_reject_foreign_schema` (`core/store/sqlite.py:751-768`) читает `schema_meta.schema_version`
> **до** DDL и поднимает `SchemaVersionMismatch` (`core/util/schema_gate.py:40`); вызовы —
> писательский `connect` (`core/store/sqlite.py:852`) и read-only `connect_read_only`
> (`core/store/sqlite.py:829`), оба до `executescript`. Содержимое файла при отказе не
> меняется — это проверено тестом (`tests/store/test_schema_gate.py:58`
> `test_previous_schema_is_rejected_and_file_is_unchanged`).
> Тот же класс и тот же формат сообщения — на **все три** БД
> (`core/util/schema_gate.py:40-60`); в `core/store/state_sqlite.py:506` и
> `core/edge/scheduler.py:642` версия тоже читается **до** DDL, а аддитивных
> `ALTER TABLE ... ADD COLUMN` при открытии нет ни в одной из трёх. Путь отвергнутого файла —
> обязательная часть сообщения (`schema_mismatch_message`,
> `core/util/schema_gate.py:62`), код `schema_version` → `EXIT_USAGE`.
> Причина снятия: аддитивная дельта, дописанная тихо, делала строку прошлой схемы
> неотличимой от текущей, то есть объявляла файл новым, не будучи им.

**Решение 29.10.2026: `SCHEMA_VERSION` остаётся `3.13.0`** (константа жила тогда на 15-й
строке того же файла; сегодня — `core/store/sqlite.py:86`, значение `3.20.0`). Все
изменения
ниже — аддитивные: колонки добавлял идемпотентный `_ensure_columns`
(удалён в 3.14.0: шимов `ALTER TABLE` больше нет, `core/store/sqlite.py`), таблицу —
`CREATE TABLE IF NOT EXISTS` в `DDL`. Гейт
`_reject_foreign_schema` (`core/store/sqlite.py:751-768`, вызовы `core/store/sqlite.py:829` и
`core/store/sqlite.py:852`) существует ради
**несовместимого** чтения, а не ради счётчика ревизий: поднимать версию на аддитивном
изменении значило бы запретить чтение уже завершённых прогонов без причины.

Практическое следствие, ради которого это и выбрано: **границу можно применять сразу после
`report`**, не дожидаясь, пока старые `runs/*/cohound.db` станут нечитаемыми. Ни один
существующий прогон не теряется. ➖ **Снято 01.11.2026** вместе с самим решением: с
`SCHEMA_VERSION = 3.14.0` старый файл **отвергается** гейтом, а не читается, и прогон
переносится пересозданием (таблица версий в заголовке). Утверждение верно только как
история — на 29.10.2026 оно было верным.

### 3.1. `runs` — новые колонки

> ⚠️ Ниже — **историческая** форма записи (как это вводилось 29.10.2026 через
> `ALTER TABLE`). С 01.11.2026 шимов нет: `embed_device` (`core/store/sqlite.py:129`),
> `run_scope`/`product_plan_json` (`core/store/sqlite.py:149-150`) объявлены прямо в `DDL`,
> а `ALTER TABLE` при открытии не выполняется ни один.

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
а не накоплением в памяти. `cells_carried`/`cells_measured` **не заводятся**: на момент
границы переноса ячеек не было (`09` §10), а сегодня их считает отчёт из durable-ячеек
(`core/pipeline/state_sync.py:274-326`, `core/pipeline/report.py:3173`).

**Побочный эффект, который надо принять явно:** `cells_total`/`cells_pending`/`baseline_complete`
из `run_product` — величины того же класса, что `product_end` пишет в `results[].notes` прогона
(`_product_notes`, `core/pipeline/corpus_coverage.py:414-494`; сама строка —
`core/pipeline/corpus_coverage.py:485-493`).
**Сверено 08.11.2026: счётчики ячеек в заметку не входят** — она называет корни, seed'ы,
элементы сеанса, документы и время; ячейки живут только в `run_product` и в отчёте. То есть
второго носителя этих трёх величин сегодня нет, и расхождение возможно только между
`run_product` и отчётом. Решение границы остаётся: **`run_product` — носитель, `notes` — для
оператора** (текст), и расхождение между ними — аномалия отчёта, а не допустимое состояние.

### 3.3. `corpus_docs` — происхождение наблюдения

```sql
ALTER TABLE corpus_docs ADD COLUMN http_status INTEGER;  -- NULL = сервер в этом цикле не отвечал
ALTER TABLE corpus_docs ADD COLUMN reuse_kind TEXT;      -- live|revalidated|reused|stale
```

Зачем: `url_state.reuse_kind` уже различает «сервер ответил 304» и «взяли retained-байты»
(`core/store/state_sqlite.py:150`), но в прогонную БД это не доезжает, и 304-документ
выглядит как живой 200 (`09` §11.2). Без этих колонок правило «`❌` нельзя публиковать на
неподтверждённом наблюдении» неисполнимо, сколько бы его ни уточнять текстом.

**Правило записи — монотонное, и это не деталь.** Значение берётся из пути страницы
**этого** прогона: живой fetch — `live`/`revalidated`, чтение retained-байтов
(`resume_retained`, `core/pipeline/corpus_ingest.py:388-490`) — `reused`. Resume перечитывает и те документы, которые
в этом же `run_id` уже были получены от сервера (до перезапуска хоста), поэтому строка
этого прогона, несущая `live`/`revalidated`, **не понижается** до `reused`: иначе resume
стирал бы состоявшееся наблюдение. Наследовать `url_state.reuse_kind` **нельзя** — там
последняя классификация, в том числе из прошлого цикла (замер 29.10: 2157 `live` + 2009
`revalidated` + 0 `reused`, при том что 629 элементов `pangolin` перечитаны из
retained-байтов).
`reuse_kind IS NULL` в `corpus_docs` = «наблюдение не записано» (legacy-строки); писатель
границы всегда пишет значение. ➖ **Правило, ради которого колонка вводилась, снято
05.11.2026** (F3 плана 20): «`❌` не публикуется при `NULL`» больше не действует — `❌`
держит датированный носитель (I2d, §8 п. 4). `reuse_kind` остаётся провенансом наблюдения
(что именно произошло с URL в этом прогоне), а не условием статуса.

**Оба значения уже в руках у единственного писателя** — новой проводки не нужно:
`CorpusDoc(...)` собирается в `core/pipeline/corpus_ingest.py:1693`, рядом живут `reuse`
(`core/pipeline/corpus_ingest.py:1810` — `reused = active_reuse is not None`) и `page.status_code`
(он же уже уходит в `url_state` как `last_http_status`, `core/pipeline/corpus_ingest.py:1833`;
второй путь — `core/pipeline/corpus_ingest.py:1650`). Точка записи одна — `persist_docs`
(`core/pipeline/corpus_ingest.py:1346`, докстринг сам это фиксирует).

**Что спецификация обязана решить, а не оставить на ходу:** поле `CorpusDoc` или kwarg
`upsert_corpus_doc`. Прецедент в коде — `origin`
(`core/store/sqlite_store.py:1426-1440`): это kwarg, а не поле модели, с доводом «доменная
модель не получает смысла, которого у неё нет». Довод
здесь **не переносится**: `origin` пишется и назад не читается, а `http_status`/`reuse_kind`
обязаны читаться — `hound diff` строит дельту документов по `corpus_docs`
(`core/cli/commands/diff.py:106`), а правило `❌`-допустимости (I2c) обязано спрашивать
происхождение наблюдения там же. Решение границы: **поля
`CorpusDoc`** + чтение в `iter_corpus_docs` (`core/store/sqlite_store.py:2667`, явные
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
per-site (`core/pipeline/crawl_frontier.py:748-751`) — два уровня одного текста в одном прогоне
законны. Колонка держит одну версию на строку, то есть теряет вторую молча. Воспроизведение
и цена — §10 п. 8.

**Где читается.** `_doc_id_by_version` (точечно, для `resolve`) и
`iter_corpus_doc_versions(run_id, product_id)` (для правила `❌`). Пишется в
`upsert_corpus_doc` — в той же транзакции, что строка корпуса, идемпотентно по PK. В БД
прошлой версии таблицы нет: оба читателя возвращают `None`/пусто, а не падают.
⚠️ Сверено 08.11.2026: при **обычном** открытии такая БД отвергается гейтом версии (§3),
поэтому терпимость — страховка для снимков/`adopt` и тестов, а не штатный путь чтения.

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
  `core/domain/models.py:514`), дефолт `None` — по той же причине, что у `pdf_device`
  (`core/domain/models.py:614-620`): дефолт run-слоя перекрыл бы app-слой в `_pick`.
- **per-site слоя нет**: устройство — свойство хоста исполнения, а не сайта (тот же довод,
  что в `resolve_pdf_device`, `core/pipeline/crawl_frontier.py:488-509`).
- Резолвер: `resolve_embed_device(app, run)` (`core/pipeline/crawl_frontier.py:511-538`) —
  копия `resolve_pdf_device` без site-слагаемого.
  Дефолт `cpu`, значения только `cpu|cuda`, `auto` **нет** (R5: объявленная ось обязана
  действовать). Enum `EmbedDevice` — в `core/domain/enums.py` рядом с `PdfDevice`
  (`core/domain/enums.py:561`).
- **Порт эмбеддера не меняется.** `core/ports/embedder.py:15-21` знает только `model_id`/`dim`,
  и это правильно: устройство — объявленная ось прогона, а не свойство адаптера, которое
  можно у него спросить (спрашивание и есть дефект `14` оговорки 5). Значение протаскивается
  **явным параметром**: `core/pipeline/phases.py:1367-1368` → `core/pipeline/corpus.py:1688`
  (объявление оси) и `core/pipeline/corpus.py:4663`/`core/pipeline/corpus.py:4738`
  (передача рядом с `embed_model_id`), там же, где уже идёт `embed_model_id`
  (`core/pipeline/corpus.py:2330` — единственный источник этого значения).

### 4.2. Контракт (**✅ as-built**)

`EXECUTION_CONTRACT_FIELDS` — 28 → **29** полей (`core/pipeline/digests.py:96-228`):
добавляется `embed_device` рядом с `extractor_stack` (та же природа: устройство как условие
измерения). «29» — размер **на момент границы**; на 08.11.2026 полей **32**
(`core/pipeline/digests.py:96`), остальные добавлены осями планов 19–23 (`embed_precision`,
`crawl_policy_digest` и др.). Значение кладёт `build_execution_contract` (`core/pipeline/run_identity.py:254`)
из `DeclaredExecutionAxes` — там же, где `extractor_stack`
(`core/cli/execution_axes.py:687-694`).

**Совместимость чтения проверена:** `execution_contract_digest(values)`
(`core/pipeline/digests.py:519-541`) требует точного набора ключей, но вызывается ровно из
одного места — `core/pipeline/run_identity.py:379`, внутри `build_execution_contract`, то есть
только на
живом прогоне. Пересчёта из сохранённого словаря нет; `hound diff` сравнивает сохранённые
строки (`core/pipeline/report.py:2585-2611`), а отсутствующее на манифесте поле уходит в
`missing`, а не в исключение. `execution_change: embed_device` diff называет и тогда, когда
поле есть только с одной стороны.
⚠️ **Сверено 08.11.2026 — оговорка к «старые прогоны читаются»:** совместимость чтения
касается **строки манифеста**, а не файла БД. БД версии ≤ 3.19.0 отвергается гейтом целиком
(§3), поэтому `hound diff` против прогона прошлой схемы невозможен не из-за поля, а из-за
версии файла. Это следствие решения 01.11.2026, а не свойство оси.

`RunManifest` (`core/domain/models.py:1667`) получает поле `embed_device`, и этого
достаточно: `_contract_field_attr` (`core/pipeline/report.py:2554-2557`) сопоставляет имя
поля контракта с атрибутом манифеста напрямую, а `_CONTRACT_FIELD_GETTERS`
(`core/pipeline/report.py:2563-2568`)
заведён только для полей **без** одноимённого атрибута (сейчас там `serp_rounds`/`serp_mode`).
Без поля на манифесте диагностика назовёт ось «недоступной» вместо «изменённой».

Строку манифеста в БД тоже надо дописать: `persist_manifest`
(`core/store/sqlite_store.py:1196`) — **явный список** из 29 колонок, а не `SELECT *`
(на 08.11.2026 список — **40** колонок, `core/store/sqlite_store.py:1228-1248`: счётчик
растёт с каждой осью, механика та же). Колонка, не вписанная туда, останется `NULL` навсегда: штатного `ALTER TABLE` больше нет
(схема версионируется, старая БД отвергается), поэтому расхождение списка с `DDL` — дефект,
а не «домигрируется». Чтение — `read_manifest` (`core/store/sqlite_store.py:2875-3118`), где необязательные поля идут
через `_has_column` (`core/store/sqlite_rows.py:77`).

### 4.3. Выбор эмбеддера перестаёт быть «что установлено» (**✅ as-built**)

**До границы** `_resolve_embedder` (`core/cli/wiring.py:2911-2954`) выбирал по факту
установки: `model2vec` → `LocalEmbedder` → `None`. Сегодня он принимает **объявленные**
`device` и `precision` (`core/cli/wiring.py:2911-2954`) и исполняет таблицу ниже;
`LiteEmbedder` — NumPy/CPU by design
(`core/adapters/embed_model2vec.py:4`), и ось он исполнить не может.

Правило границы — минимальное, backend остаётся недекларированным (это `14` §6 п. 5):

| Объявлено | Выбор |
|---|---|
| `cpu` | как сегодня (`Lite` → `Local`): обе реализации CPU-честны |
| `cuda` | `Lite` **исключается** с видимой заметкой (он не может исполнить ось), берётся `LocalEmbedder`; недоступен → `EXIT_USAGE` с числами |

Тихого фолбэка нет: прогон, объявивший `cuda`, не должен получить CPU-векторы под тем же
`run_id` — это ровно тот класс, ради которого ось вводится. И наоборот: `cuda` без
устройства — отказ, а не CPU (прецедент `require_pdf_device`).

`LocalEmbedder` получает `device=` в конструктор и передаёт в
`SentenceTransformer(...)` (`core/adapters/embed_local.py:144`, `core/adapters/embed_local.py:228`)
— до границы
аргумента не было, и устройство выбирал torch; теперь там же `precision=` (fp16 через
`torch.autocast`, `core/adapters/embed_local.py:240`).

---

## 5. Партиция: формат `partition-3`

`_PARTITION_SCHEMA = "partition-1"` (`core/pipeline/corpus_index.py:54`) → `partition-2`
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

Материал `_derive_partition_build_id` (`core/pipeline/corpus_index.py:86-151`) получает `embed_device`
рядом с `embed_model_id`; манифест партиции — ключ `embed_device` в том же явном словаре
(`core/adapters/index_npy.py:270-300`, где `extra` сливается на
`core/adapters/index_npy.py:300`; второго места нет);
проверка `_partition_reuse_reason` (`core/pipeline/corpus_index.py:866-949`) — в `expected`
(`core/pipeline/corpus_index.py:911-923`; сегодня ключей **восемь**: шесть прежних плюс
`embed_device` и `embed_precision`).

`_derive_partition_build_id` вызывается из `core/pipeline/corpus.py:4657`, а в тестах — с явными kwargs
(`tests/pipeline/test_corpus_phase_boundary.py:153-163`,
`tests/pipeline/test_document_levels.py:574-585`): новый **обязательный** аргумент даёт там
`TypeError`, поэтому параметр вводится со значением по умолчанию либо тесты правятся в той
же полосе. Пинованные значения — `PINNED_PARTITION_BUILD_ID`
(`tests/pipeline/test_corpus_stage_timings.py:83`, сверка
`tests/pipeline/test_corpus_stage_timings.py:421`) — меняются неизбежно.

Почему **не** в `embed_model_id`: он описывает **веса**, а не место счёта, и классифицируется
как `content_change` (`core/pipeline/digests.py:84-95`). Положив устройство туда, мы
прочитали бы смену прибора как смену содержимого — то, от чего `pdf_device` ушёл через
`extractor_stack`.

### 5.2. `vector_digest` — и почему одного поля в `expected` мало (**✅ as-built**)

`vector_digest` считается как хеш `chunk_ids` + `content_hashes` + байтов матрицы
(`core/util/index_partition.py:107` — **одна** формула на сборщика и на пересчёт) и пишется
в манифест (`core/adapters/index_npy.py:305`). До границы на диск `chunk_ids.jsonl` писал
**только** `{"chunk_id": …}`, и `content_hash` не сохранялся — пересчитать `vector_digest`
из партиции было **нельзя**, поэтому проверка «просто добавить его в `expected`» не
работала: сверять было бы нечего. Сегодня строка несёт `content_hash`, когда сборщик его
передаёт (`core/adapters/index_format.py:143-146`).

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
`(content_hash, embed_model_id, embed_device, embed_precision)`
(`core/pipeline/embed_cache.py:15`), а формат конверта
поднят до `2` (`core/pipeline/embed_cache.py:127`): запись без точности — промах, а не
догадка (A3).

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
`doc_hash` — `extracted.content_hash`, вызов — `core/pipeline/corpus_ingest.py:1437`) →
**`f"{product_id}:{doc_version_id}:{chunk_no}"`**.
Почему в этой границе: `chunk_id` — часть `LexicalChunk`/`RetrievedChunk` и адрес гидртации
из store, поэтому смена формата и так требует пересборки партиций; делать её отдельным
`content_change` после `partition-2` значило бы пересобирать дважды.

`doc_version_id` = `"dv_" + short_hash(product, content_hash, extractor_version, level)`
(`core/pipeline/incremental.py:279-290`) — **без двоеточий**, поэтому грамматика
`source_id` (`product:doc:chunk_no[:build_id]`, `core/util/ids.py:46-58`;
`parse_source_id` требует ровно 3 или 4 сегмента, `core/util/ids.py:60-95`) не меняется. Это не
косметика: `doc_hash` не различает уровень извлечения и версию экстрактора, поэтому
`layout`-документ и `text`-документ с одинаковым `content_hash` давали **один и тот же**
`chunk_id` при разном тексте.

### 6.1. `chunker_version` в `chunk_id` **не входит** — и это не упущение

Соблазн добавить его туда есть (`09` §14.6 п. 11 формулирует дефект именно так), но цена
не оправдана, а выигрыш мнимый:

- `chunker_version` — поле `EXECUTION_CONTRACT_FIELDS`, то есть **один чанкер на прогон**;
  внутри прогонной БД `chunk_id` уникален и без него (PK `corpus_chunks.chunk_id`);
- смена чанкера уже меняет `pb` (он в `expected` у `_partition_reuse_reason`,
  `core/pipeline/corpus_index.py:911-923`), то есть даёт новую партицию;
- evidence уже несёт физическую партицию (`cell_evidence.partition_build_id`), поэтому
  `(chunk_id, partition_build_id)` различает то, что «голый» `chunk_id` не различает.

Добавление четвёртого сегмента сломало бы грамматику: четвёртый сегмент `source_id` — это
`build_id` (`core/util/ids.py:46-58`), а `parse_source_id` требует, чтобы третий сегмент был целым
(`_CHUNK_RE`). Двух разных смыслов у одной позиции быть не может.

### 6.2. Durable-пространство не меняется

`chunk_row_id(doc_version_id, chunker_version, chunk_no)` (`core/pipeline/incremental.py:293-304`) уже
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

`StorePort.resolve` (`core/store/sqlite_store.py:3291`) сначала делает прямой lookup по
`corpus_chunks.chunk_id`, а при промахе разбирает `source_id` через `parse_source_id` и ищет
`WHERE product_id = ? AND doc_id = ? AND chunk_no = ?`. **Этот второй путь не может
сработать**: `parse_source_id` возвращает `doc_id` = второй сегмент = `doc_hash`
(проверено на живой БД: `chunk_id = digital_q_database:c78f…c7d:0`, а `corpus_chunks.doc_id`
= `digital_q_database:c78f…c7d`). То есть фолбэк ищет по голому хешу там, где лежит
`product_id:hash`.

Следствия для границы: (1) грамматика `source_id` **до** правки §6.3 не несла нагрузки —
работал только прямой lookup, поэтому смена `chunk_id` не ломала резолв; (2) фолбэк надо либо
починить (искать по `doc_id = f"{product_id}:{ref.doc_id}"`), либо удалить как мнимую
страховку. Решение границы: **починить** — иначе после смены формата он остался бы мёртвым
уже молча (`09` §14.6 п. 11). ✅ Исполнено: четыре шага в `resolve`, тесты —
`tests/store/test_resolver.py:164` (префиксная форма), `tests/store/test_resolver.py:221`
(версия документа).

---

## 7. Тесты (обязательные для полосы) — **✅ написаны**

> **As-built.** Пункты 1–10 закрыты, кроме одного названного пробела — п.2 (тест на смену
> `execution_contract_digest` осью устройства не написан, см. ниже); три файла созданы границей
> (`tests/pipeline/test_corpus_runs_boundary.py`, `tests/pipeline/test_evaluate_reuse_confirmed.py`,
> `tests/cli/test_report_provenance_payload.py`), остальные дополнены. Отдельно добавлены
> тесты, которых спецификация не требовала, а дефекты — требовали: две версии одного
> документа (обе подтверждают `❌`, обе резолвятся), `resolve` для **второй** версии,
> read-only БД прошлой версии без таблицы отображения.
>
> **07.11.2026:** `test_evaluate_reuse_confirmed.py` больше нет — правило `❌` снято F3
> плана 20 (`3699ed9`), и носитель проверки переехал в `tests/pipeline/test_absence_dated.py`
> (I2d: `_enforce_dated_absence`, токен `absence_undated`). Два других файла на месте.

Файлы названы по факту существующим (первая редакция ссылалась на пять несуществующих —
`tests/store/test_sqlite.py`, `tests/cli/test_wiring_embedder.py`,
`tests/pipeline/test_corpus_partition_reuse.py`, `tests/pipeline/test_corpus_coverage.py`,
`tests/cli/test_run_report_scope.py`; такого деления в репозитории нет).

1. `tests/store/test_schema_gate.py` + `tests/store/test_sqlite_store.py` — **сегодня**
   проверяют обратное исходному решению: свежая БД объявляет все прежние аддитивные колонки
   и индексы, файл чужой версии отвергается и остаётся неизменным. `persist_manifest` пишет
   новые колонки (иначе они `NULL` навсегда), `read_manifest` терпит необязательные колонки
   без `IndexError`.
2. **`embed_device` в контракте — тесты есть, но не там, где обещала граница.**
   `tests/pipeline/test_run_identity.py` поля `embed_device` не касается вовсе (и
   `len(EXECUTION_CONTRACT_FIELDS)` не пинуется нигде). Фактическое покрытие:
   `tests/cli/test_execution_axes.py:1283` (дефолт `cpu`),
   `tests/cli/test_execution_axes.py:1288` (run > deploy),
   `tests/cli/test_execution_axes.py:1302` (поле JSON-сериализуемо);
   `tests/pipeline/test_report.py:2277`, `tests/pipeline/test_report.py:2323`
   (манифест несёт значение как объявлено), `tests/pipeline/test_report.py:2359` (diff называет
   `execution_change: embed_device`, а не «недоступно»). **Прямого теста «`cpu`→`cuda`
   меняет `execution_contract_digest`» нет** — разница держится тем, что поле входит в
   материал дайджеста (`core/pipeline/digests.py:96`), а пин
   `PINNED_EXECUTION_CONTRACT_DIGEST` (`tests/pipeline/test_corpus_stage_timings.py:86`)
   фиксирует значение для `cpu`. Это названный пробел, а не «покрыто». ⬜ Остаток на
   08.11.2026: для точности такой тест **есть**
   (`tests/cli/test_execution_axes.py:1340` — `test_embed_precision_changes_the_contract_digest`),
   для устройства — нет.
3. `tests/config/test_deploy.py` — `deploy.embed.device` объявлен; неизвестный ключ в
   `embed:` отвергается (`extra="forbid"`, там уже есть параметризация
   `tests/config/test_deploy.py:658-676`);
   `run.yaml:embed.device` резолвится app < run, site-слой отсутствует.
4. `tests/cli/test_execution_axes.py` — `cuda` исключает `Lite` с видимой заметкой; `cuda`
   без `sentence_transformers` — `EXIT_USAGE`; `cpu` сохраняет прежний приоритет;
   `LocalEmbedder` получает `device` (подменённый конструктор).
5. `tests/pipeline/test_corpus_phase_boundary.py` (+ `tests/pipeline/test_corpus_runs_boundary.py`
   на переиспользование партиции) — `pb` различает `cpu`/`cuda`; партиция с другим `embed_device` в манифесте не
   переиспользуется с видимой причиной; подмена `vectors.fp16.npy` при неизменном манифесте
   отвергается (это и есть проверка `vector_digest`); старая партиция без `content_hash` в
   `chunk_ids.jsonl` — не переиспользуется, но читается.
6. `tests/pipeline/test_embed_cache.py` — ключ различает устройство; старая запись без
   устройства — промах, а не хит.
7. `tests/store/test_corpus_docs_partial.py` — `http_status`/`reuse_kind` доезжают до
   `corpus_docs` и читаются назад; монотонность записи (live/304 не понижаются до
   `reused`) — `tests/store/test_corpus_docs_partial.py:352`,
   `tests/store/test_corpus_docs_partial.py:373`. **Ветка «`❌` не публикуется на `reused`» переехала:** правило
   снято F3 плана 20, допустимость `❌` теперь держит датированный носитель
   (`tests/pipeline/test_absence_dated.py`).
8. `tests/pipeline/test_report.py` — `run_scope`/`product_plan` в манифесте и в `--json`
   (payload команды `report` блок манифеста **несёт**: `core/cli/commands/report.py:153`,
   `tests/cli/test_report_provenance_payload.py:96`); `run_product` заполняется по продуктам,
   `cells_carried` живёт в отчёте, а не в `run_product` (§3.2).
9. `tests/pipeline/test_report.py` (диагностика) — diff против прогона без `embed_device`
   даёт `execution_change: embed_device`, а не «поле недоступно».
10. `tests/store/test_resolver.py` — фолбэк `resolve()` по `source_id` после §6.3 находит
    чанк (`tests/store/test_resolver.py:164` — префиксная форма `doc_id`); `chunk_id` нового формата резолвится прямым
    lookup (`tests/store/test_resolver.py:202`).

### 7.1. Тесты, которые граница обязана была поправить (**✅ поправлены**; ниже — запись, что именно менялось)

- `tests/pipeline/test_corpus_stage_timings.py:83` `PINNED_PARTITION_BUILD_ID`,
  `tests/pipeline/test_corpus_stage_timings.py:86`
  `PINNED_EXECUTION_CONTRACT_DIGEST` (сверки
  `tests/pipeline/test_corpus_stage_timings.py:421`,
  `tests/pipeline/test_corpus_stage_timings.py:426`) — оба значения меняются.
- `tests/pipeline/test_corpus_phase_boundary.py:153-163`,
  `tests/pipeline/test_document_levels.py:574-585` — вызывают
  `_derive_partition_build_id(**явные kwargs)`; новый обязательный аргумент → `TypeError`.
- `tests/pipeline/test_corpus.py:934-948` — перечень вариантов чувствительности `pb`
  (подмножество параметров проходит, но перечень вариантов надо расширить).
- `tests/selfcheck.py:796-805` (список обязательных таблиц — `run_product` в него),
  `tests/selfcheck.py:819-833` (ассерты по колонкам `runs`/`corpus_docs`),
  `tests/selfcheck.py:1669-1673` (round-trip
  `source_id`).
- `docs/status-dictionary.yaml` — **✅ синхронизировано:** `pdf_device`
  (`docs/status-dictionary.yaml:439`), `embed_device`
  (`docs/status-dictionary.yaml:456`), `embed_precision`
  (`docs/status-dictionary.yaml:472`), `reuse_kind`
  (`docs/status-dictionary.yaml:761`), `run_scope`
  (`docs/status-dictionary.yaml:790`), перечень статусов продукта прогона `run_product_status`
  (`docs/status-dictionary.yaml:804`). Проверка —
  `core/domain/dictionary.py:80` `assert_dictionary_integrity`, `required`-список
  (`core/domain/dictionary.py:121` `embed_device`,
  `core/domain/dictionary.py:127` `run_product_status`), `checks`-таблица
  (`core/domain/dictionary.py:202`, `core/domain/dictionary.py:216`);
  словарь грузится selfcheck'ом.

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
   - **схема**: `core/store/sqlite.py` (DDL `run_product` —
     `core/store/sqlite.py:591-610`; `_ensure_columns` тогда существовал, **снесён
     01.11.2026** вместе с шимами, `core/store/sqlite.py:736-744`);
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
     (`resume_retained` + `consumed += 1`, `core/pipeline/corpus_drain.py:1515-1527`) и
     дренаж уже добытого расцеплены: URL
     заряжается в `consumed` и **не ставится в очередь**, а очередь строится только
     через `add_revalidate_seed` (`core/pipeline/crawl_frontier.py:1563`), чей work-set
     гейтится режимом продукта (`plan_incremental_items` под сетью отдаёт `[]` для
     `new`/`full` — `core/pipeline/corpus_ingest.py:4798-4800`). Правило: **заряженный URL обязан быть и
     дренируемым** — каждый элемент, засчитанный в `consumed`, ставится depth-0 листом
     (`add_revalidate_seed`, вне бюджета discovery) **независимо от режима**; гейт
     `mode == INCREMENTAL` для этого набора неверен, его настоящее предусловие — «есть
     durable-элементы для дренажа». ✅ **As-built** (`core/pipeline/corpus_drain.py:1581-1597`:
     `charged_urls` → `add_revalidate_seed`, комментарий называет дефект 3 и `09` §6.3).
     Плюс приписывание владельцу — тем же предикатом, что
     `group_revalidation_items` (`core/pipeline/corpus_ingest.py:4753-4785`, вызов —
     `core/pipeline/corpus.py:2608-2609`: побеждает самый специфичный
     `root_path_prefix`), а не «кто первый» (дефект 1); продукт-wide множество
     `resume_counted` (`core/pipeline/corpus.py:3548`) остаётся — меняется правило
     приписывания.
     **Почему нельзя сделать только приписывание:** измерено — у `digital_q_database`
     host-корень (prefix `/`, потолок 10) случайно забрал зарядку 2402 элементов, поэтому
     docs-корень получил `consumed = 0` и 2390 URL переингестились из retained-байтов без
     сети. Приписать их docs-корню (потолок 800) без второго полушага значит усечь его и
     потерять всё дерево — ровно судьба `pangolin`. Порядок обязателен: сначала
     «заряженное дренируется», потом «заряжается владельцу»;
   - **дефект краула 2** (дедуп очереди): `core/pipeline/crawl_frontier.py` (`_seen`,
     `core/pipeline/crawl_frontier.py:1194`, дедуп по ключу формы —
     `core/pipeline/crawl_frontier.py:1521`) — ключ дедупа должен схлопывать формы одного URL, различающиеся только
     завершающим слэшем, **не меняя** сохраняемый URL и не отменяя правило `09` §7.2 для
     **seed'ов** (там `p` и `p/` — разные утверждения автора, потому что seed задаёт
     path-prefix корня). Схлопывание **видимо**: счётчик в провенансе корня рядом с
     `skipped_blacklisted`/`skipped_documents`; тихое отбрасывание URL запрещено (тот же
     класс дефекта, что «тихий обход path-prefix», `09` §6.6);
   - тесты §7 + тесты обоих дефектов краула.

   **Чего в этом списке нет и почему:** `core/pipeline/composition.py` — первая редакция
   называла его, но в нём нет ни `embed_model_id`, ни `vector_digest`, ни `embed_device`:
   композиция оперирует только `(product_id, partition_build_id, content_digest)`
   (`core/pipeline/composition.py:93-108`, `core/pipeline/composition.py:158-166`,
   `core/pipeline/composition.py:272-280`). `core/store/state_sqlite.py`/`state_store.py` не трогаются:
   у `state.db` с 01.11 тот же гейт версии, что у `cohound.db` (шимы `_ADDITIVE_COLUMNS`/
   `_migrate` снесены волной S), а `state_adopt` читает `SELECT *`
   (`core/pipeline/state_adopt.py:629`) — новые колонки в `state.db` просто
   не попадут, и это приемлемо: `state.db` — durable-субстрат, а не прогонный провенанс.

3. **Писатель для `run_scope`/`product_plan` надо создать, а не «подключить».**
   Решение владельца 29.10.2026: `run_scope` **персистится** (`runs.run_scope` +
   `RunManifest.run_scope`), политика `--only-product` **не меняется** — scoped-прогон
   по-прежнему обрабатывает только выбранный продукт и не двигает
   `last_completed_run_id`. Закрывается дефект **видимости**: до границы читатель отчёта не
   мог отличить полное измерение от среза, не заглянув в `state.db`. `run_scope` не хранился
   нигде, кроме материала `run_id` (`core/pipeline/plan.py:94`;
   прямо зафиксировано в `core/pipeline/state_adopt.py:33`,
   `core/pipeline/state_adopt.py:120` — **текст там с 08.11.2026 сам устарел**:
   `runs.run_scope` объявлен (`core/store/sqlite.py:149`) и пишется `persist_manifest`
   (`core/store/sqlite_store.py:1244`); правка в `core/` — вне этой полосы), а `ProductPlanSet`
   существует (`core/domain/models.py:2878`) и **до границы** не персистился ни разу —
   единственное
   использование `core/pipeline/onboarding.py:95-112`. То есть `runs.run_scope` и
   `runs.product_plan_json` — новые записи, а не переезд существующей. ✅ После границы оба
   пишет `persist_manifest` (`core/store/sqlite_store.py:1303-1304`).
4. **Правило `❌` (часть той же полосы, `09` §11.2) — СНЯТО 05.11.2026 (F3 плана 20).**
   Проверка рядом с
   `_enforce_absence_coverage` (`core/pipeline/evaluate.py:2095`): у `explicit`/
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
   п. 7 §6; три места в `14`, где сказано, что `deploy.yaml` с секцией `embed:`
   отвергается `ValidationError`, становятся ложными; после границы секция существует, и
   отвергается только неизвестный ключ внутри неё; этот документ — из «спецификации» в
   «as-built» либо в архив. ✅ **Исполнено** (сверено 08.11.2026):
   `docs/architecture/14-vector-store-and-embedder-scaling.md:241-244` больше не утверждает
   отказ по секции, а говорит, что секция есть и отвергается только неизвестный ключ;
   `docs/architecture/14-vector-store-and-embedder-scaling.md:254-256` описывает резолв
   `deploy.embed.device` < `run.yaml:embed.device`.

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
  номер описывает условия измерения, а измерений под ним нет. ➖ **Снято 05.11.2026**
  (F3 планов 19/20): действует `0.3.0` (`core/domain/models.py:162`) — датированное
  отсутствие I2d.
- **п. 7** — `14` оговорка 5 и п. 7 §6 обновлены; `09` §4.3/§6.5/§11.2/§13.6/§14.1
  переведены в ✅.

**Откат:** граница не имеет обратного хода по идентичности (новый `run_id` — необратимо),
но и не разрушает ничего: старые партиции остаются на диске
иммутабельно, `state.db` не мигрирует. **Сверено 08.11.2026 — одно утверждение здесь
устарело:** «старые прогоны читаются» было верно на 29.10.2026 и ➖ **снято 01.11.2026**:
с версии `3.14.0` БД прошлой версии отвергается гейтом
(`SchemaVersionMismatch`, `core/util/schema_gate.py:40`), а не читается; прогон переносится
пересозданием (новый каталог/`run_id`). Единственное исключение — импорт
`state_adopt`: он открывает файл своим `mode=ro` соединением
(`core/pipeline/state_adopt.py:530`) и читает `schema_version` «для сведения», а не как гейт.
«Не разрушает» относится к файлам на диске:
`cohound.db` не переписывается, партиции не мутируются.

---

## 9. Чего в этой границе нет

- **carry-forward** и писателя `cell_measurement` (`09` §10) — волна 2, отдельная работа.
  **✅ Исполнена** (`9b6481b`): `cells.measurement_origin=carried`, писатель
  `cell_measurement` в `state_adopt`; `run_product` от этого не менялся (§3.2).
- **`deploy.embed` целиком** (`backend`, `base_url`, `model_version`) — `14` §6 п. 5:
  требует решения о появлении инстанса. Здесь вводится только `device` (06.11 добавил
  `precision` — тоже ось, не backend).
- **`--only-failed` межпрогонный** и `offline` — `09` §2.4. **✅ Исполнены** (`0e44d35`).
  Файловый lock на пакет (`09` §4.4) сделан отдельным коммитом `d80a6e4` и в границу не входит.
- **`SCHEMA_VERSION` bump** — сознательно не делался 29.10 (§3). ➖ **Снято 01.11.2026:**
  bump исполнен (3.14.0), на 08.11.2026 действует 3.20.0 (таблица в заголовке; канонический
  перечень истории — тоже там).
- **`corpus_docs.doc_id` → `doc_version_id`** (`09` §14.6 п. 12) — **снято** решением
  владельца 29.10.2026. `doc_id` дедуплицируется по `content_hash`, и это настоящее
  требование (один текст — одна строка корпуса); `dv` его не заменяет, он различает то,
  чего `content_hash` не различает. Обе идентичности сосуществуют: строка корпуса —
  по `content_hash`, версия — в отображении `corpus_doc_version` (§3.4).

---

## 10. Что вскрыла сверка спецификации с кодом (29.10.2026)

Первая редакция этого документа была написана по памяти о коде. Сверка с якорями
(`путь:строка`) дала семь расхождений, семь вещей, которых в спецификации не было вовсе, и
ещё семь, вскрывшихся при применении. Записаны здесь, чтобы вторая редакция не повторяла
ошибок первой.

**Расхождения, исправленные выше:** `_CONTRACT_FIELD_GETTERS` —
`core/pipeline/report.py:2563` (первая редакция ссылалась на строку 1970 того же файла);
`reuse_kind` в `url_state` —
`core/store/state_sqlite.py:150` (первая редакция ссылалась на строку 93 — там комментарий);
`_PARTITION_SCHEMA` —
`core/pipeline/corpus_index.py:54`; `composition.py` исключён из состава (§8);
«`embed_device` в `extra` **и** в корне» — одно место, а не два (§5.1); «геттер в
`_CONTRACT_FIELD_GETTERS`» — поле на манифесте достаточно (§4.2).

**Чего не было вовсе:**

1. **`EmbedConfig`/`EmbedRunConfig` не существовали** — имена были только в этом документе
   (§4.1). ✅ Созданы границей: `core/domain/models.py:514` (`EmbedRunConfig`).
2. **Порт эмбеддера без `device`** — и это правильно; устройство идёт явным параметром
   (§4.1). Иначе граница повторила бы дефект: спрашивать у адаптера, на чём он считает.
   ✅ Сверено 08.11.2026: порт по-прежнему без `device` (`core/ports/embedder.py:15-21`).
3. **Грамматика `source_id`** — `chunk_id` с четырьмя сегментами несовместим с
   `parse_source_id` (§6.1); и фолбэк `resolve()` был мёртв (починен, §6.3).
   ✅ Закрыто 29.10.2026: `core/pipeline/corpus_index.py:181-199` (форма `_chunk_id`),
   `core/store/sqlite_store.py:3291` (четыре шага `resolve`).
4. **`persist_manifest` — явный список колонок** (§4.2): колонка, не вписанная туда, не
   пишется никогда (на 29.10 — хотя `ALTER TABLE` её создал; с 3.14.0 шимов нет, колонка
   просто обязана быть в `DDL`). ✅ Сверено 08.11.2026: список по-прежнему явный,
   `core/store/sqlite_store.py:1228-1248` (40 колонок).
5. **`run_scope`/`product_plan` — не переезд, а новый писатель** (§8 п. 3).
   ✅ Закрыто 29.10.2026: `persist_manifest` пишет оба
   (`core/store/sqlite_store.py:1244`), колонки объявлены в `DDL`
   (`core/store/sqlite.py:149`).
6. **Пять из девяти тестовых файлов не существовали** (§7); плюс список тестов, которые
   сейчас пинуют старое поведение и обязаны быть поправлены (§7.1).
   ✅ Закрыто 29.10.2026: пять названных имён так и не появились (это и было утверждением —
   их не было), реальные носители — `tests/pipeline/test_corpus_runs_boundary.py`,
   `tests/cli/test_report_provenance_payload.py` и дополненные файлы §7.
7. **Три места в `14`**, где сказано, что `deploy.yaml` с секцией `embed:` отвергается,
   станут ложными после границы (§8 п. 7). ✅ Закрыто к 07.11.2026 (этой датой помечена
   сверка в самом `14`): текст переписан —
   `docs/architecture/14-vector-store-and-embedder-scaling.md:241-243` («секция
   `embed.device` **принимается**, а неизвестный ключ внутри `embed:` по-прежнему
   отвергается»), `docs/architecture/14-vector-store-and-embedder-scaling.md:254-256`
   (резолв `deploy.embed.device` < `run.yaml:embed.device`).

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
   (`core/pipeline/crawl_frontier.py:748-751`), то есть два уровня в одном прогоне законны.
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
   ➖ **Снято 05.11.2026** (F3 плана 20): правило «`❌` требует `live|revalidated` в этом
   цикле» отменено — `❌` держится датированным носителем (I2d,
   `core/pipeline/evaluate.py:2182` `_enforce_dated_absence`). Отображение
   `corpus_doc_version`
   осталось носителем адреса, по которому I2d находит `corpus_artifact`.
10. **`RunManifest` объявлял три lineage-поля, которых никто не заполнял.** `cycle_id`,
    `previous_run_id`, `change_class` (`core/domain/models.py`) есть в модели и **не
    передавались** в `build_manifest` — в `run_manifest.yaml` и `hound report --json` они
    были `None` всегда. Класс дефекта — «объявлено, но не написано»: читатель артефакта не
    видел, против какого прогона считался этот. Заодно правило «предыдущий прогон»
    существовало **в трёх копиях** (`run.py`, `state_adopt.py`, `state_sync`) — три способа
    разойтись; сведено в один резолвер `previous_run_of`
    (`core/pipeline/run_identity.py:439`). Источник манифеста — lineage-строка
    **самого** прогона, а не пересчёт: `change_class` вычисляется один раз при записи lineage.
    ✅ Сверено 08.11.2026: `persist_manifest` пишет все три
    (`core/store/sqlite_store.py:1245`).
11. **`previous_run_id` не доходил до фазы оценки.** `RunPlan` его не нёс,
    `evaluate_phase` не принимал. Для волны 2 (перенос ячеек) он нужен; сейчас потребитель —
    наблюдаемость (`log_entries` фазы называют, против какого прогона оценивали). В материал
    `run_id` он **не входит**: предшественник — не условие измерения, иначе каждый новый
    прогон менял бы идентичность от собственной истории.
12. **`CorpusReuseObservation` строится один проход на продукт, а не на ячейку.** 181
    параметр × продукты читали бы одну и ту же таблицу сотни раз. Сбой чтения виден в
    `errors` фазы, а не выдан за «наблюдений нет»: иначе правило снималось бы молча.
    ➖ **Снято 05.11.2026** (F3 плана 20, `3699ed9`): класса `CorpusReuseObservation` в
    `core/` больше нет — он существовал только ради правила `reuse_confirmed`, а то
    отменено. Наблюдением теперь служит `corpus_artifact`/`corpus_coverage.roots`.
13. **Счётчик дедупа форм URL считался, но никуда не выводился.** Спецификация (§8 п. 2)
    требовала «схлопывание **видимо**»; реализация завела `Frontier.duplicate_form_count`
    (`core/pipeline/crawl_frontier.py:1203`)
    и на этом остановилась — в провенансе корня его не было. То есть дедуп экономил бюджет
    **молча**, ровно тот класс, который запрещён («тихое отбрасывание URL»). Дописано в
    запись корня (`truncation[].duplicate_forms`, ключ появляется только при наличии
    дублей — `core/pipeline/corpus_commit.py:217`), тест —
    `tests/pipeline/test_corpus_artifacts.py:707`
    `test_duplicate_url_forms_are_visible_in_provenance`.
    Мораль: счётчик в объекте — не наблюдаемость; наблюдаемость — это поле в артефакте.
14. **`runs.cohort_ref` хранила имя файла когорты** (`cfg.cohort`), а не авторскую ревизию
    (`cfg.cohort_ref`), — при том что именно ревизия входит в `cohort_digest`. Манифест
    показывал ревизию, которой в `run_id` не было, а настоящая не сохранялась нигде.
    Писатель исправлен в двух местах (`create_run` и `build_manifest` — второй предпочитал
    `run.cohort`), `None` не подменяется именем файла: «ревизия не объявлена» и «ревизия
    равна имени файла» — разные факты. Тест различает два поля на одном входе.
    ✅ Сверено 08.11.2026: писатель — `core/store/sqlite_store.py:470` (авторская ревизия,
    комментарий называет `16` §2).
15. **`run_lineage.status` объявляет два статуса, которых никто не пишет** (перенесено из
    `09` §4.4 при его чистке 08.11.2026). DDL называет `running|complete|partial|failed|aborted`
    (`core/store/state_sqlite.py:275`), а `_LINEAGE_STATUS` (`core/pipeline/state_sync.py:67-72`)
    отображает только исходы прогона: `ok→complete`, `blocked→partial`, `failed→failed`,
    `skipped→partial`. `running` при старте и `aborted` при recovery следующего прогона
    **не пишутся** ни одной фазой: упавший посреди работы прогон в `state.db` просто
    отсутствует, хотя его run-DB (`runs/{run_id}/cohound.db`) существует. Два носителя
    описывают одну попытку и расходятся — вторая половина того же класса, что п. 10
    («объявлено, но не написано»), только здесь носитель не манифест, а линия циклов.
    Цена починки — два писателя (строка `running` при `create_run` и `aborted` при
    recovery); до неё «прерванный прогон» виден только отсутствием строки.

**Что осталось открытым на 08.11.2026 (не выдаётся за закрытое):**

- **⬜ Прямого теста «`cpu` → `cuda` меняет `execution_contract_digest`» нет** (§7 п. 2).
  Для `embed_precision` такой тест есть (`tests/cli/test_execution_axes.py:1340`), для
  устройства — нет; разница держится материалом дайджеста и пином под `cpu`.
- **⬜ Писателя `run_lineage.status='running'|'aborted'` нет** (§10 п. 15): прерванный прогон
  не оставляет строки в линии циклов, и `previous_run_of` шагает через него к предыдущему
  завершённому. Это осознанно не «долг реестра», а названный пробел провенанса.
- **✅ Расхождения §10 п. 1–7 и 8, 10, 11, 13, 14 закрыты кодом** (проверено выше), п. 9 и
  12 **сняты** вместе с правилом `reuse_confirmed` (05.11.2026). Открытых расхождений
  сверка 08.11.2026 не нашла, кроме названного теста.

---

*Документ — отчёт о применённой границе, а не задание. Все ссылки `путь:строка` проверены по
коду 08.11.2026 (HEAD `f14f866`) — каждая прочитана в файле, а не только проверена на
существование строки; если они разойдутся — прав код, а не текст. Прежняя дата сверки —
29.10.2026.*
