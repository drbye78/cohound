# 16. Версионная граница `runs`: схема, идентичность, состав

> **Статус: спецификация, кода нет.** Ничего из описанного ниже в `core/` не реализовано;
> это задание на одну полосу работ, а не отчёт о сделанном. Документ фиксирует **состав**
> границы и **порядок** её применения, чтобы она не проектировалась на ходу.
>
> **Решение владельца 29.10.2026:** всё, что меняет материал `run_id` или состав прогонной
> БД, делается **одной** границей. Две отдельные границы (`embed_device` отдельно, схема
> отдельно) обнулили бы прогресс дважды и дважды порвали бы межцикловый diff без пользы.

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
`live|revalidated`»** (`09` §11.2). Оно меняет условие публикации `❌`, то есть
absence/evidence-модель, а значит по `AGENTS.md` §13 это `methodology_change` с
обязательными `golden-full` **и** пере-аннотацией golden. Правило неотделимо от
`corpus_docs.reuse_kind`: без колонки оно неисполнимо, а колонка без правила — только
видимость. Разносить их на две границы значило бы платить за `golden-full` дважды.

**Четвёртая причина (решение владельца 29.10.2026): в ту же полосу входят два дефекта
краула**, найденные живым прогоном (`09` §6.3). Оба меняют корпус, то есть по отдельности
дали бы `execution_change` — а `golden_required_for` возвращает `full` **и** для
`execution_change` (`core/pipeline/run_identity.py:363`; `09` §12.1). Отдельная полоса
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
| `vector_digest` в проверке переиспользования | сборщик его пишет (`core/adapters/index_npy.py:275`), `_guard_immutable` сверяет (`:309`), но при переиспользовании сборка не строится — гард не исполняется | `content_change` |
| `content_hash` в `chunk_ids.jsonl` | **необходимое условие** предыдущего пункта, см. §5 | формат партиции `partition-2` |
| `embed_device` в ключе `embed_cache` | иначе CPU-прогон получит CUDA-векторы | стоимость, не идентичность |
| `runs.cohort_ref` — писать `cfg.cohort_ref`, а не имя файла когорты | манифест показывает ревизию, которой в `run_id` не было (`09` §12.2) | провенанс, `run_id` **не** меняется |
| `runs.run_scope`, `runs.product_plan_json` | `--only-product` меняет смысл среза и не виден в артефакте (`09` §13.6) | провенанс, `run_id` **не** меняется |
| таблица `run_product` | статус продукта в прогоне живёт только снимком в `state.db` (`09` §4.3/§6.5) | наблюдаемость |
| `corpus_docs.http_status`/`reuse_kind` | `❌` на retained-байтах без серверного подтверждения сегодня невыразим (`09` §11.2); колонка **исполняет** правило `❌` ⇒ `live\|revalidated`, а не только показывает его | **`methodology_change`** (условие публикации `❌` меняется) |
| `chunk_id` с `doc_version_id` вместо `doc_hash` | один и тот же текст на разных уровнях извлечения давал один `chunk_id` (`09` §14.6 п. 11); `chunker_version` в него **не** входит — довод в §6.1 | `content_change` |
| **дефект краула 1**: реконструкция бюджета на resume заряжает `max_documents` первому корню-претенденту, а не владельцу | потолок умножается на число перезапусков: измерено 1610 `fetched` при потолке 800 = `2 × 800` (`09` §6.3) | `execution_change` (корпус) |
| **дефект краула 2**: `Frontier._seen` дедуплицирует по точной строке URL | `/page` и `/page/` — два элемента очереди: измерено 1164 из 2402 `fetched` (48.5 %) у `digital_q_database`, 54 из 629 у `pangolin`, 0 у `jatoba` (`docs/reports/measurements/2026-10-29-crawl-url-duplication.md`) | `execution_change` (корпус) |

**Не входит** (остаётся на свои полосы): carry-forward ячеек и `cell_measurement` с
писателем (`09` §10 — волна 2), объявленный backend эмбеддера (`deploy.embed` целиком,
`14` §6 п. 5), межпрогонный `--only-failed` и `offline` (`09` §2.4), `RemoteEmbedder`. Файловый lock на
пакет (`09` §4.4) **сделан отдельно** (`d80a6e4`) — в границу не входит.

---

## 3. Схема прогонной БД: **без** bump `SCHEMA_VERSION`

**Решение: `SCHEMA_VERSION` остаётся `3.13.0`** (`core/store/sqlite.py:15`). Все изменения
ниже — аддитивные: колонки добавляет идемпотентный `_ensure_columns`
(`core/store/sqlite.py:496-512`), таблицу — `CREATE TABLE IF NOT EXISTS` в `DDL`. Гейт
`_reject_foreign_schema` (`:570-579`, вызовы `:638` и `:656`) существует ради
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
coverage)` (`core/pipeline/corpus.py:921`): одна функция на продукт, вызывается на границе,
а не накоплением в памяти. `cells_carried`/`cells_measured` **не заводятся**: переноса
ячеек нет (`09` §10), и колонка, всегда равная нулю, — ложь о том, что механизм
существует.

**Побочный эффект, который надо принять явно:** `cells_total`/`cells_pending`/`baseline_complete`
из `run_product` — те же величины, что `product_end` пишет в `results[].notes` прогона
(`corpus.py:786-808`). Два носителя одной величины разойдутся, если один из них обновят, а
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
(`_resume_page`, `corpus.py:3110`) — `reused`. Resume перечитывает и те документы, которые
в этом же `run_id` уже были получены от сервера (до перезапуска хоста), поэтому строка
этого прогона, несущая `live`/`revalidated`, **не понижается** до `reused`: иначе resume
стирал бы состоявшееся наблюдение. Наследовать `url_state.reuse_kind` **нельзя** — там
последняя классификация, в том числе из прошлого цикла (замер 29.10: 2157 `live` + 2009
`revalidated` + 0 `reused`, при том что 629 элементов `pangolin` перечитаны из
retained-байтов).
`reuse_kind IS NULL` в `corpus_docs` = «наблюдение не записано» (legacy-строки) ⇒ `❌` не
публикуется; писатель границы всегда пишет значение.

**Оба значения уже в руках у единственного писателя** — новой проводки не нужно:
`CorpusDoc(...)` собирается в `core/pipeline/corpus.py:5076`, рядом живут `reuse`
(`:5197` `reused = active_reuse is not None`) и `page.status_code` (он же уже уходит в
`url_state` как `last_http_status`, `:5217`; второй путь — `:5053`). Точка записи одна — `_persist_docs`
(`corpus.py:2021`, докстринг сам это фиксирует).

**Что спецификация обязана решить, а не оставить на ходу:** поле `CorpusDoc` или kwarg
`upsert_corpus_doc`. Прецедент в коде — `origin` (`corpus.py:2050-2052`): это kwarg, а не
поле модели, с доводом «доменная модель не получает смысла, которого у неё нет». Довод
здесь **не переносится**: `origin` пишется и назад не читается, а `http_status`/`reuse_kind`
обязаны читаться — `hound diff` строит дельту документов по `corpus_docs`
(`core/cli/commands/diff.py:105`), а правило `❌`-допустимости (I2c) обязано спрашивать
происхождение наблюдения там же. Решение границы: **поля
`CorpusDoc`** + чтение в `iter_corpus_docs` (`core/store/sqlite_store.py:2011-2040`, явные
kwargs) + `_has_column`-терпимость к старым БД (`:203`).

---

## 4. Идентичность: ось `embed_device`

### 4.1. Объявление

- app-слой: `deploy.yaml:embed.device` — **новая секция** `EmbedConfig` в `DeployConfig`
  (`core/config/deploy.py:578-595`; `extra="forbid"` `:588`, поэтому ключ без модели
  отвергается — секция обязана появиться вместе с полем). **Ни `EmbedConfig`, ни
  `EmbedRunConfig` в коде сегодня не существует**: имена встречаются только в этом
  документе. Обе модели создаются границей; `DeployConfig` сегодня несёт ровно
  `profile`/`edge`/`extractor_zone`/`search`/`crawl`/`llm` (`:590-596`), и `config/deploy.yaml`
  секции `embed:` не имеет.
- run-слой: `run.yaml:embed.device` (`EmbedRunConfig` рядом с `CrawlRunConfig`,
  `core/domain/models.py:477`), дефолт `None` — по той же причине, что у `pdf_device`
  (`:543-549`): дефолт run-слоя перекрыл бы app-слой в `_pick`.
- **per-site слоя нет**: устройство — свойство хоста исполнения, а не сайта (тот же довод,
  что в `resolve_pdf_device`, `core/pipeline/crawl_frontier.py:323-342`).
- Резолвер: `resolve_embed_device(app, run)` — копия `resolve_pdf_device` без site-слагаемого.
  Дефолт `cpu`, значения только `cpu|cuda`, `auto` **нет** (R5: объявленная ось обязана
  действовать). Enum `EmbedDevice` — в `core/domain/enums.py` рядом с `PdfDevice` (`:531-552`).
- **Порт эмбеддера не меняется.** `core/ports/embedder.py:15-21` знает только `model_id`/`dim`,
  и это правильно: устройство — объявленная ось прогона, а не свойство адаптера, которое
  можно у него спросить (спрашивание и есть дефект `14` оговорки 5). Значение протаскивается
  **явным параметром**: `phases.py:1096` → `corpus.py:3810`/`:2601`/`:2699`, там же, где уже
  идёт `embed_model_id` (`corpus.py:4161` — единственный источник этого значения).

### 4.2. Контракт

`EXECUTION_CONTRACT_FIELDS` — 28 → **29** полей (`core/pipeline/digests.py:80-181`):
добавляется `embed_device` рядом с `extractor_stack` (та же природа: устройство как условие
измерения). Значение кладёт `build_execution_contract` (`core/pipeline/run_identity.py:237`)
из `DeclaredExecutionAxes` — там же, где `extractor_stack` (`core/cli/execution_axes.py:943`).

**Совместимость чтения проверена:** `execution_contract_digest(values)`
(`core/pipeline/digests.py:315-337`) требует точного набора ключей, но вызывается ровно из
одного места — `run_identity.py:321`, внутри `build_execution_contract`, то есть только на
живом прогоне. Пересчёта из сохранённого словаря нет; `hound diff` сравнивает сохранённые
строки (`core/pipeline/report.py:1976-2000`), а отсутствующее на манифесте поле уходит в
`missing`, а не в исключение. Поэтому старые прогоны читаются, а их diff честно говорит
`execution_change: embed_device`.

`RunManifest` (`core/domain/models.py:1448`) получает поле `embed_device`, и этого
достаточно: `_contract_field_attr` (`core/pipeline/report.py:1968-1970`) сопоставляет имя
поля контракта с атрибутом манифеста напрямую, а `_CONTRACT_FIELD_GETTERS` (`:1960`)
заведён только для полей **без** одноимённого атрибута (сейчас там `serp_rounds`/`serp_mode`).
Без поля на манифесте диагностика назовёт ось «недоступной» вместо «изменённой».

Строку манифеста в БД тоже надо дописать: `persist_manifest`
(`core/store/sqlite_store.py:1228-1290`) — **явный список** из 29 колонок, а не `SELECT *`.
Колонка, не вписанная туда, останется `NULL` навсегда при живом `ALTER TABLE`; чтение —
`read_manifest` (`:2243-2380`), где новые поля идут через `_has_column` (`:203`), иначе
`row["embed_device"]` на старой БД даст `IndexError`.

### 4.3. Выбор эмбеддера перестаёт быть «что установлено»

`_resolve_embedder` (`core/cli/wiring.py:2670-2687`) сегодня выбирает по факту установки:
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
`SentenceTransformer(...)` (`core/adapters/embed_local.py:176-180`) — сегодня аргумента нет,
и устройство выбирает torch.

---

## 5. Партиция: формат `partition-2`

`_PARTITION_SCHEMA = "partition-1"` (`core/pipeline/corpus.py:308-310`) → `partition-2`.
Ревизия схемы уже даёт новые `pb` (старые партиции остаются на диске, но не
переиспользуются) — это и есть механизм, которым вносятся три изменения сразу.

### 5.1. `embed_device` в материале и в манифесте

Материал `_derive_partition_build_id` (`corpus.py:1959-1972`) получает `embed_device`
рядом с `embed_model_id`; манифест партиции — ключ `embed_device` в том же явном словаре
(`core/adapters/index_npy.py:250-276`, где `extra` сливается на `:270`; второго места нет);
проверка `_partition_reuse_reason` (`corpus.py:2801-2854`) — в `expected` рядом с шестью
текущими (`:2840-2846`: `content_digest`, `chunker_version`, `embed_model_id`,
`extractor_version`, `lexical_engine_version`, `vector_store`).

`_derive_partition_build_id` вызывается из `corpus.py:9130`, а в тестах — с явными kwargs
(`tests/pipeline/test_corpus_phase_boundary.py:153-162`,
`tests/pipeline/test_document_levels.py:574-585`): новый **обязательный** аргумент даёт там
`TypeError`, поэтому параметр вводится со значением по умолчанию либо тесты правятся в той
же полосе. Пинованные значения — `PINNED_PARTITION_BUILD_ID`
(`tests/pipeline/test_corpus_stage_timings.py:62`, сверка `:399`) — меняются неизбежно.

Почему **не** в `embed_model_id`: он описывает **веса**, а не место счёта, и классифицируется
как `content_change` (`core/pipeline/digests.py:68-79`). Положив устройство туда, мы
прочитали бы смену прибора как смену содержимого — то, от чего `pdf_device` ушёл через
`extractor_stack`.

### 5.2. `vector_digest` — и почему одного поля в `expected` мало

`vector_digest` считается как хеш `chunk_ids` + `content_hashes` + байтов матрицы
(`core/adapters/index_npy.py:119-130`) и пишется в манифест (`:275`). Но на диск
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

### 5.3. `embed_cache` получает третью компоненту ключа

Ключ сегодня — `(content_hash текста, embed_model_id)` (`core/pipeline/embed_cache.py`,
докстринг модуля). Векторы CPU и CUDA численно разные, а ключ их не различает — значит
после введения оси `cuda`-прогон молча взял бы `cpu`-векторы из кэша (и наоборот). Ключ
становится `(content_hash, embed_model_id, embed_device)`.

**Стоимость названа честно:** существующие записи кэша созданы без объявленного устройства,
поэтому приписать им `cuda` было бы догадкой; первый прогон после границы **пере-эмбеддит
корпус**. Число берётся из `resources.stage_timings.embed_s` текущего прогона
(`core/pipeline/corpus.py:2311`, `:2393`) — оно уже измеряется и будет опубликовано вместе с
отчётом. До этого числа границу не применять: это единственная её часть с реальной ценой.

---

## 6. `chunk_id` (run-level)

`_chunk_id` = `f"{product_id}:{doc_hash}:{chunk_no}"` (`corpus.py:2005-2013`, где
`doc_hash` — `extracted.content_hash`, `:3287`) → **`f"{product_id}:{doc_version_id}:{chunk_no}"`**.
Почему в этой границе: `chunk_id` — часть `LexicalChunk`/`RetrievedChunk` и адрес гидртации
из store, поэтому смена формата и так требует пересборки партиций; делать её отдельным
`content_change` после `partition-2` значило бы пересобирать дважды.

`doc_version_id` = `"dv_" + short_hash(product, content_hash, extractor_version, level)`
(`core/pipeline/incremental.py:271-279`) — **без двоеточий**, поэтому грамматика
`source_id` (`product:doc:chunk_no[:build_id]`, `core/util/ids.py:33-45`;
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
  `corpus.py:2840-2846`), то есть даёт новую партицию;
- evidence уже несёт физическую партицию (`cell_evidence.partition_build_id`), поэтому
  `(chunk_id, partition_build_id)` различает то, что «голый» `chunk_id` не различает.

Добавление четвёртого сегмента сломало бы грамматику: четвёртый сегмент `source_id` — это
`build_id` (`ids.py:33-45`), а `parse_source_id` требует, чтобы третий сегмент был целым
(`_CHUNK_RE`). Двух разных смыслов у одной позиции быть не может.

### 6.2. Durable-пространство не меняется

`chunk_row_id(doc_version_id, chunker_version, chunk_no)` (`incremental.py:281-296`) уже
содержит и `dv_id`, и `chunker_version`. Два идентификатора остаются разными намеренно
(`09` §14.6 п. 11).

### 6.3. Дефект, вскрытый при сверке: фолбэк `resolve()` мёртв

`StorePort.resolve` (`core/store/sqlite_store.py:2521-2560`) сначала делает прямой lookup по
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

## 7. Тесты (обязательные для полосы)

Файлы названы по факту существующим (первая редакция ссылалась на пять несуществующих —
`tests/store/test_sqlite.py`, `tests/cli/test_wiring_embedder.py`,
`tests/pipeline/test_corpus_partition_reuse.py`, `tests/pipeline/test_corpus_coverage.py`,
`tests/cli/test_run_report_scope.py`; такого деления в репозитории нет).

1. `tests/store/test_schema_gate.py` + `tests/store/test_sqlite_store.py` — аддитивные
   колонки создаются на пустой БД и на БД `3.13.0` без них; `SCHEMA_VERSION` не изменился;
   чужой `schema_version` по-прежнему отвергается; `persist_manifest` пишет новые колонки
   (иначе они `NULL` навсегда), `read_manifest` читает старую БД без `IndexError`.
2. `tests/pipeline/test_run_identity.py` — `embed_device` в контракте: `cpu`→`cuda` даёт
   другой `execution_contract_digest`; тот же — стабилен; `len(EXECUTION_CONTRACT_FIELDS) == 29`.
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

- `tests/pipeline/test_corpus_stage_timings.py:62` `PINNED_PARTITION_BUILD_ID`, `:65`
  `PINNED_EXECUTION_CONTRACT_DIGEST` (сверки `:399`, `:404`) — оба значения меняются.
- `tests/pipeline/test_corpus_phase_boundary.py:153-162`,
  `tests/pipeline/test_document_levels.py:574-585` — вызывают
  `_derive_partition_build_id(**явные kwargs)`; новый обязательный аргумент → `TypeError`.
- `tests/pipeline/test_corpus.py:875-892` — перечень вариантов чувствительности `pb`
  (подмножество параметров проходит, но перечень вариантов надо расширить).
- `tests/selfcheck.py:781-789` (список обязательных таблиц — `run_product` в него),
  `:800-826` (ассерты по колонкам `runs`/`corpus_docs`), `:1487-1490` (round-trip
  `source_id`).
- `docs/status-dictionary.yaml`: `reuse_kind` (`:651`), `run_scope` (`:670`), `pdf_device`
  (`:437`) есть, `embed_device` **нет**, перечня статусов продукта прогона **нет**.
  Синхронизация обязательна: `core/domain/dictionary.py:70` `assert_dictionary_integrity`,
  `required`-список (`:138` `reuse_kind`), `checks`-таблица (`:197`); словарь грузится
  selfcheck'ом.

---

## 8. Порядок применения

0. **Дождаться `report` текущего прогона** (`ru-pg-dbms-3d3dc7c3c969`), опубликовать
   замеры (включая `stage_timings.embed_s` — без него неизвестна цена §5.3) **и измерить
   деревья по факту**: потолки `max_documents` ставятся по замеру, а не по догадке (A3).
   Дефект краула 1 нельзя починить раньше: его цена измеряется тем же прогоном.
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
   - **дефект краула 1** (бюджет на resume): `core/pipeline/corpus.py` — реконструкция
     «уже израсходованного» (`:7379-7410`) обязана пользоваться **тем же** предикатом
     владения, что `_group_revalidation_items` (`:6653-6688`: побеждает самый
     специфичный `root_path_prefix`), а не «кто первый». Продукт-wide множество
     `resume_counted` (`:7066`) при этом остаётся — меняется правило приписывания;
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
   существует (`core/domain/models.py:2282`) и не персистится ни разу — единственное
   использование `core/pipeline/onboarding.py:95-112`. То есть `runs.run_scope` и
   `runs.product_plan_json` — новые записи, а не переезд существующей.
4. **Правило `❌` (часть той же полосы, `09` §11.2).** Проверка рядом с
   `_enforce_absence_coverage` (`core/pipeline/evaluate.py:1390`): у `explicit`/
   `authoritative` — каждый документ, на который опирается `❌`, обязан иметь
   `reuse_kind ∈ {live, revalidated}`; у корпусных классов — ни один документ корпуса
   продукта не `reused`/`stale`. Нарушение ⇒ ячейка `❓` (`not_stated`, I12) с причиной
   `reuse_unconfirmed` в `evidence_scope.reason` и `cells.validation_notes`,
   `absence_evidence=[]`. Зеркало в отчёте (как у `absence_coverage_unsatisfied`).
   `freshness`/`offline` **не** нужны: правило читает `corpus_docs.reuse_kind` этого прогона.
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

---

## 10. Что вскрыла сверка спецификации с кодом (29.10.2026)

Первая редакция этого документа была написана по памяти о коде. Сверка с якорями
(`путь:строка`) дала семь расхождений и три вещи, которых в спецификации не было вовсе.
Записаны здесь, чтобы вторая редакция не повторяла ошибок первой.

**Расхождения, исправленные выше:** `_CONTRACT_FIELD_GETTERS` —
`core/pipeline/report.py:1960`, а не `:1970`; `reuse_kind` в `url_state` —
`core/store/state_sqlite.py:106`, а не `:93` (там комментарий); `_PARTITION_SCHEMA` —
`core/pipeline/corpus.py:310`; `composition.py` исключён из состава (§8);
«`embed_device` в `extra` **и** в корне» — одно место, а не два (§5.1); «геттер в
`_CONTRACT_FIELD_GETTERS`» — поле на манифесте достаточно (§4.2).

**Чего не было вовсе:**

1. **`EmbedConfig`/`EmbedRunConfig` не существуют** — имена были только в этом документе
   (§4.1). Модели создаются границей.
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

---

*Документ — задание, а не отчёт. Все ссылки `путь:строка` проверены по коду 29.10.2026;
если они разойдутся — прав код, а не текст.*
