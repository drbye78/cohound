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
| `corpus_docs.http_status`/`reuse_kind` | `❌` на retained-байтах без серверного подтверждения сегодня невыразим (`09` §11.2) | наблюдаемость + I2c-честность |
| `chunk_id` с `doc_version_id`/`chunker_version` | смена чанкера даёт тот же id с другим текстом (`09` §14.6 п. 11) | `content_change` |

**Не входит** (остаётся на свои полосы): carry-forward ячеек и `cell_measurement` с
писателем (`09` §10 — волна 2), объявленный backend эмбеддера (`deploy.embed` целиком,
`14` §6 п. 5), межпрогонный `--only-failed` и `offline` (`09` §2.4), файловый lock на пакет
(`09` §4.4), `RemoteEmbedder`.

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
фазы — так же, как `corpus_coverage`. `cells_carried`/`cells_measured` **не заводятся**:
переноса ячеек нет (`09` §10), и колонка, всегда равная нулю, — ложь о том, что механизм
существует.

### 3.3. `corpus_docs` — происхождение наблюдения

```sql
ALTER TABLE corpus_docs ADD COLUMN http_status INTEGER;  -- NULL = сервер в этом цикле не отвечал
ALTER TABLE corpus_docs ADD COLUMN reuse_kind TEXT;      -- live|revalidated|reused|stale
```

Зачем: `url_state.reuse_kind` уже различает «сервер ответил 304» и «взяли retained-байты»
(`core/store/state_sqlite.py:93`), но в прогонную БД это не доезжает, и 304-документ
выглядит как живой 200 (`09` §11.2). Без этих колонок правило «`❌` нельзя публиковать на
неподтверждённом наблюдении» неисполнимо, сколько бы его ни уточнять текстом.

---

## 4. Идентичность: ось `embed_device`

### 4.1. Объявление

- app-слой: `deploy.yaml:embed.device` — **новая секция** `EmbedConfig` в `DeployConfig`
  (`core/config/deploy.py:578-595`; `extra="forbid"`, поэтому ключ без модели отвергается —
  секция обязана появиться вместе с полем).
- run-слой: `run.yaml:embed.device` (`EmbedRunConfig` рядом с `CrawlRunConfig`,
  `core/domain/models.py:477`), дефолт `None` — по той же причине, что у `pdf_device`
  (`:543-549`): дефолт run-слоя перекрыл бы app-слой в `_pick`.
- **per-site слоя нет**: устройство — свойство хоста исполнения, а не сайта (тот же довод,
  что в `resolve_pdf_device`, `core/pipeline/crawl_frontier.py:323-342`).
- Резолвер: `resolve_embed_device(app, run)` — копия `resolve_pdf_device` без site-слагаемого.
  Дефолт `cpu`, значения только `cpu|cuda`, `auto` **нет** (R5: объявленная ось обязана
  действовать).

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

`RunManifest` получает поле `embed_device` и геттер в `_CONTRACT_FIELD_GETTERS`
(`core/pipeline/report.py:1970`), иначе диагностика назовёт его «недоступным» вместо
«изменённым».

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
рядом с `embed_model_id`; манифест — поле `embed_device` в `extra` и в корне; проверка
`_partition_reuse_reason` (`corpus.py:2801-2854`) — в `expected` рядом с шестью текущими.

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

`_chunk_id` = `f"{product_id}:{doc_hash}:{chunk_no}"` (`corpus.py`) → включает
`doc_version_id` и `chunker_version`. Почему в этой границе: `chunk_id` — часть
`LexicalChunk`/`RetrievedChunk` и адрес гидртации из store, поэтому смена формата и так
требует пересборки партиций; делать её отдельным `content_change` после `partition-2`
значило бы пересобирать дважды.

Durable-пространство (`chunk_row_id(doc_version_id, chunker_version, chunk_no)`,
`core/pipeline/incremental.py`) не меняется — два идентификатора остаются разными намеренно
(`09` §14.6 п. 11).

---

## 7. Тесты (обязательные для полосы)

1. `tests/store/test_sqlite.py` — аддитивные колонки создаются на пустой БД и на БД
   `3.13.0` без них; `SCHEMA_VERSION` не изменился; чужой `schema_version` по-прежнему
   отвергается.
2. `tests/pipeline/test_run_identity.py` — `embed_device` в контракте: `cpu`→`cuda` даёт
   другой `execution_contract_digest`; тот же — стабилен; `len(EXECUTION_CONTRACT_FIELDS) == 29`.
3. `tests/config/test_deploy.py` — `deploy.embed.device` объявлен; неизвестный ключ в
   `embed:` отвергается (`extra="forbid"`); `run.yaml:embed.device` резолвится app < run,
   site-слой отсутствует.
4. `tests/cli/test_wiring_embedder.py` — `cuda` исключает `Lite` с видимой заметкой; `cuda`
   без `sentence_transformers` — `EXIT_USAGE`; `cpu` сохраняет прежний приоритет;
   `LocalEmbedder` получает `device` (подменённый конструктор).
5. `tests/pipeline/test_corpus_partition_reuse.py` — `pb` различает `cpu`/`cuda`; партиция с
   другим `embed_device` в манифесте не переиспользуется с видимой причиной; подмена
   `vectors.fp16.npy` при неизменном манифесте отвергается (это и есть проверка
   `vector_digest`); старая партиция без `content_hash` в `chunk_ids.jsonl` — не
   переиспользуется, но читается.
6. `tests/pipeline/test_embed_cache.py` — ключ различает устройство; старая запись без
   устройства — промах, а не хит.
7. `tests/pipeline/test_corpus_coverage.py` — `http_status`/`reuse_kind` доезжают до
   `corpus_docs`; `❌` на `reuse_kind=reused` (retained без ответа) не публикуется, на
   `revalidated` — публикуется при прочих равных.
8. `tests/cli/test_run_report_scope.py` — `run_scope`/`product_plan` в манифесте и в
   `--json`; `run_product` заполняется по продуктам, `cells_carried` не существует.
9. `tests/pipeline/test_report.py` — diff против прогона без `embed_device` даёт
   `execution_change: embed_device`, а не «поле недоступно».

---

## 8. Порядок применения

1. **Дождаться `report` текущего прогона** (`ru-pg-dbms-3d3dc7c3c969`) и опубликовать
   замеры, включая `stage_timings.embed_s` — без него неизвестна цена §5.3.
2. Один коммит: `core/store/sqlite.py` (DDL + `_ensure_columns`), `core/pipeline/digests.py`
   (+1 поле), `core/pipeline/run_identity.py`, `core/cli/execution_axes.py`,
   `core/pipeline/crawl_frontier.py` (`resolve_embed_device`), `core/config/deploy.py`,
   `core/domain/models.py` (`EmbedRunConfig`, `RunManifest.embed_device`), `core/cli/wiring.py`,
   `core/adapters/embed_local.py`, `core/pipeline/{corpus,embed_cache,composition}.py`,
   `core/adapters/index_format.py`, `core/pipeline/report.py`, тесты §7.
3. Прогон-ревизия: новый `run_id` (`execution_change`), полный `golden-full` по
   `09` §12.4; партиции пересобираются, эмбеддинг — по кэшу нового пространства.
4. Документы: `09` §4.3/§6.5/§11.2/§12.2/§13.6/§14.1 — с ⬜ на ✅; `14` оговорка 5 и
   п. 7 §6; этот документ — из «спецификации» в «as-built» либо в архив.

**Откат:** граница не имеет обратного хода по идентичности (новый `run_id` — необратимо),
но и не разрушает ничего: старые прогоны читаются, старые партиции остаются на диске
иммутабельно, `state.db` не мигрирует.

---

## 9. Чего в этой границе нет

- **carry-forward** и писателя `cell_measurement` (`09` §10) — волна 2, отдельная работа.
- **`deploy.embed` целиком** (`backend`, `base_url`, `model_version`) — `14` §6 п. 5:
  требует решения о появлении инстанса. Здесь вводится только `device`.
- **`--only-failed` межпрогонный**, `offline`, файловый lock на пакет — `09` §2.4/§4.4.
- **`SCHEMA_VERSION` bump** — сознательно не делается (§3).

---

*Документ — задание, а не отчёт. Все ссылки `путь:строка` проверены по коду 29.10.2026;
если они разойдутся — прав код, а не текст.*
