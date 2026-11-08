# Политика краула по областям и замороженный корпус (план)

**Тип:** план. **Владелец:** Roger. **Начало:** 04.11.2026, срез `1c76e6e`.
**Сверено с кодом:** 2026-11-08, HEAD `f14f866`. Действующие версии на эту дату —
`SCHEMA 3.20.0` (`core/store/sqlite.py:86`), `STATE "12"` (`core/store/state_sqlite.py:84`),
`METHODOLOGY 0.3.0` (`core/domain/models.py:162`); встречающиеся ниже «`7` → `8`»,
«`8` → `9`», «`9` → `10`» — история плана, а не текущее состояние.
**Ревью:** @oracle 04.11.2026 (14 обязательных правок внесены, см. §11).
**Отслеживание:** чеклист слоёв ниже; статус пункта — по факту кода, а не по обещанию
(правило маркеров — как в `README.md` этого каталога: ✅ / ⬜ / ➖ / ❔).

## 1. Зачем

Запрос владельца (04.11.2026):

1. Для **продукта, хоста, корня или маски пути внутри корня** задать режим краулинга:
   `auto` (как сейчас), `on_request` (первичный дискавери — автоматический, обновление — по
   ручному запросу через CLI), `scheduled` (обновление по расписанию), `network_denied`
   (ни дискавери, ни обновление).
2. Прогон может **считать ранее собранные данные краула неизменными** — все продукты когорты
   как будто в режиме «обновление по ручному запросу», включается флагом CLI.

Владелец 04.11.2026 уточнил, что нужны **оба** такта: инженерный (прогоны для апробации
решений, не связанных с краулом) и публикационный, где обновление корпуса **асинхронно**:
у разных частей корпуса продукта разные правила актуализации. Плюс артефакты можно
предоставлять **вручную, минуя прогон** (`hound package state adopt`). Отсюда следствие,
которое определяет архитектуру: **даты актуальности артефактов корпуса гарантированно
разные**, и карта обязана это не скрывать, а нести. Дата-модель артефактов и объявленный
снимок корпуса — отдельный план `20-corpus-snapshot-freshness-plan.md`; этот план даёт
**управление** обновлением, а не новую семантику вердиктов.

Зачем это нужно по существу. Сегодня «обновлять/не обновлять» решается только глобально
(`--refresh`), а отказ от обхода выражается лишь `exclude_paths` — это «не обходить», без
per-host статуса и без различения дискавери и обновления. Следствия, которые уже стоили
времени:

* **Нельзя измерить новый стек на старом корпусе.** `--refresh offline` переиспользует **чанки**
  из `state.db` и отказывает при смене `chunker_version`; пере-краула может не быть неделями.
* **Нельзя сказать «этот хост сейчас не обновляем» так, чтобы это было видно**: разница между
  «нечего обходить», «нет seed'ов» и «политика запрещает» не выражается и читается как сбой.

## 2. Решения владельца (04.11.2026)

| вопрос | решение | следствие |
|---|---|---|
| где живёт политика | **`state.db` оператора**, пакет задаёт дефолты | режим меняется без правки пакета; эффективная политика входит в идентичность |
| что значит «данные неизменны» | **ни одного запроса; корпус из сохранённых байтов** | нужен режим, отличный от `offline` (тот переиспользует чанки и отказывает при смене версий) |
| что блокирует `network_denied` | **только сеть**; продукт остаётся на карте | измеряется по сохранённым данным, в провенансе — `crawl_network_denied` |
| `❌` в замороженном прогоне | **владелец выбрал «публиковать»**; формально это правило о том, **какое наблюдение удостоверяет отсутствие**, и оно переносится в отдельный проход — план 20, фаза F3 (`methodology_change` + `golden-full`). Здесь `frozen` даёт механизм (ноль запросов, корпус из байтов), а не новую семантику `❌`. **F3 исполнена 05.11.2026** (I2d, `absence_as_of`), поэтому `frozen` публикует `❌` на датированном носителе |

## 3. Что уже есть (не изобретаем заново)

| механизм | где | чего не хватает |
|---|---|---|
| `RefreshPolicy = auto\|revalidate\|refresh\|offline`, резолв `--refresh` > `run.yaml` > `auto`, **входит в `run_id`** | `core/domain/enums.py:913`, `core/pipeline/run_identity.py:213`, `core/pipeline/plan.py:112` | нет per-scope значения; `frozen` нет; неизвестное значение молча падало в `auto` (исправлено в P1: `_as_refresh_policy`, `core/pipeline/corpus.py:434`) |
| обновление = условный GET (ETag/Last-Modified) для известных URL | `incremental.plan_incremental:116`, `url_state` (`core/store/state_sqlite.py:137`), `ReuseKind.revalidated` | — |
| ноль запросов, корпус из `state.db` | `corpus_ingest.offline_reuse_product:3784` | переиспользует **чанки**; при смене версии чанкера/пустом наборе/I19-расхождении и **наличных байтах** документ пересобирается текущим конвейером (`rebuild_from_stored_bytes`), байтов нет — `no_bytes`; не пишет `url_state`/`document_version` как «последнюю версию» — эту дисциплину наследует и `frozen` |
| per-path блэклист с видимым отказом | `blacklist_match:966`, `skipped_blacklisted` | не различает дискавери и обновление, нет per-host статуса |
| per-host[+path] оси и их digest в `run_id` | `core/config/domain_profiles.py:49`, `core/pipeline/report.py:673` | **не знает продукта** |
| `product → host+path` слои | `core/config/crawl_sources.py:51`, `core/config/crawl_sources.py:77`, `core/config/crawl_sources.py:89` | digest информационный (`core/pipeline/crawl_session.py:133`) |
| операторская запись в `state.db` (lock, append-only, `--dry-run`) | `hound package profile attest` (`core/cli/package/commands.py:2994`) | — образец для CLI политики |
| видимые отказы корня/продукта | `out_of_scope_subdomains` (`core/pipeline/crawl_frontier.py:1236`), `product_skipped` (`core/pipeline/corpus.py:5187`), `incomplete_reasons` (`core/pipeline/corpus_coverage.py:541`) | нет токена «запрещено политикой» |
| готовый канал «корень выпал, покрытие неполно» | `HOST_OUT_OF_SHARD_REASON` → `truncated_products` (`core/pipeline/corpus.py:2699`, `core/pipeline/corpus.py:2706`; `core/pipeline/corpus_coverage.py:405`) | переиспользуется политическими пропусками |

**Ни один носитель не даёт «продукт × хост × путь» без ложной идентичности:**
`crawl_sources.yaml` знает продукт, но не входит в `run_id`; `domain_profiles.yaml` входит в
`run_id`, но не знает продукта. Поэтому политика живёт в `state.db`, а в контракт входит её
**резолв** (§6).

## 4. Модель данных

**Имена.** Режимы — `auto | on_request | scheduled | network_denied`. Термин `blocked` **не
переиспользуется**: он занят трижды — `RunProductStatus.BLOCKED` (`core/domain/enums.py:1124`),
`onboarding_state='blocked'` (`core/store/state_sqlite.py:103`), `inheritance_mode=blocked`
(spec §…). В отчёте режим называется `network_denied`, токеном — `crawl_network_denied`.

**`scheduled` — это «due», а не демон.** У записи есть `interval_hours`; при прогоне режим
разрешается в решение `refresh`/`reuse` сравнением `now - observed_at` с интервалом. Так
работает MVP без фонового процесса; серверный планировщик (`core/edge/`) позже читает тот же
store. Решение «обновлять/не обновлять», принятое по часам, входит в поверхность решений
(§6.1) — иначе два прогона в разные дни дадут разные корпуса под одним `run_id`.

**Области и приоритет.** `product` | `host` | `root` (host + префикс пути) | `path` (host +
маска внутри корня). Разрешение — **самое специфичное побеждает**: `path` > `root` > `host` >
`product`; внутри вида — самый длинный совпавший литеральный префикс. При равной
специфичности `state.db` переопределяет дефолт пакета.

**Равная специфичность и разные режимы — конфликт, а не тай-брейк.** `EXIT_USAGE` с токеном
`crawl_policy_conflict`, называющим обе записи. Тай-брейк по `created_at`/`policy_id`
запрещён: он делает резолв функцией часов и порядка вставки (`created_at` — `TEXT` без
нормализации tz, `core/store/state_sqlite.py:359`), то есть одинаковый смысл даёт разные `run_id`.

**Несовпавшая запись видима, но не в дайджесте.** Строка, не совпавшая ни с одним объявленным
корнем, публикуется как `crawl_policy_unmatched` в `plan`/отчёте и **не** входит в материал
идентичности — иначе no-op правка оператора даёт `execution_change` у всех.

**Хранение (state.db).** Append-only (`superseded_by`), без `ALTER`-шимов: `STATE_SCHEMA_VERSION`
`7` → `8` (история; действующая — `12`), старая БД отвергается `SchemaVersionMismatch` (AGENTS §9).

```sql
CREATE TABLE crawl_policy (
    policy_id      INTEGER PRIMARY KEY,
    product_id     TEXT NOT NULL,
    scope_kind     TEXT NOT NULL CHECK (scope_kind IN ('product','host','root','path')),
    host           TEXT,          -- NULL только при scope_kind='product'
    path           TEXT,          -- NULL при 'product'/'host'; маска при 'path'
    mode           TEXT NOT NULL CHECK (mode IN ('auto','on_request','scheduled','network_denied')),
    interval_hours INTEGER,       -- обязателен при mode='scheduled', иначе NULL
    note           TEXT,
    actor          TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    superseded_by  INTEGER REFERENCES crawl_policy(policy_id)
);
CREATE INDEX crawl_policy_scope ON crawl_policy (product_id, scope_kind, host, path);
```

**Дефолты пакета** — `packages/{case_id}/crawl_policy.yaml` (необязательный,
`extra="forbid"`), те же три слоя, что у `crawl_sources.yaml`: `defaults` → `products[id]` →
`sites[id][host+path]`.

## 5. Семантика

Два **независимых** вопроса, не один enum:

* `network_mode ∈ fetch | deny` — разрешены ли запросы для этого корня;
* `corpus_source ∈ network | chunks | bytes` — откуда берётся корпус.

| режим политики | discovery (baseline нет) | update (baseline есть) |
|---|---|---|
| `auto` | `network` + `fetch` | `network` + `fetch` (условный GET по политике прогона) |
| `on_request` | `network` + `fetch` (первичный дискавери автоматический) | `chunks` (или `bytes` в `frozen`), `network_mode=deny` — **пока корень не назван `--refresh-scope`** |
| `scheduled` | `network` + `fetch` (первое снятие — как дискавери) | `fetch`, если `now - observed_at >= interval_hours`; иначе как `on_request` без запроса |
| `network_denied` | `deny` | `deny`; корпус — `chunks`/`bytes`, если есть сохранённое, иначе `no_sources` (видимая причина) |

**Ручной запрос — флаг прогона, не отметка в БД.** `--refresh-scope <product|host|url|mask>`
(повторяемый) разрешается тем же матчером. «Съеденная» отметка в `state.db` запрещена: два
прогона с одинаковыми объявленными входами получили бы разные корпуса под одним `run_id`.

**Композиция с политикой прогона** (`--refresh` действует **внутри** разрешённого):

* `auto`/`revalidate`/`refresh` — как сейчас, для областей `auto` и для `on_request`-областей,
  названных `--refresh-scope`;
* `offline` — ноль запросов, reuse **чанков**, а при непригодном chunk-set (чужая версия
  чанкера/пустой набор/I19) — пересборка из сохранённых байтов текущим конвейером
  (07.11.2026; три исхода `reused`/`rebuilt`/`no_bytes`, §3);
* **`frozen`** — ноль запросов, корпус из **`corpus_raw`** с пере-извлечением текущими
  версиями; `--frozen-source chunks` возвращает поведение `offline`.

В `frozen` области `auto` и `on_request` обрабатываются одинаково (сеть запрещена всем);
`network_denied` остаётся `network_denied` — различие в провенансе, а не в запросах.

## 6. Идентичность прогона

1. **`crawl_policy_digest`** — отпечаток **эффективной поверхности решений**: разрешённая
   политика после приоритета, дефолтов пакета, **per-run запросов `--refresh-scope`** и
   **разрешения `scheduled`-режимов в `refresh|reuse`** (due решается по часам, поэтому
   входит в материал как решение, а не как намерение); сортировка
   `(product_id, scope_kind, host, path, decision)`. `note`, `actor`, `created_at`,
   `policy_id`, `interval_hours` в материал **не входят** (режим и решение — входят); несовпавшие
   строки — тоже. Отдельного поля для `--refresh-scope` нет: это тот же материал (правило
   «одно поле — один материал», `core/pipeline/digests.py:186-196`).
2. Поле входит в `EXECUTION_CONTRACT_FIELDS` (`core/pipeline/digests.py:96`), 29 → **30**. Это объявленная
   версионная граница: переснять `PINNED_EXECUTION_CONTRACT_DIGEST`
   (`tests/pipeline/test_corpus_stage_timings.py:86`) и обновить литералы «29 полей»
   (`AGENTS.md:303`, `09…:3656`, `16-runs-schema-boundary.md:271`, `14…:424`,
   `docs/status-dictionary.yaml:447`).
3. **`refresh_policy`** уже в `run_id` отдельным слагаемым (`core/pipeline/plan.py:112`); `frozen` — новое
   значение. **`frozen_source` канонизируется**: в материал входит только при
   `refresh_policy=frozen`, иначе `n/a` (иначе объявленная, но не действующая ось даёт ложный
   `execution_change`).
4. **`hound crawl run` перестаёт хардкодить `AUTO`** (`core/cli/commands/crawl.py:461`): иначе
   `run` и `crawl run` адресуют разные сеансы при одном объявленном входе.
5. **Неизвестное значение политики — `EXIT_USAGE`, не `auto`.** Тихий fallback — это сеть по опечатке (AGENTS §2);
   реализовано в `_as_refresh_policy` (`core/pipeline/corpus.py:434`).
6. Время снятия (`captured_at`) — **провенанс**, не идентичность (как `extractor_version`).
7. Честная граница: резолв ветки «discovery vs update» читает `url_state`, поэтому **те же
   объявленные входы при сдвинутом `state.db` дают другой корпус** — так уже живёт `auto`
   (`incremental.plan_incremental`). План это называет, а не обещает обратное.

## 7. Слои работ

### P1 — хранилище, разрешение, CLI, дайджест (сеть не трогается) — ✅ сделано (`1c42b84`)

- ✅ Таблица `crawl_policy` (STATE 8), `StorePort`-методы, append-only через `superseded_by`
  (отзыв — самоссылка: tombstone-режима в схеме §4 нет).
- ✅ `crawl_policy.yaml` в пакете + загрузчик рядом с `crawl_sources.yaml`
  (`core/config/crawl_policy.py`).
- ✅ Матчер и `resolve_crawl_policy` (`core/pipeline/crawl_policy.py`): приоритет, дефолты,
  **конфликт равной специфичности**, `crawl_policy_unmatched`, разрешение `scheduled` в
  `refresh|reuse` по `now - observed_at` (часы — параметр, а не `datetime.now()` внутри).
- ✅ `hound crawl policy set|list|clear` по образцу `hound package profile attest`
  (lock, append-only, идемпотентность, `--dry-run` не пишет).
- ✅ `crawl_policy_digest` в контракте + переснятые пины и литералы §6.2.
- ✅ `plan` **печатает** резолв, но не исполняет.
- ✅ `_as_refresh_policy` на неизвестном значении — `ValueError` → `EXIT_USAGE`.

**Приёмка P1:** принята — `tests/pipeline/test_crawl_policy.py`
(`test_digest_ignores_order_and_note_actor_but_tracks_mode`,
`test_scheduled_due_is_a_decision_and_enters_digest_by_clock`,
`test_unmatched_is_visible_and_does_not_change_digest`), `tests/cli/test_crawl_policy_cli.py`,
`tests/store/test_state_schema_gate.py` (v7 отвергается, файл не меняется).

### P2 — исполнение: `on_request` и `network_denied` — ✅ сделано

- ✅ **Гейт выше первого сетевого вызова**: в `_author_root_plans` (`core/pipeline/corpus.py:3416`) — **до**
  `crawl_target` (`core/pipeline/corpus.py:3453`) и `expand_directory`
  (`core/pipeline/corpus.py:3459`), — и до сборки синтетических корней
  (`_synthetic_root_plans`, `core/pipeline/corpus.py:3303`). Гейт на постановке в очередь
  фронтьера (`core/pipeline/crawl_frontier.py:1532`, `add_seed`;
  `core/pipeline/crawl_frontier.py:1563`, `add_revalidate_seed`) недостаточен: он ниже
  feed/sitemap-разворота. Порядок закреплён **AST-тестом**
  (`test_gate_is_above_crawl_target_and_expand_directory`), который сравнивает номера строк
  вызовов в теле генератора: генератор — это порядок исполнения, текст комментария — нет.
- ✅ **SERP-контур** глушится политикой с `not_executed_reason=crawl_policy_skipped`
  (`core/pipeline/corpus.py:2252`), находки фильтруются по корню.
- ✅ `RootPlan.network_mode ∈ fetch|deny` и `RootPlan.corpus_source ∈ network|chunks`
  (`core/pipeline/corpus_drain.py:169`, `core/pipeline/corpus_drain.py:172`), а не один enum. `bytes` исполняется осью прогона
  (`--refresh frozen --frozen-source bytes`, P3), а не значением `RootPlan`: объявленное
  значение без исполнения дало бы ложный `execution_change`.
- ✅ `chunks`-корень проходит обобщённый offline-путь (`core/pipeline/corpus_ingest.py:3787`, «продукт →
  корень»), а не второй проход; `url_state`/`document_version` не переписываются.
- ✅ **Пропуск по политике делает покрытие неполным**: `crawl_policy_skipped` в
  `incomplete_reasons` + `truncated_products` (`core/pipeline/corpus_commit.py:105`) — тем же каналом, что
  `HOST_OUT_OF_SHARD_REASON` (`core/pipeline/corpus.py:3638`, `core/pipeline/corpus.py:3642`). `expected_sources` не уменьшается молча.
- ✅ Политический пропуск **не** идёт через zero-source: ветка `core/pipeline/corpus.py:5271` его исключает, поэтому
  `RunProductStatus.BLOCKED` («измерение не состоялось») не ставится.
- ✅ `--refresh-scope`: единый регистратор `add_refresh_scope_flag`
  (`core/cli/execution_axes.py:206`), паритет `plan`/`run`/`crawl run` по построению; значение
  входит в `crawl_policy_digest` через оси исполнения (7-кортеж решения), отдельного поля нет.
- ✅ Наблюдаемость: `crawl_network_denied`, `crawl_on_request_not_requested`,
  `crawl_policy_skipped`, `policy_scope` (+ `policy_skips` в `product_end`) и строки в
  `results[].notes` (`corpus_coverage.py:_product_notes`).

**Приёмка P2:** принята — `tests/pipeline/test_corpus_crawl_policy.py` (фейковый
crawler/search/poller/sitemap бросает на любом вызове; в `network_denied` и в `on_request` без
`--refresh-scope` вызовов ноль, `expand_directory`-шпион не позван), `tests/cli/test_refresh_scope_cli.py`
(паритет флагов, влияние на `run_id`, `unmatched_requested`, `EXIT_USAGE`). Живого прогона нет —
это названная граница, а не проверка.

### P3 — `frozen`: корпус из сохранённых байтов — ✅ сделано

- ✅ `RefreshPolicy.FROZEN` + `frozen_source ∈ bytes|chunks`. **Одна таблица диспетчеризации**:
  `core/pipeline/refresh_modes.py:83` (`_REFRESH_DISPATCH`, строка на каждое значение политики,
  без `.get(..., default)`); `dispatch_refresh` (`core/pipeline/refresh_modes.py:162`) — неизвестный enum → `AssertionError`,
  неизвестная строка → `ValueError`. Прежние ветки `is OFFLINE` в `corpus`/`incremental`/
  `corpus_ingest` заменены чтением таблицы. Статическое доказательство —
  `tests/pipeline/test_refresh_modes.py::test_every_refresh_policy_value_has_an_explicit_row`
  (сверяет множество значений enum с ключами таблицы) и
  `test_dispatch_has_no_silent_default_for_unknown_policy`.
- ✅ Путь `bytes`: `corpus_raw` → текущая экстракция → текущий чанкинг
  (`core/pipeline/corpus_ingest.py:4268`, `frozen_bytes_product`; общий шаг — `rebuild_from_stored_bytes`
  `core/pipeline/corpus_ingest.py:3527`, им же пользуется фолбэк переиспользования). Ключ `extract_cache` — хеш **сырых байтов**
  (`raw_path.stem`), поэтому повторный `frozen` в том же окружении попадает в кэш.
- ✅ **Дрейф пере-извлечения виден**: промах кэша с другим `content_hash` → токен
  `frozen_reextract_mismatch` (`core/pipeline/corpus_ingest.py:4573`), документ **не публикуется** и в
  `document_version` не попадает: ложного `document_id`, ложного дедупа и другого
  `doc_set_digest` не возникает.
- ✅ **Канонический адрес согласован и в `frozen`**: `crawl_session_items` не пишутся, дедуп
  идёт по durable-группам снятия (`corpus_capture_doc`, `doc_hash` → URL'ы), канонический
  адрес — `min(url)` **запросом**, а не гонкой готовности; результат не зависит от
  `host_concurrency`.
- ✅ **Запись снятия** — таблицы `corpus_capture` (`core/store/state_sqlite.py:371`) и
  `corpus_capture_doc` (`core/store/state_sqlite.py:388`): `captured_at`, `capture_digest`, `seeds_digest`,
  `coverage_complete`, `coverage_digest`, `doc_set_digest`, `reuse_kind` документов.
  `STATE_SCHEMA_VERSION` `8` → **`9`** (история; действующая — `12`). `url_state.last_checked_at` для этого **не годится**
  (пишется при любом исходе, включая отказ) — сказано в докстринге.
- ✅ Отсутствие байтов у известного URL — `stored_bytes_missing:{n}` (с числом),
  `unknown_roots`, причина покрытия `frozen_no_network_observation`; все — видимые причины в
  `product_end.frozen` и `results[].notes`, не `failed` (I4).
- ✅ `frozen`/`network_denied` не объявляют новую версию документа «последней»:
  `url_state`/`document_version` не переписываются (`test_frozen_bytes_writes_no_session_items`).

**Приёмка P3 (без сети):** принята — `tests/pipeline/test_corpus_frozen.py`
(`test_frozen_is_zero_network_and_reproducible_at_host_concurrency_two`: фейки бросают на любом
сетевом вызове, два прогона при `host_concurrency=2` дают равные `doc_set_digest`,
`corpus_docs.url` и текст чанков), `tests/cli/test_refresh_frozen_cli.py` (`run_id` `frozen` ≠
`auto`/`offline`, `frozen_source=bytes` при `auto` не меняет `run_id`, паритет флага),
`tests/pipeline/test_refresh_modes.py`, `tests/store/test_state_schema_gate.py` (v8 отвергается).

**Названная граница:** живой прогон на реальном `corpus_raw` не делался — проводка доказана на
фейках; STATE 9 обкатан только тестами.

### P4 — документы и отчёт — ✅ сделано

- ✅ AGENTS §6.1/§9: режимы (`auto|on_request|scheduled|network_denied`), приоритет
  областей, конфликт вместо тай-брейка, `frozen`/`frozen_source`, что входит в `run_id`
  (`refresh_policy`, канонизированный `frozen_source`, `crawl_policy_digest`), и отдельно —
  `network_denied` как **сетевая политика**, а не исключение продукта из сравнения.
- ✅ Манифест/отчёт: `crawl_policy_digest` — **колонка** `runs.crawl_policy_digest`
  (SCHEMA `3.16.0` → `3.17.0`), поэтому `hound diff` **называет** ось, а не печатает
  «различающееся поле не восстановимо из манифеста». Провенанс (`refresh_policy`,
  канонизированный `frozen_source`, `crawl_policy_unmatched`, `policy_skips`,
  `policy_zero_reasons`, `captured_at` только для `frozen`, оси `network_mode`/
  `corpus_source` по различающимся корням) — блок `runs.crawl_policy_json` и поля
  `RunManifest`; в `run_id` он **не** входит. Предел диагностики остаётся честным для осей,
  которых на манифесте по-прежнему нет (`extractor_stack`/`prompt_template`/…).
- ✅ `docs/status-dictionary.yaml` — машинные токены причин/осей (`crawl_policy_tokens`:
  конфликт, несовпадение, пропуск, отказ сети, `stored_bytes_missing:{n}`,
  `frozen_reextract_mismatch`, `unknown_roots`, `frozen_no_network_observation`).

### P5 — сквозная проверка — ✅ проведена (04.11.2026), два дефекта найдены и исправлены

- ✅ Смена одной записи политики меняет `run_id`; смена `note`/`actor` — **не** меняет
  (`tests/pipeline/test_crawl_policy.py::test_digest_ignores_order_and_note_actor_but_tracks_mode`,
  `tests/cli/test_crawl_policy_cli.py` — две политики → два `run_id`).
- ✅ `crawl run` и `run` дают один `run_id` (`tests/cli/test_run_identity_cli.py::test_run_and_crawl_derive_same_run_id`).
- ✅ `frozen` без сети на подготовленном `state.db` + `corpus_raw` даёт корпус, неотличимый по
  чанкам от `auto` на тех же байтах — **измерено** на живом прогоне (разрешение владельца,
  изолированный `COHOUND_HOME`, продукт `jatoba`, `--max-depth 0`, `--serp-rounds 0`):

  | | документов | чанков | извлечений | из кэша |
  |---|---|---|---|---|
  | `auto` | 9 | 597 | — | — |
  | `frozen` (до правок¹) | 2 | 37 | 5 | 2 |
  | `frozen` (после правок) | 7 | 326 | **0** | **7** |

  ¹ Первый прогон на том же стенде, до исправлений: 2 документа = оба авторских корня.

  По всем 7 общим `doc_id` совпадают `content_hash`, `extractor_version` и **списки чанков
  целиком** (`text` + `heading_path` + `context_prefix`), 7 из 7. Ноль извлечений —
  ключ `extract_cache` у `frozen` совпал с записью живого прогона.

- **Дефект 1 (исправлен): проход без сети видел только авторские корни.**
  `plan_incremental_items` возвращал `[]` для любого продукта не в режиме `incremental`.
  Под сетью это верно (BFS/sitemap найдут документы сами), под `offline`/`frozen` — нет:
  сети нет, значит `url_state` — единственный источник. Живой прогон шёл в режиме `full`
  (так резолвится продукт до полного снимка + оценки, `onboarding._mode_for`), поэтому
  `frozen` терял 7 из 9 известных документов — **молча**, с отчётом «нет байтов 0».
  Заметить это на `:memory:`-стенде было нельзя: приёмочные тесты P3 объявляли продукт
  `incremental`, а живой прогон — `full`. Регрессия:
  `tests/pipeline/test_incremental_items_modes.py` (режим × политика).

- **Дефект 2 (исправлен): пере-извлечение шло уровнем композиции, а не объявленным.**
  `frozen` вызывал переданный экстрактор (`docling` уровня `full`), тогда как корпус
  объявлен `layout_tables`: 4 из 5 PDF дали `frozen_reextract_mismatch` («тот же файл,
  другой текст»), выживший опубликован под чужим `extractor_version` и вторым
  `doc_version_id`. Теперь экстрактор выбирается per-URL через
  `extractor_factory(level, pdf_pipeline)` — тот же резолвер, что у живого пути и ручных
  артефактов, — один экземпляр на ключ `(уровень, механизм)`. Регрессия:
  `tests/pipeline/test_corpus_frozen.py::test_frozen_extracts_at_the_declared_level_not_the_composition_one`.

- **Остаток был назван числом и закрыт в §E.7:** 2 документа из 9 (271 чанк из 597) —
  члены контейнера `documentationJatoba18_6.zip`. Причина: контейнер — не документ, версии
  документа у него нет, поэтому путь и хеш его ретейненых байтов жили только в БД прогона
  (`archive_observations`/`corpus_members`), а проход без сети читает только `state.db`.
  Теперь их несёт `container_state` (STATE 10), и `frozen` спускает контейнер теми же
  байтами тем же обходом. Повторный замер (та же раскладка, `--document-level text` —
  на стенде не хватало памяти под `layout_tables`, preflight отказал с числами): **`auto`
  9 документов / 876 чанков, `frozen` 9 / 876**, `doc_id`/`content_hash`/
  `extractor_version` совпадают, списки чанков 9 из 9 идентичны, 1 контейнер → 2 документа
  / 731 чанк, 0 нечитаемых, 0 пустых, `без версии:18` (URL, отказавшие в живом прогоне по
  robots/404 — у них байтов нет по определению).

- **Провенанс, а не расхождение:** у двух документов `frozen` выбрал `min(url)` по
  доступному ему набору (декодированное имя файла), живой краул — по парам алиасов в
  `crawl_session_items` (percent-encoded форма). `doc_id`/`content_hash` совпадают,
  `doc_set_digest` (его поля — `doc_version_id`/`content_hash`/`extractor_version`/`level`)
  не зависит от метки URL; различается только `corpus_docs.url`. Запись снятия
  (`corpus_capture`) пишется **только** `frozen`, поэтому первый `frozen` после живого
  прогона алиасов прошлого краула не знает — это цена, названная, а не скрытая.

- **Проверено отдельно и снято как дефект: `--reset` идемпотентен.** Промежуточный замер
  показывал удвоение (19 → 38 чанков у одного документа), и причина оказалась не в
  `--reset`, а в дефекте 2: `chunk_id` включает `doc_version_id`, а пере-извлечение под
  другим уровнем давало второй `doc_version_id` — строки не совпадали по PK. После
  исправления повторный `--reset` на той же БД прогона даёт те же 7 документов и 326
  чанков, дублей нет. Чистить корпус при `--reset` не нужно и не следует: это была бы
  правка следствия.

## 8. Приёмка плана (сводно)

- Матрица приоритетов и конфликтов — таблица ожиданий, а не «похоже работает».
- Идентичность: дайджест стабилен при перестановке записей, меняется при смене режима или
  `--refresh-scope`, не меняется от `note`/`actor`/несовпавших строк.
- Сеть: в `network_denied` и `frozen` — **ноль** вызовов (фейки бросают), а не «мало».
- Покрытие: каждый пропуск по политике делает продукт неполным и назван токеном; ни один
  пропуск не выглядит как `failed` (I4) или как «нет seed'ов».
- I1–I23 не пересматриваются; `core/` нейтрален (режимы — операционные перечисления).

## 9. Риски и чего план не обещает

1. **`frozen` не делает корпус свежим** — он называет время снятия; читать его как «текущее
   состояние» нельзя.
2. **Пере-извлечение меняет текст** (`html-6`/`policy-5` на старых байтах) — это
   `content_change`+`execution_change`, а не «тот же прогон».
3. **Смешанный продукт** (часть корней `fetch`, часть `chunks`) — новая семантика исполнения
   (дедуп между reuse- и fetch-путями, порядок планирование → дренаж → переиспользование), а не
   «обобщение offline-пути». Решено оркестратором 05.11.2026 по объявленным правилам (I19, AGENTS §6.1; §E.6): переиспользование запрещённого
   корня идёт **в хвосте продукта**, после живых документов, и пропускает URL, у которого строка
   корпуса этого прогона уже есть, — поэтому живое наблюдение побеждает копию по построению, а
   не по порядку вставки.
4. **Не делаем:** расписаний и периодичности (`next_check_at` по-прежнему не читается),
   приоритетов URL внутри корня, per-URL режимов, автоподбора режима по частоте изменений.
5. **Не обещаем** экономию запросов там, где режим `auto`: цель — управляемость и
   измеримость, а не оптимизация обхода.

## 10. Журнал решений

| дата | решение | основание |
|---|---|---|
| 04.11.2026 | политика в `state.db` оператора, пакет задаёт дефолты | режим меняется без правки пакета; резолв входит в `run_id` |
| 04.11.2026 | «данные неизменны» = ноль запросов, корпус из **байтов** | `offline` переиспользует чанки и отказывает при смене версий |
| 04.11.2026 | `network_denied` блокирует только сеть; продукт остаётся | сетевая политика не убирает продукт из сравнения молча |
| 04.11.2026 | ручной запрос — флаг прогона, не отметка в БД | иначе разные корпуса под одним `run_id` |
| 04.11.2026 | режимы названы `auto\|on_request\|network_denied` | `blocked` занят трижды (`RunProductStatus`, `onboarding_state`, `inheritance_mode`) |
| 04.11.2026 | равная специфичность = конфликт, не тай-брейк | тай-брейк по часам/порядку вставки делает резолв неповторяемым |
| 04.11.2026 | `--refresh-scope` входит в `crawl_policy_digest` | он меняет корпус ⇒ обязан менять `run_id` |
| 04.11.2026 | неизвестное значение политики — `EXIT_USAGE` | тихий `auto` — сеть по опечатке |
| 05.11.2026 | смешанный продукт: переиспользование — в хвосте продукта, пропуск по **URL** | живое наблюдение обязано побеждать копию по построению, а не по порядку вставки (`reused`/`NULL` рядом с 200 — ложный провенанс) |
| 05.11.2026 | состав запрещённого корня = все durable-документы, владелец — специфичный `root_path_prefix` | под `full` ревалидационной группы нет: roots-only work-set терял листья молча (тот же failure mode, что 7 из 9 документов) |

## 11. Правки по ревью @oracle (04.11.2026)

Внесено 14 из 14: материал дайджеста включает `--refresh-scope`; конфликт вместо тай-брейка;
несовпавшие строки видимы и вне дайджеста; `frozen_source` канонизируется; гейт выше первого
сетевого вызова + SERP-контур; две оси (`network_mode` × `corpus_source`); пропуск делает
покрытие неполным; запись снятия вместо `last_checked_at`; `frozen_reextract_mismatch`; durable
группы дедупа; переименование режимов; переснятие пинов контракта; запрет переписывать
`url_state`; `EXIT_USAGE` на неизвестной политике.

## E. Что план не имеет права решать сам (нужно решение владельца)

| № | вопрос | почему это не деталь | статус |
|---|---|---|---|
| E.1 | **Публикация `❌` при различающихся датах** | правило «`❌` требует ответа сервера в этом цикле» (на момент плана — `_CONFIRMED_REUSE_KINDS` в `evaluate.py`; снято фазой F3, носитель — `core/domain/invariants.py:386`, `check_absence_dated`) и условие `observed_sources>0` (`core/pipeline/corpus_coverage.py:209`) закрепляли допущение «цикл = одна дата»; владелец требует архитектуру с **разными** датами | ✅ закрыто F3 плана 20 (05.11.2026, `methodology_change` 0.3.0): I2d — `❌` требует датированного носителя, `absence_as_of` публикуется; приёмка — `2026-11-05-absence-dated-acceptance.md` |
| E.2 | право менять правило `live\|revalidated` | решение владельца 29.10.2026 (`09…:3403`), не следствие плана | ⬜ |
| E.3 | носитель записи снятия (в плане — `STATE 8`, по факту — `STATE 9`: `STATE 8` занят `crawl_policy`) | необратимая схема-граница | ✅ реализовано и в работе (`corpus_capture`/`corpus_capture_doc`, STATE 9); подтверждение владельца — в §P5-прогоне |
| E.4 | продукт без сохранённых байтов при `network_denied` | мерить нечем: `❓`/`no_sources` или продукт выпадает из отчёта | ✅ решено владельцем 04.11.2026: продукт **остаётся на карте**, причина видна (`stored_bytes_missing`/`unknown_roots`), покрытие неполно, ни одна ячейка не публикуется как `❌` |
| E.7 | члены контейнеров в проходе без сети | `frozen`/`network_denied` пере-извлекает верхнеуровневые байты; члены контейнера приходят только из обхода (`ArchiveDescentWalker`), которого в этом проходе нет. Замер: 2 документа и 271 чанк из 597 на `jatoba` | ✅ решено владельцем 05.11.2026 («спуск контейнера в проходе без сети») и сделано: `container_state` (STATE 9 → 10 — история; действующая — `12`) пишется write-through живым спуском; `frozen` читает её **до** ветки `no_version` и спускает сохранённые байты inline (без пула, без `crawl_session_items`), называя невоспроизведённое (`stored_bytes_missing`, `frozen_container_unreadable`, `frozen_container_empty`, `frozen_container_replayed:{n}`). Замер: 9 / 876 против 9 / 876, чанки 9 из 9 |
| E.5 | `--refresh-scope` как условие измерения | если владелец считает это «операционным запросом», это противоречит `run_id = f(объявленных входов)` | ✅ решено: входит в дайджест |
| E.6 | смешанный продукт (часть корней `fetch`, часть `chunks`) | дедуп и порядок дренажа — новая семантика исполнения | ✅ решено 05.11.2026 — оркестратор, по объявленным правилам (I19, AGENTS §6.1), не решение владельца (три правила: строка корпуса — на **URL**; провенанс выжившей строки — наблюдение; состав запрещённого корня — **все** его durable-документы, владелец — самый специфичный `root_path_prefix`). Сделано: `_policy_skip` больше не переиспользует во время планирования, а только откладывает набор (`ProductContext.deferred_reuse`), хвост `_close_product` переиспользует его после живых документов с пропуском по URL (`skipped_present`, третий факт рядом с `reused`/`no_version`), состав корня считает `durable_urls_by_owner` (`state.db: url_state` — и есть work-set под `full`). Пропуск учитывает и **наблюдения прогона** (`ProductIngest.observed_urls`, пополняется в `finish_one` до дедупа): URL, наблюдённый живым путём, но схлопнутый по `doc_hash` в алиас близнеца, не получает `reused`-строку со старым носителем, хотя строки у него нет. Гейт политики остался до `crawl_target`/`expand_directory`; набор URL в сеть не изменился; пины — `tests/pipeline/test_corpus_crawl_policy.py` |
| E.8 | **`url_state.next_check_at` — второй уровень расписания или мёртвый носитель** (перенесено из `09` §6.3 при его чистке 08.11.2026) | колонка объявлена в DDL (`STATE 12`, `core/store/state_sqlite.py:147`), поле есть в порту (`core/ports/state.py:148`), значение переносится (`core/pipeline/corpus_ingest.py:960`), но **никто его не вычисляет и не читает** (`core/pipeline/incremental.py:145`). Расписание при этом уже есть — но **другой гранулярности**: `crawl_policy.mode=scheduled` с `interval_hours` решает «идти в сеть или переиспользовать» на scope (продукт/хост/корень/путь) и входит в `run_id` через `crawl_policy_digest`. Per-URL дата — это уже другой смысл: «этот документ смотреть не раньше N». Решение: либо объявить, зачем нужно per-URL расписание (и тогда получить писателя и читателя), либо **удалить носитель** — колонка без читателя объявляет возможность, которой нет (A3) | ⬜ ждёт решения владельца |