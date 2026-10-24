# coHound — архитектура: Итерация 4 — надёжность, CI, ADR

**Документ:** `docs/architecture/04-reliability-adr.md`
**Статус:** принято (ревью-гейт @oracle пройден 2026-10-08)
**Опирается на:** `00-overview.md` … `03-data-config-cli.md`
**Источник истины:** `cohound-spec-v2.md`
**Смежное:** `05-dependency-stack.md` (стек/пины/проверки V1–V5), `06-implementation-strategy.md` (план).

Детальный дизайн: (A) надёжность и обработка ошибок, (B) наблюдаемость, (C) CI-гейты,
(D) производительность, память и триггеры §14, (E) ADR-реестр, (F) полная матрица traceability.

---

## A. Надёжность и обработка ошибок

### A.1. Модель ошибок

| Класс | Где | Поведение | Инвариант |
|---|---|---|---|
| `search_failed` | Ш5/SERP | лог в `search_log`, ячейка → `failed` после retry | I4 |
| `source_unavailable` | Ш5 краул | retention других источников; не роняет прогон | — |
| `evaluation_failed` | Ш6 LLM | retry ≤2 → `failed` | §7, I4 |
| `validation_failed` | Ш6 эшелон | strict → `failed`; preliminary → кандидат | §1.4, §10 |
| `parser_failed` | Ш5 экстракция | `extractor_version` фиксируется; возможен re-extract из raw | I19 |
| `lint_*` | Ш1 | **блокирует** прогон (не failed ячейки) | §4.2 |

**Ключевое:** `failed` — это состояние **ячейки**, а не исключение. Прогон продолжается; failed
входит в знаменатель Coverage (I5). Системные/неожиданные исключения — отдельный класс (`UNEXPECTED`),
не маскируются под failed (защита эпистемологии: ошибка ≠ вердикт, §2.2 п.6).

**`unexpected` (S7).** `UNEXPECTED` — **run-level** ошибка **без ячейки** (системное исключение);
в `failure_kind` она **не входит**. `FailureKind` (`domain/enums.py:47-130`) — 16 значений
двух разных классов:

| класс | значения |
|---|---|
| **сбой ячейки / конвейера** | `search_failed`, `source_unavailable`, `evaluation_failed`, `validation_failed`, `parser_failed` |
| **наблюдаемые исходы краула** (пишутся в `crawl_session_items.error_kind`) | `not_found`, `robots_disallow`, `attempts_exhausted`, `redirect_out_of_scope`, `dlp_blocked`, `quarantined`, `extraction_timeout`, `payload_too_large`, `unsupported_binary`, `no_sources`, `coverage_missing` |

Разделение не косметическое: `not_found` — **разрешённое наблюдение отсутствия** (origin
ответил определённо), а `robots_disallow`/`parser_failed`/`extraction_timeout`/
`attempts_exhausted`/`dlp_blocked`/`quarantined` входят в `BLOCKING_FAILURE_KINDS` — они
делают корпус продукта неполным по построению (I2c), и `❌` без аттестации охвата не
публикуется. `dlp_blocked`/`quarantined` — зарезервированные классы egress-контура:
write-site'ов у них пока нет, но словарь обязан их знать заранее.

### A.1b. Модель угроз и граница зон (07)

Внешняя поверхность (краул/поиск/реестры/экстракция недоверенного контента) изолируется в **DMZ**
(`Z1 EDGE`), доверенное ядро (домен, store, оценщик, расчёт) — в **`Z0 CORE`** без веб-эгрессии.
Следствия для надёжности:
- **edge считается компрометируемым** → ядро валидирует **весь** входящий трафик шва (схема, лимиты,
  повторный `content_hash`, санитайзинг); ошибки шва → `source_unavailable`/`search_failed`, не роняют прогон;
- **сетевой слой усиливает I18**: evaluator-LLM в `Z0` без маршрута в интернет;
- **I19**: единственная точка входа внешнего контента — corpus ingest в ядре;
- отказ DMZ деградирует **все веб-волны** (реестры, корпус, SERP), но **наследование** (по `baselines`,
  без веба), **расчёт, отчёты и ревью** в `Z0` остаются работоспособными (изоляция отказов по зонам);
  частичный результат прогона сохраняется (resume).

### A.2. Отказоустойчивость фаз

- **Resume:** состояние фазы в БД; повторный запуск продолжает (см. итерацию 2 A.1).
- **Идемпотентность:** upsert по PK; сборка индекса — новая идентичность при изменении входов
  (двухуровнево: `partition_build_id` на партицию, `index_build_id` — композиция; ADR 0021).
- **Изоляция ошибок:** ошибка одного продукта/документа не прерывает остальные (собирается в
  `PhaseResult.errors`, продолжается).
- **Кэш:** живые внешние вызовы (краул, SERP) кэшируются в **глобальном content-addressed** кэше
  (`cache/serp/`, `cache/http/` в корне, ADR 0016); R7-прогон — из кэша (итерация 3 A.2), в т.ч.
  между разными `run_id`. Кроме сетевого кэша есть **два межпрогонных кэша вычислений**
  (`core/config/paths.py`): `extract_cache/` — результаты извлечения под ключом хеша retained-байтов
  (`pipeline/extract_cache.py`) и `eval_cache/` — сырые вердикты оценщика под ключом
  отрендеренного промпта + конфигурации модели (`pipeline/eval_cache.py`). Они не сетевые и
  имеют свой жизненный цикл, поэтому лежат отдельно. Практическое следствие для вопроса
  «почему прогон не ходил в сеть»: ответ может быть **не** про R7 — промах в `extract_cache`
  всё равно не требует сети, если байты уже ретейлены (`corpus_raw`), а `eval_cache` экономит
  вызов оценщика, а не фетч.

### A.3. Детерминизм (R7) — контрольные точки

| Источник нестабильности | Контрмера |
|---|---|
| LLM sampling | greedy или seed; `temperature`/`seed` из `ScenarioBinding`; constrained decoding |
| Версия весов/движка | pin в `run_manifest`; endpoint корп. облака ([OI]-совместимый API) |
| Порядок чанков | сортировка по fused-рангу с детерминированным tie-break (`chunk_id`) |
| Внешний поиск/краул | кэш + `snapshot_date` |
| Профиль/K по когорте | I16: единый `retrieval_profile`+K для всех продуктов; контроль в CI |
| Golden cut-off | I20: cut-off retrieval-golden = pipeline `retrieval_cutoff_K` |
| Поисковый бюджет | I23: фиксирован и залогирован в `search_log`; контроль в CI |
| Provenance evidence | `source_id → source_url → content_hash → build_id` (R15); резолв без сети |
| Плавающая сортировка путей | стабильная сортировка |
| Non-determinism удалённого инференса | документируется как известное ограничение (§11.7) |

`hound run` дважды с идентичными входами и кэшем должен давать ≥95% совпадений `status` (R7).

---

## B. Наблюдаемость

### B.1. Логирование

- Формат: JSON-lines, `runs/{run_id}/logs/{phase}.jsonl`.
- Поля: `ts`, `run_id`, `phase`, `component`, `level`, `message`, `ctx` (product/param/chunk_id),
  `invariant` (при нарушении).
- Уровни: `DEBUG` (retrieval-скоры), `INFO` (шаги фаз), `WARN` (аномалии §10.4), `ERROR` (сбои).

### B.2. Метрики прогона (в `run_manifest.yaml` и БД)

Источник — `pipeline/metrics.py::RunMetrics`: «метрика», которой нет в этом датаклассе, для
отчёта не существует, и наоборот — каждое поле пишется в манифест и в `metrics.json`.

| Метрика | Смысл |
|---|---|
| `cells_by_state` | pending/preliminary/evaluated/failed/reviewed |
| `failure_rate` | failed/(evaluated∪failed) — R13 |
| `coverage_aggregate` | Evidence Coverage по вселенной scope (§6/B-3a; `universe`/`completed`/`defined`) |
| `search_calls_by_phase` | расход поискового бюджета (I23) |
| `llm_calls`, `tokens` | стоимость (важно для M1/§15) |
| `duration_by_phase` | производительность |
| `crawl` | durable-сеанс краула: гистограмма `{status: count}` из `crawl_session_counts`; пусто, если сеанса нет |
| `crawl_truncation` | политическое усечение (N4): `truncated`/`truncated_reason`/`truncated_products`/`pages_fetched`/`revalidation_fetched`/`effective_limits`; пусто, если не усечён |
| `crawl_session` | жизненный цикл сеанса краула (F1/Д-6): `status`/`owner`/`heartbeat_at`/`stale_reason`; пусто, если сеанса не было. `interrupted` виден здесь без разбора БД |
| `resources` | F0 (`11` §4): `phases[phase] = {peak_rss_mb, wall_s}` (`VmHWM` и время на конец фазы), `scheduling = {extract_workers, extract_max_tasks_per_child, source}` (`source`: `declared`\|`derived` — окно выведено из бюджета памяти), `chunks_per_doc = {family: {count, p50, p90, max}}`, `documents`/`chunks`, `workers_peak_rss_mb`, `budget` (бюджет, его источник, `MemAvailable`, резерв, пол родителя, `cpu_count`, таблица пиков docling). Пусто — ресурсы не измерялись (нейтрально: «не измеряли» ≠ «ноль»). В `run_id` не входит: это расписание и наблюдение, а не условие измерения |

**Per-product события — не метрики, а заметки.** `product_start`/`product_end`/
`product_skipped` живут в `PhaseResult.log_entries` фазы корпуса и доходят до оператора
строками `results[].notes` (структурные dict-события в файл не сериализуются). В
`RunMetrics` их нет намеренно: это наблюдаемость хода работы, а не измеренная величина
прогона.

### B.3. Аудит и воспроизводимость

- `prompts/{prompt_hash}.json` — точный промпт каждой ячейки (аудит §7).
- `search_log` — полный лог поисковых вызовов (I23).
- `run_manifest.yaml` — все версии; достаточно для воспроизведения (плюс кэш/индекс).
- Провенанс evidence: `source_id → source_url → content_hash → чанк`.

---

## C. CI-гейты (§11.6)

### C.1. Проверки на PR

**`scripts/ci.sh` выполняет ровно два шага:** `python3 tests/selfcheck.py` и
`python3 -m pytest -q --cov=core` с порогом покрытия ядра (`fail_under = 80` в
`pyproject.toml`). Таблица ниже разделена по этому факту: «гейт» из второй половины в CI
**не** выполняется, даже если он обязателен к исполнению — его запускают вручную или на
релизном (full-CI) прогоне.

**В CI (`scripts/ci.sh` = `selfcheck.py` + `pytest --cov`):**

| Гейт | Что проверяет | Где |
|---|---|---|
| **Импорт-граф** | матрица зависимостей слоёв (итерация 1 §1) | `tests/selfcheck.py::_import_graph` (`ast`) |
| **Грепп-нейтральность** | предметных терминов в `core/domain`/`core/ports`/`core/pipeline` нет (три слоя — это и есть гейт) | `tests/selfcheck.py::_neutrality` |
| **DDL/триггеры** | схема + триггеры I4/I6/I7/I13/I18 | selfcheck |
| **Юнит-инварианты** | selfcheck: I1/I2/I2b/I4/I6/I7/I13 + Р-2; остальные — юниты по слоям | `tests/selfcheck.py` + `tests/*` |
| **Контрактные тесты портов** | порты — импортируемые `Protocol`; фейки детерминированы | `tests/selfcheck.py::_ports_contracts`, `tests/ports/`, `tests/fakes/` |
| **Retrieval-CI** | `NpyVectorIndex` без контейнеров, партиции и round-trip | `tests/adapters/test_index_npy.py`, `test_vector_npy.py` |
| **JSON-schema conformance** | constrained decoding даёт валидную форму | `tests/adapters/test_llm_local.py` |
| **I16 профиль** | единый `retrieval_profile`+K для когорты | `tests/pipeline/test_retrieve.py` |
| **I20 cut-off** | golden-K == pipeline `retrieval_cutoff_K` | `tests/pipeline/test_experiments.py`, `tests/cli/test_experiment_cli.py` |
| **I23 бюджет** | поисковый бюджет залогирован в `search_log` | `tests/pipeline/test_search_log.py` |
| **Provenance ячеек** | evaluated несут `index_build_id`+`retrieval_profile`; `evidence_id` детерминирован | `tests/store/` |
| **Контракт DMZ-шва (07)** | remote-proxy ↔ edge: схема запроса/ответа, версия, лимиты; ядро повторно хеширует и отвергает несоответствие | `tests/edge/` (fake-gateway + schema-тесты) |
| **Покрытие ядра** | `--cov=core`, порог 80% | pytest-cov |

**Вручную / full-CI (в `ci.sh` этого нет):**

| Гейт | Что проверяет | Инструмент |
|---|---|---|
| **golden-full** | согласие ≥80% на полной страте (см. §C.2) — нужен завершённый прогон | `hound golden --tier full` |
| **Линт пакета** | §4.2 + правила линтера на живом пакете (сам линтер покрыт `tests/pipeline/test_lint.py`) | `hound package validate` |
| **Разрешимость зависимостей** | `pyproject.toml` резолвится на базлайне Python 3.14 | `uv pip compile --python-version 3.14` |
| **Браузерный smoke (V5)** | scrapy-playwright: HTTP не видит JS, рендер видит (chromium+firefox) | `tests/smoke_scrapy_playwright.py` (отдельное окружение, 0/1/2) |

**`golden-fast` — не гейт (решение D-4).** Страта `fast` в `ru-pg-dbms` **пуста**:
`golden.csv` — 236 строк, все `tier=full` (118 `pangolin` + 118 `postgrespro`). Гейт снят
из таблицы по существу, а не из-за тихого «прошло»: пустой набор сегодня **падает** честно
(`agreement = 0.0` при нуле строк, плюс `has_absent_stratum = False` → `ok: no`,
`EXIT_ERROR`), но проверять ему нечего — зелёный результат такого прогона означал бы только
«строк не было». Он вернётся, когда страта `fast` появится: выделение дешёвых строк —
методологическое решение, а не предвидение (A3). Механизм тира живой: в `_template` есть
2 строки `fast`.

### C.2. Проверки на релиз/смену версий (golden-full)

- `golden-full` — **236 строк** в `packages/ru-pg-dbms/golden.csv`, **все** `tier=full`
  (118 `pangolin` + 118 `postgrespro`); страта `fast` пуста. Допуск по критическим классам —
  **0** false-✅ и **0** false-❌ на Кр (`max_false_ok=0`, `max_critical_false_absence=0`):
  это порог, а не измеренный результат прогона.
- retrieval-golden: R9/R10 с cut-off = pipeline-K.
- sensitivity по трём осям (R11).
- adversarial: 0 известных инъекций (R8, регрессионный gate).

Порог согласия — `min_agreement = 0.80` (`pipeline/golden.py:66`); это **временный** порог,
а не измеренная величина (A3).

### C.3. Структура тестов

```
tests/
  selfcheck.py                # инварианты (запускается без pytest; шаг 1 в ci.sh)
  test_selfcheck.py           # pytest-обёртка над selfcheck
  smoke_scrapy_playwright.py  # браузерный smoke (отдельное окружение; 0/1/2) — V5, вне ci.sh
  conftest.py                 # общие фикстуры
  domain/                     # юниты моделей/инвариантов/scoring (в т.ч. property-тесты, hypothesis)
  store/                      # схема, триггеры, транзакции, state.db
  pipeline/                   # фазы на моках портов (без сети/LLM): корпус, гейты, отчёты, эксперименты
  adapters/                   # краулеры, экстракторы, контейнеры, индексы, эмбеддеры, LLM
  cli/                        # команды и composition root
  config/                     # run.yaml/deploy.yaml/paths, профили доменов, seeds
  edge/                       # DMZ-шов: сервер, auth, audit, schema
  ports/                      # контракты портов
  packages/                   # пакетные артефакты (авторство пакета)
  registries/                 # реестровые спецификации
  util/                       # утилиты (кэш, robots, sitemap, feed, память, форматы)
  e2e/                        # сквозные сценарии
  fakes/                      # моки портов (детерминированные)
  fixtures/                   # общие фикстуры-данные
  helpers/                    # вспомогательные функции тестов
  test_util_*.py              # юниты util-слоя: кэш, robots, sitemap, feed, sanitize, ids/hashing,
                              #   rate_limit, логирование, html-ссылки, форматы/архивы
```

Структура — по факту `ls tests/`; каталога `tests/golden/` нет: golden прогоняется
командой `hound golden` против `packages/{case_id}/golden.csv`, а не тестовым каталогом.

Моки портов (`tests/fakes/`) позволяют прогонять конвейер без внешних систем (LLM/SERP/краул —
заглушки из 00 §8.1). Браузерный `smoke_scrapy_playwright.py` — ручной/full-CI тест: он требует
браузерных бинарей и не входит в `selfcheck.py`.

---

## D. Производительность и триггеры §14

| Риск | Мера сейчас (A1) | Триггер возврата |
|---|---|---|
| brute-force latency | продукт-партиции (~60k чанков), in-process | p95 > 300–500 мс на партицию → Qdrant |
| объём индекса | .npy шарды | >3–5 млн чанков/кейс → квантизация/шардирование |
| ANN-альтернатива | in-process brute-force (`.npy`) | продукт-партиция перестаёт влезать в RAM / brute-force p95 вне бюджета → ANN-адаптер `VectorIndex` (faiss-cpu / usearch) |
| эмбеддинги на CPU | батчи | ночные батчи мешают интерактиву → сервис |
| массовая запись в SQLite | WAL + батчи (200) | конкуренция записи → клиент-серверное хранилище (PostgreSQL) через тот же `StorePort`, **не** отдельная корпусная БД |
| search-адаптер latency/стоимость | бюджет + кэш | превышение бюджета → пересмотр профиля |

Ни одно «на вырост» не берётся без триггера (A1).

### D.1. Память и OOM

Главный измеренный риск прогона — не latency и не объём индекса, а **резидентная память
экстракции**: docling/torch держит рабочий набор, который Python не возвращает ОС, и RSS
растёт храповиком до OOM-kill. Ниже — рычаги as-built и их замеры. Провенанс цифр —
докстринг `core/util/memory.py` и комментарии `packages/ru-pg-dbms/run.yaml` (§`crawl`).

**1. Очередь страниц PDF — `PDF_QUEUE_MAX_SIZE = 1`.** (`adapters/extract_document.py:298`,
`_PDF_QUEUE_MAX_SIZE_BY_LEVEL`: `layout`/`layout_tables`/`full` → 1, `text` → `None` — у
`NativePdfPipeline` этого механизма нет.) Причина — не «PDF целиком», а **число страниц в
полёте**. Замер при дефолтном `queue_max_size=100` (пики воркера): `text` 1191 MiB,
`layout` 3074, `layout_tables` 3327, `full` 3911–4296 (на 68-страничной синтетике); живой
прогон `full` + 2 воркера убит OOM-killer'ом на пике 5906 MiB. При `1` на реальном
665-страничном документе: **4174 → 2126 MB (−49 %)** при байт-в-байт идентичном markdown и
без роста времени на страницу (1.46 → 1.30 s/стр., 866 с против потолка 1800 с).
Значение ручки пишется в провенанс манифеста (`pdf_queue_provenance`), хотя в
`extractor_version` и `EXECUTION_CONTRACT_FIELDS` не входит: иначе замер невоспроизводим.

**2. Возврат арен glibc — `malloc_trim(0)` после каждого документа.** `util/memory.py`
(`release_free_arenas`), вызывается из `corpus.py` (`_trim_worker_arenas`) в
**воркер-процессе** после получения результата и до возврата payload родителю. Замер
(22.10, `layout_tables`, один процесс): после 81-стр. PDF **1679 → 1084 MB**, второй trim
позже 1406 → 1240 MB; **без** trim четыре мелких PDF поднимали процесс с 1679 до 2052 MB.
Это арены аллокатора, а не объективная потребность. На системах без glibc — безопасный
no-op; наблюдаемость — счётчики вызовов, а не байты (`malloc_trim` не сообщает объём).

**3. Ленивый импорт docling.** `import core.adapters.extract_document` — **403 MB → 38 MB**,
`import core.cli.wiring` — 426 → 77 MB. «Пол» воркера, которому достался PDF, не изменился
(веса и арены никуда не делись); изменилось то, что **HTML-воркеры за него больше не
платят** (≈38 MB за импорт адаптера вместо 403 MB).

**4. Рециклинг воркеров — `extract_max_tasks_per_child`.** После N заданий процесс штатно
завершается, executor поднимает новый и заново строит экстрактор. Без этого рабочий набор
docling копится линейно (~12 MiB/документ): живой прогон OOM-killed на **456-м** документе
при RSS ~5.9 GiB. В `run.yaml` — 25 (компромисс «реже пересборка весов / ниже RSS»).
Ось действует только на процессном пуле (`local`); в `split` (потоки) — видимо
игнорируется.

**5. Члены контейнеров — через тот же пул.** Извлечение члена идёт в фазовом пуле с окном
`K = extract_window` и FIFO-доводкой (`corpus.py:5356-5460`), а не в родителе: иначе один
zip с 47 PDF держал родителя на 561 % CPU при простаивающих воркерах и копил память в
одном процессе (замер — `09 §6.7.1`). Байты членов при этом не ретейнятся — стрим во
временный файл.

**Арифметика, которую надо считать до прогона.** «Пол» docling-воркера ≈ **1.1 GB**
(torch 231 + docling-импорты 172 + веса ~500 + страница в полёте); HTML-воркер платит
≈38 MB за импорт адаптера (а не 403 MB, пока docling не импортирован).
Отсюда `layout_tables` + 2 воркера ≈ 2 × 2126 + baseline родителя ≈ 4.8 GB из 6.9 GB —
помещается, но запаса мало, поэтому 2, а не «сколько ядер». Для `full` (OCR) та же
арифметика **не проверена** после замера `queue_max_size`, а до него OOM был именно на
`full`.

**Почему это не ось контракта (ADR 0022).** Все перечисленные рычаги не меняют ни одного
значения **измерения** (текст, хеши, чанки, статусы, счётчики), поэтому они **вне**
`EXECUTION_CONTRACT_FIELDS`, не меняют `run_id` и не требуют `execution_change`. Заводить
конфигурационную ось намеренно не стали: ось попала бы в `CrawlLimits.as_dict()` →
`crawl_config_digest` → контракт, то есть гигиена памяти ветвила бы идентичность прогона.

---

## E. ADR-реестр

Решения фиксируются **таблицей ниже** — это и есть реестр ADR. Отдельного каталога
`docs/architecture/adr/` с файлами `NNNN-title.md` **нет** и он не заводится: реестр
поддерживается здесь, а детали решений живут в разделах этого документа, в регламенте и в
`07`. Формат записи — Status/Context/Decision/Consequences, сжатый до трёх колонок.

| ADR | Решение | Обоснование | Статус |
|---|---|---|---|
| 0001 | Порты/адаптеры (hexagonal), ручной DI | A2: commodity заменимо; изоляция core | accepted |
| 0002 | Продукт-партиции индекса + NpyVectorIndex | I17, A1 | accepted |
| 0003 | Порт LLM без `tools` (I18 структурно) | evidence-модель | accepted |
| 0004 | Весь внешний контент только через Ш5 (corpus_raw→hash→index) | I19 | accepted |
| 0005 | `retrieval_cutoff_K` единый для golden и pipeline | I20 | accepted |
| 0006 | Реестры как конфиг-данные + generic-адаптер | §1.3 | accepted |
| 0007 | SQLite + WAL + батчевые транзакции | A1 | accepted |
| 0008 | Иммутабельные индексные сборки, версии в манифесте | §13 | accepted |
| 0009 | Кэш внешних вызовов для R7 | R7 | accepted |
| 0010 | `failed` — состояние ячейки, не исключение | I4/I5 | accepted |
| 0011 | Гейты G1–G4 — только человеческие решения | I6/I7 | accepted · уточнён 0023 |
| 0012 | Расщепление бывших `core/schemas.py`/`core/db.py` на слои | чистота слоёв | accepted · **реализовано** (M0/W1, шимы удалены) |
| 0013 | Гранулярность индексной сборки и provenance ячеек | R7/R15, §13 | accepted · уточнён 0021 |
| 0014 | SERP-петля vs иммутабельность: append-only раунды сборок | I19, §13, K2 | accepted |
| 0015 | Политика ранжирования/демотирования: requested vs computed | A3, K6 | accepted |
| 0016 | Глобальный content-addressed кэш внешних вызовов для R7 | R7, §11.7 | accepted |
| 0017 | Контракт derived-evidence наследования/сателлитов | I1/I10/I15/I19, §4.2 | accepted |
| 0018 | Доверенные зоны + DMZ-шов (веб-контур за L7; remote-адаптеры + `ContentGateway`) | I18/I19, §10 | accepted |
| 0019 | Инспектируемый контракт шва для корп. IDP/DLP/IDS (L7-JSON, публикуемые схемы, без pinning, OIDC/mTLS) | ИБ-прозрачность, §10 | accepted |
| 0020 | Coverage по **вселенной scope** (§6/B-3a: `scope_universe`, `universe`/`completed`/`defined`, UNDEFINED) + разделение `methodology_version`/`EXECUTION_FIELDS` (§13/B-7) | A2/A3, I5/I11/I13 | accepted · **v2.1** |
| 0021 | Двухуровневая идентичность индексной сборки: `partition_build_id` (`pb_`) на партицию + `index_build_id` (`cb_`) как композиция `(product_id, partition_build_id, content_digest)`; `params`/`snapshot_date` вне `pb_` | I17, §13, R7: переиспользование партиции при смене фокуса/каталога без коллизии same-id/different-content (уточняет 0013) | accepted · **реализовано** |
| 0022 | Память экстракции — **операционный рычаг, вне** `EXECUTION_CONTRACT_FIELDS`: `PDF_QUEUE_MAX_SIZE`, `malloc_trim(0)` после документа, ленивый импорт docling, рециклинг воркеров | A1/A2: не меняет наблюдаемых значений, значит не вправе ветвить `run_id` (§D.1) | accepted · **реализовано** |
| 0023 | Гейты как **точки интеграции**: G1/G2 — внутренняя аттестация профиля, G3/G4 — приём внешнего вердикта; решение привязано к `subject_digest`; waiver видим | I6/I7, §13: устаревшее одобрение не действует, молчание внешней системы не выдаётся за отказ или одобрение (уточняет 0011) | accepted · гейт **реализован**; производителя аттестации профиля в прогоне нет (design-only) |

**Статус ADR после ревью-гейта (2026-10-08) и полосы гейтов (`48c6627`):** 0001–**0023** —
**accepted**. Правки K1–K7/S1–S13 интегрированы в регламент, схемы, DDL и словарь; 0013–0017 —
следствия ревью-гейта; 0018 — развёртывание/DMZ (07); 0019 — прозрачность контрактов для
IDP/DLP/IDS (07 §8); 0021–0023 — операционный слой после M1 (двухуровневая сборка, память,
гейты-интеграции). Оговорка по 0023: реализована **гейтовая** половина (привязка к
`subject_digest`, вердикт, waiver, экспорт request-файла); durable-аттестация профиля
(`profile_attestation`) объявлена и проверяется гейтом, но **писателя в прогоне не имеет** —
строку пишут только тесты, CLI-команды аттестации нет. Поэтому рабочий путь G1/G2 сегодня
один: per-run строка `gate_decisions`.

**v2.1 (0020).** Методологическая ревизия v2.0 → v2.1 (коммиты `5401991`/`06b0bab`/`ee4e295`)
зафиксировала **Coverage по вселенной scope** (знаменатель — `Σ w(scope)`, а не существующие ячейки:
`pending`/missing в знаменателе, `N/A`/`preliminary` — вне, I7/I13; `universe=0` → UNDEFINED, I11/G4)
и **разделение классов версий** diff-причин: `methodology_change` (`METHODOLOGY_FIELDS` + major
`methodology_version`) vs `execution_change` (`EXECUTION_FIELDS`) — §13/B-7. `ranking_epsilon`
(порог неразличимости) и `settings_provenance` (`default|measured`, A3) добавлены в `RunConfig`/
`RunManifest`; `SCHEMA_VERSION` SQLite-схемы — **`3.12.0`** (на срезе v2.1 было `3.5.0`;
дальше аддитивно: 3.10.0 — `corpus_members`/`archive_observations`, 3.11.0 — поля
`gate_decisions` полосы гейтов, 3.12.0 — `crawl_sessions.owner`/`heartbeat_at` (свидетельство
живости) и `interrupted` в `crawl_session_status`; с 3.12.0 файл другой версии **отвергается**
гейтом `11` §2.6, а не мигрируется молча), `STATE_SCHEMA_VERSION` схемы `state/{package}/state.db` —
**`"4"`** (v4 добавила `profile_attestation`). Основное из накопленного: E2 —
`runs.registry_spec_digest`, дайджест package-scoped спецификаций реестров, граница
`execution_change` §13; P1 — `run_inputs` как источник истины входов плана +
деплой-провенанс/`ranking_epsilon`; ранее P2 — контент-адресуемый `index_build_id`
(с 0021 — двухуровневый), версии/`content_digest` в `corpus_chunks`/`index_manifest`;
D3 — durable-сеанс краула `crawl_sessions`/`crawl_session_items`; Phase 2 —
`runs.crawl_provenance_json`, провенанс краула, информационно, вне границ §13;
Backlog B — `runs.crawl_strategy`/`runs.crawl_rate_limit`, операционные оси краула,
границы `execution_change`, §13.

---

## F. Полная матрица traceability (регламент → дизайн)

| Регламент | Итерации | Разделы |
|---|---|---|
| §1.3 нейтральность | 0,1 | 0§3, 1§1, 1§7 |
| §1.4 режимы | 1,2 | 1§2.1, 2A |
| §1.5/1.6 когорта/якорь | 1,3 | 1§2.2, 3D.2 |
| §2.3 инварианты | 1,4 | 1§2.3, 1§6, 4A |
| §3.1/3.2 стек | 0,1,5 | 0§4, 1§3, `05-dependency-stack.md` |
| §4.2 пакет категории | 1,2 | 1§2.1, 2B, 2D |
| §4.3 паспорт/сателлиты | 1,2 | 1§2.2, 2B.2, 2D |
| §4.4 БД | 2,3 | 2B, 3B |
| §4.5 run.yaml | 3 | 3D.1 |
| §5 конвейер | 2 | 2A–2D |
| §6 расчёт | 2 | 2E.1 |
| §7 LLM-контуры | 1,2 | 1§3.1/3.4, 2C |
| §8 человеко-машинные интерфейсы | 2,3 | 2A.2, 3B.1, 3E |
| §9 артефакты | 2,3 | 2E.2, 3A.2 |
| §10 защита | 2 | 2C.2, 07 |
| §11 golden/CI | 2,4 | 2B.3, 4C |
| §12 модель ошибок | 2,4 | 2C.2, 4A.1 |
| §13 версионирование | 3 | 3C |
| §14 отложенное | 4 | 4D |
| I1–I23 (структурно) | 1,2 | 1§6, 2, 4A |
| I3, I3b, I12, I14 | 1,3,4 | 1§2.1/2.3, 1§6 (unit), 4C |

**Структурно vs тест (K7).** Инварианты I1/I2/I2b/I4/I6/I7/I13 обеспечиваются структурно
(`domain/invariants.py` + SQL-триггеры) и проверяются `tests/selfcheck.py` (+ Р-2); I3/I3b/I12/I14,
I16/I17/I20/I21/I23 — юнит-тестами по слоям; I18/I19 — структурно (порт без `tools`, `content_hash`)
плюс selfcheck/грепп. Формулировка «selfcheck покрывает I1–I23» неверна (см. 01 §6).

---

## G. Готовность к реализации (Definition of Ready)

Проектирование считается достаточным для старта M0, когда:
1. все итерационные документы (`00`–`04`) приняты и прошли ревью-гейт @oracle — ✅ **выполнено**
   (2026-10-08; правки K1–K7/S1–S13 интегрированы). Дополнительно: `05-dependency-stack.md`
   (стек/пины) и `06-implementation-strategy.md` (стратегия/план).
2. контракты портов стабильны (итерация 1) — ✅ (с оговоркой о дрейфе `ExtractorPort`, 01 §3.5).
3. ADR зафиксированы: **0001–0023 — accepted** (0012 — план расщепления, реализация в M0/W1;
   0013–0017 — следствия ревью-гейта, уже в регламенте/схемах/DDL; 0021–0023 — операционный
   слой после M1: двухуровневая идентичность сборки, память экстракции, гейты-интеграции;
   у 0023 производитель аттестации профиля — design-only).
4. открытые вопросы ниже закрыты или явно отложены.

**Открытые вопросы (закрыть до/в ходе M0):**
- формат импорт-графа CI (статический анализ импортов `core.*`);
- `ProtocolExhaustion`/`AlignmentReport` — **контракт**, а не отложено: структура фиксируется в
  итерации 1/M0 (§2.2, §5);
- версионирование prompt-шаблонов (по файлам? по hash?);
- retention/GC индексных сборок: старые сборки не удаляются (§13) — нужен явный лимит/политика;
- стратегия бэкапа/восстановления прогона;
- публикация и версии отчётов (`reports/`): адресация, признак «не публикация»;
- стоимость/токены как метрика M1 (`llm_calls`, `tokens`, §B.2);
- политика ретраев/backoff для внешних вызовов.

**Проверки стека V1–V5 (`05-dependency-stack.md` §6):** V1 (RU/EN tantivy) — ✅ закрыт;
V2 (torch/ST под py3.14) — ✅ закрыт; V3 (выбор эмбеддинга, приоритет RU) — решение bge-m3,
финал на R9/R10; V4 (офлайн-веса docling + лицензии) — план подтверждён, реализация в M0;
V5 (scrapy-playwright) — ✅ закрыт smoke-тестом.

Отдельная корпусная БД — **отвергнута** (S6): единая БД сохраняет FK (см. 03 B.2, 04 D).
