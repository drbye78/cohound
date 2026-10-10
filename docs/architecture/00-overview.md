# coHound — архитектура: Итерация 0 — мастер-обзор

**Документ:** `docs/architecture/00-overview.md`
**Статус:** принято (ревью-гейт @oracle пройден 2026-10-08; правки K1–K7/S1–S13 интегрированы)
**Источник истины:** `cohound-spec-v2.md` (регламент v2.0). При расхождении — регламент главнее;
этот документ не вводит новой методологии, только её архитектурную развёртку.
**Смежное:** `05-dependency-stack.md` (реестр пакетов/лицензий/пинов), `06-implementation-strategy.md`
(стратегия и план реализации), `07-deployment-topology.md` (доверенные зоны/DMZ).

---

## 0. Назначение и границы проектирования

Проектируем **программную систему**, реализующую регламент coHound: CLI-инструмент, который по
четырём входам (якорь, каталог, когорта, фокус) проходит конвейер Ш1–Ш9, порождает пакеты
прогонов (`runs/{run_id}/`) и отчётные артефакты, обеспечивая инварианты I1–I23.

**В рамках этого документа:** слои, компоненты, зависимости, сквозные механизмы, план итераций.
**Вне рамок:** конкретные алгоритмы ретривала/чанкинга (итерация 2), схема CLI-флагов (итерация 3),
содержание ADR (итерация 4). **Выбор библиотек вынесен в `05-dependency-stack.md`** (снимок
версий/лицензий, проверки совместимости V1–V5).

---

## 1. Драйверы архитектуры (что диктует форму)

| # | Драйвер (из регламента) | Архитектурное следствие |
|---|---|---|
| D1 | **A2:** moat — методология, commodity — заменимо | Порты/адаптеры (hexagonal): LLM, ретривал, хранилище, поиск — за интерфейсами; ядро не знает технологий |
| D2 | **§1.3:** категорийная нейтральность `core/` | `core/` без предметных терминов; вся специфика — данные в `packages/{case_id}/`; CI-грепп-тест |
| D3 | **I18/I19:** оценщик без поиска; внешний контент только через корпус | Жёсткое разделение контуров *evaluator* и *search-adapter*; единственный вход контента — Ш5 |
| D4 | **I10/I15:** LLM не генерирует URL | Evidence всегда ссылается на `source_id`; резолв URL — детерминированный, в ядре |
| D5 | **I16/I20:** симметрия ретривала; golden cut-off = pipeline-K | Ретривал-профиль как первоклассная конфигурация прогона; `retrieval_cutoff_K` в манифесте |
| D6 | **I17:** ретривал продукт-скоупед | Индекс шардируется по продукту; оценка ищет только в партиции продукта+сателлитов |
| D7 | **§13:** иммутабельные сборки, версионные границы | `index_store/{package}/{build}/{product_id}/` только на запись; смена версий инвалидирует diff |
| D8 | **R7 / A3:** воспроизводимость и измеряемость дефолтов | Всё, что влияет на результат, пинится в `run_manifest.yaml`; эксперименты R0/R1/R2 — измерительный слой |
| D9 | **§1.4:** одной политикой, два режима | Режим (`strict`/`preliminary`) — параметр прогона, а не ветка кода |
| D10 | **A1:** минимальные средства | Стек дефолтов прост (SQLite, .npy, in-process); тяжёлое — по триггерам §15 |
| D11 | **§10:** эшелонированная защита | Валидация как отдельный слой-конвейер, независимый от LLM |

---

## 2. Контекст системы (C4 уровень 1)

```mermaid
flowchart LR
    Analyst([Аналитик]) -->|CLI, ревью| SYS[coHound]
    Customer([Заказчик]) -->|отчёты| SYS
    SYS -->|краул, SERP| Web[(Публичные источники / интернет)]
    SYS -->|реестровые запросы| Reg[(Реестры: РРПО, ФСТЭК, ...)]
    SYS -->|инференс, без поиска| LLM[(Self-host LLM-контур РФ)]
    SYS -->|search grounding| SRCH[(Search-адаптер: Yandex/GLM)]
    SYS -->|артефакты| Out[(runs/, reports/, index_store/)]
```

**Акторы:** аналитик (управляет прогоном, проходит G1–G4), заказчик (потребляет `battle_card`).
**Внешние системы:** публичный веб, реестры, LLM-инференс (self-host), search-адаптер.
**Границы данных:** корпус не покидает контур (оценщик self-host); search-адаптер — единственный
внешний канал поисковых запросов, логируемый (`search_log`).

**Доверенные зоны (см. `07-deployment-topology.md`).** Система разворачивается в двух сетевых
зонах: **Z0 CORE** (доверенная, без веб-эгрессии: оркестрация, домен, store, ретривал, оценщик,
расчёт, отчёты) и **Z1 EDGE/DMZ** (веб-эгрессия: краул, поиск, реестры, экстракция недоверенного
контента). Взаимодействие — только через **application-layer firewall (L7)** по протоколу
`/v1/fetch|search|registry`. Это усиливает **I18** (оценщик физически без маршрута в интернет) и
даёт явную границу для **I19**. Профиль `local` (один процесс) — дефолт для разработки (A1);
`split` (DMZ) — **опция уже в MVP** (M1, W13; `06`). Контракты шва — прозрачны для корпоративных
IDP/DLP/IDS (`07` §8).

---

## 3. Слои и правило зависимостей (C4 уровень 2)

Четыре слоя, зависимости — строго **внутрь** (к домену). Ядро не импортирует адаптеры; адаптеры
и CLI зависят от ядра.

```
┌──────────────────────────────────────────────────────────────┐
│ L4. Интерфейс        cli/                                     │  CLI-команды, сборка зависимостей (wiring)
├──────────────────────────────────────────────────────────────┤
│ L3. Адаптеры         adapters/ · store/                       │  .npy, tantivy, vLLM/SGLang, Yandex/GLM, Scrapy, docling, SQLite
│      (реализуют порты)                                        │
├──────────────────────────────────────────────────────────────┤
│ L2. Порты            ports/                                   │  Protocol-интерфейсы (LLM, VectorIndex, Lexical, Search, Fetcher, Crawler, Extractor, Chunker, Embedder, IndexBuilder, Store, GateStore)
├──────────────────────────────────────────────────────────────┤
│ L1. Сценарии         pipeline/                                │  Ш1–Ш9, гейты G1–G4, эксперименты
│      (use-cases)                                              │
├──────────────────────────────────────────────────────────────┤
│ L0. Домен            domain/                                  │  Модель, инварианты I1–I23, scoring, R-2 (чистый Python, без I/O)
└──────────────────────────────────────────────────────────────┘
            зависит от ──▲ (внутрь), никогда ▼
```

> `reports/` — не архитектурный слой, а каталог **выходных артефактов**; его писатель —
> L1-сценарий `pipeline/report.py` (openpyxl). В слое L4 живёт только `cli/`. Это снимает
> кажущееся противоречие «`reports/` в L4» vs «`pipeline/report.py` в L1».
>
> **Два composition root (07):** `cli/` собирает ядро (Z0), `edge/` собирает веб-адаптеры (Z1/DMZ).
> Оба — «корни сборки» и могут зависеть от `ports`+`adapters`; ни один не содержит домена. В
> профиле `local` `cli/` собирает всё in-process; в `split` ядро использует remote-прокси, а
> `edge/` публикует реальные адаптеры через gateway.

**Правило:** `domain` ← `pipeline` ← `ports`/`adapters` ← `cli`. `ports` может зависеть от `domain`.
Обратных зависимостей нет. `packages/` (данные) не импортируются кодом как Python — читаются через
порты загрузчиков (YAML/CSV).

---

## 4. Карта компонентов (C4 уровень 3)

### 4.1. Домен (L0)

| Компонент | Ответственность | Владеет инвариантами |
|---|---|---|
| `domain/enums.py` | Зеркало `docs/status-dictionary.yaml` (StrEnum) | — |
| `domain/models.py` | Cell, Evidence, Satellite, Passport, Package, Cohort, GoldenRow | Структурные |
| `domain/invariants.py` | Проверки I1–I23 для ячеек/evidence/сателлитов | I1–I14, I17–I19, I22 |
| `domain/scoring.py` | Resolved Capability, Coverage, failure_rate, Comparability, sensitivity | I5, I11, I13 |
| `domain/r2.py` | Маппинг сателлита на зачёт (§4.1) | Р-2 |
| `domain/anchors.py` | Stable slug, версионирование, `source_id`→URL резолв | I10, I15 |

`domain/anchors.resolve_source_url` не импортирует `ports/`: reader-протокол **`SourceResolver`**
объявляется внутри `domain/` (структурная типизация), конкретная реализация приходит из `store/`.
Так соблюдается матрица зависимостей (L0 не зависит от L2).

### 4.2. Сценарии (L1) — конвейер

| Компонент | Фаза | Выход | Гейт |
|---|---|---|---|
| `pipeline/normalize.py` + `lint.py` | Ш1 | План прогона, валидированные входы | G1 |
| `pipeline/discovery.py` | Ш2 | Кандидаты сателлитов/ролей | G2 |
| `pipeline/registry.py` | Ш3 | R-ячейки (`registry_facts`) | — |
| `pipeline/inheritance.py` | Ш4 | Унаследованные ячейки (`direct/conditional/blocked`) | — |
| `pipeline/corpus.py` | Ш5 | Индексная сборка + `content_hash` | — |
| `pipeline/evaluate.py` | Ш6 | Оценённые ячейки + evidence | — |
| `pipeline/validate.py` | Ш6 | Механическая валидация (эшелон §10) | — |
| `pipeline/review.py` | Ш7 | Strict-ревью / preliminary-стол | G3 |
| `pipeline/compute.py` | Ш8 | Capability/Coverage/Comparability | G4 |
| `pipeline/report.py` | Ш9 | Артефакты `battle_card` и др. | — |
| `pipeline/experiments.py` | M1 | R0/R1/R2, retrieval-golden | — |

### 4.3. Порты (L2)

| Порт | Назначение | Гарантия |
|---|---|---|
| `ports/llm.py` | Инференс оценщика (constrained decoding) | **Без веб-тула** (I18) на уровне интерфейса |
| `ports/vector_index.py` | Векторный поиск по продукт-партиции | Продукт-скоупед (I17) |
| `ports/lexical_index.py` | BM25 поиск | Симметрия профиля (I16) |
| `ports/search.py` | Внешний поиск как источник контента | Логирование в `search_log` (I23) |
| `ports/crawler.py` | Краул страниц | Возвращает сырые байты (retention) |
| `ports/extractor.py` | HTML / PDF / Office (docx/pptx/xlsx) → markdown-текст | `extractor_version` |
| `ports/chunker.py` | Текст → чанки с `heading_path` | `chunker_version` |
| `ports/embedder.py` | Текст → вектор (батч; локальная модель) | Модель не покидает контур; `embed_model_id` |
| `ports/index_builder.py` | Запись `.npy`-шардов и tantivy-индекса | `build()`/`commit()`; иммутабельная сборка (I17) |
| `ports/registry.py` | Реестровый факт | `source_url` обязателен (R1) |
| `ports/store.py` | Операционное хранилище | Транзакционность |
| `ports/gate_store.py` | Персистенция человеческих решений G1–G4 | `gate_decisions` — аудит-след (I6/I7) |
| `ports/gateway.py` | Транспорт core↔edge (L7) | Только контент/кандидаты, не evidence (I19); см. 07 |

### 4.4. Адаптeры (L3)

`adapters/vector_npy.py` (NpyVectorIndex, продукт-шарды) · `adapters/lexical_tantivy.py` ·
`adapters/llm_local.py` (vLLM/SGLang) · `adapters/search_yandex.py` · `adapters/search_glm.py` ·
`adapters/crawl_scrapy.py` · `adapters/extract_html.py` (trafilatura) / `adapters/extract_document.py`
(docling: PDF + Office docx/pptx/xlsx, лёгкий путь без torch для Office) · `adapters/chunker_*.py` ·
`store/sqlite.py` (DDL) · **`config/paths.py`** (раскладка путей). Стек/пины/лицензии —
`05-dependency-stack.md`.

**Адаптеры-писатели индекса:** `adapters/index_npy.py` (`IndexBuilderPort`: запись `.npy`-шардов
и манифеста, `build()`/`commit()`; формат — `adapters/index_format.py`) и
`adapters/lexical_tantivy.py` (`LexicalIndexPort`). Векторный поиск — `adapters/vector_npy.py`
(`VectorIndexPort`); `adapters/embed_local.py` — `EmbedderPort`. `pipeline` не пишет `.npy`/tantivy
напрямую. Реализация `StorePort`/`GateStore` — `store/sqlite_store.py` поверх DDL `store/sqlite.py`;
бэкап/восстановление прогона — `store/backup.py`.

**Реестровые фетчеры:** `ports/registry.py` — нейтральный порт; конкретные реестры (РРПО, ФСТЭК)
описываются данными в `registries/*.yaml` и обслуживаются **generic config-driven HTTP/JSON/HTML
адаптером** вне `core/`, чтобы не нарушать §1.3.

---

## 5. Поток данных: Ш1–Ш9 и гейты

```mermaid
flowchart TD
    I([4 входа]) --> N[Ш1 normalize+lint] -->|G1| P[plan]
    P --> D[Ш2 discovery] -->|G2| G{продукт-скоупед}
    G --> R[Ш3 registry] --> H[Ш4 inheritance] --> C[Ш5 corpus/index]
    C --> E[Ш6 evaluate · validate]
    E --> RV[Ш7 review] -->|G3| S[Ш8 compute]
    S -->|G4| RP[Ш9 report]
    E -.->|корпус недостаточен| SR[search-адаптер] -.->|I19: через Ш5| C
```

Ключевое: пунктирная петля — единственный легальный путь внешнего поиска. SERP не внутри оценщика,
а как **вход Ш5** (I18/I19). Каждый SERP-раунд порождает **новую иммутабельную сборку**
(`R0-base` → `R1-serp` → `R2-serp` → …), а не мутирует существующую; повторный Ш6 идёт **по новой
сборке**, инвалидируются только затронутые ячейки (`stale_reason='new_build'`), а не вся когорта.

---

## 6. Сквозные механизмы

| Механизм | Реализация | Инварианты |
|---|---|---|
| **Evidence integrity** | `source_id` → `source_url` резолв; цитата-подстрока `text`; `content_hash` | I1, I2, I10, I15, I19 |
| **Версионирование** | Все версии в `run_manifest.yaml`; иммутабельные сборки | §13 |
| **Воспроизводимость** | Пинить веса/движок/профиль/seed/железо; глобальный content-addressed кэш корпуса и SERP (ADR 0016) | R7, I16, I20 |
| **Нейтральность** | Грепп-тест `core/`; данные только в `packages/` | §1.3, §11.4 |
| **Детерминизм маршрутизации** | `scenario → {model_id,...}` таблицей, не решением LLM | I21 |
| **Валидация-эшелон** | Отдельный `pipeline/validate.py`, независимый от LLM | §10 |
| **Изоляция веб-контура** | Профиль `split`: веб-адаптеры в DMZ за L7-швом; ядро без эгрессии | I18, I19, §10 (07) |

---

## 7. Модульная структура (фактическая)

```
core/
  __init__.py
  domain/      enums.py models.py invariants.py scoring.py r2.py anchors.py dictionary.py
  ports/       llm.py vector_index.py lexical_index.py search.py crawler.py
               extractor.py chunker.py embedder.py index_builder.py
               registry.py store.py gate_store.py gateway.py discovery_llm.py types.py
  pipeline/    normalize.py lint.py plan.py discovery.py registry.py
               inheritance.py corpus.py evaluate.py validate.py review.py
               compute.py report.py experiments.py gates.py runner.py
               package_{io,admin,discovery,candidates,scout}.py
               passport_scout.py scout_common.py adversarial.py serp_loop.py golden.py
  adapters/    vector_npy.py index_npy.py index_format.py lexical_tantivy.py
               embed_local.py llm_local.py llm_discovery.py llm_llamacpp.py
               search_yandex.py search_glm.py search_google.py search_duckduckgo.py
               search_openwebsearch.py crawl_scrapy.py crawl_http.py
               extract_html.py extract_document.py chunker_markdown.py
               remote_gateway.py remote_crawler.py remote_search.py remote_registry.py
               remote_extractor.py registry_generic.py gateway_validate.py
  store/       sqlite.py sqlite_store.py backup.py
  config/      run_config.py deploy.py paths.py passports.py
  cli/         main.py parser.py context.py errors.py output.py wiring.py doctor.py __main__.py
               commands/* (реестр COMMANDS) package/* (hound package)            # composition root Z0
  edge/        server.py auth.py audit.py seam_schema.py                    # composition root Z1 (DMZ)
  util/        ids.py hashing.py logging.py cache.py sanitize.py fusion.py
               security.py text_utils.py retrieval_eval.py
packages/{case_id}/, registries/, passports/, runs/, index_store/, corpus_raw/, cache/
```

Монолит `core/schemas.py` расщеплён на `domain/{models,enums,invariants}.py` (T1.2, файл
**удалён**); DDL/триггеры `core/db.py` перенесены в `store/sqlite.py` + `ports/store.py` (T1.3,
файл **удалён**). `edge/` используется только при профиле `split` (07); в `local` не запускается.

---

## 8. План итераций

| Итерация | Содержание | Артефакт | Гейт |
|---|---|---|---|
| **0** | Мастер-обзор (этот документ) | `docs/architecture/00-overview.md` | — |
| **1** | Декомпозиция: модули, порты, контракты интерфейсов, правила зависимостей | `01-modules-and-contracts.md` | — |
| **2** | Детальный дизайн подсистем: конвейер, ретривал/индекс, LLM-контуры, реестры, отчёты | `02-subsystems.md` | — |
| **3** | Данные/хранилища, версионирование, конфигурация, CLI/UX | `03-data-config-cli.md` | — |
| **4** | Надёжность, наблюдаемость, CI, ADR-реестр, сверка с регламентом | `04-reliability-adr.md` | — |
| **R** | Независимое ревью архитектуры @oracle + интеграция | правки | ✅ пройден (2026-10-08) |
| **5** | Реестр зависимостей: пакеты, лицензии, пины, проверки V1–V5 | `05-dependency-stack.md` | — |
| **6** | Стратегия production-grade реализации + детальный план | `06-implementation-strategy.md` | — |
| **7** | Развёртывание: доверенные зоны, DMZ-шов (L7), профили | `07-deployment-topology.md` | — |

### 8.1. Вертикальный срез M0 (что реально в каркасе)

M0 (по §14 регламента) должен давать **сквозной вертикальный срез** на детерминированных волнах;
всё остальное — заглушки за портами.

| Статус | Модули |
|---|---|
| **Реально в M0** | `domain/*` (enums, models, invariants, scoring, r2, anchors); `store/sqlite.py`; `pipeline/normalize.py` + `lint.py` + `plan.py`; `pipeline/gates.py`; CLI `resolve`/`plan`; реестровый generic-адаптер + `registries/*.yaml`; golden-каркас |
| **Заглушки (stub)** | LLM-оценщик (`evaluate`); SERP/`search`-адаптер; crawler; vector/lexical-индексы (и `embedder`/`index_builder`); `edge/`-сервис (не запускается в `local`) |

Заглушки реализуют свои порты и дают детерминированный пустой ответ: конвейер собирается сквозь
них, но контуры Ш5/Ш6 в M0 рабочими не считаются. Сквозной результат M0 — мини-карта из
детерминированных волн (реестры + наследование) на 3 продуктах.

> **Стартовый актив (не greenfield).** К началу M0 уже существовали монолиты `core/schemas.py` / `core/db.py` (схемы + инварианты + DDL/триггеры), позже **расщеплены и удалены** (T1.2/T1.3);
> утилиты (`text_utils`, `security`, `fusion`, `retrieval_eval`) и адаптеры (`chunker_markdown`,
> `extract_document`, `lexical_bm25`), а также `tests/selfcheck.py` (ныне **86** проверок) и
> пройденный браузерный smoke (V5). M0 — это **рефакторинг за порты**, а не написание с нуля.
> Полная инвентаризация и влияние на оценки — `06` §0.

---

## 9. Заготовки решений (ADR seeds) и открытые вопросы

**ADR-реестр (детально — `04-reliability-adr.md` §E):**
- 0001–0011 — accepted: порты/адаптеры (hexagonal) vs монолит; продукт-партиция + NpyVectorIndex;
  LLM без `tools`; контент только через Ш5; единый cut-off K; реестры как данные; SQLite+WAL;
  иммутабельные сборки; кэш внешних вызовов; `failed` как состояние ячейки; человеческие гейты.
- 0012 — accepted и **реализовано** (T1.2/T1.3): расщепление бывших `core/schemas.py`/`core/db.py`
  на слои (`domain/`, `store/`); transitional-шимы удалены.
- 0013–0017 — accepted (ревью-гейт): гранулярность индексной сборки и provenance ячеек; SERP-петля
  vs иммутабельность (append-only раунды); политика ранжирования/демотирования (requested vs
  computed); глобальный content-addressed кэш для R7; контракт derived-evidence наследования/сателлитов.
- **0018 — accepted:** доверенные зоны и DMZ-шов (веб-контур за application-layer firewall; remote-
  адаптеры + `ContentGateway`) — `07-deployment-topology.md`.
- **0019 — accepted:** инспектируемый контракт шва для корпоративных IDP/DLP/IDS (L7-JSON, публикуемые
  схемы, без pinning, OIDC/mTLS) — `07` §8.

**Открытые вопросы (для итераций):**
- Гранулярность транзакций SQLite при массовой записи ячеек (итерация 3).
- Формат и версионирование `prompt_hash` при смене шаблона (итерация 2).
- Механизм кэша SERP/корпуса для R7 — решён как глобальный content-addressed (ADR 0016, итерация 3 A.2).
- Где живёт реестр экспериментов R0/R1/R2 и как связывается с `index_build_id` (итерация 2).

---

## 10. Сверка с регламентом (traceability)

| Раздел регламента | Покрытие в этой итерации |
|---|---|
| §1.3 нейтральность | §3 слои, §7 структура |
| §3.1 компоненты | §4 карта компонентов |
| §3.2 стек | `05-dependency-stack.md` |
| §4.4 БД | §4.4 адаптеры (store), итерация 3 |
| §5 конвейер | §5 поток данных |
| §7 LLM-контуры | §4.3 порт LLM, §5 петля SERP |
| §10 защита | §6 валидация-эшелон; `07` (изоляция веб-контура) |
| §13 версионирование | §6 |
| I1–I23 | §4.1 владение, §6 |
