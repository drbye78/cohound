# AGENTS.md — coHound

Инструкции для агентов и разработчиков, работающих в этом репозитории.

## Что это

**coHound** — методология и инструмент построения **сравнительной технической карты
программных продуктов** по публично доказуемой функциональности. Не «истина о продукте»,
а воспроизводимая карта подтверждаемости по фиксированному протоколу.

**Главный документ — `cohound-spec-v2.md`** (регламент методологии + технический проект,
v2.1). Это источник истины по семантике, инвариантам и архитектуре. Перед любой
содержательной правкой — читай соответствующий раздел регламента.

> «Паспорт» в этом проекте — только **паспорт продукта** (`passports/{product_id}/`),
> а не сам регламент.

## Аксиомы (§0 регламента) — не нарушать

- **A1.** Сначала доказательство ценности минимальными средствами, потом оптимизация.
- **A2.** Moat — методология измерения, не AI-пайплайн. Если реализация требует изменить
  методологию — это ошибка реализации.
- **A3.** Эпистемология единообразна: дефолты — это assumption/candidate; замена дефолта
  происходит по измерению, не по предвидению.

## Структура

```
cohound-spec-v2.md            # регламент v2.1 (источник истины)
docs/status-dictionary.yaml   # словарь статусов/перечислений + Р-2 (§4.1)
core/                         # ядро: категорийно-НЕЙТРАЛЬНЫЙ код (грепп-тест §1.3)
  domain/                     # enums, models, invariants, scoring, r2, anchors, dictionary — чистый Python, без I/O
  ports/                      # Protocol-контракты: LLM, индексы, embedder, index_builder, search,
                              #   crawler, extractor, chunker, registry, store, gate_store, gateway, types
  store/                      # SQLite DDL §4.4 + триггеры (sqlite.py); StorePort/GateStore-реализация
                              #   (sqlite_store.py); backup.py — бэкап/восстановление прогона
  pipeline/                   # сценарии Ш1–Ш9, гейты G1–G4, эксперименты; авторство пакета:
                              #   package_{io,admin,discovery,candidates,scout}.py, passport_scout.py,
                              #   scout_common.py
  config/                     # загрузка run.yaml/пакетов, deploy.yaml (профиль зон), раскладка путей
                              #   (paths.py), passports.py
  cli/                        # composition root Z0 (ядро): main.py (тонкий вход + реэкспорт),
                              #   parser.py (build_parser), context.py (общие DI/ГЭП-хелперы),
                              #   errors.py (EXIT_*/CliError), output.py (--json/emit_*),
                              #   wiring.py (сборка build_* адаптеров), doctor.py, __main__.py;
                              #   команды — commands/* (реестр COMMANDS), подкоманды hound package —
                              #   package/* (add_package_parser, cmd_package_*); package_commands.py —
                              #   re-export-шим (обратная совместимость)
  edge/                       # composition root Z1/DMZ — веб-адаптеры за швом (07): server.py, auth.py,
                              #   audit.py, seam_schema.py
  adapters/                   # chunker_markdown (heading_path), extract_document (docling: pdf+office),
                              #   lexical_bm25, index_npy/index_format/lexical_tantivy, search_* — см. NOTICES
  util/                       # text_utils (RU/EN), security (SSRF/slug), fusion (RRF), retrieval_eval — см. NOTICES
scripts/ci.sh                 # единая точка входа CI (§11.6): selfcheck + pytest + порог покрытия
tests/selfcheck.py            # self-contained проверка фундамента (87 проверок)
tests/test_selfcheck.py       # pytest-обёртка над selfcheck (W1/T1.5)
tests/domain/                 # property-тесты домена (hypothesis)
tests/smoke_scrapy_playwright.py  # браузерный smoke scrapy-playwright (V5; 0/1/2)
packages/{case_id}/           # пакеты категорий: package.yaml/catalog.csv(+.lock)/
                              #   baselines.csv/cohort.yaml/golden.csv/domain_profiles.yaml/
                              #   scope_attestation.yaml/run.yaml; sources/ — авторские входы
registries/ · docs/           # словари, регламент, absence-правила
contracts/seam/               # публикуемые JSON-схемы шва DMZ (/v1/*; 07 §5/§8)
passports/{product_id}/       # паспорта продуктов
runs/{run_id}/                # прогоны: SQLite, логи, metrics.json, манифесты, plan_inputs.json
index_store/{package}/{build}/{product_id}/  # иммутабельные индексные сборки (продукт-партиции)
corpus_raw/{product_id}/      # retention сырых байтов
cache/serp/{query_hash}.json · cache/http/{url_hash}.bin  # глобальный content-addressed кэш (R7)
config/paths.example.yaml     # пример раскладки операционных каталогов (COHOUND_HOME/COHOUND_*_DIR)
```

**Доверенные зоны (docs/architecture/07-deployment-topology.md).** Веб-функции (краул, поиск,
реестры, экстракция недоверенного контента) выносятся в **DMZ** (`edge/`), не-веб — в ядро без
веб-эгрессии; связь — только через application-layer firewall по протоколу `/v1/*`. Профиль `split`
(DMZ) — **опция уже в MVP** (M1, поток W13); `local` (один процесс) — дефолт. Это усиливает I18 (у
оценщика нет маршрута в интернет) и делает явной границу I19. Контракты шва проектируются
**прозрачными для корпоративных IDP/DLP/IDS** (L7-JSON, публикуемые схемы, без pinning, OIDC/mTLS);
аттестация против конкретных средств — вне рамок проекта.

## Команды

```bash
# Проверка фундамента (без pytest)
python3 tests/selfcheck.py

# CI-гейты (selfcheck + pytest + порог покрытия ядра) — провайдер-нейтрально
bash scripts/ci.sh

# pytest (dev-окружение): обёртка над selfcheck + property-тесты домена
uv pip install -e '.[dev]'           # или воспроизводимо из лока: uv sync --frozen --extra dev
python3 -m pytest -q

# Грепп-нейтральность ядра (§1.3) — предметных терминов в core/ быть не должно
grep -rniE "postgres|pangolin|sber|сбер|copywala|платформа" core/

# после расщепления core/ — нейтральны именно domain/ports/pipeline:
grep -rniE "postgres|pangolin|sber|сбер|copywala|платформа" core/domain core/ports core/pipeline

# Проверка схемы БД
python3 -c "import sys; sys.path.insert(0,'.'); from core.store.sqlite import connect; c=connect(':memory:')"

# Smoke-тест scrapy-playwright (V5) — отдельное окружение с браузерными бинарями:
# pip install 'scrapy>=2.19' 'scrapy-playwright>=0.0.48' 'playwright>=1.63'
# playwright install chromium firefox
# python3 tests/smoke_scrapy_playwright.py   # 0 — ok, 1 — fail, 2 — skip (нет deps)
```

Требуется Python **3.14** (пин-базлайн проекта; greenfield, контролируемое развёртывание, все
зависимости стека имеют cp314/abi3-колёса — `docs/architecture/05-dependency-stack.md` §1;
актуальный стабильный CPython на 2026-10 — 3.15, апгрейд — отдельное решение),
`pydantic>=2`, `pyyaml`.
Dev/CI по §11.6: `pytest`+`hypothesis`+`pytest-cov` в extra `dev`; единая точка входа — `scripts/ci.sh`.
Выбор внешних пакетов, лицензии и запреты — `docs/architecture/05-dependency-stack.md`
(реестр решений; LangChain/LlamaIndex и все hosted-парсеры отклонены).
Стратегия и детальный план реализации (потоки W1–W13, майлстоуны, DoD) —
`docs/architecture/06-implementation-strategy.md`.
Развёртывание: доверенные зоны и DMZ-шов (application-layer firewall) —
`docs/architecture/07-deployment-topology.md`.
Переменные окружения и внешние ключи (LLM, scout, поисковые адаптеры) + runbook живого
прогона корпуса — шаблон без секретов `.env.example` и `docs/architecture/03-data-config-cli.md` §D.6.

## Конвенции кода

- **Категорийная нейтральность `core/`** — обязательна. Никаких названий продуктов,
  вендоров и предметных терминов. Вся специфика — данные в `packages/`. Нарушение
  блокирует кейс (§11.4).
- **Инварианты — это код, а не комментарии.** Инварианты §2.3 (I1–I23) должны
  обеспечиваться валидаторами Pydantic (`core/domain/`) и SQL-триггерами
  (`core/store/sqlite.py`). Каждая новая предметная сущность — с проверкой своих инвариантов
  и записью в selfcheck.
- **Строгая типизация.** Pydantic-модели с `extra="forbid"`. Используй enum-классы
  (`Status`, `Origin`, …), а не строковые литералы.
- **`StrEnum`** для всех перечислений; источник истины — `docs/status-dictionary.yaml`,
  схемы — его зеркало. Это относится и к новым перечислениям: `ranking_policy`, `gate`,
  `capability_policy`, `unknown_policy` (`docs/status-dictionary.yaml` — единственный источник).
- **Скопированный сторонний код** — только с сохранением лицензии/copyright в шапке файла.
  Переносим **копированием**, а не зависимостью на внешний проект.
  Не копировать код без явной лицензии.
- Комментарии и докстринги — на русском (как в регламенте), идентификаторы — на английском.

## Инварианты, которые легко сломать (кратко)

- **I1** evaluated ✅/⚠️ → evidence ≥ 1.
- **I2/I2b** evaluated ❌ → absence-evidence; для `parameter_reference`/`distribution_absence`
  нужен `source_scope: exhaustive` **и** `scope_attested_by`. Для `registry_absence` —
  exhaustive по построению. Для `explicit` — `source_scope` не требуется.
- **I3** strict-`❓` → **полный** протокол исчерпания (`corpus`+`serp`+`registry`+`inheritance`,
  `check_unknown_exhaustion`) **или** `reviewed_by ≠ null`. Неполный протокол — **аномалия** отчёта,
  не смена статуса (`❓` остаётся `❓`, I12).
- **I4** failed → `status = null`, **не статус никогда**.
- **I5** failed **входит** в знаменатель Coverage. Universe-режим (§6): знаменатель — `Σ w(scope)`
  (`scope_universe` = scope-параметры × продукты когорты), `pending`/отсутствующая ячейка — как
  неразрешённая. `preliminary` (I13) и `N/A` (I7) — **вне** знаменателя; `universe=0` → Coverage
  `defined=False`/UNDEFINED, Capability не публикуется (I11/G4).
- **I6/I7** N/A — только человеком (`reviewed_by`).
- **I13** preliminary исключён из всех расчётов; `status` у него null.
- **I17** ретривал продукт-скоупед; evidence — только чанки паспорта и его сателлитов.
- **I18** **оценщик (evaluator) не вызывает внешний поиск** ни при каком провайдере.
- **I19** внешний контент становится evidence только после corpus_raw → hash → index.
- **I20** golden cut-off = pipeline `retrieval_cutoff_K`.
- **I21** маршрутизация модель→сценарий детерминирована фазой, а не решением LLM.

## LLM-контуры (§7) — ключевое разделение

- **evaluator** (Ш6): модели через [OI]-совместимый API **внутри корпоративного периметра**
  (внешние LLM-хосты/ключи не используются), **без веб-тула**, работает только по переданным
  чанкам; строгий JSON через constrained decoding.
- **search adapter** (Ш5): внешний поиск как источник контента, нормализуется в корпус.
- **discovery / concept scout**: поиск допустим, выход — кандидаты/гипотезы (G2).

Не встраивать веб-поиск в оценщик — это ломает evidence-модель и воспроизводимость.

## Git-конвенции

- Осмысленные атомарные коммиты; сообщение в повелительном наклонении, с префиксом
  области при необходимости: `core: ...`, `docs: ...`, `spec: ...`, `tests: ...`.
- Секреты не коммитить. `runs/`, `index_store/`, `corpus_raw/`, `__pycache__/` — в `.gitignore`.
- Slug (`param_id`, `product_id`) иммутабельны; отмена — `deprecated` + `replaced_by` (§13).
- Смена модели под сценарий или смена когорты — **версионная граница класса
  `execution_versions`** (условия измерения, commodity-слой по A2), а **не**
  `methodology_version`: блокирует межцикловый diff с пометкой `execution_change`,
  обязателен `golden-full` (§13, §7.4).
- Смена **семантики правил** (словарь статусов, scoring-политика, absence/сателлитная/
  evidence-модель, определения Coverage/Comparability/sensitivity) — версионная граница
  класса `methodology_version`: `methodology_change`, обязательны `golden-full` **и
  пере-аннотация golden** (§13).

## Чего НЕ делать

- Не добавлять предметную терминологию в `core/`.
- Не делать failed статусом; не выводить N/A автоматически.
- Не придавать якорю вес/влияние на статусы (только порядок/фокус, anchor-blind).
- Не реализовывать «LLM с веб-поиском» внутри оценщика.
- Не придавать запрошенному `ranking_policy` силу переопределять sensitivity-демотирование
  (запрошено ранжирование, вычислено демотирование — допустимое расхождение, `requested` vs `computed`).
- Не мутировать иммутабельные индексные сборки: новый раунд/SERP → **новая** сборка; правка на месте запрещена.
