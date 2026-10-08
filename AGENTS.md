# AGENTS.md — coHound

Инструкции для агентов и разработчиков, работающих в этом репозитории.

## Что это

**coHound** — методология и инструмент построения **сравнительной технической карты
программных продуктов** по публично доказуемой функциональности. Не «истина о продукте»,
а воспроизводимая карта подтверждаемости по фиксированному протоколу.

**Главный документ — `cohound-spec-v2.md`** (регламент методологии + технический проект,
v2.0). Это источник истины по семантике, инвариантам и архитектуре. Перед любой
содержательной правкой — читай соответствующий раздел регламента.

> `thesis_2.md` — предыдущая редакция (v1.3), оставлена для истории. **Не редактировать.**
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
cohound-spec-v2.md            # регламент v2.0 (источник истины)
THIRD_PARTY_NOTICES.md        # лицензии/происхождение скопированного кода
docs/status-dictionary.yaml   # словарь статусов/перечислений + Р-2 (§4.1)
core/                         # ядро: категорийно-НЕЙТРАЛЬНЫЙ код (грепп-тест §1.3)
  schemas.py                  # (текущий монолит) Pydantic-схемы §4.2/4.3/4.5/7 + инварианты
  db.py                       # (текущий) SQLite DDL §4.4 + триггеры инвариантов
  util/                       # text_utils (RU/EN), security (SSRF/slug), fusion (RRF) — см. NOTICES
  adapters/                   # chunker_markdown (heading_path), extract_document (docling: pdf+office), lexical_bm25 — см. NOTICES
  # целевое расщепление (docs/architecture/01-modules-and-contracts.md):
  domain/                     # модели, enums, invariants, scoring, r2, anchors (чистый Python, без I/O)
  ports/                      # Protocol-интерфейсы: LLM, индексы, embedder, index_builder, search,
                              #   crawler, extractor, chunker, registry, store, gate_store
  pipeline/                   # сценарии Ш1–Ш9, гейты G1–G4, эксперименты (use-cases)
  store/                      # SQLite-хранилище + paths
  config/                     # загрузка run.yaml/пакетов
  cli/                        # единственная точка сборки зависимостей (wiring)
tests/selfcheck.py            # self-contained проверка фундамента (33 проверки)
packages/{case_id}/           # (планируется) пакеты категорий: package/catalog/baselines/cohort/golden
registries/ · docs/           # словари, регламент, absence-правила
passports/{product_id}/       # (планируется) паспорта продуктов
runs/{run_id}/                # (планируется) прогоны: SQLite, логи, промпты, манифесты
index_store/{package}/{build}/{product_id}/  # иммутабельные индексные сборки (продукт-партиции)
corpus_raw/{product_id}/      # retention сырых байтов
cache/serp/{query_hash}.json · cache/http/{url_hash}.bin  # глобальный content-addressed кэш (R7)
```

## Команды

```bash
# Проверка фундамента (без pytest)
python3 tests/selfcheck.py

# Грепп-нейтральность ядра (§1.3) — предметных терминов в core/ быть не должно
grep -rniE "postgres|pangolin|sber|сбер|copywala|платформа" core/

# после расщепления core/ — нейтральны именно domain/ports/pipeline:
grep -rniE "postgres|pangolin|sber|сбер|copywala|платформа" core/domain core/ports core/pipeline

# Проверка схемы БД
python3 -c "import sys; sys.path.insert(0,'.'); from core.db import connect; c=connect(':memory:')"

# Smoke-тест scrapy-playwright (V5) — отдельное окружение с браузерными бинарями:
# pip install 'scrapy>=2.19' 'scrapy-playwright>=0.0.48' 'playwright>=1.63'
# playwright install chromium firefox
# python3 tests/smoke_scrapy_playwright.py   # 0 — ok, 1 — fail, 2 — skip (нет deps)
```

Требуется Python 3.11+ (в окружении — 3.14), `pydantic>=2`, `pyyaml`.
`pytest` пока не установлен — для CI по §11.6 его нужно добавить.
Выбор внешних пакетов, лицензии и запреты — `docs/architecture/05-dependency-stack.md`
(реестр решений; LangChain/LlamaIndex и все hosted-парсеры отклонены).
Стратегия и детальный план реализации (потоки W1–W12, майлстоуны, DoD) —
`docs/architecture/06-implementation-strategy.md`.

## Конвенции кода

- **Категорийная нейтральность `core/`** — обязательна. Никаких названий продуктов,
  вендоров и предметных терминов. Вся специфика — данные в `packages/`. Нарушение
  блокирует кейс (§11.4).
- **Инварианты — это код, а не комментарии.** Инварианты §2.3 (I1–I23) должны
  обеспечиваться валидаторами Pydantic (`core/schemas.py`) и SQL-триггерами (`core/db.py`).
  Каждая новая предметная сущность — с проверкой своих инвариантов и записью в selfcheck.
- **Строгая типизация.** Pydantic-модели с `extra="forbid"`. Используй enum-классы
  (`Status`, `Origin`, …), а не строковые литералы.
- **`StrEnum`** для всех перечислений; источник истины — `docs/status-dictionary.yaml`,
  схемы — его зеркало. Это относится и к новым перечислениям: `ranking_policy`, `gate`,
  `capability_policy`, `unknown_policy` (`docs/status-dictionary.yaml` — единственный источник).
- **Скопированный сторонний код** — только с записью в `THIRD_PARTY_NOTICES.md` и сохранением
  лицензии/copyright. Переносим **копированием**, а не зависимостью на внешний проект.
  Не копировать код без явной лицензии (напр. из `../gtm` — права не определены).
- Комментарии и докстринги — на русском (как в регламенте), идентификаторы — на английском.

## Инварианты, которые легко сломать (кратко)

- **I1** evaluated ✅/⚠️ → evidence ≥ 1.
- **I2/I2b** evaluated ❌ → absence-evidence; для `parameter_reference`/`distribution_absence`
  нужен `source_scope: exhaustive` **и** `scope_attested_by`. Для `registry_absence` —
  exhaustive по построению. Для `explicit` — `source_scope` не требуется.
- **I4** failed → `status = null`, **не статус никогда**.
- **I5** failed **входит** в знаменатель Coverage.
- **I6/I7** N/A — только человеком (`reviewed_by`).
- **I13** preliminary исключён из всех расчётов; `status` у него null.
- **I17** ретривал продукт-скоупед; evidence — только чанки паспорта и его сателлитов.
- **I18** **оценщик (evaluator) не вызывает внешний поиск** ни при каком провайдере.
- **I19** внешний контент становится evidence только после corpus_raw → hash → index.
- **I20** golden cut-off = pipeline `retrieval_cutoff_K`.
- **I21** маршрутизация модель→сценарий детерминирована фазой, а не решением LLM.

## LLM-контуры (§7) — ключевое разделение

- **evaluator** (Ш6): self-host, РФ-контур, **без веб-тула**, работает только по переданным
  чанкам; строгий JSON через constrained decoding.
- **search adapter** (Ш5): внешний поиск как источник контента, нормализуется в корпус.
- **discovery / concept scout**: поиск допустим, выход — кандидаты/гипотезы (G2).

Не встраивать веб-поиск в оценщик — это ломает evidence-модель и воспроизводимость.

## Git-конвенции

- Осмысленные атомарные коммиты; сообщение в повелительном наклонении, с префиксом
  области при необходимости: `core: ...`, `docs: ...`, `spec: ...`, `tests: ...`.
- Секреты не коммитить. `runs/`, `index_store/`, `corpus_raw/`, `__pycache__/` — в `.gitignore`.
- Slug (`param_id`, `product_id`) иммутабельны; отмена — `deprecated` + `replaced_by` (§13).
- Смена модели под сценарий или смена когорты — **версионная граница**: bump
  `methodology_version`, обязателен `golden-full` (§13).

## Чего НЕ делать

- Не редактировать `thesis_2.md`.
- Не добавлять предметную терминологию в `core/`.
- Не делать failed статусом; не выводить N/A автоматически.
- Не придавать якорю вес/влияние на статусы (только порядок/фокус, anchor-blind).
- Не реализовывать «LLM с веб-поиском» внутри оценщика.
- Не придавать запрошенному `ranking_policy` силу переопределять sensitivity-демотирование
  (запрошено ранжирование, вычислено демотирование — допустимое расхождение, `requested` vs `computed`).
- Не мутировать иммутабельные индексные сборки: новый раунд/SERP → **новая** сборка; правка на месте запрещена.
