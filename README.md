# coHound

**coHound** — методология и инструмент построения **сравнительной технической карты
программных продуктов** по публично доказуемой функциональности. Это не «истина о
продукте», а воспроизводимая карта **подтверждаемости** по фиксированному протоколу:
каждая ячейка статуса привязана к evidence, а отсутствие функциональности доказывается
absence-evidence по формальным правилам.

**Источник истины по семантике, инвариантам и архитектуре** — регламент
[`cohound-spec-v2.md`](cohound-spec-v2.md) (v2.1). Архитектурный проект — в
[`docs/architecture/`](docs/architecture/).

## Аксиомы (§0 регламента)

- **A1.** Сначала доказательство ценности минимальными средствами, потом оптимизация.
- **A2.** Moat — методология измерения, а не AI-пайплайн. Если реализация требует изменить
  методологию — это ошибка реализации.
- **A3.** Эпистемология единообразна: дефолты — это assumption/candidate; замена дефолта
  происходит по измерению, не по предвидению.

Следствия: категорийная нейтральность ядра (`core/` не содержит названий продуктов и
вендоров — вся специфика в данных `packages/`); `failed` — состояние ячейки, а не статус;
гейты G1–G4 — только человеческие решения; оценщик не вызывает внешний поиск.

## Установка

Требуется Python **3.14** (базлайн; все зависимости стека имеют cp314/abi3-колёса —
`docs/architecture/05-dependency-stack.md`), а также `uv`.

```bash
# Воспроизводимо из лока (рекомендуется):
uv sync --frozen --extra dev

# Либо в текущее окружение:
uv pip install -e '.[dev]'
```

## Быстрый старт

```bash
./hound.py --help          # список команд
./hound.py doctor          # самодиагностика инсталляции (read-only, без сети)
python3 tests/selfcheck.py # автономная проверка фундамента
bash scripts/ci.sh         # CI-гейты: selfcheck + pytest + порог покрытия ядра
```

## Три главные команды

```bash
# 1. resolve — нормализовать входы и пролинтить пакет (без записи)
./hound.py resolve --run-yaml packages/{case_id}/run.yaml --package packages/{case_id}

# 2. plan — построить план прогона и когорту (черновик run.yaml, cohort.yaml)
./hound.py plan --run-yaml packages/{case_id}/run.yaml --package packages/{case_id}

# 3. run — конвейер Ш1–Ш9 (останавливается на человеческих гейтах G1–G4)
./hound.py run --run-yaml packages/{case_id}/run.yaml --package packages/{case_id}
```

Остальная CLI-поверхность (`crawl`, `diff`, `sensitivity`, `golden`, `report`, `discover`,
`passport-scout`, `review`, `promote`, `gate`, `deploy-check`, `experiment retrieval`,
`baseline-agreement`, `doctor`,
`package …`) описана в `docs/architecture/03-data-config-cli.md` §E.1. **Источник истины по CLI —
код (`core/cli/`); документ — его зеркало.** Актуальный список — `./hound.py --help`.

## Структура репозитория (сжато)

```
cohound-spec-v2.md   # регламент v2.1 — источник истины
AGENTS.md            # инструкции для агентов/разработчиков
core/                # ядро, категорийно-нейтральный код (§1.3)
  domain/            # Pydantic-модели, enums, инварианты, r2, scoring, anchors (без I/O)
  ports/             # Protocol-контракты (LLM, индексы, store, search, gateway, …)
  pipeline/          # сценарии Ш1–Ш9, гейты G1–G4, эксперименты
  adapters/          # реализации портов (индексы, LLM, поиск, краул, экстракция)
  store/             # SQLite DDL + триггеры, StorePort/GateStore-реализация, backup
  config/            # загрузка run.yaml/пакетов, deploy.yaml, раскладка путей
  cli/               # composition root Z0 (ядро) — сборка зависимостей
  edge/              # composition root Z1/DMZ — веб-адаптеры за швом (07)
  util/              # текстовые утилиты (RU/EN), security (SSRF/slug), fusion (RRF)
docs/                # архитектурный проект, регламент, absence-правила
docs/status-dictionary.yaml  # источник истины по перечислениям/статусам (§4.1)
packages/{case_id}/  # пакеты категорий (catalog/baselines/cohort/golden/registries)
passports/{product_id}/  # паспорта продуктов и сателлитов
runs/ · index_store/ · corpus_raw/ · cache/  # операционные каталоги (runtime)
tests/               # selfcheck, property-тесты домена, smoke scrapy-playwright
```

## Архитектура и документация

- [`docs/architecture/00-overview.md`](docs/architecture/00-overview.md) — обзор слоёв и потоков.
- [`01-modules-and-contracts.md`](docs/architecture/01-modules-and-contracts.md) — модули, контракты, матрица зависимостей.
- [`02-subsystems.md`](docs/architecture/02-subsystems.md) — детальный дизайн подсистем.
- [`03-data-config-cli.md`](docs/architecture/03-data-config-cli.md) — данные/хранилища, конфиг, CLI.
- [`04-reliability-adr.md`](docs/architecture/04-reliability-adr.md) — надёжность и ADR-реестр.
- [`05-dependency-stack.md`](docs/architecture/05-dependency-stack.md) — стек, лицензии, пины.
- [`06-implementation-strategy.md`](docs/architecture/06-implementation-strategy.md) — стратегия и статус реализации.
- [`07-deployment-topology.md`](docs/architecture/07-deployment-topology.md) — доверенные зоны и DMZ.

## Доверенные зоны и DMZ (дисклеймер)

Веб-функции (краул, внешний поиск, реестры, экстракция недоверенного контента) выносятся в
**DMZ** (`core/edge/`), не-веб код остаётся в ядре без веб-эгрессии. Связь — только через
application-layer firewall по протоколу `/v1/*`. Профиль `local` (один процесс) — дефолт;
профиль `split` (DMZ) — опция уже в MVP. Оценщик (evaluator) **никогда** не вызывает внешний
поиск: внешний контент становится evidence только после `corpus_raw → hash → index`
(I18/I19). Подробности — `docs/architecture/07-deployment-topology.md`.

> Инструмент даёт воспроизводимую карту **подтверждаемости** по публично доступным
> источникам, а не юридически значимую оценку продукта. Аттестация против конкретных
> корпоративных средств IDP/DLP/IDS — вне рамок проекта.

## Лицензия

Проект распространяется под лицензией **MIT** — см. [`LICENSE`](LICENSE).

Сторонние компоненты, включённые в репозиторий, сохраняют свои лицензии (указаны в шапках
соответствующих файлов).
