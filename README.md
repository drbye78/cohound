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
гейт не проходится автоматически никогда — G1/G2 это внутренняя аттестация человеком,
G3/G4 точки интеграции, где решение приходит извне и только записывается; оценщик не
вызывает внешний поиск.

## Установка

Требуется Python **3.14** (базлайн; все зависимости стека имеют cp314/abi3-колёса —
`docs/architecture/05-dependency-stack.md`), а также `uv`.

```bash
# Минимальный набор, чтобы реально прогнать кейс: ядро + ретривал + краул + экстракция.
uv pip install -e '.[dev,retrieval,crawl,extract]'

# Только тесты/CI (без краула и экстракции):
uv pip install -e '.[dev]'
```

Набор extras и то, чего не хватает, показывает `./hound.py doctor`: он печатает группы
`core`/`retrieval`/`crawl`/`extract`/`safety`/`ops`/`embed`/`embed-lite`/`dev` и статус
каждой. Без `retrieval`/`crawl`/`extract` фазы `corpus`/`evaluate` не работают. Контейнеры:
обычный `zip` читается stdlib-ом (`zipfile`), `7z`/`rar` требуют extra `extract-archives`
(`py7zr`/`rarfile`; для `rar` со сжатыми членами нужен внешний `bsdtar`/`unrar`/`7z`).
Без бэкенда контейнер **не разворачивается видимо**: `available()=False`,
`backend_unavailable: … (extra extract-archives)`, `container_reader_unavailable` — а не
тихий успех.

Воспроизводимо из лока (рекомендуется для CI и для повторного развёртывания):

```bash
uv sync --frozen --extra dev --extra retrieval --extra crawl --extra extract
# плюс спуск в 7z/rar, если он нужен кейсу:
uv sync --frozen --extra dev --extra retrieval --extra crawl --extra extract --extra extract-archives
```

`uv lock --check` проходит: лок синхронизирован с `pyproject.toml`, включая
`extract-archives`. Если правите зависимости — обновите лок тем же коммитом, иначе
`--frozen` откажет.

## Быстрый старт

```bash
./hound.py --help          # список команд
./hound.py doctor          # самодиагностика инсталляции (read-only, без сети)
python3 tests/selfcheck.py # автономная проверка фундамента
bash scripts/ci.sh         # CI-гейты: selfcheck + pytest + порог покрытия ядра
```

## Первый прогон: plan → run → gate → report

```bash
# 1. plan — нормализовать входы, пролинтить пакет и напечатать план (stdout/--json).
#    План чисто вычислим и ничего не пишет: черновик run.yaml/cohort.yaml не создаётся.
./hound.py plan --run-yaml packages/{case_id}/run.yaml --package packages/{case_id}

# 2. run — конвейер Ш1–Ш9. --urls — операторский корпусный вход: без него корпус URL пуст
#    (doc_domains паспортов молча не фетчатся). --follow печатает прогресс сеанса краула.
./hound.py run --run-yaml packages/{case_id}/run.yaml --package packages/{case_id} \
  --urls packages/{case_id}/sources/corpus_urls.yaml --follow

# 3. gate — единственный способ записать решение гейта (гейт не проходится сам).
#    Свежий прогон встаёт на g1 (4-я фаза из 15) и возвращает код 3 (EXIT_BLOCKED);
#    на блоке он пишет runs/{run_id}/gates/G1-request.json, откуда берётся subject_digest.
./hound.py gate --runs-dir runs --run-id {run_id} \
  --gate G1 --actor {кто} --target cohort --notes "{почему}"
# если request-файла нет (решение принимается вне прогона) — отпечаток задаётся явно:
#   --subject-digest {digest}

# 4. report — пересобрать артефакты прогона из БД (пишет {out}/{run_id}/outputs/).
./hound.py report --runs-dir runs --run-id {run_id}
```

`run` останавливается на **первом** непройденном гейте, а не «на всех G1–G4 сразу».
Гейты **per-run**: новый `run_id` требует новых решений, старые не переносятся. Дальше
по фазам: `g2` — матрица роль×продукт, `g3`/`g4` — точки интеграции (решение приходит
извне, см. ниже). Продолжить с места остановки: `run … --until {фаза}`; посмотреть
очередь ревью — `./hound.py review --runs-dir runs --run-id {run_id}`, повысить
preliminary — `./hound.py promote --runs-dir runs --run-id {run_id} --actor {кто}`.

### Краул и прогресс

```bash
# Отдельный сеанс краула (фаза corpus) — тем же пакетом и тем же run_id:
./hound.py crawl run --run-yaml packages/{case_id}/run.yaml --package packages/{case_id} \
  --urls packages/{case_id}/sources/corpus_urls.yaml --follow

# Прогресс/счётчики сеанса (read-only); --per-url — построчно по элементам:
./hound.py crawl status --run-id {run_id} --per-url
./hound.py crawl list --runs-dir runs --run-id {run_id}
```

- `crawl resume` продолжает прерванный сеанс: та же `session_id`, уже `FETCHED`-элементы не
  пере-фетчатся (идемпотентно). `crawl restart` очищает сеанс и запускает заново.
- `crawl cancel` переводит сеанс в терминальный `cancelled`. В foreground-профиле `local`
  живого worker'а нет, поэтому это намерение оператора, а не прерывание процесса.
- Обычный `run` уже пройденные фазы не переделывает: он останавливается на первом `blocked`.
- «Почему у продукта ноль источников» читается в `results[].notes` (`--json` прогона), там же
  `product_start`/`product_end`/`product_skipped`. При `--follow` в `--json` печатаются
  NDJSON-события `crawl_progress` (агрегат сеанса) и `crawl_product` (границы продукта:
  агрегат не отличает «ещё в очереди» от «потерян»).

### `resolve` — пост-хок lookup, не входной шаг

```bash
# source_id → URL по БД прогона (без сети). Нужен после прогона, чтобы раскрутить evidence.
./hound.py resolve --db runs/{run_id}/cohound.db {source_id}
```

Промах — не исключение, а код возврата 4 (`EXIT_NOT_FOUND`, код `I10/I15`).

Остальная CLI-поверхность (`crawl`, `diff`, `sensitivity`, `golden`, `report`, `discover`,
`passport-scout`, `review`, `promote`, `gate`, `deploy-check`,
`experiment {retrieval, evaluator-benchmark}`,
`baseline-agreement`, `doctor`,
`package …`) описана в `docs/architecture/03-data-config-cli.md` §E.1. **Источник истины по CLI —
код (`core/cli/`); документ — его зеркало.** Актуальный список — `./hound.py --help`.

Выбор модели оценщика — измерительная задача, а не предрешение (A3):

```bash
./hound.py experiment evaluator-benchmark --runs-dir runs --run-id {run_id} \
  --golden packages/{case_id}/golden.csv --model {model_id}
```

Считает валидность JSON, согласие (Cohen's κ), confusion, abstention, `fabricated_support`,
bootstrap CI и пробу детерминизма (`--repeats`) при фиксированных прочих условиях;
`model_id` берётся **только** из флага, не из env. Смена модели под сценарий при этом
остаётся версионной границей `execution_versions` (§13).

## Форматы и контейнеры

HTML — основной источник. Plain text и markdown (со всеми расширенными вариантами) и EPUB —
**первый класс** наравне с HTML: у них собственный читатель (`extract_text`/`extract_epub`)
и собственный чанкер, а не docling. PDF и Office (docx/pptx/xlsx) идут через docling.
Заявленные контейнеры (`zip`/`7z`/`rar`) раскрываются спуском: **потолка глубины нет** —
конечность дают цикл по хешу содержимого (`archive_cycle`) и кумулятивный бюджет корня
(`archive_max_total_uncompressed_bytes`); исчерпание видно как `archive_budget_exhausted`,
а не как тихое завершение. Оси `archive_max_*` при этом ограничивают **один** контейнер,
поэтому бюджет корня — отдельная ось.

Память извлечения — операционный рычаг: после каждого документа воркер отдаёт ОС свободные
арены glibc (`malloc_trim` через `core/util/memory.py`), иначе RSS растёт храповиком до OOM.

## Гейты: что решается внутри, а что приходит извне

- **G1/G2 — внутренняя аттестация.** Человек утверждает **содержимое профиля** пакета-якоря
  (когорта, роли, объём, паспорта) и матрицу роль×продукт. Второй путь — durable-аттестация
  профиля (`state/{package_id}/state.db`, таблица `profile_attestation`) — **объявлен и
  проверяется** гейтом, но **производителя в прогоне пока нет** (design-only: строку пишут
  только тесты). Сегодня рабочий путь один — per-run решение `hound gate`, оно и остаётся
  приоритетным (аудит-след и override).
- **G3/G4 — точки интеграции.** coHound не решает ревью и публикацию: на блоке пишется
  `runs/{run_id}/gates/{gate}-request.json` — детерминированный предмет решения плюс
  `subject_digest`; решение приходит извне и только записывается.
- Решение привязано к предмету: `gate_decisions` несёт `subject_digest`, `source`
  (`internal|external`), `external_ref`, `verdict` (`approved|rejected`). Устаревшее
  одобрение (предмет изменился) гейт не проходит, и сообщение называет изменившийся
  компонент. Явный отказ блокирует отдельным сообщением: молчание внешней системы не
  выдаётся ни за отказ, ни за одобрение.
- `run.yaml: gates.{g3,g4}: external|waived` (дефолт `external`). `waived` → фаза
  `skipped` с `skip_reason=gate_delegated_external`, waiver-строка в `gate_decisions` и
  `gate_waivers` в манифесте прогона (манифест живёт строкой в `cohound.db`; `report`
  выгружает его как `run_manifest.yaml`). Невидимый waiver запрещён.
  Политика гейтов — **процесс**, а не условие измерения: в `EXECUTION_CONTRACT_FIELDS` её
  нет, `run_id` она не меняет.

## Структура репозитория (сжато)

```
cohound-spec-v2.md   # регламент v2.1 — источник истины
AGENTS.md            # инструкции для агентов/разработчиков
core/                # ядро, категорийно-нейтральный код (§1.3)
  domain/            # Pydantic-модели, enums, инварианты, r2, scoring, anchors (без I/O)
  ports/             # Protocol-контракты (LLM, discovery_llm, индексы, store, search, crawler,
                     #   crawl_executor, container, poller, state, gateway, …)
  pipeline/          # сценарии Ш1–Ш9 (runner/phases), гейты (gates, gate_subject,
                     #   profile_attestation), краул (crawl_frontier, archive_descent,
                     #   artifact_probe), корпус/оценка, эксперименты, инкрементальность
  adapters/          # реализации портов (индексы, LLM, поиск, краул, экстракция, container_*)
  store/             # SQLite DDL + триггеры, StorePort/GateStore-реализация, backup
  config/            # загрузка run.yaml/пакетов, deploy.yaml, раскладка путей
  cli/               # composition root Z0 (ядро) — сборка зависимостей
  edge/              # composition root Z1/DMZ — веб-адаптеры за швом (07)
  util/              # текст (RU/EN), security (SSRF/slug), fusion (RRF), форматы,
                     #   container_policy (бюджеты спуска), memory (malloc_trim)
docs/                # архитектурный проект, регламент, absence-правила
docs/status-dictionary.yaml  # источник истины по перечислениям/статусам (§4.1)
contracts/seam/      # публикуемые JSON-схемы шва DMZ (/v1/*; 07 §5/§8)
config/              # deploy.yaml (профиль зон) + paths.example.yaml (раскладка каталогов)
scripts/             # scripts/ci.sh — единая точка входа CI
packages/{case_id}/  # пакеты категорий (package.yaml/catalog/baselines/cohort/golden/
                     #   domain_profiles/derived_fields/run.yaml/scope_attestation);
                     #   sources/ — авторские входы
passports/{product_id}/  # паспорта продуктов и сателлитов
runs/{run_id}/       # прогон: cohound.db (входы плана, решения гейтов), logs/, metrics.json
state/{package_id}/state.db  # durable per-product состояние (09 §4.1; источник --since-run)
index_store/ · corpus_raw/ · cache/ · eval_cache/ · extract_cache/
                     # операционные каталоги (runtime; в .gitignore)
tests/               # selfcheck, property-тесты домена, smoke scrapy-playwright
```

## Архитектура и документация

**Статус каждого документа** (что сделано, что открыто, что снято — с доказательством
`путь:строка`) — [`docs/architecture/README.md`](docs/architecture/README.md). Начинать
чтение каталога стоит с него: там сводка и разбор по удалению.

- [`docs/architecture/00-overview.md`](docs/architecture/00-overview.md) — обзор слоёв и потоков.
- [`01-modules-and-contracts.md`](docs/architecture/01-modules-and-contracts.md) — модули, контракты, матрица зависимостей.
- [`02-subsystems.md`](docs/architecture/02-subsystems.md) — детальный дизайн подсистем.
- [`03-data-config-cli.md`](docs/architecture/03-data-config-cli.md) — данные/хранилища, конфиг, CLI.
- [`04-reliability-adr.md`](docs/architecture/04-reliability-adr.md) — надёжность и ADR-реестр.
- [`05-dependency-stack.md`](docs/architecture/05-dependency-stack.md) — стек, лицензии, пины.
- [`06-implementation-strategy.md`](docs/architecture/06-implementation-strategy.md) — стратегия и статус реализации.
- [`07-deployment-topology.md`](docs/architecture/07-deployment-topology.md) — доверенные зоны и DMZ.
- [`09-product-onboarding-incremental-runs.md`](docs/architecture/09-product-onboarding-incremental-runs.md) — онбординг продукта, `state/`, инкрементальные прогоны (`--update`/`--since-run`).
- [`10-extraction-pipeline-architecture.md`](docs/architecture/10-extraction-pipeline-architecture.md) — плоскость извлечения: as-built, дефекты, целевая форма.
- [`11-production-architecture-strategy.md`](docs/architecture/11-production-architecture-strategy.md) — порядок фаз доведения до production-grade (F0–F6).
- [`12-cli-architecture-review.md`](docs/architecture/12-cli-architecture-review.md) — разбор CLI: дефекты и предложения.
- [`13-manual-artifacts.md`](docs/architecture/13-manual-artifacts.md) — вручную предоставленные артефакты (закрывают пробел от `robots.txt`, а не подписывают риск).
- [`14-vector-store-and-embedder-scaling.md`](docs/architecture/14-vector-store-and-embedder-scaling.md) — нужна ли векторная СУБД и внешний эмбеддер (решение с числами).
- [`15-external-review-findings-plan.md`](docs/architecture/15-external-review-findings-plan.md) — разбор внешних аудитов и план работ по находкам.
- `08-crawl-platform-evolution-plan.md` — план развития краула (намеренно **untracked**, решение владельца).

## Доверенные зоны и DMZ (дисклеймер)

Веб-функции (краул, внешний поиск, реестры, экстракция недоверенного контента) выносятся в
**DMZ** (`core/edge/`), не-веб код остаётся в ядре без веб-эгрессии. Связь — только через
application-layer firewall по протоколу `/v1/*`. Профиль `local` (один процесс) — дефолт;
профиль `split` (DMZ) — опция уже в MVP. Значение `split-hardened` принимается ради
совместимости конфига, но **громко предупреждает**, что ничего не добавляет сверх `split`
(ни Z2 PARSE, ни egress-прокси, ни TLS-инспекции): это алиас `split`, а не другой профиль.
Оценщик (evaluator) **никогда** не вызывает внешний
поиск: внешний контент становится evidence только после `corpus_raw → hash → index`
(I18/I19). Подробности — `docs/architecture/07-deployment-topology.md`.

> Инструмент даёт воспроизводимую карту **подтверждаемости** по публично доступным
> источникам, а не юридически значимую оценку продукта. Аттестация против конкретных
> корпоративных средств IDP/DLP/IDS — вне рамок проекта.

## Лицензия

Проект распространяется под лицензией **MIT** — см. [`LICENSE`](LICENSE).

Сторонние компоненты, включённые в репозиторий, сохраняют свои лицензии (указаны в шапках
соответствующих файлов).
