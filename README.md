# coHound

**coHound** — методология и программная система для построения воспроизводимой
**сравнительной технической карты предметных сущностей** по публично доказуемым
свойствам.

coHound отвечает не на вопрос «какая сущность лучше вообще», а на более узкий и
проверяемый вопрос:

> **что именно можно подтвердить по фиксированному протоколу, публичным источникам и
> сохранённому evidence?**

Результат — карта свойств по параметрам, сравниваемым сущностям и, если это нужно
домену, ролям или сценариям использования. Каждая ячейка имеет статус, evidence и
provenance. Если свойство не найдено, это не автоматически означает доказанное
отсутствие: для отрицательного вывода действуют отдельные правила absence-evidence,
полноты охвата и аттестации.

## Предметная нейтральность

Текущие примеры и терминология coHound в основном говорят о программных продуктах,
но это **сценарий применения, а не ограничение методологии**. Ядро `core/`
категорийно нейтрально: оно не знает, что сравнивается — программные продукты,
оборудование, товары, услуги, нормативные объекты или сложные технические системы.

Смысл сравнения задаётся авторским пакетом категории:

- набором сравниваемых сущностей и их стабильными идентификаторами;
- каталогом параметров, определениями и правилами интерпретации;
- ролями, сценариями или контекстами, если они имеют смысл для домена;
- когортой, паспортами, источниками и правилами допустимого evidence.

Поэтому при наличии корпуса параметров coHound может сопоставлять, например,
характеристики товаров, промышленного оборудования, компонентов, услуг или других
сложных объектов. Для этого не требуется менять методологическое ядро или встраивать
предметную логику в `core/`: доменная семантика должна жить в данных пакета.

Граница применимости проходит не по типу сущности, а по измеримости. Сравнение
воспроизводимо, если можно:

1. однозначно определить сущности и границы когорты;
2. формально выразить свойства как параметры и правила;
3. получить публичные или вручную предоставленные допустимые источники;
4. связать утверждения с evidence и provenance;
5. отличить подтверждённое отсутствие от «не найдено», неполного охвата и сбоя
   инструмента.

То есть coHound — это **domain-neutral evidence-based measurement system**, а не
только средство сравнения программных продуктов. Текущая реализация и имеющиеся
пакеты просто лучше всего демонстрируют его на программных продуктах.

## Коротко о текущем состоянии

На **2026-10-29** coHound — это:

- рабочий вертикальный срез методологии: пакет → план → корпус → retrieval → оценка →
  review → расчёт → отчёт;
- CLI-поставка с профилями `local` и `split`;
- single-node система, а не кластерная платформа;
- кодовые контракты для будущего распределённого исполнения, но без общего fleet-
  планировщика, shared durable queue и общего object storage;
- система, в которой основной незакрытый вопрос — не наличие фаз, а качество карты,
  приёмка реальных прогонов и стоимость недельного обновления большого портфеля.

Один реальный прогон уже дошёл до `report` и создал отчётные артефакты. Это доказывает
сквозную исполнимость текущего контура, но **не означает**, что coHound уже
production-ready для целевого масштаба 100–200 профилей в неделю.

Текущий статус реализации с доказательствами ведётся в
[`docs/architecture/06-implementation-strategy.md`](docs/architecture/06-implementation-strategy.md)
и сводится в [`docs/architecture/README.md`](docs/architecture/README.md).

## Что coHound делает

1. Принимает авторский пакет категории: каталог параметров, роли, когорту сущностей,
   паспорта, источники и правила прогона.
2. Строит детерминированный план измерения и фиксирует объявленные входы прогона.
3. Получает корпус из публичных источников с контролем SSRF, host scope, robots,
   лимитов и происхождения контента.
4. Нормализует HTML, text/markdown, EPUB, PDF, Office и заявленные контейнеры.
5. Строит scoped retrieval для сравниваемой сущности и сохраняет immutable index builds.
6. Передаёт оценщику только извлечённые и проиндексированные evidence-чанки.
7. Сохраняет provenance: источник → content hash → индексная сборка → evidence → ячейка.
8. Выпускает карту, preliminary-артефакты, вопросы на проверку и отчётные материалы.
9. Экспериментально сравнивает retrieval и evaluator, не подменяя измерение выбором
   модели «по умолчанию».

## Чем coHound не является

- не юридическим заключением и не универсальным рейтингом сущностей;
- не каталогом маркетинговых утверждений вендоров;
- не LLM с веб-поиском, которому разрешено самостоятельно искать evidence;
- не generic RAG-платформой;
- не заменой человеческой аттестации профиля, review и решения о публикации;
- не готовой распределённой платформой для еженедельной актуализации 100–200 профилей.

Методологический moat coHound — **протокол измерения, инварианты и audit trail**, а не
конкретная LLM, поисковый движок или extractor.

## Принципы

- **A1 — сначала ценность.** Сначала доказать работающий вертикальный срез минимальными
  средствами, затем масштабировать измеренные bottleneck'и.
- **A2 — moat в методологии.** Инфраструктурные компоненты заменяемы за портами и не
  должны менять смысл измерения.
- **A3 — измерение вместо предвидения.** Модель, retrieval-профиль, лимиты и прочие
  влияющие входы объявляются и проверяются экспериментом, а не выбираются удобством.

Отсюда следуют категорийная нейтральность `core/`, неизменяемость индексных сборок,
явная классификация отказов, сохранение неизвестности и запрет на автоматическое
выведение `N/A` или доказанного отсутствия.

## Как устроено измерение

Сквозной run состоит из 15 фаз:

```text
normalize → lint → plan → G1 → discovery → G2 → registry → inheritance → corpus
  → evaluate → review → G3 → compute → G4 → report
```

Гейты — это процессные границы, а не скрытый автоматический классификатор:

- **G1/G2** — внутренняя аттестация содержимого профиля и матрицы роль × сущность;
- **G3/G4** — точки интеграции с review и публикацией; решение приходит извне и
  записывается в audit trail;
- **G1/G2/G3 не блокируют run при отсутствии действующего решения**: система пишет
  видимую policy-waiver запись и продолжает фазу;
- **G4 блокирует публикацию**, а явный `rejected` по любому гейту блокирует прогон;
- решение действует только для конкретного `run_id` и `subject_digest`.

Оценщик не имеет веб-доступа. Внешний контент становится evidence только после
цепочки:

```text
public source → corpus_raw → hash → extraction → index → retrieval → evaluator
```

Это одновременно требование воспроизводимости, безопасности и I18/I19 регламента.

## Быстрый старт

### Установка

Требуется Python **3.14** и `uv`.

```bash
# Полный локальный контур для реального прогона
uv pip install -e '.[dev,retrieval,crawl,extract]'

# Для 7z/rar добавьте extra extract-archives
uv pip install -e '.[dev,retrieval,crawl,extract,extract-archives]'

# Воспроизводимая установка из lock-файла
uv sync --frozen --extra dev --extra retrieval --extra crawl --extra extract
```

Проверить состав окружения можно без сети:

```bash
./hound.py doctor
```

### Первый run

```bash
# План только печатается и ничего не пишет
./hound.py plan \
  --run-yaml packages/{case_id}/run.yaml \
  --package packages/{case_id}

# --urls — явный операторский вход корпуса
./hound.py run \
  --run-yaml packages/{case_id}/run.yaml \
  --package packages/{case_id} \
  --urls packages/{case_id}/sources/corpus_urls.yaml \
  --follow

# Если run дошёл до G4, внесите внешнее решение по request-файлу
./hound.py gate \
  --runs-dir runs \
  --run-id {run_id} \
  --gate G4 \
  --actor {кто} \
  --target publication \
  --notes "{почему}"

# Экспортировать отчётные артефакты из БД прогона
./hound.py report --runs-dir runs --run-id {run_id}
```

Свежий run не должен останавливаться на G1 только из-за отсутствия решения: G1–G3
неблокирующие по текущей политике. Продолжение уже начатого прогона выполняется
командой `run … --until {фаза}`. Решения гейтов не переносятся между прогонами.

### Краул и восстановление

```bash
# Отдельный сеанс краула
./hound.py crawl run \
  --run-yaml packages/{case_id}/run.yaml \
  --package packages/{case_id} \
  --urls packages/{case_id}/sources/corpus_urls.yaml \
  --follow

# Сводный или построчный статус
./hound.py crawl status --run-id {run_id}
./hound.py crawl status --run-id {run_id} --per-url

# Post-hoc lookup source_id → URL без сети
./hound.py resolve --db runs/{run_id}/cohound.db {source_id}
```

`crawl resume` продолжает тот же durable-сеанс и не пере-фетчит уже успешно полученные
элементы. `crawl restart` запускает его заново. Причины нулевого корпуса продукта
сохраняются в результатах прогона и не маскируются как «просто пустой результат».

## Форматы и extraction

- HTML — основной веб-источник.
- Plain text, markdown и EPUB — first-class форматы со своими reader/chunker.
- PDF и Office (`docx`, `pptx`, `xlsx`) — через docling.
- `zip` читается стандартной библиотекой; `7z` и `rar` требуют
  `extract-archives` и соответствующих внешних backend'ов.
- Глубина вложенных архивов не ограничивается искусственным числом уровней; конечность
  обеспечивается циклом по content hash и объявленным бюджетом байтов.
- Исчерпание бюджета и отсутствие backend'а видимы в результате, а не превращаются в
  тихое отсутствие документов.

Текущий extraction исполняется в trusted core. Поле `extractor_zone` и целевая Z2
PARSE-зона пока являются проектом, а не рабочим переключателем. В `split` уже вынесены
веб-адаптеры, но полного паритета local/split для крупных артефактов и членов архивов
пока нет.

## Развёртывание и границы доверия

### `local`

Один процесс, локальные адаптеры и SQLite-файлы. Это дефолт для разработки, небольших
прогонов и доказательства ценности по A1.

### `split`

Core находится в доверенной зоне без веб-эгрессии, а crawler, search и registry
адаптеры — за L7 HTTP/JSON seam в edge/DMZ. Оценщик не получает интернет-маршрут.

`split` — уже существующий deployment profile, но не кластер: текущая edge queue и
blob paths локальны узлу. Load balancer перед несколькими такими узлами сам по себе
не создаёт общую очередь, leases или согласованное object storage.

Подробная схема зон и опубликованные seam-схемы:
[`docs/architecture/07-deployment-topology.md`](docs/architecture/07-deployment-topology.md)
и [`contracts/seam/`](contracts/seam/).

## Целевой образ: недельное обновление портфеля

Целевой режим coHound — регулярное обновление порядка **100–200 профилей**, в каждом
примерно **10–15 продуктов** и **100–200 фактов**. Это означает не только сотни тысяч
ячеек, но и большой поток crawl, extraction, архивов, embeddings, index builds и
LLM-вызовов.

Такой режим требует горизонтально масштабируемого execution plane. Масштабировать
следует не каждую из 15 фаз как отдельный микросервис, а независимые единицы работы и
ресурсные классы:

```text
control plane
  └─ profile/run orchestration
      ├─ crawl tasks
      ├─ extraction tasks
      ├─ product index builds
      └─ evaluation tasks

durable metadata + queue     immutable object storage
        │                              │
        ├─ edge/http/browser workers   ├─ raw artifacts
        ├─ text/PDF/archive workers    ├─ extracted artifacts
        ├─ index builders              ├─ immutable index partitions
        └─ evaluator pool              └─ prompts/results
```

Главные независимые оси:

- crawl — сеть, origin pacing и browser contexts;
- PDF/Office — CPU/RAM/VRAM и отдельные окна concurrency;
- архивы — bytes·sec и длительность одной задачи;
- index build — product partition и immutable build;
- evaluator — calls/sec, tokens, latency и quota LLM endpoint;
- retrieval — пока локальный read-only компонент рядом с индексом/evaluator, пока
  измерения не потребуют отдельного сервиса.

Целевой переход состоит из этапов:

1. измерить недельную стоимость и resource classes на одном узле;
2. добавить batch runner и product-level fan-out с независимыми run-каталогами;
3. сделать raw/extracted/index artifacts durable и доступными между worker-ами;
4. ввести shared queue, leases, retry/reclaim и PostgreSQL-compatible metadata store;
5. после этого развести deployments для edge, extraction, index и evaluator.

Это roadmap, а не заявление о текущей готовности. SQLite может оставаться локальной
per-run БД и audit store, но не должна быть авторитетной общей очередью или
координационным хранилищем fleet-инсталляции.

## Основные ограничения текущей реализации

На текущем этапе не следует выдавать за сделанное:

- горизонтальное выполнение product-stage на нескольких узлах;
- cluster-wide leases, reclaim и shared durable queue;
- общее object storage как часть correctness-модели;
- отдельную долговечную PDF/extraction queue;
- полный local/split parity для артефактов и контейнеров;
- production-grade качество карты без полноценного `golden-full`, evaluator benchmark
  и измерения недельного workload;
- доказанную пропускную способность SQLite или готовность fleet-режима.

Это не отменяет уже работающий вертикальный срез. Это разделение **as-built** и
**целевого образа**, необходимое для честного позиционирования.

## Пакеты, данные и документация

Пакет категории живёт в `packages/{case_id}/`. Его назначение и жизненный цикл описаны
в [`docs/user-guide/package-profile.md`](docs/user-guide/package-profile.md), а
подробный состав файлов и полей — в
[`docs/user-guide/package-files.md`](docs/user-guide/package-files.md).

Ключевые каталоги:

```text
cohound-spec-v2.md                 # регламент v2.1, источник истины по семантике
core/domain/                       # модели, enums, инварианты, scoring, без I/O
core/ports/                        # порты LLM, store, search, crawl, index, gateway…
core/pipeline/                     # сценарии Ш1–Ш9, corpus, evaluate, gates, report
core/adapters/                     # extraction, crawl, search, index, LLM и прочие adapters
core/store/                        # SQLite DDL/store и backup
core/edge/                         # edge/DMZ composition root и seam server
packages/{case_id}/                # данные и входы категории
passports/{product_id}/            # паспорта продуктов и сателлитов
runs/{run_id}/                     # durable состояние одного прогона и outputs
state/{package_id}/state.db        # долговременное состояние инкрементальности
index_store/ corpus_raw/ cache/    # runtime artifacts, обычно в .gitignore
contracts/seam/                    # публикуемые JSON-схемы core ↔ edge
docs/architecture/                 # архитектура, решения и планы доведения
tests/                             # selfcheck, unit/property/contract/e2e tests
```

Начинать изучение архитектуры стоит с:

- [`cohound-spec-v2.md`](cohound-spec-v2.md) — семантика и инварианты;
- [`docs/architecture/00-overview.md`](docs/architecture/00-overview.md) — карта
  компонентов и доверенных зон;
- [`docs/architecture/06-implementation-strategy.md`](docs/architecture/06-implementation-strategy.md)
  — as-built и открытые работы;
- [`docs/architecture/08-crawl-platform-evolution-plan.md`](docs/architecture/08-crawl-platform-evolution-plan.md)
  — целевая платформа краулинга и fleet-масштабирование;
- [`docs/architecture/11-production-architecture-strategy.md`](docs/architecture/11-production-architecture-strategy.md)
  — порядок доведения до production-grade;
- [`docs/architecture/14-vector-store-and-embedder-scaling.md`](docs/architecture/14-vector-store-and-embedder-scaling.md)
  — измеренные границы retrieval и решение по Qdrant.

## Разработка и проверки

```bash
./hound.py doctor
python3 tests/selfcheck.py
python3 -m pytest -q -n 3 --dist worksteal
PYTHON=.venv/bin/python bash scripts/ci.sh
```

`scripts/ci.sh` — единая точка CI: ruff, mypy-gate, selfcheck, pytest и порог покрытия.
На машине с ограниченной памятью не используйте `-n auto`; число worker-ов ограничено
RAM, а не количеством CPU-ядер.

## Лицензия

Проект распространяется под лицензией **MIT** — см. [`LICENSE`](LICENSE).

Сторонние компоненты, включённые в репозиторий, сохраняют свои лицензии; ссылки и
уведомления находятся в соответствующих файлах и реестре зависимостей.
