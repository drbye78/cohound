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
  ports/                      # Protocol-контракты: LLM, discovery_llm, индексы, embedder, index_builder,
                              #   search, crawler, crawl_executor, container, poller, extractor, chunker,
                              #   registry, store, gate_store, state, gateway, types
  store/                      # SQLite DDL §4.4 + триггеры (sqlite.py); StorePort/GateStore-реализация
                              #   (sqlite_store.py); backup.py — бэкап/восстановление прогона
  pipeline/                   # сценарии Ш1–Ш9: runner.py (PHASES + оркестратор), phases.py,
                              #   гейты: gates.py, gate_subject.py (предмет решения),
                              #   profile_attestation.py (аттестация профиля для G1/G2);
                              #   краул: crawl_frontier.py (scope хостов), crawl_session.py,
                              #   crawl_sources.py (source routing), fetch_plan.py, archive_descent.py
                              #   (спуск в контейнеры), artifact_probe.py (допуск артефактов);
                              #   корпус/оценка: corpus.py, extract_cache.py, eval_cache.py, retrieve.py,
                              #   evaluate.py, exhaustion.py (протокол исчерпания I3), prompt.py,
                              #   review.py, compute.py, report.py;
                              #   эксперименты: experiments.py, evaluator_benchmark.py, golden.py;
                              #   инкрементальность: incremental.py, onboarding.py, state_sync.py,
                              #   run_identity.py, digests.py, composition.py;
                              #   SERP-петля/search_log, observability.py; авторство пакета:
                              #   package_{io,admin,discovery,candidates,scout,add_product}.py,
                              #   passport_scout.py, scout_common.py
  config/                     # загрузка run.yaml/пакетов, deploy.yaml (профиль зон), раскладка путей
                              #   (paths.py), доменные профили (domain_profiles.py), crawl_sources.py, passports.py
  cli/                        # composition root Z0 (ядро): main.py (тонкий вход + реэкспорт),
                              #   parser.py (build_parser), context.py (общие DI/ГЭП-хелперы),
                              #   errors.py (EXIT_*/CliError), output.py (--json/emit_*),
                              #   wiring.py (сборка build_* адаптеров), doctor.py, __main__.py;
                              #   команды — commands/* (реестр COMMANDS), подкоманды hound package —
                              #   package/* (add_package_parser, cmd_package_*); package_commands.py —
                              #   re-export-шим (обратная совместимость)
  edge/                       # composition root Z1/DMZ — веб-адаптеры за швом (07): server.py, auth.py,
                              #   audit.py, seam_schema.py, scheduler.py (очередь краула), poller.py,
                              #   crawler_factory.py, config.py (EdgeRuntimeConfig), __main__.py (launcher)
  adapters/                   # chunker_markdown (heading_path) / chunker_text (абзацы) /
                              #   chunker_family (диспетчер по семейству), extract_document
                              #   (docling: pdf+office), extract_html, extract_text (plain text/md),
                              #   extract_epub (stdlib OCF), container_{zip,7z,rar,any} (спуск в архивы),
                              #   lexical_{bm25,tantivy}, index_npy/index_format, vector_npy,
                              #   embed_{local,model2vec}, llm_{local,discovery}, search_*, crawl_*,
                              #   feed_poller, remote_* (шов), gateway_validate — см. NOTICES
  util/                       # text_utils (RU/EN), security (SSRF/slug), sanitize, fusion (RRF),
                              #   retrieval_eval, agreement (Cohen's κ §11.5), feed (feed/channel-парсер),
                              #   document_formats/document_probe (классификация форматов),
                              #   container_policy (бюджеты и прогноз спуска), memory (malloc_trim),
                              #   robots, sitemap, rate_limit, cache, ids/hashing/logging — см. NOTICES
scripts/ci.sh                 # единая точка входа CI (§11.6): selfcheck + pytest + порог покрытия
tests/selfcheck.py            # self-contained проверка фундамента (автономная, без pytest)
tests/test_selfcheck.py       # pytest-обёртка над selfcheck (W1/T1.5)
tests/domain/                 # property-тесты домена (hypothesis)
tests/smoke_scrapy_playwright.py  # браузерный smoke scrapy-playwright (V5; 0/1/2)
packages/{case_id}/           # пакеты категорий: package.yaml/catalog.csv(+.lock)/
                              #   baselines.csv/cohort.yaml/golden.csv/derived_fields.csv/
                              #   domain_profiles.yaml/scope_attestation.yaml/run.yaml;
                              #   sources/ — авторские входы (seeds/corpus_urls/render_modes);
                              #   ОПЦИОНАЛЬНО: crawl_sources.yaml (source routing: feed/channel) и
                              #   registries/ (package-scoped реестровые спецификации, 03 §D.4) —
                              #   сейчас их нет ни в одном пакете, включая _template
docs/ · docs/templates/registries/  # словари, регламент, absence-правила, шаблоны реестров
contracts/seam/               # публикуемые JSON-схемы шва DMZ (/v1/*; 07 §5/§8)
passports/{product_id}/       # паспорта продуктов
runs/{run_id}/                # прогоны: cohound.db (входы плана run_inputs — источник истины;
                              #   манифест прогона — строка БД, не файл; report выгружает
                              #   run_manifest.yaml отдельно), logs/, metrics.json,
                              #   gates/{gate}-request.json; plan_inputs.json — производный экспорт
index_store/{package}/{build}/{product_id}/  # иммутабельные индексные сборки (продукт-партиции)
corpus_raw/{product_id}/      # retention сырых байтов
cache/serp/{query_hash}.json · cache/http/{url_hash}.bin  # глобальный content-addressed кэш (R7)
state/{package_id}/state.db   # durable per-product состояние (09 §4.1): lineage, previous_run_id,
                              #   change_class; источник --since-run; таблица profile_attestation
                              #   (G1/G2) — design-only: писателя в прогоне нет
eval_cache/ · extract_cache/  # межпрогонные кэши вердиктов оценщика и экстракции (в .gitignore)
config/deploy.yaml · config/paths.example.yaml  # профиль зон (edge) · пример раскладки каталогов
                              #   (COHOUND_HOME/COHOUND_*_DIR)
```

**Доверенные зоны (docs/architecture/07-deployment-topology.md).** Веб-функции (краул, поиск,
реестры, экстракция недоверенного контента) выносятся в **DMZ** (`edge/`), не-веб — в ядро без
веб-эгрессии; связь — только через application-layer firewall по протоколу `/v1/*`. Профиль `split`
(DMZ) — **опция уже в MVP** (M1, поток W13); `local` (один процесс) — дефолт. Это усиливает I18 (у
оценщика нет маршрута в интернет) и делает явной границу I19. Контракты шва проектируются
**прозрачными для корпоративных IDP/DLP/IDS** (L7-JSON, публикуемые схемы, без pinning, OIDC/mTLS);
аттестация против конкретных средств — вне рамок проекта. Значение `split-hardened`
принимается ради совместимости конфига, но громко предупреждает, что ничего не добавляет
сверх `split` (алиас, а не другой профиль). `extractor_zone` объявлено, но **не
подключено**: экстрактор всегда в ядре (D-9).

**Форматы и контейнеры.** HTML — основной источник; plain text/markdown (все расширенные
варианты) и EPUB — **первый класс** наравне с HTML (свой читатель `extract_text`/
`extract_epub` и свой чанкер, не docling); PDF и Office (docx/pptx/xlsx) — через docling
(`--document-level`: `off|text|layout|layout_tables|full`), устройство моделей — объявленной
осью `crawl.pdf_device` (`cpu|cuda`; `auto` нет: см. «Чего НЕ делать»).
**Гибридного PDF-пайплайна нет, и это решение замера, а не недоделка**: pdfium на «простых»
страницах терял бы 6.7 % идентификаторов docling-а на реальной книге, а единственный
детектор таблиц с recall 1.0 помечает 90 % страниц (`docs/reports/measurements/
2026-10-28-hybrid-quality-gate.md`). Материалы и детекторы сохранены там же как основа для
дальнейших экспериментов; условие пересмотра — размеченные страницы или классификатор с
recall 1.0 при реальной экономии (отчёт §6). Заявленные контейнеры
(`zip`/`7z`/`rar`) раскрываются спуском (`archive_descent.py`): **потолка глубины нет**, и
это держится **механикой обхода, а не только бюджетом**: обход итеративен (явный стек
кадров-генераторов), поэтому глубина не упирается в стек интерпретатора. Конечность дают
цикл по хешу содержимого (`archive_cycle`) и кумулятивный бюджет корня
(`archive_max_total_uncompressed_bytes`; оси `archive_max_*` ограничивают **один**
контейнер). Замер (почему это важно): до перехода на явный стек цепочка **различных**
маленьких архивов упиралась в рекурсию на ~990 уровнях и заканчивалась видимой, но ложно
названной ошибкой `container_reader_unavailable` (`RecursionError` гасился в выборе
читателя); цепочка 5000 различных zip теперь проходит до конца с 5001 наблюдением и нулём
ошибок. Тест — `tests/pipeline/test_corpus_descent.py::test_chain_of_5000_distinct_archives_reaches_the_end`.
Исчерпание бюджета — видимая запись `archive_budget_exhausted`, а не тихое
завершение. Бэкенды: `zip` читается stdlib-ом, `7z`/`rar` — extra `extract-archives`
(`py7zr`/`rarfile`; сжатые члены `rar` требуют внешнего `bsdtar`/`unrar`/`7z`, AES-zip —
`pyzipper`). Без бэкенда контейнер **не разворачивается видимо** (`available()=False`,
`backend_unavailable`, `container_reader_unavailable`), а не «тихо пусто». Память: после
каждого документа воркер отдаёт ОС свободные арены glibc (`core/util/memory.py`,
`malloc_trim`) — иначе RSS растёт храповиком до OOM (замеры — в докстринге модуля).

## Краул: scope — объявленные хосты

**Scope краула — только объявленные хосты.** URL ставится в очередь, если он (1) проходит
SSRF-валидацию, (2) принадлежит **объявленному** хосту, (3) укладывается в path-prefix корня
по границе сегмента, (4) не в блэклисте. SSRF-проверка **одна** — это не проверка scope.

- Хост корня — всегда в scope. Поддомен — **только если объявлен** (seed-корень продукта или
  ключ `domain_profiles.yaml`). `www.` эквивалентен базовому хосту с обеих сторон.
- Предикат — `Frontier.host_in_scope` (`core/pipeline/crawl_frontier.py`). Это **другая**
  семантика, чем `fetch_plan._host_matches`: там поддомен **наследует оси профиля**
  (`fetch`/`extract`/лимиты) — это верно и нужно; наследование осей ≠ право тратить бюджет
  чужого seed-корня.
- Отказ **видим**: `out_of_scope_subdomains` (счётчик по хостам + примеры) и
  `accepted_foreign_hosts` (сколько бюджета корня ушло на объявленный чужой хост).
- Терминальный артефакт (документ/контейнер) — исключение **только** из path-prefix: он не
  продолжает навигацию, но SSRF/host-scope/блэклист остаются обязательными.

## Гейты G1–G4

- **Что блокирует прогон.** `NON_BLOCKING_GATES = {G1, G2, G3}` (`core/pipeline/gates.py:79`,
  решение владельца 26.10.2026): нет действующего решения — фаза идёт дальше, а в
  `gate_decisions` пишется строка-политика (`actor=policy`,
  `external_ref=waiver:policy:nonblocking:{gate}`), которая в следе манифеста читается как
  `state: not_required`. Блокируют только **G4** и **явный отказ** (`verdict=rejected`) по
  текущему предмету у любого гейта: отказ — это решение, а молчание внешней системы не
  выдаётся ни за отказ, ни за одобрение.
- **G1/G2 — внутренняя аттестация.** Человек утверждает содержимое профиля пакета-якоря
  (когорта/роли/объём/паспорта) и матрицу роль×продукт. Второй путь — durable-аттестация
  профиля (`profile_attestation` в `state/{package_id}/state.db`) — **объявлен и
  проверяется** гейтом (`make_profile_gate` подключён в CLI), но **производителя в прогоне
  нет** (design-only: строку пишут только тесты), поэтому описывать его как работающий
  механизм нельзя. Рабочий путь сегодня один — per-run строка `gate_decisions` (аудит-след и
  override), и она **приоритетна**.
- **G3/G4 — точки интеграции.** coHound не решает ревью/публикацию: на блоке пишется
  `runs/{run_id}/gates/{gate}-request.json` (детерминированный предмет + `subject_digest`),
  решение приходит извне и только записывается.
- `gate_decisions`: `subject_digest`, `source` (`internal|external`), `external_ref`,
  `verdict` (`approved|rejected`). Устаревшее одобрение (предмет изменился) гейт не проходит,
  и сообщение называет изменившийся компонент. Явный отказ блокирует отдельным сообщением —
  молчание внешней системы не выдаётся ни за отказ, ни за одобрение.
- `run.yaml: gates.{g3,g4}: external|waived` (дефолт `external`). `waived` → фаза `skipped`
  с `skip_reason=gate_delegated_external`, waiver-строка в `gate_decisions` и `gate_waivers`
  в манифесте прогона (манифест — строка в `cohound.db`; `report` выгружает
  `run_manifest.yaml`). Невидимый waiver запрещён.
- `hound gate` требует `--subject-digest` (по умолчанию берётся из request-файла);
  `--source external` требует `--external-ref` и `--notes`.
- Политика гейтов — **процесс**, а не условие измерения: в `EXECUTION_CONTRACT_FIELDS` её
  нет (иначе смена процедуры меняла бы `run_id` и рвала межцикловый diff).
- `Gate` — снова G1–G4, `PHASES` — 15 фаз: `g3` между `review` и `compute`, `g4` между
  `compute` и `report`.

## Команды

### Прогон (runbook)

```bash
# 0. План печатается (stdout/--json) и ничего не пишет: черновика run.yaml/cohort.yaml нет.
python3 -m core.cli plan --run-yaml packages/{case_id}/run.yaml --package packages/{case_id}

# 1. Прогон. --urls — операторский корпусный вход; без него корпус URL пуст.
#    --follow печатает прогресс сеанса краула до терминального статуса.
python3 -m core.cli run --run-yaml packages/{case_id}/run.yaml \
  --package packages/{case_id} --urls packages/{case_id}/sources/corpus_urls.yaml --follow

# 2. Свежий прогон НЕ встаёт на g1: G1/G2/G3 не блокируют (NON_BLOCKING_GATES,
#    core/pipeline/gates.py:16, решение владельца 26.10.2026). Фаза g1 идёт с
#    skip_reason=gate_not_blocking, в gate_decisions ложится waiver-строка
#    (source=external, external_ref=waiver:policy:nonblocking:g1), и прогон
#    продолжается в discovery/corpus. Решения «разблокировать» здесь не требуется.
#    Блокируют только: G4 (публикация) и ЯВНЫЙ отказ (verdict=rejected) по текущему
#    предмету у любого гейта — отказ есть решение, а не молчание.
echo $?    # 0 — прогон дошёл до конца либо до блокирующего G4

# 3. Если прогон встал на G4 (или нужен аудит-след G1/G2/G3) — решение пишется
#    per-run. На блоке прогон пишет runs/{run_id}/gates/{gate}-request.json — оттуда
#    берётся subject_digest; если файла нет, передайте --subject-digest явно.
python3 -m core.cli gate --runs-dir runs --run-id {run_id} \
  --gate G4 --actor {кто} --target publication --notes "{почему}"

# 4. Артефакты отчёта из БД (пишет {out}/{run_id}/outputs/); очередь ревью — review.
python3 -m core.cli report --runs-dir runs --run-id {run_id}
python3 -m core.cli review --runs-dir runs --run-id {run_id}

# 5. Выбор модели оценщика — измерительная задача (A3), а не предрешение:
#    валидность JSON, κ, confusion, abstention, fabricated_support, bootstrap CI, детерминизм.
python3 -m core.cli experiment evaluator-benchmark --runs-dir runs --run-id {run_id} \
  --golden packages/{case_id}/golden.csv --model {model_id}
```

**Гейты per-run:** новый `run_id` требует новых решений; строка `gate_decisions` от
прошлого прогона не действует. Продолжить с места остановки — `--until {фаза}`,
`--reset` — форсировать перезапуск, `--from-scratch` — ещё и живой ре-фетч в обход R7-кэша.
Отдельный сеанс краула — `hound crawl run|status|list|resume|restart|cancel`
(`crawl resume` идемпотентен: та же `session_id`, уже `FETCHED`-элементы не пере-фетчатся).
Свидетельство живости (`crawl_sessions.owner`/`heartbeat_at`, `core/pipeline/liveness.py`):
протухший `running` при открытии сеанса становится `interrupted` (не `failed`/`cancelled`:
тело краула не финализировано); `crawl list`/`status` показывают `owner`/`heartbeat_at`/
`stale_reason`, а `crawl cancel` снимает `running` как `interrupted` с причиной
`cancelled_by_operator`. `interrupted` обязан оставаться в `_TERMINAL_SESSION_STATUSES`
(`core/cli/context.py`) — иначе `--follow` ждёт вечно. Это не lease: координации нод нет.
`--follow` печатает `crawl_progress` (агрегат сеанса) и `crawl_product` (границы продукта:
агрегат не отличает «ещё в очереди» от «потерян»). «Почему у продукта ноль источников»
читается в `results[].notes` (`--json` прогона), там же
`product_start`/`product_end`/`product_skipped`; живой краул — `hound crawl status --per-url`.

### Разработка и CI

```bash
# Проверка фундамента (без pytest)
python3 tests/selfcheck.py

# CI-гейты (ruff → mypy [домен «ноль» + baseline ядра] → selfcheck → pytest + порог покрытия) —
# провайдер-нейтрально
bash scripts/ci.sh

# Линтер и type-checker по отдельности (оба в extra `dev`; реестр — 05-dependency-stack §2.7.1)
.venv/bin/ruff check core tests scripts
.venv/bin/python scripts/mypy_gate.py           # домен обязан быть чистым (0 ошибок)
.venv/bin/python scripts/mypy_gate.py --update  # перегенерировать baseline ядра — осознанно

# pytest (dev-окружение): обёртка над selfcheck + property-тесты домена
uv pip install -e '.[dev]'           # или воспроизводимо из лока: uv sync --frozen --extra dev
python3 -m pytest -q

# Параллельный прогон (pytest-xdist в extra dev) — быстрее последовательного, но число
# воркеров ограничено ПАМЯТЬЮ, а не ядрами: `-n auto` берёт nproc и на хосте с 7 GiB
# упирается в OOM/ресурсные флаки (наблюдалось `test_edge_launcher[forkserver]`, в
# изоляции проходит). scripts/ci.sh по умолчанию берёт `min(4, nproc)`;
# `COHOUND_CI_WORKERS=N` переопределяет, `COHOUND_CI_WORKERS=auto` возвращает `-n auto`
# (осмысленно только при запасе RAM; замер: ~0.6 GiB RSS на воркер).
python3 -m pytest -q -n 4 --dist worksteal                     # полный набор
python3 -m pytest -q -n 4 --dist worksteal -m "not slow"       # быстрое подмножество

# Грепп-нейтральность ядра (§1.3): правило — предметных терминов в core/ быть не должно.
# Гейт проверяет ТРИ слоя (tests/selfcheck.py::_neutrality) — ровно этот набор и есть CI:
grep -rniE "postgres|pangolin|sber|сбер|copywala|платформа" core/domain core/ports core/pipeline

# Широкий грепп по всему core/ — подсказка, а не гейт: он проверяет слои, где правило
# держится дисциплиной (adapters/store/util/config/cli/edge). Сейчас чист; срабатывание —
# повод посмотреть, а не автоматический отказ (гейт от него не зависит).
grep -rniE "postgres|pangolin|sber|сбер|copywala|платформа" core/

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
Чтобы реально прогнать кейс (фазы `corpus`/`evaluate`), нужны extras `retrieval`, `crawl`,
`extract`: `uv pip install -e '.[dev,retrieval,crawl,extract]'`. `zip` читается stdlib-ом,
`7z`/`rar` — extra `extract-archives`. Проверка состава — `./hound.py doctor` (печатает группы
`core`/`retrieval`/`crawl`/`extract`/`safety`/`ops`/`embed`/`embed-lite`/`dev`).
Воспроизводимо из лока: `uv sync --frozen --extra dev --extra retrieval --extra crawl --extra extract`
(и `--extra extract-archives` для `7z`/`rar`). `uv lock --check` проходит; если правите
зависимости — обновите лок тем же коммитом, иначе `--frozen` откажет.
Dev/CI по §11.6: `pytest`+`pytest-xdist`+`hypothesis`+`pytest-cov` в extra `dev`; единая точка входа — `scripts/ci.sh`.
Выбор внешних пакетов, лицензии и запреты — `docs/architecture/05-dependency-stack.md`
(реестр решений; LangChain/LlamaIndex и все hosted-парсеры отклонены).
Стратегия и статус реализации (потоки W1–W13 as-built, майлстоуны M0–M3, DoD) —
`docs/architecture/06-implementation-strategy.md`.
Развёртывание: доверенные зоны и DMZ-шов (application-layer firewall) —
`docs/architecture/07-deployment-topology.md`.
Source routing краула (`crawl_sources.yaml`): директорные источники `feed`/`channel` разворачиваются
в записи через `PollPort` (local `LocalFeedPoller` ‖ split `RemoteFeedPoller`→шов `crawl/poll`) —
`docs/architecture/03-data-config-cli.md` §D.2.1, `02-subsystems.md` §B.1.
Переменные окружения и внешние ключи (LLM, scout, поисковые адаптеры) + runbook живого
прогона корпуса — шаблон без секретов `.env.example` и `docs/architecture/03-data-config-cli.md` §D.6.

## Конвенции кода

- **Категорийная нейтральность `core/`** — обязательна. Никаких названий продуктов,
  вендоров и предметных терминов. Вся специфика — данные в `packages/`. Нарушение
  блокирует кейс (§11.4).
  **Проверяется автоматически:** гейт — три слоя, `core/domain`, `core/ports`,
  `core/pipeline` (`tests/selfcheck.py::_neutrality`, тот же набор в `scripts/ci.sh`).
  Остальные слои (`adapters/`, `store/`, `util/`, `config/`, `cli/`, `edge/`) держит
  **правило**, а не гейт: широкий `grep` по `core/` — подсказка, и срабатывание в нём
  разбирается глазами. Сужение гейта не ослабляет правило — знание о конкретном продукте
  в ядре остаётся дефектом в любом слое.
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
  `source_scope: exhaustive` **записывается** (проверяется `_has_exhaustive_scope`), а
  `scope_attested_by` не требуется: реестр — авторитетная энумерация. Для `explicit`
  `source_scope` не требуется — но см. I2c.
- **I2c** `❌` при **неполном** покрытии корпуса (`coverage.complete is False`) — условие
  зависит от **класса отрицания**, а не от одной полноты краула
  (`check_absence_coverage`, зеркало в отчёте; вызывается в Ш6 через
  `evaluate._enforce_absence_coverage` — до 28.10 он был объявлен, но в конвейере **не
  исполнялся**):
  - `explicit` (дословная цитата вендора) и `authoritative` (авторитетная энумерация) —
    неполнота охвата их **не отменяет**: это свидетельство, а не вывод из полноты корпуса;
  - `bounded` — зависит от пространства, поэтому требует `scope_acceptance` (принятия риска
    человеком); без неё `❌` не публикуется;
  - `unobserved`/`not_stated` — `❌` не дают вообще (I2).
  Нарушение → ячейка **`❓`** (`not_stated`, I12), а не `failed`: отсутствие принятия риска
  — процедурное условие, а не отказ прибора (I4), и ячейка обязана остаться в карте. Снятый
  `❌` не публикует и протокол отсутствия (`absence_evidence=[]`): «искали и не нашли» —
  утверждение, которое здесь не доказано. Причина уходит машинным токеном
  `absence_coverage_unsatisfied` в `evidence_scope.reason` **и** `cells.validation_notes`,
  называя недостающий компонент и виновника (`{product_id}/{param_id}`).
  `coverage=None` (нет записи покрытия) — проверка неприменима.
  Подпись живёт в `packages/{case_id}/scope_attestation.yaml:attestations` (**per product**),
  ставится `hound package scope attest --package … --product … --actor …`; прогон читает её
  и передаёт в Ш6, использованные подписи видны в манифесте (`scope_attestations`).
  В `EXECUTION_CONTRACT_FIELDS` не входит: авторизация, не измерение.
- **I3** strict-`❓` → **полный** протокол исчерпания (`corpus`+`serp`+`registry`+`inheritance`,
  `check_unknown_exhaustion`) **или** `reviewed_by ≠ null`. Неполный протокол — **аномалия** отчёта,
  не смена статуса (`❓` остаётся `❓`, I12). Протокол пишет Ш6 из **наблюдённых** фактов
  (`core/pipeline/exhaustion.py`), и он называет **только реально исполненные** контуры:
  «не смог дешёво установить» трактуется как «контур не исполнялся». Поэтому на прогоне без
  SERP-контура `❓` остаётся аномалией — это верный сигнал, а не дефект, который надо заглушить.
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
- **I22** `evidence_type=direct` обязан нести **непустой** `citation_quote` (иначе это не
  доказательство наличия). Исключение — `source_kind=manual`: ручное evidence есть носитель
  человеческой аттестации. Самый частый отказ при добавлении evidence
  (`check_evidence`); SQL-триггера на I22 **нет** — колонка `citation_quote` в
  `cell_evidence` объявлена без CHECK, так что гейт здесь только доменный.

## LLM-контуры (§7) — ключевое разделение

- **evaluator** (Ш6): модели через [OI]-совместимый API **внутри корпоративного периметра**
  (внешние LLM-хосты/ключи не используются), **без веб-тула**, работает только по переданным
  чанкам; строгий JSON через constrained decoding.
  **Идентичность модели оценщика — объявленный вход**, а не окружение:
  `--llm-model` > `deploy.llm.<slot>.model_id` > **ошибка** (`EXIT_USAGE`, код `llm`).
  Дефолтной модели нет: модель, которую оператор не объявлял, — ложная провенанс-запись.
  `COHOUND_LLM_MODEL`/`COHOUND_LLM_DECODER` оценщиком **не читаются** (о факте игнорирования
  сообщает `llm_env_notices`); endpoint/ключ/таймаут — операционные и берутся из env.
  Причина: `llm_scenarios` входит в `execution_contract_digest` → `run_id`, поэтому модель
  обязана быть функцией объявленных входов (I21/§7.4). Пустой `base_url` в слоте при
  непустом env `COHOUND_LLM_BASE_URL` даёт **видимое** предупреждение: оценщик поднят вне
  контракта, `run_id` модель не описывает.
- **search adapter** (Ш5): внешний поиск как источник контента, нормализуется в корпус.
- **discovery / concept scout**: поиск допустим, выход — кандидаты/гипотезы (G2).
  Здесь env `COHOUND_LLM_*` **читается** (аргументы > env > слот) — это **намеренная
  асимметрия**: выход разведки не evidence, поэтому модель — операционная настройка, а не
  условие измерения. Смешивать «кандидатов» и «доказательства» в одну политику значило бы
  позволить окружению переписывать провенанс вердиктов.

Не встраивать веб-поиск в оценщик — это ломает evidence-модель и воспроизводимость.
Выбор модели оценщика — **измерительная** задача (A3), а не предрешение:
`hound experiment evaluator-benchmark --runs-dir runs --run-id {run_id} --golden … --model …`
считает валидность JSON, κ, confusion, abstention, `fabricated_support`, bootstrap CI и пробу
детерминизма (`--repeats`), персистит `llm_scenarios`; `model_id` берётся **только** из
флага, не из env.

## Git-конвенции

- Осмысленные атомарные коммиты; сообщение в повелительном наклонении, с префиксом
  области при необходимости: `core: ...`, `docs: ...`, `spec: ...`, `tests: ...`.
- Секреты не коммитить. `runs/`, `index_store/`, `corpus_raw/`, `__pycache__/` — в `.gitignore`.
- Slug (`param_id`, `product_id`) иммутабельны; отмена — `deprecated` + `replaced_by` (§13).
- Смена модели под сценарий или смена когорты — **версионная граница класса
  `execution_versions`** (условия измерения, commodity-слой по A2), а **не**
  `methodology_version`: блокирует межцикловый diff с пометкой `execution_change`,
  обязателен `golden-full` (§13, §7.4).
- Смена **оси стратегии краула** (`crawl_strategy`/`crawl.rate_limit` — операционные дефолты
  секции `crawl` в `run.yaml`) — версионная граница класса `execution_versions`
  (`execution_change`), **не** `methodology_version` (§13): словарь
  `docs/status-dictionary.yaml` несёт и методологические, и операционные перечисления, а класс
  энума определяется его семантикой, не файлом-носителем.
- Смена **семантики правил** (словарь статусов, scoring-политика, absence/сателлитная/
  evidence-модель, определения Coverage/Comparability/sensitivity) — версионная граница
  класса `methodology_version`: `methodology_change`, обязательны `golden-full` **и
  пере-аннотация golden** (§13).

## Чего НЕ делать

- Не добавлять предметную терминологию в `core/`.
- Не делать failed статусом; не выводить N/A автоматически.
- Не придавать якорю вес/влияние на статусы (только порядок/фокус, anchor-blind).
- Не реализовывать «LLM с веб-поиском» внутри оценщика.
- Не обходить CAPTCHA, платные/авторизационные барьеры и закрытые разделы: coHound работает
  **только с публичными сайтами**. Для публичных сайтов honest/hybrid-рендер штатного
  JS-challenge публичной страницы — исполнение механизма самой страницы, а не обход (§10).
- Не придавать запрошенному `ranking_policy` силу переопределять sensitivity-демотирование
  (запрошено ранжирование, вычислено демотирование — допустимое расхождение, `requested` vs `computed`).
- Не мутировать иммутабельные индексные сборки: новый раунд/SERP → **новая** сборка; правка на месте запрещена.
- **Не возвращать чтение env для модели оценщика.** `COHOUND_LLM_MODEL`/`COHOUND_LLM_DECODER`
  для Ш6 — не вход: модель оценщика объявлена (`--llm-model`/`deploy.llm.<slot>.model_id`),
  иначе ошибка. Возврат env-чтения вернёт «удобство», за которое платят провенансом:
  `run_id` перестанет описывать модель, которая реально работала (I21/§7.4).
  Env-чтение в разведке — не образец для копирования: там выход не evidence.
- **Не расширять scope за объявленные хосты.** Поддомен в scope только если объявлен
  (seed-корень продукта или ключ `domain_profiles.yaml`); `www.` эквивалентен. Это
  `execution_change` и повторный краул: измерено — `edu.postgrespro.ru` дал 563 элемента
  из 682 на корне, объявившем только родительский хост, и корень документации продукта
  не получил ни одного своего элемента. Не подменять `Frontier.host_in_scope` на
  `_host_matches`: там семантика наследования **профиля**, а не право тратить бюджет корня.
- **Не менять ось краула «на месте».** `crawl_strategy`, `crawl.rate_limit`,
  `crawl_config_digest` — поля `EXECUTION_CONTRACT_FIELDS`; `max_depth`/`max_documents`/
  `exclude_*`/`robots`/`document_level` входят в них **через** `crawl_config_digest`
  (`CrawlLimits.as_dict()`). Это оси **контракта**, а не провенанс: их смена даёт новый
  `run_id` (`execution_change`), а не правку текущего прогона. Иначе два разных корпуса
  окажутся одним измерением.
- **Не выводить устройство извлечения из окружения.** `crawl.pdf_device` (`cpu|cuda`) —
  объявленная ось; значения `auto` нет намеренно, как и чтения модели оценщика из env.
  `cuda` при недоступном устройстве — `EXIT_USAGE` с числами, **не** тихий CPU: иначе
  `extractor_version` назовёт GPU, на котором прогон не исполнялся. Устройство входит в
  `extractor_version` (`docling-doc-3:<level>/<device>/docling-<build>`) и в
  `extractor_stack` — то есть меняет ключ `extract_cache` и `run_id`, один раз и явно.
  Замер (`docs/reports/measurements/2026-10-28-gpu-vs-cpu-extraction.md`): 8–12× на
  документной экстракции; второй воркер на одной карте — 1.10× при +2.3 GB RSS, поэтому
  окно там 1, и это следствие замера, а не потолок, выставленный на веру.
