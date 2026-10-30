# coHound — архитектура: Итерация 1 — модули и контракты

> **Статус на 2026-10-29** (HEAD `d5a537a`): §10 закрыт полностью (5 из 5); §9 закрыт
> полностью — миграция `tests/selfcheck.py` сверена построчно (94 двойника / 8 дописанных
> тестов / 9 структурных гейтов). У каждого пункта маркер ✅/⬜/➖/❔ с доказательством
> `путь:строка`.

**Документ:** `docs/architecture/01-modules-and-contracts.md`
**Статус:** принято (ревью-гейт @oracle пройден 2026-10-08)
**Опирается на:** `00-overview.md` (слои, драйверы **AD1–AD11**)
**Источник истины:** `cohound-spec-v2.md` (регламент v2.1)
**Смежное:** `05-dependency-stack.md` (пины/лицензии), `06-implementation-strategy.md` (план).

Задача итерации — зафиксировать **границы модулей**, **контракты портов** (типы данных на стыках)
и **правила зависимостей**. Алгоритмы — итерация 2; конфиг/CLI — итерация 3.

---

## 1. Правила зависимостей (формально)

Пакет `core.*` разбит на слои; разрешённые импорты задаются матрицей. CI проверяет импорты
(статически — через `ast`, без исполнения).

| Из \ В | domain | ports | pipeline | adapters | store | config | cli | edge | util |
|---|---|---|---|---|---|---|---|---|---|
| **domain** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| **ports** | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| **pipeline** | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ |
| **adapters** | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ |
| **store** | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ❌ | ✅ |
| **config** | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ |
| **cli** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| **edge** | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ |
| **util** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |

**Композиция (wiring):** `cli` и `edge` — два **composition root**. `cli` (Z0) соединяет `pipeline`
с адаптерами (ручной DI, без фреймворка — A1); `edge` (Z1/DMZ, `07`) собирает **только веб-адаптеры**
и публикует их через gateway. `pipeline` принимает порты как аргументы, никогда не инстанцирует
адаптеры. `edge` **не зависит** от `pipeline`/`store`/`cli`: не пишет в операционную БД и не
исполняет доменную логику. `domain` для `edge` разрешён и нужен — схемы шва описывают оси
извлечения (`DocumentLevel`/`PdfPipeline`/`PdfDevice`) теми же перечислениями
(`core/edge/extract_service.py:59`, `core/edge/seam_schema.py:41`); дублировать словарь значило бы
дать ему разойтись с доменом.

**Кто это проверяет.** Матрица — не декларация, а гейт: `tests/selfcheck.py:948` `_import_graph`
разбирает импорты **статически** (`ast`, без исполнения), включая резолв relative-импортов
(`_resolve_relative_import:992`); нарушение — пара вне `matrix[layer]` (`_check_edge:1009`), то
есть проверка по принципу allow-list, отдельных «запретов» нет. Три гейта стоят рядом и не
выражаются матрицей: `_neutrality:1016` (§1.3), `_shims_removed:1123` (нет `core.schemas`/`core.db`),
`_edge_isolation:3007` (`core/edge/` не импортирует `store`/`pipeline`/`cli` — по **всему** слою, а
не по одному `server.py`: матрица разрешает `edge → domain`, поэтому запрет сформулирован по
запрещённым слоям, а не по файлам).

**Следствие для §1.3:** `domain/`, `ports/`, `pipeline/` не содержат предметных терминов.
Адаптеры, работающие с реестрами/доменами, остаются нейтральными: специфика — в данных
(`packages/{case_id}/registries/*.yaml`, `packages/*`), не в коде.

---

## 2. Домен (L0) — модули и типы

### 2.1. `domain/enums.py`

Зеркало `docs/status-dictionary.yaml`. Существующие `StrEnum` перенесены из бывшего монолита
`core/schemas.py` (удалён в T1.2) без изменения семантики. Состав ниже — **полный** (`core/domain/enums.py`,
66 классов); выборочный список здесь был бы картой, которая врёт умолчанием.

| Группа | Перечисления (значения) |
|---|---|
| Статус ячейки | `Status` (✅ ⚠️ ❌ ❓ N/A) · `CellState` (`pending`, `preliminary`, `evaluated`, `failed`, `reviewed`) · `RunMode` (`strict`, `preliminary`) |
| Доказательства | `EvidenceType` (`direct`, `derived`, `absence`) · `SourceKind` (`registry`, `inheritance`, `distribution`, `documentation`, `web`, `manual`) · `EvidenceState` (`verified`, `claimed`) |
| Отрицание и охват | `AbsenceRuleType` (`explicit`, `parameter_reference`, `distribution_absence`, `registry_absence`) · `SourceScope` (`exhaustive`, `partial`, `unknown`) · `DenialStrength` (`authoritative`, `explicit`, `bounded`, `unobserved`, `not_stated`, `instrument_failed`) · `DenialClaim` (`explicit`, `not_stated`) |
| Сбои | `FailureKind` — **17**: 5 классов сбоя ячейки (`search_failed`, `source_unavailable`, `evaluation_failed`, `validation_failed`, `parser_failed`) + 11 наблюдаемых исходов краула (`not_found`, `robots_disallow`, `attempts_exhausted`, `redirect_out_of_scope`, `dlp_blocked`, `quarantined`, `extraction_timeout`, `payload_too_large`, `unsupported_binary`, `no_sources`, `coverage_missing`) + `partial_extraction` (документ получен, прочитана часть страниц — блокирующий класс корпуса) |
| Пропуск фазы | `SkipReason` — **7**: `no_search_adapter`, `no_queries`, `no_urls`, `no_embedder`, `no_registry_spec`, `gate_delegated_external`, `gate_not_blocking` (последний — не блокирующий гейт: фаза идёт дальше, в `gate_decisions` ложится waiver-строка `actor=policy`) |
| Краул | `CrawlSessionStatus` — **8**: `created`, `running`, `paused`, `cancelled`, `completed`, `partial`, `interrupted`, `failed`; терминальных **пять** (`core/cli/context.py:1648`) · `CrawlItemStatus` (`pending`, `running`, `fetched`, `failed`, `cancelled`) · `CrawlStrategy` (`auto`, `honest`, `hybrid` — «как используется браузер») · `FetchMode` (`http`, `browser`) · `SearchMode` (`sync`, `async`) · `StaleReason` (`new_build`, `prompt_changed`, `catalog_changed`, `model_changed`) |
| Извлечение | `ExtractionRoute` (`auto`, `html`, `document`) · `DocumentLevel` (`off`, `text`, `layout`, `layout_tables`, `full`) · `PdfPipeline` (`structured`, `text_layer`) · `PdfDevice` (`cpu`, `cuda`) |
| Контейнеры | `ArchiveDescent` (`plan`, `full` — ось `crawl.archive_descent`) · `ArchiveDescentStatus` (`planned`, `descended`, `unreadable`, `budget_exhausted`) · `ContainerEncryption` (`encrypted`, `not_encrypted`, `unknown` — три состояния, а не булево: «предикат молчит» неотличим от «не зашифрован») |
| Гейты | `Gate` (G1–G4) · `GateSource` (`internal`, `external`) · `GateVerdict` (`approved`, `rejected`) · `GatePolicy` (`external`, `waived`) |
| Политики и ранжирование | `RankingPolicy` (`capability_ranked`, `coverage_fallback`, `unranked`) · `CapabilityPolicy` (`resolved-only-v1`) · `UnknownPolicy` (`excluded`, `unknown_as_zero`) · `ComparabilityLevel` (`HIGH`, `MEDIUM`, `LOW`, `INSUFFICIENT`, `cross-cycle`) · `PublishabilityClass` (`PUBLISHABLE`, `PUBLISHABLE_WITH_CAVEAT`, `ANALYTICAL_ONLY`, `NOT_COMPARABLE`) · `UnknownCause` (`not_stated`, `unobserved_scope`, `instrument_failed`, `pending`, `unspecified`) · `ExcludedCellClass` (`not_applicable`, `preliminary`, `unresolved_state`) · `RankingRestriction` (`none`, `forbidden_within_band`) · `UncertaintyProvenance` (`declared`, `measured_in_this_run`, `not_declared`) |
| LLM и поиск | `LLMScenario` (`evaluator`, `search_adapter`, `discovery`, `concept_scout`; **не** поле запроса к LLM — §3.1) · `EndpointKind` (`self_host`, `api`) · `AdapterKind` (`serp`, `llm_grounding`) · `Decoder` (`none`, `json_object`, `json_schema`) · `SearchKind` (`retrieval`, `external`) · `SearchPhase` (`discovery`, `corpus`, `serp`, `scout`) · `TerminationReason` (`budget_exhausted`, `results_exhausted`, `sufficient_evidence`, `not_found`, `error`) · `Cap` (`none`, `partial`, `ok`, `parameter_rule`) |
| Инкремент и состояние | `OnboardingState` (`ready`, `pending_seeds`, `blocked`, `retired`) · `SeedRole` (`primary`, `docs`, `satellite`, `host_root`) · `SeedSource` (`add_product`, `discovery`, `migration`, `operator`) · `SeedState` (`enabled`, `disabled`, `invalid`) · `ProductMode` (`new`, `full`, `incremental`) · `RefreshPolicy` (`auto`, `revalidate`, `refresh`, `offline`) · `ProductScope` (`full`, `product_only`) · `IncrementalKind` (`fetch`, `revalidate`, `reuse`) · `ReuseKind` (`live`, `revalidated`, `reused`, `stale`) · `ChangeClass` (`none`, `content_change`, `cohort_expansion`, `cohort_reduction`, `execution_change`, `methodology_change`) · `RunScope` (`full`, `only_failed`, `product_only`) |
| Пакет и эталон | `Origin` (`in_house`, `third_party`, `upstream`, `community`, `unknown`) · `Relationship` (`bundled`, `integrated`, `certified`, `recommended`, `external`) · `InheritanceMode` (`direct`, `conditional`, `blocked`) · `ParamType` (`fact`, `showcase`) · `Importance` (Кр, Вж, Баз, Опц) · `Archetype` (A, B, C) · `AnchorRelation` (`anchor`, `analog`) · `GoldenTier` (`fast`, `full`) |

**Почему `interrupted` терминален.** Протухший `running` при открытии сеанса становится
`interrupted`: тело краула не финализировано, поэтому это не `failed` и не `cancelled`. Без него в
множестве терминальных `--follow` ждал бы вечно сеанс, который уже никто не исполняет
(`core/cli/context.py:1648`).

**Мёртвые значения в словаре.** `SkipReason` объявляет 7 значений, но производятся **пять**
(`no_search_adapter`/`no_queries` — `core/pipeline/phases.py:976,981`, `gate_delegated_external`/
`gate_not_blocking` — `core/pipeline/runner.py:608,552`, `no_embedder` — на уровне CLI,
`core/cli/commands/experiment.py:284`);
`no_urls` и `no_registry_spec` не производятся ничем (`grep` по `core/` пуст). Пустой план
URL сейчас выражается не пропуском фазы, а zero-source причиной продукта (`ZERO_SOURCE_NO_URLS_ENTRY`,
`core/pipeline/corpus_coverage.py:184`) — это другая ось наблюдения, и подменять одну другой нельзя. Значения
оставлены в словаре как объявленные, но выдавать их за работающие механизмы нельзя.

**Оси, которые входят в `run_id` (и почему).** `DocumentLevel`, `PdfPipeline`, `PdfDevice`,
`CrawlStrategy` попадают в `crawl_config_digest`/`extractor_stack` → `run_id`: их смена —
`execution_change` (§13), а не правка текущего прогона. Причина у всех одна: ось меняет текст,
который прочитан, то есть корпус и доказательства. У `PdfDevice` **нет значения `auto`**
намеренно: при недоступном устройстве прогон отказывает с числами (`EXIT_USAGE`), а не подменяет
устройство молча — иначе `extractor_version` назвал бы GPU, на котором прогон не исполнялся
(`docs/architecture/05-dependency-stack.md`).

### 2.2. `domain/models.py`

Чистые Pydantic-модели (без I/O). Перенесены из бывшего `core/schemas.py` (удалён в T1.2); колонка
«что добавила миграция» — **историческая справка** о разнице с монолитом, а не утверждение о текущем
дереве: as-built состав моделей читается в `core/domain/models.py`.

| Модель | Что добавила миграция (историческая справка) |
|---|---|
| `PackageManifest`, `ModuleDef`, `RoleDef` | + `package_version` |
| `CatalogRow` | без изменений (11 колонок) |
| `BaselineRow` | без изменений |
| `GoldenRow` | + `tier` (fast/full) |
| `Cohort`, `CohortEntry` | без изменений |
| `Passport`, `Satellite`, `Upstream`, `Citation` | без изменений |
| `RunConfig`, `Focus` | + `ranking_policy`, `languages`, `ranking_epsilon`, `settings_provenance` (v2.1, §6/B-1, A3) |
| `Cell`, `LLMCellOutput`, `LLMEvidence` | без изменений |
| **`RunManifest`** | **новое**: агрегирует все версии (§9, §13); + `ranking_epsilon`, `settings_provenance` (v2.1, §13/B-7, A3) |
| **`CellEvidence`** | **новое**: типизированный evidence (сейчас dict) |
| **`SearchLogEntry`** | **новое**: строка `search_log` |
| **`ScenarioBinding`** | **новое**: `scenario → {model_id, version, ...}` (I21) |
| **`ProtocolExhaustion`** | **новое**: машиночитаемый протокол исчерпания (§5) |
| **`RegistryFact`** | **новое**: реестровый факт (R1) |

### 2.3. `domain/invariants.py`

Выносит проверки из `Cell._enforce_invariants` в чистые функции — чтобы переиспользовать в
pipeline, БД-слое и selfcheck независимо от Pydantic:

```python
def check_cell(cell: Cell) -> list[Violation]           # I1, I2, I2b, I4, I6/I7, I13
def check_evidence(ev: CellEvidence, cell: Cell | None = None) -> list[Violation]   # I10, I15, I19, I22
def check_inheritance(cell: Cell, baseline: BaselineRow,
                      evidence: list[CellEvidence] | None = None) -> list[Violation]  # I8 + derived-evidence
def check_derived_evidence(ev: CellEvidence, upstream: UpstreamRef) -> list[Violation]  # I1, I8, I10, I15, I19
def check_satellite(sat: Satellite) -> list[Violation]   # I9
def check_absence_coverage(cell: Cell, coverage: CorpusCoverage | None) -> list[Violation]  # I2c
def check_unknown_exhaustion(cell: Cell, exhaustion: ProtocolExhaustion | None = None) -> list[Violation]  # I3
```

**I3 — «назови причину незнания», а не «пройди четыре контура».** Прежняя редакция требовала
полного протокола исчерпания (`corpus`+`serp`+`registry`+`inheritance`); это был **рецепт
реализации**, а не инвариант: он делал честное `❓` недостижимым для параметра, который нельзя
доказать поиском, и ничего не говорил о том, **почему** отсутствие не установлено. Действующее
требование (вариант 2, `core/domain/invariants.py:74`): `reviewed_by` **ИЛИ**
(`evidence_scope.denial_strength ∈ {unobserved, not_stated}` **и** непустая `reason`).
`EXHAUSTION_CONTOURS` остался справочником того, что вообще наблюдается, а
`REQUIRED_EXHAUSTION_CONTOURS` — **deprecated-алиас**: параметр `required` игнорируется.
Протокол исчерпания — провенанс поиска (`core/pipeline/exhaustion.py`), а не условие вердикта.
Для исторических строк без `evidence_scope` работает прежнее правило по `exhaustion`: старые
прогоны обязаны остаться читаемыми. Нарушение I3 — **аномалия**, а не смена статуса: `❓`
остаётся `❓` (I12).

**I2c — класс отрицания против наблюдённого охвата** (`check_absence_coverage`, `:286`;
исполняется в Ш6: `core/pipeline/evaluate.py:1368` `_enforce_absence_coverage` →
`check_absence_coverage` на `:1418`, применяется к ячейке на `:2092`, токен причины
`absence_coverage_unsatisfied` — `evaluate.py:176`). Условие `❌` зависит от **класса**, а не от
одной полноты краула: `explicit` (цитата вендора) и `authoritative` (энумерация) неполнотой не
отменяются — это свидетельство, а не вывод из полноты корпуса; `bounded` требует
`scope_acceptance` (принятия риска человеком); `unobserved`/`not_stated` дают `❓`. Нарушение →
ячейка **`❓`** (`not_stated`, I12), а не `failed`: отсутствие принятия риска — процедурное
условие, а не отказ прибора (I4), и ячейка обязана остаться в карте. Снятый `❌` не публикует и
протокол отсутствия (`absence_evidence=[]`).

`check_inheritance` проверяет не только mode (I8): **унаследованный ✅/⚠️ обязан нести
derived-evidence** (§4.2 регламента) — `evidence_type: derived`, `source_kind: inheritance`, с
резолвом `source_url` (I10/I15) и `content_hash` (I19). `evidence=None` трактуется как
«derived-evidence не предоставлено» (fail-closed): отсутствие или нерезолвимость → violation,
иначе I1 нарушается на унаследованной ячейке. `check_derived_evidence` инкапсулирует этот
контракт (наследование и сателлиты) и проверяет I1, I8, I10, I15, I19.

`Violation(code, message, field)` — типизированная ошибка; `code` — `"I2b"` и т.п. Это позволяет
CI и ревью-эшелону ссылаться на инвариант машинно.

### 2.4. `domain/scoring.py`

```python
def resolved_capability(cells, params, weights, policy) -> Score        # I5 (failed вне числителя)
def evidence_coverage(cells, params, weights, *, universe=None) -> Coverage  # §6/B-3a; I5/I7/I13
def failure_rate(cells) -> float
def comparability(cohort, per_product_coverage, alignment: AlignmentReport | None = None) -> Comparability
def sensitivity(cells, params, weights, axes=None, *,
                requested_ranking=RankingPolicy.CAPABILITY_RANKED,
                ranking_epsilon=0.0) -> SensitivityReport            # §6, K6/B-1
def module_scores(cells, params, weights, *, universe=None) -> list[ModuleScore]
def publishability_profile(cells, params, weights, *, coverage, universe=None, ...) -> PublishabilityProfile  # A1: класс + разложение неизвестности
def endpoint_host_notices(*, rnd_endpoint, hosts) -> list[dict]        # V6: замечания о хостах LLM
```

`publishability_profile` принимает ещё пороги разложения (`unknown_share_threshold`,
`weak_denial_share_threshold`, `coverage_floor`), признаки `comparability_level`/
`comparability_reasons`/`cross_cycle`, наблюдение `corpus_coverage` и объявление стенда
(`rnd_endpoint`, `llm_hosts`) — то есть профиль публикуемости есть функция **наблюдённого**, а не
одних ячеек. `endpoint_host_notices` выносит отдельно то, что видно только по хостам: прогон на
стенде вне периметра не становится производственной публикацией от того, что ячейки сошлись.

**Coverage по вселенной scope (v2.1, §6/B-3a).** При `universe` (множество пар `(product_id,
param_id)` — декартово произведение scope-параметров на продукты когорты; строится
`compute.scope_universe(catalog, products)`) знаменатель — `Σ w(universe)`: `pending`/отсутствующая
ячейка учтены как неразрешённые, `N/A`/`preliminary` — вне (I7/I13), `failed` — внутри (I5).
`Coverage` несёт `universe`/`completed`/`defined`; `universe=0` → `value=None`, `defined=False`
(UNDEFINED). Legacy-режим (`universe=None`) сохранён. Итоговый ранг — join `requested × computed =
min` по порядку `capability_ranked ⊐ coverage_fallback ⊐ unranked` (K6/C-1): запрос `unranked` не
повышается, `ranking_epsilon` задаёт порог неразличимости пар.

`scope_universe` — публичная точка `pipeline/compute.py` (фазы Ш8/отчёты/`sensitivity` строят
вселенную из неё). Чистые функции `domain/scoring.py` остаются без I/O: загрузка ячеек/каталога — в
`pipeline`.

### 2.5. `domain/r2.py`, `domain/anchors.py`

Перенос текущих `apply_r2` и `load_dictionary/assert_dictionary_integrity`.
`anchors.py` добавляет `resolve_source_url(source_id, resolver: SourceResolver)` — **детерминированный**
резолв (I10/I15): `source_id` кодирует `product_id`+`doc_id`+`chunk_no` (+ привязку к `build_id`),
резолв — по индексу, без сети. `SourceResolver` — reader-протокол, **объявленный внутри `domain/`**
(не в `ports/`), поэтому матрица зависимостей L0 ↛ L2 соблюдается; реализация приходит из `store/`.
При резолве обязательна сверка `content_hash` evidence с чанком (I19), иначе source_id невалиден.

---

## 3. Порты (L2) — контракты

Все порты — `typing.Protocol` (структурная типизация, без наследования — упрощает адаптеры).
Каждый порт в отдельном файле. Ниже — сигнатуры (псевдо-Python, полные доказы — docstring).

### 3.0. Классификация портов по доверенной зоне (07)

Таблица ниже — **карта проектирования**: где живёт реальный адаптер порта и что инстанцирует
каждый профиль. Декларативного атрибута «зоны» у портов **нет** (`grep zone core/ports/` пуст), и
это осознанно: зону выбирает composition root по профилю, а не сам порт — иначе контракт порта
менялся бы вместе с профилем развёртывания.

| Порт | Зона | Порт | Зона |
|---|---|---|---|
| `llm` (evaluator) | **core** | `crawler` | **edge** |
| `vector_index`, `lexical_index` | **core** | `search` | **edge** |
| `index_builder`, `embedder` | **core** | `registry` | **edge** |
| `store`, `gate_store` | **core** | `extractor` | **core** (см. ниже) |
| `chunker`, `state`, `container` | **core** (спуск в контейнеры исполняется в ядре; читатель инъектируется как `Ports.container`) | `poller`, `crawl_executor` | **edge** |
| `discovery_llm` | **core** (инференс; без веб-тула) | `gateway` | **core**→edge (транспорт) |

**`extractor_zone` объявлено, но не действует (D-9).** Значения `edge|core|separate`
(`core/config/deploy.py:86`, поле `:592`) валидируются, но выбора реализации не делают:
`RemoteExtractor` поднимается **по профилю** `split` (`core/cli/wiring.py:3033`), а не по этой оси.
Экстрактор всегда в ядре; в `split` ядро получает байты через шов `extract/{upload,submit,collect,abort}`.
Ось оставлена как объявление намерения — описывать её как работающий механизм нельзя.

**Измерение локальности.** Порт с зоной `edge` реализуется дважды: реальный адаптер (живёт в
`edge/`) и **remote-proxy** (живёт в ядре, вызывает `gateway`). Профиль `local` инстанцирует
реальные адаптеры in-process; профиль `split` — remote-proxy + сервис `edge/`. Контракт порта при
этом **не меняется** (см. `07` §4). Порт `llm` — всегда `core` (I18: без маршрута в интернет).

### 3.1. `ports/llm.py` — оценщик (I18)

```python
@dataclass(frozen=True)
class LLMRequest:
    prompt: str
    response_schema: type[BaseModel]   # LLMCellOutput
    model_id: str
    model_version: str = ""
    seed: int | None = None
    temperature: float | None = None
    max_tokens: int | None = None     # потолок генерации (V5); None — дефолт провайдера

@dataclass(frozen=True)
class LLMResponse:
    output: LLMCellOutput
    raw: str
    prompt_hash: str
    decoder: str                  # none | json_object | json_schema
    usage: TokenUsage

class EvaluatorPort(Protocol):
    def complete(self, req: LLMRequest) -> LLMResponse: ...
```

`max_tokens` в порту не «настройка вида»: без потолка сервер выбирает сам, и обрезание приходит
как `finish_reason=length` с пустым текстом — неотличимое от невалидного JSON. `finish_reason`
читает **адаптер** (`core/adapters/llm_local.py`), в контракт порта он не вынесен: это диагностика
провайдера, а не часть ответа домена.

**Почему в `LLMRequest` нет `scenario`.** Сценарий (`LLMScenario`) **не** передаётся запросом:
он задаётся фазой, из которой пришёл вызов (I21 — маршрутизация модель→сценарий
детерминирована фазой, а не решением LLM и не содержимым запроса). Поле-«сценарий» позволяло
бы вызывающему подменить маршрут; вместо этого `scenario → {model_id, model_version, seed,
temperature}` фиксируется таблицей `ScenarioBinding` в манифесте (`plan`/`runner`), а
`LLMRequest` несёт уже разрешённые значения.

**Гарантия I18:** у порта **нет** параметров `tools`/`web_search`; интерфейс физически не выражает
веб-поиск. Адаптер `llm_local.py` подключается к [OI]-совместимому endpoint корпоративного облака;
без тулов (I18).
Search-enabled модели обслуживаются **другим портом** (`ports/search.py`).

### 3.2. `ports/vector_index.py` (I17)

```python
@dataclass(frozen=True)
class VectorQuery:
    product_ids: list[str]        # продукт-партиции (I17): запрос не выходит за них
    top_k: int                    # = retrieval_cutoff_K
    text: str | None = None       # raw запрос
    vector: list[float] | None = None

@dataclass(frozen=True)
class RetrievedChunk:
    chunk_id: str; product_id: str; text: str
    content_hash: str; heading_path: str
    build_id: str; score: float          # provenance единицы ретривала (S4/S5)

@dataclass(frozen=True)
class RetrievalResult:
    chunks: list[RetrievedChunk]         # fused top-K
    profile: str; cutoff_K: int; build_id: str

class VectorIndexPort(Protocol):
    build_id: str
    dim: int
    def search(self, q: VectorQuery) -> list[RetrievedChunk]: ...
    def search_many(self, queries: Sequence[VectorQuery]) -> list[list[RetrievedChunk]]: ...
    def shard_ids(self, product_id: str) -> list[str]: ...

class CompositionViewPort(Protocol):          # вид когорты: product_id → build_id
    composition_id: str
    members: Mapping[str, str]
    def build_id_for(self, product_id: str) -> str: ...
    def product_ids(self) -> list[str]: ...

def batched_search(index: VectorIndexPort,
                   queries: Sequence[VectorQuery]) -> list[list[RetrievedChunk]]
```

`RetrievedChunk` несёт `content_hash` и `build_id`, поэтому evidence и `prompt_hash` привязаны к
конкретной сборке; резолв `source_id` сверяет их (I19).

**`search_many` и `batched_search` — батч-путь, а не второй контракт.** Порт объявляет
`search_many`, но реализации без своего батч-пути его не пишут: `batched_search` берёт метод
через `getattr` и откатывается на цикл `search` (`core/ports/vector_index.py:89`). Смысл батча —
прочитать партицию **один раз на набор запросов**; там, где реализация этого не умеет, вызовы
эквивалентны поштучным, поэтому расширение контракта не ломает ни одну существующую реализацию
(структурная типизация). `pipeline` вызывает только `batched_search` — одна точка вызова.

**`CompositionViewPort` — отдельный порт, а не поле индекса.** Композиция индексных партиций
(`pipeline/composition.py`) живёт в манифесте, а не в конкретной сборке: `build_id_for(product_id)`
отвечает, какую сборку читать для продукта, не раскрывая `VectorIndexPort` деталей размещения
(`09` §8.1–§8.4).

**Усечение запросов (S13).** Число запросов на ячейку ≤3, приоритет детерминирован:
(1) `param.title`; (2) `concept_terms` в фиксированном порядке (ru, затем en, затем порядок в
пакете). Это исключает зависимость состава запросов от реализации/LLM и сохраняет симметрию (I16).

### 3.3. `ports/lexical_index.py` (I16)

```python
class LexicalIndexPort(Protocol):
    engine_version: str
    def search(self, query: str, product_ids: list[str], top_k: int,
               lang: str | None = None) -> list[RetrievedChunk]: ...
```

`lang` — ось **запроса**, не обещание языкового firewall: реализация вправе её не использовать
(анализатор может нести оба языка сразу — так делает tantivy-адаптер после C8: одно поле, оба
стеммера). Поэтому дефолт `None`, а не `"ru"`: «язык не задан» — честное состояние, и порт не
подменяет его русским по умолчанию.

Гибрид (RRF) собирается в `pipeline/retrieve.py` (`rrf_fuse_chunks`, `retrieve`) из этих двух
портов — фьюжн не спрятан в адаптер, чтобы профиль R0/R1/R2 был явным и логируемым;
`evaluate.py` вызывает `retrieve`, а не собирает слияние само.

### 3.4. `ports/search.py` — search-адаптер (Ш5, I19/I23)

```python
@dataclass(frozen=True)
class SearchQuery:
    text: str; lang: str = "ru"; limit: int = 10; phase: str = "corpus"

@dataclass(frozen=True)
class SearchResult:
    url: str; title: str = ""; snippet: str = ""; rank: int = 0
    raw_bytes: bytes | None = None       # для retention в corpus_raw

@dataclass(frozen=True)
class SearchTiming:                      # наблюдённое время одного вызова (S4)
    wait_seconds: float                  # паузы между опросами async-операции — простой прогона
    request_seconds: float               # HTTP-вызовы провайдера, включая backoff внутри них
    polls: int; measured: bool; from_cache: bool
    def merged_with(self, other: SearchTiming) -> SearchTiming: ...   # сумма наблюдений продукта

class SearchAdapterPort(Protocol):
    adapter_kind: str                    # serp | llm_grounding
    provider: str
    def search(self, q: SearchQuery) -> list[SearchResult]: ...
    def fetch(self, url: str) -> RawPage: ...   # дозагрузка выбранных
```

`phase` и `adapter_kind` объявлены **строками**, а не перечислениями (`core/ports/search.py:19,88`):
порт — commodity-стык, и адаптер не обязан импортировать доменный словарь, чтобы назвать свою
фазу. `SearchTiming` разделяет ожидание внешней async-операции и время самих запросов, потому что
это разные факты: первое — простой прогона, второе — стоимость вызова; смешав их, нельзя ответить,
что именно дорого. `merged_with` складывает наблюдения одного продукта, а `measured`/`from_cache`
берёт **конъюнкцией**: сумма, где хоть одно слагаемое не наблюдалось, не выдаётся за измеренную.

**Гарантия I19:** выход — только URL+байты; превращение в evidence происходит в Ш5 (chunk+hash).
Порт не возвращает «доказательство», только контент. Вызовы логируются вызывающим pipeline
в `search_log` (I23).

### 3.5. `ports/{crawler,crawl_errors,extractor,chunker,registry,store}.py`

`ports/crawl_errors.py` объявляет таксономию сбоев краула отдельным модулем, а не в
`crawler.py`: большинство потребителей (`crawl_http`/`crawl_browser`/`crawl_scrapy`,
`wiring`, `edge/__main__`, `crawler_factory`, `cli/package/commands`, а также
`pipeline/crawl_probe_driver` и `util/robots`) не используют сам `CrawlerPort`, и импорт
рядом с протоколом тянул бы `ports.types` ради имени исключения — то есть адаптер зависел
бы от тяжёлого модуля портов из-за одного исключения.

Здесь же живёт **причина отказа** (`refusal_reason`: `robots_disallow` /
`robots_unavailable`), а не только класс сбоя: «сайт запретил» и «ленту не удалось
прочитать» имеют одно следствие (обход не выполняется) и разные причины, и охват обязан
их различать (RFC 9309 §2.3.1.4).

Страж — ast-скан в `tests/selfcheck.py`, который падает на возврате импорта из
`core.adapters.crawl_scrapy` (таксономия не уезжает обратно в тяжёлый адаптер).

```python
class CrawlerPort(Protocol):     def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage: ...
class ExtractorPort(Protocol):   version: str
                                 def extract(self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO) -> ExtractedDoc: ...
class ChunkerPort(Protocol):     version: str
                                 def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]: ...
class RegistryFetcherPort(Protocol):
    registry_id: str
    def fetch(self, product: ProductRef, scope: dict) -> list[RegistryFact]: ...
class StorePort(Protocol):       # §4.4
    def upsert_products(...); def create_run(...); def save_cell(...)
    def add_evidence(...); def log_search(...); def save_exhaustion(...)
    def persist_manifest(...); def iter_exhaustions(run_id) -> Iterator[ProtocolExhaustion]
    def read_manifest(...); def iter_cells(run_id) -> Iterator[Cell]
```

`FetchPlan(fetch, extract, strategy, wait_for, rate_limit)` — единый value-объект плана (§3.5):
оси `fetch` (`http`/`browser` — **нужен ли браузер вообще**), `extract`
(`auto`/`html`/`document`) и **третья ось стратегии краула** `strategy` (`auto`/`honest`/`hybrid` —
**как** используется браузер при browser-capable адаптере; D4), плюс опц. JS-условие `wait_for` и
per-host диапазон `rate_limit`. Резолвится в pipeline (`pipeline/fetch_plan.py`); адаптеры план не
строят.
`RawPage(url, bytes, fetched_at, fetch_mode, lang)` — retention-единица (ось получения — из
`FetchPlan`; D2).
`ExtractedDoc(text, content_hash, extractor_version, page_count, error, family)` — нормализованный
документ; `error` — грациозная деградация адаптера (пустой `text` вместо исключения).
`family: str | None` (`"markdown"` / `"text"` / `"epub"`) — аддитивное поле: по нему
`adapters/chunker_family.py` выбирает чанкер (композитная версия правил нарезки —
`md-heading-3+text-para-2`, `composite_chunker_version`); `None` — HTML/документный путь.
`Chunk(chunk_no, text, heading_path, context_prefix, content_hash)`.

**Экстрактор — as-built (три семейства + EPUB, один реестр форматов).** Порт приведён к
контракту в composition root: `cli/wiring.py` собирает `_LocalExtractor` — композит, который
реализует `ExtractorPort` структурно и диспетчеризует по `route`/семейству:
`ExtractionRoute.HTML` → `extract_html` (trafilatura), `DOCUMENT` → `extract_document`
(docling), `AUTO` — текстовое семейство (`extract_text`, plain text/markdown **без**
trafilatura) → EPUB (`extract_epub`, stdlib OCF, главы через тот же HTML-экстрактор) →
документный суффикс/тип → docling → иначе HTML. Порядок «текст/EPUB раньше фолбэка в HTML»
осознан: этот фолбэк убивал `.txt` в `parser_failed` (замер 22.10 — `09` §6.8).
Адаптер `extract_document` предоставляет `extract_bytes(content, filename)` и
`extract_path(path, filename_hint=...)`; формат определяется расширением/`Content-Type`,
а не полем `structure`. Флаг `supports_source_path` объявляют **все** реализации, включая
`adapters/remote_extractor.py:214`: локальный композит читает байты по `RawPage.source_path`, а
remote получает их отдельной операцией шва (`extract/upload`) — то есть путь как таковой ему не
нужен, но контракт «работаю без тела в RAM» он выполняет. `corpus` проверяет флаг
(`core/pipeline/corpus.py:828`) прежде чем отдать локальный путь: адаптер без флага получил бы
путь, которого не существует в его зоне. `content_hash` считает адаптер (§10/I19).

**Единственный источник истины по форматам — `core/util/document_formats.py`**
(`DOCUMENT_SUFFIXES` + `MARKDOWN_SUFFIXES`/`PLAIN_TEXT_SUFFIXES`/`TEXTUAL_SUFFIXES`,
предикаты `textual_family`/`is_epub`/`is_terminal_artifact`); он лежит в `util`, а не в
адаптере, потому что решение «пропустить документ при `document_level=off`» принимает
`pipeline`, которому запрещено импортировать адаптеры. `SUPPORTED_FORMATS` в
`adapters/extract_document.py` — **производный** словарь (`{суффикс: layout|office}` из
`DOCUMENT_SUFFIXES`), а не отдельный реестр; `.epub` в него не входит. Поддержка Office
опирается на `docling-slim[format-office]` без torch.

**Путь вместо байтов (I19, execution_change).** `extract_path(path, filename_hint=...)` —
извлечение по retained-файлу (`corpus_raw/{sha256}.bin`) без копии тела в RAM: файл
подставляется конвертеру по жёсткой ссылке/симлинку с нужным суффиксом (копия — последний
резерв). Страница несёт аддитивное поле `RawPage.source_path`; `None` — прежнее поведение
(байты в `content`). Ключ `extract_cache` остаётся хешем retained-байтов (потоковое чтение
1 MiB), поэтому I19 не ослаблен.

**Единая операционная БД (S6).** Контракт `StorePort` описывает **одну** SQLite-БД прогона
(`runs/{run_id}/cohound.db`); вариант «отдельная корпусная БД» отвергнут — при разнесении
`corpus_chunks`/`cells`/`cell_evidence` по разным файлам SQLite не может обеспечить FK между
`cell_evidence.source_id` и `corpus_chunks.chunk_id` (и `build_id`). Единая БД сохраняет FK и
целостность provenance; объём корпуса — не основание для разнесения (триггеры масштабирования —
§14/§12, а не смена модели данных).

### 3.6. `ports/embedder.py`, `ports/index_builder.py`, `ports/store.py` (`GateStore`)

```python
class EmbedderPort(Protocol):
    model_id: str
    dim: int
    def embed(self, texts: list[str]) -> "Matrix": ...   # батч; локальная модель

class IndexBuilderPort(Protocol):
    def build(self, chunks: list[Chunk], vectors: Matrix, meta: IndexMeta) -> str: ...  # → build_id
    def commit(self) -> None: ...                        # атомарная фиксация шардов (.npy/tantivy)

class GateStore(Protocol):                               # персистенция решений G1–G4 (S9)
    def save_gate_decision(self, d: GateDecision) -> None: ...
    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...
```

`IndexBuilderPort` — **единственный** писатель `.npy`-шардов и tantivy; `pipeline` не работает с
файлами индекса напрямую. `build()` возвращает иммутабельный `build_id`, `commit()` фиксирует запись
атомарно (существующая сборка не мутируется, §13). `GateStore` — носитель `gate_decisions`,
объявлен **в `core/ports/store.py`**, рядом с `StorePort`; отдельного `ports/gate_store.py` больше
нет (шим удалён 29.10, импорты идут напрямую). `GateDecision` живёт в `core/domain/models.py` и
несёт `decision_id`, `run_id`, `gate`, `target`, `actor`, `decided_at`, `notes`, а также
интеграционную часть: `subject_digest`, `source` (`internal|external`), `external_ref`,
`verdict` (`approved|rejected`).

### 3.7. `ports/gateway.py` — транспорт core↔edge (07)

```python
class ContentGateway(Protocol):
    schema_version: str
    def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def capabilities(self) -> Mapping[str, Any]: ...   # версии edge-компонентов (в манифест)
```

Общий транспорт remote-прокси (`RemoteCrawler`/`RemoteSearch`/`RemoteRegistry`/`RemoteExtractor`/
`RemoteCrawlExecutor`).
**Гарантия (I19):** gateway возвращает контент/кандидатов, **не evidence**; ядро повторно
хеширует/валидирует входящее (`07` §5). Реализация — `adapters/remote_gateway.py` (httpx, mTLS,
таймауты, retry, лимит размера ответа). В профиле `local` не используется.

**Перечень операций шва — 12 POST + 2 GET** (`contracts/seam/openapi.json`, allowlist сервера
`core/edge/server.py:141-156`):

| Метод | Пути |
|---|---|
| POST | `/v1/fetch`, `/v1/search`, `/v1/registry`, `/v1/crawl/{submit,status,cancel,collect,poll}`, `/v1/extract/{upload,submit,collect,abort}` |
| GET | `/v1/capabilities`, `/v1/health` |

`extract/*` — **аддитивное** расширение (B1): `SEAM_SCHEMA_VERSION` остаётся `"1"`. Байты
передаются отдельной операцией `upload`, а не телом `submit`: `max_body_bytes` = 1 MiB, а
допущенный артефакт — до 250 MB, и он не влезает в один запрос ни в одном кодировании. Идентичность
артефакта — `content_hash`, поэтому повтор `upload` идемпотентен по построению.

### 3.8. `ports/poller.py` — опрос фидов/каналов

```python
class PollPort(Protocol):
    version: str
    def poll(self, url: str, *, kind: str, params: Mapping[str, Any],
             plan: FetchPlan) -> list[FeedEntry]: ...
    def validate_params(self, kind: str, params: Mapping[str, Any]) -> list[str]: ...
```

`kind` — `"feed"` (XML: RSS/Atom/RDF) либо `"channel"` (JSON/JSON-lines/список URL);
`params` (`limit`/`since`/`token_env`) валидируются **до** опроса. Выход — **кандидаты-ссылки,
не evidence** (I19): каталог не ретейнится, запись попадает в корпус только обычным путём
через `corpus_raw`→hash→index. Реализации: `adapters/feed_poller.py` (`LocalFeedPoller`),
remote-путь — операция шва `crawl/poll` (`07` §3). В `Ports` порт живёт как карта
`kind → PollPort` (`Ports.pollers`).

### 3.9. `ports/container.py` — спуск в контейнеры

```python
class ContainerPort(Protocol):
    version: str
    def available(self) -> bool: ...
    def list_members(self, path: str) -> ContainerListing: ...
    def open_member(self, path: str, member_path: str) -> Iterator[bytes]: ...
```

**Инвариант 1 — листинг не есть извлечение.** `list_members` читает **только индекс**
контейнера (zip central directory / заголовок 7z / заголовок RAR); `size` члена — заявленный
размер из индекса, а не измеренный распаковкой. **Инвариант 2 — стримящий `open_member`:**
член читается порциями, а не целиком в RAM. `ContainerListing` несёт `format`, `members`,
`solid`, `encryption`, `truncated`, `unreadable` (видимый пропуск, а не «пустой контейнер»).
`encryption` — **три состояния** (`encrypted` | `not_encrypted` | `unknown`), а не булево:
«предикат молчит» (читатель не умеет определять шифрование) неотличим от «не зашифрован»,
если значение двоичное, и тогда отказ чтения члена приходит без причины. `unknown` **не**
отсеивает членов (незнание ≠ «зашифрован») и **не** проходит молча: состояние видно в
прогнозе, а отказ чтения получает приписку `encryption_unverified`.
**Гарантия I19:** байты члена становятся контентом только через corpus_raw→hash→index.
Реализации — `adapters/container_{zip,7z,rar}.py` + диспетчер `container_any.py`
(`container_for(path)`); в `pipeline` приходит как `Ports.container`, а не импортом адаптера.
Политика допуска членов — `util/container_policy.py`; оси — `crawl.archive_*` (`09` §6.7).

### 3.10. `ports/state.py` — durable per-product состояние

```python
class ProductStatePort(Protocol):
    def get_product_state(self, product_id) -> ProductStateRow | None: ...
    def upsert_product_state(self, row) -> None: ...
    def last_snapshot(self, product_id, *, scope="full") -> SnapshotRow | None: ...
    def record_snapshot(self, row) -> None: ...
    def reconcile_seed_set(self, product_id, seeds) -> None: ...
    def seed_rows(self, product_id) -> list[SeedRow]: ...
    # url_state / document_version / chunk_meta / index_partition / index_composition — аналогично
```

Хранилище — `state/{package_id}/state.db` (**отдельная** БД на пакет, вне `runs/`: переживает
прогоны). `upsert_*`/`record_*` идемпотентны (`INSERT … ON CONFLICT DO UPDATE`);
`reconcile_seed_set` **не удаляет** строки: seed, исчезнувший из авторского файла, →
`disabled` + `retired_at`. Реализация — `store/state_store.py` (`SqliteStateStore`) поверх DDL
`store/state_sqlite.py` (`STATE_SCHEMA_VERSION = "5"`, `core/store/state_sqlite.py:41`). Схема и семантика — `09` §4.

### 3.11. `ports/crawl_executor.py` — исполнение краула (split)

```python
class CrawlExecutorPort(Protocol):
    version: str
    def submit(self, items: list[CrawlWorkItem], *, session_id, run_id,
               profile, idempotency_key) -> CrawlSubmitResult: ...
    def status(self, handle: str) -> CrawlProgress: ...
    def cancel(self, handle: str) -> CrawlProgress: ...
    def collect(self, handle, *, cursor=None, limit=None) -> CrawlBatch: ...
```

Транспорт-агностичен: локальный планировщик и edge-планировщик DMZ взаимозаменяемы.
`submit` идемпотентен по `idempotency_key` — **по набору работ**, не по сеансу;
`CrawlSubmitResult.accepted` — сигнал back-pressure. `collect` отдаёт **сырые байты** (I19).
При наличии `Ports.executor` фаза корпуса исполняет краул через него, а `crawler`/`crawlers`
не используются. Реализация — `adapters/remote_crawl_executor.py` (`EdgeCrawlExecutor`); op шва —
`/v1/crawl/{submit,status,cancel,collect}`.

### 3.12. `ports/discovery_llm.py` — LLM-разведка

```python
class DiscoveryLLMPort(Protocol):
    def complete(self, req: DiscoveryLLMRequest) -> DiscoveryLLMResponse: ...
```

Отдельный порт от оценщика: обслуживает разведку ландшафта/кандидатов
(`discovery`, `concept_scout`), выход — **кандидаты и гипотезы** (G2), не evidence.
Структурная граница I18 сохраняется: у порта нет `tools`/`web_search`. Модель этого
контура — не модель оценщика (`build_discovery_llm`; env `COHOUND_LLM_*` действует только
здесь, см. `02` §C.4).

---

## 4. Сценарии (L1) — модули конвейера

Каждый модуль — функция(ы) над доменными объектами + портами (внедрены). Возвращают результат
фазы + (опц.) запись в store.

| Модуль | Сигнатура (эскиз) | Пишет | Инварианты |
|---|---|---|---|
| `normalize.py` | `load_inputs(run_yaml, package_dir) -> RunInputs` | — | — |
| `lint.py` | `lint_run_inputs(inputs: RunInputs) -> LintReport` (блокирует) | — | §4.2 |
| `plan.py` | `plan(inputs: RunInputs) -> RunPlan` (scope, роли, когорта, перенормировка) | runs | §1.6 |
| `discovery.py` | `discover(plan, search: SearchAdapterPort) -> list[SatelliteCandidate]` | passports | §4.3, G2 |
| `registry.py` | `registry_wave(plan, fetchers) -> list[Cell]` | cells, registry_facts | I1, R1 |
| `inheritance.py` | `inheritance_wave(plan, baselines) -> list[Cell]` | cells | I8 |
| `corpus.py` | `build_corpus(plan, crawler, extractor, chunker, embedder: EmbedderPort, index_builder: IndexBuilderPort, store) -> IndexBuild` | corpus_*, index_manifest | I17, I19 |
| `retrieve.py` | `retrieve(...) -> RetrievalResult`; `rrf_fuse_chunks(...)` | — | I16, I17, I20 |
| `evaluate.py` | `evaluate_phase(...)` (внутри — `evaluate_cell`/`evaluate_batch`, `retrieve_for_cell`) | cells, evidence | I1, I16, I17, I20 |
| `validate.py` | `validate(cell, chunks_by_id) -> ValidationResult` | cell_evidence | §10 |
| `review.py` | `review_queue(run_id, store) -> ReviewSet` | cells (reviewed) | I6, I7, I14 |
| `compute.py` | `compute(run_id, store) -> ScoreBundle`; `scope_universe(catalog, products)` | runs (сводка) | I5, I11, I13 |
| `report.py` | `report(run_id, store, out_dir) -> Artifacts` | reports | I11 |
| `experiments.py` | `run_retrieval_experiment(profiles) -> ExperimentReport` | runs | R9, R10 |
| `gates.py` | `gate_g1/gate_g2/gate_g3/gate_g4(...) -> GateDecision` (общий `record_decision`) | gate_decisions | G1–G4 |

**Прочие модули (50).** Таблица выше — только сценарии фаз. Полный состав `core/pipeline/` — 65
файлов; остальные держат механизмы, которые фазы вызывают. Описание — первая строка докстринга
модуля, чтобы карта не расходилась с кодом.

| Группа | Модули |
|---|---|
| Краул | `crawl_frontier` (frontier обхода in-site ссылок и контрольные оси краула, D1/D2) · `crawl_session` (локальная оркестрация сеанса краула для фазы корпуса) · `crawl_sources` (резолвер цели краула по слоям источников) · `fetch_plan` (резолвер плана получения страницы, D2) · `crawl_probe` / `crawl_probe_driver` (ядро решений автоподбора осей краула и его read-only драйвер) · `archive_descent` (спуск в контейнеры — исполнение плана членов, B3) · `artifact_probe` (классификация артефакта **до** экстракции) · `artifacts_gap` (`artifacts_gap.yaml` — недоступные артефакты как вход следующего прогона) · `liveness` (свидетельство живости сеанса: `owner`, heartbeat, recovery; F1/Д-6) |
| Корпус и кэши | `extract_cache` (контент-адресуемый кэш экстракции, переживающий прогоны) · `eval_cache` (межпрогонный кэш вердиктов оценщика) · `embed_cache` (контент-адресуемый кэш векторов) · `partial_docs` (частичное извлечение — **числом**, а не прилагательным) · `composition` (композиция индексных партиций и её иммутабельный манифест) · `idempotency` (инвалидация и идемпотентность конвейера) |
| Идентичность | `digests` (канонические дайджесты идентичности прогона) · `run_identity` (цикл, объём, lineage) |
| Оценка | `prompt` (механическая компиляция промпта оценщика) · `exhaustion` (протокол исчерпания как провенанс поиска) · `evaluation_policy` (политика повтора оценщика — объявленное условие измерения, V3) · `measurement_uncertainty` (полоса неопределённости вердиктов) · `support_quality` (качество поддержки карты: два замера) · `evaluator_benchmark` (benchmark моделей оценщика при всём прочем фиксированном) · `extraction_eval` (extraction-golden и A/B экстракторов) · `adversarial` (adversarial-набор и регрессионный гейт) |
| SERP и поиск | `serp_loop` (append-only раунды сборок + инвалидация затронутых ячеек) · `search_log` (бюджет внешнего поиска и `search_log`) |
| Гейты и профиль | `gate_subject` (предметы решений G3/G4 и исходящий запрос на решение) · `profile_attestation` (аттестация профиля пакета-якоря для G1/G2; **design-only** — производителя в прогоне нет) · `precedence` (прецедент источника П>В) |
| Инкремент и состояние | `incremental` (планировщик инкрементального корпуса и ключи durable-субстрата) · `onboarding` (onboarding-префлайт продукта) · `state_sync` (запись durable per-product состояния по итогам прогона) · `manual_artifacts` (`sources/manual_artifacts.yaml` — объявленный вход байтов, которые не взял краулер) |
| Авторство пакета | `package_io` (I/O артефактов пакета) · `package_admin` (CRUD над пакетом) · `package_discovery` (базовый разведочный инференс) · `package_candidates` (кандидаты авторского применения) · `package_scout` (веб-заземлённая разведка пакета) · `package_add_product` (авторское добавление продукта в когорту) · `passport_scout` (черновик паспорта продукта) · `scout_common` (общие хелперы веб-разведки) |
| Эталон | `golden` (golden-харнесс и калибровка порогов) · `golden_status` (`golden_status.yaml` — объявленное состояние эталонного набора) |
| Наблюдаемость | `metrics` (метрики прогона) · `observability` (инструментирование фаз и запись метрик) · `resources` (F0 — измерительная база ресурсов: память, расписание экстракции, чанки, стадийные бакеты времени) |
| Оркестрация | `runner` (оркестратор фаз Ш1–Ш9, персистентное состояние, resume) · `phases` (сборка `PhaseFn`-замыканий) |

**Запросы Ш6 (S13).** `evaluate.py` формирует на ячейку ≤3 запроса, приоритет детерминирован:
(1) `title`; (2) `concept_terms` (сначала ru, затем en, затем порядок в пакете). Состав запросов не
зависит от реализации/LLM (сохраняет симметрию I16).

**Гейты как фазы (дельта после мержа гейтов).** `PHASES` — **15 фаз** в порядке исполнения
(`core/pipeline/runner.py:132-148`): `normalize`, `lint`, `plan`, `g1`, `discovery`, `g2`,
`registry`, `inheritance`, `corpus`, `evaluate`, `review`, `g3`, `compute`, `g4`, `report`.
`GATE_PHASES` (`:151-156`) связывает имя фазы с доменным `Gate`: `g1`→G1, `g2`→G2, `g3`→G3,
`g4`→G4; вложенность проверяется инвариантом (`:160-163`).
* **Что блокирует прогон.** `NON_BLOCKING_GATES = {G1, G2, G3}` (`core/pipeline/gates.py:79`):
  нет действующего решения — фаза идёт дальше, а в `gate_decisions` пишется строка-политика
  (`actor=policy`, `external_ref=waiver:policy:nonblocking:{gate}`), которая в следе манифеста
  читается как `state: not_required`. Блокируют только **G4** и **явный отказ**
  (`verdict=rejected`) по текущему предмету у любого гейта: отказ — решение, молчание внешней
  системы не выдаётся ни за отказ, ни за одобрение.
* G1/G2 — **внутренняя аттестация** содержимого профиля пакета-якоря. Рабочий путь —
  per-run строка `gate_decisions` (аудит-след и override). Второй путь — durable
  `profile_attestation` в `state/{package}/state.db` (`pipeline/profile_attestation.py`,
  `make_profile_gate` подключён в CLI и проверяется гейтом): механизм **объявлен, но
  производителя в прогоне нет** (design-only; строку пишут только тесты) — описывать его
  как работающий нельзя.
* G3/G4 — **точки интеграции**: coHound не решает ревью/публикацию, а экспортирует предмет в
  `runs/{run_id}/gates/{gate}-request.json` (`pipeline/gate_subject.py`, `subject_digest`) и
  **записывает** полученный вердикт. Устаревшее одобрение (предмет изменился) гейт не
  проходит; явный отказ блокирует отдельным сообщением.
* `run.yaml:gates.{g3,g4}: external|waived` (дефолт `external`): `waived` → фаза `skipped` с
  `skip_reason=gate_delegated_external` + waiver-строка в `gate_decisions` + `gate_waivers` в
  манифесте. Политика — процесс, а не условие измерения, поэтому в
  `EXECUTION_CONTRACT_FIELDS` её нет.
* `hound gate` требует `--subject-digest` (по умолчанию из request-файла); `--source external`
  требует `--external-ref` и `--notes`.

**Общий контракт фазы:** `PhaseResult(status: ok|skipped|blocked|failed, phase, cells, log_entries, errors, skip_reason, pending)`.
`skip_reason` задан ⟺ `status == "skipped"` (инвариант `__post_init__`); `pending` — advisory
(никогда не статус). Конвейер (`pipeline/runner.py`) оркестрирует последовательность
`PHASES` (15 фаз, гейты — отдельные фазы), останавливается на `blocked`/`failed` и продолжает
прогон на `skipped`.

**Resume (N4) — короткое замыкание только по `ok`.** Фаза с персистентным состоянием `ok`
пропускается; `skipped`/`failed`/`blocked` **пересчитываются** на каждом `run`/`resume`
(предпосылки могли измениться). Отсюда инвариант N4: фаза, завершившаяся `skipped`, обязана
быть **side-effect-free или идемпотентной** — иначе побочный эффект повторится на каждом
прогоне. `skipped ≠ ok`: в CLI-JSON это разные ключи (`resumed_skipped` — фазы,
короткозамкнутые персистентным `ok`, и `results[].status` — фактический статус фазы).
`skipped` **не трогает статусы ячеек** и не порождает `N/A` (I6/I7); персистится только
сам статус фазы, `skip_reason`/`pending` — нет.

**Изоляция ошибок — `run_isolated(items, fn)`** (`pipeline/runner.py`): прогоняет `fn` по
элементам, возвращает `(outputs, errors)`, собирая исключение элемента в строку ошибки.
Именно этот helper, а не `try/except` внутри каждой фазы, реализует «сбой продукта/документа
не рушит прогон»: фазы кладут `errors` в `PhaseResult.errors` и продолжают остальные
элементы. Исключение самой фазы собирается `Runner`'ом в `PhaseResult.errors` со статусом
`failed`.

---

## 5. Утилиты и сквозные типы (`util/`)

| Модуль | Содержимое |
|---|---|
| `ids.py` | `source_id(product, doc, chunk_no, build_id)`, парсинг; slug-валидация; привязка к `build_id` ИЛИ сверка `content_hash` при резолве (S5) |
| `hashing.py` | `content_hash(bytes)`, `prompt_hash(template, inputs)` |
| `logging.py` | структурированный лог (JSON lines) в `runs/{run_id}/logs/` |
| `document_formats.py` | **единственный источник истины по форматам**: `DOCUMENT_SUFFIXES`, `MARKDOWN_SUFFIXES`/`PLAIN_TEXT_SUFFIXES`/`TEXTUAL_SUFFIXES`, `textual_family`, `is_epub`, `is_terminal_artifact` (P5/P6b) |
| `document_probe.py` | классификация артефакта по голове файла (класс/формат/размер, `page_count` PDF по структуре) — без рендеринга |
| `container_policy.py` | политика спуска в контейнеры: допуск членов, бюджеты, прогноз (чистая логика, B2) |
| `memory.py` | `release_free_arenas` — `malloc_trim(0)` после документа; операционный рычаг, вне `EXECUTION_CONTRACT_FIELDS` (ADR 0022) |
| `rate_limit.py` | пейсинг и backoff краула (политика, независимая от адаптеров) |
| `robots.py`, `sitemap.py`, `html_links.py` | per-host `robots.txt`, парсер `sitemap.xml`, извлечение гиперссылок (stdlib) |
| `feed.py` | чистый парсер фидов/каналов (`FeedEntry`) для `PollPort` |
| `security.py`, `sanitize.py` | SSRF-защита URL и санитизация имён файлов; санитизация контента на границе экстракции |
| `text_utils.py` | многоязычные текстовые утилиты (RU/EN) |
| `retrieval_eval.py` | метрики ретривала (hit@K, MRR, recall@K). **RRF-слияния здесь нет**: `rrf_fuse_chunks` живёт в `pipeline/retrieve.py:98` — фьюжн вызывается из сценария, а не из утилиты, иначе профиль R0/R1/R2 перестал бы быть явным |
| `agreement.py` | согласие аннотаторов: Cohen's κ (§11.5) |
| `cache.py` | глобальный content-addressed кэш внешних вызовов (R7, ADR 0016) |
| `retry.py` | bounded retry/backoff с уважением `Retry-After` (S6) — вынесен из поискового адаптера: политика повторов общая, а провайдер — commodity |
| `redirects.py` | похоповая валидация редиректов для путей со своим HTTP-клиентом (R10) |
| `gpu.py` | единственная проба CUDA (`cuda_available`, объём карты) — общая для ядра и DMZ: матрица §1 запрещает `edge → pipeline`, и две копии разошлись бы молча |
| `index_layout.py`, `index_partition.py` | имена каталогов `index_store/`; read-only проверка продукт-партиций (09 §8.2) |
| `atomic_write.py` | атомарная запись файлов с сохранением режима |

`Clock` — **не** утилита: `Protocol` с `now_iso()` объявлен в `pipeline/runner.py` (внедряется
в `RunContext`, тестируемость); `snapshot_date` живёт в `domain/models.py`.

`prompt_hash` фиксируется: hash от (шаблон_версия + rule + absence_rule + отсортированные chunk_id).
Изменение шаблона → новый hash → ячейки инвалидируются (§13).

---

## 6. Матрица «инвариант → где обеспечивается»

| Инвариант | Обеспечено структурно | pipeline | Проверено тестом |
|---|---|---|---|
| I1, I2, I2b | domain/invariants (`check_cell`) | evaluate | selfcheck |
| **I2c** | domain/invariants (`check_absence_coverage`) | evaluate (`_enforce_absence_coverage`) | selfcheck (`_corpus_coverage_invariant`) + unit |
| I3, I12 | domain/invariants (`check_unknown_exhaustion`, `check_cell`) + report-контур | review/report | unit (`tests/domain/test_invariants.py`) + selfcheck (исторический путь) |
| I3b | domain/enums (`protocol: partial`) | review | unit (preliminary) |
| I4, I6, I7, I13 | domain/invariants + SQL-триггеры | review | selfcheck |
| I5, I11 | domain/scoring | compute | selfcheck |
| I8 | domain/invariants (`check_inheritance`, derived-evidence) | inheritance | selfcheck |
| I9 | domain/invariants (`check_satellite`) | discovery | selfcheck |
| I10, I15 | domain `SourceResolver`-резолв (код) + `source_id NOT NULL` | anchors/validate | selfcheck |
| I14 | review/promote-политика | review | unit (promote) |
| I16, I17, I20 | profile/K в `RunConfig`/манифесте | corpus/evaluate | experiments |
| I18 | порт без `tools` (структурно) + грепп | corpus/search | selfcheck + грепп |
| I19 | domain (`content_hash`), `IndexBuilderPort` | corpus/search | selfcheck |
| I21 | `ScenarioBinding`/манифест (таблица фаз) | plan/runner | selfcheck (манифест) |
| I22 | domain/invariants (`check_evidence`) | evaluate | selfcheck |
| I23 | SQL (`search_log.kind`) | corpus/search | selfcheck |

**CI-формулировка (K7).** `tests/selfcheck.py` (**111 утверждений** = 97 `check` + 14
`expect_raises`) покрывает инварианты I1/I2/I2b/**I2c**/I4/I6/I7/I13 и Р-2, а также
**структурные** гейты, у которых нет pytest-двойника: `_import_graph` (матрица §1),
`_neutrality` (§1.3), `_shims_removed`, `_edge_isolation`. Два гейта продублированы
pytest-тестами **предмета** — `_crawl_taxonomy_from_port` (`tests/test_crawl_error_taxonomy.py`,
включая подачу нарушающего файла) и `_seam_schema` (`tests/edge/test_seam_schema.py`) — и остаются
в selfcheck как гейты над деревом. Остальные инварианты
обеспечиваются структурно и проверяются юнит-тестами по слоям (I3/I3b/I12/I14, I16–I21, I23). Это
не «I1–I23 целиком» — и не должно им быть: гейт над деревом и юнит-тест отвечают на разные вопросы,
и копия гейта в pytest разошлась бы с оригиналом. Построчная сверка 29.10 (§9) показала: у 94 из
111 утверждений pytest-двойник есть, 8 дописаны, 9 — структурные по природе.

**I10/I15 (K7):** SQL обеспечивает `source_id NOT NULL` **кроме** `evidence_type='absence'`
(триггер `trg_evidence_source_id`, `core/store/sqlite.py:493`): у протокола отсутствия источника нет
по построению, и `NULL` честнее выдуманного нерезолвимого id. Детерминированный резолв
`source_id → source_url` — **код** (`domain/anchors.py` + `SourceResolver`), не триггер.

---

## 7. Реестровые фетчеры и нейтральность (§1.3)

Конкретные реестры (РРПО, ФСТЭК, …) **не кодируются в `core/`**. Механизм:

```
packages/{case_id}/
  registries/
    rrp.yaml        # endpoint, формат, маппинг полей, юрисдикция
    fstec.yaml
```

`adapters/registry_generic.py` читает YAML (config-driven: URL-шаблон, метод, парсер `json|html`,
JSONPath-маппинг полей) и реализует `RegistryFetcherPort`. Специфика реестра — чистые данные,
ядро остаётся нейтральным. Грепп-тест §1.3 не ловится, т.к. названий в `core/` нет.

**Состояние as-built:** механизм есть, **ни один пакет реестровых спецификаций сегодня не имеет**
(`packages/*/registries/` отсутствует, включая `_template`). Пустой `Ports.registry_fetchers` →
волна пуста, фаза `registry` завершается `ok` с нулём ячеек (`core/pipeline/phases.py:992`) — это
не ошибка и не «нет данных»: реестры просто не сконфигурированы. `SkipReason.NO_REGISTRY_SPEC`
объявлен, но конвейером **не производится** (`grep` по `core/pipeline/` пуст) — значение ждёт
своего производителя, и выдавать его за работающий механизм нельзя.

---

## 8. Точки расширения (как подменяется commodity — A2)

| Что меняем | Что трогаем | Что НЕ трогаем |
|---|---|---|
| LLM-провайдера | новый `adapters/llm_*.py` | domain, pipeline |
| Векторный индекс (Npy→Qdrant) | новый `adapters/vector_*.py` | pipeline/evaluate (тот же порт) |
| SERP-провайдера | новый `adapters/search_*.py` + YAML | pipeline |
| Экстрактор (docling→другой) | новый `adapters/extract_*.py` | corpus |
| Добавить формат (docx/pptx/xlsx/…) | `DOCUMENT_SUFFIXES` в `core/util/document_formats.py` (`SUPPORTED_FORMATS` выводится) | corpus |
| Добавить текстовое семейство / EPUB | `document_formats` + читатель `adapters/extract_*` + чанкер `chunker_text` | corpus |
| Экстрактор HTML (trafilatura→другой) | новый `adapters/extract_html.py` | corpus |
| Хранилище (SQLite→PG) | новый `store/*.py` | domain |
| Профиль развёртывания (`local`↔`split`) | `config/deploy.yaml` + wiring в `cli/`/`edge/` | domain, pipeline (порты те же) |
| Веб-адаптер в другую зону | новый remote-proxy в `adapters/remote_*.py` | domain, pipeline |

---

## 9. Что перенесено из прежнего кода (миграция)

| Было | Стало | Статус |
|---|---|---|
| `core/schemas.py` (монолит) | `domain/enums.py` + `domain/models.py` + `domain/invariants.py` + `domain/r2.py` + `domain/anchors.py` | ✅ сделано (T1.2; `core/domain/enums.py:1`, `core/domain/anchors.py:1`); монолит **удалён** — гейт `tests/selfcheck.py:1123` (`_shims_removed`) |
| `core/db.py` DDL/триггеры | `store/sqlite.py` (DDL) + `ports/store.py` (контракт) | ✅ сделано (T1.3; `core/store/sqlite.py:20` `DDL`); монолит **удалён** — тот же гейт `tests/selfcheck.py:1123` |
| `tests/selfcheck.py` | `tests/domain/*`, `tests/store/*` | ✅ **закрыто** (29.10) — построчная сверка всех **111 утверждений** (97 `check` + 14 `expect_raises`) с pytest-пирамидой: **94** имеют двойника (указан `путь:строка`), **8** держали покрытие в одиночку и дописаны тестами, **9** — структурные гейты над деревом, которым pytest-двойник противопоказан: `_import_graph`, `_neutrality`, `_shims_removed`, `_edge_isolation`, `_domain_all_complete`, `_ports_contracts`, `_ports_i18`, `_ports_type_reuse`, полнота CLI-поверхности. Дописанные: конструктор `CellEvidence` (I19/R1/I10 — прежний тест проверял `check_evidence` на `model_construct`, то есть в обход валидатора), `core/util/text_utils` (язык, ё→е, нарезка, RU-токены), `core/util/security` (SSRF/allowlist/traversal/slug), `core/adapters/lexical_bm25` (**единственный адаптер без тестов**), `DoclingDocumentExtractor.supports`, `MarkdownChunkerPort`↔`ChunkerPort`. Пиксель-в-пиксель дублировать оставшиеся 94 не нужно и вредно: гейт над деревом и юнит-тест отвечают на разные вопросы. selfcheck остаётся **автономным входом CI** (`scripts/ci.sh:50`), обёртка — `tests/test_selfcheck.py`; `tests/domain` — 12 файлов / 165 функций / 173 кейса, `tests/store` — 15 файлов / 189 функций / 194 кейса |

Ранее использовавшиеся transitional re-export-шимы `core/schemas.py` и `core/db.py` **удалены**:
импорты идут напрямую из `core.domain` и `core.store.sqlite`. Последовательность миграции и её
привязка к майлстоунам — `06-implementation-strategy.md` (§работа W1). Новые правки не должны
возвращать эти модули (проверяется в `tests/selfcheck.py::_shims_removed`).

**Шимов больше нет.** `core/ports/gate_store.py` удалён 29.10 вместе с единственным импортом
(`core/ports/__init__.py`): `GateStore` читается из `core.ports.store`, `GateDecision` — из
`core.domain`. Порядок CI, который держит этот класс ошибок: `scripts/ci.sh` — ruff (`:24`) →
mypy-гейт (`:32`) → `check_doc_anchors.py --strict` (`:46`) → `tests/selfcheck.py` (`:50`) →
pytest + `--cov` (`:78-88`).

---

## 10. Traceability и открытые вопросы

| Регламент | Покрытие |
|---|---|
| §3.1 компоненты | §2–§4 |
| §3.2 стек | `05-dependency-stack.md` |
| §4.1 словари | §2.1 |
| §7 LLM-контуры | §3.1 (оценщик), §3.4 (search-адаптер), §3.12 (разведка) |
| §11.6 CI | §1 (проверка импортов), §6 |
| I1–I23 | §6 матрица |
| `09` онбординг/инкремент | §3.10 (`ProductStatePort`), §3.9 (контейнеры), §4 (`run_isolated`, resume/N4) |

**Открытые вопросы к итерации 2 — закрыты все пять.** Точные сигнатуры фьюжна RRF —
`core/pipeline/retrieve.py:98` `rrf_fuse_chunks(…, rrf_k=60, top_k)`; формат `AlignmentReport` —
`core/domain/models.py:1375`; структура `ProtocolExhaustion` — `core/domain/models.py:1238`;
политика ретраев в `evaluate.py` — `core/pipeline/evaluate.py:1829` (`max_retries`), цикл `:2003`;
как передавать чанки в `validate` — **объектами из RetrievalResult**, а не выборкой из store:
`validate_output(*, chunks: Mapping[str, ChunkLike], …)` (`core/pipeline/validate.py:267`), словарь
строит вызывающий (`core/pipeline/evaluate.py:2131`).
