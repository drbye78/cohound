# coHound — архитектура: Итерация 1 — модули и контракты

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
адаптеры. `edge` **не зависит** от `pipeline`/`store`/`domain` (только `ports`+`adapters`+`config`+`util`),
поэтому не может писать в операционную БД или исполнять доменную логику.

**Следствие для §1.3:** `domain/`, `ports/`, `pipeline/` не содержат предметных терминов.
Адаптеры, работающие с реестрами/доменами, остаются нейтральными: специфика — в данных
(`packages/{case_id}/registries/*.yaml`, `packages/*`), не в коде.

---

## 2. Домен (L0) — модули и типы

### 2.1. `domain/enums.py`

Зеркало `docs/status-dictionary.yaml`. Существующие `StrEnum` (Status, SourceKind, …) перенесены
из бывшего монолита `core/schemas.py` (удалён в T1.2) без изменения семантики. Перечисления,
введённые в v2.1/итерациях 5–7 (состав — по `core/domain/enums.py`):

```python
class Gate(StrEnum):        G1 = "G1"; G2 = "G2"; G3 = "G3"; G4 = "G4"
class GateSource(StrEnum):  INTERNAL = "internal"; EXTERNAL = "external"
class GateVerdict(StrEnum): APPROVED = "approved"; REJECTED = "rejected"
class GatePolicy(StrEnum):  EXTERNAL = "external"; WAIVED = "waived"
class CapabilityPolicy(StrEnum): RESOLVED_ONLY_V1 = "resolved-only-v1"
class UnknownPolicy(StrEnum):    EXCLUDED = "excluded"; UNKNOWN_AS_ZERO = "unknown_as_zero"
class RankingPolicy(StrEnum):    CAPABILITY_RANKED = "capability_ranked"
                                  COVERAGE_FALLBACK = "coverage_fallback"; UNRANKED = "unranked"
class LLMScenario(StrEnum):  EVALUATOR="evaluator"; SEARCH_ADAPTER="search_adapter"
                             DISCOVERY="discovery"; CONCEPT_SCOUT="concept_scout"
```

**Перечисление → где используется** (кто читает/пишет; это карта, а не список «нового»):

| Перечисление | Значения | Где используется |
|---|---|---|
| `Status`, `CellState`, `Origin`, `EvidenceState`, `SatelliteState` | доменные статусы ячейки/сателлита | `domain/*`, `pipeline/{evaluate,review,compute}`, DDL/триггеры `store/sqlite.py` |
| `FailureKind` | **17**: 5 классов сбоя ячейки (`search_failed`, `source_unavailable`, `evaluation_failed`, `validation_failed`, `parser_failed`) + 11 наблюдаемых исходов краула (`not_found`, `robots_disallow`, `attempts_exhausted`, `redirect_out_of_scope`, `dlp_blocked`, `quarantined`, `extraction_timeout`, `payload_too_large`, `unsupported_binary`, `no_sources`, `coverage_missing`) + `partial_extraction` (документ получен, прочитана часть страниц — блокирующий класс корпуса) | `crawl_session_items.error_kind`, `corpus`; классы сбоя — `evaluate`/`report` |
| `CrawlSessionStatus` | `created`, `running`, `paused`, `cancelled`, `completed`, `partial`, `failed` (терминальные — четыре последних) | `crawl_sessions` (durable-сеанс, `02` §A.1) |
| `CrawlItemStatus` | `pending`, `running`, `fetched`, `failed`, `cancelled` | `crawl_session_items` (per-URL) |
| `ArchiveDescent` | `plan`, `full` — ось `crawl.archive_descent` | `corpus`/`archive_descent`, `crawl_config_digest` |
| `ArchiveDescentStatus` | `planned`, `descended`, `unreadable`, `budget_exhausted` | `archive_observations` (`09` §6.7.1) |
| `DocumentLevel` | `off`, `text`, `layout`, `layout_tables`, `full` — ось `crawl.document_level` | `CrawlLimits.as_dict()` включает её **безусловно** → `crawl_config_digest` → `run_id`: смена оси — `execution_change` (§13). Вторично она входит в наблюдаемый `extractor_version` (`docling-doc-3:{level}`) → новая индексная сборка; это следствие, а не класс изменения |
| `PdfPipeline` | `structured` (дефолт, docling), `text_layer` (текстовый слой pdfium) — ось `crawl.pdf_pipeline` / `--pdf-pipeline` | механизм, а не библиотека: `CrawlLimits.as_dict()` включает её **безусловно** → `crawl_config_digest` → `run_id` (`execution_change`, §13); `text_layer` не имеет уровней (`document_level` к нему неприменим) и входит в наблюдаемый `extractor_version` (`pdfium-text-1`) → новая сборка |
| `SkipReason` | `no_search_adapter`, `no_queries`, `no_urls`, `no_embedder`, `no_registry_spec`, `gate_delegated_external` | `PhaseResult.skip_reason` (инвариант `skip_reason ⟺ skipped`) |
| `Gate`, `GateSource`, `GateVerdict`, `GatePolicy` | G1–G4; `internal`/`external`; `approved`/`rejected`; `external`/`waived` | `gate_decisions`, `run.yaml:gates`, `hound gate`, `pipeline/{gates,runner}` |
| `OnboardingState` | `ready`, `pending_seeds`, `blocked`, `retired` | `state.db: product_state` (`09` §3.1) |
| `SeedRole`, `SeedSource`, `SeedState` | `primary`/`docs`/`satellite`/`host_root`; `add_product`/`discovery`/`migration`/`operator`; `enabled`/`disabled`/`invalid` | `state.db: seed_set` (`09` §7) |
| `CrawlStrategy` | `auto`, `honest`, `hybrid` — ось «как используется браузер» | `FetchPlan.strategy`, `crawl_config_digest` |
| `FetchMode`, `ExtractionRoute` | `http`/`browser`; `auto`/`html`/`document` | `FetchPlan` (`ports/types.py`) |
| `RankingPolicy`, `CapabilityPolicy`, `UnknownPolicy` | политики расчёта/ранжирования | `domain/scoring`, `compute`, `sensitivity` |
| `LLMScenario` | `evaluator`, `search_adapter`, `discovery`, `concept_scout` | `ScenarioBinding` (I21); **не** поле запроса к LLM (см. §3.1) |

### 2.2. `domain/models.py`

Чистые Pydantic-модели (без I/O). Перенесены из бывшего `core/schemas.py` (удалён в T1.2) и
уточнены:

| Модель | Изменение против текущей |
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
def check_cell(cell: Cell) -> list[Violation]           # I1, I2, I2b, I4, I6, I7, I13
def check_evidence(ev: CellEvidence, cell: Cell) -> list[Violation]   # I10, I15, I19, I22
def check_inheritance(cell: Cell, baseline: BaselineRow) -> list[Violation]  # I8 + derived-evidence
def check_derived_evidence(ev: CellEvidence, upstream: UpstreamRef) -> list[Violation]  # I1, I10, I15, I19
def check_satellite(sat: Satellite) -> list[Violation]   # I9
def check_unknown_exhaustion(cell: Cell, exhaustion: ProtocolExhaustion | None,
                             *, required=REQUIRED_EXHAUSTION_CONTOURS) -> list[Violation]  # I3 (§5, v2.1)
```

`REQUIRED_EXHAUSTION_CONTOURS = ("corpus", "serp", "registry", "inheritance")` — обязательные контуры
**полного** протокола исчерпания strict-`❓` (v2.1, §5). `check_unknown_exhaustion` фиксирует
отсутствие протокола/`termination_reason`/неполный перечень контуров как нарушение `I3`, но это
**аномалия**, а не смена статуса ячейки: `❓` остаётся `❓` (I12).

`check_inheritance` проверяет не только mode (I8): **унаследованный ✅/⚠️ обязан нести
derived-evidence** (§4.2 регламента) — `evidence_type: derived`, `source_kind: inheritance`, с
резолвом `source_url` (I10/I15) и `content_hash` (I19). Отсутствие или нерезолвимость
derived-evidence → violation (иначе I1 нарушается на унаследованной ячейке). `check_derived_evidence`
инкапсулирует этот контракт (наследование и сателлиты).

`Violation(code, message, field)` — типизированная ошибка; `code` — `"I2b"` и т.п. Это позволяет
CI и ревью-эшелону ссылаться на инвариант машинно.

### 2.4. `domain/scoring.py`

```python
def resolved_capability(cells, params, weights, policy) -> Score        # I5 (failed вне числителя)
def evidence_coverage(cells, params, weights, *, universe=None) -> Coverage  # §6/B-3a; I5/I7/I13
def failure_rate(cells) -> float
def comparability(cohort, per_product_coverage, alignment: AlignmentReport) -> Comparability
def sensitivity(cells, params, weights, axes, *,
                requested_ranking=RankingPolicy.CAPABILITY_RANKED,
                ranking_epsilon=0.0) -> SensitivityReport            # §6, K6/B-1
def module_scores(cells, params, weights, *, universe=None) -> list[ModuleScore]
def publishability_profile(cells, params, weights, *, coverage, universe=None, ...) -> PublishabilityProfile  # A1: класс + разложение неизвестности
```

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

Каждый порт имеет декларативный атрибут **зоны** (где исполняется его реальный адаптер):

| Порт | Зона | Порт | Зона |
|---|---|---|---|
| `llm` (evaluator) | **core** | `crawler` | **edge** |
| `vector_index`, `lexical_index` | **core** | `search` | **edge** |
| `index_builder`, `embedder` | **core** | `registry` | **edge** |
| `store`, `gate_store` | **core** | `extractor` | **edge** (по умолч.; `extractor_zone` объявлено, но не подключено — экстрактор всегда в ядре, D-9) |
| `chunker`, `state`, `container` | **core** (спуск в контейнеры исполняется в ядре; читатель инъектируется как `Ports.container`) | `poller`, `crawl_executor` | **edge** |
| `discovery_llm` | **core** (инференс; без веб-тула) | `gateway` | **core**→edge (транспорт) |

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

@dataclass(frozen=True)
class LLMResponse:
    output: LLMCellOutput
    raw: str
    prompt_hash: str
    decoder: str                  # "xgrammar" | "guidance" | ...
    usage: TokenUsage

class EvaluatorPort(Protocol):
    def complete(self, req: LLMRequest) -> LLMResponse: ...
```

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
    text: str | None              # raw запрос
    vector: list[float] | None
    top_k: int                    # = retrieval_cutoff_K

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
    def shard_ids(self, product_id: str) -> list[str]: ...
```

`RetrievedChunk` несёт `content_hash` и `build_id`, поэтому evidence и `prompt_hash` привязаны к
конкретной сборке; резолв `source_id` сверяет их (I19).

**Усечение запросов (S13).** Число запросов на ячейку ≤3, приоритет детерминирован:
(1) `param.title`; (2) `concept_terms` в фиксированном порядке (ru, затем en, затем порядок в
пакете). Это исключает зависимость состава запросов от реализации/LLM и сохраняет симметрию (I16).

### 3.3. `ports/lexical_index.py` (I16)

```python
class LexicalIndexPort(Protocol):
    engine_version: str
    def search(self, query: str, product_ids: list[str], top_k: int,
               lang: str) -> list[RetrievedChunk]: ...
```

Гибрид (RRF) собирается в `pipeline/retrieve.py` (`rrf_fuse_chunks`, `retrieve`) из этих двух
портов — фьюжн не спрятан в адаптер, чтобы профиль R0/R1/R2 был явным и логируемым;
`evaluate.py` вызывает `retrieve`, а не собирает слияние само.

### 3.4. `ports/search.py` — search-адаптер (Ш5, I19/I23)

```python
@dataclass(frozen=True)
class SearchQuery:
    text: str; lang: str; limit: int; phase: SearchPhase

@dataclass(frozen=True)
class SearchResult:
    url: str; title: str; snippet: str; rank: int
    raw_bytes: bytes | None       # для retention в corpus_raw

class SearchAdapterPort(Protocol):
    adapter_kind: AdapterKind     # serp | llm_grounding
    provider: str
    def search(self, q: SearchQuery) -> list[SearchResult]: ...
    def fetch(self, url: str) -> RawPage: ...   # дозагрузка выбранных
```

**Гарантия I19:** выход — только URL+байты; превращение в evidence происходит в Ш5 (chunk+hash).
Порт не возвращает «доказательство», только контент. Вызовы логируются вызывающим pipeline
в `search_log` (I23).

### 3.5. `ports/{crawler,crawl_errors,extractor,chunker,registry,store}.py`

`ports/crawl_errors.py` объявляет таксономию сбоев краула отдельным модулем, а не в
`crawler.py`: пять потребителей (`crawl_http`/`crawl_browser`/`wiring`/`edge/__main__`/
`crawler_factory`) не используют сам `CrawlerPort`, и импорт рядом с протоколом тянул бы
`ports.types` ради имени исключения. Страж — ast-скан в `tests/selfcheck.py`, который
падает на возврате импорта из `core.adapters.crawl_scrapy`.

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
а не полем `structure`. Композит объявляет `supports_source_path = True` — он читает
локальные байты по `RawPage.source_path` и потому пригоден для членов контейнеров
(`adapters/remote_extractor.py` объявляет `False`: шов не имеет доступа к локальному пути,
и `corpus` это проверяет). `content_hash` считает адаптер (§10/I19).

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
атомарно (существующая сборка не мутируется, §13). `GateStore` — носитель `gate_decisions`
(объявлен **в `ports/store.py`**, рядом с `StorePort`; `core/ports/gate_store.py` —
transitional re-export-шим `GateDecision`/`GateStore` для обратной совместимости импортов,
см. §9). `GateDecision` несёт `decision_id`, `run_id`, `gate`, `target`, `actor`, `decided_at`,
`notes`, а также интеграционную часть: `subject_digest`, `source` (`internal|external`),
`external_ref`, `verdict` (`approved|rejected`).

### 3.7. `ports/gateway.py` — транспорт core↔edge (07)

```python
class ContentGateway(Protocol):
    schema_version: str
    def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...
    # op ∈ {"fetch", "search", "registry", "capabilities", "health"}
    def capabilities(self) -> Mapping[str, Any]: ...   # версии edge-компонентов (в манифест)
```

Общий транспорт remote-прокси (`RemoteCrawler`/`RemoteSearch`/`RemoteRegistry`/`RemoteExtractor`/
`RemoteCrawlExecutor`).
**Гарантия (I19):** gateway возвращает контент/кандидатов, **не evidence**; ядро повторно
хеширует/валидирует входящее (`07` §5). Реализация — `adapters/remote_gateway.py` (httpx, mTLS,
таймауты, retry, лимит размера ответа). В профиле `local` не используется. Полный перечень
операций шва — 8 POST (`fetch`, `search`, `registry`, `crawl/{submit,status,cancel,collect}`,
`crawl/poll`) и 2 GET (`capabilities`, `health`), схемы — `contracts/seam/`, тела — `07` §3.

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
`store/state_sqlite.py` (`STATE_SCHEMA_VERSION = "4"`). Схема и семантика — `09` §4.

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

**Запросы Ш6 (S13).** `evaluate.py` формирует на ячейку ≤3 запроса, приоритет детерминирован:
(1) `title`; (2) `concept_terms` (сначала ru, затем en, затем порядок в пакете). Состав запросов не
зависит от реализации/LLM (сохраняет симметрию I16).

**Гейты как фазы (дельта после мержа гейтов).** `PHASES` — 15 фаз: гейты идут **отдельными
фазами** (`g1` после `plan`, `g2` после `discovery`, `g3` между `review` и `compute`, `g4`
между `compute` и `report`), `GATE_PHASES` связывает имя фазы с доменным `Gate`.
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
| `fusion.py`, `retrieval_eval.py` | RRF-слияние; метрики ретривала (hit@K, MRR, recall@K) |
| `agreement.py` | согласие аннотаторов: Cohen's κ (§11.5) |
| `cache.py` | глобальный content-addressed кэш внешних вызовов (R7, ADR 0016) |
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
| I3, I12 | domain/invariants (`check_unknown_exhaustion`, `check_cell`) + report-контур | review/report | unit (review/report) |
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

**CI-формулировка (K7):** `tests/selfcheck.py` покрывает I1/I2/I2b/I4/I6/I7/I13 и Р-2 — не
«I1–I23» целиком. Остальные инварианты обеспечиваются структурно и проверяются юнит-тестами по
слоям (I3/I3b/I12/I14, I16–I21, I23) и грепп-тестом нейтральности.

**I10/I15 (K7):** SQL обеспечивает только `source_id NOT NULL`; детерминированный резолв
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
| `core/schemas.py` (монолит) | `domain/enums.py` + `domain/models.py` + `domain/invariants.py` + `domain/r2.py` | ✅ выполнено (T1.2); монолит **удалён** |
| `core/db.py` DDL/триггеры | `store/sqlite.py` (DDL) + `ports/store.py` (контракт) | ✅ выполнено (T1.3); монолит **удалён** |
| `tests/selfcheck.py` | `tests/domain/*`, `tests/store/*` | частично (обёртка selfcheck остаётся) |

Ранее использовавшиеся transitional re-export-шимы `core/schemas.py` и `core/db.py` **удалены**:
импорты идут напрямую из `core.domain` и `core.store.sqlite`. Последовательность миграции и её
привязка к майлстоунам — `06-implementation-strategy.md` (§работа W1). Новые правки не должны
возвращать эти модули (проверяется в `tests/selfcheck.py::_shims_removed`).

**Исключение — `core/ports/gate_store.py`.** Это **действующий** transitional re-export-шим
(14 строк): канонический `GateStore` объявлен в `core/ports/store.py`, доменный
`GateDecision` — в `core/domain/models.py`, а шим реэкспортирует их для обратной
совместимости импортов `from core.ports.gate_store import …`. Удалять его можно только
вместе с правкой импортов; пока он существует, `ports/gate_store.py` **не** следует
описывать как место объявления порта (см. §3.6).

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

**Открытые вопросы к итерации 2:** точные сигнатуры фьюжна RRF; формат `AlignmentReport`;
структура `ProtocolExhaustion`; политика ретраев в `evaluate.py`; как передавать чанки в
`validate` (по `source_id` из store или из RetrievalResult).
