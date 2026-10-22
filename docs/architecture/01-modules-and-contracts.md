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
из бывшего монолита `core/schemas.py` (удалён в T1.2) без изменения семантики. Добавляется:

```python
class Gate(StrEnum):        G1 = "G1"; G2 = "G2"; G3 = "G3"; G4 = "G4"
class CapabilityPolicy(StrEnum): RESOLVED_ONLY_V1 = "resolved-only-v1"
class UnknownPolicy(StrEnum):    EXCLUDED = "excluded"; UNKNOWN_AS_ZERO = "unknown_as_zero"
class RankingPolicy(StrEnum):    CAPABILITY_RANKED = "capability_ranked"
                                  COVERAGE_FALLBACK = "coverage_fallback"; UNRANKED = "unranked"
class Scenario(StrEnum):    EVALUATOR="evaluator"; SEARCH_ADAPTER="search_adapter"
                            DISCOVERY="discovery"; CONCEPT_SCOUT="concept_scout"
```

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
| `store`, `gate_store` | **core** | `extractor` | **edge** (по умолч.) |
| `chunker` | **core** | `gateway` | **core**→edge (транспорт) |

**Измерение локальности.** Порт с зоной `edge` реализуется дважды: реальный адаптер (живёт в
`edge/`) и **remote-proxy** (живёт в ядре, вызывает `gateway`). Профиль `local` инстанцирует
реальные адаптеры in-process; профиль `split` — remote-proxy + сервис `edge/`. Контракт порта при
этом **не меняется** (см. `07` §4). Порт `llm` — всегда `core` (I18: без маршрута в интернет).

### 3.1. `ports/llm.py` — оценщик (I18)

```python
@dataclass(frozen=True)
class LLMRequest:
    scenario: Scenario            # всегда EVALUATOR для этого порта
    prompt: str
    response_schema: type[BaseModel]   # LLMCellOutput
    seed: int | None
    temperature: float | None
    model_id: str
    model_version: str

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

Гибрид (RRF) собирается в `pipeline/evaluate.py` из этих двух портов — фьюжн не спрятан в адаптер,
чтобы профиль R0/R1/R2 был явным и логируемым.

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

### 3.5. `ports/{crawler,extractor,chunker,registry,store}.py`

```python
class CrawlerPort(Protocol):     def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage: ...
class ExtractorPort(Protocol):   version: str
                                 def extract(self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO) -> ExtractedDoc: ...
class ChunkerPort(Protocol):     version: str
                                 def chunk(self, doc: ExtractedDoc) -> list[Chunk]: ...
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
`ExtractedDoc(text, extractor_version, content_hash, page_count, error)` — нормализованный документ;
`error` — грациозная деградация адаптера (пустой `text` вместо исключения).
`Chunk(chunk_no, text, heading_path, context_prefix, content_hash)`.

**Дрейф контракта экстрактора (актуализация).** Реализованный адаптер
(`adapters/extract_document.py`, docling) предоставляет `extract_bytes(content: bytes, filename: str)`
и возвращает `ExtractedDoc(text, content_hash, extractor_version, page_count, error)`: формат
определяется расширением `filename`, а не полем `structure`. Порт-контракт `extract(page: RawPage)`
остаётся целевым (Page несёт `url`+`bytes`; формат выводится из `url`/content-type). Адаптер
приводится к порту тонкой обёрткой при реализации Ш5; `content_hash` считает адаптер (§10/I19).
**Путь вместо байтов (I19, execution_change).** Адаптер также предоставляет
`extract_path(path, filename_hint=...)`: извлечение по retained-файлу (`corpus_raw/{sha256}.bin`)
без копии тела в RAM — файл подставляется конвертеру по жёсткой ссылке/симлинку с нужным
суффиксом (копия — последний резерв). Страница несёт аддитивное поле `RawPage.source_path`;
`None` — прежнее поведение (байты в `content`). Ключ `extract_cache` остаётся хешем
retained-байтов (потоковое чтение 1 MiB), поэтому I19 не ослаблен.
`SUPPORTED_FORMATS` (pdf/docx/pptx/xlsx) — единственный реестр форматов; HTML — отдельный
`extract_html.py` (trafilatura). Поддержка Office опирается на `docling-slim[format-office]` без torch.

**Единая операционная БД (S6).** Контракт `StorePort` описывает **одну** SQLite-БД прогона
(`runs/{run_id}/cohound.db`); вариант «отдельная корпусная БД» отвергнут — при разнесении
`corpus_chunks`/`cells`/`cell_evidence` по разным файлам SQLite не может обеспечить FK между
`cell_evidence.source_id` и `corpus_chunks.chunk_id` (и `build_id`). Единая БД сохраняет FK и
целостность provenance; объём корпуса — не основание для разнесения (триггеры масштабирования —
§14/§12, а не смена модели данных).

### 3.6. `ports/embedder.py`, `ports/index_builder.py`, `ports/gate_store.py`

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
атомарно (существующая сборка не мутируется, §13). `GateStore` — носитель `gate_decisions`.

### 3.7. `ports/gateway.py` — транспорт core↔edge (07)

```python
class ContentGateway(Protocol):
    schema_version: str
    def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...
    # op ∈ {"fetch", "search", "registry", "capabilities", "health"}
    def capabilities(self) -> Mapping[str, Any]: ...   # версии edge-компонентов (в манифест)
```

Общий транспорт remote-прокси (`RemoteCrawler`/`RemoteSearch`/`RemoteRegistry`/`RemoteExtractor`).
**Гарантия (I19):** gateway возвращает контент/кандидатов, **не evidence**; ядро повторно
хеширует/валидирует входящее (`07` §5). Реализация — `adapters/remote_gateway.py` (httpx, mTLS,
таймауты, retry, лимит размера ответа). В профиле `local` не используется.

---

## 4. Сценарии (L1) — модули конвейера

Каждый модуль — функция(ы) над доменными объектами + портами (внедрены). Возвращают результат
фазы + (опц.) запись в store.

| Модуль | Сигнатура (эскиз) | Пишет | Инварианты |
|---|---|---|---|
| `normalize.py` | `load_inputs(run_yaml, package_dir) -> RunInputs` | — | — |
| `lint.py` | `lint(RunInputs) -> LintReport` (блокирует) | — | §4.2 |
| `plan.py` | `plan(RunInputs) -> RunPlan` (scope, роли, когорта, перенормировка) | runs | §1.6 |
| `discovery.py` | `discover(plan, search: SearchAdapterPort) -> list[SatelliteCandidate]` | passports | §4.3, G2 |
| `registry.py` | `registry_wave(plan, fetchers) -> list[Cell]` | cells, registry_facts | I1, R1 |
| `inheritance.py` | `inheritance_wave(plan, baselines) -> list[Cell]` | cells | I8 |
| `corpus.py` | `build_corpus(plan, crawler, extractor, chunker, embedder: EmbedderPort, index_builder: IndexBuilderPort, store) -> IndexBuild` | corpus_*, index_manifest | I17, I19 |
| `evaluate.py` | `evaluate(plan, llm: EvaluatorPort, vindex, lindex, store) -> list[Cell]` | cells, evidence | I1, I16, I17, I20 |
| `validate.py` | `validate(cell, chunks_by_id) -> ValidationResult` | cell_evidence | §10 |
| `review.py` | `review_queue(run_id, store) -> ReviewSet` | cells (reviewed) | I6, I7, I14 |
| `compute.py` | `compute(run_id, store) -> ScoreBundle` | runs (сводка) | I5, I11, I13 |
| `report.py` | `report(run_id, store, out_dir) -> Artifacts` | reports | I11 |
| `experiments.py` | `run_retrieval_experiment(profiles) -> ExperimentReport` | runs | R9, R10 |
| `gates.py` | `gate_g1/r2/r3/g4(...) -> GateDecision` | gate_decisions | G1–G4 |

**Запросы Ш6 (S13).** `evaluate.py` формирует на ячейку ≤3 запроса, приоритет детерминирован:
(1) `title`; (2) `concept_terms` (сначала ru, затем en, затем порядок в пакете). Состав запросов не
зависит от реализации/LLM (сохраняет симметрию I16).

**Общий контракт фазы:** `PhaseResult(status: ok|skipped|blocked|failed, phase, cells, log_entries, errors, skip_reason, pending)`.
`skip_reason` задан ⟺ `status == "skipped"` (инвариант `__post_init__`); `pending` — advisory
(никогда не статус). Конвейер (`pipeline/runner.py`) оркестрирует последовательность,
останавливается на `blocked`/`failed` и продолжает прогон на `skipped`.

---

## 5. Утилиты и сквозные типы (`util/`)

| Модуль | Содержимое |
|---|---|
| `ids.py` | `source_id(product, doc, chunk_no, build_id)`, парсинг; slug-валидация; привязка к `build_id` ИЛИ сверка `content_hash` при резолве (S5) |
| `hashing.py` | `content_hash(bytes)`, `prompt_hash(template, inputs)` |
| `logging.py` | структурированный лог (JSON lines) в `runs/{run_id}/logs/` |
| `clock.py` | `snapshot_date`, UTC-таймстемпы (тестируемость) |

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
| Добавить формат (docx/pptx/xlsx/…) | реестр `SUPPORTED_FORMATS` в `adapters/extract_document.py` | corpus |
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

---

## 10. Traceability и открытые вопросы

| Регламент | Покрытие |
|---|---|
| §3.1 компоненты | §2–§4 |
| §3.2 стек | `05-dependency-stack.md` |
| §4.1 словари | §2.1 |
| §7 LLM-контуры | §3.1, §3.4 |
| §11.6 CI | §1 (проверка импортов), §6 |
| I1–I23 | §6 матрица |

**Открытые вопросы к итерации 2:** точные сигнатуры фьюжна RRF; формат `AlignmentReport`;
структура `ProtocolExhaustion`; политика ретраев в `evaluate.py`; как передавать чанки в
`validate` (по `source_id` из store или из RetrievalResult).
