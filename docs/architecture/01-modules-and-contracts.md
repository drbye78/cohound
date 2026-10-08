# coHound — архитектура: Итерация 1 — модули и контракты

**Документ:** `docs/architecture/01-modules-and-contracts.md`
**Статус:** черновик проектирования (итерация 1 из 4 + ревью-гейт)
**Опирается на:** `00-overview.md` (слои, драйверы D1–D11)
**Источник истины:** `cohound-spec-v2.md` (регламент v2.0)

Задача итерации — зафиксировать **границы модулей**, **контракты портов** (типы данных на стыках)
и **правила зависимостей**. Алгоритмы — итерация 2; конфиг/CLI — итерация 3.

---

## 1. Правила зависимостей (формально)

Пакет `core.*` разбит на слои; разрешённые импорты задаются матрицей. CI проверяет импорты
(статически — через `ast`, без исполнения).

| Из \ В | domain | ports | pipeline | adapters | store | config | cli | util |
|---|---|---|---|---|---|---|---|---|
| **domain** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| **ports** | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| **pipeline** | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ |
| **adapters** | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ❌ | ✅ |
| **store** | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ |
| **config** | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| **cli** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **util** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |

**Композиция (wiring):** `cli` — единственное место, где `pipeline` соединяется с `adapters`
(внедрение зависимостей вручную, без DI-фреймворка — A1). `pipeline` принимает порты как
аргументы, никогда не инстанцирует адаптеры.

**Следствие для §1.3:** `domain/`, `ports/`, `pipeline/` не содержат предметных терминов.
Адаптеры, работающие с реестрами/доменами, остаются нейтральными: специфика — в данных
(`registries/*.yaml`, `packages/*`), не в коде.

---

## 2. Домен (L0) — модули и типы

### 2.1. `domain/enums.py`

Зеркало `docs/status-dictionary.yaml`. Существующие `StrEnum` (Status, SourceKind, …) переносятся
из текущего `core/schemas.py` без изменения семантики. Добавляется:

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

Чистые Pydantic-модели (без I/O). Переносятся из `core/schemas.py` + уточняются:

| Модель | Изменение против текущей |
|---|---|
| `PackageManifest`, `ModuleDef`, `RoleDef` | + `package_version` |
| `CatalogRow` | без изменений (11 колонок) |
| `BaselineRow` | без изменений |
| `GoldenRow` | + `tier` (fast/full) |
| `Cohort`, `CohortEntry` | без изменений |
| `Passport`, `Satellite`, `Upstream`, `Citation` | без изменений |
| `RunConfig`, `Focus` | + `ranking_policy`, `languages` |
| `Cell`, `LLMCellOutput`, `LLMEvidence` | без изменений |
| **`RunManifest`** | **новое**: агрегирует все версии (§9, §13) |
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
def check_inheritance(cell: Cell, baseline: BaselineRow) -> list[Violation]  # I8
def check_satellite(sat: Satellite) -> list[Violation]   # I9
```

`Violation(code, message, field)` — типизированная ошибка; `code` — `"I2b"` и т.п. Это позволяет
CI и ревью-эшелону ссылаться на инвариант машинно.

### 2.4. `domain/scoring.py`

```python
def resolved_capability(cells, params, weights, policy) -> Score        # I5 (failed вне числителя)
def evidence_coverage(cells, params, weights) -> Coverage               # failed в знаменателе (I5)
def failure_rate(cells) -> float
def comparability(cohort, per_product_coverage, alignment: AlignmentReport) -> Comparability
def sensitivity(cells, params, weights, axes) -> SensitivityReport      # §6
def module_scores(...) -> list[ModuleScore]
```

Чистые функции над доменными объектами; никакого доступа к БД (загрузка — в pipeline).

### 2.5. `domain/r2.py`, `domain/anchors.py`

Перенос текущих `apply_r2` и `load_dictionary/assert_dictionary_integrity`.
`anchors.py` добавляет `resolve_source_url(source_id, store_reader)` — **детерминированный**
резолв (I10/I15): `source_id` кодирует `product_id`+`doc_id`+`chunk_no`, резолв — по индексу,
без сети.

---

## 3. Порты (L2) — контракты

Все порты — `typing.Protocol` (структурная типизация, без наследования — упрощает адаптеры).
Каждый порт в отдельном файле. Ниже — сигнатуры (псевдо-Python, полные доказы — docstring).

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
веб-поиск. Адаптер `llm_local.py` подключается к self-host (vLLM/SGLang) без тулов.
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
    chunk_id: str; product_id: str; text: str; heading_path: str; score: float

class VectorIndexPort(Protocol):
    build_id: str
    dim: int
    def search(self, q: VectorQuery) -> list[RetrievedChunk]: ...
    def shard_ids(self, product_id: str) -> list[str]: ...
```

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
class CrawlerPort(Protocol):     def fetch(self, url: str, render: RenderMode) -> RawPage: ...
class ExtractorPort(Protocol):   version: str
                                 def extract(self, page: RawPage) -> ExtractedDoc: ...
class ChunkerPort(Protocol):     version: str
                                 def chunk(self, doc: ExtractedDoc) -> list[Chunk]: ...
class RegistryFetcherPort(Protocol):
    registry_id: str
    def fetch(self, product: ProductRef, scope: dict) -> list[RegistryFact]: ...
class StorePort(Protocol):       # §4.4
    def upsert_products(...); def create_run(...); def save_cell(...)
    def add_evidence(...); def log_search(...); def save_exhaustion(...)
    def read_manifest(...); def iter_cells(run_id) -> Iterator[Cell]
```

`RawPage(url, bytes, fetched_at, render_mode, lang)` — retention-единица.
`ExtractedDoc(url, text, structure, extractor_version)`.
`Chunk(chunk_no, text, heading_path, context_prefix, content_hash)`.

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
| `corpus.py` | `build_corpus(plan, crawler, extractor, chunker, embedder, store) -> IndexBuild` | corpus_*, index_manifest | I17, I19 |
| `evaluate.py` | `evaluate(plan, llm: EvaluatorPort, vindex, lindex, store) -> list[Cell]` | cells, evidence | I1, I16, I17, I20 |
| `validate.py` | `validate(cell, chunks_by_id) -> ValidationResult` | cell_evidence | §10 |
| `review.py` | `review_queue(run_id, store) -> ReviewSet` | cells (reviewed) | I6, I7, I14 |
| `compute.py` | `compute(run_id, store) -> ScoreBundle` | runs (сводка) | I5, I11, I13 |
| `report.py` | `report(run_id, store, out_dir) -> Artifacts` | reports | I11 |
| `experiments.py` | `run_retrieval_experiment(profiles) -> ExperimentReport` | runs | R9, R10 |
| `gates.py` | `gate_g1/r2/r3/g4(...) -> GateDecision` | runs | G1–G4 |

**Общий контракт фазы:** `PhaseResult(status: ok|blocked|failed, cells, log_entries, errors)`.
Конвейер (`pipeline/runner.py`) оркестрирует последовательность и останавливается на гейтах.

---

## 5. Утилиты и сквозные типы (`util/`)

| Модуль | Содержимое |
|---|---|
| `ids.py` | `source_id(product, doc, chunk_no)`, парсинг; slug-валидация |
| `hashing.py` | `content_hash(bytes)`, `prompt_hash(template, inputs)` |
| `logging.py` | структурированный лог (JSON lines) в `runs/{run_id}/logs/` |
| `clock.py` | `snapshot_date`, UTC-таймстемпы (тестируемость) |

`prompt_hash` фиксируется: hash от (шаблон_версия + rule + absence_rule + отсортированные chunk_id).
Изменение шаблона → новый hash → ячейки инвалидируются (§13).

---

## 6. Матрица «инвариант → где обеспечивается»

| Инвариант | domain/invariants | SQL-триггер | pipeline | CI |
|---|---|---|---|---|
| I1, I2, I2b | ✅ | — | evaluate | selfcheck |
| I4, I6, I7, I13 | ✅ | ✅ | review | selfcheck |
| I5, I11 | ✅ (scoring) | — | compute | selfcheck |
| I8 | ✅ | — | inheritance | selfcheck |
| I9 | ✅ | — | discovery | selfcheck |
| I10, I15 | ✅ | ✅ (source_id) | anchors/validate | selfcheck |
| I16, I17, I20 | — | — | corpus/evaluate | experiments |
| I18, I19, I23 | — | ✅ (I18) | corpus/search | selfcheck + грепп |
| I21 | — | — | plan/runner | selfcheck (манифест) |
| I22 | ✅ | — | evaluate | selfcheck |

---

## 7. Реестровые фетчеры и нейтральность (§1.3)

Конкретные реестры (РРПО, ФСТЭК, …) **не кодируются в `core/`**. Механизм:

```
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
| Хранилище (SQLite→PG) | новый `store/*.py` | domain |

---

## 9. Что переносится из текущего кода (миграция)

| Сейчас | Станет | Действие |
|---|---|---|
| `core/schemas.py` (монолит) | `domain/enums.py` + `domain/models.py` + `domain/invariants.py` + `domain/r2.py` | расщепить |
| `core/db.py` DDL/триггеры | `store/sqlite.py` (DDL) + `ports/store.py` (контракт) | разделить данные/интерфейс |
| `tests/selfcheck.py` | `tests/domain/*`, `tests/store/*` | разбить по слоям |

Миграция — в итерации 3 (когда конфиг/CLI определены), чтобы не ломать текущий selfcheck.
До этого `core/schemas.py` и `core/db.py` остаются рабочими.

---

## 10. Traceability и открытые вопросы

| Регламент | Покрытие |
|---|---|
| §3.1 компоненты | §2–§4 |
| §4.1 словари | §2.1 |
| §7 LLM-контуры | §3.1, §3.4 |
| §11.6 CI | §1 (проверка импортов), §6 |
| I1–I23 | §6 матрица |

**Открытые вопросы к итерации 2:** точные сигнатуры фьюжна RRF; формат `AlignmentReport`;
структура `ProtocolExhaustion`; политика ретраев в `evaluate.py`; как передавать чанки в
`validate` (по `source_id` из store или из RetrievalResult).
