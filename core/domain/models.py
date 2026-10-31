"""coHound — доменные модели (L0): Pydantic-схемы §4.2/4.3/4.5/7.

Чистые модели без I/O; инварианты обеспечиваются валидаторами (извлечение чистых функций —
T2.2 в ``domain/invariants.py``). Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.domain.enums import (
    ABSENT_DENIAL_STRENGTHS,
    BLOCKING_FAILURE_KINDS,
    UNKNOWN_DENIAL_STRENGTHS,
    AbsenceRuleType,
    AdapterKind,
    AnchorRelation,
    Archetype,
    ArchiveDescent,
    ArchiveDescentStatus,
    CellState,
    CrawlItemStatus,
    CrawlSessionStatus,
    CrawlStrategy,
    DenialClaim,
    DenialStrength,
    DocumentLevel,
    EmbedDevice,
    EndpointKind,
    EvidenceState,
    EvidenceType,
    ExtractQueueState,
    FailureKind,
    Gate,
    GatePolicy,
    GateSource,
    GateVerdict,
    GoldenTier,
    Importance,
    InheritanceMode,
    LLMScenario,
    MeasurementOrigin,
    OnboardingState,
    Origin,
    ParamType,
    PdfDevice,
    PdfPipeline,
    ProductMode,
    ProductScope,
    RankingPolicy,
    RefreshPolicy,
    Relationship,
    ReuseKind,
    RunMode,
    SatelliteState,
    SearchKind,
    SearchMode,
    SearchPhase,
    SeedRole,
    SeedSource,
    SeedState,
    SourceKind,
    SourceScope,
    StaleReason,
    Status,
    TerminationReason,
)
from core.util.document_formats import normalize_attachment_formats
from core.util.ids import doc_version_of

_BASE = ConfigDict(extra="forbid", str_strip_whitespace=True)

#: Канонические значения :class:`FailureKind` — единый набор для валидаторов persisted-полей
#: (``crawl_session_items.error_kind`` и ``corpus_coverage.blocked``).
_KNOWN_FAILURE_KINDS: frozenset[str] = frozenset(kind.value for kind in FailureKind)

#: Строковые значения блокирующих классов — для :attr:`CorpusCoverage.blocking_failures`.
_BLOCKING_FAILURE_VALUES: frozenset[str] = frozenset(
    kind.value for kind in BLOCKING_FAILURE_KINDS
)

#: Неизменяемый конфиг для value-объектов, кладущихся в хешируемые frozen-контейнеры
#: (``FetchPlan``): ``frozen=True`` делает модель хешируемой (value-семантика), а
#: ``allow_inf_nan=False`` запрещает ``inf``/``nan`` в числовых полях.
_FROZEN = ConfigDict(
    extra="forbid",
    str_strip_whitespace=True,
    frozen=True,
    allow_inf_nan=False,
)

#: Версия методологии (§13): версионная граница **семантики** правил (словарь статусов,
#: scoring-политика, absence/evidence-модель, определения Coverage/Comparability). В отличие
#: от ``execution_versions`` (commodity-слой: модель/когорта/индекс), смена методологии
#: требует пере-аннотации golden. Пин-значение проекта; попадает в
#: ``RunConfig.methodology_version`` → ``RunManifest.methodology_version``.
#:
#: 2.2.0 — in-scope блокирующие сбои корпуса делают покрытие неполным (I2c, Ф0):
#: ``BLOCKING_FAILURE_KINDS``, а не только «origin с нулём FETCHED».
#: 2.3.0 — два правила: (1) ``CorpusCoverage.complete`` требует
#: ``observed_sources > 0``: корпус без единого наблюдения не доказывает отсутствие
#: (иначе продукт, у которого все URL ответили ``not_found``, публиковал ``complete=True``
#: и мог выдать ``❌``); (2) Comparability/``hound diff`` сравнивает
#: ``execution_contract_digest`` (условия измерения), а не ``index_build_id``: смена
#: контента при неизменном контракте — разрешённый diff класса ``content_change`` с
#: баннером и doc-delta. Оба — ``methodology_change``: ``golden-full`` + пере-аннотация
#: golden (§13, 09 §12.5).
#: 2.4.0 — определение Comparability: ``execution_contract_digest`` = отпечаток
#: **декларированных** осей (функция входа), а наблюдаемые величины портов/диска
#: (``extractor_version``/``embed_model_id``/``lexical_engine_version``/``vector_store``)
#: из него исключены: они не выводятся из входа и делали бы ``run_id`` хост-зависимым
#: (R7 §11.7). Вместо ``extractor_version`` объявлен ``extractor_stack`` (константы стека
#: извлечения + версия библиотеки docling). Принятое ограничение названо прямо: смена
#: этих четырёх величин **не блокирует** межцикловый diff, а видна через
#: ``pb``/``index_build_id`` и попадает в разрешённый класс ``content_change`` (§13, §14).
#: 0.1.0 — **сброс нумерации решением владельца (26.10.2026).** Версии 2.x описывали
#: методологию, которая не была реализована: ни одного нормального результата по ней не
#: получено, поэтому версия ничего не решала и служила ложной провенанс-записью
#: «измерение по 2.4.0». 0.1.0 называет фактическое состояние: правила есть, приёмки нет.
#: История выше **не переписывается** — это аудит-след принятых решений; записи 2.x
#: остаются в силе как описание того, что *объявлялось* тогда. Следствие для артефактов:
#: старые манифесты несут ``2.4.0`` и читаются как есть (версия манифеста — то, что было
#: записано), расхождение с текущей константой видно в ``hound diff`` строкой
#: ``methodology_change``, но **не** является ошибкой чтения прогона.
#: 0.2.0 — граница ``runs`` 29.10.2026 (``16-runs-schema-boundary``): **условие публикации
#: ``❌`` меняется** — корпусное/``explicit``/``authoritative`` отрицание опирается только на
#: наблюдение с серверным подтверждением (``corpus_docs.reuse_kind ∈ {live, revalidated}``,
#: ``09`` §11.2), иначе ячейка уходит в ``❓`` с причиной ``reuse_unconfirmed``. Это
#: absence/evidence-модель, а не исполнение, поэтому класс границы — ``methodology_change``
#: с обязательными ``golden-full`` **и** пере-аннотацией golden (§13). В ту же границу
#: вошли ось ``embed_device``, формат ``chunk_id``/партиции и дефекты краула: сам по себе
#: каждый дал бы ``execution_change``/``content_change``, но самый сильный класс поглощает
#: остальные, и объединение дешевле (одна пере-аннотация, а не две).
METHODOLOGY_VERSION = "0.2.0"

#: P0/OOM: дефолтный потолок одного HTTP-ответа/retained-файла — 20 MiB. Операционная
#: ось исполнения (``crawl.max_response_bytes``), не методология; адаптеры используют
#: его как собственный дефолт, чтобы probe/прочие call-site без ``CrawlLimits``
#: получали тот же предел.
DEFAULT_MAX_RESPONSE_BYTES = 20_971_520

#: P6: дефолтный **потоковый** потолок допущенного артефакта-контейнера — **250 MiB**
#: (``250 × 1024 × 1024 = 262 144 000`` B). Значение — **объявленный владельцем потолок**
#: хранения одного артефакта (политика, а не размер буфера), поэтому оно не степень
#: двойки: «250 MiB» и есть требование, а не удобное округление. Покрывает измеренный
#: корпусный контейнер (125 727 643 B) с запасом; страница при этом остаётся под
#: ``DEFAULT_MAX_RESPONSE_BYTES`` (in-memory потолок).
DEFAULT_MAX_ARTIFACT_BYTES = 262_144_000

#: P6: дефолтный потолок **одного члена** контейнера — 64 MiB (измеренный максимум
#: 25.1 MB). Потолки спуска — операционные оси исполнения (``execution_change``).
DEFAULT_ARCHIVE_MAX_MEMBER_BYTES = 67_108_864

#: P6: дефолтный потолок суммарного распакованного объёма контейнера — 256 MiB
#: (измерено 139 661 173 B).
DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES = 268_435_456

#: B3: дефолтный **кумулятивный** потолок распакованного объёма на один корень спуска —
#: 1 GiB. Оси ``archive_max_*`` ограничивают **один** контейнер; без потолка на корень
#: архив из 10 000 вложенных архивов исчерпывал бы CPU/диск, хотя каждый контейнер по
#: отдельности «в бюджете». Спуск без ограничения глубины обязан оставаться конечным.
#: Ось операционная (``execution_change``, §13), а не методологическая.
DEFAULT_ARCHIVE_MAX_TOTAL_UNCOMPRESSED_BYTES = 1_073_741_824



# ---------------------------------------------------------------------------
# §4.2 Пакет категории
# ---------------------------------------------------------------------------


class RoleDef(BaseModel):
    """Роль — инженерная функция с реестром в пакете (нейтрально к категории).

    ``definition`` опционально: ``None`` — **не заполнено автором** (A3). Пустой
    definition не выдаётся за контент: заполненность проверяет линтер
    (``core.pipeline.lint.check_unfilled_authoring``), а прогон неполного пакета
    блокируется. Никаких выдуманных формулировок роль не несёт.
    """

    model_config = _BASE

    role_id: str
    definition: str | None = None
    markers: list[str] = Field(default_factory=list)
    query_patterns: dict[str, list[str]] = Field(default_factory=dict)  # {"ru": [...], "en": [...]}
    core_coverage_allowed: bool = False
    multi_instance: bool = False


class ModuleDef(BaseModel):
    model_config = _BASE

    module_id: str
    weight: float = Field(ge=0, le=100)


class PackageManifest(BaseModel):
    """package.yaml (§4.2)."""

    model_config = _BASE

    case_id: str
    package_version: str = "0.0.0"
    archetype: Archetype
    modules: list[ModuleDef]
    roles: list[RoleDef]
    baselines: list[str] = Field(default_factory=list)  # baseline_id
    baseline_participant: str | None = None
    jurisdictions: list[str] = Field(default_factory=list)
    registries: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> PackageManifest:
        total = round(sum(m.weight for m in self.modules), 6)
        if total != 100:
            raise ValueError(f"сумма весов модулей должна быть 100, получено {total}")
        ids = [m.module_id for m in self.modules]
        if len(ids) != len(set(ids)):
            raise ValueError("module_id должны быть уникальны")
        rids = [r.role_id for r in self.roles]
        if len(rids) != len(set(rids)):
            raise ValueError("role_id должны быть уникальны")
        # §1.5: архетип C — эталона нет; baseline_participant должен быть null.
        if self.archetype is Archetype.C and self.baseline_participant is not None:
            raise ValueError("архетип C: baseline_participant обязан быть null")
        return self


class ScopeAttestation(BaseModel):
    """Подпись человека под аттестацией охвата источника для одного продукта (I2b/I2c).

    Артефакт пакета ``scope_attestation.yaml`` держит **процедуру** (строка ``procedure``)
    и опциональный список ``attestations`` таких подписей. Аттестация **per product**,
    потому что покрытие корпуса per product: подпись «справочник полон» для одного
    продукта не делает полным покрытие другого.

    Это тот же класс, что ``reviewed_by``: подпись авторизует заявление из тех же
    измерений, но не меняет само измерение. Поэтому в ``EXECUTION_CONTRACT_FIELDS``
    она не входит (как и политика гейтов) — смена подписанта не создаёт новый ``run_id``.

    ``attested_at``/``snapshot_date`` — строки ISO: значение записывается как пришло
    (дата снимка, к которому относится подпись), а не выводится из прогона.
    """

    model_config = _BASE

    product_id: str
    attested_by: str
    attested_at: str | None = None
    snapshot_date: str | None = None
    notes: str | None = None

    @field_validator("attested_at", "snapshot_date", mode="before")
    @classmethod
    def _iso_text(cls, value: Any) -> Any:
        """Принимает YAML-``date``/``datetime`` как ISO-строку (детерминизм байтов)."""
        if isinstance(value, (_dt.datetime, _dt.date)):
            return value.isoformat()
        return value

    @model_validator(mode="after")
    def _check(self) -> ScopeAttestation:
        if not self.product_id:
            raise ValueError("ScopeAttestation: product_id не может быть пустым")
        if not self.attested_by:
            raise ValueError(
                "ScopeAttestation: attested_by обязателен (аттестация — человеческая)"
            )
        return self


class CohortEntry(BaseModel):
    """cohort.yaml (§1.6).

    ``selection_reason`` опционально: ``None`` — **не заполнено автором** (A3). Значение
    не подменяется правдоподобным текстом: незаполненность явна и проверяется линтером
    (``check_unfilled_authoring``), блокируя прогон неполного пакета.
    """

    model_config = _BASE

    product_id: str
    role_overlap: float = Field(ge=0, le=1)
    selection_reason: str | None = None
    anchor_relation: AnchorRelation


class Cohort(BaseModel):
    model_config = _BASE

    case_id: str
    frozen_at: _dt.date
    entries: list[CohortEntry]

    @model_validator(mode="after")
    def _check(self) -> Cohort:
        anchors = [e for e in self.entries if e.anchor_relation is AnchorRelation.ANCHOR]
        if len(anchors) != 1:
            raise ValueError(f"когорта обязана содержать ровно один якорь, найдено {len(anchors)}")
        pids = [e.product_id for e in self.entries]
        if len(pids) != len(set(pids)):
            raise ValueError("product_id в когорте должны быть уникальны")
        return self


class CatalogRow(BaseModel):
    """Строка catalog.csv (11 колонок, §4.2)."""

    model_config = _BASE

    param_id: str
    title: str
    module: str
    group: str
    importance: Importance
    param_type: ParamType
    rule: str
    absence_rule: AbsenceRuleType = AbsenceRuleType.EXPLICIT
    role_binding: str | None = None
    concept_terms: dict[str, list[str]] = Field(default_factory=dict)  # {"ru": [...], "en": [...]}
    search_langs: list[str] = Field(default_factory=list)
    # §13: slug (param_id) иммутабелен; отмена — флаг ``deprecated`` + ``replaced_by``,
    # физического удаления строки нет. Поля опциональны и обратно совместимы: пакет без
    # одноимённых CSV-колонок грузится с дефолтами (False/None).
    deprecated: bool = False
    replaced_by: str | None = None

    @model_validator(mode="after")
    def _check(self) -> CatalogRow:
        if not self.param_id or self.param_id != self.param_id.lower():
            raise ValueError("param_id — slug в нижнем регистре")
        # Линтер §4.2: showcase → rule должен упоминать величину/метрику.
        if self.param_type is ParamType.SHOWCASE:
            tokens = ("величин", "metric", "велич", "%", "числ", "значени")
            if not any(t in self.rule.lower() for t in tokens):
                raise ValueError(
                    "param_type=showcase: rule обязан упоминать публичную величину/метрику"
                )
        return self


class BaselineRow(BaseModel):
    """Строка baselines.csv (§4.2)."""

    model_config = _BASE

    baseline_id: str
    param_id: str
    min_version: str | None = None
    inheritance_mode: InheritanceMode
    note: str | None = None


class GoldenRow(BaseModel):
    """Строка golden.csv (§4.2)."""

    model_config = _BASE

    product_id: str
    param_id: str
    expected_status: Status
    expected_evidence_kind: list[SourceKind]
    case_class: str
    tier: GoldenTier = GoldenTier.FULL
    note: str | None = None


# ---------------------------------------------------------------------------
# §4.3 Паспорт продукта
# ---------------------------------------------------------------------------


class Citation(BaseModel):
    model_config = _BASE

    url: str
    quote: str


class Upstream(BaseModel):
    model_config = _BASE

    baseline_id: str
    version: str
    evidence: Citation


class Satellite(BaseModel):
    model_config = _BASE

    role: str
    state: SatelliteState
    component: str | None = None
    origin: Origin | None = None
    relationship: Relationship | None = None
    evidence_state: EvidenceState | None = None
    relationship_evidence: Citation | None = None
    rejected_candidates: list[dict[str, Any]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> Satellite:
        # Precision-first: для установленного сателлита нужны обе вещи — существование
        # и связь в роли (relationship evidence).
        established = self.state in {
            SatelliteState.APPROVED,
            SatelliteState.EDITED_BY_HUMAN,
        }
        if established:
            missing = [
                f
                for f in ("component", "origin", "relationship", "evidence_state")
                if getattr(self, f) is None
            ]
            if missing:
                raise ValueError(f"установленный сателлит: не заданы поля {missing}")
            if self.relationship_evidence is None:
                raise ValueError("установленный сателлит обязан иметь relationship_evidence")
        return self


class Passport(BaseModel):
    """passports/{product_id}/{YYYY-MM-DD}.yaml (§4.3)."""

    model_config = _BASE

    product_id: str
    canonical_name: str
    vendor: str
    upstream: Upstream | None = None
    edition_assessed: str | None = None
    version: str | None = None
    jurisdiction: str
    doc_domains: list[str] = Field(default_factory=list)
    satellites: list[Satellite] = Field(default_factory=list)
    snapshot_date: _dt.date


# ---------------------------------------------------------------------------
# §4.5 run.yaml
# ---------------------------------------------------------------------------


class Focus(BaseModel):
    model_config = _BASE

    modules: list[str] = Field(default_factory=list)
    params: list[str] = Field(default_factory=list)


class RateLimitSpec(BaseModel):
    """Диапазон случайной задержки между запросами (сек), uniform(min_s, max_s), min_s<=max_s.

    Неизменяемый value-объект (``frozen``): входит в хешируемый ``FetchPlan``. ``min_s=0`` и
    ``max_s=0`` допустимы и означают «без троттлинга» (нулевая задержка). ``inf``/``nan``
    запрещены (``allow_inf_nan=False``).
    """

    model_config = _FROZEN

    min_s: float = Field(ge=0.0)
    max_s: float = Field(ge=0.0)

    @model_validator(mode="after")
    def _check(self) -> RateLimitSpec:
        if self.max_s < self.min_s:
            raise ValueError("rate_limit: max_s обязан быть >= min_s")
        return self


def canonical_rate_limit(spec: RateLimitSpec | None) -> str | None:
    """Каноническая текстовая форма :class:`RateLimitSpec` для манифеста (§13, Backlog B).

    Форма стабильна и детерминирована (граница версий не должна «дрожать» от формата
    чисел): ``uniform(min=<min_s>,max=<max_s>)``, числа — ``repr(float(...))``
    (кратчайшая round-trippable запись CPython). ``None`` → ``None`` (секция ``crawl``
    в ``run.yaml`` отсутствует → граница не заполняется). Манифест — **запись**:
    обратный разбор не требуется, сравнение — строковое.
    """
    if spec is None:
        return None
    return f"uniform(min={float(spec.min_s)!r},max={float(spec.max_s)!r})"


class EmbedRunConfig(BaseModel):
    """``run.yaml`` → секция ``embed``: run-слой устройства эмбеддера (``embed.device``).

    ``device`` — ``None`` = не задано на этом слое: резолв берёт app-дефолт ``cpu``. Не
    ``cpu`` по умолчанию — по той же причине, что у ``CrawlRunConfig.pdf_device``: дефолт
    run-слоя перекрыл бы app-слой в цепочке ``_pick``, и объявленный
    ``deploy.yaml:embed.device`` молча не действовал бы (R5).

    Per-site слоя нет: устройство — свойство **хоста исполнения**, а не сайта (тот же
    довод, что в :func:`core.pipeline.crawl_frontier.resolve_pdf_device`).
    """

    model_config = _BASE

    device: EmbedDevice | None = None


class CrawlRunConfig(BaseModel):
    """``run.yaml`` → секция ``crawl``: дефолты оси стратегии и диапазона задержек.

    Ось ``strategy`` ортогональна ``FetchMode``: она задаёт, **как** используется браузер
    (см. :class:`~core.domain.enums.CrawlStrategy`). ``rate_limit`` — необязательный
    per-host диапазон uniform(min_s, max_s). При отсутствии секции поведение неизменно.

    D1/D2: здесь же — run-profile слой осей контроля краула (``max_workers``/``max_depth``/
    ``max_documents``/``exclude_paths``/``exclude_documents``); ``None`` = «не задано на этом
    слое» (наследуется application default). Резолв цепочки app < run < site — в
    :func:`core.pipeline.crawl_frontier.resolve_crawl_limits`. ``max_documents=0`` отвергается.
    """

    model_config = _BASE

    strategy: CrawlStrategy = CrawlStrategy.AUTO
    rate_limit: RateLimitSpec | None = None
    max_workers: int | None = Field(default=None, ge=1)
    # F1 (план 17): сколько **host'ов** (ключ вежливости лимитера) этот процесс краула
    # ведёт одновременно. ``None``/``1`` = сегодняшнее поведение (одна полоса за раз).
    # Это не размер пула фетча: внутри одного host параллельность пропускной способности
    # не даёт (замер 2026-10-31), поэтому ось считает полосы, а не потоки.
    host_concurrency: int | None = Field(default=None, ge=1)
    # P4: отдельная ось CPU-параллелизма экстракции (пул процессов/потоков). ``None``/``1``
    # = поведение байт-в-байт (inline); не смешивается с ``max_workers`` (сетевой fetch-пул).
    extract_workers: int | None = Field(default=None, ge=1)
    # P4/OOM: рециклинг воркер-процессов локальной экстракции
    # (``ProcessPoolExecutor(max_tasks_per_child=N)``). ``None`` — ось не задана: при
    # ``extract_workers > 1`` пул живёт без рециклинга, при ``None``/``1`` экстракция
    # inline (поведение байт-в-байт). ``N ≥ 1`` — экстракция идёт пулом даже при
    # ``extract_workers == 1``, а воркер пересоздаётся после N заданий: рабочий набор
    # тяжёлого конвертера (docling) не накапливается в долгоживущем процессе.
    extract_max_tasks_per_child: int | None = Field(default=None, ge=1)
    # P0/OOM: run-слой потолка одного HTTP-ответа/retained-файла (байты). ``None`` —
    # не задано на этом слое (наследуется app-дефолт 20 MiB). Резолв — та же цепочка
    # app < run < site; смена — ``execution_change`` (§13), не methodology.
    max_response_bytes: int | None = Field(default=None, gt=0)
    # P6/P0: run-слой потокового потолка допущенного артефакта-контейнера (байты).
    # ``None`` — не задано на этом слое (наследуется app-дефолт 250 MiB).
    max_artifact_bytes: int | None = Field(default=None, gt=0)
    # P6: заявленные форматы-контейнеры (``zip``/``7z``/``rar``). ``None`` — наследовать
    # app-слой; сужение (напр. ``["zip"]``) допустимо.
    attachment_formats: list[str] | None = None
    # P6: границы спуска в контейнер (для следующего слоя; входят в провенанс).
    archive_max_member_bytes: int | None = Field(default=None, gt=0)
    archive_max_uncompressed_bytes: int | None = Field(default=None, gt=0)
    archive_max_members: int | None = Field(default=None, gt=0)
    #: B3: кумулятивный потолок распакованного объёма **на корень спуска** (байты).
    #: ``None`` — наследовать app-слой. Ось операционная (``execution_change``, §13).
    archive_max_total_uncompressed_bytes: int | None = Field(default=None, gt=0)
    archive_descent: ArchiveDescent | None = None
    max_depth: int | None = Field(default=None, ge=0)

    max_documents: int | None = Field(default=None, gt=0)
    #: Ops «не ограничивать, а измерять»: максимум известных URL ревалидации **на продукт**
    #: за цикл. ``None`` — ось не задана (все известные URL; поведение байт-в-байт).
    #: Заданное значение откладывает хвост с ``stale`` в ``url_state`` и видимым
    #: продуктовым усечением ``revalidate_max_documents``; смена — ``execution_change``.
    revalidate_max_documents: int | None = Field(default=None, ge=1)
    exclude_paths: list[str] | None = None
    exclude_documents: list[str] | None = None
    # P5: run-слой уровня извлечения документных форматов (``run.yaml:crawl.document_level``).
    # ``None`` = не задано на этом слое (наследуется app-дефолт ``full``). Резолв —
    # :func:`core.pipeline.crawl_frontier.resolve_crawl_limits`.
    document_level: DocumentLevel | None = None
    # P7: run-слой **механизма** извлечения PDF (``run.yaml:crawl.pdf_pipeline``).
    # ``None`` = не задано на этом слое: резолв берёт app-дефолт ``structured``
    # (сегодняшнее поведение). Не ``structured`` по умолчанию — иначе дефолт run-слоя
    # перекрывал бы app/site-слои в цепочке ``_pick`` (site > run > app) и объявленный
    # ``deploy.yaml:crawl.pdf_pipeline`` молча не действовал бы.
    pdf_pipeline: PdfPipeline | None = None
    # P9: run-слой **устройства** моделей извлечения PDF (``run.yaml:crawl.pdf_device``).
    # ``None`` = не задано на этом слое: резолв берёт app-дефолт ``cpu``. Не ``cpu`` по
    # умолчанию — по той же причине, что у ``pdf_pipeline``: дефолт run-слоя перекрыл бы
    # app-слой в цепочке ``_pick``, и объявленный ``deploy.yaml:crawl.pdf_device`` молча
    # не действовал бы (R5). Устройство — ось **хоста исполнения**, а не сайта: per-site
    # переопределения у неё нет (в ``domain_profiles.yaml`` поля нет намеренно).
    pdf_device: PdfDevice | None = None
    #: Ось «потолок времени на извлечение **одного** документа» (``crawl.document_timeout_s``,
    #: секунды). ``None`` = не задано на этом слое: резолв берёт app-слой, затем
    #: ``DEFAULT_DOCUMENT_TIMEOUT_S`` (1800 с — ровно прежнее зашитое значение, поэтому
    #: объявление оси не меняет поведение молча). Без поля ``run.yaml`` с этим ключом
    #: отвергался бы (``extra="forbid"``), а прогон оставался бы без объявленной оси:
    #: медленный источник нельзя ни ускорить, ни объяснить.
    document_timeout_s: float | None = Field(default=None, gt=0)

    @field_validator("exclude_paths", "exclude_documents")
    @classmethod
    def _check_globs(cls, value: list[str] | None) -> list[str] | None:
        """Отвергает не-строки/пустые glob (D1/H)."""
        if value is None:
            return None
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    "crawl.exclude_paths/exclude_documents: ожидались непустые строки-glob"
                )
        return value

    @field_validator("attachment_formats")
    @classmethod
    def _check_attachment_formats(cls, value: list[str] | None) -> list[str] | None:
        """Нормализует run-набор контейнеров (нижний регистр, без точек)."""
        if value is None:
            return None
        try:
            return list(normalize_attachment_formats(value))
        except ValueError as exc:
            raise ValueError(f"crawl.attachment_formats: {exc}") from exc


class SerpRunConfig(BaseModel):
    """Секция ``serp`` run.yaml — SERP-контур (I3, §5) как **объявленный вход** прогона.

    Контур внешнего поиска — не настройка удобства: он добавляет документы в корпус,
    то есть меняет доказательства. Поэтому число раундов объявлено во входе и входит в
    ``EXECUTION_CONTRACT_FIELDS`` (``serp_rounds`` → ``run_id``/``execution_change``), а
    не в ``settings_provenance``.

    :param rounds: число проходов внешнего поиска; ``0`` (дефолт) — контур выключен и
        прогон ведёт себя как без него (ни одного вызова поиска).
    :param budget: потолок запросов внешнего поиска **на продукт на раунд**. Обязан быть
        объявлен при ``rounds > 0``: бюджет определяет число ушедших запросов, а
        безлимит по умолчанию означал бы «стоимость контура решается случаем». Дефолта
        нет (см. решение владельца: потолок — признание, что измерять не умеют; но
        **молчаливый** безлимит хуже явного потолка, поэтому требуется объявление).
    """

    model_config = _BASE

    rounds: int = Field(default=0, ge=0)
    budget: int | None = Field(default=None, ge=1)
    #: Режим запроса провайдера (:class:`SearchMode`). ``None`` — исполнение возьмёт
    #: дефолт кода (``async``) и **объявит** его в контракте: условие измерения не может
    #: приходить из env (``YANDEX_SEARCH_MODE`` остаётся настройкой разведки).
    #: Зачем это вход, а не удобство: ``sync`` и ``async`` — два бэкенда одного API;
    #: замер 28.10 — sync отвечает за 6.9 с медиана, async за 180 с при опросе 30 с и
    #: отвечает ``search API backends are overloaded`` там, где sync работает. Разные
    #: бэкенды дают разный состав находок, то есть разный корпус.
    mode: SearchMode | None = None

    @model_validator(mode="after")
    def _check(self) -> SerpRunConfig:
        if self.rounds > 0 and self.budget is None:
            raise ValueError(
                "serp: объявлены rounds > 0, но budget не задан — безлимит внешнего "
                "поиска запрещён (объявите serp.budget: <запросов на продукт на раунд>)"
            )
        return self


class RunConfig(BaseModel):
    """run.yaml (§4.5): мандат прогона (case/anchor/cohort/products/package/mode) и
    необязательный выбор активного LLM-слота ``llm_slot`` (§7.4).
    """

    model_config = _BASE

    case_id: str
    anchor: str
    cohort: str
    products: list[str]
    package: str
    #: §13: версия методологии (версионная граница семантики правил). Дефолт — пин проекта;
    #: ``"0.0.0"`` в манифесте означает «не заполнено», а не реальную версию.
    methodology_version: str = METHODOLOGY_VERSION
    focus: Focus = Field(default_factory=Focus)
    languages: list[str] = Field(default_factory=list)
    snapshot_date: _dt.date
    #: 09 §2.4/§6.2: идентификатор цикла прогона (ISO-дата/ISO-неделя). ``None`` —
    #: эффективный цикл выводится из ``snapshot_date`` (run identity, MVP-2b).
    cycle_id: str | None = None
    #: 09 §12.2: авторская ссылка на состав когорты (например, ``cohort-...-r2``).
    #: Входит в ``cohort_digest``; отображаемое имя/причина выбора — нет.
    cohort_ref: str | None = None
    #: 09 §2.4: политика наблюдения корпуса по умолчанию; ``--refresh`` её перекрывает.
    refresh_policy: RefreshPolicy | None = None
    mode: RunMode
    ranking_policy: RankingPolicy = RankingPolicy.CAPABILITY_RANKED
    #: §7.4: активный LLM-слот (``cheap``/``flagship``) из ``run.yaml``. ``None`` →
    #: ``deploy.llm.slot``/дефолт. Строка (не enum/литерал): домен L0 не импортирует
    #: ``core.config`` (матрица слоёв 01 §1); допустимые значения валидирует
    #: composition root (:func:`core.config.deploy.resolve_llm_slot`).
    llm_slot: str | None = None
    # §6/B-1 (v2.1): калибруемый порог неразличимости ранжирования (доля диапазона
    # агрегата); ``None`` → 0.0. Идёт в манифест с provenance (A3).
    ranking_epsilon: float | None = None
    #: A3 (v2.1): провенанс настроек ``{setting: {"source": "default"|"measured",
    #: "experiment_id"?: str, "value"?: Any, "by_level"?: {str: Any}}}``. Строки настроек
    #: (не enum ядра) — словарь не трогаем. ``value``/``by_level`` — зафиксированное
    #: значение измеренной ручки: скаляр (ручка одна) либо карта по уровням/осям, когда
    #: ручка зависит от разрешённой оси (напр. ``document_extract.queue_max_size``).
    settings_provenance: dict[str, dict[str, Any]] = Field(default_factory=dict)
    #: Секция ``crawl`` (D4): дефолты оси стратегии и диапазона задержек. Отсутствие
    #: секции (``None``) → поведение краула не меняется (обратная совместимость).
    crawl: CrawlRunConfig | None = None
    #: Секция ``embed`` — устройство эмбеддера этого прогона (``embed.device``). ``None``
    #: — секция не задана: действует app-слой (``deploy.yaml:embed.device``), затем
    #: объявленный дефолт ``cpu``. Смена значения — ``execution_change`` (входит в
    #: ``execution_contract_digest`` через ось ``embed_device``).
    embed: EmbedRunConfig | None = None
    #: Секция ``gates`` — политика гейтов G3/G4 для этого прогона
    #: (``{g3: external|waived, g4: external|waived}``; по умолчанию ``external``).
    #: Это **управление процессом**, а не условие измерения: в
    #: ``EXECUTION_CONTRACT_FIELDS`` секция намеренно не входит (§13 — иначе смена
    #: процедуры меняла бы ``run_id`` и рвала межцикловый diff).
    gates: dict[str, GatePolicy] = Field(default_factory=dict)
    #: Секция ``serp`` — SERP-контур (I3/§5). ``None`` — не объявлен: контур выключен
    #: (поведение и ``run_id`` прогонов без контура не меняются, дефолт ``rounds = 0``).
    #: Объявленные раунды входят в ``execution_contract_digest`` (``serp_rounds``).
    serp: SerpRunConfig | None = None

    @model_validator(mode="after")
    def _check(self) -> RunConfig:
        if self.anchor not in self.products:
            raise ValueError("anchor обязан входить в products")
        # S11: products — проекция когорты. Согласованность с cohort.yaml
        # проверяется линтером/планером (там доступен Cohort); здесь — уникальность.
        if len(self.products) != len(set(self.products)):
            raise ValueError("products не должен содержать дубликаты")
        if self.mode is RunMode.PRELIMINARY and self.ranking_policy is RankingPolicy.CAPABILITY_RANKED:
            # preliminary исключён из расчётов (I13) — ранжировать нечего.
            raise ValueError("preliminary не участвует в Capability-ранжировании (I13)")
        # Политика гейтов: только делегируемые гейты (G3/G4) и только известные ключи.
        # G1/G2 — внутренняя аттестация профиля; «делегировать» её молча было бы
        # способом обойти аттестацию.
        unknown = sorted(set(self.gates) - {"g3", "g4"})
        if unknown:
            raise ValueError(
                "run.yaml:gates: политика допустима только для ['g3', 'g4'], "
                f"получено {unknown} (G1/G2 — внутренняя аттестация профиля)"
            )
        return self


# ---------------------------------------------------------------------------
# §7 Выход LLM-оценщика
# ---------------------------------------------------------------------------


class LLMEvidence(BaseModel):
    model_config = _BASE

    source_id: str
    quote: str


#: Версия схемы ``LLMCellOutput`` и её валидаторов. Входит в ключ межпрогонного кэша
#: вердиктов (``core/pipeline/eval_cache.py``): поля/инварианты схемы меняют смысл
#: сохранённого выхода, поэтому при любой такой правке константу **обязано** повысить.
#: Иначе кэш обслужит вердикт, разобранный старой схемой, под новой. Исторического
#: значения константа не имеет: схема версионируется с момента появления этого поля.
#:
#: 2 — вариант 2 силы отрицания: у модели появилось поле ``denial_strength``
#: (:class:`~core.domain.enums.DenialClaim`), а ``❌`` без цитаты перестал быть ``❌``.
#: Вердикт, закэшированный по схеме 1, означал другое правило и обслуживать новый
#: прогон не может.
#:
#: 2 (без повышения, осознанно) — терпимость к полям отрицания при ``✅``/``⚠️``. Это
#: **расширение области принимаемого**, а не смена смысла: при схеме 2 такой ответ
#: никогда не валидировался, поэтому в кэш не попадал, и ни один сохранённый вердикт не
#: начинает означать другое. Повышение здесь только обесценило бы кэш (и уже сделанный
#: живой замер) без выигрыша в корректности.
EVAL_SCHEMA_VERSION = 2


class LLMCellOutput(BaseModel):
    """Схема ячейки, навязываемая оценщику constrained decoding'ом (§7).

    ``denial_strength`` — что модель **вправе** заявить о своём «нет»: только
    ``explicit`` (и тогда цитата обязательна) либо ``not_stated``. Авторитет
    (``authoritative``/``bounded``) у модели не спрашивают: он приходит от реестра,
    человека или объявленной процедуры полноты, а ``instrument_failed`` — от кода.
    Поле опционально: если адаптер/провайдер его не отдал, класс выводится механически
    из наличия цитаты (см. ``core.pipeline.evaluate``), и выведение **никогда** не даёт
    авторитета.

    **Поля отрицания при наличии-вердикте — шум, а не ошибка формы.** Текст промпта
    просит заполнять ``denial_strength``/``denial_reason`` всегда (шаблон перечисляет
    их в схеме ответа), и недетерминированный провайдер периодически присылает их
    вместе с ``✅``/``⚠️``. Прежняя редакция отвергала такой ответ **целиком**, и по I4
    ячейка становилась ``failed``/``status=None``: позитивный вердикт с цитатой
    терялся из-за шумового поля (живая проба: ``…avtosozdanie_particij_pri_vstavke``).
    Поэтому теперь вердикт наличия **не оспаривается**: поля остаются в объекте (аудит
    видит, что именно прислала модель), но в артефакты не попадают — их отбрасывает
    единственный потребитель (:func:`core.pipeline.evaluate._present_status_denial`),
    который и пишет причину в ``cells.validation_notes``/``review_reason``.
    Отрицательный вердикт без них по-прежнему неполон: там поля несут смысл, и
    ``core.pipeline.evaluate._resolve_denial`` разбирает их явно.
    """

    model_config = _BASE

    status: Status
    justification: str
    evidence: list[LLMEvidence] = Field(default_factory=list)
    denial_strength: DenialClaim | None = None
    denial_reason: str | None = None

    @model_validator(mode="after")
    def _check(self) -> LLMCellOutput:
        if self.status is Status.NOT_APPLICABLE:
            raise ValueError("N/A запрещён на выходе LLM (I7)")
        if self.status in {Status.OK, Status.PARTIAL} and not self.evidence:
            raise ValueError("✅/⚠️ требуют evidence ≥ 1 (I1)")
        # Отрицание и наличие — не одно и то же: класс отрицания у ✅/⚠️ бессмыслен.
        # Это **не** повод отвергать ответ: вердикт наличия доказывается evidence (I1/I22),
        # а поля отрицания к нему не относятся. Отбрасывание — не молчаливое: причина
        # видна в ``validation_notes`` и (для противоречия) в ``review_reason``
        # (:func:`core.pipeline.evaluate._present_status_denial`).
        return self


class EvidenceScope(BaseModel):
    """Сила отрицания + **вектор охвата** для одной ячейки (§2.3, Вариант 2).

    Один объект отвечает на вопрос «можно ли утверждать отсутствие» вместо трёх
    независимых блокировок (I2b/I2c/I3). Вопрос решается **происхождением** утверждения
    (:class:`~core.domain.enums.DenialStrength`), а не булевым «охват исчерпан»: скаляр
    ``source_scope: exhaustive`` не измерял ничего и давал ложную точность.

    **Вектор охвата — факты, а не доля.** Покрытие описывается наблюдёнными величинами
    (``observed``/``expected``/``blocked_by_reason``/``unreadable_sources``/``enumerated``/
    ``termination``), потому что «17 из 28 URL прочитано» — мера обработанных URL, а не
    вероятность отсутствия. Единственная её производная — :attr:`url_observation_ratio`,
    и она называется именно так. Поля «вероятность отсутствия» здесь нет и не заводится:
    его нельзя измерить этим прибором.

    :param denial_strength: класс отрицания — единственный носитель вердикта.
    :param reason: **обязательна** для ``unobserved``/``not_stated``/``bounded``/
        ``instrument_failed``: незнание обязано называть свою причину, иначе ``❓``
        неотличим от «ещё не смотрели».
    :param citation_quote: **обязательна** для ``explicit``: отрицание без цитаты —
        мнение, а не доказательство (I22-родственное правило для отсутствия).
    :param observed / expected: сколько источников продукта прочитано и объявлено.
    :param blocked_by_reason: ``error_kind → count`` наблюдавшихся отказов (аудит).
    :param unreadable_sources: источники/``origin``'ы, от которых не получено ничего.
    :param enumerated: наблюдалась ли **полная** энумерация пространства (реестр/каталог):
        ``None`` — неизвестно, ``False`` — заведомо нет.
    :param termination: чем закончился контур охвата (``TerminationReason`` либо имя
        объявленной процедуры полноты); ``None`` — контур не исполнялся/неизвестно.
    :param relevant_evidence_coverage: **измеренная** доля переданных оценщику фрагментов,
        которые он использовал в доказательстве: ``|{чанки, на которые ссылается
        подтверждённый evidence}| / |{чанки, переданные оценщику}|`` (0..1). Считается в
        :func:`core.pipeline.evaluate._relevant_evidence_coverage` только по **прошедшему
        цитатную сверку** evidence, поэтому «использован» означает «процитирован дословно»,
        а не «упомянут». ``None`` — **не измерено**: знаменателя нет (оценщику не передали
        ни одного фрагмента) либо замер не производился (``instrument_failed``,
        реестровое отсутствие). Ноль и «не измерено» — разные вещи и не подменяются.
    :param scope_acceptance: человек, **принявший риск** ненаблюдённой части
        (``scope_acceptance``; прежде ``scope_attested_by``). Это принятие риска, а не
        улучшение измерения: подпись не делает охват полным и не участвует в провенансе
        контракта (в ``EXECUTION_CONTRACT_FIELDS`` не входит).
    :param basis: откуда взялся класс (``registry``/``evaluator:explicit``/…) — аудит.
    """

    model_config = _BASE

    denial_strength: DenialStrength
    reason: str | None = None
    citation_quote: str | None = None
    observed: int = Field(default=0, ge=0)
    expected: int = Field(default=0, ge=0)
    blocked_by_reason: dict[str, int] = Field(default_factory=dict)
    unreadable_sources: list[str] = Field(default_factory=list)
    enumerated: bool | None = None
    termination: str | None = None
    relevant_evidence_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    scope_acceptance: str | None = None
    basis: str | None = None

    @property
    def url_observation_ratio(self) -> float | None:
        """Прочитанная доля объявленных источников — **мера URL**, не вероятность отсутствия.

        ``None``, если объявленного пространства нет (``expected == 0``): «0 из 0» —
        не единица и не ноль, а отсутствие знаменателя.
        """
        if self.expected <= 0:
            return None
        return min(1.0, self.observed / self.expected)

    def verdict(self) -> Status:
        """Статус, который этот класс отрицания разрешает, — единственное место маппинга."""
        if self.denial_strength in ABSENT_DENIAL_STRENGTHS:
            return Status.ABSENT
        if self.denial_strength in UNKNOWN_DENIAL_STRENGTHS:
            return Status.UNKNOWN
        return Status.UNKNOWN  # instrument_failed статуса не даёт (I4) — см. Cell

    def to_json(self) -> dict[str, Any]:
        """JSON-совместимое представление (хранение в ``cells.evidence_scope``).

        Производные величины (``url_observation_ratio``) пишутся в артефакт, но **не**
        читаются обратно как поля: они пересчитываются из ``observed``/``expected``,
        поэтому хранение не может с ними разойтись.
        """
        payload = self.model_dump(mode="json")
        payload["url_observation_ratio"] = self.url_observation_ratio
        return payload

    @model_validator(mode="after")
    def _check(self) -> EvidenceScope:
        strength = self.denial_strength
        if strength is DenialStrength.EXPLICIT and not (self.citation_quote or "").strip():
            raise ValueError(
                "denial_strength=explicit требует непустую citation_quote: "
                "отрицание без цитаты — мнение, а не доказательство отсутствия"
            )
        if strength in {
            DenialStrength.UNOBSERVED,
            DenialStrength.NOT_STATED,
            DenialStrength.INSTRUMENT_FAILED,
        } and not (self.reason or "").strip():
            raise ValueError(
                f"denial_strength={strength.value} требует непустую reason: "
                "незнание обязано называть причину"
            )
        if strength is DenialStrength.BOUNDED:
            if not (self.reason or "").strip():
                raise ValueError(
                    "denial_strength=bounded требует reason (процедура полноты/основание)"
                )
            if not (self.scope_acceptance or "").strip():
                raise ValueError(
                    "denial_strength=bounded требует scope_acceptance: измерение не "
                    "покрывает пространство, поэтому риск принимает человек"
                )
        if strength is DenialStrength.AUTHORITATIVE and self.enumerated is False:
            raise ValueError(
                "denial_strength=authoritative несовместим с enumerated=False: "
                "авторитетная энумерация не может быть заведомо неполной"
            )
        return self

    @classmethod
    def for_registry_absence(
        cls, *, registry_ids: Sequence[str], basis: str = "registry"
    ) -> EvidenceScope:
        """Класс ``authoritative`` для реестровой пустоты (объявленная семантика правила).

        Не догадка: ``registry_absence`` в каталоге объявляет, что отсутствие записи в
        реестре — полноценное негативное свидетельство. Здесь это объявление лишь
        записывается тем же классом, которым пользуется вердикт.
        """
        return cls(
            denial_strength=DenialStrength.AUTHORITATIVE,
            enumerated=True,
            termination="registry_enumeration",
            basis=basis,
            reason=(
                "реестровая энумерация полна по построению; "
                f"реестры: {', '.join(sorted(registry_ids)) or '—'}"
            ),
        )

    @classmethod
    def for_instrument_failure(cls, kind: str, *, detail: str | None = None) -> EvidenceScope:
        """Класс ``instrument_failed``: прибор не смог, статуса нет (I4)."""
        reason = f"instrument_failed: {kind}"
        if detail:
            reason = f"{reason} — {detail}"
        return cls(
            denial_strength=DenialStrength.INSTRUMENT_FAILED,
            reason=reason,
            termination=kind,
            basis="pipeline",
        )


class CellEvidence(BaseModel):
    """Типизированный evidence-объект (§4.4). Единственная единица доказательства (§2.4).

    K5: derived-evidence (наследование/сателлиты) обязано иметь резолв (I10/I15) и hash (I19).
    S8: evidence_id детерминирован — идемпотентность при resume/повторном Ш6.
    """

    model_config = _BASE

    evidence_id: str
    run_id: str
    product_id: str
    param_id: str
    source_kind: SourceKind
    evidence_type: EvidenceType
    source_id: str                       # I10/I15
    source_url: str | None = None        # заполняется системным резолвом
    citation_quote: str | None = None
    content_hash: str | None = None
    source_scope: SourceScope | None = None
    #: Принятие риска человеком (прежде ``scope_attested_by``). **Не** улучшает измерение:
    #: подпись говорит «знаю, чего не видел», а не «всё видел». Носитель вердикта — не она,
    #: а :class:`EvidenceScope` ячейки.
    scope_acceptance: str | None = None
    index_build_id: str | None = None
    #: 09 §8.1: партиция, из которой пришёл чанк-цитата. ``None`` — неизвестно
    #: (исторические строки/не корпусный источник); аддитивно, без backfill.
    partition_build_id: str | None = None
    #: O5: адрес **версии документа** (``dv_...``), из которой взята цитата. Берётся из
    #: ``chunk_id`` ретривнутого чанка (:func:`core.util.ids.doc_version_of`), а не из
    #: ``content_hash``: хеш — версия **текста**, он не различает уровень извлечения и
    #: версию экстрактора, поэтому два разных адреса с одним текстом были бы неотличимы.
    #: ``None`` — версия не установлена (историческая строка, неразбираемый адрес,
    #: непечённый источник); читатель обязан трактовать это как «не записано», а не как
    #: «любая версия подходит».
    doc_version_id: str | None = None
    retrieved_at: str | None = None

    @model_validator(mode="after")
    def _check(self) -> CellEvidence:
        if not self.source_id:
            raise ValueError("I10/I15: evidence обязан иметь source_id")
        # Корпусный evidence обязан нести hash (I19).
        if (
            self.source_kind
            in {SourceKind.DOCUMENTATION, SourceKind.WEB, SourceKind.DISTRIBUTION}
            and not self.content_hash
        ):
            raise ValueError("I19: корпусный evidence обязан иметь content_hash")
        # absence-evidence: scope по типу правила проверяется на уровне Cell.
        if self.source_kind is SourceKind.REGISTRY and not self.source_url:
            raise ValueError("R1: registry evidence обязан иметь source_url")
        # O5: версия документа — не свободное поле, а второй сегмент адреса. Расхождение
        # означало бы, что evidence ссылается на чанк одной версии, а объявляет другую
        # (например, перенос «приклеил» адрес от другого документа).
        if self.doc_version_id is not None:
            if not self.doc_version_id:
                raise ValueError("O5: doc_version_id не может быть пустым")
            addressed = doc_version_of(self.source_id)
            if addressed != self.doc_version_id:
                raise ValueError(
                    f"O5: doc_version_id {self.doc_version_id!r} расходится с адресом "
                    f"source_id {self.source_id!r} (там {addressed!r})"
                )
        return self


def derived_evidence_id(
    run_id: str, product_id: str, param_id: str, source_id: str, source_kind: str, seq: int
) -> str:
    """Детерминированный evidence_id (S8)."""
    import hashlib

    raw = f"{run_id}|{product_id}|{param_id}|{source_id}|{source_kind}|{seq}".encode()
    return "ev_" + hashlib.sha256(raw).hexdigest()[:24]


def absence_evidence_id(run_id: str, product_id: str, param_id: str, seq: int) -> str:
    """Детерминированный ``evidence_id`` absence-записи (S8).

    Отличие от :func:`derived_evidence_id` — отсутствие не имеет ``source_id`` (цитировать
    нечего), поэтому ключом служит литерал ``absence`` и порядковый номер записи в ячейке.
    ``NULL`` в PK невозможен, а ключ включает ``(run, product, param)`` — записи разных
    ячеек не склеиваются, даже если обе синтетические.
    """
    import hashlib

    raw = f"{run_id}|{product_id}|{param_id}|absence|{seq}".encode()
    return "ev_" + hashlib.sha256(raw).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Инварианты ячейки (§2.3) и Р-2 (§4.1)
# ---------------------------------------------------------------------------


class Cell(BaseModel):
    """Ячейка для проверки инвариантов I1–I13.

    ``evidence_scope`` — **единственный** носитель ответа «можно ли утверждать
    отсутствие» (§2.3, Вариант 2). Пока объект не заполнен, вопрос считается открытым:
    ``❌`` без него недоказуем, ``❓`` без него — аномалия (кроме исторических строк,
    см. ``invariants.check_unknown_exhaustion``).

    ``absence_rule`` — объявленное правило отсутствия параметра (свойство каталога,
    скопированное в ячейку при оценке). Прежде оно жило только в каталоге, и при чтении
    из store подставлялся ``explicit`` — то есть прочитанная ячейка утверждала правило,
    которого никто не объявлял, и I2 по прочитанным данным не перепроверялся
    (``review.promote`` работает именно с прочитанными ячейками). Теперь правило
    хранится; ``None`` — **не записано** (строки прогонов до появления колонки), и это
    не выдаётся за ``explicit``.

    ``validation_notes`` — опубликованное свидетельство о том, что с ячейкой было
    **не так** при оценке (ремонт шумового выхода модели, отказ валидации, аномалии
    §10.4), а не отладочный лог. Список, а не последняя запись: ячейку могут оценивать
    дважды (resume), и причины накапливаются (слияние — в store, см.
    ``SqliteStore.save_cell``).
    """

    model_config = _BASE

    run_id: str
    product_id: str
    param_id: str
    state: CellState
    status: Status | None = None
    source_kind: SourceKind | None = None
    #: Правило отсутствия параметра; ``None`` — не записано (legacy-строка), а не ``explicit``.
    absence_rule: AbsenceRuleType | None = None
    evidence: list[LLMEvidence] = Field(default_factory=list)
    absence_evidence: list[dict[str, Any]] = Field(default_factory=list)
    #: Сила отрицания + вектор охвата. ``None`` — не заполнен (строки, записанные до
    #: варианта 2, и ячейки наличия: у ✅/⚠️ вопрос отсутствия не стоит).
    evidence_scope: EvidenceScope | None = None
    reviewed_by: str | None = None
    # Провенанс ячейки (R7/§13): смена сборки/профиля/промпта детектируется сравнением.
    index_build_id: str | None = None
    retrieval_profile: str | None = None
    retrieval_params_json: str | None = None
    stale_reason: StaleReason | None = None
    # Свободнотекстовая аннотация причины ``pending`` (не статус, не перечисление):
    # позволяет читателю отличить конфликт источников от условного наследования.
    # Несёт и противоречие «наличие + прямое отрицание» у evaluated-ячейки (§7).
    review_reason: str | None = None
    #: Свидетельство о дефектах оценки (ремонт/отказ валидации/аномалии §10.4).
    validation_notes: list[str] = Field(default_factory=list)
    #: Происхождение ячейки в прогоне (09 §3.1/§10): ``observed`` — вердикт получен
    #: здесь, ``carried`` — перенесён из прогона-предшественника по ``cell_reuse_key``.
    #: Дефолт ``observed``: ячейка, о переносе которой никто не заявил, измерена — обратное
    #: утверждение было бы выдумкой, а не наблюдением.
    measurement_origin: MeasurementOrigin = MeasurementOrigin.OBSERVED
    #: Прогон, из которого перенесён вердикт (``carried``); ``None`` ⟺ ``observed``.
    #: Это провенанс, а не украшение: читатель карты обязан уметь дойти до измерения,
    #: которое действительно состоялось.
    carried_from_run_id: str | None = None
    #: Ключ переноса (``cell-reuse-2``), которым помечена ячейка; ``None`` — не вычислялся
    #: (ячейка высокого источника, ``failed``, модульный вызов без условий переноса).
    cell_reuse_key: str | None = None

    @model_validator(mode="after")
    def _enforce_invariants(self) -> Cell:
        # I4: failed → status = null
        if self.state is CellState.FAILED and self.status is not None:
            raise ValueError("I4: failed → status обязан быть null")
        # I13: preliminary не несёт публикуемый status
        if self.state is CellState.PRELIMINARY and self.status is not None:
            raise ValueError("I13: preliminary-ячейка не имеет разрешённого status")
        # I7: N/A только человеком (в схеме — если статус N/A, требуется reviewed_by)
        if self.status is Status.NOT_APPLICABLE and not self.reviewed_by:
            raise ValueError("I6/I7: N/A → reviewed_by обязателен")
        # I1: evaluated ✅/⚠️ → evidence ≥ 1
        if (
            self.state is CellState.EVALUATED
            and self.status in {Status.OK, Status.PARTIAL}
            and not self.evidence
        ):
            raise ValueError("I1: evaluated ✅/⚠️ → evidence ≥ 1")
        # Объявленная семантика реестрового правила: отсутствие записи в авторитетной
        # энумерации — негативное свидетельство по построению. Класс достраивается из
        # **объявленных** данных (``absence_rule`` + наличие записи), а не выдумывается,
        # и достраивается ДО проверки «❌ без класса невозможен».
        if (
            self.evidence_scope is None
            and self.status is Status.ABSENT
            and self.absence_rule is AbsenceRuleType.REGISTRY_ABSENCE
        ):
            registry_ids: list[str] = []
            for entry in self.absence_evidence:
                registry_ids.extend(str(item) for item in entry.get("registry_ids") or ())
            self.evidence_scope = EvidenceScope.for_registry_absence(
                registry_ids=registry_ids
            )
        # I2: класс отрицания обязан согласовываться со статусом — один механизм вместо
        # I2b/I2c. Для **опубликованного** ``❌`` (evaluated) объект обязателен: «нет» без
        # объявленной силы отрицания — вердикт без основания. ``pending`` с заполненным
        # статусом ничего не публикует (это предмет решения), поэтому объекта не требует;
        # исторические строки читаются ``model_construct`` и валидацию не проходят.
        scope = self.evidence_scope
        if (
            self.state is CellState.EVALUATED
            and self.status is Status.ABSENT
            and scope is None
        ):
            raise ValueError(
                "I2: ❌ требует EvidenceScope (силу отрицания и вектор охвата) — "
                "«нет» без основания не публикуется"
            )
        if scope is not None:
            if self.status is Status.ABSENT:
                if scope.denial_strength not in ABSENT_DENIAL_STRENGTHS:
                    raise ValueError(
                        "I2: ❌ требует denial_strength ∈ "
                        f"{sorted(s.value for s in ABSENT_DENIAL_STRENGTHS)}, "
                        f"получено {scope.denial_strength.value}"
                    )
            elif self.status is Status.UNKNOWN:
                if scope.denial_strength not in UNKNOWN_DENIAL_STRENGTHS:
                    raise ValueError(
                        "I3: ❓ требует denial_strength ∈ "
                        f"{sorted(s.value for s in UNKNOWN_DENIAL_STRENGTHS)}, "
                        f"получено {scope.denial_strength.value}"
                    )
            elif self.status in {Status.OK, Status.PARTIAL, Status.NOT_APPLICABLE}:
                raise ValueError(
                    "I2: объект силы отрицания относится только к ❌/❓/failed; "
                    f"статус {self.status}, класс {scope.denial_strength.value}"
                )
        if (
            self.state is CellState.EVALUATED
            and self.status is Status.ABSENT
            and not self.absence_evidence
            and scope is None
        ):
            raise ValueError("I2: evaluated ❌ → absence-evidence обязателен")
        # I4: failed несёт только ``instrument_failed`` (сбой прибора, не отрицание).
        if (
            self.state is CellState.FAILED
            and scope is not None
            and scope.denial_strength is not DenialStrength.INSTRUMENT_FAILED
        ):
            raise ValueError("I4: failed несёт только denial_strength=instrument_failed")
        # 09 §10: перенос — это адрес источника вердикта. ``carried`` без прогона-источника
        # неотличим от «измерено здесь» с ложной пометкой, а ``observed`` с источником
        # объявляет перенос, которого не было. Обе записи отвергаются как противоречивые.
        if self.measurement_origin is MeasurementOrigin.CARRIED:
            if not self.carried_from_run_id:
                raise ValueError(
                    "09 §10: measurement_origin=carried → carried_from_run_id обязателен"
                )
        elif self.carried_from_run_id is not None:
            raise ValueError(
                "09 §10: carried_from_run_id при measurement_origin=observed — "
                "противоречивый провенанс"
            )
        return self


# ---------------------------------------------------------------------------
# §4.4 Операционные объекты (персистентные строки store; T2.1)
# ---------------------------------------------------------------------------


class ScenarioBinding(BaseModel):
    """Привязка сценарий → модель (§7.4/I21). Строка ``llm_scenarios``.

    Маршрут детерминирован фазой, а не решением LLM. Для ``EVALUATOR``
    ``web_tool`` обязан быть False (I18) — это подтверждает и SQL-триггер.
    """

    model_config = _BASE

    run_id: str
    scenario: LLMScenario
    model_id: str
    model_version: str | None = None
    endpoint_kind: EndpointKind
    web_tool: bool = False
    decoder: str | None = None
    seed: int | None = None
    temperature: float | None = None
    constrained_decoding: str | None = None
    #: Хост endpoint'а без схемы и пути (V6). Нужен потому, что «куда ушёл корпус» —
    #: факт манифеста, а не догадка: ``endpoint_kind: api`` одинаково описывает и
    #: внутренний шлюз, и внешний стенд RnD. Секрет здесь не хранится никогда.
    endpoint_host: str | None = None
    #: Потолок генерации, с которым шёл вызов (V5). ``None`` — серверный дефолт
    #: провайдера; это тоже факт условия измерения, а не отсутствие значения.
    max_tokens: int | None = None

    @model_validator(mode="after")
    def _check(self) -> ScenarioBinding:
        if self.scenario is LLMScenario.EVALUATOR and self.web_tool:
            raise ValueError("I18: evaluator не может иметь web_tool")
        return self


class SearchLogEntry(BaseModel):
    """Строка ``search_log`` (§4.4, S12). Внутренний ретривал vs внешний поиск."""

    model_config = _BASE

    run_id: str
    phase: SearchPhase
    kind: SearchKind
    provider: str | None = None
    adapter_kind: AdapterKind | None = None
    language: str | None = None
    query: str | None = None
    results_count: int | None = None
    selected: int | None = None
    lexical_score: float | None = None
    dense_score: float | None = None
    fused_rank: int | None = None
    rerank_score: float | None = None
    retrieval_score: float | None = None
    termination_reason: TerminationReason | None = None
    ts: str | None = None
    log_id: int | None = None  # AUTOINCREMENT в БД


class ProtocolExhaustion(BaseModel):
    """Машиночитаемый протокол исчерпания (§5, S13). Строка ``search_exhaustion``.

    ``❓`` = «протокол исчерпан, evidence недостаточно». Объект фиксирует, что
    именно было исчерпано: контуры, бюджеты, число раундов, терминацию.
    """

    model_config = _BASE

    run_id: str
    product_id: str
    param_id: str
    contours: list[str] = Field(default_factory=list)   # исчерпанные контуры (registry/corpus/serp)
    queries: list[str] = Field(default_factory=list)    # запросы протокола
    rounds: int = 0                                     # число SERP-раундов
    budget: dict[str, Any] = Field(default_factory=dict)
    termination_reason: TerminationReason
    exhausted_at: str | None = None
    notes: str | None = None


class RegistryFact(BaseModel):
    """Реестровый факт (R1). Строка ``registry_facts``.

    **R1: 100% фактов обязаны нести ``source_url``.** ``fact_json`` — сырое
    значение, приведённое из конфиг-driven маппинга (нейтрально к реестру).
    """

    model_config = _BASE

    run_id: str
    product_id: str
    registry_id: str
    fact_json: str
    source_url: str
    fetched_at: str | None = None

    @model_validator(mode="after")
    def _check(self) -> RegistryFact:
        if not self.source_url:
            raise ValueError("R1: реестровый факт обязан иметь source_url")
        return self


class GateDecision(BaseModel):
    """Решение гейта (S9). Строка ``gate_decisions``.

    Носитель ``reviewed_by`` и аудит-след. Гейт не считается пройденным без решения,
    **привязанного к текущему предмету**: ``subject_digest`` — отпечаток того, о чём
    решали (профиль для G1/G2, очередь ревью для G3, Coverage/Capability/артефакты для
    G4). Раньше любая строка проходила гейт навсегда, независимо от состояния прогона;
    привязка к дайджесту делает устаревшее одобрение невозможным.

    ``source``/``external_ref``/``verdict`` — интеграционная часть: внешняя система
    (ревью, публикация) **присылает** решение, а не coHound его принимает. Отказ
    (``verdict=rejected``) — такая же запись, как одобрение: иначе отказ выражался бы
    молчанием, неотличимым от «ещё никто не смотрел».

    ``subject_digest is None`` (в БД — пустая строка) — решение **без привязки**:
    legacy-строки, записанные до появления привязки. Они продолжают проходить гейт
    (обратная совместимость существующих прогонов), но новый код обязан привязывать.
    """

    model_config = _BASE

    decision_id: str
    run_id: str
    gate: Gate
    target: str | None = None
    actor: str
    decided_at: str | None = None
    notes: str | None = None
    #: Отпечаток предмета решения; ``None`` — решение без привязки (legacy/явный override).
    subject_digest: str | None = None
    #: Кто решил: ``internal`` (человек в coHound) или ``external`` (внешняя система).
    source: GateSource = GateSource.INTERNAL
    #: Идентификатор решения во внешней системе — единственная ссылка обратно.
    external_ref: str | None = None
    #: Вердикт: ``approved``/``rejected``. Отказ блокирует гейт так же явно, как
    #: отсутствие решения, но с другим сообщением (кто отказал и почему).
    verdict: GateVerdict = GateVerdict.APPROVED

    @model_validator(mode="after")
    def _check(self) -> GateDecision:
        if not self.actor:
            raise ValueError("решение гейта обязано иметь actor (человека)")
        if self.source is GateSource.EXTERNAL:
            if not (self.external_ref or "").strip():
                raise ValueError(
                    "внешнее решение обязано нести external_ref (ссылка на решающую систему)"
                )
            if not (self.notes or "").strip():
                raise ValueError(
                    "внешнее решение обязано нести notes: решение без причины — не аудит-след"
                )
        return self


class ProfileAttestationRow(BaseModel):
    """Аттестация **профиля** пакета-якоря для гейта G1/G2 — строка ``profile_attestation``.

    Профиль квази-константен: когорта/роли/объём/паспорта меняются 2–5 раз в год, а
    прогоны идут еженедельно. Поэтому человек аттестует **содержимое** профиля один раз
    (``profile_digest``), и последующие прогоны с тем же профилем не требуют повторного
    решения. Это durable-строка ``state.db`` пакета, а не per-run ``gate_decisions``:
    аттестация переживает прогоны, но **не** переносится молча на изменённый профиль.

    ``digest_material`` — JSON-компоненты дайджеста (``profile_digest``): по ним
    вычисляется, что именно изменилось, когда текущий профиль перестал совпадать с
    аттестованным. ``superseded_at`` — отзыв аттестации (не удаление строки: аудит).

    Имя таблицы намеренно не ``attestation``: та — cell-level принятие риска
    (``scope_acceptance``, Вариант 2), другая сущность.
    """

    model_config = _BASE

    attestation_id: str
    case_id: str
    package_id: str
    anchor: str
    gate: Gate
    profile_digest: str
    digest_material: dict[str, Any]
    attested_by: str
    attested_at: str
    superseded_at: str | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def _check(self) -> ProfileAttestationRow:
        if self.gate not in (Gate.G1, Gate.G2):
            raise ValueError(
                f"аттестация профиля относится только к G1/G2, получено {self.gate.value}"
            )
        if not self.attested_by:
            raise ValueError("аттестация профиля обязана иметь attested_by (человека)")
        if not self.profile_digest:
            raise ValueError("аттестация профиля обязана иметь profile_digest")
        if not self.digest_material:
            raise ValueError(
                "аттестация профиля обязана нести digest_material: без компонентов "
                "невозможно назвать изменившееся"
            )
        return self


class UpstreamRef(BaseModel):
    """Ссылка на upstream-факт при наследовании (I8, K5).

    Используется ``check_derived_evidence``: унаследованный ✅/⚠️ обязан нести
    derived-evidence с резолвом и hash.
    """

    model_config = _BASE

    product_id: str
    param_id: str
    source_id: str
    source_url: str | None = None
    content_hash: str | None = None


class AlignmentReport(BaseModel):
    """Alignment-условия Comparability (§6). Строка-сводка манифеста."""

    model_config = _BASE

    snapshot_date: str
    retrieval_profile: str
    retrieval_cutoff_K: int
    embed_model_id: str | None = None
    index_build_id: str
    package_version: str
    catalog_version: str
    editions_consistent: bool = True
    jurisdictions_consistent: bool = True
    violations: list[str] = Field(default_factory=list)


class CrawlSessionProvenance(BaseModel):
    """Провенанс одного сеанса краула в манифесте прогона (§4.4, Phase 2).

    Информационный снимок наблюдаемого исполнения краула (движки/виды источника, статус,
    счётчики). Не входит в границы версий (§13): смена движка не меняет методологию.
    """

    model_config = _BASE

    session_id: str
    profile: str
    status: str
    engines: list[str] = Field(default_factory=list)
    kinds: list[str] = Field(default_factory=list)
    total: int = 0
    fetched: int = 0
    failed: int = 0
    #: D1/G: сеанс был **политико-усечён** (``max_documents``/``max_depth``). Информационный
    #: durable-флаг (не §13): нужен отчёту, чтобы не трактовать усечённый корпус как
    #: ``exhaustive`` (срабатывает аномалия для человека — :func:`collect_crawl_truncation_anomalies`).
    truncated: bool = False
    #: D1/G: продукты с усечённым краулом (для аномалии ``exhaustive``-absence по продукту).
    truncated_products: list[str] = Field(default_factory=list)


class CrawlProvenance(BaseModel):
    """Провенанс краула прогона (§4.4, Phase 2): сеансы + агрегаты движков/видов + дайджест.

    ``engines``/``kinds`` — агрегат по всем сеансам; канонизируется (sorted, dedup)
    валидатором ``mode="after"`` при конструировании/чтении. ``heterogeneous`` — durable
    флаг неоднородности транспорта (``len(set(engines)) > 1``), вычисляется тем же
    валидатором: объявленное поле (а не ``computed_field``), поэтому переживает
    ``model_validate`` под ``extra="forbid"`` и round-trip через манифест.
    ``crawl_sources_digest`` — отпечаток конфигурации ``crawl_sources.yaml``
    (:func:`core.pipeline.crawl_sources.crawl_sources_digest`). Информационен: не влияет
    на Comparability/§6 и не входит в diff-границы (§13).
    """

    model_config = _BASE

    sessions: list[CrawlSessionProvenance] = Field(default_factory=list)
    engines: list[str] = Field(default_factory=list)  # aggregate, sorted, dedup (by construction)
    kinds: list[str] = Field(default_factory=list)  # aggregate, sorted, dedup (by construction)
    crawl_sources_digest: str | None = None
    #: Неоднородность транспорта (несколько движков) — информационный durable-флаг.
    heterogeneous: bool = False

    @model_validator(mode="after")
    def _canonicalize(self) -> CrawlProvenance:
        """Сортирует+дедуплицирует ``engines``/``kinds`` и выводит ``heterogeneous``."""
        object.__setattr__(self, "engines", sorted(set(self.engines)))
        object.__setattr__(self, "kinds", sorted(set(self.kinds)))
        object.__setattr__(self, "heterogeneous", len(set(self.engines)) > 1)
        return self


class RunManifest(BaseModel):
    """``run_manifest.yaml`` (§9, §13): агрегат всех версий и параметров прогона (R7).

    Пишется ``runner.py`` в конце; иммутабелен. Несёт всё, что влияет на результат:
    версии, ``llm_scenarios``, индексную сборку, ретривал-профиль+K, бюджет, политику расчёта.
    """

    model_config = _BASE

    run_id: str
    case_id: str
    created_at: str
    mode: RunMode
    methodology_version: str
    package_version: str
    catalog_version: str
    status_dict_version: str
    scoring_policy_version: str
    anchor_product_id: str
    cohort_ref: str | None = None
    #: 09 §12.5/§13: **дайджест состава когорты** — слагаемое ``run_id``
    #: (:func:`core.pipeline.plan.derive_run_id`) и поле ``execution_contract_digest``.
    #: Прежде его не было ни на манифесте, ни в ``runs``, поэтому при расхождении
    #: контракта ``hound diff`` не мог назвать ось и объявлял «предел диагностики».
    #: Значение — **проекция** объявленного входа (тот же дайджест, что вошёл в
    #: ``run_id``), а не второй расчёт. ``None`` — не записано (строка прошлой схемы).
    cohort_digest: str | None = None
    #: 09 §12.5/§13: **дайджест seed-набора** — то же слагаемое ``run_id`` и поле
    #: контракта (``--urls`` полностью заменяет набор и входит сюда). ``None`` — не
    #: записано (legacy-строка), а не «набор пуст»: пустой набор даёт свой дайджест.
    seeds_digest: str | None = None
    #: §13.6/09 §13.6: **срез** прогона — ``full`` | ``only_failed`` | ``product:<id>``.
    #: ``--only-product`` меняет смысл среза, поэтому без поля читатель отчёта не может
    #: отличить полное измерение от частичного, не заглядывая в ``state.db``. Провенанс,
    #: ``run_id`` **не** меняется (срез уже входит в материал ``run_id``, ``plan.py``).
    run_scope: str | None = None
    #: Сериализованный :class:`ProductPlanSet` (mode/blocked/reasons) — план по продуктам,
    #: который прогон фактически исполнял. Раньше не персистился ни разу.
    product_plan: dict[str, Any] | None = None
    #: 09 §2.4/§6.2: эффективный **цикл** прогона (``--cycle-id`` > ``run.yaml`` >
    #: ``snapshot_date``). Он входит в материал ``run_id``, то есть является условием
    #: измерения, а не подписью: без поля читатель артефакта не мог назвать цикл, не
    #: заглядывая в ``state.db``, а два прогона разных циклов различались только ``run_id``.
    #: ``None`` — не записано (legacy-строка), а не «цикл не объявлен».
    cycle_id: str | None = None
    #: 09 §12.3: прогон, **против которого** считался этот (предшественник для
    #: carry-forward). Поле было объявлено только у :class:`ProductPlanSet` и никем не
    #: заполнялось, поэтому читатель артефакта не видел, с чем сравнивался прогон.
    #: ``None`` — предшественника нет (или он не записан): догадка запрещена (A3).
    #: В материал ``run_id`` **не** входит: иначе идентичность зависела бы от истории.
    previous_run_id: str | None = None
    #: §13: класс изменения относительно предшественника (``none``/``cohort_*``/
    #: ``execution_change``/``methodology_change``) — тот же, что записан в lineage:
    #: второго расчёта нет, значение приходит из строки lineage прогона.
    change_class: str | None = None
    focus_modules: list[str] = Field(default_factory=list)
    # Индексный/ретривал-контур (§13, I16/I20/S1)
    index_build_id: str | None = None
    embed_model_id: str | None = None
    #: Устройство, **на котором считались векторы** (ось ``embed.device``, :class:`EmbedDevice`).
    #: Наблюдаемое поле: ось уже входит в ``execution_contract_digest``, но без поля «на чём
    #: считались векторы» нельзя прочитать запросом — только восстановить из контракта.
    #: Имя совпадает с именем поля контракта (``report._contract_field_attr`` сопоставляет
    #: их напрямую), иначе диагностика назвала бы ось «недоступной» вместо «изменённой».
    embed_device: str | None = None
    chunker_version: str | None = None
    extractor_version: str | None = None
    lexical_engine_version: str | None = None
    vector_store: str | None = None
    retrieval_profile: str | None = None
    retrieval_cutoff_K: int | None = None
    retrieval_params_json: str | None = None
    # Бюджеты и ранжирование (I23, K6)
    search_effort_budget_json: str | None = None
    requested_ranking: RankingPolicy | None = None
    computed_ranking: RankingPolicy | None = None
    #: §6/B-1 (v2.1): порог неразличимости ранжирования (доля диапазона агрегата, дефолт 0).
    ranking_epsilon: float = 0.0
    #: A3 (v2.1): провенанс настроек ``{setting: {"source": "default"|"measured",
    #: "experiment_id"?: str, "value"?: Any, "by_level"?: {str: Any}}}``; фиксирует, что
    #: тюнится измерением, а не предвидением. ``value``/``by_level`` — зафиксированное
    #: значение измеренной ручки (скаляр либо карта по уровням/осям).
    settings_provenance: dict[str, dict[str, Any]] = Field(default_factory=dict)
    # Маршрутизация моделей (I21)
    llm_scenarios: list[ScenarioBinding] = Field(default_factory=list)
    # Расчёт (§6)
    capability_policy: str = "resolved-only-v1"
    unknown_policy: str = "excluded"
    partial_score: float = 0.5
    snapshot_date: str | None = None
    # Деплой-провенанс (07 §7, R7/§13): профиль зон и версия контракта шва.
    # Опциональны и backward-compatible; заполняются внешним манифест-писателем.
    deploy_profile: str | None = None
    edge_schema_version: str | None = None
    edge_capabilities: dict[str, Any] = Field(default_factory=dict)  # версии edge-компонентов (gateway)
    #: §4.4/Phase 2: провенанс краула (сеансы/движки/виды источника + дайджест источников).
    #: Информационен — не влияет на §6/Comparability и не входит в diff-границы (§13).
    crawl_provenance: CrawlProvenance = Field(default_factory=CrawlProvenance)
    #: Частичное извлечение — **числом**, а не потолком: ``{"documents": n, "by_product":
    #: {...}, "reasons": {...}, "pages_converted": x, "pages_declared": y, "chars": z,
    #: "by_product_pages": {...}}``. Документ, прочитанный на 7 страницах из 2763,
    #: обязан быть виден в манифесте, а не растворяться в числе документов корпуса.
    #: Наблюдение (как ``crawl_provenance``): в границы версий §13 не входит — это факт
    #: о том, что прибор прочитал, а не смена условий измерения.
    partial_docs: dict[str, Any] = Field(default_factory=dict)
    #: §13 (Backlog B): ось стратегии краула (``run.yaml:crawl.strategy``) и каноническая
    #: форма диапазона задержек (``run.yaml:crawl.rate_limit``, :func:`canonical_rate_limit`).
    #: Это **операционные** дефолты commodity-слоя (A2): их смена — граница класса
    #: ``execution_change`` (§13), не ``methodology_change``. ``None`` — секция ``crawl``
    #: не задана (граница не заполняется, манифесты без ``crawl`` не меняются).
    crawl_strategy: str | None = None
    crawl_rate_limit: str | None = None
    #: §13 (D1/D2): операционные оси контроля краула (``run.yaml:crawl``). Их смена —
    #: граница класса ``execution_change`` (§13). ``None``/пусто — секция/поле не заданы
    #: (манифесты без новых осей не меняются).
    crawl_max_workers: int | None = None
    #: P4/§13: ось CPU-параллелизма экстракции (``run.yaml:crawl.extract_workers``). Отдельная
    #: от ``crawl_max_workers`` (сетевой fetch-пул); её смена — та же граница класса
    #: ``execution_change`` (§13). ``None`` — ось не задана (поведение неизменно).
    crawl_extract_workers: int | None = None
    #: P4/OOM/§13: рециклинг воркеров локальной экстракции
    #: (``crawl.extract_max_tasks_per_child``). Та же граница класса ``execution_change``
    #: (§13): меняет условия измерения. ``None`` — ось не задана (манифесты не меняются).
    crawl_extract_max_tasks_per_child: int | None = None
    #: P0/§13: потолок одного HTTP-ответа/retained-файла (``crawl.max_response_bytes``).
    #: Операционная ось (``execution_change``), не methodology. ``None`` — run-слой не
    #: задавал явно (в дайджесте зафиксирован эффективный дефолт 20 MiB).
    crawl_max_response_bytes: int | None = None
    #: P5/§13: уровень извлечения документных форматов (``crawl.document_level``). Его смена —
    #: та же граница класса ``execution_change``. ``None`` — не разрешён явно (в дайджесте
    #: всё равно зафиксирован эффективный ``full`` по app-дефолту).
    crawl_document_level: str | None = None
    crawl_max_depth: int | None = None
    crawl_max_documents: int | None = None
    crawl_exclude_paths: list[str] = Field(default_factory=list)
    crawl_exclude_documents: list[str] = Field(default_factory=list)
    #: §13 (N6): стабильный дайджест **разрешённой** конфигурации контроля краула
    #: (app ``deploy.yaml:crawl`` < run ``run.yaml:crawl`` < per-site ``domain_profiles``,
    #: per site key). Одно поле ловит смену осей на **любом** слое (включая app/per-site,
    #: которые индивидуальные поля ``crawl_max_*`` не покрывают) — и потому помечает
    #: ``execution_change`` (§13), не ``methodology_change``.
    crawl_config_digest: str | None = None
    #: §13 (E2): стабильный дайджест **загруженных** package-scoped спецификаций реестров
    #: (``packages/{case_id}/registries/*.yaml``), отсортированных по ``registry_id``
    #: (канонические байты — детерминированная JSON-сериализация валидированного
    #: ``RegistryConfig``). Персистится в ``runs.registry_spec_digest`` (D1/E2) и потому
    #: читается обратно :meth:`SqliteStore.read_manifest`, делая смену спецификации реальной
    #: границей ``execution_change`` в ``hound diff`` (§13), не ``methodology_change``.
    #: ``None`` — реестры не сконфигурированы (пакеты без реестров не меняются: поле
    #: аддитивно/опционально).
    registry_spec_digest: str | None = None
    #: §13 / 09 §12.5: отпечаток **условий измерения** (``execution_contract_digest``,
    #: :func:`core.pipeline.digests.execution_contract_digest`) — то, что делает два цикла
    #: сравнимыми. ``hound diff`` сравнивает его вместо ``index_build_id``: новая
    #: композиция при тех же условиях измерения — разрешённый diff с баннером
    #: ``content_change``. Это граница класса ``methodology_change`` (меняет определение
    #: Comparability, §12.5). ``None`` — legacy-прогон: дайджест не вычислялся и
    #: **не фабрикуется**; для пары с ``None`` действует прежнее сравнение
    #: :data:`~core.pipeline.report.EXECUTION_FIELDS`.
    execution_contract_digest: str | None = None
    #: Политика гейтов прогона (``run.yaml:gates``) — **процесс**, не измерение: в
    #: ``EXECUTION_CONTRACT_FIELDS`` не входит. Видна в манифесте, чтобы делегирование
    #: гейта наружу было явным фактом прогона, а не подразумеваемым.
    gate_policies: dict[str, str] = Field(default_factory=dict)
    #: След гейтов прогона — по строке на гейт: ``[{gate, state, external_ref, actor,
    #: notes, decided_at, verdict?}]``. ``state`` называет, чем гейт прошёл:
    #: ``decided`` (решение записано в ``gate_decisions``), ``delegated`` (делегирован
    #: внешнему процессу, ``run.yaml:gates.<gate>=waived``) либо ``not_required``
    #: (не блокирующий гейт G1/G2/G3: решения не требуется). Список строится **одним**
    #: механизмом (:func:`core.pipeline.gates.gate_trail`) и обязателен: прогон, прошедший
    #: гейт без внутрисистемного одобрения или вовсе без решения, не должен выглядеть как
    #: прогон с одобрением (невидимый waiver/пропуск — именно та ошибка, которой нельзя
    #: допускать). Имя ключа сохранено ради обратной совместимости артефактов: у прогонов
    #: до 0.1.0 там лежали только делегирования.
    gate_waivers: list[dict[str, Any]] = Field(default_factory=list)
    #: I2b/I2c → Вариант 2: **принятия риска** человеком, использованные прогоном
    #: (``product_id → actor``). Тот же класс, что ``reviewed_by``: принятие расширяет
    #: достижимость ``❌`` только для класса ``bounded`` (там, где измерение не покрывает
    #: пространство), но не меняет измерение. Поэтому в ``EXECUTION_CONTRACT_FIELDS``
    #: поле **не входит** (как и ``gate_policies``): смена подписанта не создаёт новый
    #: ``run_id`` и не рвёт межцикловый diff. Видно в манифесте, чтобы опубликованный
    #: ``❌`` был прослеживаем к человеку, принявшему риск.
    #:
    #: Имя сменилось с ``scope_attestations``: подпись — **принятие риска**
    #: (``scope_acceptance``), а не аттестация полноты. Старые манифесты читаются:
    #: ``_migrate_legacy_keys`` перекладывает исторический ключ.
    scope_acceptance: dict[str, str] = Field(default_factory=dict)
    #: 13-manual-artifacts §3.4: доля ручного — **публикуется числом, потолка нет**.
    #: ``{count, share_of_corpus, bytes, by_reason, by_product}``: ``count`` — предъявленные
    #: артефакты, дошедшие до корпуса; ``share_of_corpus`` — их доля в документах корпуса;
    #: ``by_product`` содержит **все** продукты плана, включая нули — правило симметрии
    #: («одинаково доступно всем продуктам», регламент §10) видно, а не подразумевается.
    #: В ``EXECUTION_CONTRACT_FIELDS`` входит только ``manual_artifacts_digest``: сам блок —
    #: наблюдение прогона (сколько байтов реально предъявлено), а не условие измерения.
    manual_artifacts: dict[str, Any] = Field(default_factory=dict)
    #: 13-manual-artifacts §3.2: отпечаток **объявленных** артефактов — поле контракта
    #: (``EXECUTION_CONTRACT_FIELDS``), поэтому ``hound diff`` называет его изменение сменой
    #: условий измерения, а не «неизвестно чем». ``None`` — legacy-манифест: дайджест не
    #: вычислялся и не фабрикуется.
    manual_artifacts_digest: str | None = None
    #: A1/§6: профиль публикуемости карты — **блок манифеста**
    #: (:class:`~core.domain.scoring.PublishabilityProfile` в JSON-форме), а не пересчёт
    #: читателя. Класс и числа, из которых он выведен, обязаны быть в записи
    #: воспроизведения: иначе опубликованный отчёт невозможно отличить от карты,
    #: которую публиковать нельзя. В ``EXECUTION_CONTRACT_FIELDS`` поле **не** входит —
    #: это результат измерения, а не его условие; смена класса не создаёт новый
    #: ``run_id``. Пустой словарь — legacy-манифест (профиль не вычислялся).
    publishability: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_keys(cls, data: Any) -> Any:
        """Читает манифесты/вызовы со старым ключом ``scope_attestations``.

        Переименование поля — часть варианта 2 (подпись = принятие риска). Строки
        манифеста и вызовы, записанные до переименования, обязаны оставаться читаемыми:
        ключ перекладывается, значение не пересчитывается.
        """
        if isinstance(data, dict) and "scope_attestations" in data:
            payload = dict(data)
            legacy = payload.pop("scope_attestations")
            payload.setdefault("scope_acceptance", legacy or {})
            return payload
        return data


class Violation(BaseModel):
    """Типизированное нарушение инварианта (T2.2).

    ``code`` — машинная ссылка на инвариант (``"I2b"``, ``"I19"``, …), чтобы CI/ревью
    ссылались на него программно.
    """

    model_config = _BASE

    code: str
    message: str
    field: str | None = None


# ---------------------------------------------------------------------------
# §4.4 Корпус и индекс (T7.3/T7.4, W8) — персистентные строки store
# ---------------------------------------------------------------------------


class CorpusDoc(BaseModel):
    """Строка ``corpus_docs`` (§4.4, Ш5): сырой документ с retention-путём и версией.

    ``raw_path`` — retention байтов (§3.2); ``extractor_version`` позволяет re-extract
    без ре-фетча. ``render_mode`` — строка (не ``ports.FetchMode``): домен L0 не
    импортирует порты L2 (матрица 01 §1). Имя поля/колонки сохранено (D2 не меняет DDL).

    **Частичность (``partial``/``partial_reason``).** Признак приходит из
    :class:`core.ports.types.ExtractedDoc` и обязан пережить границу корпуса: раньше он
    умирал здесь, и документ, прочитанный на 7 страницах из 2763, был в корпусе и в
    отчёте неотличим от прочитанного полностью. Молчание непрочитанных страниц
    превращалось в «вендор не заявил» и далее в ``❌`` (I2/I2c) — ровно та тихая потеря,
    ради видимости которой проект существует.

    ``page_count`` — сколько страниц **заявлено документом**, ``pages_converted`` —
    сколько реально попало в текст (числом, а не прилагательным); ``extracted_chars`` —
    длина извлечённого текста. Любое из чисел ``None``, если экстрактор его не сообщил:
    выдумывать нельзя (A3).

    Инвариант (``_check_partial``): ``partial`` требует непустого ``partial_reason`` и
    запрещает ``partial_reason``/``pages_converted`` без самого ``partial`` (ложная
    провенанс-запись); счётчики страниц не могут противоречить друг другу. Пустой
    текст как условие проверяет ``ExtractedDoc`` (у строки ``corpus_docs`` текста нет:
    он живёт в чанках).
    """

    model_config = _BASE

    doc_id: str
    product_id: str
    url: str
    fetched_at: str | None = None
    content_hash: str | None = None
    raw_path: str | None = None
    extractor_version: str | None = None
    render_mode: str | None = None
    lang: str | None = None
    #: Документ прочитан **не целиком** (часть страниц/членов не сконвертирована).
    partial: bool = False
    #: Почему частично: статус движка, сколько страниц получено из скольких, уровень.
    partial_reason: str | None = None
    #: Заявленное документом число страниц; ``None`` — экстрактор числа не сообщает.
    page_count: int | None = None
    #: Сколько страниц реально попало в текст; ``None`` — не измерено.
    pages_converted: int | None = None
    #: Длина извлечённого текста (не сумма чанков: чанки могут быть отброшены I19).
    extracted_chars: int | None = None
    #: HTTP-статус, которым сервер ответил **в этом цикле**; ``None`` — сервер не отвечал
    #: (документ прочитан из retained-байтов либо наблюдение не записано). Поле не
    #: провенанс-в-никуда: правило «``❌`` нельзя публиковать на неподтверждённом
    #: наблюдении» (I2c/``09`` §11.2) обязано спрашивать происхождение наблюдения здесь.
    http_status: int | None = None
    #: Происхождение наблюдения: ``live``/``revalidated`` (сервер ответил) либо
    #: ``reused``/``stale`` (взяты retained-байты). ``None`` — наблюдение не записано
    #: (legacy-строки) ⇒ ``❌`` не публикуется: наследовать ``url_state.reuse_kind``
    #: нельзя, там последняя классификация, в том числе из прошлого цикла.
    reuse_kind: ReuseKind | None = None

    @model_validator(mode="after")
    def _check_partial(self) -> CorpusDoc:
        """Держит инвариант частичности на границе корпуса (по образцу ``ExtractedDoc``)."""
        if self.partial:
            if not (self.partial_reason or "").strip():
                raise ValueError(
                    "corpus_docs: partial=True без partial_reason — частичный документ "
                    "обязан называть, почему он частичный"
                )
        else:
            if self.partial_reason is not None:
                raise ValueError(
                    "corpus_docs: partial_reason при partial=False — причина частичности "
                    "без самой частичности"
                )
            if self.pages_converted is not None:
                raise ValueError(
                    "corpus_docs: pages_converted при partial=False — счётчик прочитанных "
                    "страниц без частичности"
                )
        if self.page_count is not None and self.page_count < 0:
            raise ValueError("corpus_docs: page_count не может быть отрицательным")
        if self.pages_converted is not None:
            if self.pages_converted < 0:
                raise ValueError("corpus_docs: pages_converted не может быть отрицательным")
            if self.page_count is not None and self.pages_converted > self.page_count:
                raise ValueError(
                    "corpus_docs: pages_converted больше page_count — прочитано больше "
                    "страниц, чем документ заявляет"
                )
        if self.extracted_chars is not None and self.extracted_chars < 0:
            raise ValueError("corpus_docs: extracted_chars не может быть отрицательным")
        return self


class CorpusMember(BaseModel):
    """Строка ``corpus_members`` (B3): член контейнера — **производный** вид архива.

    Идентичность — ``(archive_content_hash, member_path, product_id)`` (§6.7): байты члена
    отдельно не ретейнятся (архив — сырьё, I19), член ре-выводится из ретейленного архива
    тем же ``open_member``. ``member_path`` — **нормализованный** путь
    (:func:`core.util.container_policy.normalize_member_path`), ``archive_path`` — путь
    ретейленного архива, ``doc_id`` — документ корпуса, если член ингестнут. ``product_id``
    входит в ключ (схема 3.13.0): один и тот же архив в двух продуктах — две строки
    провенанса, а не перезапись чужой.

    ``skip_reason`` заполнен ⟺ член **не** ингестнут (``doc_id is None``): видимый пропуск
    с причиной вместо тихого исчезновения. ``extracted_content_hash`` — хеш извлечённого
    текста (тот же, что у ``CorpusDoc``), а не байтов члена.
    """

    model_config = _BASE

    archive_content_hash: str
    member_path: str
    product_id: str
    run_id: str | None = None
    #: Корень спуска (URL, которым архив был допущен в корпус).
    root_url: str | None = None
    archive_url: str | None = None
    archive_path: str | None = None
    #: Синтетический URL члена (``{archive_url}!/{member_path}``) — адрес провенанса.
    member_url: str | None = None
    member_format: str | None = None
    declared_size: int | None = None
    extracted_content_hash: str | None = None
    doc_id: str | None = None
    skip_reason: str | None = None
    depth: int = 0
    created_at: str | None = None


class ArchiveObservation(BaseModel):
    """Строка ``archive_observations`` (B3): спуск в контейнер — прогноз и исход.

    Запись существует для **каждого** контейнера, включая пустой и нечитаемый: «спуск
    ничего не дал» — факт, который обязан быть так же виден, как «спуск дал десять
    документов». ``forecast`` фиксируется **до** извлечения членов
    (:func:`core.util.container_policy.archive_forecast`) и переживает отказ посреди
    спуска; ``outcome`` — что реально произошло (ингестнуто/пропущено по причинам,
    сработавший бюджет, остановка по циклу).

    ``observation_id`` — детерминированный отпечаток (прогон + продукт + корень + архив +
    глубина): повторный спуск того же архива не плодит строки.
    """

    model_config = _BASE

    observation_id: str
    run_id: str | None = None
    product_id: str
    root_url: str
    archive_url: str
    archive_content_hash: str
    archive_path: str | None = None
    depth: int = 0
    descent: ArchiveDescent = ArchiveDescent.FULL
    status: ArchiveDescentStatus
    forecast: dict[str, Any] = Field(default_factory=dict)
    outcome: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = None
    created_at: str | None = None


class CorpusChunk(BaseModel):
    """Строка ``corpus_chunks`` (§4.4, Ш5): единица индексации с привязкой к сборке.

    ``context_prefix`` не входит в валидацию цитат (§4.4) — цитата обязана быть
    подстрокой ``text``. ``build_id`` привязывает чанк к индексной сборке (S5/I19).
    """

    model_config = _BASE

    chunk_id: str
    doc_id: str
    product_id: str
    url: str | None = None
    heading_path: str | None = None
    lang: str | None = None
    chunk_no: int | None = None
    text: str
    context_prefix: str | None = None
    content_hash: str | None = None
    #: Версия экстрактора, породившего текст чанка (re-extract без ре-фетча, §3.2/I19).
    extractor_version: str | None = None
    build_id: str | None = None
    embed_model_id: str | None = None
    embedded_at: str | None = None


class IndexManifestEntry(BaseModel):
    """Строка ``index_manifest`` (§4.4, K1): иммутабельная сборка × продукт-партиция (I17).

    Один ``index_build_id`` даёт N партиций (по продукту). ``store_path`` указывает на
    ``index_store/{package}/{build}/{product_id}/``.
    """

    model_config = _BASE

    index_build_id: str
    #: Сборка продукт-партиции (09 §8.1). ``None`` у legacy-строк: читатель падает
    #: обратно на ``index_build_id`` (MVP-4 шаг 1: ``pb := index_build_id``).
    partition_build_id: str | None = None
    product_id: str
    package_id: str | None = None
    partition_key: str | None = None
    snapshot_date: str | None = None
    chunker_version: str | None = None
    #: Композитная версия экстрактора, использованного при сборке (build-id witness, §13).
    extractor_version: str | None = None
    embed_model_id: str | None = None
    embed_dim: int | None = None
    lexical_engine_version: str | None = None
    vector_store: str | None = None
    #: Отпечаток содержимого продукт-партиции (chunk_id × content_hash), независимый от векторов.
    content_digest: str | None = None
    store_path: str | None = None
    created_at: str | None = None
    stats_json: str | None = None


class CrawlSession(BaseModel):
    """Строка ``crawl_sessions`` (Lane B, §4.4): durable намерение краула + агрегированный прогресс.

    Ядро владеет сеансом (одинаков для ``local``/``split``), исполнение — сменный порт.
    Счётчики ``total/pending/running/fetched/failed/cancelled`` — агрегат по элементам сеанса,
    поддерживается транзакционно при записи элемента. ``options_json`` — сериализованные опции
    сеанса (from-scratch, only-фильтры); ``provenance_json`` — движки/источники (фаза провенанса).
    Статус ``CrawlSessionStatus`` — операционная сущность краула, а не доменная ячейка.
    """

    model_config = _BASE

    session_id: str
    #: Soft-link на прогон: ``None`` для standalone-сеанса (``runs/_crawl/{session_id}/``),
    #: не привязанного к строке ``runs``; FK на ``runs`` сохранён, ``NULL`` FK допустим.
    run_id: str | None = None
    #: Деплой-профиль зон: ``local`` (один процесс) | ``split`` (DMZ-планировщик), а также
    #: производные (``split-hardened`` и пр.). Значения — свободная строка: допустимый набор
    #: задаёт composition root (``core.cli.wiring.SUPPORTED_PROFILES``); домен L0 не
    #: импортирует ``cli`` (матрица слоёв 01 §1), поэтому здесь значение не валидируется.
    profile: str
    status: CrawlSessionStatus
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    total: int = 0
    pending: int = 0
    running: int = 0
    fetched: int = 0
    failed: int = 0
    cancelled: int = 0
    error: str | None = None
    #: Сериализованные опции сеанса (from-scratch, only-фильтры).
    options_json: str | None = None
    #: Движки/источники сеанса (заполняется в фазе провенанса).
    provenance_json: str | None = None
    #: F1: свидетельство живости — кто исполняет сеанс (``host/pid/старт процесса``,
    #: ``core.pipeline.liveness.current_owner``). Это **не** блокировка: конфликт двух
    #: владельцев виден, а не разрешается автоматически (`10` §Д-6, К-4).
    owner: str | None = None
    #: F1: когда владелец последний раз подавал признаки жизни (ISO-8601 UTC). Протухший
    #: ``running`` распознаёт recovery-on-open и переводит в ``interrupted``.
    heartbeat_at: str | None = None


class CrawlSessionItem(BaseModel):
    """Строка ``crawl_session_items`` (Lane B, §4.4): per-URL элемент сеанса краула.

    Per-URL элемент сеанса: провенанс попытки (``fetch_mode``/``extract_route``/``strategy``/
    ``http_status``/``content_type``/``lang``) и retention (``content_hash``/``raw_path``); ключ —
    ``session_id``, статус — ``CrawlItemStatus``. ``engine``/``kind`` — движок и вид источника
    (провенанс сеанса). ``error_kind`` переиспользует ``FailureKind``: сбой краула →
    ``source_unavailable``, сбой извлечения → ``parser_failed``. ``status`` — часть
    операционной таблицы, а не доменной ячейки.
    """

    model_config = _BASE

    session_id: str
    product_id: str
    url: str
    status: CrawlItemStatus
    attempts: int = 0
    #: D1: глубина URL в BFS-frontier seed-root (seeds — 0; ссылка с глубины ``d`` → ``d+1``).
    #: Персистится для best-effort resume; ``None`` — глубина не зафиксирована (старые записи).
    depth: int | None = None
    engine: str | None = None
    kind: str | None = None
    fetch_mode: str | None = None
    extract_route: str | None = None
    #: Ось стратегии краула (``auto|honest|hybrid``) — фактически применённая стратегия фетча.
    strategy: str | None = None
    http_status: int | None = None
    content_type: str | None = None
    lang: str | None = None
    content_hash: str | None = None
    raw_path: str | None = None
    error_kind: str | None = None
    error: str | None = None
    first_seen: str | None = None
    last_attempt: str | None = None

    @field_validator("error_kind")
    @classmethod
    def _validate_error_kind(cls, value: str | None) -> str | None:
        """Валидирует ``error_kind`` против :class:`FailureKind` (Ф0).

        Значения — persisted-данные, поэтому поле остаётся ``str``, но запись и чтение
        проходят один и тот же валидатор: неизвестный класс — явная ошибка, а не тихий
        дрейф. Документированных legacy-значений сверх ``FailureKind`` нет (все
        наблюдённые в прогонах значения — ``robots_disallow``/``attempts_exhausted``/
        ``redirect_out_of_scope`` — теперь канонические члены enum'а).
        """
        if value is None or value == "":
            return None
        known = _KNOWN_FAILURE_KINDS
        if value not in known:
            raise ValueError(
                f"error_kind={value!r} вне FailureKind "
                f"(допустимо: {', '.join(sorted(known))})"
            )
        return value


class ExtractQueueItem(BaseModel):
    """Строка ``extract_queue``: durable передача добытого документа в конверсию.

    Носитель контракта «краул выполнен ≠ корпус собран» (17 §1/§4): краул записывает
    факт ``(session_id, product_id, url, content_hash, raw_path, level, pdf_pipeline)``,
    потребитель конверсии берёт строку (``claimed``) и объявляет готовность. Строки
    пишет **только** краул, состояние меняет **только** потребитель — разделение
    владения по данным, а не по коду.

    Инвариант (I19): пока документ ``pending``/``claimed``, ``raw_path`` и
    ``content_hash`` обязаны быть непустыми — очередь обещает существующий retained
    файл, и «нет пути» означало бы разрыв цепочки evidence до начала конверсии.
    ``level``/``pdf_pipeline`` — условия измерения корня, объявленные краулом: без них
    потребитель не может воспроизвести ``extractor_version`` этого документа.
    """

    model_config = _BASE

    session_id: str
    product_id: str
    url: str
    content_hash: str = ""
    raw_path: str = ""
    #: Уровень извлечения документных форматов, объявленный корнем (``document_level``).
    level: str = ""
    #: Механизм извлечения PDF, объявленный корнем (``pdf_pipeline``).
    pdf_pipeline: str = ""
    state: ExtractQueueState = ExtractQueueState.PENDING
    attempts: int = 0
    enqueued_at: str
    state_changed_at: str | None = None
    #: Кто взял документ (``claimed``) — провенанс потребителя, а не блокировка.
    claimed_by: str | None = None
    error_kind: str | None = None
    error: str | None = None

    @model_validator(mode="after")
    def _validate_pending_has_bytes(self) -> ExtractQueueItem:
        """``pending``/``claimed`` обязаны нести адрес retained-файла и его хеш.

        Проверяется ровно то, что обещает очередь: «документ добыт и лежит на диске».
        Терминальные состояния (``done``/``failed``/``skipped``) адрес не обязаны
        сохранять — их проверяет потребитель, а не производитель.
        """
        if self.state in (ExtractQueueState.PENDING, ExtractQueueState.CLAIMED):
            missing = [
                name
                for name in ("raw_path", "content_hash")
                if not str(getattr(self, name) or "").strip()
            ]
            if missing:
                raise ValueError(
                    f"state={self.state.value} требует непустых {', '.join(missing)}: "
                    "без адреса retained-байтов и его хеша конверсия не воспроизводима (I19)"
                )
        return self


class CorpusCoverage(BaseModel):
    """Покрытие корпуса продукта по исходам краула (Ф0, §2.3/§4.4).

    Первоклассный факт, производный от **персистентных** элементов сеанса краула и
    записей об усечении — не от wall-clock и не от порядка обхода. Нужен, чтобы
    ``❌`` нельзя было опубликовать как доказательство отсутствия, когда часть
    источников продукта нечитаема (robots/сеть/парсер/усечение).

    :param product_id: продукт-партиция.
    :param complete: ``False``, если краул усечён политикой, хотя бы один **origin**
        нечитаем (из него не получено ни одного ``FETCHED``-элемента и был хотя бы один
        недочёт, кроме ``not_found``), есть ``FAILED``-элемент с ``error_kind`` из
        :data:`core.domain.enums.BLOCKING_FAILURE_KINDS`, **либо не прочитано ни одного
        элемента** (``observed_sources == 0`` при ``expected_sources > 0``): корпус без
        единого наблюдения не может доказывать отсутствие, а ``not_found`` на всех URL —
        наблюдение отсутствия этих URL, не продукта (см.
        :func:`core.pipeline.corpus_coverage.derive_corpus_coverage`).
    :param expected_sources: всего элементов сеанса по продукту.
    :param observed_sources: элементов, дошедших до ``FETCHED``.
    :param blocked: ``error_kind → count`` по ``FAILED``-элементам (канонические классы;
        аудит и объяснение причины неполноты через :attr:`blocking_failures`).
    :param unreadable_origins: origin'ы (``netloc``), от которых не получено ничего
        (отсортировано, детерминированно).
    :param partial_blocks: ``origin → {error_kind: count}`` — не-``not_found`` сбои по
        каждому origin (аудит частичных отказов; ключи отсортированы).
    :param truncated: продукт попал в ``truncated_products`` сеанса.
    :param resolved_manually: сколько блокирующих отказов закрыто объявленным ручным
        артефактом (13-manual-artifacts §3.3): отказ остаётся в :attr:`blocked` как аудит,
        но материал наблюдён, поэтому он не делает покрытие неполным.
    :param computed_at: момент вычисления (ISO-8601); детерминизм обеспечивает вызывающий.
    """

    model_config = _BASE

    product_id: str
    complete: bool
    expected_sources: int = 0
    observed_sources: int = 0
    blocked: dict[str, int] = Field(default_factory=dict)
    unreadable_origins: list[str] = Field(default_factory=list)
    partial_blocks: dict[str, dict[str, int]] = Field(default_factory=dict)
    truncated: bool = False
    computed_at: str
    #: 13-manual-artifacts §3.3: сколько **блокирующих** отказов закрыто объявленным ручным
    #: артефактом (материал наблюдён, хоть и другим путём). Отказ при этом остаётся в
    #: :attr:`blocked`/``partial_blocks`` как аудит — заменяется не факт, а его последствие
    #: для полноты покрытия. ``0`` — ручных подач не было (поведение прежнее).
    resolved_manually: int = 0
    #: Машинная причина неполноты/отсутствия наблюдения (``no_seeds``,
    #: ``all_seeds_rejected``, ``no_urls_entry``, ``coverage_missing``). Наблюдательное
    #: поле: класс сбоя ячеек остаётся ``no_sources``, словарь §4.1 не расширяется.
    #: Аддитивное поле MVP-1: строка zero-source обязана назвать причину явно (O2/O3).
    reason: str | None = None

    @property
    def blocking_failures(self) -> dict[str, int]:
        """Блокирующие сбои корпуса: ``error_kind → count`` (отсортировано).

        Производное от :attr:`blocked` представление для отчёта/аудита: читатель видит
        ``robots_disallow=50``, а не только ``complete=False``. Схема хранения не
        меняется — это вычисляемое свойство, а не колонка. Пустой словарь означает, что
        неполнота (если она есть) вызвана усечением или нечитаемым origin'ом.
        """
        return {
            kind: int(count)
            for kind, count in sorted(self.blocked.items())
            if kind in _BLOCKING_FAILURE_VALUES
        }

    @field_validator("blocked")
    @classmethod
    def _validate_blocked(cls, value: dict[str, int]) -> dict[str, int]:
        """Ключи ``blocked`` — только канонические ``FailureKind``; порядок детерминирован."""
        known = _KNOWN_FAILURE_KINDS
        unknown = sorted(set(value) - known)
        if unknown:
            raise ValueError(
                f"corpus_coverage.blocked: неизвестные error_kind {unknown}; "
                f"допустимо: {', '.join(sorted(known))}"
            )
        if any(int(count) < 0 for count in value.values()):
            raise ValueError("corpus_coverage.blocked: счётчики не могут быть отрицательными")
        return {key: int(value[key]) for key in sorted(value)}

    @field_validator("unreadable_origins")
    @classmethod
    def _validate_unreadable_origins(cls, value: list[str]) -> list[str]:
        """Origin'ы — непустые строки; дедуп и сортировка (детерминизм)."""
        origins = {str(origin).strip().lower() for origin in value}
        if "" in origins:
            raise ValueError("corpus_coverage.unreadable_origins: пустой origin")
        return sorted(origins)

    @field_validator("partial_blocks")
    @classmethod
    def _validate_partial_blocks(
        cls, value: dict[str, dict[str, int]]
    ) -> dict[str, dict[str, int]]:
        """``partial_blocks`` — канонические ``FailureKind``, неотрицательные счётчики."""
        known = _KNOWN_FAILURE_KINDS
        canonical: dict[str, dict[str, int]] = {}
        for origin in sorted(value):
            counts = value[origin] or {}
            unknown = sorted(set(counts) - known)
            if unknown:
                raise ValueError(
                    f"corpus_coverage.partial_blocks[{origin!r}]: неизвестные error_kind "
                    f"{unknown}; допустимо: {', '.join(sorted(known))}"
                )
            if any(int(count) < 0 for count in counts.values()):
                raise ValueError(
                    "corpus_coverage.partial_blocks: счётчики не могут быть отрицательными"
                )
            canonical[str(origin).strip().lower()] = {
                key: int(counts[key]) for key in sorted(counts)
            }
        return canonical

    @model_validator(mode="after")
    def _check_consistency(self) -> CorpusCoverage:
        """``complete=True`` несовместимо с усечением и нечитаемыми origin'ами."""
        if self.expected_sources < 0 or self.observed_sources < 0:
            raise ValueError("corpus_coverage: счётчики не могут быть отрицательными")
        if self.resolved_manually < 0 or self.resolved_manually > self.expected_sources:
            raise ValueError(
                "corpus_coverage: resolved_manually — число закрытых вручную отказов "
                "(0..expected_sources)"
            )
        if self.observed_sources > self.expected_sources:
            raise ValueError(
                "corpus_coverage: observed_sources не может превышать expected_sources"
            )
        if self.complete and (self.truncated or self.unreadable_origins):
            raise ValueError(
                "corpus_coverage: complete=True требует отсутствия усечения и "
                "нечитаемых origin'ов"
            )
        if self.complete and self.expected_sources == 0:
            raise ValueError(
                "corpus_coverage: complete=True требует expected_sources > 0 "
                "(zero-source — failed, а не пустой ok; O2)"
            )
        return self



# ---------------------------------------------------------------------------
# Онбординг продукта (09 §3.2, MVP-1)
# ---------------------------------------------------------------------------


def _validate_seed_url(value: str, *, field: str) -> str:
    """Проверяет seed-URL: только http(s), есть хост, нет fragment/учётных данных.

    SSRF-политика (``core.util.security.validate_url``) применяется слоем ``config``;
    домен проверяет только форму URL, чтобы оставаться чистым (L0).
    """
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"seed.{field}: пустой URL")
    parsed = urlsplit(text)
    if parsed.scheme.lower() not in ("http", "https"):
        raise ValueError(f"seed.{field}: схема только http/https, получено {parsed.scheme!r}")
    if not parsed.hostname:
        raise ValueError(f"seed.{field}: URL без hostname")
    if parsed.username or parsed.password:
        raise ValueError(f"seed.{field}: учётные данные в URL запрещены")
    # ``url`` — авторский вход и может нести fragment; ``normalized_url`` — уже
    # канонический вид, fragment в нём означал бы, что нормализация не выполнена.
    if field == "normalized_url" and parsed.fragment:
        raise ValueError(f"seed.{field}: fragment запрещён (нормализуйте URL)")
    return text


class SeedEntry(BaseModel):
    """Авторский seed продукта (09 §7.1): URL + роль + провенанс + привязка к профилю.

    ``normalized_url`` — канонический вид по §7.2 (lowercase host, без default port и
    fragment, path case/trailing slash сохранены). Дедуп/``seed_id`` считаются по нему.
    """

    model_config = _BASE

    url: str
    normalized_url: str
    role: SeedRole
    source: SeedSource
    source_ref: str | None = None
    profile_key: str | None = None
    added_at: _dt.date
    added_by: str
    enabled: bool = True
    last_verified_at: _dt.date | None = None
    state: SeedState = SeedState.ENABLED

    @field_validator("url")
    @classmethod
    def _check_url(cls, value: str) -> str:
        return _validate_seed_url(value, field="url")

    @field_validator("normalized_url")
    @classmethod
    def _check_normalized_url(cls, value: str) -> str:
        return _validate_seed_url(value, field="normalized_url")

    @field_validator("added_by")
    @classmethod
    def _check_added_by(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("seed.added_by: автор записи обязателен")
        return value

    @field_validator("profile_key")
    @classmethod
    def _check_profile_key(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = value.strip()
        if not text:
            raise ValueError("seed.profile_key: пустой ключ недопустим (None = не привязан)")
        return text

    @model_validator(mode="after")
    def _check_state(self) -> SeedEntry:
        """``enabled`` и ``state`` не противоречат друг другу (disabled/invalid — не активны)."""
        if self.enabled != (self.state is SeedState.ENABLED):
            raise ValueError(
                "seed: enabled=True соответствует только state=enabled; "
                "disabled/invalid обязаны иметь enabled=false"
            )
        return self

    @property
    def is_enabled(self) -> bool:
        """Seed участвует в корпусе (``enabled`` и ``state=enabled``)."""
        return self.enabled and self.state is SeedState.ENABLED


class ProductSeeds(BaseModel):
    """Seed-набор одного продукта в ``sources/seeds.yaml`` (09 §7.1)."""

    model_config = _BASE

    state: OnboardingState
    seeds: list[SeedEntry] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_ready_has_seed(self) -> ProductSeeds:
        """``ready`` без активного seed'а — противоречие O1 (должно быть ``pending_seeds``)."""
        if self.state is OnboardingState.READY and not any(
            seed.is_enabled for seed in self.seeds
        ):
            raise ValueError(
                "product_seeds: state=ready требует хотя бы один enabled seed "
                "(иначе pending_seeds)"
            )
        return self

    def enabled_for(self) -> list[SeedEntry]:
        """Активные seed'ы продукта (порядок сохранён)."""
        return [seed for seed in self.seeds if seed.is_enabled]


class SeedsConfig(BaseModel):
    """Корневой документ ``sources/seeds.yaml`` (schema ``seeds/1``)."""

    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )

    schema_: str = Field(default="seeds/1", alias="schema")
    products: dict[str, ProductSeeds] = Field(default_factory=dict)

    @field_validator("schema_")
    @classmethod
    def _check_schema(cls, value: str) -> str:
        """Схема — ``seeds/<n>``; неизвестная версия не парсится молча."""
        if not str(value).startswith("seeds/"):
            raise ValueError(f"seeds.schema: ожидалось seeds/<n>, получено {value!r}")
        return value

    def enabled_for(self, product_id: str) -> list[SeedEntry]:
        """Активные seed'ы продукта (пусто, если продукта/записей нет)."""
        product = self.products.get(product_id)
        return product.enabled_for() if product is not None else []

    def all_for(self, product_id: str) -> list[SeedEntry]:
        """Все seed'ы продукта, включая disabled/invalid (порядок сохранён)."""
        product = self.products.get(product_id)
        return list(product.seeds) if product is not None else []


class ProductPlan(BaseModel):
    """План продукта для onboarding-префлайта и dry-run (09 §5.3).

    Поля ``last_*_run_id`` — рубежи durable-состояния продукта (09 §5.1): последний
    прогон, где собран корпус / прошла eval-фаза / разрешены все ячейки. ``None`` —
    рубеж не достигнут (или state-строки ещё нет), что и определяет режим продукта.
    """

    model_config = _BASE

    product_id: str
    mode: ProductMode = ProductMode.NEW
    onboarding_state: OnboardingState
    seed_count: int = Field(default=0, ge=0)
    refresh_policy: RefreshPolicy = RefreshPolicy.REVALIDATE
    scope: ProductScope = ProductScope.FULL
    reasons: list[str] = Field(default_factory=list)
    #: Последний прогон, где все ячейки разрешены (evaluated/reviewed/N/A).
    last_completed_run_id: str | None = None
    #: Последний прогон, где собран корпус (eval мог не состояться).
    last_corpus_run_id: str | None = None
    #: Последний прогон, где прошла eval-фаза (pending-ячейки допустимы).
    last_evaluated_run_id: str | None = None


class ProductPlanSet(BaseModel):
    """Набор планов продуктов + список заблокированных (09 §5.3)."""

    model_config = _BASE

    products: list[ProductPlan] = Field(default_factory=list)
    blocked: list[str] = Field(default_factory=list)
    #: Идентификатор цикла прогона (run identity — отдельный срез; ``None`` — не задан).
    cycle_id: str | None = None
    #: Предыдущий прогон пакета (lineage §12.3).
    previous_run_id: str | None = None
    #: Класс изменения (``methodology_change``/``execution_change``, §13).
    change_class: str | None = None

    def plan_for(self, product_id: str) -> ProductPlan | None:
        """План продукта или ``None``, если продукта нет в наборе."""
        return next(
            (plan for plan in self.products if plan.product_id == product_id), None
        )
