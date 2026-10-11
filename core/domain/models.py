"""coHound — доменные модели (L0): Pydantic-схемы §4.2/4.3/4.5/7.

Чистые модели без I/O; инварианты обеспечиваются валидаторами (извлечение чистых функций —
T2.2 в ``domain/invariants.py``). Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.domain.enums import (
    AbsenceRuleType,
    AdapterKind,
    AnchorRelation,
    Archetype,
    CellState,
    EndpointKind,
    EvidenceState,
    EvidenceType,
    FetchStatus,
    Gate,
    GoldenTier,
    Importance,
    InheritanceMode,
    LLMScenario,
    Origin,
    ParamType,
    RankingPolicy,
    Relationship,
    RunMode,
    SatelliteState,
    SearchKind,
    SearchPhase,
    SourceKind,
    SourceScope,
    StaleReason,
    Status,
    TerminationReason,
)

_BASE = ConfigDict(extra="forbid", str_strip_whitespace=True)

#: Версия методологии (§13): версионная граница **семантики** правил (словарь статусов,
#: scoring-политика, absence/evidence-модель, определения Coverage/Comparability). В отличие
#: от ``execution_versions`` (commodity-слой: модель/когорта/индекс), смена методологии
#: требует пере-аннотации golden. Пин-значение проекта (регламент v2.1); попадает в
#: ``RunConfig.methodology_version`` → ``RunManifest.methodology_version``.
METHODOLOGY_VERSION = "2.1.0"


# ---------------------------------------------------------------------------
# §4.2 Пакет категории
# ---------------------------------------------------------------------------


class RoleDef(BaseModel):
    """Роль — инженерная функция с реестром в пакете (нейтрально к категории)."""

    model_config = _BASE

    role_id: str
    definition: str
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


class CohortEntry(BaseModel):
    """cohort.yaml (§1.6)."""

    model_config = _BASE

    product_id: str
    role_overlap: float = Field(ge=0, le=1)
    selection_reason: str
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
    #: "experiment_id"?: str}}``. Строки настроек (не enum ядра) — словарь не трогаем.
    settings_provenance: dict[str, dict[str, Any]] = Field(default_factory=dict)

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
        return self


# ---------------------------------------------------------------------------
# §7 Выход LLM-оценщика
# ---------------------------------------------------------------------------


class LLMEvidence(BaseModel):
    model_config = _BASE

    source_id: str
    quote: str


class LLMCellOutput(BaseModel):
    """Схема ячейки, навязываемая оценщику constrained decoding'ом (§7)."""

    model_config = _BASE

    status: Status
    justification: str
    evidence: list[LLMEvidence] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> LLMCellOutput:
        if self.status is Status.NOT_APPLICABLE:
            raise ValueError("N/A запрещён на выходе LLM (I7)")
        if self.status in {Status.OK, Status.PARTIAL} and not self.evidence:
            raise ValueError("✅/⚠️ требуют evidence ≥ 1 (I1)")
        return self


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
    scope_attested_by: str | None = None
    index_build_id: str | None = None
    retrieved_at: str | None = None

    @model_validator(mode="after")
    def _check(self) -> CellEvidence:
        if not self.source_id:
            raise ValueError("I10/I15: evidence обязан иметь source_id")
        # Корпусный evidence обязан нести hash (I19).
        if self.source_kind in {SourceKind.DOCUMENTATION, SourceKind.WEB, SourceKind.DISTRIBUTION}:
            if not self.content_hash:
                raise ValueError("I19: корпусный evidence обязан иметь content_hash")
        # absence-evidence: scope по типу правила проверяется на уровне Cell.
        if self.source_kind is SourceKind.REGISTRY and not self.source_url:
            raise ValueError("R1: registry evidence обязан иметь source_url")
        return self


def derived_evidence_id(
    run_id: str, product_id: str, param_id: str, source_id: str, source_kind: str, seq: int
) -> str:
    """Детерминированный evidence_id (S8)."""
    import hashlib

    raw = f"{run_id}|{product_id}|{param_id}|{source_id}|{source_kind}|{seq}".encode()
    return "ev_" + hashlib.sha256(raw).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Инварианты ячейки (§2.3) и Р-2 (§4.1)
# ---------------------------------------------------------------------------


class Cell(BaseModel):
    """Ячейка для проверки инвариантов I1–I13."""

    model_config = _BASE

    run_id: str
    product_id: str
    param_id: str
    state: CellState
    status: Status | None = None
    source_kind: SourceKind | None = None
    absence_rule: AbsenceRuleType = AbsenceRuleType.EXPLICIT
    evidence: list[LLMEvidence] = Field(default_factory=list)
    absence_evidence: list[dict[str, Any]] = Field(default_factory=list)
    reviewed_by: str | None = None
    # Провенанс ячейки (R7/§13): смена сборки/профиля/промпта детектируется сравнением.
    index_build_id: str | None = None
    retrieval_profile: str | None = None
    retrieval_params_json: str | None = None
    stale_reason: StaleReason | None = None

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
        if self.state is CellState.EVALUATED:
            if self.status in {Status.OK, Status.PARTIAL} and not self.evidence:
                raise ValueError("I1: evaluated ✅/⚠️ → evidence ≥ 1")
            # I2/I2b: evaluated ❌ → absence-evidence с учётом типа правила
            if self.status is Status.ABSENT:
                if not self.absence_evidence:
                    raise ValueError("I2: evaluated ❌ → absence-evidence обязателен")
                if self.absence_rule in {
                    AbsenceRuleType.PARAMETER_REFERENCE,
                    AbsenceRuleType.DISTRIBUTION_ABSENCE,
                }:
                    ok = any(
                        e.get("source_scope") == SourceScope.EXHAUSTIVE.value
                        and e.get("scope_attested_by")
                        for e in self.absence_evidence
                    )
                    if not ok:
                        raise ValueError(
                            "I2b: parameter_reference/distribution_absence требуют "
                            "source_scope=exhaustive и scope_attested_by"
                        )
                if self.absence_rule is AbsenceRuleType.REGISTRY_ABSENCE:
                    if not any(
                        e.get("source_scope") == SourceScope.EXHAUSTIVE.value
                        for e in self.absence_evidence
                    ):
                        raise ValueError(
                            "I2b: registry_absence — exhaustive по построению"
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
    """Человеческое решение гейта G1–G4 (S9). Строка ``gate_decisions``.

    Носитель ``reviewed_by`` и аудит-след. Гейт не считается пройденным без строки.
    """

    model_config = _BASE

    decision_id: str
    run_id: str
    gate: Gate
    target: str | None = None
    actor: str
    decided_at: str | None = None
    notes: str | None = None

    @model_validator(mode="after")
    def _check(self) -> GateDecision:
        if not self.actor:
            raise ValueError("решение гейта обязано иметь actor (человека)")
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
    focus_modules: list[str] = Field(default_factory=list)
    # Индексный/ретривал-контур (§13, I16/I20/S1)
    index_build_id: str | None = None
    embed_model_id: str | None = None
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
    #: "experiment_id"?: str}}``; фиксирует, что тюнится измерением, а не предвидением.
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


class CrawlAttempt(BaseModel):
    """Строка ``crawl_attempts`` (D3, §4.4): durable per-URL запись краула.

    Носитель аудита, очереди и resume Ш5: статус ``FetchStatus`` плюс провенанс попытки
    (``fetch_mode``/``extract_route``/``http_status``/``content_type``/``lang``) и retention
    (``content_hash``/``raw_path``). ``error_kind`` переиспользует ``FailureKind`` (без нового
    перечисления): сбой краула → ``source_unavailable``, сбой извлечения → ``parser_failed``.
    ``status`` — часть операционной таблицы, а не доменной ячейки (см. ``FetchStatus``).
    """

    model_config = _BASE

    run_id: str
    product_id: str
    url: str
    status: FetchStatus
    attempts: int = 0
    fetch_mode: str | None = None
    extract_route: str | None = None
    http_status: int | None = None
    content_type: str | None = None
    #: Язык страницы (D3): персистится, чтобы resume не терял ``RawPage.lang`` (иначе
    #: ``corpus_docs.lang``/``corpus_chunks.lang`` расходились бы между свежим и возобновлённым прогоном).
    lang: str | None = None
    content_hash: str | None = None
    raw_path: str | None = None
    error_kind: str | None = None
    error: str | None = None
    first_seen: str | None = None
    last_attempt: str | None = None


