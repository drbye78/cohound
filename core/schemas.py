"""coHound — Pydantic-схемы контрактов (ядро, категорийно-нейтральное).

Соответствует §4.2 (package/catalog/baselines/golden/cohort), §4.3 (паспорт продукта),
§4.5 (run.yaml) и §7 (выход LLM-оценщика) регламента cohound-spec-v2.md.

Ядро не содержит терминологии предметных категорий (§1.3): все поля типизированы,
но не привязаны к конкретной предметной области.
"""

from __future__ import annotations

import datetime as _dt
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

# ---------------------------------------------------------------------------
# Перечисления (источник истины — docs/status-dictionary.yaml; здесь — зеркало)
# ---------------------------------------------------------------------------


class Status(StrEnum):
    OK = "✅"
    PARTIAL = "⚠️"
    ABSENT = "❌"
    UNKNOWN = "❓"
    NOT_APPLICABLE = "N/A"


class SourceKind(StrEnum):
    REGISTRY = "registry"
    INHERITANCE = "inheritance"
    DISTRIBUTION = "distribution"
    DOCUMENTATION = "documentation"
    WEB = "web"
    MANUAL = "manual"


class EvidenceType(StrEnum):
    DIRECT = "direct"
    DERIVED = "derived"


class AbsenceRuleType(StrEnum):
    EXPLICIT = "explicit"
    PARAMETER_REFERENCE = "parameter_reference"
    DISTRIBUTION_ABSENCE = "distribution_absence"
    REGISTRY_ABSENCE = "registry_absence"


class SourceScope(StrEnum):
    EXHAUSTIVE = "exhaustive"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


class FailureKind(StrEnum):
    SEARCH_FAILED = "search_failed"
    SOURCE_UNAVAILABLE = "source_unavailable"
    EVALUATION_FAILED = "evaluation_failed"
    VALIDATION_FAILED = "validation_failed"
    PARSER_FAILED = "parser_failed"


class CellState(StrEnum):
    PENDING = "pending"
    PRELIMINARY = "preliminary"
    EVALUATED = "evaluated"
    FAILED = "failed"
    REVIEWED = "reviewed"


class RunMode(StrEnum):
    STRICT = "strict"
    PRELIMINARY = "preliminary"


class Origin(StrEnum):
    IN_HOUSE = "in_house"
    THIRD_PARTY = "third_party"
    UPSTREAM = "upstream"
    COMMUNITY = "community"
    UNKNOWN = "unknown"


class Relationship(StrEnum):
    BUNDLED = "bundled"
    INTEGRATED = "integrated"
    CERTIFIED = "certified"
    RECOMMENDED = "recommended"
    EXTERNAL = "external"


class EvidenceState(StrEnum):
    VERIFIED = "verified"
    CLAIMED = "claimed"


class SatelliteState(StrEnum):
    DRAFT = "draft"
    APPROVED = "approved"
    EDITED_BY_HUMAN = "edited_by_human"
    NOT_FOUND = "not_found"


class InheritanceMode(StrEnum):
    DIRECT = "direct"
    CONDITIONAL = "conditional"
    BLOCKED = "blocked"


class ParamType(StrEnum):
    FACT = "fact"
    SHOWCASE = "showcase"


class Importance(StrEnum):
    CRITICAL = "Кр"
    IMPORTANT = "Вж"
    BASIC = "Баз"
    OPTIONAL = "Опц"


class Archetype(StrEnum):
    A = "A"
    B = "B"
    C = "C"


class AnchorRelation(StrEnum):
    ANCHOR = "anchor"
    ANALOG = "analog"


class Gate(StrEnum):
    G1 = "G1"
    G2 = "G2"
    G3 = "G3"
    G4 = "G4"


class RankingPolicy(StrEnum):
    CAPABILITY_RANKED = "capability_ranked"
    COVERAGE_FALLBACK = "coverage_fallback"
    UNRANKED = "unranked"


class CapabilityPolicy(StrEnum):
    RESOLVED_ONLY_V1 = "resolved-only-v1"


class UnknownPolicy(StrEnum):
    EXCLUDED = "excluded"
    UNKNOWN_AS_ZERO = "unknown_as_zero"


class SearchKind(StrEnum):
    RETRIEVAL = "retrieval"
    EXTERNAL = "external"


class StaleReason(StrEnum):
    NEW_BUILD = "new_build"
    PROMPT_CHANGED = "prompt_changed"
    CATALOG_CHANGED = "catalog_changed"
    MODEL_CHANGED = "model_changed"


class LLMScenario(StrEnum):
    EVALUATOR = "evaluator"
    SEARCH_ADAPTER = "search_adapter"
    DISCOVERY = "discovery"
    CONCEPT_SCOUT = "concept_scout"


class EndpointKind(StrEnum):
    SELF_HOST = "self_host"
    API = "api"


class AdapterKind(StrEnum):
    SERP = "serp"
    LLM_GROUNDING = "llm_grounding"


class Cap(StrEnum):
    """Потолок зачёта Р-2 (§4.1)."""

    NONE = "none"
    PARTIAL = "partial"
    OK = "ok"
    PARAMETER_RULE = "parameter_rule"


_BASE = ConfigDict(extra="forbid", str_strip_whitespace=True)


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
    """run.yaml (§4.5)."""

    model_config = _BASE

    case_id: str
    anchor: str
    cohort: str
    products: list[str]
    package: str
    focus: Focus = Field(default_factory=Focus)
    languages: list[str] = Field(default_factory=list)
    snapshot_date: _dt.date
    mode: RunMode
    ranking_policy: RankingPolicy = RankingPolicy.CAPABILITY_RANKED

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


def apply_r2(
    origin: Origin,
    relationship: Relationship,
    evidence_state: EvidenceState,
    parameter_rule_cap: Cap = Cap.OK,
) -> Cap:
    """Маппинг сателлита на зачёт (Р-2, §4.1). Порядок важен, первое совпадение решает."""
    if evidence_state is EvidenceState.CLAIMED:
        return Cap.PARTIAL
    if origin is Origin.COMMUNITY and relationship is Relationship.EXTERNAL:
        return Cap.NONE
    if relationship in {
        Relationship.BUNDLED,
        Relationship.INTEGRATED,
        Relationship.CERTIFIED,
    } and evidence_state is EvidenceState.VERIFIED:
        return Cap.OK
    if relationship is Relationship.RECOMMENDED and evidence_state is EvidenceState.VERIFIED:
        return Cap.PARTIAL
    if relationship is Relationship.EXTERNAL and evidence_state is EvidenceState.VERIFIED:
        return parameter_rule_cap
    return parameter_rule_cap


# ---------------------------------------------------------------------------
# Словарь
# ---------------------------------------------------------------------------


def load_dictionary(path: str | Path) -> dict[str, Any]:
    """Загружает docs/status-dictionary.yaml."""
    text = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("status-dictionary.yaml: ожидался mapping верхнего уровня")
    return data


def assert_dictionary_integrity(path: str | Path) -> None:
    """Минимальная проверка целостности словаря относительно схем."""
    data = load_dictionary(path)
    required = [
        "status_dictionary_version",
        "statuses",
        "source_kind",
        "evidence_type",
        "absence_rule_type",
        "source_scope",
        "failure_kind",
        "cell_state",
        "run_mode",
        "satellite",
        "rule_r2",
        # S3: новые перечисления должны быть в словаре — источнике истины.
        "ranking_policy",
        "capability_policy",
        "unknown_policy",
        "gate",
        "search_kind",
        "stale_reason",
    ]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"словарь неполон, нет секций: {missing}")
    for status, meta in data["statuses"].items():
        if meta["symbol"] not in {s.value for s in Status}:
            raise ValueError(f"статус {status}: символ {meta['symbol']} вне Status")
    if data["absence_rule_type"]["default"] not in data["absence_rule_type"]["values"]:
        raise ValueError("absence_rule_type.default отсутствует в values")
    # Синхронизация новых enum со словарём (S3).
    checks = {
        "ranking_policy": {v.value for v in RankingPolicy},
        "capability_policy": {v.value for v in CapabilityPolicy},
        "unknown_policy": {v.value for v in UnknownPolicy},
        "gate": {v.value for v in Gate},
        "search_kind": {v.value for v in SearchKind},
        "stale_reason": {v.value for v in StaleReason},
    }
    for section, expected in checks.items():
        actual = set(data[section])
        if actual != expected:
            raise ValueError(
                f"словарь.{section} рассинхронизирован со схемой: "
                f"словарь={sorted(actual)}, схема={sorted(expected)}"
            )
