"""coHound — доменные перечисления (L0). Зеркало ``docs/status-dictionary.yaml`` (§4.1).

Источник истины — словарь; этот модуль — его типизированное зеркало. При расхождении
главнее словарь (проверяется ``assert_dictionary_integrity``). Нейтрален (§1.3).
"""

from __future__ import annotations

from enum import StrEnum


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


class SearchPhase(StrEnum):
    """Фазы конвейера, в которых пишется ``search_log`` (§4.4)."""

    DISCOVERY = "discovery"
    CORPUS = "corpus"
    SERP = "serp"
    SCOUT = "scout"


class TerminationReason(StrEnum):
    """Причина терминации поискового протокола — исчерпание (§4.4/§5)."""

    BUDGET_EXHAUSTED = "budget_exhausted"
    RESULTS_EXHAUSTED = "results_exhausted"
    SUFFICIENT_EVIDENCE = "sufficient_evidence"
    NOT_FOUND = "not_found"
    ERROR = "error"


class ComparabilityLevel(StrEnum):
    """Аннотация Comparability (§6): кросс-продуктовая сопоставимость."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INSUFFICIENT = "INSUFFICIENT"
    CROSS_CYCLE = "cross-cycle"


class GoldenTier(StrEnum):
    """Тир golden-набора (§11.6): быстрый PR-гейт vs полный прогон."""

    FAST = "fast"
    FULL = "full"
