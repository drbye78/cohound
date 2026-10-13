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


class FetchMode(StrEnum):
    """Ось получения страницы (D2): обычный HTTP или браузерный рендер (JS)."""

    HTTP = "http"
    BROWSER = "browser"


class ExtractionRoute(StrEnum):
    """Ось извлечения (D2): авто-детект по формату, принудительный HTML или документ."""

    AUTO = "auto"
    HTML = "html"
    DOCUMENT = "document"


class CrawlStrategy(StrEnum):
    """Ось стратегии краула: **как** используется браузер **при browser-capable адаптере**.

    Отличается от ``FetchMode`` («нужен ли браузер вообще»): ``auto`` — адаптер выбирает
    безопасный дефолт (для браузерного адаптера = ``honest``); ``honest`` — все запросы
    через браузер; ``hybrid`` — браузер только для антибот-cookie, остальное — лёгким HTTP
    с переиспользованием cookie (fallback на ``honest``).

    Инвариант (нормализуется в :func:`core.pipeline.fetch_plan.resolve_fetch_plan`):
    ``strategy != AUTO`` ⇒ ``fetch == BROWSER``. При ``fetch=HTTP`` стратегия браузера
    бессмысленна и приводится к ``AUTO``.
    """

    AUTO = "auto"
    HONEST = "honest"
    HYBRID = "hybrid"


class CrawlSessionStatus(StrEnum):
    """Жизненный цикл сеанса краула (``crawl_sessions``) — durable намерение + прогресс.

    ``CREATED`` — сеанс объявлен, работа не начата; ``RUNNING`` — исполняется; ``PAUSED`` —
    приостановлен (возобновим); ``CANCELLED`` — снят оператором/системой; ``COMPLETED`` — все
    элементы успешно добыты; ``PARTIAL`` — часть элементов упала, но тело краула добыто;
    ``FAILED`` — терминальный сбой сеанса. Терминальные: ``completed|partial|cancelled|failed``.

    ``partial`` закрывает гэп «упавшие URL прячутся за ``ok``»: успешно добытый, но неполный
    краул не выдаётся за полный. Статус относится к операционной сущности сеанса, а не к
    доменной ячейке (не путать с ``CellState``). Per-URL статус несёт :class:`CrawlItemStatus`.
    """

    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class CrawlItemStatus(StrEnum):
    """Per-URL статус элемента сеанса краула (``crawl_session_items``).

    ``PENDING`` — в очереди, ещё не фетчился; ``RUNNING`` — в обработке (при рестарте
    восстанавливается в ``PENDING`` — recovery-on-open); ``FETCHED`` — сырые байты сохранены
    (retention); ``FAILED`` — терминально после попыток (``error_kind``/``FailureKind``);
    ``CANCELLED`` — снят с работы. Терминальные: ``fetched|failed|cancelled``.

    Жизненный цикл элемента сеанса, а не доменной ячейки; заменяет прежний per-URL статус
    попытки краула (back-compat не требуется), с явным ``running`` и без промежуточного ``done``
    (материализация — отдельная фаза).
    """

    PENDING = "pending"
    RUNNING = "running"
    FETCHED = "fetched"
    FAILED = "failed"
    CANCELLED = "cancelled"
