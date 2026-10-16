"""coHound — веб-заземлённая разведка пакета категории (``hound package scout``).

Сценарий разведки **пакета категории**: pipeline сам выполняет SERP
(:class:`~core.ports.search.SearchAdapterPort`), реальный краул
(:class:`~core.ports.crawler.CrawlerPort`) и извлечение
(:class:`~core.ports.extractor.ExtractorPort`), после чего **tool-less** LLM
(:class:`~core.ports.discovery_llm.DiscoveryLLMPort`) синтезирует гипотезы ландшафта,
каталога и (опционально) базовой линии. Консервативный маппинг превращает гипотезы в
:class:`~core.pipeline.package_candidates.Candidate` + sidecar-провенанс.

Ключевые инварианты
-------------------
* **I18 — структурная граница.** LLM не имеет веб-тула: ``DiscoveryLLMPort.complete``
  физически не принимает ``tools``/``web_search``. Поиск и краул живут **только** в этом
  pipeline-модуле, а не в контуре оценщика/LLM.
* **I19 — кандидаты, не evidence.** Модуль не обращается к хранилищу прогона и не
  строит evidence-структуры; внешний контент (URL/сниппеты/текст страниц) остаётся
  кандидатом до ``corpus_raw → hash → index``. Выход — только ``Candidate``.
* **A3 / G2 — нет auto-approve.** Все кандидаты помечены ``needs_human=True`` и
  ``source="scout"``; превратить кандидата в факт пакета можно только человеческим
  ``hound package apply --actor`` (см. :mod:`core.pipeline.package_candidates`).
* **§10 — делимитеры и санитайзинг.** Текст страниц подаётся LLM в формате
  ``[source: url]`` с явным предупреждением о prompt-injection; любая цитата обязана
  быть дословной подстрокой переданного текста, а URL — из whitelist собранных
  источников.
* **§1.3 — категорийная нейтральность.** Никаких названий продуктов/вендоров/
  предметных терминов.
* **I23 — ``search_log``.** Счётчик ``search_calls`` фиксируется в sidecar; запись в
  ``search_log`` выполняется вызывающим при наличии ``store``/``run_id`` (в CLI-контуре
  scout эти ручки не задаются — см. отчёт).

Ожидаемые ключи ``hints`` (все опциональны; неизвестные игнорируются)::

    queries              # дополнительные поисковые запросы (list[str])
    terms                # термины темы (list[str] | dict[lang, list[str]])
    target_count         # целевое число параметров каталога (int)
    existing_modules     # module_id целевого пакета (list[str]; резервный ключ CLI)
    existing_roles       # role_id целевого пакета (list[str]; резервный ключ CLI)
    existing_params      # param_id целевого пакета (list[str]; резервный ключ CLI)
    existing_titles      # title целевого пакета (list[str]; резервный ключ CLI)
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field

from core.domain.enums import (
    AbsenceRuleType,
    Archetype,
    InheritanceMode,
    Importance,
    ParamType,
)
from core.pipeline.package_admin import _slug_from_title, _unique_param_id
from core.pipeline.package_candidates import Candidate, dumps_candidates
from core.pipeline.package_discovery import (
    ArchetypeHypothesis,
    infer_archetype,
    suggest_cohort_members,
    suggest_modules,
    suggest_roles,
)
from core.pipeline.package_io import atomic_write_text
from core.pipeline.prompt import _fill_template, _load_template
from core.pipeline.scout_common import (
    as_date as _as_date,
    clean as _clean,
    dedupe as _dedupe,
    fetch_one as _fetch_one,
    join_pages as _join_pages,
    url_allowed as _url_allowed,
    usage_dict as _usage_dict,
)
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMRequest, DiscoveryLLMPort
from core.ports.extractor import ExtractorPort
from core.ports.search import SearchAdapterPort, SearchQuery
from core.util.hashing import content_hash
from core.util.security import sanitize_slug

__all__ = [
    "LANDSCAPE_TEMPLATE_VERSION",
    "CATALOG_TEMPLATE_VERSION",
    "BASELINE_TEMPLATE_VERSION",
    "ScoutUpstream",
    "ScoutProduct",
    "ScoutModule",
    "ScoutRole",
    "ScoutArchetypeSignal",
    "ScoutLandscapeHypothesis",
    "ScoutParam",
    "ScoutCatalogHypothesis",
    "PackageScoutReport",
    "scout_package",
    "build_seed_from_hypotheses",
    "write_scout_artifacts",
]

#: Версии шаблонов промптов разведки (файл ``templates/{VERSION}.txt``). Смена шаблона
#: меняет ``prompt_hash`` (детерминизм, R7).
LANDSCAPE_TEMPLATE_VERSION = "package_scout_v1_landscape"
CATALOG_TEMPLATE_VERSION = "package_scout_v1_catalog"
BASELINE_TEMPLATE_VERSION = "package_scout_v1_baseline"

#: Допустимые значения ``--only`` (какие LLM-вызовы делать).
_ONLY_CHOICES: tuple[str, ...] = ("landscape", "catalog", "baselines")
#: Дефолтный набор вызовов: ландшафт + каталог (2 вызова; OQ-7).
_ONLY_DEFAULT: frozenset[str] = frozenset({"landscape", "catalog"})

#: Дефолтный вес модуля-кандидата (assumption; Σ→100 сделает ``add_module``, A3).
_DEFAULT_MODULE_WEIGHT = 10.0

#: Токены публичной величины/метрики (зеркало линтерного ``_SHOWCASE_TOKENS``).
_SHOWCASE_TOKENS: tuple[str, ...] = ("величин", "metric", "велич", "%", "числ", "значени")

#: Словарь форм важности (короткие/полные RU-формы и EN) → значение :class:`Importance`.
_IMPORTANCE_ALIASES: dict[str, str] = {
    "кр": Importance.CRITICAL.value,
    "критич": Importance.CRITICAL.value,
    "критический": Importance.CRITICAL.value,
    "critical": Importance.CRITICAL.value,
    "crit": Importance.CRITICAL.value,
    "вж": Importance.IMPORTANT.value,
    "важн": Importance.IMPORTANT.value,
    "важный": Importance.IMPORTANT.value,
    "важно": Importance.IMPORTANT.value,
    "important": Importance.IMPORTANT.value,
    "баз": Importance.BASIC.value,
    "базов": Importance.BASIC.value,
    "базовый": Importance.BASIC.value,
    "basic": Importance.BASIC.value,
    "base": Importance.BASIC.value,
    "опц": Importance.OPTIONAL.value,
    "опциональн": Importance.OPTIONAL.value,
    "опциональный": Importance.OPTIONAL.value,
    "optional": Importance.OPTIONAL.value,
    "opt": Importance.OPTIONAL.value,
}

#: Допустимые значения ``absence_rule``, требующие человеческого решения в scout.
_RISKY_ABSENCE: frozenset[str] = frozenset(
    {AbsenceRuleType.PARAMETER_REFERENCE.value, AbsenceRuleType.DISTRIBUTION_ABSENCE.value}
)


# ---------------------------------------------------------------------------
# Схемы гипотез (permissive: всё опционально + confidence/rationale, extra="ignore")
# ---------------------------------------------------------------------------


class ScoutUpstream(BaseModel):
    """Гипотеза upstream/коммерческого производного (имя + цитата-кандидат)."""

    model_config = ConfigDict(extra="ignore")

    name: str | None = None
    is_oss: bool | None = None
    url: str | None = None
    quote: str | None = None
    confidence: float = 0.0
    rationale: str | None = None


class ScoutProduct(BaseModel):
    """Гипотеза продукта/OSS-альтернативы (участник когорты или сравнения)."""

    model_config = ConfigDict(extra="ignore")

    name: str | None = None
    role_hint: str | None = None
    is_anchor_candidate: bool | None = None
    url: str | None = None
    quote: str | None = None
    confidence: float = 0.0


class ScoutModule(BaseModel):
    """Гипотеза модуля категории (вес — assumption, уточняется человеком)."""

    model_config = ConfigDict(extra="ignore")

    name: str | None = None
    weight: float | None = None
    rationale: str | None = None
    confidence: float = 0.0


class ScoutRole(BaseModel):
    """Гипотеза инженерной роли (маркеры + шаблоны запросов ru/en)."""

    model_config = ConfigDict(extra="ignore")

    name: str | None = None
    definition: str | None = None
    markers: list[str] = Field(default_factory=list)
    query_patterns: dict[str, list[str]] = Field(default_factory=dict)
    confidence: float = 0.0


class ScoutArchetypeSignal(BaseModel):
    """Дополнительный сигнал архетипа от LLM (§1.5) — не заменяет эвристику."""

    model_config = ConfigDict(extra="ignore")

    value: str | None = None
    signals: list[str] = Field(default_factory=list)
    confidence: float = 0.0


class ScoutLandscapeHypothesis(BaseModel):
    """Гипотеза ландшафта категории (выход landscape-вызова LLM)."""

    model_config = ConfigDict(extra="ignore")

    subject_name: str | None = None
    category: str | None = None
    vendor: str | None = None
    upstream: ScoutUpstream | None = None
    alternatives: list[ScoutProduct] = Field(default_factory=list)
    commercial_derivatives: list[ScoutUpstream] = Field(default_factory=list)
    cross_line: bool = False
    products: list[ScoutProduct] = Field(default_factory=list)
    modules: list[ScoutModule] = Field(default_factory=list)
    roles: list[ScoutRole] = Field(default_factory=list)
    archetype_signal: ScoutArchetypeSignal | None = None
    field_confidence: dict[str, float] = Field(default_factory=dict)
    needs_human: list[str] = Field(default_factory=list)


class ScoutParam(BaseModel):
    """Гипотеза параметра каталога (строка ``catalog.csv``)."""

    model_config = ConfigDict(extra="ignore")

    title: str | None = None
    module: str | None = None
    group: str | None = None
    importance: str | None = None
    param_type: str | None = None
    rule: str | None = None
    absence_rule: str | None = None
    role_binding: str | None = None
    concept_terms: Any = None
    reference_upstream: str | None = None
    min_version: str | None = None
    inheritance_mode: str | None = None
    url: str | None = None
    quote: str | None = None
    confidence: float = 0.0
    rationale: str | None = None


class ScoutCatalogHypothesis(BaseModel):
    """Гипотеза каталога параметров (выход catalog/baseline-вызова LLM)."""

    model_config = ConfigDict(extra="ignore")

    subject_name: str | None = None
    params: list[ScoutParam] = Field(default_factory=list)
    field_confidence: dict[str, float] = Field(default_factory=dict)
    needs_human: list[str] = Field(default_factory=list)


@dataclass
class PackageScoutReport:
    """Итог веб-заземлённой разведки пакета категории.

    :param subject_name: тема разведки.
    :param archetype: эвристическая гипотеза архетипа (§1.5), дополненная LLM-сигналом.
    :param profile: собранный (веб-заземлённый) словарь профиля для ``suggest_*``.
    :param candidates: кандидаты (все ``needs_human=True``, ``source="scout"``).
    :param needs_human: элементы, требующие человеческого решения.
    :param sidecar: sidecar-провенанс (шаблоны, ``prompt_hash``, ``usage``, ``fetched`` …).
    :param fetched: список фетчей (URL, ``content_hash``, версия экстрактора, ошибка).
    :param search_calls: число фактических вызовов ``SearchAdapterPort.search``.
    :param ok: ``False``, если были ошибки сбора/инференса.
    :param errors: человекочитаемые ошибки (без трассировок).
    """

    subject_name: str
    archetype: ArchetypeHypothesis
    profile: dict[str, Any]
    candidates: list[Candidate]
    needs_human: list[str] = field(default_factory=list)
    sidecar: dict[str, Any] = field(default_factory=dict)
    fetched: list[dict[str, Any]] = field(default_factory=list)
    search_calls: int = 0
    ok: bool = True
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Внутренние помощники
# ---------------------------------------------------------------------------


def _clamp(value: Any, default: float = 0.0) -> float:
    """Приводит ``confidence`` к ``float`` и зажимает в ``[0, 1]``."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return min(1.0, max(0.0, number))


def _render_hints(hints: dict[str, Any] | None) -> str:
    """Детерминированный рендер подсказок для слота ``HINTS`` (без резервных ключей)."""
    if not hints:
        return "(не заданы)"
    reserved = {
        "existing_modules",
        "existing_roles",
        "existing_params",
        "existing_titles",
        "target_count",
    }
    lines: list[str] = []
    for key in sorted(hints):
        if key in reserved:
            continue
        value = hints[key]
        if value in (None, "", [], {}, 0):
            continue
        if isinstance(value, (list, tuple)):
            rendered = ", ".join(str(item) for item in value)
        elif isinstance(value, dict):
            rendered = "; ".join(f"{k}: {v}" for k, v in sorted(value.items()))
        else:
            rendered = str(value)
        lines.append(f"- {key}: {rendered}")
    return "\n".join(lines) if lines else "(не заданы)"


def _render_urls(urls: list[str]) -> str:
    """Детерминированный рендер списка URL для слота ``SEED_URLS``."""
    if not urls:
        return "(не заданы)"
    return "\n".join(f"- {url}" for url in urls)


def _normalize_only(only: Any) -> set[str]:
    """Нормализует ``only`` в множество допустимых значений (``None`` → дефолт)."""
    if only is None:
        return set(_ONLY_DEFAULT)
    if isinstance(only, str):
        parts = [part.strip() for part in only.split(",")]
    else:
        parts = [str(part).strip() for part in only]
    selected = {part for part in parts if part}
    unknown = selected - set(_ONLY_CHOICES)
    if unknown:
        raise ValueError(
            f"only: неизвестные значения {sorted(unknown)}; допустимы {list(_ONLY_CHOICES)}"
        )
    return selected or set(_ONLY_DEFAULT)


def _plan_queries(
    subject_name: str,
    category: str | None,
    hints: dict[str, Any] | None,
    lang: str,
) -> list[str]:
    """Детерминированный план запросов SERP (без решений LLM)."""
    queries: list[str] = [subject_name]
    if category:
        queries.append(f"{subject_name} {category}")
    raw = (hints or {}).get("queries")
    if isinstance(raw, (list, tuple)):
        queries.extend(str(item) for item in raw if str(item).strip())
    elif isinstance(raw, str) and raw.strip():
        queries.append(raw.strip())
    terms = (hints or {}).get("terms")
    if isinstance(terms, dict):
        for term in terms.get(lang, []) or []:
            if str(term).strip():
                queries.append(f"{subject_name} {term}".strip())
    elif isinstance(terms, (list, tuple)):
        for term in terms:
            if str(term).strip():
                queries.append(f"{subject_name} {term}".strip())
    return _dedupe([query.strip() for query in queries if query and query.strip()])


def _call_llm(
    discovery_llm: DiscoveryLLMPort,
    *,
    prompt: str,
    schema: Any,
) -> tuple[Any, dict[str, Any], str | None]:
    """Один вызов LLM-разведки в схему; сбой изолируется (возврат пустой гипотезы).

    :return: ``(hypothesis, meta, error)``; ``meta`` содержит ``prompt_hash``/``decoder``/
        ``model_id``/``usage`` для sidecar-провенанса.
    """
    model_id = getattr(discovery_llm, "model_id", "") or ""
    decoder = getattr(discovery_llm, "decoder", "none") or "none"
    digest = content_hash(prompt)
    usage: Any = None
    reasoning = 0
    try:
        response = discovery_llm.complete(
            DiscoveryLLMRequest(prompt=prompt, response_schema=schema, model_id=model_id)
        )
        output = response.output
        if isinstance(output, schema):
            hypothesis = output
        elif isinstance(output, BaseModel):
            hypothesis = schema.model_validate(output.model_dump())
        else:
            hypothesis = schema()
        digest = getattr(response, "prompt_hash", "") or digest
        decoder = getattr(response, "decoder", "") or decoder
        usage = getattr(response, "usage", None)
        reasoning = int(getattr(response, "reasoning_tokens", 0) or 0)
    except Exception as exc:  # noqa: BLE001 — сбой инференса фиксируется, не роняет отчёт
        meta = {
            "prompt_hash": digest,
            "decoder": decoder,
            "model_id": model_id,
            "usage": _usage_dict(None, 0),
        }
        return schema(), meta, f"llm {schema.__name__}: {type(exc).__name__}: {exc}"
    meta = {
        "prompt_hash": digest,
        "decoder": decoder,
        "model_id": model_id,
        "usage": _usage_dict(usage, reasoning),
    }
    return hypothesis, meta, None


# ---------------------------------------------------------------------------
# Консервативный маппинг (правила OQ и дизайна — буквально)
# ---------------------------------------------------------------------------


def _coerce_hypothesis(value: Any, schema: Any) -> Any:
    """Приводит вход к pydantic-гипотезе (модель или ``dict``; ``None`` → пустая)."""
    if value is None:
        return schema()
    if isinstance(value, schema):
        return value
    if isinstance(value, BaseModel):
        return schema.model_validate(value.model_dump())
    if isinstance(value, dict):
        return schema.model_validate(value)
    raise ValueError(f"гипотеза должна быть {schema.__name__} или dict: {type(value).__name__}")


def _citation_problem(url: str | None, quote: str | None, allowed: set[str], corpus: str) -> str | None:
    """Проверяет URL/цитату; возвращает причину отказа или ``None`` (валидна)."""
    if url is None:
        return "URL не указан"
    if not _url_allowed(url, allowed):
        return f"URL вне собранных источников: {url}"
    if quote is None:
        return None
    if quote not in corpus:
        return f"цитата не найдена в извлечённом тексте: {url}"
    return None


def _coerce_importance(raw: Any) -> str:
    """Приводит важность (короткая/полная форма RU/EN) к значению :class:`Importance`."""
    text = (_clean(raw) or "").lower()
    if text in _IMPORTANCE_ALIASES:
        return _IMPORTANCE_ALIASES[text]
    for member in Importance:
        if text == member.value.lower():
            return member.value
    return Importance.BASIC.value


def _coerce_param_type(raw: Any, rule: str) -> tuple[str, bool]:
    """``param_type`` fact|showcase; showcase демотируется до fact без публичной метрики.

    :return: ``(значение, демотирован)``.
    """
    text = (_clean(raw) or ParamType.FACT.value).lower()
    if text != ParamType.SHOWCASE.value:
        return ParamType.FACT.value, False
    if any(token in (rule or "").lower() for token in _SHOWCASE_TOKENS):
        return ParamType.SHOWCASE.value, False
    return ParamType.FACT.value, True


def _coerce_absence(raw: Any) -> tuple[str, bool]:
    """``absence_rule`` из словаря (дефолт explicit); рискованные значения → human.

    :return: ``(значение, требует_человека)``.
    """
    text = (_clean(raw) or AbsenceRuleType.EXPLICIT.value).lower()
    valid = {member.value for member in AbsenceRuleType}
    if text not in valid:
        return AbsenceRuleType.EXPLICIT.value, True
    return text, text in _RISKY_ABSENCE


def _coerce_inheritance(raw: Any) -> tuple[str, bool]:
    """``inheritance_mode`` из словаря (дефолт direct); неизвестное → risky."""
    text = (_clean(raw) or InheritanceMode.DIRECT.value).lower()
    valid = {member.value for member in InheritanceMode}
    if text not in valid:
        return InheritanceMode.DIRECT.value, True
    return text, False


def _normalize_terms(raw: Any) -> dict[str, list[str]]:
    """Нормализует ``concept_terms`` в ``{lang: [terms]}`` (языки → нижний регистр)."""
    if isinstance(raw, dict):
        out: dict[str, list[str]] = {}
        for lang, terms in raw.items():
            key = str(lang).strip().lower()
            if not key:
                continue
            values = terms if isinstance(terms, list) else [terms]
            cleaned = [str(term).strip() for term in values if str(term).strip()]
            if cleaned:
                out[key] = cleaned
        return out
    if isinstance(raw, (list, tuple)):
        cleaned = [str(term).strip() for term in raw if str(term).strip()]
        return {"ru": cleaned} if cleaned else {}
    return {}


def _normalize_query_patterns(raw: Any) -> dict[str, list[str]]:
    """Нормализует ``query_patterns`` роли в ``{lang: [patterns]}``."""
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str]] = {}
    for lang, patterns in raw.items():
        key = str(lang).strip().lower()
        if not key:
            continue
        values = patterns if isinstance(patterns, list) else [patterns]
        cleaned = [str(item).strip() for item in values if str(item).strip()]
        if cleaned:
            out[key] = cleaned
    return out


def _scout_candidate(candidate: Candidate) -> Candidate:
    """Нормализует кандидата эвристики: ``source="scout"``, ``needs_human=True`` (A3)."""
    candidate.source = "scout"
    candidate.needs_human = True
    return candidate


def _accept_entity(
    name: str | None,
    url: str | None,
    quote: str | None,
    *,
    allowed: set[str],
    corpus: str,
    needs_human: list[str],
    label: str,
) -> bool:
    """Принимает сущность по URL+цитате (grounding-only); иначе → ``needs_human``."""
    clean_name = _clean(name)
    if clean_name is None:
        needs_human.append(f"{label}: имя не подтверждено — отброшено")
        return False
    problem = _citation_problem(_clean(url), _clean(quote), allowed, corpus)
    if problem is not None:
        needs_human.append(f"{label} {clean_name}: {problem} — отброшено")
        return False
    return True


def build_seed_from_hypotheses(
    *,
    subject_name: str,
    landscape: ScoutLandscapeHypothesis | dict[str, Any] | None,
    catalog: ScoutCatalogHypothesis | dict[str, Any] | None,
    allowed_urls: set[str],
    corpus_text: str,
    existing_modules: set[str] | list[str] | None = None,
    existing_roles: set[str] | list[str] | None = None,
    existing_params: set[str] | list[str] | None = None,
) -> tuple[dict[str, Any], ArchetypeHypothesis, list[Candidate], list[str]]:
    """Консервативно отображает гипотезы LLM на ``profile`` и ``Candidate[]``.

    Правила (буквально из дизайна):

    * URL принимается только из ``allowed_urls`` (точное/без trailing slash) — иначе
      элемент отбрасывается с записью в ``needs_human``;
    * ``quote`` принимается только как дословная подстрока ``corpus_text``;
    * ``confidence`` зажимается в ``[0, 1]``;
    * ``module``/``role_binding`` — ``sanitize_slug`` имени, дедуп против существующих и
      внутри батча; неизвестный модуль → параметр пропускается, неизвестная роль →
      ``role_binding=None`` (+ ``needs_human``);
    * ``param_id`` — ``_slug_from_title`` с уникализацией;
    * ``showcase`` демотируется до ``fact`` без публичной величины/метрики;
    * ``absence_rule`` по умолчанию ``explicit``; рискованные значения → ``needs_human``;
    * baselines — из принятых параметров с ``reference_upstream``;
    * **все** кандидаты — ``needs_human=True``, ``source="scout"`` (A3/G2).

    :return: ``(profile, archetype, candidates, needs_human)``.
    """
    land = _coerce_hypothesis(landscape, ScoutLandscapeHypothesis)
    cat = _coerce_hypothesis(catalog, ScoutCatalogHypothesis)

    existing_module_set = {str(item) for item in (existing_modules or [])}
    existing_role_set = {str(item) for item in (existing_roles or [])}
    existing_param_set = {str(item) for item in (existing_params or [])}

    needs_human: list[str] = list(land.needs_human) + list(cat.needs_human)
    profile: dict[str, Any] = {}

    # --- upstream / alternatives / derivatives / cross-line ------------------
    upstream_name: str | None = None
    if land.upstream is not None:
        if _accept_entity(
            land.upstream.name,
            land.upstream.url,
            land.upstream.quote,
            allowed=allowed_urls,
            corpus=corpus_text,
            needs_human=needs_human,
            label="upstream",
        ):
            upstream_name = _clean(land.upstream.name)
    if upstream_name:
        profile["upstream_name"] = upstream_name

    alternatives: list[str] = []
    for product in land.alternatives:
        if _accept_entity(
            product.name,
            product.url,
            product.quote,
            allowed=allowed_urls,
            corpus=corpus_text,
            needs_human=needs_human,
            label="OSS-альтернатива",
        ):
            alternatives.append(_clean(product.name) or "")
    if alternatives:
        profile["alternatives"] = alternatives

    derivatives: list[str] = []
    for derivative in land.commercial_derivatives:
        if _accept_entity(
            derivative.name,
            derivative.url,
            derivative.quote,
            allowed=allowed_urls,
            corpus=corpus_text,
            needs_human=needs_human,
            label="коммерческое производное",
        ):
            derivatives.append(_clean(derivative.name) or "")
    if derivatives:
        profile["commercial_derivatives"] = derivatives

    if land.cross_line:
        profile["cross_line"] = True
    if len(land.products) > 1 or land.cross_line:
        profile["upstream_shared"] = True

    # --- modules: гипотеза + модули параметров + существующие ----------------
    module_ids: list[str] = []
    module_weights: dict[str, float] = {}

    def _add_module(raw: Any, weight: Any = None) -> str:
        module_id = sanitize_slug(str(raw or "").lower())
        if module_id and module_id != "node" and module_id not in module_ids:
            module_ids.append(module_id)
        if module_id and weight is not None:
            module_weights[module_id] = float(weight or _DEFAULT_MODULE_WEIGHT)
        return module_id

    for module in land.modules:
        _add_module(module.name, module.weight if module.weight is not None else _DEFAULT_MODULE_WEIGHT)
    module_specs = [
        {"module_id": module_id, "weight": module_weights.get(module_id, _DEFAULT_MODULE_WEIGHT)}
        for module_id in module_ids
    ]
    if module_specs:
        profile["modules"] = module_specs
    allowed_modules = set(module_ids) | existing_module_set

    # --- roles ---------------------------------------------------------------
    role_ids: list[str] = []
    role_specs: list[dict[str, Any]] = []
    for role in land.roles:
        role_id = sanitize_slug(str(role.name or "").lower())
        if not role_id or role_id == "node" or role_id in role_ids:
            continue
        role_ids.append(role_id)
        spec: dict[str, Any] = {
            "role_id": role_id,
            "markers": [str(marker) for marker in role.markers if str(marker).strip()],
            "query_patterns": _normalize_query_patterns(role.query_patterns),
        }
        # A3: описание роли переносится только при наличии; пустое — не выдумывается
        # (кандидат остаётся неполным до явного заполнения человеком).
        definition = _clean(role.definition or "")
        if definition:
            spec["definition"] = definition
        role_specs.append(spec)
    if role_specs:
        profile["roles"] = role_specs
    allowed_roles = set(role_ids) | existing_role_set

    # --- cohort members (участники когорты/сравнения) ------------------------
    cohort_specs: list[dict[str, Any]] = []
    seen_products: set[str] = set()
    for product in land.products:
        if not _accept_entity(
            product.name,
            product.url,
            product.quote,
            allowed=allowed_urls,
            corpus=corpus_text,
            needs_human=needs_human,
            label="продукт",
        ):
            continue
        product_id = sanitize_slug(str(_clean(product.name) or "").lower())
        if not product_id or product_id == "node" or product_id in seen_products:
            continue
        seen_products.add(product_id)
        spec: dict[str, Any] = {
            "product_id": product_id,
            "role_overlap": 0.5,
            "anchor_relation": "analog",
        }
        # A3: обоснование — только при наличии role_hint; иначе поле не выдумывается.
        role_hint = _clean(product.role_hint)
        if role_hint:
            spec["selection_reason"] = role_hint
        cohort_specs.append(spec)
    if cohort_specs:
        profile["cohort_members"] = cohort_specs

    # --- архетип: эвристика + LLM-сигнал как дополнительное свидетельство ----
    archetype = infer_archetype(profile)
    signal = land.archetype_signal
    if signal is not None and _clean(signal.value):
        value = (_clean(signal.value) or "").upper()
        if value in {member.value for member in Archetype}:
            archetype.signals = [
                *archetype.signals,
                f"LLM archetype_signal: {value} (confidence={_clamp(signal.confidence)})",
            ]
            if archetype.archetype is None or archetype.archetype.value != value:
                archetype.needs_human = True
                needs_human.append(
                    f"архетип: эвристика={archetype.archetype.value if archetype.archetype else None}, "
                    f"LLM={value} — расхождение, требуется человек"
                )

    # --- кандидаты: модули/роли/когорта (reuse эвристик) ---------------------
    candidates: list[Candidate] = []
    for candidate in suggest_modules(profile):
        if candidate.payload.get("module_id") not in existing_module_set:
            candidates.append(_scout_candidate(candidate))
    for candidate in suggest_roles(profile):
        if candidate.payload.get("role_id") not in existing_role_set:
            candidates.append(_scout_candidate(candidate))
    for candidate in suggest_cohort_members(profile):
        candidates.append(_scout_candidate(candidate))

    # --- параметры каталога --------------------------------------------------
    used_param_ids: set[str] = set(existing_param_set)
    accepted_params: list[dict[str, Any]] = []
    for param in cat.params:
        title = _clean(param.title)
        if title is None:
            needs_human.append("параметр без заголовка — пропущен")
            continue
        module = sanitize_slug(str(param.module or "").lower())
        if module in ("", "node"):
            needs_human.append(f"параметр {title!r}: модуль не указан — пропущен")
            continue
        if module not in allowed_modules:
            needs_human.append(
                f"параметр {title!r}: модуль {module!r} не предложен — пропущен"
            )
            continue
        param_url = _clean(param.url)
        param_quote = _clean(param.quote)
        problem = _citation_problem(param_url, param_quote, allowed_urls, corpus_text)
        if problem is not None:
            needs_human.append(f"параметр {title!r}: {problem} — отброшен")
            continue

        rule = _clean(param.rule) or ""
        param_type, demoted = _coerce_param_type(param.param_type, rule)
        if demoted:
            needs_human.append(
                f"параметр {title!r}: showcase без публичной величины — демотирован до fact"
            )
        absence, absence_risky = _coerce_absence(param.absence_rule)
        if absence_risky:
            needs_human.append(
                f"параметр {title!r}: absence_rule={absence} требует человеческого решения"
            )
        role_binding = None
        raw_role = _clean(param.role_binding)
        if raw_role:
            role_id = sanitize_slug(raw_role.lower())
            if role_id in allowed_roles:
                role_binding = role_id
            else:
                needs_human.append(
                    f"параметр {title!r}: role_binding {raw_role!r} не предложен — снят"
                )

        base_slug = _slug_from_title(title)
        param_id = _unique_param_id(used_param_ids, base_slug)
        used_param_ids.add(param_id)
        concept_terms = _normalize_terms(param.concept_terms) or {}

        payload: dict[str, Any] = {
            "param_id": param_id,
            "title": title,
            "module": module,
            "group": _clean(param.group) or module,
            "importance": _coerce_importance(param.importance),
            "param_type": param_type,
            "rule": rule,
            "absence_rule": absence,
            "role_binding": role_binding,
            "concept_terms": concept_terms,
            "search_langs": ["ru", "en"],
        }
        candidates.append(
            Candidate(
                kind="param",
                payload=payload,
                confidence=_clamp(param.confidence, 0.5),
                rationale=_clean(param.rationale) or f"параметр из веб-разведки: {title}",
                source="scout",
                needs_human=True,
            )
        )
        accepted_params.append(
            {
                "param_id": param_id,
                "module": module,
                "reference_upstream": _clean(param.reference_upstream),
                "min_version": _clean(param.min_version),
                "inheritance_mode": _coerce_inheritance(param.inheritance_mode)[0],
            }
        )

    # --- baselines: из принятых параметров с reference_upstream --------------
    for entry in accepted_params:
        reference = entry["reference_upstream"]
        if not reference:
            continue
        baseline_id = sanitize_slug(str(reference).lower())
        if not baseline_id or baseline_id == "node":
            needs_human.append(
                f"baseline для {entry['param_id']}: не удалось вывести baseline_id из {reference!r}"
            )
            continue
        candidates.append(
            Candidate(
                kind="baseline",
                payload={
                    "baseline_id": baseline_id,
                    "param_id": entry["param_id"],
                    "min_version": entry["min_version"],
                    "inheritance_mode": entry["inheritance_mode"],
                    "note": "кандидат веб-разведки (не подтверждён человеком)",
                },
                confidence=0.5,
                rationale=f"наследование {entry['param_id']} от {reference}",
                source="scout",
                needs_human=True,
            )
        )

    return profile, archetype, _dedup_sorted(candidates), _dedupe(needs_human)


def _dedup_sorted(candidates: list[Candidate]) -> list[Candidate]:
    """Убирает дубли по :meth:`Candidate.key` и сортирует по ``(kind, key)``."""
    seen: set[str] = set()
    unique: list[Candidate] = []
    for candidate in candidates:
        key = candidate.key()
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return sorted(unique, key=lambda candidate: (candidate.kind, candidate.key()))


# ---------------------------------------------------------------------------
# Публичный вход
# ---------------------------------------------------------------------------


def scout_package(
    *,
    subject_name: str,
    category: str | None = None,
    seed_urls: Any = (),
    hints: dict[str, Any] | None = None,
    search: SearchAdapterPort | None = None,
    crawler: CrawlerPort,
    extractor: ExtractorPort,
    discovery_llm: DiscoveryLLMPort,
    only: Any = None,
    scope: str = "all",
    lang: str = "ru",
    limit: int = 10,
    max_pages: int = 8,
    max_chars: int = 24000,
    budget: int | None = None,
    snapshot_date: _dt.date | str | None = None,
) -> PackageScoutReport:
    """Выполняет веб-заземлённую разведку пакета категории.

    Поток: query-planning (детерминированные шаблоны) → SERP (только URL-кандидаты, I19)
    → rank/dedup → fetch seed+top-M (дефолтный ``FetchPlan``: HTTP) → extract → context assembly
    (whitelist = fetched ∪ search ∪ seed, делимитеры ``[source: url]``, усечение
    ``max_chars``) → 1–3 LLM-вызова (landscape/catalog/baselines) → консервативный маппинг
    → ``Candidate[]`` + profile.

    **I18/I19.** LLM не имеет веб-тула; поиск/краул — только здесь. Выход — кандидаты;
    никакого обращения к хранилищу/evidence.

    :param seed_urls: доверенные точки входа (фетчатся всегда, первыми).
    :param hints: подсказки темы + резервные ключи целевого пакета (см. модуль).
    :param search: ``SearchAdapterPort`` или ``None`` (``--no-search``).
    :param only: какие LLM-вызовы делать (``landscape``/``catalog``/``baselines``).
    :param budget: потолок числа SERP-вызовов (``--max-calls``); ``None`` — без лимита.
    """
    snapshot = _as_date(snapshot_date)
    selected = _normalize_only(only)
    errors: list[str] = []
    fetched: list[dict[str, Any]] = []
    page_texts: dict[str, str] = {}
    search_urls: list[str] = []
    search_calls = 0

    seed_list = _dedupe([str(url).strip() for url in (seed_urls or []) if str(url).strip()])
    hint_map: dict[str, Any] = dict(hints or {})

    # 1. SERP: только URL-кандидаты (I19).
    queries = _plan_queries(subject_name, category, hint_map, lang)
    ranked_urls: list[str] = []
    if search is not None:
        for query in queries:
            if budget is not None and search_calls >= budget:
                errors.append("search: потолок вызовов (--max-calls) исчерпан")
                break
            try:
                results = search.search(
                    SearchQuery(text=query, lang=lang, limit=max(1, limit), phase="scout")
                )
            except Exception as exc:  # noqa: BLE001 — сбой провайдера изолируется
                errors.append(f"search {query!r}: {type(exc).__name__}: {exc}")
                continue
            search_calls += 1
            for result in results:
                url = str(getattr(result, "url", "") or "").strip()
                if not url:
                    continue
                if url not in search_urls:
                    search_urls.append(url)
                if url not in ranked_urls:
                    ranked_urls.append(url)

    # 2. Fetch: seed (все) + top-M SERP (I19: retention сырых байтов остаётся у краулера).
    to_fetch: list[str] = list(seed_list)
    for url in ranked_urls:
        if len(to_fetch) >= max_pages:
            break
        if url not in to_fetch:
            to_fetch.append(url)
    for url in to_fetch:
        _fetch_one(
            url,
            crawler=crawler,
            extractor=extractor,
            fetched=fetched,
            page_texts=page_texts,
            errors=errors,
        )

    allowed: set[str] = set(page_texts) | set(search_urls) | set(seed_list)
    corpus_text = _join_pages(page_texts, max_chars)

    # 3. LLM-вызовы (landscape/catalog/baselines) — tool-less (I18).
    landscape = ScoutLandscapeHypothesis()
    catalog = ScoutCatalogHypothesis()
    meta: dict[str, Any] = {
        "landscape": None,
        "catalog": None,
        "baselines": None,
    }

    if "landscape" in selected:
        prompt = _fill_template(
            _load_template(LANDSCAPE_TEMPLATE_VERSION),
            {
                "SUBJECT_NAME": subject_name,
                "HINTS": _render_hints(hint_map),
                "SEED_URLS": _render_urls(seed_list),
                "PAGES": corpus_text,
            },
        )
        landscape, meta["landscape"], error = _call_llm(
            discovery_llm, prompt=prompt, schema=ScoutLandscapeHypothesis
        )
        if error:
            errors.append(error)

    if "catalog" in selected:
        existing_modules = [str(item) for item in (hint_map.get("existing_modules") or [])]
        existing_roles = [str(item) for item in (hint_map.get("existing_roles") or [])]
        existing_titles = [str(item) for item in (hint_map.get("existing_titles") or [])]
        target_count = int(hint_map.get("target_count") or limit)
        prompt = _fill_template(
            _load_template(CATALOG_TEMPLATE_VERSION),
            {
                "SUBJECT_NAME": subject_name,
                "MODULES": ", ".join(existing_modules) or "(нет)",
                "ROLES": ", ".join(existing_roles) or "(нет)",
                "EXISTING_TITLES": "; ".join(existing_titles) or "(нет)",
                "TARGET_COUNT": str(target_count),
                "PAGES": corpus_text,
            },
        )
        catalog, meta["catalog"], error = _call_llm(
            discovery_llm, prompt=prompt, schema=ScoutCatalogHypothesis
        )
        if error:
            errors.append(error)

    # 3b. Опциональный baseline-вызов: какие параметры upstream наследуются.
    if "baselines" in selected:
        baseline_upstream = (
            _clean(landscape.upstream.name)
            if landscape.upstream is not None
            else _clean(hint_map.get("upstream_name"))
        )
        if baseline_upstream:
            existing_params = [str(item) for item in (hint_map.get("existing_params") or [])]
            prompt = _fill_template(
                _load_template(BASELINE_TEMPLATE_VERSION),
                {
                    "UPSTREAM_NAME": baseline_upstream,
                    "PARAM_IDS": ", ".join(existing_params) or "(нет)",
                    "PAGES": corpus_text,
                },
            )
            baseline_hyp, meta["baselines"], error = _call_llm(
                discovery_llm, prompt=prompt, schema=ScoutCatalogHypothesis
            )
            if error:
                errors.append(error)
            for param in baseline_hyp.params:
                catalog.params.append(param)
        else:
            errors.append("baselines: upstream не подтверждён — вызов пропущен")

    # 4. Консервативный маппинг (A3/G2: все кандидаты — человека).
    profile, archetype, candidates, needs_human = build_seed_from_hypotheses(
        subject_name=subject_name,
        landscape=landscape,
        catalog=catalog,
        allowed_urls=allowed,
        corpus_text=corpus_text,
        existing_modules=hint_map.get("existing_modules") or [],
        existing_roles=hint_map.get("existing_roles") or [],
        existing_params=hint_map.get("existing_params") or [],
    )

    # 5. scope: ограничить baselines областью (param_id или модуль).
    if scope and scope != "all":
        param_modules = {
            candidate.payload.get("param_id"): candidate.payload.get("module")
            for candidate in candidates
            if candidate.kind == "param"
        }
        candidates = [
            candidate
            for candidate in candidates
            if candidate.kind != "baseline"
            or candidate.payload.get("param_id") == scope
            or param_modules.get(candidate.payload.get("param_id")) == scope
        ]

    ok = not errors
    if not ok:
        needs_human = _dedupe(
            [*needs_human, "разведка завершилась с ошибками — кандидаты частичные (report.ok=False)"]
        )

    sidecar = _sidecar(
        subject_name=subject_name,
        snapshot=snapshot,
        only=selected,
        scope=scope,
        lang=lang,
        meta=meta,
        archetype=archetype,
        profile=profile,
        fetched=fetched,
        search_urls=search_urls,
        search_calls=search_calls,
        needs_human=needs_human,
        candidates=candidates,
        ok=ok,
        errors=errors,
    )
    return PackageScoutReport(
        subject_name=subject_name,
        archetype=archetype,
        profile=profile,
        candidates=candidates,
        needs_human=needs_human,
        sidecar=sidecar,
        fetched=fetched,
        search_calls=search_calls,
        ok=ok,
        errors=errors,
    )


def _sidecar(
    *,
    subject_name: str,
    snapshot: _dt.date,
    only: set[str],
    scope: str,
    lang: str,
    meta: dict[str, Any],
    archetype: ArchetypeHypothesis,
    profile: dict[str, Any],
    fetched: list[dict[str, Any]],
    search_urls: list[str],
    search_calls: int,
    needs_human: list[str],
    candidates: list[Candidate],
    ok: bool,
    errors: list[str],
) -> dict[str, Any]:
    """Собирает детерминированный sidecar-провенанс разведки пакета."""
    kinds: dict[str, int] = {}
    for candidate in candidates:
        kinds[candidate.kind] = kinds.get(candidate.kind, 0) + 1
    return {
        "snapshot_date": snapshot.isoformat(),
        "subject_name": subject_name,
        "only": sorted(only),
        "scope": scope,
        "lang": lang,
        "templates": {
            "landscape": LANDSCAPE_TEMPLATE_VERSION if "landscape" in only else None,
            "catalog": CATALOG_TEMPLATE_VERSION if "catalog" in only else None,
            "baselines": BASELINE_TEMPLATE_VERSION if "baselines" in only else None,
        },
        "llm": meta,
        "archetype": archetype.as_dict(),
        "profile": {
            "upstream_name": profile.get("upstream_name"),
            "alternatives": profile.get("alternatives", []),
            "commercial_derivatives": profile.get("commercial_derivatives", []),
            "cross_line": bool(profile.get("cross_line")),
            "modules": [spec.get("module_id") for spec in profile.get("modules", [])],
            "roles": [spec.get("role_id") for spec in profile.get("roles", [])],
            "cohort_members": [spec.get("product_id") for spec in profile.get("cohort_members", [])],
        },
        "fetched": fetched,
        "search_urls": list(search_urls),
        "search_calls": search_calls,
        "needs_human": list(needs_human),
        "candidate_count": len(candidates),
        "candidate_kinds": kinds,
        "ok": ok,
        "errors": list(errors),
    }


def write_scout_artifacts(
    report: PackageScoutReport,
    out_dir: str | Path,
    snapshot_date: _dt.date | str,
) -> tuple[Path, Path]:
    """Пишет ``{date}.candidates.json`` + ``{date}.provenance.json`` (детерминированно).

    Запись **атомарна** и **не** трогает артефакты пакета: scout пишет только в
    ``out_dir`` (обычно ``packages/{case_id}/sources/scout``), никакого обращения к
    хранилищу прогона (I19). Файл кандидатов совместим с ``hound package apply --from``.

    :return: ``(candidates_path, provenance_path)``.
    """
    date_str = (
        snapshot_date.isoformat()
        if isinstance(snapshot_date, _dt.date)
        else str(snapshot_date)
    )
    directory = Path(out_dir)
    directory.mkdir(parents=True, exist_ok=True)
    candidates_path = directory / f"{date_str}.candidates.json"
    provenance_path = directory / f"{date_str}.provenance.json"

    atomic_write_text(candidates_path, dumps_candidates(report.candidates))
    sidecar = dict(report.sidecar)
    sidecar.setdefault("snapshot_date", date_str)
    sidecar.setdefault("subject_name", report.subject_name)
    sidecar.setdefault("needs_human", list(report.needs_human))
    sidecar.setdefault("ok", report.ok)
    sidecar.setdefault("errors", list(report.errors))
    atomic_write_text(
        provenance_path,
        json.dumps(sidecar, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
    )
    return candidates_path, provenance_path
