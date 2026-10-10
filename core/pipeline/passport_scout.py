"""coHound — авто-генерация **черновика** паспорта продукта (``passport-scout``, M1).

Сценарий **разведки паспорта**: по заданному URL и (опционально) внешнему поиску
собираются страницы, извлекается текст, LLM-разведка предлагает **гипотезу** паспорта,
а консервативный маппинг формирует валидный :class:`~core.domain.models.Passport`
в состоянии **черновика**.

Ключевые инварианты
-------------------
* **Черновик, не публикация.** Сателлиты получают только ``state=draft|not_found`` и
  ``evidence_state=claimed``; состояния ``approved``/``edited_by_human`` и
  ``evidence_state=verified`` **никогда** не выставляются автоматически (precision-first
  §4.3, A3). Продвижение — человеческое решение G2.
* **Whitelist URL (I10/I15).** Любой URL из выхода LLM, не входящий во множество
  реально фетченных/найденных, отбрасывается в ``needs_human`` и не попадает в YAML.
* **Цитаты не генерируются.** ``relationship_evidence``/``Upstream.evidence`` ставятся
  только если ``quote`` — дословная подстрока переданного LLM текста.
* **I18/I19 — структурная граница.** Модуль не импортирует ``core.store``,
  ``Cell``/``LLMCellOutput``/``CellEvidence`` и не пишет evidence: выход — гипотеза.
  LLM-порт не принимает ``tools``/``web_search``.
* **I13/§13.** ``failed``/публикуемые статусы здесь не возникают. ``product_id`` —
  детерминированный slug (иммутабельность): повторный запуск с тем же именем
  переиспользует каталог продукта.

Нейтрален (§1.3): без названий продуктов/вендоров и предметных терминов.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field

from core.domain.enums import EvidenceState, Origin, Relationship, SatelliteState
from core.domain.models import Citation, Passport, Satellite, Upstream
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
from core.ports.discovery_llm import (
    DiscoveryLLMRequest,
    DiscoveryLLMPort,
)
from core.ports.extractor import ExtractorPort
from core.ports.search import SearchAdapterPort, SearchQuery
from core.util.hashing import content_hash
from core.util.security import sanitize_slug

__all__ = [
    "DISCOVERY_TEMPLATE_VERSION",
    "PassportScoutError",
    "EvidenceHint",
    "SatelliteHypothesis",
    "DiscoveryPassportHypothesis",
    "PassportScoutReport",
    "scout_passport",
    "build_passport_draft",
    "write_passport_draft",
]

#: Версия шаблона промпта разведки. Имя файла — ``templates/{VERSION}.txt``
#: (детерминизм: смена шаблона = смена ``prompt_hash``).
DISCOVERY_TEMPLATE_VERSION = "discovery_passport_v1"

#: Поля-перечисления, допустимые в гипотезе сателлита.
_ORIGIN_VALUES = {item.value: item for item in Origin}
_RELATIONSHIP_VALUES = {item.value: item for item in Relationship}


class PassportScoutError(RuntimeError):
    """Черновик паспорта не удалось построить/валидировать (это баг, не данные)."""


# ---------------------------------------------------------------------------
# Схемы гипотезы (permissive: всё опционально + confidence/rationale)
# ---------------------------------------------------------------------------


class EvidenceHint(BaseModel):
    """Подсказка об источнике из выхода LLM (URL + цитата-кандидат)."""

    model_config = ConfigDict(extra="ignore")

    url: str | None = None
    quote: str | None = None
    confidence: float = 0.0
    rationale: str | None = None


class SatelliteHypothesis(BaseModel):
    """Гипотеза сателлита: существование и связь в роли — два независимых сигнала."""

    model_config = ConfigDict(extra="ignore")

    role: str | None = None
    component: str | None = None
    origin: str | None = None
    relationship: str | None = None
    evidence_hint: EvidenceHint | None = None
    exists_confidence: float = 0.0
    relationship_confidence: float = 0.0
    rationale: str | None = None


class DiscoveryPassportHypothesis(BaseModel):
    """Гипотеза паспорта из выхода LLM (permissive, всё опционально)."""

    model_config = ConfigDict(extra="ignore")

    canonical_name: str | None = None
    vendor: str | None = None
    edition_assessed: str | None = None
    version: str | None = None
    jurisdiction: str | None = None
    upstream: EvidenceHint | None = None
    upstream_baseline_id: str | None = None
    upstream_version: str | None = None
    doc_domains: list[str] = Field(default_factory=list)
    satellites: list[SatelliteHypothesis] = Field(default_factory=list)
    field_confidence: dict[str, float] = Field(default_factory=dict)
    needs_human: list[str] = Field(default_factory=list)


@dataclass
class PassportScoutReport:
    """Итог разведки паспорта: черновик, sidecar-провенанс и диагностика.

    :param passport: валидный черновик паспорта (сателлиты только ``draft``/``not_found``).
    :param candidates_json: sidecar-провенанс (``prompt_hash``, ``decoder``, ``model_id``,
        ``usage``, ``fetched[]`` с URL/``content_hash``, ``needs_human``, провенанс полей).
    :param needs_human: поля/URL, требующие человеческого решения.
    :param fetched: список фетчей (URL, ``content_hash``, версия экстрактора, ошибка).
    :param ok: ``False``, если были ошибки сбора/инференса (черновик не публикуется).
    :param errors: человекочитаемые ошибки (без трассировок).
    """

    passport: Passport
    candidates_json: dict[str, Any]
    needs_human: list[str] = field(default_factory=list)
    fetched: list[dict[str, Any]] = field(default_factory=list)
    ok: bool = True
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Внутренние помощники
# ---------------------------------------------------------------------------


def _enum_value(mapping: dict[str, Any], value: Any) -> Any:
    """Приводит строку к элементу перечисления или ``None`` (неизвестное — ``None``)."""
    text = _clean(value)
    if text is None:
        return None
    return mapping.get(text.lower())


def _doc_domains(allowed_urls: set[str]) -> list[str]:
    """Хосты реально фетченных/найденных URL, детерминированно и без дублей."""
    domains: list[str] = []
    for url in sorted(allowed_urls):
        host = (urlsplit(url).hostname or "").strip().lower()
        if host and host not in domains:
            domains.append(host)
    return domains


def _build_prompt(
    *,
    product_name: str,
    url: str,
    roles: list[str] | None,
    pages: str,
) -> str:
    """Собирает промпт разведки из версионированного шаблона (без сети)."""
    mapping = {
        "PRODUCT_NAME": product_name,
        "URL": url,
        "ROLES": ", ".join(roles) if roles else "(не заданы)",
        "PAGES": pages,
    }
    return _fill_template(_load_template(DISCOVERY_TEMPLATE_VERSION), mapping)


# ---------------------------------------------------------------------------
# Консервативный маппинг гипотезы → доменный черновик
# ---------------------------------------------------------------------------


def _build_upstream(
    hypothesis: DiscoveryPassportHypothesis,
    *,
    allowed: set[str],
    corpus_text: str,
    needs_human: list[str],
    provenance: dict[str, Any],
) -> Upstream | None:
    """Строит ``Upstream`` только при подтверждённых baseline_id+version+citation."""
    baseline_id = _clean(hypothesis.upstream_baseline_id)
    version = _clean(hypothesis.upstream_version)
    hint = hypothesis.upstream
    hint_url = _clean(hint.url) if hint is not None else None
    hint_quote = _clean(hint.quote) if hint is not None else None

    attempted = any((baseline_id, version, hint is not None))
    if not (baseline_id and version and hint_url and hint_quote):
        if attempted:
            needs_human.append(
                "upstream не подтверждён (нужны baseline_id, version и citation url+quote)"
            )
        return None
    if not _url_allowed(hint_url, allowed):
        needs_human.append(f"upstream: URL вне собранных источников отброшен: {hint_url}")
        return None
    if hint_quote not in corpus_text:
        needs_human.append("upstream: цитата не найдена в извлечённом тексте")
        return None
    provenance["upstream"] = {
        "baseline_id": baseline_id,
        "version": version,
        "citation": {"url": hint_url, "quote": hint_quote},
    }
    return Upstream(
        baseline_id=baseline_id,
        version=version,
        evidence=Citation(url=hint_url, quote=hint_quote),
    )


def _build_satellite(
    hyp: SatelliteHypothesis,
    *,
    allowed: set[str],
    corpus_text: str,
    needs_human: list[str],
    provenance: list[dict[str, Any]],
) -> Satellite:
    """Строит **черновой** сателлит: никогда ``approved``/``verified`` (precision-first)."""
    component = _clean(hyp.component)
    state = (
        SatelliteState.DRAFT
        if component is not None or hyp.exists_confidence > 0.0
        else SatelliteState.NOT_FOUND
    )
    relationship_evidence: Citation | None = None
    hint = hyp.evidence_hint
    if hint is not None:
        hint_url = _clean(hint.url)
        hint_quote = _clean(hint.quote)
        if hint_url is None:
            pass
        elif not _url_allowed(hint_url, allowed):
            needs_human.append(
                f"сателлит {component or hyp.role or '?'}: URL вне собранных источников "
                f"отброшен: {hint_url}"
            )
        elif hint_quote and hint_quote in corpus_text:
            relationship_evidence = Citation(url=hint_url, quote=hint_quote)
        else:
            needs_human.append(
                f"сателлит {component or hyp.role or '?'}: связь не подтверждена цитатой"
            )
    satellite = Satellite(
        role=_clean(hyp.role) or "unknown",
        state=state,
        component=component,
        origin=_enum_value(_ORIGIN_VALUES, hyp.origin),
        relationship=_enum_value(_RELATIONSHIP_VALUES, hyp.relationship),
        evidence_state=EvidenceState.CLAIMED,
        relationship_evidence=relationship_evidence,
    )
    provenance.append(
        {
            "role": satellite.role,
            "component": component,
            "state": state.value,
            "evidence_state": satellite.evidence_state.value if satellite.evidence_state else None,
            "relationship_evidence": (
                relationship_evidence.model_dump() if relationship_evidence else None
            ),
            "exists_confidence": hyp.exists_confidence,
            "relationship_confidence": hyp.relationship_confidence,
        }
    )
    return satellite


def build_passport_draft(
    *,
    product_name: str,
    hypothesis: DiscoveryPassportHypothesis,
    allowed_urls: set[str],
    corpus_text: str,
    snapshot_date: _dt.date,
) -> tuple[Passport, list[str], dict[str, Any]]:
    """Консервативно отображает гипотезу на доменный черновик паспорта (precision-first).

    Возвращает ``(passport, needs_human, field_provenance)``. Никакой auto-approve:
    сателлиты — только ``draft``/``not_found``; ``relationship_evidence``/``upstream`` —
    только при подтверждённом URL и дословной цитате.
    """
    needs_human: list[str] = list(hypothesis.needs_human)
    provenance: dict[str, Any] = {}

    product_id = sanitize_slug(product_name) or "product"
    canonical_name = _clean(hypothesis.canonical_name) or product_name

    vendor = _clean(hypothesis.vendor)
    if not vendor:
        vendor = "unknown"
        needs_human.append("vendor не подтверждён — 'unknown'")
    jurisdiction = _clean(hypothesis.jurisdiction)
    if not jurisdiction:
        jurisdiction = "unknown"
        needs_human.append("jurisdiction не подтверждён — 'unknown'")

    upstream = _build_upstream(
        hypothesis,
        allowed=allowed_urls,
        corpus_text=corpus_text,
        needs_human=needs_human,
        provenance=provenance,
    )

    satellite_prov: list[dict[str, Any]] = []
    satellites = [
        _build_satellite(
            hyp,
            allowed=allowed_urls,
            corpus_text=corpus_text,
            needs_human=needs_human,
            provenance=satellite_prov,
        )
        for hyp in hypothesis.satellites
    ]
    provenance["satellites"] = satellite_prov

    passport = Passport(
        product_id=product_id,
        canonical_name=canonical_name,
        vendor=vendor,
        upstream=upstream,
        edition_assessed=_clean(hypothesis.edition_assessed),
        version=_clean(hypothesis.version),
        jurisdiction=jurisdiction,
        doc_domains=_doc_domains(allowed_urls),
        satellites=satellites,
        snapshot_date=snapshot_date,
    )
    provenance["product_id"] = product_id
    provenance["field_confidence"] = dict(hypothesis.field_confidence)
    return passport, _dedupe(needs_human), provenance


# ---------------------------------------------------------------------------
# Публичный вход
# ---------------------------------------------------------------------------


def scout_passport(
    *,
    product_name: str,
    url: str,
    crawler: CrawlerPort,
    extractor: ExtractorPort,
    discovery_llm: DiscoveryLLMPort,
    search: SearchAdapterPort | None = None,
    roles: list[str] | None = None,
    max_pages: int = 5,
    max_satellites: int = 12,
    max_chars: int = 20000,
    today: _dt.date | str | None = None,
) -> PassportScoutReport:
    """Собирает страницы, зовёт LLM-разведку и строит черновик паспорта (M1).

    Поток: fetch заданного ``url`` (``RenderMode.HTTP``) → extract → собрать промпт
    (текст ≤ ``max_chars``) → ``discovery_llm.complete`` в схему
    :class:`DiscoveryPassportHypothesis` → :func:`build_passport_draft` → отчёт.

    :param search: опциональный ``SearchAdapterPort``; для M1 может быть ``None``
        (тогда используется только заданный ``url``). Найденные URL расширяют
        whitelist, но цитата всё равно обязана быть подстрокой извлечённого текста.
    :param roles: роли сателлитов из пакета (подсказка LLM; не меняет инварианты).
    :param max_pages: предел числа фетчей (включая главный URL).
    :param max_satellites: предел числа сателлитов в черновике.
    :param max_chars: предел символов текста, передаваемого LLM.
    :param today: дата снимка (``date``/ISO-строка; ``None`` → сегодня).
    """
    snapshot = _as_date(today)
    errors: list[str] = []
    fetched: list[dict[str, Any]] = []
    page_texts: dict[str, str] = {}
    search_urls: list[str] = []

    # 1. Главная страница заданного URL.
    _fetch_one(
        url,
        crawler=crawler,
        extractor=extractor,
        fetched=fetched,
        page_texts=page_texts,
        errors=errors,
    )

    # 2. Опциональный внешний поиск: только URL-кандидаты + их фетч (в пределах max_pages).
    if search is not None and len(fetched) < max_pages:
        try:
            results = search.search(
                SearchQuery(text=product_name, limit=max(1, max_pages))
            )
        except Exception as exc:  # noqa: BLE001 — сбой провайдера изолируется
            errors.append(f"search: {type(exc).__name__}: {exc}")
            results = []
        for result in results:
            if result.url not in search_urls:
                search_urls.append(result.url)
        for result in results:
            if len(fetched) >= max_pages:
                break
            if result.url in page_texts:
                continue
            _fetch_one(
                result.url,
                crawler=crawler,
                extractor=extractor,
                fetched=fetched,
                page_texts=page_texts,
                errors=errors,
            )

    allowed: set[str] = set(page_texts) | set(search_urls) | {url}
    corpus_text = _join_pages(page_texts, max_chars)
    prompt = _build_prompt(
        product_name=product_name, url=url, roles=roles, pages=corpus_text
    )

    # 3. LLM-инференс в схему гипотезы (единственная попытка, без ретраев — бюджет).
    model_id = getattr(discovery_llm, "model_id", "") or ""
    prompt_hash = content_hash(prompt)
    decoder = getattr(discovery_llm, "decoder", "none")
    usage: Any = None
    reasoning_tokens = 0
    hypothesis = DiscoveryPassportHypothesis()
    try:
        response = discovery_llm.complete(
            DiscoveryLLMRequest(
                prompt=prompt,
                response_schema=DiscoveryPassportHypothesis,
                model_id=model_id,
            )
        )
        output = response.output
        if isinstance(output, DiscoveryPassportHypothesis):
            hypothesis = output
        elif isinstance(output, BaseModel):
            hypothesis = DiscoveryPassportHypothesis.model_validate(output.model_dump())
        prompt_hash = response.prompt_hash or prompt_hash
        decoder = response.decoder or decoder
        usage = response.usage
        reasoning_tokens = int(getattr(response, "reasoning_tokens", 0) or 0)
    except Exception as exc:  # noqa: BLE001 — сбой инференса фиксируется, не роняет отчёт
        errors.append(f"llm: {type(exc).__name__}: {exc}")

    # 4. Консервативный маппинг и обязательная валидация домена.
    hypothesis.satellites = hypothesis.satellites[: max(0, max_satellites)]
    passport, needs_human, provenance = build_passport_draft(
        product_name=product_name,
        hypothesis=hypothesis,
        allowed_urls=allowed,
        corpus_text=corpus_text,
        snapshot_date=snapshot,
    )
    try:
        passport = Passport.model_validate(passport.model_dump(mode="json"))
    except Exception as exc:  # noqa: BLE001 — невалидный черновик = баг сборки
        raise PassportScoutError(
            f"собранный черновик паспорта невалиден (§4.3): {exc}"
        ) from exc

    candidates_json: dict[str, Any] = {
        "template_version": DISCOVERY_TEMPLATE_VERSION,
        "prompt_hash": prompt_hash,
        "decoder": decoder,
        "model_id": model_id,
        "usage": _usage_dict(usage, reasoning_tokens),
        "fetched": fetched,
        "search_urls": list(search_urls),
        "needs_human": list(needs_human),
        "field_provenance": provenance,
    }
    return PassportScoutReport(
        passport=passport,
        candidates_json=candidates_json,
        needs_human=needs_human,
        fetched=fetched,
        ok=not errors,
        errors=errors,
    )


def write_passport_draft(
    report: PassportScoutReport,
    passports_dir: str | Path,
    snapshot_date: _dt.date | str,
) -> tuple[Path, Path]:
    """Пишет черновик ``passports/{product_id}/{date}.yaml`` + ``.candidates.json``.

    Запись **детерминирована**: ``sort_keys=True``, ``allow_unicode=True``; sidecar
    сериализуется тем же контрактом. Возвращает ``(yaml_path, sidecar_path)``.
    """
    date_str = (
        snapshot_date.isoformat()
        if isinstance(snapshot_date, _dt.date)
        else str(snapshot_date)
    )
    out_dir = Path(passports_dir) / report.passport.product_id
    out_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = out_dir / f"{date_str}.yaml"
    sidecar_path = out_dir / f"{date_str}.candidates.json"

    yaml_path.write_text(
        yaml.safe_dump(
            report.passport.model_dump(mode="json"),
            sort_keys=True,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    sidecar = dict(report.candidates_json)
    sidecar.setdefault("product_id", report.passport.product_id)
    sidecar.setdefault("snapshot_date", date_str)
    sidecar_path.write_text(
        json.dumps(sidecar, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return yaml_path, sidecar_path
