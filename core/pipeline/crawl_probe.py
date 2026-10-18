"""coHound — ядро решений автоподбора осей краула (``hound package crawl-probe``).

Чистое ядро: наблюдения → вердикт по ключу ``domain_profiles.yaml``, плюс хелперы
URL-сорсинга, разбор/склейка шапки файла и рендер proposal. Сетевого I/O здесь нет
(краулеры инъектируются драйверу :mod:`core.pipeline.crawl_probe_driver`); запись
proposal/правка файла — тонкие обёртки над :func:`core.pipeline.package_io.atomic_write_text`.

**A3 (замена дефолта — по измерению).** Любая ось предлагается только по наблюдению
(измерению). Неразрешимый ключ **честно** попадает в вердикт как ``unresolved`` с причиной —
он не подставляется дефолтом и не исчезает из отчёта. Причина диагностична: сбой
**транспорта** (``both_tiers_failed``) отличается от «транспорт дошёл, но экстракция дала
0 символов» (``extraction_empty``, :func:`_extraction_empty`) — оператор видит, куда
смотреть: на краулер или на экстрактор.

**Оси (D2/D4).** ``fetch`` решается лестницей; ``strategy`` пишется **только** для
``fetch: browser`` (при ``http`` она нормализуется в ``AUTO``, см.
:func:`core.pipeline.fetch_plan.resolve_fetch_plan`); ``extract: document`` предлагается
только по положительному свидетельству формата, иначе в вердикте честно стоит ``null``
(«не определено — существующее значение сохраняется»: ``--write`` перетирает ``extract``
лишь при ``document``); ``wait_for`` не синтезируется **никогда** — если ключ уже несёт
авторское ``wait_for``, драйвер применяет **именно его** к браузерным тирам (это измерение
с задокументированной конфигурацией, не изобретение значения), а наблюдение честно несёт
``wait_for_applied``; при отсутствии авторского условия — ``wait_for=None`` и лишь
флаг-подсказка ``wait_for_recommended`` при положительном свидетельстве (заглушка честного
рендера); ``rate_limit`` не трогается, кроме наблюдённого ``429`` (антибот ``401/403`` — не
сигнал частоты).

**Робот-этика.** :class:`RobotsGate` разбирает ``robots.txt`` собственной реализацией
:mod:`core.util.robots` (единая реализация; ``urllib.robotparser`` не используется);
текст ленты инъектируется (локальный путь не идёт через egress-слой, энфорсящий robots).

Нейтрален к предметной категории (§1.3): домены/ключи — данные. Зависимости слоя
``pipeline`` — ``domain``/``ports``/``pipeline``/``config``/``util``.
"""

from __future__ import annotations

import copy
import datetime as _dt
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec
from core.pipeline.fetch_plan import _select_best
from core.pipeline.package_io import atomic_write_text
from core.util.document_formats import (
    DOCUMENT_SUFFIXES,
    document_content_type,
    document_suffix,
)
from core.util.robots import RobotsGate

__all__ = [
    "MIN_USABLE_TEXT",
    "PROBE_RATE_LIMIT",
    "RATE_LIMIT_HINT",
    "DEFAULT_DOCUMENT_SUFFIXES",
    "Observation",
    "ProbeConfidence",
    "ProbeKeyStatus",
    "ProbeReason",
    "ProbeSource",
    "ProbeTier",
    "RobotsGate",
    "UrlProbe",
    "Verdict",
    "decide",
    "detect_document_suffix",
    "detect_extract_route",
    "group_urls_by_key",
    "is_usable",
    "pick_urls",
    "render_profiles_with_verdicts",
    "render_proposal_yaml",
    "split_header",
    "synthesize_url",
    "unresolved_verdict",
    "write_crawl_probe_proposal",
]

#: Порог «извлекаемого текста» для признания страницы пригодной (символов).
#: Мера — именно **извлечённый** текст (реальный ``ExtractorPort``), а не сырые байты:
#: JS-заглушка размером в килобайты даёт ~0 символов и не должна считаться контентом.
MIN_USABLE_TEXT = 200

#: Диапазон межзапросных задержек зонда (сек, uniform) — вежливость к хосту.
PROBE_RATE_LIMIT = RateLimitSpec(min_s=1.0, max_s=3.0)

#: Консервативный диапазон, предлагаемый при наблюдённом ``429`` (требует подтверждения).
RATE_LIMIT_HINT = RateLimitSpec(min_s=2.0, max_s=5.0)

#: Реестр документных расширений (единый источник — ``core.util.document_formats``;
#: ``adapters/extract_document.SUPPORTED_FORMATS`` строится из того же набора).
DEFAULT_DOCUMENT_SUFFIXES: frozenset[str] = frozenset(DOCUMENT_SUFFIXES)

#: HTML-подобные типы контента (родовая классификация, без вендорных строк).
_HTML_CONTENT_TYPES: frozenset[str] = frozenset({"text/html", "application/xhtml+xml"})

#: Статус, трактуемый как сигнал частоты (единственный; 401/403 — антибот, не частота).
_RATE_SIGNAL_STATUS = 429


# ---------------------------------------------------------------------------
# Перечисления наблюдений/вердикта (зеркало sections в docs/status-dictionary.yaml)
# ---------------------------------------------------------------------------


class ProbeSource(StrEnum):
    """Источник URL для ключа (приоритет a→b→c, первый хит побеждает)."""

    URLS = "urls"
    CORPUS = "corpus"
    SYNTHESIZED = "synthesized"


class ProbeTier(StrEnum):
    """Тир лестницы зонда: ``http`` → ``hybrid`` → ``honest``."""

    HTTP = "http"
    HYBRID = "hybrid"
    HONEST = "honest"


class ProbeConfidence(StrEnum):
    """Уверенность наблюдения/вердикта."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ProbeKeyStatus(StrEnum):
    """Исход зонда по ключу: разрешён (оси предложены) либо честно неразрешён."""

    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"


class ProbeReason(StrEnum):
    """Причина ``unresolved`` (не подставляем дефолт — A3).

    ``BOTH_TIERS_FAILED`` — сбой **транспорта** (raise/``unavailable``, не-2xx, пустой
    ответ); ``EXTRACTION_EMPTY`` — транспорт дошёл до контента, но экстракция дала 0
    символов (сбой **экстрактора**). Это разные диагнозы: первый смотрит на краулер,
    второй — на экстрактор.
    """

    NO_URL = "no_url"
    UNKNOWN_KEY = "unknown_key"
    BOTH_TIERS_FAILED = "both_tiers_failed"
    EXTRACTION_EMPTY = "extraction_empty"
    BROWSER_UNAVAILABLE = "browser_unavailable"
    BROWSER_DISABLED = "browser_disabled"
    ROBOTS_DISALLOWED = "robots_disallowed"
    FETCH_BUDGET_EXHAUSTED = "fetch_budget_exhausted"


# ---------------------------------------------------------------------------
# Наблюдение страницы (результат одного тира по одному URL)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Observation:
    """Наблюдение одного тира над одним URL (вход предиката пригодности и ``decide``).

    ``raised`` — фетч бросил исключение (наблюдения страницы нет); ``unavailable`` —
    тир не смог исполниться вовсе (нет зависимости/бинарей, ``CrawlUnavailable``).
    ``status_code is None`` — **неизвестно**, а не сбой (как и ``content_type=None``).
    ``text_chars`` — длина **извлечённого** текста (реальный экстрактор), ``content_bytes`` —
    объём сырых байтов (для отличения пустого ответа от заглушки). ``wait_for_applied`` —
    было ли к этому браузерному тиру применено **авторское** JS-условие ключа (``False`` у
    HTTP-тира и у браузерного без авторского условия): оператор отличает «browser+wait_for
    сработал» от «сработал голый browser».
    """

    tier: ProbeTier
    url: str
    source: ProbeSource
    raised: bool = False
    unavailable: bool = False
    fetch_mode: FetchMode | None = None
    status_code: int | None = None
    content_type: str | None = None
    content_bytes: int = 0
    text_chars: int = 0
    error: str | None = None
    attempts: int = 1
    wait_for_applied: bool = False

    @property
    def confidence(self) -> ProbeConfidence:
        """Уверенность наблюдения: синтезированный URL — ``low`` (хост не подтверждён)."""
        return (
            ProbeConfidence.LOW
            if self.source is ProbeSource.SYNTHESIZED
            else ProbeConfidence.HIGH
        )

    @property
    def is_browser(self) -> bool:
        """Наблюдение браузерного тира."""
        return self.tier is not ProbeTier.HTTP

    @property
    def looks_like_stub(self) -> bool:
        """Похоже ли браузерное наблюдение на JS-заглушку (положительное свидетельство).

        Родовой признак: браузерный тир вернул непустые байты HTML с успешным (или
        неизвестным) статусом, но извлёк меньше :data:`MIN_USABLE_TEXT` символов — типичный
        снапшот челленджа ПОСЛЕ ``domcontentloaded`` до JS-перезагрузки (мотив ``wait_for``).
        """
        if not self.is_browser or self.raised or self.unavailable:
            return False
        if self.status_code is not None and not (200 <= self.status_code < 300):
            return False
        if self.content_bytes <= 0:
            return False
        content_type = _base_content_type(self.content_type)
        if content_type and content_type not in _HTML_CONTENT_TYPES:
            return False
        return self.text_chars < MIN_USABLE_TEXT

    def as_dict(self) -> dict[str, Any]:
        """JSON/YAML-совместимое представление наблюдения."""
        return {
            "tier": self.tier.value,
            "url": self.url,
            "source": self.source.value,
            "confidence": self.confidence.value,
            "raised": self.raised,
            "unavailable": self.unavailable,
            "fetch_mode": self.fetch_mode.value if self.fetch_mode is not None else None,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "content_bytes": self.content_bytes,
            "text_chars": self.text_chars,
            "error": self.error,
            "attempts": self.attempts,
            "wait_for_applied": self.wait_for_applied,
        }


def _base_content_type(content_type: str | None) -> str:
    """Нормализует ``content-type`` до медиатипа без параметров (нижний регистр)."""
    return (content_type or "").split(";")[0].strip().lower()


def detect_document_suffix(
    url: str,
    content_type: str | None,
    supported_suffixes: Iterable[str] = DEFAULT_DOCUMENT_SUFFIXES,
) -> str | None:
    """Документное расширение по URL (суффикс пути) или ``content_type``, иначе ``None``.

    Классификация — единый источник :mod:`core.util.document_formats` (тот же, что
    ``crawl.document_level=off`` и роутинг экстрактора): сначала суффикс пути URL
    (percent-декодированный), затем карта ``content-type``. ``None`` ⇒ HTML-путь
    (trafilatura). ``supported_suffixes`` — композиционный фильтр: результат принимается,
    только если входит в переданный набор (дефолт — реестр util).
    """
    suffixes = frozenset(str(suffix).lower() for suffix in supported_suffixes)
    suffix = document_suffix(url)
    if suffix is None:
        suffix = document_content_type(content_type)
    return suffix if suffix in suffixes else None


def detect_extract_route(
    url: str,
    content_type: str | None,
    supported_suffixes: Iterable[str] = DEFAULT_DOCUMENT_SUFFIXES,
) -> ExtractionRoute:
    """Ось ``extract``: ``document`` только по положительному свидетельству, иначе ``auto``.

    ``html`` намеренно не предлагается: принудительный HTML — избыточная оптимизация без
    ценности измерения (A1); ``auto`` сам роутит документ/HTML. В вердикт зонда идёт не
    этот роут, а :func:`_reported_extract` (``document`` либо ``None``): отчёт не должен
    читаться как рекомендация ``auto``, пока ``--write`` её не применяет.
    """
    if detect_document_suffix(url, content_type, supported_suffixes) is not None:
        return ExtractionRoute.DOCUMENT
    return ExtractionRoute.AUTO


def _reported_extract(
    url: str,
    content_type: str | None,
    supported_suffixes: Iterable[str] = DEFAULT_DOCUMENT_SUFFIXES,
) -> ExtractionRoute | None:
    """Ось ``extract`` **для вердикта**: ``document`` при свидетельстве, иначе ``None``.

    ``None`` = «не определено — существующее значение сохраняется». Так предложение и
    применённый результат не расходятся: :func:`render_profiles_with_verdicts` пишет
    ``extract`` лишь при ``document`` и никогда не форсирует ``auto``/``html`` (A3).
    """
    if detect_document_suffix(url, content_type, supported_suffixes) is not None:
        return ExtractionRoute.DOCUMENT
    return None


def is_usable(
    observation: Observation,
    *,
    min_text: int = MIN_USABLE_TEXT,
    supported_suffixes: Iterable[str] = DEFAULT_DOCUMENT_SUFFIXES,
) -> bool:
    """Предикат пригодности страницы (родовой, без вендорных строк и новых эвристик).

    ``usable(page)`` ⇔ фетч не бросил **и** статус неизвестен (``None``) либо 2xx **и**
    контент — HTML либо поддерживаемый документ (неизвестный ``content-type`` — не сбой)
    **и** извлечено не меньше :data:`MIN_USABLE_TEXT` символов.
    """
    if observation.raised or observation.unavailable:
        return False
    if observation.status_code is not None and not (
        200 <= observation.status_code < 300
    ):
        return False
    content_type = _base_content_type(observation.content_type)
    is_document = (
        detect_document_suffix(observation.url, observation.content_type, supported_suffixes)
        is not None
    )
    kind_ok = is_document or not content_type or content_type in _HTML_CONTENT_TYPES
    if not kind_ok:
        return False
    return observation.text_chars >= min_text


# ---------------------------------------------------------------------------
# Попытка по URL и вердикт по ключу
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UrlProbe:
    """Попытки одного URL: наблюдения тиров в порядке исполнения (http → hybrid → honest)."""

    url: str
    source: ProbeSource
    observations: tuple[Observation, ...] = ()


@dataclass(frozen=True)
class Verdict:
    """Вердикт по ключу ``domain_profiles.yaml`` (предложение осей либо честный отказ)."""

    key: str
    status: ProbeKeyStatus
    confidence: ProbeConfidence
    source: ProbeSource | None = None
    url: str | None = None
    fetch: FetchMode | None = None
    strategy: CrawlStrategy | None = None
    extract: ExtractionRoute | None = None
    wait_for_recommended: bool = False
    #: Авторское ``wait_for`` ключа было применено и дало пригодный контент (не «условие
    #: не нужно» — наоборот, подтверждение, что измерение шло с ним).
    wait_for_confirmed: bool = False
    needs_confirmation: bool = False
    rate_limit_hint: RateLimitSpec | None = None
    reasons: tuple[str, ...] = ()
    detail: str | None = None
    observations: tuple[Observation, ...] = ()
    urls: tuple[str, ...] = ()

    @property
    def resolved(self) -> bool:
        """Разрешён ли ключ (оси предложены)."""
        return self.status is ProbeKeyStatus.RESOLVED

    def as_dict(self) -> dict[str, Any]:
        """JSON/YAML-совместимое представление вердикта."""
        hint = self.rate_limit_hint
        return {
            "key": self.key,
            "status": self.status.value,
            "confidence": self.confidence.value,
            "source": self.source.value if self.source is not None else None,
            "url": self.url,
            "fetch": self.fetch.value if self.fetch is not None else None,
            "strategy": self.strategy.value if self.strategy is not None else None,
            "extract": self.extract.value if self.extract is not None else None,
            "wait_for_recommended": self.wait_for_recommended,
            "wait_for_confirmed": self.wait_for_confirmed,
            "needs_confirmation": self.needs_confirmation,
            "rate_limit_hint": (
                {"min_s": hint.min_s, "max_s": hint.max_s} if hint is not None else None
            ),
            "reasons": list(self.reasons),
            "detail": self.detail,
            "urls": list(self.urls),
            "observations": [observation.as_dict() for observation in self.observations],
        }


def _verdict_confidence(
    source: ProbeSource | None, *, resolved: bool, http_raised: bool
) -> ProbeConfidence:
    """Уверенность вердикта: синтез → low; транзиентный сбой Tier 1 → medium; иначе high."""
    if not resolved:
        return ProbeConfidence.LOW
    if source is ProbeSource.SYNTHESIZED:
        return ProbeConfidence.LOW
    if http_raised:
        # Tier 1 упал (не вернул страницу) — остаётся маловероятный ложный «browser».
        return ProbeConfidence.MEDIUM
    return ProbeConfidence.HIGH


def _rate_limit_hint(observations: Sequence[Observation]) -> RateLimitSpec | None:
    """Консервативный диапазон при наблюдённом ``429``; иначе ``None`` (не трогаем ось)."""
    if any(o.status_code == _RATE_SIGNAL_STATUS for o in observations):
        return RATE_LIMIT_HINT
    return None


def _stub_flag(observations: Sequence[Observation]) -> bool:
    """Есть ли положительное свидетельство заглушки честного рендера (мотив ``wait_for``).

    Учитываются только наблюдения **без** применённого ``wait_for``: если ключ уже несёт
    авторское условие и рендер с ним всё равно остался заглушкой, рекомендовать добавить
    ``wait_for`` (он уже есть) — неинформативно; сам факт применения виден в
    ``Observation.wait_for_applied``.
    """
    return any(o.looks_like_stub and not o.wait_for_applied for o in observations)


def _reached_content(observation: Observation) -> bool:
    """Дошёл ли **транспорт** до контента (не raise/``unavailable``, 2xx-или-неизвестен, байты > 0).

    Именно этот предикат отделяет сбой транспорта от сбоя экстракции: ``False`` — фетч не
    доставил байты (raise, недоступный движок, антибот-статус, пустой ответ).
    """
    if observation.raised or observation.unavailable:
        return False
    if observation.status_code is not None and not (200 <= observation.status_code < 300):
        return False
    return observation.content_bytes > 0


def _extraction_empty(observation: Observation) -> bool:
    """Транспорт дошёл до контента, но экстракция дала 0 символов ⇒ сбой **экстрактора**.

    Речь строго о **нуле**: малый ненулевой текст остаётся консервативным
    ``both_tiers_failed`` (порог пригодности — не диагноз сбоя). ``status_code=None``
    трактуется как «неизвестно, а не сбой» (как в :func:`is_usable`).
    """
    return _reached_content(observation) and observation.text_chars == 0


def _tier_detail(observation: Observation) -> str:
    """Одна строка ``detail`` про тир: что сработало — транспорт, экстракция или порог.

    При применённом авторском ``wait_for`` строка помечается (``[wait_for применён]``):
    оператор видит, что браузерный тир шёл **с** условием ключа, а не «голым».
    """
    detail = _tier_detail_core(observation)
    if observation.wait_for_applied:
        return f"{detail} [wait_for применён]"
    return detail


def _tier_detail_core(observation: Observation) -> str:
    """Ядро :func:`_tier_detail` без пометки о ``wait_for``."""
    tier = observation.tier.value
    error = f" ({observation.error})" if observation.error else ""
    if observation.raised or observation.unavailable:
        return f"{tier}: транспортный сбой{error}"
    status = "unknown" if observation.status_code is None else str(observation.status_code)
    if observation.status_code is not None and not (200 <= observation.status_code < 300):
        return f"{tier}: транспорт вернул не-2xx (status={status}){error}"
    if observation.content_bytes <= 0:
        return f"{tier}: транспорт вернул пустой ответ (status={status}){error}"
    if _extraction_empty(observation):
        return (
            f"{tier}: транспорт дошёл (status={status}, bytes={observation.content_bytes}), "
            f"но экстракция дала 0 символов — сбой экстрактора, не краулера{error}"
        )
    return (
        f"{tier}: транспорт дошёл (status={status}, bytes={observation.content_bytes}), "
        f"но извлечено {observation.text_chars} символов (< {MIN_USABLE_TEXT}){error}"
    )


def _outcome_detail(observations: Sequence[Observation]) -> str:
    """``detail`` исчерпанной лестницы: по каждому тиру — что сделал транспорт/экстрактор."""
    return "; ".join(_tier_detail(observation) for observation in observations)


def _unresolved_reasons(observations: Sequence[Observation]) -> tuple[ProbeReason, ...]:
    """Причины исчерпанной лестницы в порядке приоритета (диагностика, A3).

    * ``both_tiers_failed`` — есть тир, чей **транспорт** не дошёл до контента
      (raise/``unavailable``/не-2xx/пустой ответ): сбой фетча.
    * ``extraction_empty`` — есть тир, который дошёл до контента, но извлёк 0 символов:
      сбой **экстрактора**. Если тиры разошлись, в отчёт идут обе причины — ``detail``
      (:func:`_outcome_detail`) называет, какой тир что сделал.
    * Ни того, ни другого (дошли до контента, но извлекли мало ненулевого текста) —
      консервативно ``both_tiers_failed`` (поведение не меняется).
    """
    transport_failed = any(not _reached_content(o) for o in observations)
    extraction_empty = any(_extraction_empty(o) for o in observations)
    reasons: list[ProbeReason] = []
    if transport_failed or not extraction_empty:
        reasons.append(ProbeReason.BOTH_TIERS_FAILED)
    if extraction_empty:
        reasons.append(ProbeReason.EXTRACTION_EMPTY)
    return tuple(reasons)


def _verdict_from_probe(
    key: str, probe: UrlProbe, supported_suffixes: Iterable[str]
) -> Verdict:
    """Вердикт по одному URL: http → (hybrid|honest) → unresolved (лестница D2/D4)."""
    observations = probe.observations
    hint = _rate_limit_hint(observations)
    stub = _stub_flag(observations)
    http_obs = next((o for o in observations if o.tier is ProbeTier.HTTP), None)
    browser_obs = [o for o in observations if o.tier is not ProbeTier.HTTP]

    if http_obs is not None and is_usable(http_obs, supported_suffixes=supported_suffixes):
        return Verdict(
            key=key,
            status=ProbeKeyStatus.RESOLVED,
            confidence=_verdict_confidence(probe.source, resolved=True, http_raised=False),
            source=probe.source,
            url=http_obs.url,
            fetch=FetchMode.HTTP,
            strategy=None,  # на http-ключе strategy не пишем (нормализуется в AUTO)
            extract=_reported_extract(
                http_obs.url, http_obs.content_type, supported_suffixes
            ),
            wait_for_recommended=stub,
            rate_limit_hint=hint,
            observations=observations,
            urls=(probe.url,),
        )

    for observation in browser_obs:
        if observation.unavailable:
            return unresolved_verdict(
                key,
                ProbeReason.BROWSER_UNAVAILABLE,
                detail=observation.error,
                source=probe.source,
                url=probe.url,
                observations=observations,
                rate_limit_hint=hint,
                wait_for_recommended=stub,
            )
        if is_usable(observation, supported_suffixes=supported_suffixes):
            # Дизамбигуатор — исход браузерного тира: ``fetch_mode=HTTP`` ⇒ cookie-мост
            # сработал (hybrid); ``BROWSER`` ⇒ рендер (honest, в т.ч. fallback hybrid).
            strategy = (
                CrawlStrategy.HYBRID
                if observation.fetch_mode is FetchMode.HTTP
                else CrawlStrategy.HONEST
            )
            return Verdict(
                key=key,
                status=ProbeKeyStatus.RESOLVED,
                confidence=_verdict_confidence(
                    probe.source,
                    resolved=True,
                    http_raised=bool(http_obs is not None and http_obs.raised),
                ),
                source=probe.source,
                url=observation.url,
                fetch=FetchMode.BROWSER,
                strategy=strategy,
                extract=_reported_extract(
                    observation.url, observation.content_type, supported_suffixes
                ),
                wait_for_recommended=stub,
                # Пригодный контент **с** авторским ``wait_for`` — подтверждение условия,
                # а не свидетельство его ненужности (не понижаем и не снимаем его).
                wait_for_confirmed=observation.wait_for_applied,
                needs_confirmation=bool(stub or hint is not None),
                rate_limit_hint=hint,
                observations=observations,
                urls=(probe.url,),
            )

    if any(o.unavailable for o in browser_obs):
        first = next(o for o in browser_obs if o.unavailable)
        return unresolved_verdict(
            key,
            ProbeReason.BROWSER_UNAVAILABLE,
            detail=first.error,
            source=probe.source,
            url=probe.url,
            observations=observations,
            rate_limit_hint=hint,
            wait_for_recommended=stub,
        )

    # Ни один тир не дал пригодной страницы. Пустой список браузерных тиров означает, что
    # их и не пробовали (``--no-browser``) — это не «оба тира провалились».
    if not browser_obs:
        return unresolved_verdict(
            key,
            ProbeReason.BROWSER_DISABLED,
            detail=_outcome_detail(observations),
            source=probe.source,
            url=probe.url,
            observations=observations,
            rate_limit_hint=hint,
            wait_for_recommended=stub,
        )

    reasons = _unresolved_reasons(observations)
    return unresolved_verdict(
        key,
        reasons[0],
        extra_reasons=reasons[1:],
        detail=_outcome_detail(observations),
        source=probe.source,
        url=probe.url,
        observations=observations,
        rate_limit_hint=hint,
        wait_for_recommended=stub,
    )


def unresolved_verdict(
    key: str,
    reason: ProbeReason,
    *,
    extra_reasons: Sequence[ProbeReason] = (),
    detail: str | None = None,
    source: ProbeSource | None = None,
    url: str | None = None,
    observations: Sequence[Observation] = (),
    rate_limit_hint: RateLimitSpec | None = None,
    wait_for_recommended: bool = False,
    urls: Sequence[str] = (),
) -> Verdict:
    """Собирает честный ``unresolved``-вердикт (причина обязательна, A3).

    ``extra_reasons`` — добавочные причины, когда диагноз не один (тиры разошлись: один —
    сбой транспорта, другой — пустая экстракция): порядок сохраняется, дубликаты сняты.
    """
    reasons = tuple(dict.fromkeys((reason, *extra_reasons)))
    return Verdict(
        key=key,
        status=ProbeKeyStatus.UNRESOLVED,
        confidence=ProbeConfidence.LOW,
        source=source,
        url=url,
        wait_for_recommended=wait_for_recommended,
        needs_confirmation=bool(wait_for_recommended or rate_limit_hint is not None),
        rate_limit_hint=rate_limit_hint,
        reasons=tuple(item.value for item in reasons),
        detail=detail,
        observations=tuple(observations),
        urls=tuple(urls) if urls else ((url,) if url else ()),
    )


def decide(
    key: str,
    probes: Sequence[UrlProbe],
    *,
    supported_suffixes: Iterable[str] = DEFAULT_DOCUMENT_SUFFIXES,
) -> Verdict:
    """Сводит попытки по URL в один вердикт ключа (первый разрешённый URL побеждает).

    Порядок ``probes`` задаёт вызывающий (детерминирован: кратчайший путь, затем лексика).
    Если ни один URL не разрешился — вердикт ``unresolved`` с объединёнными причинами и
    полным списком наблюдений (не молчаливый дефолт, A3).
    """
    if not probes:
        return unresolved_verdict(key, ProbeReason.NO_URL)

    outcomes = [_verdict_from_probe(key, probe, supported_suffixes) for probe in probes]
    for verdict in outcomes:
        if verdict.resolved:
            return verdict

    # Ни один URL не разрешился: объединяем причины/наблюдения всех попыток, чтобы отчёт
    # не терял сигналы (A3) и был детерминирован (первый URL задаёт базу).
    observations = tuple(o for verdict in outcomes for o in verdict.observations)
    reasons = tuple(
        dict.fromkeys(reason for verdict in outcomes for reason in verdict.reasons)
    )
    detail = "; ".join(
        verdict.detail for verdict in outcomes if verdict.detail
    ) or None
    hint = _rate_limit_hint(observations)
    stub = _stub_flag(observations)
    return replace(
        outcomes[0],
        reasons=reasons,
        detail=detail,
        observations=observations,
        urls=tuple(probe.url for probe in probes),
        rate_limit_hint=hint,
        wait_for_recommended=stub,
        needs_confirmation=bool(stub or hint is not None),
    )


# ---------------------------------------------------------------------------
# URL-сорсинг (приоритет a→b→c; детерминированный отбор)
# ---------------------------------------------------------------------------


def synthesize_url(key: str) -> str:
    """Синтезирует ``https://{host}{path}/`` из ключа ``host[+path]`` (низкая уверенность)."""
    host, _, path = str(key).partition("/")
    base = f"https://{host.strip().lower()}"
    tail = path.strip("/")
    return f"{base}/{tail}/" if tail else f"{base}/"


def pick_urls(candidates: Iterable[str], max_urls: int) -> list[str]:
    """Детерминированный отбор URL: кратчайший путь первым, затем лексикографически.

    Дубликаты схлопываются; берётся не более ``max_urls``.
    """
    if max_urls < 1:
        raise ValueError(f"max_urls обязан быть >= 1, получено {max_urls!r}")
    unique = sorted(
        {str(url) for url in candidates},
        key=lambda url: (len(urlsplit(url).path or "/"), url),
    )
    return unique[:max_urls]


def group_urls_by_key(urls: Iterable[str], keys: Iterable[str]) -> dict[str, list[str]]:
    """Группирует URL по лучшему ключу ``host[+path]`` (переиспользует ``_select_best``)."""
    key_list = list(keys)
    mapping = {key: key for key in key_list}
    grouped: dict[str, list[str]] = {key: [] for key in key_list}
    for url in urls:
        matched = _select_best(str(url), mapping)
        if matched is not None:
            grouped[matched].append(str(url))
    return grouped


# ---------------------------------------------------------------------------
# Шапка файла и правка отображения (сохранение комментариев/полей)
# ---------------------------------------------------------------------------


def split_header(raw: str) -> tuple[str, str]:
    """Отделяет ведущий блок комментариев/пустых строк от YAML-отображения.

    PyYAML теряет комментарии, поэтому шапку (человеческое обоснование живых замеров)
    нужно отделять **до** разбора и возвращать **байт-в-байт** после сериализации.
    """
    lines = raw.splitlines(keepends=True)
    index = 0
    for line in lines:
        stripped = line.strip()
        if stripped == "" or stripped.startswith("#"):
            index += 1
        else:
            break
    return "".join(lines[:index]), "".join(lines[index:])


def _dump_mapping(data: Mapping[str, Any]) -> str:
    """Детерминированная сериализация отображения (unicode, **порядок вставки** ключей).

    ``sort_keys=False`` — порядок полей профиля модели задаёт автор/модель (``fetch`` →
    ``extract`` → ``strategy`` → ``wait_for``), а не алфавит: сортировка порождала бы
    шумный diff при неизменных значениях.
    """
    return yaml.safe_dump(
        dict(data), allow_unicode=True, sort_keys=False, default_flow_style=False
    )


#: Верхнеуровневый ключ отображения: ``key:`` либо ``key: value`` (без отступа и ``#``).
_TOP_LEVEL_KEY_RE = re.compile(r"^[^\s#][^:]*:(?:[ \t].*)?$")


def _is_top_level_key(line: str) -> bool:
    """Является ли строка верхнеуровневым ключом ``key:`` (без отступа, не комментарий)."""
    return bool(_TOP_LEVEL_KEY_RE.match(line.rstrip("\n").rstrip("\r")))


def _is_indented(line: str) -> bool:
    """Является ли строка отступным продолжением блока (непустая, начинается с отступа)."""
    return bool(line.strip()) and line[:1] in (" ", "\t")


def _split_top_level_blocks(body: str) -> list[tuple[str, str]]:
    """Разбивает тело на верхнеуровневые блоки, сохраняя байты дословно.

    Возвращает список ``(kind, text)``. ``kind="key"`` — блок ключа (строка ключа плюс
    следующие за ней отступные строки). ``kind="other"`` — дословный прогон между блоками
    (пустые строки, комментарии, «висячие» строки). Склейка в исходном порядке возвращает
    тело байт-в-байт.
    """
    lines = body.splitlines(keepends=True)
    blocks: list[tuple[str, str]] = []
    index = 0
    total = len(lines)
    while index < total:
        if _is_top_level_key(lines[index]):
            chunk = [lines[index]]
            index += 1
            while index < total and _is_indented(lines[index]):
                chunk.append(lines[index])
                index += 1
            blocks.append(("key", "".join(chunk)))
        else:
            end = index
            while end < total and not _is_top_level_key(lines[end]):
                end += 1
            if end == index:  # страховка от зацикливания (недостижимо на валидном YAML)
                end = index + 1
            blocks.append(("other", "".join(lines[index:end])))
            index = end
    return blocks


def _dump_profile(key: str, entry: Mapping[str, Any]) -> str:
    """Сериализует один блок ``key:``, сохраняя порядок полей модели (без сортировки)."""
    return yaml.safe_dump(
        {key: dict(entry)},
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )


def render_profiles_with_verdicts(
    raw: str,
    verdicts: Sequence[Verdict],
    *,
    supported_suffixes: Iterable[str] = DEFAULT_DOCUMENT_SUFFIXES,
) -> str:
    """Правит ``domain_profiles.yaml`` по вердиктам, сохраняя шапку и неизмеренные поля.

    Для разрешённых ключей: всегда ``fetch``; ``strategy`` — только для ``browser``
    (на ``http`` снимается); ``extract: document`` — только по положительному свидетельству,
    иначе существующее значение **сохраняется** (не перетираем ``auto``/``html``).
    ``wait_for``/``rate_limit`` и прочие поля не трогаются. Неразрешённые ключи не меняются.

    **Байтовая экономия.** Переписываются только верхнеуровневые блоки, чьё значение
    изменилось; неизменённые возвращаются **дословно** (ручной стиль — кавычки ``wait_for``,
    порядок полей — сохраняется). Если после применения вердиктов отображение семантически
    не изменилось, возвращается исходный ``raw`` байт-в-байт (no-op: вызывающий видит это
    сравнением строк и файл не трогает).

    :raises ValueError: корень файла не отображение.
    """
    header, body = split_header(raw)
    data = yaml.safe_load(body) if body.strip() else None
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError("domain_profiles.yaml: ожидался YAML-словарь верхнего уровня")

    updated = copy.deepcopy(data)
    for verdict in verdicts:
        if not verdict.resolved or verdict.fetch is None:
            continue
        entry = updated.get(verdict.key)
        if not isinstance(entry, dict):
            entry = {}
        entry["fetch"] = verdict.fetch.value
        if verdict.fetch is FetchMode.BROWSER:
            if verdict.strategy is not None:
                entry["strategy"] = verdict.strategy.value
        else:
            entry.pop("strategy", None)
        if verdict.extract is ExtractionRoute.DOCUMENT:
            entry["extract"] = ExtractionRoute.DOCUMENT.value
        updated[verdict.key] = entry

    if updated == data:
        return raw

    seen: set[str] = set()
    parts: list[str] = [header]
    for kind, text in _split_top_level_blocks(body):
        if kind != "key":
            parts.append(text)
            continue
        parsed = yaml.safe_load(text)
        if not (isinstance(parsed, dict) and len(parsed) == 1):
            parts.append(text)
            continue
        key = next(iter(parsed))
        seen.add(key)
        if key in updated and parsed[key] == updated[key]:
            parts.append(text)  # неизменённый профиль — дословно
        elif key in updated:
            parts.append(_dump_profile(key, updated[key]))
    for key, value in updated.items():
        if key not in seen:
            parts.append(_dump_profile(key, value))
    return "".join(parts)


# ---------------------------------------------------------------------------
# Robots: единственная реализация — ``core.util.robots`` (:class:`RobotsGate`
# промотирован туда; здесь только ре-экспорт для обратной совместимости импортов)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Proposal: рендер и запись
# ---------------------------------------------------------------------------


def render_proposal_yaml(meta: Mapping[str, Any], verdicts: Sequence[Verdict]) -> str:
    """Детерминированный YAML proposal: ``meta`` + ``keys`` + сводка ``unresolved``."""
    keys: dict[str, Any] = {}
    unresolved: list[dict[str, Any]] = []
    for verdict in verdicts:
        payload = verdict.as_dict()
        keys[verdict.key] = payload
        if not verdict.resolved:
            unresolved.append(
                {
                    "key": verdict.key,
                    "reason": list(verdict.reasons),
                    "detail": verdict.detail,
                    "urls": list(verdict.urls),
                }
            )
    document = {
        "meta": dict(meta),
        "keys": keys,
        "unresolved": unresolved,
    }
    return _dump_mapping(document) + "\n"


def write_crawl_probe_proposal(
    out_dir: str | Path,
    snapshot_date: _dt.date | str,
    meta: Mapping[str, Any],
    verdicts: Sequence[Verdict],
) -> Path:
    """Атомарно пишет ``{out}/{date}.proposal.yaml`` (пакет-скоупед; не трогает прогон).

    Зеркалит :func:`core.pipeline.package_scout.write_scout_artifacts`: только ``out_dir``,
    никакого обращения к БД прогона и артефактам пакета.
    """
    date_str = (
        snapshot_date.isoformat()
        if isinstance(snapshot_date, _dt.date)
        else str(snapshot_date)
    )
    directory = Path(out_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{date_str}.proposal.yaml"
    return atomic_write_text(path, render_proposal_yaml(meta, verdicts))
