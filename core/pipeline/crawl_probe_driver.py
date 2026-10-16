"""coHound — read-only драйвер зонда краула (``hound package crawl-probe``).

Исполняет лестницу тиров (:mod:`core.pipeline.crawl_probe`) над URL-кандидатами ключей
``domain_profiles.yaml``. Краулеры и экстрактор **инъектируются** (:class:`ProbePorts`),
поэтому тесты идут без сети; здесь нет импорта адаптеров/хранилища (матрица ``pipeline``).

**Авторское ``wait_for`` ключа.** Если ключ уже несёт ``wait_for`` в ``domain_profiles.yaml``,
драйвер применяет **именно его** к обоим браузерным тирам (``hybrid`` и ``honest``) —
измерение с задокументированной конфигурацией (A3), а не изобретение значения. Ключи без
авторского условия зондируются с ``wait_for=None`` (условие **никогда** не синтезируется).
Факт применения виден в :attr:`~core.pipeline.crawl_probe.Observation.wait_for_applied`.

**Robustness (наблюдение, не баг зонда).** Оркестратор отмечал интермиттентный сбой запуска
реального браузерного тира **дважды подряд в одном процессе** (``CrawlUnavailable: не удалось
запустить браузер Playwright``): первый запуск успешен, сразу следующий — нет, тогда как свежий
процесс успешен. Зонд строит **один** ``BrowserCrawler`` на весь прогон и переиспользует его
контекст между ключами (реальный бэкенд ``_ensure_context`` ленив и идемпотентен), поэтому
повторного запуска «на ключ» нет; на живом прогоне браузерные ключи в одном процессе прошли без
``CrawlUnavailable``. Митигейшн не добавлялся (``BrowserCrawler`` в этой задаче не трогаем).

**Лестница (дизамбигуатор — исход браузерного тира).**

1. Tier 1 — обычный HTTP (:data:`~core.domain.enums.FetchMode.HTTP`) с bounded-retry
   (:func:`~core.util.rate_limit.jittered_backoff`) для транзиентов.
2. Tier 2 — ``browser`` + ``hybrid`` (антибот-cookie мост).
3. Tier 3 — ``browser`` + ``honest`` (честный рендер).

Сообщения :class:`CrawlError` **не классифицируются** (текст ошибки — не сигнал): решение
принимает исход браузерного тира — ``RawPage.fetch_mode == HTTP`` (мост сработал ⇒ hybrid)
против ``BROWSER`` (рендер ⇒ honest). Оба тира провалились ⇒ ``unresolved`` (не выдаём
транзиентный сбой за ``browser``).

**Вежливость.** ``robots.txt`` читается :class:`~core.pipeline.crawl_probe.RobotsGate`;
межзапросная задержка — :class:`~core.util.rate_limit.RateLimiter`; общий бюджет фетчей
ограничен (:data:`MAX_TOTAL_FETCHES`). §10: honest/hybrid рендер публичной JS-страницы —
исполнение её штатного механизма; обход CAPTCHA/платности не делается.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.ports.types import FetchPlan, RawPage
from core.pipeline.crawl_probe import (
    DEFAULT_DOCUMENT_SUFFIXES,
    PROBE_RATE_LIMIT,
    Observation,
    ProbeReason,
    ProbeSource,
    ProbeTier,
    UrlProbe,
    Verdict,
    decide,
    is_usable,
    pick_urls,
    unresolved_verdict,
)
from core.util.rate_limit import RateLimiter, jittered_backoff

__all__ = [
    "HTTP_RETRIES",
    "MAX_TOTAL_FETCHES",
    "ProbePorts",
    "run_probe",
]

#: Число **повторов** Tier 1 (первая попытка + повторы) для абсорбции транзиентов.
HTTP_RETRIES = 2

#: Жёсткий потолок суммарных фетчей зонда (вежливость/предсказуемость).
MAX_TOTAL_FETCHES = 120


@dataclass
class ProbePorts:
    """Инъецируемые порты/швы драйвера (сеть — только через них).

    ``http``/``browser`` — ``CrawlerPort`` (кэш **не** задействуется: ``cache=None`` у
    адаптеров — устаревший кэш-хит вернул бы ``status_code=None`` и смазал вердикт).
    ``browser=None`` ⇒ браузерные тиры недоступны (``browser_unavailable``).
    ``robots`` — объект с ``allowed(url) -> bool`` (обычно
    :class:`~core.pipeline.crawl_probe.RobotsGate`); ``None`` → проверка пропускается.
    ``sleep``/``rng`` — тестовые швы пейсинга (без реального сна).
    """

    http: Any
    extractor: Any
    browser: Any | None = None
    robots: Any | None = None
    sleep: Callable[[float], None] = time.sleep
    rng: random.Random | None = None


class _Budget:
    """Счётчик фетчей: ``spend()`` возвращает ``False`` при исчерпании бюджета."""

    def __init__(self, limit: int) -> None:
        self._left = max(int(limit), 0)

    @property
    def exhausted(self) -> bool:
        return self._left <= 0

    def spend(self) -> bool:
        if self._left <= 0:
            return False
        self._left -= 1
        return True


class _HostPacer:
    """Минимальная межзапросная задержка по хосту (не спит на первом обращении)."""

    def __init__(self, limiter: RateLimiter) -> None:
        self._limiter = limiter
        self._seen: set[str] = set()

    def wait(self, host: str) -> None:
        if not host:
            return
        if host in self._seen:
            self._limiter.wait(host, PROBE_RATE_LIMIT)
        else:
            self._seen.add(host)


def _host(url: str) -> str:
    """Хост URL в нижнем регистре (пустая строка — если не разбирается)."""
    from urllib.parse import urlsplit

    return (urlsplit(url).hostname or "").lower()


def _is_unavailable(exc: BaseException) -> bool:
    """Недоступен ли браузерный движок (``CrawlUnavailable``) без импорта адаптеров.

    Класс ``CrawlUnavailable`` объявлен в адаптерах; ``pipeline`` не импортирует
    ``adapters`` (матрица §1), поэтому признак — имя класса-исключения.
    """
    return type(exc).__name__ == "CrawlUnavailable"


def _observe_page(
    page: RawPage,
    tier: ProbeTier,
    requested_url: str,
    source: ProbeSource,
    extractor: Any,
    *,
    wait_for_applied: bool = False,
) -> Observation:
    """Строит наблюдение из страницы, измеряя **извлечённый** текст реальным экстрактором.

    ``wait_for_applied`` — было ли к фетчу применено авторское JS-условие ключа (видно в
    артефакте: «browser+wait_for сработал» против «сработал голый browser»).
    """
    text_chars = 0
    error: str | None = None
    try:
        doc = extractor.extract(page, ExtractionRoute.AUTO)
        text_chars = len(getattr(doc, "text", "") or "")
        if getattr(doc, "error", None):
            error = f"extract: {doc.error}"
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту зонда
        error = f"extract: {type(exc).__name__}: {exc}"
    return Observation(
        tier=tier,
        url=getattr(page, "url", None) or requested_url,
        source=source,
        fetch_mode=getattr(page, "fetch_mode", None),
        status_code=getattr(page, "status_code", None),
        content_type=getattr(page, "content_type", None),
        content_bytes=len(getattr(page, "content", b"") or b""),
        text_chars=text_chars,
        error=error,
        wait_for_applied=wait_for_applied,
    )


def _probe_tier_http(
    url: str,
    source: ProbeSource,
    *,
    ports: ProbePorts,
    pacer: _HostPacer,
    budget: _Budget,
    retries: int,
) -> Observation:
    """Tier 1: HTTP с bounded-retry; при исчерпании — ``raised``-наблюдение."""
    plan = FetchPlan(FetchMode.HTTP)
    host = _host(url)
    attempts = 0
    last_error: str | None = None
    for attempt in range(retries + 1):
        if not budget.spend():
            return Observation(
                tier=ProbeTier.HTTP,
                url=url,
                source=source,
                raised=True,
                error="fetch budget exhausted",
                attempts=attempts,
            )
        attempts += 1
        pacer.wait(host)
        try:
            page = ports.http.fetch(url, plan)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            last_error = f"{type(exc).__name__}: {exc}"
            if _is_unavailable(exc):
                break
            if attempt < retries:
                ports.sleep(jittered_backoff(attempt, rng=ports.rng))
            continue
        return _observe_page(page, ProbeTier.HTTP, url, source, ports.extractor)
    return Observation(
        tier=ProbeTier.HTTP,
        url=url,
        source=source,
        raised=True,
        error=last_error,
        attempts=attempts,
    )


def _probe_tier_browser(
    tier: ProbeTier,
    strategy: CrawlStrategy,
    url: str,
    source: ProbeSource,
    *,
    ports: ProbePorts,
    pacer: _HostPacer,
    budget: _Budget,
    wait_for: str | None = None,
) -> Observation:
    """Браузерный тир (``hybrid``/``honest``): ``RawPage`` либо ``raised``/``unavailable``.

    ``wait_for`` — **авторское** JS-условие ключа (или ``None``); уходит в ``FetchPlan`` и
    отражается в наблюдении как ``wait_for_applied``. Условие не синтезируется здесь.
    """
    plan = FetchPlan(FetchMode.BROWSER, strategy=strategy, wait_for=wait_for)
    applied = bool(wait_for)
    if not budget.spend():
        return Observation(
            tier=tier,
            url=url,
            source=source,
            raised=True,
            unavailable=True,
            error="fetch budget exhausted",
        )
    pacer.wait(_host(url))
    try:
        page = ports.browser.fetch(url, plan)
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        return Observation(
            tier=tier,
            url=url,
            source=source,
            raised=True,
            unavailable=_is_unavailable(exc),
            error=f"{type(exc).__name__}: {exc}",
            wait_for_applied=applied,
        )
    return _observe_page(
        page, tier, url, source, ports.extractor, wait_for_applied=applied
    )


def _probe_url(
    url: str,
    source: ProbeSource,
    *,
    ports: ProbePorts,
    pacer: _HostPacer,
    budget: _Budget,
    no_browser: bool,
    retries: int,
    supported_suffixes: Sequence[str],
    wait_for: str | None = None,
) -> UrlProbe:
    """Прогон лестницы тиров по одному URL (ранний выход при пригодном результате).

    ``wait_for`` — авторское JS-условие ключа, применяемое к обоим браузерным тирам.
    """
    observations: list[Observation] = []
    http_obs = _probe_tier_http(
        url, source, ports=ports, pacer=pacer, budget=budget, retries=retries
    )
    observations.append(http_obs)
    if is_usable(http_obs, supported_suffixes=supported_suffixes) or no_browser:
        return UrlProbe(url, source, tuple(observations))
    if ports.browser is None:
        observations.append(
            Observation(
                tier=ProbeTier.HYBRID,
                url=url,
                source=source,
                unavailable=True,
                error="browser-краулер недоступен",
            )
        )
        return UrlProbe(url, source, tuple(observations))

    hybrid = _probe_tier_browser(
        ProbeTier.HYBRID,
        CrawlStrategy.HYBRID,
        url,
        source,
        ports=ports,
        pacer=pacer,
        budget=budget,
        wait_for=wait_for,
    )
    observations.append(hybrid)
    if is_usable(hybrid, supported_suffixes=supported_suffixes) or hybrid.unavailable:
        return UrlProbe(url, source, tuple(observations))

    honest = _probe_tier_browser(
        ProbeTier.HONEST,
        CrawlStrategy.HONEST,
        url,
        source,
        ports=ports,
        pacer=pacer,
        budget=budget,
        wait_for=wait_for,
    )
    observations.append(honest)
    return UrlProbe(url, source, tuple(observations))


def run_probe(
    keys: Sequence[str],
    *,
    candidates: Mapping[str, tuple[ProbeSource, Sequence[str]]],
    ports: ProbePorts,
    no_browser: bool = False,
    max_urls_per_key: int = 1,
    supported_suffixes: Sequence[str] = tuple(DEFAULT_DOCUMENT_SUFFIXES),
    http_retries: int = HTTP_RETRIES,
    max_total_fetches: int = MAX_TOTAL_FETCHES,
    wait_for_by_key: Mapping[str, str | None] | None = None,
) -> list[Verdict]:
    """Зондирует ключи и возвращает по вердикту на ключ (порядок ключей сохранён).

    ``candidates[key] = (source, [url, ...])`` — уже разрешённые приоритетом источника
    кандидаты; драйвер применяет детерминированный отбор (:func:`pick_urls`) и робот-этику.
    Ключ без кандидатов, с запретом ``robots.txt`` или без бюджета становится ``unresolved``
    с причиной — не подставляет дефолт и не исчезает (A3).

    ``wait_for_by_key[key]`` — **авторское** ``wait_for`` ключа из ``domain_profiles.yaml``
    (``None``/отсутствие ⇒ ``wait_for=None``). Условие передаётся в браузерные тиры как есть
    и **никогда** не синтезируется драйвером.
    """
    limiter = RateLimiter(sleep=ports.sleep, rng=ports.rng)
    pacer = _HostPacer(limiter)
    budget = _Budget(max_total_fetches)
    verdicts: list[Verdict] = []

    for key in keys:
        if budget.exhausted:
            verdicts.append(
                unresolved_verdict(
                    key,
                    ProbeReason.FETCH_BUDGET_EXHAUSTED,
                    detail="исчерпан бюджет фетчей зонда",
                )
            )
            continue
        entry = candidates.get(key)
        if entry is None:
            verdicts.append(unresolved_verdict(key, ProbeReason.NO_URL))
            continue
        source, urls = entry
        chosen = pick_urls(urls, max_urls_per_key)
        if not chosen:
            verdicts.append(
                unresolved_verdict(key, ProbeReason.NO_URL, source=source)
            )
            continue
        allowed = [url for url in chosen if _robots_allows(ports.robots, url)]
        if not allowed:
            verdicts.append(
                unresolved_verdict(
                    key,
                    ProbeReason.ROBOTS_DISALLOWED,
                    detail="robots.txt запрещает выбранные URL",
                    source=source,
                    url=chosen[0],
                    urls=chosen,
                )
            )
            continue
        key_wait_for = (
            None if wait_for_by_key is None else wait_for_by_key.get(key)
        )
        probes = [
            _probe_url(
                url,
                source,
                ports=ports,
                pacer=pacer,
                budget=budget,
                no_browser=no_browser,
                retries=http_retries,
                supported_suffixes=supported_suffixes,
                wait_for=key_wait_for,
            )
            for url in allowed
        ]
        verdicts.append(decide(key, probes, supported_suffixes=supported_suffixes))
    return verdicts


def _robots_allows(robots: Any | None, url: str) -> bool:
    """Разрешён ли URL робот-политикой (нет политики ⇒ разрешено)."""
    if robots is None:
        return True
    try:
        return bool(robots.allowed(url))
    except Exception:  # noqa: BLE001 — сбой политики не роняет зонд
        return True
