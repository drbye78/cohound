"""Лёгкий HTTP-краулер на ``httpx`` — реализация ``CrawlerPort`` (M1, A1).

Обычный сайт (статический HTML) не требует браузерного движка: достаточно
``httpx`` + ``trafilatura`` (экстрактор — отдельный адаптер). Этот краулер —
**дефолтный лёгкий путь MVP** для ``hound passport-scout``; тяжёлый браузерный
``ScrapyCrawler`` (``crawl_scrapy.py``) остаётся опцией для SPA/JS.

Роль адаптера (``ports/crawler.py``): загрузить страницу и вернуть **сырые байты**
в ``RawPage`` (retention I19). Ответственность:

* **SSRF-этика** — ``validate_url`` (``core.util.security``) до фетча; приватные/
  петлевые/metadata-цели отклоняются ``CrawlUrlError`` (подкласс ``CrawlError`` и
  ``ValueError``);
* **redirects** — ``follow_redirects=True``; ``RawPage.url`` — финальный URL после
  редиректов (``str(response.url)``), ``status_code``/``content_type`` — из ответа;
* **content-addressed кэш** — при внедрённом ``ContentAddressedCache`` попадание по
  ``http_key(url)`` отдаёт ``RawPage`` из байтов с mtime (детерминизм R7 между
  ``run_id``); промах → живой фетч + запись;
* **условный повторный фетч** — рядом с ``.bin`` лежит JSON-сайдкар
  (``.meta.json``) с ``etag``/``last_modified``/``status_code``/``content_type``/
  ``final_url``. Приоритет валидаторов: **план → мета кэша → безусловный GET**.
  Валидаторы ``plan.if_none_match``/``plan.if_modified_since`` (явное намерение
  вызывающего) имеют приоритет; если их нет, а сайдкар несёт ``etag``/
  ``last_modified``, адаптер сам строит эффективный план (``dataclasses.replace``)
  и делает **живой** условный запрос — кэш-шорткат не применяется, сервер
  спрашивается «актуально ли?» и отвечает дешёвым ``304``. Если валидаторов нет
  нигде — поведение прежнее (попадание отдаёт байты, промах фетчит). ``304``
  возвращает ``not_modified=True`` и **закэшированные** байты (пустое тело —
  ошибка).

**Ленивый ``httpx``.** Модуль импортируется без ``httpx``; пакет подтягивается только
в реальном пути. ``available`` показывает готовность (``httpx`` установлен или внедрён
шов).

**Шов для тестов.** Конструктор принимает ``fetcher`` (алиас ``http_get``) —
callable ``(url) -> bytes``. Если он задан, реальный ``httpx`` не задействуется, и
тесты работают без сети.

**BROWSER не поддерживается.** ``fetch(..., plan=FetchPlan(fetch=FetchMode.BROWSER))``
поднимает понятную ``CrawlError`` (не молчаливую подмену на HTTP): браузерный рендер — задача
``ScrapyCrawler``/playwright.

**Ошибки.** Единый тип ``CrawlError`` (переиспользован из ``crawl_scrapy``), чтобы
pipeline ловил один класс для всех краулеров; отсутствие ``httpx`` наследует
``CrawlUnavailable(CrawlError)``. Отказ SSRF — ``CrawlUrlError``. Ответ ``404``/``410`` —
``CrawlNotFound`` (``failure_kind="not_found"``): ресурса нет, а не источник недоступен.

Нейтрален (§1.3): в модуле нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from core.adapters.crawl_scrapy import (
    NOT_FOUND_STATUSES,
    CrawlError,
    CrawlNotFound,
    CrawlRefused,
    CrawlUrlError,
)
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.cache import ContentAddressedCache, http_key
from core.util.robots import RobotsGate
from core.util.security import validate_url

__all__ = [
    "CrawlUnavailable",
    "HttpCrawler",
    "HttpGetFn",
    "DEFAULT_TIMEOUT",
    "DEFAULT_USER_AGENT",
    "DEFAULT_ACCEPT",
    "DEFAULT_ACCEPT_LANGUAGE",
]

#: Внедряемый шов: ``(url) -> сырые байты страницы``.
HttpGetFn = Callable[[str], bytes]

#: Таймаут HTTP-запроса, с.
DEFAULT_TIMEOUT = 30.0
#: User-Agent по умолчанию (описательный; можно переопределить/отключить ``None``).
DEFAULT_USER_AGENT = "coHound-crawler/0.1"
#: Заголовки контент-согласования обычной HTML-страницы.
DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
DEFAULT_ACCEPT_LANGUAGE = "ru,en;q=0.7"


class CrawlUnavailable(CrawlError):
    """Лёгкий краулер недоступен: нет ``httpx`` и не внедрён HTTP-шов."""


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` живого фетча)."""
    return datetime.now(timezone.utc).isoformat()


class HttpCrawler:
    """Лёгкий HTTP-краулер (``httpx``) с SSRF-защитой и опциональным кэшем.

    ``version`` попадает в ``run_manifest``. ``fetch`` возвращает ``RawPage`` с
    сырыми байтами и финальным URL после редиректов. Нейтрален (§1.3).

    :param fetcher: тестовый шов ``(url) -> bytes``; ``None`` → реальный ``httpx``.
    :param http_get: алиас ``fetcher`` (совместимость с другими адаптерами). Задавать
        разрешено только один из двух.
    :param cache: глобальный content-addressed кэш (R7); ``None`` → без кэша.
    :param timeout: таймаут HTTP-запроса, с (>0).
    :param user_agent: значение заголовка ``User-Agent``; ``None`` → без заголовка.
    :param follow_redirects: следовать ли HTTP-редиректам (по умолчанию да).
    """

    version = "http-crawler-1"
    #: Duck-typed имя транспорта (провенанс краула, Phase 2.1/S1): фактически
    #: применённый движок попадает в ``crawl_session_items.engine``/манифест.
    engine = "http"
    #: Лёгкий HTTP-краулер не умеет браузерный рендер (D2): pipeline пропускает
    #: ``fetch: browser``-URL для такого краулера с пофURL-ошибкой.
    supports_browser = False

    def __init__(
        self,
        fetcher: HttpGetFn | None = None,
        *,
        http_get: HttpGetFn | None = None,
        cache: ContentAddressedCache | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        user_agent: str | None = DEFAULT_USER_AGENT,
        follow_redirects: bool = True,
        robots: RobotsGate | None = None,
    ) -> None:
        if fetcher is not None and http_get is not None:
            raise ValueError("задайте только один шов: fetcher или http_get")
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        self._fetcher = fetcher if fetcher is not None else http_get
        self._cache = cache
        self.timeout = timeout
        self.user_agent = user_agent
        self.follow_redirects = follow_redirects
        # D1/§10: per-host политика robots.txt (egress-слой). ``None`` → без проверки
        # (обратная совместимость); тесты инъектят политику без сети.
        self._robots = robots

    @property
    def available(self) -> bool:
        """Готов ли краулер: внедрённый шов или установленный ``httpx``."""
        return self._fetcher is not None or _httpx_available()

    # -- публичный контракт ---------------------------------------------------

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """Загружает ``url``: проверка плана → SSRF → валидаторы → кэш/живой фетч → кэш.

        Валидаторы берутся из плана, иначе — из сайдкара кэша (см. модульный докстринг):
        при наличии любых валидаторов запрос уходит на сервер (возможен ``304``).
        ``FetchMode.BROWSER`` этим адаптером не поддерживается (понятная ошибка).
        """
        if plan.fetch is FetchMode.BROWSER:
            raise CrawlError(
                "HttpCrawler не поддерживает browser-рендер; "
                "используйте ScrapyCrawler/playwright"
            )
        try:
            validate_url(url)
        except ValueError as exc:
            raise CrawlUrlError(f"URL отклонён SSRF-валидацией: {exc}") from exc

        # D1/§10: robots.txt-запрет — отказ (не сбой). Персистентный отказ не латаем
        # кэшем: проверяем до обращения к кэшу (кэш мог быть добыт до появления правил).
        # Гейт **условен**: проверяем только когда политика внедрена И ``plan.robots``
        # (политика приходит с ядра; при выключенном robots лишних fetch нет).
        if self._robots is not None and plan.robots:
            try:
                allowed = self._robots.allowed(url)
            except Exception:  # noqa: BLE001 — сбой robots-проверки не роняет краул
                allowed = True
            if not allowed:
                raise CrawlRefused(
                    f"robots.txt запрещает краул {url!r}",
                    refusal_reason="robots_disallow",
                )

        key = http_key(url)
        meta = self._cache.get_meta("http", key) if self._cache is not None else None
        # Приоритет валидаторов: план → мета кэша → безусловный GET. Валидаторы плана —
        # явное намерение вызывающего (их же прокидывает шов DMZ). Иначе адаптер сам
        # достаёт валидаторы прошлого получения из сайдкара и делает **живой** условный
        # запрос: без этого недельный цикл всегда получал бы полный ``200`` и экономия
        # ``304`` была бы нулевой. План вызывающего не мутируем.
        effective_plan = plan
        if not (plan.if_none_match or plan.if_modified_since) and meta is not None:
            cached_etag = meta.get("etag")
            cached_modified = meta.get("last_modified")
            if cached_etag or cached_modified:
                effective_plan = replace(
                    plan,
                    if_none_match=str(cached_etag) if cached_etag else None,
                    if_modified_since=str(cached_modified) if cached_modified else None,
                )
        # Условный запрос обязан дойти до сервера: кэш-шорткат его бы проглотил.
        conditional = bool(
            effective_plan.if_none_match or effective_plan.if_modified_since
        )
        if self._cache is not None and not conditional:
            cached = self._cache.get_bytes("http", key)
            if cached is not None:
                return self._page_from_cache(url, cached, meta)

        page = self._fetch_live(url, effective_plan)
        if self._cache is not None and not page.not_modified:
            self._cache.put_bytes("http", key, page.content)
            self._cache.put_meta(
                "http",
                key,
                {
                    "etag": page.etag,
                    "last_modified": page.last_modified,
                    "status_code": page.status_code,
                    "content_type": page.content_type,
                    "final_url": page.url,
                },
            )
        return page

    # -- фетч -----------------------------------------------------------------

    def _fetch_live(self, url: str, plan: FetchPlan) -> RawPage:
        """Живой фетч: внедрённый шов или реальный ``httpx``."""
        if self._fetcher is not None:
            try:
                content = bytes(self._fetcher(url))
            except CrawlError:
                raise
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise CrawlError(f"фетч {url!r} не удался: {exc}") from exc
            return RawPage(
                url=url,
                content=content,
                fetched_at=_now_iso(),
                fetch_mode=FetchMode.HTTP,
            )
        return self._get_real(url, plan)

    def _get_real(self, url: str, plan: FetchPlan) -> RawPage:
        """Реальный GET; ``304`` — не ошибка, а подтверждение кэша (см. ниже)."""
        httpx = self._import_http()
        try:
            response = httpx.get(
                url,
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=self._headers(plan),
            )
            if int(response.status_code) == 304:
                return self._page_not_modified(url, response)
            # 404/410 — определённый ответ «ресурса нет», а не недоступность источника.
            # Классифицируем ДО ``raise_for_status``: иначе статус схлопнулся бы в
            # бесцветный ``CrawlError`` и pipeline записал бы ``source_unavailable``.
            status = int(response.status_code)
            if status in NOT_FOUND_STATUSES:
                raise CrawlNotFound(
                    f"HTTP {status} для {url!r}: ресурс отсутствует",
                )
            response.raise_for_status()
        except CrawlError:
            raise
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise CrawlError(f"HTTP-фетч {url!r} не удался: {exc}") from exc
        return self._page_from_response(response)

    def _headers(self, plan: FetchPlan | None = None) -> dict[str, str]:
        """Заголовки запроса: ``Accept``/``Accept-Language``, ``User-Agent`` и валидаторы.

        ``If-None-Match``/``If-Modified-Since`` добавляются только когда план несёт
        валидаторы предыдущего получения; иначе набор байт-идентичен прежнему.
        """
        headers = {
            "Accept": DEFAULT_ACCEPT,
            "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
        }
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        if plan is not None:
            if plan.if_none_match:
                headers["If-None-Match"] = plan.if_none_match
            if plan.if_modified_since:
                headers["If-Modified-Since"] = plan.if_modified_since
        return headers

    @staticmethod
    def _page_from_response(response: Any) -> RawPage:
        """Отображает объект ответа ``httpx`` в ``RawPage`` (финальный URL, статус, тип).

        Заголовки ``ETag``/``Last-Modified`` сохраняются как валидаторы для следующего
        условного запроса.
        """
        raw_type = response.headers.get("Content-Type")
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        return RawPage(
            url=str(response.url),
            content=response.content,
            fetched_at=_now_iso(),
            fetch_mode=FetchMode.HTTP,
            status_code=response.status_code,
            content_type=raw_type or None,
            etag=etag or None,
            last_modified=last_modified or None,
        )

    def _page_not_modified(self, url: str, response: Any) -> RawPage:
        """``304``: сервер подтвердил актуальность кэша — отдаём **закэшированные** байты.

        Пустое тело здесь было бы ложью: ``304`` не несёт представления. Если кэш не
        внедрён или байтов нет, условный запрос был неконсистентен — это явная ошибка
        (``CrawlError``), а не пустая страница.
        """
        if self._cache is None:
            raise CrawlError(
                f"HTTP 304 для {url!r}, но кэш не внедрён: нет байтов представления"
            )
        key = http_key(url)
        cached = self._cache.get_bytes("http", key)
        if cached is None:
            raise CrawlError(
                f"HTTP 304 для {url!r}, но в кэше нет байтов представления "
                "(неконсистентный условный запрос)"
            )
        meta = self._cache.get_meta("http", key) or {}
        raw_type = response.headers.get("Content-Type") or meta.get("content_type")
        etag = response.headers.get("ETag") or meta.get("etag")
        last_modified = response.headers.get("Last-Modified") or meta.get("last_modified")
        return RawPage(
            url=str(response.url) or url,
            content=cached,
            fetched_at=_now_iso(),
            fetch_mode=FetchMode.HTTP,
            status_code=304,
            content_type=raw_type or None,
            etag=etag or None,
            last_modified=last_modified or None,
            not_modified=True,
        )

    # -- кэш ------------------------------------------------------------------

    def _page_from_cache(
        self, url: str, content: bytes, meta: dict[str, Any] | None = None
    ) -> RawPage:
        """Собирает ``RawPage`` из кэшированных байтов.

        Без сайдкара (легаси-запись) поведение прежнее: ``status_code``/``content_type``
        пусты, ``url`` — запрошенный, ``fetched_at`` — mtime файла кэша. При наличии
        сайдкара восстанавливаются статус/тип/финальный URL и валидаторы (``etag``/
        ``last_modified``), чтобы вызывающий мог построить условный запрос следующего
        цикла. ``not_modified`` всегда ``False``: это кэш-хит, а не подтверждение ``304``.
        """
        meta = meta or {}
        status = meta.get("status_code")
        content_type = meta.get("content_type")
        etag = meta.get("etag")
        last_modified = meta.get("last_modified")
        final_url = meta.get("final_url")
        return RawPage(
            url=str(final_url) if final_url else url,
            content=content,
            fetched_at=self._cache_mtime_iso(url),
            fetch_mode=FetchMode.HTTP,
            status_code=int(status) if isinstance(status, int) else None,
            content_type=str(content_type) if content_type else None,
            etag=str(etag) if etag else None,
            last_modified=str(last_modified) if last_modified else None,
        )

    def _cache_mtime_iso(self, url: str) -> str:
        """ISO-8601 (UTC) mtime файла кэша; при отсутствии — текущее время."""
        assert self._cache is not None
        try:
            path = self._cache.path_for("http", http_key(url), ".bin")
            stamp = path.stat().st_mtime
        except (OSError, ValueError):
            return _now_iso()
        return datetime.fromtimestamp(stamp, tz=timezone.utc).isoformat()

    # -- ленивый импорт -------------------------------------------------------

    @staticmethod
    def _import_http() -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — ``CrawlUnavailable``."""
        if not _httpx_available():
            raise CrawlUnavailable(
                "httpx не установлен: реальный HTTP-фетч недоступен. "
                "Установите extra 'crawl' (05) или внедрите fetcher/http_get."
            )
        import httpx  # type: ignore[import-not-found]

        return httpx
