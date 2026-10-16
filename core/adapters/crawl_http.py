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
  ``run_id``); промах → живой фетч + запись.

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
``CrawlUnavailable(CrawlError)``. Отказ SSRF — ``CrawlUrlError``.

Нейтрален (§1.3): в модуле нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from core.adapters.crawl_scrapy import CrawlError, CrawlRefused, CrawlUrlError
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
        """Загружает ``url``: проверка плана → SSRF → кэш → живой фетч → кэш.

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
        if self._robots is not None:
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
        if self._cache is not None:
            cached = self._cache.get_bytes("http", key)
            if cached is not None:
                return self._page_from_cache(url, cached)

        page = self._fetch_live(url)
        if self._cache is not None:
            self._cache.put_bytes("http", key, page.content)
        return page

    # -- фетч -----------------------------------------------------------------

    def _fetch_live(self, url: str) -> RawPage:
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
        return self._get_real(url)

    def _get_real(self, url: str) -> RawPage:
        """Реальный GET; отсутствие зависимости → ``CrawlUnavailable``."""
        httpx = self._import_http()
        try:
            response = httpx.get(
                url,
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=self._headers(),
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise CrawlError(f"HTTP-фетч {url!r} не удался: {exc}") from exc
        return self._page_from_response(response)

    def _headers(self) -> dict[str, str]:
        """Заголовки запроса: ``Accept``/``Accept-Language`` и опциональный ``User-Agent``."""
        headers = {
            "Accept": DEFAULT_ACCEPT,
            "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
        }
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        return headers

    @staticmethod
    def _page_from_response(response: Any) -> RawPage:
        """Отображает объект ответа ``httpx`` в ``RawPage`` (финальный URL, статус, тип)."""
        raw_type = response.headers.get("Content-Type")
        return RawPage(
            url=str(response.url),
            content=response.content,
            fetched_at=_now_iso(),
            fetch_mode=FetchMode.HTTP,
            status_code=response.status_code,
            content_type=raw_type or None,
        )

    # -- кэш ------------------------------------------------------------------

    def _page_from_cache(self, url: str, content: bytes) -> RawPage:
        """Собирает ``RawPage`` из кэшированных байтов.

        Кэшируются только байты, поэтому ``status_code``/``content_type`` в попадании
        пусты, а ``fetched_at`` берётся из mtime файла кэша (момент первого фетча) —
        это сохраняет детерминизм R7 между ``run_id``.
        """
        return RawPage(
            url=url,
            content=content,
            fetched_at=self._cache_mtime_iso(url),
            fetch_mode=FetchMode.HTTP,
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
