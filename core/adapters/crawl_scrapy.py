"""Краулер на Scrapy — реализация ``CrawlerPort`` (W7/T7.1, Ш5).

Адаптер загружает страницы и возвращает **сырые байты** в ``RawPage`` (retention,
I19; 02 §B.1). Ответственность адаптера (01 §3.2, ``ports/crawler.py``):

* **robots=obey** — реальный путь явно выставляет ``ROBOTSTXT_OBEY = True``
  (дефолт Scrapy при встраивании как библиотеки — ``False``, 05 §2.5);
* **per-domain рендер-профили** — домен берётся из host URL, профиль задаёт
  режим (HTTP или браузерный рендер JS) и опциональное ожидание JS;
* **playwright per-request** — при браузерном режиме запрос идёт с ``playwright``-
  meta (снимок после готовности JS, V5-оговорки 05 §2.5);
* **retries/backoff** — через настройки Scrapy (``RETRY_TIMES``/``DOWNLOAD_DELAY``);
* **content-addressed кэш** — перед живым фетчем консультируется глобальный кэш
  (``core.util.cache``, ADR 0016): попадание → ``RawPage`` из байтов, промах →
  фетч + ``put_bytes("http", http_key(url), content)`` (R7 между ``run_id``).

**Ленивые импорты.** ``scrapy``/``scrapy-playwright``/``playwright`` — тяжёлые и
необязательные: модуль импортируется без них, импорт происходит только в реальном
пути. ``available`` показывает готовность.

**Шов для тестов.** Конструктор принимает ``fetcher`` — callable
``(url, render) -> RawPage`` или объект с методом ``fetch(url, render)``. Если он
задан, реальный Scrapy-путь не задействуется, и юнит-тесты не запускают
Twisted-реактор и не требуют браузеров. Если ``fetcher is None`` — используется
настоящий Scrapy.

**Ошибки.** Сбой фетча поднимает ``CrawlError`` (pipeline изолирует по документу и
отображает на ``source_unavailable``). Отклонённый SSRF-валидацией URL поднимает
``CrawlUrlError`` (подкласс и ``CrawlError``, и ``ValueError``).

Нейтрален (§1.3): в модуле нет названий продуктов/вендоров; профили — данные.
"""

from __future__ import annotations

import importlib.util
import re
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable
from urllib.parse import urlsplit

from core.ports.types import RawPage, RenderMode
from core.util.cache import ContentAddressedCache, http_key
from core.util.security import validate_url

__all__ = [
    "CrawlError",
    "CrawlUrlError",
    "DEFAULT_SCRAPY_SETTINGS",
    "ScrapyCrawler",
]

# Внедряемый фетчер: callable ``(url, render) -> RawPage``. Объект с методом
# ``fetch(url, render)`` также принимается (см. ``PageFetcher``).
FetchFn = Callable[[str, RenderMode], RawPage]

# Настройки Scrapy для реального пути (05 §2.5). Пользователь может переопределить
# их через ``scrapy_settings`` — его значения имеют приоритет.
DEFAULT_SCRAPY_SETTINGS: dict[str, Any] = {
    # Этика краула (§10): дефолт библиотеки — False, поэтому выставляем явно.
    "ROBOTSTXT_OBEY": True,
    # Повторы/backoff на уровне загрузчика (05 §2.5).
    "RETRY_TIMES": 2,
    "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
    "DOWNLOAD_DELAY": 0.5,
    "DOWNLOAD_TIMEOUT": 30,
    "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    # asyncio-реактор обязателен для scrapy-playwright при concurrency>1 (V5).
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "LOG_LEVEL": "ERROR",
}

# Настройки, добавляемые только для браузерного рендера (scrapy-playwright).
_PLAYWRIGHT_SETTINGS: dict[str, Any] = {
    "DOWNLOAD_HANDLERS": {
        "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    },
    "PLAYWRIGHT_BROWSER_TYPE": "chromium",
    "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 15000,
}

_RENDER_VALUES = {mode.value: mode for mode in RenderMode}


class CrawlError(Exception):
    """Базовая ошибка краулера. Pipeline отображает её на ``source_unavailable``."""


class CrawlUrlError(CrawlError, ValueError):
    """URL отклонён SSRF-валидацией (небезопасная/некорректная цель).

    Множественное наследование намеренно: вызывающий может ловить и как
    ``CrawlError``, и как ``ValueError`` (``validate_url`` поднимает ``ValueError``).
    """


@runtime_checkable
class PageFetcher(Protocol):
    """Минимальный контракт внедряемого фетчера (альтернатива callable)."""

    def fetch(self, url: str, render: RenderMode = RenderMode.HTTP) -> RawPage: ...


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` при живом фетче)."""
    return datetime.now(timezone.utc).isoformat()


def _scrapy_available() -> bool:
    """Установлен ли Scrapy (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec("scrapy") is not None


def _normalize_render(value: Any) -> RenderMode | None:
    """Приводит значение профиля к ``RenderMode`` или ``None`` (неизвестно)."""
    if isinstance(value, RenderMode):
        return value
    if isinstance(value, str):
        return _RENDER_VALUES.get(value.strip().lower())
    return None


def _profile_render(profile: Any) -> RenderMode | None:
    """Извлекает режим рендера из профиля домена.

    Поддерживаемые формы профиля (слабая схема ``load_domain_profiles``, D.2):

    * строка: ``"http"`` | ``"browser"``;
    * словарь с ``render: "http"|"browser"``;
    * словарь с ``render: {mode: "http"|"browser", ...}`` (вложенная форма).

    Неизвестное/пустое значение → ``None`` (режим остаётся за аргументом ``fetch``).
    """
    if isinstance(profile, str):
        return _normalize_render(profile)
    if isinstance(profile, Mapping):
        raw = profile.get("render")
        direct = _normalize_render(raw)
        if direct is not None:
            return direct
        if isinstance(raw, Mapping):
            return _normalize_render(raw.get("mode"))
    return None


def _profile_wait_for(profile: Any) -> str | None:
    """JS-условие ожидания из профиля (``wait_for``) либо ``None``.

    Это выражение передаётся в ``PageMethod("wait_for_function", ...)``, чтобы
    снимок брался после готовности отложенного JS (оговорка V5).
    """
    if isinstance(profile, Mapping):
        value = profile.get("wait_for")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


class ScrapyCrawler:
    """Краулер на Scrapy (HTTP + per-request playwright) с глобальным кэшем.

    ``version`` — версия адаптера (попадает в ``run_manifest``). ``fetch``
    возвращает ``RawPage`` с сырыми байтами. Нейтрален (§1.3).
    """

    version = "scrapy-crawler-1"

    def __init__(
        self,
        fetcher: FetchFn | PageFetcher | None = None,
        *,
        cache: ContentAddressedCache | None = None,
        domain_profiles: Mapping[str, Any] | None = None,
        scrapy_settings: Mapping[str, Any] | None = None,
    ) -> None:
        """``fetcher`` — тестовый шов; ``cache`` — глобальный кэш; ``domain_profiles`` —
        per-domain рендер-профили (из ``load_domain_profiles``); ``scrapy_settings`` —
        переопределения настроек реального пути.
        """
        self._fetcher = fetcher
        self._cache = cache
        self._profiles = {
            str(domain).strip().lower(): profile
            for domain, profile in (domain_profiles or {}).items()
            if str(domain).strip()
        }
        self._scrapy_settings = dict(scrapy_settings or {})

    @property
    def available(self) -> bool:
        """Готов ли краулер: внедрённый фетчер или установленный Scrapy."""
        return self._fetcher is not None or _scrapy_available()

    # -- выбор режима ---------------------------------------------------------

    def render_for(self, url: str, default: RenderMode = RenderMode.HTTP) -> RenderMode:
        """Режим рендера для URL: профиль домена (в т.ч. родительского) или ``default``.

        Домен читается из host URL (регистронезависимо). Сначала ищется точное
        совпадение, затем — совпадение по суффиксу домена (профиль ``example.com``
        применяется к ``www.example.com``; ключи ``*.example.com``/``.example.com``
        нормализуются к ``example.com``).
        """
        host = (urlsplit(url).hostname or "").strip().lower()
        if not host:
            return default
        for candidate in self._profile_candidates(host):
            profile = self._profiles.get(candidate)
            if profile is None:
                continue
            mode = _profile_render(profile)
            if mode is not None:
                return mode
        return default

    def _profile_candidates(self, host: str) -> list[str]:
        """Кандидаты-домены для поиска профиля: сам host + его родительские суффиксы."""
        labels = host.split(".")
        return [".".join(labels[i:]) for i in range(len(labels))]

    def _profile_for(self, url: str) -> Any:
        """Профиль домена URL (точное/суффиксное совпадение) либо ``None``."""
        host = (urlsplit(url).hostname or "").strip().lower()
        if not host:
            return None
        for candidate in self._profile_candidates(host):
            if candidate in self._profiles:
                return self._profiles[candidate]
        return None

    # -- публичный контракт ---------------------------------------------------

    def fetch(self, url: str, render: RenderMode = RenderMode.HTTP) -> RawPage:
        """Загружает ``url``; ``render`` — дефолт, переопределяемый профилем домена.

        Порядок: SSRF-валидация → выбор режима → кэш → живой фетч → запись в кэш.
        """
        try:
            validate_url(url)
        except ValueError as exc:
            raise CrawlUrlError(f"URL отклонён SSRF-валидацией: {exc}") from exc

        effective = self.render_for(url, render)
        key = http_key(url)

        if self._cache is not None:
            cached = self._cache.get_bytes("http", key)
            if cached is not None:
                return self._page_from_cache(url, cached, effective)

        page = self._fetch_live(url, effective)
        self._store(url, key, page.content)
        return page

    # -- кэш ------------------------------------------------------------------

    def _page_from_cache(self, url: str, content: bytes, render: RenderMode) -> RawPage:
        """Собирает ``RawPage`` из кэшированных байтов.

        Кэшируются только байты, поэтому ``status_code``/``content_type`` в
        попадании пусты, а ``fetched_at`` берётся из mtime файла кэша (момент
        первого фетча) — это сохраняет детерминизм R7 между ``run_id``.
        """
        return RawPage(
            url=url,
            content=content,
            fetched_at=self._cache_mtime_iso(url),
            render_mode=render,
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

    def _store(self, url: str, key: str, content: bytes) -> None:
        """Пишет байты в глобальный кэш (R7), если кэш внедрён."""
        if self._cache is None:
            return
        self._cache.put_bytes("http", key, content)

    # -- фетч -----------------------------------------------------------------

    def _fetch_live(self, url: str, effective: RenderMode) -> RawPage:
        """Живой фетч: внедрённый ``fetcher`` или реальный Scrapy-путь."""
        if self._fetcher is not None:
            try:
                return self._call_fetcher(self._fetcher, url, effective)
            except CrawlError:
                raise
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise CrawlError(f"фетч {url!r} не удался: {exc}") from exc
        return self._fetch_with_scrapy(url, effective)

    @staticmethod
    def _call_fetcher(fetcher: FetchFn | PageFetcher, url: str, render: RenderMode) -> RawPage:
        """Вызывает внедрённый фетчер: объект с ``fetch`` или callable."""
        fetch_attr = getattr(fetcher, "fetch", None)
        if callable(fetch_attr):
            return cast(RawPage, fetch_attr(url, render))
        if callable(fetcher):
            return cast(RawPage, fetcher(url, render))  # type: ignore[operator]
        raise CrawlError("внедрённый fetcher не callable и не имеет метода fetch")

    def _fetch_with_scrapy(self, url: str, render: RenderMode) -> RawPage:
        """Реальный путь: один Scrapy-краул на вызов ``fetch``.

        Требует ``scrapy`` (и ``scrapy-playwright`` для браузерного режима). Запуск
        реактора — ответственность Scrapy (``CrawlerProcess``); реактор Twisted
        одноразовый, поэтому в проде адаптер живёт в короткоживущем процессе/DMZ-
        сервисе (07). Здесь путь не покрывается юнит-тестами: сеть и браузеры — в
        smoke (``tests/smoke_scrapy_playwright.py``, V5).
        """
        if not _scrapy_available():
            raise CrawlError(
                "Scrapy не установлен: установите extra 'crawl' "
                "(scrapy/scrapy-playwright/playwright) или внедрите fetcher"
            )
        try:
            from scrapy import Request, Spider  # type: ignore[import-not-found]
            from scrapy.crawler import CrawlerProcess  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - зависит от окружения
            raise CrawlError(f"не удалось импортировать Scrapy: {exc}") from exc

        wait_for = _profile_wait_for(self._profile_for(url))
        settings = self._build_settings(render)
        collected: list[RawPage] = []

        class _OneShotSpider(Spider):  # type: ignore[misc, valid-type]
            name = "cohound-fetch"

            async def start(self):  # noqa: ANN201 - сигнатура Scrapy 2.19
                meta: dict[str, Any] = {}
                if render is RenderMode.BROWSER:
                    from scrapy_playwright.page import (  # type: ignore[import-not-found]
                        PageMethod,
                    )

                    methods = []
                    if wait_for:
                        methods.append(
                            PageMethod("wait_for_function", wait_for, timeout=15000)
                        )
                    meta = {
                        "playwright": True,
                        # Page не удерживаем: иначе без закрытия краул повисает (V5).
                        "playwright_include_page": False,
                        "playwright_page_methods": methods,
                    }
                # dont_filter=True: один и тот же URL в разных fetch не дедуплицируется.
                yield Request(url, callback=self._collect, dont_filter=True, meta=meta)

            def _collect(self, response) -> None:  # noqa: ANN001 - объект Scrapy
                raw_type = response.headers.get(b"Content-Type", b"")
                content_type = raw_type.decode("latin-1") if raw_type else None
                collected.append(
                    RawPage(
                        url=response.url,
                        content=response.body,
                        fetched_at=_now_iso(),
                        render_mode=render,
                        status_code=response.status,
                        content_type=content_type or None,
                    )
                )

        process = CrawlerProcess(settings=settings)
        process.crawl(_OneShotSpider)
        try:
            process.start()
        except CrawlError:
            raise
        except Exception as exc:  # noqa: BLE001 - любой сбой движка → CrawlError
            raise CrawlError(f"Scrapy-краул {url!r} не удался: {exc}") from exc

        if not collected:
            raise CrawlError(f"Scrapy не вернул контент для {url!r}")
        return collected[0]

    def _build_settings(self, render: RenderMode) -> dict[str, Any]:
        """Итоговые настройки Scrapy: дефолты → playwright (для browser) → overrides."""
        settings = dict(DEFAULT_SCRAPY_SETTINGS)
        if render is RenderMode.BROWSER:
            settings.update(_PLAYWRIGHT_SETTINGS)
        settings.update(self._scrapy_settings)
        return settings
