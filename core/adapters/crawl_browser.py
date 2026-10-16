"""Браузерный краулер на Playwright — реализация ``CrawlerPort`` (Ш5, D2/D4).

Прямой Playwright (sync API), **без Scrapy/Twisted**: адаптер сам управляет браузером
и возвращает сырые байты в ``RawPage`` (retention, I19). Роль адаптера —
``ports/crawler.py``: план получения (``FetchPlan``) резолвится в pipeline и передаётся
готовым; адаптер лишь исполняет его.

Оси ``FetchPlan`` (D2/D4):

* ``fetch`` — «нужен ли браузер вообще» (ось выбора адаптера; composition root
  маршрутизирует её в :class:`~core.adapters.crawl_http.HttpCrawler` или сюда). Этот
  адаптер **диспетчеризуется по** ``strategy``, а ``fetch`` не влияет на поведение:
  если сюда пришёл ``fetch=HTTP`` (pipeline нормализует такую пару в ``strategy=AUTO``),
  адаптер выполняет honest-рендер браузером. Это осознанно и не является молчаливой
  подменой: контракт адаптера — браузерный рендер; истинно лёгкий HTTP — область
  :class:`~core.adapters.crawl_http.HttpCrawler`.
* ``strategy`` (:class:`~core.domain.enums.CrawlStrategy`) — «как используется браузер»:

  - ``AUTO``/``honest`` — **честный** рендер: открыть сессию → ``navigate(url, wait_for)``
    → ``RawPage(fetch_mode=BROWSER)``. Возвращаемые байты — **DOM-сериализация браузера
    (транскодирование)**, а не байт-в-байт ответ сервера (I19: это сырьё для retention).
    Живой рендер **не кэшируется** (R7).
  - ``hybrid`` — прогрев браузером (``navigate`` + ``cookies``) → лёгкий HTTP-GET тем же
    URL с зеркалированием ``User-Agent``/``Accept-Language`` и **cookie-моста** (все cookie
    хоста из сессии, включая HttpOnly). **Успех ⇔** ``200 <= status < 300`` **и** ответ не
    похож на interstitial (см. ниже) → ``RawPage(fetch_mode=HTTP, status_code, content_type)``
    и **запись в кэш** по ``http_key(url)`` (кэшируется только тело). Любой иной исход
    (challenge/не-2xx/interstitial) **не кэшируется**: ограниченный повтор с ре-прогревом и
    jittered-backoff, а при исчерпании — возврат **уже полученного браузерного рендера** (без
    лишнего честного фетча). **Сбой транспорта — fallback на honest** (живой рендер).

**Запуск браузера и automation-сигнал.** Антибот-класс публичных сайтов требует **headed**
браузера (``headless=False``; headless распознаётся отдельно) и снятия артефакта автоматизации:
Playwright при ``launch``/``launch_persistent_context`` добавляет ``--enable-automation``, из-за
чего ``navigator.webdriver === true`` и страница отдаётся как interstitial (401) **без запуска
штатного JS-челленджа**. Поэтому собственный persistent-context запускается с
``ignore_default_args=["--enable-automation"]`` **и**
``args=["--disable-blink-features=AutomationControlled"]`` (в одиночку недостаточно; см.
``_LAUNCH_ARGS``/``_IGNORE_DEFAULT_ARGS``). CDP-ветка (``connect_over_cdp``) не затрагивается —
браузер запускает оператор. Это исполнение механизма самой публичной страницы, а не обход (§10).

**Детекция challenge — родовая.** Никаких вендорных строк/префиксов (§1.3/A2); специфика
сайтов — данные в ``packages/``. Два сигнала:

1. **по статусу** (``CHALLENGE_STATUSES = {401, 403, 429}``) — очевидный невыполненный челлендж;
2. **по несоответствию объёма** — ``200``-interstitial (JS-заглушка) ловится сравнением
   HTTP-тела с браузерным рендером (ground truth из прогрева): если тело много короче
   рендера, это похоже на заглушку (:func:`_looks_like_interstitial`). Гейт **консервативен**:
   при сомнении предпочитаем честный рендер (A1 — корректность важнее «экономии» hybrid;
   ложное срабатывание не портит кэш, максимум теряем экономию).

**Швы для тестов** (:class:`BrowserBackend`/:class:`BrowserSession`). Юнит-тесты не
запускают реальный браузер и не ходят в сеть: внедряется фейковый бэкенд и/или
``http_get``.

**Ленивый Playwright.** Модуль импортируется без ``playwright``; пакет подтягивается
только в реальном пути. ``available`` — внедрён бэкенд ИЛИ ``playwright`` установлен.

**Пейсинг — ровно один источник.** Централизованный пейсинг живёт в corpus (полоса P2-3);
поле ``rate_limiter`` — **опциональный хук** для прямого/DMZ-использования, по умолчанию
``None`` (двойного сна нет): если задан, перед фетчем вызывается
``wait(host, plan.rate_limit)``.

**SSRF.** ``validate_url`` проверяет исходный URL до фетча. Hybrid-HTTP идёт с
``follow_redirects=False`` и ручным redirect-loop: каждый ``Location`` резолвится и
повторно валидируется (приватный/metadata-хост в редиректе → ``CrawlUrlError``).

**Ошибки.** ``CrawlError`` — базовая (переиспользована из ``crawl_scrapy``); ``CrawlUrlError``
— отказ SSRF; ``CrawlUnavailable`` (локально, как в ``crawl_http``) — нет ``playwright``/
``httpx`` или не поднялись браузерные бинари.

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import importlib.util
import shutil
import tempfile
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import urljoin, urlsplit

from core.adapters.crawl_scrapy import CrawlError, CrawlRefused, CrawlUrlError
from core.domain.enums import CrawlStrategy
from core.domain.models import RateLimitSpec
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.cache import ContentAddressedCache, http_key
from core.util.rate_limit import RateLimiter, jittered_backoff
from core.util.robots import RobotsGate
from core.util.security import validate_url

__all__ = [
    "BrowserCrawler",
    "BrowserBackend",
    "BrowserSession",
    "HttpGetFn",
    "CrawlUnavailable",
    "DEFAULT_TIMEOUT_MS",
    "DEFAULT_USER_AGENT",
    "DEFAULT_ACCEPT",
    "DEFAULT_ACCEPT_LANGUAGE",
    "HTTP_TIMEOUT_S",
    "CHALLENGE_STATUSES",
    "HYBRID_MAX_ATTEMPTS",
    "INTERSTITIAL_MIN_RENDER_BYTES",
    "INTERSTITIAL_LENGTH_RATIO",
]

#: Шов hybrid-HTTP: ``(url, headers, cookies) -> (status, body, resp_headers)``.
HttpGetFn = Callable[
    [str, "dict[str, str]", "list[dict[str, Any]]"],
    "tuple[int, bytes, dict[str, str]]",
]

#: Таймаут навигации браузера, мс.
DEFAULT_TIMEOUT_MS = 30000
#: User-Agent по умолчанию (описательный; ``None`` → без заголовка в hybrid-GET).
DEFAULT_USER_AGENT = "coHound-crawler/0.1"
#: Заголовки контент-согласования обычной HTML-страницы (зеркалятся в hybrid-GET).
DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
DEFAULT_ACCEPT_LANGUAGE = "ru,en;q=0.7"
#: Таймаут лёгкого HTTP-GET в hybrid-пути, с.
HTTP_TIMEOUT_S = 30.0

#: Статусы, трактуемые как невыполненный антибот-challenge (родовая детекция по статусу).
CHALLENGE_STATUSES = frozenset({401, 403, 429})
#: Верхняя граница попыток hybrid (первая + повторы с ре-прогревом).
HYBRID_MAX_ATTEMPTS = 3
#: Максимум HTTP-редиректов в ручном loop (SSRF-контроль на каждом хопе).
_MAX_REDIRECTS = 5
#: Статусы-редиректы (авто-следование выключено).
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})

#: Порог interstitial: браузерный рендер короче — сам непоказателен, гейт не применяется.
#: Безопасное направление: не гейтить при неинформативном рендере (A1).
INTERSTITIAL_MIN_RENDER_BYTES = 512
#: Порог interstitial: HTTP-тело короче ``ratio * len(rendered)`` ⇒ похоже на заглушку.
INTERSTITIAL_LENGTH_RATIO = 0.25

#: Флаги собственного запуска браузера (persistent-ветка; CDP-ветку не затрагивают).
#: Playwright по умолчанию добавляет ``--enable-automation`` → ``navigator.webdriver=true``,
#: по которому публичные антибот-системы отдают interstitial без челленджа. Нейтрализуем
#: артефакт автоматизации: ``--disable-blink-features=AutomationControlled`` (единственный
#: необходимый и достаточный по замерам). Это НЕ подделка: реальный JS-движок исполняет
#: штатный челлендж страницы как обычный браузер; §10 (только публичные сайты).
_LAUNCH_ARGS: tuple[str, ...] = ("--disable-blink-features=AutomationControlled",)
#: Убирает дефолтный ``--enable-automation`` (в одиночку не помогает — нужен вместе с
#: ``_LAUNCH_ARGS``; см. замеры в докстринге модуля).
_IGNORE_DEFAULT_ARGS: tuple[str, ...] = ("--enable-automation",)


class CrawlUnavailable(CrawlError):
    """Браузерный краулер недоступен: нет ``playwright``/``httpx`` или не поднялись бинари."""


class BrowserSession(Protocol):
    """Сессия браузера: рендер страницы и чтение cookie хоста (шов для тестов)."""

    def navigate(self, url: str, wait_for: str | None = None) -> bytes:
        """Открывает ``url`` (опционально ждёт JS-условие ``wait_for``) → байты рендера."""
        ...

    def cookies(self, url: str) -> list[dict[str, Any]]:
        """Cookie, применимые к ``url``: ``[{name, value, domain, path}, ...]``."""
        ...

    def close(self) -> None:
        """Закрывает сессию (страницу). Идемпотентно."""
        ...


class BrowserBackend(Protocol):
    """Драйвер браузера: выдаёт сессии и закрывает ресурсы (шов для тестов)."""

    def open(self) -> BrowserSession:
        """Открывает новую сессию (страницу) поверх общего контекста."""
        ...

    def close(self) -> None:
        """Закрывает контекст/браузер/движок. Идемпотентно."""
        ...


def _playwright_available() -> bool:
    """Установлен ли ``playwright`` (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec("playwright") is not None


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` живого фетча)."""
    return datetime.now(timezone.utc).isoformat()


def _looks_like_interstitial(http_body: bytes, rendered: bytes | None) -> bool:
    """Похоже ли HTTP-тело на ``200``-interstitial (JS-заглушку) вместо контента.

    Не-вендорный сигнал: сравнение объёма HTTP-ответа с браузерным рендером (ground
    truth из прогрева). Если рендер неинформативен (``None`` или короче
    ``INTERSTITIAL_MIN_RENDER_BYTES``), гейт **не применяется** (не гадаем). Пустое тело
    считается interstitial. Гейт консервативен — при сомнении предпочитаем честный рендер
    (A1); ложное срабатывание не портит кэш, максимум теряем «экономию» hybrid.
    """
    if not http_body:
        return True
    if rendered is None or len(rendered) < INTERSTITIAL_MIN_RENDER_BYTES:
        return False
    return len(http_body) < INTERSTITIAL_LENGTH_RATIO * len(rendered)


# ---------------------------------------------------------------------------
# Реальный backend Playwright (ленивая зависимость)
# ---------------------------------------------------------------------------


class _PlaywrightSession:
    """Сессия поверх страницы Playwright (context общий, страница — на сессию)."""

    def __init__(self, page: Any, context: Any, timeout_ms: int) -> None:
        self._page = page
        self._context = context
        self._timeout_ms = timeout_ms
        self._closed = False

    def navigate(self, url: str, wait_for: str | None = None) -> bytes:
        """``goto`` с ожиданием DOM; при ``wait_for`` — ожидание JS-условия → байты HTML.

        После навигации финальный URL повторно валидируется (SSRF через редирект
        браузера не обходит защиту).
        """
        if self._closed:
            raise CrawlError("браузерная сессия уже закрыта")
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=self._timeout_ms)
            final_url = str(getattr(self._page, "url", "") or url)
            try:
                validate_url(final_url)
            except ValueError as exc:
                raise CrawlUrlError(
                    f"навигация завершилась на небезопасном URL: {exc}"
                ) from exc
            if wait_for:
                # ``wait_for`` — JS-условие (совместимо с ``plan.wait_for``/crawl_scrapy).
                self._page.wait_for_function(wait_for, timeout=self._timeout_ms)
            return self._page.content().encode("utf-8")
        except CrawlError:
            raise
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise CrawlError(f"навигация {url!r} не удалась: {exc}") from exc

    def cookies(self, url: str) -> list[dict[str, Any]]:
        """Cookie контекста, применимые к ``url``; сбой чтения → пустой список."""
        if self._closed:
            return []
        try:
            return list(self._context.cookies(url))
        except Exception:  # noqa: BLE001 - cookie best-effort
            return []

    def close(self) -> None:
        """Закрывает страницу (контекст принадлежит backend). Идемпотентно."""
        if self._closed:
            return
        self._closed = True
        try:
            self._page.close()
        except Exception:  # noqa: BLE001 - best-effort
            pass


class _PlaywrightBackend:
    """Реальный драйвер Playwright: один контекст, новые страницы на сессию.

    Запускается лениво (при первом ``open``); взаимоисключающие пути: ``cdp_url`` →
    подключение к существующему браузеру (запуск — на операторе; automation-флаги не
    применяются), иначе persistent-context (``user_data_dir``; при отсутствии — временный
    профиль). Для антибот-класса нужен **headed** (``headless=False``, дефолт) и снятие
    automation-сигнала: persistent-запуск идёт с ``_IGNORE_DEFAULT_ARGS``/``_LAUNCH_ARGS``
    (см. докстринг модуля). Ошибка запуска (нет бинарей и т.п.) →
    :class:`CrawlUnavailable` с понятным текстом.
    """

    def __init__(
        self,
        *,
        cdp_url: str | None,
        user_data_dir: str | None,
        headless: bool,
        channel: str | None,
        timeout_ms: int,
    ) -> None:
        self._cdp_url = cdp_url
        self._user_data_dir = user_data_dir
        self._headless = headless
        self._channel = channel
        self._timeout_ms = timeout_ms
        self._playwright: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._temp_dir: str | None = None
        self._closed = False

    def open(self) -> BrowserSession:
        """Открывает новую страницу в общем контексте (контекст поднимается лениво)."""
        context = self._ensure_context()
        return _PlaywrightSession(context.new_page(), context, self._timeout_ms)

    def _ensure_context(self) -> Any:
        """Ленивый запуск движка и контекста; идемпотентно."""
        if self._context is not None:
            return self._context
        if not _playwright_available():
            raise CrawlUnavailable(
                "playwright не установлен: браузерный рендер недоступен. "
                "Установите extra 'crawl' (05) или внедрите backend."
            )
        try:
            from playwright.sync_api import (  # type: ignore[import-not-found]
                sync_playwright,
            )

            self._playwright = sync_playwright().start()
            chromium = self._playwright.chromium
            if self._cdp_url:
                # Взаимоисключимость: при CDP persistent-context не создаётся.
                self._browser = chromium.connect_over_cdp(self._cdp_url)
                contexts = list(getattr(self._browser, "contexts", []) or [])
                self._context = contexts[0] if contexts else self._browser.new_context()
            else:
                user_data_dir = self._user_data_dir
                if not user_data_dir:
                    self._temp_dir = tempfile.mkdtemp(prefix="cohound-browser-")
                    user_data_dir = self._temp_dir
                kwargs: dict[str, Any] = {
                    "user_data_dir": user_data_dir,
                    "headless": self._headless,
                    # Снятие automation-сигнала (см. ``_LAUNCH_ARGS``/докстринг модуля):
                    # без него публичные антибот-системы отдают interstitial без челленджа.
                    "ignore_default_args": list(_IGNORE_DEFAULT_ARGS),
                    "args": list(_LAUNCH_ARGS),
                }
                if self._channel:
                    kwargs["channel"] = self._channel
                self._context = chromium.launch_persistent_context(**kwargs)
        except CrawlError:
            raise
        except Exception as exc:  # noqa: BLE001 - нет бинарей/движка → понятная ошибка
            self._cleanup()
            raise CrawlUnavailable(
                f"не удалось запустить браузер Playwright: {exc} "
                f"(проверьте установку браузерных бинарей; в закрытом контуре — "
                f"PLAYWRIGHT_BROWSERS_PATH)"
            ) from exc
        return self._context

    def close(self) -> None:
        """Закрывает ресурсы best-effort; идемпотентно."""
        if self._closed:
            return
        self._closed = True
        self._cleanup()

    def _cleanup(self) -> None:
        """Best-effort освобождение контекста/браузера/движка и временного профиля."""
        for resource in (self._context, self._browser, self._playwright):
            if resource is None:
                continue
            closer = getattr(resource, "close", None) or getattr(resource, "stop", None)
            try:
                if callable(closer):
                    closer()
            except Exception:  # noqa: BLE001 - best-effort
                pass
        self._context = None
        self._browser = None
        self._playwright = None
        if self._temp_dir is not None:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            self._temp_dir = None


# ---------------------------------------------------------------------------
# Краулер
# ---------------------------------------------------------------------------


class BrowserCrawler:
    """Браузерный краулер на Playwright (honest/hybrid) с опциональным кэшем hybrid.

    ``version`` попадает в ``run_manifest``; ``supports_browser = True`` — pipeline
    пропускает этот краулер для ``fetch: browser``. Нейтрален (§1.3).

    :param backend: тестовый/внешний драйвер; ``None`` → реальный Playwright (лениво).
    :param http_get: шов hybrid-HTTP ``(url, headers, cookies) -> (status, body, headers)``;
        ``None`` → реальный ``httpx.get`` (при отсутствии ``httpx`` → ``CrawlUnavailable``).
    :param cache: content-addressed кэш (R7). Используется **только** hybrid-путём;
        honest-рендер всегда живой.
    :param cdp_url: подключение к существующему браузеру (CDP); взаимоисключим с профилем.
    :param user_data_dir: каталог persistent-профиля; ``None`` (без ``cdp_url``) → временный.
    :param headless: headless-режим браузера.
    :param channel: канал браузера (напр. конкретный дистрибутив); ``None`` → дефолт.
    :param timeout_ms: таймаут навигации, мс.
    :param user_agent: значение ``User-Agent`` (зеркалится в hybrid-GET); ``None`` → без него.
    :param rate_limiter: опциональный хук пейсинга. **Источник пейсинга ровно один** —
        централизованный в corpus (полоса P2-3); этот параметр — для прямого/DMZ-использования,
        по умолчанию ``None`` (двойного сна нет). Если задан, вызывается
        ``wait(host, plan.rate_limit)`` перед фетчем.
    """

    version = "browser-crawler-1"
    #: Duck-typed имя транспорта (провенанс краула, Phase 2.1/S1).
    engine = "browser"
    #: Браузерный краулер поддерживает ``fetch: browser`` (D2).
    supports_browser = True

    def __init__(
        self,
        *,
        backend: BrowserBackend | None = None,
        http_get: HttpGetFn | None = None,
        cache: ContentAddressedCache | None = None,
        cdp_url: str | None = None,
        user_data_dir: str | None = None,
        headless: bool = False,
        channel: str | None = None,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        user_agent: str | None = DEFAULT_USER_AGENT,
        rate_limiter: RateLimiter | None = None,
        robots: RobotsGate | None = None,
    ) -> None:
        self._backend = backend
        self._http_get = http_get
        self._cache = cache
        self._cdp_url = cdp_url
        self._user_data_dir = user_data_dir
        self._headless = headless
        self._channel = channel
        self._timeout_ms = timeout_ms
        self._user_agent = user_agent
        self._rate_limiter = rate_limiter
        # D1/§10: per-host политика robots.txt (egress-слой). ``None`` → без проверки;
        # тесты инъектят политику без сети.
        self._robots = robots
        self._closed = False

    @property
    def available(self) -> bool:
        """Готов ли краулер: внедрённый бэкенд или установленный ``playwright``."""
        return self._backend is not None or _playwright_available()

    # -- публичный контракт ---------------------------------------------------

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """Исполняет ``plan.strategy`` для ``url``: гард → SSRF → пейсинг → honest/hybrid.

        ``plan.fetch`` не влияет на диспетчер (см. докстринг модуля). hybrid при сбое
        транспорта откатывается на honest.
        """
        if self._closed:
            raise CrawlError("BrowserCrawler закрыт: fetch после close() запрещён")

        try:
            validate_url(url)
        except ValueError as exc:
            raise CrawlUrlError(f"URL отклонён SSRF-валидацией: {exc}") from exc

        # D1/§10: robots.txt-запрет — отказ (не сбой транспорта); элемент терминален, и
        # frontier от него не расширяется (обрабатывает pipeline).
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

        if self._rate_limiter is not None:
            host = (urlsplit(url).hostname or "").lower()
            # D1: ``Crawl-delay`` — только floor и только при отсутствии явного
            # ``rate_limit`` (явный лимит всегда важнее).
            delay_spec = plan.rate_limit
            if delay_spec is None and self._robots is not None:
                delay = self._robots.crawl_delay(url)
                if delay:
                    delay_spec = RateLimitSpec(min_s=delay, max_s=delay)
            self._rate_limiter.wait(host, delay_spec)

        if plan.strategy is CrawlStrategy.HYBRID:
            try:
                return self._fetch_hybrid(url, plan)
            except CrawlError:
                # Fallback: сбой транспорта hybrid → живой honest-рендер.
                return self._fetch_honest(url, plan)
        return self._fetch_honest(url, plan)

    def close(self) -> None:
        """Закрывает браузерный бэкенд best-effort; идемпотентно, безопасно без открытия."""
        if self._closed:
            return
        self._closed = True
        backend = self._backend
        if backend is not None:
            try:
                backend.close()
            except Exception:  # noqa: BLE001 - best-effort
                pass

    # -- honest ---------------------------------------------------------------

    def _fetch_honest(self, url: str, plan: FetchPlan) -> RawPage:
        """Честный браузерный рендер: ``navigate(url, wait_for)`` → ``RawPage(BROWSER)``.

        Без кэша. Байты — DOM-сериализация браузера (транскодирование), не байт-в-байт.
        """
        session = self._open_session()
        try:
            content = bytes(session.navigate(url, plan.wait_for))
        finally:
            session.close()
        return RawPage(
            url=url,
            content=content,
            fetched_at=_now_iso(),
            fetch_mode=FetchMode.BROWSER,
        )

    # -- hybrid ---------------------------------------------------------------

    def _fetch_hybrid(self, url: str, plan: FetchPlan) -> RawPage:
        """Прогрев → HTTP-GET с cookie-мостом; успех ⇔ 2xx и не-interstitial → кэш.

        Любой иной исход (challenge/не-2xx/interstitial) не кэшируется; при исчерпании
        повторов возвращается **уже полученный браузерный рендер**. Сбой транспорта
        (``CrawlError`` из :meth:`_http_get_hybrid`) пробрасывается — ``fetch`` уйдёт в honest.
        """
        key = http_key(url)
        if self._cache is not None:
            cached = self._cache.get_bytes("http", key)
            if cached is not None:
                return self._page_from_cache(url, cached)

        last_status: int | None = None
        last_rendered: bytes | None = None
        for attempt in range(HYBRID_MAX_ATTEMPTS):
            cookies, rendered = self._warm(url, plan)
            status, body, resp_headers = self._http_get_hybrid(url, cookies)
            if 200 <= status < 300 and not _looks_like_interstitial(body, rendered):
                page = RawPage(
                    url=url,
                    content=bytes(body),
                    fetched_at=_now_iso(),
                    fetch_mode=FetchMode.HTTP,
                    status_code=status,
                    content_type=resp_headers.get("Content-Type") or None,
                )
                if self._cache is not None:
                    self._cache.put_bytes("http", key, page.content)
                return page
            # Challenge / не-2xx / interstitial: сохраняем рендер для возможного возврата.
            last_status = status
            last_rendered = rendered
            if attempt < HYBRID_MAX_ATTEMPTS - 1:
                time.sleep(jittered_backoff(attempt))

        if last_rendered:
            # Переиспользуем уже полученный честный рендер (без лишнего фетча). Не кэшируется.
            return RawPage(
                url=url,
                content=bytes(last_rendered),
                fetched_at=_now_iso(),
                fetch_mode=FetchMode.BROWSER,
            )
        raise CrawlError(
            f"hybrid: честный рендер не дал байтов после {HYBRID_MAX_ATTEMPTS} попыток "
            f"(status={last_status})"
        )

    def _warm(self, url: str, plan: FetchPlan) -> tuple[list[dict[str, Any]], bytes]:
        """Прогревает сессию браузером: ``(cookie хоста, байты рендера)``.

        Рендер — эталон честного пути (используется гейтом interstitial и как fallback).
        """
        session = self._open_session()
        try:
            rendered = bytes(session.navigate(url, plan.wait_for))
            cookies = list(session.cookies(url))
        finally:
            session.close()
        return cookies, rendered

    def _http_get_hybrid(
        self, url: str, cookies: list[dict[str, Any]]
    ) -> tuple[int, bytes, dict[str, str]]:
        """Лёгкий HTTP-GET с зеркалированием заголовков и cookie-моста."""
        headers = self._headers()
        getter = self._http_get or self._default_http_get
        try:
            status, body, resp_headers = getter(url, headers, cookies)
        except CrawlError:
            raise
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise CrawlError(f"hybrid HTTP-GET {url!r} не удался: {exc}") from exc
        return int(status), bytes(body), dict(resp_headers or {})

    def _headers(self) -> dict[str, str]:
        """Зеркалируемые заголовки запроса (общие для hybrid-GET)."""
        headers = {
            "Accept": DEFAULT_ACCEPT,
            "Accept-Language": DEFAULT_ACCEPT_LANGUAGE,
        }
        if self._user_agent:
            headers["User-Agent"] = self._user_agent
        return headers

    def _default_http_get(
        self, url: str, headers: dict[str, str], cookies: list[dict[str, Any]]
    ) -> tuple[int, bytes, dict[str, str]]:
        """Реальный GET через ``httpx`` с cookie-jar и ручным redirect-loop.

        ``follow_redirects=False``: каждый ``Location`` резолвится и повторно валидируется
        (:func:`~core.util.security.validate_url`), чтобы редирект на приватный/metadata-хост
        не обошёл SSRF-защиту. При отказе — ``CrawlUrlError``.
        """
        httpx = self._import_http()
        jar = {
            str(cookie["name"]): str(cookie.get("value", ""))
            for cookie in cookies
            if cookie.get("name")
        }
        current = url
        for _ in range(_MAX_REDIRECTS + 1):
            try:
                response = httpx.get(
                    current,
                    headers=headers,
                    cookies=jar,
                    timeout=HTTP_TIMEOUT_S,
                    follow_redirects=False,
                )
            except CrawlError:
                raise
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise CrawlError(f"HTTP-фетч {current!r} не удался: {exc}") from exc
            location = response.headers.get("location")
            if response.status_code in _REDIRECT_STATUSES and location:
                current = urljoin(current, location)
                try:
                    validate_url(current)
                except ValueError as exc:
                    raise CrawlUrlError(
                        f"редирект на небезопасный URL отклонён SSRF-валидацией: {exc}"
                    ) from exc
                continue
            return response.status_code, response.content, dict(response.headers)
        raise CrawlError(f"превышено число редиректов ({_MAX_REDIRECTS}) при фетче {url!r}")

    # -- backend / сессии -----------------------------------------------------

    def _open_session(self) -> BrowserSession:
        """Открывает сессию через бэкенд (внедрённый или реальный)."""
        backend = self._ensure_backend()
        try:
            return backend.open()
        except CrawlError:
            raise
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise CrawlError(f"не удалось открыть браузерную сессию: {exc}") from exc

    def _ensure_backend(self) -> BrowserBackend:
        """Возвращает бэкенд, лениво строя реальный Playwright-драйвер при ``None``."""
        if self._backend is None:
            self._backend = _PlaywrightBackend(
                cdp_url=self._cdp_url,
                user_data_dir=self._user_data_dir,
                headless=self._headless,
                channel=self._channel,
                timeout_ms=self._timeout_ms,
            )
        return self._backend

    # -- кэш ------------------------------------------------------------------

    def _page_from_cache(self, url: str, content: bytes) -> RawPage:
        """``RawPage`` из закэшированных байтов (``status_code``/``content_type`` пусты)."""
        return RawPage(
            url=url,
            content=content,
            fetched_at=self._cache_mtime_iso(url),
            fetch_mode=FetchMode.HTTP,
        )

    def _cache_mtime_iso(self, url: str) -> str:
        """ISO-8601 (UTC) mtime файла кэша; при отсутствии — текущее время (R7)."""
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
                "httpx не установлен: hybrid-HTTP недоступен. Установите extra 'crawl' "
                "(05) или внедрите http_get."
            )
        import httpx  # type: ignore[import-not-found]

        return httpx
