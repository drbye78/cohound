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
  Кэш применяется **только** к ``RenderMode.HTTP``: браузерный рендер всегда
  идёт вживую и не пишется в кэш (детерминизм R7; см. ``fetch``).

**Один реактор на процесс.** Twisted-реактор одноразовый (``ReactorNotRestartable``),
поэтому ``CrawlerProcess.start()`` на каждый URL недопустим (``hound run`` делает
много ``fetch`` в одном процессе). Здесь реактор устанавливается **один раз** в
выделенном daemon-потоке (:class:`_ReactorWorker`), а каждый ``fetch`` лишь
мультиплексируется на него: свежий ``CrawlerRunner`` на submit (сбой одного краула
не отравляет следующий), результат — через ``concurrent.futures.Future``. Реактор
останавливается один раз при завершении процесса (``atexit``/``close``), но **не**
между фетчами.

**Почему ``CrawlerRunner``, а не ``CrawlerProcess``.** ``CrawlerProcess.start()``
ставит обработчики сигналов (нельзя вне главного потока) и владеет остановкой
реактора. ``CrawlerRunner`` работает поверх **уже запущенного** реактора. При этом
``CrawlerRunner`` не устанавливает реактор сам, поэтому настройка ``TWISTED_REACTOR``
в этом пути **инертна** — реактор уже установлен воркером (см. ``_default_install``);
полагаться на настройку не следует.

**Ленивые импорты.** ``scrapy``/``scrapy-playwright``/``playwright`` — тяжёлые и
необязательные: модуль импортируется без них, импорт происходит только в реальном
пути. ``available`` показывает готовность.

**Шов для тестов.** Конструктор принимает ``fetcher`` — callable
``(url, render) -> RawPage`` или объект с методом ``fetch(url, render)``. Если он
задан, реальный Scrapy-путь не задействуется, и юнит-тесты не запускают
Twisted-реактор и не требуют браузеров. Если ``fetcher is None`` — используется
настоящий Scrapy. Дополнительно ``worker`` позволяет подменить общий реактор-воркер
(детерминированные юнит-тесты без Twisted/сети).

**Ошибки.** Сбой фетча поднимает ``CrawlError`` (pipeline изолирует по документу и
отображает на ``source_unavailable``). Отклонённый SSRF-валидацией URL поднимает
``CrawlUrlError`` (подкласс и ``CrawlError``, и ``ValueError``).

Нейтрален (§1.3): в модуле нет названий продуктов/вендоров; профили — данные.
"""

from __future__ import annotations

import atexit
import importlib.util
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timezone
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

# Callable-шов воркера: ``(settings, spec) -> deferred`` (реальный CrawlerRunner
# либо тестовый фейк). Возвращённый объект обязан поддерживать ``addCallbacks``.
CrawlFn = Callable[[Mapping[str, Any], "_CrawlSpec"], Any]

# Шов установки реактора: вызывается в потоке воркера и возвращает реактор.
InstallFn = Callable[[], Any]

# Шов создания потока: ``target -> thread`` (дефолт — daemon-thread).
ThreadFactory = Callable[[Callable[[], None]], Any]

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
    # NB: под CrawlerRunner настройка инертна — реактор ставит воркер; оставлена
    # для совместимости со смоук-путём и как декларация требуемого реактора.
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

# Таймаут ожидания старта общего реактора (поток обязан успеть установить его).
_STARTUP_TIMEOUT = 30.0
# Таймаут best-effort останова общего реактора при ``close``.
_SHUTDOWN_TIMEOUT = 10.0
# Таймаут JS-ожидания для browser-профиля (совпадает с navigation timeout).
_WAIT_FOR_TIMEOUT_MS = 15000


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


# ---------------------------------------------------------------------------
# Общий реактор-воркер (один на процесс)
# ---------------------------------------------------------------------------


@dataclass
class _CrawlSpec:
    """Параметры одного краула: URL, режим, JS-ожидание и приёмник ``RawPage``.

    Мутабельный контейнер намеренно: ``collected`` наполняется пауком в потоке
    реактора, а вызывающий читает его после завершения Future.
    """

    url: str
    render: RenderMode
    wait_for: str | None
    collected: list[RawPage] = field(default_factory=list)


# Активные ``CrawlerRunner``: держим ссылку, пока краул не завершён (иначе движок
# может быть собран GC до закрытия паука).
_ACTIVE_RUNNERS: set[Any] = set()


def _default_install() -> Any:
    """Устанавливает asyncio-реактор в текущем потоке и возвращает его.

    ``scrapy.utils.reactor.install_reactor`` сам реактор не возвращает, поэтому
    импортируем установленный ``twisted.internet.reactor`` **после** установки —
    импорт реактора до установки поставил бы дефолтный (epoll). Обязателен
    asyncio-реактор (``AsyncioSelectorReactor``) — требование scrapy-playwright (V5).
    Вызывается **в потоке воркера**, чтобы asyncio-event-loop принадлежал этому потоку.
    """
    from scrapy.utils.reactor import install_reactor  # type: ignore[import-not-found]

    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    from twisted.internet import reactor  # type: ignore[import-not-found]

    return reactor


def _default_thread_factory(target: Callable[[], None]) -> threading.Thread:
    """Создаёт daemon-thread для реактора (не удерживает выход процесса)."""
    return threading.Thread(target=target, name="cohound-scrapy-reactor", daemon=True)


def _default_crawl(settings: Mapping[str, Any], spec: _CrawlSpec) -> Any:
    """Свежий ``CrawlerRunner`` на один submit; возвращает ``Deferred`` паука.

    Новый движок на каждый краул изолирует сбои: падение одного не отравляет
    следующий. ``Deferred`` срабатывает при закрытии паука (результат не важен —
    контент собирается в ``spec.collected``).
    """
    from scrapy import Request, Spider  # type: ignore[import-not-found]
    from scrapy.crawler import CrawlerRunner  # type: ignore[import-not-found]

    class _SingleRequestSpider(Spider):  # type: ignore[misc, valid-type]
        name = "cohound-fetch"

        async def start(self):  # noqa: ANN201 - сигнатура Scrapy 2.19
            meta: dict[str, Any] = {}
            if spec.render is RenderMode.BROWSER:
                from scrapy_playwright.page import (  # type: ignore[import-not-found]
                    PageMethod,
                )

                methods = []
                if spec.wait_for:
                    methods.append(
                        PageMethod(
                            "wait_for_function",
                            spec.wait_for,
                            timeout=_WAIT_FOR_TIMEOUT_MS,
                        )
                    )
                meta = {
                    "playwright": True,
                    # Page не удерживаем: иначе без закрытия краул повисает (V5).
                    "playwright_include_page": False,
                    "playwright_page_methods": methods,
                }
            # dont_filter=True: один и тот же URL в разных fetch не дедуплицируется.
            yield Request(spec.url, callback=self._collect, dont_filter=True, meta=meta)

        def _collect(self, response) -> None:  # noqa: ANN001 - объект Scrapy
            raw_type = response.headers.get(b"Content-Type", b"")
            content_type = raw_type.decode("latin-1") if raw_type else None
            spec.collected.append(
                RawPage(
                    url=response.url,
                    content=response.body,
                    fetched_at=_now_iso(),
                    render_mode=spec.render,
                    status_code=response.status,
                    content_type=content_type or None,
                )
            )

    runner = CrawlerRunner(dict(settings))
    deferred = runner.crawl(_SingleRequestSpider)
    _ACTIVE_RUNNERS.add(runner)
    # Снимаем ссылку на runner, когда краул завершился (успех или сбой), сохраняя
    # результат/сбой без изменений.
    deferred.addBoth(lambda result: (_ACTIVE_RUNNERS.discard(runner), result)[1])
    return deferred


def _safe_set_result(fut: Future[Any], value: Any) -> None:
    """Устанавливает результат Future, если он ещё не завершён (гонка с таймаутом)."""
    if not fut.done():
        fut.set_result(value)


def _safe_set_exception(fut: Future[Any], exc: BaseException) -> None:
    """Устанавливает исключение Future, если он ещё не завершён (гонка с таймаутом)."""
    if not fut.done():
        fut.set_exception(exc)


def _failure_value(failure: Any) -> BaseException:
    """Достаёт исключение из Twisted ``Failure`` (или возвращает объект как есть)."""
    value = getattr(failure, "value", None)
    return value if isinstance(value, BaseException) else cast(BaseException, failure)


def _as_crawl_error(url: str, exc: BaseException) -> CrawlError:
    """Приводит сбой краула к ``CrawlError`` с URL в сообщении."""
    if isinstance(exc, CrawlError):
        return exc
    return CrawlError(f"Scrapy-краул {url!r} не удался: {exc}")


class _ReactorWorker:
    """Один процесс-глобальный Twisted-реактор в выделенном daemon-потоке.

    Ленивый singleton (см. ``_shared_worker``): реактор Twisted глобальный на процесс.
    На каждый submit — свежий ``CrawlerRunner`` (через ``crawl``-шов), результат
    передаётся в ``concurrent.futures.Future``. Реактор не останавливается между
    краулами; ``close`` останавливает его один раз (идемпотентно).

    Швы ``install``/``crawl``/``thread_factory`` позволяют юнит-тестам подменить
    реальный Twisted/сеть детерминированным фейком.
    """

    def __init__(
        self,
        *,
        install: InstallFn | None = None,
        crawl: CrawlFn | None = None,
        thread_factory: ThreadFactory | None = None,
    ) -> None:
        self._install: InstallFn = install or _default_install
        self._crawl: CrawlFn = crawl or _default_crawl
        self._thread_factory: ThreadFactory = thread_factory or _default_thread_factory
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._reactor: Any = None
        self._thread: Any = None
        self._startup_error: BaseException | None = None
        self._started = False
        self._closed = False

    # -- жизненный цикл -------------------------------------------------------

    def _run(self) -> None:
        """Тело потока реактора: установка (в этом потоке) → ``run`` (один раз)."""
        try:
            self._reactor = self._install()
        except BaseException as exc:  # noqa: BLE001 - сообщаем через _startup_error
            self._startup_error = exc
            self._ready.set()
            return
        self._ready.set()
        self._reactor.run(installSignalHandlers=False)

    def ensure_started(self) -> Any:
        """Лениво стартует реактор и возвращает его; идемпотентно."""
        with self._lock:
            if self._closed:
                raise CrawlError("общий Scrapy-реактор уже остановлен")
            if not self._started:
                self._started = True
                thread = self._thread_factory(self._run)
                self._thread = thread
                thread.start()
                atexit.register(self.close)
            ready = self._ready
        if not ready.wait(timeout=_STARTUP_TIMEOUT):
            raise CrawlError("общий Scrapy-реактор не запустился за отведённое время")
        if self._startup_error is not None:
            raise CrawlError(
                f"не удалось установить Scrapy-реактор: {self._startup_error}"
            )
        if self._reactor is None:
            raise CrawlError("общий Scrapy-реактор не установлен")
        return self._reactor

    def submit(
        self,
        settings: Mapping[str, Any],
        spec: _CrawlSpec,
        fut: Future[Any],
        timeout_s: float,
    ) -> None:
        """Планирует краул на общем реакторе и ждёт результат/таймаут.

        Вызывается из потока-потребителя. Таймаут относится **только** к этому URL:
        осиротевший краул завершится позже и не повлияет на следующие submits.
        """
        reactor = self.ensure_started()
        reactor.callFromThread(self._launch, settings, spec, fut)
        try:
            fut.result(timeout=timeout_s)
        except FuturesTimeoutError as exc:
            raise CrawlError(
                f"Scrapy-краул {spec.url!r}: таймаут ожидания за {timeout_s:g} c"
            ) from exc

    def _launch(
        self, settings: Mapping[str, Any], spec: _CrawlSpec, fut: Future[Any]
    ) -> None:
        """Выполняется в потоке реактора: строит движок и связывает Deferred с Future."""
        try:
            deferred = self._crawl(settings, spec)
        except BaseException as exc:  # noqa: BLE001 - любой сбой движка → CrawlError
            _safe_set_exception(fut, _as_crawl_error(spec.url, exc))
            return
        deferred.addCallbacks(
            lambda _result: _safe_set_result(fut, True),
            lambda failure: _safe_set_exception(
                fut, _as_crawl_error(spec.url, _failure_value(failure))
            ),
        )

    def close(self, timeout: float = _SHUTDOWN_TIMEOUT) -> None:
        """Best-effort останавливает реактор и дожидается потока; идемпотентно.

        Вызывается и при ``atexit``, и явно (``ScrapyCrawler.close``). Если воркер
        не запускался — no-op.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            reactor = self._reactor
            thread = self._thread
        if reactor is not None:
            try:
                reactor.callFromThread(reactor.stop)
            except Exception:  # noqa: BLE001 - best-effort останов
                pass
        if thread is not None and thread.is_alive():
            thread.join(timeout)


# Ленивый process-глобальный singleton: реактор Twisted глобальный на процесс.
_SHARED_WORKER: _ReactorWorker | None = None
_SHARED_WORKER_LOCK = threading.Lock()


def _shared_worker() -> _ReactorWorker:
    """Возвращает общий воркер, создавая его лениво (потокобезопасно)."""
    global _SHARED_WORKER
    with _SHARED_WORKER_LOCK:
        if _SHARED_WORKER is None:
            _SHARED_WORKER = _ReactorWorker()
        return _SHARED_WORKER


class ScrapyCrawler:
    """Краулер на Scrapy (HTTP + per-request playwright) с глобальным кэшем.

    ``version`` — версия адаптера (попадает в ``run_manifest``). ``fetch``
    возвращает ``RawPage`` с сырыми байтами. Нейтрален (§1.3).
    """

    version = "scrapy-crawler-2"

    def __init__(
        self,
        fetcher: FetchFn | PageFetcher | None = None,
        *,
        cache: ContentAddressedCache | None = None,
        domain_profiles: Mapping[str, Any] | None = None,
        scrapy_settings: Mapping[str, Any] | None = None,
        worker: _ReactorWorker | None = None,
    ) -> None:
        """``fetcher`` — тестовый шов; ``cache`` — глобальный кэш; ``domain_profiles`` —
        per-domain рендер-профили (из ``load_domain_profiles``); ``scrapy_settings`` —
        переопределения настроек реального пути; ``worker`` — тестовый шов общего
        реактор-воркера (по умолчанию — process-глобальный singleton).
        """
        self._fetcher = fetcher
        self._cache = cache
        self._profiles = {
            str(domain).strip().lower(): profile
            for domain, profile in (domain_profiles or {}).items()
            if str(domain).strip()
        }
        self._scrapy_settings = dict(scrapy_settings or {})
        self._worker = worker

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

        Порядок: SSRF-валидация → выбор режима → (для HTTP) кэш → живой фетч →
        (для HTTP) запись в кэш. Браузерный рендер **не** консультирует и не
        пишет кэш: он недетерминирован, а общий ключ ``http_key(url)`` дал бы
        кросс-контаминацию HTTP/browser по одному URL.
        """
        try:
            validate_url(url)
        except ValueError as exc:
            raise CrawlUrlError(f"URL отклонён SSRF-валидацией: {exc}") from exc

        effective = self.render_for(url, render)
        use_cache = effective is RenderMode.HTTP

        if use_cache and self._cache is not None:
            cached = self._cache.get_bytes("http", http_key(url))
            if cached is not None:
                return self._page_from_cache(url, cached, effective)

        page = self._fetch_live(url, effective)
        if use_cache:
            self._store(url, http_key(url), page.content)
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
        """Реальный путь: мультиплексирует краул на общий реактор процесса.

        Свежий ``CrawlerRunner`` на submit (см. воркер), один реактор на процесс.
        Требует ``scrapy`` (и ``scrapy-playwright`` для браузерного режима).
        Юнит-тестами покрывается через внедряемый ``worker``; сеть/браузеры — smoke
        (``tests/smoke_scrapy_playwright.py``, V5).
        """
        if self._worker is None and not _scrapy_available():
            raise CrawlError(
                "Scrapy не установлен: установите extra 'crawl' "
                "(scrapy/scrapy-playwright/playwright) или внедрите fetcher"
            )

        wait_for = _profile_wait_for(self._profile_for(url))
        settings = self._build_settings(render)
        spec = _CrawlSpec(url=url, render=render, wait_for=wait_for)
        fut: Future[bool] = Future()
        timeout_s = self._submit_timeout(settings)
        self._worker_instance().submit(settings, spec, fut, timeout_s)

        if not spec.collected:
            raise CrawlError(f"Scrapy не вернул контент для {url!r}")
        return spec.collected[0]

    def _worker_instance(self) -> _ReactorWorker:
        """Внедрённый воркер теста либо process-глобальный singleton."""
        if self._worker is not None:
            return self._worker
        return _shared_worker()

    @staticmethod
    def _submit_timeout(settings: Mapping[str, Any]) -> float:
        """Дедлайн submit из настроек: попытки × download-timeout + запас 30 c."""
        retries = int(settings["RETRY_TIMES"])
        download = int(settings["DOWNLOAD_TIMEOUT"])
        return (retries + 1) * download + 30

    def close(self) -> None:
        """Best-effort останавливает общий реактор (идемпотентно; no-op если не стартовал).

        Общий воркер process-глобальный, поэтому закрытие затрагивает все
        экземпляры :class:`ScrapyCrawler` процесса — это ожидаемо и вызывается
        один раз в конце (после последнего ``fetch``).
        """
        if self._worker is not None:
            self._worker.close()
            return
        worker = _SHARED_WORKER
        if worker is not None:
            worker.close()

    def _build_settings(self, render: RenderMode) -> dict[str, Any]:
        """Итоговые настройки Scrapy: дефолты → playwright (для browser) → overrides."""
        settings = dict(DEFAULT_SCRAPY_SETTINGS)
        if render is RenderMode.BROWSER:
            settings.update(_PLAYWRIGHT_SETTINGS)
        settings.update(self._scrapy_settings)
        return settings
