"""Краулер на Scrapy — реализация ``CrawlerPort`` (W7/T7.1, Ш5).

Адаптер загружает страницы и возвращает **сырые байты** в ``RawPage`` (retention,
I19; 02 §B.1). Ответственность адаптера (01 §3.2, ``ports/crawler.py``):

* **robots по оси** — политика ``robots.txt`` подчиняется объявленной оси ``plan.robots``,
  а не константе: ``ROBOTSTXT_OBEY=True`` держится в дефолтных настройках (дефолт Scrapy при
  встраивании как библиотеки — ``False``, 05 §2.5), а при выключенной оси запрос несёт
  ``meta={"dont_obey_robotstxt": True}`` — стоковый ``RobotsTxtMiddleware`` в этом случае
  вообще не обращается к ленте (проверено по исходнику установленной 2.19.0:
  ``process_request`` начинается с этой проверки). До исправления объявленное
  ``crawl.robots: false`` на Scrapy-пути **не действовало** — конфиг врал;
* **лента ``robots.txt`` — один раз на origin, а не на документ.** Стоковый
  ``RobotsTxtMiddleware`` кеширует разобранную ленту в своём ``_parsers``, а инстанс
  создаётся заново на каждый ``CrawlerRunner``, то есть на каждый ``fetch``: лента
  перезапрашивалась на каждый документ (измерено 07.11.2026: у ``docs.tantorlabs.ru``
  ``/robots.txt`` → 301 на чужой хост → 404, поэтому на документ уходил лишний запрос на
  нужный хост **и** запрос на чужой — вне объявленного лимита и вне счётчика). Подкласс
  :func:`_shared_robots_middleware_class` держит **общий на процесс** кеш разобранных
  лент; проверка каждого запроса (и каждого хопа редиректа) остаётся на middleware, поэтому
  политика §10 не ослаблена. Кешируются только **разрешённые** ленты (парсер или
  «ленты нет»); ``Deferred`` не кешируется (привязан к закрытому краулеру), а недоступная
  лента — тем более (иначе сбой чтения стал бы вечным разрешением);
* **похоповая валидация редиректов** (R10) — стоковые ``RedirectMiddleware`` и
  ``MetaRefreshMiddleware`` заменяются подклассами, которые проверяют цель каждого хопа
  (``validate_url``) и при нарушении поднимают ``IgnoreRequest``, **не** выпуская запрос;
  отказ доезжает до вызывающего как ``CrawlUrlError`` (см. ``_redirect_guard_classes``);
* **playwright per-request** — при ``plan.fetch is FetchMode.BROWSER`` запрос идёт с
  ``playwright``-meta (снимок после готовности JS, ``plan.wait_for``; V5-оговорки 05 §2.5);
* **ретраи — не в движке, а в слое фетча.** ``RETRY_ENABLED=False``/``RETRY_TIMES=0``:
  резерв лимитера один на ``fetch()``, и повтор загрузчика его обходил (до двух
  внеочередных запросов на неудачную страницу, ровно на больной хост). Повтор живёт в
  ``corpus_ingest._fetch_with_paced_retries``: каждая попытка берёт свой слот
  ``RateLimiter``. Свой пейсинг Scrapy тоже выключен (``DOWNLOAD_DELAY=0``) — темп
  задаёт ``RateLimiter`` (см. ниже);
* **content-addressed кэш** — перед живым фетчем консультируется глобальный кэш
  (``core.util.cache``, ADR 0016): попадание → ``RawPage`` из байтов, промах →
  фетч + ``put_bytes("http", http_key(url), content)`` (R7 между ``run_id``).
  Кэш применяется **только** к ``FetchMode.HTTP``: браузерный рендер всегда
  идёт вживую и не пишется в кэш (детерминизм R7; см. ``fetch``);
* **условный запрос** — приоритет валидаторов: **план → мета кэша → безусловный GET**.
  Валидаторы плана (``if_none_match``/``if_modified_since``) уходят заголовками
  ``If-None-Match``/``If-Modified-Since``; если план их не несёт, адаптер берёт их из
  сайдкара ``.meta.json`` прошлого получения и делает **живой** условный запрос.
  ``ETag``/``Last-Modified`` успешного ответа сохраняются в ``RawPage`` **и** в
  сайдкар (без записи сайдкара цикл условных запросов не мог бы начаться). Ответ
  ``304`` возвращает **закэшированные** байты с ``not_modified=True``; без кэша это
  явная ошибка. Чтобы ``304`` вообще доходил до callback, ``304`` добавлен в
  ``HTTPERROR_ALLOWED_CODES`` (иначе ``HttpErrorMiddleware`` поднимает ``HttpError``
  на любой не-2xx, и ветка «байты из кэша» недостижима). Остальные не-2xx (404/410
  и прочие) по-прежнему уходят в errback — их классификация не меняется.

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
``(url, plan) -> RawPage`` или объект с методом ``fetch(url, plan)``. Если он
задан, реальный Scrapy-путь не задействуется, и юнит-тесты не запускают
Twisted-реактор и не требуют браузеров. Если ``fetcher is None`` — используется
настоящий Scrapy. Дополнительно ``worker`` позволяет подменить общий реактор-воркер
(детерминированные юнит-тесты без Twisted/сети).

**Ошибки.** Таксономия сбоев объявлена в порту — ``core.ports.crawl_errors`` (R5):
``CrawlError``, ``CrawlNotFound`` (``failure_kind="not_found"``), ``CrawlUrlError``
(``CrawlError`` + ``ValueError``), ``CrawlRefused`` (политика), ``CrawlPayloadTooLarge``,
``CrawlUnavailable`` (движок не поднялся). Этот модуль их **не объявляет** — только
поднимает. Сбой фетча здесь — ``CrawlError`` (pipeline изолирует по документу и
отображает на ``source_unavailable``). Определённый ответ «ресурса нет» (HTTP 404/410)
поднимает ``CrawlNotFound``: pipeline делает такой исход терминальным (без повторов) и
не считает origin нечитаемым. Отклонённый SSRF-валидацией URL поднимает ``CrawlUrlError``
— и на входе, и на любом хопе редиректа.

Нейтрален (§1.3): в модуле нет названий продуктов/вендоров; профили — данные.
"""

from __future__ import annotations

import atexit
import contextlib
import importlib.util
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Protocol, cast, runtime_checkable

from core.domain.enums import FailureKind
from core.domain.models import DEFAULT_MAX_RESPONSE_BYTES
from core.ports.crawl_errors import (
    NOT_FOUND_STATUSES,
    CrawlError,
    CrawlNotFound,
    CrawlPayloadTooLarge,
    CrawlRefused,
    CrawlUrlError,
)
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.cache import ContentAddressedCache, http_key
from core.util.http_audit import current_attempt
from core.util.security import validate_url

#: Таксономия сбоев (``CrawlError`` и подклассы) объявлена в **порту** —
#: ``core.ports.crawl_errors`` (R5), а не здесь: её ловит вызывающий, не зная транспорта.
#: Здесь остаются только константы исполнения этого транспорта.
__all__ = [
    "DEFAULT_MAX_RESPONSE_BYTES",
    "DEFAULT_SCRAPY_SETTINGS",
    "ScrapyCrawler",
]

# Внедряемый фетчер: callable ``(url, plan) -> RawPage``. Объект с методом
# ``fetch(url, plan)`` также принимается (см. ``PageFetcher``).
FetchFn = Callable[[str, FetchPlan], RawPage]

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
    # Повторы **вынесены из движка в слой фетча** (``corpus_ingest._fetch_with_paced_retries``):
    # резерв лимитера — один на ``fetch()``, поэтому повтор загрузчика обходил пейсинг и
    # отправлял до двух внеочередных запросов без интервала — именно на тот хост, который
    # только что ответил ошибкой. Измерено 07.11.2026: 18 отказов ``504`` дали
    # ``http_requests = 357`` на 318 элементов вместо ≈320 при ``attempts = 1``
    # (``docs/reports/measurements/2026-11-07-crawl-rate-tantor.md`` §7.6). Число попыток
    # сохранено (1 + 2), изменилось только то, что каждая берёт свой слот.
    "RETRY_ENABLED": False,
    # Ноль явно: ``_submit_timeout`` считает дедлайн как ``(RETRY_TIMES + 1)`` таймаутов,
    # и при 2 он ждал бы повторов, которых движок больше не делает.
    "RETRY_TIMES": 0,
    # След прежней политики движка. Действующее правило — в слое фетча: 5xx (в списке это
    # 500/502/503/504/522/524) плюс 408/429 (вежливое «попробуй позже»). Движок по этим
    # кодам больше не повторяет.
    "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
    # **Пейсинг один — ``RateLimiter``** (``core/util/rate_limit.py``, объявленная ось
    # ``rate_limit``). ``DOWNLOAD_DELAY`` = 0 намеренно, и ключ оставлен явной
    # декларацией: раньше здесь стояло 0.5, и это был **второй** пейсер поверх лимитера,
    # не входивший ни в ``crawl_config_digest``, ни в ``run_id``. Как «вежливость» он
    # бессмысленен (``fetch`` строит свежий ``CrawlerRunner`` на документ, поэтому
    # интервал между документами он не соблюдает), но начисляет 0.25–0.75 с **внутри**
    # одной попытки — между запросом ленты и страницы. Измерено 07.11.2026 на
    # ``docs.tantorlabs.ru`` (5 URL, ``fetch_ms`` p50): ``plan.robots=False`` — 147 мс;
    # ``robots=True`` + ``DOWNLOAD_DELAY=0`` — 246 мс; штатные настройки — **1409 мс**
    # при собственном времени ответа хоста ≈100 мс
    # (``docs/reports/measurements/2026-11-07-crawl-rate-tantor.md`` §4, §7.5).
    # A3: скрытый default заменён замером, а не предусмотрительностью.
    "DOWNLOAD_DELAY": 0,
    # Джойтер к задержке явно нулевой: при нулевой задержке он и так ничего не даёт, но
    # пусть «скрытой задержки нет» будет декларацией, а не следствием арифметики.
    "DOWNLOAD_DELAY_JITTER": 0,
    "DOWNLOAD_TIMEOUT": 30,
    # Не потолок ширины краула: ``CONCURRENT_REQUESTS_PER_DOMAIN`` действует **внутри
    # одного downloader'а**, а downloader здесь свой на каждый ``fetch`` (свежий
    # ``CrawlerRunner``) — то есть это не более двух одновременных запросов одной попытки,
    # а не limit на хост. Ширина обхода задаётся пулом/полосами узла
    # (``config/deploy.yaml: crawl.max_workers/host_concurrency``); замер: 5 потоков давали
    # ``in_flight_max`` 5–6, то есть эта настройка потолком не работала.
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

# Таймаут ожидания старта общего реактора (поток обязан успеть установить его).
_STARTUP_TIMEOUT = 30.0
# Таймаут best-effort останова общего реактора при ``close``.
_SHUTDOWN_TIMEOUT = 10.0
# Таймаут JS-ожидания для browser-профиля (совпадает с navigation timeout).
_WAIT_FOR_TIMEOUT_MS = 15000


@runtime_checkable
class PageFetcher(Protocol):
    """Минимальный контракт внедряемого фетчера (альтернатива callable)."""

    def fetch(self, url: str, plan: FetchPlan | None = None) -> RawPage: ...


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` при живом фетче)."""
    return datetime.now(UTC).isoformat()


def _conditional_headers(plan: FetchPlan) -> dict[str, str]:
    """Условные заголовки запроса из валидаторов плана (пусто — безусловный GET)."""
    headers: dict[str, str] = {}
    if plan.if_none_match:
        headers["If-None-Match"] = plan.if_none_match
    if plan.if_modified_since:
        headers["If-Modified-Since"] = plan.if_modified_since
    return headers


def _scrapy_available() -> bool:
    """Установлен ли Scrapy (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec("scrapy") is not None


# ---------------------------------------------------------------------------
# Общий реактор-воркер (один на процесс)
# ---------------------------------------------------------------------------


@dataclass
class _CrawlSpec:
    """Параметры одного краула: URL, режим, JS-ожидание и приёмник ``RawPage``.

    Мутабельный контейнер намеренно: ``collected`` наполняется пауком в потоке
    реактора, а вызывающий читает его после завершения Future.

    ``headers`` — заголовки запроса (условные ``If-None-Match``/``If-Modified-Since``);
    ``cached`` — байты текущей кэш-записи, нужные для тела ответа ``304`` (сам ``304``
    представления не несёт); ``error`` — сбой, который нельзя поднять из callback паука;
    ``error_kind`` — машинный класс сбоя (``FailureKind``), если ответ определён (404/410);
    ``http_status`` — фактический статус ответа, если он наблюдался (наблюдаемость:
    переносится в исключение и далее в ``crawl_session_items.http_status``);
    ``unsafe_redirect`` — запрос не выпущен: цель редиректа отклонена SSRF-валидацией
    (R10). Отдельный флаг, а не ``error_kind``: ``FailureKind`` описывает наблюдения
    краула/конвейера, а это отказ транспорта до запроса — он поднимается как
    ``CrawlUrlError``, ровно как отказ входного URL.

    ``robots`` — **объявленная ось** ``FetchPlan.robots``: ``True`` (дефолт) — Scrapy
    соблюдает ``robots.txt``; ``False`` — запрос несёт ``dont_obey_robotstxt``, и лента не
    запрашивается вовсе. Дефолт ``True`` повторяет прежнее поведение пути, где ось не
    читалась.
    """

    url: str
    fetch: FetchMode
    wait_for: str | None
    collected: list[RawPage] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)
    cached: bytes | None = None
    error: str | None = None
    error_kind: str | None = None
    http_status: int | None = None
    unsafe_redirect: bool = False
    robots: bool = True


# Активные ``CrawlerRunner``: держим ссылку, пока краул не завершён (иначе движок
# может быть собран GC до закрытия паука).
_ACTIVE_RUNNERS: set[Any] = set()


def _scrapy_failure_status(failure: Any) -> int | None:
    """HTTP-статус из Twisted ``Failure`` (``HttpError.response.status``), если он есть.

    Scrapy по умолчанию фильтрует не-2xx ответы ``HttpErrorMiddleware`` и зовёт
    ``errback``; статус лежит в ``failure.value.response.status``. Шов терпим к
    произвольным объектам (юнит-тесты без Scrapy) и не выдумывает статус при его
    отсутствии — тогда классификация остаётся общей (``source_unavailable``).
    """
    value = getattr(failure, "value", failure)
    response = getattr(value, "response", None)
    status = getattr(response, "status", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _record_failure(spec: _CrawlSpec, failure: Any) -> None:
    """Классифицирует сбой запроса в ``spec``: robots-запрет → ``robots_disallow``,
    404/410 → ``not_found``, oversize → ``payload_too_large``, небезопасный редирект →
    ``unsafe_redirect``, иначе общий сбой.

    Без errback'а Scrapy только логировала ошибку, и ``_fetch_with_scrapy`` сообщал
    «контент не получен» — определённый ответ об отсутствии терялся.

    ``spec.http_status`` фиксируется всегда, когда статус наблюдался: 429/403/503
    остаются общим классом (``source_unavailable``), но перестают быть неразличимы
    (наблюдаемость, не таксономия — см. :class:`CrawlError`).
    """
    status = _scrapy_failure_status(failure)
    if status is not None:
        # Наблюдаемость: статус доезжает до элемента сеанса даже когда класс
        # отказа остаётся общим (``source_unavailable``).
        spec.http_status = status
    if _is_unsafe_redirect(failure):
        # R10: цель редиректа отклонена SSRF-валидацией — запрос не выпускался.
        # ``error_kind`` не заполняется: это отказ транспорта до запроса, а не
        # наблюдение краула (см. ``_CrawlSpec.unsafe_redirect``).
        spec.unsafe_redirect = True
        spec.error = f"Scrapy-краул {spec.url!r}: {_failure_value(failure)}"
        return
    if _is_robots_refusal(failure):
        # ``robots_disallow`` входит в ``BLOCKING_FAILURE_KINDS``, а
        # ``source_unavailable`` — нет. Записанный как недоступность robots-запрет
        # понижает блокировку: ``corpus_coverage.complete`` может стать True, пока
        # целый origin закрыт robots.txt (измерено на живом прогоне: 134 URL, из них
        # 104 на одном хосте). Отказ — соблюдение политики, а не сбой источника.
        spec.error_kind = FailureKind.ROBOTS_DISALLOW.value
        spec.error = f"Scrapy-краул {spec.url!r}: robots.txt запрещает обход"
        return
    if status in NOT_FOUND_STATUSES:
        spec.error_kind = FailureKind.NOT_FOUND.value
        spec.error = f"HTTP {status} для {spec.url!r}: ресурс отсутствует"
        return
    if (evidence := _oversize_evidence(failure)) is not None:
        spec.error_kind = FailureKind.PAYLOAD_TOO_LARGE.value
        # Путь классификации — в наблюдение: структурный признак (тип загрузчика) или
        # последний рубеж (подстрока). Без этого «почему решили, что ответ велик»
        # неотличимо от догадки.
        spec.error = (
            f"Scrapy-краул {spec.url!r}: ответ превысил "
            f"DOWNLOAD_MAXSIZE ({evidence}: {_failure_value(failure)})"
        )
        return
    spec.error = f"Scrapy-краул {spec.url!r} не удался: {_failure_value(failure)}"


def _raw_page_from_scrapy_response(spec: _CrawlSpec, response: Any) -> RawPage | None:
    """Собирает ``RawPage`` из ответа Scrapy (валидаторы — ``bytes`` → ``latin-1``).

    ``304`` не несёт представления: тело берётся **только** из ``spec.cached``
    (байты текущей кэш-записи). Их отсутствие — неконсистентный условный запрос:
    фиксируем ``spec.error`` и возвращаем ``None`` (пустую страницу не фабрикуем).
    Это **не** недоступность источника, и сообщение обязано это показывать: сервер
    ответил корректно, а нечем закрыть ``304``. Отдельного ``FailureKind`` для такого
    рассинхрона в словаре нет, поэтому различитель — явный текст ошибки плюс
    наблюдаемый ``http_status = 304`` в записи элемента.

    ``404``/``410`` — определённый ответ «ресурса нет»: фиксируем ``spec.error_kind =
    not_found`` и возвращаем ``None``. Путь достижим, если ``handle_httpstatus_list``
    пропустил статус в callback; обычный путь классифицирует тот же статус в errback
    (:func:`_scrapy_failure_status`), поэтому правило одно на оба входа.

    ``spec.http_status`` фиксируется всегда, когда статус есть: это наблюдение, а не
    классификация (см. :class:`CrawlError`).
    """
    raw_type = response.headers.get(b"Content-Type", b"")
    content_type = raw_type.decode("latin-1") if raw_type else None
    raw_etag = response.headers.get(b"ETag", b"")
    raw_modified = response.headers.get(b"Last-Modified", b"")
    etag = raw_etag.decode("latin-1") if raw_etag else None
    last_modified = raw_modified.decode("latin-1") if raw_modified else None
    status = int(response.status)
    spec.http_status = status
    if status in NOT_FOUND_STATUSES:
        spec.error_kind = FailureKind.NOT_FOUND.value
        spec.error = f"HTTP {status} для {spec.url!r}: ресурс отсутствует"
        return None
    not_modified = status == 304
    if not_modified:
        if spec.cached is None:
            # Сервер подтвердил неизменность, но тела взять негде: это рассинхрон
            # условного запроса с кэшем, а не сбой источника. Пустую страницу не
            # фабрикуем (иначе документ «успешно» станет пустым).
            spec.error = (
                f"Scrapy 304 для {spec.url!r}: сервер подтвердил неизменность, "
                f"но в кэше нет байтов представления (не сбой источника)"
            )
            return None
        content = spec.cached
    else:
        content = response.body
    return RawPage(
        url=response.url,
        content=content,
        fetched_at=_now_iso(),
        fetch_mode=spec.fetch,
        status_code=response.status,
        content_type=content_type or None,
        etag=etag or None,
        last_modified=last_modified or None,
        not_modified=not_modified,
    )


def _default_install() -> Any:
    """Устанавливает asyncio-реактор в текущем потоке и возвращает его.

    ``scrapy.utils.reactor.install_reactor`` сам реактор не возвращает, поэтому
    импортируем установленный ``twisted.internet.reactor`` **после** установки —
    импорт реактора до установки поставил бы дефолтный (epoll). Обязателен
    asyncio-реактор (``AsyncioSelectorReactor``) — требование scrapy-playwright (V5).
    Вызывается **в потоке воркера**, чтобы asyncio-event-loop принадлежал этому потоку.
    """
    from scrapy.utils.reactor import install_reactor

    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    from twisted.internet import reactor

    return reactor


def _default_thread_factory(target: Callable[[], None]) -> threading.Thread:
    """Создаёт daemon-thread для реактора (не удерживает выход процесса)."""
    return threading.Thread(target=target, name="cohound-scrapy-reactor", daemon=True)


def _request_meta(spec: _CrawlSpec) -> dict[str, Any]:
    """``meta`` одного запроса Scrapy по плану: playwright-опции и ось ``robots``.

    Ось ``robots`` реализуется **per-request** (``dont_obey_robotstxt``), а не только
    глобальной настройкой ``ROBOTSTXT_OBEY``: настройка одна на движок, поэтому
    объявленное ``robots: false`` на ней не действовало (конфиг врал). Проверено на
    установленной 2.19.0: ``RobotsTxtMiddleware.process_request`` первым делом делает
    ``if request.meta.get("dont_obey_robotstxt"): return`` — то есть лента для такого
    запроса не запрашивается вовсе, а не «скачивается и игнорируется».

    Обратное направление (``spec.robots=True``) **не** ставит ``dont_obey_robotstxt``:
    соблюдение ленты остаётся на ``ROBOTSTXT_OBEY=True`` из дефолтных настроек, чтобы
    операторский ``scrapy_settings`` мог ослабить его осознанно, а адаптер не отменял
    объявленную ось двумя разными способами.
    """
    meta: dict[str, Any] = {}
    if spec.fetch is FetchMode.BROWSER:
        from scrapy_playwright.page import (
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
    if not spec.robots:
        meta["dont_obey_robotstxt"] = True
    return meta


def _default_crawl(settings: Mapping[str, Any], spec: _CrawlSpec) -> Any:
    """Свежий ``CrawlerRunner`` на один submit; возвращает ``Deferred`` паука.

    Новый движок на каждый краул изолирует сбои: падение одного не отравляет
    следующий. ``Deferred`` срабатывает при закрытии паука (результат не важен —
    контент собирается в ``spec.collected``).
    """
    from scrapy import Request, Spider
    from scrapy.crawler import CrawlerRunner

    class _SingleRequestSpider(Spider):
        name = "cohound-fetch"

        async def start(self):  # noqa: ANN201 - сигнатура Scrapy 2.19
            # dont_filter=True: один и тот же URL в разных fetch не дедуплицируется.
            # Условные заголовки — те же, что несёт план (additive; пусто = как раньше).
            # meta — playwright-опции и per-request ось robots (см. ``_request_meta``).
            yield Request(
                spec.url,
                callback=self._collect,
                errback=self._failed,
                dont_filter=True,
                meta=_request_meta(spec),
                headers=spec.headers or None,
            )

        def _collect(self, response) -> None:  # noqa: ANN001 - объект Scrapy
            page = _raw_page_from_scrapy_response(spec, response)
            if page is not None:
                spec.collected.append(page)

        def _failed(self, failure) -> None:  # noqa: ANN001 - Twisted Failure
            """Errback паука: делегирует :func:`_record_failure` (класс + статус)."""
            _record_failure(spec, failure)

    runner = CrawlerRunner(dict(settings))
    deferred = runner.crawl(_SingleRequestSpider)
    _ACTIVE_RUNNERS.add(runner)
    # Снимаем ссылку на runner, когда краул завершился (успех или сбой), сохраняя
    # результат/сбой без изменений. Именованная функция, а не лямбда с кортежом:
    # ``set.discard`` возвращает ``None``, и ``(discard(...), result)[1]`` был трюком
    # ради одного выражения — он прятал и тип колбэка (``Callable[[Any], Any]`` не
    # совпадал с ожидаемым ``addBoth``), и сам факт, что побочный эффект отдельный.
    def _release_runner(result: Any) -> Any:
        _ACTIVE_RUNNERS.discard(runner)
        return result

    deferred.addBoth(_release_runner)
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


#: Точное сообщение, с которым ``RobotsTxtMiddleware`` отказывает в обходе.
_ROBOTS_REFUSAL_MESSAGE = "Forbidden by robots.txt"


def _is_robots_refusal(failure: Any) -> bool:
    """Отказал ли **robots.txt** (а не сломался транспорт).

    Scrapy поднимает ``scrapy.exceptions.IgnoreRequest("Forbidden by robots.txt")``
    из ``RobotsTxtMiddleware.process_request_2``. Сигнал распознаётся по **типу и
    точному сообщению**, а не по подстроке: ``IgnoreRequest`` — базовый класс для
    других отказов middleware (дубликаты, offsite), а ``HttpError`` (не-2xx) вообще
    наследует ``IgnoreRequest``. Широкий предикат превратил бы в ``robots_disallow``
    любой отфильтрованный запрос.

    Импорт Scrapy ленивый: модуль импортируется и без него (см. шапку). Если Scrapy
    нет, отказать в robots-классификации безопаснее, чем угадывать.
    """
    try:
        from scrapy.exceptions import IgnoreRequest
    except ImportError:
        return False
    value = _failure_value(failure)
    if not isinstance(value, IgnoreRequest):
        return False
    return str(value).strip() == _ROBOTS_REFUSAL_MESSAGE


#: Метки пути классификации превышения потолка (пишутся в наблюдение). Структурный
#: признак — тип исключения загрузчика; подстрока — последний рубеж на случай, когда
#: типа в таксономии Scrapy не оказалось.
OVERSIZE_BY_TYPE = "тип DownloadCancelledError"
OVERSIZE_BY_MESSAGE = "подстрока сообщения"


def _download_cancelled_error() -> type[BaseException] | None:
    """Тип «загрузчик отменил загрузку» (``scrapy.exceptions.DownloadCancelledError``)."""
    try:
        from scrapy.exceptions import (
            DownloadCancelledError,
        )
    except ImportError:  # модуль импортируется и без Scrapy (см. шапку)
        return None
    return DownloadCancelledError


def _oversize_message(message: str) -> bool:
    """Подстрочный признак превышения — **последний рубеж**, а не основной путь.

    Фактическая формулировка загрузчика (``get_maxsize_msg``): «Expected to receive N
    bytes which is larger than download max size (M) in request …». Набор терпимый, потому
    что Scrapy не обещает стабильного текста между версиями; в штатном пути этот признак
    недостижим — срабатывает структурный (см. :func:`_oversize_evidence`).
    """
    return (
        "maxsize" in message
        or "maximum size" in message
        or "larger than download max size" in message
        or "downloaded more than" in message
        or "too large" in message
    )


def _oversize_evidence(failure: Any) -> str | None:
    """Чем установлено превышение потолка ``DOWNLOAD_MAXSIZE``: тип или подстрока.

    **Основной путь — структурный.** Загрузчик Scrapy отменяет загрузку, поднимая
    ``scrapy.exceptions.DownloadCancelledError``; проверено по исходнику установленной
    2.19.0: maxsize-abort поднимает его **без** ``__cause__`` (``http11.py:594`` —
    pre-check по ``Content-Length``; ``handlers/_base_streaming.py:277`` — потоковый
    путь; ``_http2/stream.py:461``), а отмена транспорта приходит обёрнутой
    (``scrapy/utils/_download_handlers.py:59``: ``raise DownloadCancelledError(str(e))
    from e``, где ``e`` — Twisted ``CancelledError``). Поэтому «тип совпал и причины нет»
    отличает потолок отмены от прочих отмен без чтения текста.

    Подстрока остаётся **последним рубежом** на случай, если тип в таксономии не найден
    (Scrapy нет в окружении / сменился тип). Прошлый набор подстрок уже промахнулся на
    фактической формулировке, поэтому он больше не единственный критерий, а путь
    классификации попадает в наблюдение (``error`` элемента сеанса).
    """
    value = _failure_value(failure)
    cancelled = _download_cancelled_error()
    if cancelled is not None and isinstance(value, cancelled):
        return OVERSIZE_BY_TYPE if value.__cause__ is None else None
    return OVERSIZE_BY_MESSAGE if _oversize_message(str(value).lower()) else None


def _is_oversize_failure(failure: Any) -> bool:
    """Похоже ли исключение на превышение ``DOWNLOAD_MAXSIZE`` (см. :func:`_oversize_evidence`)."""
    return _oversize_evidence(failure) is not None


def _as_crawl_error(url: str, exc: BaseException) -> CrawlError:
    """Приводит сбой краула к ``CrawlError`` с URL в сообщении."""
    if isinstance(exc, CrawlError):
        return exc
    return CrawlError(f"Scrapy-краул {url!r} не удался: {exc}")


#: Префикс сообщения ``IgnoreRequest``, которым наш redirect-middleware отказывает в
#: небезопасном хопе (R10). Классификация сверяет **префикс**: широкий предикат по
#: подстроке превратил бы в отказ любой отфильтрованный запрос (см. ``_is_robots_refusal``).
_REDIRECT_GUARD_MARKER = "cohound: redirect target rejected by SSRF validation"

#: Стоковые middleware редиректов Scrapy, которые заменяются нашими подклассами.
#: Оба обязательны: meta-refresh — второй способ увести запрос на чужой адрес.
_STOCK_REDIRECT_MIDDLEWARES: tuple[str, ...] = (
    "scrapy.downloadermiddlewares.redirect.RedirectMiddleware",
    "scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware",
)
#: Приоритеты взяты у стоковых middleware (``DOWNLOADER_MIDDLEWARES_BASE``).
_REDIRECT_MIDDLEWARE_PRIORITY = 600
_METAREFRESH_MIDDLEWARE_PRIORITY = 580

#: Стоковый ``RobotsTxtMiddleware`` Scrapy — заменяется подклассом с общим кешем лент.
_STOCK_ROBOTS_MIDDLEWARE = "scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware"
#: Приоритет стокового ``RobotsTxtMiddleware`` (``DOWNLOADER_MIDDLEWARES_BASE``).
_ROBOTS_MIDDLEWARE_PRIORITY = 100
#: Счётчик фактических запросов ставится **сразу после** robots: запрос, который robots
#: запретил, в сеть не уходил и HTTP-запросом не является, а сама лента
#: (``meta dont_obey_robotstxt``) проходит дальше и считается.
_HTTP_AUDIT_MIDDLEWARE_PRIORITY = 105

#: Внутренний канал «адаптер → downloader-middleware»: счётчик фактических HTTP-запросов
#: текущей попытки (:mod:`core.util.http_audit`). Не пользовательская настройка Scrapy —
#: ``CrawlerRunner`` строится на один ``fetch``, поэтому объект живёт ровно одну попытку.
#: Через ``settings``, а не через ``meta``: запрос ленты создаёт сам Scrapy (своя ``meta``),
#: и привязать его к попытке можно только уровнем краулера.
_HTTP_AUDIT_SETTING = "COHOUND_HTTP_AUDIT"

#: Общий на процесс кеш разобранных лент ``robots.txt``: ``(netloc, user_agent) -> парсер``.
#: Кешируются только **разрешённые** ленты — парсер и «ленты нет» (404/410): это наблюдение
#: origin'а. ``Deferred`` не кешируется (привязан к краулеру, который закрывается), а
#: недоступная лента (`None`) — тем более: сбой чтения не должен стать вечным разрешением.
#: Ключ включает user-agent: у разных агентов лента разная, смешивать их нельзя.
_ROBOTS_CACHE: dict[tuple[str, str], Any] = {}
_ROBOTS_CACHE_LOCK = threading.Lock()


def _clear_robots_cache() -> None:
    """Сброс кеша лент ``robots.txt`` (тесты; смена набора операторских настроек)."""
    with _ROBOTS_CACHE_LOCK:
        _ROBOTS_CACHE.clear()


@lru_cache(maxsize=1)
def _shared_robots_middleware_class() -> type:
    """Подкласс ``RobotsTxtMiddleware`` с **общим на процесс** кешем разобранных лент.

    Зачем. Стоковый middleware кеширует ленту в ``self._parsers``, а инстанс создаётся
    вместе с ``CrawlerRunner`` на каждый ``fetch`` ⇒ кеш живёт один документ. Измерено
    07.11.2026 (``docs/reports/measurements/2026-11-07-crawl-rate-tantor.md`` §7.5): у
    ``docs.tantorlabs.ru`` лента отдаёт ``301`` на чужой хост и ``404``, поэтому на каждый
    документ уходило **два** лишних запроса — на нужный хост и на чужой, вне объявленного
    лимита и вне счётчика темпа.

    **Семантика проверки не меняется**: middleware остаётся включённым на том же
    приоритете, ``process_request`` спрашивает парсер для **каждого** запроса (включая
    хопы редиректов), отказ по-прежнему ``IgnoreRequest("Forbidden by robots.txt")`` —
    его и распознаёт :func:`_is_robots_refusal` (``robots_disallow``). Кешируется только
    результат разбора ленты, а не решение по URL.

    Кешируются **только** разрешённые ленты: парсер (в том числе разобранная пустая лента —
    «правил нет», это наблюдение ``404``/``410``) и ничего больше. ``super().robot_parser``
    возвращает ``None`` при ошибке чтения (``_robots_error``), и ``None`` не кешируется:
    каждая следующая попытка спросит origin снова, то есть недоступность не превращается
    в разрешение навсегда. ``Deferred`` в общий кеш не попадает никогда — он принадлежит
    краулеру, который будет закрыт.

    Трактовка ``301``/``404`` не расходится с ``RobotsGate`` (путь ``http``,
    ``core/util/robots.py:368-375``): цепочка редиректов ленты проходится штатным
    ``RedirectMiddleware`` (с похоповой SSRF-проверкой), итоговый ответ разбирается тем же
    ``ROBOTSTXT_PARSER``; ``404`` — «ленты нет» = разрешено, недоступность — нет.
    """
    from scrapy.downloadermiddlewares.robotstxt import (
        RobotsTxtMiddleware,
    )
    from scrapy.utils.httpobj import urlparse_cached

    class _SharedRobotsTxtMiddleware(RobotsTxtMiddleware):
        """``RobotsTxtMiddleware`` + общий кеш лент по ``(origin, user-agent)``."""

        def _cache_key(self, request: Any) -> tuple[str, str]:
            """Ключ кеша: origin запроса и **тот же** user-agent, которым решается доступ.

            Агент берётся там же, где его берёт стоковый ``process_request_2``: настройка
            ``ROBOTSTXT_USER_AGENT``, иначе заголовок запроса, иначе ``USER_AGENT``
            краулера. Другой агент — другой ключ: лента для одного агента не описывает
            политику для другого.
            """
            netloc = urlparse_cached(request).netloc
            agent = self.crawler.settings.get("ROBOTSTXT_USER_AGENT")
            if not agent:
                agent = request.headers.get(
                    b"User-Agent", self.crawler.settings["USER_AGENT"]
                )
            text = agent.decode("latin-1") if isinstance(agent, bytes) else str(agent)
            return netloc, text

        async def robot_parser(self, request: Any) -> Any:
            """Парсер ленты origin'а: из общего кеша, иначе штатное чтение с запоминанием."""
            key = self._cache_key(request)
            with _ROBOTS_CACHE_LOCK:
                cached = _ROBOTS_CACHE.get(key)
            if cached is not None:
                return cached
            parser = await super().robot_parser(request)
            if parser is not None:
                with _ROBOTS_CACHE_LOCK:
                    _ROBOTS_CACHE[key] = parser
            return parser

    return _SharedRobotsTxtMiddleware


@lru_cache(maxsize=1)
def _http_audit_middleware_class() -> type:
    """Downloader-middleware, отмечающий **каждый** запрос, дошедший до загрузчика.

    Источник числа — сам загрузчик, а не форма URL: middleware вызывается на каждый
    выпускаемый запрос (лента ``robots.txt``, хоп редиректа, повтор после повтора), и
    атрибуция идёт по ``request.url`` — фактическому хосту запроса, а не хосту полосы.
    Это провенанс (``pacing.http_requests``/``foreign_hosts``), не контракт.
    """

    class _HttpAuditMiddleware:
        """Считает запросы текущей попытки в переданный счётчик."""

        def __init__(self, audit: Any) -> None:
            self._audit = audit

        @classmethod
        def from_crawler(cls, crawler: Any) -> _HttpAuditMiddleware:
            """Берёт счётчик из настроек краулера; без него middleware не нужен."""
            from scrapy.exceptions import NotConfigured

            audit = crawler.settings.get(_HTTP_AUDIT_SETTING)
            if audit is None:
                # Учёт не запрошен (кэш-попадание/юнит-тест настроек): ничего не выдумываем.
                raise NotConfigured
            return cls(audit)

        def process_request(self, request: Any) -> None:
            """Отмечает запрос перед выпуском (``None`` — пропустить дальше без изменений)."""
            self._audit.note(request.url)

    return _HttpAuditMiddleware


@lru_cache(maxsize=1)
def _redirect_guard_classes() -> tuple[type, type]:
    """Строит подклассы redirect/meta-refresh middleware с **похоповой** валидацией (R10).

    Стоковый ``RedirectMiddleware`` резолвит ``Location`` и сразу возвращает запрос —
    единственная ``validate_url`` в адаптере стоит на входном URL, поэтому редирект на
    приватный/metadata-адрес обходил SSRF-защиту. Подкласс перехватывает
    ``_redirect`` (единственную точку, через которую проходят и обычный редирект, и
    переход ``POST``→``GET``): цель проверяется ``validate_url`` до возврата запроса,
    при нарушении поднимается ``IgnoreRequest`` с меткой :data:`_REDIRECT_GUARD_MARKER`
    — запрос на небезопасный адрес **не выпускается**.

    Scrapy-классы строятся лениво (модуль импортируется и без Scrapy), поэтому фабрика,
    а не объявление на уровне модуля. ``lru_cache``: идентичность классов стабильна,
    настройки Scrapy ссылаются на них напрямую.

    Потолок хопов не вводится: действует ``REDIRECT_MAX_TIMES`` (дефолт 20), при
    превышении стоковый код поднимает ``IgnoreRequest("max redirections reached")`` и
    классификация остаётся общей (``source_unavailable``) — это не SSRF-отказ.
    """
    from scrapy.downloadermiddlewares.redirect import (
        MetaRefreshMiddleware,
        RedirectMiddleware,
    )
    from scrapy.exceptions import IgnoreRequest

    def _guard(redirected: Any) -> None:
        """Проверяет цель хопа; при отказе — ``IgnoreRequest`` (запрос не выпущен).

        Вызывается **до** построения/возврата запроса, поэтому небезопасная цель не
        доходит ни до планировщика, ни до загрузчика. Причина видна оператору текстом,
        принадлежность к SSRF-отказу несёт метка :data:`_REDIRECT_GUARD_MARKER`.
        """
        target = getattr(redirected, "url", None)
        if not target:
            return
        try:
            validate_url(str(target))
        except ValueError as exc:
            raise IgnoreRequest(f"{_REDIRECT_GUARD_MARKER}: {target!r}: {exc}") from exc

    # ``super()._redirect`` здесь недоступен: базовые классы приходят из ленивого
    # импорта, и статический анализ не видит их ``_redirect``. Вызов базы по имени
    # явный и проверяемый (`mypy`-gate: «_redirect undefined in superclass»).
    class _GuardedRedirectMiddleware(RedirectMiddleware):
        """``RedirectMiddleware`` + SSRF-проверка каждого ``Location`` (R10)."""

        def _redirect(self, redirected: Any, request: Any, reason: Any) -> Any:
            """Валидирует цель хопа до возврата запроса (R10)."""
            _guard(redirected)
            return RedirectMiddleware._redirect(self, redirected, request, reason)

    class _GuardedMetaRefreshMiddleware(MetaRefreshMiddleware):
        """``MetaRefreshMiddleware`` + та же проверка (meta-refresh — второй путь ухода)."""

        def _redirect(self, redirected: Any, request: Any, reason: Any) -> Any:
            """Валидирует цель meta-refresh до возврата запроса (R10)."""
            _guard(redirected)
            return MetaRefreshMiddleware._redirect(self, redirected, request, reason)

    return _GuardedRedirectMiddleware, _GuardedMetaRefreshMiddleware


def _is_unsafe_redirect(failure: Any) -> bool:
    """Отказал ли наш redirect-middleware (небезопасный хоп, R10).

    Предикат строгий: тип ``IgnoreRequest`` **и** префикс сообщения с меткой. Так же
    различается robots-запрет (:func:`_is_robots_refusal`): ``IgnoreRequest`` — базовый
    класс и для дубликатов, и для offsite, и для ``HttpError``.
    """
    try:
        from scrapy.exceptions import IgnoreRequest
    except ImportError:
        return False
    value = _failure_value(failure)
    if not isinstance(value, IgnoreRequest):
        return False
    return str(value).strip().startswith(_REDIRECT_GUARD_MARKER)


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
            # Best-effort останов: реактор глобальный на процесс и мог быть уже остановлен
            # (atexit идёт после явного close). Сбой остановки не должен отменять join
            # потока ниже — иначе поток остался бы жить после teardown.
            with contextlib.suppress(Exception):
                reactor.callFromThread(reactor.stop)
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
    #: Duck-typed имя транспорта (провенанс краула, Phase 2.1/S1).
    engine = "scrapy"
    #: Scrapy-краулер умеет per-request playwright-рендер (``fetch: browser``, D2).
    supports_browser = True

    def __init__(
        self,
        fetcher: FetchFn | PageFetcher | None = None,
        *,
        cache: ContentAddressedCache | None = None,
        scrapy_settings: Mapping[str, Any] | None = None,
        worker: _ReactorWorker | None = None,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    ) -> None:
        """``fetcher`` — тестовый шов; ``cache`` — глобальный кэш; ``scrapy_settings`` —
        переопределения настроек реального пути; ``worker`` — тестовый шов общего
        реактор-воркера (по умолчанию — process-глобальный singleton).

        Профили домена здесь **не** хранятся: план получения (``FetchPlan``) резолвится
        в pipeline (:func:`core.pipeline.fetch_plan.resolve_fetch_plan`) и передаётся
        готовым в :meth:`fetch`.
        """
        if max_response_bytes <= 0:
            raise ValueError(
                "max_response_bytes должен быть положительным, "
                f"получено {max_response_bytes!r}"
            )
        self._fetcher = fetcher
        self._cache = cache
        self._scrapy_settings = dict(scrapy_settings or {})
        self._worker = worker
        self._max_response_bytes = int(max_response_bytes)

    @property
    def available(self) -> bool:
        """Готов ли краулер: внедрённый фетчер или установленный Scrapy."""
        return self._fetcher is not None or _scrapy_available()

    # -- публичный контракт ---------------------------------------------------

    def fetch(self, url: str, plan: FetchPlan | None = None) -> RawPage:
        """Загружает ``url`` по готовому ``plan`` (fetch/extract/wait_for).

        Порядок: SSRF-валидация → (для HTTP) валидаторы (план → мета кэша) → кэш/живой
        фетч → (для HTTP) запись в кэш (байты + сайдкар). Браузерный рендер **не**
        консультирует и не пишет кэш: он недетерминирован, а общий ключ
        ``http_key(url)`` дал бы кросс-контаминацию HTTP/browser по одному URL.
        ``plan=None`` — дефолтный план, построенный здесь (дефолт-объект в сигнатуре был
        бы общим на все вызовы).
        """
        if plan is None:
            plan = FetchPlan()
        try:
            validate_url(url)
        except ValueError as exc:
            raise CrawlUrlError(f"URL отклонён SSRF-валидацией: {exc}") from exc

        use_cache = plan.fetch is FetchMode.HTTP
        key = http_key(url)
        cached: bytes | None = None
        meta: dict[str, Any] | None = None
        effective_plan = plan

        if use_cache and self._cache is not None:
            cached = self._cache.get_bytes("http", key)
            meta = self._cache.get_meta("http", key)
            # Приоритет валидаторов: план → мета кэша → безусловный GET. Явные валидаторы
            # плана не переопределяем; иначе берём валидаторы прошлого получения из
            # сайдкара, чтобы запрос дошёл до сервера и мог вернуть дешёвый ``304``.
            # План вызывающего не мутируем.
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
            if cached is not None and not conditional:
                return self._page_from_cache(url, cached, plan.fetch)

        page = self._fetch_live(url, effective_plan, cached)
        if use_cache and not page.not_modified:
            self._store(url, key, page)
        return page

    # -- кэш ------------------------------------------------------------------

    def _page_from_cache(self, url: str, content: bytes, fetch: FetchMode) -> RawPage:
        """Собирает ``RawPage`` из кэшированных байтов.

        Кэшируются только байты, поэтому ``status_code``/``content_type`` в
        попадании пусты, а ``fetched_at`` берётся из mtime файла кэша (момент
        первого фетча) — это сохраняет детерминизм R7 между ``run_id``.
        """
        return RawPage(
            url=url,
            content=content,
            fetched_at=self._cache_mtime_iso(url),
            fetch_mode=fetch,
        )

    def _cache_mtime_iso(self, url: str) -> str:
        """ISO-8601 (UTC) mtime файла кэша; при отсутствии — текущее время."""
        # S101: несуществующая ветка — инвариант конструктора (метод зовётся только
        # там, где кэш внедрён: ``fetched_at`` берётся из mtime файла кэша, без кэша
        # записи времени неоткуда взять). ``assert`` нужен ещё и как сужение типа для
        # mypy-gate (``self._cache`` объявлен Optional): ``if`` там же сузил бы, но
        # тогда «недостижимо» перестало бы быть проверяемым утверждением.
        assert self._cache is not None  # noqa: S101 — инвариант конструктора
        try:
            path = self._cache.path_for("http", http_key(url), ".bin")
            stamp = path.stat().st_mtime
        except (OSError, ValueError):
            return _now_iso()
        return datetime.fromtimestamp(stamp, tz=UTC).isoformat()

    def _store(self, url: str, key: str, page: RawPage) -> None:
        """Пишет байты и сайдкар валидаторов в глобальный кэш (R7), если кэш внедрён.

        Сайдкар обязателен для цикла условных запросов: без него следующее получение
        не сможет предъявить ``ETag``/``Last-Modified`` и всегда будет полным ``200``.
        Состав совпадает с HTTP-адаптером (``crawl_http.py``).
        """
        if self._cache is None:
            return
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

    # -- фетч -----------------------------------------------------------------

    def _fetch_live(self, url: str, plan: FetchPlan, cached: bytes | None = None) -> RawPage:
        """Живой фетч: внедрённый ``fetcher`` или реальный Scrapy-путь.

        ``cached`` — байты текущей кэш-записи: тело ответа ``304`` берётся только из них.
        """
        if self._fetcher is not None:
            try:
                page = self._call_fetcher(self._fetcher, url, plan)
            except CrawlError:
                raise
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise CrawlError(f"фетч {url!r} не удался: {exc}") from exc
            self._check_size(url, page.content, plan)
            return page
        return self._fetch_with_scrapy(url, plan, cached)

    def _cap_for(self, plan: FetchPlan) -> int:
        """Эффективный потолок: per-plan (site/run) поверх дефолта адаптера."""
        if plan.max_response_bytes is not None:
            return int(plan.max_response_bytes)
        return self._max_response_bytes

    def _check_size(self, url: str, content: bytes, plan: FetchPlan) -> None:
        """Единая проверка размера для внедрённого шва (реальный путь режет загрузчик)."""
        cap = self._cap_for(plan)
        if len(content) > cap:
            raise CrawlPayloadTooLarge(
                f"ответ {url!r}: {len(content)} байт > лимита {cap} (payload_too_large)"
            )

    @staticmethod
    def _call_fetcher(fetcher: FetchFn | PageFetcher, url: str, plan: FetchPlan) -> RawPage:
        """Вызывает внедрённый фетчер: объект с ``fetch`` или callable."""
        fetch_attr = getattr(fetcher, "fetch", None)
        if callable(fetch_attr):
            return cast(RawPage, fetch_attr(url, plan))
        if callable(fetcher):
            return cast(RawPage, fetcher(url, plan))
        raise CrawlError("внедрённый fetcher не callable и не имеет метода fetch")

    def _fetch_with_scrapy(
        self, url: str, plan: FetchPlan, cached: bytes | None = None
    ) -> RawPage:
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

        settings = self._build_settings(plan)
        # Условные заголовки имеют смысл только для HTTP-пути (у браузерного рендера
        # нет кэш-записи для тела 304).
        headers = _conditional_headers(plan) if plan.fetch is FetchMode.HTTP else {}
        spec = _CrawlSpec(
            url=url,
            fetch=plan.fetch,
            wait_for=plan.wait_for,
            headers=headers,
            cached=cached,
            robots=bool(plan.robots),
        )
        fut: Future[bool] = Future()
        timeout_s = self._submit_timeout(settings)
        self._worker_instance().submit(settings, spec, fut, timeout_s)

        if spec.error:
            if spec.unsafe_redirect:
                # R10: редирект вёл на небезопасный адрес — запрос не выпускался.
                # Тип тот же, что у отказа входного URL (``CrawlUrlError``): для
                # pipeline это отказ SSRF-валидации, а не наблюдение краула.
                raise CrawlUrlError(
                    spec.error,
                    http_status=spec.http_status,
                )
            if spec.error_kind == FailureKind.ROBOTS_DISALLOW.value:
                # Отказ по политике, а не сбой транспорта: ``CrawlRefused`` несёт
                # ``refusal_reason``, по которому pipeline пишет ``robots_disallow``
                # (блокирующий класс, I2c/Ф0) и не расширяет frontier.
                raise CrawlRefused(
                    spec.error,
                    refusal_reason=spec.error_kind,
                    http_status=spec.http_status,
                )
            if spec.error_kind == FailureKind.NOT_FOUND.value:
                raise CrawlNotFound(
                    spec.error,
                    failure_kind=spec.error_kind,
                    http_status=spec.http_status,
                )
            if spec.error_kind == FailureKind.PAYLOAD_TOO_LARGE.value:
                raise CrawlPayloadTooLarge(
                    spec.error,
                    failure_kind=spec.error_kind,
                    http_status=spec.http_status,
                )
            raise CrawlError(spec.error, http_status=spec.http_status)
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

    def _build_settings(self, plan: FetchPlan) -> dict[str, Any]:
        """Итоговые настройки Scrapy: дефолты → потолок загрузки → playwright → overrides.

        **Потолок загрузки — по классу плана, а не один.** Scrapy режет ответ
        ``DOWNLOAD_MAXSIZE`` **до** того, как тело дойдёт до приёмника, поэтому потолок
        обязан покрывать оба класса:

        * план **допускает артефакты** (``staging_dir`` + ``max_stream_bytes``) →
          ``DOWNLOAD_MAXSIZE = max(max_response_bytes, max_stream_bytes)``: иначе
          ``zip``/``pdf``/office убивался бы страничным потолком (20 MiB) и файловый приём
          был бы недостижим (наблюдалось: все ``payload_too_large`` — контейнеры);
        * иначе → ``max_response_bytes`` (per-plan поверх дефолта адаптера 20 MiB).

        ``max`` — потому что ``DOWNLOAD_MAXSIZE`` общий для всех запросов краулера:
        меньший ``max_stream_bytes`` не должен понижать страничный потолок.
        ``DOWNLOAD_WARNSIZE`` не выше потолка. Явные ``scrapy_settings`` оператора
        по-прежнему побеждают (обратная совместимость).

        ``304`` добавляется в ``HTTPERROR_ALLOWED_CODES`` **после** переопределений
        оператора: без этого ``HttpErrorMiddleware`` поднимает ``HttpError`` на
        не-2xx, ``304`` уходит в errback и ветка «байты из кэша» недостижима (на живом
        прогоне так потерялись 240 активных документов). Разрешён ровно ``304`` —
        404/410 и прочие не-2xx обязаны остаться на errback-пути с прежней
        классификацией.

        Там же (после переопределений) ставятся middleware политики и учёта:
        redirect с похоповой валидацией (R10, см. :meth:`_install_redirect_guard`),
        подкласс ``RobotsTxtMiddleware`` с общим кешем лент (:meth:`_install_robots_cache`)
        и счётчик фактических HTTP-запросов (:meth:`_install_http_audit`).
        """
        settings = dict(DEFAULT_SCRAPY_SETTINGS)
        cap = self._cap_for(plan)
        if plan.staging_dir is not None and plan.max_stream_bytes is not None:
            cap = max(cap, int(plan.max_stream_bytes))
        settings["DOWNLOAD_MAXSIZE"] = cap
        settings["DOWNLOAD_WARNSIZE"] = min(cap, 33_554_432)
        if plan.fetch is FetchMode.BROWSER:
            settings.update(_PLAYWRIGHT_SETTINGS)
        settings.update(self._scrapy_settings)
        allowed = settings.get("HTTPERROR_ALLOWED_CODES") or []
        if isinstance(allowed, (str, int)):
            allowed = str(allowed).split(",")
        statuses: list[int] = []
        for item in allowed:
            try:
                statuses.append(int(item))
            except (TypeError, ValueError):
                continue  # нечисловой код статуса всё равно не совпадёт
        if 304 not in statuses:
            statuses.append(304)
        settings["HTTPERROR_ALLOWED_CODES"] = statuses
        self._install_redirect_guard(settings)
        self._install_robots_cache(settings)
        self._install_http_audit(settings)
        return settings

    @staticmethod
    def _install_robots_cache(settings: dict[str, Any]) -> None:
        """Заменяет стоковый ``RobotsTxtMiddleware`` подклассом с общим кешем лент.

        Ставится **после** переопределений оператора — по той же логике, что redirect
        guard: политика §10 не должна теряться из-за забывчивости конфига. Стоковый
        отключается (``None`` — штатный способ Scrapy убрать компонент из
        ``DOWNLOADER_MIDDLEWARES_BASE``), подкласс занимает его приоритет.
        """
        middleware = dict(settings.get("DOWNLOADER_MIDDLEWARES") or {})
        middleware[_STOCK_ROBOTS_MIDDLEWARE] = None
        middleware[_shared_robots_middleware_class()] = _ROBOTS_MIDDLEWARE_PRIORITY
        settings["DOWNLOADER_MIDDLEWARES"] = middleware

    @staticmethod
    def _install_http_audit(settings: dict[str, Any]) -> None:
        """Ставит счётчик фактических HTTP-запросов, если попытка его запросила.

        Попытку открывает прибор темпа (:meth:`core.pipeline.crawl_pacing.CrawlPacing.request`);
        счётчик берётся из ``threading.local()`` вызывающего потока — движок исполняется в
        потоке реактора, поэтому объект передаётся явно через настройки краулера.
        Попытки нет (прямой вызов адаптера, кэш-попадание) — middleware не ставится:
        «не измеряли» честнее выдуманного нуля.
        """
        audit = current_attempt()
        if audit is None:
            return
        settings[_HTTP_AUDIT_SETTING] = audit
        middleware = dict(settings.get("DOWNLOADER_MIDDLEWARES") or {})
        middleware[_http_audit_middleware_class()] = _HTTP_AUDIT_MIDDLEWARE_PRIORITY
        settings["DOWNLOADER_MIDDLEWARES"] = middleware

    @staticmethod
    def _install_redirect_guard(settings: dict[str, Any]) -> None:
        """Ставит redirect-middleware с похоповой валидацией вместо стокового (R10).

        Стоковые ``RedirectMiddleware``/``MetaRefreshMiddleware`` заменяются подклассами
        с :func:`_redirect_guard_classes` на их же приоритетах (600/580), а сами стоковые
        отключаются (``None``) — иначе редирект проходил бы мимо проверки. Ставится
        **после** переопределений оператора: отказ от проверки не должен получаться
        забывчивостью конфига (та же логика, что у ``304`` выше).

        ``None`` в ``DOWNLOADER_MIDDLEWARES`` — штатный способ Scrapy выключить
        middleware из ``DOWNLOADER_MIDDLEWARES_BASE``.
        """
        guarded_redirect, guarded_metarefresh = _redirect_guard_classes()
        middleware = dict(settings.get("DOWNLOADER_MIDDLEWARES") or {})
        for name in _STOCK_REDIRECT_MIDDLEWARES:
            middleware[name] = None
        middleware[guarded_redirect] = _REDIRECT_MIDDLEWARE_PRIORITY
        middleware[guarded_metarefresh] = _METAREFRESH_MIDDLEWARE_PRIORITY
        settings["DOWNLOADER_MIDDLEWARES"] = middleware
