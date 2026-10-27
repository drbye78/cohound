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
* **redirects — похоповая валидация на транспорте** (R10). ``follow_redirects=False``
  для ``httpx``: цикл ``Location`` ведёт адаптер, и **каждый** хоп проходит
  ``validate_url`` до выпуска запроса (образец — ``crawl_browser.py``). Запрос на
  небезопасную цель не выпускается, а байты её ответа не читаются и **не кэшируются**:
  запись в ``cache/http`` происходит только после того, как финальный URL прошёл
  проверку. Потолок хопов — объявленный параметр ``max_redirects``
  (:data:`DEFAULT_MAX_REDIRECTS` = 20, ровно дефолт ``httpx``). ``RawPage.url`` —
  финальный URL (``str(response.url)``), ``status_code``/``content_type`` — из ответа.
  Куки, поставленные хопом, переносятся в следующий — с привязкой к поставившему хосту
  (``httpx``-клиент делал это сам, пока цикл был внутри одного вызова);
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

**Ошибки.** Таксономия объявлена в порту — ``core.ports.crawl_errors`` (R5), и этот
модуль её только поднимает: ``CrawlError`` — единый тип для всех транспортов (pipeline
ловит один класс); отсутствие ``httpx`` — ``CrawlUnavailable(CrawlError)``. Отказ SSRF —
``CrawlUrlError`` (и на входном URL, и на любом хопе редиректа). Ответ ``404``/``410`` —
``CrawlNotFound`` (``failure_kind="not_found"``): ресурса нет, а не источник недоступен.

Нейтрален (§1.3): в модуле нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import contextlib
import importlib.util
import os
import tempfile
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from http.cookies import CookieError, SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

from core.domain.models import DEFAULT_MAX_RESPONSE_BYTES
from core.ports.crawl_errors import (
    NOT_FOUND_STATUSES,
    CrawlError,
    CrawlNotFound,
    CrawlPayloadTooLarge,
    CrawlRefused,
    CrawlUnavailable,
    CrawlUrlError,
)
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.cache import ContentAddressedCache, http_key
from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    is_terminal_artifact,
)
from core.util.robots import RobotsGate
from core.util.security import validate_url

__all__ = [
    "DEFAULT_ACCEPT",
    "DEFAULT_ACCEPT_LANGUAGE",
    "DEFAULT_MAX_REDIRECTS",
    "DEFAULT_MAX_RESPONSE_BYTES",
    "DEFAULT_TIMEOUT",
    "DEFAULT_USER_AGENT",
    "HttpCrawler",
    "HttpGetFn",
]

#: Внедряемый шов: ``(url) -> сырые байты страницы``.
HttpGetFn = Callable[[str], bytes]

#: Таймаут HTTP-запроса, с.
DEFAULT_TIMEOUT = 30.0
#: Потолок числа редиректов на один фетч. Значение — ровно дефолт ``httpx``
#: (``httpx._config.DEFAULT_MAX_REDIRECTS``): ручной цикл по ``Location`` не вводит
#: нового поведения, а переносит валидацию каждого хопа на транспорт (R10).
DEFAULT_MAX_REDIRECTS = 20
#: Статусы, на которых транспорт делает следующий хоп (набор ``httpx``).
_REDIRECT_STATUSES: frozenset[int] = frozenset({301, 302, 303, 307, 308})
#: User-Agent по умолчанию (описательный; можно переопределить/отключить ``None``).
DEFAULT_USER_AGENT = "coHound-crawler/0.1"
#: Заголовки контент-согласования обычной HTML-страницы.
DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
DEFAULT_ACCEPT_LANGUAGE = "ru,en;q=0.7"


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` живого фетча)."""
    return datetime.now(UTC).isoformat()


def _host_of(url: str) -> str:
    """Хост URL в нижнем регистре (пустая строка, если хост не читается)."""
    return (urlsplit(url).hostname or "").lower()


def _collect_cookies(response: Any, jar: dict[str, str]) -> None:
    """Складывает ``Set-Cookie`` ответа в jar хоста ``name → value``.

    Разбирается stdlib-ом (:class:`http.cookies.SimpleCookie`), а не регуляркой: значение
    куки может содержать ``;``/``=`` внутри кавычек. ``get_list`` предпочитается
    ``get``: у ``httpx`` несколько ``Set-Cookie`` схлопываются в один заголовок с
    запятыми, а ``Expires`` их тоже содержит — разбор «по запятой» испортил бы значение.
    """
    headers = response.headers
    get_list = getattr(headers, "get_list", None)
    if callable(get_list):
        raw_values: list[str] = [str(value) for value in get_list("Set-Cookie")]
    else:  # дублёр ответа в тестах (плоский dict)
        single = headers.get("Set-Cookie")
        raw_values = [str(single)] if single else []
    for raw in raw_values:
        parsed = SimpleCookie()
        try:
            parsed.load(raw)
        except CookieError:
            # Битая кука не должна ронять фетч: она не относится к представлению.
            continue
        for name, morsel in parsed.items():
            jar[name] = morsel.value


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
    :param follow_redirects: следовать ли HTTP-редиректам (по умолчанию да). ``False``
        оставляет ответ ``3xx`` как есть — хопов тогда нет и валидировать нечего.
    :param max_redirects: потолок числа хопов (дефолт :data:`DEFAULT_MAX_REDIRECTS`);
        превышение — ``CrawlError``, как ``TooManyRedirects`` у ``httpx``.
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
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        robots: RobotsGate | None = None,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    ) -> None:
        if fetcher is not None and http_get is not None:
            raise ValueError("задайте только один шов: fetcher или http_get")
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        if max_redirects < 0:
            raise ValueError(
                f"max_redirects не может быть отрицательным, получено {max_redirects!r}"
            )
        if max_response_bytes <= 0:
            raise ValueError(
                "max_response_bytes должен быть положительным, "
                f"получено {max_response_bytes!r}"
            )
        self._fetcher = fetcher if fetcher is not None else http_get
        self._cache = cache
        self.timeout = timeout
        self.user_agent = user_agent
        self.follow_redirects = follow_redirects
        self.max_redirects = int(max_redirects)
        # D1/§10: per-host политика robots.txt (egress-слой). ``None`` → без проверки
        # (обратная совместимость); тесты инъектят политику без сети.
        self._robots = robots
        self._max_response_bytes = int(max_response_bytes)

    @property
    def available(self) -> bool:
        """Готов ли краулер: внедрённый шов или установленный ``httpx``."""
        return self._fetcher is not None or _httpx_available()

    # -- публичный контракт ---------------------------------------------------

    def fetch(self, url: str, plan: FetchPlan | None = None) -> RawPage:
        """Загружает ``url``: проверка плана → SSRF → валидаторы → кэш/живой фетч → кэш.

        Валидаторы берутся из плана, иначе — из сайдкара кэша (см. модульный докстринг):
        при наличии любых валидаторов запрос уходит на сервер (возможен ``304``).
        ``FetchMode.BROWSER`` этим адаптером не поддерживается (понятная ошибка).

        ``plan=None`` — дефолтный план (:class:`FetchPlan` без осей): строится здесь, а не
        в сигнатуре, чтобы дефолт не был общим объектом на все вызовы и чтобы
        ``fetch(url, None)`` читался как «план не задан», а не падал на ``None.fetch``.
        """
        if plan is None:
            plan = FetchPlan()
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
        # P6: staging-артефакт в память не читался — ``page.content`` пуст, и запись его
        # в кэш отравила бы ключ пустым телом. Источник истины для такого ответа —
        # retention (``corpus_raw``, I19), а не URL-кэш.
        # R10: ``_fetch_live`` возвращает страницу только после валидации **каждого**
        # хопа, поэтому запись ниже означает «финальный URL прошёл проверку». Отказ на
        # любом хопе поднимает исключение выше — байты недоступного адреса не кэшируются.
        if self._cache is not None and not page.not_modified and page.staged_path is None:
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
            self._check_size(url, content, plan)
            return RawPage(
                url=url,
                content=content,
                fetched_at=_now_iso(),
                fetch_mode=FetchMode.HTTP,
            )
        return self._get_real(url, plan)

    def _cap_for(self, plan: FetchPlan) -> int:
        """Эффективный потолок: per-plan (site/run) поверх дефолта адаптера."""
        if plan.max_response_bytes is not None:
            return int(plan.max_response_bytes)
        return self._max_response_bytes

    def _check_size(self, url: str, content: bytes, plan: FetchPlan) -> None:
        """Проверка размера для внедрённого шва (реальный путь режется потоково)."""
        cap = self._cap_for(plan)
        if len(content) > cap:
            raise CrawlPayloadTooLarge(
                f"ответ {url!r}: {len(content)} байт > лимита {cap} (payload_too_large)"
            )

    def _get_real(self, url: str, plan: FetchPlan) -> RawPage:
        """Потоковый GET с **похоповой** валидацией редиректов (R10).

        ``follow_redirects=False`` у ``httpx``: цикл ведёт адаптер, и каждый ``Location``
        разрешается относительно текущего URL и проходит ``validate_url`` **до** выпуска
        запроса. Это одна точка для всех HTTP-путей (краул, ``robots.txt``, реестры,
        discovery-проба, SERP-фетч, шов ``/v1/fetch``): проверка живёт на транспорте, а
        не в пайплайне. Отказ хопа — ``CrawlUrlError`` (тот же тип, что у браузерного
        транспорта), байты недоступного адреса не читаются.

        Потолок хопов — ``max_redirects`` (дефолт :data:`DEFAULT_MAX_REDIRECTS`, ровно
        дефолт ``httpx``); превышение — ``CrawlError``, как ``TooManyRedirects``.

        Тело **не** буферизуется целиком до проверки: ``Content-Length`` проверяется до
        чтения, а поток обрывается сразу при превышении потолка. Потолки два:

        * ``max_response_bytes`` — **in-memory** потолок страницы. Свыше него тело уходит
          в staging-файл **только** если план допускает терминальный артефакт
          (``staging_dir`` + ``max_stream_bytes`` и класс «документ или заявленный
          контейнер»), иначе — ``payload_too_large`` (прежнее поведение);
        * ``max_stream_bytes`` — потоковый потолок допущенного артефакта; выше него —
          ``payload_too_large`` с удалением staging-файла.

        Память при staging ограничена одним чанком транспорта: файл пишется по мере
        чтения, ``content`` остаётся пустым, ``head`` — первые 8 KiB.
        """
        httpx = self._import_http()
        cap = self._cap_for(plan)
        headers = self._headers(plan)
        # Наблюдаемость: статус ответа, если транспорт успел его увидеть (см. CrawlError).
        # Заполняется сразу после входа в потоковый ответ; None — ответа не было.
        observed_status: int | None = None
        # ``url`` — **запрошенный** URL: он же ключ кэша (``http_key``) и предмет записи
        # в сайдкар. ``current`` — текущий хоп, который и валидируется/фетчится.
        current = url
        hops = 0
        # Куки цепочки редиректов. ``httpx``-клиент переносил их сам (доменно-скоупед
        # jar внутри одного вызова); ручной цикл обязан сделать это явно, иначе штатный
        # сценарий «302 + Set-Cookie» (JS-challenge публичной страницы) оборвался бы на
        # первом хопе. Jar ключуется хостом: чужому хосту куки не отдаём, а отклонённому
        # (SSRF) — тем более.
        jar: dict[str, dict[str, str]] = {}
        try:
            while True:
                hop_host = _host_of(current)
                with httpx.stream(
                    "GET",
                    current,
                    timeout=self.timeout,
                    follow_redirects=False,
                    headers=headers,
                    cookies=jar.get(hop_host) or None,
                ) as response:
                    target = self._redirect_target(current, response)
                    if target is not None:
                        hops += 1
                        if hops > self.max_redirects:
                            raise CrawlError(
                                f"превышено число редиректов ({self.max_redirects}) "
                                f"при фетче {url!r}"
                            )
                        _collect_cookies(response, jar.setdefault(hop_host, {}))
                        current = target
                        # ``continue`` выходит из ``with`` (поток закрывается) и берёт
                        # следующий хоп уже провалидированным.
                        continue
                    if int(response.status_code) == 304:
                        return self._page_not_modified(url, response)
                    # 404/410 — определённый ответ «ресурса нет», а не недоступность
                    # источника. Классифицируем ДО ``raise_for_status``: иначе статус
                    # схлопнулся бы в бесцветный ``CrawlError`` и pipeline записал бы
                    # ``source_unavailable``.
                    status = int(response.status_code)
                    observed_status = status
                    if status in NOT_FOUND_STATUSES:
                        raise CrawlNotFound(
                            f"HTTP {status} для {current!r}: ресурс отсутствует",
                            http_status=status,
                        )
                    try:
                        response.raise_for_status()
                    except CrawlError:
                        raise
                    except Exception as exc:  # noqa: BLE001 - статус известен, класс общий
                        # 403/429/503 и прочие не-2xx: класс отказа прежний
                        # (``source_unavailable``), но статус теперь виден оператору.
                        raise CrawlError(
                            f"HTTP-фетч {current!r} не удался: {exc}",
                            http_status=status,
                        ) from exc
                    # P6: класс ответа известен только здесь (заголовки получены): страница —
                    # прежний in-memory потолок, допущенный артефакт — staging-файл.
                    staging_dir = self._staging_path(
                        plan,
                        url=current,
                        content_type=response.headers.get("Content-Type"),
                    )
                    ceiling = (
                        cap
                        if staging_dir is None
                        else max(cap, int(plan.max_stream_bytes or 0))
                    )
                    declared = response.headers.get("Content-Length")
                    declared_size: int | None = None
                    if declared is not None:
                        try:
                            declared_size = int(declared)
                        except (TypeError, ValueError):
                            declared_size = None
                        if declared_size is not None and declared_size > ceiling:
                            raise CrawlPayloadTooLarge(
                                f"ответ {current!r}: Content-Length {declared_size} байт "
                                f"> лимита {ceiling} (payload_too_large)"
                            )
                    content, staged_path, head = self._read_stream(
                        current,
                        response,
                        plan,
                        cap,
                        staging_dir,
                        # Известный размер свыше in-memory потолка — сразу в файл: не
                        # буферизуем ``cap`` байт ради последующей записи их на диск.
                        stage_first=(
                            staging_dir is not None
                            and declared_size is not None
                            and declared_size > cap
                        ),
                    )
                    return self._page_from_response(
                        response, content, staged_path=staged_path, head=head
                    )
        except CrawlError:
            raise
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise CrawlError(
                f"HTTP-фетч {url!r} не удался: {exc}",
                http_status=observed_status,
            ) from exc

    def _redirect_target(self, current: str, response: Any) -> str | None:
        """Цель следующего хопа или ``None`` (это не редирект / редиректы выключены).

        Возвращает **уже провалидированный** абсолютный URL: ``Location`` разрешается
        относительно ``current`` (:func:`urllib.parse.urljoin`) и проходит
        ``validate_url``. Небезопасная цель — ``CrawlUrlError``: запрос на неё не
        выпускается, её байты не читаются и не кэшируются (R10). Тип совпадает с
        отказом браузерного транспорта (``crawl_browser._default_http_get``) и с
        отказом входного URL, поэтому pipeline ловит один класс на все три случая.

        ``follow_redirects=False`` — прежняя семантика: ответ ``3xx`` отдаётся как есть,
        хопов нет, проверять нечего.
        """
        if not self.follow_redirects:
            return None
        try:
            status = int(response.status_code)
        except (TypeError, ValueError):
            return None
        if status not in _REDIRECT_STATUSES:
            return None
        location = response.headers.get("Location")
        if not location:
            # ``3xx`` без ``Location`` — не хоп: остаётся обычным ответом (как у httpx).
            return None
        target = urljoin(current, str(location))
        try:
            validate_url(target)
        except ValueError as exc:
            raise CrawlUrlError(
                f"редирект на небезопасный URL отклонён SSRF-валидацией: {exc}"
            ) from exc
        return target

    def _staging_path(
        self, plan: FetchPlan, *, url: str, content_type: str | None = None
    ) -> Path | None:
        """Каталог staging, если план допускает файловый приём **терминального артефакта**.

        Оба поля плана обязательны: ``staging_dir`` без ``max_stream_bytes`` не даёт
        потолка, а значит не даёт и права писать файл. Класс — «терминальный артефакт»
        (:func:`core.util.document_formats.is_terminal_artifact`): документ
        (``pdf``/``docx``/``pptx``/``xlsx``) **или** заявленный контейнер (URL-суффикс либо
        ``Content-Type``). Страница (HTML) свыше ``max_response_bytes`` по-прежнему получает
        ``payload_too_large`` байт-в-байт: её байты продолжают навигацию и в файл не идут.
        """
        if plan.staging_dir is None or plan.max_stream_bytes is None:
            return None
        if not is_terminal_artifact(
            url, content_type, plan.attachment_formats
        ):
            return None
        return Path(plan.staging_dir)

    def _read_stream(
        self,
        url: str,
        response: Any,
        plan: FetchPlan,
        cap: int,
        staging_dir: Path | None,
        *,
        stage_first: bool = False,
    ) -> tuple[bytes, str | None, bytes | None]:
        """Читает поток в память **или** в staging-файл (переключение на лету).

        Возвращает ``(content, staged_path, head)``: при staging ``content == b""``,
        ``staged_path`` — файл, ``head`` — первые 8 KiB (сниф без чтения файла). При
        обычном пути ``staged_path``/``head`` — ``None`` (байт-в-байт прежнее поведение).
        Неизвестный ``Content-Length`` переключает на файл по факту превышения ``cap``;
        известный размер свыше ``cap`` (``stage_first``) — сразу в файл, без буферизации
        ``cap`` байт.
        """
        limit = (
            int(plan.max_stream_bytes)
            if staging_dir is not None and plan.max_stream_bytes is not None
            else None
        )
        buffer = bytearray()
        head = bytearray()
        staged_path: str | None = None
        handle: Any = None
        total = 0
        try:
            if stage_first:
                # S101: политика вызывающего (``stage_first`` вычисляется как
                # ``staging_dir is not None and ...`` в единственной точке вызова) — ветка
                # недостижима, а ``assert`` ещё и сужает тип для mypy-gate.
                assert staging_dir is not None  # noqa: S101 — см. выше, недостижимо
                staged_path, handle = self._open_stage(staging_dir)
            for chunk in response.iter_bytes():
                total += len(chunk)
                if len(head) < BINARY_SNIFF_BYTES:
                    head.extend(chunk[: BINARY_SNIFF_BYTES - len(head)])
                if staged_path is None and total <= cap:
                    buffer.extend(chunk)
                    continue
                if staged_path is None:
                    # Переключение на файл: потолок памяти уже превышен, но артефакт
                    # вправе быть больше страницы.
                    if staging_dir is None or limit is None:
                        raise CrawlPayloadTooLarge(
                            f"ответ {url!r}: тело превысило лимит {cap} байт "
                            "(payload_too_large)"
                        )
                    if total > limit:
                        raise CrawlPayloadTooLarge(
                            f"ответ {url!r}: тело превысило лимит артефакта {limit} "
                            "байт (payload_too_large)"
                        )
                    staged_path, handle = self._open_stage(staging_dir)
                    handle.write(buffer)
                    buffer = bytearray()
                elif limit is not None and total > limit:
                    raise CrawlPayloadTooLarge(
                        f"ответ {url!r}: тело превысило лимит артефакта {limit} "
                        "байт (payload_too_large)"
                    )
                handle.write(chunk)
        except BaseException:
            if handle is not None:
                handle.close()
            if staged_path is not None:
                # Уборка staging-файла — best-effort: исходная ошибка фетча важнее, и
                # подменять её сбоем ``unlink`` нельзя (файл уже мог быть снят, каталог —
                # принадлежать вызывающему). Видимость отказа обеспечена исходным raise.
                with contextlib.suppress(OSError):
                    Path(staged_path).unlink(missing_ok=True)
            raise
        else:
            if handle is not None:
                handle.close()
        if staged_path is not None:
            return b"", staged_path, bytes(head)
        return bytes(buffer), None, None

    @staticmethod
    def _open_stage(staging_dir: Path) -> tuple[str, Any]:
        """Создаёт staging-файл в каталоге вызывающего (``mkstemp`` — без гонок имён)."""
        staging_dir.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(
            dir=str(staging_dir), prefix="stage-", suffix=".part"
        )
        return name, os.fdopen(fd, "wb")

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
    def _page_from_response(
        response: Any,
        content: bytes | None = None,
        *,
        staged_path: str | None = None,
        head: bytes | None = None,
    ) -> RawPage:
        """Отображает объект ответа ``httpx`` в ``RawPage`` (финальный URL, статус, тип).

        ``content`` — уже прочитанное потоковое тело; ``None`` (прямой вызов/тест) —
        ``response.content``. Заголовки ``ETag``/``Last-Modified`` сохраняются как
        валидаторы для следующего условного запроса. ``staged_path``/``head`` — файловый
        приём артефакта (P6): при заданном ``staged_path`` тело в память не попадает.
        """
        raw_type = response.headers.get("Content-Type")
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        body = response.content if content is None else content
        return RawPage(
            url=str(response.url),
            content=body,
            fetched_at=_now_iso(),
            fetch_mode=FetchMode.HTTP,
            status_code=response.status_code,
            content_type=raw_type or None,
            etag=etag or None,
            last_modified=last_modified or None,
            staged_path=staged_path,
            head=head,
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
