"""Внешний поиск (SERP) через библиотеку ``ddgs`` (метапоиск DuckDuckGo).

Роль: commodity-слой внешнего поиска **вне оценщика** (I18). Выход адаптера — только
URL-кандидаты (``SearchResult``) и, для ``fetch``, сырые байты страницы. Это **не**
разметка статусов: найденное становится материалом лишь дальше по конвейеру Ш5
(``corpus_raw`` → extract → chunk → ``content_hash`` → index, I19).

**I19 — структурная гарантия.** Модуль намеренно не импортирует домен/хранилище и не
конструирует объекты-доказательства/ячейки: их здесь физически нет по построению.
``search`` возвращает кандидатов **без байтов страницы** (``raw_bytes=None``);
контент получает ``fetch(url)``, который лишь кладёт сырые байты в retention/кэш.
Хэширование, нарезку и индексацию выполняет pipeline (Ш5), а не адаптер.

**Ленивый ``ddgs`` и ``httpx``.** Модуль импортируется и тестируется без этих пакетов:
библиотека подтягивается только в реальном пути поиска, HTTP-клиент — только в реальном
``fetch``; ``available`` показывает готовность.

**Изолированный шов для тестов.** Конструктор принимает ``search_fn``
(``(query, limit, region) -> list[dict]``) и ``http_get`` (``(url) -> bytes``). Если
заданы — реальные библиотека/сеть не задействуются. Иначе — реальные ``ddgs``/``httpx``.

**Язык/регион.** ``lang`` запроса отображается в регион библиотеки данными, не кодом
предметной логики: ``ru`` → ``ru-ru``, ``en`` → ``us-en``; прочее → ``ru-ru``.
Поиск ограничивается движком DuckDuckGo (``backend``) для детерминизма и скорости;
имя движка — данные-конфигурация.

**Кэш (R7).** ``fetch`` консультирует внедрённый ``ContentAddressedCache`` по
``http_key(url)``: попадание → ``RawPage`` из кэшированных байтов без сети, промах →
живой GET + запись. Ответы поиска адаптер не кэширует: нормализация запроса (включая
язык) и его кэширование — за pipeline (T11.2).

Нейтрален (§1.3): имя провайдера — данные-конфигурация, в модуле нет названий
продуктов/категорий.
"""

from __future__ import annotations

import importlib.util
import os
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from core.ports.search import SearchQuery, SearchResult
from core.ports.types import RawPage, FetchMode
from core.util.cache import ContentAddressedCache, http_key
from core.util.security import validate_url

__all__ = [
    "DEFAULT_BACKEND",
    "DEFAULT_REGION",
    "DEFAULT_TIMEOUT",
    "DuckDuckGoSearchAdapter",
    "HttpGetFn",
    "SearchAdapterUnavailable",
    "SearchError",
    "SearchFn",
]

#: Движок библиотеки ``ddgs`` по умолчанию (данные; не предметная категория).
DEFAULT_BACKEND = "duckduckgo"
#: Регион библиотеки по умолчанию, если язык запроса неизвестен (данные).
DEFAULT_REGION = "ru-ru"
#: Таймаут сетевого поиска/фетча, с.
DEFAULT_TIMEOUT = 20.0

#: Переменная среды, переопределяющая движок библиотеки (данные-конфигурация).
ENV_BACKEND = "DUCKDUCKGO_BACKEND"

#: Внедряемая функция поиска: (query, limit, region) → список записей провайдера.
SearchFn = Callable[[str, int, str], list[dict[str, Any]]]
#: Внедряемая функция GET для ``fetch``: (url) → сырые байты страницы.
HttpGetFn = Callable[[str], bytes]

#: Соответствие языка запроса региону библиотеки (данные, не категория).
_REGION_BY_LANG = {"ru": "ru-ru", "en": "us-en"}


class SearchError(RuntimeError):
    """Сбой живого вызова поиска/фетча (pipeline отображает на source_unavailable)."""


class SearchAdapterUnavailable(RuntimeError):
    """Поисковый адаптер недоступен: нет ``ddgs``/``httpx`` и не внедрён шов."""


def _ddgs_available() -> bool:
    """Установлена ли библиотека ``ddgs`` (без импорта пакета)."""
    return importlib.util.find_spec("ddgs") is not None


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` живого фетча)."""
    return datetime.now(timezone.utc).isoformat()


def _as_text(value: Any) -> str:
    """Приводит поле провайдера к строке, безопасно обрабатывая ``None``."""
    return "" if value is None else str(value)


class DuckDuckGoSearchAdapter:
    """Внешний SERP-адаптер на библиотеке ``ddgs`` (кандидаты + сырые страницы).

    Реализация ``SearchAdapterPort``. Поиск — ``DDGS().text(query, max_results=…,
    region=…, backend=…)``; запись библиотеки ``{title, href, body}`` отображается в
    ``SearchResult`` (``url=href``, ``snippet=body``, ``rank`` — позиция 1-based).

    :param http_get: шов GET для ``fetch`` (тесты/подмена); ``None`` → реальный ``httpx``.
    :param search_fn: шов поиска ``(query, limit, region) -> list[dict]``; если задан,
        библиотека ``ddgs`` не вызывается (сеть не нужна).
    :param backend: движок библиотеки (иначе ``DUCKDUCKGO_BACKEND``, дефолт ``duckduckgo``).
    :param cache: глобальный content-addressed кэш ``fetch`` (R7); ``None`` → без кэша.
    :param timeout: таймаут сетевого поиска/фетча, с.
    """

    adapter_kind = "serp"
    provider = "duckduckgo"

    def __init__(
        self,
        http_get: HttpGetFn | None = None,
        *,
        search_fn: SearchFn | None = None,
        backend: str | None = None,
        cache: ContentAddressedCache | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        resolved_backend = (
            backend if backend is not None else os.environ.get(ENV_BACKEND)
        ) or DEFAULT_BACKEND
        if not resolved_backend:
            raise ValueError("backend обязан быть непустым")
        self._http_get = http_get
        self._search_fn = search_fn
        self.backend = resolved_backend
        self._cache = cache
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Готовность
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён шов либо есть ``ddgs`` и ``httpx``."""
        if self._search_fn is not None or self._http_get is not None:
            return True
        return _ddgs_available() and _httpx_available()

    def _ensure_search_ready(self) -> None:
        """Поднимает ``SearchAdapterUnavailable``, если поиск невозможен."""
        if self._search_fn is None and not _ddgs_available():
            raise SearchAdapterUnavailable(
                "библиотека ddgs не установлена: реальный поиск недоступен. "
                "Установите extra 'search' (ddgs) или внедрите search_fn."
            )

    # ------------------------------------------------------------------
    # Контракт порта
    # ------------------------------------------------------------------

    def search(self, q: SearchQuery) -> list[SearchResult]:
        """Возвращает кандидатов провайдера по запросу (без байтов страниц).

        Лимит ``q.limit`` уходит библиотеке (``max_results``) и обрезает выдачу
        (симметрия I16 — за вызывающим). ``lang`` отображается в регион. ``raw_bytes``
        всегда ``None``: контент берёт ``fetch``.
        """
        text = q.text.strip()
        if not text:
            raise ValueError("поисковый запрос пуст")
        limit = int(q.limit)
        if limit <= 0:
            return []
        self._ensure_search_ready()
        region = self._region(q.lang)
        records = self._run_search(text, limit, region)
        return self._parse(records, limit)

    def fetch(self, url: str) -> RawPage:
        """Загружает страницу по URL: SSRF-валидация → кэш → живой GET → кэш.

        ``validate_url`` поднимает ``ValueError`` на приватные/петлевые цели (SSRF);
        при отказе живого запроса — ``SearchError``. Возвращает только сырые байты в
        ``RawPage`` (retention I19); хэширование/нарезка — вне адаптера.
        """
        validate_url(url)
        key = http_key(url)
        if self._cache is not None:
            cached = self._cache.get_bytes("http", key)
            if cached is not None:
                return self._page_from_cache(url, cached)
        page = self._fetch_live(url)
        if self._cache is not None:
            self._cache.put_bytes("http", key, page.content)
        return page

    # ------------------------------------------------------------------
    # Поиск / разбор
    # ------------------------------------------------------------------

    @staticmethod
    def _region(lang: str) -> str:
        """Регион библиотеки по языку запроса (данные-маппинг, дефолт ``ru-ru``)."""
        return _REGION_BY_LANG.get((lang or "").strip().lower(), DEFAULT_REGION)

    def _run_search(self, text: str, limit: int, region: str) -> list[dict[str, Any]]:
        """Поиск: внедрённый шов или реальная библиотека ``ddgs``."""
        if self._search_fn is not None:
            try:
                records = self._search_fn(text, limit, region)
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise SearchError(f"поиск (шов) не удался: {exc}") from exc
            return list(records)
        return self._run_search_real(text, limit, region)

    def _run_search_real(self, text: str, limit: int, region: str) -> list[dict[str, Any]]:
        """Реальный поиск через ``ddgs``; сбой библиотеки → ``SearchError``."""
        ddgs = self._import_ddgs()
        try:
            client = ddgs.DDGS(timeout=int(self.timeout))
            return list(
                client.text(
                    text,
                    max_results=limit,
                    region=region,
                    backend=self.backend,
                )
            )
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise SearchError(f"поиск ddgs не удался: {exc}") from exc

    def _parse(self, records: Iterable[Mapping[str, Any]], limit: int) -> list[SearchResult]:
        """Отображает записи библиотеки в детерминированный список ``SearchResult``."""
        results: list[SearchResult] = []
        for record in records:
            if len(results) >= limit:
                break
            if not isinstance(record, Mapping):
                continue
            url = _as_text(record.get("href") or record.get("url")).strip()
            if not url:
                continue
            results.append(
                SearchResult(
                    url=url,
                    title=_as_text(record.get("title")),
                    snippet=self._snippet(record),
                    rank=len(results) + 1,
                    raw_bytes=None,  # I19: байты страницы даёт fetch, не search
                )
            )
        return results[:limit]

    @staticmethod
    def _snippet(record: Mapping[str, Any]) -> str:
        """Собирает сниппет из ``body``/``snippet``/``description``."""
        for key in ("body", "snippet", "description"):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    # ------------------------------------------------------------------
    # Живой фетч (реальный путь httpx)
    # ------------------------------------------------------------------

    def _fetch_live(self, url: str) -> RawPage:
        """Живой фетч: внедрённый шов или реальный ``httpx`` (без кэша)."""
        if self._http_get is not None:
            try:
                content = bytes(self._http_get(url))
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise SearchError(f"HTTP-фетч {url!r} не удался: {exc}") from exc
            return RawPage(
                url=url,
                content=content,
                fetched_at=_now_iso(),
                fetch_mode=FetchMode.HTTP,
            )
        return self._get_real(url)

    def _get_real(self, url: str) -> RawPage:
        """Реальный GET страницы; отсутствие зависимости → ``SearchAdapterUnavailable``."""
        httpx = self._import_http()
        try:
            response = httpx.get(url, timeout=self.timeout, follow_redirects=True)
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise SearchError(f"HTTP-фетч {url!r} не удался: {exc}") from exc
        content_type = response.headers.get("Content-Type")
        return RawPage(
            url=str(response.url),
            content=response.content,
            fetched_at=_now_iso(),
            fetch_mode=FetchMode.HTTP,
            status_code=response.status_code,
            content_type=content_type or None,
        )

    # ------------------------------------------------------------------
    # Кэш
    # ------------------------------------------------------------------

    def _page_from_cache(self, url: str, content: bytes) -> RawPage:
        """Собирает ``RawPage`` из кэшированных байтов (без сети, R7 между ``run_id``)."""
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

    # ------------------------------------------------------------------
    # Ленивый импорт
    # ------------------------------------------------------------------

    @staticmethod
    def _import_ddgs() -> Any:
        """Лениво импортирует ``ddgs``; при отсутствии — ``SearchAdapterUnavailable``."""
        if not _ddgs_available():
            raise SearchAdapterUnavailable(
                "библиотека ddgs не установлена: реальный поиск недоступен. "
                "Установите extra 'search' (ddgs) или внедрите search_fn."
            )
        import ddgs  # type: ignore[import-not-found]

        return ddgs

    @staticmethod
    def _import_http() -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — ``SearchAdapterUnavailable``."""
        if not _httpx_available():
            raise SearchAdapterUnavailable(
                "httpx не установлен: реальный фетч недоступен. "
                "Установите extra 'crawl' (05) или внедрите http_get."
            )
        import httpx  # type: ignore[import-not-found]

        return httpx
