"""Внешний поиск (SERP) через Google Programmable Search (Custom Search JSON API).

Роль: commodity-слой внешнего поиска **вне оценщика** (I18). Выход адаптера — только
URL-кандидаты (``SearchResult``) и, для ``fetch``, сырые байты страницы. Это **не**
разметка статусов: найденное становится материалом лишь дальше по конвейеру Ш5
(``corpus_raw`` → extract → chunk → ``content_hash`` → index, I19).

**I19 — структурная гарантия.** Модуль намеренно не импортирует домен/хранилище и не
конструирует объекты-доказательства/ячейки: их здесь физически нет по построению.
``search`` возвращает кандидатов **без байтов страницы** (``raw_bytes=None``);
контент получает ``fetch(url)``, который лишь кладёт сырые байты в retention/кэш.
Хэширование, нарезку и индексацию выполняет pipeline (Ш5), а не адаптер.

**Учётные данные — из окружения/конструктора, не хардкод.** Google CSE требует пару
«ключ API + идентификатор поискового движка (cx)»: ``api_key`` (``GOOGLE_API_KEY`` или
``GOOGLE_SEARCH_API_KEY``) и ``cse_id`` (``GOOGLE_CSE_ID`` или
``GOOGLE_SEARCH_ENGINE_ID``). Без полной пары ``search`` поднимает
:class:`SearchAdapterUnavailable` — кандидаты не выдумываются (I19).

**Постраничность.** CSE отдаёт максимум 10 результатов за запрос (``num<=10``). При
``limit>10`` адаптер добирает страницы через ``start=1,11,21,…`` и обрезает итог по
``limit``. Бюджет — ответственность вызывающего pipeline (I16); логирование вызовов —
также за вызывающим (I23).

**Ленивый ``httpx``.** Модуль импортируется и тестируется без ``httpx``: HTTP-клиент
подтягивается только в реальном пути, ``available`` показывает готовность.

**Изолированный шов для тестов.** Конструктор принимает ``http_get`` (``(url) -> bytes``).
Шов обслуживает и поисковый API (по полному URL с query-string), и ``fetch`` страницы.
Если задан — реальный путь не задействуется, сеть не нужна; иначе реальные вызовы
``httpx``.

**Кэш (R7).** ``fetch`` консультирует внедрённый ``ContentAddressedCache`` по
``http_key(url)``: попадание → ``RawPage`` из кэшированных байтов без сети, промах →
живой GET + запись. Ответы поиска адаптер не кэширует: нормализация запроса (включая
язык) и его кэширование — за pipeline (T11.2).

Нейтрален (§1.3): имя провайдера — данные-конфигурация, в модуле нет названий
продуктов/категорий.
"""

from __future__ import annotations

import importlib.util
import json
import os
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from core.ports.search import SearchQuery, SearchResult
from core.ports.types import RawPage, RenderMode
from core.util.cache import ContentAddressedCache, http_key
from core.util.security import validate_url

__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_TIMEOUT",
    "ENV_API_KEY",
    "ENV_API_KEY_ALT",
    "ENV_CSE_ID",
    "ENV_CSE_ID_ALT",
    "GoogleSearchAdapter",
    "HttpGetFn",
    "SearchAdapterUnavailable",
    "SearchError",
]

#: Точка Custom Search JSON API по умолчанию. Переопределяется ``base_url``.
DEFAULT_BASE_URL = "https://www.googleapis.com/customsearch/v1"
#: Максимум результатов CSE за один запрос (жёсткое ограничение провайдера).
DEFAULT_PAGE_SIZE = 10
#: Таймаут HTTP-запроса, с.
DEFAULT_TIMEOUT = 30.0

#: Переменные среды учётных данных (секреты не хардкодятся); альтернативные имена — фолбэк.
ENV_API_KEY = "GOOGLE_API_KEY"
ENV_API_KEY_ALT = "GOOGLE_SEARCH_API_KEY"
ENV_CSE_ID = "GOOGLE_CSE_ID"
ENV_CSE_ID_ALT = "GOOGLE_SEARCH_ENGINE_ID"

#: Внедряемая функция GET: (url) → сырые байты ответа (JSON API либо страница).
HttpGetFn = Callable[[str], bytes]

#: Соответствие языка запроса параметру локализации CSE (данные, не категория).
_LR_BY_LANG = {"ru": "lang_ru", "en": "lang_en"}


class SearchError(RuntimeError):
    """Сбой живого HTTP-вызова поиска/фетча (pipeline отображает на source_unavailable)."""


class SearchAdapterUnavailable(RuntimeError):
    """Поисковый адаптер недоступен: нет учётных данных, ``httpx`` и не внедрён HTTP-шов."""


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` живого фетча)."""
    return datetime.now(timezone.utc).isoformat()


def _as_text(value: Any) -> str:
    """Приводит поле провайдера к строке, безопасно обрабатывая ``None``."""
    return "" if value is None else str(value)


class GoogleSearchAdapter:
    """Внешний SERP-адаптер Google CSE (кандидаты + сырые страницы).

    Реализация ``SearchAdapterPort``. Поиск — ``GET {base_url}?key=…&cx=…&q=…&num=…``
    (опционально ``lr`` и ``start``), ответ — ``{"items":[{"title","link","snippet"}]}``
    либо ``{"error":{…}}``.

    :param http_get: шов GET (тесты/подмена) для API и ``fetch``; ``None`` → ``httpx``.
    :param api_key: ключ API (иначе ``GOOGLE_API_KEY``/``GOOGLE_SEARCH_API_KEY``).
    :param cse_id: идентификатор поискового движка cx (иначе ``GOOGLE_CSE_ID``/
        ``GOOGLE_SEARCH_ENGINE_ID``).
    :param base_url: точка Custom Search JSON API.
    :param cache: глобальный content-addressed кэш ``fetch`` (R7); ``None`` → без кэша.
    :param timeout: таймаут HTTP-запроса, с.
    """

    adapter_kind = "serp"
    provider = "google"

    def __init__(
        self,
        http_get: HttpGetFn | None = None,
        *,
        api_key: str | None = None,
        cse_id: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        cache: ContentAddressedCache | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if not base_url:
            raise ValueError("base_url обязан быть непустым")
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        self._http_get = http_get
        self.api_key = (
            api_key
            if api_key is not None
            else (os.environ.get(ENV_API_KEY) or os.environ.get(ENV_API_KEY_ALT))
        )
        self.cse_id = (
            cse_id
            if cse_id is not None
            else (os.environ.get(ENV_CSE_ID) or os.environ.get(ENV_CSE_ID_ALT))
        )
        self.base_url = base_url.rstrip("/")
        self._cache = cache
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Готовность
    # ------------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Готов ли адаптер: есть пара ключ+cx и (внедрён шов либо установлен ``httpx``)."""
        if not (self.api_key and self.cse_id):
            return False
        return self._http_get is not None or _httpx_available()

    def _ensure_available(self) -> None:
        """Поднимает ``SearchAdapterUnavailable`` с понятной причиной, если не готов."""
        if not (self.api_key and self.cse_id):
            raise SearchAdapterUnavailable(
                "не заданы учётные данные Google CSE: нужны GOOGLE_API_KEY "
                "(или GOOGLE_SEARCH_API_KEY) и GOOGLE_CSE_ID "
                "(или GOOGLE_SEARCH_ENGINE_ID); кандидаты не выдумываются (I19)"
            )
        if self._http_get is None and not _httpx_available():
            raise SearchAdapterUnavailable(
                "httpx не установлен: реальные вызовы поиска недоступны. "
                "Установите extra 'crawl' (05) или внедрите http_get."
            )

    # ------------------------------------------------------------------
    # Контракт порта
    # ------------------------------------------------------------------

    def search(self, q: SearchQuery) -> list[SearchResult]:
        """Возвращает кандидатов провайдера по запросу (без байтов страниц).

        При ``limit>10`` добирает страницы через ``start`` (жёсткий максимум CSE — 10 на
        запрос) и обрезает итог по ``limit`` (симметрия I16 — за вызывающим).
        ``raw_bytes`` всегда ``None``: контент берёт ``fetch``.
        """
        text = q.text.strip()
        if not text:
            raise ValueError("поисковый запрос пуст")
        limit = int(q.limit)
        if limit <= 0:
            return []
        self._ensure_available()
        results: list[SearchResult] = []
        start = 1
        while len(results) < limit:
            page_size = min(DEFAULT_PAGE_SIZE, limit - len(results))
            url = self._build_url(text, q.lang, page_size, start)
            data = self._decode(self._get_search(url))
            self._raise_on_error(data)
            page_records = self._items(data)
            if not page_records:
                break
            for record in page_records:
                link = _as_text(record.get("link") or record.get("url")).strip()
                if not link:
                    continue
                results.append(
                    SearchResult(
                        url=link,
                        title=_as_text(record.get("title")),
                        snippet=self._snippet(record),
                        rank=len(results) + 1,
                        raw_bytes=None,  # I19: байты страницы даёт fetch, не search
                    )
                )
                if len(results) >= limit:
                    break
            if len(page_records) < page_size:
                break
            start += page_size
        return results[:limit]

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
    # Построение запроса / разбор ответа
    # ------------------------------------------------------------------

    def _build_url(self, text: str, lang: str, num: int, start: int) -> str:
        """URL запроса CSE с query-string (секреты уходят провайдеру как есть)."""
        params: dict[str, str] = {
            "key": self.api_key or "",
            "cx": self.cse_id or "",
            "q": text,
            "num": str(num),
        }
        lr = _LR_BY_LANG.get((lang or "").strip().lower())
        if lr:
            params["lr"] = lr
        if start > 1:
            params["start"] = str(start)
        return f"{self.base_url}?{urlencode(params)}"

    @staticmethod
    def _decode(raw: bytes | str) -> Any:
        """Разбирает сырой JSON-ответ провайдера (bytes или str)."""
        if isinstance(raw, (bytes, bytearray)):
            return json.loads(bytes(raw).decode("utf-8"))
        return json.loads(raw)

    @staticmethod
    def _items(data: Any) -> list[Mapping[str, Any]]:
        """Извлекает записи ``items`` из ответа CSE; неизвестная форма → пустой список."""
        if isinstance(data, Mapping):
            items = data.get("items")
            if isinstance(items, list):
                return [r for r in items if isinstance(r, Mapping)]
        return []

    @staticmethod
    def _raise_on_error(data: Any) -> None:
        """Ошибка в теле ответа CSE (``error``) → ``SearchError``."""
        if isinstance(data, Mapping) and data.get("error"):
            err = data["error"]
            message = err.get("message") if isinstance(err, Mapping) else err
            raise SearchError(f"ошибка поискового API: {message}")

    @staticmethod
    def _snippet(record: Mapping[str, Any]) -> str:
        """Собирает сниппет из ``snippet``/``description``."""
        for key in ("snippet", "description"):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    # ------------------------------------------------------------------
    # Живые вызовы (реальный путь httpx)
    # ------------------------------------------------------------------

    def _get_search(self, url: str) -> bytes:
        """GET поискового API: внедрённый шов или реальный ``httpx``."""
        if self._http_get is not None:
            try:
                return bytes(self._http_get(url))
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise SearchError(f"HTTP-поиск не удался: {exc}") from exc
        return self._get_real_bytes(url, what="поиск")

    def _get_real_bytes(self, url: str, *, what: str) -> bytes:
        """Реальный GET; отсутствие зависимости → ``SearchAdapterUnavailable``."""
        httpx = self._import_http()
        try:
            response = httpx.get(url, timeout=self.timeout, follow_redirects=True)
            response.raise_for_status()
            return response.content
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise SearchError(f"HTTP-{what} не удался: {exc}") from exc

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
                render_mode=RenderMode.HTTP,
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
            render_mode=RenderMode.HTTP,
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
            render_mode=RenderMode.HTTP,
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
    def _import_http() -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — ``SearchAdapterUnavailable``."""
        if not _httpx_available():
            raise SearchAdapterUnavailable(
                "httpx не установлен: реальные вызовы поиска недоступны. "
                "Установите extra 'crawl' (05) или внедрите http_get."
            )
        import httpx  # type: ignore[import-not-found]

        return httpx
