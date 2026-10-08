"""Внешний поиск (Web Search) через Z.ai Web Search API — реализация ``SearchAdapterPort``.

Роль: commodity-слой внешнего поиска **вне оценщика** (I18) — второй провайдер/вид
рядом с ``search_yandex.py``. Выход адаптера — только URL-кандидаты (``SearchResult``)
и, для ``fetch``, сырые байты страницы. Это **не доказательство** и не разметка
статусов: найденное становится материалом лишь дальше по конвейеру Ш5
(``corpus_raw`` → extract → chunk → ``content_hash`` → index, I19).

**Вид адаптера.** Провайдер заземляющего поиска (LLM-grounding): ответ —
генерируемые/ранжируемые провайдером результаты, а не классическая SERP-выдача.
Соответствует ``AdapterKind.LLM_GROUNDING`` (``core/domain/enums.py``; словарь §4.1).
Значение выбрано как string-литерал (как ``serp`` в yandex-адаптере): модуль не
импортирует домен, чтобы оставаться структурно отделённым от ядра доказательств.

**I19 — структурная гарантия.** Модуль намеренно не импортирует домен/хранилище и не
конструирует объекты-доказательства/ячейки: их здесь физически нет по построению.
``search`` возвращает кандидатов **без байтов страницы** (``raw_bytes=None``);
контент получает ``fetch(url)``, который лишь кладёт сырые байты в retention/кэш.
Хэширование, нарезку и индексацию выполняет pipeline (Ш5), а не адаптер.

**Симметрия бюджета (I16).** Адаптер не решает бюджет: он честно передаёт провайдеру
``SearchQuery.limit`` (поле ``count``) и язык (поле ``language``) и обрезает выдачу.
Равенство бюджета RU/EN — ответственность вызывающего pipeline (T11.2, ``search_log``);
логирование вызовов — также за вызывающим (I23).

**Ленивый ``httpx``.** Модуль импортируется и тестируется без ``httpx``: HTTP-клиент
подтягивается только в реальном пути, ``available`` показывает готовность.

**Изолированный шов для тестов.** Конструктор принимает ``http_post`` и ``http_get``
(``(url, payload) -> bytes`` и ``(url) -> bytes``). Если заданы — реальный путь не
задействуется, сеть не нужна. Если ``None`` — реальные вызовы ``httpx`` к API.

**Учётные данные — из окружения/конструктора, не хардкод.** ``api_key`` берётся из
аргумента либо переменной среды ``GLM_SEARCH_API_KEY``. В заголовок реального запроса
уходит ``Authorization: Bearer …``.

**Кэш (R7).** ``fetch`` консультирует внедрённый ``ContentAddressedCache`` по
``http_key(url)``: попадание → ``RawPage`` из кэшированных байтов без сети, промах →
живой GET + запись. Кэш глобальный и переживает ``run_id``. Ответы поиска адаптер не
кэширует: нормализация запроса (включая язык) и его кэширование — за pipeline (T11.2).

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

from core.ports.search import SearchQuery, SearchResult
from core.ports.types import RawPage, RenderMode
from core.util.cache import ContentAddressedCache, http_key
from core.util.security import validate_url

__all__ = [
    "GlmSearchAdapter",
    "SearchAdapterUnavailable",
    "SearchError",
    "HttpPostFn",
    "HttpGetFn",
    "DEFAULT_BASE_URL",
    "DEFAULT_SEARCH_PATH",
    "DEFAULT_SEARCH_ENGINE",
    "DEFAULT_TIMEOUT",
    "ENV_API_KEY",
]

#: База Z.ai Web Search API по умолчанию. Переопределяется ``base_url``.
DEFAULT_BASE_URL = "https://api.z.ai/api/paas/v4"
#: Путь веб-поиска (JSON-ответ с ``search_result``).
DEFAULT_SEARCH_PATH = "/web_search"
#: Движок поиска провайдера по умолчанию (данные, не категория).
DEFAULT_SEARCH_ENGINE = "search_std"
#: Таймаут HTTP-запроса, с.
DEFAULT_TIMEOUT = 30.0

#: Переменная среды для учётных данных (секреты не хардкодятся).
ENV_API_KEY = "GLM_SEARCH_API_KEY"

#: Внедряемая функция POST: (endpoint, JSON-payload) → сырые байты ответа.
HttpPostFn = Callable[[str, dict[str, Any]], bytes]
#: Внедряемая функция GET: (url) → сырые байты страницы.
HttpGetFn = Callable[[str], bytes]

#: Ключи известных форм ответа поиска (порядок — приоритет разбора).
_RESULT_KEYS = ("search_result", "results", "items", "organic", "docs")


class SearchError(RuntimeError):
    """Сбой живого HTTP-вызова поиска/фетча (pipeline отображает на source_unavailable)."""


class SearchAdapterUnavailable(RuntimeError):
    """Поисковый адаптер недоступен: нет ``httpx`` и не внедрён HTTP-шов."""


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` живого фетча)."""
    return datetime.now(timezone.utc).isoformat()


def _as_text(value: Any) -> str:
    """Приводит поле провайдера к строке, безопасно обрабатывая ``None``."""
    return "" if value is None else str(value)


class GlmSearchAdapter:
    """Заземляющий поиск-адаптер (кандидаты + сырые страницы). Реализация ``SearchAdapterPort``.

    :param http_post: шов POST (тесты/подмена); ``None`` → реальный ``httpx``.
    :param http_get: шов GET для ``fetch``; ``None`` → реальный ``httpx``.
    :param api_key: ключ API (иначе из ``GLM_SEARCH_API_KEY``); не хардкодится.
    :param base_url: база API; ``search_path`` — путь веб-поиска.
    :param search_engine: движок поиска провайдера (данные-конфигурация).
    :param cache: глобальный content-addressed кэш ``fetch`` (R7); ``None`` → без кэша.
    :param timeout: таймаут HTTP-запроса, с.
    """

    adapter_kind = "llm_grounding"
    provider = "glm"

    def __init__(
        self,
        http_post: HttpPostFn | None = None,
        http_get: HttpGetFn | None = None,
        *,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        search_path: str = DEFAULT_SEARCH_PATH,
        search_engine: str = DEFAULT_SEARCH_ENGINE,
        cache: ContentAddressedCache | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if not base_url:
            raise ValueError("base_url обязан быть непустым")
        if not search_path or not search_path.startswith("/"):
            raise ValueError("search_path обязан начинаться с '/'")
        if not search_engine:
            raise ValueError("search_engine обязан быть непустым")
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        self._http_post = http_post
        self._http_get = http_get
        self.api_key = api_key if api_key is not None else os.environ.get(ENV_API_KEY)
        self.base_url = base_url.rstrip("/")
        self.search_path = search_path
        self.search_engine = search_engine
        self._cache = cache
        self.timeout = timeout

    @property
    def endpoint(self) -> str:
        """Полный URL точки поиска (``base_url`` + ``search_path``)."""
        return f"{self.base_url}{self.search_path}"

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён HTTP-шов либо установлен ``httpx``."""
        if self._http_post is not None or self._http_get is not None:
            return True
        return _httpx_available()

    # ------------------------------------------------------------------
    # Контракт порта
    # ------------------------------------------------------------------

    def search(self, q: SearchQuery) -> list[SearchResult]:
        """Возвращает кандидатов провайдера по запросу (без байтов страниц).

        Лимит ``q.limit`` и язык ``q.lang`` уходят провайдеру, а выдача обрезается
        (симметрия I16 — за вызывающим). ``raw_bytes`` всегда ``None``: контент
        берёт ``fetch``.
        """
        text = q.text.strip()
        if not text:
            raise ValueError("поисковый запрос пуст")
        limit = int(q.limit)
        if limit <= 0:
            return []
        payload = self._build_payload(text, q.lang, limit)
        raw = self._post(self.endpoint, payload)
        data = self._decode(raw)
        return self._parse(data, limit)

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

    def _build_payload(self, text: str, lang: str, limit: int) -> dict[str, Any]:
        """JSON-payload веб-поиска (данные провайдера, без секретов).

        ``count`` — лимит, ``language`` — язык запроса: оба передаются провайдеру
        как есть (I16 pass-through, не enforcement).
        """
        payload: dict[str, Any] = {
            "search_engine": self.search_engine,
            "search_query": text,
            "count": limit,
        }
        language = (lang or "").strip()
        if language:
            payload["language"] = language
        return payload

    @staticmethod
    def _decode(raw: bytes | str) -> Any:
        """Разбирает сырой JSON-ответ провайдера (bytes или str)."""
        if isinstance(raw, (bytes, bytearray)):
            return json.loads(bytes(raw).decode("utf-8"))
        return json.loads(raw)

    def _parse(self, data: Any, limit: int) -> list[SearchResult]:
        """Отображает JSON провайдера в детерминированный список ``SearchResult``."""
        results: list[SearchResult] = []
        for index, record in enumerate(self._records(data), start=1):
            if len(results) >= limit:
                break
            url = _as_text(record.get("link") or record.get("url")).strip()
            if not url:
                continue
            results.append(
                SearchResult(
                    url=url,
                    title=_as_text(record.get("title")),
                    snippet=self._snippet(record),
                    rank=self._rank(record, index),
                    raw_bytes=None,  # I19: байты страницы даёт fetch, не search
                )
            )
        results.sort(key=lambda r: (r.rank, r.url))
        return results[:limit]

    @staticmethod
    def _records(data: Any) -> list[Mapping[str, Any]]:
        """Извлекает записи результатов из известных форм ответа провайдера.

        Поддерживаются: список записей; JSON с ``search_result`` (основная форма) и
        алиасами ``results``/``items``/``organic``/``docs``, в том числе вложенными
        под ``data``. Неизвестная форма → пустой список (грациозная деградация:
        кандидатов нет, но вызов не падает).
        """
        if isinstance(data, list):
            return [r for r in data if isinstance(r, Mapping)]
        if not isinstance(data, Mapping):
            return []
        found = GlmSearchAdapter._records_in_mapping(data)
        if found is not None:
            return found
        nested = data.get("data")
        if isinstance(nested, Mapping):
            found = GlmSearchAdapter._records_in_mapping(nested)
            if found is not None:
                return found
        return []

    @staticmethod
    def _records_in_mapping(data: Mapping[str, Any]) -> list[Mapping[str, Any]] | None:
        """Ищет список записей по известным ключам; ``None`` — ключ не найден."""
        for key in _RESULT_KEYS:
            value = data.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, Mapping)]
            if isinstance(value, Mapping):
                inner = value.get("search_result") or value.get("results") or value.get("items")
                if isinstance(inner, list):
                    return [r for r in inner if isinstance(r, Mapping)]
        return None

    @staticmethod
    def _snippet(record: Mapping[str, Any]) -> str:
        """Собирает сниппет из ``content``/``description``/``snippet``/``summary``."""
        for key in ("content", "description", "snippet", "summary"):
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _rank(record: Mapping[str, Any], index: int) -> int:
        """Rank записи: из провайдера, иначе позиция в выдаче (1-based)."""
        value = record.get("rank")
        if value is None:
            return index
        try:
            return int(value)
        except (TypeError, ValueError):
            return index

    # ------------------------------------------------------------------
    # Живые вызовы (реальный путь httpx)
    # ------------------------------------------------------------------

    def _post(self, url: str, payload: dict[str, Any]) -> bytes:
        """POST к API: внедрённый шов или реальный ``httpx``."""
        if self._http_post is not None:
            return bytes(self._http_post(url, payload))
        return self._post_real(url, payload)

    def _post_real(self, url: str, payload: dict[str, Any]) -> bytes:
        """Реальный POST веб-поиска; отсутствие зависимости → ``SearchAdapterUnavailable``."""
        httpx = self._import_http()
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        try:
            response = httpx.post(url, json=payload, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            return response.content
        except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
            raise SearchError(f"HTTP-поиск не удался: {exc}") from exc

    def _fetch_live(self, url: str) -> RawPage:
        """Живой фетч: внедрённый шов или реальный ``httpx`` (без кэша)."""
        if self._http_get is not None:
            content = bytes(self._http_get(url))
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
        """ISO-8601 (UTC) mtime файла кэша; при отсутствии — текущее время.

        mtime — момент первого фетча, что сохраняет детерминизм R7 между ``run_id``.
        """
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
                "Установите extra 'crawl' (05) или внедрите http_post/http_get."
            )
        import httpx  # type: ignore[import-not-found]

        return httpx
