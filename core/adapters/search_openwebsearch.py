"""Внешний поиск (SERP) через локальную утилиту ``open-websearch`` (subprocess).

Роль: commodity-слой внешнего поиска **вне оценщика** (I18). Выход адаптера — только
URL-кандидаты (``SearchResult``) и, для ``fetch``, сырые байты страницы. Это **не**
разметка статусов: найденное становится материалом лишь дальше по конвейеру Ш5
(``corpus_raw`` → extract → chunk → ``content_hash`` → index, I19).

**I19 — структурная гарантия.** Модуль намеренно не импортирует домен/хранилище и не
конструирует объекты-доказательства/ячейки: их здесь физически нет по построению.
``search`` возвращает кандидатов **без байтов страницы** (``raw_bytes=None``);
контент получает ``fetch(url)``, который лишь кладёт сырые байты в retention/кэш.
Хэширование, нарезку и индексацию выполняет pipeline (Ш5), а не адаптер.

Утилита вызывается **one-shot** командами (``serve`` не поднимается):
``open-websearch search <query> --limit N --engine <engine> --json``. JSON-конверт на
stdout: ``{"status":"ok","data":{"results":[{"title","url","description",…}]},…}``;
``status != "ok"`` или непустой ``error`` → :class:`SearchError`. Непустой
``partialFailures`` ошибкой не считается.

**Изолированный шов для тестов.** Конструктор принимает ``runner``
(``argv -> (returncode, stdout, stderr)``) и ``http_get``. Если заданы — реальный процесс
и сеть не задействуются. Путь бинаря переопределяется ``OPEN_WEBSEARCH_BIN``, движок —
``OPEN_WEBSEARCH_ENGINE`` (дефолт ``duckduckgo``; движок ``google`` утилитой не
поддерживается и не является дефолтом).

**Готовность.** ``available`` = задан ``runner`` ИЛИ найден исполняемый файл утилиты.
Без того и другого ``search`` поднимает :class:`SearchAdapterUnavailable` — кандидаты
не выдумываются (I19).

**``fetch`` — сырые байты, не извлечённый текст.** Утилита умеет ``fetch-web``, но он
возвращает извлечённый текст, а не сырые байты; для I19 нужны именно сырые байты,
поэтому ``fetch`` делает обычный HTTP GET (``httpx`` + SSRF-валидация).

**Кэш (R7).** ``fetch`` консультирует внедрённый ``ContentAddressedCache`` по
``http_key(url)``: попадание → ``RawPage`` без сети, промах → живой GET + запись.

Нейтрален (§1.3): имя провайдера — данные-конфигурация, в модуле нет названий
продуктов/категорий.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from core.ports.search import SearchQuery, SearchResult
from core.ports.types import RawPage, FetchMode
from core.util.cache import ContentAddressedCache, http_key
from core.util.security import validate_url

__all__ = [
    "DEFAULT_BINARY",
    "DEFAULT_ENGINE",
    "DEFAULT_TIMEOUT",
    "ENV_BINARY",
    "ENV_ENGINE",
    "HttpGetFn",
    "OpenWebSearchAdapter",
    "RunnerFn",
    "SearchAdapterUnavailable",
    "SearchError",
]

#: Имя исполняемого файла утилиты по умолчанию (переопределяется ``OPEN_WEBSEARCH_BIN``).
DEFAULT_BINARY = "open-websearch"
#: Движок поиска по умолчанию (данные; ``google`` утилитой не поддерживается).
DEFAULT_ENGINE = "duckduckgo"
#: Таймаут одного запуска процесса и сетевого фетча, с.
DEFAULT_TIMEOUT = 60.0

#: Переменные среды: путь к бинарю и движок поиска (данные-конфигурация).
ENV_BINARY = "OPEN_WEBSEARCH_BIN"
ENV_ENGINE = "OPEN_WEBSEARCH_ENGINE"

#: Внедряемый запуск процесса: argv → (returncode, stdout, stderr).
RunnerFn = Callable[[list[str]], tuple[int, str, str]]
#: Внедряемая функция GET для ``fetch``: (url) → сырые байты страницы.
HttpGetFn = Callable[[str], bytes]


class SearchError(RuntimeError):
    """Сбой вызова поиска/фетча (pipeline отображает на source_unavailable)."""


class SearchAdapterUnavailable(RuntimeError):
    """Поисковый адаптер недоступен: не найден бинарь и не внедрён ``runner``."""


def _httpx_available() -> bool:
    """Установлен ли ``httpx`` (без импорта пакета)."""
    return importlib.util.find_spec("httpx") is not None


def _now_iso() -> str:
    """Текущий UTC-таймстемп в ISO-8601 (поле ``fetched_at`` живого фетча)."""
    return datetime.now(timezone.utc).isoformat()


def _as_text(value: Any) -> str:
    """Приводит поле провайдера к строке, безопасно обрабатывая ``None``."""
    return "" if value is None else str(value)


class OpenWebSearchAdapter:
    """Внешний SERP-адаптер на локальной утилите ``open-websearch`` (subprocess).

    Реализация ``SearchAdapterPort``. Запускает one-shot команду
    ``open-websearch search <query> --limit N --engine <engine> --json`` и разбирает
    JSON-конверт stdout (записи ``data.results``).

    :param http_get: шов GET для ``fetch`` (тесты/подмена); ``None`` → реальный ``httpx``.
    :param runner: шов запуска процесса (тесты/подмена); если задан — процесс не
        запускается.
    :param binary: путь/имя утилиты (иначе ``OPEN_WEBSEARCH_BIN``).
    :param engine: движок поиска (иначе ``OPEN_WEBSEARCH_ENGINE``, дефолт ``duckduckgo``).
    :param cache: глобальный content-addressed кэш ``fetch`` (R7); ``None`` → без кэша.
    :param timeout: таймаут процесса и сетевого фетча, с.
    """

    adapter_kind = "serp"
    provider = "open-websearch"

    def __init__(
        self,
        http_get: HttpGetFn | None = None,
        *,
        runner: RunnerFn | None = None,
        binary: str | None = None,
        engine: str | None = None,
        cache: ContentAddressedCache | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if timeout <= 0:
            raise ValueError(f"timeout обязан быть положительным, получено {timeout!r}")
        resolved_binary = (
            binary if binary is not None else os.environ.get(ENV_BINARY)
        ) or DEFAULT_BINARY
        resolved_engine = (
            engine if engine is not None else os.environ.get(ENV_ENGINE)
        ) or DEFAULT_ENGINE
        if not resolved_binary:
            raise ValueError("binary обязан быть непустым")
        if not resolved_engine:
            raise ValueError("engine обязан быть непустым")
        self._http_get = http_get
        self._runner = runner
        self.binary = resolved_binary
        self.engine = resolved_engine
        self._cache = cache
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Готовность
    # ------------------------------------------------------------------

    def _binary_path(self) -> str | None:
        """Путь к исполняемому файлу утилиты в ``PATH`` (или ``None``)."""
        return shutil.which(self.binary)

    @property
    def available(self) -> bool:
        """Готов ли адаптер: внедрён ``runner`` либо найден исполняемый файл утилиты."""
        return self._runner is not None or self._binary_path() is not None

    def _ensure_available(self) -> None:
        """Поднимает ``SearchAdapterUnavailable``, если запуск поиска невозможен."""
        if self._runner is None and self._binary_path() is None:
            raise SearchAdapterUnavailable(
                f"исполняемый файл {self.binary!r} не найден: реальный поиск недоступен. "
                f"Установите утилиту (PATH), задайте {ENV_BINARY} или внедрите runner."
            )

    # ------------------------------------------------------------------
    # Контракт порта
    # ------------------------------------------------------------------

    def search(self, q: SearchQuery) -> list[SearchResult]:
        """Возвращает кандидатов провайдера по запросу (без байтов страниц).

        Запускает ``open-websearch search …`` и разбирает JSON-конверт; лимит уходит
        утилите и обрезает выдачу (симметрия I16 — за вызывающим). ``raw_bytes`` всегда
        ``None``: контент берёт ``fetch``.
        """
        text = q.text.strip()
        if not text:
            raise ValueError("поисковый запрос пуст")
        limit = int(q.limit)
        if limit <= 0:
            return []
        self._ensure_available()
        argv = self._build_argv(text, limit)
        returncode, stdout, stderr = self._run(argv)
        if returncode != 0:
            detail = stderr.strip() or stdout.strip()
            raise SearchError(
                f"open-websearch завершился с кодом {returncode}: {detail}"
            )
        data = self._decode(stdout)
        return self._parse(data, limit)

    def fetch(self, url: str) -> RawPage:
        """Загружает страницу по URL: SSRF-валидация → кэш → живой GET → кэш.

        Возвращает **сырые байты** (не извлечённый текст утилиты) — требование I19;
        ``validate_url`` поднимает ``ValueError`` на приватные/петлевые цели (SSRF).
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
    # Запуск / разбор
    # ------------------------------------------------------------------

    def _build_argv(self, text: str, limit: int) -> list[str]:
        """argv one-shot команды поиска (бинарь либо имя, если путь не найден)."""
        binary = self._binary_path() or self.binary
        return [
            binary,
            "search",
            text,
            "--limit",
            str(limit),
            "--engine",
            self.engine,
            "--json",
        ]

    def _run(self, argv: list[str]) -> tuple[int, str, str]:
        """Запуск процесса: внедрённый ``runner`` либо реальный ``subprocess.run``."""
        if self._runner is not None:
            try:
                returncode, stdout, stderr = self._runner(argv)
            except Exception as exc:  # noqa: BLE001 - унификация ошибок адаптера
                raise SearchError(f"запуск утилиты (шов) не удался: {exc}") from exc
            return int(returncode), _as_text(stdout), _as_text(stderr)
        try:
            completed = subprocess.run(  # noqa: S603 - argv без shell, путь из PATH/env
                argv,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise SearchError(
                f"open-websearch превысил таймаут {self.timeout} с"
            ) from exc
        except OSError as exc:
            raise SearchError(f"запуск open-websearch не удался: {exc}") from exc
        return completed.returncode, completed.stdout, completed.stderr

    @staticmethod
    def _decode(raw: str) -> Any:
        """Разбирает stdout как JSON; невалидный вывод → ``SearchError``."""
        try:
            return json.loads(raw)
        except (ValueError, TypeError) as exc:
            raise SearchError(f"ответ open-websearch не является JSON: {exc}") from exc

    def _parse(self, data: Any, limit: int) -> list[SearchResult]:
        """Проверяет конверт и отображает ``data.results`` в ``SearchResult``."""
        if not isinstance(data, Mapping):
            raise SearchError("ответ open-websearch не является JSON-объектом")
        error = data.get("error")
        status = data.get("status")
        if error or status != "ok":
            raise SearchError(f"open-websearch вернул ошибку: {error or status!r}")
        payload = data.get("data")
        records: Sequence[Any] = []
        if isinstance(payload, Mapping):
            value = payload.get("results")
            if isinstance(value, list):
                records = value
        results: list[SearchResult] = []
        for record in records:
            if len(results) >= limit:
                break
            if not isinstance(record, Mapping):
                continue
            url = _as_text(record.get("url") or record.get("href")).strip()
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
        """Собирает сниппет из ``description``/``snippet``/``content``."""
        for key in ("description", "snippet", "content"):
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
    def _import_http() -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — ``SearchAdapterUnavailable``."""
        if not _httpx_available():
            raise SearchAdapterUnavailable(
                "httpx не установлен: реальный фетч недоступен. "
                "Установите extra 'crawl' (05) или внедрите http_get."
            )
        import httpx  # type: ignore[import-not-found]

        return httpx
