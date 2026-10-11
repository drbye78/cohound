"""Gateway-сервер DMZ (Z1/edge) — ``EdgeGateway`` + тонкий HTTP-слой (W13/T13.2; 07 §3–§5).

Второй composition root (07 §4): публикует **реальные** веб-адаптеры (краулер,
экстрактор, поиск, реестровые фетчеры) по протоколу ``/v1/*``. Ядро инициирует,
edge **отвечает**; обратного инициирования нет (07 §3/§5).

Границы (07 §4/§5):

* **Никакого доступа к хранилищу/домену** — сервер не импортирует store/domain и
  не пишет в БД: только исполняет контентные операции над инъецированными портами.
* **I18** — веб-эгрессия вынесена за шов; ядро маршрута в интернет не имеет.
* **I19** — edge возвращает **сырые байты/кандидатов**, а не evidence; превращение
  в evidence (``corpus_raw → hash → index``) делает ядро. Хэши здесь — для
  целостности/инспектируемости, ядро их пересчитывает (07 §8.5).

Конверт ответа — ``{schema_version, request_id, status, payload, error}``; имена
полей и ``schema_version`` совпадают с клиентской стороной
(``core/adapters/remote_gateway.py``), поэтому client↔server взаимозаменяемы.
Нейтрален (§1.3).
"""

from __future__ import annotations

import base64
import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from core.edge.seam_schema import validate_fetch_request
from core.ports.crawler import CrawlerPort
from core.ports.extractor import ExtractorPort
from core.ports.search import SearchAdapterPort, SearchQuery
from core.ports.types import ExtractionRoute, FetchMode, FetchPlan, ProductRef
from core.util.hashing import content_hash

__all__ = [
    "EDGE_SCHEMA_VERSION",
    "EdgeError",
    "EdgeRequestError",
    "EdgeAdapterError",
    "EdgeGateway",
    "route_request",
    "make_handler",
    "build_server",
    "serve",
]

#: Версия конверта (07 §3/§7/§8.3). Совпадает с клиентским дефолтом
#: (``core/adapters/remote_gateway.DEFAULT_SCHEMA_VERSION``) — версионная граница.
EDGE_SCHEMA_VERSION = "1"

#: Операции, которые в протоколе edge принимают тело (07 §3).
_POST_OPS = frozenset({"fetch", "search", "registry"})
#: Операции без тела (диагностика, 07 §3).
_GET_OPS = frozenset({"health", "capabilities"})
#: Полный allowlist маршрутов шва (07 §5).
_KNOWN_OPS = _POST_OPS | _GET_OPS

#: Заголовок корреляции запрос→ответ (07 §8.2/§8.4).
REQUEST_ID_HEADER = "X-Request-Id"

#: Допустимые оси фетча/экстракции (07 §3).
_FETCH_VALUES = {mode.value: mode for mode in FetchMode}
_EXTRACT_VALUES = {route.value: route for route in ExtractionRoute}


# ---------------------------------------------------------------------------
# Ошибки
# ---------------------------------------------------------------------------


class EdgeError(RuntimeError):
    """Базовый сбой edge-операции (все ошибки сервера — подклассы)."""


class EdgeRequestError(EdgeError):
    """Некорректный/неизвестный запрос (плохой op, payload, параметры)."""


class EdgeAdapterError(EdgeError):
    """Сбой инъецированного адаптера; наружу — только сообщение, без traceback."""


# ---------------------------------------------------------------------------
# Утилиты (детерминизм и нейтральность)
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    """Текущий UTC-таймстемп ISO-8601 (дефолтные часы, если адаптер не дал ``fetched_at``)."""
    return datetime.now(timezone.utc).isoformat()


def _default_id_factory() -> str:
    """Дефолтный генератор ``request_id`` — uuid4 hex (корреляция в аудите, 07 §8)."""
    return uuid.uuid4().hex


def _coerce_fetch(value: Any) -> FetchMode:
    """Приводит ``fetch`` из payload к ``FetchMode`` (дефолт — http, 07 §3)."""
    if value is None or value == "":
        return FetchMode.HTTP
    mode = _FETCH_VALUES.get(str(value).strip().lower())
    if mode is None:
        raise EdgeRequestError(f"fetch: неизвестный режим получения: {value!r}")
    return mode


def _coerce_extract(value: Any) -> ExtractionRoute:
    """Приводит ``extract`` из payload к ``ExtractionRoute`` (дефолт — auto, 07 §3)."""
    if value is None or value == "":
        return ExtractionRoute.AUTO
    route = _EXTRACT_VALUES.get(str(value).strip().lower())
    if route is None:
        raise EdgeRequestError(f"fetch: неизвестный маршрут извлечения: {value!r}")
    return route


def _coerce_wait_for(value: Any) -> str | None:
    """Приводит ``wait_for`` из payload к строке JS-ожидания либо ``None``."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_int(value: Any, *, default: int) -> int:
    """Приводит числовое поле payload к ``int``; нечисло → ``EdgeRequestError``."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise EdgeRequestError(f"нечисловое значение: {value!r}") from exc


def _fact_json(value: Any) -> str:
    """``fact_json`` — строка (домен); dict/list сериализуется детерминированно (сорт ключей)."""
    if isinstance(value, str):
        return value
    if value is None:
        return "{}"
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _product_ref(raw: Any) -> ProductRef:
    """Собирает ``ProductRef`` из ``product_ref`` payload ``/v1/registry``."""
    if not isinstance(raw, Mapping):
        raise EdgeRequestError("registry: product_ref обязан быть отображением")
    extra_raw = raw.get("extra") or {}
    extra = (
        {str(key): str(value) for key, value in extra_raw.items()}
        if isinstance(extra_raw, Mapping)
        else {}
    )
    return ProductRef(
        product_id=str(raw.get("product_id") or ""),
        canonical_name=str(raw.get("canonical_name") or ""),
        vendor=str(raw.get("vendor") or ""),
        jurisdiction=str(raw.get("jurisdiction") or ""),
        extra=extra,
    )


def _normalize_adapters(value: Any, *, key_attr: str | None = None) -> dict[str, Any]:
    """Нормализует инъецированный адаптер(ы) в ``{key: adapter}``.

    Поддерживаются: ``None`` (пусто), отображение (ключ→адаптер), последовательность
    (ключ — ``getattr(item, key_attr)`` или индекс) и одиночный объект.
    """
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        normalized: dict[str, Any] = {}
        for index, item in enumerate(value):
            key = None
            if key_attr is not None:
                key = getattr(item, key_attr, None)
            normalized[str(key if key not in (None, "") else index)] = item
        return normalized
    key = ""
    if key_attr is not None:
        key = str(getattr(value, key_attr, "") or "")
    return {key: value}


# ---------------------------------------------------------------------------
# Ядро сервера
# ---------------------------------------------------------------------------


class EdgeGateway:
    """Публикует реальные веб-адаптеры по протоколу ``/v1/*`` (07 §3/§4).

    Ручной DI (A1): все адаптеры инъецируются конструктором. Экземпляр **не**
    касается хранилища/домена — только вызывает переданные порты. Детерминирован:
    ``clock`` и ``id_factory`` тоже инъецируются.

    :param crawler: реальный ``CrawlerPort`` (Scrapy и т.п.), исполняется в DMZ.
    :param extractor: реальный ``ExtractorPort`` (HTML/документы) — одиночный объект,
        отображение ``format→extractor`` или последовательность.
    :param searchers: SERP-адаптер(ы); одиночный объект, отображение/последовательность.
    :param registries: реестровый фетчер(ы); одиночный объект (ключ — ``registry_id``),
        отображение или последовательность.
    :param clock: часы для ``fetched_at`` (дефолт — UTC ISO-8601).
    :param id_factory: генератор ``request_id`` (дефолт — uuid4 hex).
    :param search_engine: имя поискового движка для ``/v1/capabilities``.
    :param extractor_version: переопределение версии экстрактора для capabilities.
    :param supported_formats: переопределение списка форматов для capabilities.
    :param schema_version: версия конверта (по умолчанию ``EDGE_SCHEMA_VERSION``).
    """

    def __init__(
        self,
        crawler: CrawlerPort,
        extractor: ExtractorPort | Mapping[str, ExtractorPort] | Sequence[ExtractorPort],
        *,
        searchers: SearchAdapterPort
        | Mapping[str, SearchAdapterPort]
        | Sequence[SearchAdapterPort]
        | None = None,
        registries: Any = None,
        clock: Callable[[], str] | None = None,
        id_factory: Callable[[], str] | None = None,
        search_engine: str | None = None,
        extractor_version: str | None = None,
        supported_formats: Sequence[str] | None = None,
        schema_version: str = EDGE_SCHEMA_VERSION,
    ) -> None:
        self._crawler = crawler
        self._extractors = _normalize_adapters(extractor)
        self._searchers = _normalize_adapters(searchers, key_attr="provider")
        self._registries = _normalize_adapters(registries, key_attr="registry_id")
        self._clock = clock or _utc_now_iso
        self._id_factory = id_factory or _default_id_factory
        self._search_engine = search_engine
        self._extractor_version = extractor_version
        self._supported_formats = (
            list(supported_formats) if supported_formats is not None else None
        )
        self.schema_version = schema_version

    # -- идентификаторы --------------------------------------------------------

    def _new_id(self) -> str:
        """Новый ``request_id`` (корреляция; не идемпотентность)."""
        return str(self._id_factory())

    # -- capabilities ----------------------------------------------------------

    def capabilities(self) -> dict[str, Any]:
        """Версии edge-компонентов для манифеста (``/v1/capabilities``, 07 §3/§7)."""
        return {
            "extractor_version": self._primary_extractor_version(),
            "crawler_version": str(getattr(self._crawler, "version", "") or ""),
            "search_engine": self._search_engine_name(),
            "supported_formats": self._supported_formats_list(),
        }

    def _primary_extractor_version(self) -> str:
        if self._extractor_version is not None:
            return str(self._extractor_version)
        for extractor in self._extractors.values():
            version = getattr(extractor, "version", None)
            if version:
                return str(version)
        return ""

    def _search_engine_name(self) -> str:
        if self._search_engine is not None:
            return str(self._search_engine)
        for searcher in self._searchers.values():
            for attr in ("provider", "adapter_kind"):
                value = getattr(searcher, attr, None)
                if value:
                    return str(value)
        return ""

    def _supported_formats_list(self) -> list[str]:
        if self._supported_formats is not None:
            return list(self._supported_formats)
        formats: list[str] = ["html"]
        for extractor in self._extractors.values():
            declared = getattr(extractor, "SUPPORTED_FORMATS", None)
            if isinstance(declared, Mapping):
                formats.extend(str(key).lstrip(".") for key in declared)
        # Дедуп с сохранением порядка (нейтрально, детерминированно).
        return list(dict.fromkeys(formats))

    # -- конверт ---------------------------------------------------------------

    def envelope(
        self,
        op: str,
        payload: Mapping[str, Any],
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Оборачивает результат ``handle`` в конверт 07 §3.

        ``request_id`` при наличии эхо-возвращается (сквозная корреляция, 07 §8.2);
        иначе генерируется. Любой сбой адаптера/запроса → ``status="error"`` с
        текстовым сообщением **без traceback** (07 §5: ядро не должно видеть
        внутренности edge).
        """
        rid = str(request_id) if request_id else self._new_id()
        try:
            inner = self.handle(op, payload)
        except EdgeError as exc:
            return self._envelope(rid, "error", None, str(exc))
        except Exception as exc:  # noqa: BLE001 - унификация: наружу только сообщение
            return self._envelope(rid, "error", None, f"{type(exc).__name__}: {exc}")
        if op == "fetch" and isinstance(inner, Mapping):
            # 07 §3: request_id — первое поле payload /v1/fetch.
            inner = {"request_id": rid, **inner}
        return self._envelope(rid, "ok", inner, None)

    def error_envelope(
        self, error: Any, *, request_id: str | None = None
    ) -> dict[str, Any]:
        """Конверт ошибки для транспортного слоя (маршрут/метод/JSON)."""
        rid = str(request_id) if request_id else self._new_id()
        return self._envelope(rid, "error", None, str(error))

    def _envelope(
        self, request_id: str, status: str, payload: Any, error: str | None
    ) -> dict[str, Any]:
        """Собирает конверт 07 §3 (имена полей — как у клиента)."""
        return {
            "schema_version": self.schema_version,
            "request_id": request_id,
            "status": status,
            "payload": payload,
            "error": error,
        }

    # -- чистая диспетчеризация ------------------------------------------------

    def handle(self, op: str, payload: Mapping[str, Any]) -> Any:
        """Чистая диспетчеризация по ``fetch``/``search``/``registry``/``capabilities``/``health``.

        Возвращает **payload** конверта (оборачивает ``envelope``). Неизвестный op →
        ``EdgeRequestError``. Сбои адаптеров становятся ``EdgeAdapterError``.
        """
        if not isinstance(payload, Mapping):
            raise EdgeRequestError("payload обязан быть отображением")
        handler = {
            "fetch": self._handle_fetch,
            "search": self._handle_search,
            "registry": self._handle_registry,
            "capabilities": self._handle_capabilities,
            "health": self._handle_health,
        }.get(op)
        if handler is None:
            raise EdgeRequestError(f"неизвестная операция: {op!r}")
        return handler(payload)

    @staticmethod
    def _handle_health(payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/health`` → ``{status}`` (07 §3)."""
        return {"status": "ok"}

    def _handle_capabilities(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/capabilities`` → версии компонентов (07 §3)."""
        return self.capabilities()

    # -- fetch -----------------------------------------------------------------

    def _handle_fetch(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/fetch`` → сырые байты (+ очищенный текст по маршруту) (07 §3).

        Оси ``fetch``/``extract`` берутся из запроса и складываются в ``FetchPlan``.
        Сырые байты возвращаются всегда; извлечение best-effort: сбой экстрактора не
        лишает ядро retеншн-байтов, а переносится в ``error`` (ядро решает, что делать).
        """
        url = payload.get("url")
        if not url:
            raise EdgeRequestError("fetch: поле url обязательно")
        violations = validate_fetch_request(payload)
        if violations:
            raise EdgeRequestError("; ".join(violations))
        plan = FetchPlan(
            fetch=_coerce_fetch(payload.get("fetch")),
            extract=_coerce_extract(payload.get("extract")),
            wait_for=_coerce_wait_for(payload.get("wait_for")),
        )

        try:
            page = self._crawler.fetch(str(url), plan)
        except EdgeError:
            raise
        except Exception as exc:  # noqa: BLE001 - грациозная граница адаптера
            raise EdgeAdapterError(f"fetch: {exc}") from exc

        content = bytes(getattr(page, "content", b"") or b"")
        raw = {
            "hash": content_hash(content),
            "size": len(content),
            # Явные байты для retention/инспекции (07 §8.1); ядро делает re-hash (I19).
            "bytes_b64": base64.b64encode(content).decode("ascii"),
        }
        text_obj: dict[str, Any] | None = None
        extract_error: str | None = None
        if self._extractors:
            try:
                text_obj = self._extract_text(page, plan.extract)
            except EdgeError as exc:
                extract_error = str(exc)
        return {
            "final_url": str(getattr(page, "url", None) or url),
            "status_code": getattr(page, "status_code", None),
            "content_type": getattr(page, "content_type", None),
            "fetched_at": str(getattr(page, "fetched_at", None) or self._clock()),
            "raw": raw,
            "text": text_obj,
            "error": extract_error,
        }

    def _extract_text(self, page: Any, route: ExtractionRoute) -> dict[str, Any]:
        """Запускает инъецированный экстрактор по маршруту; ``.error`` → ``EdgeAdapterError``."""
        extractor = self._select_extractor(page)
        try:
            doc = extractor.extract(page, route)
        except EdgeError:
            raise
        except Exception as exc:  # noqa: BLE001 - грациозная граница адаптера
            raise EdgeAdapterError(f"extract: {exc}") from exc

        error = getattr(doc, "error", None)
        if error:
            raise EdgeAdapterError(f"extract: {error}")
        content = getattr(doc, "text", "")
        content_str = "" if content is None else str(content)
        claimed_hash = getattr(doc, "content_hash", None)
        return {
            "content": content_str,
            "hash": str(claimed_hash) if claimed_hash else content_hash(content_str),
            "extractor_version": str(
                getattr(doc, "extractor_version", None) or self._primary_extractor_version()
            ),
        }

    # -- search ----------------------------------------------------------------

    def _handle_search(self, payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        """``/v1/search`` → кандидаты (без evidence, I19; 07 §3)."""
        text = payload.get("query")
        if not text or not str(text).strip():
            raise EdgeRequestError("search: поле query обязательно")
        query = SearchQuery(
            text=str(text),
            lang=str(payload.get("lang") or "ru"),
            limit=_coerce_int(payload.get("limit"), default=10),
            phase=str(payload.get("phase") or "corpus"),
        )
        adapter = self._select_searcher(payload)
        try:
            results = adapter.search(query)
        except EdgeError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise EdgeAdapterError(f"search: {exc}") from exc
        return [
            {
                "url": str(getattr(result, "url", "") or ""),
                "title": str(getattr(result, "title", "") or ""),
                "snippet": str(getattr(result, "snippet", "") or ""),
                "rank": _coerce_int(getattr(result, "rank", 0), default=0),
            }
            for result in results
        ]

    # -- registry --------------------------------------------------------------

    def _handle_registry(self, payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        """``/v1/registry`` → факты реестра; R1: ``source_url`` обязателен (07 §3)."""
        registry_id = payload.get("registry_id")
        if not registry_id:
            raise EdgeRequestError("registry: поле registry_id обязательно")
        product = _product_ref(payload.get("product_ref") or {})
        scope = dict(payload.get("scope") or {})
        fetcher = self._select_registry(str(registry_id))
        try:
            facts = fetcher.fetch(product, scope)
        except EdgeError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise EdgeAdapterError(f"registry: {exc}") from exc

        items: list[dict[str, Any]] = []
        for fact in facts:
            source_url = getattr(fact, "source_url", None)
            if not source_url:
                raise EdgeAdapterError("registry: R1 — факт без source_url")
            items.append(
                {
                    "fact_json": _fact_json(getattr(fact, "fact_json", None)),
                    "source_url": str(source_url),
                    "fetched_at": getattr(fact, "fetched_at", None),
                }
            )
        return items

    # -- выбор инъецированного адаптера ----------------------------------------

    def _select_extractor(self, page: Any) -> Any:
        """Выбирает экстрактор по расширению URL (или дефолтный)."""
        if not self._extractors:
            raise EdgeAdapterError("extractor не сконфигурирован")
        url = str(getattr(page, "url", "") or "")
        suffix = _url_suffix(url)
        for key in (suffix, "default", ""):
            if key and key in self._extractors:
                return self._extractors[key]
        if "" in self._extractors:
            return self._extractors[""]
        if suffix:
            for extractor in self._extractors.values():
                declared = getattr(extractor, "SUPPORTED_FORMATS", None)
                if isinstance(declared, Mapping) and f".{suffix}" in declared:
                    return extractor
        return next(iter(self._extractors.values()))

    def _select_searcher(self, payload: Mapping[str, Any]) -> Any:
        """Выбирает SERP-адаптер: явный ``engine`` из payload, иначе дефолтный/первый."""
        if not self._searchers:
            raise EdgeAdapterError("search-адаптер не сконфигурирован")
        engine = payload.get("engine")
        if engine is not None and str(engine) in self._searchers:
            return self._searchers[str(engine)]
        if "" in self._searchers:
            return self._searchers[""]
        return next(iter(self._searchers.values()))

    def _select_registry(self, registry_id: str) -> Any:
        """Выбирает реестровый фетчер по ``registry_id`` (ключ, дефолт или атрибут)."""
        if not self._registries:
            raise EdgeAdapterError("реестровые фетчеры не сконфигурированы")
        if registry_id in self._registries:
            return self._registries[registry_id]
        if "" in self._registries:
            return self._registries[""]
        for fetcher in self._registries.values():
            if str(getattr(fetcher, "registry_id", "") or "") == registry_id:
                return fetcher
        raise EdgeAdapterError(f"реестр {registry_id!r} не сконфигурирован")


def _url_suffix(url: str) -> str:
    """Расширение файла из URL без ведущей точки и в нижнем регистре (``pdf``)."""
    path = url.split("?", 1)[0].split("#", 1)[0]
    tail = path.rsplit("/", 1)[-1]
    if "." not in tail:
        return ""
    return tail.rsplit(".", 1)[-1].strip().lower()


# ---------------------------------------------------------------------------
# Тонкий HTTP-слой (stdlib; L7-inspectable JSON, 07 §3/§8.1)
# ---------------------------------------------------------------------------


def _parse_op(path: str) -> str | None:
    """Извлекает ``op`` из ``/v1/{op}`` (query/fragment игнорируются)."""
    path_only = (path or "").split("?", 1)[0].split("#", 1)[0]
    prefix = "/v1/"
    if not path_only.startswith(prefix):
        return None
    op = path_only[len(prefix):].strip("/")
    return op or None


def _decode_json_body(body: Any) -> tuple[dict[str, Any] | None, str | None]:
    """Разбирает JSON-тело запроса; ``(payload, None)`` либо ``(None, error)``."""
    if body is None:
        return {}, None
    if isinstance(body, str):
        body = body.encode("utf-8")
    if not body:
        return {}, None
    try:
        data = json.loads(bytes(body).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return None, f"некорректный JSON: {exc}"
    if not isinstance(data, Mapping):
        return None, "тело запроса обязано быть JSON-объектом"
    return dict(data), None


def route_request(
    gateway: EdgeGateway,
    method: str,
    path: str,
    body: Any = b"",
    *,
    request_id: str | None = None,
) -> tuple[int, dict[str, Any]]:
    """Чистая маршрутизация HTTP → конверт: ``(http_status, envelope)``.

    Отделена от сокета, чтобы тесты не открывали порт. Прикладные сбои остаются
    HTTP 200 с ``envelope.status == "error"``; 4xx — только транспортные
    (нет маршрута/метода/валидного JSON).
    """
    method = (method or "").upper()
    op = _parse_op(path)
    if op is None or op not in _KNOWN_OPS:
        return 404, gateway.error_envelope(f"маршрут не найден: {path!r}", request_id=request_id)

    if method == "POST":
        if op not in _POST_OPS:
            return 405, gateway.error_envelope(
                f"POST не поддерживается для {op!r}", request_id=request_id
            )
        payload, error = _decode_json_body(body)
        if error is not None:
            return 400, gateway.error_envelope(error, request_id=request_id)
        assert payload is not None  # гарантировано ветвлением выше
    elif method == "GET":
        if op not in _GET_OPS:
            return 405, gateway.error_envelope(
                f"GET не поддерживается для {op!r}", request_id=request_id
            )
        payload = {}
    else:
        return 405, gateway.error_envelope(
            f"метод {method!r} не поддерживается", request_id=request_id
        )

    return 200, gateway.envelope(op, payload, request_id=request_id)


def _content_length(headers: Any) -> int:
    """Content-Length запроса как неотрицательное ``int`` (0 на битое/отсутствие)."""
    raw = headers.get("Content-Length")
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


def make_handler(
    gateway: EdgeGateway, *, request_id_header: str = REQUEST_ID_HEADER
) -> type[BaseHTTPRequestHandler]:
    """Фабрика ``BaseHTTPRequestHandler`` поверх ``route_request`` (тестируемо без сокета).

    POST ``/v1/{fetch,search,registry}`` и GET ``/v1/{health,capabilities}``;
    ``X-Request-Id`` запроса эхо-возвращается в конверте (07 §8.2).
    """

    class _EdgeRequestHandler(BaseHTTPRequestHandler):
        server_version = "coHound-edge/1"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        def _dispatch(self, method: str) -> None:
            length = _content_length(self.headers)
            body = self.rfile.read(length) if length > 0 else b""
            rid = self.headers.get(request_id_header)
            code, envelope = route_request(
                gateway, method, self.path, body, request_id=rid
            )
            data = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802 - контракт BaseHTTPRequestHandler
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802 - контракт BaseHTTPRequestHandler
            self._dispatch("POST")

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            """Глушит стандартный лог (аудит ведётся отдельным каналом, 07 §8.4)."""

    return _EdgeRequestHandler


def build_server(
    host: str,
    port: int,
    gateway: EdgeGateway,
    *,
    server_factory: Callable[..., Any] = ThreadingHTTPServer,
    handler_factory: Callable[..., type[BaseHTTPRequestHandler]] = make_handler,
) -> Any:
    """Создаёт HTTP-сервер (без запуска). ``server_factory`` инъецируется для тестов."""
    handler_cls = handler_factory(gateway)
    return server_factory((host, port), handler_cls)


def serve(
    host: str,
    port: int,
    gateway: EdgeGateway,
    *,
    server_factory: Callable[..., Any] = ThreadingHTTPServer,
    handler_factory: Callable[..., type[BaseHTTPRequestHandler]] = make_handler,
) -> None:
    """Запускает блокирующий HTTP-сервер ``/v1/*`` (stdlib).

    Опциональный режим развёртывания ``split`` (07 §6). Сокет поднимает только
    ``serve``; ``EdgeGateway`` и ``route_request`` тестируются без сети.
    """
    server = build_server(
        host,
        port,
        gateway,
        server_factory=server_factory,
        handler_factory=handler_factory,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()
