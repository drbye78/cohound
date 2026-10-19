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
import ipaddress
import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from core.edge.audit import (
    AUDIT_STATUS_ALLOW,
    AUDIT_STATUS_DENY,
    AUDIT_STATUS_ERROR,
    AuditSink,
    Classification,
    audit_call,
)
from core.edge.auth import (
    AuthRequest,
    Authenticator,
    Principal,
    PUBLIC_OPS,
    authorize,
)
from core.edge.seam_schema import validate_crawl_request, validate_fetch_request
from core.ports.crawler import CrawlerPort
from core.ports.extractor import ExtractorPort
from core.ports.search import SearchAdapterPort, SearchQuery
from core.ports.types import ExtractionRoute, FetchMode, FetchPlan, ProductRef
from core.util.hashing import content_hash

__all__ = [
    "EDGE_SCHEMA_VERSION",
    "DEFAULT_MAX_BODY_BYTES",
    "EdgeError",
    "EdgeRequestError",
    "EdgeAdapterError",
    "EdgeSecurityError",
    "EdgeGateway",
    "is_loopback_host",
    "ensure_safe_binding",
    "route_request",
    "make_handler",
    "build_server",
    "serve",
]

#: Версия конверта (07 §3/§7/§8.3). Совпадает с клиентским дефолтом
#: (``core/adapters/remote_gateway.DEFAULT_SCHEMA_VERSION``) — версионная граница.
EDGE_SCHEMA_VERSION = "1"

#: Потолок тела запроса по умолчанию, байты (1 MiB).
#:
#: Обоснование — по самой крупной легитимной форме запроса. Тело несут только
#: POST-операции; единственная «массовая» из них — ``crawl/submit``, где ``items``
#: — список работ одной волны. Ядро по умолчанию подаёт волну
#: ``_EXECUTOR_WAVE_ITEMS = 64`` элементов (``core/pipeline/corpus.py``), каждый —
#: ``{product_id, url, engine, kind, fetch, extract, strategy, wait_for, lang,
#: robots, rate_limit}`` (``crawl.schema.json#/$defs/submit_item``); при URL ~2 КБ
#: это ~128 КБ. Остальные запросы (``fetch``/``search``/``registry``/``crawl/*``)
#: несут единицы полей и на порядки меньше. 1 MiB даёт ~8× запас над волной
#: дефолтного размера (и покрывает увеличенные волны) и при этом не превращает
#: DMZ в приёмник произвольных объёмов. Оператор переопределяет
#: ``max_body_bytes`` в ``edge.yaml``.
DEFAULT_MAX_BODY_BYTES = 1_048_576

#: Операции, которые в протоколе edge принимают тело (07 §3).
#: Crawl-операции очереди краула (Phase 3.1) и опрос фидов (Wave 5.1) — тоже POST.
_POST_OPS = frozenset(
    {
        "fetch",
        "search",
        "registry",
        "crawl/submit",
        "crawl/status",
        "crawl/cancel",
        "crawl/collect",
        "crawl/poll",
    }
)
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


class EdgeSecurityError(EdgeError):
    """Небезопасная конфигурация шва: отказ запуска (fail-fast).

    Возникает, когда сервер просят привязать к **не-loopback** адресу без
    настроенной аутентификации: публиковать неаутентифицированный шов в
    маршрутизируемый интерфейс — это и есть фактическая угроза (07 §8.2).
    """


# ---------------------------------------------------------------------------
# Утилиты (детерминизм и нейтральность)
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    """Текущий UTC-таймстемп ISO-8601 (дефолтные часы, если адаптер не дал ``fetched_at``)."""
    return datetime.now(timezone.utc).isoformat()


def is_loopback_host(host: str | None) -> bool:
    """Является ли ``host`` петлевым адресом (``True`` — можно без аутентификации).

    Петлевыми считаются **литеральные** адреса с ``ipaddress.is_loopback``
    (весь ``127.0.0.0/8`` и ``::1``) и имя ``localhost`` (регистронезависимо).
    Имена не разрешаются через DNS: разрешение сделало бы проверку
    непредсказуемой и зависимой от окружения, а «localhost» в конфиге и так
    означает петлю по соглашению. Всё остальное (включая ``0.0.0.0``, ``::`` и
    внешние имена) — **не** loopback.
    """
    text = str(host or "").strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].strip()
    if not text:
        return False
    if text.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(text).is_loopback
    except ValueError:
        return False


def ensure_safe_binding(host: str, authenticator: Authenticator | None) -> None:
    """Fail-fast: неаутентифицированный шов нельзя публиковать вне петли.

    ``authenticator is None`` — законный дефолт локального профиля, но только на
    петлевом адресе (:func:`is_loopback_host`). Привязка к любому иному адресу
    без аутентификации отвергается :class:`EdgeSecurityError` **до** создания
    сокета: «забыли настроить auth» не должно молча превращаться в открытый
    веб-эндпоинт (07 §8.2).
    """
    if authenticator is None and not is_loopback_host(host):
        raise EdgeSecurityError(
            f"отказ запуска: host={host!r} не является loopback, "
            "а аутентификация не настроена (auth.mode=none); "
            "настройте auth (mtls/oidc) или слушайте 127.0.0.1/::1/localhost"
        )


def _audit_event(
    sink: AuditSink | None,
    *,
    request_id: str,
    principal: Principal | None,
    op: str,
    status: str,
    payload: Mapping[str, Any] | None,
    remote: str | None,
    classification: Classification | None,
) -> None:
    """Пишет ровно одно аудит-событие вызова (``sink=None`` — no-op).

    ``classification`` — операторский маркер; метка для конкретного вызова
    вычисляется через :meth:`Classification.for_call` (07 §8.5). В событие не
    попадают ни тело, ни заголовки авторизации: только ``op``/``status``/
    ``request_id``/субъект/peer/метка (07 §8.4).
    """
    if sink is None:
        return
    marker = classification if classification is not None else Classification()
    audit_call(
        sink,
        request_id=request_id,
        principal=principal,
        op=op,
        status=status,
        classification=marker.for_call(op, payload or {}),
        remote=remote,
    )


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


def _coerce_opt_str(value: Any) -> str | None:
    """Приводит опциональное строковое поле payload к строке либо ``None``."""
    if value is None:
        return None
    text = str(value)
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
    :param scheduler: необязательный компонент очереди краула (Phase 3.1; Wave 3.2) с
        методами ``submit(request)``, ``status(job_id)``, ``cancel(job_id)``,
        ``collect(job_id, cursor=…, limit=…)``. При ``None`` crawl-операции отвечают
        ``status=error`` (планировщик недоступен на этом edge-узле).
    :param poller: необязательный сервис опроса фидов/каналов (Wave 5.1) с методом
        ``poll(request) -> {entries, count, truncated}``. При ``None`` операция
        ``crawl/poll`` отвечает ``status=error`` (сервис недоступен на этом edge-узле).
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
        scheduler: Any = None,
        poller: Any = None,
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
        self._scheduler = scheduler
        self._poller = poller

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
        """Чистая диспетчеризация по ``fetch``/``search``/``registry``/``capabilities``/``health``
        и crawl-операциям ``crawl/{submit,status,cancel,collect}`` (Phase 3.1).

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
            "crawl/submit": self._handle_crawl_submit,
            "crawl/status": self._handle_crawl_status,
            "crawl/cancel": self._handle_crawl_cancel,
            "crawl/collect": self._handle_crawl_collect,
            "crawl/poll": self._handle_crawl_poll,
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
            if_none_match=_coerce_opt_str(payload.get("if_none_match")),
            if_modified_since=_coerce_opt_str(payload.get("if_modified_since")),
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
            # Валидаторы условного запроса (additive, 07 §3/§8).
            "etag": getattr(page, "etag", None),
            "last_modified": getattr(page, "last_modified", None),
            "not_modified": bool(getattr(page, "not_modified", False)),
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

    # -- crawl (Phase 3.1; планировщик Wave 3.2 инъецируется опционально) ----------

    def _require_crawl_scheduler(self) -> Any:
        """Возвращает инъецированный планировщик краула либо бросает ``EdgeError``."""
        if self._scheduler is None:
            raise EdgeError("crawl scheduler is not available on this edge node")
        return self._scheduler

    def _call_crawl(self, method: str, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        """Вызывает метод планировщика и нормализует результат в отображение.

        Сбой планировщика (кроме :class:`EdgeError`) → :class:`EdgeAdapterError`
        без traceback (07 §5).
        """
        scheduler = self._require_crawl_scheduler()
        try:
            result = getattr(scheduler, method)(*args, **kwargs)
        except EdgeError:
            raise
        except Exception as exc:  # noqa: BLE001 - грациозная граница адаптера
            raise EdgeAdapterError(f"crawl/{method}: {exc}") from exc
        if not isinstance(result, Mapping):
            raise EdgeAdapterError(
                f"crawl/{method}: планировщик вернул не-объект "
                f"({type(result).__name__})"
            )
        return result

    def _crawl_counts(self, result: Mapping[str, Any], fields: Sequence[str]) -> dict[str, int]:
        """Целочисленные счётчики из ответа планировщика (дефолт 0)."""
        return {field: self._crawl_int(result.get(field), default=0) for field in fields}

    @staticmethod
    def _crawl_int(value: Any, *, default: int) -> int:
        """Мягкое приведение счётчика к ``int`` (нечисло → ``default``)."""
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _crawl_str_list(value: Any) -> list[str]:
        """Список строк (порядок сохраняется); иное → ``[]``."""
        if not isinstance(value, (list, tuple)):
            return []
        return [str(item) for item in value]

    @staticmethod
    def _crawl_opt_str(value: Any) -> str | None:
        """Опциональное строковое поле (``None``/пустое → ``None``); иначе ``str``."""
        if value is None:
            return None
        text = str(value)
        return text or None

    @staticmethod
    def _crawl_collect_item(item: Any) -> dict[str, Any]:
        """Нормализует элемент ``items`` ответа ``crawl/collect``."""
        if not isinstance(item, Mapping):
            raise EdgeAdapterError("crawl/collect: элемент items обязан быть объектом")
        raw = item.get("raw")
        normalized_raw: dict[str, Any] | None = None
        if isinstance(raw, Mapping):
            normalized_raw = {
                "hash": str(raw.get("hash") or ""),
                "size": EdgeGateway._crawl_int(raw.get("size"), default=0),
                "bytes_b64": raw.get("bytes_b64"),
            }
        return {
            "product_id": str(item.get("product_id") or ""),
            "url": str(item.get("url") or ""),
            "status": item.get("status"),
            "engine": item.get("engine"),
            "kind": item.get("kind"),
            "http_status": item.get("http_status"),
            "content_type": item.get("content_type"),
            "fetched_at": item.get("fetched_at"),
            "content_hash": item.get("content_hash"),
            "final_url": EdgeGateway._crawl_opt_str(item.get("final_url")),
            "etag": EdgeGateway._crawl_opt_str(item.get("etag")),
            "last_modified": EdgeGateway._crawl_opt_str(item.get("last_modified")),
            "not_modified": bool(item.get("not_modified")),
            "raw": normalized_raw,
            "error": item.get("error"),
        }

    def _handle_crawl_submit(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/crawl/submit`` → ``{job_id, accepted, total, session_id}`` (Phase 3.1).

        Планировщик не реализуется здесь (Wave 3.2): при его отсутствии —
        ``EdgeError`` (конверт ``status=error``), при наличии — делегирование.
        """
        self._check_crawl_request("crawl/submit", payload)
        result = self._call_crawl("submit", payload)
        return {
            "job_id": str(result.get("job_id") or ""),
            "accepted": bool(result.get("accepted")),
            "total": self._crawl_int(result.get("total"), default=0),
            "session_id": str(result.get("session_id") or payload.get("session_id") or ""),
        }

    def _handle_crawl_status(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/crawl/status`` → счётчики очереди для ``job_id`` (Phase 3.1)."""
        self._check_crawl_request("crawl/status", payload)
        job_id = str(payload["job_id"])
        result = self._call_crawl("status", job_id)
        response: dict[str, Any] = {
            "job_id": str(result.get("job_id") or job_id),
            "status": str(result.get("status") or "unknown"),
        }
        response.update(
            self._crawl_counts(
                result, ("total", "pending", "running", "fetched", "failed", "cancelled")
            )
        )
        response["engines"] = self._crawl_str_list(result.get("engines"))
        response["kinds"] = self._crawl_str_list(result.get("kinds"))
        return response

    def _handle_crawl_cancel(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/crawl/cancel`` → ``{job_id, status:"cancelled", cancelled}`` (Phase 3.1)."""
        self._check_crawl_request("crawl/cancel", payload)
        job_id = str(payload["job_id"])
        result = self._call_crawl("cancel", job_id)
        return {
            "job_id": str(result.get("job_id") or job_id),
            "status": "cancelled",
            "cancelled": self._crawl_int(result.get("cancelled"), default=0),
        }

    def _handle_crawl_collect(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/crawl/collect`` → ``{items, next_cursor, done}`` — контент, не evidence (I19)."""
        self._check_crawl_request("crawl/collect", payload)
        job_id = str(payload["job_id"])
        cursor = payload.get("cursor")
        limit = payload.get("limit")
        result = self._call_crawl(
            "collect", job_id, cursor=cursor, limit=limit
        )
        items = result.get("items")
        if not isinstance(items, (list, tuple)):
            raise EdgeAdapterError("crawl/collect: поле items обязано быть списком")
        next_cursor = result.get("next_cursor")
        return {
            "items": [self._crawl_collect_item(item) for item in items],
            "next_cursor": None if next_cursor is None else str(next_cursor),
            "done": bool(result.get("done")),
        }

    def _handle_crawl_poll(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """``/v1/crawl/poll`` → ``{entries, count, truncated}`` — ссылки, не evidence (I19).

        Фетчится **только каталоговый документ** фида/канала; записи возвращаются как
        ссылки (URL+метаданные), без байтов. Сервис инъецируется (``poller``); при его
        отсутствии — ``EdgeError`` (сервис недоступен на этом edge-узле).
        """
        self._check_crawl_request("crawl/poll", payload)
        if self._poller is None:
            raise EdgeError("crawl poller is not available on this edge node")
        try:
            result = self._poller.poll(payload)
        except EdgeError:
            raise
        except Exception as exc:  # noqa: BLE001 - грациозная граница адаптера
            raise EdgeAdapterError(f"crawl/poll: {exc}") from exc
        if not isinstance(result, Mapping):
            raise EdgeAdapterError(
                f"crawl/poll: сервис вернул не-объект ({type(result).__name__})"
            )
        entries = result.get("entries")
        if not isinstance(entries, (list, tuple)):
            raise EdgeAdapterError("crawl/poll: поле entries обязано быть списком")
        return {
            "entries": [self._crawl_poll_entry(item) for item in entries],
            "count": self._crawl_int(result.get("count"), default=0),
            "truncated": bool(result.get("truncated")),
        }

    @staticmethod
    def _crawl_poll_entry(item: Any) -> dict[str, Any]:
        """Нормализует элемент ``entries`` ответа ``crawl/poll`` (URL обязателен)."""
        if not isinstance(item, Mapping):
            raise EdgeAdapterError("crawl/poll: элемент entries обязан быть объектом")
        url = item.get("url")
        if not isinstance(url, str) or not url:
            raise EdgeAdapterError("crawl/poll: элемент entries требует непустой url")
        return {
            "url": url,
            "guid": EdgeGateway._crawl_opt_str(item.get("guid")),
            "published_at": EdgeGateway._crawl_opt_str(item.get("published_at")),
            "title": EdgeGateway._crawl_opt_str(item.get("title")),
        }

    @staticmethod
    def _check_crawl_request(op: str, payload: Mapping[str, Any]) -> None:
        """Валидирует запрос crawl-операции; нарушения → :class:`EdgeRequestError`."""
        violations = validate_crawl_request(op, payload)
        if violations:
            raise EdgeRequestError("; ".join(violations))

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
    authenticator: Authenticator | None = None,
    auth_request: AuthRequest | None = None,
    audit_sink: AuditSink | None = None,
    classification: Classification | None = None,
    remote: str | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> tuple[int, dict[str, Any]]:
    """Чистая маршрутизация HTTP → конверт: ``(http_status, envelope)``.

    Отделена от сокета, чтобы тесты не открывали порт. Прикладные сбои остаются
    HTTP 200 с ``envelope.status == "error"``; 4xx — транспортные/охранные
    (нет маршрута/метода/валидного JSON, отказ аутентификации/авторизации,
    превышение лимита тела).

    **Порядок операций (важен):**

    1. разрешение ``op`` и проверка маршрута/метода — 404/405;
    2. **аутентификация** (если ``authenticator`` задан и операция не публичная):
       сбой → 401, конверт ``status="error"``, аудит ``DENY``;
    3. **авторизация** ``principal`` по ``op`` через :func:`authorize`:
       нет права → 403, аудит ``DENY``;
    4. **лимит тела** ``max_body_bytes`` → 413, аудит ``ERROR``;
    5. разбор JSON → 400, аудит ``ERROR``;
    6. диспетчеризация в :meth:`EdgeGateway.envelope`;
    7. **аудит** ровно одного события с финальным статусом (``ALLOW``/``ERROR``).

    Функция остаётся **чистой**: состояние (аутентификатор, sink, лимит)
    приходит параметрами, а ``request_id`` генерируется инъецированным в gateway
    ``id_factory``. Один и тот же ``request_id`` попадает и в конверт, и в
    аудит-событие — второго идентификатора для одного вызова не существует.
    При ``authenticator=None`` и ``audit_sink=None`` поведение прежнее,
    байт-в-байт (локальный профиль, петлевой адрес).
    """
    # Единый идентификатор вызова: конверт и аудит обязаны совпадать (07 §8.2/§8.4).
    rid = str(request_id) if request_id else gateway._new_id()
    method = (method or "").upper()
    op = _parse_op(path)
    audit_op = op or path or "unknown"
    principal: Principal | None = None

    def deny(
        code: int, error: str, status: str, *, payload: Mapping[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        """Отказ/ошибка до диспетчеризации: аудит → конверт с тем же ``rid``."""
        _audit_event(
            audit_sink,
            request_id=rid,
            principal=principal,
            op=audit_op,
            status=status,
            payload=payload,
            remote=remote,
            classification=classification,
        )
        return code, gateway.error_envelope(error, request_id=rid)

    # 1. Маршрут и метод (404/405 — как раньше).
    if op is None or op not in _KNOWN_OPS:
        return deny(404, f"маршрут не найден: {path!r}", AUDIT_STATUS_ERROR)

    if method == "POST":
        if op not in _POST_OPS:
            return deny(405, f"POST не поддерживается для {op!r}", AUDIT_STATUS_ERROR)
    elif method == "GET":
        if op not in _GET_OPS:
            return deny(405, f"GET не поддерживается для {op!r}", AUDIT_STATUS_ERROR)
    else:
        return deny(
            405, f"метод {method!r} не поддерживается", AUDIT_STATUS_ERROR
        )

    # 2–3. Аутентификация и авторизация. Публичные операции (``health``) —
    # диагностика достижимости: доступны без идентичности (07 §3), авторизация
    # для них заведомо проходит (``auth.PUBLIC_OPS``).
    if authenticator is not None and op not in PUBLIC_OPS:
        decision = authenticator.authenticate(auth_request or {})
        if not decision.allowed or decision.principal is None:
            return deny(
                401,
                f"не аутентифицирован: {decision.reason}",
                AUDIT_STATUS_DENY,
            )
        principal = decision.principal
        if not authorize(principal, op):
            return deny(
                403,
                f"доступ к операции {op!r} запрещён для субъекта "
                f"{principal.subject!r}",
                AUDIT_STATUS_DENY,
            )

    # 4. Лимит тела — до разбора JSON и до аллокаций по декларации клиента.
    raw_body = body.encode("utf-8") if isinstance(body, str) else bytes(body or b"")
    if len(raw_body) > max_body_bytes:
        return deny(
            413,
            f"тело запроса превышает лимит {max_body_bytes} байт",
            AUDIT_STATUS_ERROR,
        )

    # 5. Тело → payload.
    if method == "POST":
        payload, error = _decode_json_body(raw_body)
        if error is not None:
            return deny(400, error, AUDIT_STATUS_ERROR)
        assert payload is not None  # гарантировано ветвлением выше
    else:
        payload = {}

    # 6. Диспетчеризация.
    envelope = gateway.envelope(op, payload, request_id=rid)

    # 7. Аудит финального статуса (прикладной сбой адаптера — ``ERROR``).
    _audit_event(
        audit_sink,
        request_id=rid,
        principal=principal,
        op=op,
        status=(
            AUDIT_STATUS_ALLOW if envelope.get("status") == "ok" else AUDIT_STATUS_ERROR
        ),
        payload=payload,
        remote=remote,
        classification=classification,
    )
    return 200, envelope


def _declared_content_length(headers: Any) -> int | None:
    """``Content-Length`` запроса как ``int`` либо ``None`` (нет/битое/отрицательное).

    Отсутствие отличается от нуля: ``None`` означает «клиент не объявил длину»
    (нужен ограниченный дочитывающий путь), ``0`` — явно пустое тело.
    """
    raw = headers.get("Content-Length")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


def _content_length(headers: Any) -> int:
    """Content-Length запроса как неотрицательное ``int`` (0 на битое/отсутствие)."""
    declared = _declared_content_length(headers)
    return 0 if declared is None else declared


def make_handler(
    gateway: EdgeGateway,
    *,
    request_id_header: str = REQUEST_ID_HEADER,
    authenticator: Authenticator | None = None,
    audit_sink: AuditSink | None = None,
    classification: Classification | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> type[BaseHTTPRequestHandler]:
    """Фабрика ``BaseHTTPRequestHandler`` поверх ``route_request`` (тестируемо без сокета).

    POST ``/v1/{fetch,search,registry}`` и GET ``/v1/{health,capabilities}``;
    ``X-Request-Id`` запроса эхо-возвращается в конверте (07 §8.2). Middleware
    (аутентификация/авторизация/аудит) прокидывается в :func:`route_request`
    параметрами; ``None`` — прежнее поведение.

    Тело читается **только** в пределах ``max_body_bytes``: объявленный
    ``Content-Length`` сверх лимита отвергается (413) до чтения, а чтение
    ограничено ``max_body_bytes + 1`` — ``read()`` без границы не вызывается
    никогда. На 413 соединение закрывается (``Connection: close``): при
    ``HTTP/1.1`` сокет переиспользуется, и непрочитанное тело было бы разобрано
    как следующий запрос — это рассинхронизация, а не просто мусор в ответе.
    """

    class _EdgeRequestHandler(BaseHTTPRequestHandler):
        server_version = "coHound-edge/1"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        def _send_envelope(
            self, code: int, envelope: Mapping[str, Any], *, close: bool = False
        ) -> None:
            """Пишет JSON-конверт; ``close=True`` — ``Connection: close`` + закрытие."""
            data = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            if close:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            self.wfile.write(data)

        def _audit_transport(
            self, request_id: str | None, op: str, status: str
        ) -> None:
            """Аудит транспортного отказа, не дошедшего до ``route_request``."""
            _audit_event(
                audit_sink,
                request_id=request_id or gateway._new_id(),
                principal=None,
                op=op,
                status=status,
                payload=None,
                remote=self._remote(),
                classification=classification,
            )

        def _remote(self) -> str | None:
            """Peer-адрес клиента для аудита (``None``, если недоступен)."""
            address = getattr(self, "client_address", None)
            if isinstance(address, (tuple, list)) and address:
                return str(address[0])
            return None

        def _dispatch(self, method: str) -> None:
            # Один request_id на вызов: заголовок клиента либо сгенерированный
            # здесь — конверт и аудит обязаны нести одно и то же значение.
            rid = self.headers.get(request_id_header) or gateway._new_id()
            op = _parse_op(self.path) or self.path or "unknown"
            declared = _declared_content_length(self.headers)

            # Транспортный guard (ресурсная защита, раньше прикладного конвейера):
            # объявленная длина сверх лимита отвергается без чтения тела.
            if declared is not None and declared > max_body_bytes:
                self._audit_transport(rid, op, AUDIT_STATUS_ERROR)
                self._send_envelope(
                    413,
                    gateway.error_envelope(
                        f"тело запроса превышает лимит {max_body_bytes} байт",
                        request_id=rid,
                    ),
                    close=True,
                )
                return

            body = b""
            if method == "POST":
                transfer_encoding = self.headers.get("Transfer-Encoding")
                if declared is None and transfer_encoding:
                    # Framing chunked шов не поддерживает (контракт — L7 JSON с
                    # Content-Length). Дочитываем не более лимита+1, чтобы не
                    # аллоцировать по декларации, и закрываем соединение: остаток
                    # chunked-потока иначе был бы разобран как следующий запрос.
                    body = self.rfile.read(max_body_bytes + 1)
                    if len(body) > max_body_bytes:
                        self._audit_transport(rid, op, AUDIT_STATUS_ERROR)
                        self._send_envelope(
                            413,
                            gateway.error_envelope(
                                f"тело запроса превышает лимит {max_body_bytes} байт",
                                request_id=rid,
                            ),
                            close=True,
                        )
                        return
                    self._audit_transport(rid, op, AUDIT_STATUS_ERROR)
                    self._send_envelope(
                        400,
                        gateway.error_envelope(
                            "Transfer-Encoding: chunked не поддерживается швом; "
                            "используйте Content-Length",
                            request_id=rid,
                        ),
                        close=True,
                    )
                    return
                if declared is not None and declared > 0:
                    # Верхняя граница чтения — лимит+1 (защита от лживой/битой
                    # декларации); объявленный сверх лимита уже отвергнут выше.
                    body = self.rfile.read(min(declared, max_body_bytes + 1))
                # Content-Length нет и chunked не объявлен → по HTTP/1.1 тела нет.
                # «Дочитать на всякий случай» здесь нельзя: read(cap+1) на
                # keep-alive соединении заблокировался бы до EOF.

            code, envelope = route_request(
                gateway,
                method,
                self.path,
                body,
                request_id=rid,
                authenticator=authenticator,
                auth_request=dict(self.headers),
                audit_sink=audit_sink,
                classification=classification,
                remote=self._remote(),
                max_body_bytes=max_body_bytes,
            )
            # 413: тело могло остаться непрочитанным — закрываем, чтобы не
            # разобрать его как следующий запрос на переиспользуемом сокете.
            self._send_envelope(code, envelope, close=code == 413)

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
    authenticator: Authenticator | None = None,
    audit_sink: AuditSink | None = None,
    classification: Classification | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> Any:
    """Создаёт HTTP-сервер (без запуска). ``server_factory`` инъецируется для тестов.

    **Fail-fast (07 §8.2):** если ``authenticator is None``, а ``host`` — не
    петлевой (:func:`is_loopback_host`), запуск отвергается
    :class:`EdgeSecurityError` до создания сокета. Петля без аутентификации —
    штатный локальный профиль; маршрутизируемый интерфейс без неё — нет.
    """
    ensure_safe_binding(host, authenticator)
    handler_cls = handler_factory(
        gateway,
        authenticator=authenticator,
        audit_sink=audit_sink,
        classification=classification,
        max_body_bytes=max_body_bytes,
    )
    return server_factory((host, port), handler_cls)


def serve(
    host: str,
    port: int,
    gateway: EdgeGateway,
    *,
    server_factory: Callable[..., Any] = ThreadingHTTPServer,
    handler_factory: Callable[..., type[BaseHTTPRequestHandler]] = make_handler,
    scheduler: Any = None,
    on_start: Callable[[], None] | None = None,
    authenticator: Authenticator | None = None,
    audit_sink: AuditSink | None = None,
    classification: Classification | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> None:
    """Запускает блокирующий HTTP-сервер ``/v1/*`` (stdlib).

    Опциональный режим развёртывания ``split`` (07 §6). Сокет поднимает только
    ``serve``; ``EdgeGateway`` и ``route_request`` тестируются без сети.
    ``authenticator``/``audit_sink``/``max_body_bytes`` — middleware шва
    (07 §8.2/§8.4); при ``None``/дефолте поведение прежнее. Fail-fast по
    привязке наследуется от :func:`build_server`.

    **Хук планировщика (Phase 3.0).** ``scheduler`` — необязательный фоновый компонент
    (напр. пул воркеров/очередь, Wave 3.2) с методами ``start()``/``stop()``. При
    ``scheduler is not None``: ``on_start()`` (если задан) → ``scheduler.start()`` →
    ``serve_forever()``; ``scheduler.stop()`` гарантированно вызывается в ``finally``
    вокруг ``server_close()`` (даже при сбое ``serve_forever``/``server_close``).
    При ``scheduler is None`` и ``on_start is None`` поведение прежнее, байт-в-байт.
    """
    server = build_server(
        host,
        port,
        gateway,
        server_factory=server_factory,
        handler_factory=handler_factory,
        authenticator=authenticator,
        audit_sink=audit_sink,
        classification=classification,
        max_body_bytes=max_body_bytes,
    )
    if on_start is not None:
        on_start()
    if scheduler is not None:
        scheduler.start()
    try:
        server.serve_forever()
    finally:
        try:
            server.server_close()
        finally:
            if scheduler is not None:
                scheduler.stop()
