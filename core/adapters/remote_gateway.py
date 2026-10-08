"""HTTP-транспорт к DMZ-сервису — ``HttpContentGateway`` (W13/T13.3; 07 §3–§5).

Реализация порта ``core.ports.gateway.ContentGateway``: единственный канал
ядро→edge по протоколу ``POST/GET /v1/{op}`` с JSON-конвертом
``{schema_version, request_id, status, payload, error}`` (07 §3). Ядро
**инициирует**, edge **отвечает**; обратного канала нет (07 §3/§5).

Гарантии:
- **I19**: gateway возвращает контент/кандидатов, а **не evidence**; ядро
  повторно хеширует/валидирует входящее (07 §5). Хэшу edge ядро не доверяет.
- **Клиентская валидация**: схема (``schema_version`` — версионная граница §13),
  ``status == "ok"``, наличие ``payload``, лимит размера ответа.
- **Без пиннинга сертификатов** (07 §8.1): доверие к корпоративному CA
  (в т.ч. TLS break-and-inspect) настраивается путём ``ca`` через ``verify``;
  никакой фиксации конкретного сертификата/отпечатка нет.

``httpx`` импортируется лениво (необязательная зависимость extra ``crawl``).
Для тестов/оффлайн-сценариев внедряется ``transport`` — callable
``(op, envelope) -> envelope``; при ``transport is None`` выполняется реальный
HTTP POST/GET. Нейтрален (§1.3).
"""

from __future__ import annotations

import base64
import binascii
import json
import time
import uuid
from collections.abc import Callable, Mapping
from typing import Any

from core.ports.types import ExtractedDoc, RawPage, RenderMode
from core.util.hashing import content_hash

__all__ = [
    "GatewayError",
    "GatewayTransportError",
    "GatewaySchemaError",
    "GatewayStatusError",
    "GatewayPayloadError",
    "GatewaySizeError",
    "RemoteProxyError",
    "RemoteContentError",
    "HttpContentGateway",
    "TransportFn",
    "raw_page_from_response",
    "extracted_doc_from_response",
    "verify_content_hash",
    "as_items",
]

#: Версия конверта по умолчанию (07 §3/§7 — фиксируется в манифесте R7).
DEFAULT_SCHEMA_VERSION = "1"
#: Лимит ответа по умолчанию: 50 MiB (07 §7 ``max_response_bytes``).
DEFAULT_MAX_RESPONSE_BYTES = 52_428_800

#: Операции, которые в протоколе edge — GET (07 §3).
_GET_OPS = frozenset({"capabilities", "health"})

#: Callable-шов транспорта: ``(op, envelope) -> envelope`` (в тестах — без сети).
TransportFn = Callable[[str, Mapping[str, Any]], Mapping[str, Any]]


# ---------------------------------------------------------------------------
# Ошибки
# ---------------------------------------------------------------------------


class GatewayError(RuntimeError):
    """Базовый сбой шва core→edge (все ошибки gateway — подклассы)."""


class GatewayTransportError(GatewayError):
    """Сетевой/транспортный сбой: HTTP-ошибка, недоступность edge, отсутствие httpx."""


class GatewaySchemaError(GatewayError):
    """Несовпадение ``schema_version`` — версионная граница (§13; 07 §8.3)."""


class GatewayStatusError(GatewayError):
    """Edge вернул ``status != "ok"`` (несёт ``error`` в тексте)."""


class GatewayPayloadError(GatewayError):
    """Конверт без ``payload``, неверного типа или несериализуемого содержимого."""


class GatewaySizeError(GatewayError):
    """Ответ edge превысил ``max_response_bytes`` (защита ядра, 07 §7)."""


class RemoteProxyError(RuntimeError):
    """Базовый сбой remote-прокси (адаптеры ``remote_*``)."""


class RemoteContentError(RemoteProxyError):
    """Содержимое ответа edge не прошло клиентскую валидацию (в т.ч. re-hash I19)."""


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------


def _httpx_available() -> bool:
    """Ленивая проверка наличия ``httpx`` (без импорта на уровне модуля)."""
    try:
        import httpx  # type: ignore[import-not-found]  # noqa: F401 - нужен только факт импорта
    except ImportError:  # pragma: no cover - зависит от окружения
        return False
    return True


class HttpContentGateway:
    """``ContentGateway`` поверх HTTP/JSON (httpx) или внедрённого ``transport``.

    :param base_url: базовый URL edge (``https://edge.internal:8443``); при
        внедрённом ``transport`` сеть не используется.
    :param schema_version: ожидаемая версия конверта; несовпадение — ошибка.
    :param transport: шов для тестов ``(op, envelope) -> envelope``; ``None`` →
        реальный HTTP-запрос.
    :param timeout: таймаут запроса, секунды.
    :param max_retries: число **повторов** транспортного сбоя (0..2); всего
        попыток ``max_retries + 1``. Валидационные ошибки не повторяются.
    :param max_response_bytes: лимит размера ответа (защита ядра).
    :param ca: путь к корпоративному CA (``verify`` для httpx). Пиннинга нет:
        доверие к CA, включая TLS-инспекцию (07 §8.1).
    :param client_cert: клиентский сертификат workload-identity (mTLS).
    :param client_key: приватный ключ клиентского сертификата.
    :param extra_headers: дополнительные заголовки (напр. OIDC bearer).
    :param retry_backoff_s: пауза между повторами, секунды (0 — без паузы).
    """

    def __init__(
        self,
        base_url: str = "",
        *,
        schema_version: str = DEFAULT_SCHEMA_VERSION,
        transport: TransportFn | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        ca: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        retry_backoff_s: float = 0.0,
    ) -> None:
        self.schema_version = schema_version
        self._base_url = base_url
        self._transport = transport
        self._timeout = timeout
        self._max_retries = max(0, int(max_retries))
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes должен быть положительным")
        self._max_response_bytes = int(max_response_bytes)
        self._ca = ca
        self._client_cert = client_cert
        self._client_key = client_key
        self._extra_headers = dict(extra_headers or {})
        self._retry_backoff_s = max(0.0, float(retry_backoff_s))
        self._last_request_id: str | None = None

    # -- свойства --------------------------------------------------------------

    @property
    def available(self) -> bool:
        """Доступен ли транспорт: внедрён ``transport`` или установлен ``httpx``."""
        if self._transport is not None:
            return True
        return _httpx_available()

    @property
    def last_request_id(self) -> str | None:
        """``request_id`` последнего вызова (корреляция/SIEM, 07 §8; не для идемпотентности)."""
        return self._last_request_id

    # -- контракт ContentGateway ----------------------------------------------

    def call(self, op: str, payload: Mapping[str, Any]) -> Any:
        """Один вызов edge-операции; возвращает **проверенный ``payload``** ответа.

        Конверт запроса генерируется здесь (``request_id`` — uuid4 hex, одна
        корреляция на вызов, сохраняется между повторами). Конверт ответа
        валидируется: ``schema_version``, ``status == "ok"``, наличие ``payload``,
        лимит размера. При успехе возвращается вложенный ``payload`` (может быть
        и списком — так, ``search``/``registry`` возвращают список кандидатов).
        """
        request_id = uuid.uuid4().hex
        self._last_request_id = request_id
        envelope: dict[str, Any] = {
            "schema_version": self.schema_version,
            "request_id": request_id,
            "status": "request",
            "payload": dict(payload),
            "error": None,
        }
        response = self._send_with_retry(op, envelope)
        return self._validate_response(response)

    def capabilities(self) -> Mapping[str, Any]:
        """Версии edge-компонентов для манифеста (``/v1/capabilities``)."""
        payload = self.call("capabilities", {})
        if not isinstance(payload, Mapping):
            raise GatewayPayloadError("capabilities: payload не является отображением")
        return dict(payload)

    # -- транспорт -------------------------------------------------------------

    def _send_with_retry(self, op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        """Повторяет **только** транспортный сбой; валидация — вне цикла."""
        attempts = self._max_retries + 1
        last_exc: GatewayTransportError | None = None
        for attempt in range(attempts):
            try:
                return self._send(op, envelope)
            except GatewayTransportError as exc:
                last_exc = exc
                if attempt + 1 >= attempts:
                    raise
                if self._retry_backoff_s:
                    time.sleep(self._retry_backoff_s)
        assert last_exc is not None  # pragma: no cover - недостижимо
        raise last_exc

    def _send(self, op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        """Один запрос: внедрённый ``transport`` или реальный HTTP."""
        if self._transport is not None:
            try:
                return self._transport(op, envelope)
            except GatewayError:
                raise
            except Exception as exc:  # noqa: BLE001 - унификация транспортных сбоев
                raise GatewayTransportError(f"transport {op!r}: {exc}") from exc
        return self._http(op, envelope)

    def _http(self, op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        """Реальный HTTP-запрос к ``{base_url}/v1/{op}`` (ленивый импорт httpx)."""
        if not _httpx_available():  # pragma: no cover - зависит от окружения
            raise GatewayTransportError(
                "httpx не установлен: установите extra 'crawl' или внедрите transport"
            )
        import httpx  # type: ignore[import-not-found]

        url = f"{self._base_url.rstrip('/')}/v1/{op}"
        headers = {
            "Accept": "application/json",
            "X-Request-Id": str(envelope["request_id"]),
            "X-Schema-Version": self.schema_version,
            **self._extra_headers,
        }
        # Без пиннинга: при заданном CA доверяем ему (в т.ч. TLS-инспекции, 07 §8.1).
        verify: bool | str = self._ca if self._ca else True
        client_kwargs: dict[str, Any] = {
            "timeout": self._timeout,
            "verify": verify,
            "follow_redirects": False,
        }
        if self._client_cert:
            client_kwargs["cert"] = (
                (self._client_cert, self._client_key)
                if self._client_key
                else self._client_cert
            )
        try:
            with httpx.Client(**client_kwargs) as client:
                if op in _GET_OPS:
                    response = client.get(url, headers=headers)
                else:
                    response = client.post(url, headers=headers, json=dict(envelope))
                response.raise_for_status()
                content = response.content
        except httpx.HTTPError as exc:
            raise GatewayTransportError(f"HTTP {op!r}: {exc}") from exc

        if len(content) > self._max_response_bytes:
            raise GatewaySizeError(
                f"ответ edge {len(content)} байт > лимита {self._max_response_bytes}"
            )
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            raise GatewayPayloadError(f"ответ edge не является JSON: {exc}") from exc
        return data

    # -- валидация ответа ------------------------------------------------------

    def _validate_response(self, response: Any) -> Any:
        """Проверяет конверт ответа и возвращает вложенный ``payload``."""
        if not isinstance(response, Mapping):
            raise GatewayPayloadError(
                f"конверт edge должен быть отображением, получено {type(response).__name__}"
            )
        if response.get("schema_version") != self.schema_version:
            raise GatewaySchemaError(
                f"несовпадение schema_version: ожидалось {self.schema_version!r}, "
                f"получено {response.get('schema_version')!r} (версионная граница §13)"
            )
        if response.get("status") != "ok":
            error = response.get("error")
            raise GatewayStatusError(
                f"edge вернул status={response.get('status')!r}: {error}"
            )
        if "payload" not in response:
            raise GatewayPayloadError("конверт edge без поля payload")
        payload = response["payload"]
        if payload is None:
            raise GatewayPayloadError("payload=edge пуст при status=ok")
        self._enforce_size(response)
        return payload

    def _enforce_size(self, response: Mapping[str, Any]) -> None:
        """Лимит размера ответа (для внедрённого ``transport`` — по сериализации)."""
        try:
            size = len(
                json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            )
        except (TypeError, ValueError) as exc:
            raise GatewayPayloadError(f"ответ edge несериализуем: {exc}") from exc
        if size > self._max_response_bytes:
            raise GatewaySizeError(
                f"ответ edge {size} байт > лимита {self._max_response_bytes}"
            )


# ---------------------------------------------------------------------------
# Общие хелперы remote-прокси (декодирование и повторный хеш, I19)
# ---------------------------------------------------------------------------


def as_items(payload: Any, *, op: str) -> list[Any]:
    """Принимает голый список (форма 07 §3) или ``{"results": [...]}``.

    Используется ops ``search``/``registry``, чей payload — список записей.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, Mapping) and isinstance(payload.get("results"), list):
        return list(payload["results"])
    raise RemoteContentError(f"{op}: ожидался список элементов в payload")


def verify_content_hash(content: bytes, claimed: str | None) -> str:
    """Пересчитывает ``content_hash`` и сверяет с заявленным edge (I19, 07 §5/§8.5).

    Ядро **не доверяет** хэшу edge: несовпадение — признак мутации/порчи
    (в т.ч. DLP/IDS-инспектором) и отвергается. Возвращает пересчитанный хэш.
    """
    computed = content_hash(content)
    if claimed is not None and claimed != computed:
        raise RemoteContentError(
            "I19: content_hash, заявленный edge, не совпал с пересчитанным в ядре"
        )
    return computed


def _decode_raw_bytes(raw: Mapping[str, Any]) -> bytes | None:
    """Декодирует явное поле сырых байтов (07 §8): ``bytes_b64`` → ``bytes``.

    ``None``, если inline-байтов нет (есть только ``ref`` или поля нет).
    Битые base64 — ошибка содержимого (не тихий фолбэк).
    """
    encoded = raw.get("bytes_b64")
    if encoded is not None:
        try:
            return base64.b64decode(str(encoded), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise RemoteContentError(f"raw.bytes_b64 не декодируется: {exc}") from exc
    content = raw.get("content")
    if content is not None:
        return content if isinstance(content, bytes) else str(content).encode("utf-8")
    return None


def raw_page_from_response(
    response: Mapping[str, Any],
    *,
    url: str,
    render: RenderMode = RenderMode.HTTP,
) -> RawPage:
    """Собирает ``RawPage`` из payload ответа ``/v1/fetch`` (07 §3).

    Порядок источников байтов: ``raw.bytes_b64`` → ``raw.content`` (явные поля
    07 §8); при их отсутствии — деградация до ``text.content`` (очищенный
    markdown, если edge вернул только его). Если ``raw.hash`` задан, ядро
    **пересчитывает** хэш и сверяет (I19).
    """
    raw = response.get("raw")
    claimed_hash: str | None = None
    content: bytes | None = None
    if isinstance(raw, Mapping):
        raw_hash = raw.get("hash")
        claimed_hash = None if raw_hash is None else str(raw_hash)
        content = _decode_raw_bytes(raw)
    if content is None:
        text = response.get("text")
        if isinstance(text, Mapping) and text.get("content") is not None:
            content = str(text["content"]).encode("utf-8")
    if content is None:
        error = response.get("error")
        raise RemoteContentError(
            f"ответ edge не содержит сырых байтов (raw.bytes_b64/raw.content/text.content)"
            + (f"; error={error}" if error else "")
        )
    if claimed_hash is not None:
        verify_content_hash(content, claimed_hash)

    final_url = response.get("final_url") or url
    status_code = response.get("status_code")
    content_type = response.get("content_type")
    fetched_at = response.get("fetched_at") or ""
    lang = response.get("lang")
    return RawPage(
        url=str(final_url),
        content=content,
        fetched_at=str(fetched_at),
        render_mode=render,
        lang=str(lang) if lang else None,
        status_code=int(status_code) if status_code is not None else None,
        content_type=str(content_type) if content_type else None,
    )


def extracted_doc_from_response(
    response: Mapping[str, Any], *, version: str
) -> ExtractedDoc:
    """Собирает ``ExtractedDoc`` из payload ответа ``/v1/fetch`` с ``extract=True``.

    ``text.content`` — очищенный markdown; ``text.hash`` пересчитывается ядром
    (I19). ``text.extractor_version`` фиксирует версию экстрактора edge.
    Грациозная деградация: при отсутствии ``text`` и наличии ``error``
    возвращается ``ExtractedDoc`` с пустым ``text`` и заполненным ``error``.
    """
    text = response.get("text")
    if not isinstance(text, Mapping):
        error = response.get("error")
        if error:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(""),
                extractor_version=version,
                error=str(error),
            )
        raise RemoteContentError("ответ edge без поля text (extract=True)")

    content = text.get("content")
    content_str = "" if content is None else str(content)
    claimed_hash = text.get("hash")
    if claimed_hash is not None:
        verify_content_hash(content_str.encode("utf-8"), str(claimed_hash))
    extractor_version = text.get("extractor_version") or version
    page_count = text.get("page_count")
    return ExtractedDoc(
        text=content_str,
        content_hash=content_hash(content_str),
        extractor_version=str(extractor_version),
        page_count=int(page_count) if page_count is not None else None,
        error=None,
    )
