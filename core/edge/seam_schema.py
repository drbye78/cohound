"""Машиночитаемые контракты шва core↔edge — загрузка и валидация (T13.1/T13.7; 07 §3/§8).

Публикуемые контракты лежат в каталоге ``contracts/seam/`` репозитория:

* ``envelope.schema.json`` — общий конверт ``{schema_version, request_id, status,
  payload, error}`` (ответ + форма запроса в ``$defs.request``);
* ``fetch.schema.json`` / ``search.schema.json`` / ``registry.schema.json`` —
  запрос+ответ операций ``POST /v1/*``;
* ``capabilities.schema.json`` / ``health.schema.json`` — ответы ``GET /v1/*``;
* ``openapi.json`` — OpenAPI 3.1 по всем эндпоинтам (для DLP/IDS/IDP-политик, 07 §8).

Модуль **нейтрален** (§1.3) и опирается только на stdlib: ``jsonschema`` — вердикт
**b** в реестре зависимостей (``docs/architecture/05-dependency-stack.md`` §2.9),
поэтому обязательной зависимостью не является. :func:`validate_envelope` и
:func:`validate_payload` реализуют рекурсивную проверку required/типов/enum-ов
вручную; при желании внешний ``jsonschema`` может валидировать те же файлы
напрямую (публикуемая схема — источник истины).

``schema_version`` — часть контракта: несовместимое изменение → bump версии
(версионная граница §13; 07 §3/§8.3). Текущее значение обязано совпадать с
``DEFAULT_SCHEMA_VERSION`` клиента (``core/adapters/remote_gateway.py``).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from core.ports.types import CrawlStrategy, ExtractionRoute, FetchMode

__all__ = [
    "SEAM_SCHEMA_VERSION",
    "SCHEMA_DIR",
    "ENDPOINT_OPS",
    "CRAWL_OPS",
    "FETCH_VALUES",
    "EXTRACT_VALUES",
    "STRATEGY_VALUES",
    "load_schema",
    "validate_envelope",
    "validate_payload",
    "validate_fetch_request",
    "validate_crawl_request",
    "contract_digest",
]

#: Версия контракта шва. Обязана совпадать с клиентской/серверной
#: ``DEFAULT_SCHEMA_VERSION`` (``core/adapters/remote_gateway.py``): несовпадение —
#: версионная граница §13 (07 §3/§8.3).
SEAM_SCHEMA_VERSION = "1"

#: Каталог опубликованных контрактов (``<repo>/contracts/seam``).
SCHEMA_DIR: Path = Path(__file__).resolve().parents[2] / "contracts" / "seam"

#: Операции протокола ``/v1/*`` (07 §3). Crawl-операции сохраняют слэш (``crawl/submit``).
ENDPOINT_OPS: frozenset[str] = frozenset(
    {
        "fetch",
        "search",
        "registry",
        "capabilities",
        "health",
        "crawl/submit",
        "crawl/status",
        "crawl/cancel",
        "crawl/collect",
    }
)

#: Допустимые значения оси ``fetch`` запроса ``/v1/fetch`` (D2).
FETCH_VALUES: frozenset[str] = frozenset(mode.value for mode in FetchMode)
#: Допустимые значения оси ``extract`` запроса ``/v1/fetch`` (D2).
EXTRACT_VALUES: frozenset[str] = frozenset(route.value for route in ExtractionRoute)
#: Допустимые значения оси ``strategy`` запроса ``/v1/fetch`` (ось стратегии краула).
STRATEGY_VALUES: frozenset[str] = frozenset(s.value for s in CrawlStrategy)

#: Допустимые статусы **ответного** конверта (07 §3). Запросный конверт несёт
#: ``status="request"`` (внутренняя форма клиента) и здесь не валидируется.
_RESPONSE_STATUSES: frozenset[str] = frozenset({"ok", "error"})

#: Имена ``*_schema.json``, за которые отвечает :func:`load_schema`.
_SCHEMA_NAMES: frozenset[str] = frozenset(
    {"envelope", "fetch", "search", "registry", "capabilities", "health", "crawl"}
)

#: Crawl-операции очереди краула (Phase 3.1): POST /v1/crawl/{submit,status,cancel,collect}.
CRAWL_OPS: frozenset[str] = frozenset(
    {"crawl/submit", "crawl/status", "crawl/cancel", "crawl/collect"}
)


# ---------------------------------------------------------------------------
# Загрузка контрактов
# ---------------------------------------------------------------------------


def _schema_path(name: str) -> Path:
    """Разрешает имя контракта в путь внутри :data:`SCHEMA_DIR`.

    Принимает ``"envelope"`` или ``"envelope.schema.json"`` (``openapi`` →
    ``openapi.json``). Защита от выхода за каталог: имя не должно содержать
    компонентов пути.
    """
    raw = str(name).strip()
    if not raw:
        raise ValueError("имя схемы не может быть пустым")
    if raw.endswith(".json"):
        filename = raw
    elif raw == "openapi":
        filename = "openapi.json"
    else:
        filename = f"{raw}.schema.json"
    if Path(filename).name != filename:
        raise ValueError(f"недопустимое имя схемы: {name!r}")
    path = SCHEMA_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"контракт не найден: {path}")
    return path


def load_schema(name: str) -> dict[str, Any]:
    """Загружает опубликованный контракт из ``contracts/seam/`` как словарь.

    :param name: ``"envelope"``, ``"fetch"``, …, ``"openapi"`` — либо полное имя
        файла (``"envelope.schema.json"``).
    :raises FileNotFoundError: контракт отсутствует в репозитории.
    """
    data = json.loads(_schema_path(name).read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"контракт {name!r} должен быть JSON-объектом")
    return data


# ---------------------------------------------------------------------------
# Вспомогательные проверки типов
# ---------------------------------------------------------------------------


def _is_int(value: Any) -> bool:
    """``int``, но не ``bool`` (``bool`` — подкласс ``int`` в Python)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _check_opt_str(violations: list[str], where: str, field: str, value: Any) -> None:
    """Поле, если присутствует/не ``None``, должно быть строкой."""
    if value is not None and not isinstance(value, str):
        violations.append(f"{where}: поле {field} должно быть строкой или null")


def _check_opt_int(violations: list[str], where: str, field: str, value: Any) -> None:
    """Поле, если присутствует/не ``None``, должно быть целым."""
    if value is not None and not _is_int(value):
        violations.append(f"{where}: поле {field} должно быть целым числом или null")


def _check_required_str(
    violations: list[str], where: str, field: str, value: Any, *, nonempty: bool = True
) -> None:
    """Поле обязано присутствовать и быть строкой (по умолчанию — непустой)."""
    if not isinstance(value, str) or (nonempty and not value):
        violations.append(f"{where}: обязательное поле {field} — непустая строка")


def _check_opt_bool(violations: list[str], where: str, field: str, value: Any) -> None:
    """Поле, если присутствует/не ``None``, должно быть булевым."""
    if value is not None and not isinstance(value, bool):
        violations.append(f"{where}: поле {field} должно быть булевым или null")


def _check_opt_str_list(
    violations: list[str], where: str, field: str, value: Any
) -> None:
    """Опциональное поле-список строк: ``None``/список допустимы."""
    if value is None:
        return
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        violations.append(f"{where}: поле {field} должно быть списком строк или null")


# ---------------------------------------------------------------------------
# Конверт
# ---------------------------------------------------------------------------


def validate_envelope(obj: Mapping[str, Any]) -> list[str]:
    """Проверяет **ответный** конверт шва (07 §3); ``[]`` — валиден.

    Контракт: ``{schema_version, request_id, status, payload, error}``.
    Проверяются:

    * наличие и совпадение ``schema_version`` (версионная граница §13);
    * ``status`` ∈ ``{ok, error}``;
    * ``request_id`` — непустая строка (корреляция/SIEM, 07 §8.2);
    * ``status=ok`` → присутствует непустой ``payload``, ``error`` = null;
    * ``status=error`` → присутствует непустая строка ``error``.

    Возвращает список человекочитаемых нарушений (пустой — конверт валиден).
    """
    violations: list[str] = []
    if not isinstance(obj, Mapping):
        return [f"конверт должен быть объектом, получено {type(obj).__name__}"]

    if "schema_version" not in obj:
        violations.append("отсутствует поле schema_version")
    elif obj.get("schema_version") != SEAM_SCHEMA_VERSION:
        violations.append(
            f"schema_version={obj.get('schema_version')!r} не совпадает с ожидаемым "
            f"{SEAM_SCHEMA_VERSION!r} (версионная граница §13)"
        )

    request_id = obj.get("request_id")
    if request_id is None:
        violations.append("отсутствует поле request_id")
    elif not isinstance(request_id, str) or not request_id:
        violations.append("request_id должен быть непустой строкой")

    status = obj.get("status")
    if status not in _RESPONSE_STATUSES:
        violations.append(f"status={status!r} недопустим (ожидается ok или error)")
        return violations

    error = obj.get("error")
    if status == "ok":
        if "payload" not in obj or obj.get("payload") is None:
            violations.append("при status=ok требуется непустой payload")
        if error is not None:
            violations.append("при status=ok поле error должно быть null")
    else:  # status == "error"
        if not isinstance(error, str) or not error:
            violations.append("при status=error требуется непустая строка error")
    return violations


# ---------------------------------------------------------------------------
# Payload эндпоинтов
# ---------------------------------------------------------------------------


def _items_of(payload: Any, op: str, violations: list[str]) -> list[Any]:
    """Списочный payload (07 §3) либо обёртка ``{"results": [...]}`` (клиент)."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, Mapping) and isinstance(payload.get("results"), list):
        return list(payload["results"])
    violations.append(
        f'{op}: payload должен быть списком (или {{"results": [...]}})'
    )
    return []


def _validate_fetch(payload: Any) -> list[str]:
    """Payload ``POST /v1/fetch`` (07 §3): сырые байты/очищенный markdown."""
    if not isinstance(payload, Mapping):
        return ["fetch: payload должен быть объектом"]
    violations: list[str] = []
    where = "fetch"
    for field in ("request_id", "final_url", "content_type", "fetched_at", "lang"):
        _check_opt_str(violations, where, field, payload.get(field))
    _check_opt_int(violations, where, "status_code", payload.get("status_code"))
    _check_opt_str(violations, where, "error", payload.get("error"))

    raw = payload.get("raw")
    if raw is not None:
        if not isinstance(raw, Mapping):
            violations.append("fetch: raw должен быть объектом или null")
        else:
            _check_opt_str(violations, "fetch.raw", "hash", raw.get("hash"))
            _check_opt_int(violations, "fetch.raw", "size", raw.get("size"))
            _check_opt_str(violations, "fetch.raw", "bytes_b64", raw.get("bytes_b64"))
            _check_opt_str(violations, "fetch.raw", "ref", raw.get("ref"))
            _check_opt_str(violations, "fetch.raw", "content", raw.get("content"))

    text = payload.get("text")
    if text is not None:
        if not isinstance(text, Mapping):
            violations.append("fetch: text должен быть объектом или null")
        else:
            _check_opt_str(violations, "fetch.text", "content", text.get("content"))
            _check_opt_str(violations, "fetch.text", "hash", text.get("hash"))
            _check_opt_str(
                violations, "fetch.text", "extractor_version", text.get("extractor_version")
            )
            _check_opt_int(violations, "fetch.text", "page_count", text.get("page_count"))

    # Клиент (``raw_page_from_response``) требует источник содержимого либо error.
    if raw is None and text is None and not payload.get("error"):
        violations.append(
            "fetch: требуется raw, text или непустой error (07 §3)"
        )
    return violations


def _validate_search(payload: Any) -> list[str]:
    """Payload ``POST /v1/search`` (07 §3): список кандидатов (I19, не evidence)."""
    violations: list[str] = []
    items = _items_of(payload, "search", violations)
    for index, item in enumerate(items):
        where = f"search[{index}]"
        if not isinstance(item, Mapping):
            violations.append(f"{where}: элемент должен быть объектом")
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url:
            violations.append(f"{where}: обязательное поле url — непустая строка")
        _check_opt_str(violations, where, "title", item.get("title"))
        _check_opt_str(violations, where, "snippet", item.get("snippet"))
        _check_opt_int(violations, where, "rank", item.get("rank"))
    return violations


def _validate_registry(payload: Any) -> list[str]:
    """Payload ``POST /v1/registry`` (07 §3): факты с обязательным ``source_url`` (R1)."""
    violations: list[str] = []
    items = _items_of(payload, "registry", violations)
    for index, item in enumerate(items):
        where = f"registry[{index}]"
        if not isinstance(item, Mapping):
            violations.append(f"{where}: элемент должен быть объектом")
            continue
        source_url = item.get("source_url")
        if not isinstance(source_url, str) or not source_url:
            violations.append(
                f"{where}: обязательное поле source_url — непустая строка (R1)"
            )
        fact_json = item.get("fact_json")
        if fact_json is not None and not isinstance(fact_json, (str, Mapping, list)):
            violations.append(
                f"{where}: fact_json должен быть строкой, объектом, списком или null"
            )
        _check_opt_str(violations, where, "fetched_at", item.get("fetched_at"))
    return violations


def _validate_capabilities(payload: Any) -> list[str]:
    """Payload ``GET /v1/capabilities`` (07 §3): версии edge-компонентов."""
    if not isinstance(payload, Mapping):
        return ["capabilities: payload должен быть объектом"]
    violations: list[str] = []
    for field in ("extractor_version", "crawler_version", "search_engine"):
        _check_opt_str(violations, "capabilities", field, payload.get(field))
    formats = payload.get("supported_formats")
    if formats is not None:
        if not isinstance(formats, list) or not all(
            isinstance(item, str) for item in formats
        ):
            violations.append(
                "capabilities: supported_formats должен быть списком строк"
            )
    return violations


def _validate_health(payload: Any) -> list[str]:
    """Payload ``GET /v1/health`` (07 §3): ``{status}``."""
    if not isinstance(payload, Mapping):
        return ["health: payload должен быть объектом"]
    status = payload.get("status")
    if not isinstance(status, str) or not status:
        return ["health: обязательное поле status — непустая строка"]
    return []


def _validate_crawl_submit_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/crawl/submit`` (Phase 3.1): ``{job_id, accepted, total, session_id}``."""
    if not isinstance(payload, Mapping):
        return ["crawl/submit: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/submit"
    _check_required_str(violations, where, "job_id", payload.get("job_id"))
    _check_opt_bool(violations, where, "accepted", payload.get("accepted"))
    _check_opt_int(violations, where, "total", payload.get("total"))
    _check_required_str(violations, where, "session_id", payload.get("session_id"))
    return violations


def _validate_crawl_status_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/crawl/status`` (Phase 3.1): счётчики очереди.

    Контракт: ``{job_id, status, total, pending, running, fetched, failed, cancelled,
    engines, kinds}``; ``engines``/``kinds`` — списки строк.
    """
    if not isinstance(payload, Mapping):
        return ["crawl/status: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/status"
    _check_required_str(violations, where, "job_id", payload.get("job_id"))
    _check_required_str(violations, where, "status", payload.get("status"))
    for field in ("total", "pending", "running", "fetched", "failed", "cancelled"):
        _check_opt_int(violations, where, field, payload.get(field))
    _check_opt_str_list(violations, where, "engines", payload.get("engines"))
    _check_opt_str_list(violations, where, "kinds", payload.get("kinds"))
    return violations


def _validate_crawl_cancel_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/crawl/cancel`` (Phase 3.1): ``{job_id, status, cancelled}``.

    ``status`` фиксирован строкой ``"cancelled"``.
    """
    if not isinstance(payload, Mapping):
        return ["crawl/cancel: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/cancel"
    _check_required_str(violations, where, "job_id", payload.get("job_id"))
    if payload.get("status") != "cancelled":
        violations.append(f"{where}: поле status должно быть 'cancelled'")
    _check_opt_int(violations, where, "cancelled", payload.get("cancelled"))
    return violations


def _validate_crawl_collect_item(item: Any, index: int) -> list[str]:
    """Элемент ``items`` ответа ``POST /v1/crawl/collect`` (Phase 3.1)."""
    where = f"crawl/collect.items[{index}]"
    if not isinstance(item, Mapping):
        return [f"{where}: элемент должен быть объектом"]
    violations: list[str] = []
    _check_required_str(violations, where, "product_id", item.get("product_id"))
    _check_required_str(violations, where, "url", item.get("url"))
    for field in ("status", "engine", "kind", "content_type", "fetched_at", "content_hash"):
        _check_opt_str(violations, where, field, item.get(field))
    _check_opt_int(violations, where, "http_status", item.get("http_status"))
    _check_opt_str(violations, where, "error", item.get("error"))
    raw = item.get("raw")
    if raw is not None:
        if not isinstance(raw, Mapping):
            violations.append(f"{where}.raw: должен быть объектом или null")
        else:
            _check_required_str(violations, f"{where}.raw", "hash", raw.get("hash"))
            _check_opt_int(violations, f"{where}.raw", "size", raw.get("size"))
            _check_opt_str(violations, f"{where}.raw", "bytes_b64", raw.get("bytes_b64"))
    return violations


def _validate_crawl_collect_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/crawl/collect`` (Phase 3.1): ``{items, next_cursor, done}``."""
    if not isinstance(payload, Mapping):
        return ["crawl/collect: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/collect"
    items = payload.get("items")
    if not isinstance(items, list):
        violations.append(f"{where}: поле items должно быть списком")
    else:
        for index, item in enumerate(items):
            violations.extend(_validate_crawl_collect_item(item, index))
    _check_opt_str(violations, where, "next_cursor", payload.get("next_cursor"))
    _check_opt_bool(violations, where, "done", payload.get("done"))
    return violations


_PAYLOAD_VALIDATORS = {
    "fetch": _validate_fetch,
    "search": _validate_search,
    "registry": _validate_registry,
    "capabilities": _validate_capabilities,
    "health": _validate_health,
    "crawl/submit": _validate_crawl_submit_response,
    "crawl/status": _validate_crawl_status_response,
    "crawl/cancel": _validate_crawl_cancel_response,
    "crawl/collect": _validate_crawl_collect_response,
}


def _opt_enum(
    violations: list[str], where: str, field: str, value: Any, allowed: frozenset[str]
) -> None:
    """Опциональное enum-поле: ``None`` допустим, иначе значение обязано быть в ``allowed``."""
    if value is None:
        return
    if not isinstance(value, str) or value not in allowed:
        violations.append(
            f"{where}: {field} должен быть одним из {sorted(allowed)} или null"
        )


def validate_fetch_request(payload: Any) -> list[str]:
    """Проверяет **запросный** payload ``POST /v1/fetch`` (D2, 07 §3); ``[]`` — валиден.

    Контракт запроса: ``{url, fetch, extract, strategy, wait_for, lang}``. ``url`` обязателен;
    ``fetch``/``extract``/``strategy`` — опциональные enum-оси (``http|browser``,
    ``auto|html|document``, ``auto|honest|hybrid``); ``wait_for``/``lang`` — строки или
    ``null``. ``strategy`` опционален: отсутствие = ``auto`` (обратная совместимость —
    поле additive). Неизвестное значение → нарушение (edge отвечает ``status=error``).
    """
    if not isinstance(payload, Mapping):
        return ["fetch: payload должен быть объектом"]
    violations: list[str] = []
    url = payload.get("url")
    if not isinstance(url, str) or not url.strip():
        violations.append("fetch: обязательное поле url — непустая строка")
    _opt_enum(violations, "fetch", "fetch", payload.get("fetch"), FETCH_VALUES)
    _opt_enum(violations, "fetch", "extract", payload.get("extract"), EXTRACT_VALUES)
    _opt_enum(violations, "fetch", "strategy", payload.get("strategy"), STRATEGY_VALUES)
    for field in ("wait_for", "lang"):
        value = payload.get(field)
        if value is not None and not isinstance(value, str):
            violations.append(f"fetch: {field} должен быть строкой или null")
    return violations


def validate_crawl_request(op: str, payload: Any) -> list[str]:
    """Проверяет **запросный** payload crawl-операции (Phase 3.1); ``[]`` — валиден.

    Контракты запросов (все — POST ``/v1/crawl/*``):

    * ``crawl/submit``: ``{run_id, session_id, idempotency_key, profile, items}``;
      ``items`` — непустой список объектов, каждый с ``product_id`` и ``url`` и
      опциональными осями ``engine``/``kind``/``fetch``/``extract``/``strategy``/
      ``wait_for``/``lang``;
    * ``crawl/status`` / ``crawl/cancel``: ``{job_id}``;
    * ``crawl/collect``: ``{job_id, cursor?, limit?}``.

    :param op: ``crawl/submit`` | ``crawl/status`` | ``crawl/cancel`` | ``crawl/collect``.
    """
    where = str(op).strip().lower()
    if where == "crawl/submit":
        return _validate_crawl_submit_request(payload)
    if where in ("crawl/status", "crawl/cancel"):
        return _validate_crawl_job_request(where, payload)
    if where == "crawl/collect":
        return _validate_crawl_collect_request(payload)
    return [f"неизвестная crawl-операция {op!r} (ожидается одна из: {sorted(CRAWL_OPS)})"]


def _validate_crawl_job_request(where: str, payload: Any) -> list[str]:
    """Запрос ``crawl/status``/``crawl/cancel``: обязательный непустой ``job_id``."""
    if not isinstance(payload, Mapping):
        return [f"{where}: payload должен быть объектом"]
    violations: list[str] = []
    _check_required_str(violations, where, "job_id", payload.get("job_id"))
    return violations


def _validate_crawl_collect_request(payload: Any) -> list[str]:
    """Запрос ``crawl/collect``: ``{job_id, cursor?, limit?}``."""
    if not isinstance(payload, Mapping):
        return ["crawl/collect: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/collect"
    _check_required_str(violations, where, "job_id", payload.get("job_id"))
    _check_opt_str(violations, where, "cursor", payload.get("cursor"))
    _check_opt_int(violations, where, "limit", payload.get("limit"))
    return violations


def _validate_crawl_submit_request(payload: Any) -> list[str]:
    """Запрос ``crawl/submit``: метаданные сессии + непустой список ``items``."""
    if not isinstance(payload, Mapping):
        return ["crawl/submit: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/submit"
    for field in ("run_id", "session_id", "idempotency_key", "profile"):
        _check_required_str(violations, where, field, payload.get(field))
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        violations.append(f"{where}: поле items — непустой список")
        return violations
    for index, item in enumerate(items):
        violations.extend(_validate_crawl_submit_item(item, index))
    return violations


def _validate_crawl_submit_item(item: Any, index: int) -> list[str]:
    """Элемент ``items`` запроса ``crawl/submit`` (оси фетча — как у ``/v1/fetch``)."""
    where = f"crawl/submit.items[{index}]"
    if not isinstance(item, Mapping):
        return [f"{where}: элемент должен быть объектом"]
    violations: list[str] = []
    for field in ("product_id", "url"):
        _check_required_str(violations, where, field, item.get(field))
    for field in ("engine", "kind"):
        _check_opt_str(violations, where, field, item.get(field))
    _opt_enum(violations, where, "fetch", item.get("fetch"), FETCH_VALUES)
    _opt_enum(violations, where, "extract", item.get("extract"), EXTRACT_VALUES)
    _opt_enum(violations, where, "strategy", item.get("strategy"), STRATEGY_VALUES)
    for field in ("wait_for", "lang"):
        _check_opt_str(violations, where, field, item.get(field))
    return violations


def validate_payload(op: str, payload: Any) -> list[str]:
    """Проверяет payload операции ``/v1/*``; ``[]`` — валиден.

    :param op: ``fetch`` | ``search`` | ``registry`` | ``capabilities`` | ``health`` |
        ``crawl/submit`` | ``crawl/status`` | ``crawl/cancel`` | ``crawl/collect``.
    :param payload: вложенный ``payload`` конверта ответа.
    """
    key = str(op).strip().lower()
    validator = _PAYLOAD_VALIDATORS.get(key)
    if validator is None:
        return [f"неизвестная операция {op!r} (ожидается одна из: {sorted(ENDPOINT_OPS)})"]
    return validator(payload)


# ---------------------------------------------------------------------------
# Digest контрактов (§13 version-pinning)
# ---------------------------------------------------------------------------


def contract_digest() -> str:
    """Детерминированный хэш (``sha256``) опубликованных JSON-контрактов.

    Хэшируются все ``contracts/seam/*.json`` (схемы + OpenAPI) в порядке имени:
    ``sha256(имя \\0 содержимое \\0 …)``. Значение пригодно для фиксации версии
    контракта в ``run_manifest.yaml`` (``edge_schema_version``, R7/§13).
    """
    digest = hashlib.sha256()
    for path in sorted(SCHEMA_DIR.glob("*.json"), key=lambda p: p.name):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"
