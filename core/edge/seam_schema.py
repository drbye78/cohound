"""Машиночитаемые контракты шва core↔edge — загрузка и валидация (T13.1/T13.7; 07 §3/§8).

Публикуемые контракты лежат в каталоге ``contracts/seam/`` репозитория:

* ``envelope.schema.json`` — общий конверт ``{schema_version, request_id, status,
  payload, error}`` (ответ + форма запроса в ``$defs.request``);
* ``fetch.schema.json`` / ``search.schema.json`` / ``registry.schema.json`` —
  запрос+ответ операций ``POST /v1/*``;
* ``crawl.schema.json`` — очередь краула ``POST /v1/crawl/*``;
* ``extract.schema.json`` — извлечение **по артефакту** ``POST /v1/extract/*``
  (B1/B2/B3; 10 §5.4): приём чанками, заказ, забор, уборка;
* ``capabilities.schema.json`` / ``health.schema.json`` — ответы ``GET /v1/*``;
* ``openapi.json`` — OpenAPI 3.1 по всем эндпоинтам (для DLP/IDS/IDP-политик, 07 §8).

Значения осей ``level``/``device`` операций извлечения берутся из **перечислений ядра**
(``DocumentLevel``/``PdfPipeline``/``PdfDevice``), а не из литералов этого модуля: шов
публикует словарь осей, а не заводит второй. Импорт ``core.domain.enums`` здесь — словарь
данных (StrEnum-объявления без I/O), а не доступ к домену: правила и store edge не видит
(изоляция слоя проверяется тестом ``test_server_source_has_no_store_or_domain_import``).

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

from core.domain.enums import DocumentLevel, PdfDevice, PdfPipeline
from core.ports.types import CrawlStrategy, ExtractionRoute, FetchMode

__all__ = [
    "CRAWL_OPS",
    "ENDPOINT_OPS",
    "EXTRACT_LEVEL_VALUES",
    "EXTRACT_OPS",
    "EXTRACT_VALUES",
    "FETCH_VALUES",
    "PDF_DEVICE_VALUES",
    "POLL_KIND_VALUES",
    "SCHEMA_DIR",
    "SEAM_SCHEMA_VERSION",
    "STRATEGY_VALUES",
    "contract_digest",
    "load_schema",
    "validate_crawl_request",
    "validate_envelope",
    "validate_extract_request",
    "validate_fetch_request",
    "validate_payload",
]

#: Версия контракта шва. Обязана совпадать с клиентской/серверной
#: ``DEFAULT_SCHEMA_VERSION`` (``core/adapters/remote_gateway.py``): несовпадение —
#: версионная граница §13 (07 §3/§8.3).
SEAM_SCHEMA_VERSION = "1"

#: Каталог опубликованных контрактов (``<repo>/contracts/seam``).
SCHEMA_DIR: Path = Path(__file__).resolve().parents[2] / "contracts" / "seam"

#: Операции протокола ``/v1/*`` (07 §3). Crawl-операции сохраняют слэш (``crawl/submit``);
#: то же — для извлечения по артефакту (``extract/upload``, 10 §5.4).
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
        "crawl/poll",
        "extract/upload",
        "extract/submit",
        "extract/collect",
        "extract/abort",
    }
)

#: Операции извлечения **по артефакту** (B1/B3; 10 §5.4): приём чанками, заказ извлечения,
#: забор результата, уборка брошенной загрузки.
EXTRACT_OPS: frozenset[str] = frozenset(
    {"extract/upload", "extract/submit", "extract/collect", "extract/abort"}
)

#: Допустимые значения оси ``level`` операций извлечения (**не** новый словарь).
#:
#: Источник — перечисления ядра: ``DocumentLevel`` (лестница ``crawl.document_level``, P5)
#: плюс ``PdfPipeline.TEXT_LAYER``. Второе слагаемое не «ещё один уровень», а **механизм**
#: (P7), у которого лестницы нет: ``documents/openapi`` публикует его как значение той же
#: оси, поэтому шов обязан принимать его там же, где принимает ``DocumentLevel``. Список
#: выводится из перечислений, а не переписывается литералами: иначе шов и ядро разошлись бы
#: молча, а ось уровня объявлена действующей (R5).
EXTRACT_LEVEL_VALUES: frozenset[str] = frozenset(
    {level.value for level in DocumentLevel} | {PdfPipeline.TEXT_LAYER.value}
)
#: Допустимые значения оси ``device`` запросов извлечения (P9, ``PdfDevice``).
PDF_DEVICE_VALUES: frozenset[str] = frozenset(device.value for device in PdfDevice)

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
    {
        "envelope",
        "fetch",
        "search",
        "registry",
        "capabilities",
        "health",
        "crawl",
        "extract",
    }
)

#: Crawl-операции очереди краула (Phase 3.1): POST /v1/crawl/{submit,status,cancel,collect}.
#: ``crawl/poll`` (Wave 5.1) — опрос фида/канала тем же семейством POST ``/v1/crawl/*``.
CRAWL_OPS: frozenset[str] = frozenset(
    {"crawl/submit", "crawl/status", "crawl/cancel", "crawl/collect", "crawl/poll"}
)

#: Допустимые виды каталогового документа запроса ``/v1/crawl/poll`` (Wave 5.1/D3).
POLL_KIND_VALUES: frozenset[str] = frozenset({"feed", "channel", "sitemap"})


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


def _check_required_int(
    violations: list[str], where: str, field: str, value: Any, *, minimum: int = 0
) -> None:
    """Поле обязано присутствовать и быть целым не меньше ``minimum``.

    ``bool`` отвергается: в Python он подкласс ``int``, поэтому ``True`` прошёл бы как
    ``1`` — и «объявленный размер» стал бы булевым без единого сообщения.
    """
    if not _is_int(value) or value < minimum:
        violations.append(f"{where}: обязательное поле {field} — целое число >= {minimum}")


def _check_required_bool(
    violations: list[str], where: str, field: str, value: Any
) -> None:
    """Поле обязано присутствовать и быть булевым."""
    if not isinstance(value, bool):
        violations.append(f"{where}: обязательное поле {field} — булево значение")


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
    # Условный фетч (additive): валидаторы ответа и признак 304.
    for field in ("etag", "last_modified"):
        _check_opt_str(violations, where, field, payload.get(field))
    _check_opt_bool(violations, where, "not_modified", payload.get("not_modified"))

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
            # Частичное извлечение (аддитивно): признак + счётчик прочитанных страниц +
            # причина. Форма проверяется здесь, потому что ядро не может вывести
            # частичность из markdown-строки — она приходит только этим каналом.
            _check_opt_int(
                violations, "fetch.text", "pages_converted", text.get("pages_converted")
            )
            _check_opt_bool(violations, "fetch.text", "partial", text.get("partial"))
            _check_opt_str(
                violations, "fetch.text", "partial_reason", text.get("partial_reason")
            )

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
    """Payload ``GET /v1/capabilities`` (07 §3): версии edge-компонентов + блок ``extract``.

    Блок ``extract`` (аддитивный, 10 §5.4) объявляет то, чего ядро иначе не знает:
    размер чанка, который узел действительно примет; уровни и устройства, которые он
    исполнит; и факт поддержки извлечения **по артефакту**. Без него ядро выбирало бы
    размер чанка из собственной константы и получало ``413`` на объявленном лимите тела.
    """
    if not isinstance(payload, Mapping):
        return ["capabilities: payload должен быть объектом"]
    violations: list[str] = []
    for field in ("extractor_version", "crawler_version", "search_engine"):
        _check_opt_str(violations, "capabilities", field, payload.get(field))
    formats = payload.get("supported_formats")
    if formats is not None and (
        not isinstance(formats, list) or not all(isinstance(item, str) for item in formats)
    ):
        violations.append("capabilities: supported_formats должен быть списком строк")
    extract = payload.get("extract")
    if extract is not None:
        violations.extend(_validate_capabilities_extract(extract))
    return violations


def _validate_capabilities_extract(extract: Any) -> list[str]:
    """Блок ``capabilities.extract``: объявленный размер чанка + исполнимые оси."""
    where = "capabilities.extract"
    if not isinstance(extract, Mapping):
        return [f"{where}: должен быть объектом или null"]
    violations: list[str] = []
    _check_required_int(
        violations, where, "max_upload_chunk_bytes", extract.get("max_upload_chunk_bytes"), minimum=1
    )
    _check_required_bool(violations, where, "supports_artifact", extract.get("supports_artifact"))
    for field in ("levels", "devices"):
        value = extract.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            violations.append(f"{where}: поле {field} должно быть списком строк")
    _check_opt_int(violations, where, "staging_ttl_s", extract.get("staging_ttl_s"))
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
    for field in (
        "status",
        "engine",
        "kind",
        "content_type",
        "fetched_at",
        "content_hash",
        "final_url",
        "etag",
        "last_modified",
    ):
        _check_opt_str(violations, where, field, item.get(field))
    _check_opt_int(violations, where, "http_status", item.get("http_status"))
    _check_opt_bool(violations, where, "not_modified", item.get("not_modified"))
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


def _validate_crawl_poll_entry(item: Any, index: int) -> list[str]:
    """Элемент ``entries`` ответа ``POST /v1/crawl/poll`` (Wave 5.1).

    Ссылка на запись: обязательный непустой ``url``; ``guid``/``published_at``/``title``
    — строки или ``null`` (без байтов — I19).
    """
    where = f"crawl/poll.entries[{index}]"
    if not isinstance(item, Mapping):
        return [f"{where}: элемент должен быть объектом"]
    violations: list[str] = []
    _check_required_str(violations, where, "url", item.get("url"))
    for field in ("guid", "published_at", "title"):
        _check_opt_str(violations, where, field, item.get(field))
    return violations


def _validate_crawl_poll_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/crawl/poll`` (Wave 5.1): ``{entries, count, truncated}``.

    ``entries`` — список ссылок на записи (URL+метаданные); ``count`` — целое;
    ``truncated`` — булево (усечение по ``limit``/``max_entries``).
    """
    if not isinstance(payload, Mapping):
        return ["crawl/poll: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/poll"
    entries = payload.get("entries")
    if not isinstance(entries, list):
        violations.append(f"{where}: поле entries должно быть списком")
    else:
        for index, item in enumerate(entries):
            violations.extend(_validate_crawl_poll_entry(item, index))
    _check_opt_int(violations, where, "count", payload.get("count"))
    _check_opt_bool(violations, where, "truncated", payload.get("truncated"))
    return violations


#: Статусы ответа ``POST /v1/extract/collect`` (10 §5.4): работа идёт / готова / упала /
#: работы нет. ``unknown`` обязателен: «нет работы» ≠ «ещё не готово», иначе ядро ждало бы
#: вечно артефакт, который никогда не загружали.
EXTRACT_COLLECT_STATUSES: frozenset[str] = frozenset(
    {"pending", "done", "error", "unknown"}
)


def _validate_extract_text(text: Any) -> list[str]:
    """Блок ``text`` ответа ``extract/collect`` — тот же состав, что ``fetch.text`` (07 §3).

    Повторяет форму ``$defs/text`` в ``extract.schema.json``: ядро получает результат
    извлечения в одном виде независимо от того, пришёл он вместе с фетчем или по артефакту.
    """
    if text is None:
        return []
    if not isinstance(text, Mapping):
        return ["extract/collect.text: должен быть объектом или null"]
    violations: list[str] = []
    where = "extract/collect.text"
    for field in ("content", "hash", "extractor_version", "partial_reason"):
        _check_opt_str(violations, where, field, text.get(field))
    for field in ("page_count", "pages_converted"):
        _check_opt_int(violations, where, field, text.get(field))
    _check_opt_bool(violations, where, "partial", text.get("partial"))
    return violations


def _validate_extract_upload_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/extract/upload``: сколько чанков и байт edge реально принял."""
    if not isinstance(payload, Mapping):
        return ["extract/upload: payload должен быть объектом"]
    violations: list[str] = []
    where = "extract/upload"
    _check_required_str(violations, where, "content_hash", payload.get("content_hash"))
    _check_required_int(violations, where, "received_chunks", payload.get("received_chunks"))
    _check_required_int(
        violations, where, "chunk_total", payload.get("chunk_total"), minimum=1
    )
    _check_required_int(violations, where, "received_bytes", payload.get("received_bytes"))
    _check_required_bool(violations, where, "complete", payload.get("complete"))
    _check_opt_str(violations, where, "expires_at", payload.get("expires_at"))
    return violations


def _validate_extract_submit_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/extract/submit``: принят ли заказ и **каким** экстрактором.

    ``extractor_version`` — идентичность исполняющего экстрактора в пространстве имён ядра
    (B2): ядро сравнивает её с объявленной, поэтому поле не подменяется «своей» строкой.
    """
    if not isinstance(payload, Mapping):
        return ["extract/submit: payload должен быть объектом"]
    violations: list[str] = []
    where = "extract/submit"
    _check_required_str(violations, where, "content_hash", payload.get("content_hash"))
    _check_required_bool(violations, where, "accepted", payload.get("accepted"))
    _check_opt_str(violations, where, "extractor_version", payload.get("extractor_version"))
    _check_opt_str(violations, where, "error", payload.get("error"))
    return violations


def _validate_extract_collect_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/extract/collect``: статус работы и (при ``done``) текст."""
    if not isinstance(payload, Mapping):
        return ["extract/collect: payload должен быть объектом"]
    violations: list[str] = []
    where = "extract/collect"
    _check_required_str(violations, where, "content_hash", payload.get("content_hash"))
    status = payload.get("status")
    if status not in EXTRACT_COLLECT_STATUSES:
        violations.append(
            f"{where}: status должен быть одним из {sorted(EXTRACT_COLLECT_STATUSES)}"
        )
    violations.extend(_validate_extract_text(payload.get("text")))
    for field in ("extractor_version", "error"):
        _check_opt_str(violations, where, field, payload.get(field))
    return violations


def _validate_extract_abort_response(payload: Any) -> list[str]:
    """Payload ``POST /v1/extract/abort``: убрана ли staging-область и сколько байт."""
    if not isinstance(payload, Mapping):
        return ["extract/abort: payload должен быть объектом"]
    violations: list[str] = []
    where = "extract/abort"
    _check_required_str(violations, where, "content_hash", payload.get("content_hash"))
    _check_required_bool(violations, where, "aborted", payload.get("aborted"))
    _check_required_int(violations, where, "removed_bytes", payload.get("removed_bytes"))
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
    "crawl/poll": _validate_crawl_poll_response,
    "extract/upload": _validate_extract_upload_response,
    "extract/submit": _validate_extract_submit_response,
    "extract/collect": _validate_extract_collect_response,
    "extract/abort": _validate_extract_abort_response,
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

    Контракт запроса: ``{url, fetch, extract, strategy, wait_for, lang,
    if_none_match, if_modified_since}``. ``url`` обязателен;
    ``fetch``/``extract``/``strategy`` — опциональные enum-оси (``http|browser``,
    ``auto|html|document``, ``auto|honest|hybrid``); ``wait_for``/``lang`` — строки или
    ``null``. ``strategy`` опционален: отсутствие = ``auto`` (обратная совместимость —
    поле additive). ``if_none_match``/``if_modified_since`` — валидаторы условного
    повторного получения (additive; отсутствие = безусловный GET). Неизвестное
    значение → нарушение (edge отвечает ``status=error``).
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
    for field in ("wait_for", "lang", "if_none_match", "if_modified_since"):
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
    * ``crawl/collect``: ``{job_id, cursor?, limit?}``;
    * ``crawl/poll``: ``{url, kind, params?, fetch?, extract?, strategy?, wait_for?}``.

    :param op: ``crawl/submit`` | ``crawl/status`` | ``crawl/cancel`` | ``crawl/collect`` |
        ``crawl/poll``.
    """
    where = str(op).strip().lower()
    if where == "crawl/submit":
        return _validate_crawl_submit_request(payload)
    if where in ("crawl/status", "crawl/cancel"):
        return _validate_crawl_job_request(where, payload)
    if where == "crawl/collect":
        return _validate_crawl_collect_request(payload)
    if where == "crawl/poll":
        return _validate_crawl_poll_request(payload)
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


def _validate_crawl_poll_request(payload: Any) -> list[str]:
    """Запрос ``crawl/poll`` (Wave 5.1): ``{url, kind, params?, <оси фетча>}``.

    ``url`` — обязательный непустой; ``kind`` — обязательный enum
    (``feed|channel|sitemap``, :data:`POLL_KIND_VALUES`); ``params`` — опциональный объект
    (``since`` строка/``limit`` целое); оси фетча — как у ``/v1/fetch``.
    """
    if not isinstance(payload, Mapping):
        return ["crawl/poll: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/poll"
    _check_required_str(violations, where, "url", payload.get("url"))
    _opt_enum(violations, where, "kind", payload.get("kind"), POLL_KIND_VALUES)
    if payload.get("kind") is None:
        violations.append(
            f"{where}: обязательное поле kind — одно из {sorted(POLL_KIND_VALUES)}"
        )
    _opt_enum(violations, where, "fetch", payload.get("fetch"), FETCH_VALUES)
    _opt_enum(violations, where, "extract", payload.get("extract"), EXTRACT_VALUES)
    _opt_enum(violations, where, "strategy", payload.get("strategy"), STRATEGY_VALUES)
    wait_for = payload.get("wait_for")
    if wait_for is not None and not isinstance(wait_for, str):
        violations.append(f"{where}: wait_for должен быть строкой или null")
    params = payload.get("params")
    if params is not None:
        violations.extend(_validate_crawl_poll_params(params))
    return violations


def _validate_crawl_poll_params(params: Any) -> list[str]:
    """``params`` запроса ``crawl/poll``: ``since`` строка/``limit`` целое (или ``null``)."""
    where = "crawl/poll.params"
    if not isinstance(params, Mapping):
        return [f"{where}: должен быть объектом"]
    violations: list[str] = []
    _check_opt_str(violations, where, "since", params.get("since"))
    _check_opt_int(violations, where, "limit", params.get("limit"))
    return violations


def _validate_crawl_submit_request(payload: Any) -> list[str]:
    """Запрос ``crawl/submit``: метаданные сессии + непустой список ``items``."""
    if not isinstance(payload, Mapping):
        return ["crawl/submit: payload должен быть объектом"]
    violations: list[str] = []
    where = "crawl/submit"
    for field in ("run_id", "session_id", "idempotency_key", "profile"):
        _check_required_str(violations, where, field, payload.get(field))
    _check_opt_str(violations, where, "user_agent", payload.get("user_agent"))
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        violations.append(f"{where}: поле items — непустой список")
        return violations
    for index, item in enumerate(items):
        violations.extend(_validate_crawl_submit_item(item, index))
    return violations


def _validate_crawl_submit_item(item: Any, index: int) -> list[str]:
    """Элемент ``items`` запроса ``crawl/submit`` (оси фетча — как у ``/v1/fetch``).

    Опциональные additive-поля: ``robots`` (bool, egress-вежливость) и ``rate_limit``
    (per-item диапазон задержек ``{min_s, max_s}`` — форма из §11.1), а также
    ``if_none_match``/``if_modified_since`` (валидаторы условного запроса).
    """
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
    for field in ("wait_for", "lang", "if_none_match", "if_modified_since"):
        _check_opt_str(violations, where, field, item.get(field))
    _check_opt_bool(violations, where, "robots", item.get("robots"))
    _validate_rate_limit(violations, where, item.get("rate_limit"))
    return violations


def _validate_rate_limit(violations: list[str], where: str, value: Any) -> None:
    """Опциональный ``rate_limit`` элемента ``crawl/submit`` (§11.1).

    Форма: ``{min_s, max_s}`` — оба обязательные неотрицательные числа (или ``null``/
    отсутствие ⇒ без пейсинга). Дополнительные ключи не допускаются.
    """
    if value is None:
        return
    if not isinstance(value, Mapping):
        violations.append(f"{where}: поле rate_limit должно быть объектом или null")
        return
    extra = set(value) - {"min_s", "max_s"}
    if extra:
        violations.append(
            f"{where}: rate_limit допускает только min_s/max_s, лишние: {sorted(extra)}"
        )
    for field in ("min_s", "max_s"):
        number = value.get(field)
        if isinstance(number, bool) or not isinstance(number, (int, float)):
            violations.append(f"{where}: rate_limit.{field} должно быть числом")
        elif number < 0:
            violations.append(f"{where}: rate_limit.{field} не может быть отрицательным")


# ---------------------------------------------------------------------------
# extract — запросы (B1/B3; 10 §5.4)
# ---------------------------------------------------------------------------


def validate_extract_request(op: str, payload: Any) -> list[str]:
    """Проверяет **запросный** payload операции извлечения по артефакту; ``[]`` — валиден.

    Контракты запросов (все — POST ``/v1/extract/*``, ``contracts/seam/extract.schema.json``):

    * ``extract/upload``: ``{content_hash, size, chunk_no, chunk_total, data_b64}`` —
      один чанк артефакта в base64;
    * ``extract/submit``: ``{content_hash, size, level, device?, format?, filename_hint?,
      lang?, extractor_declared?}`` — заказ извлечения уже принятых байтов;
    * ``extract/collect``: ``{content_hash, level?}`` — опрос результата;
    * ``extract/abort``: ``{content_hash}`` — уборка staging-области.

    ``level``/``device`` — **оси**, а не свободные строки: значения берутся из перечислений
    ядра (:data:`EXTRACT_LEVEL_VALUES`, :data:`PDF_DEVICE_VALUES`). Неизвестное значение —
    нарушение (edge отвечает ``status=error``), а не тихий дефолт: ось, объявленная в
    запросе и проигнорированная, сделала бы провенанс ложным (R5).

    :param op: ``extract/upload`` | ``extract/submit`` | ``extract/collect`` | ``extract/abort``.
    """
    where = str(op).strip().lower()
    if where == "extract/upload":
        return _validate_extract_upload_request(payload)
    if where == "extract/submit":
        return _validate_extract_submit_request(payload)
    if where == "extract/collect":
        return _validate_extract_collect_request(payload)
    if where == "extract/abort":
        return _validate_extract_abort_request(payload)
    return [
        f"неизвестная операция извлечения {op!r} "
        f"(ожидается одна из: {sorted(EXTRACT_OPS)})"
    ]


def _validate_extract_upload_request(payload: Any) -> list[str]:
    """Запрос ``extract/upload``: один чанк; ``chunk_no`` обязан укладываться в ``chunk_total``.

    Верхняя граница ``chunk_no < chunk_total`` не выводится из JSON-схемы (там два
    независимых ``minimum``), но без неё повтор с номером за пределами объявленного набора
    создал бы файл, который никто не соберёт: загрузка стала бы «завершённой» только по
    счётчику, а байт в ней не было бы.
    """
    if not isinstance(payload, Mapping):
        return ["extract/upload: payload должен быть объектом"]
    violations: list[str] = []
    where = "extract/upload"
    _check_required_str(violations, where, "content_hash", payload.get("content_hash"))
    _check_required_int(violations, where, "size", payload.get("size"))
    _check_required_int(violations, where, "chunk_no", payload.get("chunk_no"))
    _check_required_int(violations, where, "chunk_total", payload.get("chunk_total"), minimum=1)
    if not isinstance(payload.get("data_b64"), str):
        violations.append(f"{where}: обязательное поле data_b64 — строка (base64)")
    chunk_no = payload.get("chunk_no")
    chunk_total = payload.get("chunk_total")
    # Сравнение — только после сужения типа: ``payload.get`` даёт ``Any | None``, и
    # арифметика на ``None`` была бы падением, а не сообщением о нарушении контракта.
    if (
        isinstance(chunk_no, int)
        and not isinstance(chunk_no, bool)
        and isinstance(chunk_total, int)
        and not isinstance(chunk_total, bool)
        and chunk_total >= 1
        and chunk_no >= chunk_total
    ):
        violations.append(
            f"{where}: chunk_no={chunk_no} вне диапазона 0..{chunk_total - 1} "
            "(нумерация чанков — с нуля)"
        )
    return violations


def _validate_extract_submit_request(payload: Any) -> list[str]:
    """Запрос ``extract/submit``: артефакт + оси ``level`` (B3) и ``device`` (P9)."""
    if not isinstance(payload, Mapping):
        return ["extract/submit: payload должен быть объектом"]
    violations: list[str] = []
    where = "extract/submit"
    _check_required_str(violations, where, "content_hash", payload.get("content_hash"))
    _check_required_int(violations, where, "size", payload.get("size"))
    _opt_enum(violations, where, "level", payload.get("level"), EXTRACT_LEVEL_VALUES)
    if payload.get("level") is None:
        violations.append(
            f"{where}: обязательное поле level — одно из {sorted(EXTRACT_LEVEL_VALUES)}"
        )
    _opt_enum(violations, where, "device", payload.get("device"), PDF_DEVICE_VALUES)
    for field in ("format", "filename_hint", "lang", "extractor_declared"):
        _check_opt_str(violations, where, field, payload.get(field))
    return violations


def _validate_extract_collect_request(payload: Any) -> list[str]:
    """Запрос ``extract/collect``: ``content_hash`` (+ опциональное ожидание ``level``)."""
    if not isinstance(payload, Mapping):
        return ["extract/collect: payload должен быть объектом"]
    violations: list[str] = []
    where = "extract/collect"
    _check_required_str(violations, where, "content_hash", payload.get("content_hash"))
    _opt_enum(violations, where, "level", payload.get("level"), EXTRACT_LEVEL_VALUES)
    return violations


def _validate_extract_abort_request(payload: Any) -> list[str]:
    """Запрос ``extract/abort``: ``content_hash`` — единственная адресация staging-области."""
    if not isinstance(payload, Mapping):
        return ["extract/abort: payload должен быть объектом"]
    violations: list[str] = []
    _check_required_str(violations, "extract/abort", "content_hash", payload.get("content_hash"))
    return violations


def validate_payload(op: str, payload: Any) -> list[str]:
    """Проверяет payload операции ``/v1/*``; ``[]`` — валиден.

    :param op: ``fetch`` | ``search`` | ``registry`` | ``capabilities`` | ``health`` |
        ``crawl/submit`` | ``crawl/status`` | ``crawl/cancel`` | ``crawl/collect`` |
        ``crawl/poll`` | ``extract/upload`` | ``extract/submit`` | ``extract/collect`` |
        ``extract/abort``.
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
