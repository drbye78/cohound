"""Валидация всего входящего из edge — схема, лимиты, повторный хэш, санитайзинг.

Ядро **не доверяет** edge (07 §5): edge обрабатывает недоверенный контент и имеет
эгрессию, поэтому любой ответ шва ``POST/GET /v1/*`` проходит независимую проверку
на стороне ядра (07 §3/§5, I19). Слой автономен: remote-прокси могут вызывать его
для дополнительной проверки, но сами прокси **не** изменяются.

Проверяется:
- **схема/версия** конверта (проверка локальная, без импорта ``core.edge``: матрица
  слоёв 01 §1 запрещает ``adapters → edge``; контракт повторён небольшой проверкой);
- **размеры** ответа и base64-декодированного содержимого (:func:`check_size`);
- **повторный ``content_hash``** (I19): пересчитывается ядром и сверяется с
  заявленным edge/ожидаемым; несовпадение — мутация/порча (в т.ч. DLP/IDS) →
  отвергается (07 §8.5);
- **URL** (SSRF-политика ``validate_url``);
- **санитайзинг**: активный HTML не принимается на веру, но контент **не мутируется**
  (07 §8.5 «инспекция ≠ мутация») — изменение только репортится предупреждением.

Результат — :class:`ValidatedResponse` с ``ok`` (нет отклоняющих нарушений) и
списком :class:`ValidationIssue`. Коды: ``schema``/``version``/``size``/``decode``/
``hash``/``url`` — отклоняют; ``sanitize`` — предупреждает. Модуль категорийно
нейтрален (§1.3), опирается только на stdlib и утилиты ядра, детерминирован.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from core.ports.types import ExtractedDoc, FetchMode, RawPage
from core.util.hashing import content_hash
from core.util.sanitize import sanitize_html
from core.util.security import validate_url

__all__ = [
    "MAX_RESPONSE_BYTES",
    "ValidationIssue",
    "ValidatedResponse",
    "check_size",
    "decoded_base64_size",
    "validate_envelope_shape",
    "validate_raw_page",
    "validate_extracted_doc",
    "validate_search_payload",
    "validate_registry_payload",
]

#: Лимит ответа по умолчанию: 50 MiB (07 §7 ``max_response_bytes``).
MAX_RESPONSE_BYTES = 52_428_800

#: Коды нарушений, при которых ответ **отклоняется** (значение не выдаётся).
#: ``sanitize`` сюда не входит: активный HTML репортится, но контент не мутируется.
_REJECT_CODES = frozenset({"schema", "version", "size", "decode", "hash", "url"})


# ---------------------------------------------------------------------------
# Результаты валидации
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidationIssue:
    """Одно нарушение валидации входящего ответа edge.

    :param code: машинный код (``schema``/``version``/``size``/``decode``/``hash``/
        ``url``/``sanitize``).
    :param message: человекочитаемое пояснение (для логов/аудита).
    :param field: путь поля в ответе, к которому относится нарушение (или ``None``).
    """

    code: str
    message: str
    field: str | None = None


@dataclass(frozen=True)
class ValidatedResponse:
    """Итог валидации: ``ok`` — нет **отклоняющих** нарушений.

    :param ok: ``True``, если среди ``issues`` нет кодов из ``_REJECT_CODES``.
        Предупреждения (``sanitize``) не отклоняют ответ.
    :param issues: все найденные нарушения/предупреждения (детерминированный порядок).
    :param value: проверенный/нормализованный объект (``RawPage``/``ExtractedDoc``)
        либо ``None``, если ответ отклонён.
    """

    ok: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    value: Any = None


def _has_rejecting(issues: list[ValidationIssue]) -> bool:
    """Есть ли среди нарушений хотя бы одно отклоняющее (см. ``_REJECT_CODES``)."""
    return any(issue.code in _REJECT_CODES for issue in issues)


# ---------------------------------------------------------------------------
# Размерные лимиты
# ---------------------------------------------------------------------------


def _is_int(value: Any) -> bool:
    """``int``, но не ``bool`` (``bool`` — подкласс ``int``)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _base64_lenient_len(encoded: str) -> int:
    """Длина base64-декодированных байтов; при битом base64 — ``0`` (не бросаем)."""
    try:
        return len(base64.b64decode(encoded, validate=False))
    except (binascii.Error, ValueError):
        return 0


def decoded_base64_size(value: Any) -> int:
    """Суммарная длина base64-декодированных ``bytes_b64`` (рекурсивно).

    Ищутся все вложенные поля ``bytes_b64`` (в т.ч. в ``raw``/``payload``).
    Битые значения не роняют проверку — учитываются как ``0`` (их отвергает
    :func:`validate_raw_page` отдельным кодом ``decode``).
    """
    total = 0
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key == "bytes_b64" and isinstance(item, str):
                total += _base64_lenient_len(item)
            else:
                total += decoded_base64_size(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            total += decoded_base64_size(item)
    return total


def _serialized_size(response: Any) -> int:
    """Оценка размера ответа на проводе (байты UTF-8, детерминированно)."""
    if isinstance(response, (bytes, bytearray)):
        return len(response)
    if isinstance(response, str):
        return len(response.encode("utf-8"))
    try:
        return len(
            json.dumps(
                response,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                default=str,
            ).encode("utf-8")
        )
    except (TypeError, ValueError):
        return len(repr(response).encode("utf-8"))


def check_size(
    response: Any, max_bytes: int = MAX_RESPONSE_BYTES
) -> ValidationIssue | None:
    """Лимит размера ответа edge (07 §7); ``None`` — в пределах лимита.

    Учитываются **обе** величины (первой проверяется декодированная, чтобы
    поймать раздувание содержимого): суммарный размер base64-декодированного
    ``bytes_b64`` и размер сериализованного ответа. ``max_bytes <= 0`` — лимит
    отключён.
    """
    if max_bytes is None or max_bytes <= 0:
        return None
    decoded = decoded_base64_size(response)
    if decoded > max_bytes:
        return ValidationIssue(
            "size",
            f"размер base64-декодированного содержимого {decoded} байт > лимита {max_bytes}",
            "bytes_b64",
        )
    wire = _serialized_size(response)
    if wire > max_bytes:
        return ValidationIssue(
            "size", f"размер ответа edge {wire} байт > лимита {max_bytes}", None
        )
    return None


# ---------------------------------------------------------------------------
# Схема конверта (локальная проверка; без зависимости adapters → edge, 01 §1)
# ---------------------------------------------------------------------------


def _envelope_violations(obj: Mapping[str, Any]) -> list[ValidationIssue]:
    """Локальная проверка формы **ответного** конверта шва (07 §3).

    Повторяет контракт ``seam_schema.validate_envelope``, но **не импортирует**
    ``core.edge`` (адаптеры не зависят от edge, 01 §1): ``status`` ∈ ``{ok, error}``,
    ``request_id`` — непустая строка, ``status=ok`` → непустой ``payload`` и
    ``error=null``; ``status=error`` → непустая строка ``error``.
    """
    violations: list[ValidationIssue] = []
    request_id = obj.get("request_id")
    if request_id is None:
        violations.append(ValidationIssue("schema", "отсутствует поле request_id", "request_id"))
    elif not isinstance(request_id, str) or not request_id:
        violations.append(
            ValidationIssue("schema", "request_id должен быть непустой строкой", "request_id")
        )

    status = obj.get("status")
    if status not in ("ok", "error"):
        violations.append(
            ValidationIssue("schema", f"status={status!r} недопустим (ожидается ok или error)", "status")
        )
        return violations

    error = obj.get("error")
    if status == "ok":
        if "payload" not in obj or obj.get("payload") is None:
            violations.append(
                ValidationIssue("schema", "при status=ok требуется непустой payload", "payload")
            )
        if error is not None:
            violations.append(
                ValidationIssue("schema", "при status=ok поле error должно быть null", "error")
            )
    elif not isinstance(error, str) or not error:
        violations.append(
            ValidationIssue("schema", "при status=error требуется непустая строка error", "error")
        )
    return violations


def validate_envelope_shape(
    obj: Any, expected_schema_version: str = "1"
) -> list[ValidationIssue]:
    """Проверяет форму **ответного** конверта шва (07 §3); ``[]`` — валиден.

    Версия сверяется с ``expected_schema_version`` (версионная граница §13);
    остальные поля проверяются локально (:func:`_envelope_violations`) — без
    импорта ``core.edge.seam_schema`` (матрица слоёв 01 §1).
    """
    if not isinstance(obj, Mapping):
        return [
            ValidationIssue(
                "schema",
                f"конверт должен быть объектом, получено {type(obj).__name__}",
                None,
            )
        ]

    issues: list[ValidationIssue] = []
    if obj.get("schema_version") != expected_schema_version:
        issues.append(
            ValidationIssue(
                "version",
                f"schema_version={obj.get('schema_version')!r} не совпадает с ожидаемым "
                f"{expected_schema_version!r} (версионная граница §13)",
                "schema_version",
            )
        )
    issues.extend(_envelope_violations(obj))
    return issues


# ---------------------------------------------------------------------------
# Общие мелкие проверки полей
# ---------------------------------------------------------------------------


def _url_issues(value: Any, field: str) -> list[ValidationIssue]:
    """SSRF-проверка URL (``validate_url``); ``[]`` — допустим."""
    try:
        validate_url(str(value))
    except ValueError as exc:
        return [ValidationIssue("url", str(exc), field)]
    return []


def _looks_like_html(content_type: Any, text: str) -> bool:
    """Эвристика «содержимое похоже на HTML» (для выборочного санитайзинга)."""
    if content_type and "html" in str(content_type).lower():
        return True
    head = text.lstrip()[:200].lower()
    return (
        head.startswith("<!doctype")
        or head.startswith("<html")
        or "<body" in head
        or "<script" in head
        or "<style" in head
    )


def _sanitize_issues(content: bytes, content_type: Any) -> list[ValidationIssue]:
    """Репортит активный HTML; контент не мутирует (07 §8.5, инспекция ≠ мутация)."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return []
    if not _looks_like_html(content_type, text):
        return []
    if sanitize_html(text) != text:
        return [
            ValidationIssue(
                "sanitize",
                "в ответе обнаружен активный HTML; ядро не принимает его на веру "
                "(контент не мутируется, санитайзинг применяется на границе экстракции)",
                "raw.content",
            )
        ]
    return []


def _decode_raw(
    raw: Any, issues: list[ValidationIssue]
) -> tuple[bytes | None, str | None]:
    """Декодирует сырые байты ``raw`` (07 §3/§8); возвращает ``(bytes, hash)``.

    Источники: ``raw.bytes_b64`` (строгий base64) → ``raw.content``. Битый base64 —
    нарушение ``decode`` (без тихого фолбэка).
    """
    if not isinstance(raw, Mapping):
        return None, None
    claimed = raw.get("hash")
    declared = None if claimed is None else str(claimed)
    encoded = raw.get("bytes_b64")
    if encoded is not None:
        try:
            return base64.b64decode(str(encoded), validate=True), declared
        except (binascii.Error, ValueError):
            issues.append(
                ValidationIssue(
                    "decode", "raw.bytes_b64 не декодируется (битый base64)", "raw.bytes_b64"
                )
            )
            return None, declared
    content = raw.get("content")
    if content is None:
        return None, declared
    if isinstance(content, bytes):
        return content, declared
    return str(content).encode("utf-8"), declared


def _iter_items(payload: Any, op: str) -> tuple[list[Any], list[ValidationIssue]]:
    """Списочный payload (07 §3) либо обёртка ``{"results": [...]}``."""
    issues: list[ValidationIssue] = []
    if isinstance(payload, list):
        return list(payload), issues
    if isinstance(payload, Mapping) and isinstance(payload.get("results"), list):
        return list(payload["results"]), issues
    issues.append(
        ValidationIssue(
            "schema", f'{op}: payload должен быть списком (или {{"results": [...]}})', "payload"
        )
    )
    return [], issues


# ---------------------------------------------------------------------------
# Сырая страница (/v1/fetch; ось extract: auto|html|document, извлечение best-effort)
# ---------------------------------------------------------------------------


def validate_raw_page(
    response: Any,
    *,
    url: str,
    expected_hash: str | None = None,
    max_bytes: int = MAX_RESPONSE_BYTES,
    fetch: FetchMode = FetchMode.HTTP,
    sanitize: bool = True,
) -> ValidatedResponse:
    """Проверяет payload ``/v1/fetch`` с сырыми байтами и собирает ``RawPage``.

    Запрос несёт ось ``extract`` (``auto`` | ``html`` | ``document``), но сырые байты edge
    возвращает всегда — извлечение best-effort, поэтому отсутствие ``text`` допустимо.
    Байты берутся из ``raw.bytes_b64`` → ``raw.content`` → (деградация)
    ``text.content``. Ядро **пересчитывает** ``content_hash`` и сверяет с
    ``expected_hash``/``raw.hash`` (I19). При наличии активного HTML добавляется
    предупреждение ``sanitize``, но байты не мутируются (07 §8.5).
    """
    issues: list[ValidationIssue] = []
    if not isinstance(response, Mapping):
        return ValidatedResponse(
            ok=False,
            issues=[
                ValidationIssue(
                    "schema",
                    f"payload fetch должен быть объектом, получено {type(response).__name__}",
                    None,
                )
            ],
            value=None,
        )

    size_issue = check_size(response, max_bytes)
    if size_issue is not None:
        issues.append(size_issue)

    content, declared = _decode_raw(response.get("raw"), issues)
    if content is None:
        text = response.get("text")
        if isinstance(text, Mapping) and text.get("content") is not None:
            raw_text = text["content"]
            content = raw_text if isinstance(raw_text, bytes) else str(raw_text).encode("utf-8")
    if content is None:
        issues.append(
            ValidationIssue(
                "schema",
                "ответ fetch без сырых байтов (raw.bytes_b64/raw.content/text.content)",
                "raw",
            )
        )

    if content is not None:
        computed = content_hash(content)
        for label, claimed in (("expected_hash", expected_hash), ("raw.hash", declared)):
            if claimed is not None and str(claimed) != computed:
                issues.append(
                    ValidationIssue(
                        "hash",
                        f"{label}: заявленный content_hash не совпал с пересчитанным "
                        "в ядре (I19)",
                        label,
                    )
                )
        if sanitize:
            issues.extend(_sanitize_issues(content, response.get("content_type")))

    not_modified = response.get("not_modified") is True
    if not_modified and not content:
        # 304 не несёт представления: пустое тело — неконсистентный ответ edge.
        issues.append(
            ValidationIssue(
                "schema",
                "not_modified=true без закэшированных байтов представления",
                "raw",
            )
        )

    issues.extend(_url_issues(url, "url"))
    final_url = response.get("final_url")
    if final_url:
        issues.extend(_url_issues(final_url, "final_url"))

    value: RawPage | None = None
    if not _has_rejecting(issues):
        status_code = response.get("status_code")
        lang = response.get("lang")
        content_type = response.get("content_type")
        etag = response.get("etag")
        last_modified = response.get("last_modified")
        value = RawPage(
            url=str(final_url or url),
            content=content if content is not None else b"",
            fetched_at=str(response.get("fetched_at") or ""),
            fetch_mode=FetchMode(fetch),
            lang=str(lang) if lang else None,
            status_code=int(status_code) if status_code is not None else None,
            content_type=str(content_type) if content_type else None,
            etag=str(etag) if etag else None,
            last_modified=str(last_modified) if last_modified else None,
            not_modified=bool(response.get("not_modified")),
        )
    return ValidatedResponse(ok=not _has_rejecting(issues), issues=issues, value=value)


# ---------------------------------------------------------------------------
# Извлечённый документ (/v1/fetch; ось extract: auto|html|document)
# ---------------------------------------------------------------------------


def validate_extracted_doc(
    response: Any,
    *,
    expected_hash: str | None = None,
    max_bytes: int = MAX_RESPONSE_BYTES,
) -> ValidatedResponse:
    """Проверяет payload ``/v1/fetch`` с извлечённым текстом и собирает ``ExtractedDoc``.

    Ось ``extract`` запроса (``auto`` | ``html`` | ``document``) выбирает маршрут извлечения
    на edge (best-effort). ``content_hash`` очищенного текста **пересчитывается** ядром и
    сверяется с ``expected_hash``/``text.hash`` (I19); ``text.extractor_version`` обязателен.
    """
    issues: list[ValidationIssue] = []
    if not isinstance(response, Mapping):
        return ValidatedResponse(
            ok=False,
            issues=[
                ValidationIssue(
                    "schema",
                    f"payload fetch(extract) должен быть объектом, получено "
                    f"{type(response).__name__}",
                    None,
                )
            ],
            value=None,
        )

    size_issue = check_size(response, max_bytes)
    if size_issue is not None:
        issues.append(size_issue)

    text = response.get("text")
    if not isinstance(text, Mapping):
        issues.append(
            ValidationIssue("schema", "ответ fetch(extract) без поля text", "text")
        )
        return ValidatedResponse(ok=False, issues=issues, value=None)

    raw_content = text.get("content")
    if raw_content is not None and not isinstance(raw_content, str):
        issues.append(
            ValidationIssue("schema", "text.content должен быть строкой или null", "text.content")
        )
    content_str = "" if raw_content is None else str(raw_content)

    version = text.get("extractor_version")
    if not isinstance(version, str) or not version:
        issues.append(
            ValidationIssue(
                "schema",
                "text.extractor_version обязателен (непустая строка)",
                "text.extractor_version",
            )
        )

    computed = content_hash(content_str)
    for label, claimed in (("expected_hash", expected_hash), ("text.hash", text.get("hash"))):
        if claimed is not None and str(claimed) != computed:
            issues.append(
                ValidationIssue(
                    "hash",
                    f"{label}: заявленный content_hash не совпал с пересчитанным в ядре (I19)",
                    label,
                )
            )

    page_count = text.get("page_count")
    if page_count is not None and not _is_int(page_count):
        issues.append(
            ValidationIssue(
                "schema", "text.page_count должен быть целым числом или null", "text.page_count"
            )
        )

    value: ExtractedDoc | None = None
    if not _has_rejecting(issues):
        value = ExtractedDoc(
            text=content_str,
            content_hash=computed,
            extractor_version=str(version),
            page_count=page_count,
            error=None,
        )
    return ValidatedResponse(ok=not _has_rejecting(issues), issues=issues, value=value)


# ---------------------------------------------------------------------------
# Кандидаты поиска (/v1/search) и реестровые факты (/v1/registry)
# ---------------------------------------------------------------------------


def validate_search_payload(payload: Any) -> list[ValidationIssue]:
    """Проверяет payload ``/v1/search`` — список кандидатов (I19, не evidence)."""
    items, issues = _iter_items(payload, "search")
    for index, item in enumerate(items):
        where = f"search[{index}]"
        if not isinstance(item, Mapping):
            issues.append(ValidationIssue("schema", f"{where}: элемент должен быть объектом", where))
            continue
        url = item.get("url")
        if not isinstance(url, str) or not url.strip():
            issues.append(
                ValidationIssue("schema", f"{where}: обязательное поле url — непустая строка", f"{where}.url")
            )
        else:
            issues.extend(_url_issues(url, f"{where}.url"))
        for name in ("title", "snippet"):
            value = item.get(name)
            if value is not None and not isinstance(value, str):
                issues.append(
                    ValidationIssue("schema", f"{where}: {name} должно быть строкой или null", f"{where}.{name}")
                )
        rank = item.get("rank")
        if rank is not None and not _is_int(rank):
            issues.append(
                ValidationIssue("schema", f"{where}: rank должен быть целым числом или null", f"{where}.rank")
            )
    return issues


def validate_registry_payload(payload: Any) -> list[ValidationIssue]:
    """Проверяет payload ``/v1/registry`` — факты с обязательным ``source_url`` (R1)."""
    items, issues = _iter_items(payload, "registry")
    for index, item in enumerate(items):
        where = f"registry[{index}]"
        if not isinstance(item, Mapping):
            issues.append(ValidationIssue("schema", f"{where}: элемент должен быть объектом", where))
            continue
        source_url = item.get("source_url")
        if not isinstance(source_url, str) or not source_url.strip():
            issues.append(
                ValidationIssue(
                    "schema",
                    f"{where}: обязательное поле source_url — непустая строка (R1)",
                    f"{where}.source_url",
                )
            )
        else:
            issues.extend(_url_issues(source_url, f"{where}.source_url"))
        fact_json = item.get("fact_json")
        if fact_json is not None and not isinstance(fact_json, (str, Mapping, list)):
            issues.append(
                ValidationIssue(
                    "schema",
                    f"{where}: fact_json должен быть строкой, объектом, списком или null",
                    f"{where}.fact_json",
                )
            )
        fetched_at = item.get("fetched_at")
        if fetched_at is not None and not isinstance(fetched_at, str):
            issues.append(
                ValidationIssue(
                    "schema", f"{where}: fetched_at должно быть строкой или null", f"{where}.fetched_at"
                )
            )
    return issues
