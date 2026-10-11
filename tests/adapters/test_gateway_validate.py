"""Тесты ядровой валидации входящего из edge (W13/T13.5; 07 §3/§5/§8).

Сеть отсутствует: «fake gateway» — обычные payload-словари, имитирующие ответы
``/v1/*``. Проверяются: повторный ``content_hash`` (I19; T13.5 DoD — «испорченный»
ответ отвергается), лимиты размера (включая base64-декодированные), битый base64,
несовпадение ``schema_version``, обязательный ``source_url`` (R1), форма кандидатов
поиска, репорт санитайзинга, SSRF-политика URL и детерминизм. Фикстуры
категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import base64
import importlib
import json
import re
from pathlib import Path
from typing import Any

from core.adapters.gateway_validate import (
    MAX_RESPONSE_BYTES,
    ValidationIssue,
    check_size,
    decoded_base64_size,
    validate_envelope_shape,
    validate_extracted_doc,
    validate_raw_page,
    validate_registry_payload,
    validate_search_payload,
)
from core.ports.types import ExtractedDoc, RawPage, FetchMode
from core.util.hashing import content_hash

# ---------------------------------------------------------------------------
# Нейтральные фикстуры
# ---------------------------------------------------------------------------

_URL = "https://site.test/start"


def _fetch_payload(
    content: bytes,
    *,
    declared_hash: str | None = None,
    final_url: str = "https://site.test/final",
    content_type: str = "text/plain",
    text: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Payload ``/v1/fetch`` с сырыми байтами в base64 (форма 07 §3)."""
    return {
        "final_url": final_url,
        "status_code": 200,
        "content_type": content_type,
        "fetched_at": "2026-01-01T00:00:00Z",
        "raw": {
            "hash": content_hash(content) if declared_hash is None else declared_hash,
            "size": len(content),
            "bytes_b64": base64.b64encode(content).decode("ascii"),
        },
        "text": text,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Сырая страница — happy path
# ---------------------------------------------------------------------------


def test_raw_page_happy_path() -> None:
    content = b"neutral raw bytes"
    result = validate_raw_page(_fetch_payload(content), url=_URL)

    assert result.ok is True
    assert result.issues == []
    assert isinstance(result.value, RawPage)
    assert result.value.content == content
    assert result.value.url == "https://site.test/final"
    assert result.value.fetch_mode is FetchMode.HTTP
    assert result.value.status_code == 200


# ---------------------------------------------------------------------------
# T13.5 DoD: испорченный ответ отвергается (re-hash, I19)
# ---------------------------------------------------------------------------


def test_raw_page_corrupted_hash_rejected() -> None:
    content = b"actual bytes"
    payload = _fetch_payload(content, declared_hash=content_hash(b"tampered elsewhere"))

    result = validate_raw_page(payload, url=_URL)

    assert result.ok is False
    assert any(issue.code == "hash" for issue in result.issues)
    assert result.value is None


def test_raw_page_expected_hash_mismatch_rejected() -> None:
    content = b"actual bytes"
    payload = _fetch_payload(content)

    result = validate_raw_page(payload, url=_URL, expected_hash=content_hash(b"other"))

    assert result.ok is False
    assert any(issue.code == "hash" for issue in result.issues)
    assert result.value is None


def test_raw_page_expected_hash_match_accepted() -> None:
    content = b"actual bytes"
    payload = _fetch_payload(content)

    result = validate_raw_page(payload, url=_URL, expected_hash=content_hash(content))

    assert result.ok is True
    assert isinstance(result.value, RawPage)


# ---------------------------------------------------------------------------
# Битый base64
# ---------------------------------------------------------------------------


def test_raw_page_corrupt_base64_rejected() -> None:
    payload = {"raw": {"bytes_b64": "!!!not-base64!!!"}, "final_url": _URL}

    result = validate_raw_page(payload, url=_URL)

    assert result.ok is False
    assert any(issue.code == "decode" for issue in result.issues)
    assert result.value is None


# ---------------------------------------------------------------------------
# Лимиты размера (в т.ч. post-decode)
# ---------------------------------------------------------------------------


def test_check_size_within_limit_returns_none() -> None:
    assert check_size(_fetch_payload(b"small"), MAX_RESPONSE_BYTES) is None


def test_raw_page_oversized_serialized_rejected() -> None:
    content = b"x" * 90
    payload = _fetch_payload(content)
    decoded = decoded_base64_size(payload)
    wire = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    # Лимит между декодированным и сериализованным: ловит провод.
    max_bytes = (decoded + wire) // 2
    assert decoded <= max_bytes < wire

    result = validate_raw_page(payload, url=_URL, max_bytes=max_bytes)

    assert result.ok is False
    size_issues = [issue for issue in result.issues if issue.code == "size"]
    assert size_issues
    assert "декод" not in size_issues[0].message
    assert result.value is None


def test_raw_page_oversized_decoded_rejected() -> None:
    content = b"y" * 4096
    payload = _fetch_payload(content)
    decoded = decoded_base64_size(payload)
    assert decoded == len(content)

    result = validate_raw_page(payload, url=_URL, max_bytes=64)

    assert result.ok is False
    size_issues = [issue for issue in result.issues if issue.code == "size"]
    assert size_issues
    assert "декод" in size_issues[0].message
    assert result.value is None


# ---------------------------------------------------------------------------
# Схема/версия конверта
# ---------------------------------------------------------------------------


def _envelope(payload: Any, *, schema: str = "1", status: str = "ok") -> dict[str, Any]:
    return {
        "schema_version": schema,
        "request_id": "0" * 32,
        "status": status,
        "payload": payload,
        "error": None if status == "ok" else "boom",
    }


def test_envelope_shape_valid() -> None:
    assert validate_envelope_shape(_envelope({"a": 1})) == []


def test_envelope_shape_version_mismatch_rejected() -> None:
    issues = validate_envelope_shape(_envelope({"a": 1}, schema="9"))

    assert issues
    assert any(issue.code == "version" for issue in issues)


def test_envelope_shape_non_mapping_rejected() -> None:
    issues = validate_envelope_shape([1, 2, 3])

    assert issues
    assert issues[0].code == "schema"


def test_envelope_shape_custom_expected_version() -> None:
    # expected_schema_version — параметр версионной границы (§13).
    assert validate_envelope_shape(_envelope({"a": 1}, schema="2"), "2") == []


# ---------------------------------------------------------------------------
# URL / SSRF
# ---------------------------------------------------------------------------


def test_raw_page_private_url_rejected() -> None:
    content = b"body"
    result = validate_raw_page(_fetch_payload(content), url="http://127.0.0.1/internal")

    assert result.ok is False
    assert any(issue.code == "url" for issue in result.issues)
    assert result.value is None


# ---------------------------------------------------------------------------
# Санитайзинг — репорт без мутации
# ---------------------------------------------------------------------------


def test_raw_page_html_sanitization_reported() -> None:
    content = b"<html><body><script>alert(1)</script>visible</body></html>"
    payload = _fetch_payload(content, content_type="text/html; charset=utf-8")

    result = validate_raw_page(payload, url=_URL)

    assert any(issue.code == "sanitize" for issue in result.issues)
    # Предупреждение не отклоняет ответ и не мутирует байты (07 §8.5).
    assert result.ok is True
    assert isinstance(result.value, RawPage)
    assert result.value.content == content


def test_raw_page_plain_text_not_flagged() -> None:
    result = validate_raw_page(_fetch_payload(b"plain text, no markup"), url=_URL)

    assert result.ok is True
    assert all(issue.code != "sanitize" for issue in result.issues)


# ---------------------------------------------------------------------------
# Извлечённый документ
# ---------------------------------------------------------------------------


def test_extracted_doc_happy_path() -> None:
    text = "# clean markdown"
    payload = {
        "final_url": _URL,
        "text": {
            "content": text,
            "hash": content_hash(text),
            "extractor_version": "extractor-1",
            "page_count": 1,
        },
        "error": None,
    }

    result = validate_extracted_doc(payload)

    assert result.ok is True
    assert isinstance(result.value, ExtractedDoc)
    assert result.value.text == text
    assert result.value.content_hash == content_hash(text)
    assert result.value.extractor_version == "extractor-1"
    assert result.value.page_count == 1


def test_extracted_doc_corrupted_hash_rejected() -> None:
    payload = {
        "text": {
            "content": "actual",
            "hash": content_hash("tampered"),
            "extractor_version": "e1",
        }
    }

    result = validate_extracted_doc(payload)

    assert result.ok is False
    assert any(issue.code == "hash" for issue in result.issues)
    assert result.value is None


def test_extracted_doc_missing_extractor_version_rejected() -> None:
    text = "body"
    payload = {"text": {"content": text, "hash": content_hash(text)}}

    result = validate_extracted_doc(payload)

    assert result.ok is False
    assert any(issue.field == "text.extractor_version" for issue in result.issues)
    assert result.value is None


# ---------------------------------------------------------------------------
# Поиск / реестр (R1)
# ---------------------------------------------------------------------------


def test_search_payload_happy_path() -> None:
    issues = validate_search_payload(
        [{"url": "https://a.test", "title": "A", "snippet": "s", "rank": 1}]
    )
    assert issues == []


def test_search_payload_malformed_item_rejected() -> None:
    issues = validate_search_payload([{"title": "no url", "rank": 1}, "not-a-mapping"])

    assert issues
    assert all(issue.code == "schema" for issue in issues)
    assert any(issue.field == "search[0].url" for issue in issues)


def test_search_payload_non_list_rejected() -> None:
    issues = validate_search_payload({"nope": True})

    assert issues
    assert issues[0].code == "schema"


def test_registry_payload_missing_source_url_rejected() -> None:
    issues = validate_registry_payload([{"fact_json": "{}"}])

    assert issues
    assert any(issue.field == "registry[0].source_url" for issue in issues)


def test_registry_payload_blank_source_url_rejected() -> None:
    issues = validate_registry_payload([{"source_url": "   ", "fact_json": "{}"}])

    assert issues
    assert any(issue.field == "registry[0].source_url" for issue in issues)


def test_registry_payload_happy_path() -> None:
    issues = validate_registry_payload(
        [{"source_url": "https://registry.test/a", "fact_json": json.dumps({"k": "v"})}]
    )
    assert issues == []


# ---------------------------------------------------------------------------
# Детерминизм и нейтральность
# ---------------------------------------------------------------------------


def test_validation_is_deterministic() -> None:
    payload = _fetch_payload(b"deterministic", declared_hash=content_hash(b"other"))

    first = validate_raw_page(payload, url=_URL)
    second = validate_raw_page(payload, url=_URL)

    assert first == second
    assert first.issues == second.issues
    assert isinstance(first.issues[0], ValidationIssue)


def test_module_source_is_category_neutral() -> None:
    module = importlib.import_module("core.adapters.gateway_validate")
    assert module.__file__ is not None
    source = Path(module.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    hits = [line for line in source.splitlines() if forbidden.search(line)]
    assert hits == []
