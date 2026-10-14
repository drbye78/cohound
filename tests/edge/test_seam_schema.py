"""Контракт-тесты публикуемых схем шва ``/v1/*`` (W13/T13.1/T13.7; 07 §3/§8).

Проверяются: загрузка и валидность всех контрактов ``contracts/seam/*.json``;
валидатор конверта (:func:`validate_envelope`) на ok/error-формах;
валидатор payload по эндпоинтам (:func:`validate_payload`), в т.ч. R1
(``source_url``) для ``registry``; совместимость с конвертом клиента
``HttpContentGateway``; детерминированность :func:`contract_digest`; полнота
``openapi.json`` по эндпоинтам. Сети нет, зависимости — stdlib + pytest.
"""

from __future__ import annotations

import json

import pytest

from core.adapters.remote_gateway import DEFAULT_SCHEMA_VERSION, HttpContentGateway
from core.edge import seam_schema
from core.edge.seam_schema import (
    CRAWL_OPS,
    ENDPOINT_OPS,
    SEAM_SCHEMA_VERSION,
    STRATEGY_VALUES,
    contract_digest,
    load_schema,
    validate_crawl_request,
    validate_envelope,
    validate_fetch_request,
    validate_payload,
)

#: Все публикуемые JSON-контракты (схемы + OpenAPI).
_SCHEMA_FILES = (
    "envelope.schema.json",
    "fetch.schema.json",
    "search.schema.json",
    "registry.schema.json",
    "capabilities.schema.json",
    "health.schema.json",
    "crawl.schema.json",
    "openapi.json",
)

#: Контрактные файлы схем (без OpenAPI), пригодные для :func:`load_schema`.
_LOADABLE = ("envelope", "fetch", "search", "registry", "capabilities", "health", "crawl")

_REQUEST_ID = "0" * 32


def _ok(payload: object) -> dict[str, object]:
    """Валидный ответный конверт (07 §3)."""
    return {
        "schema_version": SEAM_SCHEMA_VERSION,
        "request_id": _REQUEST_ID,
        "status": "ok",
        "payload": payload,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Наличие и валидность контрактов
# ---------------------------------------------------------------------------


def test_schema_files_exist_at_expected_paths() -> None:
    assert seam_schema.SCHEMA_DIR.is_dir()
    for name in _SCHEMA_FILES:
        assert (seam_schema.SCHEMA_DIR / name).is_file(), name


@pytest.mark.parametrize("name", _SCHEMA_FILES)
def test_contracts_are_valid_json(name: str) -> None:
    data = json.loads((seam_schema.SCHEMA_DIR / name).read_text(encoding="utf-8"))
    assert isinstance(data, dict)


@pytest.mark.parametrize("name", _SCHEMA_FILES)
def test_contracts_declare_draft_2020_12(name: str) -> None:
    data = json.loads((seam_schema.SCHEMA_DIR / name).read_text(encoding="utf-8"))
    # openapi.json — не JSON Schema; у схем $schema обязателен.
    if name != "openapi.json":
        assert data["$schema"] == "https://json-schema.org/draft/2020-12/schema"


def test_envelope_schema_declares_contract_version() -> None:
    schema = load_schema("envelope")
    assert schema["properties"]["schema_version"]["const"] == SEAM_SCHEMA_VERSION
    assert SEAM_SCHEMA_VERSION == "1"


def test_schema_version_matches_client_constant() -> None:
    # Клиент и публикуемая схема обязаны согласовываться (версионная граница §13).
    assert SEAM_SCHEMA_VERSION == DEFAULT_SCHEMA_VERSION


def test_load_schema_accepts_full_filename_and_openapi() -> None:
    assert load_schema("envelope.schema.json")["title"].startswith("coHound")
    assert load_schema("openapi")["openapi"] == "3.1.0"
    assert isinstance(load_schema("fetch"), dict)


def test_load_schema_rejects_unknown_and_traversal() -> None:
    with pytest.raises(FileNotFoundError):
        load_schema("no-such-contract")
    with pytest.raises(ValueError):
        load_schema("../envelope.schema.json")


# ---------------------------------------------------------------------------
# validate_envelope
# ---------------------------------------------------------------------------


def test_validate_envelope_accepts_well_formed() -> None:
    assert validate_envelope(_ok({"answer": 42})) == []


def test_validate_envelope_rejects_non_mapping() -> None:
    assert validate_envelope(["not", "a", "mapping"])  # type: ignore[arg-type]


def test_validate_envelope_rejects_missing_schema_version() -> None:
    envelope = _ok({})
    del envelope["schema_version"]
    violations = validate_envelope(envelope)
    assert any("schema_version" in v for v in violations)


def test_validate_envelope_rejects_wrong_version() -> None:
    violations = validate_envelope(_ok({}) | {"schema_version": "999"})
    assert any("schema_version" in v for v in violations)


def test_validate_envelope_rejects_unknown_status() -> None:
    violations = validate_envelope(_ok({}) | {"status": "banana"})
    assert any("status" in v for v in violations)


def test_validate_envelope_rejects_error_without_error() -> None:
    envelope = _ok(None) | {"status": "error", "error": None}
    violations = validate_envelope(envelope)
    assert any("error" in v for v in violations)


def test_validate_envelope_rejects_ok_without_payload() -> None:
    envelope = _ok(None)
    del envelope["payload"]
    violations = validate_envelope(envelope)
    assert any("payload" in v for v in violations)


def test_validate_envelope_rejects_ok_with_none_payload() -> None:
    violations = validate_envelope(_ok(None))
    assert any("payload" in v for v in violations)


def test_validate_envelope_accepts_error_form() -> None:
    envelope = _ok(None) | {"status": "error", "error": "edge unavailable"}
    assert validate_envelope(envelope) == []


def test_validate_envelope_rejects_missing_request_id() -> None:
    envelope = _ok({})
    del envelope["request_id"]
    assert any("request_id" in v for v in validate_envelope(envelope))


# ---------------------------------------------------------------------------
# Совместимость с конвертом клиента
# ---------------------------------------------------------------------------


def test_client_shaped_envelope_from_http_gateway_validates() -> None:
    """Конверт, который принимает ``HttpContentGateway``, валиден по схеме."""
    payload = {
        "final_url": "https://site.test/final",
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
        "fetched_at": "2026-01-01T00:00:00Z",
        "raw": {"hash": "abc", "size": 3, "bytes_b64": "YWJj"},
        "text": None,
        "error": None,
    }
    envelope = _ok(payload)

    gateway = HttpContentGateway(
        "https://edge.test", transport=lambda op, env: envelope
    )
    assert gateway.call("fetch", {"url": "https://site.test/start"}) == payload
    assert validate_envelope(envelope) == []
    assert validate_payload("fetch", payload) == []


# ---------------------------------------------------------------------------
# validate_payload — fetch
# ---------------------------------------------------------------------------


def test_validate_payload_fetch_accepts_raw_or_text_or_error() -> None:
    assert validate_payload("fetch", {"raw": {"hash": "h", "size": 1}}) == []
    assert validate_payload("fetch", {"text": {"content": "# md"}}) == []
    assert validate_payload("fetch", {"text": None, "error": "gone"}) == []


def test_validate_payload_fetch_rejects_non_mapping() -> None:
    assert validate_payload("fetch", ["nope"])


def test_validate_payload_fetch_rejects_wrong_field_type() -> None:
    violations = validate_payload("fetch", {"status_code": "200"})
    assert any("status_code" in v for v in violations)


def test_validate_payload_fetch_rejects_empty_content() -> None:
    violations = validate_payload("fetch", {"final_url": "https://x.test"})
    assert any("raw" in v and "text" in v for v in violations)


# ---------------------------------------------------------------------------
# validate_fetch_request — запросные оси fetch/extract (D2)
# ---------------------------------------------------------------------------


def test_validate_fetch_request_accepts_two_axes() -> None:
    assert validate_fetch_request({"url": "https://x.test"}) == []
    assert validate_fetch_request(
        {"url": "https://x.test", "fetch": "browser", "extract": "document", "wait_for": "() => 1"}
    ) == []
    assert validate_fetch_request(
        {"url": "https://x.test", "fetch": None, "extract": None, "wait_for": None}
    ) == []


def test_validate_fetch_request_rejects_missing_url() -> None:
    assert any("url" in v for v in validate_fetch_request({"fetch": "http"}))


def test_validate_fetch_request_rejects_unknown_axes() -> None:
    assert validate_fetch_request({"url": "https://x.test", "fetch": "flash"})
    assert validate_fetch_request({"url": "https://x.test", "extract": "pdf"})


def test_validate_fetch_request_rejects_wrong_wait_for_type() -> None:
    assert validate_fetch_request({"url": "https://x.test", "wait_for": 5})


def test_strategy_values_exported() -> None:
    """Ось ``strategy`` объявлена как реэкспортируемая константа ``auto|honest|hybrid``."""
    assert STRATEGY_VALUES == frozenset({"auto", "honest", "hybrid"})


def test_fetch_schema_declares_strategy_axis() -> None:
    """Публикуемая схема запроса содержит ось ``strategy`` (additive, не в required)."""
    schema = load_schema("fetch")
    request = schema["$defs"]["request"]
    assert request["properties"]["strategy"] == {"enum": ["auto", "honest", "hybrid"]}
    assert "strategy" not in request.get("required", [])


@pytest.mark.parametrize("value", ["auto", "honest", "hybrid"])
def test_validate_fetch_request_accepts_strategy(value: str) -> None:
    assert validate_fetch_request({"url": "https://x.test", "strategy": value}) == []


def test_validate_fetch_request_strategy_optional_backward_compatible() -> None:
    """Отсутствие ``strategy`` — валидно (обратная совместимость, поле additive)."""
    assert validate_fetch_request({"url": "https://x.test"}) == []
    assert validate_fetch_request({"url": "https://x.test", "strategy": None}) == []


def test_validate_fetch_request_rejects_unknown_strategy() -> None:
    assert validate_fetch_request({"url": "https://x.test", "strategy": "flash"})


# ---------------------------------------------------------------------------
# crawl — запросы (Phase 3.1)
# ---------------------------------------------------------------------------

_VALID_SUBMIT = {
    "run_id": "r1",
    "session_id": "s1",
    "idempotency_key": "k1",
    "profile": "public",
    "items": [{"product_id": "p1", "url": "https://x.test"}],
}


def test_crawl_ops_exported() -> None:
    assert CRAWL_OPS == frozenset(
        {"crawl/submit", "crawl/status", "crawl/cancel", "crawl/collect", "crawl/poll"}
    )
    assert CRAWL_OPS <= ENDPOINT_OPS


def test_crawl_schema_declares_all_ops() -> None:
    schema = load_schema("crawl")
    defs = schema["$defs"]
    for op in ("submit", "status", "cancel", "collect", "poll"):
        assert f"{op}_request" in defs
        assert f"{op}_response" in defs
    assert schema["$defs"]["submit_response"]["required"] == [
        "job_id",
        "accepted",
        "total",
        "session_id",
    ]
    assert schema["$defs"]["cancel_response"]["properties"]["status"] == {
        "const": "cancelled"
    }
    # crawl/poll (Wave 5.1): ссылки на записи, без байтов (I19).
    assert schema["$defs"]["poll_request"]["required"] == ["url", "kind"]
    assert schema["$defs"]["poll_request"]["properties"]["kind"] == {
        "enum": ["feed", "channel"]
    }
    assert schema["$defs"]["poll_response"]["required"] == [
        "entries",
        "count",
        "truncated",
    ]
    assert schema["$defs"]["poll_entry"]["required"] == ["url"]
    assert {"$ref": "#/$defs/poll_response"} in schema["oneOf"]


def test_validate_crawl_request_submit_accepts() -> None:
    assert validate_crawl_request("crawl/submit", _VALID_SUBMIT) == []
    with_axes = {
        **_VALID_SUBMIT,
        "items": [
            {
                "product_id": "p1",
                "url": "https://x.test",
                "engine": "serp",
                "kind": "html",
                "fetch": "browser",
                "extract": "document",
                "strategy": "hybrid",
                "wait_for": "() => 1",
                "lang": "ru",
            }
        ],
    }
    assert validate_crawl_request("crawl/submit", with_axes) == []


def test_validate_crawl_request_submit_rejects_missing_metadata() -> None:
    violations = validate_crawl_request("crawl/submit", {"items": [{"product_id": "p"}]})
    assert any("run_id" in v for v in violations)
    assert any("session_id" in v for v in violations)
    assert any("idempotency_key" in v for v in violations)
    assert any("profile" in v for v in violations)


def test_validate_crawl_request_submit_rejects_empty_items() -> None:
    assert any(
        "items" in v
        for v in validate_crawl_request("crawl/submit", {**_VALID_SUBMIT, "items": []})
    )


def test_validate_crawl_request_submit_rejects_item_without_url() -> None:
    violations = validate_crawl_request(
        "crawl/submit", {**_VALID_SUBMIT, "items": [{"product_id": "p1"}]}
    )
    assert any("url" in v for v in violations)


def test_validate_crawl_request_submit_rejects_unknown_item_axis() -> None:
    violations = validate_crawl_request(
        "crawl/submit",
        {**_VALID_SUBMIT, "items": [{"product_id": "p1", "url": "u", "fetch": "flash"}]},
    )
    assert any("fetch" in v for v in violations)


@pytest.mark.parametrize("op", ["crawl/status", "crawl/cancel"])
def test_validate_crawl_request_job_ops_require_job_id(op: str) -> None:
    assert validate_crawl_request(op, {"job_id": "j1"}) == []
    assert any("job_id" in v for v in validate_crawl_request(op, {}))


def test_validate_crawl_request_collect_optional_fields() -> None:
    assert validate_crawl_request("crawl/collect", {"job_id": "j1"}) == []
    assert validate_crawl_request(
        "crawl/collect", {"job_id": "j1", "cursor": "c", "limit": 5}
    ) == []
    assert any(
        "limit" in v
        for v in validate_crawl_request("crawl/collect", {"job_id": "j1", "limit": "x"})
    )


def test_validate_crawl_request_unknown_op() -> None:
    violations = validate_crawl_request("crawl/teleport", {})
    assert violations and "crawl/teleport" in violations[0]


# ---------------------------------------------------------------------------
# crawl — payload ответов (Phase 3.1)
# ---------------------------------------------------------------------------


def test_validate_payload_crawl_submit_accepts_and_requires() -> None:
    assert validate_payload(
        "crawl/submit",
        {"job_id": "j1", "accepted": True, "total": 2, "session_id": "s1"},
    ) == []
    assert validate_payload("crawl/submit", {"accepted": True})


def test_validate_payload_crawl_status_accepts() -> None:
    assert validate_payload(
        "crawl/status",
        {
            "job_id": "j1",
            "status": "running",
            "total": 3,
            "pending": 1,
            "running": 1,
            "fetched": 1,
            "failed": 0,
            "cancelled": 0,
            "engines": ["http"],
            "kinds": ["html"],
        },
    ) == []
    assert validate_payload("crawl/status", {"job_id": "j1", "engines": [1]})


def test_validate_payload_crawl_cancel_requires_cancelled_status() -> None:
    assert validate_payload(
        "crawl/cancel", {"job_id": "j1", "status": "cancelled", "cancelled": 2}
    ) == []
    assert validate_payload(
        "crawl/cancel", {"job_id": "j1", "status": "running", "cancelled": 0}
    )


def test_validate_payload_crawl_collect_accepts_items_and_raw() -> None:
    payload = {
        "items": [
            {
                "product_id": "p1",
                "url": "https://x.test",
                "status": "fetched",
                "engine": "http",
                "kind": "html",
                "http_status": 200,
                "content_type": "text/html",
                "fetched_at": "T",
                "content_hash": "h",
                "raw": {"hash": "h", "size": 2, "bytes_b64": "YQ=="},
                "error": None,
            }
        ],
        "next_cursor": None,
        "done": True,
    }
    assert validate_payload("crawl/collect", payload) == []


def test_validate_payload_crawl_collect_rejects_bad_item() -> None:
    assert validate_payload("crawl/collect", {"items": [{"product_id": "p1"}]})
    assert validate_payload("crawl/collect", {"items": "nope"})
    assert validate_payload(
        "crawl/collect",
        {"items": [], "next_cursor": None, "done": "yes"},
    )


# ---------------------------------------------------------------------------
# crawl/poll — запрос и ответ (Wave 5.1)
# ---------------------------------------------------------------------------


def test_validate_crawl_request_poll_accepts() -> None:
    assert validate_crawl_request(
        "crawl/poll", {"url": "https://x.test/feed", "kind": "feed"}
    ) == []
    assert validate_crawl_request(
        "crawl/poll",
        {
            "url": "https://x.test/feed",
            "kind": "channel",
            "params": {"since": "2026-01-01", "limit": 10},
            "fetch": "http",
            "extract": "auto",
            "strategy": "honest",
            "wait_for": None,
        },
    ) == []


def test_validate_crawl_request_poll_requires_url_and_kind() -> None:
    assert any("url" in v for v in validate_crawl_request("crawl/poll", {"kind": "feed"}))
    assert any(
        "kind" in v for v in validate_crawl_request("crawl/poll", {"url": "https://x.test"})
    )


def test_validate_crawl_request_poll_rejects_unknown_kind() -> None:
    assert validate_crawl_request(
        "crawl/poll", {"url": "https://x.test", "kind": "sitemap"}
    )


def test_validate_crawl_request_poll_rejects_bad_params() -> None:
    assert any(
        "limit" in v
        for v in validate_crawl_request(
            "crawl/poll", {"url": "https://x.test", "kind": "feed", "params": {"limit": "x"}}
        )
    )
    assert any(
        "params" in v
        for v in validate_crawl_request(
            "crawl/poll", {"url": "https://x.test", "kind": "feed", "params": "nope"}
        )
    )


def test_validate_crawl_request_poll_rejects_unknown_axis() -> None:
    assert any(
        "strategy" in v
        for v in validate_crawl_request(
            "crawl/poll", {"url": "https://x.test", "kind": "feed", "strategy": "flash"}
        )
    )


def test_validate_payload_crawl_poll_accepts_entries() -> None:
    payload = {
        "entries": [
            {
                "url": "https://x.test/a",
                "guid": "g1",
                "published_at": "2026-01-01",
                "title": "A",
            },
            {"url": "https://x.test/b", "guid": None, "published_at": None, "title": None},
        ],
        "count": 2,
        "truncated": False,
    }
    assert validate_payload("crawl/poll", payload) == []
    assert validate_payload("crawl/poll", {"entries": [], "count": 0, "truncated": True}) == []


def test_validate_payload_crawl_poll_requires_entry_url() -> None:
    assert validate_payload(
        "crawl/poll", {"entries": [{"title": "no url"}], "count": 1, "truncated": False}
    )
    assert validate_payload("crawl/poll", {"entries": "nope", "count": 0, "truncated": False})


def test_validate_payload_crawl_poll_rejects_bad_types() -> None:
    assert validate_payload(
        "crawl/poll", {"entries": [], "count": "2", "truncated": False}
    )
    assert validate_payload(
        "crawl/poll", {"entries": [], "count": 0, "truncated": "no"}
    )
    assert validate_payload(
        "crawl/poll", {"entries": [{"url": 5}], "count": 1, "truncated": False}
    )


# ---------------------------------------------------------------------------
# validate_payload — search
# ---------------------------------------------------------------------------


def test_validate_payload_search_accepts_candidates() -> None:
    payload = [
        {"url": "https://a.test", "title": "A", "snippet": "sa", "rank": 1},
        {"url": "https://b.test", "title": "B", "snippet": "sb", "rank": 2},
    ]
    assert validate_payload("search", payload) == []


def test_validate_payload_search_accepts_results_wrapper() -> None:
    assert validate_payload("search", {"results": [{"url": "https://a.test"}]}) == []


def test_validate_payload_search_rejects_missing_url() -> None:
    violations = validate_payload("search", [{"title": "no url", "rank": 1}])
    assert any("url" in v for v in violations)


def test_validate_payload_search_rejects_non_list() -> None:
    assert validate_payload("search", {"candidates": []})


# ---------------------------------------------------------------------------
# validate_payload — registry
# ---------------------------------------------------------------------------


def test_validate_payload_registry_accepts_facts() -> None:
    payload = [
        {
            "fact_json": '{"name": "a"}',
            "source_url": "https://registry.test/a",
            "fetched_at": "2026-01-01T00:00:00Z",
        }
    ]
    assert validate_payload("registry", payload) == []


def test_validate_payload_registry_accepts_dict_fact_json() -> None:
    payload = [{"fact_json": {"name": "a"}, "source_url": "https://registry.test/a"}]
    assert validate_payload("registry", payload) == []


def test_validate_payload_registry_rejects_missing_source_url() -> None:
    violations = validate_payload("registry", [{"fact_json": "{}"}])
    assert any("source_url" in v for v in violations)


def test_validate_payload_registry_rejects_empty_source_url() -> None:
    violations = validate_payload("registry", [{"source_url": ""}])
    assert any("source_url" in v for v in violations)


# ---------------------------------------------------------------------------
# validate_payload — capabilities / health
# ---------------------------------------------------------------------------


def test_validate_payload_capabilities_accepts() -> None:
    payload = {
        "extractor_version": "trafilatura-html-1",
        "crawler_version": "scrapy-1",
        "search_engine": "serp-x",
        "supported_formats": ["html", "pdf"],
    }
    assert validate_payload("capabilities", payload) == []


def test_validate_payload_capabilities_rejects_bad_formats() -> None:
    violations = validate_payload("capabilities", {"supported_formats": "html"})
    assert any("supported_formats" in v for v in violations)


def test_validate_payload_health_accepts_and_requires_status() -> None:
    assert validate_payload("health", {"status": "ok"}) == []
    assert validate_payload("health", {})
    assert validate_payload("health", {"status": 1})


def test_validate_payload_unknown_op() -> None:
    violations = validate_payload("teleport", {})
    assert violations and "teleport" in violations[0]


# ---------------------------------------------------------------------------
# contract_digest и OpenAPI
# ---------------------------------------------------------------------------


def test_contract_digest_is_deterministic() -> None:
    first = contract_digest()
    second = contract_digest()
    assert first == second
    assert first.startswith("sha256:")
    assert len(first) == len("sha256:") + 64


def test_openapi_names_every_endpoint() -> None:
    openapi = load_schema("openapi")
    assert openapi["openapi"].startswith("3.1")
    assert openapi["info"]["version"] == SEAM_SCHEMA_VERSION
    paths = openapi["paths"]
    for op in ENDPOINT_OPS:
        path = f"/v1/{op}"
        assert path in paths, path
    assert set(paths) == {f"/v1/{op}" for op in ENDPOINT_OPS}
    # Методы: POST для контентных операций, GET для диагностики.
    assert "post" in paths["/v1/fetch"] and "post" in paths["/v1/search"]
    assert "post" in paths["/v1/registry"]
    assert "get" in paths["/v1/capabilities"] and "get" in paths["/v1/health"]


def test_openapi_response_uses_envelope() -> None:
    openapi = load_schema("openapi")
    assert "Envelope" in openapi["components"]["schemas"]
    envelope_ref = openapi["components"]["schemas"]["Envelope"]["$ref"]
    assert envelope_ref == "envelope.schema.json"
    assert (seam_schema.SCHEMA_DIR / envelope_ref).is_file()


def test_contracts_are_category_neutral() -> None:
    import re

    forbidden = re.compile(
        r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE
    )
    for name in _SCHEMA_FILES:
        text = (seam_schema.SCHEMA_DIR / name).read_text(encoding="utf-8")
        assert not forbidden.search(text), name


