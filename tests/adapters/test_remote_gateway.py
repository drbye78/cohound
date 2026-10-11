"""Тесты remote-прокси и HTTP-транспорта DMZ-шва (W13/T13.3; 07 §3–§5).

Сеть отсутствует: везде используется внедрённый ``transport`` — callable
``(op, envelope) -> envelope``. Проверяются: конверт (ok/error/schema-несовпадение),
лимит размера, ``request_id``, отсутствие пиннинга; формы результатов, идентичные
реальным контрактам (RawPage/ExtractedDoc/SearchResult/RegistryFact); R1
(``source_url``); декодирование base64/``text.content``; повторный хеш (I19);
структурное соответствие портам. Фикстуры категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import base64
import importlib
import inspect
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

import core.adapters.remote_gateway as remote_gateway_module
from core.adapters.remote_crawler import RemoteCrawler
from core.adapters.remote_extractor import RemoteExtractor
from core.adapters.remote_gateway import (
    GatewayPayloadError,
    GatewaySchemaError,
    GatewaySizeError,
    GatewayStatusError,
    GatewayTransportError,
    HttpContentGateway,
    RemoteContentError,
)
from core.adapters.remote_registry import RemoteRegistry
from core.adapters.remote_search import RemoteSearch
from core.domain import RegistryFact
from core.ports.crawler import CrawlerPort
from core.ports.extractor import ExtractorPort
from core.ports.gateway import ContentGateway
from core.ports.registry import RegistryFetcherPort
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.types import ExtractedDoc, FetchMode, FetchPlan, ProductRef, RawPage
from core.util.hashing import content_hash

_REQUEST_ID = "0" * 32


# ---------------------------------------------------------------------------
# Вспомогательные фикстуры (нейтральные)
# ---------------------------------------------------------------------------


def _envelope(
    payload: Any,
    *,
    schema: str = "1",
    status: str = "ok",
    error: str | None = None,
    request_id: str = _REQUEST_ID,
) -> dict[str, Any]:
    """Конверт ответа edge (07 §3)."""
    return {
        "schema_version": schema,
        "request_id": request_id,
        "status": status,
        "payload": payload,
        "error": error,
    }


def _gw(respond: Any, **kwargs: Any) -> HttpContentGateway:
    """Gateway с швом: ``respond(op, payload) -> response payload``."""

    def transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        return _envelope(respond(op, envelope["payload"]))

    return HttpContentGateway("https://edge.test", transport=transport, **kwargs)


# ---------------------------------------------------------------------------
# HttpContentGateway — конверт
# ---------------------------------------------------------------------------


def test_gateway_ok_returns_validated_payload() -> None:
    gateway = HttpContentGateway(
        "https://edge.test", transport=lambda op, env: _envelope({"ok": True})
    )
    assert gateway.call("fetch", {"url": "https://x.test"}) == {"ok": True}
    assert isinstance(gateway, ContentGateway)


def test_gateway_sends_envelope_with_request_id() -> None:
    envelopes: list[Mapping[str, Any]] = []

    def transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        envelopes.append(envelope)
        return _envelope({"n": len(envelopes)})

    gateway = HttpContentGateway("https://edge.test", transport=transport)
    gateway.call("fetch", {"url": "u"})
    gateway.call("fetch", {"url": "u"})

    first, second = envelopes
    # Конверт запроса: версия, статус-запрос, payload, request_id (07 §3/§8).
    assert first["schema_version"] == "1"
    assert first["status"] == "request"
    assert first["payload"] == {"url": "u"}
    assert re.fullmatch(r"[0-9a-f]{32}", str(first["request_id"]))
    # request_id уникален на вызов (корреляция, не идемпотентность).
    assert first["request_id"] != second["request_id"]
    assert gateway.last_request_id == second["request_id"]


def test_gateway_schema_mismatch_raises() -> None:
    gateway = HttpContentGateway(
        "https://edge.test",
        schema_version="2",
        transport=lambda op, env: _envelope({}, schema="9"),
    )
    with pytest.raises(GatewaySchemaError):
        gateway.call("fetch", {})


def test_gateway_status_error_raises_with_error_text() -> None:
    gateway = HttpContentGateway(
        "https://edge.test",
        transport=lambda op, env: _envelope(None, status="error", error="edge boom"),
    )
    with pytest.raises(GatewayStatusError) as excinfo:
        gateway.call("fetch", {})
    assert "edge boom" in str(excinfo.value)


def test_gateway_missing_payload_raises() -> None:
    envelope = {
        "schema_version": "1",
        "request_id": _REQUEST_ID,
        "status": "ok",
        "error": None,
    }
    gateway = HttpContentGateway("https://edge.test", transport=lambda op, env: envelope)
    with pytest.raises(GatewayPayloadError):
        gateway.call("fetch", {})


def test_gateway_none_payload_raises() -> None:
    gateway = HttpContentGateway(
        "https://edge.test", transport=lambda op, env: _envelope(None)
    )
    with pytest.raises(GatewayPayloadError):
        gateway.call("fetch", {})


def test_gateway_non_mapping_response_raises() -> None:
    def transport(op: str, env: Mapping[str, Any]) -> Any:
        return [1, 2, 3]

    gateway = HttpContentGateway("https://edge.test", transport=transport)
    with pytest.raises(GatewayPayloadError):
        gateway.call("fetch", {})


def test_gateway_oversized_response_rejected() -> None:
    gateway = HttpContentGateway(
        "https://edge.test",
        max_response_bytes=128,
        transport=lambda op, env: _envelope({"blob": "a" * 4096}),
    )
    with pytest.raises(GatewaySizeError):
        gateway.call("fetch", {})


def test_gateway_capabilities_returns_mapping() -> None:
    gateway = HttpContentGateway(
        "https://edge.test",
        transport=lambda op, env: _envelope({"crawler_version": "c1"}),
    )
    assert gateway.capabilities() == {"crawler_version": "c1"}


def test_gateway_retries_transport_failure_then_succeeds() -> None:
    envelopes: list[Mapping[str, Any]] = []

    def transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        envelopes.append(envelope)
        if len(envelopes) < 3:
            raise OSError("network down")
        return _envelope({"ok": True})

    gateway = HttpContentGateway("https://edge.test", max_retries=2, transport=transport)
    assert gateway.call("fetch", {}) == {"ok": True}
    assert len(envelopes) == 3
    # Корреляция сохраняется между повторами (один request_id на вызов).
    assert len({str(e["request_id"]) for e in envelopes}) == 1


def test_gateway_retry_exhausted_raises_transport_error() -> None:
    attempts: list[int] = []

    def transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        attempts.append(1)
        raise OSError("down")

    gateway = HttpContentGateway("https://edge.test", max_retries=2, transport=transport)
    with pytest.raises(GatewayTransportError):
        gateway.call("fetch", {})
    assert len(attempts) == 3  # 1 попытка + 2 повтора


def test_gateway_available_with_injected_transport() -> None:
    gateway = HttpContentGateway("https://edge.test", transport=lambda op, env: _envelope({}))
    assert gateway.available is True


def test_gateway_has_no_certificate_pinning_and_configurable_ca() -> None:
    source = Path(remote_gateway_module.__file__).read_text(encoding="utf-8")
    # Пиннинг/жёсткая фиксация сертификата не допускаются (07 §8.1, §9).
    assert "verify=False" not in source
    assert "CERT_NONE" not in source
    assert "pinning" not in source
    assert "fingerprint" not in source
    # Доверие к корпоративному CA (в т.ч. TLS-инспекция) — настраиваемый параметр.
    assert "ca" in inspect.signature(HttpContentGateway.__init__).parameters


# ---------------------------------------------------------------------------
# RemoteCrawler
# ---------------------------------------------------------------------------


def test_remote_crawler_decodes_base64_and_rehashes() -> None:
    content = b"<html>neutral</html>"
    seen: dict[str, Any] = {}

    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        seen["op"] = op
        seen["payload"] = payload
        return {
            "final_url": "https://site.test/final",
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "fetched_at": "2026-01-01T00:00:00Z",
            "raw": {
                "hash": content_hash(content),
                "size": len(content),
                "bytes_b64": base64.b64encode(content).decode("ascii"),
            },
            "text": None,
            "error": None,
        }

    crawler = RemoteCrawler(_gw(respond))
    page = crawler.fetch(
        "https://site.test/start", FetchPlan(fetch=FetchMode.BROWSER)
    )

    assert isinstance(page, RawPage)
    assert page.url == "https://site.test/final"
    assert page.content == content
    assert page.status_code == 200
    assert page.content_type == "text/html; charset=utf-8"
    assert page.fetch_mode is FetchMode.BROWSER
    # Запрос: /v1/fetch с осями fetch=browser/extract=auto (сырые байты — retention).
    assert seen["op"] == "fetch"
    assert seen["payload"]["url"] == "https://site.test/start"
    assert seen["payload"]["fetch"] == "browser"
    assert seen["payload"]["extract"] == "auto"


def test_remote_crawler_rejects_hash_mismatch() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {
            "raw": {
                "hash": content_hash(b"other"),
                "bytes_b64": base64.b64encode(b"actual").decode("ascii"),
            }
        }

    with pytest.raises(RemoteContentError):
        RemoteCrawler(_gw(respond)).fetch("https://site.test/x")


def test_remote_crawler_accepts_raw_content_field() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {"raw": {"content": "plain bytes"}, "fetched_at": "T"}

    page = RemoteCrawler(_gw(respond)).fetch("https://site.test/x")
    assert page.content == b"plain bytes"


def test_remote_crawler_rejects_broken_base64() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {"raw": {"bytes_b64": "!!!not-base64!!!"}}

    with pytest.raises(RemoteContentError):
        RemoteCrawler(_gw(respond)).fetch("https://site.test/x")


def test_remote_crawler_missing_content_raises() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {"raw": {"hash": "x", "ref": "edge://blob/1"}, "text": None, "error": "gone"}

    with pytest.raises(RemoteContentError):
        RemoteCrawler(_gw(respond)).fetch("https://site.test/x")


def test_remote_crawler_text_content_fallback() -> None:
    text = "cleaned markdown"

    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {"text": {"content": text}, "fetched_at": "T"}

    page = RemoteCrawler(_gw(respond)).fetch("https://site.test/x")
    assert page.content == text.encode("utf-8")


def test_remote_crawler_conforms_to_port() -> None:
    crawler = RemoteCrawler(_gw(lambda op, payload: {"raw": {"content": "x"}}))
    assert isinstance(crawler, CrawlerPort)
    assert crawler.version


# ---------------------------------------------------------------------------
# RemoteSearch
# ---------------------------------------------------------------------------


def test_remote_search_returns_results_without_evidence() -> None:
    seen: dict[str, Any] = {}

    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        seen["op"] = op
        seen["payload"] = payload
        return [
            {"url": "https://a.test", "title": "A", "snippet": "sa", "rank": 1},
            {"url": "https://b.test", "title": "B", "snippet": "sb", "rank": 2},
        ]

    search = RemoteSearch(_gw(respond), adapter_kind="serp", provider="edge")
    results = search.search(SearchQuery(text="q", lang="ru", limit=5, phase="corpus"))

    assert [r.url for r in results] == ["https://a.test", "https://b.test"]
    assert [r.rank for r in results] == [1, 2]
    assert all(isinstance(r, SearchResult) for r in results)
    # I19: кандидат, не evidence — сырых байтов нет.
    assert all(r.raw_bytes is None for r in results)
    assert seen["op"] == "search"
    assert seen["payload"] == {"query": "q", "lang": "ru", "limit": 5, "phase": "corpus"}
    assert search.adapter_kind == "serp"
    assert search.provider == "edge"
    assert isinstance(search, SearchAdapterPort)


def test_remote_search_fetch_returns_raw_page() -> None:
    content = b"page bytes"

    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        assert op == "fetch"
        assert payload["extract"] == "auto"
        return {"raw": {"content": content.decode()}, "fetched_at": "T"}

    page = RemoteSearch(_gw(respond)).fetch("https://a.test")
    assert isinstance(page, RawPage)
    assert page.content == content


def test_remote_search_result_without_url_raises() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return [{"title": "no url", "rank": 1}]

    with pytest.raises(RemoteContentError):
        RemoteSearch(_gw(respond)).search(SearchQuery(text="q"))


def test_remote_search_accepts_results_wrapper() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {"results": [{"url": "https://a.test", "rank": 3}]}

    results = RemoteSearch(_gw(respond)).search(SearchQuery(text="q"))
    assert [r.url for r in results] == ["https://a.test"]


# ---------------------------------------------------------------------------
# RemoteRegistry
# ---------------------------------------------------------------------------


def _product() -> ProductRef:
    return ProductRef(product_id="prod-a", canonical_name="Name", vendor="Vendor")


def test_remote_registry_returns_facts_with_source_url() -> None:
    seen: dict[str, Any] = {}

    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        seen["op"] = op
        seen["payload"] = payload
        return [
            {
                "fact_json": json.dumps({"name": "a"}),
                "source_url": "https://registry.test/a",
                "fetched_at": "2026-01-01T00:00:00Z",
            }
        ]

    registry = RemoteRegistry(_gw(respond), registry_id="registry-x")
    facts = registry.fetch(_product(), {"run_id": "run-1"})

    assert len(facts) == 1
    fact = facts[0]
    assert isinstance(fact, RegistryFact)
    assert fact.source_url == "https://registry.test/a"  # R1
    assert fact.run_id == "run-1"
    assert fact.product_id == "prod-a"
    assert fact.registry_id == "registry-x"
    assert json.loads(fact.fact_json) == {"name": "a"}
    assert seen["op"] == "registry"
    assert seen["payload"]["registry_id"] == "registry-x"
    assert seen["payload"]["product_ref"]["product_id"] == "prod-a"
    assert seen["payload"]["scope"] == {"run_id": "run-1"}
    assert registry.registry_id == "registry-x"
    assert isinstance(registry, RegistryFetcherPort)


def test_remote_registry_serializes_dict_fact_json() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return [{"fact_json": {"name": "a"}, "source_url": "https://registry.test/a"}]

    facts = RemoteRegistry(_gw(respond), registry_id="r").fetch(_product(), {})
    assert json.loads(facts[0].fact_json) == {"name": "a"}


def test_remote_registry_requires_source_url() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return [{"fact_json": "{}"}]

    with pytest.raises(RemoteContentError):
        RemoteRegistry(_gw(respond), registry_id="r").fetch(_product(), {})


# ---------------------------------------------------------------------------
# RemoteExtractor
# ---------------------------------------------------------------------------


def test_remote_extractor_returns_doc_and_rehashes() -> None:
    text = "# clean markdown"
    seen: dict[str, Any] = {}

    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        seen["op"] = op
        seen["payload"] = payload
        return {
            "final_url": "https://doc.test/x",
            "raw": {"hash": "ignored", "bytes_b64": base64.b64encode(b"raw").decode()},
            "text": {
                "content": text,
                "hash": content_hash(text),
                "extractor_version": "trafilatura-html-1",
            },
            "error": None,
        }

    extractor = RemoteExtractor(_gw(respond))
    page = RawPage(url="https://doc.test/x", content=b"raw", fetched_at="T")
    doc = extractor.extract(page)

    assert isinstance(doc, ExtractedDoc)
    assert doc.text == text
    assert doc.content_hash == content_hash(text)  # пересчитан ядром (I19)
    assert doc.extractor_version == "trafilatura-html-1"
    assert doc.error is None
    # 07 §3: /v1/fetch с осью extract (auto по умолчанию).
    assert seen["op"] == "fetch"
    assert seen["payload"]["url"] == "https://doc.test/x"
    assert seen["payload"]["extract"] == "auto"
    assert isinstance(extractor, ExtractorPort)
    assert extractor.version


def test_remote_extractor_rejects_hash_mismatch() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {
            "text": {
                "content": "actual",
                "hash": content_hash("tampered"),
                "extractor_version": "e1",
            }
        }

    page = RawPage(url="https://doc.test/x", content=b"raw", fetched_at="T")
    with pytest.raises(RemoteContentError):
        RemoteExtractor(_gw(respond)).extract(page)


def test_remote_extractor_graceful_degradation() -> None:
    def respond(op: str, payload: Mapping[str, Any]) -> Any:
        return {"text": None, "error": "parse failed"}

    page = RawPage(url="https://doc.test/x", content=b"raw", fetched_at="T")
    doc = RemoteExtractor(_gw(respond), version="proxy-1").extract(page)
    assert doc.text == ""
    assert doc.error == "parse failed"
    assert doc.extractor_version == "proxy-1"  # fallback-версия прокси


# ---------------------------------------------------------------------------
# Нейтральность (§1.3)
# ---------------------------------------------------------------------------

_NEUTRAL_MODULES = [
    "core.adapters.remote_gateway",
    "core.adapters.remote_crawler",
    "core.adapters.remote_search",
    "core.adapters.remote_registry",
    "core.adapters.remote_extractor",
]


@pytest.mark.parametrize("modname", _NEUTRAL_MODULES)
def test_module_source_is_category_neutral(modname: str) -> None:
    module = importlib.import_module(modname)
    assert module.__file__ is not None
    source = Path(module.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    hits = [line for line in source.splitlines() if forbidden.search(line)]
    assert hits == []
