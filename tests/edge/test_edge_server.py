"""Тесты gateway-сервера DMZ — ``core/edge/server.py`` (W13/T13.2; 07 §3–§5).

Сеть отсутствует: ``EdgeGateway`` тестируется на инъецированных fake-адаптерах,
HTTP-слой (``route_request``) — как чистая функция, ``serve`` — с внедрённой
фабрикой сервера (без сокета). Самый сильный тест — зеркальный round-trip:
клиентские remote-прокси (``HttpContentGateway``) проходят через
``EdgeGateway.envelope``, доказывая совместимость конверта client↔server.

Проверяется: формы всех эндпоинтов (07 §3); ``extract`` true/false; error-путь
(адаптер бросает/возвращает ``.error``) → ``status=error`` без traceback;
изоляция (в исходнике ``core/edge/server.py`` нет импортов store/domain);
нейтральность (§1.3). Фикстуры категорийно-нейтральны.
"""

from __future__ import annotations

import ast
import base64
import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

import pytest

import core.edge.server as edge_server_module
from core.adapters.remote_crawler import RemoteCrawler
from core.adapters.remote_extractor import RemoteExtractor
from core.adapters.remote_gateway import HttpContentGateway
from core.adapters.remote_registry import RemoteRegistry
from core.adapters.remote_search import RemoteSearch
from core.edge.server import (
    EDGE_SCHEMA_VERSION,
    EdgeGateway,
    build_server,
    make_handler,
    route_request,
    serve,
)
from core.ports.search import SearchQuery, SearchResult
from core.ports.types import (
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    ProductRef,
    RawPage,
)
from core.util.hashing import content_hash

_FIXED_ID = "0" * 32
_FIXED_TIME = "2026-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Fake-адаптеры (нейтральные; швы без сети)
# ---------------------------------------------------------------------------


class FakeCrawler:
    """Стенд-краулер: возвращает детерминированный ``RawPage`` или бросает."""

    version = "fake-crawler-1"

    def __init__(
        self,
        content: bytes = b"<html>body</html>",
        *,
        raises: Exception | None = None,
    ) -> None:
        self._content = content
        self._raises = raises
        self.calls: list[tuple[str, FetchMode]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append((url, FetchMode(plan.fetch)))
        if self._raises is not None:
            raise self._raises
        return RawPage(
            url=url,
            content=self._content,
            fetched_at=_FIXED_TIME,
            fetch_mode=FetchMode(plan.fetch),
            status_code=200,
            content_type="text/html; charset=utf-8",
        )


class FakeExtractor:
    """Стенд-экстрактор: успех, результат с ``.error`` или исключение."""

    version = "fake-extractor-1"

    def __init__(
        self,
        text: str = "# clean markdown",
        *,
        result_error: str | None = None,
        raises: Exception | None = None,
    ) -> None:
        self._text = text
        self._result_error = result_error
        self._raises = raises
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        if self._result_error is not None:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(""),
                extractor_version=self.version,
                error=self._result_error,
            )
        return ExtractedDoc(
            text=self._text,
            content_hash=content_hash(self._text),
            extractor_version=self.version,
        )


class FakeSearcher:
    """Стенд-SERP: список кандидатов или исключение."""

    adapter_kind = "serp"
    provider = "fake"

    def __init__(
        self,
        results: list[SearchResult] | None = None,
        *,
        raises: Exception | None = None,
    ) -> None:
        self._results = (
            list(results)
            if results is not None
            else [SearchResult(url="https://a.test", title="A", snippet="sa", rank=1)]
        )
        self._raises = raises
        self.last_query: SearchQuery | None = None

    def search(self, q: SearchQuery) -> list[SearchResult]:
        if self._raises is not None:
            raise self._raises
        self.last_query = q
        return list(self._results)

    def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется сервером
        raise NotImplementedError


@dataclass
class FakeFact:
    """Дак-тип реестрового факта (сервер читает атрибуты, домен не нужен)."""

    fact_json: str
    source_url: str
    fetched_at: str | None = _FIXED_TIME


class FakeRegistry:
    """Стенд-реестр: возвращает факты; помнит запрос (R1)."""

    def __init__(
        self,
        registry_id: str = "registry-x",
        facts: list[FakeFact] | None = None,
        *,
        raises: Exception | None = None,
    ) -> None:
        self.registry_id = registry_id
        self._facts = (
            list(facts)
            if facts is not None
            else [FakeFact(json.dumps({"k": "v"}), "https://registry.test/a")]
        )
        self._raises = raises
        self.seen: list[tuple[ProductRef, dict[str, Any]]] = []

    def fetch(self, product: ProductRef, scope: dict[str, Any]) -> list[FakeFact]:
        if self._raises is not None:
            raise self._raises
        self.seen.append((product, dict(scope)))
        return list(self._facts)


class FakeScheduler:
    """Стенд-планировщик краула (Phase 3.1): детерминированные ответы без фоновых задач.

    Реальный планировщик — Wave 3.2; здесь только форма контракта ``/v1/crawl/*``.
    """

    def __init__(self, *, raises: Exception | None = None) -> None:
        self._raises = raises
        self.submitted: list[dict[str, Any]] = []
        self.status_calls: list[str] = []
        self.cancel_calls: list[str] = []
        self.collect_calls: list[tuple[str, Any, Any]] = []

    def submit(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if self._raises is not None:
            raise self._raises
        self.submitted.append(dict(request))
        return {
            "job_id": "job-1",
            "accepted": True,
            "total": len(list(request.get("items") or [])),
            "session_id": request.get("session_id"),
        }

    def status(self, job_id: str) -> dict[str, Any]:
        if self._raises is not None:
            raise self._raises
        self.status_calls.append(job_id)
        return {
            "job_id": job_id,
            "status": "running",
            "total": 3,
            "pending": 1,
            "running": 1,
            "fetched": 1,
            "failed": 0,
            "cancelled": 0,
            "engines": ["http"],
            "kinds": ["html"],
        }

    def cancel(self, job_id: str) -> dict[str, Any]:
        if self._raises is not None:
            raise self._raises
        self.cancel_calls.append(job_id)
        return {"job_id": job_id, "cancelled": 2}

    def collect(
        self, job_id: str, *, cursor: Any = None, limit: Any = None
    ) -> dict[str, Any]:
        if self._raises is not None:
            raise self._raises
        self.collect_calls.append((job_id, cursor, limit))
        return {
            "items": [
                {
                    "product_id": "p1",
                    "url": "https://site.test/x",
                    "status": "fetched",
                    "engine": "http",
                    "kind": "html",
                    "http_status": 200,
                    "content_type": "text/html",
                    "fetched_at": _FIXED_TIME,
                    "content_hash": "h",
                    "raw": {"hash": "h", "size": 2, "bytes_b64": "YQ=="},
                    "error": None,
                }
            ],
            "next_cursor": None,
            "done": True,
        }


class FakePoller:
    """Стенд-сервис опроса фидов/каналов (Wave 5.1): детерминированные ссылки."""

    def __init__(self, *, raises: Exception | None = None) -> None:
        self._raises = raises
        self.calls: list[dict[str, Any]] = []

    def poll(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if self._raises is not None:
            raise self._raises
        self.calls.append(dict(request))
        return {
            "entries": [
                {
                    "url": "https://site.test/a",
                    "guid": "g1",
                    "published_at": "2026-01-01",
                    "title": "A",
                },
                {"url": "https://site.test/b", "guid": None, "published_at": None, "title": None},
            ],
            "count": 2,
            "truncated": False,
        }


# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------


def _edge(**overrides: Any) -> EdgeGateway:
    """EdgeGateway с детерминированными часами/id и всеми fake-адаптерами."""
    crawler = overrides.pop("crawler", FakeCrawler())
    extractor = overrides.pop("extractor", FakeExtractor())
    searchers = overrides.pop("searchers", FakeSearcher())
    registries = overrides.pop("registries", FakeRegistry())
    return EdgeGateway(
        crawler,
        extractor,
        searchers=searchers,
        registries=registries,
        clock=lambda: _FIXED_TIME,
        id_factory=lambda: _FIXED_ID,
        **overrides,
    )


def _client(edge: EdgeGateway) -> HttpContentGateway:
    """Клиентский gateway, чей транспорт зеркалит сервер (эхо ``request_id``)."""

    def transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        return edge.envelope(op, envelope["payload"], request_id=envelope["request_id"])

    return HttpContentGateway("https://edge.test", transport=transport)


# ---------------------------------------------------------------------------
# health / capabilities
# ---------------------------------------------------------------------------


def test_health_endpoint_shape() -> None:
    env = _edge().envelope("health", {})
    assert env["schema_version"] == EDGE_SCHEMA_VERSION
    assert env["request_id"] == _FIXED_ID
    assert env["status"] == "ok"
    assert env["payload"] == {"status": "ok"}
    assert env["error"] is None


def test_capabilities_endpoint_shape() -> None:
    edge = _edge(
        supported_formats=["html", "pdf"],
        search_engine="fake",
        extractor_version="fake-extractor-1",
    )
    caps = edge.capabilities()
    assert caps == {
        "extractor_version": "fake-extractor-1",
        "crawler_version": "fake-crawler-1",
        "search_engine": "fake",
        "supported_formats": ["html", "pdf"],
    }
    env = edge.envelope("capabilities", {})
    assert env["status"] == "ok"
    assert env["payload"] == caps


def test_capabilities_derives_engine_and_formats() -> None:
    edge = _edge()
    caps = edge.capabilities()
    assert caps["search_engine"] == "fake"
    assert caps["extractor_version"] == "fake-extractor-1"
    assert caps["crawler_version"] == "fake-crawler-1"
    assert caps["supported_formats"] == ["html"]


# ---------------------------------------------------------------------------
# /v1/fetch
# ---------------------------------------------------------------------------


def test_fetch_returns_raw_and_extracted_text() -> None:
    content = b"<html>neutral bytes</html>"
    crawler = FakeCrawler(content)
    extractor = FakeExtractor()
    env = _edge(crawler=crawler, extractor=extractor).envelope(
        "fetch", {"url": "https://site.test/x", "fetch": "http", "extract": "auto"}
    )

    assert env["status"] == "ok"
    payload = env["payload"]
    assert set(payload) == {
        "request_id",
        "final_url",
        "status_code",
        "content_type",
        "fetched_at",
        "raw",
        "text",
        "error",
    }
    assert payload["request_id"] == env["request_id"]
    assert payload["final_url"] == "https://site.test/x"
    assert payload["status_code"] == 200
    assert payload["content_type"] == "text/html; charset=utf-8"
    assert payload["fetched_at"] == _FIXED_TIME
    assert payload["raw"]["hash"] == content_hash(content)
    assert payload["raw"]["size"] == len(content)
    assert base64.b64decode(payload["raw"]["bytes_b64"]) == content
    # Извлечение выполняется по оси extract; сырые байты возвращаются всегда (retention).
    assert payload["text"]["content"] == "# clean markdown"
    assert payload["error"] is None
    assert crawler.calls == [("https://site.test/x", FetchMode.HTTP)]
    assert extractor.calls == 1


def test_fetch_with_extract_returns_text() -> None:
    env = _edge().envelope("fetch", {"url": "https://site.test/x", "extract": "html"})
    assert env["status"] == "ok"
    assert env["payload"]["text"] == {
        "content": "# clean markdown",
        "hash": content_hash("# clean markdown"),
        "extractor_version": "fake-extractor-1",
    }
    assert env["payload"]["error"] is None


def test_fetch_passes_fetch_mode_and_request_id_echo() -> None:
    crawler = FakeCrawler()
    env = _edge(crawler=crawler).envelope(
        "fetch", {"url": "https://site.test/x", "fetch": "browser"}, request_id="abc123"
    )
    assert env["request_id"] == "abc123"
    assert env["payload"]["request_id"] == "abc123"
    assert crawler.calls == [("https://site.test/x", FetchMode.BROWSER)]


def test_fetch_rejects_missing_url() -> None:
    env = _edge().envelope("fetch", {})
    assert env["status"] == "error"
    assert "url" in env["error"]


def test_fetch_rejects_unknown_fetch() -> None:
    env = _edge().envelope("fetch", {"url": "https://site.test/x", "fetch": "flash"})
    assert env["status"] == "error"
    assert "fetch" in env["error"]


# ---------------------------------------------------------------------------
# /v1/search
# ---------------------------------------------------------------------------


def test_search_returns_candidates_only() -> None:
    searcher = FakeSearcher(
        [
            SearchResult(url="https://a.test", title="A", snippet="sa", rank=2),
            SearchResult(url="https://b.test", title="B", snippet="sb", rank=1),
        ]
    )
    env = _edge(searchers=searcher).envelope(
        "search", {"query": "q", "lang": "ru", "limit": 3, "phase": "corpus"}
    )

    assert env["status"] == "ok"
    assert env["payload"] == [
        {"url": "https://a.test", "title": "A", "snippet": "sa", "rank": 2},
        {"url": "https://b.test", "title": "B", "snippet": "sb", "rank": 1},
    ]
    # I19: кандидаты, не evidence — в payload нет сырых байтов/полей статусов.
    assert all(
        set(item) == {"url", "title", "snippet", "rank"} for item in env["payload"]
    )
    assert searcher.last_query == SearchQuery(
        text="q", lang="ru", limit=3, phase="corpus"
    )


def test_search_rejects_empty_query() -> None:
    env = _edge().envelope("search", {"query": "   "})
    assert env["status"] == "error"


# ---------------------------------------------------------------------------
# /v1/registry
# ---------------------------------------------------------------------------


def test_registry_returns_facts_with_source_url() -> None:
    registry = FakeRegistry()
    env = _edge(registries=registry).envelope(
        "registry",
        {
            "registry_id": "registry-x",
            "product_ref": {
                "product_id": "p1",
                "canonical_name": "P",
                "vendor": "V",
                "extra": {"kind": "sample"},
            },
            "scope": {"run_id": "r1"},
        },
    )

    assert env["status"] == "ok"
    assert env["payload"] == [
        {
            "fact_json": json.dumps({"k": "v"}),
            "source_url": "https://registry.test/a",
            "fetched_at": _FIXED_TIME,
        }
    ]
    product, scope = registry.seen[0]
    assert product.product_id == "p1"
    assert product.canonical_name == "P"
    assert product.vendor == "V"
    assert product.extra == {"kind": "sample"}
    assert scope == {"run_id": "r1"}


def test_registry_r1_missing_source_url_is_error() -> None:
    registry = FakeRegistry(facts=[FakeFact("{}", "")])
    env = _edge(registries=registry).envelope(
        "registry", {"registry_id": "registry-x", "product_ref": {"product_id": "p1"}}
    )
    assert env["status"] == "error"
    assert "source_url" in env["error"]


# ---------------------------------------------------------------------------
# /v1/crawl/* (Phase 3.1; планировщик инъецируется, Wave 3.2 — реализация)
# ---------------------------------------------------------------------------

_SUBMIT_REQUEST: dict[str, Any] = {
    "run_id": "r1",
    "session_id": "s1",
    "idempotency_key": "k1",
    "profile": "public",
    "items": [{"product_id": "p1", "url": "https://site.test/x"}],
}


def test_crawl_submit_envelope_shape() -> None:
    scheduler = FakeScheduler()
    env = _edge(scheduler=scheduler).envelope("crawl/submit", _SUBMIT_REQUEST)
    assert env["status"] == "ok"
    assert env["payload"] == {
        "job_id": "job-1",
        "accepted": True,
        "total": 1,
        "session_id": "s1",
    }
    assert scheduler.submitted == [_SUBMIT_REQUEST]


def test_crawl_status_envelope_shape() -> None:
    scheduler = FakeScheduler()
    env = _edge(scheduler=scheduler).envelope("crawl/status", {"job_id": "job-1"})
    assert env["status"] == "ok"
    assert env["payload"] == {
        "job_id": "job-1",
        "status": "running",
        "total": 3,
        "pending": 1,
        "running": 1,
        "fetched": 1,
        "failed": 0,
        "cancelled": 0,
        "engines": ["http"],
        "kinds": ["html"],
    }
    assert scheduler.status_calls == ["job-1"]


def test_crawl_cancel_envelope_shape() -> None:
    env = _edge(scheduler=FakeScheduler()).envelope("crawl/cancel", {"job_id": "job-1"})
    assert env["status"] == "ok"
    assert env["payload"] == {"job_id": "job-1", "status": "cancelled", "cancelled": 2}


def test_crawl_collect_envelope_shape_is_content_not_evidence() -> None:
    scheduler = FakeScheduler()
    env = _edge(scheduler=scheduler).envelope(
        "crawl/collect", {"job_id": "job-1", "cursor": "c", "limit": 5}
    )
    assert env["status"] == "ok"
    payload = env["payload"]
    assert payload["next_cursor"] is None
    assert payload["done"] is True
    item = payload["items"][0]
    assert item["product_id"] == "p1"
    assert item["raw"] == {"hash": "h", "size": 2, "bytes_b64": "YQ=="}
    assert scheduler.collect_calls == [("job-1", "c", 5)]


def test_crawl_without_scheduler_is_status_error() -> None:
    env = _edge().envelope("crawl/submit", _SUBMIT_REQUEST)
    assert env["status"] == "error"
    assert env["error"] == "crawl scheduler is not available on this edge node"
    assert env["payload"] is None


def test_crawl_request_validation_error_without_scheduler() -> None:
    env = _edge().envelope("crawl/submit", {**_SUBMIT_REQUEST, "items": []})
    assert env["status"] == "error"
    assert "items" in env["error"]


def test_crawl_scheduler_exception_becomes_status_error_without_traceback() -> None:
    edge = _edge(scheduler=FakeScheduler(raises=RuntimeError("queue down")))
    env = edge.envelope("crawl/status", {"job_id": "job-1"})
    assert env["status"] == "error"
    assert "queue down" in env["error"]
    assert "Traceback" not in env["error"]


def test_crawl_scheduler_non_mapping_is_error() -> None:
    class BadScheduler:
        def submit(self, request: Mapping[str, Any]) -> list[str]:
            return ["nope"]

    env = _edge(scheduler=BadScheduler()).envelope("crawl/submit", _SUBMIT_REQUEST)
    assert env["status"] == "error"


def test_route_post_crawl_ops_are_routed() -> None:
    scheduler = FakeScheduler()
    edge = _edge(scheduler=scheduler)

    body = json.dumps(_SUBMIT_REQUEST).encode("utf-8")
    code, env = route_request(edge, "POST", "/v1/crawl/submit", body)
    assert code == 200
    assert env["payload"]["job_id"] == "job-1"

    code, env = route_request(
        edge, "POST", "/v1/crawl/status", b'{"job_id": "job-1"}'
    )
    assert code == 200 and env["payload"]["status"] == "running"

    code, env = route_request(
        edge, "POST", "/v1/crawl/cancel", b'{"job_id": "job-1"}'
    )
    assert code == 200 and env["payload"]["status"] == "cancelled"

    code, env = route_request(
        edge, "POST", "/v1/crawl/collect", b'{"job_id": "job-1"}'
    )
    assert code == 200 and env["payload"]["done"] is True


def test_route_get_crawl_is_405() -> None:
    code, env = route_request(_edge(), "GET", "/v1/crawl/submit")
    assert code == 405
    assert env["status"] == "error"


def test_route_unknown_crawl_op_is_404() -> None:
    code, env = route_request(_edge(), "POST", "/v1/crawl/teleport", b"{}")
    assert code == 404
    assert env["status"] == "error"


# ---------------------------------------------------------------------------
# /v1/crawl/poll (Wave 5.1; сервис инъецируется)
# ---------------------------------------------------------------------------


def test_crawl_poll_envelope_shape_is_links_not_evidence() -> None:
    poller = FakePoller()
    env = _edge(poller=poller).envelope(
        "crawl/poll", {"url": "https://site.test/feed", "kind": "feed"}
    )
    assert env["status"] == "ok"
    assert env["payload"] == {
        "entries": [
            {
                "url": "https://site.test/a",
                "guid": "g1",
                "published_at": "2026-01-01",
                "title": "A",
            },
            {"url": "https://site.test/b", "guid": None, "published_at": None, "title": None},
        ],
        "count": 2,
        "truncated": False,
    }
    assert poller.calls == [{"url": "https://site.test/feed", "kind": "feed"}]


def test_crawl_poll_without_poller_is_status_error() -> None:
    env = _edge().envelope("crawl/poll", {"url": "https://site.test/feed", "kind": "feed"})
    assert env["status"] == "error"
    assert env["error"] == "crawl poller is not available on this edge node"
    assert env["payload"] is None


def test_crawl_poll_request_validation_error_without_poller() -> None:
    env = _edge().envelope("crawl/poll", {"url": "https://site.test/feed"})
    assert env["status"] == "error"
    assert "kind" in env["error"]


def test_crawl_poll_poller_exception_becomes_status_error_without_traceback() -> None:
    edge = _edge(poller=FakePoller(raises=RuntimeError("feed down")))
    env = edge.envelope("crawl/poll", {"url": "https://site.test/feed", "kind": "feed"})
    assert env["status"] == "error"
    assert "feed down" in env["error"]
    assert "Traceback" not in env["error"]


def test_crawl_poll_poller_non_mapping_is_error() -> None:
    class BadPoller:
        def poll(self, request: Mapping[str, Any]) -> list[str]:
            return ["nope"]

    env = _edge(poller=BadPoller()).envelope(
        "crawl/poll", {"url": "https://site.test/feed", "kind": "feed"}
    )
    assert env["status"] == "error"


def test_crawl_poll_poller_entry_requires_url() -> None:
    class NoUrlPoller:
        def poll(self, request: Mapping[str, Any]) -> dict[str, Any]:
            return {"entries": [{"title": "no url"}], "count": 1, "truncated": False}

    env = _edge(poller=NoUrlPoller()).envelope(
        "crawl/poll", {"url": "https://site.test/feed", "kind": "feed"}
    )
    assert env["status"] == "error"
    assert "url" in env["error"]


def test_route_post_crawl_poll_is_routed() -> None:
    poller = FakePoller()
    code, env = route_request(
        _edge(poller=poller),
        "POST",
        "/v1/crawl/poll",
        json.dumps({"url": "https://site.test/feed", "kind": "channel"}).encode("utf-8"),
    )
    assert code == 200
    assert env["payload"]["count"] == 2
    assert poller.calls == [{"url": "https://site.test/feed", "kind": "channel"}]


def test_route_get_crawl_poll_is_405() -> None:
    code, env = route_request(_edge(), "GET", "/v1/crawl/poll")
    assert code == 405
    assert env["status"] == "error"


# ---------------------------------------------------------------------------
# Ошибки адаптеров → status=error без traceback
# ---------------------------------------------------------------------------


def test_crawler_exception_becomes_status_error_without_traceback() -> None:
    edge = _edge(crawler=FakeCrawler(raises=RuntimeError("upstream boom")))
    env = edge.envelope("fetch", {"url": "https://site.test/x"})
    assert env["status"] == "error"
    assert env["payload"] is None
    assert "upstream boom" in env["error"]
    assert "Traceback" not in env["error"]


def test_extractor_result_error_is_captured_best_effort() -> None:
    edge = _edge(extractor=FakeExtractor(result_error="parse failed"))
    env = edge.envelope("fetch", {"url": "https://site.test/x", "extract": "document"})
    # Сбой извлечения не лишает ядро сырых байтов: raw возвращается, ошибка — в error.
    assert env["status"] == "ok"
    assert env["payload"]["raw"] is not None
    assert env["payload"]["text"] is None
    assert "parse failed" in env["payload"]["error"]


def test_search_exception_becomes_status_error() -> None:
    edge = _edge(searchers=FakeSearcher(raises=RuntimeError("serp down")))
    env = edge.envelope("search", {"query": "q"})
    assert env["status"] == "error"
    assert "serp down" in env["error"]


def test_unknown_op_is_error() -> None:
    env = _edge().envelope("nope", {})
    assert env["status"] == "error"
    assert "неизвестная операция" in env["error"]


# ---------------------------------------------------------------------------
# Зеркальный round-trip client↔server (главный тест совместимости конверта)
# ---------------------------------------------------------------------------


def test_round_trip_fetch_via_client() -> None:
    content = b"<html>round trip</html>"
    edge = _edge(crawler=FakeCrawler(content))
    page = RemoteCrawler(_client(edge)).fetch(
        "https://site.test/x", FetchPlan(fetch=FetchMode.BROWSER)
    )

    assert isinstance(page, RawPage)
    assert page.url == "https://site.test/x"
    assert page.content == content
    assert page.status_code == 200
    assert page.content_type == "text/html; charset=utf-8"
    assert page.fetch_mode is FetchMode.BROWSER


def test_round_trip_extract_via_client() -> None:
    edge = _edge()
    page = RawPage(url="https://site.test/x", content=b"raw", fetched_at="T")
    doc = RemoteExtractor(_client(edge)).extract(page)

    assert isinstance(doc, ExtractedDoc)
    assert doc.text == "# clean markdown"
    assert doc.content_hash == content_hash("# clean markdown")
    assert doc.extractor_version == "fake-extractor-1"
    assert doc.error is None


def test_round_trip_search_via_client() -> None:
    edge = _edge()
    results = RemoteSearch(_client(edge)).search(
        SearchQuery(text="q", lang="ru", limit=5, phase="corpus")
    )
    assert [r.url for r in results] == ["https://a.test"]
    assert all(r.raw_bytes is None for r in results)  # I19: кандидат, не evidence


def test_round_trip_registry_via_client() -> None:
    edge = _edge()
    fetcher = RemoteRegistry(_client(edge), registry_id="registry-x")
    facts = fetcher.fetch(ProductRef(product_id="p1"), {"run_id": "r1"})

    assert len(facts) == 1
    assert facts[0].source_url == "https://registry.test/a"  # R1
    assert facts[0].product_id == "p1"
    assert facts[0].registry_id == "registry-x"
    assert json.loads(facts[0].fact_json) == {"k": "v"}


def test_round_trip_capabilities_via_client() -> None:
    edge = _edge()
    assert _client(edge).capabilities() == edge.capabilities()


def test_client_sees_schema_mismatch_when_server_bumps() -> None:
    edge = _edge(schema_version="2")
    gateway = _client(edge)
    # Клиент дефолт v1 против сервера v2 — версионная граница ловится клиентом.
    from core.adapters.remote_gateway import GatewaySchemaError

    with pytest.raises(GatewaySchemaError):
        gateway.call("health", {})


# ---------------------------------------------------------------------------
# HTTP-слой: чистая маршрутизация (без сокета)
# ---------------------------------------------------------------------------


def test_route_get_health() -> None:
    code, env = route_request(_edge(), "GET", "/v1/health")
    assert code == 200
    assert env["status"] == "ok"
    assert env["payload"] == {"status": "ok"}


def test_route_get_capabilities() -> None:
    code, env = route_request(_edge(), "GET", "/v1/capabilities")
    assert code == 200
    assert env["payload"]["crawler_version"] == "fake-crawler-1"


def test_route_post_fetch_decodes_json_body_and_ignores_query() -> None:
    body = json.dumps({"url": "https://site.test/x"}).encode("utf-8")
    code, env = route_request(_edge(), "POST", "/v1/fetch?trace=1", body)
    assert code == 200
    assert env["status"] == "ok"
    assert env["payload"]["final_url"] == "https://site.test/x"


def test_route_unknown_path_is_404() -> None:
    code, env = route_request(_edge(), "GET", "/nope")
    assert code == 404
    assert env["status"] == "error"


def test_route_method_not_allowed_is_405() -> None:
    code, env = route_request(_edge(), "GET", "/v1/fetch")
    assert code == 405
    assert env["status"] == "error"


def test_route_invalid_json_is_400() -> None:
    code, env = route_request(_edge(), "POST", "/v1/search", b"{not json")
    assert code == 400
    assert env["status"] == "error"


def test_route_echoes_request_id_header() -> None:
    code, env = route_request(_edge(), "GET", "/v1/health", request_id="rid-7")
    assert code == 200
    assert env["request_id"] == "rid-7"


# ---------------------------------------------------------------------------
# HTTP-слой: сокет-обвязка, но запуск через внедрённую фабрику (без сети)
# ---------------------------------------------------------------------------


def test_make_handler_returns_request_handler_subclass() -> None:
    handler = make_handler(_edge())
    assert issubclass(handler, BaseHTTPRequestHandler)


def test_build_server_uses_injected_factory() -> None:
    seen: dict[str, Any] = {}

    def server_factory(address: tuple[str, int], handler: Any) -> Any:
        seen["address"] = address
        seen["handler"] = handler
        return object()

    server = build_server(
        "127.0.0.1", 8443, _edge(), server_factory=server_factory
    )
    assert server is not None
    assert seen["address"] == ("127.0.0.1", 8443)
    assert issubclass(seen["handler"], BaseHTTPRequestHandler)


def test_serve_runs_and_closes_injected_server_without_socket() -> None:
    calls: dict[str, Any] = {}

    class FakeServer:
        def __init__(self, address: tuple[str, int], handler: Any) -> None:
            calls["address"] = address
            calls["handler"] = handler

        def serve_forever(self) -> None:
            calls["served"] = True

        def server_close(self) -> None:
            calls["closed"] = True

    serve("127.0.0.1", 0, _edge(), server_factory=FakeServer)
    assert calls["served"] is True
    assert calls["closed"] is True
    assert calls["address"] == ("127.0.0.1", 0)
    assert issubclass(calls["handler"], BaseHTTPRequestHandler)


# ---------------------------------------------------------------------------
# Изоляция (I18/I19): в исходнике сервера нет импортов store/domain
# ---------------------------------------------------------------------------


def _server_source() -> str:
    assert edge_server_module.__file__ is not None
    return Path(edge_server_module.__file__).read_text(encoding="utf-8")


def test_server_source_has_no_store_or_domain_import() -> None:
    source = _server_source()
    tree = ast.parse(source)
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
        elif isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)

    forbidden = ("core.store", "core.domain")
    assert not [m for m in imported if m == forbidden[0] or m.startswith(f"{forbidden[0]}.")]
    assert not [m for m in imported if m == forbidden[1] or m.startswith(f"{forbidden[1]}.")]
    # Прямая текстовая страховка: ни в импортах, ни в докстрингах.
    assert "core.store" not in source
    assert "core.domain" not in source


def test_edge_behavior_never_touches_a_store() -> None:
    edge = _edge()
    # Полный цикл контентных операций без какого-либо хранилища.
    assert edge.envelope("fetch", {"url": "https://site.test/x"})["status"] == "ok"
    assert edge.envelope("search", {"query": "q"})["status"] == "ok"
    assert edge.envelope("registry", {"registry_id": "registry-x"})["status"] == "ok"
    # Ни атрибута-хранилища, ни доступа к нему у сервера нет.
    assert not hasattr(edge, "store")
    assert not hasattr(edge, "db")
    assert not any("store" in name for name in vars(edge))


# ---------------------------------------------------------------------------
# Нейтральность (§1.3)
# ---------------------------------------------------------------------------

def test_module_importable_in_isolation() -> None:
    module = importlib.import_module("core.edge.server")
    assert module.EDGE_SCHEMA_VERSION == "1"
