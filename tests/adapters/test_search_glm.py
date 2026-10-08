"""Тесты ``GlmSearchAdapter`` (W11/T11.1) — реализация ``SearchAdapterPort``.

Сеть не задействуется: HTTP-вызовы внедряются через швы ``http_post``/``http_get``.
Покрываются: разбор кандидатов (url/title/snippet/rank), проброс ``limit``/``lang``
провайдеру (симметрия бюджета — за вызывающим, I16), ``fetch`` с SSRF-валидацией и
кэшем (попадание без сети, промах→попадание, ровно один живой вызов, R7),
``available`` без ``httpx``, структурное соответствие порту, I19-инвариант (модуль не
ссылается на доказательства/ячейки/хранилище), категорийная нейтральность (§1.3).

Фикстуры **категорийно-нейтральны**: example.com / «Example», без продуктов и вендоров.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

import pytest

import core.adapters.search_glm as module
from core.adapters.search_glm import (
    DEFAULT_BASE_URL,
    DEFAULT_SEARCH_PATH,
    GlmSearchAdapter,
    SearchAdapterUnavailable,
    SearchError,
)
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.types import RawPage, RenderMode
from core.util.cache import ContentAddressedCache, http_key

URL_A = "https://example.com/a"
URL_B = "https://example.com/b"
URL_C = "https://example.com/c"

#: Основная форма ответа провайдера: ``search_result`` с полем ``link``.
_RECORDS = [
    {"link": URL_A, "title": "Example A", "content": "first snippet", "rank": 1},
    {"link": URL_B, "title": "Example B", "content": "second snippet", "rank": 2},
    {"link": URL_C, "title": "Example C", "content": "third snippet", "rank": 3},
]


def _post_returning(payload: Any) -> Any:
    """Создаёт ``http_post``-шов, отдающий заданный JSON (dict/list/str/bytes)."""

    def _post(url: str, body: dict[str, Any]) -> bytes:
        assert url == f"{DEFAULT_BASE_URL}{DEFAULT_SEARCH_PATH}"
        assert isinstance(body, dict)
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
        if isinstance(payload, str):
            return payload.encode("utf-8")
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    return _post


# ---------------------------------------------------------------------------
# Контракт порта / атрибуты
# ---------------------------------------------------------------------------


def test_class_attributes() -> None:
    adapter = GlmSearchAdapter(http_post=_post_returning({"search_result": []}))
    # AdapterKind.LLM_GROUNDING — заземляющий поиск (LLM-grounding), не классическая SERP.
    assert adapter.adapter_kind == "llm_grounding"
    assert adapter.provider == "glm"


def test_implements_search_adapter_port() -> None:
    adapter = GlmSearchAdapter(http_post=_post_returning({"search_result": []}))
    assert isinstance(adapter, SearchAdapterPort)
    assert callable(adapter.search)
    assert callable(adapter.fetch)


def test_defaults_and_endpoint() -> None:
    adapter = GlmSearchAdapter()
    assert adapter.base_url == DEFAULT_BASE_URL
    assert adapter.search_path == DEFAULT_SEARCH_PATH
    assert adapter.endpoint == f"{DEFAULT_BASE_URL}{DEFAULT_SEARCH_PATH}"


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        GlmSearchAdapter(base_url="")
    with pytest.raises(ValueError):
        GlmSearchAdapter(search_path="web_search")
    with pytest.raises(ValueError):
        GlmSearchAdapter(search_engine="")
    with pytest.raises(ValueError):
        GlmSearchAdapter(timeout=0)


# ---------------------------------------------------------------------------
# search: разбор кандидатов
# ---------------------------------------------------------------------------


def test_search_parses_candidates() -> None:
    adapter = GlmSearchAdapter(http_post=_post_returning({"search_result": _RECORDS}))
    results = adapter.search(SearchQuery(text="neutral query", limit=10))

    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert [r.title for r in results] == ["Example A", "Example B", "Example C"]
    assert [r.snippet for r in results] == ["first snippet", "second snippet", "third snippet"]
    assert [r.rank for r in results] == [1, 2, 3]
    assert all(isinstance(r, SearchResult) for r in results)


def test_search_returns_candidates_without_page_bytes() -> None:
    # I19: поиск отдаёт кандидатов; байты страницы берёт fetch, не search.
    adapter = GlmSearchAdapter(http_post=_post_returning({"search_result": _RECORDS}))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert all(r.raw_bytes is None for r in results)


def test_search_result_has_no_evidence_members() -> None:
    adapter = GlmSearchAdapter(http_post=_post_returning({"search_result": _RECORDS}))
    result = adapter.search(SearchQuery(text="neutral query"))[0]
    for banned in ("source_id", "citation_quote", "content_hash", "evidence"):
        assert not hasattr(result, banned)


def test_search_accepts_alt_field_names() -> None:
    records = [{"url": URL_A, "title": "Example A", "description": "body text"}]
    adapter = GlmSearchAdapter(http_post=_post_returning({"results": records}))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert results[0].url == URL_A
    assert results[0].snippet == "body text"
    assert results[0].rank == 1  # позиция, т.к. ранг не задан


def test_search_parses_nested_data_shape() -> None:
    adapter = GlmSearchAdapter(
        http_post=_post_returning({"data": {"search_result": _RECORDS}})
    )
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A, URL_B, URL_C]


def test_search_unknown_shape_is_empty_not_error() -> None:
    adapter = GlmSearchAdapter(http_post=_post_returning({"unexpected": True}))
    assert adapter.search(SearchQuery(text="neutral query")) == []


def test_search_records_without_url_skipped() -> None:
    payload = {"search_result": [{"title": "no url"}, _RECORDS[0]]}
    adapter = GlmSearchAdapter(http_post=_post_returning(payload))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A]


# ---------------------------------------------------------------------------
# search: бюджет / лимит / язык (I16 — симметрия за вызывающим)
# ---------------------------------------------------------------------------


def test_search_respects_limit() -> None:
    adapter = GlmSearchAdapter(http_post=_post_returning({"search_result": _RECORDS * 4}))
    results = adapter.search(SearchQuery(text="neutral query", limit=5))
    assert len(results) == 5


def test_search_limit_and_lang_sent_to_provider() -> None:
    captured: dict[str, Any] = {}

    def _post(url: str, body: dict[str, Any]) -> bytes:
        captured.update(body)
        return json.dumps({"search_result": _RECORDS}).encode("utf-8")

    adapter = GlmSearchAdapter(http_post=_post)
    adapter.search(SearchQuery(text="neutral query", lang="en", limit=7))
    assert captured["count"] == 7
    assert captured["search_query"] == "neutral query"
    assert captured["language"] == "en"  # I16: язык прозрачно передан провайдеру


def test_search_limit_zero_returns_empty_without_http() -> None:
    def _boom(url: str, body: dict[str, Any]) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("HTTP не должен вызываться при limit<=0")

    adapter = GlmSearchAdapter(http_post=_boom)
    assert adapter.search(SearchQuery(text="neutral query", limit=0)) == []


def test_search_empty_query_rejected() -> None:
    adapter = GlmSearchAdapter(http_post=_post_returning({"search_result": []}))
    with pytest.raises(ValueError):
        adapter.search(SearchQuery(text="   "))


# ---------------------------------------------------------------------------
# fetch: SSRF, RawPage, кэш (R7)
# ---------------------------------------------------------------------------


def test_fetch_returns_rawpage_from_seam() -> None:
    adapter = GlmSearchAdapter(http_get=lambda url: b"<html>page</html>")
    page = adapter.fetch(URL_A)
    assert isinstance(page, RawPage)
    assert page.url == URL_A
    assert page.content == b"<html>page</html>"
    assert page.render_mode is RenderMode.HTTP
    assert page.fetched_at


def test_fetch_blocks_ssrf_loopback_without_http() -> None:
    calls: list[str] = []

    def _get(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        calls.append(url)
        return b"should not happen"

    adapter = GlmSearchAdapter(http_get=_get)
    with pytest.raises(ValueError):
        adapter.fetch("http://127.0.0.1/private")
    assert calls == []


def test_fetch_cache_hit_avoids_http_get(tmp_path: Path) -> None:
    cache = ContentAddressedCache(root=tmp_path)
    cache.put_bytes("http", http_key(URL_A), b"cached page")

    def _boom(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("http_get не должен вызываться при попадании в кэш")

    adapter = GlmSearchAdapter(http_get=_boom, cache=cache)
    page = adapter.fetch(URL_A)
    assert page.content == b"cached page"


def test_fetch_cache_miss_then_hit_calls_http_once(tmp_path: Path) -> None:
    cache = ContentAddressedCache(root=tmp_path)
    calls: list[str] = []

    def _get(url: str) -> bytes:
        calls.append(url)
        return b"live page"

    adapter = GlmSearchAdapter(http_get=_get, cache=cache)
    first = adapter.fetch(URL_A)
    second = adapter.fetch(URL_A)

    assert first.content == b"live page"
    assert second.content == b"live page"
    assert calls == [URL_A]  # ровно один живой вызов; второй — из кэша


def test_fetch_without_cache_calls_http_each_time() -> None:
    calls: list[str] = []
    adapter = GlmSearchAdapter(http_get=lambda url: (calls.append(url), b"x")[1])
    adapter.fetch(URL_A)
    adapter.fetch(URL_A)
    assert calls == [URL_A, URL_A]


# ---------------------------------------------------------------------------
# available / отсутствие зависимости
# ---------------------------------------------------------------------------


def test_available_with_seam_without_httpx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    with_seam = GlmSearchAdapter(http_post=_post_returning({"search_result": []}))
    without_seam = GlmSearchAdapter()
    assert with_seam.available is True
    assert without_seam.available is False


def test_available_reflects_httpx() -> None:
    assert GlmSearchAdapter().available is module._httpx_available()


def test_missing_dependency_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    adapter = GlmSearchAdapter()
    with pytest.raises(SearchAdapterUnavailable):
        adapter.search(SearchQuery(text="neutral query"))
    with pytest.raises(SearchAdapterUnavailable):
        adapter.fetch(URL_A)
    assert issubclass(SearchAdapterUnavailable, RuntimeError)


def test_search_error_is_runtime_error() -> None:
    assert issubclass(SearchError, RuntimeError)


# ---------------------------------------------------------------------------
# I19 структурно: модуль не ссылается на доказательства/ячейки/хранилище
# ---------------------------------------------------------------------------

_TYPE_REFERENCE_BANNED = ("CellEvidence", "Cell", "evidence", "core.domain", "core.store")


def test_i19_module_source_has_no_evidence_references() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    lowered = source.lower()
    for token in _TYPE_REFERENCE_BANNED:
        assert token.lower() not in lowered, f"I19: модуль ссылается на {token!r}"


def test_i19_module_imports_no_domain_or_store() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
    assert all(not mod.startswith(("core.domain", "core.store")) for mod in imported)


# ---------------------------------------------------------------------------
# Нейтральность модуля (§1.3)
# ---------------------------------------------------------------------------


def test_module_source_is_category_neutral() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    hits = [line for line in source.splitlines() if forbidden.search(line)]
    assert hits == []
