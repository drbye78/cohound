"""Тесты ``DuckDuckGoSearchAdapter`` (библиотека ``ddgs``).

Сеть не задействуется: поиск внедряется через шов ``search_fn``, ``fetch`` — через
``http_get``. Покрываются: маппинг записей ``{title,href,body}`` (url=href,
snippet=body, rank — позиция), маппинг языка в регион (ru→ru-ru, en→us-en), уважение
``limit``, ``fetch`` с SSRF-валидацией и кэшем (R7), ``available`` без ``ddgs``/``httpx``,
структурное соответствие порту, I19-инвариант, категорийная нейтральность (§1.3).

Фикстуры **категорийно-нейтральны**: example.com / «Example», без продуктов и вендоров.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import pytest

import core.adapters.search_duckduckgo as module
from core.adapters.search_duckduckgo import (
    DEFAULT_BACKEND,
    DuckDuckGoSearchAdapter,
    SearchAdapterUnavailable,
    SearchError,
)
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.types import RawPage, FetchMode
from core.util.cache import ContentAddressedCache, http_key

URL_A = "https://example.com/a"
URL_B = "https://example.com/b"
URL_C = "https://example.com/c"

_RECORDS = [
    {"title": "Example A", "href": URL_A, "body": "first snippet"},
    {"title": "Example B", "href": URL_B, "body": "second snippet"},
    {"title": "Example C", "href": URL_C, "body": "third snippet"},
]


def _search_returning(records: list[dict[str, Any]]) -> Any:
    """Создаёт ``search_fn``-шов, отдающий заданные записи и фиксирующий аргументы."""

    def _search(query: str, limit: int, region: str) -> list[dict[str, Any]]:
        _search.calls.append((query, limit, region))  # type: ignore[attr-defined]
        return records

    _search.calls = []  # type: ignore[attr-defined]
    return _search


# ---------------------------------------------------------------------------
# Контракт порта / атрибуты
# ---------------------------------------------------------------------------


def test_class_attributes() -> None:
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning([]))
    assert adapter.adapter_kind == "serp"
    assert adapter.provider == "duckduckgo"


def test_implements_search_adapter_port() -> None:
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning([]))
    assert isinstance(adapter, SearchAdapterPort)
    assert callable(adapter.search)
    assert callable(adapter.fetch)


def test_default_backend() -> None:
    assert DuckDuckGoSearchAdapter().backend == DEFAULT_BACKEND
    assert DEFAULT_BACKEND == "duckduckgo"


def test_construct_without_dependencies_does_not_crash() -> None:
    adapter = DuckDuckGoSearchAdapter()
    assert adapter.backend == DEFAULT_BACKEND


def test_env_backend_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(module.ENV_BACKEND, "brave")
    assert DuckDuckGoSearchAdapter().backend == "brave"


def test_invalid_timeout_rejected() -> None:
    with pytest.raises(ValueError):
        DuckDuckGoSearchAdapter(timeout=0)


# ---------------------------------------------------------------------------
# search: маппинг, регион, лимит
# ---------------------------------------------------------------------------


def test_search_parses_candidates() -> None:
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning(_RECORDS))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert [r.title for r in results] == ["Example A", "Example B", "Example C"]
    assert [r.snippet for r in results] == ["first snippet", "second snippet", "third snippet"]
    assert [r.rank for r in results] == [1, 2, 3]
    assert all(isinstance(r, SearchResult) for r in results)


def test_search_returns_candidates_without_page_bytes() -> None:
    # I19: поиск отдаёт кандидатов; байты страницы берёт fetch, не search.
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning(_RECORDS))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert all(r.raw_bytes is None for r in results)


def test_search_lang_maps_to_region() -> None:
    search = _search_returning(_RECORDS)
    adapter = DuckDuckGoSearchAdapter(search_fn=search)
    adapter.search(SearchQuery(text="neutral query", lang="ru"))
    adapter.search(SearchQuery(text="neutral query", lang="en"))
    assert search.calls[0][2] == "ru-ru"
    assert search.calls[1][2] == "us-en"


def test_search_unknown_lang_falls_back_to_default_region() -> None:
    search = _search_returning(_RECORDS)
    DuckDuckGoSearchAdapter(search_fn=search).search(
        SearchQuery(text="neutral query", lang="de")
    )
    assert search.calls[0][2] == "ru-ru"


def test_search_passes_query_and_limit_to_seam() -> None:
    search = _search_returning(_RECORDS)
    DuckDuckGoSearchAdapter(search_fn=search).search(
        SearchQuery(text="neutral query", limit=7)
    )
    assert search.calls[0][0] == "neutral query"
    assert search.calls[0][1] == 7


def test_search_respects_limit() -> None:
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning(_RECORDS * 4))
    assert len(adapter.search(SearchQuery(text="neutral query", limit=5))) == 5


def test_search_limit_zero_returns_empty_without_seam() -> None:
    def _boom(query: str, limit: int, region: str) -> list[dict[str, Any]]:
        raise AssertionError("поиск не должен вызываться при limit<=0")

    adapter = DuckDuckGoSearchAdapter(search_fn=_boom)
    assert adapter.search(SearchQuery(text="neutral query", limit=0)) == []


def test_search_empty_query_rejected() -> None:
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning(_RECORDS))
    with pytest.raises(ValueError):
        adapter.search(SearchQuery(text="   "))


def test_search_records_without_href_skipped() -> None:
    records = [{"title": "no href"}, {"title": "Example A", "href": URL_A}]
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning(records))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A]


# ---------------------------------------------------------------------------
# Ошибки
# ---------------------------------------------------------------------------


def test_seam_failure_raises_search_error() -> None:
    def _boom(query: str, limit: int, region: str) -> list[dict[str, Any]]:
        raise RuntimeError("backend down")

    adapter = DuckDuckGoSearchAdapter(search_fn=_boom)
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_search_error_is_runtime_error() -> None:
    assert issubclass(SearchError, RuntimeError)
    assert issubclass(SearchAdapterUnavailable, RuntimeError)


# ---------------------------------------------------------------------------
# fetch: SSRF, RawPage, кэш (R7)
# ---------------------------------------------------------------------------


def test_fetch_returns_rawpage_from_seam() -> None:
    adapter = DuckDuckGoSearchAdapter(http_get=lambda url: b"<html>page</html>")
    page = adapter.fetch(URL_A)
    assert isinstance(page, RawPage)
    assert page.url == URL_A
    assert page.content == b"<html>page</html>"
    assert page.fetch_mode is FetchMode.HTTP
    assert page.fetched_at


def test_fetch_blocks_ssrf_loopback_without_http() -> None:
    calls: list[str] = []

    def _get(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        calls.append(url)
        return b"should not happen"

    adapter = DuckDuckGoSearchAdapter(http_get=_get)
    with pytest.raises(ValueError):
        adapter.fetch("http://127.0.0.1/private")
    assert calls == []


def test_fetch_cache_hit_avoids_http_get(tmp_path: Path) -> None:
    cache = ContentAddressedCache(root=tmp_path)
    cache.put_bytes("http", http_key(URL_A), b"cached page")

    def _boom(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("http_get не должен вызываться при попадании в кэш")

    adapter = DuckDuckGoSearchAdapter(http_get=_boom, cache=cache)
    assert adapter.fetch(URL_A).content == b"cached page"


# ---------------------------------------------------------------------------
# available / отсутствие готовности
# ---------------------------------------------------------------------------


def test_available_with_seam() -> None:
    assert DuckDuckGoSearchAdapter(search_fn=_search_returning([])).available is True
    assert DuckDuckGoSearchAdapter(http_get=lambda url: b"x").available is True


def test_available_false_without_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_ddgs_available", lambda: False)
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    assert DuckDuckGoSearchAdapter().available is False


def test_search_without_readiness_raises_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(module, "_ddgs_available", lambda: False)
    adapter = DuckDuckGoSearchAdapter()
    with pytest.raises(SearchAdapterUnavailable):
        adapter.search(SearchQuery(text="neutral query"))


def test_fetch_without_httpx_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    adapter = DuckDuckGoSearchAdapter(search_fn=_search_returning([]))
    with pytest.raises(SearchAdapterUnavailable):
        adapter.fetch(URL_A)


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
