"""Тесты ``GoogleSearchAdapter`` (Google Programmable Search / Custom Search JSON API).

Сеть не задействуется: HTTP-вызовы внедряются через шов ``http_get`` (обслуживает и
поисковый API, и ``fetch``). Покрываются: разбор ``items`` (url/title/snippet/rank),
постраничность при ``limit>10`` (CSE — максимум 10 на запрос), маппинг языка в ``lr``,
``fetch`` с SSRF-валидацией и кэшем (R7), ``available`` без учётных данных и без
``httpx``, структурное соответствие порту, I19-инвариант (модуль не ссылается на
доказательства/ячейки/хранилище), категорийная нейтральность (§1.3).

Фикстуры **категорийно-нейтральны**: example.com / «Example», без продуктов и вендоров.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

import core.adapters.search_google as module
from core.adapters.search_google import (
    DEFAULT_BASE_URL,
    GoogleSearchAdapter,
    SearchAdapterUnavailable,
    SearchError,
)
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.types import RawPage, FetchMode
from core.util.cache import ContentAddressedCache, http_key

URL_A = "https://example.com/a"
URL_B = "https://example.com/b"
URL_C = "https://example.com/c"

_ENV_VARS = (
    module.ENV_API_KEY,
    module.ENV_API_KEY_ALT,
    module.ENV_CSE_ID,
    module.ENV_CSE_ID_ALT,
)


@pytest.fixture(autouse=True)
def _clear_google_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Изолирует тесты от случайных учётных данных Google в окружении."""
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _items(*records: dict[str, Any]) -> dict[str, Any]:
    """Ответ CSE с записями ``items``."""
    return {"items": list(records)}


def _item(num: int) -> dict[str, Any]:
    """Нейтральная запись CSE."""
    return {
        "title": f"Example {num}",
        "link": f"https://example.com/{num}",
        "snippet": f"snippet {num}",
    }


def _adapter(http_get: Any = None, **kwargs: Any) -> GoogleSearchAdapter:
    """Адаптер с валидной парой ключ+cx (для доступного пути поиска)."""
    kwargs.setdefault("api_key", "test-key")
    kwargs.setdefault("cse_id", "test-cx")
    return GoogleSearchAdapter(http_get=http_get, **kwargs)


# ---------------------------------------------------------------------------
# Контракт порта / атрибуты
# ---------------------------------------------------------------------------


def test_class_attributes() -> None:
    adapter = _adapter(lambda url: json.dumps(_items()).encode())
    assert adapter.adapter_kind == "serp"
    assert adapter.provider == "google"


def test_implements_search_adapter_port() -> None:
    adapter = _adapter(lambda url: json.dumps(_items()).encode())
    assert isinstance(adapter, SearchAdapterPort)
    assert callable(adapter.search)
    assert callable(adapter.fetch)


def test_defaults_and_base_url() -> None:
    adapter = GoogleSearchAdapter()
    assert adapter.base_url == DEFAULT_BASE_URL
    assert DEFAULT_BASE_URL.endswith("/customsearch/v1")


def test_construct_without_credentials_does_not_crash() -> None:
    adapter = GoogleSearchAdapter()
    assert adapter.api_key is None
    assert adapter.cse_id is None


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        GoogleSearchAdapter(base_url="")
    with pytest.raises(ValueError):
        GoogleSearchAdapter(timeout=0)


def test_env_aliases_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(module.ENV_API_KEY_ALT, "k")
    monkeypatch.setenv(module.ENV_CSE_ID_ALT, "c")
    adapter = GoogleSearchAdapter()
    assert adapter.api_key == "k"
    assert adapter.cse_id == "c"


# ---------------------------------------------------------------------------
# search: разбор кандидатов
# ---------------------------------------------------------------------------


def test_search_parses_candidates() -> None:
    payload = _items(
        {"title": "Example A", "link": URL_A, "snippet": "first"},
        {"title": "Example B", "link": URL_B, "snippet": "second"},
    )
    adapter = _adapter(lambda url: json.dumps(payload).encode())
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A, URL_B]
    assert [r.title for r in results] == ["Example A", "Example B"]
    assert [r.snippet for r in results] == ["first", "second"]
    assert [r.rank for r in results] == [1, 2]
    assert all(isinstance(r, SearchResult) for r in results)


def test_search_returns_candidates_without_page_bytes() -> None:
    # I19: поиск отдаёт кандидатов; байты страницы берёт fetch, не search.
    adapter = _adapter(lambda url: json.dumps(_items(_item(1))).encode())
    results = adapter.search(SearchQuery(text="neutral query"))
    assert all(r.raw_bytes is None for r in results)


def test_search_unknown_shape_is_empty_not_error() -> None:
    adapter = _adapter(lambda url: json.dumps({"unexpected": True}).encode())
    assert adapter.search(SearchQuery(text="neutral query")) == []


def test_search_records_without_link_skipped() -> None:
    payload = _items({"title": "no link"}, {"link": URL_A, "title": "Example A"})
    adapter = _adapter(lambda url: json.dumps(payload).encode())
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A]


# ---------------------------------------------------------------------------
# search: бюджет / язык / постраничность
# ---------------------------------------------------------------------------


def test_search_lang_maps_to_lr() -> None:
    captured: list[str] = []

    def _get(url: str) -> bytes:
        captured.append(url)
        return json.dumps(_items()).encode()

    adapter = _adapter(_get)
    adapter.search(SearchQuery(text="neutral query", lang="ru"))
    adapter.search(SearchQuery(text="neutral query", lang="en"))
    assert parse_qs(urlsplit(captured[0]).query)["lr"] == ["lang_ru"]
    assert parse_qs(urlsplit(captured[1]).query)["lr"] == ["lang_en"]


def test_search_num_reflects_limit() -> None:
    captured: list[str] = []

    def _get(url: str) -> bytes:
        captured.append(url)
        return json.dumps(_items(_item(1))).encode()

    adapter = _adapter(_get)
    adapter.search(SearchQuery(text="neutral query", limit=3))
    assert parse_qs(urlsplit(captured[0]).query)["num"] == ["3"]


def test_search_pages_when_limit_exceeds_ten() -> None:
    captured: list[str] = []

    def _get(url: str) -> bytes:
        captured.append(url)
        start = parse_qs(urlsplit(url).query).get("start", ["1"])[0]
        if start == "1":
            return json.dumps(_items(*(_item(i) for i in range(1, 11)))).encode()
        return json.dumps(_items(*(_item(i) for i in range(11, 14)))).encode()

    adapter = _adapter(_get)
    results = adapter.search(SearchQuery(text="neutral query", limit=12))
    assert len(results) == 12
    assert len(captured) == 2
    assert parse_qs(urlsplit(captured[1]).query)["start"] == ["11"]


def test_search_respects_limit_within_page() -> None:
    payload = _items(*(_item(i) for i in range(1, 11)))
    adapter = _adapter(lambda url: json.dumps(payload).encode())
    results = adapter.search(SearchQuery(text="neutral query", limit=5))
    assert len(results) == 5


def test_search_limit_zero_returns_empty_without_http() -> None:
    def _boom(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("HTTP не должен вызываться при limit<=0")

    adapter = _adapter(_boom)
    assert adapter.search(SearchQuery(text="neutral query", limit=0)) == []


def test_search_empty_query_rejected() -> None:
    adapter = _adapter(lambda url: json.dumps(_items()).encode())
    with pytest.raises(ValueError):
        adapter.search(SearchQuery(text="   "))


# ---------------------------------------------------------------------------
# Ошибки
# ---------------------------------------------------------------------------


def test_search_error_body_raises_search_error() -> None:
    payload = {"error": {"code": 403, "message": "quota exceeded"}}
    adapter = _adapter(lambda url: json.dumps(payload).encode())
    with pytest.raises(SearchError, match="quota exceeded"):
        adapter.search(SearchQuery(text="neutral query"))


def test_search_seam_failure_raises_search_error() -> None:
    def _boom(url: str) -> bytes:
        raise RuntimeError("network down")

    adapter = _adapter(_boom)
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def test_search_error_is_runtime_error() -> None:
    assert issubclass(SearchError, RuntimeError)
    assert issubclass(SearchAdapterUnavailable, RuntimeError)


# ---------------------------------------------------------------------------
# fetch: SSRF, RawPage, кэш (R7)
# ---------------------------------------------------------------------------


def test_fetch_returns_rawpage_from_seam() -> None:
    adapter = _adapter(lambda url: b"<html>page</html>")
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

    adapter = _adapter(_get)
    with pytest.raises(ValueError):
        adapter.fetch("http://127.0.0.1/private")
    assert calls == []


def test_fetch_cache_hit_avoids_http_get(tmp_path: Path) -> None:
    cache = ContentAddressedCache(root=tmp_path)
    cache.put_bytes("http", http_key(URL_A), b"cached page")

    def _boom(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("http_get не должен вызываться при попадании в кэш")

    adapter = _adapter(_boom, cache=cache)
    assert adapter.fetch(URL_A).content == b"cached page"


# ---------------------------------------------------------------------------
# available / отсутствие готовности
# ---------------------------------------------------------------------------


def test_available_false_without_credentials() -> None:
    assert GoogleSearchAdapter().available is False


def test_available_true_with_seam_and_credentials() -> None:
    assert _adapter(lambda url: b"{}").available is True


def test_search_without_credentials_raises_unavailable() -> None:
    adapter = GoogleSearchAdapter(http_get=lambda url: b"{}")
    with pytest.raises(SearchAdapterUnavailable):
        adapter.search(SearchQuery(text="neutral query"))


def test_missing_httpx_and_seam_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    adapter = _adapter()
    assert adapter.available is False
    with pytest.raises(SearchAdapterUnavailable):
        adapter.search(SearchQuery(text="neutral query"))
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
