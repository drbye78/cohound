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

import json
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
from core.ports.search import SearchQuery, SearchResult
from core.ports.types import FetchMode, RawPage
from core.util.cache import ContentAddressedCache, http_key
from core.util.retry import MAX_RETRY_ATTEMPTS

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
    assert page.fetch_mode is FetchMode.HTTP
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


def test_available_requires_all_required_seams(monkeypatch: pytest.MonkeyPatch) -> None:
    """S9b: readiness — конъюнкция по требуемым швам, а не «есть хотя бы один».

    Одного ``http_get`` недостаточно: ``search()`` уходит в POST-путь и упал бы —
    но readiness-гейт такой адаптер пропускал.
    """
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    post = _post_returning({"search_result": []})
    get = lambda url: b""  # noqa: E731 — шов-заглушка, вызываться не должен

    assert GlmSearchAdapter(http_post=post, http_get=get).available is True
    assert GlmSearchAdapter(http_get=get).available is False
    assert GlmSearchAdapter(http_post=post).available is False
    assert GlmSearchAdapter().available is False


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


# ---------------------------------------------------------------------------
# S6: повторы 429/5xx — общая политика core.util.retry
# ---------------------------------------------------------------------------

class _FakeTransportError(Exception):
    """Аналог ``httpx.TransportError`` для фейкового модуля."""

class _FakeResponse:
    """Минимальный ответ: статус, тело, заголовки (как у ``httpx.Response``)."""

    def __init__(
        self,
        status_code: int,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.url = URL_A

    def raise_for_status(self) -> None:
        """Как у ``httpx.Response``: ``>= 400`` — исключение (проверяется фетчем)."""
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

class _FakeHttpx:
    """Фейковый ``httpx``: очереди POST/GET-ответов, без сети."""

    TransportError = _FakeTransportError

    def __init__(
        self,
        *,
        post: list[Any] | None = None,
        get: list[Any] | None = None,
    ) -> None:
        self._post_queue = list(post or [])
        self._get_queue = list(get or [])
        self.post_calls: list[str] = []
        self.get_calls: list[str] = []

    @staticmethod
    def _next(queue: list[Any]) -> Any:
        if not queue:
            raise AssertionError("фейковый httpx: очередь ответов исчерпана")
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def post(self, url: str, **_kwargs: Any) -> Any:
        self.post_calls.append(url)
        return self._next(self._post_queue)

    def get(self, url: str, **_kwargs: Any) -> Any:
        self.get_calls.append(url)
        return self._next(self._get_queue)

def _install_fake_httpx(monkeypatch: pytest.MonkeyPatch, fake: _FakeHttpx) -> None:
    """Подменяет ленивый импорт ``httpx`` фейком (сеть не используется)."""
    monkeypatch.setattr(GlmSearchAdapter, "_import_http", staticmethod(lambda: fake))

def _json_results() -> bytes:
    return json.dumps({"search_result": _RECORDS}).encode("utf-8")

def test_post_retries_429_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    """S6: до правки адаптер падал на первом же 429 (0 ретраев в модуле)."""
    fake = _FakeHttpx(post=[_FakeResponse(429), _FakeResponse(200, _json_results())])
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = GlmSearchAdapter(sleep=sleeps.append)

    results = adapter.search(SearchQuery(text="neutral query"))

    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert len(fake.post_calls) == 2
    assert len(sleeps) == 1

def test_retry_after_seconds_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(
        post=[
            _FakeResponse(429, headers={"Retry-After": "7"}),
            _FakeResponse(200, _json_results()),
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = GlmSearchAdapter(sleep=sleeps.append)

    adapter.search(SearchQuery(text="neutral query"))

    assert sleeps == [7.0]

def test_non_retryable_status_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(post=[_FakeResponse(403)])
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = GlmSearchAdapter(sleep=sleeps.append)

    with pytest.raises(SearchError) as err:
        adapter.search(SearchQuery(text="neutral query"))

    assert "403" in str(err.value)
    assert len(fake.post_calls) == 1
    assert sleeps == []

def test_retry_exhaustion_raises_search_error(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(post=[_FakeResponse(503) for _ in range(MAX_RETRY_ATTEMPTS)])
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = GlmSearchAdapter(sleep=sleeps.append)

    with pytest.raises(SearchError) as err:
        adapter.search(SearchQuery(text="neutral query"))

    message = str(err.value)
    assert "503" in message
    assert str(MAX_RETRY_ATTEMPTS) in message
    assert len(fake.post_calls) == MAX_RETRY_ATTEMPTS
    assert len(sleeps) == MAX_RETRY_ATTEMPTS - 1

def test_transport_error_retried_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(
        post=[
            _FakeTransportError("connection reset"),
            _FakeResponse(200, _json_results()),
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = GlmSearchAdapter(sleep=sleeps.append)

    results = adapter.search(SearchQuery(text="neutral query"))

    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert len(fake.post_calls) == 2

def test_fetch_retries_transient_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """Фетч страницы идёт той же политикой: 503 не валит чтение с первой попытки."""
    fake = _FakeHttpx(
        get=[_FakeResponse(503), _FakeResponse(200, b"live page")]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = GlmSearchAdapter(sleep=sleeps.append)

    page = adapter.fetch(URL_A)

    assert page.content == b"live page"
    assert len(fake.get_calls) == 2

# ---------------------------------------------------------------------------
# R10: редиректы валидируются похопово (собственный клиент адаптера)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "internal",
    [
        "http://127.0.0.1:8080/admin",
        "http://10.0.0.1/internal",
        "http://169.254.169.254/latest/meta-data/",
    ],
)
def test_fetch_rejects_redirect_to_internal_address(
    monkeypatch: pytest.MonkeyPatch, internal: str
) -> None:
    """R10: редирект на внутренний адрес — типизированный отказ, запрос не выпущен."""
    fake = _FakeHttpx(get=[_FakeResponse(302, headers={"Location": internal})])
    _install_fake_httpx(monkeypatch, fake)
    adapter = GlmSearchAdapter(sleep=lambda _seconds: None)

    with pytest.raises(SearchError) as err:
        adapter.fetch(URL_A)

    assert "редирект на небезопасный адрес" in str(err.value)
    assert fake.get_calls == [URL_A]  # внутренний адрес НЕ запрошен


def test_fetch_forwards_public_redirect_and_keeps_final_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeHttpx(
        get=[
            _FakeResponse(307, headers={"Location": "/final"}),
            _FakeResponse(200, b"live page"),
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    adapter = GlmSearchAdapter(sleep=lambda _seconds: None)

    page = adapter.fetch(URL_A)

    assert fake.get_calls == [URL_A, "https://example.com/final"]
    assert page.content == b"live page"
    assert page.url == "https://example.com/final"


def test_fetch_redirect_loop_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Потолок хопов тот же, что у транспорта краула (дефолт ``httpx``)."""
    from core.util.redirects import DEFAULT_MAX_REDIRECTS

    fake = _FakeHttpx(
        get=[
            _FakeResponse(302, headers={"Location": "/loop"})
            for _ in range(DEFAULT_MAX_REDIRECTS + 1)
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    adapter = GlmSearchAdapter(sleep=lambda _seconds: None)

    with pytest.raises(SearchError) as err:
        adapter.fetch(URL_A)

    assert "редиректов" in str(err.value)
    assert len(fake.get_calls) == DEFAULT_MAX_REDIRECTS + 1


def test_redirect_refusal_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """Отказ SSRF не ретраибелен: повтор запроса к публичному URL не поможет."""
    fake = _FakeHttpx(get=[_FakeResponse(302, headers={"Location": "http://10.0.0.1/"})])
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = GlmSearchAdapter(sleep=sleeps.append)

    with pytest.raises(SearchError):
        adapter.fetch(URL_A)

    assert sleeps == []
    assert fake.get_calls == [URL_A]

# ---------------------------------------------------------------------------
# I19 структурно: модуль не ссылается на доказательства/ячейки/хранилище
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Нейтральность модуля (§1.3)
# ---------------------------------------------------------------------------


