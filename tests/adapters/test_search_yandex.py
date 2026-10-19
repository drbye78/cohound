"""Тесты ``YandexSearchAdapter`` (W11/T11.1) — реализация ``SearchAdapterPort``.

Сеть не задействуется: HTTP-вызовы внедряются через швы ``http_post``/``http_get``/
``http_poll``, а ожидание — через ``sleep``. Покрываются: разбор кандидатов
(url/title/snippet/rank), уважение ``limit`` (симметрия бюджета — за вызывающим, I16),
``fetch`` с SSRF-валидацией и кэшем (попадание без сети, промах→попадание, ровно один
живой вызов, R7), ``available`` без ``httpx``, структурное соответствие порту,
I19-инвариант (модуль не ссылается на доказательства/ячейки/хранилище), категорийная
нейтральность (§1.3).

Отдельно покрыт **асинхронный режим** (дефолт): запуск ``searchAsync`` → опрос
``/operations/{id}`` → ``response.rawData`` (base64-XML), обработка ``error``
(**до** ``done``), таймаут ``max_wait``, чтение режима из ``YANDEX_SEARCH_MODE`` и
валидация режима/таймингов.

Фикстуры **категорийно-нейтральны**: example.com / «Example», без продуктов и вендоров.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import pytest

import core.adapters.search_yandex as module
from core.adapters.search_yandex import (
    BASE_RETRY_DELAY,
    DEFAULT_ASYNC_SEARCH_PATH,
    DEFAULT_BASE_URL,
    DEFAULT_MAX_WAIT,
    DEFAULT_OPERATION_BASE_URL,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_SEARCH_MODE,
    DEFAULT_SEARCH_PATH,
    ENV_SEARCH_MODE,
    MAX_RETRY_ATTEMPTS,
    MAX_RETRY_DELAY,
    SearchAdapterUnavailable,
    SearchError,
    YandexSearchAdapter,
)
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.types import RawPage, FetchMode
from core.util.cache import ContentAddressedCache, http_key

URL_A = "https://example.com/a"
URL_B = "https://example.com/b"
URL_C = "https://example.com/c"
PAGE = "https://example.com/page"

_RECORDS = [
    {"url": URL_A, "title": "Example A", "snippet": "first snippet", "rank": 1},
    {"url": URL_B, "title": "Example B", "snippet": "second snippet", "rank": 2},
    {"url": URL_C, "title": "Example C", "snippet": "third snippet", "rank": 3},
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


def _sync(**kwargs: Any) -> YandexSearchAdapter:
    """Адаптер в режиме ``sync`` — обратная совместимость прежних (не-async) тестов."""
    kwargs.setdefault("search_mode", "sync")
    return YandexSearchAdapter(**kwargs)


def _noop_sleep(_seconds: float) -> None:
    """No-op ожидание: тесты не спят реально."""


def _xml_two_docs() -> str:
    """base64-XML веб-поиска v2 с двумя ``doc`` (для async-ответа ``response.rawData``)."""
    xml = (
        "<yandexsearch><response><results><grouping><group>"
        "<doc><url>{a}</url><title>Example A</title>"
        "<passages><passage>alpha</passage><passage>beta</passage></passages></doc>"
        "<doc><url>{b}</url><title>Example B</title></doc>"
        "</group></grouping></results></response></yandexsearch>"
    ).format(a=URL_A, b=URL_B)
    return base64.b64encode(xml.encode("utf-8")).decode("ascii")


# ---------------------------------------------------------------------------
# Контракт порта / атрибуты
# ---------------------------------------------------------------------------


def test_class_attributes() -> None:
    adapter = YandexSearchAdapter(http_post=_post_returning({"results": []}))
    assert adapter.adapter_kind == "serp"
    assert adapter.provider == "yandex"


def test_defaults_and_endpoint() -> None:
    adapter = YandexSearchAdapter()
    assert adapter.base_url == DEFAULT_BASE_URL
    assert adapter.search_path == DEFAULT_SEARCH_PATH
    assert adapter.endpoint == f"{DEFAULT_BASE_URL}{DEFAULT_SEARCH_PATH}"
    assert adapter.async_search_path == DEFAULT_ASYNC_SEARCH_PATH
    assert adapter.async_endpoint == f"{DEFAULT_BASE_URL}{DEFAULT_ASYNC_SEARCH_PATH}"
    assert adapter.operation_base_url == DEFAULT_OPERATION_BASE_URL
    assert adapter.search_mode == DEFAULT_SEARCH_MODE == "async"
    assert adapter.poll_interval == DEFAULT_POLL_INTERVAL
    assert adapter.max_wait == DEFAULT_MAX_WAIT


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        YandexSearchAdapter(base_url="")
    with pytest.raises(ValueError):
        YandexSearchAdapter(search_path="v2/web/search")
    with pytest.raises(ValueError):
        YandexSearchAdapter(async_search_path="v2/web/searchAsync")
    with pytest.raises(ValueError):
        YandexSearchAdapter(operation_base_url="")
    with pytest.raises(ValueError):
        YandexSearchAdapter(timeout=0)
    with pytest.raises(ValueError):
        YandexSearchAdapter(search_mode="bogus")
    with pytest.raises(ValueError):
        YandexSearchAdapter(poll_interval=0)
    with pytest.raises(ValueError):
        YandexSearchAdapter(max_wait=0)


# ---------------------------------------------------------------------------
# search: разбор кандидатов
# ---------------------------------------------------------------------------


def test_search_parses_candidates() -> None:
    adapter = _sync(http_post=_post_returning({"results": _RECORDS}))
    results = adapter.search(SearchQuery(text="neutral query", limit=10))

    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert [r.title for r in results] == ["Example A", "Example B", "Example C"]
    assert [r.snippet for r in results] == ["first snippet", "second snippet", "third snippet"]
    assert [r.rank for r in results] == [1, 2, 3]
    assert all(isinstance(r, SearchResult) for r in results)


def test_search_returns_candidates_without_page_bytes() -> None:
    # I19: SERP отдаёт кандидатов; байты страницы берёт fetch, не search.
    adapter = _sync(http_post=_post_returning({"results": _RECORDS}))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert all(r.raw_bytes is None for r in results)


def test_search_result_has_no_evidence_members() -> None:
    adapter = _sync(http_post=_post_returning({"results": _RECORDS}))
    result = adapter.search(SearchQuery(text="neutral query"))[0]
    for banned in ("source_id", "citation_quote", "content_hash", "evidence"):
        assert not hasattr(result, banned)


def test_search_accepts_alt_field_names() -> None:
    records = [{"link": URL_A, "title": "Example A", "content": "body text"}]
    adapter = _sync(http_post=_post_returning({"items": records}))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert results[0].url == URL_A
    assert results[0].snippet == "body text"
    assert results[0].rank == 1  # позиция, т.к. ранг не задан


def test_search_parses_rawdata_xml() -> None:
    xml = (
        "<yandexsearch><response><results><grouping><group>"
        "<doc><url>{a}</url><title>Example A</title>"
        "<passages><passage>alpha</passage><passage>beta</passage></passages></doc>"
        "<doc><url>{b}</url><title>Example B</title></doc>"
        "</group></grouping></results></response></yandexsearch>"
    ).format(a=URL_A, b=URL_B)
    raw_data = base64.b64encode(xml.encode("utf-8")).decode("ascii")
    adapter = _sync(http_post=_post_returning({"rawData": raw_data}))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A, URL_B]
    assert results[0].snippet == "alpha beta"


def test_search_unknown_shape_is_empty_not_error() -> None:
    adapter = _sync(http_post=_post_returning({"unexpected": True}))
    assert adapter.search(SearchQuery(text="neutral query")) == []


def test_search_records_without_url_skipped() -> None:
    payload = {"results": [{"title": "no url"}, _RECORDS[0]]}
    adapter = _sync(http_post=_post_returning(payload))
    results = adapter.search(SearchQuery(text="neutral query"))
    assert [r.url for r in results] == [URL_A]


# ---------------------------------------------------------------------------
# search: бюджет / лимит (I16 — симметрия за вызывающим)
# ---------------------------------------------------------------------------


def test_search_respects_limit() -> None:
    adapter = _sync(http_post=_post_returning({"results": _RECORDS * 4}))
    results = adapter.search(SearchQuery(text="neutral query", limit=5))
    assert len(results) == 5


def test_search_limit_sent_to_provider() -> None:
    captured: dict[str, Any] = {}

    def _post(url: str, body: dict[str, Any]) -> bytes:
        captured.update(body)
        return json.dumps({"results": _RECORDS}).encode("utf-8")

    adapter = _sync(http_post=_post)
    adapter.search(SearchQuery(text="neutral query", limit=7))
    assert captured["groupSpec"]["groupsOnPage"] == "7"
    assert captured["query"]["queryText"] == "neutral query"


def test_search_limit_zero_returns_empty_without_http() -> None:
    def _boom(url: str, body: dict[str, Any]) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("HTTP не должен вызываться при limit<=0")

    adapter = YandexSearchAdapter(http_post=_boom)
    assert adapter.search(SearchQuery(text="neutral query", limit=0)) == []


def test_search_empty_query_rejected() -> None:
    adapter = _sync(http_post=_post_returning({"results": []}))
    with pytest.raises(ValueError):
        adapter.search(SearchQuery(text="   "))


def test_sync_mode_still_works_explicitly() -> None:
    """Явный ``search_mode="sync"`` сохраняет прежний путь POST→decode→parse."""
    adapter = YandexSearchAdapter(
        http_post=_post_returning({"results": _RECORDS}), search_mode="sync"
    )
    assert adapter.search_mode == "sync"
    results = adapter.search(SearchQuery(text="neutral query", limit=2))
    assert [r.url for r in results] == [URL_A, URL_B]


# ---------------------------------------------------------------------------
# Асинхронный режим (дефолт): запуск → опрос → response.rawData
# ---------------------------------------------------------------------------


def test_async_search_happy_path() -> None:
    raw_data = _xml_two_docs()
    posts: list[str] = []
    polls: list[str] = []
    sleeps: list[float] = []

    def _post(url: str, body: dict[str, Any]) -> bytes:
        posts.append(url)
        return json.dumps({"done": False, "id": "op1", "description": "pending"}).encode()

    def _poll(url: str) -> bytes:
        polls.append(url)
        if len(polls) == 1:
            return json.dumps({"done": False}).encode()
        return json.dumps({"done": True, "response": {"rawData": raw_data}}).encode()

    adapter = YandexSearchAdapter(
        http_post=_post, http_poll=_poll, sleep=sleeps.append
    )
    assert adapter.search_mode == "async"
    results = adapter.search(SearchQuery(text="neutral query", limit=10))

    assert [r.url for r in results] == [URL_A, URL_B]
    assert [r.title for r in results] == ["Example A", "Example B"]
    assert [r.rank for r in results] == [1, 2]
    assert results[0].snippet == "alpha beta"
    assert all(r.raw_bytes is None for r in results)  # I19
    assert posts == [f"{DEFAULT_BASE_URL}{DEFAULT_ASYNC_SEARCH_PATH}"]
    assert polls == [f"{DEFAULT_OPERATION_BASE_URL}/operations/op1"] * 2
    assert sleeps == [DEFAULT_POLL_INTERVAL]


def test_async_search_error_checked_before_done() -> None:
    polls: list[str] = []

    def _post(url: str, body: dict[str, Any]) -> bytes:
        return json.dumps({"done": False, "id": "op2"}).encode()

    def _poll(url: str) -> bytes:
        polls.append(url)
        # ``error`` приходит при ``done:false`` — должен сработать раньше проверки done.
        return json.dumps({"error": {"code": 7, "message": "denied"}}).encode()

    adapter = YandexSearchAdapter(
        http_post=_post, http_poll=_poll, sleep=_noop_sleep
    )
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))
    assert len(polls) == 1  # не ушли в бесконечный цикл


def test_async_search_timeout() -> None:
    def _post(url: str, body: dict[str, Any]) -> bytes:
        return json.dumps({"done": False, "id": "op3"}).encode()

    def _poll(url: str) -> bytes:
        return json.dumps({"done": False}).encode()

    adapter = YandexSearchAdapter(
        http_post=_post,
        http_poll=_poll,
        sleep=_noop_sleep,
        poll_interval=0.01,
        max_wait=0.05,
    )
    with pytest.raises(SearchError, match="превышено время ожидания"):
        adapter.search(SearchQuery(text="neutral query"))


def test_async_search_missing_operation_id_raises() -> None:
    def _post(url: str, body: dict[str, Any]) -> bytes:
        return json.dumps({"done": False, "description": "no id"}).encode()

    adapter = YandexSearchAdapter(
        http_post=_post, http_poll=_noop_poll, sleep=_noop_sleep
    )
    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))


def _noop_poll(url: str) -> bytes:  # pragma: no cover - не должен вызываться
    """Заглушка опроса: при отсутствии operationId поллинг не стартует."""
    raise AssertionError("опрос не должен вызываться без operationId")


# ---------------------------------------------------------------------------
# Режим: env-переопределение и валидация
# ---------------------------------------------------------------------------


def test_env_search_mode_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_SEARCH_MODE, "sync")
    assert YandexSearchAdapter().search_mode == "sync"


def test_env_search_mode_async(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_SEARCH_MODE, "async")
    assert YandexSearchAdapter().search_mode == "async"


def test_env_search_mode_garbage_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_SEARCH_MODE, "bogus")
    with pytest.raises(ValueError):
        YandexSearchAdapter()


def test_explicit_search_mode_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ENV_SEARCH_MODE, "async")
    assert YandexSearchAdapter(search_mode="sync").search_mode == "sync"


def test_search_mode_validation_rejects_non_async_sync() -> None:
    with pytest.raises(ValueError):
        YandexSearchAdapter(search_mode="parallel")


def test_timing_args_validation() -> None:
    with pytest.raises(ValueError):
        YandexSearchAdapter(poll_interval=-1)
    with pytest.raises(ValueError):
        YandexSearchAdapter(max_wait=-1)


# ---------------------------------------------------------------------------
# fetch: SSRF, RawPage, кэш (R7)
# ---------------------------------------------------------------------------


def test_fetch_returns_rawpage_from_seam() -> None:
    adapter = YandexSearchAdapter(http_get=lambda url: b"<html>page</html>")
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

    adapter = YandexSearchAdapter(http_get=_get)
    with pytest.raises(ValueError):
        adapter.fetch("http://127.0.0.1/private")
    assert calls == []


def test_fetch_cache_hit_avoids_http_get(tmp_path: Path) -> None:
    cache = ContentAddressedCache(root=tmp_path)
    cache.put_bytes("http", http_key(URL_A), b"cached page")

    def _boom(url: str) -> bytes:  # pragma: no cover - не должен вызваться
        raise AssertionError("http_get не должен вызываться при попадании в кэш")

    adapter = YandexSearchAdapter(http_get=_boom, cache=cache)
    page = adapter.fetch(URL_A)
    assert page.content == b"cached page"


def test_fetch_cache_miss_then_hit_calls_http_once(tmp_path: Path) -> None:
    cache = ContentAddressedCache(root=tmp_path)
    calls: list[str] = []

    def _get(url: str) -> bytes:
        calls.append(url)
        return b"live page"

    adapter = YandexSearchAdapter(http_get=_get, cache=cache)
    first = adapter.fetch(URL_A)
    second = adapter.fetch(URL_A)

    assert first.content == b"live page"
    assert second.content == b"live page"
    assert calls == [URL_A]  # ровно один живой вызов; второй — из кэша


def test_fetch_without_cache_calls_http_each_time() -> None:
    calls: list[str] = []
    adapter = YandexSearchAdapter(http_get=lambda url: (calls.append(url), b"x")[1])
    adapter.fetch(URL_A)
    adapter.fetch(URL_A)
    assert calls == [URL_A, URL_A]


# ---------------------------------------------------------------------------
# available / отсутствие зависимости
# ---------------------------------------------------------------------------


def test_available_with_seam_without_httpx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    with_seam = YandexSearchAdapter(http_post=_post_returning({"results": []}))
    without_seam = YandexSearchAdapter()
    assert with_seam.available is True
    assert without_seam.available is False


def test_available_reflects_httpx() -> None:
    assert YandexSearchAdapter().available is module._httpx_available()


def test_missing_dependency_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    adapter = YandexSearchAdapter()
    with pytest.raises(SearchAdapterUnavailable):
        adapter.search(SearchQuery(text="neutral query"))
    with pytest.raises(SearchAdapterUnavailable):
        adapter.fetch(URL_A)
    assert issubclass(SearchAdapterUnavailable, RuntimeError)


# ---------------------------------------------------------------------------
# I19 структурно: модуль не ссылается на доказательства/ячейки/хранилище
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Нейтральность модуля (§1.3)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Retry/backoff реального HTTP (429/5xx/TransportError)
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
    monkeypatch.setattr(
        YandexSearchAdapter, "_import_http", staticmethod(lambda: fake)
    )


def _json_results() -> bytes:
    return json.dumps({"results": _RECORDS}).encode("utf-8")


def test_post_retries_429_and_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(post=[_FakeResponse(429), _FakeResponse(200, _json_results())])
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    results = adapter.search(SearchQuery(text="neutral query"))

    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert len(fake.post_calls) == 2
    assert len(sleeps) == 1
    assert BASE_RETRY_DELAY <= sleeps[0] <= BASE_RETRY_DELAY * 1.5


def test_retry_after_seconds_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(
        post=[
            _FakeResponse(429, headers={"Retry-After": "7"}),
            _FakeResponse(200, _json_results()),
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    adapter.search(SearchQuery(text="neutral query"))

    assert sleeps == [7.0]
    assert len(fake.post_calls) == 2


def test_retry_after_http_date_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    when = datetime.now(timezone.utc) + timedelta(seconds=30)
    retry_after = format_datetime(when, usegmt=True)
    fake = _FakeHttpx(
        post=[
            _FakeResponse(429, headers={"Retry-After": retry_after}),
            _FakeResponse(200, _json_results()),
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    adapter.search(SearchQuery(text="neutral query"))

    assert len(sleeps) == 1
    assert 25.0 <= sleeps[0] <= 31.0


def test_retry_after_capped_at_max_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(
        post=[
            _FakeResponse(429, headers={"Retry-After": "3600"}),
            _FakeResponse(200, _json_results()),
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    adapter.search(SearchQuery(text="neutral query"))

    assert sleeps == [MAX_RETRY_DELAY]


def test_403_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(post=[_FakeResponse(403)])
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    with pytest.raises(SearchError) as err:
        adapter.search(SearchQuery(text="neutral query"))

    message = str(err.value)
    assert "403" in message
    assert "1" in message
    assert len(fake.post_calls) == 1
    assert sleeps == []


def test_retry_exhaustion_reports_status_and_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeHttpx(
        post=[_FakeResponse(503) for _ in range(MAX_RETRY_ATTEMPTS)]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    with pytest.raises(SearchError) as err:
        adapter.search(SearchQuery(text="neutral query"))

    message = str(err.value)
    assert "503" in message
    assert str(MAX_RETRY_ATTEMPTS) in message
    assert len(fake.post_calls) == MAX_RETRY_ATTEMPTS
    assert len(sleeps) == MAX_RETRY_ATTEMPTS - 1


def test_backoff_delays_grow(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(
        post=[_FakeResponse(500) for _ in range(MAX_RETRY_ATTEMPTS)]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    with pytest.raises(SearchError):
        adapter.search(SearchQuery(text="neutral query"))

    assert len(sleeps) == MAX_RETRY_ATTEMPTS - 1
    assert all(later > earlier for earlier, later in zip(sleeps, sleeps[1:]))
    assert max(sleeps) <= MAX_RETRY_DELAY


def test_transport_error_retried_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeHttpx(
        post=[_FakeTransportError("connection reset"), _FakeResponse(200, _json_results())]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []
    adapter = YandexSearchAdapter(search_mode="sync", sleep=sleeps.append)

    results = adapter.search(SearchQuery(text="neutral query"))

    assert [r.url for r in results] == [URL_A, URL_B, URL_C]
    assert len(fake.post_calls) == 2
    assert len(sleeps) == 1


def test_poll_retries_429_then_done(monkeypatch: pytest.MonkeyPatch) -> None:
    done = json.dumps(
        {"done": True, "response": {"rawData": _xml_two_docs()}}
    ).encode("utf-8")
    fake = _FakeHttpx(
        get=[
            _FakeResponse(429, headers={"Retry-After": "2"}),
            _FakeResponse(200, done),
        ]
    )
    _install_fake_httpx(monkeypatch, fake)
    sleeps: list[float] = []

    def _post(url: str, body: dict[str, Any]) -> bytes:
        return json.dumps({"done": False, "id": "op-retry"}).encode("utf-8")

    adapter = YandexSearchAdapter(http_post=_post, sleep=sleeps.append)

    results = adapter.search(SearchQuery(text="neutral query"))

    assert [r.url for r in results] == [URL_A, URL_B]
    assert len(fake.get_calls) == 2
    assert fake.get_calls[0] == f"{DEFAULT_OPERATION_BASE_URL}/operations/op-retry"
    assert sleeps == [2.0]


