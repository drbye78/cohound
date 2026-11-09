"""Тесты HTTP-клиента реестров (``_RegistryHttpClient``): R10, похоповая валидация.

Реестровый клиент — один из трёх путей, которые держали собственный ``httpx`` и обходили
транспорт (два других — ``fetch`` поисковых адаптеров). До правки ``follow_redirects=True``
означал, что недоверенный ответ реестра мог увести запрос на ``127.0.0.1``/``10.0.0.1``/
metadata-эндпоинт, а SSRF-проверка видела бы уже готовый ответ. Теперь каждый ``Location``
валидируется **до** запроса, отказ — ``RegistryFetchError`` с причиной.

Сеть не используется: ``httpx`` подменяется фейком через ``_import_http`` (шов модуля);
шов ``registry_http_get`` здесь не нужен — он подменяет клиент **целиком**, а проверяется
как раз клиент по умолчанию.

Фикстуры категорийно-нейтральны: ``registry.example``, ``Example``.
"""

from __future__ import annotations

import socket
from typing import Any

import pytest

from core.adapters.registry_generic import RegistryFetchError
from core.cli.wiring import _RegistryHttpClient


def _host_resolver(mapping: dict[str, str]):
    """Резолвер-заглушка: имя → адрес; неизвестное имя — публичный адрес. Сети нет."""

    def resolve(hostname: str) -> list[tuple[int, int, int, str, tuple[Any, ...]]]:
        address = mapping.get(hostname, "93.184.216.34")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 0))]

    return resolve


URL = "https://registry.example/api/search/x"
INTERNAL_URLS = (
    "http://127.0.0.1:8080/admin",
    "http://10.0.0.1/internal",
    "http://169.254.169.254/latest/meta-data/",
)

class _FakeResponse:
    """Минимальный ответ httpx: статус, тело, заголовки."""

    def __init__(
        self,
        status_code: int,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

class _FakeHttpx:
    """Фейковый модуль ``httpx``: очередь ответов и запись вызовов."""

    def __init__(self, responses: list[Any]) -> None:
        self._queue = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append((url, dict(kwargs)))
        if not self._queue:
            raise AssertionError("фейковый httpx: очередь ответов исчерпана")
        item = self._queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    @property
    def urls(self) -> list[str]:
        return [url for url, _kwargs in self.calls]

def _install(monkeypatch: pytest.MonkeyPatch, fake: _FakeHttpx) -> None:
    monkeypatch.setattr(
        _RegistryHttpClient, "_import_http", staticmethod(lambda: fake)
    )

@pytest.mark.parametrize("internal", INTERNAL_URLS)
def test_redirect_to_internal_address_is_refused(
    monkeypatch: pytest.MonkeyPatch, internal: str
) -> None:
    """Типизированный отказ; запрос на внутренний адрес не выпущен."""
    fake = _FakeHttpx([_FakeResponse(302, headers={"Location": internal})])
    _install(monkeypatch, fake)
    client = _RegistryHttpClient()

    with pytest.raises(RegistryFetchError) as err:
        client.get(URL, {"Accept": "application/json"}, 5.0)

    assert "редирект на небезопасный адрес" in str(err.value)
    assert fake.urls == [URL]  # ни одного байта запроса внутрь периметра

def test_client_never_asks_httpx_to_follow_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Регресс-страж: ``follow_redirects=True`` больше не передаётся ни разу."""
    fake = _FakeHttpx([_FakeResponse(200, b'{"items": []}')])
    _install(monkeypatch, fake)

    _RegistryHttpClient().get(URL, {"Accept": "application/json"}, 5.0)

    assert fake.calls
    assert all(kwargs.get("follow_redirects") is False for _url, kwargs in fake.calls)

def test_public_redirect_is_followed(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeHttpx(
        [
            _FakeResponse(301, headers={"Location": "/moved"}),
            _FakeResponse(200, b'{"items": [{"name": "Example"}]}'),
        ]
    )
    _install(monkeypatch, fake)

    body = _RegistryHttpClient().get(URL, {"Accept": "application/json"}, 5.0)

    assert body == b'{"items": [{"name": "Example"}]}'
    assert fake.urls == [URL, "https://registry.example/moved"]  # относительный Location
    # Заголовки и таймаут едут на каждом хопе: хоп — обычный запрос, а не особый случай.
    assert all(kwargs["headers"] == {"Accept": "application/json"} for _u, kwargs in fake.calls)
    assert all(kwargs["timeout"] == 5.0 for _u, kwargs in fake.calls)

def test_redirect_to_dns_name_resolving_internally_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DNS-имя в ``Location``, резолвящееся в петлю, отклоняется при резолвере."""
    fake = _FakeHttpx(
        [_FakeResponse(302, headers={"Location": "http://internal.example/secret"})]
    )
    _install(monkeypatch, fake)
    client = _RegistryHttpClient(
        resolver=_host_resolver({"internal.example": "127.0.0.1"})
    )

    with pytest.raises(RegistryFetchError) as err:
        client.get(URL, {"Accept": "application/json"}, 5.0)

    assert "редирект на небезопасный адрес" in str(err.value)
    assert fake.urls == [URL]  # небезопасный хоп не запрошен


def test_public_dns_resolution_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Публичный ответ резолвера — редирект проходит."""
    fake = _FakeHttpx(
        [
            _FakeResponse(301, headers={"Location": "https://public.example/moved"}),
            _FakeResponse(200, b'{"items": [{"name": "Example"}]}'),
        ]
    )
    _install(monkeypatch, fake)

    body = _RegistryHttpClient(resolver=_host_resolver({})).get(URL, {}, 5.0)

    assert body == b'{"items": [{"name": "Example"}]}'
    assert fake.urls == [URL, "https://public.example/moved"]


def test_redirect_cap_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.util.redirects import DEFAULT_MAX_REDIRECTS

    fake = _FakeHttpx(
        [
            _FakeResponse(302, headers={"Location": "/loop"})
            for _ in range(DEFAULT_MAX_REDIRECTS + 1)
        ]
    )
    _install(monkeypatch, fake)

    with pytest.raises(RegistryFetchError) as err:
        _RegistryHttpClient().get(URL, {}, 5.0)

    assert "редиректов" in str(err.value)
    assert len(fake.urls) == DEFAULT_MAX_REDIRECTS + 1

def test_http_error_is_reported_as_registry_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeHttpx([_FakeResponse(403)])
    _install(monkeypatch, fake)

    with pytest.raises(RegistryFetchError):
        _RegistryHttpClient().get(URL, {}, 5.0)


@pytest.mark.parametrize("internal", INTERNAL_URLS)
def test_internal_entry_url_is_refused_before_any_request(
    monkeypatch: pytest.MonkeyPatch, internal: str
) -> None:
    """Входной URL проверяется клиентом, а не только цели редиректов.

    ``follow_validated_redirects`` стартовый URL по контракту не валидирует — без этой
    проверки первый запрос уходил бы непроверенным, хотя докстринг обещал обратное.
    """
    fake = _FakeHttpx([_FakeResponse(200, b"{}")])
    _install(monkeypatch, fake)

    with pytest.raises(RegistryFetchError) as err:
        _RegistryHttpClient().get(internal, {}, 5.0)

    assert "небезопасный URL" in str(err.value)
    assert fake.urls == []  # ни одного запроса


def test_entry_url_resolving_internally_is_refused_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """То же для DNS-имени: ``resolver`` делает проверку входного URL пост-резолвной."""
    fake = _FakeHttpx([_FakeResponse(200, b"{}")])
    _install(monkeypatch, fake)
    client = _RegistryHttpClient(
        resolver=_host_resolver({"registry.internal": "169.254.169.254"})
    )

    with pytest.raises(RegistryFetchError) as err:
        client.get("https://registry.internal/api", {}, 5.0)

    assert "небезопасный URL" in str(err.value)
    assert fake.urls == []

def test_redirect_without_location_is_a_plain_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``3xx`` без ``Location`` — не хоп (как у ``httpx``): ответ отдаётся как есть."""
    fake = _FakeHttpx([_FakeResponse(304, b"")])
    _install(monkeypatch, fake)

    assert _RegistryHttpClient().get(URL, {}, 5.0) == b""
    assert fake.urls == [URL]
