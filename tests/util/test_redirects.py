"""Тесты похоповой валидации редиректов (R10 + пост-резолвная проверка, S9g).

Без сети: ``fetch_hop`` — заготовленные ответы, резолвер — заглушка. Проверяется, что
имя, резолвящееся в не-глобальный адрес, отклоняется на редиректе при заданном резолвере,
публичное имя проходит, а ``resolver=None`` сохраняет прежнее (литеральное) поведение.
"""

from __future__ import annotations

import socket
from collections.abc import Iterable
from typing import Any

import pytest

from core.util.redirects import follow_validated_redirects

PUBLIC_IP = "93.184.216.34"
START = "https://example.com/start"


class _Response:
    """Минимальный ответ: статус и ``Location`` (форма ``httpx``)."""

    def __init__(self, status_code: int, location: str | None = None) -> None:
        self.status_code = status_code
        self.headers = {} if location is None else {"Location": location}


class _Error(RuntimeError):
    """Тип ошибки вызывающего (как ``SearchError``/``RegistryFetchError``)."""


def _resolver(mapping: dict[str, Iterable[str]]):
    """Резолвер по имени: ``mapping[hostname]`` — адреса; неизвестное имя — публичное."""

    def resolve(hostname: str) -> list[tuple[int, int, int, str, tuple[Any, ...]]]:
        addresses = list(mapping.get(hostname, [PUBLIC_IP]))
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 0))
            for address in addresses
        ]

    return resolve


def _fetch_hop(responses: list[_Response], calls: list[str]):
    def fetch(url: str) -> _Response:
        calls.append(url)
        index = min(len(calls) - 1, len(responses) - 1)
        return responses[index]

    return fetch


def test_dns_name_redirect_to_loopback_rejected_with_resolver() -> None:
    """Имя в ``Location``, резолвящееся в петлю, отклоняется до выпуска запроса."""
    calls: list[str] = []
    fetch_hop = _fetch_hop(
        [_Response(302, "http://internal.example/secret")], calls
    )
    resolver = _resolver({"internal.example": ["127.0.0.1"]})

    with pytest.raises(_Error, match="редирект на небезопасный адрес"):
        follow_validated_redirects(
            fetch_hop,
            START,
            error=_Error,
            label="тест",
            resolver=resolver,
        )

    assert calls == [START]  # небезопасный хоп не запрошен


def test_public_resolution_allows_redirect() -> None:
    """Публичный ответ резолвера — хоп проходит, финальный URL возвращается."""
    calls: list[str] = []
    fetch_hop = _fetch_hop(
        [_Response(301, "/final"), _Response(200)], calls
    )

    response, final_url = follow_validated_redirects(
        fetch_hop,
        START,
        error=_Error,
        label="тест",
        resolver=_resolver({}),
    )

    assert response.status_code == 200
    assert final_url == "https://example.com/final"
    assert calls == [START, "https://example.com/final"]


def test_mixed_resolution_is_rejected() -> None:
    """Публичный + приватный адрес в одном ответе — отказ целиком (fail closed)."""
    calls: list[str] = []
    fetch_hop = _fetch_hop(
        [_Response(302, "http://mixed.example/next")], calls
    )
    resolver = _resolver({"mixed.example": [PUBLIC_IP, "10.0.0.1"]})

    with pytest.raises(_Error, match="редирект на небезопасный адрес"):
        follow_validated_redirects(
            fetch_hop, START, error=_Error, label="тест", resolver=resolver
        )


def test_resolver_none_keeps_literal_only_behaviour() -> None:
    """Без резолвера DNS не спрашивается: имя проходит, литерал — по-прежнему нет."""
    calls: list[str] = []
    fetch_hop = _fetch_hop(
        [_Response(302, "http://internal.example/next"), _Response(200)], calls
    )

    response, final_url = follow_validated_redirects(
        fetch_hop, START, error=_Error, label="тест"
    )

    assert response.status_code == 200
    assert final_url == "http://internal.example/next"
    assert calls == [START, "http://internal.example/next"]


def test_resolver_none_still_rejects_literal_private_hop() -> None:
    """Литеральный приватный адрес закрыт и без резолвера (прежнее поведение)."""
    calls: list[str] = []
    fetch_hop = _fetch_hop([_Response(302, "http://10.0.0.1/secret")], calls)

    with pytest.raises(_Error, match="редирект на небезопасный адрес"):
        follow_validated_redirects(fetch_hop, START, error=_Error, label="тест")

    assert calls == [START]
