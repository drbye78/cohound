"""Юнит-тесты generic config-driven реестрового фетчера (W6/T6.1).

Проверяют: загрузку конфига, сборку фактов c ``source_url`` (R1) для JSON и HTML,
извлечение записей по ``records_path``/верхнеуровневому списку, использование
внедрённого HTTP-клиента (запись вызовов), поведение на битом/пустом ответе и
нейтральность исходника (§1.3). Сеть отсутствует — клиент полностью подставной.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

import pytest

from core.adapters.registry_generic import (
    GenericRegistryFetcher,
    RegistryConfigError,
    RegistryError,
    RegistryFetchError,
    RegistryParseError,
    load_registry_config,
)
from core.ports.registry import RegistryFetcherPort
from core.ports.types import ProductRef

FIXTURES = Path(__file__).parent / "fixtures"


def _product(product_id: str = "prod-a") -> ProductRef:
    return ProductRef(
        product_id=product_id,
        canonical_name="Example Name",
        vendor="Example Vendor",
        jurisdiction="XX",
    )


def _client(payload: bytes, calls: list[tuple[str, dict[str, str]]] | None = None):
    """Callable-клиент ``(url, headers) -> bytes``, записывающий вызовы."""

    def http_get(url: str, headers: dict[str, str]) -> bytes:
        if calls is not None:
            calls.append((url, dict(headers)))
        return payload

    return http_get


# ---------------------------------------------------------------------------
# Загрузка конфига
# ---------------------------------------------------------------------------


def test_load_config_from_path() -> None:
    cfg = load_registry_config(FIXTURES / "registry_json.yaml")
    assert cfg.registry_id == "registry-x"
    assert cfg.jurisdiction == "XX"
    assert cfg.format == "json"
    assert cfg.method == "GET"
    assert cfg.records_path == "items"
    assert cfg.query_param == "q"
    assert cfg.mapping == {"name": "name", "version": "ver"}


def test_load_config_rejects_unknown_field() -> None:
    with pytest.raises(RegistryConfigError):
        load_registry_config(
            {
                "registry_id": "r1",
                "jurisdiction": "XX",
                "endpoint": "https://data.example.test/{product_id}",
                "format": "json",
                "mapping": {"a": "b"},
                "unexpected": 1,
            }
        )


def test_load_config_rejects_empty_mapping() -> None:
    with pytest.raises(RegistryConfigError):
        load_registry_config(
            {
                "registry_id": "r1",
                "jurisdiction": "XX",
                "endpoint": "https://data.example.test/{product_id}",
                "format": "json",
                "mapping": {},
            }
        )


def test_load_config_rejects_non_get_method() -> None:
    with pytest.raises(RegistryConfigError):
        load_registry_config(
            {
                "registry_id": "r1",
                "jurisdiction": "XX",
                "endpoint": "https://data.example.test/{product_id}",
                "format": "json",
                "mapping": {"a": "b"},
                "method": "POST",
            }
        )


def test_load_config_rejects_bad_format() -> None:
    with pytest.raises(RegistryConfigError):
        load_registry_config(
            {
                "registry_id": "r1",
                "jurisdiction": "XX",
                "endpoint": "https://data.example.test/{product_id}",
                "format": "xml",
                "mapping": {"a": "b"},
            }
        )


# ---------------------------------------------------------------------------
# JSON-контур
# ---------------------------------------------------------------------------


def test_json_fetch_builds_facts_with_source_url() -> None:
    cfg = load_registry_config(FIXTURES / "registry_json.yaml")
    calls: list[tuple[str, dict[str, str]]] = []
    payload = (FIXTURES / "payload_json.json").read_bytes()

    fetcher = GenericRegistryFetcher(cfg, _client(payload, calls), clock=lambda: "FIXED_TS")
    facts = fetcher.fetch(_product("prod-a"), {"run_id": "run-1"})

    # Записи из records_path=items -> по факту на запись.
    assert len(facts) == 2
    assert [json.loads(f.fact_json) for f in facts] == [
        {"name": "a", "version": "1"},
        {"name": "b", "version": "2"},
    ]
    # R1: каждый факт несёт непустой source_url; он же — URL запроса.
    assert calls, "внедрённый HTTP-клиент обязан быть вызван"
    request_url = calls[0][0]
    for fact in facts:
        assert fact.source_url
        assert fact.source_url == request_url
        assert fact.product_id == "prod-a"
        assert fact.registry_id == "registry-x"
        assert fact.run_id == "run-1"
        assert fact.fetched_at == "FIXED_TS"
    # Шаблон {product_id} подставлен, query_param добавлен.
    assert "/registry/prod-a" in request_url
    assert "q=Example" in request_url


def test_json_headers_are_passed_to_client() -> None:
    cfg = load_registry_config(
        {
            "registry_id": "r1",
            "jurisdiction": "XX",
            "endpoint": "https://data.example.test/{product_id}",
            "format": "json",
            "mapping": {"name": "name"},
            "headers": {"Accept": "application/json"},
        }
    )
    calls: list[tuple[str, dict[str, str]]] = []
    fetcher = GenericRegistryFetcher(cfg, _client(b'{"name": "x"}', calls))
    fetcher.fetch(_product(), {})
    assert calls[0][1] == {"Accept": "application/json"}


def test_json_defaults_to_top_level_list() -> None:
    cfg = load_registry_config(
        {
            "registry_id": "r1",
            "jurisdiction": "XX",
            "endpoint": "https://data.example.test/{product_id}",
            "format": "json",
            "mapping": {"name": "name", "version": "ver"},
        }
    )
    payload = b'[{"name": "x", "ver": "9"}, {"name": "y", "ver": "10"}]'
    facts = GenericRegistryFetcher(cfg, _client(payload)).fetch(_product(), {})
    assert [json.loads(f.fact_json) for f in facts] == [
        {"name": "x", "version": "9"},
        {"name": "y", "version": "10"},
    ]


def test_json_nested_record_path_and_wildcard() -> None:
    cfg = load_registry_config(
        {
            "registry_id": "r1",
            "jurisdiction": "XX",
            "endpoint": "https://data.example.test/{product_id}",
            "format": "json",
            "records_path": "data.records[*]",
            "mapping": {"name": "name"},
        }
    )
    payload = b'{"data": {"records": [{"name": "n1"}, {"name": "n2"}]}}'
    facts = GenericRegistryFetcher(cfg, _client(payload)).fetch(_product(), {})
    assert [json.loads(f.fact_json) for f in facts] == [{"name": "n1"}, {"name": "n2"}]


def test_json_missing_mapped_field_is_omitted() -> None:
    cfg = load_registry_config(
        {
            "registry_id": "r1",
            "jurisdiction": "XX",
            "endpoint": "https://data.example.test/{product_id}",
            "format": "json",
            "records_path": "items",
            "mapping": {"name": "name", "version": "ver"},
        }
    )
    payload = b'{"items": [{"name": "only-name"}]}'
    facts = GenericRegistryFetcher(cfg, _client(payload)).fetch(_product(), {})
    assert json.loads(facts[0].fact_json) == {"name": "only-name"}


# ---------------------------------------------------------------------------
# HTML-контур
# ---------------------------------------------------------------------------


def test_html_fetch_builds_facts_with_mapping() -> None:
    cfg = load_registry_config(FIXTURES / "registry_html.yaml")
    payload = (FIXTURES / "payload_html.html").read_bytes()
    facts = GenericRegistryFetcher(cfg, _client(payload)).fetch(_product("prod-h"), {})

    assert len(facts) == 2
    assert [json.loads(f.fact_json) for f in facts] == [
        {"name": "alpha", "version": "1", "row_id": "r1"},
        {"name": "beta", "version": "2", "row_id": "r2"},
    ]
    for fact in facts:
        assert fact.source_url  # R1
        assert "/list/prod-h" in fact.source_url


def test_html_empty_body_returns_no_facts() -> None:
    cfg = load_registry_config(FIXTURES / "registry_html.yaml")
    facts = GenericRegistryFetcher(cfg, _client(b"")).fetch(_product(), {})
    assert facts == []


# ---------------------------------------------------------------------------
# Ошибочные сценарии
# ---------------------------------------------------------------------------


def test_malformed_json_raises_parse_error() -> None:
    cfg = load_registry_config(FIXTURES / "registry_json.yaml")
    fetcher = GenericRegistryFetcher(cfg, _client(b"{not valid json"))
    with pytest.raises(RegistryParseError):
        fetcher.fetch(_product(), {})


def test_empty_json_body_raises_parse_error() -> None:
    cfg = load_registry_config(FIXTURES / "registry_json.yaml")
    with pytest.raises(RegistryParseError):
        GenericRegistryFetcher(cfg, _client(b"")).fetch(_product(), {})


def test_empty_json_container_returns_no_facts() -> None:
    cfg = load_registry_config(
        {
            "registry_id": "r1",
            "jurisdiction": "XX",
            "endpoint": "https://data.example.test/{product_id}",
            "format": "json",
            "mapping": {"name": "name"},
        }
    )
    facts = GenericRegistryFetcher(cfg, _client(b"{}")).fetch(_product(), {})
    assert facts == []


def test_missing_records_path_raises_parse_error() -> None:
    cfg = load_registry_config(FIXTURES / "registry_json.yaml")  # records_path=items
    fetcher = GenericRegistryFetcher(cfg, _client(b'{"other": []}'))
    with pytest.raises(RegistryParseError):
        fetcher.fetch(_product(), {})


def test_transport_failure_wrapped_as_registry_error() -> None:
    cfg = load_registry_config(FIXTURES / "registry_json.yaml")

    def boom(url: str, headers: dict[str, str]) -> bytes:
        raise RuntimeError("network down")

    fetcher = GenericRegistryFetcher(cfg, boom)
    with pytest.raises(RegistryFetchError):
        fetcher.fetch(_product(), {})
    # Все ошибки адаптера — подклассы RegistryError (pipeline -> source_unavailable).
    with pytest.raises(RegistryError):
        fetcher.fetch(_product(), {})


def test_missing_template_placeholder_raises_config_error() -> None:
    cfg = load_registry_config(
        {
            "registry_id": "r1",
            "jurisdiction": "XX",
            "endpoint": "https://data.example.test/{unknown_key}",
            "format": "json",
            "mapping": {"name": "name"},
        }
    )
    fetcher = GenericRegistryFetcher(cfg, _client(b"[]"))
    with pytest.raises(RegistryConfigError):
        fetcher.fetch(_product(), {})


# ---------------------------------------------------------------------------
# Внедрение клиента-объекта с таймаутом
# ---------------------------------------------------------------------------


def test_http_client_object_receives_timeout() -> None:
    cfg = load_registry_config(
        {
            "registry_id": "r1",
            "jurisdiction": "XX",
            "endpoint": "https://data.example.test/{product_id}",
            "format": "json",
            "mapping": {"name": "name"},
            "timeout": 7.5,
        }
    )

    class ObjectClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, str], float | None]] = []

        def get(
            self, url: str, headers: Mapping[str, str], timeout: float | None = None
        ) -> bytes:
            self.calls.append((url, dict(headers), timeout))
            return b'[{"name": "x"}]'

    client = ObjectClient()
    facts = GenericRegistryFetcher(cfg, client).fetch(_product(), {})
    assert client.calls[0][2] == 7.5
    assert json.loads(facts[0].fact_json) == {"name": "x"}


# ---------------------------------------------------------------------------
# Контракт порта и нейтральность
# ---------------------------------------------------------------------------


