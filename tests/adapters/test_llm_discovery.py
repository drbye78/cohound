"""Тесты ``DiscoveryLLM`` (W14) — реализация ``DiscoveryLLMPort`` для разведки паспорта.

Сеть/ключи не требуются: генерация внедряется через шов ``generate``. Реальный
HTTP-путь проверяется без сокета — через ``respx``. Покрываются: разбор валидного
выхода, ``decoder``/``prompt_hash``/``usage``, «JSON в прозе» (fallback-вырезание
сбалансированного объекта), битый JSON → ошибка, ``available``, контракт отсутствия
зависимости, структурный I18, конформность порту и категорийная нейтральность (§1.3).
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import BaseModel, Field

import core.adapters.llm_discovery as module
from core.adapters.llm_discovery import (
    DEFAULT_DECODER,
    DEFAULT_MAX_TOKENS,
    DEFAULT_TIMEOUT,
    DiscoveryLLM,
    DiscoveryLLMError,
    DiscoveryLLMUnavailable,
)
from core.ports.discovery_llm import (
    DiscoveryLLMPort,
    DiscoveryLLMRequest,
    DiscoveryLLMResponse,
)
from core.ports.llm import TokenUsage
from core.util.hashing import content_hash

httpx = pytest.importorskip("httpx")
respx = pytest.importorskip("respx")

PROMPT = "продукт + [source: https://docs.test/x] данные страницы"
BASE_URL = "http://localhost:8000/v1"
ENDPOINT = f"{BASE_URL}/chat/completions"


class Hyp(BaseModel):
    """Минимальная схема гипотезы для тестов адаптера (permissive)."""

    name: str | None = None
    items: list[str] = Field(default_factory=list)


def _valid_json() -> str:
    return json.dumps({"name": "Alpha", "items": ["a", "b"]}, ensure_ascii=False)


def _req(**overrides: Any) -> DiscoveryLLMRequest:
    data: dict[str, Any] = {
        "prompt": PROMPT,
        "response_schema": Hyp,
        "model_id": "test-model",
    }
    data.update(overrides)
    return DiscoveryLLMRequest(**data)


# ---------------------------------------------------------------------------
# Базовый контракт / атрибуты
# ---------------------------------------------------------------------------


def test_defaults() -> None:
    llm = DiscoveryLLM(generate=lambda p, s: _valid_json())
    assert llm.decoder == DEFAULT_DECODER
    assert llm.max_tokens == DEFAULT_MAX_TOKENS
    assert llm.timeout == DEFAULT_TIMEOUT
    assert llm.available is True


def test_attributes_are_overridable() -> None:
    llm = DiscoveryLLM(
        BASE_URL,
        api_key="k",
        model_id="vendor/model",
        decoder="json_schema",
        timeout=5.0,
        max_tokens=64,
        generate=lambda p, s: _valid_json(),
    )
    assert llm.base_url == BASE_URL
    assert llm.api_key == "k"
    assert llm.model_id == "vendor/model"
    assert llm.decoder == "json_schema"
    assert llm.timeout == 5.0
    assert llm.max_tokens == 64


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        DiscoveryLLM(timeout=0)
    with pytest.raises(ValueError):
        DiscoveryLLM(max_tokens=0)


def test_implements_discovery_port() -> None:
    llm = DiscoveryLLM(generate=lambda p, s: _valid_json())
    assert isinstance(llm, DiscoveryLLMPort)


# ---------------------------------------------------------------------------
# Разбор валидного выхода через шов generate
# ---------------------------------------------------------------------------


def test_complete_parses_valid_output() -> None:
    seen: list[tuple[str, type]] = []

    def generate(prompt: str, schema: type) -> str:
        seen.append((prompt, schema))
        return _valid_json()

    resp = DiscoveryLLM(generate=generate).complete(_req())

    assert seen == [(PROMPT, Hyp)]
    assert isinstance(resp, DiscoveryLLMResponse)
    assert isinstance(resp.output, Hyp)
    assert resp.output.name == "Alpha"
    assert resp.output.items == ["a", "b"]
    assert resp.raw == _valid_json()
    assert resp.prompt_hash == content_hash(PROMPT)
    assert resp.usage == TokenUsage()


def test_decoder_is_echoed() -> None:
    llm = DiscoveryLLM(generate=lambda p, s: _valid_json(), decoder="json_schema")
    assert llm.complete(_req()).decoder == "json_schema"


def test_json_in_prose_is_extracted() -> None:
    prose = f"Вот гипотеза:\n```json\n{_valid_json()}\n```\nСпасибо."
    resp = DiscoveryLLM(generate=lambda p, s: prose).complete(_req())
    assert resp.output.name == "Alpha"
    assert resp.raw == prose


def test_nested_json_with_braces_in_strings_is_extracted() -> None:
    payload = json.dumps({"name": 'a {b} "c"', "items": ["}"]})
    prose = f"prefix {payload} suffix"
    resp = DiscoveryLLM(generate=lambda p, s: prose).complete(_req())
    assert resp.output.name == 'a {b} "c"'
    assert resp.output.items == ["}"]


def test_broken_json_raises() -> None:
    with pytest.raises(DiscoveryLLMError):
        DiscoveryLLM(generate=lambda p, s: "{not json").complete(_req())


def test_non_base_model_schema_raises() -> None:
    with pytest.raises(DiscoveryLLMError):
        DiscoveryLLM(generate=lambda p, s: "{}").complete(_req(response_schema=dict))


def test_discovery_error_is_runtime_error() -> None:
    assert issubclass(DiscoveryLLMError, RuntimeError)
    assert issubclass(DiscoveryLLMUnavailable, DiscoveryLLMError)


# ---------------------------------------------------------------------------
# Реальный HTTP-путь (перехват respx; сеть не задействуется)
# ---------------------------------------------------------------------------


def _http_body(content: str | None = None, usage: dict[str, int] | None = None) -> dict[str, Any]:
    return {
        "choices": [{"message": {"role": "assistant", "content": content or _valid_json()}}],
        "usage": usage or {"prompt_tokens": 10, "completion_tokens": 5},
    }


def test_real_path_parses_response_and_usage() -> None:
    with respx.mock(assert_all_called=True) as mock:
        route = mock.post(ENDPOINT).mock(return_value=httpx.Response(200, json=_http_body()))
        resp = DiscoveryLLM(BASE_URL, model_id="m", decoder="json_object").complete(_req())

    assert route.called
    assert resp.output.name == "Alpha"
    assert resp.usage == TokenUsage(prompt_tokens=10, completion_tokens=5)


def test_real_path_omits_response_format_when_decoder_none() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        DiscoveryLLM(BASE_URL, model_id="m", decoder="none").complete(_req())

    assert "response_format" not in captured
    assert "tools" not in captured
    assert "web_search" not in captured
    # reasoning-модель: дефолт с запасом, а не 2048 (запрос с max_tokens=0 = дефолт адаптера).
    assert captured["max_tokens"] == DEFAULT_MAX_TOKENS
    assert DEFAULT_MAX_TOKENS >= 4096


def test_real_path_requests_json_schema_when_constrained() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        DiscoveryLLM(BASE_URL, model_id="m", decoder="json_schema").complete(_req())

    fmt = captured["response_format"]
    assert fmt["type"] == "json_schema"
    assert fmt["json_schema"]["schema"] == Hyp.model_json_schema()


def test_real_path_json_object_decoder() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        DiscoveryLLM(BASE_URL, model_id="m", decoder="json_object").complete(_req())

    assert captured["response_format"] == {"type": "json_object"}


def test_real_path_sends_auth_header_when_key_set() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured["auth"] = request.headers.get("authorization")
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        DiscoveryLLM(BASE_URL, api_key="secret", model_id="m").complete(_req())

    assert captured["auth"] == "Bearer secret"


def test_real_path_http_error_raises() -> None:
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(500, text="boom"))
        with pytest.raises(DiscoveryLLMError):
            DiscoveryLLM(BASE_URL, model_id="m").complete(_req())


def test_real_path_malformed_body_raises() -> None:
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(200, json={"choices": []}))
        with pytest.raises(DiscoveryLLMError):
            DiscoveryLLM(BASE_URL, model_id="m").complete(_req())


def test_reasoning_exhausts_max_tokens_raises_clear_error() -> None:
    """REASONING-модель: пустой content + finish_reason=length → понятная ошибка."""
    body = {
        "choices": [
            {
                "message": {"role": "assistant", "content": "", "reasoning_content": "..."},
                "finish_reason": "length",
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 100},
    }
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(200, json=body))
        with pytest.raises(DiscoveryLLMError, match="max_tokens"):
            DiscoveryLLM(BASE_URL, model_id="m").complete(_req())


def test_real_path_reads_reasoning_tokens() -> None:
    body = _http_body()
    body["usage"] = {
        "prompt_tokens": 10,
        "completion_tokens": 800,
        "completion_tokens_details": {"reasoning_tokens": 600},
    }
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(200, json=body))
        resp = DiscoveryLLM(BASE_URL, model_id="m").complete(_req())
    assert resp.reasoning_tokens == 600
    assert resp.usage.completion_tokens == 800


# ---------------------------------------------------------------------------
# Доступность / контракт отсутствия зависимости
# ---------------------------------------------------------------------------


def test_available_true_with_generate() -> None:
    assert DiscoveryLLM(generate=lambda p, s: _valid_json()).available is True


def test_available_reflects_httpx_and_base_url() -> None:
    assert DiscoveryLLM(BASE_URL).available is module._httpx_available()
    assert DiscoveryLLM("").available is False  # пустой base_url и нет шва


def test_missing_dependency_raises_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    llm = DiscoveryLLM(BASE_URL)
    assert llm.available is False
    with pytest.raises(DiscoveryLLMUnavailable):
        llm.complete(_req())


# ---------------------------------------------------------------------------
# Структурный I18 и нейтральность
# ---------------------------------------------------------------------------

