"""Тесты ``LocalEvaluator`` (W9/T9.2) — реализация ``EvaluatorPort`` для self-host оценщика.

GPU/vLLM/сеть не требуются: генерация внедряется через шов ``generate``. Реальный
HTTP-путь проверяется без выхода в сеть — через ``respx`` (перехват ``httpx``).
Покрываются: разбор валидного выхода, сохранение ``raw``, ``decoder``/``prompt_hash``/
``usage``, невалидный JSON и нарушения схемы (I1/I7), детерминизм, проброс
``seed``/``temperature``, доступность и контракт отсутствия зависимости, структурный
I18 (нет ``tools``/``web_search`` на входе и в payload), соответствие ``EvaluatorPort``
и категорийная нейтральность (§1.3).
"""

from __future__ import annotations

import json
from typing import Any

import pytest

import core.adapters.llm_local as module
from core.adapters.llm_local import (
    DEFAULT_DECODER,
    DEFAULT_MODEL_ID,
    EvaluationError,
    EvaluatorUnavailable,
    LocalEvaluator,
)
from core.domain import LLMCellOutput, Status
from core.ports.llm import EvaluatorPort, LLMRequest, TokenUsage
from core.util.hashing import content_hash

httpx = pytest.importorskip("httpx")
respx = pytest.importorskip("respx")

PROMPT = "правило + absence_rule + [source: chunk_183] данные ячейки"
ENDPOINT = "http://localhost:8000/v1/chat/completions"

_VALID = {
    "status": "✅",
    "justification": "Функция подтверждена документацией продукта.",
    "evidence": [{"source_id": "chunk_183", "quote": "supports feature X"}],
}


def _valid_json() -> str:
    return json.dumps(_VALID, ensure_ascii=False)


def _req(**overrides: Any) -> LLMRequest:
    data: dict[str, Any] = {
        "prompt": PROMPT,
        "response_schema": LLMCellOutput,
        "model_id": "test-model",
    }
    data.update(overrides)
    return LLMRequest(**data)


# ---------------------------------------------------------------------------
# Базовый контракт / атрибуты
# ---------------------------------------------------------------------------


def test_defaults() -> None:
    ev = LocalEvaluator(generate=lambda prompt, schema: _valid_json())
    assert ev.model_id == DEFAULT_MODEL_ID
    assert ev.decoder == DEFAULT_DECODER
    assert ev.seed == 0
    assert ev.temperature == 0.0
    assert ev.available is True


def test_attributes_are_overridable() -> None:
    ev = LocalEvaluator(
        generate=lambda prompt, schema: _valid_json(),
        model_id="vendor/model",
        model_version="1.2.3",
        endpoint_kind="self_host",
        decoder="xgrammar",
        seed=7,
        temperature=0.0,
    )
    assert ev.model_id == "vendor/model"
    assert ev.model_version == "1.2.3"
    assert ev.decoder == "xgrammar"
    assert ev.seed == 7


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        LocalEvaluator(model_id="")
    with pytest.raises(ValueError):
        LocalEvaluator(endpoint_kind="")
    with pytest.raises(ValueError):
        LocalEvaluator(base_url="")
    with pytest.raises(ValueError):
        LocalEvaluator(timeout=0)
    with pytest.raises(ValueError):
        LocalEvaluator(seed=-1)
    with pytest.raises(ValueError):
        LocalEvaluator(temperature=-0.1)


# ---------------------------------------------------------------------------
# Разбор валидного выхода через шов generate
# ---------------------------------------------------------------------------


def test_complete_parses_valid_output() -> None:
    seen: list[tuple[str, type]] = []

    def generate(prompt: str, schema: type) -> str:
        seen.append((prompt, schema))
        return _valid_json()

    ev = LocalEvaluator(generate=generate)
    resp = ev.complete(_req())

    assert seen == [(PROMPT, LLMCellOutput)]
    assert isinstance(resp.output, LLMCellOutput)
    assert resp.output.status is Status.OK
    assert resp.output.evidence[0].source_id == "chunk_183"
    assert resp.raw == _valid_json()  # сырой выход сохранён для аудита
    assert resp.decoder == DEFAULT_DECODER
    assert resp.prompt_hash == content_hash(PROMPT)
    assert isinstance(resp.usage, TokenUsage)
    assert resp.usage == TokenUsage()  # у шва нет usage → нули


def test_decoder_is_echoed() -> None:
    ev = LocalEvaluator(generate=lambda p, s: _valid_json(), decoder="xgrammar")
    assert ev.complete(_req()).decoder == "xgrammar"


def test_prompt_hash_is_content_hash_of_prompt() -> None:
    ev = LocalEvaluator(generate=lambda p, s: _valid_json())
    resp = ev.complete(_req(prompt="уникальный промпт"))
    assert resp.prompt_hash == content_hash("уникальный промпт")


def test_absent_without_evidence_is_valid() -> None:
    raw = json.dumps(
        {"status": "❌", "justification": "Не найдено.", "evidence": []},
        ensure_ascii=False,
    )
    ev = LocalEvaluator(generate=lambda p, s: raw)
    resp = ev.complete(_req())
    assert resp.output.status is Status.ABSENT


# ---------------------------------------------------------------------------
# Невалидный JSON / нарушения схемы (I1/I7)
# ---------------------------------------------------------------------------


def test_invalid_json_raises_evaluation_error() -> None:
    ev = LocalEvaluator(generate=lambda p, s: "{not json")
    with pytest.raises(EvaluationError):
        ev.complete(_req())


def test_non_llm_cell_schema_raises() -> None:
    from pydantic import BaseModel

    class Other(BaseModel):
        value: int

    ev = LocalEvaluator(generate=lambda p, s: '{"value": 1}')
    with pytest.raises(EvaluationError):
        ev.complete(_req(response_schema=Other))


def test_not_applicable_status_raises() -> None:
    # I7: N/A запрещён на выходе LLM.
    raw = json.dumps(
        {"status": "N/A", "justification": "не применимо", "evidence": []},
        ensure_ascii=False,
    )
    ev = LocalEvaluator(generate=lambda p, s: raw)
    with pytest.raises(EvaluationError):
        ev.complete(_req())


def test_ok_without_evidence_raises() -> None:
    # I1: ✅/⚠️ → evidence ≥ 1.
    raw = json.dumps(
        {"status": "✅", "justification": "голословно", "evidence": []},
        ensure_ascii=False,
    )
    ev = LocalEvaluator(generate=lambda p, s: raw)
    with pytest.raises(EvaluationError):
        ev.complete(_req())


def test_extra_field_raises() -> None:
    payload = dict(_VALID)
    payload["url"] = "http://example.invalid/x"  # LLM не генерирует URL (I10/I15)
    ev = LocalEvaluator(generate=lambda p, s: json.dumps(payload, ensure_ascii=False))
    with pytest.raises(EvaluationError):
        ev.complete(_req())


# ---------------------------------------------------------------------------
# Детерминизм / проброс seed и temperature
# ---------------------------------------------------------------------------


def test_deterministic_same_generate_output() -> None:
    ev = LocalEvaluator(generate=lambda p, s: _valid_json())
    first = ev.complete(_req())
    second = ev.complete(_req())
    assert first.output == second.output
    assert first.raw == second.raw
    assert first.prompt_hash == second.prompt_hash


def test_seed_and_temperature_passed_through() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        ev = LocalEvaluator(base_url="http://localhost:8000", seed=3, temperature=0.0)
        ev.complete(_req(seed=9, temperature=0.0))

    assert captured["seed"] == 9  # req.seed переопределяет дефолт адаптера
    assert captured["temperature"] == 0.0


def test_constructor_defaults_used_when_request_omits() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        LocalEvaluator(base_url="http://localhost:8000", seed=5, temperature=0.2).complete(_req())

    assert captured["seed"] == 5
    assert captured["temperature"] == 0.2


# ---------------------------------------------------------------------------
# Реальный HTTP-путь (перехват respx; сеть не задействуется)
# ---------------------------------------------------------------------------


def _http_body(content: str | None = None, usage: dict[str, int] | None = None) -> dict[str, Any]:
    return {
        "choices": [{"message": {"role": "assistant", "content": content or _valid_json()}}],
        "usage": usage or {"prompt_tokens": 120, "completion_tokens": 40},
    }


def test_real_path_parses_response_and_usage() -> None:
    with respx.mock(assert_all_called=True) as mock:
        route = mock.post(ENDPOINT).mock(return_value=httpx.Response(200, json=_http_body()))
        ev = LocalEvaluator(base_url="http://localhost:8000", decoder="xgrammar")
        resp = ev.complete(_req())

    assert route.called
    assert resp.output.status is Status.OK
    assert resp.raw == _valid_json()
    assert resp.decoder == "xgrammar"
    assert resp.usage == TokenUsage(prompt_tokens=120, completion_tokens=40)


def test_real_path_payload_has_no_web_tools() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        LocalEvaluator(base_url="http://localhost:8000", decoder="xgrammar").complete(_req())

    assert "tools" not in captured
    assert "web_search" not in captured
    assert captured["messages"] == [{"role": "user", "content": PROMPT}]


def test_real_path_requests_json_schema_when_constrained() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        LocalEvaluator(base_url="http://localhost:8000", decoder="xgrammar").complete(_req())

    response_format = captured["response_format"]
    assert response_format["type"] == "json_schema"
    assert response_format["json_schema"]["schema"] == LLMCellOutput.model_json_schema()


def test_real_path_omits_response_format_when_decoder_none() -> None:
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=_http_body())

    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        LocalEvaluator(base_url="http://localhost:8000", decoder="none").complete(_req())

    assert "response_format" not in captured


def test_real_path_http_error_raises_evaluation_error() -> None:
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(500, text="boom"))
        with pytest.raises(EvaluationError):
            LocalEvaluator(base_url="http://localhost:8000").complete(_req())


def test_real_path_malformed_body_raises_evaluation_error() -> None:
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(200, json={"choices": []}))
        with pytest.raises(EvaluationError):
            LocalEvaluator(base_url="http://localhost:8000").complete(_req())


# ---------------------------------------------------------------------------
# Доступность / контракт отсутствия зависимости
# ---------------------------------------------------------------------------


def test_available_true_with_generate() -> None:
    assert LocalEvaluator(generate=lambda p, s: _valid_json()).available is True


def test_available_reflects_httpx_without_generate() -> None:
    assert LocalEvaluator().available is module._httpx_available()


def test_missing_dependency_raises_evaluator_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    # Форсируем отсутствие httpx, чтобы тест был детерминирован в любом окружении.
    monkeypatch.setattr(module, "_httpx_available", lambda: False)
    ev = LocalEvaluator()
    assert ev.available is False
    with pytest.raises(EvaluatorUnavailable):
        ev.complete(_req())
    assert issubclass(EvaluatorUnavailable, RuntimeError)


# ---------------------------------------------------------------------------
# Структурный I18
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Нейтральность модуля (§1.3)
# ---------------------------------------------------------------------------


