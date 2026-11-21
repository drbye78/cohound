"""Ход с **локальными** инструментами: ``LocalEvaluator.turn`` (§7, I18).

Проверяется то, что нельзя увидеть по вердикту:

* в теле запроса уходят **ровно** объявленные инструменты (``tools[].function``) и
  ``tool_choice="auto"``, а ``response_format`` — нет: грамматика constrained decoding и
  tool-calling конфликтуют в одном запросе у OI-совместимых серверов;
* ``web_search`` в теле нет ни в каком виде (I18);
* просьба вызвать имя **вне** объявленного набора — отказ (``ToolCallRefused``), а не
  «пропустим и продолжим»;
* вердикт, пришедший ходом, проходит тот же разбор формы/инвариантов, что ``complete``,
  и его ``decoder`` честно равен ``none`` (грамматика не запрашивалась);
* пустой ход (ни вердикта, ни вызовов) — видимый отказ формы.

Сеть не задействуется: ``httpx`` перехватывается ``respx``, как в ``test_llm_local.py``.
Нейтрально к предметной категории (§1.3).
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from core.adapters.llm_local import EvaluationError, LocalEvaluator, ToolCallRefused
from core.domain import LLMCellOutput, Status
from core.domain.tools import ToolInvocation, ToolSpec
from core.ports.llm import ToolTurnRequest

httpx = pytest.importorskip("httpx")
respx = pytest.importorskip("respx")

PROMPT = "правило + absence_rule + [source: chunk_183] данные ячейки"
ENDPOINT = "http://localhost:8000/v1/chat/completions"

_SPEC = ToolSpec(
    name="section_chunks",
    description="Чанки того же раздела документа, что и адресованный чанк.",
    parameters={
        "type": "object",
        "properties": {"chunk_id": {"type": "string"}},
        "required": ["chunk_id"],
    },
)

_VALID = {
    "status": "✅",
    "justification": "Функция подтверждена документацией продукта.",
    "evidence": [{"source_id": "chunk_183", "quote": "supports feature X"}],
}


def _req(**overrides: Any) -> ToolTurnRequest:
    data: dict[str, Any] = {
        "prompt": PROMPT,
        "response_schema": LLMCellOutput,
        "allowed_tools": (_SPEC,),
        "model_id": "test-model",
    }
    data.update(overrides)
    return ToolTurnRequest(**data)


def _tool_call_body(
    name: str = "section_chunks", arguments: str | None = None
) -> dict[str, Any]:
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": arguments
                                if arguments is not None
                                else json.dumps({"chunk_id": "chunk_183"}),
                            },
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 90, "completion_tokens": 12},
    }


def _content_body(content: str | None) -> dict[str, Any]:
    return {
        "choices": [
            {"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}
        ],
        "usage": {"prompt_tokens": 70, "completion_tokens": 30},
    }


def _capture(body: dict[str, Any]) -> Any:
    """Перехватывает тело запроса; возвращает обработчик и хранилище."""
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=body)

    return handler, captured


# ---------------------------------------------------------------------------
# Тело запроса: объявленные инструменты, без грамматики
# ---------------------------------------------------------------------------


def test_turn_sends_exactly_declared_tools_and_no_response_format() -> None:
    handler, captured = _capture(_tool_call_body())
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        LocalEvaluator(base_url="http://localhost:8000", decoder="json_schema").turn(_req())

    assert captured["tools"] == [
        {
            "type": "function",
            "function": {
                "name": _SPEC.name,
                "description": _SPEC.description,
                "parameters": dict(_SPEC.parameters),
            },
        }
    ]
    assert captured["tool_choice"] == "auto"
    # Грамматика и tool-calling не совмещаются в одном запросе: провенанс — ``none``.
    assert "response_format" not in captured
    # I18: веб-поиска здесь нет ни в каком виде.
    assert "web_search" not in json.dumps(captured, ensure_ascii=False)


def test_turn_sends_only_the_declared_subset() -> None:
    """Набор — объявленный вход: второй инструмент в тело не попадает."""
    other = ToolSpec(name="document_tables", description="Таблицы документа.")
    handler, captured = _capture(_tool_call_body())
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        LocalEvaluator(base_url="http://localhost:8000").turn(
            _req(allowed_tools=(_SPEC, other))
        )

    names = [item["function"]["name"] for item in captured["tools"]]
    assert names == ["section_chunks", "document_tables"]


# ---------------------------------------------------------------------------
# Разбор хода
# ---------------------------------------------------------------------------


def test_tool_calls_are_parsed_into_invocations() -> None:
    handler, _ = _capture(_tool_call_body())
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        turn = LocalEvaluator(base_url="http://localhost:8000").turn(_req())

    assert turn.final is None
    assert turn.tool_calls == (
        ToolInvocation(name="section_chunks", arguments={"chunk_id": "chunk_183"}),
    )
    assert turn.finish_reason == "tool_calls"


def test_tool_call_outside_the_declared_set_is_refused() -> None:
    handler, _ = _capture(_tool_call_body(name="web_search"))
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        with pytest.raises(ToolCallRefused) as excinfo:
            LocalEvaluator(base_url="http://localhost:8000").turn(_req())

    assert excinfo.value.reason == "tool_not_allowed"
    assert "web_search" in str(excinfo.value)


def test_malformed_arguments_are_refused() -> None:
    handler, _ = _capture(_tool_call_body(arguments="{not json"))
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        with pytest.raises(EvaluationError) as excinfo:
            LocalEvaluator(base_url="http://localhost:8000").turn(_req())

    assert excinfo.value.reason == "invalid_json"


def test_final_answer_goes_through_the_same_schema_validation() -> None:
    handler, _ = _capture(_content_body(json.dumps(_VALID, ensure_ascii=False)))
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        turn = LocalEvaluator(base_url="http://localhost:8000").turn(_req())

    assert turn.tool_calls == ()
    assert turn.final is not None
    assert turn.final.output.status is Status.OK
    # Грамматика не запрашивалась — провенанс это называет, а не приписывает механизм.
    assert turn.final.decoder == "none"


def test_final_answer_violating_the_contract_is_refused() -> None:
    payload = dict(_VALID)
    payload["status"] = "✅"
    payload["evidence"] = []  # I1: ✅ без evidence недопустим
    handler, _ = _capture(_content_body(json.dumps(payload, ensure_ascii=False)))
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        with pytest.raises(EvaluationError) as excinfo:
            LocalEvaluator(base_url="http://localhost:8000").turn(_req())

    assert excinfo.value.reason == "invalid_json"


def test_empty_turn_is_refused_not_returned_empty() -> None:
    """Ни вердикта, ни вызовов — отказ формы: ячейка не должна молча стать ``❓``."""
    handler, _ = _capture(_content_body(None))
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        with pytest.raises(EvaluationError) as excinfo:
            LocalEvaluator(base_url="http://localhost:8000").turn(_req())

    assert excinfo.value.reason == "empty_turn"


def test_duplicate_tool_names_are_refused_before_the_request() -> None:
    with pytest.raises(ValueError):
        LocalEvaluator(base_url="http://localhost:8000").turn(
            _req(allowed_tools=(_SPEC, _SPEC))
        )
