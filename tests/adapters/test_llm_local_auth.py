"""Тело HTTP-отказа провайдера и предпроверка ключа (A4/A5) — на уровне адаптера.

Замер 14.11.2026: на неверный/отсутствующий ключ провайдер отвечает ``HTTP 401`` и
**объясняет причину в теле**: ``{"error":{"message":"Отсутствует или неверный API-ключ.",
"type":"authentication_error","cause":"missing_key",…}}``. До правки тело выбрасывалось,
и ``401`` был неотличим от ``403``/``402`` по артефактам прогона.

Здесь фиксируется: тело доезжает до исключения (``provider_body`` + текст сообщения),
усекается и не содержит ключа/заголовков; ``probe()`` идёт **тем же** путём, что боевой
вызов (URL/заголовки/модель), но с минимальным запросом и без счётчиков расхода.

Сеть не задействуется: HTTP перехватывается ``respx``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from core.adapters.llm_local import (
    CHAT_COMPLETIONS_PATH,
    PROVIDER_BODY_LIMIT,
    EvaluationError,
    LocalEvaluator,
)
from core.domain import LLMCellOutput
from core.ports.llm import LLMRequest

httpx = pytest.importorskip("httpx")
respx = pytest.importorskip("respx")

ENDPOINT = "http://localhost:8000/v1/chat/completions"
PROMPT = "Оцени параметр по правилу."

#: Тело ответа провайдера на неверный ключ (дословно, замер 14.11.2026).
PROVIDER_BODY = (
    '{"error":{"message":"Отсутствует или неверный API-ключ.",'
    '"type":"authentication_error","code":401,"cause":"missing_key",'
    '"request_id":"req-1"}}'
)


def _req() -> LLMRequest:
    return LLMRequest(
        prompt=PROMPT,
        response_schema=LLMCellOutput,
        model_id="test-model",
    )


def _evaluator(**kwargs: Any) -> LocalEvaluator:
    return LocalEvaluator(base_url="http://localhost:8000", **kwargs)


# ---------------------------------------------------------------------------
# A4: тело HTTP-отказа не теряется
# ---------------------------------------------------------------------------


def test_http_error_message_carries_provider_body() -> None:
    """``401``: статус и тело — в атрибутах и в тексте; форма сообщения как в замере."""
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(401, text=PROVIDER_BODY))
        with pytest.raises(EvaluationError) as caught:
            _evaluator().complete(_req())

    error = caught.value
    assert error.status_code == 401
    assert error.provider_body == PROVIDER_BODY
    assert error.reason == "http_error"
    # Форма сообщения: «…: HTTP 401: {тело}» — тело видно там, где оператор ищет причину.
    assert str(error) == f"HTTP-вызов оценщика не удался: HTTP 401: {PROVIDER_BODY}"
    assert "Отсутствует или неверный API-ключ." in str(error)


def test_body_is_flattened_and_truncated() -> None:
    """Тело — одна строка и не длиннее ``PROVIDER_BODY_LIMIT``: сообщение едет в ячейки."""
    long_body = ("строка отказа\n" * 200).strip()
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(500, text=long_body))
        with pytest.raises(EvaluationError) as caught:
            _evaluator().complete(_req())

    body = caught.value.provider_body
    assert body is not None
    assert len(body) == PROVIDER_BODY_LIMIT
    assert "\n" not in body
    assert str(caught.value).endswith(body)


def test_empty_body_keeps_the_old_message_shape() -> None:
    """Пустое тело не добавляет ``": "`` — сообщение остаётся прежним."""
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(503, text=""))
        with pytest.raises(EvaluationError) as caught:
            _evaluator().complete(_req())

    assert caught.value.provider_body is None
    assert str(caught.value) == "HTTP-вызов оценщика не удался: HTTP 503"


def test_key_and_headers_never_reach_the_message() -> None:
    """Ключ не попадает ни в текст, ни в ``provider_body`` — только в заголовок."""
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(401, text=PROVIDER_BODY))
        with pytest.raises(EvaluationError) as caught:
            _evaluator(api_key="secret-key").complete(_req())

    rendered = f"{caught.value}{caught.value.provider_body}"
    assert "secret-key" not in rendered
    assert "Authorization" not in rendered


# ---------------------------------------------------------------------------
# A5: probe() — тот же путь, минимальный запрос
# ---------------------------------------------------------------------------


def test_probe_uses_the_same_path_headers_and_minimal_payload() -> None:
    """Проба идёт тем же URL/заголовками, что боевой вызов, но ``max_tokens=1``."""
    captured: dict[str, Any] = {}

    def handler(request: Any) -> Any:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"role": "assistant", "content": ""}}]},
        )

    with respx.mock(assert_all_called=True) as mock:
        mock.post(ENDPOINT).mock(side_effect=handler)
        assert _evaluator(api_key="k-1").probe() is None

    assert captured["url"] == ENDPOINT
    assert captured["auth"] == "Bearer k-1"
    body = captured["body"]
    assert body["model"] == LocalEvaluator().model_id  # объявленная модель, не догадка
    assert body["max_tokens"] == 1
    assert body["temperature"] == 0.0
    assert body["stream"] is False
    # I18: у пробы нет ни tools, ни web_search — как и у боевого вызова.
    assert "tools" not in body


def test_probe_survives_v1_tail_in_base_url() -> None:
    """Хвост ``/v1`` в базе — тот же endpoint (нормализация одна на оба пути)."""
    with respx.mock(assert_all_called=True) as mock:
        route = mock.post(ENDPOINT).mock(
            return_value=httpx.Response(200, json={"choices": [{"index": 0}]})
        )
        LocalEvaluator(base_url="http://localhost:8000/v1").probe()

    assert route.called


def test_probe_reports_status_and_body_on_rejection() -> None:
    """Отказ пробы — ``EvaluationError`` с ``status_code``/``provider_body``."""
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(401, text=PROVIDER_BODY))
        with pytest.raises(EvaluationError) as caught:
            _evaluator().probe()

    assert caught.value.status_code == 401
    assert caught.value.provider_body == PROVIDER_BODY
    assert "HTTP 401" in str(caught.value)


def test_probe_treats_empty_choices_as_failure() -> None:
    """``200`` без ``choices`` — провал: пустой ответ не доказывает, что модель жива."""
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(return_value=httpx.Response(200, json={"choices": []}))
        with pytest.raises(EvaluationError) as caught:
            _evaluator().probe()

    assert "choices" in str(caught.value)


def test_probe_is_not_a_measured_call() -> None:
    """Проба не растит счётчики расхода: это health-check, а не измерение."""
    with respx.mock(assert_all_called=False) as mock:
        mock.post(ENDPOINT).mock(
            return_value=httpx.Response(200, json={"choices": [{"index": 0}]})
        )
        evaluator = _evaluator()
        evaluator.probe()

    assert evaluator.usage_summary() == {"calls": 0, "tokens": {}}


def test_probe_path_constant_is_the_live_path() -> None:
    """Проба и боевой вызов собирают адрес одной константой — не второй формулировкой."""
    assert CHAT_COMPLETIONS_PATH == "/v1/chat/completions"
    evaluator = _evaluator()
    assert evaluator._completions_url() == ENDPOINT
