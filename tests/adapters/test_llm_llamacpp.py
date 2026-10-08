"""Тесты ``LlamaCppEvaluator`` (W9/T9.6) — CPU/GGUF fallback ``EvaluatorPort``.

GPU, ``llama-cpp-python`` и сеть не требуются: генерация внедряется через шов ``generate``,
а реальный CPU-путь проверяется подстановкой фейковой модели (без пакета). Покрываются:
разбор валидного выхода, сохранение ``raw``, ``decoder``/``prompt_hash``/``usage``,
невалидный JSON и нарушения схемы (I1/I7), детерминизм, проброс ``seed``/``temperature``
и запрос GBNF-грамматики, доступность и контракт отсутствия зависимости, структурный I18
(нет внешнего поиска в швах и AST), соответствие ``EvaluatorPort`` и категорийная
нейтральность (§1.3).
"""

from __future__ import annotations

import ast
import inspect
import json
import re
from pathlib import Path
from typing import Any

import pytest

import core.adapters.llm_llamacpp as module
from core.adapters.llm_llamacpp import (
    DEFAULT_DECODER,
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL_ID,
    EvaluationError,
    EvaluatorUnavailable,
    LlamaCppError,
    LlamaCppEvaluator,
)
from core.domain import LLMCellOutput, Status
from core.ports.llm import EvaluatorPort, LLMRequest, TokenUsage
from core.util.hashing import content_hash

PROMPT = "правило + absence_rule + [source: chunk_183] данные ячейки"

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


def _echo(req: LLMRequest) -> str:
    """Детерминированный шов: всегда валидный JSON ячейки."""
    return _valid_json()


class _FakeModel:
    """Фейковый объект ``Llama``: записывает вызовы, отдаёт форму ``choices[0].text``."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, prompt: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((prompt, kwargs))
        return {
            "choices": [{"text": self.text, "index": 0}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18},
        }


class _BoomModel:
    """Фейковая модель, падающая на вызове (проверка ``LlamaCppError``)."""

    def __call__(self, prompt: str, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("boom")


class _MalformedModel:
    """Фейковая модель с неожиданной формой ответа."""

    def __call__(self, prompt: str, **kwargs: Any) -> dict[str, Any]:
        return {"choices": []}


# ---------------------------------------------------------------------------
# Базовый контракт / атрибуты
# ---------------------------------------------------------------------------


def test_defaults() -> None:
    ev = LlamaCppEvaluator(generate=_echo)
    assert ev.model_id == DEFAULT_MODEL_ID
    assert ev.decoder == DEFAULT_DECODER
    assert ev.seed == 0
    assert ev.temperature == 0.0
    assert ev.available is True


def test_attributes_are_overridable() -> None:
    ev = LlamaCppEvaluator(
        generate=_echo,
        model_id="vendor/model",
        model_version="1.2.3",
        endpoint_kind="self_host",
        decoder="none",
        seed=7,
        temperature=0.0,
    )
    assert ev.model_id == "vendor/model"
    assert ev.model_version == "1.2.3"
    assert ev.decoder == "none"
    assert ev.seed == 7


def test_model_id_derived_from_model_path(tmp_path: Path) -> None:
    # model_id — из конфигурации (имя GGUF-файла), а не хардкод в коде.
    model = tmp_path / "weights.gguf"
    model.write_bytes(b"gguf")
    ev = LlamaCppEvaluator(model_path=str(model))
    assert ev.model_id == "weights.gguf"


def test_invalid_constructor_args_rejected() -> None:
    with pytest.raises(ValueError):
        LlamaCppEvaluator(model_id="")
    with pytest.raises(ValueError):
        LlamaCppEvaluator(endpoint_kind="")
    with pytest.raises(ValueError):
        LlamaCppEvaluator(model_path="")
    with pytest.raises(ValueError):
        LlamaCppEvaluator(seed=-1)
    with pytest.raises(ValueError):
        LlamaCppEvaluator(temperature=-0.1)
    with pytest.raises(ValueError):
        LlamaCppEvaluator(n_ctx=0)
    with pytest.raises(ValueError):
        LlamaCppEvaluator(max_tokens=0)
    with pytest.raises(ValueError):
        LlamaCppEvaluator(n_threads=0)


def test_implements_evaluator_port() -> None:
    ev = LlamaCppEvaluator(generate=_echo)
    assert isinstance(ev, EvaluatorPort)
    assert callable(ev.complete)


def test_decoder_provenance_differs_from_primary_adapter() -> None:
    import core.adapters.llm_local as primary

    assert DEFAULT_DECODER != primary.DEFAULT_DECODER


# ---------------------------------------------------------------------------
# Разбор валидного выхода через шов generate
# ---------------------------------------------------------------------------


def test_complete_parses_valid_output() -> None:
    seen: list[LLMRequest] = []

    def generate(req: LLMRequest) -> str:
        seen.append(req)
        return _valid_json()

    ev = LlamaCppEvaluator(generate=generate)
    req = _req()
    resp = ev.complete(req)

    assert seen == [req]  # шов получает исходный LLMRequest
    assert isinstance(resp.output, LLMCellOutput)
    assert resp.output.status is Status.OK
    assert resp.output.evidence[0].source_id == "chunk_183"
    assert resp.raw == _valid_json()  # сырой выход сохранён для аудита
    assert resp.decoder == DEFAULT_DECODER
    assert resp.prompt_hash == content_hash(PROMPT)
    assert isinstance(resp.usage, TokenUsage)
    assert resp.usage == TokenUsage()  # у шва нет usage → нули


def test_decoder_is_echoed() -> None:
    ev = LlamaCppEvaluator(generate=_echo, decoder="gbnf")
    assert ev.complete(_req()).decoder == "gbnf"


def test_prompt_hash_is_content_hash_of_prompt() -> None:
    ev = LlamaCppEvaluator(generate=_echo)
    resp = ev.complete(_req(prompt="уникальный промпт"))
    assert resp.prompt_hash == content_hash("уникальный промпт")


def test_absent_without_evidence_is_valid() -> None:
    raw = json.dumps(
        {"status": "❌", "justification": "Не найдено.", "evidence": []},
        ensure_ascii=False,
    )
    ev = LlamaCppEvaluator(generate=lambda req: raw)
    resp = ev.complete(_req())
    assert resp.output.status is Status.ABSENT


# ---------------------------------------------------------------------------
# Невалидный JSON / нарушения схемы (I1/I7) — зеркало основного адаптера
# ---------------------------------------------------------------------------


def test_invalid_json_raises_evaluation_error() -> None:
    ev = LlamaCppEvaluator(generate=lambda req: "{not json")
    with pytest.raises(EvaluationError):
        ev.complete(_req())


def test_non_llm_cell_schema_raises() -> None:
    from pydantic import BaseModel

    class Other(BaseModel):
        value: int

    ev = LlamaCppEvaluator(generate=lambda req: '{"value": 1}')
    with pytest.raises(EvaluationError):
        ev.complete(_req(response_schema=Other))


def test_not_applicable_status_raises() -> None:
    # I7: N/A запрещён на выходе LLM.
    raw = json.dumps(
        {"status": "N/A", "justification": "не применимо", "evidence": []},
        ensure_ascii=False,
    )
    ev = LlamaCppEvaluator(generate=lambda req: raw)
    with pytest.raises(EvaluationError):
        ev.complete(_req())


def test_ok_without_evidence_raises() -> None:
    # I1: ✅/⚠️ → evidence ≥ 1.
    raw = json.dumps(
        {"status": "✅", "justification": "голословно", "evidence": []},
        ensure_ascii=False,
    )
    ev = LlamaCppEvaluator(generate=lambda req: raw)
    with pytest.raises(EvaluationError):
        ev.complete(_req())


def test_extra_field_raises() -> None:
    payload = dict(_VALID)
    payload["url"] = "http://example.invalid/x"  # LLM не генерирует URL (I10/I15)
    ev = LlamaCppEvaluator(generate=lambda req: json.dumps(payload, ensure_ascii=False))
    with pytest.raises(EvaluationError):
        ev.complete(_req())


def test_error_types_are_runtime_errors() -> None:
    assert issubclass(EvaluationError, RuntimeError)
    assert issubclass(LlamaCppError, RuntimeError)
    assert issubclass(EvaluatorUnavailable, LlamaCppError)


# ---------------------------------------------------------------------------
# Детерминизм
# ---------------------------------------------------------------------------


def test_deterministic_same_generate_output() -> None:
    ev = LlamaCppEvaluator(generate=_echo)
    first = ev.complete(_req())
    second = ev.complete(_req())
    assert first.output == second.output
    assert first.raw == second.raw
    assert first.prompt_hash == second.prompt_hash


# ---------------------------------------------------------------------------
# Реальный CPU/GGUF-путь (фейковая модель; пакет не нужен)
# ---------------------------------------------------------------------------


def test_real_path_parses_response_and_usage() -> None:
    fake = _FakeModel(_valid_json())
    ev = LlamaCppEvaluator(decoder="none")
    ev._model = fake  # подстановка шва реального пути
    resp = ev.complete(_req())

    prompt, kwargs = fake.calls[0]
    assert prompt == PROMPT
    assert kwargs["max_tokens"] == DEFAULT_MAX_TOKENS
    assert kwargs["temperature"] == 0.0
    assert kwargs["seed"] == 0
    assert resp.output.status is Status.OK
    assert resp.raw == _valid_json()
    assert resp.usage == TokenUsage(prompt_tokens=11, completion_tokens=7)


def test_seed_and_temperature_passed_through() -> None:
    fake = _FakeModel(_valid_json())
    ev = LlamaCppEvaluator(decoder="none", seed=5, temperature=0.2)
    ev._model = fake
    ev.complete(_req(seed=9, temperature=0.0))
    _, kwargs = fake.calls[0]
    assert kwargs["seed"] == 9  # req.seed переопределяет дефолт адаптера
    assert kwargs["temperature"] == 0.0


def test_constructor_defaults_used_when_request_omits() -> None:
    fake = _FakeModel(_valid_json())
    ev = LlamaCppEvaluator(decoder="none", seed=5, temperature=0.2)
    ev._model = fake
    ev.complete(_req())
    _, kwargs = fake.calls[0]
    assert kwargs["seed"] == 5
    assert kwargs["temperature"] == 0.2


def test_real_path_requests_grammar_when_constrained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeModel(_valid_json())
    ev = LlamaCppEvaluator(decoder="gbnf")
    ev._model = fake
    monkeypatch.setattr(ev, "_build_grammar", lambda schema: "GRAMMAR")
    ev.complete(_req())
    _, kwargs = fake.calls[0]
    assert kwargs["grammar"] == "GRAMMAR"


def test_real_path_omits_grammar_when_decoder_none() -> None:
    fake = _FakeModel(_valid_json())
    ev = LlamaCppEvaluator(decoder="none")
    ev._model = fake
    ev.complete(_req())
    _, kwargs = fake.calls[0]
    assert "grammar" not in kwargs


def test_real_path_backend_error_raises_llama_cpp_error() -> None:
    ev = LlamaCppEvaluator(decoder="none")
    ev._model = _BoomModel()
    with pytest.raises(LlamaCppError):
        ev.complete(_req())


def test_real_path_malformed_body_raises_llama_cpp_error() -> None:
    ev = LlamaCppEvaluator(decoder="none")
    ev._model = _MalformedModel()
    with pytest.raises(LlamaCppError):
        ev.complete(_req())


# ---------------------------------------------------------------------------
# Доступность / контракт отсутствия зависимости
# ---------------------------------------------------------------------------


def test_available_true_with_generate() -> None:
    assert LlamaCppEvaluator(generate=_echo).available is True


def test_available_false_without_package(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_llama_cpp_available", lambda: False)
    ev = LlamaCppEvaluator(model_path="/nonexistent/model.gguf")
    assert ev.available is False


def test_available_false_when_model_file_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_llama_cpp_available", lambda: True)
    ev = LlamaCppEvaluator(model_path="/nonexistent/model.gguf")
    assert ev.available is False


def test_available_true_with_package_and_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    model = tmp_path / "model.gguf"
    model.write_bytes(b"gguf")
    monkeypatch.setattr(module, "_llama_cpp_available", lambda: True)
    ev = LlamaCppEvaluator(model_path=str(model))
    assert ev.available is True


def test_missing_dependency_raises_evaluator_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Форсируем отсутствие пакета, чтобы тест был детерминирован в любом окружении.
    monkeypatch.setattr(module, "_llama_cpp_available", lambda: False)
    ev = LlamaCppEvaluator()
    assert ev.available is False
    with pytest.raises(EvaluatorUnavailable):
        ev.complete(_req())


# ---------------------------------------------------------------------------
# Структурный I18: швы и AST
# ---------------------------------------------------------------------------

_BANNED = {"tools", "web_search", "websearch", "web_tool"}


def test_constructor_has_no_web_tool_params() -> None:
    params = set(inspect.signature(LlamaCppEvaluator.__init__).parameters)
    assert params & _BANNED == set()
    # Модель маршрутизируется фазой, а не решением LLM (I21): нет и scenario-параметра.
    assert "scenario" not in params


def test_complete_has_no_web_tool_params() -> None:
    params = set(inspect.signature(LlamaCppEvaluator.complete).parameters)
    assert params & _BANNED == set()
    assert params == {"self", "req"}


def test_evaluator_port_itself_exposes_no_tools() -> None:
    # Зеркало структурной проверки порта (selfcheck): I18 держится типом, не соглашением.
    params = set(inspect.signature(EvaluatorPort.complete).parameters)
    assert params & _BANNED == set()


def test_module_source_has_no_web_tool_tokens() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    hit = re.search(r"\b(tools|web_search|websearch|web_tool)\b", source, re.IGNORECASE)
    assert hit is None


def test_module_ast_has_no_web_tool_seams() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            args = node.args
            names = {a.arg for a in [*args.posonlyargs, *args.args, *args.kwonlyargs]}
            if args.vararg:
                names.add(args.vararg.arg)
            if args.kwarg:
                names.add(args.kwarg.arg)
            assert names & _BANNED == set()
        if isinstance(node, ast.Name):
            assert node.id not in _BANNED
        if isinstance(node, ast.Attribute):
            assert node.attr not in _BANNED
        if isinstance(node, ast.keyword) and node.arg is not None:
            assert node.arg not in _BANNED


# ---------------------------------------------------------------------------
# Нейтральность модуля (§1.3)
# ---------------------------------------------------------------------------


def test_module_source_is_category_neutral() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    hits = [line for line in source.splitlines() if forbidden.search(line)]
    assert hits == []
