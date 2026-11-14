"""Предпроверка ключа оценщика до краула (A5): health-check, не условие измерения.

Зачем. Прогон начинается с многочасовой сети, а мёртвый ключ обнаруживался только на Ш6.
Предпроверка переводит это в ошибку **входа**: ``CliError`` с кодом ``llm`` и
``EXIT_USAGE``, называющий HTTP-статус и текст провайдера.

Здесь фиксируется:

* проба запускается только если оценщик сконфигурирован **и** фаза ``evaluate`` в плане
  (``--until corpus`` её не запускает — оценщик там не понадобится);
* успех публикуется в payload прогона (``llm_probe``), отказ — ошибка входа;
* проба не входит в ``run_id``/``llm_scenarios``/манифест (это health-check).

Сеть не используется: порты инъецируются через ``main(..., ports=…)`` (тестовый шов),
адаптер оценщика — фейк, данные нейтральны (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from core.adapters.llm_local import EvaluationError
from core.cli.commands.run import _probe_llm_key
from core.cli.errors import EXIT_BLOCKED, EXIT_USAGE, CliError
from core.cli.main import main
from core.pipeline.runner import Ports
from tests.helpers.output import json_out
from tests.helpers.packages import run_argv

pytestmark = pytest.mark.usefixtures("isolated_state_dir")

E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"

PROVIDER_BODY = (
    '{"error":{"message":"Отсутствует или неверный API-ключ.",'
    '"type":"authentication_error","code":401,"cause":"missing_key"}}'
)


class _ProbeEvaluator:
    """Адаптер с пробой: ``error`` — отказ пробы (``None`` — успех)."""

    model_id = "fake-model"

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.probes = 0
        self.calls: list[Any] = []

    def probe(self) -> None:
        self.probes += 1
        if self.error is not None:
            raise self.error

    def complete(self, req: Any) -> Any:  # noqa: ANN401 — до Ш6 прогон не доходит
        self.calls.append(req)
        raise AssertionError("complete не должен вызываться на пути до g1")


class _NoProbeEvaluator:
    """Адаптер без ``probe`` (форма тестового шва): предпроверка невозможна."""

    model_id = "fake-model"

    def complete(self, req: Any) -> Any:  # noqa: ANN401
        raise AssertionError("complete не должен вызываться на пути до g1")


def _auth_error(status: int = 401) -> EvaluationError:
    return EvaluationError(
        f"проверка ключа оценщика не удалась: HTTP {status}: {PROVIDER_BODY}",
        reason="http_error",
        status_code=status,
        provider_body=PROVIDER_BODY,
    )


#: Настоящая проба, снятая при импорте: ``tests/conftest.py`` подменяет точку вызова в
#: composition root (тесты в сеть не ходят). Эти два прогонных теста её возвращают —
#: именно они и проверяют проводку пробы в ``run``.
_REAL_PROBE = _probe_llm_key


def _restore_real_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Возвращает настоящую пробу в модуль ``run`` на время теста."""
    from core.cli.commands import run as run_module

    monkeypatch.setattr(run_module, "_probe_llm_key", _REAL_PROBE)


# ---------------------------------------------------------------------------
# Условия запуска и исходы
# ---------------------------------------------------------------------------


def test_probe_skipped_when_evaluate_is_outside_the_plan() -> None:
    """``--until corpus`` — проба не запускается: оценщик в этом прогоне не нужен."""
    evaluator = _ProbeEvaluator()
    notices: list[str] = []

    result = _probe_llm_key(
        Ports(evaluator=evaluator), until="corpus", notices=notices
    )

    assert result is None
    assert evaluator.probes == 0
    assert notices == []


def test_probe_skipped_without_evaluator() -> None:
    """Оценщик не сконфигурирован — проверять нечего, и это не предупреждение."""
    notices: list[str] = []

    assert _probe_llm_key(Ports(), until=None, notices=notices) is None
    assert notices == []


def test_adapter_without_probe_is_named_not_silent() -> None:
    """Адаптер без ``probe`` — предупреждение: «пробы не было» ≠ «проба прошла»."""
    notices: list[str] = []

    result = _probe_llm_key(
        Ports(evaluator=_NoProbeEvaluator()), until=None, notices=notices
    )

    assert result is None
    assert len(notices) == 1
    assert "probe" in notices[0]


def test_success_publishes_latency_and_model() -> None:
    """Успех — наблюдённый факт: ``ok``, латентность и объявленная модель."""
    evaluator = _ProbeEvaluator()

    result = _probe_llm_key(Ports(evaluator=evaluator), until=None, notices=[])

    assert result is not None
    assert result["ok"] is True
    assert result["model_id"] == "fake-model"
    assert isinstance(result["latency_ms"], float)
    assert result["latency_ms"] >= 0.0
    assert evaluator.probes == 1


@pytest.mark.parametrize("status", [401, 402, 403])
def test_terminal_status_is_an_input_error(status: int) -> None:
    """``401``/``402``/``403`` — ``EXIT_USAGE`` с кодом ``llm`` и текстом провайдера."""
    with pytest.raises(CliError) as caught:
        _probe_llm_key(
            Ports(evaluator=_ProbeEvaluator(_auth_error(status))),
            until=None,
            notices=[],
        )

    error = caught.value
    assert error.exit_code == EXIT_USAGE
    assert error.code == "llm"
    assert f"HTTP {status}" in str(error)
    assert "Отсутствует или неверный API-ключ." in str(error)


def test_empty_choices_is_also_a_failure() -> None:
    """Ответ ``200`` без ``choices`` — провал пробы (ключ принят, модель не ответила)."""
    with pytest.raises(CliError) as caught:
        _probe_llm_key(
            Ports(
                evaluator=_ProbeEvaluator(
                    EvaluationError(
                        "проверка ключа оценщика не удалась: ответ без choices",
                        reason="invalid_json",
                        status_code=200,
                    )
                )
            ),
            until=None,
            notices=[],
        )

    assert caught.value.exit_code == EXIT_USAGE
    assert "choices" in str(caught.value)


# ---------------------------------------------------------------------------
# Прогон целиком: проба до фаз и публикация в payload
# ---------------------------------------------------------------------------


def test_run_publishes_probe_result(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Payload прогона несёт ``llm_probe`` — успех виден, а не только «не упало».

    Прогон доходит до человеческого гейта ``g1`` (``EXIT_BLOCKED``): проба исполняется
    **до** фаз, поэтому её факт виден и в остановленном прогоне.
    """
    _restore_real_probe(monkeypatch)
    evaluator = _ProbeEvaluator()
    code = main(
        run_argv(E2E_FIXTURES, run_id="probe-ok", runs_dir=tmp_path / "runs"),
        ports=Ports(evaluator=evaluator),
    )
    payload = json_out(capsys)

    assert code == EXIT_BLOCKED
    assert evaluator.probes == 1
    assert payload["llm_probe"]["ok"] is True
    assert payload["llm_probe"]["model_id"] == "fake-model"
    # Проба — health-check, не измерение: в манифест/identity она не едет.
    assert "llm_probe" not in (payload.get("contract") or {})


def test_run_refuses_to_start_with_dead_key(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Мёртвый ключ — отказ **до** фаз: краул не начинается, код возврата ``EXIT_USAGE``."""
    _restore_real_probe(monkeypatch)
    evaluator = _ProbeEvaluator(_auth_error(401))

    code = main(
        run_argv(E2E_FIXTURES, run_id="probe-dead", runs_dir=tmp_path / "runs"),
        ports=Ports(evaluator=evaluator),
    )

    captured = capsys.readouterr()
    assert code == EXIT_USAGE
    assert "HTTP 401" in (captured.err + captured.out)
    # Прогон не начался: ни ячеек, ни каталога прогона с фазами.
    assert not (tmp_path / "runs" / "probe-dead" / "logs" / "evaluate.jsonl").exists()
