"""Отказ удостоверения оценщика (``401``/``402``/``403``): класс, отсутствие повтора, стоп фазы.

Зачем отдельный файл. Замер 14.11.2026: провайдер отвечает ``HTTP 401`` + тело
``{"error":{"message":"Отсутствует или неверный API-ключ.",…}}`` на неверный ключ, а Ш6
считал это «повторяемым» отказом — паузы 1.07 с и 2.19 с, три попытки на **каждую**
ячейку, и фаза завершалась ``status="ok"``. То есть прогон с мёртвым ключом был неотличим
от измеренного: 905 ``failed``-ячеек выглядели как обычная работа.

Здесь фиксируется ровно то, что обязано держаться:

* ``401``/``402``/``403`` — класс ``auth_error`` (свойство ключа/счёта, не документа);
  ``400``/``404``/``422``/``429``/``5xx`` — прежний ``http_error``;
* ``auth_error`` не получает паузы и **не повторяется** (одна попытка на ячейку);
* первое вхождение отменяет остаток фазы: ``PhaseResult.status == "failed"`` с причиной
  (класс + HTTP-статус + текст провайдера), а не ``ok``;
* провенанс отказа (``status_code``/``error_message``) попадает в журнал вызовов.

Сеть не используется: оценщик — скриптованный порт (I18), данные нейтральны (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core.adapters.llm_local import EvaluationError
from core.pipeline.evaluate import (
    LLM_CALL_OUTCOMES,
    TERMINAL_HTTP_STATUSES,
    LLMCallJournal,
    _error_outcome,
    _retry_pause,
    evaluate_cell,
    evaluate_phase,
)
from core.pipeline.runner import Ports
from tests.pipeline.test_evaluate import (
    _chunks,
    _data,
    _no_sleep,
    _param,
    _ScriptedEvaluator,
)
from tests.pipeline.test_evaluate_batching import (
    RUN_ID,
    _CountingEmbedder,
    _plan,
    _RecordingVectorIndex,
    _store,
)

#: Тело ответа провайдера на отсутствующий/неверный ключ (замер 14.11.2026, дословно).
PROVIDER_BODY = (
    '{"error":{"message":"Отсутствует или неверный API-ключ.",'
    '"type":"authentication_error","code":401,"cause":"missing_key",'
    '"request_id":"req-1"}}'
)


def _auth_error(status: int = 401, *, body: str | None = PROVIDER_BODY) -> EvaluationError:
    """Отказ провайдера той же формы, что отдаёт боевой адаптер (``llm_local``)."""
    detail = f"HTTP {status}"
    if body:
        detail = f"{detail}: {body}"
    return EvaluationError(
        f"HTTP-вызов оценщика не удался: {detail}",
        reason="http_error",
        status_code=status,
        provider_body=body,
    )


def _journal(tmp_path: Path) -> LLMCallJournal:
    return LLMCallJournal(tmp_path / "logs" / "llm_calls.jsonl", _data()["run_id"])


def _lines(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


# ---------------------------------------------------------------------------
# A1: класс отказа и политика повтора
# ---------------------------------------------------------------------------


def test_terminal_statuses_are_the_credential_class() -> None:
    """``401``/``402``/``403`` — терминальны; остальные 4xx и ``429`` — нет.

    Класс отдельный, а не «любой 4xx»: ``400``/``404``/``422`` — отказ **запроса**, и его
    повтор остаётся как был; ``429`` — троттлинг, он повторяем по определению.
    """
    assert TERMINAL_HTTP_STATUSES == frozenset({401, 402, 403})
    for status in (401, 402, 403):
        assert _error_outcome(_auth_error(status)) == "auth_error"
    for status in (400, 404, 422, 429, 500, 503):
        assert _error_outcome(_auth_error(status)) == "http_error"
    assert "auth_error" in LLM_CALL_OUTCOMES


def test_auth_error_gets_no_pause_and_no_retry_budget() -> None:
    """``auth_error`` не назначает паузу даже при неизрасходованном бюджете повторов."""
    for attempt in (1, 2):
        assert _retry_pause(
            attempt, max_retries=2, outcome="auth_error", error=_auth_error()
        ) == (None, None, None)
    # Прежний класс не затронут: ``http_error`` по-прежнему получает паузу.
    pause, reason, _ = _retry_pause(
        1, max_retries=2, outcome="http_error", error=_auth_error(500)
    )
    assert pause is not None and reason == "http_error"


def test_auth_error_is_one_attempt_per_cell(tmp_path: Path) -> None:
    """``401`` — одна попытка на ячейку, без ``sleep``, и исход называет причину.

    Регрессия: до правки было три попытки (паузы 1.07 с и 2.19 с) — мёртвый ключ
    оплачивался временем каждой ячейки.
    """
    data = _data()
    evaluator = _ScriptedEvaluator([_auth_error(401)])
    waited: list[float] = []
    journal = _journal(tmp_path)

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        llm_journal=journal,
        sleep=waited.append,
    )

    assert len(evaluator.calls) == 1  # повтора нет
    assert waited == []  # и паузы тоже нет
    # I4 не ослаблен: ``failed`` не несёт статус, класс сбоя — прежний (``hound retry``
    # продолжает восстанавливать такие ячейки на том же ``run_id``).
    assert outcome.cell.state.value == "failed"
    assert outcome.cell.status is None
    assert outcome.failure_kind == "evaluation_failed"
    assert outcome.auth_failure is not None
    assert "auth_error: HTTP 401" in outcome.auth_failure
    assert "Отсутствует или неверный API-ключ." in outcome.auth_failure


def test_non_auth_http_error_keeps_retrying(tmp_path: Path) -> None:
    """``400`` остаётся ``http_error``: три попытки и две паузы, как до правки."""
    data = _data()
    evaluator = _ScriptedEvaluator([_auth_error(400, body="bad request")])
    waited: list[float] = []

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        sleep=waited.append,
    )

    assert len(evaluator.calls) == 3
    assert len(waited) == 2
    assert outcome.auth_failure is None
    assert outcome.failure_kind == "evaluation_failed"


# ---------------------------------------------------------------------------
# A3: провенанс отказа в журнале вызовов
# ---------------------------------------------------------------------------


def test_journal_records_status_and_provider_message(tmp_path: Path) -> None:
    """Строка отказа несёт ``status_code`` и текст; успех — ``None`` в обоих полях."""
    data = _data()
    journal = _journal(tmp_path)

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([_auth_error(401)]),
        chunks=_chunks(data),
        llm_journal=journal,
        sleep=_no_sleep,
    )

    lines = _lines(journal.path)
    assert len(lines) == 1  # повтора нет — одна строка на попытку
    assert lines[0]["outcome"] == "auth_error"
    assert lines[0]["status_code"] == 401
    assert "Отсутствует или неверный API-ключ." in lines[0]["error_message"]
    # Паузы у отказа удостоверения нет: повтор не назначался.
    assert lines[0]["retry_wait_s"] is None
    assert lines[0]["retry_reason"] is None


def test_journal_error_message_is_truncated(tmp_path: Path) -> None:
    """Текст отказа усечён: журнал — провенанс класса, а не замена консоли."""
    data = _data()
    journal = _journal(tmp_path)
    long_error = EvaluationError(
        "x" * 2000,
        reason="http_error",
        status_code=503,
    )

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([long_error]),
        chunks=_chunks(data),
        llm_journal=journal,
        sleep=_no_sleep,
    )

    lines = _lines(journal.path)
    assert len(lines[0]["error_message"]) == 500
    assert lines[0]["status_code"] == 503


# ---------------------------------------------------------------------------
# A2: первое вхождение останавливает фазу
# ---------------------------------------------------------------------------


def _phase_ports(evaluator: Any) -> Ports:
    """Порты для фазы с R1-профилем: ретривал работает, модель — скриптованная."""
    return Ports(
        evaluator=evaluator,
        embedder=_CountingEmbedder(),
        vector_index=_RecordingVectorIndex(),
    )


def test_phase_fails_on_first_auth_rejection() -> None:
    """Один ``401`` → фаза ``failed``, остаток пакета не тратится, причина названа.

    План — 2 продукта × 2 параметра = 4 ячейки; при мёртвом ключе должна состояться
    ровно **одна** попытка: это не «ошибка одной ячейки», а свойство прогона.
    """
    store = _store()
    evaluator = _ScriptedEvaluator([_auth_error(401)])
    try:
        result = evaluate_phase(
            plan=_plan(),
            ports=_phase_ports(evaluator),
            store=store,
            run_id=RUN_ID,
            profile="R1",
            cutoff_k=3,
        )
    finally:
        store.close()

    assert result.status == "failed"
    assert len(evaluator.calls) == 1  # ни повтора, ни остальных ячеек
    assert any(
        "auth_error: HTTP 401" in error and "API-ключ" in error
        for error in result.errors
    ), result.errors


@pytest.mark.parametrize("status", [401, 402, 403])
def test_phase_fails_on_every_terminal_status(status: int) -> None:
    """``402`` (средства) и ``403`` (доступ) останавливают фазу так же, как ``401``."""
    store = _store()
    evaluator = _ScriptedEvaluator([_auth_error(status, body=None)])
    try:
        result = evaluate_phase(
            plan=_plan(),
            ports=_phase_ports(evaluator),
            store=store,
            run_id=RUN_ID,
            profile="R1",
            cutoff_k=3,
        )
    finally:
        store.close()

    assert result.status == "failed"
    assert len(evaluator.calls) == 1
    assert any(f"HTTP {status}" in error for error in result.errors), result.errors


def test_phase_stays_ok_when_failures_are_not_credential_ones() -> None:
    """Изоляция одиночного отказа не сломана: ``400`` на всех ячейках — фаза ``ok``.

    ``failed``-ячейки — валидный исход измерения (I4), а не отказ прогона: прежнее
    поведение сохранено, и ``status="failed"`` теперь означает ровно одно — удостоверение
    отвергнуто. Повтор ``http_error`` проверяется отдельно (``evaluate_cell``), здесь
    ``max_retries=0`` — чтобы тест не ждал реальные паузы (фаза не принимает ``sleep``).
    """
    store = _store()
    evaluator = _ScriptedEvaluator([_auth_error(400, body="bad request")])
    try:
        result = evaluate_phase(
            plan=_plan(),
            ports=_phase_ports(evaluator),
            store=store,
            run_id=RUN_ID,
            profile="R1",
            cutoff_k=3,
            evaluation_policy={"max_retries": 0},
        )
    finally:
        store.close()

    assert result.status == "ok"
    # 4 ячейки × 1 попытка: все отработали (и все провалились), фаза не остановлена.
    assert len(evaluator.calls) == 4
    assert result.cells
    assert all(cell.state.value == "failed" for cell in result.cells)
