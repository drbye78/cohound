"""Журнал вызовов оценщика: ``runs/{run_id}/logs/llm_calls.jsonl`` (строка на **вызов**).

Зачем артефакт. До правки путь отказа Ш6 был неразбираем: ``eval_cache`` пишет только
**успешный** вердикт, ``prompts/`` — только вход. В прогоне ``ru-pg-dbms-c2e7b860ac17``
21 ячейка имела ``status=null`` (18 — ``finish_reason=length`` при ``max_tokens=4096``,
2 — невалидный JSON), и что именно вернула модель, по артефактам восстановить было
нельзя. Здесь фиксируется то, что обязано держаться:

* строка на **попытку**, не на ячейку: повторы видны поштучно (``attempt``);
* на отказе сохраняется сырой текст ответа, метка остановки и расход токенов;
* ``prompt_text_sha256`` — контентный адрес входа (``prompt_hash`` §5 не покрывает
  product/title/текст чанков);
* сбой записи журнала не отнимает уже полученные вердикты (виден в ``errors``).

Фикстуры берутся из ``test_evaluate``/``test_evaluate_batching``; сеть не используется,
данные категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.adapters.llm_local import EvaluationError
from core.domain import Status
from core.pipeline.evaluate import (
    LLM_CALL_OUTCOMES,
    LLMCallJournal,
    evaluate_cell,
    evaluate_phase,
)
from core.pipeline.exhaustion import ExhaustionFacts
from core.pipeline.prompt import compile_prompt
from core.pipeline.runner import Ports
from core.ports.llm import LLMResponse, TokenUsage
from core.util.hashing import content_hash
from tests.fakes.ports import FakeEvaluator
from tests.pipeline.test_evaluate import (
    _bad_output,
    _chunks,
    _data,
    _ok_output,
    _param,
    _ScriptedEvaluator,
)
from tests.pipeline.test_evaluate_batching import (
    _CountingEmbedder,
    _plan,
    _RecordingVectorIndex,
    _store,
)

FIELDS = {
    "attempt",
    "cell_key",
    "decoder",
    "finish_reason",
    "latency_ms",
    "max_tokens",
    "model_id",
    "outcome",
    "prompt_hash",
    "prompt_text_sha256",
    "reasoning_content",
    "reasoning_len",
    "reasoning_sha256",
    "response_len",
    "response_raw",
    "response_sha256",
    "run_id",
    "ts",
    "usage",
}


def _lines(path: Path) -> list[dict[str, Any]]:
    assert path.is_file(), f"журнала нет: {path}"
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _journal(tmp_path: Path, data: dict[str, Any] | None = None) -> LLMCallJournal:
    run_id = (data or _data())["run_id"]
    return LLMCallJournal(tmp_path / "logs" / "llm_calls.jsonl", run_id)


# ---------------------------------------------------------------------------
# Успешный вызов / повтор / отказы: строка на попытку, все поля на месте
# ---------------------------------------------------------------------------


def test_success_writes_one_line_per_call_with_all_fields(tmp_path: Path) -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    journal = _journal(tmp_path)
    expected_hash = compile_prompt(
        param=_param(data),
        product_id=data["product_id"],
        chunks=_chunks(data),
    ).prompt_hash

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        llm_journal=journal,
    )

    lines = _lines(journal.path)
    assert len(lines) == 1  # одна попытка — одна строка
    line = lines[0]
    assert set(line) == FIELDS
    assert line["run_id"] == data["run_id"]
    assert line["cell_key"] == f"{data['product_id']}/{_param(data).param_id}"
    assert line["attempt"] == 1
    assert line["prompt_hash"] == expected_hash
    assert line["prompt_text_sha256"] == content_hash(evaluator.calls[0].prompt)
    assert line["model_id"] == "fake-model"
    assert line["outcome"] == "ok"
    assert line["response_raw"] == outcome.raw
    assert line["response_sha256"] == content_hash(outcome.raw or "")
    assert line["response_len"] == len(outcome.raw or "")
    assert isinstance(line["latency_ms"], float) and line["latency_ms"] >= 0.0
    assert "ts" in line
    assert journal.written == 1


def test_length_failure_keeps_finish_reason_usage_and_empty_raw(tmp_path: Path) -> None:
    """Потолок генерации: ``finish_reason=length`` и расход видны **в журнале**.

    Это ровно тот случай, который был неразбираем: пустой ``content`` + ``length``
    приходил как ``EvaluationError``, кэш ничего не писал, и отличить обрезание от
    невалидного JSON по артефактам было нельзя.
    """
    data = _data()
    error = EvaluationError(
        "генерация оборвана по потолку токенов (finish_reason=length, max_tokens=4096)",
        reason="length",
        finish_reason="length",
        raw="",
        usage=TokenUsage(prompt_tokens=1111, completion_tokens=4096),
    )
    evaluator = _ScriptedEvaluator([error])
    journal = _journal(tmp_path)

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        llm_journal=journal,
    )

    lines = _lines(journal.path)
    assert len(lines) == 3  # max_retries=2 → три попытки, каждая — строка
    assert [line["attempt"] for line in lines] == [1, 2, 3]
    for line in lines:
        assert line["outcome"] == "length"
        assert line["finish_reason"] == "length"
        assert line["usage"] == {"prompt": 1111, "completion": 4096}
        assert line["response_raw"] == ""  # ответ был, но пустой
        assert line["response_len"] == 0
        assert line["response_sha256"] == content_hash("")
    # I4 не ослаблен: failed не несёт статус.
    assert outcome.cell.state.value == "failed"
    assert outcome.cell.status is None


def test_invalid_json_failure_keeps_raw_response_text(tmp_path: Path) -> None:
    data = _data()
    raw = '{"status": "✅", "justification": "обрыв'
    error = EvaluationError(
        "оценщик вернул невалидный JSON/форму (1 ошибок): boom",
        reason="invalid_json",
        raw=raw,
        usage=TokenUsage(prompt_tokens=10, completion_tokens=5),
    )
    journal = _journal(tmp_path)

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([error]),
        chunks=_chunks(data),
        llm_journal=journal,
    )

    first = _lines(journal.path)[0]
    assert first["outcome"] == "invalid_json"
    assert first["response_raw"] == raw
    assert first["response_sha256"] == content_hash(raw)
    assert first["response_len"] == len(raw)
    assert first["finish_reason"] is None


def test_plain_exception_is_journaled_without_response(tmp_path: Path) -> None:
    data = _data()
    journal = _journal(tmp_path)

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([RuntimeError("таймаут адаптера")]),
        chunks=_chunks(data),
        llm_journal=journal,
    )

    lines = _lines(journal.path)
    assert len(lines) == 3
    for line in lines:
        assert line["outcome"] == "exception"
        assert line["response_raw"] is None
        assert line["response_sha256"] is None
        assert line["response_len"] is None
        assert line["usage"] is None


def test_validation_failure_outcome_is_quote_not_substring(tmp_path: Path) -> None:
    """Ответ разобран, но цитата не подтверждена (I22) — отдельный исход."""
    data = _data()
    bad = _bad_output(data)
    journal = _journal(tmp_path)

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([bad]),
        chunks=_chunks(data),
        llm_journal=journal,
    )

    first = _lines(journal.path)[0]
    assert first["outcome"] == "quote_not_substring"
    assert first["response_raw"] == bad.model_dump_json()
    assert set(LLM_CALL_OUTCOMES) >= {first["outcome"]}


# ---------------------------------------------------------------------------
# Фаза: файл лежит под runs/, сбои записи не отнимают вердикты
# ---------------------------------------------------------------------------


STATUS_OK_JUSTIFICATION = "Свойство подтверждено документацией."


def _phase_ports(
    plan: object, *, status: Status = Status.OK
) -> FakeEvaluator:
    """Оценщик, отвечающий по промпту **своего** продукта.

    Чанк ``chunk-{product}`` лежит только в срезе своего продукта, поэтому evidence
    другого продукта невалиден. Ключ — отрендеренный промпт: он и есть вход модели.
    """
    from core.domain import LLMCellOutput
    from core.ports.vector_index import RetrievedChunk

    outputs: dict[str, LLMCellOutput] = {}
    for product_id in plan.products:  # type: ignore[attr-defined]
        chunk_id = f"chunk-{product_id}"
        chunk = RetrievedChunk(
            chunk_id=chunk_id,
            product_id=product_id,
            text=f"текст {chunk_id}",
            content_hash=f"hash-{chunk_id}",
            heading_path=f"H-{chunk_id}",
            build_id="build-vec",
            score=0.5,
        )
        evidence = (
            [{"source_id": chunk_id, "quote": f"текст {chunk_id}"}]
            if status is Status.OK
            else []
        )
        output = LLMCellOutput(
            status=status,
            justification=STATUS_OK_JUSTIFICATION
            if status is Status.OK
            else "В переданных фрагментах утверждения нет.",
            evidence=evidence,
        )
        for param in plan.params:  # type: ignore[attr-defined]
            compiled = compile_prompt(param=param, product_id=product_id, chunks=[chunk])
            outputs[compiled.text] = output
    evaluator = FakeEvaluator(outputs=outputs)
    #: Как у self-host оценщика: модель объявлена — провенанс ячейки её назовёт.
    evaluator.model_id = "fake-model"  # type: ignore[attr-defined]
    return evaluator


def test_evaluate_phase_writes_journal_under_the_run_directory(tmp_path: Path) -> None:
    store = _store()
    plan = _plan()
    evaluator = _phase_ports(plan)
    ports = Ports(
        evaluator=evaluator,
        embedder=_CountingEmbedder(),
        vector_index=_RecordingVectorIndex(),
    )
    journal_path = tmp_path / plan.run_id / "logs" / "llm_calls.jsonl"
    try:
        result = evaluate_phase(
            plan=plan,
            ports=ports,
            store=store,
            run_id=plan.run_id,
            profile="R1",
            cutoff_k=3,
            llm_journal=journal_path,
        )
    finally:
        store.close()

    assert result.status == "ok"
    assert result.errors == []
    lines = _lines(journal_path)
    # 2 продукта × 2 параметра — по одному успешному вызову на ячейку (повторов нет).
    assert len(lines) == 4
    assert {line["cell_key"].split("/")[0] for line in lines} == {"p1", "p2"}
    assert {line["outcome"] for line in lines} == {"ok"}
    assert all(line["prompt_text_sha256"] for line in lines)
    summary = [entry for entry in result.log_entries if "журнал вызовов" in str(entry)]
    assert summary and "4 записей" in summary[0]


def test_journal_write_failure_is_visible_and_does_not_steal_verdicts(
    tmp_path: Path,
) -> None:
    """Диск сломался: вердикт остаётся, причина видна в ``errors`` фазы."""
    store = _store()
    plan = _plan()
    evaluator = _phase_ports(plan)
    ports = Ports(
        evaluator=evaluator,
        embedder=_CountingEmbedder(),
        vector_index=_RecordingVectorIndex(),
    )
    journal_path = tmp_path / "logs" / "llm_calls.jsonl"
    # Сбой I/O: на месте файла — каталог, поэтому open("a") даёт OSError.
    journal_path.mkdir(parents=True)
    try:
        result = evaluate_phase(
            plan=plan,
            ports=ports,
            store=store,
            run_id=plan.run_id,
            profile="R1",
            cutoff_k=3,
            llm_journal=journal_path,
        )
        rows = list(
            store.conn.execute(
                "SELECT COUNT(*) FROM cells WHERE run_id = ? AND state = 'evaluated'",
                (plan.run_id,),
            )
        )
    finally:
        store.close()

    assert result.status == "ok"
    assert rows[0][0] == 4  # вердикты не отняты
    assert any("journal_write_failed" in str(err) for err in result.errors)


# ---------------------------------------------------------------------------
# Провенанс ❓: модель вызывалась — он обязан пережить протокол исчерпания
# ---------------------------------------------------------------------------


def test_unknown_cell_keeps_model_provenance_after_exhaustion_protocol(
    tmp_path: Path,
) -> None:
    """``❓`` не теряет ``prompt_hash``/``model_id``/``llm_scenario``/``justification``.

    Регрессия: протокол исчерпания пере-сохранял ``❓``-ячейку через ``INSERT OR REPLACE``,
    и колонки, которых доменная ``Cell`` не несёт, обнулялись — измерено на
    ``ru-pg-dbms-c2e7b860ac17``: 271 ``❓`` (все) с NULL против 251 с провенансом.
    """
    store = _store()
    plan = _plan()
    evaluator = _phase_ports(plan, status=Status.UNKNOWN)
    ports = Ports(
        evaluator=evaluator,
        embedder=_CountingEmbedder(),
        vector_index=_RecordingVectorIndex(),
    )
    try:
        result = evaluate_phase(
            plan=plan,
            ports=ports,
            store=store,
            run_id=plan.run_id,
            profile="R1",
            cutoff_k=3,
            # Факты исчерпания включают пере-сохранение каждой ❓-ячейки протоколом.
            exhaustion_facts=ExhaustionFacts(
                contours_run={"p1": frozenset(), "p2": frozenset()}
            ),
        )
        rows = list(
            store.conn.execute(
                "SELECT status, prompt_hash, model_id, llm_scenario, justification, "
                "evidence_scope FROM cells WHERE run_id = ?",
                (plan.run_id,),
            )
        )
    finally:
        store.close()

    assert result.status == "ok"
    assert len(rows) == 4
    for row in rows:
        assert row["status"] == Status.UNKNOWN.value
        assert row["prompt_hash"]  # не NULL
        assert row["model_id"] == "fake-model"
        assert row["llm_scenario"] == "evaluator"
        assert row["justification"]
        # Протокол исчерпания действительно записан (терминация в векторе охвата).
        assert "not_found" in (row["evidence_scope"] or "")


def test_no_fragments_keeps_model_id_none(tmp_path: Path) -> None:
    """Пустой срез: модель не вызывалась — ``model_id`` остаётся ``None`` (не выдумка)."""
    from core.store.sqlite_store import SqliteStore
    from tests.pipeline.test_eval_cache import _run_config

    data = _data()
    with SqliteStore(":memory:") as store:
        store.create_run(data["run_id"], _run_config())
        outcome = evaluate_cell(
            param=_param(data),
            product_id=data["product_id"],
            run_id=data["run_id"],
            evaluator=_ScriptedEvaluator([_ok_output(data)]),
            chunks=[],
            store=store,
            llm_journal=_journal(tmp_path),
        )
        row = store.conn.execute(
            "SELECT model_id, prompt_hash, llm_scenario, justification FROM cells "
            "WHERE run_id = ? AND product_id = ? AND param_id = ?",
            (data["run_id"], data["product_id"], _param(data).param_id),
        ).fetchone()

    assert outcome.cell.status is Status.UNKNOWN
    assert row["model_id"] is None
    assert row["prompt_hash"]  # промпт ячейки существует — hash честный
    assert row["llm_scenario"] is None
    assert row["justification"] is None
    # Вызова не было — строки журнала нет.
    assert not (
        tmp_path / "logs" / "llm_calls.jsonl"
    ).exists()


def test_journal_is_not_in_execution_contract() -> None:
    """Журнал — провенанс исполнения: в контракт ``run_id`` он не входит."""
    from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS

    assert "llm_journal" not in EXECUTION_CONTRACT_FIELDS
    assert "llm_calls" not in EXECUTION_CONTRACT_FIELDS

# ---------------------------------------------------------------------------
# Рассуждение модели: длина/хеш — всегда, текст — только на отказе
# ---------------------------------------------------------------------------


class _ReasoningEvaluator:
    """Оценщик, возвращающий рассуждение (как провайдер, отдающий ``reasoning_content``)."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0

    def __init__(self, output: Any, reasoning: str) -> None:
        self._output = output
        self._reasoning = reasoning
        self.calls: list[Any] = []

    def complete(self, req: Any) -> LLMResponse:
        self.calls.append(req)
        return LLMResponse(
            output=self._output,
            raw=self._output.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
            usage=TokenUsage(prompt_tokens=10, completion_tokens=20, reasoning_tokens=7),
            reasoning=self._reasoning,
        )


def test_success_journals_reasoning_length_and_hash_but_not_text(tmp_path: Path) -> None:
    """На успехе текст рассуждения в журнал не пишется (дублирует вердикт и растёт).

    Длина и хеш пишутся **всегда**: по ним видно, думала ли модель и сколько, — это
    провенанс, которого у вердикта нет.
    """
    data = _data()
    reasoning = "модель рассуждала: цитата подтверждает функцию"
    journal = _journal(tmp_path)

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ReasoningEvaluator(_ok_output(data), reasoning),
        chunks=_chunks(data),
        llm_journal=journal,
    )

    line = _lines(journal.path)[0]
    assert line["outcome"] == "ok"
    assert line["reasoning_len"] == len(reasoning)
    assert line["reasoning_sha256"] == content_hash(reasoning)
    assert line["reasoning_content"] is None
    assert line["usage"] == {"prompt": 10, "completion": 20, "reasoning": 7}


def test_failure_journals_the_full_reasoning_text(tmp_path: Path) -> None:
    """На отказе рассуждение — часто единственный текст ответа: он сохраняется целиком."""
    data = _data()
    reasoning = "думал: бюджет кончился на середине ответа"
    error = EvaluationError(
        "генерация оборвана по потолку токенов (finish_reason=length, max_tokens=4096)",
        reason="length",
        finish_reason="length",
        raw="",
        usage=TokenUsage(prompt_tokens=5, completion_tokens=4096),
    )
    error.reasoning = reasoning
    journal = _journal(tmp_path)

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([error]),
        chunks=_chunks(data),
        llm_journal=journal,
    )

    first = _lines(journal.path)[0]
    assert first["outcome"] == "length"
    assert first["reasoning_len"] == len(reasoning)
    assert first["reasoning_sha256"] == content_hash(reasoning)
    assert first["reasoning_content"] == reasoning


def test_absent_reasoning_is_not_invented(tmp_path: Path) -> None:
    """Провайдер не отдал рассуждение — ``None``, а не пустая строка (не выдумываем)."""
    data = _data()
    journal = _journal(tmp_path)
    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([_ok_output(data)]),
        chunks=_chunks(data),
        llm_journal=journal,
    )

    line = _lines(journal.path)[0]
    assert line["reasoning_len"] is None
    assert line["reasoning_sha256"] is None
    assert line["reasoning_content"] is None
