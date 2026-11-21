"""Диалог оценщика с **локальными** инструментами (C1, план 28 §9; I18/I22).

Проверяется то, что видно только по артефактам исполнения:

* пустой профиль — **ровно** прежний путь: один вызов ``complete``, ни одного хода,
  ни одного вызова инструмента, пустая трасса. Путь «без инструментов» обязан оставаться
  выразимым, иначе сравнение «с инструментами» и «без» перестало бы быть сравнением двух
  объявленных профилей;
* профиль с инструментом: модель просит вызов, получает результат, затем даёт вердикт —
  трасса содержит вызов, а результат попадает в промпт следующего хода;
* транскрипт детерминирован по форме: два одинаковых прогона дают побайтово тот же промпт
  второго хода (без временных меток и без порядка, зависящего от словаря);
* бюджет исчерпан — видимый факт (заметка + финальный ход обычным путём), а ячейка **не**
  становится ``failed`` от самого исчерпания;
* отказ инструмента (``ok=False``) ячейку не роняет: модель получает причину и продолжает;
* включённый профиль без транспорта/набора — ошибка сборки (``ToolWiringError``), а не
  тихий откат к пути без инструментов;
* ``value``-параметр публикует величину строкой ``value=<...>`` в ``validation_notes``:
  поля ``value`` у доменной ``Cell`` нет (чужая полоса), и величина не теряется.

Оценщик и инструменты — in-memory двойники: сеть и БД не нужны (I18). Нейтрально к
предметной категории (§1.3).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    CellState,
    Importance,
    LLMCellOutput,
    ParamClass,
    ParamType,
    Status,
)
from core.domain.doc_tables import ExtractedTable, TableRef
from core.domain.tools import (
    NO_TOOL_PROFILE,
    ToolInvocation,
    ToolOutcome,
    ToolProfile,
    ToolSpec,
)
from core.pipeline.evaluate import (
    TOOL_BUDGET_EXHAUSTED_REASON,
    TOOL_MODE_DECODER_NOTE,
    ToolWiringError,
    evaluate_cell,
)
from core.pipeline.prompt import _document_id
from core.ports.llm import LLMRequest, LLMResponse, ToolTurn, ToolTurnRequest
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk

RUN_ID = "run-tools-1"
PRODUCT_ID = "p1"
BUILD_ID = "cb_tools"
CHUNK_ID = "chunk_p1_1"
QUOTE = "Свойство Альфа доступно без дополнительной настройки."

_SPEC = ToolSpec(
    name="section_chunks",
    description="Чанки того же раздела документа, что и адресованный чанк.",
    parameters={
        "type": "object",
        "properties": {"chunk_id": {"type": "string"}},
        "required": ["chunk_id"],
    },
)


# ---------------------------------------------------------------------------
# Данные и двойники
# ---------------------------------------------------------------------------


def _param(*, param_class: ParamClass | None = None) -> CatalogRow:
    return CatalogRow(
        param_id="param_alpha",
        title="Параметр Альфа",
        module="mod_a",
        group="grp_1",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        param_class=param_class,
        rule="Подтвердить наличие свойства Альфа.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="role_a",
        concept_terms={"ru": ["свойство альфа"], "en": ["alpha feature"]},
        search_langs=["ru"],
    )


def _chunk() -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=CHUNK_ID,
        product_id=PRODUCT_ID,
        text=QUOTE,
        content_hash="hash_1",
        heading_path="Раздел 1",
        build_id=BUILD_ID,
        score=1.0,
    )


def _ok_output(value: str | None = None) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство Альфа подтверждено документацией.",
        evidence=[{"source_id": CHUNK_ID, "quote": QUOTE}],
        value=value,
    )


def _final(value: str | None = None) -> ToolTurn:
    output = _ok_output(value)
    return ToolTurn(
        final=LLMResponse(
            output=output,
            raw=output.model_dump_json(),
            prompt_hash="ph-turn",
            decoder="none",
        ),
        raw=output.model_dump_json(),
        finish_reason="stop",
    )


def _tool_turn() -> ToolTurn:
    return ToolTurn(
        tool_calls=(ToolInvocation(name="section_chunks", arguments={"chunk_id": CHUNK_ID}),),
        raw="{}",
        finish_reason="tool_calls",
    )


class _ScriptedEvaluator:
    """``EvaluatorPort``-двойник: записывает запросы, отдаёт скрипт (последний повторяется)."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0
    decoder = "json_schema"

    def __init__(self, outputs: Sequence[LLMCellOutput]) -> None:
        self.outputs = list(outputs)
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        output = self.outputs[min(len(self.calls) - 1, len(self.outputs) - 1)]
        return LLMResponse(
            output=output,
            raw=output.model_dump_json(),
            prompt_hash="ph",
            decoder=self.decoder,
        )


class _ScriptedCaller:
    """``LocalToolCallerPort``-двойник: ходы по сценарию (последний повторяется)."""

    def __init__(self, turns: Sequence[ToolTurn]) -> None:
        self.turns = list(turns)
        self.requests: list[ToolTurnRequest] = []

    def turn(self, req: ToolTurnRequest) -> ToolTurn:
        self.requests.append(req)
        return self.turns[min(len(self.requests) - 1, len(self.turns) - 1)]


class _ForbiddenCaller:
    """Транспорт, который не должен вызываться вовсе (пустой профиль)."""

    def __init__(self) -> None:
        self.requests: list[ToolTurnRequest] = []

    def turn(self, req: ToolTurnRequest) -> ToolTurn:  # pragma: no cover - не должен зваться
        self.requests.append(req)
        raise AssertionError("turn вызван на пустом профиле инструментов")


class _FakeTools:
    """``LocalToolPort``-двойник: объявленный набор и записанные вызовы."""

    def __init__(
        self,
        outcomes: dict[str, ToolOutcome] | None = None,
        specs: Sequence[ToolSpec] = (_SPEC,),
    ) -> None:
        self._outcomes = dict(outcomes or {})
        self._specs = tuple(specs)
        self.calls: list[ToolInvocation] = []

    def specs(self) -> tuple[ToolSpec, ...]:
        return self._specs

    def invoke(self, invocation: ToolInvocation) -> ToolOutcome:
        self.calls.append(invocation)
        preset = self._outcomes.get(invocation.name)
        if preset is not None:
            return preset
        return ToolOutcome(
            name=invocation.name,
            content="Раздел 1: свойство Альфа описано в двух абзацах.",
            ok=True,
        )


_PROFILE = ToolProfile(name="local", tools=("section_chunks",), max_calls=2)


def _evaluate(
    *,
    evaluator: Any,
    tool_profile: ToolProfile = NO_TOOL_PROFILE,
    tool_caller: Any = None,
    local_tools: Any = None,
    param: CatalogRow | None = None,
) -> Any:
    return evaluate_cell(
        param=param or _param(),
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=evaluator,
        chunks=[_chunk()],
        tool_profile=tool_profile,
        tool_caller=tool_caller,
        local_tools=local_tools,
    )


# ---------------------------------------------------------------------------
# Пустой профиль — прежний путь без инструментов
# ---------------------------------------------------------------------------


def test_empty_profile_calls_complete_once_and_never_touches_tools() -> None:
    evaluator = _ScriptedEvaluator([_ok_output()])
    caller = _ForbiddenCaller()
    tools = _FakeTools()

    outcome = _evaluate(evaluator=evaluator, tool_caller=caller, local_tools=tools)

    assert len(evaluator.calls) == 1
    assert caller.requests == []
    assert tools.calls == []
    assert outcome.tool_trace == ()
    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.OK
    # Заметка о режиме инструментов не появляется: режима не было.
    assert not any(TOOL_MODE_DECODER_NOTE in note for note in outcome.cell.validation_notes)


# ---------------------------------------------------------------------------
# Профиль с инструментом: вызов → результат → вердикт
# ---------------------------------------------------------------------------


def test_tool_call_is_executed_and_result_reaches_the_next_turn() -> None:
    evaluator = _ScriptedEvaluator([_ok_output()])
    caller = _ScriptedCaller([_tool_turn(), _final()])
    tools = _FakeTools()

    outcome = _evaluate(
        evaluator=evaluator,
        tool_profile=_PROFILE,
        tool_caller=caller,
        local_tools=tools,
    )

    # Модель попросила вызов, получила результат и лишь затем дала вердикт.
    assert len(caller.requests) == 2
    # Объявленные инструменты названы в промпте первого хода (иначе модель о них не знает).
    assert "Доступны локальные инструменты: section_chunks" in caller.requests[0].prompt
    assert evaluator.calls == []  # вердикт пришёл ходом, а не отдельным complete
    assert [call.name for call in tools.calls] == ["section_chunks"]
    assert [entry.name for entry in outcome.tool_trace] == ["section_chunks"]
    entry = outcome.tool_trace[0]
    assert entry.seq == 1
    assert entry.ok is True
    assert entry.chars > 0
    assert entry.arguments == {"chunk_id": CHUNK_ID}
    # Результат инструмента — в промпте следующего хода, а не только в трассе.
    assert "Раздел 1: свойство Альфа описано" in caller.requests[1].prompt
    assert outcome.cell.status is Status.OK
    # Провенанс режима: грамматика не запрашивалась, и это видно в заметках.
    assert any(TOOL_MODE_DECODER_NOTE in note for note in outcome.cell.validation_notes)


def test_transcript_is_deterministic_in_form() -> None:
    """Два одинаковых прогона дают побайтово тот же промпт второго хода."""
    prompts: list[str] = []
    for _ in range(2):
        caller = _ScriptedCaller([_tool_turn(), _final()])
        _evaluate(
            evaluator=_ScriptedEvaluator([_ok_output()]),
            tool_profile=_PROFILE,
            tool_caller=caller,
            local_tools=_FakeTools(),
        )
        prompts.append(caller.requests[1].prompt)

    assert prompts[0] == prompts[1]


def test_tool_arguments_are_in_fixed_sorted_order() -> None:
    """Транскрипт не зависит от порядка ключей аргументов (детерминизм формы)."""
    ordered = ToolInvocation(name="section_chunks", arguments={"b": 2, "a": 1})
    reversed_ = ToolInvocation(name="section_chunks", arguments={"a": 1, "b": 2})
    outcome = ToolOutcome(name="section_chunks", content="ok", ok=True)

    from core.pipeline.evaluate import _render_tool_result

    assert _render_tool_result(1, ordered, outcome) == _render_tool_result(
        1, reversed_, outcome
    )


# ---------------------------------------------------------------------------
# Бюджет и отказы инструментов
# ---------------------------------------------------------------------------


def test_budget_exhaustion_is_visible_and_cell_is_not_failed() -> None:
    evaluator = _ScriptedEvaluator([_ok_output()])
    profile = ToolProfile(name="one", tools=("section_chunks",), max_calls=1)
    # Модель просит вызов и после первого (единственного) продолжает просить.
    caller = _ScriptedCaller([_tool_turn(), _tool_turn(), _tool_turn()])

    outcome = _evaluate(
        evaluator=evaluator,
        tool_profile=profile,
        tool_caller=caller,
        local_tools=_FakeTools(),
    )

    # Бюджет израсходован, финальный ход — обычным путём оценщика.
    assert len(evaluator.calls) == 1
    assert any(
        TOOL_BUDGET_EXHAUSTED_REASON in note for note in outcome.cell.validation_notes
    )
    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.OK
    # Бюджет — объявленное число: второй вызов не исполнялся.
    assert [entry.name for entry in outcome.tool_trace] == ["section_chunks"]


def test_failed_tool_call_does_not_fail_the_cell() -> None:
    evaluator = _ScriptedEvaluator([_ok_output()])
    caller = _ScriptedCaller([_tool_turn(), _final()])
    tools = _FakeTools(
        outcomes={
            "section_chunks": ToolOutcome(
                name="section_chunks",
                content="адрес не найден в корпусе прогона",
                ok=False,
                reason="unknown_address",
            )
        }
    )

    outcome = _evaluate(
        evaluator=evaluator,
        tool_profile=_PROFILE,
        tool_caller=caller,
        local_tools=tools,
    )

    assert outcome.tool_trace[0].ok is False
    assert outcome.tool_trace[0].reason == "unknown_address"
    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.OK
    # Причина отказа — в промпте следующего хода: модель видит, что инструмент не ответил.
    assert "unknown_address" in caller.requests[1].prompt


# ---------------------------------------------------------------------------
# Сборка: включённый профиль требует транспорта и набора
# ---------------------------------------------------------------------------


def test_enabled_profile_without_caller_is_a_visible_error() -> None:
    with pytest.raises(ToolWiringError):
        _evaluate(
            evaluator=_ScriptedEvaluator([_ok_output()]),
            tool_profile=_PROFILE,
            tool_caller=None,
            local_tools=_FakeTools(),
        )


def test_enabled_profile_without_local_tools_is_a_visible_error() -> None:
    with pytest.raises(ToolWiringError):
        _evaluate(
            evaluator=_ScriptedEvaluator([_ok_output()]),
            tool_profile=_PROFILE,
            tool_caller=_ScriptedCaller([_final()]),
            local_tools=None,
        )


def test_declared_tool_missing_from_the_port_is_a_visible_error() -> None:
    """Профиль объявляет инструмент, которого порт не даёт: прогон мерил бы другое."""
    other = ToolSpec(name="document_tables", description="Таблицы документа.")
    with pytest.raises(ToolWiringError):
        _evaluate(
            evaluator=_ScriptedEvaluator([_ok_output()]),
            tool_profile=_PROFILE,
            tool_caller=_ScriptedCaller([_final()]),
            local_tools=_FakeTools(specs=(other,)),
        )


# ---------------------------------------------------------------------------
# value-параметр: величина публикуется строкой
# ---------------------------------------------------------------------------


def test_value_parameter_publishes_the_value_as_a_note() -> None:
    outcome = _evaluate(
        evaluator=_ScriptedEvaluator([_ok_output(value="18.4")]),
        param=_param(param_class=ParamClass.VALUE),
    )

    assert "value=18.4" in outcome.cell.validation_notes


def test_value_parameter_without_a_value_is_a_visible_fact() -> None:
    outcome = _evaluate(
        evaluator=_ScriptedEvaluator([_ok_output(value=None)]),
        param=_param(param_class=ParamClass.VALUE),
    )

    assert any(note.startswith("value_missing") for note in outcome.cell.validation_notes)


def test_capability_parameter_does_not_publish_value_notes() -> None:
    outcome = _evaluate(
        evaluator=_ScriptedEvaluator([_ok_output(value="18.4")]),
        param=_param(param_class=ParamClass.CAPABILITY),
    )

    assert not any(note.startswith("value=") for note in outcome.cell.validation_notes)


# ---------------------------------------------------------------------------
# Таблицы в промпте: объект передаётся, отсутствие объекта видно
# ---------------------------------------------------------------------------


def _table_chunk() -> Chunk:
    return Chunk(
        chunk_id=CHUNK_ID,
        chunk_no=1,
        text=QUOTE,
        heading_path="Раздел 1",
        context_prefix="",
        content_hash="hash_1",
        table_ref=TableRef(table_id="t1", row_start=1, row_end=1),
    )


def test_tables_are_passed_into_the_prompt() -> None:
    evaluator = _ScriptedEvaluator([_ok_output()])
    table = ExtractedTable(
        table_id="t1",
        caption="Свойства Альфа",
        header=("свойство", "значение"),
        rows=(("альфа", "включено"),),
    )

    outcome = evaluate_cell(
        param=_param(),
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=evaluator,
        chunks=[_table_chunk()],
        # Ключ карты парный и первый компонент — **адрес документа из ``chunk_id``**
        # (тот же, что печатает строка ``document:`` шапки), а не ``corpus_docs.doc_id``:
        # промпт ищет таблицу именно по нему. У этого адреса документа нет суффикса
        # ``:N``, поэтому адрес пуст — это и есть его документ в терминах промпта.
        tables={(_document_id(CHUNK_ID), "t1"): table},
    )

    assert len(evaluator.calls) == 1
    prompt = evaluator.calls[0].prompt
    assert "Свойства Альфа" in prompt
    assert "свойство | значение" in prompt
    assert "альфа | включено" in prompt
    assert outcome.cell.status is Status.OK


def test_table_address_without_object_is_a_visible_degradation() -> None:
    """Адрес есть, объекта нет — пометка в промпте, а не падение и не молчание."""
    evaluator = _ScriptedEvaluator([_ok_output()])

    outcome = evaluate_cell(
        param=_param(),
        product_id=PRODUCT_ID,
        run_id=RUN_ID,
        evaluator=evaluator,
        chunks=[_table_chunk()],
        tables=None,
    )

    assert "адрес известен, объект не передан" in evaluator.calls[0].prompt
    assert outcome.cell.status is Status.OK


# ---------------------------------------------------------------------------
# Трасса — провенанс, а не вход
# ---------------------------------------------------------------------------


def test_trace_is_json_serialisable_provenance() -> None:
    outcome = _evaluate(
        evaluator=_ScriptedEvaluator([_ok_output()]),
        tool_profile=_PROFILE,
        tool_caller=_ScriptedCaller([_tool_turn(), _final()]),
        local_tools=_FakeTools(),
    )

    payload = [
        {
            "seq": entry.seq,
            "name": entry.name,
            "arguments": dict(entry.arguments),
            "ok": entry.ok,
            "chars": entry.chars,
            "reason": entry.reason,
        }
        for entry in outcome.tool_trace
    ]
    assert json.loads(json.dumps(payload)) == payload
