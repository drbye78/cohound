"""Ш6: ремонт полей отрицания при наличии-вердикте и замер охвата доказательствами.

Живая проба оценщика (``evaluator_v3``) дала на ячейке ``✅`` дополнительно заполненные
``denial_strength``/``denial_reason``; прежний валидатор отвергал ответ, и ячейка по I4
становилась ``failed`` — наблюдение терялось целиком из-за шумового поля. Здесь
фиксируются оба исхода ремонта (шум и противоречие) и судьба вердикта: ``✅`` сохраняется,
поля в артефакты не попадают, а сам факт ремонта виден в ``validation_notes``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.domain import (
    CatalogRow,
    CellState,
    DenialStrength,
    LLMCellOutput,
    LLMEvidence,
    Status,
)
from core.pipeline.evaluate import (
    PRESENT_DENIAL_REPAIR_PREFIX,
    evaluate_cell,
)
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.types import Chunk

FIXTURES = Path(__file__).parent / "fixtures"


def _data() -> dict:
    return json.loads((FIXTURES / "evaluate_inputs.json").read_text(encoding="utf-8"))


def _param(data: dict) -> CatalogRow:
    return CatalogRow(**data["param"])


def _chunks(data: dict) -> list[Chunk]:
    return [Chunk(**c) for c in data["chunks"]]


class _ScriptedEvaluator:
    """Один ответ оценщика (без сети, I18)."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0
    decoder = "fake"

    def __init__(self, out: LLMCellOutput) -> None:
        self.out = out
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        return LLMResponse(
            output=self.out, raw=self.out.model_dump_json(), prompt_hash="ph", decoder="fake"
        )


def _ok_with_denial(**denial: object) -> LLMCellOutput:
    data = _data()
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство Альфа подтверждено документацией.",
        evidence=[
            LLMEvidence(
                source_id="chunk_a2",
                quote="Свойство Альфа доступно без дополнительной настройки.",
            )
        ],
        **denial,  # type: ignore[arg-type]
    )


def _evaluate(out: LLMCellOutput, data: dict | None = None):  # noqa: ANN202 - тестовый helper
    payload = data or _data()
    return evaluate_cell(
        param=_param(payload),
        product_id=payload["product_id"],
        run_id=payload["run_id"],
        evaluator=_ScriptedEvaluator(out),
        chunks=_chunks(payload),
    )


# ---------------------------------------------------------------------------
# Ремонт: шум (not_stated)
# ---------------------------------------------------------------------------

def test_ok_with_not_stated_keeps_verdict_and_records_repair() -> None:
    """``✅`` + ``not_stated`` ⇒ ячейка ``✅``, ремонт виден в ``validation_notes``."""
    outcome = _evaluate(
        _ok_with_denial(denial_strength="not_stated", denial_reason="не заявлено")
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.OK
    # I1/I22: доказательство наличия не потеряно.
    assert len(outcome.cell.evidence) == 1
    # Поля отрицания к вердикту не относятся — объекта силы отрицания у ✅ нет.
    assert outcome.cell.evidence_scope is None
    repair = [n for n in outcome.cell.validation_notes if n.startswith(PRESENT_DENIAL_REPAIR_PREFIX)]
    assert repair, outcome.cell.validation_notes
    assert "denial_strength=not_stated" in repair[0]
    # Шум — не противоречие: review_reason не трогается.
    assert outcome.cell.review_reason is None


def test_ok_with_denial_reason_only_records_repair() -> None:
    """Только ``denial_reason`` (без класса) — тот же ремонт, вердикт прежний."""
    outcome = _evaluate(_ok_with_denial(denial_reason="не заявлено"))

    assert outcome.cell.status is Status.OK
    repair = [n for n in outcome.cell.validation_notes if n.startswith(PRESENT_DENIAL_REPAIR_PREFIX)]
    assert repair, outcome.cell.validation_notes
    assert "denial_strength=none" in repair[0]
    assert outcome.cell.review_reason is None


def test_ok_without_denial_fields_has_no_repair_note() -> None:
    """Без полей отрицания записи о ремонте нет (нечего ремонтировать)."""
    outcome = _evaluate(_ok_with_denial())

    assert outcome.cell.status is Status.OK
    assert not [
        n
        for n in outcome.cell.validation_notes
        if n.startswith(PRESENT_DENIAL_REPAIR_PREFIX)
    ]


def test_prompt_is_not_modified_by_repair() -> None:
    """Ремонт не меняет промпт: ответ приходит на тот же запрос (иначе поехал бы prompt_hash)."""
    data = _data()
    evaluator = _ScriptedEvaluator(
        _ok_with_denial(denial_strength="not_stated", denial_reason="не заявлено")
    )
    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
    )
    assert len(evaluator.calls) == 1


# ---------------------------------------------------------------------------
# Ремонт: противоречие (explicit)
# ---------------------------------------------------------------------------

def test_ok_with_explicit_is_contradiction_visible_but_verdict_kept() -> None:
    """``✅`` + ``explicit`` ⇒ ``✅`` сохранён, противоречие видно в ``review_reason``."""
    outcome = _evaluate(
        _ok_with_denial(denial_strength="explicit", denial_reason="вендор отрицает")
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.OK
    assert len(outcome.cell.evidence) == 1
    assert outcome.cell.evidence_scope is None

    repair = [n for n in outcome.cell.validation_notes if n.startswith(PRESENT_DENIAL_REPAIR_PREFIX)]
    assert repair, outcome.cell.validation_notes
    # Тот же текст: читатель видит и запись о ремонте, и помеченный конфликт.
    assert outcome.cell.review_reason == repair[0]
    assert "denial_strength=explicit" in outcome.cell.review_reason


def test_absent_without_denial_is_still_downgraded() -> None:
    """``❌`` без класса отрицания по-прежнему не публикуется как ``❌`` (не ослаблено)."""
    data = _data()
    out = LLMCellOutput(
        status=Status.ABSENT,
        justification="не найдено в переданном срезе",
        evidence=[],
    )
    outcome = _evaluate(out, data)

    assert outcome.cell.status is Status.UNKNOWN
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.denial_strength in {
        DenialStrength.NOT_STATED,
        DenialStrength.UNOBSERVED,
    }


# ---------------------------------------------------------------------------
# relevant_evidence_coverage — настоящий замер
# ---------------------------------------------------------------------------

def test_relevant_evidence_coverage_counts_cited_chunks() -> None:
    """1 из 3 переданных фрагментов процитирован ⇒ 1/3."""
    outcome = _evaluate(
        LLMCellOutput(
            status=Status.UNKNOWN,
            justification="установить не удалось",
            denial_reason="фрагменты описывают смежную возможность",
            evidence=[
                LLMEvidence(
                    source_id="chunk_a1",
                    quote="Документ описывает экспорт данных в формате CSV.",
                )
            ],
        )
    )

    assert outcome.cell.status is Status.UNKNOWN
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.relevant_evidence_coverage == pytest.approx(1 / 3)


def test_relevant_evidence_coverage_zero_is_measured_zero() -> None:
    """Фрагменты переданы, ни один не процитирован ⇒ измеренный ноль, а не ``None``."""
    outcome = _evaluate(
        LLMCellOutput(
            status=Status.UNKNOWN,
            justification="нет дословного утверждения",
            denial_reason="материал смежный",
        )
    )

    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.relevant_evidence_coverage == 0.0


def test_relevant_evidence_coverage_none_without_candidates() -> None:
    """Оценщику не передали фрагментов ⇒ знаменателя нет, замер ``None`` (не ноль)."""
    data = _data()
    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(
            LLMCellOutput(
                status=Status.UNKNOWN,
                justification="нечего смотреть",
                denial_reason="корпус пуст",
            )
        ),
        chunks=[],
    )

    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.relevant_evidence_coverage is None


def test_failed_cell_has_no_relevance_measurement() -> None:
    """Сбой прибора — замер не производился: ``relevant_evidence_coverage`` = ``None``."""
    data = _data()

    class _Broken:
        model_id = "fake-model"

        def complete(self, req: LLMRequest) -> LLMResponse:  # noqa: ARG002
            raise RuntimeError("adapter down")

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_Broken(),  # type: ignore[arg-type]
        chunks=_chunks(data),
        max_retries=0,
    )

    assert outcome.cell.state is CellState.FAILED
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.INSTRUMENT_FAILED
    assert scope.relevant_evidence_coverage is None


# ---------------------------------------------------------------------------
# validation_notes: причина отказа переживает прогон
# ---------------------------------------------------------------------------

def test_failed_cell_records_validation_notes() -> None:
    """Отказ валидации/парсинга оставляет причину в ячейке, а не только в консоли."""
    data = _data()

    class _Broken:
        model_id = "fake-model"

        def complete(self, req: LLMRequest) -> LLMResponse:  # noqa: ARG002
            raise RuntimeError("adapter down " + "x" * 2000)

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_Broken(),  # type: ignore[arg-type]
        chunks=_chunks(data),
        max_retries=0,
    )

    notes = outcome.cell.validation_notes
    assert notes, "причина отказа обязана попасть в validation_notes"
    assert any("RuntimeError" in note for note in notes)
    # Предел длины: это свидетельство, а не журнал с килобайтами текста исключения.
    assert all(len(note) <= 400 for note in notes)


def test_ok_cell_keeps_validation_anomalies() -> None:
    """Аномалии §10.4 (✅ на единственной цитате) раньше терялись — теперь в ячейке."""
    outcome = _evaluate(_ok_with_denial())

    assert any("единственную цитату" in note for note in outcome.cell.validation_notes)


def test_empty_citation_note_reaches_cell() -> None:
    """I22: пустая цитата при ``direct`` — заметка эшелона, теперь она в ``validation_notes``.

    Путь есть и он не гипотетический: ``core.pipeline.validate`` (строка с
    ``f"I22: пустая цитата для source_id …"``) отмечает такой evidence как провал; в
    strict ячейка становится ``failed``, и раньше причина исчезала после прогона.
    """
    data = _data()
    out = LLMCellOutput(
        status=Status.OK,
        justification="претензия без цитаты",
        evidence=[LLMEvidence(source_id="chunk_a2", quote="   ")],
    )
    outcome = _evaluate(out, data)

    assert outcome.cell.state is CellState.FAILED
    joined = " ".join(outcome.cell.validation_notes)
    assert "I22" in joined and "пустая цитата" in joined
