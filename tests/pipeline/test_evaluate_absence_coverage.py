"""I2c в конвейере: ``check_absence_coverage`` исполняется, а не подразумевается.

Инвариант объявлен в домене (:func:`core.domain.invariants.check_absence_coverage`) и
продублирован в отчёте, но до этой правки в конвейере не вызывался ни разу: ``❌`` на
неполном корпусе не публиковался только потому, что :func:`_resolve_denial` выбирает
класс по наблюдённому охвату. Совпадение по построению — не защита. Здесь фиксируется
обратное: ячейка, чей класс отрицания не согласован с охватом, **понижается** до ``❓``,
причина видна машинно (``evidence_scope.reason`` + ``cells.validation_notes``), а протокол
отсутствия не публикуется.

Классы отрицания и их отношение к неполному охвату — предмет решения «Вариант 2»
(``EvidenceScope``): ``explicit`` опирается на цитату вендора и неполнотой не отменяется,
``bounded`` требует принятия риска человеком (``scope_acceptance``), а ``unobserved``/
``not_stated`` ``❌`` не дают вовсе. Правила не пересматриваются — проверяется их
**исполнение**.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    CellState,
    CorpusCoverage,
    DenialStrength,
    EvidenceScope,
    LLMCellOutput,
    Status,
    check_cell,
)
from core.pipeline.evaluate import (
    ABSENCE_COVERAGE_UNSATISFIED_REASON,
    _enforce_absence_coverage,
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
            output=self.out,
            raw=self.out.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
        )

class _RecordingStore:
    """Приёмник записей (без SQLite): проверяется, что протокол отсутствия не пишется."""

    def __init__(self) -> None:
        self.cells: list = []
        self.evidence: list = []
        self.absence: list[dict] = []

    def save_cell(self, cell) -> None:  # noqa: ANN001
        self.cells.append(cell)

    def add_evidence_batch(self, evs) -> None:  # noqa: ANN001
        self.evidence.extend(evs)

    def add_absence_evidence(self, entries) -> None:  # noqa: ANN001
        self.absence.extend(entries)

def _absent_explicit() -> LLMCellOutput:
    """``❌`` с цитатой прямого отрицания — законный путь к ``❌``."""
    return LLMCellOutput(**(_data()["absent_explicit_output"]))

def _absent_uncited() -> LLMCellOutput:
    """``❌`` без цитаты: модель сказала «нет», ничем это не подтвердив."""
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Свойство не найдено в переданном срезе.",
        evidence=[],
    )

def _incomplete_coverage() -> CorpusCoverage:
    return CorpusCoverage(
        product_id="p1",
        complete=False,
        expected_sources=6,
        observed_sources=2,
        blocked={"robots_disallow": 2, "partial_extraction": 2},
        unreadable_origins=["closed.example"],
        partial_blocks={"closed.example": {"robots_disallow": 2}},
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

def _conflicting_bounded() -> EvidenceScope:
    """``bounded`` **без** принятия риска — комбинация, которую домен собирать отказывается.

    ``model_construct`` нужен, чтобы сымитировать дрейф (новый писатель ячейки, минующий
    ``_resolve_denial``); валидаторы ``EvidenceScope`` такую строку не соберут — это и есть
    второй рубеж, который проверяется здесь.
    """
    return EvidenceScope.model_construct(
        denial_strength=DenialStrength.BOUNDED,
        reason="конечный справочник объявлен каталогом",
        enumerated=True,
        scope_acceptance=None,
    )

# ---------------------------------------------------------------------------
# Инвариант исполняется: вызов в конвейере, а не только импорт
# ---------------------------------------------------------------------------

def test_evaluate_cell_calls_the_domain_invariant(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ш6 спрашивает I2c у домена: вызов идёт с предполагаемой строкой и охватом."""
    import core.pipeline.evaluate as evaluate_module

    seen: list[tuple[Status, CorpusCoverage | None]] = []
    original = evaluate_module.check_absence_coverage

    def spy(cell, coverage):  # noqa: ANN001 - зеркало доменной подписи
        seen.append((cell.status, coverage))
        return original(cell, coverage)

    monkeypatch.setattr(evaluate_module, "check_absence_coverage", spy)
    data = _data()
    coverage = _incomplete_coverage()
    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(_absent_explicit()),
        chunks=_chunks(data),
        coverage=coverage,
    )

    assert seen == [(Status.ABSENT, coverage)]

# ---------------------------------------------------------------------------
# Нарушение: понижение до `❓`, причина машинно видна, протокол отсутствия снят
# ---------------------------------------------------------------------------

def test_conflicting_class_is_demoted_with_machine_readable_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``bounded`` без принятия риска на неполном охвате ⇒ ``❓``, а не опубликованный ``❌``."""
    import core.pipeline.evaluate as evaluate_module

    monkeypatch.setattr(
        evaluate_module,
        "_resolve_denial",
        lambda **_: (_conflicting_bounded(), []),
    )
    data = _data()
    store = _RecordingStore()
    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(_absent_explicit()),
        chunks=_chunks(data),
        coverage=_incomplete_coverage(),
        store=store,
    )

    cell = outcome.cell
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.UNKNOWN          # I12: ❓, а не «❌ без доказательства»
    assert outcome.failure_kind is None           # I4: это не отказ прибора
    scope = cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.NOT_STATED
    # Причина называет **отсутствующий компонент** и виновника (продукт/параметр).
    assert ABSENCE_COVERAGE_UNSATISFIED_REASON in (scope.reason or "")
    assert "scope_acceptance" in (scope.reason or "")
    assert "I2c" in (scope.reason or "")
    # Машинная запись переживает прогон.
    assert any(
        ABSENCE_COVERAGE_UNSATISFIED_REASON in note and "scope_acceptance" in note
        for note in cell.validation_notes
    )
    # Протокол отсутствия не публикуется: ``absence_evidence`` утверждает «искали и не нашли».
    assert cell.absence_evidence == []
    assert store.absence == []
    assert check_cell(cell) == []

def test_legacy_absent_row_without_scope_is_demoted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Строка без объекта силы отрицания (legacy) не публикует ``❌`` на неполном охвате."""
    import core.pipeline.evaluate as evaluate_module

    monkeypatch.setattr(evaluate_module, "_resolve_denial", lambda **_: (None, []))
    data = _data()
    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(_absent_uncited()),
        chunks=_chunks(data),
        coverage=_incomplete_coverage(),
    )

    assert outcome.cell.status is Status.UNKNOWN
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.NOT_STATED
    assert ABSENCE_COVERAGE_UNSATISFIED_REASON in (scope.reason or "")
    assert outcome.cell.absence_evidence == []
    assert check_cell(outcome.cell) == []

# ---------------------------------------------------------------------------
# Неприменимость: нет записи покрытия — нет и проверки
# ---------------------------------------------------------------------------

def test_check_is_inapplicable_without_coverage_record() -> None:
    """``coverage=None``: записи покрытия нет — I2c неприменим, вердикт не меняется."""
    status, scope, demoted, notes = _enforce_absence_coverage(
        out_status=Status.ABSENT,
        scope=_conflicting_bounded(),
        absence_evidence=[],
        param=_param(_data()),
        product_id="p1",
        coverage=None,
    )

    assert (status, demoted, notes) == (Status.ABSENT, False, [])
    assert scope is not None and scope.denial_strength is DenialStrength.BOUNDED

def test_explicit_denial_publishes_without_coverage_record() -> None:
    """Живой путь без записи покрытия: ``explicit`` публикует ``❌`` и протокол отсутствия."""
    data = _data()
    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(_absent_explicit()),
        chunks=_chunks(data),
    )

    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.denial_strength is DenialStrength.EXPLICIT
    assert any(
        entry.get("evidence_type") == "absence" for entry in outcome.cell.absence_evidence
    )
    assert check_cell(outcome.cell) == []

# ---------------------------------------------------------------------------
# Согласованные комбинации проходят: инвариант не «запрещает всё»
# ---------------------------------------------------------------------------

def test_bounded_with_acceptance_survives_incomplete_coverage() -> None:
    """Конечный справочник + принятие риска ⇒ ``bounded`` ⇒ ``❌`` даже на неполном охвате."""
    data = _data()
    param = _param(data).model_copy(
        update={"absence_rule": AbsenceRuleType.DISTRIBUTION_ABSENCE}
    )
    outcome = evaluate_cell(
        param=param,
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(_absent_uncited()),
        chunks=_chunks(data),
        coverage=_incomplete_coverage(),
        scope_acceptance="analyst",
    )

    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.BOUNDED
    assert scope.scope_acceptance == "analyst"
    assert outcome.cell.status is Status.ABSENT
    assert check_cell(outcome.cell) == []

def test_explicit_vendor_denial_survives_incomplete_coverage() -> None:
    """Прямое отрицание вендора — свидетельство: неполнота охвата его не отменяет."""
    data = _data()
    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(_absent_explicit()),
        chunks=_chunks(data),
        coverage=_incomplete_coverage(),
    )

    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.denial_strength is DenialStrength.EXPLICIT
    assert outcome.cell.absence_evidence != []
    assert not any(
        ABSENCE_COVERAGE_UNSATISFIED_REASON in note
        for note in outcome.cell.validation_notes
    )
