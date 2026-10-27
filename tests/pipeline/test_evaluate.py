"""Тесты фазы оценки Ш6 (T9.3, §5/§7.1): strict/preliminary, retry ≤2, I4/I13,
композер ретривала I16/I17/I20 и персистенция.

Оценщик — скриптованный in-memory ``EvaluatorPort`` (без сети, I18); чанки — нейтральные
``Chunk`` из фикстуры. Проверяется, что ``failed`` никогда не несёт статус (I4), что
preliminary-провал — кандидат без статуса (I13), и что ``retrieve_for_cell`` собирает
запросы и дедуплицирует продукт-скоупед срез.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    CorpusCoverage,
    DenialStrength,
    Importance,
    LLMCellOutput,
    LLMEvidence,
    ParamType,
    SourceKind,
    SourceScope,
    Status,
    check_cell,
    check_unknown_exhaustion,
)
from core.pipeline.evaluate import (
    ABSENCE_UNATTESTED_REVIEW_REASON,
    EvaluationOutcome,
    evaluate_batch,
    evaluate_cell,
    retrieve_for_cell,
    run_queries_for_cell,
)
from core.pipeline.prompt import compile_prompt, render_queries
from core.pipeline.retrieve import RetrievalResult
from core.pipeline.validate import ValidationResult
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Фикстуры (нейтральные данные, §1.3)
# ---------------------------------------------------------------------------


def _data() -> dict:
    return json.loads((FIXTURES / "evaluate_inputs.json").read_text(encoding="utf-8"))


def _param(data: dict) -> CatalogRow:
    return CatalogRow(**data["param"])


def _chunks(data: dict) -> list[Chunk]:
    return [Chunk(**c) for c in data["chunks"]]


def _ok_output(data: dict) -> LLMCellOutput:
    return LLMCellOutput(**data["ok_output"])


def _bad_output(data: dict) -> LLMCellOutput:
    return LLMCellOutput(**data["bad_quote_output"])


class _ScriptedEvaluator:
    """Скриптованный ``EvaluatorPort``: очередь выходов/исключений (I18: без веб-тула).

    Последний элемент скрипта повторяется при исчерпании, что позволяет проверить
    ограниченный retry. ``model_id``/``seed``/``temperature`` — как у self-host оценщика.
    """

    def __init__(self, script: list[LLMCellOutput | Exception]) -> None:
        self.script = list(script)
        self.model_id = "fake-model"
        self.model_version = "1.0"
        self.seed = 0
        self.temperature = 0.0
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        idx = min(len(self.calls) - 1, len(self.script) - 1)
        item = self.script[idx]
        if isinstance(item, Exception):
            raise item
        return LLMResponse(
            output=item,
            raw=item.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
        )


class _RecordingStore:
    """Минимальный ``StorePort``-совместимый приёмник записей (без SQLite)."""

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


class _CoverageStore(_RecordingStore):
    """Приёмник записей + чтение покрытия (проверяет store-путь гейта Ф0)."""

    def __init__(self, coverage) -> None:  # noqa: ANN001
        super().__init__()
        self.coverage = coverage

    def read_corpus_coverage(self, run_id: str, product_id: str):  # noqa: ANN201
        return self.coverage if product_id == self.coverage.product_id else None


def _evaluate(  # noqa: ANN001 - тестовый helper
    data: dict,
    evaluator,
    *,
    strict: bool = True,
    max_retries: int = 2,
    store=None,
    retrievals=None,
    min_retrieval_score: float | None = None,
    coverage=None,
    scope_acceptance: str | None = None,
    param=None,
):
    return evaluate_cell(
        param=_param(data) if param is None else param,
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        strict=strict,
        max_retries=max_retries,
        store=store,
        retrievals=retrievals,
        min_retrieval_score=min_retrieval_score,
        coverage=coverage,
        scope_acceptance=scope_acceptance,
    )


# ---------------------------------------------------------------------------
# strict: happy path
# ---------------------------------------------------------------------------


def test_strict_ok_happy_path() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    outcome = _evaluate(data, evaluator)

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.OK
    assert len(outcome.cell.evidence) == 1
    assert outcome.failure_kind is None
    assert outcome.validation.ok is True
    assert len(outcome.validation.resolved_evidence) == 1

    # prompt_hash — канонический (шаблон + rule + absence_rule + sorted chunk_ids).
    expected = compile_prompt(
        param=_param(data),
        product_id=data["product_id"],
        chunks=_chunks(data),
    )
    assert outcome.prompt_hash == expected.prompt_hash
    assert evaluator.calls[0].prompt == expected.text
    assert evaluator.calls[0].response_schema is LLMCellOutput


# ---------------------------------------------------------------------------
# strict: провал → failed (I4), retry ограничен
# ---------------------------------------------------------------------------


def test_invalid_quote_fails_with_status_none_and_bounded_retry() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_bad_output(data)])
    outcome = _evaluate(data, evaluator, max_retries=2)

    assert outcome.cell.state is CellState.FAILED
    assert outcome.cell.status is None  # I4
    assert outcome.failure_kind == "validation_failed"
    assert len(evaluator.calls) == 3  # первая попытка + 2 повтора (ограничены)
    assert any("I22" in n for n in outcome.validation.notes)


def test_retry_recovers_from_transient_invalid_output() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_bad_output(data), _ok_output(data)])
    outcome = _evaluate(data, evaluator, max_retries=2)

    assert outcome.cell.state is CellState.EVALUATED
    assert len(evaluator.calls) == 2  # второй (детерминированный) ответ прошёл


def test_adapter_exception_maps_to_evaluation_failed() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([RuntimeError("адаптер недоступен")])
    outcome = _evaluate(data, evaluator, max_retries=2)

    assert outcome.cell.state is CellState.FAILED
    assert outcome.cell.status is None  # I4
    assert outcome.failure_kind == "evaluation_failed"
    assert len(evaluator.calls) == 3


def test_max_retries_zero_single_attempt() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_bad_output(data)])
    outcome = _evaluate(data, evaluator, max_retries=0)

    assert outcome.cell.state is CellState.FAILED
    assert len(evaluator.calls) == 1


# ---------------------------------------------------------------------------
# preliminary: провал → кандидат (I13)
# ---------------------------------------------------------------------------


def test_preliminary_invalid_becomes_preliminary_candidate() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_bad_output(data)])
    outcome = _evaluate(data, evaluator, strict=False, max_retries=2)

    assert outcome.cell.state is CellState.PRELIMINARY
    assert outcome.cell.status is None  # I13
    assert outcome.failure_kind is None
    assert any("candidate" in n for n in outcome.validation.notes)


# ---------------------------------------------------------------------------
# Провенанс ретривала и персистенция
# ---------------------------------------------------------------------------


def test_retrieval_provenance_lands_on_cell() -> None:

    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    retrievals = RetrievalResult(
        chunks=[], profile="R2", cutoff_K=5, build_id="build-42"
    )
    outcome = _evaluate(data, evaluator, retrievals=retrievals)

    assert outcome.cell.index_build_id == "build-42"
    assert outcome.cell.retrieval_profile == "R2"
    assert "cutoff_K" in (outcome.cell.retrieval_params_json or "")


def test_store_persists_cell_and_evidence() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    store = _RecordingStore()
    outcome = _evaluate(data, evaluator, store=store)

    assert store.cells == [outcome.cell]
    assert len(store.evidence) == len(outcome.validation.resolved_evidence) == 1
    assert outcome.cell.source_kind is SourceKind.DOCUMENTATION


# ---------------------------------------------------------------------------
# Вариант 2: сила отрицания вместо булева «исчерпан ли охват»
# ---------------------------------------------------------------------------


def _absent_output() -> LLMCellOutput:
    """ABSENT **без цитаты**: модель сказала «нет», ничем это не подтвердив.

    Раньше такой ответ публиковался как ``❌``. Именно он дал ложные «нет» в живом
    контроле («вендор не сказал дословно» ≠ «вендор не заявил»), поэтому теперь это
    ``not_stated`` → ``❓``.
    """
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Свойство не найдено в переданном срезе.",
        evidence=[],
    )


def _absent_explicit() -> LLMCellOutput:
    """ABSENT **с цитатой прямого отрицания**: единственный законный путь к ``❌``."""
    return LLMCellOutput(**(_data()["absent_explicit_output"]))


def _absent_not_stated_with_relevant_chunk() -> LLMCellOutput:
    """Живой регресс: материал релевантен, но дословного утверждения нет."""
    return LLMCellOutput(**(_data()["absent_not_stated_output"]))


def _coverage(*, complete: bool, blocked: dict[str, int] | None = None) -> CorpusCoverage:
    blocked = dict(blocked or {})
    expected = 6
    observed = expected if complete else expected - sum(blocked.values())
    return CorpusCoverage(
        product_id="p1",
        complete=complete,
        expected_sources=expected,
        observed_sources=observed,
        blocked=blocked,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )


def test_explicit_denial_publishes_absent_with_class_and_citation() -> None:
    """``❌`` публикуется, когда есть прямое отрицание с цитатой — и видна его сила."""
    data = _data()
    outcome = _evaluate(data, _ScriptedEvaluator([_absent_explicit()]))

    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.state is CellState.EVALUATED
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.EXPLICIT
    assert scope.citation_quote and "не поддерживается" in scope.citation_quote
    assert check_cell(outcome.cell) == []


def test_explicit_claim_without_citation_is_refused_and_becomes_unknown() -> None:
    """``explicit`` без цитаты — отказ: вердикт понижается до ``❓``, не до ``❌``."""
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([LLMCellOutput(**data["absent_claim_without_citation_output"])]),
    )

    assert outcome.cell.status is Status.UNKNOWN
    assert outcome.cell.status is not Status.ABSENT
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.NOT_STATED
    assert "explicit_without_citation" in (scope.reason or "")
    assert any("explicit" in note for note in outcome.validation.notes)
    assert outcome.cell.absence_evidence == []


def test_not_stated_with_relevant_chunk_is_unknown_not_absent() -> None:
    """Регресс-тест живого ложного «нет»: релевантный фрагмент без дословности — ``❓``.

    Живой контроль: 2 из 12 ячеек получали «отсутствует» там, где вендор заявил свойство
    (не дословно). Здесь ровно этот вход: цитата подтверждённого фрагмента есть, но она
    **не** отрицание. ``❌`` запрещён, статус — ``❓`` с причиной.
    """
    data = _data()
    outcome = _evaluate(data, _ScriptedEvaluator([_absent_not_stated_with_relevant_chunk()]))

    assert outcome.cell.status is Status.UNKNOWN
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.NOT_STATED
    assert scope.reason
    assert check_cell(outcome.cell) == []
    assert check_unknown_exhaustion(outcome.cell, None) == []


def test_unknown_output_carries_reason_and_coverage_vector() -> None:
    """``❓`` несёт причину и вектор охвата; доля названа мерой URL."""
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([LLMCellOutput(**data["unknown_output"])]),
        coverage=_coverage(complete=False, blocked={"robots_disallow": 2}),
    )

    assert outcome.cell.status is Status.UNKNOWN
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.UNOBSERVED  # охват неполон ⇒ код решает
    assert "corpus_incomplete_absence" in (scope.reason or "")
    assert scope.observed == 4 and scope.expected == 6
    assert scope.blocked_by_reason == {"robots_disallow": 2}
    assert scope.url_observation_ratio == pytest.approx(4 / 6)


def test_complete_corpus_unknown_is_not_stated() -> None:
    """Полный охват, но вендор не заявил — ``not_stated``, а не ``unobserved``."""
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([LLMCellOutput(**data["unknown_output"])]),
        coverage=_coverage(complete=True),
    )
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.denial_strength is DenialStrength.NOT_STATED


def test_bounded_requires_scope_acceptance_and_publishes_absent() -> None:
    """Конечный справочник + принятие риска ⇒ ``bounded`` ⇒ ``❌``."""
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        param=_distribution_param(data),
        scope_acceptance="Roger",
    )

    assert outcome.cell.status is Status.ABSENT
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.BOUNDED
    assert scope.scope_acceptance == "Roger"
    assert scope.enumerated is True
    assert check_cell(outcome.cell) == []
    protocol = [
        entry
        for entry in outcome.cell.absence_evidence
        if entry.get("evidence_type") == "absence"
    ]
    assert protocol and protocol[0]["scope_acceptance"] == "Roger"
    assert protocol[0]["denial_strength"] == "bounded"


@pytest.mark.parametrize("acceptance", [None, ""], ids=["no_acceptance", "empty"])
def test_bounded_without_acceptance_becomes_unknown(acceptance) -> None:  # noqa: ANN001
    """Без принятия риска ``bounded`` недостижим: ``❓`` с причиной, а не ``❌`` и не ``failed``."""
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        param=_distribution_param(data),
        scope_acceptance=acceptance,
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.UNKNOWN
    assert outcome.failure_kind is None
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.NOT_STATED
    assert ABSENCE_UNATTESTED_REVIEW_REASON in (scope.reason or "")


def test_incomplete_corpus_does_not_cancel_explicit_denial() -> None:
    """Неполный охват **не** отменяет прямое отрицание вендора: это свидетельство."""
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_explicit()]),
        store=_RecordingStore(),
        coverage=_coverage(complete=False, blocked={"robots_disallow": 4}),
    )

    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.denial_strength is DenialStrength.EXPLICIT
    # Аудит блокировок корпуса остаётся видимым (отчёт/аудит), но вердикт от него не зависит.
    audit = [e for e in outcome.cell.absence_evidence if e.get("corpus_complete") is False]
    assert audit and audit[0]["corpus_blocked"] == {"robots_disallow": 4}


def test_absent_without_denial_is_unobserved_when_corpus_incomplete() -> None:
    """Неполный охват + недоказанное «нет» ⇒ ``unobserved`` ⇒ ``❓`` (не ``❌``, не ``pending``)."""
    data = _data()
    store = _RecordingStore()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        store=store,
        coverage=_coverage(complete=False, blocked={"robots_disallow": 4}),
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.UNKNOWN
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.UNOBSERVED
    assert scope.blocked_by_reason == {"robots_disallow": 4}
    assert store.cells == [outcome.cell]
    assert store.absence == []  # протокол отсутствия пишется только для ❌


def test_gate_reads_coverage_from_store_when_not_passed() -> None:
    """Путь фазы Ш6: покрытие читается из store и определяет класс отрицания."""
    data = _data()
    coverage = _coverage(complete=False, blocked={"robots_disallow": 4})
    store = _CoverageStore(coverage)

    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        store=store,
    )

    assert outcome.cell.status is Status.UNKNOWN
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.denial_strength is DenialStrength.UNOBSERVED
    assert store.cells == [outcome.cell]


def test_instrument_failure_is_failed_with_instrument_failed_class() -> None:
    """Сбой прибора ⇒ ``failed`` + ``instrument_failed``; статуса нет (I4)."""
    data = _data()
    outcome = _evaluate(data, _ScriptedEvaluator([RuntimeError("адаптер недоступен")]))

    assert outcome.cell.state is CellState.FAILED
    assert outcome.cell.status is None
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.INSTRUMENT_FAILED
    assert "evaluation_failed" in (scope.reason or "")
    assert check_cell(outcome.cell) == []


def test_absence_record_id_is_deterministic_per_cell() -> None:
    """Повторная оценка ячейки перезаписывает ту же строку, а не плодит новую (S8)."""
    data = _data()
    first = _evaluate(data, _ScriptedEvaluator([_absent_explicit()]))
    second = _evaluate(data, _ScriptedEvaluator([_absent_explicit()]))

    first_ids = [
        entry.get("evidence_id")
        for entry in first.cell.absence_evidence
        if entry.get("evidence_type") == "absence"
    ]
    second_ids = [
        entry.get("evidence_id")
        for entry in second.cell.absence_evidence
        if entry.get("evidence_type") == "absence"
    ]
    assert len(first_ids) == 1, "протокол отсутствия обязан быть ровно один"
    assert first_ids == second_ids


# ---------------------------------------------------------------------------
# Запросы Ш6 (S13/I16)
# ---------------------------------------------------------------------------


def test_run_queries_for_cell_delegates_and_bounds() -> None:
    param = _param(_data())
    assert run_queries_for_cell(param) == render_queries(param)
    assert len(run_queries_for_cell(param)) <= 3


# ---------------------------------------------------------------------------
# retrieve_for_cell: композер I16/I17/I20
# ---------------------------------------------------------------------------


class _FakeLexical:
    """BM25-фейк: скриптованные хиты по тексту запроса; фильтрует партиции (I17)."""

    engine_version = "lex-fake-1"
    build_id = "build-lex"

    def __init__(self, mapping: dict[str, list[RetrievedChunk]]) -> None:
        self.mapping = mapping
        self.calls: list[tuple] = []

    def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001
        self.calls.append((query, list(product_ids), top_k, lang))
        allowed = set(product_ids)
        return [c for c in self.mapping.get(query, []) if c.product_id in allowed][:top_k]


def _rc(chunk_id: str, product: str, score: float) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        product_id=product,
        text=f"текст {chunk_id}",
        content_hash=f"h-{chunk_id}",
        heading_path="H",
        build_id="build-lex",
        score=score,
    )


def test_retrieve_for_cell_composes_queries_dedups_and_truncates() -> None:
    param = _param(_data())
    queries = render_queries(param)
    assert queries == ["Параметр Альфа", "свойство альфа", "alpha feature"]

    mapping = {
        queries[0]: [_rc("c1", "p1", 0.9), _rc("c2", "p1", 0.5)],
        queries[1]: [_rc("c2", "p1", 0.8), _rc("c3", "p1", 0.4)],
        queries[2]: [_rc("c1", "p1", 0.7)],
    }
    lexical = _FakeLexical(mapping)
    result = retrieve_for_cell(
        param,
        product_id="p1",
        profile="R0",
        cutoff_k=3,
        lexical_index=lexical,
    )

    # Дедуп по chunk_id: c1 из q1/q3 (лучший 0.9), c2 из q1/q2 (лучший 0.8), c3 0.4.
    assert [c.chunk_id for c in result.chunks] == ["c1", "c2", "c3"]
    assert result.profile == "R0"
    assert result.build_id == "build-lex"  # сборка, не engine_version (§5/R15)
    assert result.cutoff_K == 3
    # Каждый запрос пошёл с продукт-скоупом и тем же top_k (I16/I17).
    assert [c[0] for c in lexical.calls] == queries
    assert all(c[1] == ["p1"] and c[2] == 3 for c in lexical.calls)


def test_retrieve_for_cell_applies_cutoff_k() -> None:
    param = _param(_data())
    queries = render_queries(param)
    mapping = {
        queries[0]: [_rc("c1", "p1", 0.9), _rc("c2", "p1", 0.8), _rc("c3", "p1", 0.7)],
    }
    result = retrieve_for_cell(
        param,
        product_id="p1",
        profile="R0",
        cutoff_k=2,
        lexical_index=_FakeLexical(mapping),
    )
    assert [c.chunk_id for c in result.chunks] == ["c1", "c2"]  # I20


def test_retrieve_for_cell_scopes_to_satellite_partitions() -> None:
    param = _param(_data())
    queries = render_queries(param)
    mapping = {queries[0]: [_rc("c1", "p1", 0.9), _rc("s1", "satellite", 0.8)]}
    lexical = _FakeLexical(mapping)
    result = retrieve_for_cell(
        param,
        product_id="p1",
        product_ids=["p1", "satellite"],
        profile="R0",
        cutoff_k=5,
        lexical_index=lexical,
    )
    assert {c.chunk_id for c in result.chunks} == {"c1", "s1"}
    assert lexical.calls[0][1] == ["p1", "satellite"]  # I17


def test_retrieve_for_cell_requires_vector_for_dense_profiles() -> None:
    param = _param(_data())
    with pytest.raises(ValueError):
        retrieve_for_cell(
            param,
            product_id="p1",
            profile="R1",
            cutoff_k=5,
            vector_index=None,
        )


# ---------------------------------------------------------------------------
# Батч с изоляцией сбоев
# ---------------------------------------------------------------------------


def test_evaluate_batch_returns_outcomes() -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    params = [_param(data)]
    outcomes, errors = evaluate_batch(
        params=params,
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks_by_param={params[0].param_id: _chunks(data)},
    )
    assert errors == []
    assert len(outcomes) == 1
    assert outcomes[0].cell.state is CellState.EVALUATED


def test_evaluate_batch_isolates_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    import core.pipeline.evaluate as module

    data = _data()
    good = _param(data)
    bad = CatalogRow(
        param_id="param_beta",
        title="Параметр Бета",
        module="mod_a",
        group="grp_1",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule="Правило Бета.",
        absence_rule=AbsenceRuleType.EXPLICIT,
    )

    def _fake_evaluate_cell(*, param: CatalogRow, **kwargs) -> EvaluationOutcome:  # noqa: ANN003
        if param.param_id == "param_beta":
            raise RuntimeError("сбой одного параметра")
        # Исход-заглушка: ❓ не требует evidence (I1).
        return EvaluationOutcome(
            cell=Cell(
                run_id=data["run_id"],
                product_id=data["product_id"],
                param_id=param.param_id,
                state=CellState.EVALUATED,
                status=Status.UNKNOWN,
                evidence=[],
            ),
            prompt_hash="ph",
            validation=ValidationResult(True, None),
        )

    monkeypatch.setattr(module, "evaluate_cell", _fake_evaluate_cell)
    outcomes, errors = evaluate_batch(
        params=[bad, good],
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([_ok_output(data)]),
        chunks_by_param={},
    )
    assert len(outcomes) == 1
    assert len(errors) == 1
    assert "param_beta" in errors[0]


# ---------------------------------------------------------------------------
# Доступность ❌: класс отрицания, absence-evidence и её персистенция
# ---------------------------------------------------------------------------

#: Цитата «ближайшего» фрагмента, где искомого свойства нет (естественное поведение модели).
_ABSENT_QUOTE_SOURCE = "chunk_a1"
_ABSENT_QUOTE = "Документ описывает экспорт данных в формате CSV."


def _absent_with_quote() -> LLMCellOutput:
    """``❌`` **с цитатой неотрицающего фрагмента** — это ``not_stated``, а не ``explicit``.

    Самый частый ответ модели: «искал, нашёл смежный текст, свойства в нём нет». Цитата
    подтверждает наблюдение, но не отрицание вендора, поэтому класс отрицания — слабый.
    """
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Искал «свойство альфа»; в переданных фрагментах только экспорт CSV.",
        evidence=[LLMEvidence(source_id=_ABSENT_QUOTE_SOURCE, quote=_ABSENT_QUOTE)],
    )


def _distribution_param(data: dict) -> CatalogRow:
    """Строка каталога с ``absence_rule=distribution_absence`` (конечный справочник, I2b)."""
    return _param(data).model_copy(
        update={"absence_rule": AbsenceRuleType.DISTRIBUTION_ABSENCE}
    )


def test_explicit_denial_wins_over_rule_class() -> None:
    """Прямое отрицание с цитатой даёт ``explicit`` даже на правиле конечного справочника.

    Принятие риска остаётся в силе, но сильнейший класс — свидетельство вендора, а не вывод
    из полноты: подпись человека не должна повышать слабый класс до сильного.
    """
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_explicit()]),
        param=_distribution_param(data),
        scope_acceptance="Roger",
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.ABSENT
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.EXPLICIT
    assert scope.basis == "evaluator:explicit"
    assert check_cell(outcome.cell) == []


def test_quote_without_denial_is_not_stated_even_for_distribution_rule() -> None:
    """Цитата неотрицающего фрагмента + конечный справочник без подписи ⇒ ``❓``."""
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_with_quote()]),
        param=_distribution_param(data),
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.UNKNOWN
    scope = outcome.cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.NOT_STATED
    assert ABSENCE_UNATTESTED_REVIEW_REASON in (scope.reason or "")


def test_legacy_alias_scope_attested_by_still_accepted() -> None:
    """Прежнее имя аргумента читается как принятие риска (совместимость вызовов)."""
    data = _data()
    outcome = evaluate_cell(
        param=_distribution_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator([_absent_output()]),
        chunks=_chunks(data),
        scope_attested_by="Roger",
    )

    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.scope_acceptance == "Roger"


def test_i2b_absent_without_scope_cannot_be_constructed() -> None:
    """Доменный валидатор: ``❌`` без класса отрицания не собирается (I2)."""
    with pytest.raises(ValueError, match="I2"):
        Cell(
            run_id="run_eval_1",
            product_id="p1",
            param_id="param_alpha",
            state=CellState.EVALUATED,
            status=Status.ABSENT,
            absence_rule=AbsenceRuleType.DISTRIBUTION_ABSENCE,
            absence_evidence=[
                {
                    "evidence_id": "ev_absence",
                    "source_id": None,
                    "source_scope": SourceScope.PARTIAL.value,
                }
            ],
        )


def test_persist_writes_absence_evidence_with_acceptance() -> None:
    """``_persist`` пишет протокол отсутствия (R4) с классом и подписью, а не только цитаты.

    До правки ``_persist`` писал исключительно ``validation.resolved_evidence``, поэтому
    absence-запись не попадала в store никогда — ``❌`` существовал лишь в памяти процесса.
    """
    data = _data()
    store = _RecordingStore()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        param=_distribution_param(data),
        store=store,
        scope_acceptance="Roger",
    )

    assert outcome.cell.status is Status.ABSENT
    assert store.absence, "absence-evidence не записан в store"
    record = store.absence[0]
    assert record["evidence_type"] == "absence"
    assert record["scope_acceptance"] == "Roger"
    # Класс отрицания — в объекте ячейки; протокол несёт подпись и вид доказательства.
    assert outcome.cell.evidence_scope is not None
    assert outcome.cell.evidence_scope.denial_strength is DenialStrength.BOUNDED
    # PK не NULL и не пуст: синтетическая запись получает детерминированный id.
    assert record["evidence_id"]
    assert record["run_id"] == data["run_id"]
    assert record["product_id"] == data["product_id"]
    assert record["param_id"] == data["param"]["param_id"]
