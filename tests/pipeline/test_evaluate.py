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
    Importance,
    LLMCellOutput,
    LLMEvidence,
    ParamType,
    SourceKind,
    SourceScope,
    Status,
    check_cell,
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
    scope_attested_by: str | None = None,
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
        scope_attested_by=scope_attested_by,
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
    from core.pipeline.retrieve import RetrievalResult

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
# Ф0: покрытие корпуса гейтит ❌ (I2c)
# ---------------------------------------------------------------------------


def _absent_output() -> LLMCellOutput:
    """ABSENT без цитат: отсутствие подтверждается протоколом над срезом."""
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Свойство не найдено в переданном срезе.",
        evidence=[],
    )


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


def test_absence_evidence_record_carries_run_composition() -> None:
    """Синтетическая absence-запись (без чанка) привязана к композиции прогона (R7)."""
    data = _data()
    evaluator = _ScriptedEvaluator([_absent_output()])
    retrievals = RetrievalResult(
        chunks=[], profile="R2", cutoff_K=5, build_id="cb_run"
    )
    outcome = _evaluate(data, evaluator, retrievals=retrievals)

    assert outcome.cell.status is Status.ABSENT
    synthetic = outcome.cell.absence_evidence[0]
    assert synthetic["source_id"] is None
    assert synthetic["index_build_id"] == "cb_run"


def test_incomplete_corpus_absence_becomes_pending_candidate() -> None:
    """Ф0: заблокированный источник + ABSENT → pending, а не ❌ (I2c)."""
    data = _data()
    evaluator = _ScriptedEvaluator([_absent_output()])
    store = _RecordingStore()
    coverage = _coverage(complete=False, blocked={"robots_disallow": 4})

    outcome = _evaluate(data, evaluator, store=store, coverage=coverage)

    assert outcome.cell.state is CellState.PENDING
    assert outcome.cell.status is Status.ABSENT  # предложенный, не опубликованный
    assert outcome.cell.review_reason == "corpus_incomplete_absence"
    assert outcome.cell.reviewed_by is None  # I6/I7: решает человек
    assert store.cells == [outcome.cell]
    audit = [
        entry
        for entry in outcome.cell.absence_evidence
        if entry.get("corpus_complete") is False
    ]
    assert audit and audit[0]["corpus_blocked"] == {"robots_disallow": 4}
    assert audit[0]["corpus_blocking_failures"] == {"robots_disallow": 4}
    assert "robots_disallow" in audit[0]["note"]
    assert audit[0]["corpus_observed_sources"] == 2


def test_incomplete_corpus_with_human_attestation_keeps_absent_and_records_counts() -> None:
    """Ф0: человек принял риск (scope_attested_by) — ❌ остаётся, счётчики в evidence."""
    data = _data()
    evaluator = _ScriptedEvaluator([_absent_output()])
    coverage = _coverage(complete=False, blocked={"robots_disallow": 4})

    outcome = _evaluate(
        data,
        evaluator,
        coverage=coverage,
        scope_attested_by="human-reviewer",
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.review_reason is None
    audit = [
        entry
        for entry in outcome.cell.absence_evidence
        if entry.get("corpus_complete") is False
    ]
    assert audit and audit[0]["corpus_blocked"] == {"robots_disallow": 4}
    assert audit[0]["scope_attested_by"] == "human-reviewer"

def _unreadable_origin_coverage() -> CorpusCoverage:
    """Покрытие с полностью закрытым origin'ом (origin-уровневый порог Ф0)."""
    return CorpusCoverage(
        product_id="p1",
        complete=False,
        expected_sources=2,
        observed_sources=1,
        blocked={"robots_disallow": 1},
        unreadable_origins=["closed.example"],
        partial_blocks={"closed.example": {"robots_disallow": 1}},
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )


def test_incomplete_corpus_pending_names_unreadable_origin() -> None:
    """Ф0: pending-кандидат называет закрытый origin, а не только счётчик блокировок."""
    data = _data()
    store = _RecordingStore()

    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        store=store,
        coverage=_unreadable_origin_coverage(),
    )

    assert outcome.cell.state is CellState.PENDING
    assert outcome.cell.review_reason == "corpus_incomplete_absence"
    audit = [
        entry
        for entry in outcome.cell.absence_evidence
        if entry.get("corpus_complete") is False
    ]
    assert audit
    assert audit[0]["corpus_unreadable_origins"] == ["closed.example"]
    assert "closed.example" in audit[0]["note"]
    assert audit[0]["corpus_partial_blocks"] == {
        "closed.example": {"robots_disallow": 1}
    }


def test_unreadable_origin_with_human_attestation_keeps_absent() -> None:
    """Ф0/I2c: человек аттестовал — ``❌`` остаётся, origin виден в evidence."""
    data = _data()

    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        coverage=_unreadable_origin_coverage(),
        scope_attested_by="human-reviewer",
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.review_reason is None
    audit = [
        entry
        for entry in outcome.cell.absence_evidence
        if entry.get("corpus_complete") is False
    ]
    assert audit
    assert audit[0]["corpus_unreadable_origins"] == ["closed.example"]
    assert audit[0]["scope_attested_by"] == "human-reviewer"


def test_complete_corpus_absence_is_byte_identical_to_previous_behaviour() -> None:
    """Ф0: полное покрытие не меняет ячейку — сравнение с исходом без coverage."""
    data = _data()
    baseline = _evaluate(data, _ScriptedEvaluator([_absent_output()]))
    with_coverage = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        coverage=_coverage(complete=True),
    )

    assert baseline.cell.state is CellState.EVALUATED
    assert baseline.cell.status is Status.ABSENT
    assert with_coverage.cell == baseline.cell
    assert with_coverage.validation == baseline.validation
    assert with_coverage.prompt_hash == baseline.prompt_hash


def test_nonblocking_source_unavailable_does_not_gate_absent() -> None:
    """Ф0/2.2.0: ``source_unavailable`` на читаемом origin'е не блокирует публикацию ❌."""
    data = _data()
    coverage = _coverage(complete=True, blocked={"source_unavailable": 1})

    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        coverage=coverage,
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.ABSENT
    assert outcome.cell.review_reason is None


def test_gate_reads_coverage_from_store_when_not_passed() -> None:
    """Ф0: оценщик читает покрытие из store (путь фазы Ш6), не только из аргумента."""
    data = _data()
    coverage = _coverage(complete=False, blocked={"robots_disallow": 4})
    store = _CoverageStore(coverage)

    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        store=store,
    )

    assert outcome.cell.state is CellState.PENDING
    assert outcome.cell.review_reason == "corpus_incomplete_absence"
    assert store.cells == [outcome.cell]


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
# Доступность ❌: аттестация охвата, absence-evidence и её персистенция
# ---------------------------------------------------------------------------

#: Цитата «ближайшего» фрагмента, где искомого свойства нет (естественное поведение модели).
_ABSENT_QUOTE_SOURCE = "chunk_a1"
_ABSENT_QUOTE = "Документ описывает экспорт данных в формате CSV."


def _absent_with_quote() -> LLMCellOutput:
    """``❌`` **с цитатой**: модель процитировала фрагмент, в котором свойства нет.

    Это не вырожденный случай, а самый частый: модель выдаёт ближайший найденный текст.
    До правки такая ячейка теряла подпись человека целиком — синтетическая запись-аттестация
    создавалась только при ``if not absences``, поэтому I2b не выполнялся никогда.
    """
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Искал «свойство альфа»; в переданных фрагментах только экспорт CSV.",
        evidence=[LLMEvidence(source_id=_ABSENT_QUOTE_SOURCE, quote=_ABSENT_QUOTE)],
    )


def _distribution_param(data: dict) -> CatalogRow:
    """Строка каталога с ``absence_rule=distribution_absence`` (требует аттестации, I2b)."""
    return _param(data).model_copy(
        update={"absence_rule": AbsenceRuleType.DISTRIBUTION_ABSENCE}
    )


@pytest.mark.parametrize(
    "factory",
    [_absent_output, _absent_with_quote],
    ids=["without_quote", "with_quote"],
)
def test_attested_distribution_absence_is_reachable(factory) -> None:  # noqa: ANN001
    """Подпись есть → ``❌`` публикуется, и подпись стоит на записи протокола (I2b).

    Тест обязан падать на коде до правки: ``distribution_absence`` + ABSENT уходил в
    ``failed`` при любом ответе модели (и с цитатой, и без), потому что ни одна
    absence-запись не несла ``source_scope=exhaustive`` вместе с ``scope_attested_by``.
    """
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([factory()]),
        param=_distribution_param(data),
        scope_attested_by="Roger",
    )

    assert outcome.cell.state is CellState.EVALUATED
    assert outcome.cell.status is Status.ABSENT
    assert outcome.failure_kind is None
    asserted = [
        entry
        for entry in outcome.cell.absence_evidence
        if entry.get("source_scope") == SourceScope.EXHAUSTIVE.value
        and entry.get("scope_attested_by") == "Roger"
    ]
    assert asserted, outcome.cell.absence_evidence
    # Доменный валидатор I2b принимает ячейку (исключения нет).
    assert check_cell(outcome.cell) == []
    assert not [note for note in outcome.validation.notes if "I2b" in note]


@pytest.mark.parametrize("attestation", [None, ""], ids=["no_signature", "empty_signature"])
def test_unattested_distribution_absence_is_pending_candidate_not_failed(
    attestation,
) -> None:  # noqa: ANN001
    """Нет подписи → ``pending``-кандидат с видимой причиной, а **не** ``failed`` (I4).

    Процедурное условие (нет человеческой аттестации охвата) — вопрос к человеку (I6/I7),
    а не сбой прибора: ``failed`` сделал бы честное «нет» хуже незнания (I4).
    """
    data = _data()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        param=_distribution_param(data),
        scope_attested_by=attestation,
    )

    assert outcome.cell.state is CellState.PENDING
    assert outcome.cell.state is not CellState.FAILED
    assert outcome.cell.status is None
    assert outcome.cell.review_reason == ABSENCE_UNATTESTED_REVIEW_REASON
    assert outcome.cell.reviewed_by is None  # I6/I7: решает человек
    assert outcome.failure_kind is None
    # Протокол отсутствия сохранён в ячейке: ревью видит, что искали.
    assert outcome.cell.absence_evidence
    assert any("I2b" in note for note in outcome.validation.notes)


def test_i2b_is_not_weakened_for_distribution_absence() -> None:
    """I2b остаётся прежним: без ``exhaustive`` на одной записи ``❌`` недопустим."""
    with pytest.raises(ValueError, match="I2b"):
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
                    "scope_attested_by": "Roger",
                }
            ],
        )


def test_persist_writes_absence_evidence_with_attestation() -> None:
    """``_persist`` пишет протокол отсутствия (R4), а не только цитаты модели.

    До правки ``_persist`` писал исключительно ``validation.resolved_evidence``, поэтому
    absence-запись не попадала в store никогда — ``❌`` существовал лишь в памяти процесса.
    """
    data = _data()
    store = _RecordingStore()
    outcome = _evaluate(
        data,
        _ScriptedEvaluator([_absent_output()]),
        store=store,
        scope_attested_by="Roger",
    )

    assert outcome.cell.status is Status.ABSENT
    assert store.absence, "absence-evidence не записан в store"
    record = store.absence[0]
    assert record["evidence_type"] == "absence"
    assert record["source_scope"] == SourceScope.EXHAUSTIVE.value
    assert record["scope_attested_by"] == "Roger"
    # PK не NULL и не пуст: синтетическая запись получает детерминированный id.
    assert record["evidence_id"]
    assert record["run_id"] == data["run_id"]
    assert record["product_id"] == data["product_id"]
    assert record["param_id"] == data["param"]["param_id"]


def test_absence_record_id_is_deterministic_per_cell() -> None:
    """Повторная оценка ячейки перезаписывает ту же строку, а не плодит новую (S8)."""
    data = _data()
    first = _evaluate(data, _ScriptedEvaluator([_absent_output()]))
    second = _evaluate(data, _ScriptedEvaluator([_absent_with_quote()]))

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
