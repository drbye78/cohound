"""Тесты прецедента источников P6 (§2.2 п.7/п.8, 02 §D.3): реестр/наследование поверх Ш6.

Проверяется таблица §2 гейта: согласие → высокая ячейка выживает; расхождение → видимый
``pending``-кандидат ревью (не тихая перезапись); inconclusive-Ш6 (``failed``/
``preliminary``/``❓``) не уничтожает подтверждённую высокую ячейку. Fork D — то же для
наследования. Детерминированно, без сети/LLM: скриптованный оценщик и ``SqliteStore``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    BaselineRow,
    CatalogRow,
    Cell,
    CellEvidence,
    CellState,
    Citation,
    Cohort,
    CohortEntry,
    CorpusCoverage,
    EvidenceType,
    Focus,
    InheritanceMode,
    LLMCellOutput,
    LLMEvidence,
    Passport,
    RunConfig,
    RunMode,
    SourceKind,
    SourceScope,
    Status,
    Upstream,
)
from core.pipeline import RunPlan
from core.pipeline.evaluate import evaluate_phase
from core.pipeline.inheritance import inheritance_wave
from core.pipeline.precedence import PRECEDENCE_REVIEW_REASON
from core.pipeline.review import review_queue
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-precedence"
FIXTURES = Path(__file__).parent / "fixtures"

CHUNK_TEXT = "neutral corpus evidence text"


# --------------------------------------------------------------------------- #
# Нейтральные строители
# --------------------------------------------------------------------------- #


def _fixture() -> dict:
    return json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))


def _catalog() -> dict[str, CatalogRow]:
    rows = [CatalogRow.model_validate(row) for row in _fixture()["catalog"]]
    return {row.param_id: row for row in rows}


def _plan(param_ids: list[str]) -> RunPlan:
    catalog = _catalog()
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
        params=[catalog[param_id] for param_id in param_ids],
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1"],
        package="pkg-1",
        focus=Focus(),
        languages=["en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


@pytest.fixture()
def store():
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        # MVP-1/O3: продукт плана без строки покрытия — аномалия; тесты прецедента
        # проверяют оценку по корпусу, поэтому покрытие объявляется полным.
        s.save_corpus_coverage(
            RUN_ID,
            CorpusCoverage(
                product_id="p1",
                complete=True,
                expected_sources=1,
                observed_sources=1,
                computed_at="2026-01-01T00:00:00+00:00",
            ),
        )
        yield s


def _chunk() -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id="chunk_1",
        product_id="p1",
        text=CHUNK_TEXT,
        content_hash="hash-1",
        heading_path="H",
        build_id="build-fake",
        score=0.9,
    )


class _FakeLexical:
    """BM25-фейк: один детерминированный чанк на любой запрос (I16/I17)."""

    engine_version = "lex-fake-1"

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def search(self, query, product_ids, top_k, lang=None):  # noqa: ANN001
        self.calls.append((query, list(product_ids), top_k, lang))
        allowed = set(product_ids)
        chunk = _chunk()
        return [chunk] if chunk.product_id in allowed else []


class _ScriptedEvaluator:
    """Скриптованный ``EvaluatorPort`` (I18: без веб-тула)."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0

    def __init__(self, output: LLMCellOutput | Exception) -> None:
        self.output = output
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        if isinstance(self.output, Exception):
            raise self.output
        return LLMResponse(
            output=self.output,
            raw=self.output.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
        )


def _ok_output() -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="корпус подтверждает",
        evidence=[LLMEvidence(source_id="chunk_1", quote="neutral corpus evidence")],
    )


def _absent_output() -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="корпус не подтверждает",
        evidence=[LLMEvidence(source_id="chunk_1", quote="neutral corpus evidence")],
    )


def _unknown_output() -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.UNKNOWN,
        justification="недостаточно свидетельств",
        evidence=[LLMEvidence(source_id="chunk_1", quote="neutral corpus evidence")],
    )


def _ports(output: LLMCellOutput | Exception):
    return SimpleNamespace(
        evaluator=_ScriptedEvaluator(output),
        lexical_index=_FakeLexical(),
        vector_index=None,
        embedder=None,
    )


def _registry_present(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.REGISTRY,
        evidence=[
            LLMEvidence(source_id=f"registry://reg_x/p1#{param_id}", quote="neutral record")
        ],
    )


def _registry_absence(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        source_kind=SourceKind.REGISTRY,
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
        absence_evidence=[
            {
                "source_id": f"registry://reg_x/p1#{param_id}",
                "source_scope": SourceScope.EXHAUSTIVE.value,
                "registry_ids": ["reg_x"],
            }
        ],
    )


def _inherited_present(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[
            LLMEvidence(source_id=f"upstream://base_1#{param_id}", quote="upstream fact")
        ],
    )


def _registry_evidence(param_id: str) -> CellEvidence:
    return CellEvidence(
        evidence_id=f"ev-reg-{param_id}",
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        source_kind=SourceKind.REGISTRY,
        evidence_type=EvidenceType.DIRECT,
        source_id=f"registry://reg_x/p1#{param_id}",
        source_url=f"https://reg.test/p1/{param_id}",
        citation_quote="neutral record",
    )


def _final_cell(store: SqliteStore, param_id: str) -> Cell:
    cells = [
        cell
        for cell in store.iter_cells(RUN_ID)
        if cell.product_id == "p1" and cell.param_id == param_id
    ]
    assert len(cells) == 1
    return cells[0]


def _evidence_kinds(store: SqliteStore, param_id: str) -> set[str]:
    rows = store.conn.execute(
        "SELECT source_kind FROM cell_evidence"
        " WHERE run_id = ? AND product_id = ? AND param_id = ?",
        (RUN_ID, "p1", param_id),
    ).fetchall()
    return {row["source_kind"] for row in rows}


def _run(store: SqliteStore, param_id: str, output, *, strict: bool = True):
    return evaluate_phase(
        plan=_plan([param_id]),
        ports=_ports(output),
        store=store,
        run_id=RUN_ID,
        strict=strict,
        profile="R0",
        cutoff_k=5,
    )


# --------------------------------------------------------------------------- #
# §2: реестр поверх корпуса
# --------------------------------------------------------------------------- #


def test_registry_ok_survives_corpus_ok(store: SqliteStore) -> None:
    store.save_cell(_registry_present("param_b"))

    _run(store, "param_b", _ok_output())

    final = _final_cell(store, "param_b")
    assert final.state is CellState.EVALUATED
    assert final.status is Status.OK
    assert final.source_kind is SourceKind.REGISTRY  # провенанс высокого источника
    assert final.review_reason is None


def test_registry_ok_conflict_corpus_absent_becomes_pending(store: SqliteStore) -> None:
    store.save_cell(_registry_present("param_b"))
    store.add_evidence(_registry_evidence("param_b"))

    _run(store, "param_b", _absent_output())

    final = _final_cell(store, "param_b")
    assert final.state is CellState.PENDING
    assert final.source_kind is SourceKind.REGISTRY
    assert final.review_reason == PRECEDENCE_REVIEW_REASON
    # Оба контура свидетельств сохранены в cell_evidence.
    assert _evidence_kinds(store, "param_b") == {
        SourceKind.REGISTRY.value,
        SourceKind.DOCUMENTATION.value,
    }
    # Конфликт виден человеку: без PENDING в очереди это был бы тихий pending.
    queue = review_queue(store, RUN_ID)
    assert [(item.product_id, item.param_id) for item in queue] == [("p1", "param_b")]
    assert PRECEDENCE_REVIEW_REASON in queue[0].reason


def test_registry_absence_conflict_corpus_ok_becomes_pending(store: SqliteStore) -> None:
    store.save_cell(_registry_absence("param_a"))

    _run(store, "param_a", _ok_output())

    final = _final_cell(store, "param_a")
    assert final.state is CellState.PENDING
    assert final.source_kind is SourceKind.REGISTRY
    assert final.review_reason == PRECEDENCE_REVIEW_REASON
    # Авторитетное ❌ не отменено молча: решение — за человеком.
    queue = review_queue(store, RUN_ID)
    assert [(item.product_id, item.param_id) for item in queue] == [("p1", "param_a")]


@pytest.mark.parametrize("strict", [True, False])
def test_inconclusive_sh6_does_not_annihilate_registry(
    store: SqliteStore, strict: bool
) -> None:
    store.save_cell(_registry_present("param_b"))

    _run(store, "param_b", RuntimeError("adapter down"), strict=strict)

    final = _final_cell(store, "param_b")
    assert final.state is CellState.EVALUATED
    assert final.status is Status.OK
    assert final.source_kind is SourceKind.REGISTRY


def test_registry_status_kept_on_unknown_corpus(store: SqliteStore) -> None:
    store.save_cell(_registry_absence("param_a"))

    _run(store, "param_a", _unknown_output())

    final = _final_cell(store, "param_a")
    assert final.state is CellState.EVALUATED
    assert final.status is Status.ABSENT
    assert final.source_kind is SourceKind.REGISTRY
    assert review_queue(store, RUN_ID) == []  # ❓ не меняет реестровый статус (I12)


def test_uncovered_param_unchanged(store: SqliteStore) -> None:
    _run(store, "param_b", _ok_output())

    final = _final_cell(store, "param_b")
    assert final.state is CellState.EVALUATED
    assert final.status is Status.OK
    assert final.source_kind is SourceKind.DOCUMENTATION
    assert final.review_reason is None


def test_precedence_is_idempotent_on_resume(store: SqliteStore) -> None:
    store.save_cell(_registry_present("param_b"))
    store.add_evidence(_registry_evidence("param_b"))

    _run(store, "param_b", _absent_output())
    first_cell = _final_cell(store, "param_b")
    first_evidence = {
        row["evidence_id"]
        for row in store.conn.execute(
            "SELECT evidence_id FROM cell_evidence"
            " WHERE run_id = ? AND param_id = ?",
            (RUN_ID, "param_b"),
        )
    }

    _run(store, "param_b", _absent_output())
    second_cell = _final_cell(store, "param_b")
    second_evidence = {
        row["evidence_id"]
        for row in store.conn.execute(
            "SELECT evidence_id FROM cell_evidence"
            " WHERE run_id = ? AND param_id = ?",
            (RUN_ID, "param_b"),
        )
    }

    assert (second_cell.state, second_cell.status, second_cell.source_kind) == (
        first_cell.state,
        first_cell.status,
        first_cell.source_kind,
    )
    assert second_cell.review_reason == first_cell.review_reason
    assert second_evidence == first_evidence


def test_store_guard_rejects_silent_downgrade(store: SqliteStore) -> None:
    store.save_cell(_registry_present("param_b"))

    downgrade = Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_b",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        source_kind=SourceKind.DOCUMENTATION,
        absence_evidence=[{"source_scope": SourceScope.PARTIAL.value}],
    )
    with pytest.raises(ValueError, match="прецедент источника"):
        store.save_cell(downgrade)

    # Легитимный конфликт оформляется как pending и проходит guard.
    pending = Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_b",
        state=CellState.PENDING,
        status=Status.ABSENT,
        source_kind=SourceKind.REGISTRY,
        review_reason=PRECEDENCE_REVIEW_REASON,
    )
    store.save_cell(pending)
    assert _final_cell(store, "param_b").state is CellState.PENDING


# --------------------------------------------------------------------------- #
# Fork D: наследование поверх корпуса
# --------------------------------------------------------------------------- #


def test_inherited_cell_survives_corpus_agreement(store: SqliteStore) -> None:
    store.save_cell(_inherited_present("param_b"))

    _run(store, "param_b", _ok_output())

    final = _final_cell(store, "param_b")
    assert final.state is CellState.EVALUATED
    assert final.status is Status.OK
    assert final.source_kind is SourceKind.INHERITANCE


def test_inherited_cell_conflict_becomes_pending(store: SqliteStore) -> None:
    store.save_cell(_inherited_present("param_b"))

    _run(store, "param_b", _absent_output())

    final = _final_cell(store, "param_b")
    assert final.state is CellState.PENDING
    assert final.source_kind is SourceKind.INHERITANCE
    assert final.review_reason == PRECEDENCE_REVIEW_REASON
    queue = review_queue(store, RUN_ID)
    assert [(item.product_id, item.param_id) for item in queue] == [("p1", "param_b")]


def test_inconclusive_sh6_does_not_annihilate_inherited(store: SqliteStore) -> None:
    store.save_cell(_inherited_present("param_b"))

    _run(store, "param_b", RuntimeError("adapter down"))

    final = _final_cell(store, "param_b")
    assert final.state is CellState.EVALUATED
    assert final.status is Status.OK
    assert final.source_kind is SourceKind.INHERITANCE


# --------------------------------------------------------------------------- #
# Fork D: Ш4 не затирает реестр (мёртвый apply_precedence оживает)
# --------------------------------------------------------------------------- #


def _passport() -> Passport:
    return Passport(
        product_id="p1",
        canonical_name="Product p1",
        vendor="Vendor",
        upstream=Upstream(
            baseline_id="base_1",
            version="16.0",
            evidence=Citation(
                url="https://upstream.test/doc#param_a",
                quote="upstream documents the mechanism",
            ),
        ),
        version="1.0",
        jurisdiction="RU",
        snapshot_date=_dt.date(2026, 1, 1),
    )


def _baseline() -> BaselineRow:
    return BaselineRow(
        baseline_id="base_1",
        param_id="param_a",
        min_version=None,
        inheritance_mode=InheritanceMode.DIRECT,
    )


def test_inheritance_wave_keeps_registry_on_agreement(store: SqliteStore) -> None:
    store.save_cell(_registry_present("param_a"))

    result = inheritance_wave(
        _plan(["param_a"]), [_baseline()], {"p1": _passport()}, store
    )

    assert result.errors == []
    assert result.cells == []  # высокий источник уже в store — писать нечего
    final = _final_cell(store, "param_a")
    assert final.source_kind is SourceKind.REGISTRY
    assert final.state is CellState.EVALUATED


def test_inheritance_wave_turns_registry_conflict_into_pending(store: SqliteStore) -> None:
    store.save_cell(_registry_absence("param_a"))

    result = inheritance_wave(
        _plan(["param_a"]), [_baseline()], {"p1": _passport()}, store
    )

    assert result.errors == []
    final = _final_cell(store, "param_a")
    assert final.state is CellState.PENDING
    assert final.source_kind is SourceKind.REGISTRY
    assert final.review_reason == PRECEDENCE_REVIEW_REASON
    queue = review_queue(store, RUN_ID)
    assert [(item.product_id, item.param_id) for item in queue] == [("p1", "param_a")]
