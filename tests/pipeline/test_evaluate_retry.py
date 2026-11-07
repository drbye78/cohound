"""Ремонт недоделанной Ш6: класс сбоя в БД + повтор только сбойных ячеек (§13/K2).

Проверяется то, что нельзя проверить «на глаз»:

* класс сбоя доезжает до ``cells.failure_kind`` на **пути фазы** — и в последовательном,
  и в параллельном (``llm_concurrency > 1``) исполнении: буфер записи не теряет запись;
* селектор ``select_failed_cells`` отбирает ровно повторяемые классы и **называет**
  каждую неотобранную ячейку с причиной;
* ``evaluated``/``reviewed``/``preliminary`` не попадают в набор, и оценщик по ним не
  вызывается (шпион);
* успешная попытка снимает класс сбоя и ставит текущую сборку; неуспешная обновляет
  класс и **сохраняет** прежнюю причину рядом с ``retry:attempt=N``;
* ``run_id`` не меняется: строка ``runs`` до и после повтора побайтово та же;
* повтор без провалов — no-op, ни одного вызова оценщика.

Работают на ``SqliteStore(":memory:")``; оценщик — скриптованный in-memory порт (I18).
Нейтральны к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator, Sequence

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    EvidenceScope,
    Focus,
    Importance,
    LLMCellOutput,
    LLMEvidence,
    ParamType,
    RunConfig,
    RunMode,
    SourceKind,
    Status,
)
from core.pipeline import Ports
from core.pipeline.evaluate import (
    RETRY_ATTEMPT_PREFIX,
    RETRY_NOTHING_TO_DO,
    RETRY_SKIP_CORPUS_ABSENT,
    RETRY_SKIP_NOT_RETRYABLE,
    RETRY_SKIP_UNRECORDED,
    RETRY_STATUS_NOOP,
    RETRY_STATUS_RETRIED,
    evaluate_batch,
    retry_failed_cells,
    select_failed_cells,
)
from core.pipeline.prompt import compile_prompt
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-retry-1"
OLD_BUILD = "cb_old"
NEW_BUILD = "cb_new"
QUOTE = "Свойство Альфа доступно без дополнительной настройки."


# ---------------------------------------------------------------------------
# Данные и двойники
# ---------------------------------------------------------------------------


def _param(param_id: str = "param_alpha") -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title="Параметр Альфа",
        module="mod_a",
        group="grp_1",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule="Подтвердить наличие свойства Альфа.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="role_a",
        concept_terms={"ru": ["свойство альфа"], "en": ["alpha feature"]},
        search_langs=["ru"],
    )


class _LexicalStub:
    """Лексический порт-заглушка: фиксированные чанки и провенанс сборки (R0)."""

    engine_version = "stub-1"

    def __init__(self, build_id: str, chunks: Sequence[RetrievedChunk]) -> None:
        self.build_id = build_id
        self.chunks = list(chunks)
        self.calls: list[str] = []

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]:
        self.calls.append(query)
        scoped = [c for c in self.chunks if c.product_id in set(product_ids)]
        return scoped[:top_k]


class _ScriptedEvaluator:
    """Скриптованный ``EvaluatorPort`` (I18: без веб-тула); последний элемент повторяется."""

    def __init__(self, script: list[LLMCellOutput | Exception]) -> None:
        self.script = list(script)
        self.model_id = "fake-model"
        self.model_version = "1.0"
        self.seed = 0
        self.temperature = 0.0
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        item = self.script[min(len(self.calls) - 1, len(self.script) - 1)]
        if isinstance(item, Exception):
            raise item
        return LLMResponse(
            output=item, raw=item.model_dump_json(), prompt_hash="ph", decoder="fake"
        )


def _chunk(product_id: str = "p1", build_id: str = NEW_BUILD) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=f"chunk_{product_id}_1",
        product_id=product_id,
        text=QUOTE,
        content_hash="hash_1",
        heading_path="Раздел 1",
        build_id=build_id,
        score=1.0,
    )


def _ok_output() -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство Альфа подтверждено документацией.",
        evidence=[{"source_id": "chunk_p1_1", "quote": QUOTE}],
    )


def _cell(
    *,
    product_id: str = "p1",
    param_id: str = "param_alpha",
    state: CellState = CellState.FAILED,
    status: Status | None = None,
    source_kind: SourceKind = SourceKind.DOCUMENTATION,
    index_build_id: str | None = OLD_BUILD,
    notes: Sequence[str] = (),
    scope: EvidenceScope | None = None,
) -> Cell:
    evidence = (
        [LLMEvidence(source_id="chunk_p1_1", quote=QUOTE)]
        if status is Status.OK
        else []
    )
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=state,
        status=status,
        source_kind=source_kind,
        evidence=evidence,
        evidence_scope=scope,
        index_build_id=index_build_id,
        validation_notes=list(notes),
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    """Прогон с текущей сборкой ``cb_new``."""
    with SqliteStore(":memory:") as s:
        s.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1", "p2"],
                package="pkg-1",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        s.set_run_index_build_id(RUN_ID, NEW_BUILD)
        yield s


def _seed(store: SqliteStore, cell: Cell, failure_kind: str | None = None) -> None:
    store.save_cell(cell)
    if failure_kind is not None:
        store.record_cell_failure(
            cell.run_id, str(cell.product_id), str(cell.param_id), failure_kind
        )


def _db_cell(store: SqliteStore, product_id: str = "p1") -> Cell:
    return {(c.product_id, c.param_id): c for c in store.iter_cells(RUN_ID)}[
        (product_id, "param_alpha")
    ]


def _db_failure_kind(store: SqliteStore, product_id: str = "p1") -> str | None:
    return store.read_cell_failure_kinds(RUN_ID)[(product_id, "param_alpha")]


def _seed_corpus_doc(store: SqliteStore, product_id: str) -> None:
    store.conn.execute(
        "INSERT INTO corpus_docs (doc_id, product_id, url, origin)"
        " VALUES (?, ?, ?, 'crawl')",
        (f"{product_id}:hash", product_id, f"https://example.test/{product_id}"),
    )
    store.conn.commit()


# ---------------------------------------------------------------------------
# 1. Класс сбоя доезжает до cells.failure_kind на пути фазы
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("llm_concurrency", [1, 2])
def test_phase_path_records_failure_kind(store: SqliteStore, llm_concurrency: int) -> None:
    """Сбой оценщика → ``failure_kind=evaluation_failed`` в БД (оба пути исполнения).

    До правки класс знал только ``EvaluationOutcome``: в ``cells`` он оставался NULL, и
    селектор ремонта не мог отличить отказ прибора от незаписанного класса.
    ``llm_concurrency > 1`` идёт через ``_CellWriteBuffer`` — запись класса обязана
    пережить буфер.
    """
    params = [_param("param_alpha"), _param("param_beta")]
    chunks = {p.param_id: [_chunk()] for p in params}
    evaluator = _ScriptedEvaluator([RuntimeError("оценщик недоступен")])

    outcomes, errors = evaluate_batch(
        params=params,
        product_id="p1",
        run_id=RUN_ID,
        evaluator=evaluator,
        chunks_by_param=chunks,
        store=store,
        max_retries=0,
        llm_concurrency=llm_concurrency,
    )

    assert errors == []
    assert [o.cell.state for o in outcomes] == [CellState.FAILED, CellState.FAILED]
    kinds = store.read_cell_failure_kinds(RUN_ID)
    assert kinds[("p1", "param_alpha")] == "evaluation_failed"
    assert kinds[("p1", "param_beta")] == "evaluation_failed"
    # I4: failed не несёт статус — запись класса его не выдумывает.
    assert all(cell.status is None for cell in store.iter_cells(RUN_ID))


# ---------------------------------------------------------------------------
# 2. Селектор: ровно повторяемые классы, каждый отказ назван
# ---------------------------------------------------------------------------


def test_selector_picks_instrument_failures(store: SqliteStore) -> None:
    """``evaluation_failed``/``validation_failed``/``search_failed`` — повторяемы."""
    for index, kind in enumerate(
        ["evaluation_failed", "validation_failed", "search_failed"]
    ):
        _seed(
            store,
            _cell(param_id=f"param_{index}", index_build_id=None),
            failure_kind=kind,
        )

    selection = select_failed_cells(store, RUN_ID)

    assert selection.failed_total == 3
    assert {c.param_id for c in selection.retryable_cells} == {
        "param_0",
        "param_1",
        "param_2",
    }
    assert selection.skipped == ()


def test_selector_no_sources_requires_corpus(store: SqliteStore) -> None:
    """``no_sources`` повторяем только при наличии корпуса; без него — ``corpus_absent``."""
    _seed_corpus_doc(store, "p1")
    _seed(store, _cell(product_id="p1"), failure_kind="no_sources")
    _seed(store, _cell(product_id="p2"), failure_kind="no_sources")

    selection = select_failed_cells(store, RUN_ID)

    assert [(c.product_id, c.param_id) for c in selection.retryable_cells] == [
        ("p1", "param_alpha")
    ]
    assert selection.skipped == (
        {
            "product_id": "p2",
            "param_id": "param_alpha",
            "reason": RETRY_SKIP_CORPUS_ABSENT,
            "failure_kind": "no_sources",
            "source_kind": "documentation",
            "state": "failed",
            "index_build_id": OLD_BUILD,
        },
    )


def test_selector_null_kind_with_instrument_failed_is_retryable(
    store: SqliteStore,
) -> None:
    """Прогоны до правки: ``failure_kind IS NULL`` + ``instrument_failed`` — повторяемы."""
    _seed(
        store,
        _cell(
            scope=EvidenceScope.for_instrument_failure("evaluation_failed"),
            notes=["оценщик недоступен: EvaluationError: HTTP 503"],
        ),
    )

    selection = select_failed_cells(store, RUN_ID)

    assert [c.param_id for c in selection.retryable_cells] == ["param_alpha"]
    assert selection.skipped == ()


def test_selector_names_unrecorded_and_terminal_skips(store: SqliteStore) -> None:
    """NULL без признака и терминальные классы — в ``skipped`` с причиной."""
    _seed(store, _cell(param_id="param_null", notes=["что-то случилось"]))
    _seed(store, _cell(param_id="param_terminal"), failure_kind="not_found")

    selection = select_failed_cells(store, RUN_ID)

    assert selection.retryable_cells == ()
    reasons = {row["param_id"]: row["reason"] for row in selection.skipped}
    assert reasons["param_null"] == RETRY_SKIP_UNRECORDED
    assert reasons["param_terminal"] == f"{RETRY_SKIP_NOT_RETRYABLE}:not_found"
    assert selection.failed_total == 2


def test_selector_never_returns_cells_with_verdict(store: SqliteStore) -> None:
    """``evaluated``/``reviewed``/``preliminary`` не попадают в набор никогда."""
    _seed(store, _cell(state=CellState.EVALUATED, status=Status.OK))
    _seed(store, _cell(param_id="param_reviewed", state=CellState.REVIEWED, status=Status.UNKNOWN))
    _seed(store, _cell(param_id="param_preliminary", state=CellState.PRELIMINARY))

    selection = select_failed_cells(store, RUN_ID)

    assert selection.failed_total == 0
    assert selection.retryable_cells == ()
    assert selection.skipped == ()


# ---------------------------------------------------------------------------
# 3. Успешная попытка: вердикт, текущая сборка, класс снят
# ---------------------------------------------------------------------------


def test_retry_success_clears_failure_kind_and_sets_build(store: SqliteStore) -> None:
    """Вердикт → ``evaluated``, ``stale_reason=None``, класс снят, сборка текущая."""
    _seed(store, _cell(notes=["оценщик недоступен: HTTP 503"]), "evaluation_failed")
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = retry_failed_cells(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert report.status == RETRY_STATUS_RETRIED
    assert report.failed_total == 1
    assert report.retryable == 1
    assert report.retried == 1
    assert report.recovered == 1
    assert report.still_failed == 0
    assert len(evaluator.calls) == 1  # ровно сбойная ячейка, без «всей фазы»

    cell = _db_cell(store)
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.OK
    assert cell.stale_reason is None
    assert cell.index_build_id == NEW_BUILD
    assert _db_failure_kind(store) is None  # класс снят: ячейка измерена


def test_retry_uses_the_same_prompt_as_phase(store: SqliteStore) -> None:
    """Промпт повтора — из ``compile_prompt`` (те же правила, что у Ш6)."""
    _seed(store, _cell(), "evaluation_failed")
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))
    param = _param()

    retry_failed_cells(
        store=store, run_id=RUN_ID, params={"param_alpha": param}, ports=ports
    )

    expected = compile_prompt(param=param, product_id="p1", chunks=[_chunk()])
    assert evaluator.calls[0].prompt == expected.text


# ---------------------------------------------------------------------------
# 4. Неуспешная попытка: класс обновлён, старая причина сохранена
# ---------------------------------------------------------------------------


def test_retry_failure_updates_kind_and_keeps_old_reason(store: SqliteStore) -> None:
    """Снова нет вердикта → ``failed``/``status=None``, маркер попытки рядом со старой причиной."""
    old_note = "оценщик недоступен: HTTP 503"
    _seed(store, _cell(notes=[old_note]), "evaluation_failed")
    evaluator = _ScriptedEvaluator([RuntimeError("снова 503")])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = retry_failed_cells(
        store=store,
        run_id=RUN_ID,
        params={"param_alpha": _param()},
        ports=ports,
        max_retries=0,
    )

    assert report.recovered == 0
    assert report.still_failed == 1
    cell = _db_cell(store)
    assert cell.state is CellState.FAILED
    assert cell.status is None  # I4
    assert _db_failure_kind(store) == "evaluation_failed"
    assert old_note in cell.validation_notes
    assert f"{RETRY_ATTEMPT_PREFIX}=1" in cell.validation_notes


def test_retry_no_sources_without_corpus_does_not_call_evaluator(
    store: SqliteStore,
) -> None:
    """``no_sources`` без корпуса — ``corpus_absent``, оценщик не вызывается."""
    _seed(store, _cell(), "no_sources")
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = retry_failed_cells(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert report.retryable == 0
    assert report.retried == 0
    assert [row["reason"] for row in report.skipped] == [RETRY_SKIP_CORPUS_ABSENT]
    assert evaluator.calls == []


# ---------------------------------------------------------------------------
# 5. run_id не меняется и повтор без провалов — no-op
# ---------------------------------------------------------------------------


def test_retry_does_not_change_run_identity(store: SqliteStore) -> None:
    """Строка ``runs`` до и после повтора побайтово та же — identity не переписана."""
    _seed(store, _cell(), "evaluation_failed")
    # Непустой отпечаток контракта: сравнение пустого NULL доказывало бы меньше.
    store.conn.execute(
        "UPDATE runs SET execution_contract_digest = ? WHERE run_id = ?",
        ("ec-before-retry", RUN_ID),
    )
    store.conn.commit()
    before = [dict(row) for row in store.conn.execute("SELECT * FROM runs")]
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    retry_failed_cells(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    after = [dict(row) for row in store.conn.execute("SELECT * FROM runs")]
    assert after == before
    assert after[0]["execution_contract_digest"] == "ec-before-retry"
    assert {cell.run_id for cell in store.iter_cells(RUN_ID)} == {RUN_ID}


def test_retry_without_failures_is_noop(store: SqliteStore) -> None:
    """Повтор без провалов — no-op: ни одного вызова оценщика (шпион на порту).

    ``evaluated``/``reviewed``/``preliminary`` не возвращаются селектором, поэтому
    оценщик по ним не вызывается вовсе — повтор не «пересчитывает фазу на всякий
    случай».
    """
    _seed(store, _cell(state=CellState.EVALUATED, status=Status.OK))
    _seed(
        store,
        _cell(
            param_id="param_reviewed",
            state=CellState.REVIEWED,
            status=Status.UNKNOWN,
        ),
    )
    _seed(
        store,
        _cell(param_id="param_preliminary", state=CellState.PRELIMINARY),
    )
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = retry_failed_cells(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert report.status == RETRY_STATUS_NOOP
    assert report.message == RETRY_NOTHING_TO_DO
    assert report.retried == 0
    assert evaluator.calls == []


# ---------------------------------------------------------------------------
# 6. Видимость факта: метрики и cell_diagnostics
# ---------------------------------------------------------------------------


def test_metrics_publish_cells_by_failure_kind(store: SqliteStore) -> None:
    """``metrics.json``: сколько ячеек без вердикта и почему (источник — store)."""
    from core.pipeline.metrics import compute_metrics

    _seed(store, _cell(param_id="param_a"), "evaluation_failed")
    _seed(store, _cell(param_id="param_b"), "no_sources")
    _seed(store, _cell(param_id="param_c"), None)  # класс не записан
    _seed(store, _cell(param_id="param_ok", state=CellState.EVALUATED, status=Status.OK))

    metrics = compute_metrics(store=store, run_id=RUN_ID)

    assert metrics.cells_by_failure_kind == {
        "evaluation_failed": 1,
        "no_sources": 1,
        "unrecorded": 1,
    }
    # Блок публикуется рядом с существующим счётчиком состояний.
    payload = metrics.to_dict()
    assert payload["cells_by_state"]["failed"] == 3
    assert payload["cells_by_failure_kind"]["unrecorded"] == 1


def test_cell_diagnostics_publishes_failure_kind(
    store: SqliteStore, tmp_path
) -> None:
    """``cell_diagnostics.csv``: колонка ``failure_kind``; класс сам по себе — повод."""
    import csv

    from core.pipeline.report import _write_cell_diagnostics

    _seed(store, _cell(param_id="param_a"), "evaluation_failed")  # заметок нет
    _seed(store, _cell(param_id="param_b", notes=["HTTP 503"]), None)

    path = _write_cell_diagnostics(
        list(store.iter_cells(RUN_ID)),
        tmp_path / "cell_diagnostics.csv",
        failure_kinds=store.read_cell_failure_kinds(RUN_ID),
    )

    assert path is not None
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    by_param = {row["param_id"]: row for row in rows if row["kind"] == "cell"}
    assert by_param["param_a"]["failure_kind"] == "evaluation_failed"
    assert by_param["param_b"]["failure_kind"] == ""  # «не записано» ≠ «нет сбоя»
