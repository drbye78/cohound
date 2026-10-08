"""Тесты идемпотентности/инвалидации (W4/T4.4, S8/R15).

Повторная запись того же батча не создаёт дублей (upsert по PK/evidence_id); ``mark_stale``
проставляет ``stale_reason``. Работают на ``SqliteStore(":memory:")``. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import (
    Cell,
    CellEvidence,
    CellState,
    EvidenceType,
    Focus,
    RunConfig,
    RunMode,
    SourceKind,
    StaleReason,
    derived_evidence_id,
)
from core.pipeline.idempotency import (
    evidence_id_for,
    mark_stale,
    upsert_cells,
    upsert_evidence,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-1"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1"],
                package="pkg-1",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        yield s


def _cell(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        state=CellState.PENDING,
    )


def _evidence(seq: int) -> CellEvidence:
    return CellEvidence(
        evidence_id=evidence_id_for(
            RUN_ID, "p1", "param.a", "src-1", SourceKind.MANUAL, seq
        ),
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        source_kind=SourceKind.MANUAL,
        evidence_type=EvidenceType.DIRECT,
        source_id="src-1",
    )


def test_evidence_id_is_deterministic_and_reuses_domain() -> None:
    got = evidence_id_for(RUN_ID, "p1", "param.a", "src-1", SourceKind.MANUAL, 0)
    assert got == evidence_id_for(RUN_ID, "p1", "param.a", "src-1", SourceKind.MANUAL, 0)
    assert got == derived_evidence_id(RUN_ID, "p1", "param.a", "src-1", "manual", 0)
    # строковая форма enum даёт тот же id, что и enum
    assert got == evidence_id_for(RUN_ID, "p1", "param.a", "src-1", "manual", 0)
    assert got != evidence_id_for(RUN_ID, "p1", "param.a", "src-1", SourceKind.MANUAL, 1)
    assert got != evidence_id_for(RUN_ID, "p1", "param.a", "src-2", SourceKind.MANUAL, 0)


def test_upsert_cells_rerun_has_no_duplicates(store: SqliteStore) -> None:
    batch = [_cell("param.a"), _cell("param.b"), _cell("param.c")]

    assert upsert_cells(store, batch) == 3
    assert upsert_cells(store, batch) == 3

    rows = store.conn.execute(
        "SELECT COUNT(*) AS n FROM cells WHERE run_id = ?", (RUN_ID,)
    ).fetchone()["n"]
    assert rows == 3
    assert len(list(store.iter_cells(RUN_ID))) == 3


def test_upsert_cells_updates_same_pk(store: SqliteStore) -> None:
    upsert_cells(store, [_cell("param.a")])
    updated = _cell("param.a")
    updated = updated.model_copy(update={"stale_reason": StaleReason.NEW_BUILD})
    upsert_cells(store, [updated])

    cells = list(store.iter_cells(RUN_ID))
    assert len(cells) == 1
    assert cells[0].stale_reason is StaleReason.NEW_BUILD


def test_upsert_evidence_rerun_has_no_duplicates(store: SqliteStore) -> None:
    batch = [_evidence(0), _evidence(1)]

    assert upsert_evidence(store, batch) == 2
    assert upsert_evidence(store, batch) == 2

    count = store.conn.execute(
        "SELECT COUNT(*) AS n FROM cell_evidence WHERE run_id = ?", (RUN_ID,)
    ).fetchone()["n"]
    assert count == 2


def test_mark_stale_sets_reason_without_mutating_input() -> None:
    cell = _cell("param.a")
    updated = mark_stale(cell, StaleReason.NEW_BUILD)

    assert updated.stale_reason is StaleReason.NEW_BUILD
    assert cell.stale_reason is None  # вход не мутируется
    assert updated.param_id == cell.param_id
    assert updated.run_id == RUN_ID
