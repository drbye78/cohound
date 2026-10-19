"""Тесты zero-source покрытия корпуса и аномалии отсутствия строки (O2/O3, MVP-1).

Проверяют:

* ``_persist_corpus_coverage`` пишет строку **для каждого** продукта плана, включая
  ``expected_sources=0, complete=False, reason=no_seeds``;
* ``_coverage_for(planned=True)`` возвращает anomaly-строку вместо ``None``;
* ``evaluate_phase`` для zero-source продукта даёт ``failed``-ячейки с
  ``failure_kind=no_sources`` (не ``❌``) и не зовёт оценщик.

Офлайн: SQLite в памяти, оценщик — заглушка.
"""

from __future__ import annotations

import datetime as _dt
from types import SimpleNamespace

import pytest

from core.domain import (
    CatalogRow,
    CellState,
    CrawlItemStatus,
    CrawlSessionItem,
    Focus,
    Importance,
    ParamType,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline.corpus import _persist_corpus_coverage
from core.pipeline.crawl_session import default_session_id, open_session
from core.pipeline.evaluate import _coverage_for, evaluate_phase
from core.pipeline.runner import Ports
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-zero"


def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1", "p2"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 10, 19),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


def _param() -> CatalogRow:
    return CatalogRow(
        param_id="param_a",
        title="Параметр А",
        module="mod_a",
        group="g",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule="правило",
    )


def test_persist_coverage_writes_zero_source_row_for_every_plan_product() -> None:
    store = _store()
    session = default_session_id(RUN_ID)
    open_session(store, session_id=session, run_id=RUN_ID)
    store.upsert_crawl_session_item(
        CrawlSessionItem(
            session_id=session,
            product_id="p1",
            url="https://p1.test/",
            status=CrawlItemStatus.FETCHED,
        )
    )

    _persist_corpus_coverage(
        store,
        RUN_ID,
        session,
        set(),
        "2026-10-19T00:00:00+00:00",
        products=["p1", "p2"],
    )

    p1 = store.read_corpus_coverage(RUN_ID, "p1")
    p2 = store.read_corpus_coverage(RUN_ID, "p2")
    assert p1 is not None and p1.complete is True
    assert p2 is not None
    assert p2.expected_sources == 0
    assert p2.complete is False
    assert p2.reason == "no_seeds"


def test_coverage_for_missing_planned_product_is_anomaly_not_none() -> None:
    store = _store()
    coverage = _coverage_for(None, store, RUN_ID, "p2", planned=True)
    assert coverage is not None
    assert coverage.complete is False
    assert coverage.expected_sources == 0
    assert coverage.reason == "coverage_missing"
    # Не-planned продукт без строки — по-прежнему «неизвестно».
    assert _coverage_for(None, store, RUN_ID, "p2") is None


def test_evaluate_zero_source_product_yields_failed_no_sources() -> None:
    store = _store()
    store.save_corpus_coverage(
        RUN_ID,
        _coverage_for(None, store, RUN_ID, "p1", planned=True).model_copy(
            update={"reason": "no_seeds"}
        ),
    )
    calls: list[object] = []

    class _Evaluator:
        model_id = "fake"

        def complete(self, request):
            calls.append(request)
            raise AssertionError("оценщик не должен вызываться без источников")

    plan = SimpleNamespace(
        products=["p1"],
        params=[_param()],
        run_id=RUN_ID,
        snapshot_date=_dt.date(2026, 10, 19),
        cohort=SimpleNamespace(frozen_at=_dt.date(2026, 10, 19)),
    )
    result = evaluate_phase(
        plan=plan,
        ports=Ports(evaluator=_Evaluator()),
        store=store,
        run_id=RUN_ID,
    )

    assert calls == []
    assert len(result.cells) == 1
    cell = result.cells[0]
    assert cell.state is CellState.FAILED
    assert cell.status is None  # I4: failed — не статус
    assert any("no_seeds" in error for error in result.errors)
    row = store.conn.execute(
        "SELECT failure_kind FROM cells WHERE run_id = ? AND product_id = ?"
        " AND param_id = ?",
        (RUN_ID, "p1", "param_a"),
    ).fetchone()
    assert row["failure_kind"] == "no_sources"


def test_evaluate_missing_coverage_is_anomaly() -> None:
    """Продукт плана без строки покрытия → failed-ячейки (O3), а не «неизвестно»."""
    store = _store()
    plan = SimpleNamespace(
        products=["p1"],
        params=[_param()],
        run_id=RUN_ID,
        snapshot_date=_dt.date(2026, 10, 19),
        cohort=SimpleNamespace(frozen_at=_dt.date(2026, 10, 19)),
    )

    class _Evaluator:
        model_id = "fake"

        def complete(self, request):
            raise AssertionError("оценщик не должен вызываться без покрытия")

    result = evaluate_phase(
        plan=plan,
        ports=Ports(evaluator=_Evaluator()),
        store=store,
        run_id=RUN_ID,
    )
    assert result.cells and result.cells[0].state is CellState.FAILED
    assert any("coverage_missing" in error for error in result.errors)
    row = store.conn.execute(
        "SELECT failure_kind FROM cells WHERE run_id = ? AND product_id = ?"
        " AND param_id = ?",
        (RUN_ID, "p1", "param_a"),
    ).fetchone()
    assert row["failure_kind"] == "coverage_missing"


def test_complete_zero_source_coverage_is_rejected() -> None:
    """``complete=True`` при ``expected_sources=0`` — противоречие O2."""
    from core.domain import CorpusCoverage

    with pytest.raises(ValueError):
        CorpusCoverage(
            product_id="p1",
            complete=True,
            expected_sources=0,
            observed_sources=0,
            computed_at="2026-10-19T00:00:00+00:00",
        )
