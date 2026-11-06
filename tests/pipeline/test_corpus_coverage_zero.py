"""Тесты zero-source покрытия корпуса и аномалии отсутствия строки (O2/O3, MVP-1).

Проверяют:

* ``_persist_corpus_coverage`` пишет строку **для каждого** продукта плана, включая
  ``expected_sources=0, complete=False, reason=no_seeds``;
* ``_coverage_for(planned=True)`` возвращает anomaly-строку вместо ``None``;
* ``evaluate_phase`` для zero-source продукта даёт ``failed``-ячейки с
  ``failure_kind=no_sources`` (не ``❌``) и не зовёт оценщик;
* третий член предиката (06.11.2026) — **материал**: ноль элементов сеанса в проходе без
  сети не отменяет оценку, если у продукта есть чанки сборки (иначе ``frozen`` на принятом
  корпусе неизмерим).

Офлайн: SQLite в памяти, оценщик — заглушка.
"""

from __future__ import annotations

import datetime as _dt
from types import SimpleNamespace

import pytest

from core.domain import (
    CatalogRow,
    CellState,
    CorpusChunk,
    CorpusCoverage,
    CorpusDoc,
    CrawlItemStatus,
    CrawlSessionItem,
    Focus,
    Importance,
    IndexManifestEntry,
    LLMCellOutput,
    ParamType,
    RankingPolicy,
    RunConfig,
    RunMode,
    Status,
)
from core.pipeline.corpus import (
    ZERO_SOURCE_ALL_SEEDS_REJECTED,
    ZERO_SOURCE_NO_SEEDS,
)
from core.pipeline.corpus_coverage import _persist_corpus_coverage
from core.pipeline.crawl_session import default_session_id, open_session
from core.pipeline.evaluate import _coverage_for, evaluate_phase
from core.pipeline.runner import Ports
from core.ports.state import CaptureRow
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore
from tests.fakes.ports import FakeEvaluator, FakeLexicalIndex

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

    # F3: полнота — по снятию корня, поэтому удостоверяющее снятие подаётся явно.
    _persist_corpus_coverage(
        store,
        RUN_ID,
        session,
        set(),
        "2026-10-19T00:00:00+00:00",
        products=["p1", "p2"],
        declared_roots={"p1": ["https://p1.test/"]},
        captures={
            "p1": [
                CaptureRow(
                    capture_id="cap-1",
                    product_id="p1",
                    root_url="https://p1.test/",
                    started_at="2026-10-19T00:00:00+00:00",
                    finished_at="2026-10-19T00:10:00+00:00",
                    complete=True,
                    observed_how="live",
                )
            ]
        },
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


# ---------------------------------------------------------------------------
# 06.11.2026: третий член предиката — материал продукта (чанки сборки)
# ---------------------------------------------------------------------------


def _zero_coverage(*, reason: str = "no_seeds") -> CorpusCoverage:
    """Zero-source строка **без** удостоверяющего снятия (``complete=False``)."""
    return CorpusCoverage(
        product_id="p1",
        complete=False,
        expected_sources=0,
        observed_sources=0,
        computed_at="2026-11-06T00:00:00+00:00",
        reason=reason,
    )


def _phase_plan() -> SimpleNamespace:
    return SimpleNamespace(
        products=["p1"],
        params=[_param()],
        run_id=RUN_ID,
        snapshot_date=_dt.date(2026, 10, 19),
        cohort=SimpleNamespace(frozen_at=_dt.date(2026, 10, 19)),
    )


#: Реальные id-пространства стенда 06.11.2026 (``ru-pg-dbms-c2e7b860ac17``):
#: ``runs.index_build_id`` — композиция ``cb_``, ``corpus_chunks.build_id`` — партиция ``pb_``.
COMPOSITION_ID = "cb_97e9f9d3ae7f816429710667"
PARTITION_ID = "pb_258500b0db3048c7ef65971b"


def _declare_partition(
    store: SqliteStore,
    *,
    composition_id: str = COMPOSITION_ID,
    partition_id: str | None = PARTITION_ID,
    product_id: str = "p1",
) -> None:
    """Объявляет композицию прогона и продукт-партицию в ``index_manifest`` (K1/I17)."""
    store.set_run_index_build_id(RUN_ID, composition_id)
    store.save_index_manifest(
        IndexManifestEntry(
            index_build_id=composition_id,
            partition_build_id=partition_id,
            product_id=product_id,
        )
    )


def _seed_material(
    store: SqliteStore,
    *,
    composition_id: str = COMPOSITION_ID,
    partition_id: str = PARTITION_ID,
    product_id: str = "p1",
) -> None:
    """Материал как на стенде: композиция → партиция в манифесте →чанк под ``pb_``."""
    _declare_partition(
        store,
        composition_id=composition_id,
        partition_id=partition_id,
        product_id=product_id,
    )
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id=f"{product_id}:doc_a",
            product_id=product_id,
            url="https://p1.test/doc-a",
            content_hash="hash_a1",
        )
    )
    store.add_chunk(
        CorpusChunk(
            chunk_id="chunk_a1",
            doc_id=f"{product_id}:doc_a",
            product_id=product_id,
            chunk_no=0,
            text="Свойство А доступно.",
            content_hash="hash_a1",
            build_id=partition_id,
        )
    )


def _chunks_index() -> FakeLexicalIndex:
    return FakeLexicalIndex(
        docs=[
            RetrievedChunk(
                chunk_id="chunk_a1",
                product_id="p1",
                text="Свойство А доступно.",
                content_hash="hash_a1",
                heading_path="",
                build_id=PARTITION_ID,
                score=1.0,
            )
        ]
    )


class _BombEvaluator:
    """Оценщик-ловушка: вызов доказывает, что ветка отказа не сработала."""

    model_id = "fake"

    def complete(self, request):  # pragma: no cover - не должен вызываться
        raise AssertionError("оценщик не должен вызываться без материала")


def _measurement_output() -> LLMCellOutput:
    return LLMCellOutput(status=Status.UNKNOWN, justification="не установлено")


def test_zero_source_with_material_is_measured_not_failed() -> None:
    """``expected_sources=0`` + ``complete=False`` + материал **есть** ⇒ ячейка меряется.

    Дефект 06.11.2026: в проходе без сети сеанса краула нет вовсе, поэтому
    ``expected_sources`` всегда 0; adoption даёт ``complete=0``. Двух фактов хватало,
    чтобы объявить ``failed`` весь принятый корпус (``cells_measured=0`` при 2952
    документах и 37 589 чанках). Материал (чанк сборки) обязан снимать отказ: иначе
    I2c, ожидающий оценку при неполном покрытии, недостижим.

    Стенд сеет **два разных** id-пространства: ``runs.index_build_id = cb_…``,
    ``corpus_chunks.build_id = pb_…``, связь — ``index_manifest``. Проба по композиции
    здесь вернула бы ``False`` и уронила бы всю ветку (проверено на стенде).
    """
    store = _store()
    store.save_corpus_coverage(RUN_ID, _zero_coverage())
    _seed_material(store)

    result = evaluate_phase(
        plan=_phase_plan(),
        ports=Ports(
            evaluator=FakeEvaluator(default=_measurement_output()),
            lexical_index=_chunks_index(),
        ),
        store=store,
        run_id=RUN_ID,
        profile="R0",
        cutoff_k=3,
    )

    assert len(result.cells) == 1
    cell = result.cells[0]
    assert cell.state is not CellState.FAILED
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.UNKNOWN
    assert not any("no_sources" in error for error in result.errors)
    row = store.conn.execute(
        "SELECT state, failure_kind FROM cells WHERE run_id = ? AND product_id = ?"
        " AND param_id = ?",
        (RUN_ID, "p1", "param_a"),
    ).fetchone()
    assert row["failure_kind"] is None


def test_zero_source_without_material_stays_failed_no_sources() -> None:
    """Партиция объявлена, но чанков под ней нет ⇒ прежний ``failed/no_sources`` (регрессия)."""
    store = _store()
    store.save_corpus_coverage(RUN_ID, _zero_coverage())
    # Композиция и партиция названы, чанков нет: причина — именно пустая партиция.
    _declare_partition(store, partition_id="pb_empty")

    result = evaluate_phase(
        plan=_phase_plan(),
        ports=Ports(evaluator=_BombEvaluator()),
        store=store,
        run_id=RUN_ID,
    )

    cell = result.cells[0]
    assert cell.state is CellState.FAILED
    assert cell.status is None  # I4
    assert any(
        "no_sources" in error and "material=none" in error for error in result.errors
    )
    assert any("no_seeds" in error for error in result.errors)
    # Причина называет **партицию**, а не композицию: чанки живут под ``pb_``.
    assert any("partition=pb_empty" in error for error in result.errors)
    row = store.conn.execute(
        "SELECT failure_kind FROM cells WHERE run_id = ? AND product_id = ?"
        " AND param_id = ?",
        (RUN_ID, "p1", "param_a"),
    ).fetchone()
    assert row["failure_kind"] == "no_sources"


def test_zero_source_product_absent_from_manifest_is_failed() -> None:
    """Композиция есть, но манифест не называет партицию продукта ⇒ ``failed`` + причина."""
    store = _store()
    store.save_corpus_coverage(RUN_ID, _zero_coverage())
    # Манифест объявляет **другой** продукт: «искать любой build_id» запрещено.
    _declare_partition(store, product_id="p_other")

    result = evaluate_phase(
        plan=_phase_plan(),
        ports=Ports(evaluator=_BombEvaluator()),
        store=store,
        run_id=RUN_ID,
    )

    assert result.cells[0].state is CellState.FAILED
    assert any(
        "манифест не называет партицию продукта p1" in error for error in result.errors
    )
    assert any(COMPOSITION_ID in error for error in result.errors)


def test_zero_source_manifest_read_failure_is_failed_and_reported() -> None:
    """Сбой ``iter_index_manifest`` ⇒ ``failed`` + причина в ``errors`` (не молча)."""

    class _BrokenManifest(SqliteStore):
        def iter_index_manifest(self, index_build_id: str):  # noqa: ANN201, ARG002
            raise RuntimeError("manifest is down")

    store = _BrokenManifest(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 10, 19),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    store.save_corpus_coverage(RUN_ID, _zero_coverage())
    store.set_run_index_build_id(RUN_ID, COMPOSITION_ID)

    result = evaluate_phase(
        plan=_phase_plan(),
        ports=Ports(evaluator=_BombEvaluator()),
        store=store,
        run_id=RUN_ID,
    )

    assert result.cells[0].state is CellState.FAILED
    assert any(
        "manifest read failed" in error and "manifest is down" in error
        for error in result.errors
    )
    row = store.conn.execute(
        "SELECT failure_kind FROM cells WHERE run_id = ? AND product_id = ?"
        " AND param_id = ?",
        (RUN_ID, "p1", "param_a"),
    ).fetchone()
    assert row["failure_kind"] == "no_sources"


def test_zero_source_without_known_build_stays_failed_and_names_the_reason() -> None:
    """Сборка прогона не объявлена ⇒ материал считается отсутствующим, причина в ``errors``."""
    store = _store()
    store.save_corpus_coverage(RUN_ID, _zero_coverage())

    result = evaluate_phase(
        plan=_phase_plan(),
        ports=Ports(evaluator=_BombEvaluator()),
        store=store,
        run_id=RUN_ID,
    )

    assert result.cells[0].state is CellState.FAILED
    assert any("build_id unknown" in error for error in result.errors)


def test_zero_source_probe_failure_is_failed_and_reported() -> None:
    """Сбой чтения счётчика материала ⇒ ``failed`` + причина в ``errors`` (не молча)."""

    class _BrokenChunkProbe(SqliteStore):
        def has_corpus_chunks(
            self, build_id: str, *, product_id: str | None = None
        ) -> bool:
            raise RuntimeError("probe is down")

    store = _BrokenChunkProbe(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 10, 19),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    store.save_corpus_coverage(RUN_ID, _zero_coverage())
    store.set_run_index_build_id(RUN_ID, "ib-any")
    _declare_partition(store, composition_id="ib-any", partition_id="pb-any")

    result = evaluate_phase(
        plan=_phase_plan(),
        ports=Ports(evaluator=_BombEvaluator()),
        store=store,
        run_id=RUN_ID,
    )

    assert result.cells[0].state is CellState.FAILED
    assert any(
        "chunk probe failed" in error and "probe is down" in error
        for error in result.errors
    )
    row = store.conn.execute(
        "SELECT failure_kind FROM cells WHERE run_id = ? AND product_id = ?"
        " AND param_id = ?",
        (RUN_ID, "p1", "param_a"),
    ).fetchone()
    assert row["failure_kind"] == "no_sources"


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


def test_persist_coverage_uses_truthful_reason_mapping() -> None:
    """Явная карта причин: строка покрытия называет **путь**, а не общую константу.

    ``all_seeds_rejected`` — seed'ы были, но все отклонены; ``None`` — исходов нет по
    причине, которую называют ``errors``/счётчики (причина не выдумывается).
    """
    store = _store()
    session = default_session_id(RUN_ID)
    open_session(store, session_id=session, run_id=RUN_ID)

    _persist_corpus_coverage(
        store,
        RUN_ID,
        session,
        set(),
        "2026-10-19T00:00:00+00:00",
        products=["p1", "p2"],
        reasons={"p1": ZERO_SOURCE_ALL_SEEDS_REJECTED, "p2": None},
    )

    p1 = store.read_corpus_coverage(RUN_ID, "p1")
    p2 = store.read_corpus_coverage(RUN_ID, "p2")
    assert p1 is not None and p1.reason == ZERO_SOURCE_ALL_SEEDS_REJECTED
    # ``None`` — честное «счётчики/errors объясняют», а не подмена на ``no_seeds``.
    assert p2 is not None and p2.reason is None


def test_persist_coverage_without_reason_map_keeps_legacy_no_seeds() -> None:
    """Прямой вызов без карты причин сохраняет прежнее ``no_seeds`` (совместимость)."""
    store = _store()
    session = default_session_id(RUN_ID)
    open_session(store, session_id=session, run_id=RUN_ID)

    _persist_corpus_coverage(
        store,
        RUN_ID,
        session,
        set(),
        "2026-10-19T00:00:00+00:00",
        products=["p1"],
    )

    p1 = store.read_corpus_coverage(RUN_ID, "p1")
    assert p1 is not None and p1.reason == ZERO_SOURCE_NO_SEEDS
