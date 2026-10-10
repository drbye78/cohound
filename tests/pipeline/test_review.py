"""Тесты фазы ревью Ш7 (W?, §5/§7/§8/G3): очередь ревью и повышение кандидатов.

Проверяются: детерминированная очередь (preliminary + ``❓`` evaluated), отказ повышения
для нечеловека/не-preliminary/без валидированного вердикта, честное повышение с явным
``status`` и ``reviewed_by``, пакетная изоляция. Работают на ``SqliteStore(":memory:")``.
Нейтральны (§1.3) и не импортируют ``core.adapters``.
"""

from __future__ import annotations

import datetime as _dt
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import (
    Cell,
    CellState,
    Focus,
    LLMEvidence,
    ProtocolExhaustion,
    REQUIRED_EXHAUSTION_CONTOURS,
    RunConfig,
    RunMode,
    Status,
    TerminationReason,
    check_unknown_exhaustion,
)
from core.pipeline import review as review_mod
from core.pipeline.review import (
    ReviewError,
    promote,
    promote_batch,
    review_queue,
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


def _preliminary(product_id: str, param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.PRELIMINARY,
        status=None,
    )


def _evaluated(
    product_id: str,
    param_id: str,
    status: Status = Status.UNKNOWN,
) -> Cell:
    evidence = (
        [LLMEvidence(source_id="src-1", quote="q")]
        if status in {Status.OK, Status.PARTIAL}
        else []
    )
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=status,
        evidence=evidence,
    )


def _get(store: SqliteStore, product_id: str, param_id: str) -> Cell:
    (cell,) = [
        c
        for c in store.iter_cells(RUN_ID)
        if c.product_id == product_id and c.param_id == param_id
    ]
    return cell


def _full_exhaustion(product_id: str, param_id: str) -> ProtocolExhaustion:
    """Полный протокол исчерпания I3 (все обязательные контуры)."""
    return ProtocolExhaustion(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        contours=list(REQUIRED_EXHAUSTION_CONTOURS),
        termination_reason=TerminationReason.BUDGET_EXHAUSTED,
    )


# --------------------------------------------------------------------------- #
# review_queue
# --------------------------------------------------------------------------- #


def test_review_queue_lists_preliminary_and_unknown_deterministically(
    store: SqliteStore,
) -> None:
    store.save_cell(_preliminary("p2", "param.b"))
    store.save_cell(_preliminary("p1", "param.a"))
    store.save_cell(_evaluated("p1", "param.c", Status.OK))  # ✅ — не в очереди
    store.save_cell(_evaluated("p2", "param.a", Status.UNKNOWN))  # ❓ — кандидат
    reviewed = Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.d",
        state=CellState.REVIEWED,
        status=Status.UNKNOWN,
    )
    store.save_cell(reviewed)

    queue = review_queue(store, RUN_ID)

    assert [(i.product_id, i.param_id) for i in queue] == [
        ("p1", "param.a"),  # preliminary
        ("p1", "param.d"),  # reviewed
        ("p2", "param.a"),  # evaluated ❓
        ("p2", "param.b"),  # preliminary
    ]
    by_key = {(i.product_id, i.param_id): i for i in queue}
    assert by_key[("p1", "param.a")].state is CellState.PRELIMINARY
    assert by_key[("p1", "param.a")].status is None
    assert by_key[("p2", "param.a")].state is CellState.EVALUATED
    assert by_key[("p2", "param.a")].status is Status.UNKNOWN
    assert by_key[("p1", "param.a")].evidence_count == 0
    assert by_key[("p1", "param.a")].reason  # непустая причина


def test_review_queue_empty(store: SqliteStore) -> None:
    store.save_cell(_evaluated("p1", "param.a", Status.OK))
    assert review_queue(store, RUN_ID) == []


def test_review_queue_custom_states_still_includes_unknown(store: SqliteStore) -> None:
    store.save_cell(_preliminary("p1", "param.a"))
    store.save_cell(_evaluated("p1", "param.b", Status.UNKNOWN))
    store.save_cell(
        Cell(
            run_id=RUN_ID,
            product_id="p1",
            param_id="param.c",
            state=CellState.REVIEWED,
            status=Status.UNKNOWN,
        )
    )

    keys = [
        (i.product_id, i.param_id)
        for i in review_queue(store, RUN_ID, states=(CellState.PRELIMINARY,))
    ]
    # preliminary + ❓-кандидат остаются; reviewed исключён выбранными states
    assert keys == [("p1", "param.a"), ("p1", "param.b")]


# --------------------------------------------------------------------------- #
# promote — отказы
# --------------------------------------------------------------------------- #


def test_promote_refuses_empty_actor(store: SqliteStore) -> None:
    store.save_cell(_preliminary("p1", "param.a"))
    for bad in ("", "   "):
        with pytest.raises(ReviewError):
            promote(
                store=store,
                run_id=RUN_ID,
                product_id="p1",
                param_id="param.a",
                actor=bad,
                status=Status.UNKNOWN,
            )
    # ячейка не тронута
    assert _get(store, "p1", "param.a").state is CellState.PRELIMINARY


def test_promote_refuses_non_preliminary(store: SqliteStore) -> None:
    store.save_cell(_evaluated("p1", "param.a", Status.UNKNOWN))
    with pytest.raises(ReviewError):
        promote(
            store=store,
            run_id=RUN_ID,
            product_id="p1",
            param_id="param.a",
            actor="human-1",
            status=Status.UNKNOWN,
        )


def test_promote_refuses_missing_cell(store: SqliteStore) -> None:
    with pytest.raises(ReviewError):
        promote(
            store=store,
            run_id=RUN_ID,
            product_id="p9",
            param_id="param.z",
            actor="human-1",
            status=Status.UNKNOWN,
        )


def test_promote_validated_only_refuses_unbacked_status(store: SqliteStore) -> None:
    """validated_only=True: ✅ без evidence нарушает I1 → отказ (вердикт не выдуман)."""
    store.save_cell(_preliminary("p1", "param.a"))
    with pytest.raises(ReviewError) as excinfo:
        promote(
            store=store,
            run_id=RUN_ID,
            product_id="p1",
            param_id="param.a",
            actor="human-1",
            status=Status.OK,
            validated_only=True,
        )
    assert excinfo.value.code == "I1"
    assert _get(store, "p1", "param.a").state is CellState.PRELIMINARY


def test_promote_refuses_to_fabricate_status(store: SqliteStore) -> None:
    """Нет status ни у ячейки, ни в аргументе → ReviewError, статус не подставляется."""
    store.save_cell(_preliminary("p1", "param.a"))
    with pytest.raises(ReviewError) as excinfo:
        promote(
            store=store,
            run_id=RUN_ID,
            product_id="p1",
            param_id="param.a",
            actor="human-1",
            validated_only=True,
        )
    assert excinfo.value.code == "I14"
    stored = _get(store, "p1", "param.a")
    assert stored.state is CellState.PRELIMINARY
    assert stored.status is None


# --------------------------------------------------------------------------- #
# promote — успех
# --------------------------------------------------------------------------- #


def test_promote_with_explicit_status_sets_reviewed_by(store: SqliteStore) -> None:
    store.save_cell(_preliminary("p1", "param.a"))

    promoted = promote(
        store=store,
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        actor="human-1",
        notes="переаттестовано вручную",
        status=Status.UNKNOWN,
    )

    assert promoted.state is CellState.EVALUATED
    assert promoted.status is Status.UNKNOWN
    assert promoted.reviewed_by == "human-1"
    stored = _get(store, "p1", "param.a")
    assert stored.state is CellState.EVALUATED
    assert stored.status is Status.UNKNOWN
    assert stored.reviewed_by == "human-1"


def test_promote_unknown_without_protocol_succeeds_via_reviewed_by(
    store: SqliteStore,
) -> None:
    """I3-OR (§2.3): reviewed_by ≠ null удовлетворяет I3 — протокол не требуется.

    promote выставляет ``reviewed_by = actor``, поэтому ``❓`` повышается без протокола.
    Отдельно проверяем, что машинный ``❓`` без human-ветки нарушает I3 (наблюдение).
    """
    store.save_cell(_preliminary("p1", "param.a"))

    promoted = promote(
        store=store,
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        actor="human-1",
        status=Status.UNKNOWN,
    )
    assert promoted.state is CellState.EVALUATED
    assert promoted.status is Status.UNKNOWN
    assert promoted.reviewed_by == "human-1"

    # Машинный `❓` (без reviewed_by) без протокола — нарушение I3 (report-аномалия).
    machine = promoted.model_copy(update={"reviewed_by": None})
    assert check_unknown_exhaustion(machine, None)


def test_promote_unknown_with_incomplete_protocol_succeeds_but_anomaly(
    store: SqliteStore,
) -> None:
    """I3-OR (§2.3): неполный протокол не блокирует promote (human-ветка), но это аномалия."""
    store.save_cell(_preliminary("p1", "param.a"))
    incomplete = ProtocolExhaustion(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        contours=["corpus", "serp"],  # без registry/inheritance
        termination_reason=TerminationReason.BUDGET_EXHAUSTED,
    )
    store.save_exhaustion(incomplete)

    promoted = promote(
        store=store,
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        actor="human-1",
        status=Status.UNKNOWN,
    )
    assert promoted.state is CellState.EVALUATED
    assert promoted.reviewed_by == "human-1"

    # Протокол остаётся неполным — check_unknown_exhaustion это фиксирует.
    assert check_unknown_exhaustion(promoted, incomplete)


def test_promote_unknown_with_full_protocol_succeeds(store: SqliteStore) -> None:
    """I3 (§2.3): полный протокол исчерпания — повышение проходит."""
    store.save_cell(_preliminary("p1", "param.a"))
    store.save_exhaustion(_full_exhaustion("p1", "param.a"))

    promoted = promote(
        store=store,
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        actor="human-1",
        status=Status.UNKNOWN,
    )
    assert promoted.state is CellState.EVALUATED
    assert promoted.status is Status.UNKNOWN


def test_promote_na_is_human_only_and_sets_reviewed_by(store: SqliteStore) -> None:
    """N/A проходит только человеком (I6/I7): promote проставляет reviewed_by."""
    store.save_cell(_preliminary("p1", "param.a"))
    promoted = promote(
        store=store,
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        actor="human-2",
        status=Status.NOT_APPLICABLE,
    )
    assert promoted.status is Status.NOT_APPLICABLE
    assert promoted.reviewed_by == "human-2"


def test_promote_without_validated_only_allows_pending_verdict(
    store: SqliteStore,
) -> None:
    """validated_only=False: первый шаг цикла §1.4 (promote → set-status), status=null."""
    store.save_cell(_preliminary("p1", "param.a"))
    promoted = promote(
        store=store,
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        actor="human-1",
        validated_only=False,
    )
    assert promoted.state is CellState.EVALUATED
    assert promoted.status is None
    assert promoted.reviewed_by == "human-1"


# --------------------------------------------------------------------------- #
# promote_batch
# --------------------------------------------------------------------------- #


def test_promote_batch_isolates_failures(store: SqliteStore) -> None:
    store.save_cell(_preliminary("p1", "param.a"))
    store.save_cell(_evaluated("p1", "param.b", Status.UNKNOWN))  # не preliminary

    result = promote_batch(
        store=store,
        run_id=RUN_ID,
        items=[("p1", "param.a"), ("p1", "param.b"), ("p9", "param.z")],
        actor="human-1",
        statuses={("p1", "param.a"): Status.UNKNOWN},
    )

    assert [c.param_id for c in result.promoted] == ["param.a"]
    assert result.promoted[0].reviewed_by == "human-1"
    assert {(p, q) for p, q, _ in result.skipped} == {
        ("p1", "param.b"),
        ("p9", "param.z"),
    }
    assert result.errors == []
    assert _get(store, "p1", "param.a").state is CellState.EVALUATED


def test_promote_batch_requires_actor(store: SqliteStore) -> None:
    with pytest.raises(ReviewError):
        promote_batch(
            store=store,
            run_id=RUN_ID,
            items=[("p1", "param.a")],
            actor="",
        )


# --------------------------------------------------------------------------- #
# Нейтральность и слоевая гигиена (§1.3, 01 §1)
# --------------------------------------------------------------------------- #


def test_module_source_is_category_neutral() -> None:
    source = Path(review_mod.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    assert [line for line in source.splitlines() if forbidden.search(line)] == []


def test_module_does_not_import_adapters() -> None:
    source = Path(review_mod.__file__).read_text(encoding="utf-8")
    imports = [
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith(("import ", "from "))
    ]
    assert all("core.adapters" not in line for line in imports)
