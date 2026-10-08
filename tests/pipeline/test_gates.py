"""Тесты гейтов G1–G4 (W4/T4.2): человеческое решение → строка ``gate_decisions`` (S9).

Гейт не считается пройденным без строки в ``gate_decisions``. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import Focus, Gate, GateDecision, RunConfig, RunMode
from core.pipeline.gates import (
    gate_decision_for,
    gate_g1,
    gate_g2,
    gate_g4,
    is_gate_passed,
    record_decision,
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


def test_gate_not_passed_without_row(store: SqliteStore) -> None:
    for gate in Gate:
        assert is_gate_passed(store, RUN_ID, gate) is False
    assert list(store.iter_gate_decisions(RUN_ID)) == []


def test_gate_passed_after_save_decision(store: SqliteStore) -> None:
    decision = gate_g1(store, RUN_ID, actor="human-1", target="cohort.yaml", notes="ok")

    assert isinstance(decision, GateDecision)
    assert decision.gate is Gate.G1
    assert decision.actor == "human-1"
    assert decision.decision_id
    assert decision.decided_at  # метка времени выставлена
    assert is_gate_passed(store, RUN_ID, Gate.G1) is True
    # прочие гейты по-прежнему не пройдены
    assert is_gate_passed(store, RUN_ID, Gate.G2) is False
    assert [d.decision_id for d in store.iter_gate_decisions(RUN_ID)] == [decision.decision_id]


def test_passed_after_direct_save_gate_decision(store: SqliteStore) -> None:
    # S9: носитель — сама строка gate_decisions, независимо от того, кто её записал.
    store.save_gate_decision(
        GateDecision(
            decision_id="gd-direct",
            run_id=RUN_ID,
            gate=Gate.G3,
            actor="human-2",
            decided_at="2026-01-01T00:00:00+00:00",
        )
    )
    assert is_gate_passed(store, RUN_ID, Gate.G3) is True


def test_actor_is_required(store: SqliteStore) -> None:
    with pytest.raises(ValueError):
        record_decision(store, RUN_ID, Gate.G1, actor="")
    with pytest.raises(ValueError):
        record_decision(store, RUN_ID, Gate.G1, actor="   ")
    # доменная модель тоже отвергает пустого актора
    with pytest.raises(ValueError):
        GateDecision(
            decision_id="x", run_id=RUN_ID, gate=Gate.G1, actor="", decided_at="t"
        )
    assert is_gate_passed(store, RUN_ID, Gate.G1) is False


def test_gate_wrappers_persist_their_gate(store: SqliteStore) -> None:
    gate_g1(store, RUN_ID, actor="h")
    gate_g2(store, RUN_ID, actor="h", target="role:r1")
    gate_g4(store, RUN_ID, actor="h", notes="publish")

    gates = {d.gate for d in store.iter_gate_decisions(RUN_ID)}
    assert gates == {Gate.G1, Gate.G2, Gate.G4}
    assert is_gate_passed(store, RUN_ID, Gate.G3) is False
    latest = gate_decision_for(store, RUN_ID, Gate.G2)
    assert latest is not None and latest.target == "role:r1"


def test_repeated_identical_decision_is_idempotent(store: SqliteStore) -> None:
    first = gate_g1(store, RUN_ID, actor="human-1", notes="same")
    second = gate_g1(store, RUN_ID, actor="human-1", notes="same")

    assert first.decision_id == second.decision_id
    assert len(list(store.iter_gate_decisions(RUN_ID))) == 1


def test_gate_accepts_string_and_normalizes(store: SqliteStore) -> None:
    record_decision(store, RUN_ID, "G4", actor="h")
    assert is_gate_passed(store, RUN_ID, "G4") is True
