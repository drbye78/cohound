"""Тесты гейтов G1/G2 (W4/T4.2): человеческое решение → строка ``gate_decisions`` (S9).

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
    is_gate_passed,
    record_decision,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-1"


def test_gate_set_is_g1_to_g4() -> None:
    """Набор гейтов — G1–G4, но с разной семантикой.

    ``G1``/``G2`` — внутренняя аттестация профиля и матрицы роль×продукт;
    ``G3``/``G4`` — **точки интеграции**: strict-ревью и утверждение публикации выполняет
    внешняя система, coHound записывает ответ. Значения стабильны (persisted data), поэтому
    набор зафиксирован здесь: молчаливое изменение семантики гейта недопустимо.
    """
    assert [gate.value for gate in Gate] == ["G1", "G2", "G3", "G4"]


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
            gate=Gate.G2,
            actor="human-2",
            decided_at="2026-01-01T00:00:00+00:00",
        )
    )
    assert is_gate_passed(store, RUN_ID, Gate.G2) is True
    # Другой гейт этим решением не пройден: строки per-gate, а не «гейт вообще».
    assert is_gate_passed(store, RUN_ID, Gate.G1) is False


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

    gates = {d.gate for d in store.iter_gate_decisions(RUN_ID)}
    assert gates == {Gate.G1, Gate.G2}
    latest = gate_decision_for(store, RUN_ID, Gate.G2)
    assert latest is not None and latest.target == "role:r1"


def test_repeated_identical_decision_is_idempotent(store: SqliteStore) -> None:
    first = gate_g1(store, RUN_ID, actor="human-1", notes="same")
    second = gate_g1(store, RUN_ID, actor="human-1", notes="same")

    assert first.decision_id == second.decision_id
    assert len(list(store.iter_gate_decisions(RUN_ID))) == 1


def test_gate_accepts_string_and_normalizes(store: SqliteStore) -> None:
    record_decision(store, RUN_ID, "G2", actor="h")
    assert is_gate_passed(store, RUN_ID, "G2") is True


def test_unknown_gate_value_is_rejected(store: SqliteStore) -> None:
    """Неизвестный гейт не принимается ни строкой, ни моделью."""
    for unknown in ("G5", "g1", ""):
        with pytest.raises(ValueError):
            record_decision(store, RUN_ID, unknown, actor="h")
        with pytest.raises(ValueError):
            GateDecision(
                decision_id="gd-x",
                run_id=RUN_ID,
                gate=unknown,  # type: ignore[arg-type]
                actor="h",
            )
    assert list(store.iter_gate_decisions(RUN_ID)) == []


def test_legacy_rows_without_new_columns_still_pass(store: SqliteStore) -> None:
    """Старые строки ``gate_decisions`` (без ``subject_digest``/``verdict``) проходят гейт.

    Обратная совместимость обязательна: в существующих БД решения записаны до появления
    привязки к предмету и вердикта. Колонки получают дефолты (``''``/``internal``/
    ``approved``), строка читается и гейт проходит — старые прогоны не требуют
    пере-аттестации. Привязка обязательна только для **новых** решений.
    """
    store.conn.execute(
        "INSERT INTO gate_decisions (decision_id, run_id, gate, target, actor,"
        " decided_at, notes) VALUES ('gd-legacy-g3', ?, 'G3', 'cell:x', 'old-human',"
        " '2026-01-01T00:00:00Z', 'strict review')",
        (RUN_ID,),
    )
    store.conn.execute(
        "INSERT INTO gate_decisions (decision_id, run_id, gate, target, actor,"
        " decided_at, notes) VALUES ('gd-legacy-g4', ?, 'G4', 'report', 'old-human',"
        " '2026-01-02T00:00:00Z', 'publish')",
        (RUN_ID,),
    )
    store.conn.commit()

    # Чтение не падает: старые строки видны как доменные решения без привязки.
    decisions = list(store.iter_gate_decisions(RUN_ID))
    assert {d.gate for d in decisions} == {Gate.G3, Gate.G4}
    assert all(d.subject_digest is None for d in decisions)
    assert all(d.verdict.value == "approved" for d in decisions)
    assert is_gate_passed(store, RUN_ID, Gate.G3) is True
    # Строки остались в БД (аудит не переписан).
    rows = store.conn.execute(
        "SELECT decision_id FROM gate_decisions WHERE run_id = ? ORDER BY decision_id",
        (RUN_ID,),
    ).fetchall()
    assert [row["decision_id"] for row in rows] == ["gd-legacy-g3", "gd-legacy-g4"]

    # Новые решения G1/G2 рядом с историческими читаются нормально.
    gate_g1(store, RUN_ID, actor="human-now")
    assert is_gate_passed(store, RUN_ID, Gate.G1) is True
