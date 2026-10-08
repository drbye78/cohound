"""Тесты durable-состояния фаз в ``runs.state`` (W4 follow-up, 02 §A.1).

Проверяют, что ``set_phase_state`` **аккумулирует** состояния фаз JSON-картой в
единственной колонке ``runs.state`` (DDL §4.4 не меняется), а ``get_phase_state``
устойчиво читает карту и терпит legacy/повреждённые значения. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Iterator

import pytest

from core.domain import Focus, RunConfig, RunMode
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-1"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        yield s


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1"],
        package="pkg-1",
        focus=Focus(),
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


def test_phase_state_unknown_run_returns_empty(store: SqliteStore) -> None:
    assert store.get_phase_state("nope") == {}


def test_phase_state_null_state_returns_empty(store: SqliteStore) -> None:
    # create_run пишет state = NULL
    assert store.get_phase_state(RUN_ID) == {}


def test_set_phase_state_accumulates_not_overwrites(store: SqliteStore) -> None:
    store.set_phase_state(RUN_ID, "normalize", "ok")
    store.set_phase_state(RUN_ID, "lint", "ok")
    store.set_phase_state(RUN_ID, "g1", "blocked")

    assert store.get_phase_state(RUN_ID) == {
        "normalize": "ok",
        "lint": "ok",
        "g1": "blocked",
    }
    # в колонке — ровно один JSON-объект, а не последняя строка
    raw = store.conn.execute(
        "SELECT state FROM runs WHERE run_id = ?", (RUN_ID,)
    ).fetchone()["state"]
    assert json.loads(raw) == {
        "phase_state": {"normalize": "ok", "lint": "ok", "g1": "blocked"}
    }


def test_set_phase_state_overwrites_same_phase(store: SqliteStore) -> None:
    store.set_phase_state(RUN_ID, "corpus", "failed")
    store.set_phase_state(RUN_ID, "corpus", "ok")
    assert store.get_phase_state(RUN_ID) == {"corpus": "ok"}


def test_phase_state_round_trip(store: SqliteStore) -> None:
    states = {"normalize": "ok", "evaluate": "failed", "g3": "blocked"}
    for phase, state in states.items():
        store.set_phase_state(RUN_ID, phase, state)
    assert store.get_phase_state(RUN_ID) == states


def test_legacy_phase_state_string_is_tolerated(store: SqliteStore) -> None:
    # legacy-формат "{phase}:{state}" (до JSON-носителя) — читается как отсутствие данных
    with store.conn:
        store.conn.execute(
            "UPDATE runs SET state = ? WHERE run_id = ?", ("normalize:ok", RUN_ID)
        )
    assert store.get_phase_state(RUN_ID) == {}

    # следующая запись заменяет legacy свежей картой (не пытаясь его распарсить)
    store.set_phase_state(RUN_ID, "normalize", "ok")
    assert store.get_phase_state(RUN_ID) == {"normalize": "ok"}


def test_invalid_json_state_is_tolerated(store: SqliteStore) -> None:
    with store.conn:
        store.conn.execute(
            "UPDATE runs SET state = ? WHERE run_id = ?", ("{broken json", RUN_ID)
        )
    assert store.get_phase_state(RUN_ID) == {}


def test_json_without_phase_state_key_is_tolerated(store: SqliteStore) -> None:
    with store.conn:
        store.conn.execute(
            "UPDATE runs SET state = ? WHERE run_id = ?", ('{"other": 1}', RUN_ID)
        )
    assert store.get_phase_state(RUN_ID) == {}


def test_set_phase_state_unknown_run_does_not_raise(store: SqliteStore) -> None:
    # UPDATE по несуществующему run_id не бросает; читатель тоже возвращает {}
    store.set_phase_state("does-not-exist", "normalize", "ok")
    assert store.get_phase_state("does-not-exist") == {}
