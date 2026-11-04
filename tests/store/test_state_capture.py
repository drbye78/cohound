"""Тесты снятия корня в ``state.db`` (план 20 §3/§4, STATE 11).

Офлайн: ``:memory:``/временный файл, без сети. Проверяются идемпотентность upsert'а
(повторный вход в фазу не плодит снятий и не трогает ``started_at``), порядок
``iter_captures``, явный ``superseded_by`` и удаление снятий вместе с продуктом.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.ports.state import CaptureRow
from core.store.state_store import SqliteStateStore


@pytest.fixture()
def store() -> Iterator[SqliteStateStore]:
    with SqliteStateStore(":memory:") as state:
        yield state


def _capture(
    capture_id: str = "cap-1",
    *,
    product_id: str = "p1",
    root_url: str = "https://p1.test/",
    started_at: str = "2026-01-01T00:00:00Z",
    **kw: object,
) -> CaptureRow:
    base: dict[str, object] = {
        "capture_id": capture_id,
        "product_id": product_id,
        "root_url": root_url,
        "started_at": started_at,
        "complete": True,
        "observed_how": "live",
        "run_id": "run-1",
    }
    base.update(kw)
    return CaptureRow(**base)  # type: ignore[arg-type]


def test_record_and_read_capture(store: SqliteStateStore) -> None:
    store.record_capture(
        _capture(
            finished_at="2026-01-01T00:10:00Z",
            coverage_digest="cov",
            doc_set_digest="docs",
            policy_digest="pol",
        )
    )

    row = store.get_capture("cap-1")
    assert row is not None
    assert row.product_id == "p1"
    assert row.complete is True
    assert row.observed_how == "live"
    assert row.coverage_digest == "cov"
    assert row.policy_digest == "pol"
    assert store.get_capture("missing") is None


def test_reentry_updates_outcome_but_not_started_at(
    store: SqliteStateStore,
) -> None:
    """Resume/``--reset``: новое ``started_at`` не переписывает прежнее начало.

    Дайджесты догоняются позже (граница продукта), поэтому ``None`` во второй записи
    не стирает уже известное значение.
    """
    store.record_capture(
        _capture(finished_at="2026-01-01T00:10:00Z", coverage_digest="cov-1")
    )
    store.record_capture(
        _capture(
            started_at="2026-01-02T00:00:00Z",
            finished_at="2026-01-02T00:10:00Z",
            complete=False,
        )
    )

    row = store.get_capture("cap-1")
    assert row is not None
    assert row.started_at == "2026-01-01T00:00:00Z"
    assert row.finished_at == "2026-01-02T00:10:00Z"
    assert row.complete is False
    assert row.coverage_digest == "cov-1"


def test_digests_can_be_filled_later(store: SqliteStateStore) -> None:
    """Граница корня пишет снятие без дайджестов; граница продукта их догоняет."""
    store.record_capture(_capture(finished_at="2026-01-01T00:10:00Z"))
    row = store.get_capture("cap-1")
    assert row is not None and row.coverage_digest is None

    store.record_capture(
        dataclasses.replace(
            row,
            coverage_digest="cov-late",
            doc_set_digest="docs-late",
        )
    )
    updated = store.get_capture("cap-1")
    assert updated is not None
    assert updated.coverage_digest == "cov-late"
    assert updated.doc_set_digest == "docs-late"


def test_iter_captures_is_sorted(store: SqliteStateStore) -> None:
    store.record_capture(_capture("cap-b", started_at="2026-01-02T00:00:00Z"))
    store.record_capture(_capture("cap-a", started_at="2026-01-01T00:00:00Z"))

    assert [row.capture_id for row in store.iter_captures("p1")] == [
        "cap-a",
        "cap-b",
    ]
    assert store.iter_captures("p2") == []


def test_supersede_capture_is_explicit_and_idempotent(
    store: SqliteStateStore,
) -> None:
    store.record_capture(_capture("cap-1"))
    store.record_capture(_capture("cap-2"))

    store.supersede_capture("cap-1", superseded_by="cap-2")
    store.supersede_capture("cap-1", superseded_by="cap-2")
    row = store.get_capture("cap-1")
    assert row is not None and row.superseded_by == "cap-2"

    # Повторная запись снятия не отменяет отзыв (``superseded_by`` — только явный метод).
    store.record_capture(_capture("cap-1"))
    again = store.get_capture("cap-1")
    assert again is not None and again.superseded_by == "cap-2"


def test_forget_product_removes_captures(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    with SqliteStateStore(db_path) as store:
        store.record_capture(_capture("cap-1"))
        assert store.get_capture("cap-1") is not None

        result = store.forget_product("p1")
        assert result.found is True
        assert store.get_capture("cap-1") is None