"""Хранилище политики краула ``state.db:crawl_policy`` (план 19 §4, STATE 8).

Проверяется append-only-поведение (смена режима не правит строку, а добавляет новую с
``superseded_by``), детерминированный порядок выборки и read-only-путь, которым
пользуется ``plan``/``crawl policy list``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.domain.enums import CrawlPolicyMode, CrawlPolicyScopeKind
from core.ports.state import CaptureRow, CrawlPolicyRow
from core.store.state_sqlite import STATE_SCHEMA_VERSION, connect_state, state_db_path
from core.store.state_store import (
    SqliteStateStore,
    read_capture_spaces,
    read_crawl_observations,
    read_crawl_policy_rows,
)

pytestmark = pytest.mark.usefixtures("isolated_state_dir")


def _row(
    *,
    policy_id: int | None = None,
    product_id: str = "p1",
    kind: CrawlPolicyScopeKind = CrawlPolicyScopeKind.PRODUCT,
    mode: CrawlPolicyMode = CrawlPolicyMode.AUTO,
    host: str | None = None,
    path: str | None = None,
    interval: int | None = None,
    note: str | None = None,
    actor: str = "tester",
) -> CrawlPolicyRow:
    return CrawlPolicyRow(
        policy_id=policy_id,
        product_id=product_id,
        scope_kind=kind,
        mode=mode,
        host=host,
        path=path,
        interval_hours=interval,
        note=note,
        actor=actor,
        created_at="2026-11-04T10:00:00Z",
    )


def test_record_list_and_supersede_are_append_only(tmp_path: Path) -> None:
    store = SqliteStateStore(tmp_path / "state.db")
    try:
        first = store.record_crawl_policy(
            _row(kind=CrawlPolicyScopeKind.HOST, mode=CrawlPolicyMode.AUTO, host="docs.test")
        )
        second = store.record_crawl_policy(
            _row(
                kind=CrawlPolicyScopeKind.HOST,
                mode=CrawlPolicyMode.NETWORK_DENIED,
                host="docs.test",
            )
        )
        assert second > first
        assert [row.policy_id for row in store.list_crawl_policy()] == [first, second]

        store.supersede_crawl_policy(first, superseded_by=second)
        store.supersede_crawl_policy(first, superseded_by=second)  # идемпотентно
        active = store.list_crawl_policy()
        assert [row.policy_id for row in active] == [second]
        assert active[0].mode is CrawlPolicyMode.NETWORK_DENIED
        all_rows = store.list_crawl_policy(include_superseded=True)
        assert {row.policy_id for row in all_rows} == {first, second}
        assert {row.superseded_by for row in all_rows if row.policy_id == first} == {second}
    finally:
        store.close()


def test_clear_retires_without_successor(tmp_path: Path) -> None:
    """Схема без tombstone-режима: отзыв — самоссылка, строка остаётся в аудите."""
    store = SqliteStateStore(tmp_path / "state.db")
    try:
        policy_id = store.record_crawl_policy(_row(mode=CrawlPolicyMode.NETWORK_DENIED))
        store.supersede_crawl_policy(policy_id)
        assert store.list_crawl_policy() == []
        kept = store.list_crawl_policy(include_superseded=True)
        assert kept[0].superseded_by == policy_id
    finally:
        store.close()


def test_order_is_deterministic(tmp_path: Path) -> None:
    store = SqliteStateStore(tmp_path / "state.db")
    try:
        for product_id, host in (
            ("p2", "b.test"),
            ("p1", "z.test"),
            ("p1", "a.test"),
        ):
            store.record_crawl_policy(
                _row(
                    product_id=product_id,
                    kind=CrawlPolicyScopeKind.HOST,
                    host=host,
                    mode=CrawlPolicyMode.AUTO,
                )
            )
        assert [
            (row.product_id, row.host) for row in store.list_crawl_policy()
        ] == [("p1", "a.test"), ("p1", "z.test"), ("p2", "b.test")]
    finally:
        store.close()


def test_read_only_helpers_do_not_write(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    conn = connect_state(db_path)
    conn.close()
    before = db_path.read_bytes()

    store = SqliteStateStore(db_path)
    try:
        store.record_crawl_policy(_row(mode=CrawlPolicyMode.ON_REQUEST))
    finally:
        store.close()
    after_write = db_path.read_bytes()
    assert after_write != before  # запись была

    rows = read_crawl_policy_rows(db_path)
    assert [row.mode for row in rows] == [CrawlPolicyMode.ON_REQUEST]
    assert read_crawl_policy_rows(tmp_path / "missing.db") == []
    assert db_path.read_bytes() == after_write  # чтение ничего не пишет


def test_read_observations_filters_without_last_checked(tmp_path: Path) -> None:
    from core.ports.state import UrlStateRow

    db_path = tmp_path / "state.db"
    store = SqliteStateStore(db_path)
    try:
        store.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url="https://docs.test/a",
                normalized_url="https://docs.test/a",
                last_checked_at="2026-11-04T09:00:00Z",
            )
        )
        store.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url="https://docs.test/b",
                normalized_url="https://docs.test/b",
            )
        )
        observed = read_crawl_observations(db_path)
        assert observed == {("p1", "https://docs.test/a"): "2026-11-04T09:00:00Z"}
    finally:
        store.close()


def test_schema_version_and_table_declared(tmp_path: Path) -> None:
    conn = connect_state(tmp_path / "state.db")
    try:
        assert STATE_SCHEMA_VERSION == "14"
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(crawl_policy)")
        }
        assert columns == {
            "policy_id",
            "product_id",
            "scope_kind",
            "host",
            "path",
            "mode",
            "interval_hours",
            "note",
            "actor",
            "created_at",
            "superseded_by",
        }
        indexes = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
        }
        assert "crawl_policy_scope" in indexes
    finally:
        conn.close()


def test_version_seven_file_is_rejected_and_unchanged(tmp_path: Path) -> None:
    """STATE 8 не догоняет STATE 7: видимый отказ, файл не меняется ни на байт."""
    from core.store.sqlite import SchemaVersionMismatch

    path = state_db_path(tmp_path / "state", "pkg")
    path.parent.mkdir(parents=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', '7');"
        )
        conn.commit()
    finally:
        conn.close()
    before = path.read_bytes()
    with pytest.raises(SchemaVersionMismatch):
        connect_state(path)
    assert path.read_bytes() == before

def _capture(
    capture_id: str,
    *,
    started_at: str,
    finished_at: str | None,
    digest: str,
    space_json: str | None = None,
    complete: bool = True,
) -> CaptureRow:
    return CaptureRow(
        capture_id=capture_id,
        product_id="p1",
        root_url="https://docs.test/",
        started_at=started_at,
        finished_at=finished_at,
        complete=complete,
        observed_how="live",
        policy_digest=digest,
        space_json=space_json,
    )


def test_read_capture_spaces_returns_newest_per_root(tmp_path: Path) -> None:
    """Новейшее снятие корня идёт первым — порядок и есть ответ «что последнее».

    Второй формулы свежести у вызывающего нет: ``capture_spaces_by_root`` берёт первое
    вхождение. ``finished_at`` сортируется как текст ISO — все метки в проекте в UTC.
    """
    db = state_db_path(tmp_path / "state", "pkg")
    db.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteStateStore(db)
    try:
        store.record_capture(_capture("old", started_at="2026-11-01T00:00:00Z", finished_at="2026-11-01T01:00:00Z", digest="d1"))
        store.record_capture(_capture("new", started_at="2026-11-10T00:00:00Z", finished_at="2026-11-10T01:00:00Z", digest="d2", space_json='{"x":1}'))
        store.record_capture(_capture("running", started_at="2026-11-15T00:00:00Z", finished_at=None, digest="d3", complete=False))
    finally:
        store.close()
    spaces = read_capture_spaces(db)
    assert spaces == [("p1", "https://docs.test/", "d2", '{"x":1}')]


def test_read_capture_spaces_skips_superseded(tmp_path: Path) -> None:
    """Явный отзыв побеждает свежесть: отозванное снятие носителем не считается."""
    db = state_db_path(tmp_path / "state", "pkg")
    db.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteStateStore(db)
    try:
        store.record_capture(_capture("a", started_at="2026-11-01T00:00:00Z", finished_at="2026-11-01T01:00:00Z", digest="d1"))
        # Отзыв ссылается на преемника внешним ключом: он обязан уже существовать.
        store.record_capture(_capture("c", started_at="2026-11-11T00:00:00Z", finished_at="2026-11-11T01:00:00Z", digest="d3"))
        store.record_capture(_capture("b", started_at="2026-11-10T00:00:00Z", finished_at="2026-11-10T01:00:00Z", digest="d2"))
        store.supersede_capture("b", superseded_by="c")
    finally:
        store.close()
    assert read_capture_spaces(db) == [("p1", "https://docs.test/", "d3", None)]


def test_read_capture_spaces_missing_file_is_empty(tmp_path: Path) -> None:
    """Нет файла — пространство неизвестно, а не «снятий не было» (решение уйдёт по часам)."""
    assert read_capture_spaces(tmp_path / "absent.db") == []
