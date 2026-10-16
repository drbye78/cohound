"""Тесты сеанса краула ``SqliteStore`` (Lane B): сеанс, элементы, агрегаты, recovery.

Все тесты работают на ``:memory:``-БД. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from collections.abc import Iterator

import pytest

from core.domain import (
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
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
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=["param.a"]),
        languages=["ru", "en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _session(**overrides: object) -> CrawlSession:
    base: dict[str, object] = {
        "session_id": "s1",
        "run_id": RUN_ID,
        "profile": "local",
        "status": CrawlSessionStatus.CREATED,
        "created_at": "2026-01-01T00:00:00Z",
    }
    base.update(overrides)
    return CrawlSession(**base)


def _item(url: str, status: CrawlItemStatus, product_id: str = "p1", **overrides: object) -> CrawlSessionItem:
    base: dict[str, object] = {
        "session_id": "s1",
        "product_id": product_id,
        "url": url,
        "status": status,
    }
    base.update(overrides)
    return CrawlSessionItem(**base)


def _counts(store: SqliteStore, session_id: str = "s1") -> dict[str, int]:
    return store.crawl_session_counts(session_id)


# --------------------------------------------------------------------------- #
# сеанс
# --------------------------------------------------------------------------- #


def test_upsert_get_session_round_trip(store: SqliteStore) -> None:
    session = _session(
        profile="split",
        status=CrawlSessionStatus.RUNNING,
        started_at="2026-01-01T01:00:00Z",
        options_json='{"from_scratch": false}',
        provenance_json='{"engine": "local"}',
    )
    store.upsert_crawl_session(session)
    got = store.get_crawl_session("s1")
    assert got is not None
    assert got.session_id == "s1"
    assert got.run_id == RUN_ID
    assert got.profile == "split"
    assert got.status is CrawlSessionStatus.RUNNING
    assert got.started_at == "2026-01-01T01:00:00Z"
    assert got.options_json == '{"from_scratch": false}'
    assert got.provenance_json == '{"engine": "local"}'


def test_get_unknown_session_is_none(store: SqliteStore) -> None:
    assert store.get_crawl_session("missing") is None


def test_standalone_session_nullable_run_id(store: SqliteStore) -> None:
    """C1: standalone-сеанс (``run_id=None``) сохраняется/читается без строки ``runs``."""
    store.upsert_crawl_session(
        CrawlSession(
            session_id="s-standalone",
            profile="local",
            status=CrawlSessionStatus.CREATED,
            created_at="2026-01-01T00:00:00Z",
        )
    )
    got = store.get_crawl_session("s-standalone")
    assert got is not None
    assert got.run_id is None
    assert got.status is CrawlSessionStatus.CREATED

    # Элемент standalone-сеанса тоже пишется: FK только на crawl_sessions, не на runs.
    store.upsert_crawl_session_item(
        CrawlSessionItem(
            session_id="s-standalone",
            product_id="p1",
            url="https://docs.test/x",
            status=CrawlItemStatus.PENDING,
        )
    )
    assert _counts(store, "s-standalone")["pending"] == 1


def test_upsert_session_updates_in_place(store: SqliteStore) -> None:
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session(_session(status=CrawlSessionStatus.PAUSED, profile="split"))
    rows = store.conn.execute("SELECT COUNT(*) AS n FROM crawl_sessions").fetchone()
    assert rows["n"] == 1
    got = store.get_crawl_session("s1")
    assert got is not None and got.status is CrawlSessionStatus.PAUSED and got.profile == "split"


def test_update_session_status_partial_and_idempotent(store: SqliteStore) -> None:
    store.upsert_crawl_session(_session())
    store.update_crawl_session_status(
        "s1", CrawlSessionStatus.RUNNING, started_at="2026-01-01T01:00:00Z"
    )
    got = store.get_crawl_session("s1")
    assert got is not None and got.status is CrawlSessionStatus.RUNNING
    assert got.started_at == "2026-01-01T01:00:00Z"

    # error=None не затирает ранее записанное (перекрываются только не-None).
    store.update_crawl_session_status("s1", CrawlSessionStatus.FAILED, error="boom")
    store.update_crawl_session_status("s1", CrawlSessionStatus.FAILED)
    got = store.get_crawl_session("s1")
    assert got is not None
    assert got.error == "boom"
    assert got.started_at == "2026-01-01T01:00:00Z"


def test_update_status_running_clears_finished_and_error(store: SqliteStore) -> None:
    """C3: переход в ``running``/``created`` очищает признаки завершённости."""
    store.upsert_crawl_session(_session(status=CrawlSessionStatus.CREATED))
    store.update_crawl_session_status(
        "s1", CrawlSessionStatus.COMPLETED, finished_at="t9", error="boom"
    )
    got = store.get_crawl_session("s1")
    assert got is not None
    assert got.finished_at == "t9" and got.error == "boom"

    # completed → running: finished_at/error сбрасываются в NULL.
    store.update_crawl_session_status("s1", CrawlSessionStatus.RUNNING)
    got = store.get_crawl_session("s1")
    assert got is not None
    assert got.status is CrawlSessionStatus.RUNNING
    assert got.finished_at is None
    assert got.error is None

    # Явно переданные значения при переходе в running сохраняются.
    store.update_crawl_session_status(
        "s1", CrawlSessionStatus.RUNNING, finished_at="t10", error="still"
    )
    got = store.get_crawl_session("s1")
    assert got is not None and got.finished_at == "t10" and got.error == "still"

    # Переход в running из failed-состояния тоже очищает error.
    store.update_crawl_session_status("s1", CrawlSessionStatus.FAILED, error="boom2")
    store.update_crawl_session_status("s1", CrawlSessionStatus.CREATED)
    got = store.get_crawl_session("s1")
    assert got is not None
    assert got.status is CrawlSessionStatus.CREATED
    assert got.finished_at is None and got.error is None

    # Нетерминальные/терминальные прочие статусы COALESCE — прежнее значение сохраняется.
    store.update_crawl_session_status("s1", CrawlSessionStatus.PAUSED, error="keep")
    store.update_crawl_session_status("s1", CrawlSessionStatus.PAUSED)
    got = store.get_crawl_session("s1")
    assert got is not None and got.error == "keep"


# --------------------------------------------------------------------------- #
# элементы + агрегаты
# --------------------------------------------------------------------------- #


def test_item_upsert_maintains_session_counters(store: SqliteStore) -> None:
    store.upsert_crawl_session(_session(status=CrawlSessionStatus.RUNNING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/a", CrawlItemStatus.PENDING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/b", CrawlItemStatus.RUNNING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/c", CrawlItemStatus.RUNNING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/d", CrawlItemStatus.FETCHED))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/e", CrawlItemStatus.FAILED))

    assert _counts(store) == {
        "pending": 1,
        "running": 2,
        "fetched": 1,
        "failed": 1,
        "cancelled": 0,
    }
    # Агрегаты сеанса (в одной транзакции с записью элемента) совпадают с поэлементным счётом.
    session = store.get_crawl_session("s1")
    assert session is not None
    assert (session.total, session.pending, session.running) == (5, 1, 2)
    assert (session.fetched, session.failed, session.cancelled) == (1, 1, 0)

    # Смена статуса элемента обновляет агрегат.
    store.upsert_crawl_session_item(_item("https://docs.test/p1/a", CrawlItemStatus.FETCHED))
    assert _counts(store) == {
        "pending": 0,
        "running": 2,
        "fetched": 2,
        "failed": 1,
        "cancelled": 0,
    }
    session = store.get_crawl_session("s1")
    assert session is not None and session.pending == 0 and session.fetched == 2


def test_item_round_trip_fields(store: SqliteStore) -> None:
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(
        _item(
            "https://docs.test/p1",
            CrawlItemStatus.FETCHED,
            attempts=2,
            engine="scrapy",
            kind="doc",
            fetch_mode="browser",
            extract_route="auto",
            strategy="hybrid",
            http_status=200,
            content_type="text/html",
            lang="en",
            content_hash="h1",
            raw_path="/tmp/h1.bin",
            first_seen="t0",
            last_attempt="t1",
        )
    )
    got = list(store.iter_crawl_session_items("s1"))
    assert len(got) == 1
    item = got[0]
    assert item.status is CrawlItemStatus.FETCHED
    assert item.attempts == 2
    assert item.engine == "scrapy"
    assert item.kind == "doc"
    assert item.fetch_mode == "browser"
    assert item.strategy == "hybrid"
    assert item.http_status == 200
    assert item.content_hash == "h1"
    assert item.raw_path == "/tmp/h1.bin"
    assert item.first_seen == "t0" and item.last_attempt == "t1"


def test_iter_items_order_and_filters(store: SqliteStore) -> None:
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(_item("https://docs.test/p2", CrawlItemStatus.FETCHED, product_id="p2"))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/b", CrawlItemStatus.FAILED))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/a", CrawlItemStatus.FETCHED))

    ordered = [(i.product_id, i.url) for i in store.iter_crawl_session_items("s1")]
    assert ordered == [
        ("p1", "https://docs.test/p1/a"),
        ("p1", "https://docs.test/p1/b"),
        ("p2", "https://docs.test/p2"),
    ]
    only_p1 = list(store.iter_crawl_session_items("s1", product_id="p1"))
    assert [i.url for i in only_p1] == [
        "https://docs.test/p1/a",
        "https://docs.test/p1/b",
    ]
    failed = list(store.iter_crawl_session_items("s1", status=CrawlItemStatus.FAILED))
    assert [i.url for i in failed] == ["https://docs.test/p1/b"]


def test_item_unknown_session_violates_fk(store: SqliteStore) -> None:
    # Элемент без сеанса не пишется: FK-нарушение, строка не остаётся (атомарность).
    with pytest.raises(sqlite3.IntegrityError):
        store.upsert_crawl_session_item(
            _item("https://docs.test/p1", CrawlItemStatus.PENDING, session_id="missing")
        )
    assert _counts(store, "missing") == {
        "pending": 0,
        "running": 0,
        "fetched": 0,
        "failed": 0,
        "cancelled": 0,
    }


def test_upsert_session_does_not_drop_items(store: SqliteStore) -> None:
    # ON CONFLICT DO UPDATE (не INSERT OR REPLACE): повторный upsert сеанса не удаляет
    # по FK ссылающиеся элементы.
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(_item("https://docs.test/p1", CrawlItemStatus.FETCHED))
    store.upsert_crawl_session(_session(status=CrawlSessionStatus.RUNNING))
    assert len(list(store.iter_crawl_session_items("s1"))) == 1


def test_upsert_session_does_not_reset_progress_counters(store: SqliteStore) -> None:
    """B2: ``ON CONFLICT DO UPDATE`` не трогает counter-колонки (владелец — элементы).

    Stale-модель сеанса (нулевые счётчики) при повторном upsert не должна обнулять
    агрегат, поддерживаемый :meth:`upsert_crawl_session_item`.
    """
    store.upsert_crawl_session(_session(status=CrawlSessionStatus.RUNNING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/a", CrawlItemStatus.FETCHED))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/b", CrawlItemStatus.FAILED))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/c", CrawlItemStatus.PENDING))

    # Повторный upsert (модель с нулевыми счётчиками) — ПОСЛЕДНЯЯ операция перед чтением.
    store.upsert_crawl_session(_session(status=CrawlSessionStatus.PARTIAL, finished_at="t9"))
    session = store.get_crawl_session("s1")
    assert session is not None
    # Счётчики сохранились (обновляются только элементы/recovery)...
    assert (session.total, session.pending, session.fetched, session.failed) == (3, 1, 1, 1)
    # ...а метаданные обновились.
    assert session.status is CrawlSessionStatus.PARTIAL
    assert session.finished_at == "t9"


# --------------------------------------------------------------------------- #
# recovery / clear / идемпотентность
# --------------------------------------------------------------------------- #


def test_recover_crawl_session_running_to_pending(store: SqliteStore) -> None:
    store.upsert_crawl_session(_session(status=CrawlSessionStatus.RUNNING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/a", CrawlItemStatus.RUNNING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/b", CrawlItemStatus.RUNNING))
    store.upsert_crawl_session_item(_item("https://docs.test/p1/c", CrawlItemStatus.FETCHED))

    recovered = store.recover_crawl_session("s1")
    assert recovered == 2
    assert _counts(store) == {
        "pending": 2,
        "running": 0,
        "fetched": 1,
        "failed": 0,
        "cancelled": 0,
    }
    # Счётчики сеанса пересчитаны в той же транзакции.
    session = store.get_crawl_session("s1")
    assert session is not None
    assert (session.total, session.pending, session.running) == (3, 2, 0)
    assert session.fetched == 1

    # Идемпотентность: повторный recovery не находит running.
    assert store.recover_crawl_session("s1") == 0


def test_recover_unknown_session_returns_zero(store: SqliteStore) -> None:
    assert store.recover_crawl_session("missing") == 0


def test_clear_crawl_session(store: SqliteStore) -> None:
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(_item("https://docs.test/p1", CrawlItemStatus.FETCHED))
    store.upsert_crawl_session_item(_item("https://docs.test/p2", CrawlItemStatus.FAILED, product_id="p2"))

    store.clear_crawl_session("s1")
    assert store.get_crawl_session("s1") is None
    assert list(store.iter_crawl_session_items("s1")) == []
    assert _counts(store) == {
        "pending": 0,
        "running": 0,
        "fetched": 0,
        "failed": 0,
        "cancelled": 0,
    }


def test_set_crawl_session_provenance_targeted_update(store: SqliteStore) -> None:
    """Phase 2: ``set_crawl_session_provenance`` пишет только колонку провенанса."""
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(
        _item("https://docs.test/p1", CrawlItemStatus.FETCHED)
    )
    before = store.get_crawl_session("s1")
    assert before is not None

    store.set_crawl_session_provenance("s1", '{"engines": ["http"]}')

    after = store.get_crawl_session("s1")
    assert after is not None
    assert after.provenance_json == '{"engines": ["http"]}'
    # Счётчики/статус/метаданные не затронуты (targeted UPDATE).
    assert after.total == before.total
    assert after.fetched == before.fetched
    assert after.status is before.status


def test_idempotent_double_run(store: SqliteStore) -> None:
    # Повторный upsert сеанса и элементов не дублирует строки и не портит агрегаты.
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(_item("https://docs.test/p1", CrawlItemStatus.PENDING))
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(_item("https://docs.test/p1", CrawlItemStatus.PENDING))

    assert len(list(store.iter_crawl_session_items("s1"))) == 1
    assert _counts(store)["pending"] == 1
    session = store.get_crawl_session("s1")
    assert session is not None and session.total == 1


def test_crawl_item_depth_round_trip(store: SqliteStore) -> None:
    """D1: глубина BFS-frontier персистится и читается (best-effort resume)."""
    store.upsert_crawl_session(_session())
    store.upsert_crawl_session_item(
        _item("https://docs.test/p1", CrawlItemStatus.FETCHED, depth=2)
    )
    store.upsert_crawl_session_item(
        _item("https://docs.test/p2", CrawlItemStatus.PENDING, product_id="p2")
    )
    items = {(i.product_id, i.url): i for i in store.iter_crawl_session_items("s1")}
    assert items[("p1", "https://docs.test/p1")].depth == 2
    assert items[("p2", "https://docs.test/p2")].depth is None

