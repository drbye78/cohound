"""Тесты оркестрации сеанса краула (L1a): ``core.pipeline.crawl_session``.

Хелперы изолированно проверяются на ``SqliteStore(":memory:")`` с созданной строкой
прогона (FK ``crawl_sessions.run_id``). Нейтральные фикстуры: ``p1``, ``run-fixture``.
"""

from __future__ import annotations

import datetime as _dt

import pytest

from core.domain import (
    CrawlItemStatus,
    CrawlSessionItem,
    CrawlSessionStatus,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline.crawl_session import (
    default_session_id,
    finalize_session,
    open_session,
    prior_items,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-fixture"
SESSION_ID = default_session_id(RUN_ID)


def _store_with_run() -> SqliteStore:
    """``SqliteStore`` с созданной строкой прогона (нужна для FK сеанса)."""
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


def _item(
    product_id: str, url: str, status: CrawlItemStatus, **overrides: object
) -> CrawlSessionItem:
    base: dict[str, object] = {
        "session_id": SESSION_ID,
        "product_id": product_id,
        "url": url,
        "status": status,
    }
    base.update(overrides)
    return CrawlSessionItem(**base)


def test_default_session_id() -> None:
    assert default_session_id("run-x") == "crawl:run-x"


def test_open_session_creates_then_is_idempotent() -> None:
    with _store_with_run() as store:
        first = open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        assert first.session_id == SESSION_ID
        assert first.run_id == RUN_ID
        # C1: открытие сеанса = начало исполнения → ``RUNNING`` + ``started_at``.
        assert first.status is CrawlSessionStatus.RUNNING
        assert first.started_at

        # Повторный открытый вызов не создаёт новый сеанс (идемпотентность на прогон).
        second = open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        assert second.session_id == SESSION_ID
        assert second.status is CrawlSessionStatus.RUNNING
        rows = store.conn.execute("SELECT COUNT(*) AS n FROM crawl_sessions").fetchone()
        assert rows["n"] == 1


def test_open_session_resumes_terminal_session_to_running() -> None:
    """C1: переоткрытие терминального сеанса для resume → снова ``RUNNING``, ``finished_at`` снят."""
    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/a", CrawlItemStatus.FETCHED)
        )
        finalize_session(store, SESSION_ID, finished_at="2026-01-02T00:00:00+00:00")
        done = store.get_crawl_session(SESSION_ID)
        assert done is not None and done.status is CrawlSessionStatus.COMPLETED
        assert done.finished_at == "2026-01-02T00:00:00+00:00"

        # Resume — новый запуск того же сеанса: снова RUNNING, признаки завершения сброшены.
        resumed = open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        assert resumed.status is CrawlSessionStatus.RUNNING
        assert resumed.finished_at is None
        assert resumed.started_at


def test_open_session_recovers_running_items_to_pending() -> None:
    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/a", CrawlItemStatus.RUNNING)
        )
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/b", CrawlItemStatus.FETCHED)
        )

        # recovery-on-open: running → pending, счётчики пересчитываются.
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        by_url = {
            item.url: item for item in store.iter_crawl_session_items(SESSION_ID)
        }
        assert by_url["https://docs.test/a"].status is CrawlItemStatus.PENDING
        assert by_url["https://docs.test/b"].status is CrawlItemStatus.FETCHED
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert (session.pending, session.fetched, session.total) == (1, 1, 2)


def test_finalize_completed_when_no_defects() -> None:
    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/a", CrawlItemStatus.FETCHED)
        )
        status = finalize_session(store, SESSION_ID, finished_at="2026-01-02T00:00:00+00:00")
        assert status is CrawlSessionStatus.COMPLETED
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.COMPLETED
        assert session.finished_at == "2026-01-02T00:00:00+00:00"


def test_finalize_partial_when_any_failed_or_cancelled() -> None:
    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/a", CrawlItemStatus.FETCHED)
        )
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/b", CrawlItemStatus.FAILED, error_kind="parser_failed")
        )
        assert finalize_session(store, SESSION_ID, finished_at="t") is CrawlSessionStatus.PARTIAL

    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/a", CrawlItemStatus.CANCELLED)
        )
        assert finalize_session(store, SESSION_ID, finished_at="t") is CrawlSessionStatus.PARTIAL


def test_finalize_provenance_written_on_finalize() -> None:
    """Phase 2: ``finalize_session`` пишет компактный провенанс в ``provenance_json``."""
    import json

    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/a", CrawlItemStatus.FETCHED)
        )
        finalize_session(
            store,
            SESSION_ID,
            finished_at="t",
            engines=["scrapy", "http", "http"],
            kinds=["web"],
            crawl_sources_digest="digest-1",
        )
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        payload = json.loads(session.provenance_json or "{}")
        assert payload["engines"] == ["http", "scrapy"]  # sorted + dedup
        assert payload["kinds"] == ["web"]
        assert payload["crawl_sources_digest"] == "digest-1"
        assert payload["status"] == CrawlSessionStatus.COMPLETED.value


def test_prior_items_keyed_by_product_and_url() -> None:
    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        # Один и тот же URL под разными продуктами — независимые элементы (I17/§4.4).
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/shared", CrawlItemStatus.FETCHED)
        )
        store.upsert_crawl_session_item(
            _item("p2", "https://docs.test/shared", CrawlItemStatus.FAILED)
        )
        mapping = prior_items(store, SESSION_ID)
        assert set(mapping) == {("p1", "https://docs.test/shared"), ("p2", "https://docs.test/shared")}
        assert mapping[("p1", "https://docs.test/shared")].status is CrawlItemStatus.FETCHED
        assert mapping[("p2", "https://docs.test/shared")].status is CrawlItemStatus.FAILED


def test_prior_items_empty_for_unknown_session() -> None:
    with _store_with_run() as store:
        assert prior_items(store, "crawl:unknown") == {}


def test_open_session_only_touches_items() -> None:
    """Reopen без running — no-op по статусам, счётчики не портятся (идемпотентность)."""
    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(
            _item("p1", "https://docs.test/a", CrawlItemStatus.FAILED, error_kind="source_unavailable")
        )
        first = store.get_crawl_session(SESSION_ID)
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        second = store.get_crawl_session(SESSION_ID)
        assert first is not None and second is not None
        assert second.failed == first.failed == 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
