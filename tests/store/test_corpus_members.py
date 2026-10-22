"""``corpus_members`` / ``archive_observations``: round-trip и идемпотентность (B3).

Идентичность члена — ``(archive_content_hash, normalized_member_path)`` (§6.7): повторный
спуск того же архива **не** дублирует строки, а обновляет исход. Наблюдение спуска —
одно на ``observation_id`` (детерминированный отпечаток прогона/корня/архива/глубины).
"""

from __future__ import annotations

from core.domain import (
    ArchiveDescent,
    ArchiveDescentStatus,
    ArchiveObservation,
    CorpusMember,
)
from core.store.sqlite import SCHEMA_VERSION, connect
from core.store.sqlite_store import SqliteStore

ARCHIVE_HASH = "a" * 64


def _member(**overrides: object) -> CorpusMember:
    payload: dict[str, object] = {
        "archive_content_hash": ARCHIVE_HASH,
        "member_path": "docs/manual.pdf",
        "product_id": "p1",
        "run_id": "run-1",
        "root_url": "https://docs.test/bundle.zip",
        "archive_url": "https://docs.test/bundle.zip",
        "archive_path": "corpus_raw/p1/aaaa.bin",
        "member_url": "https://docs.test/bundle.zip!/docs/manual.pdf",
        "member_format": "pdf",
        "declared_size": 1234,
        "extracted_content_hash": "b" * 64,
        "doc_id": "p1:" + "b" * 64,
        "skip_reason": None,
        "depth": 0,
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    payload.update(overrides)
    return CorpusMember(**payload)


def _observation(**overrides: object) -> ArchiveObservation:
    payload: dict[str, object] = {
        "observation_id": "ao_deadbeef",
        "run_id": "run-1",
        "product_id": "p1",
        "root_url": "https://docs.test/bundle.zip",
        "archive_url": "https://docs.test/bundle.zip",
        "archive_content_hash": ARCHIVE_HASH,
        "archive_path": "corpus_raw/p1/aaaa.bin",
        "depth": 0,
        "descent": ArchiveDescent.FULL,
        "status": ArchiveDescentStatus.DESCENDED,
        "forecast": {"members_total": 2, "members_accepted": 1, "formats": {".pdf": 1}},
        "outcome": {"members_ingested": 1, "skipped": {"not_ingestible_format": 1}},
        "reason": None,
        "created_at": "2026-01-01T00:00:00+00:00",
    }
    payload.update(overrides)
    return ArchiveObservation(**payload)


def test_schema_version_is_additive_bump() -> None:
    """Новые таблицы аддитивны: ``CREATE TABLE IF NOT EXISTS`` + бамп версии схемы."""
    assert SCHEMA_VERSION == "3.10.0"
    conn = connect(":memory:")
    try:
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {"corpus_members", "archive_observations"} <= tables
    finally:
        conn.close()


def test_corpus_members_round_trip() -> None:
    store = SqliteStore(":memory:")
    try:
        member = _member()
        store.save_corpus_members([member])
        rows = list(store.iter_corpus_members())
        assert rows == [member]
        assert list(store.iter_corpus_members(archive_content_hash=ARCHIVE_HASH)) == [
            member
        ]
        assert list(store.iter_corpus_members(product_id="other")) == []
    finally:
        store.close()


def test_corpus_members_idempotent_on_redescent() -> None:
    """Тот же архив, тот же путь члена → одна строка; меняется только исход."""
    store = SqliteStore(":memory:")
    try:
        first = _member(skip_reason="extract_failed", doc_id=None,
                        extracted_content_hash=None)
        store.save_corpus_members([first])
        second = _member()
        store.save_corpus_members([second])
        rows = list(store.iter_corpus_members())
        assert len(rows) == 1
        assert rows[0].doc_id == second.doc_id
        assert rows[0].skip_reason is None
        assert rows[0].extracted_content_hash == second.extracted_content_hash
    finally:
        store.close()


def test_corpus_members_distinguish_paths_and_archives() -> None:
    """Идентичность — пара: другой путь или другой архив — другая строка."""
    store = SqliteStore(":memory:")
    try:
        store.save_corpus_members(
            [
                _member(),
                _member(member_path="docs/other.pdf"),
                _member(archive_content_hash="c" * 64),
            ]
        )
        assert len(list(store.iter_corpus_members())) == 3
        assert len(
            list(store.iter_corpus_members(archive_content_hash=ARCHIVE_HASH))
        ) == 2
    finally:
        store.close()


def test_archive_observation_round_trip_and_upsert() -> None:
    store = SqliteStore(":memory:")
    try:
        store.save_archive_observation(_observation())
        rows = list(store.iter_archive_observations("run-1"))
        assert len(rows) == 1
        assert rows[0].status is ArchiveDescentStatus.DESCENDED
        assert rows[0].descent is ArchiveDescent.FULL
        assert rows[0].forecast == {
            "members_total": 2,
            "members_accepted": 1,
            "formats": {".pdf": 1},
        }

        # Повторный спуск перекрывает запись, а не плодит вторую (id детерминирован).
        store.save_archive_observation(
            _observation(
                status=ArchiveDescentStatus.BUDGET_EXHAUSTED,
                reason="archive_budget_exhausted",
                outcome={"members_ingested": 0, "stopped_reason": "archive_budget_exhausted"},
            )
        )
        rows = list(store.iter_archive_observations("run-1"))
        assert len(rows) == 1
        assert rows[0].status is ArchiveDescentStatus.BUDGET_EXHAUSTED
        assert rows[0].reason == "archive_budget_exhausted"
        assert rows[0].outcome["members_ingested"] == 0
        # Чужой прогон не видит наблюдений этого.
        assert list(store.iter_archive_observations("run-2")) == []
    finally:
        store.close()
