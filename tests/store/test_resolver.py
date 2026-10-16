"""Тесты ``SqliteStore`` как ``SourceResolver`` (W3/T3.4) и нагрузочный тест 10k (W3/T3.2).

Резолв детерминирован и без сети (I10/I15); ``verify_evidence_hash`` проверяет ``content_hash``
(I19). Все тесты работают на ``:memory:``-БД. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import time
from collections.abc import Iterator

import pytest

from core.domain import (
    Cell,
    CellEvidence,
    CellState,
    EvidenceType,
    Focus,
    ResolvedSource,
    RunConfig,
    RunMode,
    SourceKind,
    SourceScope,
)
from core.domain.anchors import verify_evidence_hash
from core.store.sqlite_store import SqliteStore
from core.util.ids import source_id

RUN_ID = "run-1"
_URL = "https://example.test/page#a"
_HASH = "hash-abc"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        yield s


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1"],
        package="pkg-1",
        focus=Focus(modules=[], params=[]),
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


def _insert_doc(store: SqliteStore, doc_id: str, product_id: str) -> None:
    with store.conn:
        store.conn.execute(
            "INSERT INTO corpus_docs (doc_id, product_id, url) VALUES (?, ?, ?)",
            (doc_id, product_id, "https://example.test/doc"),
        )


def _insert_chunk(
    store: SqliteStore,
    *,
    chunk_id: str,
    doc_id: str,
    product_id: str,
    content_hash: str = _HASH,
    url: str = _URL,
    chunk_no: int = 0,
    build_id: str | None = "build-1",
) -> None:
    _insert_doc(store, doc_id, product_id)
    with store.conn:
        store.conn.execute(
            "INSERT INTO corpus_chunks"
            " (chunk_id, doc_id, product_id, url, heading_path, chunk_no, text,"
            "  context_prefix, content_hash, build_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                chunk_id,
                doc_id,
                product_id,
                url,
                "H1",
                chunk_no,
                "body text",
                "prefix",
                content_hash,
                build_id,
            ),
        )


# --------------------------------------------------------------------------- #
# T3.4 — resolve / SourceResolver
# --------------------------------------------------------------------------- #


def test_resolve_raw_chunk_id(store: SqliteStore) -> None:
    _insert_chunk(store, chunk_id="chunk-1", doc_id="doc-1", product_id="p1")
    resolved = store.resolve("chunk-1")
    assert isinstance(resolved, ResolvedSource)
    assert resolved.chunk_id == "chunk-1"
    assert resolved.source_url == _URL
    assert resolved.content_hash == _HASH
    assert resolved.build_id == "build-1"


def test_resolve_formatted_source_id(store: SqliteStore) -> None:
    # chunk_id намеренно не совпадает с source_id: резолв идёт по (product, chunk_no, build).
    _insert_chunk(
        store,
        chunk_id="p1#0",
        doc_id="doc-1",
        product_id="p1",
        chunk_no=0,
        build_id="build-1",
    )
    sid = source_id("p1", "doc-1", 0, "build-1")
    resolved = store.resolve(sid)
    assert resolved is not None
    assert resolved.chunk_id == "p1#0"
    assert resolved.source_url == _URL
    assert resolved.content_hash == _HASH
    assert resolved.build_id == "build-1"


def test_resolve_formatted_without_build_id(store: SqliteStore) -> None:
    _insert_chunk(
        store,
        chunk_id="p1#3",
        doc_id="doc-1",
        product_id="p1",
        chunk_no=3,
        build_id="build-1",
    )
    resolved = store.resolve(source_id("p1", "doc-1", 3))
    assert resolved is not None
    assert resolved.chunk_id == "p1#3"


def test_resolve_unknown_and_malformed_returns_none(store: SqliteStore) -> None:
    assert store.resolve("does-not-exist") is None          # нет ':' → ValueError внутри
    assert store.resolve("") is None                        # пустой → ValueError
    assert store.resolve("a:b:c:d:e") is None               # 5 сегментов → ValueError
    assert store.resolve("p9:doc-9:3") is None              # валидный формат, но нет строки


def test_get_chunk_and_resolve_agree_on_row(store: SqliteStore) -> None:
    _insert_chunk(store, chunk_id="chunk-9", doc_id="doc-9", product_id="p1")
    chunk = store.get_chunk("chunk-9")
    resolved = store.resolve("chunk-9")
    assert chunk is not None and resolved is not None
    assert chunk.chunk_id == resolved.chunk_id
    assert chunk.content_hash == resolved.content_hash


def test_resolve_source_url_still_works(store: SqliteStore) -> None:
    _insert_chunk(store, chunk_id="chunk-1", doc_id="doc-1", product_id="p1")
    assert store.resolve_source_url("chunk-1") == _URL
    assert store.resolve_source_url(source_id("p1", "doc-1", 0, "build-1")) == _URL
    assert store.resolve_source_url("missing") is None


def test_i19_verify_evidence_hash_end_to_end(store: SqliteStore) -> None:
    _insert_chunk(store, chunk_id="chunk-1", doc_id="doc-1", product_id="p1", content_hash=_HASH)
    ev = CellEvidence(
        evidence_id="ev-1",
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.a",
        source_kind=SourceKind.MANUAL,
        evidence_type=EvidenceType.DIRECT,
        source_id="chunk-1",
        content_hash=_HASH,
        source_scope=SourceScope.PARTIAL,
    )
    assert verify_evidence_hash(ev, store) is True

    tampered = ev.model_copy(update={"content_hash": "hash-flipped"})
    assert verify_evidence_hash(tampered, store) is False

    # Нерезолвящийся source_id тоже не проходит (I19).
    unknown = ev.model_copy(update={"source_id": "no:such:chunk"})
    assert verify_evidence_hash(unknown, store) is False


# --------------------------------------------------------------------------- #
# T3.2 — нагрузочный тест 10k ячеек (DoD)
# --------------------------------------------------------------------------- #


def test_save_cells_batch_10k_within_bound_and_reads_all() -> None:
    with SqliteStore(":memory:") as store:
        store.create_run(RUN_ID, _cfg())
        cells = [
            Cell(run_id=RUN_ID, product_id="p1", param_id=f"param.{i}", state=CellState.PENDING)
            for i in range(10_000)
        ]
        start = time.perf_counter()
        store.save_cells_batch(cells)  # chunk_size по умолчанию (200) → 50 транзакций
        elapsed = time.perf_counter() - start
        assert elapsed < 5.0, f"10k ячеек записаны за {elapsed:.3f}s (порог 5s)"
        assert sum(1 for _ in store.iter_cells(RUN_ID)) == 10_000
