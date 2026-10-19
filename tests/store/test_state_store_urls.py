"""Тесты URL/document/chunk-поверхности ``SqliteStateStore`` (09 §4.2/§6.3).

Офлайн: ``:memory:``-БД, без сети. Проверяются идемпотентность upsert'ов, порядок
``iter_url_state``, поиск версии по ключу §4.2, атомарность ``replace_doc_chunks``
(включая общий текст между документами) и round-trip текста чанка.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from collections.abc import Iterator

import pytest

from core.ports.state import ChunkRow, DocumentVersionRow, UrlStateRow
from core.store.state_store import SqliteStateStore


@pytest.fixture()
def store() -> Iterator[SqliteStateStore]:
    with SqliteStateStore(":memory:") as state:
        yield state


def _url(product_id: str = "p1", normalized_url: str = "https://p1.test/a", **kw: object) -> UrlStateRow:
    base: dict[str, object] = {
        "product_id": product_id,
        "url": normalized_url,
        "normalized_url": normalized_url,
    }
    base.update(kw)
    return UrlStateRow(**base)  # type: ignore[arg-type]


def _doc(
    *,
    doc_version_id: str = "dv_1",
    product_id: str = "p1",
    content_hash: str = "h1",
    extractor_version: str = "ext-1",
    level: str = "html",
    status: str = "active",
    url: str = "https://p1.test/a",
) -> DocumentVersionRow:
    return DocumentVersionRow(
        doc_version_id=doc_version_id,
        doc_id=f"{product_id}:{content_hash}",
        product_id=product_id,
        url=url,
        content_hash=content_hash,
        extractor_version=extractor_version,
        level=level,
        first_seen_run_id="run-1",
        last_seen_run_id="run-1",
        first_seen_at="2026-01-01T00:00:00Z",
        last_seen_at="2026-01-01T00:00:00Z",
        status=status,
    )


def _chunk(
    *,
    chunk_id: str,
    doc_version_id: str = "dv_1",
    product_id: str = "p1",
    chunk_no: int = 0,
    content_hash: str = "ch1",
    chunker_version: str = "chk-1",
) -> ChunkRow:
    return ChunkRow(
        chunk_id=chunk_id,
        doc_version_id=doc_version_id,
        product_id=product_id,
        chunk_no=chunk_no,
        content_hash=content_hash,
        chunker_version=chunker_version,
    )


def test_url_state_upsert_is_idempotent(store: SqliteStateStore) -> None:
    store.upsert_url_state(
        _url(etag='"v1"', last_http_status=200, doc_version_id="dv_1")
    )
    store.upsert_url_state(
        _url(
            etag='"v2"',
            last_http_status=304,
            doc_version_id="dv_1",
            consecutive_failures=2,
            last_error_kind="timeout",
            reuse_kind="revalidated",
            seed_id="seed-1",
        )
    )

    rows = store.iter_url_state("p1")
    assert len(rows) == 1
    row = rows[0]
    assert row.etag == '"v2"'
    assert row.last_http_status == 304
    assert row.consecutive_failures == 2
    assert row.last_error_kind == "timeout"
    assert row.reuse_kind == "revalidated"
    assert row.seed_id == "seed-1"


def test_get_url_state_scoped_by_product(store: SqliteStateStore) -> None:
    store.upsert_url_state(_url("p1", "https://shared.test/a", etag='"p1"'))
    store.upsert_url_state(_url("p2", "https://shared.test/a", etag='"p2"'))

    assert store.get_url_state("p1", "https://shared.test/a").etag == '"p1"'  # type: ignore[union-attr]
    assert store.get_url_state("p2", "https://shared.test/a").etag == '"p2"'  # type: ignore[union-attr]
    assert store.get_url_state("p3", "https://shared.test/a") is None


def test_iter_url_state_is_sorted_by_normalized_url(store: SqliteStateStore) -> None:
    for path in ("c", "a", "b"):
        store.upsert_url_state(_url(normalized_url=f"https://p1.test/{path}"))

    assert [row.normalized_url for row in store.iter_url_state("p1")] == [
        "https://p1.test/a",
        "https://p1.test/b",
        "https://p1.test/c",
    ]
    assert store.iter_url_state("p2") == []


def test_document_version_find_by_unique_key(store: SqliteStateStore) -> None:
    store.upsert_document_version(_doc())

    found = store.find_document_version(
        "p1", content_hash="h1", extractor_version="ext-1", level="html"
    )
    assert found is not None
    assert found.doc_version_id == "dv_1"
    assert store.get_document_version("dv_1") == found

    # Любой элемент ключа §4.2 различает версии.
    assert (
        store.find_document_version(
            "p1", content_hash="h1", extractor_version="ext-2", level="html"
        )
        is None
    )
    assert (
        store.find_document_version(
            "p1", content_hash="h1", extractor_version="ext-1", level="pdf"
        )
        is None
    )
    assert (
        store.find_document_version(
            "p2", content_hash="h1", extractor_version="ext-1", level="html"
        )
        is None
    )


def test_upsert_document_version_updates_mutable_metadata(
    store: SqliteStateStore,
) -> None:
    store.upsert_document_version(_doc())
    store.upsert_document_version(
        dataclasses.replace(
            _doc(),
            last_seen_run_id="run-2",
            last_seen_at="2026-02-01T00:00:00Z",
            chunker_version="chk-9",
            status="superseded",
        )
    )

    row = store.get_document_version("dv_1")
    assert row is not None
    assert row.last_seen_run_id == "run-2"
    assert row.chunker_version == "chk-9"
    assert row.status == "superseded"
    assert len(store.iter_document_versions("p1", status="superseded")) == 1
    assert store.iter_document_versions("p1") == []


def test_replace_doc_chunks_is_atomic_and_preserves_shared_text(
    store: SqliteStateStore,
) -> None:
    store.upsert_document_version(_doc(doc_version_id="dv_1", content_hash="h1"))
    store.upsert_document_version(_doc(doc_version_id="dv_2", content_hash="h2"))
    for content_hash, text in (("old", "old text"), ("shared", "shared text"), ("new", "new text")):
        store.put_chunk_text(content_hash, text)

    store.replace_doc_chunks(
        "dv_1",
        [
            _chunk(chunk_id="c1", doc_version_id="dv_1", chunk_no=0, content_hash="old"),
            _chunk(chunk_id="c2", doc_version_id="dv_1", chunk_no=1, content_hash="shared"),
        ],
    )
    # Второй документ тоже ссылается на общий текст.
    store.replace_doc_chunks(
        "dv_2",
        [_chunk(chunk_id="c3", doc_version_id="dv_2", chunk_no=0, content_hash="shared")],
    )

    store.replace_doc_chunks(
        "dv_1",
        [_chunk(chunk_id="c4", doc_version_id="dv_1", chunk_no=0, content_hash="new")],
    )

    assert [c.chunk_id for c in store.iter_doc_chunks("dv_1")] == ["c4"]
    assert [c.chunk_id for c in store.iter_doc_chunks("dv_2")] == ["c3"]
    # Осиротевший текст удалён, общий — сохранён.
    assert store.get_chunk_text("old") is None
    assert store.get_chunk_text("shared") == "shared text"
    assert store.get_chunk_text("new") == "new text"


def test_replace_doc_chunks_rolls_back_on_fk_violation(
    store: SqliteStateStore,
) -> None:
    store.upsert_document_version(_doc(doc_version_id="dv_1"))
    store.put_chunk_text("ch1", "text")
    store.replace_doc_chunks(
        "dv_1", [_chunk(chunk_id="c1", doc_version_id="dv_1", content_hash="ch1")]
    )

    with pytest.raises(sqlite3.IntegrityError):
        store.replace_doc_chunks(
            "dv_1",
            [
                _chunk(
                    chunk_id="c2",
                    doc_version_id="dv_1",
                    content_hash="missing-text",
                )
            ],
        )

    # Транзакция откатилась: старый набор и текст на месте.
    assert [c.chunk_id for c in store.iter_doc_chunks("dv_1")] == ["c1"]
    assert store.get_chunk_text("ch1") == "text"


def test_replace_doc_chunks_rejects_foreign_doc_rows(
    store: SqliteStateStore,
) -> None:
    store.upsert_document_version(_doc(doc_version_id="dv_1"))
    store.upsert_document_version(_doc(doc_version_id="dv_2", content_hash="h2"))
    store.put_chunk_text("ch1", "text")
    store.replace_doc_chunks(
        "dv_1", [_chunk(chunk_id="c1", doc_version_id="dv_1", content_hash="ch1")]
    )

    with pytest.raises(ValueError):
        store.replace_doc_chunks(
            "dv_2",
            [_chunk(chunk_id="c2", doc_version_id="dv_1", content_hash="ch1")],
        )

    assert [c.chunk_id for c in store.iter_doc_chunks("dv_1")] == ["c1"]
    assert store.iter_doc_chunks("dv_2") == []


def test_chunk_text_round_trip_and_immutability(store: SqliteStateStore) -> None:
    assert store.get_chunk_text("h") is None
    store.put_chunk_text("h", "первый текст", lang="ru")
    store.put_chunk_text("h", "второй текст", lang="en")

    assert store.get_chunk_text("h") == "первый текст"
    row = store.conn.execute(
        "SELECT lang FROM chunk_store WHERE content_hash = 'h'"
    ).fetchone()
    assert row["lang"] == "en"
