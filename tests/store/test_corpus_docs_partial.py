"""Персистенция частичного извлечения: ``corpus_docs`` и блок манифеста (I2/I2c).

Признак обязан пережить запись и чтение обратно: если он теряется в БД, отчёт и
покрытие снова начинают видеть «прочитано полностью» там, где прочитана часть.
Тесты нейтральны (§1.3), работают на ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import CorpusDoc, Focus, RankingPolicy, RunConfig, RunManifest, RunMode
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-partial"

@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as instance:
        instance.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1", "p2"],
                package="pkg",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        yield instance

def _doc(doc_id: str, **overrides: object) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": "p1",
        "url": f"https://docs.test/p1/{doc_id}",
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]

def test_corpus_doc_partial_roundtrips(store: SqliteStore) -> None:
    """Частичность, причина и числа страниц/символов читаются обратно как записаны."""
    doc = _doc(
        "p1:partial",
        partial=True,
        partial_reason="docling: PARTIAL_SUCCESS, получено страниц 7 из 2763",
        page_count=2763,
        pages_converted=7,
        extracted_chars=79_810,
    )
    store.upsert_corpus_doc(doc)

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:partial"] == doc
    assert read["p1:partial"].pages_converted == 7
    assert read["p1:partial"].extracted_chars == 79_810

def test_corpus_doc_full_read_roundtrips_without_partial_marks(
    store: SqliteStore,
) -> None:
    """Полный документ не обрастает признаками: поведение прежних строк сохранено."""
    doc = _doc("p1:full")
    store.upsert_corpus_doc(doc)

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}["p1:full"]
    assert read.partial is False
    assert read.partial_reason is None
    assert read.pages_converted is None
    # В БД — 0, а не NULL: читатель не должен различать «старая строка» и «полная».
    row = store.conn.execute(
        "SELECT partial, partial_reason FROM corpus_docs WHERE doc_id = ?",
        ("p1:full",),
    ).fetchone()
    assert row["partial"] == 0
    assert row["partial_reason"] is None

def test_corpus_docs_partial_columns_migrate_additively(tmp_path: Path) -> None:
    """Старая ``corpus_docs`` без колонок получает их аддитивно (``schema_version`` цел)."""
    path = tmp_path / "legacy.db"
    legacy = sqlite3.connect(path)
    legacy.execute(
        "CREATE TABLE corpus_docs ("
        " doc_id TEXT PRIMARY KEY, product_id TEXT NOT NULL, url TEXT NOT NULL,"
        " fetched_at TEXT, content_hash TEXT, raw_path TEXT,"
        " extractor_version TEXT, render_mode TEXT, lang TEXT)"
    )
    legacy.execute(
        "INSERT INTO corpus_docs (doc_id, product_id, url)"
        " VALUES ('p1:old', 'p1', 'https://docs.test/p1/old')"
    )
    legacy.commit()
    legacy.close()

    store = SqliteStore(path)
    try:
        cols = {r["name"] for r in store.conn.execute("PRAGMA table_info(corpus_docs)")}
        assert {
            "partial",
            "partial_reason",
            "page_count",
            "pages_converted",
            "extracted_chars",
        } <= cols
    finally:
        store.close()

    # Старая строка читается как «полная»: выдумывать частичность из отсутствия
    # колонки нельзя, а падать на исторической БД — тем более.
    store = SqliteStore(path)
    try:
        store.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1"],
                package="pkg",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
        assert read["p1:old"].partial is False
        assert read["p1:old"].partial_reason is None
    finally:
        store.close()

def test_manifest_partial_docs_block_roundtrips(store: SqliteStore) -> None:
    """Блок ``partial_docs`` манифеста переживает ``persist_manifest``→``read_manifest``."""
    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="0.1.0",
        package_version="pkg",
        catalog_version="cat",
        status_dict_version="sd",
        scoring_policy_version="sp",
        anchor_product_id="p1",
        partial_docs={
            "documents": 1,
            "by_product": {"p1": 1, "p2": 0},
            "pages_converted": 7,
            "pages_declared": 2763,
            "chars": 79_810,
        },
    )
    store.persist_manifest(RUN_ID, manifest)

    read = store.read_manifest(RUN_ID)
    assert read.partial_docs == manifest.partial_docs

def test_manifest_without_partial_docs_reads_empty_block(store: SqliteStore) -> None:
    """Прогон без частичных документов: блок пуст, а не выдуман нулями."""
    manifest = RunManifest(
        run_id=RUN_ID,
        case_id="case",
        created_at="2026-01-01T00:00:00Z",
        mode=RunMode.STRICT,
        methodology_version="0.1.0",
        package_version="pkg",
        catalog_version="cat",
        status_dict_version="sd",
        scoring_policy_version="sp",
        anchor_product_id="p1",
    )
    store.persist_manifest(RUN_ID, manifest)

    assert store.read_manifest(RUN_ID).partial_docs == {}
