"""Тесты ``SqliteStore`` как ``SourceResolver`` (W3/T3.4) и нагрузочный тест 10k (W3/T3.2).

Резолв детерминирован и без сети (I10/I15); ``verify_evidence_hash`` проверяет ``content_hash``
(I19). Все тесты работают на ``:memory:``-БД. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path

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


def _insert_version(
    store: SqliteStore, doc_version_id: str, product_id: str, doc_id: str
) -> None:
    """Строка отображения версии в документ (16 §6) — носитель ``dv`` → ``doc_id``."""
    with store.conn:
        store.conn.execute(
            "INSERT INTO corpus_doc_version (doc_version_id, product_id, doc_id)"
            " VALUES (?, ?, ?)",
            (doc_version_id, product_id, doc_id),
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


def test_resolve_fallback_finds_chunk_by_product_prefixed_doc_id(
    store: SqliteStore,
) -> None:
    """Фолбэк по ``source_id`` **находит** чанк: ``doc_id`` в корпусе — ``product_id:hash``.

    16 §6.3: прежний фолбэк искал по голому хешу там, где лежит ``product_id:hash``, и не
    срабатывал ни разу — мнимая страховка. Здесь ``chunk_id`` намеренно **не** совпадает с
    ``source_id`` (новый формат ``product:doc_version_id:chunk_no``), поэтому прямой lookup
    промахивается, и чанк находится только вторым путём.
    """
    _insert_chunk(
        store,
        chunk_id="p1:dv_c0ffee:0",
        doc_id="p1:hash-abc",
        product_id="p1",
        chunk_no=0,
    )
    resolved = store.resolve(source_id("p1", "hash-abc", 0))
    assert resolved is not None, "фолбэк по (product_id, doc_id) обязан сработать"
    assert resolved.chunk_id == "p1:dv_c0ffee:0"
    assert resolved.source_url == _URL


def test_resolve_fallback_respects_build_id(store: SqliteStore) -> None:
    """Фолбэк не теряет ``build_id``: чанк другой сборки по тому же адресу не подставляется."""
    _insert_chunk(
        store,
        chunk_id="p1:dv_a:1",
        doc_id="p1:hash-a",
        product_id="p1",
        chunk_no=1,
        build_id="build-2",
    )
    assert store.resolve(source_id("p1", "hash-a", 1, "build-1")) is None
    resolved = store.resolve(source_id("p1", "hash-a", 1, "build-2"))
    assert resolved is not None and resolved.chunk_id == "p1:dv_a:1"


def test_resolve_direct_lookup_of_new_format_chunk_id(store: SqliteStore) -> None:
    """``chunk_id`` нового формата (``product:doc_version_id:chunk_no``) резолвится прямо.

    Это первый путь :meth:`resolve`; он не зависит от разбора ``source_id``, поэтому смена
    формата ``chunk_id`` (16 §6) его не ломает — в отличие от фолбэка.
    """
    _insert_chunk(
        store,
        chunk_id="p1:dv_5f3a:7",
        doc_id="p1:hash-z",
        product_id="p1",
        chunk_no=7,
    )
    resolved = store.resolve("p1:dv_5f3a:7")
    assert resolved is not None
    assert resolved.chunk_id == "p1:dv_5f3a:7"
    assert resolved.source_url == _URL


def test_resolve_source_id_carrying_doc_version(store: SqliteStore) -> None:
    """``source_id`` с версией документа во втором сегменте резолвится через ``corpus_docs``.

    Граница A (16 §6): ``chunk_id``/``source_id`` несут ``dv_...``, а ``doc_id`` в корпусе —
    ``product:content_hash``; ``dv_`` из хеша не выводится (``short_hash`` необратим).
    Прямой lookup промахивается (у ``chunk_id`` три сегмента, у ``source_id`` — четыре),
    поэтому единственный путь — отобразить версию в ``doc_id`` запросом по ``corpus_docs``.
    """
    _insert_chunk(
        store,
        chunk_id="p1:dv_c0ffee:0",
        doc_id="p1:hash-abc",
        product_id="p1",
        chunk_no=0,
    )
    _insert_version(store, "dv_c0ffee", "p1", "p1:hash-abc")

    resolved = store.resolve(source_id("p1", "dv_c0ffee", 0, "build-1"))
    assert resolved is not None, "версия обязана отобразиться в doc_id корпуса"
    assert resolved.chunk_id == "p1:dv_c0ffee:0"
    assert resolved.source_url == _URL
    assert resolved.build_id == "build-1"

def test_resolve_second_version_of_the_same_document(store: SqliteStore) -> None:
    """Вторая версия того же документа резолвится: у документа версий больше одной.

    ``dv`` включает уровень извлечения, ``doc_id`` — нет, поэтому один текст на двух
    уровнях — один документ корпуса и две версии. Отображение обязано быть тотальным
    (версия → документ), иначе evidence второй версии не адресует свой чанк.
    """
    _insert_chunk(
        store,
        chunk_id="p1:dv_text:0",
        doc_id="p1:hash-abc",
        product_id="p1",
        chunk_no=0,
        build_id="build-1",
    )
    with store.conn:
        store.conn.execute(
            "INSERT INTO corpus_chunks"
            " (chunk_id, doc_id, product_id, url, heading_path, chunk_no, text,"
            "  context_prefix, content_hash, build_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "p1:dv_layout:0",
                "p1:hash-abc",
                "p1",
                _URL,
                "H1",
                0,
                "body text",
                "prefix",
                _HASH,
                "build-1",
            ),
        )
    _insert_version(store, "dv_text", "p1", "p1:hash-abc")
    _insert_version(store, "dv_layout", "p1", "p1:hash-abc")

    for version, expected in (("dv_text", "p1:dv_text:0"), ("dv_layout", "p1:dv_layout:0")):
        resolved = store.resolve(source_id("p1", version, 0, "build-1"))
        assert resolved is not None, version
        assert resolved.chunk_id == expected

def test_resolve_doc_version_without_corpus_mapping_returns_none(
    store: SqliteStore,
) -> None:
    """Версия, которой нет в ``corpus_docs``, не резолвится — и не падает.

    ``dv_`` не выводится из ``content_hash``, поэтому догадка вместо отображения была бы
    резолвом в никуда; честный ответ здесь — ``None``.
    """
    _insert_chunk(
        store,
        chunk_id="p1:dv_other:0",
        doc_id="p1:hash-abc",
        product_id="p1",
        chunk_no=0,
    )

    assert store.resolve(source_id("p1", "dv_missing", 0)) is None

def test_resolve_on_db_without_version_table_does_not_fail(tmp_path: Path) -> None:
    """Read-only БД прошлой версии (без таблицы отображения): чтение не падает.

    Точный адрес чанка от таблицы не зависит — он и есть ``chunk_id`` (16 §6), поэтому
    резолв 4-сегментного ``source_id`` работает и здесь. Отображение версий в такой БД
    пустое: «не записано», а не сбой чтения.
    """
    path = tmp_path / "legacy.db"
    store = SqliteStore(path)
    store.create_run("run-1", _cfg())
    _insert_chunk(
        store,
        chunk_id="p1:dv_c0ffee:0",
        doc_id="p1:hash-abc",
        product_id="p1",
        chunk_no=0,
    )
    store.close()

    raw = sqlite3.connect(path)
    raw.execute("DROP TABLE corpus_doc_version")
    raw.commit()
    raw.close()

    legacy = SqliteStore(path, read_only=True)
    try:
        assert legacy.resolve("p1:dv_c0ffee:0") is not None
        resolved = legacy.resolve(source_id("p1", "dv_c0ffee", 0, "build-1"))
        assert resolved is not None
        assert resolved.chunk_id == "p1:dv_c0ffee:0"
        assert legacy.iter_corpus_doc_versions("run-1", "p1") == {}
    finally:
        legacy.close()

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
