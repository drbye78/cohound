"""v5: частичность извлечения как durable свойство версии документа (``state.db``).

Признак «прочитана часть документа» (docling ``PARTIAL_SUCCESS``, битая страница pdfium)
— **наблюдение**, а не вывод из текста, поэтому он хранится рядом с версией, к которой
относится. Проверяется три вещи:

* запись/чтение признака не теряет ни одного из трёх состояний:
  ``True`` (частично) / ``False`` (полное) / ``NULL`` (не измерено);
* свежая ``state.db`` содержит все четыре колонки частичности, и значение «не измерено»
  остаётся ``NULL``, а не превращается в ``0`` (``0`` утверждал бы полноту);
* файл схемы ≤ 5 отвергается видимо (``SchemaVersionMismatch``), а не догоняется
  ``ALTER TABLE`` при открытии: тихая миграция объявляла бы старую БД новой.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.ports.state import DocumentVersionRow
from core.store.sqlite import SchemaVersionMismatch
from core.store.state_sqlite import STATE_SCHEMA_VERSION, connect_state
from core.store.state_store import SqliteStateStore

#: ``document_version`` в редакции до v5: колонок частичности нет, версия схемы не
#: объявлена вовсе. Гейт (v6) обязан отвергнуть такой файл, а не достроить его.
V4_DOCUMENT_VERSION_DDL = """
CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE document_version (
    doc_version_id    TEXT PRIMARY KEY,
    doc_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    url               TEXT NOT NULL,
    content_hash      TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    level             TEXT NOT NULL,
    raw_path          TEXT,
    chunker_version   TEXT,
    chunk_ids_digest  TEXT,
    first_seen_run_id TEXT NOT NULL,
    last_seen_run_id  TEXT NOT NULL,
    first_seen_at     TEXT NOT NULL,
    last_seen_at      TEXT NOT NULL,
    status            TEXT NOT NULL,
    UNIQUE (product_id, content_hash, extractor_version, level)
);
"""


def _row(*, partial: bool | None, reason: str | None = None) -> DocumentVersionRow:
    """Строка версии документа с заданным состоянием частичности."""
    return DocumentVersionRow(
        doc_version_id="dv_1",
        doc_id="p1:hash_text",
        product_id="p1",
        url="https://docs.test/p1/manual",
        content_hash="hash_text",
        extractor_version="extractor-1",
        level="full",
        raw_path="corpus_raw/hash.bin",
        chunker_version="chunker-1",
        chunk_ids_digest="digest",
        first_seen_run_id="run-1",
        last_seen_run_id="run-1",
        first_seen_at="2026-01-01T00:00:00+00:00",
        last_seen_at="2026-01-01T00:00:00+00:00",
        status="active",
        extract_partial=partial,
        extract_partial_reason=reason,
        extract_pages_converted=7 if partial else None,
        extract_page_count=2763 if partial is not None else None,
    )


def _build_v4(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(V4_DOCUMENT_VERSION_DDL)
        conn.execute(
            "INSERT INTO document_version (doc_version_id, doc_id, product_id, url, "
            "content_hash, extractor_version, level, raw_path, chunker_version, "
            "chunk_ids_digest, first_seen_run_id, last_seen_run_id, first_seen_at, "
            "last_seen_at, status) VALUES ('dv_old', 'p1:h', 'p1', 'https://docs.test/a', "
            "'h', 'extractor-1', 'full', NULL, 'chunker-1', 'd', 'run-0', 'run-0', "
            "'2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', 'active')"
        )
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', '4')"
        )
        conn.commit()
    finally:
        conn.close()


def test_state_schema_version_carries_container_state() -> None:
    """``container_state`` (план 19 §E.7) объявлен в DDL, без ``ALTER`` (сейчас v11)."""
    assert STATE_SCHEMA_VERSION == "11"


def test_chunk_meta_carries_chunk_offsets() -> None:
    """Свежая БД объявляет смещения куска: без них reuse терял провенанс (L1b-2b)."""
    conn = connect_state(":memory:")
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(chunk_meta)")}
    finally:
        conn.close()
    assert {"char_start", "char_end"} <= columns


def test_document_version_carries_partiality_columns() -> None:
    """Свежая БД объявляет все четыре колонки частичности (носитель — ``STATE_DDL``)."""
    conn = connect_state(":memory:")
    try:
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(document_version)")
        }
    finally:
        conn.close()
    assert {
        "extract_partial",
        "extract_partial_reason",
        "extract_pages_converted",
        "extract_page_count",
    } <= columns


def test_partiality_round_trips_as_three_states() -> None:
    """``True``/``False``/``NULL`` переживают запись и чтение без склейки."""
    with SqliteStateStore(":memory:") as state:
        state.upsert_document_version(_row(partial=True, reason="PARTIAL_SUCCESS 7/2763"))
        stored = state.get_document_version("dv_1")
        assert stored is not None
        assert stored.extract_partial is True
        assert stored.extract_partial_reason == "PARTIAL_SUCCESS 7/2763"
        assert (stored.extract_pages_converted, stored.extract_page_count) == (7, 2763)

        # Полное извлечение того же ключа: признак и счётчики сбрасываются, а не «0/0».
        state.upsert_document_version(_row(partial=False))
        stored = state.get_document_version("dv_1")
        assert stored is not None
        assert stored.extract_partial is False
        assert stored.extract_partial_reason is None
        assert stored.extract_pages_converted is None

        # Не измерено — именно NULL, а не 0: 0 утверждал бы полноту.
        state.upsert_document_version(_row(partial=None))
        stored = state.get_document_version("dv_1")
        assert stored is not None
        assert stored.extract_partial is None
        assert stored.extract_pages_converted is None
        assert stored.extract_page_count is None

        raw = state.conn.execute(
            "SELECT extract_partial FROM document_version WHERE doc_version_id = 'dv_1'"
        ).fetchone()
        assert raw["extract_partial"] is None


def test_v4_file_is_rejected_instead_of_migrated(tmp_path: Path) -> None:
    """Файл до v5 получает **отказ**, а не тихий ``ALTER TABLE``: миграции нет.

    Прежняя проверка («строка v4 читается как ``NULL`` после миграции») жила на шиме
    ``_ADDITIVE_COLUMNS``; его удалили (решение владельца 01.11.2026), поэтому эквивалент
    переехал на два носителя: свежий ``STATE_DDL`` объявляет колонки
    (:func:`test_document_version_carries_partiality_columns`), а файл прежней версии
    отвергается видимо и **не меняется** — иначе он объявил бы себя схемой v6, не будучи ею.
    """
    db_path = tmp_path / "state.db"
    _build_v4(db_path)
    before = db_path.read_bytes()

    with pytest.raises(SchemaVersionMismatch) as excinfo:
        connect_state(db_path)

    message = str(excinfo.value)
    assert "state.db" in message  # оператор знает, какой файл пересоздавать
    assert STATE_SCHEMA_VERSION in message
    assert excinfo.value.code == "schema_version"
    # Файл не тронут: отказ — не «полумиграция».
    assert db_path.read_bytes() == before
