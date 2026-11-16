"""Персистенция структурных блоков документа: ``corpus_docs.blocks`` (O36 фаза 2).

Проверяется правило записи, а не форма хранения: ``doc_id = product_id:hash``, поэтому тот
же текст может прийти от читателя **без** блоков (HTML, текст, Office, docling,
переиспользование из набора чанков), и его ``NULL`` не вправе стереть уже прочитанные
блоки. Обратное тоже верно и различимо: пустой список — **факт** («читатель смотрел и не
дал»), он пишется как ``[]`` и перекрывает прежнее значение честно.

Тесты нейтральны (§1.3), работают на ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import (
    CorpusDoc,
    ExtractedBlock,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.store.sqlite import DDL, SCHEMA_VERSION
from core.store.sqlite_store import SqliteStore
from tests.store.test_corpus_docs_partial import _create_sql, _create_sql_without

RUN_ID = "run-blocks"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as instance:
        instance.create_run(
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
        yield instance


def _blocks() -> tuple[ExtractedBlock, ...]:
    return (
        ExtractedBlock(
            block_id="b0",
            kind="page",
            char_start=0,
            char_end=6,
            order=0,
            text_source="read",
            page_no=1,
        ),
        ExtractedBlock(
            block_id="b1",
            kind="page",
            char_start=8,
            char_end=16,
            order=1,
            text_source="read",
            page_no=2,
        ),
    )


def _doc(doc_id: str, **overrides: object) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": "p1",
        "url": f"https://docs.test/p1/{doc_id}",
        "content_hash": doc_id,
        "extracted_chars": 16,
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]


def _read(store: SqliteStore, doc_id: str) -> CorpusDoc:
    docs = {doc.doc_id: doc for doc in store.iter_corpus_docs(RUN_ID)}
    return docs[doc_id]


def test_blocks_roundtrip(store: SqliteStore) -> None:
    """Блоки читаются обратно теми же объектами (адрес, отрезок, порядок, происхождение)."""
    blocks = _blocks()
    store.upsert_corpus_doc(_doc("d1", blocks=blocks))

    assert _read(store, "d1").blocks == blocks


def test_writer_without_blocks_does_not_erase_measured(store: SqliteStore) -> None:
    """``None`` (читатель блоков не даёт) не стирает уже прочитанные блоки."""
    blocks = _blocks()
    store.upsert_corpus_doc(_doc("d1", blocks=blocks))
    store.upsert_corpus_doc(_doc("d1", blocks=None))

    assert _read(store, "d1").blocks == blocks, (
        "повторная запись того же текста без блоков не вправе стереть измеренное"
    )


def test_empty_list_is_a_fact_and_overwrites(store: SqliteStore) -> None:
    """``()`` («смотрел и не дал») — факт: он перекрывает прежнее значение."""
    store.upsert_corpus_doc(_doc("d1", blocks=_blocks()))
    store.upsert_corpus_doc(_doc("d1", blocks=()))

    assert _read(store, "d1").blocks == ()


def test_absent_blocks_read_back_as_none(store: SqliteStore) -> None:
    """``None`` («не даёт») и ``()`` («смотрел и не дал») не сводятся друг к другу."""
    store.upsert_corpus_doc(_doc("d1", blocks=None))
    store.upsert_corpus_doc(_doc("d2", blocks=()))

    assert _read(store, "d1").blocks is None
    assert _read(store, "d2").blocks == ()


def test_row_of_previous_schema_reads_as_not_measured(tmp_path: Path) -> None:
    """Строка прошлой схемы (колонки нет) читается как ``None``, а не падает.

    Гейт отвергает файл другой версии, поэтому кейс формулируется честно: файл объявляет
    **текущую** версию, но колонки ``blocks`` не имеет (пересобранный/повреждённый). Чтение
    идёт через ``_row_optional``: отсутствие обязано значить «значение не записано» — то
    есть «читатель блоков не даёт», а не сбой чтения и не пустой список.
    """
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            DDL.replace(
                _create_sql("corpus_docs"),
                _create_sql_without("corpus_docs", ("blocks",)),
            )
        )
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        conn.execute(
            "INSERT INTO runs (run_id, created_at, mode, case_id, anchor_product_id)"
            " VALUES (?, '2026-11-16T00:00:00Z', 'strict', 'case', 'p1')",
            (RUN_ID,),
        )
        conn.execute(
            "INSERT INTO corpus_docs (doc_id, product_id, url)"
            " VALUES ('p1:old', 'p1', 'https://docs.test/p1/old')"
        )
        conn.commit()
    finally:
        conn.close()

    legacy = SqliteStore(path, read_only=True)
    try:
        read = {item.doc_id: item for item in legacy.iter_corpus_docs(RUN_ID)}
        assert read["p1:old"].blocks is None
    finally:
        legacy.close()


def test_out_of_text_blocks_are_rejected_on_the_corpus_row() -> None:
    """Инвариант держится и на границе корпуса: отрезок за ``extracted_chars`` — отказ.

    Строка БД — тоже носитель факта: блок, выходящий за текст, дал бы цитате адрес
    фрагмента, которого в документе нет.
    """
    with pytest.raises(ValueError, match="выходит за текст"):
        _doc(
            "d1",
            extracted_chars=5,
            blocks=(
                ExtractedBlock(
                    block_id="b0",
                    kind="page",
                    char_start=0,
                    char_end=10,
                    order=0,
                    text_source="read",
                ),
            ),
        )
