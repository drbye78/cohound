"""Персистенция таблиц документа: ``corpus_docs.tables`` (O39 фаза 1 плана ``26``).

Проверяется правило записи, а не форма хранения: ``doc_id = product_id:content_hash``,
поэтому тот же текст может прийти от читателя **без** таблиц (HTML, ``text_layer``,
переиспользование), и его ``NULL`` не вправе стереть уже прочитанные таблицы. Обратное
тоже верно: пустой список — **факт** («читатель искал и не нашёл»), и он обязан
перекрывать прежнее значение, а не выглядеть как «не измерено».

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
    ExtractedTable,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.store.sqlite import DDL, SCHEMA_VERSION
from core.store.sqlite_store import SqliteStore
from tests.store.test_corpus_docs_partial import _create_sql, _create_sql_without

RUN_ID = "run-tables"


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


def _table(table_id: str = "t0") -> ExtractedTable:
    return ExtractedTable(
        table_id=table_id,
        page_no=4,
        caption="Параметры",
        header=("имя", "значение"),
        rows=(("work_mem", "64MB"),),
    )


def _doc(doc_id: str, **overrides: object) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": "p1",
        "url": f"https://docs.test/p1/{doc_id}",
        "content_hash": doc_id,
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]


def _read(store: SqliteStore, doc_id: str) -> CorpusDoc:
    docs = {doc.doc_id: doc for doc in store.iter_corpus_docs(RUN_ID)}
    return docs[doc_id]


def test_tables_roundtrip(store: SqliteStore) -> None:
    """Таблицы читаются обратно теми же объектами (шапка, строки, страница, подпись)."""
    table = _table()
    store.upsert_corpus_doc(_doc("d1", tables=(table,)))

    assert _read(store, "d1").tables == (table,)


def test_writer_without_tables_does_not_erase_measured(store: SqliteStore) -> None:
    """``None`` (читатель таблиц не даёт) не стирает уже прочитанные таблицы."""
    table = _table()
    store.upsert_corpus_doc(_doc("d1", tables=(table,)))
    store.upsert_corpus_doc(_doc("d1", tables=None))

    assert _read(store, "d1").tables == (table,), (
        "повторная запись того же текста без таблиц не вправе стереть измеренное"
    )


def test_empty_list_is_a_fact_and_overwrites(store: SqliteStore) -> None:
    """``()`` («искал и не нашёл») — факт: он перекрывает прежнее значение."""
    store.upsert_corpus_doc(_doc("d1", tables=(_table(),)))
    store.upsert_corpus_doc(_doc("d1", tables=()))

    assert _read(store, "d1").tables == ()


def test_row_of_previous_schema_reads_as_not_measured(tmp_path: Path) -> None:
    """Строка прошлой схемы (колонки нет) читается как ``None``, а не падает.

    Гейт отвергает файл другой версии, поэтому кейс формулируется честно: файл объявляет
    **текущую** версию, но колонки ``tables`` не имеет (пересобранный/повреждённый). Чтение
    идёт через ``_row_optional``: отсутствие обязано значить «значение не записано» — то
    есть «читатель таблиц не даёт», а не сбой чтения и не пустой список.
    """
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            DDL.replace(
                _create_sql("corpus_docs"),
                _create_sql_without("corpus_docs", ("tables",)),
            )
        )
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        # Минимальная строка прогона: читатель корпуса проверяет его существование.
        conn.execute(
            "INSERT INTO runs (run_id, created_at, mode, case_id, anchor_product_id)"
            " VALUES (?, '2026-11-15T00:00:00Z', 'strict', 'case', 'p1')",
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
        assert read["p1:old"].tables is None
    finally:
        legacy.close()
