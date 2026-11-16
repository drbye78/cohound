"""Персистенция адреса куска внутри таблицы: ``corpus_chunks.table_ref`` (O39 фаза 2).

Проверяется правило записи, а не форма хранения. Адрес — часть записи о чанке, и у него
два носителя: ``Chunk.table_ref`` (порт) и текст колонки (JSON). Поэтому:

* **round-trip**: записанный ``CorpusChunk`` читается обратно тем же ``TableRef`` — и через
  точечное чтение, и через итерацию сборки, и через батч-чтение;
* **``NULL`` → ``None``**, а не «пустой адрес»: ``None`` значит «адрес неизвестен» (читатель
  таблиц их не даёт либо сопоставление не сошлось), и это **не** «кусок не из таблицы»;
* **порча колонки → ``None``, а не исключение**: отчёт обязан строиться и на повреждённой
  БД, а адрес, собранный из испорченной записи, был бы догадкой (A3);
* **колонка объявлена в свежей БД** — схемы не догоняются ``ALTER TABLE`` при открытии
  (решение владельца 01.11.2026), поэтому отсутствие колонки было бы дефектом ``DDL``.

Тесты нейтральны (§1.3), работают на ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import (
    CorpusChunk,
    CorpusDoc,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.domain.doc_tables import TableRef
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-table-ref"
BUILD_ID = "build-table-ref"


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
        instance.upsert_corpus_doc(
            CorpusDoc(
                doc_id="d1",
                product_id="p1",
                url="https://docs.test/p1/d1",
                content_hash="d1",
            )
        )
        yield instance


def _chunk(
    chunk_id: str = "c1",
    *,
    chunk_no: int = 0,
    text: str = "| a | b |\n",
    table_ref: TableRef | None = None,
) -> CorpusChunk:
    """Чанк с точным срезом: ``char_end - char_start == len(text)`` (инвариант модели)."""
    return CorpusChunk(
        chunk_id=chunk_id,
        doc_id="d1",
        product_id="p1",
        chunk_no=chunk_no,
        text=text,
        content_hash=f"hash-{chunk_id}",
        build_id=BUILD_ID,
        char_start=0,
        char_end=len(text),
        table_ref=table_ref,
    )


# --------------------------------------------------------------------------- #
# Round-trip
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "ref",
    [TableRef("t0"), TableRef("t0", 1, 3), TableRef("таблица-2", 4, 4)],
)
def test_table_ref_round_trips(store: SqliteStore, ref: TableRef) -> None:
    """Адрес читается обратно тем же объектом — и без диапазона, и с парой, и с одной строкой."""
    store.add_chunk(_chunk(table_ref=ref))

    got = store.get_chunk("c1")

    assert got is not None
    assert got.table_ref == ref


def test_null_column_reads_as_unknown(store: SqliteStore) -> None:
    """``NULL`` — «адрес неизвестен», а не «кусок не из таблицы» и не пустой адрес."""
    store.add_chunk(_chunk())

    got = store.get_chunk("c1")

    assert got is not None
    assert got.table_ref is None


def test_batch_write_keeps_addresses(store: SqliteStore) -> None:
    """Батчевый путь записи (``add_chunks_batch``) несёт адрес так же, как точечный."""
    store.add_chunks_batch(
        [
            _chunk("c1", chunk_no=0, table_ref=TableRef("t0", 1, 2)),
            _chunk("c2", chunk_no=1, text="обычный текст", table_ref=None),
        ]
    )

    got = store.get_chunks(["c1", "c2"])

    assert got["c1"].table_ref == TableRef("t0", 1, 2)
    assert got["c2"].table_ref is None


def test_iter_chunks_keeps_addresses(store: SqliteStore) -> None:
    """Итерация сборки (resume/rebuild) не теряет адрес: это durable провенанс."""
    store.add_chunks_batch(
        [
            _chunk("c1", chunk_no=0, table_ref=TableRef("t0", 1, 2)),
            _chunk("c2", chunk_no=1, text="обычный текст", table_ref=None),
        ]
    )

    got = {chunk.chunk_id: chunk.table_ref for chunk in store.iter_chunks(BUILD_ID)}

    assert got == {"c1": TableRef("t0", 1, 2), "c2": None}


# --------------------------------------------------------------------------- #
# Порча колонки
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw",
    [
        "{broken",  # незакрытый объект
        "не json",  # мусор
        "[]",  # не объект
        '{"row_start":1,"row_end":2}',  # нет table_id
        '{"table_id":"t0","row_start":1}',  # половина диапазона
        '{"table_id":"t0","row_start":3,"row_end":1}',  # перевёрнутый диапазон
        '{"table_id":"t0","row_start":0,"row_end":2}',  # строка 0 не бывает
        '{"table_id":"t0","row_start":true,"row_end":true}',  # bool — не номер строки
    ],
)
def test_corrupted_column_reads_as_unknown(store: SqliteStore, raw: str) -> None:
    """Порча в колонке — «адрес неизвестен», а не исключение при чтении корпуса."""
    store.add_chunk(_chunk(table_ref=TableRef("t0", 1, 2)))
    # Колонку портит не писатель движка, а повреждённый файл/чужой писатель — пишем напрямую.
    store.conn.execute("UPDATE corpus_chunks SET table_ref = ? WHERE chunk_id = 'c1'", (raw,))

    got = store.get_chunk("c1")

    assert got is not None
    assert got.table_ref is None


def test_corrupted_column_does_not_break_iteration(store: SqliteStore) -> None:
    """Итерация сборки переживает порчу: одна битая запись не отменяет остальные."""
    store.add_chunks_batch(
        [
            _chunk("c1", chunk_no=0, table_ref=TableRef("t0", 1, 2)),
            _chunk("c2", chunk_no=1, text="второй", table_ref=TableRef("t1", 1, 1)),
        ]
    )
    store.conn.execute("UPDATE corpus_chunks SET table_ref = '{oops' WHERE chunk_id = 'c1'")

    got = {chunk.chunk_id: chunk.table_ref for chunk in store.iter_chunks(BUILD_ID)}

    assert got == {"c1": None, "c2": TableRef("t1", 1, 1)}


# --------------------------------------------------------------------------- #
# Схема
# --------------------------------------------------------------------------- #


def test_column_is_declared_in_fresh_database(store: SqliteStore) -> None:
    """Колонка объявлена в ``CREATE TABLE`` свежей БД: схемы не догоняются при открытии."""
    columns = {
        row["name"]: row for row in store.conn.execute("PRAGMA table_info(corpus_chunks)")
    }

    assert "table_ref" in columns
    assert (columns["table_ref"]["type"] or "").upper() == "TEXT"
    # ``NOT NULL`` не ставится: ``NULL`` — законное «адрес неизвестен».
    assert columns["table_ref"]["notnull"] == 0
