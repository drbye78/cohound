"""Читатель корпуса прогона: гейт версии, терпимость к колонкам, только чтение.

БД собирается синтетической и **в форме 3.20.0** — без ``corpus_chunks.table_ref``,
``corpus_docs.page_map``/``ocr_probe``: именно такую версию объявляют прогоны стенда, и
именно на ней проверяется, что отсутствие аддитивной колонки — не ошибка чтения.

Проверяются свойства, а не форма хранения:

* гейт версии по умолчанию сохранён, разрешение — явное и видимое полем ``mismatch``;
* ``NULL``/отсутствующая колонка не подменяются нулём и пустым адресом;
* корпус читается по продукт-партиции, порядок детерминирован, N+1 нет;
* запись невозможна, файл не меняется — читатель не «догоняет» схему.

Тесты нейтральны (§1.3) и работают на ``tmp_path`` с ``journal_mode=DELETE``: WAL-файлы
сделали бы сравнение байтов файла бессмысленным (``-wal``/``-shm`` живут рядом).
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from core.store.corpus_reader import (
    CorpusReader,
    CorpusReaderError,
    PartitionRef,
    RunCorpusRef,
)
from core.store.sqlite import SCHEMA_VERSION
from core.util.schema_gate import SchemaVersionMismatch

#: Версия, которую объявляют прогоны стенда (код ждёт :data:`SCHEMA_VERSION`).
FOREIGN_VERSION = "3.20.0"

#: Сборка продукт-партиции, в которой лежит основной набор чанков.
BUILD_ID = "pb_p1p2"

#: Урезанный DDL в форме 3.20.0: ``table_ref`` у чанка нет, ``page_map``/``ocr_probe``
#: у документа нет — ровно то, чем 3.20.0 отличается от текущей схемы.
_DDL = """
PRAGMA journal_mode = DELETE;

CREATE TABLE schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE runs (
    run_id             TEXT PRIMARY KEY,
    package_id         TEXT,
    index_build_id     TEXT,
    retrieval_profile  TEXT,
    retrieval_cutoff_K INTEGER,
    embed_model_id     TEXT
);

CREATE TABLE corpus_docs (
    doc_id       TEXT PRIMARY KEY,
    product_id   TEXT NOT NULL,
    url          TEXT NOT NULL,
    content_hash TEXT,
    lang         TEXT
);

CREATE TABLE corpus_chunks (
    chunk_id          TEXT PRIMARY KEY,
    doc_id            TEXT NOT NULL,
    product_id        TEXT NOT NULL,
    url               TEXT,
    heading_path      TEXT,
    lang              TEXT,
    chunk_no          INTEGER,
    text              TEXT NOT NULL,
    context_prefix    TEXT,
    content_hash      TEXT,
    extractor_version TEXT,
    build_id          TEXT,
    embed_model_id    TEXT,
    embedded_at       TEXT,
    char_start        INTEGER,
    char_end          INTEGER
);

CREATE TABLE index_manifest (
    index_build_id     TEXT NOT NULL,
    partition_build_id TEXT,
    product_id         TEXT NOT NULL,
    embed_model_id     TEXT,
    embed_dim          INTEGER,
    PRIMARY KEY (index_build_id, product_id)
);
"""

_RUN = ("run-1", "pkg", "ib_all", "R0", 10, "bge-m3")

#: Тип строки прогона в синтетике: ровно колонки ``INSERT`` ниже.
_RunRow = tuple[str, str, str, str, int, str]


def _build_db(
    path: Path, *, version: str = FOREIGN_VERSION, runs: list[_RunRow] | None = None
) -> Path:
    """Собирает синтетическую БД прогона в форме 3.20.0."""
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_DDL)
        conn.execute("INSERT INTO schema_meta VALUES ('schema_version', ?)", (version,))
        conn.executemany(
            "INSERT INTO runs (run_id, package_id, index_build_id, retrieval_profile,"
            " retrieval_cutoff_K, embed_model_id) VALUES (?, ?, ?, ?, ?, ?)",
            runs if runs is not None else [_RUN],
        )
        conn.executemany(
            "INSERT INTO corpus_docs (doc_id, product_id, url, content_hash, lang)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                ("d1", "p1", "https://docs.test/p1/d1", "h1", "en"),
                ("d2", "p2", "https://docs.test/p2/d2", "h2", "ru"),
            ],
        )
        conn.executemany(
            "INSERT INTO corpus_chunks (chunk_id, doc_id, product_id, url, heading_path,"
            " lang, chunk_no, text, context_prefix, content_hash, build_id,"
            " char_start, char_end) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                # c1 — полный набор провенанса: адрес и смещения заданы.
                (
                    "c1",
                    "d1",
                    "p1",
                    "https://docs.test/p1/d1",
                    "Install > Linux",
                    "en",
                    0,
                    "first chunk",
                    "Install > Linux",
                    "ch1",
                    BUILD_ID,
                    0,
                    11,
                ),
                # c2 — чанкер смещений не дал: ``NULL``, а не ноль.
                (
                    "c2",
                    "d1",
                    "p1",
                    "https://docs.test/p1/d1",
                    "Install > Linux",
                    "en",
                    1,
                    "second chunk",
                    None,
                    None,
                    BUILD_ID,
                    None,
                    None,
                ),
                # c3 — текстовые поля ``NULL``: читаются как пустые строки.
                (
                    "c3",
                    "d1",
                    "p1",
                    "https://docs.test/p1/d1",
                    None,
                    None,
                    2,
                    "third chunk",
                    None,
                    None,
                    BUILD_ID,
                    11,
                    22,
                ),
                # c4 — другой продукт: проверяет продукт-фильтр и порядок.
                (
                    "c4",
                    "d2",
                    "p2",
                    "https://docs.test/p2/d2",
                    "Intro",
                    "ru",
                    0,
                    "other product",
                    "Intro",
                    "ch4",
                    BUILD_ID,
                    0,
                    13,
                ),
                # c5 — другая партиция: в BUILD_ID попасть не должен.
                (
                    "c5",
                    "p1",
                    "p1",
                    "https://docs.test/p1/d1",
                    "Install",
                    "en",
                    0,
                    "other build",
                    "Install",
                    "ch5",
                    "pb_other",
                    0,
                    11,
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO index_manifest (index_build_id, partition_build_id, product_id,"
            " embed_model_id, embed_dim) VALUES (?, ?, ?, ?, ?)",
            [
                ("ib_all", "pb_p1", "p1", "bge-m3", 1024),
                ("ib_all", "pb_p2", "p2", "bge-m3", 1024),
                # Строка без сборки партиции: «неизвестно, чей корпус» — пропускается.
                ("ib_all", "", "p3", "bge-m3", 1024),
                # Чужая сборка: в ib_all попасть не должна.
                ("ib_other", "pb_p9", "p9", "bge-m3", 768),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return path


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Синтетическая БД прогона в форме 3.20.0."""
    return _build_db(tmp_path / "cohound.db")


def _reader(
    path: Path, *, run_id: str | None = None, allow_foreign_schema: bool = True
) -> CorpusReader:
    """Читатель с явным разрешением чужой схемы (по умолчанию — из фикстуры)."""
    return CorpusReader(path, run_id=run_id, allow_foreign_schema=allow_foreign_schema)


# --------------------------------------------------------------------- гейт версии


def test_foreign_schema_is_rejected_without_explicit_permission(db_path: Path) -> None:
    """По умолчанию гейт сохранён: чужой файл отвергается до чтения данных."""
    with pytest.raises(SchemaVersionMismatch) as exc:
        CorpusReader(db_path)
    message = str(exc.value)
    assert str(db_path) in message
    assert FOREIGN_VERSION in message
    assert SCHEMA_VERSION in message


def test_foreign_schema_reads_when_permission_is_explicit(db_path: Path) -> None:
    """С разрешением читаем и видим несовпадение полем, а не предупреждением в stderr."""
    with _reader(db_path) as reader:
        assert reader.mismatch is True
        assert reader.schema_version == FOREIGN_VERSION
        assert reader.expected_schema_version == SCHEMA_VERSION


# ------------------------------------------------------- терпимость к колонкам и NULL


def test_absent_table_ref_and_null_char_span_stay_unknown(db_path: Path) -> None:
    """Отсутствующая ``table_ref`` и ``NULL`` смещений — ``None``, а не ноль/пустой адрес."""
    with _reader(db_path) as reader:
        chunk = reader.get_chunk("c2")
    assert chunk is not None
    assert chunk.table_ref is None
    assert chunk.char_start is None
    assert chunk.char_end is None
    assert chunk.content_hash == ""
    assert chunk.context_prefix == ""


def test_null_text_fields_read_as_empty_strings(db_path: Path) -> None:
    """``NULL`` в текстовых полях — ``""``; ``chunk_no`` при ``NULL`` — ``0``."""
    with _reader(db_path) as reader:
        chunk = reader.get_chunk("c3")
    assert chunk is not None
    assert chunk.heading_path == ""
    assert chunk.content_hash == ""
    assert chunk.chunk_no == 2


def test_absent_meta_columns_are_not_invented(tmp_path: Path) -> None:
    """Колонок ``lang``/``heading_path`` в схеме нет — ключей в метаданных тоже нет."""
    path = tmp_path / "narrow.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta VALUES ('schema_version', '3.20.0');
            CREATE TABLE corpus_chunks (
                chunk_id   TEXT PRIMARY KEY,
                doc_id     TEXT NOT NULL,
                product_id TEXT NOT NULL,
                url        TEXT,
                text       TEXT NOT NULL,
                build_id   TEXT
            );
            INSERT INTO corpus_chunks (chunk_id, doc_id, product_id, url, text, build_id)
                VALUES ('c1', 'd1', 'p1', 'https://docs.test/p1/d1', 'text', 'pb_p1');
            """
        )
        conn.commit()
    finally:
        conn.close()
    with _reader(path) as reader:
        meta = reader.chunk_meta(["c1"])
        chunk = reader.get_chunk("c1")
    assert meta == {"c1": {"url": "https://docs.test/p1/d1", "product_id": "p1"}}
    assert chunk is not None
    assert chunk.heading_path == ""
    assert chunk.table_ref is None


# --------------------------------------------------------------------- корпус


def test_iter_chunks_filters_partition_and_product_in_stable_order(
    db_path: Path,
) -> None:
    """Партиция — не композиция: чанки другой сборки не видны; порядок детерминирован."""
    with _reader(db_path) as reader:
        everything = [chunk.chunk_id for chunk in reader.iter_chunks(BUILD_ID)]
        only_p1 = [
            chunk.chunk_id for chunk in reader.iter_chunks(BUILD_ID, product_id="p1")
        ]
        assert reader.count_chunks(BUILD_ID) == 4
        assert reader.count_chunks(BUILD_ID, product_id="p1") == 3
        # Повтор чтения того же файла даёт ту же последовательность.
        again = [chunk.chunk_id for chunk in reader.iter_chunks(BUILD_ID)]
    assert everything == ["c1", "c2", "c3", "c4"]
    assert only_p1 == ["c1", "c2", "c3"]
    assert again == everything


def test_get_chunks_returns_only_found_in_batches(db_path: Path) -> None:
    """1200 адресов читаются пачками; отсутствующие в результат не попадают."""
    ids = [f"missing-{index}" for index in range(1200)]
    ids.extend(["c1", "c4", "c2"])
    with _reader(db_path) as reader:
        found = reader.get_chunks(ids)
    assert set(found) == {"c1", "c2", "c4"}
    assert found["c1"].text == "first chunk"
    assert found["c2"].char_start is None


def test_chunk_meta_returns_existing_fields_only(db_path: Path) -> None:
    """Метаданные — только реальные колонки; ``NULL`` читается как ``""``."""
    with _reader(db_path) as reader:
        meta = reader.chunk_meta(["c1", "c3", "missing"])
    assert set(meta) == {"c1", "c3"}
    assert meta["c1"] == {
        "url": "https://docs.test/p1/d1",
        "lang": "en",
        "heading_path": "Install > Linux",
        "product_id": "p1",
    }
    assert meta["c3"]["lang"] == ""
    assert meta["c3"]["heading_path"] == ""


def test_partitions_come_from_manifest_and_skip_empty_rows(db_path: Path) -> None:
    """Партиции — из ``index_manifest`` по сборке; строки без продукта/сборки пропущены."""
    with _reader(db_path) as reader:
        partitions = reader.partitions("ib_all")
        assert partitions == [
            PartitionRef(
                product_id="p1",
                partition_build_id="pb_p1",
                embed_model_id="bge-m3",
                embed_dim=1024,
            ),
            PartitionRef(
                product_id="p2",
                partition_build_id="pb_p2",
                embed_model_id="bge-m3",
                embed_dim=1024,
            ),
        ]
        assert reader.partitions("ib_absent") == []


# --------------------------------------------------------------------- строка прогона


def test_run_ref_reads_the_single_run(db_path: Path) -> None:
    """Один прогон в файле — читается без указания ``run_id``."""
    with _reader(db_path) as reader:
        ref = reader.run_ref()
    assert isinstance(ref, RunCorpusRef)
    assert ref.run_id == "run-1"
    assert ref.package_id == "pkg"
    assert ref.index_build_id == "ib_all"
    assert ref.retrieval_profile == "R0"
    assert ref.retrieval_cutoff_k == 10
    assert ref.embed_model_id == "bge-m3"
    assert ref.schema_version == FOREIGN_VERSION


def test_run_ref_requires_run_id_when_several_runs(tmp_path: Path) -> None:
    """Несколько прогонов без ``run_id`` — отказ с числом найденных, а не «первый»."""
    runs = [(f"run-{index}", "pkg", "ib_all", "R0", 10, "bge-m3") for index in range(9)]
    path = _build_db(tmp_path / "many.db", runs=runs)
    with _reader(path) as reader:
        with pytest.raises(CorpusReaderError) as exc:
            reader.run_ref()
        assert "9" in str(exc.value)
        assert "run_id" in str(exc.value)
    # Явно названный прогон читается в том же файле.
    with _reader(path, run_id="run-3") as reader:
        assert reader.run_ref().run_id == "run-3"


def test_run_ref_rejects_empty_runs_table(tmp_path: Path) -> None:
    """Ноль прогонов — тоже отказ с числом: выбирать нечего."""
    path = _build_db(tmp_path / "empty.db", runs=[])
    with _reader(path) as reader, pytest.raises(CorpusReaderError) as exc:
        reader.run_ref()
    assert "0" in str(exc.value)


# ------------------------------------------------------------- только чтение


def test_write_is_impossible(db_path: Path) -> None:
    """Соединение открыто ``mode=ro``: ``INSERT`` отвергается движком."""
    with _reader(db_path) as reader, pytest.raises(sqlite3.OperationalError):
        reader.conn.execute(
            "INSERT INTO corpus_chunks (chunk_id, doc_id, product_id, text)"
            " VALUES ('x', 'd1', 'p1', 'text')"
        )


def test_file_bytes_do_not_change(db_path: Path) -> None:
    """Работа читателя не меняет файл: ни байт, ни WAL-спутников рядом не появляется."""
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()
    with _reader(db_path) as reader:
        reader.run_ref()
        reader.partitions("ib_all")
        reader.count_chunks(BUILD_ID)
        list(reader.iter_chunks(BUILD_ID))
        reader.get_chunk("c1")
        reader.get_chunks(["c1", "c2"])
        reader.chunk_meta(["c1"])
    after = hashlib.sha256(db_path.read_bytes()).hexdigest()
    assert before == after
    assert not db_path.with_name(db_path.name + "-wal").exists()


# --------------------------------------------------------------------- отказы


def test_missing_chunks_table_is_a_reader_error(tmp_path: Path) -> None:
    """Файл с чужой версией, но без ``corpus_chunks`` — не БД прогона."""
    path = tmp_path / "nochunks.db"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta VALUES ('schema_version', '3.20.0');
            CREATE TABLE runs (run_id TEXT PRIMARY KEY);
            """
        )
        conn.commit()
    finally:
        conn.close()
    with pytest.raises(CorpusReaderError):
        _reader(path)


def test_missing_file_is_a_reader_error(tmp_path: Path) -> None:
    """Отсутствующий файл — ``CorpusReaderError``, и читатель его не создаёт."""
    path = tmp_path / "absent.db"
    with pytest.raises(CorpusReaderError) as exc:
        CorpusReader(path)
    assert str(path) in str(exc.value)
    assert not path.exists()
