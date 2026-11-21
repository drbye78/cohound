"""Тесты окна в корпус для локальных инструментов (:mod:`core.pipeline.tool_corpus`).

Проверяются свойства, поломка которых даёт **правдоподобный, но чужой** ответ:

* чанк другого продукта не отдаётся ни одним методом (I17: ретривал и его адреса строго
  продуктовые) — иначе инструмент одного продукта читал бы соседа по когорте;
* ``section_chunks`` отдаёт раздел по ``heading_path``, а не окрестность по ``chunk_no``:
  граница задаётся структурой документа, и это единственное, чем T1 отличается от
  «дай ещё чанков»;
* родительский путь — префикс **по границе** пути, а не по символу («A > B» не родитель
  «A > BC»);
* ``document_tables`` различает ``None`` (читатель таблиц не даёт) и ``()`` (искал и не
  нашёл) — различие объявлено в ``core/domain/doc_tables.py``;
* адрес документа спрашивается в двух формах (``product:doc_version`` и ``doc_id`` из
  ``corpus_doc_version``), и ответ детерминирован.

Живая БД не нужна: фейковое хранилище и фейковое соединение.
"""

from __future__ import annotations

from typing import Any

from core.domain.doc_tables import (
    ExtractedTable,
    TableRef,
    table_ref_to_json,
    tables_to_json,
)
from core.pipeline.tool_corpus import HEADING_PATH_SEP, StoreToolCorpus
from core.ports.tools import ToolCorpusPort
from core.ports.types import Chunk


def _chunk(
    chunk_id: str,
    chunk_no: int,
    *,
    heading: str = "",
    text: str = "",
    table_ref: TableRef | None = None,
) -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        chunk_no=chunk_no,
        text=text or f"текст {chunk_no}",
        heading_path=heading,
        context_prefix="",
        content_hash=f"hash{chunk_no}",
        table_ref=table_ref,
    )


class _FakeCursor:
    """Курсор-заглушка: одна строка (``fetchone``) либо набор (``fetchall``)."""

    def __init__(
        self,
        row: dict[str, Any] | None = None,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self._row = row
        self._rows = rows or []

    def fetchone(self) -> dict[str, Any] | None:
        return self._row

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeConn:
    """Соединение-заглушка: отвечает на два запроса, которые делает окно.

    Строки — словари (``_column`` читает их по имени), поэтому тест не заводит SQLite
    ради двух ``SELECT``.
    """

    def __init__(
        self,
        *,
        versions: dict[tuple[str, str], str] | None = None,
        docs: dict[str, dict[str, Any]] | None = None,
        chunk_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.versions = versions or {}
        self.docs = docs or {}
        #: Строки ``corpus_chunks`` для пачечного чтения адресов (``chunk_id``/``doc_id``/
        #: ``table_ref``) — так тест видит ровно те две колонки, что читает ``table_context``.
        self.chunk_rows = chunk_rows or []
        self.queries: list[str] = []

    def execute(self, sql: str, params: tuple[Any, ...]) -> _FakeCursor:
        self.queries.append(sql)
        if "corpus_chunks" in sql:
            wanted = set(params)
            return _FakeCursor(
                rows=[row for row in self.chunk_rows if row["chunk_id"] in wanted]
            )
        if "corpus_doc_version" in sql:
            product_id, version = params
            doc_id = self.versions.get((product_id, version))
            return _FakeCursor({"doc_id": doc_id} if doc_id else None)
        if "corpus_docs" in sql and " IN (" in sql:
            wanted = set(params)
            return _FakeCursor(
                rows=[row for row in self.docs.values() if row["doc_id"] in wanted]
            )
        if "corpus_docs" in sql:
            return _FakeCursor(self.docs.get(params[0]))
        raise AssertionError(f"неожиданный запрос: {sql}")


class _FakeStore:
    """Хранилище-заглушка: чанки по адресам документов + необязательное соединение."""

    def __init__(
        self,
        chunks: list[Chunk],
        *,
        document_of: dict[str, str] | None = None,
        conn: Any | None = None,
    ) -> None:
        self._chunks = {chunk.chunk_id: chunk for chunk in chunks}
        #: ``chunk_id → адрес документа``; дефолт — префикс ``product:doc_version``.
        self._document_of = document_of or {}
        self.conn = conn

    def get_chunk(self, chunk_id: str) -> Chunk | None:
        return self._chunks.get(chunk_id)

    def iter_chunks_by_doc(self, doc_id: str):
        for chunk in self._chunks.values():
            address = self._document_of.get(chunk.chunk_id) or chunk.chunk_id.rsplit(":", 1)[0]
            if address == doc_id:
                yield chunk


def test_chunk_of_foreign_product_is_not_returned() -> None:
    """Чужой продукт — ``None``, даже если адрес существует в хранилище (I17)."""
    store = _FakeStore([_chunk("p1:dv_1:0", 0), _chunk("p2:dv_1:0", 0)])
    corpus = StoreToolCorpus(store, "p1")

    assert corpus.chunk("p1:dv_1:0") is not None
    assert corpus.chunk("p2:dv_1:0") is None


def test_document_chunks_of_foreign_product_are_empty() -> None:
    """Документ чужого продукта не отдаётся ни одним методом окна."""
    store = _FakeStore([_chunk("p2:dv_1:0", 0), _chunk("p2:dv_1:1", 1)])
    corpus = StoreToolCorpus(store, "p1")

    assert corpus.document_chunks("p2:dv_1:0") == ()
    assert corpus.section_chunks("p2:dv_1:0") == ()
    assert corpus.document_tables("p2:dv_1:0") is None


def test_document_chunks_are_ordered_and_product_filtered() -> None:
    """Чанки документа — по ``chunk_no``; чужие строки отфильтрованы, даже если их отдали."""
    chunks = [_chunk("p1:dv_1:2", 2), _chunk("p1:dv_1:0", 0)]
    store = _FakeStore([*chunks, _chunk("p1:dv_2:0", 0)])
    # Хранилище «переисполняет» адрес и возвращает лишнюю строку чужого продукта.
    store.iter_chunks_by_doc = lambda doc_id: iter(  # type: ignore[method-assign]
        [*chunks, _chunk("p2:dv_1:0", 0)]
    )
    corpus = StoreToolCorpus(store, "p1")

    found = corpus.document_chunks("p1:dv_1:0")

    assert [chunk.chunk_id for chunk in found] == ["p1:dv_1:0", "p1:dv_1:2"]


def test_document_chunks_resolve_doc_id_through_version_map() -> None:
    """Если чанки адресованы ``doc_id``, версия резолвится через ``corpus_doc_version``."""
    chunks = [_chunk("p1:dv_1:0", 0, heading="A"), _chunk("p1:dv_1:1", 1, heading="A")]
    conn = _FakeConn(versions={("p1", "dv_1"): "p1:hash1"})
    store = _FakeStore(chunks, document_of={chunk.chunk_id: "p1:hash1" for chunk in chunks}, conn=conn)
    corpus = StoreToolCorpus(store, "p1")

    found = corpus.document_chunks("p1:dv_1:0")

    assert [chunk.chunk_id for chunk in found] == ["p1:dv_1:0", "p1:dv_1:1"]
    assert any("corpus_doc_version" in sql for sql in conn.queries)


def test_section_chunks_is_the_subtree_not_the_neighbourhood() -> None:
    """T1: поддерево раздела по ``heading_path``, а не соседи по ``chunk_no``.

    Дочерний подраздел входит в раздел: адрес в «Установка», а конкретика — в
    «Установка > Шаги». Сосед «Настройка» не входит: он вне поддерева.
    """
    chunks = [
        _chunk("p1:dv_1:0", 0, heading="Установка"),
        _chunk("p1:dv_1:1", 1, heading="Установка"),
        _chunk("p1:dv_1:2", 2, heading="Настройка"),
        _chunk("p1:dv_1:3", 3, heading="Установка" + HEADING_PATH_SEP + "Шаги"),
        _chunk("p1:dv_1:4", 4, heading="Установка"),
    ]
    corpus = StoreToolCorpus(_FakeStore(chunks), "p1")

    found = corpus.section_chunks("p1:dv_1:1")

    assert [chunk.chunk_id for chunk in found] == [
        "p1:dv_1:0",
        "p1:dv_1:1",
        "p1:dv_1:3",
        "p1:dv_1:4",
    ]


def test_section_chunks_include_parent_section() -> None:
    """Родительский заголовок — тоже раздел адресованного чанка, вместе с поддеревом."""
    chunks = [
        _chunk("p1:dv_1:0", 0, heading="Ограничения"),
        _chunk("p1:dv_1:1", 1, heading="Ограничения" + HEADING_PATH_SEP + "Память"),
        _chunk("p1:dv_1:2", 2, heading="Ограничения" + HEADING_PATH_SEP + "Память и диск"),
        _chunk("p1:dv_1:3", 3, heading="Возможности"),
    ]
    corpus = StoreToolCorpus(_FakeStore(chunks), "p1")

    found = corpus.section_chunks("p1:dv_1:1")

    assert [chunk.chunk_id for chunk in found] == ["p1:dv_1:0", "p1:dv_1:1"]


def test_section_prefix_is_matched_on_path_boundary() -> None:
    """Префикс — по границе пути: «A > B» не родитель «A > BC»."""
    chunks = [
        _chunk("p1:dv_1:0", 0, heading="A" + HEADING_PATH_SEP + "B"),
        _chunk("p1:dv_1:1", 1, heading="A" + HEADING_PATH_SEP + "BC"),
    ]
    corpus = StoreToolCorpus(_FakeStore(chunks), "p1")

    assert [chunk.chunk_id for chunk in corpus.section_chunks("p1:dv_1:1")] == ["p1:dv_1:1"]


def test_unknown_address_gives_empty_answers() -> None:
    """Неизвестный адрес — пусто/``None``, а не исключение."""
    corpus = StoreToolCorpus(_FakeStore([_chunk("p1:dv_1:0", 0)]), "p1")

    assert corpus.chunk("p1:dv_9:0") is None
    assert corpus.document_chunks("p1:dv_9:0") == ()
    assert corpus.section_chunks("p1:dv_9:0") == ()
    assert corpus.document_tables("p1:dv_9:0") is None


def test_document_tables_are_read_from_document_row() -> None:
    """Таблицы документа читаются из ``corpus_docs.tables`` (только чтение)."""
    chunks = [_chunk("p1:dv_1:0", 0, table_ref=TableRef("t0", 1, 2))]
    table = ExtractedTable(table_id="t0", header=("Версия", "Поддержка"), rows=(("1", "да"),))
    conn = _FakeConn(
        versions={("p1", "dv_1"): "p1:hash1"},
        docs={"p1:hash1": {"tables": '[{"table_id":"t0","header":["Версия","Поддержка"],'
                                     '"rows":[["1","да"]]}]'}},
    )
    store = _FakeStore(chunks, document_of={"p1:dv_1:0": "p1:hash1"}, conn=conn)

    found = StoreToolCorpus(store, "p1").document_tables("p1:dv_1:0")

    assert found == (table,)


def test_document_tables_distinguish_unmeasured_from_empty() -> None:
    """``NULL`` — читатель таблиц не даёт (``None``); ``[]`` — искал и не нашёл (``()``)."""
    chunks = [_chunk("p1:dv_1:0", 0)]
    null_conn = _FakeConn(docs={"p1:dv_1": {"tables": None}})
    empty_conn = _FakeConn(docs={"p1:dv_1": {"tables": "[]"}})

    assert (
        StoreToolCorpus(_FakeStore(chunks, conn=null_conn), "p1").document_tables("p1:dv_1:0")
        is None
    )
    assert (
        StoreToolCorpus(_FakeStore(chunks, conn=empty_conn), "p1").document_tables("p1:dv_1:0")
        == ()
    )


def test_document_tables_without_connection_are_unknown() -> None:
    """Хранилище без соединения таблиц не отдаёт: ``None``, а не пустой кортеж."""
    corpus = StoreToolCorpus(_FakeStore([_chunk("p1:dv_1:0", 0)]), "p1")

    assert corpus.document_tables("p1:dv_1:0") is None


# ---------------------------------------------------------------------------
# table_context: адреса и объекты таблиц пачкой
# ---------------------------------------------------------------------------


def _tables_payload(*tables: ExtractedTable) -> str:
    assert tables_to_json(tables) is not None
    return tables_to_json(tables) or ""


def test_table_context_keys_the_document_by_chunk_id_address() -> None:
    """Ключ карты — ``(_document_id(chunk_id), table_id)``, а **не** ``corpus_docs.doc_id``.

    Это две разные строки: ``_document_id`` даёт ``{product}:{doc_version}``, а колонка
    ``corpus_chunks.doc_id`` — ``{product}:{content_hash}``. Промпт ищет таблицу по первому
    значению, поэтому ключ из второго не совпал бы ни на одном чанке — и деградация была бы
    тихой («адрес известен, объект не передан» читается как «таблиц нет»).
    """
    table = ExtractedTable(
        table_id="t0", header=("Версия", "Поддержка"), rows=(("18", "да"),)
    )
    ref = TableRef("t0", 1, 1)
    conn = _FakeConn(
        docs={
            "p1:hash1": {
                "doc_id": "p1:hash1",
                "tables": _tables_payload(table),
            }
        },
        chunk_rows=[
            {
                "chunk_id": "p1:dv_1:0",
                "doc_id": "p1:hash1",
                "table_ref": table_ref_to_json(ref),
            }
        ],
    )
    corpus = StoreToolCorpus(
        _FakeStore([_chunk("p1:dv_1:0", 0, table_ref=ref)], conn=conn), "p1"
    )

    addresses, tables = corpus.table_context(["p1:dv_1:0"])

    assert addresses == {"p1:dv_1:0": ref}
    # Ключ — адрес документа из ``chunk_id``. Ключ из ``corpus_docs.doc_id`` в карте
    # оказался бы строкой, по которой промпт ничего не найдёт.
    assert tables == {("p1:dv_1", "t0"): table}
    assert ("p1:hash1", "t0") not in tables


def test_table_context_skips_foreign_and_absent_chunks() -> None:
    """Чужой продукт (I17) и адрес вне корпуса в словарь не попадают; лишних чтений нет."""
    ref = TableRef("t0", None, None)
    conn = _FakeConn(
        docs={"p2:hash2": {"doc_id": "p2:hash2", "tables": "[]"}},
        chunk_rows=[
            {
                "chunk_id": "p2:dv_1:0",
                "doc_id": "p2:hash2",
                "table_ref": table_ref_to_json(ref),
            }
        ],
    )
    corpus = StoreToolCorpus(
        _FakeStore([_chunk("p2:dv_1:0", 0, table_ref=ref)], conn=conn), "p1"
    )

    addresses, tables = corpus.table_context(["p2:dv_1:0", "p1:dv_9:0"])

    assert addresses == {}
    assert tables == {}
    # Чужой адрес не спрашивается вовсе (право), а отсутствующий свой — спрашивается один
    # раз пачкой и просто не возвращается (цена, а не выдумка). Запроса за таблицами нет:
    # адреса в ответе не встретилось.
    assert len(conn.queries) == 1
    assert "corpus_chunks" in conn.queries[0]
    assert not any("corpus_docs" in sql for sql in conn.queries)


def test_table_context_reads_tables_only_for_documents_with_an_address() -> None:
    """Второй запрос строится только по документам, где адрес реально встретился."""
    table = ExtractedTable(table_id="t0", header=("a",), rows=(("1",),))
    ref = TableRef("t0", 1, 1)
    conn = _FakeConn(
        docs={
            "p1:hash1": {"doc_id": "p1:hash1", "tables": _tables_payload(table)},
            # У этого документа таблицы тоже есть, но адреса в срезе нет — читать его
            # значило бы платить за материал, который в промпт не пойдёт.
            "p1:hash2": {"doc_id": "p1:hash2", "tables": _tables_payload(table)},
        },
        chunk_rows=[
            {
                "chunk_id": "p1:dv_1:0",
                "doc_id": "p1:hash1",
                "table_ref": table_ref_to_json(ref),
            },
            {"chunk_id": "p1:dv_2:0", "doc_id": "p1:hash2", "table_ref": None},
        ],
    )
    chunks = [
        _chunk("p1:dv_1:0", 0, table_ref=ref),
        _chunk("p1:dv_2:0", 0),
    ]
    corpus = StoreToolCorpus(_FakeStore(chunks, conn=conn), "p1")

    addresses, tables = corpus.table_context([chunk.chunk_id for chunk in chunks])

    assert addresses == {"p1:dv_1:0": ref, "p1:dv_2:0": None}
    assert set(tables) == {("p1:dv_1", "t0")}
    assert len(conn.queries) == 2


def test_table_context_corrupt_address_is_unknown_not_an_error() -> None:
    """Порча ``table_ref`` — «адрес неизвестен» (``None``), а не исключение и не таблица."""
    conn = _FakeConn(
        docs={"p1:hash1": {"doc_id": "p1:hash1", "tables": "[]"}},
        chunk_rows=[
            {"chunk_id": "p1:dv_1:0", "doc_id": "p1:hash1", "table_ref": "{не json"}
        ],
    )
    corpus = StoreToolCorpus(
        _FakeStore([_chunk("p1:dv_1:0", 0)], conn=conn), "p1"
    )

    addresses, tables = corpus.table_context(["p1:dv_1:0"])

    assert addresses == {"p1:dv_1:0": None}
    assert tables == {}


def test_table_context_without_connection_is_empty() -> None:
    """Хранилище без соединения адресов не отдаёт: ``({}, {})`` — объявленный ответ."""
    corpus = StoreToolCorpus(_FakeStore([_chunk("p1:dv_1:0", 0)]), "p1")

    assert corpus.table_context(["p1:dv_1:0"]) == ({}, {})


def test_empty_product_id_is_refused() -> None:
    """Пустой продукт — отказ: окно обязано быть продуктовым (I17)."""
    try:
        StoreToolCorpus(_FakeStore([]), "  ")
    except ValueError as exc:
        assert "product_id" in str(exc)
    else:  # pragma: no cover - отказ обязателен
        raise AssertionError("пустой product_id обязан быть отвергнут")

def test_corpus_satisfies_port_contract() -> None:
    """Реализация структурно удовлетворяет ``ToolCorpusPort`` (окно, а не весь порт)."""
    corpus = StoreToolCorpus(_FakeStore([]), "p1")

    assert isinstance(corpus, ToolCorpusPort)
