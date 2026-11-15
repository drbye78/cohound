"""Таблицы в отчёте: ``outputs/tables.csv`` и сводка манифеста (O39 фаза 1 плана ``26``).

Предмет — **различимость двух фактов**: «таблицы прочитаны» и «на этой части корпуса
вопрос не задавался». Пока они не публикуются раздельно, отсутствие таблиц в отчёте
читается как «таблиц нет» — ровно та тихая подмена, ради видимости которой проект
существует (A3).

Источник строк — **персистентный** корпус, поэтому тесты идут через ``SqliteStore``:
сводка, посчитанная по памяти прогона, на resume называла бы другой факт.
"""

from __future__ import annotations

import csv
import datetime as _dt
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
from core.pipeline.table_provenance import (
    CSV_COLUMNS,
    collect_tables,
    human_line,
    tables_summary,
    write_tables_artifact,
)
from core.store.sqlite_store import SqliteStore

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
                products=["p1", "p2", "p3"],
                package="pkg",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
                ranking_policy=RankingPolicy.CAPABILITY_RANKED,
            ),
        )
        yield instance


def _doc(doc_id: str, product_id: str, **overrides: object) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": product_id,
        "url": f"https://docs.test/{product_id}/{doc_id}",
        "content_hash": doc_id,
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]


def _table(table_id: str, *, page_no: int | None, header: tuple[str, ...]) -> ExtractedTable:
    return ExtractedTable(
        table_id=table_id,
        page_no=page_no,
        header=header,
        rows=(tuple("x" for _ in header),),
    )


def _seed(store: SqliteStore) -> None:
    """Корпус трёх видов: с таблицами, «искал и не нашёл», «таблиц не даёт»."""
    store.upsert_corpus_doc(
        _doc(
            "d1",
            "p1",
            tables=(
                _table("t0", page_no=2, header=("a", "b")),
                _table("t1", page_no=None, header=()),
            ),
        )
    )
    store.upsert_corpus_doc(_doc("d2", "p2", tables=()))
    store.upsert_corpus_doc(_doc("d3", "p2", tables=None))


def test_csv_columns_and_order(store: SqliteStore, tmp_path: Path) -> None:
    """Строка на таблицу, колонки и порядок фиксированы; документов без таблиц нет."""
    _seed(store)

    path = write_tables_artifact(store, RUN_ID, tmp_path / "tables.csv")

    assert path is not None
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    assert list(rows[0]) == list(CSV_COLUMNS)
    assert [(row["product_id"], row["doc_id"], row["table_id"]) for row in rows] == [
        ("p1", "d1", "t0"),
        ("p1", "d1", "t1"),
    ]
    assert rows[0] == {
        "product_id": "p1",
        "doc_id": "d1",
        "table_id": "t0",
        "page_no": "2",
        "n_rows": "1",
        "n_cols": "2",
        "has_header": "1",
    }
    # Нет страницы — пустая ячейка, а не ноль; шапки нет — ``0``.
    assert rows[1]["page_no"] == ""
    assert rows[1]["has_header"] == "0"
    # Ширина без шапки — из первой строки данных (``_table`` даёт 0 столбцов при ``()``).
    assert rows[1]["n_cols"] == "0"


def test_no_tables_means_no_file(store: SqliteStore, tmp_path: Path) -> None:
    """Ни одной таблицы → файла нет: пустой CSV выдал бы отсутствие за факт."""
    store.upsert_corpus_doc(_doc("d1", "p1", tables=()))
    store.upsert_corpus_doc(_doc("d2", "p2", tables=None))

    assert collect_tables(store, RUN_ID) == []
    assert write_tables_artifact(store, RUN_ID, tmp_path / "tables.csv") is None


def test_summary_separates_measured_from_unmeasured(store: SqliteStore) -> None:
    """``documents_with_tables`` и ``documents_unmeasured`` считаются раздельно."""
    _seed(store)

    summary = tables_summary(store, RUN_ID, products=["p1", "p2", "p3"])

    assert summary is not None
    assert summary["tables"] == 2
    assert summary["documents_with_tables"] == 1
    assert summary["documents_unmeasured"] == 1, (
        "документ, где читатель таблиц их не даёт, — отдельный факт, не «таблиц нет»"
    )
    # Нули по продуктам плана — факт («у продукта таблиц нет»), а не отсутствие ключа.
    assert summary["by_product"] == {
        "p1": {"documents_with_tables": 1, "tables": 2, "documents_unmeasured": 0},
        "p2": {"documents_with_tables": 0, "tables": 0, "documents_unmeasured": 1},
        "p3": {"documents_with_tables": 0, "tables": 0, "documents_unmeasured": 0},
    }


def test_summary_is_none_without_corpus(store: SqliteStore) -> None:
    """Документов корпуса нет → ``None`` (не ноль: вопроса не было вовсе)."""
    assert tables_summary(store, RUN_ID, products=["p1"]) is None


def test_human_line_names_both_facts(store: SqliteStore) -> None:
    """Строка оператора называет оба числа; нулевые части не печатаются."""
    _seed(store)

    line = human_line(tables_summary(store, RUN_ID, products=["p1", "p2", "p3"]))

    assert line == "таблицы: 2 в 1 документах; без распознавания таблиц 1"


def test_human_line_omits_zero_parts() -> None:
    """Ноль таблиц и ноль неизмеренных — сказать нечего (``None``), а не «0 в 0»."""
    assert human_line(None) is None
    assert human_line({"tables": 0, "documents_with_tables": 0, "documents_unmeasured": 0}) is None
    assert human_line({"tables": 0, "documents_with_tables": 0, "documents_unmeasured": 4}) == (
        "без распознавания таблиц 4"
    )
    assert human_line({"tables": 3, "documents_with_tables": 2, "documents_unmeasured": 0}) == (
        "таблицы: 3 в 2 документах"
    )
