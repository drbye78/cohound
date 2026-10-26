"""Артефакт частичного извлечения: ``outputs/partial_docs.csv`` и блок манифеста (I2/I2c).

Числом, а не прилагательным: сколько документов прочитано не целиком, сколько страниц
реально получено из заявленных, сколько символов извлечено. «Не измерено» печатается
пустой строкой/``None``, а не нулём — ноль здесь означал бы измеренный факт.
Тесты нейтральны (§1.3).
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import CorpusDoc
from core.pipeline.partial_docs import (
    CSV_COLUMNS,
    human_line,
    partial_docs_summary,
    write_partial_docs_artifact,
)

RUN_ID = "run-partial"

class _FakeStore:
    """Минимальный читатель корпуса: ровно тот метод, который нужен артефакту."""

    def __init__(self, docs: list[CorpusDoc]) -> None:
        self.docs = docs

    def iter_corpus_docs(self, run_id: str) -> Iterator[CorpusDoc]:
        if run_id != RUN_ID:
            raise ValueError(f"прогон не найден: {run_id!r}")
        yield from self.docs

def _partial_doc(
    doc_id: str = "p1:partial", product_id: str = "p1", **overrides: object
) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": product_id,
        "url": f"https://docs.test/{product_id}/manual.pdf",
        "partial": True,
        "partial_reason": "docling: PARTIAL_SUCCESS, получено страниц 7 из 2763",
        "page_count": 2763,
        "pages_converted": 7,
        "extracted_chars": 79_810,
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]

def _full_doc(doc_id: str = "p1:full", product_id: str = "p1") -> CorpusDoc:
    return CorpusDoc(
        doc_id=doc_id,
        product_id=product_id,
        url=f"https://docs.test/{product_id}/full",
    )

def test_artifact_writes_rows_for_partial_documents_only(tmp_path: Path) -> None:
    """В таблице — только частичные документы: полный там был бы шумом."""
    store = _FakeStore([_partial_doc(), _full_doc()])
    path = write_partial_docs_artifact(store, RUN_ID, tmp_path / "partial_docs.csv")

    assert path is not None
    with path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["doc_id"] for row in rows] == ["p1:partial"]
    assert tuple(rows[0]) == CSV_COLUMNS
    assert rows[0]["pages_converted"] == "7"
    assert rows[0]["pages_declared"] == "2763"
    assert rows[0]["chars"] == "79810"
    assert "PARTIAL_SUCCESS" in rows[0]["partial_reason"]

def test_artifact_is_not_created_without_partial_documents(tmp_path: Path) -> None:
    """Пустой список — не артефакт: файла нет, а не пустая таблица."""
    target = tmp_path / "partial_docs.csv"
    assert write_partial_docs_artifact(_FakeStore([_full_doc()]), RUN_ID, target) is None
    assert not target.exists()

def test_artifact_prints_unmeasured_numbers_as_empty(tmp_path: Path) -> None:
    """``None`` — «экстрактор числа не сообщил»; ноль был бы выдумкой (A3)."""
    store = _FakeStore(
        [
            _partial_doc(
                doc_id="p1:unknown-pages",
                page_count=None,
                pages_converted=None,
                extracted_chars=None,
                partial_reason="pdfium: страниц не прочитано 1 из 0 (страница 1)",
            )
        ]
    )
    path = write_partial_docs_artifact(store, RUN_ID, tmp_path / "partial_docs.csv")

    assert path is not None
    with path.open(encoding="utf-8") as handle:
        row = next(csv.DictReader(handle))
    assert row["pages_converted"] == ""
    assert row["pages_declared"] == ""
    assert row["chars"] == ""

def test_summary_aggregates_numbers_and_shows_planned_zeroes() -> None:
    """Блок манифеста: числа по продуктам + нули для продуктов плана (симметрия)."""
    store = _FakeStore([_partial_doc(), _partial_doc(doc_id="p2:partial", product_id="p2")])

    summary = partial_docs_summary(store, RUN_ID, products=["p1", "p2", "p3"])

    assert summary["documents"] == 2
    assert summary["by_product"] == {"p1": 1, "p2": 1, "p3": 0}
    assert summary["pages_converted"] == 14
    assert summary["pages_declared"] == 5526
    assert summary["chars"] == 159_620

def test_summary_without_partial_documents_is_zero_documents() -> None:
    """Прогон без частичных документов: документов 0, числа не выдумываются."""
    summary = partial_docs_summary(_FakeStore([_full_doc()]), RUN_ID, products=["p1"])

    assert summary["documents"] == 0
    assert summary["by_product"] == {"p1": 0}
    assert summary["pages_converted"] is None
    assert summary["pages_declared"] is None
    assert summary["chars"] is None

def test_summary_survives_unreadable_store() -> None:
    """Сбой чтения не роняет отчёт: пустой блок честнее исключения."""
    class _Broken:
        def iter_corpus_docs(self, run_id: str) -> Iterator[CorpusDoc]:
            raise RuntimeError("db closed")

    summary = partial_docs_summary(_Broken(), RUN_ID, products=["p1"])

    assert summary["documents"] == 0
    assert summary["by_product"] == {"p1": 0}

@pytest.mark.parametrize(
    ("converted", "declared", "expected"),
    [
        (7, 2763, "страниц прочитано 7 из 2763"),
        (7, None, "страниц прочитано 7"),
        (None, None, None),
    ],
)
def test_human_line_names_measured_numbers(
    converted: int | None, declared: int | None, expected: str | None
) -> None:
    """Строка оператора называет измеренные числа; нечем назвать — не называет."""
    summary = {
        "documents": 1,
        "by_product": {"p1": 1},
        "pages_converted": converted,
        "pages_declared": declared,
        "chars": None,
    }
    line = human_line(summary)

    assert line is not None
    assert "документов прочитано не целиком 1" in line
    assert "partial_extraction" in line
    if expected is not None:
        assert expected in line

def test_human_line_is_silent_without_partial_documents() -> None:
    """Нет частичных — нет строки: норма не превращается в замечание."""
    assert human_line({"documents": 0, "by_product": {"p1": 0}}) is None
    assert human_line(None) is None
