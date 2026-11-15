"""Агрегат исходов OCR в отчёте (``outputs/ocr_provenance.csv``, O38a/O25-остаток).

Проверяется то, ради чего артефакт существует: «страница прошла распознавание» и «проба не
смогла» видны **числом**, а не только строкой журнала фазы. Три свойства держатся отдельно:

* суммы складываются только по измеренным значениям (``None`` у документа не становится
  нулём, и «не измерено» не читается как «ни одной страницы»);
* распределение различает ``ok``/``unavailable``/``unsupported``/``unknown`` и **отдельно**
  «не применимо» (``None``) — это четыре разных наблюдения;
* худший исход берётся по порядку ``unavailable`` > ``unsupported`` > ``unknown`` > ``ok``,
  чтобы провал пробы не тонул в большинстве ``ok``.

Тесты нейтральны (§1.3): документы строятся как ``CorpusDoc``, БД — ``:memory:``.
"""

from __future__ import annotations

import csv
import datetime as _dt
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import CorpusDoc, Focus, RankingPolicy, RunConfig, RunMode
from core.pipeline.ocr_provenance import (
    CSV_COLUMNS,
    SCOPE_PRODUCT,
    SCOPE_RUN,
    collect_ocr_provenance,
    ocr_probe_by_product,
    run_ocr_probe,
    write_ocr_provenance_artifact,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-ocr-report"


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


def _doc(
    doc_id: str, product_id: str = "p1", **overrides: object
) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": product_id,
        "url": f"https://docs.test/{product_id}/{doc_id}",
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]


def _rows(store: SqliteStore) -> list[dict[str, object]]:
    docs = list(store.iter_corpus_docs(RUN_ID))
    return collect_ocr_provenance(docs)


def test_product_row_sums_measured_values(store: SqliteStore) -> None:
    """Суммы — по измеренным документам; «не измерено» не превращается в ноль."""
    store.upsert_corpus_doc(
        _doc("p1:a", page_count=4, ocr_probe="ok", pages_ocr=3, pages_failed=1)
    )
    store.upsert_corpus_doc(
        _doc("p1:b", page_count=2, ocr_probe="ok", pages_ocr=0, pages_failed=0)
    )
    # HTML-документ: распознавания нет вовсе — в суммы не входит.
    store.upsert_corpus_doc(_doc("p1:c", extracted_chars=100))

    product = next(row for row in _rows(store) if row["scope"] == SCOPE_PRODUCT)
    assert product["docs"] == 3
    assert product["docs_with_ocr_pages"] == 1
    assert product["pages_ocr"] == 3
    assert product["pages_failed"] == 1
    assert product["ocr_probe_ok"] == 2
    assert product["ocr_probe_unmeasured"] == 1
    assert product["worst_ocr_probe"] == "ok"


def test_unmeasured_product_reports_none_not_zero(store: SqliteStore) -> None:
    """Ни одного измерения — пустая ячейка, а не ноль (A3)."""
    store.upsert_corpus_doc(_doc("p2:html", product_id="p2", extracted_chars=10))

    product = next(
        row
        for row in _rows(store)
        if row["scope"] == SCOPE_PRODUCT and row["product_id"] == "p2"
    )
    assert product["pages_ocr"] is None
    assert product["pages_failed"] is None
    assert product["worst_ocr_probe"] is None


def test_worst_outcome_wins_over_majority(store: SqliteStore) -> None:
    """Провал пробы не тонет в большинстве ``ok`` — и в продукте, и в итоге прогона."""
    for index in range(3):
        store.upsert_corpus_doc(
            _doc(f"p1:ok{index}", page_count=2, ocr_probe="ok", pages_ocr=1)
        )
    store.upsert_corpus_doc(
        _doc("p1:bad", page_count=2, ocr_probe="unavailable", pages_ocr=0)
    )

    assert ocr_probe_by_product(store, RUN_ID) == {"p1": "unavailable"}
    assert run_ocr_probe(store, RUN_ID) == "unavailable"
    product = next(row for row in _rows(store) if row["scope"] == SCOPE_PRODUCT)
    assert product["worst_ocr_probe"] == "unavailable"
    assert product["ocr_probe_unavailable"] == 1


def test_run_row_is_last_and_aggregates_all_products(store: SqliteStore) -> None:
    """Итог прогона — последняя строка и складывается из тех же документов."""
    store.upsert_corpus_doc(
        _doc("p1:a", page_count=3, ocr_probe="ok", pages_ocr=2, pages_failed=1)
    )
    store.upsert_corpus_doc(
        _doc("p2:b", product_id="p2", page_count=2, ocr_probe="unknown", pages_ocr=1)
    )

    rows = _rows(store)
    assert [row["scope"] for row in rows] == [SCOPE_PRODUCT, SCOPE_PRODUCT, SCOPE_RUN]
    total = rows[-1]
    assert total["docs"] == 2
    assert total["pages_ocr"] == 3
    assert total["pages_failed"] == 1
    assert total["ocr_probe_unknown"] == 1
    assert total["worst_ocr_probe"] == "unknown"


def test_artifact_written_with_stable_columns(store: SqliteStore, tmp_path: Path) -> None:
    """Файл пишется с объявленными колонками; пустая ячейка — «не измерено»."""
    store.upsert_corpus_doc(
        _doc("p1:a", page_count=2, ocr_probe="unsupported", pages_ocr=1)
    )
    target = tmp_path / "outputs" / "ocr_provenance.csv"

    written = write_ocr_provenance_artifact(store, RUN_ID, target)

    assert written == target
    with target.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert tuple(rows[0].keys()) == CSV_COLUMNS
    assert rows[-1]["scope"] == SCOPE_RUN
    assert rows[-1]["ocr_probe_unsupported"] == "1"


def test_no_corpus_means_no_artifact(store: SqliteStore, tmp_path: Path) -> None:
    """Пустой корпус — не артефакт: файл выглядел бы как «искали и не нашли»."""
    target = tmp_path / "ocr_provenance.csv"
    assert write_ocr_provenance_artifact(store, RUN_ID, target) is None
    assert not target.exists()


def test_missing_run_is_not_an_error(store: SqliteStore) -> None:
    """Неизвестный прогон — «нечего публиковать», а не исключение отчёта."""
    assert run_ocr_probe(store, "run-unknown") is None
    assert ocr_probe_by_product(store, "run-unknown") == {}
