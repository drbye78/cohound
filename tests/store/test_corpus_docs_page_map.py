"""Карта страниц в ``corpus_docs``: запись, чтение и правило ``COALESCE`` (O36, план 26).

Проверяется то, что нельзя проверить на уровне модели:

* карта переживает запись и чтение **как тот же кортеж** (JSON-колонка не теряет форму);
* ``NULL`` от писателя **без** карты не стирает уже записанную карту того же документа —
  иначе один и тот же текст, пришедший от HTML/docling/шва ``split``, терял бы номер
  страницы навсегда;
* порча колонки читается как «карты нет», а не как падение чтения корпуса.

Тесты нейтральны (§1.3), работают на ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import (
    CorpusDoc,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-page-map"

#: Карта из трёх страниц: 0..5 — первая, 8 — пустая вторая, 10..15 — третья.
PAGE_MAP = ((1, 0, 6), (2, 8, 8), (3, 10, 16))


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


def _doc(doc_id: str, **overrides: object) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": doc_id,
        "product_id": "p1",
        "url": f"https://docs.test/p1/{doc_id}",
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]


def test_page_map_roundtrips_as_the_same_tuple(store: SqliteStore) -> None:
    """Кортеж троек читается обратно тем же кортежем (JSON-носитель не меняет форму)."""
    doc = _doc("p1:pages", page_count=3, extracted_chars=16, page_map=PAGE_MAP)
    store.upsert_corpus_doc(doc)

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:pages"].page_map == PAGE_MAP
    assert read["p1:pages"] == doc


def test_missing_map_is_stored_as_null(store: SqliteStore) -> None:
    """Документ без карты пишется ``NULL``, а не пустой строкой."""
    store.upsert_corpus_doc(_doc("p1:html", extracted_chars=40))

    raw = store.conn.execute(
        "SELECT page_map FROM corpus_docs WHERE doc_id = ?", ("p1:html",)
    ).fetchone()
    assert raw["page_map"] is None
    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:html"].page_map is None


def test_null_does_not_erase_a_stored_map(store: SqliteStore) -> None:
    """Писатель без карты не стирает измеренную карту того же текста (``COALESCE``).

    ``doc_id = product_id:content_hash``, поэтому тот же текст приходит повторно от
    читателя без карты (HTML, docling, шов ``split``, переиспользование из набора чанков).
    Последний писатель здесь **не** прав: карта — единственный носитель номера страницы, и
    её потеря невосстановима тем же текстом.
    """
    store.upsert_corpus_doc(
        _doc("p1:same", page_count=3, extracted_chars=16, page_map=PAGE_MAP)
    )
    store.upsert_corpus_doc(_doc("p1:same", page_count=3, extracted_chars=16))

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:same"].page_map == PAGE_MAP


def test_new_map_replaces_the_previous_one(store: SqliteStore) -> None:
    """Обратное движение разрешено: другая карта — новое измерение, а не потеря."""
    store.upsert_corpus_doc(
        _doc("p1:same", page_count=2, extracted_chars=10, page_map=((1, 0, 4), (2, 6, 10)))
    )
    store.upsert_corpus_doc(
        _doc("p1:same", page_count=3, extracted_chars=16, page_map=PAGE_MAP)
    )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:same"].page_map == PAGE_MAP


def test_corrupt_column_reads_as_no_map(store: SqliteStore) -> None:
    """Порча колонки — «карты нет»: чтение корпуса не падает (отчёт обязан строиться)."""
    store.upsert_corpus_doc(_doc("p1:broken", extracted_chars=16))
    with store.conn:
        store.conn.execute(
            "UPDATE corpus_docs SET page_map = ? WHERE doc_id = ?",
            ("[[1, 0, 6], [2, 8", "p1:broken"),
        )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:broken"].page_map is None


def test_map_beyond_extracted_chars_is_rejected(store: SqliteStore) -> None:
    """Карта за пределами ``extracted_chars`` — ложная запись, а не «странная карта»."""
    with pytest.raises(ValueError, match="выходит за текст"):
        store.upsert_corpus_doc(
            _doc("p1:short", extracted_chars=9, page_map=PAGE_MAP)
        )
