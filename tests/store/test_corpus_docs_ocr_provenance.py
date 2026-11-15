"""Провенанс OCR в ``corpus_docs``: запись, чтение и правило ``COALESCE`` (O25/O38a).

Проверяется то, чего нельзя проверить на уровне модели:

* исход пробы и постраничные счётчики переживают запись и чтение;
* писатель **без** измерения (переиспользование ``304``/набор чанков/``frozen``) приходит
  с ``None`` и не стирает уже измеренный исход — иначе наблюдение «проба не смогла» терялось
  бы на каждом повторном проходе;
* новый непустой исход перекрывает прежний (``unavailable`` → ``ok`` после починки движка):
  ``COALESCE`` защищает от потери, а не запрещает уточнение;
* ``CorpusDoc`` не принимает необъявленный исход и противоречивые счётчики.

Тесты нейтральны (§1.3), работают на ``:memory:``-БД.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import CorpusDoc, Focus, RankingPolicy, RunConfig, RunMode
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-ocr-provenance"


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


def test_ocr_provenance_roundtrips(store: SqliteStore) -> None:
    """Исход пробы и счётчики читаются обратно теми же числами."""
    doc = _doc(
        "p1:scan",
        page_count=5,
        extracted_chars=100,
        ocr_probe="ok",
        pages_ocr=3,
        pages_failed=1,
    )
    store.upsert_corpus_doc(doc)

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:scan"].ocr_probe == "ok"
    assert (read["p1:scan"].pages_ocr, read["p1:scan"].pages_failed) == (3, 1)
    assert read["p1:scan"] == doc


def test_absent_measurement_is_stored_as_null(store: SqliteStore) -> None:
    """Документ без измерения пишется ``NULL``, а не нулями.

    ``0`` здесь означал бы «распознавание применили и ни одна страница не дала текста» —
    наблюдение, которого не было.
    """
    store.upsert_corpus_doc(_doc("p1:html", extracted_chars=40))

    raw = store.conn.execute(
        "SELECT ocr_probe, pages_ocr, pages_failed FROM corpus_docs WHERE doc_id = ?",
        ("p1:html",),
    ).fetchone()
    assert raw["ocr_probe"] is None
    assert raw["pages_ocr"] is None
    assert raw["pages_failed"] is None


def test_writer_without_measurement_does_not_erase_ocr_provenance(
    store: SqliteStore,
) -> None:
    """Повторная запись без извлечения (``None``) не стирает измеренный исход.

    ``doc_id = product_id:content_hash``: тот же текст приходит от переиспользования, где
    экстракции не было. Потерять здесь ``unavailable`` значило бы объявить документ
    непроверенным — ровно та тихая потеря, ради видимости которой поле заведено.
    """
    store.upsert_corpus_doc(
        _doc("p1:scan", page_count=2, ocr_probe="unavailable", pages_ocr=0)
    )
    # Тот же документ, измерений этого цикла нет (переиспользование/frozen).
    store.upsert_corpus_doc(_doc("p1:scan", page_count=2))

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:scan"].ocr_probe == "unavailable"
    assert read["p1:scan"].pages_ocr == 0


def test_new_measurement_overwrites_previous_outcome(store: SqliteStore) -> None:
    """Починка движка уточняет исход: ``COALESCE`` защищает от потери, не от правки."""
    store.upsert_corpus_doc(
        _doc("p1:scan", page_count=2, ocr_probe="unavailable", pages_failed=1)
    )
    store.upsert_corpus_doc(
        _doc("p1:scan", page_count=2, ocr_probe="ok", pages_ocr=2, pages_failed=0)
    )

    read = {item.doc_id: item for item in store.iter_corpus_docs(RUN_ID)}
    assert read["p1:scan"].ocr_probe == "ok"
    assert (read["p1:scan"].pages_ocr, read["p1:scan"].pages_failed) == (2, 0)


def test_unknown_probe_value_is_rejected() -> None:
    """Необъявленный исход — не «другой механизм», а испорченный провенанс."""
    with pytest.raises(ValueError):
        _doc("p1:bad", ocr_probe="probed")


def test_negative_or_oversized_counters_are_rejected() -> None:
    """Отрицательный счётчик и счётчик больше ``page_count`` — ложная запись."""
    with pytest.raises(ValueError):
        _doc("p1:neg", pages_ocr=-1)
    with pytest.raises(ValueError):
        _doc("p1:big", page_count=2, pages_failed=3)


def test_counters_do_not_require_partial() -> None:
    """Счётчики живут без частичности: упавшая страница — независимый факт."""
    doc = _doc("p1:full", page_count=3, ocr_probe="ok", pages_ocr=1, pages_failed=1)
    assert doc.partial is False
