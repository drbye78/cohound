"""Частичное извлечение в домене: инварианты ``CorpusDoc``/``ExtractedDoc`` (I2/I2c).

Признак частичности — **факт провенанса**, а не удобное поле: документ, прочитанный на
7 страницах из 2763, обязан отличаться от прочитанного полностью. Поэтому модель
запрещает три ложные записи: частичность без причины, причину без частичности и
счётчик прочитанных страниц без частичности. Тесты нейтральны (§1.3).
"""

from __future__ import annotations

import pytest

from core.domain import CorpusDoc
from core.ports.types import ExtractedDoc


def _doc(**overrides: object) -> CorpusDoc:
    base: dict[str, object] = {
        "doc_id": "p1:hash",
        "product_id": "p1",
        "url": "https://docs.test/p1/manual",
    }
    base.update(overrides)
    return CorpusDoc(**base)  # type: ignore[arg-type]

def test_corpus_doc_defaults_are_full_read() -> None:
    """Без признака документ читается как полный — поведение прежних строк неизменно."""
    doc = _doc()

    assert doc.partial is False
    assert doc.partial_reason is None
    assert doc.pages_converted is None
    assert doc.page_count is None
    assert doc.extracted_chars is None

def test_corpus_doc_partial_requires_reason() -> None:
    """``partial=True`` без причины — отказ: «частично» обязано называть, почему."""
    with pytest.raises(ValueError, match="partial_reason"):
        _doc(partial=True)
    with pytest.raises(ValueError, match="partial_reason"):
        _doc(partial=True, partial_reason="   ")

def test_corpus_doc_reason_without_partial_is_rejected() -> None:
    """Причина частичности без частичности — ложная провенанс-запись."""
    with pytest.raises(ValueError, match="partial=False"):
        _doc(partial_reason="docling: PARTIAL_SUCCESS")

def test_corpus_doc_pages_converted_without_partial_is_rejected() -> None:
    """Счётчик прочитанных страниц без частичности — тот же класс ложной записи."""
    with pytest.raises(ValueError, match="pages_converted"):
        _doc(pages_converted=7, page_count=2763)

def test_corpus_doc_page_counters_must_agree() -> None:
    """Прочитано больше, чем заявлено документом, — противоречие, а не «почти всё»."""
    with pytest.raises(ValueError, match="pages_converted больше page_count"):
        _doc(
            partial=True,
            partial_reason="docling: PARTIAL_SUCCESS",
            page_count=10,
            pages_converted=11,
        )
    with pytest.raises(ValueError, match="отрицательным"):
        _doc(partial=True, partial_reason="docling: PARTIAL_SUCCESS", pages_converted=-1)

def test_corpus_doc_accepts_measured_partial_document() -> None:
    """Живой случай: 7 страниц из 2763 — сохранённые числа, а не прилагательное."""
    doc = _doc(
        partial=True,
        partial_reason="docling: PARTIAL_SUCCESS, получено страниц 7 из 2763",
        page_count=2763,
        pages_converted=7,
        extracted_chars=79_810,
    )

    assert doc.partial is True
    assert (doc.pages_converted, doc.page_count) == (7, 2763)

def test_extracted_doc_rejects_pages_converted_without_partial() -> None:
    """Тот же инвариант на входе корпуса: счётчик существует только вместе с признаком."""
    with pytest.raises(ValueError, match="pages_converted"):
        ExtractedDoc(
            text="текст",
            content_hash="h",
            extractor_version="e",
            page_count=10,
            pages_converted=7,
        )

def test_extracted_doc_rejects_contradictory_page_counters() -> None:
    """Прочитано больше заявленного — отказ, а не «частичный успех больше документа»."""
    with pytest.raises(ValueError, match="больше page_count"):
        ExtractedDoc(
            text="текст",
            content_hash="h",
            extractor_version="e",
            page_count=5,
            partial=True,
            partial_reason="pdfium: страниц не прочитано",
            pages_converted=6,
        )
