"""Продукт-скоуп наблюдаемости фазы корпуса: границы, причины, операторский канал.

Мотив — измеренный, не гипотетический. В живом прогоне 22.10 фаза корпуса шла 2.5 часа,
и всё это время сеанс содержал элементы трёх продуктов из четырёх: продукт-цикл строго
последователен (`products = sorted(url_map)`, дренаж одного продукта может идти часами),
поэтому «продукт ещё в очереди» и «продукт потерян» выглядели **одинаково** — ни одной
различимой строки ни в `corpus.jsonl`, ни в агрегатном прогрессе.

Проверяется три вещи, каждая — про честность наблюдения:

* **границы** — `product_start`/`product_end`/`product_skipped` на каждый продукт, включая
  нулевой выход, и живой колбэк `on_product` (канал `--follow`), срабатывающий **до**
  первого дренажа, а не в конце фазы;
* **причина** — `CorpusCoverage.reason` называет фактический путь к нулю источников
  (`no_seeds` / `all_seeds_rejected` / `no_urls_entry`), а не единую константу;
* **видимость** — структурные события доходят до оператора строкой в `results[].notes`
  (единственный канал, который CLI действительно печатает), а класс сбоя ячеек остаётся
  `no_sources` — словарь §4.1 не расширяется.
"""

from __future__ import annotations

from typing import Any

import pytest

from core.pipeline.corpus import (
    ZERO_SOURCE_ALL_SEEDS_REJECTED,
    ZERO_SOURCE_NO_SEEDS,
    ZERO_SOURCE_NO_URLS_ENTRY,
    _product_notes,
)


def _end(product_id: str, **over: Any) -> dict[str, Any]:
    """Структурное событие ``product_end`` с дефолтами (переопределяемыми)."""
    event: dict[str, Any] = {
        "event": "product_end",
        "product_id": product_id,
        "roots": 2,
        "seeds_declared": 2,
        "seeds_enqueued": 2,
        "session_items": 10,
        "fetched": 8,
        "failed": 2,
        "docs": 7,
        "elapsed_seconds": 12.5,
        "reason": None,
    }
    event.update(over)
    return event


def test_product_end_becomes_one_operator_note() -> None:
    """`product_end` → одна строка: величины, которых до этого не было видно нигде."""
    notes = _product_notes([_end("postgrespro")])

    assert len(notes) == 1
    note = notes[0]
    assert "postgrespro" in note
    # Ровно те счётчики, которые отвечают на «продукт съел бюджет?».
    assert "корней 2" in note
    assert "seed'ов поставлено 2 из 2" in note
    assert "элементов 10" in note
    assert "fetched 8" in note
    assert "failed 2" in note
    assert "документов 7" in note
    assert "12.5 с" in note
    # Норма без причины — не сбой, и в заметке нет выдуманной причины.
    assert "причина" not in note


def test_product_end_zero_source_carries_reason() -> None:
    """Нулевой выход называет причину — иначе «0 элементов» снова необъяснимо."""
    notes = _product_notes(
        [_end("tantor", session_items=0, fetched=0, failed=0, docs=0,
              roots=0, seeds_declared=0, seeds_enqueued=0,
              reason=ZERO_SOURCE_NO_SEEDS)]
    )

    assert len(notes) == 1
    assert "tantor" in notes[0]
    assert f"причина {ZERO_SOURCE_NO_SEEDS}" in notes[0]


def test_all_seeds_rejected_reason_is_distinguishable_from_no_seeds() -> None:
    """Две разные причины не сливаются в один текст: они различимы оператором."""
    no_seeds = _product_notes([_end("p1", reason=ZERO_SOURCE_NO_SEEDS)])[0]
    rejected = _product_notes(
        [_end("p1", reason=ZERO_SOURCE_ALL_SEEDS_REJECTED, session_items=0, docs=0)]
    )[0]

    assert ZERO_SOURCE_NO_SEEDS in no_seeds
    assert ZERO_SOURCE_ALL_SEEDS_REJECTED in rejected
    assert no_seeds != rejected


def test_product_skipped_note_names_the_filter_cause() -> None:
    """Продукт плана вне цикла — единственный след; заметка обязана быть.

    ``products`` выводится из ``url_map``, поэтому продукт без записи в ``urls`` в цикл
    не попадает вовсе: без этой строки он не оставил бы в фазе ничего.
    """
    notes = _product_notes(
        [{"event": "product_skipped", "product_id": "tantor", "reason": ZERO_SOURCE_NO_URLS_ENTRY}]
    )

    assert len(notes) == 1
    assert "tantor" in notes[0]
    assert "пропущен" in notes[0]
    assert "urls" in notes[0]


def test_product_start_is_not_duplicated_in_notes() -> None:
    """Граница «начат» нужна живьём (`--follow`), в отчёте она бесполезна — не дублируем.

    Иначе на 15 продуктов отчёт получал бы 30 строк, половина которых не несёт величин.
    """
    notes = _product_notes(
        [
            {"event": "product_start", "product_id": "p1", "roots": 2, "seeds_declared": 2},
            _end("p1"),
        ]
    )

    assert len(notes) == 1
    assert "p1" in notes[0]
    assert "элементов 10" in notes[0]


def test_notes_preserve_product_order() -> None:
    """Порядок — порядок дренажа (детерминизм отчёта и diff между прогонами)."""
    notes = _product_notes([_end("b"), _end("a")])

    assert [note.split("продукт ")[1].split(" —")[0] for note in notes] == ["b", "a"]


def test_unknown_event_is_ignored() -> None:
    """Неизвестное событие не превращается в заметку (расширение не ломает отчёт)."""
    assert _product_notes([{"event": "something_new", "product_id": "p1"}]) == []
    assert _product_notes([]) == []


@pytest.mark.parametrize(
    "reason",
    [ZERO_SOURCE_NO_SEEDS, ZERO_SOURCE_ALL_SEEDS_REJECTED, ZERO_SOURCE_NO_URLS_ENTRY],
)
def test_zero_source_reasons_are_stable_machine_strings(reason: str) -> None:
    """Значения — машинные строки: их читают отчёты и тесты, а не человек.

    Свободное поле ``CorpusCoverage.reason`` (не перечисление §4.1): смысл правил
    покрытия не меняется, поэтому это наблюдательная, а не методологическая ось (§13).
    """
    assert reason and reason.isascii()
    assert " " not in reason
