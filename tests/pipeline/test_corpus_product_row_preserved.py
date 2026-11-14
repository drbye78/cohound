"""Строка продукта описывает корпус прогона, а не объём одного исполнения (16 §3.2).

Дефект, который закрывают тесты (измерен 14.11.2026 на
``ru-pg-dbms-145ad66ca3f9``): ``run_id`` исполнялся несколько раз (краул, его повтор,
затем ``hound extract consume``). Проход без работы по продукту писал строку
``run_product`` с ``docs_total=0`` и ``error_kind=no_seeds``, а ``corpus_coverage`` —
``expected=0/observed=0/truncated=1``. В том же файле БД у этого продукта было
1 693 документа и 21 015 чанков: строка утверждала отсутствие корпуса, которое
опровергается артефактом, на который она ссылается.

Проверяется ровно наблюдённое:

* проход без обхода **не** обнуляет измеренную строку ``run_product`` и не объявляет
  ``no_seeds`` продукту, у которого документы прогона уже есть;
* запись ``corpus_coverage`` такого продукта сохраняет измеренные ``expected``/``observed``
  (не превращается в ``0/0/truncated=1``);
* факт «в этом исполнении обхода не было» назван отдельным машинным токеном в заметках
  фазы (``no_crawl_in_execution``), а не подменяет причину корпуса;
* контроль: продукт **без** корпуса по-прежнему получает ``blocked``/``no_seeds`` —
  честный исход, который правкой не отменяется.
"""

from __future__ import annotations

from test_corpus import (
    RUN_ID,
    URL_P1_A,
    SqliteStore,
    _build,
    _make_run,
)

from core.domain import RunProductStatus
from core.pipeline.corpus_coverage import NO_CRAWL_IN_EXECUTION

#: Второе исполнение того же ``run_id``: другой сеанс (у него нет элементов по продукту) —
#: так выглядит ``hound extract consume``, у которого по продукту нет ни URL, ни обхода.
SECOND_SESSION = f"{RUN_ID}:second-execution"


def _notes(result) -> list[str]:  # noqa: ANN001 — PhaseResult
    """Строковые заметки фазы (операторский канал ``results[].notes``)."""
    return [entry for entry in result.log_entries if isinstance(entry, str)]


def test_execution_without_crawl_keeps_measured_product_row(tmp_path) -> None:  # noqa: ANN001
    """Продукт с документами в БД прогона: проход без обхода не пишет нули и не ``no_seeds``.

    Первое исполнение обходит ``p1`` и записывает измеренную строку и покрытие. Второе
    (новый сеанс, ``urls={"p1": []}``) не обходит ничего — и обязано оставить оба
    носителя как есть: ``docs_total`` из артефакта, ``error_kind`` пустой, покрытие с
    прежними ``expected``/``observed``.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        first = _build(
            tmp_path,
            pages={URL_P1_A: b"one page"},
            urls={"p1": [URL_P1_A]},
            store=store,
            run_id=RUN_ID,
        )
        assert first.errors == []
        measured = {row["product_id"]: row for row in store.iter_run_products(RUN_ID)}
        measured_coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert measured["p1"]["docs_total"] == 1
        assert measured_coverage is not None
        assert measured_coverage.expected_sources == 1
        assert measured_coverage.observed_sources == 1
        assert measured_coverage.truncated is False

        second = _build(
            tmp_path,
            pages={},
            # Продукт в объёме прогона есть, обходить нечего — ровно то, что делает
            # ``hound extract consume`` (``urls = {pid: [] for pid in products}``).
            urls={"p1": []},
            store=store,
            run_id=RUN_ID,
            session_id=SECOND_SESSION,
        )

        rows = {row["product_id"]: row for row in store.iter_run_products(RUN_ID)}
        row = rows["p1"]
        # Числа — из артефакта прогона, а не из объёма этого прохода.
        assert row["docs_total"] == 1
        assert row["docs_new"] == 1
        # ``no_seeds`` — утверждение о корпусе; здесь корпус есть, и утверждение ложно.
        assert row["error_kind"] is None
        assert row["status"] != RunProductStatus.BLOCKED.value
        assert row["coverage_complete"] == measured_coverage.complete

        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.expected_sources == 1
        assert coverage.observed_sources == 1
        assert coverage.truncated is False
        assert coverage.reason is None

        # Факт прохода назван отдельно и не выдаётся за причину корпуса.
        notes = _notes(second)
        assert any(
            "p1" in note and NO_CRAWL_IN_EXECUTION in note for note in notes
        ), notes
        assert second.errors == [] or all("p1" in e for e in second.errors)
    finally:
        store.close()


def test_product_without_corpus_still_gets_no_seeds(tmp_path) -> None:  # noqa: ANN001
    """Контроль: пустой корпус — по-прежнему ``blocked``/``no_seeds``.

    Правка запрещает объявлять отсутствие корпуса тому, у кого он есть. Продукт без
    единого документа (пакет без URL/``--only-product`` по продукту без seed'ов) такого
    материала не имеет, и его строка обязана остаться прежней: ``blocked`` с причиной.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": []},
            store=store,
            run_id=RUN_ID,
        )
        rows = {row["product_id"]: row for row in store.iter_run_products(RUN_ID)}
        row = rows["p1"]
        assert row["status"] == RunProductStatus.BLOCKED.value
        assert row["error_kind"] == "no_seeds"
        assert row["docs_total"] == 0

        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.reason == "no_seeds"
        assert coverage.expected_sources == 0
        assert not any(NO_CRAWL_IN_EXECUTION in note for note in _notes(result))
    finally:
        store.close()
