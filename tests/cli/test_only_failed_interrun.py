"""``--only-failed``: межпрогонный work-set ремонта (09 §2.4/§13.5).

Проверяется ровно то, чего не было у сеансового фильтра: провал прошлого прогона читается
из ``state.db:url_state`` (``consecutive_failures > 0``), источник называется в ``notes``,
а набор **ремонта** отделён от набора **корней** — провал глубокой страницы не становится
seed-корнем.

Носители — ``SqliteStore``/``SqliteStateStore`` в памяти: сеанс краула и durable-состояние
пакета. Сеть/фазы не поднимаются: функция — чистый резолвер composition root.

Старое поведение фиксируется в тех же тестах явно: ``_filter_corpus_urls`` (сеансовый путь
команды ``crawl``) на том же входе не видит провала прошлого прогона и возвращает **полный**
набор — это и есть дыра, которую закрывает ``_only_failed_work_set``.
"""

from __future__ import annotations

import datetime as _dt

from core.cli.context import _filter_corpus_urls, _only_failed_work_set
from core.domain import (
    CrawlItemStatus,
    CrawlSessionItem,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline.crawl_session import default_session_id, open_session
from core.ports.state import UrlStateRow
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore

RUN_ID = "run-fixture"
SESSION_ID = default_session_id(RUN_ID)

ROOT = "https://docs.test/p1/"
DEEP = "https://docs.test/p1/guide/deep/page"
OTHER = "https://docs.test/p1/other"


def _store_with_run() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


def _failed_item(url: str, *, product_id: str = "p1") -> CrawlSessionItem:
    return CrawlSessionItem(
        session_id=SESSION_ID,
        product_id=product_id,
        url=url,
        status=CrawlItemStatus.FAILED,
        error_kind="source_unavailable",
    )


def _url_state(
    state: SqliteStateStore,
    url: str,
    *,
    failures: int,
    product_id: str = "p1",
    error_kind: str | None = "source_unavailable",
    etag: str | None = None,
) -> None:
    state.upsert_url_state(
        UrlStateRow(
            product_id=product_id,
            url=url,
            normalized_url=url,
            consecutive_failures=failures,
            last_error_kind=error_kind,
            etag=etag,
        )
    )


def test_url_state_failures_are_repaired_without_any_session() -> None:
    """Сеанса нет — источник ``url_state``: провал прошлого прогона не теряется."""
    with _store_with_run() as store, SqliteStateStore(":memory:") as state:
        _url_state(state, DEEP, failures=2, etag='W/"v1"')
        _url_state(state, OTHER, failures=0, error_kind=None)

        seeds, repair, notes = _only_failed_work_set(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
            state=state,
        )

    assert repair == {"p1": [DEEP]}
    # Корни — пространство scope, а не работа: только у продукта с ремонтом.
    assert seeds == {"p1": [ROOT]}
    joined = "\n".join(notes)
    assert "url_state" in joined
    assert "1 URL" in joined


def test_session_failures_take_precedence_over_url_state() -> None:
    """Есть FAILED текущего сеанса — он и есть источник; ``url_state`` не подмешивается."""
    with _store_with_run() as store, SqliteStateStore(":memory:") as state:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(_failed_item(OTHER))
        # Провал прошлого цикла, которого в текущем сеансе нет.
        _url_state(state, DEEP, failures=3)

        _seeds, repair, notes = _only_failed_work_set(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
            state=state,
        )

    assert repair == {"p1": [OTHER]}
    assert "сеанс" in "\n".join(notes)


def test_empty_session_falls_back_to_url_state() -> None:
    """Сеанс есть, но ``FAILED`` в нём нет — берётся durable-носитель, а не «обычный прогон»."""
    with _store_with_run() as store, SqliteStateStore(":memory:") as state:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        _url_state(state, DEEP, failures=1)

        _seeds, repair, notes = _only_failed_work_set(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
            state=state,
        )

    assert repair == {"p1": [DEEP]}
    assert "url_state" in "\n".join(notes)


def test_no_source_yields_empty_set_not_full_crawl() -> None:
    """Ни сеанса, ни состояния: work-set пуст, и это сказано — а не полный обход.

    Здесь же измеряется старое поведение: ``_filter_corpus_urls`` (сеансовый путь) на том
    же входе возвращает **все** объявленные URL, то есть «прогон по всем URL» — ровно то,
    что под ``--only-failed`` не является ремонтом.
    """
    with _store_with_run() as store:
        seeds, repair, notes = _only_failed_work_set(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
            state=None,
        )
        legacy_urls, legacy_notes = _filter_corpus_urls(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
        )

    assert seeds == {} and repair == {}
    assert "state.db" in "\n".join(notes)
    # Старое поведение: полный набор + пояснение «сеанса нет».
    assert legacy_urls == {"p1": [ROOT]}
    assert any("сеанса нет" in note for note in legacy_notes)


def test_declared_roots_are_kept_only_for_products_with_repairs() -> None:
    """Продукт без провалов не попадает ни в ремонт, ни в корни — он не краулится."""
    with _store_with_run() as store, SqliteStateStore(":memory:") as state:
        _url_state(state, DEEP, failures=1)

        seeds, repair, _notes = _only_failed_work_set(
            {"p1": [ROOT], "p2": ["https://docs.test/p2/"]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
            state=state,
        )

    assert repair == {"p1": [DEEP]}
    assert seeds == {"p1": [ROOT]}


def test_failures_of_undeclared_product_are_dropped_visibly() -> None:
    """Провал продукта, которого нет в ``--urls``, назван, а не потерян молча.

    Источник — сеанс (он несёт ``product_id`` любого продукта). Межпрогонное чтение идёт
    **по объявленным** продуктам: у порта нет перечисления продуктов, а у необъявленного нет
    ни объявленных хостов, ни пространства scope — ремонтировать его нечем.
    """
    with _store_with_run() as store, SqliteStateStore(":memory:") as state:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(_failed_item(DEEP))
        store.upsert_crawl_session_item(_failed_item("https://docs.test/p9/x", product_id="p9"))

        seeds, repair, notes = _only_failed_work_set(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
            state=state,
        )

    assert repair == {"p1": [DEEP]}
    assert seeds == {"p1": [ROOT]}
    assert "p9" not in repair
    assert "не объявлен в --urls" in "\n".join(notes)


def test_only_product_restricts_repair_set() -> None:
    """``--only-product`` + ``--only-failed``: ремонт только выбранного продукта."""
    with _store_with_run() as store, SqliteStateStore(":memory:") as state:
        _url_state(state, DEEP, failures=1)
        _url_state(state, "https://docs.test/p2/x", failures=1, product_id="p2")

        seeds, repair, _notes = _only_failed_work_set(
            {"p1": [ROOT], "p2": ["https://docs.test/p2/"]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product="p2",
            state=state,
        )

    assert repair == {"p2": ["https://docs.test/p2/x"]}
    assert seeds == {"p2": ["https://docs.test/p2/"]}


def test_only_product_without_only_failed_keeps_previous_volume_semantics() -> None:
    """``--only-product`` без ``--only-failed``: набор прежний, ремонта нет."""
    with _store_with_run() as store:
        seeds, repair, notes = _only_failed_work_set(
            {"p1": [ROOT], "p2": ["https://docs.test/p2/"]},
            store=store,
            session_id=SESSION_ID,
            only_failed=False,
            only_product="p1",
            state=None,
        )

    assert seeds == {"p1": [ROOT]}
    assert repair == {}
    assert notes == []


def test_deep_failure_is_repaired_instead_of_being_lost() -> None:
    """Провал глубокой страницы: старый путь терял его, новый ставит в ремонт.

    Старый фильтр лишь **пересекал** объявленные URL с провалами, поэтому провал страницы,
    которой нет среди объявленных корней, давал пустой work-set — «пере-фетчить нечего».
    Новый путь разделяет наборы: корни (пространство scope) и ремонт (известные URL).
    """
    with _store_with_run() as store:
        open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
        store.upsert_crawl_session_item(_failed_item(DEEP))

        legacy_urls, _legacy_notes = _filter_corpus_urls(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
        )
        seeds, repair, _notes = _only_failed_work_set(
            {"p1": [ROOT]},
            store=store,
            session_id=SESSION_ID,
            only_failed=True,
            only_product=None,
            state=None,
        )

    # Старое поведение: провал глубокой страницы не пересекается с корнем — пусто.
    assert legacy_urls == {}
    # Новое: ремонт — сама глубокая страница, корень остаётся только пространством scope.
    assert repair == {"p1": [DEEP]}
    assert seeds == {"p1": [ROOT]}
