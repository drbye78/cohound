"""R1a: durable очередь обхода и снятие корня переживают падение процесса.

Два измеренных дефекта, которые здесь закреплены тестом:

1. ``Frontier._queue``/``_seen`` жили **только в памяти**, а ``crawl_session_items``
   писался лишь с терминальным статусом. Измерено на прогоне
   ``ru-pg-dbms-3a737f13962f``: 30 207 строк и **ноль** ``pending`` при
   ``queue_remaining=584`` — «обнаружено, но не взято» после падения не существовало.
2. Снятие корня писалось только на **границе** обхода: прогон, убитый OOM посреди
   корня, не оставлял снятия вовсе, и ``bounded``-отсутствие (I2d) по корню было
   недоказуемо.

Нейтральные фикстуры (§1.3): ``p1``, ``docs.test``. Сеть не трогается.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core.domain import (
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
    RunConfig,
    RunMode,
)
from core.pipeline import corpus_drain
from core.pipeline.corpus_drain import (
    PENDING_FLUSH_EVERY,
    _PendingQueueBuffer,
    lift_pending_queue,
    pending_queue_for,
)
from core.pipeline.crawl_frontier import CrawlLimits, Frontier
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore

RUN_ID = "run-durable"
SESSION_ID = "s-durable"
ROOT = "https://docs.test/p1/"
TS = "2026-01-01T00:00:00+00:00"


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1"],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


@pytest.fixture()
def store() -> Any:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        s.upsert_crawl_session(
            CrawlSession(
                session_id=SESSION_ID,
                run_id=RUN_ID,
                profile="local",
                status=CrawlSessionStatus.CREATED,
                created_at=TS,
            )
        )
        yield s


def _pending(url: str, *, depth: int | None = None, **fields: Any) -> CrawlSessionItem:
    return CrawlSessionItem(
        session_id=SESSION_ID,
        product_id="p1",
        url=url,
        status=CrawlItemStatus.PENDING,
        depth=depth,
        **fields,
    )


def _frontier(**kwargs: Any) -> Frontier:
    return Frontier(ROOT, CrawlLimits(max_depth=5), **kwargs)


# --------------------------------------------------------------------------- #
# 1. Хук постановки у Frontier
# --------------------------------------------------------------------------- #


def test_hook_fires_only_on_accept_and_carries_revalidation_flag() -> None:
    """Хук видит ровно принятые URL и различает discovery/ревалидацию.

    Отказ (дубль, блэклист) хука не касается: он сообщает о **постановке**, а не о
    попытке. Признак ревалидации идёт тем же вызовом: это одна точка решения
    (ревалидация не расходует ``max_documents``), и второй хук каждая новая точка
    постановки могла бы забыть вызвать.
    """
    seen: list[tuple[int, str, bool]] = []
    frontier = _frontier(on_enqueue=lambda d, u, r: seen.append((d, u, r)))

    assert frontier.add_seed(ROOT) == "queued"
    assert frontier.discover(ROOT + "a", 2) is True
    assert frontier.add_revalidate_seed(ROOT + "b") == "queued"

    assert seen == [(0, ROOT, False), (2, ROOT + "a", False), (0, ROOT + "b", True)]

    # Дубль и блэклист — не постановка: хука нет.
    seen.clear()
    assert frontier.discover(ROOT + "a", 2) is False
    assert seen == []


def test_hook_failure_is_visible_and_does_not_change_the_decision() -> None:
    """Сбой хука не роняет обход и не меняет решение о постановке — но виден.

    Очередь — провенанс, а не условие измерения: отказ её записи не имеет права
    уронить краул. Но «потеряли очередь» не имеет права остаться тишиной.
    """
    calls: list[str] = []

    def boom(depth: int, url: str, is_revalidation: bool) -> None:
        calls.append(url)
        raise RuntimeError("store is gone")

    frontier = _frontier(on_enqueue=boom)
    assert frontier.add_seed(ROOT) == "queued"
    assert len(frontier) == 1, "URL остался в очереди несмотря на отказ хука"
    assert frontier.enqueue_hook_failures == 1
    assert frontier.enqueue_hook_errors
    assert "store is gone" in frontier.enqueue_hook_errors[0]


def test_hook_is_none_by_default() -> None:
    """Умолчание — поведение байт-в-байт прежнее: очередь только в памяти."""
    frontier = _frontier()
    assert frontier.on_enqueue is None
    assert frontier.add_seed(ROOT) == "queued"
    assert frontier.enqueue_hook_failures == 0


def test_with_limits_keeps_the_hook() -> None:
    """Клон корня (усечённые лимиты) пишет в ту же очередь: это тот же корень."""
    seen: list[tuple[int, str, bool]] = []
    clone = _frontier(on_enqueue=lambda d, u, r: seen.append((d, u, r))).with_limits(
        CrawlLimits(max_depth=1)
    )
    assert clone.discover(ROOT + "a", 1) is True
    assert seen == [(1, ROOT + "a", False)]


# --------------------------------------------------------------------------- #
# 2. Store: pending без затирания известного (carry-forward)
# --------------------------------------------------------------------------- #


def test_mark_crawl_pending_does_not_overwrite_terminal_row(store: SqliteStore) -> None:
    """``pending`` не понижает терминальную строку: carry-forward сохранён.

    Это не косметика: ``resume_retained`` считает бюджет по ``FETCHED``/``FAILED``, и
    понижение до ``pending`` начало бы ``max_documents`` заново (лишние страницы
    добывались бы тихо).
    """
    fetched = CrawlSessionItem(
        session_id=SESSION_ID,
        product_id="p1",
        url=ROOT + "known",
        status=CrawlItemStatus.FETCHED,
        attempts=2,
        depth=1,
        content_hash="deadbeef",
        raw_path="/tmp/known.bin",
        first_seen="2025-12-01T00:00:00Z",
    )
    store.upsert_crawl_session_item(fetched)

    store.mark_crawl_pending(
        SESSION_ID, [_pending(ROOT + "known", depth=3, first_seen="2026-01-01T00:00:00Z")]
    )

    row = store.get_crawl_session_item(SESSION_ID, "p1", ROOT + "known")
    assert row is not None
    assert row.status is CrawlItemStatus.FETCHED, "терминальный исход не понижается"
    assert row.content_hash == "deadbeef"
    assert row.raw_path == "/tmp/known.bin"
    assert row.attempts == 2
    assert row.first_seen == "2025-12-01T00:00:00Z"
    assert row.depth == 1, "глубина известной строки не переписывается"


def test_mark_crawl_pending_inserts_row_and_recounts(store: SqliteStore) -> None:
    """Новая ``pending``-строка получает глубину, а счётчики пересчитаны."""
    store.mark_crawl_pending(SESSION_ID, [_pending(ROOT + "a", depth=4)])

    row = store.get_crawl_session_item(SESSION_ID, "p1", ROOT + "a")
    assert row is not None
    assert row.status is CrawlItemStatus.PENDING
    assert row.depth == 4
    session = store.get_crawl_session(SESSION_ID)
    assert session is not None
    assert (session.total, session.pending) == (1, 1)


def test_mark_crawl_pending_recount_false_leaves_counters_stale(
    store: SqliteStore,
) -> None:
    """``recount=False`` не считает счётчики; ``recount=True`` с пустой пачкой — считает.

    Так очередь платит за пересчёт **на границе корня**, а не на каждой пачке
    (измерено: пересчёт на 30 207 строк = 16 мс против 383 мс на все вставки).
    """
    store.mark_crawl_pending(
        SESSION_ID, [_pending(ROOT + "a", depth=0)], recount=False
    )
    session = store.get_crawl_session(SESSION_ID)
    assert session is not None and session.total == 0, "счётчики не тронуты"

    store.mark_crawl_pending(SESSION_ID, [], recount=True)
    session = store.get_crawl_session(SESSION_ID)
    assert session is not None and session.total == 1, "граница корня пересчитала"


def test_mark_crawl_pending_moves_running_back_to_queue(store: SqliteStore) -> None:
    """``running`` (брошенная падением попытка) возвращается в очередь."""
    store.upsert_crawl_session_item(
        CrawlSessionItem(
            session_id=SESSION_ID,
            product_id="p1",
            url=ROOT + "a",
            status=CrawlItemStatus.RUNNING,
            depth=1,
        )
    )
    store.mark_crawl_pending(SESSION_ID, [_pending(ROOT + "a", depth=1)])
    row = store.get_crawl_session_item(SESSION_ID, "p1", ROOT + "a")
    assert row is not None and row.status is CrawlItemStatus.PENDING


# --------------------------------------------------------------------------- #
# 3. Буфер и подъём очереди
# --------------------------------------------------------------------------- #


class _RecordingStore:
    """Стор-заглушка: пишет пачки, а не строки (проверяем форму вызова)."""

    def __init__(self) -> None:
        self.calls: list[tuple[int, bool]] = []

    def mark_crawl_pending(
        self, session_id: str, items: list[CrawlSessionItem], *, recount: bool = True
    ) -> None:
        self.calls.append((len(items), recount))


def test_buffer_flushes_by_size_and_at_the_boundary() -> None:
    """Пачка пишется по размеру, хвост — на границе корня (иначе терялся бы)."""
    store = _RecordingStore()
    buffer = _PendingQueueBuffer(store, SESSION_ID, "p1")
    for i in range(PENDING_FLUSH_EVERY):
        buffer.add(i % 3, f"{ROOT}{i}", False)
    assert store.calls == [(PENDING_FLUSH_EVERY, False)], "пачка по размеру, без recount"

    buffer.add(0, ROOT + "tail", False)
    buffer.flush(final=True)
    assert store.calls[-1] == (1, True), "граница корня: хвост + пересчёт счётчиков"


def test_buffer_reports_store_failure_and_keeps_the_batch() -> None:
    """Отказ записи виден и не теряет пачку: следующий сброс повторит её."""

    class _Broken:
        def __init__(self) -> None:
            self.fail = True

        def mark_crawl_pending(
            self, session_id: str, items: list[CrawlSessionItem], *, recount: bool = True
        ) -> None:
            if self.fail:
                raise RuntimeError("disk full")

    store = _Broken()
    buffer = _PendingQueueBuffer(store, SESSION_ID, "p1")
    buffer.add(1, ROOT + "a", False)
    buffer.flush()
    assert buffer.flush_failures == 1
    assert "disk full" in (buffer.last_error or "")
    assert len(buffer) == 1, "пачка осталась в буфере"

    store.fail = False
    buffer.flush(final=True)
    assert buffer.flush_failures == 1, "счётчик отказов не обнуляется"
    assert len(buffer) == 0


def test_no_session_means_no_hook() -> None:
    """Без сеанса durable-краул выключен: буфера нет, стор не вызывается вовсе."""
    assert pending_queue_for(SimpleNamespace(session_id=None, store=object()), "p1") is None

    store = _RecordingStore()
    buffer = pending_queue_for(
        SimpleNamespace(session_id=SESSION_ID, store=store), "p1"
    )
    assert isinstance(buffer, _PendingQueueBuffer)


def test_lift_resumes_from_the_same_place(store: SqliteStore) -> None:
    """Новый ``Frontier`` продолжает с сохранённой глубины, а не с нуля.

    Ровно тот случай, который потерялся на ``ru-pg-dbms-3a737f13962f``: очередь
    (584 URL) жила в памяти и после падения начиналась заново.
    """
    store.mark_crawl_pending(
        SESSION_ID,
        [_pending(ROOT + "a", depth=2), _pending(ROOT + "b", depth=1)],
    )
    # Добытое — не очередь: подъём его не ставит (терминальный исход, не ``pending``).
    store.upsert_crawl_session_item(
        CrawlSessionItem(
            session_id=SESSION_ID,
            product_id="p1",
            url=ROOT + "done",
            status=CrawlItemStatus.FETCHED,
            depth=0,
        )
    )
    frontier = _frontier()
    lifted, rejected, examples = lift_pending_queue(
        frontier,
        store=store,
        session_id=SESSION_ID,
        product_id="p1",
        owner_root=lambda url: None,
        root_url=ROOT,
    )
    assert (lifted, rejected, examples) == (2, 0, [])
    # Порядок обслуживания — ключ (depth, url): сначала мелкая глубина.
    assert frontier.next() == (ROOT + "b", 1)
    assert frontier.next() == (ROOT + "a", 2)
    assert frontier.next() is None


def test_lift_names_rejected_urls(store: SqliteStore) -> None:
    """Отвергнутый подъёмом URL назван числом и примером — не тихий дроп."""
    outside = "https://other.test/p1/a"
    store.mark_crawl_pending(SESSION_ID, [_pending(outside, depth=1)])
    frontier = _frontier()
    lifted, rejected, examples = lift_pending_queue(
        frontier,
        store=store,
        session_id=SESSION_ID,
        product_id="p1",
        owner_root=lambda url: None,
        root_url=ROOT,
    )
    assert (lifted, rejected) == (0, 1)
    assert examples == [outside]
    assert len(frontier) == 0


def test_lift_skips_another_roots_pending(store: SqliteStore) -> None:
    """``pending`` другого корня того же продукта поднимает **его** корень."""
    store.mark_crawl_pending(SESSION_ID, [_pending(ROOT + "a", depth=1)])
    frontier = _frontier()
    lifted, rejected, _ = lift_pending_queue(
        frontier,
        store=store,
        session_id=SESSION_ID,
        product_id="p1",
        owner_root=lambda url: "https://docs.test/other/",
        root_url=ROOT,
    )
    assert (lifted, rejected) == (0, 0)


def test_lift_skips_own_paths_and_depth_none_is_zero(store: SqliteStore) -> None:
    """Свой путь постановки не перехватывается; глубина ``None`` читается как 0."""
    store.mark_crawl_pending(
        SESSION_ID,
        [_pending(ROOT, depth=None), _pending(ROOT + "seed", depth=0)],
    )
    frontier = _frontier()
    lifted, rejected, _ = lift_pending_queue(
        frontier,
        store=store,
        session_id=SESSION_ID,
        product_id="p1",
        owner_root=lambda url: None,
        root_url=ROOT,
        skip={ROOT},
    )
    assert (lifted, rejected) == (1, 0)
    assert frontier.next() == (ROOT + "seed", 0)


def test_lift_respects_max_depth(store: SqliteStore) -> None:
    """Глубина сверх ``max_depth`` — отказ (названный), а не тихая потеря."""
    store.mark_crawl_pending(SESSION_ID, [_pending(ROOT + "deep", depth=9)])
    frontier = Frontier(ROOT, CrawlLimits(max_depth=2))
    lifted, rejected, examples = lift_pending_queue(
        frontier,
        store=store,
        session_id=SESSION_ID,
        product_id="p1",
        owner_root=lambda url: None,
        root_url=ROOT,
    )
    assert (lifted, rejected) == (0, 1)
    assert examples == [ROOT + "deep"]


# --------------------------------------------------------------------------- #
# 4. Снятие корня: начало обхода и перезапись на границе
# --------------------------------------------------------------------------- #


def _capture_id(product_id: str, root_url: str, run_id: str) -> str:
    from core.pipeline.corpus_ingest import capture_id_for

    return capture_id_for(product_id, root_url, run_id)


def test_capture_start_row_is_overwritten_by_the_boundary_row() -> None:
    """Снятие «начало обхода» перезаписывается тем же ``capture_id``, а не вторым.

    Проверяется формула ``_CAPTURE_UPSERT``: ``complete`` берётся у последней записи,
    ``finished_at`` — тоже, а ``started_at`` **не** перезаписывается. Именно на этом
    держится порядок «сначала ``complete=0``, потом реальное».
    """
    from core.ports.state import CaptureRow

    capture_id = _capture_id("p1", ROOT, RUN_ID)
    with SqliteStateStore(":memory:") as state:
        state.record_capture(
            CaptureRow(
                capture_id=capture_id,
                product_id="p1",
                run_id=RUN_ID,
                root_url=ROOT,
                started_at=TS,
                finished_at=None,
                complete=False,
                observed_how="live",
                policy_digest="digest-1",
                space_json='{"scope": "root"}',
            )
        )
        captures = list(state.iter_captures("p1"))
        assert len(captures) == 1
        assert captures[0].complete is False
        assert captures[0].finished_at is None

        state.record_capture(
            CaptureRow(
                capture_id=capture_id,
                product_id="p1",
                run_id=RUN_ID,
                root_url=ROOT,
                started_at="2026-01-02T00:00:00Z",
                finished_at="2026-01-02T00:00:10Z",
                complete=True,
                observed_how="live",
                policy_digest="digest-1",
                space_json='{"scope": "root"}',
            )
        )
        captures = list(state.iter_captures("p1"))
        assert len(captures) == 1, "второе снятие не создаётся"
        assert captures[0].capture_id == capture_id
        assert captures[0].complete is True
        assert captures[0].finished_at == "2026-01-02T00:00:10Z"
        assert captures[0].started_at == TS, "начало обхода не перезаписывается"


def test_capture_written_before_crawl_so_a_crash_is_dated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Прогон, убитый посреди обхода, оставляет датированное снятие ``complete=0``.

    Это и был измеренный дефект: первый проход ``postgrespro.ru/docs`` убит OOM до
    записи снятия, возобновление увидело ``scheduled/24`` ⇒ ``reuse`` ⇒ корень
    **никогда** не получил снятия (потолок 0.7735 вместо 0.9381).
    """
    from tests.pipeline.test_corpus import (
        RUN_ID as CORPUS_RUN_ID,
    )
    from tests.pipeline.test_corpus import (
        URL_P1_A,
        _build,
        _make_run,
    )

    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("OOM посреди обхода")

    store = SqliteStore(":memory:")
    _make_run(store)
    monkeypatch.setattr(corpus_drain, "root_steps", _boom)
    with SqliteStateStore(":memory:") as state:
        try:
            with pytest.raises(RuntimeError, match="OOM"):
                _build(
                    tmp_path,
                    pages={URL_P1_A: b"page"},
                    urls={"p1": [URL_P1_A]},
                    store=store,
                    run_id=CORPUS_RUN_ID,
                    state=state,
                )
            captures = list(state.iter_captures("p1"))
            assert len(captures) == 1, "начало обхода записано до первого шага"
            capture = captures[0]
            assert capture.root_url == URL_P1_A
            assert capture.complete is False, "начатый обход не удостоверяет исчерпание"
            assert capture.finished_at is None
            assert capture.started_at, "дата касания корня есть"
        finally:
            store.close()


def test_successful_crawl_writes_one_capture_with_complete_true(
    tmp_path: Path,
) -> None:
    """Успешный обход перезаписывает то же снятие: одна строка, ``complete=True``."""
    from tests.pipeline.test_corpus import (
        RUN_ID as CORPUS_RUN_ID,
    )
    from tests.pipeline.test_corpus import (
        URL_P1_A,
        _build,
        _make_run,
    )

    store = SqliteStore(":memory:")
    _make_run(store)
    with SqliteStateStore(":memory:") as state:
        try:
            _build(
                tmp_path,
                pages={URL_P1_A: b"page"},
                urls={"p1": [URL_P1_A]},
                store=store,
                run_id=CORPUS_RUN_ID,
                state=state,
            )
            captures = list(state.iter_captures("p1"))
            assert len(captures) == 1, "начало и граница — одно снятие, не два"
            assert captures[0].complete is True
            assert captures[0].finished_at is not None
            assert captures[0].started_at
        finally:
            store.close()


def test_unfetched_queue_survives_in_the_store(tmp_path: Path) -> None:
    """«Поставлено и не взято» остаётся ``pending`` в сторе после обхода корня.

    Это и есть цена дефекта: на ``ru-pg-dbms-3a737f13962f`` очередь (584 URL) жила в
    памяти, а в ``crawl_session_items`` ``pending`` было **ноль** — следующий прогон
    начинал BFS заново. Здесь ``max_documents=2``: страница корня и одна ссылка добыты,
    две поставленные ссылки не взяты — и обязаны остаться ``pending`` с верной глубиной.
    """
    from core.config.domain_profiles import DomainProfile
    from core.pipeline.crawl_session import default_session_id
    from tests.pipeline.test_corpus import (
        RUN_ID as CORPUS_RUN_ID,
    )
    from tests.pipeline.test_corpus import (
        _build,
        _make_run,
    )

    root = "https://docs.test/p1/"
    page = b'<a href="/p1/a">a</a><a href="/p1/b">b</a><a href="/p1/c">c</a>'
    store = SqliteStore(":memory:")
    _make_run(store)
    session_id = default_session_id(CORPUS_RUN_ID)
    try:
        _build(
            tmp_path,
            pages={
                root: page,
                "https://docs.test/p1/a": b"a",
                "https://docs.test/p1/b": b"b",
                "https://docs.test/p1/c": b"c",
            },
            urls={"p1": [root]},
            domain_profiles={
                "docs.test": DomainProfile(max_depth=1, max_documents=2)
            },
            store=store,
            run_id=CORPUS_RUN_ID,
        )
        rows = {
            item.url: item
            for item in store.iter_crawl_session_items(session_id, product_id="p1")
        }
        pending = {
            url: row
            for url, row in rows.items()
            if row.status is CrawlItemStatus.PENDING
        }
        assert set(pending) == {
            "https://docs.test/p1/b",
            "https://docs.test/p1/c",
        }, "поставленное и не взятое пережило границу корня"
        assert all(row.depth == 1 for row in pending.values()), "глубина сохранена"
        assert rows[root].status is CrawlItemStatus.FETCHED, "добытое — терминально"
    finally:
        store.close()


def test_resumed_run_continues_from_the_saved_queue(tmp_path: Path) -> None:
    """Повторный проход поднимает ``pending`` и **добирает** их, а не начинает заново.

    Без подъёма второй проход начал бы BFS с корня: корень уже добыт (resume из
    retained-байтов), а ``b``/``c`` из очереди не существовали бы вовсе — ровно тот
    дефект, из-за которого ``queue_remaining=584`` терялся.
    """
    from core.config.domain_profiles import DomainProfile
    from core.pipeline.crawl_session import default_session_id
    from tests.pipeline.test_corpus import (
        RUN_ID as CORPUS_RUN_ID,
    )
    from tests.pipeline.test_corpus import (
        _build,
        _Crawler,
        _make_run,
    )

    root = "https://docs.test/p1/"
    page = b'<a href="/p1/a">a</a><a href="/p1/b">b</a><a href="/p1/c">c</a>'
    pages = {
        root: page,
        "https://docs.test/p1/a": b"a",
        "https://docs.test/p1/b": b"b",
        "https://docs.test/p1/c": b"c",
    }
    session_id = default_session_id(CORPUS_RUN_ID)
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path,
            pages=pages,
            urls={"p1": [root]},
            domain_profiles={"docs.test": DomainProfile(max_depth=1, max_documents=2)},
            store=store,
            run_id=CORPUS_RUN_ID,
        )
        first_calls = {
            item.url
            for item in store.iter_crawl_session_items(session_id, product_id="p1")
            if item.status is CrawlItemStatus.PENDING
        }
        assert len(first_calls) == 2

        crawler = _Crawler(pages)
        _build(
            tmp_path,
            pages=pages,
            urls={"p1": [root]},
            crawler=crawler,
            domain_profiles={"docs.test": DomainProfile(max_depth=1, max_documents=4)},
            store=store,
            run_id=CORPUS_RUN_ID,
        )
        rows = {
            item.url: item
            for item in store.iter_crawl_session_items(session_id, product_id="p1")
        }
        assert {url for url, row in rows.items() if row.status is CrawlItemStatus.PENDING} == set()
        assert all(
            row.status is CrawlItemStatus.FETCHED for row in rows.values()
        ), "поднятая очередь добрана, а не потеряна"
        fetched_urls = {url for url, _plan in crawler.calls}
        assert fetched_urls == first_calls, (
            "второй проход сходил ровно за поднятыми URL: корень взят из retained-байтов"
        )
    finally:
        store.close()
