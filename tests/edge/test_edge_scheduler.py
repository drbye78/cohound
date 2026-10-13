"""Тесты edge-планировщика очереди краула — ``core/edge/scheduler.py`` (Phase 3.2).

Сеть отсутствует: краулеры — fake-адаптеры; ``tmp_path`` — ``state_dir``. Покрываются
обе модели исполнения: ``http`` — потоки (in-process fake), ``browser``/``scrapy`` —
процессы (реальный ``ProcessPoolExecutor`` с module-level picklable-фабрикой).
Проверяются: схема БД, идемпотентность submit, back-pressure, фетч+блоб, ``failed``,
recovery ``running→pending``, ``cancel``, ``collect`` (пагинация/base64/идемпотентность
курсора), счётчики/``status``, SSRF-отказ, чистый ``stop``. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import base64
import itertools
import pickle
import threading
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

from core.edge.config import EdgeRuntimeConfig
from core.edge.scheduler import (
    EdgeScheduler,
    ITEM_CANCELLED,
    ITEM_FETCHED,
    ITEM_PENDING,
    ITEM_RUNNING,
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_PARTIAL,
    _process_fetch,
    _process_worker_init,
)
from core.ports.types import FetchPlan, RawPage

_FIXED_TIME = "2026-01-01T00:00:00+00:00"
_CONTENT = b"<html>scheduled body</html>"


# ---------------------------------------------------------------------------
# Fake-краулеры (module-level фабрика picklable — для процесс-пула)
# ---------------------------------------------------------------------------


class FakeCrawler:
    """Стенд-краулер: детерминированный ``RawPage`` или исключение.

    ``engine`` задаётся, чтобы использоваться и в потоковом, и в процессном тесте.
    """

    version = "fake-crawler-1"

    def __init__(
        self,
        engine: str = "http",
        content: bytes = _CONTENT,
        *,
        raises: Exception | None = None,
    ) -> None:
        self.engine = engine
        self._content = content
        self._raises = raises
        self.calls = 0

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return RawPage(
            url=url,
            content=self._content,
            fetched_at=_FIXED_TIME,
            fetch_mode=plan.fetch,
            status_code=200,
            content_type="text/html; charset=utf-8",
            engine=self.engine,
        )


def FakeCrawler_factory(engine: str) -> FakeCrawler:
    """Module-level picklable фабрика для процесс-движков (вызывается в воркере)."""
    return FakeCrawler(engine=engine, content=f"proc:{engine}".encode())


class BlockingCrawler:
    """Краулер, блокирующийся на ``threading.Event`` (управление таймингом тестов)."""

    version = "blocking-1"

    def __init__(self, gate: threading.Event, engine: str = "http") -> None:
        self.engine = engine
        self._gate = gate
        self.calls = 0

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls += 1
        self._gate.wait(timeout=10)
        return RawPage(
            url=url,
            content=b"blocked",
            fetched_at=_FIXED_TIME,
            status_code=200,
            content_type="text/html",
            engine=self.engine,
        )


# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------


def _ids() -> Any:
    """Детерминированный генератор ``job_id`` (job1, job2, …)."""
    counter = itertools.count(1)
    return lambda: f"job{next(counter)}"


@pytest.fixture
def make_scheduler(tmp_path: Path) -> Iterator[Any]:
    """Фабрика планировщиков с уборкой (``stop``+``close``) на teardown."""
    created: list[EdgeScheduler] = []

    def _make(
        *,
        crawlers: Mapping[str, Any] | None = None,
        crawler_factory: Any = None,
        state_dir: Path | None = None,
        **overrides: Any,
    ) -> EdgeScheduler:
        config = EdgeRuntimeConfig(state_dir=str(state_dir or tmp_path), **overrides)
        if crawlers is None:
            crawlers = {"http": FakeCrawler()}
        scheduler = EdgeScheduler(
            config,
            crawlers,
            crawler_factory=crawler_factory,
            clock=lambda: _FIXED_TIME,
            id_factory=_ids(),
        )
        created.append(scheduler)
        return scheduler

    yield _make
    for scheduler in created:
        try:
            scheduler.stop(timeout=10)
        except Exception:  # noqa: BLE001 - уборка не должна ронять тест
            pass
        scheduler.close()


def _submit_payload(
    *, items: list[dict[str, Any]], key: str = "k1", run_id: str = "r1", session_id: str = "s1"
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "session_id": session_id,
        "idempotency_key": key,
        "profile": "public",
        "items": items,
    }


def _item(product_id: str, url: str, **axes: Any) -> dict[str, Any]:
    return {"product_id": product_id, "url": url, **axes}


def _wait_until(predicate: Any, timeout: float = 15.0) -> bool:
    """Ждёт выполнения предиката (poll), возвращает итог."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def _wait_terminal(scheduler: EdgeScheduler, job_id: str, timeout: float = 15.0) -> dict[str, Any]:
    """Ждёт терминального статуса задания; возвращает финальный ``status``."""
    state: dict[str, Any] = {}

    def _done() -> bool:
        state.update(scheduler.status(job_id))
        return state["status"] in ("completed", "partial", "cancelled", "failed")

    _wait_until(_done, timeout)
    return state


# ---------------------------------------------------------------------------
# Схема БД
# ---------------------------------------------------------------------------


def test_schema_created_idempotently(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    conn = scheduler._connect()
    names = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    assert {"edge_jobs", "edge_job_items"} <= names
    indexes = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
    }
    assert "idx_edge_job_items_job_status" in indexes
    # Повторный connect не пересоздаёт (идемпотентно) и не бросает.
    assert scheduler._connect() is conn
    # Уникальность идемпотентностного ключа объявлена на уровне схемы.
    ddl = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'edge_jobs'"
    ).fetchone()[0]
    assert "UNIQUE (run_id, session_id, idempotency_key)" in ddl


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


def test_submit_returns_shape_and_fetches(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    result = scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
    assert result == {"job_id": "job1", "accepted": True, "total": 1, "session_id": "s1"}
    state = _wait_terminal(scheduler, "job1")
    assert state["status"] == JOB_COMPLETED
    assert state["fetched"] == 1
    assert state["total"] == 1


def test_submit_idempotent_same_key(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    payload = _submit_payload(items=[_item("p1", "https://x.test/a")])
    first = scheduler.submit(payload)
    second = scheduler.submit(payload)
    assert first["job_id"] == second["job_id"] == "job1"
    # Без дублей элементов: ровно один ряд в edge_job_items.
    conn = scheduler._connect()
    count = conn.execute(
        "SELECT COUNT(*) FROM edge_job_items WHERE job_id = 'job1'"
    ).fetchone()[0]
    assert count == 1


def test_submit_back_pressure(make_scheduler: Any) -> None:
    gate = threading.Event()
    scheduler = make_scheduler(crawlers={"http": BlockingCrawler(gate)}, workers={"http": 1}, queue_limit=1)
    try:
        first = scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
        assert first["accepted"] is True
        # Дождаться, пока элемент A в работе (non-terminal) — тогда очередь занята.
        assert _wait_until(lambda: scheduler.status("job1")["pending"] + scheduler.status("job1")["running"] >= 1)
        second = scheduler.submit(
            _submit_payload(items=[_item("p2", "https://x.test/b")], key="k2")
        )
        assert second == {"job_id": "", "accepted": False, "total": 0, "session_id": "s1"}
    finally:
        gate.set()
    # После освобождения очереди задание принимается.
    _wait_terminal(scheduler, "job1")
    third = scheduler.submit(
        _submit_payload(items=[_item("p3", "https://x.test/c")], key="k3")
    )
    assert third["accepted"] is True
    assert third["job_id"] == "job2"


def test_submit_rejects_ssrf(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    with pytest.raises(ValueError):
        scheduler.submit(_submit_payload(items=[_item("p1", "http://169.254.169.254/")]))
    with pytest.raises(ValueError):
        scheduler.submit(_submit_payload(items=[_item("p1", "file:///etc/passwd")]))


def test_submit_rejects_bad_payload(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    with pytest.raises(ValueError):
        scheduler.submit(_submit_payload(items=[]))
    with pytest.raises(ValueError):
        scheduler.submit({"items": [_item("p1", "https://x.test/a")]})  # нет run_id и пр.


# ---------------------------------------------------------------------------
# Фетч / блоб / failed
# ---------------------------------------------------------------------------


def test_workers_fetch_and_store_blob(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
    state = _wait_terminal(scheduler, "job1")
    assert state["status"] == JOB_COMPLETED

    conn = scheduler._connect()
    row = conn.execute("SELECT * FROM edge_job_items WHERE job_id = 'job1'").fetchone()
    assert row["status"] == ITEM_FETCHED
    assert row["blob_ref"] == f"job1/{row['content_hash']}.bin"
    blob_path = Path(scheduler._config.resolved_blob_dir) / row["blob_ref"]
    assert blob_path.read_bytes() == _CONTENT


def test_failed_on_crawler_error(make_scheduler: Any) -> None:
    scheduler = make_scheduler(crawlers={"http": FakeCrawler(raises=RuntimeError("boom"))})
    scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
    state = _wait_terminal(scheduler, "job1")
    assert state["status"] == JOB_PARTIAL
    assert state["failed"] == 1
    conn = scheduler._connect()
    row = conn.execute("SELECT * FROM edge_job_items WHERE job_id = 'job1'").fetchone()
    assert row["status"] == "failed"
    assert "boom" in row["error"]
    assert row["blob_ref"] is None


def test_unconfigured_engine_fails_item(make_scheduler: Any) -> None:
    # Явно неизвестный движок сохраняется (без тихой подмены) → честный сбой элемента.
    scheduler = make_scheduler()
    scheduler.submit(
        _submit_payload(items=[_item("p1", "https://x.test/a", engine="teleport")])
    )
    state = _wait_terminal(scheduler, "job1")
    assert state["failed"] == 1


# ---------------------------------------------------------------------------
# Процесс-путь (browser: реальный ProcessPoolExecutor + picklable фабрика)
# ---------------------------------------------------------------------------


def test_process_engine_fetches_via_worker_process(make_scheduler: Any) -> None:
    scheduler = make_scheduler(
        crawlers={"http": FakeCrawler()},
        crawler_factory=FakeCrawler_factory,
        workers={"http": 1, "browser": 1},
    )
    assert scheduler._process_engines == ("browser",)
    scheduler.submit(
        _submit_payload(items=[_item("p1", "https://x.test/a", fetch="browser")])
    )
    state = _wait_terminal(scheduler, "job1")
    assert state["status"] == JOB_COMPLETED
    assert state["engines"] == ["browser"]

    collected = scheduler.collect("job1")
    item = collected["items"][0]
    assert item["engine"] == "browser"
    assert base64.b64decode(item["raw"]["bytes_b64"]) == b"proc:browser"


def test_process_factory_boundary_picklable() -> None:
    # Граница процессов: воркер-функции и фабрика должны переживать pickle.
    assert pickle.loads(pickle.dumps(_process_fetch)) is _process_fetch
    assert pickle.loads(pickle.dumps(_process_worker_init)) is _process_worker_init
    assert pickle.loads(pickle.dumps(FakeCrawler_factory)) is FakeCrawler_factory


# ---------------------------------------------------------------------------
# recovery
# ---------------------------------------------------------------------------


def test_recovery_resets_running_to_pending(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    conn = scheduler._connect()
    with conn:
        conn.execute(
            "INSERT INTO edge_jobs (job_id, run_id, session_id, idempotency_key, status,"
            " total, pending, running, fetched, failed, cancelled, created_at, started_at)"
            " VALUES ('jx', 'r1', 's1', 'kx', 'running', 1, 0, 1, 0, 0, 0, ?, ?)",
            (_FIXED_TIME, _FIXED_TIME),
        )
        conn.execute(
            "INSERT INTO edge_job_items (job_id, product_id, url, status)"
            " VALUES ('jx', 'p1', 'https://x.test/a', ?)",
            (ITEM_RUNNING,),
        )
    scheduler._recover(conn)
    row = conn.execute(
        "SELECT status FROM edge_job_items WHERE job_id = 'jx'"
    ).fetchone()
    assert row["status"] == ITEM_PENDING
    job = conn.execute("SELECT * FROM edge_jobs WHERE job_id = 'jx'").fetchone()
    assert job["pending"] == 1 and job["running"] == 0


def test_start_reclaims_stale_running_via_new_scheduler(make_scheduler: Any, tmp_path: Path) -> None:
    # Планировщик A оставляет «зависший» running-элемент (имитация прерванного процесса).
    a = make_scheduler()
    conn = a._connect()
    with conn:
        conn.execute(
            "INSERT INTO edge_jobs (job_id, run_id, session_id, idempotency_key, status,"
            " total, pending, running, fetched, failed, cancelled, created_at, started_at)"
            " VALUES ('j1', 'r1', 's1', 'k1', 'running', 1, 0, 1, 0, 0, 0, ?, ?)",
            (_FIXED_TIME, _FIXED_TIME),
        )
        conn.execute(
            "INSERT INTO edge_job_items (job_id, product_id, url, status)"
            " VALUES ('j1', 'p1', 'https://x.test/a', ?)",
            (ITEM_RUNNING,),
        )
    # Планировщик B стартует поверх той же БД: recovery → pending → фетч.
    b = make_scheduler(state_dir=tmp_path)
    b.start()
    state = _wait_terminal(b, "j1")
    assert state["status"] == JOB_COMPLETED
    assert state["fetched"] == 1


# ---------------------------------------------------------------------------
# cancel
# ---------------------------------------------------------------------------


def test_cancel_marks_non_terminal_items(make_scheduler: Any) -> None:
    gate = threading.Event()
    scheduler = make_scheduler(crawlers={"http": BlockingCrawler(gate)}, workers={"http": 1})
    try:
        scheduler.submit(
            _submit_payload(
                items=[_item("p1", "https://x.test/a"), _item("p2", "https://x.test/b")]
            )
        )
        assert _wait_until(lambda: scheduler.status("job1")["pending"] + scheduler.status("job1")["running"] >= 1)
        result = scheduler.cancel("job1")
        assert result == {"job_id": "job1", "cancelled": 2}
        state = scheduler.status("job1")
        assert state["status"] == JOB_CANCELLED
        assert state["cancelled"] == 2
        finished = scheduler._connect().execute(
            "SELECT finished_at FROM edge_jobs WHERE job_id = 'job1'"
        ).fetchone()[0]
        assert finished == _FIXED_TIME
    finally:
        gate.set()


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------


def test_collect_pagination_and_base64(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    scheduler.submit(
        _submit_payload(
            items=[
                _item("p1", "https://x.test/a"),
                _item("p2", "https://x.test/b"),
                _item("p3", "https://x.test/c"),
            ]
        )
    )
    state = _wait_terminal(scheduler, "job1")
    assert state["status"] == JOB_COMPLETED

    first = scheduler.collect("job1", limit=2)
    assert [i["product_id"] for i in first["items"]] == ["p1", "p2"]
    assert first["next_cursor"] is not None
    assert first["done"] is True  # все элементы уже терминальны
    for item in first["items"]:
        assert item["raw"]["size"] == len(_CONTENT)
        assert base64.b64decode(item["raw"]["bytes_b64"]) == _CONTENT

    second = scheduler.collect("job1", cursor=first["next_cursor"], limit=2)
    assert [i["product_id"] for i in second["items"]] == ["p3"]
    assert second["next_cursor"] is None


def test_collect_cursor_idempotent(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    scheduler.submit(
        _submit_payload(
            items=[_item("p1", "https://x.test/a"), _item("p2", "https://x.test/b")]
        )
    )
    _wait_terminal(scheduler, "job1")
    a = scheduler.collect("job1", limit=1)
    b = scheduler.collect("job1", limit=1)
    assert a == b
    assert a["next_cursor"] == "1"


def test_collect_base64_size_over_16kb(make_scheduler: Any) -> None:
    # base64-контент заведомо длиннее 16 KiB (специально ищем артефакты декодирования).
    big = b"x" * 20000
    scheduler = make_scheduler(crawlers={"http": FakeCrawler(content=big)})
    scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
    _wait_terminal(scheduler, "job1")
    item = scheduler.collect("job1")["items"][0]
    assert base64.b64decode(item["raw"]["bytes_b64"]) == big


# ---------------------------------------------------------------------------
# status / collect на неизвестном задании
# ---------------------------------------------------------------------------


def test_status_unknown_job_raises(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    with pytest.raises(ValueError):
        scheduler.status("nope")
    with pytest.raises(ValueError):
        scheduler.collect("nope")
    with pytest.raises(ValueError):
        scheduler.cancel("nope")


def test_status_shape_and_kinds(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    scheduler.submit(
        _submit_payload(
            items=[_item("p1", "https://x.test/a", kind="docs", engine="http")]
        )
    )
    _wait_terminal(scheduler, "job1")
    state = scheduler.status("job1")
    assert set(state) == {
        "job_id",
        "status",
        "total",
        "pending",
        "running",
        "fetched",
        "failed",
        "cancelled",
        "engines",
        "kinds",
    }
    assert state["kinds"] == ["docs"]


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


def test_stop_is_clean_and_idempotent(make_scheduler: Any) -> None:
    scheduler = make_scheduler()
    scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
    _wait_terminal(scheduler, "job1")
    scheduler.stop(timeout=10)
    assert scheduler._thread_pool is None
    assert scheduler._process_pool is None
    # Второй stop — no-op (не бросает).
    scheduler.stop(timeout=10)
    # Чтения после остановки продолжают работать.
    assert scheduler.status("job1")["status"] == JOB_COMPLETED


def test_stop_drains_running_items_to_pending(make_scheduler: Any) -> None:
    gate = threading.Event()
    scheduler = make_scheduler(crawlers={"http": BlockingCrawler(gate)}, workers={"http": 1})
    scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
    assert _wait_until(lambda: scheduler.status("job1")["running"] >= 1)
    gate.set()  # отпускаем, чтобы stop не ждал вечно
    scheduler.stop(timeout=10)
    conn = scheduler._connect()
    running = conn.execute(
        "SELECT COUNT(*) FROM edge_job_items WHERE status = ?", (ITEM_RUNNING,)
    ).fetchone()[0]
    assert running == 0


# ---------------------------------------------------------------------------
# C3 — сирота-блоб при отмене во время фетча
# ---------------------------------------------------------------------------


def test_no_orphan_blob_when_item_cancelled_during_fetch(make_scheduler: Any) -> None:
    """C3: блоб пишется только после подтверждённого перехода ``running→fetched``.

    Элемент отменяют, пока воркер ещё фетчит; по завершении guard-``UPDATE`` не находит
    ``running`` (0 строк) → блоб НЕ создаётся (нет сироты), элемент остаётся ``cancelled``.
    """
    gate = threading.Event()
    scheduler = make_scheduler(crawlers={"http": BlockingCrawler(gate)}, workers={"http": 1})
    try:
        scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
        assert _wait_until(lambda: scheduler.status("job1")["running"] >= 1)
        result = scheduler.cancel("job1")
        assert result == {"job_id": "job1", "cancelled": 1}
    finally:
        gate.set()  # отпускаем воркер уже ПОСЛЕ отмены
    # Ждём завершения callback-а (inflight → 0), чтобы гарантированно поймать запись.
    assert _wait_until(lambda: scheduler._inflight == 0)
    conn = scheduler._connect()
    row = conn.execute("SELECT * FROM edge_job_items WHERE job_id = 'job1'").fetchone()
    assert row["status"] == ITEM_CANCELLED
    assert row["blob_ref"] is None
    # Сироты блоба нет: под заданием не создано ни одного файла.
    blob_dir = Path(scheduler._config.resolved_blob_dir)
    assert not list(blob_dir.rglob("*.bin"))


# ---------------------------------------------------------------------------
# C4 — recovery/claim не трогают элементы терминальных заданий
# ---------------------------------------------------------------------------


def _insert_job(
    conn: Any,
    job_id: str,
    status: str,
    *,
    key: str,
    item_status: str,
    product_id: str = "p1",
    url: str = "https://x.test/a",
) -> None:
    """Вставляет задание + один элемент напрямую (детерминированная подготовка C4)."""
    with conn:
        conn.execute(
            "INSERT INTO edge_jobs (job_id, run_id, session_id, idempotency_key, status,"
            " total, pending, running, fetched, failed, cancelled, created_at, started_at)"
            " VALUES (?, 'r1', 's1', ?, ?, 1, 0, 1, 0, 0, 0, ?, ?)",
            (job_id, key, status, _FIXED_TIME, _FIXED_TIME),
        )
        conn.execute(
            "INSERT INTO edge_job_items (job_id, product_id, url, status) VALUES (?, ?, ?, ?)",
            (job_id, product_id, url, item_status),
        )


def test_recovery_does_not_revive_terminal_jobs(make_scheduler: Any) -> None:
    """C4: ``running``-элемент ТЕРМИНАЛЬНОГО задания не восстанавливается в ``pending``."""
    scheduler = make_scheduler()
    conn = scheduler._connect()
    _insert_job(conn, "jcancel", "cancelled", key="kc", item_status=ITEM_RUNNING)
    _insert_job(conn, "jlive", "running", key="kl", item_status=ITEM_RUNNING)

    scheduler._recover(conn)

    assert (
        conn.execute("SELECT status FROM edge_job_items WHERE job_id = 'jcancel'").fetchone()[0]
        == ITEM_RUNNING  # не ожил
    )
    assert (
        conn.execute("SELECT status FROM edge_job_items WHERE job_id = 'jlive'").fetchone()[0]
        == ITEM_PENDING  # восстановлен
    )


def test_claim_skips_items_of_terminal_jobs(make_scheduler: Any) -> None:
    """C4: элементы терминального задания никогда не клеймятся (не воскресают)."""
    scheduler = make_scheduler()
    conn = scheduler._connect()
    _insert_job(conn, "jcancel", "cancelled", key="kc", item_status=ITEM_PENDING)
    _insert_job(conn, "jlive", "created", key="kl", item_status=ITEM_PENDING)

    scheduler.start()  # recovery + dispatch: клеймится только нетерминальное задание
    state = _wait_terminal(scheduler, "jlive")
    assert state["status"] == JOB_COMPLETED

    terminal_item = conn.execute(
        "SELECT status FROM edge_job_items WHERE job_id = 'jcancel'"
    ).fetchone()[0]
    assert terminal_item == ITEM_PENDING  # элемент отменённого задания не клеймлен


# ---------------------------------------------------------------------------
# C5a — защита от реэнтрантного ``_dispatch``
# ---------------------------------------------------------------------------


def test_dispatch_is_not_reentrant(make_scheduler: Any) -> None:
    """C5a: повторный вход в ``_dispatch`` при активном диспетчере ничего не клеймит."""
    scheduler = make_scheduler()
    scheduler.start()  # пулы собраны, очередь пуста
    conn = scheduler._connect()
    _insert_job(conn, "jz", "created", key="kz", item_status=ITEM_PENDING)

    # Имитируем реэнтрантный вызов из done-callback: флаг уже поднят «внешним» циклом.
    scheduler._dispatching = True
    scheduler._dispatch()
    assert (
        conn.execute("SELECT status FROM edge_job_items WHERE job_id = 'jz'").fetchone()[0]
        == ITEM_PENDING  # активен другой диспетчер — новый ничего не взял
    )

    # Снимаем флаг — элемент нормально диспетчеризуется и добывается.
    scheduler._dispatching = False
    scheduler._dispatch()
    state = _wait_terminal(scheduler, "jz")
    assert state["status"] == JOB_COMPLETED


# ---------------------------------------------------------------------------
# C5b — ``stop(timeout)`` не висит на заблокированном воркере, а предупреждает
# ---------------------------------------------------------------------------


def test_stop_honors_timeout_and_warns(
    make_scheduler: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """C5b: ``stop(timeout)`` ограниченно ждёт и пишет предупреждение, не висит молча."""
    gate = threading.Event()
    scheduler = make_scheduler(crawlers={"http": BlockingCrawler(gate)}, workers={"http": 1})
    try:
        scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
        assert _wait_until(lambda: scheduler.status("job1")["running"] >= 1)

        start = time.monotonic()
        scheduler.stop(timeout=0.3)
        elapsed = time.monotonic() - start

        assert elapsed < 5.0  # не ждём заблокированный воркер бесконечно
        err = capsys.readouterr().err
        assert "scheduler.stop()" in err and "не завершились" in err
    finally:
        gate.set()  # отпускаем воркер, чтобы поток завершился
