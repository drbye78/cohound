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
import random
import sqlite3
import threading
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

from core.adapters.remote_crawl_executor import EdgeCrawlExecutor
from core.domain.models import RateLimitSpec
from core.edge.config import EdgeRuntimeConfig
from core.edge.crawler_factory import crawler_factory
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
from core.edge.seam_schema import validate_crawl_request
from core.ports.crawl_executor import CrawlWorkItem
from core.ports.types import FetchPlan, RawPage
from core.util.rate_limit import RateLimiter

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


def FakeCrawler_factory(engine: str, *, robots: Any = None) -> FakeCrawler:
    """Module-level picklable фабрика для процесс-движков (вызывается в воркере).

    Принимает ``robots`` (§10): воркер-процесс строит политику сам и передаёт её фабрике.
    """
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


class BlockingThenFastCrawler:
    """Блокируется на **первом** фетче (до ``gate``), далее отдаёт мгновенно.

    Моделирует «одна волна держит воркер»: доказательство back-pressure (нет дедлока,
    ``status``/``collect`` обслуживаются, пока воркер заблокирован).
    """

    version = "blocking-then-fast-1"

    def __init__(self, gate: threading.Event, engine: str = "http") -> None:
        self.engine = engine
        self._gate = gate
        self._blocked = False
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        if not self._blocked:
            self._blocked = True
            self._gate.wait(timeout=10)
        return RawPage(
            url=url,
            content=b"<html>drained</html>",
            fetched_at=_FIXED_TIME,
            status_code=200,
            content_type="text/html",
            engine=self.engine,
        )


class RedirectCrawler:
    """Отдаёт ``RawPage`` с **финальным** URL, отличным от запрошенного (§6a)."""

    version = "redirect-1"

    def __init__(self, final_url: str, engine: str = "http") -> None:
        self.engine = engine
        self._final = final_url

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        return RawPage(
            url=self._final,
            content=b"<html>redirected</html>",
            fetched_at=_FIXED_TIME,
            status_code=200,
            content_type="text/html",
            engine=self.engine,
        )


class ConditionalCrawler:
    """Отдаёт подтверждение ``304`` с валидаторами и записывает полученный ``FetchPlan``."""

    version = "conditional-1"
    engine = "http"

    def __init__(self, content: bytes = b"cached-representation") -> None:
        self._content = content
        self.plans: list[FetchPlan] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.plans.append(plan)
        return RawPage(
            url=url,
            content=self._content,
            fetched_at=_FIXED_TIME,
            status_code=304,
            content_type="text/html",
            etag='"v1"',
            last_modified="Wed, 21 Oct 2026 07:28:00 GMT",
            not_modified=True,
        )


class RecordingRateLimiter(RateLimiter):
    """``RateLimiter`` со стаб-``sleep``/``rng``: записывает сны и ``(host, spec)``.

    Без реальных задержек: ``sleep`` — список-рекордер, ``rng`` — детерминированный
    ``random.Random(seed)`` (детерминизм §11.6).
    """

    def __init__(self, seed: int = 1234) -> None:
        self.sleeps: list[float] = []
        self.waits: list[tuple[str, Any]] = []
        super().__init__(sleep=self.sleeps.append, rng=random.Random(seed))

    def wait(self, host: str, spec: Any) -> float:
        self.waits.append((host, spec))
        return super().wait(host, spec)


class StubRobotsGate:
    """Стаб ``RobotsGate`` без сети: фиксированный вердикт + запись проверок (§10)."""

    def __init__(self, *, allowed: bool = True) -> None:
        self._allowed = allowed
        self.calls: list[str] = []

    def allowed(self, url: str) -> bool:
        self.calls.append(url)
        return self._allowed


class NoopGateway:
    """Заглушка ``ContentGateway`` (в тестах сериализации шва не вызывается)."""

    def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        raise AssertionError("gateway не должен вызываться в тесте сериализации")


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
        limiter: Any = None,
        limiter_factory: Any = None,
        robots_factory: Any = None,
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
            limiter=limiter,
            limiter_factory=limiter_factory,
            robots_factory=robots_factory,
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


# ---------------------------------------------------------------------------
# §9.10–9.12 — back-pressure/ёмкость/миграция (A2)
# ---------------------------------------------------------------------------


def test_edge_repeated_rounds_no_deadlock_no_orphan_jobs(make_scheduler: Any) -> None:
    """§3/§9.10: повторные волны не дедлочат, элементы/задания терминальны, ``_inflight``→0.

    Моделирует протокол раундов ядра: одна волна в полёте, следующая — только после
    терминала предыдущей. Первый фетч блокируется (edge обслуживает ``status``/``collect``
    своими потоками — циклического ожидания нет), затем отпускается.
    """
    gate = threading.Event()
    crawler = BlockingThenFastCrawler(gate)
    scheduler = make_scheduler(crawlers={"http": crawler}, workers={"http": 1}, queue_limit=8)
    observed_jobs: list[str] = []
    try:
        for round_no in range(3):
            items = [
                _item(f"p{round_no}{i}", f"https://x.test/r{round_no}/{i}") for i in range(3)
            ]
            result = scheduler.submit(_submit_payload(items=items, key=f"k{round_no}"))
            assert result["accepted"] is True
            observed_jobs.append(result["job_id"])
            # Первая волна держит единственного воркера: ``status`` всё равно отвечает.
            assert _wait_until(
                lambda jid=result["job_id"]: scheduler.status(jid)["running"] >= 1
            )
            gate.set()  # отпускаем заблокированный фетч (последующие не блокируются)
            state = _wait_terminal(scheduler, result["job_id"])
            assert state["status"] in (JOB_COMPLETED, JOB_PARTIAL)
    finally:
        gate.set()

    assert _wait_until(lambda: scheduler._inflight == 0)
    conn = scheduler._connect()
    non_terminal_items = conn.execute(
        "SELECT COUNT(*) FROM edge_job_items"
        " WHERE status NOT IN ('fetched', 'failed', 'cancelled')"
    ).fetchone()[0]
    assert non_terminal_items == 0
    non_terminal_jobs = conn.execute(
        "SELECT COUNT(*) FROM edge_jobs"
        " WHERE status NOT IN ('completed', 'partial', 'cancelled', 'failed')"
    ).fetchone()[0]
    assert non_terminal_jobs == 0
    # Все 9 страниц добыты (SUM(fetched) = ожидаемый набор многораундового сайта).
    total_fetched = conn.execute("SELECT COALESCE(SUM(fetched), 0) FROM edge_jobs").fetchone()[0]
    assert total_fetched == 9
    assert len(observed_jobs) == 3
    assert scheduler._inflight == 0


def test_edge_queue_limit_smaller_than_wave(make_scheduler: Any) -> None:
    """§3/§9.11: волна > ``queue_limit`` отвергается (``accepted=False``), затем бисекция.

    Edge — чистый back-pressure: отвергнутая волна НЕ создаёт джоб; ядро делит её
    пополам. Одиночный элемент (≤ ``queue_limit``) принимается.
    """
    gate = threading.Event()
    scheduler = make_scheduler(
        crawlers={"http": BlockingCrawler(gate)}, workers={"http": 1}, queue_limit=1
    )
    try:
        holding = scheduler.submit(
            _submit_payload(items=[_item("p0", "https://x.test/hold")], key="khold")
        )
        assert holding["accepted"] is True
        assert _wait_until(lambda: scheduler.status(holding["job_id"])["running"] >= 1)

        wave = [_item(f"p{i}", f"https://x.test/w{i}") for i in range(3)]
        rejected = scheduler.submit(_submit_payload(items=wave, key="kwave"))
        assert rejected == {"job_id": "", "accepted": False, "total": 0, "session_id": "s1"}
        # Отвергнутая волна не создала джоб (повтор после деления безопасен).
        conn = scheduler._connect()
        assert conn.execute(
            "SELECT COUNT(*) FROM edge_jobs WHERE idempotency_key = 'kwave'"
        ).fetchone()[0] == 0
    finally:
        gate.set()
    _wait_terminal(scheduler, holding["job_id"])

    # Бисекция: половина волны (один элемент) укладывается в ``queue_limit``.
    half = scheduler.submit(_submit_payload(items=[wave[0]], key="khalf"))
    assert half["accepted"] is True
    assert _wait_terminal(scheduler, half["job_id"])["status"] == JOB_COMPLETED


def test_edge_collect_returns_final_url(make_scheduler: Any) -> None:
    """§6a/§9.12: ``final_url`` персистится из ``RawPage.url`` и отдаётся в ``collect``."""
    requested = "https://x.test/start"
    final = "https://x.test/landing/"
    scheduler = make_scheduler(crawlers={"http": RedirectCrawler(final)})
    scheduler.submit(_submit_payload(items=[_item("p1", requested)]))
    assert _wait_terminal(scheduler, "job1")["status"] == JOB_COMPLETED

    item = scheduler.collect("job1")["items"][0]
    assert item["url"] == requested  # ключ элемента сеанса — ЗАПРОШЕННЫЙ URL
    assert item["final_url"] == final  # база ссылок/redirect-scope — финальный URL
    row = scheduler._connect().execute(
        "SELECT final_url FROM edge_job_items WHERE job_id = 'job1'"
    ).fetchone()
    assert row["final_url"] == final


def test_edge_conditional_fetch_roundtrip(make_scheduler: Any) -> None:
    """Валидаторы: submit → ``FetchPlan`` → ``RawPage`` → БД → ``collect``."""
    crawler = ConditionalCrawler()
    scheduler = make_scheduler(crawlers={"http": crawler})
    scheduler.submit(
        _submit_payload(
            items=[
                _item(
                    "p1",
                    "https://x.test/a",
                    if_none_match='"v1"',
                    if_modified_since="Wed, 21 Oct 2026 07:28:00 GMT",
                )
            ]
        )
    )
    assert _wait_terminal(scheduler, "job1")["status"] == JOB_COMPLETED

    # Валидаторы запроса дошли до плана, который получил краулер.
    assert len(crawler.plans) == 1
    assert crawler.plans[0].if_none_match == '"v1"'
    assert crawler.plans[0].if_modified_since == "Wed, 21 Oct 2026 07:28:00 GMT"

    item = scheduler.collect("job1")["items"][0]
    assert item["not_modified"] is True
    assert item["http_status"] == 304
    assert item["etag"] == '"v1"'
    assert item["last_modified"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    # 304 несёт закэшированные байты, а не пустое тело.
    assert base64.b64decode(item["raw"]["bytes_b64"]) == b"cached-representation"

    row = scheduler._connect().execute(
        "SELECT if_none_match, if_modified_since, etag, last_modified, not_modified"
        " FROM edge_job_items WHERE job_id = 'job1'"
    ).fetchone()
    assert row["if_none_match"] == '"v1"'
    assert row["if_modified_since"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert row["etag"] == '"v1"'
    assert row["last_modified"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert row["not_modified"] == 1


_OLD_SCHEMA = """
CREATE TABLE edge_jobs (
    job_id          TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status          TEXT NOT NULL,
    total           INTEGER NOT NULL DEFAULT 0,
    pending         INTEGER NOT NULL DEFAULT 0,
    running         INTEGER NOT NULL DEFAULT 0,
    fetched         INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    cancelled       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    error           TEXT,
    UNIQUE (run_id, session_id, idempotency_key)
);

CREATE TABLE edge_job_items (
    job_id       TEXT NOT NULL,
    product_id   TEXT NOT NULL,
    url          TEXT NOT NULL,
    status       TEXT NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    engine       TEXT,
    kind         TEXT,
    fetch        TEXT,
    extract      TEXT,
    strategy     TEXT,
    wait_for     TEXT,
    lang         TEXT,
    http_status  INTEGER,
    content_type TEXT,
    fetched_at   TEXT,
    content_hash TEXT,
    blob_ref     TEXT,
    error        TEXT,
    PRIMARY KEY (job_id, product_id, url)
);
"""


def test_edge_schema_migration_adds_final_url(make_scheduler: Any, tmp_path: Path) -> None:
    """§6a/§11.6.6: старая ``edge_jobs.db`` доливается колонками, строки целы.

    ``CREATE TABLE IF NOT EXISTS`` не расширяет существующую таблицу — миграция обязана
    добавить ``robots``/``final_url``/``rate_limit`` и колонки условного фетча
    идемпотентно, не трогая строки.
    """
    db_path = tmp_path / "edge_jobs.db"
    legacy = sqlite3.connect(str(db_path))
    try:
        legacy.executescript(_OLD_SCHEMA)
        with legacy:
            legacy.execute(
                "INSERT INTO edge_jobs (job_id, run_id, session_id, idempotency_key, status,"
                " total, pending, running, fetched, failed, cancelled, created_at, started_at)"
                " VALUES ('jold', 'r1', 's1', 'kold', 'completed', 1, 0, 0, 1, 0, 0, ?, ?)",
                (_FIXED_TIME, _FIXED_TIME),
            )
            legacy.execute(
                "INSERT INTO edge_job_items (job_id, product_id, url, status, content_hash)"
                " VALUES ('jold', 'p1', 'https://x.test/old', 'fetched', 'legacy-hash')"
            )
        legacy.commit()
    finally:
        legacy.close()

    scheduler = make_scheduler(state_dir=tmp_path)
    scheduler.start()  # recovery + миграция
    conn = scheduler._connect()
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(edge_job_items)")}
    assert {
        "final_url",
        "rate_limit",
        "robots",
        "if_none_match",
        "if_modified_since",
        "etag",
        "last_modified",
        "not_modified",
    } <= columns

    # Существующая строка цела (не пересоздана, значения сохранены).
    row = conn.execute(
        "SELECT url, status, content_hash FROM edge_job_items WHERE job_id = 'jold'"
    ).fetchone()
    assert row["url"] == "https://x.test/old"
    assert row["status"] == "fetched"
    assert row["content_hash"] == "legacy-hash"

    # Идемпотентность: повторная миграция — no-op (набор колонок не меняется).
    scheduler._migrate(conn)
    assert {str(r[1]) for r in conn.execute("PRAGMA table_info(edge_job_items)")} == columns


# ---------------------------------------------------------------------------
# §11.6 — pacing (A2c, контент-нейтрально)
# ---------------------------------------------------------------------------


def test_edge_paces_thread_engine_with_injected_limiter(make_scheduler: Any) -> None:
    """§11.6.1: ровно один сон ожидаемой длительности; ``host``/spec дошли до лимитера."""
    limiter = RecordingRateLimiter()
    scheduler = make_scheduler(limiter=limiter)
    scheduler.submit(
        _submit_payload(
            items=[_item("p1", "https://x.test/a", rate_limit={"min_s": 0.5, "max_s": 0.5})]
        )
    )
    assert _wait_terminal(scheduler, "job1")["status"] == JOB_COMPLETED
    assert limiter.sleeps == [0.5]
    assert limiter.waits == [("x.test", RateLimitSpec(min_s=0.5, max_s=0.5))]


def test_edge_no_pacing_when_rate_limit_absent(make_scheduler: Any) -> None:
    """§11.6.2: ``rate_limit`` отсутствует ⇒ ноль снов (байт-идентичность дефолта)."""
    limiter = RecordingRateLimiter()
    scheduler = make_scheduler(limiter=limiter)
    scheduler.submit(_submit_payload(items=[_item("p1", "https://x.test/a")]))
    assert _wait_terminal(scheduler, "job1")["status"] == JOB_COMPLETED
    assert limiter.sleeps == []
    assert limiter.waits == [("x.test", None)]


def test_edge_browser_not_double_paced(monkeypatch: pytest.MonkeyPatch) -> None:
    """§11.6.3: browser не пейсится дважды (голый адаптер + ровно один ``wait`` в обёртке)."""
    # ``crawler_factory`` строит BrowserCrawler голым ⇒ внутренний хук адаптера инертен.
    assert crawler_factory("browser")._rate_limiter is None

    import core.edge.scheduler as sched

    limiter = RecordingRateLimiter()
    stub = FakeCrawler(engine="browser", content=b"browser-body")
    monkeypatch.setattr(sched, "_WORKER_LIMITER", limiter)
    monkeypatch.setattr(sched, "_WORKER_CRAWLERS", {"browser": stub})

    plan = FetchPlan(rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0), robots=False)
    result = _process_fetch("browser", "https://x.test/a", plan)
    assert result["ok"] is True
    assert stub.calls == 1
    assert len(limiter.waits) == 1  # ровно один pacing в обёртке (не два)


def test_seam_submit_item_rate_limit_roundtrip(make_scheduler: Any) -> None:
    """§11.6.4: ``RateLimitSpec`` round-trip ядро→шов→edge; malformed → ``None`` (без падения)."""
    executor = EdgeCrawlExecutor(NoopGateway())  # type: ignore[arg-type]
    spec = RateLimitSpec(min_s=0.1, max_s=0.2)
    payload_item = executor._work_item_payload(
        CrawlWorkItem(product_id="p1", url="https://x.test/a", rate_limit=spec)
    )
    assert payload_item["rate_limit"] == {"min_s": 0.1, "max_s": 0.2}
    assert payload_item["robots"] is False

    scheduler = make_scheduler()
    normalized = scheduler._normalize_items([payload_item])
    assert normalized[0]["rate_limit"] == spec
    assert normalized[0]["robots"] is False

    # Сквозной round-trip через БД: submit → строка → ``_plan_for`` восстанавливает spec.
    scheduler.submit(_submit_payload(items=[payload_item]))
    assert _wait_terminal(scheduler, "job1")["status"] == JOB_COMPLETED
    row = scheduler._connect().execute(
        "SELECT * FROM edge_job_items WHERE job_id = 'job1'"
    ).fetchone()
    plan = scheduler._plan_for(row)
    assert plan.rate_limit == spec
    assert plan.robots is False

    # Malformed/отсутствующий вход мягко деградирует в ``None`` (submit не падает).
    for bad in (
        {"min_s": "x", "max_s": 1},
        {"min_s": 0.5},
        {"min_s": 0.5, "max_s": 0.1},
        {"min_s": -1, "max_s": 1},
        "nope",
        42,
        None,
    ):
        parsed = scheduler._normalize_items(
            [{"product_id": "p", "url": "https://x.test/z", "rate_limit": bad}]
        )
        assert parsed[0]["rate_limit"] is None


def test_seam_rate_limit_none_is_null() -> None:
    """§11.6.5: ``None`` → ``null``/отсутствие, схемо-валидно."""
    executor = EdgeCrawlExecutor(NoopGateway())  # type: ignore[arg-type]
    payload_item = executor._work_item_payload(
        CrawlWorkItem(product_id="p1", url="https://x.test/a")
    )
    assert payload_item["rate_limit"] is None
    request = {
        "run_id": "r1",
        "session_id": "s1",
        "idempotency_key": "k1",
        "profile": "public",
        "items": [payload_item],
    }
    assert validate_crawl_request("crawl/submit", request) == []


# ---------------------------------------------------------------------------
# §10 (A2b) — robots-политика на edge (условна от ``plan.robots``)
# ---------------------------------------------------------------------------


def test_edge_robots_gate_applied_only_when_plan_robots(
    make_scheduler: Any,
) -> None:
    """§10/A2b: гейт вешается на краулер, но применяется ТОЛЬКО при ``plan.robots``.

    ``robots=True`` → отказ (элемент ``failed``, гейт опрошен); ``robots=False`` →
    фетч без обращения к ``robots.txt`` (дефолтные прогоны байт-идентичны).
    """
    from core.adapters.crawl_http import HttpCrawler

    gate = StubRobotsGate(allowed=False)
    scheduler = make_scheduler(
        crawlers={"http": HttpCrawler(fetcher=lambda url: b"<html>ok</html>")},
        robots_factory=lambda user_agent: gate,
    )
    # robots=True: политика применяется (гейт отклоняет) → честный failed.
    scheduler.submit(
        _submit_payload(
            items=[_item("p1", "https://x.test/blocked", robots=True)], key="k1"
        )
    )
    assert _wait_terminal(scheduler, "job1")["failed"] == 1
    assert gate.calls == ["https://x.test/blocked"]

    # robots=False: тот же краулер фетчит, гейт НЕ опрашивается.
    scheduler.submit(
        _submit_payload(
            items=[_item("p2", "https://x.test/allowed", robots=False)], key="k2"
        )
    )
    assert _wait_terminal(scheduler, "job2")["status"] == JOB_COMPLETED
    assert gate.calls == ["https://x.test/blocked"]

