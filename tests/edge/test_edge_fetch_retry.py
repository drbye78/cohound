"""Повторы фетча на **split**-пути: тот же помощник и та же константа, что у локального.

Дефект (долг назван в ``docs/architecture/22-crawl-pacing-plan.md`` §10). Пока повторы
``5xx`` жили в движке Scrapy, под ``split`` они были (вне пейсинга). Коммит ``9a8440a``
вынес их в слой фетча — и edge-планировщик, который под ``split`` зовёт ``crawler.fetch``
напрямую, остался без повтора вовсе: одиночный ``5xx`` не повторялся ни разу, тогда как
локальный путь повторял. Профиль пилота — ``local``, поэтому прогон это не задевало.

Проверяется:

1. ``5xx`` повторяется сверх первой попытки, и **каждый** повтор берёт свой слот
   лимитера (число резервов = 1 + число попыток);
2. терминальные классы (``CrawlNotFound``/``CrawlRefused``/``CrawlUnavailable``/
   ``CrawlPayloadTooLarge``/``CrawlUrlError``) не повторяются;
3. ``408``/``429`` повторяются;
4. число попыток то же, что в локальном пути: одна константа
   (:data:`core.util.fetch_retry.FETCH_RETRY_TIMES`), у edge своего литерала нет;
5. повторы доезжают до провенанса элемента: ``edge_job_items.attempts`` = claim + повторы;
6. edge зовёт **тот же** помощник и **тот же** классификатор, что локальный путь.

Данные категорийно-нейтральны (§1.3): ``a.test``/``p1``.
"""

from __future__ import annotations

import random
import time
from typing import Any

import pytest

import core.edge.scheduler as sched
import core.util.fetch_retry as fetch_retry
from core.domain import RateLimitSpec
from core.edge.config import EdgeRuntimeConfig
from core.edge.scheduler import JOB_COMPLETED, JOB_PARTIAL, EdgeScheduler, _paced_fetch
from core.pipeline.corpus_ingest import FETCH_RETRY_TIMES as LOCAL_FETCH_RETRY_TIMES
from core.ports import crawl_errors
from core.ports.crawl_errors import (
    CrawlError,
    CrawlNotFound,
    CrawlPayloadTooLarge,
    CrawlRefused,
    CrawlUnavailable,
    CrawlUrlError,
    is_transient_fetch_error,
)
from core.ports.types import FetchPlan, RawPage
from core.util.fetch_retry import FETCH_RETRY_TIMES
from core.util.rate_limit import RateLimiter

HOST = "a.test"
URL = f"https://{HOST}/page"
TS = "2026-01-01T00:00:00+00:00"
CONTENT = b"<html>ok</html>"
#: Нулевой объявленный интервал: резерв (слот) есть, сна нет — тест меряет слоты, не время.
ZERO_SPEC = RateLimitSpec(min_s=0.0, max_s=0.0)


def _page(url: str = URL) -> RawPage:
    """Успешный ответ транспорта."""
    return RawPage(url=url, content=CONTENT, fetched_at=TS, status_code=200)


class ScriptedCrawler:
    """Краулер по сценарию: отдаёт исходы по очереди и считает вызовы ``fetch``.

    Вызовов ``fetch`` ровно столько, сколько попыток сделал слой фетча, — это и есть
    число обращений к хосту (повтор движка выключен: ``RETRY_ENABLED=False``).
    """

    engine = "http"

    def __init__(self, *outcomes: Any) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class CountingLimiter(RateLimiter):
    """Боевой лимитер с фейковым сном: считает **резервы** (слоты origin'а)."""

    def __init__(self) -> None:
        self.reserves: list[str] = []
        super().__init__(sleep=lambda _seconds: None, rng=random.Random(1234))

    def _reserve(self, host: str, spec: RateLimitSpec) -> tuple[float, int]:
        self.reserves.append(host)
        return super()._reserve(host, spec)


def _plan() -> FetchPlan:
    """План элемента с нулевым (но объявленным) пейсингом."""
    return FetchPlan(rate_limit=ZERO_SPEC)


def _submit_payload(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "run_id": "r1",
        "session_id": "s1",
        "idempotency_key": "k1",
        "profile": "public",
        "items": items,
    }


def _wait_terminal(scheduler: EdgeScheduler, job_id: str, timeout: float = 15.0) -> dict[str, Any]:
    """Ждёт терминального статуса задания (poll) и возвращает его снимок."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = scheduler.status(job_id)
        if snapshot["status"] in (JOB_COMPLETED, JOB_PARTIAL):
            return snapshot
        time.sleep(0.02)
    return scheduler.status(job_id)


def _item_attempts(scheduler: EdgeScheduler, job_id: str, url: str) -> int:
    """``attempts`` элемента из durable-записи задания (провенанс повторов)."""
    conn = scheduler._connect()
    row = conn.execute(
        "SELECT attempts FROM edge_job_items WHERE job_id = ? AND url = ?",
        (job_id, url),
    ).fetchone()
    assert row is not None
    return int(row["attempts"])


# ---------------------------------------------------------------------------
# Общий помощник: одна реализация и одна константа на оба пути
# ---------------------------------------------------------------------------


def test_edge_uses_the_local_path_retry_helper_and_predicate() -> None:
    """Edge зовёт **тот же** цикл и **тот же** классификатор, что локальный путь."""
    assert sched.fetch_with_paced_retries is fetch_retry.fetch_with_paced_retries
    assert sched.is_transient_fetch_error is crawl_errors.is_transient_fetch_error


def test_retry_count_is_one_constant_for_both_paths() -> None:
    """Число попыток задаёт одна константа: локальный адрес — тот же объект.

    Своей копии литерала у edge нет вовсе (``scheduler`` её не импортирует), поэтому
    «два числа, разошедшихся молча» здесь невозможно по построению.
    """
    assert LOCAL_FETCH_RETRY_TIMES is FETCH_RETRY_TIMES
    assert not hasattr(sched, "FETCH_RETRY_TIMES")


# ---------------------------------------------------------------------------
# (а) 5xx: повтор и свой слот на каждую попытку
# ---------------------------------------------------------------------------


def test_5xx_is_retried_and_each_attempt_takes_its_own_slot() -> None:
    """``503`` → ``200``: две попытки и **два** резерва лимитера (по одному на попытку)."""
    crawler = ScriptedCrawler(CrawlError("HTTP 503", http_status=503), _page())
    limiter = CountingLimiter()

    result = _paced_fetch(lambda: crawler.fetch(URL, _plan()), URL, _plan(), limiter)

    assert result["ok"] is True
    assert result["page"].content == CONTENT
    assert result["retries"] == 1
    assert crawler.calls == 2
    assert limiter.reserves == [HOST, HOST]


def test_exhausted_retries_take_one_slot_per_attempt_and_fail_visibly() -> None:
    """Все попытки ``5xx``: резервов 1 + ``FETCH_RETRY_TIMES``, исход — ошибка, не «успех»."""
    crawler = ScriptedCrawler(
        *[CrawlError("HTTP 500", http_status=500) for _ in range(1 + FETCH_RETRY_TIMES)]
    )
    limiter = CountingLimiter()

    result = _paced_fetch(lambda: crawler.fetch(URL, _plan()), URL, _plan(), limiter)

    assert result["ok"] is False
    assert "CrawlError" in result["error"]
    assert result["retries"] == FETCH_RETRY_TIMES
    assert crawler.calls == 1 + FETCH_RETRY_TIMES
    assert limiter.reserves == [HOST] * (1 + FETCH_RETRY_TIMES)


# ---------------------------------------------------------------------------
# (б) терминальные классы не повторяются
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "terminal",
    [
        CrawlNotFound("HTTP 404", http_status=404),
        CrawlRefused("robots.txt запретил путь", refusal_reason="robots_disallow"),
        CrawlUnavailable("движок не поднялся"),
        CrawlPayloadTooLarge("слишком большой ответ", http_status=200),
        CrawlUrlError("SSRF: цель недопустима"),
    ],
    ids=lambda exc: type(exc).__name__,
)
def test_terminal_outcomes_are_not_retried(terminal: BaseException) -> None:
    """Терминальный класс отвечает сразу: повтор адресовался бы тому, что ответило отказом."""
    crawler = ScriptedCrawler(terminal, _page())
    limiter = CountingLimiter()

    result = _paced_fetch(lambda: crawler.fetch(URL, _plan()), URL, _plan(), limiter)

    assert result["ok"] is False
    assert type(terminal).__name__ in result["error"]
    assert result["retries"] == 0
    assert crawler.calls == 1
    assert limiter.reserves == [HOST]


# ---------------------------------------------------------------------------
# (в) 408/429 — «попробуй позже», а не отказ
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [408, 429])
def test_408_and_429_are_retried(status: int) -> None:
    """``408``/``429`` просят «позже»; слот лимитера и есть это «позже»."""
    crawler = ScriptedCrawler(CrawlError(f"HTTP {status}", http_status=status), _page())
    limiter = CountingLimiter()

    result = _paced_fetch(lambda: crawler.fetch(URL, _plan()), URL, _plan(), limiter)

    assert result["ok"] is True
    assert result["retries"] == 1
    assert crawler.calls == 2
    assert limiter.reserves == [HOST, HOST]


@pytest.mark.parametrize("bad", [500, 503, 504])
def test_server_errors_from_the_taxonomy_are_transient(bad: int) -> None:
    """``5xx`` — транзиентный ответ (классификатор порта, не литерал планировщика)."""
    assert is_transient_fetch_error(CrawlError("x", http_status=bad)) is True


# ---------------------------------------------------------------------------
# (г/д) провенанс: повторы durable в элементе задания, исход — как у локального пути
# ---------------------------------------------------------------------------


def test_retries_are_recorded_in_the_item_attempts_counter(tmp_path: Any) -> None:
    """Под ``split`` повтор виден в провенансе элемента: ``attempts`` = claim + повторы.

    Один claim (выдача воркеру) и один пейсящийся повтор дают ``attempts == 2`` — то же
    число фактических попыток, что у локального пути (``1 + FETCH_RETRY_TIMES`` при
    исчерпании). Пишет родитель под локом — единственный писатель SQLite.
    """
    crawler = ScriptedCrawler(CrawlError("HTTP 503", http_status=503), _page())
    limiter = CountingLimiter()
    config = EdgeRuntimeConfig(state_dir=str(tmp_path))
    scheduler = EdgeScheduler(
        config,
        {"http": crawler},
        clock=lambda: TS,
        id_factory=lambda: "job1",
        limiter=limiter,
    )
    try:
        scheduler.submit(
            _submit_payload(
                [{"product_id": "p1", "url": URL, "rate_limit": {"min_s": 0.0, "max_s": 0.0}}]
            )
        )
        snapshot = _wait_terminal(scheduler, "job1")
        assert snapshot["status"] == JOB_COMPLETED
        assert crawler.calls == 2
        assert limiter.reserves == [HOST, HOST]
        assert _item_attempts(scheduler, "job1", URL) == 2
    finally:
        scheduler.stop(timeout=10)
        scheduler.close()


def test_failed_retries_are_still_recorded_in_provenance(tmp_path: Any) -> None:
    """Исчерпание повторов: элемент ``failed``, но факт повторов не теряется."""
    crawler = ScriptedCrawler(
        *[CrawlError("HTTP 502", http_status=502) for _ in range(1 + FETCH_RETRY_TIMES)]
    )
    limiter = CountingLimiter()
    config = EdgeRuntimeConfig(state_dir=str(tmp_path))
    scheduler = EdgeScheduler(
        config,
        {"http": crawler},
        clock=lambda: TS,
        id_factory=lambda: "job1",
        limiter=limiter,
    )
    try:
        scheduler.submit(
            _submit_payload(
                [{"product_id": "p1", "url": URL, "rate_limit": {"min_s": 0.0, "max_s": 0.0}}]
            )
        )
        snapshot = _wait_terminal(scheduler, "job1")
        assert snapshot["status"] == JOB_PARTIAL
        assert snapshot["failed"] == 1
        assert crawler.calls == 1 + FETCH_RETRY_TIMES
        assert limiter.reserves == [HOST] * (1 + FETCH_RETRY_TIMES)
        assert _item_attempts(scheduler, "job1", URL) == 1 + FETCH_RETRY_TIMES
    finally:
        scheduler.stop(timeout=10)
        scheduler.close()


def test_schema_has_no_separate_retries_column(tmp_path: Any) -> None:
    """Повторы живут в существующем поле: отдельной колонки нет, версия схемы не меняется.

    Так долг не превращается в смену опубликованного формата: ``edge_jobs.db`` — это
    операционное состояние, и новая колонка означала бы новую версию с видимым отказом
    старым файлам. Проверка фиксирует выбор, а не отсутствие возможности.
    """
    config = EdgeRuntimeConfig(state_dir=str(tmp_path))
    scheduler = EdgeScheduler(config, {"http": ScriptedCrawler(_page())}, clock=lambda: TS)
    try:
        conn = scheduler._connect()
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(edge_job_items)").fetchall()
        }
        assert "attempts" in columns
        assert "retries" not in columns
        declared = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        assert declared is not None
        assert str(declared["value"]) == sched.EDGE_JOBS_SCHEMA_VERSION
    finally:
        scheduler.close()