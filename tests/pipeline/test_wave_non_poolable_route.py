"""F4 (17 §6): не-poolable (браузерный) фетч исполняется **своим** потоком, не драйвером.

Зачем это существует. ``BrowserCrawler`` водит единственную ``_page`` без лока, поэтому у
не-poolable маршрута обязан быть **один** поток-владелец. Прежний путь исполнял этот фетч
на потоке драйвера (``fetch_and_commit``): пока полоса одна, это незаметно, но при
чередовании корней один браузерный фетч остановил бы все полосы.

Что проверяется. Отправка кладёт не-poolable задание в отдельный пул из одного потока
(маршрут помечен в :class:`WaveRequest`), фетч исполняется **не** на вызывающем потоке,
`max_active == 1` (поток-владелец один), а коммит исхода остаётся за вызывающим потоком —
то есть мутации не уходят в чужой поток (T5).

Там же — пин **пулевого** пейсинга: ``local_fetch`` делает ровно **один** резерв на фетч
(``wait_reserved`` — это и pacing, и метка попытки). Inline-путь
(``fetch_and_commit``) пейсится иначе, и до этой проверки удвоение резервов в пуле
проходило незамеченным: единственный тест пейсинга мерил именно inline-ветку.
"""

from __future__ import annotations

import datetime as _dt
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any

from core.pipeline.corpus_drain import (
    WaveOutcomes,
    _await_wave,
    _dispatch_wave,
    _WaveEntry,
)
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import LocalFetch, local_fetch
from core.ports.types import FetchMode, FetchPlan, RateLimitSpec, RawPage
from core.util.rate_limit import RateLimiter


class _ThreadRecordingCrawler:
    """Фейковый «браузер»: запоминает поток каждого фетча и число одновременных."""

    supports_browser = True
    engine = "browser"

    def __init__(self) -> None:
        self.threads: list[int] = []
        self.max_active = 0
        self._active = 0
        self._lock = threading.Lock()

    def fetch(self, url: str, plan: FetchPlan) -> RawPage:
        with self._lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
            self.threads.append(threading.get_ident())
        try:
            # Порядок не важен; задержка нужна, чтобы перекрытие было наблюдаемым.
            time.sleep(0.02)
            return RawPage(
                url=url,
                content=b"<html>browser</html>",
                fetched_at="2026-01-01T00:00:00+00:00",
                fetch_mode=plan.fetch,
                lang=None,
                status_code=200,
                content_type="text/html",
                engine="browser",
            )
        finally:
            with self._lock:
                self._active -= 1


def _desc(
    crawler: _ThreadRecordingCrawler,
    url: str,
    *,
    poolable: bool,
    fetch: FetchMode = FetchMode.BROWSER,
    rate_limit: RateLimitSpec | None = None,
) -> LocalFetch:
    """Дескриптор живого фетча: ``poolable=False`` — браузерный маршрут."""
    return LocalFetch(
        pid="p1",
        wu=url,
        depth=0,
        fetch_plan=FetchPlan(fetch=fetch, rate_limit=rate_limit),
        active=crawler,
        engine_value="browser",
        prior_item=None,
        resume=None,
        browser_blocked=False,
        poolable=poolable,
        host="browser.test",
        scope_host="browser.test",
        enforce_redirect_scope=True,
        terminal=False,
        reject=None,
    )


def _wave(
    entries: list[_WaveEntry],
    *,
    non_poolable_pool: ThreadPoolExecutor,
    fetch_pool: ThreadPoolExecutor,
) -> tuple[Any, WaveOutcomes]:
    request = _dispatch_wave(
        entries,
        pool=fetch_pool,
        non_poolable_pool=non_poolable_pool,
        local_fetch=partial(local_fetch, limiter=RateLimiter()),
    )
    return request, _await_wave(request, timings=_CorpusStageTimings())


def test_browser_route_runs_on_its_own_single_thread() -> None:
    """Три не-poolable задания — один поток-владелец, и это **не** вызывающий поток."""
    crawler = _ThreadRecordingCrawler()
    entries = [
        _WaveEntry(
            "p1",
            f"https://browser.test/{i}",
            0,
            "fetch",
            desc=_desc(crawler, f"https://browser.test/{i}", poolable=False),
        )
        for i in range(3)
    ]
    caller = threading.get_ident()
    with (
        ThreadPoolExecutor(max_workers=1) as non_poolable_pool,
        ThreadPoolExecutor(max_workers=2) as fetch_pool,
    ):
        request, outcomes = _wave(
            entries, non_poolable_pool=non_poolable_pool, fetch_pool=fetch_pool
        )

    assert request.non_poolable is True
    assert len(crawler.threads) == 3
    assert all(tid != caller for tid in crawler.threads)
    # Поток-владелец ``BrowserCrawler`` один: маршрут исполняется строго последовательно.
    assert len(set(crawler.threads)) == 1
    assert crawler.max_active == 1
    # Исходы пришли в порядке отправки и каждый — состоявшаяся страница.
    assert [o.page.url if o is not None and o.page is not None else None for o in outcomes.fetch] == [
        "https://browser.test/0",
        "https://browser.test/1",
        "https://browser.test/2",
    ]


def test_poolable_route_does_not_mark_the_wave_and_bypasses_resume() -> None:
    """Обратная сторона: poolable маршрут не помечает волну, resume живого фетча не имеет."""
    crawler = _ThreadRecordingCrawler()
    entries = [
        _WaveEntry(
            "p1",
            "https://browser.test/p",
            0,
            "fetch",
            desc=_desc(crawler, "https://browser.test/p", poolable=True),
        ),
        # Блэклист и исчерпанные попытки — записи без фетча вообще.
        _WaveEntry("p1", "https://browser.test/x", 0, "blacklisted"),
    ]
    caller = threading.get_ident()
    with (
        ThreadPoolExecutor(max_workers=1) as non_poolable_pool,
        ThreadPoolExecutor(max_workers=2) as fetch_pool,
    ):
        request, outcomes = _wave(
            entries, non_poolable_pool=non_poolable_pool, fetch_pool=fetch_pool
        )

    assert request.non_poolable is False
    # Живой фетч ушёл в пул фетча, а не в пул не-poolable маршрута.
    assert len(crawler.threads) == 1
    assert crawler.threads[0] != caller
    assert outcomes.fetch[0] is not None
    # У записи без фетча исхода нет — коммит применит её локально (fresh-фетча не было).
    assert outcomes.fetch[1] is None

# --------------------------------------------------------------------------- #
# Метка попытки берётся из резерва лимитера, а не из системных часов
# --------------------------------------------------------------------------- #

#: Резерв, который вернёт лимитер в этих пинах: 2000-01-01T00:00:02.5Z.
_RESERVED = "2000-01-01T00:00:02.500000+00:00"


class _ReservationOnly:
    """Лимитер-заглушка: ``wait_reserved`` — единственный источник метки.

    ``wait`` намеренно падает: метка обязана приходить из резерва. Так пин ловит возврат
    к ``_now_iso()`` после ``limiter.wait`` — тогда ``fetch_started_at`` был бы моментом
    пробуждения (и метрика вежливости мерила бы планировщик потоков, а не пейсинг).
    """

    def wait(self, host: str, spec: Any) -> float:
        if spec is not None:
            raise AssertionError("метка должна приходить из wait_reserved, а не из wait")
        return 0.0

    def wait_reserved(self, host: str, spec: Any) -> str:
        return _RESERVED


def test_local_fetch_takes_start_label_from_limiter_reservation() -> None:
    """``local_fetch``: ``fetch_started_at`` — разрешённый момент лимитера."""
    crawler = _ThreadRecordingCrawler()
    desc = _desc(crawler, "https://browser.test/a", poolable=True)

    local_fetch(desc, limiter=_ReservationOnly())  # type: ignore[arg-type]

    assert desc.fetch_started_at == _RESERVED
    assert desc.fetch_ms is not None


#: Стена фейковых часов: от неё строится ISO-метка, чтобы резерв был валидным временем.
_WALL_EPOCH = 1_700_000_000.0


class _CountingLimiter(RateLimiter):
    """Лимитер на фейковых часах: считает резервы и суммы сна (без wall-clock).

    Наследует **боевой** :class:`RateLimiter` — дефект живёт в вызывающей стороне, не в
    примитиве, поэтому тест обязан считать именно вызовы ``_reserve``: один вызов = один
    слот origin'а, то есть один объявленный интервал.
    """

    def __init__(self) -> None:
        self.reserves: list[str] = []
        self.slept: list[float] = []
        self._now = 0.0
        super().__init__(
            sleep=self._fake_sleep,
            rng=random.Random(0),
            clock=lambda: self._now,
            wall=lambda: _WALL_EPOCH + self._now,
        )

    def _fake_sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._now += seconds

    def _reserve(self, host: str, spec: RateLimitSpec) -> tuple[float, int]:
        self.reserves.append(host)
        return super()._reserve(host, spec)


def test_pool_fetch_takes_exactly_one_reservation_per_fetch() -> None:
    """Пулевой путь (``local_fetch``): **один** резерв на фетч, интервал — объявленный.

    ``_dispatch_wave`` отправляет в пул ровно ``local_fetch`` (единственный сетевой вызов
    пула), поэтому проверяется он, а не inline ``fetch_and_commit``: у того
    ``wait_reserved`` и всегда был один. Дефект: ``wait`` + ``wait_reserved`` на один фетч
    = **два** слота, и фактический интервал между двумя запросами одного origin'а
    становится суммой двух ``uniform(min_s, max_s)`` (при объявленных 1–5 с — 2–10 с и
    среднее 6 с вместо 3 с; для бюджета 5000 документов пол 8.33 ч вместо 4.17 ч).
    """
    step = 0.5
    spec = RateLimitSpec(min_s=step, max_s=step)
    limiter = _CountingLimiter()
    crawler = _ThreadRecordingCrawler()
    first = _desc(
        crawler,
        "https://browser.test/1",
        poolable=True,
        fetch=FetchMode.HTTP,
        rate_limit=spec,
    )
    second = _desc(
        crawler,
        "https://browser.test/2",
        poolable=True,
        fetch=FetchMode.HTTP,
        rate_limit=spec,
    )

    local_fetch(first, limiter=limiter)
    local_fetch(second, limiter=limiter)

    # Один слот на фетч: два фетча — два резерва (при удвоении их четыре).
    assert limiter.reserves == ["browser.test", "browser.test"]
    # Суммарный сон = 2 × объявленный шаг, а не 4 ×.
    assert limiter.slept == [step, step]
    # Метки попытки разнесены ровно на объявленный интервал, а не на удвоенный.
    first_label = first.fetch_started_at
    second_label = second.fetch_started_at
    assert first_label is not None
    assert second_label is not None
    started = [
        _dt.datetime.fromisoformat(label) for label in (first_label, second_label)
    ]
    assert started[1] - started[0] == _dt.timedelta(seconds=step)


def test_fetch_and_commit_takes_start_label_from_limiter_reservation() -> None:
    """``fetch_and_commit`` (последовательный и браузерный путь) — тот же источник метки."""
    from core.pipeline.corpus_ingest import fetch_and_commit

    crawler = _ThreadRecordingCrawler()
    desc = _desc(crawler, "https://browser.test/b", poolable=False)
    committed: list[Any] = []

    def commit_local(desc: Any, **kwargs: Any) -> None:
        committed.append(kwargs)

    assert (
        fetch_and_commit(
            desc, limiter=_ReservationOnly(), commit_local=commit_local  # type: ignore[arg-type]
        )
        is None
    )
    assert desc.fetch_started_at == _RESERVED
    assert len(committed) == 1 and "page" in committed[0]
