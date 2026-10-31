"""F4 (17 §6): не-poolable (браузерный) фетч исполняется **своим** потоком, не драйвером.

Зачем это существует. ``BrowserCrawler`` водит единственную ``_page`` без лока, поэтому у
не-poolable маршрута обязан быть **один** поток-владелец. Прежний путь исполнял этот фетч
на потоке драйвера (``fetch_and_commit``): пока полоса одна, это незаметно, но при
чередовании корней один браузерный фетч остановил бы все полосы.

Что проверяется. Отправка кладёт не-poolable задание в отдельный пул из одного потока
(маршрут помечен в :class:`WaveRequest`), фетч исполняется **не** на вызывающем потоке,
`max_active == 1` (поток-владелец один), а коммит исхода остаётся за вызывающим потоком —
то есть мутации не уходят в чужой поток (T5).
"""

from __future__ import annotations

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
from core.ports.types import FetchMode, FetchPlan, RawPage
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


def _desc(crawler: _ThreadRecordingCrawler, url: str, *, poolable: bool) -> LocalFetch:
    """Дескриптор живого фетча: ``poolable=False`` — браузерный маршрут."""
    return LocalFetch(
        pid="p1",
        wu=url,
        depth=0,
        fetch_plan=FetchPlan(fetch=FetchMode.BROWSER),
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