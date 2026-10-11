"""Юнит-тесты общего реактор-воркера ``core/adapters/crawl_scrapy.py`` (Ш5).

Здесь проверяется главный дефект: Twisted-реактор **одноразовый**, поэтому старый
путь ``CrawlerProcess.start()`` на каждый ``fetch`` падал на втором URL с
``ReactorNotRestartable``. Воркер поднимает реактор **один** раз на процесс и
мультиплексирует краулы.

Тесты детерминированы и не тянут Twisted/сеть: воркеру внедряется ``_FakeReactor``
(очередь + отдельный поток) и ``_FakeCrawl`` (фейковый движок). Данные нейтральны
(§1.3).
"""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Future

import pytest

from core.adapters.crawl_scrapy import (
    CrawlError,
    ScrapyCrawler,
    _CrawlSpec,
    _ReactorWorker,
)
from core.ports.types import FetchMode, FetchPlan, RawPage

_SETTINGS: dict[str, int] = {"RETRY_TIMES": 0, "DOWNLOAD_TIMEOUT": 1}

_PENDING = object()


class _FakeFailure:
    """Минимальная имитация Twisted ``Failure`` (нужен атрибут ``value``)."""

    def __init__(self, value: BaseException) -> None:
        self.value = value


class _FakeDeferred:
    """Минимальный ``Deferred``: ``addCallbacks`` + ручное разрешение."""

    def __init__(self) -> None:
        self._callbacks: list[tuple] = []
        self._result: object = _PENDING
        self._failed = False

    def addCallbacks(self, on_success, on_error) -> _FakeDeferred:
        self._callbacks.append((on_success, on_error))
        self._fire_if_ready()
        return self

    def callback(self, value: object = None) -> None:
        if self._result is not _PENDING:
            return
        self._result = value
        self._failed = False
        self._fire_if_ready()

    def fail(self, exc: BaseException) -> None:
        if self._result is not _PENDING:
            return
        self._result = _FakeFailure(exc)
        self._failed = True
        self._fire_if_ready()

    def _fire_if_ready(self) -> None:
        if self._result is _PENDING:
            return
        callbacks, self._callbacks = self._callbacks, []
        for on_success, on_error in callbacks:
            if self._failed:
                on_error(self._result)
            else:
                on_success(self._result)


class _FakeCrawl:
    """Фейковый ``crawl``-шов: пишет вызовы, наполняет ``spec.collected``."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.deferreds: list[_FakeDeferred] = []
        self.fail_urls: set[str] = set()
        self.hold_urls: set[str] = set()
        self.empty_urls: set[str] = set()

    def __call__(self, settings, spec: _CrawlSpec) -> _FakeDeferred:
        self.calls.append(spec.url)
        deferred = _FakeDeferred()
        self.deferreds.append(deferred)
        if spec.url in self.fail_urls:
            deferred.fail(RuntimeError("boom"))
        elif spec.url in self.hold_urls:
            pass  # никогда не разрешается — проверяем таймаут
        else:
            if spec.url not in self.empty_urls:
                spec.collected.append(
                    RawPage(
                        url=spec.url,
                        content=b"body",
                        fetched_at="t",
                        fetch_mode=spec.fetch,
                    )
                )
            deferred.callback(None)
        return deferred


class _FakeReactor:
    """Очередь-backed реактор: ``run`` дренит ``callFromThread`` до ``stop``."""

    def __init__(self) -> None:
        self._queue: queue.Queue = queue.Queue()
        self.run_calls = 0
        self._stopped = threading.Event()

    def callFromThread(self, fn, *args, **kwargs) -> None:
        self._queue.put((fn, args, kwargs))

    def run(self, installSignalHandlers: bool = False) -> None:  # noqa: N803 - сигнатура Twisted
        self.run_calls += 1
        while not self._stopped.is_set():
            try:
                fn, args, kwargs = self._queue.get(timeout=0.02)
            except queue.Empty:
                continue
            fn(*args, **kwargs)

    def stop(self) -> None:
        self._stopped.set()


@pytest.fixture
def make_worker():
    """Фабрика воркеров с фейковым реактором; закрывает их после теста."""
    workers: list[_ReactorWorker] = []

    def _make(fake_crawl: _FakeCrawl) -> tuple[_FakeReactor, _ReactorWorker]:
        reactor = _FakeReactor()
        worker = _ReactorWorker(install=lambda: reactor, crawl=fake_crawl)
        workers.append(worker)
        return reactor, worker

    yield _make
    for worker in workers:
        worker.close()


def _submit(
    worker: _ReactorWorker,
    url: str,
    timeout: float = 1.0,
    fetch: FetchMode = FetchMode.HTTP,
) -> _CrawlSpec:
    spec = _CrawlSpec(url=url, fetch=fetch, wait_for=None)
    fut: Future = Future()
    worker.submit(_SETTINGS, spec, fut, timeout)
    return spec


# ---------------------------------------------------------------------------
# A. Детерминированный воркер
# ---------------------------------------------------------------------------


def test_sequential_submits_resolve_in_one_worker(make_worker) -> None:
    fake = _FakeCrawl()
    reactor, worker = make_worker(fake)

    for index in range(4):
        spec = _submit(worker, f"https://example.com/{index}")
        assert spec.collected and spec.collected[0].url.endswith(f"/{index}")

    # Реактор стартовал ровно один раз — регрессия в миниатюре.
    assert reactor.run_calls == 1


def test_failed_submit_does_not_poison_next(make_worker) -> None:
    fake = _FakeCrawl()
    fake.fail_urls.add("https://example.com/bad")
    reactor, worker = make_worker(fake)

    with pytest.raises(CrawlError) as excinfo:
        _submit(worker, "https://example.com/bad")
    assert "не удался" in str(excinfo.value)

    # Изоляция: следующий краул на свежем движке проходит.
    spec = _submit(worker, "https://example.com/good")
    assert spec.collected and spec.collected[0].url.endswith("/good")
    assert reactor.run_calls == 1


def test_timeout_isolated_to_one_submit(make_worker) -> None:
    fake = _FakeCrawl()
    fake.hold_urls.add("https://example.com/slow")
    reactor, worker = make_worker(fake)

    with pytest.raises(CrawlError) as excinfo:
        _submit(worker, "https://example.com/slow", timeout=0.1)
    assert "таймаут" in str(excinfo.value)

    # Осиротевший краул не мешает последующим submit.
    spec = _submit(worker, "https://example.com/after")
    assert spec.collected and spec.collected[0].url.endswith("/after")
    assert reactor.run_calls == 1


def test_close_before_start_is_noop_and_idempotent() -> None:
    worker = _ReactorWorker(install=lambda: _FakeReactor(), crawl=_FakeCrawl())
    worker.close()
    worker.close()  # повторный вызов — no-op


# ---------------------------------------------------------------------------
# Интеграция воркера с ScrapyCrawler.fetch
# ---------------------------------------------------------------------------


def test_fetch_uses_worker_and_returns_first_page(make_worker) -> None:
    fake = _FakeCrawl()
    _reactor, worker = make_worker(fake)
    crawler = ScrapyCrawler(worker=worker)

    page = crawler.fetch("https://example.com/a")

    assert page.url == "https://example.com/a"
    assert fake.calls == ["https://example.com/a"]


def test_fetch_empty_collection_raises_crawl_error(make_worker) -> None:
    fake = _FakeCrawl()
    fake.empty_urls.add("https://example.com/empty")
    _reactor, worker = make_worker(fake)
    crawler = ScrapyCrawler(worker=worker)

    with pytest.raises(CrawlError) as excinfo:
        crawler.fetch("https://example.com/empty")
    assert "не вернул контент" in str(excinfo.value)


def test_fetch_close_via_injected_worker(make_worker) -> None:
    fake = _FakeCrawl()
    _reactor, worker = make_worker(fake)
    crawler = ScrapyCrawler(worker=worker)
    crawler.fetch("https://example.com/a")
    crawler.close()  # не должен поднимать
