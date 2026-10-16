"""Тесты edge composition root и runtime-конфига (Phase 3.0; 07 §4–§6).

Покрывают: :class:`EdgeRuntimeConfig` (дефолты, env-override, строгая схема, карта
воркеров); хук планировщика в :func:`core.edge.server.serve` (``scheduler=None`` —
поведение неизменно; с фейком — ``start()`` до обслуживания и ``stop()`` в ``finally``);
``python -m core.edge serve --check`` (собирает gateway+адаптеры, не привязывает сокет);
изоляцию слоя (в ``core/edge/__main__.py`` нет импортов ``core.cli``/``core.store``).

Сеть не используется: сервер везде инъецируется фабрикой. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import ast
import pickle
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import core.edge.__main__ as edge_main
from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import ScrapyCrawler
from core.edge.config import (
    ENV_EDGE_STATE_DIR,
    EdgeRuntimeConfig,
    load_edge_runtime_config,
)
from core.edge.poller import FeedPollService
from core.edge.server import EdgeGateway, serve
from core.ports.types import ExtractedDoc, ExtractionRoute, FetchPlan, RawPage


# ---------------------------------------------------------------------------
# Fake-адаптеры / gateway (без сети)
# ---------------------------------------------------------------------------


class _Crawler:
    version = "fake-crawler-1"
    engine = "http"

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        return RawPage(url=url, content=b"<html>hi</html>", fetched_at="t")


class _Extractor:
    version = "fake-extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        return ExtractedDoc(text="hi", content_hash="h", extractor_version=self.version)


def _gateway() -> EdgeGateway:
    return EdgeGateway(_Crawler(), _Extractor())


# ---------------------------------------------------------------------------
# EdgeRuntimeConfig
# ---------------------------------------------------------------------------


def test_edge_runtime_config_defaults() -> None:
    cfg = EdgeRuntimeConfig()
    assert cfg.state_dir == "./edge_state"
    assert cfg.blob_dir is None
    assert cfg.resolved_blob_dir == "./edge_state/blobs"
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 0
    assert cfg.workers == {"http": 4, "scrapy": 2, "browser": 1}
    assert cfg.queue_limit == 256
    assert cfg.ttl_s == 86400
    assert cfg.schema_version == "1"


def test_edge_runtime_config_workers_map_override() -> None:
    cfg = EdgeRuntimeConfig(workers={"http": 8})
    assert cfg.workers == {"http": 8}


def test_edge_runtime_config_resolved_blob_dir_prefers_explicit() -> None:
    cfg = EdgeRuntimeConfig(state_dir="/s", blob_dir="/b")
    assert cfg.resolved_blob_dir == "/b"


def test_load_edge_runtime_config_defaults_without_file() -> None:
    assert load_edge_runtime_config(None) == EdgeRuntimeConfig()


def test_load_edge_runtime_config_env_overrides_state_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ENV_EDGE_STATE_DIR, str(tmp_path / "edge_home"))
    cfg = load_edge_runtime_config(None)
    assert cfg.state_dir == str(tmp_path / "edge_home")
    assert cfg.resolved_blob_dir == f"{tmp_path / 'edge_home'}/blobs"


def test_load_edge_runtime_config_env_beats_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "edge.yaml"
    path.write_text("state_dir: ./from_file\n", encoding="utf-8")
    monkeypatch.setenv(ENV_EDGE_STATE_DIR, "./from_env")
    assert load_edge_runtime_config(path).state_dir == "./from_env"


def test_load_edge_runtime_config_reads_yaml(tmp_path: Path) -> None:
    path = tmp_path / "edge.yaml"
    path.write_text(
        "state_dir: ./s\nhost: 0.0.0.0\nport: 8443\n"
        "workers: {http: 2}\nqueue_limit: 16\nttl_s: 60\n",
        encoding="utf-8",
    )
    cfg = load_edge_runtime_config(path)
    assert cfg.state_dir == "./s"
    assert cfg.host == "0.0.0.0"
    assert cfg.port == 8443
    assert cfg.workers == {"http": 2}
    assert cfg.queue_limit == 16
    assert cfg.ttl_s == 60


def test_load_edge_runtime_config_rejects_unknown_field(tmp_path: Path) -> None:
    path = tmp_path / "edge.yaml"
    path.write_text("bogus: 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="edge"):
        load_edge_runtime_config(path)


def test_load_edge_runtime_config_rejects_non_mapping_root(tmp_path: Path) -> None:
    path = tmp_path / "edge.yaml"
    path.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(ValueError, match="словарь"):
        load_edge_runtime_config(path)


def test_load_edge_runtime_config_missing_file_uses_defaults(tmp_path: Path) -> None:
    assert load_edge_runtime_config(tmp_path / "nope.yaml") == EdgeRuntimeConfig()


# ---------------------------------------------------------------------------
# serve(): хук планировщика
# ---------------------------------------------------------------------------


def _server_factory(events: list[str], *, serve_raises: bool = False) -> Any:
    """Фабрика фейкового сервера, логирующая ``serve_forever``/``server_close``."""

    class _FakeServer:
        def __init__(self, address: tuple[str, int], handler: Any) -> None:
            events.append("init")

        def serve_forever(self) -> None:
            events.append("serve")
            if serve_raises:
                raise RuntimeError("boom")

        def server_close(self) -> None:
            events.append("close")

    return _FakeServer


class _Scheduler:
    """Фейковый планировщик: логирует ``start``/``stop``."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    def start(self) -> None:
        self._events.append("start")

    def stop(self) -> None:
        self._events.append("stop")


def test_serve_without_scheduler_behavior_unchanged() -> None:
    events: list[str] = []
    serve("127.0.0.1", 0, _gateway(), server_factory=_server_factory(events))
    assert events == ["init", "serve", "close"]


def test_serve_starts_scheduler_before_serving_and_stops_in_finally() -> None:
    events: list[str] = []
    scheduler = _Scheduler(events)
    serve(
        "127.0.0.1",
        0,
        _gateway(),
        server_factory=_server_factory(events),
        scheduler=scheduler,
    )
    assert events == ["init", "start", "serve", "close", "stop"]


def test_serve_stops_scheduler_when_serving_raises() -> None:
    events: list[str] = []
    scheduler = _Scheduler(events)
    with pytest.raises(RuntimeError, match="boom"):
        serve(
            "127.0.0.1",
            0,
            _gateway(),
            server_factory=_server_factory(events, serve_raises=True),
            scheduler=scheduler,
        )
    # stop() вызван в finally несмотря на сбой serve_forever.
    assert events == ["init", "start", "serve", "close", "stop"]


def test_serve_on_start_runs_before_scheduler_start() -> None:
    events: list[str] = []
    scheduler = _Scheduler(events)
    serve(
        "127.0.0.1",
        0,
        _gateway(),
        server_factory=_server_factory(events),
        scheduler=scheduler,
        on_start=lambda: events.append("on_start"),
    )
    assert events == ["init", "on_start", "start", "serve", "close", "stop"]


# ---------------------------------------------------------------------------
# __main__: --check и изоляция слоя
# ---------------------------------------------------------------------------


def test_main_check_builds_and_exits_without_serve(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--check`` собирает gateway+адаптеры, не запускает ``serve`` (без сокета)."""

    def _boom(*args: Any, **kwargs: Any) -> None:  # pragma: no cover - не должен зваться
        raise AssertionError("--check не должен запускать serve (привязка сокета)")

    monkeypatch.setattr(edge_main, "serve", _boom)
    code = edge_main.main(["serve", "--check"])
    assert code == edge_main.EXIT_OK
    out = capsys.readouterr().out
    assert "edge check ok" in out
    assert "engines=" in out
    assert "poller=ready" in out


def test_main_no_subcommand_is_usage(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert edge_main.main([]) == edge_main.EXIT_USAGE


def test_main_unknown_config_is_usage(tmp_path: Path) -> None:
    bad = tmp_path / "edge.yaml"
    bad.write_text("bogus: 1\n", encoding="utf-8")
    assert edge_main.main(["serve", "--config", str(bad), "--check"]) == edge_main.EXIT_USAGE


def test_build_gateway_always_includes_http() -> None:
    gateway, crawlers, chunker = edge_main._build_gateway(EdgeRuntimeConfig())
    assert "http" in crawlers
    assert isinstance(crawlers["http"], HttpCrawler)
    assert chunker is not None
    assert isinstance(gateway, EdgeGateway)


class _FeedCrawler:
    """Стенд-краулер: отдаёт валидный RSS-фид (без сети) для ``crawl/poll``."""

    version = "fake-feed-crawler-1"
    engine = "http"

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        return RawPage(
            url=url,
            content=(
                b'<?xml version="1.0" encoding="utf-8"?>'
                b'<rss version="2.0"><channel>'
                b"<item><title>A</title><link>https://feed.test/a</link></item>"
                b"</channel></rss>"
            ),
            fetched_at="t",
            content_type="application/rss+xml",
        )


def test_build_gateway_wires_poller_for_crawl_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_build_gateway`` инъецирует ``FeedPollService`` тем же диспетчером, что и ``fetch``."""
    monkeypatch.setattr(
        edge_main, "_build_crawlers", lambda config: {"http": _FeedCrawler()}
    )
    gateway, _crawlers, _chunker = edge_main._build_gateway(EdgeRuntimeConfig())

    # Диспетчер построен один раз: gateway и поллер держат один объект (Wave 5.1).
    assert isinstance(gateway._poller, FeedPollService)
    assert gateway._poller._crawler is gateway._crawler

    env = gateway.envelope("crawl/poll", {"url": "https://feed.test/rss", "kind": "feed"})
    assert env["status"] == "ok"
    assert env["payload"] == {
        "entries": [
            {"url": "https://feed.test/a", "guid": None, "published_at": None, "title": "A"}
        ],
        "count": 1,
        "truncated": False,
    }

    # SSRF-защита работает и через реальную конфигурацию composition root.
    bad = gateway.envelope(
        "crawl/poll", {"url": "http://169.254.169.254/", "kind": "feed"}
    )
    assert bad["status"] == "error"


# ---------------------------------------------------------------------------
# __main__: планировщик краула в composition root (Phase 3.4a)
# ---------------------------------------------------------------------------


class _FakeScheduler:
    """Фейковый планировщик: фиксирует конструирование и жизненный цикл."""

    instances: list["_FakeScheduler"] = []

    def __init__(
        self, config: Any, crawlers: Any, *, crawler_factory: Any = None
    ) -> None:
        self.config = config
        self.crawlers = dict(crawlers)
        self.crawler_factory = crawler_factory
        self.started = False
        self.closed = False
        _FakeScheduler.instances.append(self)

    def start(self) -> None:  # pragma: no cover - в main не вызывается
        self.started = True

    def stop(self, timeout: Any = None) -> None:  # pragma: no cover - зовёт serve
        pass

    def close(self) -> None:
        self.closed = True


def _write_config(tmp_path: Path) -> Path:
    """Пишет минимальный edge.yaml с локальным ``state_dir`` (без записи на диск сервера)."""
    path = tmp_path / "edge.yaml"
    path.write_text(f"state_dir: {tmp_path / 'state'}\n", encoding="utf-8")
    return path


def test_crawler_factory_builds_expected_adapters() -> None:
    assert isinstance(edge_main.crawler_factory("http"), HttpCrawler)
    assert isinstance(edge_main.crawler_factory("scrapy"), ScrapyCrawler)
    assert isinstance(edge_main.crawler_factory("browser"), BrowserCrawler)
    # Регистронезависимо (со strip); движок-атрибут совпадает с запрошенным (провенанс).
    assert getattr(edge_main.crawler_factory("HTTP"), "engine") == "http"
    assert getattr(edge_main.crawler_factory("Scrapy"), "engine") == "scrapy"
    assert getattr(edge_main.crawler_factory(" browser "), "engine") == "browser"


def test_crawler_factory_rejects_unknown_engine() -> None:
    with pytest.raises(ValueError, match="движок"):
        edge_main.crawler_factory("teleport")


def test_crawler_factory_module_is_not_main() -> None:
    """B1: фабрика обязана жить в импортируемом модуле, а не в ``__main__``.

    Launcher исполняется как ``python -m core.edge`` (``core/edge/__main__.py`` →
    ``__module__ == "__main__"``); определённая там фабрика не picklable by reference →
    ``BrokenProcessPool`` для scrapy/browser. Фабрика вынесена в
    ``core.edge.crawler_factory``; ``__main__`` её реэкспортирует.
    """
    from core.edge import crawler_factory as factory_module

    factory = factory_module.crawler_factory
    assert factory.__module__ == "core.edge.crawler_factory"
    assert factory.__module__ != "__main__"
    # Реэкспорт из ``__main__`` — тот же объект (единая точка истины).
    assert edge_main.crawler_factory is factory


def _build_crawler_in_worker(engine: str) -> str:
    """Module-level probe: строит краулер реальной фабрикой в воркер-процессе (B1).

    Должен быть picklable by reference (module-level тестового модуля) — иначе
    ``ProcessPoolExecutor`` не поднимется; это ровно тот случай, что ломался у
    ``__main__``-фабрики.
    """
    from core.edge.crawler_factory import crawler_factory

    return type(crawler_factory(engine)).__name__


@pytest.mark.slow
@pytest.mark.parametrize("method", ["forkserver", "spawn"])
def test_worker_process_runs_real_crawler_factory(method: str) -> None:
    """B1: реальная фабрика выполняется в отдельном процессе (без сети/scrapy).

    ``http`` — дешёвый движок: строим адаптер, но НЕ фетчим (сеть не нужна).
    """
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor

    try:
        context = multiprocessing.get_context(method)
    except ValueError:  # pragma: no cover - платформо-зависимо
        pytest.skip(f"метод multiprocessing {method!r} недоступен")
    with ProcessPoolExecutor(max_workers=1, mp_context=context) as executor:
        assert executor.submit(_build_crawler_in_worker, "http").result(timeout=30) == "HttpCrawler"


def test_build_scheduler_http_in_process_and_zero_engines_excluded() -> None:
    config = EdgeRuntimeConfig(
        state_dir="/tmp/unused", workers={"http": 2, "scrapy": 0, "browser": 0}
    )
    scheduler = edge_main._build_scheduler(config, {"http": _Crawler()})
    try:
        assert scheduler._thread_engines == {"http"}
        assert scheduler._process_engines == ()
    finally:
        scheduler.close()


def test_build_scheduler_process_engines_from_positive_workers() -> None:
    config = EdgeRuntimeConfig(workers={"http": 1, "scrapy": 2, "browser": 3})
    scheduler = edge_main._build_scheduler(config, {"http": _Crawler()})
    try:
        assert scheduler._thread_engines == {"http"}
        assert set(scheduler._process_engines) == {"scrapy", "browser"}
    finally:
        scheduler.close()


def test_main_check_builds_scheduler_without_starting(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--check`` строит планировщик, но не стартует воркеры и не биндит сокет."""
    _FakeScheduler.instances.clear()

    def _boom(*args: Any, **kwargs: Any) -> None:  # pragma: no cover - не должен зваться
        raise AssertionError("--check не должен запускать serve (привязка сокета)")

    monkeypatch.setattr(edge_main, "EdgeScheduler", _FakeScheduler)
    monkeypatch.setattr(edge_main, "serve", _boom)
    code = edge_main.main(["serve", "--check"])
    assert code == edge_main.EXIT_OK
    assert len(_FakeScheduler.instances) == 1
    built = _FakeScheduler.instances[0]
    assert built.started is False  # воркеры не стартовали
    assert built.crawler_factory is edge_main.crawler_factory  # процессы через фабрику
    assert "http" in built.crawlers  # in-process только http
    assert built.closed is True
    out = capsys.readouterr().out
    assert "edge check ok" in out and "scheduler=ready" in out


def test_main_serve_passes_non_none_scheduler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``serve`` получает построенный (не ``None``) планировщик; стартует его сам serve."""
    _FakeScheduler.instances.clear()
    captured: dict[str, Any] = {}

    def _fake_serve(host: str, port: int, gateway: Any, **kwargs: Any) -> None:
        captured["host"] = host
        captured["port"] = port
        captured["scheduler"] = kwargs.get("scheduler")

    monkeypatch.setattr(edge_main, "EdgeScheduler", _FakeScheduler)
    monkeypatch.setattr(edge_main, "serve", _fake_serve)
    code = edge_main.main(["serve", "--config", str(_write_config(tmp_path))])
    assert code == edge_main.EXIT_OK
    assert captured["scheduler"] is not None
    assert captured["scheduler"] is _FakeScheduler.instances[0]
    # main не дублирует жизненный цикл: start/stop — ответственность serve().
    assert _FakeScheduler.instances[0].started is False


def test_main_serve_returns_ok_on_keyboard_interrupt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Штатный ``KeyboardInterrupt`` (в т.ч. от SIGTERM) → graceful-код 0."""
    _FakeScheduler.instances.clear()

    def _interrupt(*args: Any, **kwargs: Any) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(edge_main, "EdgeScheduler", _FakeScheduler)
    monkeypatch.setattr(edge_main, "serve", _interrupt)
    assert (
        edge_main.main(["serve", "--config", str(_write_config(tmp_path))])
        == edge_main.EXIT_OK
    )
    # serve() отвечает за stop(); main лишь закрывает ресурсы.
    assert _FakeScheduler.instances[0].closed is True
