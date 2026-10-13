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
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import core.edge.__main__ as edge_main
from core.adapters.crawl_http import HttpCrawler
from core.edge.config import (
    ENV_EDGE_STATE_DIR,
    EdgeRuntimeConfig,
    load_edge_runtime_config,
)
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


def test_edge_runtime_config_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        EdgeRuntimeConfig(bogus=1)  # type: ignore[call-arg]


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


def test_edge_main_source_has_no_cli_or_store_import() -> None:
    assert edge_main.__file__ is not None
    source = Path(edge_main.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
        elif isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
    forbidden = ("core.cli", "core.store")
    assert not [m for m in imported if m == forbidden[0] or m.startswith(f"{forbidden[0]}.")]
    assert not [m for m in imported if m == forbidden[1] or m.startswith(f"{forbidden[1]}.")]
