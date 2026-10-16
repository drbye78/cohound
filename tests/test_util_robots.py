"""Тесты единой робот-политики ``core.util.robots`` (D1/§10) и её промоушена.

Без сети: текст ленты робots инъектируется. Проверяются дефолт-разрешение при недоступной
ленте, явный запрет, ``Crawl-delay`` и то, что ``core.pipeline.crawl_probe`` ре-экспортирует
ту же реализацию (single implementation).
"""

from __future__ import annotations

from core.pipeline.crawl_probe import RobotsGate as ProbeRobotsGate
from core.util.robots import RobotsGate

DISALLOW = "User-agent: *\nDisallow: /blocked\n"
DELAY = "User-agent: *\nCrawl-delay: 5\n"


def test_unavailable_robots_is_allowed() -> None:
    gate = RobotsGate(lambda _host: None)
    assert gate.allowed("https://example.com/anything") is True


def test_disallow_refused() -> None:
    gate = RobotsGate(lambda _host: DISALLOW)
    assert gate.allowed("https://example.com/blocked") is False
    assert gate.allowed("https://example.com/open") is True


def test_crawl_delay_reported() -> None:
    gate = RobotsGate(lambda _host: DELAY)
    assert gate.crawl_delay("https://example.com/x") == 5.0
    assert RobotsGate(lambda _host: None).crawl_delay("https://example.com/x") is None


def test_fetch_error_is_allowed_default() -> None:
    def boom(_host: str) -> str:
        raise RuntimeError("no robots")

    assert RobotsGate(boom).allowed("https://example.com/x") is True


def test_crawl_probe_reexports_single_implementation() -> None:
    assert ProbeRobotsGate is RobotsGate
