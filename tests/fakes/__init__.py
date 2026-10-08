"""Пакет тестовых заглушек портов (W4/T4.3). Реэкспорт из ``tests.fakes.ports``."""

from __future__ import annotations

from tests.fakes.ports import (
    FIXED_TS,
    FakeChunker,
    FakeCrawler,
    FakeEvaluator,
    FakeExtractor,
    FakeRegistryFetcher,
    FakeSearchAdapter,
)

__all__ = [
    "FIXED_TS",
    "FakeChunker",
    "FakeCrawler",
    "FakeEvaluator",
    "FakeExtractor",
    "FakeRegistryFetcher",
    "FakeSearchAdapter",
]
