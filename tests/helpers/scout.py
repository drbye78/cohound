"""Хелперы scout-тестов: детерминированные краулеры и очистка окружения.

Классы-заглушки портов (``FakeCrawler``/``FakeSearchAdapter``) живут в
:mod:`tests.fakes.ports`; здесь — только сборка страницы и типовые операции,
дублировавшиеся в тест-файлах.
"""

from __future__ import annotations

from collections.abc import Iterable

import pytest

from core.ports.types import RawPage, RenderMode
from tests.fakes.ports import FakeCrawler

#: Фиксированная метка времени (детерминизм фейков).
FIXED_TS = "2026-01-01T00:00:00+00:00"

__all__ = ["FIXED_TS", "clear_env", "fake_crawler"]


def fake_crawler(
    url: str,
    text: str,
    *,
    fetched_at: str = FIXED_TS,
    render_mode: RenderMode = RenderMode.HTTP,
) -> FakeCrawler:
    """Краулер с единственной страницей ``url`` из текста ``text`` (UTF-8)."""
    page = RawPage(
        url=url,
        content=text.encode("utf-8"),
        fetched_at=fetched_at,
        render_mode=render_mode,
    )
    return FakeCrawler({url: page})


def clear_env(monkeypatch: pytest.MonkeyPatch, names: Iterable[str]) -> None:
    """Удаляет переменные окружения ``names`` (детерминизм конфигурации)."""
    for name in names:
        monkeypatch.delenv(name, raising=False)
