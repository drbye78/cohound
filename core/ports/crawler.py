"""Порт краулера — ``CrawlerPort`` (§3.2, Ш5).

Возвращает **сырые байты** (retention, I19). Per-domain рендер-профили и robots-этика —
ответственность адаптера (``adapters/crawl_scrapy.py``). В профиле ``split`` исполняется в DMZ;
ядро использует remote-proxy (``ports/gateway.py``). Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.ports.types import RawPage, RenderMode


@runtime_checkable
class CrawlerPort(Protocol):
    """Загрузка страниц. ``render`` выбирает HTTP или браузерный рендер (JS)."""

    version: str

    def fetch(self, url: str, render: RenderMode = RenderMode.HTTP) -> RawPage: ...
