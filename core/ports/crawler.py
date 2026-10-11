"""Порт краулера — ``CrawlerPort`` (§3.2, Ш5).

Возвращает **сырые байты** (retention, I19). План получения (``FetchPlan``: HTTP/браузер и
JS-ожидание) резолвится в pipeline и передаётся сюда готовым — адаптер лишь исполняет его.
Robots-этика — ответственность адаптера (``adapters/crawl_scrapy.py``). В профиле ``split``
исполняется в DMZ; ядро использует remote-proxy (``ports/gateway.py``). Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.ports.types import FetchPlan, RawPage


@runtime_checkable
class CrawlerPort(Protocol):
    """Загрузка страниц. ``plan.fetch`` выбирает HTTP или браузерный рендер (JS)."""

    version: str

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage: ...
