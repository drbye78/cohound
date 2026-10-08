"""Порт внешнего поиска — ``SearchAdapterPort`` (Ш5, I19/I23).

Гарантия I19: выход — только URL+байты/кандидаты, **не evidence**. Превращение в evidence
происходит в Ш5 (chunk+hash+index). Вызовы логирует вызывающий pipeline в ``search_log`` (I23).

В профиле ``split`` реальный адаптер исполняется в DMZ; ядро использует remote-proxy
(``ports/gateway.py``). Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from core.ports.types import RawPage


@dataclass(frozen=True)
class SearchQuery:
    """Поисковый запрос (внутренний контракт; SERP-специфика — в адаптере)."""

    text: str
    lang: str = "ru"
    limit: int = 10
    phase: str = "corpus"


@dataclass(frozen=True)
class SearchResult:
    """Кандидат из внешнего поиска. ``raw_bytes`` — для retention в ``corpus_raw`` (I19)."""

    url: str
    title: str = ""
    snippet: str = ""
    rank: int = 0
    raw_bytes: bytes | None = None


@runtime_checkable
class SearchAdapterPort(Protocol):
    """Внешний поиск как источник входного контента (Ш5). Commodity-слой."""

    adapter_kind: str
    provider: str

    def search(self, q: SearchQuery) -> list[SearchResult]: ...

    def fetch(self, url: str) -> RawPage: ...
