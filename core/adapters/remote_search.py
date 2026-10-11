"""Remote-прокси внешнего поиска — ``RemoteSearch`` (W13/T13.3; 07 §4).

Реализация ``SearchAdapterPort`` в ядре для профиля ``split``: реальный SERP-
адаптер исполняется в DMZ, ядро вызывает его через ``ContentGateway``.
Контракт порта **не меняется** (07 §4) — прокси drop-in для ``search_*.py``.

I19: ``/v1/search`` возвращает **кандидатов** (URL+метаданные), не evidence;
``raw_bytes`` у ``SearchResult`` здесь ``None``, дозагрузка — отдельным
``/v1/fetch``. Вызовы логирует вызывающий pipeline в ``search_log`` (I23).
Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.adapters.remote_gateway import (
    RemoteContentError,
    as_items,
    raw_page_from_response,
)
from core.ports.gateway import ContentGateway
from core.ports.search import SearchQuery, SearchResult
from core.ports.types import ExtractionRoute, FetchMode, RawPage

__all__ = ["RemoteSearch"]


class RemoteSearch:
    """``SearchAdapterPort`` через DMZ-шов. Drop-in для реального SERP-адаптера."""

    def __init__(
        self,
        gateway: ContentGateway,
        *,
        adapter_kind: str = "remote",
        provider: str = "edge",
    ) -> None:
        self._gateway = gateway
        self.adapter_kind = adapter_kind
        self.provider = provider

    @property
    def available(self) -> bool:
        """Доступен ли транспорт шва (``httpx`` или внедрённый ``transport``)."""
        return bool(getattr(self._gateway, "available", True))

    def search(self, q: SearchQuery) -> list[SearchResult]:
        """``/v1/search`` → список кандидатов (без evidence, I19)."""
        payload = {
            "query": q.text,
            "lang": q.lang,
            "limit": q.limit,
            "phase": str(q.phase),
        }
        response = self._gateway.call("search", payload)
        return _search_results_from_payload(response)

    def fetch(self, url: str) -> RawPage:
        """Дозагрузка выбранного кандидата через ``/v1/fetch`` → ``RawPage``."""
        payload = {
            "url": url,
            "fetch": FetchMode.HTTP.value,
            "lang": None,
            "extract": ExtractionRoute.AUTO.value,
            "wait_for": None,
        }
        response = self._gateway.call("fetch", payload)
        if not isinstance(response, Mapping):
            raise RemoteContentError("fetch: payload ответа edge не является отображением")
        return raw_page_from_response(response, url=url, fetch=FetchMode.HTTP)


def _search_results_from_payload(payload: Any) -> list[SearchResult]:
    """Разбирает payload ``/v1/search`` (список кандидатов) в ``SearchResult``."""
    items = as_items(payload, op="search")
    results: list[SearchResult] = []
    for item in items:
        if not isinstance(item, Mapping):
            raise RemoteContentError("search: элемент ответа не является отображением")
        url = item.get("url")
        if not url:
            raise RemoteContentError("search: кандидат без url")
        rank = item.get("rank", 0)
        results.append(
            SearchResult(
                url=str(url),
                title=str(item.get("title") or ""),
                snippet=str(item.get("snippet") or ""),
                rank=int(rank) if rank is not None else 0,
                raw_bytes=None,  # I19: кандидат, не evidence
            )
        )
    return results
