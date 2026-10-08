"""Порт транспорта core↔edge (DMZ-шов) — ``ContentGateway`` (07 §3–§4).

Общий транспорт remote-прокси (``RemoteCrawler``/``RemoteSearch``/``RemoteRegistry``/
``RemoteExtractor``). Гарантия I19: gateway возвращает контент/кандидатов, **не evidence**;
ядро повторно хеширует/валидирует входящее (07 §5). Профиль ``local`` порт не использует.
Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Any, Mapping, Protocol, runtime_checkable


@runtime_checkable
class ContentGateway(Protocol):
    """Версионированный L7-транспорт к DMZ-сервису (JSON/HTTP; без pinning — 07 §8)."""

    schema_version: str

    def call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def capabilities(self) -> Mapping[str, Any]: ...
