"""Порт реестрового фетчера — ``RegistryFetcherPort`` (Ш3, R1).

Нейтральный порт: конкретные реестры (endpoint/формат/маппинг) — **данные** в
``registries/*.yaml``, обслуживаются generic config-driven адаптером вне домена (§1.3).
R1: каждый факт обязан нести ``source_url``. В профиле ``split`` исполняется в DMZ.

``RegistryFact`` — доменный объект (``core.domain.models``). До T2.1 объявлялся здесь;
теперь реэкспортируется из домена (канонический источник).
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from core.domain import RegistryFact
from core.ports.types import ProductRef

__all__ = ["RegistryFact", "RegistryFetcherPort"]


@runtime_checkable
class RegistryFetcherPort(Protocol):
    """Обслуживает один реестр по данным конфигурации. Возвращает факты с ``source_url``."""

    registry_id: str

    def fetch(self, product: ProductRef, scope: dict[str, Any]) -> list[RegistryFact]: ...
