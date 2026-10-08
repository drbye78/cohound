"""Порт реестрового фетчера — ``RegistryFetcherPort`` (Ш3, R1).

Нейтральный порт: конкретные реестры (endpoint/формат/маппинг) — **данные** в
``registries/*.yaml``, обслуживаются generic config-driven адаптером вне домена (§1.3).
R1: каждый факт обязан нести ``source_url``. В профиле ``split`` исполняется в DMZ.

``RegistryFact`` — доменный объект; до расщепления схем (T1.2/T2.1) объявлен здесь как
минимальная структура (переносится в ``domain/`` в T2.1). Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from core.ports.types import ProductRef


@dataclass(frozen=True)
class RegistryFact:
    """Реестровый факт (R1: ``source_url`` обязателен)."""

    registry_id: str
    product_id: str
    source_url: str
    fact_json: dict[str, Any] = field(default_factory=dict)
    fetched_at: str = ""


@runtime_checkable
class RegistryFetcherPort(Protocol):
    """Обслуживает один реестр по данным конфигурации. Возвращает факты с ``source_url``."""

    registry_id: str

    def fetch(self, product: ProductRef, scope: dict[str, Any]) -> list[RegistryFact]: ...
