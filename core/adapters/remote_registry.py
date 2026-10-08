"""Remote-прокси реестрового фетчера — ``RemoteRegistry`` (W13/T13.3; 07 §4).

Реализация ``RegistryFetcherPort`` в ядре для профиля ``split``: generic
config-driven фетчер исполняется в DMZ, ядро вызывает его через
``ContentGateway``. Контракт порта **не меняется** (07 §4).

R1: 100% фактов обязаны нести ``source_url``; прокси проверяет это до сборки
``RegistryFact`` (ядро не доверяет edge, 07 §5). Идентификатор реестра —
данные (``registries/*.yaml``), задаётся при wiring, а не кодируется здесь.
Нейтрален (§1.3).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from core.adapters.remote_gateway import RemoteContentError, as_items
from core.domain import RegistryFact
from core.ports.gateway import ContentGateway
from core.ports.types import ProductRef

__all__ = ["RemoteRegistry"]


class RemoteRegistry:
    """``RegistryFetcherPort`` через DMZ-шов. Drop-in для реального фетчера."""

    def __init__(self, gateway: ContentGateway, *, registry_id: str) -> None:
        if not registry_id:
            raise ValueError("registry_id обязателен (данные конфигурации реестра)")
        self._gateway = gateway
        self.registry_id = registry_id

    @property
    def available(self) -> bool:
        """Доступен ли транспорт шва (``httpx`` или внедрённый ``transport``)."""
        return bool(getattr(self._gateway, "available", True))

    def fetch(self, product: ProductRef, scope: dict[str, Any]) -> list[RegistryFact]:
        """``/v1/registry`` → список ``RegistryFact`` с обязательным ``source_url`` (R1)."""
        payload = {
            "registry_id": self.registry_id,
            "product_ref": {
                "product_id": product.product_id,
                "canonical_name": product.canonical_name,
                "vendor": product.vendor,
                "jurisdiction": product.jurisdiction,
                "extra": dict(product.extra),
            },
            "scope": dict(scope),
        }
        response = self._gateway.call("registry", payload)
        items = as_items(response, op="registry")
        run_id = str(scope.get("run_id", ""))

        facts: list[RegistryFact] = []
        for item in items:
            if not isinstance(item, Mapping):
                raise RemoteContentError("registry: элемент ответа не является отображением")
            source_url = item.get("source_url")
            if not source_url:
                raise RemoteContentError("R1: реестровый факт без source_url")
            facts.append(
                RegistryFact(
                    run_id=run_id,
                    product_id=product.product_id,
                    registry_id=self.registry_id,
                    fact_json=_fact_json(item.get("fact_json")),
                    source_url=str(source_url),
                    fetched_at=(
                        None
                        if item.get("fetched_at") is None
                        else str(item["fetched_at"])
                    ),
                )
            )
        return facts


def _fact_json(value: Any) -> str:
    """``fact_json`` — строка (домен); dict/список из edge сериализуется детерминированно."""
    if isinstance(value, str):
        return value
    if value is None:
        return "{}"
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
