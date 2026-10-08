"""Transitional re-export: ``GateDecision``/``GateStore`` → канонические доменные/порт (W3).

До T2.1 доменный объект ``GateDecision`` и порт ``GateStore`` были объявлены здесь.
Теперь ``GateDecision`` — доменная модель (``core.domain.models``), а ``GateStore`` —
контракт в ``core.ports.store``. Модуль сохранён как re-export-шим для обратной
совместимости импортов ``from core.ports.gate_store import ...``.
"""

from __future__ import annotations

from core.domain import GateDecision
from core.ports.store import GateStore

__all__ = ["GateDecision", "GateStore"]
