"""Порт персистенции человеческих решений G1–G4 — ``GateStore`` (§8, S9, I6/I7).

Гейт не считается пройденным без строки в ``gate_decisions``; носитель ``reviewed_by``.
``GateDecision`` — доменный объект; до T2.1 объявлен здесь (переносится в ``domain/``).
Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol, runtime_checkable


@dataclass(frozen=True)
class GateDecision:
    """Решение человека на гейте: что, кто, когда, заметки (§8)."""

    run_id: str
    gate: str
    target: str
    actor: str
    ts: str
    decision: str = "accept"
    notes: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class GateStore(Protocol):
    """Персистенция и чтение решений гейтов (аудит-след, I6/I7)."""

    def save_gate_decision(self, d: GateDecision) -> None: ...

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...
