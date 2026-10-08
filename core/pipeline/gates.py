"""Гейты G1–G4 — человеческие решения и их персистенция (02 §A.2, S9, I6/I7).

Гейт **никогда не проходится автоматически**: оркестратор не «одобряет» гейт сам, он лишь
регистрирует решение, поданное человеком через CLI (W5). Носитель ``reviewed_by`` и
аудит-след — таблица ``gate_decisions`` (S9). Гейт не считается пройденным без строки в
``gate_decisions``; проверка — :func:`is_gate_passed`.

Нейтрален (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from typing import Any

from core.domain import Gate, GateDecision
from core.ports.store import GateStore

__all__ = [
    "record_decision",
    "gate_g1",
    "gate_g2",
    "gate_g3",
    "gate_g4",
    "is_gate_passed",
    "gate_decision_for",
]


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _as_gate(gate: Gate | str) -> Gate:
    """Нормализует гейт к ``Gate`` (принимает enum или строку ``"G1"``)."""
    return gate if isinstance(gate, Gate) else Gate(gate)


def _decision_id(
    run_id: str, gate: Gate, target: str | None, actor: str, notes: str | None
) -> str:
    """Детерминированный ``decision_id`` решения гейта.

    Ключ **без** метки времени: повторная подача того же решения (run/gate/target/actor/notes)
    идемпотентно перезаписывает ту же строку ``gate_decisions`` (S9), а не плодит дубли.
    Разные решения (другие ``notes``/``actor``) дают разные ``decision_id`` и сохраняются
    в аудит-след отдельно.
    """
    raw = f"{run_id}|{gate.value}|{target or ''}|{actor}|{notes or ''}".encode()
    return "gd_" + hashlib.sha256(raw).hexdigest()[:24]


def record_decision(
    store: GateStore,
    run_id: str,
    gate: Gate | str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
) -> GateDecision:
    """Строит и персистит человеческое решение гейта (S9).

    Гейты **не** автоматические (I6/I7): ``actor`` обязателен и непуст. Решение пишется
    через ``GateStore.save_gate_decision``; без этой строки гейт не считается пройденным.

    :param clock: опциональные часы с ``now_iso()`` (тестируемость); иначе системное UTC.
    :raises ValueError: пустой ``actor``.
    """
    normalized = _as_gate(gate)
    actor = (actor or "").strip()
    if not actor:
        raise ValueError(
            f"гейт {normalized.value} не может быть пройден без actor (человек, I6/I7)"
        )
    decided_at = clock.now_iso() if clock is not None else _now_iso()
    decision = GateDecision(
        decision_id=_decision_id(run_id, normalized, target, actor, notes),
        run_id=run_id,
        gate=normalized,
        target=target,
        actor=actor,
        decided_at=decided_at,
        notes=notes,
    )
    store.save_gate_decision(decision)
    return decision


def gate_g1(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
) -> GateDecision:
    """G1 — утверждение плана (когорта/роли/перенормировка/объём)."""
    return record_decision(
        store, run_id, Gate.G1, actor=actor, target=target, notes=notes, clock=clock
    )


def gate_g2(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
) -> GateDecision:
    """G2 — утверждение матрицы роль×продукт (relationship_evidence)."""
    return record_decision(
        store, run_id, Gate.G2, actor=actor, target=target, notes=notes, clock=clock
    )


def gate_g3(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
) -> GateDecision:
    """G3 — strict-ревью ячеек (set-status/promote/fix-evidence/reject)."""
    return record_decision(
        store, run_id, Gate.G3, actor=actor, target=target, notes=notes, clock=clock
    )


def gate_g4(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
) -> GateDecision:
    """G4 — публикация (publish) или удержание (hold) отчёта."""
    return record_decision(
        store, run_id, Gate.G4, actor=actor, target=target, notes=notes, clock=clock
    )


def is_gate_passed(store: GateStore, run_id: str, gate: Gate | str) -> bool:
    """Пройден ли гейт: есть ли хотя бы одна строка решения в ``gate_decisions`` (S9).

    Читает ``GateStore.iter_gate_decisions``; отсутствие строки → ``False`` (гейт не пройден,
    даже если все прочие фазы завершены).
    """
    normalized = _as_gate(gate)
    return any(d.gate is normalized for d in store.iter_gate_decisions(run_id))


def gate_decision_for(
    store: GateStore, run_id: str, gate: Gate | str
) -> GateDecision | None:
    """Последнее решение по гейту (аудит/CLI), либо ``None``.

    ``iter_gate_decisions`` упорядочивает по ``decided_at``; при равных метках — по
    ``decision_id``. Возвращается последняя строка.
    """
    normalized = _as_gate(gate)
    last: GateDecision | None = None
    for decision in store.iter_gate_decisions(run_id):
        if decision.gate is normalized:
            last = decision
    return last
