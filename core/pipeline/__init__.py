"""coHound — конвейер (L1): фазы Ш1–Ш9, гейты G1–G4, идемпотентность (W4).

Пакет принимает порты как аргументы (ручной DI, A1) и никогда не инстанцирует адаптеры:
сборка зависимостей — задача composition root (``cli/``, Z0). Здесь живут:
``runner.py`` (оркестрация фаз + ``PhaseResult``), ``gates.py`` (человеческие решения
G1–G4, S9) и ``idempotency.py`` (детерминированные ключи upsert, S8/R15).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from core.pipeline.gates import (
    gate_decision_for,
    gate_g1,
    gate_g2,
    gate_g3,
    gate_g4,
    is_gate_passed,
    record_decision,
)
from core.pipeline.idempotency import (
    evidence_id_for,
    mark_stale,
    upsert_cells,
    upsert_evidence,
)
from core.pipeline.runner import (
    GATE_PHASES,
    PHASES,
    PhaseResult,
    Ports,
    RunContext,
    Runner,
    RunReport,
    run_isolated,
)

__all__ = [
    # runner (T4.1)
    "Runner",
    "RunReport",
    "RunContext",
    "Ports",
    "PhaseResult",
    "PHASES",
    "GATE_PHASES",
    "run_isolated",
    # gates (T4.2)
    "record_decision",
    "gate_g1",
    "gate_g2",
    "gate_g3",
    "gate_g4",
    "is_gate_passed",
    "gate_decision_for",
    # idempotency (T4.4)
    "evidence_id_for",
    "mark_stale",
    "upsert_cells",
    "upsert_evidence",
]
