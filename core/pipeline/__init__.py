"""coHound — конвейер (L1): фазы Ш1–Ш9, гейты G1–G4, идемпотентность (W4).

Пакет принимает порты как аргументы (ручной DI, A1) и никогда не инстанцирует адаптеры:
сборка зависимостей — задача composition root (``cli/``, Z0). Здесь живут:
``runner.py`` (оркестрация фаз + ``PhaseResult``), ``gates.py`` (человеческие решения
G1–G4, S9), ``idempotency.py`` (детерминированные ключи upsert, S8/R15),
``normalize.py``/``lint.py``/``plan.py`` (нормализация входов, блокирующий линтер
и воспроизводимый план прогона — W5/T5.2/T5.4), ``registry.py``/``inheritance.py``
(детерминированные волны Ш3/Ш4 без LLM — W6/T6.3/T6.4).

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
from core.pipeline.lint import (
    RULES,
    LintReport,
    LintRule,
    assert_lint,
    lint_run_inputs,
)
from core.pipeline.normalize import (
    RunInputs,
    cross_check,
    load_inputs,
    load_scope_attestation,
)
from core.pipeline.inheritance import apply_precedence, inheritance_wave
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.plan import RunPlan, derive_run_id, load_plan, plan
from core.pipeline.registry import (
    registry_covered_params,
    registry_wave,
    resolve_registry_ids,
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
    # normalize (T5.4)
    "RunInputs",
    "load_inputs",
    "load_scope_attestation",
    "cross_check",
    # lint (T5.2)
    "LintReport",
    "LintRule",
    "RULES",
    "lint_run_inputs",
    "assert_lint",
    # plan (T5.4)
    "RunPlan",
    "plan",
    "load_plan",
    "derive_run_id",
    # registry wave (T6.3)
    "registry_wave",
    "registry_covered_params",
    "resolve_registry_ids",
    # inheritance wave (T6.4)
    "inheritance_wave",
    "apply_precedence",
    # corpus (T7.4, Ш5)
    "build_corpus",
    "IndexBuild",
]
