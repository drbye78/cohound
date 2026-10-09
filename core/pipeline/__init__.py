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
from core.pipeline.retrieve import (
    PROFILE_DESCRIPTIONS,
    RetrievalProfile,
    retrieve,
    rrf_fuse_chunks,
)
from core.pipeline.experiments import (
    DEFAULT_PROFILES,
    ExperimentReport,
    ProfileExperiment,
    load_retrieval_golden,
    run_retrieval_experiment,
)
from core.pipeline.prompt import (
    TEMPLATE_VERSION,
    CompiledPrompt,
    compile_prompt,
    render_queries,
    save_prompt,
)
from core.pipeline.compute import (
    ScoreBundle,
    assert_publishable,
    compute,
    per_product_bundle,
    publish_warnings,
)
from core.pipeline.evaluate import (
    EvaluationOutcome,
    evaluate_batch,
    evaluate_cell,
    evaluate_phase,
    retrieve_for_cell,
    run_queries_for_cell,
)
from core.pipeline.validate import (
    ValidationFailureKind,
    ValidationResult,
    validate_output,
)
from core.pipeline.report import (
    Artifacts,
    CrossCycleDiffError,
    PublishBlockedError,
    build_manifest,
    render_battle_card,
    render_diff,
    render_nda_questions,
    render_preliminary_findings,
    write_reports,
    write_run_manifest,
)
from core.pipeline.extraction_eval import (
    ExtractionCaseResult,
    ExtractionGoldenCase,
    ExtractionReport,
    compare_extractors,
    extractor_version_changed,
    load_extraction_golden,
    run_extraction_golden,
)
from core.pipeline.metrics import (
    RunMetrics,
    collect_phase_durations,
    compute_metrics,
)
from core.pipeline.search_log import (
    SearchBudget,
    SearchEffort,
    external_search,
    record_search,
    termination_for,
)
from core.pipeline.discovery import (
    DiscoveryCandidate,
    DiscoveryReport,
    discover,
    review_candidates,
)
from core.pipeline.passport_scout import (
    DISCOVERY_TEMPLATE_VERSION,
    DiscoveryPassportHypothesis,
    PassportScoutError,
    PassportScoutReport,
    build_passport_draft,
    scout_passport,
    write_passport_draft,
)
from core.pipeline.adversarial import (
    AdversarialBreachError,
    AdversarialCase,
    AdversarialReport,
    load_adversarial,
    run_adversarial,
)
from core.pipeline.serp_loop import (
    SerpLoopError,
    SerpLoopResult,
    SerpQuery,
    SerpRound,
    derive_round_build_id,
    invalidate_affected,
    run_serp_loop,
    run_serp_round,
)
from core.pipeline.phases import IMPLEMENTED_PHASES, build_phases
from core.pipeline.golden import (
    ERROR_CLASSES,
    GoldenCaseResult,
    GoldenReport,
    GoldenThresholdError,
    GoldenThresholds,
    assert_golden,
    check_thresholds,
    evaluate_golden,
    run_golden,
)
from core.pipeline.review import (
    PromotionResult,
    ReviewError,
    ReviewItem,
    promote,
    promote_batch,
    review_queue,
)
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
    # retrieve (T8.5, Ш6)
    "RetrievalProfile",
    "PROFILE_DESCRIPTIONS",
    "retrieve",
    "rrf_fuse_chunks",
    # experiments (T8.6, R9/R10)
    "DEFAULT_PROFILES",
    "ExperimentReport",
    "ProfileExperiment",
    "load_retrieval_golden",
    "run_retrieval_experiment",
    # prompt (T9.1, Ш6)
    "TEMPLATE_VERSION",
    "CompiledPrompt",
    "compile_prompt",
    "render_queries",
    "save_prompt",
    # compute (T10.1, Ш8)
    "ScoreBundle",
    "compute",
    "per_product_bundle",
    "publish_warnings",
    "assert_publishable",
    # evaluate (T9.3)/validate (T9.4), Ш6
    "EvaluationOutcome",
    "evaluate_cell",
    "evaluate_batch",
    "run_queries_for_cell",
    "retrieve_for_cell",
    "ValidationResult",
    "ValidationFailureKind",
    "validate_output",
    # report (T10.2), §9 — G4/I11
    "Artifacts",
    "PublishBlockedError",
    "CrossCycleDiffError",
    "write_run_manifest",
    "render_battle_card",
    "render_nda_questions",
    "render_preliminary_findings",
    "render_diff",
    "write_reports",
    # extraction-eval (T8.7), §13
    "ExtractionGoldenCase",
    "ExtractionCaseResult",
    "ExtractionReport",
    "load_extraction_golden",
    "run_extraction_golden",
    "compare_extractors",
    "extractor_version_changed",
    # metrics (T12.1), §B.2
    "RunMetrics",
    "compute_metrics",
    "collect_phase_durations",
    # search_log (T11.2), I16/I23
    "SearchBudget",
    "SearchEffort",
    "record_search",
    "termination_for",
    "external_search",
    # discovery (T11.4), G2/I19
    "DiscoveryCandidate",
    "DiscoveryReport",
    "discover",
    "review_candidates",
    # passport-scout (M1), черновик паспорта
    "DISCOVERY_TEMPLATE_VERSION",
    "DiscoveryPassportHypothesis",
    "PassportScoutReport",
    "PassportScoutError",
    "build_passport_draft",
    "scout_passport",
    "write_passport_draft",
    # adversarial (T12.3), R8
    "AdversarialCase",
    "AdversarialReport",
    "AdversarialBreachError",
    "run_adversarial",
    "load_adversarial",
    # serp_loop (T11.3), K2/§13
    "SerpQuery",
    "SerpRound",
    "SerpLoopResult",
    "SerpLoopError",
    "derive_round_build_id",
    "run_serp_round",
    "invalidate_affected",
    "run_serp_loop",
    # phases (интеграция Ш1–Ш9, composition root)
    "build_phases",
    "IMPLEMENTED_PHASES",
    "evaluate_phase",
    "build_manifest",
    # golden (T12.4), R6/R12
    "GoldenThresholds",
    "GoldenCaseResult",
    "GoldenReport",
    "GoldenThresholdError",
    "run_golden",
    "evaluate_golden",
    "check_thresholds",
    "assert_golden",
    # review (§7/§D.6), I6/I7
    "ReviewItem",
    "PromotionResult",
    "ReviewError",
    "review_queue",
    "promote",
    "promote_batch",
]
