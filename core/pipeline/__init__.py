"""coHound — конвейер (L1): фазы Ш1–Ш9, гейты G1–G4, идемпотентность (W4).

Пакет принимает порты как аргументы (ручной DI, A1) и никогда не инстанцирует адаптеры:
сборка зависимостей — задача composition root (``cli/``, Z0). Здесь живут:
``runner.py`` (оркестрация фаз + ``PhaseResult``), ``gates.py`` (решения гейтов G1–G4, S9),
``gate_subject.py`` (предмет решения G3/G4 и исходящий запрос внешней системе),
``profile_attestation.py`` (аттестация профиля пакета-якоря: дайджест,
диагностика расхождения, резолвер гейта), ``idempotency.py`` (детерминированные ключи
upsert, S8/R15), ``normalize.py``/``lint.py``/``plan.py`` (нормализация входов,
блокирующий линтер и воспроизводимый план прогона — W5/T5.2/T5.4),
``registry.py``/``inheritance.py`` (детерминированные волны Ш3/Ш4 без LLM — W6/T6.3/T6.4).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.

**Фасад ленивый (PEP 562, L0 плана декомпозиции).** Импорты модулей объявлены только
под ``TYPE_CHECKING`` — ради ``mypy``: без них PEP 562-``__getattr__`` вернул бы ``Any``,
и типизация 60 вызовов ``from core.pipeline import X`` молча деградировала бы. Во время
исполнения имя резолвится при первом обращении через :func:`__getattr__` и кэшируется в
``globals()``. Причина: ``import core.pipeline.X`` больше не исполняет пакет целиком —
``corpus`` (10k строк фазы Ш5) перестал быть платой за любое обращение к фасаду.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.pipeline.adversarial import (
        AdversarialBreachError,
        AdversarialCase,
        AdversarialReport,
        load_adversarial,
        run_adversarial,
    )
    from core.pipeline.compute import (
        ScoreBundle,
        assert_publishable,
        compute,
        per_product_bundle,
        publish_warnings,
        publishability_notes,
    )
    from core.pipeline.corpus import IndexBuild, build_corpus
    from core.pipeline.crawl_sources import CrawlTarget, resolve_crawl_target
    from core.pipeline.discovery import (
        DiscoveryCandidate,
        DiscoveryReport,
        discover,
        review_candidates,
    )
    from core.pipeline.evaluate import (
        EvaluationOutcome,
        evaluate_batch,
        evaluate_cell,
        evaluate_phase,
        retrieve_for_cell,
        run_queries_for_cell,
    )
    from core.pipeline.evaluator_benchmark import (
        BENCHMARK_CONTRACT,
        PRECONDITION_GOLDEN_DECLINED,
        BenchmarkContractError,
        BenchmarkPreconditionError,
        BenchmarkReport,
        CellCase,
        CellMeasurement,
        ModelMetrics,
        ModelUnderTest,
        PreparedCases,
        prepare_cases,
        run_benchmark,
    )
    from core.pipeline.experiments import (
        DEFAULT_PROFILES,
        ExperimentReport,
        ProfileExperiment,
        load_retrieval_golden,
        run_retrieval_experiment,
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
    from core.pipeline.fetch_plan import resolve_fetch_plan
    from core.pipeline.gate_subject import (
        DECISION_CONTRACT,
        PUBLISHED_ARTIFACTS,
        GateSubject,
        SubjectResolver,
        describe_subject_diff,
        gate_request_payload,
        publication_subject,
        read_gate_request_digest,
        read_gate_request_subject,
        review_subject,
        write_gate_request,
    )
    from core.pipeline.gates import (
        GateCheck,
        evaluate_gate,
        gate_decision_for,
        gate_g1,
        gate_g2,
        gate_g3,
        gate_g4,
        is_gate_passed,
        record_decision,
    )
    from core.pipeline.golden import (
        GoldenCaseResult,
        GoldenReport,
        GoldenThresholdError,
        GoldenThresholds,
        assert_golden,
        check_thresholds,
        evaluate_golden,
        run_golden,
    )
    from core.pipeline.idempotency import (
        evidence_id_for,
        mark_stale,
        upsert_cells,
        upsert_evidence,
    )
    from core.pipeline.inheritance import apply_precedence, inheritance_wave
    from core.pipeline.lint import (
        RULES,
        LintReport,
        LintRule,
        assert_lint,
        lint_run_inputs,
    )
    from core.pipeline.metrics import (
        RunMetrics,
        collect_phase_durations,
        compute_metrics,
    )
    from core.pipeline.normalize import (
        RunInputs,
        cross_check,
        load_inputs,
        load_scope_attestation,
    )
    from core.pipeline.package_add_product import (
        AddProductError,
        AddProductPlan,
        DiscoveredSite,
        apply_add_product,
        plan_add_product,
        write_domain_profiles_with_new_keys,
    )
    from core.pipeline.package_admin import (
        OpResult,
        PackageAdminError,
        add_baseline,
        add_module,
        add_param,
        add_role,
        bump_version,
        clear_scope_attestation,
        create_package,
        find_baselines,
        get_baseline,
        get_cohort,
        get_module,
        get_param,
        get_role,
        get_run_config,
        get_scope_attestation,
        list_baselines,
        list_golden,
        list_modules,
        list_params,
        list_roles,
        next_cohort_ref,
        remove_baseline,
        remove_module,
        remove_param,
        remove_role,
        renormalize_modules,
        set_archetype,
        set_cohort,
        set_scope_attestation,
        show_package,
        update_baseline,
        update_cohort,
        update_module,
        update_param,
        update_role,
        validate_package,
    )
    from core.pipeline.package_candidates import (
        CANDIDATE_KINDS,
        Candidate,
        apply_candidate,
        apply_candidates,
        dumps_candidates,
        loads_candidates,
        plan_candidates,
    )
    from core.pipeline.package_discovery import (
        ArchetypeHypothesis,
        BaselineProposal,
        compose_baselines,
        infer_archetype,
        suggest_cohort_members,
        suggest_modules,
        suggest_roles,
    )
    from core.pipeline.package_io import (
        ARTIFACT_FILES,
        PackageArtifacts,
        PackageSummary,
        append_apply_log,
        assert_no_new_blocking,
        atomic_write_csv,
        atomic_write_text,
        build_run_inputs,
        delete_artifact,
        get_artifact,
        list_artifacts,
        load_artifacts,
        matches,
        prospective_lint,
        save_artifact,
        scan_packages,
        validate_case_id,
    )
    from core.pipeline.package_scout import (
        BASELINE_TEMPLATE_VERSION,
        CATALOG_TEMPLATE_VERSION,
        LANDSCAPE_TEMPLATE_VERSION,
        PackageScoutReport,
        ScoutArchetypeSignal,
        ScoutCatalogHypothesis,
        ScoutLandscapeHypothesis,
        ScoutModule,
        ScoutParam,
        ScoutProduct,
        ScoutRole,
        ScoutUpstream,
        build_seed_from_hypotheses,
        scout_package,
        write_scout_artifacts,
    )
    from core.pipeline.package_seeds import (
        CORPUS_URLS_FORMAT,
        SeedsExport,
        SeedsOpResult,
        product_seed_rows,
        seeds_add,
        seeds_disable,
        seeds_enable,
        seeds_export,
        seeds_set_enabled,
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
    from core.pipeline.phases import IMPLEMENTED_PHASES, build_phases
    from core.pipeline.plan import RunPlan, derive_run_id, load_plan, plan
    from core.pipeline.profile_attestation import (
        PROFILE_MATERIAL_KEYS,
        ProfileGateResult,
        describe_material_diff,
        make_profile_gate,
        profile_attestation_id,
        profile_digest,
        profile_material,
    )
    from core.pipeline.prompt import (
        TEMPLATE_VERSION,
        CompiledPrompt,
        PromptArchive,
        compile_prompt,
        render_queries,
        save_prompt,
    )
    from core.pipeline.registry import (
        registry_covered_params,
        registry_wave,
        resolve_registry_ids,
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
    from core.pipeline.retrieve import (
        PROFILE_DESCRIPTIONS,
        RetrievalProfile,
        retrieve,
        rrf_fuse_chunks,
    )
    from core.pipeline.review import (
        PromotionResult,
        ReviewError,
        ReviewItem,
        promote,
        promote_batch,
        review_queue,
    )
    from core.pipeline.runner import (
        GATE_PHASES,
        PHASES,
        GateRuntime,
        PhaseResult,
        Ports,
        RunContext,
        Runner,
        RunReport,
        run_isolated,
    )
    from core.pipeline.search_log import (
        SearchBudget,
        SearchEffort,
        external_search,
        record_search,
        termination_for,
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
    from core.pipeline.validate import (
        ValidationFailureKind,
        ValidationResult,
        validate_output,
    )

#: Порядок намеренно сгруппирован по подсистемам (runner → gates → …), а не по алфавиту:
#: группы повторяют раскладку модулей и задокументированы комментариями. Сортировка
#: перемешала бы соседние модули и оторвала комментарии от своих имён — публичная
#: поверхность пакета читается группами.
__all__ = [  # noqa: RUF022
    # runner (T4.1)
    "Runner",
    "RunReport",
    "RunContext",
    "Ports",
    "PhaseResult",
    "PHASES",
    "GATE_PHASES",
    "run_isolated",
    # gates (T4.2) — G1/G2 внутренняя аттестация, G3/G4 точки интеграции
    "record_decision",
    "gate_g1",
    "gate_g2",
    "gate_g3",
    "gate_g4",
    "is_gate_passed",
    "gate_decision_for",
    "evaluate_gate",
    "GateCheck",
    "GateRuntime",
    # предмет решения G3/G4 и исходящий запрос (точки интеграции)
    "GateSubject",
    "SubjectResolver",
    "DECISION_CONTRACT",
    "PUBLISHED_ARTIFACTS",
    "review_subject",
    "publication_subject",
    "describe_subject_diff",
    "gate_request_payload",
    "read_gate_request_digest",
    "read_gate_request_subject",
    "write_gate_request",
    # аттестация профиля пакета-якоря (G1/G2)
    "PROFILE_MATERIAL_KEYS",
    "ProfileGateResult",
    "describe_material_diff",
    "make_profile_gate",
    "profile_attestation_id",
    "profile_digest",
    "profile_material",
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
    "resolve_fetch_plan",
    # crawl_sources (Lane A: source-routing)
    "CrawlTarget",
    "resolve_crawl_target",
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
    "PromptArchive",
    "compile_prompt",
    "render_queries",
    "save_prompt",
    # compute (T10.1, Ш8)
    "ScoreBundle",
    "compute",
    "per_product_bundle",
    "publish_warnings",
    "publishability_notes",
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
    # evaluator benchmark (A3, Ш6) — измерение моделей оценщика
    "BENCHMARK_CONTRACT",
    "PRECONDITION_GOLDEN_DECLINED",
    "BenchmarkContractError",
    "BenchmarkPreconditionError",
    "BenchmarkReport",
    "CellCase",
    "CellMeasurement",
    "ModelMetrics",
    "ModelUnderTest",
    "PreparedCases",
    "prepare_cases",
    "run_benchmark",
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
    # add-product (авторское добавление продукта в когорту)
    "AddProductError",
    "AddProductPlan",
    "DiscoveredSite",
    "apply_add_product",
    "plan_add_product",
    "write_domain_profiles_with_new_keys",
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
    # package-io / package-admin / package-discovery (итерация I1)
    "PackageArtifacts",
    "ARTIFACT_FILES",
    "atomic_write_text",
    "atomic_write_csv",
    "validate_case_id",
    "load_artifacts",
    "list_artifacts",
    "get_artifact",
    "save_artifact",
    "delete_artifact",
    "build_run_inputs",
    "prospective_lint",
    "assert_no_new_blocking",
    "append_apply_log",
    "PackageSummary",
    "scan_packages",
    "matches",
    "OpResult",
    "PackageAdminError",
    "create_package",
    "show_package",
    "validate_package",
    "set_archetype",
    "bump_version",
    "add_module",
    "update_module",
    "remove_module",
    "renormalize_modules",
    "add_role",
    "update_role",
    "remove_role",
    "get_role",
    "list_roles",
    "add_param",
    "update_param",
    "remove_param",
    "get_param",
    "list_params",
    "get_baseline",
    "find_baselines",
    "list_baselines",
    "add_baseline",
    "update_baseline",
    "remove_baseline",
    "list_golden",
    "get_run_config",
    "get_scope_attestation",
    "set_cohort",
    "update_cohort",
    "next_cohort_ref",
    "set_scope_attestation",
    "clear_scope_attestation",
    "get_cohort",
    "get_module",
    "list_modules",
    "ArchetypeHypothesis",
    "BaselineProposal",
    "infer_archetype",
    "compose_baselines",
    "suggest_modules",
    "suggest_roles",
    "suggest_cohort_members",
    "CANDIDATE_KINDS",
    "Candidate",
    "plan_candidates",
    "apply_candidate",
    "apply_candidates",
    "dumps_candidates",
    "loads_candidates",
    # package-scout (веб-заземлённая разведка пакета категории)
    "LANDSCAPE_TEMPLATE_VERSION",
    "CATALOG_TEMPLATE_VERSION",
    "BASELINE_TEMPLATE_VERSION",
    "ScoutUpstream",
    "ScoutProduct",
    "ScoutModule",
    "ScoutRole",
    "ScoutArchetypeSignal",
    "ScoutLandscapeHypothesis",
    "ScoutParam",
    "ScoutCatalogHypothesis",
    "PackageScoutReport",
    "scout_package",
    "build_seed_from_hypotheses",
    "write_scout_artifacts",
    # package-seeds (авторский seed-набор, 09 §2.3/§7)
    "CORPUS_URLS_FORMAT",
    "SeedsOpResult",
    "SeedsExport",
    "product_seed_rows",
    "seeds_add",
    "seeds_set_enabled",
    "seeds_enable",
    "seeds_disable",
    "seeds_export",
]

#: Ленивый экспорт (PEP 562): публичное имя → модуль, в котором оно объявлено.
#: Карта механически выведена из импортов выше (38 модулей) и обязана совпадать с
#: ``__all__`` — пин ``tests/pipeline/test_pipeline_exports.py``.
#: ``IndexBuild`` объявлен в ``index_build`` (L0: вынесен из ``corpus``), поэтому ведёт
#: туда, а не в ``corpus``: имя и место объявления не обязаны совпадать.
_LAZY: dict[str, str] = {
    # adversarial
    "AdversarialBreachError": "core.pipeline.adversarial",
    "AdversarialCase": "core.pipeline.adversarial",
    "AdversarialReport": "core.pipeline.adversarial",
    "load_adversarial": "core.pipeline.adversarial",
    "run_adversarial": "core.pipeline.adversarial",
    # compute
    "ScoreBundle": "core.pipeline.compute",
    "assert_publishable": "core.pipeline.compute",
    "compute": "core.pipeline.compute",
    "per_product_bundle": "core.pipeline.compute",
    "publish_warnings": "core.pipeline.compute",
    "publishability_notes": "core.pipeline.compute",
    # corpus
    "IndexBuild": "core.pipeline.index_build",
    "build_corpus": "core.pipeline.corpus",
    # crawl_sources
    "CrawlTarget": "core.pipeline.crawl_sources",
    "resolve_crawl_target": "core.pipeline.crawl_sources",
    # discovery
    "DiscoveryCandidate": "core.pipeline.discovery",
    "DiscoveryReport": "core.pipeline.discovery",
    "discover": "core.pipeline.discovery",
    "review_candidates": "core.pipeline.discovery",
    # evaluate
    "EvaluationOutcome": "core.pipeline.evaluate",
    "evaluate_batch": "core.pipeline.evaluate",
    "evaluate_cell": "core.pipeline.evaluate",
    "evaluate_phase": "core.pipeline.evaluate",
    "retrieve_for_cell": "core.pipeline.evaluate",
    "run_queries_for_cell": "core.pipeline.evaluate",
    # evaluator_benchmark
    "BENCHMARK_CONTRACT": "core.pipeline.evaluator_benchmark",
    "PRECONDITION_GOLDEN_DECLINED": "core.pipeline.evaluator_benchmark",
    "BenchmarkContractError": "core.pipeline.evaluator_benchmark",
    "BenchmarkPreconditionError": "core.pipeline.evaluator_benchmark",
    "BenchmarkReport": "core.pipeline.evaluator_benchmark",
    "CellCase": "core.pipeline.evaluator_benchmark",
    "CellMeasurement": "core.pipeline.evaluator_benchmark",
    "ModelMetrics": "core.pipeline.evaluator_benchmark",
    "ModelUnderTest": "core.pipeline.evaluator_benchmark",
    "PreparedCases": "core.pipeline.evaluator_benchmark",
    "prepare_cases": "core.pipeline.evaluator_benchmark",
    "run_benchmark": "core.pipeline.evaluator_benchmark",
    # experiments
    "DEFAULT_PROFILES": "core.pipeline.experiments",
    "ExperimentReport": "core.pipeline.experiments",
    "ProfileExperiment": "core.pipeline.experiments",
    "load_retrieval_golden": "core.pipeline.experiments",
    "run_retrieval_experiment": "core.pipeline.experiments",
    # extraction_eval
    "ExtractionCaseResult": "core.pipeline.extraction_eval",
    "ExtractionGoldenCase": "core.pipeline.extraction_eval",
    "ExtractionReport": "core.pipeline.extraction_eval",
    "compare_extractors": "core.pipeline.extraction_eval",
    "extractor_version_changed": "core.pipeline.extraction_eval",
    "load_extraction_golden": "core.pipeline.extraction_eval",
    "run_extraction_golden": "core.pipeline.extraction_eval",
    # fetch_plan
    "resolve_fetch_plan": "core.pipeline.fetch_plan",
    # gate_subject
    "DECISION_CONTRACT": "core.pipeline.gate_subject",
    "PUBLISHED_ARTIFACTS": "core.pipeline.gate_subject",
    "GateSubject": "core.pipeline.gate_subject",
    "SubjectResolver": "core.pipeline.gate_subject",
    "describe_subject_diff": "core.pipeline.gate_subject",
    "gate_request_payload": "core.pipeline.gate_subject",
    "publication_subject": "core.pipeline.gate_subject",
    "read_gate_request_digest": "core.pipeline.gate_subject",
    "read_gate_request_subject": "core.pipeline.gate_subject",
    "review_subject": "core.pipeline.gate_subject",
    "write_gate_request": "core.pipeline.gate_subject",
    # gates
    "GateCheck": "core.pipeline.gates",
    "evaluate_gate": "core.pipeline.gates",
    "gate_decision_for": "core.pipeline.gates",
    "gate_g1": "core.pipeline.gates",
    "gate_g2": "core.pipeline.gates",
    "gate_g3": "core.pipeline.gates",
    "gate_g4": "core.pipeline.gates",
    "is_gate_passed": "core.pipeline.gates",
    "record_decision": "core.pipeline.gates",
    # golden
    "GoldenCaseResult": "core.pipeline.golden",
    "GoldenReport": "core.pipeline.golden",
    "GoldenThresholdError": "core.pipeline.golden",
    "GoldenThresholds": "core.pipeline.golden",
    "assert_golden": "core.pipeline.golden",
    "check_thresholds": "core.pipeline.golden",
    "evaluate_golden": "core.pipeline.golden",
    "run_golden": "core.pipeline.golden",
    # idempotency
    "evidence_id_for": "core.pipeline.idempotency",
    "mark_stale": "core.pipeline.idempotency",
    "upsert_cells": "core.pipeline.idempotency",
    "upsert_evidence": "core.pipeline.idempotency",
    # inheritance
    "apply_precedence": "core.pipeline.inheritance",
    "inheritance_wave": "core.pipeline.inheritance",
    # lint
    "RULES": "core.pipeline.lint",
    "LintReport": "core.pipeline.lint",
    "LintRule": "core.pipeline.lint",
    "assert_lint": "core.pipeline.lint",
    "lint_run_inputs": "core.pipeline.lint",
    # metrics
    "RunMetrics": "core.pipeline.metrics",
    "collect_phase_durations": "core.pipeline.metrics",
    "compute_metrics": "core.pipeline.metrics",
    # normalize
    "RunInputs": "core.pipeline.normalize",
    "cross_check": "core.pipeline.normalize",
    "load_inputs": "core.pipeline.normalize",
    "load_scope_attestation": "core.pipeline.normalize",
    # package_add_product
    "AddProductError": "core.pipeline.package_add_product",
    "AddProductPlan": "core.pipeline.package_add_product",
    "DiscoveredSite": "core.pipeline.package_add_product",
    "apply_add_product": "core.pipeline.package_add_product",
    "plan_add_product": "core.pipeline.package_add_product",
    "write_domain_profiles_with_new_keys": "core.pipeline.package_add_product",
    # package_admin
    "OpResult": "core.pipeline.package_admin",
    "PackageAdminError": "core.pipeline.package_admin",
    "add_baseline": "core.pipeline.package_admin",
    "add_module": "core.pipeline.package_admin",
    "add_param": "core.pipeline.package_admin",
    "add_role": "core.pipeline.package_admin",
    "bump_version": "core.pipeline.package_admin",
    "clear_scope_attestation": "core.pipeline.package_admin",
    "create_package": "core.pipeline.package_admin",
    "find_baselines": "core.pipeline.package_admin",
    "get_baseline": "core.pipeline.package_admin",
    "get_cohort": "core.pipeline.package_admin",
    "get_module": "core.pipeline.package_admin",
    "get_param": "core.pipeline.package_admin",
    "get_role": "core.pipeline.package_admin",
    "get_run_config": "core.pipeline.package_admin",
    "get_scope_attestation": "core.pipeline.package_admin",
    "list_baselines": "core.pipeline.package_admin",
    "list_golden": "core.pipeline.package_admin",
    "list_modules": "core.pipeline.package_admin",
    "list_params": "core.pipeline.package_admin",
    "list_roles": "core.pipeline.package_admin",
    "next_cohort_ref": "core.pipeline.package_admin",
    "remove_baseline": "core.pipeline.package_admin",
    "remove_module": "core.pipeline.package_admin",
    "remove_param": "core.pipeline.package_admin",
    "remove_role": "core.pipeline.package_admin",
    "renormalize_modules": "core.pipeline.package_admin",
    "set_archetype": "core.pipeline.package_admin",
    "set_cohort": "core.pipeline.package_admin",
    "set_scope_attestation": "core.pipeline.package_admin",
    "show_package": "core.pipeline.package_admin",
    "update_baseline": "core.pipeline.package_admin",
    "update_cohort": "core.pipeline.package_admin",
    "update_module": "core.pipeline.package_admin",
    "update_param": "core.pipeline.package_admin",
    "update_role": "core.pipeline.package_admin",
    "validate_package": "core.pipeline.package_admin",
    # package_candidates
    "CANDIDATE_KINDS": "core.pipeline.package_candidates",
    "Candidate": "core.pipeline.package_candidates",
    "apply_candidate": "core.pipeline.package_candidates",
    "apply_candidates": "core.pipeline.package_candidates",
    "dumps_candidates": "core.pipeline.package_candidates",
    "loads_candidates": "core.pipeline.package_candidates",
    "plan_candidates": "core.pipeline.package_candidates",
    # package_discovery
    "ArchetypeHypothesis": "core.pipeline.package_discovery",
    "BaselineProposal": "core.pipeline.package_discovery",
    "compose_baselines": "core.pipeline.package_discovery",
    "infer_archetype": "core.pipeline.package_discovery",
    "suggest_cohort_members": "core.pipeline.package_discovery",
    "suggest_modules": "core.pipeline.package_discovery",
    "suggest_roles": "core.pipeline.package_discovery",
    # package_io
    "ARTIFACT_FILES": "core.pipeline.package_io",
    "PackageArtifacts": "core.pipeline.package_io",
    "PackageSummary": "core.pipeline.package_io",
    "append_apply_log": "core.pipeline.package_io",
    "assert_no_new_blocking": "core.pipeline.package_io",
    "atomic_write_csv": "core.pipeline.package_io",
    "atomic_write_text": "core.pipeline.package_io",
    "build_run_inputs": "core.pipeline.package_io",
    "delete_artifact": "core.pipeline.package_io",
    "get_artifact": "core.pipeline.package_io",
    "list_artifacts": "core.pipeline.package_io",
    "load_artifacts": "core.pipeline.package_io",
    "matches": "core.pipeline.package_io",
    "prospective_lint": "core.pipeline.package_io",
    "save_artifact": "core.pipeline.package_io",
    "scan_packages": "core.pipeline.package_io",
    "validate_case_id": "core.pipeline.package_io",
    # package_scout
    "BASELINE_TEMPLATE_VERSION": "core.pipeline.package_scout",
    "CATALOG_TEMPLATE_VERSION": "core.pipeline.package_scout",
    "LANDSCAPE_TEMPLATE_VERSION": "core.pipeline.package_scout",
    "PackageScoutReport": "core.pipeline.package_scout",
    "ScoutArchetypeSignal": "core.pipeline.package_scout",
    "ScoutCatalogHypothesis": "core.pipeline.package_scout",
    "ScoutLandscapeHypothesis": "core.pipeline.package_scout",
    "ScoutModule": "core.pipeline.package_scout",
    "ScoutParam": "core.pipeline.package_scout",
    "ScoutProduct": "core.pipeline.package_scout",
    "ScoutRole": "core.pipeline.package_scout",
    "ScoutUpstream": "core.pipeline.package_scout",
    "build_seed_from_hypotheses": "core.pipeline.package_scout",
    "scout_package": "core.pipeline.package_scout",
    "write_scout_artifacts": "core.pipeline.package_scout",
    # package_seeds
    "CORPUS_URLS_FORMAT": "core.pipeline.package_seeds",
    "SeedsExport": "core.pipeline.package_seeds",
    "SeedsOpResult": "core.pipeline.package_seeds",
    "product_seed_rows": "core.pipeline.package_seeds",
    "seeds_add": "core.pipeline.package_seeds",
    "seeds_disable": "core.pipeline.package_seeds",
    "seeds_enable": "core.pipeline.package_seeds",
    "seeds_export": "core.pipeline.package_seeds",
    "seeds_set_enabled": "core.pipeline.package_seeds",
    # passport_scout
    "DISCOVERY_TEMPLATE_VERSION": "core.pipeline.passport_scout",
    "DiscoveryPassportHypothesis": "core.pipeline.passport_scout",
    "PassportScoutError": "core.pipeline.passport_scout",
    "PassportScoutReport": "core.pipeline.passport_scout",
    "build_passport_draft": "core.pipeline.passport_scout",
    "scout_passport": "core.pipeline.passport_scout",
    "write_passport_draft": "core.pipeline.passport_scout",
    # phases
    "IMPLEMENTED_PHASES": "core.pipeline.phases",
    "build_phases": "core.pipeline.phases",
    # plan
    "RunPlan": "core.pipeline.plan",
    "derive_run_id": "core.pipeline.plan",
    "load_plan": "core.pipeline.plan",
    "plan": "core.pipeline.plan",
    # profile_attestation
    "PROFILE_MATERIAL_KEYS": "core.pipeline.profile_attestation",
    "ProfileGateResult": "core.pipeline.profile_attestation",
    "describe_material_diff": "core.pipeline.profile_attestation",
    "make_profile_gate": "core.pipeline.profile_attestation",
    "profile_attestation_id": "core.pipeline.profile_attestation",
    "profile_digest": "core.pipeline.profile_attestation",
    "profile_material": "core.pipeline.profile_attestation",
    # prompt
    "TEMPLATE_VERSION": "core.pipeline.prompt",
    "CompiledPrompt": "core.pipeline.prompt",
    "PromptArchive": "core.pipeline.prompt",
    "compile_prompt": "core.pipeline.prompt",
    "render_queries": "core.pipeline.prompt",
    "save_prompt": "core.pipeline.prompt",
    # registry
    "registry_covered_params": "core.pipeline.registry",
    "registry_wave": "core.pipeline.registry",
    "resolve_registry_ids": "core.pipeline.registry",
    # report
    "Artifacts": "core.pipeline.report",
    "CrossCycleDiffError": "core.pipeline.report",
    "PublishBlockedError": "core.pipeline.report",
    "build_manifest": "core.pipeline.report",
    "render_battle_card": "core.pipeline.report",
    "render_diff": "core.pipeline.report",
    "render_nda_questions": "core.pipeline.report",
    "render_preliminary_findings": "core.pipeline.report",
    "write_reports": "core.pipeline.report",
    "write_run_manifest": "core.pipeline.report",
    # retrieve
    "PROFILE_DESCRIPTIONS": "core.pipeline.retrieve",
    "RetrievalProfile": "core.pipeline.retrieve",
    "retrieve": "core.pipeline.retrieve",
    "rrf_fuse_chunks": "core.pipeline.retrieve",
    # review
    "PromotionResult": "core.pipeline.review",
    "ReviewError": "core.pipeline.review",
    "ReviewItem": "core.pipeline.review",
    "promote": "core.pipeline.review",
    "promote_batch": "core.pipeline.review",
    "review_queue": "core.pipeline.review",
    # runner
    "GATE_PHASES": "core.pipeline.runner",
    "PHASES": "core.pipeline.runner",
    "GateRuntime": "core.pipeline.runner",
    "PhaseResult": "core.pipeline.runner",
    "Ports": "core.pipeline.runner",
    "RunContext": "core.pipeline.runner",
    "Runner": "core.pipeline.runner",
    "RunReport": "core.pipeline.runner",
    "run_isolated": "core.pipeline.runner",
    # search_log
    "SearchBudget": "core.pipeline.search_log",
    "SearchEffort": "core.pipeline.search_log",
    "external_search": "core.pipeline.search_log",
    "record_search": "core.pipeline.search_log",
    "termination_for": "core.pipeline.search_log",
    # serp_loop
    "SerpLoopError": "core.pipeline.serp_loop",
    "SerpLoopResult": "core.pipeline.serp_loop",
    "SerpQuery": "core.pipeline.serp_loop",
    "SerpRound": "core.pipeline.serp_loop",
    "derive_round_build_id": "core.pipeline.serp_loop",
    "invalidate_affected": "core.pipeline.serp_loop",
    "run_serp_loop": "core.pipeline.serp_loop",
    "run_serp_round": "core.pipeline.serp_loop",
    # validate
    "ValidationFailureKind": "core.pipeline.validate",
    "ValidationResult": "core.pipeline.validate",
    "validate_output": "core.pipeline.validate",
}


def __getattr__(name: str) -> Any:
    """Резолв публичного имени фасада по требованию (PEP 562).

    Имя берётся из :data:`_LAZY` и импортируется из своего модуля; результат кладётся в
    ``globals()``, поэтому повторное обращение за импортом не платит.

    Имя, которого нет в карте, может быть **подмодулем**: ``from core.pipeline import
    package_admin`` — это импорт подмодуля, а не имени из ``__all__`` (так делают
    ``package_candidates``/``package_io``). Поэтому подмодуль пробуется до
    ``AttributeError``: иначе такой импорт получил бы ложный ``AttributeError``.

    Безопасно при частично инициализированном пакете: ``__getattr__`` не форсирует
    повторный импорт пакета, а если модуль уже в ``sys.modules`` и ещё не дорос до
    нужного имени, поднимается ``AttributeError`` с указанием модуля-источника.
    """
    module_name = _LAZY.get(name)
    if module_name is not None:
        try:
            value = getattr(importlib.import_module(module_name), name)
        except AttributeError as exc:
            raise AttributeError(
                f"module {__name__!r} has no attribute {name!r}: "
                f"он объявлен в {module_name!r}, но там отсутствует"
            ) from exc
        globals()[name] = value
        return value

    # Не имя фасада — возможно, подмодуль (``from core.pipeline import package_admin``).
    if not name.startswith("_"):
        try:
            module = importlib.import_module(f"{__name__}.{name}")
        except ModuleNotFoundError as exc:
            # Отличаем «нет такого подмодуля» от «подмодуль есть, но у него нет своей
            # зависимости»: второе — настоящая ошибка импорта, её нельзя глотать.
            if exc.name != f"{__name__}.{name}":
                raise
        else:
            globals()[name] = module
            return module

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Публичная поверхность пакета: объявленное в модуле + ``__all__``."""
    return sorted(set(globals()) | set(__all__))
