#!/usr/bin/env python3
"""coHound — self-contained проверка фундамента M0.

Запуск: python3 tests/selfcheck.py
Не требует pytest. Проверяет:
  - целостность docs/status-dictionary.yaml;
  - инварианты Pydantic-схем (§2.3, I1/I2/I2b/I4/I6/I7/I13, Р-2);
  - DDL SQLite и триггеры инвариантов (I4/I6/I7/I13/I18).
"""

from __future__ import annotations

import datetime as dt
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.store.sqlite import DDL, SCHEMA_VERSION, connect  # noqa: E402
from core.domain import (  # noqa: E402
    AbsenceRuleType,
    AnchorRelation,
    Archetype,
    Cell,
    CellEvidence,
    CellState,
    Cohort,
    CohortEntry,
    EvidenceState,
    EvidenceType,
    LLMCellOutput,
    LLMEvidence,
    ModuleDef,
    Origin,
    PackageManifest,
    Relationship,
    RunConfig,
    RunMode,
    SourceKind,
    SourceScope,
    Status,
    apply_r2,
    assert_dictionary_integrity,
    derived_evidence_id,
)

FAILURES: list[str] = []
CHECKS = 0


def check(name: str, fn) -> None:
    """Выполняет fn; ожидает успех."""
    global CHECKS
    CHECKS += 1
    try:
        fn()
        print(f"  ok  {name}")
    except Exception as exc:  # noqa: BLE001
        FAILURES.append(f"{name}: {exc}")
        print(f"FAIL  {name}: {exc}")


def expect_raises(name: str, exc_type, fn) -> None:
    """Выполняет fn; ожидает исключение exc_type."""
    global CHECKS
    CHECKS += 1
    try:
        fn()
    except exc_type:
        print(f"  ok  {name}")
        return
    except Exception as exc:  # noqa: BLE001
        FAILURES.append(f"{name}: ожидалось {exc_type.__name__}, получено {type(exc).__name__}: {exc}")
        print(f"FAIL  {name}: ожидалось {exc_type.__name__}, получено {type(exc).__name__}: {exc}")
        return
    FAILURES.append(f"{name}: ожидалось {exc_type.__name__}, но исключения не было")
    print(f"FAIL  {name}: исключения не было")


def section(title: str) -> None:
    print(f"\n== {title} ==")


def main() -> int:
    section("Словарь §4.1")
    check(
        "целостность status-dictionary.yaml",
        lambda: assert_dictionary_integrity(ROOT / "docs" / "status-dictionary.yaml"),
    )
    check(
        "crawl-сеанс: enum↔словарь (crawl_session_status/crawl_item_status)",
        lambda: _crawl_session_dict_mirror(),
    )

    section("Пакет категории §4.2 / когорта §1.6")
    check(
        "корректный package.yaml (веса=100)",
        lambda: PackageManifest(
            case_id="pg",
            archetype=Archetype.A,
            modules=[ModuleDef(module_id="core", weight=60), ModuleDef(module_id="ecosystem", weight=40)],
            roles=[],
        ),
    )
    expect_raises(
        "веса != 100 отклоняются",
        ValueError,
        lambda: PackageManifest(
            case_id="pg",
            archetype=Archetype.A,
            modules=[ModuleDef(module_id="core", weight=60)],
            roles=[],
        ),
    )
    expect_raises(
        "архетип C: baseline_participant обязан быть null",
        ValueError,
        lambda: PackageManifest(
            case_id="pg",
            archetype=Archetype.C,
            modules=[ModuleDef(module_id="core", weight=100)],
            roles=[],
            baseline_participant="some-baseline",
        ),
    )
    check(
        "когорта: ровно один якорь",
        lambda: Cohort(
            case_id="pg",
            frozen_at=dt.date(2026, 6, 1),
            entries=[
                CohortEntry(product_id="anchor", role_overlap=1.0, selection_reason="x", anchor_relation=AnchorRelation.ANCHOR),
                CohortEntry(product_id="alt", role_overlap=0.8, selection_reason="y", anchor_relation=AnchorRelation.ANALOG),
            ],
        ),
    )
    expect_raises(
        "когорта без якоря отклоняется",
        ValueError,
        lambda: Cohort(
            case_id="pg",
            frozen_at=dt.date(2026, 6, 1),
            entries=[CohortEntry(product_id="a", role_overlap=1.0, selection_reason="x", anchor_relation=AnchorRelation.ANALOG)],
        ),
    )

    section("Выход LLM-оценщика §7")
    check(
        "валидный ✅ с evidence",
        lambda: LLMCellOutput(status=Status.OK, justification="x", evidence=[LLMEvidence(source_id="c1", quote="q")]),
    )
    expect_raises(
        "✅ без evidence отклоняется (I1)",
        ValueError,
        lambda: LLMCellOutput(status=Status.OK, justification="x", evidence=[]),
    )
    expect_raises(
        "N/A от LLM запрещён (I7)",
        ValueError,
        lambda: LLMCellOutput(status=Status.NOT_APPLICABLE, justification="x"),
    )

    section("Инварианты ячейки §2.3")
    check(
        "evaluated ❌ с registry_absence и exhaustive (I2b)",
        lambda: Cell(
            run_id="r1",
            product_id="p",
            param_id="param",
            state=CellState.EVALUATED,
            status=Status.ABSENT,
            absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
            absence_evidence=[{"source_scope": "exhaustive"}],
        ),
    )
    expect_raises(
        "I4: failed со status отклоняется",
        ValueError,
        lambda: Cell(
            run_id="r1", product_id="p", param_id="param",
            state=CellState.FAILED, status=Status.OK,
        ),
    )
    expect_raises(
        "I13: preliminary со status отклоняется",
        ValueError,
        lambda: Cell(
            run_id="r1", product_id="p", param_id="param",
            state=CellState.PRELIMINARY, status=Status.OK,
        ),
    )
    expect_raises(
        "I7: N/A без reviewed_by отклоняется",
        ValueError,
        lambda: Cell(
            run_id="r1", product_id="p", param_id="param",
            state=CellState.REVIEWED, status=Status.NOT_APPLICABLE,
        ),
    )
    expect_raises(
        "I2b: parameter_reference не-exhaustive отклоняется",
        ValueError,
        lambda: Cell(
            run_id="r1", product_id="p", param_id="param",
            state=CellState.EVALUATED, status=Status.ABSENT,
            absence_rule=AbsenceRuleType.PARAMETER_REFERENCE,
            absence_evidence=[{"source_scope": "partial"}],
        ),
    )
    check(
        "I2b: parameter_reference с exhaustive+attested проходит",
        lambda: Cell(
            run_id="r1", product_id="p", param_id="param",
            state=CellState.EVALUATED, status=Status.ABSENT,
            absence_rule=AbsenceRuleType.PARAMETER_REFERENCE,
            absence_evidence=[{"source_scope": "exhaustive", "scope_attested_by": "analyst"}],
        ),
    )

    section("Р-2: маппинг сателлита на зачёт §4.1")
    check("claimed → ⚠️", lambda: _assert(apply_r2(Origin.THIRD_PARTY, Relationship.BUNDLED, EvidenceState.CLAIMED).value == "partial"))
    check("community+external → none", lambda: _assert(apply_r2(Origin.COMMUNITY, Relationship.EXTERNAL, EvidenceState.VERIFIED).value == "none"))
    check("bundled+verified → ok", lambda: _assert(apply_r2(Origin.THIRD_PARTY, Relationship.BUNDLED, EvidenceState.VERIFIED).value == "ok"))
    check("recommended+verified → partial", lambda: _assert(apply_r2(Origin.THIRD_PARTY, Relationship.RECOMMENDED, EvidenceState.VERIFIED).value == "partial"))

    section("run.yaml §4.5")
    check(
        "strict с capability-ранжированием",
        lambda: RunConfig(
            case_id="pg", anchor="p0", cohort="c", products=["p0", "p1"],
            package="pg", snapshot_date=dt.date(2026, 6, 1), mode=RunMode.STRICT,
        ),
    )
    expect_raises(
        "preliminary + capability_ranked отклоняется (I13)",
        ValueError,
        lambda: RunConfig(
            case_id="pg", anchor="p0", cohort="c", products=["p0"],
            package="pg", snapshot_date=dt.date(2026, 6, 1), mode=RunMode.PRELIMINARY,
        ),
    )

    section("DDL SQLite §4.4 + триггеры инвариантов")
    check("создание схемы в памяти", lambda: _db_smoke())

    section("CellEvidence / derived-evidence (K5, S5, S8, I19)")
    check(
        "корпусный evidence с hash валиден",
        lambda: CellEvidence(
            evidence_id="ev1", run_id="r", product_id="p", param_id="x",
            source_kind=SourceKind.DOCUMENTATION, evidence_type=EvidenceType.DIRECT,
            source_id="chunk_1", content_hash="abc",
        ),
    )
    expect_raises(
        "I19: корпусный evidence без content_hash отклоняется",
        ValueError,
        lambda: CellEvidence(
            evidence_id="ev2", run_id="r", product_id="p", param_id="x",
            source_kind=SourceKind.DOCUMENTATION, evidence_type=EvidenceType.DIRECT,
            source_id="chunk_1",
        ),
    )
    check(
        "derived-evidence (наследование) валиден с source_id",
        lambda: CellEvidence(
            evidence_id="ev3", run_id="r", product_id="p", param_id="x",
            source_kind=SourceKind.INHERITANCE, evidence_type=EvidenceType.DERIVED,
            source_id="upstream://baseline#param",
        ),
    )
    expect_raises(
        "R1: registry evidence без source_url отклоняется",
        ValueError,
        lambda: CellEvidence(
            evidence_id="ev4", run_id="r", product_id="p", param_id="x",
            source_kind=SourceKind.REGISTRY, evidence_type=EvidenceType.DIRECT,
            source_id="rrp:1",
        ),
    )
    check(
        "S8: derived_evidence_id детерминирован",
        lambda: _assert(
            derived_evidence_id("r", "p", "x", "s", "registry", 0)
            == derived_evidence_id("r", "p", "x", "s", "registry", 0)
        ),
    )

    section("New enums (S3) + провенанс ячейки (R7/§13)")
    check(
        "ячейка с провенансом сборки валидна",
        lambda: Cell(
            run_id="r", product_id="p", param_id="x",
            state=CellState.PENDING,
            index_build_id="b1", retrieval_profile="R2",
        ),
    )

    section("Скопированные утилиты/адаптеры (gag5, MIT) — §3.2/§5")
    check("text_utils: детект RU, ё→е, разбиение предложений", lambda: _util_text())
    check("security: SSRF-блок и санитизация", lambda: _util_security())
    check("fusion: RRF дедуп по chunk_id + детерминизм", lambda: _util_fusion())
    check("MarkdownChunker: heading_path иерархичен (I17/§3.2)", lambda: _chunker())
    check("DoclingDocumentExtractor: pdf/office + грациозная деградация", lambda: _pdf_extractor())

    section("Скопированные утилиты/адаптеры (gtm, авторские) — §11.2/§11.3")
    check("text_utils: RU-нормализация токенов", lambda: _util_ru_tokenize())
    check("BM25: продукт-скоупед и детерминизм (I17/R7)", lambda: _bm25())
    check("retrieval_eval: hit@K/recall@K/MRR (I20)", lambda: _retrieval_eval())
    check(
        "agreement: Cohen's κ ок/отдельно по N/A, undefined вместо выдумки (§11.5)",
        lambda: _agreement_kappa(),
    )

    section("Порты (L2) — контракты (01 §3, W1/T1.1)")
    check("порты импортируются, все — Protocol", lambda: _ports_contracts())
    check("I18 структурно: у оценщика нет tools/web_search", lambda: _ports_i18())
    check("адаптеры используют порт-типы (Chunk/ExtractedDoc)", lambda: _ports_type_reuse())

    section("Инженерные гейты (W1/T1.4)")
    check("матрица зависимостей core.* (01 §1, ast)", lambda: _import_graph())
    check("§1.3 нейтральность domain/ports/pipeline (грепп)", lambda: _neutrality())
    check("transitional-шимы schemas.py/db.py удалены", lambda: _shims_removed())
    check("core.domain.__all__ полон (публичные символы домена)", lambda: _domain_all_complete())

    section("Домен (L0) — W2")
    check("инварианты домена возвращают Violation (I1/I4/I13/I7)", lambda: _domain_invariants())
    check("Ф0: ❌ при неполном корпусе требует аттестации (I2c)", lambda: _corpus_coverage_invariant())
    check("derived-evidence требует hash/резолв (I1/I10/I15/I19, K5)", lambda: _domain_derived())
    check("source_id round-trip + резолв с hash (I10/I15/I19)", lambda: _domain_anchors())
    check("content_hash/prompt_hash детерминированы (§5/I19)", lambda: _util_hashing())
    check("scoring: failed в знаменателе Coverage, вне Capability (I5)", lambda: _domain_scoring())

    section("Store (L3) — W3")
    check("SqliteStore I19: резолв + сверка content_hash", lambda: _store_resolver_i19())

    section("Конвейер (L1) — W4")
    check("гейт не пройден без gate_decisions; durable resume (S9/P4)", lambda: _pipeline_gates_resume())

    section("CLI / composition root (Z0) — W5")
    check("load_inputs→lint→plan; битый пакет блокируется (I2b)", lambda: _cli_plan_lint())
    check("профили local/split: split честно недоступен (07/W13)", lambda: _cli_wiring_profiles())

    section("Корпус (Ш5) — W7")
    check("content-addressed кэш: get_or_fetch + ключи (R7)", lambda: _util_cache())
    check("HtmlExtractor: hash/деградация (I19)", lambda: _extract_html())
    check("sanitize_html: оболочка документа + активный HTML удалён (T7.5)", lambda: _util_sanitize())
    check("ScrapyCrawler: кэш + SSRF + домен-профиль", lambda: _crawl_scrapy())
    check("MarkdownChunkerPort удовлетворяет ChunkerPort (T7.3)", lambda: _chunker_port())

    section("Ретривал/индекс — W8")
    check("R0/R1/R2: профиль явный, продукт-скоуп (I16/I17)", lambda: _retrieve_profiles())
    check("RRF-фьюжн: дедуп + провенанс + tie-break (R7)", lambda: _rrf_fusion())
    check("LocalEmbedder: батч + torque-free шов (T8.1)", lambda: _embed_local())
    check("LiteEmbedder: оффлайн-шов + guard пустого входа (P4a)", lambda: _embed_model2vec())
    check("NpyIndexBuilder/VectorIndex: партиции + cosine (T8.2/T8.4)", lambda: _index_roundtrip())
    check("retrieval read-loop: сборка → vector_index, product-scoped (P4a/I17)", lambda: _retrieval_read_loop())

    section("Расчёт (Ш8) — W10")
    check("compute: I5 (failed в Coverage, вне Capability) + K6", lambda: _compute_i5())

    section("Оценка (Ш6) — W9")
    check("validate: цитата-подстрока + CellEvidence hash (I10/I19)", lambda: _validate_echelon())
    check("evaluate: strict failed≠status (I4), preliminary (I13)", lambda: _evaluate_failed())

    section("Отчётность (Ш8-9) — W10")
    check("report: I11 Coverage отсутствует → G4-блок; §13 diff-граница", lambda: _report_gates())

    section("Наблюдаемость и эксплуатация — W12")
    check("logging: поля §B.1 + invariant отдельным полем", lambda: _logging_fields())
    check("metrics: cells_by_state/failure_rate/search_calls", lambda: _run_metrics())
    check("backup: консистентный БД-снапшот + GC dry-run (§13)", lambda: _backup_gc())

    section("Экстракция (Ш5) — W8")
    check("extraction-golden: expected/forbidden + §13 триггер", lambda: _extraction_golden())

    section("SERP/бюджет/поиск — W11")
    check("search_log: бюджет симметричен на продукт (I16/I23)", lambda: _search_budget())
    check("discovery: кандидаты без evidence (I19), промоушен только G2", lambda: _discovery())
    check("serp_loop: append-only сборки + K2-инвалидация (§13)", lambda: _serp_loop())

    section("Adversarial (R8) — W12")
    check("adversarial: 0 известных инъекций + регрессионный gate", lambda: _adversarial_gate())

    section("DMZ-шов (07) — W13")
    check("seam: schema_version=1 + конверт/payload + R1", lambda: _seam_schema())
    check("edge server: зеркало клиента + изоляция от store/domain", lambda: _edge_isolation())
    check("gateway_validate: повторный content_hash → отказ (I19)", lambda: _gateway_validate())
    check("auth/audit: подделка отвергнута + поля SIEM (07 §8)", lambda: _edge_auth_audit())
    check("deploy.yaml: отсутствие → local; split требует base_url", lambda: _deploy_config())

    section("Источники краула feed/channel/sitemap (Wave 5.4/D3)")
    check("crawl_sources: слои defaults→products→sites + digest", lambda: _crawl_sources_routing())
    check("pollers/seam: PollPort + crawl/poll (scope crawl, schema=1)", lambda: _pollers_seam())
    check("directory kinds + общие хелперы времени (Ш5)", lambda: _directory_kinds_helpers())

    section("Интеграция Ш1–Ш9 (composition root)")
    check("build_phases: 11 не-гейтовых фаз, гейты не переопределены", lambda: _build_phases_gate())
    check("walking-skeleton: реальные фазы + гейт g1 блокирует без решения", lambda: _walking_skeleton())
    check("phase-vocabulary: skipped не стопит; незарегистрированная фаза → failed", lambda: _phase_vocabulary())

    section("Паспорта продукта (§4.3)")
    check("passports: latest-by-date + кандидаты URL (не evidence, I19)", lambda: _passports())

    section("Golden-harness (R6/R12) — W12")
    check("golden: R6-согласие, false-✅ гейт, страта ❌ (R12)", lambda: _golden_harness())

    section("Ревью preliminary (I6/I7) — W9")
    check("review: очередь + human-only promote без фабрикации статуса", lambda: _review_human_only())

    section("CLI-поверхность (03 §D.6) — T10.4")
    check("commands: полный набор resolve/plan/run/discover/review/promote/…", lambda: _cli_surface())
    check("run-dir: входы плана персистятся в БД (run_inputs) и восстанавливаются (03 §A.2/P1)", lambda: _cli_run_self_contained())

    section("Лексический индекс (W8) — двойной профиль")
    check("lexical: профили stemmed/identifier/raw/ngram + порт", lambda: _lexical_profiles())

    section("Шаблоны данных (T6.5) — пакет/паспорт/реестр")
    check(
        "templates: пакет линтуется без блоков и warnings; паспорт/реестр валидны",
        lambda: _data_templates(),
    )
    check(
        "registries: package-scoped спецификации + headers_env без секретов",
        lambda: _registry_package_scoped(),
    )

    section("Зонд осей краула (crawl-probe)")
    check("crawl_probe: предикат/лестница/шапка/зеркало словаря", lambda: _crawl_probe())

    section("Оси контроля краула (D1/D2)")
    check("crawl_control: precedence/scope/блэклист/усечение", lambda: _crawl_control_axes())

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} проверок пройдено")
    if FAILURES:
        print("\nПровалы:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ")
    return 0


def _assert(cond: bool) -> None:
    if not cond:
        raise AssertionError


def _util_text() -> None:
    from core.util import text_utils as t

    _assert(t.detect_language("Это предложение на русском языке") is t.TextLanguage.RUSSIAN)
    _assert(t.detect_language("This is an english sentence") is t.TextLanguage.ENGLISH)
    _assert(t.normalize_cyrillic("ёж") == "еж")
    _assert(t.split_sentences("Привет. Как дела? Хорошо!") == ["Привет.", "Как дела?", "Хорошо!"])


def _util_security() -> None:
    from core.util import security as s

    _assert(s.validate_url("https://example.com/docs") == "https://example.com/docs")
    for bad in ("http://127.0.0.1/x", "http://169.254.169.254/latest", "ftp://x"):
        try:
            s.validate_url(bad)
            raise AssertionError(f"не отклонён опасный URL: {bad}")
        except ValueError:
            pass
    try:
        s.validate_url("https://evil.com", allowed_domain="example.com")
        raise AssertionError("домен вне allowlist не отклонён")
    except ValueError:
        pass
    _assert(s.sanitize_filename("../../etc/passwd") == "etcpasswd")
    _assert(s.sanitize_slug("Platform V Pangolin!") == "Platform_V_Pangolin")


def _util_fusion() -> None:
    from core.util import fusion as f

    bm25 = [{"chunk_id": "c1", "content": "a", "score": 0.9},
            {"chunk_id": "c2", "content": "b", "score": 0.5}]
    dense = [{"chunk_id": "c2", "content": "b", "score": 0.8},
             {"chunk_id": "c3", "content": "c", "score": 0.7}]
    res = f.rrf_fuse([bm25, dense], rrf_k=60)
    # c2 присутствует в обоих списках → первый; дедуп по chunk_id даёт 3 записи.
    _assert(res[0]["content"] == "b")
    _assert(len(res) == 3)
    res2 = f.rrf_fuse([bm25, dense], rrf_k=60)
    _assert([r["content"] for r in res] == [r["content"] for r in res2])  # детерминизм


def _chunker() -> None:
    from core.adapters.chunker_markdown import MarkdownChunker

    md = "# H1\nтело\n\n## H2\nтело2\n\n### H3\nтело3\n"
    chunks = MarkdownChunker(chunk_size=100, chunk_overlap=20).chunk("doc1", md)
    _assert(chunks[0].heading_path == "H1")
    _assert(chunks[1].heading_path == "H1 > H2")
    _assert(chunks[2].heading_path == "H1 > H2 > H3")
    _assert([c.chunk_id for c in chunks] == ["doc1#0", "doc1#1", "doc1#2"])
    _assert(all(c.context_prefix == c.heading_path for c in chunks))
    _assert(all(len(c.content_hash) == 16 for c in chunks))


def _pdf_extractor() -> None:
    from core.adapters.extract_document import DoclingDocumentExtractor

    ex = DoclingDocumentExtractor()
    # При недоступном docling адаптер деградирует, а не падает.
    res = ex.extract_bytes(b"%PDF-1.4 not really a pdf", "manual.pdf")
    _assert(res.text == "")
    _assert(res.error is not None)
    _assert(res.extractor_version == DoclingDocumentExtractor.version)
    # Реестр форматов: pdf + office (§3.2).
    _assert(DoclingDocumentExtractor.supports("a.pdf"))
    _assert(DoclingDocumentExtractor.supports("a.docx"))
    _assert(DoclingDocumentExtractor.supports("a.pptx"))
    _assert(DoclingDocumentExtractor.supports("a.xlsx"))
    _assert(not DoclingDocumentExtractor.supports("a.txt"))
    # Неподдерживаемый формат → ошибка без исключения.
    bad = ex.extract_bytes(b"data", "file.bin")
    _assert(bad.text == "" and bad.error is not None)


def _util_ru_tokenize() -> None:
    from core.util import text_utils as t

    # Грубый стеммер сближает флективные формы.
    _assert(t.normalize_ru_token("перехвата") == t.normalize_ru_token("перехват"))
    tokens = t.tokenize_lexical("Инспекция трафика к LLM через CONNECT-прокси")
    _assert("LLM".lower() in tokens or "connect" in tokens)
    _assert(all(tok == tok.lower() for tok in tokens))
    _assert(not any(tok in {"и", "в", "на", "the", "of"} for tok in tokens))


def _bm25() -> None:
    from core.adapters.lexical_bm25 import Bm25Index, LexicalDoc

    docs = [
        LexicalDoc("p1#0", "p1", "резервное копирование и восстановление базы"),
        LexicalDoc("p1#1", "p1", "разграничение прав доступа пользователей"),
        LexicalDoc("p2#0", "p2", "резервное копирование по расписанию"),
    ]
    idx = Bm25Index()
    idx.index(docs)
    # Продукт-скоупед: запрос по p1 не возвращает p2 (I17).
    hits_p1 = idx.search("резервное копирование", product_ids=["p1"], top_k=5)
    _assert(all(h.product_id == "p1" for h in hits_p1))
    _assert(bool(hits_p1) and hits_p1[0].chunk_id == "p1#0")
    # Детерминизм порядка (R7).
    again = idx.search("резервное копирование", product_ids=["p1"], top_k=5)
    _assert([h.chunk_id for h in hits_p1] == [h.chunk_id for h in again])


def _retrieval_eval() -> None:
    from core.util.retrieval_eval import GoldenQuery, evaluate_retrieval

    queries = [
        GoldenQuery("резервное копирование", frozenset({"c1"})),
        GoldenQuery("права доступа", frozenset({"c2"})),
        GoldenQuery("absent concept", frozenset({"c9"})),
    ]
    ranking = {"резервное копирование": ["c0", "c1"], "права доступа": ["c2"],
               "absent concept": ["c3"]}
    report = evaluate_retrieval(queries, lambda q, k: ranking.get(q, []), k=5)
    _assert(report.k == 5)
    _assert(abs(report.hit_at_k - 2 / 3) < 1e-9)     # c1 ранк2, c2 ранк1, c9 нет
    _assert(abs(report.recall_at_k - 2 / 3) < 1e-9)
    _assert(report.mrr > 0.0)
    # I20: cut-off K усекает ранжирование.
    short = evaluate_retrieval(queries, lambda q, k: ranking.get(q, []), k=1)
    _assert(abs(short.hit_at_k - 1 / 3) < 1e-9)      # только c2 на ранке 1


def _agreement_kappa() -> None:
    """§11.5: κ ок/отдельно по N/A, непарные ячейки, ``undefined`` вместо выдумки.

    Инварианты согласия (чистая арифметика :mod:`core.util.agreement`): полное согласие → κ=1;
    вырожденный маргинал → ``kappa is None`` (не 0/1); ячейка, размеченная одним аннотатором,
    не считается согласием и исключается; N/A-κ считается по бинарному решению и **может**
    расходиться с общим κ (ради чего §11.5 и требует публиковать его отдельно).
    """
    from core.domain.enums import Status
    from core.util.agreement import compute_agreement, parse_annotations_csv

    ok, absent, na = Status.OK.value, Status.ABSENT.value, Status.NOT_APPLICABLE.value
    known = [s.value for s in Status]

    rows = parse_annotations_csv(
        "product_id,param_id,annotator_a,annotator_b\n"
        f"p1,a,{ok},{ok}\n"
        f"p1,b,{absent},{absent}\n"
        f"p1,c,{na},\n"  # непарная: размечена одним аннотатором
    )
    report = compute_agreement(rows, na_label=na, known_labels=known)
    _assert(report.paired_items == 2 and report.total_items == 3)
    _assert(report.unpaired_items == ("p1/c",))
    _assert(report.overall.kappa == 1.0 and report.overall.n == 2)

    # Полное согласие по одной метке → ожидаемое согласие = 1 → κ не определён (не 0/1).
    flat = compute_agreement(
        parse_annotations_csv(f"product_id,param_id,annotator_a,annotator_b\np1,a,{ok},{ok}\n"),
        na_label=na,
        known_labels=known,
    )
    _assert(flat.overall.kappa is None and flat.overall.undefined_reason is not None)

    # N/A-κ расходится с общим: у A и B по 3 N/A-решения, все — противоположные ячейки.
    divergent = [(ok, ok)] * 10 + [(absent, absent)] * 4 + [(na, absent)] * 3 + [(absent, na)] * 3
    text = "product_id,param_id,annotator_a,annotator_b\n" + "".join(
        f"p1,x{i},{a},{b}\n" for i, (a, b) in enumerate(divergent)
    )
    div = compute_agreement(parse_annotations_csv(text), na_label=na, known_labels=known)
    _assert(div.overall.kappa is not None and div.na_binary is not None)
    _assert(div.na_binary.kappa is not None and div.na_binary.kappa < div.overall.kappa)
    _assert(div.na_count_a == 3 and div.na_count_b == 3)

    # Метка вне словаря не выкидывается молча — попадает в отчёт.
    unknown = compute_agreement(
        parse_annotations_csv(f"product_id,param_id,annotator_a,annotator_b\np1,a,OK,{ok}\n"),
        na_label=na,
        known_labels=known,
    )
    _assert(unknown.unknown_labels == {"OK": 1})


def _ports_contracts() -> None:
    """Все порты — импортируемые Protocol-контракты (01 §3, W1/T1.1)."""
    import core.ports as P

    names = [
        "EvaluatorPort", "VectorIndexPort", "LexicalIndexPort", "SearchAdapterPort",
        "CrawlerPort", "ExtractorPort", "ChunkerPort", "RegistryFetcherPort",
        "StorePort", "GateStore", "EmbedderPort", "IndexBuilderPort", "ContentGateway",
    ]
    for n in names:
        obj = getattr(P, n)
        if not getattr(obj, "_is_protocol", False):
            raise AssertionError(f"{n} не является Protocol")


def _ports_i18() -> None:
    """I18 структурно: у порта оценщика физически нет параметров tools/web_search."""
    import inspect

    from core.ports.llm import EvaluatorPort

    params = set(inspect.signature(EvaluatorPort.complete).parameters)
    banned = {"tools", "web_search", "websearch", "search"}
    if params & banned:
        raise AssertionError(f"I18: EvaluatorPort.complete не должен принимать {params & banned}")


def _ports_type_reuse() -> None:
    """Адаптеры переиспользуют порт-типы (единый тип Chunk/ExtractedDoc, не дубль)."""
    from core.adapters.chunker_markdown import Chunk as AdapterChunk
    from core.adapters.extract_document import ExtractedDoc as AdapterDoc
    from core.ports.types import Chunk as PortChunk, ExtractedDoc as PortDoc

    if AdapterChunk is not PortChunk:
        raise AssertionError("chunker Chunk должен быть ports.types.Chunk")
    if AdapterDoc is not PortDoc:
        raise AssertionError("extractor ExtractedDoc должен быть ports.types.ExtractedDoc")


def _crawl_session_dict_mirror() -> None:
    """Lane B: словарь несёт оба новых перечисления, синхронно с доменными ``StrEnum``."""
    from core.domain import CrawlItemStatus, CrawlSessionStatus, load_dictionary

    data = load_dictionary(ROOT / "docs" / "status-dictionary.yaml")
    if set(data["crawl_session_status"]) != {s.value for s in CrawlSessionStatus}:
        raise AssertionError("crawl_session_status рассинхронизирован со схемой")
    if set(data["crawl_item_status"]) != {s.value for s in CrawlItemStatus}:
        raise AssertionError("crawl_item_status рассинхронизирован со схемой")


def _db_smoke() -> None:
    conn = connect(":memory:")
    ver = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    _assert(ver is not None and ver["value"] == "3.9.0")

    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ("products", "runs", "llm_scenarios", "corpus_docs", "corpus_chunks",
              "index_manifest", "cells", "cell_evidence", "search_log",
              "search_exhaustion", "registry_facts", "run_inputs",
              "crawl_sessions", "crawl_session_items", "corpus_coverage"):
        _assert(t in tables)

    # P2 (schema 2.2.0): build-id witness — extractor_version у чанка; extractor_version/
    # content_digest у продукт-партиции манифеста.
    chunk_cols = {r["name"] for r in conn.execute("PRAGMA table_info(corpus_chunks)")}
    _assert("extractor_version" in chunk_cols)
    manifest_cols = {r["name"] for r in conn.execute("PRAGMA table_info(index_manifest)")}
    _assert("extractor_version" in manifest_cols and "content_digest" in manifest_cols)

    # L1b (schema 3.1.0): legacy crawl_attempts удалена (замещена сеансом краула).
    _assert("crawl_attempts" not in tables)

    # Phase 2 (schema 3.2.0): провенанс краула в манифесте прогона.
    runs_cols = {r["name"] for r in conn.execute("PRAGMA table_info(runs)")}
    _assert("crawl_provenance_json" in runs_cols)

    # Backlog B (schema 3.3.0): операционные оси краула — границы ``execution_change`` (§13).
    _assert("crawl_strategy" in runs_cols and "crawl_rate_limit" in runs_cols)

    # P1 (schema 3.4.0): деплой-провенанс манифеста (07 §7) и порог ранжирования (§6/B-1)
    # — реальные колонки; ``run_inputs`` — источник истины самодостаточности прогона.
    _assert({"deploy_profile", "edge_schema_version", "edge_capabilities_json",
             "ranking_epsilon"} <= runs_cols)
    # E2 (schema 3.5.0): дайджест package-scoped спецификаций реестров — граница
    # ``execution_change`` (§13); персистится, иначе ``hound diff`` её не видит (D1).
    _assert("registry_spec_digest" in runs_cols)
    inputs_cols = {r["name"] for r in conn.execute("PRAGMA table_info(run_inputs)")}
    _assert({"run_id", "snapshot_version", "payload_json", "payload_sha256",
             "created_at"} <= inputs_cols)
    inputs_pk = [r["name"] for r in conn.execute("PRAGMA table_info(run_inputs)")
                 if r["pk"]]
    _assert(inputs_pk == ["run_id"])

    # Lane B (schema 3.0.0): сеанс краула — crawl_sessions + элементы с PK и FK.
    sess_cols = {r["name"] for r in conn.execute("PRAGMA table_info(crawl_sessions)")}
    _assert({"session_id", "run_id", "profile", "status", "created_at", "started_at",
             "finished_at", "total", "pending", "running", "fetched", "failed",
             "cancelled", "options_json", "provenance_json"} <= sess_cols)
    sess_pk = [r["name"] for r in conn.execute("PRAGMA table_info(crawl_sessions)")
               if r["pk"]]
    _assert(sess_pk == ["session_id"])
    item_cols = {r["name"] for r in conn.execute("PRAGMA table_info(crawl_session_items)")}
    _assert({"session_id", "product_id", "url", "status", "attempts", "depth", "engine",
             "kind", "fetch_mode", "extract_route", "strategy", "http_status", "content_type",
             "lang", "content_hash", "raw_path", "error_kind", "error", "first_seen",
             "last_attempt"} <= item_cols)
    item_pk = [r["name"] for r in conn.execute("PRAGMA table_info(crawl_session_items)")
               if r["pk"]]
    _assert(item_pk == ["session_id", "product_id", "url"])
    # Ф0 (schema 3.8.0): покрытие корпуса продукта — PK (run_id, product_id);
    # v3.8.0 добавила origin-уровневые колонки (нечитаемые origin'ы/частичные блокировки).
    cov_cols = {r["name"] for r in conn.execute("PRAGMA table_info(corpus_coverage)")}
    _assert({"run_id", "product_id", "complete", "expected_sources", "observed_sources",
             "blocked_json", "unreadable_origins_json", "partial_blocks_json",
             "truncated", "computed_at", "reason"} <= cov_cols)
    cov_pk = [r["name"] for r in conn.execute("PRAGMA table_info(corpus_coverage)")
              if r["pk"]]
    _assert(cov_pk == ["run_id", "product_id"])
    # Прагмы соединения (Lane B): busy_timeout=5000, synchronous=NORMAL(=1) применены.
    _assert(conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000)
    _assert(conn.execute("PRAGMA synchronous").fetchone()[0] == 1)

    conn.execute(
        "INSERT INTO runs(run_id, created_at, mode, case_id, anchor_product_id) "
        "VALUES ('r1','now','strict','pg','p0')"
    )
    # I18: evaluator с web_tool=1 должен упасть.
    try:
        conn.execute(
            "INSERT INTO llm_scenarios(run_id, scenario, model_id, endpoint_kind, web_tool) "
            "VALUES ('r1','evaluator','m','self_host',1)"
        )
        raise AssertionError("I18-триггер не сработал")
    except Exception as exc:  # noqa: BLE001
        if not isinstance(exc, Exception) or "I18" not in str(exc):
            raise
    # I4: failed со status должен упасть.
    try:
        conn.execute(
            "INSERT INTO cells(run_id, product_id, param_id, state, status) "
            "VALUES ('r1','p0','x','failed','✅')"
        )
        raise AssertionError("I4-триггер не сработал")
    except Exception as exc:  # noqa: BLE001
        if "I4" not in str(exc):
            raise
    conn.close()

    # DDL-дельта v2.1 (A3): старая БД без settings_provenance_json мигрирует идемпотентно.
    import os as _os
    import sqlite3 as _sqlite3
    import tempfile as _tf

    fd, legacy_path = _tf.mkstemp(suffix=".db")
    _os.close(fd)
    legacy = _sqlite3.connect(legacy_path)
    legacy.execute(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, created_at TEXT NOT NULL,"
        " mode TEXT, case_id TEXT, anchor_product_id TEXT)"
    )
    legacy.execute(
        "INSERT INTO runs(run_id, created_at, mode, case_id, anchor_product_id)"
        " VALUES ('r1','now','strict','c','p')"
    )
    legacy.commit()
    legacy.close()
    migrated = connect(legacy_path)
    cols = {r["name"] for r in migrated.execute("PRAGMA table_info(runs)")}
    _assert("settings_provenance_json" in cols)
    migrated.close()
    _os.unlink(legacy_path)

    # DDL-миграция v2.2 (P2): старая БД без extractor_version/content_digest мигрирует
    # идемпотентно (повторный connect не падает и не дублирует колонки).
    fd2, legacy2_path = _tf.mkstemp(suffix=".db")
    _os.close(fd2)
    legacy2 = _sqlite3.connect(legacy2_path)
    legacy2.execute(
        "CREATE TABLE corpus_chunks (chunk_id TEXT PRIMARY KEY, doc_id TEXT,"
        " product_id TEXT, text TEXT, build_id TEXT)"
    )
    legacy2.execute(
        "CREATE TABLE index_manifest (index_build_id TEXT, product_id TEXT,"
        " chunker_version TEXT, PRIMARY KEY (index_build_id, product_id))"
    )
    legacy2.commit()
    legacy2.close()

    migrated2 = connect(legacy2_path)
    m_chunk_cols = {r["name"] for r in migrated2.execute("PRAGMA table_info(corpus_chunks)")}
    m_manifest_cols = {
        r["name"] for r in migrated2.execute("PRAGMA table_info(index_manifest)")
    }
    _assert("extractor_version" in m_chunk_cols)
    _assert("extractor_version" in m_manifest_cols and "content_digest" in m_manifest_cols)
    migrated2.close()

    # Идемпотентность: повторный connect на уже мигрированной БД — no-op.
    migrated2b = connect(legacy2_path)
    m2_chunk_cols = {r["name"] for r in migrated2b.execute("PRAGMA table_info(corpus_chunks)")}
    _assert(m2_chunk_cols == m_chunk_cols)
    migrated2b.close()
    _os.unlink(legacy2_path)


def _import_graph() -> None:
    """Матрица зависимостей слоёв core.* (01 §1) — статически через ast, без исполнения."""
    import ast

    matrix = {
        "domain": {"domain", "util"},
        "ports": {"domain", "ports", "util"},
        "pipeline": {"domain", "ports", "pipeline", "config", "util"},
        "adapters": {"domain", "ports", "adapters", "store", "config", "util"},
        "store": {"domain", "ports", "store", "config", "util"},
        "config": {"domain", "config", "util"},
        "cli": {"domain", "ports", "pipeline", "adapters", "store", "config", "cli", "util"},
        "edge": {"domain", "ports", "adapters", "config", "edge", "util"},
        "util": {"domain", "util"},
    }
    root = Path(__file__).resolve().parent.parent / "core"
    violations: list[str] = []
    for layer in matrix:
        layer_dir = root / layer
        if not layer_dir.is_dir():
            continue
        for py in layer_dir.rglob("*.py"):
            tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
            for node in ast.walk(tree):
                mod = None
                if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("core."):
                    mod = node.module
                elif isinstance(node, ast.ImportFrom) and node.level and node.level > 0:
                    # Relative-импорт (``from ..domain import X``): резолвим в ``core.*``;
                    # без этого гейт молча пропускал бы межслойные нарушения (§1.3/01 §1).
                    mod = _resolve_relative_import(py, root, node.level, node.module)
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("core."):
                            _check_edge(layer, alias.name.split(".")[1], matrix, py, root, violations)
                if mod is None:
                    continue
                parts = mod.split(".")
                if len(parts) < 2 or parts[0] != "core":
                    continue
                _check_edge(layer, parts[1], matrix, py, root, violations)
    if violations:
        raise AssertionError("нарушена матрица зависимостей (01 §1): " + "; ".join(violations))

def _resolve_relative_import(py: Path, root: Path, level: int, module: str | None) -> str | None:
    """Резолвит relative-импорт (``level>0``) в абсолютное имя ``core.*`` или ``None``.

    ``level=1`` — текущий пакет файла, ``level=2`` — родительский и т.д. Возвращает
    ``None``, если импорт выходит за пределы пакета ``core``.
    """
    pkg_parts = list(py.relative_to(root).parts[:-1])
    up = level - 1
    if up > len(pkg_parts):
        return None
    base = pkg_parts[: len(pkg_parts) - up]
    parts = ["core", *base]
    if module:
        parts.append(module)
    return ".".join(parts)


def _check_edge(layer: str, target: str, matrix: dict, py: Path, root: Path, violations: list) -> None:
    if target not in matrix:
        return
    if target not in matrix[layer]:
        violations.append(f"{py.relative_to(root)}: {layer} -> {target}")


def _neutrality() -> None:
    """§1.3 грепп-тест: domain/ports/pipeline без предметных терминов."""
    import re

    bad = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    root = Path(__file__).resolve().parent.parent / "core"
    hits: list[str] = []
    for layer in ("domain", "ports", "pipeline"):
        layer_dir = root / layer
        if not layer_dir.is_dir():
            continue
        for py in layer_dir.rglob("*.py"):
            for i, line in enumerate(py.read_text(encoding="utf-8").splitlines(), 1):
                if bad.search(line):
                    hits.append(f"{py.relative_to(root)}:{i}")
    if hits:
        raise AssertionError("§1.3: предметные термины в нейтральных слоях: " + ", ".join(hits))


def _shims_removed() -> None:
    """Transitional-шимы core/schemas.py и core/db.py удалены (T1.2/T1.3 завершены)."""
    import importlib.util

    if importlib.util.find_spec("core.schemas") is not None:
        raise AssertionError("core.schemas не должен существовать (T1.2 завершён)")
    if importlib.util.find_spec("core.db") is not None:
        raise AssertionError("core.db не должен существовать (T1.3 завершён)")


def _domain_all_complete() -> None:
    """``core.domain.__all__`` полон: каждый публичный def/class модулей домена в нём (§1.3).

    Защита от будущего дрейфа: новый публичный доменный символ, не добавленный в ``__all__``,
    валит гейт (иначе он молча выпадает из единого пространства доменных типов).
    """
    import ast

    import core.domain as domain_pkg

    root = Path(__file__).resolve().parent.parent / "core" / "domain"
    declared = set(domain_pkg.__all__)
    defined: set[str] = set()
    for py in root.glob("*.py"):
        if py.name == "__init__.py":
            continue
        tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and not node.name.startswith("_"):
                defined.add(node.name)
    missing = sorted(defined - declared)
    if missing:
        raise AssertionError(
            "публичные символы домена отсутствуют в core.domain.__all__: " + ", ".join(missing)
        )


def _domain_invariants() -> None:
    """Чистые инварианты домена возвращают Violation, не исключения (T2.2)."""
    from core.domain.enums import (
        AbsenceRuleType,
        CellState,
        Status,
    )
    from core.domain.invariants import check_cell
    from core.domain.models import Cell

    def _cell(**kw):
        base = dict(
            run_id="r",
            product_id="p",
            param_id="x",
            state=CellState.EVALUATED,
            status=None,
            evidence=[],
            absence_evidence=[],
            reviewed_by=None,
            absence_rule=AbsenceRuleType.EXPLICIT,
        )
        base.update(kw)
        return Cell.model_construct(**base)  # type: ignore[call-arg]

    def _codes(c) -> set[str]:
        return {v.code for v in check_cell(c)}

    if "I1" not in _codes(_cell(status=Status.OK)):
        raise AssertionError("I1 не пойман")
    if "I4" not in _codes(_cell(state=CellState.FAILED, status=Status.OK)):
        raise AssertionError("I4 не пойман")
    if "I13" not in _codes(_cell(state=CellState.PRELIMINARY, status=Status.OK)):
        raise AssertionError("I13 не пойман")
    if "I7" not in _codes(_cell(status=Status.NOT_APPLICABLE)):
        raise AssertionError("I7 не пойман")
    if "I2" not in _codes(_cell(status=Status.ABSENT)):
        raise AssertionError("I2 не пойман")
    if "I2b" not in _codes(
        _cell(
            status=Status.ABSENT,
            absence_rule=AbsenceRuleType.PARAMETER_REFERENCE,
            absence_evidence=[{"source_scope": "exhaustive"}],
        )
    ):
        raise AssertionError("I2b не пойман (нет scope_attested_by)")
    clean = _cell(
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
        absence_evidence=[{"source_scope": "exhaustive"}],
    )
    if check_cell(clean):
        raise AssertionError("registry_absence exhaustive должен быть чистым")


def _corpus_coverage_invariant() -> None:
    """Ф0/I2c: ``❌`` при неполном покрытии корпуса без аттестации — нарушение.

    «Инварианты — это код»: проверка живёт в ``core.domain.invariants``, а selfcheck
    прогоняет её на обеих ветках (машинный ❌ vs человечески аттестованный ❌) и на
    полном покрытии. Плюс модель ``CorpusCoverage`` не даёт объявить усечённый корпус
    полным.
    """
    from core.domain.enums import (
        AbsenceRuleType,
        CellState,
        CrawlItemStatus,
        Status,
    )
    from core.domain.invariants import check_absence_coverage
    from core.domain.models import Cell, CorpusCoverage, CrawlSessionItem

    incomplete = CorpusCoverage(
        product_id="p",
        complete=False,
        expected_sources=5,
        observed_sources=2,
        blocked={"robots_disallow": 3},
        unreadable_origins=["closed.example"],
        partial_blocks={"closed.example": {"robots_disallow": 3}},
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )
    machine_absent = Cell(
        run_id="r",
        product_id="p",
        param_id="x",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.EXPLICIT,
        absence_evidence=[{"source_scope": "unknown", "scope_attested_by": None}],
    )
    violations = check_absence_coverage(machine_absent, incomplete)
    codes = {v.code for v in violations}
    if "I2c" not in codes:
        raise AssertionError("I2c: ❌ при неполном корпусе без аттестации не пойман")
    if not any("closed.example" in v.message for v in violations):
        raise AssertionError("I2c: сообщение обязано называть нечитаемый origin")

    attested = machine_absent.model_copy(
        update={
            "absence_evidence": [
                {"source_scope": "unknown", "scope_attested_by": "human"}
            ]
        }
    )
    if check_absence_coverage(attested, incomplete):
        raise AssertionError("I2c: аттестованный человеком ❌ обязан быть допустим")

    complete = incomplete.model_copy(
        update={
            "complete": True,
            "observed_sources": 5,
            "blocked": {},
            "unreadable_origins": [],
            "partial_blocks": {},
        }
    )
    if check_absence_coverage(machine_absent, complete):
        raise AssertionError("I2c: полное покрытие не должно блокировать ❌")

    try:
        CorpusCoverage(
            product_id="p",
            complete=True,
            expected_sources=5,
            observed_sources=2,
            blocked={},
            truncated=True,
            computed_at="2026-01-01T00:00:00+00:00",
        )
    except ValueError:
        pass
    else:
        raise AssertionError("CorpusCoverage: truncated=True несовместим с complete=True")

    # Ф0/2.2.0: блокирующий in-scope сбой делает покрытие неполным даже на читаемом
    # origin'е; ``blocking_failures`` — производное представление для отчёта.
    from core.pipeline.corpus import derive_corpus_coverage

    incomplete_blocking = derive_corpus_coverage(
        product_id="p",
        items=[
            CrawlSessionItem(
                session_id="crawl:r",
                product_id="p",
                url="https://a.example/1",
                status=CrawlItemStatus.FETCHED,
            ),
            CrawlSessionItem(
                session_id="crawl:r",
                product_id="p",
                url="https://a.example/2",
                status=CrawlItemStatus.FAILED,
                error_kind="robots_disallow",
            ),
        ],
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )
    if incomplete_blocking.complete is not False:
        raise AssertionError("Ф0: robots_disallow на читаемом origin'е обязан давать complete=False")
    if incomplete_blocking.blocking_failures != {"robots_disallow": 1}:
        raise AssertionError("Ф0: blocking_failures обязан называть класс и счётчик")
    if incomplete_blocking.unreadable_origins:
        raise AssertionError("Ф0: читаемый origin не должен попадать в unreadable_origins")

    nonblocking = derive_corpus_coverage(
        product_id="p",
        items=[
            CrawlSessionItem(
                session_id="crawl:r",
                product_id="p",
                url="https://a.example/1",
                status=CrawlItemStatus.FETCHED,
            ),
            CrawlSessionItem(
                session_id="crawl:r",
                product_id="p",
                url="https://a.example/2",
                status=CrawlItemStatus.FAILED,
                error_kind="source_unavailable",
            ),
        ],
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )
    if nonblocking.complete is not True:
        raise AssertionError("Ф0: source_unavailable на читаемом origin'е не блокирует")
    if nonblocking.blocking_failures:
        raise AssertionError("Ф0: неблокирующий класс не должен попадать в blocking_failures")


def _domain_derived() -> None:
    """Derived-evidence требует derived-тип, резолв и hash (I1/I8/I10/I15/I19, K5)."""
    from core.domain.enums import EvidenceType, SourceKind
    from core.domain.invariants import check_derived_evidence
    from core.domain.models import CellEvidence, UpstreamRef

    up = UpstreamRef(
        product_id="p", param_id="x", source_id="c", source_url="http://u", content_hash="h"
    )
    good = CellEvidence.model_construct(
        evidence_id="e",
        run_id="r",
        product_id="p",
        param_id="x",
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id="c",
        source_url="http://u",
        content_hash="h",
        citation_quote=None,
    )
    if check_derived_evidence(good, up):
        raise AssertionError(f"валидный derived-evidence отвергнут: {check_derived_evidence(good, up)}")
    bad = CellEvidence.model_construct(
        evidence_id="e",
        run_id="r",
        product_id="p",
        param_id="x",
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id="c",
        source_url="http://u",
        content_hash=None,
        citation_quote=None,
    )
    if "I19" not in {v.code for v in check_derived_evidence(bad, up)}:
        raise AssertionError("I19 (нет hash) не пойман")


def _domain_anchors() -> None:
    """Резолв source_id в URL со сверкой hash (I10/I15/I19)."""
    from core.domain.anchors import (
        ResolvedSource,
        resolve_source_url,
        verify_evidence_hash,
    )
    from core.domain.enums import EvidenceType, SourceKind
    from core.domain.models import CellEvidence
    from core.util.ids import SourceRef, parse_source_id, source_id

    sid = source_id("p", "doc1", 7, "bld")
    if parse_source_id(sid) != SourceRef("p", "doc1", 7, "bld"):
        raise AssertionError("source_id round-trip сломан")

    class _R:
        def resolve(self, source_id: str) -> ResolvedSource:
            return ResolvedSource("c1", "http://u", "HASH", "bld")

    if resolve_source_url("c1", _R(), expected_hash="OTHER") is not None:
        raise AssertionError("I19: расхождение hash обязано инвалидировать source_id")
    ev = CellEvidence.model_construct(
        evidence_id="e",
        run_id="r",
        product_id="p",
        param_id="x",
        source_kind=SourceKind.DOCUMENTATION,
        evidence_type=EvidenceType.DIRECT,
        source_id="c1",
        content_hash="HASH",
        source_url="http://u",
        citation_quote="q",
    )
    if not verify_evidence_hash(ev, _R()):
        raise AssertionError("I19: совпадающий hash обязан пройти")


def _util_hashing() -> None:
    """Детерминизм content_hash/prompt_hash (I19, §5)."""
    from core.util.hashing import content_hash, prompt_hash

    if content_hash("x") != content_hash(b"x"):
        raise AssertionError("content_hash: str/bytes должны совпадать")
    if prompt_hash("v", "r", "e", ["b", "a"]) != prompt_hash("v", "r", "e", ["a", "b"]):
        raise AssertionError("prompt_hash обязан быть порядко-независимым")
    if prompt_hash("v", "r", "e", ["a"]) == prompt_hash("v", "r", "e", ["b"]):
        raise AssertionError("prompt_hash обязан зависеть от состава чанков")


def _domain_scoring() -> None:
    """I5: failed вне Capability, но в знаменателе Coverage (T2.3, §6)."""
    from core.domain.enums import CellState, Importance, ParamType, Status
    from core.domain.models import CatalogRow, Cell
    from core.domain.scoring import evidence_coverage, resolved_capability

    def _cell(pid, status, state=CellState.EVALUATED):
        return Cell.model_construct(
            run_id="r",
            product_id=pid,
            param_id="x",
            state=state,
            status=status,
            evidence=[],
            absence_evidence=[],
            reviewed_by=None,
        )

    params = {
        "x": CatalogRow.model_construct(
            param_id="x",
            title="t",
            module="m",
            group="g",
            importance=Importance.CRITICAL,
            param_type=ParamType.FACT,
            rule="r",
        )
    }
    weights = {"m": 1.0}

    cells = [_cell("p", Status.OK), _cell("p", None, CellState.FAILED)]
    cap = resolved_capability(cells, params, weights)
    cov = evidence_coverage(cells, params, weights)
    if cap.denominator <= 0:
        raise AssertionError("I5: Capability обязан считать по разрешённым ячейкам")
    if cov.denominator < cap.denominator:
        raise AssertionError("I5: failed обязан входить в знаменатель Coverage")

    cap_unknown = resolved_capability([_cell("p", Status.UNKNOWN)], params, weights)
    if cap_unknown.denominator != 0:
        raise AssertionError("I5: ❓ обязан быть вне знаменателя Capability")


def _store_resolver_i19() -> None:
    """SqliteStore резолвит source_id и сверяет content_hash (I10/I15/I19, T3.4)."""
    from core.domain.anchors import verify_evidence_hash
    from core.domain.enums import EvidenceType, SourceKind
    from core.domain.models import CellEvidence
    from core.store.sqlite_store import SqliteStore
    from core.util.hashing import content_hash

    with SqliteStore(":memory:") as store:
        h = content_hash("payload")
        store.conn.execute(
            "INSERT INTO corpus_docs (doc_id, product_id, url) VALUES (?, ?, ?)",
            ("d1", "p1", "https://e.test/doc"),
        )
        store.conn.execute(
            "INSERT INTO corpus_chunks"
            " (chunk_id, doc_id, product_id, url, chunk_no, text, content_hash, build_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("c1", "d1", "p1", "https://e.test/doc#a", 0, "текст", h, "b1"),
        )
        store.conn.commit()

        ev = CellEvidence.model_construct(
            evidence_id="e",
            run_id="r",
            product_id="p1",
            param_id="x",
            source_kind=SourceKind.DOCUMENTATION,
            evidence_type=EvidenceType.DIRECT,
            source_id="c1",
            content_hash=h,
            source_url="https://e.test/doc#a",
            citation_quote="фрагмент",
        )
        if not verify_evidence_hash(ev, store):
            raise AssertionError("I19: совпадающий hash должен пройти резолв")
        bad = ev.model_copy(update={"content_hash": content_hash("other")})
        if verify_evidence_hash(bad, store):
            raise AssertionError("I19: расхождение hash обязано инвалидировать evidence")


def _pipeline_gates_resume() -> None:
    """Гейт не пройден без gate_decisions (S9); durable resume через runs.state (P4)."""
    import datetime as _dt

    from core.domain import Gate, RunConfig, RunMode
    from core.pipeline.gates import is_gate_passed, record_decision
    from core.pipeline.runner import GATE_PHASES, PHASES, PhaseResult, Runner
    from core.store.sqlite_store import SqliteStore

    def _ok_phases() -> dict:
        """Не-гейтовые фазы-успехи: изолируют поведение гейта/resume (реестр по умолчанию — гейты)."""

        def make(phase: str):
            def _fn(ctx):
                return PhaseResult(status="ok", phase=phase)

            return _fn

        return {name: make(name) for name in PHASES if name not in GATE_PHASES}

    cfg = RunConfig(
        case_id="c",
        anchor="p1",
        cohort="co1",
        products=["p1", "p2"],
        package="pk",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    with SqliteStore(":memory:") as store:
        store.create_run("r1", cfg)
        # Гейт не пройден без строки; прогон останавливается на g1.
        r1 = Runner(store, phases=_ok_phases())
        rep = r1.run("r1", until="g1")
        if rep.stopped_at != "g1" or rep.status != "blocked":
            raise AssertionError("гейт обязан остановить прогон без решения (S9)")
        if is_gate_passed(store, "r1", Gate.G1):
            raise AssertionError("гейт не пройден без строки gate_decisions")
        # Durable resume: новый Runner видит завершённые фазы через store.
        r2 = Runner(store, phases=_ok_phases())
        rep2 = r2.run("r1", until="plan")
        if rep2.results:
            raise AssertionError(f"durable resume: ok-фазы должны пропускаться, выполнено {[x.phase for x in rep2.results]}")
        # Человеческое решение проходит гейт.
        record_decision(store, "r1", Gate.G1, actor="human", target="plan")
        if not is_gate_passed(store, "r1", Gate.G1):
            raise AssertionError("гейт обязан пройтись после строки gate_decisions")


def _cli_plan_lint() -> None:
    """CLI-фасад plan/dry-run: валидный пакет → 0; битый (I2b) → блок с кодом."""
    import contextlib
    import io
    import shutil
    import tempfile

    from core.cli.main import EXIT_OK, EXIT_USAGE, main
    from core.cli.wiring import LOCAL_PROFILE, SPLIT_PROFILE, WiringError, build_adapters

    def _silent(argv: list[str]) -> int:
        """Запускает CLI, подавляя его stdout/stderr (проверяется только код возврата)."""
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return main(argv)

    fixtures = ROOT / "tests" / "config" / "fixtures"
    raw_code = _silent(
        [
            "plan",
            "--run-yaml",
            str(fixtures / "run.yaml"),
            "--package",
            str(fixtures),
            "--dry-run",
            "--json",
        ]
    )
    if raw_code != EXIT_USAGE:
        raise AssertionError(f"битый пакет (I2b) обязан блокироваться, код={raw_code}")
    with tempfile.TemporaryDirectory() as tmp:
        pkg = Path(tmp) / "package"
        pkg.mkdir()
        for item in fixtures.iterdir():
            if item.is_file():
                shutil.copy(item, pkg / item.name)
        (pkg / "scope_attestation.yaml").write_text(
            'procedure: "аттестация (selfcheck)"\n', encoding="utf-8"
        )
        good_code = _silent(
            [
                "plan",
                "--run-yaml",
                str(pkg / "run.yaml"),
                "--package",
                str(pkg),
                "--dry-run",
                "--json",
            ]
        )
        if good_code != EXIT_OK:
            raise AssertionError(f"валидный пакет обязан планироваться, код={good_code}")
    # Профиль split честно недоступен до W13 (шов 07), local — ок.
    if build_adapters(profile=LOCAL_PROFILE) is None:
        raise AssertionError("local-профиль обязан собирать Ports")
    try:
        build_adapters(profile=SPLIT_PROFILE)
        raise AssertionError("split-профиль обязан быть честно недоступен до W13")
    except WiringError:
        pass


def _cli_wiring_profiles() -> None:
    """Профили развёртывания: local ок, split — явный отказ (07/W13)."""
    import contextlib
    import io
    import tempfile

    from core.cli.main import EXIT_NOT_FOUND, main
    from core.cli.wiring import LOCAL_PROFILE, SPLIT_PROFILE, SUPPORTED_PROFILES, WiringError, build_adapters

    if set(SUPPORTED_PROFILES) != {LOCAL_PROFILE, SPLIT_PROFILE}:
        raise AssertionError(f"неожиданный набор профилей: {SUPPORTED_PROFILES}")
    try:
        build_adapters(profile="nope")
        raise AssertionError("неизвестный профиль обязан отвергаться")
    except WiringError:
        pass
    # resolve по пустой БД — промах I10/I15 (не исключение).
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "cohound.db"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = main(["resolve", "missing", "--db", str(db)])
        if code != EXIT_NOT_FOUND:
            raise AssertionError(f"промах resolve обязан давать EXIT_NOT_FOUND, код={code}")


def _util_cache() -> None:
    """Content-addressed кэш: round-trip, get_or_fetch (один фетч), детерминизм (R7)."""
    import tempfile

    from core.util.cache import ContentAddressedCache, http_key, serp_key

    with tempfile.TemporaryDirectory() as tmp:
        cache = ContentAddressedCache(root=tmp)
        key = http_key("https://example.test/a")
        if cache.get_bytes("http", key) is not None:
            raise AssertionError("промах кэша обязан возвращать None")
        cache.put_bytes("http", key, b"payload")
        if cache.get_bytes("http", key) != b"payload":
            raise AssertionError("round-trip bytes кэша сломан")

        calls: list[int] = []

        def _fetch() -> bytes:
            calls.append(1)
            return b"X"

        k2 = http_key("https://example.test/b")
        cache.get_or_fetch("http", k2, _fetch)
        cache.get_or_fetch("http", k2, _fetch)
        if len(calls) != 1:
            raise AssertionError(f"get_or_fetch обязан фетчить один раз, было {len(calls)}")
        if serp_key("  Alpha  Beta ") != serp_key("alpha beta"):
            raise AssertionError("serp_key обязан нормализовать регистр/пробелы")
        if http_key("u1") == http_key("u2"):
            raise AssertionError("разные URL обязаны давать разные ключи")


def _extract_html() -> None:
    """HtmlExtractor: hash = hash текста (I19), деградация без исключения."""
    from core.adapters.extract_html import HtmlExtractor
    from core.ports.types import RawPage
    from core.util.hashing import content_hash

    good = HtmlExtractor(extract_fn=lambda _h: "извлечённый текст")
    doc = good.extract(RawPage(url="https://e.test/a", content=b"<p>x</p>", fetched_at="t"))
    if doc.error is not None or not doc.text:
        raise AssertionError(f"успешная экстракция: {doc.error!r}")
    if doc.content_hash != content_hash(doc.text):
        raise AssertionError("I19: content_hash обязан считаться от извлечённого текста")
    if not doc.extractor_version:
        raise AssertionError("extractor_version обязателен")

    bad = HtmlExtractor(extract_fn=lambda _h: None)
    degraded = bad.extract(RawPage(url="https://e.test/a", content=b"<p>x</p>", fetched_at="t"))
    if not degraded.error or degraded.text != "":
        raise AssertionError("сбой извлечения обязан деградировать в error без исключения")


def _util_sanitize() -> None:
    """Санитайзинг сохраняет оболочку документа и вырезает активный HTML (T7.5)."""
    from core.util.sanitize import _DANGEROUS_BLOCK_TAGS, _stdlib_sanitize_html, sanitize_html

    doc = (
        "<html><head><title>Документ: главная</title>"
        "<style>body { color: red; }</style></head>"
        "<body><article><h1>Заголовок</h1>"
        "<p>main-content-marker: основной текст.</p></article>"
        "<script>alert(1)</script>"
        '<img src="x" onerror="steal()">'
        '<a href="javascript:alert(2)">j</a></body></html>'
    )
    cleaned = sanitize_html(doc)
    # Оболочка документа (маркер ``<title>``) сохраняется — иначе trafilatura
    # извлекает пустой текст (регрессия T7.5).
    _assert("<title>" in cleaned)
    _assert("main-content-marker" in cleaned)
    # Активный контент по-прежнему вырезан.
    _assert("script" not in cleaned.lower())
    _assert("style" not in cleaned.lower())
    _assert("onerror" not in cleaned.lower())
    _assert("steal" not in cleaned)
    _assert("javascript:" not in cleaned.lower())
    # Fallback: сохраняет оболочку literal-тегами и вырезает опасные блоки с содержимым.
    fb = _stdlib_sanitize_html(doc)
    _assert("<html>" in fb and "<body>" in fb and "<title>" in fb)
    for tag in _DANGEROUS_BLOCK_TAGS:
        text = _stdlib_sanitize_html(f"<p>x</p><{tag}>danger</{tag}><p>y</p>")
        _assert(f"<{tag}" not in text.lower())
        _assert("danger" not in text and "x" in text and "y" in text)


def _crawl_scrapy() -> None:
    """ScrapyCrawler: HTTP-кэш, browser-байпас кэша, SSRF-блок, FetchPlan (D2)."""
    import tempfile

    from core.adapters.crawl_scrapy import ScrapyCrawler
    from core.ports.crawler import CrawlerPort
    from core.ports.types import FetchMode, FetchPlan, RawPage
    from core.util.cache import ContentAddressedCache, http_key

    calls: list[int] = []

    def _fake(url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        calls.append(1)
        return RawPage(url=url, content=b"<html>ok</html>", fetched_at="t", fetch_mode=plan.fetch)

    with tempfile.TemporaryDirectory() as tmp:
        cache = ContentAddressedCache(root=tmp)
        crawler = ScrapyCrawler(fetcher=_fake, cache=cache)
        if not isinstance(crawler, CrawlerPort):
            raise AssertionError("ScrapyCrawler обязан удовлетворять CrawlerPort")
        # browser-ось плана: кэш НЕ консультируется и не пишется.
        first = crawler.fetch(
            "https://example.com/x", FetchPlan(FetchMode.BROWSER)
        )
        crawler.fetch("https://example.com/x", FetchPlan(FetchMode.BROWSER))
        if first.fetch_mode is not FetchMode.BROWSER:
            raise AssertionError("browser-ось плана обязана выбрать browser-режим")
        if len(calls) != 2:
            raise AssertionError(f"browser-рендер обязан идти вживую (2 фетча), было {len(calls)}")
        if cache.get_bytes("http", http_key("https://example.com/x")) is not None:
            raise AssertionError("browser-рендер не должен писать в кэш")
        # HTTP-путь кэшируется (R7): повторный вызов не дёргает фетчер.
        calls.clear()
        crawler.fetch("https://other.test/y")
        crawler.fetch("https://other.test/y")
        if len(calls) != 1:
            raise AssertionError(f"HTTP-кэш обязан дать 1 фетч, было {len(calls)}")
        try:
            crawler.fetch("http://127.0.0.1/secret")
            raise AssertionError("SSRF-цель обязана блокироваться до фетча")
        except Exception as exc:  # noqa: BLE001 — ожидаемый отказ
            if "SSRF" not in str(exc) and "приватн" not in str(exc) and "127.0.0.1" not in str(exc):
                raise


def _chunker_port() -> None:
    """MarkdownChunkerPort структурно реализует ChunkerPort (T7.3)."""
    from core.adapters.chunker_markdown import MarkdownChunkerPort
    from core.ports.chunker import ChunkerPort
    from core.ports.types import ExtractedDoc

    chunker = MarkdownChunkerPort()
    if not isinstance(chunker, ChunkerPort):
        raise AssertionError("MarkdownChunkerPort обязан удовлетворять ChunkerPort")
    chunks = chunker.chunk(
        ExtractedDoc(text="# H1\nтело", content_hash="h", extractor_version="v"), "doc1"
    )
    if [c.chunk_id for c in chunks] != ["doc1#0"] or chunks[0].heading_path != "H1":
        raise AssertionError("порт-чанкинг сломан")


def _retrieve_profiles() -> None:
    """Профили R0/R1/R2 явны; ретривал продукт-скоупед (I16/I17)."""
    from core.pipeline.retrieve import RetrievalProfile, retrieve
    from core.ports.vector_index import RetrievedChunk

    def _c(cid, product="p1"):
        return RetrievedChunk(
            chunk_id=cid, product_id=product, text="t",
            content_hash="h", heading_path="H", build_id="b", score=1.0,
        )

    class _Lex:
        engine_version = "fake"

        def search(self, query, product_ids, top_k, lang=None):
            return [c for c in [_c("c1"), _c("c9", "p2")] if c.product_id in set(product_ids)][:top_k]

    class _Vec:
        build_id = "bx"
        dim = 2

        def search(self, q):
            return [c for c in [_c("c1")] if c.product_id in set(q.product_ids)][: q.top_k]

        def shard_ids(self, product_id):
            return [product_id]

    r0 = retrieve(query_text="q", query_vector=None, product_ids=["p1"],
                  profile="R0", cutoff_k=5, lexical_index=_Lex())
    if r0.profile != "R0" or [c.chunk_id for c in r0.chunks] != ["c1"]:
        raise AssertionError(f"R0/продукт-скоуп сломан: {[c.chunk_id for c in r0.chunks]}")
    r1 = retrieve(query_text="q", query_vector=[1.0, 0.0], product_ids=["p1"],
                  profile=RetrievalProfile.R1, cutoff_k=5, vector_index=_Vec())
    if r1.profile != "R1" or r1.build_id != "bx":
        raise AssertionError("R1 сломан")


def _rrf_fusion() -> None:
    """RRF: дедуп по chunk_id, сумма вкладов, провенанс, tie-break (R7)."""
    from core.pipeline.retrieve import rrf_fuse_chunks
    from core.ports.vector_index import RetrievedChunk

    def _c(cid, build="b-lex"):
        return RetrievedChunk(
            chunk_id=cid, product_id="p1", text=f"t-{cid}",
            content_hash=f"h-{cid}", heading_path="H", build_id=build, score=1.0,
        )

    fused = rrf_fuse_chunks([_c("c1"), _c("c2")], [_c("c2", build="b-dense"), _c("c3")])
    ids = [c.chunk_id for c in fused]
    if ids[0] != "c2" or set(ids) != {"c1", "c2", "c3"}:
        raise AssertionError(f"RRF-дедуп/сумма сломан: {ids}")
    if fused[0].build_id != "b-dense":
        raise AssertionError("dense-провенанс обязан быть приоритетным")
    # tie-break детерминирован
    a = [c.chunk_id for c in rrf_fuse_chunks([_c("b"), _c("a")], [_c("a"), _c("b")])]
    if a != ["a", "b"]:
        raise AssertionError(f"tie-break не по chunk_id: {a}")


def _embed_local() -> None:
    """LocalEmbedder: батч-нарезка и torque-free контракт (T8.1)."""
    from core.adapters.embed_local import LocalEmbedder
    from core.ports.embedder import EmbedderPort

    batches: list[int] = []

    def _fake(texts):
        batches.append(len(texts))
        return [[0.0] * 3 for _ in texts]

    emb = LocalEmbedder(embed_fn=_fake, model_id="m", dim=3, batch_size=2)
    if not isinstance(emb, EmbedderPort) or emb.model_id != "m" or emb.dim != 3:
        raise AssertionError("LocalEmbedder не соответствует EmbedderPort")
    out = emb.embed(["a", "b", "c", "d", "e"])
    if len(out) != 5 or any(len(v) != 3 for v in out):
        raise AssertionError("embed обязан вернуть N×dim")
    if batches != [2, 2, 1]:
        raise AssertionError(f"батчинг сломан: {batches}")


def _embed_model2vec() -> None:
    """LiteEmbedder (P4a): оффлайн-шов, guard пустого входа, пин ревизии в ``model_id``."""
    from core.adapters.embed_model2vec import DEFAULT_MODEL_ID, LiteEmbedder
    from core.ports.embedder import EmbedderPort

    def _fake(texts):
        return [[0.0] * 3 for _ in texts]

    emb = LiteEmbedder(embed_fn=_fake, dim=3, revision="sha1")
    if not isinstance(emb, EmbedderPort) or emb.dim != 3:
        raise AssertionError("LiteEmbedder не соответствует EmbedderPort")
    if emb.model_id != f"{DEFAULT_MODEL_ID}@sha1":
        raise AssertionError(f"ревизия не в model_id: {emb.model_id!r}")
    out = emb.embed(["a", "b"])
    if len(out) != 2 or any(len(v) != 3 for v in out):
        raise AssertionError("LiteEmbedder.embed обязан вернуть N×dim")
    try:
        emb.embed(["   "])
        raise AssertionError("пустой текст обязан отвергаться (иначе нулевой вектор)")
    except ValueError:
        pass


def _index_roundtrip() -> None:
    """Индексная сборка .npy: round-trip партиции + cosine-поиск (T8.2/T8.4, I17).

    Пропускается, если numpy не установлен (selfcheck автономен, без обязательных extras).
    """
    try:
        import numpy  # noqa: F401
    except ImportError:
        return
    import tempfile

    from core.adapters import index_format
    from core.adapters.index_npy import NpyIndexBuilder
    from core.adapters.vector_npy import NpyVectorIndex
    from core.ports.index_builder import IndexBuilderPort, IndexMeta
    from core.ports.types import Chunk
    from core.ports.vector_index import VectorIndexPort, VectorQuery

    with tempfile.TemporaryDirectory() as tmp:
        builder = NpyIndexBuilder(index_root=tmp, package="pk")
        if not isinstance(builder, IndexBuilderPort):
            raise AssertionError("NpyIndexBuilder обязан реализовывать IndexBuilderPort")
        chunks = [
            Chunk(chunk_id="c0", chunk_no=0, text="a", heading_path="H",
                  context_prefix="", content_hash="h0"),
            Chunk(chunk_id="c1", chunk_no=1, text="b", heading_path="H",
                  context_prefix="", content_hash="h1"),
        ]
        meta = IndexMeta(
            package="pk", chunker_version="v1", embed_model_id="m",
            snapshot_date="2026-01-01", product_id="p1",
            extra={"partition_build_id": "pb_sc"},
        )
        bid = builder.build(chunks, [[1.0, 0.0], [0.0, 1.0]], meta)
        builder.commit()
        if bid != "pb_sc" or not index_format.is_committed(tmp, "pk", "pb_sc", "p1"):
            raise AssertionError("сборка .npy-партиции не зафиксирована")
        if index_format.list_partitions(tmp, "pk", "pb_sc") != ["p1"]:
            raise AssertionError("list_partitions обязан вернуть продукт-партиции сборки")

        lookup = {c.chunk_id: c for c in chunks}
        vindex = NpyVectorIndex(
            index_root=tmp, package="pk", build_id="pb_sc", dim=2, chunk_lookup=lookup
        )
        if not isinstance(vindex, VectorIndexPort):
            raise AssertionError("NpyVectorIndex обязан реализовывать VectorIndexPort")
        hits = vindex.search(VectorQuery(product_ids=["p1"], top_k=2, vector=[0.9, 0.1]))
        if not hits or hits[0].chunk_id != "c0" or hits[0].content_hash != "h0":
            raise AssertionError(f"cosine/provenance сломан: {[h.chunk_id for h in hits]}")


def _retrieval_read_loop() -> None:
    """P4a: ``vector_index`` строится из зафиксированной сборки (product-scoped, I17).

    Проверяет, что composition-хелпер ``build_vector_index`` резолвит единственную
    сборку пакета, читает ``dim`` из манифеста и возвращает рабочий ``NpyVectorIndex``;
    без резолвера чанков — честный ``None`` (явное отсутствие, N3). Пропускается без numpy.
    """
    try:
        import numpy  # noqa: F401
    except ImportError:
        return
    import tempfile

    from core.adapters import index_format
    from core.cli.wiring import build_vector_index
    from core.ports.types import Chunk
    from core.ports.vector_index import VectorQuery

    with tempfile.TemporaryDirectory() as tmp:
        index_format.write_partition(
            tmp, "pk", "ib_rl", "pA", [[1.0, 0.0]], ["ca"], {"dim": 2}
        )
        index_format.write_partition(
            tmp, "pk", "ib_rl", "pB", [[0.0, 1.0]], ["cb"], {"dim": 2}
        )
        lookup = {
            "ca": Chunk(chunk_id="ca", chunk_no=0, text="A", heading_path="",
                        context_prefix="", content_hash="hA"),
            "cb": Chunk(chunk_id="cb", chunk_no=0, text="B", heading_path="",
                        context_prefix="", content_hash="hB"),
        }
        if build_vector_index(index_root=tmp, package="pk", chunk_lookup=None) is not None:
            raise AssertionError("без chunk_lookup индекс обязан быть None (N3)")
        index = build_vector_index(index_root=tmp, package="pk", chunk_lookup=lookup)
        if index is None or index.build_id != "ib_rl" or index.dim != 2:
            raise AssertionError("dense read-loop не собрался из сборки")
        only_a = index.search(VectorQuery(product_ids=["pA"], top_k=5, vector=[1.0, 0.0]))
        if [c.chunk_id for c in only_a] != ["ca"]:
            raise AssertionError(f"I17 нарушен: {[c.chunk_id for c in only_a]}")


def _compute_i5() -> None:
    """compute: failed в знаменателе Coverage, вне Capability (I5); K6-демотирование."""
    import datetime as _dt

    from core.domain import CatalogRow, Cell, CellState, Importance, ParamType, RunConfig, RunMode, Status
    from core.pipeline.compute import compute
    from core.store.sqlite_store import SqliteStore

    def _cell(pid, param_id, status, state=CellState.EVALUATED):
        return Cell.model_construct(
            run_id="r1", product_id=pid, param_id=param_id, state=state, status=status,
            evidence=[{"source_id": "c", "quote": "q"}], absence_evidence=[], reviewed_by=None,
        )

    with SqliteStore(":memory:") as store:
        cfg = RunConfig(case_id="c", anchor="p1", cohort="co", products=["p1"],
                        package="pk", snapshot_date=_dt.date(2026, 1, 1), mode=RunMode.STRICT)
        store.create_run("r1", cfg)
        store.save_cells_batch([
            _cell("p1", "x", Status.OK),
            _cell("p1", "y", None, CellState.FAILED),
        ])
        row_x = CatalogRow.model_construct(
            param_id="x", title="t", module="m", group="g",
            importance=Importance.CRITICAL, param_type=ParamType.FACT, rule="r",
        )
        row_y = CatalogRow.model_construct(
            param_id="y", title="t", module="m", group="g",
            importance=Importance.CRITICAL, param_type=ParamType.FACT, rule="r",
        )
        bundle = compute(
            store=store, run_id="r1",
            catalog={"x": row_x, "y": row_y}, module_weights={"m": 1.0},
        )
        if bundle.capability.value != 1.0:
            raise AssertionError(f"I5: failed вне Capability, получено {bundle.capability.value}")
        if not 0.0 < bundle.coverage.value < 1.0:
            raise AssertionError(f"I5: failed обязан снижать Coverage, получено {bundle.coverage.value}")
        if bundle.failure_rate != 0.5:
            raise AssertionError(f"failure_rate сломан: {bundle.failure_rate}")
        if bundle.computed_ranking not in {"capability_ranked", "coverage_fallback"}:
            raise AssertionError(f"K6 computed_ranking невалиден: {bundle.computed_ranking}")


def _validate_echelon() -> None:
    """Валидация цитат: source_id∈chunks, цитата-подстрока, CellEvidence с content_hash (I10/I19)."""
    from core.domain import LLMCellOutput, Status
    from core.pipeline.validate import validate_output
    from core.ports.types import Chunk

    chunk = Chunk(chunk_id="c1", chunk_no=0, text="Резервное копирование поддерживает PITR.",
                  heading_path="H", context_prefix="", content_hash="hc1")
    chunks = {"c1": chunk}

    good = LLMCellOutput(status=Status.OK, justification="есть",
                         evidence=[LLMEvidence(source_id="c1", quote="поддерживает PITR")])
    vr = validate_output(output=good, chunks=chunks, product_id="p1", param_id="x", run_id="r1")
    if not vr.ok or len(vr.resolved_evidence) != 1:
        raise AssertionError(f"валидная цитата отклонена: {vr.notes}")
    if vr.resolved_evidence[0].content_hash != "hc1":
        raise AssertionError("I19: content_hash чанка обязан попасть в evidence")

    bad_quote = LLMCellOutput(status=Status.OK, justification="j",
                              evidence=[LLMEvidence(source_id="c1", quote="нет такой подстроки")])
    vr2 = validate_output(output=bad_quote, chunks=chunks, product_id="p1", param_id="x", run_id="r1")
    if vr2.ok or vr2.failure_kind != "validation_failed":
        raise AssertionError("цитата не-подстрока обязана провалиться")

    bad_src = LLMCellOutput(status=Status.OK, justification="j",
                            evidence=[LLMEvidence(source_id="missing", quote="x")])
    vr3 = validate_output(output=bad_src, chunks=chunks, product_id="p1", param_id="x", run_id="r1")
    if vr3.ok:
        raise AssertionError("I10/I15: несуществующий source_id обязан провалиться")


def _evaluate_failed() -> None:
    """Оценка Ш6: strict исчерпание -> failed/status None (I4); preliminary -> I13."""
    from core.domain import CatalogRow, CellState, LLMCellOutput, Status
    from core.pipeline.evaluate import evaluate_cell
    from core.ports.llm import LLMResponse, TokenUsage
    from core.ports.types import Chunk

    chunk = Chunk(chunk_id="c1", chunk_no=0, text="текст", heading_path="H",
                  context_prefix="", content_hash="hc1")
    param = CatalogRow.model_construct(
        param_id="x", title="t", module="m", group="g", importance="Кр",
        param_type="fact", rule="r", absence_rule="explicit", role_binding=None, concept_terms={},
    )

    class _BadEvaluator:
        def __init__(self):
            self.calls = 0

        def complete(self, req):
            self.calls += 1
            out = LLMCellOutput(status=Status.OK, justification="j",
                                evidence=[LLMEvidence(source_id="c1", quote="НЕТ ПОДСТРОКИ")])
            return LLMResponse(output=out, raw="{}", prompt_hash="ph", decoder="none", usage=TokenUsage())

    ev = _BadEvaluator()
    outcome = evaluate_cell(param=param, product_id="p1", run_id="r1",
                            evaluator=ev, chunks=[chunk], max_retries=2)
    if outcome.cell.state is not CellState.FAILED or outcome.cell.status is not None:
        raise AssertionError(f"I4: исчерпание strict -> failed/status None, получено {outcome.cell.state}/{outcome.cell.status}")
    if ev.calls != 3:
        raise AssertionError(f"retry ≤2 => 3 вызова, было {ev.calls}")

    outcome2 = evaluate_cell(param=param, product_id="p1", run_id="r1",
                             evaluator=ev, chunks=[chunk], strict=False, max_retries=0)
    if outcome2.cell.state is not CellState.PRELIMINARY or outcome2.cell.status is not None:
        raise AssertionError("I13: preliminary -> preliminary/status None")


def _report_gates() -> None:
    """Отчётность: G4/I11 (Coverage отсутствует/UNDEFINED → блок), §13/B-7 diff, §5/I3."""
    from core.domain import (
        Cell,
        CellState,
        CatalogRow,
        ProtocolExhaustion,
        REQUIRED_EXHAUSTION_CONTOURS,
        RunManifest,
        RunMode,
        SensitivityReport,
        Status,
        TerminationReason,
        check_unknown_exhaustion,
    )
    from core.domain.scoring import Comparability, Coverage, Score
    from core.pipeline.compute import ScoreBundle
    from core.pipeline.report import (
        CrossCycleDiffError,
        PublishBlockedError,
        collect_i3_anomalies,
        diff_boundary_violations,
        render_battle_card,
        render_diff,
    )

    bundle = ScoreBundle(
        capability=Score(value=1.0, numerator=1.0, denominator=1.0, n_used=1),
        coverage=Coverage(value=0.0, numerator=0.0, denominator=0.0, n_used=0),
        failure_rate=0.0,
        modules=[],
        comparability=Comparability(level="insufficient", min_coverage=0.0, cross_cycle=False),
        sensitivity=SensitivityReport(
            stable=True, per_axis={}, unstable_pairs=[],
            requested_ranking="capability_ranked", computed_ranking="capability_ranked",
        ),
        requested_ranking="capability_ranked",
        computed_ranking="capability_ranked",
    )
    row = CatalogRow.model_construct(
        param_id="x", title="t", module="m", group="g", importance="Кр", param_type="fact", rule="r",
    )
    import tempfile as _tf

    with _tf.TemporaryDirectory() as td:
        try:
            render_battle_card(bundle, [], {"x": row}, f"{td}/battle.csv")
            raise AssertionError("I11: battle_card без Coverage обязан быть заблокирован")
        except PublishBlockedError:
            pass

        # §6/B-3a + I11: UNDEFINED-покрытие (universe = 0) — тоже непубликуемо.
        undefined = ScoreBundle(
            capability=Score(value=0.0, numerator=0.0, denominator=0.0, n_used=0),
            coverage=Coverage(
                value=None, numerator=0.0, denominator=0.0, n_used=0,
                universe=0.0, completed=0.0, defined=False,
            ),
            failure_rate=0.0,
            modules=[],
            comparability=Comparability(level="insufficient", min_coverage=0.0, cross_cycle=True),
            sensitivity=SensitivityReport(
                stable=True, per_axis={}, unstable_pairs=[],
                requested_ranking="capability_ranked", computed_ranking="capability_ranked",
            ),
            requested_ranking="capability_ranked",
            computed_ranking="capability_ranked",
        )
        try:
            render_battle_card(undefined, [], {"x": row}, f"{td}/battle_undef.csv")
            raise AssertionError("I11: battle_card при UNDEFINED Coverage обязан быть заблокирован")
        except PublishBlockedError:
            pass

        def _manifest(index_build_id: str, crawl_strategy=None):
            return RunManifest(
                run_id="r1", case_id="c", created_at="2026-01-01", mode=RunMode.STRICT,
                methodology_version="2.0.0", package_version="1", catalog_version="1",
                status_dict_version="1", scoring_policy_version="1", anchor_product_id="p1",
                index_build_id=index_build_id, retrieval_profile="R1", retrieval_cutoff_K=20,
                retrieval_params_json="{}", crawl_strategy=crawl_strategy,
            )

        try:
            render_diff(_manifest("ib_a"), _manifest("ib_b"), [], [], f"{td}/diff.md")
            raise AssertionError("§13: diff при смене index_build_id обязан отказать")
        except CrossCycleDiffError:
            pass

        # §13/B-7 (v2.1): причина версионной границы помечена классом execution_change.
        reasons = diff_boundary_violations(_manifest("ib_a"), _manifest("ib_b"))
        if not reasons or not all(r.startswith("execution_change:") for r in reasons):
            raise AssertionError(f"§13/B-7: ожидалась метка execution_change, получено {reasons}")

        # Backlog B (§13): смена оси краула (crawl.strategy) — тоже execution_change.
        crawl_reasons = diff_boundary_violations(
            _manifest("ib_a", crawl_strategy="auto"),
            _manifest("ib_a", crawl_strategy="hybrid"),
        )
        if not crawl_reasons or not all(
            r.startswith("execution_change:") for r in crawl_reasons
        ) or not any("crawl_strategy" in r for r in crawl_reasons):
            raise AssertionError(
                f"§13/Backlog B: ожидался execution_change по crawl_strategy, получено {crawl_reasons}"
            )

        # §5/I3 (v2.1): strict-❓ без полного протокола → аномалия (статус не меняется).
        unknown = Cell(
            run_id="r1", product_id="p1", param_id="x",
            state=CellState.EVALUATED, status=Status.UNKNOWN,
        )
        if not check_unknown_exhaustion(unknown, None):
            raise AssertionError("I3: strict-❓ без протокола обязан дать нарушение")
        full = ProtocolExhaustion(
            run_id="r1", product_id="p1", param_id="x",
            contours=list(REQUIRED_EXHAUSTION_CONTOURS),
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        )
        if check_unknown_exhaustion(unknown, full):
            raise AssertionError("I3: полный протокол исчерпания не должен давать нарушение")
        if not collect_i3_anomalies([unknown], None):
            raise AssertionError("I3: collect_i3_anomalies обязан зафиксировать аномалию")


def _logging_fields() -> None:
    """Логи §B.1: поля записи; ``invariant`` — отдельное поле, не ``ctx``."""
    import json as _json
    import pathlib as _pl
    import tempfile as _tf

    from core.util.logging import PhaseLogger

    with _tf.TemporaryDirectory() as td:
        lg = PhaseLogger("r1", "corpus", logs_root=td,
                         now=lambda: "2026-01-01T00:00:00Z", component="crawler")
        lg.info("шаг", product="p1")
        lg.warn("аномалия", invariant="I2b")
        lines = (_pl.Path(td) / "r1" / "logs" / "corpus.jsonl").read_text(
            encoding="utf-8"
        ).strip().splitlines()
        recs = [_json.loads(line) for line in lines]
        if len(recs) != 2:
            raise AssertionError(f"ожидалось 2 записи, получено {len(recs)}")
        for field in ("ts", "run_id", "phase", "component", "level", "message", "ctx", "invariant"):
            if field not in recs[0]:
                raise AssertionError(f"поле §B.1 отсутствует: {field}")
        if recs[0]["ctx"] != {"product": "p1"} or recs[1]["invariant"] != "I2b":
            raise AssertionError("invariant обязан быть отдельным полем, не внутри ctx")


def _run_metrics() -> None:
    """Метрики §B.2: cells_by_state/failure_rate/search_calls_by_phase."""
    from core.domain import Cell, CellState
    from core.pipeline.metrics import compute_metrics

    def _cell(pid, state):
        return Cell.model_construct(run_id="r1", product_id=pid, param_id="x", state=state,
                                    status=None, evidence=[], absence_evidence=[], reviewed_by=None)

    class _Entry:
        def __init__(self, phase):
            self.phase = phase

    cells = [_cell("p1", CellState.EVALUATED), _cell("p1", CellState.FAILED)]
    m = compute_metrics(cells=cells, search_log=[_Entry("corpus"), _Entry("corpus")], llm_calls=3)
    if m.cells_by_state.get("evaluated") != 1 or m.cells_by_state.get("failed") != 1:
        raise AssertionError(f"cells_by_state сломан: {m.cells_by_state}")
    if m.failure_rate != 0.5:
        raise AssertionError(f"failure_rate сломан: {m.failure_rate}")
    if m.search_calls_by_phase != {"corpus": 2} or m.llm_calls != 3:
        raise AssertionError("search/llm-метрики сломаны")
    # Backlog A: счётчики краула — опциональны и сортированы; дефолт — пустая карта.
    if compute_metrics(cells=cells).crawl != {}:
        raise AssertionError("crawl по умолчанию обязан быть пустым")
    m_crawl = compute_metrics(cells=cells, crawl_counts={"fetched": 2, "failed": 1})
    if m_crawl.crawl != {"failed": 1, "fetched": 2}:
        raise AssertionError(f"crawl-метрика сломана: {m_crawl.crawl}")


def _backup_gc() -> None:
    """Бэкап/GC: reference-aware dry-run + refuse при keep<1 (09 §8.6/§13)."""
    import json as _json
    import pathlib as _pl
    import tempfile as _tf

    from core.store.backup import gc_index_builds, list_builds

    with _tf.TemporaryDirectory() as td:
        for build in ("ib_1", "ib_2", "ib_3"):
            for prod in ("p1",):
                (_pl.Path(td) / "pkg" / build / prod).mkdir(parents=True)
        if len(list_builds(td, "pkg")) != 3:
            raise AssertionError("list_builds сломан")
        # 09 §8.6: reference-aware GC — партиция, на которую ссылается композиция,
        # не удаляется даже при ``keep=1`` (старая keep-политика удалила бы её).
        comp_dir = _pl.Path(td) / "pkg" / "_compositions"
        comp_dir.mkdir(parents=True)
        (comp_dir / "cb_live.json").write_text(
            _json.dumps(
                {
                    "schema": "composition/1",
                    "composition_id": "cb_live",
                    "package": "pkg",
                    "created_at": "2026-10-20T00:00:00Z",
                    "members": [
                        {
                            "product_id": "p1",
                            "partition_build_id": "ib_1",
                            "content_digest": "d1",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        dry = gc_index_builds(td, "pkg", keep=1)
        if not dry:
            raise AssertionError("dry-run обязан вернуть кандидатов на удаление")
        if "ib_1" in dry:
            raise AssertionError("живая партиция композиции не подлежит GC")
        if not all((_pl.Path(td) / "pkg" / b).is_dir() for b in ("ib_1", "ib_2", "ib_3")):
            raise AssertionError("dry-run не имеет права удалять сборки")
        try:
            gc_index_builds(td, "pkg", keep=0)
            raise AssertionError("keep<1 обязан быть отклонён")
        except ValueError:
            pass


def _extraction_golden() -> None:
    """Extraction-golden: expected/forbidden + триггер смены extractor_version (§13)."""
    from core.pipeline.extraction_eval import (
        ExtractionGoldenCase,
        extractor_version_changed,
        run_extraction_golden,
    )
    from core.ports.types import ExtractedDoc

    class _Fake:
        version = "ext-1"

        def extract(self, page, route=None):
            text = page.content.decode("utf-8")
            return ExtractedDoc(text=text, content_hash="h", extractor_version=self.version, page_count=1)

    cases = [
        ExtractionGoldenCase(case_id="a", format="html", raw_b64="aGVsbG8gd29ybGQ=",
                             expected_substrings=["hello"], min_chars=5),
        ExtractionGoldenCase(case_id="b", format="html", raw_b64="aGVsbG8gd29ybGQ=",
                             forbidden_substrings=["hello"]),
    ]
    report = run_extraction_golden(cases, _Fake())
    if report.passed != 1 or report.total != 2:
        raise AssertionError(f"extraction-golden: ожидалось 1/2, получено {report.passed}/{report.total}")
    if not extractor_version_changed("ext-1", "ext-2"):
        raise AssertionError("§13: смена extractor_version обязана триггерить прогон")
    if extractor_version_changed("", None):
        raise AssertionError("None и '' — одна «незаданная» версия, не изменение")


def _search_budget() -> None:
    """Бюджет внешнего поиска: симметрия на продукт (I16), терминация §5 (I23)."""
    from core.domain import TerminationReason
    from core.pipeline.search_log import SearchBudget, SearchEffort, external_search
    from core.ports.search import SearchQuery, SearchResult

    budget = SearchBudget.from_mapping({"corpus": 1})
    effort = SearchEffort(budget)
    if not effort.charge("corpus", "p1") or effort.charge("corpus", "p1"):
        raise AssertionError("p1 обязан исчерпать бюджет за 1 вызов")
    if not effort.charge("corpus", "p2"):
        raise AssertionError("I16: исчерпание бюджета p1 не должно влиять на p2")

    class _Ad:
        adapter_kind = "serp"
        provider = "fake"

        def __init__(self) -> None:
            self.calls = 0

        def search(self, q):
            self.calls += 1
            return [SearchResult(url="https://example.com/a", title="t", snippet="s")]

        def fetch(self, url):
            raise NotImplementedError

    logged: list = []
    store = type("S", (), {"log_search": lambda self, e: logged.append(e)})()
    ad = _Ad()
    eff = SearchEffort(SearchBudget.from_mapping({"corpus": 1}))
    first = external_search(ad, SearchQuery(text="q"), store=store, run_id="r1",
                            effort=eff, budget=budget, product_id="p1")
    second = external_search(ad, SearchQuery(text="q"), store=store, run_id="r1",
                             effort=eff, budget=budget, product_id="p1")
    if not first or second or ad.calls != 1:
        raise AssertionError("исчерпание бюджета обязано вернуть [] без вызова адаптера (I23)")
    if logged[-1].termination_reason is not TerminationReason.BUDGET_EXHAUSTED:
        raise AssertionError("терминация при исчерпании бюджета обязана быть budget_exhausted")


def _discovery() -> None:
    """Discovery: выход — кандидаты/гипотезы, не evidence (I19); промоушен только G2."""
    import ast as _ast
    import inspect as _inspect

    import core.pipeline.discovery as _d
    from core.pipeline.discovery import discover, review_candidates
    from core.ports.search import SearchResult

    tree = _ast.parse(_inspect.getsource(_d))
    imports: set[str] = set()
    for node in _ast.walk(tree):
        if isinstance(node, _ast.ImportFrom) and node.module:
            imports.add(node.module)
        if isinstance(node, _ast.Import):
            imports.update(a.name for a in node.names)
    if {m for m in imports if m.startswith("core.store") or m == "core.domain.models"}:
        raise AssertionError("discovery обязан не импортировать store/Cell (I19)")

    class _Fake:
        adapter_kind = "serp"
        provider = "fake"

        def search(self, q):
            return [SearchResult(url="https://example.com/a", title="t", snippet="s")]

        def fetch(self, url):
            raise NotImplementedError

    report = discover(queries=["q"], adapter=_Fake(), limit=5, lang="ru")
    if len(report.candidates) != 1 or report.candidates[0].source_urls != ["https://example.com/a"]:
        raise AssertionError("кандидат должен нести source_urls, но не evidence")
    if report.candidates[0].status.value != "hypothesis":
        raise AssertionError("discover обязан выдавать гипотезы (кандидат — только через G2)")
    if any(c.status.value != "hypothesis" for c in review_candidates(report, decisions=None)):
        raise AssertionError("без решения G2 промоушена быть не должно")


def _adversarial_gate() -> None:
    """R8: 0 известных инъекций; gate() бросает при breach."""
    from pathlib import Path

    from core.pipeline.adversarial import (
        AdversarialBreachError,
        DefenseResult,
        load_adversarial,
        run_adversarial,
    )

    path = Path(__file__).parent / "pipeline" / "fixtures" / "adversarial_prompts.json"
    cases = load_adversarial(path)
    report = run_adversarial(cases)
    if report.breached != 0 or report.total == 0:
        raise AssertionError(f"R8: ожидалось 0 инъекций, получено {report.breached}/{report.total}")
    report.gate()

    class _Breach:
        def __call__(self, case, chunks, output):
            return DefenseResult(defended=False, stage="none", detail="synthetic")

    if run_adversarial(cases, validator=_Breach()).breached == 0:
        raise AssertionError("gate обязан обнаруживать синтетический breach")
    try:
        run_adversarial(cases, validator=_Breach()).gate()
        raise AssertionError("gate обязан бросить AdversarialBreachError")
    except AdversarialBreachError:
        pass


def _seam_schema() -> None:
    """Контракты шва: schema_version=1, конверт/payload, R1 source_url."""
    from core.adapters.remote_gateway import DEFAULT_SCHEMA_VERSION
    from core.edge.seam_schema import (
        ENDPOINT_OPS,
        SEAM_SCHEMA_VERSION,
        contract_digest,
        validate_envelope,
        validate_payload,
    )

    if not (SEAM_SCHEMA_VERSION == DEFAULT_SCHEMA_VERSION == "1"):
        raise AssertionError("schema_version шва обязан совпадать с клиентом (=1)")
    if validate_envelope({"schema_version": "1", "request_id": "x", "status": "ok",
                          "payload": {}, "error": None}):
        raise AssertionError("корректный конверт отвергнут")
    if not validate_envelope({"request_id": "x", "status": "ok", "payload": {}}):
        raise AssertionError("отсутствие schema_version обязано быть нарушением")
    if not validate_payload("registry", [{"fact_json": {}, "fetched_at": "t"}]):
        raise AssertionError("R1: registry без source_url обязан быть отвергнут")
    if {"fetch", "search", "registry", "health", "capabilities"} - set(ENDPOINT_OPS):
        raise AssertionError("не все /v1/* эндпоинты описаны контрактом")
    if contract_digest() != contract_digest():
        raise AssertionError("contract_digest обязан быть детерминирован")


def _edge_isolation() -> None:
    """Edge-сервер: зеркало клиента (envelope round-trip), изоляция от store/domain (I18/I19)."""
    import ast as _ast
    import inspect as _inspect

    import core.edge.server as _srv
    from core.adapters.remote_crawler import RemoteCrawler
    from core.adapters.remote_gateway import HttpContentGateway
    from core.edge.server import EdgeGateway
    from core.ports.types import ExtractedDoc, RawPage

    tree = _ast.parse(_inspect.getsource(_srv))
    imports: set[str] = set()
    for node in _ast.walk(tree):
        if isinstance(node, _ast.ImportFrom) and node.module:
            imports.add(node.module)
        if isinstance(node, _ast.Import):
            imports.update(a.name for a in node.names)
    if {m for m in imports if m.startswith("core.store") or m.startswith("core.domain")}:
        raise AssertionError("edge не имеет права импортировать store/domain (I18/I19)")

    class _Crawler:
        version = "c1"

        def fetch(self, url, plan=None, lang=None):
            return RawPage(url=url, content=b"<html>hi</html>", fetched_at="t")

    class _Extractor:
        version = "e1"

        def extract(self, page, route=None):
            return ExtractedDoc(text="hi", content_hash="h", extractor_version="e1")

    edge = EdgeGateway(crawler=_Crawler(), extractor=_Extractor(), registries={})
    client = HttpContentGateway(
        base_url="https://edge.invalid",
        transport=lambda op, env: edge.envelope(op, env.get("payload") or {},
                                                request_id=env.get("request_id")),
    )
    if RemoteCrawler(client).fetch("https://example.com").content != b"<html>hi</html>":
        raise AssertionError("client↔server round-trip шва сломан")


def _gateway_validate() -> None:
    """Валидация входящего: повторный content_hash отвергает подмену (I19, T13.5)."""
    import ast as _ast
    import base64 as _b64
    import inspect as _inspect

    import core.adapters.gateway_validate as _gv
    from core.adapters.gateway_validate import validate_raw_page
    from core.util.hashing import content_hash

    tree = _ast.parse(_inspect.getsource(_gv))
    imports: set[str] = set()
    for node in _ast.walk(tree):
        if isinstance(node, _ast.ImportFrom) and node.module:
            imports.add(node.module)
        if isinstance(node, _ast.Import):
            imports.update(a.name for a in node.names)
    if {m for m in imports if m.startswith("core.edge")}:
        raise AssertionError("01 §1: adapters не могут импортировать edge")

    raw = b"<html>hello</html>"
    declared = content_hash(raw)
    good = {"raw": {"bytes_b64": _b64.b64encode(raw).decode(), "hash": declared, "size": len(raw)}}
    if not validate_raw_page(good, url="https://example.com/a").ok:
        raise AssertionError("валидный raw-page обязан проходить")
    bad = {"raw": {"bytes_b64": _b64.b64encode(b"tampered").decode(), "hash": declared,
                   "size": len(raw)}}
    result = validate_raw_page(bad, url="https://example.com/a")
    if result.ok or not any(i.code == "hash" for i in result.issues):
        raise AssertionError("подмена байтов при заявленном хеше обязана быть отвергнута (I19)")


def _edge_auth_audit() -> None:
    """Шов: mTLS-скоупы, OIDC fail-closed (подделка), аудит-строка SIEM (07 §8)."""
    import json as _json
    import pathlib as _pl
    import tempfile as _tf

    from core.edge.audit import FileAuditSink, audit_call
    from core.edge.auth import MTLSAuthenticator, OIDCAuthenticator, authorize

    mtls = MTLSAuthenticator(subject_scopes={"core@svc": frozenset({"fetch", "search"})})
    decision = mtls.authenticate({"X-Client-Cert-Subject": "core@svc"})
    if not decision.allowed or decision.principal is None \
            or decision.principal.subject != "core@svc":
        raise AssertionError("mTLS-принципал не разрешён")
    if not authorize(decision.principal, "fetch") or authorize(decision.principal, "registry"):
        raise AssertionError("authorize: op→scope allowlist сломан")
    if mtls.authenticate({}).allowed:
        raise AssertionError("запрос без идентичности обязан быть отвергнут")

    oidc = OIDCAuthenticator(issuer="https://idp.invalid", audience="cohound")
    forged = ("eyJhbGciOiJub25lIn0."
              "eyJzdWIiOiJhZG1pbiIsImlzcyI6Imh0dHBzOi8vaWRwLmludmFsaWQiLCJhdWQiOiJjb2hvdW5kIn0.")
    if oidc.authenticate({"Authorization": f"Bearer {forged}"}).allowed:
        raise AssertionError("подделанный OIDC-токен не должен считаться проверенным")

    with _tf.TemporaryDirectory() as td:
        sink = FileAuditSink(_pl.Path(td) / "audit.jsonl", clock=lambda: "2026-01-01T00:00:00Z")
        audit_call(sink, request_id="req-1", principal=decision.principal, op="fetch",
                   status="allow")
        event = _json.loads((_pl.Path(td) / "audit.jsonl").read_text(encoding="utf-8").strip())
        for field in ("ts", "request_id", "principal", "op", "status", "classification"):
            if field not in event:
                raise AssertionError(f"поле SIEM-события отсутствует: {field}")
        if event["request_id"] != "req-1":
            raise AssertionError("событие обязано коррелироваться по request_id")


def _deploy_config() -> None:
    """deploy.yaml: отсутствие → безопасный local; split* требует edge.base_url."""
    import pathlib as _pl
    import tempfile as _tf

    from core.config.deploy import is_split, load_deploy_config

    default = load_deploy_config("/tmp/__cohound_missing_deploy__.yaml")
    if default.profile != "local" or is_split(default):
        raise AssertionError("отсутствие deploy.yaml → профиль local (07)")
    with _tf.TemporaryDirectory() as td:
        bad = _pl.Path(td) / "d.yaml"
        bad.write_text("profile: split\nedge: {}\n", encoding="utf-8")
        try:
            load_deploy_config(bad)
            raise AssertionError("split без edge.base_url обязан отказать")
        except ValueError:
            pass


def _crawl_sources_routing() -> None:
    """``crawl_sources.yaml``: послойный резолв ``kind``/``engine``/``params`` + стабильный digest.

    Lane A: слои ``defaults → products → sites`` (site побеждает по ``kind``/``params``,
    поздний ключ ``params`` перекрывает); источник-каталог ``feed`` даёт директорный ``kind``
    с ``params`` (``limit``/``since``); секреты — только по ссылке (``token_env``).
    """
    import pathlib as _pl
    import tempfile as _tf

    from core.config.crawl_sources import CrawlSourcesConfig, load_crawl_sources
    from core.pipeline.crawl_sources import crawl_sources_digest, resolve_crawl_target

    yaml_text = (
        "defaults: {kind: web, engine: http}\n"
        "products:\n"
        "  p1: {engine: scrapy}\n"
        "sites:\n"
        "  p1:\n"
        "    feed.example.com: {kind: feed, params: {limit: 50, since: '2026-01-01'}}\n"
        "    channel.example.com: {kind: channel, params: {token_env: CHANNEL_TOKEN}}\n"
    )
    with _tf.TemporaryDirectory() as td:
        path = _pl.Path(td) / "crawl_sources.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        cfg = load_crawl_sources(path)
        if not isinstance(cfg, CrawlSourcesConfig):
            raise AssertionError("load_crawl_sources обязан вернуть CrawlSourcesConfig")
        web = resolve_crawl_target("p1", "https://docs.example.com/x", sources=cfg, profiles={})
        if (web.kind, web.engine) != ("web", "scrapy"):
            raise AssertionError(f"defaults/site для web: ожидалось (web, scrapy), дано "
                                 f"({web.kind}, {web.engine})")
        feed = resolve_crawl_target(
            "p1", "https://feed.example.com/rss", sources=cfg, profiles={}
        )
        if feed.kind != "feed" or dict(feed.params) != {"limit": 50, "since": "2026-01-01"}:
            raise AssertionError(f"site-слой feed: kind/params неверны ({feed.kind}, {feed.params})")
        channel = resolve_crawl_target(
            "p1", "https://channel.example.com/c", sources=cfg, profiles={}
        )
        if channel.kind != "channel" or dict(channel.params) != {"token_env": "CHANNEL_TOKEN"}:
            raise AssertionError("секрет обязан передаваться ссылкой token_env, не инлайном")
        if crawl_sources_digest(cfg) != crawl_sources_digest(load_crawl_sources(path)):
            raise AssertionError("crawl_sources_digest обязан быть детерминирован")


def _pollers_seam() -> None:
    """``PollPort`` runtimе-checkable + реализуемы обоими поллерами; ``crawl/poll`` в шве.

    Проверяется регистрация операции в allowlist/scope (07 §8), схема шва (``schema_version``
    остаётся ``"1"``) и то, что адаптеры структурно удовлетворяют ``PollPort``.
    """
    from core.adapters.feed_poller import LocalFeedPoller, RemoteFeedPoller
    from core.edge import auth as _auth
    from core.edge.seam_schema import (
        CRAWL_OPS,
        ENDPOINT_OPS,
        POLL_KIND_VALUES,
        SEAM_SCHEMA_VERSION,
    )
    from core.ports import PollPort

    if not getattr(PollPort, "_is_runtime_protocol", False):
        raise AssertionError("PollPort обязан быть @runtime_checkable")
    if "crawl/poll" not in _auth.AUTHORIZED_OPS:
        raise AssertionError("crawl/poll обязан быть в AUTHORIZED_OPS (allowlist 07 §8)")
    if _auth._OP_SCOPE.get("crawl/poll") != _auth.SCOPE_CRAWL:
        raise AssertionError("crawl/poll обязан требовать scope crawl")
    if "crawl/poll" not in ENDPOINT_OPS or "crawl/poll" not in CRAWL_OPS:
        raise AssertionError("crawl/poll обязан быть заявлен в ENDPOINT_OPS/CRAWL_OPS")
    if SEAM_SCHEMA_VERSION != "1":
        raise AssertionError("schema_version шва обязан оставаться '1' (аддитивное расширение)")
    if set(POLL_KIND_VALUES) != {"feed", "channel", "sitemap"}:
        raise AssertionError(
            f"POLL_KIND_VALUES: ожидалось feed/channel/sitemap, дано {POLL_KIND_VALUES}"
        )

    class _Crawler:
        version = "c1"

        def fetch(self, url, plan=None):  # pragma: no cover - структура
            raise NotImplementedError

    class _Gateway:
        schema_version = "1"

        def call(self, op, payload):  # pragma: no cover - структура
            raise NotImplementedError

        def capabilities(self):  # pragma: no cover - структура
            return {}

    if not isinstance(LocalFeedPoller(_Crawler()), PollPort):
        raise AssertionError("LocalFeedPoller обязан удовлетворять PollPort")
    if not isinstance(RemoteFeedPoller(_Gateway()), PollPort):
        raise AssertionError("RemoteFeedPoller обязан удовлетворять PollPort")


def _directory_kinds_helpers() -> None:
    """Директорные ``kind`` корпуса и общие хелперы времени ``since`` (local/edge паритет).

    N1 (Wave 5.2): ``parse_instant``/``entry_not_before`` живут в ``core.util.feed`` и
    используются обоими поллерами; ``RFC822`` и ISO ``Z`` разбираются единообразно.
    """
    import datetime as _dt

    from core.pipeline.corpus import _DIRECTORY_KINDS
    from core.util.feed import FeedEntry, entry_not_before, parse_instant

    if _DIRECTORY_KINDS != frozenset({"feed", "channel"}):
        raise AssertionError(f"директорные kind неверны: {_DIRECTORY_KINDS}")
    threshold = parse_instant("2026-10-08T00:00:00Z")
    if threshold is None or threshold.tzinfo is None:
        raise AssertionError("parse_instant обязан вернуть aware datetime")
    # RFC822 10 Oct 2026 новее порога (лексикографически было бы меньше).
    newer = FeedEntry(url="https://x.test/a", published_at="10 Oct 2026 00:00:00 GMT")
    older = FeedEntry(url="https://x.test/b", published_at="01 Jan 2026 00:00:00 GMT")
    unknown = FeedEntry(url="https://x.test/c")
    if not entry_not_before(newer, threshold) or entry_not_before(older, threshold):
        raise AssertionError("фильтр since обязан разбирать дату, а не сравнивать строки")
    if not entry_not_before(unknown, threshold):
        raise AssertionError("неизвестная дата записи обязана сохраняться (консервативно)")
    if parse_instant("") is not None:
        raise AssertionError("пустая метка времени → None")


def _serp_loop() -> None:
    """SERP-петля: append-only сборки (новый id на раунд), K2-инвалидация, §13."""
    import ast as _ast
    import datetime as _dt
    import inspect as _inspect

    import core.pipeline.serp_loop as _sl
    from core.domain import (
        Cell,
        CellState,
        LLMEvidence,
        RankingPolicy,
        RunConfig,
        RunMode,
        StaleReason,
        Status,
    )
    from core.pipeline.search_log import SearchBudget, SearchEffort

    tree = _ast.parse(_inspect.getsource(_sl))
    imports: set[str] = set()
    for node in _ast.walk(tree):
        if isinstance(node, _ast.ImportFrom) and node.module:
            imports.add(node.module)
        if isinstance(node, _ast.Import):
            imports.update(a.name for a in node.names)
    if {m for m in imports if m.startswith("core.adapters")}:
        raise AssertionError("01 §1: pipeline не может импортировать adapters")

    from core.pipeline.serp_loop import (
        ImmutableBuildError,
        derive_round_build_id,
        invalidate_affected,
        run_serp_round,
    )
    from core.ports.search import SearchQuery, SearchResult
    from core.ports.types import RawPage
    from core.store.sqlite_store import SqliteStore

    class _Fake:
        adapter_kind = "serp"
        provider = "fake"

        def search(self, q):
            return [SearchResult(url="https://example.com/a", title="t", rank=1)]

        def fetch(self, url):
            raise NotImplementedError

    run_id = "run-selfcheck-serp"
    base = "ib_base"
    with SqliteStore(":memory:") as store:
        store.create_run(run_id, RunConfig(
            case_id="c", anchor="p1", cohort="cohort.yaml", products=["p1", "p2"],
            package="pkg", snapshot_date=_dt.date(2026, 1, 1), mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ))
        store.save_cells_batch([Cell(
            run_id=run_id, product_id="p1", param_id="x", state=CellState.EVALUATED,
            status=Status.OK, evidence=[LLMEvidence(source_id="p1:0", quote="q")],
            index_build_id=base,
        )])
        budget = SearchBudget.from_mapping({"serp": 5})
        queries = {"p1": [SearchQuery(text="q", lang="ru", phase="serp")]}
        rnd = run_serp_round(
            adapter=_Fake(), queries_by_product=queries, store=store, run_id=run_id,
            effort=SearchEffort(budget), budget=budget, round_no=1, parent_build_id=base,
            package="pkg", clock=lambda: "2026-01-01T00:00:00Z",
        )
        if rnd.build.index_build_id == base or not rnd.build.index_build_id.startswith("cb_"):
            raise AssertionError("раунд обязан породить новую композицию (K2)")
        if not all(
            (entry.partition_build_id or "").startswith("pb_")
            for entry in rnd.build.entries
        ):
            raise AssertionError("раунд обязан породить per-product pb_-партиции")
        invalidated = invalidate_affected(store, run_id, base, rnd.build.index_build_id)
        if not invalidated or any(c.stale_reason is not StaleReason.NEW_BUILD for c in invalidated):
            raise AssertionError("K2: затронутые ячейки обязаны помечаться stale_reason=new_build")
        row = store.conn.execute(
            "SELECT stale_reason FROM cells WHERE run_id=? AND product_id=? AND param_id=?",
            (run_id, "p1", "x"),
        ).fetchone()
        if row[0] != StaleReason.NEW_BUILD.value:
            raise AssertionError(f"K2: ячейка старой сборки обязана стать stale, получено {row[0]!r}")

    r1 = derive_round_build_id(None, 1, "pkg", "tok")
    r2 = derive_round_build_id(r1, 2, "pkg", "tok")
    if r1 == r2:
        raise AssertionError("append-only: id раундов обязаны различаться")
    try:
        from core.pipeline.serp_loop import assert_build_id_new
        assert_build_id_new(None, r1, existing_build_ids={r1})
        raise AssertionError("переиспользование сборки обязано быть запрещено")
    except ImmutableBuildError:
        pass


def _build_phases_gate() -> None:
    """Интеграция: ``build_phases`` отдаёт 11 не-гейтовых фаз и НЕ переопределяет гейты."""
    from core.pipeline.phases import IMPLEMENTED_PHASES, build_phases
    from core.pipeline.runner import GATE_PHASES, PHASES, Ports

    expected = set(PHASES) - set(GATE_PHASES)
    if set(IMPLEMENTED_PHASES) != expected:
        raise AssertionError(
            f"IMPLEMENTED_PHASES != не-гейтовые фазы: {set(IMPLEMENTED_PHASES) ^ expected}"
        )
    from core.pipeline.normalize import load_inputs
    from core.pipeline.plan import plan

    inputs = load_inputs(
        str(_here(tests_root=True) / "fixtures" / "e2e" / "run.yaml"),
        str(_here(tests_root=True) / "fixtures" / "e2e"),
    )
    run_plan = plan(inputs, run_id="selfcheck-plan")
    phases = build_phases(inputs=inputs, plan=run_plan, ports=Ports())
    if set(phases) != expected:
        raise AssertionError(f"build_phases вернул не тот набор фаз: {set(phases) ^ expected}")
    if set(phases) & set(GATE_PHASES):
        raise AssertionError("build_phases обязан НЕ переопределять гейты (S9)")


def _walking_skeleton() -> None:
    """Интеграция: реальные фазы зарегистрированы; без решения G1 прогон стоит blocked."""
    from core.cli.wiring import build_store
    from core.domain import RunMode
    from core.pipeline.normalize import load_inputs
    from core.pipeline.phases import build_phases
    from core.pipeline.plan import plan
    from core.pipeline.runner import Ports, Runner

    fixture_dir = _here(tests_root=True) / "fixtures" / "e2e"
    inputs = load_inputs(str(fixture_dir / "run.yaml"), str(fixture_dir))
    run_plan = plan(inputs, run_id="selfcheck-skeleton")

    store = build_store(":memory:")
    try:
        if hasattr(store, "create_run"):
            store.create_run(run_plan.run_id, inputs.run)
        phases = build_phases(inputs=inputs, plan=run_plan, ports=Ports())
        runner = Runner(store, ports=Ports(), phases=phases)
        report = runner.run(run_plan.run_id, until="g2")
        # Без строки решения G1 прогон обязан остановиться на гейте (I6/I7, S9).
        if report.stopped_at != "g1":
            raise AssertionError(f"без решения G1 ожидалась остановка на g1, получено {report.stopped_at!r}")
        if runner.state_of(run_plan.run_id, "g2") is not None:
            raise AssertionError("g2 не должен выполняться, пока g1 не пройден")
        if inputs.run.mode is not RunMode.STRICT:
            raise AssertionError("неожиданный режим e2e-фикстуры")
    finally:
        store.close()


def _phase_vocabulary() -> None:
    """P2a: словарь фаз — ``skipped`` не стопит прогон; незарегистрированная фаза → ``failed``.

    Проверяет: (1) фаза со ``skipped`` не останавливает прогон и последующие фазы исполняются;
    (2) дурабельное состояние ``skipped`` персистится; (3) незарегистрированная фаза даёт
    ``failed``, а не подделку ``ok``; (4) ``skip_reason`` синхронен словарю §4.1;
    (5) инвариант ``PhaseResult``: ``skip_reason`` задан ⟺ ``status == "skipped"``.
    """
    import datetime as _dt

    from core.domain import RunConfig, RunMode, SkipReason, load_dictionary
    from core.pipeline.runner import GATE_PHASES, PHASES, PhaseResult, Runner
    from core.store.sqlite_store import SqliteStore

    # (4) closed vocabulary синхронна словарю.
    data = load_dictionary(ROOT / "docs" / "status-dictionary.yaml")
    if set(data["skip_reason"]) != {r.value for r in SkipReason}:
        raise AssertionError("skip_reason рассинхронизирован со схемой")

    cfg = RunConfig(
        case_id="c",
        anchor="p1",
        cohort="co1",
        products=["p1", "p2"],
        package="pk",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )

    # (1)+(2) ``skipped`` не останавливает прогон.
    with SqliteStore(":memory:") as store:
        store.create_run("rv", cfg)
        calls: list[str] = []

        def ok(phase: str):
            def _fn(ctx):
                calls.append(phase)
                return PhaseResult(status="ok", phase=phase)

            return _fn

        def skipped(phase: str):
            def _fn(ctx):
                calls.append(phase)
                return PhaseResult(
                    status="skipped", phase=phase, skip_reason=SkipReason.NO_SEARCH_ADAPTER
                )

            return _fn

        phases = {name: ok(name) for name in PHASES if name not in GATE_PHASES}
        phases["discovery"] = skipped("discovery")
        for gate in GATE_PHASES:
            phases[gate] = ok(gate)
        report = Runner(store, phases=phases).run("rv")
        if report.stopped_at is not None or report.status != "ok":
            raise AssertionError("skipped обязан НЕ останавливать прогон")
        if calls != list(PHASES):
            raise AssertionError("после skipped последующие фазы обязаны исполниться")
        if store.get_phase_state("rv").get("discovery") != "skipped":
            raise AssertionError("durable-состояние skipped обязано персиститься")

    # (3) Незарегистрированная фаза → failed (реализация отсутствует), не фиктивное ok.
    with SqliteStore(":memory:") as store:
        store.create_run("rv2", cfg)
        rep = Runner(store).run("rv2", until="normalize")
        if rep.status != "failed" or rep.stopped_at != "normalize":
            raise AssertionError("незарегистрированная фаза обязана дать failed")
        if not any("не зарегистрирована" in err for err in rep.results[0].errors):
            raise AssertionError("failed незарегистрированной фазы обязан нести явную причину")

    # (5) Инвариант в коде: skip_reason задан ⟺ status == "skipped".
    try:
        PhaseResult(status="skipped", phase="discovery")
    except ValueError:
        pass
    else:
        raise AssertionError("skipped без skip_reason обязан падать (инвариант PhaseResult)")
    try:
        PhaseResult(status="ok", phase="review", skip_reason=SkipReason.NO_QUERIES)
    except ValueError:
        pass
    else:
        raise AssertionError("skip_reason при ok обязан падать (инвариант PhaseResult)")


def _here(*, tests_root: bool = False):
    """Каталог selfcheck (``tests/``) либо репозитория (родитель ``tests/``)."""
    import pathlib as _pl

    root = _pl.Path(__file__).resolve().parent
    return root if tests_root else root.parent


def _passports() -> None:
    """Паспорта: выборка latest-by-ISO-date; URL-кандидаты из ``doc_domains`` (I19)."""
    from core.config.passports import (
        PassportLoadError,
        load_passport,
        load_passports,
        passport_urls,
    )

    if load_passports("/tmp/__cohound_no_passports__") != {}:
        raise AssertionError("отсутствие каталога паспортов → пустой словарь (безопасный дефолт)")
    if load_passport("p1", "/tmp/__cohound_no_passports__") is not None:
        raise AssertionError("отсутствующий паспорт → None")

    fixture_dir = _here(tests_root=True) / "config" / "fixtures" / "passports"
    passport = load_passport("p1", fixture_dir)
    if passport is None or passport.product_id != "p1":
        raise AssertionError("паспорт фикстуры не загрузился")
    urls = passport_urls(passport)
    if not urls or not all(u.startswith("https://") for u in urls):
        raise AssertionError(f"doc_domains → https-кандидаты, получено {urls!r}")

    import pathlib as _pl
    import tempfile as _tf

    with _tf.TemporaryDirectory() as td:
        product_dir = _pl.Path(td) / "p9"
        product_dir.mkdir()
        for date, domain in (("2025-01-01", "old.example"), ("2026-01-01", "new.example")):
            (product_dir / f"{date}.yaml").write_text(
                "product_id: p9\ncanonical_name: Nine\nvendor: V\njurisdiction: RU\n"
                f"doc_domains: [{domain}]\nsnapshot_date: {date}\n",
                encoding="utf-8",
            )
        (product_dir / "notes.yaml").write_text("junk", encoding="utf-8")
        loaded = load_passports(td)
        if loaded.get("p9") is None or loaded["p9"].doc_domains != ["new.example"]:
            raise AssertionError("latest-by-ISO-date не выбрал новейший паспорт")
        # битый YAML → PassportLoadError
        bad_dir = _pl.Path(td) / "p8"
        bad_dir.mkdir()
        (bad_dir / "2026-01-01.yaml").write_text("product_id: [", encoding="utf-8")
        try:
            load_passport("p8", td)
            raise AssertionError("битый YAML обязан поднять PassportLoadError")
        except PassportLoadError:
            pass


def _data_templates() -> None:
    """Шаблоны данных (T6.5): пакет/паспорт/реестр валидны, линт без блоков и warnings.

    Шаблоны в ``packages/_template``, ``passports/_template`` и
    ``docs/templates/registries/example_template.yaml`` — нейтральные скелеты: они обязаны
    проходить тот же контур валидации, что и реальные данные (``load_inputs`` → ``lint`` →
    ``cross_check``, ``load_passport``, ``load_registry_config``), но без выдуманных
    значений. Нарушение любого условия — понятный ``AssertionError``.
    """
    from core.adapters.registry_generic import load_registry_config
    from core.config.passports import load_passport
    from core.pipeline.lint import lint_run_inputs
    from core.pipeline.normalize import cross_check, load_inputs

    root = _here()
    package_dir = root / "packages" / "_template"

    inputs = load_inputs(package_dir / "run.yaml", package_dir)
    report = lint_run_inputs(inputs)
    if report.blocking:
        raise AssertionError(
            "шаблон пакета: блокирующие нарушения линтера: " + "; ".join(report.blocking)
        )
    if report.warnings:
        raise AssertionError(
            "шаблон пакета: неожиданные warnings линтера: " + "; ".join(report.warnings)
        )
    violations = cross_check(inputs)
    if violations:
        raise AssertionError(
            "шаблон пакета: кросс-артефактные нарушения: " + "; ".join(violations)
        )

    passport = load_passport("example_product", root / "passports" / "_template")
    if passport is None or passport.product_id != "example_product":
        raise AssertionError("шаблон паспорта example_product не загрузился (None или чужой id)")
    if not passport.doc_domains or not all(".invalid" in d for d in passport.doc_domains):
        raise AssertionError("шаблон паспорта: doc_domains обязаны быть на TLD .invalid")

    registry = load_registry_config(
        root / "docs" / "templates" / "registries" / "example_template.yaml"
    )
    if registry.registry_id != "example_template":
        raise AssertionError("шаблон реестра example_template не загрузился")


def _registry_package_scoped() -> None:
    """Реестровые спецификации — package-scoped (P3, 03 §D.4).

    Проверяет нейтральный загрузчик ``load_package_registry_configs``: спецификация
    читается из ``packages/{case_id}/registries/{registry_id}.yaml`` (а не из корневого
    ``registries/``), ``registry_id`` из манифеста проверяется path-конфайнментом, а
    ``headers_env`` не хранит секрет в файле — значение берётся из окружения в момент
    запроса (не задано → заголовок опускается).
    """
    import tempfile

    from core.adapters.registry_generic import (
        RegistryConfigError,
        load_package_registry_configs,
        resolve_registry_headers,
    )

    spec = (
        "registry_id: reg_a\n"
        "jurisdiction: XX\n"
        "endpoint: 'https://example.invalid/api/search/{query}'\n"
        "format: json\n"
        "records_path: items\n"
        "headers:\n"
        "  Accept: application/json\n"
        "headers_env:\n"
        "  Authorization: COHOUND_TEST_REGISTRY_TOKEN\n"
        "mapping:\n"
        "  name: name\n"
    )
    with tempfile.TemporaryDirectory() as td:
        pkg = Path(td) / "case_a"
        (pkg / "registries").mkdir(parents=True)
        (pkg / "registries" / "reg_a.yaml").write_text(spec, encoding="utf-8")

        configs = load_package_registry_configs(pkg, ["reg_a"])
        if set(configs) != {"reg_a"}:
            raise AssertionError("package-scoped спецификация не загрузилась")
        cfg = configs["reg_a"]

        # Значение секрета берётся из окружения, а не из файла.
        resolved = resolve_registry_headers(cfg, env={"COHOUND_TEST_REGISTRY_TOKEN": "SEKRET"})
        if resolved.get("Authorization") != "SEKRET" or resolved.get("Accept") != "application/json":
            raise AssertionError("headers_env не подставил секрет из окружения")
        # Переменная не задана → секретный заголовок опускается (не выдумывается).
        empty = resolve_registry_headers(cfg, env={})
        if "Authorization" in empty:
            raise AssertionError("незаданная env-переменная не должна давать заголовок")
        if "SEKRET" in (pkg / "registries" / "reg_a.yaml").read_text(encoding="utf-8"):
            raise AssertionError("секрет не должен храниться в спецификации")

        # Некорректный registry_id (path-конфайнмент) отвергается.
        try:
            load_package_registry_configs(pkg, ["../evil"])
            raise AssertionError("некорректный registry_id обязан поднять RegistryConfigError")
        except RegistryConfigError:
            pass
        # Объявленный, но отсутствующий реестр — понятная ошибка.
        try:
            load_package_registry_configs(pkg, ["reg_missing"])
            raise AssertionError("отсутствующая спецификация обязана поднять RegistryConfigError")
        except RegistryConfigError:
            pass


def _golden_harness() -> None:
    """Golden-harness: R6-согласие, false-✅ фейлит гейт, страта ❌ присутствует (R12)."""
    from core.config.run_config import load_golden
    from core.domain import Status
    from core.pipeline.golden import check_thresholds, run_golden

    fixture = _here(tests_root=True) / "pipeline" / "fixtures" / "golden_min.csv"
    golden = load_golden(fixture)
    if not golden:
        raise AssertionError("golden-фикстура пуста")

    actual = {(g.product_id, g.param_id): g.expected_status for g in golden}
    perfect = run_golden(golden, actual)
    if perfect.agreement != 1.0 or not perfect.passes():
        raise AssertionError(f"полное согласие обязано проходить: agreement={perfect.agreement}")
    if not perfect.has_absent_stratum:
        raise AssertionError("R12: golden обязан содержать страту ожидаемых ❌")

    # false ✅ (там, где ожидался не-✅ статус) обязан завалить гейт
    target = next((g for g in golden if g.expected_status is not Status.OK), None)
    if target is None:
        raise AssertionError("в фикстуре нужен кейс с ожиданием ≠ ✅")
    forced = dict(actual)
    forced[(target.product_id, target.param_id)] = Status.OK
    report = run_golden(golden, forced)
    if report.false_ok != 1 or report.passes():
        raise AssertionError("false ✅ обязан фейлить golden-гейт (critical=0)")
    if not check_thresholds(report):
        raise AssertionError("check_thresholds обязан вернуть нарушение при false ✅")


def _review_human_only() -> None:
    """Ревью: очередь preliminary/❓; promote только человеком и без фабрикации статуса (I6/I7)."""
    import datetime as _dt

    from core.domain import (
        Cell,
        CellState,
        RankingPolicy,
        RunConfig,
        RunMode,
        Status,
        check_unknown_exhaustion,
    )
    from core.pipeline.review import ReviewError, promote, review_queue
    from core.store.sqlite_store import SqliteStore

    with SqliteStore(":memory:") as store:
        store.create_run("r1", RunConfig(
            case_id="c", anchor="p1", cohort="co.yaml", products=["p1"], package="pkg",
            snapshot_date=_dt.date(2026, 1, 1), mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ))
        store.save_cells_batch([
            Cell(run_id="r1", product_id="p1", param_id="x", state=CellState.PRELIMINARY,
                 status=None, evidence=[], absence_evidence=[], reviewed_by=None),
            Cell(run_id="r1", product_id="p1", param_id="y", state=CellState.EVALUATED,
                 status=Status.UNKNOWN, evidence=[], absence_evidence=[], reviewed_by=None),
        ])
        if {i.param_id for i in review_queue(store, "r1")} != {"x", "y"}:
            raise AssertionError("очередь ревью обязана включать preliminary и ❓")

        try:
            promote(store=store, run_id="r1", product_id="p1", param_id="x", actor="",
                    status=Status.UNKNOWN)
            raise AssertionError("I6/I7: promote без actor обязан быть отвергнут")
        except ReviewError:
            pass
        try:
            promote(store=store, run_id="r1", product_id="p1", param_id="x", actor="human-1")
            raise AssertionError("A3: promote без verdict обязан быть отвергнут (не фабрикуем статус)")
        except ReviewError:
            pass

        # I3-OR (§2.3): reviewed_by ≠ null удовлетворяет I3 — promote ❓ без протокола проходит.
        cell = promote(store=store, run_id="r1", product_id="p1", param_id="x",
                       actor="human-1", status=Status.UNKNOWN)
        if cell.reviewed_by != "human-1" or cell.state is not CellState.EVALUATED:
            raise AssertionError("promote обязан выставить reviewed_by и state=evaluated")

        # Машинный ❓ (без reviewed_by) без протокола исчерпания — нарушение I3
        # (фиксируется как аномалия в report, promote не блокирует).
        machine = Cell(run_id="r1", product_id="p1", param_id="z", state=CellState.EVALUATED,
                       status=Status.UNKNOWN, evidence=[], absence_evidence=[], reviewed_by=None)
        if not check_unknown_exhaustion(machine, None):
            raise AssertionError("I3: машинный ❓ без протокола обязан нарушать I3")


def _cli_surface() -> None:
    """CLI-поверхность (03 §D.6): присутствуют все команды; search-адаптеры зарегистрированы."""
    import importlib as _il

    from core.ports.search import SearchAdapterPort

    main = _il.import_module("core.cli.main")
    wiring = _il.import_module("core.cli.wiring")
    parser = main.build_parser()
    subparsers = next(
        action for action in parser._actions if hasattr(action, "choices") and isinstance(action.choices, dict)
    )
    commands = set(subparsers.choices)
    required = {
        "resolve", "plan", "run", "discover", "review", "promote",
        "report", "golden", "sensitivity", "diff", "deploy-check", "doctor",
        "baseline-agreement",
    }
    missing = required - commands
    if missing:
        raise AssertionError(f"CLI-команды отсутствуют: {sorted(missing)}")
    if not getattr(wiring, "SUPPORTED_SEARCH_ADAPTERS", ()):
        raise AssertionError("SUPPORTED_SEARCH_ADAPTERS пуст")
    if "yandex" not in wiring.SUPPORTED_SEARCH_ADAPTERS:
        raise AssertionError("адаптер yandex не зарегистрирован")
    if "glm" not in wiring.SUPPORTED_SEARCH_ADAPTERS:
        raise AssertionError("адаптер glm не зарегистрирован")
    for name in wiring.SUPPORTED_SEARCH_ADAPTERS:
        adapter = wiring.build_search_adapter(name)
        if not isinstance(adapter, SearchAdapterPort):
            raise AssertionError(f"build_search_adapter({name!r}) не SearchAdapterPort")


def _cli_run_self_contained() -> None:
    """Самодостаточность прогона (03 §A.2 / P1): входы плана персистятся в БД (``run_inputs``).

    Проверяется контракт границы читатель/формат без полной сборки ``RunInputs``:
    round-trip снимка (write→read) даёт те же значения; ``_restore_plan_inputs``
    восстанавливает каталог/веса/когорту из БД прогона без side-файла; чужой
    ``run_id``, несовместимая версия и отсутствующая запись — значения не подставляются.
    """
    import argparse as _argparse
    import tempfile as _tempfile
    from pathlib import Path as _Path

    import importlib as _il

    main = _il.import_module("core.cli.main")
    for const in (
        "RUN_YAML_FILENAME",
        "PLAN_INPUTS_FILENAME",
        "PLAN_INPUTS_SNAPSHOT_VERSION",
    ):
        if not hasattr(main, const):
            raise AssertionError(f"core.cli.main.{const} отсутствует (03 §A.2)")

    from core.domain.models import CatalogRow, Cohort
    from core.store.sqlite_store import SqliteStore

    row = CatalogRow.model_validate(
        {
            "param_id": "p_selfcheck",
            "title": "T",
            "module": "m",
            "group": "g",
            "importance": "Баз",
            "param_type": "fact",
            "rule": "r",
        }
    )
    cohort = Cohort.model_validate(
        {
            "case_id": "case_selfcheck",
            "frozen_at": "2026-01-01",
            "entries": [
                {
                    "product_id": "p1",
                    "role_overlap": 0.5,
                    "selection_reason": "r",
                    "anchor_relation": "anchor",
                }
            ],
        }
    )
    payload = {
        "snapshot_version": main.PLAN_INPUTS_SNAPSHOT_VERSION,
        "run_id": "run_selfcheck",
        "params": [row.model_dump(mode="json")],
        "module_weights": {"m": 1.0},
        "renormalized_weights": {"m": 0.5},
        "cohort": cohort.model_dump(mode="json"),
    }
    args = _argparse.Namespace(run_yaml=None, package=None)

    with _tempfile.TemporaryDirectory() as td:
        store = SqliteStore(str(_Path(td) / "cohound.db"))
        try:
            sha = main._plan_inputs_sha256(payload)
            store.save_plan_inputs("run_selfcheck", payload=payload, payload_sha256=sha)
            # Round-trip: payload из БД семантически равен записанному.
            stored = store.read_plan_inputs("run_selfcheck")
            if stored != payload:
                raise AssertionError("снимок из БД не совпал с записанным")
            # Детерминизм канонизации: повторная запись даёт тот же sha256.
            store.save_plan_inputs("run_selfcheck", payload=payload, payload_sha256=sha)
            rowsha = store.conn.execute(
                "SELECT payload_sha256 FROM run_inputs WHERE run_id = ?",
                ("run_selfcheck",),
            ).fetchone()["payload_sha256"]
            if rowsha != sha:
                raise AssertionError("sha256 снимка нестабилен между записями")
            # _restore_plan_inputs читает из БД (side-файла нет вовсе).
            catalog, weights, restored, notes = main._restore_plan_inputs(
                args, "run_selfcheck", purpose="selfcheck", store=store
            )
            if set(catalog) != {row.param_id}:
                raise AssertionError("каталог из БД восстановлен неверно")
            if weights != {"m": 0.5}:
                raise AssertionError(f"веса из БД неверны: {weights!r}")
            if restored.case_id != cohort.case_id:
                raise AssertionError("когорта из БД восстановлена неверно")
            if any("отсутствуют в БД прогона" in note for note in notes):
                raise AssertionError("валидный снимок не должен давать заметку-ограничение")
            # Чужой run_id — не подставляем (ни на уровне БД, ни на уровне валидатора).
            if store.read_plan_inputs("other_run") is not None:
                raise AssertionError("чужой run_id должен давать None")
            if main._plan_inputs_from_payload(payload, "other_run") is not None:
                raise AssertionError("чужой run_id должен давать None")
            # Несовместимая версия снимка — не подставляем.
            if main._plan_inputs_from_payload(
                {**payload, "snapshot_version": 999}, "run_selfcheck"
            ) is not None:
                raise AssertionError("несовместимая версия снимка должна давать None")
            # Отсутствующая запись — None.
            if store.read_plan_inputs("absent") is not None:
                raise AssertionError("отсутствующая запись должна давать None")
        finally:
            store.close()


def _lexical_profiles() -> None:
    """Лексический индекс: двойной профиль (stemmed + raw/ngram) и конформность порту."""
    from core.adapters.lexical_tantivy import _PROFILES, TantivyLexicalIndex
    from core.ports.lexical_index import LexicalIndexPort

    for profile in ("stemmed", "identifier", "raw", "ngram"):
        if profile not in _PROFILES:
            raise AssertionError(f"профиль лексического индекса отсутствует: {profile}")

    default = TantivyLexicalIndex(index_path=None)
    if default.profile != "stemmed":
        raise AssertionError("профиль по умолчанию обязан остаться stemmed")
    if not isinstance(default, LexicalIndexPort):
        raise AssertionError("TantivyLexicalIndex обязан реализовывать LexicalIndexPort")
    ngram = TantivyLexicalIndex(index_path=None, profile="ngram")
    if ngram.profile != "ngram":
        raise AssertionError("ngram-профиль не принят конструктором")
    try:
        TantivyLexicalIndex(index_path=None, profile="nonsense")
        raise AssertionError("неизвестный профиль обязан быть отвергнут")
    except ValueError:
        pass


def _crawl_control_axes() -> None:
    """D1/D2: оси контроля краула — precedence, scope, блэклист, честность усечения.

    Инварианты: цепочка ``app < run < site`` для новых осей (отдельно от source-routing);
    ``max_depth`` дефолт 0; ``max_documents=0`` отвергается; scope по seed-root (host +
    граница пути); блэклисты glob (путь/basename); ``RobotsGate`` — единственная реализация
    (``core.util.robots``, ре-экспорт из crawl-probe); извлечение ссылок не выходит за
    ``http(s)``.
    """
    from core.config.deploy import CrawlConfig
    from core.config.domain_profiles import DomainProfile
    from core.domain.models import CrawlRunConfig
    from core.pipeline.crawl_frontier import (
        Frontier,
        is_blacklisted,
        resolve_crawl_limits,
    )
    from core.pipeline.crawl_probe import RobotsGate as ProbeRobotsGate
    from core.util.html_links import extract_links
    from core.util.robots import RobotsGate
    from pydantic import ValidationError

    app = CrawlConfig()
    assert resolve_crawl_limits("https://h/a", app=app).max_depth == 0
    run = CrawlRunConfig(max_depth=2, max_documents=5)
    limits = resolve_crawl_limits("https://h/a", app=app, run=run)
    assert limits.max_depth == 2 and limits.max_documents == 5
    site = resolve_crawl_limits(
        "https://h/a", app=app, run=run, profiles={"h": DomainProfile(max_depth=4)}
    )
    assert site.max_depth == 4  # site побеждает run
    _assert(RobotsGate is ProbeRobotsGate)
    try:
        DomainProfile(bogus=1)  # type: ignore[call-arg]
        raise AssertionError("DomainProfile обязан отвергать неизвестные поля")
    except ValidationError:
        pass
    try:
        CrawlConfig(max_documents=0)
        raise AssertionError("max_documents=0 обязан отвергаться")
    except ValidationError:
        pass
    frontier = Frontier("https://h/documents", limits)
    assert frontier.in_scope("https://h/documents/x")
    assert not frontier.in_scope("https://h/documents-archive")
    assert not frontier.in_scope("mailto:x@y")
    bl = resolve_crawl_limits(
        "https://h/", app=app, run=CrawlRunConfig(exclude_documents=["*.pdf"])
    )
    assert is_blacklisted("https://h/a.pdf", bl) and not is_blacklisted("https://h/a.PDF", bl)
    assert extract_links(
        b'<a href="/p#f">x</a><a href="mailto:a@b">m</a>', "https://H:443/dir/"
    ) == ["https://h/p"]


def _crawl_probe() -> None:
    """Зонд осей краула: предикат пригодности, лестница ``decide``, шапка, зеркало словаря.

    Инварианты: ``status_code=None`` — не сбой; ``401/403`` эскалируются, но не трогают
    ``rate_limit``; оба тира провалились ⇒ ``unresolved`` (не ``browser``); сбой транспорта
    (``both_tiers_failed``) отличается от «транспорт дошёл, экстракция дала 0»
    (``extraction_empty``); ``strategy`` пишется только для ``browser``; правка файла
    сохраняет шапку байт-в-байт и неизмеренные поля. Перечисления зонда зеркалируются
    ``docs/status-dictionary.yaml`` (источник истины).
    """
    from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
    from core.pipeline.crawl_probe import (
        ProbeConfidence,
        ProbeKeyStatus,
        ProbeReason,
        ProbeSource,
        ProbeTier,
        Observation,
        UrlProbe,
        decide,
        is_usable,
        render_profiles_with_verdicts,
        split_header,
    )
    from core.domain.dictionary import load_dictionary

    def obs(**kw):
        base = dict(
            tier=ProbeTier.HTTP,
            url="https://example.test/",
            source=ProbeSource.URLS,
            status_code=200,
            content_type="text/html",
            content_bytes=10,
            text_chars=500,
        )
        base.update(kw)
        return Observation(**base)

    # Предикат: None-статус — неизвестно (не сбой); короткий текст — непригодно.
    if not is_usable(obs(status_code=None)):
        raise AssertionError("status_code=None обязан быть пригодным (неизвестно ≠ сбой)")
    if is_usable(obs(status_code=401)):
        raise AssertionError("401 обязан быть непригодным (антибот)")
    if is_usable(obs(text_chars=10)):
        raise AssertionError("короткий извлечённый текст обязан быть непригодным")

    # Лестница: http пригоден ⇒ fetch=http (без strategy).
    v = decide("k", [UrlProbe("https://example.test/", ProbeSource.URLS, (obs(),))])
    if v.fetch is not FetchMode.HTTP or v.strategy is not None or not v.resolved:
        raise AssertionError("http-вердикт некорректен (strategy на http писать нельзя)")
    if v.extract is not None:
        raise AssertionError(
            "extract без документа обязан быть null (не авто): пишем только document"
        )

    # Документное свидетельство ⇒ extract=document (единственная ось, которую пишет --write).
    doc_url = "https://example.test/report.pdf"
    doc = decide(
        "k",
        [
            UrlProbe(
                doc_url,
                ProbeSource.URLS,
                (obs(url=doc_url, content_type="application/pdf"),),
            )
        ],
    )
    if doc.extract is not ExtractionRoute.DOCUMENT:
        raise AssertionError("документное свидетельство обязано дать extract=document")

    # hybrid с fetch_mode=HTTP ⇒ strategy=hybrid; fetch_mode=BROWSER ⇒ strategy=honest.
    hybrid = decide(
        "k",
        [
            UrlProbe(
                "https://example.test/",
                ProbeSource.URLS,
                (
                    obs(status_code=403),
                    obs(tier=ProbeTier.HYBRID, status_code=200, fetch_mode=FetchMode.HTTP),
                ),
            )
        ],
    )
    if hybrid.strategy is not CrawlStrategy.HYBRID:
        raise AssertionError("мост hybrid (fetch_mode=HTTP) обязан дать strategy=hybrid")
    honest = decide(
        "k",
        [
            UrlProbe(
                "https://example.test/",
                ProbeSource.URLS,
                (
                    obs(status_code=403),
                    obs(tier=ProbeTier.HONEST, fetch_mode=FetchMode.BROWSER),
                ),
            )
        ],
    )
    if honest.strategy is not CrawlStrategy.HONEST:
        raise AssertionError("рендер (fetch_mode=BROWSER) обязан дать strategy=honest")

    # Оба тира провалились ⇒ unresolved (не browser) с причиной.
    failed = decide(
        "k",
        [
            UrlProbe(
                "https://example.test/",
                ProbeSource.URLS,
                (
                    obs(raised=True, error="http: transient", status_code=None, text_chars=0),
                    obs(
                        tier=ProbeTier.HONEST,
                        raised=True,
                        error="browser: fail",
                        status_code=None,
                        text_chars=0,
                    ),
                ),
            )
        ],
    )
    if failed.resolved or failed.fetch is not None:
        raise AssertionError("оба провалившихся тира обязаны дать unresolved, не browser")
    if ProbeReason.BOTH_TIERS_FAILED.value not in failed.reasons:
        raise AssertionError("причина both_tiers_failed обязана быть записана")
    if len(failed.reasons) != 1:
        raise AssertionError(
            "чистый транспортный сбой обязан давать ровно одну причину (без extraction_empty)"
        )

    # Транспорт дошёл до контента, экстракция дала 0 символов ⇒ extraction_empty: это сбой
    # экстрактора, а не краулера (лживый both_tiers_failed недопустим), ось fetch не гадаем.
    empty = decide(
        "k",
        [
            UrlProbe(
                "https://example.test/",
                ProbeSource.URLS,
                (
                    obs(text_chars=0, content_bytes=1840439, error="extract: no text"),
                    obs(
                        tier=ProbeTier.HONEST,
                        text_chars=0,
                        content_bytes=260,
                        error="extract: no text",
                    ),
                ),
            )
        ],
    )
    if empty.reasons != (ProbeReason.EXTRACTION_EMPTY.value,):
        raise AssertionError(
            f"транспорт дошёл + 0 символов обязаны дать extraction_empty, получено {empty.reasons}"
        )
    if empty.fetch is not None:
        raise AssertionError("при extraction_empty ось fetch обязана остаться null (не гадаем)")
    if "экстракт" not in (empty.detail or ""):
        raise AssertionError("detail при extraction_empty обязан указывать на экстрактор")

    # Расхождение тиров (один упал по транспорту, другой извлёк 0) ⇒ обе причины.
    mixed = decide(
        "k",
        [
            UrlProbe(
                "https://example.test/",
                ProbeSource.URLS,
                (
                    obs(
                        raised=True,
                        error="http: transient",
                        status_code=None,
                        text_chars=0,
                        content_bytes=0,
                    ),
                    obs(
                        tier=ProbeTier.HONEST,
                        text_chars=0,
                        content_bytes=260,
                        error="extract: no text",
                    ),
                ),
            )
        ],
    )
    if set(mixed.reasons) != {
        ProbeReason.BOTH_TIERS_FAILED.value,
        ProbeReason.EXTRACTION_EMPTY.value,
    }:
        raise AssertionError("расхождение тиров обязано нести обе причины в вердикте")

    # Синтез URL: source=synthesized ⇒ confidence=low на наблюдении.
    synth = decide(
        "k",
        [
            UrlProbe(
                "https://k.test/",
                ProbeSource.SYNTHESIZED,
                (obs(source=ProbeSource.SYNTHESIZED),),
            )
        ],
    )
    if synth.observations[0].confidence is not ProbeConfidence.LOW:
        raise AssertionError("synth-наблюдение обязано нести confidence=low")
    if synth.confidence is not ProbeConfidence.LOW:
        raise AssertionError("вердикт по synth-URL обязан быть low")

    # Шапка: байт-в-байт + неизмеренные поля сохраняются.
    raw = "# шапка\n# вторая строка\n\n" "a.test:\n  fetch: browser\n  strategy: honest\n  wait_for: \"x\"\n"
    empty = decide("a.test", [])
    header, _ = split_header(raw)
    rendered = render_profiles_with_verdicts(raw, [empty])
    if not rendered.startswith(header):
        raise AssertionError("шапка обязана сохраняться байт-в-байт")
    if "wait_for" not in rendered:
        raise AssertionError("неизмеренное поле wait_for обязано сохраниться")
    if "strategy" not in rendered:
        raise AssertionError("неизмеренное поле strategy обязано сохраниться")
    if "version" in rendered:
        raise AssertionError("version в domain_profiles.yaml добавлять нельзя")

    # Зеркало словаря (источник истины перечислений).
    data = load_dictionary(ROOT / "docs" / "status-dictionary.yaml")
    mirror = {
        "crawl_probe_status": {v.value for v in ProbeKeyStatus},
        "crawl_probe_source": {v.value for v in ProbeSource},
        "crawl_probe_confidence": {v.value for v in ProbeConfidence},
        "crawl_probe_reason": {v.value for v in ProbeReason},
    }
    for section, expected in mirror.items():
        if set(data.get(section, [])) != expected:
            raise AssertionError(
                f"словарь.{section} рассинхронизирован: словарь={sorted(data.get(section, []))}, "
                f"схема={sorted(expected)}"
            )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)
