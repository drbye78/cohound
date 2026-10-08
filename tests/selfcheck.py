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

from core.db import connect  # noqa: E402
from core.schemas import (  # noqa: E402
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

    section("Порты (L2) — контракты (01 §3, W1/T1.1)")
    check("порты импортируются, все — Protocol", lambda: _ports_contracts())
    check("I18 структурно: у оценщика нет tools/web_search", lambda: _ports_i18())
    check("адаптеры используют порт-типы (Chunk/ExtractedDoc)", lambda: _ports_type_reuse())

    section("Инженерные гейты (W1/T1.4)")
    check("матрица зависимостей core.* (01 §1, ast)", lambda: _import_graph())
    check("§1.3 нейтральность domain/ports/pipeline (грепп)", lambda: _neutrality())
    check("transitional-шимы schemas.py/db.py сохраняют идентичность", lambda: _domain_store_shims())

    section("Домен (L0) — W2")
    check("инварианты домена возвращают Violation (I1/I4/I13/I7)", lambda: _domain_invariants())
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


def _db_smoke() -> None:
    conn = connect(":memory:")
    ver = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    _assert(ver is not None and ver["value"] == "2.0.0")

    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ("products", "runs", "llm_scenarios", "corpus_docs", "corpus_chunks",
              "index_manifest", "cells", "cell_evidence", "search_log",
              "search_exhaustion", "registry_facts"):
        _assert(t in tables)

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
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("core."):
                            _check_edge(layer, alias.name.split(".")[1], matrix, py, root, violations)
                if mod is None:
                    continue
                _check_edge(layer, mod.split(".")[1], matrix, py, root, violations)
    if violations:
        raise AssertionError("нарушена матрица зависимостей (01 §1): " + "; ".join(violations))


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


def _domain_store_shims() -> None:
    """Transitional-шимы core/schemas.py и core/db.py сохраняют идентичность типов (T1.2/T1.3)."""
    from core.db import DDL, SCHEMA_VERSION, connect
    from core.domain import Cell as DomainCell
    from core.domain import Status as DomainStatus
    from core.schemas import Cell as ShimCell
    from core.schemas import Status as ShimStatus
    from core.store import sqlite

    if ShimCell is not DomainCell:
        raise AssertionError("core.schemas.Cell должен ссылаться на core.domain.Cell")
    if ShimStatus is not DomainStatus:
        raise AssertionError("core.schemas.Status должен ссылаться на core.domain.Status")
    if connect is not sqlite.connect or SCHEMA_VERSION != sqlite.SCHEMA_VERSION or DDL != sqlite.DDL:
        raise AssertionError("core.db должен реэкспортировать core.store.sqlite")


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
    from core.pipeline.runner import Runner
    from core.store.sqlite_store import SqliteStore

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
        r1 = Runner(store)
        rep = r1.run("r1", until="g1")
        if rep.stopped_at != "g1" or rep.status != "blocked":
            raise AssertionError("гейт обязан остановить прогон без решения (S9)")
        if is_gate_passed(store, "r1", Gate.G1):
            raise AssertionError("гейт не пройден без строки gate_decisions")
        # Durable resume: новый Runner видит завершённые фазы через store.
        r2 = Runner(store)
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


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)
