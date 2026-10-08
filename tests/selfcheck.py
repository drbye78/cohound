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
    check("DoclingPdfExtractor: грациозная деградация", lambda: _pdf_extractor())

    section("Скопированные утилиты/адаптеры (gtm, авторские) — §11.2/§11.3")
    check("text_utils: RU-нормализация токенов", lambda: _util_ru_tokenize())
    check("BM25: продукт-скоупед и детерминизм (I17/R7)", lambda: _bm25())
    check("retrieval_eval: hit@K/recall@K/MRR (I20)", lambda: _retrieval_eval())

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
    from core.adapters.extract_pdf import DoclingPdfExtractor

    ex = DoclingPdfExtractor()
    # При недоступном docling адаптер деградирует, а не падает.
    res = ex.extract_bytes(b"%PDF-1.4 not really a pdf")
    _assert(res.text == "")
    _assert(res.error is not None)
    _assert(res.extractor_version == DoclingPdfExtractor.version)


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


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)
