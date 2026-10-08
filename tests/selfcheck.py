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
