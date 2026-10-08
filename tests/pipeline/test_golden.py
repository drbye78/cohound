"""Тесты golden-харнесса и калибровки порогов (W12/T12.4, §4.2/§11.3, R6/R12).

Покрывают: арифметику agreement, классификацию ``false_ok``/``false_absence``/
``mismatch``/``missing``, пороги :class:`GoldenThresholds` (на/под порогом), правило
«false ❌ только на Кр-параметрах», наличие страты ❌ (R12), гейты
``check_thresholds``/``assert_golden``, детерминизм ``to_manifest_json`` (с записью
калибровки, A3), чтение фактических статусов из ``SqliteStore(":memory:")`` (I4:
``failed`` → ``status=None``) и пустой набор.

Фикстуры категорийно нейтральны (§1.3): ``p1``/``p2``, ``feature.*``.
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Sequence
from pathlib import Path

import pytest

from core.config.run_config import load_golden
from core.domain.enums import (
    CellState,
    GoldenTier,
    Importance,
    ParamType,
    RunMode,
    SourceKind,
    Status,
)
from core.domain.models import (
    CatalogRow,
    Cell,
    Focus,
    GoldenRow,
    LLMEvidence,
    RunConfig,
)
from core.pipeline.golden import (
    GoldenReport,
    GoldenThresholdError,
    GoldenThresholds,
    assert_golden,
    check_thresholds,
    evaluate_golden,
    run_golden,
)
from core.store.sqlite_store import SqliteStore

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "golden_min.csv"
RUN_ID = "run-golden"


# ---------------------------------------------------------------------------
# Помощники (нейтральные)
# ---------------------------------------------------------------------------


def _row(
    param_id: str,
    expected: Status,
    *,
    case_class: str = "case",
    tier: GoldenTier = GoldenTier.FULL,
    product_id: str = "p1",
) -> GoldenRow:
    """Собирает нейтральную golden-строку."""
    return GoldenRow(
        product_id=product_id,
        param_id=param_id,
        expected_status=expected,
        expected_evidence_kind=[SourceKind.DOCUMENTATION],
        case_class=case_class,
        tier=tier,
    )


def _cat(param_id: str, importance: Importance) -> CatalogRow:
    """Нейтральная строка каталога (нужна только важность)."""
    return CatalogRow(
        param_id=param_id,
        title=param_id,
        module="mod.one",
        group="grp",
        importance=importance,
        param_type=ParamType.FACT,
        rule="правило",
    )


def _catalog() -> dict[str, CatalogRow]:
    """Каталог под строки ``golden_min.csv``; ``feature.critical`` — Кр."""
    return {
        "feature.alpha": _cat("feature.alpha", Importance.IMPORTANT),
        "feature.beta": _cat("feature.beta", Importance.BASIC),
        "feature.absent": _cat("feature.absent", Importance.OPTIONAL),
        "feature.critical": _cat("feature.critical", Importance.CRITICAL),
        "feature.gamma": _cat("feature.gamma", Importance.BASIC),
    }


def _golden() -> list[GoldenRow]:
    """Golden из нейтральной фикстуры (§4.2: колонки, включая ``tier``/``note``)."""
    return load_golden(FIXTURE_PATH)


def _actual_all_match() -> dict[tuple[str, str], Status | None]:
    """Фактические статусы, совпадающие со всеми ожиданиями фикстуры."""
    return {(r.product_id, r.param_id): r.expected_status for r in _golden()}


def _report(
    pairs: Sequence[tuple[Status, Status | None]],
    *,
    catalog: dict[str, CatalogRow] | None = None,
    thresholds: GoldenThresholds | None = None,
) -> GoldenReport:
    """Отчёт по парам ``(expected, actual)`` для параметров ``feature.pN`` продукта ``p1``."""
    golden = [
        _row(f"feature.p{i}", expected, case_class=f"c{i}")
        for i, (expected, _) in enumerate(pairs)
    ]
    actual: dict[tuple[str, str], Status | None] = {
        ("p1", f"feature.p{i}"): act for i, (_, act) in enumerate(pairs)
    }
    return run_golden(golden, actual, catalog=catalog, thresholds=thresholds)


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(),
        languages=["en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


def _cell(product_id: str, param_id: str, status: Status) -> Cell:
    """Валидная ячейка под инварианты I1/I2 (``failed`` собирается отдельно)."""
    if status in {Status.OK, Status.PARTIAL}:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=CellState.EVALUATED,
            status=status,
            evidence=[LLMEvidence(source_id="src.1", quote="цитата")],
        )
    if status is Status.ABSENT:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=CellState.EVALUATED,
            status=status,
            absence_evidence=[{"source_scope": "partial"}],
        )
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=status,
    )


# ---------------------------------------------------------------------------
# Фикстура / загрузчик
# ---------------------------------------------------------------------------


def test_fixture_header_accepted_and_neutral() -> None:
    """``load_golden`` принимает заголовок фикстуры (``tier``/``note`` опциональны)."""
    rows = _golden()
    assert len(rows) == 5
    assert all(r.product_id in {"p1", "p2"} for r in rows)
    assert any(r.expected_status is Status.ABSENT for r in rows)  # R12-страта
    assert any(r.tier is GoldenTier.FULL for r in rows)
    assert any(r.tier is GoldenTier.FAST for r in rows)
    # expected_evidence_kind разобран в SourceKind.
    absent = next(r for r in rows if r.expected_status is Status.ABSENT)
    assert absent.expected_evidence_kind == [SourceKind.REGISTRY]


# ---------------------------------------------------------------------------
# Agreement и классификация
# ---------------------------------------------------------------------------


def test_agreement_math() -> None:
    report = _report(
        [
            (Status.OK, Status.OK),
            (Status.OK, Status.OK),
            (Status.PARTIAL, Status.PARTIAL),
            (Status.ABSENT, Status.ABSENT),
            (Status.OK, Status.PARTIAL),  # mismatch (не false ✅/❌)
        ]
    )
    assert report.total == 5
    assert report.matches == 4
    assert report.agreement == pytest.approx(0.8)
    assert report.error_counts == {"mismatch": 1}
    assert report.case_class_counts == {f"c{i}": 1 for i in range(5)}
    assert sum(report.expected_counts.values()) == 5


def test_error_classification() -> None:
    report = _report(
        [
            (Status.OK, Status.OK),  # match
            (Status.PARTIAL, Status.OK),  # false ✅
            (Status.OK, Status.ABSENT),  # false ❌
            (Status.OK, Status.PARTIAL),  # mismatch
            (Status.OK, None),  # missing (I4)
        ]
    )
    classes = {r.param_id: r.error_class for r in report.results}
    assert classes["feature.p0"] is None
    assert classes["feature.p1"] == "false_ok"
    assert classes["feature.p2"] == "false_absence"
    assert classes["feature.p3"] == "mismatch"
    assert classes["feature.p4"] == "missing"

    assert report.matches == 1
    assert report.agreement == pytest.approx(0.2)
    assert report.error_counts == {
        "false_ok": 1,
        "false_absence": 1,
        "mismatch": 1,
        "missing": 1,
    }
    assert report.missing == 1
    assert report.false_ok == 1


def test_missing_key_treated_as_missing() -> None:
    """Отсутствие ключа в отображении равнозначно ``actual=None`` (failed)."""
    golden = [_row("feature.x", Status.OK)]
    report = run_golden(golden, {})
    assert report.missing == 1
    assert report.results[0].error_class == "missing"
    assert report.results[0].actual is None


# ---------------------------------------------------------------------------
# Пороги / passes
# ---------------------------------------------------------------------------


def test_passes_at_and_under_threshold() -> None:
    pairs = [
        (Status.OK, Status.OK),
        (Status.OK, Status.OK),
        (Status.OK, Status.OK),
        (Status.OK, Status.OK),
        (Status.OK, Status.PARTIAL),  # mismatch → 0.8
    ]
    report = _report(pairs)
    assert report.agreement == pytest.approx(0.8)
    assert report.passes() is True  # ровно на пороге
    assert report.passes(GoldenThresholds(min_agreement=0.8)) is True
    assert report.passes(GoldenThresholds(min_agreement=0.81)) is False

    under = _report(
        [
            (Status.OK, Status.OK),
            (Status.OK, Status.OK),
            (Status.OK, Status.OK),
            (Status.OK, Status.PARTIAL),
            (Status.OK, Status.PARTIAL),
        ]
    )
    assert under.agreement == pytest.approx(0.6)
    assert under.passes() is False


def test_false_ok_fails_even_with_high_agreement() -> None:
    pairs = [(Status.OK, Status.OK)] * 9 + [(Status.PARTIAL, Status.OK)]
    report = _report(pairs)
    assert report.agreement == pytest.approx(0.9)
    assert report.false_ok == 1
    assert report.passes() is False
    report_threshold = _report(pairs, thresholds=GoldenThresholds(max_false_ok=1))
    assert report_threshold.passes() is True


def test_false_absence_non_critical_does_not_fail_critical_rule() -> None:
    """Правило R12 кусает только Кр; false ❌ на некритичном не блокирует passes."""
    pairs = [(Status.OK, Status.OK)] * 9 + [(Status.OK, Status.ABSENT)]
    catalog = {f"feature.p{i}": _cat(f"feature.p{i}", Importance.IMPORTANT) for i in range(10)}
    report = _report(pairs, catalog=catalog)
    assert report.error_counts["false_absence"] == 1
    assert report.critical_false_absence == 0
    assert report.passes() is True


# ---------------------------------------------------------------------------
# Критический класс: false ❌ только на Кр
# ---------------------------------------------------------------------------


def test_critical_false_absence_counts_only_critical() -> None:
    catalog = {
        "feature.crit": _cat("feature.crit", Importance.CRITICAL),
        "feature.imp": _cat("feature.imp", Importance.IMPORTANT),
    }
    golden = [_row("feature.crit", Status.OK), _row("feature.imp", Status.OK)]
    actual = {("p1", "feature.crit"): Status.ABSENT, ("p1", "feature.imp"): Status.ABSENT}
    report = run_golden(golden, actual, catalog=catalog)
    assert report.error_counts["false_absence"] == 2
    assert report.critical_false_absence == 1
    assert report.passes() is False

    relaxed = run_golden(
        golden,
        actual,
        catalog=catalog,
        thresholds=GoldenThresholds(min_agreement=0.0, max_critical_false_absence=1),
    )
    assert relaxed.passes() is True


def test_critical_false_absence_without_catalog_is_zero() -> None:
    golden = [_row("feature.crit", Status.OK)]
    actual = {("p1", "feature.crit"): Status.ABSENT}
    report = run_golden(golden, actual)
    assert report.critical_false_absence == 0
    assert all(r.importance is None for r in report.results)


def test_importance_resolved_from_catalog() -> None:
    catalog = {"feature.crit": _cat("feature.crit", Importance.CRITICAL)}
    report = run_golden(
        [_row("feature.crit", Status.OK)],
        {("p1", "feature.crit"): Status.OK},
        catalog=catalog,
    )
    assert report.results[0].importance is Importance.CRITICAL


# ---------------------------------------------------------------------------
# R12: страта ожидаемых ❌
# ---------------------------------------------------------------------------


def test_has_absent_stratum() -> None:
    assert _report([(Status.OK, Status.OK), (Status.ABSENT, Status.ABSENT)]).has_absent_stratum
    assert not _report([(Status.OK, Status.OK), (Status.PARTIAL, Status.PARTIAL)]).has_absent_stratum
    assert not run_golden([], {}).has_absent_stratum
    assert run_golden(_golden(), _actual_all_match()).has_absent_stratum


# ---------------------------------------------------------------------------
# check_thresholds / assert_golden
# ---------------------------------------------------------------------------


def test_check_thresholds_clean_passes() -> None:
    report = run_golden(_golden(), _actual_all_match(), catalog=_catalog())
    assert report.agreement == pytest.approx(1.0)
    assert check_thresholds(report) == []
    assert_golden(report)  # не поднимает


def test_check_thresholds_reports_r6_and_r12_violations() -> None:
    # Нет страты ❌, agreement ниже порога, false ✅ и false ❌ на Кр.
    catalog = {"feature.p1": _cat("feature.p1", Importance.CRITICAL)}
    report = _report(
        [
            (Status.PARTIAL, Status.OK),  # false ✅
            (Status.OK, Status.ABSENT),  # false ❌ на Кр (p0)
            (Status.OK, Status.ABSENT),
        ],
        catalog=catalog,
        thresholds=GoldenThresholds(min_agreement=0.9),
    )
    violations = check_thresholds(report)
    joined = " ".join(violations)
    assert any("R6" in v for v in violations)
    assert any("false ✅" in v for v in violations)
    assert any("R12" in v and "Кр" in v for v in violations)
    assert "страты ожидаемых ❌" in joined
    with pytest.raises(GoldenThresholdError) as excinfo:
        assert_golden(report)
    assert "R6" in str(excinfo.value)


def test_check_thresholds_missing_absent_stratum_only() -> None:
    report = _report([(Status.OK, Status.OK)], thresholds=GoldenThresholds(min_agreement=0.5))
    violations = check_thresholds(report)
    assert len(violations) == 1
    assert "страты ожидаемых ❌" in violations[0]
    with pytest.raises(GoldenThresholdError):
        assert_golden(report)


# ---------------------------------------------------------------------------
# Манифест (детерминизм + запись калибровки, A3)
# ---------------------------------------------------------------------------


def test_to_manifest_json_determinism_and_calibration() -> None:
    thresholds = GoldenThresholds(min_agreement=0.7, max_false_ok=0, max_critical_false_absence=0)
    report = run_golden(
        _golden(), _actual_all_match(), catalog=_catalog(), thresholds=thresholds
    )
    raw = report.to_manifest_json()
    assert raw == report.to_manifest_json()
    data = json.loads(raw)
    assert data["total"] == 5
    assert data["agreement"] == pytest.approx(1.0)
    assert data["has_absent_stratum"] is True
    assert data["thresholds"] == {
        "min_agreement": 0.7,
        "max_false_ok": 0,
        "max_critical_false_absence": 0,
    }
    # sort_keys: верхнеуровневые ключи отсортированы (детерминизм).
    assert list(data.keys()) == sorted(data.keys())
    assert data["results"][0]["expected"] == Status.OK.value


def test_summary_is_stable() -> None:
    report = run_golden(_golden(), _actual_all_match(), catalog=_catalog())
    assert report.summary() == report.summary()
    assert "PASS" in report.summary()
    empty = run_golden([], {})
    assert "0/0" in empty.summary()
    assert "FAIL" in empty.summary()


# ---------------------------------------------------------------------------
# evaluate_golden
# ---------------------------------------------------------------------------


def test_evaluate_golden_maps_store_statuses() -> None:
    golden = _golden()
    report: GoldenReport | None = None
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        s.save_cell(_cell("p1", "feature.alpha", Status.OK))
        s.save_cell(_cell("p1", "feature.beta", Status.PARTIAL))
        s.save_cell(_cell("p1", "feature.absent", Status.ABSENT))
        s.save_cell(_cell("p1", "feature.critical", Status.OK))
        s.save_cell(_cell("p2", "feature.gamma", Status.UNKNOWN))
        report = evaluate_golden(
            golden=golden, store=s, run_id=RUN_ID, catalog=_catalog()
        )
    assert report is not None
    assert report.agreement == pytest.approx(1.0)
    assert report.missing == 0
    assert report.passes() is True
    assert check_thresholds(report) == []


def test_evaluate_golden_missing_and_failed_are_missing() -> None:
    """I4: ``failed``-ячейка читается как ``status=None`` → ``missing``."""
    golden = _golden()
    report: GoldenReport | None = None
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        s.save_cell(_cell("p1", "feature.alpha", Status.OK))
        # feature.beta/critical/absent не сохранены; feature.gamma — failed.
        s.save_cell(
            Cell(
                run_id=RUN_ID,
                product_id="p2",
                param_id="feature.gamma",
                state=CellState.FAILED,
            )
        )
        report = evaluate_golden(
            golden=golden, store=s, run_id=RUN_ID, catalog=_catalog()
        )
    assert report is not None
    assert report.matches == 1
    assert report.missing == 4
    assert all(
        r.error_class == "missing"
        for r in report.results
        if r.param_id != "feature.alpha"
    )
    # R12-страта определяется ожиданиями golden, а не фактом ячейки.
    assert report.has_absent_stratum is True


def test_evaluate_golden_empty_set_no_crash() -> None:
    report: GoldenReport | None = None
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        report = evaluate_golden(golden=[], store=s, run_id=RUN_ID, catalog={})
    assert report is not None
    assert report.total == 0
    assert report.agreement == 0.0


# ---------------------------------------------------------------------------
# Пустой набор
# ---------------------------------------------------------------------------


def test_empty_golden_is_deterministic() -> None:
    report = run_golden([], {})
    assert report.results == []
    assert report.total == 0
    assert report.matches == 0
    assert report.agreement == 0.0
    assert report.false_ok == 0
    assert report.critical_false_absence == 0
    assert report.missing == 0
    assert report.error_counts == {}
    assert report.case_class_counts == {}
    assert report.expected_counts == {}
    assert report.has_absent_stratum is False
    assert report.passes() is False
    assert isinstance(json.loads(report.to_manifest_json()), dict)
