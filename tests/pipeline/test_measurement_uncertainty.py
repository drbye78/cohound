"""Полоса неопределённости измерения вердиктов (P6c): провенанс, границы, запрет ранжирования.

Главное, что пинится: артефакт **не выдаёт объявленную величину за измеренную в этом
прогоне** (``computed_from_this_run``), отсутствие объявления **отличается** от нуля, а
запрет ранжирования включается только при критических ячейках — и он машинный
(``ranking_restriction``), а не только текстом.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from core.domain.enums import CellState, Importance, Status
from core.domain.models import CatalogRow, Cell, LLMEvidence
from core.pipeline.measurement_uncertainty import (
    DECLARED_VERDICT_STABILITY_DATE,
    DECLARED_VERDICT_STABILITY_SOURCE,
    MeasuredBand,
    build_measurement_uncertainty,
    read_repeat_probes,
)
from core.pipeline.report import write_measurement_uncertainty_artifact

RUN_ID = "run-uncertainty"
PRODUCT = "prod.a"


def _row(param_id: str, importance: Importance = Importance.BASIC) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=param_id,
        module="mod.one",
        group="grp",
        importance=importance,
        param_type="fact",
        rule="правило",
    )


def _cell(
    param_id: str,
    *,
    status: Status | None = Status.OK,
    state: CellState = CellState.EVALUATED,
) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=PRODUCT,
        param_id=param_id,
        state=state,
        status=status,
        evidence=[LLMEvidence(source_id="src.1", quote="цитата")]
        if status is Status.OK
        else [],
    )


def _benchmark_payload(*, changed: int, total: int, label: str = "model.test") -> dict:
    """Артефакт ``benchmark/evaluator-*.json`` с измеренной пробой детерминизма."""
    return {
        "models": [
            {
                "label": label,
                "n_cells": total,
                "determinism": {
                    "measured": True,
                    "repeats": 2,
                    "changed_cells": changed,
                    "fraction_changed": changed / total,
                    "stable_fraction": 1.0 - changed / total,
                    "cells_total": total,
                },
            }
        ]
    }


def test_declared_band_carries_provenance(tmp_path: Path) -> None:
    """Объявленная полоса несёт источник и дату — без них число неотличимо от выдумки."""
    payload = build_measurement_uncertainty()

    assert payload["computed_from_this_run"] is False
    assert payload["provenance"]["kind"] == "declared"
    band = payload["band"]
    assert band["present"] is True
    assert band["share"] == pytest.approx(0.042)
    assert band["cells_changed"] == 5
    assert band["cells_total"] == 119
    assert band["applies_to"] == "verdict_stability_under_repeat"
    assert band["source"] == DECLARED_VERDICT_STABILITY_SOURCE
    assert band["measured_at"] == DECLARED_VERDICT_STABILITY_DATE
    assert DECLARED_VERDICT_STABILITY_SOURCE.endswith(".md")


def test_artifact_is_written_with_same_serializer(tmp_path: Path) -> None:
    payload = build_measurement_uncertainty()
    path = write_measurement_uncertainty_artifact(
        payload, tmp_path / "measurement_uncertainty.yaml", run_id=RUN_ID
    )

    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert document["artifact"] == "measurement_uncertainty"
    assert document["run_id"] == RUN_ID
    assert document["computed_from_this_run"] is False
    assert document["band"]["share"] == pytest.approx(0.042)
    assert document["provenance"]["note"]


def test_repeats_in_run_switch_to_measured(tmp_path: Path) -> None:
    """Есть измеренные повторы в прогоне — полоса считается из них, провенанс это говорит."""
    run_dir = tmp_path / "run"
    (run_dir / "benchmark").mkdir(parents=True)
    (run_dir / "benchmark" / "evaluator-20260101T000000000000Z.json").write_text(
        json.dumps(_benchmark_payload(changed=3, total=50)), encoding="utf-8"
    )

    probes = read_repeat_probes(run_dir)
    assert len(probes) == 1

    payload = build_measurement_uncertainty(run_dir=run_dir)
    assert payload["computed_from_this_run"] is True
    assert payload["provenance"]["kind"] == "measured_in_this_run"
    assert payload["band"]["share"] == pytest.approx(0.06)
    assert payload["band"]["cells_changed"] == 3
    assert payload["band"]["cells_total"] == 50


def test_repeats_without_measurement_are_ignored(tmp_path: Path) -> None:
    """Прогон с одним повтором не говорит о стабильности: полоса остаётся объявленной."""
    run_dir = tmp_path / "run"
    (run_dir / "benchmark").mkdir(parents=True)
    unmeasured = {
        "models": [
            {
                "label": "model.test",
                "n_cells": 10,
                "determinism": {
                    "measured": False,
                    "repeats": 1,
                    "changed_cells": 0,
                    "fraction_changed": None,
                    "stable_fraction": None,
                    "cells_total": 0,
                },
            }
        ]
    }
    (run_dir / "benchmark" / "evaluator-20260101T000000000000Z.json").write_text(
        json.dumps(unmeasured), encoding="utf-8"
    )

    payload = build_measurement_uncertainty(run_dir=run_dir)

    assert read_repeat_probes(run_dir) == []
    assert payload["computed_from_this_run"] is False
    assert payload["provenance"]["kind"] == "declared"


def test_absence_differs_from_zero() -> None:
    """Нет объявления — «неизвестно», ноль — измеренная устойчивость. Это разные записи."""
    absent = build_measurement_uncertainty(declared=None)
    zero = build_measurement_uncertainty(
        declared=MeasuredBand(
            share=0.0, cells_changed=0, cells_total=119, source="замер", measured_at="2026-01-01"
        )
    )

    assert absent["band"]["present"] is False
    assert absent["band"]["share"] is None
    assert absent["provenance"]["kind"] == "not_declared"
    assert "не измерена" in absent["provenance"]["note"]

    assert zero["band"]["present"] is True
    assert zero["band"]["share"] == 0.0
    assert zero["provenance"]["kind"] == "declared"


def test_ranking_is_not_forbidden_without_critical_cells() -> None:
    """Полоса есть, но критических ячеек нет — ранжирование не запрещается."""
    catalog = {"p001": _row("p001")}
    payload = build_measurement_uncertainty(
        cells=[_cell("p001")], catalog=catalog
    )

    assert payload["critical_cells"]["measured"] == 0
    assert payload["ranking_restriction"] == "none"


def test_ranking_is_forbidden_when_critical_cell_is_within_band() -> None:
    """Критическая ячейка внутри полосы — ранжирование запрещено (отчёт — нет)."""
    catalog = {"p001": _row("p001", Importance.CRITICAL)}
    payload = build_measurement_uncertainty(
        cells=[_cell("p001")], catalog=catalog
    )

    assert payload["critical_cells"]["measured"] == 1
    assert payload["critical_cells"]["at_risk_within_band"] is True
    assert payload["ranking_restriction"] == "forbidden_within_band"
    assert "ранжирование" in payload["ranking_note"]


def test_stale_and_preliminary_cells_do_not_count_as_critical() -> None:
    """``preliminary`` (I13) и ``stale`` (§13) вне расчёта: их вердикт не публикуется."""
    catalog = {"p001": _row("p001", Importance.CRITICAL)}
    cells = [
        _cell("p001", status=None, state=CellState.PRELIMINARY),
        _cell("p001").model_copy(update={"stale_reason": "prompt_changed"}),
    ]
    payload = build_measurement_uncertainty(cells=cells, catalog=catalog)

    assert payload["critical_cells"]["measured"] == 0
    assert payload["ranking_restriction"] == "none"


def test_failed_critical_cell_counts() -> None:
    """Смена «вердикт → сбой» — тоже смена вердикта (в замере таких было два из пяти)."""
    catalog = {"p001": _row("p001", Importance.CRITICAL)}
    payload = build_measurement_uncertainty(
        cells=[_cell("p001", status=None, state=CellState.FAILED)], catalog=catalog
    )

    assert payload["critical_cells"]["measured"] == 1
    assert payload["ranking_restriction"] == "forbidden_within_band"


def test_payload_is_deterministic(tmp_path: Path) -> None:
    """Детерминизм артефакта: одинаковый вход даёт одинаковый документ."""
    first = build_measurement_uncertainty(cells=[_cell("p001")], catalog={})
    second = build_measurement_uncertainty(cells=[_cell("p001")], catalog={})

    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_report_phase_emits_the_artifact(tmp_path: Path) -> None:
    """Ш9 пишет артефакт рядом с профилем публикуемости — путь виден в ``Artifacts``.

    Проверяется именно проводка отчёта: без неё величина существовала бы только в файле
    замера, а не в прогоне (ровно дефект P6c).
    """
    import datetime as _dt
    from types import SimpleNamespace

    from core.domain.enums import RunMode
    from core.domain.models import Focus, RunConfig
    from core.pipeline.compute import compute
    from core.pipeline.report import build_manifest, write_reports
    from core.store.sqlite_store import SqliteStore

    run_id = "run-uncertainty-report"
    catalog = {"p001": _row("p001", Importance.CRITICAL)}
    cell = _cell("p001").model_copy(update={"run_id": run_id})
    with SqliteStore(":memory:") as store:
        store.create_run(
            run_id,
            RunConfig(
                case_id="case-1",
                anchor=PRODUCT,
                cohort="cohort.yaml",
                products=[PRODUCT],
                package="pkg-1",
                focus=Focus(),
                languages=["en"],
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        store.save_cell(cell)
        bundle = compute(
            store=store,
            run_id=run_id,
            catalog=catalog,
            module_weights={},
            universe={(PRODUCT, "p001")},
        )
        manifest = build_manifest(
            inputs=SimpleNamespace(
                run=SimpleNamespace(
                    case_id="case-1",
                    mode=RunMode.STRICT,
                    methodology_version="0.1.0",
                    cohort="cohort.yaml",
                    ranking_policy=None,
                ),
                package=None,
            ),
            plan=SimpleNamespace(
                run_id=run_id,
                anchor=PRODUCT,
                modules=["mod.one"],
                methodology_version="0.1.0",
                catalog_version="1.0.0",
                cohort_ref="cohort.yaml",
                params=list(catalog.values()),
            ),
            bundle=bundle,
            created_at="2026-01-01T00:00:00Z",
        )
        # Предупреждения публикуемости здесь не проверяются (это предмет A1-тестов):
        # проверяется проводка артефакта P6c в фазу отчёта.
        artifacts = write_reports(
            bundle=bundle,
            cells=[cell],
            catalog=catalog,
            manifest=manifest,
            out_dir=tmp_path,
            run_id=run_id,
        )

    assert artifacts.measurement_uncertainty is not None
    document = yaml.safe_load(artifacts.measurement_uncertainty.read_text(encoding="utf-8"))
    assert document["artifact"] == "measurement_uncertainty"
    assert document["computed_from_this_run"] is False
    assert document["band"]["present"] is True
    # Критическая ячейка в измеренном множестве → ранжирование внутри полосы запрещено.
    assert document["ranking_restriction"] == "forbidden_within_band"
