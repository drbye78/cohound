"""Тесты волны наследования Ш4 (W6/T6.4): direct/conditional/blocked, K5, П>В.

Нейтральные фикстуры (``p1/p2``, ``param_a``, ``mod_a``). Доменные объекты собираются
детерминированно; там, где модель намеренно невалидна, используется ``model_construct``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from core.domain import (
    AnchorRelation,
    BaselineRow,
    Cell,
    CellEvidence,
    CellState,
    Citation,
    Cohort,
    CohortEntry,
    EvidenceType,
    InheritanceMode,
    Passport,
    SourceKind,
    Status,
    Upstream,
    UpstreamRef,
    check_cell,
    check_derived_evidence,
    check_inheritance,
)
from core.pipeline import RunPlan
from core.pipeline.inheritance import apply_precedence, inheritance_wave

RUN_ID = "run-fixture"
FIXTURES = Path(__file__).parent / "fixtures"


# --------------------------------------------------------------------------- #
# Нейтральные строители
# --------------------------------------------------------------------------- #


def _fixture() -> dict:
    return json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))


def _baselines() -> dict[str, BaselineRow]:
    rows = [BaselineRow.model_validate(row) for row in _fixture()["baselines"]]
    return {row.param_id: row for row in rows}


def _plan(params: list[str] | None = None, products: tuple[str, ...] = ("p1",)) -> RunPlan:
    from core.domain import CatalogRow

    rows = [CatalogRow.model_validate(row) for row in _fixture()["catalog"]]
    wanted = set(params) if params is not None else {row.param_id for row in rows}
    rows = [row for row in rows if row.param_id in wanted]
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
        params=rows,
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


def _passport(
    product_id: str,
    *,
    baseline_id: str = "base_1",
    version: str = "16.0",
    evidence_url: str = "https://upstream.test/doc#param_a",
    quote: str = "upstream documents the mechanism",
) -> Passport:
    return Passport(
        product_id=product_id,
        canonical_name=f"Product {product_id}",
        vendor="Vendor",
        upstream=Upstream(
            baseline_id=baseline_id,
            version=version,
            evidence=Citation(url=evidence_url, quote=quote),
        ),
        version="1.0",
        jurisdiction="RU",
        snapshot_date=_dt.date(2026, 1, 1),
    )


class _RecordingStore:
    """Минимальный store-дублёр для проверки персистенции (§4.4)."""

    def __init__(self) -> None:
        self.cells: list[Cell] = []
        self.evidence: list[CellEvidence] = []

    def save_cell(self, cell: Cell) -> None:
        self.cells.append(cell)

    def add_evidence(self, ev: CellEvidence) -> None:
        self.evidence.append(ev)


# --------------------------------------------------------------------------- #
# direct
# --------------------------------------------------------------------------- #


def test_direct_produces_cell_with_valid_derived_evidence() -> None:
    baseline = _baselines()["param_a"]  # direct
    store = _RecordingStore()
    result = inheritance_wave(
        _plan(["param_a"]), [baseline], {"p1": _passport("p1")}, store=store
    )

    assert result.status == "ok" and result.phase == "inheritance"
    [cell] = result.cells
    assert cell.state is CellState.EVALUATED and cell.status is Status.OK
    assert cell.source_kind is SourceKind.INHERITANCE
    assert check_cell(cell) == []

    [ev] = store.evidence
    assert ev.evidence_type is EvidenceType.DERIVED
    assert ev.source_kind is SourceKind.INHERITANCE
    assert ev.content_hash and ev.source_url
    upstream = UpstreamRef(
        product_id="p1",
        param_id="param_a",
        source_id=ev.source_id,
        source_url=ev.source_url,
        content_hash=ev.content_hash,
    )
    # K5-контракт производного доказательства выполнен.
    assert check_derived_evidence(ev, upstream) == []
    # I8 + derived-evidence на унаследованной ячейке выполнен.
    assert check_inheritance(cell, baseline, [ev]) == []
    assert store.cells == result.cells


def test_direct_without_resolvable_evidence_is_rejected() -> None:
    baseline = _baselines()["param_a"]
    passport = _passport("p1", evidence_url="")  # нет резолва в URL (I15)
    result = inheritance_wave(_plan(["param_a"]), [baseline], {"p1": passport})

    assert result.cells == []
    assert any("I15" in err or "наследован" in err for err in result.errors)


def test_direct_without_upstream_citation_is_rejected() -> None:
    baseline = _baselines()["param_a"]
    passport = _passport("p1")
    # Намеренно невалидный upstream без цитаты (модель не допустила бы в обычной сборке).
    passport = passport.model_copy(update={"upstream": None})
    result = inheritance_wave(_plan(["param_a"]), [baseline], {"p1": passport})

    # upstream=None трактуется как кросс-линейный параметр: ячейка не производится.
    assert result.cells == []


# --------------------------------------------------------------------------- #
# conditional / blocked / нет строки
# --------------------------------------------------------------------------- #


def test_conditional_goes_to_pending_not_resolved() -> None:
    baseline = _baselines()["param_b"]  # conditional
    result = inheritance_wave(_plan(["param_b"]), [baseline], {"p1": _passport("p1")})

    [cell] = result.cells
    assert cell.state is CellState.PENDING
    assert cell.status is None  # НЕ разрешённый статус
    assert check_cell(cell) == []


def test_blocked_is_skipped() -> None:
    baseline = _baselines()["param_c"]  # blocked
    result = inheritance_wave(_plan(["param_c"]), [baseline], {"p1": _passport("p1")})
    assert result.cells == []


def test_no_baseline_row_produces_nothing() -> None:
    # Кросс-линейный параметр (архетип B): строки baselines.csv нет → только корпус.
    result = inheritance_wave(_plan(["param_a"]), [], {"p1": _passport("p1")})
    assert result.cells == []
    assert result.errors == []


def test_missing_passport_surfaces_error() -> None:
    result = inheritance_wave(_plan(["param_a"]), [_baselines()["param_a"]], {})
    assert result.cells == []
    assert any("паспорт" in err for err in result.errors)


def test_min_version_blocks_direct_when_upstream_too_old() -> None:
    baseline = _baselines()["param_a"].model_copy(update={"min_version": "20.0"})
    passport = _passport("p1", version="16.0")
    result = inheritance_wave(_plan(["param_a"]), [baseline], {"p1": passport})
    assert result.cells == []


# --------------------------------------------------------------------------- #
# П>В
# --------------------------------------------------------------------------- #


def _inherited_cell() -> Cell:
    # model_construct: проверяется только П>В-переход, а не I1 наследуемой ячейки.
    return Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[],
        absence_evidence=[],
        reviewed_by=None,
    )


def test_precedence_returns_review_candidate_without_overwrite() -> None:
    inherited = _inherited_cell()
    # Прямое доказательство Ш6 понижает: ❌ (собрано model_construct — невалидная ячейка).
    direct = Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        source_kind=SourceKind.DOCUMENTATION,
        evidence=[],
    )
    candidate = apply_precedence(inherited, direct)

    assert candidate.state is CellState.PENDING  # кандидат на ревью, не авто-перезапись
    assert candidate.status is Status.ABSENT  # предложенный (пониженный) статус
    assert inherited.status is Status.OK  # унаследованная ячейка не мутирована


def test_precedence_same_status_keeps_inherited() -> None:
    inherited = _inherited_cell()
    direct = Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        state=CellState.EVALUATED,
        status=Status.OK,
    )
    assert apply_precedence(inherited, direct) is inherited


def test_precedence_unresolved_direct_keeps_inherited() -> None:
    inherited = _inherited_cell()
    direct = Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        state=CellState.FAILED,
        status=None,
    )
    assert apply_precedence(inherited, direct) is inherited


def test_baselines_must_be_list() -> None:
    with pytest.raises(ValueError):
        inheritance_wave(_plan(["param_a"]), {"param_a": _baselines()["param_a"]}, {})  # type: ignore[arg-type]
