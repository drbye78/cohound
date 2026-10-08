"""Тесты чистых инвариантов домена (W2/T2.2, §2.3).

Проверяют, что ``core/domain/invariants.py`` возвращает типизированные ``Violation``
вместо исключений. Для сценариев-нарушений модели собираются через ``model_construct``
(в обход Pydantic-валидатора, который сам запрещает такие объекты) — именно поэтому
инварианты и вынесены в чистые функции.
"""

from __future__ import annotations

from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from core.domain.enums import (
    AbsenceRuleType,
    CellState,
    EvidenceState,
    EvidenceType,
    InheritanceMode,
    Origin,
    Relationship,
    SatelliteState,
    SourceKind,
    SourceScope,
    Status,
)
from core.domain.invariants import (
    check_cell,
    check_derived_evidence,
    check_evidence,
    check_inheritance,
    check_satellite,
)
from core.domain.models import (
    BaselineRow,
    Cell,
    CellEvidence,
    Citation,
    LLMEvidence,
    Satellite,
    UpstreamRef,
    Violation,
)

_RUN = "run"
_PRODUCT = "prod"
_PARAM = "param"


def _codes(violations: list[Violation]) -> set[str]:
    return {v.code for v in violations}


def _cell(**kw: Any) -> Cell:
    """Ячейка в обход Pydantic-валидатора — чтобы предъявить нарушение чистым функциям."""
    base: dict[str, Any] = {
        "run_id": _RUN,
        "product_id": _PRODUCT,
        "param_id": _PARAM,
    }
    base.update(kw)
    return Cell.model_construct(**base)


def _evidence(**kw: Any) -> CellEvidence:
    """Evidence в обход Pydantic-валидатора (для нарушения I10/I19/I22)."""
    base: dict[str, Any] = {
        "evidence_id": "ev",
        "run_id": _RUN,
        "product_id": _PRODUCT,
        "param_id": _PARAM,
        "source_kind": SourceKind.DOCUMENTATION,
        "evidence_type": EvidenceType.DIRECT,
        "source_id": "chunk_1",
        "content_hash": "h1",
    }
    base.update(kw)
    return CellEvidence.model_construct(**base)


def _baseline(mode: InheritanceMode) -> BaselineRow:
    return BaselineRow(baseline_id="bl", param_id=_PARAM, inheritance_mode=mode)


# ---------------------------------------------------------------------------
# I1
# ---------------------------------------------------------------------------


def test_i1_evaluated_ok_without_evidence() -> None:
    c = _cell(state=CellState.EVALUATED, status=Status.OK, evidence=[])
    assert "I1" in _codes(check_cell(c))


def test_i1_pass_with_evidence() -> None:
    c = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.EVALUATED,
        status=Status.OK,
        evidence=[LLMEvidence(source_id="c1", quote="q")],
    )
    assert check_cell(c) == []


# ---------------------------------------------------------------------------
# I2 / I2b
# ---------------------------------------------------------------------------


def test_i2_absent_without_absence_evidence() -> None:
    c = _cell(state=CellState.EVALUATED, status=Status.ABSENT, absence_evidence=[])
    assert "I2" in _codes(check_cell(c))


def test_i2_explicit_does_not_require_scope() -> None:
    c = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.EXPLICIT,
        absence_evidence=[{"source_scope": SourceScope.PARTIAL.value}],
    )
    assert check_cell(c) == []


def test_i2b_parameter_reference_requires_attestation() -> None:
    c = _cell(
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.PARAMETER_REFERENCE,
        absence_evidence=[{"source_scope": SourceScope.EXHAUSTIVE.value}],
    )
    assert "I2b" in _codes(check_cell(c))


def test_i2b_parameter_reference_pass_with_attestation() -> None:
    c = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.PARAMETER_REFERENCE,
        absence_evidence=[
            {
                "source_scope": SourceScope.EXHAUSTIVE.value,
                "scope_attested_by": "reviewer",
            }
        ],
    )
    assert check_cell(c) == []


def test_i2b_registry_absence_exhaustive_pass() -> None:
    c = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
        absence_evidence=[{"source_scope": SourceScope.EXHAUSTIVE.value}],
    )
    assert check_cell(c) == []


def test_i2b_registry_absence_non_exhaustive_fails() -> None:
    c = _cell(
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
        absence_evidence=[{"source_scope": SourceScope.PARTIAL.value}],
    )
    assert "I2b" in _codes(check_cell(c))


# ---------------------------------------------------------------------------
# I4 / I13 / I7
# ---------------------------------------------------------------------------


def test_i4_failed_with_status() -> None:
    c = _cell(state=CellState.FAILED, status=Status.OK)
    assert "I4" in _codes(check_cell(c))


def test_i4_failed_null_status_pass() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.FAILED, status=None,
    )
    assert check_cell(c) == []


def test_i13_preliminary_with_status() -> None:
    c = _cell(state=CellState.PRELIMINARY, status=Status.OK)
    assert "I13" in _codes(check_cell(c))


def test_i13_preliminary_null_status_pass() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.PRELIMINARY, status=None,
    )
    assert check_cell(c) == []


def test_i7_na_without_reviewer() -> None:
    c = _cell(
        state=CellState.EVALUATED,
        status=Status.NOT_APPLICABLE,
        reviewed_by=None,
    )
    assert "I7" in _codes(check_cell(c))


def test_i7_na_with_reviewer_pass() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.EVALUATED, status=Status.NOT_APPLICABLE,
        reviewed_by="human",
    )
    assert check_cell(c) == []


# ---------------------------------------------------------------------------
# I10 / I19 / I22
# ---------------------------------------------------------------------------


def test_i10_missing_source_id() -> None:
    ev = _evidence(source_id="")
    assert "I10" in _codes(check_evidence(ev))


def test_i19_corpus_without_content_hash() -> None:
    ev = _evidence(source_kind=SourceKind.DOCUMENTATION, content_hash=None)
    assert "I19" in _codes(check_evidence(ev))


def test_i19_manual_not_corpus() -> None:
    ev = _evidence(
        source_kind=SourceKind.MANUAL,
        content_hash=None,
        citation_quote="verbatim",
    )
    assert "I19" not in _codes(check_evidence(ev))


def test_i22_direct_without_quote() -> None:
    ev = _evidence(evidence_type=EvidenceType.DIRECT, citation_quote=None)
    assert "I22" in _codes(check_evidence(ev))


def test_i22_whitespace_quote_is_empty() -> None:
    ev = _evidence(evidence_type=EvidenceType.DIRECT, citation_quote="   ")
    assert "I22" in _codes(check_evidence(ev))


def test_i22_pass_with_quote() -> None:
    ev = CellEvidence(
        evidence_id="ev",
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        source_kind=SourceKind.DOCUMENTATION,
        evidence_type=EvidenceType.DIRECT,
        source_id="chunk_1",
        content_hash="h1",
        citation_quote="verbatim quote",
    )
    assert check_evidence(ev) == []


def test_i22_manual_exempt() -> None:
    ev = _evidence(
        source_kind=SourceKind.MANUAL,
        evidence_type=EvidenceType.DIRECT,
        citation_quote=None,
    )
    assert "I22" not in _codes(check_evidence(ev))


def test_check_evidence_accepts_cell_context() -> None:
    ev = _evidence(source_kind=SourceKind.MANUAL, citation_quote=None)
    cell = _cell(state=CellState.EVALUATED, status=Status.OK)
    _ = check_evidence(ev, cell)  # контекст не меняет результат


# ---------------------------------------------------------------------------
# I8 (наследование)
# ---------------------------------------------------------------------------


def test_i8_blocked_cell_with_inherited_status() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.EVALUATED, status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[LLMEvidence(source_id="c1", quote="q")],
    )
    assert "I8" in _codes(check_inheritance(c, _baseline(InheritanceMode.BLOCKED)))


def test_i8_blocked_non_inherited_cell_pass() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.EVALUATED, status=Status.OK,
        source_kind=SourceKind.DOCUMENTATION,
        evidence=[LLMEvidence(source_id="c1", quote="q")],
    )
    assert check_inheritance(c, _baseline(InheritanceMode.BLOCKED)) == []


def test_i8_direct_inherited_without_derived() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.EVALUATED, status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[LLMEvidence(source_id="c1", quote="q")],
    )
    result = check_inheritance(c, _baseline(InheritanceMode.DIRECT), evidence=[])
    assert "I8" in _codes(result)


def test_i8_direct_inherited_no_evidence_argument_fails_closed() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.EVALUATED, status=Status.PARTIAL,
        source_kind=SourceKind.INHERITANCE,
        evidence=[LLMEvidence(source_id="c1", quote="q")],
    )
    assert "I8" in _codes(check_inheritance(c, _baseline(InheritanceMode.DIRECT)))


def test_i8_direct_inherited_with_derived_pass() -> None:
    c = Cell(
        run_id=_RUN, product_id=_PRODUCT, param_id=_PARAM,
        state=CellState.EVALUATED, status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[LLMEvidence(source_id="c1", quote="q")],
    )
    derived = CellEvidence(
        evidence_id="d1",
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id="upstream://bl#param",
        source_url="https://upstream.example/x",
        content_hash="hh",
    )
    assert check_inheritance(c, _baseline(InheritanceMode.DIRECT), [derived]) == []


# ---------------------------------------------------------------------------
# check_derived_evidence (K5)
# ---------------------------------------------------------------------------


def _upstream_ref(content_hash: str | None = "hh") -> UpstreamRef:
    return UpstreamRef(
        product_id="up",
        param_id=_PARAM,
        source_id="upstream://bl#param",
        source_url="https://upstream.example/x",
        content_hash=content_hash,
    )


def test_derived_evidence_happy() -> None:
    ev = CellEvidence(
        evidence_id="d1",
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id="upstream://bl#param",
        source_url="https://upstream.example/x",
        content_hash="hh",
    )
    assert check_derived_evidence(ev, _upstream_ref()) == []


def test_derived_evidence_missing_hash() -> None:
    ev = CellEvidence(
        evidence_id="d1",
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id="upstream://bl#param",
        source_url="https://upstream.example/x",
        content_hash=None,
    )
    assert "I19" in _codes(check_derived_evidence(ev, _upstream_ref(content_hash=None)))


def test_derived_evidence_hash_mismatch() -> None:
    ev = CellEvidence(
        evidence_id="d1",
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id="upstream://bl#param",
        source_url="https://upstream.example/x",
        content_hash="one",
    )
    assert "I19" in _codes(check_derived_evidence(ev, _upstream_ref(content_hash="two")))


def test_derived_evidence_satellite_documentation_path() -> None:
    ev = CellEvidence(
        evidence_id="d1",
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        source_kind=SourceKind.DOCUMENTATION,
        evidence_type=EvidenceType.DERIVED,
        source_id="chunk_sat_1",
        source_url="https://sat.example/doc",
        content_hash="hh",
    )
    assert check_derived_evidence(ev, _upstream_ref()) == []


def test_derived_evidence_non_derived_type_fails_i1() -> None:
    ev = _evidence(
        evidence_type=EvidenceType.DIRECT,
        source_kind=SourceKind.INHERITANCE,
        source_url="https://upstream.example/x",
    )
    assert "I1" in _codes(check_derived_evidence(ev, _upstream_ref()))


def test_derived_evidence_bad_source_kind_fails_i8() -> None:
    ev = _evidence(
        evidence_type=EvidenceType.DERIVED,
        source_kind=SourceKind.WEB,
        source_url="https://upstream.example/x",
    )
    assert "I8" in _codes(check_derived_evidence(ev, _upstream_ref()))


# ---------------------------------------------------------------------------
# I9 (сателлиты)
# ---------------------------------------------------------------------------


def test_i9_established_without_relationship_evidence() -> None:
    sat = Satellite.model_construct(role="role-a", state=SatelliteState.APPROVED)
    assert "I9" in _codes(check_satellite(sat))


def test_i9_established_missing_component() -> None:
    sat = Satellite.model_construct(
        role="role-a",
        state=SatelliteState.EDITED_BY_HUMAN,
        component=None,
        origin=Origin.THIRD_PARTY,
        relationship=Relationship.INTEGRATED,
        evidence_state=EvidenceState.VERIFIED,
        relationship_evidence=Citation(url="https://x", quote="q"),
    )
    assert "I9" in _codes(check_satellite(sat))


def test_i9_established_happy() -> None:
    sat = Satellite(
        role="role-a",
        state=SatelliteState.APPROVED,
        component="comp",
        origin=Origin.THIRD_PARTY,
        relationship=Relationship.INTEGRATED,
        evidence_state=EvidenceState.VERIFIED,
        relationship_evidence=Citation(url="https://x", quote="q"),
    )
    assert check_satellite(sat) == []


def test_i9_draft_without_requirement() -> None:
    sat = Satellite(role="role-a", state=SatelliteState.DRAFT)
    assert check_satellite(sat) == []


# ---------------------------------------------------------------------------
# Property: функции тотальны и не возбуждают исключений
# ---------------------------------------------------------------------------


@given(
    st.sampled_from(list(CellState)),
    st.sampled_from([None, *list(Status)]),
    st.sampled_from(list(AbsenceRuleType)),
)
def test_check_cell_total(
    state: CellState, status: Status | None, rule: AbsenceRuleType
) -> None:
    c = _cell(state=state, status=status, absence_rule=rule)
    result = check_cell(c)
    assert isinstance(result, list)
    assert all(isinstance(v, Violation) for v in result)


@given(
    st.sampled_from(list(SourceKind)),
    st.sampled_from(list(EvidenceType)),
    st.text(max_size=8),
)
def test_check_evidence_total(
    source_kind: SourceKind, evidence_type: EvidenceType, quote: str
) -> None:
    ev = _evidence(
        source_kind=source_kind,
        evidence_type=evidence_type,
        citation_quote=quote or None,
    )
    result = check_evidence(ev)
    assert all(isinstance(v, Violation) for v in result)
    # I22 инвариантен к непустой непробельной цитате для direct-претензии.
    if evidence_type is EvidenceType.DIRECT and quote.strip():
        assert "I22" not in _codes(result)
