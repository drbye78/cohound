"""Тесты чистых инвариантов домена (W2/T2.2, §2.3).

Проверяют, что ``core/domain/invariants.py`` возвращает типизированные ``Violation``
вместо исключений. Для сценариев-нарушений модели собираются через ``model_construct``
(в обход Pydantic-валидатора, который сам запрещает такие объекты) — именно поэтому
инварианты и вынесены в чистые функции.
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from core.domain.enums import (
    AbsenceRuleType,
    CellState,
    DenialStrength,
    EvidenceState,
    EvidenceType,
    InheritanceMode,
    Origin,
    Relationship,
    SatelliteState,
    SourceKind,
    Status,
    TerminationReason,
)
from core.domain.invariants import (
    EXHAUSTION_CONTOURS,
    REQUIRED_EXHAUSTION_CONTOURS,
    check_cell,
    check_derived_evidence,
    check_evidence,
    check_inheritance,
    check_satellite,
    check_unknown_exhaustion,
)
from core.domain.models import (
    BaselineRow,
    Cell,
    CellEvidence,
    Citation,
    EvidenceScope,
    LLMEvidence,
    ProtocolExhaustion,
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
# I2 / I2b (Вариант 2): сила отрицания вместо булева «исчерпан ли охват»
# ---------------------------------------------------------------------------


def test_i2_absent_without_denial_scope() -> None:
    """``❌`` без объекта силы отрицания — вердикт без основания."""
    c = _cell(state=CellState.EVALUATED, status=Status.ABSENT, absence_evidence=[])
    assert "I2" in _codes(check_cell(c))


def test_i2_explicit_scope_with_quote_passes() -> None:
    """explicit с цитатой — достаточно; принятие риска не требуется."""
    c = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.EXPLICIT,
        evidence_scope=EvidenceScope(
            denial_strength=DenialStrength.EXPLICIT,
            citation_quote="возможность не поддерживается",
        ),
    )
    assert check_cell(c) == []


def test_i2b_explicit_scope_without_quote_is_refused() -> None:
    """explicit без цитаты — отказ: отрицание без цитаты это мнение, а не доказательство."""
    with pytest.raises(ValueError, match="explicit"):
        EvidenceScope(denial_strength=DenialStrength.EXPLICIT)
    c = _cell(
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        evidence_scope=EvidenceScope.model_construct(
            denial_strength=DenialStrength.EXPLICIT, citation_quote=None
        ),
    )
    assert "I2b" in _codes(check_cell(c))


def test_i2_not_stated_scope_cannot_carry_absent() -> None:
    """``❓``-класс не даёт ``❌``: незнание не публикуется как отсутствие."""
    with pytest.raises(ValueError, match="I2"):
        Cell(
            run_id=_RUN,
            product_id=_PRODUCT,
            param_id=_PARAM,
            state=CellState.EVALUATED,
            status=Status.ABSENT,
            evidence_scope=EvidenceScope(
                denial_strength=DenialStrength.NOT_STATED, reason="нет дословного"
            ),
        )


def test_i2b_bounded_requires_scope_acceptance() -> None:
    """bounded — измерение не покрывает пространство, поэтому риск принимает человек."""
    with pytest.raises(ValueError, match="scope_acceptance"):
        EvidenceScope(
            denial_strength=DenialStrength.BOUNDED, reason="конечный справочник"
        )
    c = _cell(
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        evidence_scope=EvidenceScope.model_construct(
            denial_strength=DenialStrength.BOUNDED, reason="конечный справочник"
        ),
    )
    assert "I2b" in _codes(check_cell(c))


def test_i2b_parameter_reference_bounded_with_acceptance_passes() -> None:
    c = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.PARAMETER_REFERENCE,
        evidence_scope=EvidenceScope(
            denial_strength=DenialStrength.BOUNDED,
            reason="справочник полон, записи нет",
            scope_acceptance="reviewer",
        ),
    )
    assert check_cell(c) == []


def test_i2_authoritative_registry_absence_needs_no_acceptance() -> None:
    """authoritative без подписи — ``❌``: авторитетная энумерация не требует риска."""
    c = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
        absence_evidence=[{"registry_ids": ["reg-1"]}],
    )
    assert c.evidence_scope is not None
    assert c.evidence_scope.denial_strength is DenialStrength.AUTHORITATIVE
    assert c.evidence_scope.scope_acceptance is None
    assert check_cell(c) == []


def test_i2_authoritative_cannot_be_declared_unenumerated() -> None:
    with pytest.raises(ValueError, match="enumerated"):
        EvidenceScope(denial_strength=DenialStrength.AUTHORITATIVE, enumerated=False)


def test_i4_failed_carries_instrument_failed_only() -> None:
    """Сбой прибора — ``instrument_failed``; отрицание в ``failed`` недопустимо."""
    ok = Cell(
        run_id=_RUN,
        product_id=_PRODUCT,
        param_id=_PARAM,
        state=CellState.FAILED,
        status=None,
        evidence_scope=EvidenceScope.for_instrument_failure("evaluation_failed"),
    )
    assert check_cell(ok) == []
    with pytest.raises(ValueError, match="I4"):
        Cell(
            run_id=_RUN,
            product_id=_PRODUCT,
            param_id=_PARAM,
            state=CellState.FAILED,
            status=None,
            evidence_scope=EvidenceScope(
                denial_strength=DenialStrength.NOT_STATED, reason="нет"
            ),
        )


def test_i2_vector_instead_of_scalar() -> None:
    """Вектор охвата: факты наблюдаемы, доля названа мерой URL, вероятности нет."""
    scope = EvidenceScope(
        denial_strength=DenialStrength.UNOBSERVED,
        reason="robots_disallow",
        observed=17,
        expected=28,
        blocked_by_reason={"robots_disallow": 11},
        unreadable_sources=["closed.example"],
        termination="results_exhausted",
    )
    assert scope.observed == 17 and scope.expected == 28
    assert scope.blocked_by_reason == {"robots_disallow": 11}
    assert scope.url_observation_ratio == pytest.approx(17 / 28)
    assert not hasattr(scope, "absence_probability")
    # «0 из 0» — отсутствие знаменателя, а не единица.
    assert (
        EvidenceScope(
            denial_strength=DenialStrength.NOT_STATED, reason="x"
        ).url_observation_ratio
        is None
    )


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


# ---------------------------------------------------------------------------
# I3 (Вариант 2): ``❓`` обосновывается классом отрицания и причиной
# ---------------------------------------------------------------------------


def _exhaustion(**kw: Any) -> ProtocolExhaustion:
    base: dict[str, Any] = {
        "run_id": _RUN,
        "product_id": _PRODUCT,
        "param_id": _PARAM,
        "contours": list(EXHAUSTION_CONTOURS),
        "queries": ["q1"],
        "rounds": 1,
        "budget": {},
        "termination_reason": TerminationReason.RESULTS_EXHAUSTED,
    }
    base.update(kw)
    return ProtocolExhaustion.model_construct(**base)


def _unknown_cell(**kw: Any) -> Cell:
    base: dict[str, Any] = {
        "run_id": _RUN,
        "product_id": _PRODUCT,
        "param_id": _PARAM,
        "state": CellState.EVALUATED,
        "status": Status.UNKNOWN,
    }
    base.update(kw)
    return Cell.model_construct(**base)


def test_exhaustion_contours_are_provenance_not_invariant() -> None:
    """Перечень контуров остаётся справочником наблюдаемого, а не требованием."""
    assert EXHAUSTION_CONTOURS == ("corpus", "serp", "registry", "inheritance")
    assert REQUIRED_EXHAUSTION_CONTOURS == EXHAUSTION_CONTOURS


def test_check_unknown_exhaustion_legacy_cell_without_scope() -> None:
    """Историческая строка без объекта: прежнее правило по протоколу (читаемость)."""
    assert "I3" in _codes(check_unknown_exhaustion(_unknown_cell(), None))
    assert check_unknown_exhaustion(_unknown_cell(), _exhaustion()) == []


def test_check_unknown_exhaustion_not_stated_with_reason_passes() -> None:
    """Новый путь: причина незнания — в объекте, протокол не нужен вовсе."""
    cell = _unknown_cell(
        evidence_scope=EvidenceScope(
            denial_strength=DenialStrength.NOT_STATED,
            reason="материал релевантен, дословного утверждения нет",
        )
    )
    assert check_unknown_exhaustion(cell, None) == []


def test_check_unknown_exhaustion_requires_reason() -> None:
    cell = _unknown_cell(
        evidence_scope=EvidenceScope.model_construct(
            denial_strength=DenialStrength.NOT_STATED, reason=None
        )
    )
    violations = check_unknown_exhaustion(cell, None)
    assert "I3" in _codes(violations)


def test_check_unknown_exhaustion_contract_class_is_not_enough() -> None:
    """Класс ``❌`` при статусе ``❓`` — противоречие, а не основание."""
    cell = _unknown_cell(
        evidence_scope=EvidenceScope.model_construct(
            denial_strength=DenialStrength.EXPLICIT, citation_quote="q"
        )
    )
    assert "I3" in _codes(check_unknown_exhaustion(cell, None))


def test_check_unknown_exhaustion_human_review_satisfies_i3() -> None:
    assert check_unknown_exhaustion(_unknown_cell(reviewed_by="human"), None) == []


def test_absence_rule_none_does_not_fabricate_scope() -> None:
    """``absence_rule=None`` (legacy-строка) не достраивает класс отрицания.

    Прежде чтение подставляло ``explicit``, то есть прочитанная ячейка заявляла правило,
    которого каталог не объявлял. Теперь «правило не записано» — это отсутствие правила:
    реестровый автодострой срабатывает только на **объявленном** ``registry_absence``.
    """
    cell = Cell.model_construct(
        run_id="r1",
        product_id="p",
        param_id="param",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=None,
        evidence=[],
        absence_evidence=[{"registry_ids": ["reg-1"]}],
        evidence_scope=None,
    )
    # I2: нет класса отрицания — «нет» не публикуется; выдуманного правила не появилось.
    assert cell.evidence_scope is None
    assert "I2" in _codes(check_cell(cell))


def test_absence_rule_none_allows_presence_cells() -> None:
    """Наличие-ячейка с незаписанным правилом валидна: правило к вердикту не относится."""
    cell = Cell(
        run_id="r1",
        product_id="p",
        param_id="param",
        state=CellState.EVALUATED,
        status=Status.OK,
        absence_rule=None,
        evidence=[LLMEvidence(source_id="c1", quote="q")],
    )
    assert check_cell(cell) == []
