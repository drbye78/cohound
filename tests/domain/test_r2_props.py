"""Property-тесты Р-2 через hypothesis (W1/T1.5, §4.1).

Проверяют тотальность и ветвление ``apply_r2``: потолок зачёта определён для всех
комбинаций origin × relationship × evidence_state × parameter_rule_cap.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from core.domain import Cap, EvidenceState, Origin, Relationship, apply_r2

_origin = st.sampled_from(list(Origin))
_relationship = st.sampled_from(list(Relationship))
_evidence = st.sampled_from(list(EvidenceState))
_cap = st.sampled_from(list(Cap))


@given(_origin, _relationship, _evidence, _cap)
def test_r2_total(origin: Origin, relationship: Relationship, evidence: EvidenceState, cap: Cap) -> None:
    result = apply_r2(origin, relationship, evidence, cap)
    assert isinstance(result, Cap)


@given(_origin, _relationship, _cap)
def test_r2_claimed_always_partial(origin: Origin, relationship: Relationship, cap: Cap) -> None:
    assert apply_r2(origin, relationship, EvidenceState.CLAIMED, cap) is Cap.PARTIAL


@given(_evidence, _cap)
def test_r2_community_external_none(evidence: EvidenceState, cap: Cap) -> None:
    result = apply_r2(Origin.COMMUNITY, Relationship.EXTERNAL, evidence, cap)
    if evidence is EvidenceState.CLAIMED:
        assert result is Cap.PARTIAL
    else:
        assert result is Cap.NONE


@given(st.sampled_from([Relationship.BUNDLED, Relationship.INTEGRATED, Relationship.CERTIFIED]), _cap)
def test_r2_verified_strong_ok(relationship: Relationship, cap: Cap) -> None:
    assert apply_r2(Origin.THIRD_PARTY, relationship, EvidenceState.VERIFIED, cap) is Cap.OK


@given(_cap)
def test_r2_verified_recommended_partial(cap: Cap) -> None:
    assert apply_r2(Origin.THIRD_PARTY, Relationship.RECOMMENDED, EvidenceState.VERIFIED, cap) is Cap.PARTIAL


@given(_cap)
def test_r2_verified_external_passthrough(cap: Cap) -> None:
    assert apply_r2(Origin.THIRD_PARTY, Relationship.EXTERNAL, EvidenceState.VERIFIED, cap) is cap
