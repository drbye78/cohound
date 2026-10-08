"""coHound — Р-2: маппинг сателлита на зачёт (§4.1).

Чистая функция над перечислениями. Порядок ветвей важен, первое совпадение решает. Нейтрален.
"""

from __future__ import annotations

from core.domain.enums import Cap, EvidenceState, Origin, Relationship


def apply_r2(
    origin: Origin,
    relationship: Relationship,
    evidence_state: EvidenceState,
    parameter_rule_cap: Cap = Cap.OK,
) -> Cap:
    """Маппинг сателлита на зачёт (Р-2, §4.1). Порядок важен, первое совпадение решает."""
    if evidence_state is EvidenceState.CLAIMED:
        return Cap.PARTIAL
    if origin is Origin.COMMUNITY and relationship is Relationship.EXTERNAL:
        return Cap.NONE
    if relationship in {
        Relationship.BUNDLED,
        Relationship.INTEGRATED,
        Relationship.CERTIFIED,
    } and evidence_state is EvidenceState.VERIFIED:
        return Cap.OK
    if relationship is Relationship.RECOMMENDED and evidence_state is EvidenceState.VERIFIED:
        return Cap.PARTIAL
    if relationship is Relationship.EXTERNAL and evidence_state is EvidenceState.VERIFIED:
        return parameter_rule_cap
    return parameter_rule_cap
