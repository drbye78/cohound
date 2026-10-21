"""Тесты канонических дайджестов идентичности прогона (09 §6.2/§12.2/§12.5).

Офлайн, без I/O: только доменные модели и чистые функции ``core.pipeline.digests``.
Проверяется стабильность, чувствительность к каждому включённому полю и
нечувствительность к авторскому тексту/порядку.
"""

from __future__ import annotations

import datetime as _dt

import pytest

from core.domain.enums import (
    AnchorRelation,
    OnboardingState,
    SeedRole,
    SeedSource,
    SeedState,
)
from core.domain.models import Cohort, CohortEntry, ProductSeeds, SeedEntry, SeedsConfig
from core.pipeline.digests import (
    EXECUTION_CONTRACT_FIELDS,
    cohort_digest,
    execution_contract_digest,
    seeds_digest,
)


def _entry(
    product_id: str = "p1",
    *,
    anchor_relation: AnchorRelation = AnchorRelation.ANCHOR,
    role_overlap: float = 1.0,
    selection_reason: str | None = "якорь",
) -> CohortEntry:
    return CohortEntry(
        product_id=product_id,
        role_overlap=role_overlap,
        selection_reason=selection_reason,
        anchor_relation=anchor_relation,
    )


def _cohort(
    *entries: CohortEntry,
    case_id: str = "case_x",
    frozen_at: _dt.date = _dt.date(2026, 6, 1),
) -> Cohort:
    rows = list(entries) or [_entry()]
    return Cohort(case_id=case_id, frozen_at=frozen_at, entries=rows)


def _seed(
    *,
    product_id: str = "p1",
    url: str = "https://p1.test/",
    role: SeedRole = SeedRole.PRIMARY,
    profile_key: str | None = "p1.test",
    enabled: bool = True,
) -> tuple[str, SeedEntry]:
    state = SeedState.ENABLED if enabled else SeedState.DISABLED
    entry = SeedEntry(
        url=url,
        normalized_url=url,
        role=role,
        source=SeedSource.OPERATOR,
        profile_key=profile_key,
        added_at=_dt.date(2026, 6, 1),
        added_by="test",
        enabled=enabled,
        state=state,
    )
    return product_id, entry


def _seeds(*rows: tuple[str, SeedEntry]) -> SeedsConfig:
    products: dict[str, ProductSeeds] = {}
    for product_id, seed in rows:
        products.setdefault(product_id, ProductSeeds(state=OnboardingState.PENDING_SEEDS, seeds=[]))
        products[product_id].seeds.append(seed)
    for product_id, product in products.items():
        products[product_id] = ProductSeeds(
            state=(
                OnboardingState.READY
                if any(seed.is_enabled for seed in product.seeds)
                else OnboardingState.PENDING_SEEDS
            ),
            seeds=list(product.seeds),
        )
    return SeedsConfig(products=products)


# ---------------------------------------------------------------------------
# cohort_digest
# ---------------------------------------------------------------------------


def test_cohort_digest_is_stable_and_short() -> None:
    cohort = _cohort(_entry("p1"), _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5))
    first = cohort_digest(cohort, case_id="case_x", cohort_ref="cohort-r1")
    second = cohort_digest(cohort, case_id="case_x", cohort_ref="cohort-r1")
    assert first == second
    assert len(first) == 24


def test_cohort_digest_ignores_selection_reason_and_order() -> None:
    left = _cohort(
        _entry("p1", selection_reason="якорь"),
        _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5, selection_reason="аналог"),
    )
    right = _cohort(
        _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5, selection_reason="другой текст"),
        _entry("p1", selection_reason="и ещё один"),
    )
    assert cohort_digest(left, case_id="case_x") == cohort_digest(right, case_id="case_x")


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (
            _cohort(_entry("p1")),
            _cohort(_entry("p1", role_overlap=0.9)),
        ),
        (
            _cohort(_entry("p1"), _entry("p2", anchor_relation=AnchorRelation.ANALOG)),
            _cohort(_entry("p1", anchor_relation=AnchorRelation.ANALOG), _entry("p2")),
        ),
        (
            _cohort(_entry("p1")),
            _cohort(_entry("p1"), _entry("p2", anchor_relation=AnchorRelation.ANALOG, role_overlap=0.5)),
        ),
        (
            _cohort(_entry("p1"), frozen_at=_dt.date(2026, 6, 1)),
            _cohort(_entry("p1"), frozen_at=_dt.date(2026, 6, 2)),
        ),
    ],
)
def test_cohort_digest_is_sensitive_to_material(left: Cohort, right: Cohort) -> None:
    assert cohort_digest(left, case_id="case_x") != cohort_digest(right, case_id="case_x")


def test_cohort_digest_is_sensitive_to_case_id_and_cohort_ref() -> None:
    cohort = _cohort(_entry("p1"))
    base = cohort_digest(cohort, case_id="case_x", cohort_ref="r1")
    assert cohort_digest(cohort, case_id="case_y", cohort_ref="r1") != base
    assert cohort_digest(cohort, case_id="case_x", cohort_ref="r2") != base
    assert cohort_digest(cohort, case_id="case_x", cohort_ref=None) != base


# ---------------------------------------------------------------------------
# seeds_digest
# ---------------------------------------------------------------------------


def test_seeds_digest_is_stable_and_ignores_order() -> None:
    seed_a = _seed(product_id="p1", url="https://p1.test/")
    seed_b = _seed(product_id="p2", url="https://p2.test/", role=SeedRole.DOCS)
    first = seeds_digest(_seeds(seed_a, seed_b))
    second = seeds_digest(_seeds(seed_b, seed_a))
    assert first == second
    assert len(first) == 24


def test_seeds_digest_empty_set_is_stable_not_empty_string() -> None:
    digest = seeds_digest(SeedsConfig())
    assert digest
    assert len(digest) == 24
    assert digest == seeds_digest(SeedsConfig(products={}))


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p1", url="https://p1.test/", enabled=False)),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p1", url="https://p1.test/", role=SeedRole.DOCS)),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/", profile_key="p1.test")),
            _seeds(_seed(product_id="p1", url="https://p1.test/", profile_key="other.test")),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p1", url="https://p1.test/docs")),
        ),
        (
            _seeds(_seed(product_id="p1", url="https://p1.test/")),
            _seeds(_seed(product_id="p2", url="https://p1.test/")),
        ),
    ],
)
def test_seeds_digest_is_sensitive_to_material(left: SeedsConfig, right: SeedsConfig) -> None:
    assert seeds_digest(left) != seeds_digest(right)


# ---------------------------------------------------------------------------
# execution_contract_digest
# ---------------------------------------------------------------------------


def _contract_values() -> dict[str, object]:
    return {field: f"v-{field}" for field in EXECUTION_CONTRACT_FIELDS}


def test_execution_contract_is_stable_and_short() -> None:
    values = _contract_values()
    first = execution_contract_digest(values)
    second = execution_contract_digest(dict(reversed(list(values.items()))))
    assert first == second
    assert len(first) == 24


def test_execution_contract_includes_none_explicitly() -> None:
    values = _contract_values()
    values["chunker_version"] = None
    with_none = execution_contract_digest(values)

    other = _contract_values()
    other["chunker_version"] = "some-model"
    assert with_none != execution_contract_digest(other)

    # Пропущенный ключ — ошибка, а не «то же самое, что None»: иначе отсутствие
    # поля и явный null дали бы коллизию.
    missing = dict(values)
    del missing["chunker_version"]
    with pytest.raises(ValueError, match="отсутствуют поля"):
        execution_contract_digest(missing)


def test_execution_contract_rejects_unknown_fields() -> None:
    values = _contract_values()
    values["unexpected"] = "x"
    with pytest.raises(ValueError, match="неизвестные поля"):
        execution_contract_digest(values)


@pytest.mark.parametrize("field", EXECUTION_CONTRACT_FIELDS)
def test_execution_contract_is_sensitive_to_each_field(field: str) -> None:
    left = _contract_values()
    right = _contract_values()
    right[field] = "changed"
    assert execution_contract_digest(left) != execution_contract_digest(right)
