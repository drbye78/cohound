"""Тесты реестровой волны Ш3 (W6/T6.3): покрытие, R1, absence, изоляция, детерминизм.

Нейтральные фикстуры (``p1/p2``, ``param_a``, ``mod_a``) под ``tests/pipeline/fixtures/``.
Реестровые фетчеры — структурные заглушки, без сети/I/O.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    CatalogRow,
    Cell,
    CellEvidence,
    CellState,
    Cohort,
    CohortEntry,
    RegistryFact,
    SourceKind,
    SourceScope,
    Status,
    check_cell,
    check_evidence,
)
from core.pipeline import RunPlan
from core.pipeline.registry import (
    registry_covered_params,
    registry_wave,
    resolve_registry_ids,
)

RUN_ID = "run-fixture"
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"

# --------------------------------------------------------------------------- #
# Нейтральные фикстуры
# --------------------------------------------------------------------------- #


def _load_fixture() -> dict:
    return json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))


def _catalog() -> list[CatalogRow]:
    return [CatalogRow.model_validate(row) for row in _load_fixture()["catalog"]]


def _row(param_id: str) -> CatalogRow:
    return next(row for row in _catalog() if row.param_id == param_id)


def _plan(rows: list[CatalogRow] | None = None, products: tuple[str, ...] = ("p1", "p2")) -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
        params=list(rows if rows is not None else _catalog()),
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
                ),
                CohortEntry(
                    product_id="p2",
                    role_overlap=0.5,
                    selection_reason="analog",
                    anchor_relation=AnchorRelation.ANALOG,
                ),
            ],
        ),
        golden=[],
    )


def _fact(
    product_id: str = "p1",
    registry_id: str = "reg_x",
    url: str = "https://reg.test/entry/p1",
    fact_json: str = '{"present": true}',
) -> RegistryFact:
    return RegistryFact(
        run_id=RUN_ID,
        product_id=product_id,
        registry_id=registry_id,
        fact_json=fact_json,
        source_url=url,
        fetched_at=TS,
    )


class _StubFetcher:
    """Структурный реестровый фетчер: responder(product_id, param_id) -> [RegistryFact]."""

    def __init__(self, registry_id: str, responder=None, error: Exception | None = None) -> None:
        self.registry_id = registry_id
        self._responder = responder or (lambda product_id, param_id: [])
        self._error = error
        self.calls: list[tuple[str, str, str]] = []

    def fetch(self, product, scope):  # noqa: ANN001 — структурная заглушка порта
        self.calls.append((product.product_id, scope["param_id"], scope["query"]))
        if self._error is not None:
            raise self._error
        return list(self._responder(product.product_id, scope["param_id"]))


class _RecordingStore:
    """Минимальный store-дублёр: копит ячейки/evidence/факты (duck-typed StorePort)."""

    def __init__(self) -> None:
        self.cells: list[Cell] = []
        self.evidence: list[CellEvidence] = []
        self.registry_facts: list[RegistryFact] = []

    def save_cell(self, cell: Cell) -> None:
        self.cells.append(cell)

    def add_evidence(self, ev: CellEvidence) -> None:
        self.evidence.append(ev)

    def save_registry_fact(self, rf: RegistryFact) -> None:
        self.registry_facts.append(rf)


# --------------------------------------------------------------------------- #
# Покрытие
# --------------------------------------------------------------------------- #


def test_resolve_registry_ids_from_fetchers() -> None:
    assert resolve_registry_ids(_plan(), {}) == []
    ids = resolve_registry_ids(_plan(), {"reg_b": _StubFetcher("reg_b"), "reg_a": _StubFetcher("reg_a")})
    assert ids == ["reg_a", "reg_b"]


def test_covered_params_require_role_binding_and_registries() -> None:
    rows = _catalog()
    # Без реестров — никто не покрыт (даже role-bound).
    assert registry_covered_params(_plan(rows), []) == []
    covered = registry_covered_params(_plan(rows), ["reg_x"])
    assert [row.param_id for row in covered] == ["param_a", "param_b"]  # param_c без роли


def test_wave_produces_cells_only_for_covered_params() -> None:
    fetcher = _StubFetcher("reg_x", responder=lambda product_id, param_id: [_fact(product_id)])
    result = registry_wave(_plan(), {"reg_x": fetcher})
    assert result.status == "ok" and result.phase == "registry"
    assert {cell.param_id for cell in result.cells} == {"param_a", "param_b"}
    # param_c не покрыт — запрос по нему фетчеру не отправлялся.
    assert all(param_id != "param_c" for _pid, param_id, _q in fetcher.calls)
    # все ячейки — registry, evaluated ✅
    assert all(c.source_kind is SourceKind.REGISTRY for c in result.cells)
    assert all(c.state is CellState.EVALUATED and c.status is Status.OK for c in result.cells)


# --------------------------------------------------------------------------- #
# R1
# --------------------------------------------------------------------------- #


def test_r1_missing_source_url_is_rejected_and_flagged() -> None:
    bad = RegistryFact.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        registry_id="reg_x",
        fact_json='{"present": true}',
        source_url="",
        fetched_at=TS,
    )
    fetcher = _StubFetcher("reg_x", responder=lambda product_id, param_id: [bad])
    result = registry_wave(_plan(), {"reg_x": fetcher}, store=_RecordingStore())
    assert result.cells == []
    assert any("R1" in err for err in result.errors)


def test_r1_valid_facts_carry_source_url_in_evidence() -> None:
    store = _RecordingStore()
    fetcher = _StubFetcher(
        "reg_x",
        responder=lambda product_id, param_id: [
            _fact(product_id, url=f"https://reg.test/{product_id}/{param_id}")
        ],
    )
    registry_wave(_plan(), {"reg_x": fetcher}, store=store)
    assert store.evidence, "валидные факты обязаны стать CellEvidence"
    for ev in store.evidence:
        assert ev.source_kind is SourceKind.REGISTRY
        assert ev.source_url, "R1: source_url обязателен"
        assert ev.source_id
        assert check_evidence(ev) == []  # R1 + I10/I22 чисто


# --------------------------------------------------------------------------- #
# Реестровая пустота (❌ exhaustive)
# --------------------------------------------------------------------------- #


def test_registry_absence_yields_absent_exhaustive_for_registry_rule() -> None:
    fetcher = _StubFetcher("reg_x", responder=lambda product_id, param_id: [])
    result = registry_wave(_plan(), {"reg_x": fetcher})

    by_param = {cell.param_id: cell for cell in result.cells}
    # param_a объявлен absence_rule=registry_absence → авторитетное ❌.
    absent = by_param["param_a"]
    assert absent.status is Status.ABSENT
    assert absent.absence_rule is AbsenceRuleType.REGISTRY_ABSENCE
    assert absent.absence_evidence[0]["source_scope"] == SourceScope.EXHAUSTIVE.value
    assert check_cell(absent) == []
    # param_b объявлен absence_rule=explicit → реестр не даёт права на ❌.
    assert "param_b" not in by_param


# --------------------------------------------------------------------------- #
# Изоляция и детерминизм
# --------------------------------------------------------------------------- #


def test_failing_fetcher_is_isolated() -> None:
    boom = _StubFetcher("reg_boom", error=RuntimeError("registry down"))
    good = _StubFetcher(
        "reg_good", responder=lambda product_id, param_id: [_fact(product_id, registry_id="reg_good")]
    )
    result = registry_wave(_plan(), {"reg_boom": boom, "reg_good": good})
    # Сбой одного реестра не отменяет обработку остальных.
    assert result.cells
    assert any("reg_boom" in err for err in result.errors)


def test_wave_is_deterministic_and_sorted() -> None:
    def _run() -> list[tuple[str, str, Status | None]]:
        fetcher = _StubFetcher(
            "reg_x",
            responder=lambda product_id, param_id: [
                _fact(product_id, url=f"https://reg.test/{product_id}/{param_id}")
            ],
        )
        result = registry_wave(_plan(), {"reg_x": fetcher})
        return [(c.product_id, c.param_id, c.status) for c in result.cells]

    first, second = _run(), _run()
    assert first == second
    assert first == sorted(first)


def test_wave_writes_to_store_and_returns_cells() -> None:
    store = _RecordingStore()
    fetcher = _StubFetcher("reg_x", responder=lambda product_id, param_id: [_fact(product_id)])
    result = registry_wave(_plan(rows=[_row("param_a")]), {"reg_x": fetcher}, store=store)
    assert result.cells == store.cells
    assert store.registry_facts
    assert store.evidence


def test_fetchers_must_be_mapping() -> None:
    with pytest.raises(ValueError):
        registry_wave(_plan(), [_StubFetcher("reg_x")])  # type: ignore[arg-type]


def test_no_fetchers_returns_empty_ok() -> None:
    result = registry_wave(_plan(), {})
    assert result.status == "ok"
    assert result.cells == []
