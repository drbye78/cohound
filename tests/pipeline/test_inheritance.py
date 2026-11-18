"""Тесты волны наследования Ш4 (W6/T6.4): direct/conditional/blocked, K5, П>В.

Нейтральные фикстуры (``p1/p2``, ``param_a``, ``mod_a``). Доменные объекты собираются
детерминированно; там, где модель намеренно невалидна, используется ``model_construct``.

План 27 §6.2: ``direct`` наследует **факт** (``BaselineFact``), а не паспортную цитату, и
перебирает ``Passport.upstreams`` в объявленном порядке. Поэтому каждый тест ``direct``
передаёт факт, а причины «не наследовали» проверяются по ``decision.reasons`` и
``inheritance_basis(...)["products"][pid]["not_inherited"]``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from core.domain import (
    AnchorRelation,
    BaselineFact,
    BaselineRow,
    Cell,
    CellEvidence,
    CellState,
    Citation,
    Cohort,
    CohortEntry,
    EvidenceType,
    FactReview,
    FactStatus,
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
from core.pipeline.inheritance import (
    InheritanceDecision,
    apply_precedence,
    decide_inheritance,
    inheritance_basis,
    inheritance_wave,
    major_version,
)

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
    upstreams: list[Upstream] | None = None,
) -> Passport:
    """Паспорт продукта; по умолчанию — одна объявленная линия ``base_1``."""
    if upstreams is None:
        upstreams = [
            Upstream(
                baseline_id=baseline_id,
                version=version,
                evidence=Citation(url=evidence_url, quote=quote),
            )
        ]
    return Passport(
        product_id=product_id,
        canonical_name=f"Product {product_id}",
        vendor="Vendor",
        upstreams=upstreams,
        version="1.0",
        jurisdiction="RU",
        snapshot_date=_dt.date(2026, 1, 1),
    )


def _fact(
    baseline_id: str = "base_1",
    param_id: str = "param_a",
    version: str = "16",
    **kw: Any,
) -> BaselineFact:
    """Принятый факт ``present`` о документации предка (план 27 §3.3).

    Дефолты — «факт есть, он принят человеком и несёт цитату+хеш»: без этого
    ``direct``-наследование не выпускает ячейку (I22/I19).
    """
    payload: dict[str, Any] = {
        "baseline_id": baseline_id,
        "param_id": param_id,
        "version": version,
        "status": FactStatus.PRESENT,
        "url": "https://upstream.test/doc",
        "quote": "upstream documents the mechanism",
        "content_hash": "a" * 64,
        "review": FactReview.ACCEPTED,
        "reviewed_by": "tester",
    }
    payload.update(kw)
    return BaselineFact.model_validate(payload)


def _decide(
    passport: Passport,
    baselines: list[BaselineRow],
    facts: list[BaselineFact],
    *,
    param_id: str = "param_a",
) -> InheritanceDecision:
    """Одно решение волны по параметру (прямой вызов ``decide_inheritance``)."""
    from core.pipeline.inheritance import _baseline_index, _fact_index

    [param] = _plan([param_id]).params
    return decide_inheritance(
        run_id=RUN_ID,
        product_id=passport.product_id,
        param=param,
        upstreams=list(passport.upstreams),
        baselines=_baseline_index(baselines),
        facts=_fact_index(facts),
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
    fact = _fact(param_id="param_a")
    store = _RecordingStore()
    result = inheritance_wave(
        _plan(["param_a"]),
        [baseline],
        {"p1": _passport("p1")},
        store=store,
        facts=[fact],
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
    # Цитата, URL и хеш — из факта, а не из паспортной визитки (план 27 §6.3);
    # адрес несёт версию: один параметр в v16 и v18 — два разных факта (I15).
    assert ev.source_id == "upstream://base_1#param_a@16"
    assert ev.citation_quote == fact.quote
    assert ev.source_url == fact.url
    assert ev.content_hash == fact.content_hash
    assert [item.quote for item in cell.evidence] == [fact.quote]
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
    # Модель факта уже запрещает `present` без URL (I15) — это первый рубеж.
    with pytest.raises(ValidationError, match="I15"):
        _fact(param_id="param_a", url=None)
    # Второй рубеж — инвариант волны: невалидный факт не выпускает ячейку, а называет
    # нарушение. Собирается model_construct, чтобы обойти конструктор.
    fact = BaselineFact.model_construct(
        baseline_id="base_1",
        param_id="param_a",
        version="16",
        status=FactStatus.PRESENT,
        url=None,
        quote="upstream documents the mechanism",
        content_hash="a" * 64,
        review=FactReview.ACCEPTED,
        reviewed_by="tester",
    )
    result = inheritance_wave(
        _plan(["param_a"]), [baseline], {"p1": _passport("p1")}, facts=[fact]
    )

    assert result.cells == []
    assert any("I15" in err or "наследован" in err for err in result.errors)


def test_direct_without_upstream_citation_is_rejected() -> None:
    baseline = _baselines()["param_a"]
    # Намеренно невалидный факт без цитаты (модель не допустила бы в обычной сборке):
    # I22-дисциплина — «есть в документации» без дословной цитаты недоказуемо.
    fact = BaselineFact.model_construct(
        baseline_id="base_1",
        param_id="param_a",
        version="16",
        status=FactStatus.PRESENT,
        url="https://upstream.test/doc",
        quote="",
        content_hash="a" * 64,
        review=FactReview.ACCEPTED,
        reviewed_by="tester",
    )
    result = inheritance_wave(
        _plan(["param_a"]), [baseline], {"p1": _passport("p1")}, facts=[fact]
    )

    assert result.cells == []
    assert any("цитаты" in err or "I8" in err for err in result.errors)


def test_no_upstreams_means_cross_linear_parameter() -> None:
    # Архетип B без объявленных линий: наследовать не от кого — параметр идёт корпусом.
    passport = _passport("p1", upstreams=[])
    result = inheritance_wave(
        _plan(["param_a"]), [_baselines()["param_a"]], {"p1": passport}, facts=[_fact()]
    )

    assert result.cells == []
    assert result.errors == []


# --------------------------------------------------------------------------- #
# conditional / blocked / нет строки
# --------------------------------------------------------------------------- #


def test_conditional_goes_to_pending_not_resolved() -> None:
    # ``conditional`` с **принятым** фактом: кандидат на ревью, а не статус. Без факта
    # линия не наследует вовсе (см. ``test_conditional_without_fact_does_not_cancel_far_ancestor``).
    baseline = _baselines()["param_b"]  # conditional
    result = inheritance_wave(
        _plan(["param_b"]), [baseline], {"p1": _passport("p1")}, facts=[_fact(param_id="param_b")]
    )

    [cell] = result.cells
    assert cell.state is CellState.PENDING
    assert cell.status is None  # НЕ разрешённый статус
    assert cell.review_reason  # дискриминатор причины для очереди ревью
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


# --------------------------------------------------------------------------- #
# План 27 §6.2: порядок предков, факт как носитель, версии
# --------------------------------------------------------------------------- #


def _two_upstreams(*, first: str = "base_1", second: str = "base_2") -> list[Upstream]:
    """Две объявленные линии; порядок списка — приоритет предков (§3.2)."""
    return [
        Upstream(
            baseline_id=first,
            version="16",
            evidence=Citation(url=f"https://{first}.test/doc", quote="ancestor quote"),
        ),
        Upstream(
            baseline_id=second,
            version="16",
            evidence=Citation(url=f"https://{second}.test/doc", quote="ancestor quote"),
        ),
    ]


def _direct_row(baseline_id: str, param_id: str = "param_a") -> BaselineRow:
    return BaselineRow(
        baseline_id=baseline_id,
        param_id=param_id,
        min_version=None,
        inheritance_mode=InheritanceMode.DIRECT,
    )


def _conditional_row(baseline_id: str, param_id: str = "param_a") -> BaselineRow:
    return BaselineRow(
        baseline_id=baseline_id,
        param_id=param_id,
        min_version=None,
        inheritance_mode=InheritanceMode.CONDITIONAL,
    )


def _blocked_row(baseline_id: str, param_id: str = "param_a") -> BaselineRow:
    return BaselineRow(
        baseline_id=baseline_id,
        param_id=param_id,
        min_version=None,
        inheritance_mode=InheritanceMode.BLOCKED,
    )


def test_archetype_b_falls_through_to_second_ancestor_and_names_the_first() -> None:
    # У первого предка строки baselines.csv нет (форк параметр не описывает), у второго —
    # есть и факт принят. Наследует второй, а первая линия обязана быть названа причиной.
    passport = _passport("p1", upstreams=_two_upstreams())
    baselines = [_direct_row("base_2")]
    facts = [_fact(baseline_id="base_2", param_id="param_a", quote="fork documents it")]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    [cell] = result.cells
    assert cell.status is Status.OK
    # Ячейка пришла от второго предка — версия и линия видны в адресе факта.
    assert cell.evidence[0].source_id == "upstream://base_2#param_a@16"

    decision = _decide(passport, baselines, facts)
    assert decision.cell is not None
    assert [item["reason"] for item in decision.reasons] == ["no_policy_row"]
    assert decision.reasons[0]["baseline_id"] == "base_1"

    basis = inheritance_basis(
        baselines,
        products=["p1"],
        passports={"p1": passport},
        params=_plan(["param_a"]).params,
        cells=result.cells,
        decisions=[decision],
    )
    product = basis["products"]["p1"]
    # Параметр унаследован — значит «не наследовали вовсе» тут пусто; причина по первой
    # линии публикуется отдельно, как проседание к следующему предку (§6.2).
    assert product["not_inherited"] == []
    [fallback] = product["ancestor_fallbacks"]
    assert fallback["param_id"] == "param_a"
    assert fallback["inherited"] is True
    assert [entry["reason"] for entry in fallback["reasons"]] == ["no_policy_row"]
    assert fallback["reasons"][0]["baseline_id"] == "base_1"
    assert product["fallback_counts"] == {"no_policy_row": 1}
    assert "fallback 1" in product["summary"]


def test_archetype_b_first_declared_ancestor_wins_and_order_matters() -> None:
    baselines = [_direct_row("base_1"), _direct_row("base_2")]
    facts = [
        _fact(baseline_id="base_1", quote="first ancestor documents it"),
        _fact(baseline_id="base_2", quote="second ancestor documents it"),
    ]

    first_order = _passport("p1", upstreams=_two_upstreams())
    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": first_order}, facts=facts
    )
    [cell] = result.cells
    # Победил первый объявленный предок, а не «последний записанный в файле».
    assert cell.evidence[0].source_id == "upstream://base_1#param_a@16"
    assert cell.evidence[0].quote == "first ancestor documents it"

    swapped = _passport(
        "p1", upstreams=_two_upstreams(first="base_2", second="base_1")
    )
    [swapped_cell] = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": swapped}, facts=facts
    ).cells
    # Перестановка объявленного порядка меняет победителя (пин приоритета, §3.2).
    assert swapped_cell.evidence[0].source_id == "upstream://base_2#param_a@16"
    assert swapped_cell.evidence[0].quote == "second ancestor documents it"


@pytest.mark.parametrize(
    ("review", "reason"),
    [
        (FactReview.PENDING, "baseline_fact_pending"),
        (FactReview.REJECTED, "baseline_fact_rejected"),
    ],
)
def test_unaccepted_fact_is_not_inherited(review: FactReview, reason: str) -> None:
    passport = _passport("p1")
    baselines = [_baselines()["param_a"]]
    facts = [_fact(param_id="param_a", review=review, reviewed_by="tester")]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    assert result.cells == []

    decision = _decide(passport, baselines, facts)
    assert decision.cell is None
    assert [item["reason"] for item in decision.reasons] == [reason]


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (FactStatus.ABSENT, "baseline_fact_absent"),
        (FactStatus.UNCLEAR, "baseline_fact_unclear"),
    ],
)
def test_fact_without_presence_is_not_inherited(status: FactStatus, reason: str) -> None:
    passport = _passport("p1")
    baselines = [_baselines()["param_a"]]
    # review=accepted при status≠present — изолируем причину: отказ именно по статусу.
    facts = [
        _fact(param_id="param_a", status=status, quote=None, content_hash=None)
    ]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    assert result.cells == []

    decision = _decide(passport, baselines, facts)
    assert decision.cell is None
    assert [item["reason"] for item in decision.reasons] == [reason]


def test_missing_fact_names_the_reason() -> None:
    passport = _passport("p1")
    baselines = [_baselines()["param_a"]]
    result = inheritance_wave(_plan(["param_a"]), baselines, {"p1": passport}, facts=[])
    assert result.cells == []

    decision = _decide(passport, baselines, [])
    assert [item["reason"] for item in decision.reasons] == ["baseline_fact_missing"]


def test_fact_on_declared_major_version_is_inherited() -> None:
    # Предок объявлен как 18.4.1, факт записан на мажоре 18 → наследуется (§7.2).
    passport = _passport("p1", version="18.4.1")
    baselines = [_baselines()["param_a"]]
    facts = [_fact(param_id="param_a", version="18")]

    [cell] = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    ).cells
    assert cell.status is Status.OK
    assert cell.evidence[0].source_id == "upstream://base_1#param_a@18"


def test_fact_on_older_major_version_is_not_inherited() -> None:
    # Факт есть только на v17, предок объявлен на v18: параметр в v18 исчез — регрессия
    # upstream обязана быть видна, а не замазана «где-нибудь ≤ V» (§7.2).
    passport = _passport("p1", version="18")
    baselines = [_baselines()["param_a"]]
    facts = [_fact(param_id="param_a", version="17")]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    assert result.cells == []

    decision = _decide(passport, baselines, facts)
    assert decision.cell is None
    assert [item["reason"] for item in decision.reasons] == ["baseline_fact_missing"]


def test_no_ancestor_gives_a_fact_and_both_reasons_are_published() -> None:
    # Ни один предок не даёт принятого факта: ячейки нет, и названы **обе** линии —
    # иначе «не наследовали» выглядит как забытый параметр (§6.2).
    passport = _passport("p1", upstreams=_two_upstreams())
    baselines = [_direct_row("base_1"), _direct_row("base_2")]
    facts = [
        _fact(baseline_id="base_1", review=FactReview.PENDING),
        _fact(baseline_id="base_2", status=FactStatus.ABSENT),
    ]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    assert result.cells == []

    decision = _decide(passport, baselines, facts)
    assert decision.cell is None
    assert [item["baseline_id"] for item in decision.reasons] == ["base_1", "base_2"]
    assert [item["reason"] for item in decision.reasons] == [
        "baseline_fact_pending",
        "baseline_fact_absent",
    ]

    product = inheritance_basis(
        baselines,
        products=["p1"],
        passports={"p1": passport},
        params=_plan(["param_a"]).params,
        cells=result.cells,
        decisions=[decision],
    )["products"]["p1"]
    [not_inherited] = product["not_inherited"]
    assert not_inherited["param_id"] == "param_a"
    assert not_inherited["inherited"] is False
    assert product["ancestor_fallbacks"] == []
    assert product["reason_counts"] == {
        "baseline_fact_absent": 1,
        "baseline_fact_pending": 1,
    }
    # В строке итога число — количество причин (по линиям), а не параметров.
    assert "not_inherited 2" in product["summary"]
    assert "baseline_fact_absent: 1" in product["summary"]
    assert "baseline_fact_pending: 1" in product["summary"]


def test_major_version_unit() -> None:
    assert major_version("18.4.1") == "18"
    assert major_version("18") == "18"
    assert major_version("v2.1-beta3") == "2"
    assert major_version("abc") is None
    assert major_version("") is None
    assert major_version(None) is None


def test_min_version_blocks_direct_when_upstream_too_old() -> None:
    baseline = _baselines()["param_a"].model_copy(update={"min_version": "20.0"})
    passport = _passport("p1", version="16.0")
    fact = _fact(param_id="param_a", version="16")
    result = inheritance_wave(
        _plan(["param_a"]), [baseline], {"p1": passport}, facts=[fact]
    )
    assert result.cells == []

    # Причина — порог версии, а не «факта нет»: иначе оператор пойдёт разведывать факт,
    # который уже есть.
    decision = _decide(passport, [baseline], [fact])
    assert decision.cell is None
    assert [item["reason"] for item in decision.reasons] == ["below_min_version"]


def test_conditional_without_fact_does_not_cancel_far_ancestor() -> None:
    # Ближний предок объявил параметр как ``conditional``, но факта у этой линии нет;
    # дальний объявил ``direct`` и факт есть. Условная строка «без доказательства» — это
    # «доказательства нет», и она не вправе отменить наследование от дальнего предка.
    passport = _passport("p1", upstreams=_two_upstreams())
    baselines = [_conditional_row("base_1"), _direct_row("base_2")]
    facts = [_fact(baseline_id="base_2", quote="fork documents it")]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    [cell] = result.cells
    assert cell.status is Status.OK
    assert cell.evidence[0].source_id == "upstream://base_2#param_a@16"

    decision = _decide(passport, baselines, facts)
    assert decision.inherited_from == "base_2"
    # Ближняя линия названа причиной отказа, а не тишиной.
    assert [item["baseline_id"] for item in decision.reasons] == ["base_1"]
    assert [item["reason"] for item in decision.reasons] == ["baseline_fact_missing"]


def test_conditional_with_accepted_fact_stops_the_cycle() -> None:
    # Условная строка с принятым фактом — кандидат на ревью: решает человек, и дальний
    # предок уже не рассматривается (иначе решение человека подменялось бы автоматикой).
    passport = _passport("p1", upstreams=_two_upstreams())
    baselines = [_conditional_row("base_1"), _direct_row("base_2")]
    facts = [
        _fact(baseline_id="base_1"),
        _fact(baseline_id="base_2", quote="fork documents it"),
    ]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    [cell] = result.cells
    assert cell.state is CellState.PENDING and cell.status is None

    decision = _decide(passport, baselines, facts)
    assert decision.inherited_from == "base_1"
    assert [item["reason"] for item in decision.reasons] == ["mode_conditional"]
    assert [item["baseline_id"] for item in decision.reasons] == ["base_1"]


def test_conditional_below_min_version_does_not_produce_a_candidate() -> None:
    # Порог версии проверяется раньше режима: строка ``conditional`` с ``min_version``
    # выше объявленной версии предка не даёт даже кандидата — доказательства нет.
    baseline = _baselines()["param_b"].model_copy(update={"min_version": "20.0"})
    passport = _passport("p1", version="16.0")
    fact = _fact(param_id="param_b", version="16")

    decision = _decide(passport, [baseline], [fact], param_id="param_b")
    assert decision.cell is None
    assert [item["reason"] for item in decision.reasons] == ["below_min_version"]


def test_two_ancestors_one_row_count_as_one_planned_param() -> None:
    # Два объявленных предка, у каждого одна строка на ОДИН параметр: ближний ``blocked``,
    # дальний ``direct``, факт только у дальнего. Объявленный параметр — один, выпущена
    # одна ячейка, поэтому «blocked 1 skipped» рядом с выпущенным ✅ — ложь о работе волны.
    passport = _passport("p1", upstreams=_two_upstreams())
    baselines = [_blocked_row("base_1"), _direct_row("base_2")]
    facts = [_fact(baseline_id="base_2", quote="fork documents it")]

    result = inheritance_wave(
        _plan(["param_a"]), baselines, {"p1": passport}, facts=facts
    )
    [cell] = result.cells
    assert cell.status is Status.OK

    decision = _decide(passport, baselines, facts)
    product = inheritance_basis(
        baselines,
        products=["p1"],
        passports={"p1": passport},
        params=_plan(["param_a"]).params,
        cells=result.cells,
        decisions=[decision],
    )["products"]["p1"]

    assert product["planned"] == {"direct": 1, "conditional": 0, "blocked": 0}
    assert product["released"]["direct"]["evaluated"] == 1
    # Режим выпущенной ячейки — режим **выигравшего** предка, а не строки файла.
    assert "blocked 0 skipped" in product["summary"]
    assert "blocked 1" not in product["summary"]
    assert "direct 1/1 evaluated" in product["summary"]


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


class _RegistryStore(_RecordingStore):
    """Store-дублёр, в котором реестр уже дал ячейку (высокий источник, Fork D)."""

    def __init__(self, registry: list[Cell]) -> None:
        super().__init__()
        self._registry = registry

    def iter_registry_cells(self, run_id: str) -> list[Cell]:
        return list(self._registry)


def _registry_cell(
    param_id: str = "param_a", status: Status = Status.OK
) -> Cell:
    return Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        state=CellState.EVALUATED,
        status=status,
        source_kind=SourceKind.REGISTRY,
        evidence=[],
        absence_evidence=[],
        reviewed_by=None,
    )


def test_cell_absorbed_by_registry_is_not_an_ancestor_fallback() -> None:
    # Реестр дал согласный статус ⇒ наследованной ячейке писать нечего, волна её не
    # выпускает. Параметр при этом **не наследован**: в «проседание к предку» он попасть
    # не должен (проседания не было), иначе базис завышает унаследованное.
    store = _RegistryStore([_registry_cell()])
    result = inheritance_wave(
        _plan(["param_a"]),
        [_baselines()["param_a"]],
        {"p1": _passport("p1")},
        store=store,
        facts=[_fact(param_id="param_a")],
    )

    assert result.cells == []
    assert store.cells == []
    # Evidence без ячейки не пишется: иначе прогон публикует доказательство, которого в
    # его карте нет, и цитата «подтверждает» ячейку, отсутствующую у продукта.
    assert store.evidence == []
    [note] = result.log_entries
    assert "not_inherited 1 (superseded_by_registry: 1)" in note
    assert "fallback" not in note


def test_baselines_must_be_list() -> None:
    with pytest.raises(ValueError):
        inheritance_wave(_plan(["param_a"]), {"param_a": _baselines()["param_a"]}, {})  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Провенанс исчерпания: контур ``inheritance`` в I3-протоколе
# --------------------------------------------------------------------------- #


class _PhaseStateStore:
    """Store-дублёр только для чтения: журнал фаз и пустые выборки."""

    def __init__(self, states: dict[str, str]) -> None:
        self._states = states

    def get_phase_state(self, run_id: str) -> dict[str, str]:
        return dict(self._states)

    def iter_corpus_docs(self, run_id: str) -> list[Any]:
        return []

    def iter_corpus_coverage(self, run_id: str) -> list[Any]:
        return []

    def iter_registry_cells(self, run_id: str) -> list[Any]:
        return []

    def get_crawl_session(self, session_id: str) -> None:
        return None


def _exhaustion_facts(
    *,
    products: tuple[str, ...] = ("p1",),
    passports: dict[str, Passport] | None = None,
    baselines: list[BaselineRow] | None = None,
    inheritance_done: bool = True,
) -> dict[str, frozenset[str]]:
    """``contours_run`` из наблюдённых фактов фазы Ш4."""
    from types import SimpleNamespace

    from core.pipeline.phases import _observed_exhaustion_facts

    ctx = SimpleNamespace(
        store=_PhaseStateStore({"inheritance": "ok"} if inheritance_done else {}),
        ports=SimpleNamespace(),
        clock=None,
    )
    inputs = SimpleNamespace(
        baselines=baselines if baselines is not None else [_baselines()["param_a"]]
    )
    facts = _observed_exhaustion_facts(
        ctx,
        _plan(["param_a"], products=products),
        inputs,
        passports if passports is not None else {"p1": _passport("p1")},
    )
    return dict(facts.contours_run)


def test_inheritance_contour_counts_product_with_declared_upstream_line() -> None:
    # Контур исполнен, если у продукта есть upstream-линия с совпадающей строкой
    # ``baselines.csv``. Поля ``Passport.upstream`` не существует — чтение по нему молча
    # давало пустой контур у всех продуктов, и I3-протокол терял ``inheritance``.
    contours = _exhaustion_facts()
    assert contours["p1"] == frozenset({"inheritance"})


def test_inheritance_contour_skips_product_without_passport() -> None:
    contours = _exhaustion_facts(products=("p1", "p2"))
    assert contours["p1"] == frozenset({"inheritance"})
    assert contours["p2"] == frozenset()


def test_inheritance_contour_skips_passport_without_upstreams() -> None:
    # Пустой список линий — наследовать не от кого: контур для продукта не исполнился.
    contours = _exhaustion_facts(passports={"p1": _passport("p1", upstreams=[])})
    assert contours["p1"] == frozenset()


def test_inheritance_contour_skips_unmatched_baseline_id() -> None:
    # Линия объявлена, но строки базлайна на неё нет — волна продукт не рассматривает.
    contours = _exhaustion_facts(
        passports={"p1": _passport("p1", baseline_id="base_9")}
    )
    assert contours["p1"] == frozenset()


def test_inheritance_contour_requires_the_phase_to_have_run() -> None:
    contours = _exhaustion_facts(inheritance_done=False)
    assert contours["p1"] == frozenset()
