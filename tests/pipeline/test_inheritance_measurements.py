"""Замеры наследования: постороннее доказательство, концентрация поддержки, базис Ш4.

Проверяется то, что видит оператор, а не внутренняя механика волны:

* унаследованная ✅-ячейка с паспортной цитатой-визиткой помечается
  ``support_not_about_subject`` (в заметках и в ``review_reason``), но вердикт остаётся
  ``✅`` — это наблюдение, а не решение (A3);
* уже стоящая причина ревью не затирается при склейке;
* ``support_concentration`` публикует концентрацию (19 ячеек / 1 цитата → ``max_share=1.0``);
* ``inheritance_basis`` считает ``direct``/``conditional``/``blocked`` раздельно.

Фикстуры нейтральные (``p1/p2``, ``param_*``).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

from core.domain import (
    AnchorRelation,
    BaselineRow,
    CatalogRow,
    Cell,
    CellState,
    Citation,
    Cohort,
    CohortEntry,
    InheritanceMode,
    LLMEvidence,
    Passport,
    SourceKind,
    Status,
    Upstream,
)
from core.pipeline import RunPlan
from core.pipeline.evaluate import (
    SUPPORT_NOT_ABOUT_SUBJECT,
    compose_review_reason,
    support_concentration,
)
from core.pipeline.inheritance import (
    _annotate_support,
    inheritance_basis,
    inheritance_wave,
)
from core.pipeline.precedence import (
    CONDITIONAL_REVIEW_REASON,
    PRECEDENCE_REVIEW_REASON,
)

RUN_ID = "run-fixture"
FIXTURES = Path(__file__).parent / "fixtures"

#: Цитата-«визитка»: утверждает происхождение продукта, а не параметр. Ровно этот класс
#: цитат и размножался наследованием на все параметры upstream-линии.
VISITING_CARD = (
    "Продукт корпоративного уровня на базе проверенного ядра с расширенной "
    "функциональностью"
)


# --------------------------------------------------------------------------- #
# Строители
# --------------------------------------------------------------------------- #


def _catalog() -> dict[str, CatalogRow]:
    payload = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    rows = [CatalogRow.model_validate(row) for row in payload["catalog"]]
    return {row.param_id: row for row in rows}


def _baselines() -> dict[str, BaselineRow]:
    payload = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    rows = [BaselineRow.model_validate(row) for row in payload["baselines"]]
    return {row.param_id: row for row in rows}


def _plan(param_ids: list[str], products: tuple[str, ...] = ("p1",)) -> RunPlan:
    catalog = _catalog()
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
        params=[catalog[param_id] for param_id in param_ids],
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


def _passport(product_id: str = "p1", *, quote: str = VISITING_CARD) -> Passport:
    return Passport(
        product_id=product_id,
        canonical_name=f"Product {product_id}",
        vendor="Vendor",
        upstream=Upstream(
            baseline_id="base_1",
            version="16.0",
            evidence=Citation(url="https://upstream.test/doc#param_a", quote=quote),
        ),
        version="1.0",
        jurisdiction="RU",
        snapshot_date=_dt.date(2026, 1, 1),
    )


def _cell(
    param_id: str,
    *,
    product_id: str = "p1",
    status: Status | None = Status.OK,
    state: CellState = CellState.EVALUATED,
    source_kind: SourceKind = SourceKind.DOCUMENTATION,
    review_reason: str | None = None,
    quote: str = VISITING_CARD,
) -> Cell:
    # I1: presence-ячейка обязана нести evidence — иначе модель не соберётся.
    evidence = (
        [LLMEvidence(source_id="src", quote=quote)]
        if status in {Status.OK, Status.PARTIAL}
        else []
    )
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=state,
        status=status,
        source_kind=source_kind,
        review_reason=review_reason,
        evidence=evidence,
    )


# --------------------------------------------------------------------------- #
# (а) унаследованная ячейка: замер пишется, вердикт не меняется
# --------------------------------------------------------------------------- #


def test_inherited_cell_is_flagged_but_keeps_verdict() -> None:
    result = inheritance_wave(
        _plan(["param_a"]),
        [_baselines()["param_a"]],
        {"p1": _passport()},
    )

    [cell] = result.cells
    # Наблюдение: цитата-визитка не упоминает предмет параметра (I8/K5 при этом выполнен).
    assert cell.validation_notes and cell.validation_notes[0].startswith(
        SUPPORT_NOT_ABOUT_SUBJECT
    )
    assert cell.review_reason == cell.validation_notes[0]
    # Вердикт — прежний: замер не подменяет решение (A3).
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.OK
    assert cell.source_kind is SourceKind.INHERITANCE


def test_inherited_cell_with_related_quote_stays_silent() -> None:
    # Предмет param_a — «Param A»: цитата, упоминающая «param», замера не требует.
    result = inheritance_wave(
        _plan(["param_a"]),
        [_baselines()["param_a"]],
        {"p1": _passport(quote="param is handled by the mechanism")},
    )

    [cell] = result.cells
    assert cell.status is Status.OK
    assert cell.validation_notes == []
    assert cell.review_reason is None


# --------------------------------------------------------------------------- #
# (б) склейка причин: уже стоящая причина не затирается
# --------------------------------------------------------------------------- #


def test_existing_review_reason_is_kept_when_support_note_is_added() -> None:
    # Любая уже стоящая причина (каноническая — conditional) обязана пережить замер.
    cell = _cell("param_a", review_reason=CONDITIONAL_REVIEW_REASON)
    annotated = _annotate_support(cell, _catalog()["param_a"])

    note = annotated.validation_notes[-1]
    assert note.startswith(SUPPORT_NOT_ABOUT_SUBJECT)
    assert annotated.review_reason == compose_review_reason(
        CONDITIONAL_REVIEW_REASON, note
    )
    assert CONDITIONAL_REVIEW_REASON in annotated.review_reason
    assert SUPPORT_NOT_ABOUT_SUBJECT in annotated.review_reason
    # Исходная ячейка не мутирована: замер возвращает новое значение.
    assert cell.review_reason == CONDITIONAL_REVIEW_REASON
    assert cell.validation_notes == []


def test_annotation_is_idempotent_and_does_not_fake_quotes() -> None:
    cell = _cell("param_a")
    once = _annotate_support(cell, _catalog()["param_a"])
    twice = _annotate_support(once, _catalog()["param_a"])

    # S8/R7: повторный проход не плодит записи и не склеивает причину с самой собой.
    assert twice.validation_notes == once.validation_notes
    assert twice.review_reason == once.review_reason

    # ``no_quotes`` — не наша аномалия (I1/K5): замер не подменяет отсутствие цитаты.
    # Ячейка без evidence собирается model_construct: интересен только путь замера.
    empty = Cell.model_construct(
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[],
        absence_evidence=[],
        validation_notes=[],
        reviewed_by=None,
    )
    assert _annotate_support(empty, _catalog()["param_a"]) is empty


# --------------------------------------------------------------------------- #
# (в) концентрация поддержки
# --------------------------------------------------------------------------- #


def test_support_concentration_single_quote_across_many_cells() -> None:
    param_ids = [f"param_{index:02d}" for index in range(19)]
    cells = [_cell(param_id) for param_id in param_ids]
    measured = support_concentration(
        {("p1", param_id): [VISITING_CARD] for param_id in param_ids}, cells
    )

    assert measured["measurement"] == "support_concentration"
    [product] = measured["products"].values()
    assert product["cells_present"] == 19
    assert product["distinct_quotes"] == 1
    assert product["top_quote_count"] == 19
    assert product["max_share"] == 1.0
    assert product["top_quote"] == VISITING_CARD
    assert product["top_quote_source_kinds"] == {"documentation": 19}
    assert len(measured["cells"]) == 19


def test_support_concentration_two_quotes_on_four_cells() -> None:
    param_ids = [f"param_{index:02d}" for index in range(4)]
    cells = [_cell(param_id) for param_id in param_ids]
    quotes = {
        ("p1", "param_00"): ["первая цитата"],
        ("p1", "param_01"): ["первая цитата"],
        ("p1", "param_02"): ["вторая цитата"],
        ("p1", "param_03"): ["вторая цитата"],
    }

    measured = support_concentration(quotes, cells)
    product = measured["products"]["p1"]

    assert product["cells_present"] == 4
    assert product["distinct_quotes"] == 2
    assert product["top_quote_count"] == 2
    assert product["max_share"] == 0.5
    # Ничья разрешена детерминированно: счётчик, затем текст цитаты.
    assert product["top_quote"] == "вторая цитата"
    assert len(measured["cells"]) == 2


def test_support_concentration_multi_quote_cell_counts_once_per_quote() -> None:
    cells = [_cell("param_00"), _cell("param_01")]
    quotes = {
        ("p1", "param_00"): ["общая", "своя"],
        ("p1", "param_01"): ["общая"],
    }

    product = support_concentration(quotes, cells)["products"]["p1"]

    # Ячейка param_00 учитывается один раз в счётчике «общей» цитаты, хоть и несёт две.
    assert product["top_quote"] == "общая"
    assert product["top_quote_count"] == 2
    assert product["distinct_quotes"] == 2
    assert product["max_share"] == 1.0


def test_support_concentration_ignores_non_presence_and_missing_quotes() -> None:
    cells = [
        _cell("param_00"),
        _cell("param_01", status=Status.UNKNOWN),
        _cell("param_02", status=None, state=CellState.PENDING),
        _cell("param_03"),
    ]
    quotes = {("p1", "param_00"): ["цитата"]}

    measured = support_concentration(quotes, cells)

    assert measured["cells_present"] == 2  # «❌» и pending — не presence
    assert measured["cells_without_quotes"] == 1  # у param_03 доказательства нет
    assert measured["products"]["p1"]["max_share"] == 0.5


def test_support_concentration_truncates_display_but_groups_by_full_text() -> None:
    long_quote = "д" * 400
    cells = [_cell("param_00"), _cell("param_01")]
    measured = support_concentration(
        {("p1", "param_00"): [long_quote], ("p1", "param_01"): [long_quote]}, cells
    )

    product = measured["products"]["p1"]
    assert product["top_quote_count"] == 2
    assert product["distinct_quotes"] == 1
    assert product["top_quote"].endswith("…")
    assert len(product["top_quote"]) == measured["quote_limit"]
    assert measured["quote_limit"] == 160


# --------------------------------------------------------------------------- #
# (г) базис наследования: режимы раздельно
# --------------------------------------------------------------------------- #


def _basis_baselines() -> list[BaselineRow]:
    def row(param_id: str, mode: InheritanceMode) -> BaselineRow:
        return BaselineRow(
            baseline_id="base_1", param_id=param_id, min_version=None, inheritance_mode=mode
        )

    return [
        row("param_a", InheritanceMode.DIRECT),
        row("param_b", InheritanceMode.DIRECT),
        row("param_c", InheritanceMode.CONDITIONAL),
        row("param_d", InheritanceMode.BLOCKED),
    ]


def test_inheritance_basis_separates_modes() -> None:
    cells = [
        _cell("param_a", source_kind=SourceKind.INHERITANCE),
        _cell("param_b", source_kind=SourceKind.INHERITANCE),
        _cell(
            "param_c",
            status=None,
            state=CellState.PENDING,
            source_kind=SourceKind.INHERITANCE,
            review_reason=CONDITIONAL_REVIEW_REASON,
        ),
        # Корпусная оценка параметра с direct-строкой: не работа волны (видно в foreign).
        _cell("param_a"),
        # Унаследованная ячейка без строки базлайна: режиму не приписывается.
        _cell("param_x", source_kind=SourceKind.INHERITANCE),
    ]

    basis = inheritance_basis(
        _basis_baselines(),
        products=["p1"],
        passports={"p1": _passport()},
        params=["param_a", "param_b", "param_c", "param_d"],
        cells=cells,
    )

    product = basis["products"]["p1"]
    assert product["baseline_id"] == "base_1"
    assert product["planned"] == {"direct": 2, "conditional": 1, "blocked": 1}
    assert product["released"]["direct"]["evaluated"] == 2
    assert product["released"]["conditional"]["pending"] == 1
    assert product["released"]["blocked"] == {
        "evaluated": 0,
        "pending": 0,
        "failed": 0,
        "other": 0,
    }
    assert product["unattributed_cells"] == 1
    assert product["foreign_cells"] == 1
    assert product["cells_emitted"] == 3  # шапка: 2 direct + 1 conditional
    # Операторская строка: «объявлено / выпущено» по каждому режиму.
    assert product["summary"].startswith("p1: direct 2/2 evaluated")
    assert "conditional 1/1 pending" in product["summary"]
    assert "blocked 1 skipped" in product["summary"]
    assert basis["totals"]["planned"] == {"direct": 2, "conditional": 1, "blocked": 1}


def test_inheritance_basis_filters_by_declared_params() -> None:
    basis = inheritance_basis(
        _basis_baselines(),
        products=["p1"],
        passports={"p1": _passport()},
        params=["param_a"],
    )

    assert basis["products"]["p1"]["planned"] == {
        "direct": 1,
        "conditional": 0,
        "blocked": 0,
    }


def test_inheritance_basis_counts_precedence_candidate_as_wave_work() -> None:
    # Кандидат ревью поверх реестра наследует source_kind высокого источника, но
    # выпущен волной: базис не должен терять свои же ячейки (и не звать их foreign).
    cells = [
        _cell("param_a", source_kind=SourceKind.INHERITANCE),
        _cell(
            "param_b",
            state=CellState.PENDING,
            source_kind=SourceKind.REGISTRY,
            review_reason=PRECEDENCE_REVIEW_REASON,
        ),
    ]

    product = inheritance_basis(
        _basis_baselines(),
        products=["p1"],
        passports={"p1": _passport()},
        params=["param_a", "param_b"],
        cells=cells,
    )["products"]["p1"]

    assert product["released"]["direct"] == {
        "evaluated": 1,
        "pending": 1,
        "failed": 0,
        "other": 0,
    }
    assert product["foreign_cells"] == 0
    assert product["cells_emitted"] == 2


def test_wave_publishes_basis_in_log_entries() -> None:
    # Второй источник операторской видимости: заметка фазы (``results[].notes`` вывода).
    result = inheritance_wave(
        _plan(["param_a", "param_b", "param_c"]),
        [_baselines()[key] for key in ("param_a", "param_b", "param_c")],
        {"p1": _passport()},
    )

    [note] = result.log_entries
    assert note.startswith("inheritance_basis: ")
    assert "direct 1/1 evaluated" in note
    assert "conditional 1/1 pending" in note
    assert "blocked 1 skipped" in note
