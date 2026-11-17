"""Тесты объявленной точки отсчёта когорты (``comparison_base``, §1.6).

Поле — **презентация** (относительно кого отчёт печатает Δ), а не условие замера, но
валидатор когорты обязан не дать объявить две точки отсчёта: отчёт не должен молча
взять первую. Якорь остаётся ровно один — проверка не ослаблена и не расширена.
"""

from __future__ import annotations

import datetime as _dt

import pytest
from pydantic import ValidationError

from core.domain.enums import AnchorRelation
from core.domain.models import Cohort, CohortEntry


def _entry(
    product_id: str,
    *,
    anchor_relation: AnchorRelation = AnchorRelation.ANALOG,
    comparison_base: bool = False,
) -> CohortEntry:
    return CohortEntry(
        product_id=product_id,
        role_overlap=0.8,
        selection_reason="x",
        anchor_relation=anchor_relation,
        comparison_base=comparison_base,
    )


def _cohort(*entries: CohortEntry) -> Cohort:
    return Cohort(
        case_id="case_x", frozen_at=_dt.date(2026, 11, 17), entries=list(entries)
    )


def test_comparison_base_defaults_to_false() -> None:
    """Дефолт ``False``: пакет без предка объявлять ничего не обязан."""
    assert _entry("p1").comparison_base is False


def test_base_and_anchor_together_are_legal() -> None:
    """Точка отсчёта — не якорь: ``analog`` + ``comparison_base`` проходят валидатор."""
    cohort = _cohort(
        _entry("anchor", anchor_relation=AnchorRelation.ANCHOR),
        _entry("base", comparison_base=True),
    )
    assert [e.product_id for e in cohort.entries if e.comparison_base] == ["base"]


def test_two_comparison_bases_rejected() -> None:
    """Больше одной точки отсчёта — ошибка с перечислением объявленных."""
    with pytest.raises(ValidationError) as exc:
        _cohort(
            _entry("anchor", anchor_relation=AnchorRelation.ANCHOR),
            _entry("base1", comparison_base=True),
            _entry("base2", comparison_base=True),
        )
    assert "comparison_base" in str(exc.value)
    assert "base1" in str(exc.value) and "base2" in str(exc.value)


def test_anchor_invariant_is_not_weakened() -> None:
    """Точка отсчёта не заменяет якорь: без ``anchor`` когорта по-прежнему отклоняется."""
    with pytest.raises(ValidationError):
        _cohort(_entry("base", comparison_base=True))
