"""Тесты согласия двух аннотаторов — Cohen's κ (§11.5, :mod:`core.util.agreement`).

Покрывают ручные значения κ (полное согласие = 1.0; шансовый уровень = 0.0; N/A-специфичный
случай, где κ по всему набору и κ по N/A расходятся — ради чего §11.5 и требует публиковать
N/A отдельно), вырожденные случаи (κ ``undefined`` вместо выдуманного числа), непарные ячейки
(размеченные лишь одним аннотатором — не согласие), метки вне словаря и контракт CSV.

Данные категорийно-нейтральны (§1.3): метки берутся из ``Status``, идентификаторы — ``p1``/
``param_x``.
"""

from __future__ import annotations

import pytest

from core.domain.enums import Status
from core.util.agreement import (
    NOT_NA,
    compute_agreement,
    parse_annotations_csv,
)

OK = Status.OK.value
PARTIAL = Status.PARTIAL.value
ABSENT = Status.ABSENT.value
NA = Status.NOT_APPLICABLE.value
KNOWN = [status.value for status in Status]

HEADER = "product_id,param_id,annotator_a,annotator_b"


def _csv(pairs: list[tuple[str, str | None]], *, start: int = 0) -> str:
    """Строит CSV с идентичностями ``p1/param_{i}``; ``None`` → пустая ячейка."""
    lines = [HEADER]
    for offset, (label_a, label_b) in enumerate(pairs):
        row = [
            "p1",
            f"param_{start + offset}",
            "" if label_a is None else label_a,
            "" if label_b is None else label_b,
        ]
        lines.append(",".join(row))
    return "\n".join(lines) + "\n"


def _report(pairs: list[tuple[str, str | None]], *, start: int = 0):
    return compute_agreement(
        parse_annotations_csv(_csv(pairs, start=start)),
        na_label=NA,
        known_labels=KNOWN,
    )


# ---------------------------------------------------------------------------
# Контракт CSV
# ---------------------------------------------------------------------------


def test_parse_requires_contract_columns() -> None:
    text = "product_id,annotator_a,annotator_b\np1,✅,✅\n"
    with pytest.raises(ValueError, match="param_id"):
        parse_annotations_csv(text)


def test_parse_ignores_column_order_and_extra_columns() -> None:
    text = (
        "annotator_b,extra,param_id,product_id,annotator_a\n"
        f"{ABSENT},note,param_x,p1,{OK}\n"
    )
    rows = parse_annotations_csv(text)
    assert len(rows) == 1
    assert rows[0].product_id == "p1"
    assert rows[0].param_id == "param_x"
    assert rows[0].label_a == OK
    assert rows[0].label_b == ABSENT


def test_parse_rejects_duplicate_cell() -> None:
    text = f"{HEADER}\np1,param_x,✅,✅\np1,param_x,❌,❌\n"
    with pytest.raises(ValueError, match="дубликат"):
        parse_annotations_csv(text)


def test_parse_rejects_row_without_any_label() -> None:
    text = f"{HEADER}\np1,param_x,,\n"
    with pytest.raises(ValueError, match="без единой метки"):
        parse_annotations_csv(text)


def test_parse_rejects_empty_identity() -> None:
    text = f"{HEADER}\n,param_x,✅,✅\n"
    with pytest.raises(ValueError, match="product_id и param_id"):
        parse_annotations_csv(text)


def test_parse_blank_label_means_missing() -> None:
    rows = parse_annotations_csv(f"{HEADER}\np1,param_x,✅,\n")
    assert rows[0].label_a == OK
    assert rows[0].label_b is None


# ---------------------------------------------------------------------------
# Ручные значения κ
# ---------------------------------------------------------------------------


def test_perfect_agreement_kappa_is_one() -> None:
    report = _report([(OK, OK), (OK, OK), (ABSENT, ABSENT), (ABSENT, ABSENT)])
    assert report.overall.defined
    assert report.overall.kappa == pytest.approx(1.0)
    assert report.overall.observed_agreement == pytest.approx(1.0)
    assert report.overall.expected_agreement == pytest.approx(0.5)
    assert report.overall.n == 4


def test_chance_level_kappa_is_zero() -> None:
    # Маргиналы A: ✅×6/❌×4, B: ✅×5/❌×5 → pe=0.5; расстановка даёт po=0.5 ⇒ κ=0.
    labels_a = [OK] * 6 + [ABSENT] * 4
    labels_b = [OK, OK, OK, ABSENT, ABSENT, ABSENT, OK, OK, ABSENT, ABSENT]
    report = _report(list(zip(labels_a, labels_b)))
    assert report.overall.kappa == pytest.approx(0.0, abs=1e-12)
    assert report.overall.observed_agreement == pytest.approx(0.5)
    assert report.overall.expected_agreement == pytest.approx(0.5)


def test_negative_kappa_is_reported_not_clamped() -> None:
    # Согласие хуже случайного: A и B размечают ровно наоборот, κ = -1 и не обрезается до 0.
    labels_a = [OK, OK, ABSENT, ABSENT]
    labels_b = [ABSENT, ABSENT, OK, OK]
    report = _report(list(zip(labels_a, labels_b)))
    assert report.overall.kappa == pytest.approx(-1.0)


def test_na_kappa_diverges_from_overall() -> None:
    """Сердце фазы: общий κ положителен, N/A-κ отрицателен — числа расходятся (§11.5)."""
    labels_a = [OK] * 10 + [ABSENT] * 4 + [NA] * 3 + [ABSENT] * 3
    labels_b = [OK] * 10 + [ABSENT] * 4 + [ABSENT] * 3 + [NA] * 3
    report = _report(list(zip(labels_a, labels_b)))

    assert report.overall.kappa == pytest.approx(0.5041322314, abs=1e-9)
    assert report.na_binary is not None
    assert report.na_binary.kappa == pytest.approx(-0.1764705882, abs=1e-9)
    assert report.na_binary.kappa < report.overall.kappa
    # Счётчики N/A: у каждого аннотатора по три решения, все разошлись.
    assert report.na_count_a == 3
    assert report.na_count_b == 3
    assert len(report.na_disagreements) == 6
    assert len(report.disagreements) == 6


def test_na_disagreements_are_subset_of_disagreements() -> None:
    labels_a = [OK, OK, NA, ABSENT]
    labels_b = [OK, ABSENT, OK, ABSENT]
    report = _report(list(zip(labels_a, labels_b)))
    assert set(report.na_disagreements) <= set(report.disagreements)
    assert report.na_disagreements == ("p1/param_2",)


# ---------------------------------------------------------------------------
# Вырожденные случаи: κ undefined, а не выдуманное число
# ---------------------------------------------------------------------------


def test_zero_variance_kappa_is_undefined() -> None:
    report = _report([(OK, OK), (OK, OK)])
    assert not report.overall.defined
    assert report.overall.kappa is None
    assert report.overall.observed_agreement == pytest.approx(1.0)
    assert report.overall.expected_agreement == pytest.approx(1.0)
    assert report.overall.undefined_reason is not None


def test_na_kappa_undefined_when_no_na_decisions() -> None:
    report = _report([(OK, OK), (ABSENT, ABSENT)])
    assert report.overall.defined
    assert report.na_binary is not None
    assert not report.na_binary.defined
    assert report.na_binary.kappa is None
    assert report.na_binary.undefined_reason is not None


def test_empty_input_kappa_undefined() -> None:
    report = compute_agreement([], na_label=NA, known_labels=KNOWN)
    assert report.total_items == 0
    assert report.paired_items == 0
    assert not report.overall.defined
    assert report.overall.undefined_reason == "нет парных ячеек для расчёта"


# ---------------------------------------------------------------------------
# Непарные ячейки (один аннотатор) — не согласие
# ---------------------------------------------------------------------------


def test_single_annotator_items_are_excluded_not_agreement() -> None:
    report = _report([(OK, OK), (ABSENT, None), (None, OK), (ABSENT, ABSENT)])
    assert report.total_items == 4
    assert report.paired_items == 2
    assert set(report.unpaired_items) == {"p1/param_1", "p1/param_2"}
    # κ считается только по двум парным ячейкам (полное согласие), а не по четырём.
    assert report.overall.n == 2
    assert report.overall.kappa == pytest.approx(1.0)


def test_all_unpaired_gives_undefined_kappa() -> None:
    report = _report([(OK, None), (None, ABSENT)])
    assert report.paired_items == 0
    assert report.overall.kappa is None
    assert set(report.unpaired_items) == {"p1/param_0", "p1/param_1"}


# ---------------------------------------------------------------------------
# Метки вне словаря + матрица ошибок
# ---------------------------------------------------------------------------


def test_unknown_labels_are_counted_not_silently_dropped() -> None:
    report = _report([("OK", OK), (ABSENT, ABSENT)])
    assert report.unknown_labels == {"OK": 1}
    # Неизвестная метка остаётся в расчёте «как дана», а не выкидывается молча.
    assert report.overall.n == 2


def test_known_labels_none_disables_check() -> None:
    report = compute_agreement(
        parse_annotations_csv(f"{HEADER}\np1,param_x,OK,OK\n"), na_label=NA
    )
    assert report.unknown_labels == {}


def test_confusion_matrix_is_square_and_counts_pairs() -> None:
    report = _report([(OK, OK), (OK, ABSENT), (PARTIAL, OK)])
    matrix = report.overall_confusion
    assert matrix.labels == (PARTIAL, OK, ABSENT)
    assert matrix.counts[OK][OK] == 1
    assert matrix.counts[OK][ABSENT] == 1
    assert matrix.counts[PARTIAL][OK] == 1
    assert matrix.counts[ABSENT][OK] == 0


def test_na_confusion_uses_na_and_complement() -> None:
    report = _report([(NA, NA), (NA, OK), (OK, OK)])
    assert report.na_confusion is not None
    assert set(report.na_confusion.labels) == {NA, NOT_NA}
    assert report.na_confusion.counts[NA][NA] == 1
    assert report.na_confusion.counts[NA][NOT_NA] == 1
    assert report.na_confusion.counts[NOT_NA][NOT_NA] == 1
