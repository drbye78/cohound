"""Тесты ``core/util/retrieval_eval.py``: nDCG@K по бинарной релевантности.

Остальные метрики модуля (hit@K, recall@K, MRR, счётчик неразрешённых id) проверяются в
``tests/selfcheck.py::_retrieval_eval`` и ``tests/pipeline/test_experiments.py``; здесь —
только :func:`core.util.retrieval_eval.ndcg_at_k`, чтобы новая метрика имела собственную
регрессионную сеть. Нейтрально (§1.3).
"""

from __future__ import annotations

import math

import pytest

from core.util.retrieval_eval import ndcg_at_k

#: 1/log2(2) + 1/log2(3) — IDCG двух релевантных мест.
_IDCG_2 = 1.0 + 1.0 / math.log2(3)


def test_ndcg_ideal_order_is_one() -> None:
    """Все ожидаемые id на первых местах — метрика равна 1.0."""
    assert ndcg_at_k({"a", "b", "c"}, ["a", "b", "c", "x"], 3) == pytest.approx(1.0)


def test_ndcg_interleaved_order_below_one() -> None:
    """Ожидаемые ниже нерелевантных — метрика < 1.0 (порядок учитывается, не только факт)."""
    value = ndcg_at_k({"a", "b"}, ["x", "y", "b", "a"], 5)
    # DCG = 1/log2(4) + 1/log2(5); IDCG = 1 + 1/log2(3).
    assert value == pytest.approx((0.5 + 1.0 / math.log2(5)) / _IDCG_2)
    assert 0.0 < value < 1.0


def test_ndcg_partial_hit() -> None:
    """Найден один из двух ожидаемых на первом месте — половина «идеального» вклада."""
    value = ndcg_at_k({"a", "b"}, ["a", "x", "y"], 3)
    assert value == pytest.approx(1.0 / _IDCG_2)
    assert value < 1.0


def test_ndcg_k_smaller_than_expected_set_is_reachable() -> None:
    """IDCG считается по ``min(len(expected), k)``: при k < |expected| 1.0 достижим."""
    assert ndcg_at_k({"a", "b", "c", "d"}, ["a", "b"], 2) == pytest.approx(1.0)
    # Но ожидаемый id за пределами k вклада не даёт.
    assert ndcg_at_k({"a"}, ["x", "a"], 1) == 0.0


def test_ndcg_empty_expected_is_zero() -> None:
    """Пустой эталон — 0.0 (метрика не определена, а не «идеально»)."""
    assert ndcg_at_k(set(), ["a", "b"], 3) == 0.0
    assert ndcg_at_k(frozenset(), [], 5) == 0.0


def test_ndcg_rejects_non_positive_k() -> None:
    with pytest.raises(ValueError, match="k"):
        ndcg_at_k({"a"}, ["a"], 0)
    with pytest.raises(ValueError, match="k"):
        ndcg_at_k({"a"}, ["a"], -1)


def test_ndcg_is_deterministic_and_order_insensitive_in_input() -> None:
    """Повтор даёт то же значение; порядок и тип эталонного множества не влияют."""
    ranked = ["x", "b", "a", "c"]
    first = ndcg_at_k({"a", "b", "c"}, ranked, 4)
    second = ndcg_at_k({"a", "b", "c"}, ranked, 4)
    as_list = ndcg_at_k(["c", "a", "b"], ranked, 4)
    assert first == second == as_list
