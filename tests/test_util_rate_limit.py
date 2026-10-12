"""Тесты ``core/util/rate_limit.py`` (P2: пейсинг и backoff краула).

Без сети, без I/O: ``sleep``/``rng`` инъектируются. Проверяются: ``spec is None`` →
0.0 без сна; ``min == max`` → точное значение; детерминизм при ``random.Random(seed)``;
границы Full Jitter (``0 <= v <= cap``) и валидация ``attempt``/``base_s``/``cap_s``.
Данные нейтральны (§1.3).
"""

from __future__ import annotations

import random

import pytest

from core.domain.models import RateLimitSpec
from core.util.rate_limit import RateLimiter, jittered_backoff


class _SleepRecorder:
    """Инъектируемый ``sleep``: записывает длительности, ничего не спит."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


# --- RateLimiter: spec None / min == max / детерминизм ------------------------


def test_wait_returns_zero_without_sleep_when_spec_none() -> None:
    recorder = _SleepRecorder()
    limiter = RateLimiter(sleep=recorder)
    assert limiter.wait("example.com", None) == 0.0
    assert recorder.calls == []  # sleep не вызывался


def test_wait_sleeps_and_returns_exact_value_when_min_equals_max() -> None:
    recorder = _SleepRecorder()
    limiter = RateLimiter(sleep=recorder)
    slept = limiter.wait("example.com", RateLimitSpec(min_s=2.5, max_s=2.5))
    assert slept == 2.5
    assert recorder.calls == [2.5]


def test_wait_zero_zero_sleeps_zero() -> None:
    recorder = _SleepRecorder()
    limiter = RateLimiter(sleep=recorder)
    assert limiter.wait("example.com", RateLimitSpec(min_s=0.0, max_s=0.0)) == 0.0
    assert recorder.calls == [0.0]


def test_wait_deterministic_with_seeded_rng() -> None:
    spec = RateLimitSpec(min_s=1.0, max_s=10.0)
    first_rec = _SleepRecorder()
    second_rec = _SleepRecorder()
    first = RateLimiter(sleep=first_rec, rng=random.Random(42))
    second = RateLimiter(sleep=second_rec, rng=random.Random(42))
    a = first.wait("host", spec)
    b = second.wait("host", spec)
    assert a == b
    assert first_rec.calls == second_rec.calls == [a]
    assert 1.0 <= a <= 10.0


def test_wait_stays_within_spec_bounds() -> None:
    spec = RateLimitSpec(min_s=3.0, max_s=4.0)
    limiter = RateLimiter(sleep=_SleepRecorder(), rng=random.Random(7))
    for _ in range(50):
        delay = limiter.wait("host", spec)
        assert 3.0 <= delay <= 4.0


# --- jittered_backoff: границы, детерминизм, валидация -------------------------


@pytest.mark.parametrize("attempt", [0, 1, 2, 3, 5, 10])
def test_jittered_backoff_within_bounds(attempt: int) -> None:
    rng = random.Random(1234)
    for _ in range(25):
        value = jittered_backoff(attempt, base_s=0.5, cap_s=30.0, rng=rng)
        assert 0.0 <= value <= 30.0


def test_jittered_backoff_capped_by_cap() -> None:
    # base_s * 2**attempt превышает cap → потолок = cap.
    rng = random.Random(1)
    for attempt in (4, 8, 20):
        value = jittered_backoff(attempt, base_s=1.0, cap_s=5.0, rng=rng)
        assert 0.0 <= value <= 5.0


def test_jittered_backoff_deterministic_with_seed() -> None:
    a = jittered_backoff(3, rng=random.Random(99))
    b = jittered_backoff(3, rng=random.Random(99))
    assert a == b


def test_jittered_backoff_zero_cap_ceiling_single_value() -> None:
    # Небольшой base/cap: ceiling известен точно.
    rng = random.Random(0)
    value = jittered_backoff(0, base_s=2.0, cap_s=2.0, rng=rng)
    assert 0.0 <= value <= 2.0


def test_jittered_backoff_negative_attempt_rejected() -> None:
    with pytest.raises(ValueError, match="attempt"):
        jittered_backoff(-1)


def test_jittered_backoff_non_positive_base_rejected() -> None:
    with pytest.raises(ValueError, match="base_s"):
        jittered_backoff(0, base_s=0.0)
    with pytest.raises(ValueError, match="base_s"):
        jittered_backoff(0, base_s=-1.0)


def test_jittered_backoff_non_positive_cap_rejected() -> None:
    with pytest.raises(ValueError, match="cap_s"):
        jittered_backoff(0, cap_s=0.0)
    with pytest.raises(ValueError, match="cap_s"):
        jittered_backoff(0, cap_s=-0.5)
