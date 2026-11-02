"""Тесты ``core/util/rate_limit.py`` (P2: пейсинг и backoff краула).

Без сети, без I/O и без реального сна: ``sleep``/``clock``/``rng`` инъектируются.
Проверяются: ``spec is None`` → 0.0 без сна и без состояния; интервал **по origin**
(резервирование, а не сон под локом); независимость разных origin'ов; потокобезопасность
и отсутствие дедлока; нормализация хоста; ограничение состояния. Для ``jittered_backoff`` —
границы Full Jitter и валидация. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import random
import threading
from itertools import pairwise

import pytest

import core.util.rate_limit as rate_limit_module
from core.domain.models import RateLimitSpec
from core.util.rate_limit import RateLimiter, jittered_backoff


class _FakeClock:
    """Монотонные часы с ручным/автоматическим продвижением (без wall-clock)."""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self.now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.now += seconds


class _SleepRecorder:
    """Инъектируемый ``sleep``: записывает длительности, ничего не спит."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


class _AdvancingSleep(_SleepRecorder):
    """``sleep``, который двигает фейковые часы ровно на проспанное время."""

    def __init__(self, clock: _FakeClock) -> None:
        super().__init__()
        self._clock = clock

    def __call__(self, seconds: float) -> None:
        super().__call__(seconds)
        self._clock.advance(seconds)


# --- RateLimiter: spec None / min == max / детерминизм ------------------------


def test_wait_returns_zero_without_sleep_when_spec_none() -> None:
    recorder = _SleepRecorder()
    limiter = RateLimiter(sleep=recorder)
    assert limiter.wait("example.com", None) == 0.0
    assert recorder.calls == []  # sleep не вызывался


def test_wait_spec_none_does_not_create_state() -> None:
    clock = _FakeClock()
    recorder = _SleepRecorder()
    limiter = RateLimiter(sleep=recorder, clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    assert limiter.wait("example.com", None) == 0.0
    # Никакого резерва не появилось: первый же пейсинг — ровно одна задержка.
    assert limiter.wait("example.com", spec) == 1.0
    assert recorder.calls == [1.0]


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
    """Каждый интервал (шаг резерва) лежит в границах ``spec``.

    Возвращаемое значение — время до ``target``: при последовательных вызовах с
    продвижением часов оно равно сэмплированной задержке, т.к. предыдущий резерв уже
    наступил.
    """
    spec = RateLimitSpec(min_s=3.0, max_s=4.0)
    clock = _FakeClock()
    recorder = _AdvancingSleep(clock)
    limiter = RateLimiter(sleep=recorder, rng=random.Random(7), clock=clock)
    for _ in range(50):
        delay = limiter.wait("host", spec)
        assert 3.0 <= delay <= 4.0


# --- RateLimiter: интервал по origin ----------------------------------------


def test_same_host_sequential_targets_are_one_delay_apart() -> None:
    """Второй резерв = первый + задержка; суммарный сон = сумма резервов.

    Часы не двигаются фейковым ``sleep`` — видно именно **резервирование**: старый
    stateless-лимитер вернул бы 2.0 и во второй раз.
    """
    clock = _FakeClock(now=100.0)
    recorder = _SleepRecorder()
    limiter = RateLimiter(sleep=recorder, rng=random.Random(0), clock=clock)
    spec = RateLimitSpec(min_s=2.0, max_s=2.0)

    first = limiter.wait("example.com", spec)
    second = limiter.wait("example.com", spec)

    assert first == 2.0  # target1 = 100 + 2
    assert second == 4.0  # target2 = target1 + 2 = 104; now всё ещё 100
    assert recorder.calls == [2.0, 4.0]
    assert sum(recorder.calls) == first + second == 6.0


def test_different_hosts_do_not_block_each_other() -> None:
    """Резерв на A не задерживает B: разные origin'ы независимы."""
    clock = _FakeClock(now=10.0)
    recorder = _SleepRecorder()  # не двигает часы: B видит то же ``now``
    limiter = RateLimiter(sleep=recorder, rng=random.Random(0), clock=clock)

    long_spec = RateLimitSpec(min_s=100.0, max_s=100.0)
    short_spec = RateLimitSpec(min_s=0.0, max_s=0.0)

    assert limiter.wait("a.test", long_spec) == 100.0
    assert limiter.wait("b.test", short_spec) == 0.0
    assert recorder.calls == [100.0, 0.0]
    assert clock.now == 10.0  # B не ждал резерв A


def test_concurrent_same_host_reservations_are_unique_and_spaced() -> None:
    """N потоков к одному origin: цели уникальны и растут ровно на задержку."""
    clock = _FakeClock(now=0.0)
    recorder = _SleepRecorder()  # сон не двигает часы → все видят now=0
    limiter = RateLimiter(sleep=recorder, rng=random.Random(0), clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    threads_count = 8
    barrier = threading.Barrier(threads_count)
    results: list[float] = []
    results_lock = threading.Lock()

    def worker() -> None:
        barrier.wait(timeout=5)
        slept = limiter.wait("example.com", spec)
        with results_lock:
            results.append(slept)

    threads = [threading.Thread(target=worker) for _ in range(threads_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive(), "дедлок: поток не завершился"

    assert len(results) == threads_count
    targets = sorted(results)  # now=0 ⇒ slept == target
    assert len(set(targets)) == threads_count  # без дублей
    for earlier, later in pairwise(targets):
        assert later - earlier == pytest.approx(1.0)


def test_lock_is_not_held_during_sleep() -> None:
    """Сон одного origin'а не блокирует резерв другого (лок отпущен до ``sleep``)."""
    clock = _FakeClock()
    started = threading.Event()
    release = threading.Event()

    def blocking_sleep(_seconds: float) -> None:
        # Блокируется только первый сон; резерв B должен пройти, пока A спит.
        if not started.is_set():
            started.set()
            assert release.wait(timeout=5), "резерв B не прошёл — лок держится во сне"

    limiter = RateLimiter(sleep=blocking_sleep, clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    first = threading.Thread(target=lambda: limiter.wait("a.test", spec))
    first.start()
    assert started.wait(timeout=5), "поток A не дошёл до сна"

    # Если бы лок удерживался на время сна, этот вызов завис бы.
    assert limiter.wait("b.test", spec) == 1.0

    release.set()
    first.join(timeout=5)
    assert not first.is_alive()


# --- RateLimiter: нормализация хоста ----------------------------------------


@pytest.mark.parametrize(
    "variant",
    ["Example.COM", "example.com", "example.com:443", "example.com.", "https://Example.COM:443/x"],
)
def test_host_normalization_treats_variants_as_one_origin(variant: str) -> None:
    """``Example.COM``/``example.com``/``example.com:443`` — один origin."""
    clock = _FakeClock(now=0.0)
    recorder = _SleepRecorder()  # часы не двигаются: виден накопленный резерв
    limiter = RateLimiter(sleep=recorder, rng=random.Random(0), clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    assert limiter.wait("example.com", spec) == 1.0
    assert limiter.wait(variant, spec) == 2.0  # тот же ключ → резерв продолжен


def test_host_normalization_distinguishes_other_hosts() -> None:
    clock = _FakeClock(now=0.0)
    limiter = RateLimiter(sleep=_SleepRecorder(), clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    assert limiter.wait("a.test", spec) == 1.0
    assert limiter.wait("b.test", spec) == 1.0  # другой origin — свой резерв
    assert limiter.wait("a.test", spec) == 2.0


def test_host_normalization_handles_ipv6_literal() -> None:
    clock = _FakeClock(now=0.0)
    limiter = RateLimiter(sleep=_SleepRecorder(), clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    assert limiter.wait("[::1]:443", spec) == 1.0
    assert limiter.wait("::1", spec) == 2.0


# --- RateLimiter: ограничение состояния -------------------------------------


def test_state_is_bounded_by_pruning_inert_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Записи с резервом в прошлом вычищаются; интервалы от этого не сбрасываются."""
    monkeypatch.setattr(rate_limit_module, "_PRUNE_AT", 4)
    clock = _FakeClock(now=0.0)
    limiter = RateLimiter(sleep=_AdvancingSleep(clock), clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    for index in range(20):
        # Каждый вызов — новый origin; часы уходят вперёд, прошлые резервы инертны.
        assert limiter.wait(f"h{index}.test", spec) == 1.0

    assert len(limiter._last) <= 4  # type: ignore[attr-defined]


def test_pruning_does_not_reset_future_spacing() -> None:
    """Будущий резерв сохраняется при очистке: интервал не «обнуляется»."""
    clock = _FakeClock(now=0.0)
    recorder = _SleepRecorder()  # часы стоят: резерв остаётся в будущем
    limiter = RateLimiter(sleep=recorder, clock=clock)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    assert limiter.wait("h.test", spec) == 1.0
    assert limiter.wait("h.test", spec) == 2.0
    assert limiter.wait("h.test", spec) == 3.0


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


# --- RateLimiter: wait_reserved — разрешённый момент, а не момент пробуждения ----


class _OversleepingClock(_FakeClock):
    """``sleep`` двигает часы на проспанное **плюс** задержку возврата (передача GIL)."""

    def sleep(self, seconds: float) -> None:
        self.advance(seconds + self.oversleep_s)

    def __init__(self, now: float = 0.0, oversleep_s: float = 0.007) -> None:
        super().__init__(now)
        self.oversleep_s = oversleep_s


class _JitteryWall:
    """Стенные часы, рассогласованные с монотонными на 40 мкс (период 13 мс).

    Ровно тот дефект, из-за которого интервал «нарушался» (замер 02.11.2026: 19 975 мкс
    при объявленных 20 000): метка, выведенная из отдельного чтения стенных часов плюс
    остаток по монотонным, впитывала рассогласование ``ε`` между часами.

    Смещение — функция **времени**, а не числа чтений: реализация вправе читать стенные
    часы лишний раз, и это не должно ни маскировать дефект, ни создавать его.
    """

    def __init__(
        self,
        clock: _FakeClock,
        base: float = 1_700_000_000.0,
        period_s: float = 0.013,
        skew_s: float = 0.000_040,
    ) -> None:
        self._clock = clock
        self._base = base
        self._period_s = period_s
        self._skew_s = skew_s

    def __call__(self) -> float:
        phase = int(self._clock() / self._period_s) % 2 == 0
        return self._base + self._clock() + (self._skew_s if phase else 0.0)


def _iso(seconds_from_epoch: float) -> str:
    return _dt.datetime.fromtimestamp(seconds_from_epoch, _dt.UTC).isoformat()


def test_wait_reserved_returns_reserved_moment_not_wake_moment() -> None:
    """Метка — момент резерва: пересыпание потока в неё не попадает (прежний дефект)."""
    clock = _OversleepingClock(oversleep_s=0.007)
    limiter = RateLimiter(sleep=clock.sleep, clock=clock, wall=lambda: 1_700_000_000.0 + clock.now)
    spec = RateLimitSpec(min_s=1.0, max_s=1.0)

    first = limiter.wait_reserved("h.test", spec)
    # Резерв — база + 1.0 с; пробуждение случилось на 7 мс позже и в метку не входит.
    assert first == _iso(1_700_000_001.0)
    assert clock.now == pytest.approx(1.007)

    second = limiter.wait_reserved("h.test", spec)
    # Host простаивал (пробуждение уже за прежним резервом) ⇒ якорь — текущий момент,
    # но метка всё равно «момент + задержка»: 2.007, а не 2.014 (момент пробуждения).
    assert second == _iso(1_700_000_002.007)


def test_wait_reserved_interval_exactly_matches_declared_under_wall_jitter() -> None:
    """Шаг метки равен объявленному **ровно**: дрожание чтения часов не впитывается.

    На прежнем коде (метка = ``wall() + remaining``) первое чтение стенных часов
    отставало/опережало на 40 мкс, и интервал выходил 19 960 мкс — короче объявленных
    20 000. Цепь в целых микросекундах такой интервал даёт только при реальном опоздании
    (тогда он **длиннее** обещанного, что и есть правда).
    """
    clock = _FakeClock()
    limiter = RateLimiter(
        sleep=_AdvancingSleep(clock), clock=clock, wall=_JitteryWall(clock)
    )
    spec = RateLimitSpec(min_s=0.02, max_s=0.02)

    first = _dt.datetime.fromisoformat(limiter.wait_reserved("h.test", spec))
    second = _dt.datetime.fromisoformat(limiter.wait_reserved("h.test", spec))

    assert (second - first) == _dt.timedelta(microseconds=20_000)
    assert (second - first).total_seconds() == 0.02


def test_wait_reserved_without_spec_is_now() -> None:
    """``spec is None`` — пейсинга нет: метка равна текущему стенному времени, сна нет."""
    recorder = _SleepRecorder()
    limiter = RateLimiter(
        sleep=recorder, clock=_FakeClock(), wall=lambda: 1_700_000_000.5
    )
    assert limiter.wait_reserved("h.test", None) == _iso(1_700_000_000.5)
    assert recorder.calls == []
