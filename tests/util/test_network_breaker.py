"""Размыкатель сети: ``core/util/network_breaker.py`` (план 26 §21.2, пункт N2).

Проверяется ровно правило размыкания и ничего кроме него: машина чистая (без часов, I/O и
глобального состояния), поэтому каждый тест — одно утверждение о счётчиках.

* пять отказов на **одном** хосте не размыкают — это свойство хоста, а не канала;
* те же пять на **двух** хостах размыкают;
* ответ (в том числе ``4xx``/``5xx`` — но их в машине представляет вызывающий) сбрасывает
  серию;
* срабатывание ровно одно на серию, и после него счёт начинается заново;
* политика с неположительным порогом — ошибка, а не «разомкнуть при первом отказе»;
* ``max_trips`` исчерпывается ровно на своём срабатывании.

Второй блок проверяет **проводку** в драйвере когорты (:func:`drive_roots`): пауза
исполняется, после первой паузы фетчи продолжаются, а после исчерпания пауз выдача встаёт
и остаток остаётся ``pending``. Часы подменены: драйвер принимает ``sleep`` параметром,
поэтому тесты не спят.
"""

from __future__ import annotations

from typing import Any

import pytest

from core.pipeline import corpus_drain, corpus_driver
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import LocalFetch
from core.ports.crawl_errors import CrawlError
from core.ports.types import FetchPlan
from core.util.network_breaker import BreakerPolicy, NetworkBreaker


def _breaker(
    *,
    threshold: int = 5,
    min_hosts: int = 2,
    pause_s: float = 60.0,
    max_trips: int = 2,
) -> NetworkBreaker:
    """Размыкатель с дефолтной политикой (порог 5 на 2 хостах), поля — по вкусу теста."""
    return NetworkBreaker(
        BreakerPolicy(
            threshold=threshold,
            min_hosts=min_hosts,
            pause_s=pause_s,
            max_trips=max_trips,
        )
    )


def test_five_failures_on_one_host_do_not_trip() -> None:
    """Пять отказов на одном хосте — свойство хоста: канал не размыкается."""
    breaker = _breaker()

    trips = [breaker.note_silence("docs.example.org") for _ in range(5)]

    assert trips == [False] * 5
    assert breaker.trips == 0
    assert breaker.pending_pause_s == 0.0
    assert breaker.consecutive == 5
    assert breaker.distinct_hosts == 1


def test_five_failures_on_two_hosts_trip_once() -> None:
    """Пять отказов, покрывающих два разных хоста, — канал: размыкание."""
    breaker = _breaker()
    hosts = [
        "a.example.org",
        "b.example.org",
        "a.example.org",
        "b.example.org",
        "a.example.org",
    ]

    trips = [breaker.note_silence(host) for host in hosts]

    assert trips == [False, False, False, False, True]
    assert breaker.trips == 1
    assert breaker.pending_pause_s == BreakerPolicy().pause_s
    trip = breaker.last_trip
    assert trip is not None
    assert trip.consecutive == 5
    assert trip.hosts == ("a.example.org", "b.example.org")


def test_response_resets_the_series() -> None:
    """Ответ между отказами сбрасывает серию: 4 + ответ + 4 — не разомкнулось."""
    breaker = _breaker()

    for host in ("a.example.org", "b.example.org") * 2:
        assert breaker.note_silence(host) is False
    breaker.note_response()

    assert breaker.consecutive == 0
    assert breaker.distinct_hosts == 0
    trips = [breaker.note_silence(host) for host in ("a.example.org", "b.example.org") * 2]

    assert trips == [False] * 4
    assert breaker.trips == 0


def test_trip_happens_once_per_series() -> None:
    """Срабатывание — один раз на серию, а не на каждом следующем отказе."""
    breaker = _breaker()

    for host in ("a.example.org", "b.example.org") * 2:
        breaker.note_silence(host)
    assert breaker.note_silence("a.example.org") is True

    # Дальше серия считается заново, поэтому следующие отказы молчат.
    assert breaker.note_silence("a.example.org") is False
    assert breaker.note_silence("b.example.org") is False
    assert breaker.trips == 1


def test_counting_restarts_after_trip() -> None:
    """После размыкания серия начинается с нуля и размыкает снова только целиком."""
    breaker = _breaker()

    for host in ("a.example.org", "b.example.org") * 2:
        breaker.note_silence(host)
    assert breaker.note_silence("a.example.org") is True
    assert breaker.consecutive == 0

    trips = [breaker.note_silence(host) for host in ("a.example.org", "b.example.org") * 2]
    assert trips == [False] * 4
    assert breaker.trips == 1
    assert breaker.note_silence("b.example.org") is True
    assert breaker.trips == 2


def test_pause_is_cleared_by_a_response() -> None:
    """Пауза объявлена, пока канал не подал признак жизни."""
    breaker = _breaker()

    for host in ("a.example.org", "b.example.org") * 2:
        breaker.note_silence(host)
    assert breaker.note_silence("a.example.org") is True
    assert breaker.pending_pause_s == BreakerPolicy().pause_s

    breaker.note_response()
    assert breaker.pending_pause_s == 0.0


def test_host_normalization_is_case_insensitive() -> None:
    """Один и тот же origin в разном регистре — один хост, а не два."""
    breaker = _breaker(min_hosts=1, threshold=2)

    assert breaker.note_silence("Docs.Example.ORG") is False
    assert breaker.note_silence("docs.example.org") is True


def test_empty_host_is_refused() -> None:
    """«Отказ неизвестно где» не считается хостом: размыкать по нему нельзя."""
    breaker = _breaker()

    with pytest.raises(ValueError):
        breaker.note_silence("")


@pytest.mark.parametrize(
    "policy",
    [
        {"threshold": 0},
        {"threshold": -1},
        {"min_hosts": 0},
        {"min_hosts": -2},
        {"pause_s": 0.0},
        {"pause_s": -5.0},
        {"max_trips": 0},
        {"max_trips": -1},
    ],
)
def test_policy_validation(policy: dict[str, Any]) -> None:
    """Неположительный порог/пауза/число пауз — ошибка объявления, а не тихий сдвиг."""
    with pytest.raises(ValueError):
        BreakerPolicy(**policy)


def test_exhausted_becomes_true_on_max_trips_trip() -> None:
    """``exhausted`` — ровно на ``max_trips``-м срабатывании, не раньше и не позже.

    Число пауз — свойство политики, а не вывод из счётчика отказов: одна пауза отвечает
    на вопрос «блип или канал умер?», вторая — «пауза не помогла?».
    """
    breaker = _breaker(threshold=2, min_hosts=2, max_trips=2)

    def trip() -> bool:
        assert breaker.note_silence("a.example.org") is False
        return breaker.note_silence("b.example.org")

    assert trip() is True
    assert breaker.exhausted is False

    assert trip() is True
    assert breaker.exhausted is True
    assert breaker.trips == 2


# --- Проводка в драйвере когорты ------------------------------------------------------ #

#: Хосты волны: два разных — иначе серия не доказывает свойство канала.
_HOSTS = ("a.test", "b.test")


def _plan(root_url: str, product_id: str) -> corpus_drain.RootPlan:
    """Корень драйвера: цель и sitemap не нужны — волны строит фейковый ``begin``."""
    return corpus_drain.RootPlan(
        root_url=root_url, target=None, sitemap_seeds=False, product_id=product_id
    )


def _entry(pid: str, host: str, number: int) -> corpus_drain._WaveEntry:
    """Запись волны: один живой фетч на ``host`` (настоящий ``root_lifecycle`` не нужен)."""
    url = f"https://{host}/p{number}"
    return corpus_drain._WaveEntry(
        pid=pid,
        wu=url,
        depth=0,
        kind="fetch",
        desc=LocalFetch(
            pid=pid,
            wu=url,
            depth=0,
            fetch_plan=FetchPlan(),
            active=None,
            engine_value=None,
            prior_item=None,
            resume=None,
            browser_blocked=False,
            poolable=True,
            host=host,
            scope_host=None,
            enforce_redirect_scope=False,
        ),
    )


def _silent_begin(waves: int, width: int = 2) -> Any:
    """``begin=`` драйвера: ``waves`` волн по ``width`` молчащих фетчей на двух хостах.

    Хосты чередуются, поэтому волна шире порога (`width > threshold`) — это ровно тот
    случай, в котором остаток волны после срабатывания не должен доедаться: его отказы
    относятся к серии «до паузы».
    """

    def begin(plan: Any, *, fetch_pool: Any = None, browser_pool: Any = None) -> Any:
        pid = str(plan.product_id)
        for number in range(waves):
            yield corpus_drain.WavePlan(
                entries=[
                    _entry(pid, _HOSTS[index % len(_HOSTS)], number)
                    for index in range(width)
                ],
                hosts=frozenset(_HOSTS),
            )
        return (None, [])

    return begin


def _drive(
    *, waves: int, policy: BreakerPolicy, width: int = 2
) -> tuple[list[str], list[float], list[str], int, NetworkBreaker]:
    """Прогнать один корень через драйвер с молчащим фетчем; вернуть наблюдения.

    Часы подменены: ``sleep`` — приёмник пауз, поэтому тест не спит.
    """
    fetches: list[str] = []
    sleeps: list[float] = []
    errors: list[str] = []
    timings: dict[str, _CorpusStageTimings] = {}
    breaker = NetworkBreaker(policy)

    def local_fetch(desc: LocalFetch) -> Any:
        fetches.append(desc.wu)
        # «Ответа не было»: транзиентный ``CrawlError`` без ``http_status``
        # (таймаут/обрыв/DNS) — ровно то, что считает размыкатель.
        raise CrawlError("timed out")

    outcomes = corpus_driver.drive_roots(
        [_plan("https://a.test/", "p1")],
        host_concurrency=1,
        begin=_silent_begin(waves, width),
        lane_width=lambda plan: 1,
        local_fetch=local_fetch,
        timings_for=lambda pid: timings.setdefault(str(pid), _CorpusStageTimings()),
        errors=errors,
        network_breaker=breaker,
        sleep=sleeps.append,
        label=" теста",
    )
    return fetches, sleeps, errors, len(outcomes), breaker


def test_wide_wave_gives_one_trip_not_two() -> None:
    """Волна шире порога: срабатывание ровно одно, остаток волны не доедается.

    Это тест на конкретный дефект: при ``threshold=2`` волна из 4 отказов содержит две
    серии по два. Если разбирать остаток волны после паузы, получится два срабатывания
    подряд и ``exhausted`` — хотя проверка «ожил ли канал» не состоялась ни разу.
    Остаток относится к серии «до паузы», поэтому вердикт даёт следующая волна.
    """
    fetches, sleeps, errors, _, breaker = _drive(
        waves=1,
        width=4,
        policy=BreakerPolicy(threshold=2, min_hosts=2, pause_s=60.0, max_trips=2),
    )

    assert breaker.trips == 1
    assert breaker.exhausted is False
    assert sleeps == [60.0]
    # Пауза видима, но «паузы исчерпаны» не объявлено: до этого не дошло.
    assert len([line for line in errors if "отказов без ответа" in line]) == 1
    assert not [line for line in errors if "паузы исчерпаны" in line]
    # Фетчи волны исполнились все (они были отправлены до паузы) — наблюдение их
    # не доедает, но и работу не отменяет.
    assert len(fetches) == 4


def test_driver_pauses_and_continues_after_first_trip() -> None:
    """Первое срабатывание — пауза, а не остановка: после сна фетчи продолжаются."""
    fetches, sleeps, errors, _, breaker = _drive(
        waves=3,
        policy=BreakerPolicy(threshold=2, min_hosts=2, pause_s=60.0, max_trips=2),
    )

    # Пауза исполнена ровно одна — на первом срабатывании.
    assert sleeps == [60.0]
    # Продолжение видно числом: остановка на первом срабатывании дала бы ровно 2 фетча.
    stopped, stopped_sleeps, _, _, _ = _drive(
        waves=3,
        policy=BreakerPolicy(threshold=2, min_hosts=2, pause_s=60.0, max_trips=1),
    )
    assert stopped_sleeps == []
    assert len(stopped) == 2
    assert len(fetches) == 4
    # Пауза видима, даже когда прогон продолжился.
    assert len([line for line in errors if "отказов без ответа" in line]) == 2
    assert breaker.trips == 2


def test_driver_stops_when_pauses_are_exhausted() -> None:
    """Второе срабатывание подряд: пауза не помогла — выдача встаёт, остаток ``pending``."""
    fetches, sleeps, errors, outcomes, breaker = _drive(
        waves=3,
        policy=BreakerPolicy(threshold=2, min_hosts=2, pause_s=60.0, max_trips=2),
    )

    assert breaker.exhausted is True
    # Второе срабатывание паузы не даёт: продолжать уже незачем.
    assert sleeps == [60.0]
    # Третья волна не отправлена, корень остался без исхода (не ``failed``).
    assert len(fetches) == 4
    assert outcomes == 0
    trips = [line for line in errors if "отказов без ответа" in line]
    assert len(trips) == 2
    assert "a.test" in trips[0] and "b.test" in trips[0]
    assert len([line for line in errors if "корней без исхода" in line]) == 1

