"""Прибор темпа краула (:mod:`core.pipeline.crawl_pacing`): число вместо потолка.

Проверяется то, ради чего прибор заведён (владелец 06.11.2026: «не ограничивать, а
измерять»):

1. пик одновременных запросов на один origin — **наблюдённый** факт (два потока внутри
   ``fetch`` дают ``in_flight_max == 2``, понижение до одного не пройдёт молча);
2. ``requests_per_s`` считается из опубликованного окна, а не из выдуманного нуля;
3. кумулятивное ожидание лимитера сводится по origin (источник — сам ``RateLimiter``);
4. агрегат по продукту и по прогону: сумма запросов, **максимум** пиков (не сумма);
5. отсутствие прибора — ``None``/пусто, а не нули;
6. провенанс не становится идентичностью: ни одно имя поля не входит в
   ``EXECUTION_CONTRACT_FIELDS``, дайджест контракта от измерения не меняется.

Данные нейтральны (§1.3): ``docs.test``/``a.test``, продукты ``p1``/``p2``.
"""

from __future__ import annotations

import threading
from typing import Any

import pytest

from core.domain import RateLimitSpec
from core.pipeline.crawl_pacing import CrawlPacing, pacing_from_provenance
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS, execution_contract_digest
from core.util.rate_limit import RateLimiter

HOST = "a.test"


class _StepClock:
    """Монотонные часы по сценарию (детерминизм: никакого сна в тесте)."""

    def __init__(self, *values: float) -> None:
        self._values = iter(values)
        self._last = 0.0

    def __call__(self) -> float:
        try:
            self._last = next(self._values)
        except StopIteration:  # pragma: no cover — сценарий обязан покрыть все чтения
            raise AssertionError(
                "часы прочитаны больше раз, чем задано сценарием"
            ) from None
        return self._last


class _ManualClock:
    """Часы, которые двигает только ``sleep`` лимитера."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


# --------------------------------------------------------------------------- #
# (1) Пик одновременных запросов на origin
# --------------------------------------------------------------------------- #


def test_in_flight_peak_is_two_for_two_overlapping_fetches() -> None:
    """Два потока внутри ``pacing.request`` ⇒ ``in_flight_max == 2`` (не 1 и не 3).

    Барьер внутри контекста гарантирует перекрытие: оба потока уже учтены «в полёте»,
    и только тогда барьер открывается. Это тот самый пин, которого не было: без него
    сериализация внутри host'а не покраснела бы ни один тест.
    """
    pacing = CrawlPacing()
    barrier = threading.Barrier(2)

    def one() -> None:
        with pacing.request("p1", HOST):
            barrier.wait(timeout=5)

    threads = [threading.Thread(target=one) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    host = pacing.snapshot()["hosts"][HOST]
    assert host["in_flight_max"] == 2
    assert host["requests"] == 2
    # В полёте на момент снимка никого: пик — прошедший факт, а не текущий счётчик.
    assert host["fetch_ms"]["n"] == 2


def test_sequential_fetches_have_peak_one() -> None:
    """Последовательные запросы того же origin'а не дают ложного пика."""
    pacing = CrawlPacing()
    for _ in range(3):
        with pacing.request("p1", HOST):
            pass
    host = pacing.snapshot()["hosts"][HOST]
    assert host["in_flight_max"] == 1
    assert host["requests"] == 3


# --------------------------------------------------------------------------- #
# (2) Наблюдённая частота из опубликованного окна
# --------------------------------------------------------------------------- #


def test_requests_per_s_is_computed_from_the_published_window() -> None:
    """``requests_per_s = requests / window``, и числа в артефакте согласованы.

    Сценарий: два запроса, окно ровно 2 с (``0 → 2``). Частота — ``2 / 2 == 1.0``.
    Проверяется именно опубликованное окно: пересчёт из него обязан дать то же число.
    """
    # Один запрос = два чтения часов (вход и выход): 0→1, затем 1→2.
    clock = _StepClock(0.0, 1.0, 1.0, 2.0)
    pacing = CrawlPacing(clock=clock, wall=lambda: 1_700_000_000.0)
    for _ in range(2):
        with pacing.request("p1", HOST):
            pass

    host = pacing.snapshot()["hosts"][HOST]
    assert host["window_s"] == 2.0
    assert host["requests_per_s"] == 1.0
    assert host["requests"] / host["window_s"] == host["requests_per_s"]


def test_single_request_has_no_rate_not_zero() -> None:
    """Один запрос: окно есть (длительность вызова), но частота наблюдаема — она равна
    ``1/window``; вырожденное окно даёт ``None``, а не бесконечность."""
    clock = _StepClock(0.0, 0.0)  # окно нулевой длины
    pacing = CrawlPacing(clock=clock, wall=lambda: 1_700_000_000.0)
    with pacing.request("p1", HOST):
        pass
    host = pacing.snapshot()["hosts"][HOST]
    assert host["window_s"] is None
    assert host["requests_per_s"] is None


# --------------------------------------------------------------------------- #
# (3) Ожидание лимитера — кумулятив по origin
# --------------------------------------------------------------------------- #


def test_limiter_waits_are_merged_by_origin() -> None:
    """``RateLimiter`` знает ``remaining``; прибор публикует его сумму по origin."""
    clock = _ManualClock()
    limiter = RateLimiter(sleep=clock.sleep, clock=clock, wall=lambda: 0.0)
    pacing = CrawlPacing(limiter=limiter)

    with pacing.request("p1", HOST):
        pass
    limiter.wait(HOST, RateLimitSpec(min_s=1.0, max_s=1.0))
    limiter.wait(HOST, RateLimitSpec(min_s=1.0, max_s=1.0))

    host = pacing.snapshot()["hosts"][HOST]
    # Первый резерв ждёт объявленную секунду, второй — ещё одну (цепь резервов).
    assert host["limiter_waits"] == 2
    assert host["limiter_waited_s"] == pytest.approx(2.0)


def test_zero_pause_is_not_counted_as_a_wait() -> None:
    """``min_s = max_s = 0``: резерв был, ожидания не было — ``waits == 0``."""
    clock = _ManualClock()
    limiter = RateLimiter(sleep=clock.sleep, clock=clock, wall=lambda: 0.0)
    pacing = CrawlPacing(limiter=limiter)
    with pacing.request("p1", HOST):
        pass
    limiter.wait(HOST, RateLimitSpec(min_s=0.0, max_s=0.0))
    host = pacing.snapshot()["hosts"][HOST]
    assert host["limiter_waits"] == 0
    assert host["limiter_waited_s"] == 0.0


# --------------------------------------------------------------------------- #
# (4) Агрегат по продукту и по прогону
# --------------------------------------------------------------------------- #


def test_product_and_total_aggregates_sum_requests_and_max_peaks() -> None:
    """Два продукта на одном origin: сумма запросов, **максимум** пиков, а не сумма."""
    pacing = CrawlPacing()
    barrier = threading.Barrier(2)

    def one(product_id: str) -> None:
        with pacing.request(product_id, HOST):
            barrier.wait(timeout=5)

    threads = [
        threading.Thread(target=one, args=("p1",)),
        threading.Thread(target=one, args=("p2",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    snapshot = pacing.snapshot()
    assert snapshot["hosts"][HOST]["in_flight_max"] == 2
    assert set(snapshot["products"]) == {"p1", "p2"}
    assert all(row["requests"] == 1 for row in snapshot["products"].values())
    total = snapshot["total"]
    assert total["requests"] == 2
    assert total["products"] == 2
    assert total["hosts"] == 1
    # Сумма пиков дала бы 2 по каждому продукту = 4 — это не наблюдалось ни в один момент.
    assert total["in_flight_max"] == 2


def test_product_aggregate_does_not_double_count_shared_host() -> None:
    """Каждый продукт видит свои запросы на общем origin'е, сумма — точная."""
    pacing = CrawlPacing()
    for product_id, count in (("p1", 1), ("p2", 3)):
        for _ in range(count):
            with pacing.request(product_id, HOST):
                pass
    products = pacing.snapshot()["products"]
    assert products["p1"]["requests"] == 1
    assert products["p2"]["requests"] == 3
    assert products["p2"]["hosts"] == [HOST]
    assert pacing.snapshot()["total"]["requests"] == 4


# --------------------------------------------------------------------------- #
# (5) Без прибора — не нули
# --------------------------------------------------------------------------- #


def test_no_limiter_publishes_zero_waits_not_missing_block() -> None:
    """Лимитера нет — ожидание нулевое (ждать было нечем), блок всё равно есть."""
    pacing = CrawlPacing()
    with pacing.request("p1", HOST):
        pass
    host = pacing.snapshot()["hosts"][HOST]
    assert host["limiter_waits"] == 0
    assert host["limiter_waited_s"] == 0.0


def test_pacing_from_provenance_absent_is_none() -> None:
    """Провенанс без блока (старый/прерванный сеанс) — ``None``, а не ``{}``-факт."""
    assert pacing_from_provenance(None) is None
    assert pacing_from_provenance({}) is None
    assert pacing_from_provenance({"pacing": {}}) is None
    assert pacing_from_provenance({"pacing": {"hosts": {}}}) == {"hosts": {}}


# --------------------------------------------------------------------------- #
# (5b) Артефакт прогона: блок ``crawl`` в metrics.json
# --------------------------------------------------------------------------- #


def test_pacing_reaches_metrics_json_without_losing_the_rate(tmp_path: Any) -> None:
    """``resources.crawl`` в ``metrics.json`` — те же числа, что в провенансе.

    Частота дробная (``0.048 req/s``): политика округления блока ``resources`` (1 знак)
    превратила бы её в ``0.0``, то есть в «не измеряли». Поэтому подблок ``crawl``
    округляется отдельно — тест это и пинит.
    """
    import json
    from pathlib import Path

    from core.pipeline.observability import write_run_metrics
    from core.pipeline.resources import build_resources_block

    pacing = pacing_from_provenance(
        {
            "pacing": {
                "hosts": {
                    HOST: {
                        "requests": 2,
                        "requests_per_s": 0.048,
                        "in_flight_max": 2,
                        "limiter_waits": 2,
                        "limiter_waited_s": 41.5,
                        "fetch_ms": {"p50": 1520, "p95": 7630, "n": 2},
                        "window_s": 41.625,
                        "first_request_at": "2026-01-01T00:00:00+00:00",
                        "last_request_at": "2026-01-01T00:00:41.625000+00:00",
                    }
                },
                "products": {},
                "total": {},
            }
        }
    )
    path = write_run_metrics(
        run_id="run_pacing",
        out_dir=Path(tmp_path),
        resources=build_resources_block(crawl_pacing=pacing),
    )
    crawl = json.loads(Path(path).read_text(encoding="utf-8"))["resources"]["crawl"]
    host = crawl["hosts"][HOST]
    assert host["requests_per_s"] == 0.048  # не 0.0 — иначе замер стёрт округлением
    assert host["in_flight_max"] == 2
    assert host["limiter_waited_s"] == 41.5
    assert host["window_s"] == 41.625
    assert host["fetch_ms"] == {"p50": 1520, "p95": 7630, "n": 2}


# --------------------------------------------------------------------------- #
# (6) Провенанс, не идентичность
# --------------------------------------------------------------------------- #


def test_no_pacing_field_enters_the_execution_contract() -> None:
    """Ни одно имя поля темпа не входит в контракт — иначе замер менял бы ``run_id``."""
    for name in (
        "pacing",
        "in_flight_max",
        "requests",
        "requests_per_s",
        "limiter_waits",
        "limiter_waited_s",
        "fetch_ms",
        "window_s",
    ):
        assert name not in EXECUTION_CONTRACT_FIELDS


def test_contract_digest_is_unchanged_when_pacing_is_measured() -> None:
    """Те же объявленные входы — тот же ``execution_contract_digest``.

    Измерение приходит **провенансом** (``provenance["pacing"]``), а не полем контракта:
    попытка положить его в контракт — ошибка «неизвестные поля», а не тихое расширение.
    """
    values: dict[str, Any] = {
        field: f"v-{field}" for field in EXECUTION_CONTRACT_FIELDS
    }
    baseline = execution_contract_digest(values)
    measured = {"hosts": {HOST: {"in_flight_max": 2, "requests_per_s": 1.5}}}
    assert execution_contract_digest(values) == baseline
    with pytest.raises(ValueError, match="неизвестные поля"):
        execution_contract_digest({**values, "pacing": measured})