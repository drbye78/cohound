"""Драйвер полос продукта: чередование корней по праву на host (F4b, план 17 §6).

**Что здесь.** Один продукт — это список seed-корней в авторском порядке
(``packages/{case_id}/sources/corpus_urls.yaml``). Драйвер ведёт до
``crawl.host_concurrency`` корней одновременно и решает, **когда** волне корня можно
уйти в сеть (``_dispatch_wave``) и когда её исходы можно закоммитить.

**Полоса — не объект, а право на host.** Один корень штатно фетчит несколько host'ов
(``domain_profiles``), поэтому «полоса = host» структурой не выражается. Выражается
claim'ом: волна корня допускается, только если её множество host'ов
(:attr:`WavePlan.hosts`, ключ — ``LocalFetch.host``, тот же, что у ``limiter.wait``) не
пересекается с занятыми; claim берётся на волну и снимается на её коммите. Носитель
состояния — корень; полос-объектов (и потоков-полос) нет.

**Почему это безопасно.** Всё состояние фазы (сеанс, ``counters``, корпус, ``errors``)
мутирует только на главном потоке — то есть на потоке драйвера (T5). Потоки исполняют
**ровно одну** функцию — :func:`core.pipeline.corpus_ingest.local_fetch` (пейсинг +
сетевой вызов); коммит исхода остаётся на главном потоке. Поэтому «конкурентность» здесь
кооперативная: драйвер приостанавливает корень в точке плана волны и возобновляет его,
когда исходы готовы.

**Ожидание — мультиплексированное.** Драйвер ждёт ``concurrent.futures.wait`` по
**объединению** future'ов всех полос (``FIRST_COMPLETED``), а не ``result()`` конкретного
future: ожидание конкретного future повесило бы продукт на мёртвой полосе (медленный
браузерный фетч, таймаут источника). Завершившиеся полосы собираются по той, у которой
**все** future'ы волны готовы, — порядок коммитов внутри корня от этого не зависит (он
FIFO по записям волны).

**Авторский порядок.** Очередь корней — итератор в авторском порядке, и он **ленивый**:
подготовка корня (развёртка каталога, резолв цели, ``max_attempts``) исполняется в момент
допуска. Поэтому при ``host_concurrency`` ``None``/``1`` последовательность побочных
эффектов прежняя байт-в-байт: корень N+1 не планируется, пока корень N не дренирован.
Оракул — ``tests/pipeline/test_corpus_write_order.py``.

**Teardown.** Каждый допущенный корень обязан либо завершиться, либо быть **названным** в
``errors``: при исключении живые генераторы закрываются (их ``finally`` исполняются
сейчас, а не при сборке мусора), очередь экстракции отбрасывается через ``abort`` —
штатный путь фазы (``corpus_commit.abort_extract_pending``), а не молчаливое выпадение.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import FIRST_COMPLETED, Executor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from core.pipeline.corpus_commit import RootDrain
from core.pipeline.corpus_drain import (
    RootPlan,
    WaveOutcomes,
    WavePlan,
    _await_wave,
    _dispatch_wave,
)
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import LocalFetch
from core.pipeline.crawl_frontier import Frontier
from core.ports.types import RawPage

#: Потолок одного мультиплексированного ожидания, секунды. Это **не** таймаут фетча
#: (его у пула нет и быть не должно): ``FIRST_COMPLETED`` будит драйвер на первом готовом
#: future. Потолок — страховка от вырожденного набора: пустое множество future'ов или
#: полоса, чей future уже отменён снаружи, иначе оставили бы драйвер в вечном ``wait``.
_LANE_WAIT_TIMEOUT_S = 0.25

#: Исход корня: провенанс усечения, отказы scope у ``<loc>`` и хвост экстракции (C1).
@dataclass(frozen=True)
class RootOutcome:
    """Всё, что корень отдаёт фазе после завершения (значение ``StopIteration``)."""

    plan: RootPlan
    frontier: Frontier
    sitemap_skips: list[dict[str, Any]]
    drain: RootDrain


@dataclass
class _Lane:
    """Активный корень: генератор шагов, его пулы и занятое право на host'ы.

    Номера корня здесь нет: порядок — авторский, и его держит очередь драйвера, а не
    поле полосы (поле никем не читалось).
    """

    plan: RootPlan
    steps: (
        Generator[WavePlan, WaveOutcomes, tuple[Frontier, list[dict[str, Any]], RootDrain]]
        | None
    )
    fetch_pool: Executor | None = None
    browser_pool: Executor | None = None
    #: План волны, ещё не отправленный (``None`` — волна отправлена или корень завершён).
    wave: WavePlan | None = None
    #: Отправленная волна (``None`` — нечего ждать).
    request: Any = None
    #: Готовые исходы текущей волны — их получает генератор при возобновлении.
    outcomes: WaveOutcomes | None = None
    #: Право на host'ы, взятое на текущую волну.
    claim: frozenset[str] = frozenset()
    #: Генератор уже запускался (``next``) ⇒ следующее продвижение — ``send``.
    started: bool = False
    #: Момент, с которого драйвер ждёт эту волну (бакет ``fetch_wait_s``).
    wait_started: float | None = None
    #: Исход завершившегося корня (``(frontier, sitemap_skips, RootDrain)``).
    outcome: tuple[Frontier, list[dict[str, Any]], RootDrain] | None = None
    #: Корень завершился на этом продвижении.
    done: bool = False


_NO_PLAN: Any = object()


def drive_roots(
    plans: Iterable[RootPlan],
    *,
    host_concurrency: int | None,
    begin: Callable[..., Generator[WavePlan, WaveOutcomes, Any]],
    lane_width: Callable[[RootPlan], int | None],
    local_fetch: Callable[[LocalFetch], RawPage],
    timings: _CorpusStageTimings,
    errors: list[str],
    pool_factory: Callable[[int], Executor] | None = None,
    on_root_done: Callable[[RootOutcome], None] | None = None,
    abort: Callable[[str], None] | None = None,
    label: str = "",
) -> list[RootOutcome]:
    """Провести корни продукта полосами и вернуть их исходы в порядке завершения.

    :param plans: корни в **авторском** порядке; итератор может быть ленивым (подготовка
        корня исполняется при допуске — так ``host_concurrency`` ``None``/``1`` остаётся
        прежним последовательным циклом).
    :param host_concurrency: объявленная ось (``crawl.host_concurrency``). ``None``/``1`` —
        одна полоса, сегодняшнее поведение.
    :param begin: ``begin(plan, fetch_pool=…, browser_pool=…)`` — генератор корня
        (:func:`core.pipeline.corpus_drain.root_lifecycle` с уже связанными носителями:
        продуктовый скоуп, фазовый скоуп, операции фазы). Пулы приходят от полосы, потому
        что их число ограничено правом на host'ы, а не числом корней.
    :param lane_width: ширина пула фетча для корня
        (:func:`core.pipeline.corpus_drain.lane_width`); ``None`` — живого пула нет.
    :param pool_factory: фабрика пула фетча (D4, инжектится тестами); ``None`` —
        ``ThreadPoolExecutor``.
    :param on_root_done: вызывается **на главном потоке** сразу по завершении корня —
        здесь фаза строит финализатор провенанса и регистрирует хвост (C1).
    :param abort: отбросить очередь экстракции на teardown
        (``corpus_commit.abort_extract_pending``), назвав причину.
    :param label: префикс строк ``errors`` (обычно ``f" {product_id}"``).
    :param errors: канал видимости фазы: прерванный корень обязан быть назван.
    :returns: исходы в порядке завершения корней.

    **Порядок корней ≠ порядок записей.** ``truncation_records`` пишется в слот, занятый
    **при планировании** (``RootPlan.record_slot``), поэтому артефакт манифеста остаётся
    авторским — см. :meth:`core.pipeline.corpus_drain.RootPlan.record_slot`.
    """
    lanes_limit = 1 if host_concurrency is None else max(1, int(host_concurrency))
    queue = iter(plans)
    queue_open = True
    lanes: list[_Lane] = []
    #: Занятые host'ы: держит только отправленная волна (снимается на её коммите).
    claimed: set[str] = set()
    done: list[RootOutcome] = []

    def _close(lane: _Lane) -> None:
        """Закрыть генератор корня и его пулы. Сбой закрытия виден, но не маскирует исход."""
        if lane.steps is not None:
            try:
                lane.steps.close()
            except Exception as exc:  # noqa: BLE001 — teardown не подменяет исход
                errors.append(
                    f"crawl-драйвер{label}: close({lane.plan.root_url}): "
                    f"{type(exc).__name__}: {exc}"
                )
            lane.steps = None
        for pool in (lane.fetch_pool, lane.browser_pool):
            if pool is None:
                continue
            try:
                pool.shutdown(wait=True)
            except Exception as exc:  # noqa: BLE001 — как выше
                errors.append(
                    f"crawl-драйвер{label}: shutdown({lane.plan.root_url}): "
                    f"{type(exc).__name__}: {exc}"
                )
        lane.fetch_pool = None
        lane.browser_pool = None

    def _release(lane: _Lane) -> None:
        """Снять право корня на host'ы (только после коммита волны)."""
        claimed.difference_update(lane.claim)
        lane.claim = frozenset()

    def _open(plan: RootPlan) -> _Lane:
        """Открыть полосу под корень: пулы — по ``lane_width``, генератор — ``begin``."""
        width = lane_width(plan)
        lane = _Lane(plan=plan, steps=None)
        try:
            if width is not None:
                lane.fetch_pool = (
                    pool_factory(width)
                    if pool_factory is not None
                    else ThreadPoolExecutor(max_workers=width)
                )
                # Не-poolable маршрут (browser) — свой пул из **одного** потока:
                # ``BrowserCrawler`` водит единственную ``_page`` без лока, а исполнение
                # на потоке драйвера остановило бы все полосы. Пустой пул потоки не
                # запускает, поэтому он не стоит ничего, пока браузерной волны нет.
                lane.browser_pool = ThreadPoolExecutor(max_workers=1)
            lane.steps = begin(
                plan, fetch_pool=lane.fetch_pool, browser_pool=lane.browser_pool
            )
        except BaseException:
            _close(lane)
            raise
        return lane

    def _pump(lane: _Lane) -> bool:
        """Довести генератор корня до следующего плана волны или до его завершения.

        ``True`` — план готов (``lane.wave``); ``False`` — корень завершился
        (исход в ``lane.outcome``). Вся мутация состояния идёт здесь, на главном потоке.
        """
        steps = lane.steps
        if steps is None:
            # Инвариант: закрытый генератор не продвигается (``_finish`` снимает полосу).
            raise AssertionError(f"{lane.plan.root_url}: продвижение закрытой полосы")
        try:
            if lane.started:
                if lane.outcomes is None:
                    # Инвариант: возобновление без исходов — дефект драйвера, а не
                    # режим (``_settle`` кладёт исходы перед ``_pump``).
                    raise AssertionError(
                        f"{lane.plan.root_url}: возобновление без исходов волны"
                    )
                lane.wave = steps.send(lane.outcomes)
            else:
                lane.wave = next(steps)
                lane.started = True
        except StopIteration as finished:
            lane.wave = None
            lane.outcomes = None
            lane.outcome = finished.value
            return False
        lane.outcomes = None
        return True

    def _dispatch(lane: _Lane) -> None:
        """Отправить волну корня и взять право на её host'ы."""
        wave = lane.wave
        if wave is None:
            raise AssertionError(f"{lane.plan.root_url}: отправка без плана волны")
        if wave.non_poolable and lane.browser_pool is None:
            # Пара к ``WavePlan.non_poolable``: право на host'ы и маршрут решены до
            # отправки, поэтому отсутствие пула браузера видно здесь, с именем корня,
            # а не как общий отказ внутри ``_dispatch_wave``.
            raise AssertionError(
                f"{lane.plan.root_url}: не-poolable маршрут без пула браузера"
            )
        lane.wave = None
        claimed.update(wave.hosts)
        lane.claim = wave.hosts
        lane.request = _dispatch_wave(
            wave.entries,
            pool=lane.fetch_pool,
            non_poolable_pool=lane.browser_pool,
            local_fetch=local_fetch,
        )
        lane.wait_started = time.perf_counter()

    def _settle(lane: _Lane) -> None:
        """Дождаться волны, отдать исходы корню (коммит) и снять право на host'ы."""
        request = lane.request
        if request is None:
            raise AssertionError(f"{lane.plan.root_url}: ожидание без отправленной волны")
        started = (
            lane.wait_started
            if lane.wait_started is not None
            else time.perf_counter()
        )
        lane.request = None
        lane.wait_started = None
        lane.outcomes = _await_wave(request, timings=timings)
        # Ожидание полосы целиком — в тот же бакет «участок фетча»: после ``wait``
        # вызовы ``result`` внутри ``_await_wave`` уже не спят. При полосах это сумма
        # ожиданий корней, а не wall time продукта (он публикуется строкой продукта) —
        # иначе «ожидание» читалось бы как работа.
        timings.fetch_wait_s += time.perf_counter() - started
        lane.done = not _pump(lane)
        # Право снимается **после** коммита: коммит исполняется внутри ``_pump``
        # (генератор применяет ``_commit_wave_results`` сразу после получения исходов).
        _release(lane)

    def _finish(lane: _Lane) -> None:
        """Завершённый корень: закрыть пулы, отдать исход фазе, освободить полосу."""
        lanes.remove(lane)
        _close(lane)
        outcome = lane.outcome
        if outcome is None:
            raise AssertionError(f"{lane.plan.root_url}: завершение без исхода")
        root = RootOutcome(
            plan=lane.plan,
            frontier=outcome[0],
            sitemap_skips=outcome[1],
            drain=outcome[2],
        )
        done.append(root)
        if on_root_done is not None:
            on_root_done(root)

    try:
        while True:
            # (1) Допуск корней: полоса — слот, корни берутся в авторском порядке.
            while queue_open and len(lanes) < lanes_limit:
                plan = next(queue, _NO_PLAN)
                if plan is _NO_PLAN:
                    queue_open = False
                    break
                lane = _open(plan)
                lanes.append(lane)
                # Подготовка корня (limits/frontier/seeds/зарядка бюджета) — на первом
                # ``next``: при ``host_concurrency`` None/1 это ровно прежний момент.
                if not _pump(lane):
                    _finish(lane)
            # (2) Допуск волн: право на host'ы выдаётся в авторском порядке корней,
            # поэтому припаркованный корень не блокирует остальных, а ждёт своей очереди.
            for lane in list(lanes):
                if lane.wave is None or lane.request is not None:
                    continue
                if lane.wave.hosts & claimed:
                    continue
                _dispatch(lane)
                if lane.request is not None and not _futures(lane.request):
                    # Волна без живого фетча (блэклист/исчерпанные попытки/resume):
                    # ждать нечего — коммитим сразу, полоса не простаивает.
                    _settle(lane)
                    if lane.done:
                        _finish(lane)
            # (3) Мультиплексированное ожидание по объединению future'ов всех полос.
            in_flight = [lane for lane in lanes if lane.request is not None]
            if not in_flight:
                if lanes:
                    # Право держит только отправленная волна, поэтому «ничего не
                    # отправлено» ⇒ права пусты ⇒ все готовые волны допущены в (2).
                    # Нарушение — дефект драйвера, а не режим: лучше упасть, чем висеть.
                    raise AssertionError(
                        "crawl-драйвер: полосы без отправленной волны и без права на host"
                    )
                if not queue_open:
                    break
                continue
            futures = {
                future
                for lane in in_flight
                for future in _futures(lane.request)
            }
            wait(
                futures,
                timeout=_LANE_WAIT_TIMEOUT_S,
                return_when=FIRST_COMPLETED,
            )
            for lane in in_flight:
                pending = _futures(lane.request)
                if pending and not all(future.done() for future in pending):
                    continue
                _settle(lane)
                if lane.done:
                    _finish(lane)
    except BaseException as exc:
        # Ни один допущенный корень не выпадает молча: он либо завершился, либо назван.
        if lanes:
            names = ", ".join(lane.plan.root_url for lane in lanes)
            errors.append(
                f"crawl-драйвер{label}: {type(exc).__name__}: {exc} — прервано корней "
                f"без исхода {len(lanes)}: {names}"
            )
            if abort is not None:
                try:
                    abort(f"crawl interrupted: {type(exc).__name__}: {exc}")
                except Exception as abort_exc:  # noqa: BLE001 — не подменяем исход
                    errors.append(
                        f"crawl-драйвер{label}: abort очереди экстракции: "
                        f"{type(abort_exc).__name__}: {abort_exc}"
                    )
        raise
    finally:
        for lane in list(lanes):
            _close(lane)
        lanes.clear()
    return done


def drain_one(plan: RootPlan, **kwargs: Any) -> RootOutcome:
    """Один корень без чередования — прежний путь synthetic-бакетов и orphan'ов.

    Аргументы — те же, что у :func:`drive_roots` (``host_concurrency=1``), поэтому
    подготовка, волны, коммит и teardown у одиночного корня и у полосы — **один** код.
    """
    outcomes = drive_roots([plan], host_concurrency=1, **kwargs)
    if len(outcomes) != 1:
        raise AssertionError(
            f"{plan.root_url}: одиночный дренаж вернул исходов {len(outcomes)}"
        )
    return outcomes[0]


def _futures(request: Any) -> list[Any]:
    """Живые future'ы отправленной волны (порядок записей — порядок отправки)."""
    entries = getattr(request, "entries", ())
    return [
        entry.future for entry in entries if getattr(entry, "future", None) is not None
    ]