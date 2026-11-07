"""Пейсящиеся повторы фетча — **одно** правило на все пути краула.

Зачем отдельный модуль. Повтор ``5xx``/``408``/``429``/сетевого сбоя обязан быть один:
ретрай, живущий только в одном из путей, — это ровно тот дефект, который чинили
07.11.2026. Тогда ``RETRY_TIMES: 2`` движка Scrapy исполнялся **внутри** одного вызова
``fetch``, то есть внутри одного резерва лимитера: на уже ответивший ошибкой хост уходило
до трёх запросов без интервала, а ``attempts`` элемента оставался ``1``
(``docs/reports/measurements/2026-11-07-crawl-rate-tantor.md`` §7.6). Повторы вынесли в
слой фетча, и у слоя фетча **два** потребителя: локальный путь
(:func:`core.pipeline.corpus_ingest.local_fetch`) и edge-планировщик под ``split``
(:mod:`core.edge.scheduler`). Две копии цикла разошлись бы молча — так же, как когда-то
разошлись два пейсера.

**Что здесь есть и чего нет.** Здесь — **механика** повтора: сколько попыток
(:data:`FETCH_RETRY_TIMES`), что каждая берёт **свой** слот лимитера и что замер попытки
(``fetch_ms``, момент последнего резерва, число повторов) ведётся в одном месте. Здесь
**нет** классификации отказов: «повтор осмыслен» — свойство класса отказа, а таксономия
живёт в порту (:func:`core.ports.crawl_errors.is_transient_fetch_error`). Поэтому предикат
приходит параметром ``is_transient``: ``util`` не может импортировать ``ports`` (матрица
слоёв §1.3), а передают его оба вызывающих **один и тот же** — копии правила не возникает.

**Слот — резерв, а не сон.** Каждая попытка зовёт :meth:`RateLimiter.wait_reserved`:
это тот же резерв origin'а, что и :meth:`RateLimiter.wait` (``target = max(now, last) +
delay``, сон вне лока), но он ещё и возвращает **разрешённый** момент — его пишет
провенанс попытки (``fetch_started_at``). Считать слот по вызову ``wait`` поэтому нельзя:
``wait_reserved`` делает ровно тот же резерв, просто с меткой.

Модуль категорийно-нейтрален (§1.3): только stdlib и примитивы пейсинга.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any

from core.domain.models import RateLimitSpec
from core.util.rate_limit import RateLimiter

__all__ = [
    "FETCH_RETRY_TIMES",
    "FetchAttemptStats",
    "fetch_with_paced_retries",
]

#: Сколько раз живой фетч повторяется на транзиентный исход (сверх первой попытки).
#: Число совпадает с прежним ``RETRY_TIMES`` движка Scrapy (2), чтобы по числу попыток
#: поведение не менялось — изменилось только то, что каждая попытка берёт свой слот
#: лимитера. **Не** ось контракта: это исполнение, а не условие измерения, и в
#: ``run_id``/``crawl_config_digest`` оно не входит. Одна константа на локальный путь и
#: edge: два литерала означали бы два разных правила повтора.
FETCH_RETRY_TIMES = 2


@dataclass
class FetchAttemptStats:
    """Наблюдённые факты одной попытки-серии (провенанс, не контракт).

    Заполняется помощником даже при исключении — вызывающий читает поля в ``finally``.

    :param retries: сколько раз фетч был **повторён** (каждый повтор — свой слот).
    :param fetch_ms: полная длительность работы: все попытки и паузы между ними, без
        первого ожидания резерва (то же окно, что у прежнего замера одной попытки).
    :param started_at: **разрешённый** момент последней попытки (ISO-8601 UTC) —
        момент резерва, а не пробуждения (см. :meth:`RateLimiter.wait_reserved`).
        ``None`` — резерва не было (исключение до первой попытки).
    """

    retries: int = 0
    fetch_ms: int = 0
    started_at: str | None = None


def fetch_with_paced_retries(
    fetch: Callable[[], Any],
    *,
    host: str,
    rate_limit: RateLimitSpec | None,
    limiter: RateLimiter,
    is_transient: Callable[[BaseException], bool],
    stats: FetchAttemptStats | None = None,
    attempt_scope: Callable[[], AbstractContextManager[Any]] | None = None,
    on_retry: Callable[[], None] | None = None,
    clock: Callable[[], float] = time.perf_counter,
) -> Any:
    """Фетч с повторами, **каждый из которых берёт свой слот** лимитера.

    :param fetch: собственно сетевой вызов (``() -> page``); вызывается ровно столько
        раз, сколько нужно.
    :param host: origin пейсинга (``urlsplit(url).hostname.lower()``).
    :param rate_limit: объявленная ось пейсинга; ``None`` — резерва нет (нулевой пейсинг).
    :param limiter: резервирует слот перед каждой попыткой и спит до него.
    :param is_transient: предикат «повтор осмыслен» — **один** на все пути
        (:func:`core.ports.crawl_errors.is_transient_fetch_error`).
    :param stats: приёмник наблюдённых фактов (новый, если не передан).
    :param attempt_scope: обёртка **только** сетевого вызова (прибор темпа); ожидание
        слота в неё не входит — спящий поток к хосту не обращается.
    :param on_retry: хук решения о повторе (учёт в приборе), зовётся до следующей попытки.
    :param clock: монотонные часы длительности (инъектируются в тестах).

    Поведение то же, что было у локального пути до выноса: успешный фетч делает ровно
    один резерв, повтор — новый резерв; исчерпание повторов поднимает последнее
    исключение (не тихое «получилось»); терминальный исход поднимается сразу.
    """
    attempts = 1 + FETCH_RETRY_TIMES
    observed = stats if stats is not None else FetchAttemptStats()
    started: float | None = None
    try:
        for attempt in range(1, attempts + 1):
            observed.started_at = limiter.wait_reserved(host, rate_limit)
            if started is None:
                # Отсчёт начинается после первого резерва: ожидание слота — не работа
                # фетча (та же граница, что у прежнего замера одной попытки).
                started = clock()
            scope = (
                attempt_scope()
                if attempt_scope is not None
                else contextlib.nullcontext()
            )
            with scope:
                try:
                    return fetch()
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    if attempt >= attempts or not is_transient(exc):
                        raise
                    observed.retries += 1
                    if on_retry is not None:
                        on_retry()
    finally:
        if started is not None:
            observed.fetch_ms = int((clock() - started) * 1000)
    raise AssertionError("цикл повторов обязан вернуть результат или поднять исключение")