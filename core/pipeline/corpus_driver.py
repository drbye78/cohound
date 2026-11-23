"""Драйвер полос когорты: чередование корней по праву на host (F4b, F9 плана 17).

**Что здесь.** Когорта — это продукты в авторском порядке, каждый со списком seed-корней
(``packages/{case_id}/sources/corpus_urls.yaml``). Драйвер ведёт корни **всех** продуктов
когорты одним потоком планов и до ``crawl.host_concurrency`` корней одновременно, решает,
**когда** волне корня можно уйти в сеть (``_dispatch_wave``) и когда её исходы можно
закоммитить. Продукт — граница **коммита** (его хвост исполняется, когда завершён его
последний корень), а не граница взаимного исключения.

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
future: ожидание конкретного future повесило бы когорту на мёртвой полосе (медленный
браузерный фетч, таймаут источника). Завершившиеся полосы собираются по той, у которой
**все** future'ы волны готовы, — порядок коммитов внутри корня от этого не зависит (он
FIFO по записям волны).

**Авторский порядок.** Очередь корней — итератор в авторском порядке (продукты в порядке
когорты, корни — в авторском порядке внутри продукта), и он **ленивый**: подготовка корня
(развёртка каталога, резолв цели, ``max_attempts``) исполняется в момент допуска. Поэтому
при ``host_concurrency`` ``None``/``1`` последовательность побочных эффектов прежняя
байт-в-байт: корень N+1 не планируется, пока корень N не дренирован, а продукт N+1 не
начинается, пока не закрыт продукт N. Оракул — ``tests/pipeline/test_corpus_write_order.py``.

**Границы продукта — в потоке планов.** Итератор выдаёт не только корни, но и
:class:`ProductBoundary` — «корни продукта спланированы целиком». Без маркера драйвер не
отличил бы «у продукта ещё будут корни» от «продукт спланирован», и закрыл бы продукт на
первом же завершённом корне. Продукт с нулём корней тоже получает границу, поэтому его
хвост исполняется, как и прежде.

**Хвост продукта — на главном потоке и с видимой ценой.** Конверсия, Pass B, покрытие и
строка продукта исполняются в :func:`_close_product` в момент завершения последнего корня
продукта. Осознанный компромисс: на время хвоста **выдача новых волн стоит** (уже
отправленные доживают в пулах), поэтому пик памяти ограничен числом полос, а не числом
продуктов в полёте. Цена этого компромисса не молчит: время хвоста суммируется в
``product_tail_s`` бакетов продукта, то есть видно в ``metrics.json`` —
``resources.stage_timings``.

**Браузерный маршрут — свой пул движков.** Playwright sync-API привязан к потоку,
создавшему движок: обращение к контексту из другого потока — отказ
(``cannot switch to a different thread``). Поэтому браузерные элементы всех полос идут в
**отдельный** пул (:func:`_dispatch_wave`, ``browser_pool``), который приходит извне и
переживает полосу: пул — владелец движков, а не корень. Размер пула — объявленная ось
``browser_workers``: при ``None``/``1`` это прежний один поток и один движок, при ``N`` —
N потоков, каждый со своим движком (их поднимает фабрика фазы, лениво, внутри воркера).
Драйвер такой пул не закрывает (у полосы ``_Lane.owns_browser_pool`` ``False``: чужое не
закрываем) — его закрывает тот, кто создал (фаза — в своём ``finally``, а если пул создан
здесь, то ``drive_roots`` в своём).

**Объявленный параллелизм ≠ случившийся.** Строка ``notes`` публикует три числа: сколько
браузерных фетчей прошло, сколько их было **в полёте одновременно** на пике
(``browser_in_flight_max`` — наблюдение, а не объявление) и суммарное ожидание волн с
браузерным элементом. Иначе «объявили 3 движка» и «три действительно работали
одновременно» были бы неотличимы.

**Размыкатель сети — здесь, а не в воркерах.** Если канал пропал целиком, честные повторы
фетча превратили бы всю оставшуюся когорту в провалы, а «мы не смотрели» и «посмотрели и не
нашли» — разные факты (I2d). Наблюдение поэтому стоит на главном потоке драйвера: исходы
волны уже собраны из пула, а состояние процессов не разделяется — общий счётчик в воркерах
был бы ложью.

Срабатывание (:mod:`core.util.network_breaker`: N отказов «ответа не было» на M разных
хостах) **называется** строкой ``network_paused`` в ``errors`` с числами и хостами и даёт
паузу: выдача новых волн встаёт, уже отправленные доживают в пулах. После паузы прогон
**продолжается** — для моргающего канала (переключение wifi, короткий обрыв VPN) это
дешевле остановки, которая превратила бы блип в брошенную когорту. Если пауза не помогла и
серия собралась снова, ``BreakerPolicy.max_trips`` исчерпан: выдача прекращается, остаток
честно остаётся ``pending`` (не ``failed``), сеанс финализируется с названной причиной.

**Teardown.** Каждый допущенный корень обязан либо завершиться, либо быть **названным** в
``errors``: при исключении живые генераторы закрываются (их ``finally`` исполняются
сейчас, а не при сборке мусора), а ``abort`` называет, сколько строк очереди конверсии
осталось ``pending`` — durable-факт «документ добыт» не отбрасывается и не теряется молча.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import FIRST_COMPLETED, Executor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from core.pipeline.corpus_drain import (
    RootPlan,
    WaveOutcomes,
    WavePlan,
    WaveRequest,
    _await_wave,
    _dispatch_wave,
)
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import LocalFetch
from core.pipeline.crawl_frontier import Frontier
from core.ports.crawl_errors import TERMINAL_FETCH_ERRORS, CrawlError
from core.ports.types import RawPage
from core.util.network_breaker import NetworkBreaker

#: Потолок одного мультиплексированного ожидания, секунды. Это **не** таймаут фетча
#: (его у пула нет и быть не должно): ``FIRST_COMPLETED`` будит драйвер на первом готовом
#: future. Потолок — страховка от вырожденного набора: пустое множество future'ов или
#: полоса, чей future уже отменён снаружи, иначе оставили бы драйвер в вечном ``wait``.
_LANE_WAIT_TIMEOUT_S = 0.25


def _fetch_silence(exc: BaseException | None) -> bool:
    """Отказ **без ответа**: таймаут, обрыв, DNS — канал, а не адресат.

    Признак берётся из таксономии порта, а не из текста ошибки: транзиентный
    :class:`CrawlError` **без** ``http_status`` — это ровно «запрос не дошёл».
    Исключены терминальные классы (отказ политики, SSRF, «ресурса нет», потолок,
    движок не поднялся): это решения, а не наблюдения о канале.

    ``5xx``/``4xx``/``304`` сюда не попадают намеренно: сервер ответил, значит канал
    жив, и размыкать сеть из-за одного больного хоста нельзя (см.
    :mod:`core.util.network_breaker`).
    """
    if exc is None or not isinstance(exc, CrawlError):
        return False
    if isinstance(exc, TERMINAL_FETCH_ERRORS):
        return False
    return getattr(exc, "http_status", None) is None


def _fetch_answered(exc: BaseException | None) -> bool:
    """Был ли ответ (в том числе ``4xx``/``5xx``) — тогда канал подал признак жизни.

    Ответом считается любой :class:`CrawlError` с ``http_status``: ``404``, ``403``,
    ``503`` и ``304`` одинаково доказывают, что запрос дошёл и адресат отвечает.
    Прочие исключения (дефект транспорта) — не наблюдение о канале: они не сбрасывают
    серию, но и не удлиняют её.
    """
    if exc is None or not isinstance(exc, CrawlError):
        return False
    return getattr(exc, "http_status", None) is not None


@dataclass(frozen=True)
class ProductBoundary:
    """Граница продукта в потоке планов: «корни этого продукта спланированы целиком».

    Зачем маркер, а не вывод из ``plan.product_id``. Смена продукта в потоке говорит лишь
    «начался следующий», а последний продукт потока и продукт **без корней** так не видны;
    без маркера их хвосты не исполнились бы вовсе. Маркер называет факт прямо, поэтому
    драйвер не гадает: спланировано всё — закрывай, когда завершатся корни.
    """

    product_id: str


#: Исход корня: провенанс усечения и отказы scope у ``<loc>``.
@dataclass(frozen=True)
class RootOutcome:
    """Всё, что корень отдаёт фазе после завершения (значение ``StopIteration``).

    Хвоста экстракции здесь нет (F5): краул не конвертирует, поэтому после корня
    доводить нечего — документы уже строки очереди конверсии.
    """

    plan: RootPlan
    frontier: Frontier
    sitemap_skips: list[dict[str, Any]]


@dataclass
class _Lane:
    """Активный корень: генератор шагов, его пулы и занятое право на host'ы.

    Номера корня здесь нет: порядок — авторский, и его держит очередь драйвера, а не
    поле полосы (поле никем не читалось).
    """

    plan: RootPlan
    steps: (
        Generator[WavePlan, WaveOutcomes, tuple[Frontier, list[dict[str, Any]]]] | None
    )
    #: Бакеты стадий **продукта** этого корня (F9: у продукта свой носитель).
    timings: _CorpusStageTimings
    fetch_pool: Executor | None = None
    #: Пул браузерного маршрута. Он **общий на когорту**: в нём живут движки Playwright,
    #: привязанные к своим потокам (по движку на поток), поэтому полоса им не владеет.
    browser_pool: Executor | None = None
    #: Владеет ли полоса своим браузерным пулом. ``False`` — пул чужой (фазы или
    #: ``drive_roots``), закрывать его полосе нельзя: закрытие пула убило бы поток
    #: владельца движка для всех остальных корней.
    owns_browser_pool: bool = False
    #: Сколько элементов ушло в браузерный пул за корень (видимость параллелизма).
    browser_entries: int = 0
    #: Сколько браузерных фетчей этой полосы сейчас в полёте (отправлено, не закоммичено).
    #: Нужен ровно для одного числа — пика одновременных фетчей когорты.
    browser_in_flight: int = 0
    #: Суммарное ожидание волн, в которых был браузерный элемент, секунды.
    browser_wait_s: float = 0.0
    #: Отправленная волна содержит браузерный элемент: её ожидание идёт в
    #: ``browser_wait_s`` (ожидание относится к волне, а не к её отдельным записям —
    #: записи волны ждут одну и ту же браузерную очередь и не суммируются дважды).
    browser_wave: bool = False
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
    #: Исход завершившегося корня (``(frontier, sitemap_skips)``).
    outcome: tuple[Frontier, list[dict[str, Any]]] | None = None
    #: Корень завершился на этом продвижении.
    done: bool = False


_NO_PLAN: Any = object()

#: Метка «в очереди ничего нет» для плана, уже изъятого, но не допущенного (лимит полос
#: продукта). Отдельный объект, чтобы не путать с ``None`` (``None`` — допустимый pid).
_NO_PENDING: Any = object()


def drive_roots(
    plans: Iterable[RootPlan | ProductBoundary],
    *,
    host_concurrency: int | None,
    begin: Callable[..., Generator[WavePlan, WaveOutcomes, Any]],
    lane_width: Callable[[RootPlan], int | None],
    local_fetch: Callable[[LocalFetch], RawPage],
    timings_for: Callable[[str | None], _CorpusStageTimings],
    errors: list[str],
    pool_factory: Callable[[int], Executor] | None = None,
    browser_pool: Executor | None = None,
    browser_workers: int | None = None,
    notes: list[str] | None = None,
    on_root_done: Callable[[RootOutcome], None] | None = None,
    on_product_done: Callable[[str], None] | None = None,
    lane_limit_for: Callable[[RootPlan], int | None] | None = None,
    abort: Callable[[str], None] | None = None,
    network_breaker: NetworkBreaker | None = None,
    sleep: Callable[[float], None] = time.sleep,
    label: str = "",
) -> list[RootOutcome]:
    """Провести корни когорты полосами и вернуть их исходы в порядке завершения.

    :param plans: корни в **авторском** порядке и :class:`ProductBoundary` после
        последнего корня каждого продукта. Итератор может быть ленивым (подготовка корня
        исполняется при допуске — так ``host_concurrency`` ``None``/``1`` остаётся прежним
        последовательным циклом).
    :param host_concurrency: объявленная ось (``crawl.host_concurrency``). ``None``/``1`` —
        одна полоса, сегодняшнее поведение. Это потолок **когорты**; потолок продукта
        приходит из ``lane_limit_for`` (ось объявлена на корень, а не на когорту).
    :param begin: ``begin(plan, fetch_pool=…, browser_pool=…)`` — генератор корня
        (:func:`core.pipeline.corpus_drain.root_lifecycle` с уже связанными носителями:
        продуктовый скоуп, фазовый скоуп, операции фазы). Пулы приходят от полосы, потому
        что их число ограничено правом на host'ы, а не числом корней.
    :param lane_width: ширина пула фетча для корня
        (:func:`core.pipeline.corpus_drain.lane_width`); ``None`` — живого пула нет.
    :param timings_for: бакеты стадий **продукта** по ``plan.product_id``: ожидание волны
        продукта не имеет права попасть в строку другого продукта.
    :param pool_factory: фабрика пула фетча (D4, инжектится тестами); ``None`` —
        ``ThreadPoolExecutor``.
    :param browser_pool: пул браузерного маршрута **от владельца фазы**: ``browser_workers``
        потоков, в каждом живёт свой движок Playwright (их поднимает фабрика фазы лениво,
        внутри воркера). Драйвер его не закрывает (``_Lane.owns_browser_pool``
        ```False```) — закрывает создатель, и закрывает **на потоке каждого движка**.
        ``None`` — пул создаёт сам драйвер на время вызова (тогда он же его и
        останавливает), размером ``browser_workers``. Потоки обязаны быть именованными —
        иначе их не опознать в ``py-spy``/логах.
    :param notes: канал наблюдений фазы (``results[].notes``). Сюда уходит одна строка о
        браузерном маршруте, если он был: сколько фетчей прошло, сколько их было в полёте
        одновременно на пике и сколько полоса на них ждала. Пик — **наблюдение**: он
        отвечает «параллелизм случился», тогда как ось отвечает «параллелизм объявлен».
    :param on_root_done: вызывается **на главном потоке** сразу по завершении корня —
        здесь фаза строит финализатор провенанса и регистрирует хвост (C1).
    :param on_product_done: вызывается **на главном потоке**, когда завершён последний
        корень продукта (и его планирование закрыто маркером). Здесь фаза исполняет хвост
        продукта: конверсия, Pass B, покрытие, строка продукта. На время вызова выдача
        новых волн стоит — это измеряется в ``product_tail_s``.
    :param lane_limit_for: потолок полос **продукта** корня (``None`` — потолок когорты).
        Ось объявлена на корень (``app < run < site``), поэтому продукт с более строгим
        объявлением не получает полос «за компанию» с соседом.
    :param abort: teardown-хук фазы: вызывается при исключении и **называет** число
        строк очереди конверсии, оставшихся ``pending`` (durable-очередь переживает
        прерывание; «сколько не сконвертировано» обязано быть видно).
    :param network_breaker: размыкатель сети (:mod:`core.util.network_breaker`).
        Наблюдение идёт здесь, на главном потоке: исходы волны уже собраны из пула, а
        состояние процессов не разделяется — общий счётчик в воркерах был бы ложью.
        При срабатывании в ``errors`` пишется строка ``network_paused`` с числом отказов,
        хостов и секунд паузы, а выдача новых волн встаёт на ``pause_s``. Пока
        ``max_trips`` не исчерпан, прогон после паузы **продолжается** (моргающий канал
        дешевле переждать, чем бросить когорту); исчерпан — выдача прекращается и цикл
        выходит штатно: отправленное доживает, необработанное остаётся ``pending`` (это
        факт «не смотрели», а не ``failed`` — I2d). ``None`` — наблюдения нет (прежнее
        поведение байт-в-байт).
    :param sleep: исполнение паузы (``sleep(seconds)``). Инъектируется тестами: машина
        часов не имеет, поэтому ожидание проверяется без реального сна. Спит **главный**
        поток драйвера, а не воркеры: пауза — это «не выдаём новые запросы», а не
        «останавливаем уже отправленные».
    :param label: префикс строк ``errors``.
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
    #: Пул браузерного маршрута: чужой (фазы) или созданный здесь на время вызова.
    #: Второй случай — только прямой вызов драйвера; фаза всегда передаёт свой, потому
    #: что движок браузера обязан быть закрыт **на этом потоке** после конца когорты, а
    #: знать про движок (``CrawlerPort``) драйвер не должен.
    owned_browser_pool = browser_pool is None
    if browser_pool is None:
        # Прямой вызов драйвера: пул создаём здесь и здесь же закрываем. Размер —
        # объявленная ось (``None``/``1`` — прежний один поток).
        browser_pool = ThreadPoolExecutor(
            max_workers=1 if browser_workers is None else max(1, int(browser_workers)),
            thread_name_prefix="cohound-browser",
        )
    #: Занятые host'ы: держит только отправленная волна (снимается на её коммите).
    claimed: set[str] = set()
    done: list[RootOutcome] = []
    #: Продукт: сколько корней спланировано/завершено и закрыт ли уже хвост.
    planned: dict[str, int] = {}
    finished: dict[str, int] = {}
    planning_closed: set[str] = set()
    closed: set[str] = set()
    #: Итоги браузерного маршрута по когорте: фетчи и ожидание их волн (для строки в
    #: ``notes``). Считаются по завершённым и по прерванным полосам: факт сериализации
    #: не отменяется тем, что прогон прервали.
    browser_entries_total = 0
    browser_wait_total = 0.0
    #: Пик браузерных фетчей **в полёте** (отправлено, но не закоммичено). Это
    #: наблюдение, а не объявление: «ось сказала 3 движка» и «три фетча действительно
    #: шли одновременно» — разные факты, и второй виден только здесь.
    browser_in_flight_max = 0
    #: Причина остановки выдачи волн (``None`` — сеть не размыкалась либо паузы ещё не
    #: исчерпаны). Ставится только когда ``BreakerPolicy.max_trips`` исчерпан: пауза не
    #: помогла, канал мёртв. Тогда новые волны не выдаются, а необработанное остаётся
    #: ``pending`` — «не смотрели» не превращается в ``failed`` (I2d).
    pause_reason: str | None = None

    def _observe_wave(request: WaveRequest, outcomes: WaveOutcomes) -> None:
        """Накормить размыкатель исходами волны; срабатывание — пауза, затем продолжение.

        Признак «ответа не было» берётся у **класса отказа**
        (:func:`_fetch_silence`), а не из текста ошибки: разбор сообщений был бы
        вторым правилом рядом с таксономией порта и разошёлся бы с ней молча.
        Запись волны без живого фетча (``fetch is None``: блэклист, resume,
        исчерпанные попытки, терминальный класс) о канале не говорит ничего — она
        пропускается, а не считается ответом.

        **Срабатывание = пауза, а не остановка.** Строка ``network_paused`` с числами и
        хостами пишется в ``errors`` всегда — пауза обязана быть видимой. Дальше решает
        ``max_trips``: пока он не исчерпан, прогон после паузы **продолжается** (канал
        мог ожить, и для моргающего канала это дешевле брошенной когорты), исчерпан —
        ставится ``pause_reason`` и выдача прекращается.

        **Спит главный поток драйвера.** Уже отправленные волны при этом продолжают
        исполняться в пулах: их future'ы живут, и по возвращении из сна драйвер их
        соберёт. Поэтому «пауза» здесь означает «не выдаём новые запросы», а не
        «останавливаем работу»: добытое не теряется, а сеть получает перерыв.

        **Остаток волны после срабатывания не разбирается** (``return`` сразу после паузы):
        его отказы относятся к серии «до паузы» и ничего не говорят о том, ожил ли канал
        после неё, — вердикт «ожил/не ожил» даёт следующая волна, набранная уже после
        паузы. Иначе волна шире порога (а она легко несёт 4–10 записей при
        ``max_workers`` 10 / ``host_concurrency`` 5) давала бы два срабатывания подряд и
        исчерпывала паузы, не проверив канал ни разу.

        Счётчики машины после срабатывания обнулены, поэтому продолжение начинает новую
        серию — и вторая серия подряд упрётся в ``exhausted``, а не будет копиться вечно.
        """
        nonlocal pause_reason
        breaker = network_breaker
        if breaker is None or pause_reason is not None:
            return
        for entry, fetch in zip(request.entries, outcomes.fetch, strict=True):
            if fetch is None:
                continue
            if fetch.page is not None:
                breaker.note_response()
                continue
            if _fetch_silence(fetch.exc):
                host = entry.desc.host if entry.desc is not None else ""
                if host and breaker.note_silence(host):
                    trip = breaker.last_trip
                    if trip is None:
                        # Инвариант машины: ``True`` возвращается только при
                        # срабатывании, и провенанс пишется в тот же момент.
                        raise AssertionError(
                            "network_breaker: срабатывание без провенанса"
                        )
                    errors.append(
                        f"crawl-драйвер{label}: network_paused: "
                        f"{trip.consecutive} отказов без ответа на "
                        f"{len(trip.hosts)} хостах "
                        f"({', '.join(trip.hosts)}), пауза {trip.pause_s:.0f} с "
                        f"({breaker.trips} из {breaker.policy.max_trips})"
                    )
                    if breaker.exhausted:
                        pause_reason = (
                            f"crawl-драйвер{label}: network_paused: паузы исчерпаны "
                            f"({breaker.trips} из {breaker.policy.max_trips}) — канал "
                            "не ожил, новые волны не выдаются, необработанное остаётся "
                            "pending"
                        )
                        errors.append(pause_reason)
                        return
                    # Пауза и продолжение: переждать и вернуться к обычной работе.
                    # Остаток волны не разбираем — его отказы из серии «до паузы».
                    sleep(trip.pause_s)
                    return
            elif _fetch_answered(fetch.exc):
                breaker.note_response()

    def _account_browser(lane: _Lane) -> None:
        """Перенести браузерные числа полосы в итог когорты ровно один раз."""
        nonlocal browser_entries_total, browser_wait_total
        browser_entries_total += lane.browser_entries
        browser_wait_total += lane.browser_wait_s
        lane.browser_entries = 0
        lane.browser_wait_s = 0.0

    def _note_product(plan: RootPlan) -> None:
        """Учесть корень продукта (``None`` — корень вне когорты: границ у него нет)."""
        pid = plan.product_id
        if pid is not None:
            planned[pid] = planned.get(pid, 0) + 1

    def _close_product(product_id: str) -> None:
        """Хвост продукта на главном потоке + видимая цена остановки выдачи волн."""
        if on_product_done is None or product_id in closed:
            return
        closed.add(product_id)
        timings = timings_for(product_id)
        started = time.perf_counter()
        try:
            on_product_done(product_id)
        finally:
            # Пишется и при исключении: «выдача стояла столько» — факт, а не награда за
            # успешный хвост. Бакет не входит в ``stage_sum``: он пересекается с Pass B.
            timings.product_tail_s += time.perf_counter() - started

    def _maybe_close(product_id: str | None) -> None:
        """Закрыть продукт, если он спланирован целиком и все его корни завершены."""
        if product_id is None or product_id in closed:
            return
        if product_id not in planning_closed:
            return
        if finished.get(product_id, 0) < planned.get(product_id, 0):
            return
        _close_product(product_id)

    def _planning_done(product_id: str) -> None:
        """Маркер границы продукта: корней больше не будет — можно закрывать."""
        planning_closed.add(product_id)
        _maybe_close(product_id)

    def _close(lane: _Lane) -> None:
        """Закрыть генератор корня и **его** пулы. Сбой закрытия виден, но не маскирует исход."""
        if lane.steps is not None:
            try:
                lane.steps.close()
            except Exception as exc:  # noqa: BLE001 — teardown не подменяет исход
                errors.append(
                    f"crawl-драйвер{label}: close({lane.plan.root_url}): "
                    f"{type(exc).__name__}: {exc}"
                )
            lane.steps = None
        pools = [lane.fetch_pool]
        # Браузерный пул обычно чужой (фазы или ``drive_roots``): закрывать его здесь
        # нельзя — в его потоке живёт движок Playwright, общий на всю когорту. Закрытие
        # чужого пула убило бы поток владельца первому же завершившемуся корню.
        if lane.owns_browser_pool:
            pools.append(lane.browser_pool)
        for pool in pools:
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
        lane.owns_browser_pool = False

    def _release(lane: _Lane) -> None:
        """Снять право корня на host'ы (только после коммита волны)."""
        claimed.difference_update(lane.claim)
        lane.claim = frozenset()

    def _open(plan: RootPlan) -> _Lane:
        """Открыть полосу под корень: пулы — по ``lane_width``, генератор — ``begin``."""
        width = lane_width(plan)
        lane = _Lane(
            plan=plan, steps=None, timings=timings_for(plan.product_id)
        )
        try:
            if width is not None:
                lane.fetch_pool = (
                    pool_factory(width)
                    if pool_factory is not None
                    else ThreadPoolExecutor(max_workers=width)
                )
                # Браузерный маршрут — **общий** пул когорты: движок Playwright привязан
                # к потоку-создателю, поэтому потоков ровно столько, сколько объявлено
                # движков (``browser_workers``), и живут они дольше корня. Исполнение на
                # потоке драйвера остановило бы все полосы — поэтому не инлайн, а пул.
                # Пустой пул потоков не запускает: он не стоит ничего, пока браузерной
                # волны нет.
                lane.browser_pool = browser_pool
                lane.owns_browser_pool = False
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
        except StopIteration as stopped:
            lane.wave = None
            lane.outcomes = None
            lane.outcome = stopped.value
            return False
        lane.outcomes = None
        return True

    def _dispatch(lane: _Lane) -> None:
        """Отправить волну корня и взять право на её host'ы."""
        nonlocal browser_in_flight_max
        wave = lane.wave
        if wave is None:
            raise AssertionError(f"{lane.plan.root_url}: отправка без плана волны")
        if wave.browser and lane.browser_pool is None:
            # Пара к ``WavePlan.browser``: право на host'ы и маршрут решены до
            # отправки, поэтому отсутствие пула браузера видно здесь, с именем корня,
            # а не как общий отказ внутри ``_dispatch_wave``.
            raise AssertionError(
                f"{lane.plan.root_url}: браузерный маршрут без пула движков"
            )
        lane.wave = None
        claimed.update(wave.hosts)
        lane.claim = wave.hosts
        lane.request = _dispatch_wave(
            wave.entries,
            pool=lane.fetch_pool,
            browser_pool=lane.browser_pool,
            local_fetch=local_fetch,
        )
        lane.browser_wave = wave.browser
        _submitted = _browser_submissions(lane.request)
        lane.browser_entries += _submitted
        lane.browser_in_flight += _submitted
        if lane.browser_in_flight > browser_in_flight_max:
            browser_in_flight_max = lane.browser_in_flight
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
        # «В полёте» — это отправлено и ещё не закоммичено: снимаем на ожидании волны,
        # а не на коммите, потому что пик одновременности считается по времени полёта.
        lane.browser_in_flight = max(
            0, lane.browser_in_flight - _browser_submissions(request)
        )
        lane.outcomes = _await_wave(request, timings=lane.timings)
        # Наблюдение канала — **до** коммита: исходы уже собраны в родительском
        # процессе, и здесь же видно, был ли ответ у каждой записи волны.
        _observe_wave(request, lane.outcomes)
        # Ожидание полосы целиком — в тот же бакет «участок фетча»: после ``wait``
        # вызовы ``result`` внутри ``_await_wave`` уже не спят. При полосах это сумма
        # ожиданий корней, а не wall time продукта (он публикуется строкой продукта) —
        # иначе «ожидание» читалось бы как работа. Бакет — **своего** продукта (F9).
        _waited = time.perf_counter() - started
        lane.timings.fetch_wait_s += _waited
        if lane.browser_wave:
            # Ожидание браузерной волны — отдельным числом: сериализация браузерных
            # фетчей в одном потоке и есть её цена. Считается по волне (записи волны
            # ждут одну и ту же очередь — суммировать их ожидания значило бы посчитать
            # очередь N раз).
            lane.browser_wait_s += _waited
            lane.browser_wave = False
        lane.done = not _pump(lane)
        # Право снимается **после** коммита: коммит исполняется внутри ``_pump``
        # (генератор применяет ``_commit_wave_results`` сразу после получения исходов).
        _release(lane)

    def _finish(lane: _Lane) -> None:
        """Завершённый корень: закрыть пулы, отдать исход фазе, освободить полосу."""
        _account_browser(lane)
        lanes.remove(lane)
        _close(lane)
        outcome = lane.outcome
        if outcome is None:
            raise AssertionError(f"{lane.plan.root_url}: завершение без исхода")
        root = RootOutcome(
            plan=lane.plan,
            frontier=outcome[0],
            sitemap_skips=outcome[1],
        )
        done.append(root)
        if on_root_done is not None:
            on_root_done(root)
        pid = lane.plan.product_id
        if pid is not None:
            finished[pid] = finished.get(pid, 0) + 1
            # Провенанс корня записан выше: хвост продукта видит уже финализированные
            # слоты ``truncation_records``, а продукт закрывается только последним корнем.
            _maybe_close(pid)

    def _lane_limit(plan: RootPlan) -> int:
        """Потолок полос продукта корня (``None`` — потолок когорты)."""
        if lane_limit_for is None:
            return lanes_limit
        declared = lane_limit_for(plan)
        if declared is None:
            return lanes_limit
        return max(1, min(lanes_limit, int(declared)))

    def _product_lanes(product_id: str | None) -> int:
        """Сколько полос уже занято корнями этого продукта."""
        if product_id is None:
            return 0
        return sum(1 for lane in lanes if lane.plan.product_id == product_id)

    try:
        pending: Any = _NO_PENDING
        while True:
            # (1) Допуск корней: полоса — слот, корни берутся в авторском порядке.
            # Паузы исчерпаны — корней не допускаем: новый корень это новые запросы.
            while (
                queue_open and len(lanes) < lanes_limit and pause_reason is None
            ):
                if pending is _NO_PENDING:
                    pending = next(queue, _NO_PLAN)
                if pending is _NO_PLAN:
                    queue_open = False
                    break
                if isinstance(pending, ProductBoundary):
                    marker = pending
                    pending = _NO_PENDING
                    # Граница в потоке планов: хвост продукта исполнится здесь же, если
                    # все его корни уже завершены (продукт без корней — сразу).
                    _planning_done(marker.product_id)
                    continue
                plan = pending
                if _product_lanes(plan.product_id) >= _lane_limit(plan):
                    # Полосы продукта заняты: план уже изъят (планирование корня
                    # исполнено), но полосу ждёт. Порядок очереди от этого не меняется.
                    break
                pending = _NO_PENDING
                _note_product(plan)
                lane = _open(plan)
                lanes.append(lane)
                # Подготовка корня (limits/frontier/seeds/зарядка бюджета) — на первом
                # ``next``: при ``host_concurrency`` None/1 это ровно прежний момент.
                if not _pump(lane):
                    _finish(lane)
            # (2) Допуск волн: право на host'ы выдаётся в авторском порядке корней,
            # поэтому припаркованный корень не блокирует остальных, а ждёт своей очереди.
            for lane in list(lanes):
                if pause_reason is not None:
                    # Паузы исчерпаны: ни одной новой волны. Уже отправленные доживают
                    # в (3) и коммитятся — добытое не отбрасывается.
                    break
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
                if pause_reason is not None:
                    # Паузы исчерпаны и отправленного больше нет — штатный выход.
                    # Необработанное не «провалилось»: строки очереди остаются
                    # ``pending``, а сеанс финализируется с названной причиной
                    # (запись уже в ``errors``).
                    break
                if lanes:
                    # Ждать нечего, но это ещё не застой. Волна **без живого фетча**
                    # (resume/retained, блэклист, исчерпанные попытки) коммитится сразу
                    # там же в (2), и корень тут же отдаёт следующую — то есть после
                    # такого коммита полоса законно остаётся без отправленной волны, но
                    # **с планом**. Без этого различения resume убитого сеанса падал на
                    # первой же волне, целиком составленной из уже `FETCHED` URL
                    # (измерено 07.11.2026 на пробе `docs.tantorlabs.ru`: 91 документ,
                    # `AssertionError` вместо продолжения) — каждый сбой стоил полного
                    # пере-краула. Возвращаемся к допуску: следующий шаг — отправка.
                    if any(
                        lane.wave is not None and not (lane.wave.hosts & claimed)
                        for lane in lanes
                    ):
                        continue
                    # Право держит только отправленная волна, поэтому «ничего не
                    # отправлено» ⇒ права пусты; и здесь уже проверено, что отправить
                    # нечего — ни одной полосы с готовым планом. Нарушение (план есть,
                    # но право на host не получить — значит, его держит несуществующая
                    # волна; либо плана нет вовсе) — дефект драйвера, а не режим:
                    # лучше упасть, чем висеть.
                    raise AssertionError(
                        "crawl-драйвер: полосы без отправленной волны и без права на host"
                    )
                if not queue_open:
                    break
                continue
            # Ждём **только незавершённые** future'ы. ``wait`` с ``FIRST_COMPLETED``
            # возвращается немедленно, если в наборе есть готовый future, а волна
            # сбрасывается лишь когда готовы **все** её записи: набор без этого фильтра
            # оставлял цикл горячим простоем на всё окно между первым и последним
            # завершением записи волны. Измерено профилем py-spy 02.11.2026: 97 % одного
            # ядра внутри этого цикла при темпе, который задавал пейсинг, — то есть CPU
            # жгло ожидание, а не работа.
            futures = {
                future
                for lane in in_flight
                for future in _futures(lane.request)
                if not future.done()
            }
            if futures:
                wait(
                    futures,
                    timeout=_LANE_WAIT_TIMEOUT_S,
                    return_when=FIRST_COMPLETED,
                )
            for lane in in_flight:
                waiting = _futures(lane.request)
                if waiting and not all(future.done() for future in waiting):
                    continue
                _settle(lane)
                if lane.done:
                    _finish(lane)
        if pause_reason is not None and lanes:
            # Ни один допущенный корень не выпадает молча и при паузе: он либо завершился,
            # либо назван. «Не добыто» — не «провалилось»: строки очереди остаются
            # ``pending``, и следующий проход продолжит с них.
            names = ", ".join(lane.plan.root_url for lane in lanes)
            errors.append(
                f"crawl-драйвер{label}: network_paused: корней без исхода "
                f"{len(lanes)} (не добыто, остаётся pending): {names}"
            )
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
            _account_browser(lane)
            _close(lane)
        lanes.clear()
        if browser_entries_total > 0 and notes is not None:
            # «Не ограничивать, а измерять»: параллелизм объявлен осью, а случившийся
            # параллелизм — наблюдение. Оба числа публикуются, чтобы «объявили 3 движка»
            # и «три фетча шли одновременно» не сливались в одно утверждение.
            notes.append(
                f"crawl{label}: браузер: {browser_entries_total} фетчей, "
                f"пик в полёте {browser_in_flight_max}, "
                f"суммарное ожидание {browser_wait_total:.1f} с "
                "(полоса не блокируется)"
            )
        if owned_browser_pool:
            # Пул создан здесь — здесь и останавливается. Чужой (фазы) не трогаем: закрытие
            # движка на его потоке — работа владельца, и она идёт до этого shutdown.
            browser_pool.shutdown(wait=True)
    return done


def _futures(request: Any) -> list[Any]:
    """Живые future'ы отправленной волны (порядок записей — порядок отправки)."""
    entries = getattr(request, "entries", ())
    return [
        entry.future for entry in entries if getattr(entry, "future", None) is not None
    ]


def _browser_submissions(request: Any) -> int:
    """Сколько записей волны ушло в браузерный пул (маршрут ``browser``).

    Признак тот же, по которому :func:`core.pipeline.corpus_drain._dispatch_wave`
    выбирает ветку: у записи есть живой ``future`` и её дескриптор назвал браузерный
    маршрут (HTTP-фетчи уходят в пул фетча). Число нужно только для наблюдаемости:
    сколько фетчей прошло и сколько их было в полёте одновременно — факты, которые
    публикуются, а не ограничения.
    """
    count = 0
    for entry in getattr(request, "entries", ()):
        if getattr(entry, "future", None) is None:
            continue
        desc = getattr(entry, "desc", None)
        if desc is not None and getattr(desc, "route", "fetch") == "browser":
            count += 1
    return count