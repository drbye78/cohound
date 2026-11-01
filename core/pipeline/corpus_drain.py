"""Дренаж одного seed-root: окно фетча/экстракции, frontier, спуск в контейнеры.

Часть декомпозиции ``corpus.py`` (план ``docs/reports/2026-10-30-large-modules-refactoring-plan.md``,
L5 шаг 3). Здесь — то, что делает работу **одного корня** от постановки seed'а до фиксации
исхода: BFS-раунды ``Frontier`` (локальный пул ‖ ``executor``-волны), окно экстракции,
спуск в ретейненный контейнер и коммит исходов на главном потоке.

**Почему отдельный модуль.** Тело дренажа корня было самым крупным замыканием фазы
(1 476 строк) и читал 17 имён охватывающей области, включая ``frontier``/``limits``/
окно экстракции. Вынесен **целиком** и **дословно**: тело перенесено по AST-диапазону,
изменились только заголовок и имена вызовов.

**Почему носители, а не 60 параметров.** Всё, что раньше было свободной переменной
замыкания, по-прежнему приходит аргументом, но не поштучно: корень получает три носителя,
разделённые **по владению** — :class:`ProductDrainScope` (продуктовое состояние, общее для
всех корней продукта), :class:`PhaseDrainScope` (состояние и оси фазы) и
:class:`PhaseDrainOperations` (замкнутые операции фазы). Плоский список из 60 kwargs трёх
разных природ не отвечал на вопрос «что корень читает и что меняет»: имя носителя называет
владельца, имя поля — конкретное состояние. Носители — ``frozen`` dataclass'ы со
**ссылками** на мутируемые контейнеры: ``frozen`` запрещает подменить поле, а не изменить
содержимое, и это ровно то, что нужно (копия контейнера вернула бы тихий баг: корень
обновил бы копию, а сосед не увидел). Зависимости уровня фазы связываются один раз через
:func:`functools.partial` в точке вызова (``corpus._build_corpus``), per-продуктные —
на границе продукта.

**Границы.** Модуль импортирует ``corpus_ingest``/``corpus_commit``/``corpus_persist``,
но не ``corpus``: обратный импорт дал бы цикл. ``ingest_member_doc`` и ``persist_docs``
приходят параметрами, а не импортом, по той же причине, по которой ``finish_one``
приходит параметром: тесты подменяют их атрибутами модуля ``corpus``, и подмена обязана
доходить до писателя.

**Шаги, а не волна целиком (F4).** Волновой цикл вынесен в генератор
:func:`root_steps`, который выдаёт **план** волны (:class:`WavePlan`) и получает
:class:`WaveOutcomes`; :func:`root_lifecycle` — генератор корня целиком (подготовка +
шаги). Так исполнено чередование корней (F4b): полоса — это **право на host**, а не
объект, поэтому кооперативные шаги не требуют ни потоков-полос, ни мутаций вне главного
потока. Отправку волны и ожидание исполняет драйвер
(:mod:`core.pipeline.corpus_driver`), потому что допуск волны решается до отправки.

**Порядок записи не меняется.** Оракул — ``tests/pipeline/test_corpus_write_order.py``.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Generator, Mapping, Sequence
from concurrent.futures import Executor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.domain import (
    CrawlItemStatus,
    CrawlSessionItem,
    DocumentLevel,
    ExtractQueueItem,
    ExtractQueueState,
    FailureKind,
    ReuseKind,
)
from core.domain.models import RateLimitSpec
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import (
    LocalFetch,
    ProductIngest,
    RetainedRaw,
    RetainedReject,
    SitemapSeeds,
    looks_like_html,
    resume_retained,
    terminal_failure_kind,
)
from core.pipeline.corpus_persist import _now_iso
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import SCOPE_OUT, CrawlLimits, Frontier
from core.pipeline.extract_queue import (
    DISK_LOW_HEADROOM_REASON,
    BackpressureEvent,
    DiskHeadroomReport,
    wait_for_backlog,
)
from core.pipeline.extraction_runtime import (
    _as_text,
)
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
from core.ports.crawl_executor import CrawlExecutorPort, CrawlWorkItem
from core.ports.types import FetchPlan, RawPage
from core.util.document_formats import (
    document_content_type,
    document_suffix,
)
from core.util.html_links import extract_links

#: Phase 4/Ф0: ``error_kind`` терминального ``FAILED`` при исчерпании ``--max-attempts``.
#: Канонический класс сбоя (``FailureKind.ATTEMPTS_EXHAUSTED``): отличает «попытки
#: кончились» от сетевого/браузерного сбоя, не выходя за словарь §4.1.
_MAX_ATTEMPTS_ERROR_KIND = FailureKind.ATTEMPTS_EXHAUSTED.value

@dataclass
class _WaveEntry:
    """D4: элемент волны дренажа frontier.

    Порядок элементов — порядок отправки (FIFO). ``kind``:
    ``"blacklisted"`` | ``"attempts"`` | ``"terminal_not_found"`` (мутации коммитятся
    пост-фазой; терминальный ``not_found`` не подаётся в работу) | ``"fetch"``
    (несёт ``desc`` и, если фетч реально нужен, ``future`` пула).
    """

    pid: str
    wu: str
    depth: int | None
    kind: str
    desc: LocalFetch | None = None
    future: Any = None
    prior_item: CrawlSessionItem | None = None


@dataclass(frozen=True)
class RootPlan:
    """Один seed-root продукта: адрес, цель краула и чем его аугментировать (F4).

    Зачем отдельный тип. Дренаж корня принимает пять значений на корень; драйверу полос
    (F4b) они нужны как **значение**, иначе будущая функция драйвера стала бы
    70-параметровой — то есть перестала бы называть, что она читает. ``known_urls`` и
    ``serp_seeds`` зависят от корня, поэтому резолвятся в точке построения плана
    (``corpus._build_corpus``), а не внутри дренажа: корень их только потребляет.
    """

    root_url: str
    target: Any
    sitemap_seeds: bool
    #: Известные URL этого корня (ревалидация, 09 §6.3) — depth-0 листья вне бюджета.
    known_urls: Sequence[str] = ()
    #: Документы, найденные SERP-контуром (I3/§5): depth-0 seeds **этого** корня.
    serp_seeds: Sequence[str] = ()
    #: ``False`` — synthetic-бакет (orphan): авторского seed нет, только ревалидация.
    seed: bool = True
    #: Слот записи провенанса в ``truncation_records`` (F4b). Занимается **в момент
    #: планирования**, то есть в авторском порядке корней, а не в порядке их завершения:
    #: при чередовании корней порядок завершения другой, а артефакт манифеста
    #: (``effective_limits.roots``) обязан остаться авторским. ``None`` — корень, у
    #: которого слота нет (synthetic-бакеты): запись добавляется в конец.
    record_slot: int | None = None


@dataclass(frozen=True)
class WaveRequest:
    """Задания волны, готовые к отправке, и маршрут её не-poolable части.

    ``entries`` — записи в порядке отправки (FIFO). ``non_poolable`` сообщает, что в
    волне есть живой фетч не-poolable маршрута (browser): он исполняется **отдельным
    пулом из одного потока**, а не на потоке драйвера, — иначе один браузерный фетч
    остановил бы все полосы (F4).
    """

    entries: list[_WaveEntry]
    non_poolable: bool = False


@dataclass(frozen=True)
class WavePlan:
    """План волны: записи, право на host'ы и маршрут — **до** отправки (F4b).

    Драйвер полос решает допуск волны **до** того, как она уйдёт в пул, поэтому
    генератор отдаёт план, а не отправленную волну: отправка и ожидание — работа
    драйвера (``corpus_driver``), коммит исходов — по-прежнему этого корня.

    ``hosts`` — hosts записей с живым фетчем (``LocalFetch.host``, тот же ключ, что у
    ``limiter.wait``, — см. :func:`corpus_ingest.local_fetch`): это ровно то право,
    которое драйвер резервирует за корнем на время волны. Пустое множество (волна без
    живого фетча: блэклист/исчерпанные попытки/resume) не пересекается ни с чем и
    допускается всегда.

    ``non_poolable`` — в волне есть живой фетч не-poolable маршрута (browser): ему нужен
    пул из **одного** потока (см. :func:`_dispatch_wave`), и драйвер обязан его дать.
    """

    entries: list[_WaveEntry]
    hosts: frozenset[str] = frozenset()
    non_poolable: bool = False


def _wave_plan(entries: list[_WaveEntry]) -> WavePlan:
    """Право волны на host'ы + маршрут — по готовым записям (F4b).

    ``hosts`` берутся **из дескрипторов** (``desc.host``), а не из URL записи: ключ права
    обязан совпадать с ключом вежливости (``limiter.wait(desc.host, …)``), иначе полосы
    пейсили бы один сайт двумя независимыми бюджетами.

    ``non_poolable`` повторяет условие ветки :func:`_dispatch_wave` (живой фетч вне пула
    фетча): resume/reject/browser-guard/терминальный класс живого фетча не делают.
    Расхождение с :func:`_dispatch_wave` — это не тихая ошибка: там сработает
    ``AssertionError`` «не-poolable маршрут без своего пула».
    """
    hosts = {
        entry.desc.host
        for entry in entries
        if entry.kind == "fetch" and entry.desc is not None and entry.desc.host
    }
    non_poolable = any(
        entry.kind == "fetch"
        and entry.desc is not None
        and not entry.desc.poolable
        and entry.desc.resume is None
        and entry.desc.reject is None
        and not entry.desc.browser_blocked
        and not entry.desc.terminal
        for entry in entries
    )
    return WavePlan(
        entries=entries, hosts=frozenset(hosts), non_poolable=non_poolable
    )


@dataclass(frozen=True)
class _WaveFetch:
    """Исход одного фетча: страница **или** сбой (ровно одно из двух)."""

    page: RawPage | None = None
    exc: Exception | None = None


@dataclass(frozen=True)
class WaveOutcomes:
    """Исходы отправленной волны: по элементу на запись, в том же порядке.

    ``fetch[i] is None`` — живого фетча у записи не было (блэклист, исчерпанные попытки,
    resume/reject/browser-guard/терминальный класс). Длина списка равна длине
    ``WaveRequest.entries``: коммит идёт по парам и порядок не восстанавливает.
    """

    fetch: list[_WaveFetch | None]


def _dispatch_wave(
    entries: list[_WaveEntry],
    *,
    pool: Executor | None,
    non_poolable_pool: Executor | None,
    local_fetch: Callable[[LocalFetch], RawPage],
) -> WaveRequest:
    """Отправка волны: живой фетч — в пул, браузерный — в пул из **одного** потока.

    Мутаций состояния сеанса/корпуса здесь нет (как и в прежнем ``_commit_wave``):
    отправляется только ``local_fetch`` — чистый сетевой вызов. Разделение с
    :func:`_commit_wave_results` — по владению: отправка и коммит остаются на главном
    потоке корня, ожидание — работа драйвера.

    Не-poolable маршрут (браузерный фетч) идёт в **отдельный однопоточный** пул. Прежний
    путь исполнял его на потоке драйвера, и при полосах один браузерный фетч остановил бы
    все полосы. Поток-владелец при этом сохранён: ``BrowserCrawler`` водит единственную
    ``_page`` без лока, поэтому у маршрута ровно один поток — он и есть владелец. Коммит
    исхода остаётся на главном потоке (T5: мутации пишет только драйвер).
    """
    non_poolable = False
    for entry in entries:
        if entry.kind != "fetch":
            continue
        desc = entry.desc
        if desc is None:
            # Инвариант ``_WaveEntry``: не-attempts/не-блэклист запись несёт ``desc``.
            raise AssertionError(f"{entry.pid}/{entry.wu}: запись волны без desc")
        if desc.poolable:
            if pool is None:
                # Инвариант ветки: ``poolable`` выставляется только при созданном пуле.
                raise AssertionError(
                    f"{desc.pid}/{desc.wu}: poolable без пула фетча"
                )
            entry.future = pool.submit(local_fetch, desc)
        elif (
            desc.resume is not None
            or desc.reject is not None
            or desc.browser_blocked
            or desc.terminal
        ):
            # resume/reject/browser-guard/терминальный класс: живого фетча нет.
            continue
        else:
            if non_poolable_pool is None:
                # Инвариант ветки: волна живёт только при созданном пуле фетча, а он
                # создаётся вместе с пулом не-poolable маршрута.
                raise AssertionError(
                    f"{desc.pid}/{desc.wu}: не-poolable маршрут без своего пула"
                )
            entry.future = non_poolable_pool.submit(local_fetch, desc)
            non_poolable = True
    return WaveRequest(entries=entries, non_poolable=non_poolable)


def _await_wave(
    request: WaveRequest,
    *,
    timings: _CorpusStageTimings,
) -> WaveOutcomes:
    """Дождаться исходов волны в порядке отправки.

    ``fetch_wait_s`` — **ожидание** пула: работу делает он, коммит
    (``_commit_wave_results``) остаётся работой главного потока и в бакет не входит.
    Бакет пополняется здесь, а не в коммите, потому что ожидание — единственное, что
    делает драйвер; порядок накопления — порядок элементов, то есть прежний.
    """
    fetch: list[_WaveFetch | None] = [None] * len(request.entries)
    for index, entry in enumerate(request.entries):
        if entry.future is None:
            continue
        _fetch_started = time.perf_counter()
        try:
            page = entry.future.result()
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            timings.fetch_wait_s += time.perf_counter() - _fetch_started
            fetch[index] = _WaveFetch(exc=exc)
        else:
            timings.fetch_wait_s += time.perf_counter() - _fetch_started
            fetch[index] = _WaveFetch(page=page)
    return WaveOutcomes(fetch=fetch)


def _commit_wave_results(
    entries: list[_WaveEntry],
    outcomes: WaveOutcomes,
    *,
    commit_local: Callable[..., Any],
    commit: Callable[..., None],
    mark: Callable[..., None],
    emit: Callable[[], None],
    errors: list[str],
    max_attempts: int | None,
) -> None:
    """Коммитит исходы волны на главном потоке в FIFO-порядке отправки (D4).

    Порядок коммита = порядок отправки: детерминированы ``errors``, ``mark``,
    победитель дедупа (``seen_docs[doc_hash] = url``) и ``offer`` — единственные
    порядко-чувствительные артефакты (идентичность корпуса content-addressed).

    Вторая половина прежнего ``_commit_wave`` (F4): ожидание исходов ушло в
    :func:`_await_wave`, отправка — в :func:`_dispatch_wave`, а **порядок применения**
    остался здесь и остаётся инвариантом: ``errors``, ``mark`` и дедуп-победитель видят
    ровно прежнюю последовательность.
    """
    for index, entry in enumerate(entries):
        if entry.kind == "blacklisted":
            # D1/H: блэклист применяется и перед фетчем (в дополнение к постановке).
            errors.append(
                f"{entry.pid}/{entry.wu}: зачёркнут блэклистом перед фетчем — пропущен"
            )
            continue
        if entry.kind == "attempts":
            prior_item = entry.prior_item
            if prior_item is None:
                # Инвариант сборки ``_WaveEntry(kind="attempts")``: запись
                # создаётся только по существующему элементу сеанса.
                raise AssertionError(
                    f"{entry.pid}/{entry.wu}: attempts-запись без prior_item"
                )
            reason = (
                f"max_attempts exhausted "
                f"(attempts={prior_item.attempts} >= {max_attempts})"
            )
            errors.append(f"{entry.pid}/{entry.wu}: {reason}")
            mark(
                entry.pid,
                entry.wu,
                CrawlItemStatus.FAILED,
                error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                error=reason,
                depth=entry.depth,
            )
            emit()
            continue
        desc = entry.desc
        if desc is None:
            # Инвариант ``_WaveEntry``: не-attempts/не-блэклист запись несёт
            # либо desc, либо future (и то и другое заполняется сборкой).
            raise AssertionError(
                f"{entry.pid}/{entry.wu}: запись волны без desc"
            )
        fetched = outcomes.fetch[index]
        if fetched is not None:
            # Исход фетча пришёл от пула (ожидание и бакет ``fetch_wait_s`` — в
            # ``_await_wave``): живой фетч есть ровно у записей, у которых был
            # ``future``, поэтому ветка «нет исхода» ниже — инвариант, а не режим.
            outcome = (
                commit_local(desc, exc=fetched.exc)
                if fetched.exc is not None
                else commit_local(desc, page=fetched.page)
            )
        elif (
            desc.resume is not None
            or desc.reject is not None
            or desc.browser_blocked
            or desc.terminal
        ):
            # resume/reject/browser-guard/терминальный класс: живого фетча нет.
            outcome = commit_local(desc)
        else:
            # Инвариант ``_dispatch_wave``: живой фетч всегда отправлен (в пул или в
            # пул не-poolable маршрута), поэтому «живого фетча нет» без исхода —
            # ошибка сборки волны, а не исход.
            raise AssertionError(
                f"{entry.pid}/{entry.wu}: волна без исхода фетча"
            )
        commit(entry.pid, entry.wu, entry.depth, outcome)


def disk_headroom_line(report: DiskHeadroomReport) -> str:
    """Строка ``crawl_disk_headroom`` для ``errors`` фазы: числа, а не «стало тесно».

    Публикуется **одна** на прогон (см. :class:`core.pipeline.extract_queue.
    DiskHeadroomGuard`): ``waves_blocked`` — счётчик отказанных волн в той же записи, а не
    сотня одинаковых строк. ``applied=false`` означает «сторож не применён» (свободное
    место носителя неизвестно) — это исход наблюдения, и он назван, а не выдан за «места
    хватает». ``backlog_bytes``/``queue_rows`` — ``None``/``0``, если числа не наблюдались
    (неблокирующее наблюдение не платит прогулкой по строкам очереди).
    """
    payload = report.as_dict()
    rows: dict[str, int] = payload["queue_rows"]
    return (
        "crawl_disk_headroom: "
        f"applied={str(payload['applied']).lower()} "
        f"blocked={str(payload['blocked']).lower()} "
        f"free_bytes={payload['free_bytes']} "
        f"reserve_bytes={payload['reserve_bytes']} "
        f"backlog_bytes={payload['backlog_bytes']} "
        f"queue_rows={sum(rows.values())} "
        f"waves_blocked={payload['waves_blocked']}"
    )


def _backpressure_line(event: BackpressureEvent) -> str:
    """Строка ``crawl_backpressure`` для ``errors`` фазы: числа, а не «стало тесно».

    Публикуются все три величины §4.3 (``backlog_bytes``/``free_bytes``/``waited_s``) плюс
    предел и число файлов: без предела число байтов не с чем сравнить, а без файлов
    непонятно, «много маленьких» это или «несколько гигабайтных». ``gave_up=true`` —
    приём остановлен, и сеанс закрывается ``partial``.

    ``missing`` печатается только когда он ненулевой: строка очереди, обещавшая байты,
    которых нет, — нарушение I19, и молчать о нём нельзя.
    """
    payload = event.as_dict()
    line = (
        "crawl_backpressure: "
        f"backlog_bytes={payload['backlog_bytes']} "
        f"backlog_files={payload['backlog_files']} "
        f"backlog_limit_bytes={payload['backlog_limit_bytes']} "
        f"free_bytes={payload['free_bytes']} "
        f"waited_s={payload['waited_s']} "
        f"gave_up={str(payload['gave_up']).lower()}"
    )
    if payload["backlog_missing"]:
        line += f" backlog_missing={payload['backlog_missing']} (I19: файла нет)"
    return line


def root_steps(
    pid: str,
    root_url: str,
    frontier: Frontier,
    *,
    limits: CrawlLimits,
    items: Mapping[tuple[str, str], CrawlSessionItem],
    errors: list[str],
    emit: Callable[[], None],
    mark: Callable[..., None],
    commit_local: Callable[..., Any],
    local_prefetch: Callable[..., LocalFetch],
    process_local: Callable[..., Any],
    submit_collect: Callable[..., Mapping[tuple[str, str], Any]],
    work_item: Callable[..., CrawlWorkItem],
    resolve_target: Callable[..., Any],
    max_attempts: int | None,
    max_waves: int,
    wave_items: int,
    robots_enabled: bool,
    timings: _CorpusStageTimings,
    executor: CrawlExecutorPort | None,
    pool: Executor | None,
    max_workers: int | None,
    sitemap_skips: Sequence[dict[str, Any]],
    _has_fetch_capacity: Callable[[], bool],
    _skip_document_prefetch: Callable[[str], bool],
    _commit: Callable[..., None],
    _await_disk: Callable[[], bool],
    _await_backlog: Callable[[], str | None],
) -> Generator[WavePlan, WaveOutcomes, tuple[Frontier, list[dict[str, Any]]]]:
    """Шаги дренажа одного seed-root: волновой цикл, frontier, коммит исходов (F4).

    Тело волнового цикла корня, вынесенное **дословно** из прежнего дренажа; изменилось
    только то, что относится к отправке волны: ``_commit_wave`` разделён на
    :func:`_dispatch_wave` (отправка), :func:`_await_wave` (ожидание) и
    :func:`_commit_wave_results` (FIFO-коммит), а между отправкой и коммитом стоит
    ``yield``. С F4b генератор отдаёт **план** волны (:class:`WavePlan`): допуск волны
    (право на host'ы) решается до отправки, поэтому отправку и ожидание исполняет
    драйвер, а коммит остаётся здесь.

    **Зачем генератор.** Полоса при чередовании корней — не объект, а **право на host**:
    один корень штатно фетчит несколько host'ов, поэтому носителем состояния остаётся
    корень, а «полоса» выражается правом на host. Тотальная мутация сеанса и корпуса
    допустима только на главном потоке (T5), поэтому параллельность здесь —
    кооперативные шаги, а не потоки-полосы: генератор выдаёт план волны и получает
    исходы, драйвер решает, когда их ждать. Пока драйвер гоняет один корень до конца
    (``host_concurrency`` None/1), поведение прежнее байт-в-байт.

    **Порядок — инвариант.** Внутри корня коммит идёт FIFO-порядком отправки: от него
    зависят победитель дедупа, порядок ``errors`` и порядок записи корпуса. Оракул —
    ``tests/pipeline/test_corpus_write_order.py``.

    **Пул — параметр, а не владение.** ``pool`` принадлежит полосе: при чередовании
    корней одновременно живут не более ``host_concurrency`` пулов фетча (и столько же
    пулов браузерного маршрута), а не по пулу на корень. ``pool is None`` — прежний
    последовательный путь (``max_workers`` ``None``/``1``), байт-в-байт.

    **Backpressure (F6, §4.3).** ``_await_backlog`` вызывается в голове каждой волны —
    до изъятия узлов из frontier, то есть до того, как приём расширит бэклог. Возвращает
    причину остановки приёма (``"extract_backlog_full"``) либо ``None``; саму запись
    ``crawl_backpressure`` с числами публикует носитель (:func:`core.pipeline.extract_queue.
    wait_for_backlog` вызывается ровно там). Ожидание исполняется на главном потоке
    драйвера: бэклог лежит на общем носителе, поэтому пауза приёма — фазовая, а не
    корневая.

    **Жёсткий сторож свободного места (решение владельца 01.11).** ``_await_disk`` стоит
    **перед** ``_await_backlog``, и порядок здесь — смысл, а не вкус: ожидание бэклога
    освобождает носитель только в учёте (``corpus_raw`` хранится бессрочно, I19), поэтому
    при нехватке места ждать бессмысленно — сначала надо узнать, что места нет. ``True``
    означает «приём остановлен»: корень усечён причиной ``disk_low_headroom`` и выходит из
    цикла, а новые волны прогона не отправляются вовсе (сторож — фазовый, остановка
    окончательная). Исключения нет: остановка — исход прогона, а не сбой (I4).
    """
    waves = 0
    while True:
        # Жёсткий сторож свободного места — до изъятия работы и до ожидания бэклога:
        # места нет ⇒ приём останавливается, а причина называется усечением корня.
        if _await_disk():
            frontier.mark_truncated(DISK_LOW_HEADROOM_REASON)
            break
        # F6 (§4.3): перед изъятием новой работы — наблюдение бэклога. Предел объявлен
        # осью, и его достижение останавливает **приём**, а не вежливость: причина
        # называется усечением корня (``partial``), а не тишиной.
        backlog_stop = _await_backlog()
        if backlog_stop is not None:
            frontier.mark_truncated(backlog_stop)
            break
        # D3: seeds sitemap ставятся пачкой; ``max_documents`` обязан
        # ограничивать и их (не только обнаруженные ссылки). Без лимита —
        # всегда есть ёмкость. Проверка — в голове цикла на главном потоке.
        # 09 §6.3: ревалидация известных URL бюджет не расходует; если
        # голова очереди — такая группа, дренаж продолжается (бюджет
        # исчерпан только для discovery).
        if not _has_fetch_capacity() and not frontier.next_is_revalidation():
            frontier.mark_truncated("max_documents")
            break
        if executor is not None:
            # --- P3-A2: executor-волна (раунд). Один раунд = один submit; --- #
            # раунды строго последовательны (набор работы N+1 зависит от ссылок N).
            if waves >= max_waves:
                # Предохранитель (§1(iii)): неограниченный BFS по общему edge —
                # самонаведённый DoS. Никогда не останавливаться молча: усечение
                # ``round_budget`` + явная ошибка + сеанс ``PARTIAL``.
                frontier.mark_truncated("round_budget")
                errors.append(
                    f"{pid}/{root_url}: предохранитель раундов исчерпан "
                    f"({max_waves} волн) — обход остановлен "
                    "(round_budget)"
                )
                emit()
                break
            remaining = (
                None
                if limits.max_documents is None
                else limits.max_documents - frontier.fetched
            )
            # M2: всегда впускаем непрерывный revalidation-блок **плюс**
            # discovery-room. Иначе при 0 < remaining < wave_items волна
            # берёт лишь remaining свободных URL, fetched не растёт, и
            # предохранитель раундов срабатывает на хвосте ревалидации.
            reval_n = min(
                frontier.revalidation_head_count(wave_items),
                wave_items // 2,
            )
            room = (
                wave_items
                if remaining is None
                else min(wave_items, max(0, remaining))
            )
            wave_size = min(
                len(frontier),
                wave_items,
                max(reval_n, reval_n + room),
            )
            if wave_size <= 0:
                # A2 (F3): нулевая волна при непустой очереди — это остановка
                # по бюджету изъятия. Она обязана быть видимой: без пометки
                # усечение было бы тихим (осталось N узлов, которых не изымаем).
                if len(frontier) and not frontier.has_capacity:
                    frontier.mark_truncated("max_documents")
                break
            entries: list[_WaveEntry] = []
            work: list[CrawlWorkItem] = []
            wave_has_discovery = False
            for _ in range(wave_size):
                node = frontier.next()
                if node is None:
                    break
                work_url, depth = node
                if not frontier.is_revalidation(work_url):
                    wave_has_discovery = True
                if frontier.blacklisted(work_url):
                    # D1/H: блэклист применяется и перед фетчем.
                    entries.append(
                        _WaveEntry(pid, work_url, depth, "blacklisted")
                    )
                    continue
                if _skip_document_prefetch(work_url):
                    # P5: документ при ``off`` — pre-fetch-пропуск (виден).
                    emit()
                    continue
                prior_item = items.get((pid, work_url))
                if terminal_failure_kind(prior_item) is not None:
                    # Определённый ответ «ресурса нет» уже получен: в работу
                    # не подаём (иначе 404 стоил бы запросов на каждом цикле).
                    entries.append(
                        _WaveEntry(
                            pid,
                            work_url,
                            depth,
                            "terminal_not_found",
                            prior_item=prior_item,
                        )
                    )
                    continue
                if (
                    max_attempts is not None
                    and prior_item is not None
                    and prior_item.attempts >= max_attempts
                ):
                    entries.append(
                        _WaveEntry(
                            pid,
                            work_url,
                            depth,
                            "attempts",
                            prior_item=prior_item,
                        )
                    )
                    continue
                work_target = resolve_target(pid, work_url)
                resume_source = (
                    resume_retained(
                        prior_item,
                        max_bytes=limits.max_response_bytes,
                        artifact_max_bytes=limits.max_artifact_bytes,
                        attachment_formats=limits.attachment_formats,
                    )
                    if prior_item is not None
                    else None
                )
                reject = (
                    resume_source
                    if isinstance(resume_source, RetainedReject)
                    else None
                )
                resume = (
                    resume_source
                    if isinstance(resume_source, RetainedRaw)
                    else None
                )
                # Дескриптор коммита строится вручную (не ``local_prefetch``):
                # browser-guard/``active`` — оси локального краулера, а здесь
                # байты доставляет исполнитель; ``browser_blocked`` всегда False.
                desc = LocalFetch(
                    pid=pid,
                    wu=work_url,
                    depth=depth,
                    fetch_plan=replace(
                        work_target.plan,
                        max_response_bytes=limits.max_response_bytes,
                    ),
                    active=None,
                    engine_value=work_target.engine,
                    prior_item=prior_item,
                    resume=resume,
                    browser_blocked=False,
                    poolable=False,
                    host=(urlsplit(work_url).hostname or "").lower(),
                    scope_host=frontier.root_host,
                    enforce_redirect_scope=limits.max_depth > 0,
                    reject=reject,
                )
                entries.append(
                    _WaveEntry(pid, work_url, depth, "fetch", desc=desc)
                )
                if resume is None and reject is None:
                    # Только живой фетч подаётся исполнителю; resume минует submit.
                    work.append(
                        work_item(
                            pid, work_url, work_target, robots=robots_enabled
                        )
                    )
            # F8 (17 §7.2 п.2): объявленная вежливость host'ов этой волны — из
            # дескрипторов, то есть из тех планов, что уходят исполнителю (резолв
            # профиля сайта уже сделан). Пишется до подачи: отказ исполнителя — тоже
            # попытка (сеть трогалась), а пропущенный host сделал бы §7.2 №1
            # непроверяемым.
            _note_wave_attempts(frontier, entries)
            # ``fetch_wait_s`` split-пути: ``submit_collect`` — это подача +
            # опрос статуса + сбор результатов, то есть **ожидание** исполнителя
            # (работу делает он, а не главный поток). Тот же бакет, что и у
            # локального пула: «участок фетча» — одна стадия продукта.
            _fetch_started = time.perf_counter()
            try:
                collected = submit_collect(work)
            finally:
                timings.fetch_wait_s += time.perf_counter() - _fetch_started
            for entry in entries:
                if entry.kind == "blacklisted":
                    errors.append(
                        f"{entry.pid}/{entry.wu}: зачёркнут блэклистом "
                        "перед фетчем — пропущен"
                    )
                    continue
                if entry.kind == "terminal_not_found":
                    # Терминальный класс уже получен: повторного фетча не было,
                    # элемент остаётся ``FAILED`` со своим ``error_kind``.
                    prior_item = entry.prior_item
                    kind = (
                        terminal_failure_kind(prior_item)
                        or FailureKind.NOT_FOUND
                    )
                    mark(
                        entry.pid,
                        entry.wu,
                        CrawlItemStatus.FAILED,
                        error_kind=kind.value,
                        error=(
                            prior_item.error
                            if prior_item is not None and prior_item.error
                            else f"{kind.value}: терминально, без повтора"
                        ),
                        depth=entry.depth,
                    )
                    emit()
                    continue
                if entry.kind == "attempts":
                    prior_item = entry.prior_item
                    if prior_item is None:
                        # См. инвариант ``_WaveEntry(kind="attempts")`` выше:
                        # запись создаётся только по существующему элементу.
                        raise AssertionError(
                            f"{entry.pid}/{entry.wu}: attempts-запись "
                            "без prior_item"
                        )
                    reason = (
                        f"max_attempts exhausted "
                        f"(attempts={prior_item.attempts} >= {max_attempts})"
                    )
                    errors.append(f"{entry.pid}/{entry.wu}: {reason}")
                    mark(
                        entry.pid,
                        entry.wu,
                        CrawlItemStatus.FAILED,
                        error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                        error=reason,
                        depth=entry.depth,
                    )
                    emit()
                    continue
                desc = entry.desc
                if desc is None:
                    # Инвариант ``_WaveEntry``: не-attempts/не-блэклист запись
                    # несёт desc — иначе коммитить нечего.
                    raise AssertionError(
                        f"{entry.pid}/{entry.wu}: запись волны без desc"
                    )
                if desc.resume is not None or desc.reject is not None:
                    # Resume/reject: живого фетча нет — коммит локально без submit.
                    outcome = commit_local(desc)
                    _commit(entry.pid, entry.wu, entry.depth, outcome)
                    continue
                result = collected.get((entry.pid, entry.wu))
                if result is None:
                    errors.append(
                        f"{entry.pid}/{entry.wu}: executor: результат не получен"
                    )
                    mark(
                        entry.pid,
                        entry.wu,
                        CrawlItemStatus.FAILED,
                        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                        error="executor: результат не получен",
                        depth=entry.depth,
                    )
                    emit()
                    continue
                if result.error or result.raw_bytes is None:
                    reason = result.error or "executor: пустой raw_bytes"
                    errors.append(f"{entry.pid}/{entry.wu}: executor: {reason}")
                    # Наблюдаемость (тот же дефект, что и на локальном пути):
                    # шов уже несёт ``http_status`` отказа — не теряем его.
                    # Класс отказа не меняется (``source_unavailable``).
                    mark(
                        entry.pid,
                        entry.wu,
                        CrawlItemStatus.FAILED,
                        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                        error=reason,
                        depth=entry.depth,
                        http_status=result.http_status,
                    )
                    emit()
                    continue
                # S1: применяемый движок — фактический транспорт из результата шва.
                desc.engine_value = result.engine or desc.engine_value
                page = RawPage(
                    url=result.final_url or result.url,
                    content=result.raw_bytes,
                    fetched_at=result.fetched_at or "",
                    fetch_mode=desc.fetch_plan.fetch,
                    lang=None,
                    status_code=result.http_status,
                    content_type=result.content_type,
                    engine=result.engine,
                    # 09 §6.3: валидаторы ответа и признак 304 переносятся в
                    # ``RawPage`` (раньше молча терялись — реальный дефект):
                    # без них ``url_state`` не сохранит ETag/Last-Modified, а
                    # ``304`` не распознается как переиспользование.
                    etag=result.etag,
                    last_modified=result.last_modified,
                    not_modified=result.not_modified,
                )
                outcome = commit_local(desc, page=page)
                _commit(entry.pid, entry.wu, entry.depth, outcome)
            # M2: предохранитель — BFS-guard, поэтому чисто
            # ревалидационная волна его не расходует.
            if wave_has_discovery:
                waves += 1
            continue
        if pool is None:
            # --- Последовательный путь (max_workers None/1): как прежде. --- #
            # A2 (F3): бюджет изъятия проверяется **до** изъятия — узел,
            # снятый с очереди и отвергнутый по ёмкости, выпал бы из счёта
            # «в очереди осталось N». ``peek`` не изымает и о бюджете не решает.
            head = frontier.peek()
            if head is None:
                break
            work_url, depth = head
            # 09 §6.3: бюджет discovery исчерпан, а голова очереди — не
            # ревалидационный лист: останавливаем обход (ревалидация обрабатывается
            # отдельно и бюджет не расходует). Окно экстракции в ёмкость не входит
            # (F5 плана 17): иначе ``extract_workers`` влиял бы на ``truncated``.
            if not _has_fetch_capacity() and not frontier.is_revalidation(
                work_url
            ):
                frontier.mark_truncated("max_documents")
                break
            if frontier.next() is None:
                # Ёмкость (``fetched``) исчерпана самим ``next``: он уже поставил
                # усечение ``max_documents``.
                break
            # D1/H: блэклист применяется и перед фетчем (в дополнение к постановке).
            if frontier.blacklisted(work_url):
                errors.append(
                    f"{pid}/{work_url}: зачёркнут блэклистом перед фетчем — пропущен"
                )
                continue
            if _skip_document_prefetch(work_url):
                # P5: документ при ``off`` — pre-fetch-пропуск (виден; не в max_documents).
                emit()
                continue
            prior_item = items.get((pid, work_url))
            if (
                max_attempts is not None
                and prior_item is not None
                and prior_item.attempts >= max_attempts
                and terminal_failure_kind(prior_item) is None
            ):
                reason = (
                    f"max_attempts exhausted "
                    f"(attempts={prior_item.attempts} >= {max_attempts})"
                )
                errors.append(f"{pid}/{work_url}: {reason}")
                mark(
                    pid,
                    work_url,
                    CrawlItemStatus.FAILED,
                    error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                    error=reason,
                    depth=depth,
                )
                emit()
                continue

            work_target = resolve_target(pid, work_url)
            # F8 (17 §7.2 п.2): последовательный путь не строит ``LocalFetch`` в этой
            # области видимости (его собирает ``process_local``), поэтому берётся **тот
            # же** резолвнутый план цели, который уходит в фетч: ``local_prefetch``
            # внутри заменяет только ``max_response_bytes``, ``rate_limit`` не трогая.
            # Второго резолва профиля здесь нет.
            _note_attempt_rate(
                frontier,
                (urlsplit(work_url).hostname or "").lower(),
                work_target.plan.rate_limit,
            )
            # Последовательный путь (``max_workers in (None, 1)``): фетч inline на
            # главном потоке, поэтому ``fetch_wait_s`` здесь — **работа**, а не
            # ожидание (как и inline-ветка волны). Бакет общий: «участок фетча» —
            # одна стадия продукта.
            _fetch_started = time.perf_counter()
            try:
                outcome = process_local(
                    pid,
                    work_url,
                    work_target,
                    depth=depth,
                    scope_host=frontier.root_host,
                    # B4: жёсткий redirect-scope — только при discovery (max_depth > 0);
                    # на глубине 0 финальная страница ретейнится как прежде.
                    enforce_redirect_scope=limits.max_depth > 0,
                    robots=robots_enabled,
                    max_response_bytes=limits.max_response_bytes,
                    artifact_max_bytes=limits.max_artifact_bytes,
                    attachment_formats=limits.attachment_formats,
                )
            finally:
                timings.fetch_wait_s += time.perf_counter() - _fetch_started
            _commit(pid, work_url, depth, outcome)
            continue

        # --- D4: волна fetch-only. Размер волны = min(K, остаток ёмкости, длина
        # очереди): пере-удержание ``max_documents`` структурно невозможно.
        # Диспатч (prefetch + submit) и коммит — на главном потоке; off-thread
        # идёт только ``limiter.wait`` + ``active.fetch``. --- #
        remaining = (
            None
            if limits.max_documents is None
            else limits.max_documents - frontier.fetched
        )
        # M2: та же формула, что в executor-ветке: непрерывный
        # revalidation-блок + discovery-room, без роста batch.
        reval_n = min(
            frontier.revalidation_head_count(max_workers),
            int(max_workers) // 2,
        )
        room = (
            int(max_workers)
            if remaining is None
            else min(int(max_workers), max(0, remaining))
        )
        wave_size = min(
            len(frontier),
            int(max_workers),
            max(reval_n, reval_n + room),
        )
        if wave_size <= 0:
            # A2 (F3): та же видимость остановки по бюджету изъятия, что в
            # executor-ветке: непустая очередь + исчерпанный ``fetched``.
            if len(frontier) and not frontier.has_capacity:
                frontier.mark_truncated("max_documents")
            break
        entries: list[_WaveEntry] = []
        for _ in range(wave_size):
            node = frontier.next()
            if node is None:
                break
            work_url, depth = node
            if frontier.blacklisted(work_url):
                entries.append(
                    _WaveEntry(pid, work_url, depth, "blacklisted")
                )
                continue
            if _skip_document_prefetch(work_url):
                # P5: документ при ``off`` — pre-fetch-пропуск (виден).
                emit()
                continue
            prior_item = items.get((pid, work_url))
            if (
                max_attempts is not None
                and prior_item is not None
                and prior_item.attempts >= max_attempts
                and terminal_failure_kind(prior_item) is None
            ):
                entries.append(
                    _WaveEntry(
                        pid, work_url, depth, "attempts", prior_item=prior_item
                    )
                )
                continue
            work_target = resolve_target(pid, work_url)
            desc = local_prefetch(
                pid,
                work_url,
                work_target,
                depth=depth,
                scope_host=frontier.root_host,
                enforce_redirect_scope=limits.max_depth > 0,
                robots=robots_enabled,
                max_response_bytes=limits.max_response_bytes,
                artifact_max_bytes=limits.max_artifact_bytes,
                attachment_formats=limits.attachment_formats,
            )
            # D2/e: browser-элементы НИКОГДА не пулятся (``BrowserCrawler`` водит
            # единственную ``_page`` без лока) — у них свой маршрут с одним потоком
            # (см. ``_dispatch_wave``); resume-элементы тоже минуют пул (живого
            # фетча нет; ``poolable``).
            if desc.poolable and pool is None:
                # Инвариант ветки: ``poolable`` выставляется только когда создан
                # пул фетча этого корня (``executor is None`` путь пула не имеет вовсе).
                raise AssertionError(
                    f"{desc.pid}/{desc.wu}: poolable без пула фетча"
                )
            entries.append(
                _WaveEntry(pid, work_url, depth, "fetch", desc=desc)
            )
        # F8 (17 §7.2 п.2): та же карта для пул-волны — до выдачи плана драйверу,
        # чтобы объявленная вежливость не зависела от того, дошёл ли исход.
        _note_wave_attempts(frontier, entries)
        # Точка плана волны (F4b): корень отдаёт готовые записи **до** отправки, драйвер
        # решает допуск (право на host'ы), отправляет и ждёт. Коммит ниже остаётся
        # FIFO-порядком этого корня и исполняется на главном потоке сразу после того, как
        # драйвер вернёт исходы.
        outcomes = yield _wave_plan(entries)
        _commit_wave_results(
            entries,
            outcomes,
            commit_local=commit_local,
            commit=_commit,
            mark=mark,
            emit=emit,
            errors=errors,
            max_attempts=max_attempts,
        )
    return frontier, list(sitemap_skips)


def _note_attempt_rate(
    frontier: Frontier, host: str | None, spec: RateLimitSpec | None
) -> None:
    """F8 (17 §7.2 п.2): записать объявленную вежливость живой попытки корня.

    Значение — из **того плана**, которым пойдёт запрос (``desc.fetch_plan`` уже прошёл
    резолв профиля сайта), а не из конфига: второй резолв разошёлся бы с фактическим
    пейсингом, и провенанс стал бы ложью о вежливости. Пишется по host'у, потому что один
    корень штатно фетчит несколько host'ов с разными минимумами.

    Пишется один раз на host (первое значение побеждает) и **до** исхода: отказ фетча —
    тоже попытка (сеть трогалась, ``fetch_started_at`` записан), а пропущенный host сделал
    бы критерий §7.2 №1 непроверяемым.
    """
    frontier.note_rate_limit(host, spec)


def _note_wave_attempts(frontier: Frontier, entries: Sequence[_WaveEntry]) -> None:
    """То же для готовых записей волны — по дескрипторам, которые уйдут в работу.

    Попыткой считается только **живой** фетч: ``resume`` (retained-байты), ``reject``
    (отказ до сети), ``terminal`` (известный терминальный класс) и ``browser_blocked``
    сеть не трогают, поэтому их host в карту не попадает — иначе провенанс называл бы
    вежливость host'а, к которому корень не обращался.
    """
    for entry in entries:
        desc = entry.desc
        if entry.kind != "fetch" or desc is None:
            continue
        if (
            desc.resume is not None
            or desc.reject is not None
            or desc.terminal
            or desc.browser_blocked
        ):
            continue
        _note_attempt_rate(frontier, desc.host, desc.fetch_plan.rate_limit)


def lane_width(limits: CrawlLimits, *, executor: CrawlExecutorPort | None) -> int | None:
    """Ширина пула фетча полосы: ``max_workers`` или ``None`` (живого пула нет).

    Одна формула на два читателя — драйвер полос (создаёт пул) и сам корень (выбирает
    ветку «волна/последовательно»): второй копии правила быть не должно, иначе полоса
    получила бы пул, которого корень не ждёт, или наоборот.

    ``None`` возвращается в трёх случаях, и все три — не деградация: ``executor``
    (split: работу делает шов, ``submit_collect`` — один блокирующий вызов волны) и
    ``max_workers`` ``None``/``1`` (прежний последовательный путь без пула).
    """
    if executor is not None:
        return None
    if limits.max_workers is None or int(limits.max_workers) <= 1:
        return None
    return int(limits.max_workers)


@dataclass(frozen=True)
class ProductDrainScope:
    """Продуктовый скоуп дренажа: состояние, живущее весь продукт и общее корням.

    Один объект на продукт. Здесь то, что не принадлежит ни одному корню: накопитель
    ингеста, дедуп-множества продукт-партиции (I17), судьба SERP-находок, множество
    уже заряженных в resume-бюджет URL, все известные URL ревалидации и предикат
    владельца URL. Корни одного продукта **делят** это состояние, поэтому корень не
    вправе получить копию.

    Почему ``frozen``. Поля — ссылки на мутируемые контейнеры; ``frozen`` запрещает
    подменить поле (корень не подсунет свой словарь вместо продуктового), но не
    запрещает писать в содержимое — а писать он обязан именно в общее: копия вернула
    бы тихий дефект «корень обновил копию, сосед не увидел».

    Владелец — фаза сборки корпуса: создаётся на границе продукта
    (``corpus._build_corpus``), живёт до конца продукта.
    """

    #: Идентификатор продукта: единственное место, где он теперь назван в дренаже.
    pid: str
    #: Накопитель продукта (документы/чанки/счётчики ингеста).
    ingest: ProductIngest
    #: I17: ``content_hash → doc_id`` в пределах продукт-партиции.
    seen_docs: dict[str, str]
    #: I17: ``chunk_id`` в пределах продукт-партиции.
    seen_chunk_ids: set[str]
    #: Наблюдённая судьба SERP-находок (``enqueued``/``dup``/``blacklisted``).
    serp_ingest: dict[str, Any]
    #: F13: durable-URL, уже заряженные в resume-бюджет **продукта**.
    resume_counted: set[str]
    #: Известные URL ревалидации: вне бюджета discovery (B2/F13).
    revalidation_url_set: set[str]
    #: Самый специфичный корень-владелец URL (тот же предикат, что у группировки
    #: ревалидации); ``None`` — URL не принадлежит ни одному авторскому корню.
    owner_root: Callable[[str], str | None]


@dataclass(frozen=True)
class PhaseDrainScope:
    """Фазовый скоуп дренажа: состояние и оси одной фазы сборки корпуса.

    Один объект на фазу (не на продукт и не на корень): каналы видимости
    (``errors``), store и идентификатор прогона, порты (чанкер, экстрактор, контейнеры),
    фазовые очереди экстракции/финализаторов и пул экстракции, накопители наблюдений
    (пропуски документов, пробы артефактов, наблюдения архивов, члены контейнеров),
    оси фазы (``max_attempts``, размер волны, предохранитель раундов, таймаут) и режим
    «краул без конверсии» с адресом durable-очереди.

    Почему ``frozen``. Поля — ссылки на мутируемые контейнеры и на неизменяемые оси:
    подменять ось в полёте нельзя (она — условие измерения), а писать в общие списки
    ``errors``/наблюдений обязательно. ``frozen`` различает эти два случая.

    ``fetch_pool``/``browser_pool`` — единственные поля **полосы**: их создаёт драйвер
    на полосу (``lane_width``) и передаёт при допуске корня, поэтому на корень фазовый
    носитель копируется через :func:`dataclasses.replace`. Копия — не новое состояние:
    все контейнеры внутри неё общие, меняются только эти две ссылки.
    """

    #: Канал видимости фазы: каждая деградация обязана быть названа здесь.
    errors: list[str]
    #: Склад фазы (документы/чанки/очередь экстракции/сеанс).
    store: Any
    #: Идентификатор прогона для durable-записей провенанса (``None`` — прогона нет).
    run_id: str | None
    #: Чанкер фазы (версия входит в ``pb``).
    chunker: ChunkerPort
    #: Читатель контейнеров фазы (``None`` — бэкенда нет, спуск не открывается).
    container_for: Callable[[str | Path], ContainerPort | None] | None
    #: Durable-элементы сеанса: resume-бюджет и адрес документа (I19).
    items: Mapping[tuple[str, str], CrawlSessionItem]
    #: ``--max-attempts``: терминальный ``FAILED`` по исчерпанию попыток.
    max_attempts: int | None
    #: Размер волны фетча в узлах (ось executor-транспорта).
    wave_items: int
    #: Предохранитель раундов BFS (``round_budget``).
    max_waves: int
    #: Исполнитель краула (split: работу делает шов, локального пула нет).
    executor: CrawlExecutorPort | None
    #: Накопители фазы (наблюдённые объёмы, а не лимиты).
    counters: CorpusCounters
    #: Бакеты стадий продукта: ожидание фетча, нарезка, эмбеддинг.
    timings: _CorpusStageTimings
    #: Объявленные host'ы по продуктам (scope корня, §6).
    scope_hosts_by_product: Mapping[str, frozenset[str]]
    #: F12: ``(pid, loc)`` sitemap, уже принятые в очередь: один loc — один фетч.
    sitemap_seed_used: set[tuple[str, str]]
    #: P5: видимые пропуски документов (``off``-уровень) для агрегата сборки.
    document_skips: list[dict[str, Any]]
    #: F5 (17 §4.1): очередь конверсии (:class:`core.pipeline.extract_queue.ExtractQueue`).
    #: При активном сеансе она durable, без сеанса — в памяти процесса; краул в обоих
    #: случаях только пишет строки и никогда не конвертирует.
    queue: Any
    #: Адрес сеанса краула (``None`` — durable-очередь не адресуема, очередь в памяти).
    session_id: str | None
    #: F6 (17 §4.3): корень retention. Нужен только для одного — публиковать
    #: ``free_bytes`` рядом с ``backlog_bytes``; ``None`` — носитель неизвестен, и тогда
    #: число не выдумывается (предел по-прежнему применяется к байтам очереди).
    raw_root: str | None = None
    #: Жёсткий сторож свободного места (решение владельца 01.11): **один на фазу**, потому
    #: что свободное место принадлежит носителю фазы, а не корню. ``None`` — сторож не
    #: подключён (прямой вызов дренажа): приём не останавливается, и числа о месте никто
    #: не публикует. Тип — :class:`core.pipeline.extract_queue.DiskHeadroomGuard`; поле
    #: ``Any`` намеренно, потому что ``corpus_drain`` не владеет этим контрактом.
    disk_guard: Any = None
    #: F4b: пул фетча **полосы** (создаёт драйвер, см. :func:`lane_width`).
    fetch_pool: Executor | None = None
    #: F4b: пул не-poolable маршрута (браузер) — один поток на полосу.
    browser_pool: Executor | None = None
    #: F7 (17 §2): host'ы **своего шарда** (``None`` — шардов нет, фильтра нет). Фильтр
    #: стоит рядом со scope-предикатом и не подменяет его: scope отвечает «этот URL
    #: принадлежит корню», шард — «это моё железо». Отказ виден счётчиком
    #: ``host_out_of_shard`` в провенансе корня.
    shard_hosts: frozenset[str] | None = None
    #: F7: узел, исполнивший сеанс (провенанс развёртывания; в ``run_id`` не входит).
    node_id: str | None = None


@dataclass(frozen=True)
class PhaseDrainOperations:
    """Замкнутые операции фазы: **чем** корень действует, а не что он читает/меняет.

    Все поля — callable'ы, связанные один раз на фазу в ``corpus._build_corpus``
    (``partial`` и замыкания над сеансом, складом и наблюдением фазы). Корень их только
    вызывает: подменить операцию нельзя (``frozen``), и это важно — операция несёт
    адресацию записи (какой продукт, какой сеанс), а не параметр корня.

    **Сигнатуры операций не меняются.** Их держат ``corpus.py`` и тесты (в том числе
    подменой атрибутов модуля ``corpus``), поэтому носитель их не переписывает: он
    называет владельца, а не форму вызова.

    Владелец — фаза: носитель создаётся один раз, до цикла продуктов, и переиспользуется
    всеми продуктами и корнями.
    """

    #: Наблюдаемость: границы продукта/строки прогресса (мутация — на главном потоке).
    emit: Callable[[], None]
    #: Отметка элемента сеанса (``CrawlItemStatus`` + класс сбоя).
    mark: Callable[..., None]
    #: Коммит исхода на главном потоке (retention + доводка + BFS).
    commit_local: Callable[..., Any]
    #: Локальный фетч (единственная функция, исполняемая в потоке пула).
    local_prefetch: Callable[..., LocalFetch]
    #: Доводка одного локального элемента (запись страницы в сеанс).
    process_local: Callable[..., Any]
    #: Волна через шов (split): подача + опрос + сбор результатов.
    submit_collect: Callable[..., Mapping[tuple[str, str], Any]]
    #: Рабочий элемент исполнителя из разрешённой цели.
    work_item: Callable[..., CrawlWorkItem]
    #: Резолв цели краула (движок/вид из слоёв источников).
    resolve_target: Callable[..., Any]
    #: D3: развёртка per-site sitemap корня в подчинённые depth-0 seeds.
    fresh_sitemap_seeds: Callable[..., SitemapSeeds]
    #: Оси краула корня (``app < run < site``) — единственная формула резолва.
    limits_for: Callable[[str], CrawlLimits]


def root_lifecycle(
    root: RootPlan,
    *,
    product: ProductDrainScope,
    phase: PhaseDrainScope,
    operations: PhaseDrainOperations,
) -> Generator[WavePlan, WaveOutcomes, tuple[Frontier, list[dict[str, Any]]]]:
    """D1/D3: BFS-drain одного seed-root (scope/блэклист/лимиты/глубина).

    Seeds — глубина 0; ссылка со страницы глубины ``d`` → ``d+1``. ``root.sitemap_seeds``
    — развернуть per-site sitemap корня в дополнительные depth-0 seeds (D3); они
    **подчинены** scope корня (``frontier.in_scope``), проходят тот же блэклист/
    бюджет ``max_documents``, что и seed оператора.
    ``root.known_urls`` — известные URL этого корня (scope inference): ставятся
    **после** авторского seed и **до** BFS отдельным путём
    (:meth:`Frontier.add_revalidate_seed`), поэтому не расходуют ``max_documents``,
    не расширяют BFS (листья) и дедуплицируются ``_seen`` с переоткрытием ссылкой.
    ``root.serp_seeds`` — документы, найденные внешним поиском (I3/§5): ставятся
    depth-0 seed'ами **этого** корня через :meth:`Frontier.add_seed`, то есть
    расходуют его ``max_documents`` и проходят блэклист, но не становятся новыми
    корнями. Так «доход» поиска платит из бюджета уже объявленного корня
    (нет умножения бюджета числом находок) и ``seeds_declared`` остаётся
    авторским.
    ``root.seed=False`` — synthetic-бакет (orphan): авторского seed нет, только
    ревалидация; усечение такого бакета не пишется вызывающим.
    Ставка в очередь гейтится scope (объявленный host + path-граница, C) и
    блэклистом; ``max_documents`` — жёсткий стоп (не исключение): останавливает
    постановку/обход этого корня.
    Возвращает frontier с провенансом усечения (``fetched``/``truncated``/
    ``truncated_reason``), агрегированный провенанс отказов scope у ``<loc>``
    (пусто без sitemap/без отказов). Хвоста экстракции у корня нет (F5): краул
    заканчивается durable-строкой ``extract_queue``, конверсию исполняет потребитель.

    **Носители — по владению.** ``product`` (:class:`ProductDrainScope`) — общее
    состояние продукта, корни его делят, а не копируют. ``phase``
    (:class:`PhaseDrainScope`) — состояние и оси фазы, включая адрес durable-очереди и
    режим ``crawl_only``. ``operations`` (:class:`PhaseDrainOperations`) — замкнутые
    операции фазы (их сигнатуры держат ``corpus.py`` и тесты). Тело читает и пишет
    через эти три имени, поэтому «кто владеет этим полем» отвечает точка доступа, а не
    память читателя.

    ``phase.crawl_only`` (17 §4, F2) — режим «краул без конверсии»: добытый документ
    записывается durable-фактом в ``extract_queue``, а зонд артефакта, спуск в
    контейнеры, ``ingest_one``/``finish_one`` и нарезка **не** выполняются. Так
    «краул выполнен» перестаёт быть синонимом «корпус собран»: сборка — работа
    потребителя очереди. Счётчики (``max_documents``/``pages_fetched``/усечение) и
    расширение BFS-ссылок остаются теми же — иначе режим был бы другим краулом, а не
    тем же без конверсии. ``phase.session_id`` — адрес очереди; без него режим
    невозможен (факту некуда лечь).

    **F4b (17 §6): генератор шагов, а не функция.** Подготовка
    (limits/frontier/seeds/зарядка бюджета) исполняется при первом ``next`` — то есть в
    момент допуска корня драйвером, а не при создании объекта. Дальше корень выдаёт
    :class:`WavePlan` и получает :class:`WaveOutcomes`; **отправку** волны и **ожидание**
    исполняет драйвер (:mod:`core.pipeline.corpus_driver`), потому что допуск волны
    (право на host'ы) решается до отправки. Коммит исходов остаётся здесь и идёт
    FIFO-порядком отправки.

    ``phase.fetch_pool``/``phase.browser_pool`` — пулы **полосы**, их создаёт драйвер
    (одна пара на активную полосу, не на корень) и передаёт при допуске корня, из-за
    чего фазовый носитель копируется на корень через :func:`dataclasses.replace`. Их
    отсутствие при объявленном ``max_workers`` — ``AssertionError``: молчаливый
    inline-фетч на главном потоке схлопнул бы полосы.
    """
    root_url = root.root_url
    sitemap_seeds = root.sitemap_seeds
    revalidate = root.known_urls
    serp_seeds = root.serp_seeds
    seed = root.seed
    limits = operations.limits_for(root_url)
    frontier = Frontier(
        root_url,
        limits,
        scope_hosts=phase.scope_hosts_by_product.get(product.pid, frozenset()),
        # F7 (17 §2): host'ы своего шарда. ``None`` — карты нет, поведение байт-в-байт.
        shard_hosts=phase.shard_hosts,
    )
    # D3/дефект B (I17): ``<loc>`` sitemap — кандидаты **этого** корня; scope-фильтр
    # применяется до дедупа ``(pid, loc)``. Дедуп-пометка — не здесь, а по факту
    # приёма в очередь (F12, см. цикл ``extra_seeds`` ниже). Без sitemap
    # ``_expand_sitemap`` отдаёт ``[]`` без обращения к поллеру — путь не исполняется
    # (поведение байт-в-байт).
    sitemap = (
        operations.fresh_sitemap_seeds(product.pid, root_url, frontier)
        if sitemap_seeds
        else SitemapSeeds()
    )
    extra_seeds = sitemap.fresh
    # P5: уровень извлечения документов — свойство корня. Краул по нему решает, что
    # считается документом (pre/post-fetch пропуски), а извлекает по нему потребитель
    # очереди (строка несёт ``level``): второго источника уровня нет.
    level = limits.document_level

    def _record_document_skip(skip_url: str, reason: str) -> None:
        """P5: видимая запись пропуска документа (frontier + агрегат ``IndexBuild``).

        Параметр назван ``skip_url``, а не ``url``, намеренно: ``url`` —
        переменная цикла внешней области, и совпадение имён делало бы связывание
        неотличимым от позднего (B023).
        """
        frontier.note_document_skip(skip_url, reason)
        phase.document_skips.append(
            {
                "product_id": product.pid,
                "url": skip_url,
                "reason": reason,
                "level": level.value,
            }
        )

    def _skip_document_prefetch(work_url: str) -> bool:
        """P5: pre-fetch-пропуск документа при ``off`` (суффикс URL).

        ``True`` — URL пропущен (не фетчится, не ретейнится, не входит в
        ``max_documents``; запись видна в ``document_skips``). Блэклист побеждает
        (проверяется раньше). Content-type-детект суффикс-less URL — post-fetch путь.
        """
        if level is not DocumentLevel.OFF:
            return False
        if document_suffix(work_url) is None:
            return False
        _record_document_skip(work_url, "url_suffix")
        return True

    # P4/C1: окно экстракции резолвится **потребителем очереди**, а не корнем: краул
    # больше не конвертирует. Здесь остаётся только ёмкость discovery — чистый
    # ``max_documents − fetched``: работа конверсии «в полёте» бюджет не занимает,
    # потому что её нет в крауле.
    def _has_fetch_capacity() -> bool:
        """Есть ли у корня ёмкость ``max_documents`` (без in-flight экстракции)."""
        if limits.max_documents is None:
            return True
        return limits.max_documents - frontier.fetched > 0

    # A2b/§6(b)/§10: egress-политика robots — **условна от discovery** и
    # вычисляется ядром **на корень** (edge строит краулеры «голыми»). Значение
    # резолвится **одной** формулой в :func:`resolve_crawl_limits` (app-слой
    # ``crawl.robots`` переопределяет автодетект; иначе robots включён ровно при
    # обходе ссылок ``max_depth > 0`` или наличии sitemap у сайта корня) и там же
    # входит в ``crawl_config_digest``: поведение и идентичность не расходятся.
    # Без discovery — ``False`` (никаких лишних fetch ``robots.txt``, байт-в-байт).
    robots_enabled = limits.robots
    # B2 (resume-бюджет): уже израсходованные страницы этого корня считаются из
    # durable-элементов сеанса (status FETCHED/FAILED с реально доступным retained
    # raw), чей URL в scope корня. Иначе повторный прогон того же session_id начал
    # бы ``fetched`` с нуля, заново обошёл resumed-страницу, превысил max_documents
    # и ложно финализировался COMPLETED. Считаем только resume-пригодные элементы
    # (fresh-фетч этого прогона инкрементит счётчик сам), чтобы не задвоить бюджет.
    #
    # F13: бюджет — **продукта**, а не корня. При нескольких корнях на host URL
    # в scope двух корней иначе заряжался бы обоим, и ``max_documents`` исчерпывался
    # бы досрочно на resume. Заряжает **владелец** — самый специфичный
    # ``root_path_prefix`` (``owner_root``, тот же предикат, что у группировки
    # ревалидации), а не первый корень-претендент: иначе host-корень с префиксом
    # ``/`` забирал зарядку у docs-корня и потолок умножался на число перезапусков.
    # ``resume_counted`` остаётся продукт-wide страховкой от двойной зарядки.
    consumed = 0
    #: Заряженные этим корнем URL: каждый обязан быть и **дренируемым** (см. ниже).
    charged_urls: list[str] = []
    for (ipid, iurl), item in phase.items.items():
        if ipid != product.pid or iurl in product.resume_counted:
            continue
        # Известные URL ревалидации — вне бюджета discovery: иначе resumed
        # сессия заряжала бы ими ``max_documents``, seed стал бы ``truncated``
        # и продукт ложно ``complete=False`` (регрессия B2/F13 уровнем выше).
        if iurl in product.revalidation_url_set:
            continue
        # B023-исключение: ``owner_root`` пересоздаётся на каждом продукте
        # вместе с дренажом продукта (обе привязки — в одной итерации цикла), и
        # дренаж продукта завершается до следующей итерации, поэтому поздняя
        # привязка ячейки не подменяет владельца URL.
        owner = product.owner_root(iurl)  # noqa: B023 — см. выше
        if owner is not None:
            if owner != root_url:
                # Владелец другой (более специфичный) корень — он и зарядит.
                continue
        elif frontier.scope_admission(iurl) == SCOPE_OUT:
            # URL не принадлежит ни одному авторскому корню (например, запись
            # каталога feed/channel): прежний путь — корень, чей scope его
            # принимает, заряжает сам, а ``resume_counted`` не даёт задвоить.
            #
            # P6b: терминальный артефакт вне path-prefix этим корнем тоже
            # допускается, поэтому его resumed-элемент обязан заряжать бюджет —
            # иначе повторный прогон тихо добывал бы лишнюю страницу сверх
            # ``max_documents``.
            continue
        counted = resume_retained(
            item,
            max_bytes=limits.max_response_bytes,
            artifact_max_bytes=limits.max_artifact_bytes,
            attachment_formats=limits.attachment_formats,
        )
        if not isinstance(counted, RetainedRaw):
            # Нет пригодного resume (oversize/binary/несовпадение) — не бюджет.
            continue
        consumed += 1
        product.resume_counted.add(iurl)
        charged_urls.append(iurl)
    frontier.note_consumed(consumed)
    charged_set = set(charged_urls)
    if seed and not frontier.has_capacity and root_url in charged_set:
        # A2 (F3 плана 17): бюджет закрыт resume-зарядкой ⇒ как discovery seed его
        # никто не изымет (``next`` откажет). Но байты корня уже ретенированы и
        # зарядили бюджет — это ровно ревалидация известного URL, а её путь бюджет не
        # расходует. Иначе страница корня выпала бы из корпуса (дефект 3, 09 §6.3).
        seed_status = frontier.add_revalidate_seed(root_url)
    else:
        seed_status = frontier.add_seed(root_url) if seed else "synthetic"
    if seed_status == "queued":
        phase.counters.product_seeds_enqueued += 1
    seed_blacklisted = seed_status == "blacklisted"
    if seed_blacklisted:
        phase.errors.append(
            f"{product.pid}/{root_url}: seed зачёркнут блэклистом "
            "(exclude_paths/exclude_documents) — не enqueued"
        )
    # F7 (17 §2): корень принадлежит **чужому** шарду — этот узел его не краулит, и это
    # названо (счётчик ``host_out_of_shard`` в провенансе корня уже поставлен
    # ``Frontier.shard_admission``; здесь — строка, чтобы причина была в ``errors``, а не
    # только в структурном поле).
    seed_shard_blocked = seed_status == "host_out_of_shard"
    if seed_shard_blocked:
        phase.errors.append(
            f"{product.pid}/{root_url}: корень на host'е чужого шарда — не enqueued "
            f"(мой шард: {sorted(phase.shard_hosts or ()) or '—'}; "
            f"корпус этого host'а снимает другой узел)"
        )
    # A2 (F3 плана 17): бюджет ``max_documents`` решается **на изъятии**, поэтому
    # ``add_seed`` его больше не проверяет и статуса ``truncated`` не возвращает.
    # Но корень, чей бюджет уже израсходован (resume-зарядка ``note_consumed``), не
    # изымет **ничего**: ``next`` откажет на первом же узле. Это надо назвать сейчас,
    # потому что sitemap/SERP-находки расходуют **общий на продукт** loc-пул
    # (``sitemap_seed_used``): постановка их корнем, который за ними не сойдёт,
    # «съела» бы loc у другого корня того же продукта (F12).
    budget_closed = not frontier.has_capacity
    if budget_closed:
        frontier.mark_truncated("max_documents")
        phase.errors.append(
            f"{product.pid}/{root_url}: discovery не изымается — достигнут max_documents "
            "(усечение на изъятии; ревалидация известных URL бюджет не расходует)"
        )

    # 09 §6.3: известные URL — depth-0 листья **после** авторского seed и **до**
    # BFS. Enqueue до BFS позволяет ``frontier._seen`` схлопнуть известный URL,
    # переоткрытый ссылкой (иначе безусловный второй GET — потеря 304-экономии).
    # Путь вне бюджета discovery (``add_revalidate_seed``): ревалидация не является
    # «документом, который добавляет обход». Ранние возвраты ниже не должны
    # терять группу: blacklisted/truncated корень всё равно ревалидирует известные URL.
    revalidation_queued = 0
    for known in revalidate:
        if frontier.add_revalidate_seed(known) == "queued":
            revalidation_queued += 1
    # 09 §6.3 (дефект 3): **заряженный URL обязан быть и дренируемым.** Зарядка
    # бюджета (``consumed``) и работа расцеплены не должны быть: зарядка без
    # постановки в очередь платит бюджетом за работу, которой не будет. Поэтому
    # каждый заряженный элемент ставится depth-0 листом тем же путём, что и
    # ревалидация — **независимо от режима продукта**: предусловие набора «есть
    # durable-элементы для дренажа», а не «продукт инкрементальный». Измерено на
    # продукте, чей корень заряжался ровно в потолок 600/600 при 509 документах и
    # 25 чанках, тогда как 4935 чанков лежали в durable-субстрате: ``add_seed``
    # возвращал ``truncated``, дренаж выходил до обхода, и корпус терялся не сетью.
    #
    # Бюджет повторно **не** тратится: ``add_revalidate_seed`` ёмкость не проверяет,
    # а ``_commit_outcome`` считает такой fetch в ``revalidation_fetched``, не в
    # ``note_fetched`` (то есть ``max_documents`` не двигается).
    charged_queued = 0
    for known in charged_urls:
        if frontier.add_revalidate_seed(known) == "queued":
            charged_queued += 1
    # Ранний выход — только если дренировать действительно нечего: заряженные URL
    # (``charged_queued``) считаются наравне с ревалидацией известных URL, иначе
    # усечённый корень терял бы уже добытое дерево.
    if (
        (seed_blacklisted or budget_closed or seed_shard_blocked)
        and revalidation_queued == 0
        and charged_queued == 0
    ):
        operations.emit()
        return frontier, list(sitemap.skipped)

    # D3: дополнительные depth-0 seeds (``<loc>`` sitemap) — под тем же блэклистом и
    # бюджетом, что и seed оператора (``add_seed`` возвращает статус). Scope-фильтр
    # живёт **ровно в одном месте** — ``fresh_sitemap_seeds`` (тот же ``frontier``):
    # ``extra_seeds`` приходят только оттуда, поэтому повторный scope-чек здесь был бы
    # недостижимой защитой (F7 — удалён; тест на удаление фильтра есть).
    #
    # F12: ``(pid, loc)`` помечается использованным **только при приёме в очередь**
    # (``queued``/``dup`` — loc уже у этого корня). Отказ блэклистом/бюджетом не
    # расходует loc: другой корень того же продукта ещё вправе его взять. Инвариант
    # «один loc — не более одного фетча на продукт» держится именно этой пометкой.
    for extra in (() if (seed_blacklisted or budget_closed) else extra_seeds):
        extra_status = frontier.add_seed(extra)
        if extra_status in ("queued", "dup"):
            phase.sitemap_seed_used.add((product.pid, extra))
            if extra_status == "queued":
                phase.counters.product_seeds_enqueued += 1
        elif extra_status == "blacklisted":
            phase.errors.append(
                f"{product.pid}/{extra}: sitemap-seed зачёркнут блэклистом "
                "(exclude_paths/exclude_documents) — не enqueued"
            )

    # I3/§5: документы, найденные SERP-контуром, — depth-0 seeds **этого** корня.
    # Тот же путь, что у sitemap-`<loc>` (блэклист + ``max_documents`` + дедуп
    # ``_seen``), поэтому бюджет продукта не умножается на число находок, а
    # отказ виден (блэклист/потолок) — молчаливого дропа нет. Scope/SSRF хоста
    # уже проверены при допуске кандидата (``serp_discover``), path-prefix здесь
    # не применяется — как у терминальных артефактов: найденный документ не
    # продолжает навигацию корня, поэтому «раздел» к нему не относится.
    for found in serp_seeds:
        found_status = frontier.add_seed(found)
        if found_status == "queued":
            phase.counters.product_seeds_enqueued += 1
            product.serp_ingest["enqueued"] += 1
        elif found_status == "dup":
            product.serp_ingest["dup"] += 1
        elif found_status == "blacklisted":
            product.serp_ingest["blacklisted"].append(found)
            phase.errors.append(
                f"{product.pid}/{found}: serp-находка зачёркнута блэклистом "
                "(exclude_paths/exclude_documents) — не enqueued"
            )
        # A2 (F3): отказа по ``max_documents`` на постановке больше нет — находка
        # ставится в очередь, а бюджет решается на изъятии. Поэтому усечения на
        # постановке не бывает вовсе, и ключа ``truncated`` в наблюдении нет:
        # «поставлена и не изъята» видно числом ``queue_remaining`` в провенансе
        # корня. Мёртвый ключ (объявлен, но никем не наполнялся) удалён, а не
        # оставлен пустым: пустой список читался бы как наблюдённый факт
        # «усечения не было».

    def _extend_links(page: RawPage, depth: int | None, is_revalidation: bool) -> None:
        """D1: расширение BFS ссылками страницы — общий хвост обоих режимов.

        Вынесено в один носитель ради ``crawl_only`` (F2): режим «краул без конверсии»
        обязан обходить **тот же** frontier, иначе «краул выполнен» означало бы «обошли
        только seeds». При ``max_depth=0`` извлечение не выполняется вовсе (байт-в-байт
        прежнее поведение). 09 §6.3: ревалидация — лист; её ссылки не расширяют
        discovery (иначе глубокая страница, пере-добытая на depth 0, меняла бы
        coverage/стоимость).
        """
        if not is_revalidation and limits.max_depth > 0 and looks_like_html(page):
            frontier.offer(extract_links(page.content, page.url), depth)

    def _enqueue_extract(
        pid: str,
        work_url: str,
        page: RawPage,
        raw_path: Path,
        fetch_plan: FetchPlan,
        reuse_kind: ReuseKind,
    ) -> None:
        """Пишет факт «документ добыт» в очередь конверсии (17 §4.1).

        Адрес берётся из элемента сеанса: ``retain`` уже проставил там ``content_hash``
        и ``raw_path``, посчитанные по **retained-байтам**. Пересчёт из ``page.content``
        был бы другим хешем для staged-артефакта (тело в файле, ``content`` пуст) и
        разорвал бы цепочку I19 ещё до конверсии.

        Провенанс **получения** приходит из наблюдённого результата (``page`` и
        ``fetch_plan``), а не из строки сеанса: это ровно те значения, которые уходят в
        ``_ExtractRequest`` этого документа
        (:func:`core.pipeline.extract_consume._row_request`), поэтому потребитель
        воспроизводит условия измерения из строки вместо того, чтобы выводить их заново.
        Иначе тот же документ изъят другим маршрутом — другой
        ``extractor_version``/``doc_version_id`` — то есть другой корпус при тех же
        объявленных входах (I19, A3).

        Отсутствие адреса (элемент без хеша) — не повод молча потерять документ: факт
        уходит в ``errors`` фазы, то есть в ``results[].notes`` и манифест.

        ``reuse_kind`` — происхождение наблюдения **этого** исполнения: строка очереди
        durable и его не несёт (у resume тот же адрес и тот же ``status_code``, что у
        первого фетча), а ``corpus_docs.reuse_kind``/``http_status`` — факт именно цикла
        (16 §3.3). Поэтому он передаётся в очередь вместе со строкой.
        """
        item = phase.items.get((pid, work_url))
        #: Адрес — из элемента сеанса, если он есть. Без сеанса (durable-краул выключен,
        #: ``store``/``run_id`` не заданы) элемента нет, и адрес берётся из того же
        #: носителя, что и у сеанса: ``retain`` называет файл ``{hash}.bin``, поэтому
        #: ``raw_path.stem`` — хеш **retained-байтов** (I19), а не пересчёт из
        #: ``page.content`` (у staged-артефакта тело в файле, ``content`` пуст).
        content_value = getattr(item, "content_hash", None) or raw_path.stem
        raw_value = getattr(item, "raw_path", None) or str(raw_path)
        if not content_value or not raw_value:
            phase.errors.append(
                f"{pid}/{work_url}: extract_queue: нет адреса документа "
                "(content_hash/raw_path) — документ НЕ поставлен в очередь конверсии"
            )
            return
        phase.queue.put(
            work_url=work_url,
            reuse_kind=reuse_kind,
            item=ExtractQueueItem(
                session_id=phase.session_id or "",
                product_id=pid,
                #: Адрес документа — **финальный** URL ответа (``RawPage.url``), а не
                #: запрошенный: редирект меняет адрес, и ``corpus_docs.url`` обязан
                #: называть место, где документ лежит (по нему же разрешаются

                #: относительные ссылки). ``work_url`` — адрес работы краула и ключ
                #: элемента сеанса; он остаётся ключом аудита.
                url=page.url or work_url,
                content_hash=str(content_value),
                raw_path=str(raw_value),
                level=level.value,
                pdf_pipeline=limits.pdf_pipeline.value,
                #: Режим получения — **наблюдённый** (``RawPage.fetch_mode``), а не
                #: объявленный планом: браузерный адаптер вправе доставить байты
                #: HTTP-путём (``crawl_browser.py``), и именно это значение несла бы
                #: заявка экстракции.
                fetch_mode=_as_text(page.fetch_mode),
                #: Ось извлечения — из плана: она объявлена краулом для этого URL и
                #: определяет ``extractor_version`` документа.
                extract_route=_as_text(fetch_plan.extract),
                #: Движок: названный страницей (фактически доставивший байты) — первым,
                #: объявленный осью — если адаптер себя не назвал.
                engine=page.engine or fetch_plan.engine,
                content_type=page.content_type,
                lang=page.lang,
                #: ``RawPage.status_code`` — тот же факт, что ушёл бы в заявку. Для
                #: retained-байтов (``resume``) это ответ **прошлого** цикла: сервер в
                #: этом прогоне не отвечал, но заявка экстракции несла ровно его.
                status_code=page.status_code,
                #: Время получения — штамп страницы: он уже посчитан там, где ответ
                #: получен (``local_fetch``) или восстановлен из элемента сеанса.
                fetched_at=page.fetched_at or _now_iso(),
                enqueued_at=_now_iso(),
            )
        )

    def _close_refused_row(pid: str, work_url: str) -> None:
        """Терминальный отказ источника — строка очереди закрывается как ``skipped``.

        **Почему краул, а не потребитель.** Отказ источника (``payload_too_large``/
        ``unsupported_binary``/``not_found``) — факт получения: байтов по адресу нет, и
        потребителю нечего конвертировать. Транзиентные сбои (сеть, таймаут) сюда не
        входят: ранее добытая строка остаётся конвертируемой, её исход решает потребитель.
        """
        item = phase.items.get((pid, work_url))
        if terminal_failure_kind(item) is None:
            return
        phase.queue.close(
            pid,
            work_url,
            ExtractQueueState.SKIPPED,
            error_kind=item.error_kind if item is not None else None,
            error=item.error if item is not None else None,
        )

    def _commit_outcome(
        pid: str,
        work_url: str,
        depth: int | None,
        outcome: tuple[RawPage, Path, FetchPlan, bool] | None,
    ) -> None:
        """Пост-фетч коммит дренажа: retention-счётчики, пропуск, очередь, BFS.

        Конверсии здесь **нет** (F5): зонд артефакта, спуск в контейнер, извлечение,
        нарезка и запись корпуса — работа потребителя очереди
        (:func:`core.pipeline.extract_consume.consume`). Краул кончается durable-фактом
        «документ добыт», поэтому «краул выполнен» перестаёт означать «корпус собран».
        Ветка одна: раньше она называлась ``crawl_only`` и была режимом, теперь это
        единственный путь.

        Вызывается на главном потоке в FIFO-порядке; ``note_fetched`` — только здесь
        (жёсткий стоп ``max_documents`` не может быть превышен из воркеров пула).
        """
        if outcome is None:
            # Терминальный отказ источника закрывает строку очереди: конвертировать
            # нечего, а строка, оставшаяся ``pending``/``done``, пообещала бы потребителю
            # retained-файл, которого нет (``_close_refused_row``). Frontier от отказа не
            # расширяется — обход остаётся прежним.
            _close_refused_row(pid, work_url)
            return
        page, _raw_path, fetch_plan, freshly_fetched = outcome
        # 09 §6.3: ревалидация известного URL — depth-0 лист и **вне** бюджета
        # discovery. ``pages_fetched``/``truncated`` продолжают означать ровно
        # «документы, которые добавил обход этого корня».
        is_revalidation = frontier.is_revalidation(work_url)
        if freshly_fetched:
            if is_revalidation:
                # S1: ревалидационный трафик не смешивается с discovery-счётчиком
                # ``fetched``/``pages_fetched``, но обязан быть видимым.
                frontier.revalidation_fetched += 1
            else:
                frontier.note_fetched()
        # P5: post-fetch-пропуск документа при ``off`` (тип узнан только по Content-Type;
        # фетч был ⇒ страница уже учтена в ``max_documents``, элемент сеанса — FETCHED
        # из retention; ``FAILED`` не ставим — skip ≠ сбой, §2/§11.3). Байты ретейнены.
        if (
            level is DocumentLevel.OFF
            and document_content_type(page.content_type) is not None
        ):
            _record_document_skip(work_url, "content_type")
            operations.emit()
            return
        # 16 §3.3: происхождение наблюдения — из пути страницы этого прогона, а не из
        # ``url_state`` (там последняя классификация, в том числе из прошлого цикла).
        # ``freshly_fetched=False`` — единственный путь без сети (``resume_page``);
        # ``304`` — сервер ответил, версия не изменилась.
        reuse_kind = (
            ReuseKind.REUSED
            if not freshly_fetched
            else (
                ReuseKind.REVALIDATED
                if page.not_modified
                else ReuseKind.LIVE
            )
        )
        _enqueue_extract(pid, work_url, page, _raw_path, fetch_plan, reuse_kind)
        operations.emit()
        # D1: извлечение in-site ссылок и следующий уровень BFS — тот же код, что был
        # общим хвостом inline-пути: обход остаётся полным без конверсии.
        _extend_links(page, depth, is_revalidation)

    # D4: ограниченный пул ТОЛЬКО на сетевой fetch локального пути. Пул создаёт
    # **драйвер полосы** (``lane_width`` — одна формула на обоих), потому что полоса
    # живёт дольше одного корня: при чередовании одновременно существуют не более
    # ``host_concurrency`` пулов фетча, а не по пулу на корень. В пул идут лишь
    # work-items с живым HTTP-фетчем; браузерные элементы — никогда (см.
    # ``local_prefetch`` и ``_dispatch_wave``).
    max_workers = limits.max_workers
    pool = phase.fetch_pool
    non_poolable_pool = phase.browser_pool
    if lane_width(limits, executor=phase.executor) is not None and (
        pool is None or non_poolable_pool is None
    ):
        # Не тихая деградация: без пула живой фетч пошёл бы inline на главном потоке и
        # полосы схлопнулись бы в одну без единой ошибки в артефактах.
        raise AssertionError(
            f"{product.pid}/{root_url}: полоса без пула фетча при max_workers="
            f"{max_workers} — пул обязан создать драйвер"
        )

    def _await_disk() -> bool:
        """Жёсткий сторож свободного места носителя retention (решение владельца 01.11).

        Резерв — **объявленный** ``crawl.max_artifact_bytes``: самый крупный единственный
        объект, который краул пишет. Нового числа здесь нет, поэтому смена оси сдвигает
        порог, а не прячется за константой в коде.

        ``True`` — приём остановлен. Сторож фазовый: первая же нехватка останавливает
        приём до конца прогона, а не только этот корень; повторного замера после остановки
        нет намеренно — ожидание носитель не освобождает (``corpus_raw`` бессрочен, I19).
        Строку ``crawl_disk_headroom`` (одну на прогон, с числами и ``waves_blocked``)
        публикует фаза из записи сторожа, а не этот вызов: иначе счётчик волн пришлось бы
        править в уже опубликованной строке.
        """
        guard = phase.disk_guard
        if guard is None:
            return False
        report = guard.observe(
            phase.queue,
            reserve_bytes=limits.max_artifact_bytes,
            raw_root=phase.raw_root,
        )
        return bool(report.blocked)

    def _await_backlog() -> str | None:
        """F6 (§4.3): наблюдение бэклога конверсии и видимая запись ``crawl_backpressure``.

        Предел — объявленная ось ``crawl.extract_backlog_mb`` (app < run). Не объявлена ⇒
        предела нет: приём не останавливается, а ``backlog_bytes``/``free_bytes``
        публикуются потребителем и ``hound crawl status``.

        При достижении предела приём **ждёт** (ограничено ``EXTRACT_BACKLOG_WAIT_S``):
        иначе «краул без потребителя» копил бы гигабайты. Ожидание идёт на главном потоке
        драйвера намеренно — бэклог лежит на общем носителе фазы, поэтому пауза приёма
        фазовая, а не корневая: ждать на одной полосе, пока другие пишут, значило бы
        не остановить приём вовсе.

        Возвращает причину остановки приёма (``extract_backlog_full``) только когда
        ожидание исчерпано: сеанс закрывается ``partial`` (§4.3), а не притворяется
        полным. Исключений нет — остановка это исход прогона, а не сбой.
        """
        limit_mb = limits.extract_backlog_mb
        if limit_mb is None:
            return None
        event = wait_for_backlog(
            phase.queue,
            limit_mb=limit_mb,
            raw_root=phase.raw_root,
        )
        if event is None:
            return None
        phase.errors.append(_backpressure_line(event))
        return "extract_backlog_full" if event.gave_up else None

    # F4b: шаги корня — генератор. Отправку волны и ожидание исполняет драйвер
    # (:mod:`core.pipeline.corpus_driver`): «отправить и дождаться» не может быть
    # работой корня, потому что допуск волны (право на host'ы) решается до отправки.
    return (yield from root_steps(
        product.pid,
        root_url,
        frontier,
        limits=limits,
        items=phase.items,
        errors=phase.errors,
        emit=operations.emit,
        mark=operations.mark,
        commit_local=operations.commit_local,
        local_prefetch=operations.local_prefetch,
        process_local=operations.process_local,
        submit_collect=operations.submit_collect,
        work_item=operations.work_item,
        resolve_target=operations.resolve_target,
        max_attempts=phase.max_attempts,
        max_waves=phase.max_waves,
        wave_items=phase.wave_items,
        robots_enabled=robots_enabled,
        timings=phase.timings,
        executor=phase.executor,
        pool=pool,
        max_workers=max_workers,
        sitemap_skips=sitemap.skipped,
        _has_fetch_capacity=_has_fetch_capacity,
        _skip_document_prefetch=_skip_document_prefetch,
        _commit=_commit_outcome,
        _await_disk=_await_disk,
        _await_backlog=_await_backlog,
    ))
