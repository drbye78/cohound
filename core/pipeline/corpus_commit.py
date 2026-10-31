"""Фиксация исхода корня и жизненный цикл фазового пула экстракции.

Часть декомпозиции ``corpus.py`` (план ``docs/reports/2026-10-30-large-modules-refactoring-plan.md``,
L5 шаг 2). Здесь — то, что закрывает работу **после** того, как содержимое добыто:
провенанс корня, терминальное состояние сеанса, доводка хвоста экстракции и её пула.

**Почему отдельный модуль.** Эти шаги — единственное место, где фаза меняет **итоговые**
записи (``crawl_sessions``, ``truncation_records``, статус продукта) и где живёт
bounded-пересборка пула. Их порядок и есть наблюдаемое поведение; держать их рядом с
разбором страницы значило бы смешивать «что прочитали» и «что записали как итог».

**Границы.** Модуль импортирует :mod:`core.pipeline.corpus_ingest` (одно направление), но
не ``corpus``: обратный импорт дал бы цикл. Всё, что зависит от конкретного корня
(``frontier``, ``limits``, окно экстракции), приходит параметром — эти значения живут в
``_drain_root`` и остаются там до шага 3.

**Порядок записи не меняется.** Тела перенесены дословно; изменились только способы
получить то, что раньше было свободной переменной замыкания. Оракул —
``tests/pipeline/test_corpus_write_order.py``.
"""

from __future__ import annotations

import contextlib
import json
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.config.crawl_sources import CrawlSourcesConfig
from core.domain import CrawlSessionItem, FailureKind
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import looks_like_html
from core.pipeline.corpus_persist import _now_iso
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import CrawlLimits, Frontier
from core.pipeline.crawl_session import finalize_session
from core.pipeline.crawl_sources import crawl_sources_digest
from core.pipeline.extraction_runtime import (
    _EXTRACT_MAX_POOL_REBUILDS,
    _ExtractPoolKey,
    _ExtractPools,
)
from core.ports.types import FetchPlan, RawPage


@dataclass
class PhaseLifecycle:
    """Адрес durable-сеанса краула для обработчика прерывания в обёртке фазы.

    ``_build_corpus`` открывает сеанс внутри себя, а обработчик прерывания принадлежит
    обёртке :func:`build_corpus` (там же единственный ``pools.shutdown``). Без этого
    адреса прерванная фаза оставляла бы сеанс в ``running`` навсегда — ровно дефект
    «фантомных краулов в полёте» в ``hound crawl status``.

    ``completed`` ставится **только** после штатной финализации сеанса
    (:func:`core.pipeline.crawl_session.finalize_session`): любое исключение (включая
    ``KeyboardInterrupt``/``BaseException``) оставляет ``False``, и обёртка закрывает
    сеанс терминально (:func:`_finalize_interrupted_session`). Обратное тоже важно:
    исключение **после** успешной финализации не переписывает легитимный
    ``completed``/``partial`` на статус прерывания.

    Граница честности: ``SIGKILL``/OOM-killer не перехватывается ни одним Python-кодом,
    поэтому после него сеанс остаётся ``running``; это лечится recovery-on-open
    (:func:`core.pipeline.crawl_session.open_session`) при следующем запуске, а не
    обработчиком прерывания.
    """

    store: Any = None
    session_id: str | None = None
    completed: bool = False


@dataclass
class RootDrain:
    """C1: хвост экстракции одного seed-root в **фазовой** очереди доводки.

    Пул и FIFO-очередь экстракции живут в области фазы (:func:`build_corpus`), поэтому
    задание может быть доведено уже после возврата своего корня. ``pending`` — число
    недоведённых заданий корня; ``finalize`` — замыкание вызывающего, которое считает
    провенанс корня (``revalidation``/``truncation``) **после** доводки хвоста; оно
    вызывается ровно один раз (:func:`finalize_ready_roots`), в порядке корней.
    """

    pending: int = 0
    finalize: Callable[[], None] | None = None
    finalized: bool = False


@dataclass
class PendingExtract:
    """P4: отложенная экстракция (главный поток, FIFO-очередь доводки).

    Несёт всё, что нужно финишу: реконструированный ``RawPage``, ``raw_path`` (перечитывается
    воркером, I19), ``fetch_plan`` (ось ``extract``), флаг свежести (для ``note_fetched`` и
    учёта ёмкости с in-flight) и ``future`` пула. Мутаций состояния сеанса/корпуса здесь нет.

    C1: очередь — **фазовая**, поэтому задание несёт ``root`` (свой корень: фильтр ёмкости,
    учёт хвоста) и ``commit`` — замыкание ``_commit_outcome`` именно своего корня. Это
    позволяет доводить хвост корня A во время фетча корня B без потери провенанса.
    """

    pid: str
    url: str
    depth: int | None
    page: RawPage
    raw_path: Path
    fetch_plan: FetchPlan
    freshly_fetched: bool
    future: Any
    #: C1: корень задания (хвост/финализация провенанса).
    root: RootDrain
    #: C1: доводка исхода — ``(extracted, extract_error, failure_kind) → None``.
    commit: Callable[[Any, str | None, FailureKind | None], None]
    #: C1: ключ фазового пула ``(level, window)`` — учёт завершений/таймаутов и пересборка.
    pool_key: _ExtractPoolKey


def persist_partial_crawl_provenance(
    *,
    store: Any,
    session: str | None,
    errors: list[str],
    truncation_records: Sequence[Mapping[str, Any]],
) -> None:
    """Пишет наблюдение scope/усечения в ``crawl_sessions.provenance_json`` **по корню**.

    Почему не только в ``finalize_session``: живой прогон 22.10 умер через ~3 часа, и
    ``provenance_json`` остался ``{}`` — вся картина поддоменов (кто сколько забрал)
    пропала вместе с процессом, хотя именно она объясняла дефект. Поэтому провенанс
    корня персистится сразу после его drain, а не в конце фазы: упавший прогон всё
    равно отвечает числом «сколько дали поддомены».

    Форма совместима с финализацией (``effective_limits.roots`` — тот же канал, что
    читает :func:`core.pipeline.crawl_session.truncation_summary`);
    ``finalize_session`` позже перезаписывает его **полным** набором корней.
    ``partial: true``/``status: "running"`` помечают незавершённый сеанс, чтобы
    частичный провенанс не выглядел финальным.
    """
    if store is None or session is None:
        return
    try:
        store.set_crawl_session_provenance(
            session,
            json.dumps(
                {
                    "partial": True,
                    "status": "running",
                    "effective_limits": {"roots": truncation_records},
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
    except Exception as exc:  # noqa: BLE001 — наблюдаемость не валит фазу
        errors.append(f"crawl_provenance(partial): {type(exc).__name__}: {exc}")


def record_root_outcome(
    product_id: str,
    root_url: str,
    frontier: Frontier,
    sitemap_skips: Sequence[Mapping[str, Any]],
    *,
    counters: CorpusCounters,
    truncated_products: set[str],
    truncation_records: list[dict[str, Any]],
    store: Any,
    session: str | None,
    errors: list[str],
    revalidation: Mapping[str, Any] | None = None,
    record_slot: int | None = None,
) -> None:
    """Провенанс одного seed-root после drain: усечение/пропуски (D1/G, P5, D3/F10).

    Общий шаг авторских корней и известных URL инкрементального work-set: порядок
    записей и их содержимое не зависят от того, откуда пришёл корень.

    ``revalidation`` — аддитивная наблюдаемость ревалидации **в любом режиме**:
    ``{"selected": N, "fetched": M}`` (enqueued и фактически добытые) и, если
    операторский потолок отложил/отсёк URL, ``"deferred": K``/``"skipped": S``.
    ``None`` — у корня не было известных URL: ключ не появляется, payload
    ``state is None`` байт-в-байт прежний.

    ``record_slot`` (F4b) — слот, занятый при **планировании** корня: при чередовании
    корней запись приходит в порядке завершения, а артефакт манифеста
    (``effective_limits.roots``) обязан остаться авторским. ``None`` — слота нет
    (synthetic-бакеты, repair-пути): запись добавляется в конец, как прежде.
    """
    counters.pages_fetched_total += frontier.fetched
    counters.revalidation_fetched_total += frontier.revalidation_fetched
    if frontier.truncated:
        truncated_products.add(product_id)
    # P5: пропуск документов «прореживает» корпус продукта по построению — он
    # питает тот же канал аномалии, что и усечение (защита от false ❌ I2/I2b:
    # `source_scope: exhaustive` поверх «прореженного» продукта — аномалия
    # для человека). Причина — не ``max_*``, а ``document_level=off``.
    if frontier.skipped_documents:
        truncated_products.add(product_id)
    record: dict[str, Any] = {
        "product_id": product_id,
        "root": root_url,
        "pages_fetched": frontier.fetched,
        "truncated": frontier.truncated,
        "truncated_reason": frontier.truncated_reason,
        "limits": frontier.limits.as_dict(),
        "skipped_blacklisted": list(frontier.skipped_blacklisted),
        # P5: видимые пропуски документов (``url_suffix``/``content_type``).
        "skipped_documents": [dict(rec) for rec in frontier.skipped_documents],
    }
    # A2 (F3 плана 17): бюджет ``max_documents`` решается **на изъятии**, поэтому
    # ``truncated`` без числа читался бы как «дошли до конца». Очередь, оставшаяся
    # непустой при исчерпанном бюджете, публикуется числом — это цена объявленного
    # потолка, а не тихая остановка. Ключ появляется только у усечённого корня с
    # непустой очередью: записи остальных корней остаются байт-в-байт прежними.
    if frontier.truncated and len(frontier):
        record["queue_remaining"] = len(frontier)
    # 09 §6.3, дефект 2: схлопывание форм одного URL (`/page` и `/page/`) обязано быть
    # **видимым**, а не тихим: тихое отбрасывание URL — тот же класс дефекта, что тихий
    # обход path-prefix. Счётчик живёт в frontier, поэтому его видно только здесь —
    # без этой строки дедуп экономил бы бюджет молча, и «сколько сэкономили» нельзя
    # было бы прочитать из артефакта. Ключ появляется только при наличии дублей:
    # запись корня без них остаётся байт-в-байт прежней.
    if frontier.duplicate_form_count:
        record["duplicate_forms"] = frontier.duplicate_form_count
    # P6b: обход path-prefix терминальными артефактами (документ/заявленный контейнер
    # вне префикса корня) — счётчик + ограниченные примеры. Ключ появляется только при
    # наличии обхода: запись корня без артефактов остаётся байт-в-байт прежней.
    if frontier.artifacts_outside_prefix_count:
        record["artifacts_outside_prefix"] = {
            "count": frontier.artifacts_outside_prefix_count,
            "examples": list(frontier.artifacts_outside_prefix),
        }
    # C (execution_change): видимость поддоменов. ``out_of_scope_subdomains`` —
    # отвергнутые поддомены хоста корня (объявленных хостов не хватило): точный
    # счётчик по хостам + ограниченные примеры. ``accepted_foreign_hosts`` — сколько
    # принятых элементов корня пришло **не** с его собственного хоста (объявленный
    # поддомен, у которого нет своего корня). Вместе они отвечают числом на вопрос
    # «сколько дали поддомены» — и в живом прогоне, и в посмертном разборе.
    # Ключи появляются только при наличии данных: запись корня без поддоменов
    # остаётся байт-в-байт прежней.
    if frontier.out_of_scope_subdomains:
        record["out_of_scope_subdomains"] = {
            "count": sum(frontier.out_of_scope_subdomains.values()),
            "by_host": dict(sorted(frontier.out_of_scope_subdomains.items())),
            "examples": {
                host: list(urls)
                for host, urls in sorted(
                    frontier.out_of_scope_subdomain_examples.items()
                )
            },
        }
    if frontier.accepted_foreign_hosts:
        record["accepted_foreign_hosts"] = dict(
            sorted(frontier.accepted_foreign_hosts.items())
        )
    # D3/F10: отказы scope у ``<loc>`` sitemap — провенанс, не ``errors``.
    # Ключ появляется только при наличии отказов: без sitemap запись байт-в-байт
    # прежняя. Классы ``host``/``path`` раздельны (F6).
    if sitemap_skips:
        record["skipped_out_of_scope"] = [dict(skip) for skip in sitemap_skips]
    if revalidation is not None:
        record["revalidation"] = dict(revalidation)
    if record_slot is None or record_slot >= len(truncation_records):
        truncation_records.append(record)
    else:
        # Слот занят при планировании корня (F4b): перезаписываем заглушку **на месте**,
        # поэтому порядок записей — авторский, а не порядок завершения корней.
        truncation_records[record_slot] = record
    # Наблюдаемость не должна умирать вместе с процессом: провенанс корня
    # персистится сразу (см. ``persist_partial_crawl_provenance``).
    persist_partial_crawl_provenance(
        store=store,
        session=session,
        errors=errors,
        truncation_records=truncation_records,
    )


def finalize_ready_roots(*, extract_finalizers: deque[RootDrain]) -> None:
    """Вызывает финализаторы корней, чьи хвосты доведены, — строго в порядке корней."""
    while extract_finalizers and extract_finalizers[0].pending == 0:
        drain = extract_finalizers.popleft()
        finalize = drain.finalize
        if finalize is not None and not drain.finalized:
            drain.finalized = True
            finalize()


def recover_extract_pool(
    pool_key: _ExtractPoolKey,
    label: str,
    *,
    pools: _ExtractPools,
    errors: list[str],
) -> None:
    """Отравленный пул: discard + bounded rebuild; исчерпание — терминальный отказ.

    Повторное использование мёртвого пула запрещено; авто-ретрая нет. Inline после
    исчерпания не включается: на memory-constrained хосте docling в родителе — худший
    режим, а тихая деградация скрыла бы дефект.
    """
    if pools.rebuild(pool_key):
        return
    errors.append(
        f"{label}: пул экстракции {pool_key[0].value}/{pool_key[1].value}"
        f"/W={pool_key[2]} исчерпал "
        f"пересборки ({_EXTRACT_MAX_POOL_REBUILDS}) — остаток прогона терминально "
        "FAILED (parser_failed), без inline-экстракции"
    )


def abort_extract_pending(
    reason: str,
    *,
    extract_pending: deque[PendingExtract],
    extract_finalizers: deque[RootDrain],
) -> None:
    """Помечает все in-flight задания (все корни) ``FAILED`` — пул мёртв.

    Хвосты уже отложенных корней тоже принадлежат этому пулу, поэтому отказ получают
    все: исход видим, а не потерян вместе с пересборкой.
    """
    while extract_pending:
        item = extract_pending.popleft()
        item.commit(None, reason, None)
        item.root.pending -= 1
    finalize_ready_roots(extract_finalizers=extract_finalizers)


def finish_extract(
    item: PendingExtract,
    *,
    pools: _ExtractPools,
    errors: list[str],
    timings: _CorpusStageTimings,
    extract_timeout: float,
    extract_pending: deque[PendingExtract],
    extract_finalizers: deque[RootDrain],
) -> None:
    """Доводит oldest задание фазовой очереди: ``future.result`` → доводка корня (FIFO).

    P4/C1: ``BrokenProcessPool`` — воркер умер (краш/OOM): все in-flight → ``FAILED``,
    пул discard+rebuild (bounded) либо терминальный отказ. Таймаут — **per-artifact**:
    терминально помечается только это задание (``extraction_timeout``), пул продолжает
    работу; пересборка — только если пул не дал ни одного завершения за ``2 × timeout``
    (:meth:`_ExtractPools.note_timeout`).

    Наблюдаемость: время ``future.result()`` (включая таймаут и смерть воркера) идёт в
    ``timings.extract_wait_s`` — это **ожидание** главного потока, а не работа: при окне
    ``extract_workers > 1`` оно включает и чужие задания, поэтому окно публикуется рядом.
    """
    try:
        _wait_started = time.perf_counter()
        try:
            extracted = item.future.result(timeout=extract_timeout)
        finally:
            # ``extract_wait_s`` — **ожидание** экстракции на главном потоке (в т.ч. при
            # таймауте/сбое воркера). При окне > 1 включает ожидание чужих заданий:
            # рядом публикуется фактическое окно (``extract_workers``).
            timings.extract_wait_s += time.perf_counter() - _wait_started
    except TimeoutError:
        # C1: зависший артефакт инвалидируется точечно; пул (и его здоровые воркеры)
        # продолжает работу. ``cancel`` на уже исполняемом future — no-op.
        cancel = getattr(item.future, "cancel", None)
        if cancel is not None:
            # Инвалидация best-effort: задание всё равно коммитится как
            # ``extraction_timeout`` ниже, поэтому оператор видит исход артефакта
            # независимо от того, удалось ли отменить future.
            with contextlib.suppress(Exception):
                cancel()
        item.commit(None, "extraction timeout", FailureKind.EXTRACTION_TIMEOUT)
        if pools.note_timeout(item.pool_key):
            # ``2 × timeout`` без единого завершения: пул мёртв — reap + bounded rebuild.
            pools.terminate(item.pool_key)
            abort_extract_pending(
                "extraction pool terminated after timeout",
                extract_pending=extract_pending,
                extract_finalizers=extract_finalizers,
            )
            recover_extract_pool(
                item.pool_key,
                f"{item.pid}/{item.url}",
                pools=pools,
                errors=errors,
            )
    except BrokenProcessPool:
        item.commit(None, "extraction worker died", None)
        abort_extract_pending(
            "extraction worker died",
            extract_pending=extract_pending,
            extract_finalizers=extract_finalizers,
        )
        recover_extract_pool(
            item.pool_key,
            f"{item.pid}/{item.url}",
            pools=pools,
            errors=errors,
        )
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        item.commit(None, f"{type(exc).__name__}: {exc}", None)
    else:
        pools.note_completion(item.pool_key)
        item.commit(extracted, None, None)
    finally:
        item.root.pending -= 1
        finalize_ready_roots(extract_finalizers=extract_finalizers)


def finalize_crawl_session(
    *,
    session: str | None,
    counters: CorpusCounters,
    items: Mapping[tuple[str, str], CrawlSessionItem],
    store: Any,
    crawl_sources: CrawlSourcesConfig | None,
    truncated_products: set[str],
    truncation_records: list[dict[str, Any]],
    errors: list[str],
    lifecycle: PhaseLifecycle | None,
) -> None:
    """L1a: финализация сеанса (``COMPLETED`` без сбоев, иначе ``PARTIAL``).

    **Граница — краул, а не фаза.** Сеанс закрывается, когда дренированы все продукты
    (последний ``product_end``), а не когда закончилась фаза корпуса: эмбеддинг и сборка
    индекса — работа фазы, к сеансу краула отношения не имеющая. Пока
    ``finalize_session`` стоял после Pass B, сеанс висел ``running`` всё время
    эмбеддинга и ``stale_reason`` (heartbeat, порог 600 с) называл ложную причину у
    живого прогона: 2026-10-25 после ``crawl product end`` сеанс «протух» на 1280 с при
    505 % CPU.

    Инвариант :class:`PhaseLifecycle` сохранён: ``completed`` ставится **только** после
    штатной финализации. Исключение до неё оставляет ``False``, и обёртка закрывает
    сеанс терминально (:func:`_finalize_interrupted_session`); исключение после неё
    (эмбеддинг/сборка/композиция) легитимный ``completed``/``partial`` не переписывает.
    """
    if session is None or counters.session_finalized:
        return
    counters.session_finalized = True
    try:
        # Phase 2/2.1: провенанс сеанса — движки/виды источников из **персистенных
        # элементов** (``items.values()``: фактически применённый ``engine`` и ``kind``
        # элемента) + дайджест конфигурации источников. ``targets`` для агрегата не
        # годится: там лишь запрошенный движок (может быть ``None``).
        engines = sorted(
            {str(i.engine) for i in items.values() if i.engine}
        )
        kinds = sorted(
            {str(i.kind) for i in items.values() if i.kind}
        )
        digest = (
            crawl_sources_digest(crawl_sources)
            if crawl_sources is not None
            else None
        )
        finalize_session(
            store,
            session,
            finished_at=_now_iso(),
            engines=engines,
            kinds=kinds,
            crawl_sources_digest=digest,
            # D1/G: политико-усечённый краул не рапортуется ``COMPLETED`` (→ ``PARTIAL``);
            # эффективные лимиты/число добытых страниц/усечённые продукты — в провенанс.
            truncated=bool(truncated_products),
            effective_limits={"roots": truncation_records},
            pages_fetched=counters.pages_fetched_total,
            revalidation_fetched=counters.revalidation_fetched_total,
            truncated_products=sorted(truncated_products),
        )
    except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
        errors.append(f"finalize_session({session}): {type(exc).__name__}: {exc}")
    else:
        # Штатный финал: обёртке больше нечего закрывать (иначе ``finally``
        # перезаписал бы ``completed``/``partial`` на терминальный сбой).
        if lifecycle is not None:
            lifecycle.completed = True


def finalize_root(
    pid: str,
    root_url: str,
    frontier: Frontier,
    sitemap_skips: Sequence[Mapping[str, Any]],
    known_urls: Sequence[str],
    *,
    all_revalidation_groups: Mapping[str, Sequence[str]],
    cap_fired: bool,
    deferred_by_root: Mapping[str, int],
    skipped_by_root: Mapping[str, int],
    counters: CorpusCounters,
    truncated_products: set[str],
    truncation_records: list[dict[str, Any]],
    store: Any,
    session: str | None,
    errors: list[str],
    record_slot: int | None = None,
) -> None:
    """Провенанс корня после доводки его экстракционного хвоста (C1).

    Счётчики frontier (``fetched``/``revalidation_fetched``/усечение)
    меняются доводкой отложенных заданий, поэтому запись откладывается
    до момента, когда хвост корня доведён (:func:`finalize_ready_roots`).
    Порядок записей — порядок корней (FIFO финализаторов); ``record_slot`` (F4b)
    закрепляет за корнем слот, занятый при его планировании, поэтому при чередовании
    корней записи всё равно выходят в авторском порядке.
    """
    #: Наблюдаемость в любом режиме: ключ есть ровно у корня, который имел
    #: известные URL (даже если cap отложил все — ``selected=0``). У корня
    #: без известных URL ключа нет: ``state is None`` остаётся байт-в-байт.
    revalidation_info: dict[str, Any] | None = None
    if all_revalidation_groups.get(root_url):
        revalidation_info = {
            "selected": sum(
                1
                for url in known_urls
                if frontier.is_revalidation(url)
            ),
            # S1: фактическая ревалидация, не «выбрано»; discovery-счётчик
            # ``pages_fetched`` не смешивается с этим полем.
            "fetched": frontier.revalidation_fetched,
        }
        if cap_fired:
            revalidation_info["deferred"] = deferred_by_root.get(
                root_url, 0
            )
        if skipped_by_root.get(root_url):
            revalidation_info["skipped"] = skipped_by_root[root_url]
    record_root_outcome(
        pid,
        root_url,
        frontier,
        sitemap_skips,
        counters=counters,
        truncated_products=truncated_products,
        truncation_records=truncation_records,
        store=store,
        session=session,
        errors=errors,
        revalidation=revalidation_info,
        record_slot=record_slot,
    )


def inflight_fresh(
    *,
    extract_pending: Sequence[PendingExtract],
    frontier: Frontier,
    root_drain: RootDrain,
) -> int:
    """Свеже-добытые страницы **этого корня** «в полёте» (ёмкость с окном, P4/§3).

    Очередь экстракции — фазовая (C1), поэтому фильтр по ``item.root``: хвосты
    других корней не отнимают ёмкость у этого. Ревалидация исключена: она не
    расходует ``max_documents`` и не должна отнимать ёмкость у discovery.
    """
    return sum(
        1
        for item in extract_pending
        if item.root is root_drain
        and item.freshly_fetched
        and not frontier.is_revalidation(item.url)
    )


def has_fetch_capacity(
    *,
    frontier: Frontier,
    limits: CrawlLimits,
    extract_pending: Sequence[PendingExtract],
    root_drain: RootDrain,
) -> bool:
    """Ёмкость корня с учётом in-flight: ``max_documents − fetched − inflight``."""
    if limits.max_documents is None:
        return True
    return (
        limits.max_documents
        - frontier.fetched
        - inflight_fresh(
            extract_pending=extract_pending,
            frontier=frontier,
            root_drain=root_drain,
        )
        > 0
    )


def tail_may_extend_frontier(
    *,
    extract_pending: Sequence[PendingExtract],
    frontier: Frontier,
    limits: CrawlLimits,
    root_drain: RootDrain,
) -> bool:
    """Может ли хвост **этого** корня расширить его frontier (BFS-ссылки)?

    ``_commit_outcome`` предлагает исходящие ссылки только для не-ревалидационной
    HTML-страницы при ``max_depth > 0``. Если ни одно задание хвоста этого не
    может, доводка безопасна после возврата корня: BFS от её момента не зависит,
    а фетч следующего корня не ждёт экстракции (C1). Иначе хвост доводится здесь
    же — прежний порядок байт-в-байт.
    """
    if limits.max_depth <= 0:
        return False
    for item in extract_pending:
        if item.root is not root_drain:
            continue
        if frontier.is_revalidation(item.url):
            continue
        if looks_like_html(item.page):
            return True
    return False


def pending_count_for_root(
    *,
    extract_pending: Sequence[PendingExtract],
    root_drain: RootDrain,
) -> int:
    """Сколько заданий **этого** корня ждёт доводки в фазовой очереди (F4).

    Окно экстракции — свойство корня, а не общей очереди: ``len(extract_pending)``
    считает и чужие задания, поэтому корень ждал бы окна, которого не использовал, а
    окно памяти по корням подменялось бы максимумом. Пока полоса одна, в очереди лежат
    задания только текущего корня — число совпадает с прежним.
    """
    return sum(1 for item in extract_pending if item.root is root_drain)


def popleft_for_root(
    *,
    extract_pending: deque[PendingExtract],
    root_drain: RootDrain,
) -> PendingExtract:
    """Изъять самое старое задание **этого** корня (FIFO корня — инвариант).

    ``deque.popleft()`` берёт голову **общей** очереди, то есть при нескольких корнях —
    чужое задание: корень A доводил бы хвост B и переставал быть хозяином своего
    порядка. Порядок изъятия внутри корня не меняется — меняется только чьё задание
    берётся, поэтому при одной полосе поведение прежнее (F4: чистый перенос).
    """
    for index, item in enumerate(extract_pending):
        if item.root is root_drain:
            del extract_pending[index]
            return item
    raise AssertionError(
        "очередь доводки: у корня нет своих заданий — счётчик pending расходится "
        "с очередью"
    )


def drain_for_window(
    *,
    extract_pending: deque[PendingExtract],
    extract_window: int | None,
    frontier: Frontier,
    limits: CrawlLimits,
    root_drain: RootDrain,
    pools: _ExtractPools,
    errors: list[str],
    timings: _CorpusStageTimings,
    extract_timeout: float,
    extract_finalizers: deque[RootDrain],
) -> None:
    """Доводит oldest **своего** корня, пока окно полно или ёмкость исчерпана.

    ``own`` — задания этого корня: окно считается по ним, а не по длине общей очереди
    (F4: у каждого корня своё окно; пул экстракции при этом остаётся фазовым).
    """
    own = pending_count_for_root(
        extract_pending=extract_pending, root_drain=root_drain
    )
    while own and (
        own >= extract_window
        or not has_fetch_capacity(
            frontier=frontier,
            limits=limits,
            extract_pending=extract_pending,
            root_drain=root_drain,
        )
    ):
        finish_extract(
            popleft_for_root(
                extract_pending=extract_pending, root_drain=root_drain
            ),
            pools=pools,
            errors=errors,
            timings=timings,
            extract_timeout=extract_timeout,
            extract_pending=extract_pending,
            extract_finalizers=extract_finalizers,
        )
        own = pending_count_for_root(
            extract_pending=extract_pending, root_drain=root_drain
        )


