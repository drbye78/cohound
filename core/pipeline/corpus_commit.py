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
(``frontier``, ``limits``), приходит параметром — эти значения живут в корневом цикле и
остаются там до шага 3.

**Порядок записи не меняется.** Тела перенесены дословно; изменились только способы
получить то, что раньше было свободной переменной замыкания. Оракул —
``tests/pipeline/test_corpus_write_order.py``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from core.config.crawl_sources import CrawlSourcesConfig
from core.domain import CrawlSessionItem
from core.pipeline.corpus_persist import _now_iso
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import Frontier
from core.pipeline.crawl_session import finalize_session
from core.pipeline.crawl_sources import crawl_sources_digest


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
    """Провенанс корня: итоги обхода, записанные на его слот (C1/F4b).

    Счётчики frontier (``fetched``/``revalidation_fetched``/усечение) окончательны к
    моменту, когда корень завершён: с F5 (плана ``17``) краул не конвертирует вовсе, поэтому
    «хвоста экстракции», который мог бы дозреть позже, у корня больше нет — конверсия идёт
    отдельным проходом по durable-очереди. ``record_slot`` (F4b) закрепляет за корнем слот,
    занятый при его планировании, поэтому при чередовании корней записи выходят в авторском
    порядке, а не в порядке завершения.
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
