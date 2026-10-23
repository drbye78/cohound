"""Производство машиночитаемого протокола исчерпания для strict-``❓`` (I3, §5/S13).

``❓`` = «протокол исчерпан, evidence недостаточно». До этого модуля протокол существовал
как **тип** (``ProtocolExhaustion``), таблица (``search_exhaustion``) и писатель
(``StorePort.save_exhaustion``), но производителя не было: ``❓`` от оценщика всегда
оставался без строки, и отчёт честно (но бесполезно) помечал каждый такой случай
аномалией «нет протокола».

**Главное правило модуля.** Протокол называет **только тот контур, который реально
исполнился**. Неполный протокол — правильный сигнал «``❓`` ещё не обоснован», а не
дефект, который надо заглушить. Поэтому «не смог дешёво установить факт» трактуется
как «контур не исполнялся»: занижение честно, завышение фабрикует доказательство.
``report.collect_i3_anomalies`` при этом остаётся наблюдением (``❓`` не меняет статус,
I12) — модуль не «лечит» аномалии, а даёт им настоящий предмет.

Терминация — только из :class:`~core.domain.enums.TerminationReason`:

* ``budget_exhausted`` — бюджет **действительно** исчерпан (факт, не догадка);
* ``not_found`` — ни один исполненный контур ничего не нашёл (в т.ч. вакуумный случай:
  ни один контур не исполнялся — тогда это видно по пустому ``contours``);
* ``results_exhausted`` — исполненные контуры дали результаты, но evidence недостаточно;
* ``error`` — передан вызывающим как факт сбоя контура;
* ``sufficient_evidence`` — **запрещён** для ``❓``: эта причина значит «нашли
  достаточно», и ``❓`` с ней противоречил бы сам себе.

Модуль категорийно нейтрален (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from core.domain.enums import Status, TerminationReason
from core.domain.invariants import REQUIRED_EXHAUSTION_CONTOURS
from core.domain.models import Cell, ProtocolExhaustion

__all__ = [
    "ExhaustionFacts",
    "build_protocols",
    "contours_run",
    "protocol_map",
]

#: Четыре контура полного протокола (I3) — единый источник в ``invariants``.
CONTOURS: tuple[str, ...] = REQUIRED_EXHAUSTION_CONTOURS


@dataclass(frozen=True)
class ExhaustionFacts:
    """Наблюдённые факты прогона, из которых строится протокол (передаёт ``phases``).

    Поля намеренно «сырые»: это не намерение, а наблюдение. Всё, что вызывающий не
    наблюдал, остаётся пустым — и тогда соответствующий контур/раунд/бюджет в протокол
    не попадает.
    """

    #: ``product_id → контуры, которые реально исполнились`` (:func:`contours_run`).
    contours_run: Mapping[str, frozenset[str]] = field(default_factory=dict)
    #: ``product_id → число фактических SERP-раундов`` (0, если SERP не исполнялся).
    rounds_by_product: Mapping[str, int] = field(default_factory=dict)
    #: ``product_id → фактические счётчики/бюджет ``SearchEffort```` (пусто — не тратили).
    budget_by_product: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    #: ``product_id → запросы, которые были выпущены`` (пусто — запросов не было).
    queries_by_product: Mapping[str, Sequence[str]] = field(default_factory=dict)
    #: ``product_id → дал ли исполненный контур хоть что-то`` (``None`` — неизвестно).
    yielded_by_product: Mapping[str, bool] = field(default_factory=dict)
    #: Явная причина терминации как **факт** (сбой контура и т.п.); иначе выводится.
    termination_by_product: Mapping[str, TerminationReason] = field(default_factory=dict)
    #: Метка времени исчерпания (UTC ISO); ``None`` — не фиксируем (детерминизм).
    exhausted_at: str | None = None


def contours_run(
    products: Iterable[str],
    *,
    corpus: Iterable[str] = (),
    serp: Iterable[str] = (),
    registry: Iterable[str] = (),
    inheritance: Iterable[str] = (),
) -> dict[str, frozenset[str]]:
    """Per product — какие из четырёх контуров **фактически исполнились** (I3).

    Каждый аргумент — множество продуктов, для которых соответствующий контур
    действительно работал (по наблюдённым фактам, не по конфигурации). Продукты вне
    ``products`` отбрасываются: протокол существует только для продуктов плана.
    """
    observed = {
        "corpus": {str(item) for item in corpus},
        "serp": {str(item) for item in serp},
        "registry": {str(item) for item in registry},
        "inheritance": {str(item) for item in inheritance},
    }
    return {
        str(product): frozenset(
            name for name, members in observed.items() if str(product) in members
        )
        for product in products
    }


def protocol_map(
    protocols: Iterable[ProtocolExhaustion],
) -> dict[tuple[str, str], ProtocolExhaustion]:
    """Индексирует протоколы по ``(product_id, param_id)`` — форма входа отчёта."""
    return {
        (protocol.product_id, protocol.param_id): protocol for protocol in protocols
    }


def _termination_for(
    *,
    product_id: str,
    ran: frozenset[str],
    budget: Mapping[str, Any],
    yielded: bool | None,
    override: TerminationReason | str | None,
) -> TerminationReason:
    """Причина терминации для ``❓`` — только реальная, без ``sufficient_evidence``.

    :raises ValueError: вызывающий передал ``sufficient_evidence`` для ``❓``.
    """
    if override is not None:
        reason = TerminationReason(str(override))
        if reason is TerminationReason.SUFFICIENT_EVIDENCE:
            raise ValueError(
                "I3: sufficient_evidence противоречит ❓ "
                f"({product_id}: evidence достаточно — статус был бы не ❓)"
            )
        return reason
    # Бюджет — факт, а не вывод: «остаток 0» без явного признака исчерпания не считается.
    if budget.get("exhausted") is True:
        return TerminationReason.BUDGET_EXHAUSTED
    if not ran:
        # Ни один контур не исполнился: «ничего не найдено» — буквально верно; пустой
        # ``contours`` в самой строке и есть честная запись этого состояния.
        return TerminationReason.NOT_FOUND
    if yielded is False:
        return TerminationReason.NOT_FOUND
    return TerminationReason.RESULTS_EXHAUSTED


def build_protocols(
    cells: Iterable[Cell],
    *,
    run_id: str,
    contours_run: Mapping[str, frozenset[str]],
    rounds_by_product: Mapping[str, int] | None = None,
    budget: Mapping[str, Mapping[str, Any]] | None = None,
    termination_by_product: Mapping[str, TerminationReason | str] | None = None,
    queries_by_product: Mapping[str, Sequence[str]] | None = None,
    yielded_by_product: Mapping[str, bool] | None = None,
    exhausted_at: str | None = None,
) -> list[ProtocolExhaustion]:
    """Одна строка протокола на ``(product_id, param_id)`` со статусом ``❓``.

    ``contours`` — ровно исполненные контуры (отсортированы для детерминизма);
    ``rounds``/``budget``/``queries`` — факты SERP-контура (нули/пусто, если его не было).
    Порядок строк детерминирован: ``(product_id, param_id)``.
    """
    rounds_by_product = rounds_by_product or {}
    budget = budget or {}
    termination_by_product = termination_by_product or {}
    queries_by_product = queries_by_product or {}
    yielded_by_product = yielded_by_product or {}

    rows: dict[tuple[str, str], ProtocolExhaustion] = {}
    for cell in cells:
        if getattr(cell, "status", None) is not Status.UNKNOWN:
            continue
        product_id = str(cell.product_id)
        param_id = str(cell.param_id)
        ran = frozenset(contours_run.get(product_id, frozenset()))
        rows[(product_id, param_id)] = ProtocolExhaustion(
            run_id=run_id,
            product_id=product_id,
            param_id=param_id,
            contours=sorted(ran),
            queries=list(queries_by_product.get(product_id, ())),
            rounds=int(rounds_by_product.get(product_id, 0)),
            budget=dict(budget.get(product_id, {})),
            termination_reason=_termination_for(
                product_id=product_id,
                ran=ran,
                budget=budget.get(product_id, {}),
                yielded=yielded_by_product.get(product_id),
                override=termination_by_product.get(product_id),
            ),
            exhausted_at=exhausted_at,
        )
    return [rows[key] for key in sorted(rows)]
