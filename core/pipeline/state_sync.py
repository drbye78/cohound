"""coHound — запись durable per-product состояния по итогам прогона (09 §4.4/§5.1).

Модуль — **писатель** стороны state-контура: ``resolve_product_modes`` только читает
рубежи, поэтому без этих вызовов ``state.db`` остаётся инертным и каждый продукт навсегда
остаётся ``new``.

Два шага, оба идемпотентны:

* :func:`sync_product_state` — материализует готовность продукта (``product_state``) из
  onboarding-плана: готовый продукт получает строку ``ready`` (или обновляет только
  готовность/причину), неготовый — ``pending_seeds`` с первой машинной причиной;
  ``last_*_run_id``/``added_*`` существующей строки **не** затираются.
* :func:`record_run_outcome` — по итогам прогона пишет ``product_snapshot`` на продукт,
  двигает рубежи ``last_corpus_run_id``/``last_evaluated_run_id``/``last_completed_run_id``
  и пишет ``run_lineage``.

Границы честности: счётчики и статусы выводятся только из того, что реально лежит в БД
прогона (``corpus_coverage`` + ячейки). Чего в этом срезе нет — не выдумывается: carry-forward
отсутствует (``cells_carried = 0``), ``docs_new``/``docs_reused``/``partition_build_id`` не
заполняются, ``snapshot_digest`` продукта не трогается.

Слой ``pipeline``: зависимости — ``domain``/``ports``/``util`` (store-порт приходит
аргументом, адаптеры не импортируются). Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
from collections.abc import Iterable, Sequence
from typing import Any

from core.domain.enums import CellState, OnboardingState, ProductScope
from core.domain.models import CorpusCoverage, ProductPlan, SeedsConfig
from core.ports.state import (
    ProductStatePort,
    ProductStateRow,
    RunLineageRow,
    SnapshotRow,
)
from core.ports.store import StorePort
from core.util.hashing import short_hash

__all__ = ["record_run_outcome", "sync_product_state"]

#: Статус снимка: корпус полон (coverage.complete).
SNAPSHOT_COMPLETE = "complete"
#: Статус снимка: покрытие есть, но неполно (усечение/нечитаемые origin'ы/сбои).
SNAPSHOT_PARTIAL = "partial"
#: Статус снимка: покрытия нет вовсе (прогон не дошёл до корпуса).
SNAPSHOT_FAILED = "failed"

#: Разрешённые состояния ячейки (§5.1): вердикт получен или подтверждён человеком.
_RESOLVED_CELL_STATES = frozenset({CellState.EVALUATED, CellState.REVIEWED})

#: Статус прогона (``RunReport.status``/``PhaseStatus``) → статус lineage §4.2.
_LINEAGE_STATUS = {
    "ok": "complete",
    "blocked": "partial",
    "failed": "failed",
    "skipped": "partial",
}


def _today_iso(today: _dt.date | None) -> str:
    """Дата (ISO) для ``added_at``; ``None`` — сегодняшняя."""
    return (today or _dt.date.today()).isoformat()


def _blocked_reason(plan: ProductPlan, seeds: SeedsConfig | None) -> str | None:
    """Первая машинная причина неготовности (``no_seeds`` — только если seed'ов нет)."""
    if plan.reasons:
        return plan.reasons[0]
    if seeds is not None and not seeds.enabled_for(plan.product_id):
        return "no_seeds"
    return None


def sync_product_state(
    state: ProductStatePort,
    *,
    products: Iterable[str],
    onboarding_plans: Sequence[ProductPlan],
    seeds: SeedsConfig | None = None,
    actor: str = "run",
    today: _dt.date | None = None,
) -> None:
    """Материализует готовность продуктов в ``product_state`` (09 §5.3).

    Для готового продукта (``onboarding_state=ready``, причин нет): строки нет — создаётся
    со ``state=ready``, ``canonical_name=product_id`` (отображаемое имя появится в
    следующем срезе), ``added_at`` = ``today``, ``added_by`` = ``actor``; строка есть —
    обновляются **только** ``onboarding_state``/``onboarding_reason`` (рубежи
    ``last_*_run_id`` и провенанс ``added_*`` не затираются).

    Для неготового продукта пишется ``onboarding_state=pending_seeds`` и
    ``onboarding_reason`` = первая причина плана (fallback — ``no_seeds``, если активных
    seed'ов нет). Идемпотентно: повторный вызов с теми же планами — no-op по смыслу.

    :param products: ``product_id`` когорты (детерминированно сортируются).
    :param onboarding_plans: планы префлайта; продукты вне планов не трогаются.
    :param seeds: эффективный авторский seed-набор (только для fallback-причины).
    """
    plans = {plan.product_id: plan for plan in onboarding_plans}
    added_at = _today_iso(today)
    for product_id in sorted({str(pid) for pid in products}):
        plan = plans.get(product_id)
        if plan is None:
            continue
        existing = state.get_product_state(product_id)
        ready = plan.onboarding_state is OnboardingState.READY and not plan.reasons
        if ready:
            onboarding_state = OnboardingState.READY
            reason: str | None = None
        else:
            onboarding_state = OnboardingState.PENDING_SEEDS
            reason = _blocked_reason(plan, seeds)
        if existing is None:
            state.upsert_product_state(
                ProductStateRow(
                    product_id=product_id,
                    canonical_name=product_id,
                    onboarding_state=onboarding_state,
                    onboarding_reason=reason,
                    added_at=added_at,
                    added_by=actor,
                )
            )
            continue
        state.upsert_product_state(
            dataclasses.replace(
                existing,
                onboarding_state=onboarding_state,
                onboarding_reason=reason,
            )
        )


def _coverage_digest(coverage: CorpusCoverage | None) -> str | None:
    """Стабильный дайджест семантики покрытия (без ``computed_at``).

    ``computed_at`` — момент вычисления, а не факт: два прогона с одинаковым покрытием
    обязаны давать одинаковый дайджест, иначе carry-forward/attestation-сверка (§4.2/§10)
    ломались бы на каждом пересчёте. Берутся только семантические поля:
    ``complete``/``expected_sources``/``observed_sources``/``truncated``/``reason``.
    """
    if coverage is None:
        return None
    payload = {
        "complete": coverage.complete,
        "expected_sources": coverage.expected_sources,
        "observed_sources": coverage.observed_sources,
        "truncated": coverage.truncated,
        "reason": coverage.reason,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return short_hash(canonical, 24)


def _cell_stats(store: StorePort, run_id: str) -> dict[str, tuple[int, int]]:
    """``product_id → (всего ячеек, разрешено)`` по durable-ячейкам прогона.

    Разрешённая ячейка — ``evaluated``/``reviewed`` (§5.1); ``pending``/``preliminary``/
    ``failed`` остаются неразрешёнными. ``N/A`` — статус разрешённой ячейки, поэтому
    отдельной ветки не требует.
    """
    stats: dict[str, list[int]] = {}
    for cell in store.iter_cells(run_id):
        bucket = stats.setdefault(cell.product_id, [0, 0])
        bucket[0] += 1
        if cell.state in _RESOLVED_CELL_STATES:
            bucket[1] += 1
    return {
        product_id: (total, resolved) for product_id, (total, resolved) in stats.items()
    }


def _snapshot_status(coverage: CorpusCoverage | None) -> str:
    """Статус снимка по покрытию корпуса: complete / partial / failed."""
    if coverage is None:
        return SNAPSHOT_FAILED
    return SNAPSHOT_COMPLETE if coverage.complete else SNAPSHOT_PARTIAL


def record_run_outcome(
    state: ProductStatePort,
    *,
    run_id: str,
    package_id: str,
    cycle_id: str,
    products: Iterable[str],
    store: StorePort,
    status: str,
    started_at: str,
    completed_at: str | None,
    previous_run_id: str | None = None,
    cohort_digest: str | None = None,
    seeds_digest: str | None = None,
    execution_contract_digest: str | None = None,
    change_class: str = "none",
    scope: str = ProductScope.FULL.value,
    refresh_policy: str = "auto",
    evaluate_ok: bool = False,
) -> None:
    """Фиксирует итог прогона в durable state: снимки, рубежи, lineage (09 §4.4/§5.1).

    На продукт:

    * ``product_snapshot`` — ``status`` = ``complete`` при ``coverage.complete``,
      ``partial`` при наличии покрытия, ``failed`` без покрытия; ``coverage_digest`` —
      :func:`_coverage_digest`; ``docs_total`` — ``coverage.observed_sources``; счётчики
      ячеек — из durable-ячеек прогона; ``cells_carried = 0`` (carry-forward не
      реализован), ``docs_new``/``docs_reused``/``partition_build_id`` не выдумываются;
    * ``last_corpus_run_id`` — при наличии строки покрытия;
    * ``last_evaluated_run_id`` — если фаза оценки прошла (``evaluate_ok``) и покрытие есть;
    * ``last_completed_run_id``/``last_completed_cycle_id`` — только для ``scope=full``,
      если ячейки есть и **все** разрешены (``evaluated``/``reviewed``/``N/A``) и фаза
      оценки прошла; иначе рубеж не двигается (не «достраивается» догадкой);
    * ``last_attempt_run_id``/``last_attempt_status`` — всегда (это попытка, не рубеж).

    Затем пишется ``run_lineage`` (статус прогона отображается в словарь §4.2:
    ``ok→complete``, ``blocked/skipped→partial``, ``failed→failed``).

    :param status: статус прогона из ``RunReport`` (``ok``/``blocked``/``failed``/``skipped``).
    :param evaluate_ok: фаза ``evaluate`` завершилась ``ok`` (иначе eval не состоялся).
    :param scope: ``full``|``product_only`` (§6.2/§13.6); ``product_only`` не двигает
        ``last_completed_run_id`` — baseline остаётся от полного прогона.
    :param refresh_policy: политика наблюдения прогона (``auto``|…); пишется в lineage.
    :param change_class: класс изменения относительно предыдущего прогона (§12.1).
    """
    stats = _cell_stats(store, run_id)
    product_ids = sorted({str(pid) for pid in products})
    for product_id in product_ids:
        coverage = store.read_corpus_coverage(run_id, product_id)
        coverage_digest = _coverage_digest(coverage)
        cells_total, cells_resolved = stats.get(product_id, (0, 0))
        cells_pending = cells_total - cells_resolved
        baseline_complete = bool(
            scope == ProductScope.FULL.value
            and evaluate_ok
            and coverage is not None
            and cells_total > 0
            and cells_pending == 0
        )
        state.record_snapshot(
            SnapshotRow(
                snapshot_id=short_hash(f"{run_id}\x1f{product_id}", 24),
                product_id=product_id,
                cycle_id=cycle_id,
                run_id=run_id,
                status=_snapshot_status(coverage),
                scope=scope,
                coverage_complete=bool(coverage is not None and coverage.complete),
                coverage_digest=coverage_digest,
                docs_total=coverage.observed_sources if coverage is not None else 0,
                cells_total=cells_total,
                cells_measured=cells_resolved,
                cells_pending=cells_pending,
                baseline_complete=baseline_complete,
                created_at=completed_at or started_at,
            )
        )

        existing = state.get_product_state(product_id)
        if existing is None:
            existing = ProductStateRow(
                product_id=product_id,
                canonical_name=product_id,
                onboarding_state=OnboardingState.READY,
                added_at=started_at,
                added_by="run",
            )
        updates: dict[str, Any] = {
            "last_attempt_run_id": run_id,
            "last_attempt_status": status,
        }
        if coverage is not None:
            updates["last_corpus_run_id"] = run_id
            if evaluate_ok:
                updates["last_evaluated_run_id"] = run_id
        if baseline_complete:
            updates["last_completed_run_id"] = run_id
            updates["last_completed_cycle_id"] = cycle_id
        state.upsert_product_state(dataclasses.replace(existing, **updates))

    state.record_run_lineage(
        RunLineageRow(
            run_id=run_id,
            package_id=package_id,
            cycle_id=cycle_id,
            previous_run_id=previous_run_id,
            # NOT NULL-колонки: пустая строка остаётся только для вызовов без
            # дайджестов (старые тесты/ручные записи), а не выдуманным значением.
            cohort_digest=cohort_digest or "",
            seeds_digest=seeds_digest or "",
            execution_contract_digest=execution_contract_digest or "",
            change_class=change_class,
            status=_LINEAGE_STATUS.get(status, "partial"),
            started_at=started_at,
            completed_at=completed_at,
            products=product_ids,
            refresh_policy=refresh_policy,
        )
    )
