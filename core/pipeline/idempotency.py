"""Инвалидация и идемпотентность конвейера (T4.4, S8/R15).

Повторный прогон (resume/повторный Ш6) **не** должен плодить дубли. Достигается
детерминированными ключами upsert:

* **Ячейки** — PK ``(run_id, product_id, param_id)``: ``StorePort.save_cell(s)`` делает
  ``INSERT OR REPLACE``, поэтому повторная запись обновляет ту же строку.
* **Evidence** — PK ``evidence_id = H(run_id, product_id, param_id, source_id, source_kind,
  seq)`` (S8): один и тот же кусок доказательства при resume получает тот же id и
  перезаписывается, а не дублируется.
* **Решения гейтов** — ``decision_id`` в ``gate_decisions`` (см. ``gates.py``).

Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Any

from core.domain import Cell, StaleReason, derived_evidence_id
from core.ports.store import StorePort

__all__ = [
    "evidence_id_for",
    "mark_stale",
    "upsert_cells",
    "upsert_evidence",
]


def evidence_id_for(
    run_id: str,
    product_id: str,
    param_id: str,
    source_id: str,
    source_kind: Any,
    seq: int,
) -> str:
    """Детерминированный ``evidence_id`` (S8) — тонкая обёртка над доменом.

    Повторный Ш6/resume на тех же входах даёт тот же id → upsert по PK ``cell_evidence``,
    а не новая строка. ``source_kind`` принимается как enum или строка; для стабильности
    хеша берётся ``.value`` (строка сериализуется как есть).
    """
    kind = getattr(source_kind, "value", source_kind)
    return derived_evidence_id(
        run_id, product_id, param_id, source_id, str(kind), seq
    )


def mark_stale(cell: Cell, reason: StaleReason) -> Cell:
    """Помечает ячейку ``stale_reason`` (R15) и возвращает обновлённую копию.

    Не мутирует вход: возвращается ``model_copy`` с новым ``stale_reason``. Stale-ячейки
    исключаются из расчёта до переоценки/ревью (R15).
    """
    return cell.model_copy(update={"stale_reason": reason})


def upsert_cells(store: StorePort, cells: list[Cell]) -> int:
    """Идемпотентно пишет ячейки пачкой (upsert по PK ``(run_id, product_id, param_id)``).

    Возвращает число переданных ячеек. Повторный вызов с тем же набором не создаёт новых
    строк: ``INSERT OR REPLACE`` перезаписывает существующие по PK. Батчевая
    транзакционность — на стороне ``StorePort`` (§B.2).
    """
    items = list(cells)
    if items:
        store.save_cells_batch(items)
    return len(items)


def upsert_evidence(store: StorePort, evs: list[Any]) -> int:
    """Идемпотентно пишет evidence пачкой (upsert по ``evidence_id``, S8).

    Возвращает число переданных объектов. ``evidence_id`` должен быть построен через
    :func:`evidence_id_for`, иначе идемпотентность не гарантируется.
    """
    items = list(evs)
    if items:
        store.add_evidence_batch(items)
    return len(items)
