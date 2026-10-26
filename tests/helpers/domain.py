"""Доменные билдеры тестов (валидные ячейки и т.п.).

Объединяет идентичные копии ``_cell`` из CLI-тестов. Билдеры повторяют ровно прежнюю
семантику и не претендуют на универсальность: доменные варианты с другой сигнатурой
(store/pipeline) остаются локальными в своих тест-файлах.
"""

from __future__ import annotations

from core.domain.enums import CellState, Status
from core.domain.models import Cell, EvidenceScope, LLMEvidence

__all__ = ["cell"]


def cell(run_id: str, product_id: str, param_id: str, status: Status) -> Cell:
    """Собирает валидную ячейку под инварианты I1/I2."""
    if status in {Status.OK, Status.PARTIAL}:
        return Cell(
            run_id=run_id,
            product_id=product_id,
            param_id=param_id,
            state=CellState.EVALUATED,
            status=status,
            evidence=[LLMEvidence(source_id="src.1", quote="q")],
        )
    if status is Status.ABSENT:
        # Вариант 2: публикуемое «нет» обязано нести класс силы отрицания (I2).
        return Cell(
            run_id=run_id,
            product_id=product_id,
            param_id=param_id,
            state=CellState.EVALUATED,
            status=status,
            evidence_scope=EvidenceScope.for_registry_absence(registry_ids=["reg"]),
        )
    return Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=status,
    )
