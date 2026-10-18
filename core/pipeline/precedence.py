"""coHound — прецедент источника (П>В, §2.2 п.7/п.8): общий helper Ш4/Ш6.

Единая реализация правила «ячейку более высокого источника нельзя молча заменить
ячейкой более низкого» (§2.2 п.7: реестры → наследование → дистрибутив → корпус → web;
§2.2 п.8: противоречащее свидетельство понижает до ревью, а не перезаписывает).

Семантика (таблица §2 гейта P6), для пары «высокая ячейка ↔ низкая ячейка»:

* совпадение наличия-статуса → высокая ячейка остаётся как есть (``kept_high=True``);
* низкая ячейка ``inconclusive`` (``status is None`` — ``failed``/``preliminary``,
  либо ``❓``) → высокая ячейка остаётся как есть (I4/I12/I13: неуверенность не является
  контр-свидетельством);
* расхождение наличия-статусов → ``pending``-кандидат ревью: копия низкой ячейки с
  ``state=pending``, ``source_kind`` высокого источника и ``review_reason``; автоматически
  ничего не разрешается (I6/I7). ``status`` кандидата — **предложенный** низким
  источником (advisory), как в историческом ``apply_precedence``.

Модуль нейтрален (§1.3), без I/O и без зависимостей от store-реализации.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.domain import Cell, CellState, SourceKind, Status

__all__ = [
    "CONDITIONAL_REVIEW_REASON",
    "PRECEDENCE_REVIEW_REASON",
    "PrecedenceResult",
    "apply_precedence",
    "collect_protected_cells",
    "is_inconclusive",
    "resolve_precedence",
    "source_rank",
]

#: Свободнотекстовая аннотация причины ``pending`` (не статус и не новое перечисление):
#: конфликт источников, ожидающий решения человека.
PRECEDENCE_REVIEW_REASON = "precedence_conflict"

#: Причина ``pending`` для ``conditional``-наследования (латентный гэп очереди ревью).
CONDITIONAL_REVIEW_REASON = "conditional_inheritance"

#: Порядок силы источников (§2.2 п.7): реестр > наследование > дистрибутив > корпус > web.
#: ``manual`` — человеческий ввод, выше автоматических источников; ``None`` — вне порядка.
_SOURCE_RANK: dict[SourceKind, int] = {
    SourceKind.MANUAL: 5,
    SourceKind.REGISTRY: 4,
    SourceKind.INHERITANCE: 3,
    SourceKind.DISTRIBUTION: 2,
    SourceKind.DOCUMENTATION: 1,
    SourceKind.WEB: 0,
}

#: Состояния, в которых ячейка высокого источника уже несёт решение (включая ``pending``
#: конфликта: повторный Ш6 не должен «оживлять» её в корпусную).
_PROTECTED_STATES: frozenset[CellState] = frozenset(
    {CellState.EVALUATED, CellState.PENDING, CellState.REVIEWED}
)


def source_rank(kind: SourceKind | None) -> int:
    """Место источника в порядке §2.2 п.7 (больше — сильнее; ``None`` → ``-1``)."""
    if kind is None:
        return -1
    return _SOURCE_RANK.get(kind, -1)


def is_inconclusive(cell: Cell) -> bool:
    """Неуверенный результат: не статус (``failed``/``preliminary``) либо ``❓``."""
    return cell.status is None or cell.status is Status.UNKNOWN


@dataclass(frozen=True)
class PrecedenceResult:
    """Итог наложения источников.

    :param cell: ячейка, которую следует считать итоговой (высокая либо pending-кандидат).
    :param kept_high: ``True`` — высокая ячейка остаётся без записи; ``False`` — ``cell``
        является pending-кандидатом и подлежит персистенции.
    """

    cell: Cell
    kept_high: bool


def resolve_precedence(
    high_cell: Cell,
    low_cell: Cell,
    *,
    reason: str = PRECEDENCE_REVIEW_REASON,
) -> PrecedenceResult:
    """Применяет таблицу прецедента §2 к паре «высокий ↔ низкий» источник.

    :param high_cell: ячейка более высокого источника (реестр/наследование).
    :param low_cell: ячейка более низкого источника (корпус/прямое доказательство).
    :param reason: свободнотекстовая аннотация причины для ``pending``-кандидата.
    """
    if is_inconclusive(low_cell) or low_cell.status == high_cell.status:
        return PrecedenceResult(cell=high_cell, kept_high=True)
    candidate = low_cell.model_copy(
        update={
            "state": CellState.PENDING,
            "source_kind": high_cell.source_kind,
            "review_reason": reason,
            "reviewed_by": None,
        }
    )
    return PrecedenceResult(cell=candidate, kept_high=False)


def apply_precedence(
    high_cell: Cell,
    low_cell: Cell,
    *,
    reason: str = PRECEDENCE_REVIEW_REASON,
) -> Cell:
    """Совместимая обёртка :func:`resolve_precedence` (исторический контракт Ш4).

    Возвращает высокую ячейку при согласии/inconclusive и ``pending``-кандидата при
    расхождении. Вызывающий, которому нужно знать, писать ли ячейку, использует
    :func:`resolve_precedence` напрямую.
    """
    return resolve_precedence(high_cell, low_cell, reason=reason).cell


def collect_protected_cells(
    store: Any, run_id: str
) -> dict[tuple[str, str], Cell]:
    """Собирает ячейки высоких источников, которые Ш6 не должен затирать.

    Читает реестровые ячейки через документированную поверхность
    ``iter_registry_cells`` (fallback — фильтр ``iter_cells``), затем добавляет
    унаследованные ячейки (Fork D). Для одного ключа ``(product_id, param_id)``
    остаётся ячейка более высокого источника (реестр сильнее наследования).

    Включаются только ячейки с решённым/предложенным ``status``: ``conditional``
    -наследование (``status=None``) не является высоким свидетельством и остаётся
    обычной очередью ревью. ``pending``-конфликт включается намеренно — иначе повторный
    Ш6 «оживил» бы его в корпусную ячейку и идемпотентность (S8/R7) сломалась бы.

    ``store`` без методов чтения (тестовые duck-typed дублёры) даёт пустую карту —
    обратная совместимость вызовов без store.
    """
    protected: dict[tuple[str, str], Cell] = {}
    if store is None:
        return protected

    registry_reader = getattr(store, "iter_registry_cells", None)
    iter_cells = getattr(store, "iter_cells", None)
    if callable(registry_reader):
        registry_cells = registry_reader(run_id)
    elif callable(iter_cells):
        registry_cells = (
            cell
            for cell in iter_cells(run_id)
            if cell.source_kind is SourceKind.REGISTRY
        )
    else:
        registry_cells = ()

    for cell in registry_cells:
        if _is_protectable(cell):
            protected[(cell.product_id, cell.param_id)] = cell

    # Fork D: наследование — следующий по силе источник (§2.2 п.7). Реестр не перетирается.
    if callable(iter_cells):
        for cell in iter_cells(run_id):
            if cell.source_kind is not SourceKind.INHERITANCE:
                continue
            if not _is_protectable(cell):
                continue
            protected.setdefault((cell.product_id, cell.param_id), cell)
    return protected


def _is_protectable(cell: Cell) -> bool:
    """Ячейка высокого источника несёт решение и подлежит защите от нижнего источника."""
    return cell.status is not None and cell.state in _PROTECTED_STATES
