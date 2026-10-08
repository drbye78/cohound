"""coHound — фаза ревью Ш7: очередь ревью и повышение (promotion) кандидатов.

Фаза Ш7 — «очередь ревью / preliminary-рабочий стол» (§5) и ГЕЙТ G3 (§8). Модуль даёт
две вещи:

* :func:`review_queue` — детерминированная очередь ячеек, требующих человеческого
  внимания: preview-кандидаты (``preliminary``, вне расчётов по I13) и ``❓``
  (``UNKNOWN``) ``evaluated``-ячейки (I3: протокол исчерпан / требуется человек).
  Это «preliminary-рабочий стол» §1.4/§7.
* :func:`promote` / :func:`promote_batch` — **человеческое** повышение ``preliminary`` →
  ``evaluated`` (переаттестация, I14). CLI: ``hound promote --validated-only``
  (§D.6/E.1); пакетный режим — :func:`promote_batch`.

**Повышение — это решение человека (I6/I7, §2.2 п.10).** Актор (``actor``) обязателен и
непуст. Модуль **никогда не выдумывает вердикт** (A3): если ячейка не несёт валидированного
``status``, он берётся только из явного аргумента ``status`` (переаттестационный вердикт,
§I14). Что именно считается согласием:

* ``validated_only=True`` (дефолт; соответствует ``hound promote --validated-only``) —
  валидированный вердикт обязателен. Нет ни ``cell.status``, ни явного ``status`` →
  :class:`ReviewError` (код ``I14``). Вердикт **не подставляется** дефолтом.
* ``validated_only=False`` — разрешено повышение без вердикта (``status`` остаётся ``null``):
  это первый шаг цикла кандидата §1.4 «``promote`` · ``set-status`` (actor=human) ·
  ``fix-evidence`` · ``reject``».

**Инварианты — всегда.** Независимо от ``validated_only`` построенная ``evaluated``-ячейка
прогоняется через :func:`core.domain.invariants.check_cell` (I1/I2/I4/I6/I7/I13). Нарушение
(например, ``✅`` без evidence — I1, или ``❌`` без absence-evidence — I2) → :class:`ReviewError`;
в strict-слой нечестная ячейка не попадает.

**Ограничение поверхности чтения.** ``StorePort.iter_cells`` (§B.1) не возвращает
``evidence``/``absence_evidence`` (они живут в ``cell_evidence``). Поэтому при загрузке из
store :attr:`ReviewItem.evidence_count` всегда ``0``, а статусы, требующие evidence
(``✅``/``⚠️``/``❌``), при ``validated_only=True`` отвергаются до появления отдельного
шага ``fix-evidence`` (и/или порт-метода чтения evidence). Это осознанная fail-closed
позиция, а не молчаливая подстановка — вердикт обязан опираться на доказательство.

Модуль категорийно нейтрален (§1.3), детерминирован, зависит только от stdlib и
domain/pipeline; ``core.adapters`` не импортируется (матрица 01 §1).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from core.domain import Cell, CellState, Status, check_cell
from core.pipeline.runner import Clock, run_isolated
from core.ports.store import StorePort

__all__ = [
    "PromotionResult",
    "ReviewError",
    "ReviewItem",
    "promote",
    "promote_batch",
    "review_queue",
]

#: Состояния по умолчанию в очереди ревью: preliminary-контур + уже отревьюенные строки.
DEFAULT_REVIEW_STATES: tuple[CellState, ...] = (
    CellState.PRELIMINARY,
    CellState.REVIEWED,
)


class ReviewError(Exception):
    """Ошибка ревью/повышения. ``code`` — машинная ссылка на инвариант/правило (§E.2)."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class ReviewItem:
    """Ячейка, поставленная в очередь человеческого внимания (§7 Ш7 / G3).

    :param product_id / param_id: ключ ячейки.
    :param state: состояние ячейки.
    :param status: текущий ``status`` (``None`` для ``preliminary``/``failed``).
    :param reason: почему ячейка в очереди (нейтральное описание причины).
    :param evidence_count: число evidence, доступных на загруженной ячейке (см. ограничение
        поверхности чтения в докстринге модуля — из store всегда ``0``).
    :param index_build_id: сборка, к которой привязан срез ячейки (R7), если известна.
    """

    product_id: str
    param_id: str
    state: CellState
    status: Status | None
    reason: str
    evidence_count: int
    index_build_id: str | None


@dataclass
class PromotionResult:
    """Агрегат пакетного повышения (§D.6/E.1 ``hound promote``).

    :param promoted: успешно повышенные ``evaluated``-ячейки.
    :param skipped: ``(product_id, param_id, reason)`` для отказавших по правилу ячеек.
    :param errors: изолированные неожиданные ошибки (не сбои правил).
    """

    promoted: list[Cell] = field(default_factory=list)
    skipped: list[tuple[str, str, str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Очередь ревью
# ---------------------------------------------------------------------------


def _review_reason(cell: Cell) -> str:
    """Нейтральная причина постановки ячейки в очередь ревью."""
    if cell.state is CellState.PRELIMINARY:
        return (
            "preliminary-кандидат (I13: вне расчётов; "
            "promote · set-status · fix-evidence · reject)"
        )
    if cell.state is CellState.REVIEWED:
        return "ячейка, отмеченная reviewed"
    # evaluated + ❓
    return "unknown (❓): протокол исчерпан или требуется человек (I3)"


def _review_item(cell: Cell) -> ReviewItem:
    """Собирает :class:`ReviewItem` из доменной ячейки."""
    return ReviewItem(
        product_id=cell.product_id,
        param_id=cell.param_id,
        state=cell.state,
        status=cell.status,
        reason=_review_reason(cell),
        evidence_count=len(cell.evidence),
        index_build_id=cell.index_build_id,
    )


def review_queue(
    store: StorePort,
    run_id: str,
    *,
    states: Sequence[CellState] = DEFAULT_REVIEW_STATES,
) -> list[ReviewItem]:
    """Очередь ревью Ш7 для прогона: ячейки в ``states`` + ``❓`` ``evaluated``-кандидаты.

    Детерминирована: сортировка по ``(product_id, param_id)``. ``states`` по умолчанию —
    ``(preliminary, reviewed)``; ``preliminary`` — это preliminary-контур (I13, вне
    расчётов). Дополнительно **всегда** включаются ``evaluated``-ячейки со
    ``status = ❓`` (``UNKNOWN``): это кандидаты на человеческое решение (I3, §9
    ``nda_questions``/G3), независимо от ``states``.
    """
    wanted = set(states)
    items: list[ReviewItem] = []
    for cell in store.iter_cells(run_id):
        if cell.state in wanted:
            items.append(_review_item(cell))
        elif cell.state is CellState.EVALUATED and cell.status is Status.UNKNOWN:
            items.append(_review_item(cell))
    items.sort(key=lambda item: (item.product_id, item.param_id))
    return items


# ---------------------------------------------------------------------------
# Повышение (promotion) — человеческая переаттестация
# ---------------------------------------------------------------------------


def _require_actor(actor: str) -> None:
    """I6/I7: повышение/N/A — только человек; пустой актор запрещён."""
    if not actor or not actor.strip():
        raise ReviewError(
            "повышение требует непустого actor (человек)",
            code="I6/I7",
        )


def _find_cell(
    store: StorePort, run_id: str, product_id: str, param_id: str
) -> Cell:
    """Находит ячейку прогона по ключу ``(product_id, param_id)`` (детерминированно)."""
    for cell in store.iter_cells(run_id):
        if cell.product_id == product_id and cell.param_id == param_id:
            return cell
    raise ReviewError(
        f"ячейка не найдена: {product_id}/{param_id}",
        code="I14",
    )


def promote(
    *,
    store: StorePort,
    run_id: str,
    product_id: str,
    param_id: str,
    actor: str,
    notes: str | None = None,
    status: Status | None = None,
    validated_only: bool = True,
    clock: Clock | None = None,
) -> Cell:
    """Повышает одну ``preliminary``-ячейку в ``evaluated`` (переаттестация, I14).

    :param store / run_id: прогон и его хранилище.
    :param product_id / param_id: ключ ячейки.
    :param actor: человек, выполняющий переаттестацию (I6/I7). Пустой → :class:`ReviewError`.
    :param notes: человеческая заметка к решению. В доменной модели ``Cell`` нет поля
        заметки, поэтому здесь она не персистится (аудит-след — ``gate_decisions``).
    :param status: явный вердикт человека. Если не задан, берётся ``cell.status``.
    :param validated_only: ``True`` (дефолт, ``hound promote --validated-only``) — без
        валидированного вердикта повышение отклоняется (``I14``); ``False`` — допускается
        ``evaluated`` без вердикта (первый шаг цикла §1.4, ``set-status`` позднее).
    :param clock: зарезервировано (``Cell`` не несёт ``updated_at``); для совместимости.

    Возвращает сохранённую ``evaluated``-ячейку с ``reviewed_by = actor``.

    **Никогда не выдумывает вердикт.** Итоговый ``status`` — это явный ``status`` либо
    ``cell.status``; иначе при ``validated_only=True`` возбуждается :class:`ReviewError`.
    Готовая ячейка проверяется :func:`check_cell` (I1/I2/I4/I6/I7/I13): нарушение → отказ,
    нечестная strict-ячейка не персистится.
    """
    _require_actor(actor)
    cell = _find_cell(store, run_id, product_id, param_id)

    if cell.state is not CellState.PRELIMINARY:
        raise ReviewError(
            f"повышение применимо только к preliminary-ячейкам, "
            f"получено state={cell.state.value}",
            code="I14",
        )

    target_status = status if status is not None else cell.status
    if target_status is None and validated_only:
        raise ReviewError(
            "нет валидированного вердикта: требуется явный status "
            "(переаттестация, I14); пустой статус не подставляется",
            code="I14",
        )

    promoted = cell.model_copy(
        update={
            "state": CellState.EVALUATED,
            "status": target_status,
            "reviewed_by": actor,
        }
    )

    violations = check_cell(promoted)
    if violations:
        codes = ",".join(sorted({v.code for v in violations}))
        raise ReviewError(
            "повышенная ячейка нарушает инварианты "
            f"[{codes}]: {violations[0].message}",
            code=violations[0].code,
        )

    store.save_cell(promoted)
    return promoted


@dataclass
class _Skip:
    """Внутренний маркер отказа по правилу (для :func:`promote_batch`)."""

    product_id: str
    param_id: str
    reason: str


def promote_batch(
    *,
    store: StorePort,
    run_id: str,
    items: Iterable[tuple[str, str]],
    actor: str,
    notes: str | None = None,
    validated_only: bool = True,
    statuses: Mapping[tuple[str, str], Status] | None = None,
    clock: Clock | None = None,
) -> PromotionResult:
    """Пакетное повышение ``[(product_id, param_id), …]`` с изоляцией сбоев (§E.1).

    Каждый элемент обрабатывается независимо (:func:`run_isolated`): отказ по правилу
    попадает в ``skipped``, неожиданная ошибка — в ``errors``. Вердикты задаются картой
    ``statuses`` по ключу ``(product_id, param_id)`` (иначе ``None`` — см. :func:`promote`).
    Пустой ``actor`` отклоняется на уровне батча (I6/I7).
    """
    _require_actor(actor)
    pairs = [(str(product_id), str(param_id)) for product_id, param_id in items]
    verdicts = statuses or {}

    def _one(pair: tuple[str, str]) -> Cell | _Skip:
        product_id, param_id = pair
        try:
            return promote(
                store=store,
                run_id=run_id,
                product_id=product_id,
                param_id=param_id,
                actor=actor,
                notes=notes,
                status=verdicts.get(pair),
                validated_only=validated_only,
                clock=clock,
            )
        except ReviewError as exc:
            return _Skip(product_id, param_id, str(exc))

    outputs, errors = run_isolated(pairs, _one)
    promoted = [out for out in outputs if isinstance(out, Cell)]
    skipped = [
        (out.product_id, out.param_id, out.reason)
        for out in outputs
        if isinstance(out, _Skip)
    ]
    return PromotionResult(promoted=promoted, skipped=skipped, errors=list(errors))
