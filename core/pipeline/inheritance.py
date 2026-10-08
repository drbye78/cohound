"""Волна наследования — Ш4 (без LLM): перенос upstream-факта по ``baselines.csv`` (02 §D.2).

Детерминированная волна (детерминированное раньше поиска, §2.2 п.7). По строке
``baselines.csv`` и совпадению upstream-паспорта:

======  ===================================================================
mode    действие
======  ===================================================================
direct  upstream-факт переносится как статус (✅) — **при** несущем
        derived-evidence (I8/K5): ``evidence_type=derived``,
        ``source_kind=inheritance``, резолв ``source_url`` (I10/I15) и
        ``content_hash`` (I19). Контракт проверяется
        :func:`core.domain.invariants.check_inheritance` /
        :func:`core.domain.invariants.check_derived_evidence`; нарушение →
        ячейка не выпускается, ошибка собирается в ``errors``.
conditional
        уходит в ревью/кандидат: ``state=pending``, ``status=None`` (НЕ
        разрешённый статус).
blocked upstream-факт не используется — пропуск (ячейка не производится).
======  ===================================================================

Нет строки ``baselines.csv`` = кросс-линейный параметр (архетип B) — здесь не
производится (только корпус, Ш5/Ш6).

**П>В** (§2.2 п.8): прямое доказательство Ш6 старше наследования. Противоречащий
прямой статус **понижает** унаследованный, но не перезаписывает его автоматически —
см. :func:`apply_precedence`: возвращается кандидат на ревью (``pending``).

Нейтрален (§1.3): нет названий продуктов/вендоров/реестров.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from core.domain import (
    BaselineRow,
    Cell,
    CellEvidence,
    CellState,
    EvidenceType,
    InheritanceMode,
    LLMEvidence,
    Passport,
    SourceKind,
    Status,
    UpstreamRef,
    check_derived_evidence,
    check_inheritance,
    derived_evidence_id,
)
from core.pipeline.runner import PhaseResult
from core.util.hashing import content_hash

__all__ = [
    "inheritance_wave",
    "apply_precedence",
]

#: Разделитель полей для детерминированного хэша цитаты (unit separator).
_FIELD_SEP = "\x1f"


def _passport_index(passports: Any) -> dict[str, Passport]:
    """Индексирует паспорта по ``product_id`` (принимает mapping или итерируемое)."""
    if passports is None:
        return {}
    if isinstance(passports, Mapping):
        return {str(key): value for key, value in passports.items()}
    if isinstance(passports, Iterable):
        return {
            str(passport.product_id): passport
            for passport in passports
            if passport is not None
        }
    return {}


def _baseline_index(baselines: Iterable[BaselineRow]) -> dict[tuple[str, str], BaselineRow]:
    """Индексирует строки baseline по ``(baseline_id, param_id)`` (детерминированно).

    При дублях остаётся первая по сортировке ``(baseline_id, param_id, mode)``; повторные
    не создают неоднозначности.
    """
    ordered = sorted(
        (row for row in baselines if row is not None),
        key=lambda row: (row.baseline_id, row.param_id, row.inheritance_mode.value),
    )
    index: dict[tuple[str, str], BaselineRow] = {}
    for row in ordered:
        index.setdefault((row.baseline_id, row.param_id), row)
    return index


def _numeric_version(value: str | None) -> tuple[int, ...] | None:
    """Числовые компоненты версии (``"16.2-beta1"`` → ``(16, 2, 1)``)."""
    if not value:
        return None
    parts = re.findall(r"\d+", str(value))
    return tuple(int(part) for part in parts) if parts else None


def _satisfies_min_version(version: str | None, min_version: str | None) -> bool:
    """Проверяет ``upstream.version >= min_version`` (мягко: нераспознанное — пропуск).

    ``min_version=None`` → всегда ``True``. Если хотя бы одна версия не содержит числовых
    компонент, сравнение невозможно: возвращается ``True`` (fail-open) — решение
    документируется вызывающим; жёсткая блокировка по недоказуемому сравнению была бы
    произволом (A3).
    """
    if min_version is None:
        return True
    current = _numeric_version(version)
    minimum = _numeric_version(min_version)
    if current is None or minimum is None:
        return True
    length = max(len(current), len(minimum))
    current = current + (0,) * (length - len(current))
    minimum = minimum + (0,) * (length - len(minimum))
    return current >= minimum


def _upstream_source_id(baseline: BaselineRow, param_id: str) -> str:
    """Нейтральный ``source_id`` паспортной цитаты upstream (K5, §4.2 регламента)."""
    return f"upstream://{baseline.baseline_id}#{param_id}"


def _derived_evidence(
    run_id: str,
    product_id: str,
    param_id: str,
    baseline: BaselineRow,
    passport: Passport,
) -> tuple[CellEvidence, UpstreamRef] | None:
    """Строит derived-evidence наследования и upstream-ссылку для проверки (K5).

    ``None`` — если у паспорта нет upstream-цитаты (нет резолва/hash): нарушение I1/I10/I15.
    """
    upstream = passport.upstream
    if upstream is None or upstream.evidence is None:
        return None
    source_id = _upstream_source_id(baseline, param_id)
    url = upstream.evidence.url
    quote = upstream.evidence.quote
    digest = content_hash(f"{url}{_FIELD_SEP}{quote}")
    evidence = CellEvidence(
        evidence_id=derived_evidence_id(
            run_id, product_id, param_id, source_id, SourceKind.INHERITANCE.value, 0
        ),
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id=source_id,
        source_url=url,
        citation_quote=quote,
        content_hash=digest,
    )
    upstream_ref = UpstreamRef(
        product_id=product_id,
        param_id=param_id,
        source_id=source_id,
        source_url=url,
        content_hash=digest,
    )
    return evidence, upstream_ref


def _direct_cell(
    run_id: str,
    product_id: str,
    param_id: str,
    baseline: BaselineRow,
    passport: Passport,
    errors: list[str],
) -> tuple[Cell | None, CellEvidence | None]:
    """Строит унаследованную ✅-ячейку ``direct``; при нарушении K5 — ``(None, None)``.

    Возвращает ``(ячейка, derived-evidence)``. Ошибки инвариантов собираются в ``errors``.
    """
    built = _derived_evidence(run_id, product_id, param_id, baseline, passport)
    if built is None:
        errors.append(
            f"I8/I15: {product_id}/{param_id}: direct-наследование без upstream-цитаты "
            f"(нет резолва/hash) — ячейка не выпущена"
        )
        return None, None
    evidence, upstream_ref = built

    # K5: контракт производного доказательства (I1/I8/I10/I15/I19).
    derived_violations = check_derived_evidence(evidence, upstream_ref)
    if derived_violations:
        errors.append(
            f"{product_id}/{param_id}: derived-evidence наследования невалиден: "
            + "; ".join(f"{v.code}: {v.message}" for v in derived_violations)
        )
        return None, None

    cell = Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[
            LLMEvidence(source_id=evidence.source_id, quote=evidence.citation_quote or "")
        ],
    )

    # I8 + derived-evidence: правило наследования обязано быть выполнено.
    violations = check_inheritance(cell, baseline, [evidence])
    if violations:
        errors.append(
            f"{product_id}/{param_id}: наследование нарушено: "
            + "; ".join(f"{v.code}: {v.message}" for v in violations)
        )
        return None, None
    return cell, evidence


def _conditional_cell(
    run_id: str,
    product_id: str,
    param_id: str,
) -> Cell:
    """Кандидат на ревью для ``conditional``: ``pending`` без разрешённого статуса."""
    return Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        state=CellState.PENDING,
        status=None,
        source_kind=SourceKind.INHERITANCE,
    )


def inheritance_wave(
    plan: Any,
    baselines: list[BaselineRow],
    passports: Any,
    store: Any = None,
) -> PhaseResult:
    """Ш4: волна наследования. Возвращает ``PhaseResult`` (без LLM).

    :param plan: ``RunPlan`` (продукты, параметры, ``run_id``).
    :param baselines: строки ``baselines.csv``.
    :param passports: ``Passport``-набор — mapping ``product_id → Passport`` либо итерируемое.
    :param store: опциональный ``StorePort`` (пишет ячейки и derived-evidence).
    :raises ValueError: если ``baselines`` не список (контракт входа).
    """
    if not isinstance(baselines, list):
        raise ValueError("baselines обязан быть списком BaselineRow")

    run_id = str(getattr(plan, "run_id", ""))
    products = sorted(str(pid) for pid in getattr(plan, "products", []))
    params = sorted(
        getattr(plan, "params", []), key=lambda row: getattr(row, "param_id", "")
    )
    passport_by_product = _passport_index(passports)
    baseline_by_key = _baseline_index(baselines)

    cells: list[Cell] = []
    evidence_items: list[CellEvidence] = []
    errors: list[str] = []

    for product_id in products:
        passport = passport_by_product.get(product_id)
        if passport is None:
            # Паспорт — предусловие G2; без него upstream-совпадение недоказуемо.
            errors.append(f"нет паспорта для продукта {product_id!r} — Ш4 пропущен")
            continue
        upstream = passport.upstream
        if upstream is None:
            # Нет upstream-линии → кросс-линейный параметр (архетип B): не здесь.
            continue

        for row in params:
            baseline = baseline_by_key.get((upstream.baseline_id, row.param_id))
            if baseline is None:
                # Нет строки baselines.csv → кросс-линейный параметр (только корпус).
                continue

            mode = baseline.inheritance_mode
            if mode is InheritanceMode.BLOCKED:
                # upstream-факт не используется: пропуск.
                continue
            if mode is InheritanceMode.CONDITIONAL:
                cells.append(_conditional_cell(run_id, product_id, row.param_id))
                continue

            # mode == DIRECT: применяем порог min_version (upstream ≥ min_version).
            if not _satisfies_min_version(upstream.version, baseline.min_version):
                continue
            cell, evidence = _direct_cell(
                run_id, product_id, row.param_id, baseline, passport, errors
            )
            if cell is not None:
                cells.append(cell)
            if evidence is not None:
                evidence_items.append(evidence)

    cells.sort(key=lambda cell: (cell.product_id, cell.param_id))
    _persist(store, cells, evidence_items, errors)
    return PhaseResult(status="ok", phase="inheritance", cells=cells, errors=errors)


def _persist(
    store: Any,
    cells: list[Cell],
    evidence: list[CellEvidence],
    errors: list[str],
) -> None:
    """Пишет ячейки/evidence; сбой персистенции изолируется (не роняет волну)."""
    if store is None:
        return
    for ev in evidence:
        try:
            store.add_evidence(ev)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"add_evidence: {type(exc).__name__}: {exc}")
    for cell in cells:
        try:
            store.save_cell(cell)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"save_cell: {type(exc).__name__}: {exc}")


def apply_precedence(inherited_cell: Cell, direct_cell: Cell) -> Cell:
    """П>В (§2.2 п.8): прямое доказательство Ш6 старше наследования.

    Если ``direct_cell`` несёт разрешённый статус, **отличающийся** от унаследованного,
    возвращается кандидат на ревью: копия прямой ячейки с ``state=pending``. Прямой статус
    **не** применяется автоматически и унаследованная ячейка не перезаписывается — решение
    принимает человек на ревью (I6/I7-дисциплина). Совпадающий (или отсутствующий у прямого)
    статус означает отсутствие противоречия: возвращается ``inherited_cell`` без изменений.
    """
    if direct_cell.status is None or direct_cell.status == inherited_cell.status:
        return inherited_cell
    return direct_cell.model_copy(update={"state": CellState.PENDING})
