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

**Наследование не доказывает downstream-факт** (регламент, стр. 288): оно переносит
возможность проверить применимость upstream-факта. Паспортная цитата у продукта одна,
поэтому унаследованная ячейка получает тот же замер «о предмете ли доказательство», что и
оценённая (:func:`core.pipeline.evaluate.support_subject_overlap`): постороннее
доказательство видно в ``validation_notes``/``review_reason``, но вердикт не меняется
(A3). Базис волны — :func:`inheritance_basis` — публикует «объявлено/выпущено» по
каждому режиму, чтобы ``direct`` и ``conditional`` не сливались в один итог.

Нейтрален (§1.3): нет названий продуктов/вендоров/реестров.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from core.domain import (
    BaselineRow,
    CatalogRow,
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
from core.pipeline.evaluate import (
    SUPPORT_STATE_UNRELATED,
    compose_review_reason,
    support_subject_overlap,
)
from core.pipeline.precedence import (
    CONDITIONAL_REVIEW_REASON,
    PRECEDENCE_REVIEW_REASON,
    apply_precedence,
    collect_protected_cells,
    resolve_precedence,
)
from core.pipeline.runner import PhaseResult
from core.util.hashing import content_hash

__all__ = [
    "apply_precedence",
    "inheritance_basis",
    "inheritance_wave",
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


def _annotate_support(cell: Cell, param: CatalogRow) -> Cell:
    """Пишет замер «о предмете ли доказательство» в унаследованную ячейку (K5/§4.2).

    Наследование переносит **возможность проверить применимость** upstream-факта, а не
    утверждение о downstream-параметре (регламент, стр. 288). Паспортная цитата у
    продукта одна, поэтому без замера 19 разных параметров выглядели бы подтверждёнными
    одной визиткой («мы на базе X»), и ни один из них не был бы подтверждён по существу.

    Замер — тот же, что у оценённой ячейки (:func:`support_subject_overlap`), без второй
    реализации и без копии стеммера. Что пишется:

    * ``validation_notes`` — строка ``support_not_about_subject: N/M …`` (только для
      ``unrelated_support``);
    * ``review_reason`` — та же строка **для ``✅``** и только в склейке с уже стоящей
      причиной (:func:`compose_review_reason`): ``conditional``-причина и конфликт
      источников не затираются;
    * вердикт не меняется (A3): это наблюдение, а не решение.

    ``no_quotes`` здесь не наша аномалия (унаследованный ``✅`` без цитаты запрещён I1/K5
    и отсекается :func:`check_derived_evidence`); ``supported`` и неизмеримый предмет
    молчат — заметки публикуют дефект, а не норму.
    """
    measured = support_subject_overlap(param, [ev.quote for ev in cell.evidence])
    if measured is None or measured.state != SUPPORT_STATE_UNRELATED:
        return cell
    note = measured.note()
    if note is None:  # pragma: no cover - защита от рассинхронизации state/note
        return cell
    notes = list(cell.validation_notes)
    if note in notes:
        # Повторный проход (resume, S8/R7) уже сказал это: не плодим записи и не
        # склеиваем причину саму с собой.
        return cell
    notes.append(note)
    reason = cell.review_reason
    if cell.status is Status.OK:
        reason = compose_review_reason(reason, note)
    return cell.model_copy(update={"validation_notes": notes, "review_reason": reason})


def _direct_cell(
    run_id: str,
    product_id: str,
    param: CatalogRow,
    baseline: BaselineRow,
    passport: Passport,
    errors: list[str],
) -> tuple[Cell | None, CellEvidence | None]:
    """Строит унаследованную ✅-ячейку ``direct``; при нарушении K5 — ``(None, None)``.

    Возвращает ``(ячейка, derived-evidence)``. Ошибки инвариантов собираются в ``errors``.
    """
    param_id = str(param.param_id)
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
    # Наблюдение «о предмете ли доказательство» — после инвариантов: оно описывает
    # выпущенную ячейку и вердикт не оспаривает.
    return _annotate_support(cell, param), evidence


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
        review_reason=CONDITIONAL_REVIEW_REASON,
    )


def _apply_higher_source(
    cell: Cell,
    protected: Mapping[tuple[str, str], Cell],
    product_id: str,
    param_id: str,
) -> Cell | None:
    """Накладывает более высокий источник (реестр) на унаследованную ячейку (Fork D).

    Возвращает унаследованную ячейку, ``pending``-кандидата (расхождение) либо ``None``
    (согласие/inconclusive: высокий источник уже в store, писать нечего).
    """
    high = protected.get((product_id, param_id))
    if high is None or high.source_kind is not SourceKind.REGISTRY:
        return cell
    result = resolve_precedence(high, cell)
    return None if result.kept_high else result.cell


#: Ключи состояний выпущенных ячеек в публикуемом базисе (порядок фиксирован — артефакт
#: должен быть воспроизводим, R7). ``other`` — состояние вне трёх ожидаемых (``reviewed``,
#: ``preliminary``): не прячем, иначе базис перестал бы сходиться с числом ячеек.
_RELEASED_STATE_KEYS: tuple[str, ...] = ("evaluated", "pending", "failed", "other")


def _state_key(state: Any) -> str:
    """Ключ состояния ячейки для публикуемого базиса (``evaluated/pending/failed/other``)."""
    value = str(getattr(state, "value", state))
    return value if value in _RELEASED_STATE_KEYS else "other"


def _basis_summary(
    product_id: str,
    planned: Mapping[str, int],
    released: Mapping[str, Mapping[str, int]],
    unattributed: int,
) -> str:
    """Одна строка «что объявлено / что выпущено» продукта — для ``log_entries``.

    Читается как ``direct 19/19 evaluated; conditional 0/14 pending; blocked 0 skipped``:
    оператор видит режим каждого параметра, а не только итог «19 ✅». Именно разница
    «объявлено N, выпущено M» показывает, что режим не сработал (порог версии, отказ
    инварианта), а не что параметров не было.
    """
    direct = released.get("direct", {})
    conditional = released.get("conditional", {})
    parts = [
        f"direct {direct.get('evaluated', 0)}/{planned.get('direct', 0)} evaluated",
    ]
    extra = [
        f"{key} {direct.get(key, 0)}"
        for key in ("pending", "failed", "other")
        if direct.get(key, 0)
    ]
    if extra:
        parts.append("direct " + ", ".join(extra))
    parts.append(
        f"conditional {conditional.get('pending', 0)}/{planned.get('conditional', 0)} pending"
    )
    extra = [
        f"{key} {conditional.get(key, 0)}"
        for key in ("evaluated", "failed", "other")
        if conditional.get(key, 0)
    ]
    if extra:
        parts.append("conditional " + ", ".join(extra))
    parts.append(f"blocked {planned.get('blocked', 0)} skipped")
    if unattributed:
        parts.append(f"unattributed {unattributed}")
    return f"{product_id}: " + "; ".join(parts)


def inheritance_basis(
    baselines: Sequence[BaselineRow],
    *,
    products: Iterable[str],
    passports: Any = None,
    params: Iterable[Any] | None = None,
    cells: Iterable[Cell] = (),
) -> dict[str, Any]:
    """**Базис наследования**: объявленный режим против фактически выпущенного.

    Итог волны («15 ✅, 4 pending») этого не показывает: по нему нельзя отличить
    ``direct``-ячейку от ``conditional``-кандидата, а «объявлено 19, выпущено 19» —
    единственное свидетельство, что режим не был тихо подавлен (порог ``min_version``,
    отказ K5). Поэтому публикуются обе стороны по каждому продукту.

    Откуда берутся числа (второй раз ничего не считается):

    * **объявлено** (``planned``) — строки ``baselines.csv`` для ``upstream.baseline_id``
      продукта; при заданном ``params`` — только по объявленным параметрам плана;
    * **выпущено** (``released``) — ``cells`` **результата волны** (``PhaseResult.cells``
      Ш4), а не повторное чтение стора: базис описывает работу этой волны. Режим ячейки
      берётся из той же строки ``baselines.csv`` по ``param_id``. Ячейка **другого**
      источника (корпусная оценка того же параметра) режиму не приписывается — она не
      работа Ш4 — но её число публикуется как ``foreign_cells``: исключение видно;
      унаследованная ячейка без строки базлайна попадает в ``unattributed_cells``.

    :param baselines: строки ``baselines.csv``.
    :param products: ``product_id`` плана (итерируемое).
    :param passports: ``Passport``-набор (mapping либо итерируемое) — источник
        ``upstream.baseline_id``.
    :param params: объявленные параметры плана (``CatalogRow`` или ``param_id``);
        ``None`` — фильтра нет (считаются все строки базлайна).
    :param cells: ячейки результата волны.
    :returns: JSON-совместимый словарь по продуктам (``planned``/``released``/
        ``cells_emitted``/``unattributed_cells``/``summary``), итоги и список строк
        ``summary``. В БД и в файлы функция ничего не пишет.
    """
    passport_by_product = _passport_index(passports)
    wanted: set[str] | None = None
    if params is not None:
        wanted = {str(getattr(param, "param_id", param)) for param in params}

    cells_by_product: dict[str, list[Cell]] = {}
    for cell in cells:
        cells_by_product.setdefault(str(cell.product_id), []).append(cell)

    report: dict[str, dict[str, Any]] = {}
    planned_total = {mode.value: 0 for mode in InheritanceMode}
    released_total = {
        mode.value: {key: 0 for key in _RELEASED_STATE_KEYS} for mode in InheritanceMode
    }
    summaries: list[str] = []
    for product_id in sorted(str(pid) for pid in products):
        passport = passport_by_product.get(product_id)
        upstream = passport.upstream if passport is not None else None
        baseline_id = upstream.baseline_id if upstream is not None else None

        # Объявлено: строки базлайна этой upstream-линии (режим — свойство строки).
        planned = {mode.value: 0 for mode in InheritanceMode}
        mode_by_param: dict[str, InheritanceMode] = {}
        for row in baselines:
            if row is None or row.baseline_id != baseline_id:
                continue
            if wanted is not None and row.param_id not in wanted:
                continue
            planned[row.inheritance_mode.value] += 1
            mode_by_param[row.param_id] = row.inheritance_mode

        # Выпущено: ровно то, что вернула волна (никакого второго чтения стора). Ячейка
        # другого источника — не работа Ш4, даже если у параметра есть строка базлайна
        # (корпусная оценка того же параметра): она не приписывается режиму, но и не
        # прячется — её число публикуется как ``foreign_cells``. Кандидат ревью, который
        # волна выпустила поверх реестра, наследует ``source_kind`` высокого источника —
        # он остаётся работой Ш4 (иначе базис «терял» бы свои же ячейки).
        released = {
            mode.value: {key: 0 for key in _RELEASED_STATE_KEYS}
            for mode in InheritanceMode
        }
        unattributed = 0
        foreign = 0
        product_cells = cells_by_product.get(product_id, [])
        for cell in product_cells:
            wave_cell = (
                cell.source_kind is SourceKind.INHERITANCE
                or cell.review_reason == PRECEDENCE_REVIEW_REASON
            )
            mode = mode_by_param.get(str(cell.param_id))
            if mode is None:
                if wave_cell:
                    unattributed += 1
                continue
            if not wave_cell:
                foreign += 1
                continue
            released[mode.value][_state_key(cell.state)] += 1
        wave_cells = sum(
            count
            for states in released.values()
            for count in states.values()
        )

        summary = _basis_summary(product_id, planned, released, unattributed)
        for mode in InheritanceMode:
            planned_total[mode.value] += planned[mode.value]
            for key in _RELEASED_STATE_KEYS:
                released_total[mode.value][key] += released[mode.value][key]
        report[product_id] = {
            "baseline_id": baseline_id,
            "planned": planned,
            "released": released,
            "cells_emitted": wave_cells,
            "unattributed_cells": unattributed,
            "foreign_cells": foreign,
            "summary": summary,
        }
        # Продукт без объявленных строк и без выпущенных ячеек не несёт факта:
        # «0/0» в артефакте выглядело бы наблюдением.
        if sum(planned.values()) or wave_cells:
            summaries.append(summary)

    return {
        "measurement": "inheritance_basis",
        "products": report,
        "totals": {"planned": planned_total, "released": released_total},
        "summary": summaries,
    }


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
    # Ш3 уже отработал (PHASES: registry → inheritance): реестровые ячейки — высокий
    # источник, который Ш4 не должен молча затирать (Fork D, §2.2 п.7).
    protected = collect_protected_cells(store, run_id)

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
                run_id, product_id, row, baseline, passport, errors
            )
            if cell is not None:
                cell = _apply_higher_source(
                    cell, protected, product_id, row.param_id
                )
            if cell is not None:
                cells.append(cell)
            if evidence is not None:
                evidence_items.append(evidence)

    cells.sort(key=lambda cell: (cell.product_id, cell.param_id))
    _persist(store, cells, evidence_items, errors)
    # Базис наследования — тот же результат волны, а не второе чтение стора: оператор
    # видит «direct N/M evaluated, conditional K/L pending» там же, где итог фазы
    # (``results[].notes`` в выводе прогона), и не догадывается, что под «✅» разные режимы.
    basis = inheritance_basis(
        baselines,
        products=products,
        passports=passport_by_product,
        params=params,
        cells=cells,
    )
    log_entries = (
        ["inheritance_basis: " + " | ".join(basis["summary"])]
        if basis["summary"]
        else []
    )
    return PhaseResult(
        status="ok",
        phase="inheritance",
        cells=cells,
        errors=errors,
        log_entries=log_entries,
    )


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
