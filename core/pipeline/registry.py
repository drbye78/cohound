"""Реестровая волна — Ш3 (без LLM): R-ячейки из реестровых фактов (02 §D.1, R1).

Детерминированная волна (детерминированное раньше поиска, §2.2 п.7): ядро **не знает**
конкретных реестров. Специфика живёт в данных (``packages/{case_id}/registries/*.yaml``)
и в адаптере
``adapters/registry_generic.py``, реализующем :class:`RegistryFetcherPort`. Здесь —
категорийно-нейтральная оркестрация:

1. по плану определить параметры, покрываемые реестрами;
2. для каждой пары (продукт × параметр) вызвать фетчеры (``fetch(ProductRef, scope)``);
3. построить R-ячейки (``source_kind=registry``): ✅ при наличии подтверждающего факта,
   ❌ при реестровой пустоте (``registry_absence`` — exhaustive по построению, I2b);
4. **R1:** каждый факт/evidence обязан нести непустой ``source_url`` — иначе факт
   отклоняется и попадает в ``errors``.

**Модель покрытия (что именно реализовано).** Параметр считается реестрово-покрытым,
если у него задан ``role_binding`` **и** в прогоне есть хотя бы один реестр (непустой
набор ``registries`` пакета, материализованный в инъекции фетчеров). ``RunPlan`` не несёт
список реестров пакета, поэтому набор реестров выводится из ключей ``fetchers`` (плюс
необязательный forward-compat атрибут ``plan.registries``, если он когда-либо появится).
Параметр без ``role_binding`` (например, чисто корпусный) реестром не покрывается.

**Изоляция.** Сбой одного продукта/реестра не прерывает волну: ошибка собирается в
``PhaseResult.errors`` (контракт фаз, ``runner.py``). Отсутствие подтверждающего факта
трактуется как ❌ только если параметр объявлен с ``absence_rule=registry_absence``;
иначе ячейка не производится (реестр не даёт права на отрицание — R12).

Нейтрален (§1.3): нет названий реестров/вендоров/продуктов.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellEvidence,
    CellState,
    EvidenceType,
    LLMEvidence,
    RegistryFact,
    SourceKind,
    SourceScope,
    Status,
    derived_evidence_id,
)
from core.pipeline.runner import PhaseResult
from core.ports.registry import RegistryFetcherPort
from core.ports.types import ProductRef

__all__ = [
    "registry_wave",
    "registry_covered_params",
    "resolve_registry_ids",
]


def resolve_registry_ids(
    plan: Any,
    fetchers: Mapping[str, RegistryFetcherPort],
) -> list[str]:
    """Детерминированный набор идентификаторов реестров прогона.

    Источник — ключи ``fetchers`` (композиция из ``package.yaml.registries``). Дополнительно
    учитывается необязательный forward-compat атрибут ``plan.registries`` (если ``RunPlan``
    когда-либо начнёт его нести). Пустой набор означает «реестры не сконфигурированы».
    """
    ids: set[str] = {str(key) for key in fetchers}
    declared = getattr(plan, "registries", None)
    if isinstance(declared, (list, tuple, set, frozenset)):
        ids.update(str(item) for item in declared)
    return sorted(ids)


def registry_covered_params(
    plan: Any,
    registry_ids: Iterable[str] = (),
) -> list[CatalogRow]:
    """Параметры плана, покрываемые реестрами, в детерминированном порядке.

    Покрытие — конъюнкция: ``role_binding`` задан **и** набор реестров непуст (см.
    docstring модуля). Возвращает отсортированный по ``param_id`` список.
    """
    has_registries = bool(list(registry_ids))
    if not has_registries:
        return []
    rows = [
        row
        for row in getattr(plan, "params", [])
        if bool(getattr(row, "role_binding", None))
    ]
    return sorted(rows, key=lambda row: row.param_id)


def _query_for(row: CatalogRow) -> str:
    """Детерминированный поисковый запрос для параметра (title + первый синоним).

    Языковой приоритет фиксирован: ``ru`` → ``en`` (расширение — первым синонимом),
    затем ``title``. Состав запроса не зависит от реализации/LLM (симметрия).
    """
    terms = row.concept_terms.get("ru") or row.concept_terms.get("en") or []
    if terms:
        return f"{row.title} {terms[0]}".strip()
    return row.title


def _evidence_for(
    run_id: str,
    product_id: str,
    row: CatalogRow,
    fact: RegistryFact,
    seq: int,
) -> CellEvidence:
    """Строит registry-``CellEvidence`` с обязательным ``source_url`` (R1/I10).

    ``source_id`` — нейтральная схема ``registry://{registry_id}/{product_id}#{param_id}``;
    ``citation_quote`` — сырое отображённое значение факта (``fact_json``). Корпусного
    ``content_hash`` не требуется: реестровый источник не входит в корпусные kinds (I19).
    """
    source_id = f"registry://{fact.registry_id}/{product_id}#{row.param_id}"
    return CellEvidence(
        evidence_id=derived_evidence_id(
            run_id, product_id, row.param_id, source_id, SourceKind.REGISTRY.value, seq
        ),
        run_id=run_id,
        product_id=product_id,
        param_id=row.param_id,
        source_kind=SourceKind.REGISTRY,
        evidence_type=EvidenceType.DIRECT,
        source_id=source_id,
        source_url=fact.source_url,
        citation_quote=fact.fact_json,
        index_build_id=None,
        retrieved_at=fact.fetched_at,
    )


def _collect_facts(
    run_id: str,
    product_id: str,
    row: CatalogRow,
    fetchers: Mapping[str, RegistryFetcherPort],
    errors: list[str],
) -> tuple[list[RegistryFact], bool, bool]:
    """Опрашивает все фетчеры по (продукт × параметр).

    Возвращает ``(валидные_факты, был_сбой, были_R1_отклонения)``. Валидные факты
    отсортированы детерминированно (``registry_id``, ``source_url``, ``fact_json``).
    """
    scope: dict[str, Any] = {
        "run_id": run_id,
        "query": _query_for(row),
        "param_id": row.param_id,
        "role_binding": row.role_binding,
        "languages": list(row.search_langs),
    }
    product = ProductRef(product_id=product_id)

    valid: list[RegistryFact] = []
    had_error = False
    had_r1_violation = False
    for key in sorted(fetchers):
        fetcher = fetchers[key]
        try:
            produced = fetcher.fetch(product, dict(scope))
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            had_error = True
            errors.append(
                f"{product_id}/{row.param_id}: реестр {key!r}: "
                f"{type(exc).__name__}: {exc}"
            )
            continue
        for fact in produced or []:
            # R1: факт без source_url не может стать evidence — отклоняем и фиксируем.
            if not getattr(fact, "source_url", None):
                had_r1_violation = True
                errors.append(
                    f"R1: {product_id}/{row.param_id}: реестр {key!r} вернул факт "
                    f"без source_url — отклонён"
                )
                continue
            valid.append(fact)

    valid.sort(
        key=lambda f: (
            str(getattr(f, "registry_id", "")),
            str(f.source_url),
            str(getattr(f, "fact_json", "")),
        )
    )
    return valid, had_error, had_r1_violation


def _absence_cell(
    run_id: str,
    product_id: str,
    row: CatalogRow,
    registry_ids: list[str],
) -> Cell:
    """R-ячейка реестровой пустоты: ❌ + absence-evidence с ``source_scope=exhaustive``.

    ``registry_absence`` — авторитетная энумерация: пустота реестра есть валидное
    негативное свидетельство **по построению** (I2b); ``scope_attested_by`` не требуется.
    """
    source_id = f"registry://{'+'.join(registry_ids)}/{product_id}#{row.param_id}"
    return Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=row.param_id,
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        source_kind=SourceKind.REGISTRY,
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
        absence_evidence=[
            {
                "source_id": source_id,
                "source_scope": SourceScope.EXHAUSTIVE.value,
                "registry_ids": list(registry_ids),
            }
        ],
    )


def _presence_cell(
    run_id: str,
    product_id: str,
    row: CatalogRow,
    facts: list[RegistryFact],
) -> tuple[Cell, list[CellEvidence]]:
    """R-ячейка наличия: ✅ + один ``CellEvidence`` на каждый подтверждающий факт (R1)."""
    evidence = [
        _evidence_for(run_id, product_id, row, fact, seq)
        for seq, fact in enumerate(facts)
    ]
    cell = Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=row.param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.REGISTRY,
        absence_rule=row.absence_rule,
        # I1: наличие ≥1 факта (сортированного) → evidence ≥1. Цитата — сырое значение
        # факта; при его отсутствии допускается URL (R1 гарантирует непустоту URL).
        evidence=[
            LLMEvidence(source_id=ev.source_id, quote=ev.citation_quote or ev.source_url or "")
            for ev in evidence
        ]
    )
    return cell, evidence


def _persist(
    store: Any,
    cells: list[Cell],
    evidence: list[CellEvidence],
    facts: list[RegistryFact],
    errors: list[str],
) -> None:
    """Пишет факты/evidence/ячейки; сбой персистенции изолируется (не роняет волну)."""
    if store is None:
        return
    for fact in facts:
        try:
            store.save_registry_fact(fact)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"save_registry_fact: {type(exc).__name__}: {exc}")
    for ev in evidence:
        try:
            store.add_evidence(ev)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"add_evidence: {type(exc).__name__}: {exc}")
    for cell in cells:
        try:
            store.save_cell(cell)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"save_cell: {type(exc).__name__}: {exc}")


def registry_wave(
    plan: Any,
    fetchers: Mapping[str, RegistryFetcherPort],
    store: Any = None,
) -> PhaseResult:
    """Ш3: реестровая волна. Возвращает ``PhaseResult`` с R-ячейками (без LLM).

    :param plan: ``RunPlan`` (продукты, параметры, ``run_id``).
    :param fetchers: реестровые фетчеры ``{registry_key: RegistryFetcherPort}``.
    :param store: опциональный ``StorePort`` (пишет ``registry_facts``/``cell_evidence``/ячейки).
    :raises ValueError: если ``plan``/``fetchers``/``store`` имеют несовместимый тип.
    """
    if not isinstance(fetchers, Mapping):
        raise ValueError("fetchers обязан быть отображением registry_key → RegistryFetcherPort")

    run_id = str(getattr(plan, "run_id", ""))
    registry_ids = resolve_registry_ids(plan, fetchers)
    covered = registry_covered_params(plan, registry_ids)

    products = sorted(str(pid) for pid in getattr(plan, "products", []))
    all_cells: list[Cell] = []
    all_evidence: list[CellEvidence] = []
    all_facts: list[RegistryFact] = []
    errors: list[str] = []

    if not covered:
        # Реестров нет либо ни один параметр не привязан к роли — волна пуста.
        return PhaseResult(status="ok", phase="registry", errors=errors)

    for product_id in products:
        for row in covered:
            facts, had_error, had_r1_violation = _collect_facts(
                run_id, product_id, row, fetchers, errors
            )
            for fact in facts:
                all_facts.append(fact)
            if facts:
                cell, evidence = _presence_cell(run_id, product_id, row, facts)
                all_cells.append(cell)
                all_evidence.extend(evidence)
            elif (
                not had_error
                and not had_r1_violation
                and row.absence_rule is AbsenceRuleType.REGISTRY_ABSENCE
            ):
                # Реестр опрошен, все фетчеры ответили пусто → авторитетное ❌.
                all_cells.append(_absence_cell(run_id, product_id, row, registry_ids))
            # Иначе: реестр не даёт права на отрицание (R12) — ячейка не производится.

    _persist(store, all_cells, all_evidence, all_facts, errors)
    return PhaseResult(
        status="ok",
        phase="registry",
        cells=all_cells,
        errors=errors,
    )
