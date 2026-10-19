"""coHound — план прогона (W5/T5.4): scope, роли, когорта, перенормировка весов.

План — детерминированное, воспроизводимое представление того, **что** будет измерено
в прогоне (§4.5): набор модулей (scope), выбранный каталог параметров, роли scope,
веса модулей и их перенормировка на применимое подмножество.

Фокус (``run.focus``) меняет scope и перенормировку весов, но **не методологию** (§4.5):

- ``focus.modules`` задаёт подмножество модулей; пусто → все модули пакета;
- ``focus.params`` дополнительно фильтрует параметры по slug; пусто → все параметры
  выбранных модулей.

Перенормировка: объявленные веса модулей делятся на сумму весов **применимых**
модулей (тех, чьи параметры вошли в план) так, чтобы сумма равнялась 1.0. Округление —
до 6 знаков; остаток округления приписывается последнему по порядку модулю, поэтому
сумма точна в пределах :data:`_ROUND_DIGITS`.

Композиция с линтером: CLI вызывает ``load_inputs → lint.raise_if_blocking → plan``
(см. :func:`load_plan`). Якорь — только ось отчёта/фокус по умолчанию, **не вес**
(§1.6): в расчётах весов он не участвует.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from core.domain.models import CatalogRow, Cohort, GoldenRow, PackageManifest, RunConfig
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import RunInputs, load_inputs
from core.util.hashing import short_hash

__all__ = [
    "RunPlan",
    "plan",
    "load_plan",
    "derive_run_id",
]

#: Точность округления весов при перенормировке (знаки после запятой).
_ROUND_DIGITS = 6


@dataclass
class RunPlan:
    """Воспроизводимый план прогона (§4.5/§1.6).

    :param products: продукты когорты в детерминированном (алфавитном) порядке.
    :param modules: scope-модули (``focus.modules`` либо все модули пакета).
    :param roles: роли scope — объединение ``role_binding`` выбранных параметров.
    :param params: выбранный (focused) каталог, отсортирован по ``param_id``.
    :param module_weights: объявленные веса scope-модулей (из ``package.yaml``).
    :param renormalized_weights: веса, перенормированные на применимые модули (Σ = 1.0).
    :param golden: golden-набор (весь, детерминированно отсортирован).
    :param warnings: предупреждения плана + lint-warnings входов (не блокируют).
    """

    run_id: str
    products: list[str]
    anchor: str
    modules: list[str]
    roles: list[str]
    params: list[CatalogRow]
    module_weights: dict[str, float]
    renormalized_weights: dict[str, float]
    cohort: Cohort
    golden: list[GoldenRow]
    warnings: list[str] = field(default_factory=list)


def derive_run_id(
    inputs: RunInputs,
    *,
    cycle_id: str,
    cohort_digest: str,
    seeds_digest: str,
    execution_contract_digest: str,
    run_scope: str = "full",
    refresh_policy: str = "auto",
) -> str:
    """Детерминированный ``run_id`` от значимых полей измерения (09 §6.2).

    Материал (порядок фиксирован, разделитель — unit separator)::

        case_id, package, anchor, cycle_id, cohort_digest, seeds_digest,
        execution_contract_digest, run_scope, refresh_policy,
        sorted focus.modules, sorted focus.params

    ``snapshot_date`` и ``sorted(products)`` **не** входят: цикл задаёт ``cycle_id``, а
    состав когорты уже зафиксирован ``cohort_digest``. Все дайджесты и ``cycle_id``
    обязательны — молчаливого fallback на «старую» формулу нет.
    """
    run = inputs.run
    material = "\x1f".join(
        (
            run.case_id,
            run.package,
            run.anchor,
            cycle_id,
            cohort_digest,
            seeds_digest,
            execution_contract_digest,
            run_scope,
            refresh_policy,
            ",".join(sorted(run.focus.modules)),
            ",".join(sorted(run.focus.params)),
        )
    )
    return f"{run.case_id}-{short_hash(material, 12)}"


def _declared_weights(package: PackageManifest) -> dict[str, float]:
    """Объявленные веса модулей ``{module_id: weight}`` (порядок фиксирован сортировкой)."""
    return {module.module_id: float(module.weight) for module in package.modules}


def _selected_modules(
    run: RunConfig, declared: dict[str, float]
) -> tuple[list[str], list[str]]:
    """Определяет scope-модули и предупреждения о неизвестных модулях фокуса."""
    focus_modules = sorted(set(run.focus.modules))
    if focus_modules:
        warnings = [
            f"focus.modules: модуль '{module}' отсутствует в package.yaml"
            for module in focus_modules
            if module not in declared
        ]
        return focus_modules, warnings
    modules = sorted(declared)
    warnings = []
    if not modules:
        warnings.append("scope пуст: package.yaml не содержит модулей")
    return modules, warnings


def _selected_params(
    inputs: RunInputs, modules: list[str]
) -> tuple[list[CatalogRow], list[str]]:
    """Фильтрует каталог по scope-модулям и ``focus.params``; собирает предупреждения."""
    module_set = set(modules)
    selected = [
        row
        for row in sorted(inputs.catalog, key=lambda row: row.param_id)
        if row.module in module_set
    ]
    focus_params = sorted(set(inputs.run.focus.params))
    if not focus_params:
        return selected, []

    catalog_ids = {row.param_id for row in inputs.catalog}
    selected_ids = {row.param_id for row in selected}
    warnings: list[str] = []
    for param_id in focus_params:
        if param_id not in catalog_ids:
            warnings.append(f"focus.params: параметр '{param_id}' отсутствует в catalog.csv")
        elif param_id not in selected_ids:
            warnings.append(
                f"focus.params: параметр '{param_id}' вне выбранных focus.modules"
            )
    chosen = [row for row in selected if row.param_id in set(focus_params)]
    return chosen, warnings


def _renormalize(
    declared: dict[str, float], applicable: list[str]
) -> dict[str, float]:
    """Перенормирует объявленные веса на применимые модули (Σ = 1.0).

    ``applicable`` — отсортированный список модулей, чьи параметры вошли в план.
    Если сумма объявленных весов применимых модулей равна нулю (вырожденный пакет),
    применяется равномерное распределение. Остаток округления приписывается
    последнему по порядку модулю, поэтому сумма точна до :data:`_ROUND_DIGITS`.
    """
    if not applicable:
        return {}
    total = sum(declared[module] for module in applicable)
    if total <= 0:
        share = round(1.0 / len(applicable), _ROUND_DIGITS)
        weights = {module: share for module in applicable}
    else:
        weights = {
            module: round(declared[module] / total, _ROUND_DIGITS)
            for module in applicable
        }
    residual = round(1.0 - round(sum(weights.values()), _ROUND_DIGITS), _ROUND_DIGITS)
    if residual:
        last = applicable[-1]
        weights[last] = round(weights[last] + residual, _ROUND_DIGITS)
    return weights


def plan(
    inputs: RunInputs,
    run_id: str | None = None,
    *,
    cycle_id: str | None = None,
    cohort_digest: str | None = None,
    seeds_digest: str | None = None,
    execution_contract_digest: str | None = None,
    run_scope: str = "full",
    refresh_policy: str = "auto",
) -> RunPlan:
    """Строит :class:`RunPlan` из нормализованных входов (§4.5/§1.6).

    Детерминирован: сортировка модулей/параметров/продуктов/ролей/golden и
    перенормировка без зависимости от порядка словарей. Предупреждения плана
    объединены с lint-warnings входов (план **не** блокирует — это делает линтер).

    ``run_id`` обычно передаёт composition root (уже выведенный из
    :func:`derive_run_id`). Если он не задан, ``cycle_id`` и все три дайджеста
    обязательны: ``plan`` выводит идентификатор той же формулой. Молчаливой подмены
    старым материалом нет — отсутствие аргументов поднимает :class:`ValueError`.
    """
    declared = _declared_weights(inputs.package)
    modules, warnings = _selected_modules(inputs.run, declared)
    params, param_warnings = _selected_params(inputs, modules)
    warnings.extend(param_warnings)

    roles = sorted({row.role_binding for row in params if row.role_binding})
    known_roles = {role.role_id for role in inputs.package.roles}
    warnings.extend(
        f"role_binding: роль '{role}' отсутствует в реестре package.yaml"
        for role in roles
        if role not in known_roles
    )

    applicable = sorted({row.module for row in params if row.module in declared})
    renormalized = _renormalize(declared, applicable)

    # Веса есть только у объявленных модулей; неизвестные из focus остаются в scope,
    # но предупреждают и не участвуют в перенормировке (веса им неоткуда взять).
    module_weights = {module: declared[module] for module in modules if module in declared}

    golden = sorted(
        inputs.golden,
        key=lambda row: (row.product_id, row.param_id, row.tier.value, row.case_class),
    )

    warnings.extend(lint_run_inputs(inputs).warnings)

    if run_id is None:
        missing = [
            name
            for name, value in (
                ("cycle_id", cycle_id),
                ("cohort_digest", cohort_digest),
                ("seeds_digest", seeds_digest),
                ("execution_contract_digest", execution_contract_digest),
            )
            if value is None
        ]
        if missing:
            raise ValueError(
                f"plan: для вывода run_id нужны аргументы {missing} "
                "(либо передайте run_id явно)"
            )
        run_id = derive_run_id(
            inputs,
            cycle_id=cycle_id,
            cohort_digest=cohort_digest,
            seeds_digest=seeds_digest,
            execution_contract_digest=execution_contract_digest,
            run_scope=run_scope,
            refresh_policy=refresh_policy,
        )

    return RunPlan(
        run_id=run_id,
        products=sorted(inputs.run.products),
        anchor=inputs.run.anchor,
        modules=modules,
        roles=roles,
        params=params,
        module_weights=module_weights,
        renormalized_weights=renormalized,
        cohort=inputs.cohort,
        golden=golden,
        warnings=warnings,
    )


def load_plan(
    run_yaml_path: str,
    package_dir: str,
    run_id: str | None = None,
    *,
    lint: bool = True,
) -> RunPlan:
    """Композиция CLI: ``load_inputs → lint → plan`` (§D.3/E.1).

    :param lint: если ``True`` (по умолчанию), блокирующие нарушения поднимают
        :class:`ValueError` до построения плана. ``False`` — диагностический обход
        (план без блокировки; использовать осознанно).
    """
    inputs = load_inputs(run_yaml_path, package_dir)
    if lint:
        lint_run_inputs(inputs).raise_if_blocking()
    return plan(inputs, run_id=run_id)
