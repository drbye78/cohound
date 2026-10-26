"""``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

**Зачем плану оси краула, SERP и контрактные флаги ретривала.** ``plan`` существует ради
ответа «чем я измеряю» **до** прогона: он печатает ``run_id`` и слагаемые контракта.
Поэтому он принимает те же входы, что ``run``/``crawl``, которые входят в
``execution_contract_digest``:

* операторские оси краула (:data:`core.cli.execution_axes.CRAWL_AXIS_DESTS`) —
  :func:`core.cli.execution_axes.add_crawl_axis_flags`, значения через
  ``resolve_declared_axes`` → ``crawl_control_digest``;
* ``--serp-rounds`` — :func:`core.cli.execution_axes.add_serp_rounds_flag`, значения через
  ``resolve_serp_rounds``;
* ``--retrieval-profile``/``--cutoff-k``
  (:data:`core.cli.execution_axes.CONTRACT_FLAGS`) — :func:`add_contract_flags`, значения в
  ``build_execution_contract(retrieval_profile=…, cutoff_K=…)`` тем же способом, каким их
  передаёт ``run``.

Второй формулы нет: иначе напечатанный ``run_id`` не описывал бы прогон, который можно
запустить. Паритет флагов ``run``/``plan`` закреплён тестами, читающими **собранные**
парсеры (``tests/cli/test_plan_crawl_axes.py``, ``tests/cli/test_plan_contract_flags.py``).

Оси, объявленные в ``run.yaml`` (``crawl.*``, ``serp.*``), план видит и без флагов — они
приходят из ``load_inputs``. Флаг ``--dry-run`` удалён: он ничего не менял (``plan`` и так
ничего не пишет) и намекал на существование «не-dry» режима.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import sys
from typing import Any

from core.cli.context import (
    _add_common,
    _add_llm_model_flag,
    _add_llm_slot_flag,
    _plan_payload,
    _print_json,
    _print_plan,
)
from core.cli.errors import EXIT_OK, EXIT_USAGE, CliError, LintFailure
from core.cli.execution_axes import (
    DeclaredExecutionAxes,
    add_contract_flags,
    add_crawl_axis_flags,
    add_serp_rounds_flag,
    contract_flag_values,
    declared_status_dict_version,
    resolve_declared_axes,
)
from core.cli.wiring import llm_env_notices, serp_env_notices
from core.config.deploy import load_deploy_config, profile_notices
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.plan import plan as build_plan
from core.pipeline.run_identity import build_execution_contract, resolve_cycle_id


def _declared_payload(
    declared: DeclaredExecutionAxes, contract: str, contract_flags: dict[str, Any]
) -> dict[str, Any]:
    """Слагаемые контракта, которые план обязан назвать: «чем я измеряю».

    ``execution_contract_digest`` → ``run_id``; ``crawl_config_digest`` — свёртка
    разрешённых осей краула (app < run < site + операторские флаги), поэтому оператор
    видит, что флаги/``run.yaml`` действительно вошли в идентичность, а не потерялись.
    ``serp_rounds``/``serp_source`` — контур поиска и **уровень**, который его задал
    (``cli``/``env``/``run.yaml``/``deploy.yaml``/``default``): «0 раундов» без источника
    читается как «поиск не нужен», а не как «env его выключил».

    ``retrieval_profile``/``cutoff_K`` — из :data:`core.cli.execution_axes.CONTRACT_FLAGS`
    (имена — как у полей ``EXECUTION_CONTRACT_FIELDS``). Печатаются отдельно от дайджеста,
    потому что иначе «какой профиль/вырез поехал в ``run_id``» читается только разбором
    хеша. ``None`` — флаг не объявлен: план не подставляет дефолт Ш6, иначе показывал бы
    значение, которого в измерении нет.
    """
    return {
        "execution_contract_digest": contract,
        "crawl_config_digest": declared.crawl_config_digest,
        "serp_rounds": declared.serp_rounds,
        "serp_source": declared.serp_source,
        "retrieval_profile": contract_flags.get("retrieval_profile"),
        "cutoff_K": contract_flags.get("cutoff_K"),
    }


def _axis_lines(payload: dict[str, Any]) -> list[str]:
    """Человекочитаемые строки «какие оси вошли в ``run_id``» (после ``_print_plan``)."""
    digest = payload.get("execution_contract_digest")
    if digest is None:
        # ``--run-id``: идентичность задана оператором, контракт не считался — не выдумываем.
        return ["contract: — (run_id задан явно, оси не считались)"]
    return [
        f"contract: {digest}",
        f"crawl axes: digest={payload.get('crawl_config_digest') or '—'}",
        f"serp: rounds={payload.get('serp_rounds')} "
        f"(source: {payload.get('serp_source') or '—'})",
        f"retrieval: profile={payload.get('retrieval_profile') or '—'} "
        f"cutoff_K={payload.get('cutoff_K') if payload.get('cutoff_K') is not None else '—'}",
    ]


def run(args: argparse.Namespace) -> int:
    """``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

    План чисто вычислим и **ничего не пишет** — ни ``runs/``, ни пакета. Блокирующий линт
    прерывает построение плана с кодом правила.

    Оси краула/SERP и контрактные флаги ретривала считаются тем же резолвером/теми же
    аргументами, что у ``run`` (:func:`core.cli.execution_axes.resolve_declared_axes`,
    :func:`core.cli.execution_axes.contract_flag_values`): контракт плана и прогона — одна
    функция входа (§13), иначе «план» описывал бы другой прогон.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    if not report.ready:
        raise LintFailure([*report.blocking, *report.incomplete], report.warnings)

    # D-8: профиль развёртывания резолвится и для явного ``--run-id``: предупреждение о
    # ``split-hardened`` (принимается, но ничего не добавляет сверх ``split``) должно быть
    # видно всегда, когда этот профиль активен, а не только при выводе ``run_id``.
    deploy_config = load_deploy_config(None)
    notices: list[str] = list(profile_notices(deploy_config))
    declared: DeclaredExecutionAxes | None = None
    contract: str | None = None
    # §13/I16/I20: профиль ретривала и ``cutoff_K`` — слагаемые контракта, значит и
    # ``run_id``. Читаются из единого инвентаря (``CONTRACT_FLAGS``), а не перечисляются
    # здесь: перечисление и было дефектом (``plan`` печатал чужой ``run_id``).
    contract_flags = contract_flag_values(args)
    if args.run_id is not None:
        result = build_plan(inputs, run_id=args.run_id)
    else:
        # Та же идентичность, что у ``run`` с дефолтами (без --update/--refresh):
        # иначе ``plan`` показывал бы иной run_id, чем реальный прогон.
        profiles = getattr(inputs, "domain_profiles", None) or {}
        seed_config, _ = load_effective_seeds(
            args.package, inputs.run.products, profiles=profiles
        )
        cohort = cohort_digest(
            inputs.cohort,
            case_id=inputs.run.case_id,
            cohort_ref=getattr(inputs.run, "cohort_ref", None),
        )
        seeds = seeds_digest(seed_config)
        # §13 (2.4.0): декларированные оси — тот же резолвер и тот же deploy-конфиг, что
        # у ``run``/``crawl``. Иначе ``plan`` показывал бы иной ``run_id``, чем реальный
        # прогон: контракт перестал быть функцией только ``run.yaml``.
        # §7.4/I21: env-настройки модели оценщика не читаются — предупреждение видно в
        # payload ``plan`` (identity-preview), а не только в логе прогона.
        notices.extend(
            llm_env_notices(
                deploy_llm=getattr(deploy_config, "llm", None),
                slot=getattr(args, "llm_slot", None),
                run_slot=getattr(getattr(inputs, "run", None), "llm_slot", None),
            )
        )
        # I3/§5: env-троттл SERP-контура виден и в плане — он меняет ``serp_rounds``, то
        # есть ``run_id``; молчаливая подмена здесь была бы подменой измерения.
        notices.extend(
            serp_env_notices(
                deploy_search=getattr(deploy_config, "search", None),
                run_rounds=getattr(
                    getattr(getattr(inputs, "run", None), "serp", None), "rounds", None
                ),
                cli_rounds=getattr(args, "serp_rounds", None),
            )
        )
        try:
            declared = resolve_declared_axes(
                args=args, inputs=inputs, deploy=deploy_config
            )
        except ValueError as exc:
            raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
        contract = build_execution_contract(
            inputs,
            cohort_digest=cohort,
            seeds_digest=seeds,
            declared=declared,
            status_dict_version=declared_status_dict_version(),
            # I16/I20: те же аргументы, что передаёт ``run`` (``run.py``), из того же
            # инвентаря флагов. ``None`` — флаг не объявлен: контракт пишет ``null``,
            # а не дефолт Ш6 (``run_id`` описывает объявленное).
            **contract_flags,
        )
        result = build_plan(
            inputs,
            cycle_id=resolve_cycle_id(
                cli_cycle_id=None,
                run_config=inputs.run,
                update=False,
                today=_dt.date.today(),
            ),
            cohort_digest=cohort,
            seeds_digest=seeds,
            execution_contract_digest=contract,
        )
    payload = _plan_payload(result)
    if declared is not None and contract is not None:
        payload.update(_declared_payload(declared, contract, contract_flags))
    if notices:
        payload["warnings"] = list(notices)
    if args.json:
        _print_json({"ok": True, **payload})
    else:
        _print_plan(payload)
        for line in _axis_lines(payload):
            sys.stdout.write(line + "\n")
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``plan``."""
    p_plan = sub.add_parser(
        "plan", help="нормализация входов + линт + план прогона (без записи)"
    )
    p_plan.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    p_plan.add_argument("--package", required=True, help="каталог пакета категории")
    p_plan.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    # §13/AGENTS «не менять ось краула на месте»: у ``plan`` те же оси, что у ``run``, —
    # один инвентарь (``CRAWL_AXIS_DESTS``), а паритет флагов закреплён тестом.
    add_crawl_axis_flags(p_plan)
    add_serp_rounds_flag(p_plan)
    # §13/I16/I20: профиль ретривала и ``cutoff_K`` — слагаемые ``run_id``; флаги порождены
    # инвентарём ``CONTRACT_FLAGS`` (не второй список), паритет с ``run`` — в тесте.
    add_contract_flags(p_plan)
    _add_common(p_plan)
    _add_llm_slot_flag(p_plan)
    _add_llm_model_flag(p_plan)
    p_plan.set_defaults(handler=run)
