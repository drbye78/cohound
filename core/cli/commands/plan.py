"""``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

**Зачем плану оси краула, SERP и все флаги идентичности.** ``plan`` существует ради
ответа «чем я измеряю» **до** прогона: он печатает ``run_id`` и слагаемые контракта.
Поэтому он принимает те же входы, что ``run``/``crawl``, чьи значения входят в
идентичность прогона:

* операторские оси краула (:data:`core.cli.execution_axes.CRAWL_AXIS_DESTS`) —
  :func:`core.cli.execution_axes.add_crawl_axis_flags`, значения через
  ``resolve_declared_axes`` → ``crawl_control_digest``;
* ``--serp-rounds`` — :func:`core.cli.execution_axes.add_serp_rounds_flag`, значения через
  ``resolve_serp_rounds``;
* ``--retrieval-profile``/``--cutoff-k``
  (:data:`core.cli.execution_axes.CONTRACT_FLAGS`, материал ``contract``) —
  :func:`add_contract_flags`, значения в
  ``build_execution_contract(retrieval_profile=…, cutoff_K=…)`` тем же способом, каким их
  передаёт ``run``;
* ``--urls``, ``--cycle-id``, ``--update``, ``--only-failed``, ``--only-product``,
  ``--refresh`` — **тот же** инвентарь (иные материалы), значения через
  :func:`core.cli.execution_axes.identity_flag_values` и **те же** чистые резолверы, что у
  прогона: ``seeds_digest`` (:func:`~core.pipeline.onboarding.load_effective_seeds` с
  ``urls_override``), ``cycle_id`` (``resolve_cycle_id``), ``run_scope``
  (``resolve_run_scope``), ``refresh_policy`` (``resolve_refresh_policy``).

Второй формулы нет: иначе напечатанный ``run_id`` не описывал бы прогон, который можно
запустить. Паритет флагов ``run``/``plan`` закреплён тестами, читающими **собранные**
парсеры (``tests/cli/test_plan_crawl_axes.py``, ``tests/cli/test_plan_contract_flags.py``).
``plan`` повторяет и **отказы**, которые прогон делает на этих входах
(``--update``+``--only-failed``, ``--only-failed``+``--only-product``, ``--update`` с
прежним циклом): предсказание «прогон, который не стартует» — тоже ложь об идентичности.

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
    _crawl_policy_resolution,
    _load_corpus_urls,
    _plan_payload,
    _previous_cycle_id,
    _print_json,
    _print_plan,
)
from core.cli.errors import EXIT_OK, EXIT_USAGE, CliError, LintFailure
from core.cli.execution_axes import (
    DeclaredExecutionAxes,
    IdentityFlags,
    add_contract_flags,
    add_crawl_axis_flags,
    add_frozen_source_flag,
    add_refresh_scope_flag,
    add_serp_rounds_flag,
    contract_flag_values,
    declared_status_dict_version,
    identity_flag_values,
    resolve_declared_axes,
)
from core.cli.wiring import llm_env_notices, serp_env_notices
from core.config.deploy import load_deploy_config, profile_notices, shard_notices
from core.pipeline.crawl_policy import CrawlPolicyConflict
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.plan import plan as build_plan
from core.pipeline.run_identity import (
    build_execution_contract,
    check_update_scope,
    resolve_cycle_id,
    resolve_refresh_policy,
    resolve_run_scope,
)


def _declared_payload(
    declared: DeclaredExecutionAxes,
    contract: str,
    contract_flags: dict[str, Any],
    *,
    cycle_id: str,
    run_scope: str,
    refresh_policy: str,
) -> dict[str, Any]:
    """Слагаемые идентичности, которые план обязан назвать: «чем я измеряю».

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

    ``deploy_profile`` печатается рядом с прочими «чем я измеряю», потому что он —
    **слагаемое контракта** (``local`` и ``split`` исполняют работу в разных зонах и
    паритета между ними нет), а не провенанс хоста.

    ``cycle_id``/``run_scope``/``refresh_policy`` — остальной материал ``run_id``
    (09 §6.2): без них оператор не отличит «план не увидел флаг» от «флаг не влияет».
    """
    return {
        "execution_contract_digest": contract,
        "deploy_profile": declared.deploy_profile,
        "crawl_config_digest": declared.crawl_config_digest,
        "serp_rounds": declared.serp_rounds,
        "serp_source": declared.serp_source,
        "retrieval_profile": contract_flags.get("retrieval_profile"),
        "cutoff_K": contract_flags.get("cutoff_K"),
        "cycle_id": cycle_id,
        "run_scope": run_scope,
        "refresh_policy": refresh_policy,
    }


def _axis_lines(payload: dict[str, Any]) -> list[str]:
    """Человекочитаемые строки «какие оси вошли в ``run_id``» (после ``_print_plan``)."""
    policy = payload.get("crawl_policy") or {}
    policy_lines = [
        f"crawl policy: digest={policy.get('digest') or '—'} "
        f"decisions={len(policy.get('decisions') or ())} "
        f"unmatched={len(policy.get('unmatched') or ())}",
        f"crawl_policy_unmatched: {len(policy.get('unmatched') or ())}",
    ]
    for decision in policy.get("decisions") or ():
        scope = decision.get("host") or "—"
        if decision.get("path"):
            scope += decision["path"]
        policy_lines.append(
            f"crawl policy: {decision['product_id']} [{decision['scope_kind']}] {scope} "
            f"mode={decision['mode']} decision={decision['decision']} "
            f"network={decision.get('network_mode')} corpus={decision.get('corpus_source')} "
            f"reason={decision.get('reason') or '—'} source={decision['source']}"
        )
    for item in policy.get("unmatched_requested") or ():
        policy_lines.append(
            f"crawl_policy_unmatched_requested: {item} "
            "(запрос не совпал ни с одним объявленным корнем)"
        )
    for item in policy.get("unmatched") or ():
        policy_lines.append(
            f"crawl_policy_unmatched: {item['product_id']} [{item['scope_kind']}] "
            f"host={item['host'] or '—'} path={item['path'] or '—'} mode={item['mode']} "
            f"source={item['source']}"
        )
    digest = payload.get("execution_contract_digest")
    if digest is None:
        # ``--run-id``: идентичность задана оператором, контракт не считался — не выдумываем.
        return [
            *policy_lines,
            "contract: — (run_id задан явно, оси не считались)",
        ]
    return [
        *policy_lines,
        f"contract: {digest}",
        f"deploy: profile={payload.get('deploy_profile') or '—'}",
        f"crawl axes: digest={payload.get('crawl_config_digest') or '—'}",
        f"serp: rounds={payload.get('serp_rounds')} "
        f"(source: {payload.get('serp_source') or '—'})",
        f"retrieval: profile={payload.get('retrieval_profile') or '—'} "
        f"cutoff_K={payload.get('cutoff_K') if payload.get('cutoff_K') is not None else '—'}",
        f"identity: cycle={payload.get('cycle_id')} scope={payload.get('run_scope')} "
        f"refresh={payload.get('refresh_policy')}",
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
    notices: list[str] = [
        *profile_notices(deploy_config),
        # F7 (17 §2): пустой шард — объявленный узел без host'ов. Это не ошибка конфига,
        # но и не тишина: «host'ов ноль и работы ноль» оператор обязан увидеть **до**
        # старта, а не выяснять из отсутствия артефактов.
        *shard_notices(getattr(deploy_config.crawl, "shards", None)),
    ]
    declared: DeclaredExecutionAxes | None = None
    contract: str | None = None
    # §13/I16/I20: профиль ретривала и ``cutoff_K`` — слагаемые контракта, значит и
    # ``run_id``. Читаются из единого инвентаря (``CONTRACT_FLAGS``), а не перечисляются
    # здесь: перечисление и было дефектом (``plan`` печатал чужой ``run_id``).
    contract_flags = contract_flag_values(args)
    # 09 §6.2: остальной материал ``run_id`` — seed-набор (``--urls``), цикл
    # (``--cycle-id``/``--update``), объём (``--only-failed``/``--only-product``) и политика
    # наблюдения (``--refresh``). Тот же инвентарь, те же чистые резолверы, что у ``run``:
    # вторая формула разошлась бы с прогоном молча — именно это и было измерено.
    identity_flags: IdentityFlags = identity_flag_values(args)
    try:
        run_scope = resolve_run_scope(
            only_failed=bool(identity_flags.only_failed),
            only_product=identity_flags.only_product,
        )
        refresh_policy = resolve_refresh_policy(
            cli=identity_flags.refresh, run_config=inputs.run
        )
        # Отказ прогона повторяется планом: иначе план печатал бы ``run_id`` прогона,
        # который не стартует (§13 — 09 §6.2).
        check_update_scope(update=bool(identity_flags.update), run_scope=run_scope)
        cycle_id = resolve_cycle_id(
            cli_cycle_id=identity_flags.cycle_id,
            run_config=inputs.run,
            update=bool(identity_flags.update),
            today=_dt.date.today(),
            # Предыдущий цикл нужен только проверкой ``--update`` и читается **без записи**
            # (``mode=ro``): ``plan`` обещает не трогать ``state.db``.
            previous_cycle_id=(
                _previous_cycle_id(args) if identity_flags.update else None
            ),
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    # ``--urls`` заменяет пакетный seed-набор **целиком** → ``seeds_digest`` (поле
    # контракта). Тот же загрузчик и то же значение, что увидит прогон.
    operator_urls = (
        _load_corpus_urls(identity_flags.urls)
        if identity_flags.urls is not None
        else None
    )
    # План 19 §6.1/P1: эффективная политика краула — часть ``run_id``. Резолв печатается
    # всегда (в т.ч. при ``--run-id``), но ничего не исполняет: сеть на этом слое не
    # трогается. Конфликт равной специфичности — ``EXIT_USAGE``, а не «первая запись».
    profiles = getattr(inputs, "domain_profiles", None) or {}
    seed_config, _ = load_effective_seeds(
        args.package,
        inputs.run.products,
        profiles=profiles,
        urls_override=operator_urls,
    )
    try:
        policy_resolution = _crawl_policy_resolution(args, inputs, seed_config)
    except CrawlPolicyConflict as exc:
        raise CliError(
            str(exc), code=exc.code, exit_code=EXIT_USAGE
        ) from exc
    except ValueError as exc:
        # План 19 §5: неразбираемый ``--refresh-scope`` — отказ использования, а не
        # «запрос не совпал»: иначе оператор считает, что обновление запрошено.
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    if args.run_id is not None:
        result = build_plan(inputs, run_id=args.run_id)
    else:
        # Та же идентичность, что у ``run`` с дефолтами (без --update/--refresh):
        # иначе ``plan`` показывал бы иной run_id, чем реальный прогон.
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
            # План 19 §6.1: эффективная политика — то же значение, что у ``run``/``crawl``.
            crawl_policy_digest=policy_resolution.digest,
            # План 19 §6.3: ось ``frozen_source`` канонизируется внутри контракта —
            # ``n/a`` при политике, отличной от ``frozen``.
            frozen_source=getattr(args, "frozen_source", None),
            refresh_policy=refresh_policy,
            # I16/I20: те же аргументы, что передаёт ``run`` (``run.py``), из того же
            # инвентаря флагов. ``None`` — флаг не объявлен: контракт пишет ``null``,
            # а не дефолт Ш6 (``run_id`` описывает объявленное).
            **contract_flags,
        )
        result = build_plan(
            inputs,
            cycle_id=cycle_id,
            cohort_digest=cohort,
            seeds_digest=seeds,
            execution_contract_digest=contract,
            # 09 §6.2: объём и политика наблюдения — материал ``derive_run_id``. Без них
            # ``plan`` считал бы ``full``/``auto`` при объявленных флагах и печатал иной
            # ``run_id``, чем прогон.
            run_scope=run_scope,
            refresh_policy=refresh_policy.value,
        )
    payload = _plan_payload(result)
    # План 19 §P1: план **печатает** резолв («что было бы запрещено/отложено»), но не
    # исполняет его. Несовпавшие строки видимы токеном ``crawl_policy_unmatched`` и в
    # дайджест не входят (no-op правка оператора не даёт ``execution_change``).
    payload["crawl_policy"] = policy_resolution.as_dict()
    payload["crawl_policy_unmatched"] = payload["crawl_policy"]["unmatched"]
    if declared is not None and contract is not None:
        payload.update(
            _declared_payload(
                declared,
                contract,
                contract_flags,
                cycle_id=cycle_id,
                run_scope=run_scope,
                refresh_policy=refresh_policy.value,
            )
        )
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
    # План 19 §5: запрос обновления — тот же флаг и тот же резолв, что у ``run``.
    add_refresh_scope_flag(p_plan)
    # План 19 §P3: источник корпуса при ``frozen`` — тот же регистратор, что у
    # ``run``/``crawl run``; значение входит в контракт (``frozen_source``).
    add_frozen_source_flag(p_plan)
    add_serp_rounds_flag(p_plan)
    # §13/I16/I20 + 09 §6.2: **весь** материал ``run_id``, который приходит флагами
    # (контракт, seed-набор, цикл, объём, политика наблюдения); флаги порождены инвентарём
    # ``CONTRACT_FLAGS`` (не второй список), паритет с ``run``/``crawl`` — в тесте,
    # читающем собранные парсеры.
    add_contract_flags(p_plan)
    _add_common(p_plan)
    _add_llm_slot_flag(p_plan)
    _add_llm_model_flag(p_plan)
    p_plan.set_defaults(handler=run)
