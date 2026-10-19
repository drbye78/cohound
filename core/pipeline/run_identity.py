"""coHound — идентичность прогона (09 §2.4/§6.2/§12.3): цикл, объём, lineage.

Модуль слоя ``pipeline``: разрешает ``cycle_id``, ``run_scope``, ``refresh_policy``,
собирает execution contract и класс изменения между прогонами. Сам ``run_id`` выводит
:func:`core.pipeline.plan.derive_run_id` — здесь только входные значения и
:class:`RunIdentity` как единый носитель результата.

Разделение ответственности:

* ``cycle_id`` — идентификатор цикла: ``--cycle-id`` > ``run.yaml:cycle_id`` >
  ``run.yaml:snapshot_date`` (без ``--update``). ``--update`` объявляет новый цикл и
  требует значения, отличного от предыдущего; только если ни CLI, ни ``run.yaml`` не
  задали цикл, берётся ``today()`` (иначе retry следующего дня стал бы новым прогоном).
* ``run_scope`` — ``full`` | ``only_failed`` | ``product:<id>``; входит в ``run_id``,
  чтобы scoped-прогон не совпал с полным и не был пропущен как resume.
* ``refresh_policy`` — ``--refresh`` > ``run.yaml:refresh_policy`` > ``auto``.
* ``change_class`` — честная классификация по предыдущему lineage: сначала состав
  когорты (expansion/reduction), затем execution contract, иначе ``none``.

Модуль не читает БД/файлы: все значения приходят аргументами. Нейтрален к предметной
категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Sequence
from dataclasses import dataclass

from core.domain.enums import ChangeClass, RefreshPolicy, RunScope
from core.domain.models import RunConfig
from core.pipeline.digests import execution_contract_digest
from core.pipeline.normalize import RunInputs

__all__ = [
    "RunIdentity",
    "build_execution_contract",
    "build_run_identity",
    "classify_change",
    "golden_required_for",
    "resolve_cycle_id",
    "resolve_refresh_policy",
    "resolve_run_scope",
]


@dataclass(frozen=True)
class RunIdentity:
    """Идентичность прогона: всё, что нужно lineage/dry-run (§12.3).

    :param run_id: детерминированный идентификатор (``derive_run_id``) либо ``--run-id``.
    :param cycle_id: эффективный идентификатор цикла.
    :param previous_run_id: predecessor для carry-forward (``None`` — истории нет/resume).
    :param cohort_digest: дайджест состава когорты.
    :param seeds_digest: дайджест эффективного seed-набора.
    :param execution_contract_digest: дайджест условий измерения.
    :param run_scope: ``full`` | ``only_failed`` | ``product:<id>``.
    :param refresh_policy: политика наблюдения корпуса.
    :param change_class: класс изменения относительно предыдущего прогона.
    :param golden_required: ``none`` | ``fast`` | ``full`` (консервативно ``full`` для
        любого ``execution_change``: по одному дайджесту нельзя назвать изменившееся поле).
    """

    run_id: str
    cycle_id: str
    previous_run_id: str | None
    cohort_digest: str
    seeds_digest: str
    execution_contract_digest: str
    run_scope: str
    refresh_policy: RefreshPolicy
    change_class: ChangeClass
    golden_required: str


def resolve_cycle_id(
    *,
    cli_cycle_id: str | None,
    run_config: RunConfig,
    update: bool,
    today: _dt.date,
    previous_cycle_id: str | None = None,
) -> str:
    """Эффективный ``cycle_id`` (09 §2.4/§6.2).

    Без ``--update``: ``--cycle-id`` > ``run.yaml:cycle_id`` > ``run.yaml:snapshot_date``.
    Это стабильный идентификатор, поэтому повтор после падения resume'ит тот же прогон.

    С ``--update``: новый цикл обязателен. Если ``--cycle-id``/``run.yaml:cycle_id``
    заданы — берутся они; иначе ``today().isoformat()``. Скрытого ``today()`` без
    ``--update`` нет. Если эффективное значение совпало с ``previous_cycle_id``,
    поднимается :class:`ValueError` («``--update`` требует нового cycle_id»).

    :param today: инжектируемая дата (детерминизм тестов); не вызывается, пока цикл
        задан явно.
    """
    explicit = cli_cycle_id or getattr(run_config, "cycle_id", None)
    if not update:
        return explicit or run_config.snapshot_date.isoformat()
    cycle_id = explicit or today.isoformat()
    if previous_cycle_id is not None and cycle_id == previous_cycle_id:
        raise ValueError("--update требует нового cycle_id")
    return cycle_id


def resolve_run_scope(*, only_failed: bool, only_product: str | None) -> str:
    """Объём прогона (09 §6.2): ``full`` | ``only_failed`` | ``product:<id>``.

    ``--only-failed`` и ``--only-product`` взаимоисключающие: неоднозначный work-set
    не должен разрешаться молчаливым приоритетом.
    """
    if only_failed and only_product is not None:
        raise ValueError("--only-failed и --only-product взаимоисключающие")
    if only_failed:
        return RunScope.ONLY_FAILED.value
    if only_product is not None:
        return f"product:{only_product}"
    return RunScope.FULL.value


def resolve_refresh_policy(
    *, cli: str | None, run_config: RunConfig
) -> RefreshPolicy:
    """Политика наблюдения корпуса: ``--refresh`` > ``run.yaml`` > ``auto``."""
    if cli:
        return RefreshPolicy(cli)
    configured = getattr(run_config, "refresh_policy", None)
    return RefreshPolicy(configured) if configured is not None else RefreshPolicy.AUTO


def build_execution_contract(
    inputs: RunInputs,
    *,
    cohort_digest: str,
    seeds_digest: str,
    retrieval_profile: str | None = None,
    cutoff_K: int | None = None,
    retrieval_params_json: str | None = None,
    status_dict_version: str | None = None,
    scoring_policy_version: str | None = None,
    catalog_version: str | None = None,
) -> str:
    """Собирает execution contract из честно доступных значений и хеширует его.

    Сегодня **заполняются**: ``methodology_version`` (``run.yaml``), ``package_version``
    (``package.yaml``), ``cohort_digest``, ``seeds_digest``, ``retrieval_profile``/
    ``cutoff_K``/``retrieval_params_json`` (если передал composition root). Версии
    ``status_dict_version``/``scoring_policy_version``/``catalog_version`` принимаются
    аргументами, но в этом срезе composition root их не передаёт — ``None``.

    Сегодня **``None``** (проводка — следующие срезы): ``llm_scenarios``,
    ``chunker_version``, ``extractor_version``, ``embed_model_id``,
    ``lexical_engine_version``, ``vector_store``, ``crawl_strategy``,
    ``crawl_rate_limit``, ``crawl_config_digest``, ``registry_spec_digest``,
    ``deploy_profile``, ``edge_schema_version``. Это осознанная неполнота: хешировать
    только run-слой краула, не зная app/site-дефолтов, значило бы соврать о контракте.
    ``None`` — часть контракта (см. :func:`core.pipeline.digests.execution_contract_digest`).
    """
    values = {
        "methodology_version": inputs.run.methodology_version,
        "scoring_policy_version": scoring_policy_version,
        "status_dict_version": status_dict_version,
        "catalog_version": catalog_version,
        "package_version": inputs.package.package_version,
        "cohort_digest": cohort_digest,
        "seeds_digest": seeds_digest,
        "llm_scenarios": None,
        "retrieval_profile": retrieval_profile,
        "cutoff_K": cutoff_K,
        "retrieval_params_json": retrieval_params_json,
        "chunker_version": None,
        "extractor_version": None,
        "embed_model_id": None,
        "lexical_engine_version": None,
        "vector_store": None,
        "crawl_strategy": None,
        "crawl_rate_limit": None,
        "crawl_config_digest": None,
        "registry_spec_digest": None,
        "deploy_profile": None,
        "edge_schema_version": None,
    }
    return execution_contract_digest(values)


def classify_change(
    *,
    previous_run_id: str | None,
    previous_products: Sequence[str] | None,
    current_products: Sequence[str],
    previous_execution_contract_digest: str | None,
    execution_contract_digest: str,
) -> ChangeClass:
    """Класс изменения относительно предыдущего прогона (09 §12.1, O9).

    Матрица:

    * нет предыдущего прогона → ``none``;
    * состав когорты вырос → ``cohort_expansion``; уменьшился → ``cohort_reduction``
      (проверяется первым: ``cohort_digest`` входит в contract, и без приоритета
      расширение выглядело бы просто ``execution_change``);
    * иначе contract отличается → ``execution_change``;
    * иначе → ``none``.

    ``previous_products is None`` означает «состав неизвестен» (legacy-строка без
    ``products_json``), а не пустой набор: сравнение тогда пропускается.
    """
    if previous_run_id is None:
        return ChangeClass.NONE
    if previous_products is not None:
        previous = set(previous_products)
        current = set(current_products)
        if current - previous:
            return ChangeClass.COHORT_EXPANSION
        if previous - current:
            return ChangeClass.COHORT_REDUCTION
    if (
        previous_execution_contract_digest is not None
        and previous_execution_contract_digest != execution_contract_digest
    ):
        return ChangeClass.EXECUTION_CHANGE
    return ChangeClass.NONE


def golden_required_for(change_class: ChangeClass) -> str:
    """Требуемый golden-набор для класса изменения (09 §12.1).

    ``none``/``content_change`` → ``none``; ``cohort_*``/``methodology_change`` →
    ``full``. Для ``execution_change`` консервативно ``full``: по одному дайджесту
    нельзя отличить смену модели/каталога (``full``) от смены seed'ов/ретривала
    (``fast``), поэтому берётся более строгое требование.
    """
    if change_class in (
        ChangeClass.COHORT_EXPANSION,
        ChangeClass.COHORT_REDUCTION,
        ChangeClass.EXECUTION_CHANGE,
        ChangeClass.METHODOLOGY_CHANGE,
    ):
        return "full"
    return "none"


def build_run_identity(
    *,
    run_id: str,
    cycle_id: str,
    previous_run_id: str | None,
    previous_products: Sequence[str] | None,
    previous_execution_contract_digest: str | None,
    cohort_digest: str,
    seeds_digest: str,
    execution_contract_digest: str,
    run_scope: str,
    refresh_policy: RefreshPolicy,
    current_products: Sequence[str],
) -> RunIdentity:
    """Собирает :class:`RunIdentity`, вычисляя ``change_class``/``golden_required``.

    ``current_products`` — продукты плана текущего прогона (для сравнения состава).
    Без предыдущего прогона класс — ``none``; ``golden_required`` из него — ``none``.
    """
    change_class = classify_change(
        previous_run_id=previous_run_id,
        previous_products=previous_products,
        current_products=current_products,
        previous_execution_contract_digest=previous_execution_contract_digest,
        execution_contract_digest=execution_contract_digest,
    )
    return RunIdentity(
        run_id=run_id,
        cycle_id=cycle_id,
        previous_run_id=previous_run_id,
        cohort_digest=cohort_digest,
        seeds_digest=seeds_digest,
        execution_contract_digest=execution_contract_digest,
        run_scope=run_scope,
        refresh_policy=refresh_policy,
        change_class=change_class,
        golden_required=golden_required_for(change_class),
    )
