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
  Несовместимость ``--update`` с ``only_failed`` проверяется здесь же
  (:func:`check_update_scope`) — одна формула на ``run`` и ``plan``.
* ``refresh_policy`` — ``--refresh`` > ``run.yaml:refresh_policy`` > ``auto``.
* ``change_class`` — честная классификация по предыдущему lineage: сначала состав
  когорты (expansion/reduction), затем execution contract, иначе ``none``.

Модуль не читает БД/файлы: все значения приходят аргументами. Нейтрален к предметной
категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from core.domain.enums import ChangeClass, RefreshPolicy, RunScope
from core.domain.models import RunConfig
from core.pipeline.digests import (
    catalog_digest,
    execution_contract_digest,
)
from core.pipeline.digests import (
    scoring_policy_version as _scoring_policy_identity,
)
from core.pipeline.normalize import RunInputs
from core.pipeline.refresh_modes import canonical_frozen_source
from core.ports.state import RunLineageRow
from core.util.hashing import content_hash

__all__ = [
    "DeclaredAxesLike",
    "RunIdentity",
    "build_execution_contract",
    "build_run_identity",
    "check_update_scope",
    "classify_change",
    "golden_required_for",
    "previous_run_of",
    "resolve_cycle_id",
    "resolve_refresh_policy",
    "resolve_run_scope",
]


class DeclaredAxesLike(Protocol):
    """Структурный тип объявленных осей исполнения (§13).

    ``pipeline`` **не** импортирует ``cli`` (слои): конкретный носитель —
    :class:`core.cli.execution_axes.DeclaredExecutionAxes` (composition root). Здесь
    описан только контракт доступа: девять объявленных осей, без наблюдаемых величин.
    """

    crawl_strategy: str | None
    crawl_rate_limit: str | None
    crawl_config_digest: str | None
    #: I3/§5: число раундов SERP-контура (0 — контур выключен). Объявленная ось: раунд
    #: добавляет документы в корпус, поэтому входит в контракт, а не в провенанс.
    serp_rounds: int | None
    #: I3/§5: режим запроса внешнего поиска (``sync``/``async``). Условие измерения:
    #: два бэкенда одного API дают разный состав находок, значит разный корпус.
    serp_mode: str | None
    registry_spec_digest: str | None
    deploy_profile: str | None
    edge_schema_version: str | None
    chunker_version: str | None
    #: C3: геометрия нарезки + состав входа эмбеддера (``composite_chunking_spec``).
    #: Отдельно от ``chunker_version``: метка не выводится из геометрии, поэтому правка
    #: ``chunk_size`` проходила под прежним ``run_id``.
    chunking_spec: dict[str, Any] | None
    #: V3: политика повтора оценщика (``max_retries``). Повтор переиздаёт запрос, значит
    #: вердикт зависит от истории сэмплов — это условие измерения.
    evaluation_policy: dict[str, Any] | None
    llm_scenarios: tuple[dict[str, Any], ...]
    extractor_stack: str
    #: 16-runs-schema-boundary §4.2: устройство эмбеддера (``cpu`` | ``cuda``). Место
    #: счёта — условие измерения: векторы CPU и CUDA численно разные, поэтому один
    #: ``run_id`` не может описывать оба исполнения. ``None`` — ось не передана.
    embed_device: str | None = None
    #: 16 §4.1 (решение владельца 06.11.2026): точность счёта вектора (``fp32`` | ``fp16``)
    #: — отдельное условие измерения, а не свойство устройства: fp16- и fp32-векторы одной
    #: модели на одном устройстве численно разные. ``None`` — ось не передана.
    embed_precision: str | None = None
    #: 13-manual-artifacts §3.2: отпечаток объявленных ручных артефактов. Раунд подачи
    #: добавляет документы корпуса, то есть доказательства, — это **условие измерения**,
    #: а не провенанс записи: спрятать его значило бы дать корпусу измениться без
    #: изменения ``run_id``.
    manual_artifacts_digest: str | None = None
    #: §11.1: объявленные входы **корпуса**, не входившие в идентичность: per-URL режим
    #: рендера (``--render-modes``) и содержимое паспортов (``--passports-dir``). Оба
    #: определяют, какие байты станут доказательствами, поэтому это условия измерения, а
    #: не провенанс хоста. ``--urls`` здесь **отсутствует** осознанно: он заменяет
    #: seed-набор и уже входит в ``seeds_digest`` — второе поле на тот же материал
    #: нарушило бы правило «одно поле — один материал».
    render_modes_digest: str | None = None
    passports_digest: str | None = None
    #: C8: объявленная схема лексического индекса (состав полей + цепочка анализатора).
    #: Меняет лексическую выдачу, значит вердикты; версия движка сюда не входит.
    lexical_spec: dict[str, Any] | None = None


def _declared(declared: DeclaredAxesLike | None, name: str) -> Any:
    """Значение объявленной оси (``None`` — оси не переданы: legacy/чистый вызов)."""
    return getattr(declared, name, None) if declared is not None else None


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


def check_update_scope(*, update: bool, run_scope: str) -> None:
    """``--update`` несовместим с ``only_failed`` — одна проверка на ``run`` и ``plan``.

    Причина запрета — 09 §6.2: «новый цикл по уже проваленному подмножеству» не описывает
    измерения (виден только хвост корпуса, а цикл объявляется новым целиком). Проверка
    живёт здесь, а не в composed root, потому что ``plan`` обязан воспроизводить **отказ**
    прогона: иначе он печатал бы ``run_id`` прогона, который не стартует, — та же ошибка
    предсказания, что и расхождение по ``run_id``.

    :raises ValueError: ``--update`` объявлен вместе с ``--only-failed``.
    """
    if update and run_scope == RunScope.ONLY_FAILED.value:
        raise ValueError("--update несовместим с --only-failed (09 §6.2)")


def resolve_refresh_policy(
    *, cli: str | None, run_config: RunConfig
) -> RefreshPolicy:
    """Политика наблюдения корпуса: ``--refresh`` > ``run.yaml`` > ``auto``."""
    if cli:
        return RefreshPolicy(cli)
    configured = getattr(run_config, "refresh_policy", None)
    return RefreshPolicy(configured) if configured is not None else RefreshPolicy.AUTO


def _prompt_template_identity() -> str:
    """``имя_шаблона@хеш_содержимого`` — условие измерения, а не провенанс (§7.1).

    Зачем хеш: один ``TEMPLATE_VERSION`` не описывает текст. Правка шаблона меняет
    вердикты (``v3`` перестал выдумывать ``❌``, ``v4`` вернул различение частичного
    утверждения), и без хеша такая правка прошла бы под прежним ``run_id``. Файл может
    отсутствовать (шаблон подменён в процессе пробы) — тогда честно называется только
    имя: выдумывать хеш нечего.
    """
    from core.domain.models import LLMCellOutput
    from core.pipeline.prompt import ABSENCE_GUIDANCE, TEMPLATE_VERSION, _load_template

    try:
        text = _load_template(TEMPLATE_VERSION)
    except OSError:
        # Шаблон подменён в процессе пробы (файла нет) — честно называется только имя.
        return TEMPLATE_VERSION
    # **Композиция, а не файл.** Отрендеренный промпт собирается из трёх текстов:
    # шаблона, подсказки по правилу отсутствия (``ABSENCE_GUIDANCE`` живёт в коде
    # ``prompt.py``) и JSON-схемы ответа. Хешировать только файл значило бы дать правке
    # подсказки изменить промпт у всех ячеек, не сменив ``run_id`` — ровно тот класс,
    # что ``chunker_version`` без геометрии (C3). Схема входит потому, что она задаёт,
    # какой ответ вообще считается заявленным.
    payload = {
        "template": text,
        "guidance": {rule.value: hint for rule, hint in ABSENCE_GUIDANCE.items()},
        "schema": LLMCellOutput.model_json_schema(),
    }
    composite = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{TEMPLATE_VERSION}@{content_hash(composite)[:12]}"

def build_execution_contract(
    inputs: RunInputs,
    *,
    cohort_digest: str,
    seeds_digest: str,
    declared: DeclaredAxesLike | None = None,
    retrieval_profile: str | None = None,
    cutoff_K: int | None = None,
    retrieval_params_json: str | None = None,
    status_dict_version: str | None = None,
    scoring_policy_version: str | None = None,
    catalog_version: str | None = None,
    crawl_policy_digest: str | None = None,
    frozen_source: str | None = None,
    refresh_policy: RefreshPolicy | str | None = None,
) -> str:
    """Собирает execution contract из честно доступных значений и хеширует его.

    **Декларированный слой** (``declared``) заполняется composition root
    (:func:`core.cli.execution_axes.resolve_declared_axes`): оси краула, дайджесты
    контроля краула/реестров, деплой-профиль, версия seam-схемы, версия чанкера,
    таблица сценариев LLM и стек извлечения. Это «функция входа»: значения вычислимы
    из ``args``/``run.yaml``/файлов пакета/``deploy.yaml``/констант кода **до** сборки
    портов, поэтому два хоста с одинаковыми входами дают один ``run_id`` (R7 §11.7).

    **Наблюдаемые величины портов/диска** (``extractor_version``/``embed_model_id``/
    ``lexical_engine_version``/``vector_store``) в контракт **не** входят (методология
    2.4.0, §13): они остаются провенансом манифеста/``pb``, но не блокируют diff —
    их нельзя объявить на входе, а хеширование сделало бы ``run_id``
    хост-зависимым. Вместо ``extractor_version`` объявлен ``extractor_stack``.

    Прочие поля: ``methodology_version`` (``run.yaml``), ``package_version``
    (``package.yaml``), ``cohort_digest``, ``seeds_digest``, ретривал-профиль/K/params
    (передаёт composition root после нормализации args). ``status_dict_version`` —
    из словаря статусов. ``catalog_version``/``scoring_policy_version`` при ``None``
    выводятся из объявленных входов: отпечаток содержимого ``inputs.catalog``
    (:func:`core.pipeline.digests.catalog_digest`) и политика расчёта §6
    (:func:`core.pipeline.digests.scoring_policy_version`). До 29.10.2026 источника у
    них не было, и в контракт уходило ``None``, а в манифест ``"0.0.0"`` — смена
    каталога не меняла ``run_id``. ``None`` остаётся только у «входа нет вовсе»
    (stub-объект без ``catalog``), и это отличимо от «каталог объявлен пустым».

    Функция **чистая**: не читает БД/файлы, все значения приходят аргументами.
    """
    # 09 §12.5: ``catalog_version``/``scoring_policy_version`` были полями контракта без
    # источника — в контракт уходило ``None``, в манифест ``"0.0.0"``, и смена каталога
    # или политики расчёта не меняла ``run_id`` (два одинаковых ``"0.0.0"`` в diff).
    # Источник теперь настоящий: отпечаток содержимого каталога и объявленная политика
    # §6. Оба вычислимы из ``inputs``/констант кода до сборки портов, поэтому «функция
    # входа» сохраняется. Аргументы остаются переопределением (тесты, будущий слой
    # ``run.yaml``): явно переданное значение не перезаписывается.
    if catalog_version is None:
        catalog_rows = getattr(inputs, "catalog", None)
        catalog_version = (
            catalog_digest(catalog_rows) if catalog_rows is not None else None
        )
    if scoring_policy_version is None:
        scoring_policy_version = _scoring_policy_identity()
    values = {
        "methodology_version": inputs.run.methodology_version,
        "scoring_policy_version": scoring_policy_version,
        "status_dict_version": status_dict_version,
        "catalog_version": catalog_version,
        "package_version": inputs.package.package_version,
        "cohort_digest": cohort_digest,
        "seeds_digest": seeds_digest,
        "llm_scenarios": _declared(declared, "llm_scenarios"),
        "retrieval_profile": retrieval_profile,
        "cutoff_K": cutoff_K,
        "retrieval_params_json": retrieval_params_json,
        "chunker_version": _declared(declared, "chunker_version"),
        # C3/V3: геометрия нарезки и политика повтора. ``None`` (ось не передана) —
        # честное «не объявлено» для чистых вызовов; живой прогон передаёт обе.
        "chunking_spec": _declared(declared, "chunking_spec"),
        "evaluation_policy": _declared(declared, "evaluation_policy"),
        # §11.1: входы корпуса. ``None`` — ось не передана (чистые вызовы): «не
        # объявлено» остаётся отличимым от «объявлено пустое множество».
        "render_modes_digest": _declared(declared, "render_modes_digest"),
        "passports_digest": _declared(declared, "passports_digest"),
        "lexical_spec": _declared(declared, "lexical_spec"),
        "extractor_stack": _declared(declared, "extractor_stack"),
        # 16-runs-schema-boundary §4.2: устройство эмбеддера. Объявленная ось
        # (``deploy.yaml:embed.device`` < ``run.yaml:embed.device``), вычислима до сборки
        # портов, поэтому остаётся «функцией входа». ``None`` — ось не передана (чистые
        # вызовы): «не объявлено» отличимо от «объявлено ``cpu``».
        "embed_device": _declared(declared, "embed_device"),
        # 16 §4.1: точность — тем же правом, что устройство. Объявленная ось
        # (``deploy.yaml:embed.precision`` < ``run.yaml:embed.precision``), вычислима до
        # сборки портов, поэтому остаётся «функцией входа». ``None`` — ось не передана
        # (чистые вызовы): «не объявлено» отличимо от «объявлено ``fp32``».
        "embed_precision": _declared(declared, "embed_precision"),
        "crawl_strategy": _declared(declared, "crawl_strategy"),
        "crawl_rate_limit": _declared(declared, "crawl_rate_limit"),
        "crawl_config_digest": _declared(declared, "crawl_config_digest"),
        # План 19 §6.1: эффективная поверхность решений политики краула. Приходит
        # аргументом от composition root (резолв требует объявленных корней и часов):
        # ``None`` — «не разрешалась» (чистые вызовы), отличимо от пустой поверхности.
        "crawl_policy_digest": crawl_policy_digest,
        # План 19 §6.3: ось ``frozen_source`` входит в материал **только** при
        # ``refresh_policy=frozen``; иначе канонизируется в ``n/a``. Без канонизации
        # поданный, но не действующий флаг менял бы ``run_id`` (ложный execution_change).
        "frozen_source": canonical_frozen_source(
            RefreshPolicy.AUTO if refresh_policy is None else refresh_policy,
            frozen_source,
        ),
        # I3/§5: раунды SERP-контура — объявленный вход (``run.yaml: serp.rounds``,
        # переопределяемый ``--serp-rounds``/``COHOUND_SERP``/``deploy.search.serp``).
        # Идут в контракт: раунд меняет корпус, значит и ``run_id`` (execution_change).
        "serp_rounds": _declared(declared, "serp_rounds"),
        # Режим поиска — тем же правом, что и раунды: находка становится документом,
        # а какой бэкенд её отдал, зависит от режима. ``None`` здесь означал бы
        # «неизвестно, чем спрашивали», поэтому резолвер подставляет эффективный дефолт.
        "serp_mode": _declared(declared, "serp_mode"),
        "registry_spec_digest": _declared(declared, "registry_spec_digest"),
        "deploy_profile": _declared(declared, "deploy_profile"),
        "edge_schema_version": _declared(declared, "edge_schema_version"),
        # 13-manual-artifacts §3.2: подача ручных артефактов меняет корпус, значит и
        # условия измерения. Пустой набор — стабильный дайджест пустого множества, а не
        # ``None``: «артефактов не объявлено» — это факт входа, а не отсутствие значения.
        "manual_artifacts_digest": _declared(declared, "manual_artifacts_digest"),
        # §7.1/§7.4: шаблон промпта оценщика. Имя версии без содержимого позволило бы
        # переписать текст шаблона, не сменив идентичность, поэтому в контракт идёт пара
        # «имя + хеш файла»: шаблон задаёт, что считается заявленным, а что отсутствием.
        "prompt_template": _prompt_template_identity(),
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


def previous_run_of(
    state: Any,
    package_id: str,
    *,
    run_id: str,
    explicit: str | None = None,
) -> RunLineageRow | None:
    """Предыдущий прогон пакета — **единственная** формула правила (§12.3).

    Зачем отдельная функция: «предыдущий прогон» вычислялся в двух местах по-разному
    (``run`` с ``--since-run`` и исключением себя, adoption durable-состояния), и две
    копии правила — это два способа разойтись: манифест называл бы один предшественник,
    а carry-forward считался бы против другого. Здесь правило одно:

    * ``explicit`` задан (``--since-run``) → строка по нему; нет строки — :class:`ValueError`
      (сообщение называет ``--since-run``; CLI переводит его в ``EXIT_USAGE``). Отсутствие
      ``state`` при явном запросе — тоже отказ: «не нашли» не подменяется «нет истории»;
    * иначе — последняя строка lineage пакета (:meth:`ProductStatePort.previous_lineage`);
    * найденный ``run_id`` совпал с текущим → ``None``: resume текущего прогона не
      «предыдущий», carry-forward против самого себя не выполняется (§12.3);
    * ``state is None`` (модульный вызов/``--until plan``) → ``None``: истории нет, а не
      «история пуста и это факт измерения».

    **Предшественник не входит в материал ``run_id``** (:func:`derive_run_id`): иначе
    идентичность прогона зависела бы от собственной истории — тот же объявленный вход
    давал бы разный ``run_id`` в зависимости от того, что случилось раньше, а resume
    перестал бы находить свою строку (``run_id`` вычисляется до исполнения фаз, когда
    предшественник ещё может измениться). Он входит в lineage/манифест — провенанс
    сравнения, а не условие измерения.

    :param state: durable-состояние пакета (:class:`~core.ports.state.ProductStatePort`)
        либо ``None``.
    :param package_id: идентичность пакета **той же формы**, что у записи lineage
        (имя каталога пакета) — иначе «последний прогон» ищется в чужой истории.
    :param run_id: идентификатор текущего прогона (исключение себя).
    :param explicit: явно названный предшественник (``--since-run``).
    :raises ValueError: явный предшественник не найден в ``state.db``.
    """
    if explicit is not None:
        row = state.get_run_lineage(explicit) if state is not None else None
        if row is None:
            raise ValueError(
                f"--since-run: прогон '{explicit}' не найден в state.db"
            )
        return None if row.run_id == run_id else row
    if state is None:
        return None
    row = state.previous_lineage(package_id)
    if row is None or row.run_id == run_id:
        return None
    return row


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
