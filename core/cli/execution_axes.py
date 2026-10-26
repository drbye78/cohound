"""coHound — объявленные оси исполнения (declared-слой execution contract, §13).

Модуль слоя ``cli`` (**composition root**, Z0): единственное место, где из
``(args, inputs, deploy)`` разрешаются **декларированные** оси исполнения. Значение
используется двумя потребителями без второго резолвера:

* :func:`core.pipeline.run_identity.build_execution_contract` — отпечаток условий
  измерения (``execution_contract_digest`` → ``run_id``/lineage/``hound diff``);
* :func:`core.pipeline.report.build_manifest` — поля ``run_manifest.yaml``.

**Почему эти девять осей объявлены, а наблюдаемые — нет.** Предикат — «функция входа»
против «функции собранных портов»:

* объявленная ось вычислима на старте из ``args``/``run.yaml``/файлов пакета/
  ``deploy.yaml``/констант кода, то есть **до** сборки портов и корпуса. Два хоста с
  одинаковыми входами получат одно и то же значение (R7 §11.7);
* наблюдаемая величина (``extractor_version``, ``embed_model_id``,
  ``lexical_engine_version``, ``vector_store``) — функция **собранных** портов/диска:
  версия экстрактора — per-product композит фазы корпуса, эмбеддер читается с диска
  при сборке индекса, лексический движок и векторное хранилище зависят от
  установленных wheel'ов. Такие значения остаются провенансом манифеста/``pb``, но в
  контракт **не** входят: иначе два хоста с одним ``run.yaml`` дали бы разный
  ``run_id``, сломав воспроизводимость (R7 §11.7) и запретив diff по причине, которую
  нельзя объявить на входе (методология 2.4.0, §13).

``extractor_stack`` — **объявленное** поле контракта: константы стека извлечения
(HTML-адаптер + семейство docling-адаптера + фактическая версия установленной
библиотеки ``docling``). Сборка движка (``docling-<version>``) берётся из
:func:`core.adapters.extract_document.docling_engine_build` — **того же** вызова, из
которого строится ``extractor_version`` документа (``docling-doc-3:<level>/docling-<build>``,
ключ ``extract_cache`` и слагаемое ``pb``). Две независимые формулы разошлись бы, и
``run_id`` объявлял бы движок, которого не было (Д-8, §2.5). ``extractor_version`` при
этом остаётся наблюдаемой величиной и в контракт не входит: в ``run_id`` едет только
объявленный стек, поэтому добавление сборки в ключ кэша ``run_id`` не меняет.

``DOCLING_ADAPTER_FAMILY`` — реэкспорт из адаптера, чтобы у строки семейства был **один**
источник (``__all__`` сохраняет прежнюю публичную поверхность модуля). Уровень извлечения —
отдельная операционная ось ``crawl.document_level``, уже входящая в
``crawl_config_digest``; дублировать его в стеке значило бы считать одну ось дважды.

``chunker_version`` — **композит** правил нарезки, а не версия одного чанкера: у plain text
свой абзацный чанкер, у markdown/HTML/EPUB — заголовочный, поэтому значение —
``"md-heading-2+text-para-1"`` (:func:`core.adapters.chunker_family.composite_chunker_version`).
Формула одна на декларацию и на собранный порт: разойдись они — ``execution_contract_digest``
описывал бы не ту нарезку, которой получен корпус (R7 §11.7). Смена строки — версионная
граница класса ``execution_versions`` (``execution_change``, §13): меняется ``run_id``,
``methodology_version`` — нет.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.adapters.chunker_family import composite_chunker_version
from core.adapters.extract_document import (
    DOCLING_ADAPTER_FAMILY,
    docling_engine_build,
)
from core.adapters.extract_html import HtmlExtractor
from core.cli.context import _load_registry_specs
from core.cli.errors import EXIT_USAGE, CliError
from core.cli.wiring import (
    manifest_deploy_fields,
    registry_specs_digest,
    resolve_evaluator_model_identity,
)
from core.config.deploy import ENV_SERP, DeployConfig, resolve_llm_slot, resolve_serp_rounds
from core.domain.dictionary import load_dictionary
from core.domain.models import canonical_rate_limit
from core.pipeline.normalize import RunInputs
from core.pipeline.manual_artifacts import load_manual_artifacts
from core.pipeline.report import crawl_control_digest

__all__ = [
    "DeclaredExecutionAxes",
    "DOCLING_ADAPTER_FAMILY",
    "declared_status_dict_version",
    "extractor_stack_version",
    "resolve_declared_axes",
]

#: Путь словаря статусов относительно корня репозитория (источник ``status_dict_version``).
_STATUS_DICTIONARY_PATH = (
    Path(__file__).resolve().parents[2] / "docs" / "status-dictionary.yaml"
)


@dataclass(frozen=True)
class DeclaredExecutionAxes:
    """Декларированные оси исполнения — вход контракта и манифеста (§13).

    Все значения — «функция входа» (``args``/``run.yaml``/файлы пакета/``deploy.yaml``/
    константы кода); наблюдаемые величины портов/диска здесь отсутствуют по построению
    (см. докстринг модуля). ``llm_scenarios`` — кортеж отображений (детерминированная
    таблица §7.4/I21 без ``run_id``: идентификатор прогона выводится из контракта и не
    может входить в его материал).
    """

    crawl_strategy: str | None
    crawl_rate_limit: str | None
    crawl_config_digest: str | None
    #: I3/§5: число раундов SERP-контура и **уровень**, который его задал
    #: (``cli``/``env``/``run.yaml``/``deploy.yaml``/``default``). Раунды входят в
    #: контракт (``serp_rounds``): они меняют корпус, значит и условия измерения.
    serp_rounds: int | None
    serp_source: str | None
    registry_spec_digest: str | None
    deploy_profile: str | None
    edge_schema_version: str | None
    chunker_version: str | None
    llm_scenarios: tuple[dict[str, Any], ...]
    extractor_stack: str
    #: 13-manual-artifacts §3.2: отпечаток объявленных ручных артефактов
    #: (``sources/manual_artifacts.yaml``). Объявленный вход (файл пакета), поэтому
    #: вычислим до сборки портов; материал — ``(product_id, url, sha256)``, где
    #: ``sha256`` **вычислен из байтов** (объявленный — сверка, не основание доверия).
    #: ``None`` — резолвер не вызывался (DI/legacy): «неизвестно», не «пусто».
    manual_artifacts_digest: str | None = None


def extractor_stack_version() -> str:
    """Декларированный стек извлечения: ``<html>+<docling-family>/<build>``.

    Пример: ``trafilatura-html-2+docling-doc-3/docling-2.135.0`` (при отсутствии
    библиотеки — ``.../docling-unknown``). ``<build>`` — **тот же** вызов
    (:func:`core.adapters.extract_document.docling_engine_build`), что даёт сборку в
    ``extractor_version`` документа, поэтому стек и ключ ``extract_cache`` называют одну
    и ту же версию библиотеки (Д-8).
    """
    return f"{HtmlExtractor.version}+{DOCLING_ADAPTER_FAMILY}/{docling_engine_build()}"


def declared_status_dict_version() -> str | None:
    """``status_dict_version`` из ``docs/status-dictionary.yaml`` (честный источник).

    Словарь — источник истины перечислений (§4.1); его версия — декларированная
    величина. Файл отсутствует (установка без ``docs/``) → ``None``: значение не
    подменяется правдоподобным дефолтом.
    """
    try:
        data = load_dictionary(_STATUS_DICTIONARY_PATH)
    except (OSError, ValueError):
        return None
    version = data.get("status_dictionary_version")
    return str(version) if version is not None else None


def _crawl_strategy(run_crawl: Any) -> str | None:
    """Стратегия краула run-слоя (``run.yaml:crawl.strategy``) либо ``None``."""
    value = getattr(run_crawl, "strategy", None)
    return None if value is None else str(getattr(value, "value", value))


def _resolve_llm_scenarios(
    *, args: argparse.Namespace, inputs: RunInputs, deploy: DeployConfig
) -> tuple[dict[str, Any], ...]:
    """Детерминированная таблица ``scenario → модель`` (§7.4/I21) из объявленных входов.

    Источник — ``deploy.llm`` (слоты) + ``--llm-slot``/``run.yaml:llm_slot`` +
    ``--llm-model``; env-ключи ``COHOUND_LLM_*`` **не** читаются: это операционная среда
    хоста, а не декларированный вход прогона (иначе два хоста с одним ``run.yaml`` получили
    бы разный ``run_id``).

    Идентичность модели резолвится **той же** функцией, что и runtime-оценщик
    (:func:`core.cli.wiring.resolve_evaluator_model_identity`): аргумент ``--llm-model`` >
    поле слота > дефолт ``decoder``. Двух формул нет — иначе контракт и рантайм разошлись бы
    (именно этот разрыв и был измерен: манифест называл модель, которая не работала).

    Сценарий ``evaluator`` объявляется, только если слот **активен** — задан ``base_url``
    (условие активности зеркалит ``build_evaluator_llm``): пустой слот означает, что
    оценщик не поднимается, и выдумывать маршрут нельзя. Остальные сценарии
    (``search_adapter``/``discovery``/``concept_scout``) в ``deploy.llm`` слотов не имеют
    и здесь не объявляются — пустая таблица честнее выдуманной.

    Endpoint из env (``COHOUND_LLM_BASE_URL``) сюда не входит по построению: он не
    декларирован, поэтому таблица остаётся пустой, а факт «оценщик поднят из env вне
    контракта» сообщает :func:`core.cli.wiring.llm_env_notices`.

    :raises core.cli.errors.CliError: слот активен, но модель не объявлена ни слотом,
        ни ``--llm-model`` (тихий дефолт запрещён — это была бы ложная провенанс-запись).
    """
    llm = getattr(deploy, "llm", None)
    explicit = getattr(args, "llm_slot", None)
    if explicit is None:
        explicit = getattr(getattr(inputs, "run", None), "llm_slot", None)
    slot = resolve_llm_slot(llm, explicit=explicit)
    config = getattr(llm, slot, None) if llm is not None else None
    base_url = getattr(config, "base_url", None)
    if not (base_url and str(base_url).strip()):
        return ()
    model_id, decoder = resolve_evaluator_model_identity(
        slot_config=config,
        slot_name=slot,
        model_id=getattr(args, "llm_model", None),
    )
    row = {
        "scenario": "evaluator",
        "model_id": model_id,
        "model_version": getattr(config, "model_version", None) or None,
        "endpoint_kind": "api",
        # I18: оценщик без веб-тула — структурная гарантия, а не настройка.
        "web_tool": False,
        "decoder": decoder,
    }
    return (row,)


def _crawl_axis_overrides(args: argparse.Namespace) -> dict[str, Any]:
    """Операторские оси краула из CLI (``--document-level``/``--pdf-pipeline``).

    Это **объявление** прогона, а не уточнение исполнения: обе оси меняют текст документа
    (уровень docling / механизм PDF), значит корпус и ``run_id`` (§13). Слой применяется
    к разрешённой конфигурации (поверх app < run < site) в
    :func:`core.pipeline.report.crawl_control_digest`; здесь — только чтение флагов.

    Отдельный шаг вместо подстановки в ``inputs.run.crawl``: синтез секции ``crawl``
    объявил бы за оператора и остальные оси (``strategy`` имеет непустой дефолт), и
    ``--pdf-pipeline structured`` — эффективно no-op — дал бы другой ``run_id``.

    ``{}`` — оси не заданы (поведение и ``run_id`` неизменны).
    """
    overrides: dict[str, Any] = {}
    document_level = getattr(args, "document_level", None)
    if document_level is not None:
        overrides["document_level"] = getattr(document_level, "value", document_level)
    pdf_pipeline = getattr(args, "pdf_pipeline", None)
    if pdf_pipeline is not None:
        overrides["pdf_pipeline"] = getattr(pdf_pipeline, "value", pdf_pipeline)
    return overrides


def _manual_artifacts_digest(args: argparse.Namespace) -> str:
    """Отпечаток объявленных ручных артефактов (13-manual-artifacts §3.2).

    Читается **тот же** файл пакета тем же загрузчиком, что и фаза корпуса
    (:func:`core.pipeline.manual_artifacts.load_manual_artifacts`): двух формул
    дайджеста нет, иначе ``run_id`` описывал бы не тот корпус, который собран.

    Загрузка — здесь, а не в фазе корпуса, потому что ось объявлена: она обязана быть
    видна ``run_id`` **до** запуска. Объявленный, но нечитаемый/несовпавший по ``sha256``
    артефакт — отказ ``EXIT_USAGE`` (§3.1: сверка хеша — не основание доверия, а проверка).
    Отсутствие файла — не ошибка: стабильный дайджест пустого множества.

    :raises core.cli.errors.CliError: декларация есть, но невалидна.
    """
    package_dir = getattr(args, "package", None)
    try:
        return load_manual_artifacts(package_dir).digest()
    except ValueError as exc:
        raise CliError(
            f"manual_artifacts: {exc}",
            code="manual_artifacts",
            exit_code=EXIT_USAGE,
        ) from exc


def _serp_axis(
    *, args: argparse.Namespace, run: Any, deploy: DeployConfig
) -> tuple[int, str]:
    """Эффективные раунды SERP-контура и уровень-источник (I3/§5).

    Приоритет — как у LLM-слота (``CLI > env > run.yaml > deploy.yaml > default``,
    :func:`core.config.deploy.resolve_serp_rounds`). Env ``COHOUND_SERP`` здесь
    **читается**, в отличие от ``COHOUND_LLM_MODEL``: это операционный троттл контура, а
    раунды — условие измерения. Скрыть env-значение из контракта значило бы дать прогон,
    у которого ``run_id`` описывает выключенный контур, а корпус собран с поиском.

    Бюджет объявляется **только** в ``run.yaml: serp.budget``; включение контура без
    бюджета — отказ ``EXIT_USAGE``, а не молчаливый безлимит.

    :raises ValueError: значение уровня не разбирается (не целое/отрицательное).
    :raises core.cli.errors.CliError: раунды включены, а бюджет не объявлен.
    """
    run_serp = getattr(run, "serp", None)
    rounds, source = resolve_serp_rounds(
        getattr(deploy, "search", None),
        cli=getattr(args, "serp_rounds", None),
        env=os.environ.get(ENV_SERP),
        run_rounds=getattr(run_serp, "rounds", None),
    )
    if rounds > 0 and getattr(run_serp, "budget", None) is None:
        raise CliError(
            f"SERP-контур включён ({rounds} раунд(ов), источник {source}), но "
            "run.yaml: serp.budget не объявлен. Бюджет определяет число ушедших "
            "запросов, то есть стоимость и состав корпуса: безлимит по умолчанию "
            "запрещён — объявите serp.budget: <запросов на продукт на раунд>",
            code="serp",
            exit_code=EXIT_USAGE,
        )
    return rounds, source


def resolve_declared_axes(
    *,
    args: argparse.Namespace,
    inputs: RunInputs,
    deploy: DeployConfig | None,
) -> DeclaredExecutionAxes:
    """Разрешает объявленные оси исполнения из входов (без портов и корпуса).

    Чистая функция входа: читает ``args``/``inputs``/``deploy`` и файлы пакета
    (спецификации реестров — только YAML), не собирает адаптеры и не трогает диск
    прогона. Один и тот же вызов — источник и контракта, и манифеста.

    :param args: разобранные аргументы CLI (нужны ``--package``, ``--llm-slot`` и
        ``--llm-model``).
    :param inputs: нормализованные входы прогона (``run``/``package``/профили домена).
    :param deploy: resolved ``DeployConfig``; ``None`` — безопасный дефолт ``local``
        (значения не выдумываются сверх дефолтов конфига).
    :raises ValueError: неизвестное имя LLM-слота (``--llm-slot``/``run.yaml``).
    :raises core.cli.errors.CliError: объявленный реестр пакета отсутствует/невалиден; либо
        LLM-слот активен, а модель оценщика не объявлена ни слотом, ни ``--llm-model``.
    """
    config = deploy if deploy is not None else DeployConfig(profile="local")
    run = getattr(inputs, "run", None)
    run_crawl = getattr(run, "crawl", None)
    axis_overrides = _crawl_axis_overrides(args)
    deploy_fields = manifest_deploy_fields(config)
    serp_rounds, serp_source = _serp_axis(args=args, run=run, deploy=config)
    return DeclaredExecutionAxes(
        crawl_strategy=_crawl_strategy(run_crawl),
        crawl_rate_limit=canonical_rate_limit(getattr(run_crawl, "rate_limit", None)),
        # Чистая функция pipeline (app < run < per-site) — вызывается, а не дублируется:
        # вторая формула дайджеста разошлась бы с манифестом.
        crawl_config_digest=crawl_control_digest(
            run_crawl=run_crawl,
            app=config.crawl,
            profiles=getattr(inputs, "domain_profiles", None),
            # P5/P7: операторские оси краула применяются поверх разрешённой цепочки —
            # флаг CLI объявляет этот прогон (меняет текст документа ⇒ ``run_id``).
            overrides=axis_overrides,
        ),
        serp_rounds=serp_rounds,
        serp_source=serp_source,
        registry_spec_digest=registry_specs_digest(_load_registry_specs(args, inputs)),
        deploy_profile=deploy_fields["deploy_profile"],
        edge_schema_version=deploy_fields["edge_schema_version"],
        # Композит правил нарезки (markdown + plain text), а не версия одного чанкера:
        # с появлением текстового чанкера набор чанков определяется парой правил.
        # Смена строки — версионная граница ``execution_versions`` (§13), меняет ``run_id``;
        # ``METHODOLOGY_VERSION`` не затрагивается.
        chunker_version=composite_chunker_version(),
        llm_scenarios=_resolve_llm_scenarios(args=args, inputs=inputs, deploy=config),
        extractor_stack=extractor_stack_version(),
        # 13-manual-artifacts §3.2: ручные артефакты объявлены файлом пакета, поэтому
        # отпечаток — функция входа (как seeds_digest), а не наблюдение прогона.
        manual_artifacts_digest=_manual_artifacts_digest(args),
    )
