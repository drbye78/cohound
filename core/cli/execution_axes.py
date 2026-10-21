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

``extractor_stack`` — новое **объявленное** поле контракта: константы стека извлечения
(HTML-адаптер + семейство docling-адаптера + фактическая версия установленной
библиотеки ``docling``). Версия библиотеки сегодня не видна нигде: ``docling-doc-3:<level>``
в ``DoclingDocumentExtractor.version`` — ключ ``extract_cache`` и ``partition_build_id``,
поэтому туда она не добавляется (это инвалидировало бы кэш и переизвлекало корпус);
она едет только в ``extractor_stack``.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import importlib.metadata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.adapters.chunker_markdown import MarkdownChunker
from core.adapters.extract_html import HtmlExtractor
from core.adapters.llm_local import (
    DEFAULT_DECODER as EVALUATOR_DEFAULT_DECODER,
)
from core.adapters.llm_local import (
    DEFAULT_MODEL_ID as EVALUATOR_DEFAULT_MODEL_ID,
)
from core.cli.context import _load_registry_specs
from core.cli.wiring import manifest_deploy_fields, registry_specs_digest
from core.config.deploy import DeployConfig, resolve_llm_slot
from core.domain.dictionary import load_dictionary
from core.domain.models import canonical_rate_limit
from core.pipeline.normalize import RunInputs
from core.pipeline.report import crawl_control_digest

__all__ = [
    "DeclaredExecutionAxes",
    "DOCLING_ADAPTER_FAMILY",
    "declared_status_dict_version",
    "extractor_stack_version",
    "resolve_declared_axes",
]

#: Семейство docling-адаптера (без per-level суффикса ``:level``). Уровень извлечения —
#: отдельная операционная ось ``crawl.document_level``, уже входящая в
#: ``crawl_config_digest``; дублировать его в стеке значило бы считать одну ось дважды.
#: Значение зеркалит ``DoclingDocumentExtractor.version`` (``extract_document.py``);
#: адаптер намеренно не импортируется: он тянет тяжёлую необязательную ``docling``.
DOCLING_ADAPTER_FAMILY = "docling-doc-3"

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
    registry_spec_digest: str | None
    deploy_profile: str | None
    edge_schema_version: str | None
    chunker_version: str | None
    llm_scenarios: tuple[dict[str, Any], ...]
    extractor_stack: str


def _docling_library_version() -> str:
    """Версия установленной библиотеки ``docling`` без тяжёлого импорта.

    ``importlib.metadata`` читает метаданные дистрибутива: ``import docling`` тянул бы
    torch/модели и был бы недопустим на старте прогона. Библиотека ставится под двумя
    именами дистрибутива: полный ``docling`` и лёгкий ``docling-slim`` (тот же код без
    torch; 05-dependency-stack) — проверяются оба, иначе на slim-установке версия молча
    терялась бы. Отсутствие пакета — честный ``"unknown"`` (значение не выдумывается, A3).
    """
    for dist in ("docling", "docling-slim"):
        try:
            return importlib.metadata.version(dist)
        except importlib.metadata.PackageNotFoundError:
            continue
        except Exception:  # noqa: BLE001 — метаданные не роняют резолв осей
            continue
    return "unknown"


def extractor_stack_version() -> str:
    """Декларированный стек извлечения: ``<html>+<docling-family>/docling-<version>``.

    Пример: ``trafilatura-html-2+docling-doc-3/docling-2.135.0`` (при отсутствии
    библиотеки — ``.../docling-unknown``).
    """
    return f"{HtmlExtractor.version}+{DOCLING_ADAPTER_FAMILY}/docling-{_docling_library_version()}"


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

    Источник — ``deploy.llm`` (слоты) + ``--llm-slot``/``run.yaml:llm_slot``; env-ключи
    ``COHOUND_LLM_*`` **не** читаются: это операционная среда хоста, а не декларированный
    вход прогона (иначе два хоста с одним ``run.yaml`` получили бы разный ``run_id``).

    Сценарий ``evaluator`` объявляется, только если слот **активен** — задан ``base_url``
    (условие активности зеркалит ``build_evaluator_llm``): пустой слот означает, что
    оценщик не поднимается, и выдумывать маршрут нельзя. Остальные сценарии
    (``search_adapter``/``discovery``/``concept_scout``) в ``deploy.llm`` слотов не имеют
    и здесь не объявляются — пустая таблица честнее выдуманной.
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
    row = {
        "scenario": "evaluator",
        "model_id": getattr(config, "model_id", None) or EVALUATOR_DEFAULT_MODEL_ID,
        "model_version": getattr(config, "model_version", None) or None,
        "endpoint_kind": "api",
        # I18: оценщик без веб-тула — структурная гарантия, а не настройка.
        "web_tool": False,
        "decoder": getattr(config, "decoder", None) or EVALUATOR_DEFAULT_DECODER,
    }
    return (row,)


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

    :param args: разобранные аргументы CLI (нужны ``--package`` и ``--llm-slot``).
    :param inputs: нормализованные входы прогона (``run``/``package``/профили домена).
    :param deploy: resolved ``DeployConfig``; ``None`` — безопасный дефолт ``local``
        (значения не выдумываются сверх дефолтов конфига).
    :raises ValueError: неизвестное имя LLM-слота (``--llm-slot``/``run.yaml``).
    :raises core.cli.errors.CliError: объявленный реестр пакета отсутствует/невалиден.
    """
    config = deploy if deploy is not None else DeployConfig(profile="local")
    run = getattr(inputs, "run", None)
    run_crawl = getattr(run, "crawl", None)
    deploy_fields = manifest_deploy_fields(config)
    return DeclaredExecutionAxes(
        crawl_strategy=_crawl_strategy(run_crawl),
        crawl_rate_limit=canonical_rate_limit(getattr(run_crawl, "rate_limit", None)),
        # Чистая функция pipeline (app < run < per-site) — вызывается, а не дублируется:
        # вторая формула дайджеста разошлась бы с манифестом.
        crawl_config_digest=crawl_control_digest(
            run_crawl=run_crawl,
            app=config.crawl,
            profiles=getattr(inputs, "domain_profiles", None),
        ),
        registry_spec_digest=registry_specs_digest(_load_registry_specs(args, inputs)),
        deploy_profile=deploy_fields["deploy_profile"],
        edge_schema_version=deploy_fields["edge_schema_version"],
        chunker_version=MarkdownChunker.version,
        llm_scenarios=_resolve_llm_scenarios(args=args, inputs=inputs, deploy=config),
        extractor_stack=extractor_stack_version(),
    )
