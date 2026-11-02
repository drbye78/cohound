"""Тесты декларированных осей исполнения (§13, методология 2.4.0).

Проверяется главное свойство: ``execution_contract_digest`` — отпечаток **функции входа**
(``args``/``run.yaml``/файлы пакета/``deploy.yaml``/константы кода), а не собранных
портов/диска. Отсюда:

* один резолвер (:func:`core.cli.execution_axes.resolve_declared_axes`) на контракт и
  манифест, каждая ось меняет ровно свой компонент;
* ``plan``/``crawl``/``run`` дают один ``run_id`` и все три **вызывают** резолвер;
* ``run_id`` не зависит от сборки портов (регресс на порядок);
* наблюдаемые величины (``extractor_version``/``embed_model_id``/
  ``lexical_engine_version``/``vector_store``) в контракт не входят — принятое
  ограничение закреплено пином, чтобы его не «починили» случайно.

Офлайн: ``--until plan``/``--dry-run`` не запускают сеть и не собирают корпус.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

import core.cli.commands.crawl as crawl_mod
import core.cli.commands.plan as plan_mod
import core.cli.commands.run as run_mod
import core.cli.wiring as wiring_mod
from core.adapters import extract_document
from core.adapters.chunker_markdown import MarkdownChunker
from core.adapters.chunker_text import TextChunker
from core.adapters.extract_document import DoclingDocumentExtractor
from core.cli import execution_axes
from core.cli.errors import CliError
from core.cli.execution_axes import (
    DeclaredExecutionAxes,
    declared_status_dict_version,
    resolve_declared_axes,
)
from core.cli.main import EXIT_ERROR, EXIT_OK, main
from core.cli.wiring import ENV_LLM_DECODER, ENV_LLM_MODEL
from core.config.deploy import (
    CrawlConfig,
    DeployConfig,
    EdgeConfig,
    EmbedConfig,
    LLMDeployConfig,
    LLMSlotConfig,
)
from core.domain.enums import CrawlStrategy, DocumentLevel, EmbedDevice, PdfDevice
from core.domain.models import CrawlRunConfig, RateLimitSpec
from core.pipeline.digests import (
    EXECUTION_CONTRACT_FIELDS,
    execution_contract_digest,
)
from core.pipeline.normalize import load_inputs
from core.pipeline.plan import plan as build_plan
from core.pipeline.report import build_manifest
from core.pipeline.run_identity import build_execution_contract
from tests.helpers.packages import REGISTRY_FIXTURES, valid_package

#: Наблюдаемые величины портов/диска — провенанс манифеста, но **не** вход контракта.
OBSERVED_AXES: tuple[str, ...] = (
    "extractor_version",
    "embed_model_id",
    "lexical_engine_version",
    "vector_store",
)


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD: деплой-конфиг — дефолтный ``local``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.chdir(tmp_path)
    return root


def _package(tmp_path: Path, name: str = "package", **kwargs: Any) -> Path:
    """Линт-чистый пакет в отдельном подкаталоге (несколько пакетов в одном тесте)."""
    base = tmp_path / name
    base.mkdir()
    return valid_package(base, **kwargs)


def _inputs(package: Path) -> Any:
    return load_inputs(package / "run.yaml", package)


def _args(
    package: Path, *, llm_slot: str | None = None, llm_model: str | None = None
) -> SimpleNamespace:
    return SimpleNamespace(package=str(package), llm_slot=llm_slot, llm_model=llm_model)


def _resolve(
    package: Path,
    *,
    inputs: Any = None,
    deploy: DeployConfig | None = None,
    args: Any = None,
) -> DeclaredExecutionAxes:
    return resolve_declared_axes(
        args=args or _args(package),
        inputs=inputs if inputs is not None else _inputs(package),
        deploy=deploy if deploy is not None else DeployConfig(),
    )


# ---------------------------------------------------------------------------
# resolve_declared_axes: детерминизм и «одна ось — один компонент»
# ---------------------------------------------------------------------------


def test_declared_axes_are_deterministic(tmp_path: Path) -> None:
    """Одинаковый вход → одинаковый объект (дайджесты/стек стабильны)."""
    package = _package(tmp_path)
    first = _resolve(package)
    second = _resolve(package)
    assert first == second
    assert first.extractor_stack.startswith(
        "trafilatura-html-4+text-plain-2+epub-2+pdfium-text-2+docling-office-1"
        "+docling-doc-3/cpu/docling-"
    )
    # Объявленные оси — ровно по списку §13 (+ ``serp_rounds``: раунд внешнего поиска
    # меняет состав корпуса, то есть условие измерения; + ``manual_artifacts_digest``:
    # поданный файл добавляет документы корпуса, 13-manual-artifacts §3.2); наблюдаемых
    # здесь нет. ``serp_source`` — провенанс («какой уровень включил контур»), не измерение.
    assert {field.name for field in dataclasses.fields(first)} == {
        "crawl_strategy",
        "crawl_rate_limit",
        "crawl_config_digest",
        "registry_spec_digest",
        "deploy_profile",
        "edge_schema_version",
        "chunker_version",
        "llm_scenarios",
        "extractor_stack",
        # 16 §4.2: устройство эмбеддера — объявленная ось (``deploy < run``), а не
        # наблюдение: векторы CPU и CUDA численно разные.
        "embed_device",
        "serp_rounds",
        "serp_source",
        # I3/§5: режим запроса поиска — отдельное условие измерения (какой бэкенд
        # отвечает), поэтому отдельная ось от ``serp_rounds``.
        "serp_mode",
        "manual_artifacts_digest",
        # C3: геометрия нарезки отдельно от метки ``chunker_version`` — правка
        # ``chunk_size`` меняла корпус, не меняя ``run_id``.
        "chunking_spec",
        # V3: политика повтора меняет вердикты (повтор переиздаёт запрос).
        "evaluation_policy",
        # §11.1: объявленные входы корпуса — режим рендера и содержимое паспортов.
        # Оба меняют доказательства, оставаясь до правки под прежним ``run_id``.
        # ``--urls`` здесь отсутствует осознанно: он уже входит в ``seeds_digest``.
        "render_modes_digest",
        "passports_digest",
        # C8: схема лексического индекса — меняет лексическую выдачу, значит вердикты.
        "lexical_spec",
    }


def _with_serp(package: Path, *, rounds: int | None, budget: int | None) -> Path:
    """Дописывает секцию ``serp`` в ``run.yaml`` пакета (вход прогона)."""
    run_yaml = package / "run.yaml"
    data = yaml.safe_load(run_yaml.read_text(encoding="utf-8"))
    section: dict[str, Any] = {}
    if rounds is not None:
        section["rounds"] = rounds
    if budget is not None:
        section["budget"] = budget
    data["serp"] = section
    run_yaml.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return package


def test_serp_axis_reads_run_yaml_and_names_its_source(tmp_path: Path) -> None:
    """``serp.rounds`` из ``run.yaml`` — ось контракта; уровень-источник назван."""
    package = _with_serp(_package(tmp_path), rounds=2, budget=5)
    axes = _resolve(package)
    assert axes.serp_rounds == 2
    assert axes.serp_source == "run.yaml"


def test_serp_axis_cli_overrides_run_yaml(tmp_path: Path) -> None:
    """``--serp-rounds`` сильнее ``run.yaml`` (и это видно по источнику)."""
    package = _with_serp(_package(tmp_path), rounds=0, budget=5)
    args = SimpleNamespace(
        package=str(package), llm_slot=None, llm_model=None, serp_rounds=3
    )
    axes = _resolve(package, args=args)
    assert axes.serp_rounds == 3 and axes.serp_source == "cli"


def test_serp_axis_requires_declared_budget(tmp_path: Path) -> None:
    """``rounds > 0`` без ``serp.budget`` — отказ, а не молчаливый безлимит.

    Два слоя одной политики: схема ``run.yaml`` (:class:`SerpRunConfig`) отвергает такую
    комбинацию при загрузке входа, а резолвер осей — при прямом вызове (DI/тесты), где
    схема обойдена. Бюджет определяет число ушедших запросов, то есть стоимость и состав
    корпуса: молчаливый безлимит был бы невидимым изменением условий измерения.
    """
    package = _with_serp(_package(tmp_path), rounds=1, budget=None)
    with pytest.raises(ValidationError) as excinfo:
        _inputs(package)
    assert "budget" in str(excinfo.value)

    serp = SimpleNamespace(rounds=1, budget=None)
    run = SimpleNamespace(serp=serp)
    with pytest.raises(CliError) as cli_error:
        execution_axes._serp_axis(  # noqa: SLF001 — гвард проверяется напрямую
            args=SimpleNamespace(serp_rounds=None),
            run=run,
            deploy=DeployConfig(),
        )
    assert cli_error.value.code == "serp"
    assert cli_error.value.exit_code != 0


def test_chunker_version_is_a_composite_of_both_chunkers(tmp_path: Path) -> None:
    """``chunker_version`` — композит правил нарезки (markdown + plain text), не одна версия.

    С появлением абзацного чанкера набор чанков определяется парой правил; строка
    стабильна и содержит обе версии. Это ``execution_change`` (§13): меняется ``run_id``,
    ``methodology_version`` — нет.
    """
    declared = _resolve(_package(tmp_path))

    assert declared.chunker_version == f"{MarkdownChunker.version}+{TextChunker.version}"
    assert declared.chunker_version == "md-heading-4+text-para-2"
    assert MarkdownChunker.version in declared.chunker_version
    assert TextChunker.version in declared.chunker_version


def _with_run_crawl(inputs: Any, crawl: CrawlRunConfig) -> Any:
    return dataclasses.replace(
        inputs, run=inputs.run.model_copy(update={"crawl": crawl})
    )


def _split_deploy() -> DeployConfig:
    return DeployConfig(
        profile="split",
        edge=EdgeConfig(base_url="https://edge.test:8443"),
    )


@pytest.mark.parametrize(
    "field",
    [
        "crawl_strategy",
        "crawl_rate_limit",
        "crawl_config_digest",
        "registry_spec_digest",
        "deploy_profile",
        "edge_schema_version",
        "chunker_version",
        "llm_scenarios",
        "extractor_stack",
    ],
)
def test_each_axis_changes_exactly_its_component(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    """Табличный тест: мутация одной оси меняет **только** свой компонент."""
    package = _package(tmp_path)
    inputs = _inputs(package)
    deploy = DeployConfig()
    if field in {"crawl_strategy", "crawl_rate_limit"}:
        # База уже с секцией ``crawl``: иначе добавление секции меняло бы и
        # ``crawl_strategy`` (None → "auto") — тест ловил бы не свою ось.
        inputs = _with_run_crawl(inputs, CrawlRunConfig())
    base = _resolve(package, inputs=inputs, deploy=deploy)

    mutated_package = package
    mutated_inputs = inputs
    mutated_deploy = deploy
    mutated_args: Any = _args(package)

    if field == "crawl_strategy":
        mutated_inputs = _with_run_crawl(
            inputs, CrawlRunConfig(strategy=CrawlStrategy.HONEST)
        )
    elif field == "crawl_rate_limit":
        mutated_inputs = _with_run_crawl(
            inputs, CrawlRunConfig(rate_limit=RateLimitSpec(min_s=1.0, max_s=2.0))
        )
    elif field == "crawl_config_digest":
        # Ось app-слоя: run-слой не задан, стратегия/задержки не меняются.
        mutated_deploy = DeployConfig(crawl=CrawlConfig(max_depth=2))
    elif field == "registry_spec_digest":
        spec = (REGISTRY_FIXTURES / "reg_x.yaml").read_text(encoding="utf-8")
        mutated_package = _package(
            tmp_path, "package_reg", registries={"reg_x": spec}
        )
        mutated_inputs = _inputs(mutated_package)
        mutated_args = _args(mutated_package)
    elif field == "deploy_profile":
        mutated_deploy = _split_deploy()
    elif field == "edge_schema_version":
        monkeypatch.setattr(wiring_mod, "DEFAULT_SCHEMA_VERSION", "9.9.9")
    elif field == "chunker_version":
        monkeypatch.setattr(MarkdownChunker, "version", "md-test-9")
    elif field == "llm_scenarios":
        mutated_deploy = DeployConfig(
            llm=LLMDeployConfig(
                cheap=LLMSlotConfig(
                    base_url="https://llm.test/v1", model_id="model-x"
                )
            )
        )
    elif field == "extractor_stack":
        monkeypatch.setattr(
            extract_document, "docling_library_version", lambda: "9.9.9"
        )

    mutated = _resolve(
        mutated_package,
        inputs=mutated_inputs,
        deploy=mutated_deploy,
        args=mutated_args,
    )
    changed = {
        item.name
        for item in dataclasses.fields(DeclaredExecutionAxes)
        if getattr(base, item.name) != getattr(mutated, item.name)
    }
    assert changed == {field}


def test_llm_scenarios_empty_without_active_slot(tmp_path: Path) -> None:
    """Пустой ``base_url`` → оценщик не поднимается → маршрут не выдумывается."""
    package = _package(tmp_path)
    deploy = DeployConfig(
        llm=LLMDeployConfig(cheap=LLMSlotConfig(model_id="model-x"))
    )
    assert _resolve(package, deploy=deploy).llm_scenarios == ()


def test_llm_scenarios_are_deterministic_rows(tmp_path: Path) -> None:
    """Активный слот → одна строка evaluator без ``run_id`` (I18/I21)."""
    package = _package(tmp_path)
    deploy = DeployConfig(
        llm=LLMDeployConfig(
            cheap=LLMSlotConfig(
                base_url="https://llm.test/v1",
                model_id="model-x",
                model_version="v1",
                decoder="json_object",
            )
        )
    )
    rows = _resolve(package, deploy=deploy).llm_scenarios
    assert rows == (
        {
            "scenario": "evaluator",
            "model_id": "model-x",
            "model_version": "v1",
            "endpoint_kind": "api",
            "web_tool": False,
            "decoder": "json_object",
            "constrained_decoding": "json_object",
            "seed": 0,
            "temperature": 0.0,
            "endpoint_host": "llm.test",
            "max_tokens": None,
        },
    )
    assert "run_id" not in rows[0]


def test_llm_scenarios_use_declared_model_from_flag(tmp_path: Path) -> None:
    """``--llm-model`` — объявленная модель оценщика: побеждает слот в ``llm_scenarios``."""
    package = _package(tmp_path)
    deploy = DeployConfig(
        llm=LLMDeployConfig(
            cheap=LLMSlotConfig(
                base_url="https://llm.test/v1",
                model_id="slot-model",
                decoder="json_object",
            )
        )
    )

    without_flag = _resolve(package, deploy=deploy).llm_scenarios
    with_flag = _resolve(
        package, deploy=deploy, args=_args(package, llm_model="flag-model")
    ).llm_scenarios

    assert without_flag[0]["model_id"] == "slot-model"
    assert with_flag[0]["model_id"] == "flag-model"
    # Декодер объявлен слотом — флаг модели его не трогает.
    assert with_flag[0]["decoder"] == "json_object"


def test_llm_scenarios_active_slot_without_declared_model_is_error(
    tmp_path: Path,
) -> None:
    """Активный слот без объявленной модели — ошибка, а не тихий дефолт (ложный провенанс)."""
    package = _package(tmp_path)
    deploy = DeployConfig(
        llm=LLMDeployConfig(cheap=LLMSlotConfig(base_url="https://llm.test/v1"))
    )

    with pytest.raises(CliError) as excinfo:
        _resolve(package, deploy=deploy)

    assert excinfo.value.code == "llm"
    assert "--llm-model" in str(excinfo.value)


def test_docling_version_uses_installed_distribution() -> None:
    """Версия библиотеки читается из метаданных дистрибутива (``docling``/``docling-slim``).

    Единственный источник — ``core.adapters.extract_document.docling_library_version``;
    ``extractor_stack`` берёт из него то же значение (Д-8).
    """
    from importlib.metadata import PackageNotFoundError, version

    expected: list[str] = []
    for dist in ("docling", "docling-slim"):
        try:
            expected.append(version(dist))
        except PackageNotFoundError:
            continue
    library = extract_document.docling_library_version()
    assert library in (expected or ["unknown"])
    stack = execution_axes.extractor_stack_version()
    # Хвост стека: ``/<build>/<policy>/<finalize>`` — сборка стоит третьей от конца.
    assert stack.split("/")[-3] == f"docling-{library}"


def test_status_dict_version_comes_from_dictionary() -> None:
    """``status_dict_version`` — честный источник: ``docs/status-dictionary.yaml``."""
    from core.domain.dictionary import load_dictionary

    root = Path(__file__).resolve().parents[2]
    expected = load_dictionary(root / "docs" / "status-dictionary.yaml")[
        "status_dictionary_version"
    ]
    assert declared_status_dict_version() == str(expected)
    assert declared_status_dict_version() is not None


# ---------------------------------------------------------------------------
# Состав контракта: объявленные входят, наблюдаемые — нет (принятое ограничение)
# ---------------------------------------------------------------------------


def test_contract_fields_exclude_observed_and_include_extractor_stack() -> None:
    """Пин принятого ограничения (2.4.0): наблюдаемые не вход контракта."""
    assert set(OBSERVED_AXES).isdisjoint(EXECUTION_CONTRACT_FIELDS)
    assert "extractor_stack" in EXECUTION_CONTRACT_FIELDS
    signature = inspect.signature(build_execution_contract)
    for name in OBSERVED_AXES:
        assert name not in signature.parameters


def _contract(package: Path, declared: DeclaredExecutionAxes) -> str:
    return build_execution_contract(
        _inputs(package),
        cohort_digest="cohort",
        seeds_digest="seeds",
        declared=declared,
        status_dict_version=declared_status_dict_version(),
    )


@pytest.mark.parametrize(
    "field, value",
    [
        ("chunker_version", "md-other"),
        ("extractor_stack", "other-stack/9"),
        ("deploy_profile", "split"),
        ("crawl_strategy", "honest"),
    ],
)
def test_declared_axis_changes_contract_digest(
    tmp_path: Path, field: str, value: str
) -> None:
    """Смена объявленной оси меняет ``execution_contract_digest``."""
    package = _package(tmp_path)
    declared = _resolve(package)
    changed = dataclasses.replace(declared, **{field: value})
    assert _contract(package, declared) != _contract(package, changed)


def test_contract_ignores_observed_manifest_values(tmp_path: Path) -> None:
    """Наблюдаемые величины не влияют на контракт даже как «похожие» аргументы.

    ``build_execution_contract`` не принимает их вовсе (см. пин выше), поэтому проверяем
    фактическую границу: контракт одинаков для одного входа независимо от того, что
    фаза отчёта запишет в манифест как ``extractor_version``/``embed_model_id``/….
    """
    package = _package(tmp_path)
    declared = _resolve(package)
    first = _contract(package, declared)
    # Манифест с другими наблюдаемыми значениями, но тем же контрактом.
    plan = build_plan(_inputs(package), run_id="run-x")
    base_manifest = build_manifest(
        inputs=_inputs(package),
        plan=plan,
        created_at="2026-06-01T00:00:00Z",
        axes=declared,
        execution_contract_digest=first,
    )
    observed_manifest = base_manifest.model_copy(
        update={
            "extractor_version": "docling-doc-3:full+other",
            "embed_model_id": "other-embedder",
            "lexical_engine_version": "other-lexical",
            "vector_store": "other-vector",
        }
    )
    assert observed_manifest.execution_contract_digest == first
    assert _contract(package, declared) == first


def test_extractor_stack_names_the_declared_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P9: устройство входит в ``extractor_stack`` — иначе ``run_id`` не различает его.

    ``extractor_version`` (где устройство тоже есть) наблюдаем и в контракт не входит
    (методология 2.4.0). Однако текст документа от устройства зависит, поэтому два
    прогона с разными устройствами обязаны иметь **разный** ``run_id`` — иначе один
    ``run_id`` описывал бы два разных корпуса. Поле контракта при этом не новое:
    устройство едет внутрь уже объявленного ``extractor_stack``.
    """
    monkeypatch.setattr(extract_document, "docling_library_version", lambda: "9.9.9")

    cpu = execution_axes.extractor_stack_version(PdfDevice.CPU)
    cuda = execution_axes.extractor_stack_version(PdfDevice.CUDA)

    policy = execution_axes.DOCLING_MARKDOWN_POLICY
    finalize = extract_document.DOCLING_TEXT_FINALIZE
    assert cpu.split("/")[-3:] == ["docling-9.9.9", policy, finalize]
    assert cuda.split("/")[-3:] == ["docling-9.9.9", policy, finalize]
    assert "+docling-doc-3/cpu/" in cpu and "+docling-doc-3/cuda/" in cuda
    assert cpu != cuda
    # Устройство и сборка не смешиваются: сборка у обоих одна, политика — тоже.
    assert cpu.split("/")[-3] == cuda.split("/")[-3]


def test_extractor_stack_and_extractor_version_share_the_engine_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Д-8: ``run_id``-стек и ключ ``extract_cache`` называют **одну** сборку движка.

    ``extractor_stack`` (объявленная ось контракта) и ``extractor_version`` документа
    (ключ ``extract_cache``/слагаемое ``pb``) собираются из одного вызова
    :func:`core.adapters.extract_document.docling_engine_build`. Тест подменяет версию
    библиотеки и проверяет оба конца: разойдись формулы — прогон объявил бы движок,
    которого не было.
    """
    monkeypatch.setattr(extract_document, "docling_library_version", lambda: "9.9.9")

    stack = execution_axes.extractor_stack_version()
    version = DoclingDocumentExtractor(DocumentLevel.FULL).version

    assert stack.split("/")[-3] == "docling-9.9.9"
    assert (
        version
        == "docling-doc-3:full/cpu/docling-9.9.9/md-plain-underscore/finalize-1"
    )
    # Одна и та же строка сборки в обоих идентификаторах (политика и финализация — тоже).
    assert stack.split("/")[-3] == version.split("/")[-3]
    assert stack.split("/")[-2] == version.split("/")[-2]
    assert stack.split("/")[-1] == version.split("/")[-1]


def test_engine_build_does_not_enter_execution_contract() -> None:
    """Сборка в ключе кэша — не контракт: ``run_id`` от неё не зависит (Д-8).

    ``extractor_version`` остаётся наблюдаемой величиной; в контракт входит только
    ``extractor_stack``. Поэтому расширение ключа ``extract_cache`` сборкой не двигает
    ``run_id`` — иначе одна и та же правка «чинит кэш» и ломает межцикловый diff.
    """
    assert "extractor_version" not in EXECUTION_CONTRACT_FIELDS
    assert "extractor_stack" in EXECUTION_CONTRACT_FIELDS

    values = dict.fromkeys(EXECUTION_CONTRACT_FIELDS, None)
    digest = execution_contract_digest(values)
    # Поле ключа кэша не расширяет контракт: оно не «необязательное», его там нет.
    with pytest.raises(ValueError):
        execution_contract_digest(
            {**values, "extractor_version": "docling-doc-3:full/docling-9.9.9"}
        )
    assert digest == execution_contract_digest(values)


def test_build_manifest_takes_declared_axes(tmp_path: Path) -> None:
    """Манифест берёт оси из того же резолвера, что контракт (один источник)."""
    package = _package(tmp_path)
    inputs = _inputs(package)
    plan = build_plan(inputs, run_id="run-x")
    declared = DeclaredExecutionAxes(
        crawl_strategy="honest",
        crawl_rate_limit="uniform(min=1.0,max=2.0)",
        crawl_config_digest="crawl-dig",
        serp_rounds=2,
        serp_source="run.yaml",
        serp_mode="sync",
        registry_spec_digest="reg-dig",
        deploy_profile="split",
        edge_schema_version="9.9.9",
        chunker_version="md-test-9",
        llm_scenarios=(
            {
                "scenario": "evaluator",
                "model_id": "model-x",
                "model_version": "v1",
                "endpoint_kind": "api",
                "web_tool": False,
                "decoder": "json_object",
            },
        ),
        extractor_stack="html-1+docling-doc-3/docling-9.9.9",
    )
    manifest = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-06-01T00:00:00Z",
        axes=declared,
        declared_status_dict_version=declared_status_dict_version(),
    )
    assert manifest.deploy_profile == "split"
    assert manifest.edge_schema_version == "9.9.9"
    assert manifest.chunker_version == "md-test-9"
    assert manifest.crawl_strategy == "honest"
    assert manifest.crawl_rate_limit == "uniform(min=1.0,max=2.0)"
    assert manifest.crawl_config_digest == "crawl-dig"
    assert manifest.registry_spec_digest == "reg-dig"
    assert manifest.status_dict_version == declared_status_dict_version()
    assert [(binding.scenario.value, binding.model_id) for binding in manifest.llm_scenarios] == [
        ("evaluator", "model-x")
    ]
    assert manifest.llm_scenarios[0].run_id == "run-x"


# ---------------------------------------------------------------------------
# Паритет plan/crawl/run и порядок относительно портов
# ---------------------------------------------------------------------------


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def _common(package: Path) -> list[str]:
    return [
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--json",
    ]


def test_plan_crawl_run_derive_identical_run_id(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Все три входа считают контракт одной функцией от ``(args, inputs, deploy)``."""
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["plan", *common]) == EXIT_OK
    plan_id = _payload(capsys)["run_id"]

    assert main(["crawl", "run", *common, "--until", "plan"]) == EXIT_OK
    crawl_id = _payload(capsys)["run_id"]

    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    run_id = _payload(capsys)["run_id"]

    assert plan_id == crawl_id == run_id


def test_llm_slot_flag_is_part_of_declared_axes_parity(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--llm-slot`` — та же декларированная ось у всех трёх входов (§7.4/I21)."""
    import yaml

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "deploy.yaml").write_text(
        yaml.safe_dump(
            {
                "profile": "local",
                "llm": {
                    "slot": "cheap",
                    "cheap": {
                        "base_url": "https://llm.test/v1",
                        "model_id": "cheap-model",
                    },
                    "flagship": {
                        "base_url": "https://llm.test/v1",
                        "model_id": "flagship-model",
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    package = valid_package(tmp_path)
    common = [*_common(package), "--llm-slot", "flagship"]

    assert main(["plan", *common]) == EXIT_OK
    plan_id = _payload(capsys)["run_id"]
    assert main(["crawl", "run", *common, "--until", "plan"]) == EXIT_OK
    crawl_id = _payload(capsys)["run_id"]
    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    run_id = _payload(capsys)["run_id"]
    assert plan_id == crawl_id == run_id

    # Слот — часть контракта: другой слот → другой run_id.
    assert main(["run", *_common(package), "--llm-slot", "cheap", "--dry-run"]) == EXIT_OK
    assert _payload(capsys)["run_id"] != run_id


def test_llm_model_flag_is_part_of_declared_axes_parity(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--llm-model`` — та же декларированная ось у всех трёх входов (§7.4/I21).

    Флаг питает и runtime-оценщик, и ``llm_scenarios``; ``plan``/``crawl`` считают тот же
    ``run_id``, что ``run`` — иначе identity-preview расходился бы с прогоном.
    """
    import yaml

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "deploy.yaml").write_text(
        yaml.safe_dump(
            {
                "profile": "local",
                "llm": {
                    "slot": "cheap",
                    "cheap": {
                        "base_url": "https://llm.test/v1",
                        "model_id": "slot-model",
                        "decoder": "json_object",
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    package = valid_package(tmp_path)
    common = [*_common(package), "--llm-model", "flag-model"]

    assert main(["plan", *common]) == EXIT_OK
    plan_id = _payload(capsys)["run_id"]
    assert main(["crawl", "run", *common, "--until", "plan"]) == EXIT_OK
    crawl_id = _payload(capsys)["run_id"]
    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    run_id = _payload(capsys)["run_id"]
    assert plan_id == crawl_id == run_id

    # Модель — часть контракта: слот без флага → другой run_id.
    assert main(["run", *_common(package), "--dry-run"]) == EXIT_OK
    assert _payload(capsys)["run_id"] != run_id


def test_pdf_pipeline_flag_is_part_of_execution_contract(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--pdf-pipeline`` — ось механизма извлечения, а не уточнение исполнения (§13).

    Механизм определяет текст документа, значит корпус: прогон с ``text_layer`` — другое
    измерение. Поэтому флаг обязан попадать в ``crawl_config_digest`` → ``run_id`` (иначе
    два разных корпуса жили бы под одним идентификатором), а ``run`` и ``crawl`` —
    считать его одинаково. Дефолт без флага — прежний (``structured``/docling).
    """
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    default_id = _payload(capsys)["run_id"]

    assert main(["run", *common, "--dry-run", "--pdf-pipeline", "structured"]) == EXIT_OK
    structured_id = _payload(capsys)["run_id"]
    assert structured_id == default_id

    assert main(["run", *common, "--dry-run", "--pdf-pipeline", "text_layer"]) == EXIT_OK
    text_layer_id = _payload(capsys)["run_id"]
    assert text_layer_id != default_id

    # Паритет: ``crawl`` адресует тот же прогон, что ``run``.
    assert (
        main(
            [
                "crawl",
                "run",
                *common,
                "--until",
                "plan",
                "--pdf-pipeline",
                "text_layer",
            ]
        )
        == EXIT_OK
    )
    assert _payload(capsys)["run_id"] == text_layer_id


def test_document_level_flag_is_part_of_execution_contract(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--document-level`` — тот же класс оси: уровень меняет текст, значит ``run_id``.

    Регресс на подмену: значения флагов применялись только при сборке фаз, поэтому
    ``run_id`` объявлял один уровень извлечения, а измерялся другой.
    """
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    default_id = _payload(capsys)["run_id"]

    assert main(["run", *common, "--dry-run", "--document-level", "text"]) == EXIT_OK
    text_id = _payload(capsys)["run_id"]
    assert text_id != default_id

    assert (
        main(["crawl", "run", *common, "--until", "plan", "--document-level", "text"])
        == EXIT_OK
    )
    assert _payload(capsys)["run_id"] == text_id


def test_env_model_ignored_notice_is_visible_once_in_run_payload(
    tmp_path: Path,
    home: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Env model/decoder не читаются оценщиком — предупреждение видно в payload, один раз."""
    monkeypatch.setenv(ENV_LLM_MODEL, "from-env")
    monkeypatch.setenv(ENV_LLM_DECODER, "none")
    package = valid_package(tmp_path)

    assert main(["run", *_common(package), "--dry-run"]) == EXIT_OK

    payload = _payload(capsys)
    warnings = payload.get("warnings", [])
    model_notes = [note for note in warnings if ENV_LLM_MODEL in note]
    decoder_notes = [note for note in warnings if ENV_LLM_DECODER in note]
    assert len(model_notes) == 1
    assert len(decoder_notes) == 1
    assert "--llm-model" in model_notes[0]
    # Env не входит в контракт: смена env-модели не меняет run_id (env не в дайджесте).
    monkeypatch.setenv(ENV_LLM_MODEL, "another-env-model")
    assert main(["run", *_common(package), "--dry-run"]) == EXIT_OK
    assert _payload(capsys)["run_id"] == payload["run_id"]


def test_all_three_entrypoints_call_declared_axes_resolver(
    tmp_path: Path,
    home: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Зелёный ``run_id`` без вызова резолвера означал бы три ``None`` — ловим явно."""
    package = valid_package(tmp_path)
    common = _common(package)
    calls: list[str] = []
    real = execution_axes.resolve_declared_axes

    def spy(**kwargs: Any) -> DeclaredExecutionAxes:
        calls.append(str(kwargs["inputs"].run.case_id))
        return real(**kwargs)

    monkeypatch.setattr(plan_mod, "resolve_declared_axes", spy)
    monkeypatch.setattr(crawl_mod, "resolve_declared_axes", spy)
    monkeypatch.setattr(run_mod, "resolve_declared_axes", spy)

    assert main(["plan", *common]) == EXIT_OK
    assert main(["crawl", "run", *common, "--until", "plan"]) == EXIT_OK
    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    assert calls == ["case_x", "case_x", "case_x"]


def test_run_id_does_not_depend_on_ports(
    tmp_path: Path,
    home: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Регресс на порядок: контракт/``run_id`` вычислены **до** сборки портов."""
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    expected = _payload(capsys)["run_id"]

    seen: dict[str, Any] = {}

    def boom(*args: Any, **kwargs: Any) -> Any:
        seen["run_id"] = kwargs.get("run_id")
        raise RuntimeError("порты не должны требоваться для контракта")

    monkeypatch.setattr(run_mod, "_resolve_ports", boom)
    assert main(["run", *common, "--until", "plan"]) == EXIT_ERROR
    assert "порты не должны требоваться" in _payload(capsys)["message"]

    assert seen["run_id"] == expected


# ---------------------------------------------------------------------------
# 13-manual-artifacts §3.2: объявленные ручные артефакты — условие измерения
# ---------------------------------------------------------------------------


def _with_manual_artifact(package: Path, *, sha256: str | None = None) -> Path:
    """Пишет ``sources/manual_artifacts.yaml`` + реальный файл внутрь пакета."""
    body = b"%PDF-1.4 provided by the operator"
    target = package / "sources" / "manual" / "1C.pdf"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    record: dict[str, Any] = {
        "product_id": "p1",
        "url": "https://docs.invalid/files/1C.pdf",
        "path": "sources/manual/1C.pdf",
        "reason": "robots_disallow",
    }
    if sha256 is not None:
        record["sha256"] = sha256
    (package / "sources" / "manual_artifacts.yaml").write_text(
        yaml.safe_dump(
            {"schema": "manual_artifacts/1", "provide": [record]},
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return package


def test_manual_artifacts_change_run_id_and_invalid_declaration_is_usage_error(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Подача меняет корпус ⇒ меняет ``run_id``; битая декларация — видимый отказ.

    Обратная сторона того же требования: объявленный, но несовпавший/нечитаемый артефакт
    не должен молча стать «подачи нет» — иначе прогон посчитал бы корпус без документа,
    который оператор считал предоставленным.
    """
    package = valid_package(tmp_path)

    assert main(["run", *_common(package), "--dry-run"]) == EXIT_OK
    without = _payload(capsys)["run_id"]

    _with_manual_artifact(package)
    assert main(["run", *_common(package), "--dry-run"]) == EXIT_OK
    with_artifact = _payload(capsys)["run_id"]
    assert with_artifact != without

    # Декларация та же, но объявленный ``sha256`` не совпадает с байтами → EXIT_USAGE.
    _with_manual_artifact(package, sha256="0" * 64)
    assert main(["run", *_common(package), "--dry-run"]) == 2  # EXIT_USAGE
    payload = _payload(capsys)
    assert payload["error"] == "manual_artifacts"
    assert "sha256" in payload["message"]


def test_declared_manual_artifact_digest_is_resolver_value(
    tmp_path: Path,
) -> None:
    """Резолвер и загрузчик дают **один** дайджест: второй формулы нет."""
    package = _with_manual_artifact(_package(tmp_path))
    axes = _resolve(package)
    from core.pipeline.manual_artifacts import load_manual_artifacts

    assert axes.manual_artifacts_digest == load_manual_artifacts(package).digest()
    # Пакет без декларации — стабильный дайджест пустого множества (не ``None``).
    empty = _resolve(_package(tmp_path, name="empty"))
    assert empty.manual_artifacts_digest == load_manual_artifacts(None).digest()
    assert empty.manual_artifacts_digest != axes.manual_artifacts_digest

# ---------------------------------------------------------------------------
# §11.1: объявленные входы корпуса — набор URL, режим рендера, паспорта
# ---------------------------------------------------------------------------

def _write_json(path: Path, payload: Any) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path

def test_corpus_urls_are_already_in_the_contract_via_seeds(tmp_path: Path) -> None:
    """``--urls`` входит в идентичность **через** ``seeds_digest``, отдельного поля нет.

    Пин против двух ошибок сразу. Первая: думать, что операторский корпусный вход не
    учтён, и добавить второе поле на тот же материал — тогда два поля менялись бы
    вместе, а разбор расхождения прогонов не смог бы их различить. Вторая: убрать
    ``--urls`` из материала seed-набора, решив, что это «настройка удобства».

    Проверяется наблюдаемое следствие: разные ``--urls`` дают разные ``seeds_digest``
    (А потому и разные ``run_id``), а порядок URL внутри продукта — тоже, потому что
    бюджет краула расходуется в порядке обхода.
    """
    package = _package(tmp_path)
    first = _write_json(tmp_path / "urls_a.json", {"p1": ["https://p1.test/docs"]})
    second = _write_json(tmp_path / "urls_b.json", {"p1": ["https://p1.test/other"]})
    forward = _write_json(
        tmp_path / "urls_fwd.json", {"p1": ["https://p1.test/a", "https://p1.test/b"]}
    )
    backward = _write_json(
        tmp_path / "urls_bwd.json", {"p1": ["https://p1.test/b", "https://p1.test/a"]}
    )

    def _seeds_digest_of(urls: Path) -> str:
        from core.cli.context import _load_corpus_urls
        from core.pipeline.digests import seeds_digest
        from core.pipeline.onboarding import load_effective_seeds

        inputs = _inputs(package)
        config, _ = load_effective_seeds(
            str(package),
            inputs.run.products,
            urls_override=_load_corpus_urls(str(urls)),
        )
        return seeds_digest(config)

    digests = [
        _seeds_digest_of(first),
        _seeds_digest_of(second),
        _seeds_digest_of(forward),
        _seeds_digest_of(backward),
    ]
    assert len(set(digests)) == len(digests), (
        "каждый объявленный набор URL обязан давать свой seeds_digest"
    )
    # Отдельного поля на тот же материал в контракте нет.
    assert "corpus_urls_digest" not in EXECUTION_CONTRACT_FIELDS
    # Резолвер осей не заводит второй оси на тот же материал.
    assert not hasattr(_resolve(package), "corpus_urls_digest")

def test_render_modes_change_the_contract(tmp_path: Path) -> None:
    """``http`` и ``browser`` дают разный текст страницы — значит другое измерение."""
    package = _package(tmp_path)
    http_modes = _write_json(tmp_path / "m_http.json", {"https://p1.test/": "http"})
    browser_modes = _write_json(tmp_path / "m_browser.json", {"https://p1.test/": "browser"})
    a = _resolve(
        package, args=SimpleNamespace(package=str(package), render_modes=str(http_modes))
    )
    b = _resolve(
        package,
        args=SimpleNamespace(package=str(package), render_modes=str(browser_modes)),
    )
    assert a.render_modes_digest != b.render_modes_digest

def test_passports_change_the_contract(tmp_path: Path) -> None:
    """Правка байтов паспорта меняет контракт; переезд каталога — нет.

    Паспорт даёт документы корпуса и сателлиты, то есть доказательства. ``--passports-dir``
    указывал на другой набор файлов, не меняя ``run_id``.
    """
    package = _package(tmp_path)
    products = [entry.product_id for entry in _inputs(package).cohort.entries]
    assert products, "фикстура должна содержать продукты когорты"
    product_id = products[0]

    def _passport_dir(name: str, body: str) -> Path:
        directory = tmp_path / name
        (directory / product_id).mkdir(parents=True)
        (directory / product_id / "2026-01-01.yaml").write_text(body, encoding="utf-8")
        return directory

    first = _passport_dir("pp_a", "title: Паспорт А\n")
    second = _passport_dir("pp_b", "title: Паспорт Б\n")
    moved = tmp_path / "pp_a_moved"
    moved.mkdir()
    (moved / product_id).mkdir()
    (moved / product_id / "2026-01-01.yaml").write_text(
        "title: Паспорт А\n", encoding="utf-8"
    )

    a = _resolve(
        package, args=SimpleNamespace(package=str(package), passports_dir=str(first))
    )
    b = _resolve(
        package, args=SimpleNamespace(package=str(package), passports_dir=str(second))
    )
    relocated = _resolve(
        package, args=SimpleNamespace(package=str(package), passports_dir=str(moved))
    )
    assert a.passports_digest != b.passports_digest
    # Путь — провенанс: те же байты в другом каталоге дают тот же дайджест.
    assert a.passports_digest == relocated.passports_digest


# ---------------------------------------------------------------------------
# 16 §4: ось ``embed.device`` — объявленное устройство эмбеддера
# ---------------------------------------------------------------------------


def _with_embed(package: Path, *, device: str) -> Path:
    """Дописывает секцию ``embed`` в ``run.yaml`` пакета (run-слой оси)."""
    run_yaml = package / "run.yaml"
    data = yaml.safe_load(run_yaml.read_text(encoding="utf-8"))
    data["embed"] = {"device": device}
    run_yaml.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    return package


def test_embed_device_axis_defaults_to_cpu(tmp_path: Path) -> None:
    """Ось не объявлена — ``cpu``: объявленный дефолт, а не «что выберет torch»."""
    assert _resolve(_package(tmp_path)).embed_device == "cpu"


def test_embed_device_run_layer_overrides_deploy(tmp_path: Path) -> None:
    """``run.yaml:embed.device`` сильнее ``deploy.embed.device`` (app < run)."""
    package = _with_embed(_package(tmp_path), device="cuda")
    declared = _resolve(package, deploy=DeployConfig(embed=EmbedConfig(device="cuda")))
    assert declared.embed_device == "cuda"

    # run-слой побеждает app: объявленный ``cpu`` в прогоне отменяет ``cuda`` конфига.
    run_cpu = _with_embed(_package(tmp_path, name="cpu"), device="cpu")
    declared_cpu = _resolve(
        run_cpu, deploy=DeployConfig(embed=EmbedConfig(device="cuda"))
    )
    assert declared_cpu.embed_device == "cpu"


def test_embed_device_value_is_json_serializable(tmp_path: Path) -> None:
    """Поле контракта/манифеста — строка, а не enum: иначе словарь не сериализуется."""
    declared = _resolve(_package(tmp_path))
    assert json.loads(json.dumps({"embed_device": declared.embed_device})) == {
        "embed_device": "cpu"
    }


# ---------------------------------------------------------------------------
# 16 §4.3: выбор эмбеддера по объявленному устройству
# ---------------------------------------------------------------------------


class _FakeEmbedder:
    """Подмена порта эмбеддера: запоминает переданное устройство."""

    def __init__(self, device: str = "cpu") -> None:
        self.device = device
        self.model_id = "fake"
        self.dim = 4
        self.available = True


def _stub_embed_stack(
    monkeypatch: pytest.MonkeyPatch,
    *,
    model2vec: bool,
    sentence_transformers: bool,
) -> dict[str, Any]:
    """Подменяет availability-предикаты и ``LocalEmbedder`` в wiring."""
    seen: dict[str, Any] = {}

    class _Local(_FakeEmbedder):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(device=kwargs.get("device", "<не передан>"))
            seen["local_kwargs"] = dict(kwargs)

    monkeypatch.setattr(wiring_mod, "model2vec_available", lambda: model2vec)
    monkeypatch.setattr(
        wiring_mod, "_module_installed", lambda name: sentence_transformers
    )
    monkeypatch.setattr(wiring_mod, "LocalEmbedder", _Local)
    return seen


def test_cuda_excludes_lite_embedder_with_visible_note(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cuda``: ``LiteEmbedder`` не может исполнить ось — исключён, и это видно.

    Молчаливый выбор Lite дал бы CPU-векторы под объявленным ``cuda`` — тот самый класс,
    ради которого ось вводится. Заметка обязательна: решение не должно быть тихим.
    """
    seen = _stub_embed_stack(monkeypatch, model2vec=True, sentence_transformers=True)
    lite = _FakeEmbedder()
    monkeypatch.setattr(wiring_mod, "build_lite_embedder", lambda: lite)

    notices: list[str] = []
    resolved = wiring_mod._resolve_embedder(  # noqa: SLF001 — таблица §4.3 проверяется напрямую
        EmbedDevice.CUDA, notices=notices
    )

    assert resolved is not lite
    assert isinstance(resolved, _FakeEmbedder) and resolved.device == "cuda"
    assert seen["local_kwargs"] == {"device": "cuda"}
    assert len(notices) == 1
    assert "LiteEmbedder исключён" in notices[0]
    assert "cuda" in notices[0]


def test_cuda_without_sentence_transformers_is_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cuda`` без стека ``LocalEmbedder`` — отказ (``EXIT_USAGE``), а не CPU-фолбэк."""
    _stub_embed_stack(monkeypatch, model2vec=True, sentence_transformers=False)

    with pytest.raises(wiring_mod.WiringError) as excinfo:
        wiring_mod._resolve_embedder(EmbedDevice.CUDA)  # noqa: SLF001
    message = str(excinfo.value)
    assert "embed.device" in message
    assert "embed.device=cpu" in message
    assert "sentence-transformers" in message


def test_cpu_keeps_lite_embedder_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    """``cpu``: прежний приоритет (Lite → Local) — обе реализации CPU-честны."""
    _stub_embed_stack(monkeypatch, model2vec=True, sentence_transformers=True)
    lite = _FakeEmbedder()
    monkeypatch.setattr(wiring_mod, "build_lite_embedder", lambda: lite)

    assert wiring_mod._resolve_embedder(EmbedDevice.CPU) is lite  # noqa: SLF001


def test_local_embedder_receives_declared_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``LocalEmbedder`` получает ``device`` из оси — устройство не выбирает библиотека."""
    seen = _stub_embed_stack(monkeypatch, model2vec=False, sentence_transformers=True)

    cpu = wiring_mod._resolve_embedder(EmbedDevice.CPU)  # noqa: SLF001
    assert isinstance(cpu, _FakeEmbedder) and cpu.device == "cpu"
    assert seen["local_kwargs"] == {"device": "cpu"}

    cuda = wiring_mod._resolve_embedder("cuda")  # noqa: SLF001 — строка тоже принимается
    assert isinstance(cuda, _FakeEmbedder) and cuda.device == "cuda"
    assert seen["local_kwargs"] == {"device": "cuda"}


def test_require_embed_device_passes_cpu_always() -> None:
    """``cpu`` не требует устройства — проверка проходит на любом хосте."""
    wiring_mod.require_embed_device(EmbedDevice.CPU)
    wiring_mod.require_embed_device("cpu")


def test_require_embed_device_cuda_without_device_is_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cuda`` без устройства — отказ с числами (``EXIT_USAGE``), а не тихий CPU."""
    monkeypatch.setattr(wiring_mod, "cuda_available", lambda: False)
    monkeypatch.setattr(wiring_mod, "cuda_total_vram_mb", lambda: None)

    with pytest.raises(wiring_mod.WiringError) as excinfo:
        wiring_mod.require_embed_device(EmbedDevice.CUDA)
    message = str(excinfo.value)
    # Сообщение называет **свою** ось (не ``crawl.pdf_device``) и несёт факты.
    assert "embed.device=cuda" in message
    assert "crawl.pdf_device" not in message
    assert "cuda_available=False" in message
    assert "device_vram_mb=нет устройства" in message
