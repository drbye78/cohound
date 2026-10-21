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

import core.cli.commands.crawl as crawl_mod
import core.cli.commands.plan as plan_mod
import core.cli.commands.run as run_mod
import core.cli.wiring as wiring_mod
from core.cli import execution_axes
from core.cli.execution_axes import (
    DeclaredExecutionAxes,
    declared_status_dict_version,
    resolve_declared_axes,
)
from core.cli.main import EXIT_ERROR, EXIT_OK, main
from core.config.deploy import (
    CrawlConfig,
    DeployConfig,
    EdgeConfig,
    LLMDeployConfig,
    LLMSlotConfig,
)
from core.domain.enums import CrawlStrategy
from core.domain.models import CrawlRunConfig, RateLimitSpec
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS
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


def _args(package: Path, *, llm_slot: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(package=str(package), llm_slot=llm_slot)


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
        "trafilatura-html-2+docling-doc-3/docling-"
    )
    # Девять объявленных осей — ровно по списку §13; наблюдаемых здесь нет.
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
    }


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
        monkeypatch.setattr(execution_axes.MarkdownChunker, "version", "md-test-9")
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
            execution_axes, "_docling_library_version", lambda: "9.9.9"
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
        },
    )
    assert "run_id" not in rows[0]


def test_docling_version_uses_installed_distribution() -> None:
    """Версия библиотеки читается из метаданных дистрибутива (``docling``/``docling-slim``)."""
    from importlib.metadata import PackageNotFoundError, version

    expected: list[str] = []
    for dist in ("docling", "docling-slim"):
        try:
            expected.append(version(dist))
        except PackageNotFoundError:
            continue
    assert execution_axes._docling_library_version() in (expected or ["unknown"])


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


def test_build_manifest_takes_declared_axes(tmp_path: Path) -> None:
    """Манифест берёт оси из того же резолвера, что контракт (один источник)."""
    package = _package(tmp_path)
    inputs = _inputs(package)
    plan = build_plan(inputs, run_id="run-x")
    declared = DeclaredExecutionAxes(
        crawl_strategy="honest",
        crawl_rate_limit="uniform(min=1.0,max=2.0)",
        crawl_config_digest="crawl-dig",
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
