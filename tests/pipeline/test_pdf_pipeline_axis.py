"""Ось ``crawl.pdf_pipeline`` (P7): механизм извлечения PDF как условие измерения.

Проверяется главное свойство оси — **механизм определяет текст документа**, поэтому:

* дефолт — ``structured`` (сегодняшнее поведение, docling), а не «оптимизация по умолчанию»;
* ось входит в ``CrawlLimits.as_dict()`` **безусловно** (как ``document_level``/``robots``):
  прогон с другим механизмом — другое измерение, и молчать об этом в дайджесте нельзя;
* резолвится цепочкой app < run < site, причём ``None`` на слое означает «наследовать»
  (иначе дефолт run-слоя перекрыл бы объявленный ``deploy.yaml``);
* смена механизма меняет ``crawl_config_digest`` → ``execution_contract_digest`` →
  ``run_id`` (``execution_change``, §13), а ``METHODOLOGY_VERSION`` не трогает.
"""

from __future__ import annotations

import datetime as _dt
from types import SimpleNamespace
from typing import Any

from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain.enums import PdfPipeline
from core.domain.models import CrawlRunConfig, RunConfig
from core.pipeline.crawl_frontier import CrawlLimits, resolve_crawl_limits
from core.pipeline.plan import derive_run_id
from core.pipeline.report import crawl_control_digest
from core.pipeline.run_identity import build_execution_contract

URL = "https://docs.example.test/guide/"


def test_effective_default_is_structured() -> None:
    """Дефолт = сегодняшнее поведение: docling (layout + таблицы + OCR)."""
    assert CrawlConfig().pdf_pipeline is PdfPipeline.STRUCTURED
    assert CrawlLimits().pdf_pipeline is PdfPipeline.STRUCTURED
    resolved = resolve_crawl_limits(URL, app=CrawlConfig(), run=CrawlRunConfig())
    assert resolved.pdf_pipeline is PdfPipeline.STRUCTURED


def test_run_layer_default_is_inherit_not_structured() -> None:
    """``None`` на run-слое = «наследовать»: иначе app/site-слои стали бы недостижимы."""
    assert CrawlRunConfig().pdf_pipeline is None
    app = CrawlConfig(pdf_pipeline=PdfPipeline.TEXT_LAYER)
    resolved = resolve_crawl_limits(URL, app=app, run=CrawlRunConfig())
    assert resolved.pdf_pipeline is PdfPipeline.TEXT_LAYER


def test_axis_is_unconditional_in_as_dict() -> None:
    """Ключ есть всегда (механизм — условие измерения, а не опциональная ручка)."""
    assert CrawlLimits().as_dict()["pdf_pipeline"] == "structured"
    explicit = CrawlLimits(pdf_pipeline=PdfPipeline.TEXT_LAYER)
    assert explicit.as_dict()["pdf_pipeline"] == "text_layer"


def test_layers_override_in_order_site_run_app() -> None:
    app = CrawlConfig(pdf_pipeline=PdfPipeline.TEXT_LAYER)
    run = CrawlRunConfig(pdf_pipeline=PdfPipeline.STRUCTURED)
    assert (
        resolve_crawl_limits(URL, app=app, run=run).pdf_pipeline
        is PdfPipeline.STRUCTURED
    )
    profiles = {"docs.example.test": DomainProfile(pdf_pipeline=PdfPipeline.TEXT_LAYER)}
    assert (
        resolve_crawl_limits(URL, app=app, run=run, profiles=profiles).pdf_pipeline
        is PdfPipeline.TEXT_LAYER
    )


def test_pipeline_changes_crawl_control_digest() -> None:
    structured = crawl_control_digest(
        run_crawl=CrawlRunConfig(pdf_pipeline=PdfPipeline.STRUCTURED),
        app=CrawlConfig(),
        profiles=None,
    )
    text_layer = crawl_control_digest(
        run_crawl=CrawlRunConfig(pdf_pipeline=PdfPipeline.TEXT_LAYER),
        app=CrawlConfig(),
        profiles=None,
    )
    assert structured != text_layer


def _run_config() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor="p1",
        cohort="c1",
        products=["p1"],
        package="pkg",
        snapshot_date=_dt.date(2026, 6, 1),
        mode="strict",
    )


def _inputs(pipeline: PdfPipeline) -> Any:
    """Минимальные входы контракта: run-слой (``crawl`` — ось) + версия пакета."""
    return SimpleNamespace(
        run=_run_config(),
        package=SimpleNamespace(package_version="1.0.0"),
        domain_profiles=None,
        crawl=CrawlRunConfig(pdf_pipeline=pipeline),
    )


def _run_id(pipeline: PdfPipeline) -> str:
    """``run_id`` через настоящую цепочку: digest краула → контракт → ``derive_run_id``.

    Дайджест считается тем же :func:`crawl_control_digest`, что у манифеста, а не второй
    формулой: иначе тест проверял бы копию формулы, а не поведение.
    """
    run_crawl = CrawlRunConfig(pdf_pipeline=pipeline)
    declared = SimpleNamespace(
        crawl_config_digest=crawl_control_digest(
            run_crawl=run_crawl, app=CrawlConfig(), profiles=None
        )
    )
    inputs = _inputs(pipeline)
    contract = build_execution_contract(
        inputs,
        cohort_digest="cohort-digest",
        seeds_digest="seeds-digest",
        declared=declared,
        status_dict_version="1",
    )
    return derive_run_id(
        inputs,
        cycle_id="2026-W22",
        cohort_digest="cohort-digest",
        seeds_digest="seeds-digest",
        execution_contract_digest=contract,
    )


def test_pipeline_changes_run_id() -> None:
    """Смена механизма PDF — другой ``run_id`` (``execution_change``, §13)."""
    assert _run_id(PdfPipeline.STRUCTURED) != _run_id(PdfPipeline.TEXT_LAYER)
    # Тот же механизм → тот же идентификатор (детерминизм).
    assert _run_id(PdfPipeline.STRUCTURED) == _run_id(PdfPipeline.STRUCTURED)
