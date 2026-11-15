"""Проводка бюджета кэша экстракции из deploy-конфига (D8, решение 15.11.2026).

Механизм вытеснения живёт в ядре (``ExtractionCache._evict_if_needed``, LRU по ``mtime``)
и проверяется своим тестом (``tests/pipeline/test_extract_cache.py``). Здесь держится
**шов**, который юнит-тест кэша не видит: бюджет — параметр развёртывания, и если
composition root (``core.cli.context._resolve_phases``) не передаст его в конструктор,
ось существует только на бумаге.

Два независимых факта:

* ``deploy.extract.cache_max_bytes`` доезжает до ``ExtractionCache.max_bytes``;
* не объявленный бюджет (``None``) остаётся ``None`` — вытеснения нет, поведение прежнее.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.cli.context import _resolve_phases
from core.config.deploy import DeployConfig, ExtractDeployConfig
from core.config.paths import PathsConfig


def _recording_factory() -> tuple[Any, dict[str, Any]]:
    """``phases_factory``, который запоминает kwargs вместо сборки фаз."""
    captured: dict[str, Any] = {}

    def factory(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "phases-sentinel"

    return factory, captured


def _args(factory: Any) -> Any:
    """Минимальный ``Namespace``: ``_resolve_phases`` читает из него флаги и фабрику."""
    import argparse

    return argparse.Namespace(phases_factory=factory)


def _inputs() -> Any:
    """Минимальный ``RunInputs``: доменные профили и слои источников пусты."""
    import argparse

    return argparse.Namespace(domain_profiles={}, crawl_sources=None, run=None)


def test_declared_budget_reaches_the_extract_cache(tmp_path: Path) -> None:
    """``deploy.extract.cache_max_bytes`` → ``ExtractionCache.max_bytes`` (и каталог раскладки)."""
    factory, captured = _recording_factory()
    paths = PathsConfig(extract_cache=tmp_path / "extract-cache")

    _resolve_phases(
        args=_args(factory),
        inputs=_inputs(),
        plan=None,
        ports=None,
        deploy_config=DeployConfig(extract=ExtractDeployConfig(cache_max_bytes=4096)),
        paths=paths,
    )

    cache = captured["extract_cache"]
    assert cache is not None, "resolved-раскладка ⇒ кэш включён"
    assert cache.max_bytes == 4096
    assert Path(cache.root) == tmp_path / "extract-cache"


def test_absent_budget_keeps_eviction_off(tmp_path: Path) -> None:
    """Бюджет не объявлен ⇒ ``None``: вытеснения нет (сегодняшнее поведение)."""
    factory, captured = _recording_factory()
    paths = PathsConfig(extract_cache=tmp_path / "extract-cache")

    _resolve_phases(
        args=_args(factory),
        inputs=_inputs(),
        plan=None,
        ports=None,
        # Секция не объявлена вовсе — дефолт ``None``.
        deploy_config=DeployConfig(),
        paths=paths,
    )

    assert captured["extract_cache"].max_bytes is None
