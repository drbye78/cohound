"""Проводка межпрогонного кэша векторов эмбеддинга (``embed_cache``) из CLI.

Кэш живёт в ядре (``core/pipeline/embed_cache.py``) и принимается ядром
(``build_corpus``), но собирается он в composition root — ``core.cli.context._resolve_phases``,
рядом с ``extract_cache``/``eval_cache``. Тест держит именно шов, который нельзя проверить
юнит-тестом кэша: если CLI не положит объект в kwargs, фаза его не получит, и фича
существует только на бумаге.

Проверяется два независимых факта:

* ``embed_cache`` **есть** в kwargs фаз и построен по resolved-раскладке
  (``paths.embed_cache``) — иначе кэш не подключён;
* при ``paths is None`` он ``None``, как у соседних кэшей: у кэша без resolved-раскладки нет
  определённой области, а CWD-дефолт сделал бы его общим для всех процессов и тестов.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.cli.context import _resolve_phases
from core.config.paths import PathsConfig


def _recording_factory() -> tuple[Any, dict[str, Any]]:
    """``phases_factory``, который запоминает kwargs вместо сборки фаз."""
    captured: dict[str, Any] = {}

    def factory(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "phases-sentinel"

    return factory, captured


def test_cli_passes_embed_cache_to_phases_from_layout(tmp_path: Path) -> None:
    """``embed_cache`` доезжает до фаз и указывает в resolved-каталог раскладки."""
    factory, captured = _recording_factory()
    paths = PathsConfig(embed_cache=tmp_path / "vec-cache")

    _resolve_phases(
        args=_args(factory),
        inputs=_inputs(),
        plan=None,
        ports=None,
        deploy_config=None,
        paths=paths,
    )

    assert "embed_cache" in captured, "CLI обязан передать кэш векторов фазам"
    cache = captured["embed_cache"]
    assert cache is not None, "resolved-раскладка ⇒ кэш включён"
    assert Path(getattr(cache, "root", getattr(cache, "_root", ""))) == tmp_path / "vec-cache"


def test_cli_disables_embed_cache_without_layout() -> None:
    """``paths is None`` ⇒ кэш выключен (та же причина, что у ``extract_cache``)."""
    factory, captured = _recording_factory()

    _resolve_phases(
        args=_args(factory),
        inputs=_inputs(),
        plan=None,
        ports=None,
        deploy_config=None,
        paths=None,
    )

    assert captured["embed_cache"] is None
    # Соседние кэши ведут себя так же — поведение не расходится.
    assert captured["extract_cache"] is None
    assert captured["eval_cache"] is None


def _args(factory: Any) -> Any:
    """Минимальный ``Namespace``: ``_resolve_phases`` читает из него флаги и фабрику."""
    import argparse

    return argparse.Namespace(phases_factory=factory)


def _inputs() -> Any:
    """Минимальный ``RunInputs``: доменные профили и слои источников пусты.

    ``_resolve_phases`` читает из входов ровно эти два поля до сборки kwargs; всё остальное
    (кэши, пути, бюджеты) приходит аргументами. Заглушка держит тест на шве проводки, а не
    на загрузке пакета.
    """
    import argparse

    return argparse.Namespace(domain_profiles={}, crawl_sources=None, run=None)
