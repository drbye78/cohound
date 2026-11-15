"""D9 (план 26): продуктовый слой лимитов PDF доезжает до идентичности прогона.

**Какую регрессию закрывает файл.** Схема и резолвер продуктовый слой знали
(:class:`ProductCrawlAxes`, ``resolve_crawl_limits(product=...)``,
``crawl_control_digest(product_axes=...)``), а **производственный** путь его не передавал:
ось, объявленная в ``packages/{case}/crawl_policy.yaml: product_axes``, молча не
действовала — «объявил и не подействовало» было бы ложным провенансом (§13). Поэтому
проверка идёт **тем же путём**, что ``hound plan``/``hound run``: пакет на диске →
``load_inputs`` → ``resolve_declared_axes``, а не прямым вызовом ``crawl_control_digest``.

Два факта, которые обязаны держаться вместе:

* объявленная ось **меняет** ``crawl_config_digest`` (лимит решает состав текста
  документа, то есть состав корпуса — ``execution_change``);
* прогон **без** оси даёт прежний дайджест байт-в-байт (появление кода не сдвигает
  ``run_id`` у прогона, который ничего не объявлял).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from core.cli.execution_axes import resolve_declared_axes
from core.config.deploy import DeployConfig
from core.pipeline.normalize import load_inputs
from core.pipeline.report import crawl_control_digest
from tests.helpers.packages import valid_package

#: Тело секции ``product_axes``: продукт ``p1`` — тот, что объявлен когортой фикстуры.
_AXES_YAML = (
    "product_axes:\n"
    "  p1:\n"
    "    pdf_max_pages: 5\n"
    "    pdf_max_extracted_chars: 1000\n"
)


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD: деплой-конфиг — дефолтный ``local``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.chdir(tmp_path)
    return root


def _package(tmp_path: Path, name: str, *, axes: str | None) -> Path:
    """Линт-чистый пакет фикстуры; ``axes`` — тело ``crawl_policy.yaml`` (``None`` — нет файла)."""
    base = tmp_path / name
    base.mkdir()
    package = valid_package(base)
    if axes is not None:
        (package / "crawl_policy.yaml").write_text(axes, encoding="utf-8")
    return package


def _declared(package: Path) -> tuple[Any, Any]:
    """``(inputs, declared)`` — нормализованные входы и декларированные оси прогона."""
    inputs = load_inputs(package / "run.yaml", package)
    declared = resolve_declared_axes(
        args=SimpleNamespace(
            package=str(package),
            llm_slot=None,
            llm_model=None,
            llm_reasoning_effort=None,
        ),
        inputs=inputs,
        deploy=DeployConfig(),
    )
    return inputs, declared


def _digest(inputs: Any, *, product_axes: Any = None) -> str | None:
    """Дайджест **той же** формулой, что у контракта (вторая формула разошлась бы)."""
    return crawl_control_digest(
        run_crawl=getattr(inputs.run, "crawl", None),
        app=DeployConfig().crawl,
        profiles=inputs.domain_profiles,
        product_axes=product_axes,
    )


def test_absent_product_axes_keep_the_declared_digest_byte_identical(
    tmp_path: Path, home: Path
) -> None:
    """Пакет без ``product_axes``: ``resolve_declared_axes`` даёт **прежний** дайджест.

    Проверяется именно производственный путь: вызов без ``product_axes`` — эталон
    (формула до D9), и ``resolve_declared_axes`` обязан прийти к тому же значению.
    """
    package = _package(tmp_path, "plain", axes=None)
    inputs, declared = _declared(package)

    assert inputs.crawl_policy.product_axes == {}
    assert declared.crawl_config_digest == _digest(inputs)


def test_package_declared_product_axes_change_the_declared_digest(
    tmp_path: Path, home: Path
) -> None:
    """Ось из ``crawl_policy.yaml`` меняет дайджест **через** ``resolve_declared_axes``.

    Это и есть регрессия: до проводки пакет с объявленной осью давал тот же дайджест,
    что пакет без неё, — то есть два разных состава корпуса жили под одной идентичностью.
    """
    plain_pkg = _package(tmp_path, "plain", axes=None)
    axes_pkg = _package(tmp_path, "axes", axes=_AXES_YAML)
    _, plain_declared = _declared(plain_pkg)
    axes_inputs, axes_declared = _declared(axes_pkg)

    # Загрузчик прочитал секцию: ось — данные пакета, а не выдумка теста.
    axes = axes_inputs.crawl_policy.axes_for("p1")
    assert axes is not None
    assert axes.pdf_max_pages == 5
    assert axes.pdf_max_extracted_chars == 1000

    assert axes_declared.crawl_config_digest != plain_declared.crawl_config_digest
    # Производственный путь передаёт **тот же** загруженный конфиг, а не вторую формулу.
    assert axes_declared.crawl_config_digest == _digest(
        axes_inputs, product_axes=axes_inputs.crawl_policy.product_axes
    )
    # Другой предел — другая величина (слой входит в материал, а не только в факт наличия).
    other = _digest(
        axes_inputs,
        product_axes={"p1": axes_inputs.crawl_policy.product_axes["p1"].model_copy(
            update={"pdf_max_pages": 50}
        )},
    )
    assert other != axes_declared.crawl_config_digest
