"""Тесты доменной модели ``CatalogRow`` и §13-полей ``deprecated``/``replaced_by``.

Проверяют дефолты (обратная совместимость), явные значения и чтение опциональных
CSV-колонок загрузчиком ``core.config.run_config.load_catalog``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from core.config.run_config import load_catalog
from core.domain.enums import Importance, ParamType
from core.domain.models import CatalogRow, CrawlRunConfig, RateLimitSpec
from core.ports.types import FetchPlan


def _row(**overrides: object) -> CatalogRow:
    base: dict[str, object] = {
        "param_id": "param.x",
        "title": "X",
        "module": "m",
        "group": "g",
        "importance": Importance.BASIC,
        "param_type": ParamType.FACT,
        "rule": "rule",
    }
    base.update(overrides)
    return CatalogRow.model_validate(base)


def test_catalog_row_deprecation_defaults() -> None:
    row = _row()
    assert row.deprecated is False
    assert row.replaced_by is None


def test_catalog_row_deprecation_explicit() -> None:
    row = _row(deprecated=True, replaced_by="param.y")
    assert row.deprecated is True
    assert row.replaced_by == "param.y"


def test_load_catalog_without_optional_columns(tmp_path: Path) -> None:
    path = tmp_path / "catalog.csv"
    path.write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        "param.a,T,m,g,Баз,fact,rule,explicit,,,ru\n",
        encoding="utf-8",
    )
    row = load_catalog(path)[0]
    assert row.deprecated is False
    assert row.replaced_by is None


def test_load_catalog_with_optional_columns(tmp_path: Path) -> None:
    path = tmp_path / "catalog.csv"
    path.write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs,deprecated,replaced_by\n"
        "param.a,T,m,g,Баз,fact,rule,explicit,,,ru,true,param.b\n",
        encoding="utf-8",
    )
    row = load_catalog(path)[0]
    assert row.deprecated is True
    assert row.replaced_by == "param.b"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("true", True), ("YES", True), ("1", True), ("да", True), ("false", False), ("", False)],
)
def test_load_catalog_bool_variants(tmp_path: Path, raw: str, expected: bool) -> None:
    path = tmp_path / "catalog.csv"
    path.write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs,deprecated,replaced_by\n"
        f"param.a,T,m,g,Баз,fact,rule,explicit,,,ru,{raw},\n",
        encoding="utf-8",
    )
    assert load_catalog(path)[0].deprecated is expected


# ---------------------------------------------------------------------------
# D4: RateLimitSpec / CrawlRunConfig
# ---------------------------------------------------------------------------


def test_rate_limit_spec_equal_bounds_ok() -> None:
    spec = RateLimitSpec(min_s=3.0, max_s=3.0)
    assert spec.min_s == 3.0
    assert spec.max_s == 3.0


def test_rate_limit_spec_max_below_min_rejected() -> None:
    with pytest.raises(ValidationError, match="max_s"):
        RateLimitSpec(min_s=5.0, max_s=1.0)


def test_rate_limit_spec_negative_rejected() -> None:
    with pytest.raises(ValidationError):
        RateLimitSpec(min_s=-1.0, max_s=2.0)


def test_crawl_run_config_defaults() -> None:
    cfg = CrawlRunConfig()
    assert cfg.strategy == "auto"
    assert cfg.rate_limit is None


def test_crawl_run_config_strategy_and_rate() -> None:
    cfg = CrawlRunConfig.model_validate(
        {"strategy": "hybrid", "rate_limit": {"min_s": 3, "max_s": 10}}
    )
    assert cfg.strategy == "hybrid"
    assert cfg.rate_limit == RateLimitSpec(min_s=3.0, max_s=10.0)


# ---------------------------------------------------------------------------
# D4: неизменяемость и числовая корректность RateLimitSpec (G1)
# ---------------------------------------------------------------------------


def test_rate_limit_spec_zero_zero_valid_no_throttle() -> None:
    """``(0, 0)`` — валидный «без троттлинга»."""
    spec = RateLimitSpec(min_s=0.0, max_s=0.0)
    assert spec.min_s == 0.0
    assert spec.max_s == 0.0


def test_rate_limit_spec_rejects_inf() -> None:
    with pytest.raises(ValidationError):
        RateLimitSpec(min_s=0.0, max_s=float("inf"))


def test_rate_limit_spec_rejects_nan() -> None:
    with pytest.raises(ValidationError):
        RateLimitSpec(min_s=float("nan"), max_s=1.0)


@pytest.mark.parametrize(
    ("factory", "kwargs"),
    [
        pytest.param(RateLimitSpec, {"min_s": 1.0, "max_s": 2.0}, id="rate-limit-spec"),
        pytest.param(
            FetchPlan,
            {"rate_limit": RateLimitSpec(min_s=1.0, max_s=2.0)},
            id="fetch-plan-with-rate-limit",
        ),
    ],
)
def test_frozen_value_objects_hashable(factory, kwargs) -> None:
    """Frozen value-объекты хешируемы (в т.ч. непустой ``rate_limit`` не ломает ``FetchPlan``)."""
    assert hash(factory(**kwargs)) == hash(factory(**kwargs))
