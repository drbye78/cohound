"""Тесты доменной модели ``CatalogRow`` и §13-полей ``deprecated``/``replaced_by``.

Проверяют дефолты (обратная совместимость), явные значения и чтение опциональных
CSV-колонок загрузчиком ``core.config.run_config.load_catalog``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.config.run_config import load_catalog
from core.domain.enums import Importance, ParamType
from core.domain.models import CatalogRow


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
