"""Тесты загрузчиков конфигурации и входов (W5/T5.1).

Проверяют happy-path каждого артефакта из ``tests/config/fixtures/`` и негативные
случаи валидации домена. Фикстуры и данные категорийно-нейтральны: ``p1/p2``,
``case_x``, ``mod_a``, ``role_a``, ``param_a`` — без названий продуктов/вендоров (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from core.config import (
    cross_check_products,
    load_baselines,
    load_catalog,
    load_cohort,
    load_domain_profiles,
    load_golden,
    load_package_manifest,
    load_run_config,
)
from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    Archetype,
    CrawlStrategy,
    ExtractionRoute,
    FetchMode,
    GoldenTier,
    InheritanceMode,
    ParamType,
    RankingPolicy,
    RateLimitSpec,
    RunMode,
    SourceKind,
    Status,
)

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, name: str, text: str) -> Path:
    """Пишет временный артефакт и возвращает путь."""
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _load_yaml(path: Path) -> dict[str, Any]:
    """Читает YAML-фикстуру как словарь для последующей мутации."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


# ---------------------------------------------------------------------------
# Happy-path
# ---------------------------------------------------------------------------


def test_load_run_config_happy() -> None:
    cfg = load_run_config(FIXTURES / "run.yaml")
    assert cfg.case_id == "case_x"
    assert cfg.anchor == "p1"
    assert cfg.products == ["p1", "p2"]
    assert cfg.mode is RunMode.STRICT
    assert cfg.ranking_policy is RankingPolicy.CAPABILITY_RANKED
    assert cfg.focus.modules == ["mod_a"]


def test_load_run_config_crawl_absent_is_none() -> None:
    cfg = load_run_config(FIXTURES / "run.yaml")
    assert cfg.crawl is None


def test_load_run_config_crawl_parsed(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "run.yaml")
    data["crawl"] = {"strategy": "hybrid", "rate_limit": {"min_s": 3, "max_s": 10}}
    path = _write(tmp_path, "run.yaml", yaml.safe_dump(data, allow_unicode=True))
    cfg = load_run_config(path)
    assert cfg.crawl is not None
    assert cfg.crawl.strategy is CrawlStrategy.HYBRID
    assert cfg.crawl.rate_limit == RateLimitSpec(min_s=3.0, max_s=10.0)


def test_load_run_config_crawl_strategy_only(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "run.yaml")
    data["crawl"] = {"strategy": "honest"}
    path = _write(tmp_path, "run.yaml", yaml.safe_dump(data, allow_unicode=True))
    cfg = load_run_config(path)
    assert cfg.crawl is not None
    assert cfg.crawl.strategy is CrawlStrategy.HONEST
    assert cfg.crawl.rate_limit is None


def test_load_package_manifest_happy() -> None:
    manifest = load_package_manifest(FIXTURES / "package.yaml")
    assert manifest.case_id == "case_x"
    assert manifest.archetype is Archetype.B
    assert round(sum(m.weight for m in manifest.modules), 6) == 100
    assert manifest.baseline_participant == "baseline_a"
    assert manifest.roles[0].role_id == "role_a"


def test_load_catalog_happy() -> None:
    rows = load_catalog(FIXTURES / "catalog.csv")
    assert [r.param_id for r in rows] == ["param_a", "param_b"]
    fact = rows[0]
    assert fact.param_type is ParamType.FACT
    assert fact.absence_rule is AbsenceRuleType.EXPLICIT
    assert fact.concept_terms == {"ru": ["a"], "en": ["a"]}
    assert fact.search_langs == ["ru", "en"]
    showcase = rows[1]
    assert showcase.param_type is ParamType.SHOWCASE
    assert showcase.absence_rule is AbsenceRuleType.DISTRIBUTION_ABSENCE
    assert showcase.concept_terms == {"ru": ["b"], "en": ["b"]}


def test_load_baselines_happy() -> None:
    rows = load_baselines(FIXTURES / "baselines.csv")
    assert [r.baseline_id for r in rows] == ["baseline_a", "baseline_b"]
    assert rows[0].inheritance_mode is InheritanceMode.DIRECT
    assert rows[1].inheritance_mode is InheritanceMode.CONDITIONAL
    assert rows[1].min_version is None


def test_load_cohort_happy() -> None:
    cohort = load_cohort(FIXTURES / "cohort.yaml")
    assert cohort.case_id == "case_x"
    assert {e.product_id for e in cohort.entries} == {"p1", "p2"}
    anchors = [e for e in cohort.entries if e.anchor_relation is AnchorRelation.ANCHOR]
    assert len(anchors) == 1 and anchors[0].product_id == "p1"


def test_load_golden_happy() -> None:
    rows = load_golden(FIXTURES / "golden.csv")
    by_id = {r.param_id: r for r in rows}
    assert by_id["param_a"].tier is GoldenTier.FAST
    assert by_id["param_a"].expected_status is Status.OK
    assert by_id["param_a"].expected_evidence_kind == [SourceKind.DOCUMENTATION, SourceKind.WEB]
    assert by_id["param_b"].tier is GoldenTier.FULL
    assert by_id["param_b"].expected_status is Status.ABSENT


def test_load_golden_tier_defaults_to_full(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "golden.csv",
        "product_id,param_id,expected_status,expected_evidence_kind,case_class\n"
        "p1,param_a,✅,documentation,class_a\n",
    )
    (row,) = load_golden(path)
    assert row.tier is GoldenTier.FULL


def test_load_domain_profiles_happy() -> None:
    profiles = load_domain_profiles(FIXTURES / "domain_profiles.yaml")
    assert profiles["case_x"].fetch is FetchMode.HTTP
    assert profiles["case_x"].extract is ExtractionRoute.HTML


# ---------------------------------------------------------------------------
# S11: products — проекция когорты (§4.5/§1.6)
# ---------------------------------------------------------------------------


def test_cross_check_products_ok() -> None:
    cfg = load_run_config(FIXTURES / "run.yaml")
    cohort = load_cohort(FIXTURES / "cohort.yaml")
    assert cross_check_products(cfg, cohort) == []


def test_cross_check_products_detects_extra() -> None:
    cfg = load_run_config(FIXTURES / "run.yaml")
    cohort = load_cohort(FIXTURES / "cohort.yaml")
    # model_copy не запускает валидаторы — намеренно вносим рассинхрон S11.
    cfg = cfg.model_copy(update={"products": ["p1", "p2", "p3"]})
    violations = cross_check_products(cfg, cohort)
    assert len(violations) == 1
    assert "S11" in violations[0] and "p3" in violations[0]


def test_cross_check_products_detects_missing() -> None:
    cfg = load_run_config(FIXTURES / "run.yaml")
    cohort = load_cohort(FIXTURES / "cohort.yaml")
    cfg = cfg.model_copy(update={"products": ["p1"]})
    violations = cross_check_products(cfg, cohort)
    assert len(violations) == 1
    assert "S11" in violations[0] and "p2" in violations[0]


# ---------------------------------------------------------------------------
# Негативные: RunConfig
# ---------------------------------------------------------------------------


def test_run_config_anchor_not_in_products_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "run.yaml")
    data["products"] = ["p1", "p2"]
    data["anchor"] = "p3"
    path = _write(tmp_path, "run.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="anchor"):
        load_run_config(path)


def test_run_config_duplicate_products_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "run.yaml")
    data["products"] = ["p1", "p1"]
    path = _write(tmp_path, "run.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="дубликат"):
        load_run_config(path)


def test_run_config_preliminary_capability_ranked_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "run.yaml")
    data["mode"] = "preliminary"
    data["ranking_policy"] = "capability_ranked"
    path = _write(tmp_path, "run.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="I13"):
        load_run_config(path)


# ---------------------------------------------------------------------------
# Негативные: PackageManifest
# ---------------------------------------------------------------------------


def test_package_weights_not_100_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "package.yaml")
    data["modules"][1]["weight"] = 30  # 60 + 30 = 90
    path = _write(tmp_path, "package.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="100"):
        load_package_manifest(path)


def test_package_duplicate_module_id_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "package.yaml")
    data["modules"][1]["module_id"] = "mod_a"
    path = _write(tmp_path, "package.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="module_id"):
        load_package_manifest(path)


def test_package_archetype_c_with_baseline_participant_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "package.yaml")
    data["archetype"] = "C"
    data["baseline_participant"] = "baseline_a"
    path = _write(tmp_path, "package.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="архетип C"):
        load_package_manifest(path)


# ---------------------------------------------------------------------------
# Негативные: Cohort
# ---------------------------------------------------------------------------


def test_cohort_zero_anchors_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "cohort.yaml")
    for entry in data["entries"]:
        entry["anchor_relation"] = "analog"
    path = _write(tmp_path, "cohort.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="якор"):
        load_cohort(path)


def test_cohort_two_anchors_rejected(tmp_path: Path) -> None:
    data = _load_yaml(FIXTURES / "cohort.yaml")
    data["entries"][1]["anchor_relation"] = "anchor"
    path = _write(tmp_path, "cohort.yaml", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValidationError, match="якор"):
        load_cohort(path)


# ---------------------------------------------------------------------------
# Негативные: CatalogRow
# ---------------------------------------------------------------------------


def test_catalog_showcase_without_metric_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "catalog.csv",
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        "param_z,Параметр Z,mod_a,grp,Баз,showcase,описание без метрики,explicit,role_a,,ru\n",
    )
    with pytest.raises(ValueError, match="showcase"):
        load_catalog(path)


def test_catalog_uppercase_slug_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "catalog.csv",
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        "Param_Z,Параметр Z,mod_a,grp,Баз,fact,граница,explicit,role_a,,ru\n",
    )
    with pytest.raises(ValueError, match="slug"):
        load_catalog(path)


def test_catalog_missing_column_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "catalog.csv",
        "param_id,title,module,group,importance,param_type,rule\n"
        "param_z,Параметр Z,mod_a,grp,Баз,fact,граница\n",
    )
    with pytest.raises(ValueError, match="колонк"):
        load_catalog(path)


# ---------------------------------------------------------------------------
# Негативные: domain_profiles / отсутствующий файл
# ---------------------------------------------------------------------------


def test_domain_profiles_non_mapping_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "domain_profiles.yaml", "- a\n- b\n")
    with pytest.raises(ValueError, match="словарь"):
        load_domain_profiles(path)


def test_domain_profiles_entry_non_mapping_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "domain_profiles.yaml", "case_x: not_a_mapping\n")
    with pytest.raises(ValueError, match="словарь"):
        load_domain_profiles(path)


def test_missing_file_raises_valueerror(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="прочитать"):
        load_run_config(tmp_path / "does_not_exist.yaml")
