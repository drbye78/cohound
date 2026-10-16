"""Тесты нейтральных шаблонов-скелетов данных (T6.5).

Проверяют, что пакет ``packages/_template``, паспорт ``passports/_template`` и
реестровый шаблон ``docs/templates/registries/example_template.yaml`` проходят тот же контур
валидации, что и реальные данные: ``load_inputs`` → ``lint_run_inputs`` →
``cross_check``, ``load_passport``, ``load_registry_config``.

Шаблон обязан быть **чистым**: без блокирующих И без предупреждений (concept_terms
заполнены, оба тира golden присутствуют). Негативные тесты доказывают, что правила
линтера действительно срабатывают (warning ``empty-concept-terms``; блок golden-full
без страты ``❌``).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from core.adapters.registry_generic import RegistryConfig, load_registry_config
from core.config.passports import load_passport
from core.domain.models import Passport
from core.pipeline.lint import RULES, lint_run_inputs
from core.pipeline.normalize import cross_check, load_inputs

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PACKAGE = ROOT / "packages" / "_template"
TEMPLATE_PASSPORTS = ROOT / "passports" / "_template"
TEMPLATE_REGISTRY = ROOT / "docs" / "templates" / "registries" / "example_template.yaml"

#: Обязательные артефакты раскладки пакета (``03`` §A.1, ``normalize.load_inputs``).
MANDATORY_PACKAGE_FILES = (
    "package.yaml",
    "catalog.csv",
    "baselines.csv",
    "cohort.yaml",
    "golden.csv",
)

_CONCEPT_TERMS_RULE = next(rule for rule in RULES if rule.code == "empty-concept-terms")


def _copy_template_package(dest: Path) -> Path:
    """Копирует шаблонный пакет в ``dest`` и возвращает каталог копии."""
    shutil.copytree(TEMPLATE_PACKAGE, dest)
    return dest


# ---------------------------------------------------------------------------
# Happy-path: шаблоны валидны
# ---------------------------------------------------------------------------


def test_template_package_has_all_mandatory_files() -> None:
    """В ``packages/_template`` присутствуют все 5 обязательных артефактов."""
    missing = [name for name in MANDATORY_PACKAGE_FILES if not (TEMPLATE_PACKAGE / name).is_file()]
    assert missing == [], f"отсутствуют обязательные файлы пакета: {missing}"


def test_template_package_loads_and_lints_clean() -> None:
    """Шаблонный пакет грузится, линтуется без блоков и без предупреждений."""
    inputs = load_inputs(TEMPLATE_PACKAGE / "run.yaml", TEMPLATE_PACKAGE)
    report = lint_run_inputs(inputs)
    assert report.blocking == [], report.blocking
    assert report.warnings == [], report.warnings
    assert report.ok is True


def test_template_package_cross_check_clean() -> None:
    """S11/якорь: ``products`` = когорта, ``anchor ∈ products`` — нарушений нет."""
    inputs = load_inputs(TEMPLATE_PACKAGE / "run.yaml", TEMPLATE_PACKAGE)
    assert cross_check(inputs) == []


def test_template_package_has_both_golden_tiers() -> None:
    """В golden есть оба тира (``fast`` и ``full``) — иначе был бы warning."""
    inputs = load_inputs(TEMPLATE_PACKAGE / "run.yaml", TEMPLATE_PACKAGE)
    tiers = {row.tier.value for row in inputs.golden}
    assert {"fast", "full"} <= tiers


def test_template_passport_loads_and_validates() -> None:
    """Шаблонный паспорт ``example_product`` грузится и валиден (§4.3)."""
    passport = load_passport("example_product", TEMPLATE_PASSPORTS)
    assert passport is not None
    assert isinstance(passport, Passport)
    assert passport.product_id == "example_product"
    assert passport.upstream is None  # архетип B
    assert passport.doc_domains and all(".invalid" in d for d in passport.doc_domains)
    # Установленный сателлит несёт все обязательные поля + relationship_evidence.
    approved = [s for s in passport.satellites if s.state.value == "approved"]
    assert approved, "в шаблоне обязан быть установленный сателлит"
    sat = approved[0]
    assert sat.component and sat.origin and sat.relationship and sat.evidence_state
    assert sat.relationship_evidence is not None


def test_template_registry_config_loads() -> None:
    """Реестровый шаблон грузится и соответствует схеме ``RegistryConfig``."""
    cfg = load_registry_config(TEMPLATE_REGISTRY)
    assert isinstance(cfg, RegistryConfig)
    assert cfg.registry_id == "example_template"
    assert cfg.format == "json"
    assert cfg.mapping  # непустой маппинг


# ---------------------------------------------------------------------------
# Негативный: пустые concept_terms → warning (не блок)
# ---------------------------------------------------------------------------


def test_catalog_without_concept_terms_warns_not_blocks(tmp_path: Path) -> None:
    """Каталог без ``concept_terms`` даёт warning ``empty-concept-terms``, не блок."""
    package = _copy_template_package(tmp_path / "package")
    (package / "catalog.csv").write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        "param.alpha_capability,T,module_core,g,Баз,fact,rule,explicit,role_primary,,ru\n",
        encoding="utf-8",
    )
    inputs = load_inputs(package / "run.yaml", package)
    report = lint_run_inputs(inputs)

    # Правило действительно срабатывает и имеет уровень warning, а не blocking.
    assert _CONCEPT_TERMS_RULE.severity == "warning"
    produced = _CONCEPT_TERMS_RULE.check(inputs)
    assert produced, "правило empty-concept-terms обязано что-то вернуть"
    assert report.ok is True, report.blocking
    assert any("concept_terms" in warning for warning in report.warnings), report.warnings


# ---------------------------------------------------------------------------
# Негативный: golden-full без страты ❌ → блок
# ---------------------------------------------------------------------------


def test_golden_full_without_absent_stratum_blocks(tmp_path: Path) -> None:
    """golden-full без строки ``❌`` обязан блокироваться линтером (§4.2/§11.6)."""
    package = _copy_template_package(tmp_path / "package")
    (package / "golden.csv").write_text(
        "product_id,param_id,expected_status,expected_evidence_kind,case_class,tier,note\n"
        "product_anchor,param.alpha_capability,✅,documentation,class_ok,fast,\n"
        "product_anchor,param.beta_capability,✅,documentation,class_ok,full,\n",
        encoding="utf-8",
    )
    inputs = load_inputs(package / "run.yaml", package)
    report = lint_run_inputs(inputs)

    assert report.ok is False
    assert any("golden-full" in message or "❌" in message for message in report.blocking), (
        report.blocking
    )


if __name__ == "__main__":  # pragma: no cover - локальный запуск
    raise SystemExit(pytest.main([__file__, "-q"]))
