"""Тесты CRUD-операций над пакетом (итерация I1, ``package_admin``).

Ключевое: каждая мутация проходит проспективный линт и **отказывается писать** при
появлении новых блокирующих нарушений; Σ весов модулей держится на 100; §13-поля
``deprecated``/``replaced_by`` и append-only журнал мутаций работают.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.domain.models import BaselineRow
from core.domain.enums import InheritanceMode
from core.pipeline import package_admin as pa
from core.pipeline.package_io import load_artifacts, save_artifact


def _make(tmp_path: Path, case: str = "case_x", archetype: str = "B") -> Path:
    pa.create_package(case, archetype, dir=tmp_path)
    return tmp_path / case


def _weights(package: Path) -> list[float]:
    artifacts = load_artifacts(package)
    assert artifacts.manifest is not None
    return [module.weight for module in artifacts.manifest.modules]


# ---------------------------------------------------------------------------
# Создание / чтение / валидация
# ---------------------------------------------------------------------------


def test_create_from_template_is_lint_clean(tmp_path: Path) -> None:
    result = pa.create_package("case_x", "B", dir=tmp_path)
    package = tmp_path / "case_x"
    for name in ("package.yaml", "catalog.csv", "baselines.csv", "cohort.yaml", "golden.csv", "run.yaml"):
        assert (package / name).is_file()
    assert result.lint["blocking"] == []
    assert pa.validate_package(package).lint["blocking"] == []
    assert pa.show_package(package)["ok"] is True
    assert (package / "package.yaml").read_text(encoding="utf-8").count("case_id: case_x") == 1


def test_create_existing_refuses(tmp_path: Path) -> None:
    _make(tmp_path)
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.create_package("case_x", "B", dir=tmp_path)
    assert exc.value.kind == "usage"


def test_create_dry_run_writes_nothing(tmp_path: Path) -> None:
    result = pa.create_package("case_y", "C", dir=tmp_path, dry_run=True)
    assert result.dry_run is True
    assert result.written == []
    assert not (tmp_path / "case_y").exists()


def test_create_minimal_without_template(tmp_path: Path) -> None:
    result = pa.create_package("case_z", "C", dir=tmp_path, from_template=False)
    assert result.lint["blocking"] == []
    assert pa.show_package(tmp_path / "case_z")["catalog"] == 0


def test_create_bad_case_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        pa.create_package("Bad-Id", "B", dir=tmp_path)


def test_create_bad_archetype(tmp_path: Path) -> None:
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.create_package("case_a", "Z", dir=tmp_path)
    assert exc.value.kind == "usage"


def test_show_reports_deprecated_and_counts(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_param(package, "T", "rule", module="module_core", group="g", importance="Баз", param_id="param.t")
    pa.remove_param(package, "param.t")
    summary = pa.show_package(package)
    assert summary["deprecated_params"] == ["param.t"]
    assert summary["catalog"] == 6


def test_validate_reports_parse_errors(tmp_path: Path) -> None:
    package = _make(tmp_path)
    (package / "catalog.csv").write_text("bad,header\n1,2\n", encoding="utf-8")
    result = pa.validate_package(package)
    assert result.lint["blocking"]


# ---------------------------------------------------------------------------
# Архетип / версия
# ---------------------------------------------------------------------------


def test_set_archetype_ok(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.set_archetype(package, "A")
    assert pa.show_package(package)["archetype"] == "A"


def test_set_archetype_c_blocked_with_baselines(tmp_path: Path) -> None:
    package = _make(tmp_path)
    save_artifact(
        package,
        "baselines",
        [BaselineRow(baseline_id="up", param_id="param.alpha_capability", inheritance_mode=InheritanceMode.DIRECT)],
    )
    before = (package / "package.yaml").read_bytes()
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.set_archetype(package, "C")
    assert exc.value.kind == "blocked"
    assert exc.value.code == "archetype-c-baselines"
    assert (package / "package.yaml").read_bytes() == before


def test_bump_version_levels(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.bump_version(package, level="major")
    assert pa.show_package(package)["package_version"] == "1.0.0"
    result = pa.bump_version(package, level="major")
    assert any("§13" in warning for warning in result.warnings)
    pa.bump_version(package, level="minor")
    pa.bump_version(package, level="patch")
    assert pa.show_package(package)["package_version"] == "2.1.1"


def test_bump_version_bad_level(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError):
        pa.bump_version(package, level="huge")


# ---------------------------------------------------------------------------
# Модули
# ---------------------------------------------------------------------------


def test_add_module_renormalizes_to_100(tmp_path: Path) -> None:
    package = _make(tmp_path)
    result = pa.add_module(package, "module_extra", 10)
    assert result.lint["blocking"] == []
    assert abs(sum(_weights(package)) - 100.0) < 1e-6
    assert "module_extra" in {m.module_id for m in load_artifacts(package).manifest.modules}


def test_add_duplicate_module_refuses(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.add_module(package, "module_core", 10)
    assert exc.value.kind == "usage"


def test_update_module_keeps_sum(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.update_module(package, "module_core", 90)
    assert abs(sum(_weights(package)) - 100.0) < 1e-6


def test_remove_module_in_use_blocked_then_reassign(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.remove_module(package, "module_core")
    assert exc.value.kind == "blocked"
    result = pa.remove_module(package, "module_core", reassign_to="module_ecosystem")
    assert "catalog" in result.changed
    ids = {m.module_id for m in load_artifacts(package).manifest.modules}
    assert "module_core" not in ids
    assert abs(sum(_weights(package)) - 100.0) < 1e-6


def test_remove_last_module_refuses(tmp_path: Path) -> None:
    pa.create_package("case_min", "C", dir=tmp_path, from_template=False)
    package = tmp_path / "case_min"
    with pytest.raises(pa.PackageAdminError):
        pa.remove_module(package, "core")


def test_renormalize_modules(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.renormalize_modules(package)
    assert abs(sum(_weights(package)) - 100.0) < 1e-6


# ---------------------------------------------------------------------------
# Роли
# ---------------------------------------------------------------------------


def test_role_crud(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_role(package, "role_extra", "доп. роль", markers=["m"], actor="a")
    assert "role_extra" in {r.role_id for r in pa.list_roles(package)}
    pa.update_role(package, "role_extra", definition="обновлено")
    assert next(r for r in pa.list_roles(package) if r.role_id == "role_extra").definition == "обновлено"
    pa.remove_role(package, "role_extra")
    assert "role_extra" not in {r.role_id for r in pa.list_roles(package)}


def test_add_duplicate_role_refuses(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError):
        pa.add_role(package, "role_primary", "x")


def test_remove_role_in_use_blocked_then_clear(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.remove_role(package, "role_primary")
    assert exc.value.kind == "blocked"
    # Пустой reassign очищает role_binding у зависимых параметров.
    result = pa.remove_role(package, "role_primary", reassign_to="")
    assert "catalog" in result.changed
    assert all(row.role_binding != "role_primary" for row in pa.list_params(package))
    assert "role_primary" not in {r.role_id for r in pa.list_roles(package)}


# ---------------------------------------------------------------------------
# Каталог
# ---------------------------------------------------------------------------


def test_param_crud_and_deprecated(tmp_path: Path) -> None:
    package = _make(tmp_path)
    result = pa.add_param(
        package,
        "Функция X",
        "точная граница",
        module="module_core",
        group="g",
        importance="Баз",
        role_binding="role_primary",
        concept_terms={"ru": ["x"]},
        search_langs=["ru"],
        param_id="param.x",
    )
    assert result.created_id == "param.x"
    assert pa.get_param(package, "param.x") is not None
    pa.update_param(package, "param.x", title="Обновлено")
    assert pa.get_param(package, "param.x").title == "Обновлено"
    pa.remove_param(package, "param.x", replaced_by="param.y")
    removed = pa.get_param(package, "param.x")
    assert removed.deprecated is True and removed.replaced_by == "param.y"
    assert all(row.param_id != "param.x" for row in pa.list_params(package, include_deprecated=False))


def test_add_param_duplicate_title_gets_suffix(tmp_path: Path) -> None:
    package = _make(tmp_path)
    first = pa.add_param(package, "Alpha", "rule", module="module_core", group="g", importance="Баз")
    second = pa.add_param(package, "Alpha", "rule", module="module_core", group="g", importance="Баз")
    assert first.created_id == "alpha"
    assert second.created_id == "alpha_2"


def test_add_param_unknown_module_and_role(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.add_param(package, "T", "r", module="nope", group="g", importance="Баз")
    assert exc.value.kind == "usage"
    with pytest.raises(pa.PackageAdminError):
        pa.add_param(
            package, "T", "r", module="module_core", group="g", importance="Баз", role_binding="nope"
        )


def test_add_showcase_requires_metric(tmp_path: Path) -> None:
    package = _make(tmp_path)
    with pytest.raises(pa.PackageAdminError):
        pa.add_param(
            package, "T", "нет метрики", module="module_core", group="g", importance="Баз", param_type="showcase"
        )


def test_get_missing_param_returns_none(tmp_path: Path) -> None:
    package = _make(tmp_path)
    assert pa.get_param(package, "param.nope") is None


# ---------------------------------------------------------------------------
# Главный инвариант: новые blocking → отказ без записи
# ---------------------------------------------------------------------------


def test_new_blocking_refuses_without_writing(tmp_path: Path) -> None:
    package = _make(tmp_path)
    (package / "scope_attestation.yaml").unlink()
    before = (package / "catalog.csv").read_bytes()
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.update_param(package, "param.alpha_capability", absence_rule="parameter_reference")
    assert exc.value.kind == "blocked"
    assert exc.value.blocking
    assert (package / "catalog.csv").read_bytes() == before


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    package = _make(tmp_path)
    before = (package / "package.yaml").read_bytes()
    result = pa.add_module(package, "module_dry", 5, dry_run=True)
    assert result.dry_run is True
    assert result.written == []
    assert (package / "package.yaml").read_bytes() == before


def test_apply_log_appended(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_module(package, "module_log", 5, actor="tester")
    log = package / ".authoring" / "apply_log.jsonl"
    assert log.is_file()
    assert '"actor": "tester"' in log.read_text(encoding="utf-8")


def test_repeated_save_is_byte_identical(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_module(package, "module_idem", 5)
    first = (package / "package.yaml").read_bytes()
    pa.renormalize_modules(package)
    second = (package / "package.yaml").read_bytes()
    assert first == second


# ---------------------------------------------------------------------------
# Read-only обзор: list_modules / get_cohort / фильтры
# ---------------------------------------------------------------------------


def test_list_modules_reads_manifest(tmp_path: Path) -> None:
    package = _make(tmp_path)
    modules = pa.list_modules(package)
    assert {m.module_id for m in modules} == {"module_core", "module_ecosystem"}
    assert abs(sum(m.weight for m in modules) - 100.0) < 1e-6


def test_list_modules_requires_manifest(tmp_path: Path) -> None:
    with pytest.raises(pa.PackageAdminError) as exc:
        pa.list_modules(tmp_path / "nope")
    assert exc.value.kind == "usage"


def test_get_cohort_present_and_absent(tmp_path: Path) -> None:
    package = _make(tmp_path)
    cohort = pa.get_cohort(package)
    assert cohort is not None
    assert cohort.case_id == "case_x"
    assert len(cohort.entries) == 3
    # Пакет без cohort.yaml — None (не исключение).
    pa.create_package("case_min", "C", dir=tmp_path, from_template=False)
    minimal = tmp_path / "case_min"
    (minimal / "cohort.yaml").unlink()
    assert pa.get_cohort(minimal) is None


def test_list_params_filters(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_param(
        package,
        "Фича",
        "граница",
        module="module_core",
        group="grp_x",
        importance="Кр",
        param_type="fact",
        role_binding="role_primary",
        concept_terms={"ru": ["термин"]},
        param_id="param.feat",
    )
    assert [r.param_id for r in pa.list_params(package, module="module_core") if r.param_id == "param.feat"] == ["param.feat"]
    assert pa.list_params(package, module="nope") == []
    assert [r.param_id for r in pa.list_params(package, group="grp_x")] == ["param.feat"]
    assert "param.feat" in {r.param_id for r in pa.list_params(package, importance="Кр")}
    assert all(r.importance.value == "Кр" for r in pa.list_params(package, importance="Кр"))
    assert "param.feat" in {r.param_id for r in pa.list_params(package, param_type="fact")}
    assert "param.feat" in {r.param_id for r in pa.list_params(package, role_binding="role_primary")}
    assert all(r.role_binding == "role_primary" for r in pa.list_params(package, role_binding="role_primary"))
    assert [r.param_id for r in pa.list_params(package, query="ТЕРМИН")] == ["param.feat"]
    assert [r.param_id for r in pa.list_params(package, query="граница")] == ["param.feat"]
    assert pa.list_params(package, query="нет_такого") == []


def test_list_params_exclude_deprecated_with_query(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_param(package, "T", "r", module="module_core", group="g", importance="Баз", param_id="param.t")
    pa.remove_param(package, "param.t")
    assert any(r.param_id == "param.t" for r in pa.list_params(package, query="param.t"))
    assert pa.list_params(package, query="param.t", include_deprecated=False) == []


def test_list_roles_filter(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_role(package, "role_extra", "функция обзора")
    assert [r.role_id for r in pa.list_roles(package, query="ОБЗОР")] == ["role_extra"]
    assert [r.role_id for r in pa.list_roles(package, query="role_extra")] == ["role_extra"]
    assert pa.list_roles(package, query="нет") == []


def test_list_baselines_filter(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_baseline(package, "up", "param.alpha_capability", note="основной upstream")
    pa.add_baseline(package, "down", "param.alpha_capability", inheritance_mode="conditional")
    assert {r.baseline_id for r in pa.list_baselines(package, baseline_id="up")} == {"up"}
    assert {r.baseline_id for r in pa.list_baselines(package, query="DOWN")} == {"down"}
    assert {r.baseline_id for r in pa.list_baselines(package, query="upstream")} == {"up"}
    assert pa.list_baselines(package, query="нет") == []


# ---------------------------------------------------------------------------
# Read-only get-функции (единичные элементы и наборы)
# ---------------------------------------------------------------------------


def test_get_module_found_and_missing(tmp_path: Path) -> None:
    package = _make(tmp_path)
    module = pa.get_module(package, "module_core")
    assert module is not None and module.module_id == "module_core"
    assert pa.get_module(package, "module_nope") is None
    # Пакет без package.yaml — None (не исключение; значения не выдумываются).
    assert pa.get_module(tmp_path / "nope", "module_core") is None


def test_list_modules_search(tmp_path: Path) -> None:
    package = _make(tmp_path)
    assert {m.module_id for m in pa.list_modules(package, query="CORE")} == {"module_core"}
    assert {m.module_id for m in pa.list_modules(package, query="module")} == {
        "module_core",
        "module_ecosystem",
    }
    assert pa.list_modules(package, query="nomatch") == []


def test_get_role_found_and_missing(tmp_path: Path) -> None:
    package = _make(tmp_path)
    role = pa.get_role(package, "role_primary")
    assert role is not None and role.role_id == "role_primary"
    assert pa.get_role(package, "role_nope") is None
    assert pa.get_role(tmp_path / "nope", "role_primary") is None


def test_get_baseline_exact_key(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_baseline(package, "up", "param.alpha_capability", note="x")
    row = pa.get_baseline(package, baseline_id="up", param_id="param.alpha_capability")
    assert row is not None and row.baseline_id == "up"
    assert pa.get_baseline(package, baseline_id="up", param_id="param.nope") is None


def test_get_baseline_single_key_ambiguity(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_baseline(package, "up", "param.alpha_capability")
    # Один ключ, одно совпадение → найдено.
    assert pa.get_baseline(package, baseline_id="up") is not None
    # Один ключ, несколько совпадений → None (неоднозначность разрешает CLI).
    pa.add_baseline(package, "up", "param.beta_capability")
    assert pa.get_baseline(package, baseline_id="up") is None
    assert len(pa.find_baselines(package, baseline_id="up")) == 2


def test_find_baselines_by_param_and_query(tmp_path: Path) -> None:
    package = _make(tmp_path)
    pa.add_baseline(package, "up", "param.alpha_capability", note="основной")
    pa.add_baseline(package, "down", "param.beta_capability", note="запасной")
    assert {r.baseline_id for r in pa.find_baselines(package, param_id="param.alpha_capability")} == {"up"}
    assert {r.baseline_id for r in pa.find_baselines(package, query="запасной")} == {"down"}
    assert pa.find_baselines(package, query="нет") == []


def test_get_scope_attestation_present_and_absent(tmp_path: Path) -> None:
    package = _make(tmp_path)
    procedure = pa.get_scope_attestation(package)
    assert isinstance(procedure, str) and procedure
    (package / "scope_attestation.yaml").unlink()
    assert pa.get_scope_attestation(package) is None


def test_get_run_config_present_and_absent(tmp_path: Path) -> None:
    package = _make(tmp_path)
    run = pa.get_run_config(package)
    assert run is not None and run.case_id == "case_x"
    (package / "run.yaml").unlink()
    assert pa.get_run_config(package) is None


def test_list_golden_filters(tmp_path: Path) -> None:
    package = _make(tmp_path)
    rows = pa.list_golden(package)
    assert rows
    # Точный фильтр по product_id/param_id.
    golden = rows[0]
    assert all(r.product_id == golden.product_id for r in pa.list_golden(package, product_id=golden.product_id))
    assert [r.param_id for r in pa.list_golden(package, param_id=golden.param_id)] == [golden.param_id]
    # Поиск по expected_status (✅/❌) и case_class.
    assert pa.list_golden(package, query="class_ok")
    assert pa.list_golden(package, query="nomatch") == []
