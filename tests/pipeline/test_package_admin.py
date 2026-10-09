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
