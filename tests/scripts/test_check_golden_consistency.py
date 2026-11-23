"""Гейт ``scripts/check_golden_consistency.py``: эталон согласован с объявленным каталогом.

Дефект, который эти тесты держат закрытым: ``golden.csv`` — производный артефакт (собран из
снимка сравнительной таблицы), и его ``expected_evidence_kind`` был одинаков
(``documentation``) у всех 905 строк, пока каталог был целиком ``explicit``. После
пере-квалификации 18.11.2026 четыре параметра объявлены ``registry_absence``, и эталонный
``❌`` по ним доказывается **реестром** — строка эталона обязана это называть, иначе
производный артефакт описывает правило, которого больше нет.
"""

from __future__ import annotations

from pathlib import Path

from scripts.check_golden_consistency import check, main, write

CATALOG_HEADER = "param_id,title,absence_rule\n"
GOLDEN_HEADER = (
    "product_id,param_id,expected_status,expected_evidence_kind,case_class,tier,note\n"
)


def _package(
    tmp_path: Path,
    *,
    catalog: str,
    golden: str,
    cells: str | None = None,
) -> Path:
    package = tmp_path / "pkg"
    package.mkdir(parents=True)
    (package / "catalog.csv").write_text(CATALOG_HEADER + catalog, encoding="utf-8")
    (package / "golden.csv").write_text(GOLDEN_HEADER + golden, encoding="utf-8")
    if cells is not None:
        (package / "sources").mkdir()
        (package / "sources" / "golden_00_cells.csv").write_text(
            "product_id,param_id,normalized_status\n" + cells, encoding="utf-8"
        )
    return package


def _kinds(report: object) -> dict[str, int]:
    return report.by_kind()  # type: ignore[attr-defined]


def test_consistent_package_has_no_findings(tmp_path: Path) -> None:
    package = _package(
        tmp_path,
        catalog="p.a,Альфа,distribution_absence\n",
        golden="p1,p.a,❌,documentation,class_absent,full,\n",
    )

    report = check(package)

    assert report.findings == []
    assert report.rows == 1
    assert report.absent_rows == 1


def test_registry_absence_requires_registry_evidence_kind(tmp_path: Path) -> None:
    """Объявленный реестр — не исчерпание документации: эталон обязан это назвать."""
    package = _package(
        tmp_path,
        catalog="p.a,Сертификат,registry_absence\n",
        golden="p1,p.a,❌,documentation,class_absent,full,\n",
    )

    report = check(package)

    assert _kinds(report) == {"kind_vs_rule": 1}
    assert report.findings[0].expected == "registry_absence → registry"
    assert report.fixable == 1


def test_write_fixes_only_rule_derived_field_and_is_idempotent(tmp_path: Path) -> None:
    package = _package(
        tmp_path,
        catalog="p.a,Сертификат,registry_absence\n",
        golden="p1,p.a,❌,documentation,class_absent,full,note-1\n",
    )

    assert write(package) == 1
    text = (package / "golden.csv").read_text(encoding="utf-8")
    assert "❌,registry|documentation,class_absent,full,note-1" in text, (
        "статус, класс и примечание не трогаются — правится только вид свидетельства"
    )
    assert write(package) == 0, "второй проход ничего не меняет"
    assert check(package).findings == []


def test_unknown_param_is_reported(tmp_path: Path) -> None:
    package = _package(
        tmp_path,
        catalog="p.a,Альфа,distribution_absence\n",
        golden="p1,p.gone,✅,documentation,class_ok,full,\n",
    )

    report = check(package)

    assert _kinds(report) == {"unknown_param": 1}


def test_foreign_case_class_is_not_a_finding(tmp_path: Path) -> None:
    """``case_class`` — свободная строка: чужой словарь пакета не объявлен этим гейтом."""
    package = _package(
        tmp_path,
        catalog="p.a,Гамма,registry_absence\n",
        golden="p1,p.a,❌,registry,absence_registry,full,\n",
    )

    assert check(package).findings == []


def test_status_vs_class_for_known_class(tmp_path: Path) -> None:
    package = _package(
        tmp_path,
        catalog="p.a,Альфа,distribution_absence\n",
        golden="p1,p.a,✅,documentation,class_absent,full,\n",
    )

    report = check(package)

    assert _kinds(report) == {"status_vs_class": 1}
    assert report.findings[0].expected == "class_absent → ❌"


def test_duplicate_pair_is_reported(tmp_path: Path) -> None:
    package = _package(
        tmp_path,
        catalog="p.a,Альфа,distribution_absence\n",
        golden=(
            "p1,p.a,❌,documentation,class_absent,full,\n"
            "p1,p.a,❌,documentation,class_absent,full,\n"
        ),
    )

    assert _kinds(check(package)) == {"duplicate": 1}


def test_snapshot_divergence_is_visible(tmp_path: Path) -> None:
    """Эталон правили руками — снимок перестал быть источником, и это видно."""
    package = _package(
        tmp_path,
        catalog="p.a,Альфа,distribution_absence\n",
        golden="p1,p.a,✅,documentation,class_ok,full,\n",
        cells="p1,p.a,❌\n",
    )

    report = check(package)

    assert _kinds(report) == {"raw_vs_status": 1}
    assert report.findings[0].expected == "❌"
    assert report.findings[0].actual == "✅"


def test_main_exit_code_follows_findings(tmp_path: Path, capsys: object) -> None:
    clean = _package(
        tmp_path,
        catalog="p.a,Альфа,distribution_absence\n",
        golden="p1,p.a,❌,documentation,class_absent,full,\n",
    )
    dirty = _package(
        tmp_path / "second",
        catalog="p.a,Сертификат,registry_absence\n",
        golden="p1,p.a,❌,documentation,class_absent,full,\n",
    )

    assert main(["--package", str(clean)]) == 0
    assert main(["--package", str(dirty)]) == 1
