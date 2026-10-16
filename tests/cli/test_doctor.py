"""Тесты ``hound doctor`` — самодиагностики инсталляции (03 §E.1).

Проверяют: структуру отчёта и детерминизм JSON; устойчивость (не падает при исключении
внутри проверки и на пустом корне); отсутствие секретов в выводе; коды выхода (fail →
``EXIT_ERROR``, ``--strict`` → ``EXIT_ERROR`` на warn); env-проверки (scout/ключи);
группы конфигурации/каталогов; запуск ``selfcheck`` через ``--with-selfcheck``.

Read-only, без сети. Данные/подкаталоги — во временных каталогах (``tmp_path``).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import core.cli.doctor as doctor
from core.cli.main import EXIT_ERROR, EXIT_OK, build_parser, main
from core.cli.doctor import (
    DEPENDENCY_GROUPS,
    CheckGroup,
    DoctorReport,
    _dependency_group,
    run_doctor,
)


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _check(report: DoctorReport, name: str) -> doctor.CheckResult:
    """Находит проверку по имени (единственная в отчёте)."""
    matches = [c for c in report.checks() if c.name == name]
    assert matches, f"проверка {name!r} не найдена"
    assert len(matches) == 1, f"проверка {name!r} неоднозначна"
    return matches[0]


def _json_payload(capsys: pytest.CaptureFixture[str]) -> dict:
    """Читает единственный JSON-объект из stdout."""
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# Структура и детерминизм
# ---------------------------------------------------------------------------


def test_report_structure() -> None:
    report = run_doctor()
    payload = report.to_payload()
    assert set(payload) == {"ok", "summary", "groups", "python", "executable"}
    assert isinstance(payload["ok"], bool)
    assert set(payload["summary"]) == {"ok", "warn", "fail"}
    assert sum(payload["summary"].values()) == len(report.checks())
    groups = {g["group"] for g in payload["groups"]}
    # Обязательные группы на месте (deps:* — по extras).
    for required in ("runtime", "env", "config", "dirs", "cli", "foundation"):
        assert required in groups
    for group_name, _required, _mods in DEPENDENCY_GROUPS:
        assert group_name in groups


def test_json_is_deterministic(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(Path(__file__).resolve().parents[2])
    first = main(["doctor", "--json"])
    out1 = capsys.readouterr().out
    second = main(["doctor", "--json"])
    out2 = capsys.readouterr().out
    assert first == EXIT_OK and second == EXIT_OK
    assert out1 == out2
    # sort_keys=True: ключи верхнего уровня отсортированы.
    keys = list(json.loads(out1.splitlines()[-1]).keys())
    assert keys == sorted(keys)


def test_never_crashes_on_internal_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(_root: Path) -> str:
        raise RuntimeError("сломанный парсер")

    monkeypatch.setattr(doctor, "_required_python", _boom)
    report = run_doctor()
    check = _check(report, "python_version")
    assert check.status == doctor.FAIL
    assert "сломанный парсер" in check.detail
    # Отчёт всё равно собран целиком.
    assert report.checks()


def test_never_crashes_on_empty_root(tmp_path: Path) -> None:
    report = run_doctor(root=tmp_path)  # пустой корень: нет hound.py/config/registries
    assert _check(report, "hound_root_script").status == doctor.WARN
    assert _check(report, "deploy_yaml").status == doctor.WARN
    # Проверки не падают; неудачи — только там, где действительно нет доступа.
    assert report.summary()[doctor.FAIL] == 0


# ---------------------------------------------------------------------------
# Секреты не печатаются
# ---------------------------------------------------------------------------


def test_secrets_not_printed(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("COHOUND_LLM_API_KEY", "SECRET-LLM-abc")
    monkeypatch.setenv("YANDEX_SEARCH_API_KEY", "SECRET-YANDEX-xyz")
    monkeypatch.setenv("YANDEX_FOLDER_ID", "SECRET-FOLDER-42")
    assert main(["doctor"]) == EXIT_OK
    human = capsys.readouterr().out
    assert main(["doctor", "--json"]) == EXIT_OK
    machine = capsys.readouterr().out
    for secret in ("SECRET-LLM-abc", "SECRET-YANDEX-xyz", "SECRET-FOLDER-42"):
        assert secret not in human
        assert secret not in machine


# ---------------------------------------------------------------------------
# Env-проверки
# ---------------------------------------------------------------------------


def test_scout_crawler_valid_and_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COHOUND_SCOUT_CRAWLER", "scrapy")
    assert _check(run_doctor(), "scout_crawler").status == doctor.OK

    monkeypatch.setenv("COHOUND_SCOUT_CRAWLER", "selenium")
    check = _check(run_doctor(), "scout_crawler")
    assert check.status == doctor.FAIL
    assert "selenium" in check.detail


def test_scout_crawler_browser_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    """``browser`` — валидное значение: не FAIL; готовность playwright — OK/WARN."""
    monkeypatch.setenv("COHOUND_SCOUT_CRAWLER", "browser")
    check = _check(run_doctor(), "scout_crawler")
    assert check.status in (doctor.OK, doctor.WARN)
    assert check.status != doctor.FAIL
    assert "browser" in check.detail
    if check.status == doctor.WARN:
        assert check.hint is not None and "playwright" in check.hint


def test_scout_crawler_invalid_gives_error_exit(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("COHOUND_SCOUT_CRAWLER", "selenium")
    assert main(["doctor", "--json"]) == EXIT_ERROR
    payload = _json_payload(capsys)
    assert payload["ok"] is False
    assert payload["summary"]["fail"] >= 1


def test_env_missing_is_warn_not_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "COHOUND_LLM_API_KEY",
        "YANDEX_SEARCH_API_KEY",
        "YANDEX_FOLDER_ID",
        "GLM_SEARCH_API_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    report = run_doctor()
    assert _check(report, "llm_api_key").status == doctor.WARN
    assert _check(report, "search_yandex").status == doctor.WARN
    assert _check(report, "search_glm").status == doctor.WARN
    # Отсутствие ключей — не fail (local-профиль работает без сети).
    env_group = next(g for g in report.groups if g.group == "env")
    assert all(c.status != doctor.FAIL for c in env_group.checks)


def test_llm_api_key_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COHOUND_LLM_API_KEY", raising=False)
    check = _check(run_doctor(), "llm_api_key")
    assert check.status == doctor.WARN
    assert "не задан" in check.detail

    monkeypatch.setenv("COHOUND_LLM_API_KEY", "x")
    ok = _check(run_doctor(), "llm_api_key")
    assert ok.status == doctor.OK
    assert "COHOUND_LLM_API_KEY" in ok.detail


# ---------------------------------------------------------------------------
# Зависимости
# ---------------------------------------------------------------------------


def test_optional_dependency_missing_is_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    group = _dependency_group("deps:test", False, ("no_such_module_xyz",))
    assert group.checks[0].status == doctor.WARN


def test_required_dependency_missing_is_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    group = _dependency_group("deps:test", True, ("no_such_module_xyz",))
    assert group.checks[0].status == doctor.FAIL


# ---------------------------------------------------------------------------
# Конфигурация и каталоги
# ---------------------------------------------------------------------------


def test_config_group_reads_deploy_and_packages(tmp_path: Path) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "deploy.yaml").write_text("profile: local\n", encoding="utf-8")
    (tmp_path / "packages" / "a").mkdir(parents=True)
    (tmp_path / "packages" / "a" / "package.yaml").write_text("package_id: a\n", encoding="utf-8")
    (tmp_path / "packages" / "b").mkdir()
    (tmp_path / "packages" / "b" / "package.yaml").write_text("package_id: b\n", encoding="utf-8")
    (tmp_path / ".env.example").write_text("", encoding="utf-8")

    report = run_doctor(root=tmp_path)
    assert _check(report, "deploy_yaml").status == doctor.OK
    assert "profile=local" in _check(report, "deploy_yaml").detail
    # Реестры — package-scoped: без --package это информационная проверка (не WARN).
    assert _check(report, "registries").status == doctor.OK
    assert _check(report, "packages").status == doctor.OK
    assert "a, b" in _check(report, "packages").detail
    assert _check(report, "env_example").status == doctor.OK


def test_registries_check_does_not_warn_on_missing_root(tmp_path: Path) -> None:
    """Отсутствие корневого ``registries/`` больше не WARN (false signal, P3)."""
    report = run_doctor(root=tmp_path)
    assert _check(report, "registries").status == doctor.OK


def test_registries_check_validates_package_scoped_specs(tmp_path: Path) -> None:
    """``--package`` → проверяются package-scoped спецификации пакета."""
    package = tmp_path / "packages" / "case_a"
    (package / "registries").mkdir(parents=True)
    (package / "registries" / "reg_a.yaml").write_text(
        "registry_id: reg_a\njurisdiction: XX\n"
        "endpoint: 'https://example.invalid/api/search/{query}'\n"
        "format: json\nrecords_path: items\nmapping:\n  name: name\n",
        encoding="utf-8",
    )
    report = run_doctor(root=tmp_path, package=package)
    check = _check(report, "registries")
    assert check.status == doctor.OK
    assert "reg_a.yaml" in check.detail


def test_registries_check_fails_on_bad_package_spec(tmp_path: Path) -> None:
    """Некорректная package-scoped спецификация → FAIL (заметно в doctor)."""
    package = tmp_path / "packages" / "case_b"
    (package / "registries").mkdir(parents=True)
    (package / "registries" / "bad.yaml").write_text(
        "registry_id: bad\njurisdiction: XX\nendpoint: 'https://x.invalid/'\n"
        "format: json\nmapping: {}\n",
        encoding="utf-8",
    )
    report = run_doctor(root=tmp_path, package=package)
    assert _check(report, "registries").status == doctor.FAIL


def test_dir_check_fails_on_file_in_place(tmp_path: Path) -> None:
    (tmp_path / "runs").write_text("not a dir", encoding="utf-8")
    report = run_doctor(root=tmp_path)
    check = _check(report, "runs")
    assert check.status == doctor.FAIL
    assert "не каталог" in check.detail


# ---------------------------------------------------------------------------
# CLI-поверхность
# ---------------------------------------------------------------------------


def test_search_adapters_ok() -> None:
    report = run_doctor()
    assert _check(report, "search_adapters").status == doctor.OK


# ---------------------------------------------------------------------------
# --strict и коды выхода
# ---------------------------------------------------------------------------


def test_strict_turns_warn_into_error(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Убеждаемся, что есть хотя бы один warn (не задан LLM base-url и т.п.).
    monkeypatch.delenv("COHOUND_LLM_BASE_URL", raising=False)
    report = run_doctor()
    assert report.summary()[doctor.WARN] >= 1
    assert report.summary()[doctor.FAIL] == 0
    assert main(["doctor", "--json"]) == EXIT_OK
    capsys.readouterr()
    assert main(["doctor", "--json", "--strict"]) == EXIT_ERROR
    capsys.readouterr()


# ---------------------------------------------------------------------------
# --with-selfcheck
# ---------------------------------------------------------------------------


def _write_selfcheck(root: Path, body: str) -> None:
    tests = root / "tests"
    tests.mkdir(parents=True, exist_ok=True)
    (tests / "selfcheck.py").write_text(body, encoding="utf-8")


def test_with_selfcheck_runs_subprocess(tmp_path: Path) -> None:
    _write_selfcheck(tmp_path, "print('LAST-OK-LINE')\n")
    report = run_doctor(root=tmp_path, with_selfcheck=True)
    check = _check(report, "selfcheck_run")
    assert check.status == doctor.OK
    assert "rc=0" in check.detail and "LAST-OK-LINE" in check.detail


def test_with_selfcheck_failure_is_fail(tmp_path: Path) -> None:
    _write_selfcheck(tmp_path, "import sys\nprint('boom')\nsys.exit(3)\n")
    report = run_doctor(root=tmp_path, with_selfcheck=True)
    check = _check(report, "selfcheck_run")
    assert check.status == doctor.FAIL
    assert "rc=3" in check.detail


def test_selfcheck_skipped_by_default(tmp_path: Path) -> None:
    _write_selfcheck(tmp_path, "print('should not run')\n")
    report = run_doctor(root=tmp_path, with_selfcheck=False)
    check = _check(report, "selfcheck_run")
    assert check.status == doctor.OK
    assert "пропущен" in check.detail
