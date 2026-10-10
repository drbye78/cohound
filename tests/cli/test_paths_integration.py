"""Интеграция раскладки операционных каталогов в composition root (03 §A.1, ЭТАП 2).

Проверяют, что:

* без env/файла/флага поведение прежнее — относительные дефолты от CWD (обратная
  совместимость);
* ``COHOUND_RUNS_DIR`` меняет эффективный каталог прогонов end-to-end;
* ``COHOUND_HOME`` сдвигает все относительные пути;
* CLI-флаг (``--runs-dir``) побеждает env;
* ``COHOUND_PATHS_FILE`` (``paths.yaml``) питает раскладку;
* ``hound doctor`` показывает resolved-раскладку и источник;
* ``wiring`` получает ``index_root`` из resolved-путей.

Данные категорийно-нейтральны (§1.3); сеть не используется.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from core.cli.main import main
from core.cli.wiring import build_adapters
from core.config.paths import PathsConfig
from tests.helpers.packages import capturing_factory, run_argv, valid_package


# ---------------------------------------------------------------------------
# Обратная совместимость (без env/файла)
# ---------------------------------------------------------------------------


def test_default_runs_dir_is_relative_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без env/файла эффективный ``out_dir`` — прежний относительный ``runs``."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    captured, factory = capturing_factory()
    code = main(run_argv(package, run_id="bt-1", until="plan"), phases_factory=factory)
    assert code == 0
    assert captured["out_dir"] == "runs"
    assert captured["raw_root"] == "corpus_raw"


def test_default_run_writes_db_under_relative_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Прогон без флагов пишет БД в ``./runs/{run_id}/cohound.db`` (как раньше)."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    assert main(run_argv(package, run_id="bt-2", until="plan")) == 0
    assert (tmp_path / "runs" / "bt-2" / "cohound.db").is_file()


# ---------------------------------------------------------------------------
# COHOUND_RUNS_DIR
# ---------------------------------------------------------------------------


def test_env_runs_dir_changes_effective_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COHOUND_RUNS_DIR", str(tmp_path / "envruns"))
    package = valid_package(tmp_path)
    assert main(run_argv(package, run_id="env-1", until="plan")) == 0
    assert (tmp_path / "envruns" / "env-1" / "cohound.db").is_file()
    assert not (tmp_path / "runs").exists()


def test_cli_flag_wins_over_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COHOUND_RUNS_DIR", str(tmp_path / "envruns"))
    package = valid_package(tmp_path)
    argv = [*run_argv(package, run_id="flag-1", until="plan"), "--runs-dir", str(tmp_path / "flagruns")]
    assert main(argv) == 0
    assert (tmp_path / "flagruns" / "flag-1" / "cohound.db").is_file()
    assert not (tmp_path / "envruns").exists()


# ---------------------------------------------------------------------------
# COHOUND_HOME
# ---------------------------------------------------------------------------


def test_home_shifts_relative_dirs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COHOUND_HOME", str(tmp_path / "home"))
    package = valid_package(tmp_path)
    assert main(run_argv(package, run_id="home-1", until="plan")) == 0
    assert (tmp_path / "home" / "runs" / "home-1" / "cohound.db").is_file()


def test_home_shifts_raw_root_and_out_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COHOUND_HOME", str(tmp_path / "home"))
    package = valid_package(tmp_path)
    captured, factory = capturing_factory()
    assert main(run_argv(package, run_id="home-2", until="plan"), phases_factory=factory) == 0
    assert captured["out_dir"] == str(tmp_path / "home" / "runs")
    assert captured["raw_root"] == str(tmp_path / "home" / "corpus_raw")


# ---------------------------------------------------------------------------
# COHOUND_PATHS_FILE (paths.yaml)
# ---------------------------------------------------------------------------


def test_paths_file_feeds_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    paths_file = tmp_path / "paths.yaml"
    paths_file.write_text(
        yaml.safe_dump({"runs": "customruns", "corpus_raw": "customraw"}),
        encoding="utf-8",
    )
    monkeypatch.setenv("COHOUND_PATHS_FILE", str(paths_file))
    package = valid_package(tmp_path)
    assert main(run_argv(package, run_id="file-1", until="plan")) == 0
    assert (tmp_path / "customruns" / "file-1" / "cohound.db").is_file()


# ---------------------------------------------------------------------------
# doctor: resolved-раскладка
# ---------------------------------------------------------------------------


def _layout_detail(report: Any, name: str) -> str:
    for group in report.groups:
        if group.group == "layout":
            for check in group.checks:
                if check.name == name:
                    return check.detail
    raise AssertionError(f"layout-проверка {name!r} не найдена")


def test_doctor_layout_reports_env_source(monkeypatch: pytest.MonkeyPatch) -> None:
    from core.cli.doctor import run_doctor

    monkeypatch.setenv("COHOUND_RUNS_DIR", "/abs/paths/runs")
    report = run_doctor()
    detail = _layout_detail(report, "layout.runs")
    assert "/abs/paths/runs" in detail
    assert "env" in detail


def test_doctor_layout_reports_home_and_absolute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.cli.doctor import run_doctor

    monkeypatch.delenv("COHOUND_RUNS_DIR", raising=False)
    monkeypatch.setenv("COHOUND_HOME", str(tmp_path / "home"))
    report = run_doctor()
    detail = _layout_detail(report, "layout.runs")
    assert str(tmp_path / "home" / "runs") in detail


def test_doctor_layout_reports_default_source(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.cli.doctor import run_doctor

    for env in (
        "COHOUND_HOME",
        "COHOUND_PATHS_FILE",
        "COHOUND_RUNS_DIR",
    ):
        monkeypatch.delenv(env, raising=False)
    report = run_doctor()
    assert "default" in _layout_detail(report, "layout.runs")


# ---------------------------------------------------------------------------
# wiring: index_root из resolved-путей
# ---------------------------------------------------------------------------


def test_resolve_ports_passes_index_root(tmp_path: Path) -> None:
    """``build_adapters`` получает ``index_root`` из resolved ``paths.index_store``."""
    from core.cli.main import _resolve_ports

    resolved = PathsConfig(index_store=tmp_path / "idx")
    args = SimpleNamespace(
        profile="local",
        ports=None,
        gateway_factory=None,
        deploy_transport=None,
        paths=resolved,
    )
    ports = _resolve_ports(args, deploy_config=None)  # type: ignore[arg-type]
    if ports.index_builder is not None:  # numpy доступен
        assert getattr(ports.index_builder, "index_root") == tmp_path / "idx"


def test_build_adapters_index_root_default_matches_pathsconfig() -> None:
    """Дефолт ``index_root`` в wiring совпадает с дефолтом :class:`PathsConfig`."""
    ports = build_adapters(profile="local")
    assert PathsConfig().index_store == Path("index_store")
    if ports.index_builder is not None:
        assert getattr(ports.index_builder, "index_root") == Path("index_store")
