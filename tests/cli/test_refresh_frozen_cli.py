"""``--refresh frozen`` и ``--frozen-source`` в идентичности и в формах флагов (план 19 §P3).

Проверяется то, чего нельзя доказать единицей модуля:

* ``plan`` печатает **другой** ``run_id`` для ``frozen``, чем для ``auto`` и ``offline``
  (``refresh_policy`` — слагаемое ``run_id``);
* ``frozen_source=bytes`` при ``auto`` **не** меняет ``run_id`` (канонизация §6.3), а при
  ``frozen`` — меняет (ось действует);
* флаг ``--frozen-source`` объявлен одной и той же формой у ``plan``/``run``/``crawl
  run``: разъехавшиеся формы дали бы разные идентичности при одном объявленном входе.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_OK, main
from core.cli.parser import build_parser
from tests.helpers.packages import valid_package


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD: деплой-конфиг — дефолтный ``local``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.chdir(tmp_path)
    return root


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def _common(package: Path) -> list[str]:
    return ["--run-yaml", str(package / "run.yaml"), "--package", str(package), "--json"]


def _plan_run_id(
    package: Path, capsys: pytest.CaptureFixture[str], *flags: str
) -> str:
    assert main(["plan", *_common(package), *flags]) == EXIT_OK
    return _payload(capsys)["run_id"]


def test_plan_frozen_run_id_differs_from_auto_and_offline(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``frozen`` — своя идентичность: иная, чем у сетевого ``auto`` и у ``offline``."""
    package = valid_package(tmp_path)
    auto = _plan_run_id(package, capsys)
    offline = _plan_run_id(package, capsys, "--refresh", "offline")
    frozen = _plan_run_id(package, capsys, "--refresh", "frozen")

    assert len({auto, offline, frozen}) == 3


def test_frozen_source_is_neutral_outside_frozen_and_effective_inside(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """§6.3: ось канонизируется в ``n/a`` вне ``frozen`` и действует внутри неё."""
    package = valid_package(tmp_path)
    auto = _plan_run_id(package, capsys)
    auto_with_chunks = _plan_run_id(
        package, capsys, "--frozen-source", "chunks"
    )
    assert auto_with_chunks == auto, "недействующая ось не меняет run_id"

    frozen_default = _plan_run_id(package, capsys, "--refresh", "frozen")
    frozen_bytes = _plan_run_id(
        package, capsys, "--refresh", "frozen", "--frozen-source", "bytes"
    )
    frozen_chunks = _plan_run_id(
        package, capsys, "--refresh", "frozen", "--frozen-source", "chunks"
    )
    assert frozen_bytes == frozen_default, "дефолт оси — объявленный bytes"
    assert frozen_chunks != frozen_default


def _nested(sub: Any, *names: str) -> Any:
    parser = sub
    for name in names:
        action = next(
            action
            for action in parser._subparsers._group_actions  # type: ignore[union-attr]
            if hasattr(action, "choices") and action.choices
        )
        parser = action.choices[name]
    return parser


def test_frozen_source_flag_has_one_form_across_plan_run_and_crawl() -> None:
    """Один регистратор → одна форма на ``plan``/``run``/``crawl run`` (паритет)."""
    parser = build_parser()
    plan = _nested(parser, "plan")
    run = _nested(parser, "run")
    crawl_run = _nested(parser, "crawl", "run")

    def action(where: Any) -> Any:
        return {a.dest: a for a in where._actions}["frozen_source"]

    reference = action(plan)
    for where in (run, crawl_run):
        other = action(where)
        assert other.option_strings == reference.option_strings == ["--frozen-source"]
        assert other.default == reference.default is None
        assert other.choices == reference.choices == ["bytes", "chunks"]
        assert other.help == reference.help
