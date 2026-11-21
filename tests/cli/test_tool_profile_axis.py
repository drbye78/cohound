"""Ось профиля инструментов оценщика: паритет ``plan``/``run``, резолв, отказы.

План ``28`` §9: профиль локальных инструментов — **условие измерения**, поэтому он входит
в ``EXECUTION_CONTRACT_FIELDS`` (``tool_profile``) ⇒ в ``run_id``. Из этого следуют три
требования, и каждое проверяется вызовом, а не чтением кода:

* **один вход.** ``plan`` и ``run`` объявляют флаги одинаково (option strings, ``dest``,
  дефолт, тип, ``metavar``, help). Разъехавшись, они дали бы две идентичности при одном
  объявленном профиле — тот же класс дефекта, что был у ``--retrieval-profile``.
* **один резолв.** ``--tool-profile`` > ``deploy.tools.profile`` > ``none``; бюджет —
  ``--tool-max-calls`` > ``deploy.tools.max_calls``. В контракт уходит **резолвнутое**
  значение: ``null`` при исполненном дефолте недопустим (``run_id`` описывал бы не тот
  профиль).
* **видимые отказы.** Неизвестная метка и несогласованный бюджет (инструменты без
  бюджета, бюджет без инструментов) — ``EXIT_USAGE`` с перечислением допустимого, а не
  молчаливый ``none``: набор доступных вызовов — вход измерения.

Офлайн: ``plan`` и ``run --dry-run`` не запускают сеть и не собирают корпус.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core.cli.execution_axes import (
    TOOL_PROFILE_TOKENS,
    resolve_tool_profile,
)
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.cli.parser import build_parser
from core.config.deploy import DeployConfig, ToolsDeployConfig
from core.domain.tools import NO_TOOL_PROFILE, ToolProfile
from tests.helpers.packages import valid_package

#: Ось целиком: оба флага — одно условие измерения (профиль без бюджета неисполним).
_TOOL_DESTS = ("tool_profile", "tool_max_calls")


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


def _subparser_args(command: str) -> dict[str, Any]:
    """Аргументы подкоманды из **собранного** парсера (по ``dest``)."""
    parser = build_parser()
    sub = next(
        action
        for action in parser._subparsers._group_actions  # type: ignore[union-attr]
        if hasattr(action, "choices") and action.choices
    )
    return {action.dest: action for action in sub.choices[command]._actions}


def _deploy(profile: str = "none", max_calls: int = 0) -> DeployConfig:
    """Деплой-конфиг с объявленной секцией ``tools`` (остальное — дефолты)."""
    return DeployConfig(
        profile="local", tools=ToolsDeployConfig(profile=profile, max_calls=max_calls)
    )


# ---------------------------------------------------------------------------
# 1. Паритет флагов plan/run
# ---------------------------------------------------------------------------


def test_tool_flags_parity_plan_and_run() -> None:
    """``plan`` и ``run`` объявляют ось **одинаково** — иначе напечатанный ``run_id`` врёт.

    Сравнивается форма, а не только наличие ``dest``: флаг с тем же именем, но другим
    ``type`` принимал бы другое значение (``--tool-max-calls -1``), и идентичности
    разъехались бы молча.
    """
    plan_args = _subparser_args("plan")
    run_args = _subparser_args("run")
    for dest in _TOOL_DESTS:
        assert dest in plan_args, f"plan не принимает флаг {dest}"
        assert dest in run_args, f"run не принимает флаг {dest}"
        plan_action = plan_args[dest]
        run_action = run_args[dest]
        assert plan_action.option_strings == run_action.option_strings, dest
        assert plan_action.default is None, f"{dest}: дефолт обязан быть «не задан»"
        assert run_action.default is None, f"{dest}: дефолт обязан быть «не задан»"
        assert plan_action.type is run_action.type, f"{dest}: тип разошёлся"
        assert plan_action.metavar == run_action.metavar, f"{dest}: metavar разошёлся"
        assert plan_action.help == run_action.help, f"{dest}: help разошёлся"


# ---------------------------------------------------------------------------
# 2. Резолв: флаг > deploy > none
# ---------------------------------------------------------------------------


def test_resolve_tool_profile_precedence() -> None:
    """``--tool-profile`` > ``deploy.tools.profile`` > ``none``; бюджет — тем же порядком."""
    # Ни флага, ни секции — объявленный путь без инструментов, а не ``None``.
    assert resolve_tool_profile(None, None, None).canonical() == "-;max_calls=0"
    assert resolve_tool_profile(None, None, DeployConfig(profile="local")).canonical() == (
        NO_TOOL_PROFILE.canonical()
    )
    # Деплой объявляет профиль — он и действует (флаг не задан).
    assert resolve_tool_profile(None, None, _deploy("t1", 2)).canonical() == (
        "expand_to_section;max_calls=2"
    )
    # Флаг сильнее деплоя, бюджет — тоже.
    assert resolve_tool_profile("t4", 5, _deploy("t1", 2)).canonical() == (
        "table_row;max_calls=5"
    )
    # Список меток — множество: порядок не меняет каноническую строку (и ``run_id``).
    left = resolve_tool_profile("t1+t7", 3, None).canonical()
    right = resolve_tool_profile("t7+t1", 3, None).canonical()
    assert left == right == "expand_to_section,subject_mention;max_calls=3"


def test_resolve_tool_profile_refuses_unknown_token_and_inconsistent_budget() -> None:
    """Неизвестная метка и несогласованный бюджет — отказ, а не «пропустим и продолжим»."""
    with pytest.raises(ValueError, match="неизвестные метки"):
        resolve_tool_profile("t9", 2, None)
    with pytest.raises(ValueError, match="неизвестные метки"):
        resolve_tool_profile("t1+t2", 2, None)
    # Инструменты без бюджета — профиль, который ничего не делает.
    with pytest.raises(ValueError, match="бюджет нулевой"):
        resolve_tool_profile("t1", None, _deploy("none", 0))
    # Бюджет без инструментов — число, которое вводило бы в заблуждение.
    with pytest.raises(ValueError, match="бюджет объявлен без инструментов"):
        resolve_tool_profile("none", 3, None)
    # ``none`` нельзя сочетать с метками: это либо путь без инструментов, либо набор.
    with pytest.raises(ValueError, match="нельзя сочетать"):
        resolve_tool_profile("none+t1", 2, None)


def test_tool_profile_tokens_name_declared_tools() -> None:
    """Метки CLI → **имена** объявленных инструментов (план ``28`` §9.2).

    Контракт и исполнение называют инструмент именем (``expand_to_section``), а оператор
    — меткой гипотезы (``t1``): переименование инструмента обязано быть видно в
    ``ToolProfile``, поэтому в профиль едет имя.
    """
    assert TOOL_PROFILE_TOKENS == {
        "t1": "expand_to_section",
        "t4": "table_row",
        "t7": "subject_mention",
    }
    profile = resolve_tool_profile("t1+t4+t7", 4, None)
    assert profile.tools == ("expand_to_section", "table_row", "subject_mention")
    assert profile.max_calls == 4
    assert profile.enabled is True


# ---------------------------------------------------------------------------
# 3. Идентичность: ось меняет run_id, plan и run согласны
# ---------------------------------------------------------------------------


def test_tool_profile_changes_run_id_and_plan_matches_run(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Профиль входит в ``run_id``, и ``plan`` печатает **тот же** id, что ``run``.

    Паритет без изменения идентичности был бы паритетом нулей: если бы обе стороны
    одинаково игнорировали флаг, тест паритета проходил бы, а инструмент врал бы так же
    дружно, как до правки.
    """
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    base_run_id = _payload(capsys)["run_id"]

    flags = ["--tool-profile", "t1+t4", "--tool-max-calls", "2"]
    assert main(["run", *common, "--dry-run", *flags]) == EXIT_OK
    with_tools = _payload(capsys)
    assert with_tools["run_id"] != base_run_id

    assert main(["plan", *common, *flags]) == EXIT_OK
    planned = _payload(capsys)
    assert planned["run_id"] == with_tools["run_id"]
    assert planned["tool_profile"] == "expand_to_section,table_row;max_calls=2"


def test_plan_payload_names_declared_default_tool_profile(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Незаданный флаг печатается **резолвнутым** ``none``, а не ``null``.

    ``null`` в контракте означал бы «профиль не назван», тогда как исполнение шло бы по
    дефолту развёртывания: ``run_id`` описывал бы не то условие измерения (D2 плана 26).
    """
    package = valid_package(tmp_path)
    assert main(["plan", *_common(package)]) == EXIT_OK
    payload = _payload(capsys)
    assert payload["tool_profile"] == "-;max_calls=0"


# ---------------------------------------------------------------------------
# 4. Видимый отказ на неизвестном профиле
# ---------------------------------------------------------------------------


def test_plan_refuses_unknown_tool_profile(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Неизвестная метка — ``EXIT_USAGE`` с перечислением допустимого, а не ``none``."""
    package = valid_package(tmp_path)
    code = main(["plan", *_common(package), "--tool-profile", "t9", "--tool-max-calls", "2"])
    captured = capsys.readouterr()
    message = captured.out + captured.err
    assert code == EXIT_USAGE
    assert "t9" in message
    assert "t1" in message


def test_run_refuses_tool_profile_without_budget(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Инструменты без объявленного бюджета — отказ: «профиль, который ничего не делает».

    Число не подставляется кодом (A3: объявленный дефолт живёт в ``deploy``): оператор
    обязан назвать бюджет, иначе профиль исполнился бы как ``none`` под именем ``t1``.
    """
    package = valid_package(tmp_path)
    code = main(["run", *_common(package), "--dry-run", "--tool-profile", "t1"])
    captured = capsys.readouterr()
    assert code == EXIT_USAGE
    assert "бюджет" in captured.out + captured.err


def test_tool_profile_object_invariants_hold() -> None:
    """Домен отвергает несогласованный профиль сам — CLI лишь переводит это в отказ."""
    with pytest.raises(ValueError):
        ToolProfile(name="t1", tools=("expand_to_section",), max_calls=0)
    with pytest.raises(ValueError):
        ToolProfile(name="none", tools=(), max_calls=2)
