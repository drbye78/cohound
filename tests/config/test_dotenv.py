"""Тесты dotenv-парсера и его подключения к CLI (``03`` §D.6).

Покрывается измеренный дефект: CLI не загружал ``.env``, и потеря
``COHOUND_SCOUT_CRAWLER=scrapy`` молча меняла движок краула. Проверяются:

* разбор: ``export``, комментарии (whole-line/trailing), кавычки/escape'ы, ``\\r\\n``,
  BOM, UTF-8, некавыченные значения и видимые диагностики некорректных строк;
* инъекция без переопределения уже заданных переменных процесса;
* приоритет источника файла: ``--env-file`` > ``COHOUND_ENV_FILE`` > ``./.env``;
* opt-out ``COHOUND_NO_DOTENV=1`` и тишина при отсутствующем дефолтном ``.env``;
* CLI-вход печатает диагностику в stderr и молчит в ``--json``.

Данные категорийно-нейтральны (§1.3): ключи/значения — просто строки.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pytest

from core.cli.errors import EXIT_USAGE
from core.cli.main import _apply_env_file, main
from core.config.dotenv import (
    DEFAULT_DOTENV_FILENAME,
    ENV_DOTENV_FILE,
    ENV_NO_DOTENV,
    apply_dotenv,
    dotenv_disabled,
    parse_dotenv,
    resolve_env_file,
)

_PREFIX = "COHOUND_TEST_DOTENV_"


def _args(**overrides: object) -> argparse.Namespace:
    """Минимальный namespace CLI-входа (доступные поля ``_apply_env_file``)."""
    values: dict[str, object] = {"env_file": None, "json": False}
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Изолирует тесты от окружения хоста (иначе реальный ``.env``/env оператора влияет)."""
    monkeypatch.delenv(ENV_NO_DOTENV, raising=False)
    monkeypatch.delenv(ENV_DOTENV_FILE, raising=False)


# ---------------------------------------------------------------------------
# Парсер
# ---------------------------------------------------------------------------


def test_parse_basic_export_and_blank_lines() -> None:
    result = parse_dotenv(
        "\nA=1\nexport B = two\nC=\n"
    )
    assert result.values == {"A": "1", "B": "two", "C": ""}
    assert result.diagnostics == ()


def test_parse_comments_crlf_and_bom() -> None:
    text = (
        "\ufeff# whole-line comment\r\n"
        "\r\n"
        "A=value # trailing comment\r\n"
        "B=keep#literal\r\n"
        "C='quoted # not a comment'\r\n"
        "# last\r\n"
    )
    result = parse_dotenv(text)
    assert result.values == {
        "A": "value",
        "B": "keep#literal",
        "C": "quoted # not a comment",
    }
    assert result.diagnostics == ()


def test_parse_quoted_values_and_escapes() -> None:
    result = parse_dotenv(
        'A="line1\\nline2\\ttab\\\\slash\\"quote"\n'
        "B='literal \\n stays'\n"
        'C="a # b" # comment after quote\n'
    )
    assert result.values["A"] == 'line1\nline2\ttab\\slash"quote'
    assert result.values["B"] == "literal \\n stays"
    assert result.values["C"] == "a # b"


def test_parse_unquoted_value_trims_trailing_whitespace() -> None:
    result = parse_dotenv("A=  spaced value   \nB=v\t\n")
    assert result.values == {"A": "spaced value", "B": "v"}


def test_parse_duplicate_key_last_wins() -> None:
    result = parse_dotenv("A=first\nA=second\n")
    assert result.values == {"A": "second"}


def test_parse_invalid_lines_are_diagnostics_not_crashes() -> None:
    result = parse_dotenv(
        "GOOD=ok\n"
        "no separator here\n"
        "bad key=value\n"
        'UNTERMINATED="open\n'
        'SINGLE=\'open\n'
        'JUNK="ok"trailing\n'
        "AFTER=also-ok\n"
    )
    assert result.values == {"GOOD": "ok", "AFTER": "also-ok"}
    # Каждая некорректная строка видна, с номером строки.
    assert len(result.diagnostics) == 5
    for lineno in (2, 3, 4, 5, 6):
        assert any(f"строка {lineno}" in message for message in result.diagnostics)


def test_parse_utf8_values() -> None:
    result = parse_dotenv("NAME=значение\n")
    assert result.values == {"NAME": "значение"}


# ---------------------------------------------------------------------------
# Инъекция и приоритеты
# ---------------------------------------------------------------------------


def test_apply_dotenv_does_not_override_existing(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_text("A=from-file\nB=from-file\n", encoding="utf-8")
    environ = {"A": "from-process"}

    result = apply_dotenv(path, environ)

    assert environ == {"A": "from-process", "B": "from-file"}
    assert result.values == {"A": "from-file", "B": "from-file"}
    assert result.applied == ("B",)


def test_apply_dotenv_reads_utf8_bom(tmp_path: Path) -> None:
    path = tmp_path / ".env"
    path.write_bytes("\ufeffA=1\n".encode("utf-8"))
    environ: dict[str, str] = {}

    result = apply_dotenv(path, environ)

    assert environ == {"A": "1"}
    assert result.diagnostics == ()


def test_apply_dotenv_missing_file_is_diagnostic(tmp_path: Path) -> None:
    result = apply_dotenv(tmp_path / "absent.env", {})
    assert result.values == {}
    assert result.diagnostics and "не удалось прочитать" in result.diagnostics[0]


def test_resolve_env_file_precedence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env = {ENV_DOTENV_FILE: str(tmp_path / "from-env.env")}
    assert resolve_env_file(str(tmp_path / "from-flag.env"), env) == (
        tmp_path / "from-flag.env",
        "flag",
    )
    assert resolve_env_file(None, env) == (tmp_path / "from-env.env", "env")
    assert resolve_env_file(None, {}) == (Path(DEFAULT_DOTENV_FILENAME), "default")
    assert resolve_env_file("", env) == (tmp_path / "from-env.env", "env")


@pytest.mark.parametrize("value", ["1", "true", "YES", "on", " On "])
def test_dotenv_disabled_truthy(value: str) -> None:
    assert dotenv_disabled({ENV_NO_DOTENV: value}) is True


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_dotenv_disabled_falsey(value: str) -> None:
    assert dotenv_disabled({ENV_NO_DOTENV: value}) is False


# ---------------------------------------------------------------------------
# CLI-вход: загрузка/тишина/диагностика/opt-out
# ---------------------------------------------------------------------------


def test_cli_loads_env_file_without_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / ".env"
    path.write_text(
        f"{_PREFIX}KEEP=file\n{_PREFIX}NEW=file\n", encoding="utf-8"
    )
    monkeypatch.setenv(f"{_PREFIX}KEEP", "process")
    monkeypatch.delenv(f"{_PREFIX}NEW", raising=False)

    assert _apply_env_file(_args(env_file=str(path))) is None

    assert os.environ[f"{_PREFIX}KEEP"] == "process"
    assert os.environ[f"{_PREFIX}NEW"] == "file"


def test_cli_uses_env_file_variable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "custom.env"
    path.write_text(f"{_PREFIX}FROM_ENV=yes\n", encoding="utf-8")
    monkeypatch.setenv(ENV_DOTENV_FILE, str(path))
    monkeypatch.delenv(f"{_PREFIX}FROM_ENV", raising=False)

    assert _apply_env_file(_args()) is None
    assert os.environ[f"{_PREFIX}FROM_ENV"] == "yes"


def test_cli_missing_default_dotenv_is_silent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)

    assert _apply_env_file(_args()) is None

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cli_missing_explicit_env_file_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _apply_env_file(_args(env_file=str(tmp_path / "absent.env"))) == EXIT_USAGE
    assert "env-file" in capsys.readouterr().err


def test_cli_missing_env_variable_file_is_diagnostic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(ENV_DOTENV_FILE, str(tmp_path / "absent.env"))
    assert _apply_env_file(_args()) is None
    assert ENV_DOTENV_FILE in capsys.readouterr().err


def test_cli_opt_out_skips_loading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / ".env"
    path.write_text(f"{_PREFIX}SKIPPED=yes\n", encoding="utf-8")
    monkeypatch.setenv(ENV_NO_DOTENV, "1")
    monkeypatch.delenv(f"{_PREFIX}SKIPPED", raising=False)

    assert _apply_env_file(_args(env_file=str(path))) is None
    assert f"{_PREFIX}SKIPPED" not in os.environ


def test_cli_prints_invalid_line_diagnostics(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / ".env"
    path.write_text("GOOD=1\nbroken line\n", encoding="utf-8")

    assert _apply_env_file(_args(env_file=str(path))) is None
    err = capsys.readouterr().err
    assert str(path) in err
    assert "строка 2" in err


def test_cli_json_suppresses_diagnostics(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / ".env"
    path.write_text("broken line\n", encoding="utf-8")

    assert _apply_env_file(_args(env_file=str(path), json=True)) is None
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_main_applies_env_file_before_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Реальный ``main()`` загружает ``.env`` до исполнения команды (не только хелпер)."""
    env_file = tmp_path / "cli.env"
    env_file.write_text(f"{_PREFIX}MAIN=loaded\n", encoding="utf-8")
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()
    monkeypatch.setenv(ENV_DOTENV_FILE, str(env_file))
    monkeypatch.delenv(f"{_PREFIX}MAIN", raising=False)

    code = main(["package", "list", "--dir", str(packages_dir), "--json"])

    assert code == 0
    assert os.environ[f"{_PREFIX}MAIN"] == "loaded"
