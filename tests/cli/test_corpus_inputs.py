"""Тесты CLI-входа корпуса: паспорта (``--passports-dir``) и URL (``--urls``).

Проверяют проводку ``run``:

* ``--passports-dir`` — существующий валидный каталог даёт паспорта фазе Ш4;
  отсутствующий каталог → ``{}`` (безопасный дефолт); существующий, но
  несоответствующий схеме §4.3 → понятная ошибка CLI (``EXIT_USAGE``, код
  ``passports``), а не падение;
* ``--urls`` — JSON/YAML-отображение ``{product_id: [url, ...]}`` принимается и
  передаётся в Ш5; не-отображение/не-список строк/отсутствующий файл →
  ``EXIT_USAGE`` с кодом ``urls``. ``doc_domains`` паспортов **не** фетчатся молча.

Данные категорийно-нейтральны (§1.3); используются ``tmp_path`` и ``phases_factory``
(тестовый шов composition root — прогон без внешних систем).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from tests.helpers.packages import capturing_factory, run_argv, valid_package

CONFIG_FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"


# ---------------------------------------------------------------------------
# --passports-dir
# ---------------------------------------------------------------------------


def test_passports_dir_loads_valid_passports(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    passports = tmp_path / "passports"
    shutil.copytree(CONFIG_FIXTURES / "passports", passports)

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--passports-dir", str(passports)],
        phases_factory=factory,
    )
    assert code == EXIT_OK
    assert set(captured["passports"]) == {"p1"}
    assert captured["passports"]["p1"].product_id == "p1"


def test_passports_dir_missing_is_empty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--passports-dir", str(tmp_path / "nope")],
        phases_factory=factory,
    )
    assert code == EXIT_OK
    assert captured["passports"] == {}


def test_malformed_passports_dir_is_clear_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    passports = tmp_path / "passports"
    (passports / "p1").mkdir(parents=True)
    # YAML-список вместо словаря паспорта → PassportLoadError (§4.3).
    (passports / "p1" / "2026-01-01.yaml").write_text("- not\n- a\n- map\n", encoding="utf-8")

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--passports-dir", str(passports)],
        phases_factory=factory,
    )
    assert code == EXIT_USAGE
    captured_out = capsys.readouterr()
    assert "passports" in captured_out.out + captured_out.err
    assert captured == {}  # фазы не собирались


def test_default_passports_dir_absent_is_noop(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без ``--passports-dir`` дефолт ``passports`` (нет каталога в cwd) → ``{}``.

    Дефолт относителен cwd, поэтому тест обязан быть cwd-независимым: переходим в
    ``tmp_path``, где каталога ``passports`` заведомо нет. Иначе наличие реального
    ``passports/`` в корне репозитория (пакеты кейсов) даёт ложный провал.
    """
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    captured, factory = capturing_factory()
    code = main(run_argv(package, run_id="inputs-1", db=db, until="plan"), phases_factory=factory)
    assert code == EXIT_OK
    assert captured["passports"] == {}


# ---------------------------------------------------------------------------
# --urls
# ---------------------------------------------------------------------------


def test_urls_json_mapping_accepted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    urls_path = tmp_path / "urls.json"
    urls_path.write_text(
        json.dumps({"p1": ["https://docs.example/p1"], "p2": []}), encoding="utf-8"
    )

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--urls", str(urls_path)], phases_factory=factory
    )
    assert code == EXIT_OK
    assert captured["urls"] == {
        "p1": ["https://docs.example/p1"],
        "p2": [],
    }


def test_urls_yaml_mapping_accepted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    urls_path = tmp_path / "urls.yaml"
    urls_path.write_text(
        "p1:\n  - https://docs.example/p1\n", encoding="utf-8"
    )

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--urls", str(urls_path)], phases_factory=factory
    )
    assert code == EXIT_OK
    assert captured["urls"] == {"p1": ["https://docs.example/p1"]}


def test_urls_without_flag_is_empty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    captured, factory = capturing_factory()
    code = main(run_argv(package, run_id="inputs-1", db=db, until="plan"), phases_factory=factory)
    assert code == EXIT_OK
    assert captured["urls"] == {}


@pytest.mark.parametrize(
    "payload",
    [
        '["https://docs.example/p1"]',  # список, а не отображение
        '{"p1": "https://docs.example/p1"}',  # значение — строка, не список
        '{"p1": [1, 2]}',  # список не-строк
    ],
)
def test_urls_invalid_shape_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], payload: str
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    urls_path = tmp_path / "urls.json"
    urls_path.write_text(payload, encoding="utf-8")

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--urls", str(urls_path)], phases_factory=factory
    )
    assert code == EXIT_USAGE
    captured_out = capsys.readouterr()
    assert "urls" in captured_out.out + captured_out.err
    assert captured == {}


def test_urls_missing_file_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--urls", str(tmp_path / "absent.json")],
        phases_factory=factory,
    )
    assert code == EXIT_USAGE
    captured_out = capsys.readouterr()
    assert "urls" in captured_out.out + captured_out.err


def test_urls_bad_json_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    urls_path = tmp_path / "urls.json"
    urls_path.write_text("{not json", encoding="utf-8")

    _, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="inputs-1", db=db, until="plan"), "--urls", str(urls_path)], phases_factory=factory
    )
    assert code == EXIT_USAGE
    captured_out = capsys.readouterr()
    assert "urls" in captured_out.out + captured_out.err
