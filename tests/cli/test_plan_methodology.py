"""``plan``: ярлык методологии в ``run.yaml`` сверяется с семантикой кода (§13).

``methodology_version`` — объявленный вход: он уходит в манифест и в
``execution_contract_digest`` → ``run_id``, а правила исполняются из кода
(:data:`core.domain.models.METHODOLOGY_VERSION`). Разойдясь, эти два числа дают ложный
провенанс — прогон записывает одну методологию, а меряет по другой. Проверка обязана
быть **видимой** (``payload["warnings"]``) и **не блокирующей** (``EXIT_OK``, ``ok: true``):
ярлык пакета не делает измерение невозможным.

Сравнение — точное строковое: ``'0.3'`` и ``'0.3.0'`` — разные ярлыки, а не одно
semver-выражение (нормализация скрыла бы ровно тот дрейф, который ищется).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.cli.main import EXIT_OK, main
from core.domain.models import METHODOLOGY_VERSION
from tests.helpers.packages import valid_package


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD: деплой-конфиг — дефолтный ``local``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.chdir(tmp_path)
    return root


def _declare(package: Path, version: str | None) -> None:
    """Пишет ``methodology_version`` в ``run.yaml`` копии фикстуры (``None`` — убрать)."""
    path = package / "run.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if version is None:
        data.pop("methodology_version", None)
    else:
        data["methodology_version"] = version
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


def _plan(
    package: Path, capsys: pytest.CaptureFixture[str]
) -> dict[str, Any]:
    """``plan --json``; проверяет неблокирующий исход и возвращает payload целиком."""
    assert (
        main(
            [
                "plan",
                "--run-yaml",
                str(package / "run.yaml"),
                "--package",
                str(package),
                "--json",
            ]
        )
        == EXIT_OK
    )
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def test_plan_warns_when_declared_methodology_differs_from_code(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Чужой ярлык: предупреждение с обоими числами, но план остаётся ``ok``/``EXIT_OK``."""
    package = valid_package(tmp_path)
    _declare(package, "0.9.9")

    payload = _plan(package, capsys)

    assert payload["ok"] is True
    warnings = payload.get("warnings") or []
    matching = [w for w in warnings if "methodology_version" in w]
    assert len(matching) == 1, warnings
    warning = matching[0]
    # Диагностика: названы оба числа и сказано, что манифест запишет объявленное.
    assert "0.9.9" in warning
    assert METHODOLOGY_VERSION in warning
    assert "манифест" in warning


def test_plan_is_silent_when_declared_methodology_matches_code(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Совпадение ярлыка с кодом — предупреждения нет (ни в блоке, ни в ``warnings``)."""
    package = valid_package(tmp_path)
    _declare(package, METHODOLOGY_VERSION)

    payload = _plan(package, capsys)

    assert payload["ok"] is True
    assert not any(
        "methodology_version" in warning for warning in payload.get("warnings") or ()
    )


def test_plan_is_silent_when_methodology_is_not_declared(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Ярлык не объявлен → действует дефолт кода, расхождения нет (не выдумка)."""
    package = valid_package(tmp_path)
    _declare(package, None)

    payload = _plan(package, capsys)

    assert not any(
        "methodology_version" in warning for warning in payload.get("warnings") or ()
    )


def test_plan_compares_methodology_labels_exactly(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``'0.3'`` ≠ ``'0.3.0'``: ярлык сравнивается точно, без нормализации версий."""
    package = valid_package(tmp_path)
    _declare(package, ".".join(METHODOLOGY_VERSION.split(".")[:2]))

    payload = _plan(package, capsys)

    assert any(
        "methodology_version" in warning for warning in payload.get("warnings") or ()
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))