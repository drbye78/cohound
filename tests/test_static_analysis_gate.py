"""Гейты статического анализа: конфиг линтера и baseline типов — часть контракта CI.

Проверки не запускают сам ``ruff``/``mypy`` (это делает ``scripts/ci.sh``), а защищают
**форму** гейта от тихой эрозии: набор правил не пустеет, baseline не впитывает ошибки
домена, а ``ci.sh`` не забывает вызвать проверку. Именно так выглядит деградация гейта,
которую не видно в зелёном прогоне: правила «временно» расширили исключениями, а
baseline перегенерировали, не читая.
"""

from __future__ import annotations

import pathlib
import re
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASELINE = ROOT / "docs" / "architecture" / "mypy-baseline.txt"
GATE_SCRIPT = ROOT / "scripts" / "mypy_gate.py"
CI_SCRIPT = ROOT / "scripts" / "ci.sh"

#: Правила, ради которых линтер вообще включён. Каждое ловит класс дефекта, который
#: runtime-валидация (Pydantic `extra="forbid"`, `StrEnum`) и selfcheck не видят:
#: мёртвый код и неиспользуемые имена (`F`), молчание вместо видимого отказа (`BLE`,
#: `S110`), позднее связывание в замыкании цикла (`B023`), undefined-name в аннотации.
REQUIRED_SELECT = ("F", "B", "BLE", "I", "UP", "SIM", "S", "RUF")


def _ruff_config() -> dict:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data["tool"]["ruff"]


def test_ruff_select_keeps_the_rules_that_justify_the_gate() -> None:
    select = _ruff_config()["lint"]["select"]
    for code in REQUIRED_SELECT:
        assert code in select, f"правило {code} выпало из ruff.select — гейт ослаблен"


def test_ruff_ignores_are_named_with_a_reason() -> None:
    """Игноры допустимы, но их состав фиксирован: добавление — отдельное решение.

    ``RUF001/002/003`` — кириллица в докстрингах и комментариях (норма проекта);
    ``RUF100`` — «неиспользуемая noqa» при заведомо суженном наборе правил;
    ``SIM300`` — автофикс ruff 0.17.3 инвертирует сравнение с вызовом-операндом и создаёт
    йода-условие, против которого правило написано (замер — 05 §2.7.1).
    """
    config = _ruff_config()
    assert set(config["lint"]["ignore"]) == {"RUF001", "RUF002", "RUF003", "RUF100", "SIM300"}


def test_ruff_keeps_asserts_allowed_only_in_tests() -> None:
    """``assert`` — механизм тестов, но не проверка в ``core/``."""
    per_file = _ruff_config()["lint"]["per-file-ignores"]
    assert "S101" in per_file["tests/**"]
    assert not any(code == "S101" for code in _ruff_config()["lint"].get("ignore", []))


def test_mypy_domain_gate_is_declared_and_checked() -> None:
    """Домен обязан быть типизирован: это гейт «ноль», а не baseline."""
    assert "core/domain/" in GATE_SCRIPT.read_text(encoding="utf-8")
    ci = CI_SCRIPT.read_text(encoding="utf-8")
    assert "mypy_gate.py" in ci, "ci.sh перестал запускать гейт типов"
    assert "ruff" in ci, "ci.sh перестал запускать линтер"


def test_baseline_contains_no_domain_errors() -> None:
    """Запись ``core/domain/...`` в baseline — способ обойти гейт домена."""
    assert BASELINE.is_file(), "baseline типов отсутствует: python3 scripts/mypy_gate.py --update"
    entries = _baseline_entries()
    assert entries, "пустой baseline означает, что долг не измерен, а не что его нет"
    offenders = [e for e in entries if e.startswith("core/domain/")]
    assert not offenders, f"ошибки домена попали в baseline: {offenders[:3]}"


def _baseline_entries() -> list[str]:
    lines = BASELINE.read_text(encoding="utf-8").splitlines()
    return [ln for ln in lines if ln.strip() and not ln.lstrip().startswith("#")]


def test_baseline_entries_are_well_formed_and_line_number_free() -> None:
    """Ключ без номера строки: иначе baseline перегенерируют после каждой правки выше."""
    pattern = re.compile(r"^core/[^:]+: .+\[[a-z-]+\]$")
    bad = [e for e in _baseline_entries() if pattern.match(e) is None]
    assert not bad, f"строки baseline не в формате `путь: сообщение [код]`: {bad[:3]}"
