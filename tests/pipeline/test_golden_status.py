"""Тесты объявления состояния эталонного набора (``golden_status/1``, §11.3).

Предмет: ``golden.csv`` — проверочный инструмент, а не условие измерения, поэтому
«сверять не с чем» — законный режим (первая версия таблицы сравнения, новый продукт в
когорте). Но режим обязан быть **видимым**: состояние объявляется файлом пакета
``sources/golden_status.yaml`` (``absent`` | ``present`` | ``declined``), а не выводится
из молчаливого отсутствия файла.

Проверяется:

* вывод состояния из наличия ``golden.csv``, когда объявления нет;
* три объявленных состояния, включая ``declined`` — файл остаётся на месте;
* ошибки пакета: ``present`` без файла, ``absent`` при файле, ``declined`` без причины
  или автора, чужая схема, неизвестный ключ/состояние, ``reason`` вне ``declined``;
* как состояние доходит до входов прогона (``load_inputs``/``plan``): при
  ``absent``/``declined`` строки эталона не читаются вовсе, состояние видно вызывающему.

Данные категорийно-нейтральны (§1.3): ни названий продуктов, ни предметных терминов.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

from core.pipeline.golden_status import (
    GOLDEN_STATUS_SCHEMA,
    STATE_ABSENT,
    STATE_DECLINED,
    STATE_PRESENT,
    golden_status_path,
    read_golden_status,
)
from core.pipeline.normalize import load_inputs
from core.pipeline.plan import plan

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"

#: Артефакты, которых достаточно для ``load_inputs`` (golden.csv — по требованию теста).
_PACKAGE_FILES = (
    "package.yaml",
    "catalog.csv",
    "baselines.csv",
    "cohort.yaml",
    "domain_profiles.yaml",
)

_HEADER = "schema: golden_status/1\n"


def _package(tmp_path: Path, *, golden: bool = True) -> Path:
    """Копия фикстур в ``tmp_path``; ``golden=False`` — без ``golden.csv``."""
    for name in _PACKAGE_FILES:
        shutil.copy(FIXTURES / name, tmp_path / name)
    if golden:
        shutil.copy(FIXTURES / "golden.csv", tmp_path / "golden.csv")
    return tmp_path


def _declare(package: Path, text: str) -> Path:
    """Пишет объявление в ``{package}/sources/golden_status.yaml``."""
    path = golden_status_path(package)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Вывод состояния без объявления
# ---------------------------------------------------------------------------


def test_no_declaration_with_file_is_present(tmp_path: Path) -> None:
    status = read_golden_status(_package(tmp_path))
    assert status.state == STATE_PRESENT
    assert status.declared is False
    assert status.file_present is True
    assert status.comparison_available is True
    assert status.reason is None


def test_no_declaration_without_file_is_absent(tmp_path: Path) -> None:
    status = read_golden_status(_package(tmp_path, golden=False))
    assert status.state == STATE_ABSENT
    assert status.declared is False
    assert status.file_present is False
    assert status.comparison_available is False


def test_comment_only_declaration_declares_nothing(tmp_path: Path) -> None:
    """Файл-шаблон из одних комментариев не объявляет состояние (иначе шаблон решал бы за автора)."""
    package = _package(tmp_path)
    _declare(package, "# пример: раскомментируйте state: absent\n")
    status = read_golden_status(package)
    assert status.state == STATE_PRESENT
    assert status.declared is False


# ---------------------------------------------------------------------------
# Объявленные состояния
# ---------------------------------------------------------------------------


def test_declared_present(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\n")
    status = read_golden_status(package)
    assert status.state == STATE_PRESENT
    assert status.declared is True
    assert status.comparison_available is True


def test_declared_absent_without_file(tmp_path: Path) -> None:
    package = _package(tmp_path, golden=False)
    _declare(package, _HEADER + "state: absent\n")
    status = read_golden_status(package)
    assert status.state == STATE_ABSENT
    assert status.declared is True
    assert status.comparison_available is False
    assert "state: absent" in status.describe()


def test_declared_declined_keeps_file(tmp_path: Path) -> None:
    """``declined`` не удаляет и не переименовывает файл: данные переделывают, а не теряют."""
    package = _package(tmp_path)
    _declare(
        package,
        _HEADER
        + "state: declined\n"
        + 'reason: "набор размечали не на эти продукты"\n'
        + "declined_by: analyst\n"
        + "declined_at: '2026-01-31'\n",
    )
    status = read_golden_status(package)
    assert status.state == STATE_DECLINED
    assert status.declared is True
    assert status.comparison_available is False
    assert status.file_present is True
    assert status.reason == "набор размечали не на эти продукты"
    assert status.declined_by == "analyst"
    assert status.declined_at == "2026-01-31"
    assert (package / "golden.csv").is_file()
    assert "размечали не на эти продукты" in status.describe()


def test_declined_at_accepts_yaml_date_scalar(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(
        package,
        _HEADER
        + "state: declined\n"
        + 'reason: "устарел"\n'
        + "declined_by: analyst\n"
        + "declined_at: 2026-01-31\n",
    )
    assert read_golden_status(package).declined_at == "2026-01-31"


def test_status_as_dict_is_json_ready(tmp_path: Path) -> None:
    package = _package(tmp_path, golden=False)
    _declare(package, _HEADER + "state: absent\n")
    payload = read_golden_status(package).as_dict()
    assert payload == {
        "state": "absent",
        "declared": True,
        "file_present": False,
        "reason": None,
        "declined_by": None,
        "declined_at": None,
    }


# ---------------------------------------------------------------------------
# Ошибки пакета (объявлено ↔ на диске)
# ---------------------------------------------------------------------------


def test_present_without_file_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path, golden=False)
    _declare(package, _HEADER + "state: present\n")
    with pytest.raises(ValueError) as excinfo:
        read_golden_status(package)
    message = str(excinfo.value)
    assert message.startswith("golden_status.yaml:")
    assert "golden.csv отсутствует" in message


def test_absent_with_existing_file_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: absent\n")
    with pytest.raises(ValueError) as excinfo:
        read_golden_status(package)
    message = str(excinfo.value)
    assert message.startswith("golden_status.yaml:")
    assert "неоднозначность" in message
    # Файл не тронут: разбирать противоречие — человеку, а не команде.
    assert (package / "golden.csv").is_file()


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param("", id="both-missing"),
        pytest.param('reason: "устарел"\n', id="no-author"),
        pytest.param("declined_by: analyst\n", id="no-reason"),
        pytest.param('reason: "  "\ndeclined_by: "  "\n', id="blank-values"),
    ],
)
def test_declined_requires_reason_and_author(tmp_path: Path, extra: str) -> None:
    """Waiver без причины и автора запрещён: это был бы тихий отказ от сверки."""
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: declined\n" + extra)
    with pytest.raises(ValueError) as excinfo:
        read_golden_status(package)
    message = str(excinfo.value)
    assert message.startswith("golden_status.yaml:")
    assert "declined" in message


def test_wrong_schema_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, "schema: golden_status/9\nstate: present\n")
    with pytest.raises(ValueError, match=GOLDEN_STATUS_SCHEMA):
        read_golden_status(package)


def test_missing_schema_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, "state: present\n")
    with pytest.raises(ValueError, match="schema обязателен"):
        read_golden_status(package)


def test_unknown_state_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: maybe\n")
    with pytest.raises(ValueError, match="неизвестное состояние"):
        read_golden_status(package)


def test_unknown_key_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\nskip_comparison: true\n")
    with pytest.raises(ValueError, match="неизвестные ключи"):
        read_golden_status(package)


def test_reason_outside_declined_is_error(tmp_path: Path) -> None:
    """``reason`` при ``present`` — неоднозначное объявление, а не комментарий."""
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\nreason: \"просто так\"\n")
    with pytest.raises(ValueError, match="только при state: declined"):
        read_golden_status(package)


def test_bad_declined_at_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(
        package,
        _HEADER
        + "state: declined\n"
        + 'reason: "устарел"\n'
        + "declined_by: analyst\n"
        + "declined_at: '31.01.2026'\n",
    )
    with pytest.raises(ValueError, match="ISO-8601"):
        read_golden_status(package)


def test_non_mapping_declaration_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, "- state: present\n")
    with pytest.raises(ValueError, match="YAML-словарь"):
        read_golden_status(package)


# ---------------------------------------------------------------------------
# Входы прогона и план
# ---------------------------------------------------------------------------


def test_load_inputs_without_golden_file_has_empty_golden(tmp_path: Path) -> None:
    inputs = load_inputs(RUN_YAML, _package(tmp_path, golden=False))
    assert inputs.golden == []
    assert inputs.golden_status is not None
    assert inputs.golden_status.state == STATE_ABSENT
    assert inputs.golden_status.declared is False


def test_load_inputs_declined_does_not_read_rows(tmp_path: Path) -> None:
    """Объявленный непригодным набор не является входом: строки не читаются вовсе."""
    package = _package(tmp_path)
    (package / "golden.csv").write_text("это не CSV-набор, а мусор\n", encoding="utf-8")
    _declare(
        package,
        _HEADER
        + "state: declined\n"
        + 'reason: "разметка непригодна"\n'
        + "declined_by: analyst\n",
    )
    inputs = load_inputs(RUN_YAML, package)
    assert inputs.golden == []
    assert inputs.golden_status is not None
    assert inputs.golden_status.state == STATE_DECLINED


def test_load_inputs_present_reads_rows(tmp_path: Path) -> None:
    """``present`` — прежнее поведение: набор читается и участвует в сверке."""
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\n")
    inputs = load_inputs(RUN_YAML, package)
    assert len(inputs.golden) == 2
    assert inputs.golden_status is not None
    assert inputs.golden_status.comparison_available is True


def test_load_inputs_propagates_package_error(tmp_path: Path) -> None:
    package = _package(tmp_path, golden=False)
    _declare(package, _HEADER + "state: present\n")
    with pytest.raises(ValueError, match=re.escape("golden_status.yaml")):
        load_inputs(RUN_YAML, package)


def test_plan_carries_golden_status(tmp_path: Path) -> None:
    package = _package(tmp_path, golden=False)
    _declare(package, _HEADER + "state: absent\n")
    result = plan(load_inputs(RUN_YAML, package), run_id="run.x")
    assert result.golden == []
    assert result.golden_status is not None
    assert result.golden_status.state == STATE_ABSENT
    assert result.golden_status.declared is True
