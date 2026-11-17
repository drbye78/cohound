"""``plan``: дата и источник эталона — рядом с числом, а не в отдельном файле.

Предмет — видимость факта: ``golden: 905 (state: present)`` не говорит, что эталон
полуторамесячной давности. Источник даты — **один**: ``sources/provenance.yaml`` пакета
(:func:`core.pipeline.golden_status.read_provenance`); второго носителя нет.

Печатается только объявленный факт:

* ``built`` — ``generated_at`` (когда собран **пакет**);
* ``snapshot`` — ``snapshot_date`` (дата **состояния источника**), и только если она
  **отличается** от ``generated_at``: совпадение — одна дата, объявленная дважды, а не
  дата таблицы;
* ``source``/``sha256`` — первый объявленный источник и первые 12 hex отпечатка.

Файла нет — нет и даты: строка остаётся прежней, пустых скобок не появляется.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.pipeline.golden_status import provenance_path
from tests.helpers.output import json_out
from tests.helpers.packages import valid_package

SHA = "6c7edf0d8219640bc5fe8d5e1d75a29fa0fb9973349ddad817ef677f0d95d183"

_PROVENANCE = (
    "case_id: case_x\n"
    "generated_at: '2026-10-28T00:00:00Z'\n"
    "generator: build.py\n"
    "snapshot_date: '2026-10-28'\n"
    "sources:\n"
    "- path: /tmp/export/golden_00.xlsx\n"
    "  role: сравнительная таблица\n"
    f"  sha256: {SHA}\n"
    "  sheet: Лист1\n"
)


def _write_provenance(package: Path, text: str = _PROVENANCE) -> Path:
    """Пишет ``sources/provenance.yaml`` в пакет."""
    path = provenance_path(package)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _plan(
    package: Path, capsys: pytest.CaptureFixture[str], *, json: bool
) -> dict[str, object] | str:
    """Запускает ``plan`` и отдаёт payload (``--json``) либо человеческий вывод."""
    argv = [
        "plan",
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
    ]
    if json:
        argv.append("--json")
    assert main(argv) == EXIT_OK
    captured = capsys.readouterr()
    return _payload(captured.out) if json else captured.out


def _payload(out: str) -> dict[str, object]:
    """JSON-объект плана из stdout."""
    return json.loads(out.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# Объявленное происхождение — видно и в payload, и в строке
# ---------------------------------------------------------------------------


def test_plan_payload_carries_built_source_and_digest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Дата сборки, имя первого источника и первые 12 hex — в ``plan --json``."""
    package = valid_package(tmp_path)
    _write_provenance(package)

    payload = _plan(package, capsys, json=True)

    assert isinstance(payload, dict)
    assert payload["golden_built"] == "2026-10-28"
    assert payload["golden_source"] == "golden_00.xlsx"
    assert payload["golden_source_sha256"] == "6c7edf0d8219"


def test_plan_payload_hides_snapshot_equal_to_generated_at(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Совпадение дат не публикуется как дата источника: это одна дата дважды."""
    package = valid_package(tmp_path)
    _write_provenance(package)

    payload = _plan(package, capsys, json=True)

    assert isinstance(payload, dict)
    assert payload["golden_snapshot"] is None


def test_plan_payload_publishes_distinct_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Отдельно объявленная дата источника видна отдельным полем."""
    package = valid_package(tmp_path)
    _write_provenance(package, _PROVENANCE.replace("'2026-10-28'\n", "'2026-10-01'\n"))

    payload = _plan(package, capsys, json=True)

    assert isinstance(payload, dict)
    assert payload["golden_built"] == "2026-10-28"
    assert payload["golden_snapshot"] == "2026-10-01"


def test_plan_prints_date_and_source_with_the_count(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Строка ``golden:`` несёт дату сборки, источник и отпечаток — точный текст."""
    package = valid_package(tmp_path)
    _write_provenance(package)

    out = _plan(package, capsys, json=False)

    assert isinstance(out, str)
    line = next(line for line in out.splitlines() if line.startswith("golden:"))
    assert line == (
        "golden: 2 (state: present, built: 2026-10-28, "
        "source: golden_00.xlsx, sha256: 6c7edf0d8219)"
    )


def test_plan_prints_distinct_snapshot_between_built_and_source(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``snapshot`` печатается, когда дата источника объявлена отдельно."""
    package = valid_package(tmp_path)
    _write_provenance(package, _PROVENANCE.replace("'2026-10-28'\n", "'2026-10-01'\n"))

    out = _plan(package, capsys, json=False)

    assert isinstance(out, str)
    line = next(line for line in out.splitlines() if line.startswith("golden:"))
    assert line == (
        "golden: 2 (state: present, built: 2026-10-28, snapshot: 2026-10-01, "
        "source: golden_00.xlsx, sha256: 6c7edf0d8219)"
    )


# ---------------------------------------------------------------------------
# Нет файла — нет даты: строка прежняя, без пустых скобок
# ---------------------------------------------------------------------------


def test_plan_without_provenance_keeps_old_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет ``provenance.yaml`` — не печатаем ни ``unknown``, ни пустых скобок."""
    package = valid_package(tmp_path)

    out = _plan(package, capsys, json=False)

    assert isinstance(out, str)
    line = next(line for line in out.splitlines() if line.startswith("golden:"))
    assert line == "golden: 2 (state: present)"


def test_plan_without_provenance_payload_fields_are_none(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Форма payload стабильна: ключи есть, значение ``None`` — не пропуск ключа."""
    package = valid_package(tmp_path)

    payload = _plan(package, capsys, json=True)

    assert isinstance(payload, dict)
    assert payload["golden_built"] is None
    assert payload["golden_snapshot"] is None
    assert payload["golden_source"] is None
    assert payload["golden_source_sha256"] is None


# ---------------------------------------------------------------------------
# Опечатка в объявлении — видимая ошибка пакета
# ---------------------------------------------------------------------------


def test_plan_with_broken_provenance_is_visible_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``snapshot_date`` не ISO — ``EXIT_USAGE`` с кодом ``provenance.yaml``."""
    package = valid_package(tmp_path)
    _write_provenance(package, _PROVENANCE.replace("'2026-10-28'\n", "'28.10.2026'\n"))

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
        == EXIT_USAGE
    )
    payload = json_out(capsys)
    assert payload["error"] == "provenance.yaml"
    assert "не является датой ISO-8601" in payload["message"]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
