"""Тесты CLI-команды ``baseline-agreement`` (§11.5, :mod:`core.cli.commands.baseline`).

Покрывают: успешный расчёт (``--json`` отдаёт оба κ, счётчики, матрицы; код ``EXIT_OK``);
человекочитаемый вывод; жёсткий контракт CSV (неверная форма → ``EXIT_USAGE`` до расчёта);
метки вне словаря статусов → ``ok: no`` + ``EXIT_USAGE``; честный ``undefined`` для
вырожденного κ; непарные ячейки (один аннотатор) в списке, но вне κ; отсутствующий файл.

Данные категорийно-нейтральны (§1.3): метки из ``Status``, идентификаторы ``p1``/``param_x``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.cli.commands import COMMANDS
from core.cli.main import EXIT_OK, EXIT_USAGE, build_parser, main
from core.domain.enums import Status
from tests.helpers.output import json_out

OK = Status.OK.value
ABSENT = Status.ABSENT.value
NA = Status.NOT_APPLICABLE.value
HEADER = "product_id,param_id,annotator_a,annotator_b"

#: Полный набор меток, где общий κ и N/A-κ расходятся (сердце §11.5).
DIVERGENT_ROWS = (
    [(OK, OK)] * 10 + [(ABSENT, ABSENT)] * 4 + [(NA, ABSENT)] * 3 + [(ABSENT, NA)] * 3
)


def _write(path: Path, rows: list[tuple[str, str]], *, header: str = HEADER) -> Path:
    lines = [header, *(f"p1,param_{i},{a},{b}" for i, (a, b) in enumerate(rows))]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _argv(path: Path, *extra: str) -> list[str]:
    return ["baseline-agreement", "--annotations", str(path), *extra]


# ---------------------------------------------------------------------------
# Регистрация и успешный расчёт
# ---------------------------------------------------------------------------


def test_json_reports_overall_and_na_kappa(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "annotations.csv", DIVERGENT_ROWS)
    code = main(_argv(path, "--json"))
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["ok"] is True
    assert payload["total_items"] == 20
    assert payload["paired_items"] == 20
    assert payload["overall"]["defined"] is True
    assert payload["overall"]["kappa"] == pytest.approx(0.5041322314, abs=1e-9)
    assert payload["na"]["kappa"] == pytest.approx(-0.1764705882, abs=1e-9)
    assert payload["na"]["kappa"] < payload["overall"]["kappa"]
    assert payload["na_counts"] == {"annotator_a": 3, "annotator_b": 3}
    assert len(payload["na_disagreements"]) == 6
    assert payload["unknown_labels"] == {}


def test_human_output_prints_both_kappa(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "annotations.csv", DIVERGENT_ROWS)
    code = main(_argv(path))
    out = capsys.readouterr().out

    assert code == EXIT_OK
    assert "overall kappa:" in out
    assert "na kappa:" in out
    assert f"[{NA}: a=3, b=3]" in out
    assert "ok: yes" in out


# ---------------------------------------------------------------------------
# Контракт CSV: неверная форма отвергается, а не додумывается
# ---------------------------------------------------------------------------


def test_bad_shape_is_rejected(tmp_path: Path, capsys) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("product_id,annotator_a,annotator_b\np1,✅,✅\n", encoding="utf-8")
    code = main(_argv(path))

    assert code == EXIT_USAGE
    assert "param_id" in capsys.readouterr().err


def test_duplicate_cell_is_rejected(tmp_path: Path, capsys) -> None:
    path = tmp_path / "dup.csv"
    path.write_text(
        f"{HEADER}\np1,param_x,✅,✅\np1,param_x,❌,❌\n", encoding="utf-8"
    )
    code = main(_argv(path))

    assert code == EXIT_USAGE
    assert "дубликат" in capsys.readouterr().err


def test_missing_file_is_usage_error(tmp_path: Path, capsys) -> None:
    code = main(_argv(tmp_path / "absent.csv"))
    assert code == EXIT_USAGE
    assert "не удалось прочитать" in capsys.readouterr().err


def test_unknown_label_fails_but_still_reports(tmp_path: Path, capsys) -> None:
    path = _write(
        tmp_path / "unknown.csv",
        [("OK", OK), (ABSENT, ABSENT)],
    )
    code = main(_argv(path, "--json"))
    payload = json_out(capsys)

    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert payload["unknown_labels"] == {"OK": 1}


# ---------------------------------------------------------------------------
# Вырожденные случаи и непарные ячейки — сообщаются, не фабрикуются
# ---------------------------------------------------------------------------


def test_undefined_na_kappa_is_reported(tmp_path: Path, capsys) -> None:
    # N/A-решений нет вовсе → маргинал бинарной разметки вырожден: κ не определён.
    path = _write(tmp_path / "no_na.csv", [(OK, OK), (ABSENT, ABSENT)])
    code = main(_argv(path, "--json"))
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["overall"]["defined"] is True
    assert payload["na"]["defined"] is False
    assert payload["na"]["kappa"] is None
    assert payload["na"]["undefined_reason"]


def test_fully_degenerate_kappa_is_undefined(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "flat.csv", [(OK, OK), (OK, OK)])
    code = main(_argv(path, "--json"))
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["overall"]["defined"] is False
    assert payload["overall"]["kappa"] is None
    assert payload["overall"]["undefined_reason"]


def test_single_annotator_items_are_listed_not_counted(tmp_path: Path, capsys) -> None:
    path = tmp_path / "unpaired.csv"
    path.write_text(
        f"{HEADER}\np1,param_x,✅,✅\np1,param_y,❌,\np1,param_z,,✅\n",
        encoding="utf-8",
    )
    code = main(_argv(path, "--json"))
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["total_items"] == 3
    assert payload["paired_items"] == 1
    assert set(payload["unpaired_items"]) == {"p1/param_y", "p1/param_z"}
