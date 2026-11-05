"""``plan``: разброс дат наблюдений числом и предупреждение по порогу отображения (план 20 §7 F2).

Источник у плана свой — ``state.db`` (``url_state.observed_at``): прогон ещё не начался, и
``corpus_artifact`` не существует. Порог ``--freshness-warn-days`` — **параметр
отображения**, а не ось: он печатается, ничего не блокирует и не входит в ``run_id``
(это и запинено ниже). Разброс печатается числом **всегда**, когда он есть.

Офлайн: поддельный ``state.db`` в изолированном ``COHOUND_HOME``, сеть не используется.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_OK, main
from core.config.paths import resolve_paths
from core.store.state_sqlite import state_db_path
from tests.helpers.packages import valid_package

D1 = "2026-01-01T00:00:00+00:00"
D9 = "2026-01-09T00:00:00+00:00"
D30 = "2026-01-31T00:00:00+00:00"


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD: деплой-конфиг — дефолтный ``local``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.chdir(tmp_path)
    return root


def _seed_state(package: Path, observations: dict[str, list[str]]) -> Path:
    """Пишет минимальный ``state.db`` с наблюдениями ``url_state`` (только чтение планом)."""
    path = state_db_path(resolve_paths().state, package.name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE url_state ("
            "product_id TEXT NOT NULL, url TEXT NOT NULL,"
            " normalized_url TEXT NOT NULL, observed_at TEXT)"
        )
        conn.executemany(
            "INSERT INTO url_state (product_id, url, normalized_url, observed_at)"
            " VALUES (?, ?, ?, ?)",
            [
                (product_id, f"https://{product_id}.test/{index}", f"https://{product_id}.test/{index}", observed_at)
                for product_id, dates in observations.items()
                for index, observed_at in enumerate(dates)
            ],
        )
    return path


def _plan(
    package: Path, capsys: pytest.CaptureFixture[str], *flags: str
) -> tuple[str, dict[str, Any]]:
    assert (
        main(
            [
                "plan",
                "--run-yaml",
                str(package / "run.yaml"),
                "--package",
                str(package),
                "--json",
                *flags,
            ]
        )
        == EXIT_OK
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1])
    return payload["run_id"], payload


def test_plan_warns_when_spread_exceeds_display_threshold(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Разброс 8 суток > порога 5 → предупреждение; число печатается в обоих случаях."""
    package = valid_package(tmp_path)
    _seed_state(package, {"p1": [D1, D9], "p2": [D30]})

    _, payload = _plan(package, capsys, "--freshness-warn-days", "5")

    block = payload["observation_freshness"]
    assert block["threshold_days"] == 5.0
    rows = {row["product_id"]: row for row in block["products"]}
    # Число — всегда, когда наблюдения есть (в том числе у продукта с одной датой).
    assert rows["p1"]["spread_days"] == 8.0
    assert rows["p1"]["from"] == D1
    assert rows["p1"]["to"] == D9
    assert rows["p2"]["spread_days"] == 0.0
    assert any("p1" in warning and "8.00" in warning for warning in block["warnings"])
    assert any("p1" in warning for warning in payload["warnings"])


def test_plan_is_silent_when_spread_is_within_threshold(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Порог 30 > разброса 8 → предупреждения нет, а число всё равно напечатано."""
    package = valid_package(tmp_path)
    _seed_state(package, {"p1": [D1, D9]})

    _, payload = _plan(package, capsys, "--freshness-warn-days", "30")

    block = payload["observation_freshness"]
    assert block["products"][0]["spread_days"] == 8.0
    assert block["warnings"] == []
    assert not any("разнесены" in warning for warning in payload.get("warnings") or ())


def test_freshness_flag_does_not_change_run_id(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Порог отображения — не ось измерения: ``run_id`` одинаков при любом значении."""
    package = valid_package(tmp_path)
    _seed_state(package, {"p1": [D1, D9]})

    baseline, _ = _plan(package, capsys)
    wide, _ = _plan(package, capsys, "--freshness-warn-days", "0")
    narrow, _ = _plan(package, capsys, "--freshness-warn-days", "400")

    assert baseline == wide == narrow


def test_plan_prints_spread_in_human_output(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Человеческий вывод называет разброс числом, а не только ``--json``."""
    package = valid_package(tmp_path)
    _seed_state(package, {"p1": [D1, D9]})

    assert (
        main(
            [
                "plan",
                "--run-yaml",
                str(package / "run.yaml"),
                "--package",
                str(package),
            ]
        )
        == EXIT_OK
    )
    out = capsys.readouterr().out
    assert "observation freshness: p1" in out
    assert "разброс 8.00 дн." in out


def test_plan_without_state_db_prints_nothing_about_freshness(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет ``state.db`` — нет и наблюдений: блок пуст, предупреждений нет (не выдумка)."""
    package = valid_package(tmp_path)
    _, payload = _plan(package, capsys)
    assert payload["observation_freshness"]["products"] == []
    assert payload["observation_freshness"]["warnings"] == []


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))