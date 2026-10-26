"""I21/§7.4: объявленная таблица ``scenario → модель`` персистится в строку прогона.

Регресс на разрыв, найденный измерением: ``_resolve_llm_scenarios`` входила в
``execution_contract_digest``, но строки ``llm_scenarios`` не писались **никем** (у
``SqliteStore.save_llm_scenarios`` не было ни одного вызова в ``core/``). Следствие:
смена модели под сценарий меняла ``run_id``/контракт, но в БД прогона нельзя было
прочитать, какая модель его обслуживала, — ``hound report``/``hound diff`` не видели
имени модели.

Проверяется:

* ``run`` и ``crawl`` пишут строку из **декларированных** осей (одна формула с контрактом);
* пустой (неактивный) слот не выдумывает строку;
* сбой записи не роняет прогон (``instrumentation_errors``), т.е. durable-факт не
  становится новой точкой отказа;
* ``web_tool=False`` и ``endpoint_kind=api`` — значения по построению, а не настройка (I18).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from core.cli.commands.run import _persist_llm_scenarios
from core.cli.main import EXIT_OK, main
from core.domain.enums import EndpointKind, LLMScenario

#: Корень e2e-фикстур: прогон останавливается на человеческом гейте ``g1`` — до сети/LLM.
E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"

#: Декларированный маршрут, который должен попасть в БД прогона.
DECLARED_ROW: dict[str, Any] = {
    "scenario": "evaluator",
    "model_id": "cheap-model",
    "model_version": None,
    "endpoint_kind": "api",
    "web_tool": False,
    "decoder": "json_object",
}


def _deploy_yaml(tmp_path: Path, *, active: bool = True) -> Path:
    """``config/deploy.yaml`` в CWD теста (путь резолвится относительно CWD)."""
    (tmp_path / "config").mkdir(exist_ok=True)
    cheap: dict[str, Any] = {"model_id": "cheap-model", "decoder": "json_object"}
    if active:
        cheap["base_url"] = "https://llm.test/v1"
    target = tmp_path / "config" / "deploy.yaml"
    target.write_text(
        yaml.safe_dump(
            {"profile": "local", "llm": {"slot": "cheap", "cheap": cheap}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return target


def _common(runs_dir: Path, run_id: str) -> list[str]:
    return [
        "--run-yaml",
        str(E2E_FIXTURES / "run.yaml"),
        "--package",
        str(E2E_FIXTURES),
        "--runs-dir",
        str(runs_dir),
        "--run-id",
        run_id,
        "--json",
    ]


def _rows(runs_dir: Path, run_id: str) -> list[tuple[Any, ...]]:
    db = runs_dir / run_id / "cohound.db"
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return conn.execute(
            "SELECT run_id, scenario, model_id, model_version, endpoint_kind, web_tool,"
            " decoder, seed, temperature, constrained_decoding FROM llm_scenarios"
        ).fetchall()
    finally:
        conn.close()


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD: ``config/deploy.yaml`` ищется в ``tmp_path``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.chdir(tmp_path)
    return root


def test_run_persists_declared_scenario_binding(
    tmp_path: Path, home: Path
) -> None:
    """``run`` пишет строку сразу после создания строки прогона (до фаз)."""
    _deploy_yaml(tmp_path)
    runs_dir = tmp_path / "runs"

    # ``--until g2``: проверяется персист строки до фаз, а не исполнение корпуса/оценки
    # (внешние контуры здесь не нужны — сеть запрещена контрактом теста).
    assert (
        main(["run", *_common(runs_dir, "llm-scen-run"), "--until", "g2"]) == EXIT_OK
    )

    rows = _rows(runs_dir, "llm-scen-run")
    assert len(rows) == 1
    run_id, scenario, model_id, version, endpoint, web_tool, decoder, seed, temp, _ = rows[0]
    assert run_id == "llm-scen-run"
    assert scenario == "evaluator"
    assert model_id == "cheap-model"
    assert version is None
    assert endpoint == "api"
    assert web_tool == 0  # I18: оценщик без веб-тула — по построению
    assert decoder == "json_object"
    assert seed is None and temp is None  # не объявлены — не выдумываем


def test_crawl_persists_declared_scenario_binding(tmp_path: Path, home: Path) -> None:
    """``crawl`` создаёт ту же строку прогона — манифест не должен расходиться с БД."""
    _deploy_yaml(tmp_path)
    runs_dir = tmp_path / "runs"

    argv = [
        "crawl",
        "run",
        *_common(runs_dir, "llm-scen-crawl"),
        "--until",
        "plan",
    ]
    assert main(argv) == EXIT_OK

    rows = _rows(runs_dir, "llm-scen-crawl")
    assert [(r[1], r[2]) for r in rows] == [("evaluator", "cheap-model")]


def test_inactive_slot_writes_no_row(tmp_path: Path, home: Path) -> None:
    """Пустой ``base_url`` = оценщик не поднимается: строку не выдумываем."""
    _deploy_yaml(tmp_path, active=False)
    runs_dir = tmp_path / "runs"

    # ``--until g2``: G1/G2 не блокируют, поэтому прогон доходит до второй фазы-гейта;
    # проверяется отсутствие строки сценария, а не исход фаз.
    assert (
        main(["run", *_common(runs_dir, "llm-scen-empty"), "--until", "g2"]) == EXIT_OK
    )
    assert _rows(runs_dir, "llm-scen-empty") == []


def test_store_failure_is_reported_not_raised() -> None:
    """Сбой записи — ``instrumentation_errors``, а не падение прогона."""

    class _Boom:
        def save_llm_scenarios(self, bindings: list[Any]) -> None:  # noqa: ANN401
            raise sqlite3.OperationalError("database is locked")

    errors = _persist_llm_scenarios(
        _Boom(), "run-x", SimpleNamespace(llm_scenarios=(DECLARED_ROW,))
    )
    assert len(errors) == 1
    assert errors[0].startswith("llm_scenarios: OperationalError:")
    assert "locked" in errors[0]


def test_binding_carries_enum_values_and_i18_flag() -> None:
    """Значения — enum'ы (не строковые литералы), ``web_tool`` — False."""
    captured: list[Any] = []

    class _Store:
        def save_llm_scenarios(self, bindings: list[Any]) -> None:  # noqa: ANN401
            captured.extend(bindings)

    assert (
        _persist_llm_scenarios(
            _Store(), "run-y", SimpleNamespace(llm_scenarios=(DECLARED_ROW,))
        )
        == []
    )
    (binding,) = captured
    assert binding.run_id == "run-y"
    assert binding.scenario is LLMScenario.EVALUATOR
    assert binding.endpoint_kind is EndpointKind.API
    assert binding.web_tool is False


def test_undeclared_rows_are_a_noop() -> None:
    """Пустая таблица (нет активного слота) — ноль вызовов хранилища."""

    class _Store:
        def save_llm_scenarios(self, bindings: list[Any]) -> None:  # noqa: ANN401
            raise AssertionError("не должно вызываться")

    assert _persist_llm_scenarios(_Store(), "run-z", SimpleNamespace(llm_scenarios=())) == []
