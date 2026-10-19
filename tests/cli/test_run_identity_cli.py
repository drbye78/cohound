"""Тесты CLI-идентичности прогона (09 §2.4/§6.2): ``--update``, cycle_id, lineage.

Офлайн: ``--dry-run`` не пишет state, кроме явно подготовленной тестом ``state.db``;
``crawl --until plan`` не запускает сеть.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.cli.main import EXIT_BLOCKED, EXIT_OK, EXIT_USAGE, main
from core.ports.state import RunLineageRow
from core.store.state_sqlite import state_db_path
from core.store.state_store import SqliteStateStore
from tests.helpers.packages import valid_package

#: Имя каталога пакета в :func:`tests.helpers.packages.valid_package`.
PACKAGE_ID = "package"


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME``: раскладка state/runs — в tmp."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    return root


def _argv(
    package: Path, *, dry_run: bool = True, extra: tuple[str, ...] = ()
) -> list[str]:
    """``run`` без ``--run-id``: идентификатор выводится из входов."""
    argv = [
        "run",
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--json",
    ]
    if dry_run:
        argv.append("--dry-run")
    return [*argv, *extra]


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def _seed_lineage(home: Path, **overrides: Any) -> None:
    """Пишет предыдущую строку lineage в ``state.db`` пакета."""
    path = state_db_path(home / "state", PACKAGE_ID)
    path.parent.mkdir(parents=True, exist_ok=True)
    base: dict[str, Any] = {
        "run_id": "prev-run",
        "package_id": PACKAGE_ID,
        "cycle_id": "2026-06-01",
        "cohort_digest": "cohort-prev",
        "seeds_digest": "seeds-prev",
        "execution_contract_digest": "contract-prev",
        "change_class": "none",
        "status": "complete",
        "started_at": "2026-06-01T00:00:00Z",
        "completed_at": "2026-06-01T01:00:00Z",
        "products": ["p1", "p2"],
        "refresh_policy": "auto",
    }
    base.update(overrides)
    store = SqliteStateStore(path)
    try:
        store.record_run_lineage(RunLineageRow(**base))
    finally:
        store.close()


def _add_product(package: Path, product_id: str = "p3") -> None:
    """Добавляет продукт в когорту и ``run.yaml`` (согласованно, без seed'а)."""
    cohort = yaml.safe_load((package / "cohort.yaml").read_text(encoding="utf-8"))
    cohort["entries"].append(
        {
            "product_id": product_id,
            "role_overlap": 0.5,
            "selection_reason": "новый продукт (тест)",
            "anchor_relation": "analog",
        }
    )
    (package / "cohort.yaml").write_text(
        yaml.safe_dump(cohort, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    run = yaml.safe_load((package / "run.yaml").read_text(encoding="utf-8"))
    run["products"] = [*run["products"], product_id]
    (package / "run.yaml").write_text(
        yaml.safe_dump(run, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )


def test_update_requires_new_cycle_id(
    tmp_path: Path, home: Path
) -> None:
    package = valid_package(tmp_path)
    _seed_lineage(home, cycle_id="2026-06-01")

    code = main(_argv(package, extra=("--update", "--cycle-id", "2026-06-01")))

    assert code == EXIT_USAGE


def test_update_with_new_cycle_id_is_allowed(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    _seed_lineage(home, cycle_id="2026-06-01")

    code = main(_argv(package, extra=("--update", "--cycle-id", "2026-W43")))
    payload = _payload(capsys)

    assert code == EXIT_OK
    assert payload["cycle_id"] == "2026-W43"


def test_update_rejects_only_failed(tmp_path: Path, home: Path) -> None:
    package = valid_package(tmp_path)

    code = main(_argv(package, extra=("--update", "--only-failed")))

    assert code == EXIT_USAGE


def test_same_inputs_produce_same_run_id(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)

    assert main(_argv(package)) == EXIT_OK
    first = _payload(capsys)
    assert main(_argv(package)) == EXIT_OK
    second = _payload(capsys)

    assert first["run_id"] == second["run_id"]
    assert first["cycle_id"] == second["cycle_id"] == "2026-06-01"


def test_adding_product_to_cohort_changes_run_id(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Состав когорты входит в ``cohort_digest``; список products — уже нет."""
    package = valid_package(tmp_path)

    assert main(_argv(package)) == EXIT_OK
    before = _payload(capsys)

    _add_product(package)
    # Продукт без seed'а блокирует dry-run, но run_id в payload уже вычислен.
    assert main(_argv(package)) == EXIT_BLOCKED
    after = _payload(capsys)

    assert before["cohort_digest"] != after["cohort_digest"]
    assert before["run_id"] != after["run_id"]


def test_dry_run_exposes_identity_fields(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)

    assert main(_argv(package)) == EXIT_OK
    payload = _payload(capsys)

    assert payload["cycle_id"] == "2026-06-01"
    assert payload["previous_run_id"] is None
    assert payload["change_class"] == "none"
    assert payload["golden_required"] == "none"
    assert payload["run_scope"] == "full"
    assert payload["refresh_policy"] == "auto"
    assert payload["cohort_digest"] and payload["seeds_digest"]
    assert payload["execution_contract_digest"]
    modes = {plan["product_id"]: plan["mode"] for plan in payload["products"]}
    assert modes == {"p1": "new", "p2": "new"}


def test_previous_lineage_yields_cohort_expansion(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    # Текущая когорта — p1+p2; предыдущая знала только p1.
    _seed_lineage(home, products=["p1"], execution_contract_digest="old")

    assert main(_argv(package)) == EXIT_OK
    payload = _payload(capsys)

    assert payload["previous_run_id"] == "prev-run"
    assert payload["change_class"] == "cohort_expansion"
    assert payload["golden_required"] == "full"


def test_run_and_crawl_derive_same_run_id(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)

    assert main(_argv(package)) == EXIT_OK
    run_payload = _payload(capsys)

    crawl_code = main(
        [
            "crawl",
            "run",
            "--run-yaml",
            str(package / "run.yaml"),
            "--package",
            str(package),
            "--until",
            "plan",
            "--json",
        ]
    )
    crawl_payload = _payload(capsys)

    assert crawl_code == EXIT_OK
    assert crawl_payload["run_id"] == run_payload["run_id"]
