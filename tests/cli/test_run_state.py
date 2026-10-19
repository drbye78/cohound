"""Тесты durable state в ``run`` (09 §4.1/§5.3): dry-run read-only, блокировка, режимы.

Проверяют: ``--dry-run`` без ``state.db`` не создаёт ``state/``; заблокированный
онбордингом реальный прогон тоже не создаёт ``state/``; при существующем ``state.db``
dry-run показывает режим каждого продукта; реальный прогон синхронизирует seed-набор в
``state.db`` (``reconcile_seed_set``). Офлайн: tmp-каталоги, без сети.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from core.cli.main import EXIT_BLOCKED, EXIT_OK, main
from core.cli.wiring import build_store
from core.domain import LLMCellOutput, Status
from core.domain.enums import OnboardingState
from core.pipeline import Ports, build_phases
from core.pipeline.gates import gate_g1, gate_g2, gate_g3, gate_g4
from core.ports.state import ProductStateRow, SnapshotRow
from core.store.state_sqlite import state_db_path
from core.store.state_store import SqliteStateStore
from tests.fakes.crawl_probe import page
from tests.fakes.ports import (
    FakeChunker,
    FakeCrawler,
    FakeEvaluator,
    FakeExtractor,
    FakeLexicalIndex,
)
from tests.helpers.output import json_out
from tests.helpers.packages import run_argv, valid_package

#: Имя каталога пакета в :func:`tests.helpers.packages.valid_package`.
PACKAGE_ID = "package"


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME``: вся операционная раскладка — в tmp."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    return root


def _blocked_product(package: Path, product_id: str = "p3") -> None:
    """Добавляет продукт в когорту/run без seed'а."""
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


def _state_db(home: Path) -> Path:
    return state_db_path(home / "state", PACKAGE_ID)


def _product(product_id: str, **overrides: object) -> ProductStateRow:
    base: dict[str, object] = {
        "product_id": product_id,
        "canonical_name": product_id.upper(),
        "onboarding_state": OnboardingState.READY,
        "added_at": "2026-01-01T00:00:00Z",
        "added_by": "test",
    }
    base.update(overrides)
    return ProductStateRow(**base)  # type: ignore[arg-type]


def _snapshot(product_id: str, run_id: str) -> SnapshotRow:
    return SnapshotRow(
        snapshot_id=f"snap-{product_id}",
        product_id=product_id,
        cycle_id="cycle-1",
        run_id=run_id,
        status="complete",
        scope="full",
        coverage_complete=True,
        created_at="2026-01-01T00:00:00Z",
    )


def test_dry_run_without_state_creates_no_state_dir(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = valid_package(tmp_path)
    code = main(
        run_argv(
            package, run_id="dry-no-state", db=tmp_path / "cohound.db", dry_run=True
        )
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    assert payload["blocked"] == []
    assert not (home / "state").exists()
    plans = {plan["product_id"]: plan for plan in payload["products"]}
    assert plans["p1"]["mode"] == "new"


def test_blocked_real_run_creates_no_state_dir(tmp_path: Path, home: Path) -> None:
    """Блокировка онбординга — человеческая предпосылка; state не материализуется."""
    package = valid_package(tmp_path)
    _blocked_product(package)

    code = main(
        run_argv(package, run_id="blocked-no-state", db=tmp_path / "cohound.db")
    )

    assert code == EXIT_BLOCKED
    assert not (home / "state").exists()


def test_dry_run_with_state_shows_modes(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Существующий ``state.db`` — read-only вход dry-run: режимы из durable-рубежей."""
    package = valid_package(tmp_path)
    db_path = _state_db(home)
    db_path.parent.mkdir(parents=True)
    with SqliteStateStore(db_path) as state:
        state.upsert_product_state(
            _product("p1", last_corpus_run_id="run-1", last_evaluated_run_id="run-1")
        )
        state.record_snapshot(_snapshot("p1", "run-1"))
        state.upsert_product_state(_product("p2", last_corpus_run_id="run-1"))

    code = main(
        run_argv(
            package, run_id="dry-with-state", db=tmp_path / "cohound.db", dry_run=True
        )
    )
    payload = json_out(capsys)

    assert code == EXIT_OK
    plans = {plan["product_id"]: plan for plan in payload["products"]}
    assert plans["p1"]["mode"] == "incremental"
    assert plans["p1"]["refresh_policy"] == "revalidate"
    assert plans["p2"]["mode"] == "full"
    assert plans["p2"]["refresh_policy"] == "refresh"


def test_real_run_reconciles_seed_set(tmp_path: Path, home: Path) -> None:
    """Реальный (незаблокированный) прогон материализует state и синхронизирует seed'ы."""
    package = valid_package(tmp_path)

    # ``--until g1``: онбординг в области прогона; дальше гейт без решения → blocked.
    code = main(
        run_argv(
            package,
            run_id="real-state",
            db=tmp_path / "cohound.db",
            until="g1",
        )
    )

    assert code == EXIT_BLOCKED
    db_path = _state_db(home)
    assert db_path.exists()
    with SqliteStateStore(db_path) as state:
        assert [row.normalized_url for row in state.seed_rows("p1")] == [
            "https://p1.test/"
        ]
        assert [row.normalized_url for row in state.seed_rows("p2")] == [
            "https://p2.test/"
        ]
        # Готовность материализована (writer-сторона MVP-2a), но прогон остановлен на g1:
        # рубежей корпуса/оценки ещё нет — значения не выдумываются.
        row = state.get_product_state("p1")
        assert row is not None
        assert row.onboarding_state is OnboardingState.READY
        assert row.last_corpus_run_id is None
        assert row.last_evaluated_run_id is None


# ---------------------------------------------------------------------------
# Writer-сторона durable state в реальном (офлайн) прогоне
# ---------------------------------------------------------------------------


def _offline_ports() -> Ports:
    """Офлайн-прогон фикстурных seed'ов: краул/экстракция/оценка — заглушки, без сети."""
    pages = {
        url: page(
            url,
            content=b"<html><body><p>fixture documentation text</p></body></html>",
        )
        for url in ("https://p1.test/", "https://p2.test/")
    }
    return Ports(
        evaluator=FakeEvaluator(
            default=LLMCellOutput(
                status=Status.ABSENT, justification="нет в срезе", evidence=[]
            )
        ),
        lexical_index=FakeLexicalIndex(),
        crawler=FakeCrawler(pages),
        extractor=FakeExtractor(),
        chunker=FakeChunker(),
    )


def _phases_factory(**kwargs: Any) -> dict:
    """Реальные фазы с фиксированным профилем ретривала (I16/I20)."""
    kwargs["retrieval_profile"] = "R0"
    kwargs["cutoff_k"] = 3
    return build_phases(**kwargs)


def _finalize_gates(db: Path, run_id: str) -> None:
    """Предзаписывает человеческие решения гейтов (I6/I7), чтобы прогон дошёл до Ш9."""
    with build_store(db) as store:
        for gate_fn in (gate_g1, gate_g2, gate_g3, gate_g4):
            gate_fn(store, run_id, actor="cli-test")


def test_real_run_writes_product_state_snapshot_and_lineage(
    tmp_path: Path, home: Path
) -> None:
    """Полный офлайн-прогон материализует state: рубеж корпуса, снимок, lineage."""
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "state-e2e"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)

    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _finalize_gates(db, run_id)

    assert main(argv, ports=_offline_ports(), phases_factory=_phases_factory) == EXIT_OK

    db_path = _state_db(home)
    assert db_path.exists()
    with SqliteStateStore(db_path) as state:
        row = state.get_product_state("p1")
        assert row is not None
        assert row.onboarding_state is OnboardingState.READY
        assert row.last_corpus_run_id == run_id
        assert row.last_attempt_run_id == run_id
        snapshot = state.last_snapshot("p1", scope="full")
        assert snapshot is not None
        assert snapshot.run_id == run_id
        assert snapshot.status == "complete"
        assert snapshot.coverage_complete is True
        assert snapshot.coverage_digest
        lineage = state.conn.execute(
            "SELECT * FROM run_lineage WHERE run_id = ?", (run_id,)
        ).fetchone()
        assert lineage is not None
        assert lineage["package_id"] == PACKAGE_ID
        assert lineage["status"] == "complete"
        assert lineage["completed_at"]


def test_second_run_keeps_corpus_milestone(tmp_path: Path, home: Path) -> None:
    """Повторный прогон тех же входов не теряет ``last_corpus_run_id`` (идемпотентность)."""
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "state-twice"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)

    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _finalize_gates(db, run_id)
    assert main(argv, ports=_offline_ports(), phases_factory=_phases_factory) == EXIT_OK
    assert main(argv, ports=_offline_ports(), phases_factory=_phases_factory) == EXIT_OK

    with SqliteStateStore(_state_db(home)) as state:
        row = state.get_product_state("p1")
        assert row is not None
        assert row.last_corpus_run_id == run_id
        assert row.last_evaluated_run_id == run_id
        # Resume того же run_id не пишет вторую строку lineage.
        assert (
            state.conn.execute(
                "SELECT COUNT(*) AS n FROM run_lineage WHERE run_id = ?", (run_id,)
            ).fetchone()["n"]
            == 1
        )
