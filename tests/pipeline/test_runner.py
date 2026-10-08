"""Тесты оркестратора конвейера (W4/T4.1): порядок, состояние, resume, изоляция, гейты.

Работают на ``SqliteStore(":memory:")``; фазы — детерминированные заглушки, реальные порты
не вызываются. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import Focus, RunConfig, RunMode
from core.pipeline import PHASES, PhaseResult, Ports, Runner, run_isolated
from core.pipeline.gates import gate_g1
from core.store.sqlite_store import SqliteStore
from tests.fakes import (
    FakeChunker,
    FakeCrawler,
    FakeEvaluator,
    FakeExtractor,
    FakeRegistryFetcher,
    FakeSearchAdapter,
)

RUN_ID = "run-1"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        yield s


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=["param.a"]),
        languages=["ru", "en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


def _recorder(calls: list[str]):
    """Фабрика фаз-регистраторов: фиксирует имя фазы в общем списке и возвращает ``ok``."""

    def _fn(ctx) -> PhaseResult:
        calls.append(ctx.run_id)  # sanity: контекст передан
        return PhaseResult(status="ok", phase="?")

    return _fn


def test_phases_run_in_order(store: SqliteStore) -> None:
    order: list[str] = []

    def make(phase: str):
        def _fn(ctx) -> PhaseResult:
            order.append(phase)
            return PhaseResult(status="ok", phase=phase)

        return _fn

    phases = {name: make(name) for name in PHASES}
    runner = Runner(store, phases=phases)
    report = runner.run(RUN_ID)

    assert report.ok
    assert order == list(PHASES)
    assert report.stopped_at is None
    assert [r.phase for r in report.results] == list(PHASES)


def test_phase_state_persisted_to_store(store: SqliteStore) -> None:
    runner = Runner(store, phases={name: _recorder([]) for name in PHASES})
    runner.run(RUN_ID, until="plan")
    # Durable-карта фаз хранится JSON-объектом в runs.state (несколько фаз аккумулируются).
    assert store.get_phase_state(RUN_ID) == {"normalize": "ok", "lint": "ok", "plan": "ok"}
    assert runner.state_of(RUN_ID, "plan") == "ok"


def test_run_stops_at_gate_without_decision(store: SqliteStore) -> None:
    runner = Runner(store)  # дефолтный реестр: G1 без решения → blocked
    report = runner.run(RUN_ID)

    assert not report.ok
    assert report.stopped_at == "g1"
    assert report.status == "blocked"
    assert [r.phase for r in report.results if r.status == "blocked"] == ["g1"]
    # discovery не выполнялась
    assert runner.state_of(RUN_ID, "discovery") is None


def test_resume_skips_completed_phases(store: SqliteStore) -> None:
    calls: list[str] = []

    def make(phase: str):
        def _fn(ctx) -> PhaseResult:
            calls.append(phase)
            return PhaseResult(status="ok", phase=phase)

        return _fn

    phases = {name: make(name) for name in PHASES}
    runner = Runner(store, phases=phases)
    first = runner.run(RUN_ID)
    assert first.ok
    assert calls == list(PHASES)

    second = runner.resume(RUN_ID)
    assert second.ok
    assert second.skipped == list(PHASES)
    assert second.results == []
    # повторного вызова фаз не было
    assert calls == list(PHASES)


def test_interrupted_run_resumes_without_losing_progress(store: SqliteStore) -> None:
    calls: list[str] = []
    fail_once = {"corpus": True}

    def make(phase: str):
        def _fn(ctx) -> PhaseResult:
            calls.append(phase)
            if phase == "corpus" and fail_once["corpus"]:
                fail_once["corpus"] = False
                raise RuntimeError("временный сбой корпуса")
            return PhaseResult(status="ok", phase=phase)

        return _fn

    runner = Runner(store, phases={name: make(name) for name in PHASES})
    first = runner.run(RUN_ID)
    assert first.status == "failed"
    assert first.stopped_at == "corpus"
    assert runner.state_of(RUN_ID, "normalize") == "ok"
    assert runner.state_of(RUN_ID, "corpus") == "failed"

    calls.clear()
    second = runner.resume(RUN_ID)
    assert second.ok
    # завершённые фазы не перезапускались; corpus и последующие — да
    assert calls[0] == "corpus"
    assert "normalize" not in calls
    assert second.skipped == list(PHASES[: PHASES.index("corpus")])


def test_gate_passes_after_decision_and_run_continues(store: SqliteStore) -> None:
    runner = Runner(store)
    assert runner.run(RUN_ID, until="g1").status == "blocked"

    gate_g1(store, RUN_ID, actor="human-1", notes="cohort accepted")
    report = runner.resume(RUN_ID, until="g1")
    assert report.ok
    assert runner.state_of(RUN_ID, "g1") == "ok"


def test_gate_error_isolated_not_aborting(store: SqliteStore) -> None:
    def noisy(ctx) -> PhaseResult:
        return PhaseResult(status="ok", phase="evaluate", errors=["p2: сбой одного продукта"])

    runner = Runner(store, phases={"evaluate": noisy})
    # default g1..g4 blocked; чтобы дойти до evaluate, подменим гейты на ok-заглушки
    for gate_phase in ("g1", "g2", "g3", "g4"):
        runner.register(gate_phase, _recorder([]))
    report = runner.run(RUN_ID)
    assert report.ok
    assert report.errors == ["p2: сбой одного продукта"]


def test_run_isolated_collects_errors() -> None:
    def fn(item: int) -> int:
        if item == 2:
            raise ValueError("bad item")
        return item * 2

    outputs, errors = run_isolated([1, 2, 3], fn)
    assert outputs == [2, 6]
    assert len(errors) == 1 and "bad item" in errors[0]


def test_until_must_be_known_phase(store: SqliteStore) -> None:
    with pytest.raises(ValueError):
        Runner(store).run(RUN_ID, until="nope")


def test_register_rejects_unknown_phase(store: SqliteStore) -> None:
    with pytest.raises(ValueError):
        Runner(store).register("nope", _recorder([]))


def test_fakes_satisfy_ports_and_evaluator_has_no_tools() -> None:
    from core.ports.chunker import ChunkerPort
    from core.ports.crawler import CrawlerPort
    from core.ports.extractor import ExtractorPort
    from core.ports.llm import EvaluatorPort
    from core.ports.registry import RegistryFetcherPort
    from core.ports.search import SearchAdapterPort

    assert isinstance(FakeEvaluator(), EvaluatorPort)
    assert isinstance(FakeSearchAdapter(), SearchAdapterPort)
    assert isinstance(FakeCrawler(), CrawlerPort)
    assert isinstance(FakeExtractor(), ExtractorPort)
    assert isinstance(FakeChunker(), ChunkerPort)
    assert isinstance(FakeRegistryFetcher(), RegistryFetcherPort)
    # I18: у порта оценщика нет веб-тула/инструментов
    evaluator = FakeEvaluator()
    assert not hasattr(evaluator, "tools")
    assert not hasattr(evaluator, "web_search")


def test_ports_container_is_optional() -> None:
    ports = Ports()
    assert all(
        value is None
        for value in (
            ports.evaluator,
            ports.search,
            ports.crawler,
            ports.extractor,
            ports.chunker,
            ports.registry,
        )
    )


# --------------------------------------------------------------------------- #
# durable (cross-process) resume через runs.state (02 §A.1)
# --------------------------------------------------------------------------- #


def _recording_phases(calls: list[str]) -> dict:
    """Реестр фаз-регистраторов, пишущих имя фазы в ``calls`` и возвращающих ``ok``."""

    def make(phase: str):
        def _fn(ctx) -> PhaseResult:
            calls.append(phase)
            return PhaseResult(status="ok", phase=phase)

        return _fn

    return {name: make(name) for name in PHASES}


def test_durable_resume_across_runner_instances(store: SqliteStore) -> None:
    calls_a: list[str] = []
    runner_a = Runner(store, phases=_recording_phases(calls_a))
    first = runner_a.run(RUN_ID, until="plan")
    assert first.ok
    assert calls_a == ["normalize", "lint", "plan"]
    # durable-карта фаз записана в runs.state
    assert store.get_phase_state(RUN_ID) == {
        "normalize": "ok",
        "lint": "ok",
        "plan": "ok",
    }

    # Новый инстанс (эмуляция нового процесса) над той же БД: завершённые фазы пропущены.
    calls_b: list[str] = []
    runner_b = Runner(store, phases=_recording_phases(calls_b))
    second = runner_b.run(RUN_ID, until="plan")
    assert second.ok
    assert calls_b == []
    assert second.results == []
    assert second.skipped == ["normalize", "lint", "plan"]


def test_durable_gate_blocked_state_resumes_across_instances(store: SqliteStore) -> None:
    runner_a = Runner(store)
    first = runner_a.run(RUN_ID, until="g1")
    assert first.status == "blocked"
    assert store.get_phase_state(RUN_ID) == {
        "normalize": "ok",
        "lint": "ok",
        "plan": "ok",
        "g1": "blocked",
    }

    # Новый инстанс видит blocked: ранние ok-фазы пропускаются, гейт перезапускается.
    runner_b = Runner(store)
    second = runner_b.run(RUN_ID, until="g1")
    assert second.status == "blocked"
    assert second.skipped == ["normalize", "lint", "plan"]
    assert [r.phase for r in second.results] == ["g1"]

    # Человеческое решение в gate_decisions → новый инстанс продолжает с гейта.
    gate_g1(store, RUN_ID, actor="human-1", notes="accepted")
    runner_c = Runner(store)
    third = runner_c.run(RUN_ID, until="g1")
    assert third.ok
    assert runner_c.state_of(RUN_ID, "g1") == "ok"
    assert store.get_phase_state(RUN_ID)["g1"] == "ok"


def test_reset_forces_full_restart_ignoring_durable_state(store: SqliteStore) -> None:
    calls: list[str] = []
    runner = Runner(store, phases=_recording_phases(calls))
    runner.run(RUN_ID, until="plan")
    assert calls == ["normalize", "lint", "plan"]

    runner.reset(RUN_ID)
    calls.clear()
    report = runner.run(RUN_ID, until="plan")
    assert report.ok
    # после reset durable-карта не разогревается — фазы выполняются заново
    assert calls == ["normalize", "lint", "plan"]


class _StoreWithoutGetter:
    """Минимальная заглушка store без ``get_phase_state`` (обратная совместимость)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def set_phase_state(self, run_id: str, phase: str, state: str) -> None:
        self.calls.append((run_id, phase, state))


def test_runner_tolerates_store_without_get_phase_state() -> None:
    stub = _StoreWithoutGetter()
    runner = Runner(stub)  # type: ignore[arg-type]
    report = runner.run(RUN_ID, until="plan")
    assert report.ok
    assert [phase for _, phase, _ in stub.calls] == ["normalize", "lint", "plan"]
