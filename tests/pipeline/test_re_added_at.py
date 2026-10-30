"""Тесты перехода ``retired → активен``: ``product_state.re_added_at`` (09 §5.1–§5.3, T15).

Метка ``re_added_at`` объявлена в DDL (``core/store/state_sqlite.py``), в порте
(``core/ports/state.py``) и читается ``_mode_for`` (``core/pipeline/onboarding.py``), где
даёт режим ``new`` — полный краул, потому что за время отсутствия состояние устарело.
Писателя у неё не было: ветка режима была недостижима, а сценарий T15 (§15.1) —
design-only.

Писатель — сверка seed-набора (:meth:`ProductStatePort.reconcile_seed_set`): retirement и
возврат продукта — решения человека в данных ``sources/seeds.yaml``, поэтому переход
наблюдается при сверке файла с durable-состоянием, а не отдельной командой.

Тесты идут **через** ``reconcile_seed_set``: метка не выставляется руками, иначе тест
доказывал бы только чтение поля (как ``test_readded_after_retired_is_new`` в
``test_onboarding_modes.py``), а не достижимость ветки.

Офлайн: ``:memory:``-state, без сети.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain.enums import (
    OnboardingState,
    ProductMode,
    RefreshPolicy,
    SeedRole,
    SeedSource,
    SeedState,
)
from core.domain.models import ProductSeeds, SeedEntry, SeedsConfig
from core.pipeline.onboarding import _mode_for, resolve_product_modes
from core.ports.state import ProductStateRow, RunLineageRow, SnapshotRow
from core.store.state_store import SqliteStateStore

PROFILES = {"p1.test": {}}


@pytest.fixture()
def state() -> Iterator[SqliteStateStore]:
    with SqliteStateStore(":memory:") as store:
        yield store


@pytest.fixture()
def clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Фиксированные часы сверки: метки времени детерминированы, порядок — проверяем.

    ``_now`` — единственный источник ``retired_at``/``re_added_at`` в хранилище, поэтому
    подмена модульной функции делает переход воспроизводимым без «почти равных» сравнений.
    """
    monkeypatch.setattr(
        "core.store.state_store._now", lambda: "2026-03-01T00:00:00Z"
    )


def _seed(
    url: str = "https://p1.test/",
    *,
    enabled: bool = True,
    state_: SeedState = SeedState.ENABLED,
) -> SeedEntry:
    return SeedEntry(
        url=url,
        normalized_url=url,
        role=SeedRole.PRIMARY,
        source=SeedSource.OPERATOR,
        profile_key="p1.test",
        added_at=_dt.date(2026, 1, 1),
        added_by="operator",
        enabled=enabled,
        state=state_,
    )


def _product(product_id: str = "p1", **overrides: object) -> ProductStateRow:
    base: dict[str, object] = {
        "product_id": product_id,
        "canonical_name": "Product One",
        "onboarding_state": OnboardingState.READY,
        "added_at": "2026-01-01T00:00:00Z",
        "added_by": "test",
    }
    base.update(overrides)
    return ProductStateRow(**base)  # type: ignore[arg-type]


def _snapshot(
    product_id: str = "p1", *, run_id: str = "run-1", status: str = "complete"
) -> SnapshotRow:
    return SnapshotRow(
        snapshot_id=f"snap-{product_id}-{run_id}",
        product_id=product_id,
        cycle_id="cycle-1",
        run_id=run_id,
        status=status,
        scope="full",
        coverage_complete=status == "complete",
        created_at="2026-01-02T00:00:00Z",
    )


def _lineage(run_id: str, *, completed_at: str | None) -> RunLineageRow:
    return RunLineageRow(
        run_id=run_id,
        package_id="case_x",
        cycle_id="cycle-2",
        cohort_digest="cohort",
        seeds_digest="seeds",
        execution_contract_digest="contract",
        change_class="initial",
        status="complete" if completed_at else "running",
        started_at="2026-02-01T00:00:00Z",
        completed_at=completed_at,
    )


def _mode(state: SqliteStateStore, product_id: str = "p1") -> ProductMode:
    return _mode_for(state, product_id, state.get_product_state(product_id))


def _measured_product(state: SqliteStateStore, product_id: str = "p1") -> None:
    """Продукт с полным baseline: corpus + eval + ``complete``-снимок ⇒ ``incremental``."""
    state.upsert_product_state(
        _product(
            product_id, last_corpus_run_id="run-1", last_evaluated_run_id="run-1"
        )
    )
    state.record_snapshot(_snapshot(product_id, run_id="run-1"))


# --------------------------------------------------------------------------- #
# Переход: retired → активен
# --------------------------------------------------------------------------- #


def test_return_after_retirement_sets_re_added_at(
    state: SqliteStateStore, clock: None
) -> None:
    """Возврат после ``retired`` достижим сверкой seed-набора и даёт режим ``new``.

    Без писателя метки (дефект «объявлено, но не написано») ветка ``_mode_for`` не
    срабатывает: продукт с полным baseline остался бы ``incremental``.
    """
    _measured_product(state)
    state.reconcile_seed_set("p1", [_seed()])
    assert _mode(state) is ProductMode.INCREMENTAL

    state.reconcile_seed_set("p1", [])  # продукт выведен из работы (нет активных seed'ов)
    retired = state.get_product_state("p1")
    assert retired is not None
    assert retired.onboarding_state is OnboardingState.RETIRED
    assert retired.retired_at is not None

    state.reconcile_seed_set("p1", [_seed()])  # вернули
    returned = state.get_product_state("p1")
    assert returned is not None
    assert returned.re_added_at == "2026-03-01T00:00:00Z"
    assert returned.retired_at is None
    assert returned.onboarding_state is OnboardingState.READY
    # Смысл метки: полный краул, а не инкремент от устаревшего baseline (§5.1).
    assert _mode(state) is ProductMode.NEW


def test_re_added_at_is_visible_in_plan(
    state: SqliteStateStore, clock: None
) -> None:
    """Метка читается планом продукта: ``mode=new`` + ``refresh_policy=refresh`` (§5.3)."""
    _measured_product(state)
    state.reconcile_seed_set("p1", [_seed()])
    state.reconcile_seed_set("p1", [])
    state.reconcile_seed_set("p1", [_seed()])

    config = SeedsConfig(
        products={"p1": ProductSeeds(state=OnboardingState.READY, seeds=[_seed()])}
    )
    plan = resolve_product_modes(
        ["p1"], config, profiles=PROFILES, state=state
    ).plan_set.plan_for("p1")

    assert plan is not None
    assert plan.mode is ProductMode.NEW
    assert plan.refresh_policy is RefreshPolicy.REFRESH
    assert plan.reasons == []


def test_product_level_retirement_is_evidence(
    state: SqliteStateStore, clock: None
) -> None:
    """Продукт, помеченный ``retired`` в ``product_state``, — тоже признак (§5.1).

    Строка с ``onboarding_state='retired'``/``retired_at`` — durable-решение о продукте
    (сейчас её пишет только сверка seed-набора, но поле объявлено и читается гейтом
    режима); возврат активного seed'а обязан дать ``re_added_at`` и здесь.
    """
    _measured_product(
        state,
    )
    state.upsert_product_state(
        dataclasses.replace(
            state.get_product_state("p1"),  # type: ignore[arg-type]
            onboarding_state=OnboardingState.RETIRED,
            retired_at="2026-02-01T00:00:00Z",
        )
    )
    state.reconcile_seed_set("p1", [_seed()])

    row = state.get_product_state("p1")
    assert row is not None
    assert row.re_added_at == "2026-03-01T00:00:00Z"
    assert row.retired_at is None
    assert _mode(state) is ProductMode.NEW


def test_seed_disabled_by_author_is_not_retirement(
    state: SqliteStateStore, clock: None
) -> None:
    """Выключенный автором seed (``retired_at IS NULL``) — не retirement продукта."""
    _measured_product(state)
    state.reconcile_seed_set(
        "p1", [_seed(enabled=False, state_=SeedState.DISABLED)]
    )
    state.reconcile_seed_set("p1", [_seed()])

    row = state.get_product_state("p1")
    assert row is not None
    assert row.re_added_at is None
    assert _mode(state) is ProductMode.INCREMENTAL


def test_repeated_reconcile_does_not_invent_re_add(
    state: SqliteStateStore, clock: None
) -> None:
    """Обычная сверка (retirement не наблюдался) метку не ставит и не двигает."""
    _measured_product(state)
    state.reconcile_seed_set("p1", [_seed()])
    state.reconcile_seed_set("p1", [_seed()])

    row = state.get_product_state("p1")
    assert row is not None
    assert row.re_added_at is None
    assert _mode(state) is ProductMode.INCREMENTAL


# --------------------------------------------------------------------------- #
# Снятие метки: иначе продукт остался бы ``new`` навсегда
# --------------------------------------------------------------------------- #


def test_re_added_at_cleared_after_corpus_run(
    state: SqliteStateStore, clock: None
) -> None:
    """Прогон, собравший корпус после возврата, снимает метку (``last_corpus_run_id``).

    Это рабочий случай живой когорты: рубеж «все ячейки разрешены»
    (``last_completed_run_id``) там не достигается — ``pending``-ячейки уходят на ревью, —
    и метка, привязанная только к нему, осталась бы навсегда.
    """
    _measured_product(state)
    state.reconcile_seed_set("p1", [_seed()])
    state.reconcile_seed_set("p1", [])
    state.reconcile_seed_set("p1", [_seed()])
    returned = state.get_product_state("p1")
    assert returned is not None and returned.re_added_at is not None

    state.record_run_lineage(_lineage("run-2", completed_at="2026-04-01T00:00:00Z"))
    state.upsert_product_state(
        dataclasses.replace(returned, last_corpus_run_id="run-2")
    )
    # Снимок прогона после возврата — каким его пишет ``record_run_outcome`` (неполный:
    # eval ещё не состоялся). Дальше режим решает обычное правило §5.1, а не метка.
    state.record_snapshot(_snapshot(run_id="run-2", status="partial"))

    state.reconcile_seed_set("p1", [_seed()])

    after = state.get_product_state("p1")
    assert after is not None
    assert after.re_added_at is None
    assert _mode(state) is ProductMode.FULL


def test_re_added_at_cleared_after_completed_run(
    state: SqliteStateStore, clock: None
) -> None:
    """Завершённое измерение после возврата снимает метку: baseline снова свежий."""
    _measured_product(state)
    state.reconcile_seed_set("p1", [_seed()])
    state.reconcile_seed_set("p1", [])
    state.reconcile_seed_set("p1", [_seed()])
    returned = state.get_product_state("p1")
    assert returned is not None and returned.re_added_at is not None

    # Полный краул после возврата завершён: рубеж ``last_completed`` — новее метки.
    state.record_run_lineage(_lineage("run-2", completed_at="2026-04-01T00:00:00Z"))
    state.upsert_product_state(
        dataclasses.replace(returned, last_completed_run_id="run-2")
    )
    state.record_snapshot(_snapshot(run_id="run-2"))

    state.reconcile_seed_set("p1", [_seed()])

    after = state.get_product_state("p1")
    assert after is not None
    assert after.re_added_at is None
    assert _mode(state) is ProductMode.INCREMENTAL


def test_re_added_at_kept_when_run_not_completed(
    state: SqliteStateStore, clock: None
) -> None:
    """Незавершённый (или неизвестный) прогон метку не снимает — baseline ещё устарел."""
    _measured_product(state)
    state.reconcile_seed_set("p1", [_seed()])
    state.reconcile_seed_set("p1", [])
    state.reconcile_seed_set("p1", [_seed()])
    returned = state.get_product_state("p1")
    assert returned is not None and returned.re_added_at is not None

    state.record_run_lineage(_lineage("run-2", completed_at=None))
    state.upsert_product_state(
        dataclasses.replace(returned, last_completed_run_id="run-2")
    )
    state.reconcile_seed_set("p1", [_seed()])
    assert state.get_product_state("p1").re_added_at is not None  # type: ignore[union-attr]

    # Рубеж называет прогон, которого нет в lineage: «неизвестно» ≠ «завершён».
    state.upsert_product_state(
        dataclasses.replace(returned, last_completed_run_id="run-unknown")
    )
    state.reconcile_seed_set("p1", [_seed()])
    assert state.get_product_state("p1").re_added_at is not None  # type: ignore[union-attr]


# --------------------------------------------------------------------------- #
# Retirement как durable факт
# --------------------------------------------------------------------------- #


def test_empty_seed_set_marks_product_retired(
    state: SqliteStateStore, clock: None
) -> None:
    """Пустой набор пишет retirement в ``product_state``, а не только в ``seed_set``."""
    _measured_product(state)
    state.reconcile_seed_set("p1", [_seed()])
    state.reconcile_seed_set("p1", [])

    row = state.get_product_state("p1")
    assert row is not None
    assert row.onboarding_state is OnboardingState.RETIRED
    assert row.retired_at == "2026-03-01T00:00:00Z"
    assert row.onboarding_reason == "no_seeds"
    (seed_row,) = state.seed_rows("p1")
    assert seed_row.state is SeedState.DISABLED
    assert seed_row.retired_at is not None


def test_empty_seed_set_does_not_create_product_row(
    state: SqliteStateStore, clock: None
) -> None:
    """Строки продукта нет — retirement не выдумывается (создание — забота прогона)."""
    state.reconcile_seed_set("p1", [])

    assert state.get_product_state("p1") is None
    assert state.seed_rows("p1") == []
