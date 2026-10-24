"""Свидетельство живости сеанса краула (F1/Д-6): ``owner``, heartbeat, recovery-on-open.

Проверяется ровно то, что отличает мёртвый сеанс от медленного: протухший ``running``
становится ``interrupted``, живой (живой pid на этом хосте или свежий heartbeat) не трогается,
повторный recovery идемпотентен, и незнание (чужой хост/нет метки) не выдаётся за смерть.
Нейтральные фикстуры (``p1``/``run-fixture``), сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
import os
import sqlite3
from pathlib import Path

import pytest

from core.domain import (
    CrawlSession,
    CrawlSessionStatus,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline.liveness import (
    HEARTBEAT_INTERVAL_S,
    SESSION_STALE_AFTER_S,
    Heartbeat,
    current_owner,
    interrupt_session,
    owner_is_alive,
    recover_interrupted_sessions,
    stale_reason,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-fixture"
SESSION_ID = "crawl:run-fixture"
OTHER_SESSION_ID = "crawl:other"
NOW = _dt.datetime(2026, 6, 1, 12, 0, 0, tzinfo=_dt.timezone.utc)


def _store() -> SqliteStore:
    """``SqliteStore`` с созданной строкой прогона (нужна для FK сеанса)."""
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


def _session(
    session_id: str = SESSION_ID,
    *,
    status: CrawlSessionStatus = CrawlSessionStatus.RUNNING,
    owner: str | None = None,
    heartbeat_at: str | None = None,
) -> CrawlSession:
    return CrawlSession(
        session_id=session_id,
        run_id=RUN_ID,
        profile="local",
        status=status,
        created_at="2026-01-01T00:00:00+00:00",
        owner=owner,
        heartbeat_at=heartbeat_at,
    )


def _iso(moment: _dt.datetime) -> str:
    return moment.isoformat()


# --------------------------------------------------------------------------- #
# stale_reason: порядок сигналов
# --------------------------------------------------------------------------- #


def test_live_owner_wins_over_old_heartbeat() -> None:
    """Живой pid на этом хосте сильнее heartbeat: медленный, но живой сеанс не трогается."""
    session = _session(owner="this-host/42/777", heartbeat_at="2020-01-01T00:00:00+00:00")
    assert stale_reason(session, now=NOW, owner_alive=lambda _owner: True) is None


def test_dead_owner_is_interrupted_even_with_fresh_heartbeat() -> None:
    """Мёртвый владелец на этом хосте — точный ответ, свежий heartbeat его не перекрывает."""
    session = _session(owner="this-host/42/777", heartbeat_at=_iso(NOW))
    reason = stale_reason(session, now=NOW, owner_alive=lambda _owner: False)
    assert reason is not None and reason.startswith("owner_dead")


def test_unknown_owner_falls_back_to_heartbeat_threshold() -> None:
    """Чужой хост/нет метки → ответ ``None``; тогда решает возраст heartbeat."""
    fresh = _session(owner="other-host/1/?", heartbeat_at=_iso(NOW))
    assert stale_reason(fresh, now=NOW, owner_alive=lambda _owner: None) is None

    old = _session(
        owner="other-host/1/?",
        heartbeat_at=_iso(NOW - _dt.timedelta(seconds=SESSION_STALE_AFTER_S + 1)),
    )
    reason = stale_reason(old, now=NOW, owner_alive=lambda _owner: None)
    assert reason is not None and reason.startswith("stale_heartbeat")


def test_missing_heartbeat_is_not_evidence_of_life() -> None:
    """``running`` без heartbeat и без владельца — свидетельств жизни нет (``no_heartbeat``)."""
    reason = stale_reason(_session(), now=NOW, owner_alive=lambda _owner: None)
    assert reason is not None and reason.startswith("no_heartbeat")


def test_unparsable_heartbeat_is_named_not_guessed() -> None:
    reason = stale_reason(
        _session(heartbeat_at="not-a-date"), now=NOW, owner_alive=lambda _owner: None
    )
    assert reason is not None and reason.startswith("bad_heartbeat")


def test_terminal_session_is_never_stale() -> None:
    """Протухание — про ``running``; завершённый сеанс recovery не интересует."""
    for status in (
        CrawlSessionStatus.COMPLETED,
        CrawlSessionStatus.PARTIAL,
        CrawlSessionStatus.CANCELLED,
        CrawlSessionStatus.FAILED,
        CrawlSessionStatus.INTERRUPTED,
        CrawlSessionStatus.CREATED,
        CrawlSessionStatus.PAUSED,
    ):
        assert stale_reason(_session(status=status), now=NOW) is None


# --------------------------------------------------------------------------- #
# recovery-on-open
# --------------------------------------------------------------------------- #


def test_recovery_marks_stale_running_as_interrupted() -> None:
    with _store() as store:
        store.upsert_crawl_session(
            _session(owner="other-host/1/?", heartbeat_at="2020-01-01T00:00:00+00:00")
        )
        recovered = recover_interrupted_sessions(store, now=NOW)

        assert recovered == [SESSION_ID]
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.INTERRUPTED
        # Причина названа, а провенанса нет: финализации не было, выдумывать нечего.
        assert session.error is not None and session.error.startswith("stale_heartbeat")
        assert session.finished_at == _iso(NOW)
        assert session.provenance_json is None


def test_recovery_is_idempotent() -> None:
    with _store() as store:
        store.upsert_crawl_session(_session(heartbeat_at="2020-01-01T00:00:00+00:00"))
        assert recover_interrupted_sessions(store, now=NOW) == [SESSION_ID]
        first = store.get_crawl_session(SESSION_ID)

        # Второй проход не находит ``running`` и ничего не переписывает.
        assert recover_interrupted_sessions(store, now=NOW) == []
        second = store.get_crawl_session(SESSION_ID)
        assert first is not None and second is not None
        assert (second.status, second.error) == (first.status, first.error)


def test_recovery_leaves_live_session_alone() -> None:
    """Живой владелец на этом хосте — сеанс не трогается, даже если heartbeat старый."""
    with _store() as store:
        store.upsert_crawl_session(
            _session(owner="this-host/42/777", heartbeat_at="2020-01-01T00:00:00+00:00")
        )
        assert (
            recover_interrupted_sessions(
                store, now=NOW, owner_alive=lambda _owner: True
            )
            == []
        )
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.RUNNING


def test_recovery_of_real_dead_process_on_this_host() -> None:
    """Реальный (а не подставной) мёртвый владелец этого хоста распознаётся сразу."""
    dead_pid_owner = current_owner(pid=999_999)  # такого pid нет
    assert owner_is_alive(dead_pid_owner) is False

    with _store() as store:
        store.upsert_crawl_session(
            _session(owner=dead_pid_owner, heartbeat_at=_iso(NOW))
        )
        assert recover_interrupted_sessions(store, now=NOW) == [SESSION_ID]
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.INTERRUPTED
        assert session.error is not None and session.error.startswith("owner_dead")


def test_recovery_covers_other_sessions_in_the_same_db() -> None:
    """Recovery ищет протухшие ``running`` по всей БД, а не только открываемый сеанс."""
    with _store() as store:
        store.upsert_crawl_session(
            _session(OTHER_SESSION_ID, heartbeat_at="2020-01-01T00:00:00+00:00")
        )
        store.upsert_crawl_session(
            _session(SESSION_ID, heartbeat_at=_iso(NOW), owner=current_owner())
        )
        assert recover_interrupted_sessions(store, now=NOW) == [OTHER_SESSION_ID]
        alive = store.get_crawl_session(SESSION_ID)
        assert alive is not None and alive.status is CrawlSessionStatus.RUNNING


def test_recovery_does_not_touch_items_or_counters() -> None:
    """Recovery меняет только статус сеанса: элементы — дело ``recover_crawl_session``."""
    with _store() as store:
        store.upsert_crawl_session(_session(heartbeat_at="2020-01-01T00:00:00+00:00"))
        recover_interrupted_sessions(store, now=NOW)
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert (session.total, session.pending, session.running) == (0, 0, 0)


def test_interrupt_session_names_reason() -> None:
    with _store() as store:
        store.upsert_crawl_session(_session())
        interrupt_session(store, SESSION_ID, reason="cancelled_by_operator: test", at=_iso(NOW))
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.INTERRUPTED
        assert session.error == "cancelled_by_operator: test"
        assert session.provenance_json is None


# --------------------------------------------------------------------------- #
# owner / pid-liveness
# --------------------------------------------------------------------------- #


def test_current_owner_is_alive_for_own_process() -> None:
    owner = current_owner()
    assert owner_is_alive(owner) is True


def test_owner_of_foreign_host_is_unknown() -> None:
    """Процессы другой ноды локально не видны: незнание не выдаётся за смерть."""
    assert owner_is_alive("other-host/1/2") is None
    assert owner_is_alive(None) is None
    assert owner_is_alive("") is None
    assert owner_is_alive("no-separators") is None
    assert owner_is_alive("this-host/not-a-pid/0") is None


def test_pid_reuse_is_detected(tmp_path: Path) -> None:
    """Совпал номер pid, но не момент старта ⇒ «тот» процесс мёртв (защита от переиспользования).

    ``/proc`` подменяется каталогом-двойником, а pid берётся настоящий (живой) — иначе
    ``os.kill`` ответил бы «процесса нет», и проверка старта не выполнялась бы вовсе.
    """
    pid = os.getpid()
    proc = tmp_path / str(pid)
    proc.mkdir()
    (proc / "stat").write_text(
        f"{pid} (python) S {' '.join(['0'] * 18)} 4242 {' '.join(['0'] * 10)}\n",
        encoding="utf-8",
    )
    host = "this-host"
    # Момент старта в метке совпадает с /proc ⇒ живой.
    assert owner_is_alive(f"{host}/{pid}/4242", hostname=host, proc_root=tmp_path) is True
    # Другой старт при том же pid ⇒ переиспользованный номер.
    assert owner_is_alive(f"{host}/{pid}/1", hostname=host, proc_root=tmp_path) is False
    # Отсутствующий процесс.
    assert owner_is_alive(f"{host}/999999/4242", hostname=host, proc_root=tmp_path) is False


# --------------------------------------------------------------------------- #
# Heartbeat
# --------------------------------------------------------------------------- #


def test_heartbeat_is_disabled_without_session() -> None:
    assert Heartbeat(None, None).enabled is False
    assert Heartbeat(None, None).touch() is False


def test_heartbeat_throttles_to_interval_then_writes() -> None:
    """Троттлинг: признак жизни не стоит записи на каждый URL, но и не теряется."""
    ticks = iter([0.0, 1.0, HEARTBEAT_INTERVAL_S + 2.0, HEARTBEAT_INTERVAL_S + 3.0])
    stamps = iter(["t0", "t1", "t2", "t3"])
    with _store() as store:
        store.upsert_crawl_session(_session(owner="other-host/1/?"))
        heartbeat = Heartbeat(
            store,
            SESSION_ID,
            owner="this-host/42/777",
            clock=lambda: next(ticks),
            stamp=lambda: next(stamps),
        )
        assert heartbeat.touch() is True  # первый — всегда
        assert heartbeat.touch() is False  # внутри интервала — нет записи
        assert heartbeat.touch() is True  # интервал прошёл
        assert heartbeat.touch() is False

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.heartbeat_at == "t1"
        assert session.owner == "this-host/42/777"


def test_heartbeat_force_and_owner_override() -> None:
    """``force`` пишет вне интервала; владелец — тот, кем объект создан."""
    with _store() as store:
        store.upsert_crawl_session(_session(owner="this-host/42/777"))
        heartbeat = Heartbeat(store, SESSION_ID, interval_s=3600.0, stamp=lambda: "t9")
        assert heartbeat.touch() is True
        assert heartbeat.touch() is False
        assert heartbeat.touch(force=True) is True

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.heartbeat_at == "t9"
        # Heartbeat всегда объявляет своего владельца — им становится текущий процесс.
        assert session.owner == current_owner()


def test_store_touch_without_owner_keeps_previous_owner() -> None:
    """``owner=None`` на уровне стора не затирает владельца: смена объявляется явно."""
    with _store() as store:
        store.upsert_crawl_session(_session(owner="this-host/42/777"))
        store.touch_crawl_session_heartbeat(SESSION_ID, at="t1")
        store.touch_crawl_session_heartbeat(SESSION_ID, at="t2", owner="other-host/9/1")
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert (session.owner, session.heartbeat_at) == ("other-host/9/1", "t2")


def test_heartbeat_does_not_change_status_or_counters() -> None:
    """Признак жизни — не изменение измерения: статус и счётчики не двигаются."""
    with _store() as store:
        store.upsert_crawl_session(_session(status=CrawlSessionStatus.PAUSED))
        Heartbeat(store, SESSION_ID, stamp=lambda: "t").touch()
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.PAUSED
        assert (session.total, session.pending) == (0, 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))


def test_heartbeat_failure_does_not_break_the_phase() -> None:
    """Сбой записи heartbeat не поднимается: наблюдаемость не роняет фазу краула."""

    class _BrokenStore:
        def touch_crawl_session_heartbeat(self, *_args: object, **_kwargs: object) -> None:
            raise sqlite3.OperationalError("database is locked")

    heartbeat = Heartbeat(_BrokenStore(), "crawl:x", owner="this-host/1/2")
    assert heartbeat.touch() is False
    # Неудачная попытка не «съедает» окно: следующий вызов снова пытается записать.
    assert heartbeat.touch() is False
