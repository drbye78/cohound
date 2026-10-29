"""Тесты файлового лока пакета (09 §4.4): захват, протухание, отказ, проводка в CLI.

Без сети и без прогонов: лок проверяется на временных каталогах, «живой» чужой держатель
эмулируется дескриптором с ``flock`` внутри тестового процесса, «мёртвый» — pid заведомо
завершённого процесса.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from core.cli.errors import EXIT_USAGE, CliError
from core.config.paths import PathsConfig
from core.util.package_lock import (
    LockHolder,
    PackageLock,
    PackageLocked,
    package_lock_path,
)

# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _dead_pid() -> int:
    """pid заведомо завершённого (и пожатого) процесса: ``os.kill(pid, 0)`` не находит его."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def _write_holder(path: Path, **fields: Any) -> None:
    """Пишет содержимое файла лока напрямую (в обход :class:`PackageLock`)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fields), encoding="utf-8")


@contextlib.contextmanager
def _held_flock(path: Path) -> Iterator[int]:
    """Держит ``flock`` на ``path`` напрямую — эмуляция «лок занят живым держателем».

    Нужен там, где содержимое файла должно быть **чужим** (мёртвый pid, чужой хост,
    битый JSON), а сам лок — реально занятым: через :class:`PackageLock` так не сделать,
    он пишет свой дескриптор под локом.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        yield fd
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# ---------------------------------------------------------------------------
# Базовый цикл: взять / отпустить / взять снова
# ---------------------------------------------------------------------------


def test_lock_acquired_and_released(tmp_path: Path) -> None:
    """Захват пишет держателя, освобождение снимает лок, файл остаётся на месте."""
    path = tmp_path / "pkg" / ".lock"
    lock = PackageLock(path, command="run")
    lock.acquire()
    try:
        assert lock.held
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["host"] == socket.gethostname()
        assert payload["pid"] == os.getpid()
        assert payload["command"] == "run"
        assert payload["started_at"]
    finally:
        lock.release()
    assert not lock.held
    # Файл не удаляется: удаление гоняет ABA (см. докстринг модуля).
    assert path.exists()

    # Повторное взятие после выхода — успешно, и это не «перехват».
    with PackageLock(path, command="run") as again:
        assert again.held
        assert again.reclaimed_from is None
    with PackageLock(path) as third:
        assert third.held


def test_package_lock_path_is_state_layout() -> None:
    """Путь лока — ``{state_root}/{package_id}/.lock`` (точка ресурса — state пакета)."""
    assert package_lock_path("state", "acme-db") == Path("state/acme-db/.lock")
    assert package_lock_path(Path("/srv/state"), "acme-db") == Path(
        "/srv/state/acme-db/.lock"
    )


# ---------------------------------------------------------------------------
# Отказы: тот же процесс, чужой хост, битое содержимое, таймаут
# ---------------------------------------------------------------------------


def test_second_lock_in_same_process_names_holder(tmp_path: Path) -> None:
    """Второй лок на тот же путь в том же процессе — ``PackageLocked`` с нашим pid."""
    path = tmp_path / ".lock"
    first = PackageLock(path, command="run").acquire()
    try:
        with pytest.raises(PackageLocked) as exc:
            PackageLock(path, command="crawl run").acquire()
        message = str(exc.value)
        assert str(os.getpid()) in message
        assert str(path) in message
        assert exc.value.holder is not None
        assert exc.value.holder.pid == os.getpid()
        assert exc.value.reason is not None
    finally:
        first.release()


def test_stale_local_dead_pid_is_reclaimed(tmp_path: Path) -> None:
    """Мёртвый локальный pid (след SIGKILL) — перехват, и он виден в ``reclaimed_from``."""
    path = tmp_path / ".lock"
    dead = _dead_pid()
    _write_holder(
        path,
        host=socket.gethostname(),
        pid=dead,
        started_at="2026-10-29T00:00:00+00:00",
        command="run",
    )
    with PackageLock(path, command="run") as lock:
        assert lock.reclaimed_from is not None
        assert lock.reclaimed_from.pid == dead
        assert lock.reclaimed_from.command == "run"
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["pid"] == os.getpid()


def test_dead_pid_with_held_flock_is_not_stolen(tmp_path: Path) -> None:
    """Мёртвый pid в файле не даёт права отобрать **реально занятый** лок."""
    path = tmp_path / ".lock"
    with _held_flock(path):
        _write_holder(
            path,
            host=socket.gethostname(),
            pid=_dead_pid(),
            started_at="2026-10-29T00:00:00+00:00",
            command="run",
        )
        with pytest.raises(PackageLocked) as exc:
            PackageLock(path, timeout_s=0.1).acquire()
        assert str(path) in str(exc.value)
        assert exc.value.holder is not None


def test_foreign_host_is_not_reclaimed(tmp_path: Path) -> None:
    """Чужой хост: «неизвестно» ≠ «мертво» — отказ с именем хоста и путём файла."""
    path = tmp_path / ".lock"
    with _held_flock(path):
        _write_holder(
            path,
            host="other-node",
            pid=1,
            started_at="2026-10-29T00:00:00+00:00",
            command="run",
        )
        with pytest.raises(PackageLocked) as exc:
            PackageLock(path, timeout_s=0.05).acquire()
        message = str(exc.value)
        assert "other-node" in message
        assert str(path) in message
        assert "неизвестным" not in message


def test_broken_json_while_locked_is_not_a_parse_error(tmp_path: Path) -> None:
    """Битый/пустой JSON у живого лока — отказ лока, а не исключение разбора."""
    for content in ("", "{not json", '{"host": 1, "pid": "нет"'):
        path = tmp_path / f"broken-{len(content)}" / ".lock"
        with _held_flock(path):
            path.write_text(content, encoding="utf-8")
            with pytest.raises(PackageLocked) as exc:
                PackageLock(path, timeout_s=0.05).acquire()
            assert "неизвестным держателем" in str(exc.value)


def test_timeout_waits_then_raises(tmp_path: Path) -> None:
    """``timeout_s > 0``: занятый лок ждёт и падает по сроку (ожидание не нулевое)."""
    path = tmp_path / ".lock"
    with _held_flock(path):
        started = time.monotonic()
        with pytest.raises(PackageLocked):
            PackageLock(path, timeout_s=0.3).acquire()
        elapsed = time.monotonic() - started
    assert elapsed >= 0.25
    assert elapsed < 5.0


def test_lock_released_on_exception(tmp_path: Path) -> None:
    """Исключение внутри ``with`` не оставляет лок взятым (иначе пакет заперт до ребута)."""
    path = tmp_path / ".lock"
    with pytest.raises(RuntimeError), PackageLock(path):
        raise RuntimeError("boom")
    # Повторный захват — доказательство, что flock действительно снят, а не только флаг.
    with PackageLock(path) as again:
        assert again.held


def test_holder_describe_is_operator_friendly() -> None:
    """``describe`` называет хост/pid/старт/команду, а у неизвестного — честно молчит."""
    holder = LockHolder(
        host="node-1", pid=42, started_at="2026-10-29T00:00:00+00:00", command="run"
    )
    text = holder.describe()
    assert "node-1/42" in text
    assert "2026-10-29T00:00:00+00:00" in text
    assert "run" in text
    assert "неизвестным держателем" in LockHolder().describe()


# ---------------------------------------------------------------------------
# Проводка в CLI: занятый лок — отказ, state.db не создаётся
# ---------------------------------------------------------------------------


def test_state_adopt_refuses_when_package_locked(tmp_path: Path) -> None:
    """``hound package state adopt`` на занятом пакете — ``EXIT_USAGE`` и никакого state.db.

    Проводка покрыта тестом у самой дешёвой из трёх команд (adoption не краулит).
    ``cmd_run``/``_execute_crawl`` проверены чтением кода: у них та же обёртка
    ``_package_lock`` и тот же порядок «лок до открытия БД».
    """
    from core.cli.package.commands import cmd_package_state_adopt

    package_dir = tmp_path / "packages" / "acme-db"
    package_dir.mkdir(parents=True)
    (package_dir / "package.yaml").write_text("package_id: acme-db\n", encoding="utf-8")
    state_root = tmp_path / "state"
    paths = PathsConfig(
        packages=tmp_path / "packages",
        runs=tmp_path / "runs",
        index_store=tmp_path / "index_store",
        state=state_root,
    )
    args = type(
        "Args",
        (),
        {
            "package": str(package_dir),
            "paths": paths,
            "runs_dir": None,
            "index_store": None,
            "from_run": None,
            "cycle_id": None,
            "actor": "tester",
            "dry_run": False,
            "json": False,
        },
    )()

    lock_path = package_lock_path(state_root, "acme-db")
    with PackageLock(lock_path, command="run"):
        with pytest.raises(CliError) as exc:
            cmd_package_state_adopt(args)
        assert exc.value.exit_code == EXIT_USAGE
        assert exc.value.code == "state"
        message = str(exc.value)
        assert "acme-db" in message
        assert str(os.getpid()) in message
        assert str(lock_path) in message
    assert not (state_root / "acme-db" / "state.db").exists()
