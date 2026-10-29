"""coHound — файловый lock пакета (09 §4.4): один писатель на ``state/{package_id}/``.

**Зачем.** Два одновременных ``hound run`` на один пакет не отвергаются. SQLite WAL с
``PRAGMA busy_timeout = 5000`` сериализует **запись в БД** (второй писатель ждёт до 5 с и
получает «database is locked»), но это защита носителя, а не пакета: сеть, бюджет краула,
``corpus_raw/``, ``index_store/`` и ``state.db`` принадлежат **пакету**, и два краула
одного пакета делят их, а не отвергают друг друга. Lock — свойство пакета, поэтому
носитель — файл рядом с durable-состоянием: ``state/{package_id}/.lock``.

**Носитель — ``flock(2)``.** ``fcntl.flock(LOCK_EX | LOCK_NB)`` на дескрипторе файла: лок
держится дескриптором и снимается ядром при завершении процесса (в том числе при
``SIGKILL``/OOM — в отличие от pid-файлов, которые остаются врать). Удаление файла для
снятия лока не требуется и запрещено (см. ниже).

**Почему файл не удаляется.** Удаление гоняет ABA: процесс A держит лок на inode X, B
удаляет файл и создаёт новый (inode Y), C берёт лок на Y — и B с C одновременно «владеют»
локом пакета. Поэтому файл живёт постоянно, а перезаписывается только его **содержимое**,
и только под взятым локом. Смысл файла — не существование, а дескриптор, на который взят
flock.

**Содержимое** — ``{"host", "pid", "started_at", "command"}``: оно нужно оператору, а не
алгоритму (кроме одного решения — протухания, см. ниже). Пустой или битый JSON у **живого**
лока — не ошибка разбора: держатель просто неизвестен, а лок всё равно занят.

**Протухание: «неизвестно» ≠ «мертво».** Живость pid проверяется только для **своего**
хоста: локальные средства не видят процессов другой ноды (тот же довод, что в
``core/pipeline/liveness.py``), поэтому чужой хост не перехватывается никогда — вместо
этого :class:`PackageLocked` называет хост и путь файла, чтобы оператор разобрался сам.
Перехват возможен только для мёртвого **локального** pid, и оба случая оставляют след в
``reclaimed_from``:

* flock свободен, а содержимое называет мёртвый локальный pid — обычный след ``SIGKILL``
  (ядро лок сняло, файл остался);
* flock занят, а содержимое называет мёртвый локальный pid — лок держит не тот процесс,
  что записан (например, дескриптор унаследован потомком): делается одна повторная
  попытка, и если лок успел освободиться, он перехватывается.

``pid`` == текущий pid при занятом локе — это повторный вход **в том же процессе**: ошибка
вызывающего, а не протухание (перехват свёл бы два прогона в один).

Слой ``util``: живость проверяется локально (``os.kill(pid, 0)``), без импорта
``core.pipeline`` — матрица слоёв запрещает ``util → pipeline``.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import datetime as _dt
import errno
import fcntl
import json
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "LOCK_FILE_NAME",
    "POLL_INTERVAL_S",
    "LockHolder",
    "PackageLock",
    "PackageLocked",
    "package_lock_path",
]

#: Имя файла лока внутри ``state/{package_id}/``.
LOCK_FILE_NAME = ".lock"

#: Пауза между попытками взять лок. Опрос, а не блокирующий ``flock``: нужен срок
#: (``timeout_s``), по истечении которого отказ обязан назвать держателя.
POLL_INTERVAL_S = 0.05


def package_lock_path(state_root: str | Path, package_id: str) -> Path:
    """Путь файла лока пакета: ``{state_root}/{package_id}/.lock`` (09 §4.4).

    Точка ресурса — каталог durable-состояния пакета: он один на пакет и переживает
    прогоны. Раскладку резолвит вызывающий (``core.config.paths``), модуль её не знает.
    """
    return Path(state_root) / str(package_id) / LOCK_FILE_NAME


def _utc_now() -> str:
    """Текущее время UTC в ISO-8601 (метка ``started_at`` в файле лока)."""
    return _dt.datetime.now(_dt.UTC).isoformat()


def _current_host() -> str:
    """Имя хоста текущего процесса (``unknown`` — если система не дала имени)."""
    return socket.gethostname() or "unknown"


def _pid_alive(pid: int) -> bool:
    """Жив ли процесс ``pid`` **на этом хосте** (``os.kill(pid, 0)``).

    Возвращает ``True`` и в случае неопределённости: ``PermissionError`` означает, что
    процесс существует, но принадлежит другому пользователю, а прочие ``OSError`` —
    «неизвестно», и выдавать незнание за смерть запрещено (тот же довод, что в
    ``core/pipeline/liveness.py``). Переиспользованный pid тоже даёт ``True`` — это
    безопасное направление ошибки: живой лок не перехватывается по ошибке.
    """
    if pid <= 0:
        # ``os.kill(0, 0)`` адресует группу процессов, а не процесс: ответа нет.
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


@dataclass(frozen=True)
class LockHolder:
    """Держатель лока — то, что записано в файле (``None`` у неизвестных полей).

    Неизвестное поле — не ошибка: файл мог быть записан другой версией кода, а пустой или
    битый JSON вообще не даёт полей. Решение о перехвате принимается только по паре
    ``host``+``pid`` (см. докстринг модуля), поэтому остальные поля — для оператора.
    """

    host: str | None = None
    pid: int | None = None
    started_at: str | None = None
    command: str | None = None

    @property
    def known(self) -> bool:
        """Есть ли хоть что-то, что позволяет назвать держателя."""
        return self.host is not None or self.pid is not None

    @classmethod
    def from_payload(cls, payload: Any) -> LockHolder | None:
        """Собирает держателя из разобранного JSON; ``None`` — если это не словарь.

        Поля не проверяются строго: ``pid`` принимается только целым (строка/``None`` →
        неизвестен), остальные — только строками. Кривое значение поля не делает держателя
        неизвестным целиком, если известно другое.
        """
        if not isinstance(payload, dict):
            return None
        host = payload.get("host")
        pid = payload.get("pid")
        started_at = payload.get("started_at")
        command = payload.get("command")
        return cls(
            host=host if isinstance(host, str) and host else None,
            pid=pid if isinstance(pid, int) and not isinstance(pid, bool) else None,
            started_at=(
                started_at if isinstance(started_at, str) and started_at else None
            ),
            command=command if isinstance(command, str) and command else None,
        )

    def as_dict(self) -> dict[str, Any]:
        """Поля держателя для записи в файл лока (детерминированный порядок)."""
        return {
            "host": self.host,
            "pid": self.pid,
            "started_at": self.started_at,
            "command": self.command,
        }

    def describe(self) -> str:
        """Формулировка держателя для сообщения оператору (творительный падеж).

        Складывается с префиксом вида «пакет '…' уже используется …»: возвращается
        «процессом host/pid (начат …, команда …)» либо, если держатель не определяется,
        честное «неизвестным держателем (…)».
        """
        if not self.known:
            return (
                "неизвестным держателем (содержимое файла лока пусто или повреждено; "
                "лок всё равно занят)"
            )
        host = self.host or "?"
        pid = "?" if self.pid is None else str(self.pid)
        started = self.started_at or "?"
        command = self.command or "?"
        return f"процессом {host}/{pid} (начат {started}, команда {command})"


class PackageLocked(Exception):
    """Лок пакета занят и не перехвачен.

    Сообщение называет держателя и путь файла: оператору нужен ответ «кто именно и где»,
    а не «занято». ``holder`` — прочитанное содержимое (``None``, если файл не читается),
    ``reason`` — уточнение для случаев, когда занятость не про протухание (повторный вход
    в том же процессе).
    """

    def __init__(
        self,
        path: str | Path,
        holder: LockHolder | None = None,
        *,
        reason: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.holder = holder
        self.reason = reason
        text = f"лок {self.path} занят: {(holder or LockHolder()).describe()}"
        if reason:
            text += f"; {reason}"
        super().__init__(text)


class PackageLock:
    """Файловый lock пакета (``state/{package_id}/.lock``) как контекстный менеджер.

    ``with PackageLock(path) as lock:`` берёт исключительный ``flock`` и снимает его на
    выходе (в том числе на исключении внутри блока). Файл не удаляется никогда — см.
    докстринг модуля (ABA). ``timeout_s`` — сколько ждать занятый лок; ``0`` — одна
    попытка. По истечении срока — :class:`PackageLocked` с держателем из файла.

    ``reclaimed_from`` заполняется, если лок перехвачен у протухшего держателя: факт
    перехвата обязан быть видимым, иначе «пакет оставлен посреди прогона» выглядит как
    чистый старт.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        timeout_s: float = 0.0,
        command: str | None = None,
    ) -> None:
        self.path = Path(path)
        self.timeout_s = max(0.0, float(timeout_s))
        self.command = command
        #: Метка старта **команды** (а не момента получения лока): при долгом ожидании
        #: оператору важно, когда процесс начал, а не когда дождался.
        self.started_at = _utc_now()
        #: Держатель, у которого лок перехвачен как протухший (``None`` — обычный захват).
        self.reclaimed_from: LockHolder | None = None
        #: Собственный дескриптор (``None`` — лок не взят).
        self._fd: int | None = None

    # -- контекстный менеджер ------------------------------------------------- #

    def __enter__(self) -> PackageLock:
        return self.acquire()

    def __exit__(self, *exc_info: object) -> None:
        self.release()

    # -- состояние ------------------------------------------------------------ #

    @property
    def held(self) -> bool:
        """Взят ли лок этим объектом."""
        return self._fd is not None

    # -- API ------------------------------------------------------------------ #

    def acquire(self) -> PackageLock:
        """Берёт лок или бросает :class:`PackageLocked` (держатель назван).

        Алгоритм: ``flock(LOCK_EX|LOCK_NB)``; при занятости — читается содержимое файла и
        решается, протух ли держатель (см. докстринг модуля); иначе — ожидание до
        ``timeout_s`` с опросом :data:`POLL_INTERVAL_S`.
        """
        if self._fd is not None:
            raise PackageLocked(
                self.path, self._own_holder(), reason="лок уже взят этим объектом"
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            previous = self._read_holder()
            deadline = time.monotonic() + self.timeout_s
            while True:
                if self._try_flock(fd):
                    # Свободный лок: прежний держатель мог умереть, оставив содержимое
                    # (SIGKILL снимает flock, файл — нет). Перехват виден, а не молчалив.
                    if previous is not None and self._is_dead_local(previous):
                        self.reclaimed_from = previous
                    self._write_holder(fd)
                    self._fd = fd
                    return self
                holder = self._read_holder() or previous
                if holder is not None and holder.pid == os.getpid():
                    # Повторный вход в том же процессе: перехват свёл бы два прогона в
                    # один, поэтому это отказ, а не протухание.
                    raise PackageLocked(
                        self.path,
                        holder,
                        reason="лок уже держит этот процесс (повторный вход)",
                    )
                stale = holder is not None and self._is_dead_local(holder)
                # Протух, но flock не снялся: держит не тот процесс, что записан. Одна
                # повторная попытка — лок мог освободиться между проверками.
                if stale and self._try_flock(fd):
                    self.reclaimed_from = holder
                    self._write_holder(fd)
                    self._fd = fd
                    return self
                if time.monotonic() >= deadline:
                    raise PackageLocked(self.path, holder)
                time.sleep(POLL_INTERVAL_S)
        except BaseException:
            os.close(fd)
            raise

    def release(self) -> None:
        """Снимает ``flock`` и закрывает дескриптор; файл остаётся на месте.

        Идемпотентен: повторный вызов после успешного освобождения ничего не делает.
        """
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    # -- внутреннее ----------------------------------------------------------- #

    def _own_holder(self) -> LockHolder:
        """Собственный дескриптор держателя (для сообщений и для записи в файл)."""
        return LockHolder(
            host=_current_host(),
            pid=os.getpid(),
            started_at=self.started_at,
            command=self.command,
        )

    def _is_dead_local(self, holder: LockHolder) -> bool:
        """Протух ли держатель: свой хост **и** мёртвый pid.

        Чужой хост или неизвестный pid — не протухание: «неизвестно» не значит «мертво».
        """
        if holder.pid is None:
            return False
        if holder.host != _current_host():
            return False
        return not _pid_alive(holder.pid)

    def _try_flock(self, fd: int) -> bool:
        """Одна неблокирующая попытка взять исключительный лок на ``fd``."""
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        except OSError as exc:  # pragma: no cover — EACCES/ENOLCK на экзотических ФС
            if exc.errno in (errno.EAGAIN, errno.EACCES):
                return False
            raise
        return True

    def _read_holder(self) -> LockHolder | None:
        """Читает держателя из файла лока; ``None`` — файл пуст/битый/нечитаем.

        Битый JSON у живого лока — **не** ошибка: держатель неизвестен, лок занят. Чтение
        идёт без лока (файл могут писать прямо сейчас), поэтому частичный JSON — нормальный
        исход, а не сбой.
        """
        try:
            text = self.path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None
        if not text.strip():
            return None
        try:
            payload = json.loads(text)
        except ValueError:
            return None
        return LockHolder.from_payload(payload)

    def _write_holder(self, fd: int) -> None:
        """Перезаписывает содержимое файла своим держателем (под взятым локом).

        Усечение + запись + ``fsync``: содержимое — не «признак занятости» (им является
        flock), а опознание для оператора, поэтому недописанный файл безвреден, но
        сбрасывать на диск его дешевле, чем разбираться с пустым.
        """
        data = json.dumps(self._own_holder().as_dict(), ensure_ascii=False).encode("utf-8")
        os.lseek(fd, 0, os.SEEK_SET)
        os.ftruncate(fd, 0)
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
