"""coHound — свидетельство живости сеанса краула (F1/Д-6): ``owner``, heartbeat, recovery.

Мёртвый элемент неотличим от медленного, а процесс, убитый SIGKILL/OOM, не оставляет
Python-размотки: строка ``crawl_sessions`` навсегда остаётся ``running``. Распределённого
lease (аренды) здесь нет и быть не может — run-DB это SQLite, локальный одно-писательный
файл, и название «lease» обещало бы больше, чем даёт носитель (``10`` §9 К-4). Правильная
рамка — **свидетельство живости** плюс **recovery-on-open** на следующей попытке.

Два сигнала, в порядке силы:

1. ``owner`` — ``host/pid/старт-процесса`` исполнителя. На **этом** хосте ответ точен:
   pid жив и момент старта совпадает (иначе это переиспользованный pid) ⇒ сеанс живой
   независимо от возраста heartbeat. Чужой хост ⇒ ответа нет (``None``): локальные средства
   не видят процессов другой ноды, и выдавать незнание за смерть запрещено.
2. ``heartbeat_at`` — когда владелец последний раз подавал признаки жизни. Применяется
   только там, где ответ ``owner`` неизвестен. Heartbeat ничего не резервирует — он
   оставляет след; поэтому его отсутствие не «освобождает» сеанс автоматически, а лишь
   позволяет назвать его прерванным.

Статус, которым это заканчивается, — ``interrupted``: тело краула не финализировано, и
``provenance_json`` не пишется (финализации не было). Причина остаётся в ``error``.
``partial`` занят политическим усечением, ``failed``/``cancelled`` — заявлениями о теле
краула, которых никто не делал.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import os
import socket
import time
from pathlib import Path
from typing import Any, Callable

from core.domain import CrawlSessionStatus

__all__ = [
    "HEARTBEAT_INTERVAL_S",
    "SESSION_STALE_AFTER_S",
    "Heartbeat",
    "current_owner",
    "interrupt_session",
    "owner_is_alive",
    "recover_interrupted_sessions",
    "stale_reason",
]

#: Объявленный интервал обновления ``heartbeat_at`` по ходу сеанса (60 s).
#: Выбран как компромисс: чаще — лишняя запись на каждый URL, реже — восстановление
#: позже обнаруживает смерть. Порог протухания выражен в этих же интервалах.
HEARTBEAT_INTERVAL_S = 60.0

#: Порог протухания: ``running``-сеанс, чей ``heartbeat_at`` старше, считается прерванным.
#:
#: Обоснование (не магия): heartbeat — **точечный**, а не таймерный: признак жизни пишется
#: на старте сеанса, на каждой записи элемента очереди и на границе продукта. Значит самый
#: длинный честный разрыв между точками — одна экстракция документа. Замеренный потолок
#: одной экстракции — ``full`` ≈ 151 s (``10`` §9.1, пики docling), ``600 s`` — примерно
#: четырёхкратный запас к нему и десять интервалов heartbeat. Меньший порог рисковал бы
#: объявить прерванным **живой** сеанс на медленном документе (ошибка хуже позднего
#: восстановления); больший — держал бы фантом ``running`` дольше нужного. На своём хосте
#: этот порог вообще не главный сигнал: живой pid сильнее heartbeat (см. :func:`stale_reason`).
SESSION_STALE_AFTER_S = 600.0

#: Разделитель полей ``owner``: ``host/pid/start``.
_OWNER_SEP = "/"

#: Поле ``starttime`` в ``/proc/<pid>/stat`` (22-е поле целиком; после ``comm`` — индекс 19).
_PROC_START_INDEX = 19


def _now_utc() -> _dt.datetime:
    """Текущее время в UTC (aware)."""
    return _dt.datetime.now(_dt.timezone.utc)


def _parse_instant(text: str | None) -> _dt.datetime | None:
    """Разбирает ISO-8601 метку в aware-datetime (naive трактуется как UTC).

    Нераспознаваемая метка → ``None``: «не знаю» честнее, чем выдуманный возраст.
    """
    if not text:
        return None
    try:
        parsed = _dt.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.timezone.utc)
    return parsed


def _proc_start_token(pid: int, proc_root: str | Path = "/proc") -> str | None:
    """Момент старта процесса из ``/proc/<pid>/stat`` (защита от переиспользования pid).

    ``None`` — ядро ОС не даёт этого поля (не Linux); тогда сверка старта
    пропускается, но сам факт существования pid остаётся ответом.
    """
    try:
        text = (Path(proc_root) / str(pid) / "stat").read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        fields = text.rsplit(")", 1)[1].split()
    except IndexError:  # pragma: no cover — нечитаемый /proc/stat маловероятен
        return None
    if len(fields) <= _PROC_START_INDEX:  # pragma: no cover — старый/усечённый формат
        return None
    return fields[_PROC_START_INDEX]


def current_owner(
    *,
    pid: int | None = None,
    hostname: str | None = None,
    proc_root: str | Path = "/proc",
) -> str:
    """Идентификатор исполняющего процесса: ``host/pid/старт``.

    PID сам по себе недостаточен (переиспользуется ОС), поэтому в метку входит момент
    старта процесса — иначе «мёртвый владелец» мог бы совпасть с посторонним живым pid.
    """
    resolved_pid = os.getpid() if pid is None else int(pid)
    host = hostname if hostname is not None else socket.gethostname()
    start = _proc_start_token(resolved_pid, proc_root) or "?"
    return _OWNER_SEP.join((host, str(resolved_pid), start))


def owner_is_alive(
    owner: str | None,
    *,
    hostname: str | None = None,
    proc_root: str | Path = "/proc",
) -> bool | None:
    """Жив ли процесс-владелец по ``owner`` — троичный ответ.

    * ``True`` — владелец на этом хосте и его процесс существует (и старт совпадает);
    * ``False`` — владелец на этом хосте, но процесса нет либо pid переиспользован;
    * ``None`` — ответа нет: владельца нет, метка не разбирается или он на другом хосте.

    ``None`` не означает «мёртв»: незнание не выдаётся за смерть (иначе recovery убивал бы
    живые сеансы чужой ноды).
    """
    if not owner:
        return None
    parts = str(owner).split(_OWNER_SEP)
    if len(parts) < 2:
        return None
    host, pid_text = parts[0], parts[1]
    resolved_host = hostname if hostname is not None else socket.gethostname()
    if host != resolved_host:
        return None
    try:
        pid = int(pid_text)
    except ValueError:
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover — чужой uid: процесс существует
        return True
    except OSError:  # pragma: no cover — система без сигналов
        return None
    recorded_start = parts[2] if len(parts) > 2 else ""
    actual_start = _proc_start_token(pid, proc_root)
    if recorded_start and recorded_start != "?" and actual_start:
        if recorded_start != actual_start:
            # PID переиспользован: «тот» процесс мёртв, хотя номер занят.
            return False
    return True


def stale_reason(
    session: Any,
    *,
    now: _dt.datetime | None = None,
    stale_after_s: float = SESSION_STALE_AFTER_S,
    owner_alive: Callable[[str | None], bool | None] = owner_is_alive,
) -> str | None:
    """Причина считать ``running``-сеанс прерванным, либо ``None`` (сеанс живой/неизвестно).

    Порядок сигналов: ``owner`` (если ответ ``True`` — сеанс живой, проверка кончается),
    затем ``heartbeat_at``. Машинно-читаемая причина начинается с кода:
    ``owner_dead``/``no_heartbeat``/``bad_heartbeat``/``stale_heartbeat``.
    """
    if getattr(session, "status", None) != CrawlSessionStatus.RUNNING:
        return None
    owner = getattr(session, "owner", None)
    alive = owner_alive(owner)
    if alive is True:
        return None
    if alive is False:
        return f"owner_dead: owner={owner} не исполняется на этом хосте"
    heartbeat_at = getattr(session, "heartbeat_at", None)
    if not heartbeat_at:
        return f"no_heartbeat: heartbeat_at не записан (owner={owner or '—'})"
    moment = now if now is not None else _now_utc()
    stamp = _parse_instant(heartbeat_at)
    if stamp is None:
        return f"bad_heartbeat: heartbeat_at={heartbeat_at!r} не разбирается"
    age = (moment - stamp).total_seconds()
    if age > float(stale_after_s):
        return (
            f"stale_heartbeat: возраст {age:.0f} s > порога {float(stale_after_s):.0f} s"
            f" (heartbeat_at={heartbeat_at}, owner={owner or '—'})"
        )
    return None


def interrupt_session(
    store: Any,
    session_id: str,
    *,
    reason: str,
    at: str | None = None,
) -> None:
    """Переводит сеанс в ``interrupted`` с причиной в ``error`` (одна точка семантики).

    ``provenance_json`` не пишется: финализации не было, и выдумывать её запрещено.
    """
    store.update_crawl_session_status(
        session_id,
        CrawlSessionStatus.INTERRUPTED,
        finished_at=at or _now_utc().isoformat(),
        error=reason,
    )


def recover_interrupted_sessions(
    store: Any,
    *,
    now: _dt.datetime | None = None,
    stale_after_s: float = SESSION_STALE_AFTER_S,
    owner_alive: Callable[[str | None], bool | None] = owner_is_alive,
) -> list[str]:
    """Recovery-on-open: протухшие ``running`` → ``interrupted``; возвращает их ``session_id``.

    Идемпотентен: после перевода статус не ``running``, поэтому повторный вызов ничего не
    находит (``[]``). Живой сеанс (живой ``owner`` на этом хосте или свежий heartbeat) не
    трогается. Сети не требует — только чтение БД и ``/proc`` этого хоста.
    """
    moment = now if now is not None else _now_utc()
    stamp = moment.isoformat()
    recovered: list[str] = []
    for session in store.iter_crawl_sessions(status=CrawlSessionStatus.RUNNING):
        reason = stale_reason(
            session,
            now=moment,
            stale_after_s=stale_after_s,
            owner_alive=owner_alive,
        )
        if reason is None:
            continue
        interrupt_session(store, session.session_id, reason=reason, at=stamp)
        recovered.append(session.session_id)
    return recovered


class Heartbeat:
    """Точки жизни сеанса: обновляет ``heartbeat_at`` не чаще объявленного интервала.

    Вызывается синхронно из тех мест, где сеанс **действительно** работает (запись элемента
    очереди, граница продукта), и троттлится по ``time.monotonic``: признак жизни не должен
    стоить записи на каждый URL. Отдельного потока нет намеренно — heartbeat обязан
    исчезать вместе с процессом (SIGKILL/OOM убивает и поток), а не переживать его.

    ``store``/``session_id`` равные ``None`` делают объект пустым: durable-краул может быть
    выключен (нет стора/``run_id``), и тогда все вызовы — no-op.
    """

    def __init__(
        self,
        store: Any | None,
        session_id: str | None,
        *,
        interval_s: float = HEARTBEAT_INTERVAL_S,
        owner: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        stamp: Callable[[], str] = lambda: _now_utc().isoformat(),
    ) -> None:
        self._store = store
        self._session_id = session_id
        self._interval_s = float(interval_s)
        self._owner = owner if owner is not None else (
            current_owner() if store is not None and session_id is not None else None
        )
        self._clock = clock
        self._stamp = stamp
        self._last: float | None = None

    @property
    def enabled(self) -> bool:
        """Включён ли heartbeat (есть стор и сеанс)."""
        return self._store is not None and self._session_id is not None

    def touch(self, *, force: bool = False) -> bool:
        """Отмечает признак жизни, если прошёл интервал (или ``force``). ``True`` — записали.

        Сбой записи не поднимается: свидетельство живости — наблюдаемость, а не измерение, и
        падение фазы из-за него было бы хуже его отсутствия. Деградация при этом **видима**,
        а не молчалива: без ``heartbeat_at`` сеанс описывается причинами
        ``no_heartbeat``/``stale_heartbeat`` (recovery назовёт его ``interrupted``), а на этом
        хосте живость всё равно видна по ``owner`` — поэтому отсутствие heartbeat не превращает
        живой сеанс в мёртвый (см. :func:`stale_reason`).
        """
        if not self.enabled:
            return False
        moment = self._clock()
        if not force and self._last is not None and moment - self._last < self._interval_s:
            return False
        try:
            self._store.touch_crawl_session_heartbeat(
                self._session_id, at=self._stamp(), owner=self._owner
            )
        except Exception:  # noqa: BLE001 — наблюдаемость не роняет фазу краула
            return False
        self._last = moment
        return True
