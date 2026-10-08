"""Аудит шва core↔edge → SIEM (W13/T13.9; 07 §8.4–§8.5).

Каждый вызов ``/v1/*`` фиксируется **структурным JSON-событием** — одна запись
на строку (JSON-lines). Такой поток пригоден для экспорта в SIEM (файл/syslog/
агент — механизм на стороне заказчика) и **коррелируется по ``request_id``**
сквозным образом с логами фаз ядра (``core/util/logging.py``, 04 §B).

Контракт SIEM-экспорта (07 §8.4):

* ровно одна JSON-строка на вызов;
* обязательные поля: ``ts``, ``request_id``, ``principal``, ``op``, ``status``,
  ``classification``, ``remote``;
* сериализация детерминирована: UTF-8, ``ensure_ascii=False``, ``sort_keys=True``
  (та же форма, что у структурированных логов ядра);
* ``status`` — решение по вызову: ``allow`` / ``deny`` / ``error``.

Классификация контента (07 §8.5) — **одна явная метка, задаваемая оператором**
(:data:`CLASSIFICATION_DEFAULT` = ``internal``); coHound **не изобретает**
таксономию данных, лишь предоставляет хук переопределения (:func:`classify`).
DLP-политики различают по ней веб-контент (``untrusted-public``) и внутренние
данные.

Модуль **категорийно нейтрален** (§1.3); сертификатного pinning нет (07 §8.1).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "AUDIT_STATUS_ALLOW",
    "AUDIT_STATUS_DENY",
    "AUDIT_STATUS_ERROR",
    "AUDIT_STATUSES",
    "CLASSIFICATION_DEFAULT",
    "CLASSIFICATION_INTERNAL",
    "CLASSIFICATION_UNTRUSTED_PUBLIC",
    "AuditEvent",
    "AuditSink",
    "FileAuditSink",
    "MemoryAuditSink",
    "Classification",
    "classify",
    "audit_call",
]

# ---------------------------------------------------------------------------
# Константы решений и классификации
# ---------------------------------------------------------------------------

#: Вызов разрешён.
AUDIT_STATUS_ALLOW = "allow"
#: Вызов отклонён аутентификацией/авторизацией.
AUDIT_STATUS_DENY = "deny"
#: Вызов завершился ошибкой исполнения (07 §8.4: ``decision=error``).
AUDIT_STATUS_ERROR = "error"

#: Допустимые решения аудита (07 §8.4).
AUDIT_STATUSES: frozenset[str] = frozenset(
    {AUDIT_STATUS_ALLOW, AUDIT_STATUS_DENY, AUDIT_STATUS_ERROR}
)

#: Метка по умолчанию: внутренние данные/метаданные (07 §8.5).
CLASSIFICATION_INTERNAL = "internal"
#: Метка веб-контента: публично недоверенное (07 §8.5).
CLASSIFICATION_UNTRUSTED_PUBLIC = "untrusted-public"
#: Метка по умолчанию для аудита (оператор может переопределить).
CLASSIFICATION_DEFAULT = CLASSIFICATION_INTERNAL


def _utc_now_iso() -> str:
    """Текущее время UTC в ISO-8601 (дефолтные часы аудита)."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Классификация контента (одна явная метка оператора; 07 §8.5)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Classification:
    """Явная классификация контента шва (07 §8.5).

    Одна метка, задаваемая оператором (``internal`` | ``untrusted-public`` | …;
    coHound не изобретает таксономию). :attr:`override` — необязательный хук
    ``(op, payload) -> label``, позволяющий оператору вычислить метку; без него
    возвращается :attr:`label`.

    :param label: метка по умолчанию.
    :param override: хук переопределения метки для конкретного вызова.
    """

    label: str = CLASSIFICATION_DEFAULT
    override: Callable[[str, Mapping[str, Any]], str] | None = None

    def __post_init__(self) -> None:
        if not str(self.label).strip():
            raise ValueError("Classification.label не может быть пустым")

    def __str__(self) -> str:
        return str(self.label)

    def for_call(self, op: str, payload: Mapping[str, Any] | None = None) -> str:
        """Метка для вызова ``op`` с ``payload`` (с учётом хука оператора)."""
        return classify(op, payload, override=self.override, default=self.label)


def classify(
    op: str,
    payload: Mapping[str, Any] | None = None,
    *,
    override: Callable[[str, Mapping[str, Any]], str] | None = None,
    default: str = CLASSIFICATION_DEFAULT,
) -> str:
    """Возвращает метку классификации для вызова ``op``.

    Без ``override`` — нейтральный ``default`` (``internal``). ``override`` —
    единственный явный хук оператора; никакой собственной таксономии данных
    coHound не навязывает (07 §8.5).
    """
    if override is not None:
        return str(override(str(op), payload if payload is not None else {}))
    return str(default)


def _principal_subject(principal: Any) -> str | None:
    """Приводит принципала к строке-субъекту для аудита (``None`` — анонимно).

    Принимает ``None``, готовую строку-субъект или объект с атрибутом ``subject``
    (:class:`core.edge.auth.Principal`) — без импорта, чтобы не связывать модули.
    """
    if principal is None:
        return None
    if isinstance(principal, str):
        return principal
    subject = getattr(principal, "subject", None)
    return None if subject is None else str(subject)


# ---------------------------------------------------------------------------
# Событие аудита
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AuditEvent:
    """Одно структурированное аудит-событие шва (SIEM-форма, 07 §8.4).

    :param ts: метка времени ISO-8601 (UTC); пустая → дозаполняется сink-ом.
    :param request_id: идентификатор корреляции (сквозной, 07 §8.2).
    :param principal: субъект-источник (workload-identity) или ``None``.
    :param op: операция шва (``fetch``/``search``/…).
    :param status: решение — :data:`AUDIT_STATUS_ALLOW` / ``DENY`` / ``ERROR``.
    :param classification: явная метка классификации контента (07 §8.5).
    :param remote: сетевой источник (peer) или ``None``.
    """

    ts: str
    request_id: str
    principal: str | None
    op: str
    status: str
    classification: str
    remote: str | None = None

    def __post_init__(self) -> None:
        if not str(self.request_id).strip():
            raise ValueError("request_id обязателен для корреляции SIEM (07 §8.4)")
        if not str(self.op).strip():
            raise ValueError("op обязателен в аудит-событии")
        if self.status not in AUDIT_STATUSES:
            raise ValueError(
                f"недопустимый статус аудита {self.status!r} "
                f"(ожидается одно из: {sorted(AUDIT_STATUSES)})"
            )
        if not str(self.classification).strip():
            raise ValueError("classification обязателен (07 §8.5)")

    def to_dict(self) -> dict[str, Any]:
        """Словарная форма ровно с обязательными SIEM-полями (07 §8.4)."""
        return {
            "ts": self.ts,
            "request_id": self.request_id,
            "principal": self.principal,
            "op": self.op,
            "status": self.status,
            "classification": self.classification,
            "remote": self.remote,
        }

    def to_json(self) -> str:
        """Детерминированная JSON-строка (UTF-8, сорт ключей; как логи ядра)."""
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)


# ---------------------------------------------------------------------------
# Sink-и (07 §8.4)
# ---------------------------------------------------------------------------


@runtime_checkable
class AuditSink(Protocol):
    """Контракт приёмника аудит-событий шва (файл/syslog/агент — за оператором)."""

    def emit(self, event: AuditEvent) -> None: ...


class FileAuditSink:
    """Пишет JSON-lines аудит в файл (append), одна строка на событие.

    Поток пригоден для SIEM-экспорта (07 §8.4). Если ``event.ts`` пуст, событие
    дозаполняется меткой инъецированного ``clock``.

    :param path: путь JSON-lines файла (каталоги создаются по требованию).
    :param clock: фабрика ``ts`` для событий без метки (внедряемые часы).
    """

    def __init__(self, path: str | Path, *, clock: Callable[[], str] | None = None) -> None:
        self.path = Path(path)
        self._clock = clock or _utc_now_iso

    def emit(self, event: AuditEvent) -> None:
        """Дописывает одно событие одной JSON-строкой."""
        if not event.ts:
            event = replace(event, ts=str(self._clock()))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(event.to_json() + "\n")


class MemoryAuditSink:
    """Приёмник в памяти (для тестов): аккумулирует события в списке."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        """Сохраняет событие в списке :attr:`events`."""
        self.events.append(event)


# ---------------------------------------------------------------------------
# Удобный вызов
# ---------------------------------------------------------------------------


def audit_call(
    sink: AuditSink,
    *,
    request_id: str,
    principal: Any = None,
    op: str,
    status: str,
    classification: str | Classification = CLASSIFICATION_DEFAULT,
    remote: str | None = None,
    clock: Callable[[], str] | None = None,
) -> AuditEvent:
    """Собирает :class:`AuditEvent`, отправляет в ``sink`` и возвращает его.

    :param principal: субъект-строка или объект с атрибутом ``subject``
        (:class:`core.edge.auth.Principal`); ``None`` — анонимный вызов.
    :param status: :data:`AUDIT_STATUS_ALLOW` / ``DENY`` / ``ERROR``.
    :param classification: метка-строка или :class:`Classification`.
    :param clock: фабрика ``ts`` (внедряемые часы для детерминизма).
    """
    now = clock or _utc_now_iso
    event = AuditEvent(
        ts=str(now()),
        request_id=str(request_id),
        principal=_principal_subject(principal),
        op=str(op),
        status=str(status),
        classification=str(classification),
        remote=None if remote is None else str(remote),
    )
    sink.emit(event)
    return event
