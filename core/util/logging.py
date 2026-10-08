"""Структурированные JSON-lines логи по фазам конвейера (W12/T12.1, 04 §B.1, 03 §A.2).

Каждая фаза пишет свой поток в ``runs/{run_id}/logs/{phase}.jsonl``: одна запись —
одна JSON-строка. Поля фиксированы (04 §B.1): ``ts``, ``run_id``, ``phase``,
``component``, ``level``, ``message``, ``ctx`` (произвольный словарь:
``product``/``param``/``chunk_id``/…), ``invariant`` (при нарушении инварианта).

Уровни: ``DEBUG`` (retrieval-скоры), ``INFO`` (шаги фаз), ``WARN`` (аномалии §10.4),
``ERROR`` (сбои).

Контракт сериализации
---------------------
``structlog`` (05 §2.7, вердикт **a**) — **опциональный** backend рендеринга: модуль
импортируется и работает без него (stdlib ``json.dumps``). Оба пути дают **идентичную**
форму: UTF-8, ``ensure_ascii=False``, ``sort_keys=True``, ровно одна строка на вызов.
Наличие ``structlog`` фиксируется модульным флагом :data:`HAS_STRUCTLOG` (его можно
подменить в тестах).

Модуль категорийно нейтрален (§1.3): никаких предметных терминов; ``ctx`` задаёт
вызывающий код.
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Callable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

from core.util.security import sanitize_filename

__all__ = ["PhaseLogger", "LEVELS", "LEVEL_RANK", "HAS_STRUCTLOG"]

#: Допустимые уровни (04 §B.1), по возрастанию важности.
LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "WARN", "ERROR")

#: Числовой ранг уровня — для фильтрации по ``min_level``.
LEVEL_RANK: dict[str, int] = {level: index for index, level in enumerate(LEVELS)}

#: Наличие ``structlog`` (опциональный backend рендеринга; см. докстринг модуля).
try:  # pragma: no cover - наличие зависит от окружения
    import structlog as _structlog  # noqa: F401

    HAS_STRUCTLOG = True
except ImportError:  # pragma: no cover - штатный путь без внешней зависимости
    HAS_STRUCTLOG = False


def _utc_now_iso() -> str:
    """Текущее время UTC в ISO-8601 (дефолтные часы)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _json_dumps(obj: Any, **kwargs: Any) -> str:
    """Детерминированная JSON-сериализация: UTF-8-совместимо, сортировка ключей."""
    kwargs.setdefault("ensure_ascii", False)
    kwargs.setdefault("sort_keys", True)
    return json.dumps(obj, **kwargs)


@lru_cache(maxsize=1)
def _structlog_renderer() -> Callable[[Any, str, Mapping[str, Any]], str]:  # pragma: no cover
    """Ленивый ``structlog``-рендерер (JSON-lines), создаётся один раз.

    Сериализатор — тот же :func:`_json_dumps`, поэтому вывод backend-ов неотличим.
    """
    import structlog

    return structlog.processors.JSONRenderer(serializer=_json_dumps)


def _render_event(event: Mapping[str, Any]) -> str:
    """Рендерит событие в одну JSON-строку (без завершающего ``\\n``).

    При доступном ``structlog`` используется его ``JSONRenderer``; при его отсутствии —
    прямой stdlib-путь. Форма вывода одинакова (см. :func:`_json_dumps`).
    """
    if HAS_STRUCTLOG:  # pragma: no cover - зависит от окружения (structlog опционален)
        return _structlog_renderer()(None, "info", dict(event))
    return _json_dumps(event)


def _normalize_level(level: str) -> str:
    """Нормализует уровень к верхнему регистру; неизвестный → :class:`ValueError`."""
    normalized = str(level).upper()
    if normalized not in LEVEL_RANK:
        raise ValueError(
            f"неизвестный уровень логирования: {level!r} (допустимо: {', '.join(LEVELS)})"
        )
    return normalized


class PhaseLogger:
    """Пишет JSON-lines лог одной фазы в ``runs/{run_id}/logs/{phase}.jsonl``.

    :param run_id: идентификатор прогона (имя подкаталога ``runs/``).
    :param phase: имя фазы (имя файла без расширения).
    :param logs_root: корень логов (по умолчанию ``runs``); каталоги создаются по требованию.
    :param component: компонент-источник записи; ``None`` → совпадает с ``phase``.
    :param min_level: минимальный уровень для записи (фильтрация), из :data:`LEVELS`.
    :param now: фабрика ``ts`` (внедряемые часы для детерминированных тестов);
        по умолчанию — текущее время UTC в ISO-8601.
    """

    def __init__(
        self,
        run_id: str,
        phase: str,
        logs_root: str | Path = "runs",
        *,
        component: str | None = None,
        min_level: str = "DEBUG",
        now: Callable[[], str] | None = None,
    ) -> None:
        if not run_id:
            raise ValueError("run_id не может быть пустым")
        if not phase:
            raise ValueError("phase не может быть пустой")
        self.run_id = run_id
        self.phase = phase
        self.component = component if component is not None else phase
        self.min_level = _normalize_level(min_level)
        self._min_rank = LEVEL_RANK[self.min_level]
        self._now = now or _utc_now_iso
        self.path = (
            Path(logs_root)
            / sanitize_filename(run_id)
            / "logs"
            / f"{sanitize_filename(phase)}.jsonl"
        )

    # -- публичный интерфейс --------------------------------------------- #
    def log(
        self,
        level: str,
        message: str,
        ctx: Mapping[str, Any] | None = None,
        invariant: str | None = None,
    ) -> dict[str, Any] | None:
        """Добавляет одну запись; возвращает событие либо ``None``, если отфильтровано.

        :raises ValueError: неизвестный ``level``.
        """
        normalized = _normalize_level(level)
        if LEVEL_RANK[normalized] < self._min_rank:
            return None
        event: dict[str, Any] = {
            "ts": self._now(),
            "run_id": self.run_id,
            "phase": self.phase,
            "component": self.component,
            "level": normalized,
            "message": message,
            "ctx": dict(ctx) if ctx else {},
            "invariant": invariant,
        }
        line = _render_event(event) + "\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
        return event

    def debug(self, message: str, **ctx: Any) -> dict[str, Any] | None:
        """Запись уровня ``DEBUG`` (retrieval-скоры)."""
        return self._convenience("DEBUG", message, ctx)

    def info(self, message: str, **ctx: Any) -> dict[str, Any] | None:
        """Запись уровня ``INFO`` (шаги фаз)."""
        return self._convenience("INFO", message, ctx)

    def warn(self, message: str, **ctx: Any) -> dict[str, Any] | None:
        """Запись уровня ``WARN`` (аномалии §10.4)."""
        return self._convenience("WARN", message, ctx)

    def error(self, message: str, **ctx: Any) -> dict[str, Any] | None:
        """Запись уровня ``ERROR`` (сбои)."""
        return self._convenience("ERROR", message, ctx)

    def _convenience(
        self, level: str, message: str, ctx: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Общий путь удобных методов: ``invariant`` выносится в отдельное поле (04 §B.1)."""
        fields = dict(ctx)
        invariant = fields.pop("invariant", None)
        return self.log(level, message, fields, invariant=invariant)
