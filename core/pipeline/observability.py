"""Наблюдаемость прогона: инструментирование фаз §B.1 и запись метрик §B.2 (W12/T12.1).

Модуль — **точка сочленения** наблюдаемости, не меняющая контракт ``Runner``/``RunContext``
(W4). Он оборачивает реестр ``{phase: PhaseFn}`` так, что каждая фаза:

* пишет ``INFO``-запись о старте и ``INFO``/``WARN``-запись о завершении в
  ``runs/{run_id}/logs/{phase}.jsonl`` через :class:`~core.util.logging.PhaseLogger`
  (поля §B.1: ``ts``/``run_id``/``phase``/``component``/``level``/``message``/``ctx``;
  ``ctx`` несёт ``status``/``cells``/``errors``);
* измеряет длительность ``time.perf_counter`` (карта ``{phase: seconds}``);
* возвращает **исходный** :class:`~core.pipeline.runner.PhaseResult` без мутаций
  (``status``/``cells``/``errors`` не переписываются).

Метрики прогона (04 §B.2) собираются :func:`write_run_metrics` в
``runs/{run_id}/metrics.json`` — детерминированный JSON (``sort_keys=True``,
``ensure_ascii=False``), в том числе при остановке на гейте (метрики отражают то, что
реально отработало).

Принципы
--------
* **Ключи/версии — в held-out JSON-контракте** §B.1/§B.2: поля не выдумываются,
  а берутся из готовых :class:`~core.util.logging.PhaseLogger`/``RunMetrics``.
* **Детерминизм.** ``clock`` инжектируется для ``ts`` логов; длительности для метрик
  можно передать явной картой (см. :func:`write_run_metrics`).
* **Ошибки наблюдаемости не роняют прогон.** Сбой записи лога/исключение фазы
  фиксируются, но не подменяют результат фазы и не прерывают конвейер; исключение
  фазы пробрасывается дальше — его изолирует ``Runner`` (W4).

Модуль категорийно нейтрален (§1.3) и не импортирует ``core.adapters``/``core.edge``
(матрица 01 §1), поэтому пригоден внутри ядра. Только stdlib.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.pipeline.metrics import compute_metrics
from core.pipeline.runner import Clock, PhaseFn, PhaseResult, RunContext
from core.util.logging import PhaseLogger
from core.util.security import sanitize_filename

__all__ = ["InstrumentedPhases", "instrument_phases", "write_run_metrics"]

#: Имя файла метрик прогона (04 §B.2).
METRICS_FILENAME = "metrics.json"


@dataclass
class InstrumentedPhases:
    """Результат :func:`instrument_phases`: обёрнутые фазы, тайминг и логгеры.

    :param phases: новый словарь ``{phase: PhaseFn}`` — обёртки, делегирующие в оригинал.
    :param durations: карта ``{phase: seconds}`` (``time.perf_counter``), заполняется по мере
        исполнения обёрнутых фаз.
    """

    phases: dict[str, PhaseFn] = field(default_factory=dict)
    durations: dict[str, float] = field(default_factory=dict)
    _loggers: dict[str, PhaseLogger] = field(default_factory=dict, repr=False)
    _errors: list[str] = field(default_factory=list, repr=False)

    def logger_for(self, phase: str) -> PhaseLogger:
        """Логгер фазы (для инспекции/тестов); ``KeyError`` для неизвестной фазы."""
        return self._loggers[phase]

    @property
    def instrumentation_errors(self) -> list[str]:
        """Накопленные сбои самой наблюдаемости (не роняют прогон, но не скрываются)."""
        return list(self._errors)


def _safe_log(
    instrumented: InstrumentedPhases,
    phase: str,
    level: str,
    message: str,
    ctx: Mapping[str, Any],
) -> None:
    """Пишет запись лога, изолируя сбой наблюдаемости (не роняет фазу/прогон)."""
    try:
        instrumented._loggers[phase].log(level, message, ctx)
    except Exception as exc:  # noqa: BLE001 — наблюдаемость вторична к прогону
        instrumented._errors.append(
            f"{phase}: log({level}): {type(exc).__name__}: {exc}"
        )


def _instrument_phase(
    phase: str, fn: PhaseFn, instrumented: InstrumentedPhases
) -> PhaseFn:
    """Оборачивает одну фазу: старт/конец в лог §B.1 + тайминг; результат не мутируется."""

    def _wrapped(ctx: RunContext) -> PhaseResult:
        started = time.perf_counter()
        _safe_log(instrumented, phase, "INFO", "старт фазы", {"event": "start"})
        try:
            result = fn(ctx)
        except BaseException as exc:  # noqa: BLE001 — пробрасываем, зафиксировав тайминг
            instrumented.durations[phase] = time.perf_counter() - started
            _safe_log(
                instrumented,
                phase,
                "ERROR",
                "фаза завершилась исключением",
                {"event": "error", "error": type(exc).__name__},
            )
            raise
        instrumented.durations[phase] = time.perf_counter() - started
        level = "INFO" if result.status == "ok" else "WARN"
        _safe_log(
            instrumented,
            phase,
            level,
            "конец фазы",
            {
                "event": "end",
                "status": result.status,
                "cells": len(getattr(result, "cells", None) or ()),
                "errors": len(getattr(result, "errors", None) or ()),
            },
        )
        return result

    _wrapped.__name__ = f"instrumented_{phase}"
    _wrapped.__qualname__ = _wrapped.__name__
    return _wrapped


def instrument_phases(
    phases: Mapping[str, PhaseFn],
    *,
    run_id: str,
    logs_root: str | Path = "runs",
    clock: Clock | None = None,
    component: str | None = None,
) -> InstrumentedPhases:
    """Оборачивает реестр фаз логированием §B.1 и таймингом (W12/T12.1).

    :param phases: реестр ``{phase: PhaseFn}`` (например, из ``build_phases`` + гейты).
    :param run_id: идентификатор прогона (подкаталог ``runs/``).
    :param logs_root: корень логов (по умолчанию ``runs``).
    :param clock: часы прогона (``now_iso()``) — инжектируются в ``ts`` логов
        (детерминизм тестов); ``None`` → системное UTC.
    :param component: компонент-источник записей; ``None`` → совпадает с фазой.
    :returns: :class:`InstrumentedPhases` с новым ``.phases``, картой ``.durations``
        и доступом ``.logger_for(phase)``.
    """
    loggers = {
        phase: PhaseLogger(
            run_id,
            phase,
            logs_root,
            component=component,
            now=(clock.now_iso if clock is not None else None),
        )
        for phase in phases
    }
    instrumented = InstrumentedPhases(phases={}, durations={}, _loggers=loggers)
    for phase, fn in phases.items():
        instrumented.phases[phase] = _instrument_phase(phase, fn, instrumented)
    return instrumented


def write_run_metrics(
    *,
    run_id: str,
    out_dir: str | Path = "runs",
    durations: Mapping[str, Any] | None = None,
    store: Any = None,
    cells: Iterable[Any] | None = None,
    score_bundle: Any = None,
    search_log: Iterable[Any] | None = None,
    llm_calls: int = 0,
    tokens: Mapping[str, Any] | None = None,
) -> Path:
    """Считает :class:`~core.pipeline.metrics.RunMetrics` и пишет ``runs/{run_id}/metrics.json``.

    Метрики (04 §B.2) собираются готовой :func:`~core.pipeline.metrics.compute_metrics`
    (формулы не дублируются). JSON детерминирован (``sort_keys=True``,
    ``ensure_ascii=False``) через :meth:`RunMetrics.to_manifest_json`.

    :param run_id: идентификатор прогона.
    :param out_dir: корень прогонов (по умолчанию ``runs``); файл — ``{out_dir}/{run_id}/metrics.json``.
    :param durations: карта ``{phase: seconds}`` (тайминг инжектирует
        :class:`InstrumentedPhases`); служит источником ``duration_by_phase``.
    :param store: ``StorePort``; источник ячеек (``iter_cells``), если ``cells`` не передан.
    :param cells: явный список ячеек (приоритетнее ``store``).
    :param score_bundle: готовый ``ScoreBundle`` (``failure_rate``/``coverage_aggregate``).
    :param search_log: iterable записей с атрибутом ``.phase`` (I23).
    :param llm_calls: число вызовов LLM (стоимость, M1/§16).
    :param tokens: токены по категориям (``prompt``/``completion``/…).
    :returns: путь записанного ``metrics.json``.
    """
    metrics = compute_metrics(
        store=store,
        run_id=run_id,
        cells=cells,
        score_bundle=score_bundle,
        search_log=search_log,
        phase_durations=durations,
        llm_calls=llm_calls,
        tokens=tokens,
    )
    target = Path(out_dir) / sanitize_filename(run_id) / METRICS_FILENAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(metrics.to_manifest_json(), encoding="utf-8")
    return target
