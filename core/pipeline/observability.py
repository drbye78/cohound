"""Наблюдаемость прогона: инструментирование фаз §B.1 и запись метрик §B.2 (W12/T12.1).

Модуль — **точка сочленения** наблюдаемости, не меняющая контракт ``Runner``/``RunContext``
(W4). Он оборачивает реестр ``{phase: PhaseFn}`` так, что каждая фаза:

* пишет ``INFO``-запись о старте и ``INFO``/``WARN``-запись о завершении в
  ``runs/{run_id}/logs/{phase}.jsonl`` через :class:`~core.util.logging.PhaseLogger`
  (поля §B.1: ``ts``/``run_id``/``phase``/``component``/``level``/``message``/``ctx``;
  ``ctx`` несёт ``status``/``cells``/``errors``). ``WARN`` — только на ``blocked``/``failed``;
  ``skipped`` — легитимное «работы не было», поэтому ``INFO``;
* измеряет длительность ``time.perf_counter`` (карта ``{phase: seconds}``);
* возвращает **исходный** :class:`~core.pipeline.runner.PhaseResult` без мутаций
  (``status``/``cells``/``errors`` не переписываются).

**Словарь фаз (P2a/P2b).** Запись завершения несёт ``status`` и — ортогонально —
``skip_reason`` (машинная причина при ``skipped``, ``None`` иначе) и ``pending``
(число незакрытых для человека пунктов) + ``pending_items`` (сам список, ограничен фазой).
Самодокументирующая заметка о предполёте живёт в ``log_entries`` фазы, а не в ``ctx``.

**Политика пропуска (N3).** ``skipped`` при отсутствии предпосылки — **не** аномалия:
он логируется на ``INFO`` (``WARN`` остаётся за ``blocked``/``failed``). Пропуск, который
«морит» нижележащие фазы (напр. пустой корпус → ``evaluate`` без входов), не эскалируется
до ``WARN``: любая такая фаза обязана сама быть precondition-guarded и выдать свой
``skipped``/``ok``, а не выдумать результат. Run-level сводка пропусков выносится
**не** в лог, а в CLI-JSON отчёт (``_run_report_payload``: ``skip_summary``) — лог на фазу
остаётся точным, а оператор видит «что не отработало» в одном месте.

Метрики прогона (04 §B.2) собираются :func:`write_run_metrics` в
``runs/{run_id}/metrics.json`` — детерминированный JSON (``sort_keys=True``,
``ensure_ascii=False``), в том числе при остановке на гейте (метрики отражают то, что
реально отработало). Блок ``resources`` (F0) приходит готовым из
:func:`core.pipeline.resources.build_resources_block`; здесь же измеряется пик RSS на
конец каждой фазы (``VmHWM``).

**Стоимость LLM накапливается через файл.** ``usage_summary()`` адаптера живёт ровно один
процесс, а прогон продолжается заходами (``--until corpus`` → продолжение). Поэтому
:func:`write_run_metrics` перед записью читает **существующий** ``metrics.json`` и берёт
его ``llm_usage``/``llm_calls``/``tokens`` как вклад предыдущих процессов, а
``llm_calls``/``tokens`` аргументы — как вклад текущего
(:func:`core.pipeline.metrics.merge_llm_usage`). Схема БД при этом не меняется: файл —
носитель накопления. Граница названа в ``llm_usage.accounting``: заход, убитый до записи
метрик, свой вклад теряет, и ``processes`` делает это вычислимым.

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

import os
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.pipeline.metrics import (
    compute_metrics,
    merge_llm_usage,
    read_prior_llm_usage,
)
from core.pipeline.resources import read_peak_rss_mb
from core.pipeline.runner import Clock, PhaseFn, PhaseResult, RunContext
from core.util.logging import PhaseLogger
from core.util.security import sanitize_filename

__all__ = [
    "InstrumentedPhases",
    "LlmUsageSnapshot",
    "collect_llm_usage",
    "instrument_phases",
    "write_run_metrics",
]

#: Имя файла метрик прогона (04 §B.2).
METRICS_FILENAME = "metrics.json"


@dataclass(frozen=True)
class LlmUsageSnapshot:
    """Вклад **текущего** процесса в стоимость LLM (M1/§15).

    :param calls: вызовы LLM за процесс (``usage_summary()["calls"]``).
    :param tokens: токены за процесс по категориям.
    :param source: откуда вклад — ``adapter`` (адаптер посчитал), ``absent``
        (``usage_summary`` нет: шов/иной адаптер) или ``error`` (метод есть, но сбой).
        Различает «ноль вызовов» и «расход неизвестен»: без этого различия метрика снова
        лгала бы формой.
    :param error: текст сбоя получения вклада (``None``, если сбоя не было).
    """

    calls: int = 0
    tokens: dict[str, int] = field(default_factory=dict)
    source: str = "absent"
    error: str | None = None


def collect_llm_usage(evaluator: Any) -> LlmUsageSnapshot:
    """Снимает вклад процесса с адаптера оценщика (duck typing, без импорта адаптеров).

    Отсутствие ``usage_summary`` — не ошибка (шов/иной адаптер): вклад нулевой и
    помечен ``source="absent"``, то есть «не умеем считать», а не «вызовов не было».
    Сбой самого метода тоже не роняет прогон (наблюдаемость вторична), но публикуется
    как ``source="error"`` с текстом.

    :param evaluator: порт оценщика (``ports.evaluator``) или ``None``.
    """
    summary = getattr(evaluator, "usage_summary", None)
    if not callable(summary):
        return LlmUsageSnapshot()
    try:
        raw = summary()
    except Exception as exc:  # noqa: BLE001 — наблюдаемость вторична к прогону
        return LlmUsageSnapshot(source="error", error=f"{type(exc).__name__}: {exc}")
    if not isinstance(raw, Mapping):
        return LlmUsageSnapshot(
            source="error",
            error=f"usage_summary вернул {type(raw).__name__}, не объект",
        )
    try:
        calls = int(raw.get("calls", 0) or 0)
    except (TypeError, ValueError) as exc:
        return LlmUsageSnapshot(
            source="error", error=f"usage.calls: {type(exc).__name__}: {exc}"
        )
    tokens_raw = raw.get("tokens") or {}
    tokens: dict[str, int] = {}
    if isinstance(tokens_raw, Mapping):
        for key, value in tokens_raw.items():
            try:
                tokens[str(key)] = int(value)
            except (TypeError, ValueError) as exc:
                return LlmUsageSnapshot(
                    source="error",
                    error=f"usage.tokens.{key}: {type(exc).__name__}: {exc}",
                )
    return LlmUsageSnapshot(
        calls=calls if calls >= 0 else 0,
        tokens=dict(sorted(tokens.items())),
        source="adapter",
    )



@dataclass
class InstrumentedPhases:
    """Результат :func:`instrument_phases`: обёрнутые фазы, тайминг и логгеры.

    :param phases: новый словарь ``{phase: PhaseFn}`` — обёртки, делегирующие в оригинал.
    :param durations: карта ``{phase: seconds}`` (``time.perf_counter``), заполняется по мере
        исполнения обёрнутых фаз.
    :param peak_rss_mb: карта ``{phase: peak MB}`` — ``VmHWM`` процесса на **конец** фазы
        (F0). ``VmHWM`` кумулятивен, поэтому значение фазы — максимум по всем фазам до неё,
        а не «сколько съела эта фаза»; дельты из high-water mark не выводятся. Ключ
        отсутствует, если пик прочитать не удалось (метрика не выдумывается).
    """

    phases: dict[str, PhaseFn] = field(default_factory=dict)
    durations: dict[str, float] = field(default_factory=dict)
    peak_rss_mb: dict[str, float] = field(default_factory=dict)
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


def _note_peak(instrumented: InstrumentedPhases, phase: str) -> None:
    """Фиксирует пик RSS на конец фазы; сбой чтения — не ошибка прогона (F0).

    ``VmHWM`` кумулятивен: значение на конец фазы — максимум по всем фазам до неё. Это
    ожидаемое поведение метрики, а не дефект: «сколько процесс занимал к этому моменту»
    отвечает на вопрос «помещается ли прогон», а «дельта фазы» из high-water mark не
    выводится вовсе.
    """
    try:
        peak = read_peak_rss_mb()
    except Exception as exc:  # noqa: BLE001 — наблюдаемость вторична к прогону
        instrumented._errors.append(
            f"{phase}: peak_rss: {type(exc).__name__}: {exc}"
        )
        return
    if peak is not None:
        instrumented.peak_rss_mb[phase] = peak


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
            _note_peak(instrumented, phase)
            _safe_log(
                instrumented,
                phase,
                "ERROR",
                "фаза завершилась исключением",
                {"event": "error", "error": type(exc).__name__},
            )
            raise
        instrumented.durations[phase] = time.perf_counter() - started
        _note_peak(instrumented, phase)
        # ``skipped`` — легитимное «работы не было» (не аномалия): INFO, как ``ok``.
        # WARN остаётся за ``blocked``/``failed``.
        level = "INFO" if result.status in ("ok", "skipped") else "WARN"
        pending = list(getattr(result, "pending", None) or ())
        skip_reason = getattr(result, "skip_reason", None)
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
                # Причина пропуска (машиночитаемо) и объём незакрытых пунктов для человека
                # (advisory ``pending``); сам список — ``pending_items`` (ограничен фазой).
                "skip_reason": skip_reason.value if skip_reason is not None else None,
                "pending": len(pending),
                "pending_items": pending,
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
    :param logs_root: корень логов (по умолчанию ``runs``; composition root передаёт
        resolved ``paths.runs``, ``03`` §A.1). Дефолт совпадает с :class:`PathsConfig`.
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
    llm_usage_source: str = "adapter",
    llm_usage_error: str | None = None,
    crawl_counts: Mapping[str, Any] | None = None,
    crawl_truncation: Mapping[str, Any] | None = None,
    crawl_session: Mapping[str, Any] | None = None,
    resources: Mapping[str, Any] | None = None,
) -> Path:
    """Считает :class:`~core.pipeline.metrics.RunMetrics` и пишет ``runs/{run_id}/metrics.json``.

    Метрики (04 §B.2) собираются готовой :func:`~core.pipeline.metrics.compute_metrics`
    (формулы не дублируются). JSON детерминирован (``sort_keys=True``,
    ``ensure_ascii=False``) через :meth:`RunMetrics.to_manifest_json`.

    **Накопление стоимости LLM.** ``llm_calls``/``tokens`` — вклад **этого** процесса;
    перед записью читается уже существующий ``metrics.json`` и его ``llm_usage`` (или, для
    файлов прежнего формата, ``llm_calls``/``tokens``) берётся как вклад предыдущих
    процессов. Итог пишется в ``llm_calls``/``tokens`` (накопленное) и в блок ``llm_usage``
    (:func:`~core.pipeline.metrics.merge_llm_usage`) с разбивкой и числом заходов. Поэтому
    функция **не идемпотентна по вкладу процесса**: два вызова за один процесс посчитают
    вклад дважды. В прогоне call-site один (``core/cli/commands/run.py``, метрики пишутся
    ровно один раз после исполнения фаз); это проверяется тестом, а не обещанием.

    Запись атомарна (временный файл + ``os.replace``): убитый посреди записи процесс
    оставляет прежний ``metrics.json`` целым, а не полфайла — иначе следующий заход читал бы
    битый файл и терял накопленное.

    :param run_id: идентификатор прогона.
    :param out_dir: корень прогонов (по умолчанию ``runs``; composition root передаёт
        resolved ``paths.runs``, ``03`` §A.1); файл — ``{out_dir}/{run_id}/metrics.json``.
    :param durations: карта ``{phase: seconds}`` (тайминг инжектирует
        :class:`InstrumentedPhases`); служит источником ``duration_by_phase``.
    :param store: ``StorePort``; источник ячеек (``iter_cells``), если ``cells`` не передан.
    :param cells: явный список ячеек (приоритетнее ``store``).
    :param score_bundle: готовый ``ScoreBundle`` (``failure_rate``/``coverage_aggregate``).
    :param search_log: iterable записей с атрибутом ``.phase`` (I23).
    :param llm_calls: вызовы LLM **текущего процесса** (``usage_summary()["calls"]``).
    :param tokens: токены **текущего процесса** по категориям (``prompt``/``completion``/…).
    :param llm_usage_source: как получен вклад процесса — ``adapter`` | ``absent`` |
        ``error`` (:class:`LlmUsageSnapshot`); виден в ``llm_usage.current_source``.
    :param llm_usage_error: текст сбоя получения вклада (``None``, если сбоя не было).
    :param crawl_counts: счётчики durable-сеанса краула (Backlog A) — источник поля
        ``crawl`` метрик; ``None`` → пустая карта.
    :param crawl_truncation: сводка политического усечения краула (N4) — источник поля
        ``crawl_truncation``; ``None`` → пустой dict (не выдумываем усечение).
    :param crawl_session: жизненный цикл сеанса краула (F1/Д-6) — источник поля
        ``crawl_session`` (``status``/``owner``/``heartbeat_at``/``stale_reason``);
        ``None`` → пустой dict.
    :param resources: готовый блок ``resources`` (F0) —
        :func:`core.pipeline.resources.build_resources_block`; ``None`` → пустой dict
        (поле присутствует всегда, но нейтрально: «не измеряли» ≠ «ноль»).
    :returns: путь записанного ``metrics.json``.
    """
    target = Path(out_dir) / sanitize_filename(run_id) / METRICS_FILENAME
    # Накопление через файл: схема БД не меняется, носитель — сам ``metrics.json``.
    # Битый/нечитаемый файл не роняет прогон: предыдущее считается нулём, но факт виден
    # в ``llm_usage.previous_source``/``previous_error``.
    usage = merge_llm_usage(
        read_prior_llm_usage(target),
        calls_this_process=llm_calls,
        tokens_this_process=tokens,
        current_source=llm_usage_source,
        current_error=llm_usage_error,
    )
    metrics = compute_metrics(
        store=store,
        run_id=run_id,
        cells=cells,
        score_bundle=score_bundle,
        search_log=search_log,
        phase_durations=durations,
        # ``llm_calls``/``tokens`` верхнего уровня — накопленные: внешние потребители
        # читают их и раньше, поэтому смысл поля сохранён, а разбивка — в ``llm_usage``.
        llm_calls=int(usage["calls_cumulative"]),
        tokens=usage["tokens_cumulative"],
        llm_usage=usage,
        crawl_counts=crawl_counts,
        crawl_truncation=crawl_truncation,
        crawl_session=crawl_session,
        resources=resources,
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    # Атомарная замена: прежний файл остаётся целым, если процесс убьют посреди записи.
    staging = target.with_name(f"{METRICS_FILENAME}.{os.getpid()}.tmp")
    staging.write_text(metrics.to_manifest_json(), encoding="utf-8")
    os.replace(staging, target)
    return target
