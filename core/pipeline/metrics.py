"""Метрики прогона (W12/T12.1, 04 §B.2, 02 §E.1).

Собирает сводку прогона в :class:`RunMetrics` — та же форма, что строка таблицы
метрик ``run_manifest.yaml`` (04 §B.2):

* ``cells_by_state`` — ``pending``/``preliminary``/``evaluated``/``failed``/``reviewed``;
* ``failure_rate`` — ``failed / (evaluated ∪ failed)`` (R13, I5);
* ``coverage_aggregate`` — Evidence Coverage (§6);
* ``search_calls_by_phase`` — расход поискового бюджета (I23);
* ``llm_calls``/``tokens`` — стоимость (M1/§15);
* ``duration_by_phase`` — производительность.

Принципы
--------
* **Не дублируем формулы.** ``failure_rate``/``coverage_aggregate`` берутся из готового
  :class:`~core.pipeline.compute.ScoreBundle`; без бандла ``failure_rate`` считается
  доменной функцией :func:`core.domain.scoring.failure_rate`.
* **Тайминг инжектируется.** :class:`~core.pipeline.runner.PhaseResult`/``RunReport``
  (W4) намеренно **не несут** полей длительности — мы их не выдумываем. Длительности
  передаёт runner явной картой ``{phase: seconds}``; см. :func:`collect_phase_durations`.
* **Детерминизм.** Все словари возвращаются с отсортированными ключами; JSON —
  ``sort_keys=True``.

Модуль категорийно нейтрален (§1.3), зависимостей не добавляет.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from core.domain.enums import CellState
from core.domain.scoring import failure_rate as _failure_rate

__all__ = ["RunMetrics", "compute_metrics", "collect_phase_durations"]


def _value_of(raw: Any) -> str:
    """Приводит enum/строку к строковому значению (без привязки к конкретному типу)."""
    value = getattr(raw, "value", raw)
    return str(value)


def _resolve_cells(
    store: Any, run_id: str | None, cells: Iterable[Any] | None
) -> list[Any]:
    """Источник ячеек: явный список → ``store.iter_cells(run_id)`` → пусто (duck typing)."""
    if cells is not None:
        return list(cells)
    if store is not None and run_id is not None:
        return list(store.iter_cells(run_id))
    return []


def _count_cells_by_state(cells: Iterable[Any]) -> dict[str, int]:
    """Считает ячейки по состояниям; все известные состояния присутствуют даже с нулём.

    Схема стабильна для манифеста: ключи — ``CellState`` в отсортированном порядке.
    ``state`` читается duck-typing-ом (``str`` или ``CellState``).
    """
    counts: dict[str, int] = {state.value: 0 for state in CellState}
    for cell in cells:
        state = getattr(cell, "state", None)
        key = _value_of(state) if state is not None else "unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _count_search_calls(search_log: Iterable[Any] | None) -> dict[str, int]:
    """Карта ``{phase: count}`` по записям поискового лога (I23).

    Записи читаются duck-typing-ом: нужен только атрибут ``.phase`` (``str``/``SearchPhase``).
    Типы ``SearchLogEntry`` намеренно не импортируются — метрика не зависит от порта.
    """
    if search_log is None:
        return {}
    counts: dict[str, int] = {}
    for entry in search_log:
        phase = getattr(entry, "phase", None)
        if phase is None:
            continue
        key = _value_of(phase)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _normalize_tokens(tokens: Mapping[str, Any] | None) -> dict[str, int]:
    """Нормализует ``tokens`` к ``{str: int}`` с сортировкой ключей."""
    if not tokens:
        return {}
    return dict(sorted((str(key), int(value)) for key, value in tokens.items()))


def _normalize_durations(phase_durations: Mapping[str, Any] | None) -> dict[str, float]:
    """Нормализует длительности к ``{str: float}`` с сортировкой ключей."""
    if not phase_durations:
        return {}
    return dict(sorted((str(key), float(value)) for key, value in phase_durations.items()))


def _normalize_crawl(crawl_counts: Mapping[str, Any] | None) -> dict[str, int]:
    """Нормализует счётчики краула к ``{str: int}`` с сортировкой ключей (Backlog A)."""
    if not crawl_counts:
        return {}
    return dict(sorted((str(key), int(value)) for key, value in crawl_counts.items()))


def _normalize_crawl_truncation(
    crawl_truncation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Нормализует сводку усечения краула к детерминированному dict (N4).

    Отсутствие/пустота → ``{}`` (поле присутствует всегда, но нейтрально). ``roots``
    сортируются по ``(product_id, root)`` — детерминизм JSON.
    """
    if not crawl_truncation:
        return {}
    data = dict(crawl_truncation)
    roots = data.get("effective_limits", {}).get("roots") if isinstance(
        data.get("effective_limits"), Mapping
    ) else None
    if roots:
        data["effective_limits"] = {
            **dict(data["effective_limits"]),
            "roots": sorted(
                (dict(root) for root in roots),
                key=lambda r: (str(r.get("product_id", "")), str(r.get("root", ""))),
            ),
        }
    products = data.get("truncated_products")
    if products is not None:
        data["truncated_products"] = sorted({str(p) for p in products})
    return data


@dataclass
class RunMetrics:
    """Метрики прогона (04 §B.2); JSON-совместимы и детерминированы.

    :param cells_by_state: количество ячеек по состояниям (все ``CellState``).
    :param failure_rate: ``failed / (evaluated ∪ failed)`` (R13).
    :param coverage_aggregate: Evidence Coverage (0.0, если бандл не передан).
    :param search_calls_by_phase: вызовы поиска по фазам (I23).
    :param llm_calls: число вызовов LLM (стоимость, M1/§15).
    :param tokens: токены по категориям (``prompt``/``completion``/…).
    :param duration_by_phase: длительности фаз в секундах (инжектируются runner-ом).
    """

    cells_by_state: dict[str, int]
    failure_rate: float
    coverage_aggregate: float
    search_calls_by_phase: dict[str, int]
    llm_calls: int
    tokens: dict[str, int]
    duration_by_phase: dict[str, float]
    #: Backlog A (Phase 4): счётчики durable-сеанса краула (``{status: count}``) из
    #: ``crawl_session_counts``; пустая карта, если сеанса/краула нет.
    crawl: dict[str, int] = field(default_factory=dict)
    #: N4: сводка политического усечения краула (``truncated``/``truncated_reason``/
    #: ``truncated_products``/``pages_fetched``/``effective_limits``); пусто, если не усечён.
    crawl_truncation: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Плоский JSON-совместимый словарь метрик (для манифеста/логов)."""
        return {
            "cells_by_state": dict(self.cells_by_state),
            "failure_rate": self.failure_rate,
            "coverage_aggregate": self.coverage_aggregate,
            "search_calls_by_phase": dict(self.search_calls_by_phase),
            "llm_calls": self.llm_calls,
            "tokens": dict(self.tokens),
            "duration_by_phase": dict(self.duration_by_phase),
            "crawl": dict(self.crawl),
            "crawl_truncation": dict(self.crawl_truncation),
        }

    def to_manifest_json(self) -> str:
        """Детерминированный JSON метрик (``ensure_ascii=False``, ``sort_keys=True``)."""
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)


def collect_phase_durations(
    report: Any, phase_durations: Mapping[str, Any] | None = None
) -> dict[str, float]:
    """Собирает ``{phase: seconds}`` из отчёта прогона и/или явной карты.

    ``PhaseResult``/``RunReport`` (W4) **не несут** тайминга — его инжектирует runner
    явной картой. Функция также подхватывает атрибут ``duration``/``duration_s`` у
    результатов, если он когда-либо появится (без выдумывания полей и без изменения
    runner-контракта). Значения из отчёта имеют приоритет над явной картой.

    :param report: ``RunReport`` (или ``None``); читается duck-typing-ом.
    :param phase_durations: явная карта длительностей от runner-а.
    """
    durations: dict[str, float] = {}
    for result in getattr(report, "results", None) or []:
        raw = getattr(result, "duration", None)
        if raw is None:
            raw = getattr(result, "duration_s", None)
        if raw is None:
            continue
        phase = getattr(result, "phase", "")
        durations[str(phase)] = float(raw)
    for phase, seconds in (phase_durations or {}).items():
        durations.setdefault(str(phase), float(seconds))
    return dict(sorted(durations.items()))


def compute_metrics(
    *,
    store: Any = None,
    run_id: str | None = None,
    cells: Iterable[Any] | None = None,
    score_bundle: Any = None,
    search_log: Iterable[Any] | None = None,
    phase_durations: Mapping[str, Any] | None = None,
    llm_calls: int = 0,
    tokens: Mapping[str, Any] | None = None,
    crawl_counts: Mapping[str, Any] | None = None,
    crawl_truncation: Mapping[str, Any] | None = None,
) -> RunMetrics:
    """Собирает :class:`RunMetrics` прогона (04 §B.2, W12/T12.1).

    :param store: ``StorePort``; источник ячеек, если ``cells`` не передан.
    :param run_id: прогон для ``store.iter_cells``.
    :param cells: явный список ячеек (имеет приоритет над ``store``).
    :param score_bundle: готовый ``ScoreBundle``; при наличии — источник
        ``failure_rate`` и ``coverage_aggregate``.
    :param search_log: iterable записей с атрибутом ``.phase`` (duck typing).
    :param phase_durations: явная карта ``{phase: seconds}`` (тайминг инжектирует runner).
    :param llm_calls: число вызовов LLM (передаёт evaluator/runner).
    :param tokens: токены (передаёт evaluator/runner).
    :param crawl_counts: счётчики durable-сеанса краула (``crawl_session_counts``) — источник
        поля ``crawl`` (Backlog A). Вызывающий читает их у стора (least coupling: метрика
        не знает соглашения об ``session_id``). ``None`` → пустая карта.
    :param crawl_truncation: сводка политического усечения краула (N4) — источник поля
        ``crawl_truncation`` (:func:`core.pipeline.crawl_session.truncation_summary``).
        ``None`` → пустой dict.
    """
    resolved = _resolve_cells(store, run_id, cells)
    if score_bundle is not None:
        failure = float(score_bundle.failure_rate)
        coverage = float(score_bundle.coverage.value)
    else:
        failure = float(_failure_rate(resolved))
        coverage = 0.0
    return RunMetrics(
        cells_by_state=_count_cells_by_state(resolved),
        failure_rate=failure,
        coverage_aggregate=coverage,
        search_calls_by_phase=_count_search_calls(search_log),
        llm_calls=int(llm_calls),
        tokens=_normalize_tokens(tokens),
        duration_by_phase=_normalize_durations(phase_durations),
        crawl=_normalize_crawl(crawl_counts),
        crawl_truncation=_normalize_crawl_truncation(crawl_truncation),
    )
