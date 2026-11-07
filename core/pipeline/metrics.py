"""Метрики прогона (W12/T12.1, 04 §B.2, 02 §E.1).

Собирает сводку прогона в :class:`RunMetrics` — та же форма, что строка таблицы
метрик ``run_manifest.yaml`` (04 §B.2):

* ``cells_by_state`` — ``pending``/``preliminary``/``evaluated``/``failed``/``reviewed``;
* ``failure_rate`` — ``failed / (evaluated ∪ failed)`` (R13, I5);
* ``coverage_aggregate`` — Evidence Coverage (§6);
* ``search_calls_by_phase`` — расход поискового бюджета (I23);
* ``llm_calls``/``tokens`` — стоимость прогона (M1/§15): **накопленная** по всем процессам
  заходам (см. ниже);
* ``llm_usage`` — та же стоимость с разбивкой «прогон / текущий процесс» и явной границей
  учёта (:func:`merge_llm_usage`);
* ``duration_by_phase`` — производительность;
* ``resources`` — F0: пики RSS по фазам, расписание экстракции, чанки на документ,
  пик воркера и бюджет памяти (:mod:`core.pipeline.resources`).

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
* **Стоимость прогона ≠ стоимость процесса.** ``usage_summary()`` адаптера LLM живёт ровно
  один процесс, поэтому накопление идёт через уже записанный ``metrics.json``
  (:func:`read_prior_llm_usage` + :func:`merge_llm_usage`). Без этого продолженный прогон
  публиковал в машинном отчёте число вызовов **последнего** процесса — «стоимость
  фрагмента» под видом стоимости прогона.

Модуль категорийно нейтрален (§1.3), зависимостей не добавляет.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.domain.enums import CellState
from core.domain.scoring import failure_rate as _failure_rate

__all__ = [
    "LLM_USAGE_ACCOUNTING",
    "PriorLlmUsage",
    "RunMetrics",
    "collect_phase_durations",
    "compute_metrics",
    "merge_llm_usage",
    "read_prior_llm_usage",
]

#: Семантика поля ``llm_usage.accounting``.
#:
#: Накопление считает **заходы, дошедшие до записи** ``metrics.json``. Процесс, убитый
#: (OOM/сигнал/обрыв) до записи метрик, свой вклад теряет: его вызовы LLM уже оплачены, но
#: в файл не попали. Это названная граница, а не незамеченная погрешность — поэтому
#: ``processes`` публикуется: читатель видит, сколько заходов учтено, и не принимает
#: накопленное за полную стоимость многосуточного прогона, продолженного десяток раз.
LLM_USAGE_ACCOUNTING = "processes_that_wrote"


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


def _count_cells_by_failure_kind(
    store: Any, run_id: str | None, cells: Iterable[Any]
) -> dict[str, int]:
    """Считает ячейки **без вердикта** по классу сбоя (``cells.failure_kind``).

    Источник — store, а не пересчёт по журналу вызовов: класс живёт в колонке
    (``read_cell_failure_kinds``), и метрика обязана называть то же, что читает
    селектор ремонта и отчёт. ``unrecorded`` — ячейки ``failed`` без записанного класса
    (так выглядят прогоны до правки write-site): «класс не записан» ≠ «сбоя не было»,
    поэтому нулевое значение публикуется всегда.

    Пустая карта — «не считали»: store не умеет читать класс либо чтение не удалось.
    Ноль здесь выдал бы неизвестность за факт.
    """
    if store is None or run_id is None:
        return {}
    reader = getattr(store, "read_cell_failure_kinds", None)
    if not callable(reader):
        return {}
    try:
        kinds = dict(reader(str(run_id)))
    except Exception:  # noqa: BLE001 — метрика не роняет прогон
        return {}
    counts: dict[str, int] = {}
    for cell in cells:
        if _value_of(getattr(cell, "state", None)) != CellState.FAILED.value:
            continue
        kind = kinds.get(
            (str(getattr(cell, "product_id", "")), str(getattr(cell, "param_id", "")))
        )
        label = kind or "unrecorded"
        counts[label] = counts.get(label, 0) + 1
    counts.setdefault("unrecorded", 0)
    return dict(sorted(counts.items()))


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


def _coerce_count(value: Any) -> int | None:
    """Приводит счётчик к неотрицательному ``int``; ``None`` — значение непригодно.

    ``bool`` отвергается: ``true`` в поле расхода — испорченный файл, а не «один вызов».
    Строка-число принимается: JSON от внешних инструментов бывает снисходительным к типам,
    а нам важна величина, не происхождение.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if not value.is_integer() or value < 0:
            return None
        return int(value)
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def _coerce_tokens(raw: Any, notes: list[str], field_name: str) -> dict[str, int]:
    """Нормализует карту токенов из файла; непригодные записи считаются нулём с заметкой.

    Метрика — не измерение: испорченное поле не повод уронить прогон, но и не повод молча
    выдать ноль. Всё, что не удалось прочитать, попадает в ``notes`` и публикуется в
    ``llm_usage.previous_error``.
    """
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        notes.append(f"{field_name}: ожидался объект, получено {type(raw).__name__}")
        return {}
    tokens: dict[str, int] = {}
    for key, value in raw.items():
        count = _coerce_count(value)
        if count is None:
            notes.append(f"{field_name}.{key}: непригодное значение {value!r} → 0")
            continue
        tokens[str(key)] = tokens.get(str(key), 0) + count
    return dict(sorted(tokens.items()))


@dataclass(frozen=True)
class PriorLlmUsage:
    """Расход LLM, накопленный **предыдущими** процессами прогона (M1/§15).

    :param calls: суммарные вызовы предыдущих процессов (0, если неизвестно).
    :param tokens: токены по категориям — сумма предыдущих процессов.
    :param processes: число заходов, чей вклад попал в файл. Для ``metrics.json``,
        записанного до накопления (нет блока ``llm_usage``), — 1, если в нём есть
        ненулевой расход, иначе 0: точное число заходов там не восстановить, поэтому
        значение — нижняя граница, а не догадка о факте.
    :param source: ``none`` (файла нет) | ``ok`` | ``unreadable`` (есть, но не разобран).
    :param error: текст проблемы чтения/разбора (``None``, если проблемы не было).
    """

    calls: int = 0
    tokens: dict[str, int] = field(default_factory=dict)
    processes: int = 0
    source: str = "none"
    error: str | None = None


def read_prior_llm_usage(path: str | Path) -> PriorLlmUsage:
    """Читает накопленный расход LLM из уже записанного ``metrics.json``.

    Отсутствие файла — нормальный первый заход (``source="none"``, всё по нулям). Битый или
    нечитаемый файл — ``source="unreadable"``: предыдущее считается нулём (продолжать
    прогон важнее, чем угадать цену), но факт публикуется в ``llm_usage.previous_error``,
    чтобы «ноль из-за сбоя чтения» не выглядел как «ноль вызовов».

    :param path: путь к ``runs/{run_id}/metrics.json``.
    """
    target = Path(path)
    try:
        raw = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return PriorLlmUsage()
    except (OSError, UnicodeDecodeError) as exc:
        return PriorLlmUsage(
            source="unreadable", error=f"{type(exc).__name__}: {exc}"
        )
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return PriorLlmUsage(
            source="unreadable", error=f"{type(exc).__name__}: {exc}"
        )
    if not isinstance(data, Mapping):
        return PriorLlmUsage(
            source="unreadable",
            error=f"корень metrics.json — {type(data).__name__}, не объект",
        )

    notes: list[str] = []
    block = data.get("llm_usage")
    if isinstance(block, Mapping):
        calls = _coerce_count(block.get("calls_cumulative"))
        if calls is None:
            notes.append("llm_usage.calls_cumulative непригоден → 0")
            calls = 0
        tokens = _coerce_tokens(
            block.get("tokens_cumulative"), notes, "llm_usage.tokens_cumulative"
        )
        processes = _coerce_count(block.get("processes"))
        if processes is None:
            notes.append("llm_usage.processes непригоден → 0")
            processes = 0
    else:
        # Формат до накопления: ``llm_calls``/``tokens`` описывают ОДИН заход — тот, что
        # писал файл последним. Это ровно та неполнота, которую мы закрываем, поэтому
        # прочитанное берётся как вклад одного захода, а не как «весь прогон».
        calls = _coerce_count(data.get("llm_calls"))
        if calls is None:
            notes.append("llm_calls непригоден → 0")
            calls = 0
        tokens = _coerce_tokens(data.get("tokens"), notes, "tokens")
        processes = 1 if (calls > 0 or tokens) else 0

    return PriorLlmUsage(
        calls=calls,
        tokens=tokens,
        processes=processes,
        source="ok",
        error="; ".join(notes) if notes else None,
    )


def merge_llm_usage(
    prior: PriorLlmUsage,
    *,
    calls_this_process: int = 0,
    tokens_this_process: Mapping[str, Any] | None = None,
    current_source: str = "adapter",
    current_error: str | None = None,
) -> dict[str, Any]:
    """Накопленный блок ``llm_usage``: заход + предыдущие процессы (M1/§15).

    Считает **в одном месте** (здесь), а не в вызывающем: ``calls_cumulative`` =
    ``prior.calls`` + вклад процесса, токены — по категориям. ``processes`` —
    число заходов, дошедших до записи (``LLM_USAGE_ACCOUNTING``): заход, убитый до записи
    метрик, свой вклад теряет, и это видно по числу.

    :param prior: прочитанное предыдущее состояние (:func:`read_prior_llm_usage`).
    :param calls_this_process: вызовы LLM за текущий процесс (``usage_summary()["calls"]``).
    :param tokens_this_process: токены текущего процесса по категориям.
    :param current_source: как получен вклад процесса — ``adapter`` (адаптер отдал
        ``usage_summary``) | ``absent`` (шов/адаптер не умеет считать) | ``error``
        (метод есть, но сбой). Различает «ноль» и «неизвестно», иначе метрика врёт формой.
    :param current_error: текст сбоя получения вклада (``None``, если сбоя не было).
    """
    calls = _coerce_count(calls_this_process)
    if calls is None:
        calls = 0
    tokens = _coerce_tokens(tokens_this_process, [], "tokens")
    cumulative = dict(prior.tokens)
    for key, value in tokens.items():
        cumulative[key] = cumulative.get(key, 0) + value
    return {
        "calls_cumulative": prior.calls + calls,
        "calls_this_process": calls,
        "tokens_cumulative": dict(sorted(cumulative.items())),
        "tokens_this_process": tokens,
        "processes": prior.processes + 1,
        "accounting": LLM_USAGE_ACCOUNTING,
        "current_source": str(current_source),
        "current_error": current_error,
        "previous_source": prior.source,
        "previous_error": prior.error,
    }


def _normalize_llm_usage(llm_usage: Mapping[str, Any] | None) -> dict[str, Any]:
    """Нормализует готовый блок ``llm_usage`` к детерминированному JSON-совместимому dict.

    Блок собирает :func:`merge_llm_usage`; здесь только гарантия формы (сортировка карт
    токенов). Пусто — блок не передан (``compute_metrics`` вызван напрямую): тогда поля нет
    смысла выдумывать, и в отчёте остаётся пустая карта — «не считали».
    """
    if not llm_usage:
        return {}
    data = dict(llm_usage)
    for key in ("tokens_cumulative", "tokens_this_process"):
        tokens = data.get(key)
        if isinstance(tokens, Mapping):
            data[key] = dict(sorted((str(k), int(v)) for k, v in tokens.items()))
    return data


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


def _normalize_crawl_session(
    crawl_session: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Нормализует блок жизненного цикла сеанса краула к детерминированному dict (F1).

    Отсутствие блока → ``{}`` (поле присутствует всегда, но нейтрально: durable-краул мог
    быть выключен). Значения не пересчитываются — метрика не выводит статус из счётчиков:
    ``interrupted`` ставит тот, кто знает факт прерывания, а не арифметика.
    """
    if not crawl_session:
        return {}
    return _round_floats(dict(crawl_session))


def _round_floats(value: Any, *, digits: int = 1) -> Any:
    """Рекурсивно округляет float до ``digits`` знаков (без «float-мусора» в метриках).

    Целые, строки и ``None`` возвращаются как есть: округление — про дробные секунды и
    MB, а не про счётчики. Списки сохраняют порядок (он может быть смыслом), словари
    сортируются по ключу — детерминизм JSON (§R7).

    ``digits`` — политика **блока**: по умолчанию 1 знак (ресурсы/секунды), но темп
    (``resources.crawl``) публикуется с 3 знаками: ``requests_per_s = 0.048`` при
    округлении до одного знака превратилось бы в ``0.0`` — то есть в «не измеряли».
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return round(value, digits)
    if isinstance(value, Mapping):
        return {str(key): _round_floats(value[key], digits=digits) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_round_floats(item, digits=digits) for item in value]
    return value


def _normalize_resources(resources: Mapping[str, Any] | None) -> dict[str, Any]:
    """Нормализует блок ``resources`` (F0) к детерминированному JSON-совместимому dict.

    Блок собирается :func:`core.pipeline.resources.build_resources_block`; здесь только
    гарантия формы: отсортированные ключи и округление до 1 знака. Отсутствие блока —
    пустой dict (поле присутствует всегда, но нейтрально: «не измеряли» ≠ «ноль»).

    Подблок ``crawl`` (темп обхода) округляется до 6 знаков: частота запросов дробная
    по своей природе, а окно бывает короче секунды — политика 1 знака стёрла бы и то,
    и другое (см. :func:`_round_floats`).
    """
    if not resources:
        return {}
    data = dict(resources)
    pacing = data.pop("crawl", None)
    normalized = _round_floats(data)
    if isinstance(pacing, Mapping):
        normalized["crawl"] = _round_floats(dict(pacing), digits=6)
    return dict(sorted(normalized.items()))


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
    :param cells_by_failure_kind: ячейки **без вердикта** по классу сбоя
        (``cells.failure_kind``); ``unrecorded`` — класс не записан. Пусто — не читали.
    :param failure_rate: ``failed / (evaluated ∪ failed)`` (R13).
    :param coverage_aggregate: Evidence Coverage (0.0, если бандл не передан).
    :param search_calls_by_phase: вызовы поиска по фазам (I23).
    :param llm_calls: число вызовов LLM (стоимость, M1/§15) — **накопленное** по всем
        заходам прогона; разбивка «прогон / процесс» — в :attr:`llm_usage`.
    :param tokens: токены по категориям (``prompt``/``completion``/…), накопленные.
    :param duration_by_phase: длительности фаз в секундах (инжектируются runner-ом).
    :param llm_usage: блок :func:`merge_llm_usage` — накопленное и вклад процесса рядом,
        плюс источник вклада и граница учёта (``accounting``).
    """

    cells_by_state: dict[str, int]
    failure_rate: float
    coverage_aggregate: float
    search_calls_by_phase: dict[str, int]
    llm_calls: int
    tokens: dict[str, int]
    duration_by_phase: dict[str, float]
    #: Ячейки **без вердикта** по классу сбоя (``{"evaluation_failed": 312,
    #: "no_sources": 362, "unrecorded": 0}``); ``unrecorded`` — класс не записан.
    #: Пусто — store не умеет читать класс (неизвестность, а не ноль).
    cells_by_failure_kind: dict[str, int] = field(default_factory=dict)
    #: M1/§15: стоимость LLM с разбивкой «накоплено прогоном / вклад этого процесса»,
    #: источником вклада (``adapter``/``absent``/``error``) и границей учёта
    #: (``accounting``). Пусто — блок не передан (прямой вызов ``compute_metrics``).
    llm_usage: dict[str, Any] = field(default_factory=dict)
    #: Backlog A (Phase 4): счётчики durable-сеанса краула (``{status: count}``) из
    #: ``crawl_session_counts``; пустая карта, если сеанса/краула нет.
    crawl: dict[str, int] = field(default_factory=dict)
    #: N4: сводка политического усечения краула (``truncated``/``truncated_reason``/
    #: ``truncated_products``/``pages_fetched``/``effective_limits``); пусто, если не усечён.
    crawl_truncation: dict[str, Any] = field(default_factory=dict)
    #: F1/Д-6: жизненный цикл сеанса краула (``status``/``owner``/``heartbeat_at``/
    #: ``stale_reason``) — чтобы ``interrupted`` был виден в ``metrics.json`` без разбора БД.
    #: Пусто — сеанса не было (durable-краул выключен); поле присутствует всегда.
    crawl_session: dict[str, Any] = field(default_factory=dict)
    #: F0: блок ``resources`` — пики RSS по фазам, разрешённое расписание экстракции,
    #: распределение чанков на документ по семействам, пик воркера и бюджет памяти.
    #: Пусто — ресурсы не измерялись (поле присутствует всегда, но нейтрально).
    resources: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Плоский JSON-совместимый словарь метрик (для манифеста/логов)."""
        return {
            "cells_by_state": dict(self.cells_by_state),
            "cells_by_failure_kind": dict(self.cells_by_failure_kind),
            "failure_rate": self.failure_rate,
            "coverage_aggregate": self.coverage_aggregate,
            "search_calls_by_phase": dict(self.search_calls_by_phase),
            "llm_calls": self.llm_calls,
            "tokens": dict(self.tokens),
            "llm_usage": dict(self.llm_usage),
            "duration_by_phase": dict(self.duration_by_phase),
            "crawl": dict(self.crawl),
            "crawl_truncation": dict(self.crawl_truncation),
            "crawl_session": dict(self.crawl_session),
            "resources": dict(self.resources),
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
    llm_usage: Mapping[str, Any] | None = None,
    crawl_counts: Mapping[str, Any] | None = None,
    crawl_truncation: Mapping[str, Any] | None = None,
    crawl_session: Mapping[str, Any] | None = None,
    resources: Mapping[str, Any] | None = None,
) -> RunMetrics:
    """Собирает :class:`RunMetrics` прогона (04 §B.2, W12/T12.1).

    :param store: ``StorePort``; источник ячеек, если ``cells`` не передан.
    :param run_id: прогон для ``store.iter_cells``.
    :param cells: явный список ячеек (имеет приоритет над ``store``).
    :param score_bundle: готовый ``ScoreBundle``; при наличии — источник
        ``failure_rate`` и ``coverage_aggregate``.
    :param search_log: iterable записей с атрибутом ``.phase`` (duck typing).
    :param phase_durations: явная карта ``{phase: seconds}`` (тайминг инжектирует runner).
    :param llm_calls: накопленное число вызовов LLM (передаёт runner/CLI после слияния с
        предыдущими заходами; формула накопления — :func:`merge_llm_usage`).
    :param tokens: накопленные токены (передаёт runner/CLI).
    :param llm_usage: блок :func:`merge_llm_usage` (накопленное + вклад процесса + граница
        учёта). ``None`` → пустая карта: «не считали», а не выдуманные нули.
    :param crawl_counts: счётчики durable-сеанса краула (``crawl_session_counts``) — источник
        поля ``crawl`` (Backlog A). Вызывающий читает их у стора (least coupling: метрика
        не знает соглашения об ``session_id``). ``None`` → пустая карта.
    :param crawl_truncation: сводка политического усечения краула (N4) — источник поля
        ``crawl_truncation`` (:func:`core.pipeline.crawl_session.truncation_summary``).
        ``None`` → пустой dict.
    :param crawl_session: жизненный цикл сеанса краула (F1/Д-6) — источник поля
        ``crawl_session`` (``status``/``owner``/``heartbeat_at``/``stale_reason``).
        Вызывающий читает сеанс у стора; ``None`` → пустой dict («сеанса не было»).
    :param resources: блок ``resources`` (F0) — источник одноимённого поля
        (:func:`core.pipeline.resources.build_resources_block`). ``None`` → пустой dict.
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
        cells_by_failure_kind=_count_cells_by_failure_kind(store, run_id, resolved),
        failure_rate=failure,
        coverage_aggregate=coverage,
        search_calls_by_phase=_count_search_calls(search_log),
        llm_calls=int(llm_calls),
        tokens=_normalize_tokens(tokens),
        llm_usage=_normalize_llm_usage(llm_usage),
        duration_by_phase=_normalize_durations(phase_durations),
        crawl=_normalize_crawl(crawl_counts),
        crawl_truncation=_normalize_crawl_truncation(crawl_truncation),
        crawl_session=_normalize_crawl_session(crawl_session),
        resources=_normalize_resources(resources),
    )
