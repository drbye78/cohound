"""Бюджет внешнего поиска и ``search_log`` — W11/T11.2 (§4.4/§5, I16/I19/I23).

Модуль даёт три вещи:

* :class:`SearchBudget` — фиксированные потолки внешних поисковых вызовов **по фазам**
  (``discovery``/``corpus``/``serp``/``scout``), разбираемые из
  ``run_config.search_effort_budget_json`` или обычного mapping (§5 «SERP-бюджет»,
  I23: бюджет фиксирован и залогирован для каждого продукта).
* :class:`SearchEffort` — счётчик израсходованного усилия **на (фаза, продукт)**.
  Ключ включает ``product_id``, поэтому исчерпание бюджета одним продуктом не
  «съедает» усилие другого: симметрия бюджета (I16).
* :func:`external_search` — ограниченный бюджетом одиночный вызов ``adapter.search``
  с записью в ``search_log``. Возвращает **только кандидатов** :class:`SearchResult`,
  никогда не evidence (I19): превращение внешнего контента в доказательство возможно
  лишь после Ш5 (``corpus_raw`` → extract → chunk → ``content_hash`` → index).

Протокол исчерпания §5 кодируется :func:`termination_for`
(``budget_exhausted``/``results_count``/``sufficient``/``error`` → ``TerminationReason``).

Модуль категорийно нейтрален (§1.3), новых зависимостей не добавляет. Часы для ``ts``
инжектируются (по умолчанию — UTC ISO), поэтому записи детерминированы.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from core.domain.enums import (
    AdapterKind,
    SearchKind,
    SearchPhase,
    TerminationReason,
)
from core.domain.models import SearchLogEntry
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult

__all__ = [
    "SearchBudget",
    "SearchEffort",
    "record_search",
    "termination_for",
    "external_search",
]

#: Ключи mapping-бюджета, задающие «потолок по умолчанию» для фаз без явного лимита.
_DEFAULT_KEYS = frozenset({"default", "*", "all"})


def _utcnow() -> str:
    """Текущее время в UTC ISO-8601 (дефолтные часы для :func:`record_search`)."""
    return datetime.now(timezone.utc).isoformat()


def _as_enum(enum_cls: type[Any], value: Any) -> Any:
    """Приводит значение к члену перечисления; ``None`` пропускает как есть."""
    if value is None or isinstance(value, enum_cls):
        return value
    return enum_cls(str(value))


def _phase_value(phase: SearchPhase | str) -> str:
    """Строковое значение фазы, устойчивое к «чужим» строкам (lenient-ключ бюджета)."""
    if isinstance(phase, SearchPhase):
        return phase.value
    try:
        return SearchPhase(str(phase)).value
    except ValueError:
        return str(phase)


def _require_phase(phase: SearchPhase | str) -> SearchPhase:
    """Строгая фаза для записи ``search_log`` (неизвестная строка — ошибка)."""
    if isinstance(phase, SearchPhase):
        return phase
    return SearchPhase(str(phase))


def _as_cap(value: Any, key: Any) -> int:
    """Неотрицательный целый потолок; не-число/отрицательное — ``ValueError``."""
    if isinstance(value, bool):
        raise ValueError(f"SearchBudget: потолок для {key!r} не может быть bool")
    if isinstance(value, int):
        cap = value
    else:
        try:
            cap = int(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"SearchBudget: потолок для {key!r} не целое: {value!r}"
            ) from None
    if cap < 0:
        raise ValueError(f"SearchBudget: потолок для {key!r} отрицателен: {cap}")
    return cap


@dataclass(frozen=True)
class SearchBudget:
    """Потолки внешних поисковых вызовов по фазам (§5, I23).

    :param limits: карта ``{phase: cap}``; ключи — значения ``SearchPhase``.
    :param default: потолок для фаз без явного лимита (``None`` — без лимита).
    """

    limits: Mapping[str, int] = field(default_factory=dict)
    default: int | None = None

    @classmethod
    def from_config(cls, run_config: Any) -> SearchBudget:
        """Строит бюджет из ``run_config``.

        Принимает либо mapping напрямую, либо объект с атрибутом
        ``search_effort_budget_json`` (JSON-строка или mapping) — так бюджет читается
        как из ``RunManifest``, так и из любого конфига прогона. Отсутствие значения,
        ``None`` или пустая строка дают «безлимитный» бюджет.
        """
        if run_config is None:
            return cls()
        if isinstance(run_config, Mapping):
            return cls.from_mapping(run_config)
        return cls.from_mapping(getattr(run_config, "search_effort_budget_json", None))

    @classmethod
    def from_mapping(cls, raw: Any) -> SearchBudget:
        """Разбирает ``{phase: cap}`` из mapping / JSON-строки (``None`` → пусто)."""
        if raw is None:
            return cls()
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8")
        data: Any = raw
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return cls()
            data = json.loads(text)
        if not isinstance(data, Mapping):
            raise ValueError("SearchBudget: ожидается mapping или JSON-объект")

        limits: dict[str, int] = {}
        default: int | None = None
        for key, value in data.items():
            if value is None:
                continue
            cap = _as_cap(value, key)
            name = str(key).strip().lower()
            if name in _DEFAULT_KEYS:
                default = cap
            else:
                limits[name] = cap
        return cls(limits=limits, default=default)

    def limit_for(self, phase: SearchPhase | str) -> int | None:
        """Потолок для фазы; ``None`` — фаза без лимита (не ограничена)."""
        value = _phase_value(phase)
        if value in self.limits:
            return int(self.limits[value])
        return self.default


@dataclass
class SearchEffort:
    """Счётчик внешних поисковых вызовов на ``(phase, product_id)`` (I16, I23).

    Бюджет применяется **на продукт**: собственные счётчики у каждого ``product_id``
    не пересекаются, поэтому исчерпание одним продуктом не лишает бюджета другой —
    симметрия поискового усилия (I16).

    :param budget: потолки по фазам; пустой бюджет — без ограничений.
    """

    budget: SearchBudget = field(default_factory=SearchBudget)
    _counts: dict[tuple[str, str], int] = field(
        default_factory=dict, init=False, repr=False
    )

    @property
    def counts(self) -> dict[tuple[str, str], int]:
        """Копия израсходованных значений ``{(phase, product_id): n}`` (детерминизм)."""
        return dict(self._counts)

    def used(self, phase: SearchPhase | str, product_id: str) -> int:
        """Сколько внешних вызовов израсходовано для ``(phase, product_id)``."""
        return self._counts.get((_phase_value(phase), str(product_id)), 0)

    def remaining(self, phase: SearchPhase | str, product_id: str) -> int | None:
        """Остаток бюджета; ``None`` — фаза не ограничена (безлимит)."""
        limit = self.budget.limit_for(phase)
        if limit is None:
            return None
        return max(limit - self.used(phase, product_id), 0)

    def charge(self, phase: SearchPhase | str, product_id: str, n: int = 1) -> bool:
        """Списывает ``n`` вызовов, если бюджет позволяет.

        :return: ``True`` — в пределах бюджета (счётчик увеличен);
            ``False`` — исчерпан (счётчик **не** меняется, вызов не состоялся).
        """
        if isinstance(n, bool) or not isinstance(n, int):
            raise ValueError(f"SearchEffort: n должно быть целым, получено {n!r}")
        if n < 0:
            raise ValueError(f"SearchEffort: n не может быть отрицательным: {n}")
        key = (_phase_value(phase), str(product_id))
        current = self._counts.get(key, 0)
        limit = self.budget.limit_for(phase)
        if limit is not None and current + n > limit:
            return False
        self._counts[key] = current + n
        return True


def record_search(
    store: Any,
    *,
    run_id: str,
    phase: SearchPhase | str,
    kind: SearchKind | str = SearchKind.EXTERNAL,
    provider: str | None = None,
    adapter_kind: AdapterKind | str | None = None,
    language: str | None = None,
    query: str | None = None,
    results_count: int | None = None,
    selected: int | None = None,
    termination_reason: TerminationReason | str | None = None,
    clock: Callable[[], str] | None = None,
    **scores: Any,
) -> SearchLogEntry:
    """Строит :class:`SearchLogEntry` и (при наличии ``store``) пишет ``search_log``.

    ``kind`` по умолчанию — ``SearchKind.EXTERNAL`` (внешний поиск; внутренний
    ретривал логируется с ``kind=SearchKind.RETRIEVAL``). Именованные ``**scores``
    (``lexical_score``/``dense_score``/``fused_rank``/``rerank_score``/
    ``retrieval_score``) переносятся в запись как есть. ``ts`` заполняется текущим
    UTC ISO; часы можно инжектировать через ``clock`` для детерминизма.

    ``search_log`` — единственное место логирования любых поисковых вызовов (§4.4, I23),
    включая tool-based.
    """
    entry = SearchLogEntry(
        run_id=run_id,
        phase=_require_phase(phase),
        kind=_as_enum(SearchKind, kind),
        provider=provider,
        adapter_kind=_as_enum(AdapterKind, adapter_kind),
        language=language,
        query=query,
        results_count=results_count,
        selected=selected,
        termination_reason=_as_enum(TerminationReason, termination_reason),
        ts=(clock or _utcnow)(),
        **scores,
    )
    if store is not None:
        store.log_search(entry)
    return entry


def termination_for(
    budget_exhausted: bool,
    results_count: int | None,
    sufficient: bool,
    error: BaseException | None,
) -> TerminationReason | None:
    """Отображение сигналов исчерпания в причину терминации (§5).

    Приоритет (сверху вниз):

    1. ``error`` задан → ``ERROR`` (сбой поискового вызова);
    2. ``sufficient`` → ``SUFFICIENT_EVIDENCE`` (доказательств достаточно);
    3. ``budget_exhausted`` → ``BUDGET_EXHAUSTED`` (потолок усилия исчерпан);
    4. ``results_count is None`` → ``None`` (не терминально, протокол продолжается);
    5. ``results_count == 0`` → ``NOT_FOUND`` (ничего не найдено);
    6. иначе → ``RESULTS_EXHAUSTED`` (доступные результаты исчерпаны).
    """
    if error is not None:
        return TerminationReason.ERROR
    if sufficient:
        return TerminationReason.SUFFICIENT_EVIDENCE
    if budget_exhausted:
        return TerminationReason.BUDGET_EXHAUSTED
    if results_count is None:
        return None
    if results_count <= 0:
        return TerminationReason.NOT_FOUND
    return TerminationReason.RESULTS_EXHAUSTED


def external_search(
    adapter: SearchAdapterPort,
    query: SearchQuery,
    *,
    store: Any,
    run_id: str,
    effort: SearchEffort,
    budget: SearchBudget,
    product_id: str,
    clock: Callable[[], str] | None = None,
) -> list[SearchResult]:
    """Ограниченный бюджетом одиночный внешний поиск с записью в ``search_log`` (I16/I19/I23).

    Порядок: сначала проверяется **продукт-специфичный** остаток бюджета; если он
    исчерпан — пишется ``BUDGET_EXHAUSTED`` и возвращается ``[]`` без вызова адаптера.
    Иначе вызывается ``adapter.search(query)``, расход списывается через
    :meth:`SearchEffort.charge`, а результат (``results_count``) фиксируется в логе.

    Гарантия I19: возвращаются только кандидаты :class:`SearchResult`; функция не
    производит evidence и не обращается к хранилищу за доказательствами.

    :param effort: счётчик усилия на ``(phase, product_id)`` (ключ симметрии I16).
    :param budget: бюджет прогона; используется, если ``effort`` его не несёт.
    :param product_id: продукт, на который списывается усилие (I16).
    """
    phase = _require_phase(query.phase)
    # ``effort`` — носитель бюджета (charge не принимает бюджет отдельно). Если он пуст,
    # а явный ``budget`` задаёт лимит для фазы — принимаем его, чтобы аргумент не был мёртвым.
    if budget is not None and effort.budget.limit_for(phase) is None:
        if budget.limit_for(phase) is not None:
            effort.budget = budget

    if effort.remaining(phase, product_id) == 0:
        record_search(
            store,
            run_id=run_id,
            phase=phase,
            provider=getattr(adapter, "provider", None),
            adapter_kind=getattr(adapter, "adapter_kind", None),
            language=query.lang,
            query=query.text,
            results_count=0,
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
            clock=clock,
        )
        return []

    error: BaseException | None = None
    results: list[SearchResult] = []
    try:
        results = list(adapter.search(query))
    except Exception as exc:  # noqa: BLE001 - сбой одного продукта не роняет остальные (P4)
        error = exc

    # Вызов (в т.ч. неуспешный) израсходовал усилие провайдера — списываем бюджет.
    effort.charge(phase, product_id)

    if error is not None:
        termination: TerminationReason | None = termination_for(
            False, None, False, error
        )
    elif not results:
        termination = termination_for(False, 0, False, None)
    else:
        # Результаты есть — протокол продолжается; терминация фиксируется вызывающим.
        termination = None

    record_search(
        store,
        run_id=run_id,
        phase=phase,
        provider=getattr(adapter, "provider", None),
        adapter_kind=getattr(adapter, "adapter_kind", None),
        language=query.lang,
        query=query.text,
        results_count=(None if error is not None else len(results)),
        termination_reason=termination,
        clock=clock,
    )
    return results
