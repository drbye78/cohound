"""coHound — Discovery / concept scout (W11/T11.4, Ш2, §7.3, I18/I19).

Сценарий **разведки**: по ролям scope (discovery) или для пополнения словаря
концептов (concept scout) внешний поиск **разрешён**. Но выход — только
**кандидаты/гипотезы**: пары URL+сниппет, ещё не ставшие evidence.

Ключевые инварианты
-------------------
* **I19** — внешний контент становится evidence только после
  ``corpus_raw → hash → index``. Этот модуль *структурно* не пишет evidence: он не
  импортирует ``core.store``, не строит ``Cell``/``CellEvidence`` и не вызывает
  ``save_cell``/``add_evidence``/``log_search``. Всё, что он отдаёт, — гипотезы.
* **I18** — оценщик (Ш6) не вызывает поиск. Поиск живёт в отдельном сценарии
  (discovery/scout); смешивать контуры нельзя.
* **A3** — дефолт (в т.ч. эвристическая ``confidence``) — это **assumption**, а не
  измерение. Продвижение гипотезы в кандидат — только человеческим решением G2.

Гейт G2 (человеческое подтверждение relationship) реализован в
:func:`review_candidates`. Промоушен **никогда не автоматический**: без явной строки
решения ``gate_decisions`` (S9) кандидат остаётся гипотезой. Только после G2 запись
может попасть в каталог/паспорт — сам каталог этот модуль не пишет.

Нейтрален (§1.3): без названий продуктов/вендоров и предметных терминов.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from core.domain.enums import Gate, TerminationReason
from core.pipeline.gates import gate_g2, is_gate_passed
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.util.security import sanitize_slug

__all__ = [
    "DiscoveryStatus",
    "ReviewAction",
    "BudgetPort",
    "ReviewDecision",
    "DiscoveryCandidate",
    "DiscoveryReport",
    "discover",
    "review_candidates",
]

#: Префикс детерминированного идентификатора кандидата.
CANDIDATE_ID_PREFIX = "dc_"

#: Предельная длина slug-заготовки ``param_id``.
_PARAM_ID_MAX_LENGTH = 64


class DiscoveryStatus(StrEnum):
    """Рабочий статус результата разведки (не доменный ``Status`` ячейки).

    * ``HYPOTHESIS`` — сырой выход поиска: неподтверждённая гипотеза (дефолт).
    * ``CANDIDATE`` — гипотеза, продвинутая **человеческим** решением G2; ещё не
      evidence и ещё не строка каталога (I19).

    Это локальное рабочее перечисление: оно не описывает статус ячейки и не
    фигурирует в ``docs/status-dictionary.yaml`` (там — ``Status``/``cell_state``).
    """

    HYPOTHESIS = "hypothesis"
    CANDIDATE = "candidate"


class ReviewAction(StrEnum):
    """Исход человеческого решения G2 по конкретному кандидату."""

    PROMOTE = "promote"
    REJECT = "reject"


@runtime_checkable
class BudgetPort(Protocol):
    """Контракт бюджета поисковых вызовов (per-product или глобальный).

    ``charge(phase, product_id)`` вызывается **перед** каждым обращением к адаптеру
    и возвращает ``True``, если вызов разрешён и списан с бюджета, иначе ``False``
    (бюджет исчерпан → терминация ``budget_exhausted``). Реализация решает, как
    учитывать лимиты (например, симметрия RU/EN на продукт-параметр, §7.2).
    """

    def charge(self, phase: str, product_id: str | None) -> bool: ...


@dataclass(frozen=True)
class ReviewDecision:
    """Явное человеческое решение G2 по одному кандидату (S9).

    ``actor`` обязателен и непуст — гейт не проходится автоматически (I6/I7).
    """

    candidate_id: str
    action: ReviewAction
    actor: str
    notes: str | None = None


@dataclass
class DiscoveryCandidate:
    """Гипотеза разведки: URL+сниппет, **не** evidence (I19).

    :param candidate_id: детерминированный id (хэш URL); иммутабелен в рамках прогона.
    :param param_id: предложенный slug параметра (заготовка для каталога, не финал).
    :param module: модуль, к которому предположительно относится гипотеза (может быть
        пустым, если контекст запроса его не задаёт).
    :param rationale: человекочитаемое обоснование, почему гипотеза предложена.
    :param source_urls: URL-источники результата поиска.
    :param evidence_refs: сниппеты/URL — **ссылки-кандидаты**, а не evidence (I19).
    :param confidence: эвристическая уверенность ∈ [0, 1]; assumption, не измерение (A3).
    :param status: ``hypothesis`` по умолчанию; ``candidate`` только после G2.
    """

    candidate_id: str
    param_id: str
    title: str
    module: str
    rationale: str
    source_urls: list[str] = field(default_factory=list)
    evidence_refs: list[str] = field(default_factory=list)
    confidence: float = 0.0
    status: DiscoveryStatus = DiscoveryStatus.HYPOTHESIS

    def to_dict(self) -> dict[str, Any]:
        """Детерминированное представление для манифеста (JSON-совместимое)."""
        return {
            "candidate_id": self.candidate_id,
            "param_id": self.param_id,
            "title": self.title,
            "module": self.module,
            "rationale": self.rationale,
            "source_urls": list(self.source_urls),
            "evidence_refs": list(self.evidence_refs),
            "confidence": self.confidence,
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DiscoveryCandidate:
        """Восстанавливает кандидата из :meth:`to_dict` (round-trip)."""
        return cls(
            candidate_id=str(data["candidate_id"]),
            param_id=str(data["param_id"]),
            title=str(data["title"]),
            module=str(data.get("module", "")),
            rationale=str(data.get("rationale", "")),
            source_urls=[str(u) for u in data.get("source_urls", [])],
            evidence_refs=[str(r) for r in data.get("evidence_refs", [])],
            confidence=float(data.get("confidence", 0.0)),
            status=DiscoveryStatus(data.get("status", DiscoveryStatus.HYPOTHESIS)),
        )


@dataclass
class DiscoveryReport:
    """Итог разведки: запросы, гипотезы и причина терминации протокола поиска.

    :param query: тексты запросов в порядке обхода (детерминированный порядок).
    :param candidates: гипотезы в порядке первого появления; дубликаты по URL слиты.
    :param search_calls: число фактических обращений к ``SearchAdapterPort.search``.
    :param termination_reason: ``results_exhausted`` | ``budget_exhausted`` |
        ``not_found`` | ``error`` (см. :class:`~core.domain.enums.TerminationReason`).
    :param phase: фаза конвейера, в которой выполнялась разведка (``discovery``/
        ``scout``), для ``search_log`` (I23).
    """

    query: list[str] = field(default_factory=list)
    candidates: list[DiscoveryCandidate] = field(default_factory=list)
    search_calls: int = 0
    termination_reason: TerminationReason = TerminationReason.RESULTS_EXHAUSTED
    phase: str = "discovery"

    def to_dict(self) -> dict[str, Any]:
        """Детерминированное JSON-совместимое представление отчёта."""
        return {
            "query": list(self.query),
            "candidates": [c.to_dict() for c in self.candidates],
            "search_calls": self.search_calls,
            "termination_reason": self.termination_reason.value,
            "phase": self.phase,
        }

    def to_manifest_json(self) -> str:
        """JSON для манифеста/лога: ``sort_keys`` + ``ensure_ascii=False``.

        Детерминирован: одинаковый отчёт даёт байт-в-байт одинаковую строку.
        """
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DiscoveryReport:
        """Восстанавливает отчёт из :meth:`to_dict` (round-trip)."""
        return cls(
            query=[str(q) for q in data.get("query", [])],
            candidates=[
                DiscoveryCandidate.from_dict(item)
                for item in data.get("candidates", [])
            ],
            search_calls=int(data.get("search_calls", 0)),
            termination_reason=TerminationReason(
                data.get("termination_reason", TerminationReason.RESULTS_EXHAUSTED)
            ),
            phase=str(data.get("phase", "discovery")),
        )


# ---------------------------------------------------------------------------
# Внутренние хелперы
# ---------------------------------------------------------------------------


def _phase_value(phase: str | Any) -> str:
    """Нормализует фазу к строке (принимает ``SearchPhase``/строку)."""
    value = getattr(phase, "value", phase)
    return str(value)


def _as_query(query: str | SearchQuery, *, limit: int, lang: str, phase: str) -> SearchQuery:
    """Приводит элемент ``queries`` к :class:`SearchQuery`.

    Строка оборачивается с общими ``limit``/``lang``/``phase``; готовый
    :class:`SearchQuery` передаётся как есть (его собственные поля уважаются).
    """
    if isinstance(query, SearchQuery):
        return query
    return SearchQuery(text=str(query), lang=lang, limit=limit, phase=phase)


def _candidate_id(url: str) -> str:
    """Детерминированный ``candidate_id`` из URL (иммутабельный якорь дедупа)."""
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return f"{CANDIDATE_ID_PREFIX}{digest}"


def _propose_param_id(title: str, candidate_id: str) -> str:
    """Предлагает slug ``param_id`` из заголовка (заготовка, не финал каталога)."""
    source = (title or "").strip()
    if not source:
        return candidate_id
    slug = sanitize_slug(source).lower()[:_PARAM_ID_MAX_LENGTH].strip("_")
    return slug or candidate_id


def _confidence_for_rank(rank: int) -> float:
    """Эвристическая уверенность по ранку выдачи: ``1 / (1 + rank)``.

    Это **assumption** (A3), не калиброванная величина; при необходимости
    заменяется измерением. Для отрицательного/нулевого ранка даёт 1.0.
    """
    return 1.0 / (1.0 + max(rank, 0))


def _candidate_from(query: SearchQuery, result: SearchResult) -> DiscoveryCandidate:
    """Превращает результат поиска в гипотезу-кандидата (без evidence, I19)."""
    candidate_id = _candidate_id(result.url)
    title = result.title or result.url
    refs = [result.snippet] if result.snippet else [result.url]
    rationale = (
        f"Поисковая выдача по запросу {query.text!r} (rank={result.rank}); "
        "URL/сниппет — гипотеза-кандидат, не evidence (I19)."
    )
    return DiscoveryCandidate(
        candidate_id=candidate_id,
        param_id=_propose_param_id(title, candidate_id),
        title=title,
        module="",
        rationale=rationale,
        source_urls=[result.url],
        evidence_refs=list(refs),
        confidence=_confidence_for_rank(result.rank),
        status=DiscoveryStatus.HYPOTHESIS,
    )


def _merge_into(existing: DiscoveryCandidate, result: SearchResult) -> None:
    """Сливает повторный URL-результат в уже найденную гипотезу (тот же URL).

    Сохраняет порядок первого появления; добавляет уникальный сниппет и берёт
    максимум ``confidence`` (детерминировано, независимо от порядка результатов).
    """
    if result.url not in existing.source_urls:
        existing.source_urls.append(result.url)
    if result.snippet and result.snippet not in existing.evidence_refs:
        existing.evidence_refs.append(result.snippet)
    existing.confidence = max(existing.confidence, _confidence_for_rank(result.rank))


def _budget_allows(
    budget: int | BudgetPort | None,
    *,
    phase: str,
    product_id: str | None,
    calls: int,
) -> bool:
    """Разрешает ли бюджет следующий поисковый вызов.

    * ``None`` — без бюджета (всегда ``True``).
    * ``int`` — максимум вызовов (жёсткий потолок по счётчику).
    * объект с ``charge(phase, product_id)`` — решение делегировано бюджету.
    """
    if budget is None:
        return True
    if isinstance(budget, int):
        return calls < budget
    return bool(budget.charge(phase, product_id))


# ---------------------------------------------------------------------------
# Публичный вход
# ---------------------------------------------------------------------------


def discover(
    *,
    queries: Iterable[str | SearchQuery],
    adapter: SearchAdapterPort,
    limit: int = 10,
    lang: str = "ru",
    phase: str = "discovery",
    budget: int | BudgetPort | None = None,
    product_id: str | None = None,
) -> DiscoveryReport:
    """Выполняет разведку: гоняет ``adapter.search`` по запросам и собирает гипотезы.

    Выход — только :class:`DiscoveryCandidate` (URL+сниппет). Модуль **не** строит
    ``Cell``/``CellEvidence`` и не пишет в хранилище: чтобы стать evidence, найденное
    обязано пройти ``corpus_raw → hash → index`` (I19). Вызовы адаптера считаются в
    ``search_calls``; запись в ``search_log`` (I23) выполняет вызывающий pipeline
    (у этого модуля нет доступа к ``StorePort`` — структурная граница I19).

    :param queries: строки запросов или готовые :class:`SearchQuery` (порядок
        сохраняется; он же — детерминированный порядок кандидатов).
    :param adapter: ``SearchAdapterPort`` (SERP/grounding; инъекция из composition root).
    :param limit: лимит результатов на строковый запрос.
    :param lang: язык по умолчанию для строковых запросов.
    :param phase: фаза для ``search_log`` (``discovery``/``scout``).
    :param budget: ``None`` — без бюджета; ``int`` — максимум вызовов; объект
        :class:`BudgetPort` — пер-продуктовый/глобальный учёт через ``charge``.
    :param product_id: продукт для ``BudgetPort.charge`` (None — глобальный бюджет).

    Терминация: ``budget_exhausted`` (бюджет исчерпан), ``error`` (хотя бы один вызов
    упал), ``not_found`` (ноль кандидатов без ошибок), ``results_exhausted`` (иначе).
    """
    phase_value = _phase_value(phase)
    normalized: list[SearchQuery] = [
        _as_query(q, limit=limit, lang=lang, phase=phase_value) for q in queries
    ]

    candidates: list[DiscoveryCandidate] = []
    by_url: dict[str, DiscoveryCandidate] = {}
    search_calls = 0
    had_error = False
    budget_stopped = False

    for query in normalized:
        if not _budget_allows(
            budget, phase=phase_value, product_id=product_id, calls=search_calls
        ):
            budget_stopped = True
            break
        search_calls += 1
        try:
            results = adapter.search(query)
        except Exception:  # noqa: BLE001 — сбой провайдера изолируется, разведка продолжается
            had_error = True
            continue
        for result in results:
            existing = by_url.get(result.url)
            if existing is None:
                candidate = _candidate_from(query, result)
                by_url[result.url] = candidate
                candidates.append(candidate)
            else:
                _merge_into(existing, result)

    if budget_stopped:
        termination = TerminationReason.BUDGET_EXHAUSTED
    elif had_error:
        termination = TerminationReason.ERROR
    elif not candidates:
        termination = TerminationReason.NOT_FOUND
    else:
        termination = TerminationReason.RESULTS_EXHAUSTED

    return DiscoveryReport(
        query=[q.text for q in normalized],
        candidates=candidates,
        search_calls=search_calls,
        termination_reason=termination,
        phase=phase_value,
    )


def _action_from_notes(notes: str | None) -> ReviewAction:
    """Восстанавливает действие из ``notes`` решения G2 (по умолчанию — promote)."""
    text = (notes or "").strip().lower()
    if text.startswith("reject"):
        return ReviewAction.REJECT
    # Решение G2 по конкретному target — это подтверждение (approve per satellite).
    return ReviewAction.PROMOTE


def _persisted_actions(
    store: Any, run_id: str | None
) -> dict[str, ReviewAction]:
    """Читает уже записанные решения G2 (``target → action``) из ``gate_decisions``.

    Возвращает пустую карту, если гейт G2 не пройден (нет ни одной строки, S9):
    без строки в ``gate_decisions`` гейт не считается пройденным.
    """
    if store is None or run_id is None:
        return {}
    if not is_gate_passed(store, run_id, Gate.G2):
        return {}
    actions: dict[str, ReviewAction] = {}
    for decision in store.iter_gate_decisions(run_id):
        if decision.gate is not Gate.G2 or not decision.target:
            continue
        actions[decision.target] = _action_from_notes(decision.notes)
    return actions


def review_candidates(
    report: DiscoveryReport,
    *,
    store: Any | None = None,
    run_id: str | None = None,
    decisions: Sequence[ReviewDecision] | None = None,
    clock: Any | None = None,
) -> list[DiscoveryCandidate]:
    """Гейт G2: продвижение гипотез в кандидаты — **только человеком** (I6/I7, A3).

    Без явного решения (``decisions``) и без уже сохранённых решений G2 в
    ``gate_decisions`` список возвращается без изменений: ни одна гипотеза не
    повышается автоматически. Явные решения персистятся через
    :func:`~core.pipeline.gates.gate_g2` (строка ``gate_decisions``, S9), после чего
    кандидат получает ``status=DiscoveryStatus.CANDIDATE``. Отвергнутые исключаются.

    **Важно:** сам каталог/паспорт этот модуль не пишет. Запись в каталог выполняется
    отдельно и **только после** G2; здесь фиксируется лишь человеческое решение и
    рабочий статус гипотезы (I19/A3).

    :param decisions: явные решения человека; ``actor`` обязателен и непуст.
    :param store: ``GateStore`` для персистенции/чтения решений; без него промоушен
        с явными решениями невозможен (S9) — поднимается :class:`ValueError`.
    :param run_id: прогон, к которому привязаны решения.
    :param clock: часы с ``now_iso()`` (тестируемость), передаются в ``gate_g2``.
    :raises ValueError: переданы ``decisions`` без ``store``/``run_id``.
    """
    explicit: dict[str, ReviewDecision] = {}
    for decision in decisions or ():
        explicit[decision.candidate_id] = decision

    if explicit:
        if store is None or run_id is None:
            raise ValueError(
                "промоушен G2 требует store и run_id: гейт не пройден без строки "
                "в gate_decisions (S9)"
            )
        # Явные человеческие решения фиксируются в gate_decisions (S9).
        for candidate_id, decision in explicit.items():
            gate_g2(
                store,
                run_id,
                actor=decision.actor,
                target=candidate_id,
                notes=decision.notes or decision.action.value,
                clock=clock,
            )

    persisted = _persisted_actions(store, run_id)

    reviewed: list[DiscoveryCandidate] = []
    for candidate in report.candidates:
        if candidate.candidate_id in explicit:
            action: ReviewAction | None = explicit[candidate.candidate_id].action
        else:
            action = persisted.get(candidate.candidate_id)

        if action is ReviewAction.REJECT:
            continue  # отвергнутая гипотеза не попадает в reviewed-набор
        if action is ReviewAction.PROMOTE:
            reviewed.append(replace(candidate, status=DiscoveryStatus.CANDIDATE))
        else:
            # Без решения гипотеза остаётся гипотезой — авто-промоушена нет (A3/I18).
            reviewed.append(replace(candidate, status=DiscoveryStatus.HYPOTHESIS))
    return reviewed
