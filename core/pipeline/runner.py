"""Оркестратор конвейера — фазы Ш1–Ш9, персистентное состояние, resume (02 §A.1, W4/T4.1).

Прогон — идемпотентная последовательность фаз с персистентным состоянием. Реестр фаз
плагинируем (``dict[str, PhaseFn]``): composition root подставляет реальные реализации
(:func:`~core.pipeline.phases.build_phases`). Фаза **без зарегистрированной реализации** —
ошибка: ``Runner`` вернёт ``failed`` («не зарегистрирована»), а не выдаст фиктивную удачу
(echo-заглушка удалена). Гейты G1–G4 останавливают прогон, пока нет человеческого решения
в ``gate_decisions`` (S9, I6/I7).

**Статусы фаз.** ``ok`` — отработала и незакрытого не осталось; ``skipped`` — предпосылка
отсутствовала, работы не было (**не** сбой, прогон **не** останавливает); ``blocked`` —
остановка на человеческом гейте без решения; ``failed`` — ошибка. Остаток для человека —
это метаданные, а не статус: :attr:`PhaseResult.skip_reason` называет машинную причину
пропуска (:class:`~core.domain.enums.SkipReason`), :attr:`PhaseResult.pending` — незакрытые
пункты. ``pending`` **advisory**: он никогда не меняет статус фазы и не порождает
автоматический ``N/A`` (иначе были бы нарушены I6/I7).

**Изоляция ошибок.** Исключение одной фазы не роняет прогон: оно собирается в
``PhaseResult.errors`` со статусом ``failed``. Ошибки отдельных продуктов/документов
фаза обязана собирать сама (см. :func:`run_isolated`), чтобы один сбой не отменял
обработку остальных.

**Resume.** Фаза с сохранённым состоянием ``ok`` пропускается. Состояния ``skipped``/
``failed``/``blocked`` пересчитываются при следующем ``run``/``resume``: пропуск фиксирует
лишь то, что предпосылки не было **в тот момент**, а она могла измениться между прогонами
(например, оператор добавил поисковый адаптер) — ре-деривация дешева. Состояния фаз
кэшируются в памяти ``Runner`` под ``run_id`` и durable-персистятся в
``StorePort.set_phase_state`` (карта ``{phase: state}`` в ``runs.state``). При пустом
in-memory-журнале он разогревается из ``StorePort.get_phase_state``, поэтому **новый
процесс/``Runner`` продолжает прерванный прогон** (cross-process resume, 02 §A.1).

Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from core.domain import Cell, Gate, SkipReason
from core.ports.chunker import ChunkerPort
from core.ports.crawl_executor import CrawlExecutorPort
from core.ports.crawler import CrawlerPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.index_builder import IndexBuilderPort
from core.ports.lexical_index import LexicalIndexPort
from core.ports.llm import EvaluatorPort
from core.ports.poller import PollPort
from core.ports.registry import RegistryFetcherPort
from core.ports.search import SearchAdapterPort
from core.ports.store import StorePort
from core.ports.vector_index import VectorIndexPort
from core.pipeline.gates import is_gate_passed

__all__ = [
    "Runner",
    "RunReport",
    "RunContext",
    "Ports",
    "PhaseResult",
    "PhaseStatus",
    "PhaseFn",
    "Clock",
    "PHASES",
    "GATE_PHASES",
    "run_isolated",
]

#: Статус фазы. ``ok`` — отработала, незакрытого не осталось; ``skipped`` — предпосылка
#: отсутствовала, работы не было (**не** сбой; прогон продолжается); ``blocked`` — остановка
#: на человеческом гейте без решения; ``failed`` — ошибка. ``skipped`` — не то же самое, что
#: ``ok`` (нечего делать ≠ сделано) и не то же, что ``failed``; причина пропуска —
#: :attr:`PhaseResult.skip_reason` (:class:`~core.domain.enums.SkipReason`).
PhaseStatus = Literal["ok", "skipped", "blocked", "failed"]

#: Порядок фаз конвейера (02 §A.3). Гейты идут отдельными фазами и останавливают прогон.
PHASES: tuple[str, ...] = (
    "normalize",
    "lint",
    "plan",
    "g1",
    "discovery",
    "g2",
    "registry",
    "inheritance",
    "corpus",
    "evaluate",
    "review",
    "g3",
    "compute",
    "g4",
    "report",
)

#: Соответствие «фаза-гейт → доменный гейт» (S9).
GATE_PHASES: dict[str, Gate] = {
    "g1": Gate.G1,
    "g2": Gate.G2,
    "g3": Gate.G3,
    "g4": Gate.G4,
}

assert set(GATE_PHASES) <= set(PHASES), "фазы-гейты обязаны входить в PHASES"


@dataclass
class PhaseResult:
    """Результат одной фазы (общий контракт фаз, 01 §4).

    ``status``: ``ok`` — отработала, незакрытого не осталось; ``skipped`` — предпосылка
    отсутствовала, работы не было (не сбой; прогон продолжается); ``blocked`` — ждать
    человека на гейте; ``failed`` — ошибка. ``cells`` — записанные/изменённые ячейки,
    ``log_entries`` — записи лога, ``errors`` — накопленные изолированные ошибки (не
    прерывают весь прогон).

    ``skip_reason`` — машинная причина при ``status == "skipped"``
    (:class:`~core.domain.enums.SkipReason`); ``None`` для прочих статусов.

    ``pending`` — незакрытые пункты, требующие человека (напр. очередь ревью, Ш10/Ш11;
    наполняется в P2b). **Advisory**: ``pending`` никогда не меняет ``status`` и не
    порождает автоматический ``N/A`` — иначе I6/I7 были бы нарушены (N/A и преодоление
    ``❓`` — только человеком, не автоматикой).
    """

    status: PhaseStatus
    phase: str
    cells: list[Cell] = field(default_factory=list)
    log_entries: list[Any] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    skip_reason: SkipReason | None = None
    pending: list[str] = field(default_factory=list)


@dataclass
class Ports:
    """Внедрённые порты (ручной DI, A1). Все поля опциональны: фаза берёт нужные.

    ``pipeline`` никогда не инстанцирует адаптеры — их собирает composition root (``cli/``).

    ``crawler`` — краулер по умолчанию (fallback). ``crawlers`` — карта ``engine → CrawlerPort``
    для per-engine выбора в фазе корпуса (Ш5): ``build_corpus`` берёт адаптер по
    разрешённому ``engine`` источника краула, а при отсутствии/``None`` — ``crawler``.

    ``executor`` — опциональный исполнитель краула (``CrawlExecutorPort``, Phase 3.3):
    при наличии ``build_corpus`` исполняет краул **через него** (``split``/edge-планировщик),
    а ``crawler``/``crawlers`` не используются. ``None`` → прежний локальный per-URL путь.

    ``pollers`` — карта ``kind → PollPort`` (Phase 5.1), параллельная ``crawlers``: фаза
    корпуса (Ш5) берёт поллер по виду источника (``feed``/``channel``), чтобы дополнить
    корпус ссылками из публичных лент/каналов (кандидаты, не evidence — I19). ``None`` →
    опрос фидов выключен.
    """

    evaluator: EvaluatorPort | None = None
    vector_index: VectorIndexPort | None = None
    lexical_index: LexicalIndexPort | None = None
    embedder: EmbedderPort | None = None
    index_builder: IndexBuilderPort | None = None
    search: SearchAdapterPort | None = None
    crawler: CrawlerPort | None = None
    crawlers: Mapping[str, CrawlerPort] | None = None
    executor: CrawlExecutorPort | None = None
    pollers: Mapping[str, PollPort] | None = None
    extractor: ExtractorPort | None = None
    chunker: ChunkerPort | None = None
    registry: RegistryFetcherPort | None = None


@runtime_checkable
class Clock(Protocol):
    """Источник времени (тестируемость)."""

    def now_iso(self) -> str: ...


@dataclass
class RunContext:
    """Контекст прогона, передаваемый каждой фазе."""

    run_id: str
    store: StorePort
    ports: Ports
    clock: Clock | None = None


#: Функция фазы: принимает контекст, возвращает результат.
PhaseFn = Callable[[RunContext], PhaseResult]


@dataclass
class RunReport:
    """Итог ``Runner.run``/``resume``: результаты, пропуски resume и точка остановки.

    ``skipped`` — имена фаз, **короткозамкнутых** персистентным ``ok`` (resume), а не фазы
    со статусом ``PhaseStatus.skipped`` (те попадают в ``results`` как обычные результаты).
    ``stopped_at`` — фаза, остановившая прогон: только ``blocked`` или ``failed``.
    """

    run_id: str
    results: list[PhaseResult] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    stopped_at: str | None = None

    @property
    def status(self) -> PhaseStatus:
        """Итоговый статус: статус фазы остановки либо ``ok``."""
        if self.stopped_at is None:
            return "ok"
        for result in self.results:
            if result.phase == self.stopped_at:
                return result.status
        return "ok"

    @property
    def ok(self) -> bool:
        """Прогон завершён без остановки/провала."""
        return self.status == "ok"

    @property
    def errors(self) -> list[str]:
        """Все изолированные ошибки прогона (агрегат по фазам)."""
        return [err for result in self.results for err in result.errors]

    @property
    def cells(self) -> list[Cell]:
        """Все ячейки, затронутые фазами прогона."""
        return [cell for result in self.results for cell in result.cells]


def _gate_phase(phase: str, gate: Gate) -> PhaseFn:
    """Фаза-гейт: ``ok`` если человек оставил решение в ``gate_decisions``, иначе ``blocked``.

    Гейт **не** проходится сам: без строки решения прогон останавливается и ждёт CLI (W5).
    """

    def _fn(ctx: RunContext) -> PhaseResult:
        if is_gate_passed(ctx.store, ctx.run_id, gate):
            return PhaseResult(status="ok", phase=phase)
        return PhaseResult(
            status="blocked",
            phase=phase,
            errors=[
                f"гейт {gate.value} не пройден: нет строки в gate_decisions (S9)"
            ],
        )

    return _fn


def _default_registry() -> dict[str, PhaseFn]:
    """Реестр по умолчанию — **только гейты**; не-гейтовые фазы регистрирует composition root.

    Реализации фаз поставляются :func:`~core.pipeline.phases.build_phases`. Незаполненная
    фаза больше не заглушается удачей: :meth:`Runner._execute` вернёт ``failed``
    («не зарегистрирована»), чтобы отсутствие реализации было видно, а не скрыто.
    """
    return {name: _gate_phase(name, gate) for name, gate in GATE_PHASES.items()}


def run_isolated(items: Iterable[Any], fn: Callable[[Any], Any]) -> tuple[list[Any], list[str]]:
    """Прогоняет ``fn`` по элементам, изолируя сбои (ошибка продукта/документа не рушит прогон).

    Возвращает ``(успешные_результаты, ошибки)``. Фазы используют helper, чтобы собрать
    ошибки в ``PhaseResult.errors``, а не прерывать обработку остальных элементов.
    """
    outputs: list[Any] = []
    errors: list[str] = []
    for item in items:
        try:
            outputs.append(fn(item))
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(f"{item!r}: {type(exc).__name__}: {exc}")
    return outputs, errors


class Runner:
    """Оркестратор фаз Ш1–Ш9 с resume и остановкой на гейтах.

    :param store: ``StorePort`` (персистенция ячеек/состояний, чтение решений гейтов).
    :param ports: внедрённые порты (``Ports``); можно передать позже через ``run``-контекст.
    :param clock: часы (опц.).
    :param phases: переопределение реестра фаз ``{имя: PhaseFn}`` (W5–W11).
    """

    def __init__(
        self,
        store: StorePort,
        ports: Ports | None = None,
        clock: Clock | None = None,
        phases: dict[str, PhaseFn] | None = None,
    ) -> None:
        self.store = store
        self.ports = ports or Ports()
        self.clock = clock
        self._fns: dict[str, PhaseFn] = _default_registry()
        if phases:
            self._fns.update(phases)
        # Пер-фазное состояние прогонов: run_id -> {phase: ok|skipped|blocked|failed}.
        self._states: dict[str, dict[str, str]] = {}
        # Прогоны, сброшенные через reset(): для них seeding из store пропускается один раз.
        self._force_restart: set[str] = set()

    # -- регистрация фаз ------------------------------------------------- #
    def register(self, phase: str, fn: PhaseFn) -> PhaseFn:
        """Подменяет функцию фазы (W5–W11).

        :raises ValueError: фаза не входит в :data:`PHASES`.
        """
        if phase not in PHASES:
            raise ValueError(f"неизвестная фаза: {phase!r}")
        self._fns[phase] = fn
        return fn

    # -- состояние ------------------------------------------------------- #
    def states(self, run_id: str) -> dict[str, str]:
        """Копия карты состояний фаз прогона."""
        return dict(self._states.get(run_id, {}))

    def state_of(self, run_id: str, phase: str) -> str | None:
        """Состояние фазы (``ok``/``skipped``/``blocked``/``failed``) или ``None``, если не выполнялась."""
        return self._states.get(run_id, {}).get(phase)

    def reset(self, run_id: str) -> None:
        """Сбрасывает пер-фазное состояние прогона (принудительный полный перезапуск).

        Помимо очистки in-memory-журнала помечает прогон как принудительно
        перезапускаемый, чтобы при ближайшем ``run``/``resume`` не подтянуть durable-карту
        фаз из store (иначе resume вернул бы пропуск завершённых фаз).
        """
        self._states.pop(run_id, None)
        self._force_restart.add(run_id)

    def _seed_from_store(self, run_id: str, ledger: dict[str, str]) -> None:
        """Разогревает журнал фаз из durable-карты store (cross-process resume, §A.1).

        Вызывается, когда in-memory-журнал прогона пуст (новый ``Runner``/новый процесс).
        Обратная совместимость: хранилище без ``get_phase_state`` или сбой чтения не роняют
        прогон — журнал просто остаётся пустым.
        """
        getter = getattr(self.store, "get_phase_state", None)
        if getter is None:
            return
        try:
            ledger.update(dict(getter(run_id)))
        except Exception:  # noqa: BLE001 — resume не должен падать на чтении состояния
            return

    # -- прогон ---------------------------------------------------------- #
    def run(self, run_id: str, until: str | None = None) -> RunReport:
        """Выполняет фазы по порядку, пропуская уже ``ok`` и останавливаясь на ``blocked``/``failed``.

        Прогон останавливают только ``blocked`` (человеческий гейт без решения) и ``failed``
        (ошибка). ``skipped`` — не остановка: фаза не нашла себе работы, но следующие фазы
        обязаны исполниться.

        Журнал фаз разогревается из ``store.get_phase_state`` при пустом in-memory-журнале,
        поэтому новый ``Runner`` (новый процесс) продолжает ранее прерванный прогон.
        Короткое замыкание — **только** для персистентного ``ok``; персистентный ``skipped``
        пересчитывается (предпосылка могла измениться между прогонами).

        :param until: последняя выполняемая фаза (включительно); ``None`` — все фазы.
        :raises ValueError: ``until`` не входит в :data:`PHASES`.
        """
        if until is not None and until not in PHASES:
            raise ValueError(f"неизвестная фаза until={until!r}")
        end = PHASES.index(until) if until is not None else len(PHASES) - 1
        ledger = self._states.setdefault(run_id, {})
        if not ledger and run_id not in self._force_restart:
            self._seed_from_store(run_id, ledger)
        self._force_restart.discard(run_id)
        ctx = RunContext(run_id=run_id, store=self.store, ports=self.ports, clock=self.clock)
        report = RunReport(run_id=run_id)

        for phase in PHASES[: end + 1]:
            # Только ``ok`` короткозамкнут: ``skipped`` пересчитывается на следующем прогоне.
            if ledger.get(phase) == "ok":
                report.skipped.append(phase)
                continue
            result = self._execute(ctx, phase)
            ledger[phase] = result.status
            self._persist_state(run_id, phase, result)
            report.results.append(result)
            # Останавливают только гейт без решения и ошибка; ``skipped`` продолжает прогон.
            if result.status in ("blocked", "failed"):
                report.stopped_at = phase
                break
        return report

    def resume(self, run_id: str, until: str | None = None) -> RunReport:
        """Продолжает прерванный прогон: ``ok``-фазы пропускаются, прочие перезапускаются."""
        return self.run(run_id, until=until)

    # -- внутреннее ------------------------------------------------------ #
    def _execute(self, ctx: RunContext, phase: str) -> PhaseResult:
        """Запускает фазу, изолируя исключения в ``failed``-результат.

        Незарегистрированная фаза — тоже ``failed`` (не подделка ``ok``): отсутствие
        реализации обязано быть видимым, а не замаскированным успехом.
        """
        fn = self._fns.get(phase)
        if fn is None:
            return PhaseResult(
                status="failed",
                phase=phase,
                errors=[f"фаза {phase!r} не зарегистрирована: реализация отсутствует"],
            )
        try:
            result = fn(ctx)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            return PhaseResult(
                status="failed",
                phase=phase,
                errors=[f"{type(exc).__name__}: {exc}"],
            )
        if not isinstance(result, PhaseResult):
            return PhaseResult(
                status="failed",
                phase=phase,
                errors=[f"фаза {phase!r} вернула не PhaseResult: {type(result).__name__}"],
            )
        result.phase = phase
        return result

    def _persist_state(self, run_id: str, phase: str, result: PhaseResult) -> None:
        """Зеркалит состояние фазы в store; сбой персистенции не роняет прогон.

        Персистится и ``skipped`` (состояние видно в ``runs.state``), но короткое замыкание
        resume — **только** по ``ok``: ``skipped`` пересчитывается при следующем прогоне.
        """
        try:
            self.store.set_phase_state(run_id, phase, result.status)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            result.errors.append(f"set_phase_state({phase}): {type(exc).__name__}: {exc}")
