"""Оркестратор конвейера — фазы Ш1–Ш9, персистентное состояние, resume (02 §A.1, W4/T4.1).

Прогон — идемпотентная последовательность фаз с персистентным состоянием. Фазы
плагинируемы (``dict[str, PhaseFn]``): W5–W11 подставляют реальные реализации, а до тех
пор для незаполненных фаз действует echo-заглушка, возвращающая ``ok`` (конвейер
тестируем уже сейчас). Гейты G1–G4 останавливают прогон, пока нет человеческого решения
в ``gate_decisions`` (S9, I6/I7).

**Изоляция ошибок.** Исключение одной фазы не роняет прогон: оно собирается в
``PhaseResult.errors`` со статусом ``failed``. Ошибки отдельных продуктов/документов
фаза обязана собирать сама (см. :func:`run_isolated`), чтобы один сбой не отменял
обработку остальных.

**Resume.** Фаза со сохранённым состоянием ``ok`` пропускается; фаза ``failed``/``blocked``
перезапускается при следующем ``run``/``resume``. Состояния фаз кэшируются в памяти
``Runner`` под ``run_id`` и durable-персистятся в ``StorePort.set_phase_state`` (карта
``{phase: state}`` в ``runs.state``). При пустом in-memory-журнале он разогревается из
``StorePort.get_phase_state``, поэтому **новый процесс/``Runner`` продолжает прерванный
прогон** (cross-process resume, 02 §A.1).

Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

from core.domain import Cell, Gate
from core.ports.chunker import ChunkerPort
from core.ports.crawler import CrawlerPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.index_builder import IndexBuilderPort
from core.ports.lexical_index import LexicalIndexPort
from core.ports.llm import EvaluatorPort
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

#: Статус фазы: ``ok`` — успех, ``blocked`` — остановка на гейте, ``failed`` — ошибка.
PhaseStatus = Literal["ok", "blocked", "failed"]

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

    ``status``: ``ok`` | ``blocked`` (ждать человека на гейте) | ``failed`` (ошибка).
    ``cells`` — записанные/изменённые ячейки, ``log_entries`` — записи лога,
    ``errors`` — накопленные изолированные ошибки (не прерывают весь прогон).
    """

    status: PhaseStatus
    phase: str
    cells: list[Cell] = field(default_factory=list)
    log_entries: list[Any] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class Ports:
    """Внедрённые порты (ручной DI, A1). Все поля опциональны: фаза берёт нужные.

    ``pipeline`` никогда не инстанцирует адаптеры — их собирает composition root (``cli/``).

    ``crawler`` — краулер по умолчанию (fallback). ``crawlers`` — карта ``engine → CrawlerPort``
    для per-engine выбора в фазе корпуса (Ш5): ``build_corpus`` берёт адаптер по
    разрешённому ``engine`` источника краула, а при отсутствии/``None`` — ``crawler``.
    """

    evaluator: EvaluatorPort | None = None
    vector_index: VectorIndexPort | None = None
    lexical_index: LexicalIndexPort | None = None
    embedder: EmbedderPort | None = None
    index_builder: IndexBuilderPort | None = None
    search: SearchAdapterPort | None = None
    crawler: CrawlerPort | None = None
    crawlers: Mapping[str, CrawlerPort] | None = None
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
    """Итог ``Runner.run``/``resume``: результаты, пропуски и точка остановки."""

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


def _echo_phase(phase: str) -> PhaseFn:
    """Заглушка незаполненной фазы: успешный no-op (конвейер тестируем до W5–W11)."""

    def _fn(ctx: RunContext) -> PhaseResult:
        return PhaseResult(status="ok", phase=phase)

    return _fn


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
    """Реестр фаз по умолчанию: гейты — реальные проверки, прочее — echo-заглушки."""
    registry: dict[str, PhaseFn] = {
        name: _echo_phase(name) for name in PHASES if name not in GATE_PHASES
    }
    for name, gate in GATE_PHASES.items():
        registry[name] = _gate_phase(name, gate)
    return registry


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
        # Пер-фазное состояние прогонов: run_id -> {phase: ok|blocked|failed}.
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
        """Состояние фазы (``ok``/``blocked``/``failed``) или ``None``, если не выполнялась."""
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
        """Выполняет фазы по порядку, пропуская уже ``ok`` и останавливаясь на гейте.

        Журнал фаз разогревается из ``store.get_phase_state`` при пустом in-memory-журнале,
        поэтому новый ``Runner`` (новый процесс) продолжает ранее прерванный прогон.

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
            if ledger.get(phase) == "ok":
                report.skipped.append(phase)
                continue
            result = self._execute(ctx, phase)
            ledger[phase] = result.status
            self._persist_state(run_id, phase, result)
            report.results.append(result)
            if result.status != "ok":
                report.stopped_at = phase
                break
        return report

    def resume(self, run_id: str, until: str | None = None) -> RunReport:
        """Продолжает прерванный прогон: ``ok``-фазы пропускаются, прочие перезапускаются."""
        return self.run(run_id, until=until)

    # -- внутреннее ------------------------------------------------------ #
    def _execute(self, ctx: RunContext, phase: str) -> PhaseResult:
        """Запускает фазу, изолируя исключения в ``failed``-результат."""
        fn = self._fns.get(phase) or _echo_phase(phase)
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
        """Зеркалит состояние фазы в store; сбой персистенции не роняет прогон."""
        try:
            self.store.set_phase_state(run_id, phase, result.status)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            result.errors.append(f"set_phase_state({phase}): {type(exc).__name__}: {exc}")
