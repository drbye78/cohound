"""Оркестратор конвейера — фазы Ш1–Ш9, персистентное состояние, resume (02 §A.1, W4/T4.1).

Прогон — идемпотентная последовательность фаз с персистентным состоянием. Реестр фаз
плагинируем (``dict[str, PhaseFn]``): composition root подставляет реальные реализации
(:func:`~core.pipeline.phases.build_phases`). Фаза **без зарегистрированной реализации** —
ошибка: ``Runner`` вернёт ``failed`` («не зарегистрирована»), а не выдаст фиктивную удачу
(echo-заглушка удалена).

**Гейты.** Блокирует прогон только ``G4`` (публикация — внешнее решение): он ждёт решения,
привязанного к текущему предмету (``gate_decisions``, S9/I6/I7), экспортирует предмет в
``runs/{run_id}/gates/{gate}-request.json``, а отказ (``verdict='rejected'``) блокирует
так же явно, как отсутствие решения. ``G1``/``G2``/``G3`` **не блокируют** (решение
владельца 26.10.2026, :data:`~core.pipeline.gates.NON_BLOCKING_GATES`): нет действующего
решения — фаза возвращает ``skipped`` с причиной ``gate_not_blocking`` и **пишет**
waiver-строку «решение не требуется»; записанное решение по-прежнему видно и учитывается,
а явный отказ по текущему предмету остаётся остановкой. Политика ``run.yaml:gates``
(``waived``) снимает ожидание для ``G3``/``G4``, но **записывает** факт делегирования
(waiver-строка + ``gate_waivers`` в манифесте). Невидимый пропуск/waiver запрещён: след
обязан быть в манифесте.

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

**Инвариант ``skipped`` (N4): пропуск обязан быть side-effect-free или идемпотентным.**
``skipped`` **не** короткозамкнут: он пересчитывается на каждом ``run``/``resume``.
Поэтому фаза, завершившаяся ``skipped``, не имеет права оставлять мутацию, которую
следующий прогон применил бы повторно (иначе побочный эффект «проточится» на каждый
resume). Единственное допустимое поведение пропуска — чистое наблюдение/детект
предпосылки; любая запись в store должна быть идемпотентной. Персистится лишь сам
статус фазы (``set_phase_state``); ``skip_reason``/``pending`` **намеренно не**
персистятся (resume'у нужен только статус) — они поверхностны и доступны в CLI-JSON
(``_run_report_payload``) и в ctx-логе фазы (``observability``).

Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable

from core.domain import (
    Cell,
    Gate,
    GateDecision,
    GatePolicy,
    GateSource,
    GateVerdict,
    SkipReason,
)
from core.pipeline.gate_subject import (
    GateSubject,
    SubjectResolver,
    describe_subject_diff,
    read_gate_request_subject,
    write_gate_request,
)
from core.pipeline.gates import (
    NON_BLOCKING_GATES,
    NON_BLOCKING_REF_PREFIX,
    WAIVER_REF_PREFIX,
    evaluate_gate,
    record_decision,
)
from core.pipeline.profile_attestation import ProfileGateResult
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
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

__all__ = [
    "GATE_PHASES",
    "NON_BLOCKING_GATES",
    "PHASES",
    "Clock",
    "PhaseFn",
    "PhaseResult",
    "PhaseStatus",
    "Ports",
    "RunContext",
    "RunReport",
    "Runner",
    "run_isolated",
]

#: Статус фазы. ``ok`` — отработала, незакрытого не осталось; ``skipped`` — предпосылка
#: отсутствовала, работы не было (**не** сбой; прогон продолжается); ``blocked`` — остановка
#: на человеческом гейте без решения; ``failed`` — ошибка. ``skipped`` — не то же самое, что
#: ``ok`` (нечего делать ≠ сделано) и не то же, что ``failed``; причина пропуска —
#: :attr:`PhaseResult.skip_reason` (:class:`~core.domain.enums.SkipReason`).
PhaseStatus = Literal["ok", "skipped", "blocked", "failed"]

#: Порядок фаз конвейера (02 §A.3). Гейты идут отдельными фазами; блокирует прогон только
#: ``g4`` (остальные пишут след и идут дальше — :data:`~core.pipeline.gates.NON_BLOCKING_GATES`).
#:
#: ``g1``/``g2`` — внутренняя аттестация (профиль пакета-якоря, матрица роль×продукт);
#: ``g3``/``g4`` — точки интеграции: strict-ревью и утверждение публикации выполняются
#: внешними системами, а coHound экспортирует предмет решения и записывает ответ.
#: Фаза ``review`` — экспортная поверхность очереди ревью (advisory), она же источник
#: ``export`` для G3.
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

# Рассогласование объявлений — ошибка сборки, а не данных: проверка исполняется всегда
# (не ``assert``, который исчезает под ``python -O``), поэтому рассинхрон не переживёт импорт.
if not set(GATE_PHASES) <= set(PHASES):
    raise RuntimeError(
        f"фазы-гейты обязаны входить в PHASES: {sorted(set(GATE_PHASES) - set(PHASES))}"
    )


def sorted_error_lines(errors: Iterable[str]) -> list[str]:
    """Детерминированный порядок строк ``errors`` **при записи в артефакт** (17 §6, F3).

    ``errors`` наполняется по мере событий (сеть, пул воркеров, изоляция записей), поэтому
    при одинаковом результате два прогона давали разный порядок строк — артефакт зависел от
    планировщика. Здесь список приводится к лексикографическому порядку в момент
    публикации (payload прогона, ``PhaseResult``), а не в момент создания: накопление
    остаётся хронологическим (по нему видно, что случилось первым), сортируется только
    записываемая форма.

    **Дедупликации нет намеренно:** две одинаковые строки — это два события, и схлопывание
    скрыло бы повтор (например, один и тот же отказ на двух попытках). ``sorted`` устойчив
    относительно равных элементов, так что дубликаты сохраняют взаимный порядок.
    """
    return sorted(str(error) for error in errors)


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

    ``pending`` — незакрытые пункты, требующие человека (напр. очередь ревью, Ш7).
    **Advisory**: ``pending`` никогда не меняет ``status`` и не порождает автоматический
    ``N/A`` — иначе I6/I7 были бы нарушены (N/A и преодоление ``❓`` — только человеком,
    не автоматикой).

    **Инвариант (код, не комментарий).** ``skip_reason`` заполнен **тогда и только тогда**,
    когда ``status == "skipped"`` (см. :meth:`__post_init__`): ``skipped`` без причины —
    немая неполнота, а причина у ``ok``/``blocked``/``failed`` — противоречивая запись
    (у ``blocked``/``failed`` причина иная — ``errors``).
    """

    status: PhaseStatus
    phase: str
    cells: list[Cell] = field(default_factory=list)
    log_entries: list[Any] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    skip_reason: SkipReason | None = None
    pending: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Инвариант: ``skip_reason`` задан ⟺ ``status == "skipped"``.

        Словарь статусов не позволяет пользоваться ими несогласованно: ``skipped`` без
        :attr:`skip_reason` (немая неполнота) и :attr:`skip_reason` при любом другом
        статусе (противоречивая запись) одинаково запрещены.
        """
        if (self.skip_reason is not None) != (self.status == "skipped"):
            raise ValueError(
                "PhaseResult: skip_reason обязан быть задан ⟺ status == 'skipped' "
                f"(status={self.status!r}, skip_reason={self.skip_reason!r})"
            )


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

    ``registry_fetchers`` — карта ``registry_id → RegistryFetcherPort``, собранная
    composition root из package-scoped спецификаций
    (``packages/{case_id}/registries/*.yaml``, E2/03 §D.4):
    реестровая волна Ш3 передаёт её в ``registry_wave``. ``None``/пусто → реестры не
    сконфигурированы (волна пуста, без ячеек). ``registry_spec_digest`` — стабильный
    дайджест загруженных спецификаций (провенанс манифеста, §13 ``execution_change``);
    персистится в ``runs.registry_spec_digest`` и читается обратно ``read_manifest``, поэтому
    ``hound diff`` действительно видит смену спецификации реестра. ``None`` — спецификаций нет.
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
    #: B3: инъекция читателя контейнеров — ``Callable[[path], ContainerPort | None]``
    #: (composition root передаёт ``core.adapters.container_any.container_for``). Фаза
    #: корпуса не импортирует адаптеры: читатель — такой же внешний порт, как краулер.
    container: Callable[[Any], ContainerPort | None] | None = None
    registry_fetchers: Mapping[str, RegistryFetcherPort] | None = None
    registry_spec_digest: str | None = None


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
    Во избежание путаницы в CLI-JSON это поле сериализуется под ключом ``resumed_skipped``
    (см. :func:`core.cli.context._run_report_payload`), а ``PhaseStatus.skipped`` —
    как ``results[].status == "skipped"``. ``stopped_at`` — фаза, остановившая прогон:
    только ``blocked`` или ``failed``.
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


@dataclass(frozen=True)
class GateRuntime:
    """Внешние зависимости фазы-гейта: то, чего нет в :class:`RunContext`.

    :param subject: резолвер предмета решения ``Gate → GateSubject | None``. ``None`` —
        отпечаток предмета не вычисляется, и гейт сверяет решения без привязки
        (legacy-поведение; так работают вызовы из тестов/selfcheck).
    :param profile_gate: резолвер аттестации профиля (второй путь G1/G2, см.
        :func:`core.pipeline.profile_attestation.make_profile_gate`).
    :param policies: политика гейтов из ``run.yaml:gates`` (``{g3: external|waived}``).
        Действует только для делегируемых гейтов G3/G4; отсутствие ключа — ``external``.
    :param runs_dir: корень прогонов — куда писать запрос на решение
        (``runs/{run_id}/gates/{gate}-request.json``) и откуда читать предыдущий предмет
        для диагностики устаревшего решения.
    :param clock: часы для waiver-строки (тестируемость); ``None`` — системное UTC.
    """

    subject: SubjectResolver | None = None
    profile_gate: Callable[[Gate], ProfileGateResult] | None = None
    policies: Mapping[str, GatePolicy] = field(default_factory=dict)
    runs_dir: str | Path | None = None
    clock: Any = None


#: Гейты, которые можно делегировать внешнему процессу (``run.yaml:gates``).
_DELEGABLE_GATES: frozenset[Gate] = frozenset({Gate.G3, Gate.G4})


def _gate_policy(runtime: GateRuntime | None, gate: Gate) -> GatePolicy:
    """Политика гейта: ``waived`` только для G3/G4 и только если так записано в ``run.yaml``."""
    if runtime is None or gate not in _DELEGABLE_GATES:
        return GatePolicy.EXTERNAL
    raw = runtime.policies.get(gate.value) or runtime.policies.get(gate.value.lower())
    if raw is None:
        return GatePolicy.EXTERNAL
    return raw if isinstance(raw, GatePolicy) else GatePolicy(raw)


def _resolve_gate_subject(
    runtime: GateRuntime | None, gate: Gate
) -> GateSubject | None:
    """Предмет решения гейта (``None`` — резолвера нет либо он не смог его построить)."""
    if runtime is None or runtime.subject is None:
        return None
    return runtime.subject(gate)


def _previous_subject(
    runtime: GateRuntime | None, run_id: str, gate: Gate
) -> dict[str, Any] | None:
    """Материал предмета из последнего запроса на решение (``None`` — запроса нет).

    Именно запрос — запись того состояния, о котором спрашивали внешнюю систему: по нему
    объясняется, **что** изменилось с момента решения. Файла нет (решение записано
    out-of-band) — расхождение объясняется без перечисления компонентов, честно.
    """
    if runtime is None or runtime.runs_dir is None:
        return None
    return read_gate_request_subject(runtime.runs_dir, run_id, gate)


def _write_gate_request(
    runtime: GateRuntime | None,
    run_id: str,
    subject: GateSubject | None,
) -> Path | None:
    """Пишет запрос на решение, если есть куда и есть о чём (иначе ``None``)."""
    if subject is None or runtime is None or runtime.runs_dir is None:
        return None
    return write_gate_request(runtime.runs_dir, run_id, subject)


def _request_hint(gate: Gate, subject: GateSubject | None, path: Path | None) -> str:
    """Действенная подсказка: где запрос и какой командой ответить."""
    if subject is None:
        return (
            " Предмет решения не вычислен (нет контекста резолвера) — запрос на решение "
            "не записан; гейт принимает решение без привязки к предмету."
        )
    where = f" Запрос на решение: {path}." if path is not None else ""
    return (
        f"{where} Ответ: hound gate --gate {gate.value} --actor <кто> "
        f"--subject-digest {subject.digest} [--source external --external-ref <id>] "
        "--verdict approved|rejected --notes <почему>."
    )


def _stale_detail(
    runtime: GateRuntime | None,
    run_id: str,
    gate: Gate,
    subject: GateSubject | None,
) -> str:
    """Чем текущий предмет отличается от того, по которому принимали решение."""
    if subject is None:
        return "изменения предмета не перечислены: текущий предмет не вычислен"
    previous = _previous_subject(runtime, run_id, gate)
    if previous is None:
        return (
            "предыдущий предмет не сохранён (файла запроса нет), поэтому изменения не "
            "перечислены; текущие компоненты: "
            + ", ".join(sorted(subject.material))
        )
    changes = describe_subject_diff(previous, subject.material)
    if not changes:
        return "материал предмета совпадает; расхождение отпечатков — иного происхождения"
    return "изменения — " + "; ".join(changes)


def _rejected_message(gate: Gate, decision: GateDecision | None) -> str:
    """Явный отказ: кто отказал и почему (не «решения нет»)."""
    ref = (decision.external_ref if decision is not None else None) or (
        decision.actor if decision is not None else "?"
    )
    notes = (decision.notes if decision is not None else None) or "причина не указана"
    return f"гейт {gate.value} отклонён внешней системой ({ref}): {notes}"


def _rejected_stale_message(
    gate: Gate,
    decision: GateDecision | None,
    detail: str,
) -> str:
    """Отказ относится к устаревшему состоянию: повторное ревью может быть обосновано."""
    ref = (decision.external_ref if decision is not None else None) or (
        decision.actor if decision is not None else "?"
    )
    return (
        f"гейт {gate.value}: отклонение внешней системы ({ref}) относится к устаревшему "
        f"состоянию предмета; {detail}. Повторное ревью текущего состояния может быть "
        "обосновано."
    )


def _waiver_row(
    ctx: RunContext, gate: Gate, runtime: GateRuntime | None
) -> GateDecision:
    """Идемпотентно записывает waiver-строку (факт делегирования гейта наружу).

    Делегирование **не** прячется: строка в ``gate_decisions`` (``source='external'``,
    ``external_ref='waiver:...'``, ``verdict='approved'``) и запись в манифесте
    (``gate_waivers``) — единственное доказательство того, что прогон прошёл гейт **без**
    внутрисистемного одобрения. Повторный вызов строку не дублирует.
    """
    ref = f"{WAIVER_REF_PREFIX}run.yaml:gates.{gate.value.lower()}"
    for decision in ctx.store.iter_gate_decisions(ctx.run_id):
        if decision.gate is gate and decision.external_ref == ref:
            return decision
    notes = (
        f"гейт {gate.value} делегирован внешнему процессу "
        f"(run.yaml:gates.{gate.value.lower()}=waived): внутрисистемного одобрения нет"
    )
    return record_decision(
        ctx.store,
        ctx.run_id,
        gate,
        actor="external-process",
        target="waiver",
        notes=notes,
        clock=runtime.clock if runtime is not None else None,
        subject_digest=None,
        source=GateSource.EXTERNAL,
        external_ref=ref,
        verdict=GateVerdict.APPROVED,
    )


def _nonblocking_row(
    ctx: RunContext, gate: Gate, runtime: GateRuntime | None, reason: str
) -> GateDecision:
    """Идемпотентно записывает waiver-строку «решение не требуется» (не блокирующий гейт).

    Тот же механизм, что у :func:`_waiver_row`: строка в ``gate_decisions``
    (``source='external'``, ``external_ref='waiver:policy:nonblocking:<gate>'``,
    ``verdict='approved'``) — иначе :func:`evaluate_gate` принял бы её за одобрение, здесь
    она **не** решение, а запись «действующего решения нет и это не остановка». Причина
    названа в ``notes``: пропуск гейта обязан быть объяснён, а не подразумеваться.
    Повторный вызов строку не дублирует (поиск по ``external_ref``).
    """
    ref = f"{NON_BLOCKING_REF_PREFIX}{gate.value.lower()}"
    for decision in ctx.store.iter_gate_decisions(ctx.run_id):
        if decision.gate is gate and decision.external_ref == ref:
            return decision
    notes = (
        f"гейт {gate.value} не блокирует прогон: решение не требуется "
        f"(не блокирующий гейт, решение владельца); причина: {reason}"
    )
    return record_decision(
        ctx.store,
        ctx.run_id,
        gate,
        actor="policy",
        target="nonblocking",
        notes=notes,
        clock=runtime.clock if runtime is not None else None,
        subject_digest=None,
        source=GateSource.EXTERNAL,
        external_ref=ref,
        verdict=GateVerdict.APPROVED,
    )


def _nonblocking_result(
    ctx: RunContext,
    runtime: GateRuntime | None,
    gate: Gate,
    phase: str,
    reason: str,
    subject: GateSubject | None = None,
) -> PhaseResult:
    """Результат неблокирующего гейта без действующего решения: ``skipped`` + след.

    Причина остаётся видимой оператору (``log_entries``: почему решения нет — «нет строки»,
    «одобрение устарело», «профиль не аттестован»), а факт «гейт не блокирует» фиксируется
    строкой в ``gate_decisions`` и попадает в манифест (``gate_waivers``).

    Запрос на решение (``gates/{gate}-request.json``) пишется и здесь: это **экспорт
    предмета**, а не ожидание — из него ``hound gate`` берёт ``subject_digest``, поэтому
    записать решение по неблокирующему гейту (и увидеть его в следе) можно без ручного
    отпечатка. Файл детерминирован и на прогон не влияет.
    """
    _nonblocking_row(ctx, gate, runtime, reason)
    path = _write_gate_request(runtime, ctx.run_id, subject)
    return PhaseResult(
        status="skipped",
        phase=phase,
        skip_reason=SkipReason.GATE_NOT_BLOCKING,
        log_entries=[reason + _request_hint(gate, subject, path)],
    )


def _gate_phase(
    phase: str,
    gate: Gate,
    runtime: GateRuntime | None = None,
) -> PhaseFn:
    """Фаза-гейт: ``ok`` если гейт пройден, ``skipped`` если он не блокирующий, иначе ``blocked``.

    Гейт **не** проходится сам. Пути прохождения, в порядке приоритета:

    1. **Waiver** (только G3/G4) — ``run.yaml:gates.<gate>=waived``: решение делегировано
       внешнему процессу целиком. Фаза возвращает ``skipped`` с причиной
       ``gate_delegated_external`` и **записывает** waiver-строку: делегирование видно.
    2. **Per-run решение** — строка в ``gate_decisions`` (S9), **привязанная к текущему
       предмету** (``subject_digest``) и с вердиктом ``approved``. Устаревшее одобрение
       (предмет изменился) гейт не проходит — иначе одобрение действовало бы вечно.
    3. **Аттестация профиля** (только G1/G2) — ``profile_gate``: человек аттестовал
       содержимое профиля пакета-якоря, и текущий профиль совпадает с аттестованным.
       Профиль квази-константен (2–5 правок в год при еженедельных прогонах), поэтому
       повторное решение на каждый прогон не требуется.

    **Неблокирующие гейты** (:data:`~core.pipeline.gates.NON_BLOCKING_GATES` — G1/G2/G3):
    если действующего одобрения нет (нет строки / одобрение устарело / профиль не
    аттестован), фаза **не останавливает** прогон: возвращает ``skipped`` с причиной
    ``gate_not_blocking``, пишет waiver-строку «решение не требуется» и продолжает
    конвейер. Пропуск назван в логе фазы и виден в манифесте — невидимый waiver запрещён.

    **Отказ блокирует так же явно, как отсутствие решения, но другим сообщением.**
    ``verdict='rejected'`` по текущему предмету → «отклонён внешней системой (ref): notes».
    Отказ по **устаревшему** предмету (``rejected_stale``) у неблокирующего гейта не
    останавливает прогон (это отказ о прошлом состоянии, а не о текущем), но остаётся в
    записи с этой оговоркой. Молчание внешней системы не превращается в отказ и не
    выдаётся за одобрение.

    **На блоке пишется запрос на решение** — ``runs/{run_id}/gates/{gate}-request.json``
    (детерминированный, без меток времени): внешняя система получает предмет и
    ``subject_digest``, который обязан вернуть в решении. Неблокирующий гейт пишет тот же
    файл: это экспорт предмета (из него ``hound gate`` берёт отпечаток), а не ожидание —
    прогон файла не ждёт.

    :param runtime: зависимости сверх :class:`RunContext`. ``None`` — поведение прежнее
        (решения без привязки, без экспорта); так вызывают тесты и selfcheck.
    """

    def _fn(ctx: RunContext) -> PhaseResult:
        policy = _gate_policy(runtime, gate)
        subject = _resolve_gate_subject(runtime, gate)
        if policy is GatePolicy.WAIVED:
            _waiver_row(ctx, gate, runtime)
            return PhaseResult(
                status="skipped",
                phase=phase,
                skip_reason=SkipReason.GATE_DELEGATED_EXTERNAL,
                log_entries=[
                    f"гейт {gate.value} делегирован внешнему процессу "
                    f"(run.yaml:gates.{gate.value.lower()}=waived): "
                    "внутрисистемного одобрения нет"
                ],
            )

        non_blocking = gate in NON_BLOCKING_GATES
        digest = subject.digest if subject is not None else None
        check = evaluate_gate(ctx.store, ctx.run_id, gate, digest)
        if check.status == "passed":
            return PhaseResult(status="ok", phase=phase)
        if check.status == "rejected":
            # Явный отказ: сначала отказ, потом всё остальное — отказ не перекрывается
            # аттестацией профиля и не выдаётся за «решения нет». Он остаётся остановкой
            # и у неблокирующего гейта: отказ — записанное решение, а не молчание.
            return PhaseResult(
                status="blocked",
                phase=phase,
                errors=[_rejected_message(gate, check.decision)],
            )
        if gate in (Gate.G1, Gate.G2) and runtime is not None and runtime.profile_gate:
            verdict = runtime.profile_gate(gate)
            if verdict.passed:
                return PhaseResult(status="ok", phase=phase)
            if verdict.detail and check.status == "missing":
                if non_blocking:
                    return _nonblocking_result(
                        ctx, runtime, gate, phase, verdict.detail, subject
                    )
                path = _write_gate_request(runtime, ctx.run_id, subject)
                return PhaseResult(
                    status="blocked",
                    phase=phase,
                    errors=[verdict.detail + _request_hint(gate, subject, path)],
                )
        if check.status == "rejected_stale":
            detail = _stale_detail(runtime, ctx.run_id, gate, subject)
            message = _rejected_stale_message(gate, check.previous, detail)
            if non_blocking:
                return _nonblocking_result(
                    ctx, runtime, gate, phase, message, subject
                )
            path = _write_gate_request(runtime, ctx.run_id, subject)
            return PhaseResult(
                status="blocked",
                phase=phase,
                errors=[message + _request_hint(gate, subject, path)],
            )
        if check.status == "stale":
            detail = _stale_detail(runtime, ctx.run_id, gate, subject)
            message = (
                f"гейт {gate.value} не пройден: решение относится к устаревшему "
                f"состоянию предмета; {detail}."
            )
            if non_blocking:
                return _nonblocking_result(
                    ctx, runtime, gate, phase, message, subject
                )
            path = _write_gate_request(runtime, ctx.run_id, subject)
            return PhaseResult(
                status="blocked",
                phase=phase,
                errors=[message + _request_hint(gate, subject, path)],
            )
        message = f"гейт {gate.value} не пройден: нет строки в gate_decisions (S9)."
        if non_blocking:
            return _nonblocking_result(ctx, runtime, gate, phase, message, subject)
        path = _write_gate_request(runtime, ctx.run_id, subject)
        return PhaseResult(
            status="blocked",
            phase=phase,
            errors=[message + _request_hint(gate, subject, path)],
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

        Прогон останавливают только ``blocked`` (гейт, который ждёт решения: ``g4`` либо
        явный отказ по текущему предмету) и ``failed`` (ошибка). ``skipped`` — не
        остановка: фаза не нашла себе работы (в т.ч. неблокирующий гейт без решения), но
        следующие фазы обязаны исполниться.

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

        **Персистится только ``status`` (осознанно).** Resume'у нужна лишь строка статуса,
        поэтому :attr:`PhaseResult.skip_reason`/``pending`` в ledger **не** пишутся — их
        поверхностный след идёт в CLI-JSON (``_run_report_payload``) и ctx-лог фазы
        (``observability``). Расширение формы ledger до полной записи ``PhaseResult`` не
        требуется и не делается: единый источник строки статуса — ``set_phase_state``.
        """
        try:
            self.store.set_phase_state(run_id, phase, result.status)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            result.errors.append(f"set_phase_state({phase}): {type(exc).__name__}: {exc}")
