"""coHound — адаптеры фаз конвейера: сборка ``PhaseFn``-замыканий (L2, W4/W13).

Модуль — **точка сочленения** между ``Runner`` и конкретными реализациями фаз Ш1–Ш9.
Он не выполняет работу сам: каждая возвращаемая функция — тонкое замыкание, читающее
``ctx.store``/``ctx.ports`` и захваченный «бандл» (``inputs``/``plan``/параметры).
Композиция зависимостей остаётся за composition root (``cli/``): здесь нет ни одного
импорта ``core.adapters``/``core.edge`` и не инстанцируется ни один адаптер (A1).

**Границы ответственности.**

* ``normalize``/``lint``/``plan`` — явные no-op: предполётные ``load_inputs → lint → plan``
  выполняет composition root (CLI), а повторный ``plan()`` внутри фазы дал бы иной
  ``run_id`` и двойной линт. Фазы фиксируют факт «уже посчитано», не пересчитывая.
* ``discovery`` — no-op, пока не задан search-адаптер **и** запросы; иначе
  :func:`~core.pipeline.discovery.discover` и только кандидаты/гипотезы (I19, без evidence).
* ``registry``/``inheritance``/``corpus``/``evaluate``/``compute``/``report`` — делегаты
  к существующим реализациям; логика не дублируется.
* ``review`` — no-op: отдельного модуля ревью пока нет.
* Гейты ``g1``–``g4`` — **не** переопределяются: замыкания гейтов остаются дефолтными
  (``_gate_phase`` в ``runner.py``, S9).

**Канал между фазами.** Единственный разрешённый канал — замыкание: ``compute`` кладёт
:class:`~core.pipeline.compute.ScoreBundle` в изменяемый ``holder``, а ``report`` его
читает (плюс ``index_build_id``, захваченный из ``corpus``). Никаких глобальных
переменных и никаких записей в ``RunContext`` (его форма фиксирована, ``runner.py``).

Нейтрален (§1.3): нет названий продуктов/вендоров/реестров.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from core.domain.anchors import SourceResolver
from core.domain.enums import CrawlItemStatus, CrawlSessionStatus
from core.domain.models import (
    CrawlProvenance,
    CrawlSessionProvenance,
    Satellite,
)
from core.config.crawl_sources import CrawlSourcesConfig
from core.config.domain_profiles import DomainProfile
from core.pipeline.compute import compute, scope_universe
from core.pipeline.corpus import build_corpus
from core.pipeline.crawl_session import default_session_id
from core.pipeline.crawl_sources import crawl_sources_digest
from core.pipeline.discovery import discover
from core.pipeline.evaluate import (
    DEFAULT_RETRIEVAL_CUTOFF_K,
    DEFAULT_RETRIEVAL_PROFILE,
    evaluate_phase,
)
from core.pipeline.inheritance import inheritance_wave
from core.pipeline.registry import registry_wave
from core.pipeline.report import build_manifest, write_reports
from core.pipeline.retrieve import RetrievalProfile
from core.pipeline.runner import PhaseFn, PhaseResult, Ports, RunContext
from core.ports.types import FetchMode
from core.util.rate_limit import RateLimiter

if TYPE_CHECKING:  # только аннотации: без рантайм-импорта входа/плана
    from core.pipeline.normalize import RunInputs
    from core.pipeline.plan import RunPlan

__all__ = ["build_phases"]

#: Имена фаз, реализуемых этим модулем (гейты намеренно отсутствуют).
IMPLEMENTED_PHASES: tuple[str, ...] = (
    "normalize",
    "lint",
    "plan",
    "discovery",
    "registry",
    "inheritance",
    "corpus",
    "evaluate",
    "review",
    "compute",
    "report",
)


# ---------------------------------------------------------------------------
# Внутренние помощники
# ---------------------------------------------------------------------------


def _retrieval_params_json(holder: Mapping[str, Any]) -> str | None:
    """``retrieval_params_json`` манифеста в формате ячейки: ``{profile, cutoff_K}`` (R7/§13).

    Тот же ``sort_keys=True``-формат, что пишет :func:`core.pipeline.evaluate._provenance`,
    поэтому манифест и ячейки несут одно и то же провенанс-значение.
    """
    import json as _json

    profile = holder.get("retrieval_profile")
    cutoff = holder.get("retrieval_cutoff_K")
    if profile is None and cutoff is None:
        return None
    return _json.dumps(
        {"profile": profile, "cutoff_K": cutoff}, ensure_ascii=False, sort_keys=True
    )


def _as_phase(result: Any, phase: str) -> PhaseResult:
    """Приводит результат делегата к ``PhaseResult`` с именем фазы (контракт ``Runner``)."""
    if not isinstance(result, PhaseResult):
        raise TypeError(
            f"фаза {phase!r} вернула {type(result).__name__}, ожидался PhaseResult"
        )
    result.phase = phase
    return result


def _package_id(inputs: Any, plan: Any) -> str:
    """Пакет корпуса: ``inputs.package.package_id`` (если появится) либо ``plan.run_id``."""
    package = getattr(inputs, "package", None)
    return str(getattr(package, "package_id", None) or getattr(plan, "run_id", "") or "")


def _capture_index_build(holder: dict[str, Any], result: PhaseResult) -> None:
    """Захватывает ``index_build_id`` из ``log_entries`` фазы корпуса (провенанс манифеста)."""
    for entry in getattr(result, "log_entries", None) or ():
        build_id = getattr(entry, "index_build_id", None)
        if build_id:
            holder["index_build_id"] = build_id
            return


def _discovery_queries(inputs: Any) -> list[str]:
    """Запросы разведки, если их задал composition root (``inputs`` или ``inputs.run``)."""
    for holder in (inputs, getattr(inputs, "run", None)):
        if holder is None:
            continue
        value = getattr(holder, "discovery_queries", None)
        if value:
            return [str(query) for query in value]
    return []


def _created_at(ctx: RunContext) -> str | None:
    """Метка времени манифеста из ``ctx.clock`` (детерминизм/тестируемость)."""
    clock = getattr(ctx, "clock", None)
    now_iso = getattr(clock, "now_iso", None)
    if now_iso is None:
        return None
    return str(now_iso())


def _session_provenance_json(session: Any) -> dict[str, Any]:
    """Читает ``provenance_json`` сеанса (Phase 2) как dict; любая ошибка → ``{}``."""
    raw = getattr(session, "provenance_json", None)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _crawl_provenance(store: Any, inputs: Any, run_id: str) -> CrawlProvenance:
    """Собирает :class:`CrawlProvenance` из сеанса краула прогона (§4.4, Phase 2).

    Читает ``crawl_sessions`` (``get_crawl_session``), элементы сеанса
    (``iter_crawl_session_items``) и счётчики (``crawl_session_counts``). Движки/виды
    берутся из провенанса сеанса (``provenance_json``), записанного
    :func:`~core.pipeline.crawl_session.finalize_session`, объединяются с per-item
    ``engine``/``kind``. Дайджест — отпечаток ``inputs.crawl_sources`` (Phase 2). Любая
    ошибка чтения изолируется: провенанс **информационен** и не роняет фазу отчёта.
    """
    sources = getattr(inputs, "crawl_sources", None)
    try:
        digest = crawl_sources_digest(sources) if sources is not None else None
    except Exception:  # noqa: BLE001 — провенанс информационен
        digest = None

    sessions: list[CrawlSessionProvenance] = []
    engines: set[str] = set()
    kinds: set[str] = set()
    try:
        session_id = default_session_id(run_id)
        session = store.get_crawl_session(session_id)
        if session is not None:
            prov = _session_provenance_json(session)
            item_engines: set[str] = set()
            item_kinds: set[str] = set()
            for item in store.iter_crawl_session_items(session_id):
                if getattr(item, "engine", None):
                    item_engines.add(str(item.engine))
                if getattr(item, "kind", None):
                    item_kinds.add(str(item.kind))
            session_engines = sorted(
                {str(e) for e in (prov.get("engines") or [])} | item_engines
            )
            session_kinds = sorted(
                {str(k) for k in (prov.get("kinds") or [])} | item_kinds
            )
            counts = store.crawl_session_counts(session_id)
            total = sum(int(n) for n in counts.values())
            fetched = int(counts.get(CrawlItemStatus.FETCHED.value, 0))
            failed = int(counts.get(CrawlItemStatus.FAILED.value, 0)) + int(
                counts.get(CrawlItemStatus.CANCELLED.value, 0)
            )
            sessions.append(
                CrawlSessionProvenance(
                    session_id=session_id,
                    profile=str(session.profile),
                    status=str(session.status),
                    engines=session_engines,
                    kinds=session_kinds,
                    total=total,
                    fetched=fetched,
                    failed=failed,
                )
            )
            engines |= set(session_engines)
            kinds |= set(session_kinds)
    except Exception:  # noqa: BLE001 — провенанс информационен, фазу не роняем
        pass

    return CrawlProvenance(
        sessions=sessions,
        engines=sorted(engines),
        kinds=sorted(kinds),
        crawl_sources_digest=digest,
    )


# ---------------------------------------------------------------------------
# Публичная сборка фаз
# ---------------------------------------------------------------------------


def build_phases(
    *,
    inputs: RunInputs,
    plan: RunPlan,
    ports: Ports,
    urls: Mapping[str, list[str]] | None = None,
    passports: Mapping[str, Any] | None = None,
    out_dir: str = "runs",
    raw_root: str = "corpus_raw",
    snapshot_date: Any = None,
    retrieval_profile: RetrievalProfile | str | None = None,
    cutoff_k: int | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    explicit_fetch: Mapping[str, FetchMode] | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    rate_limiter: RateLimiter | None = None,
    satellites: Sequence[Satellite] = (),
    resolver: SourceResolver | None = None,
    strict: bool = True,
    deploy_profile: str | None = None,
    edge_schema_version: str | None = None,
    edge_capabilities: Mapping[str, Any] | None = None,
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
) -> dict[str, PhaseFn]:
    """Возвращает реестр ``{phase_name: PhaseFn}`` для не-гейтовых фаз конвейера.

    Композиция: ``Runner(store, ports, clock, phases=build_phases(...))``. Гейты
    ``g1``–``g4`` не входят в словарь — их (де)фолтные замыкания остаются в ``Runner``,
    поэтому прогон по-прежнему останавливается на человеке (S9).

    :param inputs: нормализованные входы (``RunInputs``): ``run``/``package``/``baselines``.
    :param plan: детерминированный план прогона (``RunPlan``): ``run_id``/``params``/веса.
    :param ports: внедрённые порты (оценщик, индексы, краулер/экстрактор/чанкер, реестр).
    :param urls: ``product_id → [url]`` для Ш5 (план URL не несёт — тонкий вход).
    :param passports: ``product_id → Passport`` для Ш4 (наследование).
    :param out_dir: базовый каталог отчётов (Ш9); composition root передаёт resolved
        ``paths.runs`` (``03`` §A.1). Дефолт ``"runs"`` совпадает с :class:`PathsConfig`.
    :param raw_root: корень retention сырых байтов (Ш5); composition root передаёт resolved
        ``paths.corpus_raw``. Дефолт ``"corpus_raw"`` совпадает с :class:`PathsConfig`.
    :param snapshot_date: окно снимка (Ш5); ``None`` → ``plan``/когорта.
    :param retrieval_profile: зафиксированный профиль R0/R1/R2 (I16); ``None`` → дефолт Ш6.
    :param cutoff_k: зафиксированный ``retrieval_cutoff_K`` (I20); ``None`` → дефолт Ш6.
    :param domain_profiles: типизированные доменные профили (D.2) для резолва ``FetchPlan``
        в фазе корпуса (Ш5).
    :param explicit_fetch: явные переопределения оси ``fetch`` по URL (``--render-modes``);
        побеждают профиль домена, но только по оси ``fetch`` (D2). ``None``/пусто → профиль.
    :param satellites: сателлиты как контекст промпта (I17, не доказательство).
    :param resolver: резолвер ``source_id`` → URL (I10/I15).
    :param strict: strict vs preliminary для Ш6 (I13).
    :param deploy_profile: профиль доверенных зон (``local``/``split``/…) для деплой-провенанса
        манифеста Ш9 (07 §7); ``None`` → не заполняется.
    :param edge_schema_version: версия контракта шва (``/v1/*``) для манифеста Ш9; ``None`` →
        не заполняется.
    :param edge_capabilities: версии edge-компонентов (gateway) для манифеста Ш9; ``None`` →
        пустой mapping (backward-compatible).
    :param max_attempts: Phase 4 — потолок попыток по ``(product_id, url)`` в Ш5
        (``--max-attempts``); исчерпавшие URL → терминальный ``FAILED`` ``attempts_exhausted``.
        ``None`` → без потолка.
    :param progress: Phase 4 — колбэк снимка прогресса сеанса краула (``--follow``); ошибки
        изолируются в :func:`core.pipeline.corpus.build_corpus`. ``None`` → без прогресса.

    Каждое замыкание детерминировано и не импортирует адаптеры; единственный
    межфазовый канал — замыкание ``holder`` (см. docstring модуля).
    """
    holder: dict[str, Any] = {}

    # -- Ш1–Ш2: предполёт уже выполнен composition root (CLI) ---------------- #
    def normalize_phase(ctx: RunContext) -> PhaseResult:
        """Ш1: no-op — нормализацию входов выполняет composition root (CLI) до ``Runner``."""
        return PhaseResult(status="ok", phase="normalize")

    def lint_phase(ctx: RunContext) -> PhaseResult:
        """Ш2: no-op — блокирующий линт выполняет composition root (CLI) до ``Runner``."""
        return PhaseResult(status="ok", phase="lint")

    def plan_phase(ctx: RunContext) -> PhaseResult:
        """Ш2: no-op — план строит composition root (CLI); повторный ``plan()`` запрещён."""
        return PhaseResult(status="ok", phase="plan")

    # -- Ш2: разведка (кандидаты, не evidence, I19) ------------------------- #
    discovery_queries = _discovery_queries(inputs)

    def discovery_phase(ctx: RunContext) -> PhaseResult:
        """Ш2: разведка кандидатов; без адаптера/запросов — no-op (I19: не evidence)."""
        adapter = getattr(ctx.ports, "search", None)
        if adapter is None or not discovery_queries:
            return PhaseResult(status="ok", phase="discovery")
        report = discover(
            queries=list(discovery_queries),
            adapter=adapter,
            phase="discovery",
        )
        return PhaseResult(status="ok", phase="discovery", log_entries=[report])

    # -- Ш3: реестровая волна (без LLM) ------------------------------------- #
    def registry_phase(ctx: RunContext) -> PhaseResult:
        """Ш3: реестровая волна. ``Ports.registry`` — один фетчер → адаптер в mapping."""
        registry = getattr(ctx.ports, "registry", None)
        fetchers: dict[str, Any] = (
            {str(registry.registry_id): registry} if registry is not None else {}
        )
        return _as_phase(registry_wave(plan, fetchers, ctx.store), "registry")

    # -- Ш4: наследование upstream (без LLM) -------------------------------- #
    def inheritance_phase(ctx: RunContext) -> PhaseResult:
        """Ш4: волна наследования по ``baselines`` и паспортам (I8)."""
        baselines = list(getattr(inputs, "baselines", None) or [])
        return _as_phase(
            inheritance_wave(plan, baselines, passports or {}, ctx.store),
            "inheritance",
        )

    # -- Ш5: сборка корпуса и индекса --------------------------------------- #
    def corpus_phase(ctx: RunContext) -> PhaseResult:
        """Ш5: retention → extract → chunk → index; I19 обеспечивается ``build_corpus``."""
        result = build_corpus(
            plan,
            getattr(ctx.ports, "crawler", None),  # type: ignore[arg-type]
            getattr(ctx.ports, "extractor", None),  # type: ignore[arg-type]
            getattr(ctx.ports, "chunker", None),  # type: ignore[arg-type]
            ctx.store,
            urls=urls or {},
            snapshot_date=snapshot_date,
            embedder=getattr(ctx.ports, "embedder", None),
            index_builder=getattr(ctx.ports, "index_builder", None),
            raw_root=raw_root,
            package_id=_package_id(inputs, plan),
            domain_profiles=domain_profiles or {},
            explicit_fetch=explicit_fetch,
            # Lane A/L3a: слои источников краула и карта per-engine краулеров; пейсинг —
            # единый владелец (RateLimiter), протягивается composition root'ом.
            crawl_sources=crawl_sources,
            crawlers=getattr(ctx.ports, "crawlers", None),
            # Phase 5.2: карта ``kind → PollPort`` для источников-каталогов (feed/channel).
            pollers=getattr(ctx.ports, "pollers", None),
            rate_limiter=rate_limiter,
            # Phase 3.3: исполнитель краула (``split``/edge-планировщик). ``None`` → локальный
            # per-URL путь (``crawler``/``crawlers``); при наличии — краул через шов.
            executor=getattr(ctx.ports, "executor", None),
            # D4 (P2-3): секция ``crawl`` run.yaml → дефолты оси стратегии/лимита для ``FetchPlan``.
            # ``RunConfig`` доступен как ``inputs.run`` (RunInputs.run), поле ``crawl`` опционально.
            crawl_defaults=getattr(getattr(inputs, "run", None), "crawl", None),
            # D3: durable-краул (аудит/очередь/resume) привязан к прогону; store уже есть.
            run_id=getattr(plan, "run_id", None),
            # Phase 4: потолок попыток (--max-attempts) и колбэк прогресса (--follow).
            max_attempts=max_attempts,
            progress=progress,
        )
        _capture_index_build(holder, result)
        return _as_phase(result, "corpus")

    # -- Ш6: оценка (ретривал → оценщик) ------------------------------------ #
    def evaluate_phase_fn(ctx: RunContext) -> PhaseResult:
        """Ш6: делегирует :func:`~core.pipeline.evaluate.evaluate_phase` (I16–I20)."""
        # Разрешённые профиль/K фиксируются в holder для провенанса манифеста Ш9 (R7/§13):
        # ячейка несёт те же значения, что и манифест (иначе манифест — не самостоятельная
        # запись воспроизведения). Дефолты — те же, что применяет evaluate_phase.
        holder["retrieval_profile"] = (
            retrieval_profile.value
            if isinstance(retrieval_profile, RetrievalProfile)
            else retrieval_profile
        ) or DEFAULT_RETRIEVAL_PROFILE.value
        holder["retrieval_cutoff_K"] = (
            int(cutoff_k) if cutoff_k is not None else DEFAULT_RETRIEVAL_CUTOFF_K
        )
        return evaluate_phase(
            plan=plan,
            ports=ctx.ports,
            store=ctx.store,
            run_id=plan.run_id,
            satellites=satellites,
            resolver=resolver,
            profile=retrieval_profile,
            cutoff_k=cutoff_k,
            strict=strict,
        )

    # -- Ревью: модуля нет ------------------------------------------------ #
    def review_phase(ctx: RunContext) -> PhaseResult:
        """Ревью: no-op — отдельного модуля ревью в ядре пока нет (I6/I7 не автоматизируем)."""
        return PhaseResult(status="ok", phase="review")

    # -- Ш8: расчёт (бандл → holder для report) ----------------------------- #
    def compute_phase(ctx: RunContext) -> PhaseResult:
        """Ш8: считает ``ScoreBundle`` и кладёт его в ``holder`` для фазы отчёта."""
        catalog = {p.param_id: p for p in plan.params}
        module_weights = (
            getattr(plan, "renormalized_weights", None)
            or getattr(plan, "module_weights", None)
            or {}
        )
        run = getattr(inputs, "run", None)
        universe = scope_universe(catalog, plan.products)
        ranking_epsilon = getattr(run, "ranking_epsilon", None) or 0.0
        bundle = compute(
            store=ctx.store,
            run_id=plan.run_id,
            catalog=catalog,
            module_weights=module_weights,
            cohort=getattr(plan, "cohort", None),
            requested_ranking=getattr(run, "ranking_policy", None),
            universe=universe,
            ranking_epsilon=ranking_epsilon,
        )
        holder["bundle"] = bundle
        return PhaseResult(status="ok", phase="compute")

    # -- Ш9: отчёты (читает holder) ----------------------------------------- #
    def report_phase(ctx: RunContext) -> PhaseResult:
        """Ш9: собирает манифест и артефакты; без бандла — ``failed`` (compute не выполнен)."""
        bundle = holder.get("bundle")
        if bundle is None:
            return PhaseResult(
                status="failed",
                phase="report",
                errors=["compute-бандл отсутствует: фаза compute не выполнена"],
            )
        manifest = build_manifest(
            inputs=inputs,
            plan=plan,
            bundle=bundle,
            index_build_id=holder.get("index_build_id"),
            created_at=_created_at(ctx),
            deploy_profile=deploy_profile,
            edge_schema_version=edge_schema_version,
            edge_capabilities=edge_capabilities,
            retrieval_profile=holder.get("retrieval_profile"),
            retrieval_cutoff_K=holder.get("retrieval_cutoff_K"),
            retrieval_params_json=_retrieval_params_json(holder),
            crawl_provenance=_crawl_provenance(ctx.store, inputs, plan.run_id),
        )
        # R7/§13: финальный манифест — самостоятельная запись воспроизведения,
        # поэтому персистится в операционную БД прогона (версии/индекс/провенанс).
        ctx.store.persist_manifest(plan.run_id, manifest)
        # I3/§5: протоколы исчерпания идут в nda_questions и в сбор аномалий.
        exhaustions = {
            (exhaustion.product_id, exhaustion.param_id): exhaustion
            for exhaustion in ctx.store.iter_exhaustions(plan.run_id)
        }
        artifacts = write_reports(
            bundle=bundle,
            cells=list(ctx.store.iter_cells(plan.run_id)),
            catalog={p.param_id: p for p in plan.params},
            manifest=manifest,
            out_dir=out_dir,
            run_id=plan.run_id,
            exhaustions=exhaustions,
        )
        holder["artifacts"] = artifacts
        return PhaseResult(status="ok", phase="report", log_entries=[artifacts])

    return {
        "normalize": normalize_phase,
        "lint": lint_phase,
        "plan": plan_phase,
        "discovery": discovery_phase,
        "registry": registry_phase,
        "inheritance": inheritance_phase,
        "corpus": corpus_phase,
        "evaluate": evaluate_phase_fn,
        "review": review_phase,
        "compute": compute_phase,
        "report": report_phase,
    }
