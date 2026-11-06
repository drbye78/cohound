"""coHound — адаптеры фаз конвейера: сборка ``PhaseFn``-замыканий (L2, W4/W13).

Модуль — **точка сочленения** между ``Runner`` и конкретными реализациями фаз Ш1–Ш9.
Он не выполняет работу сам: каждая возвращаемая функция — тонкое замыкание, читающее
``ctx.store``/``ctx.ports`` и захваченный «бандл» (``inputs``/``plan``/параметры).
Композиция зависимостей остаётся за composition root (``cli/``): здесь нет ни одного
импорта ``core.adapters``/``core.edge`` и не инстанцируется ни один адаптер (A1).

**Границы ответственности.**

* ``normalize``/``lint``/``plan`` — явные no-op: предполётные ``load_inputs → lint → plan``
  выполняет composition root (CLI), а повторный ``plan()`` внутри фазы дал бы иной
  ``run_id`` и двойной линт. Фазы возвращают ``ok`` с самодокументирующей заметкой в
  ``log_entries`` («сделано предполётом»), чтобы читатель отличал «сделано в другом месте»
  от «работы не было» (последнее — ``skipped``).
* ``discovery`` — ``skipped`` с ``skip_reason`` (``no_search_adapter``/``no_queries``), пока
  не заданы search-адаптер **и** запросы; иначе :func:`~core.pipeline.discovery.discover`
  и только кандидаты/гипотезы (I19, без evidence).
* ``registry``/``inheritance``/``corpus``/``evaluate``/``compute``/``report`` — делегаты
  к существующим реализациям; логика не дублируется.
* ``review`` — ``ok`` + advisory ``pending`` (сводка :func:`~core.pipeline.review.review_queue`):
  сообщает, что осталось человеку, но **ничего не решает** за него (I6/I7 не автоматизируем;
  ``pending`` не меняет статус).
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
from pathlib import Path
from typing import TYPE_CHECKING, Any

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain.anchors import SourceResolver
from core.domain.enums import (
    CrawlItemStatus,
    DocumentLevel,
    EmbedDevice,
    PdfPipeline,
    RefreshPolicy,
    SkipReason,
    TerminationReason,
)
from core.domain.models import (
    CrawlProvenance,
    CrawlRunConfig,
    CrawlSessionProvenance,
    Satellite,
)
from core.pipeline.compute import bundle_for_run
from core.pipeline.corpus import build_corpus
from core.pipeline.crawl_frontier import resolve_crawl_limits
from core.pipeline.crawl_policy import CRAWL_POLICY_SKIPPED_REASON
from core.pipeline.crawl_session import default_session_id
from core.pipeline.crawl_sources import crawl_sources_digest
from core.pipeline.discovery import discover
from core.pipeline.embed_cache import EmbedCache
from core.pipeline.eval_cache import EvalVerdictCache, eval_conditions_from_run
from core.pipeline.evaluate import (
    DEFAULT_RETRIEVAL_CUTOFF_K,
    DEFAULT_RETRIEVAL_PROFILE,
    evaluate_phase,
)
from core.pipeline.exhaustion import ExhaustionFacts, contours_run
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.extraction_runtime import ExtractionPool
from core.pipeline.gates import gate_trail
from core.pipeline.inheritance import inheritance_wave
from core.pipeline.manual_artifacts import (
    load_manual_artifacts,
    manual_artifacts_summary,
)
from core.pipeline.normalize import load_scope_attestations
from core.pipeline.partial_docs import (
    human_line as partial_docs_human_line,
)
from core.pipeline.partial_docs import (
    partial_docs_summary,
)
from core.pipeline.precedence import collect_protected_cells
from core.pipeline.registry import (
    registry_covered_params,
    registry_wave,
    resolve_registry_ids,
)
from core.pipeline.report import (
    build_manifest,
    compact_search_effort,
    declared_identity_digests,
    write_reports,
)
from core.pipeline.retrieve import RetrievalProfile
from core.pipeline.review import review_queue
from core.pipeline.runner import PhaseFn, PhaseResult, Ports, RunContext
from core.ports.extractor import ExtractorPort
from core.ports.types import FetchMode
from core.util.rate_limit import RateLimiter

if TYPE_CHECKING:  # только аннотации: без рантайм-импорта входа/плана
    from core.pipeline.normalize import RunInputs
    from core.pipeline.plan import RunPlan

__all__ = [
    "MEASURED_RETRIEVAL_SETTING",
    "build_phases",
    "measured_conflict_message",
    "measured_retrieval_fixation",
    "resolve_measured_retrieval",
]

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


def _observed_retrieval(
    holder: Mapping[str, Any], store: Any, run_id: str
) -> tuple[str | None, int | None, str | None]:
    """Профиль/``K``/params ретривала для манифеста: держатель → ячейки (R7/§13).

    ``holder`` живёт в памяти одного процесса, поэтому на resume (фаза ``evaluate`` уже
    ``ok`` и короткозамкнута) он пуст — ровно как у ``index_build_id``. Ячейки несут тот
    же провенанс (``evaluate._provenance``), поэтому при пустом держателе читаем их:
    иначе ``run_manifest.yaml`` объявлял ``retrieval_profile: null`` при ячейках,
    оцененных профилем ``R0``, — то есть карта не называла прибор, которым получены
    вердикты, хотя ``retrieval_profile`` входит в ``EXECUTION_CONTRACT_FIELDS``.

    Несколько разных профилей в одном прогоне — неоднозначность (например, resume с
    другими аргументами): тогда профиль **не объявляется** (``None``), а не подменяется
    первым попавшимся — манифест не должен угадывать.
    """
    import json as _json

    profile = holder.get("retrieval_profile")
    cutoff = holder.get("retrieval_cutoff_K")
    if profile is not None:
        return profile, cutoff, _retrieval_params_json(holder)

    try:
        cells = list(store.iter_cells(run_id))
    except Exception:  # noqa: BLE001 — провенанс не роняет отчёт
        return None, None, None
    profiles = {cell.retrieval_profile for cell in cells if cell.retrieval_profile}
    if len(profiles) != 1:
        return None, None, None
    resolved_profile = next(iter(profiles))

    stored = {cell.retrieval_params_json for cell in cells if cell.retrieval_params_json}
    params_json = next(iter(stored)) if len(stored) == 1 else None
    if params_json is None:
        params_json = _retrieval_params_json(
            {"retrieval_profile": resolved_profile, "retrieval_cutoff_K": cutoff}
        )
    if cutoff is None:
        try:
            cutoff = _json.loads(params_json).get("cutoff_K")
        except (TypeError, ValueError, AttributeError):
            cutoff = None
    return resolved_profile, cutoff, params_json


#: Настройка, измеряемая экспериментом фиксации (``hound experiment retrieval``, A3).
#: Единственный писатель ``source: measured`` — та команда; run-путь только **читает**
#: персистентную фиксацию и не должен молча понижать её дефолтом (``default``).
MEASURED_RETRIEVAL_SETTING = "retrieval_profile"


def _profile_value(profile: Any) -> str | None:
    """Нормализует профиль ретривала (``StrEnum`` либо строка) к строковому значению."""
    if profile is None:
        return None
    return str(getattr(profile, "value", profile))


def measured_retrieval_fixation(store: Any, run_id: str) -> dict[str, Any] | None:
    """Возвращает персистентную измеренную фиксацию ретривала, если она есть (A3/§13).

    Измеренной считается настройка :data:`MEASURED_RETRIEVAL_SETTING`, чей провенанс в уже
    записанной строке ``runs`` помечен ``source: measured``. Значение+провенанс читаются из
    персистентного манифеста (``read_manifest``), а не пересчитываются «по предвидению»
    (A3). Любая ошибка чтения (нет строки/БД, чужой store) трактуется как «фиксации нет» —
    значения не выдумываются.
    """
    try:
        persisted = store.read_manifest(run_id)
    except Exception:  # noqa: BLE001 — нет строки/БД ⇒ нет фиксации
        return None
    provenance = getattr(persisted, "settings_provenance", None) or {}
    entry = provenance.get(MEASURED_RETRIEVAL_SETTING)
    if not isinstance(entry, dict) or entry.get("source") != "measured":
        return None
    profile = _profile_value(getattr(persisted, "retrieval_profile", None))
    if profile is None:
        return None
    return {
        "retrieval_profile": profile,
        "retrieval_cutoff_K": getattr(persisted, "retrieval_cutoff_K", None),
        "retrieval_params_json": getattr(persisted, "retrieval_params_json", None),
        "provenance": dict(entry),
    }


def measured_conflict_message(requested: Any, measured: str) -> str:
    """Сообщение отказа при явном профиле, противоречащем измеренному (A3/§13)."""
    return (
        f"настройка retrieval_profile измерена как {measured}; запрошен "
        f"{_profile_value(requested)}. Изменение измеренной настройки требует "
        "пере-измерения — выполните `hound experiment retrieval` (§13/A3)."
    )


def resolve_measured_retrieval(
    store: Any, run_id: str, explicit_profile: Any
) -> dict[str, Any] | None:
    """Резолвит измеренную фиксацию ретривала, отвергая противоречащий явный профиль.

    Возвращает фиксацию (см. :func:`measured_retrieval_fixation`) либо ``None``, если
    настройка не измерена. Явный профиль, **равный** измеренному, принимается; явный
    **противоречащий** — отвергается (A3): изменить измеренную настройку можно только
    пере-измерением.

    :raises ValueError: явный профиль противоречит измеренному.
    """
    fixation = measured_retrieval_fixation(store, run_id)
    if fixation is None:
        return None
    if explicit_profile is not None and _profile_value(explicit_profile) != fixation[
        "retrieval_profile"
    ]:
        raise ValueError(
            measured_conflict_message(explicit_profile, fixation["retrieval_profile"])
        )
    return fixation


def _as_phase(result: Any, phase: str) -> PhaseResult:
    """Приводит результат делегата к ``PhaseResult`` с именем фазы (контракт ``Runner``)."""
    if not isinstance(result, PhaseResult):
        raise TypeError(
            f"фаза {phase!r} вернула {type(result).__name__}, ожидался PhaseResult"
        )
    result.phase = phase
    return result


def resolve_package_id(inputs: Any, plan: Any = None) -> str:
    """Идентичность пакета для ``index_store/{package}/`` и ``partition_build_id`` (09 §8.2).

    Порядок разрешения:

    1. ``inputs.package.package_id`` — если модель когда-нибудь его получит;
    2. ``inputs.run.package`` — ``run.yaml:package`` (например, ``ru-pg-dbms``): это
       **настоящая** идентичность пакета и та же величина, что уже персистится в
       ``runs.package_id``;
    3. ``plan.cohort.case_id`` — идентификатор кейса, если прогон собран без ``run``;
    4. ``plan.run_id`` — последний резерв (прямой вызов фазы без пакета).

    У :class:`~core.domain.models.PackageManifest` поля ``package_id`` сегодня **нет**
    (``case_id`` — идентификатор кейса, а не каталога пакета), поэтому основным источником
    служит ``run.package``. ``run_id`` в качестве пакета делает партиции run-scoped и
    запрещает переиспользование между прогонами по построению, поэтому он не должен
    выбираться, пока известно имя пакета.
    """
    package = getattr(inputs, "package", None)
    explicit = getattr(package, "package_id", None)
    if explicit:
        return str(explicit)
    run_package = getattr(getattr(inputs, "run", None), "package", None)
    if run_package:
        return str(run_package)
    cohort = getattr(plan, "cohort", None) if plan is not None else None
    case_id = getattr(cohort, "case_id", None)
    if case_id:
        return str(case_id)
    if plan is None:
        return ""
    return str(getattr(plan, "run_id", "") or "")


def _package_id(inputs: Any, plan: Any) -> str:
    """Пакет корпуса: общий порядок :func:`resolve_package_id` (для фаз и CLI)."""
    return resolve_package_id(inputs, plan)


def _capture_index_build(holder: dict[str, Any], result: PhaseResult) -> None:
    """Захватывает ``index_build_id`` из ``log_entries`` фазы корпуса (провенанс манифеста)."""
    for entry in getattr(result, "log_entries", None) or ():
        build_id = getattr(entry, "index_build_id", None)
        if build_id:
            holder["index_build_id"] = build_id
            return


def _partition_reuse_note(result: PhaseResult) -> str | None:
    """Строка лога о партициях корпуса: переиспользовано vs собрано + композиция.

    ``IndexBuild.partitions_reused`` (09 §8.5) считался, но никуда не выводился —
    оператор не видел выигрыша переиспользования. Строка детерминирована (счётчики из
    сборки, id композиции — её ``index_build_id``) и **не появляется**, когда партиций
    нет (нечего рассказывать). Это не ``errors``: переиспользование — норма, не сбой.
    """
    build = next(
        (
            entry
            for entry in getattr(result, "log_entries", None) or ()
            if getattr(entry, "index_build_id", None)
        ),
        None,
    )
    if build is None:
        return None
    entries = list(getattr(build, "entries", None) or ())
    if not entries:
        return None
    reused = int(getattr(build, "partitions_reused", 0) or 0)
    built = max(0, len(entries) - reused)
    return (
        f"corpus: партиции — переиспользовано {reused}, собрано {built} "
        f"(композиция {build.index_build_id})"
    )


def _duplicate_notes(result: PhaseResult) -> list[str]:
    """Строки лога о дедупе по ``content_hash``: по одной на продукт с ненулевым счётом.

    ``IndexBuild.duplicates``/``duplicate_count`` считались, но никуда не выводились —
    цепочка «N версий-алиасов → 1 документ» была невидима оператору. Дедуп **легитимен**
    (I17: партиция продукта), поэтому это note, а не ``errors``; строка появляется
    только при ``>0`` (нулевой дедуп не засоряет отчёт). Порядок — по ``product_id``
    (детерминизм вывода).
    """
    build = next(
        (
            entry
            for entry in getattr(result, "log_entries", None) or ()
            if getattr(entry, "index_build_id", None)
        ),
        None,
    )
    if build is None:
        return []
    duplicates = getattr(build, "duplicates", None) or {}
    return [
        f"corpus: {product_id}: {int(count)} документов схлопнуто по content_hash "
        "(алиасы версий)"
        for product_id, count in sorted(duplicates.items())
        if int(count) > 0
    ]


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


def _observed_index_versions(store: Any, index_build_id: str | None) -> dict[str, Any]:
    """Наблюдаемые версии сборки из durable-манифеста ``index_manifest`` (провенанс Ш9).

    Нужны, когда живой ``holder`` пуст: на resume фаза ``corpus`` короткозамкнута
    персистентным ``ok``, поэтому версии прибора (эмбеддер, экстрактор, лексический движок,
    векторное хранилище) в процессе отчёта неизвестны. Пока их неоткуда было взять, манифест
    завершённого прогона публиковал ``embed_model_id: null`` при 9398 векторах, построенных
    bge-m3, — то есть молчал о приборе, которым сделано измерение (а провенанс прибора —
    это то, по чему решается, сравнима ли карта с прошлой).

    Источник — **манифест той сборки**, а не текущие порты: сборка описывает прогон,
    порты описывают окружение повторного запуска (после смены эмбеддера они другие).

    Партиции одной сборки обязаны сходиться в версиях; расхождение — сигнал дефекта, но
    провенанс не роняет отчёт: берётся значение первой партиции в детерминированном
    порядке (``iter_index_manifest`` сортирует по ``product_id``).
    """
    empty: dict[str, Any] = {}
    if not index_build_id:
        return empty
    try:
        partitions = list(store.iter_index_manifest(str(index_build_id)))
    except Exception:  # noqa: BLE001 — провенанс информационен, отчёт не роняем
        return empty
    if not partitions:
        return empty
    first = partitions[0]
    return {
        key: value
        for key, value in (
            ("embed_model_id", getattr(first, "embed_model_id", None)),
            ("extractor_version", getattr(first, "extractor_version", None)),
            ("lexical_engine_version", getattr(first, "lexical_engine_version", None)),
            ("vector_store", getattr(first, "vector_store", None)),
            ("chunker_version", getattr(first, "chunker_version", None)),
        )
        if value
    }


def _product_plan_provenance(
    store: Any, run_id: str, notes: list[str]
) -> dict[str, Any] | None:
    """План продуктов прогона из снимка входов (``run_inputs.product_plan``).

    ``RunManifest.product_plan``/``runs.product_plan_json`` были объявлены, писались и
    читались, но **производителя не было**: фаза отчёта значение не передавала, и колонка
    оставалась ``NULL`` навсегда — тот же класс «объявлено, но не написано», что закрывался
    у ``cycle_id``/``previous_run_id``. Без плана читатель артефакта не отличал полное
    измерение от среза ``--only-product``, не заглядывая в ``state.db``.

    Источник — **снимок входов плана** (``run_inputs``), который composition root пишет до
    исполнения фаз из ``onboarding.plan_set.model_dump(mode="json")``. Это ровно то же
    значение, а не второй расчёт: снимок v2 и ``runs.product_plan_json`` обязаны совпадать,
    иначе на один прогон было бы два разных «плана продуктов».

    ``None`` — снимка нет, он v1 (поле не носилось) либо значение не словарь: «не
    записано». Реконструировать план из ``product_modes`` (``product_id → mode``) запрещено:
    там нет ``scope``/``reasons``/``blocked``, и такая подстановка была бы догадкой (A3).
    Сбой чтения — замечание в ``notes``, а не падение фазы (как у провенанса краула).
    """
    try:
        payload = store.read_plan_inputs(run_id)
    except Exception as exc:  # noqa: BLE001 — провенанс не роняет отчёт
        notes.append(
            f"product_plan: {type(exc).__name__}: {exc} — план продуктов не прочитан"
        )
        return None
    if not isinstance(payload, Mapping):
        return None
    # Снимок адресуется ``run_id`` и в строке БД, и в самом payload: расхождение значит
    # «чужой/испорченный снимок», и значение из него не подставляется (как в
    # ``_plan_inputs_from_payload``).
    if payload.get("run_id") != run_id:
        return None
    raw = payload.get("product_plan")
    return dict(raw) if isinstance(raw, Mapping) else None


def _crawl_provenance(
    store: Any, inputs: Any, run_id: str, notes: list[str]
) -> CrawlProvenance:
    """Собирает :class:`CrawlProvenance` из сеанса краула прогона (§4.4, Phase 2).

    Читает ``crawl_sessions`` (``get_crawl_session``), элементы сеанса
    (``iter_crawl_session_items``) и счётчики (``crawl_session_counts``). Движки/виды
    берутся из провенанса сеанса (``provenance_json``), записанного
    :func:`~core.pipeline.crawl_session.finalize_session`, объединяются с per-item
    ``engine``/``kind``. Дайджест — отпечаток ``inputs.crawl_sources`` (Phase 2). Любая
    ошибка чтения изолируется: провенанс **информационен** и не роняет фазу отчёта.

    Изоляция не означает молчания: причина отказа дописывается в ``notes`` (канал
    ``pending`` фазы), потому что пустой провенанс без причины читался бы как «краула не
    было» — утверждение о прогоне, которого мы не проверяли.
    """
    sources = getattr(inputs, "crawl_sources", None)
    try:
        digest = crawl_sources_digest(sources) if sources is not None else None
    except Exception as exc:  # noqa: BLE001 — провенанс информационен
        digest = None
        notes.append(
            "crawl_provenance: дайджест crawl_sources не вычислен "
            f"({type(exc).__name__}: {exc})"
        )

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
                    truncated=bool(prov.get("truncated")),
                    truncated_products=sorted(
                        {str(p) for p in (prov.get("truncated_products") or [])}
                    ),
                    # F7: узел шарда — провенанс развёртывания (не ``run_id``). Читается
                    # из того же durable-блока, что писал ``finalize_session``.
                    node_id=(
                        str((prov.get("shard") or {}).get("node") or "") or None
                        if isinstance(prov.get("shard"), dict)
                        else None
                    ),
                )
            )
            engines |= set(session_engines)
            kinds |= set(session_kinds)
    except Exception as exc:  # noqa: BLE001 — провенанс информационен, фазу не роняем
        notes.append(
            "crawl_provenance: провенанс сеанса не прочитан "
            f"({type(exc).__name__}: {exc}): сеансы/движки в манифесте неполны"
        )

    return CrawlProvenance(
        sessions=sessions,
        engines=sorted(engines),
        kinds=sorted(kinds),
        crawl_sources_digest=digest,
    )


def _crawl_policy_provenance_facts(
    store: Any,
    state: Any,
    *,
    run_id: str,
    products: Sequence[str],
    refresh_policy: Any,
) -> dict[str, Any]:
    """Наблюдённые факты исполнения политики краула для манифеста (план 19 §P4).

    Источники **durable**, а не пересказ состояния в памяти: ``crawl_sessions.provenance_json``
    (записи корней с ``truncated_reason=crawl_policy_skipped``), ``corpus_coverage`` (причина
    ``crawl_policy_skipped`` у продукта без источников) и ``corpus_capture`` (время снятия
    для ``frozen``). Второй формулы нет: всё это уже записано исполнением.

    ``captured_at`` возвращается **только** при ``refresh_policy=frozen``: при прочих
    политиках снятия не было, и последнее известное чужое значение выдало бы чужое
    наблюдение за это (A3). Нет записи — ``None``, а не выдуманное время. Значение —
    **самое позднее** из записей снятия продуктов плана (снятие идёт по продуктам и
    может разойтись на секунды); полные per-product записи остаются в ``state.db``.

    Любая ошибка чтения изолируется: провенанс информационен и не роняет фазу отчёта.
    """
    frozen = str(refresh_policy) == RefreshPolicy.FROZEN.value

    skips: list[dict[str, Any]] = []
    try:
        session = store.get_crawl_session(default_session_id(run_id))
        if session is not None:
            prov = _session_provenance_json(session)
            roots = (prov.get("effective_limits") or {}).get("roots") or []
            for record in roots:
                if not isinstance(record, Mapping):
                    continue
                if str(record.get("truncated_reason")) != CRAWL_POLICY_SKIPPED_REASON:
                    continue
                skips.append(
                    {
                        "product_id": str(record.get("product_id") or ""),
                        "root": str(record.get("root") or ""),
                        "scope": record.get("policy_scope"),
                        "reasons": [
                            str(reason)
                            for reason in (record.get("policy_reasons") or ())
                        ],
                        "network_mode": record.get("network_mode"),
                        "corpus_source": record.get("corpus_source"),
                    }
                )
    except Exception:  # noqa: BLE001, S110 — провенанс не роняет отчёт
        pass
    skips.sort(key=lambda item: (item["product_id"], item["root"]))

    zero_reasons: dict[str, str] = {}
    try:
        for coverage in store.iter_corpus_coverage(run_id):
            reason = getattr(coverage, "reason", None)
            if str(reason) == CRAWL_POLICY_SKIPPED_REASON:
                zero_reasons[str(coverage.product_id)] = str(reason)
    except Exception:  # noqa: BLE001, S110 — провенанс не роняет отчёт
        pass

    captured_at: str | None = None
    if frozen and state is not None:
        reader = getattr(state, "last_corpus_capture", None)
        if callable(reader):
            values: list[str] = []
            for product_id in products:
                try:
                    capture = reader(product_id)
                except Exception:  # noqa: BLE001, S112 — провенанс не роняет отчёт
                    continue
                if capture is not None and getattr(capture, "captured_at", None):
                    values.append(str(capture.captured_at))
            if values:
                captured_at = max(values)

    return {
        "captured_at": captured_at,
        "policy_skips": skips,
        "policy_zero_reasons": dict(sorted(zero_reasons.items())),
    }


def _preflight_noop(phase: str, reason: str) -> PhaseResult:
    """Явный no-op фазы, чья работа выполнена предполётом composition root (CLI).

    Статус — ``ok``: работа **сделана** (в другом месте), а не пропущена (для «работы не
    было» есть ``skipped``). Самодокументирующая заметка в ``log_entries`` отличает
    «сделано предполётом» от «фаза ничего не сделала».
    """
    return PhaseResult(
        status="ok", phase=phase, log_entries=[f"preflight (CLI): {reason}"]
    )


def _review_pending_summary(items: Sequence[Any], *, preview: int = 3) -> str:
    """Ограниченная человекочитаемая сводка очереди ревью (advisory ``pending``, I6/I7).

    Возвращает **одну** строку вида
    ``review: N ячеек ожидают решения (states=preliminary,reviewed; первые: p1/param_a, …)``.
    Список ключей ограничен ``preview`` (а не всей очередью): ``pending`` — сигнал человеку,
    а не второй реестр; полная очередь — за командой ``hound review`` (гейта G3 нет: ревью — внешний контур).
    """
    states = ",".join(sorted({str(item.state.value) for item in items}))
    keys = ", ".join(f"{item.product_id}/{item.param_id}" for item in items[:preview])
    if len(items) > preview:
        keys += f" (+{len(items) - preview})"
    tail = f"; первые: {keys}" if keys else ""
    return f"review: {len(items)} ячеек ожидают решения (states={states}{tail})"


# ---------------------------------------------------------------------------
# Публичная сборка фаз
# ---------------------------------------------------------------------------


def _phase_done(ctx: RunContext, run_id: str, phase: str) -> bool:
    """Отработала ли фаза в **этом или прошлом** процессе (durable-состояние ``ok``).

    ``holder`` живёт в памяти одного процесса; после resume он пуст, хотя фаза уже
    выполнена. Без этой проверки фазы, которым нужен результат прошлого процесса,
    не могли бы продолжить прогон.
    """
    try:
        states = ctx.store.get_phase_state(run_id) or {}
    except Exception:  # noqa: BLE001 — чтение состояния не должно ронять отчёт
        return False
    return states.get(phase) == "ok"


def _serp_rounds(axes: Any) -> int:
    """Эффективные раунды SERP-контура из объявленных осей (``None``/0 — выключен).

    Ось разрешается **один** раз composition root'ом
    (:func:`core.cli.execution_axes.resolve_declared_axes`) и используется двумя
    потребителями: контрактом (``serp_rounds`` → ``run_id``) и фазой корпуса. Второй
    резолвер (со своим порядком приоритетов) дал бы прогон, у которого ``run_id``
    описывает не тот контур, который реально исполнялся.
    """
    try:
        return max(int(getattr(axes, "serp_rounds", 0) or 0), 0)
    except (TypeError, ValueError):
        return 0


def _serp_facts(store: Any, run_id: str) -> dict[str, Any]:
    """Наблюдённые факты SERP-контура из провенанса **сеанса** краула (I3/§5).

    Контур пишет своё наблюдение в ``crawl_sessions.provenance_json`` (ключ ``serp``,
    ``corpus._persist_serp_provenance``): ``StorePort`` не даёт отдельного места под факты
    поиска, а сеанс — durable-адрес того же краула, которого контур расширял.

    Пусто/сбой чтения → ``{}``: фактов нет, и завышать их нельзя («не наблюдал» ≠
    «исполнился»). Выключенный контур пишет ``enabled: false`` — это тоже факт: его
    отличаем от отсутствия контура по полю ``enabled``.
    """
    if not run_id:
        return {}
    try:
        session = store.get_crawl_session(default_session_id(str(run_id)))
    except Exception:  # noqa: BLE001 — факты информационны, протокол не роняем
        return {}
    if session is None:
        return {}
    serp = _session_provenance_json(session).get("serp")
    return serp if isinstance(serp, dict) else {}


def _observed_exhaustion_facts(
    ctx: RunContext,
    plan: Any,
    inputs: Any,
    passports: Mapping[str, Any] | None,
) -> ExhaustionFacts:
    """Наблюдённые факты для I3-протокола (какие контуры **реально** исполнились).

    Принцип: дешёвый факт или «не исполнялся». Занижение честно, завышение фабрикует
    доказательство исчерпания. Поэтому:

    * ``corpus`` — продукт, у которого **наблюдён** корпус: есть документ корпуса либо
      строка покрытия с ``observed_sources > 0``. Строка покрытия сама по себе — не факт
      исполнения: ``coverage_missing``/``no_seeds`` пишутся и для продукта, который не
      краулили вовсе (``observed_sources == 0``), и объявить по ним «корпус исчерпан»
      значило бы выдать отсутствие работы за исчерпание;
    * ``registry`` — волна реестров отработала (durable ``ok``) **и** конфигурация
      реально опрашивала реестры (фетчеры + покрытые параметры), иначе контура не было;
    * ``inheritance`` — фаза отработала (durable ``ok``) **и** у продукта есть upstream
      с совпадающей строкой ``baselines.csv`` (ровно условие, при котором волна Ш4
      рассматривает продукт);
    * ``serp`` — контур **реально** выпускал запросы к адаптеру внешнего поиска
      (``budget.used > 0`` по продукту из провенанса контура). Выключенный контур,
      объявленный без адаптера или без запросов контуром не считается: наличие
      ``serp`` в провенансе само по себе — не исполнение. Раунды/бюджет/запросы/
      терминация берутся оттуда же как **наблюдения**, а не из объявления.

    ``yielded`` — дал ли исполненный контур хоть что-то (документы корпуса либо ячейка
    высокого источника). Неизвестное не подменяется догадкой.
    """
    run_id = str(getattr(plan, "run_id", ""))
    products = [str(product) for product in getattr(plan, "products", []) or []]

    corpus_docs: set[str] = set()
    try:
        corpus_docs = {str(doc.product_id) for doc in ctx.store.iter_corpus_docs(run_id)}
    except Exception:  # noqa: BLE001 — нет прогона/чужой store ⇒ факта нет
        corpus_docs = set()
    coverage_products: set[str] = set()
    try:
        coverage_products = {
            str(row.product_id)
            for row in ctx.store.iter_corpus_coverage(run_id)
            # Строка покрытия без единого наблюдения — это «не краулили», а не
            # «корпус исчерпан»: ``coverage_missing``/``no_seeds`` пишутся и для
            # продукта вне работы. Засчитываем только наблюдённый корпус.
            if int(getattr(row, "observed_sources", 0) or 0) > 0
        }
    except Exception:  # noqa: BLE001 — покрытие информационно, факт не выдумываем
        coverage_products = set()
    corpus_products = corpus_docs | coverage_products

    registry_products: set[str] = set()
    if _phase_done(ctx, run_id, "registry"):
        fetchers = dict(getattr(ctx.ports, "registry_fetchers", None) or {})
        if fetchers and registry_covered_params(
            plan, resolve_registry_ids(plan, fetchers)
        ):
            # Волна Ш3 обходит **все** продукты плана при покрытых параметрах.
            registry_products = set(products)

    inheritance_products: set[str] = set()
    if _phase_done(ctx, run_id, "inheritance"):
        baselines = list(getattr(inputs, "baselines", None) or [])
        for product in products:
            passport = (passports or {}).get(product)
            upstream = getattr(passport, "upstream", None)
            if upstream is None:
                continue
            if any(row.baseline_id == upstream.baseline_id for row in baselines):
                inheritance_products.add(product)

    protected = collect_protected_cells(ctx.store, run_id)
    protected_products = {owner for owner, _ in protected}
    yielded = {
        product: product in corpus_docs or product in protected_products
        for product in products
    }

    # I3/§5: SERP-контур — наблюдённые факты из провенанса сеанса (пишет фаза корпуса).
    # «Исполнился» = адаптер действительно вызывался (``used > 0``), а не «контур объявлен».
    serp_facts = _serp_facts(ctx.store, run_id)
    serp_products: set[str] = set()
    serp_termination: dict[str, TerminationReason] = {}
    serp_queries: dict[str, list[str]] = {}
    serp_budget: dict[str, dict[str, Any]] = {}
    serp_rounds = 0
    if serp_facts.get("enabled"):
        serp_rounds = int((serp_facts.get("rounds") or {}).get("executed", 0) or 0)
        block = serp_facts.get("budget") or {}
        used = block.get("used") or {}
        exhausted = block.get("exhausted") or {}
        for product, count in used.items():
            if int(count or 0) <= 0:
                continue
            serp_products.add(str(product))
            serp_budget[str(product)] = {
                "declared": block.get("effective_per_product"),
                "used": int(count or 0),
                # Факт исчерпания — только явный флаг контура, не «остаток 0» вычислением.
                "exhausted": bool(exhausted.get(product)),
            }
        serp_queries = {
            str(product): list(values or ())
            for product, values in (serp_facts.get("queries") or {}).items()
            if str(product) in serp_products
        }
        for product, reason in (serp_facts.get("termination") or {}).items():
            if str(product) not in serp_products or not reason:
                continue
            try:
                serp_termination[str(product)] = TerminationReason(str(reason))
            except ValueError:
                serp_termination[str(product)] = TerminationReason.ERROR

    return ExhaustionFacts(
        contours_run=contours_run(
            products,
            corpus=corpus_products,
            serp=serp_products,
            registry=registry_products,
            inheritance=inheritance_products,
        ),
        rounds_by_product={
            product: (serp_rounds if product in serp_products else 0)
            for product in products
        },
        budget_by_product=serp_budget,
        queries_by_product=serp_queries,
        yielded_by_product=yielded,
        termination_by_product=serp_termination,
        exhausted_at=(
            ctx.clock.now_iso() if getattr(ctx, "clock", None) is not None else None
        ),
    )


def _gate_provenance(ctx: RunContext, plan: Any, inputs: Any) -> dict[str, Any]:
    """Политика гейтов и след гейтов для манифеста (§13: процесс, не измерение).

    ``gate_policies`` — что объявлено в ``run.yaml:gates``; ``gate_waivers`` — след гейтов,
    которые прогон **не блокировали**: записанное решение (``state=decided``),
    делегирование (``state=delegated``) или «решение не требуется» у неблокирующего гейта
    G1/G2/G3 (``state=not_required``). След строится **одним** механизмом —
    :func:`core.pipeline.gates.gate_trail` поверх ``gate_decisions``: второго списка в
    манифесте нет. Он обязателен: прогон, прошедший гейт без одобрения (или вообще без
    решения), не должен выглядеть как прогон с одобрением. В
    ``EXECUTION_CONTRACT_FIELDS`` эти поля не входят (иначе смена процедуры меняла бы
    ``run_id`` и рвала межцикловый diff).
    """
    policies = {
        str(key).lower(): getattr(value, "value", value)
        for key, value in (
            getattr(getattr(inputs, "run", None), "gates", None) or {}
        ).items()
    }
    return {
        "gate_policies": policies,
        "gate_waivers": gate_trail(ctx.store, plan.run_id),
    }


def _lineage_provenance(
    state: Any,
    run_id: str,
    *,
    declared_cycle: str | None,
    declared_previous: str | None,
    declared_change: str | None,
    notes: list[str],
) -> tuple[str | None, str | None, str | None]:
    """Цикл, предшественник и класс изменения для манифеста Ш9 (09 §2.4/§12.3/§13).

    Источник — **строка lineage текущего прогона**: ``change_class`` вычисляется при её
    записи (:func:`core.pipeline.run_identity.build_run_identity` →
    ``record_run_lineage``), поэтому второго расчёта здесь нет — только чтение.

    Строку пишет ``record_run_outcome`` **после** фаз (иначе она ссылалась бы на себя,
    ``core/cli/commands/run.py``), поэтому на свежем прогоне её ещё нет. Тогда берутся те
    же значения, переданные composition root из ``RunIdentity``: строка — их проекция, то
    есть это не второй расчёт и не догадка, а тот же факт до записи. На resume/adoption
    строка существует и побеждает: манифест обязан совпадать с ``state.db``, а не с
    идентичностью процесса, который мог запуститься с другим ``--since-run``.

    Сбой чтения — замечание в ``provenance_notes``, а не падение фазы: провенанс не
    роняет отчёт (тот же класс, что у провенанса краула). ``state is None`` — значения
    не записаны, и это честное ``None``, а не «истории нет».
    """
    if state is not None:
        try:
            row = state.get_run_lineage(run_id)
        except Exception as exc:  # noqa: BLE001 — провенанс не роняет отчёт
            notes.append(
                f"lineage: {type(exc).__name__}: {exc} — cycle_id/previous_run_id/"
                "change_class взяты из идентичности прогона"
            )
            row = None
        if row is not None:
            return row.cycle_id, row.previous_run_id, row.change_class
    return declared_cycle, declared_previous, declared_change


def build_phases(
    *,
    inputs: RunInputs,
    plan: RunPlan,
    ports: Ports,
    package_dir: str | None = None,
    urls: Mapping[str, list[str]] | None = None,
    passports: Mapping[str, Any] | None = None,
    out_dir: str = "runs",
    raw_root: str = "corpus_raw",
    index_root: str | None = None,
    snapshot_date: Any = None,
    retrieval_profile: RetrievalProfile | str | None = None,
    cutoff_k: int | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    explicit_fetch: Mapping[str, FetchMode] | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    rate_limiter: RateLimiter | None = None,
    extraction_pool: ExtractionPool | None = None,
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None = None,
    extract_cache: ExtractionCache | None = None,
    embed_cache: EmbedCache | None = None,
    eval_cache: EvalVerdictCache | None = None,
    satellites: Sequence[Satellite] = (),
    resolver: SourceResolver | None = None,
    strict: bool = True,
    deploy_profile: str | None = None,
    edge_schema_version: str | None = None,
    edge_capabilities: Mapping[str, Any] | None = None,
    llm_rnd_endpoint: bool | None = None,
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
    on_product: Callable[[str, str], None] | None = None,
    max_depth: int | None = None,
    max_documents: int | None = None,
    exclude_paths: Sequence[str] | None = None,
    exclude_documents: Sequence[str] | None = None,
    crawl_app: CrawlConfig | None = None,
    node_id: str | None = None,
    document_level: DocumentLevel | None = None,
    pdf_pipeline: PdfPipeline | None = None,
    document_timeout_s: float | None = None,
    host_concurrency: int | None = None,
    extract_backlog_mb: int | None = None,
    state: Any = None,
    product_modes: Mapping[str, str] | None = None,
    refresh_policy: str = "auto",
    frozen_source: str | None = None,
    crawl_policy: Any = None,
    only_failed_repairs: Mapping[str, list[str]] | None = None,
    execution_contract_digest: str | None = None,
    axes: Any = None,
    declared_status_dict_version: str | None = None,
    mem_budget_mb: int | None = None,
    derive_extract_workers: bool = False,
    previous_run_id: str | None = None,
    previous_store: Any = None,
    change_class: str | None = None,
    cycle_id: str | None = None,
    run_scope: str | None = None,
    product_plan: Mapping[str, Any] | None = None,
    crawl_only: bool = False,
    consume_only: bool = False,
) -> dict[str, PhaseFn]:
    """Возвращает реестр ``{phase_name: PhaseFn}`` для не-гейтовых фаз конвейера.

    Композиция: ``Runner(store, ports, clock, phases=build_phases(...))``. Гейты
    ``g1``–``g4`` не входят в словарь — их (де)фолтные замыкания остаются в ``Runner``,
    поэтому прогон по-прежнему останавливается на человеке (S9).

    :param inputs: нормализованные входы (``RunInputs``): ``run``/``package``/``baselines``.
    :param plan: детерминированный план прогона (``RunPlan``): ``run_id``/``params``/веса.
    :param ports: внедрённые порты (оценщик, индексы, краулер/экстрактор/чанкер, реестр).
    :param package_dir: каталог пакета категории — источник per-product подписей
        ``scope_attestation.yaml:attestations`` (I2b/I2c). ``None`` (модульный вызов) ⇒
        подписей нет: канал остаётся недостижимым ровно как до этого параметра.
    :param urls: ``product_id → [url]`` для Ш5 (план URL не несёт — тонкий вход).
    :param passports: ``product_id → Passport`` для Ш4 (наследование).
    :param out_dir: базовый каталог отчётов (Ш9); composition root передаёт resolved
        ``paths.runs`` (``03`` §A.1). Дефолт ``"runs"`` совпадает с :class:`PathsConfig`.
    :param raw_root: корень retention сырых байтов (Ш5); composition root передаёт resolved
        ``paths.corpus_raw``. Дефолт ``"corpus_raw"`` совпадает с :class:`PathsConfig`.
    :param index_root: корень ``index_store/`` для переиспользования партиций и записи
        манифеста композиции (09 §8.1–§8.5). ``None`` — легаси-путь без чтения/записи
        индекса (композиция не пишется, переиспользования нет).
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
    :param llm_rnd_endpoint: объявленное исключение стенда RnD (``deploy.llm.rnd_endpoint``,
        §10): ``None`` — не объявлено, ``True`` — endpoint вне корпоративного периметра.
        Протягивается в профиль публикуемости (Ш8) и оттуда в манифест/артефакты. На
        ``run_id`` не влияет: это декларация о месте исполнения, а не условие измерения.
    :param max_attempts: Phase 4 — потолок попыток по ``(product_id, url)`` в Ш5
        (``--max-attempts``); исчерпавшие URL → терминальный ``FAILED`` ``attempts_exhausted``.
        ``None`` → без потолка.
    :param progress: Phase 4 — колбэк снимка прогресса сеанса краула (``--follow``); ошибки
        изолируются в :func:`core.pipeline.corpus.build_corpus`. ``None`` → без прогресса.
    :param on_product: Phase 4 — колбэк границ продукта ``(phase, product_id)``
        (``--follow``): ``start`` перед первым дренажом, ``end`` после тела продукта
        (включая нулевой выход). Ошибки изолируются в
        :func:`core.pipeline.corpus.build_corpus`. ``None`` → без вывода.
    :param extraction_pool: P4 — исполнитель параллельной экстракции (:class:`ExtractionPool`,
        фабрика пула + воркер от composition root), протягивается в Ш5. Отдельная ось
        ``crawl.extract_workers``; ``None`` ⇒ экстракция inline (поведение байт-в-байт).
    :param extract_cache: Ф1.2 — межпрогонный кэш экстракции (:class:`ExtractionCache`),
        протягивается в Ш5. ``None`` ⇒ кэш выключен (поведение байт-в-байт). Ключ —
        хеш ретенированных байтов (I19), поэтому хит не подменяет контент.
    :param embed_cache: межпрогонный кэш **векторов** эмбеддинга (:class:`EmbedCache`),
        протягивается в Ш5. ``None`` ⇒ кэш выключен (поведение байт-в-байт). Ключ —
        ``(content_hash текста чанка, embed_model_id)``: вектор переиспользуется там, где
        текст совпал, независимо от экстрактора/чанкера, поэтому смена механизма извлечения
        пересчитывает только изменившийся текст. ``pb``/манифест партиции кэш не меняет.
    :param eval_cache: Ф3 — межпрогонный кэш вердиктов оценщика (:class:`EvalVerdictCache`),
        протягивается в Ш6. ``None`` ⇒ кэш выключен (поведение байт-в-байт). Кэшируется
        сырой ``LLMCellOutput``; валидация/Ф0/прецедент исполняются на каждом прогоне.
    :param state: durable per-product состояние (:class:`~core.ports.state.ProductStatePort`,
        09 §4.1) — протягивается в Ш5 (``build_corpus``). ``None`` ⇒ инкрементальный
        субстрат выключен, поведение корпуса байт-в-байт прежнее.
    :param product_modes: ``product_id → mode`` (``new``|``full``|``incremental``) из
        onboarding-плана; ``None`` ⇒ все продукты трактуются как ``new``.
    :param refresh_policy: политика наблюдения прогона (``auto``|``revalidate``|
        ``refresh``|``offline``) — влияет на условный GET и переиспользование в Ш5.
    :param crawl_policy: разрешённая политика краула (:class:`CrawlPolicyResolution`,
        план 19 §P2) — **та же**, что дала ``crawl_policy_digest`` для ``run_id``. Ш5
        применяет её **до** первого сетевого вызова; ``None`` — поведение прежнее
        (политики нет: ``fetch``+``network`` у каждого корня).
    :param only_failed_repairs: ``product_id → [url, ...]`` — work-set ремонта
        ``--only-failed`` (резолвится composition root'ом, ``core.cli.context``), идёт в
        Ш5 отдельным входом: ремонт — известные depth-0 URL, а не обход авторских корней,
        и не зависит от режима продукта. ``None`` ⇒ прежнее поведение байт-в-байт.
    :param execution_contract_digest: готовый отпечаток условий измерения (09 §12.5),
        вычисленный composition root для ``run_id``/lineage. Протягивается в манифест Ш9
        без пересчёта; ``None`` — неизвестен (legacy/вызов без идентичности), тогда
        ``hound diff`` применяет прежнее сравнение ``EXECUTION_FIELDS``.
    :param axes: декларированные оси исполнения
        (:class:`core.cli.execution_axes.DeclaredExecutionAxes`) — один резолвер с
        контрактом; из них манифест Ш9 берёт deploy-профиль/версию seam-схемы/чанкер/
        оси краула/реестры/``llm_scenarios``. ``None`` — legacy/модульный вызов (прежние
        цепочки ``_first_present``). Типизируется структурно (``Any``): ``pipeline`` не
        импортирует ``cli``.
    :param declared_status_dict_version: версия словаря статусов (декларированная ось);
        используется манифестом, только если ``run``/``inputs`` её не несут.
    :param mem_budget_mb: F0 — бюджет памяти прогона, MB, разрешённый composition root
        (:func:`core.pipeline.resources.resolve_mem_budget_mb`). Протягивается в Ш5 явным
        параметром: фаза окружение не читает, иначе расписание/тесты стали бы функцией
        памяти хоста. ``None`` → бюджета нет (окно экстракции из него не выводится).
    :param derive_extract_workers: F0 — выводить окно экстракции из бюджета, когда ось
        ``crawl.extract_workers`` не объявлена. ``True`` только для локального экстрактора;
        под ``split`` — ``False`` (воркеры живут на edge со своим бюджетом). Решает
        composition root, фаза лишь передаёт флаг в :func:`build_corpus`.
    :param previous_run_id: прогон, против которого считается этот (09 §12.3), — результат
        :func:`core.pipeline.run_identity.previous_run_of`. Резолвит composition root: он
        один знает идентичность пакета, под которой пишется ``run_lineage``
        (``Path(--package).name``), и разрешает правило **один** раз. Фаза оценки называет
        его в ``log_entries`` (``previous=<id>`` / ``previous=—``), манифест Ш9 — полем
        ``previous_run_id``. ``None`` — предшественника нет/resume текущего прогона; в
        материал ``run_id`` он не входит (иначе идентичность зависела бы от истории).
    :param previous_store: ``StorePort`` БД прогона-предшественника, открытый **только на
        чтение**. Источник ячейки и её evidence для переноса (09 §10.2). Открывает и
        закрывает его composition root: путь ``runs/{run_id}/cohound.db`` — свойство
        раскладки, а ``pipeline`` раскладку не читает. ``None`` ⇒ перенос выключен; фаза
        называет это строкой ``carry=—`` в логе, а не выдаёт за «ключи не совпали».
    :param change_class: класс изменения относительно предшественника (§13) из той же
        ``RunIdentity`` — для манифеста Ш9. Не пересчитывается: расчёт живёт в
        ``build_run_identity``, а его запись — в ``run_lineage``.
    :param cycle_id: эффективный цикл прогона (09 §2.4/§6.2) — тот же материал, что вошёл
        в ``run_id``, поэтому манифест обязан его называть: без поля два прогона разных
        циклов различались только ``run_id``. Источник — строка lineage (единственный
        писатель), см. :func:`_lineage_provenance`.
    :param product_plan: сериализованный ``ProductPlanSet``
        (``onboarding.plan_set.model_dump(mode="json")``) — провенанс среза прогона
        (16 §3.1): без него полное измерение неотличимо от ``--only-product``, не
        заглядывая в ``state.db``. Явный аргумент приоритетен; ``None`` — значение берётся
        из снимка входов плана (``run_inputs.product_plan``), который composition root
        пишет тем же значением до исполнения фаз (:func:`_product_plan_provenance`).
        Пересчёт из ``product_modes`` запрещён: там только режим, без ``scope``/
        ``reasons``/``blocked`` — это была бы догадка (A3).
    :param crawl_only: F2 (17 §4) — «краул без конверсии»: Ш5 исполняет frontier/фетч/
        retention/``FETCHED`` и публикует ``extract_queue``, но не конвертирует. Вход
        приходит только от ``hound crawl run|resume|restart --crawl-only``; полный ``run``
        его не объявляет (иначе прогон печатал бы корпус без документов). ``False`` —
        прежнее поведение байт-в-байт.

    Каждое замыкание детерминировано и не импортирует адаптеры; единственный
    межфазовый канал — замыкание ``holder`` (см. docstring модуля).
    """
    holder: dict[str, Any] = {}

    # I2b/I2c: подписи аттестации охвата **per product**. Отсутствие файла → {} (механизм
    # опционален); невалидный присутствующий файл — ``ValueError`` (не глушим: подпись
    # авторизует публикацию ❌, молчаливая подмена сделала бы её недоказуемой).
    scope_attestations = (
        load_scope_attestations(package_dir) if package_dir else {}
    )

    # -- Ш1–Ш2: предполёт уже выполнен composition root (CLI) ---------------- #
    def normalize_phase(ctx: RunContext) -> PhaseResult:
        """Ш1: ``ok``-no-op — нормализацию входов выполнил composition root (CLI).

        Не ``skipped``: работа не пропущена, а сделана предполётом (``normalize.load_inputs``);
        заметка в ``log_entries`` фиксирует это для читателя.
        """
        return _preflight_noop("normalize", "входы нормализованы (normalize.load_inputs)")

    def lint_phase(ctx: RunContext) -> PhaseResult:
        """Ш2: ``ok``-no-op — блокирующий линт выполнил composition root (CLI) предполётом."""
        return _preflight_noop("lint", "линт выполнен (lint.lint_run_inputs)")

    def plan_phase(ctx: RunContext) -> PhaseResult:
        """Ш2: ``ok``-no-op — план построил composition root (CLI); повторный ``plan()`` запрещён."""
        return _preflight_noop("plan", "план построен (plan.plan); повторный plan() запрещён")

    # -- Ш2: разведка (кандидаты, не evidence, I19) ------------------------- #
    discovery_queries = _discovery_queries(inputs)

    def discovery_phase(ctx: RunContext) -> PhaseResult:
        """Ш2: разведка кандидатов; без адаптера/запросов — ``skipped`` (I19: не evidence).

        Отсутствие адаптера/запросов — легитимная предпосылка, а не сбой и не ``ok``:
        статус ``skipped`` с машинной причиной, прогон продолжается.
        """
        adapter = getattr(ctx.ports, "search", None)
        if adapter is None:
            return PhaseResult(
                status="skipped",
                phase="discovery",
                skip_reason=SkipReason.NO_SEARCH_ADAPTER,
            )
        if not discovery_queries:
            return PhaseResult(
                status="skipped",
                phase="discovery",
                skip_reason=SkipReason.NO_QUERIES,
            )
        report = discover(
            queries=list(discovery_queries),
            adapter=adapter,
            phase="discovery",
        )
        return PhaseResult(status="ok", phase="discovery", log_entries=[report])

    # -- Ш3: реестровая волна (без LLM) ------------------------------------- #
    def registry_phase(ctx: RunContext) -> PhaseResult:
        """Ш3: реестровая волна. ``Ports.registry_fetchers`` — mapping ``registry_id → fetcher``.

        Mapping собирает composition root (E2) из package-scoped спецификаций; фаза только
        передаёт его в :func:`registry_wave` (контракт волны не меняется). Пусто/``None`` →
        волна пуста (реестры не сконфигурированы) — не молчаливая подмена одним портом.
        """
        fetchers: dict[str, Any] = dict(
            getattr(ctx.ports, "registry_fetchers", None) or {}
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
        # D1/D2: CLI-флаги (--max-depth/--max-documents/--exclude-*) — операторское уточнение
        # **run-слоя** (поверх run.yaml): цепочка остаётся трёхуровневой app < run < site.
        run_crawl: CrawlRunConfig | None = getattr(
            getattr(inputs, "run", None), "crawl", None
        )
        # Секция ``serp`` run.yaml: бюджет объявлен **только** там (решение: один бюджет,
        # и он определяет число ушедших запросов, то есть состав корпуса).
        run_serp = getattr(getattr(inputs, "run", None), "serp", None)
        override: dict[str, Any] = {}
        if max_depth is not None:
            override["max_depth"] = max_depth
        if max_documents is not None:
            override["max_documents"] = max_documents
        if exclude_paths:
            override["exclude_paths"] = list(exclude_paths)
        if exclude_documents:
            override["exclude_documents"] = list(exclude_documents)
        if document_level is not None:
            override["document_level"] = document_level
        if pdf_pipeline is not None:
            override["pdf_pipeline"] = pdf_pipeline
        if document_timeout_s is not None:
            override["document_timeout_s"] = document_timeout_s
        # F1/F6 (17 §4, §4.3): оси параллельности и бэклога — такие же оси краула, как
        # ``max_depth``. Без этой проводки они попадали в ``crawl_config_digest`` (то есть
        # в ``run_id``) и не доезжали до Ш5: прогон объявлял 11 полос, а исполнял одну —
        # ложь о условиях измерения того же класса, что env-модель оценщика (§7.4).
        if host_concurrency is not None:
            override["host_concurrency"] = host_concurrency
        if extract_backlog_mb is not None:
            override["extract_backlog_mb"] = extract_backlog_mb
        if override:
            base = run_crawl if run_crawl is not None else CrawlRunConfig()
            run_crawl = base.model_copy(update=override)
        # План 19 §P3: запись снятия называет объявленный seed-набор, из которого
        # собрано снятие. Считается только для ``frozen`` — та же функция и те же
        # входы, что у манифеста (второй формулы нет).
        capture_seeds_digest: str | None = None
        if refresh_policy == RefreshPolicy.FROZEN.value:
            _, capture_seeds_digest = declared_identity_digests(
                inputs,
                package_dir=package_dir,
                urls=urls,
                domain_profiles=domain_profiles,
            )
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
            # 13-manual-artifacts §4: отчёт о недоступных артефактах пишется на границе
            # фазы в ``{out_dir}/{run_id}/outputs/`` — оператор, остановившийся на
            # ``--until corpus``, видит пробел до дорогой Ш6. Тот же resolved
            # ``paths.runs``, что у Ш9: один генератор, два вызова.
            out_dir=out_dir,
            index_root=index_root,
            package_id=_package_id(inputs, plan),
            domain_profiles=domain_profiles or {},
            # 13-manual-artifacts §3: объявленные файлы оператора — **вход фазы**
            # (``sources/manual_artifacts.yaml`` пакета). Ось задана файлом, а не
            # флагом: флаг теряется в следующем вызове, а ``run_id`` перестал бы
            # описывать корпус. Загрузка здесь, а не в ``execution_axes``: там она
            # уже была (для контракта) и там же отказывает ``EXIT_USAGE``, здесь —
            # прямое исполнение (resume/прямой вызов модуля).
            manual_artifacts=load_manual_artifacts(package_dir),
            explicit_fetch=explicit_fetch,
            # Lane A/L3a: слои источников краула и карта per-engine краулеров; пейсинг —
            # единый владелец (RateLimiter), протягивается composition root'ом.
            crawl_sources=crawl_sources,
            crawlers=getattr(ctx.ports, "crawlers", None),
            # Phase 5.2: карта ``kind → PollPort`` для источников-каталогов (feed/channel).
            pollers=getattr(ctx.ports, "pollers", None),
            rate_limiter=rate_limiter,
            # P4: исполнитель экстракции (process-local / thread-split) — аддитивная ось
            # ``crawl.extract_workers``; ``None`` ⇒ inline (поведение байт-в-байт).
            extraction_pool=extraction_pool,
            # P8: потолок ожидания **одного** документа. Пул экстракции один на прогон,
            # поэтому per-site слой здесь не применяется (он резолвится там, где адрес
            # известен — краул/фетч); app < run — ровно те слои, которые объявляют
            # стоимость извлечения. ``None`` ⇒ прежнее зашитое значение, поведение
            # байт-в-байт (I19/§13: ось не меняет прогон, пока её не объявили).
            extract_task_timeout_s=resolve_crawl_limits(
                "",
                app=crawl_app if crawl_app is not None else CrawlConfig(),
                run=run_crawl,
            ).document_timeout_s,
            # P5: фабрика per-level экстрактора для inline-пути (переданная ссылка из
            # composition root; ``None`` ⇒ уровень на inline-пути не действует, как прежде).
            extractor_factory=extractor_factory,
            # Ф1.2: межпрогонный кэш экстракции (content-addressed по ретенированным байтам).
            # ``None`` ⇒ кэш выключен, поведение байт-в-байт прежнее.
            extract_cache=extract_cache,
            # Кэш **векторов** эмбеддинга (почанково, ключ ``(content_hash текста,
            # embed_model_id, embed_device)`` — 16 §5.3: CPU- и CUDA-векторы численно
            # разные). ``None`` ⇒ кэш выключен, поведение байт-в-байт прежнее.
            embed_cache=embed_cache,
            # Phase 3.3: исполнитель краула (``split``/edge-планировщик). ``None`` → локальный
            # per-URL путь (``crawler``/``crawlers``); при наличии — краул через шов.
            executor=getattr(ctx.ports, "executor", None),
            # D4 (P2-3): секция ``crawl`` run.yaml → дефолты оси стратегии/лимита для ``FetchPlan``.
            # ``RunConfig`` доступен как ``inputs.run`` (RunInputs.run), поле ``crawl`` опционально.
            crawl_defaults=run_crawl,
            # 16 §4.1: устройство эмбеддера — **объявленная ось** прогона, уже резолвленная
            # composition root'ом в ``axes`` (:func:`core.pipeline.crawl_frontier.resolve_embed_device`,
            # app < run, per-site слоя нет). Фаза не читает env и не спрашивает адаптер:
            # ``EmbedderPort`` об устройстве не знает, а «спросить» его — ровно тот дефект,
            # ради которого ось вводится. Дефолт — объявленный дефолт оси (``cpu``).
            embed_device=str(
                getattr(axes, "embed_device", None) or EmbedDevice.CPU.value
            ),
            # D1/D2: application-слой осей краула (deploy.yaml:crawl) — низший приоритет.
            crawl_app=crawl_app,
            # F7 (17 §2): узел шарда. Резолвится composition root'ом (``--node``/
            # ``COHOUND_NODE``): фаза окружение не читает, а узел — операционный выбор,
            # а не измерение (в ``run_id`` не входит).
            node_id=node_id,
            # D3: durable-краул (аудит/очередь/resume) привязан к прогону; store уже есть.
            run_id=getattr(plan, "run_id", None),
            # Phase 4: потолок попыток (--max-attempts) и колбэк прогресса (--follow).
            max_attempts=max_attempts,
            progress=progress,
            # Phase 4: живая граница продукта (--follow) — продукт-скоуп к агрегату сеанса.
            on_product=on_product,
            # 09 §6.3 (MVP-3b): durable-субстрат, режимы продуктов и политика наблюдения.
            # ``state is None`` ⇒ прежнее поведение корпуса байт-в-байт.
            state=state,
            product_modes=product_modes,
            refresh_policy=refresh_policy,
            # План 19 §P3: источник корпуса при ``frozen`` (``bytes`` → пере-извлечение
            # сохранённых байтов, ``chunks`` → поведение ``offline``).
            frozen_source=frozen_source,
            # План 19 §P3: объявленный seed-набор — в запись снятия (``capture_digest``).
            seeds_digest=capture_seeds_digest,
            # План 19 §P2: гейт сети — вход Ш5, а не решение CLI: резолв уже вошёл в
            # ``run_id``, и фаза обязана исполнить **тот же** объект.
            crawl_policy=crawl_policy,
            # ``--only-failed`` (межпрогонный ремонт): разрешённое множество провалов по
            # продукту — depth-0 известные URL, авторские корни не обходятся. ``None`` ⇒
            # прежнее поведение байт-в-байт.
            only_failed_repairs=only_failed_repairs,
            # B3: спуск в контейнеры — читатель инъектируется composition root'ом
            # (``container_any.container_for``); ``None`` → спуск не исполняется, но видимо.
            container_for=getattr(ctx.ports, "container", None),
            # F0: бюджет памяти — **явный** вход composition root (фаза окружение не
            # читает); ``derive_extract_workers`` решён там же по профилю зон.
            mem_budget_mb=mem_budget_mb,
            derive_extract_workers=derive_extract_workers,
            # I3/§5: SERP-контур — внутри этой же фазы (не отдельная фаза ``PHASES``).
            # Адаптер поиска приходит портом (``split`` — шов ``/v1/search``; ``local`` —
            # ``None``, тогда объявленные раунды дают видимый отказ в ``errors``).
            search=getattr(ctx.ports, "search", None),
            serp_rounds=_serp_rounds(axes),
            serp_budget=getattr(run_serp, "budget", None),
            serp_source=getattr(axes, "serp_source", None),
            # Режим поиска — объявленная ось (``run.yaml: serp.mode``); пишется в
            # провенанс контура парой «объявлено/исполнено», чтобы ``serp_mode`` в
            # контракте был восстановим из манифеста (иначе расхождение прогонов по
            # этой оси объявлялось бы невосстановимым).
            serp_mode=getattr(axes, "serp_mode", None),
            serp_langs=getattr(inputs.run, "languages", None),
            # Запросы контура = каталог + **имя продукта** (замер: 45/63 против 27/63):
            # имя берётся из паспорта (канонический), а не из product_id.
            product_names={
                str(pid): str(getattr(passport, "canonical_name", "") or "")
                for pid, passport in (passports or {}).items()
                if getattr(passport, "canonical_name", None)
            },
            # 17 §4 (F2): «краул без конверсии» — публикация durable-очереди
            # ``extract_queue`` вместо сборки корпуса. ``False`` ⇒ байт-в-байт прежнее.
            crawl_only=crawl_only,
            # F6 (17 §6): «только конверсия» — та же фаза без дренажа корней: вход
            # ``hound extract consume``. Логика та же (``extract_consume.consume``),
            # отличается только отсутствием сетевых шагов.
            consume_only=consume_only,
        )
        _capture_index_build(holder, result)
        # 09 §8.5: переиспользование партиций — видимая оператору строка лога (не ошибка).
        reuse_note = _partition_reuse_note(result)
        if reuse_note is not None:
            result.log_entries.append(reuse_note)
        # Наблюдаемость дедупа: строка на продукт (только при ненулевом схлопывании).
        result.log_entries.extend(_duplicate_notes(result))
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
            # I2b/I2c: подпись per product (покрытие per product). Продукт без подписи
            # остаётся недостижимым для ❌ — канал не «упрощает» отрицание.
            scope_acceptance={
                product_id: attestation.attested_by
                for product_id, attestation in scope_attestations.items()
            },
            eval_cache=eval_cache,
            # 09 §10.5: условия, по которым посчитан вердикт, — часть ключа кэша. Строятся
            # из **объявленных** входов прогона (run.yaml:methodology_version, версия
            # словаря статусов — та же, что уходит в контракт/манифест, политика §6).
            # ``eval_cache is None`` ⇒ условия не нужны, но и не вредны: они только
            # описывают прогон.
            eval_conditions=eval_conditions_from_run(
                inputs.run, status_dict_version=declared_status_dict_version
            ),
            # V7: аудит исполненных промптов — `runs/{run_id}/prompts/{sha256(text)}.json`
            # (03 §A.2, 04 §B.3, 09 §14.4). Путь — из resolved-раскладки прогона
            # (`out_dir`), а не выдуман: фаза не читает окружение.
            prompt_dir=Path(out_dir) / plan.run_id / "prompts",
            # Журнал вызовов оценщика: строка на попытку — метка остановки, расход,
            # латентность и полный текст ответа. Без него путь отказа неразбираем:
            # ``eval_cache`` пишет только успех, ``prompts/`` — только вход. Файл —
            # провенанс исполнения, в ``run_id``/DDL не входит.
            llm_journal=Path(out_dir) / plan.run_id / "logs" / "llm_calls.jsonl",
            # I3/§5: факты других фаз — только наблюдённые (см. helper).
            exhaustion_facts=_observed_exhaustion_facts(
                ctx, plan, inputs, passports
            ),
            # 09 §12.3: база сравнения — в ``log_entries`` фазы (``previous=<id>`` /
            # ``previous=—``), чтобы она читалась в отчёте прогона, а не только в
            # ``state.db``. Резолв — в composition root (:func:`previous_run_of`).
            previous_run_id=previous_run_id,
            # 09 §10.2: индекс переноса (``state.db.cell_measurement``) и БД
            # предшественника — оба нужны, чтобы ячейка не мерилась повторно. Любой
            # ``None`` выключает механизм, и это видно строкой ``carry=—`` в логе фазы.
            state=state,
            previous_store=previous_store,
        )

    # -- Ш7: ревью (advisory-очередь, ничего не решает за человека) ---------- #
    def review_phase(ctx: RunContext) -> PhaseResult:
        """Ш7: прикрепляет очередь ревью как **advisory** ``pending``; ничего не решает.

        Фаза читает :func:`~core.pipeline.review.review_queue` (preliminary-контур I13 +
        ``❓`` ``evaluated``-кандидаты I3) и возвращает ``ok`` с ограниченной сводкой в
        :attr:`PhaseResult.pending` — сигналом «что осталось человеку». **Ничего не
        разрешает**: не промоутит, не пишет ``N/A``, не трогает статусы ячеек, не
        превращает ``pending`` в статус (I6/I7: N/A и преодоление ``❓`` — только человеком).
        Фаза ничего не решает и не останавливает прогон: G3 — **точка интеграции**, его
        решение принимает внешняя система по экспортированной очереди (см.
        :mod:`core.pipeline.gate_subject`). ``pending`` не влияет ни на ``status``, ни на
        resume, ни на гейты.

        Пустая очередь → ``pending == []`` (честное «нечего решать»). Сбой чтения очереди
        изолируется (ср. изоляцию ``run_isolated``/информационный провенанс Ш9): фаза
        возвращает ``ok`` с самодокументирующей заметкой в ``log_entries``, а не роняет
        прогон ``failed`` — очередь ревью вторична к прогону, а остановка на решении и без
        того обеспечивается гейтами — очередь лишь экспорт.
        """
        try:
            items = review_queue(ctx.store, plan.run_id)
        except Exception as exc:  # noqa: BLE001 — очередь ревью вторична к прогону
            return PhaseResult(
                status="ok",
                phase="review",
                log_entries=[
                    f"review: очередь недоступна: {type(exc).__name__}: {exc}"
                ],
            )
        pending = [_review_pending_summary(items)] if items else []
        return PhaseResult(status="ok", phase="review", pending=pending)

    # -- Ш8: расчёт (бандл → holder для report) ----------------------------- #
    def compute_phase(ctx: RunContext) -> PhaseResult:
        """Ш8: считает ``ScoreBundle`` и кладёт его в ``holder`` для фазы отчёта.

        Профиль публикуемости несёт объявленное исключение стенда (§10) и хосты
        endpoint'ов из ``llm_scenarios``; хосты читаются из store (durable), декларация
        приходит из composition root. Записывается она **сразу** (``save_publishability``):
        ``persist_manifest`` пишет блок только в Ш9, а предмет G4 собирается между Ш8 и
        Ш9 — без ранней записи пост-хок ``hound report`` пересобрал бы профиль без
        декларации и перезаписал опубликованную карту как производственную.
        """
        bundle = bundle_for_run(
            ctx.store,
            run_id=plan.run_id,
            plan=plan,
            inputs=inputs,
            rnd_endpoint=llm_rnd_endpoint,
        )
        holder["bundle"] = bundle
        profile = bundle.publishability
        writer = getattr(ctx.store, "save_publishability", None)
        if profile is not None and callable(writer):
            try:
                writer(plan.run_id, profile.to_dict())
            except Exception as exc:  # noqa: BLE001 — провенанс не роняет расчёт
                return PhaseResult(
                    status="ok",
                    phase="compute",
                    log_entries=[
                        f"compute: профиль публикуемости не записан в runs: "
                        f"{type(exc).__name__}: {exc}"
                    ],
                )
        return PhaseResult(status="ok", phase="compute")


    # -- Ш9: отчёты (читает holder) ----------------------------------------- #
    def report_phase(ctx: RunContext) -> PhaseResult:
        """Ш9: собирает манифест и артефакты; без бандла — ``failed`` (compute не выполнен)."""
        bundle = holder.get("bundle")
        if bundle is None and _phase_done(ctx, plan.run_id, "compute"):
            # Resume: ``compute`` уже отработал в прошлом процессе (durable ``ok``) и
            # короткозамкнут, поэтому ``holder`` пуст. Расчёт детерминирован по ячейкам —
            # пересчитываем, иначе прогон, дошедший до G4 в прошлом процессе, не смог бы
            # завершиться (``report`` падал бы «бандла нет» на каждом resume).
            bundle = bundle_for_run(
                ctx.store, run_id=plan.run_id, plan=plan, inputs=inputs
            )
            holder["bundle"] = bundle
        if bundle is None:
            return PhaseResult(
                status="failed",
                phase="report",
                errors=["compute-бандл отсутствует: фаза compute не выполнена"],
            )
        # Resume: фаза корпуса короткозамкнута персистентным ``ok``, поэтому ``holder``
        # пуст. Берём composition id из ``runs`` (его пишет пост-корпусная перепривязка
        # composition root сразу после Ш5): иначе ``persist_manifest`` обнулил бы провенанс.
        index_build_id = holder.get("index_build_id")
        if index_build_id is None:
            try:
                ref = ctx.store.read_run_index_ref(plan.run_id)
            except Exception:  # noqa: BLE001 — провенанс не роняет отчёт
                ref = None
            index_build_id = getattr(ref, "index_build_id", None)
        # R7/§13: профиль ретривала — тоже durable-провенанс, и на resume ``holder`` пуст
        # ровно как у сборки. Ячейки его несут (``_provenance`` Ш6), поэтому читаем оттуда,
        # а не оставляем ``null``: манифест обязан называть прибор, которым получены
        # вердикты, иначе ``retrieval_profile`` (поле контракта) в прогоне не объявлен.
        observed_profile, observed_cutoff, observed_params_json = _observed_retrieval(
            holder, ctx.store, plan.run_id
        )
        # Изолированные отказы провенанса краула собираются здесь и уходят в ``pending``:
        # строка видима человеку, но фазу отчёта не роняет.
        provenance_notes: list[str] = []
        # 09 §12.3/§13: предшественник и класс изменения — из строки lineage прогона
        # (единственный носитель ``change_class``: он считается при её записи), а пока
        # строки нет — из идентичности, переданной composition root (та же величина).
        lineage_cycle, lineage_previous, lineage_change = _lineage_provenance(
            state,
            plan.run_id,
            declared_cycle=cycle_id,
            declared_previous=previous_run_id,
            declared_change=change_class,
            notes=provenance_notes,
        )
        # 09 §12.5: дайджесты когорты и seed-набора — слагаемые ``run_id`` и поля
        # ``execution_contract_digest``. Прежде их не было ни на манифесте, ни в ``runs``,
        # и ``hound diff`` называл ось невосстановимой вместо причины. Считаются теми же
        # функциями и на тех же объявленных входах, что у composition root (второй
        # формулы нет), и уходят на манифест готовыми.
        identity_cohort, identity_seeds = declared_identity_digests(
            inputs,
            package_dir=package_dir,
            urls=urls,
            domain_profiles=domain_profiles,
        )
        # План 19 §P4: провенанс исполнения политики краула — из durable-источников
        # (сеанс/покрытие/снятие), а не из состояния памяти: resume обязан опубликовать
        # тот же факт. Идентичность (digest/refresh/frozen) считается ниже тем же объектом,
        # что дал ``run_id``.
        policy_facts = _crawl_policy_provenance_facts(
            ctx.store,
            state,
            run_id=plan.run_id,
            products=plan.products,
            refresh_policy=refresh_policy,
        )
        # 16 §3.1: план продуктов — провенанс среза. Значение приходит от composition
        # root, а если его нет — читается из снимка входов плана, куда тот же
        # composition root положил ``onboarding.plan_set.model_dump(mode="json")``
        # (одно значение, а не второй расчёт; см. :func:`_product_plan_provenance`).
        # Приоритет — **снимок**, а не аргумент: снимок входов иммутабелен (O11) и
        # фиксирует план, объявленный в начале прогона. На resume preflight считается
        # заново по сдвинувшемуся состоянию (``last_corpus_run_id`` уже стоит), и его план
        # описывает **продолжение**, а не исполненное измерение: отдать его на манифест
        # значило бы записать ``mode=full`` прогону, который шёл как ``new``. Аргумент —
        # резерв ровно на один случай: первый проход остановился до онбординга
        # (``--until plan``), снимок записан без плана и перезаписаться не может; тогда
        # плана нет ни в одном носителе, а измерительных фаз до этого не исполнялось —
        # то есть план возобновлённого прохода и есть план исполненного прогона.
        # План 20 §F1: снимок корпуса — durable-провенанс фазы ``corpus`` (снятия и даты
        # артефактов). На resume ``holder`` пуст, а снимок обязан пережить его: читаем из
        # ``runs.corpus_snapshot_json`` и передаём в манифест готовым — второго расчёта
        # содержания нет. Отсутствие читателя (лёгкий тестовый стор) — не ошибка фазы:
        # снимок вторичен к корпусу.
        corpus_snapshot: dict[str, Any] | None = None
        snapshot_reader = getattr(ctx.store, "read_corpus_snapshot", None)
        if callable(snapshot_reader):
            try:
                corpus_snapshot = snapshot_reader(plan.run_id)
            except Exception as exc:  # noqa: BLE001 — провенанс не роняет отчёт
                provenance_notes.append(
                    f"corpus_snapshot: {type(exc).__name__}: {exc}"
                )
        product_plan_provenance = (
            _product_plan_provenance(ctx.store, plan.run_id, provenance_notes)
            or (dict(product_plan) if product_plan is not None else None)
        )
        manifest = build_manifest(
            inputs=inputs,
            plan=plan,
            bundle=bundle,
            index_build_id=index_build_id,
            # Наблюдаемые величины прибора берутся из durable-манифеста той сборки, на
            # которой считался прогон: ``holder`` пуст на resume (``corpus`` уже ``ok``),
            # а ``runs.embed_model_id`` заполняется только этим же манифестом — иначе
            # получался круг «null потому что null». Порты нового процесса для этого
            # непригодны: они описывают окружение повторного запуска, а не ту сборку,
            # на которой строились вердикты.
            observed_versions=_observed_index_versions(ctx.store, index_build_id),
            created_at=_created_at(ctx),
            deploy_profile=deploy_profile,
            edge_schema_version=edge_schema_version,
            edge_capabilities=edge_capabilities,
            retrieval_profile=observed_profile,
            retrieval_cutoff_K=observed_cutoff,
            retrieval_params_json=observed_params_json,
            crawl_provenance=_crawl_provenance(
                ctx.store, inputs, plan.run_id, provenance_notes
            ),
            # N6: разрешённая конфигурация контроля краула (app<run<site) — дайджест в
            # манифест, чтобы смена осей на app/per-site слое тоже давала execution_change.
            domain_profiles=domain_profiles,
            crawl_app=crawl_app,
            # P5: операторский ``--document-level`` — в провенанс манифеста, а не только в
            # дайджест: манифест обязан называть уровень, которым реально извлекали.
            operator_document_level=document_level,
            # E2/§13: дайджест загруженных package-scoped спецификаций реестров — провенанс
            # аудируемости (R7). Composition root кладёт его на ``Ports``; здесь лишь
            # прокидывается в манифест (``None`` — реестры не сконфигурированы).
            registry_spec_digest=getattr(ctx.ports, "registry_spec_digest", None),
            # 09 §12.5: готовый отпечаток условий измерения (не пересчитывается здесь) —
            # носитель границы ``hound diff``; ``None`` не подменяется пустой строкой.
            execution_contract_digest=execution_contract_digest,
            # 13-manual-artifacts §3.4: доля ручного — **числом**, не потолком.
            # Источник — персистентный аудит (``manual_artifacts``), поэтому resume
            # публикует тот же факт; нули по продуктам плана видны (симметрия).
            manual_artifacts=manual_artifacts_summary(
                ctx.store, plan.run_id, products=plan.products
            ),
            manual_artifacts_digest=getattr(axes, "manual_artifacts_digest", None),
            # I2/I2c: документы, прочитанные не целиком — в манифест **числом**.
            # Источник — персистентный корпус (как у ``manual_artifacts``), поэтому resume
            # публикует тот же факт; нули по продуктам плана видны (симметрия).
            partial_docs=partial_docs_summary(
                ctx.store, plan.run_id, products=plan.products
            ),
            # §13 (2.4.0): декларированные оси — тот же резолвер, что у контракта.
            axes=axes,
            declared_status_dict_version=declared_status_dict_version,
            # 09 §12.3/§13: база сравнения прогона и класс изменения (см. helper выше).
            previous_run_id=lineage_previous,
            change_class=lineage_change,
            # 09 §13.6/16 §3.1: **срез** прогона (``full`` | ``only_failed`` |
            # ``product:<id>``). Колонка ``runs.run_scope`` и поле манифеста были
            # объявлены, писались и читались, но производителя не было: значение ни разу
            # не передавалось сюда, и читатель артефакта не отличал полное измерение от
            # среза ``--only-product``. Берётся из той же идентичности, что вошла в
            # ``run_id`` (``resolve_run_scope``), — второй формулы нет.
            run_scope=run_scope,
            # 09 §2.4/§6.2: цикл прогона — тот же материал, что вошёл в ``run_id``.
            cycle_id=lineage_cycle,
            # 09 §12.5: дайджесты когорты и seed-набора — тот же материал ``run_id``
            # (проекция объявленного входа; см. helper выше).
            cohort_digest=identity_cohort,
            seeds_digest=identity_seeds,
            # План 19 §P4: эффективная политика краула — тот же объект, что дал
            # ``crawl_policy_digest`` для ``run_id``; провенанс исполнения — durable-факты.
            crawl_policy=crawl_policy,
            refresh_policy=refresh_policy,
            frozen_source=frozen_source,
            policy_provenance=policy_facts,
            # Вариант 2: принятия риска, которыми прогон достиг ❌ класса bounded
            # (product → actor). Только продукты плана: подпись продукта вне плана
            # прогоном не использовалась.
            scope_attestations={
                product_id: attestation.attested_by
                for product_id, attestation in scope_attestations.items()
                if product_id in set(plan.products)
            },
            # I23/§5: состояние SERP-контура (раунды, израсходованный и объявленный бюджет,
            # терминация) — в ``runs.search_effort_budget_json``. Колонка существовала и
            # читалась отчётом, но никем не писалась (всегда ``null``); источник — durable
            # провенанс сеанса, поэтому значение переживает resume и не выдумывается.
            search_effort=compact_search_effort(_serp_facts(ctx.store, plan.run_id)),
            # 16 §3.1: план продуктов — тот же JSON, что лёг в снимок входов (см. выше).
            product_plan=product_plan_provenance,
            # План 20 §F1: снимок корпуса — из durable-носителя фазы ``corpus`` (см. выше),
            # не пересчитывается здесь: одна формула на величину.
            corpus_snapshot=corpus_snapshot,
        )
        # §13/точки интеграции: политика гейтов и waiver-строки — в манифест, чтобы
        # делегирование гейта наружу было видно в артефакте, а не подразумевалось.
        manifest = manifest.model_copy(update=_gate_provenance(ctx, plan, inputs))
        # R7/§13: финальный манифест — самостоятельная запись воспроизведения,
        # поэтому персистится в операционную БД прогона (версии/индекс/провенанс).
        # E1/A3: персист **не-понижающий** — измеренная фиксация ретривала не затирается
        # дефолтом повторного прогона. Значение+провенанс берутся из уже персистентной
        # строки ``runs``; явный противоречащий ``--retrieval-profile`` отвергнут (A3:
        # изменить измеренную настройку можно только пере-измерением). Явный аргумент
        # сохраняет приоритет в ``build_manifest`` — тот код (report.py) не меняется.
        fixation = resolve_measured_retrieval(
            ctx.store, plan.run_id, retrieval_profile
        )
        if fixation is not None:
            provenance = dict(manifest.settings_provenance or {})
            provenance[MEASURED_RETRIEVAL_SETTING] = fixation["provenance"]
            update: dict[str, Any] = {
                "retrieval_profile": fixation["retrieval_profile"],
                "settings_provenance": provenance,
            }
            # K/params — НЕ измеренная настройка (input-setting): заполняем из фиксации
            # только если манифест их не несёт (Ш6 не исполнялся/resume). Явный
            # ``--cutoff-k`` не переопределяется молча — приоритет оператора сохраняется.
            if (
                fixation["retrieval_cutoff_K"] is not None
                and manifest.retrieval_cutoff_K is None
            ):
                update["retrieval_cutoff_K"] = fixation["retrieval_cutoff_K"]
            if (
                fixation["retrieval_params_json"] is not None
                and manifest.retrieval_params_json is None
            ):
                update["retrieval_params_json"] = fixation["retrieval_params_json"]
            manifest = manifest.model_copy(update=update)
        # §13/A3: провенанс **замеренной** ручки экстрактора (``queue_max_size``). Ручка не
        # входит ни в ``extractor_version``, ни в ``EXECUTION_CONTRACT_FIELDS`` (это не
        # граница контракта), но её значение обязано быть видно в манифесте — иначе замер
        # (−44% peak RSS при идентичном markdown) невоспроизводим. Запись аддитивна: один
        # ключ в существующем канале ``settings_provenance`` (схема манифеста не меняется).
        # Значение — карта по уровням извлечения, а не скаляр по «разрешённому» уровню: так
        # запись не зависит от того, заполнен ли ``crawl_document_level`` (legacy/частичный
        # манифест), и прямо документирует, что у ``text`` ручки нет (``None``).
        # Экстрактор без такой ручки (HTML-only) — записи нет (A3: значения не выдумываем).
        extract_options = getattr(
            getattr(ctx.ports, "extractor", None), "options_provenance", None
        )
        if isinstance(extract_options, Mapping) and extract_options:
            provenance = dict(manifest.settings_provenance or {})
            provenance["document_extract.queue_max_size"] = {
                "source": "measured",
                "by_level": dict(extract_options),
            }
            manifest = manifest.model_copy(update={"settings_provenance": provenance})
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
            store=ctx.store,
        )
        holder["artifacts"] = artifacts
        # §13/K2: устаревшие ячейки исключены из расчёта — «что требует пере-оценки» идёт
        # advisory-каналом (``pending``), а не тишиной: это работа человека, не дефект.
        # Исполняется она не новым прогоном, а выборочной пере-оценкой: ``hound revalidate``
        # сужает Ш6 до этих ячеек (селектор ``evaluate.select_stale_cells``).
        # отчёт: изолированные отказы провенанса краула — сначала, они объясняют пустоту.
        pending: list[str] = list(provenance_notes)
        stale_count = len(bundle.stale_cells)
        # I2/I2c: частичное извлечение — advisory-строкой, потому что оно меняет
        # достижимость ❌ на всём корпусе продукта, а не только отчётную таблицу.
        partial_line = partial_docs_human_line(artifacts.partial_docs_summary)
        if partial_line:
            pending.append(f"отчёт: {partial_line} (список: outputs/partial_docs.csv)")
        if artifacts.stale_cells is not None:
            pending.append(
                f"отчёт: {stale_count} ячеек исключены из расчёта как устаревшие "
                "(stale_reason) — требуют пере-оценки: hound revalidate "
                "(список: outputs/stale_cells.csv)"
            )
        return PhaseResult(
            status="ok",
            phase="report",
            log_entries=[
                artifacts,
                *(
                    [f"report: {stale_count} stale-ячеек исключены из расчёта"]
                    if artifacts.stale_cells is not None
                    else []
                ),
            ],
            pending=pending,
        )

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
