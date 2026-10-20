"""``run``: собрать прогон и провести ``Runner`` по реальным фазам Ш1–Ш9.

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from core.adapters.index_npy import numpy_available
from core.adapters.lexical_tantivy import tantivy_available
from core.cli.context import (
    _add_common,
    _add_phase4_flags,
    _concise,
    _ensure_run,
    _filter_corpus_urls,
    _follow_session,
    _load_corpus_urls,
    _load_render_modes,
    _open_state_store,
    _phases_until,
    _positive_int,
    _print_json,
    _print_run_dry,
    _print_run_report,
    _progress_callback,
    _resolve_phases,
    _resolve_plan,
    _resolve_ports,
    _run_report_payload,
    _session_snapshot,
    _snapshot_run_inputs,
)
from core.cli.errors import (
    EXIT_BLOCKED,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    CliError,
    LintFailure,
)
from core.cli.wiring import (
    SPLIT_PROFILE,
    WiringError,
    build_runner,
    build_store,
    build_vector_index,
    manifest_deploy_fields,
    run_db_path,
    seed_lexical_index,
)
from core.config.deploy import is_split, load_deploy_config
from core.domain import (
    CrawlItemStatus,
    CrawlSessionStatus,
    ProductScope,
    RefreshPolicy,
    RunScope,
)
from core.pipeline import GATE_PHASES, PHASES, Ports
from core.pipeline.crawl_session import (
    default_session_id,
    read_provenance,
    truncation_summary,
)
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.observability import instrument_phases, write_run_metrics
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.onboarding import effective_seeds, load_effective_seeds, preflight
from core.pipeline.phases import resolve_measured_retrieval, resolve_package_id
from core.pipeline.plan import derive_run_id
from core.pipeline.report import Artifacts
from core.pipeline.retrieve import RetrievalProfile
from core.pipeline.run_identity import (
    RunIdentity,
    build_execution_contract,
    build_run_identity,
    resolve_cycle_id,
    resolve_refresh_policy,
    resolve_run_scope,
)
from core.pipeline.runner import PhaseFn, PhaseResult, RunContext, _gate_phase
from core.pipeline.state_sync import record_run_outcome, sync_product_state

#: Phase 4: хук сна цикла ``--follow`` (тесты инжектят no-op через monkeypatch).
_SLEEP: Callable[[float], None] = time.sleep


def _utc_now() -> str:
    """Текущее время UTC в ISO-8601 (метки lineage/попытки в durable state)."""
    return _dt.datetime.now(_dt.UTC).isoformat()


def _onboarding_in_scope(until: str | None) -> bool:
    """Попадает ли фаза ``onboarding`` (между ``plan`` и ``g1``) в запрошенный диапазон.

    ``--until plan`` останавливает прогон **до** онбординга, поэтому seed-префлайт не
    запускается (иначе он блокировал бы прогон, который не собирается краулить).
    """
    if until is None:
        return True
    return PHASES.index(until) >= PHASES.index("g1")


def _identity_payload(identity: RunIdentity) -> dict[str, Any]:
    """Поля идентичности прогона для ``--json``/``--dry-run`` (09 §2.4/§12.3)."""
    return {
        "cycle_id": identity.cycle_id,
        "previous_run_id": identity.previous_run_id,
        "change_class": identity.change_class.value,
        "golden_required": identity.golden_required,
        "run_scope": identity.run_scope,
        "refresh_policy": identity.refresh_policy.value,
        "cohort_digest": identity.cohort_digest,
        "seeds_digest": identity.seeds_digest,
        "execution_contract_digest": identity.execution_contract_digest,
    }


def _blocked_payload(
    run_id: str,
    db_path: Path,
    onboarding: Any,
    *,
    warnings: list[str],
) -> dict[str, Any]:
    """Payload остановки onboarding: продукт + причина, без corpus/cells."""
    missing = {plan.product_id: list(plan.reasons) for plan in onboarding.plans}
    return {
        "ok": False,
        "run_id": run_id,
        "db": str(db_path),
        "status": "blocked",
        "stopped_at": "onboarding",
        "reason": "product_not_ready",
        "blocked": list(onboarding.blocked),
        "onboarding_missing": {
            product_id: reasons
            for product_id, reasons in missing.items()
            if reasons
        },
        "products": [
            plan.model_dump(mode="json") for plan in onboarding.plans
        ],
        "errors": [
            f"product_not_ready: {product_id} "
            f"({', '.join(onboarding.reasons_for(product_id))}); next run blocked"
            for product_id in onboarding.blocked
        ],
        "results": [],
        "resumed_skipped": [],
        "warnings": list(warnings),
    }


def _lexical_rebuild_reason(store: Any) -> str:
    """Почему лексический индекс не собрался (видимая причина, а не тихая дыра)."""
    if store is None:
        return "store не открыт"
    if not tantivy_available():
        return "tantivy недоступен"
    return "в композиции нет чанков"


def _vector_rebuild_reason() -> str:
    """Почему dense-индекс не собрался (видимая причина, а не тихая дыра)."""
    if not numpy_available():
        return "numpy недоступен"
    return "нет манифеста/размерности сборки"


def _rebind_retrieval_after_corpus(
    fn: PhaseFn,
    *,
    ports: Ports,
    store: Any,
    index_root: str,
    package: str,
    run_id: str,
    profile: str,
) -> PhaseFn:
    """Оборачивает фазу ``corpus``: привязывает ретривал к **её** композиции (09 §8.1/§8.4).

    Порты строятся до корпуса, когда ``index_build_id`` ещё неизвестен:
    ``_single_persisted_build`` либо не находит сборки, либо выбирает устаревшую/чужую
    композицию. После ``ok``-корпуса composition id известен (``IndexBuild.index_build_id``),
    поэтому обёртка:

    1. сразу персистит его в ``runs.index_build_id`` (до фазы отчёта; переживает resume);
    2. пересобирает лексический/dense-индексы **по этому id** и присваивает их в тот же
       ``Ports``, который увидит фаза ``evaluate``.

    Сбой фазы не перепривязывает ничего: прежние порты остаются, а ``evaluate`` честно
    сообщает свою ошибку. ``None`` от билдера — легитимное отсутствие предпосылки (нет
    ``tantivy``/``numpy`` или пустая композиция), но не молчаливое: причина попадает в
    ``errors``/``log_entries`` фазы. ``split`` не собирает локальные порты ретривала —
    composition id всё равно персистится, перепривязки нет.
    """

    def wrapped(ctx: RunContext) -> PhaseResult:
        result = fn(ctx)
        if result.status != "ok":
            # Неуспешный корпус не даёт валидной композиции: не трогаем порты и провенанс.
            return result
        build = next(
            (
                entry
                for entry in getattr(result, "log_entries", None) or ()
                if getattr(entry, "index_build_id", None)
            ),
            None,
        )
        composition_id = getattr(build, "index_build_id", None)
        if not composition_id:
            result.errors.append(
                "retrieval: корпус не вернул composition id — "
                "порты ретривала не перепривязаны"
            )
            return result
        try:
            store.set_run_index_build_id(run_id, composition_id)
        except Exception as exc:  # noqa: BLE001 — провенанс не роняет прогон
            result.errors.append(
                f"retrieval: set_run_index_build_id({composition_id}): "
                f"{type(exc).__name__}: {_concise(exc)}"
            )
        if profile == SPLIT_PROFILE:
            # В split локальные индексные порты не собираются (веб/индекс — вне ядра);
            # composition id уже персистирован, перепривязывать нечего.
            result.log_entries.append(
                "retrieval: split — локальные порты ретривала не собираются "
                f"(composition {composition_id})"
            )
            return result
        lexical = None
        vector = None
        try:
            lexical = seed_lexical_index(
                index_root=index_root,
                package=package,
                store=store,
                build_id=composition_id,
            )
            vector = build_vector_index(
                index_root=index_root,
                package=package,
                chunk_lookup=store.get_chunk,
                build_id=composition_id,
            )
        except Exception as exc:  # noqa: BLE001 — перепривязка не роняет успешный корпус
            # Сбой пересборки — видимая диагностика, а не смена статуса Ш5: корпус уже
            # зафиксирован на диске, прежние порты остаются, evaluate сообщит своё.
            result.errors.append(
                f"retrieval: перепривязка к композиции {composition_id} не удалась: "
                f"{type(exc).__name__}: {_concise(exc)}"
            )
            return result
        if lexical is None:
            result.errors.append(
                "retrieval: лексический индекс не пересобран для композиции "
                f"{composition_id}: {_lexical_rebuild_reason(store)}"
            )
        if vector is None:
            result.log_entries.append(
                "retrieval: dense-индекс не пересобран для композиции "
                f"{composition_id}: {_vector_rebuild_reason()}"
            )
        ports.lexical_index = lexical
        ports.vector_index = vector
        return result

    return wrapped


def run(args: argparse.Namespace) -> int:
    """``run``: собрать прогон и провести ``Runner`` по реальным фазам Ш1–Ш9.

    Composition root строит план и реестр фаз (:func:`build_phases`) и связывает их с
    портами профиля (:func:`_resolve_ports`). Гейты G1–G4 намеренно **не** автоматизируются
    (I6/I7): без строки в ``gate_decisions`` прогон останавливается ``blocked`` на ``g1``.
    Resume honour'ится: строка прогона **не пересоздаётся**, если она уже есть, поэтому
    durable-карта фаз в ``runs.state`` сохраняется между процессами (``02`` §A.1).

    ``--dry-run`` показывает план фаз/линт без записи; ``--reset`` форсирует полный
    перезапуск в обход durable-состояния.

    Durable per-product состояние (09 §4.1): ``--dry-run`` читает существующий
    ``state/{package_id}/state.db`` (отсутствующий не создаёт), реальный незаблокированный
    прогон открывает/создаёт его, синхронизирует авторский seed-набор
    (``reconcile_seed_set``) и резолвит режимы продуктов (``new``/``full``/``incremental``).
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    package_id = Path(args.package).name
    # Момент старта прогона — для lineage state-контура (09 §4.2 ``run_lineage``).
    started_at = _utc_now()

    # --- Идентичность прогона (09 §2.4/§6.2/§12.3) ------------------------- #
    # Объём/политика проверяются до любых записей: неоднозначный work-set — ошибка
    # использования, а не молчаливый приоритет флага.
    try:
        run_scope = resolve_run_scope(
            only_failed=bool(getattr(args, "only_failed", False)),
            only_product=getattr(args, "only_product", None),
        )
        refresh_policy = resolve_refresh_policy(
            cli=getattr(args, "refresh", None), run_config=inputs.run
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    if bool(getattr(args, "update", False)) and run_scope == RunScope.ONLY_FAILED.value:
        raise CliError(
            "--update несовместим с --only-failed (09 §6.2)",
            code="usage",
            exit_code=EXIT_USAGE,
        )

    # Предыдущий lineage читается ДО вывода run_id (нужен предыдущий cycle_id для
    # проверки --update) и ДО записи текущей строки (иначе она сослалась бы на себя).
    # ``create=False``: dry-run/ошибочный вызов не создаёт state.db.
    state_store: Any = _open_state_store(args, create=False)
    previous_lineage = (
        state_store.previous_lineage(package_id) if state_store is not None else None
    )
    try:
        cycle_id = resolve_cycle_id(
            cli_cycle_id=getattr(args, "cycle_id", None),
            run_config=inputs.run,
            update=bool(getattr(args, "update", False)),
            today=_dt.date.today(),
            previous_cycle_id=(
                previous_lineage.cycle_id if previous_lineage is not None else None
            ),
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc

    profiles = getattr(inputs, "domain_profiles", None) or {}
    operator_urls: dict[str, list[str]] | None = None
    if getattr(args, "urls", None) is not None:
        operator_urls = _load_corpus_urls(args.urls)
    # Эффективный seed-набор нужен уже для run_id (seeds_digest), поэтому читается до
    # префлайта. Preflight перечитывает его же (идемпотентно, без записи).
    seed_config, _ = load_effective_seeds(
        args.package,
        inputs.run.products,
        profiles=profiles,
        urls_override=operator_urls,
    )
    cohort_d = cohort_digest(
        inputs.cohort,
        case_id=inputs.run.case_id,
        cohort_ref=getattr(inputs.run, "cohort_ref", None),
    )
    seeds_d = seeds_digest(seed_config)
    contract_d = build_execution_contract(
        inputs,
        cohort_digest=cohort_d,
        seeds_digest=seeds_d,
        retrieval_profile=getattr(args, "retrieval_profile", None),
        cutoff_K=getattr(args, "cutoff_k", None),
    )
    run_id = args.run_id or derive_run_id(
        inputs,
        cycle_id=cycle_id,
        cohort_digest=cohort_d,
        seeds_digest=seeds_d,
        execution_contract_digest=contract_d,
        run_scope=run_scope,
        refresh_policy=refresh_policy.value,
    )

    previous_run_id = getattr(args, "since_run", None)
    previous = None
    if previous_run_id is not None:
        previous = (
            state_store.get_run_lineage(previous_run_id)
            if state_store is not None
            else None
        )
        if previous is None:
            raise CliError(
                f"--since-run: прогон '{previous_run_id}' не найден в state.db",
                code="usage",
                exit_code=EXIT_USAGE,
            )
    else:
        previous = previous_lineage
        previous_run_id = previous.run_id if previous is not None else None
    # Resume текущего прогона — не «предыдущий»: carry-forward не выполняется (§12.3).
    if previous_run_id == run_id:
        previous_run_id = None
        previous = None
    identity = build_run_identity(
        run_id=run_id,
        cycle_id=cycle_id,
        previous_run_id=previous_run_id,
        previous_products=previous.products if previous is not None else None,
        previous_execution_contract_digest=(
            previous.execution_contract_digest if previous is not None else None
        ),
        cohort_digest=cohort_d,
        seeds_digest=seeds_d,
        execution_contract_digest=contract_d,
        run_scope=run_scope,
        refresh_policy=refresh_policy,
        current_products=inputs.run.products,
    )

    db_path = Path(args.db) if args.db is not None else run_db_path(args.runs_dir, run_id)
    # План строится ОДИН раз здесь и переиспользуется фазами/префлайтом: повторный
    # ``plan()`` дал бы иной ``run_id``/двойной линт (см. core/pipeline/phases.py).
    run_plan = _resolve_plan(args, inputs, run_id)
    # Валидация per-URL режимов рендера (Ш5) — до dry-run, чтобы ошибка входа была
    # видна и в сухом прогоне (I19: никакого молчаливого фетча/подмены).
    render_modes = _load_render_modes(getattr(args, "render_modes", None))

    # Durable per-product состояние (09 §4.1): ``create=False`` выше — только чтение;
    # реальный прогон открывает/создаёт state после проверки блокировки онбординга
    # (заблокированный прогон state не создаёт).
    try:
        # MVP-1: onboarding-префлайт (O1). ``--urls`` — полная замена пакетного seed-набора;
        # иначе seeds.yaml[product] > corpus_urls.yaml[product] (legacy fallback + warning).
        onboarding = None
        # ``--only-product`` сужает префлайт: заблокированный продукт **вне** области
        # прогона не должен останавливать частичный прогон (09 §6.2/§13.6). Полный
        # прогон по-прежнему блокируется любым неготовым продуктом когорты.
        scope = (
            ProductScope.PRODUCT_ONLY
            if getattr(args, "only_product", None) is not None
            else ProductScope.FULL
        )
        preflight_products = (
            [str(args.only_product)]
            if scope is ProductScope.PRODUCT_ONLY
            else run_plan.products
        )
        if _onboarding_in_scope(args.until):
            onboarding = preflight(
                args.package,
                preflight_products,
                profiles=profiles,
                urls_override=operator_urls,
                scope=scope,
                state=state_store,
            )
        onboarding_warnings = list(onboarding.warnings) if onboarding is not None else []

        if args.dry_run:
            if not report.ready:
                raise LintFailure([*report.blocking, *report.incomplete], report.warnings)
            if onboarding is not None and onboarding.blocked:
                payload = _blocked_payload(
                    run_id,
                    db_path,
                    onboarding,
                    warnings=[*report.warnings, *onboarding_warnings],
                )
                payload.update(_identity_payload(identity))
                payload["dry_run"] = True
                payload["until"] = args.until
                payload["phases"] = _phases_until(args.until)
                if args.json:
                    _print_json(payload)
                else:
                    _print_run_dry(payload)
                return EXIT_BLOCKED
            payload = {
                "ok": True,
                "dry_run": True,
                "run_id": run_id,
                "db": str(db_path),
                "until": args.until,
                "phases": _phases_until(args.until),
                "render_modes": {url: mode.value for url, mode in render_modes.items()},
                "warnings": [*report.warnings, *onboarding_warnings],
                **_identity_payload(identity),
            }
            if onboarding is not None:
                payload["products"] = [
                    plan.model_dump(mode="json") for plan in onboarding.plans
                ]
                payload["blocked"] = list(onboarding.blocked)
                payload["seeds"] = {
                    plan.product_id: plan.seed_count for plan in onboarding.plans
                }
            if args.json:
                _print_json(payload)
            else:
                _print_run_dry(payload)
            return EXIT_OK

        if not report.ready:
            raise LintFailure([*report.blocking, *report.incomplete], report.warnings)

        # Заблокированный продукт: пишем снимок входов (прогон виден), но не собираем
        # корпус/ячейки. ``onboarding`` — человеческая предпосылка, а не сбой кода.
        if onboarding is not None and onboarding.blocked:
            blocked_store = build_store(db_path, profile=args.profile)
            try:
                _ensure_run(blocked_store, run_id, inputs)
                _snapshot_run_inputs(
                    args, run_id, inputs=inputs, run_plan=run_plan, store=blocked_store
                )
            finally:
                blocked_store.close()
            payload = _blocked_payload(
                run_id,
                db_path,
                onboarding,
                warnings=[*report.warnings, *onboarding_warnings],
            )
            payload.update(_identity_payload(identity))
            if args.json:
                _print_json(payload)
            else:
                _print_run_report(payload)
            return EXIT_BLOCKED

        # Реальный незаблокированный прогон: durable state пакета открывается/создаётся
        # здесь (заблокированный прогон state не создаёт). Перед префлайтом синхронизируем
        # авторский seed-набор → ``seed_set`` (09 §5.3): ``seeds.yaml`` — источник истины,
        # таблица — операционный кэш; seed, удалённый из файла, становится ``disabled``.
        if _onboarding_in_scope(args.until) and state_store is None:
            state_store = _open_state_store(args, create=True)
        if state_store is not None and onboarding is not None:
            for plan in onboarding.plans:
                rows = effective_seeds(
                    onboarding.seeds, plan.product_id, profiles=profiles
                )
                if rows:
                    state_store.reconcile_seed_set(plan.product_id, rows)
            onboarding = preflight(
                args.package,
                preflight_products,
                profiles=profiles,
                urls_override=operator_urls,
                scope=scope,
                state=state_store,
            )
            onboarding_warnings = list(onboarding.warnings)
            # Материализация готовности продукта (09 §5.3): без этой записи
            # ``resolve_product_modes`` всегда видит ``None`` и все продукты остаются
            # ``new``. Рубежи ``last_*_run_id`` существующих строк не затираются.
            try:
                sync_product_state(
                    state_store,
                    products=[plan.product_id for plan in onboarding.plans],
                    onboarding_plans=onboarding.plans,
                    seeds=onboarding.seeds,
                )
            except Exception as exc:  # noqa: BLE001 — state не роняет прогон
                onboarding_warnings.append(
                    f"state: sync_product_state: {type(exc).__name__}: {_concise(exc)}"
                )

        # E1/A3 (R7/§13): не-понижающий персист измеренной фиксации ретривала **и её
        # усыновление**. Резолвим персистентную фиксацию ДО сборки фаз: обычный повторный
        # прогон должен **усыновить** измеренные значения (профиль + K) в аргументы, чтобы
        # оценка и ячейки несли ту же настройку, что и манифест. Прежнее поведение
        # (переклеить только манифест) давало ЛОЖНУЮ запись: ячейки R0, манифест measured R2 —
        # это ломало §13-diff и сравнение golden-выреза I20. Явный CLI-профиль/``--cutoff-k``
        # приоритетнее фиксации и не переопределяется молча; явный **противоречащий** профиль
        # отвергается (A3: изменить измеренное можно только пере-измерением).
        explicit_profile = getattr(args, "retrieval_profile", None)
        explicit_cutoff = getattr(args, "cutoff_k", None)
        if db_path.exists():
            with build_store(db_path, profile=args.profile) as check_store:
                try:
                    fixation = resolve_measured_retrieval(
                        check_store, run_id, explicit_profile
                    )
                except ValueError as exc:
                    raise CliError(str(exc), code="§13", exit_code=EXIT_ERROR) from exc
            if fixation is not None and explicit_profile is None:
                args.retrieval_profile = fixation["retrieval_profile"]
                if explicit_cutoff is None and fixation["retrieval_cutoff_K"] is not None:
                    args.cutoff_k = fixation["retrieval_cutoff_K"]

        # R7 (07 §7): деплой-провенанс манифеста прогона. Конфиг загружается всегда:
        # секция ``crawl`` (03 §D.5/D.6) — операционная политика HTTP-краулинга корпуса и в
        # профиле ``local``; для ``split`` дополнительно валидируется шов. Сам деплой-провенанс
        # манифеста течёт через ``_resolve_phases(deploy_config=…)`` → фазу Ш9 (run_manifest.yaml
        # и строка ``runs``); здесь те же поля кладутся в отчёт прогона (``deploy``) для
        # наблюдаемости CLI. Версии edge-компонентов (``/v1/capabilities``) cmd_run не собирает —
        # пусто, если не известны (значения не выдумываются).
        deploy_fields: dict[str, Any] = {}
        deploy_config = load_deploy_config(None)
        if args.profile == SPLIT_PROFILE:
            if not is_split(deploy_config):
                raise WiringError(
                    "профиль split требует deploy-конфиг с profile split/split-hardened "
                    "(07 §6/§7)"
                )
        deploy_fields = manifest_deploy_fields(deploy_config)

        # Phase 4: ``--from-scratch`` не строит R7-кэш (cache=None) — живой ре-фетч; URL-фильтры
        # (``--only-failed``/``--only-product``) читают durable-сеанс до сборки фаз, чтобы
        # исполнитель и локальный путь видели один work-set.
        from_scratch = bool(getattr(args, "from_scratch", False))
        only_failed = bool(getattr(args, "only_failed", False))
        only_product = getattr(args, "only_product", None)
        max_attempts = getattr(args, "max_attempts", None)
        max_depth = getattr(args, "max_depth", None)
        max_documents = getattr(args, "max_documents", None)
        exclude_paths = getattr(args, "exclude_paths", None)
        exclude_documents = getattr(args, "exclude_documents", None)
        document_level = getattr(args, "document_level", None)
        follow = bool(getattr(args, "follow", False))
        interval = float(getattr(args, "interval", 1.0) or 0.0)
        session_id = default_session_id(run_id)
        filter_notes: list[str] = []
        if onboarding is not None:
            corpus_urls = dict(onboarding.effective_urls)
        else:
            corpus_urls = _load_corpus_urls(getattr(args, "urls", None))
        # Одно соединение на прогон (R7): фильтрация URL, сборка портов (``store``/
        # ``chunk_lookup`` для лексического/dense плеча), Runner и пост-корпусная
        # перепривязка ретривала работают на этом объекте. Открываем **до**
        # ``_resolve_ports``: без ``store``/``chunk_lookup`` оба билдера ретривала
        # возвращают ``None``, и Ш6 падает на каждой ячейке.
        store = build_store(db_path, profile=args.profile)
        ports: Ports | None = None
        try:
            if only_failed or only_product is not None:
                corpus_urls, filter_notes = _filter_corpus_urls(
                    corpus_urls,
                    store=store,
                    session_id=session_id,
                    only_failed=only_failed,
                    only_product=only_product,
                )

            ports = _resolve_ports(
                args,
                deploy_config,
                inputs=inputs,
                no_cache=from_scratch,
                store=store,
                run_id=run_id,
            )
            phases = _resolve_phases(
                args,
                inputs=inputs,
                plan=run_plan,
                ports=ports,
                deploy_config=deploy_config,
                paths=getattr(args, "paths", None),
                render_modes=render_modes,
                urls=corpus_urls,
                max_attempts=max_attempts,
                max_depth=max_depth,
                max_documents=max_documents,
                exclude_paths=exclude_paths,
                exclude_documents=exclude_documents,
                document_level=document_level,
                progress=(
                    _progress_callback(session_id=session_id, json_mode=bool(args.json))
                    if follow
                    else None
                ),
                # 09 §6.3 (MVP-3b): уже открытый state пакета (``None`` — например, dry-run
                # сюда не доходит), режимы продуктов из onboarding-плана и политика наблюдения.
                # Dry-run не создаёт ``state/``: фазы строятся только после его выхода.
                state=state_store,
                product_modes=(
                    {plan.product_id: plan.mode.value for plan in onboarding.plans}
                    if onboarding is not None
                    else None
                ),
                refresh_policy=refresh_policy.value,
                # 09 §12.5: тот же дайджест, что ушёл в ``run_id``/lineage (``contract_d``),
                # — манифест/``runs`` не пересчитывают его из другого набора значений.
                execution_contract_digest=contract_d,
            )
            # 09 §8.1/§8.4: порты ретривала строятся до корпуса, где composition id
            # ещё неизвестен. После успешного корпуса перепривязываем их к его
            # композиции и сразу фиксируем ``runs.index_build_id`` (до фазы отчёта,
            # переживает resume). Инъецированные ``ports`` (тестовый шов) не трогаем:
            # ручной DI приоритетнее автосборки.
            if getattr(args, "ports", None) is None and "corpus" in phases:
                phases["corpus"] = _rebind_retrieval_after_corpus(
                    phases["corpus"],
                    ports=ports,
                    store=store,
                    index_root=(
                        str(args.paths.index_store)
                        if getattr(args, "paths", None) is not None
                        else "index_store"
                    ),
                    package=resolve_package_id(inputs),
                    run_id=run_id,
                    profile=args.profile,
                )
            # Наблюдаемость (W12/T12.1, 04 §B.1/§B.2): оборачиваем фазы логом+таймингом.
            # Гейты — дефолтные замыкания Runner; оборачиваем и их, чтобы §B.1-лог покрывал
            # точку остановки (g1 без человеческого решения тоже оставляет logs/g1.jsonl).
            clock = getattr(args, "clock", None)
            instrumentable: dict[str, PhaseFn] = dict(phases)
            for gate_name, gate in GATE_PHASES.items():
                instrumentable.setdefault(gate_name, _gate_phase(gate_name, gate))
            instrumentation = instrument_phases(
                instrumentable,
                run_id=run_id,
                logs_root=args.runs_dir,
                clock=clock,
            )

        except BaseException:
            # Сборка портов/фаз не должна утекать соединением: закрываем и пробрасываем.
            store.close()
            raise

        instrumentation_errors: list[str] = []
        final_status: str | None = None
        crawl_counts: dict[str, int] = {}
        truncation: dict[str, Any] = {}
        try:
            _ensure_run(store, run_id, inputs)
            # 03 §A.2 / P1: снимок входов прогона (иммутабельная копия run.yaml +
            # производный plan_inputs.json) и источник истины — таблица ``run_inputs`` в БД
            # (каталог/веса/когорта). Пишется ДО исполнения фаз, чтобы последующие команды
            # были самодостаточны без исходного пакета. Сбой снимка не роняет прогон:
            # фиксируется в instrumentation_errors и не подменяет код возврата.
            try:
                instrumentation_errors.extend(
                    _snapshot_run_inputs(
                        args, run_id, inputs=inputs, run_plan=run_plan, store=store
                    )
                )
            except Exception as exc:  # noqa: BLE001 — самодостаточность не роняет прогон
                instrumentation_errors.append(
                    f"snapshot: {type(exc).__name__}: {_concise(exc)}"
                )
            runner = build_runner(
                store,
                profile=args.profile,
                ports=ports,
                phases=instrumentation.phases,
                clock=clock,
            )
            if args.reset or from_scratch:
                runner.reset(run_id)
                # D3/L1b: сброс durable-краула — очищаем сеанс краула прогона, чтобы resume не
                # подхватил старые retained-элементы. Канонический ``session_id`` сеанса прогона —
                # ``f"crawl:{run_id}"`` (:func:`core.pipeline.crawl_session.default_session_id`).
                # Phase 4 ``--from-scratch`` = сброс сеанса + живой ре-фетч (cache=None);
                # ``--reset`` форсирует повторную попытку, но кэш не обходит.
                try:
                    if store.get_crawl_session(session_id) is not None:
                        store.clear_crawl_session(session_id)
                except Exception as exc:  # noqa: BLE001 — сброс best-effort, не роняет прогон
                    instrumentation_errors.append(
                        f"reset crawl_session: {type(exc).__name__}: {_concise(exc)}"
                    )
            run_report = runner.run(run_id, until=args.until)
            # Durable state — итог прогона (09 §4.4/§5.1): снимки продуктов, рубежи
            # ``last_corpus_run_id``/``last_evaluated_run_id``/``last_completed_run_id`` и
            # ``run_lineage``. ``previous_run_id`` читается **до** записи текущей строки
            # lineage (иначе она указала бы на саму себя). Сбой state-записи — диагностика,
            # а не смена кода возврата прогона.
            if state_store is not None and onboarding is not None:
                try:
                    # Идентичность выведена ДО исполнения фаз (previous_run_id — до
                    # записи текущей строки lineage, иначе она сослалась бы на себя).
                    # В lineage идут реальные cycle_id/дайджесты/класс изменения.
                    record_run_outcome(
                        state_store,
                        run_id=run_id,
                        package_id=package_id,
                        cycle_id=cycle_id,
                        products=[plan.product_id for plan in onboarding.plans],
                        store=store,
                        status=run_report.status,
                        started_at=started_at,
                        completed_at=_utc_now(),
                        previous_run_id=previous_run_id,
                        cohort_digest=cohort_d,
                        seeds_digest=seeds_d,
                        execution_contract_digest=contract_d,
                        change_class=identity.change_class.value,
                        scope=scope.value,
                        refresh_policy=refresh_policy.value,
                        evaluate_ok=any(
                            result.phase == "evaluate" and result.status == "ok"
                            for result in run_report.results
                        ),
                    )
                except Exception as exc:  # noqa: BLE001 — state не роняет прогон
                    instrumentation_errors.append(
                        f"state: record_run_outcome: {type(exc).__name__}: {_concise(exc)}"
                    )
            # Phase 4: ``--follow``/``--watch`` — прогресс до терминала (foreground: сеанс уже
            # терминален → один финальный снимок). Иначе — финальный статус для кода возврата.
            try:
                if follow:
                    final_status = _follow_session(
                        store,
                        session_id,
                        interval=interval,
                        json_mode=bool(args.json),
                        sleep=_SLEEP,
                    )
                else:
                    final_status, _ = _session_snapshot(store, session_id)
            except Exception as exc:  # noqa: BLE001 — наблюдение не роняет прогон
                final_status = None
                instrumentation_errors.append(
                    f"follow: {type(exc).__name__}: {_concise(exc)}"
                )
            # Метрики (04 §B.2) пишутся ДО закрытия store (источник ячеек) и независимо от
            # остановки на гейте — отражают реально отработавшие фазы. Сбой наблюдаемости
            # фиксируется, но не подменяет код возврата прогона.
            try:
                # Backlog A: счётчики durable-сеанса краула в метрики. Соглашение об
                # ``session_id`` живёт на стороне CLI (least coupling): метрики не знают о нём.
                # N4: сводка политического усечения — из провенанса сеанса (для metrics.json).
                try:
                    session = store.get_crawl_session(default_session_id(run_id))
                    crawl_counts = store.crawl_session_counts(default_session_id(run_id))
                    truncation = (
                        truncation_summary(read_provenance(session))
                        if session is not None
                        else {}
                    )
                except Exception as exc:  # noqa: BLE001 — метрики best-effort
                    instrumentation_errors.append(
                        f"crawl_counts: {type(exc).__name__}: {_concise(exc)}"
                    )
                write_run_metrics(
                    run_id=run_id,
                    out_dir=args.runs_dir,
                    durations=instrumentation.durations,
                    store=store,
                    crawl_counts=crawl_counts,
                    crawl_truncation=truncation or None,
                )
            except Exception as exc:  # noqa: BLE001 — наблюдаемость не роняет прогон
                instrumentation_errors.append(
                    f"metrics: {type(exc).__name__}: {_concise(exc)}"
                )
        finally:
            store.close()
            # Best-effort остановка краулера: только если он есть и умеет ``close`` (после
            # разделения реактора — ScrapyCrawler; у HttpCrawler/None ``close`` нет). Проверка
            # существования делает вызов безопасным для обоих краулеров.
            crawler = getattr(ports, "crawler", None) if ports is not None else None
            close = getattr(crawler, "close", None)
            if callable(close):
                close()

        payload = _run_report_payload(run_id, run_report, db_path)
        payload["session_id"] = session_id
        payload["session_status"] = final_status
        # B3: аномалии (I3 ❓-протокол + crawl-усечение) — из артефактов фазы report, если она
        # отработала; видны в run/--json payload (не только в nda_questions.csv).
        artifacts_obj = next(
            (
                entry
                for result_item in run_report.results
                for entry in getattr(result_item, "log_entries", []) or []
                if isinstance(entry, Artifacts)
            ),
            None,
        )
        if artifacts_obj is not None:
            payload["i3_anomalies"] = list(artifacts_obj.i3_anomalies)
            payload["crawl_truncation_anomalies"] = list(
                artifacts_obj.crawl_truncation_anomalies
            )
        if truncation:
            # N4: усечение видно в run/--json payload (а не только в провенансе сеанса).
            payload["crawl_truncation"] = truncation
        if follow:
            payload["follow"] = True
        if filter_notes:
            payload["filter_notes"] = list(filter_notes)
        if onboarding_warnings:
            payload["warnings"] = list(onboarding_warnings)
        if instrumentation_errors:
            payload["instrumentation_errors"] = instrumentation_errors
        if deploy_fields:
            payload["deploy"] = deploy_fields
        # N4: «только политическое усечение» — PARTIAL без реальных сбоев фетча. Такой прогон
        # честно завершён (лимит достигнут), а не сломан: exit success + явная строка-предупреждение.
        truncation_only = bool(truncation.get("truncated")) and not (
            int(crawl_counts.get(CrawlItemStatus.FAILED.value, 0))
            + int(crawl_counts.get(CrawlItemStatus.CANCELLED.value, 0))
        )
        if truncation_only:
            warning = (
                "краул усечён политикой (max_documents/max_depth): корпус неполон; "
                f"продукты={truncation.get('truncated_products')}; "
                f"страниц={truncation.get('pages_fetched')}"
            )
            payload["crawl_truncation_warning"] = warning
        if args.json:
            _print_json(payload)
        else:
            _print_run_report(payload)
        if run_report.stopped_at is None:
            # Phase 4: полный краул (или его отсутствие) → EXIT_OK; ``partial``/``failed`` сеанс
            # (исчерпание попыток, back-pressure, source_unavailable) → EXIT_ERROR. N4: PARTIAL
            # только из-за политического усечения (сбоев фетча нет) — EXIT_OK с предупреждением.
            if final_status in (
                CrawlSessionStatus.PARTIAL.value,
                CrawlSessionStatus.FAILED.value,
            ):
                if truncation_only and final_status == CrawlSessionStatus.PARTIAL.value:
                    return EXIT_OK
                return EXIT_ERROR
            return EXIT_OK
        if run_report.status == "blocked":
            return EXIT_BLOCKED
        return EXIT_ERROR
    finally:
        if state_store is not None:
            state_store.close()


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``run``."""
    p_run = sub.add_parser(
        "run", help="минимальный прогон: создать прогон и провести Runner по фазам"
    )
    p_run.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    p_run.add_argument("--package", required=True, help="каталог пакета категории")
    p_run.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    p_run.add_argument(
        "--update",
        action="store_true",
        help="явно начать новый цикл: требует cycle_id, отличного от предыдущего (09 §2.4)",
    )
    p_run.add_argument(
        "--cycle-id",
        dest="cycle_id",
        default=None,
        help="идентификатор цикла (ISO-дата/ISO-неделя); без --update — "
        "run.yaml:cycle_id > run.yaml:snapshot_date",
    )
    p_run.add_argument(
        "--since-run",
        dest="since_run",
        default=None,
        help="явный predecessor для carry-forward/lineage (по умолчанию — последний "
        "прогон пакета из state.db)",
    )
    p_run.add_argument(
        "--refresh",
        choices=[policy.value for policy in RefreshPolicy],
        default=None,
        help="политика обновления корпуса: auto|revalidate|refresh|offline (дефолт auto)",
    )
    p_run.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db); дефолт — раскладка "
        "(config/paths.yaml/COHOUND_RUNS_DIR/COHOUND_HOME), иначе runs",
    )
    p_run.add_argument("--db", default=None, help="путь к БД прогона (иначе runs/{run_id}/...)")
    p_run.add_argument(
        "--until", choices=PHASES, default=None, help="последняя выполняемая фаза (включительно)"
    )
    p_run.add_argument(
        "--dry-run", action="store_true", help="показать план фаз без записи и исполнения"
    )
    p_run.add_argument(
        "--reset", action="store_true", help="форсировать полный перезапуск (обход resume)"
    )
    _add_phase4_flags(p_run)
    p_run.add_argument(
        "--passports-dir",
        default=None,
        help="каталог паспортов passports/{product_id}/{YYYY-MM-DD}.yaml (дефолт — "
        "раскладка: config/paths.yaml/COHOUND_PASSPORTS_DIR/COHOUND_HOME, иначе passports; "
        "отсутствие каталога → без паспортов)",
    )
    p_run.add_argument(
        "--urls",
        default=None,
        help="опциональный JSON/YAML {product_id: [url, ...]} — операторский корпусный вход "
        "Ш5; без него корпус URL пуст (doc_domains паспортов не фетчатся молча)",
    )
    p_run.add_argument(
        "--render-modes",
        dest="render_modes",
        default=None,
        metavar="FILE",
        help="опциональный JSON/YAML {url: \"http\"|\"browser\"} — per-URL режим рендера Ш5 "
        "(дефолт: HTTP/по домен-профилю)",
    )
    p_run.add_argument(
        "--retrieval-profile",
        dest="retrieval_profile",
        choices=[profile.value for profile in RetrievalProfile],
        default=None,
        metavar="PROFILE",
        help="зафиксированный профиль ретривала R0|R1|R2 (I16). Изменение измеренной "
        "настройки требует пере-измерения (`hound experiment retrieval`, A3)",
    )
    p_run.add_argument(
        "--cutoff-k",
        dest="cutoff_k",
        type=_positive_int,
        default=None,
        metavar="K",
        help="retrieval_cutoff_K (I20) выреза ретривала для фазы оценки; дефолт — "
        "DEFAULT_RETRIEVAL_CUTOFF_K (10)",
    )
    p_run.add_argument(
        "--llm-slot",
        dest="llm_slot",
        choices=["cheap", "flagship"],
        default=None,
        help="активный LLM-слот оценщика (override COHOUND_LLM_SLOT/deploy.llm.slot)",
    )
    _add_common(p_run)
    p_run.set_defaults(handler=run)
