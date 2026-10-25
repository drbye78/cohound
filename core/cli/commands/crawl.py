"""``crawl``: управление сеансами краула (local) — run/status/list/resume/restart/cancel.

Подпакет ``hound crawl`` (``03`` §E.1) — тонкая CLI-поверхность над durable-сеансом
краула (Lane B/L1a): :func:`core.pipeline.corpus.build_corpus` сам открывает/восстанавливает
и финализирует сеанс, поэтому команда только резолвит прогон/порты/фазы (как ``run``) и
вызывает фазу корпуса Ш5 — логика краула здесь **не** дублируется.

Живого worker'а в foreground-профиле ``local`` нет: ``cancel`` терминально снимает сеанс,
а не прерывает работающий процесс. ``running``-сеанс при этом получает ``interrupted``
(тело краула не финализировано никем), а не ``cancelled`` — см. :func:`cancel`.

Семантика подкоманд:

* ``run``     — запустить сеанс краула (фаза корпуса); ``--reset`` очищает сеанс до старта.
* ``status``  — сеанс и счётчики (``--per-url`` — построчно). Сеанса нет → ``EXIT_OK`` с
  ``status: null`` (пустой, но валидный машинночитаемый ответ).
* ``list``    — перечислить сеансы в БД прогона (со свидетельством живости, F1).
* ``resume``  — продолжить прерванный сеанс: та же ``session_id``, уже ``FETCHED``-элементы
  не пере-фетчатся (recovery-on-open + retained raw); идемпотентно.
* ``restart`` — очистить сеанс и запустить заново; ``--from-scratch`` документирует обход
  R7-кэша (Phase 4 — флаг принимается, но кэш пока **не** обходится).
* ``cancel``  — терминально снять сеанс: ``running`` → ``interrupted`` (best-effort;
  foreground без worker'а), неисполнявшийся → ``cancelled``.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import sys
import time
from pathlib import Path
from typing import Any, Callable

from core.cli.context import (
    _add_common,
    _add_llm_model_flag,
    _add_llm_slot_flag,
    _add_phase4_flags,
    _ensure_run,
    _filter_corpus_urls,
    _follow_session,
    _load_corpus_urls,
    _load_render_modes,
    _phases_until,
    _persist_llm_scenarios,
    _print_json,
    _product_callback,
    _progress_callback,
    _resolve_db_path,
    _resolve_plan,
    _resolve_ports,
    _resolve_phases,
    _session_snapshot,
)
from core.cli.errors import (
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    CliError,
    LintFailure,
)
from core.cli.execution_axes import (
    declared_status_dict_version,
    resolve_declared_axes,
)
from core.cli.wiring import (
    LOCAL_PROFILE,
    build_store,
    llm_env_notices,
    preflight_extraction_budget,
    resolve_extraction_budget,
)
from core.config.deploy import load_deploy_config, profile_notices
from core.domain import (
    CrawlItemStatus,
    CrawlRunConfig,
    CrawlSessionStatus,
    RefreshPolicy,
)
from core.pipeline import PHASES
from core.pipeline.crawl_session import (
    default_session_id,
    read_provenance,
    truncation_summary,
)
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.lint import lint_run_inputs
from core.pipeline.liveness import interrupt_session, stale_reason
from core.pipeline.normalize import load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.plan import derive_run_id
from core.pipeline.run_identity import (
    build_execution_contract,
    resolve_cycle_id,
    resolve_run_scope,
)
from core.pipeline.runner import RunContext

__all__ = [
    "configure",
    "run",
    "status",
    "list_sessions",
    "resume",
    "restart",
    "cancel",
]

#: Phase 4: хук сна цикла ``--follow`` (тесты инжектят no-op через monkeypatch).
_SLEEP: Callable[[float], None] = time.sleep


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC) — метка финала сеанса (``cancel``)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _effective_runs_dir(args: argparse.Namespace) -> str:
    """Эффективный ``runs``-каталог: флаг > resolved-раскладка > прежний литерал ``runs``.

    ``crawl`` не входит в ``main._RUNS_DIR_COMMANDS``, поэтому дефолт раскладки
    подставляется здесь (composition root уже положил ``args.paths``).
    """
    runs_dir = getattr(args, "runs_dir", None)
    if runs_dir is None:
        paths = getattr(args, "paths", None)
        runs_dir = str(paths.runs) if paths is not None else "runs"
    return str(runs_dir)


def _crawl_db_path(args: argparse.Namespace) -> Path:
    """Резолвит БД сеанса: ``--db`` приоритетнее ``--runs-dir`` + ``--run-id``.

    :raises CliError: ``--runs-dir`` без ``--run-id`` (:data:`EXIT_USAGE`).
    """
    args.runs_dir = _effective_runs_dir(args)
    return Path(_resolve_db_path(args))


def _set_runs_dir(args: argparse.Namespace) -> None:
    """Проставляет эффективный ``runs_dir`` для ``_resolve_phases`` (out_dir Ш9)."""
    if getattr(args, "runs_dir", None) is None:
        args.runs_dir = _effective_runs_dir(args)


def _session_id_for(args: argparse.Namespace) -> tuple[str, str]:
    """``(run_id, session_id)`` для адресации сеанса; ``--run-id`` обязателен.

    :raises CliError: ``--run-id`` не задан (:data:`EXIT_USAGE`).
    """
    run_id = getattr(args, "run_id", None)
    if not run_id:
        raise CliError(
            "нужен --run-id (канонический session_id — f\"crawl:{run_id}\")",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return str(run_id), default_session_id(str(run_id))


def _crawl_counts(result: Any) -> dict[str, int]:
    """Счётчики краула из ``IndexBuild`` фазы корпуса (пусто, если фаза не запускалась)."""
    for entry in getattr(result, "log_entries", None) or ():
        if hasattr(entry, "crawl_pending"):
            return {
                "pending": int(entry.crawl_pending),
                "fetched": int(entry.crawl_fetched),
                "done": int(entry.crawl_done),
                "failed": int(entry.crawl_failed),
            }
    return {}


def _item_row(item: Any) -> dict[str, Any]:
    """JSON-совместимая строка элемента сеанса (``crawl status --per-url``)."""
    status = item.status
    return {
        "product_id": item.product_id,
        "url": item.url,
        "status": status.value if isinstance(status, CrawlItemStatus) else str(status),
        "attempts": item.attempts,
        "engine": item.engine,
        "kind": item.kind,
        "http_status": item.http_status,
        "content_type": item.content_type,
        "lang": item.lang,
        "error_kind": item.error_kind,
        "error": item.error,
    }


def _session_fields(session: Any) -> dict[str, Any]:
    """Поля жизненного цикла сеанса для payload (``None`` при отсутствии сеанса).

    F1: ``owner``/``heartbeat_at`` — свидетельство живости, ``stale_reason`` — почему
    ``running``-сеанс считается прерванным (``None`` — сеанс живой или уже терминален).
    Оператору эти поля нужны, чтобы отличать «ещё работает» от «умер и ждёт recovery».
    """
    if session is None:
        return {
            "status": None,
            "created_at": None,
            "started_at": None,
            "finished_at": None,
            "total": 0,
            "error": None,
            "owner": None,
            "heartbeat_at": None,
            "stale_reason": None,
        }
    return {
        "status": session.status.value,
        "created_at": session.created_at,
        "started_at": session.started_at,
        "finished_at": session.finished_at,
        "total": session.total,
        "error": session.error,
        "owner": getattr(session, "owner", None),
        "heartbeat_at": getattr(session, "heartbeat_at", None),
        "stale_reason": stale_reason(session),
    }


def _print_crawl_run(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``crawl run``/``resume``/``restart``."""
    lines = [
        f"mode: {payload['mode']}",
        f"run_id: {payload['run_id']}",
        f"session_id: {payload['session_id']}",
        f"db: {payload['db']}",
        f"status: {payload['status']}",
    ]
    if payload.get("cleared"):
        lines.append("session: очищен перед запуском")
    if payload.get("session_status"):
        lines.append(f"session_status: {payload['session_status']}")
    counts = payload.get("crawl") or {}
    if counts:
        lines.append(
            "crawl: " + ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
        )
    for note in payload.get("notes", ()):
        lines.append(f"note: {note}")
    for error in payload.get("errors", ()):
        lines.append(f"error: {error}")
    import sys

    sys.stdout.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# run / resume / restart
# ---------------------------------------------------------------------------


def _execute_crawl(
    args: argparse.Namespace, *, mode: str, clear_first: bool, from_scratch: bool = False
) -> int:
    """Общий путь ``run``/``resume``/``restart``: резолв прогона → фаза корпуса Ш5.

    Сеанс открывается/восстанавливается/финализируется самим ``build_corpus``; здесь
    только DI: план/порты/фазы строятся тем же composition root'ом, что и ``run``.

    Phase 4: ``from_scratch`` — сброс сеанса + живой ре-фетч в обход R7-кэша
    (``cache=None``); ``--only-failed``/``--only-product`` фильтруют URL **до** сборки фаз
    (исполнитель, чей work-set выводится из ``urls``, видит тот же набор); ``--max-attempts``
    — потолок попыток; ``--follow`` печатает прогресс сеанса до терминального статуса. Код
    возврата различает полный краул и ``partial`` (частичные сбои → :data:`EXIT_ERROR`).
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    if not report.ready:
        raise LintFailure([*report.blocking, *report.incomplete], report.warnings)

    # Идентичность прогона — та же формула, что у ``run`` (09 §6.2): иначе crawl
    # адресовал бы другой ``run_id``/сеанс. ``--cycle-id`` здесь единственный
    # identity-флаг; refresh/scope берутся из общих флагов и дефолтов.
    try:
        run_scope = resolve_run_scope(
            only_failed=bool(getattr(args, "only_failed", False)),
            only_product=getattr(args, "only_product", None),
        )
        cycle_id = resolve_cycle_id(
            cli_cycle_id=getattr(args, "cycle_id", None),
            run_config=inputs.run,
            update=False,
            today=_dt.date.today(),
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    profiles = getattr(inputs, "domain_profiles", None) or {}
    operator_urls = (
        _load_corpus_urls(args.urls) if getattr(args, "urls", None) is not None else None
    )
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
    # §13 (2.4.0): декларированные оси — тот же резолвер и deploy-конфиг, что у ``run``
    # (иначе crawl адресовал бы другой ``run_id``/сеанс).
    # P5/P7: операторские оси краула (``--document-level``/``--pdf-pipeline``) входят в
    # дайджест через ``resolve_declared_axes`` — они меняют текст корпуса, значит ``run_id``.
    deploy_config = load_deploy_config(None)
    try:
        declared = resolve_declared_axes(args=args, inputs=inputs, deploy=deploy_config)
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    status_dict_version = declared_status_dict_version()
    contract_d = build_execution_contract(
        inputs,
        cohort_digest=cohort_d,
        seeds_digest=seeds_d,
        declared=declared,
        status_dict_version=status_dict_version,
    )
    run_id = args.run_id or derive_run_id(
        inputs,
        cycle_id=cycle_id,
        cohort_digest=cohort_d,
        seeds_digest=seeds_d,
        execution_contract_digest=contract_d,
        run_scope=run_scope,
        refresh_policy=RefreshPolicy.AUTO.value,
    )
    # ``_resolve_db_path`` требует явный ``--run-id`` при ``--runs-dir``; выведенный
    # идентификатор фиксируем в args, чтобы crawl адресовал тот же прогон, что run.
    args.run_id = run_id
    _set_runs_dir(args)
    db_path = _crawl_db_path(args)
    render_modes = _load_render_modes(getattr(args, "render_modes", None))

    session_id = default_session_id(run_id)
    until = getattr(args, "until", None) or "corpus"
    json_mode = bool(getattr(args, "json", False))
    only_failed = bool(getattr(args, "only_failed", False))
    only_product = getattr(args, "only_product", None)
    max_attempts = getattr(args, "max_attempts", None)
    max_depth = getattr(args, "max_depth", None)
    max_documents = getattr(args, "max_documents", None)
    exclude_paths = getattr(args, "exclude_paths", None)
    exclude_documents = getattr(args, "exclude_documents", None)
    document_level = getattr(args, "document_level", None)
    pdf_pipeline = getattr(args, "pdf_pipeline", None)
    follow = bool(getattr(args, "follow", False))
    interval = float(getattr(args, "interval", 1.0) or 0.0)
    # Phase 4: ``--from-scratch`` обходит R7-кэш (cache=None — единственный способ) и по
    # семантике «сброс + живой ре-фетч» очищает сеанс (как ``--reset``/``restart``).
    no_cache = bool(from_scratch)
    clear_first = clear_first or bool(getattr(args, "reset", False)) or no_cache

    notes: list[str] = []
    if from_scratch:
        notes.append("from-scratch: R7-кэш обойдён (cache=None) — живой ре-фетч")
    # §7.4/I21: env COHOUND_LLM_MODEL/_DECODER оценщиком не читаются, а endpoint из env
    # вообще вне execution contract — оба расхождения видны в payload, а не молча
    # (эти переменные живут в runbook'ах операторов).
    notes.extend(
        llm_env_notices(
            deploy_llm=getattr(deploy_config, "llm", None),
            slot=getattr(args, "llm_slot", None),
            run_slot=getattr(getattr(inputs, "run", None), "llm_slot", None),
        )
    )
    # D-8: профиль ``split-hardened`` принимается, но ничего не добавляет сверх ``split`` —
    # это видимое предупреждение, а не молчаливое обещание хардненинга.
    notes.extend(profile_notices(deploy_config))

    store = build_store(db_path, profile=args.profile)
    ports: Any = None
    result: Any = None
    cleared = False
    final_status: str | None = None
    final_counts: dict[str, int] = {}
    truncation: dict[str, Any] = {}
    try:
        _ensure_run(store, run_id, inputs)
        # I21/§7.4: объявленный маршрут «сценарий → модель» — durable-факт строки прогона.
        # Crawl-прогон создаёт ту же строку, что и ``run``: манифест не должен показывать
        # контракт с моделью, у которой нет записи в ``llm_scenarios``.
        notes.extend(_persist_llm_scenarios(store, run_id, declared))
        # Phase 4: фильтр URL по durable-сеансу — ДО резолва фаз, чтобы исполнитель
        # (work-set выводится из ``urls``) и локальный путь видели один набор.
        corpus_urls, filter_notes = _filter_corpus_urls(
            _load_corpus_urls(getattr(args, "urls", None)),
            store=store,
            session_id=session_id,
            only_failed=only_failed,
            only_product=only_product,
        )
        notes.extend(filter_notes)

        run_plan = _resolve_plan(args, inputs, run_id)
        # F0/§3.3: бюджет памяти резолвится в composition root и едет в пайплайн явно
        # (фаза окружение не читает). Preflight — тот же, что у ``run``: сеанс краула
        # исполняет ту же Ш5, и заведомо не помещающаяся конфигурация должна отказать
        # на входе, а не OOM-killer'ом на первом документе.
        mem_budget_mb, derive_extract_workers = resolve_extraction_budget(
            deploy_config.profile if deploy_config is not None else LOCAL_PROFILE,
            injected_ports=getattr(args, "ports", None) is not None,
        )
        if "corpus" in _phases_until(until):
            # CLI-уточнение уровня (``--document-level``) — часть run-слоя, ровно как его
            # увидит Ш5: preflight обязан проверять **разрешённый** уровень, иначе отказ
            # и исполнение разошлись бы.
            budget_run_crawl = getattr(getattr(inputs, "run", None), "crawl", None)
            if document_level is not None:
                base_crawl = (
                    budget_run_crawl if budget_run_crawl is not None else CrawlRunConfig()
                )
                budget_run_crawl = base_crawl.model_copy(
                    update={"document_level": document_level}
                )
            budget_error, budget_warnings = preflight_extraction_budget(
                roots=[root for urls in corpus_urls.values() for root in urls],
                deploy=deploy_config,
                run_crawl=budget_run_crawl,
                profiles=getattr(inputs, "domain_profiles", None) or {},
                mem_budget_mb=mem_budget_mb,
                derive_extract_workers=derive_extract_workers,
            )
            notes.extend(budget_warnings)
            if budget_error is not None:
                raise CliError(
                    budget_error.describe(), code="memory", exit_code=EXIT_USAGE
                )
        ports = _resolve_ports(
            args,
            deploy_config,
            inputs=inputs,
            no_cache=no_cache,
            run_id=run_id,
        )
        progress = (
            _progress_callback(session_id=session_id, json_mode=json_mode)
            if follow
            else None
        )
        # Phase 4: продукт-скоуп живого прогресса — «продукт начат/завершён» виден
        # во время дренажа, а не только в конце фазы (та же ось ``--follow``).
        on_product = (
            _product_callback(session_id=session_id, json_mode=json_mode)
            if follow
            else None
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
            pdf_pipeline=pdf_pipeline,
            progress=progress,
            on_product=on_product,
            # §13 (2.4.0): те же декларированные оси, что у контракта.
            axes=declared,
            declared_status_dict_version=status_dict_version,
            # F0: бюджет/окно экстракции — явные входы Ш5 (тот же резолв, что у preflight).
            mem_budget_mb=mem_budget_mb,
            derive_extract_workers=derive_extract_workers,
        )
        if clear_first:
            store.clear_crawl_session(session_id)
            cleared = True
        if "corpus" in _phases_until(until):
            ctx = RunContext(
                run_id=run_id,
                store=store,
                ports=ports,
                clock=getattr(args, "clock", None),
            )
            result = phases["corpus"](ctx)
        else:
            notes.append(f"корпус отсечён --until={until}")
        # Phase 4: ``--follow``/``--watch`` — опрос до терминала (foreground: сеанс уже
        # терминален → один финальный снимок). Иначе читаем финальный статус для кода возврата.
        if follow:
            final_status = _follow_session(
                store,
                session_id,
                interval=interval,
                json_mode=json_mode,
                sleep=_SLEEP,
            )
        else:
            final_status, _ = _session_snapshot(store, session_id)
        # N4: сводка усечения и счётчики — из сеанса (до закрытия стора), для payload и
        # различения «усечено политикой» vs «реальные сбои фетча» при выборе кода возврата.
        try:
            session_obj = store.get_crawl_session(session_id)
            final_counts = store.crawl_session_counts(session_id)
            truncation = (
                truncation_summary(read_provenance(session_obj))
                if session_obj is not None
                else {}
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость best-effort
            notes.append(f"truncation: {type(exc).__name__}: {exc}")
    finally:
        store.close()
        close = getattr(getattr(ports, "crawler", None), "close", None)
        if callable(close):
            close()

    phase_status = result.status if result is not None else "skipped"
    payload = {
        "ok": phase_status == "ok",
        "mode": mode,
        "run_id": run_id,
        "session_id": session_id,
        "db": str(db_path),
        "status": phase_status,
        "session_status": final_status,
        "cleared": cleared,
        "follow": follow,
        "crawl": _crawl_counts(result),
        "errors": list(getattr(result, "errors", []) or []),
        "notes": notes,
    }
    if truncation:
        # N4: усечение видно в crawl/--json payload (не только во внутреннем провенансе).
        payload["crawl_truncation"] = truncation
    truncation_only = bool(truncation.get("truncated")) and not (
        int(final_counts.get(CrawlItemStatus.FAILED.value, 0))
        + int(final_counts.get(CrawlItemStatus.CANCELLED.value, 0))
    )
    if truncation_only:
        warning = (
            "краул усечён политикой (max_documents/max_depth): корпус неполон; "
            f"продукты={truncation.get('truncated_products')}; "
            f"страниц={truncation.get('pages_fetched')}; "
            f"ревалидация={truncation.get('revalidation_fetched')}"
        )
        payload.setdefault("notes", []).append(warning)
    if args.json:
        _print_json(payload)
    else:
        _print_crawl_run(payload)
    if phase_status not in ("ok", "skipped"):
        return EXIT_ERROR
    # Phase 4: ``partial``/``failed`` сеанс (исчерпание попыток, back-pressure, любые
    # ``source_unavailable``) → :data:`EXIT_ERROR`; полный краул/без сеанса → :data:`EXIT_OK`.
    # N4: PARTIAL исключительно из-за политического усечения (сбоев фетча нет) — честно
    # завершённый ограниченный краул: exit success + предупреждение, не ошибка.
    # F1: ``interrupted`` — прогон прерван без размотки (сеанс снят/убит), тело краула не
    # финализировано: это не успех, как и ``partial``/``failed``.
    if final_status in (
        CrawlSessionStatus.PARTIAL.value,
        CrawlSessionStatus.FAILED.value,
        CrawlSessionStatus.INTERRUPTED.value,
    ):
        if truncation_only and final_status == CrawlSessionStatus.PARTIAL.value:
            return EXIT_OK
        return EXIT_ERROR
    return EXIT_OK


def run(args: argparse.Namespace) -> int:
    """``crawl run``: запустить сеанс краула (``--reset`` очищает сеанс до старта)."""
    return _execute_crawl(
        args,
        mode="run",
        clear_first=bool(getattr(args, "reset", False)),
        from_scratch=bool(getattr(args, "from_scratch", False)),
    )


def resume(args: argparse.Namespace) -> int:
    """``crawl resume``: продолжить сеанс (идемпотентно; ``FETCHED`` не пере-фетчатся)."""
    return _execute_crawl(
        args,
        mode="resume",
        clear_first=False,
        from_scratch=bool(getattr(args, "from_scratch", False)),
    )


def restart(args: argparse.Namespace) -> int:
    """``crawl restart``: очистить сеанс и запустить заново (``--from-scratch`` — Phase 4)."""
    return _execute_crawl(
        args,
        mode="restart",
        clear_first=True,
        from_scratch=bool(getattr(args, "from_scratch", False)),
    )


# ---------------------------------------------------------------------------
# status / list / cancel
# ---------------------------------------------------------------------------


def status(args: argparse.Namespace) -> int:
    """``crawl status``: сеанс и счётчики; отсутствие сеанса → ``status: null`` (``EXIT_OK``)."""
    db_path = _crawl_db_path(args)
    run_id, session_id = _session_id_for(args)
    per_url = bool(getattr(args, "per_url", False))
    with build_store(db_path, profile=args.profile, read_only=True) as store:
        session = store.get_crawl_session(session_id)
        counts = store.crawl_session_counts(session_id) if session is not None else {}
        items = (
            list(store.iter_crawl_session_items(session_id))
            if session is not None and per_url
            else []
        )
        payload: dict[str, Any] = {
            "ok": True,
            "run_id": run_id,
            "session_id": session_id,
            "db": str(db_path),
            "counts": counts,
            **_session_fields(session),
        }
        if per_url:
            payload["items"] = [_item_row(item) for item in items]
        if args.json:
            _print_json(payload)
        elif session is None:
            sys.stdout.write(f"session_id: {session_id}\nstatus: —\n(сеанс не найден)\n")
        else:
            lines = [
                f"session_id: {session_id}",
                f"status: {session.status.value}",
                f"total: {session.total}",
                "counts: " + ", ".join(f"{key}={counts[key]}" for key in sorted(counts)),
            ]
            # F1: свидетельство живости — видно, что ``running`` живой, а не фантом.
            if payload.get("heartbeat_at") or payload.get("owner"):
                lines.append(
                    f"liveness: owner={payload.get('owner') or '—'}"
                    f" heartbeat_at={payload.get('heartbeat_at') or '—'}"
                )
            if payload.get("stale_reason"):
                lines.append(f"stale: {payload['stale_reason']}")
            if payload.get("error"):
                lines.append(f"error: {payload['error']}")
            for row in payload.get("items", ()):
                lines.append(f"  {row['product_id']} {row['status']} {row['url']}")
            sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


def list_sessions(args: argparse.Namespace) -> int:
    """``crawl list``: перечислить сеансы в БД прогона (read-only).

    F1: строка несёт свидетельство живости (``owner``/``heartbeat_at``) и ``stale_reason``,
    поэтому ``running``-фантом отличим от живого сеанса без отдельной команды. Recovery
    здесь **не** выполняется: ``list`` читает, а не лечит (лечит ``open_session``/``run``).
    """
    db_path = _crawl_db_path(args)
    run_filter = getattr(args, "run_id", None)
    with build_store(db_path, profile=args.profile, read_only=True) as store:
        sessions = [
            {
                "session_id": session.session_id,
                "run_id": session.run_id,
                "profile": session.profile,
                "status": session.status.value,
                "created_at": session.created_at,
                "started_at": session.started_at,
                "finished_at": session.finished_at,
                "total": session.total,
                "pending": session.pending,
                "running": session.running,
                "fetched": session.fetched,
                "failed": session.failed,
                "cancelled": session.cancelled,
                "owner": session.owner,
                "heartbeat_at": session.heartbeat_at,
                "stale_reason": stale_reason(session),
            }
            for session in store.iter_crawl_sessions(run_id=run_filter)
        ]
        if args.json:
            _print_json(
                {
                    "ok": True,
                    "db": str(db_path),
                    "run_id": run_filter,
                    "count": len(sessions),
                    "sessions": sessions,
                }
            )
        else:
            lines = [f"db: {db_path}"]
            if run_filter is not None:
                lines.append(f"run_id: {run_filter}")
            lines.append(f"sessions: {len(sessions)}")
            for session in sessions:
                suffix = ""
                if session["stale_reason"]:
                    suffix = f" stale({session['stale_reason'].split(':', 1)[0]})"
                lines.append(
                    f"  {session['session_id']} [{session['status']}]{suffix} "
                    f"total={session['total']} fetched={session['fetched']} "
                    f"failed={session['failed']}"
                )
            sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


def cancel(args: argparse.Namespace) -> int:
    """``crawl cancel``: терминально снять сеанс (best-effort, foreground без worker'а).

    F1: ``running``-сеанс переводится в ``interrupted``, а не в ``cancelled``. Причина —
    честность: в профиле ``local`` живого worker'а нет, снимать нечего, и тело краула
    **не финализируется** никем. ``cancelled`` означало бы «краул снят и это проверено» —
    заявление, которого никто не делал; ``interrupted`` означает ровно «прервано, тело не
    финализировано» (``10`` §5.6). Причина всё равно названа: ``error`` начинается с
    ``cancelled_by_operator``, поэтому «оператор снял» отличимо от «процесс убит».
    Сеанс, который не исполняется (``created``/``paused``/терминальный), получает
    ``cancelled``: тела в полёте нет, решение оператора применяется напрямую.

    Отсутствие сеанса → ``EXIT_NOT_FOUND`` с понятным сообщением (отменять нечего).
    """
    db_path = _crawl_db_path(args)
    run_id, session_id = _session_id_for(args)
    store = build_store(db_path, profile=args.profile)
    try:
        existing = store.get_crawl_session(session_id)
        if existing is None:
            if args.json:
                _print_json(
                    {
                        "ok": False,
                        "run_id": run_id,
                        "session_id": session_id,
                        "db": str(db_path),
                        "error": "not_found",
                        "status": None,
                    }
                )
            else:
                sys.stdout.write(
                    f"session_id: {session_id}\n(сеанс не найден — отменять нечего)\n"
                )
            return EXIT_NOT_FOUND
        reason = f"cancelled_by_operator: снят командой crawl cancel (owner={existing.owner or '—'})"
        if existing.status is CrawlSessionStatus.RUNNING:
            interrupt_session(store, session_id, reason=reason, at=_now_iso())
            note = (
                "local: живого worker'а нет — тело краула не финализировано, "
                "статус переведён в interrupted (cancelled_by_operator)"
            )
        else:
            store.update_crawl_session_status(
                session_id,
                CrawlSessionStatus.CANCELLED,
                finished_at=_now_iso(),
                error=reason,
            )
            note = "local: сеанс не исполнялся — статус переведён в cancelled"
        session = store.get_crawl_session(session_id)
        payload = {
            "ok": True,
            "run_id": run_id,
            "session_id": session_id,
            "db": str(db_path),
            "notes": [note],
            **_session_fields(session),
        }
        if args.json:
            _print_json(payload)
        else:
            sys.stdout.write(f"session_id: {session_id}\nstatus: {payload['status']}\n")
        return EXIT_OK
    finally:
        store.close()


def _no_subcommand(args: argparse.Namespace) -> int:
    """``hound crawl`` без подкоманды: понятная ошибка использования (``EXIT_USAGE``)."""
    raise CliError(
        "не указана подкоманда crawl (run|status|list|resume|restart|cancel)",
        code="usage",
        exit_code=EXIT_USAGE,
    )


# ---------------------------------------------------------------------------
# Регистрация
# ---------------------------------------------------------------------------


def _add_source_flags(parser: argparse.ArgumentParser) -> None:
    """Источники корпуса Ш5: ``--urls`` и ``--render-modes`` (как у ``run``)."""
    parser.add_argument(
        "--urls",
        default=None,
        help="опциональный JSON/YAML {product_id: [url, ...]} — операторский корпусный вход Ш5",
    )
    parser.add_argument(
        "--render-modes",
        dest="render_modes",
        default=None,
        metavar="FILE",
        help="опциональный JSON/YAML {url: \"http\"|\"browser\"} — per-URL режим рендера Ш5",
    )


def _add_run_target_flags(parser: argparse.ArgumentParser) -> None:
    """Общие флаги резолва прогона (как у ``run``): run-yaml/package/run-id/runs-dir/db."""
    parser.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    parser.add_argument("--package", required=True, help="каталог пакета категории")
    parser.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    parser.add_argument(
        "--cycle-id",
        dest="cycle_id",
        default=None,
        help="идентификатор цикла (как у run: run.yaml:cycle_id > snapshot_date); "
        "нужен для совпадения run_id с run",
    )
    parser.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db); дефолт — resolved раскладка, иначе runs",
    )
    parser.add_argument("--db", default=None, help="путь к БД прогона (иначе runs/{run_id}/...)")


def _add_session_target_flags(
    parser: argparse.ArgumentParser, *, require_run_id: bool
) -> None:
    """Флаги адресации сеанса: ``--run-id`` (``session_id = f"crawl:{run_id}"``),
    ``--runs-dir``, ``--db``."""
    parser.add_argument(
        "--run-id",
        required=require_run_id,
        default=None,
        help="идентификатор прогона (канонический session_id — f\"crawl:{run_id}\")",
    )
    parser.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db); дефолт — resolved раскладка, иначе runs",
    )
    parser.add_argument("--db", default=None, help="путь к БД прогона (приоритетнее --runs-dir)")


def configure(sub: Any) -> None:
    """Регистрирует группу ``crawl`` с подкомандами run/status/list/resume/restart/cancel."""
    p_crawl = sub.add_parser(
        "crawl", help="сеансы краула (local): run/status/list/resume/restart/cancel"
    )
    crawl = p_crawl.add_subparsers(dest="crawl_command", metavar="ДЕЙСТВИЕ")

    p_run = crawl.add_parser("run", help="запустить сеанс краула (фаза корпуса Ш5)")
    _add_run_target_flags(p_run)
    _add_source_flags(p_run)
    p_run.add_argument(
        "--until",
        choices=PHASES,
        default="corpus",
        help="фаза-потолок (дефолт corpus); если corpus отсечён — краул не запускается",
    )
    p_run.add_argument(
        "--reset", action="store_true", help="очистить сеанс краула перед запуском"
    )
    _add_phase4_flags(p_run)
    _add_common(p_run)
    _add_llm_slot_flag(p_run)
    _add_llm_model_flag(p_run)
    p_run.set_defaults(handler=run)

    p_status = crawl.add_parser("status", help="статус сеанса и счётчики (read-only)")
    _add_session_target_flags(p_status, require_run_id=True)
    p_status.add_argument(
        "--per-url", dest="per_url", action="store_true", help="построчно: элементы сеанса"
    )
    _add_common(p_status)
    p_status.set_defaults(handler=status)

    p_list = crawl.add_parser("list", help="перечислить сеансы в БД прогона (read-only)")
    _add_session_target_flags(p_list, require_run_id=False)
    _add_common(p_list)
    p_list.set_defaults(handler=list_sessions)

    p_resume = crawl.add_parser("resume", help="продолжить прерванный сеанс (идемпотентно)")
    _add_run_target_flags(p_resume)
    _add_source_flags(p_resume)
    _add_phase4_flags(p_resume)
    _add_common(p_resume)
    _add_llm_slot_flag(p_resume)
    _add_llm_model_flag(p_resume)
    p_resume.set_defaults(handler=resume)

    p_restart = crawl.add_parser("restart", help="очистить сеанс и запустить заново")
    _add_run_target_flags(p_restart)
    _add_source_flags(p_restart)
    _add_phase4_flags(p_restart)
    _add_common(p_restart)
    _add_llm_slot_flag(p_restart)
    _add_llm_model_flag(p_restart)
    p_restart.set_defaults(handler=restart)

    p_cancel = crawl.add_parser("cancel", help="терминально снять сеанс (interrupted|cancelled)")
    _add_session_target_flags(p_cancel, require_run_id=True)
    _add_common(p_cancel)
    p_cancel.set_defaults(handler=cancel)

    p_crawl.set_defaults(handler=_no_subcommand)
