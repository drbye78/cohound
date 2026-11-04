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
* ``status``  — сеанс и счётчики (``--per-url`` — построчно, ``--per-host`` — метрика
  §7.2: host'ы/интервалы/тайминги/backlog; флаги независимы и складываются). Сеанса нет →
  ``EXIT_OK`` с ``status: null`` (пустой, но валидный машинночитаемый ответ).
* ``list``    — перечислить сеансы в БД прогона (со свидетельством живости, F1).
* ``resume``  — продолжить прерванный сеанс: та же ``session_id``, уже ``FETCHED``-элементы
  не пере-фетчатся (recovery-on-open + retained raw); идемпотентно.
* ``restart`` — очистить сеанс и запустить заново; ``--from-scratch`` документирует обход
  R7-кэша (Phase 4 — флаг принимается, но кэш пока **не** обходится).
* ``cancel``  — терминально снять сеанс: ``running`` → ``interrupted`` (best-effort;
  foreground без worker'а), неисполнявшийся → ``cancelled``.

F2 (``17`` §4): ``run``/``resume``/``restart`` принимают ``--crawl-only`` — краул без
конверсии (frontier + фетч + retention + ``FETCHED``), документы уходят в durable-очередь
``extract_queue``, корпус **не** собирается. Режим допустим только при ``--until`` не позже
``corpus``; у полного ``run`` этого флага нет.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _add_llm_model_flag,
    _add_llm_slot_flag,
    _add_phase4_flags,
    _crawl_policy_resolution,
    _ensure_run,
    _filter_corpus_urls,
    _follow_session,
    _load_corpus_urls,
    _load_render_modes,
    _open_state_store,
    _package_lock,
    _persist_llm_scenarios,
    _phases_until,
    _print_json,
    _product_callback,
    _progress_callback,
    _resolve_db_path,
    _resolve_paths,
    _resolve_phases,
    _resolve_plan,
    _resolve_ports,
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
from core.config.crawl_policy import (
    CRAWL_POLICY_FILENAME,
    CrawlPolicyConfig,
    load_crawl_policy,
    parse_site_key,
)
from core.config.deploy import load_deploy_config, profile_notices, shard_notices
from core.config.run_config import load_cohort
from core.domain import (
    CrawlItemStatus,
    CrawlPolicyMode,
    CrawlPolicyScopeKind,
    CrawlRunConfig,
    CrawlSessionStatus,
)
from core.pipeline import PHASES
from core.pipeline.crawl_metric import build_crawl_metric
from core.pipeline.crawl_policy import (
    CrawlPolicyConflict,
    declared_roots,
    observations_by_root,
    resolve_crawl_policy,
)
from core.pipeline.crawl_session import (
    default_session_id,
    read_provenance,
    truncation_summary,
)
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.extract_queue import ExtractQueue
from core.pipeline.lint import lint_run_inputs
from core.pipeline.liveness import interrupt_session, stale_reason
from core.pipeline.normalize import load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.plan import derive_run_id
from core.pipeline.run_identity import (
    build_execution_contract,
    resolve_cycle_id,
    resolve_refresh_policy,
    resolve_run_scope,
)
from core.pipeline.runner import RunContext
from core.ports.state import CrawlPolicyRow
from core.store.state_sqlite import state_db_path
from core.store.state_store import read_crawl_observations, read_crawl_policy_rows

__all__ = [
    "cancel",
    "configure",
    "list_sessions",
    "restart",
    "resume",
    "run",
    "status",
]

#: Phase 4: хук сна цикла ``--follow`` (тесты инжектят no-op через monkeypatch).
_SLEEP: Callable[[float], None] = time.sleep


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC) — метка финала сеанса (``cancel``)."""
    return _dt.datetime.now(_dt.UTC).isoformat()


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


def _phase_notes(result: Any) -> list[str]:
    """Строковые заметки фазы корпуса (``log_entries``) для payload ``crawl run``.

    В полном ``run`` эти строки уходят в ``results[].notes``; ``hound crawl run``
    собирает свой payload и до правки их **терял** — а именно в этом режиме оператор
    читает факты краула (полосы, браузерную очередь, границы продуктов) и именно им
    идёт измерительный протокол 17 §7.2. Строки не ``errors``: это наблюдения, а не
    сбои, поэтому они и попадают в ``notes``.

    Только ``str``: структурные записи ``log_entries`` (``IndexBuild`` с
    ``crawl_*``-счётчиками) в текст не превращаются — у них свой потребитель
    (:func:`_crawl_counts`). Порядок — как у ``errors``: он зависит от порядка
    завершения корней, то есть от планировщика, и в артефакт попадать не должен.
    """
    entries = getattr(result, "log_entries", None) or ()
    return sorted(str(entry) for entry in entries if isinstance(entry, str))


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
    # F2 (17 §4): «краул без конверсии» виден явно, иначе пустой корпус читался бы как
    # провал сборки, а не как объявленный режим.
    if payload.get("crawl_only"):
        lines.append("crawl_only: true")
        queue = payload.get("extract_queue") or {}
        lines.append(
            "extract_queue: "
            + (
                ", ".join(f"{key}={queue[key]}" for key in sorted(queue))
                if queue
                else "пусто"
            )
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
    """``crawl run|resume|restart`` под файловым локом пакета (09 §4.4).

    Лок берётся на всю операцию и до открытия БД прогона: две одновременные операции над
    одним пакетом (два краула, краул и ``run``) делят сеть, бюджет, ``corpus_raw/`` и
    ``index_store/``, а не отвергают друг друга. Занятый лок — ``EXIT_USAGE`` с именем
    держателя и путём файла. ``resume``/``restart`` идут тем же путём осознанно: они
    продолжают сеанс того же пакета и попадают под тот же ресурс.

    F2 (17 §4): ``--crawl-only`` проверяется **до** лока и до открытия БД — это отказ во
    входе (несовместимый ``--until``), а не операция над пакетом.
    """
    _reject_crawl_only_beyond_corpus(args)
    with _package_lock(args, command=f"crawl {mode}"):
        return _execute_crawl_locked(
            args, mode=mode, clear_first=clear_first, from_scratch=from_scratch
        )


def _reject_crawl_only_beyond_corpus(args: argparse.Namespace) -> None:
    """F2 (17 §4): ``--crawl-only`` разрешён, только если фаза-потолок не позже ``corpus``.

    Режим «краул без конверсии» корпус **не** собирает, поэтому ``--until evaluate``
    обещал бы оценку по пустому корпусу — то есть выдал бы отсутствие документов за
    результат измерения. Отказ — ``EXIT_USAGE`` с объяснением, а не тихое усечение.
    """
    if not bool(getattr(args, "crawl_only", False)):
        return
    until = getattr(args, "until", None) or "corpus"
    beyond = [
        phase
        for phase in _phases_until(until)
        if PHASES.index(phase) > PHASES.index("corpus")
    ]
    if beyond:
        raise CliError(
            f"crawl-only не производит корпус: --until={until} требует фаз после corpus "
            f"({', '.join(beyond)}), а они читают собранный корпус. Допустимо "
            "--until corpus (дефолт) или фаза не позже corpus.",
            code="usage",
            exit_code=EXIT_USAGE,
        )


def _execute_crawl_locked(
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
    # План 19 §6.1: политика краула — та же поверхность решений, что у ``plan``/``run``,
    # иначе ``crawl run`` адресовал бы другой ``run_id``. P1 не исполняет политику.
    try:
        policy_resolution = _crawl_policy_resolution(args, inputs, seed_config)
    except CrawlPolicyConflict as exc:
        raise CliError(str(exc), code=exc.code, exit_code=EXIT_USAGE) from exc
    except ValueError as exc:
        # План 19 §5: неразбираемый ``--refresh-scope`` — отказ использования, а не
        # «запрос не совпал»: иначе оператор считает, что обновление запрошено.
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
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
        crawl_policy_digest=policy_resolution.digest,
    )
    # План 19 §6.4: ``crawl run`` больше не хардкодит ``AUTO`` — иначе ``run`` и
    # ``crawl run`` адресовали бы разные сеансы при одном объявленном входе. Политика
    # наблюдения резолвится той же функцией, что у ``run`` (``--refresh`` > ``run.yaml``
    # > ``auto``), и уходит **и** в ``run_id``, **и** в исполнение Ш5.
    try:
        refresh_policy = resolve_refresh_policy(
            cli=getattr(args, "refresh", None), run_config=inputs.run
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    run_id = args.run_id or derive_run_id(
        inputs,
        cycle_id=cycle_id,
        cohort_digest=cohort_d,
        seeds_digest=seeds_d,
        execution_contract_digest=contract_d,
        run_scope=run_scope,
        refresh_policy=refresh_policy.value,
    )
    # ``_resolve_db_path`` требует явный ``--run-id`` при ``--runs-dir``; выведенный
    # идентификатор фиксируем в args, чтобы crawl адресовал тот же прогон, что run.
    args.run_id = run_id
    _set_runs_dir(args)
    db_path = _crawl_db_path(args)
    render_modes = _load_render_modes(getattr(args, "render_modes", None))

    session_id = default_session_id(run_id)
    until = getattr(args, "until", None) or "corpus"
    crawl_only = bool(getattr(args, "crawl_only", False))
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
    # F7 (17 §2): пустой шард — видимый простой, а не тишина.
    notes.extend(shard_notices(getattr(deploy_config.crawl, "shards", None)))

    store = build_store(db_path, profile=args.profile)
    ports: Any = None
    result: Any = None
    cleared = False
    final_status: str | None = None
    final_counts: dict[str, int] = {}
    truncation: dict[str, Any] = {}
    extract_queue: dict[str, int] = {}
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
            # План 19 §5/§6.4: политика наблюдения и политика краула — те же значения,
            # что дали ``run_id``. Иначе ``crawl run`` исполнял бы ``auto`` под чужим
            # идентификатором.
            refresh_policy=refresh_policy.value,
            crawl_policy=policy_resolution,
            # F0: бюджет/окно экстракции — явные входы Ш5 (тот же резолв, что у preflight).
            mem_budget_mb=mem_budget_mb,
            derive_extract_workers=derive_extract_workers,
            # F2 (17 §4): режим «краул без конверсии» — публикация extract_queue.
            crawl_only=crawl_only,
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
            # F2 (17 §4): очередь конверсии — публикуемое число, а не внутренний факт:
            # «краул добыл 900 документов, конверсия не начиналась» обязан быть виден
            # одной командой (``crawl status`` показывает то же самое).
            extract_queue = store.count_extract_queue(session_id)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость best-effort
            notes.append(f"truncation: {type(exc).__name__}: {exc}")
    finally:
        store.close()
        close = getattr(getattr(ports, "crawler", None), "close", None)
        if callable(close):
            close()

    phase_status = result.status if result is not None else "skipped"
    # 17 §6 (F3): публикуемая форма ``errors`` — лексикографический порядок (тот же, что у
    # ``results[].errors``): порядок событий краула зависит от планировщика и не должен
    # попадать в артефакт. Сортировка — **in place** по скопированному списку: значение
    # остаётся прежнего типа (``list[Any]``), поэтому форма payload'а не меняется.
    phase_errors = list(getattr(result, "errors", []) or [])
    phase_errors.sort()
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
        "errors": phase_errors,
        # 17 §7.2/§4: заметки фазы (полосы, браузерная очередь, границы продуктов) —
        # в том же payload, что и счётчики: иначе `crawl run` публикует числа, но не
        # называет, чем они получены.
        "notes": [*notes, *_phase_notes(result)],
        # F2 (17 §4): режим «краул без конверсии» и содержимое durable-очереди.
        # ``crawl_only=false`` в полном крауле — не украшение: по этому полю видно,
        # что корпус собирался, а не только добывался.
        "crawl_only": crawl_only,
        "extract_queue": extract_queue,
    }
    if truncation:
        # N4: усечение видно в crawl/--json payload (не только во внутреннем провенансе).
        payload["crawl_truncation"] = truncation
    truncation_only = bool(truncation.get("truncated")) and not (
        int(final_counts.get(CrawlItemStatus.FAILED.value, 0))
        + int(final_counts.get(CrawlItemStatus.CANCELLED.value, 0))
    )
    if truncation_only:
        # Причина — из провенанса корня (см. `run.py`): политик усечения больше двух.
        warning = (
            "краул усечён политикой "
            f"({truncation.get('truncated_reason') or 'причина не записана'}): "
            "корпус неполон; "
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


def _fmt_metric(value: Any, digits: int = 3) -> str:
    """Число для таблицы метрики; ``None`` → ``—`` (измерять нечего, а не ноль)."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "да" if value else "нет"
    if isinstance(value, int):
        return str(value)
    return f"{float(value):.{digits}f}"


def _print_crawl_metric(metric: dict[str, Any]) -> list[str]:
    """Человекочитаемая форма метрики краула (§7.2): шапка + выровненная таблица по host'ам.

    Таблица выравнивается по ширине host'а: числа только наблюдённые, неизмеренное — ``—``.
    Вежливость печатается отдельной строкой ``politeness:`` с **масштабом** (сколько
    интервалов короче объявленного из скольких) и медианой: минимум без них читается как
    систематический обгон, даже когда коротких интервалов единицы.
    """
    wall = metric.get("crawl_only_wall") or {}
    seconds = wall.get("seconds")
    lines = [
        "crawl_only_wall: "
        + (
            _fmt_metric(seconds) + " s"
            if seconds is not None
            else "— (окна нет: started_at не записан)"
        )
        + (
            " (running — окно не завершено, время прошедшее)"
            if wall.get("running")
            else " (итог: started_at → finished_at)"
        ),
        f"urls: {metric.get('urls', 0)}, hosts: {metric.get('hosts', 0)}",
        "fetch_ms: p50={p50} p95={p95} n={n} unknown={unknown}".format(
            p50=_fmt_metric((metric.get("fetch_ms") or {}).get("p50"), 0),
            p95=_fmt_metric((metric.get("fetch_ms") or {}).get("p95"), 0),
            n=(metric.get("fetch_ms") or {}).get("n", 0),
            unknown=metric.get("fetch_ms_unknown", 0),
        ),
        "requests_by_state: "
        + ", ".join(
            f"{key}={value}" for key, value in sorted(metric.get("requests_by_state", {}).items())
        ),
        "backlog: bytes={bytes} files={files} missing={missing} queue_rows={rows}".format(
            bytes=metric.get("backlog_bytes", 0),
            files=metric.get("backlog_files", 0),
            missing=metric.get("backlog_missing", 0),
            rows=", ".join(
                f"{key}={value}" for key, value in sorted(metric.get("queue_rows", {}).items())
            )
            or "—",
        ),
    ]
    errors = metric.get("errors_by_kind") or {}
    if errors:
        lines.append(
            "errors_by_kind: " + ", ".join(f"{key}={value}" for key, value in sorted(errors.items()))
        )
    rows = metric.get("per_host") or []
    if rows:
        header = (
            f"{'host':<28} {'items':>6} {'requests':>9} {'min_ivl_s':>10} {'med_ivl_s':>10} "
            f"{'declared_s':>11} {'short':>11} {'p50_ms':>7} {'p95_ms':>7} {'unknown':>8}"
        )
        lines.append("  " + header)
        for row in rows:
            short = row.get("intervals_below_declared")
            total = row.get("intervals_total") or 0
            lines.append(
                "  "
                + f"{row['host']:<28} {row['items']:>6} {row['requests']:>9} "
                f"{_fmt_metric(row.get('observed_min_interval_s')):>10} "
                f"{_fmt_metric(row.get('median_interval_s')):>10} "
                f"{_fmt_metric(row.get('declared_min_interval_s')):>11} "
                f"{(f'{short}/{total}' if short is not None else '—'):>11} "
                f"{_fmt_metric((row.get('fetch_ms') or {}).get('p50'), 0):>7} "
                f"{_fmt_metric((row.get('fetch_ms') or {}).get('p95'), 0):>7} "
                f"{row.get('fetch_ms_unknown', 0):>8}"
            )
    else:
        lines.append("hosts: — (элементов нет)")
    for row in rows:
        short = row.get("intervals_below_declared")
        if short is None:
            continue
        declared = _fmt_metric(row.get("declared_min_interval_s"))
        if short:
            # Назван масштаб, а не только минимум: два коротких интервала из 801 и
            # систематический обгон — разные факты, и «НАРУШЕНИЕ» без числа стирало разницу.
            lines.append(
                f"politeness: {row['host']} — {short} из {row.get('intervals_total', 0)} "
                f"интервалов короче объявленного ({_fmt_metric(row.get('share_below_declared_pct'))}%; "
                f"минимум {_fmt_metric(row.get('observed_min_interval_s'))} s, медиана "
                f"{_fmt_metric(row.get('median_interval_s'))} s, объявлено {declared} s)"
            )
        else:
            lines.append(
                f"politeness: {row['host']} — интервалов короче объявленного нет "
                f"(минимум {_fmt_metric(row.get('observed_min_interval_s'))} s, медиана "
                f"{_fmt_metric(row.get('median_interval_s'))} s, объявлено {declared} s)"
            )
    for note in metric.get("notes", ()):
        lines.append(f"note: {note}")
    return lines


def status(args: argparse.Namespace) -> int:
    """``crawl status``: сеанс и счётчики; отсутствие сеанса → ``status: null`` (``EXIT_OK``).

    ``--per-host`` (F8, ``17`` §7.2) печатает **метрику краула** рядом с уже существующим
    ``--per-url``: числа считаются из тех же durable-фактов (:mod:`core.pipeline.crawl_metric`),
    второй носитель не заводится. Флаги независимы и складываются: ``--per-url --per-host``
    даёт и построчный список, и агрегат в одном ответе (элементы читаются один раз).
    """
    db_path = _crawl_db_path(args)
    run_id, session_id = _session_id_for(args)
    per_url = bool(getattr(args, "per_url", False))
    per_host = bool(getattr(args, "per_host", False))
    with build_store(db_path, profile=args.profile, read_only=True) as store:
        session = store.get_crawl_session(session_id)
        counts = store.crawl_session_counts(session_id) if session is not None else {}
        items = (
            list(store.iter_crawl_session_items(session_id))
            if session is not None and (per_url or per_host)
            else []
        )
        payload: dict[str, Any] = {
            "ok": True,
            "run_id": run_id,
            "session_id": session_id,
            "db": str(db_path),
            "counts": counts,
            # F2 (17 §4): очередь конверсии — то же число, что печатает ``crawl run``:
            # «краул добыл N, конверсия взяла 0» видно, не открывая БД.
            "extract_queue": (
                store.count_extract_queue(session_id) if session is not None else {}
            ),
            **_session_fields(session),
        }
        if per_url:
            payload["items"] = [_item_row(item) for item in items]
        if per_host and session is not None:
            # §7.2 п.4: backlog — тот же расчёт, что у backpressure (``ExtractQueue.backlog``),
            # иначе «байты в очереди» существовали бы в двух версиях. Сеанса нет — метрики
            # нет вовсе (печатать нули за несуществующий сеанс значило бы выдумать факт).
            payload["metric"] = build_crawl_metric(
                session=session,
                items=items,
                provenance=read_provenance(session),
                backlog=ExtractQueue(store=store, session_id=session_id).backlog(),
            )
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
            # F2 (17 §4): очередь конверсии — рядом со счётчиками краула.
            queue = payload.get("extract_queue") or {}
            if queue:
                lines.append(
                    "extract_queue: "
                    + ", ".join(f"{key}={queue[key]}" for key in sorted(queue))
                )
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
            # F8 (17 §7.2): метрика краула — до построчного списка: агрегат отвечает на
            # вопрос «быстро ли и вежливо ли», список — на «что именно».
            if "metric" in payload:
                lines.extend(_print_crawl_metric(payload["metric"]))
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


# ---------------------------------------------------------------------------
# policy: операторская политика краула (план 19 §4, STATE 8)
# ---------------------------------------------------------------------------


def _policy_now() -> str:
    """Метка решения оператора в ISO-8601 (UTC, ``Z``) — как остальные TEXT-колонки."""
    return (
        _dt.datetime.now(_dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _policy_package_dir(args: argparse.Namespace) -> str:
    """``--package``: путь как есть либо ``case_id`` под каталогом пакетов."""
    from core.cli.package.commands import _resolve_package_dir

    package = _resolve_package_dir(args)
    if not package or not Path(package).is_dir():
        raise CliError(
            f"пакет не найден: {getattr(args, 'package', None)!r}",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    args.package = package
    return package


def _policy_scope_values(
    args: argparse.Namespace,
) -> tuple[str, CrawlPolicyScopeKind, str | None, str | None]:
    """Разбирает ``--scope/--id`` в ``(product_id, kind, host, path)``.

    ``product`` — ``--id`` product_id; ``host`` — host; ``root``/``path`` — ``host/path``.
    Host нормализуется тем же правилом, что резолвер (``www.`` эквивалентен bare-host):
    иначе запись оператора не совпала бы с корнем, и запрет сети молча не действовал бы.
    """
    raw_scope = str(getattr(args, "scope", None) or CrawlPolicyScopeKind.PRODUCT.value)
    try:
        kind = CrawlPolicyScopeKind(raw_scope)
    except ValueError as exc:
        raise CliError(
            f"--scope: неизвестная область {raw_scope!r}; допустимо "
            f"{', '.join(item.value for item in CrawlPolicyScopeKind)}",
            code="usage",
            exit_code=EXIT_USAGE,
        ) from exc
    ident = str(getattr(args, "id", None) or "").strip()
    if not ident:
        raise CliError(
            "--id обязателен: он называет продукт, host или host/path области",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    if kind is CrawlPolicyScopeKind.PRODUCT:
        return ident, kind, None, None
    if "://" in ident:
        raise CliError(
            f"--id: ожидается host[+path] без схемы URL, получено {ident!r}",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    from core.pipeline.crawl_policy import normalize_policy_host, normalize_policy_path

    host, path = parse_site_key(ident)
    return ident, kind, normalize_policy_host(host), normalize_policy_path(path)


def _policy_scope_label(
    product_id: str, kind: CrawlPolicyScopeKind, host: str | None, path: str | None
) -> str:
    """Человекочитаемая метка области."""
    if kind is CrawlPolicyScopeKind.PRODUCT:
        return f"{product_id} [product]"
    target = host or "—"
    if path:
        target += path
    return f"{product_id} [{kind.value}] {target}"


def _policy_mode_values(args: argparse.Namespace) -> tuple[CrawlPolicyMode, int | None]:
    """``--mode``/``--interval-hours`` с проверкой «интервал только у scheduled»."""
    raw = getattr(args, "mode", None)
    if not raw:
        raise CliError("--mode обязателен для crawl policy set", code="usage", exit_code=EXIT_USAGE)
    try:
        mode = CrawlPolicyMode(raw)
    except ValueError as exc:
        raise CliError(
            f"--mode: неизвестный режим {raw!r}; допустимо "
            f"{', '.join(item.value for item in CrawlPolicyMode)}",
            code="usage",
            exit_code=EXIT_USAGE,
        ) from exc
    raw_interval = getattr(args, "interval_hours", None)
    interval = int(raw_interval) if raw_interval is not None else None
    if mode is CrawlPolicyMode.SCHEDULED:
        if interval is None or interval <= 0:
            raise CliError(
                "--interval-hours обязателен и положителен при --mode scheduled",
                code="usage",
                exit_code=EXIT_USAGE,
            )
    elif interval is not None:
        raise CliError(
            "--interval-hours допустим только при --mode scheduled",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return mode, interval


def _same_policy_scope(
    row: CrawlPolicyRow,
    product_id: str,
    kind: CrawlPolicyScopeKind,
    host: str | None,
    path: str | None,
) -> bool:
    """Строка описывает ровно ту же область (для идемпотентности и замены)."""
    return (
        row.product_id == product_id
        and row.scope_kind is kind
        and (row.host or None) == host
        and (row.path or None) == (path or None)
    )


def _policy_row_payload(row: CrawlPolicyRow) -> dict[str, Any]:
    """JSON-представление действующей записи ``crawl_policy``."""
    return {
        "policy_id": row.policy_id,
        "product_id": row.product_id,
        "scope_kind": row.scope_kind.value,
        "host": row.host,
        "path": row.path,
        "mode": row.mode.value,
        "interval_hours": row.interval_hours,
        "note": row.note,
        "actor": row.actor,
        "created_at": row.created_at,
    }


def policy_set(args: argparse.Namespace) -> int:
    """``hound crawl policy set`` — записать решение оператора (append-only, STATE 8).

    Порядок как у ``hound package profile attest``: lock пакета → открыть ``state.db`` →
    записать. ``--dry-run`` не пишет **ничего** и говорит об этом. Повтор с тем же
    ``(mode, interval_hours)`` для той же области идемпотентен: новая строка не
    появляется, ``created=false``; расхождение ``--note``/``--actor`` лишь предупреждает.
    Смена режима — новая строка + ``superseded_by`` прежней (аудит не теряется).
    """
    package = _policy_package_dir(args)
    actor = str(getattr(args, "actor", None) or "").strip()
    if not actor:
        raise CliError(
            "--actor обязателен: политика — человеческое решение (аудит)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    product_id, kind, host, path = _policy_scope_values(args)
    mode, interval = _policy_mode_values(args)
    note = str(getattr(args, "note", None) or "").strip() or None
    created_at = _policy_now()
    dry_run = bool(getattr(args, "dry_run", False))

    def _body() -> int:
        store = None if dry_run else _open_state_store(args, create=True)
        if not dry_run and store is None:  # pragma: no cover — пакет уже проверен
            raise CliError(
                f"state.db пакета недоступен: {package}", code="state", exit_code=EXIT_USAGE
            )
        warnings: list[str] = []
        created: bool | None = None
        policy_id: int | None = None
        superseded: list[int] = []
        effective_at = created_at
        try:
            if store is not None:
                active = store.list_crawl_policy(product_id=product_id)
            else:
                # ``--dry-run`` всё равно читает: иначе «created» было бы догадкой.
                # Чтение — ``mode=ro``, схема не применяется, файл не меняется.
                paths = getattr(args, "paths", None) or _resolve_paths(args)
                db_path = state_db_path(paths.state, Path(package).name)
                active = [
                    row
                    for row in read_crawl_policy_rows(db_path)
                    if row.product_id == product_id
                ]
            exact = [
                row for row in active if _same_policy_scope(row, product_id, kind, host, path)
            ]
            same = [
                row
                for row in exact
                if row.mode is mode and row.interval_hours == interval
            ]
            if dry_run:
                created = not same
                policy_id = exact[0].policy_id if exact else None
                if same:
                    effective_at = same[0].created_at
                warnings.append("dry-run: строка не записана — решение не действует")
            elif same:
                created = False
                effective_at = same[0].created_at
                policy_id = same[0].policy_id
                if note is not None and note != same[0].note:
                    warnings.append(
                        "no-op: политика уже записана; --note не применялся"
                    )
                if actor != same[0].actor:
                    warnings.append(
                        f"no-op: политика уже записана actor={same[0].actor}; "
                        "--actor не применялся"
                    )
            else:
                assert store is not None  # noqa: S101 — сужение типа: выше проверено
                row = CrawlPolicyRow(
                    product_id=product_id,
                    scope_kind=kind,
                    mode=mode,
                    actor=actor,
                    created_at=created_at,
                    host=host,
                    path=path,
                    interval_hours=interval,
                    note=note,
                )
                policy_id = store.record_crawl_policy(row)
                created = True
                for old in exact:
                    if old.policy_id is not None:
                        store.supersede_crawl_policy(
                            old.policy_id, superseded_by=policy_id
                        )
                        superseded.append(old.policy_id)
            if store is not None:
                active_after = store.list_crawl_policy(product_id=product_id)
                stored = [row for row in active_after if row.policy_id == policy_id]
                if stored and stored[0].created_at != effective_at:
                    effective_at = stored[0].created_at
        finally:
            if store is not None:
                store.close()
        payload = {
            "ok": True,
            "dry_run": dry_run,
            "created": created,
            "policy_id": policy_id,
            "product_id": product_id,
            "scope_kind": kind.value,
            "host": host,
            "path": path,
            "mode": mode.value,
            "interval_hours": interval,
            "note": note,
            "actor": actor,
            "created_at": effective_at,
            "superseded": superseded,
            "warnings": warnings,
        }
        if args.json:
            _print_json(payload)
        else:
            lines = [
                f"dry-run: {'yes' if dry_run else 'no'}",
                f"created: {'yes' if created else 'no'}",
                f"policy_id: {policy_id if policy_id is not None else '—'}",
                f"scope: {_policy_scope_label(product_id, kind, host, path)}",
                f"mode: {mode.value}",
                f"interval_hours: {interval if interval is not None else '—'}",
                f"actor: {actor}",
                f"created_at: {effective_at}",
            ]
            for warning in warnings:
                lines.append(f"warning: {warning}")
            sys.stdout.write("\n".join(lines) + "\n")
        return EXIT_OK

    if dry_run:
        return _body()
    with _package_lock(args, command="crawl policy set"):
        return _body()


def policy_clear(args: argparse.Namespace) -> int:
    """``hound crawl policy clear`` — отозвать решение оператора (append-only).

    ``--scope``/``--id`` ограничивают область; без них отзываются все действующие
    записи пакета. Отзыв — ``superseded_by = policy_id`` (схема §4 не имеет
    tombstone-режима, а удаление нарушило бы append-only), запись остаётся в аудите и
    выпадает из действующих: эффективная политика снова берётся из дефолтов пакета.
    ``--dry-run`` не пишет и называет, сколько строк было бы отозвано.
    """
    package = _policy_package_dir(args)
    actor = str(getattr(args, "actor", None) or "").strip()
    if not actor:
        raise CliError(
            "--actor обязателен: отзыв политики — человеческое решение (аудит)",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    scope = getattr(args, "scope", None)
    target: tuple[str, CrawlPolicyScopeKind, str | None, str | None] | None = None
    if scope is not None or getattr(args, "id", None) is not None:
        target = _policy_scope_values(args)
    dry_run = bool(getattr(args, "dry_run", False))

    def _body() -> int:
        store = None if dry_run else _open_state_store(args, create=True)
        warnings: list[str] = []
        try:
            if store is not None:
                rows = store.list_crawl_policy(
                    product_id=target[0] if target is not None else None
                )
            else:
                paths = getattr(args, "paths", None) or _resolve_paths(args)
                db_path = state_db_path(paths.state, Path(package).name)
                rows = read_crawl_policy_rows(db_path)
                if target is not None:
                    rows = [row for row in rows if row.product_id == target[0]]
            if target is not None:
                product_id, kind, host, path = target
                rows = [
                    row
                    for row in rows
                    if _same_policy_scope(row, product_id, kind, host, path)
                ]
            cleared = [int(row.policy_id) for row in rows if row.policy_id is not None]
            if dry_run:
                warnings.append("dry-run: строки не отозваны — решение продолжает действовать")
            else:
                assert store is not None  # noqa: S101 — сужение типа: выше проверено
                for row in rows:
                    if row.policy_id is not None:
                        store.supersede_crawl_policy(row.policy_id)
        finally:
            if store is not None:
                store.close()
        payload = {
            "ok": True,
            "dry_run": dry_run,
            "clear_all": target is None,
            "scope": (
                _policy_scope_label(*target) if target is not None else None
            ),
            "actor": actor,
            "cleared": cleared,
            "count": len(cleared),
            "warnings": warnings,
        }
        if args.json:
            _print_json(payload)
        else:
            lines = [
                f"dry-run: {'yes' if dry_run else 'no'}",
                f"scope: {payload['scope'] or 'все записи пакета'}",
                f"cleared: {len(cleared)}"
                + (f" ({', '.join(str(item) for item in cleared)})" if cleared else ""),
            ]
            for warning in warnings:
                lines.append(f"warning: {warning}")
            sys.stdout.write("\n".join(lines) + "\n")
        return EXIT_OK

    if dry_run:
        return _body()
    with _package_lock(args, command="crawl policy clear"):
        return _body()


def policy_list(args: argparse.Namespace) -> int:
    """``hound crawl policy list`` — эффективный режим и источник (пакет/оператор).

    Read-only: ``state.db`` читается ``mode=ro`` (как ``plan``), схема не применяется и
    ``schema_version`` не переписывается. Для объявленных корней печатается **решение**
    (для ``scheduled`` — ``refresh``/``reuse``), а несовпавшие записи — отдельным
    списком ``crawl_policy_unmatched``.
    """
    package = _policy_package_dir(args)
    policy_path = Path(package) / CRAWL_POLICY_FILENAME
    config = load_crawl_policy(policy_path) if policy_path.is_file() else CrawlPolicyConfig()
    cohort = load_cohort(Path(package) / "cohort.yaml")
    seed_config, _ = load_effective_seeds(
        package, [entry.product_id for entry in cohort.entries]
    )
    roots = declared_roots(seed_config)
    paths = getattr(args, "paths", None) or _resolve_paths(args)
    db_path = state_db_path(paths.state, Path(package).name)
    rows = read_crawl_policy_rows(db_path)
    observed = observations_by_root(read_crawl_observations(db_path), roots)
    try:
        resolution = resolve_crawl_policy(
            package_policy=config,
            operator_rows=rows,
            roots=roots,
            now=_dt.datetime.now(_dt.UTC),
            observed_at=observed,
        )
    except CrawlPolicyConflict as exc:
        raise CliError(str(exc), code=exc.code, exit_code=EXIT_USAGE) from exc
    payload = {
        "ok": True,
        "package": package,
        "state_db": str(db_path),
        "digest": resolution.digest,
        "decisions": [item.as_dict() for item in resolution.decisions],
        "unmatched": [
            {
                "product_id": item.product_id,
                "scope_kind": item.scope_kind.value,
                "host": item.host,
                "path": item.path,
                "mode": item.mode.value,
                "interval_hours": item.interval_hours,
                "source": item.source,
                "policy_id": item.policy_id,
            }
            for item in resolution.unmatched
        ],
        "records": [_policy_row_payload(row) for row in rows],
    }
    if args.json:
        _print_json(payload)
    else:
        lines = [
            f"package: {package}",
            f"state_db: {db_path}",
            f"crawl_policy_digest: {resolution.digest}",
            f"decisions: {len(resolution.decisions)}",
        ]
        for decision in resolution.decisions:
            lines.append(
                "  "
                + _policy_scope_label(
                    decision.product_id,
                    decision.scope_kind,
                    decision.host,
                    decision.path,
                )
                + f" mode={decision.mode.value} decision={decision.decision} "
                f"source={decision.source}"
            )
        lines.append(f"operator records: {len(rows)}")
        for row in rows:
            lines.append(
                "  "
                + _policy_scope_label(row.product_id, row.scope_kind, row.host, row.path)
                + f" mode={row.mode.value} policy_id={row.policy_id} actor={row.actor}"
            )
        lines.append(f"crawl_policy_unmatched: {len(resolution.unmatched)}")
        for item in resolution.unmatched:
            lines.append(
                "  "
                + _policy_scope_label(
                    item.product_id, item.scope_kind, item.host, item.path
                )
                + f" mode={item.mode.value} source={item.source}"
            )
        sys.stdout.write("\n".join(lines) + "\n")
    return EXIT_OK


def _no_policy_subcommand(args: argparse.Namespace) -> int:
    """``hound crawl policy`` без подкоманды: явный отказ использования."""
    raise CliError(
        "не указана подкоманда crawl policy (set|list|clear)",
        code="usage",
        exit_code=EXIT_USAGE,
    )


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


def _add_crawl_only_flag(parser: argparse.ArgumentParser) -> None:
    """F2 (17 §4): ``--crawl-only`` — краул без конверсии, с публикацией ``extract_queue``.

    Объявлен только у ``crawl run|resume|restart``: полному ``run`` этот режим
    противопоказан (он печатал бы корпус без документов), и отсутствие флага там —
    проверяемое свойство CLI, а не соглашение.
    """
    parser.add_argument(
        "--crawl-only",
        dest="crawl_only",
        action="store_true",
        help="краул без конверсии: frontier+фетч+retention+FETCHED, документы — в "
        "durable-очередь extract_queue (корпус не собирается; допустимо только при "
        "--until не позже corpus)",
    )


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


def _add_policy_target_flags(
    parser: argparse.ArgumentParser,
    *,
    require_scope: bool = True,
    actor: bool = True,
) -> None:
    """Флаги области политики: ``--package``, ``--scope``, ``--id``, ``--actor``."""
    parser.add_argument(
        "--package",
        required=True,
        help="пакет: путь или case_id (резолвится в каталоге пакетов)",
    )
    parser.add_argument(
        "--dir",
        default=None,
        help="каталог пакетов для резолва case_id (дефолт — resolved paths.packages)",
    )
    parser.add_argument(
        "--scope",
        default=None,
        choices=[item.value for item in CrawlPolicyScopeKind],
        help="область записи: product | host | root | path",
    )
    parser.add_argument(
        "--id",
        default=None,
        help="product_id (scope=product), host (scope=host) или host/path (root/path)",
    )
    if actor:
        parser.add_argument(
            "--actor", default=None, help="человек, принявший решение (аудит)"
        )
    if require_scope:
        return
    # ``list``/``clear`` без области законны: list — все корни, clear — все записи.


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
    _add_crawl_only_flag(p_run)
    _add_common(p_run)
    _add_llm_slot_flag(p_run)
    _add_llm_model_flag(p_run)
    p_run.set_defaults(handler=run)

    p_status = crawl.add_parser("status", help="статус сеанса и счётчики (read-only)")
    _add_session_target_flags(p_status, require_run_id=True)
    p_status.add_argument(
        "--per-url", dest="per_url", action="store_true", help="построчно: элементы сеанса"
    )
    p_status.add_argument(
        "--per-host",
        dest="per_host",
        action="store_true",
        help="метрика краула (17 §7.2): host'ы, observed_min_interval_s и отношение к "
        "объявленному минимуму, fetch_ms p50/p95, backlog_bytes; складывается с --per-url",
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
    _add_crawl_only_flag(p_resume)
    _add_common(p_resume)
    _add_llm_slot_flag(p_resume)
    _add_llm_model_flag(p_resume)
    p_resume.set_defaults(handler=resume)

    p_restart = crawl.add_parser("restart", help="очистить сеанс и запустить заново")
    _add_run_target_flags(p_restart)
    _add_source_flags(p_restart)
    _add_phase4_flags(p_restart)
    _add_crawl_only_flag(p_restart)
    _add_common(p_restart)
    _add_llm_slot_flag(p_restart)
    _add_llm_model_flag(p_restart)
    p_restart.set_defaults(handler=restart)

    p_cancel = crawl.add_parser("cancel", help="терминально снять сеанс (interrupted|cancelled)")
    _add_session_target_flags(p_cancel, require_run_id=True)
    _add_common(p_cancel)
    p_cancel.set_defaults(handler=cancel)

    # План 19 §P1: операторская политика краула (state.db, append-only). Только
    # запись/чтение решений — исполнение политики (network_mode/corpus_source) отдельный
    # слой; здесь ни один сетевой механизм не включается.
    p_policy = crawl.add_parser(
        "policy", help="политика краула по областям (state.db; set|list|clear)"
    )
    policy = p_policy.add_subparsers(dest="policy_command", metavar="ДЕЙСТВИЕ")

    p_pol_set = policy.add_parser("set", help="записать режим области (append-only)")
    _add_policy_target_flags(p_pol_set)
    p_pol_set.add_argument(
        "--mode",
        required=True,
        choices=[item.value for item in CrawlPolicyMode],
        help="режим области: auto | on_request | scheduled | network_denied",
    )
    p_pol_set.add_argument(
        "--interval-hours",
        dest="interval_hours",
        type=int,
        default=None,
        help="интервал обновления (обязателен и > 0 при --mode scheduled)",
    )
    p_pol_set.add_argument("--note", default=None, help="человеческая заметка (провенанс)")
    p_pol_set.add_argument(
        "--dry-run", action="store_true", help="показать решение без записи"
    )
    _add_common(p_pol_set)
    p_pol_set.set_defaults(handler=policy_set)

    p_pol_list = policy.add_parser(
        "list", help="эффективный режим и источник по объявленным корням (read-only)"
    )
    _add_policy_target_flags(p_pol_list, require_scope=False, actor=False)
    _add_common(p_pol_list)
    p_pol_list.set_defaults(handler=policy_list)

    p_pol_clear = policy.add_parser(
        "clear", help="отозвать записи оператора (append-only, без удаления)"
    )
    _add_policy_target_flags(p_pol_clear, require_scope=False)
    p_pol_clear.add_argument(
        "--dry-run", action="store_true", help="показать, что было бы отозвано, без записи"
    )
    _add_common(p_pol_clear)
    p_pol_clear.set_defaults(handler=policy_clear)

    p_policy.set_defaults(handler=_no_policy_subcommand)

    p_crawl.set_defaults(handler=_no_subcommand)
