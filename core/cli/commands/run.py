"""``run``: собрать прогон и провести ``Runner`` по реальным фазам Ш1–Ш9.

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from core.adapters.index_npy import numpy_available
from core.adapters.lexical_tantivy import tantivy_available
from core.cli.context import (
    _acquire_package_lock,
    _add_common,
    _add_llm_model_flag,
    _add_phase4_flags,
    _concise,
    _crawl_policy_resolution,
    _ensure_run,
    _follow_session,
    _gate_runtime,
    _load_corpus_urls,
    _load_render_modes,
    _only_failed_work_set,
    _open_state_store,
    _package_lock_handle,
    _persist_llm_scenarios,
    _phases_until,
    _positive_int,
    _print_json,
    _print_run_dry,
    _print_run_report,
    _product_callback,
    _progress_callback,
    _resolve_phases,
    _resolve_plan,
    _resolve_ports,
    _run_report_payload,
    _session_snapshot,
    _snapshot_run_inputs,
    add_freshness_warn_days_flag,
    resolve_content_cache,
)
from core.cli.errors import (
    EXIT_BLOCKED,
    EXIT_ERROR,
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
    SPLIT_PROFILE,
    SearchResolutionError,
    WiringError,
    build_runner,
    build_store,
    build_vector_index,
    llm_env_notices,
    llm_host_notices,
    manifest_deploy_fields,
    preflight_extraction_budget,
    resolve_and_build_search_adapter,
    resolve_extraction_budget,
    run_db_path,
    seed_lexical_index,
    serp_env_notices,
)
from core.config.deploy import (
    CrawlConfig,
    is_split,
    load_deploy_config,
    profile_notices,
    shard_notices,
)
from core.domain import (
    CrawlItemStatus,
    CrawlRunConfig,
    CrawlSessionStatus,
    ProductScope,
    RefreshPolicy,
)
from core.pipeline import GATE_PHASES, PHASES, Ports
from core.pipeline.compute import bundle_for_run
from core.pipeline.corpus_freshness import (
    FRESHNESS_WARN_DAYS_DEFAULT,
    document_observation_freshness,
)
from core.pipeline.crawl_frontier import resolve_pdf_device
from core.pipeline.crawl_policy import CrawlPolicyConflict
from core.pipeline.crawl_session import (
    default_session_id,
    read_provenance,
    truncation_summary,
)
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.lint import lint_run_inputs
from core.pipeline.liveness import stale_reason
from core.pipeline.normalize import load_inputs
from core.pipeline.observability import (
    collect_llm_usage,
    instrument_phases,
    write_run_metrics,
)
from core.pipeline.onboarding import effective_seeds, load_effective_seeds, preflight
from core.pipeline.phases import resolve_measured_retrieval, resolve_package_id
from core.pipeline.plan import derive_run_id
from core.pipeline.report import Artifacts
from core.pipeline.resources import (
    ResourcesObservation,
    build_resources_block,
    mem_budget_provenance,
)
from core.pipeline.retrieve import RetrievalProfile
from core.pipeline.run_identity import (
    RunIdentity,
    build_execution_contract,
    build_run_identity,
    check_update_scope,
    previous_run_of,
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


def _state_not_adopted_notice(
    args: argparse.Namespace, package_id: str, previous_lineage: Any
) -> str | None:
    """Предупреждение ``state_not_adopted``: state пуст, а завершённые прогоны есть (§4.6).

    Без adoption первый ``run --update`` видит у всех продуктов ``last_corpus_run_id =
    NULL`` и делает их ``new`` — полный краул когорты, хотя корпус уже собран. Полный краул
    не запрещается (это валидный режим), но обязан быть виден: иначе «переиспользования
    нет» выглядит как свойство продукта, а не как незаполненное состояние.

    Сканирование ``runs/`` выполняется **только** когда durable-состояние пусто (нет ни
    одной строки lineage): после первого прогона или adoption условие ложно и лишних
    чтений не происходит. Сбой сканирования предупреждение не роняет (оно наблюдательное).
    """
    if previous_lineage is not None:
        return None
    try:
        from core.pipeline.state_adopt import completed_runs

        runs = completed_runs(getattr(args, "runs_dir", "runs"), package_id)
    except Exception:  # noqa: BLE001 — предупреждение не вправе ронять прогон
        return None
    if not runs:
        return None
    names = ", ".join(item.run_id for item in runs[:3])
    if len(runs) > 3:
        names += ", …"
    return (
        "state_not_adopted: durable состояние пакета пусто, а завершённые прогоны есть "
        f"({names}); продукты получат mode=new и полный краул. Готово к запуску: "
        "hound package state adopt --package "
        f"{getattr(args, 'package', package_id)} --actor <человек>"
    )


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


def _reconcile_declared_retirements(state_store: Any, seed_config: Any) -> None:
    """Отражает авторский ``state: retired`` в durable ``product_state`` (09 §5.1, T15).

    ``sources/seeds.yaml`` — авторский источник: продукт, объявленный ``retired``, обязан
    стать ``retired`` и в durable-состоянии, иначе переход «выведен → возвращён» не
    наблюдаем, а ``re_added_at`` (единственный вход режима ``new`` в ``_mode_for``)
    никогда не выставляется.

    Пустой набор передаётся **только** при объявленном ``retired``: продукт, у которого
    просто нет активных seed'ов (``pending_seeds``/``blocked``), выведенным из работы не
    считается — это разные решения человека. Продукт, отсутствующий в ``seeds.yaml``
    вовсе, здесь не обрабатывается: у прогона нет о нём объявления (состав когорты —
    ``run.yaml``), и выводить решение из молчания файла значило бы его выдумать.

    Вызывается **до** проверки блокировки: заблокированный прогон — ровно тот, где
    продукт выведен, и откладывать запись до «разблокируется» значило бы не записать
    никогда. ``state_store`` открыт с ``create=False``, поэтому §4.1 («заблокированный
    прогон state не создаёт») не нарушается.
    """
    from core.domain.enums import OnboardingState

    products = getattr(seed_config, "products", None) or {}
    for product_id, product in products.items():
        if getattr(product, "state", None) is not OnboardingState.RETIRED:
            continue
        # Объявленный ``retired`` при **живых** seed'ах — противоречие в данных (O1:
        # готовность считается по активным seed'ам, а не по полю ``state``). Здесь не
        # действуем: записать ``retired`` значило бы на следующем прогоне получить
        # ``if rows:`` → ``reconcile_seed_set(pid, rows)`` → ``re_added_at`` → снова
        # ``new``, то есть мигание режима на одном и том же файле. Расхождение остаётся
        # видимым (план покажет ``onboarding_state: ready``), а решение — за автором:
        # убрать seed'ы или снять ``retired``.
        if any(seed.is_enabled for seed in getattr(product, "seeds", ()) or ()):
            continue
        state_store.reconcile_seed_set(product_id, [])

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


def _open_previous_store(
    runs_dir: str | None, previous_run_id: str | None
) -> tuple[Any, str | None]:
    """Открывает БД прогона-предшественника **только на чтение** (09 §10.2).

    Возвращает ``(store | None, причина | None)``: причина — машинно-читаемый текст, почему
    перенос ячеек не сможет читать предшественника. ``None`` в обоих полях — предшественника
    нет (первый прогон/resume текущего): это **не** дефект и не требует предупреждения.

    Почему это делает composition root, а не фаза: путь ``runs/{run_id}/cohound.db`` —
    свойство раскладки, а ``core/pipeline`` раскладку не читает. И почему ``read_only``:
    БД завершённого прогона — иммутабельное свидетельство; писатель применил бы к ней
    DDL/миграции и изменил то, против чего считается diff.

    Нечитаемая БД (схема старше текущей — гейт ``connect_read_only``) — **видимая** причина,
    а не тихий пропуск: перенос выключится, и это попадёт в предупреждения прогона.
    """
    if not previous_run_id:
        return None, None
    from core.store.sqlite_store import SqliteStore

    base = runs_dir or "runs"
    db_path = Path(base) / previous_run_id / "cohound.db"
    if not db_path.exists():
        return None, (
            f"carry: БД предшественника {previous_run_id} не найдена ({db_path}) — "
            "ячейки будут измерены заново"
        )
    try:
        return SqliteStore(db_path, read_only=True), None
    except Exception as exc:  # noqa: BLE001 — перенос не роняет прогон, но видим
        return None, (
            f"carry: БД предшественника {previous_run_id} не открыта: "
            f"{type(exc).__name__}: {_concise(exc)} — ячейки будут измерены заново"
        )


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


class _ConsoleTee:
    """Поток, дублирующий запись в несколько приёмников (консоль прогона → файл).

    Нужен, чтобы убитый прогон оставлял улику. Без него выключение хоста, OOM-killer или
    ``SIGKILL`` неотличимы от зависания: в ``logs/`` остаются только пофазные ``jsonl``,
    а вопрос «на чём он стоял» решается реконструкцией по косвенным записям.

    Вторичный приёмник **не** ломает вывод: консоль первична, лог — наблюдаемость.
    Ошибка записи в файл (диск кончился, права) не должна ни падать, ни менять
    поведение прогона, поэтому глушится здесь, а не у вызывающего.

    ``__getattr__`` делегирует **первичному** потоку: ``tqdm``/``docling`` спрашивают
    ``isatty``/``fileno``/``encoding`` у ``stderr``, и отвечать за них должен терминал,
    а не файл — иначе прогресс-бар перестал бы определять терминал и вывод изменился бы.
    """

    __slots__ = ("_streams",)

    def __init__(self, *streams: Any) -> None:
        self._streams = streams

    def write(self, data: str) -> int:
        for stream in self._streams:
            # Тишина верна и обязательна: консоль первична, файл-приёмник вторичен, и сбой
            # записи в него (диск кончился, канал закрыт) не должен ни падать, ни менять
            # вывод прогона (см. докстринг класса). ``suppress`` — форма подавления:
            # ``Exception`` не сужается, потому что писать может любой объект-поток.
            with contextlib.suppress(Exception):
                stream.write(data)
        return len(data)

    def flush(self) -> None:
        for stream in self._streams:
            # См. ``write``: сбой вторичного приёмника не ломает восстановление консоли.
            with contextlib.suppress(Exception):
                stream.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._streams[0], name)


def _install_console_log(db_path: Path) -> tuple[Any, Any, Any] | None:
    """Дублирует stdout/stderr прогона в ``runs/{run_id}/logs/console.log``.

    Файл открывается в режиме дозаписи и **построчно** (``buffering=1``): построчная
    запись переживает падение процесса, а буферизованная — нет, а «последняя строка»
    как раз и есть ответ на вопрос «где он был».

    :returns: маркер для :func:`_restore_console` либо ``None`` — лог не включён
        (каталог недоступен для записи). Отсутствие лога не является ошибкой прогона.
    """
    path = db_path.parent / "logs" / "console.log"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a", encoding="utf-8", errors="replace", buffering=1)
        handle.write(f"\n=== запуск {_utc_now()} (pid={os.getpid()}) ===\n")
    except OSError:
        return None
    out, err = sys.stdout, sys.stderr
    sys.stdout = _ConsoleTee(out, handle)
    sys.stderr = _ConsoleTee(err, handle)
    return (out, err, handle)


def _restore_console(marker: tuple[Any, Any, Any] | None) -> None:
    """Возвращает исходные потоки и закрывает файл (вызывается из ``finally``)."""
    if marker is None:
        return
    out, err, handle = marker
    for stream in (sys.stdout, sys.stderr):
        # Восстановление не вправе падать: потоки уже могли быть закрыты (см. write).
        with contextlib.suppress(Exception):
            stream.flush()
    sys.stdout, sys.stderr = out, err
    with contextlib.suppress(OSError):  # pragma: no cover — закрытие уже закрытого
        handle.close()


def _append_console(marker: tuple[Any, Any, Any] | None, text: str) -> None:
    """Дописывает строку **только** в файл: консоль её напечатает вызывающий.

    Нужна отдельной функцией, потому что отказ уходит в ``core/cli/main.py``: он печатает
    сообщение **после** того, как :func:`run` вернул управление. Без этой записи причина
    отказа не попала бы в файл — а именно её и читают, когда прогон не оставил другой
    улики. Печать через ``print`` дала бы дубль в консоли, поэтому пишем прямо в дескриптор.
    """
    if marker is None:
        return
    # См. ``_ConsoleTee.write``: без этой записи причина отказа не попала бы в файл,
    # но её недоступность не вправе ломать сам вывод ошибки.
    with contextlib.suppress(Exception):
        marker[2].write(text.rstrip("\n") + "\n")


def run(args: argparse.Namespace) -> int:
    """``run``: держать файловый lock пакета и провести ``Runner`` по фазам Ш1–Ш9.

    Файловый lock пакета (09 §4.4) берётся на время **реального** прогона: второй ``run``
    на тот же пакет получает ``EXIT_USAGE`` с именем держателя, а не делит с ним
    ``state.db``/``corpus_raw/``/``index_store/``. Захват отложен до момента, когда прогон
    действительно занял пакет (см. :func:`_run_body`): ``--dry-run`` ничего не пишет, а
    заблокированный онбордингом прогон не создаёт state, поэтому файла лока в этих случаях
    тоже не появляется. Освобождение — здесь, в ``finally``: пакет занят до конца команды.
    """
    if bool(getattr(args, "dry_run", False)):
        return _run_body(args)
    lock = _package_lock_handle(args, command="run")
    try:
        return _run_body(args, package_lock=lock)
    finally:
        if lock is not None:
            lock.release()


def _run_body(args: argparse.Namespace, *, package_lock: Any = None) -> int:
    """``run`` без захвата файлового лока пакета (захват — внутри, 09 §4.4).

    Composition root строит план и реестр фаз (:func:`build_phases`) и связывает их с
    портами профиля (:func:`_resolve_ports`). Гейты намеренно **не** автоматизируются
    (I6/I7): без решения в ``gate_decisions`` (G1/G2 — внутренняя аттестация, G3/G4 —
    ответ внешней системы) прогон останавливается ``blocked`` на соответствующей фазе, а
    для G3/G4 пишет запрос на решение в ``runs/{run_id}/gates/``.
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
        # 09 §6.2: ``--update`` + ``--only-failed`` — неоднозначный work-set. Формула одна
        # на ``run`` и ``plan`` (``resolve_identity``-резолверы): план обязан повторить и
        # отказ, иначе напечатал бы ``run_id`` прогона, который не стартует.
        check_update_scope(
            update=bool(getattr(args, "update", False)), run_scope=run_scope
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc

    # Предыдущий lineage читается ДО вывода run_id (нужен предыдущий cycle_id для
    # проверки --update) и ДО записи текущей строки (иначе она сослалась бы на себя).
    # ``create=False``: dry-run/ошибочный вызов не создаёт state.db.
    state_store: Any = _open_state_store(args, create=False)
    previous_lineage = (
        state_store.previous_lineage(package_id) if state_store is not None else None
    )
    # 09 §4.6: пустое состояние при наличии завершённых прогонов означает полный краул
    # всей когорты (все продукты ``new``). Молчаливый полный краул допустим, но обязан
    # быть виден: без adoption первый ``run --update`` не переиспользует ни один документ.
    # Предупреждение адресовано именно ``--update`` (§4.6): у обычного прогона ожидания
    # переиспользования нет, и шум в каждом отчёте обесценил бы сигнал.
    state_not_adopted = (
        _state_not_adopted_notice(args, package_id, previous_lineage)
        if bool(getattr(args, "update", False))
        else None
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
    # R7 (07 §7) + §13 (методология 2.4.0): деплой-конфиг и **декларированные** оси нужны
    # ДО контракта — они входят в него (deploy-профиль, версия seam-схемы, оси краула,
    # стек извлечения). Один резолвер на контракт и манифест: второй формулы нет.
    # P5/P7: операторские оси краула поднимаются в run-слой **до** контракта: они меняют
    # механизм извлечения, то есть текст корпуса, и обязаны попасть в ``run_id``.
    deploy_config = load_deploy_config(None)
    try:
        declared = resolve_declared_axes(
            args=args, inputs=inputs, deploy=deploy_config
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    # План 19 §6.1: эффективная политика краула входит в ``run_id`` тем же резолвом, что
    # у ``plan``/``crawl``. P1 её только разрешает: исполнение (network_mode/corpus_source)
    # — отдельный слой, поэтому здесь нет ни одного сетевого решения по политике.
    # ``crawl_config_digest`` берётся у уже разрешённых осей (то же значение, что входит в
    # контракт): лимиты определяют удостоверяемое пространство снятия (I2d), и вторая
    # формула разошлась бы с контрактом.
    try:
        policy_resolution = _crawl_policy_resolution(
            args,
            inputs,
            seed_config,
            crawl_config_digest=declared.crawl_config_digest,
        )
    except CrawlPolicyConflict as exc:
        raise CliError(str(exc), code=exc.code, exit_code=EXIT_USAGE) from exc
    except ValueError as exc:
        # План 19 §5: неразбираемый ``--refresh-scope`` — отказ использования, а не
        # «запрос не совпал»: иначе оператор считает, что обновление запрошено.
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    # §7.4/I21: env COHOUND_LLM_MODEL/_DECODER оценщиком не читаются, а endpoint из env
    # вообще вне execution contract. Оба расхождения — видимые (payload + logs/wiring.jsonl),
    # не молчаливые: эти переменные живут в runbook'ах операторов.
    # D-8: профиль ``split-hardened`` тоже даёт видимое предупреждение — он принимается,
    # но ничего не добавляет сверх ``split`` (ложный «хардненинг» молчать не должен).
    llm_env_texts = llm_env_notices(
        deploy_llm=getattr(deploy_config, "llm", None),
        slot=getattr(args, "llm_slot", None),
        run_slot=getattr(getattr(inputs, "run", None), "llm_slot", None),
    )
    # §10: хосты endpoint'а — объявленное исключение стенда RnD. Текст env-расхождения
    # переиспользуется из ``llm_env_notices`` (см. ``llm_host_notices``), поэтому здесь
    # берутся только те замечания, которых в списке ещё нет: дубль обесценивает сигнал.
    host_notices = llm_host_notices(
        deploy_llm=getattr(deploy_config, "llm", None),
        hosts=[
            str(row.get("endpoint_host"))
            for row in (getattr(declared, "llm_scenarios", ()) or ())
            if row.get("endpoint_host")
        ],
        slot=getattr(args, "llm_slot", None),
        run_slot=getattr(getattr(inputs, "run", None), "llm_slot", None),
    )
    config_notices = [
        *(
            note["message"]
            for note in host_notices
            if note.get("message") not in llm_env_texts
        ),
        *llm_env_texts,
        *profile_notices(deploy_config),
        # F7 (17 §2): пустой шард виден до старта: «узел объявлен, host'ов ноль» —
        # объявленный простой, а не тишина (видно и в `plan`, и в `run --json`).
        *shard_notices(getattr(deploy_config.crawl, "shards", None)),
        # I3/§5: env-троттл SERP-контура виден, как и LLM-расхождения: он меняет корпус,
        # а значит и ``run_id`` — молчаливая подмена здесь была бы подменой измерения.
        *serp_env_notices(
            deploy_search=getattr(deploy_config, "search", None),
            run_rounds=getattr(
                getattr(getattr(inputs, "run", None), "serp", None), "rounds", None
            ),
            cli_rounds=getattr(args, "serp_rounds", None),
        ),
    ]
    if state_not_adopted is not None:
        config_notices.append(state_not_adopted)
    # План 19 §4: несовпавшая запись видима и НЕ входит в дайджест. Молчать о ней нельзя:
    # оператор считает, что запретил область, а запись не совпала ни с одним корнем.
    config_notices.extend(
        "crawl_policy_unmatched: "
        f"{item.product_id} [{item.scope_kind.value}] "
        f"host={item.host or '—'} path={item.path or '—'} mode={item.mode.value} "
        f"source={item.source}"
        for item in policy_resolution.unmatched
    )
    # План 19 §5: запрос обновления, не совпавший ни с одним объявленным корнем, виден:
    # иначе оператор считает, что обновил область, а прогон её не тронул.
    config_notices.extend(
        f"crawl_policy_unmatched_requested: {item.raw} — запрос не совпал ни с одним "
        "объявленным корнем"
        for item in policy_resolution.unmatched_requested
    )
    status_dict_version = declared_status_dict_version()
    contract_d = build_execution_contract(
        inputs,
        cohort_digest=cohort_d,
        seeds_digest=seeds_d,
        declared=declared,
        retrieval_profile=getattr(args, "retrieval_profile", None),
        cutoff_K=getattr(args, "cutoff_k", None),
        status_dict_version=status_dict_version,
        crawl_policy_digest=policy_resolution.digest,
        # План 19 §6.3: ось ``frozen_source`` — объявленный вход, канонизируемый в ``n/a``
        # при политике, отличной от ``frozen``.
        frozen_source=getattr(args, "frozen_source", None),
        refresh_policy=refresh_policy,
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
    db_path = Path(args.db) if args.db is not None else run_db_path(args.runs_dir, run_id)

    # E1/A3 (R7/§13): измеренная фиксация ретривала усыновляется в аргументы **до**
    # финального контракта. Прежний порядок (контракт раньше усыновления) давал на resume
    # контракт с ``None`` при манифесте/ячейках с измеренным ``R2`` — ложную границу §13.
    # Явный CLI-профиль/``--cutoff-k`` приоритетнее фиксации; явный **противоречащий**
    # профиль отвергается (A3: изменить измеренное можно только пере-измерением).
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
            # Контракт пересчитывается с усыновлёнными значениями. ``run_id`` остаётся
            # идентичностью **создания** прогона: иначе resume после измерения не нашёл бы
            # свою строку в БД (адресация идёт по выведенному до усыновления id).
            contract_d = build_execution_contract(
                inputs,
                cohort_digest=cohort_d,
                seeds_digest=seeds_d,
                declared=declared,
                retrieval_profile=args.retrieval_profile,
                cutoff_K=args.cutoff_k,
                status_dict_version=status_dict_version,
                crawl_policy_digest=policy_resolution.digest,
                frozen_source=getattr(args, "frozen_source", None),
                refresh_policy=refresh_policy,
            )

    # 09 §12.3: предшественник — по **одной** формуле (:func:`previous_run_of`): явный
    # ``--since-run`` (нет строки — отказ использования, а не «истории нет»), иначе
    # последняя строка lineage пакета; совпадение с текущим ``run_id`` — не предшественник
    # (resume не делает carry-forward против себя).
    try:
        previous = previous_run_of(
            state_store,
            package_id,
            run_id=run_id,
            explicit=getattr(args, "since_run", None),
        )
    except ValueError as exc:
        raise CliError(str(exc), code="usage", exit_code=EXIT_USAGE) from exc
    previous_run_id = previous.run_id if previous is not None else None
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

    # План строится ОДИН раз здесь и переиспользуется фазами/префлайтом: повторный
    # ``plan()`` дал бы иной ``run_id``/двойной линт (см. core/pipeline/phases.py).
    run_plan = _resolve_plan(args, inputs, run_id)
    # Валидация per-URL режимов рендера (Ш5) — до dry-run, чтобы ошибка входа была
    # видна и в сухом прогоне (I19: никакого молчаливого фетча/подмены).
    render_modes = _load_render_modes(getattr(args, "render_modes", None))

    # Консоль прогона дублируется в ``runs/{run_id}/logs/console.log`` (см.
    # :func:`_install_console_log`). ``--dry-run`` ничего не пишет по контракту, поэтому
    # лог для него не открывается. Маркер снимается в ``finally`` ниже.
    console = None if args.dry_run else _install_console_log(db_path)

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
                    warnings=[*report.warnings, *onboarding_warnings, *config_notices],
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
                "warnings": [*report.warnings, *onboarding_warnings, *config_notices],
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

        # 09 §5.1/§15.1 (T15): retirement — авторское решение в ``sources/seeds.yaml``
        # (``state: retired``), отдельной команды у него нет. Отражаем его в durable
        # ``product_state`` **до** выхода на блокировку: набор «seed'ы убраны» до сверки
        # не доходит (``if rows:`` ниже), поэтому без этого переход ``retired → возврат``
        # не наблюдаем, и ``re_added_at`` остаётся без производителя — то есть ветка
        # ``_mode_for → new`` недостижима. ``state_store`` здесь открыт с ``create=False``,
        # поэтому заблокированный прогон по-прежнему **не создаёт** ``state.db`` (§4.1):
        # нет состояния — нечего отражать, и ничего не появляется.
        if state_store is not None:
            _reconcile_declared_retirements(state_store, seed_config)

        # Заблокированный продукт: пишем снимок входов (прогон виден), но не собираем
        # корпус/ячейки. ``onboarding`` — человеческая предпосылка, а не сбой кода.
        if onboarding is not None and onboarding.blocked:
            blocked_store = build_store(db_path, profile=args.profile)
            scenario_errors: list[str] = []
            try:
                _ensure_run(blocked_store, run_id, inputs)
                # Замечания снимка входов не выбрасываются: заблокированный прогон —
                # ровно тот случай, когда оператор читает отчёт о том, чего не хватило
                # (``run_inputs`` — источник истины плана), и молча потерять их значило бы
                # оставить его без причины блокировки.
                scenario_errors.extend(
                    _snapshot_run_inputs(
                        args,
                        run_id,
                        inputs=inputs,
                        run_plan=run_plan,
                        store=blocked_store,
                        # v2 снимка (09 §14.2): цепочка циклов — та же идентичность, что
                        # уже вошла в ``run_id``; план продуктов — то же значение, что
                        # уходит в ``runs.product_plan_json``.
                        cycle_id=cycle_id,
                        cohort_digest=cohort_d,
                        seeds_digest=seeds_d,
                        previous_run_id=previous_run_id,
                        product_plan=onboarding.plan_set.model_dump(mode="json"),
                    )
                )
                scenario_errors.extend(
                    _persist_llm_scenarios(blocked_store, run_id, declared)
                )
            finally:
                blocked_store.close()
            payload = _blocked_payload(
                run_id,
                db_path,
                onboarding,
                warnings=[*report.warnings, *onboarding_warnings, *scenario_errors, *config_notices],
            )
            payload.update(_identity_payload(identity))
            if args.json:
                _print_json(payload)
            else:
                _print_run_report(payload)
            return EXIT_BLOCKED

        # 09 §4.4: пакет занимается **здесь**, а не на входе команды. Заблокированный
        # онбордингом прогон state не создаёт (контракт §4.1) — значит и файла лока не
        # создаёт; ``--dry-run`` вернулся выше. Прогон, который останавливается **до**
        # онбординга (``--until plan``), пакет тоже не занимает: он не открывает
        # ``state.db`` и не краулит, то есть нечего сериализовать. С этого места пакет
        # занят до конца команды (освобождение — в :func:`run`).
        if _onboarding_in_scope(args.until):
            _acquire_package_lock(package_lock)
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

        # R7 (07 §7): деплой-провенанс манифеста прогона. Конфиг уже загружен выше (он
        # нужен контракту — declared-оси); здесь только split-валидация шва и поля отчёта.
        # Сам деплой-провенанс манифеста течёт через ``_resolve_phases(deploy_config=…)`` →
        # фазу Ш9 (run_manifest.yaml и строка ``runs``); здесь те же поля кладутся в отчёт
        # прогона (``deploy``) для наблюдаемости CLI. Версии edge-компонентов
        # (``/v1/capabilities``) cmd_run не собирает — пусто, если не известны.
        if args.profile == SPLIT_PROFILE and not is_split(deploy_config):
            raise WiringError(
                "профиль split требует deploy-конфиг с profile split/split-hardened "
                "(07 §6/§7)"
            )
        deploy_fields: dict[str, Any] = manifest_deploy_fields(deploy_config)

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
        pdf_pipeline = getattr(args, "pdf_pipeline", None)
        follow = bool(getattr(args, "follow", False))
        interval = float(getattr(args, "interval", 1.0) or 0.0)
        session_id = default_session_id(run_id)
        filter_notes: list[str] = []
        if onboarding is not None:
            corpus_urls = dict(onboarding.effective_urls)
        else:
            corpus_urls = _load_corpus_urls(getattr(args, "urls", None))
        # F0/§3.3: бюджет памяти и решение «окно экстракции из бюджета» резолвятся
        # **здесь**, в composition root, один раз на прогон. Пайплайн получит их явными
        # параметрами (он не читает окружение: иначе расписание стало бы функцией хоста).
        extraction_profile = (
            deploy_config.profile if deploy_config is not None else LOCAL_PROFILE
        )
        mem_budget_mb, derive_extract_workers = resolve_extraction_budget(
            extraction_profile,
            # Тестовый шов (``args.ports``): стек экстракции не тот, чьи пики измерены, —
            # окно из бюджета не выводится (иначе тесты зависели бы от памяти хоста).
            injected_ports=getattr(args, "ports", None) is not None,
        )
        # F0: провенанс бюджета фиксируется **здесь**, на старте, и едет в метрики как есть.
        # Иначе блок ``resources.budget`` описывал бы ``MemAvailable`` конца прогона —
        # другое число, чем то, из которого выведено окно (часы краула меняют его на
        # гигабайты). Публиковать «бюджет» и «окно из бюджета» из разных мгновений значило
        # бы соврать провенансом — ту же ошибку, что метка ``env`` у отвергнутого значения.
        #
        # P9: устройство — та же разрешённая ось, что увидит экстракция (``app < run``),
        # поэтому VRAM-часть провенанса описывает именно тот режим, который исполняется.
        budget_provenance = mem_budget_provenance(
            pdf_device=resolve_pdf_device(
                deploy_config.crawl if deploy_config is not None else CrawlConfig(),
                getattr(getattr(inputs, "run", None), "crawl", None),
            )
        )
        # Одно соединение на прогон (R7): фильтрация URL, сборка портов (``store``/
        # ``chunk_lookup`` для лексического/dense плеча), Runner и пост-корпусная
        # перепривязка ретривала работают на этом объекте. Открываем **до**
        # ``_resolve_ports``: без ``store``/``chunk_lookup`` оба билдера ретривала
        # возвращают ``None``, и Ш6 падает на каждой ячейке.
        store = build_store(db_path, profile=args.profile)
        ports: Ports | None = None
        # 09 §10.2: соединение с БД предшественника открывается внутри ``try`` (путь
        # зависит от раскладки), но объявляется здесь: закрытие — в ``finally`` ниже, и
        # обращение к необъявленному имени на пути исключения было бы NameError вместо
        # исходной ошибки.
        previous_store: Any = None
        #: ``--only-failed``: work-set ремонта (``product_id → [url]``) — отдельный вход Ш5.
        #: Провал глубокой страницы не становится seed-корнем: ремонт идёт известными
        #: depth-0 URL, авторские корни не обходятся (см. ``_only_failed_work_set``).
        only_failed_repairs: dict[str, list[str]] = {}
        try:
            if only_failed or only_product is not None:
                corpus_urls, only_failed_repairs, filter_notes = _only_failed_work_set(
                    corpus_urls,
                    store=store,
                    session_id=session_id,
                    only_failed=only_failed,
                    only_product=only_product,
                    # Межпрогонный носитель провалов: тот же ``state.db`` пакета, что увидит
                    # Ш5. ``None`` — состояние недоступно (тогда ремонт пуст и это видно).
                    state=state_store,
                )
            # F0/§3.3: preflight бюджета памяти — **до** запуска фаз и до сборки портов.
            # Это отказ начинать заведомо невозможную конфигурацию (0 воркеров при
            # выведенном окне), а не потолок на работу: если воркеры помещаются, никакого
            # лимита не вводится. Явно объявленная ось ``crawl.extract_workers`` сильнее
            # бюджета — тогда только видимое предупреждение с числами.
            if "corpus" in _phases_until(args.until):
                budget_run_crawl = getattr(getattr(inputs, "run", None), "crawl", None)
                if document_level is not None:
                    base_crawl = (
                        budget_run_crawl
                        if budget_run_crawl is not None
                        else CrawlRunConfig()
                    )
                    budget_run_crawl = base_crawl.model_copy(
                        update={"document_level": document_level}
                    )
                budget_error, budget_warnings = preflight_extraction_budget(
                    roots=[
                        root for roots in corpus_urls.values() for root in roots
                    ],
                    deploy=deploy_config,
                    run_crawl=budget_run_crawl,
                    profiles=inputs.domain_profiles or {},
                    mem_budget_mb=mem_budget_mb,
                    derive_extract_workers=derive_extract_workers,
                )
                config_notices.extend(budget_warnings)
                if budget_error is not None:
                    raise CliError(
                        budget_error.describe(),
                        code="memory",
                        exit_code=EXIT_USAGE,
                    )

            ports = _resolve_ports(
                args,
                deploy_config,
                inputs=inputs,
                no_cache=from_scratch,
                store=store,
                run_id=run_id,
            )
            # I3/§5: SERP-контур исполняется в фазе корпуса и требует адаптера внешнего
            # поиска. В ``split`` он приходит швом из портов; в ``local`` сетевые порты не
            # собираются автоматически — поэтому при **объявленных** раундах адаптер
            # собирается здесь (единственная точка решения провайдера — deploy-конфиг,
            # как у ``hound discover``). Объявленные раунды без адаптера были бы тихим
            # «контур есть, но не работал» — вместо этого фаза пишет видимый отказ.
            # Адаптер нужен только если фаза корпуса вообще будет исполнена: на
            # ``--until plan`` сборка портов поиска была бы работой ради уведомления.
            if (
                getattr(args, "ports", None) is None
                and getattr(declared, "serp_rounds", 0) > 0
                and getattr(ports, "search", None) is None
                and "corpus" in _phases_until(args.until)
            ):
                search_lang = (
                    str(getattr(inputs.run, "languages", None)[0])
                    if getattr(inputs.run, "languages", None)
                    else "ru"
                )
                try:
                    # R7 (P1): адаптер собирается с тем же кэшем, что краулеры, — pipeline
                    # кэширует ответы поиска (``cache/serp``) через него, без второй точки
                    # построения кэша. ``--from-scratch`` → без кэша (живой ре-фетч).
                    serp_adapter, serp_provider = resolve_and_build_search_adapter(
                        lang=search_lang,
                        explicit=None,
                        cache=resolve_content_cache(args, no_cache=from_scratch),
                        # Режим — из объявленных осей (``run.yaml: serp.mode``), а не из
                        # env: он определяет, какой бэкенд отдаёт находки, значит и
                        # корпус. Env-значение остаётся настройкой разведки.
                        search_mode=getattr(declared, "serp_mode", None),
                    )
                except (SearchResolutionError, WiringError) as exc:
                    # Не отказ прогона: корпус строится без находок поиска, а причина
                    # остаётся видимой строкой предупреждения (не тихим пропуском).
                    config_notices.append(f"serp: адаптер не собран — {exc}")
                else:
                    if serp_adapter is not None:
                        ports.search = serp_adapter
                        config_notices.append(
                            f"serp: адаптер внешнего поиска {serp_provider or 'injected'} "
                            "(провайдер из deploy.search)"
                        )
            # 09 §10.2: БД предшественника открывается **до** сборки фаз и закрывается в
            # ``finally`` рядом с ``store``: перенос ячеек читает из неё ячейку и evidence.
            # Причина отказа читать — предупреждение прогона, а не тихий пропуск: иначе
            # «переноса не было» неотличимо от «механизм выключен».
            previous_store, previous_store_note = _open_previous_store(
                getattr(args, "runs_dir", None), previous_run_id
            )
            if previous_store_note:
                config_notices.append(previous_store_note)
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
                progress=(
                    _progress_callback(session_id=session_id, json_mode=bool(args.json))
                    if follow
                    else None
                ),
                # Phase 4: продукт-скоуп живого прогресса — «продукт начат/завершён»
                # виден во время многочасового дренажа, а не только в конце фазы.
                on_product=(
                    _product_callback(session_id=session_id, json_mode=bool(args.json))
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
                # 16 §3.1: план продуктов — **тот же** объект, что composition root кладёт
                # в снимок входов (``run_inputs.product_plan``). Без него манифест берёт
                # значение из снимка, а на ``--until plan`` снимок писался **до**
                # онбординга и плана в нём нет — то есть срез остался бы неотличим от
                # полного измерения. Значение не пересчитывается: это ровно тот
                # ``model_dump``, что уходит в снимок.
                product_plan=(
                    onboarding.plan_set.model_dump(mode="json")
                    if onboarding is not None
                    else None
                ),
                # 09 §13.6: срез прогона — та же величина, что вошла в ``run_id``.
                # Без неё ``runs.run_scope`` остаётся NULL, и читатель артефакта не
                # отличает полное измерение от ``--only-product``/``--only-failed``.
                run_scope=run_scope,
                refresh_policy=refresh_policy.value,
                # План 19 §P3: источник корпуса при ``frozen`` — та же ось, что вошла
                # в контракт; ``None`` при иной политике (канонизация не нужна: фаза
                # исполняет только ``frozen``).
                frozen_source=getattr(args, "frozen_source", None),
                # План 19 §P2: исполнение политики — тот же объект, что дал ``run_id``.
                crawl_policy=policy_resolution,
                # ``--only-failed``: work-set ремонта — отдельный вход Ш5 (не ``urls``:
                # провал глубокой страницы не становится seed-корнем). Пусто ⇒ прежнее
                # поведение; продукты без провалов в набор не попадают и не краулятся.
                only_failed_repairs=only_failed_repairs or None,
                # 09 §12.5: тот же дайджест, что ушёл в ``run_id``/lineage (``contract_d``),
                # — манифест/``runs`` не пересчитывают его из другого набора значений.
                execution_contract_digest=contract_d,
                # §13 (2.4.0): те же декларированные оси, что у контракта, — один резолвер.
                axes=declared,
                declared_status_dict_version=status_dict_version,
                # 09 §12.3/§13: база сравнения прогона и класс изменения. Предшественник
                # разрешён **один** раз выше (:func:`previous_run_of` — та же величина, что
                # пойдёт в ``run_lineage``); класс — из той же ``RunIdentity``. Фаза оценки
                # называет предшественника в логе, манифест — полем ``previous_run_id``.
                previous_run_id=previous_run_id,
                previous_store=previous_store,
                change_class=identity.change_class.value,
                # 09 §2.4/§6.2: цикл — та же величина, что вошла в ``run_id``; в манифест
                # он попадает полем (иначе цикл читался бы только из ``run_id``/lineage).
                cycle_id=identity.cycle_id,
                # F0: бюджет памяти — тот же, что проверил preflight (один резолв на прогон).
                mem_budget_mb=mem_budget_mb,
                derive_extract_workers=derive_extract_workers,
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
            # Резолвер аттестации профиля строится здесь же: ``state_store`` — тот же
            # ``state.db`` пакета, что видит инкрементальный корпус. ``None`` (dry-run/
            # ``--until plan``) → гейт проверяет только ``gate_decisions``, как раньше.
            clock = getattr(args, "clock", None)
            # Единый runtime гейтов: предмет решения (G1/G2 — профиль, G3 — очередь
            # ревью, G4 — публикуемое содержимое), аттестация профиля, политика
            # ``run.yaml:gates`` и каталог прогонов для запроса на решение.
            gate_runtime = _gate_runtime(
                args,
                inputs,
                run_plan,
                cohort_digest=cohort_d,
                seeds_digest=seeds_d,
                state=state_store,
                store=store,
                # §10: тот же вход, что у фазы Ш8 — предмет G4 обязан описывать тот же
                # профиль публикуемости (включая признак стенда), который публикуется.
                rnd_endpoint=getattr(deploy_config.llm, "rnd_endpoint", None),
            )
            instrumentable: dict[str, PhaseFn] = dict(phases)
            for gate_name, gate in GATE_PHASES.items():
                instrumentable.setdefault(
                    gate_name, _gate_phase(gate_name, gate, gate_runtime)
                )
            instrumentation = instrument_phases(
                instrumentable,
                run_id=run_id,
                logs_root=args.runs_dir,
                clock=clock,
            )

        except BaseException:
            # Сборка портов/фаз не должна утекать соединением: закрываем и пробрасываем.
            if previous_store is not None:
                previous_store.close()
            store.close()
            raise

        instrumentation_errors: list[str] = []
        final_status: str | None = None
        crawl_counts: dict[str, int] = {}
        truncation: dict[str, Any] = {}
        crawl_session_lifecycle: dict[str, Any] = {}
        try:
            _ensure_run(store, run_id, inputs)
            # I21/§7.4: объявленная таблица ``scenario → модель`` пишется в строку прогона
            # сразу после её создания — иначе смена модели под сценарий видна только по
            # дайджесту контракта, но не по имени модели (``hound report``/``hound diff``).
            instrumentation_errors.extend(
                _persist_llm_scenarios(store, run_id, declared)
            )
            # 03 §A.2 / P1: снимок входов прогона (иммутабельная копия run.yaml +
            # производный plan_inputs.json) и источник истины — таблица ``run_inputs`` в БД
            # (каталог/веса/когорта). Пишется ДО исполнения фаз, чтобы последующие команды
            # были самодостаточны без исходного пакета. Сбой снимка не роняет прогон:
            # фиксируется в instrumentation_errors и не подменяет код возврата.
            try:
                instrumentation_errors.extend(
                    _snapshot_run_inputs(
                        args,
                        run_id,
                        inputs=inputs,
                        run_plan=run_plan,
                        store=store,
                        # v2 снимка (09 §14.2): цепочка циклов — та же идентичность, что
                        # уже вошла в ``run_id``; план продуктов — то же значение, что
                        # уходит в ``runs.product_plan_json`` (``None`` — онбординг вне
                        # области прогона: ``--until`` до фазы онбординга).
                        cycle_id=cycle_id,
                        cohort_digest=cohort_d,
                        seeds_digest=seeds_d,
                        previous_run_id=previous_run_id,
                        product_plan=(
                            onboarding.plan_set.model_dump(mode="json")
                            if onboarding is not None
                            else None
                        ),
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
            # F0: наблюдение фазы корпуса живёт в ``log_entries`` (рядом с ``IndexBuild``);
            # если фаза не отработала, наблюдения нет — и блок ``resources`` остаётся
            # нейтральным (``None``/пустые карты), а не выдумывает нули.
            corpus_observation: ResourcesObservation | None = next(
                (
                    entry
                    for result_item in run_report.results
                    if result_item.phase == "corpus"
                    for entry in getattr(result_item, "log_entries", []) or []
                    if isinstance(entry, ResourcesObservation)
                ),
                None,
            )
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
                    # F1/Д-6: жизненный цикл сеанса — в ``metrics.json`` (``interrupted``
                    # должен быть виден без разбора БД). ``stale_reason`` — чистая функция
                    # свидетельства живости, ничего не пишет.
                    crawl_session_lifecycle = (
                        {
                            "status": session.status.value,
                            "owner": session.owner,
                            "heartbeat_at": session.heartbeat_at,
                            "stale_reason": stale_reason(session),
                        }
                        if session is not None
                        else {}
                    )
                except Exception as exc:  # noqa: BLE001 — метрики best-effort
                    instrumentation_errors.append(
                        f"crawl_counts: {type(exc).__name__}: {_concise(exc)}"
                    )
                # F0: блок ``resources`` — пики RSS родителя по фазам (наблюдаемость),
                # наблюдение фазы корпуса (расписание/чанки на документ/пик воркера) и
                # провенанс бюджета памяти. Блок нейтрален к контракту: ни одно поле не
                # входит в ``run_id`` — это измерения исполнения, а не условия измерения.
                # Стоимость (M1/§15): расход считает **адаптер оценщика** — выше по стеку
                # ячеек ``usage`` уже нет, а ``metrics.json`` до этого писал ``llm_calls: 0``
                # при ~117 живых вызовах, то есть метрика стоимости молча лгала.
                # ``usage_summary()`` живёт **один процесс**, поэтому передаём вклад этого
                # захода, а накопление по заходам делает ``write_run_metrics`` из уже
                # записанного ``metrics.json`` (схема БД не меняется). Отсутствие метода — не
                # ошибка (шов/иной адаптер): вклад нулевой и помечен ``source="absent"``,
                # то есть «расход неизвестен», а не «вызовов не было».
                usage = collect_llm_usage(getattr(ports, "evaluator", None))
                if usage.error is not None:
                    instrumentation_errors.append(f"llm_usage: {usage.error}")
                # Coverage/Capability — из **той же** формулы, что у фазы ``compute`` и
                # предмета G4 (``bundle_for_run``), а не пересчёт «своими руками»: без бандла
                # ``compute_metrics`` публиковал ``coverage_aggregate: 0.0`` при реальных
                # 0.208, то есть не «нет данных», а **неверное число** в машинном отчёте.
                # Сбой расчёта не роняет наблюдаемость: поле остаётся пустым дефолтом, и это
                # видно в ``instrumentation_errors``.
                try:
                    score_bundle = bundle_for_run(
                        store,
                        run_id=run_id,
                        plan=run_plan,
                        inputs=inputs,
                        rnd_endpoint=getattr(deploy_config.llm, "rnd_endpoint", None),
                    )
                except Exception as exc:  # noqa: BLE001 — метрики не роняют прогон
                    score_bundle = None
                    instrumentation_errors.append(
                        f"score_bundle: {type(exc).__name__}: {_concise(exc)}"
                    )
                write_run_metrics(
                    run_id=run_id,
                    out_dir=args.runs_dir,
                    durations=instrumentation.durations,
                    store=store,
                    score_bundle=score_bundle,
                    crawl_counts=crawl_counts,
                    crawl_truncation=truncation or None,
                    crawl_session=crawl_session_lifecycle or None,
                    llm_calls=usage.calls,
                    tokens=usage.tokens or None,
                    llm_usage_source=usage.source,
                    llm_usage_error=usage.error,
                    resources=build_resources_block(
                        phase_peaks=instrumentation.peak_rss_mb,
                        phase_durations=instrumentation.durations,
                        observation=corpus_observation,
                        budget=budget_provenance,
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — наблюдаемость не роняет прогон
                instrumentation_errors.append(
                    f"metrics: {type(exc).__name__}: {_concise(exc)}"
                )
        finally:
            store.close()
            if previous_store is not None:
                # Чтение закончено: БД предшественника — чужое иммутабельное свидетельство,
                # держать соединение после фазы оценки незачем.
                previous_store.close()
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
            # План 20 §7 F2: разброс дат **вошедшего** корпуса — числом, и предупреждение
            # по порогу отображения. Источник — документ отчёта (``corpus_freshness``), а
            # не ``state.db``: прогон сдвинул наблюдения, и прежний разброс по ним уже не
            # восстановить. Ни вердиктов, ни ``run_id`` это не касается.
            freshness_document = getattr(artifacts_obj, "corpus_freshness_summary", None)
            if freshness_document:
                freshness_block = document_observation_freshness(
                    freshness_document,
                    warn_days=float(
                        getattr(
                            args,
                            "freshness_warn_days",
                            FRESHNESS_WARN_DAYS_DEFAULT,
                        )
                    ),
                )
                payload["observation_freshness"] = freshness_block
                if freshness_block["warnings"]:
                    payload["warnings"] = [
                        *(payload.get("warnings") or []),
                        *freshness_block["warnings"],
                    ]
        if truncation:
            # N4: усечение видно в run/--json payload (а не только в провенансе сеанса).
            payload["crawl_truncation"] = truncation
        if follow:
            payload["follow"] = True
        if filter_notes:
            payload["filter_notes"] = list(filter_notes)
        if onboarding_warnings or config_notices:
            payload["warnings"] = list([*onboarding_warnings, *config_notices])
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
            # Причина называется та, что записана в провенансе корня, а не список политик:
            # усечение бывает не только по max_documents/max_depth — F6/F8 добавили
            # extract_backlog_full и disk_low_headroom, 09 §6.3 — revalidate_max_documents.
            # «Лимит достигнут (какой-то)» оператору не помогает.
            warning = (
                "краул усечён политикой "
                f"({truncation.get('truncated_reason') or 'причина не записана'}): "
                "корпус неполон; "
                f"продукты={truncation.get('truncated_products')}; "
                f"страниц={truncation.get('pages_fetched')}; "
                f"ревалидация={truncation.get('revalidation_fetched')}"
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
            # F1: ``interrupted`` (сеанс снят/убит, тело краула не финализировано) — не успех.
            if final_status in (
                CrawlSessionStatus.PARTIAL.value,
                CrawlSessionStatus.FAILED.value,
                CrawlSessionStatus.INTERRUPTED.value,
            ):
                if truncation_only and final_status == CrawlSessionStatus.PARTIAL.value:
                    return EXIT_OK
                return EXIT_ERROR
            return EXIT_OK
        if run_report.status == "blocked":
            return EXIT_BLOCKED
        return EXIT_ERROR
    except BaseException as exc:
        # Отказ печатает main.py — уже после возврата управления. Дублируем в файл, иначе
        # «почему не пошло» не будет в логе прогона (Ctrl-C тоже здесь).
        _append_console(console, f"error: {type(exc).__name__}: {exc}")
        raise
    finally:
        _restore_console(console)
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
        help="политика обновления корпуса: auto|revalidate|refresh|offline|frozen (дефолт auto)",
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
    _add_llm_model_flag(p_run)
    # План 20 §7 F2: тот же порог **отображения**, что у ``plan`` (общий регистратор:
    # план не принимает флагов, которых прогон не знает). Здесь он применяется к датам
    # **вошедшего** корпуса (документ ``corpus_freshness`` отчёта): к концу прогона
    # ``state.db`` уже сдвинут этим прогоном, и «разброс до измерения» по нему не
    # восстановить.
    add_freshness_warn_days_flag(p_run)
    p_run.add_argument(
        "--serp-rounds",
        dest="serp_rounds",
        default=None,
        metavar="N",
        help="раунды SERP-контура внешнего поиска (0 — выключить). Приоритет: "
        "флаг > COHOUND_SERP > run.yaml: serp.rounds > deploy.search.serp.rounds > 0. "
        "Значение входит в execution contract (serp_rounds) ⇒ новый run_id; бюджет "
        "объявляется в run.yaml: serp.budget",
    )
    _add_common(p_run)
    p_run.set_defaults(handler=run)
