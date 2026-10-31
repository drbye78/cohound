"""coHound — общий контекст и DI-хелперы CLI (composition root Z0, ``03`` §E).

Разделяемые помощники команд, вынесенные из ``core.cli.main`` (P1-A): разрешение
раскладки/портов/фаз, загрузка входов (паспорта/корпус/снапшоты), печать отчётов,
диагностика DMZ-шва. Модуль не содержит хендлеров команд — только переиспользуемую
машинерию; команды в :mod:`core.cli.commands` импортируют её отсюда.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

import yaml

from core.adapters.gateway_validate import validate_envelope_shape
from core.adapters.registry_generic import (
    RegistryConfig,
    RegistryConfigError,
    load_package_registry_configs,
)
from core.adapters.remote_gateway import (
    DEFAULT_SCHEMA_VERSION,
    GatewayError,
    GatewayStatusError,
    GatewayTransportError,
    TransportFn,
)
from core.cli.errors import EXIT_USAGE, FALLBACK_CODE, CliError
from core.cli.output import (
    concise,
    emit_error,
    emit_lint_failure,
    print_json,
)
from core.cli.wiring import (
    LOCAL_PROFILE,
    SUPPORTED_PROFILES,
    build_adapters,
    build_evaluator_llm,
    build_extraction_pool,
    build_gateway,
    build_store,
    build_worker_extractor,
    llm_env_notices,
    manifest_deploy_fields,
    preflight_pdf_device,
    resolve_extraction_budget,
    run_db_path,
)
from core.config import (
    PassportLoadError,
    load_golden,
    load_package_manifest,
    load_passports,
    passport_content_digests,
    passport_urls,
)
from core.config.deploy import LLM_SLOTS, DeployConfig, profile_notices
from core.domain.enums import (
    CrawlItemStatus,
    CrawlSessionStatus,
    DocumentLevel,
    EndpointKind,
    Gate,
    LLMScenario,
    PdfDevice,
    PdfPipeline,
    Status,
)
from core.domain.models import CatalogRow, Cohort, ScenarioBinding
from core.pipeline import PHASES, Ports, RunReport, build_phases
from core.pipeline.compute import bundle_for_run
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.embed_cache import EmbedCache
from core.pipeline.eval_cache import EvalVerdictCache
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.gate_subject import (
    GateSubject,
    publication_subject,
    review_subject,
)
from core.pipeline.normalize import RunInputs, load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.phases import resolve_package_id
from core.pipeline.plan import RunPlan
from core.pipeline.plan import plan as build_plan
from core.pipeline.profile_attestation import (
    ProfileGateResult,
    make_profile_gate,
    material_digest,
    profile_material,
)
from core.pipeline.review import ReviewItem, review_queue
from core.pipeline.runner import GateRuntime, PhaseFn, sorted_error_lines
from core.ports.gateway import ContentGateway
from core.ports.store import PlanInputsConflict, StorePort
from core.ports.types import FetchMode
from core.store.state_sqlite import STATE_SCHEMA_VERSION
from core.util.cache import ContentAddressedCache
from core.util.logging import PhaseLogger
from core.util.rate_limit import RateLimiter
from core.util.security import sanitize_filename

# -- Типовые швы (тестовые инъекции) --------------------------------------- #
#: Фабрика диагностического gateway по deploy-конфигу (тестовый шов ``deploy-check``).
GatewayFactory = Callable[[DeployConfig], ContentGateway]
#: Фабрика реестра фаз прогона (тестовый шов ``run``: подмена реального ``build_phases``).
PhasesFactory = Callable[..., dict[str, PhaseFn]]
#: Фабрика плана прогона (тестовый шов ``run``: подмена детерминированного ``plan``).
PlanFactory = Callable[..., RunPlan]

# -- Коды возврата/ошибки --------------------------------------------------- #
# Коды возврата, :class:`CliError`/:class:`LintFailure`, :func:`extract_code` и вывод
# (JSON/ошибки) вынесены в :mod:`core.cli.errors`/:mod:`core.cli.output`; здесь —
# реэкспорт приватных алиасов вывода (историческая поверхность ``main``).
_FALLBACK_CODE = FALLBACK_CODE
_concise = concise
_print_json = print_json
_emit_error = emit_error
_emit_lint_failure = emit_lint_failure

#: Допустимые явные вердикты переаттестации для ``hound promote --status`` (I14).
#: N/A сюда не входит: назначение N/A — отдельное человеческое решение (§5), не
#: повышение ``preliminary``-кандидата.
_PROMOTE_STATUSES: tuple[str, ...] = (
    Status.OK.value,
    Status.PARTIAL.value,
    Status.ABSENT.value,
    Status.UNKNOWN.value,
)

#: Имя иммутабельной копии входа в каталоге прогона (``03`` §A.2).
RUN_YAML_FILENAME = "run.yaml"
#: Имя снимка входов плана в каталоге прогона (самодостаточность прогона, ``03`` §A.2).
PLAN_INPUTS_FILENAME = "plan_inputs.json"
#: Версия формата :data:`PLAN_INPUTS_FILENAME` (эволюция без поломки чтения).
#:
#: 1 — идентичность пакета + ``params``/``module_weights``/``renormalized_weights``/
#:     ``cohort``: этого хватает, чтобы пересобрать каталог/веса/когорту без пакета, но
#:     **не** чтобы восстановить цепочку циклов (нет цикла, дайджестов, плана продуктов).
#: 2 — добавлены ``cycle_id``/``cohort_digest``/``seeds_digest``/``previous_run_id``/
#:     ``product_plan``/``state_schema_version`` (09 §14.2, «состав payload»).
#: Смена версии — **граница снимка**, а не «дописать поле»: ``payload_sha256`` меняется,
#: поэтому уже записанный снимок прежней версии не перезаписывается (O11), а факт
#: называется видимой строкой в ``issues`` (:func:`_snapshot_run_inputs`).
PLAN_INPUTS_SNAPSHOT_VERSION = 2
#: Версии снимка, которые **читатель** принимает (пишется всегда только текущая).
#: v1 читается по своим полям, новые поля у неё — ``None`` («не записано»), а не
#: ``0``/``""``: значения не выдумываются (A3).
PLAN_INPUTS_SUPPORTED_VERSIONS: tuple[int, ...] = (1, 2)

__all__ = [
    "PLAN_INPUTS_FILENAME",
    "PLAN_INPUTS_SNAPSHOT_VERSION",
    "PLAN_INPUTS_SUPPORTED_VERSIONS",
    "RUN_YAML_FILENAME",
    "_FALLBACK_CODE",
    # Константы и приватные алиасы
    "_PROMOTE_STATUSES",
    # Типовые швы
    "GatewayFactory",
    "PhasesFactory",
    "PlanFactory",
    "PlanInputsSnapshot",
    "_acquire_package_lock",
    "_add_common",
    "_add_llm_model_flag",
    "_add_llm_slot_flag",
    "_artifacts_payload",
    "_concise",
    "_diagnostic_gateway",
    "_discover_seed_queries",
    "_emit_deploy_check",
    "_emit_error",
    "_emit_lint_failure",
    "_ensure_run",
    "_gate_runtime",
    "_infer_run_id",
    "_load_corpus_urls",
    "_load_golden_arg",
    "_load_package_roles",
    "_load_passports_dir",
    "_load_render_modes",
    "_manifest_partial",
    "_only_failed_work_set",
    "_open_state_store",
    "_package_lock",
    "_package_lock_handle",
    "_persist_llm_scenarios",
    "_phases_until",
    "_plan_inputs_from_payload",
    "_plan_inputs_payload",
    "_plan_inputs_sha256",
    "_plan_payload",
    "_positive_int",
    "_previous_cycle_id",
    "_print_discover",
    "_print_gate",
    "_print_json",
    "_print_passport_scout",
    "_print_plan",
    "_print_promote",
    "_print_review",
    "_print_run_dry",
    "_print_run_report",
    "_print_sensitivity",
    "_profile_gate_resolver",
    "_profile_material_for_package",
    "_profile_material_for_run",
    "_read_cycle",
    "_resolve_db_path",
    "_resolve_passports_dir",
    # DI-хелперы и машинерия команд
    "_resolve_paths",
    "_resolve_phases",
    "_resolve_plan",
    "_resolve_ports",
    "_restore_plan_inputs",
    "_review_item_payload",
    "_run_report_payload",
    "_seam_diagnostics",
    "_short_json",
    "_snapshot_plan_inputs",
    "_snapshot_run_inputs",
    "_snapshot_run_yaml",
    "_validate_capabilities_payload",
    "_validate_health_payload",
]

def _resolve_paths(args: argparse.Namespace) -> Any:
    """Резолвит эффективную раскладку операционных каталогов (``03`` §A.1).

    Источник — :func:`core.config.paths.resolve_paths`: env (``COHOUND_*_DIR`` /
    ``COHOUND_HOME``) > ``config/paths.yaml`` (или ``COHOUND_PATHS_FILE``) > дефолты модели
    (относительные — от CWD). CLI-флаги сюда не читаются: их приоритет обеспечивает
    :func:`main` (флаг побеждает resolved-дефолт естественным образом). Ленивый импорт
    держит ``core.config.paths`` вне горячего пути команд, раскладку не запрашивающих.
    """
    from core.config.paths import resolve_paths

    return resolve_paths()


def _open_state_store(args: argparse.Namespace, *, create: bool = False) -> Any:
    """Открывает durable per-product состояние пакета (``state/{package_id}/state.db``).

    Раскладка — ``args.paths.state`` (composition root уже резолвит ``COHOUND_STATE_DIR``/
    ``COHOUND_HOME``/``paths.yaml``); без ``args.paths`` (прямой вызов хендлера в тестах) —
    :func:`core.config.paths.resolve_paths`. ``package_id`` — имя каталога пакета.

    ``create=False`` (``--dry-run``) — **только чтение**: отсутствующий ``state.db`` →
    ``None``, каталоги не создаются. ``create=True`` (реальный прогон) — каталог
    ``state/{package_id}/`` создаётся при необходимости.

    ``None`` также возвращается, если ``--package`` не является реальным каталогом:
    состояние привязано к пакету, а не к произвольной строке.
    """
    from core.store.state_sqlite import state_db_path
    from core.store.state_store import SqliteStateStore

    package = getattr(args, "package", None)
    if not package or not Path(package).is_dir():
        return None
    paths = getattr(args, "paths", None)
    if paths is None:
        paths = _resolve_paths(args)
    db_path = state_db_path(paths.state, Path(package).name)
    if not create and not db_path.exists():
        return None
    if create:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteStateStore(db_path)


def _package_lock_handle(
    args: argparse.Namespace, *, command: str | None = None
) -> Any:
    """Создаёт :class:`PackageLock` для пакета команды; ``None`` — пакет не определён.

    Точка ресурса — каталог durable-состояния пакета (``state/{package_id}/``), файл —
    ``state/{package_id}/.lock``: он один на пакет, переживает прогоны и уже резолвится
    раскладкой (``args.paths.state`` / ``COHOUND_STATE_DIR``).

    Лок **не берётся** здесь: вызывающий решает, когда пакет действительно занимается
    (например, ``run`` не занимает его для заблокированного онбордингом прогона, который
    state не создаёт). Отдельная функция нужна ещё и потому, что ``CliError`` о занятости
    формируется в :func:`_acquire_package_lock` — по одному сообщению на все команды.

    Пакет не определяется (нет ``--package`` или это не каталог) — ``None``: поведение
    прежнее, это не ошибка.
    """
    from core.util.package_lock import PackageLock, package_lock_path

    package = getattr(args, "package", None)
    if not package or not Path(package).is_dir():
        return None
    paths = getattr(args, "paths", None)
    if paths is None:
        paths = _resolve_paths(args)
    return PackageLock(
        package_lock_path(paths.state, Path(package).name),
        timeout_s=float(getattr(args, "lock_timeout", 0.0) or 0.0),
        command=command,
    )


def _acquire_package_lock(lock: Any) -> None:
    """Берёт лок пакета; занятый — :class:`CliError` с кодом ``state`` и ``EXIT_USAGE``.

    Сообщение называет держателя (хост/pid/старт/команду) и путь файла: оператору нужен
    ответ «кто именно», а не «занято». ``lock is None`` (пакет не определён) — no-op.
    """
    if lock is None:
        return
    from core.util.package_lock import LockHolder, PackageLocked

    try:
        lock.acquire()
    except PackageLocked as exc:
        holder = (exc.holder or LockHolder()).describe()
        reason = f"; {exc.reason}" if exc.reason else ""
        raise CliError(
            f"пакет '{lock.path.parent.name}' уже используется {holder}{reason}; "
            f"файл лока: {lock.path}",
            code="state",
            exit_code=EXIT_USAGE,
        ) from exc


@contextlib.contextmanager
def _package_lock(
    args: argparse.Namespace, *, command: str | None = None
) -> Iterator[Any]:
    """Держит файловый lock пакета на время команды (09 §4.4).

    Зачем: WAL + ``busy_timeout`` сериализует запись в **БД**, но две операции над одним
    пакетом (два краула, краул и ``run``) делят сеть, бюджет, ``corpus_raw/`` и
    ``index_store/``, а не отвергают друг друга (``core/util/package_lock.py``).

    ``--dry-run`` лок не берёт **сам** (см. вызывающих): создание файла лока было бы
    записью, а флаг обещает, что её нет.
    """
    lock = _package_lock_handle(args, command=command)
    _acquire_package_lock(lock)
    try:
        yield lock
    finally:
        if lock is not None:
            lock.release()


def _previous_cycle_id(args: argparse.Namespace) -> str | None:
    """Предыдущий ``cycle_id`` пакета — **только чтение** ``state.db`` (09 §2.4).

    Нужен ``plan``, чтобы ``--update`` вёл себя как у прогона: ``resolve_cycle_id``
    отвергает ``--update`` с циклом, равным предыдущему, и план обязан повторить **этот
    отказ**, иначе напечатает ``run_id`` прогона, который не стартует.

    Штатное открытие состояния (:func:`_open_state_store`) применяет схему и пишет
    ``schema_meta`` — для ``plan`` это нарушило бы обещание «ничего не пишет». Поэтому
    читается одна колонка ``run_lineage.cycle_id`` через ``mode=ro``
    (:func:`core.store.state_store.read_previous_cycle_id`), без миграции и без записи.

    ``None`` — истории нет либо пакет не является каталогом: «неизвестно», не «пусто».
    """
    from core.store.state_sqlite import state_db_path
    from core.store.state_store import read_previous_cycle_id

    package = getattr(args, "package", None)
    if not package or not Path(package).is_dir():
        return None
    paths = getattr(args, "paths", None)
    if paths is None:
        paths = _resolve_paths(args)
    return read_previous_cycle_id(state_db_path(paths.state, Path(package).name), Path(package).name)


def _read_cycle(args: argparse.Namespace, run_id: str) -> tuple[Any, list[Any], list[Any]]:
    """Читает цикл прогона: ``(манифест, ячейки, документы корпуса)`` для ``diff``.

    ``--db`` (общая БД) приоритетнее ``--runs-dir``; иначе БД резолвится
    :func:`core.cli.wiring.run_db_path`. Документы (``iter_corpus_docs``) нужны для
    per-product doc-delta ``content_change`` (09 §12.5).
    """
    db_path = args.db if args.db is not None else run_db_path(args.runs_dir, run_id)
    manifest: Any = None
    cells: list[Any] = []
    docs: list[Any] = []
    with build_store(db_path, profile=args.profile, read_only=True) as store:
        manifest = store.read_manifest(run_id)
        cells = list(store.iter_cells(run_id))
        docs = list(store.iter_corpus_docs(run_id))
    return manifest, cells, docs


def _infer_run_id(store: Any) -> str:
    """Выводит единственный ``run_id`` из БД прогона (для ``sensitivity --db``).

    :raises CliError: БД пуста либо содержит несколько прогонов (нужен ``--run-id``).
    """
    rows = store.conn.execute("SELECT run_id FROM runs ORDER BY run_id").fetchall()
    if len(rows) == 1:
        return str(rows[0]["run_id"])
    if not rows:
        raise CliError("в БД нет ни одного прогона", code="usage", exit_code=EXIT_USAGE)
    raise CliError(
        f"--db содержит {len(rows)} прогонов; укажите --run-id",
        code="usage",
        exit_code=EXIT_USAGE,
    )


def _resolve_db_path(args: argparse.Namespace) -> str | Path:
    """Резолвит БД прогона: явный ``--db`` приоритетнее ``--runs-dir`` + ``--run-id``.

    Общий контракт выбора хранилища для ``sensitivity``/``golden``/``report``.

    :raises CliError: ``--runs-dir`` задан без ``--run-id`` (:data:`EXIT_USAGE`).
    """
    if args.db is not None:
        return args.db
    if not args.run_id:
        raise CliError(
            "--runs-dir требует --run-id", code="usage", exit_code=EXIT_USAGE
        )
    return run_db_path(args.runs_dir, args.run_id)


def _load_golden_arg(path: str | Path) -> list[Any]:
    """Загружает golden-набор (:func:`core.config.load_golden`) с понятной ошибкой CLI.

    :raises CliError: файл не читается или не соответствует схеме ``golden.csv``
        (§4.2/§11.6) — :data:`EXIT_USAGE` с кодом ``golden``.
    """
    try:
        return load_golden(path)
    except (OSError, ValueError) as exc:
        raise CliError(_concise(exc), code="golden", exit_code=EXIT_USAGE) from exc


def _manifest_partial(manifest: Any) -> bool:
    """Незаполнен ли манифест прогона (финальная фаза ``report`` не завершалась).

    ``SqliteStore.read_manifest`` подставляет заглушки ``"0.0.0"`` для версий, пока
    фаза ``report`` их не записала. Такое состояние честно помечается, а не выдаётся за
    реальный провенанс.
    """
    return getattr(manifest, "methodology_version", None) in (None, "", "0.0.0")


def _artifacts_payload(artifacts: Any) -> dict[str, Any]:
    """JSON-совместимое представление :class:`~core.pipeline.report.Artifacts`.

    B3: помимо путей, несёт собранные аномалии — I3 ``❓``-протокола (§5) и
    crawl-усечения (D1/G). Так оператор видит их в ``--json`` payload, а не только в
    ``nda_questions.csv``.
    """

    def _path(value: Any) -> str | None:
        return str(value) if value is not None else None

    return {
        "battle_card": _path(artifacts.battle_card),
        "preliminary_findings": _path(artifacts.preliminary_findings),
        "nda_questions": _path(artifacts.nda_questions),
        "diff": _path(artifacts.diff),
        "manifest": _path(artifacts.manifest),
        # 13-manual-artifacts §4: заполняемый вход следующего прогона — в ``--json``
        # payload, чтобы оператор видел путь, не угадывая раскладку ``outputs/``.
        "artifacts_gap": _path(getattr(artifacts, "artifacts_gap", None)),
        # A1/§6: профиль публикуемости — артефакт разложения неизвестности.
        "publishability": _path(getattr(artifacts, "publishability", None)),
        # P6c: полоса неопределённости вердиктов — артефакт условия измерения.
        "measurement_uncertainty": _path(
            getattr(artifacts, "measurement_uncertainty", None)
        ),
        # §13/K2 и P6: списки работы, которые иначе пришлось бы искать в ``outputs/``
        # по догадке. Артефакт без объявленного пути — та же невидимость, что и его
        # отсутствие: оператор не знает, что смотреть.
        "stale_cells": _path(getattr(artifacts, "stale_cells", None)),
        "cell_diagnostics": _path(getattr(artifacts, "cell_diagnostics", None)),
        # 09 §10.1: происхождение ячеек (перенос из прошлого прогона) — путь к таблице.
        # ``None`` — перенесённых нет, файл не создавался; это отличается от «путь забыт».
        "cell_origin": _path(getattr(artifacts, "cell_origin", None)),
        # I2/I2c: документы, прочитанные не целиком — путь к таблице (числа — в ней и в
        # строке заметки). Артефакт без объявленного пути — та же невидимость, что и его
        # отсутствие: оператор не знает, что смотреть.
        "partial_docs": _path(getattr(artifacts, "partial_docs", None)),
        # A3: качество поддержки карты — путь к файлу с двумя замерами и их адресами.
        # Числа публикуются и строкой-заметкой (см. ``commands/report.py``), путь нужен,
        # чтобы оператор знал, куда смотреть.
        "support_quality": _path(getattr(artifacts, "support_quality", None)),
        "i3_anomalies": list(getattr(artifacts, "i3_anomalies", None) or []),
        "crawl_truncation_anomalies": list(
            getattr(artifacts, "crawl_truncation_anomalies", None) or []
        ),
        # §11.6: продукты с объявленным waiver'ом эталона — видно в артефактах прогона,
        # а не только в файле пакета.
        "golden_exempt_products": list(
            getattr(artifacts, "golden_exempt_products", None) or []
        ),
    }


def _load_package_roles(package: str | Path | None) -> list[str] | None:
    """Читает ``role_id`` из ``{package}/package.yaml`` (подсказка LLM; опционально).

    :raises CliError: пакет задан, но не читается/невалиден (:data:`EXIT_USAGE`).
    """
    if package is None:
        return None
    try:
        manifest = load_package_manifest(Path(package) / "package.yaml")
    except (OSError, ValueError) as exc:
        raise CliError(_concise(exc), code="package", exit_code=EXIT_USAGE) from exc
    return [role.role_id for role in manifest.roles]


def _print_passport_scout(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``passport-scout``: черновик и диагностика."""
    lines = [
        f"product_id: {payload['product_id']}",
        f"canonical_name: {payload['canonical_name']}",
        f"vendor: {payload['vendor']}",
        f"jurisdiction: {payload['jurisdiction']}",
        f"satellites: {payload['satellites']}",
        f"doc_domains: {', '.join(payload['doc_domains']) or '—'}",
        f"fetched: {len(payload['fetched'])}",
        f"dry-run: {'yes' if payload.get('dry_run') else 'no'}",
    ]
    for label in ("passport_path", "candidates_path"):
        if label in payload:
            lines.append(f"{label}: {payload[label]}")
    for item in payload["needs_human"]:
        lines.append(f"needs_human: {item}")
    for error in payload["errors"]:
        lines.append(f"error: {error}")
    lines.append(f"ok: {'yes' if payload['ok'] else 'no'}")
    sys.stdout.write("\n".join(lines) + "\n")


def _discover_seed_queries(passports_dir: str | Path | None) -> list[str]:
    """Собирает seed-запросы из ``doc_domains`` паспортов (только подсказки, I19).

    Возвращает URL-кандидаты (:func:`core.config.passport_urls`) в детерминированном
    порядке: продукты по имени, домены — как в ``doc_domains``. Домены **не фетчатся**:
    это лишь подсказки для ручного/автоматического поиска, не evidence.
    """
    if passports_dir is None:
        return []
    passports = _load_passports_dir(passports_dir)
    seeds: list[str] = []
    for product_id in sorted(passports):
        for url in passport_urls(passports[product_id]):
            if url not in seeds:
                seeds.append(url)
    return seeds


def _review_item_payload(item: ReviewItem) -> dict[str, Any]:
    """JSON-совместимое представление :class:`~core.pipeline.review.ReviewItem`."""
    return {
        "product_id": item.product_id,
        "param_id": item.param_id,
        "state": item.state.value,
        "status": item.status.value if item.status is not None else None,
        "reason": item.reason,
        "evidence_count": item.evidence_count,
        "index_build_id": item.index_build_id,
    }


def _print_discover(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``discover``: кандидаты-гипотезы (не evidence, I19)."""
    lines = [
        f"queries: {len(payload.get('query', []))}",
        f"search_calls: {payload.get('search_calls', 0)}",
        f"termination_reason: {payload.get('termination_reason')}",
        f"candidates: {len(payload.get('candidates', []))}",
    ]
    for candidate in payload.get("candidates", []):
        urls = candidate.get("source_urls") or []
        url = urls[0] if urls else "—"
        lines.append(
            f"- {candidate['param_id']}\t{candidate['title']}\t{url}"
            f"\tconfidence={candidate['confidence']}"
        )
    if not payload.get("candidates"):
        lines.append("candidates: none — гипотез не найдено (evidence не создаётся, I19)")
    lines.append(f"note: {payload.get('note', '')}")
    sys.stdout.write("\n".join(lines) + "\n")


def _print_review(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``review``: очередь ревью (Ш7; ревью — внешний контур, гейта G3 нет)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"queue: {payload['count']} item(s)",
    ]
    if not payload["items"]:
        lines.append("queue empty: нет ячеек, требующих ревью (preliminary/❓, Ш7)")
    for item in payload["items"]:
        status = item["status"] if item["status"] is not None else "—"
        lines.append(
            f"- {item['product_id']}/{item['param_id']}\t{item['state']}\t"
            f"{status}\t{item['reason']}"
        )
    sys.stdout.write("\n".join(lines) + "\n")


def _print_gate(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``gate``: записанное решение гейта (S9/I6/I7).

    Вывод различает вердикт: ``approved`` продолжает прогон, ``rejected`` его **не**
    продолжает. Печать «гейт пройден» на отказе была бы ложью, из-за которой оператор
    ждал бы продолжения прогона, которого не будет.
    """
    verdict = payload.get("verdict", "approved")
    lines = [
        f"run_id: {payload['run_id']}",
        f"gate: {payload['gate']}",
        f"actor: {payload['actor']}",
        f"source: {payload.get('source', 'internal')}",
        f"verdict: {verdict}",
        f"subject_digest: {payload.get('subject_digest')}",
        f"decided_at: {payload['decided_at']}",
        f"decision_id: {payload['decision_id']}",
    ]
    if payload.get("external_ref") is not None:
        lines.append(f"external_ref: {payload['external_ref']}")
    if payload.get("target") is not None:
        lines.append(f"target: {payload['target']}")
    if payload.get("notes") is not None:
        lines.append(f"notes: {payload['notes']}")
    if verdict == "rejected":
        lines.append(
            f"решение записано: гейт {payload['gate']} ОТКЛОНЁН ({payload['actor']}) — "
            "прогон заблокирован отказом до нового решения по текущему предмету"
        )
    else:
        lines.append(
            f"решение записано: гейт {payload['gate']} пройден (продолжите hound run)"
        )
    sys.stdout.write("\n".join(lines) + "\n")


def _print_promote(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``promote``: повышение + отказы с причинами (I6/I7/I14)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"actor: {payload['actor']}",
        f"requested: {payload['requested']}",
        f"promoted: {payload['promoted']}",
    ]
    if not payload["promoted"] and not payload["skipped"] and not payload["errors"]:
        lines.append(
            "нечего повышать: очередь ревью пуста либо не найдено preliminary-ячеек"
        )
    for cell in payload["promoted_cells"]:
        lines.append(
            f"promoted {cell['product_id']}/{cell['param_id']} -> {cell['status']}"
        )
    for item in payload["skipped"]:
        lines.append(
            f"skip {item['product_id']}/{item['param_id']}: {item['reason']}"
        )
    for error in payload["errors"]:
        lines.append(f"error: {error}")
    sys.stdout.write("\n".join(lines) + "\n")


def _restore_plan_inputs(
    args: argparse.Namespace,
    run_id: str,
    *,
    purpose: str,
    store: Any,
) -> tuple[dict[str, Any], dict[str, float], Any, list[str]]:
    """Восстанавливает ``(catalog, module_weights, cohort, notes)`` для пост-хок команд.

    Приоритет источников:

    (a) **явные** ``--run-yaml`` + ``--package`` — высший приоритет: escape-hatch
        ре-деривации каталога из исходного пакета;
    (b) **снимок прогона в БД** (``run_inputs``; P1) — источник истины самодостаточности
        ``runs/{run_id}``. Заметка сообщает, что значения восстановлены из БД прогона;
    (c) иначе — **пустые** значения и явная заметка-ограничение (defense-in-depth):
        каталог/веса/когорта **не выдумываются** (вес ячейки = 1.0, когорта ``None``).

    :param store: открытый стор прогона (метод ``read_plan_inputs``); источник (b).
        Post-hoc команды уже резолвят БД (:func:`_resolve_db_path`) и передают стор сюда.
    :param purpose: имя команды в тексте ограничивающей заметки
        (``sensitivity``/``golden``/``report``).
    """
    notes: list[str] = []
    if args.run_yaml and args.package:
        inputs = load_inputs(args.run_yaml, args.package)
        run_plan = build_plan(inputs, run_id=run_id)
        catalog = {row.param_id: row for row in run_plan.params}
        module_weights = dict(
            getattr(run_plan, "renormalized_weights", None)
            or getattr(run_plan, "module_weights", None)
            or {}
        )
        return catalog, module_weights, getattr(run_plan, "cohort", None), notes

    if args.run_yaml or args.package:
        notes.append(
            "для восстановления каталога нужны ОБА --run-yaml и --package; указан только "
            "один — предпринята попытка восстановления из БД прогона"
        )

    raw: Any = None
    if store is not None:
        try:
            raw = store.read_plan_inputs(run_id)
        except Exception as exc:  # noqa: BLE001 — чтение best-effort, не роняет команду
            notes.append(f"входы плана из БД недоступны: {_concise(exc)}")
            raw = None
    restored = _plan_inputs_from_payload(raw, run_id) if raw is not None else None
    if restored is not None:
        notes.append("каталог/веса/когорта восстановлены из БД прогона")
        return restored.catalog, restored.module_weights, restored.cohort, notes

    notes.append(
        "входы плана (каталог/веса/когорта) отсутствуют в БД прогона — "
        f"{purpose} рассчитан с пустым каталогом (вес ячейки = 1.0) и без когорты; "
        "передайте --run-yaml и --package для точного воспроизведения"
    )
    return {}, {}, None, notes


@dataclass(frozen=True)
class PlanInputsSnapshot:
    """Разобранный снимок входов плана (``run_inputs``) — одна форма для всех версий.

    Поля v1: ``catalog``/``module_weights``/``cohort`` — ровно то, что нужно для
    пересборки плана без пакета. Поля v2 (``cycle_id``/``cohort_digest``/``seeds_digest``/
    ``previous_run_id``/``product_plan``/``state_schema_version``) добавлены, чтобы
    снимок позволял восстановить **цепочку циклов**, а не только каталог. У v1 они
    ``None`` — «не записано», а не ``0``/``""``: снимок v1 этих величин не нёс, и
    подставить их значило бы выдать догадку за записанный вход (A3).
    """

    snapshot_version: int
    run_id: str
    catalog: dict[str, CatalogRow]
    module_weights: dict[str, float]
    cohort: Cohort
    cycle_id: str | None = None
    cohort_digest: str | None = None
    seeds_digest: str | None = None
    previous_run_id: str | None = None
    product_plan: dict[str, Any] | None = None
    state_schema_version: str | None = None


#: Маркер «поле есть, но не того типа» — отличается от ``None`` («не записано»).
_INVALID_SNAPSHOT_FIELD: Any = object()


def _snapshot_text(data: Mapping[str, Any], key: str) -> Any:
    """Строковое поле снимка: ``None`` (не записано) либо ``str``; иначе маркер порчи."""
    value = data.get(key)
    if value is None:
        return None
    return value if isinstance(value, str) else _INVALID_SNAPSHOT_FIELD


def _plan_inputs_from_payload(
    data: Any, run_id: str
) -> PlanInputsSnapshot | None:
    """Валидирует разобранный снимок входов плана и восстанавливает его значения.

    Принимает **обе** версии формата (:data:`PLAN_INPUTS_SUPPORTED_VERSIONS`): v1 —
    по её собственным полям (новые остаются ``None``), v2 — по новым. Возвращает
    :class:`PlanInputsSnapshot` либо ``None`` при отсутствии/частичности/несовместимой
    версии снимка или ``run_id``-несоответствии. Несовпадение ``run_id`` трактуется как
    чужой снимок: значения не подставляются (не выдумываем).

    Единая точка валидации формата снимка (``snapshot_version``/``run_id`` + состав),
    общая для чтения из БД (``StorePort.read_plan_inputs``) и производного экспорта
    ``plan_inputs.json`` (:func:`_snapshot_plan_inputs`).
    """
    if not isinstance(data, Mapping):
        return None
    version = data.get("snapshot_version")
    if version not in PLAN_INPUTS_SUPPORTED_VERSIONS:
        return None
    if data.get("run_id") != run_id:
        return None
    raw_params = data.get("params")
    raw_cohort = data.get("cohort")
    if not isinstance(raw_params, list) or not isinstance(raw_cohort, Mapping):
        return None
    try:
        catalog = {
            row.param_id: row
            for row in (CatalogRow.model_validate(item) for item in raw_params)
        }
        cohort = Cohort.model_validate(raw_cohort)
    except (ValueError, TypeError):
        return None
    weights = data.get("renormalized_weights")
    if weights is None:
        weights = data.get("module_weights")
    if not isinstance(weights, Mapping):
        return None
    try:
        module_weights = {str(key): float(value) for key, value in weights.items()}
    except (TypeError, ValueError):
        return None

    # Поля v2: у снимка v1 их нет вовсе, поэтому «не записано» = ``None``. Тип проверяется
    # только у v2 — у v1 этих ключей быть не может, и подставлять их из чужих ключей нельзя.
    cycle_id: str | None = None
    cohort_digest: str | None = None
    seeds_digest: str | None = None
    previous_run_id: str | None = None
    state_schema_version: str | None = None
    product_plan: dict[str, Any] | None = None
    if version >= 2:
        cycle_id = _snapshot_text(data, "cycle_id")
        cohort_digest = _snapshot_text(data, "cohort_digest")
        seeds_digest = _snapshot_text(data, "seeds_digest")
        previous_run_id = _snapshot_text(data, "previous_run_id")
        state_schema_version = _snapshot_text(data, "state_schema_version")
        raw_plan = data.get("product_plan")
        if raw_plan is not None:
            if not isinstance(raw_plan, Mapping):
                return None
            product_plan = dict(raw_plan)
        if any(
            value is _INVALID_SNAPSHOT_FIELD
            for value in (
                cycle_id,
                cohort_digest,
                seeds_digest,
                previous_run_id,
                state_schema_version,
            )
        ):
            return None

    return PlanInputsSnapshot(
        snapshot_version=int(version),
        run_id=run_id,
        catalog=catalog,
        module_weights=module_weights,
        cohort=cohort,
        cycle_id=cycle_id,
        cohort_digest=cohort_digest,
        seeds_digest=seeds_digest,
        previous_run_id=previous_run_id,
        product_plan=product_plan,
        state_schema_version=state_schema_version,
    )


def _plan_inputs_sha256(payload: Mapping[str, Any]) -> str:
    """Детерминированный SHA-256 канонической JSON-формы снимка входов (P1/§13/R7).

    ``sort_keys=True``/``ensure_ascii=False`` — та же канонизация, что кладётся в
    ``run_inputs.payload_json``; повторная запись идентичного снимка даёт тот же хеш
    (diff-visible провенанс; каталог/когорта и так ``execution_versions``, поэтому
    нового версионного класса не возникает).
    """
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _snapshot_run_inputs(
    args: argparse.Namespace,
    run_id: str,
    *,
    inputs: RunInputs,
    run_plan: RunPlan,
    store: Any = None,
    cycle_id: str | None = None,
    cohort_digest: str | None = None,
    seeds_digest: str | None = None,
    previous_run_id: str | None = None,
    product_plan: Mapping[str, Any] | None = None,
) -> list[str]:
    """Пишет снимок входов прогона: в БД прогона (источник истины) и в ``runs/{run_id}/``.

    Состав:

    * ``run_inputs`` (БД, P1) — **источник истины** самодостаточности ``runs/{run_id}``:
      пост-хок команды восстанавливают каталог/веса/когорту без исходного пакета и без
      side-файла (``StorePort.save_plan_inputs``; дайджест — :func:`_plan_inputs_sha256`).
      Снимок **иммутабелен** (O11, 09 §3.3): идентичный повтор — no-op, расхождение
      ``payload_sha256`` — отказ (``PlanInputsConflict``), а не перезапись: расхождение
      означает другой прогон, и виден он в ``issues``, а не тишиной;
    * ``run.yaml`` — **иммутабельная копия** входа (байт-в-байт); существующий
      отличающийся файл **не** перезаписывается (иначе мутировал бы завершённый
      прогон), идентичный — оставляется как есть;
    * ``plan_inputs.json`` — **производный экспорт** того же снимка (детерминированный
      JSON, ``sort_keys=True``/``ensure_ascii=False``); читатели на него больше не
      опираются (источник — БД), но файл сохраняется как человекочитаемый артефакт и
      держится **той же формы**, что строка в БД (второго формата быть не должно).

    **Снимок другой версии формата не перезаписывается.** Если у ``run_id`` уже лежит
    снимок версии, отличной от :data:`PLAN_INPUTS_SNAPSHOT_VERSION` (например, v1 после
    подъёма формата до v2), запись **не выполняется**: ``payload_sha256`` заведомо другой,
    и «разрешить конфликт, если отличается только версия» было бы ослаблением O11 —
    снимок обязан остаться ровно тем, что записано первым. Факт называется видимой
    строкой ``snapshot: run_inputs_v{n}_preserved: …`` в ``issues``, а прогон
    продолжается (resume завершённого/прерванного прогона не ломается).

    :param cycle_id: эффективный цикл прогона (тот же, что вошёл в ``run_id``).
    :param cohort_digest: дайджест состава когорты (слагаемое ``run_id``).
    :param seeds_digest: дайджест эффективного seed-набора (слагаемое ``run_id``).
    :param previous_run_id: предшественник из lineage (``None`` — не записан).
    :param product_plan: сериализованный ``ProductPlanSet`` (mode/scope/reasons) — то же
        значение и та же форма, что уходят в ``runs.product_plan_json``; второй расчёт
        не заводится. ``None`` — план продуктов не резолвился в этом прогоне.

    Никогда не бросает исключений: любая ошибка записи возвращается строкой для
    ``instrumentation_errors`` прогона (сбой снимка не подменяет код возврата).
    """
    run_dir = Path(args.runs_dir) / run_id
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return [f"snapshot: не удалось создать каталог прогона {run_dir}: {_concise(exc)}"]

    payload = _plan_inputs_payload(
        run_id,
        inputs=inputs,
        run_plan=run_plan,
        cycle_id=cycle_id,
        cohort_digest=cohort_digest,
        seeds_digest=seeds_digest,
        previous_run_id=previous_run_id,
        product_plan=product_plan,
    )
    issues = _snapshot_run_yaml(args.run_yaml, run_dir)

    # O11 (09 §3.3/§14.2): сохранённый снимок — идентичность прогона. Сначала читаем его,
    # чтобы отличить «версионную границу формата» (запись не выполняется, факт назван) от
    # «того же формата, но другого содержимого» (конфликт дайджеста). Чтение best-effort:
    # не удалось — идём в запись, конфликт всё равно поймается ниже и ничего не перезапишет.
    stored: Any = None
    if store is not None:
        try:
            stored = store.read_plan_inputs(run_id)
        except Exception as exc:  # noqa: BLE001 — чтение best-effort, снимок не роняет прогон
            issues.append(
                f"snapshot: не удалось прочитать сохранённый снимок входов: {_concise(exc)}"
            )
    stored_version = (
        stored.get("snapshot_version") if isinstance(stored, Mapping) else None
    )
    if isinstance(stored_version, int) and stored_version != PLAN_INPUTS_SNAPSHOT_VERSION:
        issues.append(
            f"snapshot: run_inputs_v{stored_version}_preserved: run_id={run_id};"
            f" сохранён снимок формата {stored_version}, текущий"
            f" {PLAN_INPUTS_SNAPSHOT_VERSION} — не перезаписан (иммутабельность, O11)"
        )
        # Производный экспорт — та же форма, что в БД: пишем сохранённый снимок, а не
        # текущий, иначе файл и строка разошлись бы по версии формата.
        issues.extend(_snapshot_plan_inputs(run_dir, stored))
        return issues

    issues.extend(_snapshot_plan_inputs(run_dir, payload))
    if store is not None:
        try:
            store.save_plan_inputs(
                run_id, payload=payload, payload_sha256=_plan_inputs_sha256(payload)
            )
        except PlanInputsConflict as exc:
            # O11 (09 §3.3): снимок — идентичность прогона. Расхождение дайджеста не
            # перезаписывается и не подменяет код возврата: строка называет оба хеша и
            # решение (новый run_id), иначе завершённый прогон стал бы неотличим от
            # неисполнявшегося.
            issues.append(
                f"snapshot: run_inputs_conflict: run_id={exc.run_id};"
                f" сохранённый sha256={exc.stored_sha256};"
                f" предъявленный sha256={exc.incoming_sha256};"
                " снимок входов иммутабелен — нужен новый run_id, а не перезапись"
                " (O11, 09 §3.3)"
            )
            # Экспорт обязан описывать **сохранённый** снимок, а не предъявленный:
            # иначе файл утверждал бы о прогоне то, чего в БД нет.
            if isinstance(stored, Mapping):
                issues.extend(_snapshot_plan_inputs(run_dir, stored))
        except Exception as exc:  # noqa: BLE001 — самодостаточность best-effort
            issues.append(
                f"snapshot: не удалось записать входы плана в БД прогона: {_concise(exc)}"
            )
    return issues


def _snapshot_run_yaml(source: str | Path, run_dir: Path) -> list[str]:
    """Копирует ``run.yaml`` в каталог прогона как иммутабельный вход (``03`` §A.2)."""
    target = run_dir / RUN_YAML_FILENAME
    try:
        data = Path(source).read_bytes()
    except OSError as exc:
        return [f"snapshot: не удалось прочитать run.yaml {source}: {_concise(exc)}"]

    if target.exists():
        try:
            existing = target.read_bytes()
        except OSError as exc:
            return [f"snapshot: не удалось прочитать существующий {target}: {_concise(exc)}"]
        if existing != data:
            return [
                f"snapshot: {target} уже существует и отличается от входа — "
                "не перезаписан (иммутабельность, 03 §A.2)"
            ]
        return []  # идентичный вход: идемпотентный resume, перезапись не требуется

    try:
        target.write_bytes(data)
    except OSError as exc:
        return [f"snapshot: не удалось записать {target}: {_concise(exc)}"]
    return []


def _snapshot_plan_inputs(run_dir: Path, payload: Mapping[str, Any]) -> list[str]:
    """Пишет ``plan_inputs.json`` — производный экспорт снимка плановых входов (P1).

    Источник истины — таблица ``run_inputs`` в БД прогона; файл лишь удобен человеку и
    больше не читается пост-хок командами. Поэтому ``payload`` — **тот же** снимок, что
    лежит (или ляжет) в БД: при сохранённом снимке другой версии формата
    (:func:`_snapshot_run_inputs`) сюда передаётся именно он, а не текущий — иначе файл и
    строка разошлись бы по версии, то есть завёлся бы второй формат.
    """
    target = run_dir / PLAN_INPUTS_FILENAME
    try:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        target.write_text(text, encoding="utf-8")
    except (OSError, TypeError, ValueError) as exc:
        return [f"snapshot: не удалось записать {target}: {_concise(exc)}"]
    return []


def _plan_inputs_payload(
    run_id: str,
    *,
    inputs: RunInputs,
    run_plan: RunPlan,
    cycle_id: str | None = None,
    cohort_digest: str | None = None,
    seeds_digest: str | None = None,
    previous_run_id: str | None = None,
    product_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Сериализует минимальный снимок входов плана (без всего :class:`RunInputs`).

    Ключи (``snapshot_version = 2``, 09 §14.2):

    * ``snapshot_version`` — версия формата;
    * идентичность прогона — ``run_id``/``case_id``/``package_id``/``package_version``,
      ``anchor``/``products``;
    * то, что нужно для пересборки без пакета — ``params``
      (``CatalogRow.model_dump(mode="json")``), ``module_weights``/
      ``renormalized_weights`` и ``cohort`` (``Cohort``);
    * то, что нужно для восстановления **цепочки циклов** — ``cycle_id`` (эффективный
      цикл прогона, тот же материал, что у ``run_id``), ``cohort_digest``/``seeds_digest``
      (слагаемые ``run_id``), ``previous_run_id`` (предшественник из lineage; ``None`` —
      не записан), ``product_plan`` (сериализованный ``ProductPlanSet``: mode/scope/
      reasons по продуктам — то же значение и форма, что уходят в
      ``runs.product_plan_json``), ``state_schema_version``
      (:data:`core.store.state_sqlite.STATE_SCHEMA_VERSION` — схема ``state.db``, в
      которой живёт цепочка).

    Ключей ``params``/``cohort`` достаточно для :func:`_plan_inputs_from_payload`; прочие —
    провенанс, а не «истина о продукте». Отсутствующее значение остаётся ``None``: снимок
    не выдумывает ни цикла, ни дайджеста (A3).
    """
    return {
        "snapshot_version": PLAN_INPUTS_SNAPSHOT_VERSION,
        "run_id": run_id,
        "case_id": run_plan.cohort.case_id,
        "package_id": inputs.run.package,
        "package_version": inputs.package.package_version,
        "anchor": run_plan.anchor,
        "products": list(run_plan.products),
        "params": [row.model_dump(mode="json") for row in run_plan.params],
        "module_weights": dict(run_plan.module_weights),
        "renormalized_weights": dict(run_plan.renormalized_weights),
        "cohort": run_plan.cohort.model_dump(mode="json"),
        # v2: цепочка циклов. Значения приходят из той же идентичности, что уже вошла в
        # ``run_id`` (второго расчёта нет), план продуктов — то же значение, что уходит в
        # ``runs.product_plan_json`` (``build_manifest`` читает тот же атрибут ``plan``).
        "cycle_id": cycle_id,
        "cohort_digest": cohort_digest,
        "seeds_digest": seeds_digest,
        "previous_run_id": previous_run_id,
        "product_plan": (
            dict(product_plan)
            if product_plan is not None
            else getattr(run_plan, "product_plan", None)
        ),
        "state_schema_version": STATE_SCHEMA_VERSION,
    }


def _print_sensitivity(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``sensitivity`` (оси + устойчивость)."""
    pairs = ", ".join(f"{a}|{b}" for a, b in payload["unstable_pairs"]) or "—"
    lines = [
        f"run_id: {payload['run_id']}",
        f"stable: {'yes' if payload['stable'] else 'no'}",
        f"requested_ranking: {payload['requested_ranking']}",
        f"computed_ranking: {payload['computed_ranking']}",
        f"unstable_pairs: {pairs}",
    ]
    for axis, data in sorted(payload["per_axis"].items()):
        values = ", ".join(str(value) for value in data.get("values", []))
        stable = "yes" if data.get("stable") else "no"
        lines.append(f"axis {axis}: values=[{values}] stable={stable}")
    for note in payload.get("notes", ()):
        lines.append(f"note: {note}")
    sys.stdout.write("\n".join(lines) + "\n")


def _diagnostic_gateway(
    config: DeployConfig,
    *,
    transport: TransportFn | None,
    factory: GatewayFactory | None,
) -> tuple[ContentGateway, dict[str, Mapping[str, Any]]]:
    """Собирает gateway для ``deploy-check`` и, при возможности, записывает конверты.

    ``raw_envelopes`` пополняется только для внедрённого ``transport`` (обёртка
    фиксирует ответ до клиентской валидации) — это позволяет проверить конверт
    против контракта. Для реального HTTP подтверждённый конверт недоступен (gateway
    валидирует его сам); для ``factory`` — ответственность фабрики.
    """
    raw: dict[str, Mapping[str, Any]] = {}
    if factory is not None:
        return factory(config), raw
    if transport is not None:

        def _recording(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
            response = transport(op, envelope)
            raw[op] = response
            return response

        return build_gateway(config, transport=_recording), raw
    return build_gateway(config), raw


def _seam_diagnostics(
    config: DeployConfig,
    gateway: ContentGateway,
    raw_envelopes: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Прогоняет health/capabilities и собирает отчёт диагностики шва.

    Сбои транспорта/схемы ловятся в ``GatewayError`` и попадают в ``issues`` (не
    роняют команду): команда возвращает ненулевой код, но отчёт остаётся машинночитаемым.

    Дополнительно выполняется **неразрушающий** зонд crawl-операций
    (:func:`_crawl_probe`): его результат кладётся в ``crawl`` и описывает доступность
    очереди краула edge, но **не** влияет на ``ok``/код возврата (аддитивно, 07 §3).
    """
    issues: list[str] = []
    health: Any = None
    capabilities: Any = None
    try:
        health = gateway.call("health", {})
    except GatewayError as exc:
        issues.append(f"health: {type(exc).__name__}: {_concise(exc)}")
    try:
        capabilities = gateway.call("capabilities", {})
    except GatewayError as exc:
        issues.append(f"capabilities: {type(exc).__name__}: {_concise(exc)}")

    if health is not None:
        issues.extend(_validate_health_payload(health))
    if capabilities is not None:
        issues.extend(_validate_capabilities_payload(capabilities))

    for op in ("health", "capabilities"):
        envelope = raw_envelopes.get(op)
        if envelope is None:
            continue
        for violation in validate_envelope_shape(envelope, DEFAULT_SCHEMA_VERSION):
            issues.append(f"{op}: конверт {violation.code}: {violation.message}")

    fields = manifest_deploy_fields(
        config, capabilities if isinstance(capabilities, Mapping) else None
    )
    return {
        "ok": not issues,
        "split": True,
        "profile": config.profile,
        "base_url": config.edge.base_url,
        "deploy_profile": fields["deploy_profile"],
        "edge_schema_version": fields["edge_schema_version"],
        "health": health,
        "capabilities": capabilities,
        "edge_versions": {
            key: capabilities.get(key)
            for key in ("extractor_version", "crawler_version", "search_engine")
        }
        if isinstance(capabilities, Mapping)
        else {},
        "crawl": _crawl_probe(gateway),
        "issues": issues,
    }


#: Фиктивный ``job_id`` для безопасного зонда ``crawl/status`` (только чтение, без мутаций).
_CRAWL_PROBE_JOB_ID = "__deploy_check_probe__"


def _crawl_probe(gateway: ContentGateway) -> dict[str, Any]:
    """Неразрушающе проверяет доступность crawl-операций edge (Phase 3.4b).

    **Выбор зонда.** ``/v1/capabilities`` машиночитаемо crawl-ops не рекламирует (схема
    фиксирована, 07 §3), поэтому доступность выводится поведенчески: вызывается
    **только чтение** ``crawl/status`` с заведомо несуществующим ``job_id``. Задание не
    подаётся, состояние edge не мутируется.

    Интерпретация ответа:

    * транспортный сбой (в т.ч. HTTP 404 на неизвестном маршруте) → edge не публикует
      crawl-операции;
    * ``status=error`` с признаком неизвестного задания → операция маршрутизируется и
      планировщик ответил (``available``);
    * ``status=error`` «crawl scheduler is not available …» → операция есть, планировщик
      не сконфигурирован (``unavailable``);
    * ``status=error`` «неизвестная операция» → edge не публикует crawl-операции;
    * ``status=ok`` (payload) → планировщик ответил успешно (``available``).

    :returns: ``{"available": bool, "reason": str | None}`` — причина только при недоступности.
    """
    try:
        gateway.call("crawl/status", {"job_id": _CRAWL_PROBE_JOB_ID})
    except GatewayStatusError as exc:
        message = _concise(exc)
        lowered = message.lower()
        if "неизвестн" in lowered and "задани" in lowered:
            # Операция маршрутизировалась, планировщик отверг несуществующее задание —
            # очередь краула жива (liveness-доказательство без мутаций).
            return {"available": True, "reason": None}
        if "scheduler" in lowered:
            return {
                "available": False,
                "reason": "crawl-планировщик не сконфигурирован на edge",
            }
        if "неизвестная операция" in lowered:
            return {
                "available": False,
                "reason": "edge не публикует crawl-операции",
            }
        return {"available": False, "reason": message}
    except GatewayTransportError:
        return {
            "available": False,
            "reason": "edge не публикует crawl-операции (транспорт/404)",
        }
    except GatewayError as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {_concise(exc)}"}
    return {"available": True, "reason": None}


def _validate_health_payload(payload: Any) -> list[str]:
    """Форма payload ``GET /v1/health`` (``07`` §3): ``{status: <непустая строка>}``."""
    if not isinstance(payload, Mapping):
        return ["health: payload должен быть объектом"]
    status = payload.get("status")
    if not isinstance(status, str) or not status:
        return ["health: обязательное поле status — непустая строка (07 §3)"]
    return []


def _validate_capabilities_payload(payload: Any) -> list[str]:
    """Форма payload ``GET /v1/capabilities`` (``07`` §3): версии + форматы edge."""
    if not isinstance(payload, Mapping):
        return ["capabilities: payload должен быть объектом"]
    issues: list[str] = []
    for field in ("extractor_version", "crawler_version", "search_engine"):
        value = payload.get(field)
        if value is not None and not isinstance(value, str):
            issues.append(f"capabilities: поле {field} должно быть строкой или null")
    formats = payload.get("supported_formats")
    if formats is not None and (
        not isinstance(formats, list) or not all(isinstance(item, str) for item in formats)
    ):
        issues.append("capabilities: supported_formats должен быть списком строк (07 §3)")
    return issues


def _emit_deploy_check(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    """Печатает отчёт ``deploy-check`` как JSON (``--json``) или человеку."""
    if getattr(args, "json", False):
        _print_json(payload)
        return
    lines = [
        f"profile: {payload['profile']}",
        f"split: {'yes' if payload['split'] else 'no'}",
    ]
    if payload["split"]:
        lines.append(f"base_url: {payload['base_url']}")
        lines.append(f"edge_schema_version: {payload['edge_schema_version']}")
        lines.append(f"health: {_short_json(payload.get('health'))}")
        lines.append(f"capabilities: {_short_json(payload.get('capabilities'))}")
        crawl = payload.get("crawl")
        if isinstance(crawl, Mapping):
            if crawl.get("available"):
                lines.append("crawl: available")
            else:
                reason = crawl.get("reason") or "недоступно"
                lines.append(f"crawl: unavailable ({reason})")
    else:
        lines.append(f"note: {payload['note']}")
    for issue in payload.get("issues", ()):
        lines.append(f"issue: {issue}")
    for warning in payload.get("warnings", ()):
        lines.append(f"warning: {warning}")
    lines.append(f"ok: {'yes' if payload['ok'] else 'no'}")
    sys.stdout.write("\n".join(lines) + "\n")


def _short_json(value: Any) -> str:
    """Однострочное JSON-представление значения для человекочитаемого отчёта."""
    if value is None:
        return "—"
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _phases_until(until: str | None) -> list[str]:
    """Список фаз от начала до ``until`` включительно (все — при ``None``)."""
    if until is None:
        return list(PHASES)
    return list(PHASES[: PHASES.index(until) + 1])


def _ensure_run(store: Any, run_id: str, inputs: RunInputs) -> None:
    """Создаёт строку прогона, если её ещё нет (сохраняет durable-состояние resume).

    ``SqliteStore.create_run`` использует ``INSERT OR REPLACE`` и обнулил бы ``runs.state``;
    поэтому вызов обёрнут проверкой существования строки — повторный ``run`` не теряет
    прогресс (P4/``02`` §A.1).
    """
    row = store.conn.execute(
        "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if row is None:
        store.create_run(run_id, inputs.run)

def _persist_llm_scenarios(store: Any, run_id: str, declared: Any) -> list[str]:
    """Пишет объявленную таблицу ``scenario → модель`` (I21) в строку прогона (§7.4).

    Источник — **только** :attr:`DeclaredExecutionAxes.llm_scenarios`, то есть ровно те
    строки, что уже вошли в ``execution_contract_digest``. Второй формулы нет: контракт
    и таблица ``llm_scenarios`` не могут разойтись, а ``--llm-slot``/``run.yaml:llm_slot``
    остаются единственным входом (env ``COHOUND_LLM_*`` по-прежнему не читается — иначе
    два хоста с одним ``run.yaml`` получили бы разный маршрут).

    Строка ``llm_scenarios`` — это durable-факт для ``hound report``/``hound diff``:
    без неё смена модели под сценарий (§13, граница класса ``execution_versions``)
    видна только по дайджесту контракта, но не по имени модели.

    Сбой записи **не роняет прогон** (как и снимок входов): возвращается текст ошибки
    для ``instrumentation_errors``. Upsert идемпотентен, поэтому повторный ``run``
    (resume) переписывает те же строки.

    :returns: список ошибок инструментирования (пустой — запись удалась).
    """
    rows = getattr(declared, "llm_scenarios", ()) or ()
    if not rows:
        # Пустая таблица — честное «маршрут не объявлен»: слот неактивен, оценщик не
        # поднимается. Выдумывать строку нельзя (зеркалит ``_resolve_llm_scenarios``).
        return []
    try:
        bindings = [
            ScenarioBinding(
                run_id=run_id,
                scenario=LLMScenario(row["scenario"]),
                model_id=row["model_id"],
                model_version=row.get("model_version") or None,
                endpoint_kind=EndpointKind(row.get("endpoint_kind") or EndpointKind.API),
                web_tool=bool(row.get("web_tool", False)),
                decoder=row.get("decoder") or None,
                seed=row.get("seed"),
                temperature=row.get("temperature"),
                constrained_decoding=row.get("constrained_decoding"),
                # V5/V6: условия генерации. Строка объявлена таблицей
                # ``_resolve_llm_scenarios`` и обязана дойти до манифеста целиком —
                # иначе «куда ушёл корпус» и «обрезали ли ответ» остаются догадкой.
                endpoint_host=row.get("endpoint_host"),
                max_tokens=row.get("max_tokens"),
            )
            for row in rows
        ]
        store.save_llm_scenarios(bindings)
    except Exception as exc:  # noqa: BLE001 — durable-факт не роняет прогон
        return [f"llm_scenarios: {type(exc).__name__}: {_concise(exc)}"]
    return []


def _resolve_plan(
    args: argparse.Namespace, inputs: RunInputs, run_id: str
) -> RunPlan:
    """Строит план прогона (``plan_factory`` — тестовый шов; иначе ``plan``).

    ``run_id`` переиспользуется выведенный вызывающим: ``plan()`` не пересчитывает его
    (детерминизм resume, §13).
    """
    factory = getattr(args, "plan_factory", None)
    if factory is not None:
        return factory(inputs, run_id=run_id)
    return build_plan(inputs, run_id=run_id)


def _resolve_passports_dir(args: argparse.Namespace, paths: Any = None) -> str:
    """Каталог паспортов: ``--passports-dir`` > resolved ``paths.passports`` > ``passports``.

    Единственная формула для всех потребителей (фазы Ш4/Ш5 и дайджест профиля G1/G2):
    второй формулы быть не должно, иначе аттестация профиля считалась бы по другому
    набору паспортов, чем тот, что читают фазы.
    """
    passports_dir = getattr(args, "passports_dir", None)
    if passports_dir is None and paths is not None:
        passports_dir = str(paths.passports)
    if passports_dir is None:
        passports_dir = "passports"
    return str(passports_dir)


def _profile_material_for_run(
    args: argparse.Namespace,
    inputs: RunInputs,
    plan: RunPlan,
    *,
    cohort_digest: str,
    seeds_digest: str,
) -> dict[str, Any]:
    """Материал дайджеста профиля пакета-якоря для текущего прогона.

    Единственная точка сборки материала в composition root: и аттестация (``hound package
    profile attest``), и проверка гейта обязаны видеть **один** набор
    компонентов, иначе аттестованное и проверяемое разойдутся.

    Паспорта читаются здесь же (:func:`core.config.passports.passport_content_digest` по
    каждому продукту плана): «паспорта фиксируются на цикл», поэтому их правка меняет
    профиль. Отсутствие паспорта у продукта — наблюдаемое ``None``, а не пропуск
    компонента: появление паспорта обязано требовать пере-аттестации.
    """
    paths = getattr(args, "paths", None)
    if paths is None:
        try:
            paths = _resolve_paths(args)
        except Exception:  # noqa: BLE001 — раскладка опциональна, дефолт ниже
            paths = None
    passports_dir = _resolve_passports_dir(args, paths)
    return profile_material(
        case_id=inputs.run.case_id,
        package_id=resolve_package_id(inputs),
        anchor=inputs.run.anchor,
        cohort_digest=cohort_digest,
        products=plan.products,
        focus_modules=plan.modules,
        focus_params=inputs.run.focus.params,
        seeds_digest=seeds_digest,
        roles=plan.roles,
        passports=passport_content_digests(plan.products, passports_dir),
    )


#: Метка ``run_id`` для плана, который строится ради **профиля**, а не прогона.
#: ``run_id`` в материал дайджеста не входит (:data:`PROFILE_MATERIAL_KEYS`), поэтому
#: подставляется явная метка: выдуманная идентичность прогона была бы ложным следом.
_PROFILE_PLAN_LABEL = "profile-attest"


def _profile_material_for_package(args: argparse.Namespace) -> dict[str, Any]:
    """Материал дайджеста профиля по каталогу пакета (путь CLI, а не прогона).

    Единственная точка сборки материала **вне** прогона: ``hound package profile
    attest`` обязан видеть тот же материал, что увидит гейт G1/G2, иначе аттестованное
    и проверяемое разойдутся. Последовательность — как у ``plan``/``run``:
    ``load_inputs`` → ``load_effective_seeds`` → ``cohort_digest``/``seeds_digest`` →
    ``build_plan`` → :func:`_profile_material_for_run`. Второй формулы дайджеста нет.

    :param args: аргументы команды; читаются ``package``, ``run_yaml`` (по умолчанию
        ``{package}/run.yaml``), ``paths`` и ``passports_dir``.
    :raises CliError: в пакете нет ``run.yaml`` — профиль (когорта, объём, паспорта) не
        из чего собрать; пустой материал аттестовал бы несуществующий профиль.

    Операторский ``--urls`` здесь не поддерживается: он меняет ``seeds_digest``, и
    аттестация такого прогона — отдельное решение, а не тот же профиль.
    """
    package = str(args.package)
    run_yaml = getattr(args, "run_yaml", None) or str(Path(package) / RUN_YAML_FILENAME)
    if not Path(run_yaml).is_file():
        raise CliError(
            f"в пакете нет {RUN_YAML_FILENAME}: {run_yaml} — профиль (когорта, объём, "
            "паспорта) не из чего собрать",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    inputs = load_inputs(run_yaml, package)
    profiles = getattr(inputs, "domain_profiles", None) or {}
    seed_config, _ = load_effective_seeds(
        package, inputs.run.products, profiles=profiles
    )
    cohort = cohort_digest(
        inputs.cohort,
        case_id=inputs.run.case_id,
        cohort_ref=getattr(inputs.run, "cohort_ref", None),
    )
    seeds = seeds_digest(seed_config)
    run_plan = build_plan(inputs, run_id=_PROFILE_PLAN_LABEL)
    return _profile_material_for_run(
        args, inputs, run_plan, cohort_digest=cohort, seeds_digest=seeds
    )


def _profile_gate_resolver(
    args: argparse.Namespace,
    inputs: RunInputs,
    plan: RunPlan,
    *,
    cohort_digest: str,
    seeds_digest: str,
    state: Any,
) -> Callable[[Gate], ProfileGateResult] | None:
    """Резолвер гейтов G1/G2 по аттестации **профиля** пакета-якоря (или ``None``).

    Профиль квази-константен (правки 2–5 раз в год при еженедельных прогонах), поэтому
    человек аттестует его содержимое один раз, а прогоны с тем же содержимым не требуют
    повторного решения. Материал собирает :func:`_profile_material_for_run`.

    :param state: открытый ``ProductStatePort`` пакета (``state.db``). ``None`` — аттестации
        недоступны (``--dry-run``/``--until plan`` или пакет не каталог): резолвер не
        строится, и гейт ведёт себя как раньше — только ``gate_decisions``. Молчаливой
        подмены «аттестация есть» нет.
    :returns: замыкание ``Gate → ProfileGateResult`` для
        :func:`core.pipeline.runner._gate_phase`.
    """
    if state is None:
        return None
    material = _profile_material_for_run(
        args, inputs, plan, cohort_digest=cohort_digest, seeds_digest=seeds_digest
    )
    return make_profile_gate(state, material)


def _gate_subject_resolver(
    args: argparse.Namespace,
    inputs: RunInputs,
    plan: RunPlan,
    *,
    cohort_digest: str,
    seeds_digest: str,
    store: StorePort,
    runs_dir: str,
    rnd_endpoint: bool | None = None,
) -> Callable[[Gate], GateSubject | None]:
    """Резолвер **предмета решения** для всех четырёх гейтов (S9, точки интеграции).

    Предмет — то, о чём спрашивают решение; его отпечаток (``subject_digest``) обязан
    вернуться в решении, иначе гейт не пройден. Единый резолвер на все гейты — чтобы
    «что утверждали» и «что проверяем» собирались одной формулой:

    * ``G1``/``G2`` — материал профиля пакета-якоря (тот же, что у аттестации профиля):
      per-run решение и аттестация профиля говорят об **одном** предмете;
    * ``G3`` — очередь ревью (Ш7): внешний ревьюер утверждает конкретный её состав;
    * ``G4`` — Coverage/Capability/Comparability/ранжирование и содержимое публикуемых
      артефактов: внешняя система утверждает публикацию именно этого.

    Резолвер **ленивый**: предмет считается в момент фазы-гейта, а не при сборке
    конвейера (``compute`` для G4 читает ячейки, которых до Ш6/Ш8 ещё нет).

    :param rnd_endpoint: объявленное исключение стенда (§10) — тот же вход, что у фазы
        Ш8: предмет G4 обязан описывать **тот же** профиль публикуемости, который будет
        опубликован (иначе внешняя система утверждала бы не то, что выходит).
    """
    outputs_dir = Path(runs_dir) / plan.run_id / "outputs"

    def _resolve(gate: Gate) -> GateSubject | None:
        if gate in (Gate.G1, Gate.G2):
            material = _profile_material_for_run(
                args,
                inputs,
                plan,
                cohort_digest=cohort_digest,
                seeds_digest=seeds_digest,
            )
            return GateSubject(
                gate=gate,
                digest=material_digest(material),
                material=material,
                export={"profile": material},
            )
        if gate is Gate.G3:
            items = review_queue(store, plan.run_id)
            return review_subject(items, run_id=plan.run_id)
        if gate is Gate.G4:
            bundle = bundle_for_run(
                store,
                run_id=plan.run_id,
                plan=plan,
                inputs=inputs,
                rnd_endpoint=rnd_endpoint,
            )
            return publication_subject(
                bundle,
                run_id=plan.run_id,
                cells=store.iter_cells(plan.run_id),
                outputs_dir=outputs_dir,
            )
        return None

    return _resolve


def _gate_runtime(
    args: argparse.Namespace,
    inputs: RunInputs,
    plan: RunPlan,
    *,
    cohort_digest: str,
    seeds_digest: str,
    state: Any,
    store: StorePort,
    rnd_endpoint: bool | None = None,
) -> GateRuntime:
    """Собирает :class:`~core.pipeline.runner.GateRuntime` для фаз-гейтов.

    Один объект на прогон: предмет решения (все гейты), аттестация профиля (G1/G2),
    политика из ``run.yaml:gates`` (``waived`` — только G3/G4), каталог прогонов для
    запроса на решение (``runs/{run_id}/gates/{gate}-request.json``).

    :param rnd_endpoint: объявленное исключение стенда (§10) — входит в профиль
        публикуемости, то есть и в предмет G4. Один вход с фазой Ш8: гейт утверждает ровно
        тот профиль, который публикуется.
    """
    runs_dir = str(getattr(args, "runs_dir", None) or "runs")
    return GateRuntime(
        subject=_gate_subject_resolver(
            args,
            inputs,
            plan,
            cohort_digest=cohort_digest,
            seeds_digest=seeds_digest,
            store=store,
            runs_dir=runs_dir,
            rnd_endpoint=rnd_endpoint,
        ),
        profile_gate=_profile_gate_resolver(
            args,
            inputs,
            plan,
            cohort_digest=cohort_digest,
            seeds_digest=seeds_digest,
            state=state,
        ),
        policies=dict(getattr(inputs.run, "gates", None) or {}),
        runs_dir=runs_dir,
    )


def _load_registry_specs(
    args: argparse.Namespace, inputs: RunInputs | None
) -> dict[str, RegistryConfig]:
    """Загружает package-scoped спецификации реестров из ``package.registries`` (E2, 03 §D.4).

    Composition root (Z0) — единственное место загрузки спецификаций; pipeline получает уже
    готовый mapping фетчеров (``Ports.registry_fetchers``). Пакет без объявленных реестров
    (или запуск без ``--package``) → ``{}``: реестры опциональны. Объявленный, но
    нечитаемый/отсутствующий реестр — **понятная ошибка CLI**, а не молчаливая частичная
    проводка (объявленный реестр без данных недопустим).

    **``--package`` без ``inputs`` — не «пусто», а отказ.** Раньше этот случай тоже давал
    ``{}``, и это было хуже ошибки: объявленные реестры молча не доезжали до волны Ш3, а
    ``registry_spec_digest`` становился ``None``. Дайджест входит в условия измерения, так
    что пустой ответ на этом пути подменял бы идентичность прогона (``run_id``) тишиной.
    Проверить объявления без ``inputs`` нельзя — значит, ответ «не знаю» обязан быть
    видимым. Законные вызывающие передают ``inputs`` (``run``/``crawl``), а
    ``resolve_declared_axes`` типизирован ``RunInputs`` и этого случая не создаёт.

    :raises CliError: объявленная спецификация реестра отсутствует/невалидна; либо
        ``--package`` задан, а входы пакета не загружены (внутренняя несогласованность).
    """
    package_dir = getattr(args, "package", None)
    if not package_dir:
        return {}
    if inputs is None:
        raise CliError(
            f"реестры пакета {package_dir!r}: входы пакета не загружены, поэтому "
            "объявленные реестры проверить нечем — спецификации не загружаются, а "
            "registry_spec_digest остался бы пустым (условие измерения подменялось бы "
            "тишиной). Передайте inputs (run/crawl) или уберите --package",
            code="registries",
            exit_code=EXIT_USAGE,
        )
    declared = list(getattr(getattr(inputs, "package", None), "registries", None) or [])
    if not declared:
        return {}
    try:
        return load_package_registry_configs(package_dir, declared)
    except RegistryConfigError as exc:
        raise CliError(
            f"реестры пакета: {exc}", code="registries", exit_code=EXIT_USAGE
        ) from exc


def resolve_content_cache(
    args: argparse.Namespace, *, no_cache: bool = False
) -> ContentAddressedCache | None:
    """R7-кэш прогона по resolved-раскладке (``paths.cache``); ``None`` — без кэша.

    Единственная точка построения объекта: им пользуются и порты (краулеры, поисковый
    адаптер), и SERP-контур. ``no_cache`` (``--from-scratch``) — единственный
    документированный способ обхода кэша; без resolved-раскладки (тесты/``--runs-dir``
    без ``paths``) кэш не строится.
    """
    if no_cache:
        return None
    paths = getattr(args, "paths", None)
    return ContentAddressedCache(paths.cache) if paths is not None else None

def _resolve_ports(
    args: argparse.Namespace,
    deploy_config: DeployConfig | None,
    *,
    inputs: RunInputs | None = None,
    no_cache: bool = False,
    store: StorePort | None = None,
    run_id: str | None = None,
) -> Ports:
    """Собирает порты профиля (``ports`` — тестовый шов; иначе :func:`build_adapters`).

    Для ``split`` используются инъектируемые швы (``deploy_transport``/``gateway_factory``),
    поэтому прогон в тестах не требует сокета. Корень индексного хранилища берётся из
    resolved-раскладки (``paths.index_store``, ``03`` §A.1), чтобы индекс-контур писался
    в настроенный каталог.

    **R7-кэш и домен-профили.** При наличии resolved-раскладки (``args.paths``) строится
    глобальный content-addressed кэш :class:`ContentAddressedCache` от ``paths.cache``
    (``03`` §A.1); ``inputs.domain_profiles`` (``domain_profiles.yaml`` пакета, §D.2)
    протягиваются в локальный краулер. ``inputs=None`` сохраняет прежнее поведение
    (домен-профили не передаются) — обратная совместимость вызывающих.

    **Phase 4 ``--from-scratch``.** ``no_cache=True`` собирает краулеры **без** R7-кэша
    (``cache=None``) — единственный документированный способ обхода кэша: живой фетч
    игнорирует дедупликацию идентичных байтов. Семантика — «сброс + живой ре-фетч».

    **Оценщик (Ш6, I18).** При наличии deploy-конфига оценщик собирается
    :func:`build_evaluator_llm` из активного LLM-слота (``deploy.llm`` + ``--llm-slot``).
    Если у слота пустой ``base_url`` (дефолт) — билдер вернёт ``None``: поведение
    прежнее (оценщик не поднимается). Профиль ``split`` получает ``evaluator`` как
    сегодня (remote-порты его не используют).

    **Ретривал (P4a, I17).** ``package`` разрешается :func:`resolve_package_id`
    (``run.package`` — идентичность пакета, а не ``run_id``), а открытый ``store``
    протягивается в :func:`build_adapters` как источник чанков (``seed_lexical_index``)
    и резолвер метаданных (``chunk_lookup=store.get_chunk``). Без ``store`` оба плеча
    остаются ``None`` (явное отсутствие, N3), и Ш6 не сможет исполнять R0.

    **Resume.** Если ``run_id`` известен и в ``runs`` уже записан ``index_build_id``,
    он передаётся билдерам как явная сборка: на resume фаза корпуса короткозамкнута,
    и пост-корпусная перепривязка не выполняется — без этого сборка была бы «угадана»
    или неоднозначна.

    **Реестры (E2).** Package-scoped спецификации (``packages/{case_id}/registries/*.yaml``,
    объявленные в ``package.yaml: registries``) загружаются здесь (:func:`_load_registry_specs`)
    и передаются в :func:`build_adapters`, который собирает mapping ``Ports.registry_fetchers``
    и дайджест спецификаций. Так реестровая волна Ш3 перестаёт быть инертной. Объявленный
    несобираемый реестр — понятная ошибка CLI (не молчаливая частичная проводка).

    :param no_cache: Phase 4 — не строить/не передавать R7-кэш (``--from-scratch``).
    :param store: открытое хранилище прогона (то же, что получит ``Runner``); ``None`` —
        ретривал не собирается (поведение прежнее).
    :param run_id: прогон для чтения ``runs.index_build_id`` (resume); ``None`` — не читаем.
    """
    injected = getattr(args, "ports", None)
    if injected is not None:
        return injected
    gateway_factory = getattr(args, "gateway_factory", None)
    gateway = (
        gateway_factory(deploy_config)
        if gateway_factory is not None and deploy_config is not None
        else None
    )
    paths = getattr(args, "paths", None)
    index_root = str(paths.index_store) if paths is not None else "index_store"
    cache = resolve_content_cache(args, no_cache=no_cache)
    evaluator = (
        build_evaluator_llm(
            deploy_llm=deploy_config.llm,
            slot=getattr(args, "llm_slot", None),
            run_slot=getattr(getattr(inputs, "run", None), "llm_slot", None),
            # §7.4/I21: модель оценщика — объявленный вход. ``--llm-model`` питает и runtime,
            # и ``llm_scenarios`` (execution_axes) — один вход, две проекции, без расхождения.
            model_id=getattr(args, "llm_model", None),
        )
        if deploy_config is not None
        else None
    )
    # E2: package-scoped спецификации реестров → mapping фетчеров в ``build_adapters``
    # (``local``: GenericRegistryFetcher; ``split``: RemoteRegistry) + дайджест в манифест.
    # Объявленный несобираемый реестр — явная ошибка (не молчаливая частичная проводка).
    registry_specs = _load_registry_specs(args, inputs)
    package = resolve_package_id(inputs) if inputs is not None else None
    # Resume: composition id уже записан пост-корпусной перепривязкой в предыдущем
    # процессе. Чтение провенанса не роняет сборку портов (неизвестный прогон → ``None``).
    index_build_id: str | None = None
    if store is not None and run_id:
        try:
            ref = store.read_run_index_ref(run_id)
        except Exception:  # noqa: BLE001 — провенанс не роняет сборку портов
            ref = None
        index_build_id = getattr(ref, "index_build_id", None)
    # Наблюдаемость выбора движка краула (03 §D.6): при известном прогоне строки идут в
    # `runs/{run_id}/logs/wiring.jsonl` — в тот же корень, что и логи фаз (`--runs-dir`
    # оператора приоритетнее resolved-раскладки). Движок — commodity-слой (A2) и не входит
    # в execution_contract_digest, поэтому без лога подмена движка не оставляет следа.
    logs_root = getattr(args, "runs_dir", None)
    if logs_root is None and paths is not None:
        logs_root = paths.runs
    engine_log = (
        PhaseLogger(run_id, "wiring", logs_root)
        if run_id and logs_root is not None
        else None
    )
    # §7.4/I21: идентичность оценщика — объявленный вход, а env-настройки модели/endpoint —
    # нет. Расхождение не должно быть молчаливым (эти переменные живут в runbook'ах):
    # пишем по строке на каждое расхождение в тот же ``wiring.jsonl``, что и выбор движка.
    if engine_log is not None and deploy_config is not None:
        for notice in llm_env_notices(
            deploy_llm=deploy_config.llm,
            slot=getattr(args, "llm_slot", None),
            run_slot=getattr(getattr(inputs, "run", None), "llm_slot", None),
        ):
            engine_log.warn(notice, event="llm_env_note")
        # D-8: профиль ``split-hardened`` ничего не добавляет сверх ``split`` — пишем в
        # тот же журнал, чтобы «хардненинг» не остался только в имени профиля.
        for notice in profile_notices(deploy_config):
            engine_log.warn(notice, event="deploy_profile_note")
    return build_adapters(
        profile=args.profile,
        deploy=deploy_config,
        transport=getattr(args, "deploy_transport", None),
        gateway=gateway,
        evaluator=evaluator,
        registry_specs=registry_specs,
        index_root=index_root,
        package=package,
        cache=cache,
        domain_profiles=(inputs.domain_profiles if inputs is not None else None),
        # Lane A/L3a: слои источников краула (crawl_sources.yaml) — wiring решает по их
        # ``engine``, какие per-engine краулеры строить. Без ``inputs`` — не передаются.
        crawl_sources=(getattr(inputs, "crawl_sources", None) if inputs is not None else None),
        # N1: run-слой осей краула и CLI ``--max-depth`` — для автодетекта discovery,
        # включающего принуждение ``robots.txt`` в адаптерах (без discovery — не трогаем).
        run_crawl=(
            getattr(getattr(inputs, "run", None), "crawl", None)
            if inputs is not None
            else None
        ),
        # Ось ``embed.device``: run-слой (``run.yaml:embed``) уходит тем же путём, что
        # ``run_crawl``; резолв ``app < run`` делает wiring той же функцией, что
        # ``resolve_declared_axes``. Окружение здесь не читается: устройство объявлено
        # входом, иначе ``run_id`` не описывал бы место счёта векторов.
        run_embed=(
            getattr(getattr(inputs, "run", None), "embed", None)
            if inputs is not None
            else None
        ),
        cli_max_depth=getattr(args, "max_depth", None),
        # P7: механизм PDF — объявленная ось прогона; порты (пробный экстрактор,
        # диагностика) обязаны видеть тот же, что и Ш5 (``--pdf-pipeline``).
        pdf_pipeline=getattr(args, "pdf_pipeline", None),
        # P4a/I17: store — источник персистентных чанков для лексического индекса и
        # резолвер метаданных для dense-плеча. Без него оба билдера возвращают ``None``.
        store=store,
        chunk_lookup=store.get_chunk if store is not None else None,
        index_build_id=index_build_id,
        engine_log=engine_log,
    )


def _load_passports_dir(passports_dir: str | Path) -> dict[str, Any]:
    """Загружает паспорта продуктов для фаз Ш4/Ш5 (:func:`core.config.load_passports`).

    Отсутствующий каталог даёт ``{}`` (безопасный дефолт — прогон, которому паспорта не
    нужны, не падает; :func:`core.config.passports.load_passports` уже так устроен).
    Каталог **есть**, но паспорт не разбирается/не соответствует схеме §4.3 — это
    конфигурационная ошибка, и она поднимается как понятная ошибка CLI (:data:`EXIT_USAGE`),
    а не глушится.

    :raises CliError: только при существующем, но некорректном паспортном каталоге.
    """
    root = Path(passports_dir)
    if not root.is_dir():
        return {}
    try:
        return load_passports(root)
    except PassportLoadError as exc:
        raise CliError(_concise(exc), code="passports", exit_code=EXIT_USAGE) from exc


def _load_corpus_urls(path: str | Path | None) -> dict[str, list[str]]:
    """Читает опциональный JSON/YAML-файл ``{product_id: [url, ...]}`` (Ш5).

    Источник URL — **оператор** (Discovery/человек): специфицированного артефакта URL нет.
    Файл задаётся ``--urls`` и валидируется как отображение ``product_id → список строк``.
    При ``None`` возвращается ``{}``: нулевые URL — легитимный вход (см. заметку в
    :func:`_resolve_phases`), а не приглашение фетчить домены из паспортов.

    :raises CliError: файл не читается/не разбирается/не является отображением списков строк.
    """
    if path is None:
        return {}
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise CliError(
            f"не удалось прочитать {p}: {_concise(exc)}",
            code="urls",
            exit_code=EXIT_USAGE,
        ) from exc
    try:
        data = json.loads(text) if p.suffix.lower() == ".json" else yaml.safe_load(text)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise CliError(
            f"некорректный {p}: {_concise(exc)}", code="urls", exit_code=EXIT_USAGE
        ) from exc

    if not isinstance(data, Mapping):
        raise CliError(
            f"{p}: ожидался объект {{product_id: [url, ...]}}",
            code="urls",
            exit_code=EXIT_USAGE,
        )
    result: dict[str, list[str]] = {}
    for product_id, value in data.items():
        if not isinstance(value, list) or not all(
            isinstance(item, str) for item in value
        ):
            raise CliError(
                f"{p}: {product_id!r} должен быть списком строк (URL)",
                code="urls",
                exit_code=EXIT_USAGE,
            )
        result[str(product_id)] = list(value)
    return result


def _filter_corpus_urls(
    urls: Mapping[str, list[str]],
    *,
    store: Any,
    session_id: str,
    only_failed: bool,
    only_product: str | None,
) -> tuple[dict[str, list[str]], list[str]]:
    """Phase 4: фильтр набора URL по durable-сеансу краула (``--only-failed``/``--only-product``).

    Путь **отдельного сеанса краула** (``hound crawl run``): провалы здесь — это провалы
    *этого* сеанса, и пере-фетч идёт от объявленных URL. ``hound run`` этим путём больше не
    идёт: межпрогонный ремонт (``url_state`` + depth-0 известные URL) живёт в
    :func:`_only_failed_work_set`, потому что там провал глубокой страницы не имеет права
    становиться seed-корнем.

    Единственный владелец состояния — ``store``; чтение сеанса делается здесь, до резолва
    фаз, чтобы исполнитель (его work-set выводится из отфильтрованных ``urls``) увидел тот
    же набор, что и локальный путь.

    * ``--only-failed`` — оставляет только ``(product_id, url)`` со статусом элемента
      ``FAILED`` (``iter_crawl_session_items(..., status=FAILED)``). Сеанс отсутствует →
      фильтр **не** применяется (graceful degradation: нечего ретраить — обычный прогон),
      и в ``notes`` попадает пояснение.
    * ``--only-product <id>`` — оставляет только этот продукт (независимо от сеанса).

    Возвращает ``(отфильтрованные_urls, notes)``; ``notes`` — человекочитаемые пояснения
    для payload (ни один фильтр не «пропадает» тихо).

    :param store: открытый стор прогона (методы ``get_crawl_session``/
        ``iter_crawl_session_items``).
    :param session_id: канонический ``f"crawl:{run_id}"``.
    """
    result = {str(pid): [str(u) for u in values] for pid, values in urls.items()}
    notes: list[str] = []
    if not only_failed and only_product is None:
        return result, notes

    failed: set[tuple[str, str]] | None = None
    if only_failed:
        session = store.get_crawl_session(session_id) if session_id else None
        if session is None:
            notes.append(
                "only-failed: сеанса нет — фильтр не применён (прогон по всем URL)"
            )
        else:
            failed = {
                (item.product_id, item.url)
                for item in store.iter_crawl_session_items(
                    session_id, status=CrawlItemStatus.FAILED
                )
            }

    filtered: dict[str, list[str]] = {}
    for product_id, product_urls in result.items():
        if only_product is not None and product_id != str(only_product):
            continue
        kept = (
            [url for url in product_urls if (product_id, url) in failed]
            if failed is not None
            else list(product_urls)
        )
        if kept:
            filtered[product_id] = kept

    if only_product is not None and str(only_product) not in result:
        notes.append(f"only-product: {only_product!r} нет в --urls — набор пуст")
    if only_failed and failed is not None and not filtered:
        notes.append("only-failed: failed-элементов нет — пере-фетчить нечего")
    return filtered, notes


def _only_failed_work_set(
    urls: Mapping[str, list[str]],
    *,
    store: Any,
    session_id: str,
    only_failed: bool,
    only_product: str | None,
    state: Any = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]], list[str]]:
    """Phase 4: work-set ``--only-failed`` — сеанс текущего прогона **или** durable-состояние.

    Возвращает ``(seed_urls, repair_urls, notes)``:

    * ``seed_urls`` — объявленные корни, которые увидит Ш5. Под ``--only-failed`` это
      **не** провалы, а объявленные корни тех продуктов, у которых есть что ремонтировать:
      они нужны как пространство scope-инференса, и Ш5 их **не** дренит (ремонт — не
      обход). Продукт без провалов в набор не попадает и не краулится вовсе.
    * ``repair_urls`` — ``product_id → [url]`` — собственно ремонт. Он идёт в Ш5 отдельным
      входом (``only_failed_repairs``) и попадает в дренаж как **известные** depth-0 URL
      (``Frontier.add_revalidate_seed``), а не как BFS-корни.
    * ``notes`` — что именно сработало и сколько URL дало. Молчаливого выбора источника нет.

    **Порядок источников (решение, не эвристика):**

    1. ``FAILED``-элементы **текущего** сеанса ``crawl:{run_id}``. Это точный набор: прогон
       возобновляется на том же ``run_id`` и знает, что именно не доехало.
    2. Если сеанса нет **или** в нём нет ``FAILED``-элементов — строки ``url_state``
       пакета с ``consecutive_failures > 0``. Это межпрогонный носитель: ``url_state``
       пишется на **каждый** исход URL (:func:`core.pipeline.corpus_ingest.record_url_state`),
       сбой увеличивает счётчик, успех его сбрасывает. Он нужен именно потому, что сеанс
       живёт один прогон: добавление продукта без ``--update`` уже меняет ``run_id`` (и
       сеанс), и провалы прошлого цикла сеансовым фильтром были не видны.

    ``not_found`` (404) в наборе не ретраится: счётчик ``consecutive_failures`` на него не
    растёт (терминальное наблюдение, а не транзиентный сбой) — отсев терминальных строк
    делает Ш5 видимой причиной ``not_found_terminal``
    (:func:`core.pipeline.corpus_ingest.plan_repair_items`).

    ``state is None`` (``state.db`` пакета не открыт — например, состояние ещё не создано)
    означает, что межпрогонный источник недоступен: тогда work-set **пуст** и в ``notes``
    сказано почему. Обычный прогон «по всем URL» здесь был бы не ремонтом, а полным
    обходом — то есть другим измерением, чем объявлено флагом.

    :param store: открытый стор прогона (``get_crawl_session``/``iter_crawl_session_items``).
    :param session_id: канонический ``f"crawl:{run_id}"``.
    :param state: durable-состояние пакета (``iter_url_state``); ``None`` — недоступно.
    """
    declared = {str(pid): [str(u) for u in values] for pid, values in urls.items()}
    notes: list[str] = []
    if not only_failed and only_product is None:
        return declared, {}, notes

    if only_product is not None:
        if str(only_product) not in declared:
            notes.append(f"only-product: {only_product!r} нет в --urls — набор пуст")
        declared = {
            pid: values for pid, values in declared.items() if pid == str(only_product)
        }

    if not only_failed:
        return declared, {}, notes

    # (а) текущий сеанс: FAILED-элементы — точный набор того, что не доехало.
    found: dict[str, list[str]] = {}
    source: str | None = None
    session_failed: set[tuple[str, str]] = set()
    session = store.get_crawl_session(session_id) if session_id else None
    if session is not None:
        session_failed = {
            (item.product_id, item.url)
            for item in store.iter_crawl_session_items(
                session_id, status=CrawlItemStatus.FAILED
            )
        }
    if session_failed:
        source = "session"
        for pid, url in sorted(session_failed):
            found.setdefault(pid, []).append(url)
        notes.append(
            f"only-failed: источник — сеанс {session_id or '—'}: "
            f"{len(session_failed)} failed-элемент(ов)"
        )
    else:
        reason = (
            f"сеанса {session_id} нет"
            if session is None and session_id
            else (
                "сеанс не определён"
                if session is None
                else f"в сеансе {session_id} failed-элементов нет"
            )
        )
        if state is None:
            notes.append(
                f"only-failed: {reason}, state.db пакета не открыт — межпрогонный носитель "
                "не прочитан; work-set пуст (полный обход не подменяет ремонт)"
            )
            return {}, {}, notes
        # (б) межпрогонный носитель: провалы, зафиксированные durable-состоянием.
        # Область чтения — **объявленные** продукты: у порта нет перечисления продуктов, а
        # продукт, которого нет в ``--urls``, не имеет ни объявленных хостов, ни пространства
        # scope, то есть ремонтировать его всё равно нечем (это сказано в ``notes``).
        for product_id in declared:
            try:
                rows = list(state.iter_url_state(product_id))
            except Exception as exc:  # noqa: BLE001 — чтение состояния не роняет прогон
                notes.append(
                    f"only-failed: url_state({product_id}) не прочитан: "
                    f"{type(exc).__name__}: {_concise(exc)} — провалы продукта неизвестны"
                )
                continue
            failed_urls = [
                row.url for row in rows if int(row.consecutive_failures or 0) > 0
            ]
            if failed_urls:
                found[product_id] = failed_urls
        source = "url_state"
        notes.append(
            f"only-failed: {reason} — источник url_state предыдущего прогона "
            f"(объявленные продукты: {', '.join(sorted(declared)) or '—'}): "
            f"{sum(len(v) for v in found.values())} URL с consecutive_failures>0"
        )

    repair: dict[str, list[str]] = {}
    undeclared = 0
    for product_id, product_urls in found.items():
        if product_id not in declared:
            # Продукт не объявлен в ``--urls``: у него нет ни объявленных хостов, ни
            # пространства scope. Не выдумываем корни — считаем отброшенным.
            undeclared += len(product_urls)
            continue
        seen: set[str] = set()
        kept: list[str] = []
        for url in product_urls:
            if url in seen:
                continue
            seen.add(url)
            kept.append(url)
        if kept:
            repair[product_id] = kept
    if undeclared:
        notes.append(
            f"only-failed: {undeclared} URL отброшено — продукт не объявлен в --urls"
        )
    if not repair:
        notes.append(
            "only-failed: провалов нет — пере-фетчить нечего (авторские корни не обходятся)"
        )
    else:
        notes.append(
            f"only-failed: ремонт {sum(len(v) for v in repair.values())} URL по "
            f"{len(repair)} продукт(ам) из {source} — depth-0 известные URL, "
            "авторские корни не обходятся"
        )
    # Корни — только у продуктов с ремонтом: продукт без провалов не краулится вовсе.
    seeds = {pid: list(values) for pid, values in declared.items() if pid in repair}
    return seeds, repair, notes


#: Phase 4: терминальные статусы сеанса краула (дальнейший опрос бесполезен).
#: F1/Д-6: ``interrupted`` сюда **обязан** входить — иначе ``--follow`` ждал бы вечно
#: сеанс, который уже никто не исполняет.
_TERMINAL_SESSION_STATUSES: frozenset[CrawlSessionStatus] = frozenset(
    {
        CrawlSessionStatus.COMPLETED,
        CrawlSessionStatus.PARTIAL,
        CrawlSessionStatus.CANCELLED,
        CrawlSessionStatus.FAILED,
        CrawlSessionStatus.INTERRUPTED,
    }
)


def _is_terminal_session_status(status: str | None) -> bool:
    """Phase 4: терминален ли статус сеанса (``completed|partial|cancelled|failed|interrupted``)."""
    if status is None:
        return True
    try:
        return CrawlSessionStatus(status) in _TERMINAL_SESSION_STATUSES
    except ValueError:
        return True


def _session_snapshot(store: Any, session_id: str) -> tuple[str | None, dict[str, int]]:
    """Phase 4: ``(status, counts)`` сеанса краула; ``status=None`` при отсутствии сеанса."""
    session = store.get_crawl_session(session_id)
    status = session.status.value if session is not None else None
    return status, dict(store.crawl_session_counts(session_id))


def _emit_session_progress(
    status: str | None,
    counts: Mapping[str, int],
    *,
    session_id: str,
    json_mode: bool,
) -> None:
    """Phase 4: одна строка прогресса (NDJSON при ``--json``, иначе человекочитаемо).

    Пишется в stdout и работает при перенаправлении (TTY не требуется).
    """
    if json_mode:
        _print_json(
            {
                "event": "crawl_progress",
                "session_id": session_id,
                "status": status,
                "counts": dict(sorted(counts.items())),
            }
        )
    else:
        summary = ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
        sys.stdout.write(f"crawl[{status or '—'}]: {summary or '—'}\n")
        sys.stdout.flush()


def _progress_callback(
    *, session_id: str, json_mode: bool
) -> Callable[[dict[str, int]], None]:
    """Phase 4: колбэк прогресса во время ``build_corpus`` (``--follow``)."""

    def _callback(counts: dict[str, int]) -> None:
        _emit_session_progress(None, counts, session_id=session_id, json_mode=json_mode)

    return _callback


def _emit_product_progress(
    phase: str, product_id: str, *, session_id: str, json_mode: bool
) -> None:
    """Phase 4: живая граница продукта (``start``/``end``) в stdout.

    Дополняет ``crawl_progress`` (агрегат сеанса) **продукт-скоупом**: агрегат не
    отличает «продукт ещё в очереди» от «продукт потерян», а продукт-цикл
    последователен и один продукт может идти часами. При ``--json`` — NDJSON-строка
    ``crawl_product``; иначе — одна короткая человекочитаемая строка.
    """
    if json_mode:
        _print_json(
            {
                "event": "crawl_product",
                "session_id": session_id,
                "phase": phase,
                "product_id": product_id,
            }
        )
    else:
        sys.stdout.write(f"crawl product {phase}: {product_id}\n")
        sys.stdout.flush()


def _product_callback(
    *, session_id: str, json_mode: bool
) -> Callable[[str, str], None]:
    """Phase 4: колбэк границ продукта во время ``build_corpus`` (``--follow``)."""

    def _callback(phase: str, product_id: str) -> None:
        _emit_product_progress(
            phase, product_id, session_id=session_id, json_mode=json_mode
        )

    return _callback


def _follow_session(
    store: Any,
    session_id: str,
    *,
    interval: float,
    json_mode: bool,
    sleep: Callable[[float], None],
) -> str | None:
    """Phase 4 (``--follow``/``--watch``): опрос сеанса до терминального статуса.

    Печатает снимок ``crawl_session_counts`` (NDJSON при ``--json``) и возвращает итоговый
    статус. Терминал/no-сеанс → один снимок без сна. ``sleep`` инжектируется (тесты —
    no-op). Работает при перенаправлении (TTY не требуется).
    """
    while True:
        status, counts = _session_snapshot(store, session_id)
        _emit_session_progress(
            status, counts, session_id=session_id, json_mode=json_mode
        )
        if _is_terminal_session_status(status):
            return status
        sleep(interval)


def _load_render_modes(path: str | Path | None) -> dict[str, FetchMode]:
    """Читает опциональный JSON/YAML-файл ``{url: "http"|"browser"}`` (Ш5).

    Явное переопределение оси ``fetch`` (D2): браузерный рендер нужен для SPA/JS-страниц.
    Файл задаётся ``--render-modes``; значения валидируются через :class:`FetchMode`
    (``http``/``browser``). При ``None`` возвращается ``{}``: пустое отображение даёт
    профиль домена/дефолт HTTP в :func:`core.pipeline.fetch_plan.resolve_fetch_plan`.

    :raises CliError: файл не читается/не разбирается; документ не словарь либо значение
        не является допустимым ``FetchMode`` (:data:`EXIT_USAGE`, код ``render-modes``).
    """
    if path is None:
        return {}
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise CliError(
            f"не удалось прочитать {p}: {_concise(exc)}",
            code="render-modes",
            exit_code=EXIT_USAGE,
        ) from exc
    try:
        data = json.loads(text) if p.suffix.lower() == ".json" else yaml.safe_load(text)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise CliError(
            f"некорректный {p}: {_concise(exc)}",
            code="render-modes",
            exit_code=EXIT_USAGE,
        ) from exc

    if data is None:
        return {}
    if not isinstance(data, Mapping):
        raise CliError(
            f"{p}: ожидался объект {{url: \"http\"|\"browser\"}}",
            code="render-modes",
            exit_code=EXIT_USAGE,
        )
    modes: dict[str, FetchMode] = {}
    for url, mode in data.items():
        if not isinstance(mode, str):
            raise CliError(
                f"{p}: {url!r} должен указывать режим строкой (http|browser)",
                code="render-modes",
                exit_code=EXIT_USAGE,
            )
        try:
            modes[str(url)] = FetchMode(mode.strip())
        except ValueError as exc:
            raise CliError(
                f"{p}: недопустимый fetch-режим {mode!r} для {url!r}; "
                "ожидается один из http|browser",
                code="render-modes",
                exit_code=EXIT_USAGE,
            ) from exc
    return modes


def _worker_extractor_factory(
    pdf_device: PdfDevice, document_timeout: float | None
) -> Any:
    """Фабрика per-level экстрактора для inline-пути с объявленными осями (P8/P9).

    Возвращает **саму** модульную функцию, когда привязывать нечего (``cpu`` без
    таймаута): сохранение прежнего объекта важно, потому что фабрика передаётся в
    ``core.pipeline.corpus`` как «ссылка на движок по умолчанию», и подмена её на
    ``partial`` без нужды меняла бы наблюдаемое поведение (и identity в тестах).

    ``partial`` (а не замыкание) — требование picklability: фабрика уезжает в
    воркер-процесс, где замыкание не разрешилось бы. Оба значения — скаляры/StrEnum.
    """
    kwargs: dict[str, Any] = {}
    if document_timeout is not None:
        kwargs["document_timeout"] = document_timeout
    if PdfDevice(pdf_device) is not PdfDevice.CPU:
        kwargs["pdf_device"] = PdfDevice(pdf_device)
    if not kwargs:
        return build_worker_extractor
    return partial(build_worker_extractor, **kwargs)


def _resolve_phases(
    args: argparse.Namespace, *, inputs: RunInputs, plan: RunPlan, ports: Ports,
    deploy_config: DeployConfig | None = None,
    paths: Any = None,
    render_modes: Mapping[str, FetchMode] | None = None,
    urls: Mapping[str, list[str]] | None = None,
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
    on_product: Callable[[str, str], None] | None = None,
    max_depth: int | None = None,
    max_documents: int | None = None,
    exclude_paths: Sequence[str] | None = None,
    exclude_documents: Sequence[str] | None = None,
    document_level: DocumentLevel | None = None,
    pdf_pipeline: PdfPipeline | None = None,
    state: Any = None,
    product_modes: Mapping[str, str] | None = None,
    product_plan: Mapping[str, Any] | None = None,
    run_scope: str | None = None,
    refresh_policy: str = "auto",
    only_failed_repairs: Mapping[str, list[str]] | None = None,
    execution_contract_digest: str | None = None,
    axes: Any = None,
    declared_status_dict_version: str | None = None,
    mem_budget_mb: int | None = None,
    derive_extract_workers: bool | None = None,
    previous_run_id: str | None = None,
    previous_store: Any = None,
    change_class: str | None = None,
    cycle_id: str | None = None,
    crawl_only: bool = False,
) -> dict[str, PhaseFn]:
    """Собирает реестр не-гейтовых фаз (``phases_factory`` — тестовый шов).

    **Паспорта** загружаются из ``--passports-dir`` (дефолт — resolved ``paths.passports``,
    ``03`` §A.1) и передаются фазе Ш4 (наследование по ``upstream``). **URL корпуса** —
    только явный операторский вход ``--urls`` (JSON/YAML ``{product_id: [url, ...]}``);
    специфицированного артефакта URL в регламенте **нет**, поэтому ``doc_domains``
    паспортов молча **не** фетчатся: homepage — не корпус документации (I19: контент
    становится evidence лишь после ``corpus_raw`` → hash → index). Без ``--urls`` фазы Ш5
    получает ``{}`` и легитимно работает как no-op. ``passport_urls`` — лишь возможный
    seed-источник для будущего Discovery/оператора, но здесь он не подставляется неявно.

    Операционные каталоги (``out_dir``, ``raw_root``) берутся из resolved-раскладки
    (``paths``): ``runs`` для отчётов Ш9, ``corpus_raw`` для retention сырых байтов Ш5.

    **Деплой-провенанс (07 §7).** Из resolved :class:`DeployConfig` выводятся
    ``deploy_profile``/``edge_schema_version``/``edge_capabilities`` и протягиваются в
    :func:`build_phases` → фазу Ш9, откуда попадают в ``run_manifest.yaml`` и в
    ``runs`` (реальные колонки). Версии edge-компонентов известны лишь split-пути
    диагностики (``/v1/capabilities``); здесь они не выдумываются — пусто, если не заданы.

    Гейты ``g1``–``g4`` сюда не входят — их (де)фолтные замыкания остаются в
    :class:`Runner` (I6/I7, S9), поэтому ``run`` честно останавливается ``blocked`` без
    человеческого решения.

    :param deploy_config: resolved профиль развёртывания; ``None`` → деплой-провенанс
        манифеста не заполняется (значения не выдумываются).
    :param urls: Phase 4 — готовый (уже отфильтрованный ``--only-failed``/``--only-product``)
        набор ``product_id → [url]``; ``None`` → читается ``--urls`` как раньше.
    :param max_attempts: Phase 4 — потолок попыток корпуса (``--max-attempts``); ``None`` →
        без потолка (протягивается в :func:`build_phases`/``build_corpus``).
    :param progress: Phase 4 — колбэк снимка прогресса сеанса (``--follow``); ``None`` → без.
    :param on_product: Phase 4 — колбэк границ продукта (``--follow``): ``start``/``end``
        по продукту **живо**, пока идёт Ш5; ``None`` → без вывода (поведение неизменно).
    :param state: открытый ``ProductStatePort`` пакета (09 §4.1); ``None`` → инкрементальный
        субстрат корпуса выключен (dry-run/``crawl`` без состояния).
    :param product_modes: ``product_id → mode`` из onboarding-плана; ``None`` → все ``new``.
    :param run_scope: срез прогона (``full`` | ``only_failed`` | ``product:<id>``) из
        :func:`core.pipeline.run_identity.resolve_run_scope` — тот же, что вошёл в ``run_id``.
        ``None`` → манифест пробует другие носители и оставляет ``None``, если их нет:
        значение не подставляется догадкой (``full`` по умолчанию выдал бы полное
        измерение за срез).
    :param product_plan: сериализованный ``ProductPlanSet`` (mode/scope/reasons по продуктам)
        — тот же объект, что composition root кладёт в снимок входов (``run_inputs``).
        Протягивается в Ш9 → ``RunManifest.product_plan``/``runs.product_plan_json``; без
        него срез ``--only-product`` неотличим от полного измерения, не заглядывая в
        ``state.db``. ``None`` → значение берётся из снимка входов (Ш9), а если снимка нет —
        остаётся ``None``: план не реконструируется из ``product_modes`` (там только режим).
    :param refresh_policy: политика наблюдения прогона (значение ``RefreshPolicy``).
    :param only_failed_repairs: ``--only-failed`` — work-set ремонта ``product_id → [url]``
        (см. :func:`_only_failed_work_set`); идёт в Ш5 отдельным входом. ``None`` ⇒
        прежнее поведение.
    :param execution_contract_digest: готовый ``execution_contract_digest`` прогона
        (09 §12.5), вычисленный composition root до исполнения фаз; протягивается в
        манифест Ш9 без пересчёта (``None`` — неизвестен, значение не выдумывается).
    :param axes: декларированные оси исполнения (см.
        :func:`core.cli.execution_axes.resolve_declared_axes`) — тот же резолвер, что у
        контракта; из них манифест берёт deploy-профиль/чанкер/оси краула/реестры/
        ``llm_scenarios``. ``None`` — legacy/модульный вызов.
    :param declared_status_dict_version: версия словаря статусов (декларированная ось).
    :param mem_budget_mb: F0 — бюджет памяти прогона, MB, разрешённый composition root
        (:func:`core.cli.wiring.resolve_extraction_budget`). Передаётся в Ш5 **явным**
        параметром: пайплайн окружение не читает. ``None`` — бюджета нет.
    :param derive_extract_workers: F0 — выводить окно экстракции из бюджета (только для
        локального экстрактора). ``None`` — не передано: composition root резолвит бюджет
        и флаг сам по профилю зон (здесь же, один раз на сборку фаз).
    :param crawl_only: F2 (17 §4) — «краул без конверсии» (``crawl run --crawl-only``).
        Здесь же решается несовместимость с пулом экстракции: пул **не передаётся**
        (``None``), потому что режим не извлекает документы, а ``build_corpus`` отвергает
        их сочетание явным ``ValueError``. ``False`` — прежнее поведение байт-в-байт.
    """
    passports_dir = _resolve_passports_dir(args, paths)
    raw_root = str(paths.corpus_raw) if paths is not None else "corpus_raw"
    # 09 §8.1–§8.5: тот же корень, что у ``IndexBuilderPort`` (composition root), —
    # переиспользование партиций и запись ``_compositions/*.json``.
    index_root = str(paths.index_store) if paths is not None else "index_store"
    corpus_urls = (
        dict(urls)
        if urls is not None
        else _load_corpus_urls(getattr(args, "urls", None))
    )
    # 07 §7: деплой-провенанс манифеста — из resolved deploy-конфига; иначе пусто.
    deploy_profile: str | None = None
    edge_schema_version: str | None = None
    edge_capabilities: Mapping[str, Any] = {}
    if deploy_config is not None:
        deploy_fields = manifest_deploy_fields(deploy_config)
        deploy_profile = deploy_fields["deploy_profile"]
        edge_schema_version = deploy_fields["edge_schema_version"]
        edge_capabilities = deploy_fields["edge_capabilities"]
    # F0/§3.3: бюджет памяти резолвится **один раз** в composition root и едет в пайплайн
    # явным параметром. ``None`` — вызывающий (команда) уже нашёл значения: повторно env
    # не читаем, иначе два места в одном прогоне могли бы дать разные числа.
    if derive_extract_workers is None:
        mem_budget_mb, derive_extract_workers = resolve_extraction_budget(
            (
                deploy_config.profile
                if deploy_config is not None
                else getattr(args, "profile", LOCAL_PROFILE)
            ),
            # Инъецированные порты — тестовый шов: измеренные пики docling описывают не
            # тот стек, поэтому окно из бюджета не выводится (A3), явная ось — работает.
            injected_ports=getattr(args, "ports", None) is not None,
        )
    # F0: сток пика памяти воркеров — рядом с логами/метриками прогона (та же раскладка,
    # что у ``metrics.json``: ``{runs}/{run_id}/``). Ядро не собирает пик под ``split``
    # (там воркеры на edge) — решение принимает ``build_extraction_pool``.
    run_id_for_paths = getattr(plan, "run_id", None)
    worker_rss_dir = (
        str(
            Path(getattr(args, "runs_dir", None) or "runs")
            / sanitize_filename(str(run_id_for_paths))
            / "workers"
        )
        if run_id_for_paths
        else None
    )
    # P4/OOM: run-дефолт рециклинга воркеров экстракции захватывается фабрикой пула;
    # per-site значение резолвится в pipeline (site > run > app) и передаётся аргументом.
    run_extract_max_tasks = getattr(
        getattr(getattr(inputs, "run", None), "crawl", None),
        "extract_max_tasks_per_child",
        None,
    )
    # P9: устройство моделей — объявленная ось (``run.yaml``/``deploy.yaml:crawl.pdf_device``).
    # Резолв и проверка исполнимости в одной точке (:func:`preflight_pdf_device`): ``cuda``
    # без устройства — EXIT_USAGE до прогона, а не тихий CPU. Значение едет и в пул
    # (инициализатор воркера), и в ``extractor_factory`` портов (inline-путь/проба);
    # CLI-флага у оси нет намеренно: устройство принадлежит **хосту исполнения**, а не
    # операторской правке пакета.
    resolved_pdf_device = preflight_pdf_device(
        deploy=deploy_config,
        run_crawl=getattr(getattr(inputs, "run", None), "crawl", None),
    )
    kwargs: dict[str, Any] = {
        "inputs": inputs,
        "plan": plan,
        "ports": ports,
        # I2b/I2c: каталог пакета — источник per-product подписей scope_attestation.yaml.
        "package_dir": getattr(args, "package", None),
        "deploy_profile": deploy_profile,
        "edge_schema_version": edge_schema_version,
        "edge_capabilities": edge_capabilities,
        # §10: объявленное исключение стенда RnD — в профиль публикуемости (Ш8). Источник —
        # deploy-конфиг (единственное место, где исключение объявляется); ``None`` — не
        # объявлено, и это видно замечанием в профиле, а не подразумевается.
        "llm_rnd_endpoint": (
            getattr(getattr(deploy_config, "llm", None), "rnd_endpoint", None)
            if deploy_config is not None
            else None
        ),
        # URLs: только явный операторский вход; doc_domains не фетчатся молча (I19).
        "urls": corpus_urls,
        # Паспорта: --passports-dir либо resolved paths.passports; отсутствие → {}.
        "passports": _load_passports_dir(passports_dir),
        "out_dir": getattr(args, "runs_dir", None) or "runs",
        "raw_root": raw_root,
        "index_root": index_root,
        "retrieval_profile": getattr(args, "retrieval_profile", None),
        "cutoff_k": getattr(args, "cutoff_k", None),
        # D2: доменные профили (из domain_profiles.yaml) + явное переопределение
        # оси fetch (--render-modes); резолв FetchPlan — в pipeline Ш5.
        "domain_profiles": inputs.domain_profiles,
        "explicit_fetch": render_modes or {},
        # Lane A/L3a: слои источников краула (crawl_sources.yaml) и единый владелец
        # politeness-пейсинга (RateLimiter) — протягиваются в фазу корпуса Ш5.
        "crawl_sources": getattr(inputs, "crawl_sources", None),
        "rate_limiter": RateLimiter(),
        # Phase 4: потолок попыток (--max-attempts) и колбэк прогресса (--follow).
        "max_attempts": max_attempts,
        "progress": progress,
        # Phase 4: живая граница продукта (--follow) — продукт-скоуп к агрегату сеанса.
        "on_product": on_product,
        # D1/D2: CLI-уточнения run-слоя осей краула (пробрасываются в build_phases →
        # run-слой CrawlRunConfig) и application-слой (deploy.yaml:crawl).
        "max_depth": max_depth,
        "max_documents": max_documents,
        "exclude_paths": list(exclude_paths) if exclude_paths else None,
        "exclude_documents": list(exclude_documents) if exclude_documents else None,
        # P5: CLI-уточнение run-слоя уровня извлечения документов (--document-level).
        "document_level": document_level,
        # P7: CLI-уточнение run-слоя механизма извлечения PDF (--pdf-pipeline).
        "pdf_pipeline": pdf_pipeline,
        # 09 §6.3 (MVP-3b): durable-субстрат пакета, режимы продуктов и политика наблюдения
        # → build_corpus. ``state=None`` (dry-run/``crawl``) — поведение байт-в-байт.
        "state": state,
        "product_modes": product_modes,
        # 16 §3.1/09 §14.2: план продуктов — тот же объект, что уходит в снимок входов.
        "product_plan": product_plan,
        # 17 §4 (F2): «краул без конверсии» — вход Ш5; полный ``run`` его не выставляет.
        "crawl_only": crawl_only,
        # 09 §13.6/16 §3.1: срез прогона — та же величина, что вошла в ``run_id``
        # (``resolve_run_scope``); без неё ``runs.run_scope`` остаётся NULL и срез
        # неотличим от полного измерения.
        "run_scope": run_scope,
        "refresh_policy": refresh_policy,
        # ``--only-failed``: work-set ремонта (провалы из сеанса либо ``url_state``) —
        # отдельный вход Ш5: ремонт идёт известными depth-0 URL, а не обходом корней.
        "only_failed_repairs": only_failed_repairs,
        # 09 §12.5: готовый отпечаток условий измерения → манифест Ш9 (без пересчёта).
        "execution_contract_digest": execution_contract_digest,
        # 09 §12.3/§13: база сравнения прогона (результат ``previous_run_of`` — резолвится
        # один раз в команде ``run``) и класс изменения из той же ``RunIdentity``.
        "previous_run_id": previous_run_id,
        # 09 §10.2: БД прогона-предшественника **только на чтение** — источник ячейки и её
        # evidence для переноса. Открывает и закрывает её composition root
        # (``_open_previous_store``): путь ``runs/{run_id}/cohound.db`` знает раскладка, а
        # ``pipeline`` раскладку не читает. ``None`` ⇒ перенос выключен (нет
        # предшественника, нет его БД или она нечитаема текущей схемой).
        "previous_store": previous_store,
        "change_class": change_class,
        # 09 §2.4/§6.2: цикл прогона — тот же, что вошёл в ``run_id``.
        "cycle_id": cycle_id,
        # §13 (2.4.0): декларированные оси исполнения — один резолвер с контрактом
        # (deploy-профиль/чанкер/оси краула/реестры/llm_scenarios).
        "axes": axes,
        "declared_status_dict_version": declared_status_dict_version,
        "crawl_app": deploy_config.crawl if deploy_config is not None else None,
        # P8: та же ось, что видит исполнение (``corpus_phase`` резолвит app < run).
        "document_timeout_s": getattr(args, "document_timeout_s", None),
        # P4: исполнитель параллельной экстракции по профилю зон (local — процессы,
        # split — потоки поверх RemoteExtractor). Ось ``crawl.extract_workers`` решается
        # в pipeline; при ``None``/``1`` пул не создаётся (поведение байт-в-байт).
        # F2 (17 §4): в режиме «краул без конверсии» пул **не передаётся** вовсе —
        # ``build_corpus`` отвергает сочетание ``crawl_only`` + пул явным ``ValueError``
        # (извлечение в режиме «без извлечения» было бы молчаливым противоречием).
        "extraction_pool": (
            build_extraction_pool(
                deploy_config.profile if deploy_config is not None else LOCAL_PROFILE,
                extractor=getattr(ports, "extractor", None),
                max_tasks_per_child=run_extract_max_tasks,
                # F0: внутри профильной фабрики — ``None`` для ``split`` (пик воркеров edge
                # ядру не виден), путь стока для локального процессного пула.
                worker_rss_dir=worker_rss_dir,
                # P9: устройство — свойство хоста, поэтому захватывается фабрикой пула, а не
                # приходит в payload задания; под ``split`` игнорируется (edge своим конфигом).
                pdf_device=resolved_pdf_device,
            )
            if not crawl_only
            else None
        ),
        # F0: бюджет памяти и решение «окно из бюджета» — явные входы Ш5 (не env).
        "mem_budget_mb": mem_budget_mb,
        "derive_extract_workers": bool(derive_extract_workers),
        # Ф1.2: межпрогонный кэш экстракции. Каталог — resolved ``paths.extract_cache``
        # (``COHOUND_EXTRACT_CACHE_DIR`` / ``COHOUND_HOME`` / ``paths.yaml``).
        # ``paths is None`` ⇒ кэш **выключен**, а не «относительно CWD»: у кэша без
        # resolved-раскладки нет определённой области, а CWD-дефолт делает его общим
        # для всех вызовов процесса — ровно та же причина, по которой ``_resolve_ports``
        # отдаёт ``cache=None``. Иначе тесты и параллельные прогоны делят состояние.
        # Бюджет не задаётся: вытеснение по размеру — операционное решение, дефолт —
        # без вытеснения. Ключ — хеш ретенированных байтов (I19), поэтому кэш не может
        # подсунуть неверифицированный контент.
        "extract_cache": (
            ExtractionCache(str(paths.extract_cache)) if paths is not None else None
        ),
        # Ф3: межпрогонный кэш вердиктов оценщика. Каталог — resolved ``paths.eval_cache``
        # (``COHOUND_EVAL_CACHE_DIR`` / ``COHOUND_HOME`` / ``paths.yaml``); ``paths is None``
        # ⇒ выключен, по той же причине, что и выше. Ключ (``EVAL_CACHE_FORMAT = 2``,
        # 09 §10.5) — отрендеренный промпт + конфигурация оценщика + **правила ответа**
        # (``methodology_version``/``status_dict_version``/политика расчёта) + дайджест
        # строки каталога + условия ретривала, поэтому ни смена сателлитов/заголовка/
        # чанков/модели, ни смена методологических правил не даёт ложного хита.
        "eval_cache": (
            EvalVerdictCache(str(paths.eval_cache)) if paths is not None else None
        ),
        # Кэш **векторов** эмбеддинга (``core/pipeline/embed_cache.py``). Каталог — resolved
        # ``paths.embed_cache`` (``COHOUND_EMBED_CACHE_DIR`` / ``COHOUND_HOME`` / ``paths.yaml``);
        # ``paths is None`` ⇒ выключен, по той же причине, что у кэшей выше.
        #
        # Ключ — ``(content_hash ТЕКСТА чанка, embed_model_id)``, без ``extractor_version`` и
        # ``chunker_version``: вектор есть функция текста и модели. Именно поэтому смена
        # ``pdf_pipeline``/экстрактора не пересчитывает векторы неизменённого текста — до этой
        # правки переиспользование было возможно только целой партицией (``pb`` включает
        # ``extractor_version``), и один новый документ стоил полного пересчёта продукта.
        "embed_cache": (
            EmbedCache(str(paths.embed_cache)) if paths is not None else None
        ),
        # P5: фабрика per-level экстрактора для inline-пути. Под ``split`` ось
        # ``document_level`` не действует (R-S: edge строит конвертеры сам) — ``None``.
        # F7: ось ``crawl.document_timeout_s`` обязана доезжать до **воркеров пула** —
        # это основной путь извлечения. Фабрика передаётся в процесс по ссылке, поэтому
        # объявленное значение привязывается ``partial`` (picklable: модульная функция +
        # скалярный kwarg), а не замыканием. Не объявлена — ``None``, и действует
        # per-level таблица docling: подставлять дефолт оси нельзя, он про задачу фазы.
        "extractor_factory": (
            _worker_extractor_factory(
                resolved_pdf_device, getattr(args, "document_timeout_s", None)
            )
            if (deploy_config.profile if deploy_config is not None else LOCAL_PROFILE)
            == LOCAL_PROFILE
            else None
        ),
    }
    factory = getattr(args, "phases_factory", None)
    if factory is not None:
        return factory(**kwargs)
    return build_phases(**kwargs)


def _plan_payload(result: RunPlan) -> dict[str, Any]:
    """Сериализует :class:`RunPlan` в JSON-совместимый словарь.

    Состояние эталона (``golden_state``/``golden_declared``/``golden_reason``/
    ``golden_declined_by``/``golden_file_present``) печатается явно: «сверять не с чем» —
    законный режим (:mod:`core.pipeline.golden_status`), и оператор обязан увидеть его до
    прогона, а не догадываться по отсутствию ``golden.csv``. ``golden_count`` — строки,
    реально участвующие в сверке (0 при ``absent``/``declined``).
    ``golden_exempt_products`` — продукты, для которых эталон объявленно отсутствует
    (waiver): ``golden_count`` без него читался бы как «когорта покрыта целиком».
    """
    status = result.golden_status
    return {
        "run_id": result.run_id,
        "case_id": result.cohort.case_id,
        "anchor": result.anchor,
        "products": list(result.products),
        "modules": list(result.modules),
        "roles": list(result.roles),
        "params": [row.param_id for row in result.params],
        "module_weights": dict(result.module_weights),
        "renormalized_weights": dict(result.renormalized_weights),
        "golden_count": len(result.golden),
        # ``golden_status is None`` — план собран в обход ``load_inputs`` (напр. в тестах):
        # состояние не определялось, показываем прежнее представление «есть строки → present».
        "golden_state": (
            status.state if status is not None else ("present" if result.golden else "absent")
        ),
        "golden_declared": status.declared if status is not None else False,
        "golden_file_present": (
            status.file_present if status is not None else bool(result.golden)
        ),
        "golden_reason": status.reason if status is not None else None,
        "golden_declined_by": status.declined_by if status is not None else None,
        # §11.6: waiver'ы по продуктам (``sources/golden_status.yaml: exempt_products``).
        # Печатаются списком, а не счётчиком: оператору нужно, для каких продуктов сверки
        # нет и почему, — иначе «golden_count» читается как «покрыта вся когорта».
        "golden_exempt_products": (
            [entry.as_dict() for entry in status.exempt_products]
            if status is not None
            else []
        ),
        "warnings": list(result.warnings),
    }


def _run_report_payload(run_id: str, report: RunReport, db_path: Path) -> dict[str, Any]:
    """Сериализует :class:`RunReport` в JSON-совместимый словарь.

    Словарь фаз (P2a/P2b) поверхностен и здесь: каждый результат несёт ``skip_reason``
    (машинную причину при ``skipped``, иначе ``None``) и ``pending`` (advisory-пункты для
    человека — напр. сводка очереди ревью Ш7). ``skip_summary`` — run-level срез «что не
    отработало» (N3), чтобы пропуск не оставался виден только внутри ``results``.

    **N6.** ``resumed_skipped`` — список фаз, короткозамкнутых персистентным ``ok`` при
    resume (:attr:`RunReport.skipped`); это **не** ``PhaseStatus.skipped`` (тот виден как
    ``results[].status == "skipped"``). Ключ назван однозначно, чтобы два ``skipped`` не
    сливались в одном payload.

    **Заметки фазы.** ``results[].notes`` — строковые записи ``PhaseResult.log_entries``
    в исходном порядке: человекочитаемый канал «что фаза сделала предполётом» (напр.
    ``corpus: партиции — переиспользовано N, собрано M``). Нестроковые записи
    (``IndexBuild``/``Artifacts`` и пр.) — данные, а не заметки, и в payload не попадают.
    Это не ``errors`` и не меняет ``status``.
    """
    return {
        "ok": report.ok,
        "run_id": run_id,
        "status": report.status,
        "stopped_at": report.stopped_at,
        "db": str(db_path),
        "resumed_skipped": list(report.skipped),
        "skip_summary": [
            {
                "phase": item.phase,
                "skip_reason": (
                    item.skip_reason.value if item.skip_reason is not None else None
                ),
            }
            for item in report.results
            if item.status == "skipped"
        ],
        "errors": [
            # 17 §6 (F3): лексикографический порядок **внутри фазы** — артефакт не должен
            # зависеть от порядка событий (сеть/пул воркеров). Группировка по фазам
            # сохраняется: ``results[].errors`` — срез «кто это сказал», и перемешивать
            # фазы в общем списке значило бы потерять адрес ошибки.
            err
            for item in report.results
            for err in sorted_error_lines(item.errors)
        ],
        "results": [
            {
                "phase": item.phase,
                "status": item.status,
                "skip_reason": (
                    item.skip_reason.value if item.skip_reason is not None else None
                ),
                "pending": list(item.pending),
                "errors": sorted_error_lines(item.errors),
                "notes": [entry for entry in item.log_entries if isinstance(entry, str)],
            }
            for item in report.results
        ],
    }


def _print_plan(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод плана."""
    weights = ", ".join(
        f"{module}={weight}" for module, weight in sorted(payload["renormalized_weights"].items())
    )
    lines = [
        f"run_id: {payload['run_id']}",
        f"case_id: {payload['case_id']}",
        f"anchor: {payload['anchor']}",
        f"products: {', '.join(payload['products'])}",
        f"modules: {', '.join(payload['modules'])}",
        f"params: {', '.join(payload['params'])}",
        f"roles: {', '.join(payload['roles'])}",
        f"weights: {weights or '—'}",
        f"golden: {payload['golden_count']} (state: {payload['golden_state']})",
    ]
    # Причина непригодности эталона — рядом со счётчиком, а не только в ``--json``:
    # «0 строк» без причины читается как «эталон потеряли».
    if payload.get("golden_reason"):
        author = payload.get("golden_declined_by")
        lines.append(
            f"golden reason: {payload['golden_reason']}"
            + (f" (declined_by={author})" if author else "")
        )
    for warning in payload.get("warnings", ()):
        lines.append(f"warning: {warning}")
    sys.stdout.write("\n".join(lines) + "\n")


def _print_run_dry(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``run --dry-run`` (план фаз без исполнения)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"db: {payload['db']}",
        f"until: {payload['until'] or '—'}",
        f"phases: {', '.join(payload['phases'])}",
        "dry-run: yes (без записи)",
    ]
    for warning in payload.get("warnings", ()):
        lines.append(f"warning: {warning}")
    for plan in payload.get("products", ()):  # MVP-1: mode/blocked в dry-run
        reasons = ", ".join(plan.get("reasons") or ())
        lines.append(
            f"product {plan.get('product_id')}: mode={plan.get('mode')} "
            f"onboarding={plan.get('onboarding_state')} seeds={plan.get('seed_count')}"
            + (f" reasons={reasons}" if reasons else "")
        )
    if payload.get("blocked"):
        lines.append(f"blocked: {', '.join(payload['blocked'])}")
    sys.stdout.write("\n".join(lines) + "\n")


def _print_run_report(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``run`` (итог и точка остановки)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"db: {payload['db']}",
        f"status: {payload['status']}",
        f"stopped_at: {payload['stopped_at'] or '—'}",
    ]
    for item in payload["results"]:
        lines.append(f"phase {item['phase']}: {item['status']}")
        if item.get("skip_reason"):
            lines.append(f"  skip: {item['skip_reason']}")
        for note in item.get("pending", ()):
            lines.append(f"  pending: {note}")
        for error in item["errors"]:
            lines.append(f"  error: {error}")
        # Человекочитаемые заметки фазы («что сделано предполётом»); не errors.
        for note in item.get("notes", ()):
            lines.append(f"  note: {note}")
    if payload["resumed_skipped"]:
        lines.append(f"resumed (ok, пропущены): {', '.join(payload['resumed_skipped'])}")
    # Предупреждения (env-расхождения LLM, профиль ``split-hardened``) — видимы и в
    # человеческом выводе, не только в ``--json``.
    for warning in payload.get("warnings", ()):
        lines.append(f"warning: {warning}")
    if payload.get("blocked"):
        for error in payload.get("errors", ()):
            lines.append(f"error: {error}")
        lines.append(
            "добавьте seed: hound package seeds add --package <DIR> "
            "--product <ID> --url <URL> --actor <вы>"
        )
    if payload.get("crawl_truncation_warning"):
        lines.append(f"warning: {payload['crawl_truncation_warning']}")
    if payload["stopped_at"] == "onboarding":
        lines.append(
            "примечание: прогон остановлен до краула — продукт не готов (нет активных "
            "seed'ов); это не гейт G1/G2, запись gate-решения не поможет"
        )
    elif payload["stopped_at"] is not None:
        lines.append(
            "примечание: гейт требует явного человеческого решения (03 §E.2); "
            "продолжите после записи решения: hound gate --db <db> --gate <G1|G2> --actor <вы>"
        )
    sys.stdout.write("\n".join(lines) + "\n")


def _add_common(parser: argparse.ArgumentParser) -> None:
    """Общие флаги команд: ``--json``, ``--profile``, ``--env-file``."""
    parser.add_argument(
        "--json", action="store_true", help="машинночитаемый вывод (JSON) в stdout"
    )
    parser.add_argument(
        "--profile",
        choices=SUPPORTED_PROFILES,
        default=LOCAL_PROFILE,
        help="профиль развёртывания (07): local (дефолт) | split (DMZ, требует config/deploy.yaml)",
    )
    parser.add_argument(
        "--env-file",
        dest="env_file",
        default=None,
        metavar="PATH",
        help="dotenv-файл, загружаемый в окружение до исполнения команды "
        "(приоритет: --env-file > COHOUND_ENV_FILE > ./.env; уже заданные переменные "
        "не переопределяются; COHOUND_NO_DOTENV=1 отключает загрузку; 03 §D.6)",
    )


def _positive_int(raw: str) -> int:
    """``argparse``-тип: положительное целое (иначе ``--cutoff-k`` отвергается, I20).

    :raises argparse.ArgumentTypeError: нечисловое либо ``<= 0`` значение; ``argparse``
        печатает usage и завершает процесс кодом :data:`EXIT_USAGE`.
    """
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"ожидалось целое, получено {raw!r}") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError(f"значение обязано быть положительным, получено {raw!r}")
    return value


def _document_level(raw: str) -> DocumentLevel:
    """``argparse``-тип оси ``--document-level`` (P5): значение enum ``DocumentLevel``.

    :raises argparse.ArgumentTypeError: значение вне лестницы ``off|text|layout|layout_tables|full``.
    """
    try:
        return DocumentLevel(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"ожидался один из off|text|layout|layout_tables|full, получено {raw!r}"
        ) from exc


def _pdf_pipeline(raw: str) -> PdfPipeline:
    """``argparse``-тип оси ``--pdf-pipeline`` (P7): значение enum ``PdfPipeline``.

    :raises argparse.ArgumentTypeError: значение вне ``structured|text_layer``.
    """
    try:
        return PdfPipeline(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"ожидался один из structured|text_layer, получено {raw!r}"
        ) from exc


def _non_negative_int(raw: str) -> int:
    """``argparse``-тип: целое ``>= 0`` (для ``--max-depth``; 0 — только seeds, D1).

    :raises argparse.ArgumentTypeError: нечисловое либо отрицательное значение.
    """
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"ожидалось целое, получено {raw!r}") from exc
    if value < 0:
        raise argparse.ArgumentTypeError(
            f"значение обязано быть неотрицательным, получено {raw!r}"
        )
    return value


def _add_llm_slot_flag(parser: argparse.ArgumentParser) -> None:
    """Флаг ``--llm-slot`` для входов, считающих identity (§7.4/I21, §13).

    Ось ``llm_scenarios`` — декларированный вход контракта, поэтому ``plan``/``crawl``
    обязаны уметь получить то же переопределение, что ``run``: иначе ``hound plan``
    показал бы иной ``run_id``, чем прогон с ``--llm-slot``.
    """
    parser.add_argument(
        "--llm-slot",
        dest="llm_slot",
        choices=list(LLM_SLOTS),
        default=None,
        help="активный LLM-слот оценщика (override COHOUND_LLM_SLOT/deploy.llm.slot)",
    )


def _add_llm_model_flag(parser: argparse.ArgumentParser) -> None:
    """Флаг ``--llm-model`` для входов, считающих identity (§7.4/I21, §13).

    Модель оценщика — **объявленный** вход: она входит в ``llm_scenarios``, а значит в
    ``execution_contract_digest`` и ``run_id``. Env ``COHOUND_LLM_MODEL`` для оценщика не
    читается (это настройка LLM-разведки), поэтому единственный способ переопределить
    модель слота без правки ``deploy.yaml`` — этот флаг. Он питает **и** runtime-оценщик
    (``_resolve_ports`` → ``build_evaluator_llm``), **и** ``_resolve_llm_scenarios``: один
    вход, две проекции — контракт и рантайм не могут разойтись.

    ``plan``/``crawl`` получают флаг наравне с ``run`` по той же причине, что и
    ``--llm-slot``: они считают тот же ``run_id``, и иначе показали бы иную идентичность.
    """
    parser.add_argument(
        "--llm-model",
        dest="llm_model",
        default=None,
        metavar="MODEL_ID",
        help=(
            "model_id оценщика (override deploy.llm.<slot>.model_id); входит в run_id. "
            "Env COHOUND_LLM_MODEL оценщиком не читается (только LLM-разведка)"
        ),
    )


def _add_phase4_flags(parser: argparse.ArgumentParser) -> None:
    """Phase 4: флаги ``run``/``crawl run|resume|restart`` — кэш, фильтры, потолок, follow.

    ``--from-scratch`` — сброс сеанса + живой ре-фетч в обход R7-кэша (``cache=None``);
    ``--only-failed`` — ремонт провалов (``FAILED`` сеанса, иначе ``url_state``) известными
    depth-0 URL; ``--only-product`` — фильтр объёма; ``--max-attempts`` —
    потолок попыток по URL; ``--follow``/``--watch`` (+``--interval``) — прогресс до терминала.
    """
    parser.add_argument(
        "--from-scratch",
        dest="from_scratch",
        action="store_true",
        help="сброс сеанса + живой ре-фетч в обход R7-кэша (cache=None)",
    )
    parser.add_argument(
        "--only-failed",
        dest="only_failed",
        action="store_true",
        help="только провалы: FAILED текущего сеанса, иначе url_state прошлого прогона; "
        "ремонт идёт известными URL (авторские корни не обходятся)",
    )
    parser.add_argument(
        "--only-product",
        dest="only_product",
        default=None,
        metavar="ID",
        help="только указанный продукт из --urls",
    )
    parser.add_argument(
        "--max-attempts",
        dest="max_attempts",
        type=_positive_int,
        default=None,
        metavar="N",
        help="потолок попыток по URL; исчерпание → терминальный failed (attempts_exhausted)",
    )
    # D1/D2: оси контроля краула (run-слой поверх run.yaml; цепочка app < run < site).
    parser.add_argument(
        "--max-depth",
        dest="max_depth",
        type=_non_negative_int,
        default=None,
        metavar="D",
        help="глубина BFS-обхода in-site ссылок (0 — только seeds; дефолт 0)",
    )
    parser.add_argument(
        "--max-documents",
        dest="max_documents",
        type=_positive_int,
        default=None,
        metavar="N",
        help="потолок добытых страниц на (продукт, seed-root); усечение → partial-сеанс",
    )
    parser.add_argument(
        "--exclude-path",
        dest="exclude_paths",
        action="append",
        default=None,
        metavar="GLOB",
        help="glob блэклиста по пути URL (fnmatchcase, повторяемый)",
    )
    parser.add_argument(
        "--exclude-document",
        dest="exclude_documents",
        action="append",
        default=None,
        metavar="GLOB",
        help="glob блэклиста по basename URL, напр. '*.pdf' (повторяемый)",
    )
    # P5: уровень извлечения документных форматов (run-слой поверх run.yaml).
    parser.add_argument(
        "--document-level",
        dest="document_level",
        type=_document_level,
        default=None,
        metavar="LEVEL",
        help=(
            "уровень извлечения документов: off|text|layout|layout_tables|full "
            "(дефолт full = сегодняшнее поведение)"
        ),
    )
    # P7: механизм извлечения PDF (run-слой поверх run.yaml). ``document_level`` действует
    # только при ``structured``: у текстового слоя уровней нет.
    parser.add_argument(
        "--pdf-pipeline",
        dest="pdf_pipeline",
        type=_pdf_pipeline,
        default=None,
        metavar="PIPELINE",
        help=(
            "механизм извлечения PDF: structured|text_layer "
            "(дефолт structured = docling: layout/таблицы/OCR)"
        ),
    )
    # P8: потолок времени на извлечение одного документа (run-слой поверх run.yaml).
    # Дефолт ``None`` = ось не объявлена: резолв берёт app-слой, затем прежнее зашитое
    # значение (1800 с), поэтому объявление флага — единственный способ изменить `run_id`.
    parser.add_argument(
        "--document-timeout",
        dest="document_timeout_s",
        type=float,
        default=None,
        metavar="SECONDS",
        help=(
            "потолок времени на извлечение одного документа в секундах "
            "(дефолт 1800 = сегодняшнее поведение; ось входит в run_id при объявлении)"
        ),
    )
    parser.add_argument(
        "--host-concurrency",
        dest="host_concurrency",
        type=_positive_int,
        default=None,
        metavar="N",
        help=(
            "сколько host'ов процесс краула ведёт одновременно "
            "(дефолт: последовательно, как сегодня; ось входит в run_id при объявлении)"
        ),
    )
    parser.add_argument(
        "--follow",
        "--watch",
        dest="follow",
        action="store_true",
        help="печатать прогресс сеанса до терминального статуса",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        metavar="S",
        help="период опроса при --follow (секунды, дефолт 1.0)",
    )
