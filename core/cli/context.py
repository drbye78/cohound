"""coHound — общий контекст и DI-хелперы CLI (composition root Z0, ``03`` §E).

Разделяемые помощники команд, вынесенные из ``core.cli.main`` (P1-A): разрешение
раскладки/портов/фаз, загрузка входов (паспорта/корпус/снапшоты), печать отчётов,
диагностика DMZ-шва. Модуль не содержит хендлеров команд — только переиспользуемую
машинерию; команды в :mod:`core.cli.commands` импортируют её отсюда.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Callable, Mapping, Sequence
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
    manifest_deploy_fields,
    run_db_path,
)
from core.config import (
    PassportLoadError,
    load_golden,
    load_package_manifest,
    load_passports,
    passport_urls,
)
from core.config.deploy import LLM_SLOTS, DeployConfig
from core.domain.enums import (
    CrawlItemStatus,
    CrawlSessionStatus,
    DocumentLevel,
    Status,
)
from core.domain.models import CatalogRow, Cohort
from core.pipeline import PHASES, Ports, RunReport, build_phases
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.eval_cache import EvalVerdictCache
from core.pipeline.normalize import RunInputs, load_inputs
from core.pipeline.phases import resolve_package_id
from core.pipeline.plan import RunPlan, plan as build_plan
from core.pipeline.review import ReviewItem
from core.pipeline.runner import PhaseFn
from core.ports.gateway import ContentGateway
from core.ports.store import StorePort
from core.ports.types import FetchMode
from core.util.cache import ContentAddressedCache
from core.util.rate_limit import RateLimiter

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
#: Версия формата :data:`PLAN_INPUTS_FILENAME` (для будущей эволюции без поломки чтения).
PLAN_INPUTS_SNAPSHOT_VERSION = 1

__all__ = [
    # Типовые швы
    "GatewayFactory",
    "PhasesFactory",
    "PlanFactory",
    # DI-хелперы и машинерия команд
    "_resolve_paths",
    "_resolve_plan",
    "_resolve_ports",
    "_resolve_phases",
    "_open_state_store",
    "_load_passports_dir",
    "_load_corpus_urls",
    "_load_render_modes",
    "_load_package_roles",
    "_restore_plan_inputs",
    "_plan_inputs_from_payload",
    "_plan_inputs_sha256",
    "_read_cycle",
    "_infer_run_id",
    "_resolve_db_path",
    "_load_golden_arg",
    "_manifest_partial",
    "_artifacts_payload",
    "_review_item_payload",
    "_discover_seed_queries",
    "_diagnostic_gateway",
    "_seam_diagnostics",
    "_validate_health_payload",
    "_validate_capabilities_payload",
    "_short_json",
    "_phases_until",
    "_ensure_run",
    "_plan_payload",
    "_run_report_payload",
    "_plan_inputs_payload",
    "_snapshot_run_inputs",
    "_snapshot_run_yaml",
    "_snapshot_plan_inputs",
    "_emit_deploy_check",
    "_print_plan",
    "_print_run_dry",
    "_print_run_report",
    "_print_sensitivity",
    "_print_discover",
    "_print_review",
    "_print_promote",
    "_print_gate",
    "_print_passport_scout",
    "_add_common",
    "_positive_int",
    # Константы и приватные алиасы
    "_PROMOTE_STATUSES",
    "RUN_YAML_FILENAME",
    "PLAN_INPUTS_FILENAME",
    "PLAN_INPUTS_SNAPSHOT_VERSION",
    "_FALLBACK_CODE",
    "_concise",
    "_print_json",
    "_emit_error",
    "_emit_lint_failure",
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
    with build_store(db_path, profile=args.profile) as store:
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
        "i3_anomalies": list(getattr(artifacts, "i3_anomalies", None) or []),
        "crawl_truncation_anomalies": list(
            getattr(artifacts, "crawl_truncation_anomalies", None) or []
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
    """Человекочитаемый вывод ``review``: очередь ревью (Ш7/G3)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"queue: {payload['count']} item(s)",
    ]
    if not payload["items"]:
        lines.append("queue empty: нет ячеек, требующих ревью (preliminary/❓, Ш7/G3)")
    for item in payload["items"]:
        status = item["status"] if item["status"] is not None else "—"
        lines.append(
            f"- {item['product_id']}/{item['param_id']}\t{item['state']}\t"
            f"{status}\t{item['reason']}"
        )
    sys.stdout.write("\n".join(lines) + "\n")


def _print_gate(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``gate``: записанное человеческое решение (S9/I6/I7)."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"gate: {payload['gate']}",
        f"actor: {payload['actor']}",
        f"decided_at: {payload['decided_at']}",
        f"decision_id: {payload['decision_id']}",
    ]
    if payload.get("target") is not None:
        lines.append(f"target: {payload['target']}")
    if payload.get("notes") is not None:
        lines.append(f"notes: {payload['notes']}")
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
        catalog, module_weights, cohort = restored
        notes.append("каталог/веса/когорта восстановлены из БД прогона")
        return catalog, module_weights, cohort, notes

    notes.append(
        "входы плана (каталог/веса/когорта) отсутствуют в БД прогона — "
        f"{purpose} рассчитан с пустым каталогом (вес ячейки = 1.0) и без когорты; "
        "передайте --run-yaml и --package для точного воспроизведения"
    )
    return {}, {}, None, notes


def _plan_inputs_from_payload(
    data: Any, run_id: str
) -> tuple[dict[str, Any], dict[str, float], Any] | None:
    """Валидирует разобранный снимок входов плана и восстанавливает его значения.

    Возвращает ``(catalog, module_weights, cohort)`` либо ``None`` при отсутствии/
    частичности/несовместимой версии снимка или ``run_id``-несоответствии. Несовпадение
    ``run_id`` трактуется как чужой снимок: значения не подставляются (не выдумываем).

    Единая точка валидации формата снимка (``snapshot_version``/``run_id`` + состав),
    общая для чтения из БД (``StorePort.read_plan_inputs``) и производного экспорта
    ``plan_inputs.json`` (:func:`_snapshot_plan_inputs`).
    """
    if not isinstance(data, Mapping):
        return None
    if data.get("snapshot_version") != PLAN_INPUTS_SNAPSHOT_VERSION:
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
    return catalog, module_weights, cohort


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
) -> list[str]:
    """Пишет снимок входов прогона: в БД прогона (источник истины) и в ``runs/{run_id}/``.

    Состав:

    * ``run_inputs`` (БД, P1) — **источник истины** самодостаточности ``runs/{run_id}``:
      пост-хок команды восстанавливают каталог/веса/когорту без исходного пакета и без
      side-файла (``StorePort.save_plan_inputs``; дайджест — :func:`_plan_inputs_sha256`);
    * ``run.yaml`` — **иммутабельная копия** входа (байт-в-байт); существующий
      отличающийся файл **не** перезаписывается (иначе мутировал бы завершённый
      прогон), идентичный — оставляется как есть;
    * ``plan_inputs.json`` — **производный экспорт** того же снимка (детерминированный
      JSON, ``sort_keys=True``/``ensure_ascii=False``); читатели на него больше не
      опираются (источник — БД), но файл сохраняется как человекочитаемый артефакт.

    Никогда не бросает исключений: любая ошибка записи возвращается строкой для
    ``instrumentation_errors`` прогона (сбой снимка не подменяет код возврата).
    """
    run_dir = Path(args.runs_dir) / run_id
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return [f"snapshot: не удалось создать каталог прогона {run_dir}: {_concise(exc)}"]

    payload = _plan_inputs_payload(run_id, inputs=inputs, run_plan=run_plan)
    issues = _snapshot_run_yaml(args.run_yaml, run_dir)
    issues.extend(_snapshot_plan_inputs(run_dir, payload))
    if store is not None:
        try:
            store.save_plan_inputs(
                run_id, payload=payload, payload_sha256=_plan_inputs_sha256(payload)
            )
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
    больше не читается пост-хок командами.
    """
    target = run_dir / PLAN_INPUTS_FILENAME
    try:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        target.write_text(text, encoding="utf-8")
    except (OSError, TypeError, ValueError) as exc:
        return [f"snapshot: не удалось записать {target}: {_concise(exc)}"]
    return []


def _plan_inputs_payload(
    run_id: str, *, inputs: RunInputs, run_plan: RunPlan
) -> dict[str, Any]:
    """Сериализует минимальный снимок входов плана (без всего :class:`RunInputs`).

    Ключи: ``snapshot_version`` (версия формата), идентичность прогона
    (``run_id``/``case_id``/``package_id``/``package_version``), и то, что нужно для
    пересборки без пакета — ``params`` (``CatalogRow.model_dump(mode="json")``),
    ``module_weights``/``renormalized_weights`` и ``cohort`` (``Cohort``). Ключей
    ``params``/``cohort`` достаточно для :func:`_plan_inputs_from_payload`; прочие —
    провенанс, а не «истина о продукте».
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


def _load_registry_specs(
    args: argparse.Namespace, inputs: RunInputs | None
) -> dict[str, RegistryConfig]:
    """Загружает package-scoped спецификации реестров из ``package.registries`` (E2, 03 §D.4).

    Composition root (Z0) — единственное место загрузки спецификаций; pipeline получает уже
    готовый mapping фетчеров (``Ports.registry_fetchers``). Пакет без объявленных реестров
    (или без ``inputs``/``--package``) → ``{}``: реестры опциональны. Объявленный, но
    нечитаемый/отсутствующий реестр — **понятная ошибка CLI**, а не молчаливая частичная
    проводка (объявленный реестр без данных недопустим).

    :raises CliError: объявленная спецификация реестра отсутствует/невалидна.
    """
    package_dir = getattr(args, "package", None)
    if inputs is None or not package_dir:
        return {}
    declared = list(getattr(getattr(inputs, "package", None), "registries", None) or [])
    if not declared:
        return {}
    try:
        return load_package_registry_configs(package_dir, declared)
    except RegistryConfigError as exc:
        raise CliError(
            f"реестры пакета: {exc}", code="registries", exit_code=EXIT_USAGE
        ) from exc


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
    cache = (
        None
        if no_cache
        else (ContentAddressedCache(paths.cache) if paths is not None else None)
    )
    evaluator = (
        build_evaluator_llm(
            deploy_llm=deploy_config.llm,
            slot=getattr(args, "llm_slot", None),
            run_slot=getattr(getattr(inputs, "run", None), "llm_slot", None),
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
        cli_max_depth=getattr(args, "max_depth", None),
        # P4a/I17: store — источник персистентных чанков для лексического индекса и
        # резолвер метаданных для dense-плеча. Без него оба билдера возвращают ``None``.
        store=store,
        chunk_lookup=store.get_chunk if store is not None else None,
        index_build_id=index_build_id,
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
        if p.suffix.lower() == ".json":
            data = json.loads(text)
        else:
            data = yaml.safe_load(text)
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


#: Phase 4: терминальные статусы сеанса краула (дальнейший опрос бесполезен).
_TERMINAL_SESSION_STATUSES: frozenset[CrawlSessionStatus] = frozenset(
    {
        CrawlSessionStatus.COMPLETED,
        CrawlSessionStatus.PARTIAL,
        CrawlSessionStatus.CANCELLED,
        CrawlSessionStatus.FAILED,
    }
)


def _is_terminal_session_status(status: str | None) -> bool:
    """Phase 4: терминален ли статус сеанса (``completed|partial|cancelled|failed``)."""
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
        if p.suffix.lower() == ".json":
            data = json.loads(text)
        else:
            data = yaml.safe_load(text)
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


def _resolve_phases(
    args: argparse.Namespace, *, inputs: RunInputs, plan: RunPlan, ports: Ports,
    deploy_config: DeployConfig | None = None,
    paths: Any = None,
    render_modes: Mapping[str, FetchMode] | None = None,
    urls: Mapping[str, list[str]] | None = None,
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
    max_depth: int | None = None,
    max_documents: int | None = None,
    exclude_paths: Sequence[str] | None = None,
    exclude_documents: Sequence[str] | None = None,
    document_level: DocumentLevel | None = None,
    state: Any = None,
    product_modes: Mapping[str, str] | None = None,
    refresh_policy: str = "auto",
    execution_contract_digest: str | None = None,
    axes: Any = None,
    declared_status_dict_version: str | None = None,
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
    :param state: открытый ``ProductStatePort`` пакета (09 §4.1); ``None`` → инкрементальный
        субстрат корпуса выключен (dry-run/``crawl`` без состояния).
    :param product_modes: ``product_id → mode`` из onboarding-плана; ``None`` → все ``new``.
    :param refresh_policy: политика наблюдения прогона (значение ``RefreshPolicy``).
    :param execution_contract_digest: готовый ``execution_contract_digest`` прогона
        (09 §12.5), вычисленный composition root до исполнения фаз; протягивается в
        манифест Ш9 без пересчёта (``None`` — неизвестен, значение не выдумывается).
    :param axes: декларированные оси исполнения (см.
        :func:`core.cli.execution_axes.resolve_declared_axes`) — тот же резолвер, что у
        контракта; из них манифест берёт deploy-профиль/чанкер/оси краула/реестры/
        ``llm_scenarios``. ``None`` — legacy/модульный вызов.
    :param declared_status_dict_version: версия словаря статусов (декларированная ось).
    """
    passports_dir = getattr(args, "passports_dir", None)
    if passports_dir is None and paths is not None:
        passports_dir = str(paths.passports)
    if passports_dir is None:
        passports_dir = "passports"
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
    # P4/OOM: run-дефолт рециклинга воркеров экстракции захватывается фабрикой пула;
    # per-site значение резолвится в pipeline (site > run > app) и передаётся аргументом.
    run_extract_max_tasks = getattr(
        getattr(getattr(inputs, "run", None), "crawl", None),
        "extract_max_tasks_per_child",
        None,
    )
    kwargs: dict[str, Any] = {
        "inputs": inputs,
        "plan": plan,
        "ports": ports,
        "deploy_profile": deploy_profile,
        "edge_schema_version": edge_schema_version,
        "edge_capabilities": edge_capabilities,
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
        # D1/D2: CLI-уточнения run-слоя осей краула (пробрасываются в build_phases →
        # run-слой CrawlRunConfig) и application-слой (deploy.yaml:crawl).
        "max_depth": max_depth,
        "max_documents": max_documents,
        "exclude_paths": list(exclude_paths) if exclude_paths else None,
        "exclude_documents": list(exclude_documents) if exclude_documents else None,
        # P5: CLI-уточнение run-слоя уровня извлечения документов (--document-level).
        "document_level": document_level,
        # 09 §6.3 (MVP-3b): durable-субстрат пакета, режимы продуктов и политика наблюдения
        # → build_corpus. ``state=None`` (dry-run/``crawl``) — поведение байт-в-байт.
        "state": state,
        "product_modes": product_modes,
        "refresh_policy": refresh_policy,
        # 09 §12.5: готовый отпечаток условий измерения → манифест Ш9 (без пересчёта).
        "execution_contract_digest": execution_contract_digest,
        # §13 (2.4.0): декларированные оси исполнения — один резолвер с контрактом
        # (deploy-профиль/чанкер/оси краула/реестры/llm_scenarios).
        "axes": axes,
        "declared_status_dict_version": declared_status_dict_version,
        "crawl_app": deploy_config.crawl if deploy_config is not None else None,
        # P4: исполнитель параллельной экстракции по профилю зон (local — процессы,
        # split — потоки поверх RemoteExtractor). Ось ``crawl.extract_workers`` решается
        # в pipeline; при ``None``/``1`` пул не создаётся (поведение байт-в-байт).
        "extraction_pool": build_extraction_pool(
            deploy_config.profile if deploy_config is not None else LOCAL_PROFILE,
            extractor=getattr(ports, "extractor", None),
            max_tasks_per_child=run_extract_max_tasks,
        ),
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
        # ⇒ выключен, по той же причине, что и выше. Ключ — отрендеренный промпт +
        # конфигурация оценщика, поэтому смена сателлитов/заголовка/чанков/модели не даёт
        # ложных хитов.
        "eval_cache": (
            EvalVerdictCache(str(paths.eval_cache)) if paths is not None else None
        ),
        # P5: фабрика per-level экстрактора для inline-пути. Под ``split`` ось
        # ``document_level`` не действует (R-S: edge строит конвертеры сам) — ``None``.
        "extractor_factory": (
            build_worker_extractor
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
    """Сериализует :class:`RunPlan` в JSON-совместимый словарь."""
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
        "errors": list(report.errors),
        "results": [
            {
                "phase": item.phase,
                "status": item.status,
                "skip_reason": (
                    item.skip_reason.value if item.skip_reason is not None else None
                ),
                "pending": list(item.pending),
                "errors": list(item.errors),
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
        f"golden: {payload['golden_count']}",
        f"dry-run: {'yes' if payload.get('dry_run') else 'no'}",
    ]
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
            "seed'ов); это не гейт G1–G4, запись gate-решения не поможет"
        )
    elif payload["stopped_at"] is not None:
        lines.append(
            "примечание: гейт требует явного человеческого решения (03 §E.2); "
            "продолжите после записи решения: hound gate --db <db> --gate <G1..G4> --actor <вы>"
        )
    sys.stdout.write("\n".join(lines) + "\n")


def _add_common(parser: argparse.ArgumentParser) -> None:
    """Общие флаги команд: ``--json`` и ``--profile``."""
    parser.add_argument(
        "--json", action="store_true", help="машинночитаемый вывод (JSON) в stdout"
    )
    parser.add_argument(
        "--profile",
        choices=SUPPORTED_PROFILES,
        default=LOCAL_PROFILE,
        help="профиль развёртывания (07): local (дефолт) | split (DMZ, требует config/deploy.yaml)",
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


def _add_phase4_flags(parser: argparse.ArgumentParser) -> None:
    """Phase 4: флаги ``run``/``crawl run|resume|restart`` — кэш, фильтры, потолок, follow.

    ``--from-scratch`` — сброс сеанса + живой ре-фетч в обход R7-кэша (``cache=None``);
    ``--only-failed``/``--only-product`` — фильтр URL по durable-сеансу; ``--max-attempts`` —
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
        help="только URL со статусом failed в сеансе (нет сеанса → обычный прогон)",
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
