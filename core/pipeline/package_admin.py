"""coHound — CRUD-операции над пакетом категории (итерация I1).

Операции авторского редактирования пакета: создание из шаблона, модули, роли, каталог,
архетип и версия. Каждая мутация подчиняется **главному инварианту записи**:

1. прочитать артефакты (``load_artifacts``);
2. посчитать линт **до** (``prospective_lint``);
3. мутировать **копию** in-memory состояния;
4. построить проспективные ``RunInputs`` и прогреть линт **после**;
5. если появились **новые** блокирующие нарушения — отказать
   (:class:`PackageAdminError`), **ничего не записав**;
6. иначе (и если не ``dry_run``) детерминированно записать изменённые артефакты и
   добавить строку в ``.authoring/apply_log.jsonl``.

Никакого auto-approve (A3): разведка (:mod:`core.pipeline.package_discovery`) отдаёт лишь
кандидатов; зафиксировать данные можно только явными CRUD-операциями этого модуля.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.domain.enums import (
    AbsenceRuleType,
    AnchorRelation,
    Archetype,
    Importance,
    InheritanceMode,
    ParamType,
    RunMode,
)
from core.domain.models import (
    BaselineRow,
    CatalogRow,
    Cohort,
    CohortEntry,
    GoldenRow,
    ModuleDef,
    PackageManifest,
    RoleDef,
    RunConfig,
)
from core.pipeline.package_io import (
    PackageArtifacts,
    append_apply_log,
    assert_no_new_blocking,
    build_run_inputs,
    delete_artifact,
    get_artifact,
    list_artifacts,
    load_artifacts,
    matches,
    prospective_lint,
    save_artifact,
    validate_case_id,
)
from core.pipeline.normalize import cross_check, load_scope_attestation
from core.util.security import sanitize_slug

__all__ = [
    "OpResult",
    "PackageAdminError",
    "create_package",
    "show_package",
    "validate_package",
    "set_archetype",
    "bump_version",
    "add_module",
    "update_module",
    "remove_module",
    "renormalize_modules",
    "get_module",
    "list_modules",
    "add_role",
    "update_role",
    "remove_role",
    "get_role",
    "list_roles",
    "add_param",
    "update_param",
    "remove_param",
    "get_param",
    "list_params",
    "get_baseline",
    "find_baselines",
    "list_baselines",
    "add_baseline",
    "update_baseline",
    "remove_baseline",
    "set_cohort",
    "update_cohort",
    "get_cohort",
    "get_scope_attestation",
    "set_scope_attestation",
    "clear_scope_attestation",
    "list_golden",
    "get_run_config",
    "list_artifacts",
    "get_artifact",
]

#: Идентификаторы пакетных сущностей — ``[a-z0-9_]+``.
_IDENT_RE = re.compile(r"^[a-z0-9_]+$")


class PackageAdminError(RuntimeError):
    """Ошибка авторской операции над пакетом.

    :param kind: ``"usage"`` (битый ввод/enum/неизвестная сущность) или ``"blocked"``
        (операция запрещена инвариантом; возможен непустой ``blocking``). CLI маппит
        ``usage`` → :data:`EXIT_USAGE`, ``blocked`` → :data:`EXIT_BLOCKED`.
    :param blocking: блокирующие сообщения (новые нарушения или причина отказа).
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "package",
        kind: str = "usage",
        blocking: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.kind = kind
        self.blocking = list(blocking or [])


@dataclass
class OpResult:
    """Результат авторской операции: что изменилось, что записано, итог линта.

    :param changed: имена изменённых артефактов (ключи ``package_io.ARTIFACT_FILES``).
    :param written: фактически записанные пути (пусто при ``dry_run``).
    :param lint: ``{"blocking": [], "warnings": [...]}`` — после операции блокирующих нет
        по построению (иначе поднят :class:`PackageAdminError`); warnings допустимы.
    :param dry_run: запись не производилась.
    :param warnings: предупреждения линта (и дополнительные §13-предупреждения).
    :param incomplete: авторские поля, не заполненные человеком (A3); не блокируют
        операцию, но запрещают прогон (``LintReport.ready``).
    :param created_id: идентификатор созданной сущности (расширение I1; ``param_id`` и т.п.).
    """

    changed: list[str] = field(default_factory=list)
    written: list[Path] = field(default_factory=list)
    lint: dict[str, list[str]] = field(default_factory=dict)
    dry_run: bool = False
    warnings: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)
    created_id: str | None = None


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _now() -> str:
    """UTC-метка времени авторской операции (для журнала; не влияет на артефакты)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _validate_ident(value: str, label: str) -> str:
    """Проверяет идентификатор на ``[a-z0-9_]+``."""
    if not value or not _IDENT_RE.match(value):
        raise PackageAdminError(
            f"{label} должен соответствовать [a-z0-9_]+: {value!r}", kind="usage"
        )
    return value


def _template_dir(override: str | Path | None = None) -> Path:
    """Каталог шаблона пакета (по умолчанию ``packages/_template`` репозитория)."""
    if override is not None:
        return Path(override)
    root = Path(__file__).resolve().parents[2]
    return root / "packages" / "_template"


def _load(package_dir: str | Path) -> PackageArtifacts:
    """Читает артефакты; ``package.yaml`` обязателен для авторских операций."""
    artifacts = load_artifacts(package_dir)
    if artifacts.manifest is None:
        raise PackageAdminError(
            f"в {package_dir} отсутствует package.yaml — это не пакет категории", kind="usage"
        )
    return artifacts


def _artifact_data(artifacts: PackageArtifacts, name: str) -> Any:
    """Данные артефакта по имени для :func:`save_artifact`."""
    return {
        "package": artifacts.manifest,
        "catalog": artifacts.catalog,
        "baselines": artifacts.baselines,
        "cohort": artifacts.cohort,
        "golden": artifacts.golden,
        "domain_profiles": artifacts.domain_profiles,
        "scope_attestation": artifacts.scope_attestation,
        "run": artifacts.run,
    }[name]


def _rebuild_manifest(manifest: PackageManifest, **updates: Any) -> PackageManifest:
    """Пересобирает манифест с валидаторами (``model_copy`` их не применяет)."""
    return PackageManifest.model_validate({**manifest.model_dump(mode="python"), **updates})


def _finalize(
    package_dir: str | Path,
    artifacts: PackageArtifacts,
    before: Any,
    changed: list[str],
    *,
    op: str,
    actor: str | None,
    dry_run: bool,
    extra_warnings: list[str] | None = None,
    deleted: list[str] | None = None,
) -> OpResult:
    """Общий хвост мутации: проспективный линт → отказ/запись → журнал.

    ``deleted`` — имена артефактов, файлы которых требуется удалить (например,
    ``clear_scope_attestation``); удаление выполняется только после успешного линта и
    только при ``not dry_run``.
    """
    after = prospective_lint(artifacts)
    new_blocking = assert_no_new_blocking(before, after)
    if new_blocking:
        raise PackageAdminError(
            "новые блокирующие нарушения: " + "; ".join(new_blocking),
            code="lint",
            kind="blocked",
            blocking=new_blocking,
        )
    warnings = list(extra_warnings or []) + list(after.warnings)
    written: list[Path] = []
    removed = list(deleted or [])
    if not dry_run:
        for name in changed:
            data = _artifact_data(artifacts, name)
            if data is None:
                continue
            written.append(save_artifact(package_dir, name, data))
        for name in removed:
            delete_artifact(package_dir, name)
        append_apply_log(
            package_dir,
            {"op": op, "actor": actor, "changed": list(changed), "deleted": removed, "ts": _now()},
        )
    return OpResult(
        changed=[*changed, *removed],
        written=written,
        lint={"blocking": [], "warnings": warnings},
        dry_run=dry_run,
        warnings=warnings,
        incomplete=list(after.incomplete),
    )


def _empty_artifacts(case_id: str, archetype: Archetype) -> PackageArtifacts:
    """Минимальный валидный пакет (``from_template=False``)."""
    manifest = PackageManifest(
        case_id=case_id,
        package_version="0.0.0",
        archetype=archetype,
        modules=[ModuleDef(module_id="core", weight=100.0)],
        roles=[],
        baselines=[],
        baseline_participant=None,
    )
    frozen = _dt.date(1970, 1, 1)
    cohort = Cohort(
        case_id=case_id,
        frozen_at=frozen,
        entries=[
            CohortEntry(
                product_id="anchor",
                role_overlap=1.0,
                selection_reason=None,
                anchor_relation=AnchorRelation.ANCHOR,
            )
        ],
    )
    run = RunConfig(
        case_id=case_id,
        anchor="anchor",
        cohort=f"cohort_{case_id}",
        products=["anchor"],
        package=case_id,
        snapshot_date=frozen,
        mode=RunMode.STRICT,
    )
    return PackageArtifacts(manifest=manifest, cohort=cohort, run=run)


def _renormalize_100(declared: dict[str, float]) -> dict[str, float]:
    """Перенормирует веса модулей к Σ = 100 (2 знака, остаток — последнему по сортировке).

    Детерминированное зеркало ``plan._renormalize`` (там — к Σ = 1.0): гарантирует
    соблюдение инварианта Σ=100 для пакета после структурных правок.
    """
    if not declared:
        return {}
    total = sum(declared.values())
    if total <= 0:
        share = round(100.0 / len(declared), 2)
        weights = {key: share for key in declared}
    else:
        weights = {key: round(value * 100.0 / total, 2) for key, value in declared.items()}
    residual = round(100.0 - round(sum(weights.values()), 2), 2)
    if residual:
        last = sorted(weights)[-1]
        weights[last] = round(weights[last] + residual, 2)
    return weights


def _apply_weights(modules: list[ModuleDef], *, reassign: dict[str, float] | None = None) -> list[ModuleDef]:
    """Возвращает модули с перенормированными весами (Σ = 100)."""
    declared = {module.module_id: module.weight for module in modules}
    if reassign:
        declared.update(reassign)
    weights = _renormalize_100(declared)
    return [module.model_copy(update={"weight": weights[module.module_id]}) for module in modules]


def _parse_semver(text: str) -> tuple[int, int, int]:
    """Разбирает ``X.Y.Z`` в тройку int (терпимо к отсутствующим частям)."""
    parts = re.findall(r"\d+", text or "")
    if not parts:
        raise PackageAdminError(f"некорректная версия пакета: {text!r}", kind="usage")
    numbers = [int(part) for part in parts[:3]]
    while len(numbers) < 3:
        numbers.append(0)
    return numbers[0], numbers[1], numbers[2]


# ---------------------------------------------------------------------------
# Создание / чтение / валидация
# ---------------------------------------------------------------------------


def create_package(
    case_id: str,
    archetype: str,
    *,
    dir: str | Path = "packages",
    from_template: bool = True,
    template_dir: str | Path | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Создаёт пакет ``packages/{case_id}/`` (§4.2). Ничего не пишет при ``dry_run``.

    По умолчанию копирует ``packages/_template/`` и заменяет ``case_id`` в
    ``package.yaml``/``run.yaml``/``cohort.yaml``, а также архетип. Новый пакет обязан
    пройти линт без блокирующих нарушений (иначе отказ без записи).

    ``dir`` — каталог пакетов; дефолт ``"packages"`` совпадает с дефолтом раскладки
    (:class:`~core.config.paths.PathsConfig`). Composition root может передать эффективный
    каталог из :func:`core.config.paths.resolve_paths` (``paths.packages``, 03 §A.1):
    приоритет CLI-флаг > env > ``paths.yaml`` > дефолт.
    """
    validate_case_id(case_id)
    try:
        arch = Archetype(archetype)
    except ValueError as exc:
        raise PackageAdminError(
            f"неизвестный архетип: {archetype!r} (ожидается A|B|C)", kind="usage"
        ) from exc

    dest = Path(dir).expanduser().resolve() / case_id
    if dest.exists():
        raise PackageAdminError(f"пакет уже существует: {dest}", kind="usage")

    if from_template:
        source = _template_dir(template_dir)
        if not source.is_dir():
            raise PackageAdminError(f"шаблон пакета не найден: {source}", kind="usage")
        artifacts = load_artifacts(source)
        if artifacts.manifest is None:
            raise PackageAdminError(f"шаблон {source} не содержит package.yaml", kind="usage")
        artifacts.manifest = _rebuild_manifest(
            artifacts.manifest, case_id=case_id, archetype=arch
        )
        if artifacts.cohort is not None:
            artifacts.cohort = artifacts.cohort.model_copy(update={"case_id": case_id})
        if artifacts.run is not None:
            artifacts.run = artifacts.run.model_copy(
                update={"case_id": case_id, "package": case_id, "cohort": f"cohort_{case_id}"}
            )
        changed = [name for name in ("package", "catalog", "baselines", "cohort", "golden", "run") if _artifact_data(artifacts, name) is not None]
    else:
        artifacts = _empty_artifacts(case_id, arch)
        changed = ["package", "catalog", "baselines", "cohort", "golden", "run"]

    report = prospective_lint(artifacts)
    if report.blocking:
        raise PackageAdminError(
            "создаваемый пакет не проходит линт: " + "; ".join(report.blocking),
            code="lint",
            kind="blocked",
            blocking=list(report.blocking),
        )

    warnings = list(report.warnings)
    written: list[Path] = []
    if not dry_run:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if from_template:
            shutil.copytree(_template_dir(template_dir), dest)
        else:
            dest.mkdir(parents=True, exist_ok=False)
        for name in changed:
            data = _artifact_data(artifacts, name)
            if data is None:
                continue
            written.append(save_artifact(dest, name, data))
        append_apply_log(dest, {"op": "new", "actor": actor, "changed": changed, "ts": _now()})
        warnings.insert(0, f"создан пакет: {dest}")
    return OpResult(
        changed=changed,
        written=written,
        lint={"blocking": [], "warnings": warnings},
        dry_run=dry_run,
        warnings=warnings,
        incomplete=list(report.incomplete),
    )


def show_package(package_dir: str | Path) -> dict[str, Any]:
    """Сводка по пакету (read-only): счётчики, линт, ``cross_check`` (S11). Ничего не пишет."""
    artifacts = load_artifacts(package_dir)
    report = prospective_lint(artifacts)
    inputs = build_run_inputs(artifacts)
    manifest = artifacts.manifest
    return {
        "path": str(Path(package_dir)),
        "case_id": manifest.case_id if manifest is not None else None,
        "package_version": manifest.package_version if manifest is not None else None,
        "archetype": manifest.archetype.value if manifest is not None else None,
        "modules": len(manifest.modules) if manifest is not None else 0,
        "roles": len(manifest.roles) if manifest is not None else 0,
        "catalog": len(artifacts.catalog),
        "deprecated_params": sorted(row.param_id for row in artifacts.catalog if row.deprecated),
        "baselines": len(artifacts.baselines),
        "golden": len(artifacts.golden),
        "cohort": len(artifacts.cohort.entries) if artifacts.cohort is not None else 0,
        "lint": {"blocking": list(report.blocking), "warnings": list(report.warnings)},
        "incomplete": list(report.incomplete),
        "complete": report.complete,
        "cross_check": cross_check(inputs),
        "ok": report.ok,
    }


def validate_package(package_dir: str | Path) -> OpResult:
    """Линт пакета без записи; ``blocking``/``warnings`` в :class:`OpResult`.

    Ошибки разбора превращаются в блокирующее сообщение (а не исключение), чтобы
    команда ``validate`` всегда возвращала отчёт.
    """
    try:
        artifacts = load_artifacts(package_dir)
        report = prospective_lint(artifacts)
    except (ValueError, OSError) as exc:
        message = " ".join(str(exc).split())
        return OpResult(
            changed=[],
            written=[],
            lint={"blocking": [message], "warnings": []},
            dry_run=True,
            warnings=[],
        )
    return OpResult(
        changed=[],
        written=[],
        lint={"blocking": list(report.blocking), "warnings": list(report.warnings)},
        dry_run=True,
        warnings=list(report.warnings),
        incomplete=list(report.incomplete),
    )


# ---------------------------------------------------------------------------
# Архетип / версия
# ---------------------------------------------------------------------------


def set_archetype(
    package_dir: str | Path,
    archetype: str,
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Устанавливает архетип пакета. Архетип C запрещён при непустых baselines (§1.5)."""
    try:
        arch = Archetype(archetype)
    except ValueError as exc:
        raise PackageAdminError(
            f"неизвестный архетип: {archetype!r} (ожидается A|B|C)", kind="usage"
        ) from exc
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    if arch is Archetype.C and (artifacts.manifest.baselines or artifacts.baselines):
        raise PackageAdminError(
            "archetype-c-baselines: архетип C не заполняет baselines (§1.5/§4.2)",
            code="archetype-c-baselines",
            kind="blocked",
            blocking=["archetype-c-baselines: архетип C не заполняет baselines (§1.5/§4.2)"],
        )
    updates: dict[str, Any] = {"archetype": arch}
    if arch is Archetype.C:
        updates["baseline_participant"] = None
    artifacts.manifest = _rebuild_manifest(artifacts.manifest, **updates)
    return _finalize(
        package_dir, artifacts, before, ["package"], op="set-archetype", actor=actor, dry_run=dry_run
    )


def bump_version(
    package_dir: str | Path,
    *,
    level: str,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Инкрементирует ``package_version`` (semver). ``major`` → warning о границе §13."""
    if level not in ("major", "minor", "patch"):
        raise PackageAdminError(
            f"level должен быть major|minor|patch: {level!r}", kind="usage"
        )
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    major, minor, patch = _parse_semver(artifacts.manifest.package_version)
    if level == "major":
        major, minor, patch = major + 1, 0, 0
    elif level == "minor":
        minor, patch = minor + 1, 0
    else:
        patch += 1
    extra = (
        ["§13: major bump — версионная граница; обязателен golden-full"]
        if level == "major"
        else []
    )
    artifacts.manifest = _rebuild_manifest(
        artifacts.manifest, package_version=f"{major}.{minor}.{patch}"
    )
    return _finalize(
        package_dir,
        artifacts,
        before,
        ["package"],
        op="bump-version",
        actor=actor,
        dry_run=dry_run,
        extra_warnings=extra,
    )


# ---------------------------------------------------------------------------
# Модули
# ---------------------------------------------------------------------------


def add_module(
    package_dir: str | Path,
    module_id: str,
    weight: float,
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Добавляет модуль и перенормирует веса к Σ = 100 (§4.2)."""
    _validate_ident(module_id, "module_id")
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    if any(module.module_id == module_id for module in artifacts.manifest.modules):
        raise PackageAdminError(f"модуль уже существует: {module_id}", kind="usage")
    try:
        candidate = ModuleDef(module_id=module_id, weight=float(weight))
    except ValueError as exc:
        raise PackageAdminError(f"module weight: {exc}", kind="usage") from exc
    modules = _apply_weights([*artifacts.manifest.modules, candidate])
    artifacts.manifest = _rebuild_manifest(artifacts.manifest, modules=modules)
    return _finalize(
        package_dir, artifacts, before, ["package"], op="module-add", actor=actor, dry_run=dry_run
    )


def update_module(
    package_dir: str | Path,
    module_id: str,
    weight: float,
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Меняет вес модуля; остальные перенормируются так, чтобы Σ = 100 (§4.2)."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    if not any(module.module_id == module_id for module in artifacts.manifest.modules):
        raise PackageAdminError(f"модуль не найден: {module_id}", kind="usage")
    try:
        float(weight)
    except (TypeError, ValueError) as exc:
        raise PackageAdminError(f"module weight: {exc}", kind="usage") from exc
    modules = [
        module.model_copy(update={"weight": float(weight)}) if module.module_id == module_id else module
        for module in artifacts.manifest.modules
    ]
    modules = _apply_weights(modules)
    artifacts.manifest = _rebuild_manifest(artifacts.manifest, modules=modules)
    return _finalize(
        package_dir, artifacts, before, ["package"], op="module-update", actor=actor, dry_run=dry_run
    )


def remove_module(
    package_dir: str | Path,
    module_id: str,
    *,
    reassign_to: str | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Удаляет модуль. Если на него ссылается каталог — нужен ``reassign_to`` (§4.2)."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    if not any(module.module_id == module_id for module in artifacts.manifest.modules):
        raise PackageAdminError(f"модуль не найден: {module_id}", kind="usage")
    affected = sorted(row.param_id for row in artifacts.catalog if row.module == module_id)
    if affected and reassign_to is None:
        raise PackageAdminError(
            f"module-in-use: модуль '{module_id}' используется параметрами: "
            f"{', '.join(affected)}; укажите --reassign-to",
            code="module-in-use",
            kind="blocked",
            blocking=[f"module-in-use: {module_id} <- {', '.join(affected)}"],
        )
    remaining = [module for module in artifacts.manifest.modules if module.module_id != module_id]
    if not remaining:
        raise PackageAdminError("нельзя удалить последний модуль пакета", kind="usage")
    changed = ["package"]
    if reassign_to is not None:
        if reassign_to not in {module.module_id for module in remaining}:
            raise PackageAdminError(
                f"модуль для переназначения не найден: {reassign_to}", kind="usage"
            )
        artifacts.catalog = [
            row.model_copy(update={"module": reassign_to}) if row.module == module_id else row
            for row in artifacts.catalog
        ]
        changed.append("catalog")
    modules = _apply_weights(remaining)
    artifacts.manifest = _rebuild_manifest(artifacts.manifest, modules=modules)
    return _finalize(
        package_dir, artifacts, before, changed, op="module-remove", actor=actor, dry_run=dry_run
    )


def renormalize_modules(
    package_dir: str | Path,
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Перенормирует веса модулей к Σ = 100 (§4.2)."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    modules = _apply_weights(list(artifacts.manifest.modules))
    artifacts.manifest = _rebuild_manifest(artifacts.manifest, modules=modules)
    return _finalize(
        package_dir,
        artifacts,
        before,
        ["package"],
        op="module-renormalize",
        actor=actor,
        dry_run=dry_run,
    )


def get_module(package_dir: str | Path, module_id: str) -> ModuleDef | None:
    """Возвращает модуль по ``module_id`` или ``None`` (read-only).

    Отсутствие пакета/``package.yaml`` или модуля — ``None`` (CLI печатает ``not found``);
    значения не выдумываются. Невалидный присутствующий ``package.yaml`` поднимает
    :class:`ValueError` (не маскируется под отсутствие).
    """
    artifacts = load_artifacts(package_dir)
    if artifacts.manifest is None:
        return None
    return next(
        (module for module in artifacts.manifest.modules if module.module_id == module_id), None
    )


def list_modules(package_dir: str | Path, *, query: str | None = None) -> list[ModuleDef]:
    """Список модулей пакета (read-only) из ``package.yaml``.

    ``package.yaml`` обязателен — иначе :class:`PackageAdminError` (kind ``usage``):
    без манифеста состав модулей неизвестен и не выдумывается. ``query`` (keyword-only,
    обратно совместимо) фильтрует по подстроке в ``module_id`` (:func:`matches`,
    регистронезависимо, ё→е).
    """
    artifacts = _load(package_dir)
    assert artifacts.manifest is not None  # гарантировано _load
    modules = list(artifacts.manifest.modules)
    if query:
        modules = [module for module in modules if matches(module.module_id, query)]
    return modules


# ---------------------------------------------------------------------------
# Роли
# ---------------------------------------------------------------------------


def _build_role(
    role_id: str,
    definition: str | None,
    *,
    markers: list[str] | None,
    query_patterns: dict[str, list[str]] | None,
    core_coverage_allowed: bool,
    multi_instance: bool,
) -> RoleDef:
    try:
        return RoleDef(
            role_id=role_id,
            definition=definition,
            markers=list(markers or []),
            query_patterns=dict(query_patterns or {}),
            core_coverage_allowed=core_coverage_allowed,
            multi_instance=multi_instance,
        )
    except ValueError as exc:
        raise PackageAdminError(f"role: {exc}", kind="usage") from exc


def add_role(
    package_dir: str | Path,
    role_id: str,
    definition: str | None = None,
    *,
    markers: list[str] | None = None,
    query_patterns: dict[str, list[str]] | None = None,
    core_coverage_allowed: bool = False,
    multi_instance: bool = False,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Добавляет роль в реестр ``package.yaml`` (уникальный ``role_id``)."""
    _validate_ident(role_id, "role_id")
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    if any(role.role_id == role_id for role in artifacts.manifest.roles):
        raise PackageAdminError(f"роль уже существует: {role_id}", kind="usage")
    role = _build_role(
        role_id,
        definition,
        markers=markers,
        query_patterns=query_patterns,
        core_coverage_allowed=core_coverage_allowed,
        multi_instance=multi_instance,
    )
    artifacts.manifest = _rebuild_manifest(
        artifacts.manifest, roles=[*artifacts.manifest.roles, role]
    )
    return _finalize(
        package_dir, artifacts, before, ["package"], op="role-add", actor=actor, dry_run=dry_run
    )


def update_role(
    package_dir: str | Path,
    role_id: str,
    *,
    definition: str | None = None,
    markers: list[str] | None = None,
    query_patterns: dict[str, list[str]] | None = None,
    core_coverage_allowed: bool | None = None,
    multi_instance: bool | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Обновляет поля роли (``role_id`` иммутабелен)."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    target = next((role for role in artifacts.manifest.roles if role.role_id == role_id), None)
    if target is None:
        raise PackageAdminError(f"роль не найдена: {role_id}", kind="usage")
    updates: dict[str, Any] = {}
    if definition is not None:
        updates["definition"] = definition
    if markers is not None:
        updates["markers"] = list(markers)
    if query_patterns is not None:
        updates["query_patterns"] = dict(query_patterns)
    if core_coverage_allowed is not None:
        updates["core_coverage_allowed"] = core_coverage_allowed
    if multi_instance is not None:
        updates["multi_instance"] = multi_instance
    try:
        updated = RoleDef.model_validate({**target.model_dump(mode="python"), **updates})
    except ValueError as exc:
        raise PackageAdminError(f"role: {exc}", kind="usage") from exc
    roles = [updated if role.role_id == role_id else role for role in artifacts.manifest.roles]
    artifacts.manifest = _rebuild_manifest(artifacts.manifest, roles=roles)
    return _finalize(
        package_dir, artifacts, before, ["package"], op="role-update", actor=actor, dry_run=dry_run
    )


def remove_role(
    package_dir: str | Path,
    role_id: str,
    *,
    reassign_to: str | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Удаляет роль. Если на неё ссылается каталог — нужен ``reassign_to`` (§4.2)."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    if not any(role.role_id == role_id for role in artifacts.manifest.roles):
        raise PackageAdminError(f"роль не найдена: {role_id}", kind="usage")
    affected = sorted(
        row.param_id for row in artifacts.catalog if row.role_binding == role_id
    )
    remaining = [role for role in artifacts.manifest.roles if role.role_id != role_id]
    changed = ["package"]
    if affected and reassign_to is None:
        raise PackageAdminError(
            f"role-in-use: роль '{role_id}' используется параметрами: "
            f"{', '.join(affected)}; укажите --reassign",
            code="role-in-use",
            kind="blocked",
            blocking=[f"role-in-use: {role_id} <- {', '.join(affected)}"],
        )
    if reassign_to is not None:
        valid = {role.role_id for role in remaining}
        if reassign_to and reassign_to not in valid:
            raise PackageAdminError(
                f"роль для переназначения не найдена: {reassign_to}", kind="usage"
            )
        replacement = reassign_to or None
        artifacts.catalog = [
            row.model_copy(update={"role_binding": replacement})
            if row.role_binding == role_id
            else row
            for row in artifacts.catalog
        ]
        changed.append("catalog")
    artifacts.manifest = _rebuild_manifest(artifacts.manifest, roles=remaining)
    return _finalize(
        package_dir, artifacts, before, changed, op="role-remove", actor=actor, dry_run=dry_run
    )


def get_role(package_dir: str | Path, role_id: str) -> RoleDef | None:
    """Возвращает роль по ``role_id`` или ``None`` (read-only).

    Отсутствие пакета/``package.yaml`` или роли — ``None``; значения не выдумываются.
    """
    artifacts = load_artifacts(package_dir)
    if artifacts.manifest is None:
        return None
    return next((role for role in artifacts.manifest.roles if role.role_id == role_id), None)


def list_roles(package_dir: str | Path, *, query: str | None = None) -> list[RoleDef]:
    """Список ролей пакета (read-only).

    ``query`` фильтрует по подстроке в ``role_id``/``definition`` (:func:`matches`,
    регистронезависимо, ё→е).
    """
    artifacts = load_artifacts(package_dir)
    roles = list(artifacts.manifest.roles) if artifacts.manifest is not None else []
    if query:
        roles = [
            role
            for role in roles
            if matches(role.role_id, query) or matches(role.definition, query)
        ]
    return roles


# ---------------------------------------------------------------------------
# Каталог
# ---------------------------------------------------------------------------


def _coerce_terms(raw: Any) -> dict[str, list[str]]:
    """Приводит ``concept_terms`` к ``{lang: [terms]}`` (dict/list/None)."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(lang): [str(term) for term in terms] for lang, terms in raw.items()}
    return {}


def _validate_links(
    manifest: PackageManifest,
    *,
    module: str | None,
    role_binding: str | None,
) -> None:
    """Проверяет ссылки каталога на реестры манифеста (§4.2)."""
    if module is not None and module not in {item.module_id for item in manifest.modules}:
        raise PackageAdminError(f"module '{module}' отсутствует в package.yaml", kind="usage")
    if role_binding is not None and role_binding not in {item.role_id for item in manifest.roles}:
        raise PackageAdminError(
            f"role_binding '{role_binding}' отсутствует в package.yaml", kind="usage"
        )


def _unique_param_id(existing: set[str], base: str) -> str:
    """Подбирает уникальный ``param_id``: ``base``, ``base_2``, ``base_3`` …"""
    if base not in existing:
        return base
    suffix = 2
    while f"{base}_{suffix}" in existing:
        suffix += 1
    return f"{base}_{suffix}"


def _slug_from_title(title: str) -> str:
    """Slug ``param_id`` из заголовка: ``sanitize_slug(title)`` + fallback для не-латиницы.

    ``sanitize_slug`` (``core.util.security``) допускает только ``[a-zA-Z0-9_-]`` и на
    кириллическом заголовке возвращает сентинел ``"node"``. Чтобы кириллические названия
    не схлопывались в один slug, в этом случае берётся детерминированный
    ``param_<sha1[:8]>`` от исходного заголовка (тот же вход → тот же slug).
    """
    base = sanitize_slug(title.lower())
    if base == "node":
        digest = hashlib.sha1(title.encode("utf-8")).hexdigest()[:8]
        return f"param_{digest}"
    return base


def add_param(
    package_dir: str | Path,
    title: str,
    rule: str,
    *,
    module: str,
    group: str,
    importance: str,
    param_type: str = "fact",
    absence_rule: str = "explicit",
    role_binding: str | None = None,
    concept_terms: dict[str, list[str]] | None = None,
    search_langs: list[str] | None = None,
    param_id: str | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Добавляет параметр в ``catalog.csv`` (slug из title; коллизия → суффикс ``_2``)."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    _validate_links(artifacts.manifest, module=module, role_binding=role_binding)
    base = param_id or _slug_from_title(title)
    candidate = _unique_param_id({row.param_id for row in artifacts.catalog}, base)
    try:
        row = CatalogRow(
            param_id=candidate,
            title=title,
            module=module,
            group=group,
            importance=Importance(importance),
            param_type=ParamType(param_type),
            rule=rule,
            absence_rule=AbsenceRuleType(absence_rule),
            role_binding=role_binding,
            concept_terms=_coerce_terms(concept_terms),
            search_langs=list(search_langs or []),
        )
    except (ValueError, KeyError) as exc:
        raise PackageAdminError(f"param: {exc}", kind="usage") from exc
    artifacts.catalog = [*artifacts.catalog, row]
    result = _finalize(
        package_dir, artifacts, before, ["catalog"], op="param-add", actor=actor, dry_run=dry_run
    )
    result.created_id = candidate
    return result


def update_param(
    package_dir: str | Path,
    param_id: str,
    *,
    title: str | None = None,
    module: str | None = None,
    group: str | None = None,
    importance: str | None = None,
    param_type: str | None = None,
    rule: str | None = None,
    absence_rule: str | None = None,
    role_binding: str | None = None,
    concept_terms: dict[str, list[str]] | None = None,
    search_langs: list[str] | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Обновляет параметр; ``param_id`` иммутабелен, инварианты перепроверяются."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    assert artifacts.manifest is not None
    index = next(
        (i for i, row in enumerate(artifacts.catalog) if row.param_id == param_id), None
    )
    if index is None:
        raise PackageAdminError(f"параметр не найден: {param_id}", kind="usage")
    _validate_links(
        artifacts.manifest,
        module=module,
        role_binding=role_binding,
    )
    updates: dict[str, Any] = {}
    for key, value in (
        ("title", title),
        ("module", module),
        ("group", group),
        ("param_type", param_type),
        ("rule", rule),
        ("absence_rule", absence_rule),
        ("role_binding", role_binding),
        ("search_langs", search_langs),
    ):
        if value is not None:
            updates[key] = value
    if importance is not None:
        updates["importance"] = importance
    if concept_terms is not None:
        updates["concept_terms"] = _coerce_terms(concept_terms)
    try:
        updated = CatalogRow.model_validate(
            {**artifacts.catalog[index].model_dump(mode="python"), **updates}
        )
    except (ValueError, KeyError) as exc:
        raise PackageAdminError(f"param: {exc}", kind="usage") from exc
    artifacts.catalog[index] = updated
    return _finalize(
        package_dir, artifacts, before, ["catalog"], op="param-update", actor=actor, dry_run=dry_run
    )


def remove_param(
    package_dir: str | Path,
    param_id: str,
    *,
    replaced_by: str | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Soft-delete параметра: ``deprecated=True`` (+ ``replaced_by``); физического нет."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    index = next(
        (i for i, row in enumerate(artifacts.catalog) if row.param_id == param_id), None
    )
    if index is None:
        raise PackageAdminError(f"параметр не найден: {param_id}", kind="usage")
    artifacts.catalog[index] = artifacts.catalog[index].model_copy(
        update={"deprecated": True, "replaced_by": replaced_by}
    )
    return _finalize(
        package_dir, artifacts, before, ["catalog"], op="param-remove", actor=actor, dry_run=dry_run
    )


def get_param(package_dir: str | Path, param_id: str) -> CatalogRow | None:
    """Возвращает параметр по ``param_id`` или ``None`` (read-only)."""
    artifacts = load_artifacts(package_dir)
    return next((row for row in artifacts.catalog if row.param_id == param_id), None)


def _param_matches(row: CatalogRow, query: str) -> bool:
    """True, если ``query`` входит в ``param_id``/``title``/``rule``/``concept_terms``."""
    if matches(row.param_id, query) or matches(row.title, query) or matches(row.rule, query):
        return True
    return any(
        matches(term, query) for terms in row.concept_terms.values() for term in terms
    )


def list_params(
    package_dir: str | Path,
    *,
    include_deprecated: bool = True,
    query: str | None = None,
    module: str | None = None,
    group: str | None = None,
    importance: str | None = None,
    param_type: str | None = None,
    role_binding: str | None = None,
) -> list[CatalogRow]:
    """Список параметров (read-only) с необязательными поиском/фильтрами.

    Порядок сохраняется как в ``catalog.csv`` (детерминирован при записи).

    :param include_deprecated: ``False`` скрывает §13-отменённые параметры.
    :param query: подстрока в ``param_id``/``title``/``rule``/``concept_terms``.
    :param module: точное равенство ``module``.
    :param group: точное равенство ``group``.
    :param importance: значение :class:`~core.domain.enums.Importance` (например ``"Баз"``).
    :param param_type: значение :class:`~core.domain.enums.ParamType` (``fact``/``showcase``).
    :param role_binding: точное равенство ``role_binding``.
    """
    artifacts = load_artifacts(package_dir)
    rows = list(artifacts.catalog)
    if not include_deprecated:
        rows = [row for row in rows if not row.deprecated]
    if module is not None:
        rows = [row for row in rows if row.module == module]
    if group is not None:
        rows = [row for row in rows if row.group == group]
    if importance is not None:
        rows = [row for row in rows if row.importance.value == importance]
    if param_type is not None:
        rows = [row for row in rows if row.param_type.value == param_type]
    if role_binding is not None:
        rows = [row for row in rows if row.role_binding == role_binding]
    if query:
        rows = [row for row in rows if _param_matches(row, query)]
    return rows


# ---------------------------------------------------------------------------
# Baselines (baselines.csv, §4.2)
# ---------------------------------------------------------------------------


def _baseline_index(
    rows: list[BaselineRow], baseline_id: str, param_id: str
) -> int | None:
    """Индекс строки наследования по ключу ``(baseline_id, param_id)`` (или ``None``)."""
    return next(
        (
            i
            for i, row in enumerate(rows)
            if row.baseline_id == baseline_id and row.param_id == param_id
        ),
        None,
    )


def list_baselines(
    package_dir: str | Path,
    *,
    query: str | None = None,
    baseline_id: str | None = None,
) -> list[BaselineRow]:
    """Список строк наследования ``baselines.csv`` (read-only).

    :param query: подстрока в ``param_id``/``baseline_id``/``note`` (:func:`matches`).
    :param baseline_id: точное равенство ``baseline_id``.
    """
    artifacts = load_artifacts(package_dir)
    rows = list(artifacts.baselines)
    if baseline_id is not None:
        rows = [row for row in rows if row.baseline_id == baseline_id]
    if query:
        rows = [
            row
            for row in rows
            if matches(row.param_id, query)
            or matches(row.baseline_id, query)
            or matches(row.note, query)
        ]
    return rows


def find_baselines(
    package_dir: str | Path,
    *,
    baseline_id: str | None = None,
    param_id: str | None = None,
    query: str | None = None,
) -> list[BaselineRow]:
    """Ищет строки наследования по критериям (read-only) → список совпадений.

    Комбинация фильтров: ``baseline_id``/``param_id`` — точное равенство; ``query`` —
    подстрока в ``param_id``/``baseline_id``/``note`` (:func:`matches`). Пустой список —
    валидный результат (CLI решает ``not found``); для неоднозначного доступа CLI
    показывает список кандидатов, не «угадывая» первый.
    """
    rows = list_baselines(package_dir, query=query, baseline_id=baseline_id)
    if param_id is not None:
        rows = [row for row in rows if row.param_id == param_id]
    return rows


def get_baseline(
    package_dir: str | Path,
    *,
    baseline_id: str | None = None,
    param_id: str | None = None,
) -> BaselineRow | None:
    """Возвращает строку наследования по точному ключу ``(baseline_id, param_id)``.

    Если заданы **оба** ключа — точный доступ (``None``, если строки нет). Если задан
    только один из них — возвращается единственное совпадение; при **неоднозначности**
    (совпадений >1) или отсутствии возвращается ``None`` (никаких «догадок» — CLI для
    разрешения неоднозначности использует :func:`find_baselines`).
    """
    if baseline_id is not None and param_id is not None:
        artifacts = load_artifacts(package_dir)
        return next(
            (
                row
                for row in artifacts.baselines
                if row.baseline_id == baseline_id and row.param_id == param_id
            ),
            None,
        )
    candidates = find_baselines(package_dir, baseline_id=baseline_id, param_id=param_id)
    return candidates[0] if len(candidates) == 1 else None


def add_baseline(
    package_dir: str | Path,
    baseline_id: str,
    param_id: str,
    *,
    inheritance_mode: str = "direct",
    min_version: str | None = None,
    note: str | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Добавляет строку наследования (§4.2). Ключ строки ``(baseline_id, param_id)``.

    ``param_id`` обязан существовать в каталоге; ``inheritance_mode`` ∈ enum. На
    архетипе C непустой ``baselines`` блокируется линтером (§1.5) — операция откажет.
    """
    if not (baseline_id or "").strip():
        raise PackageAdminError("baseline_id не может быть пустым", kind="usage")
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    if not any(row.param_id == param_id for row in artifacts.catalog):
        raise PackageAdminError(
            f"param_id '{param_id}' отсутствует в каталоге — нечего наследовать", kind="usage"
        )
    if _baseline_index(artifacts.baselines, baseline_id, param_id) is not None:
        raise PackageAdminError(
            f"строка baseline уже существует: ({baseline_id}, {param_id})", kind="usage"
        )
    try:
        row = BaselineRow(
            baseline_id=baseline_id,
            param_id=param_id,
            min_version=min_version,
            inheritance_mode=InheritanceMode(inheritance_mode),
            note=note,
        )
    except ValueError as exc:
        raise PackageAdminError(f"baseline: {exc}", kind="usage") from exc
    artifacts.baselines = [*artifacts.baselines, row]
    result = _finalize(
        package_dir, artifacts, before, ["baselines"], op="baseline-add", actor=actor, dry_run=dry_run
    )
    result.created_id = f"{baseline_id}:{param_id}"
    return result


def update_baseline(
    package_dir: str | Path,
    baseline_id: str,
    param_id: str,
    *,
    inheritance_mode: str | None = None,
    min_version: str | None = None,
    note: str | None = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Обновляет строку наследования по ключу ``(baseline_id, param_id)``.

    Ключ иммутабелен (для смены ключа — ``remove`` + ``add``). ``None`` = не менять.
    """
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    index = _baseline_index(artifacts.baselines, baseline_id, param_id)
    if index is None:
        raise PackageAdminError(
            f"строка baseline не найдена: ({baseline_id}, {param_id})", kind="usage"
        )
    updates: dict[str, Any] = {}
    if inheritance_mode is not None:
        try:
            updates["inheritance_mode"] = InheritanceMode(inheritance_mode)
        except ValueError as exc:
            raise PackageAdminError(f"baseline: {exc}", kind="usage") from exc
    if min_version is not None:
        updates["min_version"] = min_version
    if note is not None:
        updates["note"] = note
    try:
        updated = BaselineRow.model_validate(
            {**artifacts.baselines[index].model_dump(mode="python"), **updates}
        )
    except ValueError as exc:
        raise PackageAdminError(f"baseline: {exc}", kind="usage") from exc
    artifacts.baselines[index] = updated
    return _finalize(
        package_dir, artifacts, before, ["baselines"], op="baseline-update", actor=actor, dry_run=dry_run
    )


def remove_baseline(
    package_dir: str | Path,
    baseline_id: str,
    param_id: str,
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Удаляет строку наследования по ключу ``(baseline_id, param_id)``."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    index = _baseline_index(artifacts.baselines, baseline_id, param_id)
    if index is None:
        raise PackageAdminError(
            f"строка baseline не найдена: ({baseline_id}, {param_id})", kind="usage"
        )
    artifacts.baselines = [
        row for i, row in enumerate(artifacts.baselines) if i != index
    ]
    return _finalize(
        package_dir, artifacts, before, ["baselines"], op="baseline-remove", actor=actor, dry_run=dry_run
    )


# ---------------------------------------------------------------------------
# Golden (golden.csv, §4.2/§11.6)
# ---------------------------------------------------------------------------


def list_golden(
    package_dir: str | Path,
    *,
    query: str | None = None,
    product_id: str | None = None,
    param_id: str | None = None,
) -> list[GoldenRow]:
    """Список golden-кейсов ``golden.csv`` (read-only) с поиском/фильтрами.

    :param query: подстрока в ``product_id``/``param_id``/``expected_status``/``case_class``
        (:func:`matches`, регистронезависимо, ё→е).
    :param product_id: точное равенство ``product_id``.
    :param param_id: точное равенство ``param_id``.
    """
    artifacts = load_artifacts(package_dir)
    rows = list(artifacts.golden)
    if product_id is not None:
        rows = [row for row in rows if row.product_id == product_id]
    if param_id is not None:
        rows = [row for row in rows if row.param_id == param_id]
    if query:
        rows = [
            row
            for row in rows
            if matches(row.product_id, query)
            or matches(row.param_id, query)
            or matches(row.expected_status.value, query)
            or matches(row.case_class, query)
        ]
    return rows


# ---------------------------------------------------------------------------
# Cohort (cohort.yaml, §1.6)
# ---------------------------------------------------------------------------


#: Поля cohort.yaml, которые можно менять операцией (``case_id`` иммутабелен).
_COHORT_MUTABLE_FIELDS = frozenset({"entries", "frozen_at"})


def update_cohort(
    package_dir: str | Path,
    fields: dict[str, Any],
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Меняет поля ``cohort.yaml`` (§1.6). ``case_id`` иммутабелен.

    Изменение состава (``entries``) — **версионная граница §13**: операция также
    синхронизирует проекцию ``run.yaml`` (``products``/``anchor``), чтобы S11 и «anchor ∈
    products» оставались выполнимыми, и добавляет warning о bump/``golden-full``.
    Валидация — повторный :class:`Cohort.model_validate` (ровно один якорь, уникальные id).
    """
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    if artifacts.cohort is None:
        raise PackageAdminError("cohort.yaml отсутствует в пакете", kind="usage")
    unknown = set(fields) - _COHORT_MUTABLE_FIELDS
    if unknown:
        raise PackageAdminError(
            f"неизменяемые/неизвестные поля cohort: {sorted(unknown)}", kind="usage"
        )
    data = artifacts.cohort.model_dump(mode="python")
    data.update(fields)
    try:
        cohort = Cohort.model_validate(data)
    except ValueError as exc:
        raise PackageAdminError(f"cohort: {exc}", kind="usage") from exc
    artifacts.cohort = cohort

    changed = ["cohort"]
    extra: list[str] = []
    if "entries" in fields:
        products = [entry.product_id for entry in cohort.entries]
        anchor = next(
            entry.product_id
            for entry in cohort.entries
            if entry.anchor_relation is AnchorRelation.ANCHOR
        )
        if artifacts.run is not None:
            artifacts.run = artifacts.run.model_copy(
                update={"products": products, "anchor": anchor}
            )
            changed.append("run")
        extra.append(
            "§13: смена состава когорты — версионная граница; выполните bump-version "
            "и пересоберите golden-full"
        )
    return _finalize(
        package_dir,
        artifacts,
        before,
        changed,
        op="cohort-update",
        actor=actor,
        dry_run=dry_run,
        extra_warnings=extra,
    )


def set_cohort(
    package_dir: str | Path,
    entries: list[Any],
    *,
    frozen_at: Any = None,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Заменяет состав когорты (``entries``) и, опционально, ``frozen_at`` (§1.6)."""
    fields: dict[str, Any] = {"entries": list(entries)}
    if frozen_at is not None:
        fields["frozen_at"] = frozen_at
    return update_cohort(package_dir, fields, actor=actor, dry_run=dry_run)


def get_cohort(package_dir: str | Path) -> Cohort | None:
    """Состав когорты (read-only) или ``None``, если ``cohort.yaml`` нет в пакете.

    Отсутствие пакета/когорты — не исключение (CLI печатает аккуратное ``not found``).
    Невалидный присутствующий ``cohort.yaml`` поднимает :class:`ValueError` (не маскируется
    под отсутствие — значения не выдумываются).
    """
    artifacts = load_artifacts(package_dir)
    return artifacts.cohort


def get_run_config(package_dir: str | Path) -> RunConfig | None:
    """Возвращает ``run.yaml`` как :class:`RunConfig` или ``None`` (read-only).

    Отсутствие пакета/``run.yaml`` — ``None`` (значения не выдумываются); невалидный
    присутствующий ``run.yaml`` поднимает :class:`ValueError`.
    """
    return load_artifacts(package_dir).run


# ---------------------------------------------------------------------------
# Scope attestation (scope_attestation.yaml, I2b)
# ---------------------------------------------------------------------------


def get_scope_attestation(package_dir: str | Path) -> str | None:
    """Читает процедуру аттестации полноты источника (read-only) или ``None`` (I2b).

    Отсутствие файла/пустое значение → ``None`` (CLI печатает ``not found``). Невалидный
    присутствующий ``scope_attestation.yaml`` поднимает :class:`ValueError`.
    """
    return load_scope_attestation(package_dir)


def set_scope_attestation(
    package_dir: str | Path,
    procedure: str,
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Настраивает процедуру аттестации полноты источника (I2b).

    Артефакт ``scope_attestation.yaml`` — строка процедуры (а не объект). Если
    ``parameter_reference``/``distribution_absence`` требуют ``exhaustive`` +
    ``scope_attested_by``, линтер отразит остаточные требования; установка процедуры
    снимает блок ``absence-attestation``.
    """
    if not (procedure or "").strip():
        raise PackageAdminError(
            "procedure не может быть пустой (для очистки — scope clear)", kind="usage"
        )
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    artifacts.scope_attestation = procedure.strip()
    return _finalize(
        package_dir,
        artifacts,
        before,
        ["scope_attestation"],
        op="scope-set",
        actor=actor,
        dry_run=dry_run,
    )


def clear_scope_attestation(
    package_dir: str | Path,
    *,
    actor: str | None = None,
    dry_run: bool = False,
) -> OpResult:
    """Удаляет ``scope_attestation.yaml`` (I2b). Прогон, опирающийся на эту процедуру,
    будет заблокирован линтером — операция откажет, если появятся новые blocking."""
    artifacts = _load(package_dir)
    before = prospective_lint(artifacts)
    artifacts.scope_attestation = None
    result = _finalize(
        package_dir,
        artifacts,
        before,
        [],
        op="scope-clear",
        actor=actor,
        dry_run=dry_run,
        deleted=["scope_attestation"],
    )
    return result

