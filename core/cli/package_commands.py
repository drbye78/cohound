"""coHound — CLI-команды управления пакетом категории (``hound package …``, итерация I1).

Парсер вложенных подкоманд и хендлеры для авторского CRUD над пакетом
(:mod:`core.pipeline.package_admin`) и базового разведочного инференса
(:mod:`core.pipeline.package_discovery`). Команды ничего не вычисляют сами: они
валидируют аргументы, вызывают операцию и печатают результат (человекочитаемо или
``--json``).

Коды выхода (из :mod:`core.cli.main`): ``EXIT_OK``, ``EXIT_USAGE`` (битый ввод/enum),
``EXIT_BLOCKED`` (новые/неустранимые блокирующие нарушения), ``EXIT_ERROR``.

Импорт :mod:`core.cli.main` здесь безопасен: ``main`` подключает этот модуль лениво
внутри :func:`core.cli.main.build_parser`, поэтому циклического импорта нет.
Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

from core.cli.main import (
    EXIT_BLOCKED,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    CliError,
    _print_json,
)
from core.pipeline.package_admin import (
    OpResult,
    PackageAdminError,
    add_baseline,
    add_module,
    add_param,
    add_role,
    bump_version,
    clear_scope_attestation,
    create_package,
    get_cohort,
    get_param,
    list_baselines,
    list_modules,
    list_params,
    list_roles,
    remove_baseline,
    remove_module,
    remove_param,
    remove_role,
    renormalize_modules,
    set_archetype,
    set_cohort,
    set_scope_attestation,
    show_package,
    update_baseline,
    update_cohort,
    update_module,
    update_param,
    update_role,
    validate_package,
)
from core.pipeline.package_candidates import (
    Candidate,
    apply_candidates,
    dumps_candidates,
    loads_candidates,
    plan_candidates,
)
from core.pipeline.package_discovery import (
    compose_baselines,
    infer_archetype,
    suggest_cohort_members,
    suggest_modules,
    suggest_roles,
)
from core.pipeline.package_io import atomic_write_text, scan_packages

__all__ = ["add_package_parser", "cmd_package"]


# ---------------------------------------------------------------------------
# Помощники вывода/ошибок
# ---------------------------------------------------------------------------


def _invoke(fn: Any) -> Any:
    """Вызывает операцию, маппит :class:`PackageAdminError` в :class:`CliError`."""
    try:
        return fn()
    except PackageAdminError as exc:
        exit_code = EXIT_BLOCKED if exc.kind == "blocked" else EXIT_USAGE
        raise CliError(str(exc), code=exc.code, exit_code=exit_code) from exc


def _emit(args: argparse.Namespace, payload: dict[str, Any], lines: list[str]) -> None:
    """Печатает ``payload`` как JSON при ``--json``, иначе построчно ``lines``."""
    if getattr(args, "json", False):
        _print_json(payload)
    else:
        sys.stdout.write("\n".join(lines) + "\n")


def _op_payload(result: OpResult) -> dict[str, Any]:
    """JSON-представление :class:`OpResult`."""
    return {
        "ok": not result.lint.get("blocking"),
        "dry_run": result.dry_run,
        "changed": list(result.changed),
        "written": [str(path) for path in result.written],
        "created_id": result.created_id,
        "lint": result.lint,
        "warnings": list(result.warnings),
    }


def _op_lines(result: OpResult) -> list[str]:
    """Человекочитаемое представление :class:`OpResult`."""
    lines = [
        f"dry-run: {'yes' if result.dry_run else 'no'}",
        f"changed: {', '.join(result.changed) or '—'}",
        f"written: {len(result.written)} файл(ов)",
    ]
    if result.created_id:
        lines.append(f"created_id: {result.created_id}")
    for message in result.lint.get("warnings", []):
        lines.append(f"warning: {message}")
    lines.append("ok: yes")
    return lines


def _load_profile(args: argparse.Namespace) -> dict[str, Any]:
    """Собирает словарь профиля: из ``--profile`` (JSON/YAML) или из флагов."""
    if getattr(args, "profile", None):
        path = Path(args.profile)
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if data is None:
            return {}
        if not isinstance(data, dict):
            raise CliError("профиль должен быть словарём", code="usage", exit_code=EXIT_USAGE)
        return data
    profile: dict[str, Any] = {}
    if getattr(args, "upstream_name", None):
        profile["upstream_name"] = args.upstream_name
    if getattr(args, "upstream_id", None):
        profile["upstream_id"] = args.upstream_id
    if getattr(args, "upstream_shared", False):
        profile["upstream_shared"] = True
    if getattr(args, "alternative", None):
        profile["alternatives"] = list(args.alternative)
    if getattr(args, "commercial_derivative", None):
        profile["commercial_derivatives"] = list(args.commercial_derivative)
    if getattr(args, "cross_line", False):
        profile["cross_line"] = True
    return profile


def _coerce_terms(entries: list[str] | None) -> dict[str, list[str]]:
    """Разбирает ``--concept-term lang=term`` в ``{lang: [terms]}``."""
    result: dict[str, list[str]] = {}
    for entry in entries or []:
        if "=" not in entry:
            raise CliError(
                f"concept-term ожидает формат lang=term, получено {entry!r}",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        lang, term = entry.split("=", 1)
        result.setdefault(lang.strip(), []).append(term.strip())
    return result


def _read_mapping(path: str | Path) -> dict[str, Any]:
    """Читает JSON/YAML-файл и требует словарь верхнего уровня."""
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise CliError(f"не удалось прочитать {path}: {exc}", code="usage", exit_code=EXIT_USAGE) from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise CliError(
            f"{path}: ожидался словарь верхнего уровня, получено {type(data).__name__}",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return data


def _read_list(path: str | Path) -> list[Any]:
    """Читает JSON/YAML-файл и требует список верхнего уровня."""
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise CliError(f"не удалось прочитать {path}: {exc}", code="usage", exit_code=EXIT_USAGE) from exc
    if not isinstance(data, list):
        raise CliError(
            f"{path}: ожидался список верхнего уровня, получено {type(data).__name__}",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return data


# ---------------------------------------------------------------------------
# Хендлеры: создание/чтение/валидация
# ---------------------------------------------------------------------------


def cmd_package_new(args: argparse.Namespace) -> int:
    """``hound package new`` — создать пакет (из шаблона)."""
    result = _invoke(
        lambda: create_package(
            args.case_id,
            args.archetype,
            dir=args.dir,
            from_template=not args.no_from_template,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    payload = _op_payload(result)
    payload["case_id"] = args.case_id
    payload["path"] = str(Path(args.dir) / args.case_id)
    _emit(args, payload, _op_lines(result))
    return EXIT_OK


def cmd_package_show(args: argparse.Namespace) -> int:
    """``hound package show`` — сводка по пакету (read-only)."""
    payload = _invoke(lambda: show_package(args.package))
    lines = [
        f"case_id: {payload['case_id']}",
        f"archetype: {payload['archetype']}",
        f"version: {payload['package_version']}",
        f"modules/roles: {payload['modules']}/{payload['roles']}",
        f"catalog: {payload['catalog']} (deprecated: {len(payload['deprecated_params'])})",
        f"baselines/golden/cohort: {payload['baselines']}/{payload['golden']}/{payload['cohort']}",
        f"ok: {'yes' if payload['ok'] else 'no'}",
    ]
    for message in payload["lint"]["blocking"]:
        lines.append(f"blocking: {message}")
    for message in payload["lint"]["warnings"]:
        lines.append(f"warning: {message}")
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_validate(args: argparse.Namespace) -> int:
    """``hound package validate`` — линт пакета без записи."""
    result = _invoke(lambda: validate_package(args.package))
    payload = _op_payload(result)
    lines = [f"ok: {'yes' if payload['ok'] else 'no'}"]
    for message in result.lint.get("blocking", []):
        lines.append(f"blocking: {message}")
    for message in result.lint.get("warnings", []):
        lines.append(f"warning: {message}")
    _emit(args, payload, lines)
    return EXIT_OK if payload["ok"] else EXIT_BLOCKED


def cmd_package_list(args: argparse.Namespace) -> int:
    """``hound package list`` — обзор всех пакетов каталога (read-only).

    ``--search`` фильтрует по ``case_id``/``archetype``/``path``/``package_version``;
    ``--include-template`` добавляет ``packages/_template``. Пустой результат — валидный
    :data:`EXIT_OK` с ``count: 0``. Ничего не пишет.
    """
    summaries = _invoke(
        lambda: scan_packages(
            args.dir, include_template=args.include_template, query=args.search
        )
    )
    payload = {
        "ok": True,
        "count": len(summaries),
        "packages": [summary.as_dict() for summary in summaries],
    }
    lines = [f"packages: {len(summaries)}"]
    for summary in summaries:
        status = "ok" if summary.ok else "FAIL"
        extra = f" error: {summary.error}" if summary.error else ""
        lines.append(
            f"  {summary.case_id}\t{summary.archetype}\t{summary.package_version}\t"
            f"mod={summary.modules} roles={summary.roles} cat={summary.catalog} "
            f"base={summary.baselines} gold={summary.golden} coh={summary.cohort} "
            f"{status}{extra}"
        )
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: архетип/версия
# ---------------------------------------------------------------------------


def cmd_package_set_archetype(args: argparse.Namespace) -> int:
    """``hound package set-archetype`` (C запрещён при непустых baselines)."""
    result = _invoke(
        lambda: set_archetype(
            args.package, args.archetype, actor=args.actor, dry_run=args.dry_run
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_bump_version(args: argparse.Namespace) -> int:
    """``hound package bump-version`` (semver; major → warning §13)."""
    result = _invoke(
        lambda: bump_version(
            args.package, level=args.level, actor=args.actor, dry_run=args.dry_run
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: модули
# ---------------------------------------------------------------------------


def cmd_package_module_add(args: argparse.Namespace) -> int:
    """``module add`` — добавить модуль (веса перенормируются к Σ=100)."""
    result = _invoke(
        lambda: add_module(
            args.package, args.module_id, args.weight, actor=args.actor, dry_run=args.dry_run
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_module_update(args: argparse.Namespace) -> int:
    """``module update`` — изменить вес модуля."""
    result = _invoke(
        lambda: update_module(
            args.package, args.module_id, args.weight, actor=args.actor, dry_run=args.dry_run
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_module_remove(args: argparse.Namespace) -> int:
    """``module remove`` — удалить модуль (нужен ``--reassign-to``, если он в каталоге)."""
    result = _invoke(
        lambda: remove_module(
            args.package,
            args.module_id,
            reassign_to=args.reassign_to,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_module_renormalize(args: argparse.Namespace) -> int:
    """``module renormalize`` — перенормировать веса к Σ=100."""
    result = _invoke(
        lambda: renormalize_modules(args.package, actor=args.actor, dry_run=args.dry_run)
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_module_list(args: argparse.Namespace) -> int:
    """``module list`` — модули пакета (id: weight) и Σ весов (read-only)."""
    modules = _invoke(lambda: list_modules(args.package))
    total = round(sum(module.weight for module in modules), 6)
    payload = {
        "ok": True,
        "count": len(modules),
        "weight_total": total,
        "modules": [module.model_dump(mode="json") for module in modules],
    }
    lines = [f"modules: {len(modules)} (Σ={total})"]
    for module in modules:
        lines.append(f"  {module.module_id}: {module.weight}")
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: роли
# ---------------------------------------------------------------------------


def cmd_package_role_add(args: argparse.Namespace) -> int:
    """``role add``."""
    result = _invoke(
        lambda: add_role(
            args.package,
            args.role_id,
            args.definition,
            markers=args.marker,
            core_coverage_allowed=args.core_coverage_allowed,
            multi_instance=args.multi_instance,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_role_update(args: argparse.Namespace) -> int:
    """``role update``."""
    result = _invoke(
        lambda: update_role(
            args.package,
            args.role_id,
            definition=args.definition,
            markers=args.marker,
            core_coverage_allowed=args.core_coverage_allowed,
            multi_instance=args.multi_instance,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_role_remove(args: argparse.Namespace) -> int:
    """``role remove`` (нужен ``--reassign``, если роль в каталоге)."""
    result = _invoke(
        lambda: remove_role(
            args.package,
            args.role_id,
            reassign_to=args.reassign,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_role_list(args: argparse.Namespace) -> int:
    """``role list`` — список ролей (read-only); ``--search`` по ``role_id``/``definition``."""
    roles = _invoke(lambda: list_roles(args.package, query=args.search))
    payload = {"ok": True, "count": len(roles), "roles": [role.model_dump(mode="json") for role in roles]}
    lines = [f"roles: {len(roles)}"]
    for role in roles:
        lines.append(f"  {role.role_id}: {role.definition}")
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: каталог
# ---------------------------------------------------------------------------


def cmd_package_param_add(args: argparse.Namespace) -> int:
    """``param add``."""
    result = _invoke(
        lambda: add_param(
            args.package,
            args.title,
            args.rule,
            module=args.module,
            group=args.group,
            importance=args.importance,
            param_type=args.param_type,
            absence_rule=args.absence_rule,
            role_binding=args.role_binding,
            concept_terms=_coerce_terms(args.concept_term),
            search_langs=args.lang,
            param_id=args.param_id,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_param_update(args: argparse.Namespace) -> int:
    """``param update`` (``param_id`` иммутабелен)."""
    importance = args.importance
    result = _invoke(
        lambda: update_param(
            args.package,
            args.param_id,
            title=args.title,
            module=args.module,
            group=args.group,
            importance=importance,
            param_type=args.param_type,
            rule=args.rule,
            absence_rule=args.absence_rule,
            role_binding=args.role_binding,
            concept_terms=_coerce_terms(args.concept_term) if args.concept_term else None,
            search_langs=args.lang,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_param_remove(args: argparse.Namespace) -> int:
    """``param remove`` — soft-delete (§13: ``deprecated=True``)."""
    result = _invoke(
        lambda: remove_param(
            args.package,
            args.param_id,
            replaced_by=args.replaced_by,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_param_get(args: argparse.Namespace) -> int:
    """``param get`` — один параметр (read-only)."""
    row = _invoke(lambda: get_param(args.package, args.param_id))
    if row is None:
        payload = {"ok": False, "found": False, "param_id": args.param_id}
        _emit(args, payload, [f"not found: {args.param_id}"])
        return EXIT_USAGE
    payload = {"ok": True, "found": True, "param": row.model_dump(mode="json")}
    lines = [f"param_id: {row.param_id}", f"title: {row.title}", f"module: {row.module}"]
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_param_list(args: argparse.Namespace) -> int:
    """``param list`` — список параметров (read-only) с поиском/фильтрами."""
    rows = _invoke(
        lambda: list_params(
            args.package,
            include_deprecated=not args.exclude_deprecated,
            query=args.search,
            module=args.module,
            group=args.group,
            importance=args.importance,
            param_type=args.param_type,
            role_binding=args.role,
        )
    )
    payload = {
        "ok": True,
        "count": len(rows),
        "params": [row.model_dump(mode="json") for row in rows],
    }
    lines = [f"params: {len(rows)}"]
    for row in rows:
        marker = " [deprecated]" if row.deprecated else ""
        lines.append(f"  {row.param_id}\t{row.module}\t{row.importance.value}{marker}")
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: discovery/inferring
# ---------------------------------------------------------------------------


def cmd_package_infer_archetype(args: argparse.Namespace) -> int:
    """``infer-archetype`` — эвристика архетипа (§1.5); ничего не пишет."""
    profile = _invoke(lambda: _load_profile(args))
    hypothesis = _invoke(lambda: infer_archetype(profile))
    payload = {"ok": True, **hypothesis.as_dict()}
    lines = [
        f"archetype: {payload['archetype']}",
        f"confidence: {payload['confidence']}",
        f"needs_human: {'yes' if payload['needs_human'] else 'no'}",
        f"rationale: {payload['rationale']}",
    ]
    for signal in payload["signals"]:
        lines.append(f"signal: {signal}")
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_compose_baselines(args: argparse.Namespace) -> int:
    """``compose-baselines`` — черновик наследования; ничего не пишет (A3)."""
    profile = _invoke(lambda: _load_profile(args))
    catalog = _invoke(lambda: list_params(args.package))
    proposal = _invoke(
        lambda: compose_baselines(
            profile, catalog, scope=args.scope, unreferenced=args.unreferenced
        )
    )
    payload = {"ok": True, **proposal.as_dict()}
    lines = [
        f"baseline_id: {payload['baseline_id'] or '—'}",
        f"baseline_participant: {payload['baseline_participant'] or '—'}",
        f"rows: {len(payload['rows'])}",
        f"needs_human: {'yes' if payload['needs_human'] else 'no'}",
    ]
    for note in payload["notes"]:
        lines.append(f"note: {note}")
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: baselines
# ---------------------------------------------------------------------------


def cmd_package_baseline_add(args: argparse.Namespace) -> int:
    """``baseline add`` — строка наследования (ключ baseline_id+param_id)."""
    result = _invoke(
        lambda: add_baseline(
            args.package,
            args.baseline_id,
            args.param_id,
            inheritance_mode=args.inheritance_mode,
            min_version=args.min_version,
            note=args.note,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_baseline_update(args: argparse.Namespace) -> int:
    """``baseline update``."""
    result = _invoke(
        lambda: update_baseline(
            args.package,
            args.baseline_id,
            args.param_id,
            inheritance_mode=args.inheritance_mode,
            min_version=args.min_version,
            note=args.note,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_baseline_remove(args: argparse.Namespace) -> int:
    """``baseline remove``."""
    result = _invoke(
        lambda: remove_baseline(
            args.package,
            args.baseline_id,
            args.param_id,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_baseline_list(args: argparse.Namespace) -> int:
    """``baseline list`` — строки наследования (read-only); ``--search``/``--baseline``."""
    rows = _invoke(
        lambda: list_baselines(args.package, query=args.search, baseline_id=args.baseline)
    )
    payload = {
        "ok": True,
        "count": len(rows),
        "baselines": [row.model_dump(mode="json") for row in rows],
    }
    lines = [f"baselines: {len(rows)}"]
    for row in rows:
        lines.append(f"  {row.baseline_id}\t{row.param_id}\t{row.inheritance_mode.value}")
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: cohort / scope
# ---------------------------------------------------------------------------


def cmd_package_cohort_update(args: argparse.Namespace) -> int:
    """``cohort update`` — слить поля в ``cohort.yaml`` (смена состава → warning §13)."""
    fields = _invoke(lambda: _read_mapping(args.fields))
    result = _invoke(
        lambda: update_cohort(args.package, fields, actor=args.actor, dry_run=args.dry_run)
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_cohort_set(args: argparse.Namespace) -> int:
    """``cohort set`` — заменить состав когорты (``--entries FILE``)."""
    entries = _invoke(lambda: _read_list(args.entries))
    result = _invoke(
        lambda: set_cohort(
            args.package,
            entries,
            frozen_at=args.frozen_at,
            actor=args.actor,
            dry_run=args.dry_run,
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_cohort_show(args: argparse.Namespace) -> int:
    """``cohort show`` — состав когорты (read-only): ``cohort_id``/``frozen_at``/``entries``.

    Отсутствие пакета/``cohort.yaml`` — аккуратное ``not found`` с :data:`EXIT_NOT_FOUND`
    (значения не выдумываются; невалидный файл → :data:`EXIT_USAGE` как битый ввод).
    """
    cohort = _invoke(lambda: get_cohort(args.package))
    if cohort is None:
        payload = {"ok": False, "found": False, "package": args.package}
        _emit(args, payload, [f"not found: cohort.yaml отсутствует в {args.package}"])
        return EXIT_NOT_FOUND
    payload = {"ok": True, "found": True, "cohort": cohort.model_dump(mode="json")}
    lines = [
        f"case_id: {cohort.case_id}",
        f"frozen_at: {cohort.frozen_at.isoformat()}",
        f"entries: {len(cohort.entries)}",
    ]
    for entry in cohort.entries:
        lines.append(
            f"  {entry.product_id}\t{entry.anchor_relation.value}\toverlap={entry.role_overlap}"
        )
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_scope_set(args: argparse.Namespace) -> int:
    """``scope set`` — настроить процедуру аттестации полноты источника (I2b)."""
    result = _invoke(
        lambda: set_scope_attestation(
            args.package, args.procedure, actor=args.actor, dry_run=args.dry_run
        )
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


def cmd_package_scope_clear(args: argparse.Namespace) -> int:
    """``scope clear`` — удалить ``scope_attestation.yaml`` (I2b)."""
    result = _invoke(
        lambda: clear_scope_attestation(args.package, actor=args.actor, dry_run=args.dry_run)
    )
    _emit(args, _op_payload(result), _op_lines(result))
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: suggest-* (read-only эвристики)
# ---------------------------------------------------------------------------


def _emit_candidates(args: argparse.Namespace, candidates: list[Candidate]) -> None:
    """Печатает кандидатов (JSON или человекочитаемо)."""
    payload = {"ok": True, "count": len(candidates), "candidates": [c.as_dict() for c in candidates]}
    lines = [f"candidates: {len(candidates)}"]
    for candidate in candidates:
        lines.append(f"  [{candidate.kind}] {candidate.rationale} ({candidate.source})")
    _emit(args, payload, lines)


def cmd_package_suggest_modules(args: argparse.Namespace) -> int:
    """``suggest modules`` — кандидаты модулей из профиля (без записи)."""
    profile = _invoke(lambda: _load_profile(args))
    _emit_candidates(args, _invoke(lambda: suggest_modules(profile)))
    return EXIT_OK


def cmd_package_suggest_roles(args: argparse.Namespace) -> int:
    """``suggest roles`` — кандидаты ролей из профиля (без записи)."""
    profile = _invoke(lambda: _load_profile(args))
    _emit_candidates(args, _invoke(lambda: suggest_roles(profile)))
    return EXIT_OK


def cmd_package_suggest_cohort(args: argparse.Namespace) -> int:
    """``suggest cohort`` — кандидаты участников когорты (без записи)."""
    profile = _invoke(lambda: _load_profile(args))
    _emit_candidates(args, _invoke(lambda: suggest_cohort_members(profile)))
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: candidates / apply
# ---------------------------------------------------------------------------


def cmd_package_candidates(args: argparse.Namespace) -> int:
    """``candidates`` — план кандидатов (не пишет в пакет; ``--write`` — во внешний файл)."""
    profile = _invoke(lambda: _load_profile(args))
    candidates = _invoke(
        lambda: plan_candidates(args.package, profile=profile, scope=args.scope)
    )
    if args.write:
        _invoke(lambda: atomic_write_text(args.write, dumps_candidates(candidates)))
    payload = {
        "ok": True,
        "count": len(candidates),
        "candidates": [c.as_dict() for c in candidates],
    }
    if args.write:
        payload["written"] = str(args.write)
    lines = [f"candidates: {len(candidates)}"]
    for candidate in candidates:
        lines.append(f"  [{candidate.kind}] {candidate.rationale} ({candidate.source})")
    if args.write:
        lines.append(f"written: {args.write}")
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_apply(args: argparse.Namespace) -> int:
    """``apply`` — применить кандидатов (обязателен ``--actor``; человеческий шлюз)."""
    if getattr(args, "from_file", None):
        text = _invoke(lambda: Path(args.from_file).read_text(encoding="utf-8"))
    elif getattr(args, "stdin", False):
        text = sys.stdin.read()
    else:
        raise CliError(
            "укажите источник кандидатов: --from FILE или --stdin",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    candidates = _invoke(lambda: loads_candidates(text))
    results = _invoke(
        lambda: apply_candidates(
            args.package, candidates, actor=args.actor, dry_run=args.dry_run
        )
    )
    payload = {
        "ok": True,
        "applied": len(results),
        "dry_run": bool(args.dry_run),
        "results": [_op_payload(result) for result in results],
    }
    lines = [f"applied: {len(results)}"]
    for result in results:
        lines.extend(_op_lines(result))
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Сборка парсера
# ---------------------------------------------------------------------------


def _add_op_common(parser: argparse.ArgumentParser, *, dry_run: bool = True) -> None:
    """Общие флаги мутирующих подкоманд: ``--package``, ``--dry-run``, ``--json``, ``--actor``."""
    parser.add_argument("--package", required=True, help="каталог пакета категории")
    if dry_run:
        parser.add_argument(
            "--dry-run", action="store_true", help="показать проспективный результат без записи"
        )
    parser.add_argument("--json", action="store_true", help="машинночитаемый вывод (JSON)")
    parser.add_argument("--actor", default=None, help="человек, выполняющий операцию (аудит)")


def _add_read_common(parser: argparse.ArgumentParser, *, package: bool = True) -> None:
    """Общие флаги read-only подкоманд."""
    if package:
        parser.add_argument("--package", required=True, help="каталог пакета категории")
    parser.add_argument("--json", action="store_true", help="машинночитаемый вывод (JSON)")


def _add_profile_flags(parser: argparse.ArgumentParser) -> None:
    """Флаги профиля для ``infer-archetype``/``compose-baselines``."""
    parser.add_argument(
        "--profile", default=None, help="JSON/YAML-файл профиля (альтернатива флагам ниже)"
    )
    parser.add_argument("--upstream-name", default=None, help="явный общий OSS-upstream/лидер")
    parser.add_argument("--upstream-id", default=None, help="product_id участника базовой линии")
    parser.add_argument("--upstream-shared", action="store_true", help="upstream общий для линейки")
    parser.add_argument(
        "--alternative", action="append", default=None, metavar="NAME", help="OSS-альтернатива (повторяемо)"
    )
    parser.add_argument(
        "--commercial-derivative",
        action="append",
        default=None,
        metavar="NAME",
        help="коммерческое производное (повторяемо)",
    )
    parser.add_argument("--cross-line", action="store_true", help="кросс-линейный набор продуктов")


def add_package_parser(subparsers: Any) -> None:
    """Регистрирует namespace ``hound package`` с вложенными подкомандами."""
    p_package = subparsers.add_parser(
        "package", help="управление пакетом категории (CRUD + discovery/inferring)"
    )
    pkg = p_package.add_subparsers(dest="package_command", metavar="ПОДКОМАНДА")

    # new ------------------------------------------------------------------ #
    p_new = pkg.add_parser("new", help="создать пакет (из шаблона packages/_template)")
    p_new.add_argument("--case-id", required=True, help="идентификатор пакета ([a-z0-9_]+)")
    p_new.add_argument("--archetype", required=True, choices=["A", "B", "C"], help="архетип §1.5")
    p_new.add_argument("--dir", default="packages", help="каталог пакетов (дефолт packages)")
    p_new.add_argument(
        "--no-from-template", action="store_true", help="не копировать шаблон, создать минимальный каркас"
    )
    p_new.add_argument("--dry-run", action="store_true", help="проверить без записи")
    p_new.add_argument("--json", action="store_true", help="машинночитаемый вывод (JSON)")
    p_new.add_argument("--actor", default=None, help="человек, выполняющий операцию (аудит)")
    p_new.set_defaults(handler=cmd_package_new)

    # show / validate ------------------------------------------------------ #
    p_show = pkg.add_parser("show", help="сводка по пакету (read-only)")
    _add_read_common(p_show)
    p_show.set_defaults(handler=cmd_package_show)

    p_validate = pkg.add_parser("validate", help="линт пакета без записи")
    _add_read_common(p_validate)
    p_validate.set_defaults(handler=cmd_package_validate)

    # list (обзор пакетов) ------------------------------------------------- #
    p_list = pkg.add_parser("list", help="обзор пакетов каталога (read-only)")
    p_list.add_argument("--dir", default="packages", help="каталог пакетов (дефолт packages)")
    p_list.add_argument("--search", default=None, help="фильтр по case_id/archetype/path/version")
    p_list.add_argument(
        "--include-template", action="store_true", help="включить packages/_template"
    )
    p_list.add_argument("--json", action="store_true", help="машинночитаемый вывод (JSON)")
    p_list.set_defaults(handler=cmd_package_list)

    # set-archetype / bump-version ---------------------------------------- #
    p_arch = pkg.add_parser("set-archetype", help="установить архетип (C запрещён при baselines)")
    _add_op_common(p_arch)
    p_arch.add_argument("--archetype", required=True, choices=["A", "B", "C"])
    p_arch.set_defaults(handler=cmd_package_set_archetype)

    p_bump = pkg.add_parser("bump-version", help="semver-инкремент package_version")
    _add_op_common(p_bump)
    p_bump.add_argument("--level", required=True, choices=["major", "minor", "patch"])
    p_bump.set_defaults(handler=cmd_package_bump_version)

    # module --------------------------------------------------------------- #
    p_module = pkg.add_parser("module", help="модули package.yaml")
    mod = p_module.add_subparsers(dest="module_command", metavar="ДЕЙСТВИЕ")

    p_mod_add = mod.add_parser("add", help="добавить модуль (веса → Σ=100)")
    _add_op_common(p_mod_add)
    p_mod_add.add_argument("--module-id", required=True)
    p_mod_add.add_argument("--weight", required=True, type=float)
    p_mod_add.set_defaults(handler=cmd_package_module_add)

    p_mod_upd = mod.add_parser("update", help="изменить вес модуля")
    _add_op_common(p_mod_upd)
    p_mod_upd.add_argument("--module-id", required=True)
    p_mod_upd.add_argument("--weight", required=True, type=float)
    p_mod_upd.set_defaults(handler=cmd_package_module_update)

    p_mod_rm = mod.add_parser("remove", help="удалить модуль")
    _add_op_common(p_mod_rm)
    p_mod_rm.add_argument("--module-id", required=True)
    p_mod_rm.add_argument("--reassign-to", default=None, help="модуль для параметров удаляемого")
    p_mod_rm.set_defaults(handler=cmd_package_module_remove)

    p_mod_ren = mod.add_parser("renormalize", help="перенормировать веса к Σ=100")
    _add_op_common(p_mod_ren)
    p_mod_ren.set_defaults(handler=cmd_package_module_renormalize)

    p_mod_ls = mod.add_parser("list", help="список модулей и Σ весов (read-only)")
    _add_read_common(p_mod_ls)
    p_mod_ls.set_defaults(handler=cmd_package_module_list)

    # role ----------------------------------------------------------------- #
    p_role = pkg.add_parser("role", help="роли package.yaml")
    role = p_role.add_subparsers(dest="role_command", metavar="ДЕЙСТВИЕ")

    p_role_add = role.add_parser("add", help="добавить роль")
    _add_op_common(p_role_add)
    p_role_add.add_argument("--role-id", required=True)
    p_role_add.add_argument("--definition", required=True)
    p_role_add.add_argument("--marker", action="append", default=None, help="маркер (повторяемо)")
    p_role_add.add_argument("--core-coverage-allowed", action="store_true")
    p_role_add.add_argument("--multi-instance", action="store_true")
    p_role_add.set_defaults(handler=cmd_package_role_add)

    p_role_upd = role.add_parser("update", help="обновить поля роли")
    _add_op_common(p_role_upd)
    p_role_upd.add_argument("--role-id", required=True)
    p_role_upd.add_argument("--definition", default=None)
    p_role_upd.add_argument("--marker", action="append", default=None)
    p_role_upd.add_argument(
        "--core-coverage-allowed", action="store_true", default=None
    )
    p_role_upd.add_argument("--multi-instance", action="store_true", default=None)
    p_role_upd.set_defaults(handler=cmd_package_role_update)

    p_role_rm = role.add_parser("remove", help="удалить роль")
    _add_op_common(p_role_rm)
    p_role_rm.add_argument("--role-id", required=True)
    p_role_rm.add_argument("--reassign", default=None, help="роль для переназначения параметров")
    p_role_rm.set_defaults(handler=cmd_package_role_remove)

    p_role_ls = role.add_parser("list", help="список ролей (read-only)")
    _add_read_common(p_role_ls)
    p_role_ls.add_argument("--search", default=None, help="поиск по role_id/definition")
    p_role_ls.set_defaults(handler=cmd_package_role_list)

    # param ---------------------------------------------------------------- #
    p_param = pkg.add_parser("param", help="каталог catalog.csv")
    param = p_param.add_subparsers(dest="param_command", metavar="ДЕЙСТВИЕ")

    p_param_add = param.add_parser("add", help="добавить параметр")
    _add_op_common(p_param_add)
    p_param_add.add_argument("--title", required=True)
    p_param_add.add_argument("--rule", required=True, help="точная формулировка границы зачёта")
    p_param_add.add_argument("--module", required=True)
    p_param_add.add_argument("--group", required=True)
    p_param_add.add_argument("--importance", required=True, choices=["Кр", "Вж", "Баз", "Опц"])
    p_param_add.add_argument("--param-type", default="fact", choices=["fact", "showcase"])
    p_param_add.add_argument(
        "--absence-rule",
        default="explicit",
        choices=["explicit", "parameter_reference", "distribution_absence", "registry_absence"],
    )
    p_param_add.add_argument("--role-binding", default=None)
    p_param_add.add_argument("--param-id", default=None, help="явный slug (иначе из title)")
    p_param_add.add_argument(
        "--concept-term", action="append", default=None, metavar="LANG=TERM", help="повторяемо"
    )
    p_param_add.add_argument("--lang", action="append", default=None, help="язык поиска (повторяемо)")
    p_param_add.set_defaults(handler=cmd_package_param_add)

    p_param_upd = param.add_parser("update", help="обновить параметр")
    _add_op_common(p_param_upd)
    p_param_upd.add_argument("--param-id", required=True)
    p_param_upd.add_argument("--title", default=None)
    p_param_upd.add_argument("--rule", default=None)
    p_param_upd.add_argument("--module", default=None)
    p_param_upd.add_argument("--group", default=None)
    p_param_upd.add_argument("--importance", default=None, choices=["Кр", "Вж", "Баз", "Опц"])
    p_param_upd.add_argument("--param-type", default=None, choices=["fact", "showcase"])
    p_param_upd.add_argument(
        "--absence-rule",
        default=None,
        choices=["explicit", "parameter_reference", "distribution_absence", "registry_absence"],
    )
    p_param_upd.add_argument("--role-binding", default=None)
    p_param_upd.add_argument("--concept-term", action="append", default=None, metavar="LANG=TERM")
    p_param_upd.add_argument("--lang", action="append", default=None)
    p_param_upd.set_defaults(handler=cmd_package_param_update)

    p_param_rm = param.add_parser("remove", help="soft-delete параметра (§13 deprecated)")
    _add_op_common(p_param_rm)
    p_param_rm.add_argument("--param-id", required=True)
    p_param_rm.add_argument("--replaced-by", default=None, help="slug заменяющего параметра")
    p_param_rm.set_defaults(handler=cmd_package_param_remove)

    p_param_get = param.add_parser("get", help="получить параметр (read-only)")
    _add_read_common(p_param_get)
    p_param_get.add_argument("--param-id", required=True)
    p_param_get.set_defaults(handler=cmd_package_param_get)

    p_param_ls = param.add_parser("list", help="список параметров (read-only)")
    _add_read_common(p_param_ls)
    p_param_ls.add_argument(
        "--search",
        default=None,
        help="поиск по param_id/title/rule/concept_terms",
    )
    p_param_ls.add_argument("--module", default=None, help="точный фильтр по модулю")
    p_param_ls.add_argument("--group", default=None, help="точный фильтр по группе")
    p_param_ls.add_argument(
        "--importance", default=None, choices=["Кр", "Вж", "Баз", "Опц"], help="точный фильтр важности"
    )
    p_param_ls.add_argument(
        "--param-type", default=None, choices=["fact", "showcase"], help="точный фильтр типа"
    )
    p_param_ls.add_argument("--role", default=None, help="точный фильтр по role_binding")
    p_param_ls.add_argument(
        "--exclude-deprecated", action="store_true", help="скрыть §13-отменённые параметры"
    )
    p_param_ls.set_defaults(handler=cmd_package_param_list)

    # infer-archetype / compose-baselines --------------------------------- #
    p_infer = pkg.add_parser("infer-archetype", help="эвристика архетипа (§1.5, без записи)")
    _add_profile_flags(p_infer)
    p_infer.add_argument("--json", action="store_true")
    p_infer.set_defaults(handler=cmd_package_infer_archetype)

    p_compose = pkg.add_parser("compose-baselines", help="черновик наследования (без записи)")
    p_compose.add_argument("--package", required=True, help="каталог пакета (источник каталога)")
    _add_profile_flags(p_compose)
    p_compose.add_argument("--scope", default="all", help="'all', модуль или param_id")
    p_compose.add_argument(
        "--unreferenced",
        default="skip",
        choices=["skip", "direct", "conditional"],
        help="политика параметров без референса (дефолт skip)",
    )
    p_compose.add_argument("--json", action="store_true")
    p_compose.set_defaults(handler=cmd_package_compose_baselines)

    # baseline ------------------------------------------------------------- #
    p_baseline = pkg.add_parser("baseline", help="строки наследования baselines.csv")
    baseline = p_baseline.add_subparsers(dest="baseline_command", metavar="ДЕЙСТВИЕ")

    p_bl_add = baseline.add_parser("add", help="добавить строку наследования")
    _add_op_common(p_bl_add)
    p_bl_add.add_argument("--baseline-id", required=True)
    p_bl_add.add_argument("--param-id", required=True)
    p_bl_add.add_argument(
        "--inheritance-mode", default="direct", choices=["direct", "conditional", "blocked"]
    )
    p_bl_add.add_argument("--min-version", default=None)
    p_bl_add.add_argument("--note", default=None)
    p_bl_add.set_defaults(handler=cmd_package_baseline_add)

    p_bl_upd = baseline.add_parser("update", help="обновить строку наследования")
    _add_op_common(p_bl_upd)
    p_bl_upd.add_argument("--baseline-id", required=True)
    p_bl_upd.add_argument("--param-id", required=True)
    p_bl_upd.add_argument(
        "--inheritance-mode", default=None, choices=["direct", "conditional", "blocked"]
    )
    p_bl_upd.add_argument("--min-version", default=None)
    p_bl_upd.add_argument("--note", default=None)
    p_bl_upd.set_defaults(handler=cmd_package_baseline_update)

    p_bl_rm = baseline.add_parser("remove", help="удалить строку наследования")
    _add_op_common(p_bl_rm)
    p_bl_rm.add_argument("--baseline-id", required=True)
    p_bl_rm.add_argument("--param-id", required=True)
    p_bl_rm.set_defaults(handler=cmd_package_baseline_remove)

    p_bl_ls = baseline.add_parser("list", help="список строк наследования (read-only)")
    _add_read_common(p_bl_ls)
    p_bl_ls.add_argument("--search", default=None, help="поиск по param_id/baseline_id/note")
    p_bl_ls.add_argument("--baseline", default=None, help="точный фильтр по baseline_id")
    p_bl_ls.set_defaults(handler=cmd_package_baseline_list)

    # cohort --------------------------------------------------------------- #
    p_cohort = pkg.add_parser("cohort", help="состав когорты cohort.yaml (§1.6)")
    cohort = p_cohort.add_subparsers(dest="cohort_command", metavar="ДЕЙСТВИЕ")

    p_coh_upd = cohort.add_parser("update", help="слить поля cohort.yaml (смена состава → §13)")
    _add_op_common(p_coh_upd)
    p_coh_upd.add_argument(
        "--fields", required=True, metavar="FILE", help="JSON/YAML-словарь полей (entries/frozen_at)"
    )
    p_coh_upd.set_defaults(handler=cmd_package_cohort_update)

    p_coh_set = cohort.add_parser("set", help="заменить состав когорты (--entries FILE)")
    _add_op_common(p_coh_set)
    p_coh_set.add_argument("--entries", required=True, metavar="FILE", help="JSON/YAML-список записей")
    p_coh_set.add_argument("--frozen-at", default=None, help="дата фиксации когорты (YYYY-MM-DD)")
    p_coh_set.set_defaults(handler=cmd_package_cohort_set)

    p_coh_show = cohort.add_parser("show", help="состав когорты (read-only)")
    _add_read_common(p_coh_show)
    p_coh_show.set_defaults(handler=cmd_package_cohort_show)

    # scope ---------------------------------------------------------------- #
    p_scope = pkg.add_parser("scope", help="процедура аттестации полноты источника (I2b)")
    scope = p_scope.add_subparsers(dest="scope_command", metavar="ДЕЙСТВИЕ")

    p_scope_set = scope.add_parser("set", help="настроить процедуру (scope_attestation.yaml)")
    _add_op_common(p_scope_set)
    p_scope_set.add_argument("--procedure", required=True, help="описание процедуры аттестации")
    p_scope_set.set_defaults(handler=cmd_package_scope_set)

    p_scope_clr = scope.add_parser("clear", help="удалить scope_attestation.yaml")
    _add_op_common(p_scope_clr)
    p_scope_clr.set_defaults(handler=cmd_package_scope_clear)

    # suggest -------------------------------------------------------------- #
    p_suggest = pkg.add_parser("suggest", help="эвристики-кандидаты из профиля (без записи)")
    suggest = p_suggest.add_subparsers(dest="suggest_command", metavar="ЧТО")

    p_sug_mod = suggest.add_parser("modules", help="кандидаты модулей")
    _add_profile_flags(p_sug_mod)
    p_sug_mod.add_argument("--json", action="store_true")
    p_sug_mod.set_defaults(handler=cmd_package_suggest_modules)

    p_sug_role = suggest.add_parser("roles", help="кандидаты ролей")
    _add_profile_flags(p_sug_role)
    p_sug_role.add_argument("--json", action="store_true")
    p_sug_role.set_defaults(handler=cmd_package_suggest_roles)

    p_sug_coh = suggest.add_parser("cohort", help="кандидаты участников когорты")
    _add_profile_flags(p_sug_coh)
    p_sug_coh.add_argument("--json", action="store_true")
    p_sug_coh.set_defaults(handler=cmd_package_suggest_cohort)

    # candidates / apply --------------------------------------------------- #
    p_cand = pkg.add_parser("candidates", help="план кандидатов (в пакет не пишет)")
    p_cand.add_argument("--package", required=True, help="каталог пакета категории")
    _add_profile_flags(p_cand)
    p_cand.add_argument("--scope", default="all", help="'all', модуль или param_id (для baselines)")
    p_cand.add_argument("--write", default=None, metavar="FILE", help="записать JSON-план в файл")
    p_cand.add_argument("--json", action="store_true")
    p_cand.set_defaults(handler=cmd_package_candidates)

    p_apply = pkg.add_parser("apply", help="применить кандидатов (обязателен --actor)")
    p_apply.add_argument("--package", required=True, help="каталог пакета категории")
    apply_source = p_apply.add_mutually_exclusive_group()
    apply_source.add_argument(
        "--from", dest="from_file", default=None, metavar="FILE", help="JSON-файл кандидатов"
    )
    apply_source.add_argument("--stdin", action="store_true", help="читать кандидатов из stdin")
    p_apply.add_argument(
        "--actor", required=True, help="человек, выполняющий применение (шлюз I6/I7; обязателен)"
    )
    p_apply.add_argument(
        "--dry-run", action="store_true", help="показать применение без записи"
    )
    p_apply.add_argument("--json", action="store_true", help="машинночитаемый вывод (JSON)")
    p_apply.set_defaults(handler=cmd_package_apply)

    p_package.set_defaults(handler=cmd_package, _package_parser=p_package)


def cmd_package(args: argparse.Namespace) -> int:
    """Диспетчер ``hound package`` без подкоманды: печатает использование."""
    parser = getattr(args, "_package_parser", None)
    if parser is not None:
        parser.print_help()
    else:  # pragma: no cover — парсер всегда доступен в реальном CLI
        sys.stderr.write("hound package: укажите подкоманду\n")
    return EXIT_USAGE
