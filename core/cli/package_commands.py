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
import datetime as _dt
import sys
from pathlib import Path
from typing import Any

import yaml

from core.cli.main import (
    EXIT_BLOCKED,
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    CliError,
    _concise,
    _emit_error,
    _llm_configured,
    _print_json,
    _search_adapter_configured,
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
    find_baselines,
    get_baseline,
    get_cohort,
    get_module,
    get_param,
    get_role,
    get_run_config,
    get_scope_attestation,
    list_baselines,
    list_golden,
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
from core.pipeline.package_io import (
    ARTIFACT_FILES,
    atomic_write_text,
    get_artifact,
    list_artifacts,
    load_artifacts,
    scan_packages,
)
from core.pipeline.package_scout import (
    PackageScoutReport,
    scout_package,
    write_scout_artifacts,
)

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


def _not_found(args: argparse.Namespace, key: str, value: Any) -> int:
    """Печатает ``not found`` (JSON/человекочитаемо) и возвращает :data:`EXIT_NOT_FOUND`."""
    payload = {"ok": False, "found": False, key: value}
    _emit(args, payload, [f"not found: {value}"])
    return EXIT_NOT_FOUND


def _ambiguous(
    args: argparse.Namespace, label: str, candidates: list[dict[str, Any]]
) -> int:
    """Печатает список кандидатов неоднозначного ``get`` → :data:`EXIT_USAGE`."""
    message = (
        f"{label}: неоднозначный запрос — {len(candidates)} совпадений; "
        "уточните --param-id/--baseline-id"
    )
    payload: dict[str, Any] = {
        "ok": False,
        "found": False,
        "ambiguous": True,
        "count": len(candidates),
        "message": message,
        "candidates": candidates,
    }
    lines = [message]
    for candidate in candidates:
        ident = (
            candidate.get("param_id")
            or candidate.get("baseline_id")
            or candidate.get("module_id")
            or candidate.get("role_id")
            or "?"
        )
        lines.append(f"  {ident}")
    _emit(args, payload, lines)
    return EXIT_USAGE


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


def cmd_package_module_get(args: argparse.Namespace) -> int:
    """``module get`` — один модуль по ``--module-id`` (read-only)."""
    module = _invoke(lambda: get_module(args.package, args.module_id))
    if module is None:
        return _not_found(args, "module_id", args.module_id)
    payload = {"ok": True, "found": True, "module": module.model_dump(mode="json")}
    lines = [f"module_id: {module.module_id}", f"weight: {module.weight}"]
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_module_list(args: argparse.Namespace) -> int:
    """``module list`` — модули пакета (id: weight) и Σ весов (read-only).

    ``--search`` фильтрует по подстроке в ``module_id``.
    """
    modules = _invoke(lambda: list_modules(args.package, query=args.search))
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


def cmd_package_role_get(args: argparse.Namespace) -> int:
    """``role get`` — одна роль по ``--role-id`` (read-only)."""
    role = _invoke(lambda: get_role(args.package, args.role_id))
    if role is None:
        return _not_found(args, "role_id", args.role_id)
    payload = {"ok": True, "found": True, "role": role.model_dump(mode="json")}
    lines = [f"role_id: {role.role_id}", f"definition: {role.definition}"]
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
    """``param get`` — один параметр (read-only) по точному ``--param-id`` или ``--search``.

    Точный ``--param-id``: найден → :data:`EXIT_OK`; нет → :data:`EXIT_NOT_FOUND`.
    ``--search`` (подстрока): 0 совпадений → :data:`EXIT_NOT_FOUND`; >1 → :data:`EXIT_USAGE`
    со списком кандидатов (первый не «угадывается»); ровно 1 → вывод.
    """
    param_id = getattr(args, "param_id", None)
    if param_id:
        row = _invoke(lambda: get_param(args.package, param_id))
        if row is None:
            return _not_found(args, "param_id", param_id)
    else:
        rows = _invoke(lambda: list_params(args.package, query=args.search))
        if not rows:
            return _not_found(args, "search", args.search)
        if len(rows) > 1:
            return _ambiguous(
                args, f"param get --search {args.search!r}", [r.model_dump(mode="json") for r in rows]
            )
        row = rows[0]
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


def cmd_package_baseline_get(args: argparse.Namespace) -> int:
    """``baseline get`` — строка наследования (read-only).

    Точный ключ ``(baseline_id, param_id)`` → вывод/``not found``; если задан лишь один
    из ключей либо ``--search`` — поиск по критерию; неоднозначность (>1) → :data:`EXIT_USAGE`
    со списком кандидатов; 0 → :data:`EXIT_NOT_FOUND`.
    """
    baseline_id = getattr(args, "baseline_id", None)
    param_id = getattr(args, "param_id", None)
    query = getattr(args, "search", None)
    exact = baseline_id is not None and param_id is not None and not query
    if exact:
        row = _invoke(
            lambda: get_baseline(args.package, baseline_id=baseline_id, param_id=param_id)
        )
        if row is None:
            return _not_found(args, "baseline", f"({baseline_id}, {param_id})")
    else:
        rows = _invoke(
            lambda: find_baselines(
                args.package, baseline_id=baseline_id, param_id=param_id, query=query
            )
        )
        if not rows:
            criterion = query or f"({baseline_id}, {param_id})"
            return _not_found(args, "baseline", criterion)
        if len(rows) > 1:
            criterion = query or f"({baseline_id}, {param_id})"
            return _ambiguous(
                args, f"baseline get {criterion!r}", [r.model_dump(mode="json") for r in rows]
            )
        row = rows[0]
    payload = {"ok": True, "found": True, "baseline": row.model_dump(mode="json")}
    lines = [
        f"baseline_id: {row.baseline_id}",
        f"param_id: {row.param_id}",
        f"inheritance_mode: {row.inheritance_mode.value}",
    ]
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


def cmd_package_scope_show(args: argparse.Namespace) -> int:
    """``scope show`` — процедура аттестации полноты источника (read-only, I2b).

    Отсутствие процедуры → ``not found`` с :data:`EXIT_NOT_FOUND` (значение не выдумывается).
    """
    procedure = _invoke(lambda: get_scope_attestation(args.package))
    if procedure is None:
        payload = {"ok": False, "found": False, "package": args.package}
        _emit(args, payload, [f"not found: scope_attestation.yaml отсутствует в {args.package}"])
        return EXIT_NOT_FOUND
    payload = {"ok": True, "found": True, "procedure": procedure}
    lines = [f"procedure: {procedure}"]
    _emit(args, payload, lines)
    return EXIT_OK


# ---------------------------------------------------------------------------
# Хендлеры: golden / run / artifact (read-only обзор артефактов)
# ---------------------------------------------------------------------------


def cmd_package_golden_list(args: argparse.Namespace) -> int:
    """``golden list`` — golden-кейсы (read-only); ``--search``/``--product``/``--param``."""
    rows = _invoke(
        lambda: list_golden(
            args.package, query=args.search, product_id=args.product, param_id=args.param
        )
    )
    payload = {
        "ok": True,
        "count": len(rows),
        "golden": [row.model_dump(mode="json") for row in rows],
    }
    lines = [f"golden: {len(rows)}"]
    for row in rows:
        lines.append(
            f"  {row.product_id}\t{row.param_id}\t{row.expected_status.value}\t{row.tier.value}"
        )
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_run_show(args: argparse.Namespace) -> int:
    """``run show`` — ``run.yaml`` как структура (read-only).

    Отсутствие ``run.yaml`` → ``not found`` с :data:`EXIT_NOT_FOUND`.
    """
    run = _invoke(lambda: get_run_config(args.package))
    if run is None:
        return _not_found(args, "run", args.package)
    payload = {"ok": True, "found": True, "run": run.model_dump(mode="json")}
    lines = [
        f"case_id: {run.case_id}",
        f"anchor: {run.anchor}",
        f"cohort: {run.cohort}",
        f"products: {', '.join(run.products)}",
        f"mode: {run.mode.value}",
        f"snapshot_date: {run.snapshot_date.isoformat()}",
        f"ranking_policy: {run.ranking_policy.value}",
    ]
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_artifact_list(args: argparse.Namespace) -> int:
    """``artifact list`` — обзор всех 8 артефактов пакета (read-only)."""
    items = _invoke(lambda: list_artifacts(args.package))
    payload = {"ok": True, "count": len(items), "artifacts": items}
    lines = [f"artifacts: {len(items)}"]
    for item in items:
        marker = "present" if item["present"] else "absent"
        count = item["count"] if item["count"] is not None else "—"
        lines.append(
            f"  {item['name']}\t{marker}\tsize={item['size']}\tcount={count}\t{item['file']}"
        )
    _emit(args, payload, lines)
    return EXIT_OK


def cmd_package_artifact_get(args: argparse.Namespace) -> int:
    """``artifact get`` — один артефакт по ``--name`` (read-only); ``--raw`` — сырой текст.

    Отсутствие файла → ``not found`` с :data:`EXIT_NOT_FOUND`; неизвестное ``--name``
    отсекается ``argparse`` (choices→:data:`EXIT_USAGE`).
    """
    name = args.name
    raw = bool(getattr(args, "raw", False))
    if raw and getattr(args, "json", False):
        raise CliError(
            "--raw и --json несовместимы: сырой текст не является JSON-документом",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    data, path = _invoke(lambda: get_artifact(args.package, name, raw=raw))
    if data is None:
        return _not_found(args, "artifact", name)
    if raw:
        sys.stdout.write(data)
        if not data.endswith("\n"):
            sys.stdout.write("\n")
        return EXIT_OK
    payload = {"ok": True, "found": True, "name": name, "path": str(path), "data": data}
    lines = [f"artifact: {name}", f"path: {path}"]
    _emit(args, payload, lines)
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
# Хендлеры: веб-заземлённая разведка пакета (scout)
# ---------------------------------------------------------------------------


def _parse_only(raw: str | None) -> list[str] | None:
    """Разбирает ``--only`` (через запятую) в список из {landscape,catalog,baselines}."""
    if raw is None or not str(raw).strip():
        return None
    parts = [part.strip() for part in str(raw).split(",") if part.strip()]
    valid = {"landscape", "catalog", "baselines"}
    unknown = set(parts) - valid
    if unknown:
        raise CliError(
            f"--only: неизвестные значения {sorted(unknown)}; допустимы {sorted(valid)}",
            code="usage",
            exit_code=EXIT_USAGE,
        )
    return parts


def cmd_package_scout(args: argparse.Namespace) -> int:
    """``package scout`` — веб-заземлённая разведка категории: SERP+краул+tool-less LLM.

    Pipeline сам делает внешний поиск (:class:`SearchAdapterPort`), реальный краул
    (:class:`CrawlerPort`) и извлечение (:class:`ExtractorPort`), затем **tool-less** LLM
    (:class:`DiscoveryLLMPort`, I18) синтезирует гипотезы → консервативный маппинг →
    кандидаты. Все кандидаты ``needs_human=True`` — auto-approve нет (A3/G2). Scout
    **не пишет в пакет** (I19): только ``sources/scout/{date}.candidates.json`` +
    ``.provenance.json``; применение — человеческим ``hound package apply --actor``.

    Ошибки разведки (``report.ok=False``) не отменяют запись: частичные кандидаты
    сохраняются с пометкой в sidecar/``needs_human`` (OQ-3). Без LLM-конфигурации или
    учётных данных поиска — понятная ошибка, кандидаты не выдумываются.

    **Выбор провайдера (I16/I21/I23).** Провайдер резолвится детерминированно
    :func:`core.config.deploy.resolve_search_provider`: явный ``--search-adapter`` >
    ``search.by_language[lang]`` из ``config/deploy.yaml`` > ``search.adapter`` >
    ``yandex``. Маршрут зависит только от языка (не от продукта) — симметричный протокол
    I16 сохраняется; решение вычисляется по правилу, а не LLM (дух I21). Выбранный
    провайдер попадает в payload и sidecar-провенанс (``search_provider``/
    ``search_adapter_kind``, I23). Неизвестный провайдер из конфига честно отвергается
    (:data:`EXIT_USAGE`, ``search``), а не подменяется — кандидаты не выдумываются (I19).
    Тестовый шов ``search_adapter`` (аргумент :func:`core.cli.main.main`) имеет высший
    приоритет и пропускает резолв/валидацию провайдера.

    :returns: :data:`EXIT_OK` — разведка без ошибок; :data:`EXIT_ERROR` — ошибки
        сбора/инференса; :data:`EXIT_USAGE` — битый ``--package``/LLM не сконфигурирован/
        неизвестный провайдер поиска.
    """
    from core.cli.wiring import (
        LOCAL_PROFILE,
        SUPPORTED_SEARCH_ADAPTERS,
        WiringError,
        build_adapters,
        build_discovery_llm,
        build_scout_crawler,
        build_search_adapter,
    )

    pkg_dir = Path(args.package)
    if not (pkg_dir / "package.yaml").is_file():
        raise CliError(
            f"{args.package}: package.yaml отсутствует — это не пакет категории",
            code="package",
            exit_code=EXIT_USAGE,
        )
    artifacts = _invoke(lambda: load_artifacts(pkg_dir))
    manifest = artifacts.manifest
    case_id = manifest.case_id if manifest is not None else pkg_dir.name

    # 1. LLM-разведка (tool-less, I18): инъекция теста ИЛИ сборка из env/флагов.
    injected_llm = getattr(args, "discovery_llm", None)
    if injected_llm is not None:
        discovery_llm = injected_llm
    else:
        try:
            discovery_llm = build_discovery_llm(
                base_url=args.llm_base_url,
                model_id=args.llm_model,
                decoder=args.llm_decoder,
                max_tokens=args.llm_max_tokens,
            )
        except WiringError as exc:
            _emit_error(args, "llm", _concise(exc))
            return EXIT_USAGE
    if not _llm_configured(discovery_llm):
        _emit_error(
            args,
            "llm",
            "LLM-разведка не сконфигурирована: задайте --llm-base-url/--llm-model "
            "или COHOUND_LLM_BASE_URL/COHOUND_LLM_MODEL; кандидаты не выдумываются",
        )
        return EXIT_USAGE

    # 2. Поиск: инъекция теста, --no-search либо резолв провайдера из deploy-конфига.
    #    Провайдер выбирается детерминированно (I16: функция от lang, не от продукта;
    #    дух I21: по правилу, а не LLM). Выбранный провайдер фиксируется в провенансе (I23).
    injected_search = getattr(args, "search_adapter", None)
    resolved_provider: str | None = None
    search: Any
    if injected_search is not None:
        # Тестовый шов: резолв/валидация провайдера пропускаются (провайдер зафиксирован
        # вне конфига — ответственность теста).
        search = injected_search
    elif args.no_search:
        search = None
    else:
        from core.config.deploy import load_deploy_config, resolve_search_provider

        config = load_deploy_config(None)
        resolved_provider = resolve_search_provider(
            config.search, lang=args.lang, explicit=args.search_adapter_kind
        )
        # Валидация против реестра адаптеров — в composition root (config-слой
        # провайдеро-нейтрален). Неизвестный провайдер → понятная ошибка (I19: не выдумываем).
        if resolved_provider not in SUPPORTED_SEARCH_ADAPTERS:
            _emit_error(
                args,
                "search",
                f"неизвестный провайдер поиска {resolved_provider!r} "
                "(config/deploy.yaml search.adapter/by_language либо --search-adapter); "
                f"поддерживаются: {', '.join(SUPPORTED_SEARCH_ADAPTERS)}",
            )
            return EXIT_USAGE
        try:
            search = build_search_adapter(resolved_provider)
        except WiringError as exc:
            _emit_error(args, "I19", _concise(exc))
            return EXIT_USAGE
        if not _search_adapter_configured(search):
            _emit_error(
                args,
                "I19",
                "поисковый адаптер недоступен: нет httpx либо учётных данных "
                "(yandex: YANDEX_SEARCH_API_KEY/YANDEX_FOLDER_ID; glm: GLM_SEARCH_API_KEY); "
                "кандидаты не выдумываются (используйте --no-search с --url)",
            )
            return EXIT_ERROR

    # Имя выбранного провайдера для провенанса (I23): резолв из конфига либо атрибут шва.
    provider_label: str | None = resolved_provider
    if provider_label is None and search is not None:
        provider_label = getattr(search, "provider", "") or getattr(search, "adapter_kind", "")

    # 3. Краул/экстракция: как в ``cmd_passport_scout`` (инъекции/локальный профиль).
    crawler = getattr(args, "crawler", None) or build_scout_crawler()
    extractor: Any = getattr(args, "extractor", None)
    if extractor is None:
        ports = build_adapters(profile=LOCAL_PROFILE, package=str(pkg_dir))
        extractor = ports.extractor

    # 4. Подсказки: пользовательские + резервный контекст целевого пакета.
    user_hints = _invoke(lambda: _read_mapping(args.hints)) if args.hints else {}
    reserved = {
        "existing_modules": [m.module_id for m in (manifest.modules if manifest else [])],
        "existing_roles": [r.role_id for r in (manifest.roles if manifest else [])],
        "existing_params": [row.param_id for row in artifacts.catalog],
        "existing_titles": [row.title for row in artifacts.catalog],
    }
    hints = {**user_hints, **reserved}

    # 5. Разведка (кандидаты; в пакет не пишет — I19).
    report: PackageScoutReport = _invoke(
        lambda: scout_package(
            subject_name=args.subject_name,
            category=args.category,
            seed_urls=list(args.seed_urls or []),
            hints=hints,
            search=search,
            crawler=crawler,
            extractor=extractor,
            discovery_llm=discovery_llm,
            only=_parse_only(args.only),
            scope=args.scope,
            lang=args.lang,
            limit=args.limit,
            max_pages=args.max_pages,
            budget=args.max_calls,
        )
    )

    # Провайдер поиска в sidecar-провенанс (I23): имя провайдера и вид адаптера.
    # Мутируем только копию-словарь отчёта; глубоко pipeline не трогаем.
    if provider_label:
        report.sidecar.setdefault("search_provider", provider_label)
    adapter_kind = getattr(search, "adapter_kind", "") if search is not None else ""
    if adapter_kind:
        report.sidecar.setdefault("search_adapter_kind", adapter_kind)

    # 6. Запись артефактов (OQ-3: частичные кандидаты тоже пишутся; --dry-run не пишет).
    out_dir = Path(args.out) if args.out else pkg_dir / "sources" / "scout"
    written: dict[str, str] = {}
    if not args.dry_run:
        candidates_path, provenance_path = _invoke(
            lambda: write_scout_artifacts(
                report, out_dir, report.sidecar.get("snapshot_date") or _today()
            )
        )
        written = {
            "candidates_path": str(candidates_path),
            "provenance_path": str(provenance_path),
        }

    payload: dict[str, Any] = {
        "ok": report.ok,
        "subject_name": report.subject_name,
        "case_id": case_id,
        "archetype": report.archetype.as_dict(),
        "candidates": len(report.candidates),
        "candidate_kinds": dict(report.sidecar.get("candidate_kinds", {})),
        "needs_human": report.needs_human,
        "errors": report.errors,
        "fetched": len(report.fetched),
        "search_calls": report.search_calls,
        "search_provider": provider_label,
        "search_adapter_kind": adapter_kind or None,
        "dry_run": bool(args.dry_run),
        **written,
    }
    _emit(args, payload, _print_package_scout_lines(payload))
    return EXIT_OK if report.ok else EXIT_ERROR


def _today() -> str:
    """Сегодняшняя дата ISO (fallback даты снимка артефактов scout)."""
    return _dt.date.today().isoformat()


def _print_package_scout_lines(payload: dict[str, Any]) -> list[str]:
    """Строит строки человекочитаемого вывода ``package scout`` (для :func:`_emit`)."""
    lines = [
        f"subject_name: {payload['subject_name']}",
        f"case_id: {payload['case_id']}",
        f"archetype: {payload['archetype']['archetype']}",
        f"candidates: {payload['candidates']}",
        f"search_calls: {payload['search_calls']}",
        f"fetched: {payload['fetched']}",
        f"dry-run: {'yes' if payload.get('dry_run') else 'no'}",
    ]
    if payload.get("search_provider"):
        detail = payload.get("search_adapter_kind")
        suffix = f" ({detail})" if detail else ""
        lines.append(f"search_provider: {payload['search_provider']}{suffix}")
    for label in ("candidates_path", "provenance_path"):
        if label in payload:
            lines.append(f"{label}: {payload[label]}")
    for kind, count in sorted(payload.get("candidate_kinds", {}).items()):
        lines.append(f"  [{kind}] {count}")
    for item in payload["needs_human"]:
        lines.append(f"needs_human: {item}")
    for error in payload["errors"]:
        lines.append(f"error: {error}")
    lines.append(f"ok: {'yes' if payload['ok'] else 'no'}")
    return lines


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
    p_mod_ls.add_argument("--search", default=None, help="поиск по module_id")
    p_mod_ls.set_defaults(handler=cmd_package_module_list)

    p_mod_get = mod.add_parser("get", help="получить модуль по id (read-only)")
    _add_read_common(p_mod_get)
    p_mod_get.add_argument("--module-id", required=True)
    p_mod_get.set_defaults(handler=cmd_package_module_get)

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

    p_role_get = role.add_parser("get", help="получить роль по id (read-only)")
    _add_read_common(p_role_get)
    p_role_get.add_argument("--role-id", required=True)
    p_role_get.set_defaults(handler=cmd_package_role_get)

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
    get_source = p_param_get.add_mutually_exclusive_group(required=True)
    get_source.add_argument("--param-id", default=None, help="точный slug параметра")
    get_source.add_argument("--search", default=None, help="подстрока (неоднозначность → EXIT_USAGE)")
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

    p_bl_get = baseline.add_parser("get", help="получить строку наследования (read-only)")
    _add_read_common(p_bl_get)
    p_bl_get.add_argument("--baseline-id", default=None, help="точный baseline_id (ключ)")
    p_bl_get.add_argument("--param-id", default=None, help="точный param_id (ключ)")
    p_bl_get.add_argument("--search", default=None, help="подстрока (неоднозначность → EXIT_USAGE)")
    p_bl_get.set_defaults(handler=cmd_package_baseline_get)

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

    p_scope_show = scope.add_parser("show", help="показать процедуру аттестации (read-only)")
    _add_read_common(p_scope_show)
    p_scope_show.set_defaults(handler=cmd_package_scope_show)

    # golden --------------------------------------------------------------- #
    p_golden = pkg.add_parser("golden", help="golden-кейсы golden.csv (§4.2/§11.6)")
    golden = p_golden.add_subparsers(dest="golden_command", metavar="ДЕЙСТВИЕ")

    p_golden_ls = golden.add_parser("list", help="список golden-кейсов (read-only)")
    _add_read_common(p_golden_ls)
    p_golden_ls.add_argument(
        "--search", default=None, help="поиск по product_id/param_id/expected_status/case_class"
    )
    p_golden_ls.add_argument("--product", default=None, help="точный фильтр по product_id")
    p_golden_ls.add_argument("--param", default=None, help="точный фильтр по param_id")
    p_golden_ls.set_defaults(handler=cmd_package_golden_list)

    # run ------------------------------------------------------------------ #
    p_run = pkg.add_parser("run", help="run.yaml как структура (read-only)")
    run_cmd = p_run.add_subparsers(dest="run_command", metavar="ДЕЙСТВИЕ")

    p_run_show = run_cmd.add_parser("show", help="показать run.yaml (read-only)")
    _add_read_common(p_run_show)
    p_run_show.set_defaults(handler=cmd_package_run_show)

    # artifact ------------------------------------------------------------- #
    p_artifact = pkg.add_parser("artifact", help="обобщённый доступ к артефактам пакета (read-only)")
    artifact = p_artifact.add_subparsers(dest="artifact_command", metavar="ДЕЙСТВИЕ")

    p_artifact_ls = artifact.add_parser("list", help="обзор всех артефактов пакета (read-only)")
    _add_read_common(p_artifact_ls)
    p_artifact_ls.set_defaults(handler=cmd_package_artifact_list)

    p_artifact_get = artifact.add_parser("get", help="прочитать один артефакт (read-only)")
    _add_read_common(p_artifact_get)
    p_artifact_get.add_argument(
        "--name", required=True, choices=sorted(ARTIFACT_FILES), help="имя артефакта"
    )
    p_artifact_get.add_argument(
        "--raw", action="store_true", help="сырой текст файла (несовместимо с --json)"
    )
    p_artifact_get.set_defaults(handler=cmd_package_artifact_get)

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

    # scout (веб-заземлённая разведка) ------------------------------------- #
    from core.cli.wiring import SUPPORTED_SEARCH_ADAPTERS

    p_scout = pkg.add_parser(
        "scout",
        help="веб-заземлённая разведка пакета: SERP+краул+tool-less LLM → кандидаты",
    )
    _add_op_common(p_scout)
    p_scout.add_argument("--subject-name", required=True, help="тема разведки (категория/продукт)")
    p_scout.add_argument("--category", default=None, help="уточнение категории (для запросов/промпта)")
    p_scout.add_argument("--hints", default=None, metavar="FILE", help="JSON/YAML-файл подсказок")
    p_scout.add_argument(
        "--url",
        action="append",
        dest="seed_urls",
        default=None,
        metavar="URL",
        help="доверенная точка входа (повторяемо; фетчится всегда)",
    )
    p_scout.add_argument(
        "--search-adapter",
        dest="search_adapter_kind",
        default=None,
        choices=list(SUPPORTED_SEARCH_ADAPTERS),
        help="провайдер поиска; по умолчанию — из config/deploy.yaml "
        "(search.adapter/by_language по --lang) или yandex",
    )
    p_scout.add_argument("--no-search", action="store_true", help="не делать SERP (только --url)")
    p_scout.add_argument("--limit", type=int, default=10, help="лимит результатов SERP на запрос")
    p_scout.add_argument("--max-pages", type=int, default=8, help="потолок числа фетчей")
    p_scout.add_argument(
        "--max-calls", type=int, default=None, help="потолок числа SERP-вызовов (бюджет)"
    )
    p_scout.add_argument(
        "--only",
        default="landscape,catalog",
        help="какие LLM-вызовы делать: landscape,catalog,baselines (через запятую)",
    )
    p_scout.add_argument("--scope", default="all", help="область baselines: 'all', модуль или param_id")
    p_scout.add_argument("--lang", choices=["ru", "en"], default="ru", help="язык запросов")
    p_scout.add_argument("--llm-base-url", default=None, help="override COHOUND_LLM_BASE_URL")
    p_scout.add_argument("--llm-model", default=None, help="override COHOUND_LLM_MODEL")
    p_scout.add_argument(
        "--llm-decoder",
        choices=["none", "json_object", "json_schema"],
        default=None,
        help="механизм декодирования (override COHOUND_LLM_DECODER)",
    )
    p_scout.add_argument(
        "--llm-max-tokens", type=int, default=None, help="потолок генерации LLM"
    )
    p_scout.add_argument(
        "--out",
        default=None,
        help="каталог артефактов scout (дефолт packages/{case_id}/sources/scout)",
    )
    p_scout.set_defaults(handler=cmd_package_scout)

    p_package.set_defaults(handler=cmd_package, _package_parser=p_package)


def cmd_package(args: argparse.Namespace) -> int:
    """Диспетчер ``hound package`` без подкоманды: печатает использование."""
    parser = getattr(args, "_package_parser", None)
    if parser is not None:
        parser.print_help()
    else:  # pragma: no cover — парсер всегда доступен в реальном CLI
        sys.stderr.write("hound package: укажите подкоманду\n")
    return EXIT_USAGE
