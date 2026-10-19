"""coHound — парсер подкоманд ``hound package`` (``03`` §E.1).

Регистрирует namespace ``hound package`` с вложенными подкомандами и связывает их с
хендлерами из :mod:`core.cli.package.commands`. Вынесено из ``package_commands.py``
(P1-A). Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
from typing import Any

from core.pipeline.package_io import ARTIFACT_FILES

from core.cli.package.commands import (
    cmd_package_new,
    cmd_package_show,
    cmd_package_validate,
    cmd_package_list,
    cmd_package_set_archetype,
    cmd_package_bump_version,
    cmd_package_module_add,
    cmd_package_module_update,
    cmd_package_module_remove,
    cmd_package_module_renormalize,
    cmd_package_module_get,
    cmd_package_module_list,
    cmd_package_role_add,
    cmd_package_role_update,
    cmd_package_role_remove,
    cmd_package_role_list,
    cmd_package_role_get,
    cmd_package_param_add,
    cmd_package_param_update,
    cmd_package_param_remove,
    cmd_package_param_get,
    cmd_package_param_list,
    cmd_package_infer_archetype,
    cmd_package_compose_baselines,
    cmd_package_baseline_add,
    cmd_package_baseline_update,
    cmd_package_baseline_remove,
    cmd_package_baseline_list,
    cmd_package_baseline_get,
    cmd_package_cohort_update,
    cmd_package_cohort_set,
    cmd_package_cohort_show,
    cmd_package_scope_set,
    cmd_package_scope_clear,
    cmd_package_scope_show,
    cmd_package_golden_list,
    cmd_package_run_show,
    cmd_package_artifact_list,
    cmd_package_artifact_get,
    cmd_package_suggest_modules,
    cmd_package_suggest_roles,
    cmd_package_suggest_cohort,
    cmd_package_candidates,
    cmd_package_apply,
    cmd_package_scout,
    cmd_package_crawl_probe,
    cmd_package_add_product,
    cmd_package,
)


# ---------------------------------------------------------------------------
# Сборка парсера
# ---------------------------------------------------------------------------


def _add_op_common(parser: argparse.ArgumentParser, *, dry_run: bool = True) -> None:
    """Общие флаги мутирующих подкоманд: ``--package``, ``--dry-run``, ``--json``, ``--actor``."""
    parser.add_argument(
        "--package",
        required=True,
        help="пакет: путь или case_id (резолвится в каталоге пакетов)",
    )
    if dry_run:
        parser.add_argument(
            "--dry-run", action="store_true", help="показать проспективный результат без записи"
        )
    parser.add_argument("--json", action="store_true", help="машинночитаемый вывод (JSON)")
    parser.add_argument("--actor", default=None, help="человек, выполняющий операцию (аудит)")


def _add_read_common(parser: argparse.ArgumentParser, *, package: bool = True) -> None:
    """Общие флаги read-only подкоманд."""
    if package:
        parser.add_argument(
            "--package",
            required=True,
            help="пакет: путь или case_id (резолвится в каталоге пакетов)",
        )
    parser.add_argument(
        "--dir",
        default=None,
        help="каталог пакетов для резолва case_id (дефолт — resolved paths.packages, иначе packages)",
    )
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
    p_new.add_argument(
        "--dir",
        default=None,
        help="каталог пакетов (дефолт — resolved paths.packages, иначе packages)",
    )
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
    p_list.add_argument(
        "--dir",
        default=None,
        help="каталог пакетов (дефолт — resolved paths.packages, иначе packages)",
    )
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
    p_compose.add_argument(
        "--package",
        required=True,
        help="пакет: путь или case_id (резолвится в каталоге пакетов; источник каталога)",
    )
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
    p_cand.add_argument(
        "--package",
        required=True,
        help="пакет: путь или case_id (резолвится в каталоге пакетов)",
    )
    _add_profile_flags(p_cand)
    p_cand.add_argument("--scope", default="all", help="'all', модуль или param_id (для baselines)")
    p_cand.add_argument("--write", default=None, metavar="FILE", help="записать JSON-план в файл")
    p_cand.add_argument("--json", action="store_true")
    p_cand.set_defaults(handler=cmd_package_candidates)

    p_apply = pkg.add_parser("apply", help="применить кандидатов (обязателен --actor)")
    p_apply.add_argument(
        "--package",
        required=True,
        help="пакет: путь или case_id (резолвится в каталоге пакетов)",
    )
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
        "--llm-slot",
        choices=["cheap", "flagship"],
        default=None,
        help="активный LLM-слот (override COHOUND_LLM_SLOT/deploy.llm.slot)",
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

    # crawl-probe (автоподбор осей краула) --------------------------------- #
    from core.cli.context import _add_common

    p_probe = pkg.add_parser(
        "crawl-probe",
        help="зонд осей краула по ключам domain_profiles (proposal; --write правит файл)",
    )
    _add_common(p_probe)
    p_probe.add_argument(
        "--package",
        required=True,
        help="пакет: путь или case_id (резолвится в каталоге пакетов)",
    )
    p_probe.add_argument(
        "--urls",
        default=None,
        metavar="FILE",
        help="явное отображение {key: [url, ...]} (JSON/YAML; приоритет над corpus/synth)",
    )
    p_probe.add_argument(
        "--corpus-db",
        dest="corpus_db",
        default=None,
        metavar="DB",
        help="БД прогона: источники URL из corpus_docs (read-only)",
    )
    p_probe.add_argument(
        "--only",
        action="append",
        default=None,
        metavar="KEY",
        help="зондировать только эти ключи domain_profiles (повторяемо)",
    )
    p_probe.add_argument(
        "--out",
        default=None,
        help="каталог proposal (дефолт packages/{case_id}/sources/crawl_probe)",
    )
    p_probe.add_argument(
        "--max-urls-per-key",
        dest="max_urls_per_key",
        type=int,
        default=1,
        help="сколько URL на ключ зондировать (по умолчанию 1)",
    )
    p_probe.add_argument(
        "--no-browser",
        dest="no_browser",
        action="store_true",
        help="только Tier 1 (обычный HTTP), без эскалации на браузер",
    )
    p_probe.add_argument(
        "--write",
        action="store_true",
        help="внести измеренные оси в domain_profiles.yaml (требует --actor)",
    )
    p_probe.add_argument(
        "--actor",
        default=None,
        help="человек, выполняющий запись (обязателен с --write)",
    )
    p_probe.set_defaults(handler=cmd_package_crawl_probe)

    # add-product (продукт → когорта + домены; plan-only по умолчанию) ------------- #
    p_add = pkg.add_parser(
        "add-product",
        help="добавить продукт в когорту: SERP+scout+ключи доменов (plan-only; --apply пишет)",
    )
    _add_op_common(p_add)
    p_add.add_argument("product_name", metavar="NAME", help="человеческое имя продукта")
    p_add.add_argument(
        "--url", default=None, help="подсказка URL продукта (идентификация при синонимах)"
    )
    p_add.add_argument(
        "--search-adapter",
        dest="search_adapter_kind",
        default=None,
        choices=list(SUPPORTED_SEARCH_ADAPTERS),
        help="провайдер поиска; по умолчанию — из config/deploy.yaml "
        "(search.adapter/by_language по --lang) или yandex",
    )
    p_add.add_argument("--category", default=None, help="категория (для запросов/артефакта)")
    p_add.add_argument(
        "--reason", default=None, help="selection_reason когорты (иначе — из scout)"
    )
    p_add.add_argument(
        "--apply", action="store_true", help="применить план (требует --actor); без него — plan-only"
    )
    p_add.add_argument(
        "--passport", action="store_true", help="также записать черновик паспорта (с --apply)"
    )
    p_add.add_argument(
        "--passports-dir",
        default=None,
        help="каталог паспортов для --passport (дефолт — resolved paths.passports)",
    )
    p_add.add_argument("--lang", choices=["ru", "en"], default="ru", help="язык SERP-запросов")
    p_add.add_argument("--limit", type=int, default=10, help="лимит результатов SERP на запрос")
    p_add.add_argument("--max-pages", type=int, default=5, help="потолок фетчей разведки")
    p_add.set_defaults(handler=cmd_package_add_product)

    p_package.set_defaults(handler=cmd_package, _package_parser=p_package)
