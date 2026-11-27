"""``gc``: уборка артефактов на диске, на которые не ссылается ни одна ``state.db``.

Команда — единственная поверхность уборки: логика живёт в
:mod:`core.pipeline.gc` (перенесена из прибора ``scripts/prune_orphans.py``, который
остался тонкой обёрткой над :func:`main`). Здесь только аргументы, печать и коды
возврата — поведение и семантика описаны в модуле логики.

Три отчётных класса появились вместе с командой и **не** удаляются никогда:

* ``runs_signed`` — прогон с подписанным G4 (опубликованное evidence);
* ``runs_resumable`` — прогон с возобновляемым сеансом (переигровка без сети);
* ``runs_unsigned`` — всё остальное. Это **не** список кандидатов на удаление: waiver
  вместо G4 легален, а прогон с ячейками уже произвёл вердикты, и его ``outputs/`` без
  него не восстановить. Удаление — только явным ``--prune-run RUN_ID``.

Коды возврата: ``0`` — успех, ``1`` — часть файлов удалить не удалось, ``2`` — отказ до
удаления (ничего не записано).

Нейтрален к предметной категории (§1.3): продукт приходит только как ``--package``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from core.cli.context import _add_common, _resolve_paths
from core.cli.errors import EXIT_OK, EXIT_USAGE
from core.pipeline.gc import (
    ALL_CLASSES,
    CACHE_BUDGET_CLASS,
    CLASS_ORDER,
    DELETABLE_CLASSES,
    ONLY_GROUPS,
    REPORT_ONLY_CLASSES,
    RUN_CLASS_ORDER,
    CacheBudget,
    GcRefused,
    Plan,
    RunCensus,
    RunPrice,
    aggregate,
    apply_gc,
    build_gc_plan,
    human_bytes,
    total,
)

__all__ = ["configure", "main", "run"]

#: Сколько примеров печатать в человекочитаемом отчёте, чтобы он не стал дампом.
_SAMPLES = 3

#: Текст, который обязан стоять под классом ``runs_unsigned``: класс отчётный.
_UNSIGNED_NOTE = (
    "runs_unsigned — НЕ кандидаты на удаление: waiver вместо G4 легален, а прогон "
    "с ячейками\n   уже произвёл вердикты; удаление только явным --prune-run RUN_ID"
)


def _add_gc_arguments(parser: argparse.ArgumentParser) -> None:
    """Аргументы ``hound gc`` (общие для CLI и для обёртки-скрипта)."""
    parser.add_argument(
        "--package",
        required=True,
        type=Path,
        help="каталог пакета (его пространство решает судьбу http-кэша)",
    )
    parser.add_argument(
        "--also-package",
        action="append",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "дополнительный пакет: его пространство тоже участвует в решении по "
            "http-кэшу (cache/http общий для всех пакетов); повторяемый"
        ),
    )
    parser.add_argument(
        "--state-db",
        action="append",
        type=Path,
        default=None,
        metavar="PATH",
        help="state.db пакета; повторяемый — живое множество считается по всем сразу",
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        default=None,
        metavar="DIR",
        help="добавить все найденные под DIR state.db (rglob) к набору",
    )
    parser.add_argument(
        "--runs-dir",
        action="append",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "каталог прогонов: добавляет {DIR}/*/cohound.db — по ним считается ЦЕНА "
            "удаления и отчёт по прогонам; повторяемый"
        ),
    )
    parser.add_argument(
        "--runs-root",
        action="append",
        type=Path,
        default=None,
        metavar="DIR",
        help="добавить все найденные под DIR cohound.db (rglob); повторяемый",
    )
    parser.add_argument(
        "--corpus-raw",
        type=Path,
        default=None,
        help="корень corpus_raw (дефолт — раскладка: paths.corpus_raw)",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=None,
        help="корень cache (дефолт — раскладка: paths.cache)",
    )
    parser.add_argument(
        "--extract-cache",
        type=Path,
        default=None,
        help="каталог кэша экстракции для отчёта cache_budget (дефолт — paths.extract_cache)",
    )
    parser.add_argument(
        "--embed-cache",
        type=Path,
        default=None,
        help="каталог кэша векторов для отчёта cache_budget (дефолт — paths.embed_cache)",
    )
    parser.add_argument(
        "--eval-cache",
        type=Path,
        default=None,
        help="каталог кэша вердиктов для отчёта cache_budget (дефолт — paths.eval_cache)",
    )
    parser.add_argument(
        "--only",
        default=None,
        help=(
            "ограничить группы: corpus-raw,http-cache,runs,cache-budget "
            "(или имена классов)"
        ),
    )
    parser.add_argument(
        "--apply", action="store_true", help="удалить файлы (без флага — только отчёт)"
    )
    parser.add_argument("--actor", default=None, help="кто удаляет (обязателен с --apply)")
    parser.add_argument(
        "--audit",
        type=Path,
        default=None,
        help="JSONL-аудит (по умолчанию рядом с первой state.db набора)",
    )
    parser.add_argument(
        "--prune-run",
        action="append",
        default=None,
        metavar="RUN_ID",
        help=(
            "удалить каталог прогона целиком (повторяемый); отказ на подписанном G4 и на "
            "возобновляемом сеансе"
        ),
    )


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``gc``."""
    parser = sub.add_parser(
        "gc",
        help=(
            "уборка артефактов вне state.db: corpus_raw, http-кэш; отчёт по прогонам "
            "и бюджетам кэшей (dry-run по умолчанию)"
        ),
        description=(
            "Удалить артефакты на диске, не упомянутые ни в одной из переданных state.db "
            "(dry-run по умолчанию). Живое множество — ОБЪЕДИНЕНИЕ по всем БД: один "
            "каталог corpus_raw делят канонная БД и БД стендов, и прогон по одной БД "
            "удаляет чужой корпус (замер 27.11.2026: 5 455 файлов / 1.09 GB)."
        ),
    )
    _add_gc_arguments(parser)
    _add_common(parser)
    parser.set_defaults(handler=run)


# ------------------------------------------------------------------ печать


def _parse_only(raw: str | None) -> frozenset[str] | None:
    """``--only corpus-raw,http-cache`` → набор классов (``None`` — все)."""
    if raw is None:
        return None
    classes: set[str] = set()
    for token in (part.strip() for part in raw.split(",")):
        if not token:
            continue
        if token in ONLY_GROUPS:
            classes.update(ONLY_GROUPS[token])
        elif token in ALL_CLASSES:
            classes.add(token)
        else:
            raise GcRefused(
                f"неизвестный класс/группа в --only: {token!r}; "
                f"известны: {', '.join(sorted(ONLY_GROUPS))} "
                f"(или имена классов: {', '.join(ALL_CLASSES)})"
            )
    if not classes:
        raise GcRefused("--only пуст: не указан ни один класс")
    return frozenset(classes)


def _print_packages(plan: Plan) -> None:
    """Пакеты, чьи пространства решали судьбу http-кэша, и цена этого правила."""
    if len(plan.packages) > 1:
        print("пакеты (пространства объединены):")
        for item in plan.packages:
            print(f"  {item}")
    rescued = plan.rescued_by_other_packages
    print(
        f"спасено соседними пакетами: {rescued['files']} файлов "
        f"({human_bytes(rescued['bytes'])}) — остались бы удалены при одном --package"
    )


def _print_run_census(runs: RunCensus) -> None:
    """Сколько прогонных БД прочитано и сколько из них живых — контекст цены."""
    if not runs.known:
        print("прогонные БД: не переданы — цена удаления неизвестна")
        return
    print(
        f"прогонных БД: {len(runs.dbs)}, живых {len(runs.live_runs)}, "
        f"ссылок на байты {len(runs.paths)}"
    )


def _print_state_dbs(plan: Plan) -> None:
    """Список участвовавших БД и живых путей в каждой — что именно объединили."""
    state = plan.state
    print(f"state.db в наборе: {len(state.dbs)}")
    for db in state.dbs:
        print(f"  {db.path}: версий {db.version_rows}, живых путей {db.live_paths}")
    print(f"  объединение: живых путей {len(state.live)} (объединение по всем БД)")


def _print_price(price: RunPrice) -> None:
    """Разбивка цены под классом ``corpus_raw_run_history``.

    Три строки — **не** разбиение: ``by_resumable_session`` пересекается с обеими
    остальными (флаг), поэтому и подписан флагом, а не категорией.
    """
    rows = (
        ("цена: живые прогоны", price.by_live_runs, ""),
        ("цена: только мёртвые прогоны", price.by_dead_runs, ""),
        (
            "цена: возобновляемые сеансы",
            price.by_resumable_session,
            "  (флаг, пересекается с двумя строками выше)",
        ),
    )
    for label, bucket, note in rows:
        print(
            f"   {label:<28}{bucket['files']:>8} файлов "
            f"{human_bytes(bucket['bytes']):>12}{note}"
        )
    print(
        "   цена: байты переигровки без сети и возобновляемых сеансов — "
        "resume/frozen по этим прогонам пойдут в сеть"
    )
    drain = price.by_drain
    print(
        f"   цена: дренаж/возобновляемый сеанс — {drain['files']} файлов "
        f"({human_bytes(drain['bytes'])}); после удаления докачка начнётся заново"
    )


def _print_report(
    orphans: list[Any],
    totals: dict[str, dict[str, int]],
    price: RunPrice,
) -> None:
    for cls in CLASS_ORDER:
        bucket = totals[cls]
        if not bucket["files"]:
            continue
        mark = "удаляется" if cls in DELETABLE_CLASSES else "только отчёт"
        print(
            f"{cls:<28}{bucket['files']:>8} файлов {human_bytes(bucket['bytes']):>12}  "
            f"({mark})"
        )
        for orphan in [item for item in orphans if item.cls == cls][:_SAMPLES]:
            print(f"    {orphan.path}")
        if cls == "corpus_raw_run_history":
            _print_price(price)
    deletable = total(totals, DELETABLE_CLASSES)
    reported = total(totals, REPORT_ONLY_CLASSES)
    print(
        f"{'ИТОГО удаляется':<28}{deletable['files']:>8} файлов "
        f"{human_bytes(deletable['bytes']):>12}"
    )
    print(
        f"{'ИТОГО в отчёте':<28}{reported['files']:>8} файлов "
        f"{human_bytes(reported['bytes']):>12}"
    )


def _print_runs(plan: Plan) -> None:
    """Отчётные классы прогонов: ``run_id``, ячейки, ``outputs/`` и размер каталога.

    Печатается каждая прогонная БД, а не выборка: иначе 1267-ячеечная карта неотличима
    от пустой пробы, а именно это различие решает, удалять ли прогон.
    """
    grouped = plan.runs_by_class()
    printed = False
    for cls in RUN_CLASS_ORDER:
        dbs = grouped[cls]
        if not dbs:
            continue
        printed = True
        size = sum(db.bytes for db in dbs)
        print(
            f"{cls:<28}{len(dbs):>8} прогонов {human_bytes(size):>12}  (только отчёт)"
        )
        if cls == "runs_unsigned":
            print(f"   {_UNSIGNED_NOTE}")
        for db in dbs:
            print(
                f"    {db.run_id:<40} ячеек {db.cells:>6}  outputs {db.outputs:>4}  "
                f"{human_bytes(db.bytes):>10}"
            )
    if not printed:
        print("прогоны: ни одной прогонной БД не прочитано")


def _print_cache_budget(rows: list[CacheBudget]) -> None:
    """Объём кэшей против объявленных бюджетов."""
    if not rows:
        return
    print("кэши:")
    for row in rows:
        if row.budget is None:
            print(f"  {row.name:<14}{human_bytes(row.bytes):>12}  бюджета нет")
            continue
        share = row.share or 0.0
        mark = "ПРЕВЫШЕН" if row.over_budget else "в пределах"
        print(
            f"  {row.name:<14}{human_bytes(row.bytes):>12}  бюджет "
            f"{human_bytes(row.budget)}  {share:.0%}  {mark}"
        )


# ------------------------------------------------------------------ тело команды


def _effective_paths(args: argparse.Namespace) -> Any:
    """Раскладка: ``args.paths`` (composition root) либо резолв по env/файлу."""
    paths = getattr(args, "paths", None)
    return paths if paths is not None else _resolve_paths(args)


def _payload(
    *,
    plan: Plan,
    args: argparse.Namespace,
    corpus_raw: Path,
    cache: Path,
    totals: dict[str, dict[str, int]],
    deleted: list[Any],
    pruned: list[Any],
    failures: list[tuple[Path, str]],
    missing_roots: list[str],
) -> dict[str, Any]:
    """Машинный отчёт: прежние поля плюс прогоны, бюджеты кэшей и удалённые прогоны."""
    state = plan.state
    classes: dict[str, Any] = {
        cls: {
            "files": totals[cls]["files"],
            "bytes": totals[cls]["bytes"],
            "deletable": cls in DELETABLE_CLASSES,
        }
        for cls in CLASS_ORDER
    }
    grouped = plan.runs_by_class()
    runs: dict[str, list[dict[str, Any]]] = {}
    for cls in RUN_CLASS_ORDER:
        dbs = grouped[cls]
        classes[cls] = {
            "runs": len(dbs),
            "bytes": sum(db.bytes for db in dbs),
            "cells": sum(db.cells for db in dbs),
            # Отчётный класс: удаляется только явным --prune-run.
            "deletable": False,
        }
        runs[cls] = [db.as_dict() for db in dbs]
    return {
        "package": str(args.package),
        "packages": [str(item) for item in plan.packages],
        "rescued_by_other_packages": plan.rescued_by_other_packages,
        "state_dbs": [str(db.path) for db in state.dbs],
        "state_db_info": state.as_dict()["state_dbs"],
        "live_paths": len(state.live),
        "uncovered_state_dbs": plan.uncovered_state_dbs,
        "run_census": plan.runs.as_dict(),
        "price": plan.price.as_dict(),
        "runs": runs,
        "cache_budget": [row.as_dict() for row in plan.cache_budgets],
        "corpus_raw": str(corpus_raw),
        "cache": str(cache),
        "apply": bool(args.apply),
        "classes": classes,
        "missing_roots": missing_roots,
        "total": total(totals, DELETABLE_CLASSES),
        "report_only": total(totals, REPORT_ONLY_CLASSES),
        "deleted": {
            "files": len(deleted),
            "bytes": sum(item.size for item in deleted),
        },
        "pruned": {
            "runs": len(pruned),
            "bytes": sum(item.db.bytes for item in pruned),
        },
        "failed": [{"path": str(p), "reason": r} for p, r in failures],
    }


def run(args: argparse.Namespace) -> int:
    """``gc``: строит план, печатает отчёт и (с ``--apply``) удаляет файлы.

    :returns: ``0`` — успех, ``1`` — часть файлов удалить не удалось, ``2`` — отказ.
    """
    paths = _effective_paths(args)
    corpus_raw = Path(args.corpus_raw) if args.corpus_raw else Path(paths.corpus_raw)
    cache = Path(args.cache) if args.cache else Path(paths.cache)
    extract_cache = (
        Path(args.extract_cache) if args.extract_cache else Path(paths.extract_cache)
    )
    embed_cache = Path(args.embed_cache) if args.embed_cache else Path(paths.embed_cache)
    eval_cache = Path(args.eval_cache) if args.eval_cache else Path(paths.eval_cache)

    try:
        if args.apply and not args.actor:
            raise GcRefused("--apply без --actor: удаление без имени в аудите запрещено")
        only = _parse_only(args.only)
        wanted = ALL_CLASSES if only is None else only
        deploy = getattr(args, "deploy", None)
        budget: int | None = None
        if CACHE_BUDGET_CLASS in wanted:
            if deploy is None:
                from core.config.deploy import load_deploy_config

                deploy = load_deploy_config()
            budget = deploy.extract.cache_max_bytes
        plan = build_gc_plan(
            package=args.package,
            also_packages=args.also_package,
            state_dbs=args.state_db,
            state_root=args.state_root,
            runs_dirs=args.runs_dir,
            runs_roots=args.runs_root,
            corpus_raw=corpus_raw,
            cache=cache,
            extract_cache=extract_cache,
            embed_cache=embed_cache,
            eval_cache=eval_cache,
            extract_cache_budget=budget,
            prune_runs=args.prune_run,
            only=only,
        )
    except GcRefused as exc:
        print(f"ОТКАЗ: {exc}", file=sys.stderr)
        return EXIT_USAGE

    orphans = plan.orphans
    totals = aggregate(orphans)
    failures: list[tuple[Path, str]] = []
    deleted: list[Any] = []
    pruned: list[Any] = []
    audit_path = args.audit or (plan.state.dbs[0].path.parent / "prune_orphans.jsonl")

    if not plan.runs.known:
        print(
            "ВНИМАНИЕ: прогонные БД не переданы (--runs-dir/--runs-root) — цена удаления "
            "неизвестна; класс corpus_raw_run_history не вычисляется",
            file=sys.stderr,
        )

    for entry in plan.uncovered_state_dbs:
        print(
            f"ВНИМАНИЕ: рядом есть state.db вне набора — {entry['path']} "
            f"(продукты: {', '.join(entry['products'])}); живое множество может быть "
            "неполным, добавь --state-db или --state-root",
            file=sys.stderr,
        )

    # Отсутствующий корень — не «файлов нет», а «смотреть было некуда»: молчаливый ноль
    # читался бы как чистый каталог.
    missing_roots = [str(path) for path in (corpus_raw, cache) if not path.is_dir()]

    # Предупреждения о названных прогонах печатаются ДО удаления: решение принимает
    # человек, и он обязан увидеть, что именно теряет.
    for warning in plan.prune_warnings:
        print(warning, file=sys.stderr)

    if args.apply and (any(item.cls in DELETABLE_CLASSES for item in orphans) or plan.prune_runs):
        result = apply_gc(
            plan,
            actor=str(args.actor),
            audit=audit_path,
            package=args.package,
            totals=totals,
        )
        deleted = list(result.deleted)
        pruned = list(result.pruned)
        failures = list(result.failures)

    if args.json:
        print(
            json.dumps(
                _payload(
                    plan=plan,
                    args=args,
                    corpus_raw=corpus_raw,
                    cache=cache,
                    totals=totals,
                    deleted=deleted,
                    pruned=pruned,
                    failures=failures,
                    missing_roots=missing_roots,
                ),
                ensure_ascii=False,
                indent=1,
            )
        )
    else:
        _print_packages(plan)
        _print_state_dbs(plan)
        _print_run_census(plan.runs)
        print()
        _print_report(orphans, totals, plan.price)
        _print_runs(plan)
        _print_cache_budget(plan.cache_budgets)
        for root in missing_roots:
            print(f"ВНИМАНИЕ: каталога нет — {root}", file=sys.stderr)
        if not args.apply:
            print("dry-run: ничего не удалено (--apply --actor NAME для удаления)")
        else:
            freed = sum(item.size for item in deleted) + sum(
                item.db.bytes for item in pruned
            )
            print(
                f"удалено файлов: {len(deleted)}, прогонов: {len(pruned)}, "
                f"освобождено {human_bytes(freed)}"
            )
            if deleted or pruned:
                print(f"аудит: {audit_path}")
        for path, reason in failures:
            print(f"НЕ УДАЛЕНО: {path}: {reason}", file=sys.stderr)

    return 1 if failures else EXIT_OK


def main(argv: list[str] | None = None) -> int:
    """Standalone-вход (``scripts/prune_orphans.py``): те же флаги, вывод и коды."""
    parser = argparse.ArgumentParser(
        prog="prune_orphans.py",
        description=(
            "Удалить артефакты на диске, не упомянутые ни в одной из переданных state.db "
            "(dry-run по умолчанию). Тонкая обёртка над ``core.pipeline.gc``: логика "
            "живёт в core, здесь — только разбор аргументов."
        ),
    )
    _add_gc_arguments(parser)
    _add_common(parser)
    args = parser.parse_args(argv)
    return run(args)


if __name__ == "__main__":  # pragma: no cover - ручной запуск
    raise SystemExit(main())
