"""``golden``: сверка вердиктов прогона с эталонным ``golden.csv`` (R6/R12, §11.3).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse
import json
import sys

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _load_golden_arg,
    _print_json,
    _resolve_db_path,
    _restore_plan_inputs,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK
from core.cli.wiring import build_store
from core.pipeline import check_thresholds, evaluate_golden


def run(args: argparse.Namespace) -> int:
    """``golden``: сверка вердиктов прогона с эталонным ``golden.csv`` (R6/R12, §11.3).

    Хранилище резолвится как в ``sensitivity``: явный ``--db`` (``run_id`` из
    ``--run-id`` либо выводится, если в БД ровно один прогон) **или** ``--runs-dir`` +
    ``--run-id`` → ``runs/{run_id}/cohound.db``.

    Каталог (для классификации критических параметров, R12) берётся из явных
    ``--run-yaml`` + ``--package`` при их наличии, иначе из снимка прогона в БД
    (``run_inputs``). Лишь если оба источника недоступны, ``evaluate_golden``
    получает **пустой** каталог и важность параметров неизвестна — правило «false ❌ на Кр»
    тогда не срабатывает, что честно печатается в ``notes``.

    ``--tier fast|full`` фильтрует строки :class:`~core.domain.models.GoldenRow` по
    ``tier`` **до** оценки (PR-гейт vs полный набор, §11.6).

    **Контракт кода возврата.** :data:`EXIT_OK` — только если одновременно
    ``report.passes()`` (R6: agreement + критические классы) **и**
    ``report.has_absent_stratum`` (R12/§4.2: в наборе есть страта ожидаемых ❌); иначе
    :data:`EXIT_ERROR`. Нарушения порогов (``check_thresholds``) печатаются явно.

    :param args: ``--golden`` (путь), источник хранилища, ``--tier``, ``--run-yaml``/
        ``--package`` (опц.).
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    notes: list[str] = []
    report = None
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        golden = _load_golden_arg(args.golden)
        if args.tier is not None:
            golden = [row for row in golden if row.tier.value == args.tier]
        catalog, _weights, _cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="golden", store=store
        )
        report = evaluate_golden(
            golden=golden, store=store, run_id=run_id, catalog=catalog
        )

    assert report is not None  # evaluate_golden выполняется безусловно (см. выше)
    violations = check_thresholds(report)
    ok = report.passes() and report.has_absent_stratum
    payload = {
        "ok": ok,
        "run_id": run_id,
        "tier": args.tier,
        "violations": violations,
        "notes": notes,
        **json.loads(report.to_manifest_json()),
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(report.summary() + "\n")
        for note in notes:
            sys.stdout.write(f"note: {note}\n")
        for violation in violations:
            sys.stdout.write(f"violation: {violation}\n")
        sys.stdout.write(f"ok: {'yes' if ok else 'no'}\n")
    return EXIT_OK if ok else EXIT_ERROR


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``golden``."""
    p_golden = sub.add_parser(
        "golden", help="сверка прогона с golden.csv (R6/R12, §11.3; --tier fast|full)"
    )
    p_golden.add_argument(
        "--golden", required=True, help="путь к golden.csv (§4.2/§11.6)"
    )
    golden_source = p_golden.add_mutually_exclusive_group(required=True)
    golden_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    golden_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_golden.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_golden.add_argument(
        "--run-yaml",
        default=None,
        help="run.yaml для каталога (важность Кр, R12; вместе с --package)",
    )
    p_golden.add_argument(
        "--package",
        default=None,
        help="каталог пакета для каталога (вместе с --run-yaml)",
    )
    p_golden.add_argument(
        "--tier",
        choices=["fast", "full"],
        default=None,
        help="фильтр golden-строк по тиру до оценки (PR-гейт fast vs полный full)",
    )
    _add_common(p_golden)
    p_golden.set_defaults(handler=run)
