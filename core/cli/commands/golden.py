"""``golden``: сверка вердиктов прогона с эталонным ``golden.csv`` (R6/R12, §11.3).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

**Эталон может отсутствовать.** ``golden.csv`` — проверочный инструмент, а не условие
измерения, поэтому «сверять не с чем» — законный режим (первая версия таблицы сравнения,
новый продукт в когорте). Состояние эталона объявляется файлом пакета
(:mod:`core.pipeline.golden_status`):

* ``present`` — набор сверяется (прежнее поведение, в т.ч. явный ``--golden``);
* ``absent``/``declined`` — команда отвечает «неприменимо» и возвращает
  :data:`~core.cli.errors.EXIT_USAGE` (2) с кодом ``golden_not_configured`` и причиной из
  объявления. Это **не** :data:`~core.cli.errors.EXIT_OK` («не выполнилось» не должно
  выглядеть зелёным в CI) и **не** провал порога: смешение этих исходов и было дефектом,
  когда пустой отбор печатал ``R6: agreement 0.0% < 80%``.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _load_golden_arg,
    _print_json,
    _resolve_db_path,
    _restore_plan_inputs,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, EXIT_USAGE, CliError
from core.cli.wiring import build_store
from core.pipeline import check_thresholds, evaluate_golden
from core.pipeline.golden_status import GOLDEN_FILE, GoldenStatus, read_golden_status


def _not_configured(
    args: argparse.Namespace, run_id: str | None, status: GoldenStatus
) -> int:
    """Печатает «сверка неприменима» и возвращает :data:`EXIT_USAGE`.

    Отдельный исход, а не провал порога и не успех: пороги не считались, потому что
    сравнивать не с чем. Причина и автор берутся из объявления, поэтому ответ объясняет
    **почему**, а не только «нет».
    """
    message = status.describe()
    if args.json:
        _print_json(
            {
                "ok": False,
                "error": "golden_not_configured",
                "message": message,
                "run_id": run_id,
                "golden_state": status.state,
                "golden_declared": status.declared,
                "golden_file_present": status.file_present,
                "golden_reason": status.reason,
                "golden_declined_by": status.declined_by,
                "golden_declined_at": status.declined_at,
            }
        )
    else:
        sys.stdout.write(f"golden: неприменимо — {status.describe()}\n")
        sys.stdout.write(
            "golden: пороги не считались (это не провал R6/R12); код возврата 2 — "
            "команда не выполнилась\n"
        )
    return EXIT_USAGE


def run(args: argparse.Namespace) -> int:
    """``golden``: сверка вердиктов прогона с эталонным ``golden.csv`` (R6/R12, §11.3).

    Хранилище резолвится как в ``sensitivity``: явный ``--db`` (``run_id`` из
    ``--run-id`` либо выводится, если в БД ровно один прогон) **или** ``--runs-dir`` +
    ``--run-id`` → ``runs/{run_id}/cohound.db``.

    **Источник эталона.** Явный ``--golden`` — прежний контракт (путь берётся как есть).
    Без ``--golden`` набор берётся из пакета (``--package``): читается объявление
    ``sources/golden_status.yaml``, и при ``absent``/``declined`` команда не сравнивает с
    пустотой, а сообщает «неприменимо» (:func:`_not_configured`). Противоречие «явный
    ``--golden`` при объявлении ``declined``» — видимая ошибка, а не тихий выбор одной из
    сторон: эталон, объявленный непригодным, не должен молча попасть в сверку.

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
    Неприменимый эталон — :data:`EXIT_USAGE` (вход не выполним), отдельно от обоих.

    :param args: ``--golden`` (путь, опц.) либо ``--package`` (объявление эталона),
        источник хранилища, ``--tier``, ``--run-yaml``/``--package`` (опц.).
    """
    explicit = args.golden is not None
    declared: GoldenStatus | None = None
    if not explicit:
        if args.package is None:
            raise CliError(
                "нужен --golden (путь к набору) либо --package (каталог пакета с "
                "объявлением эталона: sources/golden_status.yaml)",
                code="usage",
                exit_code=EXIT_USAGE,
            )
        # Объявление читается до открытия БД: ответ «сверка неприменима» не зависит от
        # прогона, и оператор получает его без валидного ``cohound.db``.
        declared = read_golden_status(args.package)
        if not declared.comparison_available:
            return _not_configured(args, args.run_id, declared)

    db_path = _resolve_db_path(args)
    run_id = args.run_id
    notes: list[str] = []
    report = None
    with build_store(db_path, profile=args.profile, read_only=True) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        if explicit:
            if args.package is not None:
                declared = read_golden_status(args.package)
                if not declared.comparison_available:
                    raise CliError(
                        "--golden задан явно, но пакет объявляет эталон неприменимым "
                        f"({declared.describe()}); уберите --golden либо приведите "
                        "объявление в соответствие — молча выбирать сторону нельзя",
                        code="golden_status_conflict",
                        exit_code=EXIT_USAGE,
                    )
            golden = _load_golden_arg(args.golden)
        else:
            golden = _load_golden_arg(Path(args.package) / GOLDEN_FILE)
        if args.tier is not None:
            selected = [row for row in golden if row.tier.value == args.tier]
            if not selected:
                # Пустой отбор — **не** провал порога, а невыполнимый запрос: набор не
                # содержит строк этого tier'а. Прежде пустой список уходил в
                # ``evaluate_golden``, и оператор видел «agreement 0.0% < 80%» — то есть
                # сообщение о качестве оценщика там, где проблема в данных golden.csv
                # (живой факт: 236 строк, все ``full``; ``--tier fast`` выбирал ноль).
                available = dict(sorted(Counter(row.tier.value for row in golden).items()))
                message = (
                    f"в golden-наборе нет строк tier={args.tier!r}: "
                    f"доступно {available or '{}'}, всего строк {len(golden)} — "
                    "фильтр выбрал бы 0 строк"
                )
                if args.json:
                    _print_json(
                        {
                            "ok": False,
                            "error": "golden_tier_empty",
                            "message": message,
                            "run_id": run_id,
                            "tier": args.tier,
                            "available_tiers": available,
                            "total_rows": len(golden),
                        }
                    )
                else:
                    sys.stdout.write(f"golden: {message} (неисполнимый запрос)\n")
                return EXIT_USAGE
            golden = selected
        catalog, _weights, _cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="golden", store=store
        )
        report = evaluate_golden(
            golden=golden, store=store, run_id=run_id, catalog=catalog
        )

    # Сужение типа для mypy: ``report`` присваивается безусловно (ветка выше возвращает
    # EXIT_USAGE, когда золотой набор не исполним), поэтому ``None`` здесь недостижим.
    # ``raise`` вместо ``assert`` завёл бы новый путь ошибки с выдуманным кодом для
    # состояния, которого нет; молчание исключено — это проверка инварианта, не отказ.
    assert report is not None  # noqa: S101 — сужение типа для недостижимого None
    violations = check_thresholds(report)
    ok = report.passes() and report.has_absent_stratum
    payload = {
        "ok": ok,
        "run_id": run_id,
        "tier": args.tier,
        "golden_source": "cli" if explicit else "package",
        # ``None`` — явный ``--golden`` без ``--package``: объявления не читали, и
        # выдавать файл за «объявленный эталон пакета» было бы провенанс-ложью.
        "golden_state": declared.state if declared is not None else None,
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
        "golden",
        help=(
            "сверка прогона с golden.csv (R6/R12, §11.3; --tier fast|full); "
            "эталон берётся из --golden либо из объявления пакета (--package)"
        ),
    )
    p_golden.add_argument(
        "--golden",
        default=None,
        help=(
            "путь к golden.csv (§4.2/§11.6); без флага набор берётся из объявления "
            "пакета (--package): state present — сверка, absent/declined — «неприменимо»"
        ),
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
        help=(
            "каталог пакета: объявление эталона (sources/golden_status.yaml) и каталог "
            "для R12 (вместе с --run-yaml)"
        ),
    )
    p_golden.add_argument(
        "--tier",
        choices=["fast", "full"],
        default=None,
        help="фильтр golden-строк по тиру до оценки (PR-гейт fast vs полный full)",
    )
    _add_common(p_golden)
    p_golden.set_defaults(handler=run)
