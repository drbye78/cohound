"""coHound — CLI-ядро (composition root Z0, ручной DI, A1; ``03`` §E).

Точка входа :func:`main` реализует команды W5/T5.3: ``resolve``, ``plan`` и ``run``,
который связывает реальный реестр фаз (:func:`core.pipeline.build_phases`) с портами
профиля (composition root, L3). Команды ``diff`` (§9/§13), ``sensitivity`` (§6/K6),
``golden`` (R6/R12, §11.3) и ``report`` (§9/Ш9) экспонируют уже построенные возможности
``pipeline/report.py``, ``pipeline/compute.py`` и ``pipeline/golden.py`` без дублирования
логики. ``run`` загружает паспорта (``--passports-dir``) и операторский корпус URL
(``--urls``) — специфицированного артефакта URL нет, поэтому ``doc_domains`` молча не
фетчатся (I19). Только stdlib (``argparse``/``json``/``sys``) + ``yaml`` (зависимость
ядра) — без фреймворков (A1).

Команды разведки/ревью экспонируют уже готовые модули ``pipeline/discovery.py`` и
``pipeline/review.py`` (T10.4, ``03`` §D.6/E.1):

* ``discover`` — внешний поиск (SERP) как источник **кандидатов-гипотез** (Ш2/Ш5).
  Выход — только ``DiscoveryCandidate`` (URL+сниппет); evidence **не** пишется и ни одна
  гипотеза не одобряется автоматически (I19/G2, A3). ``doc_domains`` паспортов
  (``--passports-dir``) используются **лишь** как seed-подсказки запросов и не фетчатся.
* ``review`` — детерминированная очередь ревью (preliminary-контур + ``❓``-кандидаты,
  Ш7/G3).
* ``promote`` — человеческое повышение ``preliminary`` → ``evaluated`` (переаттестация,
  I14). Актор обязателен и непуст (I6/I7); без валидированного вердикта при
  ``--validated-only`` (дефолт) повышение отклоняется — вердикт не выдумывается.

**Принципы UX (``03`` §E.2).**

- гейты — явные человеческие решения: ``run`` останавливается и печатает, что требуется;
- идемпотентность/resume: повторный ``run`` продолжает с прерванной фазы;
- ``--dry-run`` — показать план/бюджеты без исполнения и без записи;
- ``--json`` — машинночитаемый вывод для CI и интеграций;
- ошибки несут **код инварианта/правила**: ``I2b: ...``, ``S11: ...`` (ссылка на регламент).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from core.adapters.gateway_validate import validate_envelope_shape
from core.adapters.remote_gateway import (
    DEFAULT_SCHEMA_VERSION,
    GatewayError,
    TransportFn,
)
from core.cli.wiring import (
    LOCAL_PROFILE,
    SPLIT_PROFILE,
    SUPPORTED_PROFILES,
    SUPPORTED_SEARCH_ADAPTERS,
    WiringError,
    build_adapters,
    build_discovery_llm,
    build_gateway,
    build_runner,
    build_scout_crawler,
    build_search_adapter,
    build_store,
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
from core.config.deploy import DeployConfig, is_split, load_deploy_config
from core.domain.enums import Status
from core.domain.models import CatalogRow, Cohort
from core.pipeline import (
    GATE_PHASES,
    PHASES,
    Ports,
    RunReport,
    build_phases,
    check_thresholds,
    discover,
    evaluate_golden,
    promote_batch,
    review_queue,
    scout_passport,
    write_passport_draft,
)
from core.pipeline.compute import compute
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import RunInputs, load_inputs
from core.pipeline.observability import instrument_phases, write_run_metrics
from core.pipeline.plan import RunPlan, derive_run_id, plan as build_plan
from core.pipeline.report import CrossCycleDiffError, render_diff, write_reports
from core.pipeline.review import PromotionResult, ReviewError, ReviewItem
from core.pipeline.runner import Clock, PhaseFn, _gate_phase
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMPort
from core.ports.gateway import ContentGateway
from core.ports.search import SearchAdapterPort

__all__ = [
    "EXIT_OK",
    "EXIT_ERROR",
    "EXIT_USAGE",
    "EXIT_BLOCKED",
    "EXIT_NOT_FOUND",
    "CliError",
    "LintFailure",
    "main",
    "build_parser",
]

#: Фабрика диагностического gateway по deploy-конфигу (тестовый шов ``deploy-check``).
GatewayFactory = Callable[[DeployConfig], ContentGateway]

#: Фабрика реестра фаз прогона (тестовый шов ``run``: подмена реального ``build_phases``).
PhasesFactory = Callable[..., dict[str, PhaseFn]]
#: Фабрика плана прогона (тестовый шов ``run``: подмена детерминированного ``plan``).
PlanFactory = Callable[..., RunPlan]

# -- Коды возврата ---------------------------------------------------------- #
#: Успех.
EXIT_OK = 0
#: Непредвиденная ошибка исполнения.
EXIT_ERROR = 1
#: Ошибка входа/конфигурации: блокирующий линт, валидация, неразобранные аргументы.
EXIT_USAGE = 2
#: Прогон остановлен на гейте (нужно человеческое решение, ``03`` §E.2).
EXIT_BLOCKED = 3
#: Запрошенный объект не найден (например, ``source_id`` не резолвится).
EXIT_NOT_FOUND = 4

#: Код по умолчанию для ошибок без распознанного инварианта/правила.
_FALLBACK_CODE = "E"

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

#: ``CODE: message`` — сообщения линтера/инвариантов начинаются с кода правила.
_CODE_RE = re.compile(r"^\s*([^\s:]+):")
#: Код инварианта внутри текста (например, ``I2b``).
_INVARIANT_RE = re.compile(r"\bI\d+[a-z]?\b")
#: Ссылка на раздел регламента (например, ``§4.2``/``§4.2/I2b``).
_SECTION_RE = re.compile(r"§[\d.]+(?:/[^\s:,;]+)?")


class CliError(Exception):
    """Ошибка CLI с кодом инварианта/правила и кодом возврата.

    Сообщение печатается как ``CODE: message`` в stderr (или в JSON при ``--json``).
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        exit_code: int = EXIT_ERROR,
    ) -> None:
        super().__init__(message)
        self.code = code or extract_code(message)
        self.exit_code = exit_code


class LintFailure(CliError):
    """Блокирующие нарушения линтера входов (``03`` §D.3/§E.2).

    ``blocking``/``warnings`` сохраняются раздельно: CLI печатает блокирующие в stderr
    (каждое сообщение уже несёт код правила), предупреждения — отдельной строкой.
    """

    def __init__(self, blocking: Sequence[str], warnings: Sequence[str] = ()) -> None:
        super().__init__(
            "блокирующие нарушения линтера", code="lint", exit_code=EXIT_USAGE
        )
        self.blocking = list(blocking)
        self.warnings = list(warnings)


# ---------------------------------------------------------------------------
# Разбор кодов и вывод
# ---------------------------------------------------------------------------


def extract_code(message: str) -> str:
    """Извлекает код инварианта/правила из сообщения (для ``CODE: message``).

    Порядок попыток: ведущий ``CODE:`` (сообщения линтера), затем инвариант вида
    ``I2b`` в тексте (сообщения домена), затем ссылка ``§4.2``. Если ничего не найдено —
    :data:`_FALLBACK_CODE`.
    """
    text = (message or "").strip()
    if not text:
        return _FALLBACK_CODE
    match = _CODE_RE.match(text)
    if match and len(match.group(1)) <= 32:
        return match.group(1)
    for pattern in (_INVARIANT_RE, _SECTION_RE):
        found = pattern.search(text)
        if found:
            return found.group(0)
    return _FALLBACK_CODE


def _concise(exc: BaseException) -> str:
    """Однострочное представление исключения (без простыней traceback)."""
    return " ".join(str(exc).split()) or type(exc).__name__


def _print_json(payload: dict[str, Any]) -> None:
    """Печатает JSON-объект в stdout (машинночитаемый вывод, ``--json``)."""
    sys.stdout.write(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    )


def _emit_lint_failure(args: argparse.Namespace, exc: LintFailure) -> None:
    """Печатает блокирующий линт: каждое сообщение уже несёт код правила."""
    if getattr(args, "json", False):
        _print_json(
            {
                "ok": False,
                "error": "lint",
                "blocking": exc.blocking,
                "warnings": exc.warnings,
            }
        )
        return
    for message in exc.blocking:
        sys.stderr.write(f"{message}\n")
    for message in exc.warnings:
        sys.stderr.write(f"{_FALLBACK_CODE}: предупреждение: {message}\n")


def _emit_error(args: argparse.Namespace, code: str, message: str) -> None:
    """Печатает ошибку как ``CODE: message`` (stderr) или JSON (stdout)."""
    if getattr(args, "json", False):
        _print_json({"ok": False, "error": code, "message": message})
        return
    sys.stderr.write(f"{code}: {message}\n")


# ---------------------------------------------------------------------------
# Команды
# ---------------------------------------------------------------------------


def cmd_resolve(args: argparse.Namespace) -> int:
    """``resolve``: детерминированный резолв ``source_id`` → URL (I10/I15).

    Резолв идёт по операционной БД прогона (``core.store.sqlite.SqliteStore``), без сети.
    Используются оба метода хранилища: :meth:`resolve` (метаданные: hash/build) и
    :meth:`resolve_source_url` (URL). Промах — не исключение, а код возврата
    :data:`EXIT_NOT_FOUND` с кодом ``I10/I15``.
    """
    resolved = None
    url = None
    with build_store(args.db, profile=args.profile) as store:
        resolved = store.resolve(args.source_id)
        url = store.resolve_source_url(args.source_id)

    if resolved is None or url is None:
        message = f"source_id не резолвится: {args.source_id}"
        if args.json:
            _print_json(
                {
                    "ok": False,
                    "found": False,
                    "source_id": args.source_id,
                    "error": "I10/I15",
                    "message": message,
                }
            )
        else:
            sys.stderr.write(f"I10/I15: {message}\n")
        return EXIT_NOT_FOUND

    payload = {
        "ok": True,
        "found": True,
        "source_id": args.source_id,
        "source_url": url,
        "content_hash": resolved.content_hash,
        "build_id": resolved.build_id,
        "chunk_id": resolved.chunk_id,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(f"{url}\n")
    return EXIT_OK


def cmd_plan(args: argparse.Namespace) -> int:
    """``plan``: ``load_inputs → lint → plan`` (``03`` §D.3/E.1).

    План чисто вычислим и **ничего не пишет**; флаг ``--dry-run`` фиксирует это явно
    (запись черновика ``run.yaml``/``cohort.yaml`` — будущий шаг W10/T10.4). Блокирующий
    линт прерывает построение плана с кодом правила.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    if not report.ok:
        raise LintFailure(report.blocking, report.warnings)

    result = build_plan(inputs, run_id=args.run_id)
    payload = _plan_payload(result)
    payload["dry_run"] = bool(args.dry_run)
    if args.json:
        _print_json({"ok": True, **payload})
    else:
        _print_plan(payload)
    return EXIT_OK


def cmd_run(args: argparse.Namespace) -> int:
    """``run``: собрать прогон и провести ``Runner`` по реальным фазам Ш1–Ш9.

    Composition root строит план и реестр фаз (:func:`build_phases`) и связывает их с
    портами профиля (:func:`_resolve_ports`). Гейты G1–G4 намеренно **не** автоматизируются
    (I6/I7): без строки в ``gate_decisions`` прогон останавливается ``blocked`` на ``g1``.
    Resume honour'ится: строка прогона **не пересоздаётся**, если она уже есть, поэтому
    durable-карта фаз в ``runs.state`` сохраняется между процессами (``02`` §A.1).

    ``--dry-run`` показывает план фаз/линт без записи; ``--reset`` форсирует полный
    перезапуск в обход durable-состояния.
    """
    inputs = load_inputs(args.run_yaml, args.package)
    report = lint_run_inputs(inputs)
    run_id = args.run_id or derive_run_id(inputs)
    db_path = Path(args.db) if args.db is not None else run_db_path(args.runs_dir, run_id)

    if args.dry_run:
        if not report.ok:
            raise LintFailure(report.blocking, report.warnings)
        payload = {
            "ok": True,
            "dry_run": True,
            "run_id": run_id,
            "db": str(db_path),
            "until": args.until,
            "phases": _phases_until(args.until),
            "warnings": list(report.warnings),
        }
        if args.json:
            _print_json(payload)
        else:
            _print_run_dry(payload)
        return EXIT_OK

    if not report.ok:
        raise LintFailure(report.blocking, report.warnings)

    # R7 (07 §7): сведения о профиле зон для манифеста прогона. ГЭП: CLI W5 ещё не
    # собирает run_manifest.yaml (это W10/T10.4), поэтому поля добавляются в отчёт
    # прогона (`deploy`) и готовы для edge_capabilities через manifest_deploy_fields.
    deploy_fields: dict[str, Any] = {}
    deploy_config: DeployConfig | None = None
    if args.profile == SPLIT_PROFILE:
        deploy_config = load_deploy_config(None)
        if not is_split(deploy_config):
            raise WiringError(
                "профиль split требует deploy-конфиг с profile split/split-hardened "
                "(07 §6/§7)"
            )
        deploy_fields = manifest_deploy_fields(deploy_config)

    # План строится ОДИН раз здесь и переиспользуется фазами: повторный ``plan()`` внутри
    # фазы дал бы иной ``run_id``/двойной линт (см. core/pipeline/phases.py). ``run_id``
    # переиспользуется тот же, что выведен выше, — второго derive нет.
    run_plan = _resolve_plan(args, inputs, run_id)
    ports = _resolve_ports(args, deploy_config)
    phases = _resolve_phases(args, inputs=inputs, plan=run_plan, ports=ports)
    # Наблюдаемость (W12/T12.1, 04 §B.1/§B.2): оборачиваем фазы логом+таймингом.
    # Гейты — дефолтные замыкания Runner; оборачиваем и их, чтобы §B.1-лог покрывал
    # точку остановки (g1 без человеческого решения тоже оставляет logs/g1.jsonl).
    clock = getattr(args, "clock", None)
    instrumentable: dict[str, PhaseFn] = dict(phases)
    for gate_name, gate in GATE_PHASES.items():
        instrumentable.setdefault(gate_name, _gate_phase(gate_name, gate))
    instrumentation = instrument_phases(
        instrumentable,
        run_id=run_id,
        logs_root=args.runs_dir,
        clock=clock,
    )

    instrumentation_errors: list[str] = []
    store = build_store(db_path, profile=args.profile)
    try:
        _ensure_run(store, run_id, inputs)
        # 03 §A.2: снимок входов прогона (иммутабельная копия run.yaml + plan_inputs.json)
        # записывается ДО исполнения фаз, чтобы последующие команды были самодостаточны.
        # Сбой снимка не роняет прогон: фиксируется в instrumentation_errors и не
        # подменяет код возврата.
        try:
            instrumentation_errors.extend(
                _snapshot_run_inputs(args, run_id, inputs=inputs, run_plan=run_plan)
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
        if args.reset:
            runner.reset(run_id)
        run_report = runner.run(run_id, until=args.until)
        # Метрики (04 §B.2) пишутся ДО закрытия store (источник ячеек) и независимо от
        # остановки на гейте — отражают реально отработавшие фазы. Сбой наблюдаемости
        # фиксируется, но не подменяет код возврата прогона.
        try:
            write_run_metrics(
                run_id=run_id,
                out_dir=args.runs_dir,
                durations=instrumentation.durations,
                store=store,
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость не роняет прогон
            instrumentation_errors.append(
                f"metrics: {type(exc).__name__}: {_concise(exc)}"
            )
    finally:
        store.close()

    payload = _run_report_payload(run_id, run_report, db_path)
    if instrumentation_errors:
        payload["instrumentation_errors"] = instrumentation_errors
    if deploy_fields:
        payload["deploy"] = deploy_fields
    if args.json:
        _print_json(payload)
    else:
        _print_run_report(payload)
    if run_report.stopped_at is None:
        return EXIT_OK
    if run_report.status == "blocked":
        return EXIT_BLOCKED
    return EXIT_ERROR


def cmd_deploy_check(args: argparse.Namespace) -> int:
    """``deploy-check``: диагностика DMZ-шва (T13.6; ``07`` §3/§7/§8).

    Загружает deploy-конфиг и для профилей ``split``/``split-hardened`` строит gateway
    по секции ``edge.*``, вызывает ``/v1/health`` и ``/v1/capabilities`` и проверяет
    конверт/полезную нагрузку против опубликованного контракта шва. Профиль ``local``
    шва не использует — команда сообщает об этом и успешна.

    Диагностика **инъектируема**: аргументы ``deploy_transport`` (шов
    ``(op, envelope) -> envelope``) или ``gateway_factory`` позволяют тестировать без
    сокета. Контракт валидируется через :mod:`core.adapters.gateway_validate` —
    слой-безопасное зеркало ``core/edge/seam_schema`` (``cli`` не импортирует ``edge``,
    матрица ``01`` §1); форма payload ``health``/``capabilities`` проверяется локально.

    :returns: :data:`EXIT_OK` при достижимом и валидном шве; :data:`EXIT_ERROR` иначе.
    """
    config = load_deploy_config(args.deploy)
    if not is_split(config):
        _emit_deploy_check(
            args,
            {
                "ok": True,
                "split": False,
                "profile": config.profile,
                "note": "профиль local: DMZ-шов не используется (07 §6); диагностика не требуется",
            },
        )
        return EXIT_OK

    transport = getattr(args, "deploy_transport", None)
    factory = getattr(args, "gateway_factory", None)
    gateway, raw_envelopes = _diagnostic_gateway(
        config, transport=transport, factory=factory
    )
    report = _seam_diagnostics(config, gateway, raw_envelopes)
    _emit_deploy_check(args, report)
    return EXIT_OK if report["ok"] else EXIT_ERROR


def cmd_diff(args: argparse.Namespace) -> int:
    """``diff``: межцикловый diff ``run_a`` → ``run_b`` (§9/§13).

    Резолв хранилищ (контракт выбора):

    * ``--runs-dir`` (дефолт ``runs``) + два позиционных ``run_id`` →
      ``runs/{run_id}/cohound.db`` через :func:`core.cli.wiring.run_db_path`;
    * единый явный ``--db`` — когда оба цикла лежат в **одной** БД; он переопределяет
      ``--runs-dir``.

    Оба цикла читаются как реальные prev/curr (без заглушки): ``read_manifest`` +
    ``iter_cells``. :func:`core.pipeline.report.render_diff` пишет ``diff.md``; при
    нарушении версионной границы §13 (major ``methodology_version`` либо расхождение
    cohort/profile+K/index_build/retrieval_params) :class:`CrossCycleDiffError` →
    :data:`EXIT_ERROR` с причиной ``§13: ...``.

    Дефолт ``--out`` — ``runs/{run_b}/outputs/diff.md`` (родительские каталоги создаются).
    """
    out_path = (
        Path(args.out)
        if args.out is not None
        else Path(args.runs_dir) / args.run_b / "outputs" / "diff.md"
    )
    prev_manifest, prev_cells = _read_cycle(args, args.run_a)
    curr_manifest, curr_cells = _read_cycle(args, args.run_b)
    try:
        written = render_diff(
            prev_manifest, curr_manifest, prev_cells, curr_cells, out_path
        )
    except CrossCycleDiffError as exc:
        message = _concise(exc)
        code = extract_code(message)
        prefix = f"{code}:"
        if message.startswith(prefix):
            message = message[len(prefix) :].strip()
        _emit_error(args, code, message)
        return EXIT_ERROR

    payload = {
        "ok": True,
        "out": str(written),
        "run_a": args.run_a,
        "run_b": args.run_b,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(f"{written}\n")
    return EXIT_OK


def cmd_sensitivity(args: argparse.Namespace) -> int:
    """``sensitivity``: пост-хок анализ устойчивости по осям (§6, K6).

    Хранилище резолвится одним из двух способов: явный ``--db`` (``run_id`` берётся из
    ``--run-id`` либо выводится, если в БД ровно один прогон) **или**
    ``--runs-dir`` + ``--run-id`` → ``runs/{run_id}/cohound.db``.

    Бандл пересобирается :func:`core.pipeline.compute.compute` из ячеек прогона. Источник
    каталога/весов/когорты (приоритет): явные ``--run-yaml`` + ``--package``, иначе снимок
    прогона ``runs/{run_id}/plan_inputs.json`` (``cmd_run`` пишет его — прогон
    самодостаточен, ``03`` §A.2). Лишь при отсутствии обоих источников расчёт идёт с
    **пустым** каталогом (вес ячейки = 1.0) и без когорты, а ограничение явно печатается
    в поле ``notes`` (значения не выдумываются).

    Печатает оси ``per_axis``, флаг ``stable``, ``unstable_pairs`` и
    ``requested_ranking``/``computed_ranking`` (человеку или JSON).
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    bundle = None
    notes: list[str] = []
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        catalog, module_weights, cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="sensitivity", run_dir=Path(db_path).parent
        )
        bundle = compute(
            store=store,
            run_id=run_id,
            catalog=catalog,
            module_weights=module_weights,
            cohort=cohort,
        )

    assert bundle is not None  # compute выполняется безусловно (см. выше)
    report = bundle.sensitivity
    payload = {
        "ok": True,
        "run_id": run_id,
        "stable": report.stable,
        "per_axis": report.per_axis,
        "unstable_pairs": [list(pair) for pair in report.unstable_pairs],
        "requested_ranking": report.requested_ranking,
        "computed_ranking": report.computed_ranking,
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        _print_sensitivity(payload)
    return EXIT_OK


def cmd_golden(args: argparse.Namespace) -> int:
    """``golden``: сверка вердиктов прогона с эталонным ``golden.csv`` (R6/R12, §11.3).

    Хранилище резолвится как в ``sensitivity``: явный ``--db`` (``run_id`` из
    ``--run-id`` либо выводится, если в БД ровно один прогон) **или** ``--runs-dir`` +
    ``--run-id`` → ``runs/{run_id}/cohound.db``.

    Каталог (для классификации критических параметров, R12) берётся из явных
    ``--run-yaml`` + ``--package`` при их наличии, иначе из снимка прогона
    ``runs/{run_id}/plan_inputs.json``. Лишь если оба источника недоступны, ``evaluate_golden``
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
            args, run_id, purpose="golden", run_dir=Path(db_path).parent
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


def cmd_report(args: argparse.Namespace) -> int:
    """``report``: пересборка артефактов прогона из БД (Ш9, §9, ``03`` §D.6).

    Хранилище резолвится как в ``sensitivity`` (``--db`` либо ``--runs-dir`` +
    ``--run-id``). Бандл пересобирается :func:`core.pipeline.compute.compute`, ячейки
    читаются :meth:`~core.pipeline.runner.StorePort.iter_cells`, манифест —
    ``read_manifest``. Артефакты пишутся :func:`core.pipeline.report.write_reports` в
    ``{out}/{run_id}/outputs/`` (``--out``; дефолт — ``--runs-dir`` либо ``runs``).

    Каталог/веса/когорта восстанавливаются из явных ``--run-yaml`` + ``--package`` либо из
    снимка прогона ``runs/{run_id}/plan_inputs.json``; лишь без обоих источников расчёт идёт
    с пустым каталогом и явной заметкой о ГЭП.
    Если ``read_manifest`` вернул незаполненный манифест (финальная фаза ``report`` не
    завершалась), это также сообщается в ``notes`` — артефакты строятся из текущего
    состояния ячеек, а не выдумываются версии.

    :returns: :data:`EXIT_OK` при записанных артефактах.
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    notes: list[str] = []
    manifest = None
    bundle = None
    catalog: dict[str, Any] = {}
    cells: list[Any] = []
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        manifest = store.read_manifest(run_id)
        catalog, module_weights, cohort, notes = _restore_plan_inputs(
            args, run_id, purpose="report", run_dir=Path(db_path).parent
        )
        bundle = compute(
            store=store,
            run_id=run_id,
            catalog=catalog,
            module_weights=module_weights,
            cohort=cohort,
        )
        cells = list(store.iter_cells(run_id))

    assert run_id is not None and bundle is not None and manifest is not None
    out_dir = args.out if args.out is not None else (args.runs_dir or "runs")
    artifacts = write_reports(
        bundle=bundle,
        cells=cells,
        catalog=catalog,
        manifest=manifest,
        out_dir=out_dir,
        run_id=run_id,
    )
    partial = _manifest_partial(manifest)
    if partial:
        notes.append(
            "манифест не заполнен (фаза report не завершалась): версии — заглушки; "
            "артефакты построены из текущего состояния ячеек"
        )
    payload = {
        "ok": True,
        "run_id": run_id,
        "out": str(Path(out_dir) / run_id / "outputs"),
        "manifest_partial": partial,
        "artifacts": _artifacts_payload(artifacts),
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        for name, path in payload["artifacts"].items():
            sys.stdout.write(f"{name}: {path}\n")
        for note in notes:
            sys.stdout.write(f"note: {note}\n")
    return EXIT_OK


def cmd_discover(args: argparse.Namespace) -> int:
    """``discover``: внешний поиск как источник **кандидатов-гипотез** (Ш2/Ш5, I19/G2).

    Собирает поисковый адаптер (по умолчанию ``yandex``; тестовый шов —
    ``search_adapter`` аргумент :func:`main`) и гоняет :func:`core.pipeline.discover`
    по запросам ``--query`` (можно повторять). Выход — только
    :class:`~core.pipeline.discovery.DiscoveryCandidate` (URL+сниппет): **evidence не
    пишется**, ни одна гипотеза не одобряется автоматически (I19/G2, A3). Продвижение
    гипотезы — отдельное человеческое решение G2.

    **Недоступный адаптер** (нет ``httpx`` или учётных данных при сборке реального
    адаптера) → :data:`EXIT_ERROR` с ясным сообщением; кандидаты **не выдумываются**.
    Неизвестный ``--search-adapter`` отвергается ``argparse`` (:data:`EXIT_USAGE`).

    ``--passports-dir`` **засеивает** дополнительные запросы из ``doc_domains`` паспортов
    (через :func:`core.config.passport_urls`) — это **лишь подсказки-кандидаты**, домены
    не фетчатся (I19). Хотя бы один запрос обязателен (``--query`` либо seed).

    :returns: :data:`EXIT_OK` (список кандидатов может быть пуст); :data:`EXIT_ERROR`
        при недоступном адаптере; :data:`EXIT_USAGE` при пустом наборе запросов.
    """
    injected = getattr(args, "search_adapter", None)

    queries: list[str] = list(args.query or [])
    seeds = _discover_seed_queries(args.passports_dir)
    for seed in seeds:
        if seed not in queries:
            queries.append(seed)
    if not queries:
        raise CliError(
            "нужен хотя бы один --query либо --passports-dir с doc_domains",
            code="usage",
            exit_code=EXIT_USAGE,
        )

    adapter: SearchAdapterPort
    if injected is not None:
        adapter = injected
    else:
        adapter = build_search_adapter(args.search_adapter_kind)
        if not _search_adapter_configured(adapter):
            _emit_error(
                args,
                "I19",
                "поисковый адаптер недоступен: нет httpx либо учётных данных "
                "(yandex: YANDEX_SEARCH_API_KEY/YANDEX_FOLDER_ID; "
                "glm: GLM_SEARCH_API_KEY); кандидаты не выдумываются",
            )
            return EXIT_ERROR
    if not getattr(adapter, "available", True):
        _emit_error(
            args,
            "I19",
            "поисковый адаптер недоступен (available=false): "
            "кандидаты не выдумываются",
        )
        return EXIT_ERROR

    report = discover(
        queries=queries, adapter=adapter, limit=args.limit, lang=args.lang
    )
    payload: dict[str, Any] = json.loads(report.to_manifest_json())
    payload["ok"] = True
    payload["search_adapter"] = getattr(adapter, "adapter_kind", "serp")
    payload["seeded_queries"] = seeds
    payload["note"] = (
        "кандидаты — гипотезы, не evidence (I19); продвижение возможно только "
        "человеческим решением G2"
    )
    if args.json:
        _print_json(payload)
    else:
        _print_discover(payload)
    return EXIT_OK


def cmd_passport_scout(args: argparse.Namespace) -> int:
    """``passport-scout``: авто-генерация **черновика** паспорта продукта (M1).

    Собирает страницы заданного ``--url`` (и, при переданном поиске, URL выдачи),
    зовёт LLM-разведку в схему гипотезы и строит консервативный черновик
    (:func:`core.pipeline.scout_passport`). Сателлиты — только ``draft``/``not_found``;
    ``relationship_evidence``/``upstream`` — лишь при подтверждённом URL и дословной
    цитате (I10/I15, precision-first). ``--dry-run`` не пишет. При ошибках сбора/LLM
    (``report.ok=False``) черновик **не публикуется**, код возврата :data:`EXIT_ERROR`.

    Без LLM-конфигурации (пустые ``base_url``/``model_id``) — понятная ошибка ``llm``
    (:data:`EXIT_USAGE`); кандидаты не выдумываются. Тестовые швы ``discovery_llm``/
    ``crawler``/``extractor`` инъектируются в :func:`main` (без сети).

    :returns: :data:`EXIT_OK` — черновик построен; :data:`EXIT_ERROR` — ошибки;
        :data:`EXIT_USAGE` — LLM не сконфигурирован/битый ``--package``.
    """
    injected_llm = getattr(args, "discovery_llm", None)
    if injected_llm is not None:
        discovery_llm: DiscoveryLLMPort = injected_llm
    else:
        try:
            discovery_llm = build_discovery_llm(
                base_url=args.llm_base_url,
                model_id=args.llm_model,
                decoder=args.llm_decoder,
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

    # Порты профиля: для ``local`` crawler не инстанцируется (нет URL-конфига) —
    # берём локальный ScrapyCrawler; для ``split`` используется remote-прокси из Ports.
    profile_ports = build_adapters(
        profile=args.profile, package=getattr(args, "package", None)
    )
    crawler: CrawlerPort = (
        getattr(args, "crawler", None)
        or getattr(profile_ports, "crawler", None)
        or build_scout_crawler()
    )
    extractor: Any = getattr(args, "extractor", None) or profile_ports.extractor

    roles = _load_package_roles(getattr(args, "package", None))
    report = scout_passport(
        product_name=args.product_name,
        url=args.url,
        crawler=crawler,
        extractor=extractor,
        discovery_llm=discovery_llm,
        search=getattr(args, "search_adapter", None),
        roles=roles,
    )

    written: dict[str, str] = {}
    if not args.dry_run and report.ok:
        yaml_path, sidecar_path = write_passport_draft(
            report, args.passports_dir, report.passport.snapshot_date
        )
        written = {
            "passport_path": str(yaml_path),
            "candidates_path": str(sidecar_path),
        }

    payload: dict[str, Any] = {
        "ok": report.ok,
        "product_id": report.passport.product_id,
        "canonical_name": report.passport.canonical_name,
        "vendor": report.passport.vendor,
        "jurisdiction": report.passport.jurisdiction,
        "satellites": len(report.passport.satellites),
        "doc_domains": report.passport.doc_domains,
        "needs_human": report.needs_human,
        "errors": report.errors,
        "fetched": report.fetched,
        "dry_run": bool(args.dry_run),
        **written,
    }
    if args.json:
        _print_json(payload)
    else:
        _print_passport_scout(payload)
    return EXIT_OK if report.ok else EXIT_ERROR


def cmd_review(args: argparse.Namespace) -> int:
    """``review``: очередь ревью — preliminary-контур + ``❓``-кандидаты (Ш7/G3, ``03`` §D.6).

    Резолвит хранилище как ``sensitivity`` (``--db`` приоритетнее ``--runs-dir`` +
    ``--run-id``; ``run_id`` выводится, если в БД ровно один прогон) и печатает
    :func:`core.pipeline.review_queue` — детерминированный (по ``product_id``/``param_id``)
    список ячеек, требующих человеческого внимания. Пустая очередь — валидный результат
    :data:`EXIT_OK` с явной пометкой «queue empty».
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    items: list[ReviewItem] = []
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        items = review_queue(store, run_id)

    payload = {
        "ok": True,
        "run_id": run_id,
        "count": len(items),
        "items": [_review_item_payload(item) for item in items],
    }
    if args.json:
        _print_json(payload)
    else:
        _print_review(payload)
    return EXIT_OK


def cmd_promote(args: argparse.Namespace) -> int:
    """``promote``: человеческое повышение ``preliminary`` → ``evaluated`` (I6/I7/I14).

    Резолвит хранилище как ``review``, берёт :func:`core.pipeline.review_queue` и
    повышает :func:`core.pipeline.promote_batch`:

    * ``--param`` задан — повышаются только пары ``(product_id, param_id)`` очереди с
      указанными ``param_id``;
    * иначе — **все** элементы очереди.

    ``--actor`` обязателен и непуст (I6/I7). Вердикт переаттестации — явный ``--status``;
    **без** ``--status`` он берётся из ``cell.status``. При ``--validated-only`` (дефолт)
    отсутствие вердикта у ``preliminary``-ячейки → отказ ``I14`` (вердикт **не
    выдумывается**); ``--no-validated-only`` допускает ``evaluated`` без вердикта (первый
    шаг цикла §1.4). Отказы попадают в ``skipped`` с причиной; если не повышено **ни
    одной** ячейки — :data:`EXIT_ERROR`.

    :returns: :data:`EXIT_OK`, если повышена хотя бы одна ячейка; иначе :data:`EXIT_ERROR`.
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    target_status = Status(args.status) if args.status else None
    wanted_params = set(args.param or [])
    result: PromotionResult | None = None
    target_count = 0
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        queue = review_queue(store, run_id)
        if wanted_params:
            targets = [
                (item.product_id, item.param_id)
                for item in queue
                if item.param_id in wanted_params
            ]
        else:
            targets = [(item.product_id, item.param_id) for item in queue]
        target_count = len(targets)
        statuses = (
            {pair: target_status for pair in targets}
            if target_status is not None
            else {}
        )
        try:
            result = promote_batch(
                store=store,
                run_id=run_id,
                items=targets,
                actor=args.actor,
                notes=args.notes,
                validated_only=args.validated_only,
                statuses=statuses,
            )
        except ReviewError as exc:
            _emit_error(args, exc.code or "I14", _concise(exc))
            return EXIT_ERROR

    assert result is not None  # promote_batch выполняется безусловно (см. выше)
    payload: dict[str, Any] = {
        "ok": bool(result.promoted),
        "run_id": run_id,
        "actor": args.actor,
        "status": target_status.value if target_status is not None else None,
        "validated_only": args.validated_only,
        "requested": target_count,
        "promoted": len(result.promoted),
        "promoted_cells": [
            {
                "product_id": cell.product_id,
                "param_id": cell.param_id,
                "status": cell.status.value if cell.status is not None else None,
            }
            for cell in result.promoted
        ],
        "skipped": [
            {"product_id": product_id, "param_id": param_id, "reason": reason}
            for product_id, param_id, reason in result.skipped
        ],
        "errors": list(result.errors),
    }
    if args.json:
        _print_json(payload)
    else:
        _print_promote(payload)
    return EXIT_OK if result.promoted else EXIT_ERROR


# ---------------------------------------------------------------------------
# Помощники команд
# ---------------------------------------------------------------------------


def _read_cycle(args: argparse.Namespace, run_id: str) -> tuple[Any, list[Any]]:
    """Читает цикл прогона: ``(манифест, ячейки)`` для ``diff``.

    ``--db`` (общая БД) приоритетнее ``--runs-dir``; иначе БД резолвится
    :func:`core.cli.wiring.run_db_path`.
    """
    db_path = args.db if args.db is not None else run_db_path(args.runs_dir, run_id)
    manifest: Any = None
    cells: list[Any] = []
    with build_store(db_path, profile=args.profile) as store:
        manifest = store.read_manifest(run_id)
        cells = list(store.iter_cells(run_id))
    return manifest, cells


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


def _artifacts_payload(artifacts: Any) -> dict[str, str | None]:
    """JSON-совместимое представление :class:`~core.pipeline.report.Artifacts`."""

    def _path(value: Any) -> str | None:
        return str(value) if value is not None else None

    return {
        "battle_card": _path(artifacts.battle_card),
        "preliminary_findings": _path(artifacts.preliminary_findings),
        "nda_questions": _path(artifacts.nda_questions),
        "diff": _path(artifacts.diff),
        "manifest": _path(artifacts.manifest),
    }


def _llm_configured(adapter: Any) -> bool:
    """Сконфигурирован ли LLM-адаптер (готовность и непустые ``base_url``/``model_id``)."""
    return bool(getattr(adapter, "available", False)) and bool(
        getattr(adapter, "model_id", "") or getattr(adapter, "base_url", "")
    )


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


def _search_adapter_configured(adapter: Any) -> bool:
    """Готов ли реальный поисковый адаптер к вызовам (есть зависимость и учётные данные).

    Проверяет ``available`` и наличие ``api_key`` (``YANDEX_SEARCH_API_KEY``): без ключа
    реальный SERP-вызов невозможен, и кандидаты не выдумываются (I19). Для инъектированного
    тестового шва эта проверка не применяется (см. :func:`cmd_discover`).
    """
    return bool(getattr(adapter, "available", True)) and bool(
        getattr(adapter, "api_key", None)
    )


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
    run_dir: str | Path | None = None,
) -> tuple[dict[str, Any], dict[str, float], Any, list[str]]:
    """Восстанавливает ``(catalog, module_weights, cohort, notes)`` для пост-хок команд.

    Приоритет источников:

    (a) **явные** ``--run-yaml`` + ``--package`` — высший приоритет (поведение сохранено);
    (b) **снимок прогона** ``runs/{run_id}/plan_inputs.json`` (``03`` §A.2), если каталог
        пакета больше не нужен — прогон самодостаточен. Заметка сообщает, что значения
        восстановлены из снимка (ГЭП отсутствует);
    (c) иначе — **пустые** значения и явная ГЭП-заметка: каталог/веса/когорта **не
        выдумываются** (вес ячейки = 1.0, когорта ``None``).

    :param run_dir: каталог прогона (``runs/{run_id}``); ``None`` → выводится из
        ``--runs-dir``/``--db`` (:func:`_resolve_run_dir`). Команды передают родителя
        резолвнутой БД, что покрывает оба контракта выбора хранилища.
    :param purpose: имя команды в тексте ГЭП-заметки (``sensitivity``/``golden``/``report``).
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
            "один — предпринята попытка восстановления из plan_inputs.json прогона"
        )

    resolved_dir = run_dir if run_dir is not None else _resolve_run_dir(args, run_id)
    restored = _load_plan_inputs_snapshot(resolved_dir, run_id)
    if restored is not None:
        catalog, module_weights, cohort = restored
        notes.append("каталог/веса/когорта восстановлены из plan_inputs.json прогона")
        return catalog, module_weights, cohort, notes

    notes.append(
        "ГЭП: каталог/веса/когорта не персистятся в БД прогона — "
        f"{purpose} рассчитан с пустым каталогом (вес ячейки = 1.0) и без когорты; "
        "передайте --run-yaml и --package для точного воспроизведения"
    )
    return {}, {}, None, notes


def _resolve_run_dir(args: argparse.Namespace, run_id: str) -> Path | None:
    """Выводит каталог прогона ``runs/{run_id}`` из аргументов команды.

    Приоритет: явный ``--runs-dir`` (``{runs_dir}/{run_id}``), иначе родитель явного
    ``--db`` (для канонической раскладки ``--db runs/{run_id}/cohound.db`` это тот же
    каталог). ``--db :memory:`` и отсутствие обоих дают ``None`` — снимок не ищется,
    команда честно уходит в ГЭП-ветку.
    """
    runs_dir = getattr(args, "runs_dir", None)
    if runs_dir:
        return Path(runs_dir) / run_id
    db = getattr(args, "db", None)
    if db is not None and str(db) != ":memory:":
        return Path(db).parent
    return None


def _load_plan_inputs_snapshot(
    run_dir: str | Path | None, run_id: str
) -> tuple[dict[str, Any], dict[str, float], Any] | None:
    """Читает и валидирует ``runs/{run_id}/plan_inputs.json`` (снимок входов плана).

    Возвращает ``(catalog, module_weights, cohort)`` либо ``None`` при отсутствии/
    частичности/несовместимой версии снимка или ``run_id``-несоответствии. Несовпадение
    ``run_id`` трактуется как чужой снимок: значения не подставляются (не выдумываем).
    """
    if run_dir is None:
        return None
    path = Path(run_dir) / PLAN_INPUTS_FILENAME
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
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


def _snapshot_run_inputs(
    args: argparse.Namespace,
    run_id: str,
    *,
    inputs: RunInputs,
    run_plan: RunPlan,
) -> list[str]:
    """Пишет снимок входов прогона в ``runs/{run_id}/`` (``03`` §A.2).

    Состав:

    * ``run.yaml`` — **иммутабельная копия** входа (байт-в-байт); существующий
      отличающийся файл **не** перезаписывается (иначе мутировал бы завершённый
      прогон), идентичный — оставляется как есть;
    * ``plan_inputs.json`` — детерминированный JSON (``sort_keys=True``,
      ``ensure_ascii=False``) с тем, что нужно для пересборки каталога/весов/когорты
      без исходного пакета (см. :func:`_plan_inputs_payload`).

    Никогда не бросает исключений: любая ошибка записи возвращается строкой для
    ``instrumentation_errors`` прогона (сбой снимка не подменяет код возврата).
    """
    run_dir = Path(args.runs_dir) / run_id
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return [f"snapshot: не удалось создать каталог прогона {run_dir}: {_concise(exc)}"]

    issues = _snapshot_run_yaml(args.run_yaml, run_dir)
    issues.extend(_snapshot_plan_inputs(run_dir, run_id, inputs=inputs, run_plan=run_plan))
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


def _snapshot_plan_inputs(
    run_dir: Path, run_id: str, *, inputs: RunInputs, run_plan: RunPlan
) -> list[str]:
    """Пишет ``plan_inputs.json`` — снимок плановых входов для пост-хок команд."""
    target = run_dir / PLAN_INPUTS_FILENAME
    payload = _plan_inputs_payload(run_id, inputs=inputs, run_plan=run_plan)
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
    ``params``/``cohort`` достаточно для :func:`_load_plan_inputs_snapshot`; прочие —
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
        "issues": issues,
    }


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


def _resolve_ports(args: argparse.Namespace, deploy_config: DeployConfig | None) -> Ports:
    """Собирает порты профиля (``ports`` — тестовый шов; иначе :func:`build_adapters`).

    Для ``split`` используются инъектируемые швы (``deploy_transport``/``gateway_factory``),
    поэтому прогон в тестах не требует сокета.
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
    return build_adapters(
        profile=args.profile,
        deploy=deploy_config,
        transport=getattr(args, "deploy_transport", None),
        gateway=gateway,
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


def _resolve_phases(
    args: argparse.Namespace, *, inputs: RunInputs, plan: RunPlan, ports: Ports
) -> dict[str, PhaseFn]:
    """Собирает реестр не-гейтовых фаз (``phases_factory`` — тестовый шов).

    **Паспорта** загружаются из ``--passports-dir`` (дефолт ``passports``) и передаются
    фазе Ш4 (наследование по ``upstream``). **URL корпуса** — только явный операторский
    вход ``--urls`` (JSON/YAML ``{product_id: [url, ...]}``); специфицированного артефакта
    URL в регламенте **нет**, поэтому ``doc_domains`` паспортов молча **не** фетчатся:
    homepage — не корпус документации (I19: контент становится evidence лишь после
    ``corpus_raw`` → hash → index). Без ``--urls`` фазы Ш5 получает ``{}`` и легитимно
    работает как no-op. ``passport_urls`` — лишь возможный seed-источник для будущего
    Discovery/оператора, но здесь он не подставляется неявно.

    Гейты ``g1``–``g4`` сюда не входят — их (де)фолтные замыкания остаются в
    :class:`Runner` (I6/I7, S9), поэтому ``run`` честно останавливается ``blocked`` без
    человеческого решения.
    """
    kwargs: dict[str, Any] = {
        "inputs": inputs,
        "plan": plan,
        "ports": ports,
        # URLs: только явный операторский вход; doc_domains не фетчатся молча (I19).
        "urls": _load_corpus_urls(getattr(args, "urls", None)),
        # Паспорта: --passports-dir (дефолт passports); отсутствие каталога → {}.
        "passports": _load_passports_dir(
            getattr(args, "passports_dir", "passports")
        ),
        "out_dir": args.runs_dir,
        "retrieval_profile": getattr(args, "retrieval_profile", None),
        "cutoff_k": getattr(args, "cutoff_k", None),
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
    """Сериализует :class:`RunReport` в JSON-совместимый словарь."""
    return {
        "ok": report.ok,
        "run_id": run_id,
        "status": report.status,
        "stopped_at": report.stopped_at,
        "db": str(db_path),
        "skipped": list(report.skipped),
        "errors": list(report.errors),
        "results": [
            {
                "phase": item.phase,
                "status": item.status,
                "errors": list(item.errors),
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
        for error in item["errors"]:
            lines.append(f"  error: {error}")
    if payload["skipped"]:
        lines.append(f"skipped: {', '.join(payload['skipped'])}")
    if payload["stopped_at"] is not None:
        lines.append(
            "примечание: гейт требует явного человеческого решения (03 §E.2); "
            "продолжите после записи решения в gate_decisions"
        )
    sys.stdout.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Парсер и точка входа
# ---------------------------------------------------------------------------


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


def build_parser() -> argparse.ArgumentParser:
    """Собирает argparse-парсер CLI (stdlib, без фреймворков, A1)."""
    parser = argparse.ArgumentParser(
        prog="hound",
        description=(
            "coHound — сравнительная техническая карта продуктов по публично "
            "доказуемой функциональности. Composition root Z0 (ручной DI, A1)."
        ),
    )
    sub = parser.add_subparsers(dest="command", metavar="КОМАНДА")

    # resolve ------------------------------------------------------------- #
    p_resolve = sub.add_parser(
        "resolve", help="резолв source_id → URL по БД прогона (I10/I15)"
    )
    p_resolve.add_argument("source_id", help="идентификатор источника (chunk_id/source_id)")
    p_resolve.add_argument(
        "--db", required=True, help="путь к БД прогона (runs/{run_id}/cohound.db)"
    )
    _add_common(p_resolve)
    p_resolve.set_defaults(handler=cmd_resolve)

    # plan ---------------------------------------------------------------- #
    p_plan = sub.add_parser(
        "plan", help="нормализация входов + линт + план прогона (без записи)"
    )
    p_plan.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    p_plan.add_argument("--package", required=True, help="каталог пакета категории")
    p_plan.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    p_plan.add_argument(
        "--dry-run",
        action="store_true",
        help="линт + план без записи (состояние W5: запись черновика — W10/T10.4)",
    )
    _add_common(p_plan)
    p_plan.set_defaults(handler=cmd_plan)

    # run ----------------------------------------------------------------- #
    p_run = sub.add_parser(
        "run", help="минимальный прогон: создать прогон и провести Runner по фазам"
    )
    p_run.add_argument("--run-yaml", required=True, help="путь к run.yaml (§4.5)")
    p_run.add_argument("--package", required=True, help="каталог пакета категории")
    p_run.add_argument("--run-id", default=None, help="переопределить детерминированный run_id")
    p_run.add_argument(
        "--runs-dir", default="runs", help="каталог прогонов (runs/{run_id}/cohound.db)"
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
    p_run.add_argument(
        "--passports-dir",
        default="passports",
        help="каталог паспортов passports/{product_id}/{YYYY-MM-DD}.yaml (дефолт passports; "
        "отсутствие каталога → без паспортов)",
    )
    p_run.add_argument(
        "--urls",
        default=None,
        help="опциональный JSON/YAML {product_id: [url, ...]} — операторский корпусный вход "
        "Ш5; без него корпус URL пуст (doc_domains паспортов не фетчатся молча)",
    )
    _add_common(p_run)
    p_run.set_defaults(handler=cmd_run)

    # diff ---------------------------------------------------------------- #
    p_diff = sub.add_parser(
        "diff",
        help="межцикловый diff run_a → run_b (запрещён через major/границу, §13)",
    )
    p_diff.add_argument("run_a", help="предыдущий прогон (run_id)")
    p_diff.add_argument("run_b", help="текущий прогон (run_id)")
    p_diff.add_argument(
        "--runs-dir",
        default="runs",
        help="каталог прогонов: runs/{run_id}/cohound.db (дефолт runs); --db переопределяет",
    )
    p_diff.add_argument(
        "--db",
        default=None,
        help="один общий cohound.db для обоих прогонов (оба run_id в одной БД)",
    )
    p_diff.add_argument(
        "--out",
        default=None,
        help="путь diff.md (дефолт runs/{run_b}/outputs/diff.md)",
    )
    _add_common(p_diff)
    p_diff.set_defaults(handler=cmd_diff)

    # sensitivity --------------------------------------------------------- #
    p_sens = sub.add_parser(
        "sensitivity", help="пост-хок sensitivity по осям (§6, K6)"
    )
    sens_source = p_sens.add_mutually_exclusive_group(required=True)
    sens_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    sens_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_sens.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_sens.add_argument(
        "--run-yaml",
        default=None,
        help="run.yaml для восстановления каталога/весов/когорты (вместе с --package)",
    )
    p_sens.add_argument(
        "--package",
        default=None,
        help="каталог пакета для восстановления каталога/весов/когорты (вместе с --run-yaml)",
    )
    _add_common(p_sens)
    p_sens.set_defaults(handler=cmd_sensitivity)

    # golden -------------------------------------------------------------- #
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
    p_golden.set_defaults(handler=cmd_golden)

    # report -------------------------------------------------------------- #
    p_report = sub.add_parser(
        "report", help="пересборка артефактов прогона из БД (Ш9, §9)"
    )
    report_source = p_report.add_mutually_exclusive_group(required=True)
    report_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    report_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_report.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_report.add_argument(
        "--run-yaml",
        default=None,
        help="run.yaml для восстановления каталога/весов/когорты (вместе с --package)",
    )
    p_report.add_argument(
        "--package",
        default=None,
        help="каталог пакета для восстановления каталога/весов/когорты (вместе с --run-yaml)",
    )
    p_report.add_argument(
        "--out",
        default=None,
        help="базовый каталог артефактов (дефолт --runs-dir либо runs); пишет {out}/{run_id}/outputs/",
    )
    _add_common(p_report)
    p_report.set_defaults(handler=cmd_report)

    # discover ------------------------------------------------------------ #
    p_discover = sub.add_parser(
        "discover",
        help="разведка кандидатов внешним поиском (I19/G2: только гипотезы, без evidence)",
    )
    p_discover.add_argument(
        "--query",
        action="append",
        default=None,
        metavar="Q",
        help="поисковый запрос (можно повторять); выход — гипотезы-кандидаты, не evidence (I19)",
    )
    p_discover.add_argument(
        "--lang",
        choices=["ru", "en"],
        default="ru",
        help="язык строковых запросов (дефолт ru)",
    )
    p_discover.add_argument(
        "--limit",
        type=int,
        default=10,
        help="лимит результатов на запрос (дефолт 10)",
    )
    p_discover.add_argument(
        "--search-adapter",
        dest="search_adapter_kind",
        choices=list(SUPPORTED_SEARCH_ADAPTERS),
        default="yandex",
        help="внешний поисковый адаптер (дефолт yandex)",
    )
    p_discover.add_argument(
        "--passports-dir",
        default=None,
        help="каталог паспортов: doc_domains используется ТОЛЬКО как seed-подсказки "
        "запросов и не фетчится (I19)",
    )
    _add_common(p_discover)
    p_discover.set_defaults(handler=cmd_discover)

    # passport-scout ------------------------------------------------------ #
    p_scout = sub.add_parser(
        "passport-scout",
        help="авто-генерация черновика паспорта продукта (сателлиты draft, без auto-approve)",
    )
    p_scout.add_argument(
        "--product-name", required=True, help="человеческое имя продукта (→ product_id slug)"
    )
    p_scout.add_argument("--url", required=True, help="главный URL продукта (фетчится HTTP)")
    p_scout.add_argument(
        "--passports-dir",
        default="passports",
        help="каталог паспортов: пишет passports/{product_id}/{date}.yaml (дефолт passports)",
    )
    p_scout.add_argument(
        "--package",
        default=None,
        help="опциональный пакет категории: роли сателлитов из package.yaml::roles",
    )
    p_scout.add_argument(
        "--llm-base-url",
        default=None,
        help="base_url LLM-разведки (override COHOUND_LLM_BASE_URL)",
    )
    p_scout.add_argument(
        "--llm-model",
        default=None,
        help="model_id LLM-разведки (override COHOUND_LLM_MODEL)",
    )
    p_scout.add_argument(
        "--llm-decoder",
        choices=["none", "json_object", "json_schema"],
        default=None,
        help="механизм декодирования (override COHOUND_LLM_DECODER; дефолт none)",
    )
    p_scout.add_argument(
        "--dry-run", action="store_true", help="показать черновик без записи файлов"
    )
    _add_common(p_scout)
    p_scout.set_defaults(handler=cmd_passport_scout)

    # review -------------------------------------------------------------- #
    p_review = sub.add_parser(
        "review", help="очередь ревью: preliminary-контур + ❓-кандидаты (Ш7/G3)"
    )
    review_source = p_review.add_mutually_exclusive_group(required=True)
    review_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    review_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_review.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    _add_common(p_review)
    p_review.set_defaults(handler=cmd_review)

    # promote ------------------------------------------------------------- #
    p_promote = sub.add_parser(
        "promote",
        help="человеческое повышение preliminary → evaluated (I6/I7/I14)",
    )
    promote_source = p_promote.add_mutually_exclusive_group(required=True)
    promote_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    promote_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_promote.add_argument(
        "--run-id",
        default=None,
        help="run_id (обязателен с --runs-dir; с --db выводится, если прогон один)",
    )
    p_promote.add_argument(
        "--actor",
        required=True,
        help="человек, выполняющий промоушен (I6/I7; обязателен и непуст)",
    )
    p_promote.add_argument(
        "--param",
        action="append",
        default=None,
        metavar="PARAM_ID",
        help="ограничить повышение этими param_id (можно повторять; иначе — вся очередь)",
    )
    p_promote.add_argument(
        "--status",
        choices=list(_PROMOTE_STATUSES),
        default=None,
        help="явный вердикт переаттестации; без него берётся из ячейки (а при "
        "--validated-only отсутствие вердикта → отказ I14)",
    )
    p_promote.add_argument(
        "--validated-only",
        dest="validated_only",
        action="store_true",
        default=True,
        help="дефолт: без валидированного вердикта повышение запрещено (I14)",
    )
    p_promote.add_argument(
        "--no-validated-only",
        dest="validated_only",
        action="store_false",
        help="разрешить evaluated без вердикта (первый шаг цикла §1.4, set-status позднее)",
    )
    p_promote.add_argument(
        "--notes", default=None, help="заметка к решению (аудит-след)"
    )
    _add_common(p_promote)
    p_promote.set_defaults(handler=cmd_promote)

    # deploy-check -------------------------------------------------------- #
    p_deploy = sub.add_parser(
        "deploy-check",
        help="диагностика DMZ-шва: /v1/health + /v1/capabilities + схема (07 §3/§7/§8)",
    )
    p_deploy.add_argument(
        "--deploy",
        default=None,
        help="путь к deploy.yaml (по умолчанию config/deploy.yaml, 03 D.5)",
    )
    p_deploy.add_argument(
        "--json", action="store_true", help="машинночитаемый вывод (JSON) в stdout"
    )
    p_deploy.set_defaults(handler=cmd_deploy_check)

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    deploy_transport: TransportFn | None = None,
    gateway_factory: GatewayFactory | None = None,
    ports: Ports | None = None,
    phases_factory: PhasesFactory | None = None,
    plan_factory: PlanFactory | None = None,
    retrieval_profile: Any = None,
    cutoff_k: int | None = None,
    clock: Clock | None = None,
    search_adapter: SearchAdapterPort | None = None,
    discovery_llm: DiscoveryLLMPort | None = None,
    crawler: CrawlerPort | None = None,
    extractor: Any = None,
) -> int:
    """Точка входа CLI. Возвращает код выхода (``0`` — успех).

    ``argparse`` сам печатает ``--help`` и usage-ошибки; здесь его ``SystemExit``
    превращается в обычный код возврата, поэтому :func:`main` тестируем без процесса.

    Ошибки домена/линта печатаются как ``CODE: message`` (код инварианта/правила).
    Непредвиденные исключения не проглатываются traceback'ом: печатается сжатое
    сообщение и возвращается ненулевой код.

    :param deploy_transport: тестовый шов транспорта для ``deploy-check``/``split``
        (без сокета).
    :param gateway_factory: фабрика ``ContentGateway`` для ``deploy-check``/``split``.
    :param ports: инъекция готового :class:`Ports` для ``run`` (тесты без внешних систем).
    :param phases_factory: фабрика реестра фаз ``run`` (тестовый шов; по умолчанию —
        реальный :func:`core.pipeline.build_phases`).
    :param plan_factory: фабрика плана ``run`` (тестовый шов; по умолчанию — ``plan``).
    :param retrieval_profile: зафиксированный профиль R0/R1/R2 (I16) для фаз ``run``.
    :param cutoff_k: зафиксированный ``retrieval_cutoff_K`` (I20) для фаз ``run``.
    :param clock: часы прогона (детерминизм ``created_at`` манифеста, тесты).
    :param search_adapter: инъекция готового ``SearchAdapterPort`` для ``discover``
        (тесты без сети/учётных данных; провайдерская специфика — в адаптере).
    :param discovery_llm: инъекция ``DiscoveryLLMPort`` для ``passport-scout`` (тесты без сети).
    :param crawler: инъекция ``CrawlerPort`` для ``passport-scout`` (тесты без сети).
    :param extractor: инъекция ``ExtractorPort`` для ``passport-scout`` (тесты без сети).
    """
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return _system_exit_code(exc)

    # Инъекция швов команд (тесты без сети/внешних систем).
    # `deploy-check` использует диагностические швы; `run` — ports/phases/plan-фабрики;
    # `discover` — готовый поисковый адаптер (иначе собирается реальный `yandex`).
    args.deploy_transport = deploy_transport
    args.gateway_factory = gateway_factory
    args.ports = ports
    args.phases_factory = phases_factory
    args.plan_factory = plan_factory
    args.retrieval_profile = retrieval_profile
    args.cutoff_k = cutoff_k
    args.clock = clock
    args.search_adapter = search_adapter
    args.discovery_llm = discovery_llm
    args.crawler = crawler
    args.extractor = extractor

    if getattr(args, "command", None) is None:
        parser.print_help()
        return EXIT_USAGE

    try:
        return int(args.handler(args))
    except LintFailure as exc:
        _emit_lint_failure(args, exc)
        return exc.exit_code
    except CliError as exc:
        _emit_error(args, exc.code, _concise(exc))
        return exc.exit_code
    except WiringError as exc:
        # Профиль/сборка: это конфигурационная ошибка (split до W13).
        _emit_error(args, "profile", _concise(exc))
        return EXIT_USAGE
    except ValueError as exc:  # включает pydantic.ValidationError (I-инварианты домена)
        _emit_error(args, extract_code(str(exc)), _concise(exc))
        return EXIT_USAGE
    except OSError as exc:
        _emit_error(args, _FALLBACK_CODE, _concise(exc))
        return EXIT_ERROR
    except Exception as exc:  # noqa: BLE001 — консолидированный ненулевой выход
        _emit_error(args, _FALLBACK_CODE, f"{type(exc).__name__}: {exc}")
        return EXIT_ERROR


def _system_exit_code(exc: SystemExit) -> int:
    """Нормализует код ``SystemExit`` argparse к целому (``--help`` → 0)."""
    if exc.code is None:
        return EXIT_OK
    if isinstance(exc.code, int):
        return exc.code
    return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover — точка входа модуля
    raise SystemExit(main())
