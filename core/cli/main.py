"""coHound — CLI-ядро: точка входа и обратная совместимость (Z0, A1; ``03`` §E).

После рефакторинга P1-A модуль **тонкий**: тела команд вынесены в
:mod:`core.cli.commands.*`, парсер — в :mod:`core.cli.parser`, разделяемые DI-хелперы —
в :mod:`core.cli.context`. Здесь остаётся :func:`main` (единая точка входа, инъекция
тестовых швов, резолв раскладки, маппинг исключений в коды возврата) и **реэкспорт**
исторической поверхности ``core.cli.main`` (``build_parser``, ``cmd_*``, ``EXIT_*``,
``CliError``/``LintFailure``, приватные алиасы вывода, константы снимка прогона) — чтобы
не ломать многочисленные импорты тестов и внешних вызывающих.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from core.adapters.remote_gateway import TransportFn
from core.cli.commands.deploy import run as cmd_deploy_check
from core.cli.commands.diff import run as cmd_diff
from core.cli.commands.discover import run as cmd_discover
from core.cli.commands.golden import run as cmd_golden
from core.cli.commands.passport_scout import run as cmd_passport_scout
from core.cli.commands.plan import run as cmd_plan
from core.cli.commands.promote import run as cmd_promote
from core.cli.commands.report import run as cmd_report
from core.cli.commands.resolve import run as cmd_resolve
from core.cli.commands.review import run as cmd_review
from core.cli.commands.run import run as cmd_run
from core.cli.commands.sensitivity import run as cmd_sensitivity
from core.cli.context import *  # noqa: F401,F403 — реэкспорт исторической поверхности main
from core.cli.context import (
    PLAN_INPUTS_FILENAME,
    PLAN_INPUTS_SNAPSHOT_VERSION,
    RUN_YAML_FILENAME,
    GatewayFactory,
    PhasesFactory,
    PlanFactory,
    _FALLBACK_CODE,
    _PROMOTE_STATUSES,
    _concise,
    _emit_error,
    _emit_lint_failure,
    _print_json,
    _resolve_paths,
    _resolve_ports,
)
from core.cli.errors import (
    EXIT_BLOCKED,
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    CliError,
    LintFailure,
    extract_code,
)
from core.cli.parser import build_parser
from core.cli.wiring import (
    WiringError,
    _llm_configured,
    _search_adapter_configured,
)
from core.pipeline import Ports
from core.pipeline.runner import Clock
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMPort
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

#: Команды, чей ``--runs-dir`` — операционный дефолт раскладки (03 §A.1): при отсутствии
#: флага подставляется resolved ``paths.runs`` (composition root — единственная точка).
_RUNS_DIR_COMMANDS: frozenset[str] = frozenset({"run", "diff"})
#: Команды, чей ``--passports-dir`` — операционный дефолт раскладки: без флага —
#: resolved ``paths.passports``. ``discover`` сюда не входит: его ``--passports-dir`` —
#: opt-in seed-источник запросов (I19), а не каталог-дефолт.
_PASSPORTS_DIR_COMMANDS: frozenset[str] = frozenset({"run", "passport-scout"})

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
    # ``--cutoff-k`` (CLI) выставляется парсером; инъекция ``cutoff_k`` — тестовый шов и
    # переопределяет CLI только при явном значении (иначе сохраняем ввод оператора).
    if cutoff_k is not None:
        args.cutoff_k = cutoff_k
    elif not hasattr(args, "cutoff_k"):
        args.cutoff_k = None
    args.clock = clock
    args.search_adapter = search_adapter
    args.discovery_llm = discovery_llm
    args.crawler = crawler
    args.extractor = extractor

    if getattr(args, "command", None) is None:
        parser.print_help()
        return EXIT_USAGE

    # Composition root — единственная точка разрешения раскладки (03 §A.1): резолвим
    # эффективные каталоги (env/paths.yaml/дефолты) и подставляем их как дефолты
    # argparse-флагов, если оператор флаг не задал. Приоритет: CLI-флаг > env > файл >
    # дефолт. Без env/файла/флага значения совпадают с прежними литералами — обратная
    # совместимость байт-в-байт.
    paths = _resolve_paths(args)
    args.paths = paths
    # ``hound package … --package`` принимает путь ИЛИ case_id: нормализуем один раз в
    # composition root, чтобы все подкоманды видели разрешённый путь (ленивый импорт —
    # как принято для package).
    if args.command == "package" and hasattr(args, "package"):
        from core.cli.package.commands import _resolve_package_dir

        args.package = _resolve_package_dir(args)
    if args.command in _RUNS_DIR_COMMANDS and getattr(args, "runs_dir", None) is None:
        args.runs_dir = str(paths.runs)
    if (
        args.command in _PASSPORTS_DIR_COMMANDS
        and getattr(args, "passports_dir", None) is None
    ):
        args.passports_dir = str(paths.passports)

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
