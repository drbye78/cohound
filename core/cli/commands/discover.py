"""``discover``: внешний поиск как источник **кандидатов-гипотез** (Ш2/Ш5, I19/G2).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from core.cli.context import (
    _add_common,
    _concise,
    _discover_seed_queries,
    _emit_error,
    _print_discover,
    _print_json,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, EXIT_USAGE, CliError
from core.cli.wiring import (
    SUPPORTED_SEARCH_ADAPTERS,
    SearchResolutionError,
    resolve_and_build_search_adapter,
)
from core.pipeline import discover


def run(args: argparse.Namespace) -> int:
    """``discover``: внешний поиск как источник **кандидатов-гипотез** (Ш2/Ш5, I19/G2).

    Собирает поисковый адаптер и гоняет :func:`core.pipeline.discover` по запросам
    ``--query`` (можно повторять). Выход — только
    :class:`~core.pipeline.discovery.DiscoveryCandidate` (URL+сниппет): **evidence не
    пишется**, ни одна гипотеза не одобряется автоматически (I19/G2, A3). Продвижение
    гипотезы — отдельное человеческое решение G2.

    **Выбор провайдера (I16/I21/I23).** Провайдер резолвится детерминированно
    :func:`core.config.deploy.resolve_search_provider`: явный ``--search-adapter`` >
    ``search.by_language[lang]`` из ``config/deploy.yaml`` > ``search.adapter`` >
    ``yandex``. Маршрут зависит только от языка (не от продукта) — симметричный протокол
    I16 сохраняется; выбранный провайдер фиксируется в payload (``provider``/``adapter_kind``)
    как провенанс (I23). Неизвестный провайдер из конфига честно отвергается
    (:data:`EXIT_USAGE`), а не подменяется — кандидаты не выдумываются (I19).

    **Недоступный адаптер** (нет ``httpx`` или учётных данных при сборке реального
    адаптера) → :data:`EXIT_ERROR` с ясным сообщением; кандидаты **не выдумываются**.
    Тестовый шов ``search_adapter`` (аргумент :func:`main`) имеет высший приоритет и
    пропускает резолв/валидацию провайдера.

    ``--passports-dir`` **засеивает** дополнительные запросы из ``doc_domains`` паспортов
    (через :func:`core.config.passport_urls`) — это **лишь подсказки-кандидаты**, домены
    не фетчатся (I19). Хотя бы один запрос обязателен (``--query`` либо seed).

    :returns: :data:`EXIT_OK` (список кандидатов может быть пуст); :data:`EXIT_ERROR`
        при недоступном адаптере; :data:`EXIT_USAGE` при пустом наборе запросов либо
        неизвестном провайдере из конфига.
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

    adapter: Any
    try:
        adapter, _provider = resolve_and_build_search_adapter(
            lang=args.lang,
            explicit=args.search_adapter_kind,
            injected=injected,
        )
    except SearchResolutionError as exc:
        _emit_error(args, exc.code, _concise(exc))
        return EXIT_USAGE if exc.code == "search" else EXIT_ERROR
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
    # Провенанс выбранного провайдера (I23): и вид адаптера, и имя провайдера.
    payload["adapter_kind"] = getattr(adapter, "adapter_kind", "serp")
    payload["provider"] = getattr(adapter, "provider", "")
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


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``discover``."""
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
        default=None,
        help="провайдер поиска (I23); по умолчанию — из config/deploy.yaml "
        "(search.adapter/by_language по --lang) или yandex",
    )
    p_discover.add_argument(
        "--passports-dir",
        default=None,
        help="каталог паспортов: doc_domains используется ТОЛЬКО как seed-подсказки "
        "запросов и не фетчится (I19)",
    )
    _add_common(p_discover)
    p_discover.set_defaults(handler=run)
