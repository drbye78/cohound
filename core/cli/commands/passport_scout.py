"""``passport-scout``: авто-генерация **черновика** паспорта продукта (M1).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
from typing import Any

from core.cli.context import (
    _add_common,
    _concise,
    _emit_error,
    _load_package_roles,
    _print_json,
    _print_passport_scout,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK, EXIT_USAGE
from core.cli.wiring import (
    WiringError,
    _llm_configured,
    build_adapters,
    build_discovery_llm,
    build_scout_crawler,
)
from core.config.deploy import load_deploy_config
from core.pipeline import scout_passport, write_passport_draft
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMPort


def run(args: argparse.Namespace) -> int:
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
                slot=getattr(args, "llm_slot", None),
                deploy_llm=load_deploy_config(None).llm,
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
    # Корень индекса — из resolved-раскладки (03 §A.1); passport-scout индекс не пишет,
    # но каталог должен быть согласован с остальными командами.
    _scout_paths = getattr(args, "paths", None)
    profile_ports = build_adapters(
        profile=args.profile,
        package=getattr(args, "package", None),
        index_root=(
            str(_scout_paths.index_store) if _scout_paths is not None else "index_store"
        ),
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


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``passport-scout``."""
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
        default=None,
        help="каталог паспортов: пишет passports/{product_id}/{date}.yaml (дефолт — "
        "раскладка: config/paths.yaml/COHOUND_PASSPORTS_DIR/COHOUND_HOME, иначе passports)",
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
        "--llm-slot",
        choices=["cheap", "flagship"],
        default=None,
        help="активный LLM-слот (override COHOUND_LLM_SLOT/deploy.llm.slot)",
    )
    p_scout.add_argument(
        "--dry-run", action="store_true", help="показать черновик без записи файлов"
    )
    _add_common(p_scout)
    p_scout.set_defaults(handler=run)
