"""``deploy-check``: диагностика DMZ-шва (T13.6; ``07`` §3/§7/§8).

Вынесено из ``core.cli.main`` (P1-A). Модуль экспортирует :func:`configure`
(регистрация аргументов подкоманды) и :func:`run` (тело команды).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from typing import Any

import argparse

from core.cli.context import (
    _diagnostic_gateway,
    _emit_deploy_check,
    _seam_diagnostics,
)
from core.cli.errors import EXIT_ERROR, EXIT_OK
from core.config.deploy import is_split, load_deploy_config


def run(args: argparse.Namespace) -> int:
    """``deploy-check``: диагностика DMZ-шва (T13.6; ``07`` §3/§7/§8).

    Загружает deploy-конфиг и для профилей ``split``/``split-hardened`` строит gateway
    по секции ``edge.*``, вызывает ``/v1/health`` и ``/v1/capabilities`` и проверяет
    конверт/полезную нагрузку против опубликованного контракта шва. Дополнительно
    выполняется **неразрушающий** зонд crawl-операций (``crawl/status`` с фиктивным
    ``job_id``): доступность очереди краула попадает в отчёт строкой ``crawl`` (аддитивно;
    на код возврата не влияет). Профиль ``local`` шва не использует — команда сообщает
    об этом и успешна.

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


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``deploy-check``."""
    p_deploy = sub.add_parser(
        "deploy-check",
        help="диагностика DMZ-шва: /v1/health + /v1/capabilities + crawl + схема (07 §3/§7/§8)",
    )
    p_deploy.add_argument(
        "--deploy",
        default=None,
        help="путь к deploy.yaml (по умолчанию config/deploy.yaml, 03 D.5)",
    )
    p_deploy.add_argument(
        "--json", action="store_true", help="машинночитаемый вывод (JSON) в stdout"
    )
    p_deploy.set_defaults(handler=run)
