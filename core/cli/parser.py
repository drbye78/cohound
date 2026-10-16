"""coHound — сборка argparse-парсера CLI (composition root Z0; ``03`` §E).

:func:`build_parser` собирает top-level команды через ``configure``-функции из
:mod:`core.cli.commands` (реестр :data:`~core.cli.commands.COMMANDS`), добавляет
``doctor`` (ленивый импорт) и подпакет ``package`` (``add_package_parser``). Вынесено
из ``core.cli.main`` (P1-A); порядок подкоманд сохранён 1:1.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse

from core.cli.commands import COMMANDS
from core.cli.context import _add_common
from core.cli.package.parser import add_package_parser

__all__ = ["build_parser"]


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

    # Top-level команды: порядок задан реестром COMMANDS (наблюдаемый --help 1:1).
    for module in COMMANDS:
        module.configure(sub)

    # doctor -------------------------------------------------------------- #
    p_doctor = sub.add_parser(
        "doctor",
        help="самодиагностика инсталляции: runtime, зависимости, env, конфиг, каталоги, CLI",
    )
    p_doctor.add_argument(
        "--with-selfcheck",
        dest="with_selfcheck",
        action="store_true",
        help="запустить tests/selfcheck.py (subprocess) и включить результат в отчёт",
    )
    p_doctor.add_argument(
        "--package",
        default=None,
        help="каталог пакета: проверить его package-scoped реестровые спецификации "
        "(packages/{case_id}/registries/)",
    )
    p_doctor.add_argument(
        "--strict",
        action="store_true",
        help="считать warn ошибкой (ненулевой код возврата при предупреждениях)",
    )
    _add_common(p_doctor)
    # Ленивый импорт: логика проверок не подтягивается, пока команда не запрошена
    # (образец подключения — ``package``); doctor не создаёт циклических импортов.
    from core.cli.doctor import cmd_doctor

    p_doctor.set_defaults(handler=cmd_doctor)

    # package ------------------------------------------------------------- #
    # Подпакет ``hound package`` (CRUD + discovery, I1); регистрация через
    # ``core.cli.package.parser`` (хендлеры — ``core.cli.package.commands``).
    add_package_parser(sub)

    return parser
