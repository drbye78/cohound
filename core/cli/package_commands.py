"""coHound — обратно-совместимый шим над :mod:`core.cli.package` (P1-A).

Исторический god-модуль ``package_commands`` разбит на подпакет :mod:`core.cli.package`
(:mod:`~core.cli.package.parser` — регистрация подкоманд, :mod:`~core.cli.package.commands`
— хендлеры). Этот модуль оставлен **тонким re-export-шимом** для обратной совместимости
импортов (внешние вызывающие, ``tests/cli/*``, ``core.cli.main``): реэкспортирует
``add_package_parser`` и все ``cmd_package_*``. Удаление шима — отдельный шаг после
миграции всех импортов.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from core.cli.package.commands import *  # noqa: F401,F403
from core.cli.package.commands import __all__ as _commands_all
from core.cli.package.parser import (  # noqa: F401
    _add_op_common,
    _add_profile_flags,
    _add_read_common,
    add_package_parser,
)

__all__ = [*_commands_all, "add_package_parser"]
