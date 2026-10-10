"""coHound — подпакет CLI-команд ``hound package`` (P1-A).

Парсер/регистрация подкоманд — :mod:`core.cli.package.parser`; хендлеры —
:mod:`core.cli.package.commands`. ``core/cli/package_commands.py`` оставлен тонким
re-export-шимом для обратной совместимости.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from core.cli.package.commands import *  # noqa: F401,F403
from core.cli.package.commands import __all__ as _commands_all
from core.cli.package.parser import add_package_parser

__all__ = [*_commands_all, "add_package_parser"]
