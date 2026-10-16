"""coHound — реестр top-level CLI-команд (P1-A).

Каждый модуль команды экспортирует :func:`configure` (регистрация подкоманды в
argparse) и :func:`run` (тело команды). :data:`COMMANDS` фиксирует **порядок**
регистрации подкоманд, наблюдаемый в ``hound --help``. Вынесено из ``core.cli.main``.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from core.cli.commands import (
    crawl,
    deploy,
    diff,
    discover,
    experiment,
    gate,
    golden,
    passport_scout,
    plan,
    promote,
    report,
    resolve,
    review,
    run,
    sensitivity,
)

#: Порядок подкоманд в CLI (совпадает с историческим ``build_parser`` 1:1).
COMMANDS = (
    resolve,
    plan,
    run,
    crawl,
    diff,
    sensitivity,
    golden,
    report,
    discover,
    passport_scout,
    review,
    promote,
    gate,
    deploy,
    experiment,
)

__all__ = ["COMMANDS"]
