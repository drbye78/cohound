"""coHound — реестр top-level CLI-команд (P1-A).

Каждый модуль команды экспортирует :func:`configure` (регистрация подкоманды в
argparse) и :func:`run` (тело команды). :data:`COMMANDS` фиксирует **порядок**
регистрации подкоманд, наблюдаемый в ``hound --help``. Вынесено из ``core.cli.main``.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from core.cli.commands import (
    baseline,
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
    runs,
    sensitivity,
    status,
)

#: Порядок подкоманд в CLI: ``runs``/``status`` — вход (обнаружить прогон и понять, что
#: дальше), затем исторический порядок ``build_parser`` 1:1. Новые команды вставляются
#: осознанно: наблюдаемый ``hound --help`` — часть интерфейса.
COMMANDS = (
    runs,
    status,
    resolve,
    plan,
    run,
    crawl,
    diff,
    sensitivity,
    golden,
    baseline,
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
