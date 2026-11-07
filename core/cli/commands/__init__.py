"""coHound — реестр top-level CLI-команд (P1-A).

Каждый модуль команды экспортирует :func:`configure` (регистрация подкоманды в
argparse) и :func:`run` (тело команды). :data:`COMMANDS` фиксирует **порядок**
регистрации подкоманд, наблюдаемый в ``hound --help``. Вынесено из ``core.cli.main``.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from core.cli.commands import (
    backup,
    baseline,
    crawl,
    deploy,
    diff,
    discover,
    experiment,
    extract,
    gate,
    golden,
    passport_scout,
    plan,
    promote,
    report,
    resolve,
    retry,
    revalidate,
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
    extract,
    diff,
    sensitivity,
    golden,
    baseline,
    report,
    # Бэкап/восстановление стоят сразу за отчётом: это операции над готовым прогоном
    # (``runs/{run_id}/``) и его durable-состоянием, а не часть измерения.
    backup,
    discover,
    passport_scout,
    review,
    promote,
    revalidate,
    retry,
    gate,
    deploy,
    experiment,
)

__all__ = ["COMMANDS"]
