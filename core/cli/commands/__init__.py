"""coHound — реестр top-level CLI-команд (P1-A).

Каждый модуль команды экспортирует :func:`configure` (регистрация подкоманды в
argparse) и :func:`run` (тело команды). :data:`COMMANDS` фиксирует **порядок**
регистрации подкоманд, наблюдаемый в ``hound --help``. Вынесено из ``core.cli.main``.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from core.cli.commands import (
    artifacts,
    backup,
    baseline,
    baseline_products,
    crawl,
    deploy,
    diff,
    discover,
    experiment,
    extract,
    gate,
    gc,
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
    search,
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
    # ``artifacts`` стоит сразу за ``crawl``: это ответ на его же отказы — что именно
    # краул не взял и как объявить байты, добытые человеком (§10).
    artifacts,
    extract,
    diff,
    sensitivity,
    golden,
    baseline,
    # ``baseline_products`` — группа ``hound baseline`` (reference-продукты профиля,
    # план 27 §5): стоит рядом с ``baseline-agreement`` (согласие аннотаторов, §11.5),
    # потому что оба — про эталон, но предметы разные: измерение предка против
    # человеческой разметки.
    baseline_products,
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
    # ``search`` стоит рядом с ``review``: обе читают готовый прогон и ничего не пишут,
    # но ``review`` — очередь решений человека, а ``search`` — материал для них.
    search,
    gate,
    # ``gc`` стоит сразу за ``gate``: обе команды — про готовый результат, но
    # ``gate`` решает публиковать ли, а ``gc`` убирает то, на что уже никто не
    # ссылается (и печатает отчёт по прогонам, прежде чем их тронуть).
    gc,
    deploy,
    experiment,
)

__all__ = ["COMMANDS"]
