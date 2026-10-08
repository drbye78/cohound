"""coHound — CLI (composition root Z0, ручной DI, A1; ``03`` §E, ``01`` §1).

Пакет собирает зависимости ядра и предоставляет пользовательские команды. Это
**единственное** место сборки ядра: ``pipeline`` получает порты как аргументы и не знает
о профиле развёртывания (``03`` §E.3). Второй composition root — ``edge/`` (Z1/DMZ),
он ядром не импортируется.

Публичная поверхность: :func:`main` (точка входа) и сборка зависимостей из
:mod:`core.cli.wiring`.

**О коллизии имён.** Подмодуль ``core.cli.main`` и экспортируемая функция ``main``
носят одно имя; импорт подмодуля выставляет атрибут пакета ``main`` в *модуль*. Чтобы
фасад отдавал *функцию*, ``main`` импортируется здесь эагерно (после загрузки подмодуля
атрибут перезаписывается функцией). Побочный эффект — runpy предупреждает при
``python -m core.cli.main``; это известный false-positive, точечно подавляется ниже
(рекомендуемая точка входа — ``python -m core.cli``; см. ``__main__.py``).

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import warnings

# runpy ругается, что core.cli.main уже в sys.modules (пакет импортирует его эагерно).
# Это ожидаемо для пакета, экспортирующего символ `main`; предупреждение не отражает
# реальную проблему (исполнение `-m core.cli.main` корректно), поэтому гасим точечно.
warnings.filterwarnings(
    "ignore",
    message=r".*'core\.cli\.main' found in sys\.modules.*",
    category=RuntimeWarning,
)

from core.cli.main import (  # noqa: E402 — импорт после настройки фильтра
    EXIT_BLOCKED,
    EXIT_ERROR,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    CliError,
    LintFailure,
    build_parser,
    main,
)
from core.cli.wiring import (  # noqa: E402
    LOCAL_PROFILE,
    SPLIT_PROFILE,
    SUPPORTED_PROFILES,
    WiringError,
    build_adapters,
    build_runner,
    build_store,
    run_db_path,
)

__all__ = [
    # точка входа и коды возврата (T5.3)
    "main",
    "build_parser",
    "CliError",
    "LintFailure",
    "EXIT_OK",
    "EXIT_ERROR",
    "EXIT_USAGE",
    "EXIT_BLOCKED",
    "EXIT_NOT_FOUND",
    # сборка зависимостей (composition root Z0)
    "build_store",
    "build_adapters",
    "build_runner",
    "run_db_path",
    "WiringError",
    "LOCAL_PROFILE",
    "SPLIT_PROFILE",
    "SUPPORTED_PROFILES",
]
