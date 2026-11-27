#!/usr/bin/env python3
"""Тонкая обёртка над ``hound gc`` (логика — :mod:`core.pipeline.gc`).

Историческая точка входа прибора: флаги, вывод и коды возврата те же, что у
``./hound.py gc``. Никакой второй копии логики здесь нет и быть не должно — два
определения «что такое сирота» разошлись бы на первой же правке.

Описание семантики (живое множество как объединение по всем ``state.db``, цена
удаления, классы, отказы, аудит) — в докстринге :mod:`core.pipeline.gc`.

Запуск::

    python3 scripts/prune_orphans.py --package packages/ru-pg-dbms \\
        --state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db \\
        --state-db ~/.cohound/data/state/ru-pg-dbms/state.db --json

    # то же, но одной командой CLI
    ./hound.py gc --package packages/ru-pg-dbms --state-root ~/.cohound --json
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.cli.commands.gc import main  # noqa: E402

__all__ = ["main"]

if __name__ == "__main__":
    raise SystemExit(main())
