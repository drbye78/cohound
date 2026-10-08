"""Точка входа ``python -m core.cli`` — чистый запуск без runpy-предупреждения.

Эквивалент ``python -m core.cli.main``, но пакет не импортирует подмодуль ``main``
эагерно до его исполнения, поэтому ``runpy`` не предупреждает о двойном импорте.
"""

from __future__ import annotations

from core.cli.main import main

if __name__ == "__main__":  # pragma: no cover — точка входа модуля
    raise SystemExit(main())
