"""coHound — transitional re-export-шим (T1.3).

Монолитный DDL переехал в ``core/store/sqlite.py``. Модуль сохраняет обратную совместимость
импортов ``from core.db import connect`` на время миграции. После перевода импортов на
``core.store`` шим удаляется.
"""

from __future__ import annotations

from core.store.sqlite import DDL, SCHEMA_VERSION, connect

__all__ = ["DDL", "SCHEMA_VERSION", "connect"]
