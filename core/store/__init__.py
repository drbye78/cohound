"""coHound — хранилище (store, L3): SQLite + пути.

Реализует ``StorePort``/``GateStore`` (``core/ports/``). Схема — ``core/store/sqlite.py`` (§4.4).
"""

from __future__ import annotations

__all__ = ["sqlite", "backup"]
