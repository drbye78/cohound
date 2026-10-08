"""coHound — transitional re-export-шим (T1.2).

Изначально здесь был монолит схем/инвариантов/Р-2/словаря. В W1/T1.2 он расщеплён на
``core/domain/`` (``enums`` · ``models`` · ``r2`` · ``dictionary``). Этот модуль сохраняет
обратную совместимость импортов ``from core.schemas import ...`` на время миграции.

План: после перевода всех импортов на ``core.domain`` шим удаляется (T1.3/T1.4).
"""

from __future__ import annotations

from core.domain import *  # noqa: F401,F403
from core.domain import __all__ as _domain_all

__all__ = list(_domain_all)
