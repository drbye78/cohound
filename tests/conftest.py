"""Pytest-фикстуры: корень репозитория на sys.path (плоская раскладка, §3.3).

**Изоляция межпрогонных кэшей.** ``resolve_paths()`` без ``COHOUND_HOME``/``paths.yaml``
отдаёт CWD-относительные дефолты, поэтому кэши Ф1.2/Ф3 (``extract_cache/``,
``eval_cache/``) без этой фикстуры попадали бы в корень репозитория — общий каталог на
все прогоны процесса. Это делало набор тестов **неидемпотентным**: первый прогон
наполнял кэш, второй получал хит и падал на утверждениях вида «оценщик вызван».
Кэш обязан переживать прогоны (в этом его смысл) — значит изолировать его должны
тесты, а не боевой код. Сессионный временный каталог даёт ровно это: внутри сессии
кэш общий (как в бою), между сессиями — пустой.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

#: Env-переменные каталогов межпрогонных кэшей (см. ``core/config/paths.py``).
_CACHE_ENV = ("COHOUND_EXTRACT_CACHE_DIR", "COHOUND_EVAL_CACHE_DIR")


@pytest.fixture(scope="session", autouse=True)
def _isolated_operational_caches(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Уводит межпрогонные кэши из корня репозитория во временный каталог сессии.

    ``autouse`` и ``session``: кэш общий для всех тестов сессии (так проверяется
    переиспользование), но не переживает её — иначе состояние протекает между
    запусками ``pytest`` и вердикты из предыдущего прогона обслуживают следующий.
    """
    root = tmp_path_factory.mktemp("operational_caches")
    for index, name in enumerate(_CACHE_ENV):
        os.environ[name] = str(root / f"cache{index}")
