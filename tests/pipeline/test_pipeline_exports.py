"""Фасад ``core.pipeline`` — ленивый экспорт (PEP 562, L0 плана декомпозиции).

Проверяется ровно то, чем шаг L0 доказывается:

1. карта ``_LAZY`` и ``__all__`` не расходятся (иначе имя есть в публичной
   поверхности, но резолвится в ``AttributeError``);
2. каждое имя из ``__all__`` резолвится и не ``None``;
3. **ленивость**: ``import core.pipeline`` в свежем интерпретаторе не импортирует
   ``core.pipeline.corpus`` — иначе барьер импорта не снят, а переехал;
4. ``from core.pipeline import build_corpus`` работает, как и импорт **подмодуля**
   через фасад (``from core.pipeline import package_admin``) — вторая ветка
   ``__getattr__``, без которой такие импорты получили бы ложный ``AttributeError``.

Подпроцесс ровно один: проверка ленивости по ``sys.modules`` возможна только в
чистом процессе, а каждый такой запуск стоит полной цены импорта пакета.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import core.pipeline as facade

ROOT = Path(__file__).resolve().parent.parent.parent


def test_lazy_map_matches_public_surface() -> None:
    """``_LAZY`` и ``__all__`` — одно и то же множество имён."""
    assert set(facade._LAZY) == set(facade.__all__)


def test_every_public_name_resolves() -> None:
    """Каждое имя ``__all__`` резолвится через ``__getattr__`` и не пустое."""
    unresolved: list[str] = []
    empty: list[str] = []
    for name in facade.__all__:
        try:
            value = getattr(facade, name)
        except AttributeError:  # pragma: no cover - сообщается списком ниже
            unresolved.append(name)
            continue
        if value is None:
            empty.append(name)
    assert unresolved == []
    assert empty == []


def test_import_package_does_not_pull_corpus() -> None:
    """``import core.pipeline`` не тянет ``corpus``: фасад ленив по факту, не по имени."""
    code = (
        "import json, sys\n"
        "import core.pipeline\n"
        "print(json.dumps({\n"
        "    'corpus': 'core.pipeline.corpus' in sys.modules,\n"
        "    'serp_loop': 'core.pipeline.serp_loop' in sys.modules,\n"
        "    'all': len(core.pipeline.__all__),\n"
        "    'lazy': len(core.pipeline._LAZY),\n"
        "}))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    observed = json.loads(completed.stdout)
    assert observed["corpus"] is False, "import core.pipeline импортировал corpus"
    assert observed["serp_loop"] is False
    assert observed["all"] == observed["lazy"] == len(facade.__all__)


def test_public_and_submodule_imports_through_facade() -> None:
    """Импорт имени и импорт подмодуля через фасад — обе ветки ``__getattr__``."""
    from core.pipeline import build_corpus, package_admin

    assert callable(build_corpus)
    assert package_admin.__name__ == "core.pipeline.package_admin"
    # Подмодуль импортируется и как модуль (первый шаг ``package_candidates``).
    import core.pipeline.package_candidates as candidates

    assert candidates.__name__ == "core.pipeline.package_candidates"
