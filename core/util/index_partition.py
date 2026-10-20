"""Read-only раскладка и проверка продукт-партиций индекса (02 §B.2, 09 §8.2).

Здесь живёт **чтение** раскладки ``index_store/{package}/{build}/{product_id}/``:
пути, перечисление сборок/партиций и проверка «партиция зафиксирована» (три артефакта
на месте + читаемый манифест). Писатель формата — ``adapters/index_format.py``; он
переиспользует эти функции, поэтому раскладка не дублируется.

Модуль вынесен в ``util`` намеренно: его читают три слоя, которым запрещены встречные
импорты — ``adapters/index_format`` (формат), ``pipeline/corpus`` (переиспользование
партиции, 09 §8.5) и ``store/backup`` (GC, 09 §8.6). ``pipeline`` по матрице зависимостей
(01 §1) не может импортировать ``core.adapters.*``, поэтому физическая проверка обязана
жить в разрешённом слое.

Нейтрален (§1.3), только stdlib.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.util.index_layout import COMPOSITIONS_DIR

__all__ = [
    "CHUNK_IDS_FILE",
    "MANIFEST_FILE",
    "VECTORS_FILE",
    "build_root",
    "compositions_dir",
    "is_committed",
    "list_build_ids",
    "list_composition_ids",
    "list_partitions",
    "partition_dir",
    "read_manifest",
]

#: Имена трёх артефактов зафиксированной продукт-партиции.
VECTORS_FILE = "vectors.fp16.npy"
CHUNK_IDS_FILE = "chunk_ids.jsonl"
MANIFEST_FILE = "manifest.json"


def build_root(
    index_root: str | Path, package: str, build_id: str, product_id: str
) -> Path:
    """Каталог продукт-партиции: ``index_store/{package}/{build}/{product_id}/``."""
    return Path(index_root) / package / build_id / product_id


def partition_dir(
    index_root: str | Path, package: str, build_id: str, product_id: str
) -> Path:
    """Псевдоним :func:`build_root` (явное имя для читателей)."""
    return build_root(index_root, package, build_id, product_id)


def is_committed(
    index_root: str | Path, package: str, build_id: str, product_id: str
) -> bool:
    """Зафиксирована ли партиция: три артефакта на месте и манифест читается.

    Читаемость манифеста — часть «зафиксирована»: битый JSON не должен выглядеть как
    валидная иммутабельная партиция (иначе guard K2/§13 пропустит перезапись).
    """
    root = build_root(index_root, package, build_id, product_id)
    if not all(
        (root / name).is_file()
        for name in (VECTORS_FILE, CHUNK_IDS_FILE, MANIFEST_FILE)
    ):
        return False
    try:
        read_manifest(index_root, package, build_id, product_id)
    except (OSError, ValueError):
        return False
    return True


def read_manifest(
    index_root: str | Path, package: str, build_id: str, product_id: str
) -> dict[str, Any]:
    """Читает ``manifest.json`` партиции (пустой dict, если файла нет)."""
    path = build_root(index_root, package, build_id, product_id) / MANIFEST_FILE
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def list_partitions(
    index_root: str | Path, package: str, build_id: str
) -> list[str]:
    """Продукт-партиции сборки (имена подкаталогов), отсортированные детерминированно.

    Единый носитель раскладки: читатель индекса не дублирует знание о том, что партиция —
    это подкаталог ``{product_id}/``. Служебный ``_compositions`` — не партиция (09 §8.2).
    Пустой список — сборки/пакета нет.
    """
    build_dir = Path(index_root) / package / build_id
    if not build_dir.is_dir():
        return []
    return sorted(
        child.name
        for child in build_dir.iterdir()
        if child.is_dir() and child.name != COMPOSITIONS_DIR
    )


def list_build_ids(index_root: str | Path, package: str) -> list[str]:
    """Идентификаторы индексных сборок пакета (каталоги верхнего уровня).

    Служебный ``_compositions`` исключён здесь — в одном месте для всех читателей
    (``wiring``/``backup``), чтобы каталог композиций не выглядел сборкой.
    """
    package_dir = Path(index_root) / package
    if not package_dir.is_dir():
        return []
    return sorted(
        child.name
        for child in package_dir.iterdir()
        if child.is_dir() and child.name != COMPOSITIONS_DIR
    )


def compositions_dir(index_root: str | Path, package: str) -> Path:
    """Каталог манифестов композиций: ``index_store/{package}/_compositions/``."""
    return Path(index_root) / package / COMPOSITIONS_DIR


def list_composition_ids(index_root: str | Path, package: str) -> list[str]:
    """Идентификаторы манифестов композиций пакета (``*.json``), детерминированно.

    Служебный каталог не является сборкой; здесь перечисляются именно **композиции**
    (09 §8.2) — по имени файла без суффикса. Битые/посторонние файлы не фильтруются по
    содержимому: это дело читателя (:func:`core.pipeline.composition.read_composition`).
    """
    directory = compositions_dir(index_root, package)
    if not directory.is_dir():
        return []
    return sorted(
        path.stem
        for path in directory.iterdir()
        if path.is_file() and path.suffix == ".json"
    )
