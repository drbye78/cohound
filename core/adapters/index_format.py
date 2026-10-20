"""Общий формат индексной сборки на диске (02 §B.2, K1/I17) — точка согласия сборщика и поиска.

Раскладка (иммутабельная сборка, продукт-партиции I17)::

    index_store/{package}/{build}/{product_id}/
      vectors.fp16.npy      # float16, shape (N, dim)
      chunk_ids.jsonl       # строка i ↔ chunk_id (порядок строк = порядок векторов)
      manifest.json         # embed_model_id, dim, chunker_version, stats
    index_store/{package}/_compositions/{composition_id}.json
                          # манифест композиции партиций (09 §8.2), не сборка

Модуль — единственный носитель формата: ``index_npy.py`` (``IndexBuilderPort``) пишет
через :func:`write_partition`, ``vector_npy.py`` (``VectorIndexPort``) читает через
:func:`read_partition`. Это исключает расхождение раскладки между писателем и читателем.

Существующая сборка **не мутируется** (K2/§13): запись идёт в staging-каталог и
фиксируется атомарным ``os.replace``. Нейтрален (§1.3).
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from core.util.index_layout import COMPOSITIONS_DIR

__all__ = [
    "PartitionData",
    "build_root",
    "partition_dir",
    "list_partitions",
    "list_build_ids",
    "compositions_dir",
    "write_partition",
    "read_partition",
    "read_manifest",
    "is_committed",
]

_VECTORS = "vectors.fp16.npy"
_CHUNK_IDS = "chunk_ids.jsonl"
_MANIFEST = "manifest.json"

#: Служебный каталог композиций (09 §8.2): не сборка и не продукт-партиция.
_COMPOSITIONS_DIR = COMPOSITIONS_DIR


@dataclass(frozen=True)
class PartitionData:
    """Прочитанная продукт-партиция: матрица векторов, id чанков и манифест."""

    product_id: str
    vectors: Any            # numpy.ndarray (N, dim) float16
    chunk_ids: list[str]
    manifest: dict[str, Any]


def build_root(index_root: str | Path, package: str, build_id: str, product_id: str) -> Path:
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
    if not all((root / name).is_file() for name in (_VECTORS, _CHUNK_IDS, _MANIFEST)):
        return False
    try:
        read_manifest(index_root, package, build_id, product_id)
    except (OSError, ValueError):
        return False
    return True


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
        if child.is_dir() and child.name != _COMPOSITIONS_DIR
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
        if child.is_dir() and child.name != _COMPOSITIONS_DIR
    )


def compositions_dir(index_root: str | Path, package: str) -> Path:
    """Каталог манифестов композиций: ``index_store/{package}/_compositions/``."""
    return Path(index_root) / package / _COMPOSITIONS_DIR


def write_partition(
    index_root: str | Path,
    package: str,
    build_id: str,
    product_id: str,
    vectors: Sequence[Sequence[float]],
    chunk_ids: Sequence[str],
    manifest: dict[str, Any],
) -> Path:
    """Пишет партицию **атомарно** в ``index_store/{package}/{build}/{product_id}/``.

    Байты формируются в staging-каталоге рядом с целью, затем каталог заменяется
    через ``os.replace`` (существующая сборка не мутируется на месте — K2/§13).
    Векторы конвертируются в ``float16`` (02 §B.2). Возвращает путь партиции.

    :raises ValueError: если длины ``vectors`` и ``chunk_ids`` не совпадают.
    """
    import numpy as np

    if len(vectors) != len(chunk_ids):
        raise ValueError(
            f"vectors ({len(vectors)}) и chunk_ids ({len(chunk_ids)}) должны совпадать"
        )
    target = build_root(index_root, package, build_id, product_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=str(target.parent)))
    try:
        matrix = np.asarray(vectors, dtype=np.float16)
        if matrix.ndim != 2:
            matrix = matrix.reshape(len(vectors), -1)
        np.save(staging / _VECTORS, matrix)
        with (staging / _CHUNK_IDS).open("w", encoding="utf-8") as fh:
            for chunk_id in chunk_ids:
                fh.write(json.dumps({"chunk_id": chunk_id}, ensure_ascii=False) + "\n")
        (staging / _MANIFEST).write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        if target.exists():
            shutil.rmtree(target)
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return target


def read_manifest(
    index_root: str | Path, package: str, build_id: str, product_id: str
) -> dict[str, Any]:
    """Читает ``manifest.json`` партиции (пустой dict, если нет)."""
    path = build_root(index_root, package, build_id, product_id) / _MANIFEST
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def read_partition(
    index_root: str | Path, package: str, build_id: str, product_id: str
) -> PartitionData:
    """Читает продукт-партицию (векторы + chunk_ids + манифест).

    :raises FileNotFoundError: если партиция не зафиксирована.
    """
    import numpy as np

    root = build_root(index_root, package, build_id, product_id)
    vectors_path = root / _VECTORS
    chunk_ids_path = root / _CHUNK_IDS
    if not vectors_path.is_file() or not chunk_ids_path.is_file():
        raise FileNotFoundError(f"партиция не зафиксирована: {root}")
    vectors = np.load(vectors_path)
    chunk_ids = [
        json.loads(line)["chunk_id"]
        for line in chunk_ids_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return PartitionData(
        product_id=product_id,
        vectors=vectors,
        chunk_ids=chunk_ids,
        manifest=read_manifest(index_root, package, build_id, product_id),
    )
