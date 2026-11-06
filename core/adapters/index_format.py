"""Общий формат индексной сборки на диске (02 §B.2, K1/I17) — точка согласия сборщика и поиска.

Раскладка (иммутабельная сборка, продукт-партиции I17)::

    index_store/{package}/{build}/{product_id}/
      vectors.fp16.npy      # float16, shape (N, dim)
      chunk_ids.jsonl       # строка i ↔ chunk_id [+ content_hash] (порядок строк = порядок векторов)
      manifest.json         # embed_model_id, embed_device, embed_precision, dim, chunker_version, stats
    index_store/{package}/_compositions/{composition_id}.json
                          # манифест композиции партиций (09 §8.2), не сборка

Модуль — единственный носитель формата: ``index_npy.py`` (``IndexBuilderPort``) пишет
через :func:`write_partition`, ``vector_npy.py`` (``VectorIndexPort``) читает через
:func:`read_partition`. Это исключает расхождение раскладки между писателем и читателем.

Существующая сборка **не мутируется** (K2/§13): запись идёт в staging-каталог, прежняя
партиция уводится одним rename в мусорное имя, и на её место встаёт staging атомарным
``os.replace`` — пути партиции не бывает «наполовину удалённым». Нейтрален (§1.3).

**Две формы ``chunk_ids.jsonl`` (ревизия ``partition-2``).** Строка — либо
``{"chunk_id": …}`` (исходный формат), либо ``{"chunk_id": …, "content_hash": …}``:
вторую пишет сборщик, передавая ``content_hashes``. ``content_hash`` нужен, чтобы полный
отпечаток партиции (``vector_digest``, :func:`core.util.index_partition.vector_digest`)
пересчитывался **с диска**, а не только из входов сборки (09 §8.5). Обе формы читаются
одинаково: отсутствие ``content_hash`` — не ошибка чтения, а отсутствие доказательства
дайджеста (старая партиция остаётся читаемой, но не переиспользуемой по новому гейту).

**Чтение — через ``mmap``.** ``read_partition`` отображает ``vectors.fp16.npy`` в память
(``mmap_mode="r"``), а не читает его в кучу на каждый вызов; формат на диске остаётся
``float16``. Конверсия в ``float32`` для скалярного произведения — забота читателя
(``vector_npy``), и она делается **один раз на партицию**, а не на каждый запрос.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.util.index_partition import (
    CHUNK_IDS_FILE as _CHUNK_IDS,
)
from core.util.index_partition import (
    MANIFEST_FILE as _MANIFEST,
)
from core.util.index_partition import (
    VECTORS_FILE as _VECTORS,
)
from core.util.index_partition import (
    build_root,
    compositions_dir,
    is_committed,
    list_build_ids,
    list_composition_ids,
    list_partitions,
    partition_dir,
    read_manifest,
)

__all__ = [
    "PartitionData",
    "build_root",
    "compositions_dir",
    "is_committed",
    "list_build_ids",
    "list_composition_ids",
    "list_partitions",
    "partition_dir",
    "read_manifest",
    "read_partition",
    "write_partition",
]


@dataclass(frozen=True)
class PartitionData:
    """Прочитанная продукт-партиция: матрица векторов, id чанков и манифест."""

    product_id: str
    vectors: Any            # numpy.ndarray (N, dim) float16
    chunk_ids: list[str]
    manifest: dict[str, Any]


def write_partition(
    index_root: str | Path,
    package: str,
    build_id: str,
    product_id: str,
    vectors: Sequence[Sequence[float]],
    chunk_ids: Sequence[str],
    manifest: dict[str, Any],
    *,
    content_hashes: Sequence[str] | None = None,
) -> Path:
    """Пишет партицию **атомарно** в ``index_store/{package}/{build}/{product_id}/``.

    Байты формируются в staging-каталоге рядом с целью. Прежняя партиция (если есть)
    уводится **целиком одним rename** в мусорное имя, затем на её место встаёт staging
    через ``os.replace``; мусор удаляется после. Существующая сборка не мутируется
    на месте (K2/§13). Векторы конвертируются в ``float16`` (02 §B.2). Возвращает путь
    партиции.

    Почему не ``rmtree`` + ``replace``: ``rmtree`` удаляет файл за файлом, и на всё это
    время каталог партиции **существует частично** — конкурентный читатель
    (``read_partition``/``is_committed``/``list_partitions``) получал ``FileNotFoundError``
    на ``chunk_ids.jsonl`` или манифест. Теперь партиция на пути видна либо целиком
    старая, либо целиком новая: окно между двумя rename — один системный вызов, а не
    длительность удаления дерева файлов.

    :raises ValueError: если длины ``vectors`` и ``chunk_ids`` не совпадают (а также
        ``content_hashes`` и ``chunk_ids``, когда ``content_hashes`` переданы).
    """
    import numpy as np

    if len(vectors) != len(chunk_ids):
        raise ValueError(
            f"vectors ({len(vectors)}) и chunk_ids ({len(chunk_ids)}) должны совпадать"
        )
    if content_hashes is not None and len(content_hashes) != len(chunk_ids):
        raise ValueError(
            f"content_hashes ({len(content_hashes)}) и chunk_ids ({len(chunk_ids)}) "
            "должны совпадать"
        )
    target = build_root(index_root, package, build_id, product_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=str(target.parent)))
    trash: Path | None = None
    try:
        matrix = np.asarray(vectors, dtype=np.float16)
        if matrix.ndim != 2:
            matrix = matrix.reshape(len(vectors), -1)
        np.save(staging / _VECTORS, matrix)
        with (staging / _CHUNK_IDS).open("w", encoding="utf-8") as fh:
            for index, chunk_id in enumerate(chunk_ids):
                row: dict[str, str] = {"chunk_id": chunk_id}
                if content_hashes is not None:
                    # ``partition-2``: хеш текста чанка рядом с id — иначе полный
                    # отпечаток партиции нельзя пересчитать с диска (09 §8.5).
                    row["content_hash"] = content_hashes[index]
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        (staging / _MANIFEST).write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2),
            encoding="utf-8",
        )
        if target.exists():
            # Имя мусора уникально: staging-каталог уже создан с уникальным суффиксом.
            trash = staging.with_name(f"{staging.name}-old")
            os.replace(target, trash)
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        if trash is not None and trash.exists() and not target.exists():
            # Новая партиция не встала на место — возвращаем прежнюю, чтобы не потерять сборку.
            with contextlib.suppress(OSError):
                os.replace(trash, target)
        raise
    if trash is not None:
        shutil.rmtree(trash, ignore_errors=True)
    return target


def read_partition(
    index_root: str | Path,
    package: str,
    build_id: str,
    product_id: str,
    *,
    mmap: bool = True,
) -> PartitionData:
    """Читает продукт-партицию (векторы + chunk_ids + манифест).

    Строки ``chunk_ids.jsonl`` обеих форм (``partition-2`` и исходной) читаются одинаково:
    берётся ``chunk_id``, а необязательный ``content_hash`` не требуется — старая партиция
    остаётся читаемой (её отпечаток просто недоказуем; см. модульный docstring).

    **Векторы — через ``mmap`` по умолчанию.** ``np.load`` без ``mmap_mode`` читает
    ``vectors.fp16.npy`` целиком в кучу на **каждый** вызов: для партиции 11 400 × 1024
    это 23.3 MB чтения и новая аллокация на каждый поиск. При ``mmap=True`` файл
    отображается в память, страницы приходят по обращению, а сам массив остаётся
    ``float16`` (формат на диске не меняется). Сборки иммутабельны (K2/§13), поэтому
    отображение не может «сдвинуться» под читателем; удаление файла на Linux отображение
    не ломает (inode жив, пока маппинг открыт).

    :param mmap: ``False`` — прежний путь (полное чтение в память). ``True`` — mmap с
        откатом на полное чтение, если ``numpy``/формат его не поддерживают: отсутствие
        возможности ускорить чтение не является поводом падать.
    :raises FileNotFoundError: если партиция не зафиксирована.
    """
    import numpy as np

    root = build_root(index_root, package, build_id, product_id)
    vectors_path = root / _VECTORS
    chunk_ids_path = root / _CHUNK_IDS
    if not vectors_path.is_file() or not chunk_ids_path.is_file():
        raise FileNotFoundError(f"партиция не зафиксирована: {root}")
    vectors = _load_vectors(np, vectors_path, mmap=mmap)
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


def _load_vectors(np: Any, path: Path, *, mmap: bool) -> Any:
    """``np.load`` с ``mmap_mode="r"`` и откатом на полное чтение (старый ``numpy``)."""
    if mmap:
        try:
            return np.load(path, mmap_mode="r")
        except (TypeError, ValueError):  # pragma: no cover - старый numpy/несовместимый файл
            pass
    return np.load(path)
