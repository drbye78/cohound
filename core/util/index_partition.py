"""Read-only раскладка и проверка продукт-партиций индекса (02 §B.2, 09 §8.2).

Здесь живёт **чтение** раскладки ``index_store/{package}/{build}/{product_id}/``:
пути, перечисление сборок/партиций, проверка «партиция зафиксирована» (три артефакта
на месте + читаемый манифест) и пересчёт полного отпечатка партиции с диска
(:func:`recompute_vector_digest`). Писатель формата — ``adapters/index_format.py``; он
переиспользует эти функции, поэтому раскладка не дублируется.

Модуль вынесен в ``util`` намеренно: его читают три слоя, которым запрещены встречные
импорты — ``adapters/index_format`` (формат), ``pipeline/corpus`` (переиспользование
партиции, 09 §8.5) и ``store/backup`` (GC, 09 §8.6). ``pipeline`` по матрице зависимостей
(01 §1) не может импортировать ``core.adapters.*``, поэтому физическая проверка обязана
жить в разрешённом слое. Ровно по этой причине **формула отпечатка** партиции
(:func:`vector_digest`) тоже здесь, а не в адаптере-сборщике: переиспользование
пересчитывает отпечаток из того слоя, куда ему разрешено импортировать.

Зависимости — stdlib, кроме ``numpy``: он нужен только :func:`recompute_vector_digest`
(чтение ``vectors.fp16.npy``) и импортируется **лениво**, поэтому модуль остаётся
импортируемым без extra ``retrieval``.

Нейтрален (§1.3).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from core.util.hashing import content_hash
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
    "recompute_vector_digest",
    "vector_digest",
]

#: Имена трёх артефактов зафиксированной продукт-партиции.
VECTORS_FILE = "vectors.fp16.npy"
CHUNK_IDS_FILE = "chunk_ids.jsonl"
MANIFEST_FILE = "manifest.json"

#: Разделитель полей material-строки отпечатка (unit separator): однозначная склейка,
#: не встречающаяся ни в ``chunk_id``, ни в hex-хеше.
_FIELD_SEP = "\x1f"
#: Разделитель групп «id чанков ↔ хеши» — исключает коллизии плоской склейки.
_GROUP_SEP = "\x1e"


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


def vector_digest(
    chunk_ids: Sequence[str], content_hashes: Sequence[str], matrix_bytes: bytes
) -> str:
    """Полный отпечаток содержимого партиции: id/хеши чанков + байты fp16-матрицы (I19).

    Формула — **единственная** (её использует и сборщик при записи ``vector_digest`` в
    манифест, и :func:`recompute_vector_digest` при проверке переиспользования): склейка
    «id чанков ↔ их хеши» через ``_GROUP_SEP``, затем байты матрицы через тот же
    разделитель. Порядок значим: строка ``chunk_ids.jsonl`` ↔ строка матрицы.
    """
    meta_material = _GROUP_SEP.join(
        (_FIELD_SEP.join(chunk_ids), _FIELD_SEP.join(content_hashes))
    ).encode("utf-8")
    return content_hash(meta_material + _GROUP_SEP.encode("utf-8") + matrix_bytes)


def recompute_vector_digest(partition_dir: str | Path) -> str | None:
    """Пересчитывает ``vector_digest`` партиции **с диска** (09 §8.5).

    Нужен второму, физическому контролю переиспользования: сборщик пишет отпечаток в
    манифест, но при переиспользовании сборка не строится, поэтому сверить его можно
    только пересчётом артефактов. Формула — та же :func:`vector_digest`, а не её копия.

    :returns: отпечаток либо ``None``. ``None`` — доказательство недоступно: файлов нет,
    вход нечитаем/битый, ``chunk_ids.jsonl`` не несёт ``content_hash`` хотя бы в одной
    строке (партиция исходного формата: её отпечаток недоказуем ⇒ не переиспользуется),
    ``numpy`` не установлен. Исключений функция не бросает: битая партиция — повод
    пересобрать её, а не уронить прогон.
    """
    root = Path(partition_dir)
    rows = _read_chunk_id_rows(root / CHUNK_IDS_FILE)
    if rows is None:
        return None
    chunk_ids, content_hashes = rows
    try:
        import numpy as np
    except ImportError:  # pragma: no cover - зависит от окружения
        return None
    try:
        matrix = np.load(root / VECTORS_FILE)
    except (OSError, ValueError, EOFError):
        return None
    # Квантизация та же, что у сборщика: отпечаток покрывает значения, которые реально
    # лежат в индексе (``float16``), а не исходную точность эмбеддера.
    return vector_digest(
        chunk_ids, content_hashes, np.asarray(matrix, dtype=np.float16).tobytes()
    )


def _read_chunk_id_rows(path: Path) -> tuple[list[str], list[str]] | None:
    """Читает ``chunk_ids.jsonl``: ``(chunk_ids, content_hashes)`` либо ``None``.

    Обе формы формата: ``{"chunk_id": …}`` и ``{"chunk_id": …, "content_hash": …}``.
    Вторая обязательна для пересчёта отпечатка, поэтому строка без ``content_hash``
    делает весь пересчёт недоказуемым (``None``), а не «частичным».
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    chunk_ids: list[str] = []
    content_hashes: list[str] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            return None
        if not isinstance(row, dict):
            return None
        chunk_id = row.get("chunk_id")
        digest = row.get("content_hash")
        if not isinstance(chunk_id, str) or not isinstance(digest, str):
            return None
        chunk_ids.append(chunk_id)
        content_hashes.append(digest)
    if not chunk_ids:
        return None
    return chunk_ids, content_hashes


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
