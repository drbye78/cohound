"""Композиция индексных партиций — вид когорты и её иммутабельный манифест (09 §8.1–§8.4).

В целевой раскладке у индекса **два** идентификатора:

* ``partition_build_id`` (``pb_...``) — сборка одной продукт-партиции;
* ``composition_id`` (``cb_...``) — композиция партиций когорты; это ``runs.index_build_id``.

**MVP-4 шаг 2.** Формулы разделены: ``partition_build_id`` выводит
:func:`core.pipeline.corpus._derive_partition_build_id` (per-product, без
``params_token``/``snapshot_date``), а ``composition_id`` — :func:`derive_composition_id`.
:func:`legacy_view` остаётся только для чтения раскладки, созданной **до** композиций
(``index_store/{package}/{build}/{product}/`` как одна композиция); новые сборки его не
используют.

Слой ``pipeline``: модуль импортирует только ``util`` и stdlib, не ``adapters``.
Нейтрален (§1.3).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from core.util.atomic_write import atomic_write_text
from core.util.hashing import short_hash
from core.util.index_layout import COMPOSITIONS_DIR

__all__ = [
    "CompositionView",
    "ImmutableCompositionError",
    "derive_composition_id",
    "legacy_view",
    "composition_path",
    "write_composition",
    "read_composition",
]

#: Схема входов деривации id (09 §8.1): меняется только вместе с формулой.
_COMPOSITION_SCHEMA = "composition-1"

#: Схема файла манифеста композиции (09 §8.2).
_COMPOSITION_FILE_SCHEMA = "composition/1"


class ImmutableCompositionError(RuntimeError):
    """Попытка изменить существующий манифест композиции (K2/§13, 09 §8.6).

    Композиция — только создание: другой состав при том же ``composition_id`` означает
    расхождение id-формулы и данных, а не повод перезаписать файл.
    """


@dataclass(frozen=True)
class CompositionView:
    """Вид композиции: ``composition_id`` + отображение ``product_id → partition_build_id``.

    ``members`` копируется в неизменяемое представление: вид — снимок состава, а не
    окно в чужой mutable-dict.
    """

    composition_id: str
    members: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "members", MappingProxyType(dict(self.members)))

    def build_id_for(self, product_id: str) -> str:
        """``partition_build_id`` продукта.

        :raises KeyError: продукт не входит в композицию — это не «пустая партиция»,
            а неверный запрос (I17), поэтому ошибка явная.
        """
        try:
            return self.members[product_id]
        except KeyError:
            raise KeyError(
                f"продукт {product_id!r} не входит в композицию {self.composition_id!r}"
            ) from None

    def product_ids(self) -> list[str]:
        """Продукты композиции в детерминированном (лексикографическом) порядке."""
        return sorted(self.members)

    @property
    def is_empty(self) -> bool:
        """Пустая композиция (нет ни одной продукт-партиции)."""
        return not self.members


def derive_composition_id(
    package_id: str,
    members: Mapping[str, str],
    digests: Mapping[str, str] | None = None,
) -> str:
    """``composition_id = "cb_" + short_hash(schema, package, sorted triples)`` (09 §8.1).

    Триплет — ``(product_id, partition_build_id, content_digest)``; отсутствующий дайджест
    даёт пустую строку. Сортировка делает id независимым от порядка ``members``.
    Детерминирован для одинакового состава и одинаковых версий/дайджестов.
    """
    resolved_digests = digests or {}
    triples = sorted(
        (product_id, partition_build_id, str(resolved_digests.get(product_id, "")))
        for product_id, partition_build_id in members.items()
    )
    raw = json.dumps(
        {
            "schema": _COMPOSITION_SCHEMA,
            "package": package_id,
            "members": triples,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return "cb_" + short_hash(raw)


def legacy_view(build_id: str, product_ids: Sequence[str]) -> CompositionView:
    """Shim прежней раскладки: один верхнеуровневый каталог = одна композиция.

    ``partition_build_id := build_id`` для каждого продукта, ``composition_id := build_id``.
    Так читаются сборки, созданные **до** появления композиций. Новые сборки всегда несут
    ``_compositions/{composition_id}.json`` и в этот путь не попадают.
    """
    return CompositionView(
        composition_id=build_id,
        members={product_id: build_id for product_id in product_ids},
    )


def composition_path(
    index_root: str | Path, package_id: str, composition_id: str
) -> Path:
    """Путь манифеста: ``index_store/{package}/_compositions/{composition_id}.json``."""
    return Path(index_root) / package_id / COMPOSITIONS_DIR / f"{composition_id}.json"


def write_composition(
    index_root: str | Path,
    package_id: str,
    view: CompositionView,
    *,
    created_at: str,
    digests: Mapping[str, str] | None = None,
) -> Path:
    """Пишет манифест композиции атомарно; повтор с тем же составом — no-op.

    Иммутабельность (09 §8.6): существующий файл с **другим** составом/дайджестами
    отвергается :class:`ImmutableCompositionError`; идентичный состав не перезаписывается
    (mtime/байты не меняются). ``created_at`` входит в файл, но не в сравнение состава:
    повторная запись того же состава с новой меткой — всё ещё no-op.
    """
    if not view.composition_id:
        raise ValueError("composition_id обязан быть непустым")
    resolved_digests = digests or {}
    rows = [
        {
            "product_id": product_id,
            "partition_build_id": view.build_id_for(product_id),
            "content_digest": str(resolved_digests.get(product_id, "")),
        }
        for product_id in view.product_ids()
    ]
    path = composition_path(index_root, package_id, view.composition_id)

    if path.is_file():
        existing = _read_payload(path)
        stored_id = existing.get("composition_id")
        if stored_id not in (None, view.composition_id):
            raise ImmutableCompositionError(
                f"файл {path} уже принадлежит композиции {stored_id!r}, "
                f"а не {view.composition_id!r}"
            )
        if _same_members(existing, package_id, rows):
            return path
        raise ImmutableCompositionError(
            f"композиция {view.composition_id!r} уже зафиксирована в {path} с другим "
            "составом/дайджестами: правка на месте запрещена (09 §8.6, K2/§13)"
        )

    payload = {
        "schema": _COMPOSITION_FILE_SCHEMA,
        "composition_id": view.composition_id,
        "package": package_id,
        "created_at": created_at,
        "members": rows,
    }
    atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
    )
    return path


def read_composition(
    index_root: str | Path, package_id: str, composition_id: str
) -> CompositionView | None:
    """Читает манифест композиции; отсутствие — ``None``, порча — явная ошибка.

    :raises ValueError: файл есть, но не соответствует схеме ``composition/1``
        (битый JSON, чужой ``composition_id``/пакет, дубликат продукта).
    """
    path = composition_path(index_root, package_id, composition_id)
    if not path.is_file():
        return None
    payload = _read_payload(path)
    if payload.get("schema") != _COMPOSITION_FILE_SCHEMA:
        raise ValueError(
            f"композиция {path}: ожидается схема {_COMPOSITION_FILE_SCHEMA!r}, "
            f"получено {payload.get('schema')!r}"
        )
    stored_id = payload.get("composition_id")
    if stored_id != composition_id:
        raise ValueError(
            f"композиция {path}: composition_id {stored_id!r} не совпадает с именем "
            f"файла {composition_id!r}"
        )
    stored_package = payload.get("package")
    if stored_package is not None and stored_package != package_id:
        raise ValueError(
            f"композиция {path}: пакет {stored_package!r} не совпадает с {package_id!r}"
        )
    members_raw = payload.get("members")
    if not isinstance(members_raw, list):
        raise ValueError(f"композиция {path}: members обязан быть списком")

    members: dict[str, str] = {}
    for item in members_raw:
        if not isinstance(item, dict):
            raise ValueError(f"композиция {path}: member обязан быть объектом")
        product_id = item.get("product_id")
        partition_build_id = item.get("partition_build_id")
        if not isinstance(product_id, str) or not product_id:
            raise ValueError(f"композиция {path}: member без product_id")
        if not isinstance(partition_build_id, str) or not partition_build_id:
            raise ValueError(
                f"композиция {path}: member {product_id!r} без partition_build_id"
            )
        if product_id in members:
            raise ValueError(f"композиция {path}: дубликат продукта {product_id!r}")
        members[product_id] = partition_build_id

    return CompositionView(composition_id=composition_id, members=members)


def _read_payload(path: Path) -> dict[str, object]:
    """Читает JSON-объект манифеста (порча → ``ValueError`` с именем файла)."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"композиция {path}: не читается как JSON ({exc})") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"композиция {path}: ожидается JSON-объект")
    return payload


def _same_members(
    payload: Mapping[str, object], package_id: str, rows: Sequence[Mapping[str, str]]
) -> bool:
    """Совпадает ли состав существующего файла с новым (без учёта ``created_at``)."""
    if payload.get("schema") != _COMPOSITION_FILE_SCHEMA:
        return False
    if payload.get("package") not in (None, package_id):
        return False
    existing = payload.get("members")
    if not isinstance(existing, list) or not all(isinstance(item, dict) for item in existing):
        return False
    existing_triples = sorted(
        (
            str(item.get("product_id", "")),
            str(item.get("partition_build_id", "")),
            str(item.get("content_digest", "")),
        )
        for item in existing
    )
    new_triples = sorted(
        (row["product_id"], row["partition_build_id"], row["content_digest"])
        for row in rows
    )
    return existing_triples == new_triples
