"""Стабильные идентификаторы и их разбор: ``source_id`` ↔ ``SourceRef``, slug-валидация.

Инварианты I10/I15: ``source_id`` кодирует ``product_id`` + ``doc_id`` + ``chunk_no``
(и, опционально, привязку к ``build_id`` — S5). Идентификатор детерминированно
резолвится в URL без сети (см. ``core/domain/anchors.py``).

Модуль категорийно-нейтрален (§1.3), зависимостей на ядро не имеет.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Слаг — стабильный якорь данных (§2.4): только [a-z0-9_], непустой.
_SLUG_RE = re.compile(r"^[a-z0-9_]+$")
# Номер чанка — неотрицательное целое в десятичной записи (без знаков/пробелов).
_CHUNK_RE = re.compile(r"^[0-9]+$")

__all__ = ["SourceRef", "parse_source_id", "source_id", "validate_slug"]


@dataclass(frozen=True)
class SourceRef:
    """Разобранный ``source_id``: продукт, документ, номер чанка и (опц.) сборка."""

    product_id: str
    doc_id: str
    chunk_no: int
    build_id: str | None = None


def source_id(
    product_id: str, doc_id: str, chunk_no: int, build_id: str | None = None
) -> str:
    """Собирает стабильный `source_id` вида ``product:doc:chunk_no[:build_id]``.

    Формат фиксирован: ``f"{product_id}:{doc_id}:{chunk_no}"``; при непустом
    ``build_id`` добавляется ``f":{build_id}"`` (привязка к индексной сборке, S5).
    """
    base = f"{product_id}:{doc_id}:{chunk_no}"
    if build_id:
        return f"{base}:{build_id}"
    return base


def parse_source_id(source_id: str) -> SourceRef:
    """Разбирает ``source_id`` в :class:`SourceRef`.

    Малформед-вход (не строка, пустой, не 3–4 сегментов, пустые сегменты,
    нецелый номер чанка) → :class:`ValueError`.
    """
    if not isinstance(source_id, str) or not source_id:
        raise ValueError("source_id не может быть пустым")

    parts = source_id.split(":")
    if len(parts) not in (3, 4):
        raise ValueError(
            f"source_id '{source_id}': ожидается 3 или 4 сегмента через ':', "
            f"получено {len(parts)}"
        )

    product_id, doc_id, chunk_raw = parts[0], parts[1], parts[2]
    if not product_id or not doc_id:
        raise ValueError(f"source_id '{source_id}': product_id и doc_id обязательны")
    if not _CHUNK_RE.match(chunk_raw):
        raise ValueError(
            f"source_id '{source_id}': chunk_no '{chunk_raw}' — не неотрицательное целое"
        )

    build_id: str | None = None
    if len(parts) == 4:
        build_id = parts[3]
        if not build_id:
            raise ValueError(f"source_id '{source_id}': build_id не может быть пустым")

    return SourceRef(
        product_id=product_id,
        doc_id=doc_id,
        chunk_no=int(chunk_raw),
        build_id=build_id,
    )


def validate_slug(slug: str) -> str:
    """Проверяет, что ``slug`` — непустая строка из ``[a-z0-9_]``.

    Возвращает ``slug`` без изменений; иначе :class:`ValueError`. Slug — стабильный
    якорь данных (§2.4): ``product_id``, ``doc_id``, ``param_id`` иммутабельны.
    """
    if not isinstance(slug, str) or not slug:
        raise ValueError("slug не может быть пустым")
    if not _SLUG_RE.match(slug):
        raise ValueError(f"slug '{slug}': допустимы только [a-z0-9_]")
    return slug
