"""Порт сборщика индекса — ``IndexBuilderPort`` (§3.2, 02 §B.1, K3).

**Единственный** писатель ``.npy``-шардов и tantivy; ``pipeline`` не работает с файлами
индекса напрямую. ``build()`` возвращает иммутабельный ``build_id``; ``commit()`` фиксирует
запись атомарно (существующая сборка не мутируется, §13). Нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, Sequence, runtime_checkable

from core.ports.types import Chunk


@dataclass(frozen=True)
class IndexMeta:
    """Метаданные индексной сборки (§13): версии, влияющие на ``build_id``."""

    package: str
    chunker_version: str
    embed_model_id: str
    snapshot_date: str
    product_id: str
    extra: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class IndexBuilderPort(Protocol):
    """Запись индексной сборки (`.npy`-шарды + лексический индекс) по продукт-партициям."""

    def build(
        self,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        meta: IndexMeta,
    ) -> str: ...

    def commit(self) -> None: ...
