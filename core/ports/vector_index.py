"""Порт векторного индекса — ``VectorIndexPort`` (§3.2, I17).

Продукт-скоупед (I17): поиск не выходит за переданные ``product_ids`` (продукт + сателлиты).
``RetrievedChunk`` несёт ``content_hash`` и ``build_id`` — provenance единицы ретривала (R15).

Transitional-импорт типов — см. ``ports/types.py``/``ports/llm.py``. Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class VectorQuery:
    """Векторный запрос в пределах продукт-партиций (I17)."""

    product_ids: list[str]
    top_k: int
    text: str | None = None
    vector: list[float] | None = None


@dataclass(frozen=True)
class RetrievedChunk:
    """Чанк, возвращённый ретривалом, с полным провенансом (S4/S5)."""

    chunk_id: str
    product_id: str
    text: str
    content_hash: str
    heading_path: str
    build_id: str
    score: float


@dataclass(frozen=True)
class RetrievalResult:
    """Результат ретривала: fused top-K, профиль и сборка (I16/I19/I20)."""

    chunks: list[RetrievedChunk] = field(default_factory=list)
    profile: str = ""
    cutoff_K: int = 0
    build_id: str = ""


@runtime_checkable
class CompositionViewPort(Protocol):
    """Структурный контракт вида композиции партиций (09 §8.4).

    Конкретная реализация — :class:`core.pipeline.composition.CompositionView`.
    Порт живёт в ``ports``, чтобы ``adapters`` не импортировали ``pipeline``
    (матрица зависимостей 01 §1). Адаптеру нужны только эти три члена.
    """

    composition_id: str
    members: Mapping[str, str]

    def build_id_for(self, product_id: str) -> str: ...

    def product_ids(self) -> list[str]: ...


@runtime_checkable
class VectorIndexPort(Protocol):
    """Brute-force/ANN поиск по продукт-партиции. Дефолт — ``NpyVectorIndex`` (§3.2)."""

    build_id: str
    dim: int

    def search(self, q: VectorQuery) -> list[RetrievedChunk]: ...

    def shard_ids(self, product_id: str) -> list[str]: ...
