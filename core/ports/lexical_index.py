"""Порт лексического индекса — ``LexicalIndexPort`` (§3.2, I16).

Симметрия профиля (I16): один и тот же движок/параметры для всех продуктов когорты.
Гибрид (RRF) собирается в ``pipeline/evaluate.py`` из этого порта и ``VectorIndexPort`` —
фьюжн не спрятан в адаптер, чтобы профиль R0/R1/R2 был явным и логируемым (02 §B.3).

Transitional-типы — см. ``ports/vector_index.py``. Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.ports.vector_index import RetrievedChunk


@runtime_checkable
class LexicalIndexPort(Protocol):
    """BM25-поиск по продукт-партициям. Дефолт — tantivy; baseline — in-process BM25."""

    engine_version: str

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]: ...
