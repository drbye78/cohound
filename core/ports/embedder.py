"""Порт эмбеддера — ``EmbedderPort`` (§3.2, Ш5).

Локальная модель (bge-m3 / multilingual-e5), батч. Модель не покидает контур; ``model_id``
пишется в манифест (§13). Порт **не** импортирует numpy/torch — вектора передаются как
последовательности чисел (адаптер внутри может использовать numpy). Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable


@runtime_checkable
class EmbedderPort(Protocol):
    """Текст → вектор. Возвращает ``N × dim`` (список векторов фиксированной длины)."""

    model_id: str
    dim: int

    def embed(self, texts: Sequence[str]) -> list[list[float]]: ...
