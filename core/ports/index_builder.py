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
    """Запись индексной сборки (`.npy`-шарды + лексический индекс) по продукт-партициям.

    ``vector_store`` и ``lexical_engine_version`` — **часть контракта сборки**, а не
    диагностика: оба входят в ``partition_build_id`` (``pb_``, §13:676) и в манифест
    партиции, поэтому обязаны честно описывать то, чем сборка реально произведена
    (второй, физический контроль переиспользования — 09 §8.5). ``runtime_checkable``
    проверяет лишь **наличие** атрибутов, а не их правдивость: пустое
    ``lexical_engine_version`` допустимо только когда лексического движка в прогоне нет
    (R0/R2 пропускаются), а не когда версия «неизвестна».
    """

    #: Метка физического векторного хранилища (например, ``"npy-fp16"``).
    vector_store: str
    #: Версия лексического движка, обслуживающего R0/R2 (например, ``"tantivy-0.26.2"``).
    lexical_engine_version: str

    def build(
        self,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        meta: IndexMeta,
    ) -> str: ...

    def commit(self) -> None: ...
