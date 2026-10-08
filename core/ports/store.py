"""Порт операционного хранилища — ``StorePort`` (§4.4, 02 §B, S6).

Единая SQLite-БД прогона (``runs/{run_id}/cohound.db``) — вариант «отдельная корпусная БД»
отвергнут (S6): FK между ``cell_evidence.source_id`` и ``corpus_chunks.chunk_id`` сохраняются.

Transitional: до добавления новых моделей (T2.1) порт описывает методы над существующими
типами; методы для ``RunManifest``/``SearchLogEntry``/``ProtocolExhaustion``/``ScenarioBinding``
добавляются после T2.1. Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Iterator, Protocol, runtime_checkable

from core.domain import Cell, CellEvidence, RunConfig
from core.ports.types import Chunk


@runtime_checkable
class StorePort(Protocol):
    """Операционное хранилище прогона. Транзакционность; WAL; батчевые записи (§B.2)."""

    # -- запись ---------------------------------------------------------- #
    def create_run(self, run_id: str, cfg: RunConfig) -> None: ...

    def set_phase_state(self, run_id: str, phase: str, state: str) -> None: ...

    def save_cell(self, cell: Cell) -> None: ...

    def save_cells_batch(self, cells: list[Cell]) -> None: ...

    def add_evidence(self, ev: CellEvidence) -> None: ...

    # -- чтение ---------------------------------------------------------- #
    def iter_cells(self, run_id: str, state: str | None = None) -> Iterator[Cell]: ...

    def get_chunk(self, chunk_id: str) -> Chunk | None: ...

    def resolve_source_url(self, source_id: str) -> str | None: ...
