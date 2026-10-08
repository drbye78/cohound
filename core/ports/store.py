"""Порт операционного хранилища — ``StorePort`` / ``GateStore`` (§4.4, 02 §B, S6, S9).

Единая SQLite-БД прогона (``runs/{run_id}/cohound.db``) — вариант «отдельная корпусная БД»
отвергнут (S6): FK между ``cell_evidence.source_id`` и ``corpus_chunks.chunk_id`` сохраняются.

Контракт описывает полную поверхность доступа из 03 §B.1 (T3.1). Реализация —
``core.store.sqlite_store.SqliteStore``. Оба протокола структурные
(``typing.Protocol``) и ``runtime_checkable``: проверка — по наличию методов, без
наследования. Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Any, Iterable, Iterator, Protocol, runtime_checkable

from core.domain import (
    Cell,
    CellEvidence,
    CorpusChunk,
    CorpusDoc,
    GateDecision,
    IndexManifestEntry,
    ProtocolExhaustion,
    RegistryFact,
    RunConfig,
    RunManifest,
    ScenarioBinding,
    SearchLogEntry,
)
from core.ports.types import Chunk


@runtime_checkable
class StorePort(Protocol):
    """Операционное хранилище прогона. Транзакционность; WAL; батчевые записи (§B.2)."""

    # -- запись ---------------------------------------------------------- #
    def upsert_products(self, products: Iterable[Any]) -> None: ...

    def create_run(self, run_id: str, cfg: RunConfig) -> None: ...

    def set_phase_state(self, run_id: str, phase: str, state: str) -> None:
        """Фиксирует состояние фазы прогона (durable-носитель resume, 02 §A.1).

        Состояния фаз аккумулируются, а не перетирают друг друга: реализация обязана
        сохранять карту ``{phase: state}`` целиком.
        """
        ...

    def get_phase_state(self, run_id: str) -> dict[str, str]:
        """Возвращает карту ``{phase: "ok"|"blocked"|"failed"}`` прогона для resume.

        Пустой словарь — если прогон неизвестен, состояние не записано или повреждено
        (legacy-формат/невалидный JSON). Метод не бросает исключений на неизвестный ``run_id``.
        """
        ...

    def save_cell(self, cell: Cell) -> None: ...

    def save_cells_batch(self, cells: list[Cell], chunk_size: int = 200) -> None: ...

    def add_evidence(self, ev: CellEvidence) -> None: ...

    def add_evidence_batch(self, evs: list[CellEvidence]) -> None: ...

    def log_search(self, entry: SearchLogEntry) -> None: ...

    def save_exhaustion(self, pe: ProtocolExhaustion) -> None: ...

    def save_gate_decision(self, d: GateDecision) -> None: ...

    def save_registry_fact(self, rf: RegistryFact) -> None: ...

    def save_llm_scenarios(self, bindings: list[ScenarioBinding]) -> None: ...

    # -- корпус (Ш5, W7) -------------------------------------------------- #
    def upsert_corpus_doc(self, doc: CorpusDoc) -> None: ...

    def add_chunk(self, chunk: CorpusChunk) -> None: ...

    def add_chunks_batch(self, chunks: list[CorpusChunk]) -> None: ...

    def save_index_manifest(self, entry: IndexManifestEntry) -> None: ...

    # -- чтение ---------------------------------------------------------- #
    def iter_cells(self, run_id: str, state: str | None = None) -> Iterator[Cell]: ...

    def get_chunk(self, chunk_id: str) -> Chunk | None: ...

    def resolve_source_url(self, source_id: str) -> str | None: ...

    def read_manifest(self, run_id: str) -> RunManifest: ...

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...


@runtime_checkable
class GateStore(Protocol):
    """Персистенция и чтение человеческих решений G1–G4 (аудит-след, I6/I7, S9).

    Совпадает по методам с ``StorePort`` (хранилище реализует оба протокола), но
    выделен отдельно: гейт не считается пройденным без строки в ``gate_decisions``.
    """

    def save_gate_decision(self, d: GateDecision) -> None: ...

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...
