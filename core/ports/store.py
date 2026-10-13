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
    CrawlAttempt,
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
    FetchStatus,
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

    def persist_manifest(self, run_id: str, manifest: RunManifest) -> None:
        """Персистит финальный манифест прогона в операционную БД (R7/§13, A3).

        Обновляет версии/индексные/ретривал-поля строки ``runs`` и ``settings_provenance_json``
        из манифеста (в т.ч. ``ranking_epsilon`` в составе провенанса настроек).
        """
        ...

    # -- корпус (Ш5, W7) -------------------------------------------------- #
    def upsert_corpus_doc(self, doc: CorpusDoc) -> None: ...

    def add_chunk(self, chunk: CorpusChunk) -> None: ...

    def add_chunks_batch(self, chunks: list[CorpusChunk]) -> None: ...

    def save_index_manifest(self, entry: IndexManifestEntry) -> None: ...

    # -- краул (D3: durable audit/queue/resume) --------------------------- #
    def upsert_crawl_attempt(self, attempt: CrawlAttempt) -> None:
        """Полный upsert строки ``crawl_attempts`` по PK ``(run_id, product_id, url)``."""
        ...

    def mark_crawl_status(
        self,
        run_id: str,
        product_id: str,
        url: str,
        status: FetchStatus,
        *,
        error_kind: str | None = None,
        error: str | None = None,
        content_hash: str | None = None,
        raw_path: str | None = None,
        http_status: int | None = None,
        content_type: str | None = None,
        lang: str | None = None,
        fetch_mode: str | None = None,
        extract_route: str | None = None,
        strategy: str | None = None,
        ts: str | None = None,
    ) -> None:
        """Частичный upsert статуса per-URL записи краула (идемпотентный).

        Сохраняет ``first_seen`` (проставляется один раз), обновляет ``last_attempt``
        (``ts`` или текущее время), инкрементирует ``attempts`` на новом этапе-исходе
        (``fetched``/``done``/``failed`` при смене статуса; повтор ``done`` идемпотентен).
        Переданные (не ``None``) поля перекрывают текущие; прочие сохраняются.
        """
        ...

    def iter_crawl_attempts(
        self,
        run_id: str,
        product_id: str | None = None,
        status: FetchStatus | None = None,
    ) -> Iterator[CrawlAttempt]:
        """Итерирует записи краула прогона; порядок ``product_id, url``; фильтры опциональны."""
        ...

    def clear_crawl_attempts(self, run_id: str, product_id: str | None = None) -> None:
        """Удаляет durable-состояние краула прогона (все или по продукту) — ``--reset``."""
        ...

    # -- сеанс краула (Lane B: durable намерение + прогресс) --------------- #
    def upsert_crawl_session(self, session: CrawlSession) -> None:
        """Upsert строки ``crawl_sessions`` по PK ``session_id`` (полная запись)."""
        ...

    def get_crawl_session(self, session_id: str) -> CrawlSession | None:
        """Возвращает сеанс краула или ``None``, если он неизвестен."""
        ...

    def update_crawl_session_status(
        self,
        session_id: str,
        status: CrawlSessionStatus,
        *,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        """Частичный upsert жизненного цикла сеанса (идемпотентный).

        Переданные (не ``None``) поля перекрывают текущие; прочие сохраняются.
        """
        ...

    def upsert_crawl_session_item(self, item: CrawlSessionItem) -> None:
        """Upsert элемента сеанса по PK ``(session_id, product_id, url)``.

        Атомарно (в одной транзакции) пишет строку элемента **и** пересчитывает/персистит
        агрегированные счётчики сеанса — прогресс не имеет разрывов.
        """
        ...

    def iter_crawl_session_items(
        self,
        session_id: str,
        *,
        product_id: str | None = None,
        status: CrawlItemStatus | None = None,
    ) -> Iterator[CrawlSessionItem]:
        """Итерирует элементы сеанса; порядок ``product_id, url``; фильтры опциональны."""
        ...

    def crawl_session_counts(self, session_id: str) -> dict[str, int]:
        """Число элементов сеанса по каждому статусу ``CrawlItemStatus`` (отсутствующие — 0)."""
        ...

    def recover_crawl_session(self, session_id: str) -> int:
        """Recovery-on-open: ``running`` → ``pending`` и пересчёт счётчиков (одна транзакция).

        Возвращает число восстановленных элементов.
        """
        ...

    def clear_crawl_session(self, session_id: str) -> None:
        """Удаляет элементы сеанса, затем сам сеанс (одна транзакция)."""
        ...

    # -- чтение ---------------------------------------------------------- #
    def iter_cells(self, run_id: str, state: str | None = None) -> Iterator[Cell]: ...

    def get_chunk(self, chunk_id: str) -> Chunk | None: ...

    def iter_chunks(
        self, build_id: str, *, product_id: str | None = None
    ) -> Iterator[Chunk]:
        """Итерирует чанки индексной сборки ``build_id`` как порт-типы ``Chunk``.

        Нужен для seed/rebuild индекса в возобновлённом процессе (resume): чанки
        восстанавливаются из ``corpus_chunks`` по ``build_id``. Порядок
        детерминирован: ``product_id, chunk_no, chunk_id``. ``product_id=None`` —
        все продукты сборки; иначе — только указанная продукт-партиция. Поля ``url``
        нет (как и в ``get_chunk``): URL резолвится отдельно
        (``resolve_source_url``/``resolve``).
        """
        ...

    def resolve_source_url(self, source_id: str) -> str | None: ...

    def read_manifest(self, run_id: str) -> RunManifest: ...

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...

    def iter_exhaustions(self, run_id: str) -> Iterator[ProtocolExhaustion]:
        """Итерирует машиночитаемые протоколы исчерпания прогона (§5/S13).

        Вход для :func:`core.domain.invariants.check_unknown_exhaustion` (I3): каждая
        запись несёт перечень пройденных контуров и ``termination_reason``.
        """
        ...


@runtime_checkable
class GateStore(Protocol):
    """Персистенция и чтение человеческих решений G1–G4 (аудит-след, I6/I7, S9).

    Совпадает по методам с ``StorePort`` (хранилище реализует оба протокола), но
    выделен отдельно: гейт не считается пройденным без строки в ``gate_decisions``.
    """

    def save_gate_decision(self, d: GateDecision) -> None: ...

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]: ...
