"""Порт операционного хранилища — ``StorePort`` / ``GateStore`` (§4.4, 02 §B, S6, S9).

Единая SQLite-БД прогона (``runs/{run_id}/cohound.db``) — вариант «отдельная корпусная БД»
отвергнут (S6): FK между ``cell_evidence.source_id`` и ``corpus_chunks.chunk_id`` сохраняются.

Контракт описывает полную поверхность доступа из 03 §B.1 (T3.1). Реализация —
``core.store.sqlite_store.SqliteStore``. Оба протокола структурные
(``typing.Protocol``) и ``runtime_checkable``: проверка — по наличию методов, без
наследования. Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Protocol, runtime_checkable

from core.domain import (
    Cell,
    CellEvidence,
    CorpusChunk,
    CorpusCoverage,
    CorpusDoc,
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
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


@dataclass(frozen=True)
class RunIndexRef:
    """Ссылка на индексную сборку прогона из строки ``runs`` (P4b, нейтрально, §13/I17).

    Несёт ровно то, что нужно пост-хок команде эксперимента, чтобы найти **зафиксированную**
    индексную сборку **без** сканирования каталогов ``index_store/``: объявленный пакет
    (``runs.package_id``) и идентификатор сборки (``runs.index_build_id``; заполняется
    персистенцией манифеста на фазе отчёта). ``None``-поля — значения не выдумываются:
    отсутствие — явное.
    """

    run_id: str
    package_id: str | None = None
    index_build_id: str | None = None


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

        Обновляет версии/индексные/ретривал-поля строки ``runs``,
        ``settings_provenance_json``/``ranking_epsilon`` (провенанс настроек) и
        деплой-провенанс (``deploy_profile``/``edge_schema_version``/``edge_capabilities``,
        07 §7).
        """
        ...

    def set_run_index_build_id(self, run_id: str, index_build_id: str) -> None:
        """Записывает ``runs.index_build_id`` сразу после фазы корпуса (идемпотентно).

        Composition id известен уже на выходе Ш5, а ``persist_manifest`` пишет весь
        манифест только в Ш9 — слишком поздно для resume и для пост-корпусной привязки
        ретривала. Повторная запись того же значения — no-op; неизвестный ``run_id`` —
        no-op (строку прогона создаёт ``create_run``, а не этот метод).
        """
        ...

    def save_plan_inputs(
        self, run_id: str, *, payload: Mapping[str, Any], payload_sha256: str
    ) -> None:
        """Персистит снимок плановых входов прогона (каталог/веса/когорта) в ``run_inputs``.

        Источник истины самодостаточности ``runs/{run_id}`` (P1): пост-хок команды
        восстанавливают каталог без исходного пакета и без side-файла ``plan_inputs.json``.
        ``payload`` — канонический снимок (:func:`core.cli.context._plan_inputs_payload`);
        ``payload_sha256`` — детерминированный дайджест его канонической формы
        (``json.dumps(sort_keys=True)``). Запись идемпотентна (``INSERT OR REPLACE``).
        """
        ...

    def read_plan_inputs(self, run_id: str) -> dict[str, Any] | None:
        """Читает снимок плановых входов прогона; ``None``, если записи нет.

        Возвращает уже разобранный ``payload`` (каноническая форма хранится в
        ``run_inputs.payload_json``); валидацию ``snapshot_version``/``run_id`` выполняет
        вызывающий. Метод не бросает исключений на неизвестный ``run_id``.
        """
        ...

    # -- корпус (Ш5, W7) -------------------------------------------------- #
    def upsert_corpus_doc(self, doc: CorpusDoc) -> None: ...

    def add_chunk(self, chunk: CorpusChunk) -> None: ...

    def add_chunks_batch(self, chunks: list[CorpusChunk]) -> None: ...

    def save_index_manifest(self, entry: IndexManifestEntry) -> None: ...

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

    def set_crawl_session_provenance(self, session_id: str, provenance_json: str) -> None:
        """Записывает ``provenance_json`` сеанса (targeted ``UPDATE``; Phase 2).

        Не задевает счётчики/метаданные сеанса — безопасно для провенанса, вычисленного
        по элементам после финализации.
        """
        ...

    # -- покрытие корпуса (Ф0: гейт ❌) ----------------------------------- #
    def save_corpus_coverage(self, run_id: str, coverage: CorpusCoverage) -> None:
        """Идемпотентный upsert покрытия корпуса по PK ``(run_id, product_id)``.

        Покрытие — производный факт от персистентных элементов краула и записей об
        усечении; повторная запись того же продукта перекрывает прежнюю.
        """
        ...

    def read_corpus_coverage(
        self, run_id: str, product_id: str
    ) -> CorpusCoverage | None:
        """Читает покрытие продукта; ``None``, если записи нет (legacy/без краула).

        ``None`` означает «покрытие неизвестно», а не «полное»: вызывающий обязан
        сохранить прежнее поведение, а не выдумывать ``complete=True``.
        """
        ...

    def iter_corpus_coverage(self, run_id: str) -> Iterator[CorpusCoverage]:
        """Итерирует покрытие прогона; детерминированный порядок ``product_id``."""
        ...

    def iter_corpus_docs(self, run_id: str) -> Iterator[CorpusDoc]:
        """Итерирует документы корпуса прогона (детерминированно: ``product_id, doc_id``).

        Read-only источник per-product doc-delta для ``hound diff`` (09 §12.5): наборы
        ``content_hash`` по продуктам обоих циклов. Схема не меняется. В штатной
        раскладке БД принадлежит одному прогону; в общей ``--db`` таблица
        ``corpus_docs`` — объединение корпусов (ограничение раскладки, не подмена).

        :raises ValueError: прогона ``run_id`` нет в этой БД.
        """
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

    def read_run_index_ref(self, run_id: str) -> RunIndexRef | None:
        """Читает ``runs.package_id``/``runs.index_build_id`` прогона (P4b).

        Возвращает :class:`RunIndexRef` либо ``None``, если прогон неизвестен. Значения
        (пакет/сборка) могут быть ``None``, пока их не записал манифест — это честное
        отсутствие, а не подстановка. Метод не бросает исключений на неизвестный ``run_id``.
        """
        ...

    def iter_index_manifest(self, index_build_id: str) -> Iterator[IndexManifestEntry]:
        """Итерирует продукт-партиции индексной сборки ``index_build_id`` (P4b).

        Зеркало :meth:`save_index_manifest`: пост-хок эксперимент разрешает пакет сборки и её
        ``embed_model_id``/``embed_dim`` из БД, не сканируя ``index_store/``. Порядок
        детерминирован — ``product_id``. Пустой итератор — сборки в БД нет.
        """
        ...

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
