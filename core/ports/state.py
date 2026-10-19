"""coHound — порт durable per-product состояния ``ProductStatePort`` (09 §4.1/§5/§14.3).

Контракт между ``pipeline`` (резолв режима продукта, планирование инкрементального
корпуса, wiring прогона) и хранилищем ``state/{package_id}/state.db`` (реализация —
``core/store/state_store.py``). Порт описывает поверхность, нужную ``resolve_product_modes``
и durable-субстрату документов (MVP-3a: ``url_state``/``document_version``/``chunk_store``/
``chunk_meta``); индексные партиции и cell measurement добавляются следующими срезами по
мере появления потребителей.

Строки — простые dataclass'ы (домен остаётся Pydantic; порт может использовать dataclass).
Временные метки — ISO-8601 строки (как они лежат в SQLite TEXT). Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from core.domain.enums import OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import SeedEntry

__all__ = [
    "ChunkRow",
    "DocumentVersionRow",
    "ProductStatePort",
    "ProductStateRow",
    "RunLineageRow",
    "SeedRow",
    "SnapshotRow",
    "UrlStateRow",
]


@dataclass(frozen=True)
class ProductStateRow:
    """Строка ``product_state``: готовность продукта и рубежи последних измерений.

    ``onboarding_state`` — :class:`OnboardingState`; ``last_*_run_id`` — рубежи §5.1
    (``last_corpus_run_id`` / ``last_evaluated_run_id`` / ``last_completed_run_id``).
    """

    product_id: str
    canonical_name: str
    onboarding_state: OnboardingState
    added_at: str
    added_by: str
    vendor: str | None = None
    onboarding_reason: str | None = None
    re_added_at: str | None = None
    retired_at: str | None = None
    last_corpus_run_id: str | None = None
    last_evaluated_run_id: str | None = None
    last_completed_run_id: str | None = None
    last_completed_cycle_id: str | None = None
    last_attempt_run_id: str | None = None
    last_attempt_status: str | None = None
    snapshot_digest: str | None = None


@dataclass(frozen=True)
class SnapshotRow:
    """Строка ``product_snapshot``: снимок продукта за прогон (после eval/coverage)."""

    snapshot_id: str
    product_id: str
    cycle_id: str
    run_id: str
    status: str
    scope: str
    coverage_complete: bool
    created_at: str
    coverage_digest: str | None = None
    doc_set_digest: str | None = None
    partition_build_id: str | None = None
    docs_total: int = 0
    docs_reused: int = 0
    docs_new: int = 0
    cells_total: int = 0
    cells_carried: int = 0
    cells_measured: int = 0
    cells_pending: int = 0
    baseline_complete: bool = False


@dataclass(frozen=True)
class SeedRow:
    """Строка ``seed_set``: операционное состояние seed'а (authoring — ``seeds.yaml``)."""

    seed_id: str
    product_id: str
    url: str
    normalized_url: str
    role: SeedRole
    source: SeedSource
    state: SeedState
    added_at: str
    added_by: str
    source_ref: str | None = None
    profile_key: str | None = None
    last_verified_at: str | None = None
    retired_at: str | None = None


@dataclass(frozen=True)
class UrlStateRow:
    """Строка ``url_state``: последняя версия документа URL + условная валидация.

    Поля зеркалят DDL §4.2 (порядок колонок сохранён, кроме обязательных раньше
    опциональных). ``reuse_kind`` — :class:`ReuseKind` в виде строки (порт не навязывает
    доменный enum хранилищу). ``seed_id`` — из какого seed'а обнаружен URL.
    ``next_check_at`` хранится, но в этом срезе планировщик его **не читает** (§6.3).
    """

    product_id: str
    url: str
    normalized_url: str
    doc_version_id: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    last_http_status: int | None = None
    last_checked_at: str | None = None
    last_changed_at: str | None = None
    next_check_at: str | None = None
    consecutive_failures: int = 0
    last_error_kind: str | None = None
    reuse_kind: str | None = None
    seed_id: str | None = None


@dataclass(frozen=True, kw_only=True)
class DocumentVersionRow:
    """Строка ``document_version``: immutable-версия документа (§4.2/§9).

    Поля идут в порядке DDL §4.2 (``kw_only`` снимает ограничение dataclass на
    «обязательные раньше опциональных»). Уникальность —
    ``UNIQUE (product_id, content_hash, extractor_version, level)``; ``doc_version_id``
    детерминированно выводится из этой же четвёрки
    (:func:`core.pipeline.incremental.doc_version_id`), поэтому PK и UNIQUE не
    расходятся. ``raw_path`` может быть ``NULL`` после retention.
    """

    doc_version_id: str
    doc_id: str
    product_id: str
    url: str
    content_hash: str
    extractor_version: str
    level: str
    raw_path: str | None = None
    chunker_version: str | None = None
    chunk_ids_digest: str | None = None
    first_seen_run_id: str
    last_seen_run_id: str
    first_seen_at: str
    last_seen_at: str
    status: str


@dataclass(frozen=True)
class ChunkRow:
    """Строка ``chunk_meta``: метаданные чанка документа (§4.2).

    Текст лежит отдельно в ``chunk_store`` по ``content_hash`` (I19): один и тот же
    текст может разделяться несколькими документами. ``chunk_id`` — идентификатор
    durable-субстрата (:func:`core.pipeline.incremental.chunk_row_id`), это **не**
    run-level ``chunk_id`` вида ``product:doc_hash:no``.
    """

    chunk_id: str
    doc_version_id: str
    product_id: str
    chunk_no: int
    content_hash: str
    chunker_version: str
    heading_path: str | None = None
    context_prefix: str | None = None
    lang: str | None = None


@dataclass(frozen=True)
class RunLineageRow:
    """Строка ``run_lineage``: провенанс прогона в цепочке циклов (§12.3).

    ``products`` — состав когорты прогона (``products_json``). ``None`` — состав
    неизвестен (legacy-строка без колонки), пустой список — прогон без продуктов.
    ``refresh_policy`` — политика наблюдения, с которой запускался прогон.
    """

    run_id: str
    package_id: str
    cycle_id: str
    cohort_digest: str
    seeds_digest: str
    execution_contract_digest: str
    change_class: str
    status: str
    started_at: str
    previous_run_id: str | None = None
    completed_at: str | None = None
    error: str | None = None
    products: list[str] | None = None
    refresh_policy: str | None = None


@runtime_checkable
class ProductStatePort(Protocol):
    """Durable per-product состояние (``state/{package_id}/state.db``, 09 §4.2).

    Идемпотентность: ``upsert_*``/``record_*`` — ``INSERT ... ON CONFLICT DO UPDATE``;
    повторный вызов с теми же данными не создаёт дублей. ``reconcile_seed_set`` не удаляет
    строки: seed, отсутствующий в авторском файле, становится ``disabled`` + ``retired_at``.
    """

    # -- продукт --------------------------------------------------------- #
    def get_product_state(self, product_id: str) -> ProductStateRow | None:
        """Строка продукта или ``None`` (отсутствие — нормально для нового продукта)."""
        ...

    def upsert_product_state(self, row: ProductStateRow) -> None:
        """Создаёт/обновляет строку продукта (идемпотентно)."""
        ...

    # -- снимки ---------------------------------------------------------- #
    def last_snapshot(
        self, product_id: str, *, scope: str = "full"
    ) -> SnapshotRow | None:
        """Последний снимок продукта в заданном ``scope`` (``full``|``product_only``)."""
        ...

    def record_snapshot(self, row: SnapshotRow) -> None:
        """Записывает снимок продукта (идемпотентно по ``snapshot_id``)."""
        ...

    # -- seeds ----------------------------------------------------------- #
    def reconcile_seed_set(
        self, product_id: str, seeds: Sequence[SeedEntry]
    ) -> None:
        """Синхронизирует авторский seed-набор → ``seed_set`` (upsert; без удаления)."""
        ...

    def seed_rows(self, product_id: str) -> list[SeedRow]:
        """Все строки seed'ов продукта, включая ``disabled``/``invalid``."""
        ...

    # -- URL/document/chunk (MVP-3a: durable субстрат, 09 §4.2/§6.3) ---- #
    def get_url_state(
        self, product_id: str, normalized_url: str
    ) -> UrlStateRow | None:
        """Состояние URL продукта или ``None`` (URL ещё не наблюдался)."""
        ...

    def iter_url_state(self, product_id: str) -> Iterable[UrlStateRow]:
        """Все строки ``url_state`` продукта в порядке ``normalized_url``."""
        ...

    def upsert_url_state(self, row: UrlStateRow) -> None:
        """Создаёт/обновляет состояние URL (идемпотентно по ``(product_id, normalized_url)``)."""
        ...

    def get_document_version(self, doc_version_id: str) -> DocumentVersionRow | None:
        """Версия документа по ``doc_version_id`` (``None`` — нет)."""
        ...

    def find_document_version(
        self,
        product_id: str,
        *,
        content_hash: str,
        extractor_version: str,
        level: str,
    ) -> DocumentVersionRow | None:
        """Версия по ключу §4.2 ``UNIQUE (product_id, content_hash, extractor_version, level)``."""
        ...

    def upsert_document_version(self, row: DocumentVersionRow) -> None:
        """Создаёт/обновляет версию документа (идемпотентно по ``doc_version_id``)."""
        ...

    def iter_document_versions(
        self, product_id: str, *, status: str = "active"
    ) -> Iterable[DocumentVersionRow]:
        """Версии продукта с заданным ``status`` (по умолчанию только ``active``)."""
        ...

    def put_chunk_text(
        self, content_hash: str, text: str, lang: str | None = None
    ) -> None:
        """Кладёт текст чанка в ``chunk_store`` (идемпотентно по ``content_hash``).

        Текст иммутабелен относительно ``content_hash``: повторная запись обновляет
        только ``lang``, но не текст.
        """
        ...

    def get_chunk_text(self, content_hash: str) -> str | None:
        """Текст чанка по ``content_hash`` (``None`` — текст не сохранён/вытеснен)."""
        ...

    def replace_doc_chunks(
        self, doc_version_id: str, chunks: Sequence[ChunkRow]
    ) -> None:
        """Атомарно заменяет набор ``chunk_meta`` документа (delete + insert).

        Текст должен быть заранее положен в ``chunk_store`` (:meth:`put_chunk_text`).
        Тексты, на которые ссылаются другие документы, не удаляются.
        """
        ...

    def iter_doc_chunks(self, doc_version_id: str) -> Iterable[ChunkRow]:
        """Чанки документа в порядке ``chunk_no`` (детерминированно)."""
        ...

    # -- lineage --------------------------------------------------------- #
    def record_run_lineage(self, row: RunLineageRow) -> None:
        """Записывает/обновляет строку lineage прогона (идемпотентно по ``run_id``)."""
        ...

    def previous_run(self, package_id: str) -> str | None:
        """``run_id`` последнего известного прогона пакета (``None`` — истории нет)."""
        ...

    def get_run_lineage(self, run_id: str) -> RunLineageRow | None:
        """Строка lineage по ``run_id`` или ``None`` (например, ``--since-run``)."""
        ...

    def previous_lineage(self, package_id: str) -> RunLineageRow | None:
        """Последняя строка lineage пакета (``None`` — истории нет)."""
        ...
