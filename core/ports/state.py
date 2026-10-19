"""coHound — порт durable per-product состояния ``ProductStatePort`` (09 §4.1/§5/§14.3).

Контракт между ``pipeline`` (резолв режима продукта, wiring прогона) и хранилищем
``state/{package_id}/state.db`` (реализация — ``core/store/state_store.py``). Порт
описывает **минимальную** поверхность, нужную ``resolve_product_modes`` и сборке прогона;
расширение (URL/document-версии, индексные партиции, cell measurement) добавляется
следующими срезами по мере появления потребителей.

Строки — простые dataclass'ы (домен остаётся Pydantic; порт может использовать dataclass).
Временные метки — ISO-8601 строки (как они лежат в SQLite TEXT). Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from core.domain.enums import OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import SeedEntry

__all__ = [
    "ProductStatePort",
    "ProductStateRow",
    "RunLineageRow",
    "SeedRow",
    "SnapshotRow",
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
class RunLineageRow:
    """Строка ``run_lineage``: провенанс прогона в цепочке циклов (§12.3)."""

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

    # -- lineage --------------------------------------------------------- #
    def record_run_lineage(self, row: RunLineageRow) -> None:
        """Записывает/обновляет строку lineage прогона (идемпотентно по ``run_id``)."""
        ...

    def previous_run(self, package_id: str) -> str | None:
        """``run_id`` последнего известного прогона пакета (``None`` — истории нет)."""
        ...
