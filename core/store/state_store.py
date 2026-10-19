"""coHound — реализация ``ProductStatePort`` поверх ``state.db`` (09 §4.1/§4.4).

``SqliteStateStore`` — операционное хранилище durable per-product состояния пакета:
``state/{package_id}/state.db`` (схема — ``core/store/state_sqlite.py``). Все записи
идемпотентны (``INSERT ... ON CONFLICT DO UPDATE``), транзакции короткие (одна операция —
одна транзакция, WAL включён схемой), ``close()`` освобождает соединение.

Семантика seed-набора (§5.3): авторский ``sources/seeds.yaml`` — источник истины;
``reconcile_seed_set`` делает upsert по ``(product_id, normalized_url)``, а seed'ы,
отсутствующие в файле, помечает ``state=disabled`` + ``retired_at`` (никогда не удаляет) —
удаление из git обратимо и явно. Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path

from core.config.seeds import seed_id
from core.domain.enums import OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import SeedEntry
from core.ports.state import ProductStateRow, RunLineageRow, SeedRow, SnapshotRow
from core.store.state_sqlite import connect_state

__all__ = ["SqliteStateStore"]

#: Колонки ``product_state`` в порядке записи (кроме ``product_id`` — он ключ).
_PRODUCT_STATE_COLUMNS = (
    "canonical_name",
    "vendor",
    "onboarding_state",
    "onboarding_reason",
    "added_at",
    "added_by",
    "re_added_at",
    "retired_at",
    "last_corpus_run_id",
    "last_evaluated_run_id",
    "last_completed_run_id",
    "last_completed_cycle_id",
    "last_attempt_run_id",
    "last_attempt_status",
    "snapshot_digest",
)

#: Колонки ``product_snapshot`` в порядке записи (кроме ``snapshot_id`` — он ключ).
_SNAPSHOT_COLUMNS = (
    "product_id",
    "cycle_id",
    "run_id",
    "status",
    "scope",
    "coverage_complete",
    "coverage_digest",
    "doc_set_digest",
    "partition_build_id",
    "docs_total",
    "docs_reused",
    "docs_new",
    "cells_total",
    "cells_carried",
    "cells_measured",
    "cells_pending",
    "baseline_complete",
    "created_at",
)

#: Колонки ``run_lineage`` в порядке записи (кроме ``run_id`` — он ключ).
_LINEAGE_COLUMNS = (
    "package_id",
    "cycle_id",
    "previous_run_id",
    "cohort_digest",
    "seeds_digest",
    "execution_contract_digest",
    "change_class",
    "status",
    "started_at",
    "completed_at",
    "error",
    "products_json",
    "refresh_policy",
)


def _now() -> str:
    """Текущее время в ISO-8601 (UTC, ``Z``) — как в остальных TEXT-колонках."""
    return (
        _dt.datetime.now(_dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _upsert_sql(table: str, key: str, columns: Sequence[str]) -> str:
    """Строит ``INSERT ... ON CONFLICT(key) DO UPDATE`` для полного набора колонок."""
    names = ", ".join((key, *columns))
    placeholders = ", ".join("?" for _ in range(len(columns) + 1))
    updates = ", ".join(f"{name} = excluded.{name}" for name in columns)
    return (
        f"INSERT INTO {table} ({names}) VALUES ({placeholders}) "
        f"ON CONFLICT({key}) DO UPDATE SET {updates}"
    )


class SqliteStateStore:
    """``ProductStatePort`` над ``state/{package_id}/state.db`` (09 §4.2).

    :param db_path: путь к ``state.db`` (``":memory:"`` — для тестов). Родительский
        каталог создаёт вызывающий (composition root), а не хранилище.
    """

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.path = Path(db_path)
        self._conn = connect_state(db_path)

    # -- жизненный цикл --------------------------------------------------- #
    @property
    def conn(self) -> sqlite3.Connection:
        """Соединение SQLite (диагностика/тесты; писать через методы хранилища)."""
        return self._conn

    def close(self) -> None:
        """Закрывает соединение (идемпотентно на уровне sqlite3)."""
        self._conn.close()

    def __enter__(self) -> SqliteStateStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- продукт ---------------------------------------------------------- #
    def get_product_state(self, product_id: str) -> ProductStateRow | None:
        row = self._conn.execute(
            "SELECT * FROM product_state WHERE product_id = ?", (product_id,)
        ).fetchone()
        if row is None:
            return None
        return ProductStateRow(
            product_id=row["product_id"],
            canonical_name=row["canonical_name"],
            vendor=row["vendor"],
            onboarding_state=OnboardingState(row["onboarding_state"]),
            onboarding_reason=row["onboarding_reason"],
            added_at=row["added_at"],
            added_by=row["added_by"],
            re_added_at=row["re_added_at"],
            retired_at=row["retired_at"],
            last_corpus_run_id=row["last_corpus_run_id"],
            last_evaluated_run_id=row["last_evaluated_run_id"],
            last_completed_run_id=row["last_completed_run_id"],
            last_completed_cycle_id=row["last_completed_cycle_id"],
            last_attempt_run_id=row["last_attempt_run_id"],
            last_attempt_status=row["last_attempt_status"],
            snapshot_digest=row["snapshot_digest"],
        )

    def upsert_product_state(self, row: ProductStateRow) -> None:
        values = [getattr(row, name) for name in _PRODUCT_STATE_COLUMNS]
        values = [
            value.value if isinstance(value, OnboardingState) else value
            for value in values
        ]
        with self._conn:
            self._conn.execute(
                _upsert_sql("product_state", "product_id", _PRODUCT_STATE_COLUMNS),
                (row.product_id, *values),
            )

    # -- снимки ------------------------------------------------------------ #
    def last_snapshot(
        self, product_id: str, *, scope: str = "full"
    ) -> SnapshotRow | None:
        row = self._conn.execute(
            "SELECT * FROM product_snapshot WHERE product_id = ? AND scope = ? "
            "ORDER BY created_at DESC, run_id DESC LIMIT 1",
            (product_id, scope),
        ).fetchone()
        if row is None:
            return None
        return SnapshotRow(
            snapshot_id=row["snapshot_id"],
            product_id=row["product_id"],
            cycle_id=row["cycle_id"],
            run_id=row["run_id"],
            status=row["status"],
            scope=row["scope"],
            coverage_complete=bool(row["coverage_complete"]),
            coverage_digest=row["coverage_digest"],
            doc_set_digest=row["doc_set_digest"],
            partition_build_id=row["partition_build_id"],
            docs_total=row["docs_total"],
            docs_reused=row["docs_reused"],
            docs_new=row["docs_new"],
            cells_total=row["cells_total"],
            cells_carried=row["cells_carried"],
            cells_measured=row["cells_measured"],
            cells_pending=row["cells_pending"],
            baseline_complete=bool(row["baseline_complete"]),
            created_at=row["created_at"],
        )

    def record_snapshot(self, row: SnapshotRow) -> None:
        values = [
            int(value) if isinstance(value, bool) else value
            for value in (getattr(row, name) for name in _SNAPSHOT_COLUMNS)
        ]
        with self._conn:
            self._conn.execute(
                _upsert_sql("product_snapshot", "snapshot_id", _SNAPSHOT_COLUMNS),
                (row.snapshot_id, *values),
            )

    # -- seeds ------------------------------------------------------------- #
    def reconcile_seed_set(
        self, product_id: str, seeds: Sequence[SeedEntry]
    ) -> None:
        """Upsert авторских seed'ов; отсутствующие в наборе — ``disabled`` + ``retired_at``.

        ``added_at``/``added_by`` существующей строки не перезаписываются (провенанс
        первого появления), ``retired_at`` снимается, если seed вернулся в файл.
        """
        now = _now()
        incoming = {seed.normalized_url for seed in seeds}
        with self._conn:
            for seed in seeds:
                self._conn.execute(
                    """
                    INSERT INTO seed_set (
                        seed_id, product_id, url, normalized_url, role, source,
                        source_ref, profile_key, state, added_at, added_by,
                        last_verified_at, retired_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    ON CONFLICT(seed_id) DO UPDATE SET
                        url = excluded.url,
                        normalized_url = excluded.normalized_url,
                        role = excluded.role,
                        source = excluded.source,
                        source_ref = excluded.source_ref,
                        profile_key = excluded.profile_key,
                        state = excluded.state,
                        last_verified_at = excluded.last_verified_at,
                        retired_at = NULL
                    """,
                    (
                        seed_id(product_id, seed.normalized_url),
                        product_id,
                        seed.url,
                        seed.normalized_url,
                        seed.role.value,
                        seed.source.value,
                        seed.source_ref,
                        seed.profile_key,
                        seed.state.value,
                        seed.added_at.isoformat(),
                        seed.added_by,
                        seed.last_verified_at.isoformat()
                        if seed.last_verified_at is not None
                        else None,
                    ),
                )
            stale = [
                row["normalized_url"]
                for row in self._conn.execute(
                    "SELECT normalized_url FROM seed_set WHERE product_id = ?",
                    (product_id,),
                )
                if row["normalized_url"] not in incoming
            ]
            for normalized_url in stale:
                self._conn.execute(
                    "UPDATE seed_set SET state = ?, retired_at = ? "
                    "WHERE product_id = ? AND normalized_url = ? AND state != ?",
                    (
                        SeedState.DISABLED.value,
                        now,
                        product_id,
                        normalized_url,
                        SeedState.DISABLED.value,
                    ),
                )

    def seed_rows(self, product_id: str) -> list[SeedRow]:
        rows = self._conn.execute(
            "SELECT * FROM seed_set WHERE product_id = ? "
            "ORDER BY normalized_url, seed_id",
            (product_id,),
        ).fetchall()
        return [
            SeedRow(
                seed_id=row["seed_id"],
                product_id=row["product_id"],
                url=row["url"],
                normalized_url=row["normalized_url"],
                role=SeedRole(row["role"]),
                source=SeedSource(row["source"]),
                state=SeedState(row["state"]),
                added_at=row["added_at"],
                added_by=row["added_by"],
                source_ref=row["source_ref"],
                profile_key=row["profile_key"],
                last_verified_at=row["last_verified_at"],
                retired_at=row["retired_at"],
            )
            for row in rows
        ]

    # -- lineage ----------------------------------------------------------- #
    def record_run_lineage(self, row: RunLineageRow) -> None:
        values = []
        for name in _LINEAGE_COLUMNS:
            if name == "products_json":
                # NULL = «состав неизвестен»; [] = прогон без продуктов (разные факты).
                values.append(
                    None
                    if row.products is None
                    else json.dumps(row.products, ensure_ascii=False)
                )
            else:
                values.append(getattr(row, name))
        with self._conn:
            self._conn.execute(
                _upsert_sql("run_lineage", "run_id", _LINEAGE_COLUMNS),
                (row.run_id, *values),
            )

    @staticmethod
    def _lineage_row(row: sqlite3.Row) -> RunLineageRow:
        """Строка SQLite → :class:`RunLineageRow` (``products_json`` разбирается)."""
        raw_products = row["products_json"]
        products = None if raw_products is None else list(json.loads(raw_products))
        return RunLineageRow(
            run_id=row["run_id"],
            package_id=row["package_id"],
            cycle_id=row["cycle_id"],
            previous_run_id=row["previous_run_id"],
            cohort_digest=row["cohort_digest"],
            seeds_digest=row["seeds_digest"],
            execution_contract_digest=row["execution_contract_digest"],
            change_class=row["change_class"],
            status=row["status"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            error=row["error"],
            products=products,
            refresh_policy=row["refresh_policy"],
        )

    def previous_run(self, package_id: str) -> str | None:
        """``run_id`` последнего прогона пакета по ``started_at`` (тай-брейк — ``run_id``)."""
        row = self._conn.execute(
            "SELECT run_id FROM run_lineage WHERE package_id = ? "
            "ORDER BY started_at DESC, run_id DESC LIMIT 1",
            (package_id,),
        ).fetchone()
        return str(row["run_id"]) if row is not None else None

    def get_run_lineage(self, run_id: str) -> RunLineageRow | None:
        """Строка lineage по ``run_id`` (``None`` — прогона нет в истории)."""
        row = self._conn.execute(
            "SELECT * FROM run_lineage WHERE run_id = ?", (run_id,)
        ).fetchone()
        return self._lineage_row(row) if row is not None else None

    def previous_lineage(self, package_id: str) -> RunLineageRow | None:
        """Последняя строка lineage пакета (``None`` — истории нет)."""
        row = self._conn.execute(
            "SELECT * FROM run_lineage WHERE package_id = ? "
            "ORDER BY started_at DESC, run_id DESC LIMIT 1",
            (package_id,),
        ).fetchone()
        return self._lineage_row(row) if row is not None else None
