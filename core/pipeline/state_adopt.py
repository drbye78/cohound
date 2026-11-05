"""coHound — adoption завершённых прогонов в durable состояние пакета (09 §4.6).

``state/{package_id}/state.db`` наполняется **самим прогоном** (``core/pipeline/state_sync``).
Пока adoption не было, первый ``run --update`` после появления ``state.db`` видел у всех
продуктов ``last_corpus_run_id = NULL`` и объявлял их ``new`` — то есть полный краул всей
когорты, хотя завершённые прогоны лежали рядом в ``runs/``. Решение владельца 29.10.2026:
**явная команда** (автоматический ``--adopt-latest`` отклонён — он скрывает выбор baseline).

Модуль — две операции:

* :func:`plan_adoption` — **только чтение** прогонной БД: выбирает прогон, считает по
  продуктам, что будет принято, и называет, чего не хватает. Ничего не пишет;
* :func:`apply_adoption` — пишет по плану **через порт** :class:`ProductStatePort`
  (сырого SQL по ``state.db`` здесь нет). Идемпотентно: естественные ключи таблиц и
  ``upsert``-семантика порта дают повторный вызов без дублей и без сдвига ``created_at``.

**Почему прогонная БД читается сырым ``sqlite3``, а не через ``StorePort``.**
``StorePort`` открывается через :func:`core.store.sqlite.connect_read_only`, а тот
применяет гейт ``_reject_foreign_schema`` и отвергает **любое** расхождение
``schema_version``. Adoption затем и существует, чтобы принять прогоны, сделанные раньше
текущей схемы: гейт превратил бы «старый прогон» в отказ там, где нужен импорт. Плюс
``connect`` (путь писателя) применил бы DDL/миграции к чужой завершённой БД. Поэтому
здесь — read-only соединение (``mode=ro``) и явные ``SELECT`` по таблицам §4.4;
``schema_meta.schema_version`` читается **для сведения** (попадает в план), но не как
гейт. Это импорт, а не работа прогона.

**Что не переносится (и почему это видно оператору).** План несёт
``limitations`` — машинно названные ограничения, которые команда печатает:

* ``cohort_digest``/``seeds_digest`` в прогонной БД не хранятся (нет ни в ``runs``, ни в
  манифесте) — в ``run_lineage`` пишется пустая строка, как в
  :func:`core.pipeline.state_sync.record_run_outcome` при отсутствии дайджестов;
* ``run_scope`` прогон не записывает — снимок получает объявленный ``scope`` (по умолчанию
  ``full``);
* ``etag``/``last_modified`` в прогонной БД отсутствуют → в ``url_state`` ``NULL``: первый
  инкрементальный прогон сделает безусловный ``GET`` и сравнит ``content_hash`` (§4.6);
* ``refresh_policy`` прогона не восстанавливается → ``NULL``;
* ``catalog_row_digest``/``cell_reuse_key``/``evidence_digest`` в ``cell_measurement`` не
  восстанавливаются → ``NULL`` (ключи carry-forward считаются при измерении, а не импортом).

Слой ``pipeline``: зависимости — ``domain``/``ports``/``util``/``config`` и stdlib
(адаптеры/``store``/``cli`` не импортируются). Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from core.config.seeds import normalize_seed_url
from core.domain.enums import DocumentLevel, OnboardingState, ProductScope
from core.domain.models import METHODOLOGY_VERSION, CorpusCoverage
from core.pipeline.corpus_ingest import CAPTURE_OBSERVED_ADOPTED, capture_id_for
from core.pipeline.corpus_persist import _now_iso
from core.pipeline.incremental import chunk_row_id, doc_set_digest, doc_version_id
from core.pipeline.package_admin import PackageAdminError
from core.pipeline.run_identity import classify_change, previous_run_of
from core.pipeline.state_sync import (
    _coverage_digest,
    _doc_counters,
    _snapshot_status,
    snapshot_id,
)
from core.ports.state import (
    CaptureRow,
    CellMeasurementRow,
    ChunkRow,
    DocumentVersionRow,
    IndexPartitionRow,
    ProductStatePort,
    ProductStateRow,
    RunLineageRow,
    SnapshotRow,
    UrlStateRow,
)
from core.util.hashing import content_hash, short_hash
from core.util.index_partition import is_committed, read_manifest

__all__ = [
    "AdoptionPlan",
    "AdoptionResult",
    "CellsAdoption",
    "LineagePlan",
    "PartitionAdoption",
    "ProductAdoption",
    "ProductAdoptionResult",
    "RunCandidate",
    "SnapshotPlan",
    "StateSummary",
    "apply_adoption",
    "completed_runs",
    "plan_adoption",
    "read_state_summary",
]

#: Имя операционной БД прогона в ``runs/{run_id}/`` (03 §A.2).
_RUN_DB_NAME = "cohound.db"
#: Имя копии объявленного входа прогона в ``runs/{run_id}/`` (03 §A.2).
_RUN_YAML_COPY = "run.yaml"
#: Фазы, которые обязаны быть ``ok`` у принимаемого прогона (09 §4.6).
_REQUIRED_PHASES = ("corpus", "evaluate")
#: Разделитель полей — тот же unit separator, что в ``core.pipeline.corpus``/
#: ``core.pipeline.incremental``: склейка ``chunk_id``/``content_hash`` однозначна.
_FIELD_SEP = "\x1f"
#: Статус активной версии документа (§4.2).
_DOC_ACTIVE = "active"
#: Разрешённые состояния ячейки (§5.1) — та же пара, что у писателя снимка.
_RESOLVED_CELL_STATES = frozenset({"evaluated", "reviewed"})

#: Ограничения adoption, которые действуют **всегда** (печатаются оператору).
_BASE_LIMITATIONS: tuple[str, ...] = (
    "cohort_digest/seeds_digest в прогонной БД не хранятся — в run_lineage пустая строка",
    "run_scope прогон не записывает — снимок получает объявленный scope",
    "etag/last_modified в прогонной БД отсутствуют — в url_state NULL "
    "(первый incremental сделает безусловный GET и сравнит content_hash)",
    "refresh_policy прогона не восстанавливается — NULL в run_lineage",
    "catalog_row_digest/cell_reuse_key/evidence_digest не восстанавливаются — NULL "
    "в cell_measurement",
)


# --------------------------------------------------------------------------- #
# Строки плана
# --------------------------------------------------------------------------- #


@dataclasses.dataclass(frozen=True)
class RunCandidate:
    """Прогон пакета, увиденный в ``runs/`` (read-only, наблюдённые значения).

    ``complete`` — прогон пригоден к adoption: фазы ``corpus``/``evaluate`` = ``ok`` и
    есть хотя бы одна строка ``corpus_coverage``. ``reason`` заполнен, когда не пригоден
    (``phase_corpus_failed``, ``no_corpus_coverage``, …): «почему нельзя» — факт, который
    оператор обязан увидеть, а не догадываться о нём.
    """

    run_id: str
    db: str
    created_at: str | None
    snapshot_date: str | None
    schema_version: str | None
    methodology_version: str | None
    execution_contract_digest: str | None
    phases: Mapping[str, str]
    complete: bool
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление."""
        return {
            "run_id": self.run_id,
            "db": self.db,
            "created_at": self.created_at,
            "snapshot_date": self.snapshot_date,
            "schema_version": self.schema_version,
            "methodology_version": self.methodology_version,
            "execution_contract_digest": self.execution_contract_digest,
            "phases": dict(self.phases),
            "complete": self.complete,
            "reason": self.reason,
        }


@dataclasses.dataclass(frozen=True)
class PartitionAdoption:
    """Продукт-партиция прогона и результат сверки с диском (09 §8.2/§8.5).

    ``content_digest`` — отпечаток из ``index_manifest`` прогона, ``recomputed_digest`` —
    пересчитанный по ``corpus_chunks`` тем же правилом (sorted ``(chunk_id,
    content_hash)``). Расхождение не «исправляется» записью: партиция не принимается, а
    причина названа.
    """

    partition_build_id: str
    verified: bool
    reason: str | None
    content_digest: str | None
    recomputed_digest: str | None
    store_path: str
    chunks: int
    row: IndexPartitionRow | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (без строки порта)."""
        return {
            "partition_build_id": self.partition_build_id,
            "verified": self.verified,
            "reason": self.reason,
            "content_digest": self.content_digest,
            "recomputed_digest": self.recomputed_digest,
            "store_path": self.store_path,
            "chunks": self.chunks,
        }


@dataclasses.dataclass(frozen=True)
class CellsAdoption:
    """Перенос измерений ячеек в ``cell_measurement`` (09 §4.2, carry-forward).

    ``adopted`` ложно, когда переносить нечего или нельзя: ``no_cells`` (ячеек нет),
    ``evaluate_not_ok`` (фаза оценки не прошла), ``methodology_mismatch`` (вердикты
    получены по другим правилам — корпус переиспользуется, оценка делается заново).
    """

    adopted: bool
    reason: str | None
    total: int
    resolved: int
    pending: int

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление."""
        return {
            "adopted": self.adopted,
            "reason": self.reason,
            "total": self.total,
            "resolved": self.resolved,
            "pending": self.pending,
        }


@dataclasses.dataclass(frozen=True)
class SnapshotPlan:
    """Что получится в ``product_snapshot`` (статус/покрытие/ячейки).

    ``docs_total``/``docs_new``/``docs_reused`` сюда не входят: они считаются при записи
    тем же :func:`core.pipeline.state_sync._doc_counters`, что и у прогона (по durable
    doc-set после вставки версий), иначе счётчики adopted-снимка расходились бы с
    прогонными.
    """

    status: str
    scope: str
    coverage_complete: bool
    coverage_digest: str | None
    doc_set_digest: str
    cells_total: int
    cells_measured: int
    cells_pending: int
    baseline_complete: bool
    partition_build_id: str | None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление."""
        return {
            "status": self.status,
            "scope": self.scope,
            "coverage_complete": self.coverage_complete,
            "coverage_digest": self.coverage_digest,
            "doc_set_digest": self.doc_set_digest,
            "cells_total": self.cells_total,
            "cells_measured": self.cells_measured,
            "cells_pending": self.cells_pending,
            "baseline_complete": self.baseline_complete,
            "partition_build_id": self.partition_build_id,
        }


@dataclasses.dataclass(frozen=True)
class LineagePlan:
    """Что получится в ``run_lineage`` (§4.2/§12.3).

    ``previous_run_id``/``change_class`` заполняются **при записи**: предыдущий прогон
    берётся из ``state.db`` (это и есть цепочка циклов), класс изменения —
    :func:`core.pipeline.run_identity.classify_change` относительно него.
    """

    run_id: str
    package_id: str
    cycle_id: str
    execution_contract_digest: str
    status: str
    started_at: str
    completed_at: str | None
    products: tuple[str, ...]
    cohort_digest: str = ""
    seeds_digest: str = ""
    refresh_policy: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление."""
        return {
            "run_id": self.run_id,
            "package_id": self.package_id,
            "cycle_id": self.cycle_id,
            "execution_contract_digest": self.execution_contract_digest,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "products": list(self.products),
            "cohort_digest": self.cohort_digest,
            "seeds_digest": self.seeds_digest,
            "refresh_policy": self.refresh_policy,
        }


@dataclasses.dataclass(frozen=True)
class ProductAdoption:
    """Что будет принято по одному продукту (и что именно отсутствует).

    Принятие **продукт-атомарно**: либо документы, чанки и ``url_state`` переносятся
    целиком, либо не переносится ничего и причины названы. Полупринятый продукт опаснее
    непринятого: версия без chunk-set невидима инкрементальному плану, но выглядит как
    известный документ.

    ``documents``/``chunks``/``url_states`` — **наблюдённые** в прогоне числа (они видны
    и при отказе: «в прогоне есть, но не принято» — отдельный факт).
    """

    product_id: str
    accepted: bool
    reasons: tuple[str, ...]
    documents: int
    chunks: int
    url_states: int
    coverage_complete: bool | None
    coverage_reason: str | None
    partitions: tuple[PartitionAdoption, ...]
    cells: CellsAdoption
    snapshot: SnapshotPlan | None
    coverage: CorpusCoverage | None = None
    document_rows: tuple[DocumentVersionRow, ...] = ()
    chunk_rows: tuple[tuple[ChunkRow, str], ...] = ()
    url_rows: tuple[UrlStateRow, ...] = ()
    measurement_rows: tuple[CellMeasurementRow, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (строки порта не сериализуются)."""
        return {
            "product_id": self.product_id,
            "accepted": self.accepted,
            "reasons": list(self.reasons),
            "documents": self.documents,
            "chunks": self.chunks,
            "url_states": self.url_states,
            "coverage_complete": self.coverage_complete,
            "coverage_reason": self.coverage_reason,
            "partitions": [item.as_dict() for item in self.partitions],
            "cells": self.cells.as_dict(),
            "snapshot": None if self.snapshot is None else self.snapshot.as_dict(),
        }


@dataclasses.dataclass(frozen=True)
class AdoptionPlan:
    """План adoption: выбранный прогон, продукты, ограничения и причины отказа.

    ``run_id is None`` — принимать нечего; ``reasons`` называет почему
    (``no_completed_run``, ``run_not_adoptable:<причина>``), а ``candidates`` — что вообще
    нашлось в ``runs/``.

    ``evaluate_ok`` — фаза ``evaluate`` прогона прошла; это рубеж ``last_evaluated_run_id``
    (то же условие, что у писателя прогона, а не догадка по статусу lineage).
    """

    package_id: str
    run_id: str | None
    cycle_id: str
    source_db: str | None
    schema_version: str | None
    methodology_version: str | None
    methodology_matches: bool
    evaluate_ok: bool
    document_level: str
    document_level_source: str
    products: tuple[ProductAdoption, ...]
    lineage: LineagePlan | None
    candidates: tuple[RunCandidate, ...]
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    limitations: tuple[str, ...]

    @property
    def accepted_products(self) -> tuple[ProductAdoption, ...]:
        """Продукты, которые будут приняты."""
        return tuple(item for item in self.products if item.accepted)

    @property
    def skipped_products(self) -> tuple[ProductAdoption, ...]:
        """Продукты, которые останутся ``new`` (с причинами)."""
        return tuple(item for item in self.products if not item.accepted)

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (``hound package state adopt --json``)."""
        return {
            "ok": self.run_id is not None,
            "package": self.package_id,
            "run_id": self.run_id,
            "cycle_id": self.cycle_id,
            "db": self.source_db,
            "schema_version": self.schema_version,
            "methodology_version": self.methodology_version,
            "methodology_matches": self.methodology_matches,
            "evaluate_ok": self.evaluate_ok,
            "document_level": self.document_level,
            "document_level_source": self.document_level_source,
            "accepted": [item.product_id for item in self.accepted_products],
            "skipped": [item.product_id for item in self.skipped_products],
            "products": [item.as_dict() for item in self.products],
            "lineage": None if self.lineage is None else self.lineage.as_dict(),
            "candidates": [item.as_dict() for item in self.candidates],
            "reasons": list(self.reasons),
            "warnings": list(self.warnings),
            "limitations": list(self.limitations),
        }


@dataclasses.dataclass(frozen=True)
class ProductAdoptionResult:
    """Что фактически записано по продукту (после :func:`apply_adoption`)."""

    product_id: str
    adopted: bool
    reasons: tuple[str, ...]
    documents: int
    chunks: int
    url_states: int
    partitions: int
    cells: int

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление."""
        return {
            "product_id": self.product_id,
            "adopted": self.adopted,
            "reasons": list(self.reasons),
            "documents": self.documents,
            "chunks": self.chunks,
            "url_states": self.url_states,
            "partitions": self.partitions,
            "cells": self.cells,
        }


@dataclasses.dataclass(frozen=True)
class AdoptionResult:
    """Итог записи: по продуктам + тоталы и то, что осталось непринятым."""

    package_id: str
    run_id: str
    cycle_id: str
    dry_run: bool
    actor: str
    products: tuple[ProductAdoptionResult, ...]
    totals: Mapping[str, int]
    lineage_written: bool
    warnings: tuple[str, ...]
    limitations: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (``--json``)."""
        return {
            "ok": True,
            "package": self.package_id,
            "run_id": self.run_id,
            "cycle_id": self.cycle_id,
            "dry_run": self.dry_run,
            "actor": self.actor,
            "products": [item.as_dict() for item in self.products],
            "totals": dict(self.totals),
            "lineage_written": self.lineage_written,
            "warnings": list(self.warnings),
            "limitations": list(self.limitations),
        }


@dataclasses.dataclass(frozen=True)
class StateSummary:
    """Что сейчас лежит в ``state.db`` (``hound package state show``).

    ``exists=False`` — файла нет. ``empty=True`` — файл есть, но ни ``product_state``, ни
    ``run_lineage`` не заполнены: состояние не наполнялось (ни прогоном, ни adoption).
    ``empty`` — про **рубежи**, а не про данные: ``substrate`` может быть непуст (запись
    идёт на границе продукта, §4.7), и тогда «пусто» означает «данные есть, рубежей нет».
    """

    db: str
    exists: bool
    schema_version: str | None
    empty: bool
    products: tuple[dict[str, Any], ...]
    snapshots: tuple[dict[str, Any], ...]
    lineage: tuple[dict[str, Any], ...]
    counts: Mapping[str, int]
    warnings: tuple[str, ...]
    #: Субстрат по продуктам: ``{product_id: {"docs", "chunks", "partitions"}}``.
    #: Нужен потому, что рубежи (``product_state``) и субстрат пишутся в разных точках:
    #: прошлые прогоны успели записать ``document_version``/``chunk_meta`` на границе
    #: продукта, но не дошли до ``record_run_outcome``. Без этого разреза «в state.db
    #: пусто» и «в state.db 53k чанков без рубежей» выглядели бы одинаково.
    substrate: Mapping[str, Mapping[str, int]] = dataclasses.field(
        default_factory=dict
    )

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление."""
        return {
            "ok": True,
            "db": self.db,
            "exists": self.exists,
            "schema_version": self.schema_version,
            "empty": self.empty,
            "products": list(self.products),
            "snapshots": list(self.snapshots),
            "lineage": list(self.lineage),
            "counts": dict(self.counts),
            "warnings": list(self.warnings),
            "substrate": {key: dict(value) for key, value in self.substrate.items()},
        }


# --------------------------------------------------------------------------- #
# Чтение прогонной БД (read-only, без гейта версии схемы)
# --------------------------------------------------------------------------- #


def _open_run_db(db_path: str | Path) -> sqlite3.Connection:
    """Read-only соединение с прогонной БД (``mode=ro``, без DDL и миграций).

    Гейт ``schema_version`` здесь **не** применяется: см. докстринг модуля. ``busy_timeout``
    ставится, потому что рядом может работать живой писатель (WAL).
    """
    conn = sqlite3.connect(f"file:{Path(db_path).absolute().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    """Есть ли таблица (старая БД могла её не иметь)."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone()
    return row is not None


def _get(row: Mapping[str, Any] | sqlite3.Row, name: str, default: Any = None) -> Any:
    """Значение колонки или ``default`` (старая БД без колонки — не ошибка чтения)."""
    try:
        value = row[name]
    except (IndexError, KeyError):
        return default
    return default if value is None else value


def _parse_phases(raw: Any) -> dict[str, str]:
    """Карта ``{phase: state}`` из ``runs.state`` (формат ``{"phase_state": {...}}``).

    Толерантна к ``NULL``/legacy-строке/повреждённому JSON — как канонический читатель
    ``core.store.sqlite_store._parse_phase_state`` (``pipeline`` не импортирует ``store``;
    здесь повторён **формат**, а не логика домена).
    """
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    inner = payload.get("phase_state")
    if not isinstance(inner, Mapping):
        return {}
    return {str(key): str(value) for key, value in inner.items() if value is not None}


def _schema_version(conn: sqlite3.Connection) -> str | None:
    """Объявленная версия схемы прогонной БД (для сведения, не как гейт)."""
    if not _has_table(conn, "schema_meta"):
        return None
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    return None if row is None else str(row["value"])


def _scan_runs(runs_dir: str | Path, package_id: str) -> tuple[RunCandidate, ...]:
    """Все прогоны пакета в ``runs/`` с вердиктом пригодности (новые первыми).

    Чужая/битая БД **не исчезает** из списка: она попадает в ``candidates`` с причиной —
    «прогон есть, но не читается» отличается от «прогона нет».
    """
    root = Path(runs_dir)
    if not root.is_dir():
        return ()
    found: list[RunCandidate] = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        db_path = entry / _RUN_DB_NAME
        if not db_path.is_file():
            continue
        candidate = _read_candidate(db_path, entry.name, package_id)
        if candidate is not None:
            found.append(candidate)
    found.sort(key=lambda item: (item.created_at or "", item.run_id), reverse=True)
    return tuple(found)


def _read_candidate(
    db_path: Path, run_id: str, package_id: str
) -> RunCandidate | None:
    """Читает вердикт по одному прогону (``None`` — БД не прогон этого пакета)."""
    try:
        conn = _open_run_db(db_path)
    except sqlite3.Error as exc:
        return RunCandidate(
            run_id=run_id,
            db=str(db_path),
            created_at=None,
            snapshot_date=None,
            schema_version=None,
            methodology_version=None,
            execution_contract_digest=None,
            phases={},
            complete=False,
            reason=f"db_unreadable:{type(exc).__name__}",
        )
    try:
        if not _has_table(conn, "runs"):
            return None
        row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        declared_package = _get(row, "package_id")
        if declared_package is not None and str(declared_package) != package_id:
            # Каталог прогона принадлежит другому пакету — это не «нашего» прогона запись.
            return None
        phases = _parse_phases(_get(row, "state"))
        reason: str | None = None
        for phase in _REQUIRED_PHASES:
            state = phases.get(phase)
            if state != "ok":
                reason = f"phase_{phase}_{state or 'missing'}"
                break
        if reason is None and not _has_coverage(conn):
            reason = "no_corpus_coverage"
        return RunCandidate(
            run_id=run_id,
            db=str(db_path),
            created_at=_as_text(_get(row, "created_at")),
            snapshot_date=_as_text(_get(row, "snapshot_date")),
            schema_version=_schema_version(conn),
            methodology_version=_as_text(_get(row, "methodology_version")),
            execution_contract_digest=_as_text(_get(row, "execution_contract_digest")),
            phases=phases,
            complete=reason is None,
            reason=reason,
        )
    except sqlite3.Error as exc:
        return RunCandidate(
            run_id=run_id,
            db=str(db_path),
            created_at=None,
            snapshot_date=None,
            schema_version=None,
            methodology_version=None,
            execution_contract_digest=None,
            phases={},
            complete=False,
            reason=f"db_unreadable:{type(exc).__name__}",
        )
    finally:
        conn.close()


def _has_coverage(conn: sqlite3.Connection) -> bool:
    """Есть ли в БД хотя бы одна строка покрытия корпуса."""
    if not _has_table(conn, "corpus_coverage"):
        return False
    return (
        conn.execute("SELECT 1 FROM corpus_coverage LIMIT 1").fetchone() is not None
    )


def completed_runs(runs_dir: str | Path, package_id: str) -> tuple[RunCandidate, ...]:
    """Пригодные к adoption прогоны пакета (новые первыми).

    «Завершён» здесь — операционное определение §4.6: фазы ``corpus``/``evaluate`` = ``ok``
    и есть покрытие корпуса. Возвращаются только такие; непригодные остаются видимыми в
    плане (``AdoptionPlan.candidates``) с причиной.
    """
    return tuple(item for item in _scan_runs(runs_dir, package_id) if item.complete)


def _as_text(value: Any) -> str | None:
    """Непустая строка или ``None`` (пустое значение не выдаётся за факт)."""
    if value is None:
        return None
    text = str(value)
    return text if text.strip() else None


# --------------------------------------------------------------------------- #
# План
# --------------------------------------------------------------------------- #


def plan_adoption(
    runs_dir: str | Path,
    package_id: str,
    *,
    from_run: str | None = None,
    index_root: str | Path | None = None,
    methodology_version: str = METHODOLOGY_VERSION,
    scope: str = ProductScope.FULL.value,
) -> AdoptionPlan:
    """Считает, что будет принято из завершённого прогона (только чтение).

    Выбор прогона: ``from_run`` (явно) либо **последний** пригодный прогон пакета
    (:func:`completed_runs`; порядок — ``created_at``, затем ``run_id``). Автоматического
    «возьми что найдёшь» нет: baseline выбирает человек, а план показывает, что именно
    он выбрал (и что было бы принято из других прогонов).

    ``cycle_id`` берётся из ``runs.snapshot_date`` (объявленный вход прогона). Если его
    нет — из даты ``runs.created_at``, и это видно в ``warnings``
    (``cycle_from_created_at``): цикл — не «сегодняшняя дата», а атрибут прогона.

    :param index_root: корень ``index_store/`` для физической сверки партиций. ``None`` —
    сверять не с чем: партиции не принимаются с причиной ``index_root_missing`` (тихая
    запись непроверенного ``content_digest`` запрещена).
    :param methodology_version: текущая версия методологии; при расхождении с прогоном
    корпус принимается, а измерения ячеек — нет.
    :param scope: ``full``|``product_only`` для снимка (прогон свой scope не записывает).
    """
    candidates = _scan_runs(runs_dir, package_id)
    warnings: list[str] = []
    reasons: list[str] = []
    chosen: RunCandidate | None = None
    if from_run is not None:
        chosen = next((item for item in candidates if item.run_id == from_run), None)
        if chosen is None:
            reasons.append(f"run_not_found:{from_run}")
        elif not chosen.complete:
            reasons.append(f"run_not_adoptable:{chosen.reason or 'unknown'}")
            chosen = None
    else:
        chosen = next((item for item in candidates if item.complete), None)
        if chosen is None:
            reasons.append("no_completed_run")
    if chosen is None:
        return AdoptionPlan(
            package_id=package_id,
            run_id=None,
            cycle_id="",
            source_db=None,
            schema_version=None,
            methodology_version=None,
            methodology_matches=False,
            evaluate_ok=False,
            document_level=DocumentLevel.FULL.value,
            document_level_source="default",
            products=(),
            lineage=None,
            candidates=candidates,
            reasons=tuple(reasons),
            warnings=(),
            limitations=_BASE_LIMITATIONS,
        )

    cycle_id = _as_text(chosen.snapshot_date)
    if cycle_id is None:
        cycle_id = _as_text(chosen.created_at[:10] if chosen.created_at else None) or ""
        warnings.append("cycle_from_created_at: у прогона нет snapshot_date")
    level, level_source = _document_level(Path(chosen.db).parent, warnings)
    methodology_matches = chosen.methodology_version == methodology_version

    content = _read_run_content(Path(chosen.db), warnings)
    products = tuple(
        _plan_product(
            product_id=product_id,
            docs=content["docs"].get(product_id, ()),
            chunks_by_doc=content["chunks"],
            coverage=content["coverage"].get(product_id),
            manifest=content["manifest"].get(product_id),
            cells=content["cells"].get(product_id, ()),
            run_id=chosen.run_id,
            package_id=package_id,
            document_level=level,
            index_root=index_root,
            methodology_matches=methodology_matches,
            evaluate_ok=chosen.phases.get("evaluate") == "ok",
            scope=scope,
            completed_at=_as_text(chosen.created_at),
            warnings=warnings,
        )
        for product_id in content["products"]
    )
    if not products:
        reasons.append("no_products_in_run")
    lineage = LineagePlan(
        run_id=chosen.run_id,
        package_id=package_id,
        cycle_id=cycle_id,
        execution_contract_digest=chosen.execution_contract_digest or "",
        status=_lineage_status(chosen.phases),
        started_at=_as_text(chosen.created_at) or "",
        completed_at=_as_text(chosen.created_at),
        products=content["products"],
    )
    return AdoptionPlan(
        package_id=package_id,
        run_id=chosen.run_id,
        cycle_id=cycle_id,
        source_db=chosen.db,
        schema_version=chosen.schema_version,
        methodology_version=chosen.methodology_version,
        methodology_matches=methodology_matches,
        evaluate_ok=chosen.phases.get("evaluate") == "ok",
        document_level=level,
        document_level_source=level_source,
        products=products,
        lineage=lineage,
        candidates=candidates,
        reasons=tuple(reasons),
        warnings=tuple(warnings),
        limitations=_BASE_LIMITATIONS,
    )


def _lineage_status(phases: Mapping[str, str]) -> str:
    """Статус lineage по карте фаз прогона (словарь §4.2: complete/partial/failed).

    Тот же смысл, что у :data:`core.pipeline.state_sync._LINEAGE_STATUS`: ``report`` = ``ok``
    — прогон дошёл до конца; любая ``failed`` — ``failed``; иначе ``partial``.
    """
    if any(state == "failed" for state in phases.values()):
        return "failed"
    if phases.get("report") == "ok":
        return "complete"
    return "partial"


def _document_level(
    run_dir: Path, warnings: list[str]
) -> tuple[str, str]:
    """Уровень извлечения документов принятого прогона (ось ``crawl.document_level``).

    Источник — **объявленный вход**: копия ``run.yaml`` в каталоге прогона (03 §A.2).
    Уровень входит в ``doc_version_id`` (``H(product_id, content_hash, extractor_version,
    level)``), поэтому угаданный уровень сделал бы adopted-документы невидимыми для
    инкрементального плана — это главный риск adoption. Если копии нет, берётся дефолт оси
    (``full``) и это названо видимо (``document_level_defaulted``), а не выдано за факт.
    """
    path = run_dir / _RUN_YAML_COPY
    declared: Any = None
    if path.is_file():
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            payload = None
        if isinstance(payload, Mapping):
            crawl = payload.get("crawl")
            if isinstance(crawl, Mapping):
                declared = crawl.get("document_level")
    text = _as_text(declared)
    if text is not None and text in {level.value for level in DocumentLevel}:
        return text, "run.yaml"
    warnings.append(
        "document_level_defaulted: уровень не объявлен в копии run.yaml — принят дефолт "
        f"{DocumentLevel.FULL.value}; при другом уровне документа версии не совпадут "
        "с формулой корпуса"
    )
    return DocumentLevel.FULL.value, "default"


def _read_run_content(
    db_path: Path, warnings: list[str]
) -> dict[str, Any]:
    """Читает содержимое прогона, нужное плану (read-only, толерантно к старой схеме)."""
    content: dict[str, Any] = {
        "products": (),
        "docs": {},
        "chunks": {},
        "coverage": {},
        "manifest": {},
        "cells": {},
    }
    conn = _open_run_db(db_path)
    try:
        docs: dict[str, list[dict[str, Any]]] = {}
        if _has_table(conn, "corpus_docs"):
            for row in conn.execute(
                "SELECT * FROM corpus_docs ORDER BY product_id, doc_id"
            ):
                docs.setdefault(str(row["product_id"]), []).append(dict(row))
        chunks: dict[str, list[dict[str, Any]]] = {}
        if _has_table(conn, "corpus_chunks"):
            for row in conn.execute(
                "SELECT * FROM corpus_chunks ORDER BY doc_id, chunk_no, chunk_id"
            ):
                chunks.setdefault(str(row["doc_id"]), []).append(dict(row))
        coverage: dict[str, CorpusCoverage] = {}
        if _has_table(conn, "corpus_coverage"):
            for row in conn.execute("SELECT * FROM corpus_coverage"):
                product_id = str(row["product_id"])
                coverage[product_id] = _coverage_from_row(row, product_id, warnings)
        manifest: dict[str, dict[str, Any]] = {}
        if _has_table(conn, "index_manifest"):
            for row in conn.execute(
                "SELECT * FROM index_manifest ORDER BY product_id, partition_build_id"
            ):
                manifest.setdefault(str(row["product_id"]), dict(row))
        cells: dict[str, list[dict[str, Any]]] = {}
        if _has_table(conn, "cells"):
            for row in conn.execute(
                "SELECT * FROM cells ORDER BY product_id, param_id"
            ):
                cells.setdefault(str(row["product_id"]), []).append(dict(row))
    finally:
        conn.close()
    products = tuple(
        sorted(
            set(docs)
            | set(coverage)
            | set(cells)
            | {pid for pid, rows in manifest.items() if rows}
        )
    )
    content.update(
        products=products, docs=docs, chunks=chunks, coverage=coverage,
        manifest=manifest, cells=cells,
    )
    return content


def _coverage_from_row(
    row: sqlite3.Row, product_id: str, warnings: list[str]
) -> CorpusCoverage:
    """Строка ``corpus_coverage`` → доменная модель (для дайджеста/статуса снимка).

    ``blocked``/``unreadable_origins``/``partial_blocks`` — JSON-колонки поздних версий
    схемы: отсутствие колонки означает «не записывалось» (пустые значения), а не ошибку.
    Если ``blocked`` содержит неизвестные ``error_kind`` (старый словарь), строка не
    выбрасывается: дайджест такого покрытия назван в ``warnings`` как расходящийся.
    """
    blocked = _json_dict(_get(row, "blocked_json", "{}"))
    unreadable = _json_list(_get(row, "unreadable_origins_json", "[]"))
    partial_blocks = _json_dict(_get(row, "partial_blocks_json", "{}"))
    base = {
        "product_id": product_id,
        "complete": bool(_get(row, "complete", 0)),
        "expected_sources": int(_get(row, "expected_sources", 0) or 0),
        "observed_sources": int(_get(row, "observed_sources", 0) or 0),
        "unreadable_origins": unreadable,
        "partial_blocks": partial_blocks,
        "truncated": bool(_get(row, "truncated", 0)),
        "computed_at": str(_get(row, "computed_at", "") or ""),
        "resolved_manually": int(_get(row, "resolved_manually", 0) or 0),
        "reason": _as_text(_get(row, "reason")),
    }
    try:
        return CorpusCoverage(**base, blocked=blocked)  # type: ignore[arg-type]
    except ValueError:
        warnings.append(
            f"coverage_blocked_unparsed:{product_id}: неизвестные error_kind в "
            "corpus_coverage.blocked — дайджест покрытия не совпадёт с прогонным"
        )
        return CorpusCoverage(**base, blocked={})  # type: ignore[arg-type]


def _json_dict(raw: Any) -> dict[str, Any]:
    """JSON-объект из TEXT-колонки (``{}`` при NULL/порче)."""
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _json_list(raw: Any) -> list[Any]:
    """JSON-массив из TEXT-колонки (``[]`` при NULL/порче)."""
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return list(value) if isinstance(value, list) else []


def _plan_product(
    *,
    product_id: str,
    docs: Sequence[Mapping[str, Any]],
    chunks_by_doc: Mapping[str, Sequence[Mapping[str, Any]]],
    coverage: CorpusCoverage | None,
    manifest: Mapping[str, Any] | None,
    cells: Sequence[Mapping[str, Any]],
    run_id: str,
    package_id: str,
    document_level: str,
    index_root: str | Path | None,
    methodology_matches: bool,
    evaluate_ok: bool,
    scope: str,
    completed_at: str | None,
    warnings: list[str],
) -> ProductAdoption:
    """План по одному продукту: атомарное принятие корпуса + отдельные решения по
    партиции и измерениям ячеек.
    """
    reasons: list[str] = []
    chunk_count = sum(len(chunks_by_doc.get(str(doc["doc_id"]), ())) for doc in docs)
    if coverage is None:
        reasons.append("no_coverage")
    chunker_version = _as_text(_get(manifest or {}, "chunker_version"))
    if manifest is None:
        reasons.append("no_index_manifest")
    elif chunker_version is None:
        reasons.append("chunker_version_unknown")
    if not docs:
        reasons.append("no_documents")

    document_rows: list[DocumentVersionRow] = []
    chunk_rows: list[tuple[ChunkRow, str]] = []
    content_pairs: list[tuple[str, str]] = []
    url_rows: list[UrlStateRow] = []
    incomplete = 0
    without_chunks = 0
    hash_mismatch = 0
    span_mismatch = 0
    for doc in docs:
        doc_id = str(doc["doc_id"])
        doc_hash = _as_text(_get(doc, "content_hash"))
        extractor = _as_text(_get(doc, "extractor_version"))
        url = _as_text(_get(doc, "url"))
        if doc_hash is None or extractor is None or url is None:
            incomplete += 1
            continue
        dv_id = doc_version_id(product_id, doc_hash, extractor, document_level)
        raw_chunks = chunks_by_doc.get(doc_id, ())
        doc_chunks: list[tuple[ChunkRow, str]] = []
        for chunk in raw_chunks:
            text = _get(chunk, "text")
            chunk_no = _get(chunk, "chunk_no")
            if not isinstance(text, str) or not text.strip() or chunk_no is None:
                continue
            digest = content_hash(text)
            stored = _as_text(_get(chunk, "content_hash"))
            if stored is not None and stored != digest:
                # I19: адрес текста — его хеш. Расхождение означает несогласованную
                # строку; принимать её значило бы импортировать неверную адресацию.
                hash_mismatch += 1
                continue
            # L1b-2b: смещения куска — из ``corpus_chunks`` источника как есть
            # (``_get`` терпит прогон старой схемы: колонки нет ⇒ None). Пара проверяется
            # здесь же, по тому же принципу, что и хеш: рваная пара или диапазон, не
            # совпадающий с длиной текста, — признак несогласованной строки, и принимать
            # её значило бы отложить отказ до ``CorpusChunk`` (или, хуже, до цитаты).
            pair = _span_pair(
                _get(chunk, "char_start"), _get(chunk, "char_end"), text
            )
            if pair is None:
                span_mismatch += 1
                continue
            char_start, char_end = pair
            doc_chunks.append(
                (
                    ChunkRow(
                        chunk_id=chunk_row_id(dv_id, chunker_version or "", int(chunk_no)),
                        doc_version_id=dv_id,
                        product_id=product_id,
                        chunk_no=int(chunk_no),
                        content_hash=digest,
                        chunker_version=chunker_version or "",
                        heading_path=_as_text(_get(chunk, "heading_path")),
                        context_prefix=_as_text(_get(chunk, "context_prefix")),
                        lang=_as_text(_get(chunk, "lang")),
                        char_start=char_start,
                        char_end=char_end,
                    ),
                    text,
                )
            )
            content_pairs.append((str(_get(chunk, "chunk_id") or ""), digest))
        if not doc_chunks:
            without_chunks += 1
            continue
        fetched_at = _as_text(_get(doc, "fetched_at")) or completed_at or ""
        normalized = _normalize(url)
        if normalized is None:
            incomplete += 1
            continue
        document_rows.append(
            DocumentVersionRow(
                doc_version_id=dv_id,
                doc_id=f"{product_id}:{doc_hash}",
                product_id=product_id,
                url=url,
                content_hash=doc_hash,
                extractor_version=extractor,
                level=document_level,
                raw_path=_as_text(_get(doc, "raw_path")),
                chunker_version=chunker_version,
                chunk_ids_digest=_chunk_ids_digest(doc_chunks),
                extract_partial=(
                    bool(_get(doc, "partial", 0)) if _get(doc, "partial") is not None else None
                ),
                extract_partial_reason=(
                    _as_text(_get(doc, "partial_reason"))
                    if bool(_get(doc, "partial", 0))
                    else None
                ),
                extract_pages_converted=(
                    _as_int(_get(doc, "pages_converted"))
                    if bool(_get(doc, "partial", 0))
                    else None
                ),
                extract_page_count=_as_int(_get(doc, "page_count")),
                first_seen_run_id=run_id,
                last_seen_run_id=run_id,
                first_seen_at=fetched_at,
                last_seen_at=fetched_at,
                status=_DOC_ACTIVE,
            )
        )
        chunk_rows.extend(doc_chunks)
        url_rows.append(
            UrlStateRow(
                product_id=product_id,
                url=url,
                normalized_url=normalized,
                doc_version_id=dv_id,
                # Валидаторы и HTTP-статус в прогонной БД не хранятся (§4.6): NULL —
                # честное «не записано», первый incremental сделает безусловный GET.
                etag=None,
                last_modified=None,
                last_http_status=None,
                last_checked_at=fetched_at or None,
                last_changed_at=fetched_at or None,
                reuse_kind=None,
            )
        )
    if incomplete:
        reasons.append(f"documents_incomplete:{incomplete}")
    if without_chunks:
        reasons.append(f"documents_without_chunks:{without_chunks}")
    if hash_mismatch:
        reasons.append(f"chunk_hash_mismatch:{hash_mismatch}")
    if span_mismatch:
        reasons.append(f"chunk_span_mismatch:{span_mismatch}")

    cells_adoption = _cells_adoption(
        product_id=product_id,
        cells=cells,
        evaluate_ok=evaluate_ok,
        methodology_matches=methodology_matches,
    )
    partitions = _partitions(
        product_id=product_id,
        package_id=package_id,
        manifest=manifest,
        content_pairs=content_pairs,
        index_root=index_root,
        run_id=run_id,
        completed_at=completed_at,
        warnings=warnings,
    )
    accepted = not reasons
    snapshot = None
    if accepted and coverage is not None:
        verified = next((item for item in partitions if item.verified), None)
        cells_pending = cells_adoption.pending
        baseline_complete = bool(
            scope == ProductScope.FULL.value
            and evaluate_ok
            and cells_adoption.total > 0
            and cells_pending == 0
        )
        snapshot = SnapshotPlan(
            status=_snapshot_status(coverage),
            scope=scope,
            coverage_complete=coverage.complete,
            coverage_digest=_coverage_digest(coverage),
            doc_set_digest=doc_set_digest(document_rows),
            cells_total=cells_adoption.total,
            cells_measured=cells_adoption.resolved,
            cells_pending=cells_pending,
            baseline_complete=baseline_complete,
            partition_build_id=(verified.partition_build_id if verified is not None else None),
        )
    measurement_rows = (
        _measurement_rows(
            product_id=product_id,
            run_id=run_id,
            cells=cells,
            snapshot=snapshot,
            partitions=partitions,
            completed_at=completed_at,
        )
        if cells_adoption.adopted and snapshot is not None
        else ()
    )
    if not accepted:
        # Продукт-атомарность: непринятый продукт не пишет ни документов, ни чанков,
        # ни url_state, ни партиций, ни измерений — причины названы выше.
        return ProductAdoption(
            product_id=product_id,
            accepted=False,
            reasons=tuple(reasons),
            documents=len(docs),
            chunks=chunk_count,
            url_states=len(url_rows),
            coverage_complete=None if coverage is None else coverage.complete,
            coverage_reason=None if coverage is None else coverage.reason,
            partitions=partitions,
            cells=cells_adoption,
            snapshot=None,
        )
    return ProductAdoption(
        product_id=product_id,
        accepted=True,
        reasons=(),
        documents=len(document_rows),
        chunks=len(chunk_rows),
        url_states=len(url_rows),
        coverage_complete=coverage.complete if coverage is not None else None,
        coverage_reason=coverage.reason if coverage is not None else None,
        partitions=partitions,
        cells=cells_adoption,
        snapshot=snapshot,
        coverage=coverage,
        document_rows=tuple(document_rows),
        chunk_rows=tuple(chunk_rows),
        url_rows=tuple(url_rows),
        measurement_rows=measurement_rows,
    )


def _cells_adoption(
    *,
    product_id: str,
    cells: Sequence[Mapping[str, Any]],
    evaluate_ok: bool,
    methodology_matches: bool,
) -> CellsAdoption:
    """Решение по ``cell_measurement``: переносим ли вердикты прогона.

    Перенос запрещён при другой ``methodology_version``: это защита от переноса вердиктов,
    полученных по старым правилам. Корпус при этом переиспользуется — оценка делается
    заново (ячейки нового прогона останутся ``pending`` до измерения).
    """
    total = len(cells)
    resolved = sum(1 for cell in cells if str(cell.get("state")) in _RESOLVED_CELL_STATES)
    pending = total - resolved
    if total == 0:
        return CellsAdoption(False, "no_cells", 0, 0, 0)
    if not evaluate_ok:
        return CellsAdoption(False, "evaluate_not_ok", total, resolved, pending)
    if not methodology_matches:
        return CellsAdoption(False, "methodology_mismatch", total, resolved, pending)
    return CellsAdoption(True, None, total, resolved, pending)


def _measurement_rows(
    *,
    product_id: str,
    run_id: str,
    cells: Sequence[Mapping[str, Any]],
    snapshot: SnapshotPlan,
    partitions: Sequence[PartitionAdoption],
    completed_at: str | None,
) -> tuple[CellMeasurementRow, ...]:
    """Строки ``cell_measurement`` из ячеек прогона (ключи carry-forward — ``NULL``)."""
    partition_id = next(
        (item.partition_build_id for item in partitions if item.verified), None
    )
    rows: list[CellMeasurementRow] = []
    for cell in cells:
        param_id = _as_text(_get(cell, "param_id"))
        if param_id is None:
            continue
        rows.append(
            CellMeasurementRow(
                run_id=run_id,
                product_id=product_id,
                param_id=param_id,
                state=str(_get(cell, "state")),
                status=_as_text(_get(cell, "status")),
                prompt_hash=_as_text(_get(cell, "prompt_hash")),
                model_id=_as_text(_get(cell, "model_id")),
                llm_scenario=_as_text(_get(cell, "llm_scenario")),
                index_build_id=_as_text(_get(cell, "index_build_id")),
                partition_build_id=partition_id,
                doc_set_digest=snapshot.doc_set_digest,
                coverage_digest=snapshot.coverage_digest,
                catalog_row_digest=None,
                cell_reuse_key=None,
                evidence_digest=None,
                measured_at=(
                    _as_text(_get(cell, "updated_at")) or completed_at or ""
                ),
            )
        )
    return tuple(rows)


def _partitions(
    *,
    product_id: str,
    package_id: str,
    manifest: Mapping[str, Any] | None,
    content_pairs: Sequence[tuple[str, str]],
    index_root: str | Path | None,
    run_id: str,
    completed_at: str | None,
    warnings: list[str],
) -> tuple[PartitionAdoption, ...]:
    """Партиция прогона: пересчёт ``content_digest`` и физическая сверка с диском.

    Два независимых контроля (09 §8.5): отпечаток содержимого пересчитывается по
    ``corpus_chunks`` и сверяется с ``index_manifest.content_digest``; затем партиция
    проверяется на диске (три артефакта + ``manifest.json`` с тем же ``content_digest`` и
    теми же версиями). Расхождение → партиция **не** принимается, причина видна: тихая
    запись неверного дайджеста сделала бы реестр ложным.
    """
    if manifest is None:
        return ()
    partition_id = _as_text(_get(manifest, "partition_build_id")) or _as_text(
        _get(manifest, "index_build_id")
    )
    if partition_id is None:
        return (
            PartitionAdoption(
                partition_build_id="",
                verified=False,
                reason="no_partition_build_id",
                content_digest=None,
                recomputed_digest=None,
                store_path="",
                chunks=0,
            ),
        )
    declared = _as_text(_get(manifest, "content_digest"))
    recomputed = _content_token(content_pairs)
    # Путь пересчитывается по раскладке §8.2 (``index_store/{package}/{build}/{product}/``),
    # а не берётся из манифеста на веру: реестр партиций обязан указывать на то место, где
    # партицию найдут читатели.
    store_path = f"index_store/{package_id}/{partition_id}/{product_id}/"
    declared_path = _as_text(_get(manifest, "store_path"))
    chunk_count = len(content_pairs)
    reason: str | None = None
    if declared is not None and declared != recomputed:
        reason = f"content_digest_mismatch:{declared}!={recomputed}"
    elif declared_path is not None and declared_path.rstrip("/") != store_path.rstrip("/"):
        reason = f"partition_store_path_mismatch:{declared_path}"
    elif index_root is None:
        reason = "index_root_missing"
    else:
        root = Path(index_root)
        if not is_committed(root, package_id, partition_id, product_id):
            reason = "partition_not_committed"
        else:
            try:
                on_disk = read_manifest(root, package_id, partition_id, product_id)
            except (OSError, ValueError) as exc:
                reason = f"partition_manifest_unreadable:{type(exc).__name__}"
            else:
                disk_digest = _as_text(on_disk.get("content_digest"))
                if disk_digest != (declared or recomputed):
                    reason = f"partition_disk_digest_mismatch:{disk_digest}"
    if reason is not None:
        if reason.startswith("content_digest_mismatch"):
            warnings.append(f"partition_content_digest_mismatch:{product_id}:{partition_id}")
        return (
            PartitionAdoption(
                partition_build_id=partition_id,
                verified=False,
                reason=reason,
                content_digest=declared,
                recomputed_digest=recomputed,
                store_path=store_path,
                chunks=chunk_count,
            ),
        )
    row = IndexPartitionRow(
        package_id=package_id,
        product_id=product_id,
        partition_build_id=partition_id,
        content_digest=declared or recomputed,
        chunker_version=_as_text(_get(manifest, "chunker_version")) or "",
        embed_model_id=_as_text(_get(manifest, "embed_model_id")) or "",
        extractor_version=_as_text(_get(manifest, "extractor_version")) or "",
        lexical_engine_version=_as_text(_get(manifest, "lexical_engine_version")) or "",
        vector_store=_as_text(_get(manifest, "vector_store")) or "",
        store_path=store_path,
        chunks=chunk_count,
        dim=_as_int(_get(manifest, "embed_dim")),
        created_at=_as_text(_get(manifest, "created_at")) or completed_at or "",
        last_used_run_id=run_id,
    )
    return (
        PartitionAdoption(
            partition_build_id=partition_id,
            verified=True,
            reason=None,
            content_digest=declared,
            recomputed_digest=recomputed,
            store_path=store_path,
            chunks=chunk_count,
            row=row,
        ),
    )


def _content_token(pairs: Iterable[tuple[str, str]]) -> str:
    """Отпечаток содержимого партиции — та же формула, что ``corpus_index._product_content_token``.

    Сортированные пары ``(chunk_id, content_hash)``, склеенные ``_FIELD_SEP``. Здесь
    ``chunk_id`` — **прогонное** пространство идентификаторов (``corpus_chunks.chunk_id``),
    а не durable ``chunk_meta.chunk_id``: именно его хешировал прогон при сборке партиции,
    поэтому сверка отпечатка имеет смысл только на нём.
    """
    material = _FIELD_SEP.join(
        f"{chunk_id}{_FIELD_SEP}{chunk_hash}"
        for chunk_id, chunk_hash in sorted(pairs)
    )
    return short_hash(material, 24)


def _chunk_ids_digest(rows: Sequence[tuple[ChunkRow, str]]) -> str:
    """``chunk_ids_digest`` версии — та же формула, что у корпуса при записи версии."""
    return short_hash(_FIELD_SEP.join(sorted(row.chunk_id for row, _text in rows)), 24)


def _normalize(url: str) -> str | None:
    """Ключ ``url_state``: канонизация §7.2 (:func:`core.config.seeds.normalize_seed_url`)."""
    try:
        return normalize_seed_url(url)
    except ValueError:
        return None


def _span_pair(
    start: Any, end: Any, text: str
) -> tuple[int | None, int | None] | None:
    """Смещения куска из источника: пара целиком и по длине текста, иначе ``None``.

    Два допустимых исхода: оба ``NULL`` (старая схема или абзацный чанкер — кусок не
    является точным срезом) и корректная пара (``0 <= start < end`` и
    ``end - start == len(text)``). ``None`` — отказ: рваная пара, перевёрнутый или
    несовпадающий по длине диапазон — признак несогласованной строки, и принимать её
    значило бы отложить отказ до ``CorpusChunk`` (а с ним — до цитаты, I22), то есть туда,
    где причина уже не видна.
    """
    if start is None and end is None:
        return (None, None)
    first, second = _as_int(start), _as_int(end)
    if first is None or second is None:
        return None
    if not 0 <= first < second or second - first != len(text):
        return None
    return (first, second)


def _as_int(value: Any) -> int | None:
    """``int`` или ``None`` (значение не выдумывается из ``None``)."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Запись (через порт)
# --------------------------------------------------------------------------- #


def apply_adoption(
    plan: AdoptionPlan,
    store: ProductStatePort | None,
    *,
    actor: str,
    cycle_id: str,
    dry_run: bool = False,
) -> AdoptionResult:
    """Пишет план в durable состояние пакета (идемпотентно, только через порт).

    Порядок записи продукта — по FK: ``document_version`` → ``chunk_store`` →
    ``chunk_meta`` → ``url_state`` → ``product_snapshot`` → ``product_state`` →
    ``index_partition`` → ``cell_measurement``. ``run_lineage`` пишется последним:
    ``previous_run_id``/``change_class`` считаются по **прежней** цепочке ``state.db``,
    иначе строка ссылалась бы на себя.

    Идемпотентность: ``upsert``-семантика порта по естественным ключам
    (``doc_version_id``, ``(product_id, normalized_url)``, ``snapshot_id``,
    ``(package, product, partition_build_id)``, ``(run_id, product, param)``). Повторный
    adoption того же прогона не плодит строк и не двигает ``created_at`` снимка
    (он берётся из прогона, а не из часов). ``first_seen_*`` существующей версии
    документа сохраняется: повторный импорт не выдаёт старый документ за новый.

    :param store: durable состояние; ``None`` допустим только при ``dry_run=True``.
    :param cycle_id: цикл записи; вызывающий передаёт объявленный (``--cycle-id``) либо
        выведенный планом из ``runs.snapshot_date``.
    :param dry_run: ничего не писать (результат — что было бы записано).
    :raises PackageAdminError: нет автора, нечего принимать или store не открыт.
    """
    actor_name = str(actor or "").strip()
    if not actor_name:
        raise PackageAdminError(
            "actor обязателен: adoption durable-состояния пишется с провенансом "
            "(--actor <человек>)",
            code="usage",
        )
    if plan.run_id is None:
        raise PackageAdminError(
            "принимать нечего: " + ("; ".join(plan.reasons) or "нет завершённого прогона"),
            code="not_found",
        )
    if not dry_run and store is None:
        raise PackageAdminError(
            "durable состояние не открыто: запись невозможна (state.db не создан)",
            code="state",
        )
    cycle = str(cycle_id or "").strip() or plan.cycle_id
    warnings = list(plan.warnings)
    # 09 §12.3: предшественник — по одной формуле (:func:`previous_run_of`). Повторный
    # adoption того же прогона не «предыдущий»: цепочка не замыкается на себя.
    previous = previous_run_of(store, plan.package_id, run_id=plan.run_id)
    previous_run_id = None if previous is None else previous.run_id

    results: list[ProductAdoptionResult] = []
    totals = {
        "documents": 0,
        "chunks": 0,
        "url_states": 0,
        "partitions": 0,
        "cells": 0,
        "snapshots": 0,
    }
    for item in plan.products:
        if not item.accepted:
            results.append(
                ProductAdoptionResult(
                    product_id=item.product_id,
                    adopted=False,
                    reasons=item.reasons,
                    documents=0,
                    chunks=0,
                    url_states=0,
                    partitions=0,
                    cells=0,
                )
            )
            continue
        result = _apply_product(
            plan, item, store, actor=actor_name, cycle_id=cycle, dry_run=dry_run
        )
        results.append(result)
        totals["documents"] += result.documents
        totals["chunks"] += result.chunks
        totals["url_states"] += result.url_states
        totals["partitions"] += result.partitions
        totals["cells"] += result.cells
        if item.snapshot is not None:
            totals["snapshots"] += 1

    lineage_written = False
    if plan.lineage is not None and not dry_run and store is not None:
        _write_lineage(
            plan, store, cycle_id=cycle, previous=previous, previous_run_id=previous_run_id
        )
        lineage_written = True
    return AdoptionResult(
        package_id=plan.package_id,
        run_id=plan.run_id,
        cycle_id=cycle,
        dry_run=dry_run,
        actor=actor_name,
        products=tuple(results),
        totals=totals,
        lineage_written=lineage_written,
        warnings=tuple(warnings),
        limitations=plan.limitations,
    )


def _apply_product(
    plan: AdoptionPlan,
    item: ProductAdoption,
    store: ProductStatePort | None,
    *,
    actor: str,
    cycle_id: str,
    dry_run: bool,
) -> ProductAdoptionResult:
    """Пишет один принятый продукт; при ``dry_run`` — только считает объём."""
    partitions = [
        part for part in item.partitions if part.verified and part.row is not None
    ]
    measurements = item.measurement_rows
    if dry_run or store is None:
        return ProductAdoptionResult(
            product_id=item.product_id,
            adopted=True,
            reasons=(),
            documents=len(item.document_rows),
            chunks=len(item.chunk_rows),
            url_states=len(item.url_rows),
            partitions=len(partitions),
            cells=len(measurements),
        )
    run_id = str(plan.run_id or "")
    # План 20 §3/§4: принятие артефакта — тоже **снятие**, но `complete=False`: то, что
    # артефакт предоставлен человеком, не является обходом корня и исчерпания не
    # удостоверяет (это факт, а не осторожность). Корень такого снятия — URL артефакта:
    # в adoption нет ни frontier, ни seed-корня, поэтому «корень» здесь — сам адрес
    # (детерминированно ``min(url)``, чтобы повторный adoption дал тот же ``capture_id``).
    adopted_at = _now_iso()
    adoption_root = min((row.url for row in item.url_rows), default=None)
    adoption_capture_id: str | None = None
    if adoption_root is not None:
        adoption_capture_id = capture_id_for(item.product_id, adoption_root, run_id)
        store.record_capture(
            CaptureRow(
                capture_id=adoption_capture_id,
                product_id=item.product_id,
                run_id=run_id,
                root_url=adoption_root,
                started_at=adopted_at,
                finished_at=adopted_at,
                complete=False,
                observed_how=CAPTURE_OBSERVED_ADOPTED,
                # ``policy_digest`` не выдумывается: adoption не разрешает политику
                # краула и не имеет объявленного пространства (режимы + лимиты), поэтому
                # отпечаток — ``None``/«неизвестно» (A3). Ложное «другое пространство» из
                # этого не следует: сверка решает только по наблюдённому несовпадению, а
                # ``complete=False`` adopted-снятие всё равно не удостоверяет ``bounded``.
                coverage_digest=(
                    None if item.snapshot is None else item.snapshot.coverage_digest
                ),
                doc_set_digest=(
                    None if item.snapshot is None else item.snapshot.doc_set_digest
                ),
                actor=actor,
            )
        )
    for doc_row in item.document_rows:
        existing = store.get_document_version(doc_row.doc_version_id)
        if existing is not None:
            # Провенанс первого появления не перезаписывается: adopted-документ,
            # известный с прошлого цикла, не должен выглядеть новым (docs_new).
            doc_row = dataclasses.replace(
                doc_row,
                first_seen_run_id=existing.first_seen_run_id,
                first_seen_at=existing.first_seen_at,
            )
        store.upsert_document_version(doc_row)
    for chunk_row, text in item.chunk_rows:
        store.put_chunk_text(chunk_row.content_hash, text, lang=chunk_row.lang)
    by_doc: dict[str, list[ChunkRow]] = {}
    for chunk_row, _text in item.chunk_rows:
        by_doc.setdefault(chunk_row.doc_version_id, []).append(chunk_row)
    for dv_id, rows in by_doc.items():
        store.replace_doc_chunks(dv_id, rows)
    for url_row in item.url_rows:
        if adoption_capture_id is not None:
            # Наблюдение артефакта: время принятия и актор — провенанс человека, а не
            # сети. ``observed_how='adopted'`` отличает его от ``live``/``revalidated``.
            url_row = dataclasses.replace(
                url_row,
                observed_at=adopted_at,
                observed_how=CAPTURE_OBSERVED_ADOPTED,
                capture_id=adoption_capture_id,
                observed_actor=actor,
            )
        store.upsert_url_state(url_row)
    _write_snapshot(plan, item, store, cycle_id=cycle_id, run_id=run_id)
    _write_product_state(plan, item, store, actor=actor, cycle_id=cycle_id, run_id=run_id)
    for part in partitions:
        assert part.row is not None  # noqa: S101 — отфильтровано выше
        store.register_index_partition(part.row)
    if measurements:
        store.record_cell_measurements(measurements)
    return ProductAdoptionResult(
        product_id=item.product_id,
        adopted=True,
        reasons=(),
        documents=len(item.document_rows),
        chunks=len(item.chunk_rows),
        url_states=len(item.url_rows),
        partitions=len(partitions),
        cells=len(measurements),
    )


def _write_snapshot(
    plan: AdoptionPlan,
    item: ProductAdoption,
    store: ProductStatePort,
    *,
    cycle_id: str,
    run_id: str,
) -> None:
    """Пишет ``product_snapshot``: счётчики документов — тем же правилом, что у прогона."""
    snapshot = item.snapshot
    if snapshot is None:
        return
    docs_total, docs_new, docs_reused = _doc_counters(
        store, item.product_id, run_id=run_id, coverage=item.coverage
    )
    store.record_snapshot(
        SnapshotRow(
            snapshot_id=snapshot_id(run_id, item.product_id),
            product_id=item.product_id,
            cycle_id=cycle_id,
            run_id=run_id,
            status=snapshot.status,
            scope=snapshot.scope,
            coverage_complete=snapshot.coverage_complete,
            coverage_digest=snapshot.coverage_digest,
            doc_set_digest=snapshot.doc_set_digest,
            partition_build_id=snapshot.partition_build_id,
            docs_total=docs_total,
            docs_reused=docs_reused,
            docs_new=docs_new,
            cells_total=snapshot.cells_total,
            cells_measured=snapshot.cells_measured,
            cells_pending=snapshot.cells_pending,
            baseline_complete=snapshot.baseline_complete,
            # Метка прогона, а не часов: повторный adoption не двигает created_at.
            created_at=_run_stamp(plan),
        )
    )


def _write_product_state(
    plan: AdoptionPlan,
    item: ProductAdoption,
    store: ProductStatePort,
    *,
    actor: str,
    cycle_id: str,
    run_id: str,
) -> None:
    """Двигает рубежи продукта по §5.1 — те же условия, что у прогона.

    ``last_corpus_run_id`` — покрытие есть; ``last_evaluated_run_id`` — фаза оценки прошла;
    ``last_completed_run_id``/``last_completed_cycle_id`` — снимок ``baseline_complete``.
    ``last_attempt_status`` получает словарь lineage (``complete``/``partial``/``failed``):
    отчётный статус прогона (``ok``/``blocked``) в БД не хранится и не выдумывается.
    """
    status = plan.lineage.status if plan.lineage is not None else "partial"
    existing = store.get_product_state(item.product_id)
    if existing is None:
        existing = ProductStateRow(
            product_id=item.product_id,
            canonical_name=item.product_id,
            onboarding_state=OnboardingState.READY,
            # Метка прогона, а не часы: повторный adoption не делает продукт «новым».
            added_at=_run_stamp(plan)[:10] or _dt.date.today().isoformat(),
            added_by=actor,
        )
    updates: dict[str, Any] = {
        "last_attempt_run_id": run_id,
        "last_attempt_status": status,
    }
    if item.coverage is not None:
        updates["last_corpus_run_id"] = run_id
        if plan.evaluate_ok:
            updates["last_evaluated_run_id"] = run_id
    snapshot = item.snapshot
    if snapshot is not None and snapshot.baseline_complete:
        updates["last_completed_run_id"] = run_id
        updates["last_completed_cycle_id"] = cycle_id
    store.upsert_product_state(dataclasses.replace(existing, **updates))


def _write_lineage(
    plan: AdoptionPlan,
    store: ProductStatePort,
    *,
    cycle_id: str,
    previous: RunLineageRow | None,
    previous_run_id: str | None,
) -> None:
    """Пишет ``run_lineage`` принятого прогона (цепочка циклов §12.3)."""
    lineage = plan.lineage
    if lineage is None:
        return
    change = classify_change(
        previous_run_id=previous_run_id,
        previous_products=None if previous is None else previous.products,
        current_products=list(lineage.products),
        previous_execution_contract_digest=(
            None if previous is None else previous.execution_contract_digest
        ),
        execution_contract_digest=lineage.execution_contract_digest,
    )
    store.record_run_lineage(
        RunLineageRow(
            run_id=lineage.run_id,
            package_id=lineage.package_id,
            cycle_id=cycle_id,
            previous_run_id=previous_run_id,
            cohort_digest=lineage.cohort_digest,
            seeds_digest=lineage.seeds_digest,
            execution_contract_digest=lineage.execution_contract_digest,
            change_class=change.value,
            status=lineage.status,
            started_at=lineage.started_at,
            completed_at=lineage.completed_at,
            products=list(lineage.products),
            refresh_policy=lineage.refresh_policy,
        )
    )


def _run_stamp(plan: AdoptionPlan) -> str:
    """Метка времени прогона для ``created_at`` снимка (стабильна между повторами)."""
    if plan.lineage is None:
        return ""
    return plan.lineage.completed_at or plan.lineage.started_at


# --------------------------------------------------------------------------- #
# Чтение ``state.db`` для ``hound package state show``
# --------------------------------------------------------------------------- #


def read_state_summary(db_path: str | Path, *, package_id: str) -> StateSummary:
    """Что сейчас лежит в ``state.db`` (read-only, без применения схемы).

    Штатное открытие (:func:`core.store.state_store.SqliteStateStore`) применяет схему и
    пишет ``schema_meta`` — для read-only команды это запись в файл состояния оператора.
    Поэтому здесь соединение ``mode=ro`` и явные ``SELECT``; отсутствующая таблица —
    «не наполнялось», а не ошибка.

    ``package_id`` участвует в выводе и в проверке ``run_lineage.package_id`` (чужая
    строка в файле пакета — видимая аномалия, а не тихий импорт).
    """
    path = Path(db_path)
    if not path.is_file():
        return StateSummary(
            db=str(path),
            exists=False,
            schema_version=None,
            empty=True,
            products=(),
            snapshots=(),
            lineage=(),
            counts={},
            warnings=(),
        )
    conn = _open_run_db(path)
    warnings: list[str] = []
    try:
        schema_version = _schema_version(conn)
        products: list[dict[str, Any]] = []
        if _has_table(conn, "product_state"):
            for row in conn.execute("SELECT * FROM product_state ORDER BY product_id"):
                products.append(
                    {
                        "product_id": row["product_id"],
                        "onboarding_state": row["onboarding_state"],
                        "onboarding_reason": _as_text(_get(row, "onboarding_reason")),
                        "last_corpus_run_id": _as_text(_get(row, "last_corpus_run_id")),
                        "last_evaluated_run_id": _as_text(
                            _get(row, "last_evaluated_run_id")
                        ),
                        "last_completed_run_id": _as_text(
                            _get(row, "last_completed_run_id")
                        ),
                        "last_completed_cycle_id": _as_text(
                            _get(row, "last_completed_cycle_id")
                        ),
                        "last_attempt_run_id": _as_text(_get(row, "last_attempt_run_id")),
                        "last_attempt_status": _as_text(_get(row, "last_attempt_status")),
                    }
                )
        snapshots: list[dict[str, Any]] = []
        if _has_table(conn, "product_snapshot"):
            for row in conn.execute(
                "SELECT * FROM product_snapshot ORDER BY product_id, created_at DESC,"
                " run_id DESC"
            ):
                snapshots.append(
                    {
                        "snapshot_id": row["snapshot_id"],
                        "product_id": row["product_id"],
                        "run_id": row["run_id"],
                        "cycle_id": row["cycle_id"],
                        "status": row["status"],
                        "scope": row["scope"],
                        "coverage_complete": bool(row["coverage_complete"]),
                        "docs_total": row["docs_total"],
                        "cells_total": row["cells_total"],
                        "cells_pending": row["cells_pending"],
                        "baseline_complete": bool(row["baseline_complete"]),
                        "created_at": row["created_at"],
                    }
                )
        lineage: list[dict[str, Any]] = []
        if _has_table(conn, "run_lineage"):
            for row in conn.execute(
                "SELECT * FROM run_lineage ORDER BY started_at DESC, run_id DESC"
            ):
                row_package = _as_text(_get(row, "package_id"))
                if row_package is not None and row_package != package_id:
                    warnings.append(
                        f"lineage_package_mismatch:{row['run_id']}:{row_package}"
                    )
                lineage.append(
                    {
                        "run_id": row["run_id"],
                        "package_id": row_package,
                        "cycle_id": row["cycle_id"],
                        "previous_run_id": _as_text(_get(row, "previous_run_id")),
                        "execution_contract_digest": _as_text(
                            _get(row, "execution_contract_digest")
                        ),
                        "change_class": _as_text(_get(row, "change_class")),
                        "status": row["status"],
                        "started_at": row["started_at"],
                        "completed_at": _as_text(_get(row, "completed_at")),
                        "products": _json_list(_get(row, "products_json")),
                    }
                )
        substrate = _substrate_counts(conn)
        counts = {
            table: _count(conn, table)
            for table in (
                "document_version",
                "chunk_meta",
                "chunk_store",
                "url_state",
                "index_partition",
                "cell_measurement",
                "product_snapshot",
                "run_lineage",
            )
        }
    finally:
        conn.close()
    return StateSummary(
        db=str(path),
        exists=True,
        schema_version=schema_version,
        empty=not products and not lineage,
        products=tuple(products),
        snapshots=tuple(snapshots),
        lineage=tuple(lineage),
        counts=counts,
        warnings=tuple(warnings),
        substrate=substrate,
    )


def _substrate_counts(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    """Документы/чанки/партиции по продуктам — из durable-таблиц, не из рубежей.

    Разрез обязателен: ``product_state`` пишется в конце прогона
    (``record_run_outcome``), а субстрат — на границе продукта (§4.7). Убитый прогон
    оставляет одно без другого, и «пусто» перестаёт отличаться от «данные есть, рубежа
    нет». Пустой словарь — субстрата нет вовсе.
    """
    result: dict[str, dict[str, int]] = {}
    for table, key in (
        ("document_version", "docs"),
        ("chunk_meta", "chunks"),
        ("index_partition", "partitions"),
    ):
        if not _has_table(conn, table):
            continue
        for row in conn.execute(
            f"SELECT product_id AS pid, COUNT(*) AS n FROM {table} GROUP BY product_id"  # noqa: S608 - имя таблицы из литерала
        ):
            pid = _as_text(row["pid"])
            if pid is None:
                continue
            result.setdefault(pid, {"docs": 0, "chunks": 0, "partitions": 0})[key] = int(
                row["n"]
            )
    return result

def _count(conn: sqlite3.Connection, table: str) -> int:
    """Число строк таблицы (``0``, если таблицы нет).

    ``table`` приходит из фиксированного кортежа в :func:`read_state_summary`, а не из
    входа — поэтому подстановка имени безопасна (S608: значения всегда bind).
    """
    if not _has_table(conn, table):
        return 0
    row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()  # noqa: S608 - имя таблицы из литерала
    return int(row["n"]) if row is not None else 0
