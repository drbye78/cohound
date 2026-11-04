"""coHound — реализация ``ProductStatePort`` поверх ``state.db`` (09 §4.1/§4.4).

``SqliteStateStore`` — операционное хранилище durable per-product состояния пакета:
``state/{package_id}/state.db`` (схема — ``core/store/state_sqlite.py``). Все записи
идемпотентны (``INSERT ... ON CONFLICT DO UPDATE``), транзакции короткие (одна операция —
одна транзакция, WAL включён схемой), ``close()`` освобождает соединение.

Семантика seed-набора (§5.3): авторский ``sources/seeds.yaml`` — источник истины;
``reconcile_seed_set`` делает upsert по ``(product_id, normalized_url)``, а seed'ы,
отсутствующие в файле, помечает ``state=disabled`` + ``retired_at`` (никогда не удаляет) —
удаление из git обратимо и явно. Та же сверка — единственный писатель меток
``product_state.retired_at``/``re_added_at``: пустой набор означает retirement, возврат
активного seed'а — ``re_added_at`` (``_mode_for`` даёт ``new``), а завершённый прогон после
возврата метку снимает.

``forget_product`` — человеческое решение «забыть продукт»: консистентный снимок
``state.db`` рядом с файлом (``sqlite3`` backup API) и удаление строк **состояния**
продукта одной транзакцией; история прогонов (``run_lineage``/``cycle``) не трогается.
Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path

from core.config.seeds import seed_id
from core.domain.enums import (
    CrawlPolicyMode,
    CrawlPolicyScopeKind,
    Gate,
    OnboardingState,
    SeedRole,
    SeedSource,
    SeedState,
)
from core.domain.models import ProfileAttestationRow, SeedEntry
from core.ports.state import (
    CapturedDocRow,
    CellMeasurementRow,
    ChunkRow,
    ContainerStateRow,
    CorpusCaptureRow,
    CrawlPolicyRow,
    DocumentVersionRow,
    ForgetProductResult,
    IndexCompositionRow,
    IndexPartitionRow,
    ProductStateRow,
    RunLineageRow,
    SeedRow,
    SnapshotRow,
    UrlStateRow,
)
from core.store.state_sqlite import connect_state

__all__ = [
    "FORGET_PRODUCT_TABLES",
    "SqliteStateStore",
    "backup_state_db",
    "read_crawl_observations",
    "read_crawl_policy_rows",
    "read_previous_cycle_id",
    "read_product_counts",
]

#: Выборка последней строки lineage пакета — **одна** формула на штатное чтение
#: (:meth:`SqliteStateStore.previous_lineage`) и на чтение без записи
#: (:func:`read_previous_cycle_id`). Порядок ``started_at DESC, run_id DESC`` — тот же,
#: что у lineage: «последний» не должен означать разное в двух местах.
_PREVIOUS_LINEAGE_SQL = (
    "SELECT * FROM run_lineage WHERE package_id = ? "
    "ORDER BY started_at DESC, run_id DESC LIMIT 1"
)


def read_previous_cycle_id(db_path: str | Path, package_id: str) -> str | None:
    """Последний ``cycle_id`` пакета — **без записи** в ``state.db`` (``mode=ro``).

    Зачем отдельная функция: штатное открытие (:func:`connect_state`) применяет схему и
    пишет ``schema_meta.schema_version`` — идемпотентно, но это **запись**, а предсказание
    идентичности (``hound plan``) обещает «ничего не пишет». Кроме того, открытие меняет файл
    состояния оператора ради ответа на один вопрос.

    Читается одна колонка ``cycle_id`` (она есть с первой версии схемы, семантика не
    менялась). Гейт версии здесь сознательно **не** применяется: этот путь ничего не пишет и
    не объявляет файл текущим, а отказ предсказания из-за чужой версии ``state.db`` скрыл бы
    ответ «истории нет». Отказ по версии всё равно виден — но на штатном открытии прогона.

    Отсутствующий файл — ``None`` («истории нет»), а не ошибка: ``plan`` в чистом каталоге —
    законный режим.

    Только для чтения: писать через этот путь нельзя (соединение открыто ``mode=ro``).
    """
    path = Path(db_path)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            _PREVIOUS_LINEAGE_SQL, (str(package_id),)
        ).fetchone()
    except sqlite3.DatabaseError:
        # Битый/несовместимый файл состояния не должен подменять отказ прогона отказом
        # предсказания: «история неизвестна» — честное состояние (A3), а не выдумка.
        return None
    finally:
        conn.close()
    return None if row is None else str(row["cycle_id"])


def _crawl_policy_row(row: sqlite3.Row) -> CrawlPolicyRow:
    """Строка SQLite ``crawl_policy`` → :class:`CrawlPolicyRow` (enum'ы — из строк)."""
    return CrawlPolicyRow(
        policy_id=int(row["policy_id"]),
        product_id=str(row["product_id"]),
        scope_kind=CrawlPolicyScopeKind(row["scope_kind"]),
        host=row["host"],
        path=row["path"],
        mode=CrawlPolicyMode(row["mode"]),
        interval_hours=row["interval_hours"],
        note=row["note"],
        actor=str(row["actor"]),
        created_at=str(row["created_at"]),
        superseded_by=row["superseded_by"],
    )


def read_crawl_policy_rows(
    db_path: str | Path, *, include_superseded: bool = False
) -> list[CrawlPolicyRow]:
    """Действующие записи ``crawl_policy`` — **без записи** в ``state.db`` (``mode=ro``).

    Нужен ``plan``/``list``: предсказание идентичности обещает «ничего не пишет», а
    штатное открытие (:func:`connect_state`) пишет ``schema_meta.schema_version``.
    Отсутствующий файл/таблица (и битый файл) — ``[]``: политика неизвестна, а не
    «пустая и это факт»; отказ по версии всё равно виден на штатном открытии прогона.
    """
    path = Path(db_path)
    if not path.exists():
        return []
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        sql = "SELECT * FROM crawl_policy"
        if not include_superseded:
            sql += " WHERE superseded_by IS NULL"
        sql += " ORDER BY product_id, scope_kind, host, path, policy_id"
        return [_crawl_policy_row(row) for row in conn.execute(sql)]
    except sqlite3.DatabaseError:
        return []
    finally:
        conn.close()


def read_crawl_observations(db_path: str | Path) -> dict[tuple[str, str], str]:
    """Наблюдения ``url_state`` продукта: ``{(product_id, url): last_checked_at}``.

    Только читает (``mode=ro``). Используется разрешением ``scheduled``: «последнее
    наблюдение корня» — это максимум по URL под ним, а не отметка самой записи политики
    (у неё нет времени наблюдения, только ``interval_hours``).
    """
    path = Path(db_path)
    if not path.exists():
        return {}
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT product_id, url, normalized_url, last_checked_at FROM url_state"
            " WHERE last_checked_at IS NOT NULL"
        )
        return {
            (str(row["product_id"]), str(row["normalized_url"] or row["url"])): str(
                row["last_checked_at"]
            )
            for row in rows
        }
    except sqlite3.DatabaseError:
        return {}
    finally:
        conn.close()

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

#: Колонки ``url_state`` в порядке записи (кроме составного ключа
#: ``(product_id, normalized_url)``).
_URL_STATE_COLUMNS = (
    "url",
    "doc_version_id",
    "etag",
    "last_modified",
    "last_http_status",
    "last_checked_at",
    "last_changed_at",
    "next_check_at",
    "consecutive_failures",
    "last_error_kind",
    "reuse_kind",
    "seed_id",
)

#: Колонки ``container_state`` в порядке записи (кроме составного ключа
#: ``(product_id, normalized_url)``). ``first_seen_*`` здесь **нет** намеренно: первое
#: наблюдение контейнера не перезаписывается повторной записью.
_CONTAINER_STATE_COLUMNS = (
    "url",
    "archive_content_hash",
    "raw_path",
    "format",
    "status",
    "last_seen_run_id",
    "last_seen_at",
)

#: Upsert контейнера: обновляет всё, кроме ``first_seen_*`` — «когда URL впервые оказался
#: контейнером» остаётся фактом истории (та же дисциплина, что у ``superseded_at``
#: аттестации профиля).
_CONTAINER_STATE_UPSERT = (
    "INSERT INTO container_state (product_id, normalized_url, "  # noqa: S608 - имена колонок из константы, значения — bind
    "first_seen_run_id, first_seen_at, "
    + ", ".join(_CONTAINER_STATE_COLUMNS)
    + ") VALUES (?, ?, ?, ?, "
    + ", ".join("?" for _ in _CONTAINER_STATE_COLUMNS)
    + ") ON CONFLICT(product_id, normalized_url) DO UPDATE SET "
    + ", ".join(
        # Формат, не названный повторным наблюдением (проход без сети не классифицирует
        # байты заново), не стирает уже известный: COALESCE-семантика — та же дисциплина,
        # что у валидаторов ``url_state``.
        (
            "format = COALESCE(excluded.format, container_state.format)"
            if name == "format"
            else f"{name} = excluded.{name}"
        )
        for name in _CONTAINER_STATE_COLUMNS
    )
)

#: Колонки ``document_version`` в порядке записи (кроме ``doc_version_id`` — он ключ).
_DOCUMENT_VERSION_COLUMNS = (
    "doc_id",
    "product_id",
    "url",
    "content_hash",
    "extractor_version",
    "level",
    "raw_path",
    "chunker_version",
    "chunk_ids_digest",
    # v5 (частичность извлечения): ``None`` пишется как SQL NULL = «не измерено».
    "extract_partial",
    "extract_partial_reason",
    "extract_pages_converted",
    "extract_page_count",
    "first_seen_run_id",
    "last_seen_run_id",
    "first_seen_at",
    "last_seen_at",
    "status",
)

#: Колонки ``chunk_meta`` в порядке записи (``chunk_id`` — ключ, идёт первым).
_CHUNK_COLUMNS = (
    "chunk_id",
    "doc_version_id",
    "product_id",
    "chunk_no",
    "content_hash",
    "chunker_version",
    "heading_path",
    "context_prefix",
    "lang",
    "char_start",
    "char_end",
)

#: Колонки ``cell_measurement`` в порядке записи (кроме составного ключа
#: ``(run_id, product_id, param_id)``).
_CELL_MEASUREMENT_COLUMNS = (
    "state",
    "status",
    "prompt_hash",
    "model_id",
    "llm_scenario",
    "index_build_id",
    "partition_build_id",
    "doc_set_digest",
    "coverage_digest",
    "catalog_row_digest",
    "cell_reuse_key",
    "evidence_digest",
    "measured_at",
)

#: Колонки ``profile_attestation`` в порядке записи (кроме ``attestation_id`` — он ключ).
#: ``digest_material`` в БД — JSON-текст (сериализуется в :meth:`_save_profile_attestation`).
_PROFILE_ATTESTATION_COLUMNS = (
    "case_id",
    "package_id",
    "anchor",
    "gate",
    "profile_digest",
    "digest_material",
    "attested_by",
    "attested_at",
    "superseded_at",
    "notes",
)

#: Upsert аттестации профиля. Отличие от :func:`_upsert_sql`: ``superseded_at`` не
#: затирается повторной подачей — отозванная аттестация не «воскресает» молча
#: (COALESCE берёт уже проставленную метку, если она есть).
_PROFILE_ATTESTATION_UPSERT = (
    f"INSERT INTO profile_attestation (attestation_id, "  # noqa: S608 - имена из _PROFILE_ATTESTATION_COLUMNS, значения — bind
    f"{', '.join(_PROFILE_ATTESTATION_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in range(len(_PROFILE_ATTESTATION_COLUMNS) + 1))}) "
    "ON CONFLICT(attestation_id) DO UPDATE SET "
    + ", ".join(
        f"{name} = excluded.{name}"
        for name in _PROFILE_ATTESTATION_COLUMNS
        if name != "superseded_at"
    )
    + ", superseded_at = COALESCE(profile_attestation.superseded_at,"
    " excluded.superseded_at)"
)


def _now() -> str:
    """Текущее время в ISO-8601 (UTC, ``Z``) — как в остальных TEXT-колонках."""
    return (
        _dt.datetime.now(_dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _upsert_sql(
    table: str, key: str | Sequence[str], columns: Sequence[str]
) -> str:
    """Строит ``INSERT ... ON CONFLICT(key) DO UPDATE`` для полного набора колонок.

    ``key`` — одна колонка или составной ключ (например, ``url_state``:
    ``(product_id, normalized_url)``).

    Все аргументы — **имена**, не данные, и все вызовы модуль-приватны: ``table`` и
    ``key`` передаются литералами, ``columns`` — модульной константой
    (``_PRODUCT_STATE_COLUMNS`` и т. п.). Внешний вход (URL, id продукта, имя файла)
    сюда не попадает: значения идут bind-параметрами в ``execute``.
    """
    keys = (key,) if isinstance(key, str) else tuple(key)
    names = ", ".join((*keys, *columns))
    placeholders = ", ".join("?" for _ in range(len(keys) + len(columns)))
    updates = ", ".join(f"{name} = excluded.{name}" for name in columns)
    return (
        f"INSERT INTO {table} ({names}) VALUES ({placeholders}) "  # noqa: S608 - только имена таблицы/колонок, см. докстринг
        f"ON CONFLICT({', '.join(keys)}) DO UPDATE SET {updates}"
    )


#: Таблицы ``state.db``, строки которых принадлежат **продукту** (порядок — порядок
#: удаления: ``chunk_meta`` ссылается на ``document_version`` и ``chunk_store``, поэтому
#: уходит первым). Намеренно **не** входят: ``run_lineage``/``cycle`` (история прогонов и
#: циклов), ``index_composition`` (иммутабельный реестр композиций когорты — общий для
#: продуктов) и ``profile_attestation`` (человеческая аттестация профиля пакета-якоря, не
#: свойство продукта). Сборки ``index_store/`` на диске тоже остаются: их прунит
#: ``hound package index-gc``, а не забывание продукта.
FORGET_PRODUCT_TABLES = (
    "chunk_meta",
    "document_version",
    # Дочерняя таблица — раньше родительской: без ``ON DELETE CASCADE`` удаление
    # ``corpus_capture`` при включённых FK отказало бы на существующих строках.
    "corpus_capture_doc",
    "corpus_capture",
    "container_state",
    "url_state",
    "product_snapshot",
    "cell_measurement",
    "seed_set",
    "product_state",
    "attestation",
    "index_partition",
)

#: Тексты ``chunk_store``, которые уйдут вместе с продуктом: их знает только он
#: (осиротевшие). Текст, разделяемый другим продуктом, остаётся — адресация по
#: ``content_hash`` (I19) не даёт удалить чужое доказательство.
_ORPHAN_CHUNK_COUNT_SQL = (
    "SELECT COUNT(*) FROM chunk_store"
    " WHERE content_hash IN (SELECT content_hash FROM chunk_meta WHERE product_id = ?)"
    " AND content_hash NOT IN"
    " (SELECT content_hash FROM chunk_meta WHERE product_id <> ?)"
)

#: Удаление осиротевшего текста — по одному ``content_hash`` (после удаления ``chunk_meta``).
_DELETE_ORPHAN_CHUNK_SQL = (
    "DELETE FROM chunk_store WHERE content_hash = ? AND NOT EXISTS ("
    "SELECT 1 FROM chunk_meta WHERE chunk_meta.content_hash = chunk_store.content_hash)"
)


def _timestamp_token(at: str | None) -> str:
    """Метка времени для имени снимка: ``2026-10-30T12:00:00Z`` → ``20261030T120000Z``."""
    stamp = at or _now()
    return str(stamp).replace("-", "").replace(":", "")


def _snapshot_path(db_path: Path, at: str | None) -> Path:
    """Путь снимка рядом с ``state.db``; существующий файл не перезаписывается.

    Метка времени — секунды, поэтому два снимка в одну секунду (или повторный запуск в
    тесте) получают суффикс ``-2``/``-3``: снимок оператора не затирается молча.
    """
    base = f"{db_path.name}.bak-{_timestamp_token(at)}"
    candidate = db_path.with_name(base)
    counter = 1
    while candidate.exists():
        counter += 1
        candidate = db_path.with_name(f"{base}-{counter}")
    return candidate


def _snapshot_into(conn: sqlite3.Connection, target: Path) -> Path:
    """Консистентный снимок открытой БД в ``target`` (``sqlite3`` backup API).

    Не копирование файла: при WAL копия байтов может рассогласоваться с ``-wal``/``-shm``.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    dest = sqlite3.connect(str(target))
    try:
        conn.backup(dest)
    finally:
        dest.close()
    return target


def backup_state_db(db_path: str | Path, *, at: str | None = None) -> Path:
    """Снимок ``state.db`` рядом с файлом (09 §14.4; ``sqlite3`` backup API).

    ``:memory:``-БД и отсутствующий файл — ``ValueError``: снимок кладётся **рядом** с
    ``state.db``, и «некуда положить» не должно превращаться в тихое отсутствие бэкапа.
    """
    path = Path(db_path)
    if str(path) == ":memory:" or not path.exists():
        raise ValueError(
            f"backup_state_db: нужен файл state.db рядом с которым кладётся снимок: {path}"
        )
    target = _snapshot_path(path, at)
    conn = sqlite3.connect(str(path))
    try:
        return _snapshot_into(conn, target)
    finally:
        conn.close()


def _product_row_counts(conn: sqlite3.Connection, product_id: str) -> dict[str, int]:
    """Число строк продукта по таблицам состояния (``chunk_store`` — осиротевшие тексты).

    Таблица без колонки ``product_id`` сюда попасть не может: ``FORGET_PRODUCT_TABLES`` —
    модульная константа с именами, значения идут bind-параметрами.
    """
    counts: dict[str, int] = {}
    for table in FORGET_PRODUCT_TABLES:
        counts[table] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE product_id = ?",  # noqa: S608 - имя из константы
                (product_id,),
            ).fetchone()[0]
        )
    counts["chunk_store"] = int(
        conn.execute(
            _ORPHAN_CHUNK_COUNT_SQL, (product_id, product_id)
        ).fetchone()[0]
    )
    return counts


def read_product_counts(
    db_path: str | Path, product_id: str
) -> dict[str, int] | None:
    """Строки продукта по таблицам **без записи** в ``state.db`` (``mode=ro``).

    Нужно ``--dry-run``: штатное открытие (:func:`connect_state`) применяет схему и пишет
    ``schema_meta`` — это запись в файл оператора, а флаг обещает, что её нет.
    Отсутствующий файл — ``None`` («состояния нет»), а не ошибка. Гейт версии, как и в
    :func:`read_previous_cycle_id`, здесь не применяется: путь ничего не пишет и не объявляет
    файл текущим.
    """
    path = Path(db_path)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        conn.row_factory = sqlite3.Row
        return _product_row_counts(conn, product_id)
    finally:
        conn.close()


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
    def _retired_evidence(self, product_id: str) -> bool:
        """Был ли продукт выведен из работы — по **durable** состоянию (09 §5.1).

        Два независимых свидетеля:

        * строка ``product_state`` помечена ``retired`` (``onboarding_state='retired'``
          либо ``retired_at``) — решение человека о продукте;
        * **весь** durable seed-набор продукта выведен: у каждой строки ``seed_set``
          ``state='disabled'`` **и** ``retired_at`` (метку ставит
          :meth:`reconcile_seed_set`, выключая seed'ы, исчезнувшие из авторского файла).
          Seed, выключенный автором (``enabled: false``, ``retired_at IS NULL``), сюда не
          относится: это не retirement, а отключение записи.

        Строки нет и seed'ов нет — retirement не наблюдался (``False``): «не было» не равно
        «было и прошло».
        """
        row = self._conn.execute(
            "SELECT onboarding_state, retired_at FROM product_state WHERE product_id = ?",
            (product_id,),
        ).fetchone()
        if row is not None and (
            row["retired_at"] is not None
            or str(row["onboarding_state"]) == OnboardingState.RETIRED.value
        ):
            return True
        seeds = self._conn.execute(
            "SELECT COUNT(*) AS total,"
            " COALESCE(SUM(CASE WHEN state = ? AND retired_at IS NOT NULL"
            " THEN 1 ELSE 0 END), 0) AS retired"
            " FROM seed_set WHERE product_id = ?",
            (SeedState.DISABLED.value, product_id),
        ).fetchone()
        total = int(seeds["total"] or 0)
        return total > 0 and int(seeds["retired"] or 0) == total

    def _consume_re_added_at(self, product_id: str) -> None:
        """Снимает ``re_added_at``, если возврат уже отработан прогоном после метки.

        ``_mode_for`` отдаёт ``new`` при любой непустой метке, поэтому «вечная» метка
        означала бы вечный полный краул. Метка живёт до первого прогона, который после неё
        **собрал корпус** (``last_corpus_run_id``) или завершил измерение
        (``last_completed_run_id``) — оба рубежа двигает ``record_run_outcome``.

        Почему не только ``last_completed_run_id``: рубеж «все ячейки разрешены» на живой
        когорте не достигается (``pending``-ячейки уходят в очередь ревью), и метка осталась
        бы навсегда. Корпус же собирается в любом дошедшем до Ш5 прогоне — это и есть
        «повторный полный краул случился».

        Прогон без ``completed_at`` (упал/идёт) или неизвестный в ``run_lineage`` метку не
        снимает: baseline ещё устарел, и это честное состояние, а не повод объявить его
        свежим.
        """
        self._conn.execute(
            """
            UPDATE product_state SET re_added_at = NULL
             WHERE product_id = ?
               AND re_added_at IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM run_lineage
                    WHERE run_lineage.run_id IN (product_state.last_corpus_run_id,
                                                 product_state.last_completed_run_id)
                      AND run_lineage.completed_at IS NOT NULL
                      AND run_lineage.completed_at >= product_state.re_added_at
               )
            """,
            (product_id,),
        )

    def reconcile_seed_set(
        self, product_id: str, seeds: Sequence[SeedEntry]
    ) -> None:
        """Upsert авторских seed'ов; отсутствующие в наборе — ``disabled`` + ``retired_at``.

        ``added_at``/``added_by`` существующей строки не перезаписываются (провенанс
        первого появления), ``retired_at`` снимается, если seed вернулся в файл.

        Здесь же — переходы человеческого решения «выведен/возвращён» (09 §5.1–§5.3),
        потому что ``sources/seeds.yaml`` — авторский источник, а таблица — его durable
        отражение; отдельной команды у retirement нет:

        * **пустой набор** (в файле не осталось записей продукта) ⇒ продукт помечается
          ``retired`` (``onboarding_state`` + ``retired_at``). Раньше выключались только
          строки ``seed_set``, а ``product_state`` оставался ``ready`` — два разных факта об
          одном продукте. Набор, где все записи выключены автором, retirement **не**
          объявляет: файл по-прежнему их перечисляет;
        * **возврат активного seed'а после retirement** ⇒ ``re_added_at`` = момент перехода,
          ``retired_at`` снимается: ``_mode_for`` даёт ``new`` (полный краул — состояние за
          время отсутствия устарело);
        * **метка снимается**, когда после неё есть завершённый прогон
          (:meth:`_consume_re_added_at`) — иначе продукт остался бы ``new`` навсегда.

        Готовность (``ready``/``pending_seeds``) материализует ``sync_product_state`` по плану
        префлайта; здесь снимается только отметка retirement — вместе с состоянием
        ``retired``, которое перестало быть верным (``retired_at`` и ``re_added_at``
        одновременно не живут).
        """
        now = _now()
        incoming = {seed.normalized_url for seed in seeds}
        active = any(seed.is_enabled for seed in seeds)
        was_retired = self._retired_evidence(product_id)
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
            if not seeds:
                # Набора нет — продукт выведен из работы: пишем это в product_state, а не
                # только в seed_set, иначе возврат неотличим от первого появления.
                # Пустой набор ≠ «все seed'ы выключены автором»: во втором случае файл
                # по-прежнему перечисляет записи, и продукт лишь остался без активного
                # корпуса (``pending_seeds``/``blocked``) — retirement не объявляется.
                self._conn.execute(
                    "UPDATE product_state SET onboarding_state = ?,"
                    " retired_at = COALESCE(retired_at, ?),"
                    " onboarding_reason = COALESCE(onboarding_reason, ?)"
                    " WHERE product_id = ?",
                    (
                        OnboardingState.RETIRED.value,
                        now,
                        "no_seeds",
                        product_id,
                    ),
                )
            elif active and was_retired:
                self._conn.execute(
                    "UPDATE product_state SET re_added_at = ?, retired_at = NULL,"
                    " onboarding_state = ?, onboarding_reason = NULL"
                    " WHERE product_id = ?",
                    (now, OnboardingState.READY.value, product_id),
                )
            else:
                self._consume_re_added_at(product_id)

    def forget_product(
        self, product_id: str, *, at: str | None = None
    ) -> ForgetProductResult:
        """Удаляет строки состояния продукта после снимка ``state.db`` (09 §4.1).

        Условие, а не удобство: снимок делается **до** удаления (консистентно, через
        ``sqlite3`` backup API) и кладётся рядом с ``state.db``; при невозможности снимка
        метод отказывает и ничего не удаляет. Удаление — одна транзакция: ошибка на любой
        таблице откатывает всё, включая уже удалённые строки.

        Что остаётся намеренно: ``run_lineage``/``cycle`` (история прогонов — провенанс, а
        не состояние продукта), ``index_composition`` (иммутабельный реестр композиций
        когорты), ``profile_attestation`` (аттестация профиля пакета-якоря) и сборки
        ``index_store/`` на диске (их прунит ``hound package index-gc``).

        Продукта в состоянии нет — ``found=False``, снимок не создаётся: удалять нечего, и
        бэкап «на всякий случай» не выдаётся за выполненную работу.
        """
        counts = _product_row_counts(self._conn, product_id)
        if sum(counts.values()) == 0:
            return ForgetProductResult(
                product_id=product_id, found=False, counts=counts
            )
        path = Path(self.path)
        if str(path) == ":memory:" or not path.exists():
            raise ValueError(
                "forget_product: нужен файловый state.db — снимок кладётся рядом с ним"
            )
        backup = _snapshot_into(self._conn, _snapshot_path(path, at))
        with self._conn:
            hashes = [
                str(row["content_hash"])
                for row in self._conn.execute(
                    "SELECT content_hash FROM chunk_meta WHERE product_id = ?",
                    (product_id,),
                )
            ]
            for table in FORGET_PRODUCT_TABLES:
                self._conn.execute(
                    f"DELETE FROM {table} WHERE product_id = ?",  # noqa: S608 - имя из константы
                    (product_id,),
                )
            for content_hash in sorted(set(hashes)):
                self._conn.execute(_DELETE_ORPHAN_CHUNK_SQL, (content_hash,))
        return ForgetProductResult(
            product_id=product_id,
            found=True,
            counts=counts,
            backup_path=str(backup),
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

    # -- URL/document/chunk (MVP-3a, 09 §4.2/§6.3) ------------------------- #
    @staticmethod
    def _url_state_row(row: sqlite3.Row) -> UrlStateRow:
        """Строка SQLite → :class:`UrlStateRow`."""
        return UrlStateRow(
            product_id=row["product_id"],
            url=row["url"],
            normalized_url=row["normalized_url"],
            doc_version_id=row["doc_version_id"],
            etag=row["etag"],
            last_modified=row["last_modified"],
            last_http_status=row["last_http_status"],
            last_checked_at=row["last_checked_at"],
            last_changed_at=row["last_changed_at"],
            next_check_at=row["next_check_at"],
            consecutive_failures=row["consecutive_failures"],
            last_error_kind=row["last_error_kind"],
            reuse_kind=row["reuse_kind"],
            seed_id=row["seed_id"],
        )

    def get_url_state(
        self, product_id: str, normalized_url: str
    ) -> UrlStateRow | None:
        row = self._conn.execute(
            "SELECT * FROM url_state WHERE product_id = ? AND normalized_url = ?",
            (product_id, normalized_url),
        ).fetchone()
        return self._url_state_row(row) if row is not None else None

    def iter_url_state(self, product_id: str) -> list[UrlStateRow]:
        rows = self._conn.execute(
            "SELECT * FROM url_state WHERE product_id = ? ORDER BY normalized_url",
            (product_id,),
        ).fetchall()
        return [self._url_state_row(row) for row in rows]

    def upsert_url_state(self, row: UrlStateRow) -> None:
        values = [getattr(row, name) for name in _URL_STATE_COLUMNS]
        with self._conn:
            self._conn.execute(
                _upsert_sql(
                    "url_state",
                    ("product_id", "normalized_url"),
                    _URL_STATE_COLUMNS,
                ),
                (row.product_id, row.normalized_url, *values),
            )

    @staticmethod
    def _container_state_row(row: sqlite3.Row) -> ContainerStateRow:
        """Строка SQLite → :class:`ContainerStateRow` (поля — как объявлены в DDL)."""
        return ContainerStateRow(
            product_id=str(row["product_id"]),
            url=str(row["url"]),
            normalized_url=str(row["normalized_url"]),
            archive_content_hash=str(row["archive_content_hash"]),
            raw_path=str(row["raw_path"]),
            format=row["format"],
            status=str(row["status"]),
            first_seen_run_id=str(row["first_seen_run_id"]),
            last_seen_run_id=str(row["last_seen_run_id"]),
            first_seen_at=str(row["first_seen_at"]),
            last_seen_at=str(row["last_seen_at"]),
        )

    def record_container_state(self, row: ContainerStateRow) -> None:
        """Пишет наблюдение контейнера; ``first_seen_*`` не перезаписывается.

        Идемпотентно по ``(product_id, normalized_url)``: повторный спуск того же архива
        обновляет хеш/путь/формат/исход и ``last_seen_*``, а первое наблюдение остаётся.
        """
        values = [getattr(row, name) for name in _CONTAINER_STATE_COLUMNS]
        with self._conn:
            self._conn.execute(
                _CONTAINER_STATE_UPSERT,
                (
                    row.product_id,
                    row.normalized_url,
                    row.first_seen_run_id,
                    row.first_seen_at,
                    *values,
                ),
            )

    def get_container_state(
        self, product_id: str, normalized_url: str
    ) -> ContainerStateRow | None:
        """Строка контейнера по нормализованному URL (``None`` — URL не был контейнером)."""
        row = self._conn.execute(
            "SELECT * FROM container_state WHERE product_id = ? AND normalized_url = ?",
            (product_id, normalized_url),
        ).fetchone()
        return self._container_state_row(row) if row is not None else None

    def iter_container_state(self, product_id: str) -> list[ContainerStateRow]:
        """Все строки ``container_state`` продукта в порядке ``normalized_url``."""
        rows = self._conn.execute(
            "SELECT * FROM container_state WHERE product_id = ?"
            " ORDER BY normalized_url",
            (product_id,),
        ).fetchall()
        return [self._container_state_row(row) for row in rows]

    @staticmethod
    def _document_version_row(row: sqlite3.Row) -> DocumentVersionRow:
        """Строка SQLite → :class:`DocumentVersionRow`.

        Частичность читается как **три состояния**: ``NULL`` в ``extract_partial`` —
        «не измерено» (строка записана до v5 или извлечение не наблюдалось), а не «полное».
        ``sqlite3`` отдаёт ``INTEGER``, поэтому признак приводится к ``bool | None`` явно:
        ``bool(0)`` и ``False`` в Python — одно и то же только пока об этом помнят.
        """
        partial_raw = row["extract_partial"]
        return DocumentVersionRow(
            doc_version_id=row["doc_version_id"],
            doc_id=row["doc_id"],
            product_id=row["product_id"],
            url=row["url"],
            content_hash=row["content_hash"],
            extractor_version=row["extractor_version"],
            level=row["level"],
            raw_path=row["raw_path"],
            chunker_version=row["chunker_version"],
            chunk_ids_digest=row["chunk_ids_digest"],
            extract_partial=(None if partial_raw is None else bool(partial_raw)),
            extract_partial_reason=row["extract_partial_reason"],
            extract_pages_converted=row["extract_pages_converted"],
            extract_page_count=row["extract_page_count"],
            first_seen_run_id=row["first_seen_run_id"],
            last_seen_run_id=row["last_seen_run_id"],
            first_seen_at=row["first_seen_at"],
            last_seen_at=row["last_seen_at"],
            status=row["status"],
        )

    def get_document_version(self, doc_version_id: str) -> DocumentVersionRow | None:
        row = self._conn.execute(
            "SELECT * FROM document_version WHERE doc_version_id = ?",
            (doc_version_id,),
        ).fetchone()
        return self._document_version_row(row) if row is not None else None

    def find_document_version(
        self,
        product_id: str,
        *,
        content_hash: str,
        extractor_version: str,
        level: str,
    ) -> DocumentVersionRow | None:
        row = self._conn.execute(
            "SELECT * FROM document_version WHERE product_id = ? AND content_hash = ? "
            "AND extractor_version = ? AND level = ?",
            (product_id, content_hash, extractor_version, level),
        ).fetchone()
        return self._document_version_row(row) if row is not None else None

    def upsert_document_version(self, row: DocumentVersionRow) -> None:
        """Upsert версии документа.

        ``doc_version_id`` — детерминированная функция ключа
        ``(product_id, content_hash, extractor_version, level)``, поэтому конфликт по PK
        и по UNIQUE-ограничению §4.2 указывает на одну и ту же логическую строку.
        Расхождение (вызывающий передал чужой ``doc_version_id`` для существующего
        ключа) — ошибка вызывающего и проявится как ``IntegrityError``, а не как
        молчаливое раздвоение версии.
        """
        values = [getattr(row, name) for name in _DOCUMENT_VERSION_COLUMNS]
        with self._conn:
            self._conn.execute(
                _upsert_sql(
                    "document_version", "doc_version_id", _DOCUMENT_VERSION_COLUMNS
                ),
                (row.doc_version_id, *values),
            )

    def iter_document_versions(
        self, product_id: str, *, status: str = "active"
    ) -> list[DocumentVersionRow]:
        rows = self._conn.execute(
            "SELECT * FROM document_version WHERE product_id = ? AND status = ? "
            "ORDER BY doc_version_id",
            (product_id, status),
        ).fetchall()
        return [self._document_version_row(row) for row in rows]

    def put_chunk_text(
        self, content_hash: str, text: str, lang: str | None = None
    ) -> None:
        """Текст иммутабелен по ``content_hash``: обновляется только ``lang``.

        ``content_hash`` — hash чистого текста (I19); перезапись текста под тем же
        хешем означала бы расхождение evidence с адресацией, поэтому запрещена.
        """
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO chunk_store (content_hash, text, lang) VALUES (?, ?, ?)
                ON CONFLICT(content_hash) DO UPDATE SET
                    lang = COALESCE(excluded.lang, chunk_store.lang)
                """,
                (content_hash, text, lang),
            )

    def get_chunk_text(self, content_hash: str) -> str | None:
        row = self._conn.execute(
            "SELECT text FROM chunk_store WHERE content_hash = ?", (content_hash,)
        ).fetchone()
        return str(row["text"]) if row is not None else None

    def replace_doc_chunks(
        self, doc_version_id: str, chunks: Sequence[ChunkRow]
    ) -> None:
        """Атомарная замена chunk-set документа (delete + insert в одной транзакции).

        Старые ``chunk_meta`` удаляются, новые вставляются; при ошибке вставки
        (например, FK на отсутствующий ``chunk_store``) транзакция откатывается и
        прежний набор остаётся. После вставки удаляются **осиротевшие** тексты из
        ``chunk_store``: только те ``content_hash``, которые были у этого документа и
        больше ни одним ``chunk_meta`` не referenced. Тексты, разделяемые другими
        документами, сохраняются; тексты, не относившиеся к этому документу, не
        трогаются вовсе.
        """
        old_hashes = {
            row["content_hash"]
            for row in self._conn.execute(
                "SELECT content_hash FROM chunk_meta WHERE doc_version_id = ?",
                (doc_version_id,),
            )
        }
        mismatched = [
            chunk.chunk_id
            for chunk in chunks
            if chunk.doc_version_id != doc_version_id
        ]
        if mismatched:
            # Иначе delete затронул бы один документ, а insert — другой.
            raise ValueError(
                "replace_doc_chunks: чанки принадлежат другому документу: "
                + ", ".join(sorted(mismatched))
            )
        new_hashes = {chunk.content_hash for chunk in chunks}
        with self._conn:
            self._conn.execute(
                "DELETE FROM chunk_meta WHERE doc_version_id = ?", (doc_version_id,)
            )
            self._conn.executemany(
                f"INSERT INTO chunk_meta ({', '.join(_CHUNK_COLUMNS)}) "  # noqa: S608 - имена из _CHUNK_COLUMNS, значения — bind
                f"VALUES ({', '.join('?' for _ in _CHUNK_COLUMNS)})",
                [
                    tuple(getattr(chunk, name) for name in _CHUNK_COLUMNS)
                    for chunk in chunks
                ],
            )
            for content_hash in sorted(old_hashes - new_hashes):
                self._conn.execute(
                    "DELETE FROM chunk_store WHERE content_hash = ? AND NOT EXISTS ("
                    "SELECT 1 FROM chunk_meta WHERE chunk_meta.content_hash = "
                    "chunk_store.content_hash)",
                    (content_hash,),
                )

    @staticmethod
    def _chunk_row(row: sqlite3.Row) -> ChunkRow:
        """Строка SQLite → :class:`ChunkRow`."""
        return ChunkRow(
            chunk_id=row["chunk_id"],
            doc_version_id=row["doc_version_id"],
            product_id=row["product_id"],
            chunk_no=row["chunk_no"],
            content_hash=row["content_hash"],
            chunker_version=row["chunker_version"],
            heading_path=row["heading_path"],
            context_prefix=row["context_prefix"],
            lang=row["lang"],
            char_start=row["char_start"],
            char_end=row["char_end"],
        )

    def iter_doc_chunks(self, doc_version_id: str) -> list[ChunkRow]:
        rows = self._conn.execute(
            "SELECT * FROM chunk_meta WHERE doc_version_id = ? "
            "ORDER BY chunk_no, chunk_id",
            (doc_version_id,),
        ).fetchall()
        return [self._chunk_row(row) for row in rows]

    # -- измерения ячеек (carry-forward, 09 §4.2/§10.1) -------------------- #
    def record_cell_measurements(self, rows: Sequence[CellMeasurementRow]) -> None:
        """Upsert измерений ячеек (ключ ``(run_id, product_id, param_id)``).

        Строки с пустым ``param_id`` отбрасываются: ключ измерения не выдумывается, а
        пустая строка сделала бы все такие измерения одним и тем же «параметром».
        Транзакция одна на пачку — индекс carry-forward пишется пачкой продукта.
        """
        payload = [
            (
                row.run_id,
                row.product_id,
                row.param_id,
                *(getattr(row, name) for name in _CELL_MEASUREMENT_COLUMNS),
            )
            for row in rows
            if str(row.param_id or "").strip()
        ]
        if not payload:
            return
        with self._conn:
            self._conn.executemany(
                _upsert_sql(
                    "cell_measurement",
                    ("run_id", "product_id", "param_id"),
                    _CELL_MEASUREMENT_COLUMNS,
                ),
                payload,
            )

    def get_cell_measurement(
        self, run_id: str, product_id: str, param_id: str
    ) -> CellMeasurementRow | None:
        """Измерение одной ячейки (09 §10.2): строка или ``None``.

        Читается по составному ключу (PK) — один SELECT: перенос проверяет ключ **на
        каждой** паре «продукт × параметр», поэтому чтение всего продукта здесь было бы
        лишней работой на каждой ячейке.
        """
        row = self._conn.execute(
            "SELECT * FROM cell_measurement"
            " WHERE run_id = ? AND product_id = ? AND param_id = ?",
            (run_id, product_id, param_id),
        ).fetchone()
        return None if row is None else self._cell_measurement_row(row)

    def save_cell_measurement(self, row: CellMeasurementRow) -> None:
        """Upsert одного измерения ячейки (ключ ``(run_id, product_id, param_id)``).

        Строка с пустым ``param_id`` отбрасывается — тот же контракт, что у батчевого
        ``record_cell_measurements``: ключ измерения не выдумывается, иначе все такие
        записи слились бы в один «параметр». Пишется и для ``observed``, и для
        ``carried``: индекс переноса обязан описывать **все** ячейки прогона, иначе
        цепочка переносов рвётся на втором цикле.
        """
        if not str(row.param_id or "").strip():
            return
        with self._conn:
            self._conn.execute(
                _upsert_sql(
                    "cell_measurement",
                    ("run_id", "product_id", "param_id"),
                    _CELL_MEASUREMENT_COLUMNS,
                ),
                (
                    row.run_id,
                    row.product_id,
                    row.param_id,
                    *(getattr(row, name) for name in _CELL_MEASUREMENT_COLUMNS),
                ),
            )

    def iter_cell_measurements(
        self, product_id: str, *, run_id: str | None = None
    ) -> list[CellMeasurementRow]:
        """Измерения продукта (все или одного прогона), порядок ``param_id``."""
        if run_id is None:
            rows = self._conn.execute(
                "SELECT * FROM cell_measurement WHERE product_id = ? "
                "ORDER BY run_id, param_id",
                (product_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM cell_measurement WHERE product_id = ? AND run_id = ? "
                "ORDER BY param_id",
                (product_id, run_id),
            ).fetchall()
        return [self._cell_measurement_row(row) for row in rows]

    @staticmethod
    def _cell_measurement_row(row: sqlite3.Row) -> CellMeasurementRow:
        """Строка SQLite → :class:`CellMeasurementRow`."""
        return CellMeasurementRow(
            run_id=row["run_id"],
            product_id=row["product_id"],
            param_id=row["param_id"],
            state=row["state"],
            measured_at=row["measured_at"],
            status=row["status"],
            prompt_hash=row["prompt_hash"],
            model_id=row["model_id"],
            llm_scenario=row["llm_scenario"],
            index_build_id=row["index_build_id"],
            partition_build_id=row["partition_build_id"],
            doc_set_digest=row["doc_set_digest"],
            coverage_digest=row["coverage_digest"],
            catalog_row_digest=row["catalog_row_digest"],
            cell_reuse_key=row["cell_reuse_key"],
            evidence_digest=row["evidence_digest"],
        )

    # -- индексные партиции/композиции (09 §8.1–§8.2) --------------------- #
    def register_index_partition(self, row: IndexPartitionRow) -> None:
        """Upsert продукт-партиции; метаданные иммутабельны, двигается только ``last_used``.

        ``ON CONFLICT`` обновляет ``last_used_run_id``, но **не** переписывает
        ``content_digest``/версии/``created_at``: партиция иммутабельна (K2/§13), а строка
        реестра — её отражение. Если на диске/в БД уже другой digest для того же
        ``partition_build_id`` — это расхождение id-формулы; хранилище не «исправляет»
        его молча, а сохраняет первое зафиксированное значение.
        """
        self._conn.execute(
            "INSERT INTO index_partition (package_id, product_id, partition_build_id,"
            " content_digest, chunker_version, embed_model_id, extractor_version,"
            " lexical_engine_version, vector_store, store_path, chunks, dim, created_at,"
            " last_used_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(package_id, product_id, partition_build_id) DO UPDATE SET"
            " last_used_run_id = excluded.last_used_run_id",
            (
                row.package_id,
                row.product_id,
                row.partition_build_id,
                row.content_digest,
                row.chunker_version,
                row.embed_model_id,
                row.extractor_version,
                row.lexical_engine_version,
                row.vector_store,
                row.store_path,
                row.chunks,
                row.dim,
                row.created_at,
                row.last_used_run_id,
            ),
        )
        self._conn.commit()

    def iter_index_partitions(self, package_id: str) -> list[IndexPartitionRow]:
        """Все известные партиции пакета (детерминированный порядок)."""
        rows = self._conn.execute(
            "SELECT * FROM index_partition WHERE package_id = ?"
            " ORDER BY product_id, partition_build_id",
            (package_id,),
        ).fetchall()
        return [
            IndexPartitionRow(
                package_id=row["package_id"],
                product_id=row["product_id"],
                partition_build_id=row["partition_build_id"],
                content_digest=row["content_digest"],
                chunker_version=row["chunker_version"],
                embed_model_id=row["embed_model_id"],
                extractor_version=row["extractor_version"],
                lexical_engine_version=row["lexical_engine_version"],
                vector_store=row["vector_store"],
                store_path=row["store_path"],
                chunks=row["chunks"],
                dim=row["dim"],
                created_at=row["created_at"],
                last_used_run_id=row["last_used_run_id"],
            )
            for row in rows
        ]

    def save_index_composition(self, row: IndexCompositionRow) -> None:
        """Upsert композиции (идемпотентно по ``composition_id``)."""
        members_json = json.dumps(
            [
                {
                    "product_id": product_id,
                    "partition_build_id": partition_build_id,
                    "content_digest": content_digest,
                }
                for product_id, partition_build_id, content_digest in row.members
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        # Композиция иммутабельна (K2/§13): повтор того же id — no-op, а не перезапись.
        # Другой состав при том же id отвергает ``write_composition`` до этой записи.
        self._conn.execute(
            "INSERT INTO index_composition (composition_id, package_id, members_json,"
            " created_at) VALUES (?, ?, ?, ?)"
            " ON CONFLICT(composition_id) DO NOTHING",
            (row.composition_id, row.package_id, members_json, row.created_at),
        )
        self._conn.commit()

    def get_index_composition(
        self, composition_id: str
    ) -> IndexCompositionRow | None:
        """Композиция по id (``None`` — неизвестна)."""
        row = self._conn.execute(
            "SELECT * FROM index_composition WHERE composition_id = ?",
            (composition_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["members_json"])
        except (TypeError, json.JSONDecodeError):
            payload = []
        members = tuple(
            (
                str(item.get("product_id", "")),
                str(item.get("partition_build_id", "")),
                str(item.get("content_digest", "")),
            )
            for item in payload
            if isinstance(item, dict)
        )
        return IndexCompositionRow(
            composition_id=row["composition_id"],
            package_id=row["package_id"],
            members=members,
            created_at=row["created_at"],
        )

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
            _PREVIOUS_LINEAGE_SQL,
            (package_id,),
        ).fetchone()
        return self._lineage_row(row) if row is not None else None

    # -- аттестация профиля пакета-якоря (G1/G2, state v4) ---------------- #
    def save_profile_attestation(self, row: ProfileAttestationRow) -> None:
        """Upsert аттестации профиля (идемпотентно по ``attestation_id``).

        ``superseded_at`` повторной подачей не затирается: отзыв аттестации необратим
        записью (только новой аттестацией с другим ``attestation_id``).
        """
        values: list[object] = []
        for name in _PROFILE_ATTESTATION_COLUMNS:
            value = getattr(row, name)
            if name == "digest_material":
                value = json.dumps(value, ensure_ascii=False, sort_keys=True)
            elif isinstance(value, Gate):
                value = value.value
            values.append(value)
        with self._conn:
            self._conn.execute(
                _PROFILE_ATTESTATION_UPSERT, (row.attestation_id, *values)
            )

    @staticmethod
    def _profile_attestation_row(row: sqlite3.Row) -> ProfileAttestationRow:
        """Строка SQLite → :class:`ProfileAttestationRow` (``digest_material`` — JSON)."""
        try:
            material = json.loads(row["digest_material"])
        except (TypeError, json.JSONDecodeError):
            material = {}
        return ProfileAttestationRow(
            attestation_id=row["attestation_id"],
            case_id=row["case_id"],
            package_id=row["package_id"],
            anchor=row["anchor"],
            gate=Gate(row["gate"]),
            profile_digest=row["profile_digest"],
            digest_material=material,
            attested_by=row["attested_by"],
            attested_at=row["attested_at"],
            superseded_at=row["superseded_at"],
            notes=row["notes"],
        )

    def find_profile_attestation(
        self,
        case_id: str,
        package_id: str,
        anchor: str,
        gate: Gate,
        profile_digest: str,
    ) -> ProfileAttestationRow | None:
        """Аттестация точного дайджеста (включая отозванные — решает вызывающий)."""
        row = self._conn.execute(
            "SELECT * FROM profile_attestation WHERE case_id = ? AND package_id = ?"
            " AND anchor = ? AND gate = ? AND profile_digest = ?"
            " ORDER BY attested_at DESC, attestation_id DESC LIMIT 1",
            (case_id, package_id, anchor, Gate(gate).value, profile_digest),
        ).fetchone()
        return self._profile_attestation_row(row) if row is not None else None

    def iter_profile_attestations(
        self,
        *,
        case_id: str,
        package_id: str,
        anchor: str,
        gate: Gate,
        include_superseded: bool = False,
    ) -> list[ProfileAttestationRow]:
        """Аттестации профиля для ``(case_id, package_id, anchor, gate)``."""
        sql = (
            "SELECT * FROM profile_attestation WHERE case_id = ? AND package_id = ?"
            " AND anchor = ? AND gate = ?"
        )
        params: list[object] = [case_id, package_id, anchor, Gate(gate).value]
        if not include_superseded:
            sql += " AND superseded_at IS NULL"
        sql += " ORDER BY attested_at, attestation_id"
        return [
            self._profile_attestation_row(row) for row in self._conn.execute(sql, params)
        ]

    def supersede_profile_attestation(
        self, attestation_id: str, *, superseded_at: str
    ) -> None:
        """Отзывает аттестацию (идемпотентно: проставляет ``superseded_at`` один раз)."""
        with self._conn:
            self._conn.execute(
                "UPDATE profile_attestation SET superseded_at = ?"
                " WHERE attestation_id = ? AND superseded_at IS NULL",
                (superseded_at, attestation_id),
            )

    # -- политика краула по областям (план 19 §4, STATE 8) ---------------- #
    def record_crawl_policy(self, row: CrawlPolicyRow) -> int:
        """Вставляет запись политики, возвращает выданный ``policy_id`` (append-only)."""
        with self._conn:
            cursor = self._conn.execute(
                "INSERT INTO crawl_policy (product_id, scope_kind, host, path, mode,"
                " interval_hours, note, actor, created_at, superseded_by)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row.product_id,
                    CrawlPolicyScopeKind(row.scope_kind).value,
                    row.host,
                    row.path,
                    CrawlPolicyMode(row.mode).value,
                    row.interval_hours,
                    row.note,
                    row.actor,
                    row.created_at,
                    row.superseded_by,
                ),
            )
            last_id = cursor.lastrowid
            if last_id is None:  # pragma: no cover — INSERT всегда даёт rowid
                raise RuntimeError("crawl_policy: INSERT не вернул policy_id")
            return int(last_id)

    def list_crawl_policy(
        self,
        *,
        product_id: str | None = None,
        include_superseded: bool = False,
    ) -> list[CrawlPolicyRow]:
        """Записи политики в порядке ``(product_id, scope_kind, host, path, policy_id)``."""
        sql = "SELECT * FROM crawl_policy"
        clauses: list[str] = []
        params: list[object] = []
        if product_id is not None:
            clauses.append("product_id = ?")
            params.append(product_id)
        if not include_superseded:
            clauses.append("superseded_by IS NULL")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY product_id, scope_kind, host, path, policy_id"
        return [_crawl_policy_row(row) for row in self._conn.execute(sql, params)]

    def record_corpus_capture(self, row: CorpusCaptureRow) -> int:
        """Пишет запись снятия корпуса и её документы; возвращает ``capture_id``.

        Append-only: прежние снятия не переписываются. Документы пишутся той же
        транзакцией, что и запись, — «снятие без групп дедупа» было бы записью, которую
        следующий ``frozen`` не может прочитать (он не обращается к сеансу краула).
        """
        with self._conn:
            cursor = self._conn.execute(
                "INSERT INTO corpus_capture (product_id, run_id, captured_at,"
                " capture_digest, seeds_digest, source, coverage_complete,"
                " coverage_digest, doc_set_digest)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row.product_id,
                    row.run_id,
                    row.captured_at,
                    row.capture_digest,
                    row.seeds_digest,
                    row.source,
                    1 if row.coverage_complete else 0,
                    row.coverage_digest,
                    row.doc_set_digest,
                ),
            )
            last_id = cursor.lastrowid
            if last_id is None:  # pragma: no cover — INSERT всегда даёт rowid
                raise RuntimeError("corpus_capture: INSERT не вернул capture_id")
            capture_id = int(last_id)
            self._conn.executemany(
                "INSERT INTO corpus_capture_doc (capture_id, product_id, doc_hash,"
                " url, normalized_url, reuse_kind) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        capture_id,
                        row.product_id,
                        doc.doc_hash,
                        doc.url,
                        doc.normalized_url,
                        doc.reuse_kind,
                    )
                    for doc in row.docs
                ],
            )
            return capture_id

    def last_corpus_capture(self, product_id: str) -> CorpusCaptureRow | None:
        """Последнее снятие продукта (``captured_at``, затем ``capture_id``) или ``None``."""
        head = self._conn.execute(
            "SELECT * FROM corpus_capture WHERE product_id = ?"
            " ORDER BY captured_at DESC, capture_id DESC LIMIT 1",
            (product_id,),
        ).fetchone()
        if head is None:
            return None
        docs = [
            CapturedDocRow(
                doc_hash=str(doc["doc_hash"]),
                url=str(doc["url"]),
                normalized_url=str(doc["normalized_url"]),
                reuse_kind=str(doc["reuse_kind"]),
            )
            for doc in self._conn.execute(
                "SELECT doc_hash, url, normalized_url, reuse_kind"
                " FROM corpus_capture_doc WHERE capture_id = ?"
                " ORDER BY doc_hash, url",
                (int(head["capture_id"]),),
            )
        ]
        return CorpusCaptureRow(
            product_id=str(head["product_id"]),
            run_id=str(head["run_id"]),
            captured_at=str(head["captured_at"]),
            capture_digest=str(head["capture_digest"]),
            seeds_digest=str(head["seeds_digest"]),
            source=str(head["source"]),
            coverage_complete=bool(head["coverage_complete"]),
            coverage_digest=head["coverage_digest"],
            doc_set_digest=str(head["doc_set_digest"]),
            docs=tuple(docs),
        )

    def supersede_crawl_policy(
        self, policy_id: int, *, superseded_by: int | None = None
    ) -> None:
        """Помечает запись заменённой; ``None`` — отзыв без преемника (идемпотентно).

        ``superseded_by = policy_id`` (самоссылка) — валидный FK и единственный способ
        отозвать запись, не выдумывая tombstone-режима: схема §4 не имеет значения
        «наследовать», а удаление нарушило бы append-only.
        """
        marker = policy_id if superseded_by is None else superseded_by
        with self._conn:
            self._conn.execute(
                "UPDATE crawl_policy SET superseded_by = ?"
                " WHERE policy_id = ? AND superseded_by IS NULL",
                (marker, policy_id),
            )
