"""Тесты ``hound package state forget-product`` (09 §4.1; офлайн, ``tmp_path``).

Сброс durable-состояния продукта раньше делался удалением ``state/{package_id}/state.db``
целиком: терялась история **всех** продуктов пакета. Команда удаляет строки одного
продукта, оставляя провенанс (``run_lineage``/``cycle``/``index_composition``), и делает
это только после консистентного снимка ``state.db``.

Проверяются обещания команды: ``--dry-run`` ничего не пишет, ``--actor`` обязателен,
снимок создан **до** удаления и назван, продукт без состояния — видимое «нечего забывать»
(не ошибка), общий текст ``chunk_store`` другого продукта не удаляется, откат при ошибке
на любой таблице.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.domain.enums import OnboardingState, SeedRole, SeedSource
from core.domain.models import SeedEntry
from core.ports.state import (
    CellMeasurementRow,
    ChunkRow,
    DocumentVersionRow,
    IndexPartitionRow,
    ProductStateRow,
    RunLineageRow,
    SnapshotRow,
    UrlStateRow,
)
from core.store.state_sqlite import state_db_path
from core.store.state_store import FORGET_PRODUCT_TABLES, SqliteStateStore
from tests.helpers.packages import valid_package

PACKAGE_ID = "case_x"
#: Текст, разделяемый p1 и p2: удалять его вместе с p1 нельзя (I19 — адресация по хешу).
SHARED_TEXT = "общий текст двух продуктов"
#: Текст, который знает только p1: уходит вместе с ним.
OWN_TEXT = "текст только первого продукта"


def _setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path]:
    """Пакет ``case_x`` и путь ``state/{package_id}/state.db`` при доме = ``tmp_path``.

    ``COHOUND_HOME`` объявляется явно: сессионная фикстура уводит операционные каталоги во
    временный дом сессии, и без объявления ``state`` уехал бы туда — тест утверждал бы путь,
    которого никто не пишет.
    """
    package = valid_package(tmp_path).rename(tmp_path / "case_x")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COHOUND_HOME", str(tmp_path))
    return package, state_db_path(tmp_path / "state", PACKAGE_ID)


def _seed(product_id: str) -> SeedEntry:
    host = f"{product_id}.test"
    return SeedEntry(
        url=f"https://{host}/",
        normalized_url=f"https://{host}/",
        role=SeedRole.PRIMARY,
        source=SeedSource.OPERATOR,
        profile_key=host,
        added_at="2026-01-01",
        added_by="operator",
    )


def _doc_version(product_id: str, *, doc_version_id: str, content_hash: str) -> DocumentVersionRow:
    return DocumentVersionRow(
        doc_version_id=doc_version_id,
        doc_id=f"{product_id}:{content_hash}",
        product_id=product_id,
        url=f"https://{product_id}.test/",
        content_hash=content_hash,
        extractor_version="extract-1",
        level="full",
        first_seen_run_id="run-1",
        last_seen_run_id="run-1",
        first_seen_at="2026-01-01T00:00:00Z",
        last_seen_at="2026-01-01T00:00:00Z",
        status="active",
    )


def _chunk(
    product_id: str, *, doc_version_id: str, no: int, content_hash: str
) -> ChunkRow:
    return ChunkRow(
        chunk_id=f"{doc_version_id}:{no}",
        doc_version_id=doc_version_id,
        product_id=product_id,
        chunk_no=no,
        content_hash=content_hash,
        chunker_version="chunker-1",
    )


def _partition(product_id: str) -> IndexPartitionRow:
    return IndexPartitionRow(
        package_id=PACKAGE_ID,
        product_id=product_id,
        partition_build_id=f"pb_{product_id}",
        content_digest=f"digest-{product_id}",
        chunker_version="chunker-1",
        embed_model_id="embed-1",
        extractor_version="extract-1",
        lexical_engine_version="bm25-1",
        vector_store="npy",
        store_path=f"index_store/{PACKAGE_ID}/pb_{product_id}/{product_id}/",
        chunks=2,
        created_at="2026-01-01T00:00:00Z",
    )


@pytest.fixture()
def populated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[Path, Path]]:
    """``state.db`` с двумя продуктами: общий чанк, свои строки, lineage пакета."""
    package, db_path = _setup(tmp_path, monkeypatch)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteStateStore(db_path)
    try:
        for product_id in ("p1", "p2"):
            store.upsert_product_state(
                ProductStateRow(
                    product_id=product_id,
                    canonical_name=product_id.upper(),
                    onboarding_state=OnboardingState.READY,
                    added_at="2026-01-01T00:00:00Z",
                    added_by="test",
                    last_corpus_run_id="run-1",
                    last_evaluated_run_id="run-1",
                )
            )
            store.reconcile_seed_set(product_id, [_seed(product_id)])
            store.upsert_url_state(
                UrlStateRow(
                    product_id=product_id,
                    url=f"https://{product_id}.test/",
                    normalized_url=f"https://{product_id}.test/",
                )
            )
            store.save_cell_measurement(
                CellMeasurementRow(
                    run_id="run-1",
                    product_id=product_id,
                    param_id="param-1",
                    state="evaluated",
                    measured_at="2026-01-01T00:00:00Z",
                )
            )
            store.register_index_partition(_partition(product_id))
            store.record_snapshot(
                SnapshotRow(
                    snapshot_id=f"snap-{product_id}",
                    product_id=product_id,
                    cycle_id="cycle-1",
                    run_id="run-1",
                    status="complete",
                    scope="full",
                    coverage_complete=True,
                    created_at="2026-01-01T00:00:00Z",
                )
            )

        # p1: два URL и документ с двумя чанками (общий текст + собственный).
        store.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url="https://p1.test/docs/",
                normalized_url="https://p1.test/docs/",
            )
        )
        shared_hash = "hash-shared"
        own_hash = "hash-own-p1"
        store.put_chunk_text(shared_hash, SHARED_TEXT)
        store.put_chunk_text(own_hash, OWN_TEXT)
        store.upsert_document_version(
            _doc_version("p1", doc_version_id="dv_p1", content_hash="content-p1")
        )
        store.replace_doc_chunks(
            "dv_p1",
            [
                _chunk("p1", doc_version_id="dv_p1", no=0, content_hash=shared_hash),
                _chunk("p1", doc_version_id="dv_p1", no=1, content_hash=own_hash),
            ],
        )
        # p2: документ с тем же общим текстом (не должен пострадать).
        store.upsert_document_version(
            _doc_version("p2", doc_version_id="dv_p2", content_hash="content-p2")
        )
        store.replace_doc_chunks(
            "dv_p2",
            [_chunk("p2", doc_version_id="dv_p2", no=0, content_hash=shared_hash)],
        )
        store.record_run_lineage(
            RunLineageRow(
                run_id="run-1",
                package_id=PACKAGE_ID,
                cycle_id="cycle-1",
                cohort_digest="cohort",
                seeds_digest="seeds",
                execution_contract_digest="contract",
                change_class="initial",
                status="complete",
                started_at="2026-01-01T00:00:00Z",
                completed_at="2026-01-01T01:00:00Z",
            )
        )
    finally:
        store.close()
    yield package, db_path


def _run(
    capsys: pytest.CaptureFixture[str], argv: Sequence[str]
) -> tuple[int, str, str]:
    code = main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _forget_argv(
    package: Path, *extra: str, actor: str | None = "tester", json_out: bool = True
) -> list[str]:
    argv = [
        "package",
        "state",
        "forget-product",
        "--package",
        str(package),
        "--product",
        "p1",
        *extra,
    ]
    if actor is not None:
        argv += ["--actor", actor]
    if json_out:
        argv.append("--json")
    return argv


def _count(db_path: Path, table: str, product_id: str | None = None) -> int:
    """Число строк таблицы (при ``product_id`` — только этого продукта)."""
    conn = sqlite3.connect(db_path)
    try:
        return _count_conn(conn, table, product_id)
    finally:
        conn.close()


def _count_conn(
    conn: sqlite3.Connection, table: str, product_id: str | None = None
) -> int:
    """То же по открытому соединению (проверка отката — до закрытия ``state.db``)."""
    sql = f"SELECT COUNT(*) AS n FROM {table}"  # noqa: S608 - имена из констант теста
    params: tuple[Any, ...] = ()
    if product_id is not None:
        sql += " WHERE product_id = ?"
        params = (product_id,)
    return int(conn.execute(sql, params).fetchone()[0])


def _counts(db_path: Path, product_id: str) -> dict[str, int]:
    """Строки продукта по таблицам забывания — в **той же** метрике, что у команды.

    ``chunk_store`` считает только осиротевшие тексты (текст, разделяемый другим
    продуктом, не удаляется, значит и не считается удалённым).
    """
    counts = {table: _count(db_path, table, product_id) for table in FORGET_PRODUCT_TABLES}
    conn = sqlite3.connect(db_path)
    try:
        counts["chunk_store"] = int(
            conn.execute(
                "SELECT COUNT(*) FROM chunk_store WHERE content_hash IN"
                " (SELECT content_hash FROM chunk_meta WHERE product_id = ?)"
                " AND content_hash NOT IN"
                " (SELECT content_hash FROM chunk_meta WHERE product_id <> ?)",
                (product_id, product_id),
            ).fetchone()[0]
        )
    finally:
        conn.close()
    return counts


def _backups(db_path: Path) -> list[Path]:
    """Снимки, лежащие рядом со ``state.db``."""
    return sorted(db_path.parent.glob(f"{db_path.name}.bak-*"))


# --------------------------------------------------------------------------- #
# --dry-run: считает и не пишет
# --------------------------------------------------------------------------- #


def test_dry_run_counts_rows_and_writes_nothing(
    populated: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """``--dry-run`` печатает число строк по таблицам, но не пишет и не снимает бэкап."""
    package, db_path = populated
    before = _counts(db_path, "p1")

    code, out, _ = _run(capsys, _forget_argv(package, "--dry-run"))

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["found"] is True
    assert payload["counts"] == before
    assert payload["counts"]["url_state"] == 2
    # Текст, разделяемый с p2, не осиротел — в счёт удаляемого не входит.
    assert payload["counts"]["chunk_store"] == 1
    assert payload["total"] == sum(before.values())
    assert payload["backup"] is None
    # Ни снимка, ни удаления: состояние побитово то же по строкам.
    assert _backups(db_path) == []
    assert _counts(db_path, "p1") == before


# --------------------------------------------------------------------------- #
# Забывание: снимок, удаление, что осталось
# --------------------------------------------------------------------------- #


def test_forget_removes_product_rows_after_backup(
    populated: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Снимок создан **до** удаления и назван; удалены строки только этого продукта."""
    package, db_path = populated
    before = _counts(db_path, "p1")
    p2_before = _counts(db_path, "p2")

    code, out, _ = _run(capsys, _forget_argv(package))

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["found"] is True
    assert payload["total"] == sum(before.values())
    backup = Path(payload["backup"])
    assert backup.is_file()
    assert backup.parent == db_path.parent  # «рядом» — не в другом каталоге
    # Снимок сделан до удаления: строки продукта в нём ещё есть.
    assert _count(backup, "product_state", "p1") == 1
    assert _count(backup, "chunk_meta", "p1") == 2

    # Состояние продукта забыто.
    for table in FORGET_PRODUCT_TABLES:
        assert _count(db_path, table, "p1") == 0, table
    # Чужой продукт не тронут: его строки те же (метрика «что было бы удалено» у p2
    # меняется законно — общий текст после ухода p1 остаётся только за ним).
    for table in FORGET_PRODUCT_TABLES:
        assert _count(db_path, table, "p2") == p2_before[table], table
    store = SqliteStateStore(db_path)
    try:
        assert store.get_chunk_text("hash-shared") == SHARED_TEXT
        assert store.get_chunk_text("hash-own-p1") is None
    finally:
        store.close()
    # История прогонов — провенанс, а не состояние продукта.
    assert _count(db_path, "run_lineage") == 1
    assert payload["kept"] == ["run_lineage", "cycle", "index_composition"]


def test_forget_is_idempotent_and_says_nothing_to_forget(
    populated: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Повтор — видимое «нечего забывать»: не ошибка, но и не пустой успех."""
    package, db_path = populated
    assert _run(capsys, _forget_argv(package))[0] == EXIT_OK
    (backup,) = _backups(db_path)

    code, out, _ = _run(capsys, _forget_argv(package, json_out=False))

    assert code == EXIT_OK
    assert "нечего забывать" in out
    assert "summary: deleted=0" in out
    assert _backups(db_path) == [backup]  # второй снимок не создаётся


def test_unknown_product_is_not_an_error(
    populated: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Продукта нет в состоянии — ``found=False``, снимок не нужен (удалять нечего)."""
    package, db_path = populated
    before = _counts(db_path, "p1")

    argv = _forget_argv(package)
    argv[argv.index("p1")] = "ghost"
    code, out, _ = _run(capsys, argv)

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["found"] is False
    assert payload["total"] == 0
    assert payload["backup"] is None
    assert _backups(db_path) == []
    assert _counts(db_path, "p1") == before


def test_without_state_db_nothing_is_created(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Нет ``state.db`` — «нечего забывать» и файл не создаётся ради ответа."""
    package, db_path = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(capsys, _forget_argv(package))

    assert code == EXIT_OK
    assert json.loads(out)["found"] is False
    assert not db_path.exists()


# --------------------------------------------------------------------------- #
# Человеческий шлюз и транзакционность
# --------------------------------------------------------------------------- #


def test_actor_is_required(
    populated: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Без ``--actor`` — ``EXIT_USAGE`` и ни одной удалённой строки."""
    package, db_path = populated
    before = _counts(db_path, "p1")

    code, _, err = _run(capsys, _forget_argv(package, actor=None, json_out=False))

    assert code == EXIT_USAGE
    assert "--actor" in err
    assert _counts(db_path, "p1") == before
    assert _backups(db_path) == []


def test_failure_midway_rolls_back(
    populated: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ошибка на любой таблице откатывает всё удаление (одна транзакция).

    Сбой имитируется несуществующей таблицей в списке: удаление уже прошло по первым
    таблицам, и именно откат должен вернуть строки. Снимок при этом остаётся — он сделан
    **до** удаления, и его наличие как раз и есть условие операции.
    """
    from core.store import state_store as state_store_module

    _package, db_path = populated
    before = _counts(db_path, "p1")
    # Сбой — в самом конце транзакции: удаление уже прошло по таблицам, и откат обязан
    # вернуть их. Список таблиц не подменяем: его же читает подсчёт строк до удаления.
    monkeypatch.setattr(
        state_store_module, "_DELETE_ORPHAN_CHUNK_SQL", "DELETE FROM no_such_table"
    )
    store = SqliteStateStore(db_path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            store.forget_product("p1")
        # Откат: строки продукта на месте, включая удалённые «до» сбоя chunk_meta.
        assert _count_conn(store.conn, "chunk_meta", "p1") == 2
    finally:
        store.close()
    assert _counts(db_path, "p1") == before
    assert len(_backups(db_path)) == 1
