"""Гейт версии ``state.db`` и полнота её объявленной схемы (09 §4.1, решение владельца 01.11.2026).

Прежде ``connect_state`` **никогда** не сверял версию: он применял ``STATE_DDL``, досоздавал
аддитивные колонки (``ALTER TABLE ... ADD COLUMN``), создавал индексы после миграции и в конце
писал ``schema_meta.schema_version`` — то есть старая БД молча «догонялась» и объявляла себя
новой ровно тем действием, которым её открывали. Здесь проверяется обратное:

* файл прежней версии отвергается :class:`~core.store.sqlite.SchemaVersionMismatch`, и
  **файл не меняется** (отказ — не «полумиграция»);
* файл с таблицами, но без объявленной версии отвергается так же: это не «новая БД», и
  приписать его строки текущей схеме нечем;
* пустая БД (``:memory:``/новый путь) открывается и получает текущую версию;
* свежая БД содержит **все** колонки, которые прежде досоздавались, и все индексы, которые
  прежде создавались после миграции. Списки ниже — данные теста: удаление колонки из
  ``STATE_DDL`` роняет проверку, а не проходит незамеченным.

Файл-предшественник ``test_state_sqlite_v2_migration.py`` проверял шим v2 → v3 и прожил ровно
столько, сколько прожил шим; его проверки перенесены сюда на новый носитель (``STATE_DDL``) по
правилу AGENTS §4.10 — ни одно утверждение не потеряно, кроме самого факта «ALTER при открытии».
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.cli.main import EXIT_USAGE, main
from core.store.sqlite import SchemaVersionMismatch
from core.store.state_sqlite import STATE_SCHEMA_VERSION, connect_state

#: Колонки, которые до v6 досоздавались при открытии (``_ADDITIVE_COLUMNS``). Теперь они
#: обязаны быть в ``STATE_DDL``; список остаётся здесь данными, чтобы забытая колонка ловилась.
FORMER_ADDITIVE_COLUMNS: dict[str, tuple[str, ...]] = {
    "run_lineage": ("products_json", "refresh_policy"),
    "url_state": (
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
    ),
    "document_version": (
        "doc_id",
        "product_id",
        "url",
        "content_hash",
        "extractor_version",
        "level",
        "raw_path",
        "chunker_version",
        "chunk_ids_digest",
        # v5: nullable by design — отсутствие значения означает «не измерено».
        "extract_partial",
        "extract_partial_reason",
        "extract_pages_converted",
        "extract_page_count",
        "first_seen_run_id",
        "last_seen_run_id",
        "first_seen_at",
        "last_seen_at",
        "status",
    ),
    "chunk_store": ("text", "lang"),
    "chunk_meta": (
        "doc_version_id",
        "product_id",
        "chunk_no",
        "content_hash",
        "chunker_version",
        "heading_path",
        "context_prefix",
        "lang",
    ),
}

#: Индексы, которые до v6 создавались отдельным скриптом **после** миграции колонок.
FORMER_POST_MIGRATION_INDEXES = ("idx_url_state_product", "idx_chunk_meta_doc")


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _legacy_state_db(path: Path, version: str | None) -> Path:
    """Файл ``state.db`` «прошлой схемы»: таблицы есть, версия — как указано (или нет)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE url_state (product_id TEXT NOT NULL, url TEXT NOT NULL,"
            " normalized_url TEXT NOT NULL, PRIMARY KEY (product_id, normalized_url));"
        )
        if version is not None:
            conn.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)",
                (version,),
            )
        conn.commit()
    finally:
        conn.close()
    return path


def test_fresh_db_declares_every_former_additive_column() -> None:
    """Колонки, прежде досоздаваемые ``ALTER TABLE``, объявлены в ``STATE_DDL``."""
    conn = connect_state(":memory:")
    try:
        for table, columns in FORMER_ADDITIVE_COLUMNS.items():
            missing = sorted(set(columns) - _columns(conn, table))
            assert not missing, f"{table}: колонки не объявлены в STATE_DDL: {missing}"
    finally:
        conn.close()


def test_fresh_db_declares_every_former_migration_index() -> None:
    """Индексы, прежде создаваемые после миграции, объявлены в ``STATE_DDL``."""
    conn = connect_state(":memory:")
    try:
        indexes = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
        }
    finally:
        conn.close()
    assert set(FORMER_POST_MIGRATION_INDEXES) <= indexes


@pytest.mark.parametrize("version", ["5", "4", "2", "0.9", "7", "99"])
def test_previous_version_file_is_rejected_and_left_untouched(
    tmp_path: Path, version: str
) -> None:
    """Иная версия — отказ с путём файла; содержимое файла не меняется ни на байт."""
    db_path = _legacy_state_db(tmp_path / "state.db", version)
    before = db_path.read_bytes()

    with pytest.raises(SchemaVersionMismatch) as excinfo:
        connect_state(db_path)

    message = str(excinfo.value)
    assert "state.db" in message  # оператор знает, какой файл пересоздавать
    assert version in message  # что объявлено
    assert STATE_SCHEMA_VERSION in message  # что ожидает код
    assert "schema_version" in message  # что именно сверялось
    assert excinfo.value.code == "schema_version"
    assert not message.startswith("schema_version:")
    assert "пересоздайте state.db" in message
    assert db_path.read_bytes() == before


def test_file_with_tables_but_without_declared_version_is_rejected(
    tmp_path: Path,
) -> None:
    """Таблицы есть, ``schema_version`` нет — это не «новая БД», а файл неизвестной схемы."""
    db_path = _legacy_state_db(tmp_path / "state.db", None)
    before = db_path.read_bytes()

    with pytest.raises(SchemaVersionMismatch) as excinfo:
        connect_state(db_path)

    message = str(excinfo.value)
    assert "не объявляет schema_version" in message
    assert STATE_SCHEMA_VERSION in message
    assert db_path.read_bytes() == before


def test_schema_meta_without_version_row_is_rejected(tmp_path: Path) -> None:
    """Таблица ``schema_meta`` создана, строки версии нет — та же причина отказа."""
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(SchemaVersionMismatch):
        connect_state(db_path)


def test_empty_db_opens_and_declares_current_version(tmp_path: Path) -> None:
    """Новый файл и ``:memory:``: сверять нечего, схема создаётся с текущей версией."""
    for target in (":memory:", tmp_path / "fresh.db"):
        conn = connect_state(target)
        try:
            row = conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            assert row is not None and row["value"] == STATE_SCHEMA_VERSION
        finally:
            conn.close()


def test_reopen_is_idempotent(tmp_path: Path) -> None:
    """Повторное открытие текущей БД не падает и не переписывает данные."""
    db_path = tmp_path / "state.db"
    first = connect_state(db_path)
    first.execute(
        "INSERT INTO product_state (product_id, canonical_name, onboarding_state,"
        " added_at, added_by) VALUES ('p1', 'Product One', 'ready', '2026-01-01', 'test')"
    )
    first.commit()
    first.close()

    second = connect_state(db_path)
    try:
        row = second.execute(
            "SELECT canonical_name FROM product_state WHERE product_id = 'p1'"
        ).fetchone()
        assert row is not None and row["canonical_name"] == "Product One"
        assert (
            second.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()["value"]
            == STATE_SCHEMA_VERSION
        )
    finally:
        second.close()


def test_cli_maps_state_refusal_to_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Отказ ``state.db`` доходит до общего обработчика: ``EXIT_USAGE`` + ``CODE: message``.

    Проверяется именно **путь CLI**, а не сообщение класса: ``SchemaVersionMismatch`` из
    ``state.db`` обязан печататься как ``schema_version: …`` и не вылетать трейсбеком.
    Пакет подменён минимальным каталогом — предмет проверки здесь БД, а не пакет.
    """
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "package.yaml").write_text("case_id: pkg\n", encoding="utf-8")
    db_path = _legacy_state_db(tmp_path / "state" / "pkg" / "state.db", "5")

    import core.cli.context as context_mod

    monkeypatch.setattr(
        context_mod,
        "_open_state_store",
        lambda args, create=False: connect_state(db_path),
    )

    code = main(
        [
            "package",
            "state",
            "forget-product",
            "--package",
            str(package),
            "--product",
            "p1",
            "--actor",
            "tester",
        ]
    )

    captured = capsys.readouterr()
    assert code == EXIT_USAGE
    assert "schema_version:" in captured.err
    assert "state.db" in captured.err
    assert "Traceback" not in captured.err


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))