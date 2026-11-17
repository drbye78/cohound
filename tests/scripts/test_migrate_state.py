"""Тесты явного переноса ``state.db`` 12 → 13 (``scripts/migrate_state.py``).

Штатное открытие (:func:`core.store.state_sqlite.connect_state`) файл другой версии
**отвергает** — миграций при открытии нет (решение владельца 01.11.2026). Перенос делает
отдельный инструмент, и здесь проверяется то, ради чего он существует:

* **результат, а не действие**: схема после переноса структурно равна чистому v13 —
  сравнением тем же :func:`scripts.migrate_state.compare_signatures`, которым инструмент
  печатает дельту (второго сравнения в тесте нет намеренно: два похожих сравнения
  разошлись бы на первой правке и пропустили бы ровно то, что должны ловить);
* **данные переживают перенос** — проверяется содержимое строк, а не только их число:
  «строк столько же» верно и тогда, когда строки другие;
* **отказы**: чужая версия, неизвестный объект, несогласованный файл (версия 12, а
  колонка уже есть), занятый путь бэкапа — код ``2`` **до** записи;
* **провал проверки не подменяет цель** — код ``1``, файл прежний (шов
  :func:`scripts.migrate_state.verify_migrated_copy` подменён);
* **``--dry-run`` ничего не пишет** — ни копии, ни бэкапа, ни байта в самом файле.

Фикстура v12 собирается из ``STATE_DDL`` **без** объявления ``chunk_meta.table_ref``: это
и есть определение известного состояния v12 (v13 минус ровно одна колонка). Способ
``ALTER TABLE chunk_meta DROP COLUMN table_ref`` на чистом v13 **не работает** в этом
``.venv`` (SQLite 3.50.4): переписывая объявление таблицы, он оставляет запятую перед
закрывающей скобкой после блока комментариев, и файл отвечает
``error in table chunk_meta after drop column: incomplete input``. Поэтому фикстура
строится из текста DDL, а не из готового файла; отдельный тест ниже сторожит, что
удалённая колонка действительно единственная.

Сеть не используется, ``state.db`` стенда не трогается: всё в ``tmp_path``.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

from core.store.state_sqlite import STATE_DDL, connect_state

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль инструмента по пути: ``scripts/`` не пакет."""
    path = _ROOT / "scripts" / "migrate_state.py"
    spec = importlib.util.spec_from_file_location("migrate_state", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["migrate_state"] = module
    spec.loader.exec_module(module)
    return module


migrate_state = _load_module()

#: Колонка, которой нет в v12 и которая появляется в v13.
ADDED_COLUMN = "table_ref"


# ------------------------------------------------------------------------ фикстуры


def _v12_ddl() -> str:
    """``STATE_DDL`` без объявления ``chunk_meta.table_ref`` — схема v12.

    Удаляется и блок комментариев перед колонкой (он документирует именно её), а у
    предыдущей колонки снимается запятая: ``char_end INTEGER,`` перед ``)`` — синтаксис,
    которого SQLite не принимает.
    """
    lines = STATE_DDL.splitlines()
    index = next(
        i for i, line in enumerate(lines) if line.strip().startswith(f"{ADDED_COLUMN} ")
    )
    start = index
    while start > 0 and lines[start - 1].strip().startswith("--"):
        start -= 1
    del lines[start : index + 1]
    assert lines[start - 1].rstrip().endswith(","), lines[start - 1]
    lines[start - 1] = lines[start - 1].rstrip()[:-1]
    return "\n".join(lines)


def _build(path: Path, ddl: str, version: str) -> Path:
    """БД с объявленной версией — мимо ``connect_state`` (он чужую версию отвергает)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(ddl)
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta (key, value) VALUES"
            " ('schema_version', ?)",
            (version,),
        )
        conn.commit()
    finally:
        conn.close()
    return path


def _v12_fixture(path: Path, version: str = "12") -> Path:
    return _build(path, _v12_ddl(), version)


def _v13_fixture(path: Path) -> Path:
    conn = connect_state(path)
    conn.close()
    return path


def _seed(path: Path) -> dict[str, tuple]:
    """Строки во всех таблицах, которые переживают перенос; возвращает их снимок."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "INSERT INTO chunk_store (content_hash, text, lang)"
            " VALUES ('h1', 'привет мир', 'ru')"
        )
        conn.execute(
            "INSERT INTO url_state (product_id, url, normalized_url, last_http_status,"
            " observed_at, observed_how)"
            " VALUES ('p1', 'https://example.test/a', 'https://example.test/a', 200,"
            " '2026-11-01T00:00:00', 'live')"
        )
        conn.execute(
            "INSERT INTO capture (capture_id, product_id, root_url, started_at,"
            " complete, observed_how) VALUES"
            " ('cap1', 'p1', 'https://example.test/', '2026-11-01T00:00:00', 1, 'live')"
        )
        conn.execute(
            "INSERT INTO document_version (doc_version_id, doc_id, product_id, url,"
            " content_hash, extractor_version, level, first_seen_run_id,"
            " last_seen_run_id, first_seen_at, last_seen_at, status)"
            " VALUES ('dv1', 'd1', 'p1', 'https://example.test/a', 'h1', 'html-7',"
            " 'text', 'run1', 'run1', '2026-11-01', '2026-11-01', 'active')"
        )
        conn.execute(
            "INSERT INTO chunk_meta (chunk_id, doc_version_id, product_id, chunk_no,"
            " content_hash, chunker_version, heading_path, context_prefix, lang,"
            " char_start, char_end) VALUES"
            " ('c1', 'dv1', 'p1', 0, 'h1', 'policy-6', 'Раздел', 'Раздел\n', 'ru',"
            " 0, 10)"
        )
        conn.commit()
    finally:
        conn.close()
    return _snapshot(path)


def _snapshot(path: Path) -> dict[str, list[dict]]:
    """Полное содержимое затронутых таблиц — сравнение по значению, не по числу строк.

    Колонка ``table_ref`` из снимка убирается: её добавляет перенос, и она обязана быть
    ``NULL`` (это проверяет отдельный тест) — иначе сравнение «до/после» сравнивало бы
    строки разной ширины и падало бы на самой правильной миграции.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        snapshot: dict[str, list[dict]] = {}
        for table in ("chunk_store", "chunk_meta", "url_state", "capture"):
            rows = [
                dict(row)
                for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")
            ]
            for row in rows:
                row.pop(ADDED_COLUMN, None)
            snapshot[table] = rows
        return snapshot
    finally:
        conn.close()


def _hash(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


# --------------------------------------------------------------- фикстура против кода


def test_fixture_removes_exactly_the_added_column() -> None:
    """Сторож фикстуры: v12 отличается от v13 **одной** колонкой, иначе тесты врут."""
    reference = _v12_ddl()
    assert f"{ADDED_COLUMN} " not in reference
    assert "chunk_meta" in reference
    # Вырезано ровно объявление колонки, остальной DDL не тронут.
    removed = len(STATE_DDL.splitlines()) - len(reference.splitlines())
    assert removed == 4  # три строки комментария + объявление


# -------------------------------------------------------------------- успешный перенос


def test_migration_makes_schema_equal_to_clean_v13(tmp_path: Path) -> None:
    """Схема после переноса структурно равна чистому v13 (тем же сравнением, что в приборе)."""
    db = _v12_fixture(tmp_path / "state.db")

    assert migrate_state.main(["--db", str(db), "--apply", "--no-backup"]) == 0

    reference = _v13_fixture(tmp_path / "reference.db")
    actual_conn = migrate_state._connect_ro(db)
    reference_conn = migrate_state._connect_ro(reference)
    try:
        assert (
            migrate_state.compare_signatures(
                migrate_state.schema_signature(actual_conn),
                migrate_state.schema_signature(reference_conn),
            )
            == []
        )
    finally:
        actual_conn.close()
        reference_conn.close()


def test_migration_preserves_rows_and_leaves_table_ref_null(tmp_path: Path) -> None:
    """Число строк сохранено, ``table_ref IS NULL`` у всех кусков, файл цел."""
    db = _v12_fixture(tmp_path / "state.db")
    _seed(db)
    before = migrate_state.table_row_counts(migrate_state._connect_ro(db))

    assert migrate_state.main(["--db", str(db), "--apply", "--no-backup"]) == 0

    conn = migrate_state._connect_ro(db)
    try:
        after = migrate_state.table_row_counts(conn)
        assert after == before
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert (
            conn.execute(
                "SELECT count(*) FROM chunk_meta WHERE table_ref IS NOT NULL"
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()


def test_row_contents_survive_the_migration(tmp_path: Path) -> None:
    """Данные переживают перенос по значению: «строк столько же» верно и для других строк."""
    db = _v12_fixture(tmp_path / "state.db")
    before = _seed(db)

    assert migrate_state.main(["--db", str(db), "--apply", "--no-backup"]) == 0

    assert _snapshot(db) == before


def test_backup_keeps_the_source_version(tmp_path: Path) -> None:
    """Бэкап по умолчанию: рядом лежит копия исходной версии, и она читается как v12."""
    db = _v12_fixture(tmp_path / "state.db")
    _seed(db)

    assert migrate_state.main(["--db", str(db), "--apply"]) == 0

    backup = tmp_path / "state.db.v12"
    assert backup.is_file()
    conn = sqlite3.connect(str(backup))
    try:
        assert (
            conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == "12"
        )
        assert ADDED_COLUMN not in {
            row[1] for row in conn.execute("PRAGMA table_info(chunk_meta)")
        }
    finally:
        conn.close()


def test_migrated_file_opens_through_the_standard_gate(tmp_path: Path) -> None:
    """Штатное открытие мигрированного файла не бросает ``SchemaVersionMismatch``."""
    db = _v12_fixture(tmp_path / "state.db")
    assert migrate_state.main(["--db", str(db), "--apply", "--no-backup"]) == 0

    conn = connect_state(db)  # не должно бросить
    try:
        assert (
            conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == migrate_state.TARGET_VERSION
        )
    finally:
        conn.close()


# ------------------------------------------------------------------------- dry-run


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    """``--dry-run`` (режим по умолчанию) не создаёт ни копии, ни бэкапа и не меняет файл.

    Пин стоит на файлах переноса: ``state.db.v12`` и рабочей копии. Read-only открытие
    WAL-БД само по себе создаёт ``-shm``/пустой ``-wal`` (восстановление wal-index, как у
    любого read-only читателя репозитория) — это не запись в ``state.db``, и хеш файла
    ниже это подтверждает.
    """
    db = _v12_fixture(tmp_path / "state.db")
    before = _hash(db)

    assert migrate_state.main(["--db", str(db)]) == 0

    assert _hash(db) == before
    assert not (tmp_path / "state.db.v12").exists()
    assert not list(tmp_path.glob(".state.db.migrating-*"))


# ---------------------------------------------------------------------------- отказы


def test_other_version_is_refused_with_the_supported_pair(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Версия 11 — отказ с кодом 2; текст называет поддерживаемую пару ``12 → 13``."""
    db = _v12_fixture(tmp_path / "state.db", version="11")
    before = _hash(db)

    assert migrate_state.main(["--db", str(db), "--apply"]) == 2

    captured = capsys.readouterr()
    assert "12 → 13" in captured.out
    assert "'11'" in captured.out
    assert _hash(db) == before


def test_unknown_object_is_refused_by_name(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Объект, которого нет в чистом v13, — отказ; имя объекта в сообщении."""
    db = _v12_fixture(tmp_path / "state.db")
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("CREATE TABLE foreign_shim (x TEXT)")
        conn.commit()
    finally:
        conn.close()
    before = _hash(db)

    assert migrate_state.main(["--db", str(db), "--apply"]) == 2

    captured = capsys.readouterr()
    assert "foreign_shim" in captured.out
    assert _hash(db) == before


def test_version_12_with_the_column_already_present_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Версия 12, а ``table_ref`` уже есть — несогласованный файл, а не «догоним молча»."""
    db = _v13_fixture(tmp_path / "state.db")
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("UPDATE schema_meta SET value = '12' WHERE key = 'schema_version'")
        conn.commit()
    finally:
        conn.close()
    before = _hash(db)

    assert migrate_state.main(["--db", str(db), "--apply"]) == 2

    captured = capsys.readouterr()
    assert "table_ref" in captured.out
    assert "уже есть" in captured.out
    assert _hash(db) == before


def test_file_without_tables_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Пустой файл — не ``state.db``: отказ, а не «создам схему»."""
    db = tmp_path / "state.db"
    sqlite3.connect(str(db)).close()

    assert migrate_state.main(["--db", str(db), "--apply"]) == 2
    assert "нет таблиц" in capsys.readouterr().out


def test_file_without_declared_version_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Таблицы есть, версии нет — сверять не с чем."""
    db = tmp_path / "state.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.commit()
    finally:
        conn.close()

    assert migrate_state.main(["--db", str(db), "--apply"]) == 2
    assert "не объявляет schema_version" in capsys.readouterr().out


def test_file_that_is_not_sqlite_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Не-SQLite файл (опечатка в пути) — отказ с кодом 2, а не трейсбек."""
    db = tmp_path / "state.db"
    db.write_bytes(b"not a database at all" * 100)

    assert migrate_state.main(["--db", str(db), "--apply"]) == 2
    assert "не читается как SQLite" in capsys.readouterr().out


def test_non_empty_wal_blocks_the_swap(tmp_path: Path) -> None:
    """Непустой ``-wal`` рядом с файлом — отказ до подмены (пустой не мешает).

    Проверяется сам гейт: содержимое бэкапа снимается через backup API и включает
    закоммиченные кадры, но подменять основной файл под живым писателем нельзя.
    """
    db = tmp_path / "state.db"
    db.write_bytes(b"")
    assert migrate_state._wal_refusal(db) is None

    wal = Path(str(db) + "-wal")
    wal.write_bytes(b"\x00" * 4096)
    reason = migrate_state._wal_refusal(db)
    assert reason is not None and "непустой" in reason

    wal.write_bytes(b"")
    assert migrate_state._wal_refusal(db) is None


# -------------------------------------------------------------- бэкап и провал проверки


def test_occupied_backup_path_needs_force(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Занятый путь бэкапа — отказ без ``--force`` и работа с ним."""
    db = _v12_fixture(tmp_path / "state.db")
    backup = tmp_path / "state.db.v12"
    backup.write_bytes(b"not a database")
    before = _hash(db)

    assert migrate_state.main(["--db", str(db), "--apply"]) == 2
    assert "занят" in capsys.readouterr().out
    assert _hash(db) == before
    assert backup.read_bytes() == b"not a database"

    assert migrate_state.main(["--db", str(db), "--apply", "--force"]) == 0
    assert _hash(db) != before
    conn = sqlite3.connect(str(backup))
    try:
        assert (
            conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            == "12"
        )
    finally:
        conn.close()


def test_failed_verification_does_not_replace_the_target(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Провал проверки — код 1, целевой файл прежний, рабочая копия убрана.

    Подменяется именно шов проверки: инструмент обязан **спросить** результат, а не
    объявить его сам.
    """
    db = _v12_fixture(tmp_path / "state.db")
    _seed(db)
    before = _hash(db)

    monkeypatch.setattr(
        migrate_state,
        "verify_migrated_copy",
        lambda copy_path, reference_path, rows_before: ["подменённая проверка: расхождение"],
    )

    assert migrate_state.main(["--db", str(db), "--apply", "--no-backup"]) == 1

    captured = capsys.readouterr()
    assert "ПРОВЕРКА НЕ ПРОЙДЕНА" in captured.out
    assert "подменённая проверка" in captured.out
    assert _hash(db) == before
    assert not list(tmp_path.glob(".state.db.migrating-*"))


def test_already_current_file_is_left_byte_identical(tmp_path: Path) -> None:
    """Версия 13 — код 0 и ни одного изменённого байта."""
    db = _v13_fixture(tmp_path / "state.db")
    before = _hash(db)

    assert migrate_state.main(["--db", str(db), "--apply"]) == 0

    assert _hash(db) == before
    assert not (tmp_path / "state.db.v12").exists()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
