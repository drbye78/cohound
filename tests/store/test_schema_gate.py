"""Гейт версии схемы run DB (`11` §2.6): старая БД отвергается сообщением, а не читается молча.

Прогон — durable-артефакт, но **не** переносится между версиями схемы: аддитивные дельты
сделали бы «старую» строку неотличимой от «новой», и прогон объявлял бы условия измерения,
которых не применял. Проверяется: отказ с обоими номерами версий, отсутствие трейсбека в CLI
(сообщение + ``EXIT_USAGE``) и то, что свежая БД открывается повторно (идемпотентность).
Нейтральные фикстуры (§1.3), сеть не используется.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from core.cli.main import EXIT_USAGE, main
from core.store.sqlite import SCHEMA_VERSION, SchemaVersionMismatch, connect


def _legacy_db(path: Path, version: str) -> Path:
    """Файл БД «прошлой версии»: только ``schema_meta`` с чужим номером схемы."""
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        )
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)",
            (version,),
        )
        conn.commit()
    finally:
        conn.close()
    return path


def test_older_schema_is_rejected_with_message(tmp_path: Path) -> None:
    """Старая БД не мигрируется молча: отказ называет обе версии и действие оператора."""
    db = _legacy_db(tmp_path / "cohound.db", "3.11.0")

    with pytest.raises(SchemaVersionMismatch) as excinfo:
        connect(db)

    message = str(excinfo.value)
    assert "3.11.0" in message  # найденная версия
    assert SCHEMA_VERSION in message  # ожидаемая версия
    assert "schema_version" in message  # что именно сверялось
    assert "cohound.db" in message  # что именно отвергнуто
    # Код правила не дублируется в тексте (иначе CLI печатал бы «code: code: …»).
    assert excinfo.value.code == "schema_version"
    assert not message.startswith("schema_version:")
    # Отказ, а не трейсбек: сообщение объясняет, что делать.
    assert "новом каталоге" in message or "удалите" in message


def test_newer_schema_is_rejected_too(tmp_path: Path) -> None:
    """Симметрия: файл «из будущего» тоже чужой — читать его нельзя (нет гарантий формы)."""
    db = _legacy_db(tmp_path / "future.db", "99.0.0")
    with pytest.raises(SchemaVersionMismatch):
        connect(db)


def test_fresh_db_is_created_and_reopened(tmp_path: Path) -> None:
    """Свежий файл получает текущую версию; повторное открытие — та же схема (идемпотентно)."""
    db = tmp_path / "fresh.db"
    for _ in range(3):
        conn = connect(db)
        try:
            row = conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            assert row is not None and row["value"] == SCHEMA_VERSION
        finally:
            conn.close()


def test_in_memory_db_is_not_gated() -> None:
    """``:memory:`` всегда новый — гейту нечего сверять (и он не мешает тестам/CLI)."""
    conn = connect(":memory:")
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        assert row is not None and row["value"] == SCHEMA_VERSION
    finally:
        conn.close()


def test_cli_reports_schema_mismatch_without_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """CLI: ``EXIT_USAGE`` + ``schema_version: …`` в stderr, без питоновского трейсбека."""
    db = _legacy_db(tmp_path / "cohound.db", "3.10.0")

    code = main(["crawl", "list", "--run-id", "r1", "--db", str(db)])

    captured = capsys.readouterr()
    assert code == EXIT_USAGE
    assert "schema_version:" in captured.err
    assert "3.10.0" in captured.err
    assert "Traceback" not in captured.err


def test_cli_reports_schema_mismatch_in_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Машинночитаемый путь: ``--json`` несёт код ``schema_version`` и человеческое сообщение."""
    import json

    db = _legacy_db(tmp_path / "cohound.db", "3.10.0")

    code = main(["crawl", "status", "--run-id", "r1", "--db", str(db), "--json"])

    assert code == EXIT_USAGE
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["ok"] is False
    assert payload["error"] == "schema_version"
    assert "3.10.0" in payload["message"]


def test_boundary_columns_exist_on_fresh_db_and_version_is_not_bumped() -> None:
    """Граница ``runs`` (16 §3): аддитивные колонки и таблица ``run_product`` — без bump.

    ``SCHEMA_VERSION`` здесь пинуется намеренно: подъём версии на аддитивном изменении
    (11 §2.6) отверг бы уже завершённые прогоны, то есть выполнимое требование «старые
    прогоны остаются читаемыми» было бы нарушено. Пин — единственный способ заметить
    случайный bump в ревью, а не в живом чтении.
    """
    assert SCHEMA_VERSION == "3.13.0"

    conn = connect(":memory:")
    try:
        runs_cols = {r["name"] for r in conn.execute("PRAGMA table_info(runs)")}
        assert {"run_scope", "product_plan_json", "embed_device"} <= runs_cols

        docs_cols = {r["name"] for r in conn.execute("PRAGMA table_info(corpus_docs)")}
        assert {"http_status", "reuse_kind"} <= docs_cols

        tables = {
            r["name"]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "run_product" in tables
        product_cols = {
            r["name"] for r in conn.execute("PRAGMA table_info(run_product)")
        }
        # 16 §3.2: ровно эти колонки; ``cells_carried``/``cells_measured`` не заводятся
        # (переноса ячеек нет, 09 §10, и колонка, всегда равная нулю, — ложь о механизме).
        assert product_cols == {
            "run_id",
            "product_id",
            "mode",
            "status",
            "scope",
            "phase",
            "coverage_complete",
            "docs_total",
            "docs_reused",
            "docs_new",
            "cells_total",
            "cells_pending",
            "baseline_complete",
            "error_kind",
            "error",
            "updated_at",
        }
        assert "cells_carried" not in product_cols
        assert "cells_measured" not in product_cols

        pk = [
            r["name"]
            for r in conn.execute("PRAGMA table_info(run_product)")
            if r["pk"]
        ]
        assert pk == ["run_id", "product_id"]
        fks = list(conn.execute("PRAGMA foreign_key_list(run_product)"))
        assert [(fk["table"], fk["from"], fk["to"]) for fk in fks] == [
            ("runs", "run_id", "run_id")
        ]
    finally:
        conn.close()


def test_3_13_0_db_without_boundary_columns_is_accepted_and_migrated(
    tmp_path: Path,
) -> None:
    """Файл той же версии без новых колонок читается и получает их (аддитивность).

    Это и есть смысл отказа от bump: граница применяется сразу после ``report``, и ни один
    существующий ``runs/*/cohound.db`` не теряется.
    """
    db = tmp_path / "cohound.db"
    conn = connect(db)
    try:
        for column in ("run_scope", "product_plan_json", "embed_device"):
            conn.execute("ALTER TABLE runs DROP COLUMN " + column)
        for column in ("http_status", "reuse_kind"):
            conn.execute("ALTER TABLE corpus_docs DROP COLUMN " + column)
        conn.execute("DROP TABLE run_product")
        conn.commit()
        # Версия в файле — та же (bump не делался), поэтому гейт молчит.
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        assert row["value"] == SCHEMA_VERSION
    finally:
        conn.close()

    migrated = connect(db)
    try:
        runs_cols = {r["name"] for r in migrated.execute("PRAGMA table_info(runs)")}
        assert {"run_scope", "product_plan_json", "embed_device"} <= runs_cols
        docs_cols = {
            r["name"] for r in migrated.execute("PRAGMA table_info(corpus_docs)")
        }
        assert {"http_status", "reuse_kind"} <= docs_cols
        tables = {
            r["name"]
            for r in migrated.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "run_product" in tables
    finally:
        migrated.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
