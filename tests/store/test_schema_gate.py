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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
