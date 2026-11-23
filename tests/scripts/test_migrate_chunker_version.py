"""Тесты пере-штамповки ``chunker_version`` (``scripts/migrate_chunker_version.py``).

Проверяется то, ради чего инструмент существует, — и то, чем он опасен:

* **dry-run ничего не пишет**: дамп обеих таблиц до и после совпадает байт-в-байт, файла
  аудита нет;
* **``--apply`` меняет ровно строки ``--from``**: другая версия (``md-heading-4`` — другое
  семейство чанкера) остаётся нетронутой, ``table_ref`` сохраняется, ``chunk_id`` и
  ``chunk_ids_digest`` пересчитаны **формулой писателя** (:func:`chunk_row_id`), а не
  оставлены прежними;
* **отказы до записи** (код ``2``): ``--to`` не равен объявленной версии, неизвестный
  ``--from`` (печатаются фактические версии), нет ``--reason``, нет обязательного
  аргумента, чужая схема, смешанное состояние;
* **аудит** — ровно одна строка JSONL с правильными ``rows``/``by_product``; повторный
  запуск отказывает и вторую строку не пишет.

Сеть не используется, ``state.db`` стенда не трогается: всё в ``tmp_path``.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from core.adapters.chunker_family import composite_chunker_version
from core.pipeline.incremental import _FIELD_SEP, chunk_row_id
from core.store.state_sqlite import STATE_SCHEMA_VERSION, connect_state
from core.util.hashing import content_hash, short_hash

_ROOT = Path(__file__).resolve().parents[2]

#: Семейство «другое»: его строки миграция трогать не должна ни при каких условиях.
OTHER_FAMILY = "md-heading-4+text-para-2"

#: Мигрируемая версия в фикстурах: полная строка нужна, чтобы отказ «неизвестный --from»
#: печатал её рядом с фактическим распределением.
FROM_VERSION = "md-ast-1:test/policy-old+text-para-4"


def _load_module():
    """Модуль инструмента по пути: ``scripts/`` не пакет."""
    path = _ROOT / "scripts" / "migrate_chunker_version.py"
    spec = importlib.util.spec_from_file_location("migrate_chunker_version", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["migrate_chunker_version"] = module
    spec.loader.exec_module(module)
    return module


migrate = _load_module()

#: Целевая версия — та, что объявляет код (инструмент отказывает на любой другой).
TO_VERSION = composite_chunker_version()


# ------------------------------------------------------------------------ фикстуры


def _state_db(tmp_path: Path) -> Path:
    """Пустой ``state.db`` текущей схемы — фикстура строится кодом, а не копией стенда."""
    path = tmp_path / "state.db"
    conn = connect_state(path)
    conn.close()
    return path


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _add_doc(
    conn: sqlite3.Connection,
    *,
    product: str,
    version: str,
    suffix: str,
    chunks: int = 3,
    table_ref: str | None = None,
) -> str:
    """Документ + его набор кусков, записанные **как у писателя корпуса**.

    ``chunk_id`` — :func:`chunk_row_id`, ``chunk_ids_digest`` — дайджест набора id: иначе
    фикстура описывала бы состояние, которого код не создаёт, и проверка «id совпадает с
    формулой» была бы проверкой самой себя.
    """
    doc_version_id = f"dv_{product}_{suffix}"
    doc_hash = content_hash(f"{product} {suffix}")
    conn.execute(
        "INSERT INTO document_version (doc_version_id, doc_id, product_id, url,"
        " content_hash, extractor_version, level, raw_path, chunker_version,"
        " chunk_ids_digest, first_seen_run_id, last_seen_run_id, first_seen_at,"
        " last_seen_at, status)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            doc_version_id,
            f"{product}:{doc_hash}",
            product,
            f"https://example.test/{product}/{suffix}",
            doc_hash,
            "extractor-1",
            "text",
            None,
            version,
            None,
            "run-1",
            "run-1",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
            "active",
        ),
    )
    ids: list[str] = []
    for number in range(chunks):
        text = f"{product} {suffix} chunk {number}"
        digest = content_hash(text)
        conn.execute(
            "INSERT OR IGNORE INTO chunk_store (content_hash, text, lang) VALUES (?,?,?)",
            (digest, text, "ru"),
        )
        chunk_id = chunk_row_id(doc_version_id, version, number)
        ids.append(chunk_id)
        conn.execute(
            "INSERT INTO chunk_meta (chunk_id, doc_version_id, product_id, chunk_no,"
            " content_hash, chunker_version, heading_path, context_prefix, lang,"
            " char_start, char_end, table_ref) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                chunk_id,
                doc_version_id,
                product,
                number,
                digest,
                version,
                None,
                None,
                "ru",
                0,
                len(text),
                table_ref,
            ),
        )
    conn.execute(
        "UPDATE document_version SET chunk_ids_digest = ? WHERE doc_version_id = ?",
        (short_hash(_FIELD_SEP.join(sorted(ids)), 24), doc_version_id),
    )
    conn.commit()
    return doc_version_id


def _seed(path: Path) -> dict[str, str]:
    """Два продукта в ``--from``, один — в другом семействе, один — уже в ``--to``."""
    conn = _connect(path)
    try:
        ids = {
            "postgrespro": _add_doc(
                conn, product="postgrespro", version=FROM_VERSION, suffix="a", chunks=4
            ),
            "tantor": _add_doc(
                conn, product="tantor", version=FROM_VERSION, suffix="b", chunks=2
            ),
            "pangolin": _add_doc(
                conn,
                product="pangolin",
                version=OTHER_FAMILY,
                suffix="c",
                chunks=3,
                table_ref='{"table_id":"t1","row_start":0,"row_end":1}',
            ),
            "jatoba": _add_doc(
                conn, product="jatoba", version=TO_VERSION, suffix="d", chunks=2
            ),
        }
    finally:
        conn.close()
    return ids


def _dump(path: Path) -> tuple[list[tuple], list[tuple], list[tuple]]:
    """Полный дамп обеих таблиц версий + ``chunk_store`` (текст тоже не должен меняться)."""
    conn = _connect(path)
    try:
        return (
            [
                tuple(row)
                for row in conn.execute(
                    "SELECT * FROM document_version ORDER BY doc_version_id"
                )
            ],
            [
                tuple(row)
                for row in conn.execute("SELECT * FROM chunk_meta ORDER BY chunk_id")
            ],
            [
                tuple(row)
                for row in conn.execute(
                    "SELECT * FROM chunk_store ORDER BY content_hash"
                )
            ],
        )
    finally:
        conn.close()


def _run(path: Path, *, apply: bool = False, **overrides: str) -> int:
    """Вызов ``main`` с аргументами по умолчанию, перекрываемыми по имени."""
    args = {
        "state_db": str(path),
        "from_version": FROM_VERSION,
        "to_version": TO_VERSION,
        "reason": "policy-6: table_ref не входит в индекс/embed_input",
    }
    args.update(overrides)
    argv = [
        "--state-db",
        args["state_db"],
        "--from",
        args["from_version"],
        "--to",
        args["to_version"],
        "--reason",
        args["reason"],
    ]
    if apply:
        argv.append("--apply")
    return migrate.main(argv)


def _audit_path(path: Path) -> Path:
    return path.parent / migrate.AUDIT_NAME


# ------------------------------------------------------------------- dry-run / apply


def test_dry_run_changes_nothing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = _state_db(tmp_path)
    _seed(path)
    before = _dump(path)

    code = _run(path)

    assert code == migrate.EXIT_OK
    assert _dump(path) == before, "dry-run обязан не менять ни байта"
    assert not _audit_path(path).exists(), "dry-run не пишет аудит"
    out = capsys.readouterr().out
    assert "dry-run" in out
    assert "postgrespro" in out and "tantor" in out


def test_apply_restamps_only_from_version(tmp_path: Path) -> None:
    path = _state_db(tmp_path)
    ids = _seed(path)

    assert _run(path, apply=True) == migrate.EXIT_OK

    conn = _connect(path)
    try:
        versions = {
            str(row["product_id"]): str(row["chunker_version"])
            for row in conn.execute("SELECT product_id, chunker_version FROM document_version")
        }
        assert versions["postgrespro"] == TO_VERSION
        assert versions["tantor"] == TO_VERSION
        # Другое семейство и уже мигрированный продукт — не тронуты.
        assert versions["pangolin"] == OTHER_FAMILY
        assert versions["jatoba"] == TO_VERSION

        chunk_versions = {
            (str(row["product_id"]), str(row["chunker_version"]))
            for row in conn.execute("SELECT product_id, chunker_version FROM chunk_meta")
        }
        assert ("postgrespro", FROM_VERSION) not in chunk_versions
        assert ("tantor", FROM_VERSION) not in chunk_versions
        assert ("pangolin", OTHER_FAMILY) in chunk_versions

        migrated = int(
            conn.execute(
                "SELECT count(*) FROM document_version WHERE chunker_version = ?",
                (TO_VERSION,),
            ).fetchone()[0]
        )
        assert migrated == 3, "два мигрированных + один уже в целевой версии"
        assert (
            int(
                conn.execute(
                    "SELECT count(*) FROM document_version WHERE chunker_version = ?",
                    (FROM_VERSION,),
                ).fetchone()[0]
            )
            == 0
        )
        # table_ref другого семейства сохранён как был (адрес не выдумывается и не стирается).
        table_ref = conn.execute(
            "SELECT table_ref FROM chunk_meta WHERE doc_version_id = ? ORDER BY chunk_no",
            (ids["pangolin"],),
        ).fetchone()[0]
        assert table_ref == '{"table_id":"t1","row_start":0,"row_end":1}'
    finally:
        conn.close()


def test_apply_recomputes_chunk_ids_and_digest(tmp_path: Path) -> None:
    path = _state_db(tmp_path)
    ids = _seed(path)

    assert _run(path, apply=True) == migrate.EXIT_OK

    conn = _connect(path)
    try:
        for doc_version_id in (ids["postgrespro"], ids["tantor"]):
            rows = conn.execute(
                "SELECT chunk_id, chunk_no, chunker_version FROM chunk_meta"
                " WHERE doc_version_id = ? ORDER BY chunk_no",
                (doc_version_id,),
            ).fetchall()
            expected_ids = []
            for row in rows:
                expected = chunk_row_id(doc_version_id, TO_VERSION, int(row["chunk_no"]))
                assert str(row["chunk_id"]) == expected, (
                    "PK обязан совпадать с формулой писателя, иначе строка противоречит"
                    " собственной формуле"
                )
                assert str(row["chunker_version"]) == TO_VERSION
                expected_ids.append(expected)
            stored = conn.execute(
                "SELECT chunk_ids_digest FROM document_version WHERE doc_version_id = ?",
                (doc_version_id,),
            ).fetchone()[0]
            assert stored == short_hash(_FIELD_SEP.join(sorted(expected_ids)), 24)
        # Адреса не выдуманы: у мигрированных чанков table_ref остался NULL.
        null_refs = conn.execute(
            "SELECT count(*) FROM chunk_meta WHERE doc_version_id IN (?, ?)"
            " AND table_ref IS NOT NULL",
            (ids["postgrespro"], ids["tantor"]),
        ).fetchone()[0]
        assert null_refs == 0
    finally:
        conn.close()


def test_null_digest_stays_null(tmp_path: Path) -> None:
    """``chunk_ids_digest = NULL`` («не записано») не выдумывается, версия мигрирует."""
    path = _state_db(tmp_path)
    conn = _connect(path)
    try:
        doc_version_id = _add_doc(
            conn, product="postgrespro", version=FROM_VERSION, suffix="n", chunks=2
        )
        conn.execute(
            "UPDATE document_version SET chunk_ids_digest = NULL WHERE doc_version_id = ?",
            (doc_version_id,),
        )
        conn.commit()
    finally:
        conn.close()

    assert _run(path, apply=True) == migrate.EXIT_OK

    conn = _connect(path)
    try:
        row = conn.execute(
            "SELECT chunker_version, chunk_ids_digest FROM document_version"
            " WHERE doc_version_id = ?",
            (doc_version_id,),
        ).fetchone()
        assert str(row["chunker_version"]) == TO_VERSION
        assert row["chunk_ids_digest"] is None, "NULL обязан остаться NULL"
        ids = [
            str(item["chunk_id"])
            for item in conn.execute(
                "SELECT chunk_id, chunk_no FROM chunk_meta WHERE doc_version_id = ?"
                " ORDER BY chunk_no",
                (doc_version_id,),
            )
        ]
        assert ids == [
            chunk_row_id(doc_version_id, TO_VERSION, number) for number in range(2)
        ]
    finally:
        conn.close()


# --------------------------------------------------------------------------- отказы


def test_refuses_when_to_is_not_declared(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _state_db(tmp_path)
    _seed(path)
    before = _dump(path)

    code = _run(path, apply=True, to_version="md-ast-1:made-up/policy-99")

    assert code == migrate.EXIT_USAGE
    assert _dump(path) == before
    out = capsys.readouterr().out
    assert "ОТКАЗ" in out
    assert "md-ast-1:made-up/policy-99" in out and TO_VERSION in out


def test_refuses_unknown_from(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = _state_db(tmp_path)
    _seed(path)
    before = _dump(path)

    code = _run(path, apply=True, from_version="md-ast-1:nope/policy-x")

    assert code == migrate.EXIT_USAGE
    assert _dump(path) == before
    out = capsys.readouterr().out
    assert "ОТКАЗ" in out
    # Фактическое распределение напечатано с количествами — «искали не то» видно.
    assert FROM_VERSION in out
    assert OTHER_FAMILY in out
    assert "фактическое распределение версий" in out


def test_refuses_without_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _state_db(tmp_path)
    _seed(path)
    before = _dump(path)

    code = _run(path, apply=True, reason="   ")

    assert code == migrate.EXIT_USAGE
    assert _dump(path) == before
    assert "--reason" in capsys.readouterr().out


def test_refuses_without_required_argument(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _state_db(tmp_path)
    _seed(path)
    before = _dump(path)

    code = migrate.main(["--state-db", str(path), "--to", TO_VERSION, "--reason", "x"])

    assert code == migrate.EXIT_USAGE
    assert _dump(path) == before
    assert "--from" in capsys.readouterr().out


def test_refuses_foreign_schema(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = _state_db(tmp_path)
    _seed(path)
    conn = _connect(path)
    try:
        conn.execute(
            "UPDATE schema_meta SET value = '13' WHERE key = 'schema_version'"
        )
        conn.commit()
    finally:
        conn.close()
    before = _dump(path)

    code = _run(path, apply=True)

    assert code == migrate.EXIT_USAGE
    assert _dump(path) == before
    out = capsys.readouterr().out
    assert STATE_SCHEMA_VERSION in out and "'13'" in out


def test_refuses_mixed_state(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Версия документа ``--from``, а куски — уже ``--to``: половина миграции."""
    path = _state_db(tmp_path)
    conn = _connect(path)
    try:
        doc_version_id = _add_doc(
            conn, product="postgrespro", version=FROM_VERSION, suffix="m", chunks=2
        )
        # Подменяем версию кусков: набор «другой версии», чем версия документа.
        conn.execute(
            "UPDATE chunk_meta SET chunker_version = ? WHERE doc_version_id = ?",
            (TO_VERSION, doc_version_id),
        )
        conn.commit()
    finally:
        conn.close()
    before = _dump(path)

    code = _run(path, apply=True)

    assert code == migrate.EXIT_USAGE
    assert _dump(path) == before
    out = capsys.readouterr().out
    assert "состояние смешано" in out


# ---------------------------------------------------------------------------- аудит


def test_audit_line_written_once(tmp_path: Path) -> None:
    path = _state_db(tmp_path)
    _seed(path)

    assert _run(path, apply=True) == migrate.EXIT_OK

    audit = _audit_path(path)
    assert audit.is_file()
    lines = audit.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["from"] == FROM_VERSION
    assert record["to"] == TO_VERSION
    assert record["reason"] == "policy-6: table_ref не входит в индекс/embed_input"
    assert record["rows"] == 2, "строк document_version мигрировано"
    assert record["chunk_rows"] == 6, "4 + 2 куска"
    assert record["by_product"] == {"postgrespro": 1, "tantor": 1}
    assert isinstance(record["ts"], str) and record["ts"]

    # Повторный запуск: --from больше нет — отказ, вторая строка не дописана.
    assert _run(path, apply=True) == migrate.EXIT_USAGE
    assert len(audit.read_text(encoding="utf-8").splitlines()) == 1
