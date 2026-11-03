"""Тесты ``hound package state adopt`` / ``state show`` (09 §4.6; офлайн, ``tmp_path``).

``state.db`` наполняется только прогоном, поэтому без adoption первый ``run --update``
видит у всех продуктов ``last_corpus_run_id = NULL`` и делает их ``new`` — полный краул
всей когорты. Проверяются обещания §4.6: принятие **per-product** (продукт без покрытия
остаётся ``new`` с видимой причиной), идемпотентность, ``--dry-run`` без записи,
``doc_version_id`` по формуле корпуса, отказ партиции при расхождении ``content_digest``,
перенос измерений ячеек только при совпадении ``methodology_version`` и предупреждение
``state_not_adopted`` в ``state show``/``run --update``.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, main
from core.domain.models import METHODOLOGY_VERSION
from core.pipeline.incremental import doc_version_id
from core.pipeline.onboarding import _mode_for
from core.pipeline.state_adopt import plan_adoption, read_state_summary
from core.store.state_sqlite import state_db_path
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash, short_hash
from tests.helpers.packages import valid_package

#: Разделитель полей — тот же unit separator, что в корпусе/инкрементальном плане.
_SEP = "\x1f"
#: Уровень извлечения, который объявляет тестовый ``run.yaml`` (входит в ``doc_version_id``).
_LEVEL = "full"


# --------------------------------------------------------------------------- #
# Синтетический прогон: run DB + партиция индекса на диске
# --------------------------------------------------------------------------- #


def _chunk_text(product_id: str, no: int) -> str:
    """Детерминированный текст чанка (адресация — его ``content_hash``)."""
    return f"текст {product_id} №{no}"


def _content_token(pairs: Sequence[tuple[str, str]]) -> str:
    """Отпечаток содержимого партиции — формула ``corpus._product_content_token``."""
    material = _SEP.join(f"{cid}{_SEP}{digest}" for cid, digest in sorted(pairs))
    return short_hash(material, 24)


def _write_partition(
    index_root: Path,
    package_id: str,
    partition_id: str,
    product_id: str,
    *,
    digest: str,
) -> None:
    """Фиксирует продукт-партицию на диске (три артефакта + ``manifest.json``)."""
    root = index_root / package_id / partition_id / product_id
    root.mkdir(parents=True, exist_ok=True)
    (root / "vectors.fp16.npy").write_bytes(b"npy")
    (root / "chunk_ids.jsonl").write_text("", encoding="utf-8")
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "schema": "partition/1",
                "package_id": package_id,
                "product_id": product_id,
                "partition_build_id": partition_id,
                "content_digest": digest,
                "chunker_version": "md-heading-2+text-para-1",
                "extractor_version": "extract-1",
                "embed_model_id": "embed-1",
                "lexical_engine_version": "bm25-1",
                "vector_store": "npy",
                "embed_dim": 8,
                "created_at": "2026-10-01T09:00:00+00:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _product_spec(
    product_id: str,
    *,
    docs: int = 1,
    chunks_per_doc: int = 2,
    coverage: bool = True,
    coverage_complete: bool = True,
    cells: Sequence[str] = ("param_a",),
    partition: str | None = "pb_1",
) -> dict[str, Any]:
    """Описание продукта в прогоне: документы, чанки, покрытие, ячейки, партиция."""
    return {
        "product_id": product_id,
        "docs": [
            {
                "doc_id": f"{product_id}:h{no}",
                "url": f"https://{product_id}.example/docs/{no}",
                "content_hash": f"h{no:04d}" + "0" * 60,
                "fetched_at": "2026-10-01T08:00:00+00:00",
                "extractor_version": "extract-1",
            }
            for no in range(docs)
        ],
        "chunks_per_doc": chunks_per_doc,
        "coverage": (
            {"complete": coverage_complete, "expected": 1, "observed": 1}
            if coverage
            else None
        ),
        "cells": list(cells),
        "partition": partition,
    }


def _write_run(
    runs_dir: Path,
    index_root: Path,
    run_id: str,
    *,
    package_id: str,
    products: Sequence[Mapping[str, Any]],
    phases: Mapping[str, str] | None = None,
    methodology_version: str = METHODOLOGY_VERSION,
    snapshot_date: str | None = "2026-10-01",
    created_at: str = "2026-10-01T10:00:00+00:00",
    digest_override: str | None = None,
    write_partition: bool = True,
    run_yaml: bool = True,
) -> Path:
    """Создаёт ``runs/{run_id}/cohound.db`` (+ копию ``run.yaml``) и партиции на диске.

    ``digest_override`` подменяет ``index_manifest.content_digest`` — так проверяется
    отказ от партиции с неверным отпечатком.
    """
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    if run_yaml:
        (run_dir / "run.yaml").write_text(
            "case_id: case_x\n"
            "package: case_x\n"
            f"crawl:\n  document_level: {_LEVEL}\n",
            encoding="utf-8",
        )
    state = {"phase_state": dict(phases or {"corpus": "ok", "evaluate": "ok"})}
    conn = sqlite3.connect(run_dir / "cohound.db")
    try:
        conn.executescript(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                created_at TEXT,
                package_id TEXT,
                methodology_version TEXT,
                execution_contract_digest TEXT,
                snapshot_date TEXT,
                state TEXT
            );
            CREATE TABLE corpus_docs (
                doc_id TEXT PRIMARY KEY, product_id TEXT, url TEXT, fetched_at TEXT,
                content_hash TEXT, raw_path TEXT, extractor_version TEXT,
                partial INTEGER DEFAULT 0, partial_reason TEXT, page_count INTEGER,
                pages_converted INTEGER
            );
            CREATE TABLE corpus_chunks (
                chunk_id TEXT PRIMARY KEY, doc_id TEXT, product_id TEXT, chunk_no INTEGER,
                text TEXT, content_hash TEXT, heading_path TEXT, context_prefix TEXT,
                lang TEXT
            );
            CREATE TABLE corpus_coverage (
                run_id TEXT, product_id TEXT, complete INTEGER,
                expected_sources INTEGER, observed_sources INTEGER,
                computed_at TEXT, reason TEXT, truncated INTEGER DEFAULT 0
            );
            CREATE TABLE index_manifest (
                index_build_id TEXT, partition_build_id TEXT, product_id TEXT,
                package_id TEXT, chunker_version TEXT, extractor_version TEXT,
                embed_model_id TEXT, embed_dim INTEGER, lexical_engine_version TEXT,
                vector_store TEXT, content_digest TEXT, store_path TEXT, created_at TEXT,
                PRIMARY KEY (index_build_id, product_id)
            );
            CREATE TABLE cells (
                run_id TEXT, product_id TEXT, param_id TEXT, state TEXT, status TEXT,
                prompt_hash TEXT, model_id TEXT, llm_scenario TEXT, index_build_id TEXT,
                updated_at TEXT,
                PRIMARY KEY (run_id, product_id, param_id)
            );
            """
        )
        conn.execute(
            "INSERT INTO runs (run_id, created_at, package_id, methodology_version,"
            " execution_contract_digest, snapshot_date, state)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                created_at,
                package_id,
                methodology_version,
                "contract-digest-1",
                snapshot_date,
                json.dumps(state),
            ),
        )
        for spec in products:
            product_id = str(spec["product_id"])
            pairs: list[tuple[str, str]] = []
            for doc in spec["docs"]:
                conn.execute(
                    "INSERT INTO corpus_docs (doc_id, product_id, url, fetched_at,"
                    " content_hash, raw_path, extractor_version, partial, page_count)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, 0, 3)",
                    (
                        doc["doc_id"],
                        product_id,
                        doc["url"],
                        doc["fetched_at"],
                        doc["content_hash"],
                        f"corpus_raw/{product_id}/raw.html",
                        doc["extractor_version"],
                    ),
                )
                for no in range(int(spec["chunks_per_doc"])):
                    text = _chunk_text(product_id, no)
                    digest = content_hash(text)
                    chunk_id = f"{doc['doc_id']}:{no}"
                    conn.execute(
                        "INSERT INTO corpus_chunks (chunk_id, doc_id, product_id,"
                        " chunk_no, text, content_hash, heading_path, context_prefix,"
                        " lang) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            chunk_id,
                            doc["doc_id"],
                            product_id,
                            no,
                            text,
                            digest,
                            f"{product_id} > раздел {no}",
                            f"{product_id} > раздел {no}",
                            "ru",
                        ),
                    )
                    pairs.append((chunk_id, digest))
            coverage = spec.get("coverage")
            if coverage is not None:
                conn.execute(
                    "INSERT INTO corpus_coverage (run_id, product_id, complete,"
                    " expected_sources, observed_sources, computed_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        product_id,
                        int(coverage["complete"]),
                        int(coverage["expected"]),
                        int(coverage["observed"]),
                        "2026-10-01T09:30:00+00:00",
                    ),
                )
            partition = spec.get("partition")
            if partition:
                digest = _content_token(pairs)
                conn.execute(
                    "INSERT INTO index_manifest (index_build_id, partition_build_id,"
                    " product_id, package_id, chunker_version, extractor_version,"
                    " embed_model_id, embed_dim, lexical_engine_version, vector_store,"
                    " content_digest, store_path, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "ib_1",
                        partition,
                        product_id,
                        package_id,
                        "md-heading-2+text-para-1",
                        "extract-1",
                        "embed-1",
                        8,
                        "bm25-1",
                        "npy",
                        digest_override or digest,
                        f"index_store/{package_id}/{partition}/{product_id}/",
                        "2026-10-01T09:00:00+00:00",
                    ),
                )
                if write_partition:
                    _write_partition(
                        index_root, package_id, partition, product_id, digest=digest
                    )
            for param_id in spec.get("cells", ()):
                conn.execute(
                    "INSERT INTO cells (run_id, product_id, param_id, state, status,"
                    " prompt_hash, model_id, llm_scenario, index_build_id, updated_at)"
                    " VALUES (?, ?, ?, 'evaluated', '✅', 'ph1', 'model-1', 'scenario-1',"
                    " 'ib_1', '2026-10-01T09:40:00+00:00')",
                    (run_id, product_id, param_id),
                )
        conn.commit()
    finally:
        conn.close()
    return run_dir


# --------------------------------------------------------------------------- #
# Обвязка CLI/раскладки
# --------------------------------------------------------------------------- #


def _setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path]:
    """Пакет ``case_x`` + ``runs/`` + ``index_store/``; дом раскладки = ``tmp_path``.

    ``COHOUND_HOME`` объявляется **явно**: сессионная фикстура (``tests/conftest.py``)
    уводит операционные каталоги во временный дом сессии, и без объявления ``state``
    уехал бы туда — тест утверждал бы ``tmp_path/state/{package_id}/state.db``, которого
    никто не пишет. Дом ``tmp_path`` возвращает прежнюю раскладку, но теперь намеренно.
    ``packages``/``passports`` фикстура закрепляет абсолютными путями репозитория, так что
    пакет здесь по-прежнему передаётся флагом ``--package``.
    """
    package = valid_package(tmp_path).rename(tmp_path / "case_x")
    monkeypatch.chdir(tmp_path)  # cwd = tmp_path: относительные пути теста совпадают с домом
    monkeypatch.setenv("COHOUND_HOME", str(tmp_path))
    runs_dir = tmp_path / "runs"
    index_root = tmp_path / "index_store"
    runs_dir.mkdir()
    index_root.mkdir()
    return package, runs_dir, index_root


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _adopt_argv(
    package: Path,
    runs_dir: Path,
    index_root: Path,
    *,
    extra: Sequence[str] = (),
) -> list[str]:
    """``state adopt`` с явными ``--runs-dir``/``--index-store`` и ``--json``."""
    return [
        "package",
        "state",
        "adopt",
        "--package",
        str(package),
        "--runs-dir",
        str(runs_dir),
        "--index-store",
        str(index_root),
        "--actor",
        "tester",
        "--json",
        *extra,
    ]


def _state_db(tmp_path: Path, package_id: str = "case_x") -> Path:
    """Ожидаемый путь ``state/{package_id}/state.db`` при cwd = ``tmp_path``."""
    return state_db_path(tmp_path / "state", package_id)


def _counts(db_path: Path) -> dict[str, int]:
    """Число строк durable-таблиц (для проверок идемпотентности/атомарности)."""
    tables = (
        "document_version",
        "chunk_meta",
        "chunk_store",
        "url_state",
        "index_partition",
        "cell_measurement",
        "product_snapshot",
        "run_lineage",
    )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])
            for table in tables
        }
    finally:
        conn.close()


def _query(db_path: Path, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
    """Строки запроса к ``state.db`` (read-only по смыслу: только ``SELECT``)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return list(conn.execute(sql, tuple(params)))
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Принятие: корпус, партиция, снимок, lineage
# --------------------------------------------------------------------------- #


def test_adopt_writes_corpus_partition_and_lineage(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Полный adoption: документы, чанки, ``url_state``, партиция, снимок, lineage."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["run_id"] == "run-1"
    assert payload["cycle_id"] == "2026-10-01"
    assert payload["dry_run"] is False
    assert payload["plan"]["accepted"] == ["p1"]
    assert payload["lineage_written"] is True
    db_path = _state_db(tmp_path)
    assert db_path.is_file()
    counts = _counts(db_path)
    assert counts["document_version"] == 1
    assert counts["chunk_meta"] == 2
    assert counts["url_state"] == 1
    assert counts["index_partition"] == 1
    assert counts["product_snapshot"] == 1
    assert counts["run_lineage"] == 1
    assert counts["cell_measurement"] == 1

    url_row = _query(db_path, "SELECT * FROM url_state")[0]
    # §4.6: валидаторы и статус в прогонной БД не хранятся — NULL, а не выдумка.
    assert url_row["etag"] is None
    assert url_row["last_modified"] is None

    snapshot = _query(db_path, "SELECT * FROM product_snapshot")[0]
    assert snapshot["status"] == "complete"
    assert snapshot["scope"] == "full"
    assert snapshot["coverage_complete"] == 1
    assert snapshot["baseline_complete"] == 1
    assert snapshot["docs_total"] == 1
    assert snapshot["docs_new"] == 1
    assert snapshot["partition_build_id"] == "pb_1"
    # Метка снимка — время прогона, а не часы: повторный adoption её не сдвинет.
    assert snapshot["created_at"] == "2026-10-01T10:00:00+00:00"

    state_row = _query(db_path, "SELECT * FROM product_state")[0]
    assert state_row["last_corpus_run_id"] == "run-1"
    assert state_row["last_evaluated_run_id"] == "run-1"
    assert state_row["last_completed_run_id"] == "run-1"
    assert state_row["last_completed_cycle_id"] == "2026-10-01"

    lineage = _query(db_path, "SELECT * FROM run_lineage")[0]
    assert lineage["run_id"] == "run-1"
    assert lineage["cycle_id"] == "2026-10-01"
    assert lineage["execution_contract_digest"] == "contract-digest-1"
    # Ограничение названо: дайджестов когорты/seed'ов в прогонной БД нет — пустая строка.
    assert lineage["cohort_digest"] == ""
    assert lineage["seeds_digest"] == ""
    assert lineage["previous_run_id"] is None
    assert any("cohort_digest" in item for item in payload["limitations"])


def test_adopt_doc_version_id_matches_corpus_formula(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``doc_version_id`` считается формулой корпуса, иначе документ невидим плану."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )

    code, _, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    expected = doc_version_id("p1", "h0000" + "0" * 60, "extract-1", _LEVEL)
    rows = _query(_state_db(tmp_path), "SELECT doc_version_id, level FROM document_version")
    assert [row["doc_version_id"] for row in rows] == [expected]
    assert rows[0]["level"] == _LEVEL


def test_adopt_is_idempotent(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Повторный adoption не плодит строк и не замыкает цепочку lineage на себя."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )

    assert _run(capsys, _adopt_argv(package, runs_dir, index_root))[0] == EXIT_OK
    first = _counts(_state_db(tmp_path))
    assert _run(capsys, _adopt_argv(package, runs_dir, index_root))[0] == EXIT_OK
    second = _counts(_state_db(tmp_path))

    assert first == second
    lineage = _query(_state_db(tmp_path), "SELECT * FROM run_lineage")
    assert len(lineage) == 1
    assert lineage[0]["previous_run_id"] is None


def test_adopt_dry_run_does_not_create_state_db(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--dry-run`` не пишет и не создаёт ``state.db`` (обещание флага)."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root, extra=["--dry-run"]))

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["lineage_written"] is False
    assert payload["totals"]["documents"] == 1
    assert not _state_db(tmp_path).exists()
    assert not (tmp_path / "state").exists()


# --------------------------------------------------------------------------- #
# Продукт-атомарность и причины отказа
# --------------------------------------------------------------------------- #


def test_adopt_skips_product_without_coverage(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Продукт без покрытия остаётся ``new``: видимая причина, ни одной строки."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[
            _product_spec("p1"),
            _product_spec("p2", coverage=False, partition=None, cells=()),
        ],
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["plan"]["accepted"] == ["p1"]
    assert payload["plan"]["skipped"] == ["p2"]
    skipped = next(
        item for item in payload["plan"]["products"] if item["product_id"] == "p2"
    )
    assert "no_coverage" in skipped["reasons"]
    assert skipped["snapshot"] is None

    db_path = _state_db(tmp_path)
    products = {
        row["product_id"]
        for row in _query(db_path, "SELECT DISTINCT product_id FROM document_version")
    }
    assert products == {"p1"}
    assert not _query(db_path, "SELECT 1 FROM product_state WHERE product_id = 'p2'")


def test_adopt_rejects_incomplete_run(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Частичный прогон (оценка не прошла) не принимается и причина названа."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
        phases={"corpus": "ok", "evaluate": "failed"},
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_NOT_FOUND
    payload = json.loads(out)
    assert payload["found"] is False
    assert payload["reasons"] == ["no_completed_run"]
    # Причина непригодности прогона не исчезает: она в кандидатах, а не в догадке.
    assert [item["run_id"] for item in payload["candidates"]] == ["run-1"]
    assert payload["candidates"][0]["complete"] is False
    assert payload["candidates"][0]["reason"] == "phase_evaluate_failed"
    assert not _state_db(tmp_path).exists()

    # Явно названный прогон отвечает точнее: отказ относится именно к нему.
    code, out, _ = _run(
        capsys, _adopt_argv(package, runs_dir, index_root, extra=["--from-run", "run-1"])
    )

    assert code == EXIT_NOT_FOUND
    assert json.loads(out)["reasons"] == ["run_not_adoptable:phase_evaluate_failed"]


def test_adopt_requires_actor(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--actor`` обязателен: мутация durable-состояния пишется с провенансом."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )
    argv = _adopt_argv(package, runs_dir, index_root)
    argv = [item for item in argv if item not in ("--actor", "tester")]

    code, _, err = _run(capsys, argv)

    assert code == EXIT_USAGE
    assert "--actor" in err


def test_adopt_methodology_mismatch_keeps_cells_out(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Другая ``methodology_version``: корпус принят, измерения ячеек — нет."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
        methodology_version="0.0.9",
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["plan"]["methodology_matches"] is False
    cells = payload["plan"]["products"][0]["cells"]
    assert cells["adopted"] is False
    assert cells["reason"] == "methodology_mismatch"
    db_path = _state_db(tmp_path)
    assert _counts(db_path)["document_version"] == 1
    assert _counts(db_path)["cell_measurement"] == 0


def test_adopt_rejects_partition_with_digest_mismatch(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Расхождение ``content_digest`` — партиция не принята, причина видна."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
        digest_override="digest-from-another-build",
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    payload = json.loads(out)
    partition = payload["plan"]["products"][0]["partitions"][0]
    assert partition["verified"] is False
    assert partition["reason"].startswith("content_digest_mismatch")
    assert partition["recomputed_digest"] != partition["content_digest"]
    assert any(
        warning.startswith("partition_content_digest_mismatch:p1")
        for warning in payload["plan"]["warnings"]
    )
    db_path = _state_db(tmp_path)
    # Корпус принят (продукт-атомарность — про документы), партиция — нет.
    assert _counts(db_path)["document_version"] == 1
    assert _counts(db_path)["index_partition"] == 0


def test_adopt_rejects_partition_missing_on_disk(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Партиции нет на диске — не принимается (непроверенный digest не пишется)."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
        write_partition=False,
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    partition = json.loads(out)["plan"]["products"][0]["partitions"][0]
    assert partition["verified"] is False
    assert partition["reason"] == "partition_not_committed"
    assert _counts(_state_db(tmp_path))["index_partition"] == 0


def test_adopt_rejects_partition_with_foreign_store_path(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``store_path`` пересчитывается по раскладке: чужой путь — причина, не запись."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )
    conn = sqlite3.connect(runs_dir / "run-1" / "cohound.db")
    try:
        conn.execute(
            "UPDATE index_manifest SET store_path = 'index_store/case_x/pb_1/other/'"
        )
        conn.commit()
    finally:
        conn.close()

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    partition = json.loads(out)["plan"]["products"][0]["partitions"][0]
    assert partition["verified"] is False
    assert partition["reason"].startswith("partition_store_path_mismatch")
    # Путь в плане — пересчитанный, а не взятый из манифеста.
    assert partition["store_path"] == "index_store/case_x/pb_1/p1/"
    assert _counts(_state_db(tmp_path))["index_partition"] == 0


def test_adopt_document_level_defaulted_is_visible(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без объявленного уровня в копии ``run.yaml`` дефолт назван, а не выдан за факт."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
        run_yaml=False,
    )

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    plan = json.loads(out)["plan"]
    assert plan["document_level"] == "full"
    assert plan["document_level_source"] == "default"
    assert any(
        warning.startswith("document_level_defaulted") for warning in plan["warnings"]
    )


def test_adopt_explicit_from_run_selects_named_run(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--from-run`` выбирает названный прогон, а не последний."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-old",
        package_id="case_x",
        products=[_product_spec("p1")],
        created_at="2026-09-01T10:00:00+00:00",
        snapshot_date="2026-09-01",
    )
    _write_run(
        runs_dir,
        index_root,
        "run-new",
        package_id="case_x",
        products=[_product_spec("p1")],
        created_at="2026-10-01T10:00:00+00:00",
        snapshot_date="2026-10-01",
    )

    code, out, _ = _run(
        capsys, _adopt_argv(package, runs_dir, index_root, extra=["--from-run", "run-old"])
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["run_id"] == "run-old"
    assert payload["cycle_id"] == "2026-09-01"


# --------------------------------------------------------------------------- #
# ``state show`` и предупреждение ``state_not_adopted``
# --------------------------------------------------------------------------- #


def test_state_show_warns_state_not_adopted(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пустое состояние + завершённый прогон → ``state_not_adopted`` с подсказкой."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )

    code, out, _ = _run(
        capsys,
        [
            "package",
            "state",
            "show",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--json",
        ],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["exists"] is False
    assert payload["empty"] is True
    assert any("state_not_adopted" in item for item in payload["warnings"])
    assert any("state adopt" in item for item in payload["warnings"])
    # Read-only: показ состояния не создаёт файл.
    assert not _state_db(tmp_path).exists()


def test_state_show_after_adopt_reports_milestones(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """После adoption ``show`` видит рубежи, снимок и lineage; предупреждения нет."""
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )
    assert _run(capsys, _adopt_argv(package, runs_dir, index_root))[0] == EXIT_OK

    code, out, _ = _run(
        capsys,
        [
            "package",
            "state",
            "show",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--json",
        ],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["exists"] is True
    assert payload["empty"] is False
    assert [row["product_id"] for row in payload["products"]] == ["p1"]
    assert payload["products"][0]["last_completed_run_id"] == "run-1"
    assert payload["counts"]["document_version"] == 1
    assert [row["run_id"] for row in payload["lineage"]] == ["run-1"]
    assert payload["warnings"] == []
    # Субстрат по продукту: разрез «данные» отдельно от «рубежей».
    assert payload["substrate"]["p1"]["docs"] == 1
    assert payload["substrate"]["p1"]["chunks"] == 2
    assert payload["substrate"]["p1"]["partitions"] == 1

def test_state_show_separates_substrate_from_milestones(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Субстрат есть, рубежей нет: ``empty=True``, но счётчики названы, а не спрятаны.

    Это не гипотетический случай: запись идёт на границе продукта (§4.7), а рубежи ставит
    ``record_run_outcome`` в конце. Убитый прогон оставляет ровно такое состояние, и
    «пусто» рядом с непустым субстратом читалось бы как ложь.
    """
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )
    assert _run(capsys, _adopt_argv(package, runs_dir, index_root))[0] == EXIT_OK

    # Снимаем рубежи, оставляя субстрат: ровно то, что оставляет прерванный прогон.
    db_path = _state_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DELETE FROM product_state")
        conn.execute("DELETE FROM run_lineage")
        conn.commit()
    finally:
        conn.close()

    code, out, _ = _run(
        capsys,
        [
            "package",
            "state",
            "show",
            "--package",
            str(package),
            "--runs-dir",
            str(runs_dir),
            "--json",
        ],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["empty"] is True
    assert payload["substrate"]["p1"]["docs"] == 1
    assert payload["substrate"]["p1"]["chunks"] == 2
    (warning,) = payload["warnings"]
    assert warning.startswith("state_not_adopted:")
    assert "docs=1" in warning and "chunks=2" in warning


def test_state_show_without_state_and_runs_is_quiet(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ни состояния, ни прогонов — предупреждения нет (нечего принимать)."""
    package, runs_dir, _index_root = _setup(tmp_path, monkeypatch)

    code, out, _ = _run(
        capsys,
        ["package", "state", "show", "--package", str(package), "--runs-dir", str(runs_dir), "--json"],
    )

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["exists"] is False
    assert payload["warnings"] == []


def test_run_update_notice_names_adopt_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``run --update`` получает ``state_not_adopted`` с готовой командой (§4.6)."""
    from core.cli.commands.run import _state_not_adopted_notice

    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )
    args = argparse.Namespace(runs_dir=str(runs_dir), package=str(package))

    notice = _state_not_adopted_notice(args, "case_x", None)

    assert notice is not None
    assert notice.startswith("state_not_adopted")
    assert "hound package state adopt" in notice
    assert "run-1" in notice
    # Есть lineage — состояние наполнялось, предупреждения нет.
    assert _state_not_adopted_notice(args, "case_x", object()) is None


def test_run_update_notice_quiet_without_completed_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Незавершённые прогоны не повод для ``state_not_adopted``."""
    from core.cli.commands.run import _state_not_adopted_notice

    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
        phases={"corpus": "ok", "evaluate": "failed"},
    )
    args = argparse.Namespace(runs_dir=str(runs_dir), package=str(package))

    assert _state_not_adopted_notice(args, "case_x", None) is None


# --------------------------------------------------------------------------- #
# Следствие adoption: режим продукта и сводка состояния
# --------------------------------------------------------------------------- #


def test_mode_for_after_adoption_is_incremental(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adoption делает продукт ``incremental``, а не ``new`` (смысл команды)."""
    from core.domain.enums import ProductMode

    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )
    assert _run(capsys, _adopt_argv(package, runs_dir, index_root))[0] == EXIT_OK

    store = SqliteStateStore(_state_db(tmp_path))
    try:
        row = store.get_product_state("p1")
        assert row is not None
        assert _mode_for(store, "p1", row) is ProductMode.INCREMENTAL
    finally:
        store.close()


def test_read_state_summary_marks_empty_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Отсутствующий ``state.db`` — ``exists=False``/``empty=True``, а не ошибка."""
    monkeypatch.chdir(tmp_path)

    summary = read_state_summary(tmp_path / "state" / "case_x" / "state.db", package_id="case_x")

    assert summary.exists is False
    assert summary.empty is True
    assert summary.products == ()


def test_plan_adoption_without_runs_says_why(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Нет прогонов — ``no_completed_run``: «нечего принимать» отличается от ошибки."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "runs").mkdir()

    plan = plan_adoption(tmp_path / "runs", "case_x")

    assert plan.run_id is None
    assert plan.reasons == ("no_completed_run",)
    assert plan.candidates == ()


# --------------------------------------------------------------------------- #
# L1b-2b: смещения куска при adoption
# --------------------------------------------------------------------------- #


def _span_pair(
    start: Any, end: Any, text: str
) -> tuple[int | None, int | None] | None:
    """Обёртка над одноимённой функцией модуля (импорт внутри: не расширяем шапку)."""
    from core.pipeline.state_adopt import _span_pair as pair

    return pair(start, end, text)


def test_span_pair_accepts_absent_and_exact_only() -> None:
    """Пара смещений: либо оба ``NULL``, либо точный диапазон; рваная/кривая — отказ.

    ``None`` здесь означает «не принимать строку», а не «смещений нет»: последнее —
    законный исход и выражается парой ``(None, None)``. Иначе `0`/`0` или половина пары
    доехали бы до ``chunk_meta`` и всплыли бы только на ``CorpusChunk`` (или на цитате).
    """
    text = "кусок\n\n"
    assert _span_pair(None, None, text) == (None, None)
    assert _span_pair(0, len(text), text) == (0, len(text))
    # Рваная пара, перевёрнутый диапазон, длина не сходится, мусор — всё отказ.
    assert _span_pair(0, None, text) is None
    assert _span_pair(None, 5, text) is None
    assert _span_pair(5, 3, text) is None
    assert _span_pair(0, 5, text) is None
    assert _span_pair("ноль", "пять", text) is None


def test_adopt_rejects_chunk_with_inconsistent_span(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Несогласованный диапазон не переносится в ``chunk_meta``, и причина видна.

    Проверка стоит там же, где сверка хеша (I19): адрес текста — его хеш, а позиция текста
    в документе — его смещения. Принимать строку с рваной парой значило бы отложить отказ
    до ``CorpusChunk``, где причина уже не видна.
    """
    package, runs_dir, index_root = _setup(tmp_path, monkeypatch)
    run_dir = _write_run(
        runs_dir,
        index_root,
        "run-1",
        package_id="case_x",
        products=[_product_spec("p1")],
    )
    # Прогон схемы 3.16: колонки есть, у второго куска диапазон не сходится с длиной текста.
    run_db = run_dir / "cohound.db"
    with sqlite3.connect(run_db) as conn:
        conn.execute("ALTER TABLE corpus_chunks ADD COLUMN char_start INTEGER")
        conn.execute("ALTER TABLE corpus_chunks ADD COLUMN char_end INTEGER")
        conn.execute(
            "UPDATE corpus_chunks SET char_start = 0, char_end = ? WHERE chunk_no = 0",
            (len(_chunk_text("p1", 0)),),
        )
        conn.execute("UPDATE corpus_chunks SET char_start = 0, char_end = 3 WHERE chunk_no = 1")

    code, out, _ = _run(capsys, _adopt_argv(package, runs_dir, index_root))

    assert code == EXIT_OK
    payload = json.loads(out)
    product = payload["plan"]["products"][0]
    assert "chunk_span_mismatch:1" in product["reasons"], product["reasons"]
    # Продукт-атомарность: несогласованная строка отклоняет продукт целиком, а не тихо
    # укорачивает его корпус (тот же исход, что у расхождения хеша, I19).
    assert payload["plan"]["accepted"] == []
    assert payload["plan"]["skipped"] == ["p1"]
    assert _counts(_state_db(tmp_path))["chunk_meta"] == 0
