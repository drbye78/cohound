"""Гейт версии схемы run DB (`11` §2.6): старая БД отвергается сообщением, а не читается молча.

Прогон — durable-артефакт, но **не** переносится между версиями схемы. Проверяется: отказ с
обоими номерами версий и путём файла, отсутствие трейсбека в CLI (сообщение + ``EXIT_USAGE``),
идемпотентность открытия свежей БД и полнота объявленной схемы — все колонки, которые до
3.14.0 досоздавались шимом ``_ensure_columns``, обязаны быть в ``DDL`` (иначе открытие
старой БД было бы единственным способом их получить, а такого пути больше нет).
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


def test_previous_schema_3_17_is_rejected_and_file_is_unchanged(tmp_path: Path) -> None:
    """F1 плана 20: файл 3.17.0 отвергается гейтом 3.18.0, не изменяя байты.

    Новая колонка ``runs.corpus_snapshot_json`` объявлена только в ``CREATE TABLE``;
    шимов ``ALTER TABLE`` нет. Поэтому «догнать» старый файл нечем — и проверка
    фиксирует это наблюдаемо: отказ видим, файл остаётся прежним (A3: тихий догон
    объявил бы чужую схему текущей).
    """
    db = _legacy_db(tmp_path / "cohound.db", "3.17.0")
    before = db.read_bytes()

    with pytest.raises(SchemaVersionMismatch) as excinfo:
        connect(db)

    message = str(excinfo.value)
    assert "3.17.0" in message
    assert "3.18.0" in message
    assert db.read_bytes() == before


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


#: Колонки, которые до 3.14.0 досоздавались при открытии (``_ensure_columns``). Теперь они
#: обязаны быть объявлены в ``DDL``; список остаётся здесь **данными**, поэтому удаление
#: колонки из ``DDL`` роняет проверку, а не проходит незамеченным.
FORMER_SHIM_COLUMNS: dict[str, tuple[str, ...]] = {
    "runs": (
        "settings_provenance_json",
        "crawl_provenance_json",
        "crawl_strategy",
        "crawl_rate_limit",
        "crawl_config_digest",
        "registry_spec_digest",
        "execution_contract_digest",
        "manual_artifacts_digest",
        "manual_artifacts_json",
        "partial_docs_json",
        "publishability_json",
        "ranking_epsilon",
        "deploy_profile",
        "edge_schema_version",
        "edge_capabilities_json",
        "run_scope",
        "product_plan_json",
        "embed_device",
        "previous_run_id",
        "change_class",
        "cycle_id",
        "cohort_digest",
        "seeds_digest",
    ),
    "cells": (
        "review_reason",
        "evidence_scope",
        "absence_rule",
        "measurement_origin",
        "carried_from_run_id",
        "cell_reuse_key",
    ),
    "llm_scenarios": ("endpoint_host", "max_tokens"),
    "cell_evidence": ("scope_acceptance", "partition_build_id", "doc_version_id"),
    "corpus_chunks": ("extractor_version",),
    "index_manifest": ("extractor_version", "content_digest", "partition_build_id"),
    "crawl_session_items": ("depth", "doc_hash"),
    "corpus_coverage": (
        "unreadable_origins_json",
        "partial_blocks_json",
        "reason",
        "resolved_manually",
    ),
    "corpus_docs": (
        "origin",
        "partial",
        "partial_reason",
        "page_count",
        "pages_converted",
        "extracted_chars",
        "http_status",
        "reuse_kind",
    ),
    "gate_decisions": ("subject_digest", "source", "external_ref", "verdict"),
}


def test_fresh_db_declares_every_former_shim_column() -> None:
    """Каждая колонка из бывших ``_ensure_columns`` объявлена в ``DDL`` свежей БД."""
    conn = connect(":memory:")
    try:
        for table, columns in FORMER_SHIM_COLUMNS.items():
            have = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
            missing = sorted(set(columns) - have)
            assert not missing, f"{table}: колонки не объявлены в DDL: {missing}"
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("table", "column", "declared_type", "not_null", "default"),
    [
        # Тип/ограничения перенесены из шима ровно как были: читатель опирается на дефолт,
        # а не на «допишем при открытии».
        ("cells", "measurement_origin", "TEXT", True, "'observed'"),
        ("corpus_docs", "origin", "TEXT", True, "'crawl'"),
        ("corpus_docs", "partial", "INTEGER", True, "0"),
        ("corpus_coverage", "unreadable_origins_json", "TEXT", True, "'[]'"),
        ("corpus_coverage", "partial_blocks_json", "TEXT", True, "'{}'"),
        ("corpus_coverage", "resolved_manually", "INTEGER", True, "0"),
        ("gate_decisions", "subject_digest", "TEXT", True, "''"),
        ("gate_decisions", "source", "TEXT", True, "'internal'"),
        ("gate_decisions", "verdict", "TEXT", True, "'approved'"),
        ("crawl_session_items", "depth", "INTEGER", False, None),
        ("runs", "ranking_epsilon", "REAL", False, None),
    ],
)
def test_shim_column_types_and_defaults_are_preserved(
    table: str,
    column: str,
    declared_type: str,
    not_null: bool,
    default: str | None,
) -> None:
    """Тип, ``NOT NULL`` и ``DEFAULT`` перенесённой колонки совпадают с прежним шимом."""
    conn = connect(":memory:")
    try:
        info = {
            r["name"]: r for r in conn.execute(f"PRAGMA table_info({table})")
        }
    finally:
        conn.close()
    assert column in info, f"{table}.{column} не объявлена"
    row = info[column]
    assert row["type"] == declared_type
    assert bool(row["notnull"]) is not_null
    assert row["dflt_value"] == default


def test_boundary_columns_exist_on_fresh_db_and_version_is_pinned() -> None:
    """Граница ``runs``/``corpus_docs`` (16 §3) объявлена в DDL; версия — пин.

    ``SCHEMA_VERSION`` пинуется намеренно: 3.14.0 — это версия, в которой шимы
    (``_ensure_columns``) свёрнуты в ``DDL``, то есть **отказ** всем файлам ≤ 3.13.0.
    Случайный bump здесь так же опасен, как случайное отсутствие bump: и то и другое
    меняет множество читаемых прогонов, поэтому пин — единственный способ увидеть это в
    ревью, а не в живом чтении.

    3.15.0 (F6, план ``17`` §4.2/§7.2) — **один** bump на четыре колонки замера и аренды:
    ``crawl_session_items.fetch_started_at/fetch_ms`` и ``extract_queue.claimed_at/
    heartbeat_at``. Шимов нет (``ALTER TABLE ... ADD COLUMN`` при открытии запрещены),
    поэтому прогоны ≤ 3.14.0 отвергаются гейтом, а не досоздаются молча.

    3.16.0 (L1b-2b, план ``18``) — **один** bump на две колонки провенанса врезки:
    ``corpus_chunks.char_start/char_end`` (смещения куска в тексте документа). Тот же
    запрет на шимы: прогоны ≤ 3.15.0 отвергаются гейтом версии, старые ``runs/*/
    cohound.db`` переносятся пересозданием, а не тихим догоном схемы.

    3.17.0 (P4 плана ``19``) — ``runs.crawl_policy_digest`` (31-е поле контракта: ось
    политики краула, которую diff обязан назвать) и ``runs.crawl_policy_json`` (её
    провенанс). Шимов нет: прогоны ≤ 3.16.0 отвергаются гейтом, а не досоздаются.

    3.18.0 (F1 плана ``20``) — ``runs.corpus_snapshot_json``: снимок корпуса прогона
    (``corpus_snapshot_id``, снятия прогона, ``corpus_as_of``). Это **провенанс**, а не
    объявленный вход: он известен только после краула и в ``run_id`` не входит. Шимов нет:
    прогоны ≤ 3.17.0 отвергаются гейтом, а не досоздаются.
    """
    assert SCHEMA_VERSION == "3.18.0"

    conn = connect(":memory:")
    try:
        runs_cols = {r["name"] for r in conn.execute("PRAGMA table_info(runs)")}
        assert {
            "run_scope",
            "product_plan_json",
            "embed_device",
            "crawl_policy_digest",
            "crawl_policy_json",
            "corpus_snapshot_json",
        } <= runs_cols

        docs_cols = {r["name"] for r in conn.execute("PRAGMA table_info(corpus_docs)")}
        assert {"http_status", "reuse_kind"} <= docs_cols

        chunk_cols = {r["name"] for r in conn.execute("PRAGMA table_info(corpus_chunks)")}
        assert {"char_start", "char_end"} <= chunk_cols

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


def test_3_13_0_db_is_rejected_and_left_untouched(tmp_path: Path) -> None:
    """Файл прежней версии — **отказ**, а не тихий ``ALTER TABLE`` при открытии.

    Прежняя проверка («файл той же версии без новых колонок принимается и мигрирует»)
    держалась на шиме ``_ensure_columns``. Шим удалён вместе с ``SCHEMA_VERSION`` 3.14.0,
    поэтому эквивалент переехал на два носителя: свежий ``DDL`` объявляет колонки
    (:func:`test_fresh_db_declares_every_former_shim_column`), а чужой файл отвергается
    видимо и не меняется — иначе он объявил бы себя схемой 3.14.0, не будучи ею.
    """
    db = _legacy_db(tmp_path / "cohound.db", "3.13.0")
    before = db.read_bytes()

    with pytest.raises(SchemaVersionMismatch) as excinfo:
        connect(db)

    message = str(excinfo.value)
    assert "3.13.0" in message
    assert SCHEMA_VERSION in message
    assert db.read_bytes() == before


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
