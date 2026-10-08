"""Тесты CLI — composition root Z0 (W5/T5.3, ``03`` §E).

Покрывают ``resolve`` (hit/miss, ``--json``), ``plan`` (валидный ``--dry-run`` и
блокирующий линт с кодом правила), минимальный ``run`` (dry-run, resume, остановка на
гейте), ``--help`` и шов профилей ``local``/``split``. Используют ``capsys``/``tmp_path``.

Данные категорийно-нейтральны (§1.3): ``p1``/``p2``, ``case_x``, ``param_a``. Валидный
пакет — копия ``tests/config/fixtures/`` с добавленным ``scope_attestation.yaml`` (иначе
сырые фикстуры справедливо блокируются правилом I2b).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from core.cli.main import (
    EXIT_BLOCKED,
    EXIT_NOT_FOUND,
    EXIT_OK,
    EXIT_USAGE,
    main,
)
from core.cli.wiring import (
    LOCAL_PROFILE,
    SPLIT_PROFILE,
    WiringError,
    build_adapters,
    build_store,
    run_db_path,
)
from core.pipeline import Ports
from core.store.sqlite_store import SqliteStore

CONFIG_FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML_NAME = "run.yaml"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _valid_package(tmp_path: Path) -> Path:
    """Копия ``tests/config/fixtures/`` + ``scope_attestation.yaml`` → линт-чистый пакет."""
    package = tmp_path / "package"
    package.mkdir()
    for item in CONFIG_FIXTURES.iterdir():
        if item.is_file():
            shutil.copy(item, package / item.name)
    (package / "scope_attestation.yaml").write_text(
        'procedure: "ручная аттестация полноты источника (тест)"\n', encoding="utf-8"
    )
    return package


def _seed_run_db(
    db_path: Path,
    *,
    chunk_id: str = "c1",
    url: str = "https://example.test/doc#1",
) -> None:
    """Создаёт БД прогона с одним документом/чанком (для резолва I10/I15)."""
    with SqliteStore(db_path) as store:
        store.conn.execute(
            "INSERT INTO corpus_docs(doc_id, product_id, url) VALUES (?, ?, ?)",
            ("d1", "p1", url),
        )
        store.conn.execute(
            "INSERT INTO corpus_chunks"
            " (chunk_id, doc_id, product_id, url, chunk_no, text, content_hash, build_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (chunk_id, "d1", "p1", url, 0, "text", "h1", "b1"),
        )
        store.conn.commit()


def _json_out(capsys: pytest.CaptureFixture[str]) -> dict:
    """Читает единственный JSON-объект из stdout."""
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# --help / диспетчер
# ---------------------------------------------------------------------------


def test_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--help"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "resolve" in out
    assert "plan" in out
    assert "run" in out


def test_no_command_is_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == EXIT_USAGE
    assert "usage" in capsys.readouterr().out


def test_unknown_command_is_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["nope"]) == EXIT_USAGE
    assert "usage" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# resolve
# ---------------------------------------------------------------------------


def test_resolve_hit_exit_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "cohound.db"
    _seed_run_db(db)

    assert main(["resolve", "c1", "--db", str(db)]) == EXIT_OK
    assert "https://example.test/doc#1" in capsys.readouterr().out


def test_resolve_hit_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "cohound.db"
    _seed_run_db(db)

    assert main(["resolve", "c1", "--db", str(db), "--json"]) == EXIT_OK
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert payload["found"] is True
    assert payload["source_url"] == "https://example.test/doc#1"
    assert payload["content_hash"] == "h1"
    assert payload["build_id"] == "b1"


def test_resolve_miss_exit_nonzero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "cohound.db"
    _seed_run_db(db)

    assert main(["resolve", "nope", "--db", str(db)]) == EXIT_NOT_FOUND
    captured = capsys.readouterr()
    assert "I10/I15" in captured.err
    assert "nope" in captured.err


def test_resolve_miss_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db = tmp_path / "cohound.db"
    _seed_run_db(db)

    assert main(["resolve", "nope", "--db", str(db), "--json"]) == EXIT_NOT_FOUND
    payload = _json_out(capsys)
    assert payload["ok"] is False
    assert payload["found"] is False
    assert payload["error"] == "I10/I15"


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def test_plan_dry_run_valid_fixtures_returns_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _valid_package(tmp_path)

    code = main(
        [
            "plan",
            "--run-yaml",
            str(package / RUN_YAML_NAME),
            "--package",
            str(package),
            "--dry-run",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert payload["dry_run"] is True
    assert payload["anchor"] == "p1"
    assert payload["products"] == ["p1", "p2"]
    assert payload["modules"] == ["mod_a"]
    assert payload["params"] == ["param_a"]
    assert payload["run_id"]
    assert sum(payload["renormalized_weights"].values()) == pytest.approx(1.0)


def test_plan_human_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _valid_package(tmp_path)

    code = main(
        [
            "plan",
            "--run-yaml",
            str(package / RUN_YAML_NAME),
            "--package",
            str(package),
            "--dry-run",
        ]
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "anchor: p1" in out
    assert "dry-run: yes" in out


def test_plan_blocking_lint_prints_rule_code(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Сырые фикстуры: distribution_absence без процедуры аттестации → блок I2b.
    code = main(
        [
            "plan",
            "--run-yaml",
            str(CONFIG_FIXTURES / RUN_YAML_NAME),
            "--package",
            str(CONFIG_FIXTURES),
            "--dry-run",
        ]
    )
    assert code == EXIT_USAGE
    captured = capsys.readouterr()
    assert "I2b" in captured.err


def test_plan_blocking_lint_json(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        [
            "plan",
            "--run-yaml",
            str(CONFIG_FIXTURES / RUN_YAML_NAME),
            "--package",
            str(CONFIG_FIXTURES),
            "--dry-run",
            "--json",
        ]
    )
    assert code == EXIT_USAGE
    payload = _json_out(capsys)
    assert payload["ok"] is False
    assert payload["error"] == "lint"
    assert any("I2b" in message for message in payload["blocking"])


# ---------------------------------------------------------------------------
# run (минимальный)
# ---------------------------------------------------------------------------


def test_run_dry_run_does_not_write(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _valid_package(tmp_path)
    db = tmp_path / "unused.db"

    code = main(
        [
            "run",
            "--run-yaml",
            str(package / RUN_YAML_NAME),
            "--package",
            str(package),
            "--db",
            str(db),
            "--dry-run",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert payload["dry_run"] is True
    assert "normalize" in payload["phases"]
    assert not db.exists()


def test_run_resume_skips_completed_phases(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    common = [
        "run",
        "--run-yaml",
        str(package / RUN_YAML_NAME),
        "--package",
        str(package),
        "--db",
        str(db),
        "--run-id",
        "fixed-1",
        "--until",
        "plan",
        "--json",
    ]

    assert main(common) == EXIT_OK
    first = _json_out(capsys)
    assert [item["phase"] for item in first["results"]] == ["normalize", "lint", "plan"]
    assert first["skipped"] == []

    assert main(common) == EXIT_OK
    second = _json_out(capsys)
    assert second["skipped"] == ["normalize", "lint", "plan"]
    assert second["results"] == []


def test_run_stops_at_gate_blocked(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    code = main(
        [
            "run",
            "--run-yaml",
            str(package / RUN_YAML_NAME),
            "--package",
            str(package),
            "--db",
            str(db),
            "--run-id",
            "fixed-2",
            "--json",
        ]
    )
    assert code == EXIT_BLOCKED
    payload = _json_out(capsys)
    assert payload["status"] == "blocked"
    assert payload["stopped_at"] == "g1"


def test_run_split_profile_unavailable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    code = main(
        [
            "run",
            "--run-yaml",
            str(package / RUN_YAML_NAME),
            "--package",
            str(package),
            "--db",
            str(db),
            "--until",
            "plan",
            "--profile",
            SPLIT_PROFILE,
        ]
    )
    assert code == EXIT_USAGE
    assert "split" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# wiring (composition root)
# ---------------------------------------------------------------------------


def test_build_adapters_local_returns_empty_ports() -> None:
    ports = build_adapters(profile=LOCAL_PROFILE)
    assert isinstance(ports, Ports)
    assert ports.evaluator is None
    assert ports.crawler is None


def test_build_adapters_split_raises_clean_seam() -> None:
    with pytest.raises(WiringError):
        build_adapters(profile=SPLIT_PROFILE)


def test_build_store_unknown_profile_raises() -> None:
    with pytest.raises(WiringError):
        build_store(":memory:", profile="nope")


def test_run_db_path_follows_layout() -> None:
    assert run_db_path("runs", "run-1") == Path("runs/run-1/cohound.db")


def test_facade_exports_main() -> None:
    import core.cli as cli

    assert callable(cli.main)
    assert cli.LOCAL_PROFILE == LOCAL_PROFILE
    assert "main" in cli.__all__
