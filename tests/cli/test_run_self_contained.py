"""Самодостаточность каталога прогона: снимок входов в БД (``run_inputs``, P1).

``cmd_run`` пишет в БД прогона снимок плановых входов (каталог/веса/когорта) и
производный экспорт ``runs/{run_id}/plan_inputs.json`` **и** ``run.yaml``. Последующие
команды (``sensitivity``/``report``/``golden``) восстанавливают каталог из БД **без**
исходного пакета и **без** side-файла. Явные ``--run-yaml``/``--package`` сохраняют
приоритет; при отсутствии записи в БД остаётся пустой каталог и заметка-ограничение.

Фикстуры категорийно нейтральны (§1.3): ``tests/fixtures/e2e`` + ``tests/config/fixtures``.
Сеть не используется: прогон доходит лишь до человеческого гейта ``g1``.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import (
    EXIT_BLOCKED,
    EXIT_OK,
    PLAN_INPUTS_SNAPSHOT_VERSION,
    main,
)
from tests.helpers.output import json_out
from tests.helpers.packages import run_argv

#: Корень e2e-фикстур (``tests/fixtures/e2e``) — валидный пакет для ``run``.
E2E_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"
#: Фикстуры пакета (``tests/config/fixtures``) — второй (отличный) каталог для приоритета.
CONFIG_FIXTURES = Path(__file__).resolve().parents[1] / "config" / "fixtures"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _config_package(tmp_path: Path) -> Path:
    """Копия ``tests/config/fixtures`` — отличный от e2e каталог (param_a)."""
    package = tmp_path / "config_package"
    package.mkdir()
    for item in CONFIG_FIXTURES.iterdir():
        if item.is_file():
            shutil.copy(item, package / item.name)
    return package


def _notes(payload: dict[str, Any]) -> str:
    return " ".join(payload.get("notes") or [])


# ---------------------------------------------------------------------------
# cmd_run: запись снимка
# ---------------------------------------------------------------------------


def test_run_writes_immutable_run_yaml_and_plan_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-run"

    code = main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir))
    assert code == EXIT_BLOCKED  # g1 — человеческое решение (I6/I7)
    capsys.readouterr()

    run_dir = runs_dir / run_id
    # run.yaml — байт-в-байт копия входа (иммутабельный вход, 03 §A.2).
    assert (run_dir / "run.yaml").read_bytes() == (E2E_FIXTURES / "run.yaml").read_bytes()

    snapshot_path = run_dir / "plan_inputs.json"
    assert snapshot_path.is_file()
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))

    assert snapshot["snapshot_version"] == PLAN_INPUTS_SNAPSHOT_VERSION
    assert snapshot["run_id"] == run_id
    assert snapshot["package_id"] == "e2e"
    assert snapshot["package_version"] == "1.0.0"
    assert snapshot["case_id"] == "e2e"
    # Каталог, веса и когорта присутствуют (достаточно для пересборки без пакета).
    assert {row["param_id"] for row in snapshot["params"]} == {"feature.alpha"}
    assert snapshot["renormalized_weights"] == {"m1": 1.0}
    assert snapshot["cohort"]["case_id"] == "e2e"
    assert {entry["product_id"] for entry in snapshot["cohort"]["entries"]} == {"p1"}
    # Детерминированный JSON: ключи отсортированы.
    assert list(snapshot) == sorted(snapshot)


def test_run_dry_run_writes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-dry"

    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir, dry_run=True)) == EXIT_OK
    assert json_out(capsys)["dry_run"] is True
    assert not (runs_dir / run_id).exists()
    assert not (runs_dir / run_id / "run.yaml").exists()
    assert not (runs_dir / run_id / "plan_inputs.json").exists()


def test_second_run_does_not_overwrite_differing_run_yaml(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-immutable"

    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    target = runs_dir / run_id / "run.yaml"
    tampered = b"case_id: tampered\n"
    target.write_bytes(tampered)

    # Повторный прогон (resume) не должен мутировать завершённый вход.
    code = main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir))
    assert code == EXIT_BLOCKED
    assert target.read_bytes() == tampered


# ---------------------------------------------------------------------------
# Пост-хок команды: восстановление из снимка
# ---------------------------------------------------------------------------


def test_sensitivity_restores_from_db(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Источник каталога — БД прогона (``run_inputs``); side-файл не обязателен."""
    from core.cli.wiring import run_db_path

    runs_dir = tmp_path / "runs"
    run_id = "sc-sens"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    # Убираем производный экспорт: БД остаётся единственным источником (регрессия P1).
    (runs_dir / run_id / "plan_inputs.json").unlink()
    assert run_db_path(runs_dir, run_id).exists()

    # Ни --run-yaml, ни --package: единственный источник — БД прогона.
    code = main(
        ["sensitivity", "--runs-dir", str(runs_dir), "--run-id", run_id, "--json"]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["run_id"] == run_id
    notes = _notes(payload)
    assert "БД прогона" in notes
    assert "отсутствуют в БД прогона" not in notes


def test_sensitivity_restores_from_db_via_db_parent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--db runs/{run_id}/cohound.db`` берёт каталог из БД того же прогона."""
    runs_dir = tmp_path / "runs"
    run_id = "sc-db-parent"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    code = main(
        [
            "sensitivity",
            "--db",
            str(runs_dir / run_id / "cohound.db"),
            "--run-id",
            run_id,
            "--json",
        ]
    )
    assert code == EXIT_OK
    assert "БД прогона" in _notes(json_out(capsys))


def test_report_restores_from_db(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-report"
    out = tmp_path / "out"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    code = main(
        [
            "report",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            run_id,
            "--out",
            str(out),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    notes = _notes(payload)
    assert "БД прогона" in notes
    assert "отсутствуют в БД прогона" not in notes
    assert Path(payload["artifacts"]["manifest"]).exists()


def test_golden_restores_from_db(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-golden"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    # Код возврата зависит от вердиктов (ячеек нет → приёмка не пройдена); проверяем
    # именно источник каталога: БД использована, заметки-ограничения нет.
    main(
        [
            "golden",
            "--golden",
            str(E2E_FIXTURES / "golden.csv"),
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            run_id,
            "--json",
        ]
    )
    payload = json_out(capsys)
    notes = _notes(payload)
    assert "БД прогона" in notes
    assert "отсутствуют в БД прогона" not in notes


def test_run_persists_plan_inputs_to_db(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``cmd_run`` пишет снимок плановых входов в ``run_inputs`` (P1): payload + sha256."""
    from core.cli.wiring import run_db_path
    from core.store.sqlite_store import SqliteStore

    runs_dir = tmp_path / "runs"
    run_id = "sc-db-row"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        payload = store.read_plan_inputs(run_id)
        row = store.conn.execute(
            "SELECT snapshot_version, payload_sha256 FROM run_inputs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    finally:
        store.close()

    assert payload is not None
    assert payload["run_id"] == run_id
    assert payload["package_id"] == "e2e"
    assert {row_["param_id"] for row_ in payload["params"]} == {"feature.alpha"}
    assert payload["renormalized_weights"] == {"m1": 1.0}
    assert row is not None
    assert row["snapshot_version"] == PLAN_INPUTS_SNAPSHOT_VERSION
    assert row["payload_sha256"]  # детерминированный дайджест записан


def test_explicit_inputs_take_priority_over_db(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs_dir = tmp_path / "runs"
    run_id = "sc-explicit"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    package = _config_package(tmp_path)  # отличный каталог (param_a), не снимок e2e
    code = main(
        [
            "sensitivity",
            "--runs-dir",
            str(runs_dir),
            "--run-id",
            run_id,
            "--run-yaml",
            str(package / "run.yaml"),
            "--package",
            str(package),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    # Явный источник: ни restore-заметки, ни ограничения (БД/снимок не читались).
    assert payload["notes"] == []


def test_absent_run_inputs_keeps_limit_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без записи ``run_inputs`` и без явных входов — пустой каталог + заметка (не выдумываем)."""
    from core.cli.wiring import run_db_path
    from core.store.sqlite_store import SqliteStore

    runs_dir = tmp_path / "runs"
    run_id = "sc-gap"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    # Эмулируем прогон без персиста входов: удаляем запись из БД.
    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        with store.conn:
            store.conn.execute("DELETE FROM run_inputs WHERE run_id = ?", (run_id,))
    finally:
        store.close()

    code = main(
        ["sensitivity", "--runs-dir", str(runs_dir), "--run-id", run_id, "--json"]
    )
    assert code == EXIT_OK
    assert "отсутствуют в БД прогона" in _notes(json_out(capsys))


# ---------------------------------------------------------------------------
# L1b: ``--reset`` очищает durable-краул (сеанс краула)
# ---------------------------------------------------------------------------


def test_reset_clears_crawl_session(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``hound run --reset`` очищает сеанс краула прогона ``f"crawl:{run_id}"`` (L1b)."""
    from core.cli.wiring import run_db_path
    from core.domain import (
        CrawlItemStatus,
        CrawlSession,
        CrawlSessionItem,
        CrawlSessionStatus,
    )
    from core.pipeline.crawl_session import default_session_id
    from core.store.sqlite_store import SqliteStore

    runs_dir = tmp_path / "runs"
    run_id = "sc-reset"
    session_id = default_session_id(run_id)
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()
    db_path = run_db_path(runs_dir, run_id)

    store = SqliteStore(db_path)
    try:
        store.upsert_crawl_session(
            CrawlSession(
                session_id=session_id,
                run_id=run_id,
                profile="local",
                status=CrawlSessionStatus.CREATED,
                created_at="2026-01-01T00:00:00+00:00",
            )
        )
        store.upsert_crawl_session_item(
            CrawlSessionItem(
                session_id=session_id,
                product_id="p1",
                url="https://example.com/a",
                status=CrawlItemStatus.FETCHED,
            )
        )
        assert store.get_crawl_session(session_id) is not None
        assert len(list(store.iter_crawl_session_items(session_id))) == 1
    finally:
        store.close()

    # Повторный прогон с --reset очищает durable-состояние краула (сеанс + элементы).
    code = main(
        run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir, extra=["--reset"])
    )
    assert code == EXIT_BLOCKED
    capsys.readouterr()
    store = SqliteStore(db_path)
    try:
        assert store.get_crawl_session(session_id) is None
        assert list(store.iter_crawl_session_items(session_id)) == []
    finally:
        store.close()
