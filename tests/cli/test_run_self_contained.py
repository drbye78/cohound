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
from collections.abc import Mapping
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

# ``state/{package_id}/`` — эксклюзивный ресурс пакета (файловый лок, 09 §4.4):
# у каждого теста свой каталог состояния, иначе xdist-воркеры упираются в лок друг
# друга на общем ``<repo>/state/package/``.
pytestmark = pytest.mark.usefixtures("isolated_state_dir")

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


def test_second_run_reports_run_inputs_conflict_without_failing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """O11 (09 §3.3): расхождение ``payload_sha256`` — видимая строка, а не падение.

    Снимок входов в БД иммутабелен: повторная запись с другим дайджестом не перезаписывает
    строку, а попадает в ``instrumentation_errors`` прогона (``snapshot: run_inputs_conflict``).
    """
    from core.cli.wiring import run_db_path
    from core.store.sqlite_store import SqliteStore

    runs_dir = tmp_path / "runs"
    run_id = "sc-conflict"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    # Эмулируем снимок другого прогона под тем же run_id: дайджест не совпадёт.
    foreign_sha = "f" * 64
    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        with store.conn:
            store.conn.execute(
                "UPDATE run_inputs SET payload_sha256 = ? WHERE run_id = ?",
                (foreign_sha, run_id),
            )
        stored_payload = store.read_plan_inputs(run_id)
    finally:
        store.close()

    code = main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir))
    payload = json_out(capsys)

    # Прогон не падает: конфликт — запись в instrumentation_errors, а не исключение.
    assert code == EXIT_BLOCKED
    errors = payload.get("instrumentation_errors") or []
    conflict = [e for e in errors if "run_inputs_conflict" in e]
    assert conflict, errors
    line = conflict[0]
    assert line.startswith("snapshot: run_inputs_conflict:")
    assert run_id in line
    assert foreign_sha in line  # сохранённый дайджест назван
    assert "нужен новый run_id" in line

    # Сохранённая строка не перезаписана: дайджест и payload остались прежними.
    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        row = store.conn.execute(
            "SELECT payload_sha256 FROM run_inputs WHERE run_id = ?", (run_id,)
        ).fetchone()
        after = store.read_plan_inputs(run_id)
    finally:
        store.close()
    assert row is not None
    assert row["payload_sha256"] == foreign_sha
    assert after == stored_payload


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


# ---------------------------------------------------------------------------
# Снимок v2: цепочка циклов (09 §14.2) и чтение обеих версий
# ---------------------------------------------------------------------------

#: Поля, добавленные снимком v2: у снимка v1 их нет вовсе.
V2_CHAIN_KEYS = (
    "cycle_id",
    "cohort_digest",
    "seeds_digest",
    "previous_run_id",
    "product_plan",
    "state_schema_version",
)


def _v1_view(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Снимок в формате v1: только поля v1 (так писал прежний писатель)."""
    v1 = {key: value for key, value in payload.items() if key not in V2_CHAIN_KEYS}
    v1["snapshot_version"] = 1
    return v1


def _write_stored_snapshot(
    runs_dir: Path, run_id: str, payload: Mapping[str, Any]
) -> str:
    """Кладёт ``payload`` в ``run_inputs`` как сохранённый снимок (эмуляция прежней записи).

    Пишется ровно то, что положил бы писатель той версии: канонический JSON + дайджест
    (:func:`core.cli.context._plan_inputs_sha256`). Возвращает записанный ``payload_sha256``.
    """
    from core.cli.main import _plan_inputs_sha256
    from core.cli.wiring import run_db_path
    from core.store.sqlite_store import SqliteStore

    sha = _plan_inputs_sha256(payload)
    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        with store.conn:
            store.conn.execute(
                "UPDATE run_inputs SET payload_json = ?, payload_sha256 = ?,"
                " snapshot_version = ? WHERE run_id = ?",
                (
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    sha,
                    payload["snapshot_version"],
                    run_id,
                ),
            )
    finally:
        store.close()
    return sha


def test_plan_inputs_snapshot_v2_carries_cycle_chain(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Снимок v2 восстанавливает **цепочку циклов**, а не только каталог/веса/когорту.

    Состав v2 (09 §14.2): ``cycle_id``/``cohort_digest``/``seeds_digest``/
    ``previous_run_id``/``product_plan``/``state_schema_version``. Значения — та же
    идентичность, что уже вошла в ``run_id``, а не второй расчёт; ``plan_inputs.json``
    держит ту же форму, что строка в БД.
    """
    from core.cli.wiring import run_db_path
    from core.store.sqlite_store import SqliteStore
    from core.store.state_sqlite import STATE_SCHEMA_VERSION

    runs_dir = tmp_path / "runs"
    run_id = "sc-v2-chain"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        payload = store.read_plan_inputs(run_id)
    finally:
        store.close()

    assert payload is not None
    assert payload["snapshot_version"] == PLAN_INPUTS_SNAPSHOT_VERSION == 2
    # Цикл — эффективный (``snapshot_date`` фикстуры), тот же материал, что у ``run_id``.
    assert payload["cycle_id"] == "2026-06-01"
    # Дайджесты — 24 hex-символа (``short_hash`` — то же, что в материале ``run_id``).
    assert isinstance(payload["cohort_digest"], str) and len(payload["cohort_digest"]) == 24
    assert isinstance(payload["seeds_digest"], str) and len(payload["seeds_digest"]) == 24
    # Первый прогон: предшественника нет — ``None``, а не выдуманная строка.
    assert payload["previous_run_id"] is None
    assert payload["state_schema_version"] == STATE_SCHEMA_VERSION
    # План продуктов — mode/scope/reasons по продуктам (то же, что уходит в runs).
    plan = payload["product_plan"]
    assert [item["product_id"] for item in plan["products"]] == ["p1"]
    assert plan["products"][0]["scope"] == "full"
    assert isinstance(plan["products"][0]["reasons"], list)
    assert plan["blocked"] == []

    # Производный экспорт — та же форма, что в БД (второго формата нет).
    exported = json.loads((runs_dir / run_id / "plan_inputs.json").read_text(encoding="utf-8"))
    assert exported == payload


def test_plan_inputs_reader_reads_v1_and_v2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Читатель принимает **обе** версии: v1 — по своим полям, v2 — по новым.

    У снимка v1 новые поля именно ``None`` («не записано»), а не ``0``/``""``: значения
    не выдумываются. Версия вне списка поддержанных — отказ (значения не подставляются).
    """
    from core.cli.main import _plan_inputs_from_payload
    from core.cli.wiring import run_db_path
    from core.store.sqlite_store import SqliteStore

    runs_dir = tmp_path / "runs"
    run_id = "sc-reader-versions"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        payload = store.read_plan_inputs(run_id)
    finally:
        store.close()
    assert payload is not None

    # v2 — по новым полям.
    v2 = _plan_inputs_from_payload(payload, run_id)
    assert v2 is not None
    assert v2.snapshot_version == 2
    assert v2.cycle_id == payload["cycle_id"]
    assert v2.cohort_digest == payload["cohort_digest"]
    assert v2.seeds_digest == payload["seeds_digest"]
    assert v2.previous_run_id is None
    assert v2.product_plan == payload["product_plan"]
    assert v2.state_schema_version == payload["state_schema_version"]
    assert set(v2.catalog) == {"feature.alpha"}
    assert v2.module_weights == {"m1": 1.0}

    # v1 — по своим полям; новых полей у неё нет, и они остаются ``None``.
    v1 = _plan_inputs_from_payload(_v1_view(payload), run_id)
    assert v1 is not None
    assert v1.snapshot_version == 1
    assert set(v1.catalog) == {"feature.alpha"}
    assert v1.module_weights == {"m1": 1.0}
    assert v1.cohort.case_id == "e2e"
    assert v1.cycle_id is None
    assert v1.cohort_digest is None
    assert v1.seeds_digest is None
    assert v1.previous_run_id is None
    assert v1.product_plan is None
    assert v1.state_schema_version is None

    # v1 читается **по своим** полям: посторонние ключи v2 у неё не подхватываются.
    contaminated = {**payload, "snapshot_version": 1}
    as_v1 = _plan_inputs_from_payload(contaminated, run_id)
    assert as_v1 is not None
    assert as_v1.cycle_id is None
    assert as_v1.product_plan is None

    # Версия вне списка поддержанных — снимок не подставляется (не «ближайшая»).
    assert _plan_inputs_from_payload({**payload, "snapshot_version": 99}, run_id) is None
    assert _plan_inputs_from_payload({**payload, "snapshot_version": 0}, run_id) is None
    # Чужой run_id — тоже отказ.
    assert _plan_inputs_from_payload(payload, "other-run") is None


def test_resume_preserves_v1_snapshot_without_rewrite(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """O11: снимок прежней версии **не перезаписывается** — факт называется, прогон идёт.

    Подъём формата снимка (1 → 2) меняет ``payload_sha256``, поэтому «дописать поля» в
    уже записанный снимок нельзя: это была бы перезапись идентичности прогона. Resume
    прогона со снимком v1 обязан (а) продолжиться, (б) назвать факт видимой строкой,
    (в) оставить строку БД и ``plan_inputs.json`` ровно такими, какими их записали.
    """
    from core.cli.wiring import run_db_path
    from core.store.sqlite_store import SqliteStore

    runs_dir = tmp_path / "runs"
    run_id = "sc-v1-preserve"
    assert main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir)) == EXIT_BLOCKED
    capsys.readouterr()

    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        written = store.read_plan_inputs(run_id)
    finally:
        store.close()
    assert written is not None

    # Эмулируем снимок, записанный прежней версией формата: v1-состав + v1-дайджест.
    v1 = _v1_view(written)
    v1_sha = _write_stored_snapshot(runs_dir, run_id, v1)
    snapshot_path = runs_dir / run_id / "plan_inputs.json"
    snapshot_path.write_text(
        json.dumps(v1, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    # Resume: прогон продолжается (не падает и не требует нового run_id).
    code = main(run_argv(E2E_FIXTURES, run_id=run_id, runs_dir=runs_dir))
    payload = json_out(capsys)
    assert code == EXIT_BLOCKED
    errors = payload.get("instrumentation_errors") or []

    preserved = [line for line in errors if "run_inputs_v1_preserved" in line]
    assert preserved, errors
    line = preserved[0]
    assert line.startswith("snapshot: run_inputs_v1_preserved:")
    assert run_id in line
    assert "сохранён снимок формата 1, текущий 2" in line
    assert "не перезаписан (иммутабельность, O11)" in line
    # Это не «конфликт, который разрешили»: перезаписи не было вовсе.
    assert not [item for item in errors if "run_inputs_conflict" in item]

    # Снимок в БД остался ровно тем, что было записано первым (дайджест, версия, payload).
    store = SqliteStore(run_db_path(runs_dir, run_id))
    try:
        row = store.conn.execute(
            "SELECT payload_sha256, snapshot_version FROM run_inputs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        after = store.read_plan_inputs(run_id)
    finally:
        store.close()
    assert row is not None
    assert row["payload_sha256"] == v1_sha
    assert row["snapshot_version"] == 1
    assert after == v1

    # Производный экспорт — та же форма, что в БД (v1), а не предъявленный v2.
    exported = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert exported == v1
    assert exported["snapshot_version"] == 1


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
        # Ш5 исполняется (G1–G3 больше не блокируют), поэтому в сеансе уже могут быть
        # элементы прошлого прогона: проверяется наличие нашего, а не общее число.
        assert any(
            item.url == "https://example.com/a"
            for item in store.iter_crawl_session_items(session_id)
        )
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
        # Сброшенный сеанс не переживает reset: ручной элемент исчез (сеанс мог быть
        # создан заново текущим прогоном, но уже без него).
        assert not any(
            item.url == "https://example.com/a"
            for item in store.iter_crawl_session_items(session_id)
        )
    finally:
        store.close()
