"""Тесты прибора ``scripts/measure_golden_ceiling.py``.

Семантика достижимости проверяется в ``tests/pipeline/test_absence_reach.py`` — прибор её
только импортирует, чтобы не было двух наборов на одно правило. Здесь остаётся то, что
принадлежит **прибору**:

* страта ``❌`` по маркеру отрицания (это разрез прибора, а не отчёт);
* чтение артефактов прогона по путям: носители из ``corpus_coverage`` и отказ на чужой
  ``schema_version`` без явного флага (вердикты чужой схемы — исторический факт);
* пересчёт носителей **текущими правилами** по первичным артефактам (``--state-db``):
  записанное покрытие отвечает на вопрос прежней формулы, поэтому прибор не имеет права
  принимать его за ответ нового правила.

Живой корпус не нужен: ``tmp_path`` + ``SqliteStore``.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import measure_golden_ceiling as measure  # noqa: E402

from core.domain.enums import (  # noqa: E402
    RankingPolicy,
    RunMode,
    SourceKind,
    Status,
)
from core.domain.models import (  # noqa: E402
    CorpusCoverage,
    CorpusRootCoverage,
    Focus,
    GoldenRow,
    RunConfig,
)
from core.pipeline.absence_reach import CellVerdict  # noqa: E402
from core.ports.state import CaptureRow  # noqa: E402
from core.store.sqlite import SCHEMA_VERSION  # noqa: E402
from core.store.sqlite_store import SqliteStore  # noqa: E402
from core.store.state_store import SqliteStateStore  # noqa: E402


def _golden(product: str, param: str, status: Status) -> GoldenRow:
    return GoldenRow(
        product_id=product,
        param_id=param,
        expected_status=status,
        expected_evidence_kind=[SourceKind.DOCUMENTATION],
        case_class="class_ok",
    )


def _root(url: str, *, complete: bool) -> CorpusRootCoverage:
    return CorpusRootCoverage(
        root_url=url,
        capture_id="cap" if complete else None,
        complete=complete,
        observed_how="live" if complete else None,
        observed_at="2026-11-07T00:00:00+00:00" if complete else None,
        policy_digest="d",
        policy_matched=True,
    )


# ---------------------------------------------------------------------------
# Страта ❌ по маркеру отрицания
# ---------------------------------------------------------------------------


def test_absent_stratum_splits_by_denial_marker() -> None:
    """Половины страты ``❌`` называют свой эталонный статус, а не только размер."""
    golden = [
        _golden("prod", "p", Status.ABSENT),
        _golden("prod", "q", Status.OK),
    ]
    marker = re.compile(r"не поддержива|not supported", re.IGNORECASE)
    cells = {
        ("prod", "p"): CellVerdict(
            status=Status.ABSENT, quotes=("функция не поддерживается",)
        ),
        ("prod", "q"): CellVerdict(
            status=Status.ABSENT, quotes=("доступна через расширение",)
        ),
    }
    stratum = measure.absent_stratum(golden, cells, marker)
    assert stratum["cells"] == 2
    assert stratum["survive"] == 1
    assert stratum["demote"] == 1
    assert stratum["survive_by_golden"] == {Status.ABSENT.value: 1}
    assert stratum["demote_by_golden"] == {Status.OK.value: 1}


def test_absent_stratum_counts_cell_once_with_several_quotes() -> None:
    """Ячейка с двумя цитатами — одна ячейка: иначе «переживает» удваивалось бы."""
    golden = [_golden("prod", "p", Status.ABSENT)]
    marker = re.compile(r"не поддержива", re.IGNORECASE)
    cells = {
        ("prod", "p"): CellVerdict(
            status=Status.ABSENT,
            quotes=("доступна через расширение", "не поддерживается в этой версии"),
        )
    }
    stratum = measure.absent_stratum(golden, cells, marker)
    assert stratum["cells"] == 1
    assert stratum["survive"] == 1
    assert stratum["demote"] == 0


# ---------------------------------------------------------------------------
# Чтение артефактов прогона
# ---------------------------------------------------------------------------


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor="p1",
        cohort="cohort_case_x",
        products=["p1"],
        package="case_x",
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def test_read_carriers_reads_roots_and_policy_reason(tmp_path: Path) -> None:
    """Носитель читается из БД прогона: снятия корней и причина политики — как записаны."""
    db = tmp_path / "cohound.db"
    with SqliteStore(db) as store:
        store.create_run("run.c", _cfg())
        store.save_corpus_coverage(
            "run.c",
            CorpusCoverage(
                product_id="p1",
                complete=False,
                roots=[_root("https://a/", complete=False)],
                computed_at="2026-11-07T00:00:00+00:00",
                reason="crawl_policy_skipped",
            ),
        )

    conn = measure._open_run_db(db, allow_foreign=False)
    try:
        carriers = measure.read_carriers(conn)
    finally:
        conn.close()
    carrier = carriers["p1"]
    assert carrier.complete is False
    assert carrier.skipped_by_policy is True
    assert [root.root_url for root in carrier.roots] == ["https://a/"]
    assert carrier.bounded_ok() is False


def test_open_run_db_refuses_foreign_schema_without_flag(tmp_path: Path) -> None:
    """Вердикты чужой ``schema_version`` читаются только явно — иначе отказ с путём."""
    db = tmp_path / "foreign.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)",
        (f"{SCHEMA_VERSION}-foreign",),
    )
    conn.commit()
    conn.close()

    with pytest.raises(SystemExit) as excinfo:
        measure._open_run_db(db, allow_foreign=False)
    assert "allow-foreign-schema" in str(excinfo.value)

    # С флагом — открывается: это исторический факт, а не пересчёт.
    conn = measure._open_run_db(db, allow_foreign=True)
    conn.close()


# ---------------------------------------------------------------------------
# Пересчёт носителей текущими правилами (--state-db)
# ---------------------------------------------------------------------------


RUN_ID = "run.ceiling"
ROOT = "https://docs.test/p1/"


def _run_with_declared_roots(tmp_path: Path, roots: list[str]) -> Path:
    """БД прогона: объявленные корни в покрытии + сеанс с провенансом фронтьера."""
    db = tmp_path / "cohound.db"
    with SqliteStore(db) as store:
        store.create_run(RUN_ID, _cfg())
        store.save_corpus_coverage(
            RUN_ID,
            CorpusCoverage(
                product_id="p1",
                complete=False,
                roots=[_root(url, complete=False) for url in roots],
                computed_at="2026-11-07T00:00:00+00:00",
                reason="crawl_policy_skipped",
            ),
        )
    return db


def _provenance(db: Path, roots: list[tuple[str, int, bool]]) -> None:
    """Провенанс сеанса: ``(root, pages_fetched, truncated)`` — первичный счётчик фронтьера."""
    payload = {
        "effective_limits": {
            "roots": [
                {
                    "product_id": "p1",
                    "root": root,
                    "pages_fetched": pages,
                    "skipped_documents": [],
                    "truncated": truncated,
                }
                for root, pages, truncated in roots
            ]
        }
    }
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "INSERT INTO crawl_sessions (session_id, run_id, profile, status, created_at,"
            " provenance_json) VALUES ('crawl:x', ?, 'local', 'completed',"
            " '2026-11-07T00:00:00+00:00', ?)",
            (RUN_ID, json.dumps(payload)),
        )
        conn.commit()
    finally:
        conn.close()


def _capture(*, complete: bool, digest: str, space_json: str | None = None) -> CaptureRow:
    return CaptureRow(
        capture_id=f"cap-{digest}-{int(complete)}-{space_json is not None}",
        product_id="p1",
        root_url=ROOT,
        started_at="2026-11-07T00:00:00+00:00",
        finished_at="2026-11-07T00:10:00+00:00",
        complete=complete,
        observed_how="live",
        run_id=RUN_ID,
        policy_digest=digest,
        space_json=space_json,
    )


def test_state_carriers_recompute_complete_by_current_rule(tmp_path: Path) -> None:
    """``complete`` берётся по нынешнему предикату, а не по записанному флагу.

    Записанное снятие говорит ``complete=False`` (так писала формула 0.6.0, когда
    ``robots`` ещё делал корень неисчерпанным), а первичный счётчик фронтьера —
    ``pages_fetched=3``, ``truncated=False``. Текущее правило обязано дать ``True``:
    иначе пере-замер мерил бы старую формулу, а не новую.
    """
    db = _run_with_declared_roots(tmp_path, [ROOT])
    _provenance(db, [(ROOT, 3, False)])
    state_path = tmp_path / "state.db"
    with SqliteStateStore(state_path) as state:
        state.record_capture(_capture(complete=False, digest="d1"))

    conn = measure._open_run_db(db, allow_foreign=False)
    try:
        carriers = measure.read_carriers_from_state(conn, state_path, RUN_ID)
    finally:
        conn.close()
    assert carriers["p1"].complete is True
    assert carriers["p1"].bounded_ok() is True


def test_state_carriers_keep_unobserved_root_incomplete(tmp_path: Path) -> None:
    """Корень без прочитанных страниц не удостоверяет исчерпания — и после пересчёта."""
    db = _run_with_declared_roots(tmp_path, [ROOT])
    _provenance(db, [(ROOT, 0, False)])
    state_path = tmp_path / "state.db"
    with SqliteStateStore(state_path) as state:
        state.record_capture(_capture(complete=True, digest="d1"))

    conn = measure._open_run_db(db, allow_foreign=False)
    try:
        carriers = measure.read_carriers_from_state(conn, state_path, RUN_ID)
    finally:
        conn.close()
    assert carriers["p1"].complete is False
    assert carriers["p1"].bounded_ok() is False


def test_state_carriers_legacy_file_without_space_json(tmp_path: Path) -> None:
    """Файл схемы ≤ 13 (без ``capture.space_json``) читается: материала нет — сверка по отпечатку.

    Фикстура — минимальная таблица ``capture`` без колонки материала, а не полный DDL:
    прибор читает только её, а ``ALTER TABLE ... DROP COLUMN`` на чистом v14 в этом
    ``.venv`` не работает (SQLite оставляет запятую перед ``)`` после блока комментариев —
    тот же дефект, из-за которого ``tests/scripts/test_migrate_state.py`` собирает
    фикстуру из текста DDL).
    """
    db = _run_with_declared_roots(tmp_path, [ROOT])
    _provenance(db, [(ROOT, 5, False)])
    state_path = tmp_path / "state.db"
    raw = sqlite3.connect(state_path)
    try:
        raw.execute(
            "CREATE TABLE capture (capture_id TEXT PRIMARY KEY, product_id TEXT NOT NULL,"
            " run_id TEXT, root_url TEXT NOT NULL, started_at TEXT NOT NULL,"
            " finished_at TEXT, complete INTEGER NOT NULL, coverage_digest TEXT,"
            " doc_set_digest TEXT, policy_digest TEXT, observed_how TEXT NOT NULL,"
            " actor TEXT, superseded_by TEXT)"
        )
        raw.execute(
            "INSERT INTO capture (capture_id, product_id, root_url, started_at,"
            " finished_at, complete, observed_how, run_id, policy_digest) VALUES"
            " ('cap-old', 'p1', ?, '2026-11-07T00:00:00+00:00',"
            " '2026-11-07T00:10:00+00:00', 1, 'live', ?, 'd1')",
            (ROOT, RUN_ID),
        )
        raw.commit()
    finally:
        raw.close()

    conn = measure._open_run_db(db, allow_foreign=False)
    try:
        carriers = measure.read_carriers_from_state(conn, state_path, RUN_ID)
    finally:
        conn.close()
    assert carriers["p1"].complete is True
    assert carriers["p1"].bounded_ok() is True


def test_state_carriers_refuse_two_declared_spaces(tmp_path: Path) -> None:
    """Снятия прогона с разными отпечатками — отказ: объявленное пространство не определено."""
    db = _run_with_declared_roots(tmp_path, [ROOT])
    state_path = tmp_path / "state.db"
    with SqliteStateStore(state_path) as state:
        state.record_capture(_capture(complete=True, digest="d1"))
        state.record_capture(
            CaptureRow(
                capture_id="cap-other",
                product_id="p1",
                root_url="https://docs.test/p1/second/",
                started_at="2026-11-07T00:00:00+00:00",
                complete=True,
                observed_how="live",
                run_id=RUN_ID,
                policy_digest="d2",
            )
        )

    conn = measure._open_run_db(db, allow_foreign=False)
    try:
        with pytest.raises(SystemExit) as excinfo:
            measure.read_carriers_from_state(conn, state_path, RUN_ID)
    finally:
        conn.close()
    assert "разные отпечатки" in str(excinfo.value)
