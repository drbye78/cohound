"""Тесты прибора ``scripts/measure_golden_ceiling.py``.

Семантика достижимости проверяется в ``tests/pipeline/test_absence_reach.py`` — прибор её
только импортирует, чтобы не было двух наборов на одно правило. Здесь остаётся то, что
принадлежит **прибору**:

* страта ``❌`` по маркеру отрицания (это разрез прибора, а не отчёт);
* чтение артефактов прогона по путям: носители из ``corpus_coverage`` и отказ на чужой
  ``schema_version`` без явного флага (вердикты чужой схемы — исторический факт).

Живой корпус не нужен: ``tmp_path`` + ``SqliteStore``.
"""

from __future__ import annotations

import datetime as _dt
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
from core.store.sqlite import SCHEMA_VERSION  # noqa: E402
from core.store.sqlite_store import SqliteStore  # noqa: E402


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
