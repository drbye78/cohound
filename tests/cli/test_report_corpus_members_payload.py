"""``hound report --json`` называет пропущенные члены контейнеров (B3).

Раньше вопрос «почему этого файла из архива нет в корпусе» имел ответ только в таблице
прогона: в ``outputs/`` не было ни строки, ни сводки. Проверяется проводка целиком —
SQLite → ``write_reports`` → ``outputs/corpus_members.csv`` и блок ``--json``, собранные
из **одного** набора строк (``skip_reason IS NOT NULL``).

Офлайн: SQLite в файле, сеть не используется.
"""

from __future__ import annotations

import csv
import datetime as _dt
from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, main
from core.domain.enums import CellState, RankingPolicy, RunMode
from core.domain.models import Cell, CorpusMember, RunConfig, RunManifest
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.members"
PRODUCT = "p1"


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor=PRODUCT,
        cohort="cohort_case_x",
        products=[PRODUCT],
        package="case_x",
        languages=["en"],
        snapshot_date=_dt.date(2026, 3, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _manifest() -> RunManifest:
    return RunManifest(
        run_id=RUN_ID,
        case_id="case_x",
        created_at="2026-03-01T00:00:00+00:00",
        mode=RunMode.STRICT,
        methodology_version="2.0.0",
        package_version="1.0.0",
        catalog_version="1.0.0",
        status_dict_version="1.0.0",
        scoring_policy_version="1.0.0",
        anchor_product_id=PRODUCT,
        cohort_ref="cohort_case_x",
        snapshot_date="2026-03-01",
    )


def _member(member_path: str, *, skip_reason: str | None, doc_id: str | None) -> CorpusMember:
    return CorpusMember(
        archive_content_hash="sha256:archive-a",
        member_path=member_path,
        product_id=PRODUCT,
        run_id=RUN_ID,
        archive_url="https://docs.test/bundle.zip",
        member_url=f"https://docs.test/bundle.zip!/{member_path}",
        member_format="pdf",
        declared_size=None if skip_reason else 1024,
        doc_id=doc_id,
        skip_reason=skip_reason,
        depth=1,
    )


@pytest.fixture()
def seeded_db(tmp_path: Path) -> Path:
    """Прогон с одной ячейкой и двумя членами контейнера: один принят, один отбит."""
    db = tmp_path / "cohound.db"
    with SqliteStore(db) as store:
        store.create_run(RUN_ID, _cfg())
        store.persist_manifest(RUN_ID, _manifest())
        store.save_cell(
            Cell(
                run_id=RUN_ID,
                product_id=PRODUCT,
                param_id="param_a",
                state=CellState.PENDING,
            )
        )
        store.save_corpus_members(
            [
                _member("docs/ok.pdf", skip_reason=None, doc_id="p1:doc-ok"),
                _member("docs/huge.pdf", skip_reason="member_too_large", doc_id=None),
            ]
        )
    return db


def test_report_publishes_members_csv_and_json_block(
    seeded_db: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """CSV — только пропущенные; сводка ``--json`` — из того же набора; путь объявлен."""
    out = tmp_path / "out"
    assert (
        main(
            [
                "report",
                "--db",
                str(seeded_db),
                "--run-id",
                RUN_ID,
                "--out",
                str(out),
                "--json",
            ]
        )
        == EXIT_OK
    )
    payload = json_out(capsys)

    with (out / RUN_ID / "outputs" / "corpus_members.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert [row["member_path"] for row in rows] == ["docs/huge.pdf"]
    assert rows[0]["skip_reason"] == "member_too_large"
    # Не измерено — пустая строка, не ноль (A3).
    assert rows[0]["declared_size"] == ""

    block = payload["corpus_members"]
    assert block["members_without_document"] == 1
    assert block["by_product"] == {PRODUCT: 1}
    assert block["by_reason"] == {"member_too_large": 1}
    assert payload["artifacts"]["corpus_members"] is not None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))