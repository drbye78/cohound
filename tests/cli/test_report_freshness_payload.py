"""``--json`` отдаёт блок ``corpus_freshness`` из **того же** сборщика, что YAML (план 20 §7 F2).

Проверяется конвенция «один сборщик» (``report.py``: манифест): машинный payload и файл
``outputs/corpus_freshness.yaml`` не могут разойтись, потому что оба — один словарь.
Здесь же видно, что путь к файлам объявлен в ``artifacts`` (артефакт без пути — та же
невидимость, что и его отсутствие).

Офлайн: SQLite в файле, сеть не используется.
"""

from __future__ import annotations

import csv
import datetime as _dt
from pathlib import Path

import pytest
import yaml

from core.cli.main import EXIT_OK, main
from core.domain.enums import (
    CellState,
    EvidenceType,
    RankingPolicy,
    RunMode,
    SourceKind,
)
from core.domain.models import (
    Cell,
    CellEvidence,
    CorpusCaptureRef,
    CorpusSnapshot,
    RunConfig,
    RunManifest,
)
from core.ports.store import CorpusArtifactRow
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.fresh"
PRODUCT = "p1"
D1 = "2026-03-01T00:00:00+00:00"
D2 = "2026-03-08T00:00:00+00:00"
D3 = "2026-03-15T00:00:00+00:00"


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
        corpus_snapshot=CorpusSnapshot(
            corpus_snapshot_id="snap",
            captures=[
                CorpusCaptureRef(
                    capture_id="cap-a",
                    product_id=PRODUCT,
                    root_url="https://a.test/",
                    started_at=D1,
                    complete=True,
                    observed_how="live",
                )
            ],
            corpus_as_of={"from": D1, "to": D3},
        ),
    )


@pytest.fixture()
def seeded_db(tmp_path: Path) -> Path:
    """Прогон с тремя датами наблюдений и одной датированной ячейкой."""
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
        store.add_evidence(
            CellEvidence(
                evidence_id="ev-a",
                run_id=RUN_ID,
                product_id=PRODUCT,
                param_id="param_a",
                source_kind=SourceKind.DOCUMENTATION,
                evidence_type=EvidenceType.DIRECT,
                source_id=f"{PRODUCT}:https://a.test/doc:0",
                source_url="https://a.test/doc",
                citation_quote="цитата",
                content_hash="hash",
            )
        )
        store.replace_corpus_artifacts(
            RUN_ID,
            PRODUCT,
            [
                CorpusArtifactRow(
                    run_id=RUN_ID,
                    product_id=PRODUCT,
                    url_key="https://a.test/doc",
                    doc_version_id="dv_a",
                    observed_at=D1,
                    observed_how="live",
                    capture_id="cap-a",
                    root_url="https://a.test/",
                ),
                CorpusArtifactRow(
                    run_id=RUN_ID,
                    product_id=PRODUCT,
                    url_key="https://a.test/other",
                    doc_version_id="dv_b",
                    observed_at=D3,
                    observed_how="live",
                    capture_id="cap-a",
                    root_url="https://a.test/",
                ),
            ],
        )
    return db


def test_json_block_equals_yaml_file(
    seeded_db: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--json.corpus_freshness`` — ровно то, что записано в YAML (байт-в-байт по значениям)."""
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

    document = yaml.safe_load(
        (out / RUN_ID / "outputs" / "corpus_freshness.yaml").read_text(encoding="utf-8")
    )
    assert payload["corpus_freshness"] == document
    # Три даты в блоке: края снимка и дата цитируемого артефакта.
    assert document["corpus_as_of"] == {"from": D1, "to": D3}
    assert document["reference"] == D3
    # Ячейка одна и она датирована: знаменатель рядом с числителем — оба числа в payload
    # и в файле одинаковы (один сборщик).
    assert document["cells"] == 1
    assert document["products"][PRODUCT]["cells"] == 1
    assert document["products"][PRODUCT]["cells_without_dated_evidence"] == 0
    assert payload["corpus_freshness"]["cells"] == 1
    assert (
        payload["corpus_freshness"]["products"][PRODUCT][
            "cells_without_dated_evidence"
        ]
        == 0
    )
    # Пути артефактов объявлены — иначе оператор не знает, что смотреть.
    assert payload["artifacts"]["corpus_freshness"] is not None
    assert payload["artifacts"]["cell_freshness"] is not None

    with (out / RUN_ID / "outputs" / "cell_freshness.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["evidence_as_of_from"] == D1
    assert rows[0]["evidence_as_of_to"] == D1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))