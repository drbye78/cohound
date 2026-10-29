"""Провенанс прогона в payload ``hound report``: манифест, срез и статус продукта.

Закрывается дефект **видимости** (16 §3.1/§3.2, 09 §13.6): до этой правки в payload
команды ``report`` блока манифеста не было вовсе, поэтому ``run_scope``
(``full`` | ``only_failed`` | ``product:<id>``), ``product_plan`` и наблюдаемая ось
``embed_device`` не читались потребителем без заглядывания в ``state.db`` и разбора
``run_manifest.yaml``. Теперь payload несёт ``manifest`` — **тот же** словарь, что файл
(единственный сборщик ``manifest_payload``), и ``run_product`` — строки статуса продукта
в прогоне.

Офлайн: SQLite в файле, сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pytest
import yaml

from core.cli.main import EXIT_OK, main
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig, RunManifest
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

RUN_ID = "run.prov"
PRODUCT = "p1"


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor=PRODUCT,
        cohort="cohort_case_x",
        products=[PRODUCT],
        package="case_x",
        focus=Focus(modules=["mod_a"], params=[]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _manifest() -> RunManifest:
    """Манифест с провенансом среза и объявленной осью устройства."""
    return RunManifest(
        run_id=RUN_ID,
        case_id="case_x",
        created_at="2026-06-01T00:00:00+00:00",
        mode=RunMode.STRICT,
        methodology_version="0.1.0",
        package_version="1.0.0",
        catalog_version="1.0.0",
        status_dict_version="2.2.0",
        scoring_policy_version="1.0.0",
        anchor_product_id=PRODUCT,
        cohort_ref="cohort_case_x",
        run_scope=f"product:{PRODUCT}",
        product_plan={"mode": "incremental", "blocked": [], "reasons": {}},
        embed_device="cuda",
        # 09 §2.4: цикл прогона — материал ``run_id``, поэтому он обязан читаться из
        # артефакта, а не восстанавливаться из идентификатора.
        cycle_id="2026-W43",
        # 09 §12.3/§13: провенанс сравнения — «против какого прогона считался этот».
        previous_run_id="run.prev",
        change_class="execution_change",
    )


@pytest.fixture()
def seeded_db(tmp_path: Path) -> Path:
    """Прогон с персистентным манифестом и строкой ``run_product``."""
    db = tmp_path / "cohound.db"
    with SqliteStore(db) as store:
        store.create_run(RUN_ID, _cfg())
        store.persist_manifest(RUN_ID, _manifest())
        store.save_run_product(
            RUN_ID,
            PRODUCT,
            mode="incremental",
            status="complete",
            scope="full",
            coverage_complete=True,
            docs_total=12,
            docs_reused=9,
            docs_new=3,
            cells_total=40,
            cells_pending=2,
        )
    return db


def test_report_json_payload_carries_manifest_and_run_product(
    seeded_db: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--json``: манифест (срез/ось) и строки ``run_product`` — без чтения ``state.db``."""
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

    manifest = payload["manifest"]
    # 09 §13.6: срез отличим от «полного» прогона прямо в payload.
    assert manifest["run_scope"] == f"product:{PRODUCT}"
    assert manifest["product_plan"]["mode"] == "incremental"
    # 16 §4.1: «на чём считались векторы» читается запросом, а не из контракта.
    assert manifest["embed_device"] == "cuda"
    # 09 §2.4/§12.3/§13: цикл, база сравнения и класс изменения — в payload, а не только в
    # state.db. До правки поля были объявлены и всегда ``None`` (``cycle_id`` не объявлен
    # вовсе): читатель артефакта не видел ни цикла, ни того, с чем сравнивался прогон.
    assert manifest["cycle_id"] == "2026-W43"
    assert manifest["previous_run_id"] == "run.prev"
    assert manifest["change_class"] == "execution_change"

    # Тот же факт — в файле прогона: артефакт и payload собираются одним сборщиком
    # (``manifest_payload``), поэтому расходиться не могут.
    manifest_file = yaml.safe_load(
        (out / RUN_ID / "run_manifest.yaml").read_text(encoding="utf-8")
    )
    assert manifest_file["cycle_id"] == "2026-W43"
    assert manifest_file["previous_run_id"] == "run.prev"
    assert manifest_file["change_class"] == "execution_change"

    # 16 §3.2: статус продукта в прогоне — строками, а не только файлом.
    assert payload["run_product"][0]["product_id"] == PRODUCT
    assert payload["run_product"][0]["status"] == "complete"
    assert payload["run_product"][0]["docs_reused"] == 9

    # Артефакт на диске — тот же носитель: путь объявлен, файл записан.
    run_product_path = payload["run_product_csv"]
    assert run_product_path is not None
    assert Path(run_product_path).exists()


def test_report_human_output_names_scope_and_device(
    seeded_db: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Человеческий вывод называет срез и устройство: «полный» ≠ ``--only-product``."""
    assert (
        main(
            [
                "report",
                "--db",
                str(seeded_db),
                "--run-id",
                RUN_ID,
                "--out",
                str(tmp_path / "out"),
            ]
        )
        == EXIT_OK
    )
    captured = capsys.readouterr()
    assert f"run_scope: product:{PRODUCT}" in captured.out
    assert "embed_device: cuda" in captured.out
    assert f"product: {PRODUCT}" in captured.out


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
