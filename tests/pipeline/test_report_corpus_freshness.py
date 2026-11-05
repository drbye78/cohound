"""Даты актуальности артефактов корпуса в отчёте (план 20 §7 F2).

Главная приёмка плана: на подготовленном корпусе с **тремя разными** датами
``observed_at`` отчёт печатает три даты, а не одну; ``--json`` и YAML собираются одним
расчётом и расходятся быть не могут. Здесь же проверяются:

* ``evidence_as_of`` ячейки — min/max по датам цитируемых артефактов; ячейка без
  датированного evidence даёт ``null``, в ``stale_roots``/распределение не попадает;
* ``stale_roots`` — только корни, отставшие от самого свежего артефакта **своего**
  продукта; равные даты — не stale; корень без датированных артефактов считается
  отдельно (``roots_without_observation``);
* детерминизм: два вызова отчёта на одной БД дают байт-идентичные файлы (``now()`` нет);
* пустой ``corpus_artifact`` (прогон без проекции) — нули и ``corpus_as_of=None``, без
  падения и без пустого CSV.

Вердикты не проверяются и не меняются: дата в этой фазе — провенанс (план 20 §2), а
датированное отсутствие — F3 (``methodology_change``).
"""

from __future__ import annotations

import csv
import datetime as _dt
import warnings
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

from core.domain.enums import CellState, RankingPolicy, RunMode
from core.domain.models import (
    Cell,
    CorpusCaptureRef,
    CorpusSnapshot,
    RunConfig,
    RunManifest,
)
from core.domain.scoring import Comparability, Coverage, Score, SensitivityReport
from core.pipeline.compute import ScoreBundle
from core.pipeline.report import write_reports
from core.ports.store import CorpusArtifactRow
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-fresh"
PRODUCT = "p1"
ROOT_A = "https://a.test/"
ROOT_B = "https://b.test/"
ROOT_C = "https://c.test/"

D1 = "2026-01-01T00:00:00+00:00"
D2 = "2026-01-11T00:00:00+00:00"
D3 = "2026-02-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor=PRODUCT,
        cohort="cohort_case_x",
        products=[PRODUCT],
        package="case_x",
        languages=["en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as instance:
        instance.create_run(RUN_ID, _cfg())
        yield instance


def _artifact(
    url_key: str,
    observed_at: str | None,
    *,
    root_url: str | None = None,
    doc_version_id: str | None = None,
) -> CorpusArtifactRow:
    return CorpusArtifactRow(
        run_id=RUN_ID,
        product_id=PRODUCT,
        url_key=url_key,
        doc_version_id=doc_version_id,
        observed_at=observed_at,
        observed_how=None if observed_at is None else "live",
        capture_id=None if observed_at is None else "cap",
        root_url=root_url,
    )


def _cell(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=PRODUCT,
        param_id=param_id,
        state=CellState.PENDING,
    )


def _manifest(
    *, snapshot_as_of: dict[str, str] | None = None, roots: list[str] | None = None
) -> RunManifest:
    """Манифест с необязательным снимком корпуса (носитель корней продукта)."""
    snapshot = None
    if snapshot_as_of is not None or roots:
        snapshot = CorpusSnapshot(
            corpus_snapshot_id="snap",
            captures=[
                CorpusCaptureRef(
                    capture_id=f"cap-{index}",
                    product_id=PRODUCT,
                    root_url=root,
                    started_at="2026-01-01T00:00:00+00:00",
                    complete=True,
                    observed_how="live",
                )
                for index, root in enumerate(roots or [])
            ],
            corpus_as_of=snapshot_as_of,
        )
    return RunManifest(
        run_id=RUN_ID,
        case_id="case_x",
        created_at="2026-01-01T00:00:00+00:00",
        mode=RunMode.STRICT,
        methodology_version="2.0.0",
        package_version="1.0.0",
        catalog_version="1.0.0",
        status_dict_version="1.0.0",
        scoring_policy_version="1.0.0",
        anchor_product_id=PRODUCT,
        cohort_ref="cohort_case_x",
        focus_modules=[],
        snapshot_date="2026-01-01",
        corpus_snapshot=snapshot,
    )


def _bundle() -> ScoreBundle:
    """Бандл без покрытия: battle card не эмитится, прочие артефакты пишутся."""
    return ScoreBundle(
        capability=Score(value=0.0, numerator=0.0, denominator=0.0, n_used=0),
        coverage=Coverage(
            value=None,
            numerator=0.0,
            denominator=0.0,
            n_used=0,
            universe=0.0,
            completed=0.0,
            defined=False,
        ),
        failure_rate=0.0,
        modules=[],
        comparability=Comparability(
            level="insufficient", min_coverage=0.0, cross_cycle=True
        ),
        sensitivity=SensitivityReport(
            stable=True,
            per_axis={},
            unstable_pairs=[],
            requested_ranking="capability_ranked",
            computed_ranking="capability_ranked",
        ),
        requested_ranking="capability_ranked",
        computed_ranking="capability_ranked",
    )


def _report(
    store: SqliteStore,
    tmp_path: Path,
    *,
    manifest: RunManifest | None = None,
    run_name: str = "out",
):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return write_reports(
            bundle=_bundle(),
            cells=list(store.iter_cells(RUN_ID)),
            catalog={},
            manifest=manifest or _manifest(),
            out_dir=tmp_path / run_name,
            run_id=RUN_ID,
            store=store,
        )


def _yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


# ---------------------------------------------------------------------------
# Тест 1 — три даты: приёмка плана
# ---------------------------------------------------------------------------


def test_three_observation_dates_are_published_not_one(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Три артефакта, три даты → три даты в артефактах и распределение по ним.

    ``corpus_as_of`` даёт края (D1/D3), ``stale_roots`` называет отставшие корни с их
    датами, распределение считает медиану/p90/max **по трём** наблюдениям.
    """
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}doc", D1, root_url=ROOT_A),
            _artifact(f"{ROOT_B}doc", D2, root_url=ROOT_B),
            _artifact(f"{ROOT_C}doc", D3, root_url=ROOT_C),
        ],
    )
    store.save_cell(_cell("param_a"))
    store.add_evidence(_evidence("param_a", f"{ROOT_A}doc"))
    store.add_evidence(_evidence("param_a", f"{ROOT_C}doc"))

    artifacts = _report(store, tmp_path)

    assert artifacts.corpus_freshness is not None
    document = _yaml(artifacts.corpus_freshness)
    # Одна дата на прогон была бы выдумкой: края — это D1 и D3, а не «день прогона».
    assert document["corpus_as_of"] == {"from": D1, "to": D3}
    assert document["reference"] == D3
    product = document["products"][PRODUCT]
    assert product["corpus_as_of"] == {"from": D1, "to": D3}
    # Распределение — по трём наблюдениям (возраст относительно D3): 31, 21, 0 суток.
    # Числа — вывод объявленной формулы (``statistics.quantiles(n=10,
    # method="inclusive")[8]``), а не вкусовая величина.
    assert product["corpus_age"] == {
        "artifacts": 3,
        "median_days": 21.0,
        "p90_days": 29.0,
        "max_days": 31.0,
    }
    # Отставшие корни — с их собственными датами, порядок по убыванию лага.
    assert product["stale_roots"] == [
        {"root_url": ROOT_A, "as_of": D1, "lag_seconds": 2_678_400, "lag_days": 31.0},
        {"root_url": ROOT_B, "as_of": D2, "lag_seconds": 1_814_400, "lag_days": 21.0},
    ]
    # Ячейка цитирует два артефакта разных дат → evidence_as_of = min/max.
    rows = _csv(artifacts.cell_freshness)
    assert rows == [
        {
            "product_id": PRODUCT,
            "param_id": "param_a",
            "evidence_as_of_from": D1,
            "evidence_as_of_to": D3,
            # I2d: дата слабой опоры ``❌`` — у ячейки её нет (``None`` ⇒ пустое поле,
            # не ``''`` как значение и не выдуманная дата).
            "absence_as_of": "",
            "artifacts": "2",
        }
    ]
    # Три даты видны в одном документе: D1/D2 в stale_roots, D3 в reference/corpus_as_of.
    published = {
        document["reference"],
        document["corpus_as_of"]["from"],
        document["corpus_as_of"]["to"],
        *[item["as_of"] for item in product["stale_roots"]],
    }
    assert published == {D1, D2, D3}
    # YAML и машинный payload — один словарь (один сборщик), сравнение один-в-один.
    assert artifacts.corpus_freshness_summary == document


def _evidence(param_id: str, source_url: str):
    """Evidence-строка с адресом артефакта (без ``doc_version_id``: резолв по адресу)."""
    from core.domain.enums import EvidenceType, SourceKind
    from core.domain.models import CellEvidence

    return CellEvidence(
        evidence_id=f"ev-{param_id}-{source_url}",
        run_id=RUN_ID,
        product_id=PRODUCT,
        param_id=param_id,
        source_kind=SourceKind.DOCUMENTATION,
        evidence_type=EvidenceType.DIRECT,
        source_id=f"{PRODUCT}:{source_url}:0",
        source_url=source_url,
        citation_quote="цитата",
        content_hash="hash",
    )


# ---------------------------------------------------------------------------
# Тест 2 — evidence на двух артефактах; ячейка без даты
# ---------------------------------------------------------------------------


def test_cell_evidence_as_of_is_min_max_and_missing_date_stays_null(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Две даты у одной ячейки → ``{from: min, to: max}``; без даты — ``null``.

    Ячейка без датированного evidence **не** считается устаревшей: ``❓`` — не ``❌``
    (план 20 §6). Она видна числом ``cells_without_dated_evidence`` и отсутствует в CSV.
    """
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}old", D1, root_url=ROOT_A),
            _artifact(f"{ROOT_A}new", D3, root_url=ROOT_A),
        ],
    )
    store.save_cell(_cell("param_dated"))
    store.save_cell(_cell("param_undated"))
    store.add_evidence(_evidence("param_dated", f"{ROOT_A}old"))
    store.add_evidence(_evidence("param_dated", f"{ROOT_A}new"))
    # Адрес, которого нет в проекции артефактов, — даты не даёт.
    store.add_evidence(_evidence("param_undated", "https://unknown.test/doc"))

    artifacts = _report(store, tmp_path)
    document = _yaml(artifacts.corpus_freshness)
    product = document["products"][PRODUCT]

    rows = _csv(artifacts.cell_freshness)
    assert [(row["param_id"], row["evidence_as_of_from"], row["evidence_as_of_to"]) for row in rows] == [
        ("param_dated", D1, D3)
    ]
    assert product["cells_without_dated_evidence"] == 1
    # Даты ячеек не влияют ни на распределение, ни на stale_roots.
    assert product["corpus_age"]["artifacts"] == 2
    assert product["stale_roots"] == []


def test_cell_without_any_evidence_is_not_in_distribution_or_stale(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Ячейка без evidence вовсе — тоже ``null``, а не «свежая дата прогона»."""
    store.replace_corpus_artifacts(
        RUN_ID, PRODUCT, [_artifact(f"{ROOT_A}doc", D1, root_url=ROOT_A)]
    )
    store.save_cell(_cell("param_a"))

    artifacts = _report(store, tmp_path)
    document = _yaml(artifacts.corpus_freshness)
    assert artifacts.cell_freshness is None  # датированных ячеек нет — файла нет
    assert document["products"][PRODUCT]["cells_without_dated_evidence"] == 1
    assert document["products"][PRODUCT]["stale_roots"] == []
    assert document["products"][PRODUCT]["corpus_age"]["max_days"] == 0.0


# ---------------------------------------------------------------------------
# Тест 3 — stale_roots
# ---------------------------------------------------------------------------


def test_stale_roots_names_only_the_older_root(store: SqliteStore, tmp_path: Path) -> None:
    """Два корня, разные даты → stale ровно старший; оба поля лага — от свежего продукта."""
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}doc", D1, root_url=ROOT_A),
            _artifact(f"{ROOT_B}doc", D3, root_url=ROOT_B),
        ],
    )
    artifacts = _report(store, tmp_path)
    product = _yaml(artifacts.corpus_freshness)["products"][PRODUCT]

    assert product["stale_roots"] == [
        {"root_url": ROOT_A, "as_of": D1, "lag_seconds": 2_678_400, "lag_days": 31.0}
    ]
    assert product["roots_without_observation"] == 0


def test_equal_dates_are_not_stale(store: SqliteStore, tmp_path: Path) -> None:
    """Ничьи — не stale: «старше» значит строго старше, и равенство точное (до микросекунды).

    Даты записаны **разными формами одного мгновения** (UTC и ``+03:00``): сравниваются
    моменты, а не строки, поэтому ``lag_seconds == 0`` и корня в ``stale_roots`` нет.
    """
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}doc", "2026-01-11T00:00:00.000000+00:00", root_url=ROOT_A),
            _artifact(f"{ROOT_B}doc", "2026-01-11T03:00:00.000000+03:00", root_url=ROOT_B),
        ],
    )
    artifacts = _report(store, tmp_path)
    product = _yaml(artifacts.corpus_freshness)["products"][PRODUCT]
    assert product["stale_roots"] == []
    assert product["corpus_age"]["max_days"] == 0.0


def test_sub_day_lag_is_stale_with_zero_days_but_positive_seconds(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Разница 5 секунд → корень **в** ``stale_roots``: ``lag_seconds=5``, ``lag_days=0.0``.

    Это и есть дефект, который снимает точная величина: решение принимается по интервалу
    (``lag_seconds > 0``), а ``lag_days`` — только печать. «Отстаёт и на 0.0 суток» —
    разная точность, а не противоречие; читатель видит секунды рядом с днями.
    """
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}doc", "2026-01-02T00:00:00.000000+00:00", root_url=ROOT_A),
            _artifact(f"{ROOT_B}doc", "2026-01-02T00:00:05.000000+00:00", root_url=ROOT_B),
        ],
    )
    artifacts = _report(store, tmp_path)
    product = _yaml(artifacts.corpus_freshness)["products"][PRODUCT]

    assert product["stale_roots"] == [
        {
            "root_url": ROOT_A,
            "as_of": "2026-01-02T00:00:00.000000+00:00",
            "lag_seconds": 5,
            "lag_days": 0.0,
        }
    ]
    # Распределение тоже не «решает» ничего по округлённому: точный максимум — 5 с.
    assert product["corpus_age"]["max_days"] == 0.0


def test_two_day_lag_has_exact_seconds(store: SqliteStore, tmp_path: Path) -> None:
    """Ровно 2 суток → ``lag_seconds == 172800`` и ``lag_days == 2.0`` (без расхождения)."""
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}doc", "2026-01-01T00:00:00+00:00", root_url=ROOT_A),
            _artifact(f"{ROOT_B}doc", "2026-01-03T00:00:00+00:00", root_url=ROOT_B),
        ],
    )
    artifacts = _report(store, tmp_path)
    product = _yaml(artifacts.corpus_freshness)["products"][PRODUCT]

    assert product["stale_roots"] == [
        {
            "root_url": ROOT_A,
            "as_of": "2026-01-01T00:00:00+00:00",
            "lag_seconds": 172_800,
            "lag_days": 2.0,
        }
    ]


def test_root_without_dated_artifact_is_counted_separately(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Корень без датированных артефактов — счётчик, а не stale: даты нет, утверждения нет.

    Корень известен из снятия (снимок прогона), а не из строки артефакта: у артефакта без
    наблюдения ``root_url`` пуст по построению (проекция не выдумывает связь).
    """
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}doc", D1, root_url=ROOT_A),
            _artifact(f"{ROOT_B}doc", None, root_url=None),
        ],
    )
    manifest = _manifest(snapshot_as_of={"from": D1, "to": D1}, roots=[ROOT_A, ROOT_B])
    artifacts = _report(store, tmp_path, manifest=manifest)
    product = _yaml(artifacts.corpus_freshness)["products"][PRODUCT]

    assert product["roots_without_observation"] == 1
    assert product["stale_roots"] == []
    assert product["artifacts_without_observation"] == 1
    # Распределение — только по датированному артефакту, без подстановки.
    assert product["corpus_age"]["artifacts"] == 1


# ---------------------------------------------------------------------------
# Тест 4 — детерминизм (никакого ``now()``)
# ---------------------------------------------------------------------------


def test_report_is_byte_identical_across_calls(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Два вызова на одной БД — байт-идентичные YAML/CSV: опорная точка — наблюдение."""
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [
            _artifact(f"{ROOT_A}doc", D1, root_url=ROOT_A),
            _artifact(f"{ROOT_B}doc", D3, root_url=ROOT_B),
        ],
    )
    store.save_cell(_cell("param_a"))
    store.add_evidence(_evidence("param_a", f"{ROOT_A}doc"))

    first = _report(store, tmp_path, run_name="first")
    second = _report(store, tmp_path, run_name="second")

    assert first.corpus_freshness is not None and second.corpus_freshness is not None
    assert first.corpus_freshness.read_bytes() == second.corpus_freshness.read_bytes()
    assert first.cell_freshness is not None and second.cell_freshness is not None
    assert first.cell_freshness.read_bytes() == second.cell_freshness.read_bytes()


# ---------------------------------------------------------------------------
# Тест 5 — пустой corpus_artifact (прогон без проекции / legacy)
# ---------------------------------------------------------------------------


def test_empty_corpus_artifact_yields_nulls_and_no_csv(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Нет строк — ``corpus_as_of=None`` и нули; CSV не создан; падения нет."""
    store.save_cell(_cell("param_a"))
    artifacts = _report(store, tmp_path)

    assert artifacts.corpus_freshness is not None
    document = _yaml(artifacts.corpus_freshness)
    assert document["corpus_as_of"] is None
    assert document["reference"] is None
    assert document["products"][PRODUCT]["corpus_as_of"] is None
    assert document["products"][PRODUCT]["corpus_age"] == {
        "artifacts": 0,
        "median_days": None,
        "p90_days": None,
        "max_days": None,
    }
    assert document["products"][PRODUCT]["stale_roots"] == []
    assert document["products"][PRODUCT]["cells"] == 1
    assert document["products"][PRODUCT]["cells_without_dated_evidence"] == 1
    assert document["cells"] == 1
    assert artifacts.cell_freshness is None
    assert not (tmp_path / "out" / RUN_ID / "outputs" / "cell_freshness.csv").exists()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))

def test_evidence_resolves_by_doc_version_first(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Резолв: ``doc_version_id`` → ``corpus_artifact.doc_version_id``, адрес — второй шаг.

    ``source_url`` здесь отсутствует намеренно: дата обязана найтись по версии
    документа (план 20 §F2), а не только по адресу. Версия — точный ключ, адрес —
    каноническая форма; порядок назван в модели, и он проверяется.
    """
    from core.domain.enums import EvidenceType, SourceKind
    from core.domain.models import CellEvidence

    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [_artifact(f"{ROOT_A}doc", D2, root_url=ROOT_A, doc_version_id="dv_1")],
    )
    store.save_cell(_cell("param_a"))
    store.add_evidence(
        CellEvidence(
            evidence_id="ev-version",
            run_id=RUN_ID,
            product_id=PRODUCT,
            param_id="param_a",
            source_kind=SourceKind.DOCUMENTATION,
            evidence_type=EvidenceType.DIRECT,
            source_id=f"{PRODUCT}:dv_1:0",
            citation_quote="цитата",
            content_hash="hash",
            doc_version_id="dv_1",
        )
    )

    artifacts = _report(store, tmp_path)
    rows = _csv(artifacts.cell_freshness)
    assert [(row["evidence_as_of_from"], row["artifacts"]) for row in rows] == [(D2, "1")]


def test_document_freshness_reads_the_report_block(tmp_path: Path) -> None:
    """``run`` берёт разброс **из документа отчёта**, а не из ``state.db`` (план 20 §7 F2).

    К концу прогона ``state.db`` уже сдвинут этим прогоном — прежний разброс по нему не
    восстановить. Поэтому ``run`` публикует тот же блок из ``corpus_freshness``: форма и
    предупреждение — общие с ``plan``.
    """
    from core.pipeline.corpus_freshness import document_observation_freshness

    document = {
        "products": {
            "p1": {
                "corpus_as_of": {"from": D1, "to": D3},
                "corpus_age": {"artifacts": 3},
            },
            "p2": {"corpus_as_of": None, "corpus_age": {"artifacts": 0}},
        }
    }
    block = document_observation_freshness(document, warn_days=5)

    assert block["threshold_days"] == 5.0
    assert block["products"] == [
        {
            "product_id": "p1",
            "observations": 3,
            "from": D1,
            "to": D3,
            "spread_days": 31.0,
        }
    ]
    assert len(block["warnings"]) == 1
    assert "p1" in block["warnings"][0]
    # Порог выше разброса — предупреждения нет, число остаётся.
    assert document_observation_freshness(document, warn_days=400)["warnings"] == []


def test_cells_counter_has_a_denominator_on_empty_run(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Прогон без ячеек (``--until corpus``) — ``cells: 0`` рядом с ``…without…: 0``.

    Ноль без знаменателя читался бы как «все ячейки датированы», хотя датировать было
    нечего. Оба числа печатаются, и на уровне прогона тоже. Артефакт корпуса есть
    (фаза ``corpus`` исполнилась) — поэтому блок продукта существует, а ячеек в нём ноль.
    """
    store.replace_corpus_artifacts(
        RUN_ID, PRODUCT, [_artifact(f"{ROOT_A}doc", D1, root_url=ROOT_A)]
    )
    artifacts = _report(store, tmp_path)
    document = _yaml(artifacts.corpus_freshness)

    assert document["cells"] == 0
    assert document["products"][PRODUCT]["cells"] == 0
    assert document["products"][PRODUCT]["cells_without_dated_evidence"] == 0
    assert artifacts.cell_freshness is None


def test_cells_counter_separates_dated_and_undated_cells(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Три ячейки, из них одна без даты → ``cells: 3`` и ``…without…: 1``."""
    store.replace_corpus_artifacts(
        RUN_ID, PRODUCT, [_artifact(f"{ROOT_A}doc", D1, root_url=ROOT_A)]
    )
    for param_id in ("param_a", "param_b", "param_c"):
        store.save_cell(_cell(param_id))
    store.add_evidence(_evidence("param_a", f"{ROOT_A}doc"))
    store.add_evidence(_evidence("param_b", f"{ROOT_A}doc"))

    document = _yaml(_report(store, tmp_path).corpus_freshness)
    assert document["cells"] == 3
    assert document["products"][PRODUCT]["cells"] == 3
    assert document["products"][PRODUCT]["cells_without_dated_evidence"] == 1


def test_run_level_cells_is_the_sum_over_products(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Корневой ``cells`` — сумма по продуктам, а не число строк одного продукта."""
    from core.pipeline.corpus_freshness import build_corpus_freshness

    cell_rows = [
        {"product_id": "p1", "param_id": "a"},
        {"product_id": "p1", "param_id": "b"},
        {"product_id": "p2", "param_id": "a"},
    ]
    result = build_corpus_freshness(run_id=RUN_ID, cells=cell_rows)
    document = result.document
    assert document["cells"] == 3
    assert document["products"]["p1"]["cells"] == 2
    assert document["products"]["p2"]["cells"] == 1
    assert (
        sum(block["cells"] for block in document["products"].values())
        == document["cells"]
    )
