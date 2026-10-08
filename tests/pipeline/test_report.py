"""Тесты отчётных артефактов (W10/T10.2, Ш9): ``pipeline/report.py`` (§9, 02 §E.2).

Покрывают:

* ``write_run_manifest`` — YAML round-trip доменного ``RunManifest``;
* ``battle_card`` — запись при достаточном Coverage; **G4/I11**: при полностью
  отсутствующем Coverage — отказ (:class:`PublishBlockedError`)/``battle_card is None``,
  при недостаточном — приглушение с пометкой; при демотировании — fallback-порядок по
  Coverage с явной меткой «упорядочено по полноте исследования, не по функциональности»
  (K6/A3);
* матрицу (статус + обоснование + primary citation, «+N источников»), сателлиты,
  модульное покрытие, sensitivity-приложение;
* ``nda_questions`` — только ``❓`` (продукт, параметр, контуры);
* ``preliminary_findings`` — маркер «не публикация» (I13);
* ``diff`` — отказ при смене major ``methodology_version``/версионной границе (§13);
* ``write_reports`` — заполненные ``Artifacts``.

Работают на ``SqliteStore(":memory:")``; фикстуры категорийно нейтральны (§1.3).
``openpyxl`` опционален — тесты читают и XLSX, и CSV-fallback единообразно.
"""

from __future__ import annotations

import csv
import datetime as _dt
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.domain.enums import (
    CellState,
    EndpointKind,
    LLMScenario,
    RankingPolicy,
    RunMode,
    Status,
    TerminationReason,
)
from core.domain.models import (
    CatalogRow,
    Cell,
    LLMEvidence,
    ProtocolExhaustion,
    RunConfig,
    RunManifest,
    ScenarioBinding,
)
from core.pipeline.compute import compute, publish_warnings
from core.pipeline.report import (
    FALLBACK_ORDER_LABEL,
    INSUFFICIENT_COVERAGE_NOTE,
    NOT_FOR_PUBLICATION,
    Artifacts,
    CrossCycleDiffError,
    PublishBlockedError,
    render_battle_card,
    render_diff,
    render_nda_questions,
    render_preliminary_findings,
    write_reports,
    write_run_manifest,
)
from core.store.sqlite_store import SqliteStore

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "report_catalog.json"
RUN_ID = "run-report"
PRODUCT_A = "prod.a"
PRODUCT_B = "prod.b"


# ---------------------------------------------------------------------------
# Помощники: входные данные
# ---------------------------------------------------------------------------


def _catalog() -> dict[str, CatalogRow]:
    """Нейтральный каталог из JSON-фикстуры."""
    rows = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return {row["param_id"]: CatalogRow(**row) for row in rows}


def _cfg(ranking: RankingPolicy = RankingPolicy.CAPABILITY_RANKED) -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort.yaml",
        products=[PRODUCT_A, PRODUCT_B],
        package="pkg-1",
        languages=["en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=ranking,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    """Хранилище с созданным прогоном (FK cells → runs включён)."""
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        yield s


def _cell(
    product_id: str,
    param_id: str,
    status: Status | None,
    state: CellState = CellState.EVALUATED,
) -> Cell:
    """Собирает валидную ячейку под инварианты I1/I2/I4/I6/I7/I13."""
    if state in {CellState.FAILED, CellState.PRELIMINARY}:
        return Cell(run_id=RUN_ID, product_id=product_id, param_id=param_id, state=state)
    if status in {Status.OK, Status.PARTIAL}:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            evidence=[LLMEvidence(source_id="src.1", quote="цитата")],
        )
    if status is Status.ABSENT:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            absence_evidence=[{"source_scope": "partial"}],
        )
    if status is Status.NOT_APPLICABLE:
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            reviewed_by="human",
        )
    return Cell(
        run_id=RUN_ID, product_id=product_id, param_id=param_id, state=state, status=status
    )


def _save(store: SqliteStore, cells: list[Cell]) -> None:
    for cell in cells:
        store.save_cell(cell)


def _manifest(run_id: str = RUN_ID, **overrides: Any) -> RunManifest:
    """Нейтральный ``RunManifest`` со всеми версиями/профилем+K/моделями."""
    data: dict[str, Any] = {
        "run_id": run_id,
        "case_id": "case-1",
        "created_at": "2026-01-01T00:00:00+00:00",
        "mode": RunMode.STRICT,
        "methodology_version": "2.0.0",
        "package_version": "1.0.0",
        "catalog_version": "1.0.0",
        "status_dict_version": "1.0.0",
        "scoring_policy_version": "1.0.0",
        "anchor_product_id": PRODUCT_A,
        "cohort_ref": "cohort-1",
        "focus_modules": [],
        "index_build_id": "build.1",
        "embed_model_id": "embed.1",
        "chunker_version": "chunker.1",
        "extractor_version": "extractor.1",
        "lexical_engine_version": "lexical.1",
        "vector_store": "npy",
        "retrieval_profile": "baseline",
        "retrieval_cutoff_K": 10,
        "retrieval_params_json": '{"rrf_k": 60}',
        "search_effort_budget_json": '{"serp_rounds": 3}',
        "requested_ranking": RankingPolicy.CAPABILITY_RANKED,
        "computed_ranking": RankingPolicy.CAPABILITY_RANKED,
        "llm_scenarios": [
            ScenarioBinding(
                run_id=run_id,
                scenario=LLMScenario.EVALUATOR,
                model_id="model.1",
                model_version="1",
                endpoint_kind=EndpointKind.SELF_HOST,
                web_tool=False,
            )
        ],
        "snapshot_date": "2026-01-01",
    }
    data.update(overrides)
    return RunManifest(**data)


# ---------------------------------------------------------------------------
# Помощники: чтение артефактов (XLSX либо CSV-fallback)
# ---------------------------------------------------------------------------


def _artifact_text(path: Path) -> str:
    """Текст артефакта: XLSX (все строковые клетки) либо CSV (как есть)."""
    if path.suffix == ".xlsx":
        openpyxl = pytest.importorskip("openpyxl")
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        chunks: list[str] = []
        for worksheet in workbook.worksheets:
            for row in worksheet.iter_rows(values_only=True):
                for value in row:
                    if value is not None:
                        chunks.append(str(value))
        return "\n".join(chunks)
    return path.read_text(encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _matrix_product_order(path: Path) -> list[str]:
    """Порядок продуктов в матрице battle card (XLSX-лист или CSV-секция)."""
    if path.suffix == ".xlsx":
        openpyxl = pytest.importorskip("openpyxl")
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        worksheet = workbook["matrix"]
        order: list[str] = []
        for row in worksheet.iter_rows(min_row=2, values_only=True):
            if row and row[0] is not None:
                order.append(str(row[0]))
        return order
    order = []
    for row in _read_csv(path):
        if row["section"] == "matrix" and row["product"] not in order:
            order.append(row["product"])
    return order


# ---------------------------------------------------------------------------
# run_manifest.yaml
# ---------------------------------------------------------------------------


def test_write_run_manifest_round_trip(tmp_path: Path) -> None:
    manifest = _manifest()
    path = write_run_manifest(manifest, tmp_path)

    assert path == tmp_path / "run_manifest.yaml"
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert RunManifest(**loaded) == manifest

    # Детерминизм: повторная запись даёт тот же YAML.
    again = write_run_manifest(manifest, tmp_path / "again")
    assert again.read_text(encoding="utf-8") == path.read_text(encoding="utf-8")
    assert loaded["retrieval_cutoff_K"] == 10
    assert loaded["llm_scenarios"][0]["scenario"] == "evaluator"


# ---------------------------------------------------------------------------
# battle_card
# ---------------------------------------------------------------------------


def test_battle_card_written_with_sufficient_coverage(store: SqliteStore, tmp_path: Path) -> None:
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.OK), _cell(PRODUCT_A, "param.p1", Status.OK)])
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    assert bundle.coverage.value == pytest.approx(1.0)

    path = render_battle_card(bundle, cells, _catalog(), tmp_path / "battle_card.xlsx")
    assert path.exists()
    text = _artifact_text(path)
    assert Status.OK.value in text
    assert INSUFFICIENT_COVERAGE_NOTE not in text
    assert FALLBACK_ORDER_LABEL not in text


def test_battle_card_matrix_renders_rich_cell(tmp_path: Path) -> None:
    """Матрица несёт обоснование, primary citation, «+N источников» и сателлиты."""
    store = SqliteStore(":memory:")
    store.create_run(RUN_ID, _cfg())
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.OK)])
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})

    views = [
        _CellView(
            product_id=PRODUCT_A,
            param_id="param.ok",
            state=CellState.EVALUATED,
            status=Status.OK,
            justification="обоснование-1",
            evidence=[_Ev("src.a", "цитата-a"), _Ev("src.b", "цитата-b")],
            satellites=[_Sat("role.1", "approved", "third_party", "integrated", "verified")],
        )
    ]
    path = render_battle_card(bundle, views, _catalog(), tmp_path / "battle_card.xlsx")
    text = _artifact_text(path)
    assert Status.OK.value in text
    assert "обоснование-1" in text
    assert "src.a" in text
    assert "+1 источников" in text
    assert "role.1" in text
    store.close()


def test_battle_card_dimmed_when_coverage_insufficient(store: SqliteStore, tmp_path: Path) -> None:
    # 1 разрешённая + 2 ❓ равного веса → Coverage 0.4 < порога.
    _save(
        store,
        [
            _cell(PRODUCT_A, "param.ok", Status.OK),
            _cell(PRODUCT_A, "param.u2", Status.UNKNOWN),
            _cell(PRODUCT_A, "param.u3", Status.UNKNOWN),
        ],
    )
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    assert bundle.coverage.value < 0.5

    path = render_battle_card(bundle, cells, _catalog(), tmp_path / "battle_card.xlsx")
    assert path.exists()
    text = _artifact_text(path)
    assert INSUFFICIENT_COVERAGE_NOTE in text


def test_battle_card_refused_without_coverage(store: SqliteStore, tmp_path: Path) -> None:
    """G4/I11: нет Coverage вообще (только N/A) → battle card не публикуется."""
    _save(store, [_cell(PRODUCT_A, "param.na", Status.NOT_APPLICABLE)])
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    assert publish_warnings(bundle)

    with pytest.raises(PublishBlockedError):
        render_battle_card(bundle, cells, _catalog(), tmp_path / "battle_card.xlsx")
    assert not (tmp_path / "battle_card.xlsx").exists()
    assert not (tmp_path / "battle_card.csv").exists()


def test_battle_card_demotion_uses_coverage_fallback_order(store: SqliteStore, tmp_path: Path) -> None:
    """K6/A3: неустойчивый порядок → fallback-порядок по Coverage + явная метка."""
    _save(
        store,
        [
            _cell(PRODUCT_A, "param.p1", Status.OK),
            _cell(PRODUCT_A, "param.p2", Status.UNKNOWN),
            _cell(PRODUCT_B, "param.p1", Status.OK),
            _cell(PRODUCT_B, "param.p2", Status.PARTIAL),
        ],
    )
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    assert bundle.computed_ranking == RankingPolicy.COVERAGE_FALLBACK.value

    path = render_battle_card(bundle, cells, _catalog(), tmp_path / "battle_card.xlsx")
    text = _artifact_text(path)
    assert FALLBACK_ORDER_LABEL in text
    # Coverage(B)=1.0 > Coverage(A)≈0.571 → B раньше A.
    assert _matrix_product_order(path) == [PRODUCT_B, PRODUCT_A]


# ---------------------------------------------------------------------------
# nda_questions
# ---------------------------------------------------------------------------


def test_nda_questions_contains_only_unknown(tmp_path: Path) -> None:
    cells = [
        _cell(PRODUCT_A, "param.unknown", Status.UNKNOWN),
        _cell(PRODUCT_A, "param.ok", Status.OK),
        _cell(PRODUCT_B, "param.u2", Status.UNKNOWN),
        _cell(PRODUCT_A, "param.na", Status.NOT_APPLICABLE),
    ]
    exhaustions = {
        (PRODUCT_A, "param.unknown"): ProtocolExhaustion(
            run_id=RUN_ID,
            product_id=PRODUCT_A,
            param_id="param.unknown",
            contours=["corpus", "serp"],
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        )
    }
    path = render_nda_questions(cells, tmp_path / "nda_questions.csv", exhaustions=exhaustions)
    rows = _read_csv(path)

    assert {(r["product_id"], r["param_id"]) for r in rows} == {
        (PRODUCT_A, "param.unknown"),
        (PRODUCT_B, "param.u2"),
    }
    assert all(r["status"] == Status.UNKNOWN.value for r in rows)
    by_key = {(r["product_id"], r["param_id"]): r for r in rows}
    assert by_key[(PRODUCT_A, "param.unknown")]["exhausted_contours"] == "corpus,serp"
    assert by_key[(PRODUCT_B, "param.u2")]["exhausted_contours"] == ""


# ---------------------------------------------------------------------------
# preliminary_findings
# ---------------------------------------------------------------------------


def test_preliminary_findings_marked_not_for_publication(tmp_path: Path) -> None:
    cells = [
        _cell(PRODUCT_A, "param.prelim", None, CellState.PRELIMINARY),
        _cell(PRODUCT_A, "param.ok", Status.OK),
    ]
    path = render_preliminary_findings(cells, tmp_path / "preliminary_findings.xlsx")
    assert path.exists()
    text = _artifact_text(path)
    assert NOT_FOR_PUBLICATION in text
    assert "param.prelim" in text
    assert "param.ok" not in text  # не preliminary → не кандидат


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------


def test_diff_refuses_on_major_methodology_mismatch(tmp_path: Path) -> None:
    prev = _manifest(methodology_version="2.0.0")
    curr = _manifest(methodology_version="3.0.0")
    with pytest.raises(CrossCycleDiffError):
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    assert not (tmp_path / "diff.md").exists()


def test_diff_refuses_on_version_boundary(tmp_path: Path) -> None:
    prev = _manifest()
    curr = _manifest(retrieval_cutoff_K=25)
    with pytest.raises(CrossCycleDiffError):
        render_diff(prev, curr, [], [], tmp_path / "diff.md")


def test_diff_writes_changed_statuses_and_evidence(tmp_path: Path) -> None:
    prev = _manifest(run_id="run-prev")
    curr = _manifest(run_id=RUN_ID)
    prev_cells = [_cell(PRODUCT_A, "param.ok", Status.OK)]
    curr_cells = [
        _cell(PRODUCT_A, "param.ok", Status.PARTIAL),
        _cell(PRODUCT_A, "param.absent", Status.ABSENT),
    ]
    path = render_diff(prev, curr, prev_cells, curr_cells, tmp_path / "diff.md")
    text = path.read_text(encoding="utf-8")

    assert "run-prev → run-report" in text
    assert Status.OK.value in text
    assert Status.PARTIAL.value in text
    assert "src.1" in text  # evidence_id
    assert "param.absent" in text


# ---------------------------------------------------------------------------
# write_reports
# ---------------------------------------------------------------------------


def test_write_reports_returns_populated_artifacts(store: SqliteStore, tmp_path: Path) -> None:
    _save(
        store,
        [
            _cell(PRODUCT_A, "param.ok", Status.OK),
            _cell(PRODUCT_A, "param.p1", Status.OK),
            _cell(PRODUCT_A, "param.prelim", None, CellState.PRELIMINARY),
            _cell(PRODUCT_A, "param.u2", Status.UNKNOWN),
        ],
    )
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})

    artifacts = write_reports(
        bundle=bundle,
        cells=cells,
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path,
        run_id=RUN_ID,
    )

    assert isinstance(artifacts, Artifacts)
    assert artifacts.manifest == tmp_path / RUN_ID / "run_manifest.yaml"
    assert artifacts.manifest.exists()
    assert artifacts.battle_card is not None and artifacts.battle_card.exists()
    assert artifacts.preliminary_findings is not None and artifacts.preliminary_findings.exists()
    assert artifacts.nda_questions is not None and artifacts.nda_questions.exists()
    assert artifacts.diff is not None and artifacts.diff.exists()
    assert "не передан" in artifacts.diff.read_text(encoding="utf-8")


def test_write_reports_omits_battle_card_without_coverage(store: SqliteStore, tmp_path: Path) -> None:
    _save(store, [_cell(PRODUCT_A, "param.na", Status.NOT_APPLICABLE)])
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})

    with pytest.warns(RuntimeWarning):  # compute.assert_publishable (I11)
        artifacts = write_reports(
            bundle=bundle,
            cells=cells,
            catalog=_catalog(),
            manifest=_manifest(),
            out_dir=tmp_path,
            run_id=RUN_ID,
        )

    assert artifacts.battle_card is None
    assert artifacts.manifest.exists()
    assert artifacts.nda_questions is not None and artifacts.nda_questions.exists()


def test_write_reports_renders_diff_with_previous_cycle(store: SqliteStore, tmp_path: Path) -> None:
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.PARTIAL)])
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})

    artifacts = write_reports(
        bundle=bundle,
        cells=cells,
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path,
        run_id=RUN_ID,
        prev_manifest=_manifest(run_id="run-prev"),
        prev_cells=[_cell(PRODUCT_A, "param.ok", Status.OK)],
    )
    assert artifacts.diff is not None
    assert "run-prev → run-report" in artifacts.diff.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Вспомогательные носители (нейтральные) для проверки рендера
# ---------------------------------------------------------------------------


@dataclass
class _Ev:
    source_id: str
    quote: str


@dataclass
class _Sat:
    role: str
    state: str
    origin: str
    relationship: str
    evidence_state: str


@dataclass
class _CellView:
    """Ячейка-представление с обоснованием/сателлитами (доменная ``Cell`` их не несёт)."""

    product_id: str
    param_id: str
    state: CellState
    status: Status | None = None
    evidence: list[Any] = field(default_factory=list)
    absence_evidence: list[Any] = field(default_factory=list)
    justification: str = ""
    satellites: list[Any] = field(default_factory=list)
