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
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from core.domain import REQUIRED_EXHAUSTION_CONTOURS
from core.domain.enums import (
    CellState,
    CrawlStrategy,
    EndpointKind,
    LLMScenario,
    RankingPolicy,
    RunMode,
    Status,
    TerminationReason,
)
from core.domain.models import (
    METHODOLOGY_VERSION,
    CatalogRow,
    Cell,
    CorpusCoverage,
    CrawlProvenance,
    CrawlRunConfig,
    CrawlSessionProvenance,
    LLMEvidence,
    ProtocolExhaustion,
    RateLimitSpec,
    RunConfig,
    RunManifest,
    ScenarioBinding,
    canonical_rate_limit,
)
from core.domain.scoring import Comparability, Coverage, Score, SensitivityReport
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.pipeline.compute import ScoreBundle, compute, publish_warnings
from core.pipeline.report import (
    DIFF_BOUNDARY_FIELDS,
    EXECUTION_FIELDS,
    FALLBACK_ORDER_LABEL,
    INSUFFICIENT_COVERAGE_NOTE,
    METHODOLOGY_FIELDS,
    NOT_FOR_PUBLICATION,
    Artifacts,
    CrossCycleDiffError,
    PublishBlockedError,
    build_manifest,
    collect_i3_anomalies,
    collect_corpus_coverage_anomalies,
    diff_boundary_violations,
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


def test_build_manifest_carries_provenance_and_epsilon(tmp_path: Path) -> None:
    """§9/A3 (v2.1): ``settings_provenance``/``ranking_epsilon`` идут из ``run`` в манифест."""
    provenance = {
        "ranking_epsilon": {"source": "measured", "experiment_id": "exp-1"},
        "partial_score": {"source": "default"},
    }
    run = SimpleNamespace(
        case_id="case-1",
        mode=RunMode.STRICT,
        methodology_version="2.1.0",
        cohort="cohort.yaml",
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        settings_provenance=provenance,
        ranking_epsilon=0.05,
    )
    inputs = SimpleNamespace(run=run, package=None)
    plan = SimpleNamespace(
        run_id="run-build",
        anchor=PRODUCT_A,
        modules=[],
        methodology_version="2.1.0",
        catalog_version="1.0.0",
        cohort_ref="cohort.yaml",
    )

    manifest = build_manifest(
        inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z"
    )

    assert manifest.ranking_epsilon == pytest.approx(0.05)
    assert manifest.settings_provenance == provenance

    # Round-trip через run_manifest.yaml.
    path = write_run_manifest(manifest, tmp_path)
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert loaded["ranking_epsilon"] == pytest.approx(0.05)
    assert loaded["settings_provenance"] == provenance
    assert RunManifest(**loaded) == manifest

    # Аргумент имеет приоритет над ``run`` (явный провенанс).
    override = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-01-01T00:00:00Z",
        settings_provenance={"ranking_epsilon": {"source": "default"}},
        ranking_epsilon=0.0,
    )
    assert override.ranking_epsilon == 0.0
    assert override.settings_provenance == {"ranking_epsilon": {"source": "default"}}


def test_run_config_methodology_version_default_and_flow() -> None:
    """P2: ``RunConfig.methodology_version`` по умолчанию — пин проекта и идёт в манифест."""
    cfg = RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort-1",
        products=[PRODUCT_A],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    assert cfg.methodology_version == METHODOLOGY_VERSION

    inputs = SimpleNamespace(run=cfg, package=None)
    plan = SimpleNamespace(
        run_id="run-mv", anchor=PRODUCT_A, modules=[], cohort_ref="cohort-1"
    )
    manifest = build_manifest(
        inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z"  # type: ignore[arg-type]
    )
    assert manifest.methodology_version == METHODOLOGY_VERSION


def test_build_manifest_carries_crawl_axes_from_run_config() -> None:
    """Backlog B (§13): ``crawl.strategy``/``crawl.rate_limit`` идут из ``run.crawl`` в манифест."""
    cfg = RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort-1",
        products=[PRODUCT_A],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        crawl=CrawlRunConfig(
            strategy=CrawlStrategy.HYBRID,
            rate_limit=RateLimitSpec(min_s=0.5, max_s=2.0),
        ),
    )
    inputs = SimpleNamespace(run=cfg, package=None)
    plan = SimpleNamespace(
        run_id="run-crawl", anchor=PRODUCT_A, modules=[], cohort_ref="cohort-1"
    )
    manifest = build_manifest(
        inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z"  # type: ignore[arg-type]
    )
    assert manifest.crawl_strategy == "hybrid"
    assert manifest.crawl_rate_limit == canonical_rate_limit(
        RateLimitSpec(min_s=0.5, max_s=2.0)
    )


def test_build_manifest_crawl_axes_none_without_section() -> None:
    """Backlog B: без секции ``crawl`` оси — ``None`` (прежние манифесты не меняются)."""
    cfg = RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort-1",
        products=[PRODUCT_A],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    inputs = SimpleNamespace(run=cfg, package=None)
    plan = SimpleNamespace(
        run_id="run-nocrawl", anchor=PRODUCT_A, modules=[], cohort_ref="cohort-1"
    )
    manifest = build_manifest(
        inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z"  # type: ignore[arg-type]
    )
    assert manifest.crawl_strategy is None
    assert manifest.crawl_rate_limit is None


def test_crawl_config_digest_covers_all_layers_and_flags_execution_change() -> None:
    """N6: дайджест разрешённой конфигурации контроля краула ловит смену на app/per-site
    слое и помечает ``execution_change`` (§13)."""
    cfg = RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort-1",
        products=[PRODUCT_A],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    inputs = SimpleNamespace(run=cfg, package=None)
    plan = SimpleNamespace(
        run_id="run-digest", anchor=PRODUCT_A, modules=[], cohort_ref="cohort-1"
    )
    base = build_manifest(
        inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z"  # type: ignore[arg-type]
    )
    assert "crawl_config_digest" in EXECUTION_FIELDS
    assert base.crawl_config_digest

    # App-слой (deploy.yaml:crawl) — run-поле не менялось, но дайджест обязан измениться.
    app = CrawlConfig(max_depth=1)
    app_changed = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-01-01T00:00:00Z",  # type: ignore[arg-type]
        crawl_app=app,
    )
    assert app_changed.crawl_config_digest != base.crawl_config_digest
    violations = diff_boundary_violations(base, app_changed)
    assert any(
        v.startswith("execution_change: crawl_config_digest") for v in violations
    )

    # Per-site слой (domain_profiles) — тоже обязан изменить дайджест.
    site_a = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-01-01T00:00:00Z",  # type: ignore[arg-type]
        domain_profiles={"example.com": DomainProfile(max_depth=1)},
    )
    site_b = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-01-01T00:00:00Z",  # type: ignore[arg-type]
        domain_profiles={"example.com": DomainProfile(max_depth=2)},
    )
    assert site_a.crawl_config_digest != site_b.crawl_config_digest
    assert site_a.crawl_config_digest != base.crawl_config_digest


def test_canonical_rate_limit_is_stable_string() -> None:
    """Backlog B: каноническая форма ``RateLimitSpec`` детерминирована; ``None`` → ``None``."""
    assert canonical_rate_limit(None) is None
    assert canonical_rate_limit(RateLimitSpec(min_s=0, max_s=0)) == "uniform(min=0.0,max=0.0)"
    assert (
        canonical_rate_limit(RateLimitSpec(min_s=0.5, max_s=2.0))
        == "uniform(min=0.5,max=2.0)"
    )


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


def test_battle_card_header_surfaces_heterogeneous_crawl_engines(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Phase 2: провенанс краула в шапке — движки + флаг неоднородности (информационно)."""
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.OK)])
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    provenance = CrawlProvenance(
        sessions=[
            CrawlSessionProvenance(
                session_id="crawl:run-report",
                profile="local",
                status="completed",
                engines=["http", "scrapy"],
                kinds=["web"],
                total=2,
                fetched=2,
            )
        ],
        engines=["http", "scrapy"],
        kinds=["web"],
        crawl_sources_digest="deadbeef",
    )

    path = render_battle_card(
        bundle,
        cells,
        _catalog(),
        tmp_path / "battle_card.xlsx",
        crawl_provenance=provenance,
    )
    text = _artifact_text(path)
    assert "crawl_engines" in text
    assert "http, scrapy" in text
    assert "crawl_engines_heterogeneous" in text


def test_battle_card_header_homogeneous_engines_no_flag(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Один движок → флаг неоднородности не выставляется (наблюдение, не демотирование)."""
    _save(store, [_cell(PRODUCT_A, "param.ok", Status.OK)])
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    provenance = CrawlProvenance(engines=["http"], kinds=["web"])

    path = render_battle_card(
        bundle,
        cells,
        _catalog(),
        tmp_path / "battle_card.xlsx",
        crawl_provenance=provenance,
    )
    text = _artifact_text(path)
    assert "crawl_engines" in text
    assert "crawl_engines_heterogeneous" not in text


def test_build_manifest_accepts_crawl_provenance_mapping(tmp_path: Path) -> None:
    """``build_manifest`` принимает mapping и валидирует его в ``CrawlProvenance``."""
    inputs = SimpleNamespace(
        run=SimpleNamespace(
            methodology_version="2.0.0", ranking_epsilon=0.0, mode=RunMode.STRICT
        ),
        ranking_epsilon=None,
    )
    plan = SimpleNamespace(run_id="run-report", methodology_version="2.0.0")
    manifest = build_manifest(
        inputs=inputs,
        plan=plan,
        crawl_provenance={"engines": ["scrapy"], "kinds": ["web"]},
    )
    assert manifest.crawl_provenance.engines == ["scrapy"]
    assert manifest.crawl_provenance.kinds == ["web"]


def test_heterogeneous_flag_is_durable_even_when_battle_card_blocked(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Phase 2.1/S3: флаг переживает ``read_manifest`` даже при блокировке battle card (I11).

    Шапка battle card использует **durable** флаг провенанса; блокировка публикации (I11/G4)
    на сам провенанс не влияет — наблюдение остаётся в манифесте.
    """
    provenance = CrawlProvenance(engines=["http", "scrapy"], kinds=["web"])
    inputs = SimpleNamespace(
        run=SimpleNamespace(
            methodology_version="2.0.0", ranking_epsilon=0.0, mode=RunMode.STRICT
        ),
        ranking_epsilon=None,
    )
    plan = SimpleNamespace(run_id=RUN_ID, methodology_version="2.0.0")
    manifest = build_manifest(inputs=inputs, plan=plan, crawl_provenance=provenance)
    store.persist_manifest(RUN_ID, manifest)
    assert store.read_manifest(RUN_ID).crawl_provenance.heterogeneous is True

    # Только N/A → Coverage отсутствует: battle card обязан отказать (I11/G4).
    _save(store, [_cell(PRODUCT_A, "param.na", Status.NOT_APPLICABLE)])
    cells = list(store.iter_cells(RUN_ID))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    with pytest.raises(PublishBlockedError):
        render_battle_card(
            bundle,
            cells,
            _catalog(),
            tmp_path / "battle_card.xlsx",
            crawl_provenance=store.read_manifest(RUN_ID).crawl_provenance,
        )
    assert not (tmp_path / "battle_card.xlsx").exists()
    assert store.read_manifest(RUN_ID).crawl_provenance.heterogeneous is True


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


def _undefined_bundle() -> ScoreBundle:
    """Бандл с ``coverage.defined is False`` (``UNDEFINED``: universe = 0, §6/B-3a)."""
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


def test_battle_card_blocked_when_coverage_undefined(tmp_path: Path) -> None:
    """§6/B-3a + I11/G4: ``coverage.defined is False`` → Capability не публикуется."""
    bundle = _undefined_bundle()
    assert publish_warnings(bundle)  # UNDEFINED покрытие непубликуемо

    with pytest.raises(PublishBlockedError):
        render_battle_card(bundle, [], _catalog(), tmp_path / "battle_card.xlsx")
    assert not (tmp_path / "battle_card.xlsx").exists()
    assert not (tmp_path / "battle_card.csv").exists()


def test_write_reports_omits_battle_card_when_coverage_undefined(
    tmp_path: Path,
) -> None:
    """I11/G4: артефакт не эмитится при UNDEFINED-покрытии (мягкое предупреждение + отказ)."""
    bundle = _undefined_bundle()
    with pytest.warns(RuntimeWarning):  # compute.assert_publishable (I11)
        artifacts = write_reports(
            bundle=bundle,
            cells=[],
            catalog=_catalog(),
            manifest=_manifest(),
            out_dir=tmp_path,
            run_id=RUN_ID,
        )
    assert artifacts.battle_card is None
    assert artifacts.manifest.exists()


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


def test_nda_questions_records_i3_anomaly(tmp_path: Path) -> None:
    """§5/I3 (v2.1): неполный протокол → аномалия, статус ``❓`` не меняется."""
    cells = [
        _cell(PRODUCT_A, "param.full", Status.UNKNOWN),
        _cell(PRODUCT_A, "param.partial", Status.UNKNOWN),
        _cell(PRODUCT_A, "param.missing", Status.UNKNOWN),
    ]
    exhaustions = {
        (PRODUCT_A, "param.full"): ProtocolExhaustion(
            run_id=RUN_ID,
            product_id=PRODUCT_A,
            param_id="param.full",
            contours=list(REQUIRED_EXHAUSTION_CONTOURS),
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        ),
        (PRODUCT_A, "param.partial"): ProtocolExhaustion(
            run_id=RUN_ID,
            product_id=PRODUCT_A,
            param_id="param.partial",
            contours=["corpus", "serp"],
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        ),
    }
    path = render_nda_questions(
        cells, tmp_path / "nda_questions.csv", exhaustions=exhaustions
    )
    rows = _read_csv(path)
    by_key = {(r["product_id"], r["param_id"]): r for r in rows}

    assert by_key[(PRODUCT_A, "param.full")]["protocol_status"] == "complete"
    assert by_key[(PRODUCT_A, "param.full")]["i3_anomaly"] == ""

    assert by_key[(PRODUCT_A, "param.partial")]["protocol_status"] == "incomplete"
    assert by_key[(PRODUCT_A, "param.partial")]["i3_anomaly"]

    assert by_key[(PRODUCT_A, "param.missing")]["protocol_status"] == "incomplete"
    assert by_key[(PRODUCT_A, "param.missing")]["i3_anomaly"]

    # I12: статус неизменен — аномалия не превращает ❓ в ❌.
    assert all(r["status"] == Status.UNKNOWN.value for r in rows)

    anomalies = collect_i3_anomalies(cells, exhaustions)
    assert {(a["product_id"], a["param_id"]) for a in anomalies} == {
        (PRODUCT_A, "param.partial"),
        (PRODUCT_A, "param.missing"),
    }
    assert all(a["reason"].startswith("I3:") for a in anomalies)


def test_nda_questions_includes_crawl_truncation_rows(tmp_path: Path) -> None:
    """B3: аномалии усечённого краула дописываются в операторский CSV и отличимы."""
    cells = [_cell(PRODUCT_A, "param.u", Status.UNKNOWN)]
    truncation = [
        {"product_id": PRODUCT_A, "param_id": "param.abs", "reason": "I2b/G: ..."}
    ]
    path_with = render_nda_questions(
        cells,
        tmp_path / "with.csv",
        crawl_truncation_anomalies=truncation,
    )
    rows = _read_csv(path_with)
    trunc_rows = [r for r in rows if r.get("anomaly_kind") == "crawl_truncation"]
    assert len(trunc_rows) == 1
    assert trunc_rows[0]["status"] == "crawl_truncation"
    assert trunc_rows[0]["product_id"] == PRODUCT_A
    assert "I2b/G" in trunc_rows[0]["i3_anomaly"]
    # I3-строки остаются отличимыми.
    assert all(
        r["anomaly_kind"] == "i3_protocol"
        for r in rows
        if r.get("anomaly_kind") != "crawl_truncation"
    )

    # Без усечения — ни одной crawl_truncation-строки.
    path_without = render_nda_questions(cells, tmp_path / "without.csv")
    assert not [
        r
        for r in _read_csv(path_without)
        if r.get("anomaly_kind") == "crawl_truncation"
    ]


def test_write_reports_surfaces_truncation_anomaly_in_artifacts_and_csv(
    store: SqliteStore, tmp_path: Path
) -> None:
    """B3: усечённый продукт + ``exhaustive``-absence → строкa в CSV и поле в артефактах;
    untruncated-прогон их не даёт."""
    from core.cli.context import _artifacts_payload

    cells = [
        Cell(
            run_id=RUN_ID,
            product_id=PRODUCT_A,
            param_id="param.abs",
            state=CellState.EVALUATED,
            status=Status.ABSENT,
            absence_evidence=[
                {"source_scope": "exhaustive", "scope_attested_by": "human"}
            ],
        )
    ]
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    provenance = CrawlProvenance(
        sessions=[
            CrawlSessionProvenance(
                session_id="crawl:run-report",
                profile="local",
                status="partial",
                truncated=True,
                truncated_products=[PRODUCT_A],
            )
        ]
    )
    manifest = _manifest(crawl_provenance=provenance)

    artifacts = write_reports(
        bundle=bundle,
        cells=cells,
        catalog=_catalog(),
        manifest=manifest,
        out_dir=tmp_path,
        run_id=RUN_ID,
    )

    assert artifacts.crawl_truncation_anomalies
    assert artifacts.crawl_truncation_anomalies[0]["product_id"] == PRODUCT_A
    rows = _read_csv(artifacts.nda_questions)
    assert any(r.get("anomaly_kind") == "crawl_truncation" for r in rows)

    payload = _artifacts_payload(artifacts)
    assert payload["crawl_truncation_anomalies"][0]["product_id"] == PRODUCT_A
    assert payload["i3_anomalies"]

    # Untruncated-прогон — ни строки, ни артефакт-записи.
    clean = write_reports(
        bundle=bundle,
        cells=cells,
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path / "clean",
        run_id=RUN_ID,
    )
    assert clean.crawl_truncation_anomalies == []
    assert not any(
        r.get("anomaly_kind") == "crawl_truncation"
        for r in _read_csv(clean.nda_questions)
    )
    assert _artifacts_payload(clean)["crawl_truncation_anomalies"] == []


def test_corpus_coverage_anomaly_is_visible_in_csv_and_artifacts(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Ф0: неполное покрытие продукта видно в отчёте рядом с truncation-аномалией."""
    coverage = CorpusCoverage(
        product_id=PRODUCT_A,
        complete=False,
        expected_sources=10,
        observed_sources=6,
        blocked={"robots_disallow": 4},
        unreadable_origins=["closed.example"],
        partial_blocks={"closed.example": {"robots_disallow": 4}},
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})

    anomalies = collect_corpus_coverage_anomalies([], {PRODUCT_A: coverage})
    assert anomalies[0]["product_id"] == PRODUCT_A
    assert anomalies[0]["complete"] is False
    assert anomalies[0]["blocked"] == {"robots_disallow": 4}
    assert anomalies[0]["blocking_failures"] == {"robots_disallow": 4}
    assert anomalies[0]["unreadable_origins"] == ["closed.example"]
    assert anomalies[0]["partial_blocks"] == {
        "closed.example": {"robots_disallow": 4}
    }
    assert "closed.example" in anomalies[0]["reason"]
    assert "robots_disallow=4" in anomalies[0]["reason"]

    artifacts = write_reports(
        bundle=bundle,
        cells=[],
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path,
        run_id=RUN_ID,
        coverage={PRODUCT_A: coverage},
    )
    assert artifacts.corpus_coverage_anomalies
    rows = _read_csv(artifacts.nda_questions)
    coverage_rows = [r for r in rows if r.get("anomaly_kind") == "corpus_incomplete"]
    assert coverage_rows and "robots_disallow" in coverage_rows[0]["i3_anomaly"]
    # I12: аномалия — наблюдение; она входит в общий список аномалий.
    assert any(
        a.get("product_id") == PRODUCT_A for a in artifacts.i3_anomalies
    )


def test_corpus_coverage_anomaly_derived_from_pending_cell_without_store(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Ф0: гейт пишет аудит покрытия в ячейку — отчёт видит его без чтения БД."""
    pending = Cell(
        run_id=RUN_ID,
        product_id=PRODUCT_A,
        param_id="param.abs",
        state=CellState.PENDING,
        status=Status.ABSENT,
        review_reason="corpus_incomplete_absence",
        absence_evidence=[
            {
                "source_scope": "unknown",
                "scope_attested_by": None,
                "corpus_complete": False,
                "corpus_expected_sources": 5,
                "corpus_observed_sources": 2,
                "corpus_blocked": {"robots_disallow": 3},
                "corpus_truncated": False,
            }
        ],
    )
    anomalies = collect_corpus_coverage_anomalies([pending])
    assert len(anomalies) == 1
    assert anomalies[0]["blocked"] == {"robots_disallow": 3}
    assert anomalies[0]["blocking_failures"] == {"robots_disallow": 3}
    assert anomalies[0]["expected_sources"] == 5
    assert anomalies[0]["observed_sources"] == 2

    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    artifacts = write_reports(
        bundle=bundle,
        cells=[pending],
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path,
        run_id=RUN_ID,
    )
    rows = _read_csv(artifacts.nda_questions)
    assert any(r.get("anomaly_kind") == "corpus_incomplete" for r in rows)


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


def test_diff_boundary_alias_points_to_execution_fields() -> None:
    """Обратная совместимость: ``DIFF_BOUNDARY_FIELDS`` — алиас :data:`EXECUTION_FIELDS`."""
    assert DIFF_BOUNDARY_FIELDS == EXECUTION_FIELDS
    assert set(METHODOLOGY_FIELDS).isdisjoint(EXECUTION_FIELDS)


def test_diff_boundary_violations_are_class_labeled() -> None:
    """§13/B-7 (v2.1): причины помечаются классом ``methodology_change``/``execution_change``."""
    prev = _manifest()
    assert diff_boundary_violations(prev, prev) == []

    method = _manifest(scoring_policy_version="2.0.0")
    method_reasons = diff_boundary_violations(prev, method)
    assert method_reasons
    assert all(r.startswith("methodology_change:") for r in method_reasons)
    assert any("scoring_policy_version" in r for r in method_reasons)

    execution = _manifest(index_build_id="build.2")
    exec_reasons = diff_boundary_violations(prev, execution)
    assert exec_reasons
    assert all(r.startswith("execution_change:") for r in exec_reasons)
    assert any("index_build_id" in r for r in exec_reasons)


def test_diff_boundary_violations_on_crawl_axes() -> None:
    """Backlog B (§13): смена осей краула → ``execution_change``; идентичность → нет."""
    prev = _manifest(
        crawl_strategy="auto", crawl_rate_limit=canonical_rate_limit(
            RateLimitSpec(min_s=0.5, max_s=2.0)
        )
    )
    # Идентичные оси (в т.ч. каноническая форма) — ложного срабатывания нет.
    assert diff_boundary_violations(prev, _manifest(
        crawl_strategy="auto", crawl_rate_limit="uniform(min=0.5,max=2.0)"
    )) == []

    strategy_reasons = diff_boundary_violations(prev, _manifest(crawl_strategy="hybrid"))
    assert strategy_reasons
    assert all(r.startswith("execution_change:") for r in strategy_reasons)
    assert any("crawl_strategy" in r for r in strategy_reasons)

    rate_reasons = diff_boundary_violations(
        prev, _manifest(crawl_rate_limit="uniform(min=1.0,max=3.0)")
    )
    assert rate_reasons
    assert all(r.startswith("execution_change:") for r in rate_reasons)
    assert any("crawl_rate_limit" in r for r in rate_reasons)


def test_diff_guard_marks_execution_change_on_crawl_axis(tmp_path: Path) -> None:
    """Живой diff-guard: разная ось краула → отказ с меткой ``execution_change`` (§13)."""
    prev = _manifest(crawl_strategy="auto")
    curr = _manifest(crawl_strategy="hybrid")
    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    message = str(excinfo.value)
    assert "execution_change" in message
    assert "crawl_strategy" in message
    assert not (tmp_path / "diff.md").exists()


def test_diff_guard_marks_execution_change_on_index_build(tmp_path: Path) -> None:
    """Живой diff-guard: разный ``index_build_id`` → отказ с меткой ``execution_change``."""
    prev = _manifest(index_build_id="build.1")
    curr = _manifest(index_build_id="build.2")
    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    message = str(excinfo.value)
    assert "execution_change" in message
    assert "index_build_id" in message
    assert not (tmp_path / "diff.md").exists()


def test_diff_boundary_flags_registry_spec_digest_change() -> None:
    """D1/E2 (§13): циклы, различающиеся **только** ``registry_spec_digest``, — ``execution_change``.

    Дайджест персистится (``runs.registry_spec_digest``) и читается ``read_manifest``,
    поэтому смена спецификации реестра — реальная граница, а не декоративная.
    """
    assert "registry_spec_digest" in EXECUTION_FIELDS
    prev = _manifest(registry_spec_digest=None)
    assert diff_boundary_violations(prev, prev) == []

    curr = _manifest(registry_spec_digest="digest-e2")
    reasons = diff_boundary_violations(prev, curr)
    assert reasons
    assert all(r.startswith("execution_change:") for r in reasons)
    assert any("registry_spec_digest" in r for r in reasons)


def test_diff_guard_marks_execution_change_on_registry_spec_digest(tmp_path: Path) -> None:
    """Живой diff-guard: разный ``registry_spec_digest`` → отказ ``execution_change`` (§13)."""
    prev = _manifest(registry_spec_digest="digest-a")
    curr = _manifest(registry_spec_digest="digest-b")
    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    message = str(excinfo.value)
    assert "execution_change" in message
    assert "registry_spec_digest" in message
    assert not (tmp_path / "diff.md").exists()


def test_diff_guard_marks_methodology_change_on_major(tmp_path: Path) -> None:
    """Major ``methodology_version`` → отказ с меткой ``methodology_change``."""
    prev = _manifest(methodology_version="2.0.0")
    curr = _manifest(methodology_version="3.0.0")
    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    assert "methodology_change" in str(excinfo.value)


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
