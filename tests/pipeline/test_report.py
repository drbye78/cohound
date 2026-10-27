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

from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain import REQUIRED_EXHAUSTION_CONTOURS
from core.domain.enums import (
    AbsenceRuleType,
    CellState,
    ChangeClass,
    CrawlStrategy,
    DenialStrength,
    DocumentLevel,
    EndpointKind,
    LLMScenario,
    RankingPolicy,
    RunMode,
    StaleReason,
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
    EvidenceScope,
    LLMEvidence,
    ProtocolExhaustion,
    RateLimitSpec,
    RunConfig,
    RunManifest,
    ScenarioBinding,
    canonical_rate_limit,
)
from core.domain.scoring import Comparability, Coverage, Score, SensitivityReport
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
    collect_corpus_coverage_anomalies,
    collect_i3_anomalies,
    compact_search_effort,
    diff_boundary_violations,
    render_battle_card,
    render_diff,
    render_nda_questions,
    render_preliminary_findings,
    resolve_diff_change_class,
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
        # Вариант 2: публикуемое «нет» несёт класс силы отрицания (I2).
        return Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=state,
            status=status,
            evidence_scope=EvidenceScope.for_registry_absence(registry_ids=["reg"]),
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


def test_diff_names_serp_rounds_change_from_provenance() -> None:
    """``hound diff`` называет смену раундов SERP **значением**, а не «иная ось».

    Отдельного столбца у оси нет — объявленное число живёт в
    ``search_effort_budget_json.serp.rounds.declared``. Без этого геттера смена раундов
    (новый корпус, новый ``run_id``) диагностировалась бы общей строкой «поле не
    восстановимо из манифеста», и оператор не увидел бы, **что** изменилось.
    """
    def _effort(rounds: int) -> str:
        return json.dumps({"serp": {"rounds": {"declared": rounds, "executed": rounds}}})

    prev = _manifest(
        execution_contract_digest="ec_1", search_effort_budget_json=_effort(0)
    )
    curr = _manifest(
        execution_contract_digest="ec_2", search_effort_budget_json=_effort(2)
    )
    reasons = diff_boundary_violations(prev, curr)
    assert (
        "execution_change: execution_contract_digest: serp_rounds: 0 → 2" in reasons
    ), reasons
    # Историческая строка без канала (оба ``None``) — по-прежнему «не восстановимо».
    legacy = diff_boundary_violations(
        _manifest(execution_contract_digest="ec_1"),
        _manifest(execution_contract_digest="ec_2"),
    )
    assert any(
        "serp_rounds" in reason and "не восстановимо" in reason for reason in legacy
    ), legacy


def test_build_manifest_records_search_effort_state() -> None:
    """I23/§5: объявленный и израсходованный поисковый бюджет доезжает до манифеста.

    Колонка ``runs.search_effort_budget_json`` читалась отчётом, но не писалась никем —
    всегда ``null``. Теперь её заполняет фаза отчёта из durable-провенанса контура;
    здесь проверяется контракт ``build_manifest``: явное наблюдение сильнее строки
    прогона, а пустое наблюдение ничего не выдумывает.
    """
    run = SimpleNamespace(
        methodology_version="2.1.0",
        catalog_version="1.0.0",
        cohort_ref="cohort.yaml",
        mode=RunMode.STRICT,
        search_effort_budget_json=None,
    )
    inputs = SimpleNamespace(run=run, package=None)
    plan = SimpleNamespace(
        run_id="run-serp-effort",
        anchor=PRODUCT_A,
        modules=[],
        methodology_version="2.1.0",
        catalog_version="1.0.0",
        cohort_ref="cohort.yaml",
    )
    observed = {
        "serp": {
            "enabled": True,
            "rounds": {"declared": 2, "executed": 1, "source": "run.yaml"},
            "budget": {
                "declared_per_round": 5,
                "effective_per_product": 10,
                "used": {PRODUCT_A: 4},
                "exhausted": {PRODUCT_A: False},
            },
            "accepted": {PRODUCT_A: ["https://docs.test/a", "https://docs.test/b"]},
            "termination": {PRODUCT_A: "results_exhausted"},
            "ingest": {PRODUCT_A: {"enqueued": 2}},
            "unavailable_reason": None,
        }
    }
    manifest = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-01-01T00:00:00Z",
        search_effort=compact_search_effort(observed),
    )
    payload = json.loads(manifest.search_effort_budget_json)
    assert payload["serp"]["rounds"] == {
        "declared": 2,
        "executed": 1,
        "source": "run.yaml",
    }
    assert payload["serp"]["budget"]["used"] == {PRODUCT_A: 4}
    # Список запросов в манифест не копируется: он уже в провенансе сеанса.
    assert "queries" not in payload["serp"]
    # «Нашли» и «вошло в корпус» — разные факты, и второй назван сводкой.
    assert payload["serp"]["ingest"] == {
        PRODUCT_A: {"enqueued": 2, "dup": 0, "blacklisted": 0, "truncated": 0}
    }

    # Контур не наблюдался → поле остаётся пустым (не выдумываем нули).
    absent = build_manifest(
        inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z", search_effort=None
    )
    assert absent.search_effort_budget_json is None


def test_build_manifest_carries_execution_contract_digest() -> None:
    """09 §12.5: готовый контракт идёт в манифест как есть; без него — ``None``."""
    run = SimpleNamespace(
        case_id="case-1",
        mode=RunMode.STRICT,
        methodology_version="2.4.0",
        cohort="cohort.yaml",
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )
    inputs = SimpleNamespace(run=run, package=None)
    plan = SimpleNamespace(
        run_id="run-contract",
        anchor=PRODUCT_A,
        modules=[],
        methodology_version="2.4.0",
        catalog_version="1.0.0",
        cohort_ref="cohort.yaml",
    )

    manifest = build_manifest(
        inputs=inputs,  # type: ignore[arg-type]
        plan=plan,  # type: ignore[arg-type]
        created_at="2026-01-01T00:00:00Z",
        execution_contract_digest="ec_123",
    )
    assert manifest.execution_contract_digest == "ec_123"

    # Не передан → ``None`` (не пустая строка и не фабрикация).
    default = build_manifest(
        inputs=inputs,  # type: ignore[arg-type]
        plan=plan,  # type: ignore[arg-type]
        created_at="2026-01-01T00:00:00Z",
    )
    assert default.execution_contract_digest is None


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


def test_build_manifest_operator_document_level_wins() -> None:
    """P5: операторский ``--document-level`` виднее run/app — манифест называет его.

    Иначе провенанс расходился бы с ``crawl_config_digest``: манифест сказал бы уровень
    run/``app``-слоя, а извлекали бы другим (``full`` в публикации, ``text`` в отпечатке).
    """
    cfg = RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort-1",
        products=[PRODUCT_A],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        crawl=CrawlRunConfig(document_level=DocumentLevel.LAYOUT),
    )
    inputs = SimpleNamespace(run=cfg, package=None)
    plan = SimpleNamespace(
        run_id="run-level", anchor=PRODUCT_A, modules=[], cohort_ref="cohort-1"
    )

    # Без оператора — уровень run-слоя.
    base = build_manifest(
        inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z"  # type: ignore[arg-type]
    )
    assert base.crawl_document_level == "layout"

    # Оператор сильнее run/app-слоя (``CrawlConfig`` здесь — app-дефолт ``full``).
    operator = build_manifest(
        inputs=inputs,  # type: ignore[arg-type]
        plan=plan,  # type: ignore[arg-type]
        created_at="2026-01-01T00:00:00Z",
        crawl_app=CrawlConfig(document_level=DocumentLevel.FULL),
        operator_document_level="text",
    )
    assert operator.crawl_document_level == "text"


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
    """§5/I3 (Вариант 2): аномалия — ``❓`` **без причины**, а не без перечня контуров."""
    cells = [
        _cell(PRODUCT_A, "param.known", Status.UNKNOWN),
        _cell(PRODUCT_A, "param.legacy", Status.UNKNOWN).model_copy(
            update={"evidence_scope": None}
        ),
        _cell(PRODUCT_A, "param.missing", Status.UNKNOWN).model_copy(
            update={"evidence_scope": None}
        ),
    ]
    exhaustions = {
        # Полный протокол у ячейки с причиной: протокол — провенанс, не условие.
        (PRODUCT_A, "param.known"): ProtocolExhaustion(
            run_id=RUN_ID,
            product_id=PRODUCT_A,
            param_id="param.known",
            contours=list(REQUIRED_EXHAUSTION_CONTOURS),
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        ),
        # Неполный протокол у исторической строки без объекта: тоже не аномалия —
        # перечень контуров больше не обосновывает ``❓``.
        (PRODUCT_A, "param.legacy"): ProtocolExhaustion(
            run_id=RUN_ID,
            product_id=PRODUCT_A,
            param_id="param.legacy",
            contours=["corpus", "serp"],
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
        ),
    }
    path = render_nda_questions(
        cells, tmp_path / "nda_questions.csv", exhaustions=exhaustions
    )
    rows = _read_csv(path)
    by_key = {(r["product_id"], r["param_id"]): r for r in rows}

    assert by_key[(PRODUCT_A, "param.known")]["protocol_status"] == "complete"
    assert by_key[(PRODUCT_A, "param.known")]["i3_anomaly"] == ""

    assert by_key[(PRODUCT_A, "param.legacy")]["protocol_status"] == "complete"
    assert by_key[(PRODUCT_A, "param.legacy")]["i3_anomaly"] == ""

    assert by_key[(PRODUCT_A, "param.missing")]["protocol_status"] == "incomplete"
    assert by_key[(PRODUCT_A, "param.missing")]["i3_anomaly"]
    assert "причин" in by_key[(PRODUCT_A, "param.missing")]["i3_anomaly"]

    # I12: статус неизменен — аномалия не превращает ❓ в ❌.
    assert all(r["status"] == Status.UNKNOWN.value for r in rows)

    anomalies = collect_i3_anomalies(cells, exhaustions)
    assert {(a["product_id"], a["param_id"]) for a in anomalies} == {
        (PRODUCT_A, "param.missing")
    }


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
            absence_evidence=[{"source_scope": "exhaustive"}],
            evidence_scope=EvidenceScope(
                denial_strength=DenialStrength.BOUNDED,
                reason="конечный справочник",
                enumerated=True,
                scope_acceptance="human",
            ),
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


def test_diff_boundary_compares_contract_not_index_build_id() -> None:
    """09 §12.5: при наличии контракта ``index_build_id`` — **не** граница.

    Новая композиция при тех же условиях измерения (недельный инкремент) — разрешённый
    diff; сравнивается ``execution_contract_digest``.
    """
    prev = _manifest(index_build_id="build.1", execution_contract_digest="ec_1")
    curr = _manifest(index_build_id="build.2", execution_contract_digest="ec_1")
    assert diff_boundary_violations(prev, curr) == []


def test_diff_boundary_contract_mismatch_names_contract_field() -> None:
    """Расхождение контракта: строка дайджеста + имя различающегося поля.

    Дайджест — непрозрачная свёртка; оператор обязан видеть **что** изменилось
    (``chunker_version``), не разбирая хеш. Строка с дайджестами сохраняется как граница.
    """
    prev = _manifest(execution_contract_digest="ec_1", chunker_version="chunker.1")
    curr = _manifest(execution_contract_digest="ec_2", chunker_version="chunker.2")
    reasons = diff_boundary_violations(prev, curr)
    assert reasons[0] == "execution_change: execution_contract_digest: ec_1 → ec_2"
    assert reasons[1:] == [
        "execution_change: execution_contract_digest: chunker_version: "
        "'chunker.1' → 'chunker.2'"
    ]
    # Класс изменения не меняется: детализация — reporting-only.
    assert resolve_diff_change_class(prev, curr) is ChangeClass.EXECUTION_CHANGE


def test_diff_boundary_contract_mismatch_names_deploy_profile(tmp_path: Path) -> None:
    """``deploy_profile`` — слагаемое контракта, и ``hound diff`` его **называет**.

    ``local`` и ``split`` исполняют работу в разных зонах: в ``split`` игнорируется
    ``document_level``, делается двойной фетч и не извлекаются члены контейнеров. Значит
    вердикты могут отличаться при одинаковом ``run.yaml`` — поле обязано быть и в
    контракте, и в диагностике расхождения (иначе оператор видит смену ``run_id`` без
    причины). Поле читается с манифеста (``runs.deploy_profile``), поэтому отдельного
    геттера не требуется — это и проверяется здесь; живой ``render_diff`` подтверждает,
    что строка доходит до отказа ``hound diff``, а не остаётся в helper'е.
    """
    prev = _manifest(execution_contract_digest="ec_1", deploy_profile="local")
    curr = _manifest(execution_contract_digest="ec_2", deploy_profile="split")
    reasons = diff_boundary_violations(prev, curr)
    assert reasons[0] == "execution_change: execution_contract_digest: ec_1 → ec_2"
    assert reasons[1:] == [
        "execution_change: execution_contract_digest: deploy_profile: "
        "'local' → 'split'"
    ]
    assert resolve_diff_change_class(prev, curr) is ChangeClass.EXECUTION_CHANGE

    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    message = str(excinfo.value)
    assert "deploy_profile" in message
    assert "'local' → 'split'" in message
    assert not (tmp_path / "diff.md").exists()


def test_diff_boundary_contract_mismatch_names_fields_in_contract_order() -> None:
    """Несколько различающихся полей — все названы в порядке ``EXECUTION_CONTRACT_FIELDS``.

    Пара выбрана так, чтобы порядок контракта **отличался** от алфавитного:
    ``llm_scenarios`` идёт в кортеже раньше ``chunker_version``.
    """
    prev = _manifest(
        execution_contract_digest="ec_1",
        chunker_version="chunker.1",
        llm_scenarios=[
            ScenarioBinding(
                run_id=RUN_ID,
                scenario=LLMScenario.EVALUATOR,
                model_id="model.a",
                endpoint_kind=EndpointKind.SELF_HOST,
                temperature=0.0,
            )
        ],
    )
    curr = _manifest(
        execution_contract_digest="ec_2",
        chunker_version="chunker.2",
        llm_scenarios=[
            ScenarioBinding(
                run_id=RUN_ID,
                scenario=LLMScenario.EVALUATOR,
                model_id="model.a",
                endpoint_kind=EndpointKind.SELF_HOST,
                temperature=0.7,
            )
        ],
    )
    reasons = diff_boundary_violations(prev, curr)
    assert reasons[0] == "execution_change: execution_contract_digest: ec_1 → ec_2"
    details = reasons[1:]
    assert len(details) == 2
    assert "llm_scenarios" in details[0]
    assert "chunker_version" in details[1]
    assert resolve_diff_change_class(prev, curr) is ChangeClass.EXECUTION_CHANGE


def test_diff_boundary_contract_mismatch_states_diagnostic_limit() -> None:
    """Дайджесты разные, доступные поля совпадают → предел диагностики, без выдумки имён.

    ``cohort_digest``/``seeds_digest``/``extractor_stack``/``prompt_template`` входят в
    контракт, но на ``RunManifest`` их нет (только ``cohort_ref``; ни стека извлечения, ни
    шаблона промпта манифест не несёт): причина не восстановима и **не** выдумывается (A3).
    ``prompt_template`` попадает сюда сознательно: восстановить применённый шаблон из
    манифеста нельзя, а объявить на манифесте текущий значило бы соврать про прогон,
    сделанный другим шаблоном.
    """
    prev = _manifest(execution_contract_digest="ec_1")
    curr = _manifest(execution_contract_digest="ec_2")
    reasons = diff_boundary_violations(prev, curr)
    assert reasons == [
        "execution_change: execution_contract_digest: ec_1 → ec_2",
        "execution_change: execution_contract_digest: различающееся поле не "
        "восстановимо из манифеста (cohort_digest/seeds_digest/extractor_stack/"
        "serp_rounds/prompt_template/chunking_spec/evaluation_policy/lexical_spec/render_modes_digest/passports_digest либо иная ось)",
    ]
    assert not any("chunker_version" in reason for reason in reasons)
    assert not any("embed_model_id" in reason for reason in reasons)
    assert resolve_diff_change_class(prev, curr) is ChangeClass.EXECUTION_CHANGE


def test_diff_boundary_contract_mismatch_names_contract_field_without_manifest_row_dump() -> None:
    """Поле контракта названо и при ином имени на манифесте, без дампа ``EXECUTION_FIELDS``.

    ``retrieval_cutoff_K`` — манифестное имя ``cutoff_K``: детализация называет поле
    **контракта**, а legacy-строка ``execution_change: retrieval_cutoff_K`` не дублируется.
    """
    prev = _manifest(execution_contract_digest="ec_1", retrieval_cutoff_K=10)
    curr = _manifest(execution_contract_digest="ec_2", retrieval_cutoff_K=25)
    reasons = diff_boundary_violations(prev, curr)
    assert reasons == [
        "execution_change: execution_contract_digest: ec_1 → ec_2",
        "execution_change: execution_contract_digest: cutoff_K: 10 → 25",
    ]
    assert not any(reason.startswith("execution_change: retrieval_cutoff_K:") for reason in reasons)
    assert resolve_diff_change_class(prev, curr) is ChangeClass.EXECUTION_CHANGE


def test_diff_boundary_names_cohort_change_instead_of_contract_mismatch() -> None:
    """Состав когорты различим: ``cohort_expansion``/``cohort_reduction`` вместо контракта."""
    prev = _manifest(execution_contract_digest="ec_1")
    curr = _manifest(execution_contract_digest="ec_2")

    grown = diff_boundary_violations(prev, curr, prev_products=["p1"], curr_products=["p1", "p2"])
    assert grown == ["execution_change: cohort_expansion: p2"]

    shrunk = diff_boundary_violations(prev, curr, prev_products=["p1", "p2"], curr_products=["p1"])
    assert shrunk == ["execution_change: cohort_reduction: p2"]

    # Состав неизвестен — набор не выдумывается, причина остаётся общей (с детализацией).
    assert diff_boundary_violations(prev, curr) == [
        "execution_change: execution_contract_digest: ec_1 → ec_2",
        "execution_change: execution_contract_digest: различающееся поле не "
        "восстановимо из манифеста (cohort_digest/seeds_digest/extractor_stack/"
        "serp_rounds/prompt_template/chunking_spec/evaluation_policy/lexical_spec/render_modes_digest/passports_digest либо иная ось)",
    ]


def test_diff_boundary_legacy_pair_keeps_index_build_id_boundary() -> None:
    """Legacy-пара (дайджест ``None``) — прежнее поведение, включая ``index_build_id``."""
    prev = _manifest(index_build_id="build.1")
    curr = _manifest(index_build_id="build.2")
    reasons = diff_boundary_violations(prev, curr)
    assert reasons
    assert all(r.startswith("execution_change:") for r in reasons)
    assert any("index_build_id" in r for r in reasons)


def test_diff_boundary_legacy_pair_keeps_row_by_row_output() -> None:
    """Legacy-ветка не задета: построчное сравнение ``EXECUTION_FIELDS`` байт-в-байт."""
    prev = _manifest(chunker_version="chunker.1", embed_model_id="embed.1")
    curr = _manifest(chunker_version="chunker.2", embed_model_id="embed.1")
    assert diff_boundary_violations(prev, curr) == [
        "execution_change: chunker_version: 'chunker.1' → 'chunker.2'"
    ]


def test_resolve_diff_change_class_content_change() -> None:
    """``content_change`` — класс **времени diff** (контракт тот же, композиция новая)."""
    prev = _manifest(index_build_id="build.1", execution_contract_digest="ec_1")
    curr = _manifest(index_build_id="build.2", execution_contract_digest="ec_1")
    assert resolve_diff_change_class(prev, curr) is ChangeClass.CONTENT_CHANGE

    same = _manifest(index_build_id="build.1", execution_contract_digest="ec_1")
    assert resolve_diff_change_class(prev, same) is ChangeClass.NONE

    # ``None`` с одной стороны — тоже изменение контента (композиция неизвестна).
    assert (
        resolve_diff_change_class(_manifest(execution_contract_digest="ec_1"), curr)
        is ChangeClass.CONTENT_CHANGE
    )


def test_resolve_diff_change_class_execution_and_cohort() -> None:
    """Контракт различается → ``execution_change``; известный состав — ``cohort_*``."""
    prev = _manifest(execution_contract_digest="ec_1")
    curr = _manifest(execution_contract_digest="ec_2")
    assert resolve_diff_change_class(prev, curr) is ChangeClass.EXECUTION_CHANGE
    assert (
        resolve_diff_change_class(prev, curr, prev_products=["p1"], curr_products=["p1", "p2"])
        is ChangeClass.COHORT_EXPANSION
    )
    assert (
        resolve_diff_change_class(prev, curr, prev_products=["p1", "p2"], curr_products=["p1"])
        is ChangeClass.COHORT_REDUCTION
    )


def test_resolve_diff_change_class_legacy_never_claims_content_change() -> None:
    """Legacy-пара: ``content_change`` недоказуем — расхождение полей даёт ``execution_change``."""
    prev = _manifest(index_build_id="build.1")
    curr = _manifest(index_build_id="build.2")
    assert resolve_diff_change_class(prev, curr) is ChangeClass.EXECUTION_CHANGE
    assert resolve_diff_change_class(prev, prev) is ChangeClass.NONE


def test_render_diff_content_change_banner_and_delta(tmp_path: Path) -> None:
    """Баннер ``content_change`` + per-product doc-delta (сортировка по продукту)."""
    prev = _manifest(run_id="run-prev", index_build_id="build.1", execution_contract_digest="ec_1")
    curr = _manifest(index_build_id="build.2", execution_contract_digest="ec_1")
    path = render_diff(
        prev,
        curr,
        [],
        [],
        tmp_path / "diff.md",
        doc_delta={
            "p2": {"new": 0, "reused": 4, "removed": 2},
            "p1": {"new": 3, "reused": 1, "removed": 0},
        },
    )
    text = path.read_text(encoding="utf-8")
    assert "content_change" in text
    assert "| product | new | reused | removed |" in text
    # Детерминированно: p1 раньше p2.
    assert text.index("| p1 |") < text.index("| p2 |")
    assert "| p1 | 3 | 1 | 0 |" in text
    assert "| p2 | 0 | 4 | 2 |" in text


def test_render_diff_no_content_change_banner_when_index_unchanged(tmp_path: Path) -> None:
    """Одинаковые контракт и композиция → баннера ``content_change`` нет."""
    prev = _manifest(run_id="run-prev", execution_contract_digest="ec_1")
    curr = _manifest(execution_contract_digest="ec_1")
    path = render_diff(prev, curr, [], [], tmp_path / "diff.md")
    text = path.read_text(encoding="utf-8")
    assert "content_change" not in text
    assert "| product | new | reused | removed |" not in text


def test_diff_minor_methodology_bump_is_methodology_change(tmp_path: Path) -> None:
    """§13: смена ``methodology_version`` (minor/patch) — ``methodology_change``, не execution.

    ``methodology_version`` входит в ``EXECUTION_CONTRACT_FIELDS``, поэтому без явной
    классификации 2.2.0 → 2.4.0 читалось бы как «изменились условия измерения». Причина
    обязана называть версию и **не** содержать общую строку контракта.
    """
    prev = _manifest(methodology_version="2.2.0", execution_contract_digest="ec_1")
    curr = _manifest(methodology_version="2.4.0", execution_contract_digest="ec_1")
    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    message = str(excinfo.value)
    reason = message.split("— ", 1)[-1]
    assert reason.startswith("methodology_change: methodology_version:")
    assert "2.2.0 → 2.4.0" in reason
    assert "execution_contract_digest" not in message
    assert not (tmp_path / "diff.md").exists()


def test_diff_minor_methodology_bump_suppresses_generic_contract_line() -> None:
    """Контракт тоже различается (version входит в него) — общая строка не дублируется."""
    prev = _manifest(methodology_version="2.2.0", execution_contract_digest="ec_1")
    curr = _manifest(methodology_version="2.4.0", execution_contract_digest="ec_2")
    assert diff_boundary_violations(prev, curr) == [
        "methodology_change: methodology_version: 2.2.0 → 2.4.0"
    ]


def test_resolve_diff_change_class_minor_methodology_bump() -> None:
    """Классификатор diff повторяет приоритет границы: версия раньше контракта."""
    prev = _manifest(methodology_version="2.2.0", execution_contract_digest="ec_1")
    curr = _manifest(methodology_version="2.4.0", execution_contract_digest="ec_2")
    assert resolve_diff_change_class(prev, curr) is ChangeClass.METHODOLOGY_CHANGE


def test_diff_guard_methodology_major_with_contracts(tmp_path: Path) -> None:
    """Major ``methodology_version`` остаётся ``methodology_change`` и при контрактах."""
    prev = _manifest(methodology_version="2.0.0", execution_contract_digest="ec_1")
    curr = _manifest(methodology_version="3.0.0", execution_contract_digest="ec_2")
    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    assert "methodology_change" in str(excinfo.value)
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


def test_max_response_bytes_is_execution_axis_and_changes_digest() -> None:
    """P0: ось ``crawl.max_response_bytes`` — ``execution_change``, не methodology."""
    cfg = RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort-1",
        products=[PRODUCT_A],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    plan = SimpleNamespace(
        run_id="run-response-bytes", anchor=PRODUCT_A, modules=[], cohort_ref="cohort-1"
    )
    base = build_manifest(
        inputs=SimpleNamespace(run=cfg, package=None),  # type: ignore[arg-type]
        plan=plan,
        created_at="2026-01-01T00:00:00Z",  # type: ignore[arg-type]
    )
    assert "crawl_max_response_bytes" in EXECUTION_FIELDS
    assert base.crawl_max_response_bytes is None

    cfg2 = cfg.model_copy(
        update={"crawl": CrawlRunConfig(max_response_bytes=1_048_576)}
    )
    changed = build_manifest(
        inputs=SimpleNamespace(run=cfg2, package=None),  # type: ignore[arg-type]
        plan=plan,
        created_at="2026-01-01T00:00:00Z",  # type: ignore[arg-type]
    )
    assert changed.crawl_max_response_bytes == 1_048_576
    assert changed.crawl_config_digest != base.crawl_config_digest
    assert any(
        v.startswith("execution_change: crawl_max_response_bytes")
        for v in diff_boundary_violations(base, changed)
    )


def test_build_manifest_uses_observed_versions_from_index_build() -> None:
    """Resume: наблюдаемые версии прибора берутся из манифеста сборки, а не теряются.

    Манифест — запись воспроизведения. Пока версии приходили только из ``run``/``inputs``,
    прогон, дошедший до отчёта в другом процессе (``corpus`` короткозамкнут персистентным
    ``ok``), публиковал ``embed_model_id: null`` при 9398 векторах, построенных bge-m3:
    отчёт молчал о приборе, которым сделано измерение.
    """
    run = SimpleNamespace(
        case_id="case-1",
        mode=RunMode.STRICT,
        methodology_version="2.4.0",
        cohort="cohort.yaml",
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )
    inputs = SimpleNamespace(run=run, package=None)
    plan = SimpleNamespace(
        run_id="run-observed",
        anchor=PRODUCT_A,
        modules=[],
        methodology_version="2.4.0",
        catalog_version="1.0.0",
        cohort_ref="cohort.yaml",
    )

    bare = build_manifest(inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z")
    assert bare.embed_model_id is None

    observed = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-01-01T00:00:00Z",
        observed_versions={
            "embed_model_id": "BAAI/bge-m3",
            "extractor_version": "docling-doc-3:layout_tables/docling-2.135.0",
            "lexical_engine_version": "tantivy-0.26.2",
            "vector_store": "npy-fp16",
        },
    )
    assert observed.embed_model_id == "BAAI/bge-m3"
    assert observed.extractor_version.startswith("docling-doc-3:layout_tables")
    assert observed.lexical_engine_version == "tantivy-0.26.2"
    assert observed.vector_store == "npy-fp16"

    # Живое наблюдение приоритетнее: ``run`` описывает то, что реально работало в процессе.
    run.embed_model_id = "observed-live-model"
    live = build_manifest(
        inputs=inputs,
        plan=plan,
        created_at="2026-01-01T00:00:00Z",
        observed_versions={"embed_model_id": "BAAI/bge-m3"},
    )
    assert live.embed_model_id == "observed-live-model"

# ---------------------------------------------------------------------------
# §13/K2: «требует пере-оценки» — устаревшие ячейки видны, а не вычтены молча
# ---------------------------------------------------------------------------

def test_write_reports_lists_stale_cells_for_re_evaluation(tmp_path: Path) -> None:
    """Ячейки со ``stale_reason`` исключены из расчёта, но обязаны быть видны человеку.

    Исключение без артефакта было бы тихим вычитанием: цифры «похудели», а работы
    (пере-оценка на новой сборке) не видно. Файл ``outputs/stale_cells.csv`` и
    ``pending``-строка называют её прямо.
    """
    store = SqliteStore(":memory:")
    store.create_run(RUN_ID, _cfg())
    cells = [
        _cell(PRODUCT_A, "param.ok", Status.OK),
        _cell(PRODUCT_A, "param.stale", Status.OK).model_copy(
            update={"stale_reason": StaleReason.NEW_BUILD, "index_build_id": "build.0"}
        ),
    ]
    _save(store, cells)
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    assert bundle.stale_cells  # расчёт уже исключил её

    artifacts = write_reports(
        bundle=bundle,
        cells=list(store.iter_cells(RUN_ID)),
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path,
        run_id=RUN_ID,
    )
    assert artifacts.stale_cells is not None
    rows = _read_csv(artifacts.stale_cells)
    assert [(r["product_id"], r["param_id"], r["stale_reason"]) for r in rows] == [
        (PRODUCT_A, "param.stale", "new_build")
    ]

    # Устаревших нет — артефакт не создаётся (пустой список — не артефакт).
    clean_cells = [_cell(PRODUCT_A, "param.ok", Status.OK)]
    clean_store = SqliteStore(":memory:")
    clean_store.create_run(RUN_ID, _cfg())
    _save(clean_store, clean_cells)
    clean_bundle = compute(
        store=clean_store, run_id=RUN_ID, catalog=_catalog(), module_weights={}
    )
    clean = write_reports(
        bundle=clean_bundle,
        cells=clean_cells,
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path / "clean",
        run_id=RUN_ID,
    )
    assert clean.stale_cells is None
    store.close()
    clean_store.close()


# ---------------------------------------------------------------------------
# P6/absence_rule/validation_notes: записи ячейки обязаны быть видны человеку
# ---------------------------------------------------------------------------

def test_write_cell_diagnostics_lists_each_reason(tmp_path: Path) -> None:
    """``outputs/cell_diagnostics.csv``: три новые записи ячейки не остаются невидимыми.

    До этого файла ``cells.validation_notes`` (почему запись не удалась/что было при
    персисте), ``cells.absence_rule`` (правило отсутствия не объявлено) и
    ``review_reason`` (причина ``pending``) лежали в БД и не попадали ни в один артефакт:
    человек видел последствие, не видя записи. Каждый повод — отдельная строка; пустые
    значения печатаются пустыми, а не нулём («не измерено» ≠ 0).
    """
    from core.pipeline.report import _write_cell_diagnostics

    with_scope = _cell(PRODUCT_A, "param.ok", Status.OK).model_copy(
        update={
            "validation_notes": ["цитата не найдена в чанке", "повтор после resume"],
            "evidence_scope": EvidenceScope(
                denial_strength=DenialStrength.BOUNDED,
                reason="справочник с объявленной процедурой полноты",
                observed=3,
                expected=4,
                relevant_evidence_coverage=2 / 3,
                scope_acceptance="reviewer",
            ),
        }
    )
    no_rule = _cell(PRODUCT_A, "param.norule", Status.OK)  # absence_rule=None
    review = _cell(PRODUCT_A, "param.pending", Status.UNKNOWN).model_copy(
        update={"review_reason": "конфликт источников"}
    )
    # Смешанный прогон: правило записано хотя бы у одной ячейки, поэтому незаписанное
    # правило у ``param.norule`` — аномалия именно этой ячейки.
    with_rule = _cell(PRODUCT_A, "param.rule", Status.OK).model_copy(
        update={"absence_rule": AbsenceRuleType.EXPLICIT}
    )
    path = tmp_path / "cell_diagnostics.csv"

    written = _write_cell_diagnostics([with_scope, review, no_rule, with_rule], path)

    assert written == path
    rows = {row["param_id"]: row for row in _read_csv(path)}
    # ``param.rule`` не попал: у него нет ни одной записи, а правило объявлено.
    assert set(rows) == {"param.ok", "param.norule", "param.pending"}
    assert rows["param.ok"]["validation_notes"] == (
        "цитата не найдена в чанке | повтор после resume"
    )
    assert rows["param.ok"]["denial_strength"] == "bounded"
    assert rows["param.ok"]["relevant_evidence_coverage"] == "0.667"
    # Правило не записано — пусто, а не подставленное «explicit».
    assert rows["param.norule"]["absence_rule"] == ""
    assert rows["param.norule"]["relevant_evidence_coverage"] == ""  # не измерено ≠ 0
    assert rows["param.pending"]["review_reason"] == "конфликт источников"


def test_write_cell_diagnostics_absent_when_nothing_to_report(tmp_path: Path) -> None:
    """Полное отсутствие поводов ⇒ ``None``: файл не создаётся.

    Пустой список — не артефакт: пустой ``cell_diagnostics.csv`` читался бы как
    «проверено, замечаний нет», хотя проверять было нечего.
    """
    from core.pipeline.report import _write_cell_diagnostics

    clean = [
        _cell(PRODUCT_A, "param.ok", Status.OK).model_copy(
            update={"absence_rule": AbsenceRuleType.EXPLICIT}
        ),
        _cell(PRODUCT_B, "param.ok", Status.OK).model_copy(
            update={"absence_rule": AbsenceRuleType.EXPLICIT}
        ),
    ]
    path = tmp_path / "cell_diagnostics.csv"

    assert _write_cell_diagnostics(clean, path) is None
    assert not path.exists()


def test_write_cell_diagnostics_reports_total_legacy_as_one_row(tmp_path: Path) -> None:
    """Тотальное «правило не записано» — свойство прогона, а не N задач.

    Живой факт: прогон, сделанный до появления колонки, давал 476 строк — по строке на
    каждую ячейку с единственным общим поводом. 476 здесь — число ячеек, а не число
    находок, и очередь разбора вырождалась в полный список. Тотальное отсутствие правила
    публикуется одной строкой ``kind=run``; выдумывать правило из каталога нельзя —
    объявление и факт измерения разные вещи.
    """
    from core.pipeline.report import _write_cell_diagnostics

    legacy = [_cell(PRODUCT_A, f"param.{i}", Status.OK) for i in range(5)]
    path = tmp_path / "cell_diagnostics.csv"

    written = _write_cell_diagnostics(legacy, path)

    assert written == path
    rows = _read_csv(path)
    assert len(rows) == 1
    assert rows[0]["kind"] == "run"
    assert rows[0]["param_id"] == ""
    assert "не записан ни у одной из 5 ячеек" in rows[0]["validation_notes"]

def test_registry_denial_is_not_a_truncation_anomaly() -> None:
    """Авторитетная реестровая пустота не зависит от полноты краула (Вариант 2).

    Детектор ``collect_crawl_truncation_anomalies`` ищет противоречие «заявлен
    исчерпывающий охват, а краул усечён политикой». Реестровое ``❌`` такого заявления не
    делает: реестр — авторитетная энумерация, и усечение краула её не опровергает. Прежде
    детектор ключевался на устаревшем скаляре ``source_scope=exhaustive``, который
    ``registry.py`` продолжает писать, поэтому каждый реестровый ``❌`` усечённого продукта
    давал **ложную** аномалию (проверено вызовом до правки: 1 из 1).

    Под правилом остаётся ``bounded`` — единственный класс, чья сила именно в объявленной
    полноте, — и legacy-строки без объекта силы отрицания.
    """
    from core.domain import CatalogRow
    from core.domain.enums import AbsenceRuleType, DenialStrength, Importance, ParamType
    from core.domain.models import EvidenceScope
    from core.pipeline.registry import _absence_cell
    from core.pipeline.report import collect_crawl_truncation_anomalies

    row = CatalogRow(
        param_id="p1",
        title="t",
        module="m",
        group="g",
        importance=next(iter(Importance)),
        param_type=ParamType.FACT,
        rule="r",
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
    )
    registry_cell = _absence_cell("r1", "prod", row, ["reg-1"])
    assert registry_cell.evidence_scope is not None
    assert registry_cell.evidence_scope.denial_strength is DenialStrength.AUTHORITATIVE

    # Свидетельство о факте: усечение краула его не опровергает.
    assert collect_crawl_truncation_anomalies([registry_cell], truncated_products={"prod"}) == []

    explicit_cell = Cell(
        run_id="r1",
        product_id="prod",
        param_id="p2",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.EXPLICIT,
        evidence_scope=EvidenceScope(
            denial_strength=DenialStrength.EXPLICIT,
            citation_quote="функция не поддерживается",
            reason="цитата",
        ),
    )
    assert collect_crawl_truncation_anomalies([explicit_cell], truncated_products={"prod"}) == []

    # ``bounded`` — заявление о полноте, поэтому аномалия остаётся и требует человека.
    bounded_cell = Cell(
        run_id="r1",
        product_id="prod",
        param_id="p3",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_rule=AbsenceRuleType.DISTRIBUTION_ABSENCE,
        evidence_scope=EvidenceScope(
            denial_strength=DenialStrength.BOUNDED,
            reason="конечный справочник",
            scope_acceptance="Roger",
        ),
    )
    anomalies = collect_crawl_truncation_anomalies([bounded_cell], truncated_products={"prod"})
    assert len(anomalies) == 1
    assert "bounded" in anomalies[0]["reason"]
