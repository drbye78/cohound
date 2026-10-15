"""Тесты сборки фаз конвейера (L2, W4/W13): ``build_phases``/``evaluate_phase``/``build_manifest``.

Покрывают:

* ``build_phases`` возвращает замыкания ровно для не-гейтовых фаз и **не** переопределяет
  ``g1``–``g4`` (S9);
* каждое замыкание возвращает ``PhaseResult`` с именем фазы; registry/inheritance/corpus/
  evaluate/compute/report проходятся на ``SqliteStore(":memory:")`` + фейках портов;
* ``evaluate_phase`` использует ретривал (I16/I17/I20), не вызывает внешний поиск (I18) и
  честно падает без оценщика;
* ``build_manifest`` детерминирован, несёт обязательный провенанс и не сбрасывает
  ``methodology_version`` в ``0.0.0``, если значение есть на входе;
* модуль ``phases.py`` не импортирует ``core.adapters``/``core.edge`` (AST, матрица 01 §1).

Фикстуры категорийно нейтральны (§1.3); сеть/диск не используются, кроме ``tmp_path`` для
артефактов отчёта.
"""

from __future__ import annotations

import ast
import datetime as _dt
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    Archetype,
    BaselineRow,
    CatalogRow,
    Cell,
    CellState,
    Citation,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    CrawlStrategy,
    Importance,
    InheritanceMode,
    LLMCellOutput,
    LLMEvidence,
    METHODOLOGY_VERSION,
    ModuleDef,
    PackageManifest,
    ParamType,
    Passport,
    RankingPolicy,
    RateLimitSpec,
    RoleDef,
    RunConfig,
    RunMode,
    SkipReason,
    Status,
    Upstream,
)
from core.config.crawl_sources import CrawlSource, CrawlSourcesConfig
from core.pipeline.compute import compute
from core.pipeline.evaluate import evaluate_phase
from core.pipeline.normalize import RunInputs
from core.pipeline.phases import IMPLEMENTED_PHASES, build_phases
from core.pipeline.plan import RunPlan
from core.pipeline.report import build_manifest
from core.pipeline.runner import (
    GATE_PHASES,
    PHASES,
    PhaseResult,
    Ports,
    RunContext,
    Runner,
)
from core.ports.types import FetchPlan, RawPage
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore
from core.util.rate_limit import RateLimiter
from tests.fakes.ports import (
    FakeChunker,
    FakeCrawler,
    FakeEmbedder,
    FakeEvaluator,
    FakeExtractor,
    FakeIndexBuilder,
    FakeLexicalIndex,
    FakeRegistryFetcher,
)

RUN_ID = "run-phases"
CASE_ID = "case-1"
FIXED_TS = "2026-01-01T00:00:00+00:00"
REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Фикстуры (нейтральные, §1.3)
# ---------------------------------------------------------------------------


def _run_config(**overrides: Any) -> RunConfig:
    data: dict[str, Any] = {
        "case_id": CASE_ID,
        "anchor": "p1",
        "cohort": "cohort.yaml",
        "products": ["p1", "p2"],
        "package": "pkg-1",
        "languages": ["ru"],
        "snapshot_date": _dt.date(2026, 1, 1),
        "mode": RunMode.STRICT,
        "ranking_policy": RankingPolicy.CAPABILITY_RANKED,
    }
    data.update(overrides)
    return RunConfig(**data)


def _package() -> PackageManifest:
    return PackageManifest(
        case_id=CASE_ID,
        package_version="1.2.3",
        archetype=Archetype.B,
        modules=[ModuleDef(module_id="mod_a", weight=100.0)],
        roles=[RoleDef(role_id="role_a", definition="Роль А")],
        baselines=["base_1"],
    )


def _catalog() -> list[CatalogRow]:
    return [
        CatalogRow(
            param_id="param_a",
            title="Параметр А",
            module="mod_a",
            group="grp_1",
            importance=Importance.CRITICAL,
            param_type=ParamType.FACT,
            rule="Подтвердить свойство А.",
            absence_rule=AbsenceRuleType.EXPLICIT,
            role_binding="role_a",
            concept_terms={"ru": ["свойство а"], "en": ["feature a"]},
            search_langs=["ru", "en"],
        )
    ]


def _baselines() -> list[BaselineRow]:
    return [
        BaselineRow(
            baseline_id="base_1",
            param_id="param_a",
            inheritance_mode=InheritanceMode.DIRECT,
        )
    ]


def _cohort() -> Cohort:
    return Cohort(
        case_id=CASE_ID,
        frozen_at=_dt.date(2026, 1, 1),
        entries=[
            CohortEntry(
                product_id="p1",
                role_overlap=1.0,
                selection_reason="anchor",
                anchor_relation=AnchorRelation.ANCHOR,
            ),
            CohortEntry(
                product_id="p2",
                role_overlap=0.5,
                selection_reason="analog",
                anchor_relation=AnchorRelation.ANALOG,
            ),
        ],
    )


def _inputs() -> RunInputs:
    return RunInputs(
        run=_run_config(),
        package=_package(),
        catalog=_catalog(),
        baselines=_baselines(),
        cohort=_cohort(),
        golden=[],
        domain_profiles={},
        scope_attestation_procedure=None,
    )


def _plan(products: tuple[str, ...] = ("p1", "p2")) -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=_catalog(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=_cohort(),
        golden=[],
        warnings=[],
    )


def _passports() -> dict[str, Passport]:
    return {
        "p1": Passport(
            product_id="p1",
            canonical_name="Product 1",
            vendor="Vendor",
            jurisdiction="RU",
            snapshot_date=_dt.date(2026, 1, 1),
            upstream=Upstream(
                baseline_id="base_1",
                version="1",
                evidence=Citation(url="https://up.test/p1", quote="upstream quote"),
            ),
        )
    }


class _Clock:
    """Фиксированные часы (детерминизм манифеста)."""

    def now_iso(self) -> str:
        return FIXED_TS


def _context(store: SqliteStore, ports: Ports) -> RunContext:
    return RunContext(run_id=RUN_ID, store=store, ports=ports, clock=_Clock())


def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(RUN_ID, _run_config())
    return store


# --- порты-заглушки ---------------------------------------------------------


class _BombSearch:
    """Адаптер-ловушка: любой вызов доказывает нарушение I18 (оценщик не ищет)."""

    adapter_kind = "serp"
    provider = "bomb"

    def search(self, q: Any) -> list[Any]:  # pragma: no cover - не должен вызываться
        raise AssertionError("evaluate_phase не должен вызывать внешний поиск (I18)")

    def fetch(self, url: str) -> RawPage:  # pragma: no cover
        raise AssertionError("evaluate_phase не должен фетчить (I18)")


def _ok_output() -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Подтверждено.",
        evidence=[LLMEvidence(source_id="chunk_a1", quote="Свойство А доступно.")],
    )


def _chunks_index() -> FakeLexicalIndex:
    return FakeLexicalIndex(
        docs=[
            RetrievedChunk(
                chunk_id="chunk_a1",
                product_id="p1",
                text="Свойство А доступно.",
                content_hash="hash_a1",
                heading_path="",
                build_id="ib_x",
                score=1.0,
            )
        ]
    )


# ---------------------------------------------------------------------------
# build_phases: состав реестра
# ---------------------------------------------------------------------------


def test_build_phases_implements_non_gate_phases_only() -> None:
    phases = build_phases(inputs=_inputs(), plan=_plan(), ports=Ports())
    assert set(phases) == set(IMPLEMENTED_PHASES)
    assert set(phases).isdisjoint(GATE_PHASES)  # гейты не переопределяются (S9)
    assert set(phases) <= set(PHASES)
    assert all(callable(fn) for fn in phases.values())


def test_gate_phases_still_block_when_phases_registered() -> None:
    """Регистрация build_phases не обходит гейт: без решения прогон стоит на g1 (S9)."""
    store = _store()
    runner = Runner(store, Ports(), clock=_Clock(), phases=build_phases(
        inputs=_inputs(), plan=_plan(), ports=Ports()
    ))
    report = runner.run(RUN_ID, until="g1")
    assert report.stopped_at == "g1"
    assert report.status == "blocked"


# ---------------------------------------------------------------------------
# Каждое замыкание возвращает PhaseResult
# ---------------------------------------------------------------------------


def test_every_closure_returns_phase_result(tmp_path: Path) -> None:
    inputs = _inputs()
    plan = _plan()
    phases = build_phases(inputs=inputs, plan=plan, ports=Ports(), out_dir=str(tmp_path))
    store = _store()
    ctx = _context(store, Ports())
    for name, fn in phases.items():
        result = fn(ctx)
        assert isinstance(result, PhaseResult), name
        assert result.phase == name


# ---------------------------------------------------------------------------
# registry / inheritance / corpus / evaluate / compute / report
# ---------------------------------------------------------------------------


def test_registry_phase_adapts_single_port_to_mapping() -> None:
    inputs = _inputs()
    plan = _plan()
    ports = Ports(registry=FakeRegistryFetcher(registry_id="reg_x", run_id=RUN_ID))
    store = _store()
    result = build_phases(inputs=inputs, plan=plan, ports=ports)["registry"](_context(store, ports))
    assert result.status == "ok"
    assert result.cells and {c.product_id for c in result.cells} == {"p1", "p2"}
    # R1/Ш3: ячейки реестровые (адаптация одного порта в mapping сработала).
    assert all(c.source_kind is not None and c.source_kind.value == "registry" for c in result.cells)


def test_inheritance_phase_uses_baselines_and_passports() -> None:
    inputs = _inputs()
    plan = _plan()
    ports = Ports()
    store = _store()
    result = build_phases(
        inputs=inputs, plan=plan, ports=ports, passports=_passports()
    )["inheritance"](_context(store, ports))
    assert result.status == "ok"
    inherited = [c for c in result.cells if c.product_id == "p1"]
    assert inherited and inherited[0].status is Status.OK
    # p2 без паспорта — изолированная ошибка, но волна завершилась ok.
    assert any("p2" in err for err in result.errors)


def test_corpus_phase_builds_and_captures_index_build_id(tmp_path: Path) -> None:
    inputs = _inputs()
    plan = _plan()
    pages = {
        "https://docs.test/p1": RawPage(
            url="https://docs.test/p1",
            content="Свойство А доступно.".encode(),
            fetched_at=FIXED_TS,
        ),
        "https://docs.test/p2": RawPage(
            url="https://docs.test/p2",
            content="Документ продукта два.".encode(),
            fetched_at=FIXED_TS,
        ),
    }
    ports = Ports(
        crawler=FakeCrawler(pages=pages),
        extractor=FakeExtractor(),
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        index_builder=FakeIndexBuilder(),
    )
    store = _store()
    result = build_phases(
        inputs=inputs,
        plan=plan,
        ports=ports,
        urls={"p1": ["https://docs.test/p1"], "p2": ["https://docs.test/p2"]},
        raw_root=str(tmp_path / "raw"),
    )["corpus"](_context(store, ports))
    assert result.status == "ok"
    assert len(result.log_entries) == 1
    build = result.log_entries[0]
    assert build.index_build_id.startswith("ib_")
    assert build.chunk_count == 2


def test_corpus_phase_forwards_run_config_crawl(monkeypatch: pytest.MonkeyPatch) -> None:
    """``RunConfig.crawl`` (run.yaml ``crawl:``) доезжает до ``build_corpus`` (P2-3).

    Замыкание ``corpus_phase`` обязано прокинуть секцию ``crawl`` как ``crawl_defaults``:
    иначе ``run.yaml: crawl:`` — тихий no-op.
    """
    import core.pipeline.phases as phases_mod

    captured: dict[str, Any] = {}

    def _spy(*args: Any, **kwargs: Any) -> PhaseResult:
        captured.update(kwargs)
        return PhaseResult(status="ok", phase="corpus")

    monkeypatch.setattr(phases_mod, "build_corpus", _spy)

    crawl = CrawlRunConfig(
        strategy=CrawlStrategy.HYBRID, rate_limit=RateLimitSpec(min_s=1.0, max_s=2.0)
    )
    inputs = replace(_inputs(), run=_run_config(crawl=crawl))
    store = _store()
    try:
        phases = build_phases(inputs=inputs, plan=_plan(), ports=Ports())
        result = phases["corpus"](_context(store, Ports()))
    finally:
        store.close()

    assert result.status == "ok"
    assert captured["crawl_defaults"] == crawl


def test_corpus_phase_crawl_defaults_none_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без секции ``crawl`` в run.yaml ``crawl_defaults is None`` (поведение неизменно)."""
    import core.pipeline.phases as phases_mod

    captured: dict[str, Any] = {}

    def _spy(*args: Any, **kwargs: Any) -> PhaseResult:
        captured.update(kwargs)
        return PhaseResult(status="ok", phase="corpus")

    monkeypatch.setattr(phases_mod, "build_corpus", _spy)

    store = _store()
    try:
        phases = build_phases(inputs=_inputs(), plan=_plan(), ports=Ports())
        phases["corpus"](_context(store, Ports()))
    finally:
        store.close()

    assert captured["crawl_defaults"] is None


# ---------------------------------------------------------------------------
# Lane A/L3a: проброс crawl_sources/crawlers/rate_limiter в build_corpus
# ---------------------------------------------------------------------------


def _forwards_spy(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Подменяет ``build_corpus`` шпионом, собирающим переданные kwargs."""
    import core.pipeline.phases as phases_mod

    captured: dict[str, Any] = {}

    def _spy(*args: Any, **kwargs: Any) -> PhaseResult:
        captured.update(kwargs)
        return PhaseResult(status="ok", phase="corpus")

    monkeypatch.setattr(phases_mod, "build_corpus", _spy)
    return captured


def test_corpus_phase_forwards_crawl_sources_crawlers_rate_limiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``crawl_sources``/``crawlers``/``rate_limiter`` доезжают до ``build_corpus`` (L3a)."""
    captured = _forwards_spy(monkeypatch)
    cfg = CrawlSourcesConfig(defaults=CrawlSource(engine="http"))
    limiter = RateLimiter(sleep=lambda _s: None)
    fake = FakeCrawler(pages={})
    ports = Ports(crawlers={"http": fake})
    store = _store()
    try:
        phases = build_phases(
            inputs=_inputs(),
            plan=_plan(),
            ports=ports,
            crawl_sources=cfg,
            rate_limiter=limiter,
        )
        result = phases["corpus"](_context(store, ports))
    finally:
        store.close()

    assert result.status == "ok"
    assert captured["crawl_sources"] == cfg
    assert captured["crawlers"] == {"http": fake}
    assert captured["rate_limiter"] is limiter


def test_corpus_phase_crawl_knobs_default_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без новых kwargs ``crawl_sources``/``crawlers``/``rate_limiter`` = ``None`` (совместимо)."""
    captured = _forwards_spy(monkeypatch)
    store = _store()
    try:
        phases = build_phases(inputs=_inputs(), plan=_plan(), ports=Ports())
        phases["corpus"](_context(store, Ports()))
    finally:
        store.close()

    assert captured["crawl_sources"] is None
    assert captured["crawlers"] is None
    assert captured["rate_limiter"] is None


class _RecordingCrawler:
    """Краулер-заглушка, записывающий обращения (проверка выбора по ``engine``)."""

    def __init__(self, pages: dict[str, RawPage]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(url=url, content=b"", fetched_at=FIXED_TS, fetch_mode=plan.fetch)


def test_corpus_phase_selects_crawler_by_engine(tmp_path: Path) -> None:
    """``crawl_sources.defaults.engine`` выбирает адаптер из ``Ports.crawlers`` (L3a)."""
    url = "https://docs.test/p1"
    pages = {
        url: RawPage(
            url=url, content="Свойство А доступно.".encode(), fetched_at=FIXED_TS
        )
    }
    base = _RecordingCrawler(pages)
    engine_crawler = _RecordingCrawler(pages)
    cfg = CrawlSourcesConfig(defaults=CrawlSource(engine="scrapy"))
    ports = Ports(
        crawler=base,  # type: ignore[arg-type]
        crawlers={"scrapy": engine_crawler},  # type: ignore[dict-item]
        extractor=FakeExtractor(),
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        index_builder=FakeIndexBuilder(),
    )
    store = _store()
    try:
        result = build_phases(
            inputs=_inputs(),
            plan=_plan(),
            ports=ports,
            urls={"p1": [url]},
            crawl_sources=cfg,
            raw_root=str(tmp_path / "raw"),
        )["corpus"](_context(store, ports))
    finally:
        store.close()

    assert result.status == "ok"
    assert engine_crawler.calls == [url]  # активный выбран по ``engine``
    assert base.calls == []  # дефолтный fallback не тронут


def test_evaluate_phase_uses_retrieval_and_never_search() -> None:
    plan = _plan()
    lexical = _chunks_index()
    evaluator = FakeEvaluator(default=_ok_output())
    ports = Ports(
        evaluator=evaluator,
        lexical_index=lexical,
        search=_BombSearch(),  # I18: не должен быть вызван
    )
    store = _store()
    result = evaluate_phase(
        plan=plan, ports=ports, store=store, run_id=RUN_ID, profile="R0", cutoff_k=3
    )
    assert result.status == "ok"
    p1 = [c for c in result.cells if c.product_id == "p1"]
    assert p1 and p1[0].state is CellState.EVALUATED and p1[0].status is Status.OK
    # Ретривал продукт-скоупед (I17) и с зафиксированным K (I20).
    assert lexical.calls and all(call[2] == 3 for call in lexical.calls)
    assert all(call[1] == ["p1"] for call in lexical.calls if "p1" in call[1])
    # Провенанс ретривала (I16/I20) лёг на ячейку.
    assert p1[0].retrieval_profile == "R0"
    assert "cutoff_K" in (p1[0].retrieval_params_json or "")


def test_evaluate_phase_without_evaluator_fails() -> None:
    result = evaluate_phase(
        plan=_plan(), ports=Ports(), store=_store(), run_id=RUN_ID
    )
    assert result.status == "failed"
    assert result.errors == ["evaluator не сконфигурирован"]


def test_full_phase_chain_writes_report_artifacts(tmp_path: Path) -> None:
    inputs = _inputs()
    plan = _plan()
    pages = {
        "https://docs.test/p1": RawPage(
            url="https://docs.test/p1",
            content="Свойство А доступно.".encode(),
            fetched_at=FIXED_TS,
        ),
        "https://docs.test/p2": RawPage(
            url="https://docs.test/p2",
            content="Документ продукта два.".encode(),
            fetched_at=FIXED_TS,
        ),
    }
    ports = Ports(
        evaluator=FakeEvaluator(default=_ok_output()),
        lexical_index=_chunks_index(),
        registry=FakeRegistryFetcher(registry_id="reg_x", run_id=RUN_ID),
        crawler=FakeCrawler(pages=pages),
        extractor=FakeExtractor(),
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        index_builder=FakeIndexBuilder(),
    )
    phases = build_phases(
        inputs=inputs,
        plan=plan,
        ports=ports,
        urls={"p1": ["https://docs.test/p1"], "p2": ["https://docs.test/p2"]},
        passports=_passports(),
        out_dir=str(tmp_path),
        raw_root=str(tmp_path / "raw"),
    )
    store = _store()
    ctx = _context(store, ports)

    for name in ("registry", "inheritance", "corpus", "evaluate", "compute"):
        result = phases[name](ctx)
        assert result.status == "ok", (name, result.errors)

    report = phases["report"](ctx)
    assert report.status == "ok", report.errors
    artifacts = report.log_entries[0]
    assert artifacts.manifest == tmp_path / RUN_ID / "run_manifest.yaml"
    assert artifacts.manifest.exists()

    manifest = yaml.safe_load(artifacts.manifest.read_text(encoding="utf-8"))
    assert manifest["run_id"] == RUN_ID
    # index_build_id захвачен из фазы corpus (единственный межфазовый канал — замыкание).
    assert str(manifest["index_build_id"]).startswith("ib_")


def test_report_phase_without_compute_fails() -> None:
    inputs = _inputs()
    plan = _plan()
    phases = build_phases(inputs=inputs, plan=plan, ports=Ports())
    store = _store()
    result = phases["report"](_context(store, Ports()))
    assert result.status == "failed"
    assert "compute" in result.errors[0]


# ---------------------------------------------------------------------------
# Деплой-провенанс: build_phases прокидывает его в манифест Ш9 (07 §7, R7/§13)
# ---------------------------------------------------------------------------


def _run_chain_to_report(tmp_path: Path, **phase_kwargs: Any) -> dict[str, Any]:
    """Прогоняет registry→…→report на фейках и возвращает ``run_manifest.yaml`` как dict."""
    inputs = _inputs()
    plan = _plan()
    pages = {
        "https://docs.test/p1": RawPage(
            url="https://docs.test/p1",
            content="Свойство А доступно.".encode(),
            fetched_at=FIXED_TS,
        ),
        "https://docs.test/p2": RawPage(
            url="https://docs.test/p2",
            content="Документ продукта два.".encode(),
            fetched_at=FIXED_TS,
        ),
    }
    ports = Ports(
        evaluator=FakeEvaluator(default=_ok_output()),
        lexical_index=_chunks_index(),
        registry=FakeRegistryFetcher(registry_id="reg_x", run_id=RUN_ID),
        crawler=FakeCrawler(pages=pages),
        extractor=FakeExtractor(),
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        index_builder=FakeIndexBuilder(),
    )
    phases = build_phases(
        inputs=inputs,
        plan=plan,
        ports=ports,
        urls={"p1": ["https://docs.test/p1"], "p2": ["https://docs.test/p2"]},
        passports=_passports(),
        out_dir=str(tmp_path),
        raw_root=str(tmp_path / "raw"),
        **phase_kwargs,
    )
    store = _store()
    ctx = _context(store, ports)
    for name in ("registry", "inheritance", "corpus", "evaluate", "compute"):
        result = phases[name](ctx)
        assert result.status == "ok", (name, result.errors)
    report = phases["report"](ctx)
    assert report.status == "ok", report.errors
    artifacts = report.log_entries[0]
    return yaml.safe_load(artifacts.manifest.read_text(encoding="utf-8"))


def test_build_phases_forwards_deploy_provenance_to_manifest(tmp_path: Path) -> None:
    """Переданные в ``build_phases`` деплой-поля доходят до ``run_manifest.yaml`` (Ш9)."""
    caps = {"gateway": "0.4.2", "edge": "1"}
    manifest = _run_chain_to_report(
        tmp_path,
        deploy_profile="split",
        edge_schema_version="1.0.0",
        edge_capabilities=caps,
    )
    assert manifest["deploy_profile"] == "split"
    assert manifest["edge_schema_version"] == "1.0.0"
    assert manifest["edge_capabilities"] == caps


def test_build_phases_without_deploy_provenance_is_backward_compatible(
    tmp_path: Path,
) -> None:
    """Без новых kwargs манифест строится: деплой-поля пусты (backward-compatible)."""
    manifest = _run_chain_to_report(tmp_path)
    assert manifest["deploy_profile"] is None
    assert manifest["edge_schema_version"] is None
    assert manifest["edge_capabilities"] == {}


def test_build_phases_records_default_retrieval_provenance(tmp_path: Path) -> None:
    """R7/§13: манифест несёт эффективный профиль/K даже без явных kwargs (не null).

    Ранее манифест писал ``retrieval_profile=null`` при том, что ячейка несла ``R0`` —
    манифест не был самостоятельной записью воспроизведения. Теперь значения совпадают
    с форматом ячейки (``_provenance``).
    """
    from core.pipeline.evaluate import (
        DEFAULT_RETRIEVAL_CUTOFF_K,
        DEFAULT_RETRIEVAL_PROFILE,
    )

    manifest = _run_chain_to_report(tmp_path)
    assert manifest["retrieval_profile"] == DEFAULT_RETRIEVAL_PROFILE.value
    assert manifest["retrieval_cutoff_K"] == DEFAULT_RETRIEVAL_CUTOFF_K
    assert json.loads(manifest["retrieval_params_json"]) == {
        "profile": DEFAULT_RETRIEVAL_PROFILE.value,
        "cutoff_K": DEFAULT_RETRIEVAL_CUTOFF_K,
    }


def test_build_phases_forwards_explicit_retrieval_provenance(tmp_path: Path) -> None:
    """Явные ``retrieval_profile``/``cutoff_k`` доходят до манифеста в формате ячейки."""
    manifest = _run_chain_to_report(tmp_path, retrieval_profile="R1", cutoff_k=25)
    assert manifest["retrieval_profile"] == "R1"
    assert manifest["retrieval_cutoff_K"] == 25
    assert json.loads(manifest["retrieval_params_json"]) == {
        "profile": "R1",
        "cutoff_K": 25,
    }


def test_discovery_noop_without_adapter() -> None:
    inputs = _inputs()
    plan = _plan()
    phases = build_phases(inputs=inputs, plan=plan, ports=Ports())
    store = _store()
    result = phases["discovery"](_context(store, Ports()))
    # P2a: отсутствие адаптера — ``skipped`` с машинной причиной, а не фиктивное ``ok``.
    assert result.status == "skipped"
    assert result.skip_reason is SkipReason.NO_SEARCH_ADAPTER
    assert result.log_entries == []  # без адаптера никаких кандидатов (I19)


def test_discovery_runs_with_adapter_and_queries() -> None:
    from core.ports.search import SearchResult

    class _Adapter:
        adapter_kind = "serp"
        provider = "fake"

        def search(self, q: Any) -> list[SearchResult]:
            return [SearchResult(url="https://serp.test/1", title="t", snippet="s", rank=0)]

        def fetch(self, url: str) -> RawPage:  # pragma: no cover
            return RawPage(url=url, content=b"", fetched_at=FIXED_TS)

    inputs = SimpleNamespace(
        run=SimpleNamespace(),
        package=_package(),
        baselines=_baselines(),
        discovery_queries=["запрос"],
    )
    phases = build_phases(
        inputs=inputs,  # type: ignore[arg-type] — duck-typed view for discovery queries
        plan=_plan(),
        ports=Ports(search=_Adapter()),
    )
    store = _store()
    result = phases["discovery"](_context(store, Ports(search=_Adapter())))
    assert result.status == "ok"
    assert len(result.log_entries) == 1
    candidates = result.log_entries[0].candidates
    assert candidates and candidates[0].source_urls == ["https://serp.test/1"]


# ---------------------------------------------------------------------------
# review: advisory `pending` (P2b) — ничего не решает, не меняет ячейки (I6/I7)
# ---------------------------------------------------------------------------


def _save_preliminary(store: SqliteStore, product_id: str, param_id: str) -> None:
    """Кладёт в store ``preliminary``-ячейку (кандидат очереди ревью Ш7)."""
    store.save_cell(
        Cell(
            run_id=RUN_ID,
            product_id=product_id,
            param_id=param_id,
            state=CellState.PRELIMINARY,
            status=None,
        )
    )


def test_review_phase_attaches_pending_summary_when_queue_non_empty() -> None:
    """Ш7: непустая очередь → ``ok`` + ограниченная сводка в ``pending`` (не статус)."""
    store = _store()
    _save_preliminary(store, "p1", "param_a")
    _save_preliminary(store, "p2", "param_b")
    phases = build_phases(inputs=_inputs(), plan=_plan(), ports=Ports())
    result = phases["review"](_context(store, Ports()))

    assert result.status == "ok"  # pending — advisory, статус не меняет (I6/I7)
    assert len(result.pending) == 1
    summary = result.pending[0]
    assert "review: 2 ячеек ожидают решения" in summary
    assert "states=preliminary" in summary
    assert "p1/param_a" in summary and "p2/param_b" in summary


def test_review_phase_pending_empty_when_queue_empty() -> None:
    """Ш7: пустая очередь → ``pending == []`` (честное «нечего решать»)."""
    store = _store()
    phases = build_phases(inputs=_inputs(), plan=_plan(), ports=Ports())
    result = phases["review"](_context(store, Ports()))

    assert result.status == "ok"
    assert result.pending == []


def test_review_phase_does_not_mutate_cell_state() -> None:
    """I6/I7-регрессия: фаза ревью **не** трогает статусы ячеек (byte-identical).

    Ни ``N/A``, ни promote, ни преодоление ``❓`` — очередь только читается; снимок строк
    до/после обязан совпасть побайтово.
    """
    store = _store()
    _save_preliminary(store, "p1", "param_a")
    store.save_cell(
        Cell(
            run_id=RUN_ID,
            product_id="p2",
            param_id="param_b",
            state=CellState.EVALUATED,
            status=Status.UNKNOWN,
        )
    )
    store.save_cell(
        Cell(
            run_id=RUN_ID,
            product_id="p1",
            param_id="param_c",
            state=CellState.EVALUATED,
            status=Status.OK,
            evidence=[LLMEvidence(source_id="src.1", quote="q")],  # ✅ — не в очереди
        )
    )

    def _snapshot() -> list[str]:
        return sorted(cell.model_dump_json() for cell in store.iter_cells(RUN_ID))

    before = _snapshot()
    phases = build_phases(inputs=_inputs(), plan=_plan(), ports=Ports())
    result = phases["review"](_context(store, Ports()))

    assert result.status == "ok"
    assert result.cells == []  # фаза ничего не «производит»
    assert _snapshot() == before  # ни один статус/состояние не изменился


# ---------------------------------------------------------------------------
# build_manifest: детерминизм и провенанс
# ---------------------------------------------------------------------------


class _RunView:
    """Обёртка ``RunConfig`` с дополнительными полями провенанса (duck typing)."""

    def __init__(self, run: RunConfig, **overrides: Any) -> None:
        self._run = run
        self._overrides = overrides

    def __getattr__(self, name: str) -> Any:
        if name in self._overrides:
            return self._overrides[name]
        return getattr(self._run, name)


def _bundle(store: SqliteStore):
    store.save_cell(
        Cell(
            run_id=RUN_ID,
            product_id="p1",
            param_id="param_a",
            state=CellState.EVALUATED,
            status=Status.OK,
            evidence=[LLMEvidence(source_id="src.1", quote="quote")],
        )
    )
    return compute(
        store=store,
        run_id=RUN_ID,
        catalog={row.param_id: row for row in _catalog()},
        module_weights={"mod_a": 1.0},
        cohort=_cohort(),
        requested_ranking=RankingPolicy.CAPABILITY_RANKED,
    )


def _manifest_inputs(inputs: RunInputs) -> RunInputs:
    run = _RunView(
        inputs.run,
        methodology_version="2.0.0",
        catalog_version="catalog-9",
        status_dict_version="2.0.0",
        scoring_policy_version="scoring-3",
        retrieval_profile="R2",
        retrieval_cutoff_K=5,
        retrieval_params_json={"rrf_k": 60, "profile": "R2"},
        search_effort_budget_json={"serp_rounds": 3},
    )
    return replace(inputs, run=run)


def test_build_manifest_deterministic_and_populated() -> None:
    store = _store()
    bundle = _bundle(store)
    inputs = _manifest_inputs(_inputs())
    plan = _plan()

    manifest = build_manifest(
        inputs=inputs,
        plan=plan,
        bundle=bundle,
        index_build_id="ib_test",
        deploy_profile="split",
        edge_schema_version="1.0.0",
        edge_capabilities={"edge": "1"},
        created_at=FIXED_TS,
    )
    again = build_manifest(
        inputs=inputs,
        plan=plan,
        bundle=bundle,
        index_build_id="ib_test",
        deploy_profile="split",
        edge_schema_version="1.0.0",
        edge_capabilities={"edge": "1"},
        created_at=FIXED_TS,
    )
    assert manifest == again  # детерминизм (R7/§13)

    # Обязательный провенанс.
    assert manifest.run_id == RUN_ID
    assert manifest.case_id == CASE_ID
    assert manifest.mode is RunMode.STRICT
    assert manifest.anchor_product_id == "p1"
    assert manifest.focus_modules == ["mod_a"]
    assert manifest.snapshot_date == "2026-01-01"
    assert manifest.package_version == "1.2.3"
    # methodology_version берётся с входа, а не откатывается к "0.0.0".
    assert manifest.methodology_version == "2.0.0"
    assert manifest.catalog_version == "catalog-9"
    assert manifest.status_dict_version == "2.0.0"
    assert manifest.scoring_policy_version == "scoring-3"
    assert manifest.index_build_id == "ib_test"
    assert manifest.retrieval_profile == "R2"
    assert manifest.retrieval_cutoff_K == 5
    assert manifest.requested_ranking is RankingPolicy.CAPABILITY_RANKED
    assert manifest.computed_ranking is RankingPolicy(bundle.computed_ranking)
    # Детерминированный JSON (sort_keys=True).
    assert manifest.retrieval_params_json == json.dumps(
        {"rrf_k": 60, "profile": "R2"}, ensure_ascii=False, sort_keys=True
    )
    assert manifest.search_effort_budget_json == '{"serp_rounds": 3}'
    # Деплой-провенанс (07 §7).
    assert manifest.deploy_profile == "split"
    assert manifest.edge_schema_version == "1.0.0"
    assert manifest.edge_capabilities == {"edge": "1"}


def test_build_manifest_defaults_without_extra_run_fields() -> None:
    """Без обогащённого run манифест строится: ``package_version`` из package, версии — дефолт."""
    store = _store()
    bundle = _bundle(store)
    manifest = build_manifest(
        inputs=_inputs(), plan=_plan(), bundle=bundle, created_at=FIXED_TS
    )
    assert manifest.package_version == "1.2.3"
    # P2: ``RunConfig`` несёт пин-дефолт методологии; манифест берёт его со входа, а не
    # «0.0.0» (заглушка остаётся лишь для промежуточной записи runs без версии).
    assert manifest.methodology_version == METHODOLOGY_VERSION
    assert manifest.requested_ranking is RankingPolicy.CAPABILITY_RANKED


# ---------------------------------------------------------------------------
# Слой/нейтральность: phases.py не тянет адаптеры/edge
# ---------------------------------------------------------------------------


def test_phases_module_does_not_import_adapters_or_edge() -> None:
    tree = ast.parse((REPO_ROOT / "core" / "pipeline" / "phases.py").read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
        elif isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
    forbidden = [
        module
        for module in imported
        if module.startswith(("core.adapters", "core.edge"))
    ]
    assert forbidden == [], f"phases.py импортирует запрещённые слои: {forbidden}"
