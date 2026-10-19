"""Тесты веб-заземлённой разведки пакета категории (``hound package scout``).

Без сети/ключей: ``search``/``crawler``/``extractor`` — детерминированные фейки,
``discovery_llm`` — реальный адаптер с швом ``generate`` (фикстурные гипотезы). Проверяются
инварианты precision-first: I18/I19 (выход — кандидаты, не evidence), A3/G2 (нет
auto-approve), whitelist URL (фабрикация → отброс), цитата-подстрока, консервативный
маппинг (showcase→fact, scope, дедуп), частичный ``ok=False`` и детерминизм записи.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from core.adapters.llm_discovery import DiscoveryLLM
from core.pipeline.package_candidates import Candidate, loads_candidates
from core.pipeline.package_scout import (
    PackageScoutReport,
    ScoutCatalogHypothesis,
    ScoutLandscapeHypothesis,
    build_seed_from_hypotheses,
    scout_package,
    write_scout_artifacts,
)
from core.ports.search import SearchQuery, SearchResult
from core.ports.types import FetchPlan, RawPage
from tests.fakes.ports import FakeExtractor
from tests.helpers.scout import FIXED_TS, fake_crawler

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "package_scout"
URL = "https://docs.test/alpha"
URL2 = "https://docs.test/beta"
SUBJECT = "Alpha Product"
PAGE_TEXT = (
    "Alpha Product documentation.\n"
    "The Alpha Product is built on the Open Core engine, an open-source upstream.\n"
    "Vendor: ACME.\n"
    "The Alpha Product provides Backup and Restore with verified snapshots.\n"
    "It supports Query Planner hints for parallel queries.\n"
)


def _landscape() -> dict[str, Any]:
    return json.loads((FIXTURES / "landscape.json").read_text(encoding="utf-8"))


def _catalog() -> dict[str, Any]:
    return json.loads((FIXTURES / "catalog.json").read_text(encoding="utf-8"))


def _llm(
    *,
    landscape: dict[str, Any] | None = None,
    catalog: dict[str, Any] | None = None,
    fail_landscape: bool = False,
    fail_catalog: bool = False,
) -> DiscoveryLLM:
    """Детерминированный шов ``generate``: диспетчеризация по схеме ответа."""
    landscape_payload = landscape if landscape is not None else _landscape()
    catalog_payload = catalog if catalog is not None else _catalog()

    def generate(prompt: str, schema: type) -> str:
        if schema.__name__ == "ScoutLandscapeHypothesis":
            if fail_landscape:
                raise RuntimeError("no backend")
            return json.dumps(landscape_payload, ensure_ascii=False)
        if fail_catalog:
            raise RuntimeError("no backend")
        return json.dumps(catalog_payload, ensure_ascii=False)

    return DiscoveryLLM(generate=generate, model_id="m1", decoder="none")


def _report(**overrides: Any) -> PackageScoutReport:
    params: dict[str, Any] = {
        "subject_name": SUBJECT,
        "seed_urls": [URL],
        "crawler": fake_crawler(URL, PAGE_TEXT),
        "extractor": FakeExtractor(),
        "discovery_llm": _llm(),
    }
    params.update(overrides)
    return scout_package(**params)


def _of_kind(report: PackageScoutReport, kind: str) -> list[Candidate]:
    return [c for c in report.candidates if c.kind == kind]


# ---------------------------------------------------------------------------
# A3/I18/I19: только кандидаты, все — человеку
# ---------------------------------------------------------------------------


def test_all_candidates_require_human_and_source_scout() -> None:
    report = _report()
    assert report.ok is True
    assert report.candidates
    for candidate in report.candidates:
        assert candidate.needs_human is True
        assert candidate.source == "scout"


def test_candidates_are_not_evidence() -> None:
    report = _report()
    # Никаких доменных evidence-структур: только Candidate.
    for candidate in report.candidates:
        assert isinstance(candidate, Candidate)
    assert all(c.kind in {"param", "module", "role", "baseline", "cohort_member"} for c in report.candidates)


def test_candidate_kinds_present() -> None:
    report = _report()
    kinds = {candidate.kind for candidate in report.candidates}
    assert "param" in kinds
    assert "module" in kinds
    assert "role" in kinds
    assert "baseline" in kinds


def test_determinism_of_candidates_order() -> None:
    first = _report().candidates
    second = _report().candidates
    assert [c.key() for c in first] == [c.key() for c in second]


# ---------------------------------------------------------------------------
# Whitelist URL / цитаты
# ---------------------------------------------------------------------------


def test_fabricated_url_param_is_dropped() -> None:
    report = _report()
    titles = {c.payload.get("title") for c in _of_kind(report, "param")}
    assert "Ghost Param" not in titles
    assert any("fab.test" in item for item in report.needs_human)


def test_bad_quote_param_is_dropped() -> None:
    report = _report()
    titles = {c.payload.get("title") for c in _of_kind(report, "param")}
    assert "Bad Quote Param" not in titles
    assert any("цитата" in item for item in report.needs_human)


def test_fabricated_upstream_is_dropped() -> None:
    landscape = _landscape()
    landscape["upstream"]["url"] = "https://fab.test/upstream"
    report = _report(discovery_llm=_llm(landscape=landscape))
    assert report.profile.get("upstream_name") is None
    assert any("upstream" in item for item in report.needs_human)


def test_quotes_never_generated_in_candidates() -> None:
    """Любая цитата-полевка кандидатов заземлена в реально переданный текст."""
    report = _report()
    for candidate in report.candidates:
        url = candidate.payload.get("url")
        quote = candidate.payload.get("quote")
        if url is not None:
            assert url in {URL, URL2}
        if quote is not None:
            assert quote in PAGE_TEXT


# ---------------------------------------------------------------------------
# Консервативный маппинг
# ---------------------------------------------------------------------------


def test_unknown_module_param_is_skipped() -> None:
    report = _report()
    titles = {c.payload.get("title") for c in _of_kind(report, "param")}
    assert "Unknown Module Param" not in titles
    assert any("ghost_module" in item for item in report.needs_human)


def test_showcase_without_metric_demoted_to_fact() -> None:
    report = _report()
    planner = next(c for c in _of_kind(report, "param") if c.payload.get("title") == "Query Planner")
    assert planner.payload["param_type"] == "fact"
    assert any("демотирован до fact" in item for item in report.needs_human)


def test_param_id_is_slug_and_lowercase() -> None:
    report = _report()
    backup = next(c for c in _of_kind(report, "param") if c.payload.get("title") == "Backup and Restore")
    assert backup.payload["param_id"] == "backup_and_restore"


def test_baseline_from_reference_upstream() -> None:
    report = _report()
    baselines = _of_kind(report, "baseline")
    assert baselines
    entry = baselines[0].payload
    assert entry["baseline_id"] == "open_core"
    assert entry["param_id"] == "backup_and_restore"
    assert entry["inheritance_mode"] == "direct"


def test_archetype_from_confirmed_upstream() -> None:
    report = _report()
    assert report.archetype.archetype is not None
    assert report.archetype.archetype.value == "A"
    assert report.profile.get("upstream_name") == "Open Core"


def test_archetype_signal_disagreement_flags_needs_human() -> None:
    landscape = _landscape()
    landscape["archetype_signal"] = {"value": "C", "signals": ["no leader"], "confidence": 0.5}
    report = _report(discovery_llm=_llm(landscape=landscape))
    assert any("расхождение" in item for item in report.needs_human)


def test_scope_filters_baselines() -> None:
    # scope=module другого модуля → baseline (по backup-параметру) отсекается.
    report = _report(scope="core")
    assert _of_kind(report, "baseline") == []


def test_confidence_is_clamped() -> None:
    catalog = _catalog()
    for param in catalog["params"]:
        if param["title"] == "Backup and Restore":
            param["confidence"] = 5.0
    report = _report(discovery_llm=_llm(catalog=catalog))
    backup = next(c for c in _of_kind(report, "param") if c.payload.get("title") == "Backup and Restore")
    assert backup.confidence == 1.0


# ---------------------------------------------------------------------------
# Частичный результат (OQ-3)
# ---------------------------------------------------------------------------


def test_partial_failure_keeps_landscape_candidates() -> None:
    report = _report(discovery_llm=_llm(fail_catalog=True))
    assert report.ok is False
    assert report.errors
    # Ландшафт собрался — частичные кандидаты (модули/роли) есть, с пометкой.
    assert _of_kind(report, "module") or _of_kind(report, "role")
    assert any("частичные" in item for item in report.needs_human)


def test_search_failure_is_isolated() -> None:
    class _BoomSearch:
        adapter_kind = "serp"
        provider = "boom"

        def search(self, q: SearchQuery) -> list[SearchResult]:
            raise RuntimeError("search down")

        def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется
            raise AssertionError

    report = _report(search=_BoomSearch())
    assert report.ok is False
    assert any("search" in error for error in report.errors)


def test_unavailable_candidate_page_is_soft_error() -> None:
    """Один 404 среди SERP-кандидатов не роняет разведку пакета."""

    class _Search:
        adapter_kind = "serp"
        provider = "fake"

        def search(self, q: SearchQuery) -> list[SearchResult]:
            return [SearchResult(url=URL2, title="beta", rank=1)]

        def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется
            raise AssertionError

    class _PartialCrawler:
        version = "partial"

        def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
            if url == URL:
                return RawPage(
                    url=URL,
                    content=PAGE_TEXT.encode("utf-8"),
                    fetched_at=FIXED_TS,
                )
            raise RuntimeError("HTTP 404")

    report = _report(crawler=_PartialCrawler(), search=_Search())
    assert report.ok is True
    assert report.errors == []
    assert report.page_errors and "404" in report.page_errors[0]
    assert any("недоступна" in item for item in report.needs_human)
    assert report.sidecar["page_errors"] == report.page_errors
    assert any(
        item.get("url") == URL2 and item.get("error") for item in report.fetched
    )


def test_no_page_extracted_is_fatal() -> None:
    class _BoomCrawler:
        version = "boom"

        def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
            raise RuntimeError("network down")

    report = _report(crawler=_BoomCrawler())
    assert report.ok is False
    assert any("ни одна страница не извлечена" in error for error in report.errors)
    assert report.page_errors


def test_search_urls_extend_whitelist() -> None:
    class _Search:
        adapter_kind = "serp"
        provider = "fake"

        def search(self, q: SearchQuery) -> list[SearchResult]:
            return [SearchResult(url=URL2, title="beta", rank=1)]

        def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется
            raise AssertionError

    report = _report(search=_Search())
    assert URL2 in report.sidecar["search_urls"]
    assert report.search_calls >= 1


def test_max_calls_budget_stops_search() -> None:
    calls = {"n": 0}

    class _Search:
        adapter_kind = "serp"
        provider = "fake"

        def search(self, q: SearchQuery) -> list[SearchResult]:
            calls["n"] += 1
            return [SearchResult(url=URL2, title="beta", rank=1)]

        def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется
            raise AssertionError

    report = _report(search=_Search(), budget=1, hints={"queries": ["q2", "q3"]})
    assert report.search_calls == 1
    assert calls["n"] == 1


# ---------------------------------------------------------------------------
# build_seed_from_hypotheses (прямой вызов)
# ---------------------------------------------------------------------------


def test_build_seed_accepts_empty_hypotheses() -> None:
    profile, archetype, candidates, needs_human = build_seed_from_hypotheses(
        subject_name="P",
        landscape=ScoutLandscapeHypothesis(),
        catalog=ScoutCatalogHypothesis(),
        allowed_urls=set(),
        corpus_text="",
    )
    assert profile == {}
    assert archetype.archetype is None
    assert candidates == []
    assert needs_human == []


def test_build_seed_skips_existing_ids() -> None:
    profile, _archetype, candidates, _needs = build_seed_from_hypotheses(
        subject_name=SUBJECT,
        landscape=ScoutLandscapeHypothesis.model_validate(_landscape()),
        catalog=ScoutCatalogHypothesis.model_validate(_catalog()),
        allowed_urls={URL},
        corpus_text=PAGE_TEXT,
        existing_modules={"backup", "core"},
        existing_roles={"backup_role"},
        existing_params={"backup_and_restore"},
    )
    assert _of_kind_local(candidates, "module") == []
    assert _of_kind_local(candidates, "role") == []
    # Параметры унифицируются против существующих (не переиспользуют существующий id).
    param_ids = {c.payload["param_id"] for c in _of_kind_local(candidates, "param")}
    assert "backup_and_restore" not in param_ids
    assert "backup_and_restore_2" in param_ids


def _of_kind_local(candidates: list[Candidate], kind: str) -> list[Candidate]:
    return [c for c in candidates if c.kind == kind]


def test_only_catalog_disables_landscape_call() -> None:
    report = _report(only=["catalog"])
    assert report.sidecar["templates"]["landscape"] is None
    assert report.sidecar["templates"]["catalog"] is not None


# ---------------------------------------------------------------------------
# Запись артефактов (детерминизм, apply-совместимость, пакет не трогается)
# ---------------------------------------------------------------------------


def test_write_creates_candidates_and_provenance(tmp_path: Path) -> None:
    report = _report()
    candidates_path, provenance_path = write_scout_artifacts(report, tmp_path, "2026-06-01")
    assert candidates_path == tmp_path / "2026-06-01.candidates.json"
    assert provenance_path == tmp_path / "2026-06-01.provenance.json"
    assert candidates_path.is_file() and provenance_path.is_file()
    # Файл кандидатов совместим с apply --from (loads_candidates).
    parsed = loads_candidates(candidates_path.read_text(encoding="utf-8"))
    assert len(parsed) == len(report.candidates)
    sidecar = json.loads(provenance_path.read_text(encoding="utf-8"))
    assert sidecar["ok"] is True
    assert sidecar["fetched"][0]["url"] == URL


def test_write_is_deterministic(tmp_path: Path) -> None:
    first = write_scout_artifacts(_report(), tmp_path / "a", "2026-06-01")
    second = write_scout_artifacts(_report(), tmp_path / "b", "2026-06-01")
    assert first[0].read_bytes() == second[0].read_bytes()
    assert first[1].read_bytes() == second[1].read_bytes()


def test_write_does_not_touch_package(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    manifest = package / "package.yaml"
    manifest.write_text("case_id: p\n", encoding="utf-8")
    write_scout_artifacts(_report(), package / "sources" / "scout", "2026-06-01")
    # Артефакты пакета не изменены; добавлен только каталог sources/scout (OQ-4).
    assert manifest.read_text(encoding="utf-8") == "case_id: p\n"
    assert sorted(p.name for p in package.iterdir()) == ["package.yaml", "sources"]
    assert (package / "sources" / "scout" / "2026-06-01.candidates.json").is_file()


def test_sidecar_has_provenance() -> None:
    sidecar = _report().sidecar
    assert sidecar["llm"]["landscape"]["prompt_hash"]
    assert sidecar["llm"]["catalog"]["prompt_hash"]
    assert sidecar["llm"]["catalog"]["decoder"] == "none"
    assert sidecar["llm"]["catalog"]["model_id"] == "m1"
    assert sidecar["templates"]["landscape"] == "package_scout_v1_landscape"
