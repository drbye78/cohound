"""Тесты ``passport-scout`` (M1): консервативный черновик паспорта.

Без сети/ключей: ``crawler``/``extractor`` — детерминированные фейки, ``discovery_llm`` —
реальный адаптер с швом ``generate`` (фикстурная гипотеза). Проверяются инварианты
precision-first: нет auto-approve (только ``draft``/``not_found``), ``evidence_state``
никогда не ``verified``, whitelist URL (фабрикация → ``needs_human``), цитата-подстрока,
``unknown`` для неподтверждённых vendor/jurisdiction, ``upstream`` только при
подтверждении, детерминизм записи и валидность YAML-черновика (§4.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

from core.adapters.llm_discovery import DiscoveryLLM
from core.config.passports import load_passport
from core.domain.enums import EvidenceState, SatelliteState
from core.domain.models import Passport
from core.pipeline.passport_scout import (
    DiscoveryPassportHypothesis,
    PassportScoutReport,
    build_passport_draft,
    scout_passport,
    write_passport_draft,
)
from core.ports.search import SearchQuery, SearchResult
from core.ports.types import FetchPlan, RawPage
from tests.fakes.ports import FakeExtractor
from tests.helpers.scout import FIXED_TS, fake_crawler

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "passport_scout"
URL = "https://docs.test/alpha"
URL2 = "https://docs.test/beta"
PAGE_TEXT = (
    "Alpha Product documentation.\n"
    "Vendor: ACME.\n"
    "The Alpha Product supports Feature X natively.\n"
)
SNAPSHOT = _dt.date(2026, 6, 1)
PRODUCT = "Alpha Product"


def _hypothesis() -> dict[str, Any]:
    return json.loads((FIXTURES / "hypothesis.json").read_text(encoding="utf-8"))


def _llm(hyp: dict[str, Any] | None = None) -> DiscoveryLLM:
    payload = json.dumps(hyp if hyp is not None else _hypothesis(), ensure_ascii=False)
    return DiscoveryLLM(
        generate=lambda prompt, schema: payload, model_id="m1", decoder="none"
    )


def _report(**overrides: Any) -> PassportScoutReport:
    params: dict[str, Any] = {
        "product_name": PRODUCT,
        "url": URL,
        "crawler": fake_crawler(URL, PAGE_TEXT),
        "extractor": FakeExtractor(),
        "discovery_llm": _llm(),
        "today": SNAPSHOT,
    }
    params.update(overrides)
    return scout_passport(**params)


# ---------------------------------------------------------------------------
# Инварианты черновика (нет auto-approve)
# ---------------------------------------------------------------------------


def test_no_satellite_is_approved_or_verified() -> None:
    report = _report()
    assert report.ok is True
    assert report.passport.satellites  # гипотезы есть
    for sat in report.passport.satellites:
        assert sat.state in {SatelliteState.DRAFT, SatelliteState.NOT_FOUND}
        assert sat.state not in {
            SatelliteState.APPROVED,
            SatelliteState.EDITED_BY_HUMAN,
        }
        assert sat.evidence_state is EvidenceState.CLAIMED
        assert sat.evidence_state is not EvidenceState.VERIFIED


def test_passport_draft_is_valid_domain_model() -> None:
    report = _report()
    # Конструируем заново из dump — валидаторы §4.3 проходят.
    revalidated = Passport.model_validate(report.passport.model_dump(mode="json"))
    assert revalidated.product_id == report.passport.product_id


def test_product_id_is_slug_of_name() -> None:
    assert _report().passport.product_id == "Alpha_Product"


# ---------------------------------------------------------------------------
# Whitelist URL / цитаты
# ---------------------------------------------------------------------------


def test_fabricated_satellite_url_is_discarded() -> None:
    report = _report()
    yaml_text = report.passport.model_dump_json()
    assert "fab.test" not in yaml_text  # URL вне источников не попал в YAML
    assert any("fab.test" in item for item in report.needs_human)

    ghost = next(s for s in report.passport.satellites if s.component == "Ghost Component")
    assert ghost.relationship_evidence is None


def test_quote_not_substring_drops_relationship_evidence() -> None:
    report = _report()
    bad = next(s for s in report.passport.satellites if s.component == "Bad Quote")
    assert bad.relationship_evidence is None
    assert any("не подтверждена цитатой" in item for item in report.needs_human)


def test_valid_quote_sets_relationship_evidence() -> None:
    report = _report()
    good = next(s for s in report.passport.satellites if s.component == "Feature X")
    assert good.relationship_evidence is not None
    assert good.relationship_evidence.url == URL
    assert good.relationship_evidence.quote in PAGE_TEXT


def test_quotes_are_never_generated() -> None:
    """Любая цитата в черновике — подстрока реально переданного текста."""
    report = _report()
    for sat in report.passport.satellites:
        if sat.relationship_evidence is not None:
            assert sat.relationship_evidence.quote in PAGE_TEXT


def test_doc_domains_only_from_fetched_urls() -> None:
    report = _report()
    assert report.passport.doc_domains == ["docs.test"]
    assert "evil.test" not in report.passport.doc_domains


# ---------------------------------------------------------------------------
# unknown vendor/jurisdiction и upstream
# ---------------------------------------------------------------------------


def test_unknown_vendor_and_jurisdiction_flagged() -> None:
    report = _report()
    assert report.passport.vendor == "unknown"
    assert report.passport.jurisdiction == "unknown"
    assert any("vendor" in item for item in report.needs_human)
    assert any("jurisdiction" in item for item in report.needs_human)


def test_hypothesis_needs_human_is_propagated() -> None:
    assert "проверить версию" in _report().needs_human


def test_upstream_built_with_confirmed_citation() -> None:
    report = _report()
    upstream = report.passport.upstream
    assert upstream is not None
    assert upstream.baseline_id == "base-1"
    assert upstream.version == "1.0"
    assert upstream.evidence.url == URL
    assert upstream.evidence.quote in PAGE_TEXT


def test_upstream_with_fabricated_url_is_dropped() -> None:
    hyp = _hypothesis()
    hyp["upstream"] = {"url": "https://fab.test/base", "quote": "supports Feature X natively"}
    report = _report(discovery_llm=_llm(hyp))
    assert report.passport.upstream is None
    assert any("upstream" in item for item in report.needs_human)


def test_upstream_with_unconfirmed_quote_is_dropped() -> None:
    hyp = _hypothesis()
    hyp["upstream"] = {"url": URL, "quote": "NOT IN TEXT AT ALL"}
    report = _report(discovery_llm=_llm(hyp))
    assert report.passport.upstream is None
    assert any("цитата" in item for item in report.needs_human)


def test_partial_upstream_flags_needs_human() -> None:
    hyp = _hypothesis()
    hyp["upstream"] = None
    hyp["upstream_baseline_id"] = None
    hyp["upstream_version"] = None
    report = _report(discovery_llm=_llm(hyp))
    assert report.passport.upstream is None


# ---------------------------------------------------------------------------
# build_passport_draft (прямой вызов, permissive ввод)
# ---------------------------------------------------------------------------


def test_build_passport_draft_accepts_empty_hypothesis() -> None:
    passport, needs_human, provenance = build_passport_draft(
        product_name="P",
        hypothesis=DiscoveryPassportHypothesis(),
        allowed_urls={"https://x.test/"},
        corpus_text="",
        snapshot_date=SNAPSHOT,
    )
    assert passport.vendor == "unknown"
    assert passport.jurisdiction == "unknown"
    assert passport.satellites == []
    assert provenance["product_id"] == "P"


def test_max_satellites_limits_draft() -> None:
    report = _report(max_satellites=1)
    assert len(report.passport.satellites) == 1


# ---------------------------------------------------------------------------
# Отчёт / ошибки LLM
# ---------------------------------------------------------------------------


def test_llm_failure_reported_without_fabrication() -> None:
    def boom(prompt: str, schema: type) -> str:
        raise RuntimeError("no backend")

    report = _report(discovery_llm=DiscoveryLLM(generate=boom, model_id="m1"))
    assert report.ok is False
    assert report.errors
    # Гипотеза пустая, но черновик остаётся консервативно валидным.
    assert report.passport.vendor == "unknown"
    assert report.passport.satellites == []


def test_candidates_json_has_provenance() -> None:
    report = _report()
    cj = report.candidates_json
    assert cj["prompt_hash"]
    assert cj["decoder"] == "none"
    assert cj["model_id"] == "m1"
    assert cj["usage"] == {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "reasoning_tokens": 0,
    }
    assert cj["fetched"][0]["url"] == URL
    assert cj["fetched"][0]["content_hash"]


def test_fetch_error_is_isolated() -> None:
    class _BoomCrawler:
        version = "boom"

        def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
            raise RuntimeError("network down")

    report = _report(crawler=_BoomCrawler())
    assert report.ok is False
    assert any("ни одна страница не извлечена" in error for error in report.errors)
    assert report.page_errors
    assert report.fetched[0]["error"]


def test_unavailable_candidate_page_is_soft_error() -> None:
    """Один 404 среди SERP-кандидатов не роняет разведку (живой дефект FTData)."""

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
    assert report.candidates_json["page_errors"] == report.page_errors
    assert any(
        item.get("url") == URL2 and item.get("error") for item in report.fetched
    )


# ---------------------------------------------------------------------------
# Запись черновика (детерминизм)
# ---------------------------------------------------------------------------


def test_write_passport_draft_creates_yaml_and_sidecar(tmp_path: Path) -> None:
    report = _report()
    yaml_path, sidecar_path = write_passport_draft(report, tmp_path, SNAPSHOT)
    assert yaml_path == tmp_path / "Alpha_Product" / "2026-06-01.yaml"
    assert sidecar_path == tmp_path / "Alpha_Product" / "2026-06-01.candidates.json"
    assert yaml_path.is_file() and sidecar_path.is_file()
    # YAML-черновик читается штатным загрузчиком (§4.3).
    loaded = load_passport("Alpha_Product", tmp_path)
    assert loaded is not None
    assert loaded.canonical_name == "Alpha Product"
    assert loaded.vendor == "unknown"


def test_write_is_deterministic(tmp_path: Path) -> None:
    report = _report()
    first_yaml, first_sidecar = write_passport_draft(report, tmp_path / "a", SNAPSHOT)
    second_yaml, second_sidecar = write_passport_draft(report, tmp_path / "b", SNAPSHOT)
    assert first_yaml.read_bytes() == second_yaml.read_bytes()
    assert first_sidecar.read_bytes() == second_sidecar.read_bytes()


def test_write_sidecar_is_json() -> None:
    report = _report()
    sidecar = json.loads(json.dumps(report.candidates_json, sort_keys=True))
    assert sidecar["fetched"][0]["url"] == URL


# ---------------------------------------------------------------------------
# Опциональный поиск расширяет whitelist
# ---------------------------------------------------------------------------


def test_search_extends_whitelist_for_doc_domains() -> None:
    from core.ports.search import SearchQuery, SearchResult

    class _Search:
        adapter_kind = "serp"
        provider = "fake"

        def search(self, q: SearchQuery) -> list[SearchResult]:
            return [SearchResult(url="https://extra.test/a", title="x", rank=1)]

        def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется
            raise AssertionError

    report = _report(search=_Search())
    assert "extra.test" in report.passport.doc_domains
    assert "https://extra.test/a" in report.candidates_json["search_urls"]
