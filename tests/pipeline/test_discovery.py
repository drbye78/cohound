"""Тесты Discovery / concept scout (W11/T11.4, Ш2, §7.3, I18/I19).

Проверяют: сборку гипотез из ``SearchResult`` (URL+сниппет, **без** evidence),
дедуп по URL, детерминированный порядок, бюджеты, терминацию протокола, гейт G2
(человеческий промоушен), round-trip манифеста и **структурный I19** (модуль не
импортирует ``core.store`` и не пишет evidence).

Все фикстуры категорийно-нейтральны (§1.3). Нейтральны и сами тесты.
"""

from __future__ import annotations

import ast
import inspect
import json
from collections.abc import Iterable, Iterator

import pytest

from core.domain import Gate, GateDecision
from core.pipeline import discovery as discovery_mod
from core.pipeline.discovery import (
    DiscoveryCandidate,
    DiscoveryReport,
    DiscoveryStatus,
    ReviewAction,
    ReviewDecision,
    discover,
    review_candidates,
)
from core.pipeline.gates import gate_g2
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult

RUN_ID = "run-1"


# ---------------------------------------------------------------------------
# Фейки портов (нейтральные, in-memory)
# ---------------------------------------------------------------------------


class FakeSearch:
    """Минимальный ``SearchAdapterPort``: детерминированная карта запрос→результаты."""

    adapter_kind = "fake_serp"
    provider = "fake"

    def __init__(
        self,
        mapping: dict[str, list[SearchResult]] | None = None,
        errors: Iterable[str] = (),
    ) -> None:
        self.mapping = mapping or {}
        self.errors = set(errors)
        self.calls: list[SearchQuery] = []

    def search(self, q: SearchQuery) -> list[SearchResult]:
        self.calls.append(q)
        if q.text in self.errors:
            raise RuntimeError(f"провайдер упал на {q.text!r}")
        return list(self.mapping.get(q.text, []))

    def fetch(self, url: str):  # noqa: ANN201 — в разведке не используется
        raise NotImplementedError


class FakeGateStore:
    """In-memory ``GateStore``: аудит-след решений G1–G4 (S9)."""

    def __init__(self) -> None:
        self.decisions: list[GateDecision] = []

    def save_gate_decision(self, d: GateDecision) -> None:
        self.decisions = [x for x in self.decisions if x.decision_id != d.decision_id]
        self.decisions.append(d)

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]:
        for d in self.decisions:
            if d.run_id == run_id:
                yield d


class FakeBudget:
    """``BudgetPort``-совместимый бюджет: списывает вызовы, ведёт учёт фазы/продукта."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.charged = 0
        self.charges: list[tuple[str, str | None]] = []

    def charge(self, phase: str, product_id: str | None) -> bool:
        self.charges.append((phase, product_id))
        if self.charged >= self.total:
            return False
        self.charged += 1
        return True


def _result(url: str, title: str = "", snippet: str = "", rank: int = 0) -> SearchResult:
    return SearchResult(url=url, title=title, snippet=snippet, rank=rank)


def _report(
    candidates: list[DiscoveryCandidate] | None = None,
) -> DiscoveryReport:
    return DiscoveryReport(
        query=["q"],
        candidates=list(candidates or []),
        search_calls=1,
        termination_reason=discovery_mod.TerminationReason.RESULTS_EXHAUSTED,
    )


# ---------------------------------------------------------------------------
# discover: сборка гипотез
# ---------------------------------------------------------------------------


def test_discover_builds_hypotheses_and_preserves_urls() -> None:
    adapter = FakeSearch(
        {
            "q1": [
                _result("https://a.example.test/x", title="Widget Export", snippet="s1", rank=0),
                _result("https://b.example.test/y", title="Widget Import", snippet="s2", rank=1),
            ]
        }
    )
    report = discover(queries=["q1"], adapter=adapter, limit=5, lang="ru")

    assert isinstance(adapter, SearchAdapterPort)  # структурная совместимость порта
    assert report.search_calls == 1
    assert report.phase == "discovery"
    assert [c.source_urls for c in report.candidates] == [
        ["https://a.example.test/x"],
        ["https://b.example.test/y"],
    ]
    first = report.candidates[0]
    # Гипотеза, не evidence: статус hypothesis, evidence_refs — сниппет/URL (I19).
    assert first.status is DiscoveryStatus.HYPOTHESIS
    assert first.evidence_refs == ["s1"]
    assert first.param_id == "widget_export"
    assert first.candidate_id.startswith("dc_")
    assert first.confidence == 1.0
    # в модуле нет ни одной доменной сущности evidence
    assert not hasattr(first, "evidence")
    assert not hasattr(first, "source_id")


def test_discover_uses_url_as_ref_when_no_snippet() -> None:
    adapter = FakeSearch({"q": [_result("https://a.example.test/x", title="t")]})
    report = discover(queries=["q"], adapter=adapter)
    assert report.candidates[0].evidence_refs == ["https://a.example.test/x"]


def test_discover_passes_limit_lang_and_phase_to_query() -> None:
    adapter = FakeSearch({"q": []})
    discover(queries=["q"], adapter=adapter, limit=3, lang="en", phase="scout")
    sent = adapter.calls[0]
    assert (sent.text, sent.limit, sent.lang, sent.phase) == ("q", 3, "en", "scout")


def test_discover_accepts_prebuilt_search_query() -> None:
    adapter = FakeSearch({"q": [_result("https://a.example.test/x")]})
    query = SearchQuery(text="q", lang="ru", limit=1, phase="discovery")
    report = discover(queries=[query], adapter=adapter)
    assert report.query == ["q"]
    assert adapter.calls[0] is query


# ---------------------------------------------------------------------------
# discover: дедуп и детерминизм
# ---------------------------------------------------------------------------


def test_discover_dedups_by_url_and_merges_snippets() -> None:
    adapter = FakeSearch(
        {
            "q1": [_result("https://a.example.test/x", snippet="s1", rank=0)],
            "q2": [
                _result("https://a.example.test/x", snippet="s2", rank=2),
                _result("https://b.example.test/y", snippet="s3", rank=0),
            ],
        }
    )
    report = discover(queries=["q1", "q2"], adapter=adapter)

    assert len(report.candidates) == 2
    merged = report.candidates[0]
    assert merged.source_urls == ["https://a.example.test/x"]
    assert merged.evidence_refs == ["s1", "s2"]  # оба сниппета, без дублей
    assert merged.confidence == 1.0  # максимум по ранкам
    assert report.candidates[1].source_urls == ["https://b.example.test/y"]


def test_discover_is_deterministic() -> None:
    def run() -> DiscoveryReport:
        adapter = FakeSearch(
            {
                "q1": [_result("https://a.example.test/x", snippet="s", rank=1)],
                "q2": [_result("https://b.example.test/y", snippet="t", rank=0)],
            }
        )
        return discover(queries=["q1", "q2"], adapter=adapter)

    first, second = run(), run()
    assert first == second
    assert first.to_manifest_json() == second.to_manifest_json()


# ---------------------------------------------------------------------------
# discover: бюджет и терминация
# ---------------------------------------------------------------------------


def test_discover_int_budget_limits_calls() -> None:
    adapter = FakeSearch({"q1": [], "q2": [], "q3": []})
    report = discover(queries=["q1", "q2", "q3"], adapter=adapter, budget=2)

    assert report.search_calls == 2
    assert [q.text for q in adapter.calls] == ["q1", "q2"]
    assert report.termination_reason is discovery_mod.TerminationReason.BUDGET_EXHAUSTED


def test_discover_zero_budget_stops_immediately() -> None:
    adapter = FakeSearch({"q1": []})
    report = discover(queries=["q1"], adapter=adapter, budget=0)
    assert report.search_calls == 0
    assert adapter.calls == []
    assert report.termination_reason is discovery_mod.TerminationReason.BUDGET_EXHAUSTED


def test_discover_budget_port_charges_per_call() -> None:
    adapter = FakeSearch({"q1": [], "q2": [], "q3": []})
    budget = FakeBudget(total=2)
    report = discover(
        queries=["q1", "q2", "q3"],
        adapter=adapter,
        budget=budget,
        phase="scout",
        product_id="p1",
    )

    assert budget.charges == [("scout", "p1"), ("scout", "p1"), ("scout", "p1")]
    assert report.search_calls == 2
    assert report.termination_reason is discovery_mod.TerminationReason.BUDGET_EXHAUSTED


def test_discover_termination_reasons() -> None:
    not_found = discover(queries=["q"], adapter=FakeSearch({"q": []}))
    assert not_found.termination_reason is discovery_mod.TerminationReason.NOT_FOUND

    found = discover(
        queries=["q"], adapter=FakeSearch({"q": [_result("https://a.example.test/x")]})
    )
    assert found.termination_reason is discovery_mod.TerminationReason.RESULTS_EXHAUSTED

    errored = discover(queries=["q"], adapter=FakeSearch(errors=["q"]))
    assert errored.termination_reason is discovery_mod.TerminationReason.ERROR
    assert errored.candidates == []


def test_discover_empty_queries_is_not_found() -> None:
    report = discover(queries=[], adapter=FakeSearch())
    assert report.search_calls == 0
    assert report.candidates == []
    assert report.termination_reason is discovery_mod.TerminationReason.NOT_FOUND


def test_discover_isolates_provider_error_and_continues() -> None:
    adapter = FakeSearch(
        {"q2": [_result("https://a.example.test/x")]}, errors=["q1"]
    )
    report = discover(queries=["q1", "q2"], adapter=adapter)
    # ошибка первого запроса не мешает второму; протокол помечен error
    assert report.search_calls == 2
    assert len(report.candidates) == 1
    assert report.termination_reason is discovery_mod.TerminationReason.ERROR


# ---------------------------------------------------------------------------
# review_candidates: гейт G2 (человеческий промоушен)
# ---------------------------------------------------------------------------


def _sample_report() -> DiscoveryReport:
    return _report(
        [
            DiscoveryCandidate(
                candidate_id="dc_1",
                param_id="widget_export",
                title="Widget Export",
                module="m1",
                rationale="r",
                source_urls=["https://a.example.test/x"],
                evidence_refs=["s"],
                confidence=1.0,
            ),
            DiscoveryCandidate(
                candidate_id="dc_2",
                param_id="widget_import",
                title="Widget Import",
                module="m1",
                rationale="r",
                source_urls=["https://b.example.test/y"],
                evidence_refs=["s"],
                confidence=0.5,
            ),
        ]
    )


def test_review_does_not_promote_without_decision() -> None:
    store = FakeGateStore()
    reviewed = review_candidates(_sample_report(), store=store, run_id=RUN_ID)

    assert [c.status for c in reviewed] == [
        DiscoveryStatus.HYPOTHESIS,
        DiscoveryStatus.HYPOTHESIS,
    ]
    assert store.decisions == []  # ни одной строки gate_decisions — гейт не пройден


def test_review_persists_and_promotes_explicit_human_decision() -> None:
    store = FakeGateStore()
    reviewed = review_candidates(
        _sample_report(),
        store=store,
        run_id=RUN_ID,
        decisions=[
            ReviewDecision("dc_1", ReviewAction.PROMOTE, actor="human-1", notes="promote"),
            ReviewDecision("dc_2", ReviewAction.REJECT, actor="human-1", notes="reject"),
        ],
    )

    # отвергнутая гипотеза исключена; продвинутая стала candidate
    assert [c.candidate_id for c in reviewed] == ["dc_1"]
    assert reviewed[0].status is DiscoveryStatus.CANDIDATE
    # решение человека персистентно в gate_decisions (S9)
    g2 = [d for d in store.decisions if d.gate is Gate.G2]
    assert {d.target for d in g2} == {"dc_1", "dc_2"}
    assert all(d.actor == "human-1" for d in g2)
    # сама гипотеза evidence не несёт (I19): evidence_refs остаются ссылками-кандидатами
    assert reviewed[0].evidence_refs == ["s"]


def test_review_requires_actor_for_promotion() -> None:
    store = FakeGateStore()
    with pytest.raises(ValueError):
        review_candidates(
            _sample_report(),
            store=store,
            run_id=RUN_ID,
            decisions=[ReviewDecision("dc_1", ReviewAction.PROMOTE, actor="   ")],
        )


def test_review_requires_store_for_explicit_decisions() -> None:
    with pytest.raises(ValueError):
        review_candidates(
            _sample_report(),
            decisions=[ReviewDecision("dc_1", ReviewAction.PROMOTE, actor="human-1")],
        )


def test_review_reads_persisted_decisions_without_new_input() -> None:
    store = FakeGateStore()
    gate_g2(store, RUN_ID, actor="human-1", target="dc_1", notes="promote")
    gate_g2(store, RUN_ID, actor="human-1", target="dc_2", notes="reject")

    reviewed = review_candidates(_sample_report(), store=store, run_id=RUN_ID)
    assert [c.candidate_id for c in reviewed] == ["dc_1"]
    assert reviewed[0].status is DiscoveryStatus.CANDIDATE


def test_review_ignores_decisions_of_other_runs() -> None:
    store = FakeGateStore()
    gate_g2(store, "other-run", actor="human-1", target="dc_1", notes="promote")
    reviewed = review_candidates(_sample_report(), store=store, run_id=RUN_ID)
    assert all(c.status is DiscoveryStatus.HYPOTHESIS for c in reviewed)


def test_review_is_pure_and_does_not_mutate_report() -> None:
    store = FakeGateStore()
    report = _sample_report()
    review_candidates(
        report,
        store=store,
        run_id=RUN_ID,
        decisions=[ReviewDecision("dc_1", ReviewAction.PROMOTE, actor="human-1")],
    )
    assert all(c.status is DiscoveryStatus.HYPOTHESIS for c in report.candidates)


# ---------------------------------------------------------------------------
# Сериализация манифеста
# ---------------------------------------------------------------------------


def test_manifest_json_round_trip() -> None:
    report = _sample_report()
    report.candidates[0].status = DiscoveryStatus.CANDIDATE
    payload = json.loads(report.to_manifest_json())
    restored = DiscoveryReport.from_dict(payload)

    assert restored == report
    assert restored.candidates[0].status is DiscoveryStatus.CANDIDATE
    assert payload["termination_reason"] == "results_exhausted"
    assert json.loads(restored.to_manifest_json()) == payload


# ---------------------------------------------------------------------------
# Структурный I19 + нейтральность
# ---------------------------------------------------------------------------

_FORBIDDEN_IMPORTS = {"core.store", "core.store.sqlite", "core.store.sqlite_store"}
_FORBIDDEN_NAMES = {"Cell", "CellEvidence"}
_FORBIDDEN_WRITE_ATTRS = {
    "save_cell",
    "save_cells_batch",
    "add_evidence",
    "add_evidence_batch",
    "log_search",
    "save_exhaustion",
}


def test_i19_module_is_structurally_evidence_free() -> None:
    source = inspect.getsource(discovery_mod)
    tree = ast.parse(source)

    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            imported.add(module)
            imported.update(f"{module}.{alias.name}" for alias in node.names)

    # никаких импортов хранилища
    assert not any(name == f or name.startswith(f"{f}.") for f in _FORBIDDEN_IMPORTS for name in imported)
    # никаких доменных сущностей evidence
    assert all(not name.split(".")[-1] in _FORBIDDEN_NAMES for name in imported)
    # ни одного *использования* имён evidence в коде (упоминания в докстринге не в счёт)
    bare_names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    attr_names = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    assert not (bare_names & _FORBIDDEN_NAMES)
    assert not (attr_names & _FORBIDDEN_NAMES)

    # никаких вызовов записи ячеек/evidence/лога поиска
    write_attrs = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in _FORBIDDEN_WRITE_ATTRS
    }
    assert write_attrs == set()
    # discover не имеет доступа к StorePort вообще
    assert "StorePort" not in imported
    assert "store" not in inspect.signature(discover).parameters


def test_module_is_category_neutral() -> None:
    source = inspect.getsource(discovery_mod).lower()
    for term in ("postgres", "pangolin", "sber", "сбер", "copywala", "платформа"):
        assert term not in source
