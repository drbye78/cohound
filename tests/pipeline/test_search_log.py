"""Тесты ``search_log`` (W11/T11.2): бюджет, терминация, I16/I19/I23.

Покрываются: разбор ``SearchBudget`` (mapping/JSON/конфиг, дефолт, ошибки),
``SearchEffort.charge`` в пределах/за пределами бюджета, **симметрия на продукт**
(I16 — исчерпание одним продуктом не лишает другой), персистенция ``record_search``
через ``SqliteStore(":memory:")``, таблица истинности ``termination_for`` (§5) и
``external_search`` (бюджет → вызов/лог; исчерпание → ``[]`` без вызова; I19 —
только кандидаты, без evidence; обработка ошибки адаптера).

Фикстуры **категорийно-нейтральны**: example.com / «Example», без продуктов и вендоров.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest

from core.domain import (
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
    SearchKind,
    SearchPhase,
    TerminationReason,
)
from core.pipeline.search_log import (
    SearchBudget,
    SearchEffort,
    external_search,
    external_search_observed,
    record_search,
    termination_for,
)
from core.ports.search import SearchQuery, SearchResult
from core.ports.types import RawPage
from core.store.sqlite_store import SqliteStore
from core.util.cache import ContentAddressedCache

RUN_ID = "run-1"
FIXED_TS = "2026-10-08T00:00:00+00:00"
URL_A = "https://example.com/a"
URL_B = "https://example.com/b"


# --------------------------------------------------------------------------- #
# Фикстуры
# --------------------------------------------------------------------------- #


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        yield s


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=["param.a"]),
        languages=["ru", "en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


class _FakeAdapter:
    """Детерминированный фейк ``SearchAdapterPort`` (счётчик вызовов)."""

    adapter_kind = "serp"
    provider = "fake"

    def __init__(
        self,
        results: list[SearchResult] | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.results = list(results or [])
        self._error = error
        self.calls: list[SearchQuery] = []

    def search(self, q: SearchQuery) -> list[SearchResult]:
        self.calls.append(q)
        if self._error is not None:
            raise self._error
        return list(self.results)

    def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется
        raise NotImplementedError


def _row(store: SqliteStore, *, phase: str) -> list[Any]:
    return list(
        store.conn.execute(
            "SELECT * FROM search_log WHERE run_id = ? AND phase = ? ORDER BY log_id",
            (RUN_ID, phase),
        ).fetchall()
    )


# --------------------------------------------------------------------------- #
# SearchBudget: разбор и limit_for
# --------------------------------------------------------------------------- #


def test_budget_from_mapping_limits_by_phase() -> None:
    budget = SearchBudget.from_mapping({"serp": 5, "discovery": 2})
    assert budget.limit_for(SearchPhase.SERP) == 5
    assert budget.limit_for("discovery") == 2
    assert budget.limit_for(SearchPhase.SCOUT) is None


def test_budget_default_key_is_fallback() -> None:
    budget = SearchBudget.from_mapping({"serp": 5, "default": 3})
    assert budget.limit_for(SearchPhase.SERP) == 5
    assert budget.limit_for(SearchPhase.CORPUS) == 3


def test_budget_from_config_json_string() -> None:
    cfg = SimpleNamespace(search_effort_budget_json='{"serp": 4, "scout": 1}')
    budget = SearchBudget.from_config(cfg)
    assert budget.limit_for(SearchPhase.SERP) == 4
    assert budget.limit_for(SearchPhase.SCOUT) == 1


def test_budget_from_config_mapping_directly() -> None:
    budget = SearchBudget.from_config({"corpus": 7})
    assert budget.limit_for(SearchPhase.CORPUS) == 7


def test_budget_from_none_and_empty_is_unbounded() -> None:
    assert SearchBudget.from_config(None).limit_for(SearchPhase.SERP) is None
    assert SearchBudget.from_config(SimpleNamespace(search_effort_budget_json=None)).limits == {}
    assert SearchBudget.from_config(SimpleNamespace(search_effort_budget_json="  ")).limits == {}


def test_budget_rejects_invalid_values() -> None:
    with pytest.raises(ValueError):
        SearchBudget.from_mapping({"serp": "many"})
    with pytest.raises(ValueError):
        SearchBudget.from_mapping({"serp": -1})
    with pytest.raises(ValueError):
        SearchBudget.from_mapping({"serp": True})
    with pytest.raises(ValueError):
        SearchBudget.from_mapping('{"serp": ')  # битый JSON
    with pytest.raises(ValueError):
        SearchBudget.from_mapping([("serp", 1)])  # не mapping


# --------------------------------------------------------------------------- #
# SearchEffort: charge / remaining
# --------------------------------------------------------------------------- #


def test_charge_within_and_over_budget() -> None:
    effort = SearchEffort(SearchBudget.from_mapping({"serp": 2}))
    assert effort.charge("serp", "p1") is True
    assert effort.charge("serp", "p1") is True
    assert effort.charge("serp", "p1") is False  # исчерпан
    assert effort.used("serp", "p1") == 2
    assert effort.remaining("serp", "p1") == 0


def test_charge_batch_over_budget_does_not_consume() -> None:
    effort = SearchEffort(SearchBudget.from_mapping({"serp": 3}))
    assert effort.charge("serp", "p1", n=2) is True
    assert effort.charge("serp", "p1", n=2) is False  # 4 > 3, счётчик не двигается
    assert effort.used("serp", "p1") == 2
    assert effort.remaining("serp", "p1") == 1


def test_charge_unbounded_always_within() -> None:
    effort = SearchEffort()
    assert effort.remaining("serp", "p1") is None
    assert effort.charge("serp", "p1") is True
    assert effort.charge("serp", "p1", n=1000) is True
    assert effort.remaining("serp", "p1") is None


def test_charge_rejects_bad_n() -> None:
    effort = SearchEffort()
    with pytest.raises(ValueError):
        effort.charge("serp", "p1", n=-1)
    with pytest.raises(ValueError):
        effort.charge("serp", "p1", n=True)  # type: ignore[arg-type]


def test_budget_is_symmetric_per_product_i16() -> None:
    """Исчерпание бюджета продуктом p1 не лишает продукт p2 (I16)."""
    effort = SearchEffort(SearchBudget.from_mapping({"serp": 1}))
    assert effort.charge("serp", "p1") is True
    assert effort.charge("serp", "p1") is False  # p1 исчерпан
    assert effort.remaining("serp", "p1") == 0
    # p2 имеет собственный счётчик — не «заражён» исчерпанием p1.
    assert effort.remaining("serp", "p2") == 1
    assert effort.charge("serp", "p2") is True
    assert effort.charge("serp", "p2") is False


# --------------------------------------------------------------------------- #
# record_search: построение и персистенция
# --------------------------------------------------------------------------- #


def test_record_search_persists_and_returns_entry(store: SqliteStore) -> None:
    entry = record_search(
        store,
        run_id=RUN_ID,
        phase=SearchPhase.CORPUS,
        provider="fake",
        adapter_kind="serp",
        language="ru",
        query="neutral query",
        results_count=3,
        selected=1,
        termination_reason=TerminationReason.NOT_FOUND,
        lexical_score=0.5,
        fused_rank=2,
        clock=lambda: FIXED_TS,
    )
    assert entry.kind is SearchKind.EXTERNAL  # kind по умолчанию
    assert entry.ts == FIXED_TS
    assert entry.adapter_kind is not None and entry.adapter_kind.value == "serp"
    assert entry.termination_reason is TerminationReason.NOT_FOUND

    rows = _row(store, phase="corpus")
    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == RUN_ID
    assert row["kind"] == "external"
    assert row["provider"] == "fake"
    assert row["adapter_kind"] == "serp"
    assert row["language"] == "ru"
    assert row["query"] == "neutral query"
    assert row["results_count"] == 3
    assert row["selected"] == 1
    assert row["lexical_score"] == 0.5
    assert row["fused_rank"] == 2
    assert row["termination_reason"] == "not_found"
    assert row["ts"] == FIXED_TS


def test_record_search_kind_override_retrieval(store: SqliteStore) -> None:
    entry = record_search(
        store,
        run_id=RUN_ID,
        phase="corpus",
        kind=SearchKind.RETRIEVAL,
        clock=lambda: FIXED_TS,
    )
    assert entry.kind is SearchKind.RETRIEVAL


def test_record_search_without_store_does_not_persist() -> None:
    entry = record_search(None, run_id=RUN_ID, phase="scout", clock=lambda: FIXED_TS)
    assert entry.run_id == RUN_ID
    assert entry.phase is SearchPhase.SCOUT


def test_record_search_default_ts_is_utc_iso() -> None:
    entry = record_search(None, run_id=RUN_ID, phase="serp")
    assert entry.ts is not None and entry.ts.endswith("+00:00")


# --------------------------------------------------------------------------- #
# termination_for: таблица истинности §5
# --------------------------------------------------------------------------- #


def test_termination_for_truth_table() -> None:
    assert termination_for(False, 3, False, None) is TerminationReason.RESULTS_EXHAUSTED
    assert termination_for(False, 0, False, None) is TerminationReason.NOT_FOUND
    assert termination_for(False, None, False, None) is None
    assert termination_for(True, 0, False, None) is TerminationReason.BUDGET_EXHAUSTED
    assert termination_for(False, 3, True, None) is TerminationReason.SUFFICIENT_EVIDENCE
    assert termination_for(False, 3, False, ValueError("x")) is TerminationReason.ERROR


def test_termination_for_precedence() -> None:
    # Ошибка доминирует над остальными сигналами.
    assert termination_for(True, 0, True, ValueError("x")) is TerminationReason.ERROR
    # Достаточность доказательств доминирует над исчерпанием бюджета.
    assert termination_for(True, 3, True, None) is TerminationReason.SUFFICIENT_EVIDENCE
    # Исчерпание бюджета доминирует над «ничего не найдено».
    assert termination_for(True, 0, False, None) is TerminationReason.BUDGET_EXHAUSTED


# --------------------------------------------------------------------------- #
# external_search: бюджет, лог, I19
# --------------------------------------------------------------------------- #


def _results() -> list[SearchResult]:
    return [SearchResult(url=URL_A, title="Example A", rank=1), SearchResult(url=URL_B, rank=2)]


def test_external_search_within_budget_calls_adapter_once(store: SqliteStore) -> None:
    budget = SearchBudget.from_mapping({"serp": 5})
    effort = SearchEffort(budget)
    adapter = _FakeAdapter(_results())

    out = external_search(
        adapter,
        SearchQuery(text="neutral query", lang="ru", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=budget,
        product_id="p1",
        clock=lambda: FIXED_TS,
    )

    assert out == adapter.results
    assert len(adapter.calls) == 1
    assert effort.used("serp", "p1") == 1
    rows = _row(store, phase="serp")
    assert len(rows) == 1
    assert rows[0]["kind"] == "external"
    assert rows[0]["results_count"] == 2
    assert rows[0]["termination_reason"] is None
    assert rows[0]["query"] == "neutral query"
    assert rows[0]["language"] == "ru"


def test_external_search_exhausted_budget_returns_empty_without_adapter_call(
    store: SqliteStore,
) -> None:
    budget = SearchBudget.from_mapping({"serp": 1})
    effort = SearchEffort(budget)
    adapter = _FakeAdapter(_results())

    first = external_search(
        adapter,
        SearchQuery(text="q1", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=budget,
        product_id="p1",
        clock=lambda: FIXED_TS,
    )
    assert len(first) == 2
    calls_after_first = len(adapter.calls)

    second = external_search(
        adapter,
        SearchQuery(text="q2", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=budget,
        product_id="p1",
        clock=lambda: FIXED_TS,
    )
    assert second == []
    assert len(adapter.calls) == calls_after_first  # адаптер не вызван
    assert effort.used("serp", "p1") == 1

    rows = _row(store, phase="serp")
    assert len(rows) == 2
    assert rows[1]["results_count"] == 0
    assert rows[1]["termination_reason"] == "budget_exhausted"


def test_external_search_budget_per_product_symmetry_i16(store: SqliteStore) -> None:
    budget = SearchBudget.from_mapping({"serp": 1})
    effort = SearchEffort(budget)
    adapter = _FakeAdapter(_results())

    def _run(product_id: str) -> list[SearchResult]:
        return external_search(
            adapter,
            SearchQuery(text=f"q-{product_id}", phase="serp"),
            store=store,
            run_id=RUN_ID,
            effort=effort,
            budget=budget,
            product_id=product_id,
            clock=lambda: FIXED_TS,
        )

    _run("p1")  # p1 израсходовал свой единственный вызов
    assert _run("p1") == []  # повторно — исчерпан
    assert len(_run("p2")) == 2  # p2 не затронут (I16)
    assert effort.used("serp", "p1") == 1
    assert effort.used("serp", "p2") == 1


def test_external_search_empty_results_is_not_found(store: SqliteStore) -> None:
    budget = SearchBudget.from_mapping({"serp": 3})
    effort = SearchEffort(budget)
    adapter = _FakeAdapter([])
    out = external_search(
        adapter,
        SearchQuery(text="neutral query", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=budget,
        product_id="p1",
        clock=lambda: FIXED_TS,
    )
    assert out == []
    assert len(adapter.calls) == 1
    rows = _row(store, phase="serp")
    assert rows[0]["termination_reason"] == "not_found"


def test_external_search_error_is_logged_and_budget_charged(store: SqliteStore) -> None:
    budget = SearchBudget.from_mapping({"serp": 2})
    effort = SearchEffort(budget)
    adapter = _FakeAdapter(error=RuntimeError("search backend down"))

    out = external_search(
        adapter,
        SearchQuery(text="neutral query", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=budget,
        product_id="p1",
        clock=lambda: FIXED_TS,
    )
    assert out == []
    assert effort.used("serp", "p1") == 1  # неуспешный вызов тоже расходует усилие
    rows = _row(store, phase="serp")
    assert rows[0]["termination_reason"] == "error"
    assert rows[0]["results_count"] is None


def test_external_search_returns_candidates_not_evidence_i19(store: SqliteStore) -> None:
    budget = SearchBudget.from_mapping({"serp": 2})
    effort = SearchEffort(budget)
    out = external_search(
        _FakeAdapter(_results()),
        SearchQuery(text="neutral query", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=budget,
        product_id="p1",
    )
    assert all(isinstance(r, SearchResult) for r in out)
    for result in out:
        for banned in ("source_id", "citation_quote", "content_hash", "evidence"):
            assert not hasattr(result, banned)


# --------------------------------------------------------------------------- #
# S9a: бюджет счётчика не подменяется на месте
# --------------------------------------------------------------------------- #

def test_external_search_does_not_mutate_effort_budget(store: SqliteStore) -> None:
    """Бюджет приходит из ``budget``, но объект ``effort`` не меняется (S9a).

    Прежняя редакция присваивала ``effort.budget = budget`` — то есть правило о
    лимите оседало в чужом объекте, разделяемом с вызывающим.
    """
    bare = SearchEffort()  # бюджет не несёт лимитов
    budget = SearchBudget.from_mapping({"serp": 1})
    adapter = _FakeAdapter(_results())

    def _run() -> list[SearchResult]:
        return external_search(
            adapter,
            SearchQuery(text="neutral query", lang="ru", phase="serp"),
            store=store,
            run_id=RUN_ID,
            effort=bare,
            budget=budget,
            product_id="p1",
            clock=lambda: FIXED_TS,
        )

    assert len(_run()) == 2
    # Счётчик исходного объекта не подменён...
    assert bare.budget.limit_for("serp") is None
    # ...но расход и лимит из ``budget`` учтены: второй вызов упирается в потолок 1.
    assert bare.used("serp", "p1") == 1
    assert _run() == []
    rows = _row(store, phase="serp")
    assert rows[1]["termination_reason"] == "budget_exhausted"

# --------------------------------------------------------------------------- #
# SERP-кэш (P1, R7): попадание без вызова адаптера и без списания бюджета
# --------------------------------------------------------------------------- #

@pytest.fixture()
def cache(tmp_path: Any) -> ContentAddressedCache:
    return ContentAddressedCache(tmp_path / "cache")


def _search(
    store: SqliteStore,
    adapter: _FakeAdapter,
    *,
    cache: ContentAddressedCache | None = None,
    effort: SearchEffort | None = None,
    query: SearchQuery | None = None,
) -> list[SearchResult]:
    budget = SearchBudget.from_mapping({"serp": 5})
    return external_search(
        adapter,
        query or SearchQuery(text="neutral query", lang="ru", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort or SearchEffort(budget),
        budget=budget,
        product_id="p1",
        cache=cache,
        clock=lambda: FIXED_TS,
    )


def test_two_runs_second_served_from_cache_without_live_call(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    """R7: второй прогон того же входа не делает живых вызовов поиска (P1)."""
    adapter = _FakeAdapter(_results())

    first = _search(store, adapter, cache=cache)
    calls_after_first = len(adapter.calls)
    second_effort = SearchEffort(SearchBudget.from_mapping({"serp": 5}))
    second = _search(store, adapter, cache=cache, effort=second_effort)

    assert [r.url for r in first] == [URL_A, URL_B]
    assert [r.url for r in second] == [URL_A, URL_B]
    assert calls_after_first == 1
    assert len(adapter.calls) == calls_after_first  # живого вызова на втором прогоне нет
    # Файл кэша существует и он — SERP-запись (cache/serp/{query_hash}.json).
    assert list((cache.root / "serp").glob("*.json"))
    # Второй прогон обслужен, но бюджет **не** списан (иначе менялись бы search_log/метрики).
    assert second_effort.used("serp", "p1") == 0
    logs = _row(store, phase="serp")
    assert len(logs) == 2
    assert logs[1]["results_count"] == 2
    assert logs[1]["termination_reason"] is None


def test_cache_hit_does_not_charge_budget(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    adapter = _FakeAdapter(_results())
    effort = SearchEffort(SearchBudget.from_mapping({"serp": 1}))

    assert _search(store, adapter, cache=cache, effort=effort) == adapter.results
    assert effort.used("serp", "p1") == 1
    # Бюджет исчерпан, но запрос уже в кэше: попадание обслуживается без вызова и
    # без списания — исчерпание не подменяет ответ кэша.
    outcome = external_search_observed(
        adapter,
        SearchQuery(text="neutral query", lang="ru", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=effort.budget,
        product_id="p1",
        cache=cache,
        clock=lambda: FIXED_TS,
    )
    assert outcome.from_cache is True
    assert outcome.charged is False
    assert len(outcome.results) == 2
    assert effort.used("serp", "p1") == 1
    assert len(adapter.calls) == 1


def test_cache_key_differs_by_language(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    """Ось языка: одинаковый текст на разных языках — разные записи и живые вызовы."""
    adapter = _FakeAdapter(_results())
    _search(store, adapter, cache=cache, query=SearchQuery(text="neutral query", lang="ru", phase="serp"))
    _search(store, adapter, cache=cache, query=SearchQuery(text="neutral query", lang="en", phase="serp"))

    assert len(adapter.calls) == 2  # ключи различны → второй запрос ушёл провайдеру
    assert len(list((cache.root / "serp").glob("*.json"))) == 2


def test_provider_without_declared_name_is_not_cached(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    """Без объявленного провайдера ключ неразличим между системами — не кэшируем."""

    class _Anonymous(_FakeAdapter):
        provider = ""

    adapter = _Anonymous(_results())
    _search(store, adapter, cache=cache)
    _search(store, adapter, cache=cache)
    assert len(adapter.calls) == 2
    assert not list((cache.root / "serp").glob("*.json"))


def test_error_is_not_cached(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    """Сбой в кэш не попадает: иначе R7 зафиксировал бы поломку как результат."""
    failing = _FakeAdapter(error=RuntimeError("backend down"))
    assert _search(store, failing, cache=cache) == []
    assert not list((cache.root / "serp").glob("*.json"))

    healthy = _FakeAdapter(_results())
    assert [r.url for r in _search(store, healthy, cache=cache)] == [URL_A, URL_B]
    assert len(healthy.calls) == 1


def test_corrupt_cache_entry_is_a_miss(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    adapter = _FakeAdapter(_results())
    _search(store, adapter, cache=cache)
    entry = next((cache.root / "serp").glob("*.json"))
    entry.write_text("{not json", encoding="utf-8")

    assert [r.url for r in _search(store, adapter, cache=cache)] == [URL_A, URL_B]
    assert len(adapter.calls) == 2  # порча записи = промах, а не отказ прогона


def test_empty_result_is_cached_and_replayed(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    """«Не нашлось» — тоже ответ: повтор отдаётся без живого вызова, но с not_found."""
    adapter = _FakeAdapter([])
    _search(store, adapter, cache=cache)
    outcome = external_search_observed(
        adapter,
        SearchQuery(text="neutral query", lang="ru", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=SearchEffort(SearchBudget.from_mapping({"serp": 5})),
        budget=SearchBudget.from_mapping({"serp": 5}),
        product_id="p1",
        cache=cache,
        clock=lambda: FIXED_TS,
    )
    assert outcome.from_cache is True
    assert outcome.termination is TerminationReason.NOT_FOUND
    assert len(adapter.calls) == 1


def test_external_search_reports_error_in_outcome(
    store: SqliteStore, cache: ContentAddressedCache
) -> None:
    """S8: сбой адаптера виден вызывающему, а не растворяется в пустом списке."""
    outcome = external_search_observed(
        _FakeAdapter(error=RuntimeError("backend down")),
        SearchQuery(text="neutral query", lang="ru", phase="serp"),
        store=store,
        run_id=RUN_ID,
        effort=SearchEffort(SearchBudget.from_mapping({"serp": 5})),
        budget=SearchBudget.from_mapping({"serp": 5}),
        product_id="p1",
        cache=cache,
        clock=lambda: FIXED_TS,
    )
    assert outcome.results == ()
    assert isinstance(outcome.error, RuntimeError)
    assert outcome.termination is TerminationReason.ERROR

# --------------------------------------------------------------------------- #
# Нейтральность модуля (§1.3)
# --------------------------------------------------------------------------- #


