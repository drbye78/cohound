"""Тесты ``serp_discover`` — производственного прохода SERP-контура (S3/S7/S8, P1).

Покрывается то, чего раньше не проверял **ни один** тест (0 тестов на ``serp_discover``):

* **S3** — раунд, не меняющий набор запросов, не исполняется и не жжёт бюджет; отказ
  виден в наблюдении (``rounds.not_executed``/``rounds.not_executed_reason``);
* **S7** — ``queries`` пишутся до ``continue``, поэтому знаменатель «принято/queries»
  не завышается пустыми выдачами и сбоями;
* **S8** — сбой адаптера даёт ``termination_reason=error`` и считается в ``errors``,
  а не выглядит как «ничего не нашлось»;
* **P1** — повторный проход с тем же кэшем не делает живых вызовов: ответ берётся из
  ``cache/serp``, бюджет не списывается, но запрос считается обслуженным;
* **S9d** — дедупликация кандидатов нормализует percent-encoding и dot-segments.

Фикстуры категорийно-нейтральны: ``p1``, example.com, «Example».
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
    TerminationReason,
)
from core.pipeline.search_log import SearchBudget, SearchEffort
from core.pipeline.serp_loop import (
    EARLY_TERMINATION_REASON,
    NO_QUERY_VARIATION_REASON,
    QUERY_OUTCOME_BUDGET,
    QUERY_OUTCOME_ERROR,
    QUERY_OUTCOME_NO_RESULTS,
    QUERY_OUTCOME_RESULTS,
    serp_discover,
)
from core.ports.search import SearchQuery, SearchResult, SearchTiming
from core.ports.types import RawPage
from core.store.sqlite_store import SqliteStore
from core.util.cache import ContentAddressedCache

RUN_ID = "run-serp-discover"
FIXED_TS = "2026-10-08T00:00:00+00:00"
URL_A = "https://example.com/a"

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
        products=["p1"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=["param.a"]),
        languages=["ru", "en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )

class _FakeAdapter:
    """Детерминированный фейк ``SearchAdapterPort`` со счётчиком вызовов."""

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

def _queries(text: str = "neutral query", lang: str = "ru") -> dict[str, list[SearchQuery]]:
    return {"p1": [SearchQuery(text=text, lang=lang, phase="serp")]}

def _discover(
    store: SqliteStore,
    adapter: _FakeAdapter,
    *,
    rounds: int = 1,
    queries: dict[str, list[SearchQuery]] | None = None,
    cache: ContentAddressedCache | None = None,
    budget: int = 5,
    known: list[str] | None = None,
    mode: str | None = None,
) -> Any:
    effort = SearchEffort(SearchBudget.from_mapping({"serp": budget * rounds}))
    return serp_discover(
        adapter=adapter,
        queries_by_product=queries if queries is not None else _queries(),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        rounds=rounds,
        own_hosts_by_product={"p1": frozenset({"example.com"})},
        declared_hosts=frozenset({"example.com"}),
        known_urls_by_product={"p1": list(known or [])},
        source="test",
        budget_declared=budget,
        mode=mode,
        cache=cache,
        clock=lambda: FIXED_TS,
    )

def test_mode_is_recorded_as_declared_and_used(store: SqliteStore) -> None:
    """Режим поиска записан **парой** «объявлено/исполнено», а не одним значением.

    Объявленный режим — условие измерения (входит в ``run_id`` как ``serp_mode``),
    исполненный — свойство собранного адаптера. Если сборка не донесла объявленное
    значение, это видно здесь, а не выводится догадкой «наверное, тот же».
    """
    adapter = _FakeAdapter([SearchResult(url=URL_A, title="Example", rank=1)])
    adapter.search_mode = "sync"

    observation = _discover(store, adapter, mode="sync").to_observation()

    assert observation["mode"] == {"declared": "sync", "used": "sync"}

def test_mode_used_differs_from_declared_is_visible(store: SqliteStore) -> None:
    """Расхождение «объявлено ≠ исполнено» — видимое наблюдение, а не догадка."""
    adapter = _FakeAdapter([SearchResult(url=URL_A, title="Example", rank=1)])
    adapter.search_mode = "async"

    observation = _discover(store, adapter, mode="sync").to_observation()

    assert observation["mode"] == {"declared": "sync", "used": "async"}

# --------------------------------------------------------------------------- #
# S3: раунд без новых запросов не исполняется
# --------------------------------------------------------------------------- #

def test_rounds_above_one_without_query_variation_are_not_executed(
    store: SqliteStore,
) -> None:
    """S3: запросы между раундами не меняются → раунды 2..n не жгут бюджет."""
    adapter = _FakeAdapter([SearchResult(url=URL_A, title="Example", rank=1)])

    discovery = _discover(store, adapter, rounds=3, budget=5)

    assert len(adapter.calls) == 1  # ровно один живой вызов, а не три
    assert discovery.executed_rounds == 1
    assert discovery.not_executed_rounds == 2
    assert discovery.not_executed_reason == NO_QUERY_VARIATION_REASON
    observation = discovery.to_observation()
    assert observation["rounds"]["declared"] == 3
    assert observation["rounds"]["executed"] == 1
    assert observation["rounds"]["not_executed"] == 2
    assert observation["rounds"]["not_executed_reason"] == "no_query_variation"
    # Бюджет израсходован на один запрос, а не на три (корпус/бюджет × rounds).
    assert observation["budget"]["used"] == {"p1": 1}
    assert discovery.accepted_total == 1

def test_single_round_has_no_not_executed_marker(store: SqliteStore) -> None:
    adapter = _FakeAdapter([SearchResult(url=URL_A, rank=1)])
    discovery = _discover(store, adapter, rounds=1)
    assert discovery.executed_rounds == 1
    assert discovery.not_executed_rounds == 0
    assert discovery.not_executed_reason is None

# --------------------------------------------------------------------------- #
# S7: queries_log пишется до continue
# --------------------------------------------------------------------------- #

def test_queries_log_records_empty_and_failed_calls(store: SqliteStore) -> None:
    """S7: выпущенный запрос попадает в ``queries``, даже если выдача пуста."""
    empty = _discover(store, _FakeAdapter([]), rounds=1)
    assert empty.queries_by_product == {"p1": ["neutral query"]}
    assert empty.termination_by_product == {"p1": TerminationReason.NOT_FOUND.value}

def test_queries_log_is_not_duplicated_across_rounds(store: SqliteStore) -> None:
    adapter = _FakeAdapter([SearchResult(url=URL_A, rank=1)])
    discovery = _discover(store, adapter, rounds=2)
    assert discovery.queries_by_product == {"p1": ["neutral query"]}

# --------------------------------------------------------------------------- #
# S8: сбой адаптера отличим от «не нашлось»
# --------------------------------------------------------------------------- #

def test_adapter_error_terminates_as_error(store: SqliteStore) -> None:
    adapter = _FakeAdapter(error=RuntimeError("search backend down"))
    discovery = _discover(store, adapter, rounds=1)

    assert discovery.errors_by_product == {"p1": 1}
    assert discovery.termination_by_product == {"p1": TerminationReason.ERROR.value}
    observation = discovery.to_observation()
    assert observation["errors"] == {"p1": 1}
    assert observation["termination"] == {"p1": "error"}

def test_not_found_stays_not_found_and_is_not_an_error(store: SqliteStore) -> None:
    discovery = _discover(store, _FakeAdapter([]), rounds=1)
    assert discovery.errors_by_product == {}
    assert discovery.to_observation()["termination"] == {"p1": "not_found"}

def test_error_of_one_product_does_not_rename_another(store: SqliteStore) -> None:
    """S8: терминация считается **по продукту** — сбой одного не «заражает» другой."""

    class _SelectiveAdapter(_FakeAdapter):
        def search(self, q: SearchQuery) -> list[SearchResult]:
            self.calls.append(q)
            if "p1" in q.text:
                raise RuntimeError("p1 backend down")
            return []

    queries = {
        "p1": [SearchQuery(text="p1 query", lang="ru", phase="serp")],
        "p2": [SearchQuery(text="p2 query", lang="ru", phase="serp")],
    }
    effort = SearchEffort(SearchBudget.from_mapping({"serp": 5}))
    discovery = serp_discover(
        adapter=_SelectiveAdapter(),
        queries_by_product=queries,
        store=store,
        run_id=RUN_ID,
        effort=effort,
        rounds=1,
        own_hosts_by_product={"p1": frozenset({"example.com"})},
        declared_hosts=frozenset({"example.com"}),
        known_urls_by_product={"p1": [], "p2": []},
        source="test",
        budget_declared=5,
        clock=lambda: FIXED_TS,
    )

    assert discovery.errors_by_product == {"p1": 1}
    assert discovery.termination_by_product == {
        "p1": TerminationReason.ERROR.value,
        "p2": TerminationReason.NOT_FOUND.value,
    }

# --------------------------------------------------------------------------- #
# P1: повторный проход с тем же кэшем — без живых вызовов
# --------------------------------------------------------------------------- #

def test_second_discovery_run_is_served_from_cache(
    store: SqliteStore, tmp_path: Path
) -> None:
    cache = ContentAddressedCache(tmp_path / "cache")
    results = [SearchResult(url=URL_A, title="Example", rank=1)]

    first_adapter = _FakeAdapter(results)
    first = _discover(store, first_adapter, cache=cache)
    assert len(first_adapter.calls) == 1
    assert first.to_observation()["budget"]["charged"] == {"p1": 1}

    second_adapter = _FakeAdapter(results)
    second = _discover(store, second_adapter, cache=cache)
    assert second_adapter.calls == []  # R7: живого вызова нет
    assert second.cache_hits_by_product == {"p1": 1}
    observation = second.to_observation()
    # Обслужено (запрос исполнен), но бюджет не списан.
    assert observation["budget"]["used"] == {"p1": 1}
    assert observation["budget"]["charged"] == {"p1": 0}
    assert observation["budget"]["from_cache"] == {"p1": 1}
    assert second.accepted_total == 1

def test_cache_falls_back_to_adapter_cache(store: SqliteStore, tmp_path: Path) -> None:
    """Кэш адаптера — тот же объект: второй точки построения кэша нет."""
    cache = ContentAddressedCache(tmp_path / "cache")
    results = [SearchResult(url=URL_A, rank=1)]

    live = _FakeAdapter(results)
    _discover(store, live, cache=cache)

    cached_adapter = _FakeAdapter(results)
    cached_adapter.cache = cache  # type: ignore[attr-defined]
    discovery = _discover(store, cached_adapter)
    assert cached_adapter.calls == []
    assert discovery.cache_hits_by_product == {"p1": 1}

# --------------------------------------------------------------------------- #
# S9d: дедупликация кандидатов нормализует URL
# --------------------------------------------------------------------------- #

def test_candidate_dedup_normalizes_percent_and_dot_segments(
    store: SqliteStore,
) -> None:
    adapter = _FakeAdapter(
        [
            SearchResult(url="https://example.com/a/b", rank=1),
            SearchResult(url="https://example.com/a/./b", rank=2),
            SearchResult(url="https://EXAMPLE.com/a/%62", rank=3),
            SearchResult(url="https://example.com/a/b/../b", rank=4),
        ]
    )
    discovery = _discover(store, adapter, rounds=1)

    assert discovery.accepted_by_product["p1"] == ["https://example.com/a/b"]
    assert discovery.rejected_by_product["p1"] == {"known": 3}

def test_known_urls_are_normalized_too(store: SqliteStore) -> None:
    """Известный корпус сравнивается тем же ключом: ``%61`` не даёт «новый» документ."""
    adapter = _FakeAdapter([SearchResult(url="https://example.com/a/b", rank=1)])
    discovery = _discover(
        store, adapter, rounds=1, known=["https://example.com/a/%62"]
    )
    assert discovery.accepted_by_product["p1"] == []
    assert discovery.rejected_by_product["p1"] == {"known": 1}
# --------------------------------------------------------------------------- #
# S4: стоимость ожидания видна в наблюдении контура
# --------------------------------------------------------------------------- #

class _TimedFakeAdapter(_FakeAdapter):
    """Фейк, публикующий тайминг каждого вызова (как реальный SERP-адаптер)."""

    def __init__(
        self,
        results: list[SearchResult] | None = None,
        timings: list[SearchTiming] | None = None,
        error: BaseException | None = None,
    ) -> None:
        super().__init__(results=results, error=error)
        self._timings = list(timings or [])
        self.last_timing = SearchTiming()

    def search(self, q: SearchQuery) -> list[SearchResult]:
        if self._timings:
            self.last_timing = self._timings.pop(0)
        return super().search(q)


def test_timing_is_aggregated_per_product_and_published(store: SqliteStore) -> None:
    """S4: время вызовов складывается по продукту и видно в наблюдении контура."""
    adapter = _TimedFakeAdapter(
        [SearchResult(url=URL_A, rank=1)],
        timings=[
            SearchTiming(wait_seconds=30.0, request_seconds=1.0, polls=2, measured=True),
            SearchTiming(wait_seconds=30.0, request_seconds=0.5, polls=2, measured=True),
        ],
    )

    discovery = _discover(
        store,
        adapter,
        rounds=1,
        queries={
            "p1": [
                SearchQuery(text="neutral query", lang="ru", phase="serp"),
                SearchQuery(text="neutral query two", lang="ru", phase="serp"),
            ]
        },
    )

    assert discovery.timing_by_product["p1"].wait_seconds == pytest.approx(60.0)
    timing = discovery.to_observation()["timing"]["p1"]
    assert timing["measured"] is True
    assert timing["wait_seconds"] == pytest.approx(60.0)
    assert timing["request_seconds"] == pytest.approx(1.5)
    assert timing["polls"] == 4
    assert timing["from_cache"] is False


def test_zero_wait_is_published_not_omitted(store: SqliteStore) -> None:
    """«Ноль ожидания» — факт: поле присутствует и равно 0.0, а не отсутствует."""
    adapter = _TimedFakeAdapter(
        [SearchResult(url=URL_A, rank=1)],
        timings=[SearchTiming(wait_seconds=0.0, request_seconds=0.2, polls=1, measured=True)],
    )

    observation = _discover(store, adapter, rounds=1).to_observation()

    assert "timing" in observation
    assert observation["timing"]["p1"]["wait_seconds"] == 0.0
    assert observation["timing"]["p1"]["measured"] is True


def test_adapter_without_timing_is_not_published_as_zero(store: SqliteStore) -> None:
    """Шов без таймингов: ``measured=False``, а не «ждали ноль секунд»."""
    discovery = _discover(
        store, _FakeAdapter([SearchResult(url=URL_A, rank=1)]), rounds=1
    )

    timing = discovery.to_observation()["timing"]["p1"]
    assert timing["measured"] is False
    assert timing["wait_seconds"] == 0.0

# --------------------------------------------------------------------------- #
# Арифметика раундов и исход каждого запроса (S3/S7/S8)
# --------------------------------------------------------------------------- #

def test_rounds_arithmetic_closes_on_early_termination(store: SqliteStore) -> None:
    """``executed + not_executed == declared`` и на ранней терминации.

    До правки ранний выход писал ``not_executed=0``: наблюдение утверждало, что
    объявленные раунды исполнены (или не объявлены), и «сколько бюджета осталось на
    бумаге» было неверным.
    """
    adapter = _FakeAdapter([SearchResult(url=URL_A, rank=1)])

    discovery = _discover(store, adapter, rounds=3, known=[URL_A])

    rounds = discovery.to_observation()["rounds"]
    assert rounds["declared"] == 3
    assert rounds["executed"] == 1
    assert rounds["not_executed"] == 2
    assert rounds["not_executed_reason"] == EARLY_TERMINATION_REASON
    assert rounds["executed"] + rounds["not_executed"] == rounds["declared"]

def test_query_outcomes_record_no_results_as_observation(store: SqliteStore) -> None:
    """S7: пустая выдача — строка ``no_results``, а не отсутствие записи."""
    discovery = _discover(store, _FakeAdapter([]), rounds=1)

    outcomes = discovery.to_observation()["query_outcomes"]["p1"]
    assert [row["outcome"] for row in outcomes] == [QUERY_OUTCOME_NO_RESULTS]
    assert outcomes[0]["query"] == "neutral query"
    assert outcomes[0]["results"] == 0

def test_query_outcomes_carry_accepted_per_query(store: SqliteStore) -> None:
    """Строка запроса знает свой вклад: сколько кандидатов и сколько из них допущено."""
    adapter = _FakeAdapter(
        [
            SearchResult(url=URL_A, rank=1),
            SearchResult(url="https://example.com/a/./", rank=2),
        ]
    )
    discovery = _discover(store, adapter, rounds=1)

    row = discovery.to_observation()["query_outcomes"]["p1"][0]
    assert row["outcome"] == QUERY_OUTCOME_RESULTS
    assert row["results"] == 2
    assert row["accepted"] == 1  # второй — тот же документ (S9d)

def test_error_outcome_names_the_refusal(store: SqliteStore) -> None:
    """S6/S8: отказ после исчерпания повторов виден текстом, а не как «не нашлось».

    Адаптеры транслируют исчерпание ретраев в ``SearchError`` с числом попыток; без
    текста в наблюдении «провайдер отказал» и «выдача пуста» отличались бы только
    счётчиком ``errors``, а причина отказа терялась бы.
    """
    adapter = _FakeAdapter(error=RuntimeError("HTTP-поиск не удался: HTTP 503 после 5 попыток"))

    discovery = _discover(store, adapter, rounds=1)

    observation = discovery.to_observation()
    row = observation["query_outcomes"]["p1"][0]
    assert row["outcome"] == QUERY_OUTCOME_ERROR
    assert "после 5 попыток" in (row["error"] or "")
    assert observation["errors"] == {"p1": 1}
    # Списание видно: неуспешный вызов израсходовал сеть и бюджет.
    assert observation["budget"]["charged"] == {"p1": 1}

def test_yield_separates_errors_from_empty(store: SqliteStore) -> None:
    """S8: в знаменателе доходности сбой — отдельная строка, а не «ничего не нашлось»."""
    discovery = _discover(store, _FakeAdapter(error=RuntimeError("boom")), rounds=1)

    block = discovery.to_observation()["yield"]["p1"]
    assert block["errors"] == 1
    assert block["no_results"] == 0
    assert block["served"] == 1
    assert block["accepted_per_served"] == 0.0

def test_budget_refusal_is_not_served_and_not_in_denominator(
    store: SqliteStore,
) -> None:
    """S7: отказ бюджета — «вызова не было»; в знаменатель доходности он не входит."""
    adapter = _FakeAdapter([SearchResult(url=URL_A, rank=1)])
    queries = {
        "p1": [
            SearchQuery(text="neutral query", lang="ru", phase="serp"),
            SearchQuery(text="neutral query two", lang="ru", phase="serp"),
        ]
    }

    discovery = _discover(store, adapter, rounds=1, queries=queries, budget=1)

    block = discovery.to_observation()["yield"]["p1"]
    assert block["issued"] == 2
    assert block["served"] == 1
    assert block["budget_refused"] == 1
    # Доля считается от обслуженного запроса, а не от выпущенного: иначе она падала бы
    # из-за запроса, до провайдера не дошедшего.
    assert block["accepted_per_served"] == 1.0
    outcomes = [row["outcome"] for row in discovery.to_observation()["query_outcomes"]["p1"]]
    assert outcomes == [QUERY_OUTCOME_RESULTS, QUERY_OUTCOME_BUDGET]

def test_cache_hit_is_a_served_query_without_charge(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Попадание в кэш: запрос обслужен (входит в знаменатель), но живого вызова нет."""
    cache = ContentAddressedCache(tmp_path / "cache")
    results = [SearchResult(url=URL_A, rank=1)]
    _discover(store, _FakeAdapter(results), cache=cache)

    discovery = _discover(store, _FakeAdapter(results), cache=cache)

    row = discovery.to_observation()["query_outcomes"]["p1"][0]
    assert row["from_cache"] is True
    assert row["outcome"] == QUERY_OUTCOME_RESULTS
    block = discovery.to_observation()["yield"]["p1"]
    assert block["served"] == 1
    assert block["cached"] == 1
    assert discovery.to_observation()["budget"]["charged"] == {"p1": 0}

def test_query_with_foreign_phase_is_refused(store: SqliteStore) -> None:
    """I16: запрос с чужой фазой списал бы чужой бюджет — отказ с именем виновника."""
    queries = {"p1": [SearchQuery(text="neutral query", lang="ru", phase="corpus")]}

    with pytest.raises(ValueError) as err:
        _discover(store, _FakeAdapter([]), rounds=1, queries=queries)
    message = str(err.value)
    assert "corpus" in message
    assert "serp" in message
    assert "neutral query" in message
