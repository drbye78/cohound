"""Тесты SERP-петли (W11/T11.3): append-only раунды + инвалидация ячеек (K2).

Проверяются DoD и инварианты:

* один раунд порождает **новую** сборку;
* два раунда → ``R1``, затем ``R2`` с **различными** ``index_build_id`` (append-only);
* затронутые ячейки получают ``stale_reason='new_build'`` (K2), незатронутые — нет;
* попытка переиспользовать/перезаписать сборку поднимает ``ImmutableBuildError``;
* детерминизм (те же входы → те же id/манифест);
* ``to_manifest_json`` — round-trip.

Хранилище — ``SqliteStore(":memory:")``; адаптер поиска — детерминированный фейк.
Слоёвая гигиена: ``core.adapters`` не импортируется (матрица 01 §1).
Фикстуры категорийно-нейтральны: ``p1/p2``, example.com, «Example».
"""

from __future__ import annotations

import datetime as _dt
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import core.pipeline.serp_loop as module
from core.domain import (
    Cell,
    CellState,
    Focus,
    IndexManifestEntry,
    LLMEvidence,
    RankingPolicy,
    RunConfig,
    RunMode,
    StaleReason,
    Status,
    TerminationReason,
)
from core.pipeline.corpus import IndexBuild
from core.pipeline.search_log import SearchBudget, SearchEffort
from core.pipeline.serp_loop import (
    ImmutableBuildError,
    SerpLoopResult,
    SerpLoopStoreGap,
    assert_build_id_new,
    default_round_builder,
    derive_round_build_id,
    invalidate_affected,
    run_serp_loop,
    run_serp_round,
)
from core.ports.search import SearchQuery, SearchResult
from core.ports.types import RawPage
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-serp"
FIXED_TS = "2026-10-08T00:00:00+00:00"
BASE_BUILD = "ib_base_round"
URL_A = "https://example.com/a"
URL_B = "https://example.com/b"
URL_C = "https://example.com/c"


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
    """Детерминированный фейк ``SearchAdapterPort`` со счётчиком вызовов."""

    adapter_kind = "serp"
    provider = "fake"

    def __init__(self, results: list[SearchResult] | None = None) -> None:
        self.results = list(results or [])
        self.calls: list[SearchQuery] = []

    def search(self, q: SearchQuery) -> list[SearchResult]:
        self.calls.append(q)
        return list(self.results)

    def fetch(self, url: str) -> RawPage:  # pragma: no cover - не используется
        raise NotImplementedError


def _results() -> list[SearchResult]:
    return [
        SearchResult(url=URL_A, title="Example A", rank=1),
        SearchResult(url=URL_B, title="Example B", rank=2),
    ]


def _cell(product_id: str, param_id: str, build_id: str | None) -> Cell:
    """Evaluated-ячейка с одним evidence (проходит I1), привязанная к сборке (R15)."""
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        evidence=[LLMEvidence(source_id=f"p:{product_id}:0", quote="example")],
        index_build_id=build_id,
    )


def _queries() -> dict[str, list[SearchQuery]]:
    return {"p1": [SearchQuery(text="neutral query", lang="ru", phase="serp")]}


def _round(store: SqliteStore, adapter: _FakeAdapter, **kwargs: Any):
    budget = kwargs.pop("budget", SearchBudget.from_mapping({"serp": 5}))
    effort = kwargs.pop("effort", SearchEffort(budget))
    return run_serp_round(
        adapter=adapter,
        queries_by_product=kwargs.pop("queries_by_product", _queries()),
        store=store,
        run_id=RUN_ID,
        effort=effort,
        budget=budget,
        package="pkg-1",
        clock=lambda: FIXED_TS,
        **kwargs,
    )


def _manifest_rows(store: SqliteStore) -> list[Any]:
    return list(
        store.conn.execute(
            "SELECT * FROM index_manifest ORDER BY index_build_id, product_id"
        ).fetchall()
    )


def _cell_row(store: SqliteStore, product_id: str, param_id: str) -> Any:
    return store.conn.execute(
        "SELECT * FROM cells WHERE run_id = ? AND product_id = ? AND param_id = ?",
        (RUN_ID, product_id, param_id),
    ).fetchone()


# --------------------------------------------------------------------------- #
# Один раунд → новая сборка
# --------------------------------------------------------------------------- #


def test_one_round_creates_new_build(store: SqliteStore) -> None:
    adapter = _FakeAdapter(_results())
    rnd = _round(store, adapter, parent_build_id=BASE_BUILD, round_no=1)

    assert rnd.round_no == 1
    assert rnd.build.index_build_id != BASE_BUILD
    assert rnd.build.index_build_id.startswith("ib_")
    assert len(adapter.calls) == 1
    assert rnd.search_calls == 1
    assert [c.url for c in rnd.candidates] == [URL_A, URL_B]

    rows = _manifest_rows(store)
    assert len(rows) == 1
    assert rows[0]["index_build_id"] == rnd.build.index_build_id
    assert rows[0]["product_id"] == "p1"


def test_round_returns_candidates_not_evidence_i19(store: SqliteStore) -> None:
    rnd = _round(store, _FakeAdapter(_results()))
    assert all(isinstance(c, SearchResult) for c in rnd.candidates)
    for candidate in rnd.candidates:
        for banned in ("source_id", "citation_quote", "content_hash", "evidence"):
            assert not hasattr(candidate, banned)


def test_empty_queries_make_empty_build_without_search(store: SqliteStore) -> None:
    adapter = _FakeAdapter(_results())
    rnd = _round(store, adapter, queries_by_product={})
    assert adapter.calls == []
    assert rnd.candidates == []
    assert rnd.build.index_build_id  # новый id всё равно детерминирован
    assert _manifest_rows(store) == []


# --------------------------------------------------------------------------- #
# Два раунда → R1, R2 с различными id (append-only)
# --------------------------------------------------------------------------- #


def test_two_rounds_distinct_builds_append_only(store: SqliteStore) -> None:
    adapter = _FakeAdapter(_results())
    result = run_serp_loop(
        adapter=adapter,
        queries_by_product=_queries(),
        store=store,
        run_id=RUN_ID,
        effort=SearchEffort(SearchBudget.from_mapping({"serp": 5})),
        budget=SearchBudget.from_mapping({"serp": 5}),
        rounds=2,
        parent_build_id=BASE_BUILD,
        package="pkg-1",
        clock=lambda: FIXED_TS,
    )

    assert [r.round_no for r in result.rounds] == [1, 2]
    ids = [r.build.index_build_id for r in result.rounds]
    assert ids[0] != ids[1] != BASE_BUILD
    assert BASE_BUILD not in ids
    # Цепочка: R2 строится от сборки R1 (родитель меняется на каждой итерации).
    assert result.final_build is not None
    assert result.final_build.index_build_id == ids[1]

    rows = _manifest_rows(store)
    assert {row["index_build_id"] for row in rows} == set(ids)
    # Ни одна строка не указывает на базовую сборку — её не перезаписывали.
    assert all(row["index_build_id"] != BASE_BUILD for row in rows)


def test_loop_zero_rounds_is_empty() -> None:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        result = run_serp_loop(
            adapter=_FakeAdapter(),
            queries_by_product={},
            store=s,
            run_id=RUN_ID,
            effort=SearchEffort(),
            rounds=0,
        )
        assert result.rounds == []
        assert result.final_build is None


def test_loop_rejects_negative_rounds(store: SqliteStore) -> None:
    with pytest.raises(ValueError):
        run_serp_loop(
            adapter=_FakeAdapter(),
            queries_by_product=_queries(),
            store=store,
            run_id=RUN_ID,
            effort=SearchEffort(),
            rounds=-1,
        )


# --------------------------------------------------------------------------- #
# Инвалидация затронутых ячеек (K2)
# --------------------------------------------------------------------------- #


def test_affected_cells_marked_new_build_unaffected_untouched(store: SqliteStore) -> None:
    store.save_cell(_cell("p1", "param.a", BASE_BUILD))
    store.save_cell(_cell("p2", "param.b", BASE_BUILD))
    store.save_cell(_cell("p1", "param.c", "ib_other_round"))  # не затронута

    invalidated = invalidate_affected(store, RUN_ID, BASE_BUILD, "ib_new_round")

    # Детерминированный порядок (product_id, param_id).
    assert [(c.product_id, c.param_id) for c in invalidated] == [
        ("p1", "param.a"),
        ("p2", "param.b"),
    ]
    assert all(c.stale_reason is StaleReason.NEW_BUILD for c in invalidated)
    # Привязка к сборке сохранена (переоценка — отдельный повторный Ш6, §13).
    assert all(c.index_build_id == BASE_BUILD for c in invalidated)

    assert _cell_row(store, "p1", "param.a")["stale_reason"] == "new_build"
    assert _cell_row(store, "p2", "param.b")["stale_reason"] == "new_build"
    untouched = _cell_row(store, "p1", "param.c")
    assert untouched["stale_reason"] is None
    assert untouched["index_build_id"] == "ib_other_round"


def test_invalidate_is_idempotent(store: SqliteStore) -> None:
    store.save_cell(_cell("p1", "param.a", BASE_BUILD))
    first = invalidate_affected(store, RUN_ID, BASE_BUILD, "ib_new")
    second = invalidate_affected(store, RUN_ID, BASE_BUILD, "ib_new")
    assert len(first) == 1
    assert second == []  # уже помечена — повторный вызов не трогает строку


def test_invalidate_can_be_scoped(store: SqliteStore) -> None:
    store.save_cell(_cell("p1", "param.a", BASE_BUILD))
    store.save_cell(_cell("p2", "param.b", BASE_BUILD))
    invalidated = invalidate_affected(
        store, RUN_ID, BASE_BUILD, "ib_new", product_ids=["p2"]
    )
    assert [(c.product_id, c.param_id) for c in invalidated] == [("p2", "param.b")]
    assert _cell_row(store, "p1", "param.a")["stale_reason"] is None


def test_invalidate_rejects_same_or_empty_ids(store: SqliteStore) -> None:
    with pytest.raises(ValueError):
        invalidate_affected(store, RUN_ID, BASE_BUILD, BASE_BUILD)
    with pytest.raises(ValueError):
        invalidate_affected(store, RUN_ID, "", "ib_new")


def test_loop_invalidates_affected_cells_end_to_end(store: SqliteStore) -> None:
    store.save_cell(_cell("p1", "param.a", BASE_BUILD))
    store.save_cell(_cell("p1", "param.skip", "ib_other"))

    run_serp_loop(
        adapter=_FakeAdapter(_results()),
        queries_by_product=_queries(),
        store=store,
        run_id=RUN_ID,
        effort=SearchEffort(SearchBudget.from_mapping({"serp": 5})),
        budget=SearchBudget.from_mapping({"serp": 5}),
        rounds=2,
        parent_build_id=BASE_BUILD,
        package="pkg-1",
        clock=lambda: FIXED_TS,
    )

    assert _cell_row(store, "p1", "param.a")["stale_reason"] == "new_build"
    assert _cell_row(store, "p1", "param.skip")["stale_reason"] is None


# --------------------------------------------------------------------------- #
# Иммутабельность: переиспользование/перезапись сборки запрещены (K2/§13)
# --------------------------------------------------------------------------- #


def test_reusing_parent_build_id_raises(store: SqliteStore) -> None:
    adapter = _FakeAdapter(_results())
    with pytest.raises(ImmutableBuildError):
        _round(
            store,
            adapter,
            parent_build_id=BASE_BUILD,
            round_no=1,
            make_build_id=lambda parent, no, token: BASE_BUILD,
        )


def test_existing_build_id_raises(store: SqliteStore) -> None:
    adapter = _FakeAdapter(_results())
    with pytest.raises(ImmutableBuildError):
        _round(
            store,
            adapter,
            existing_build_ids={"ib_known"},
            make_build_id=lambda parent, no, token: "ib_known",
        )


def test_append_only_rerun_same_round_raises(store: SqliteStore) -> None:
    adapter = _FakeAdapter(_results())
    first = _round(store, adapter, parent_build_id=BASE_BUILD, round_no=1)
    # Повтор того же раунда с той же родительской сборкой даёт тот же id — запись поверх
    # существующей сборки запрещена (append-only, K2).
    with pytest.raises(ImmutableBuildError):
        _round(
            store,
            adapter,
            parent_build_id=BASE_BUILD,
            round_no=1,
            existing_build_ids={first.build.index_build_id},
        )


def test_builder_immutable_error_is_translated(store: SqliteStore) -> None:
    class ImmutableBuildError(RuntimeError):  # одноимённый адаптерный тип
        pass

    def _builder(ctx: Any) -> IndexBuild:
        raise ImmutableBuildError("adapters: сборка уже существует")

    with pytest.raises(module.ImmutableBuildError):
        _round(store, _FakeAdapter(_results()), builder=_builder)


def test_assert_build_id_new_guards() -> None:
    assert_build_id_new(None, "ib_x")
    with pytest.raises(ImmutableBuildError):
        assert_build_id_new("ib_a", "ib_a")
    with pytest.raises(ImmutableBuildError):
        assert_build_id_new(None, "")
    with pytest.raises(ImmutableBuildError):
        assert_build_id_new(None, "ib_x", existing_build_ids={"ib_x"})


def test_builder_may_return_foreign_id_but_round_renormalizes(store: SqliteStore) -> None:
    def _builder(ctx: Any) -> IndexBuild:
        return IndexBuild(
            index_build_id="ib_builder_derived",
            entries=[
                IndexManifestEntry(
                    index_build_id="ib_builder_derived", product_id="p1", package_id="pkg-1"
                )
            ],
            docs=[],
            chunk_count=0,
        )

    rnd = _round(
        store,
        _FakeAdapter(_results()),
        parent_build_id=BASE_BUILD,
        round_no=1,
        builder=_builder,
    )
    assert rnd.build.index_build_id != "ib_builder_derived"
    assert rnd.build.index_build_id.startswith("ib_")
    assert rnd.build.entries[0].index_build_id == rnd.build.index_build_id


# --------------------------------------------------------------------------- #
# Детерминизм и сериализация
# --------------------------------------------------------------------------- #


def test_round_is_deterministic(store: SqliteStore) -> None:
    kwargs = dict(parent_build_id=BASE_BUILD, round_no=1)
    first = _round(store, _FakeAdapter(_results()), **kwargs)
    second = _round(store, _FakeAdapter(_results()), **kwargs)
    assert first.build.index_build_id == second.build.index_build_id
    assert [c.url for c in first.candidates] == [c.url for c in second.candidates]


def test_derive_round_build_id_chain_and_sensitivity() -> None:
    a = derive_round_build_id(BASE_BUILD, 1, "pkg", "p1\x1e1\x1eurl")
    b = derive_round_build_id(a, 2, "pkg", "p1\x1e1\x1eurl")
    c = derive_round_build_id(BASE_BUILD, 1, "pkg", "p1\x1e1\x1eOTHER")
    assert a != b  # цепочка
    assert a != c  # чувствительность к кандидатам
    assert a.startswith("ib_") and len(a) == len("ib_") + 24
    assert derive_round_build_id(BASE_BUILD, 1, "pkg", "p1\x1e1\x1eurl") == a  # детерминизм


def test_loop_is_deterministic(store: SqliteStore) -> None:
    def _run() -> SerpLoopResult:
        return run_serp_loop(
            adapter=_FakeAdapter(_results()),
            queries_by_product=_queries(),
            store=store,
            run_id=RUN_ID,
            effort=SearchEffort(SearchBudget.from_mapping({"serp": 5})),
            budget=SearchBudget.from_mapping({"serp": 5}),
            rounds=2,
            parent_build_id=BASE_BUILD,
            package="pkg-1",
            clock=lambda: FIXED_TS,
        )

    assert _run().to_manifest_json() == _run().to_manifest_json()


def test_to_manifest_json_round_trip(store: SqliteStore) -> None:
    result = run_serp_loop(
        adapter=_FakeAdapter(_results()),
        queries_by_product=_queries(),
        store=store,
        run_id=RUN_ID,
        effort=SearchEffort(SearchBudget.from_mapping({"serp": 5})),
        budget=SearchBudget.from_mapping({"serp": 5}),
        rounds=2,
        parent_build_id=BASE_BUILD,
        package="pkg-1",
        clock=lambda: FIXED_TS,
    )
    payload = json.loads(result.to_manifest_json())
    final = result.final_build
    assert final is not None
    assert payload["final_build_id"] == final.index_build_id
    assert [r["round_no"] for r in payload["rounds"]] == [1, 2]
    assert payload["rounds"][0]["candidates"][0]["url"] == URL_A
    assert payload["rounds"][0]["termination_reason"] is None

    empty = json.loads(SerpLoopResult().to_manifest_json())
    assert empty == {"rounds": [], "final_build_id": None}


# --------------------------------------------------------------------------- #
# Бюджет и гэпы хранилища
# --------------------------------------------------------------------------- #


def test_exhausted_budget_terminates_round(store: SqliteStore) -> None:
    budget = SearchBudget.from_mapping({"serp": 0})
    adapter = _FakeAdapter(_results())
    rnd = _round(store, adapter, budget=budget, effort=SearchEffort(budget))
    assert rnd.candidates == []
    assert rnd.search_calls == 0
    assert rnd.termination_reason is TerminationReason.BUDGET_EXHAUSTED
    assert adapter.calls == []  # бюджет исчерпан до вызова адаптера


def test_loop_stops_on_budget_exhaustion(store: SqliteStore) -> None:
    budget = SearchBudget.from_mapping({"serp": 1})
    adapter = _FakeAdapter(_results())
    result = run_serp_loop(
        adapter=adapter,
        queries_by_product=_queries(),
        store=store,
        run_id=RUN_ID,
        effort=SearchEffort(budget),
        budget=budget,
        rounds=3,
        parent_build_id=BASE_BUILD,
        package="pkg-1",
        clock=lambda: FIXED_TS,
    )
    # R1 израсходовал единственный вызов; R2 терминально исчерпан → петля остановлена.
    assert [r.round_no for r in result.rounds] == [1, 2]
    assert result.rounds[-1].termination_reason is TerminationReason.BUDGET_EXHAUSTED


def test_missing_store_method_raises_store_gap() -> None:
    class _BareStore:
        """Store без ``save_index_manifest`` — гэп фиксируется типизированно."""

    with pytest.raises(SerpLoopStoreGap):
        run_serp_round(
            adapter=_FakeAdapter(_results()),
            queries_by_product=_queries(),
            store=_BareStore(),  # type: ignore[arg-type] — намеренно неполный store
            run_id=RUN_ID,
            effort=SearchEffort(),
        )


def test_invalidate_missing_store_method_raises_store_gap() -> None:
    class _NoIterStore:
        def save_cells_batch(self, cells: list[Cell]) -> None:  # pragma: no cover
            pass

    with pytest.raises(SerpLoopStoreGap):
        invalidate_affected(_NoIterStore(), RUN_ID, "ib_old", "ib_new")  # type: ignore[arg-type]


def test_default_round_builder_uses_explicit_new_id() -> None:
    from core.pipeline.serp_loop import RoundBuildContext

    ctx = RoundBuildContext(
        new_build_id="ib_explicit",
        round_no=3,
        package="pkg-1",
        snapshot_date=FIXED_TS,
        candidates_by_product={"p1": _results()},
    )
    build = default_round_builder(ctx)
    assert build.index_build_id == "ib_explicit"
    assert build.entries[0].index_build_id == "ib_explicit"
    assert build.entries[0].partition_key == "ib_explicit:p1"


# --------------------------------------------------------------------------- #
# Нейтральность и слоевая гигиена (§1.3, 01 §1)
# --------------------------------------------------------------------------- #


def test_module_source_is_category_neutral() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    assert [line for line in source.splitlines() if forbidden.search(line)] == []


def test_module_does_not_import_adapters() -> None:
    """Матрица зависимостей 01 §1: pipeline не импортирует adapters."""
    source = Path(module.__file__).read_text(encoding="utf-8")
    imports = [
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith(("import ", "from "))
    ]
    assert all("core.adapters" not in line for line in imports)
