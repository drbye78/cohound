"""``--only-failed``: ремонт как work-set **известных** URL, а не как обход (09 §6.3/§13.5).

Проверяется конвейерное поведение ``build_corpus`` с входом ``only_failed_repairs``:

* ремонт идёт depth-0 листьями (``Frontier.add_revalidate_seed``) — авторские корни **не**
  дренятся, ``seeds_enqueued`` остаётся нулём, BFS/sitemap не исполняются;
* режим продукта не гейт: ``new``/``full`` ремонтируют ровно переданное множество, а не
  ``url_state`` целиком (иначе это был бы полный краул);
* ``not_found`` не ретраится (терминальное наблюдение), отброс виден в заметке;
* провал вне объявленных хостов отброшен **видимо**;
* валидаторы условного GET переносятся из ``url_state``.

Контроль старого поведения — в том же файле: без ``only_failed_repairs`` тот же продукт в
режиме ``new`` обходит авторский корень (``crawler.calls == [ROOT]``). Один и тот же вход,
разные наборы работ — это и есть различие, которое вносит правка.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.ports.state import UrlStateRow
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash

RUN_ID = "run-repair"
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"

ROOT = "https://docs.test/p1/"
DEEP = "https://docs.test/p1/guide/deep/page"
OTHER_HOST = "https://elsewhere.test/p1/x"

PAGES = {
    ROOT: b"root page",
    DEEP: b"deep page",
}


# --------------------------------------------------------------------------- #
# Харнесс (самодостаточный: тесты соседних файлов в этой волне не редактируются)
# --------------------------------------------------------------------------- #

def _plan() -> RunPlan:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=[CatalogRow.model_validate(row) for row in raw["catalog"]],
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


class _Crawler:
    version = "crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 — сигнатура порта
        self.calls.append((url, plan))
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
        )


class _Extractor:
    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        text = page.content.decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(page.content),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="h1",
                context_prefix="ctx",
                content_hash=doc.content_hash,
            )
        ]


def _run(
    tmp_path: Path,
    *,
    store: SqliteStore,
    state: SqliteStateStore,
    crawler: _Crawler,
    repairs: dict[str, list[str]] | None,
    mode: str = "new",
) -> Any:
    return build_corpus(
        _plan(),
        crawler,
        _Extractor(),
        _Chunker(),
        store,
        urls={"p1": [ROOT]},
        raw_root=tmp_path,
        run_id=RUN_ID,
        state=state,
        product_modes={"p1": mode},
        only_failed_repairs=repairs,
    )


def _make_run(store: SqliteStore) -> None:
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


def _build_of(result: Any) -> IndexBuild:
    builds = [entry for entry in result.log_entries if isinstance(entry, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _events(result: Any) -> list[dict[str, Any]]:
    return [
        entry
        for entry in result.log_entries
        if isinstance(entry, dict) and "event" in entry
    ]


def _product_end(result: Any) -> dict[str, Any]:
    return next(e for e in _events(result) if e["event"] == "product_end")


def _notes(result: Any) -> str:
    return "\n".join(
        entry for entry in result.log_entries if isinstance(entry, str)
    )


def _state_failure(
    state: SqliteStateStore,
    url: str,
    *,
    failures: int = 1,
    error_kind: str = "source_unavailable",
    etag: str | None = None,
) -> None:
    state.upsert_url_state(
        UrlStateRow(
            product_id="p1",
            url=url,
            normalized_url=url,
            consecutive_failures=failures,
            last_error_kind=error_kind,
            etag=etag,
        )
    )


# --------------------------------------------------------------------------- #
# Тесты
# --------------------------------------------------------------------------- #

def test_repair_of_new_product_does_not_crawl_authored_root(tmp_path: Path) -> None:
    """``new``-продукт: ремонт — depth-0 лист, авторский корень не обходится.

    Режим продукта не гейт: набор берётся из ``url_state`` (здесь — из переданного
    множества), а не из ``plan_incremental``, который для ``new`` вернул бы корни.
    """
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        _state_failure(state, DEEP, failures=2, etag='W/"v1"')
        crawler = _Crawler(PAGES)
        result = _run(
            tmp_path, store=store, state=state, crawler=crawler, repairs={"p1": [DEEP]}
        )

        assert result.errors == []
        # Ремонт — единственный фетч: корень не дренится вовсе.
        assert [url for url, _plan in crawler.calls] == [DEEP]
        end = _product_end(result)
        assert end["seeds_enqueued"] == 0  # ни одного seed-корня в очередь
        # Валидатор условного GET перенесён из ``url_state``.
        assert crawler.calls[0][1].if_none_match == 'W/"v1"'
        docs = [doc.url for doc in store.iter_corpus_docs(RUN_ID)]
        assert docs == [DEEP]

    note = _notes(result)
    assert "ремонт --only-failed" in note
    assert "в work-set 1 из 1 URL" in note


def test_without_repair_set_same_product_crawls_the_root(tmp_path: Path) -> None:
    """Контроль: без ``only_failed_repairs`` тот же продукт обходит авторский корень.

    Это поведение до правки; тест фиксирует, что различие даёт именно новый вход, а не
    режим продукта/состав ``urls``.
    """
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        crawler = _Crawler(PAGES)
        result = _run(
            tmp_path, store=store, state=state, crawler=crawler, repairs=None
        )

        assert [url for url, _plan in crawler.calls] == [ROOT]
        assert _product_end(result)["seeds_enqueued"] == 1
        assert "ремонт --only-failed" not in _notes(result)


def test_not_found_is_not_repaired_and_drop_is_visible(tmp_path: Path) -> None:
    """404 — терминальное наблюдение, а не транзиентный сбой: в ремонт не идёт."""
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        # Счётчик остался от прежних транзиентных сбоев, но последний исход — ``not_found``.
        _state_failure(
            state, DEEP, failures=1, error_kind="not_found"
        )
        crawler = _Crawler(PAGES)
        result = _run(
            tmp_path, store=store, state=state, crawler=crawler, repairs={"p1": [DEEP]}
        )

        assert crawler.calls == []
        assert _product_end(result)["session_items"] == 0

    note = _notes(result)
    assert "в work-set 0 из 1 URL" in note
    assert "not_found_terminal=1" in note


def test_failure_outside_declared_hosts_is_dropped_visibly(tmp_path: Path) -> None:
    """Провал чужого хоста не тратит бюджет продукта — и это названо числом."""
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        _state_failure(state, OTHER_HOST)
        crawler = _Crawler(PAGES)
        result = _run(
            tmp_path,
            store=store,
            state=state,
            crawler=crawler,
            repairs={"p1": [OTHER_HOST]},
        )

        assert crawler.calls == []

    note = _notes(result)
    assert "out_of_declared_hosts=1" in note


def test_repair_works_for_full_mode_too(tmp_path: Path) -> None:
    """``full`` — тот же ремонт: гейт режима снят только для ремонта, не для плана."""
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        _state_failure(state, DEEP)
        crawler = _Crawler(PAGES)
        result = _run(
            tmp_path,
            store=store,
            state=state,
            crawler=crawler,
            repairs={"p1": [DEEP]},
            mode="full",
        )

        assert [url for url, _plan in crawler.calls] == [DEEP]
        assert result.errors == []


def test_incremental_plan_is_unchanged_without_repairs(tmp_path: Path) -> None:
    """Без ремонта инкрементальный продукт по-прежнему ревалидирует ``url_state``."""
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        _state_failure(state, DEEP, failures=0, error_kind="source_unavailable")
        crawler = _Crawler(PAGES)
        result = _run(
            tmp_path,
            store=store,
            state=state,
            crawler=crawler,
            repairs=None,
            mode="incremental",
        )

        fetched = [url for url, _plan in crawler.calls]
        assert ROOT in fetched
        assert DEEP in fetched
        assert "ремонт --only-failed" not in _notes(result)


def test_repair_outside_path_prefix_is_orphan_not_silent_drop(tmp_path: Path) -> None:
    """Провал вне path-prefix корня — orphan-ревалидация, а не тихий дроп.

    Известный URL не продолжает навигацию, поэтому он не становится ни seed-корнем, ни
    «вне scope»: он ревалидируется synthetic-бакетом своего origin, и это видно в
    ``revalidation_orphans``/заметке фазы (авторский scope разошёлся с ``url_state``).
    """
    deep_root = "https://docs.test/p1/docs/"
    outside = "https://docs.test/p1/other/page"
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        _state_failure(state, outside)
        crawler = _Crawler({deep_root: b"root", outside: b"outside page"})
        result = build_corpus(
            _plan(),
            crawler,
            _Extractor(),
            _Chunker(),
            store,
            urls={"p1": [deep_root]},
            raw_root=tmp_path,
            run_id=RUN_ID,
            state=state,
            product_modes={"p1": "new"},
            only_failed_repairs={"p1": [outside]},
        )

        assert [url for url, _plan in crawler.calls] == [outside]
        assert _product_end(result)["seeds_enqueued"] == 0

    build = _build_of(result)
    assert build.revalidation_orphans
    assert "outside every authored root" in _notes(result)


def test_repair_run_never_claims_complete_coverage(tmp_path: Path) -> None:
    """Ремонт не даёт ``complete``-покрытия: наблюдался только проваленный URL.

    Иначе «все элементы ремонта дошли до ``FETCHED``» читалось бы как «сайт прочитан», и
    ``❌`` (I2/I2c) публиковался бы на корпусе, который в этом прогоне не наблюдался.
    Причина названа машинным токеном, а не выведена из счётчиков.
    """
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        _state_failure(state, DEEP)
        crawler = _Crawler(PAGES)
        _run(tmp_path, store=store, state=state, crawler=crawler, repairs={"p1": [DEEP]})

        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.observed_sources == 1
        assert coverage.complete is False
        assert coverage.reason == "only_failed_repair"


def test_ordinary_run_still_reports_complete_coverage(tmp_path: Path) -> None:
    """Контроль: обычный прогон того же продукта покрытие не теряет (правка узкая)."""
    with SqliteStore(":memory:") as store, SqliteStateStore(":memory:") as state:
        _make_run(store)
        crawler = _Crawler(PAGES)
        _run(tmp_path, store=store, state=state, crawler=crawler, repairs=None)

        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is True
        assert coverage.reason is None
