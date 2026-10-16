"""Тесты BFS-frontier корпуса (D1/D2): глубина, scope, блэклисты, лимит документов, усечение.

Порты подменяются детерминированными in-memory заглушками (без сети/I/O, кроме ``tmp_path``
для retention). Нейтральные данные (§1.3). Проверяются замороженные решения D1/D2.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    CrawlSessionStatus,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id
from core.pipeline.report import collect_crawl_truncation_anomalies
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-frontier"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"

ROOT = "https://docs.test/"


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=_catalog(),
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


class _Crawler:
    """Краулер ``url -> bytes`` с записью вызовов и опциональным подлогом финального URL."""

    version = "crawler-1"
    engine = "http"

    def __init__(
        self,
        pages: dict[str, bytes],
        *,
        final_url: dict[str, str] | None = None,
    ) -> None:
        self.pages = dict(pages)
        self.final_url = dict(final_url or {})
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        return RawPage(
            url=self.final_url.get(url, url),
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
        return ExtractedDoc(
            text=page.content.decode("utf-8", errors="replace"),
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


def _build(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    crawler: _Crawler | None = None,
    **kwargs: Any,
):
    return build_corpus(
        _plan(),
        crawler or _Crawler(pages),
        _Extractor(),
        _Chunker(),
        kwargs.pop("store", None),
        urls=kwargs.pop("urls", {"p1": [ROOT]}),
        raw_root=tmp_path,
        **kwargs,
    )


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


# --------------------------------------------------------------------------- #
# (a) max_depth=0 — только seeds (байт-в-байт прежнее поведение)
# --------------------------------------------------------------------------- #


def test_max_depth_zero_fetches_seeds_only(tmp_path: Path) -> None:
    pages = {
        ROOT: b'<html><a href="/a">a</a><a href="/b">b</a></html>',
        "https://docs.test/a": b"<html>a</html>",
    }
    crawler = _Crawler(pages)
    result = _build(tmp_path, pages=pages, crawler=crawler)
    assert result.status == "ok"
    assert crawler.calls == [ROOT]  # ссылки не обходятся
    assert _build_of(result).truncated is False


# --------------------------------------------------------------------------- #
# (b) BFS до глубины N и остановка
# --------------------------------------------------------------------------- #


def test_bfs_follows_in_site_links_to_depth_n(tmp_path: Path) -> None:
    pages = {
        ROOT: b'<html><a href="/a">a</a><a href="/b">b</a></html>',
        "https://docs.test/a": b"<html>a</html>",
        "https://docs.test/b": b'<html><a href="/c">c</a></html>',
        "https://docs.test/c": b"<html>c</html>",
    }
    depth1 = _Crawler(pages)
    _build(
        tmp_path / "d1",
        pages=pages,
        crawler=depth1,
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    assert set(depth1.calls) == {ROOT, "https://docs.test/a", "https://docs.test/b"}

    depth2 = _Crawler(pages)
    _build(
        tmp_path / "d2",
        pages=pages,
        crawler=depth2,
        crawl_defaults=CrawlRunConfig(max_depth=2),
    )
    assert set(depth2.calls) == {
        ROOT,
        "https://docs.test/a",
        "https://docs.test/b",
        "https://docs.test/c",
    }


# --------------------------------------------------------------------------- #
# (c) вне-scope ссылки не обходятся
# --------------------------------------------------------------------------- #


def test_out_of_scope_links_not_followed(tmp_path: Path) -> None:
    seed = "https://docs.test/documents"
    pages = {
        seed: (
            b'<html>'
            b'<a href="/documents/x">in</a>'
            b'<a href="/documents-archive">boundary</a>'
            b'<a href="https://other.test/y">ext</a>'
            b'<a href="mailto:z@y">mail</a>'
            b'<a href="javascript:alert(1)">js</a>'
            b'<a href="#frag">frag</a>'
            b"</html>"
        ),
        "https://docs.test/documents/x": b"<html>in</html>",
        "https://docs.test/documents-archive": b"<html>arch</html>",
        "https://other.test/y": b"<html>ext</html>",
    }
    crawler = _Crawler(pages)
    _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        urls={"p1": [seed]},
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    assert set(crawler.calls) == {seed, "https://docs.test/documents/x"}
    # Граница сегмента: /documents-archive НЕ матчит префикс /documents.
    assert "https://docs.test/documents-archive" not in crawler.calls
    assert "https://other.test/y" not in crawler.calls


# --------------------------------------------------------------------------- #
# (e) блэклисты (путь + basename) применяются и к обнаруженным URL
# --------------------------------------------------------------------------- #


def test_blacklist_filters_discovered_urls(tmp_path: Path) -> None:
    pages = {
        ROOT: (
            b'<html>'
            b'<a href="/keep">k</a>'
            b'<a href="/skip/a">s</a>'
            b'<a href="/docs/file.pdf">p</a>'
            b"</html>"
        ),
        "https://docs.test/keep": b"<html>k</html>",
        "https://docs.test/skip/a": b"<html>s</html>",
        "https://docs.test/docs/file.pdf": b"%PDF-1.4",
    }
    crawler = _Crawler(pages)
    _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        crawl_defaults=CrawlRunConfig(
            max_depth=1, exclude_paths=["/skip/*"], exclude_documents=["*.pdf"]
        ),
    )
    assert crawler.calls == [ROOT, "https://docs.test/keep"]


def test_blacklisted_seed_surfaced_not_dropped(tmp_path: Path) -> None:
    pages = {ROOT: b"<html>x</html>"}
    crawler = _Crawler(pages)
    result = _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        crawl_defaults=CrawlRunConfig(exclude_paths=["/"]),
    )
    assert crawler.calls == []
    assert any("зачёркнут блэклистом" in e for e in result.errors)


# --------------------------------------------------------------------------- #
# (f) max_documents — жёсткий стоп + честность усечения
# --------------------------------------------------------------------------- #


def test_max_documents_hard_stop_truncation_and_partial_session(tmp_path: Path) -> None:
    pages = {
        ROOT: b'<html><a href="/a">a</a><a href="/b">b</a></html>',
        "https://docs.test/a": b"<html>a</html>",
        "https://docs.test/b": b"<html>b</html>",
    }
    crawler = _Crawler(pages)
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            pages=pages,
            crawler=crawler,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1, max_documents=1),
        )
    finally:
        store.close()

    # Жёсткий стоп: добыт только seed, без исключения.
    assert crawler.calls == [ROOT]
    build = _build_of(result)
    assert build.truncated is True
    assert build.truncation[0]["truncated_reason"] == "max_documents"

    # Сеанс — PARTIAL (не COMPLETED), провенанс несёт усечение.
    store2 = SqliteStore(":memory:")
    try:
        _make_run(store2)
        _build(
            tmp_path / "again",
            pages=pages,
            crawler=_Crawler(pages),
            store=store2,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1, max_documents=1),
        )
        session = store2.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
        assert session.provenance_json is not None
        assert '"truncated": true' in session.provenance_json
        assert '"truncated_products": ["p1"]' in session.provenance_json
    finally:
        store2.close()


def test_exhaustive_absence_anomaly_fires_on_truncated_product() -> None:
    class _Cell:
        product_id = "p1"
        param_id = "param_a"
        absence_evidence = [{"source_scope": "exhaustive"}]

    anomalies = collect_crawl_truncation_anomalies([_Cell()], ["p1"])
    assert len(anomalies) == 1
    assert anomalies[0]["product_id"] == "p1"
    assert "I2b/G" in anomalies[0]["reason"]
    # Продукт без усечения — аномалии нет.
    assert collect_crawl_truncation_anomalies([_Cell()], ["p2"]) == []


# --------------------------------------------------------------------------- #
# (h) HTTP redirect-escape: off-host финальный URL не ретейнится
# --------------------------------------------------------------------------- #


def test_redirect_escape_off_host_dropped(tmp_path: Path) -> None:
    pages = {ROOT: b"<html>landing</html>"}
    crawler = _Crawler(pages, final_url={ROOT: "https://other.test/landing"})
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            pages=pages,
            crawler=crawler,
            store=store,
            run_id=RUN_ID,
            # B4: жёсткий redirect-scope включён только при discovery (max_depth > 0).
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )
        items = {
            (i.product_id, i.url): i
            for i in store.iter_crawl_session_items(SESSION_ID)
        }
        item = items.get(("p1", ROOT))
    finally:
        store.close()

    build = _build_of(result)
    assert build.docs == []  # не ingested
    assert not (tmp_path / "p1").exists() or not any(
        (tmp_path / "p1").iterdir()
    )  # raw не ретейнится
    assert item is not None and item.error_kind == "redirect_out_of_scope"


def test_redirect_escape_off_host_retained_at_depth_zero(tmp_path: Path) -> None:
    """B4: на глубине 0 (discovery выключен) оператор явно запросил URL — финальная
    cross-host страница ретейнится как прежде (без silent corpus loss)."""
    pages = {ROOT: b"<html>landing</html>"}
    crawler = _Crawler(pages, final_url={ROOT: "https://other.test/landing"})
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path, pages=pages, crawler=crawler, store=store, run_id=RUN_ID
        )
        items = {
            (i.product_id, i.url): i
            for i in store.iter_crawl_session_items(SESSION_ID)
        }
    finally:
        store.close()
    build = _build_of(result)
    assert build.crawl_fetched == 1
    assert build.docs  # страница ingested
    assert items[("p1", ROOT)].error_kind is None


def test_www_to_apex_seed_retained_at_depth_zero(tmp_path: Path) -> None:
    """B4-регрессия: www→apex редирект seed'а на глубине 0 НЕ теряет страницу
    (асимметрия ``_host_matches`` больше не роняет корпус: ``crawl_fetched >= 1``)."""
    www = "https://www.docs.test/"
    apex = "https://docs.test/"
    pages = {www: b"<html>apex landing</html>"}
    crawler = _Crawler(pages, final_url={www: apex})
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            pages=pages,
            crawler=crawler,
            urls={"p1": [www]},
            store=store,
            run_id=RUN_ID,
        )
        item = {
            (i.product_id, i.url): i
            for i in store.iter_crawl_session_items(SESSION_ID)
        }[("p1", www)]
    finally:
        store.close()
    build = _build_of(result)
    assert build.crawl_fetched >= 1
    assert build.docs  # build не пуст
    assert crawler.calls == [www]
    # B4: глубина персистится как 0 (не NULL) — seed-root это глубина 0.
    assert item.depth == 0


def test_www_to_apex_retained_under_discovery(tmp_path: Path) -> None:
    """B4: при discovery (``max_depth>0``) проверка симметрична — www→apex не теряется."""
    www = "https://www.docs.test/"
    apex = "https://docs.test/"
    pages = {www: b"<html>apex landing</html>"}
    crawler = _Crawler(pages, final_url={www: apex})
    result = _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        urls={"p1": [www]},
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    assert _build_of(result).docs
    assert crawler.calls == [www]


def test_unrelated_redirect_dropped_with_reason_under_discovery(tmp_path: Path) -> None:
    """B4: при discovery настоящий уход из семейства → явный ``redirect_out_of_scope``."""
    pages = {ROOT: b"<html>landing</html>"}
    crawler = _Crawler(pages, final_url={ROOT: "https://unrelated.test/x"})
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            pages=pages,
            crawler=crawler,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )
        item = {
            (i.product_id, i.url): i
            for i in store.iter_crawl_session_items(SESSION_ID)
        }[("p1", ROOT)]
    finally:
        store.close()
    assert _build_of(result).docs == []
    assert item.error_kind == "redirect_out_of_scope"
    assert any("redirect" in e for e in result.errors)


# --------------------------------------------------------------------------- #
# (i) max_depth > 0 под executor/split — конфиг-ошибка
# --------------------------------------------------------------------------- #


def test_max_depth_under_executor_is_config_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="max_depth"):
        _build(
            tmp_path,
            pages={ROOT: b"<html>x</html>"},
            executor=object(),
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )


# --------------------------------------------------------------------------- #
# (B1) max_documents как жёсткий потолок: batched seeds, cap >= 2
# --------------------------------------------------------------------------- #


def test_max_documents_cap_two_batched_seed_fetches_exactly_two(
    tmp_path: Path,
) -> None:
    """B1-регрессия: seed с 3 ссылками, ``max_documents=2`` → ровно 2 фетча (потолок)."""
    pages = {
        ROOT: (
            b'<html>'
            b'<a href="/a">a</a><a href="/b">b</a><a href="/c">c</a>'
            b"</html>"
        ),
        "https://docs.test/a": b"<html>a</html>",
        "https://docs.test/b": b"<html>b</html>",
        "https://docs.test/c": b"<html>c</html>",
    }
    crawler = _Crawler(pages)
    result = _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        crawl_defaults=CrawlRunConfig(max_depth=1, max_documents=2),
    )
    assert len(crawler.calls) == 2
    assert crawler.calls[0] == ROOT
    build = _build_of(result)
    assert build.truncated is True
    assert build.truncation[0]["truncated_reason"] == "max_documents"


def test_two_roots_each_get_own_budget(tmp_path: Path) -> None:
    """B1: бюджет ``max_documents`` — per (product, seed-root), а не общий на продукт."""
    root_a = "https://docs.test/a"
    root_b = "https://docs.test/b"
    pages = {
        root_a: b'<html><a href="/a/x">x</a></html>',
        root_b: b'<html><a href="/b/y">y</a></html>',
        "https://docs.test/a/x": b"<html>x</html>",
        "https://docs.test/b/y": b"<html>y</html>",
    }
    crawler = _Crawler(pages)
    result = _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        urls={"p1": [root_a, root_b]},
        crawl_defaults=CrawlRunConfig(max_depth=1, max_documents=1),
    )
    # Каждый корень израсходовал собственный бюджет=1 (свой seed) — всего два фетча.
    assert set(crawler.calls) == {root_a, root_b}
    build = _build_of(result)
    records = {row["root"]: row for row in build.truncation}
    assert records[root_a]["pages_fetched"] == 1
    assert records[root_b]["pages_fetched"] == 1
    assert records[root_a]["truncated"] is True


# --------------------------------------------------------------------------- #
# (B2) resume не превышает бюджет и остаётся PARTIAL
# --------------------------------------------------------------------------- #


def test_resume_respects_remaining_budget_and_stays_partial(tmp_path: Path) -> None:
    """B2-регрессия: повторный прогон того же session_id не превышает ``max_documents``,
    не пере-фетчит и остаётся ``PARTIAL`` с ``truncated=True`` в провенансе."""
    pages = {
        ROOT: b'<html><a href="/a">a</a><a href="/b">b</a></html>',
        "https://docs.test/a": b"<html>a</html>",
        "https://docs.test/b": b"<html>b</html>",
    }
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        first_crawler = _Crawler(pages)
        _build(
            tmp_path,
            pages=pages,
            crawler=first_crawler,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1, max_documents=2),
        )
        assert len(first_crawler.calls) == 2  # seed + 1 ссылка

        second_crawler = _Crawler(pages)
        _build(
            tmp_path,
            pages=pages,
            crawler=second_crawler,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1, max_documents=2),
        )
        # Остаток бюджета = 0 → ни одного нового фетча (бюджет пережил новый прогон).
        assert second_crawler.calls == []

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.PARTIAL
        assert session.provenance_json is not None
        assert '"truncated": true' in session.provenance_json
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (N8) guard executor: max_depth учитывается только для краулимых host'ов
# --------------------------------------------------------------------------- #


def test_max_depth_requested_narrowed_to_crawled_hosts() -> None:
    """N8: профиль с ``max_depth>0`` для host'а вне охвата executor не блокирует прогон."""
    from core.config.deploy import CrawlConfig
    from core.config.domain_profiles import DomainProfile
    from core.pipeline.corpus import _max_depth_requested

    app = CrawlConfig()
    profiles = {
        "uncrawled.test": DomainProfile(max_depth=3),
        "crawled.test": DomainProfile(),
    }
    # Краулим host без max_depth → не ошибка (профиль uncrawled.test не в охвате).
    assert (
        _max_depth_requested(app, None, profiles, ["https://crawled.test/x"]) == 0
    )
    # Краулим host с max_depth>0 → глубина запрошена (guard сработает).
    assert (
        _max_depth_requested(app, None, profiles, ["https://uncrawled.test/x"]) == 3
    )
    # App/run-уровни учитываются безусловно.
    assert _max_depth_requested(app, CrawlRunConfig(max_depth=4), profiles, []) == 4
