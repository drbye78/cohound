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
