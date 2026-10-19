"""P3-A2, §7 — критерий сравнимости (crux): паритет ``local`` ‖ ``split``.

Гоняем ``build_corpus`` **дважды** на одном логическом сайте: (1) локальным
``_LocalCrawler``-харнессом; (2) фейковым ``CrawlExecutorPort``, возвращающим
**байт-идентичный** результат (включая ``final_url``). Затем утверждаем равенство
наборов ``CorpusDoc.content_hash``/URL по продукту, ``IndexBuild.chunk_count``,
per-product/per-chunk content-токенов, ``index_build_id``, per-root ``truncated``/
``truncated_reason``, ``build.truncated``, статуса сеанса и ``pages_fetched``.

Сайт обязан включать многоуровневый BFS (``max_depth=2``), реально срабатывающий кэп
``max_documents``, блэклист, **same-host редирект с trailing slash** (единственный кейс,
делающий перенос ``final_url`` неоспоримым) и off-host редирект
(``redirect_out_of_scope``). Второй вариант — feed-каталог + sitemap (решение §5).
Третий кейс — непустой ``rate_limit`` с no-sleep лимитером (§11.6): контент-независимость
тайминга.

Харнесс — самодостаточный (в ``tests/pipeline`` нет ``__init__.py``): детерминированный,
без сети/реальных снов, только ``tmp_path`` для retention. Нейтральные данные (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import random
from pathlib import Path
from typing import Any

from core.config.crawl_sources import CrawlSource, CrawlSourcesConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    RateLimitSpec,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id, read_provenance
from core.ports.crawl_executor import (
    CrawlBatch,
    CrawlProgress,
    CrawlResultItem,
    CrawlSubmitResult,
    CrawlWorkItem,
)
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.feed import FeedEntry
from core.util.hashing import content_hash
from core.util.rate_limit import RateLimiter

RUN_ID = "run-parity"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"


# --------------------------------------------------------------------------- #
# Общий харнесс (план, прогон, порты-заглушки)
# --------------------------------------------------------------------------- #


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


class _LocalCrawler:
    """Локальный ``CrawlerPort``: ``url -> bytes`` + подлог финального URL (редиректы)."""

    version = "crawler-1"
    engine = "http"

    def __init__(
        self, pages: dict[str, bytes], *, final_url: dict[str, str] | None = None
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


class _BombCrawler:
    """Локальный краулер, обязанный НЕ вызываться в split-режиме."""

    version = "crawler-1"
    engine = "http"

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        raise AssertionError("локальный краулер вызван в split-режиме")


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


class _FakePoller:
    """Детерминированный ``PollPort``-двойник (feed/sitemap)."""

    version = "poller-1"

    def __init__(self, entries: list[FeedEntry] | None = None) -> None:
        self.entries = list(entries or ())
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def validate_params(self, kind: str, params: dict[str, Any]) -> list[str]:
        return []

    def poll(
        self, url: str, *, kind: str, params: dict[str, Any], plan: FetchPlan
    ) -> list[FeedEntry]:
        self.calls.append((url, kind, dict(params)))
        return list(self.entries)


class _FakeExecutor:
    """``CrawlExecutorPort``-двойник: отдаёт **байт-идентичный** результат, включая
    ``final_url`` (иначе same-host редирект с trailing slash разошёлся бы: база ссылок и
    ``CorpusDoc.url`` остались бы запрошенными). Журналирует submit/status/collect.
    """

    version = "fake-executor-1"
    profile = "split"

    def __init__(
        self, pages: dict[str, bytes], *, final_url: dict[str, str] | None = None
    ) -> None:
        self.pages = dict(pages)
        self.final_url = dict(final_url or {})
        self.submits: list[list[CrawlWorkItem]] = []
        self.status_calls: list[str] = []

    def submit(
        self,
        items: list[CrawlWorkItem],
        *,
        session_id: str,
        run_id: str,
        profile: str,
        idempotency_key: str,
    ) -> CrawlSubmitResult:
        self.submits.append(list(items))
        return CrawlSubmitResult(job_id="job-1", accepted=True)

    def status(self, handle: str) -> CrawlProgress:
        self.status_calls.append("completed")
        return CrawlProgress(job_id=handle, status="completed")

    def cancel(self, handle: str) -> CrawlProgress:
        return CrawlProgress(job_id=handle, status="cancelled")

    def collect(
        self, handle: str, *, cursor: str | None = None, limit: int | None = None
    ) -> CrawlBatch:
        items = [self._result(work) for work in self.submits[-1]]
        return CrawlBatch(items=items, next_cursor=None, done=True)

    def _result(self, work: CrawlWorkItem) -> CrawlResultItem:
        # ``self.pages[work.url]`` (а не ``.get``) — симметрично локальному ``_Crawler``:
        # отсутствующая страница обязана «взорваться» одинаково, а не дать пустой чанк.
        content = self.pages[work.url]
        return CrawlResultItem(
            product_id=work.product_id,
            url=work.url,
            status="fetched",
            engine=work.engine or "http",
            kind=work.kind,
            fetched_at=TS,
            content_hash=content_hash(content),
            raw_bytes=content,
            final_url=self.final_url.get(work.url),
        )


def _build(
    tmp_path: Path,
    *,
    store: SqliteStore,
    crawler: Any,
    executor: Any,
    urls: dict[str, list[str]],
    crawl_defaults: CrawlRunConfig | None = None,
    domain_profiles: dict[str, DomainProfile] | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    pollers: dict[str, Any] | None = None,
    rate_limiter: RateLimiter | None = None,
):
    return build_corpus(
        _plan(),
        crawler,
        _Extractor(),
        _Chunker(),
        store,
        urls=urls,
        raw_root=tmp_path,
        run_id=RUN_ID,
        crawl_defaults=crawl_defaults,
        domain_profiles=domain_profiles,
        crawl_sources=crawl_sources,
        pollers=pollers,
        rate_limiter=rate_limiter,
        executor=executor,
        executor_poll_interval=0.0,
        executor_sleep=lambda _s: None,
    )


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _snapshot(result: Any, store: SqliteStore) -> dict[str, Any]:
    """Канонический снимок паритет-значимых артефактов прогона (§7)."""
    build = _build_of(result)
    session = store.get_crawl_session(SESSION_ID)
    prov = read_provenance(session)

    docs_by_product: dict[str, list[str]] = {}
    urls_by_product: dict[str, list[str]] = {}
    for doc in build.docs:
        docs_by_product.setdefault(doc.product_id, []).append(doc.content_hash or "")
        urls_by_product.setdefault(doc.product_id, []).append(doc.url)
    for bucket in docs_by_product.values():
        bucket.sort()
    for bucket in urls_by_product.values():
        bucket.sort()

    truncation = sorted(
        (
            {
                "product_id": rec["product_id"],
                "root": rec["root"],
                "pages_fetched": rec["pages_fetched"],
                "truncated": rec["truncated"],
                "truncated_reason": rec["truncated_reason"],
                "limits": rec["limits"],
                "skipped_blacklisted": sorted(rec["skipped_blacklisted"]),
            }
            for rec in build.truncation
        ),
        key=lambda r: (r["product_id"], r["root"]),
    )

    return {
        "index_build_id": build.index_build_id,
        "chunk_count": build.chunk_count,
        "truncated": build.truncated,
        "truncation": truncation,
        "docs_by_product": docs_by_product,
        "urls_by_product": urls_by_product,
        "chunk_tokens_by_product": {
            e.product_id: e.content_digest for e in build.entries
        },
        "stats_by_product": {e.product_id: e.stats_json for e in build.entries},
        "crawl_fetched": build.crawl_fetched,
        "crawl_failed": build.crawl_failed,
        "session_status": session.status.value if session is not None else None,
        "session_truncated": prov.get("truncated"),
        "session_pages_fetched": prov.get("pages_fetched"),
        "session_truncated_products": prov.get("truncated_products"),
        "session_effective_limits": prov.get("effective_limits"),
        "errors": sorted(result.errors),
    }


def _assert_parity(local: dict[str, Any], split: dict[str, Any]) -> None:
    for key in local:
        assert local[key] == split[key], (
            f"паритет нарушен в {key!r}:\n  local={local[key]!r}\n  split={split[key]!r}"
        )


def _run_local(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    final_url: dict[str, str] | None = None,
    crawl_defaults: CrawlRunConfig | None = None,
    urls: dict[str, list[str]],
    domain_profiles: dict[str, DomainProfile] | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    pollers: dict[str, Any] | None = None,
    rate_limiter: RateLimiter | None = None,
):
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            store=store,
            crawler=_LocalCrawler(pages, final_url=final_url),
            executor=None,
            urls=urls,
            crawl_defaults=crawl_defaults,
            domain_profiles=domain_profiles,
            crawl_sources=crawl_sources,
            pollers=pollers,
            rate_limiter=rate_limiter,
        )
        snapshot = _snapshot(result, store)
        items = {
            (i.product_id, i.url): i for i in store.iter_crawl_session_items(SESSION_ID)
        }
        return result, snapshot, items
    finally:
        store.close()


def _run_split(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    final_url: dict[str, str] | None = None,
    crawl_defaults: CrawlRunConfig | None = None,
    urls: dict[str, list[str]],
    domain_profiles: dict[str, DomainProfile] | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    pollers: dict[str, Any] | None = None,
):
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        executor = _FakeExecutor(pages, final_url=final_url)
        result = _build(
            tmp_path,
            store=store,
            crawler=_BombCrawler(),
            executor=executor,
            urls=urls,
            crawl_defaults=crawl_defaults,
            domain_profiles=domain_profiles,
            crawl_sources=crawl_sources,
            pollers=pollers,
        )
        snapshot = _snapshot(result, store)
        items = {
            (i.product_id, i.url): i for i in store.iter_crawl_session_items(SESSION_ID)
        }
        return result, snapshot, items, executor
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Сайт №1: многоуровневый BFS + кэп + блэклист + same-host (trailing slash) и
# off-host редиректы. Явно покрывает все обязательные кейсы §7.
# --------------------------------------------------------------------------- #

P_ROOT = "https://parity.test/"
P_A = "https://parity.test/a"
P_B = "https://parity.test/b"
P_BLOCKED = "https://parity.test/blocked/x"
P_REDIR = "https://parity.test/redir"
P_REDIR_FINAL = "https://parity.test/redir/"  # same-host redirect + trailing slash
P_REDIR_CHILD = "https://parity.test/redir/child"
P_AWAY = "https://parity.test/away"
P_AWAY_FINAL = "https://other.test/x"  # off-host → redirect_out_of_scope
P_A_CHILD = "https://parity.test/a/child"
P_B_CHILD = "https://parity.test/b/child"


def _site_full_bfs() -> tuple[dict[str, bytes], dict[str, str]]:
    pages = {
        P_ROOT: (
            b'<html><a href="/a">a</a><a href="/b">b</a>'
            b'<a href="/blocked/x">bl</a><a href="/redir">r</a>'
            b'<a href="/away">w</a></html>'
        ),
        P_A: b'<html><a href="/a/child">c</a></html>',
        P_B: b'<html><a href="/b/child">c</a></html>',
        P_BLOCKED: b"<html>blocked</html>",
        # Относительная ссылка: при финальном ``/redir/`` резолвится в ``/redir/child``,
        # при запрошенном ``/redir`` — в ``/child``. Это и делает ``final_url`` обязательным.
        P_REDIR: b'<html><a href="child">c</a></html>',
        P_REDIR_CHILD: b"<html>redir child</html>",
        P_AWAY: b"<html>away</html>",
        P_A_CHILD: b"<html>a child</html>",
        P_B_CHILD: b"<html>b child</html>",
    }
    final_url = {P_REDIR: P_REDIR_FINAL, P_AWAY: P_AWAY_FINAL}
    return pages, final_url


FULL_BFS_DEFAULTS = CrawlRunConfig(
    max_depth=2, max_documents=5, exclude_paths=["/blocked/*"]
)


def test_parity_local_vs_split_full_bfs_site(tmp_path: Path) -> None:
    """§7: local и split дают идентичный корпус/сборку/усечение на одном сайте."""
    pages, final_url = _site_full_bfs()
    urls = {"p1": [P_ROOT]}

    local_result, local, local_items = _run_local(
        tmp_path / "local",
        pages=pages,
        final_url=final_url,
        crawl_defaults=FULL_BFS_DEFAULTS,
        urls=urls,
    )
    _split_result, split, split_items, executor = _run_split(
        tmp_path / "split",
        pages=pages,
        final_url=final_url,
        crawl_defaults=FULL_BFS_DEFAULTS,
        urls=urls,
    )

    # --- Санитария: сайт реально исполняет обязательные кейсы (не вакуумный паритет). ---
    build = _build_of(local_result)
    assert build.truncated is True
    assert build.truncation[0]["truncated_reason"] == "max_documents"
    assert build.truncation[0]["pages_fetched"] == 5  # кэп действительно сработал
    assert P_BLOCKED in build.truncation[0]["skipped_blacklisted"]
    assert P_A_CHILD in {d.url for d in build.docs}  # BFS до глубины 2
    assert P_REDIR_FINAL in {d.url for d in build.docs}  # финальный URL применён
    assert P_AWAY_FINAL not in {d.url for d in build.docs}
    away = local_items[("p1", P_AWAY)]
    assert away.error_kind == "redirect_out_of_scope"
    assert P_AWAY not in {d.url for d in build.docs}
    assert P_REDIR_CHILD not in {d.url for d in build.docs}  # глубина 3 недостижима
    assert len(executor.submits) >= 3  # минимум: seed / depth-1 / depth-2

    # --- Паритет. ---
    _assert_parity(local, split)
    assert local["index_build_id"] == split["index_build_id"]
    assert local["chunk_tokens_by_product"] == split["chunk_tokens_by_product"]
    assert split_items[("p1", P_AWAY)].error_kind == "redirect_out_of_scope"
    assert split_items[("p1", P_REDIR)].status is local_items[("p1", P_REDIR)].status


# --------------------------------------------------------------------------- #
# Сайт №2: feed-каталог + sitemap (решение §5): разворот каталога и sitemap-seeds
# делят один корневой фронтир; развёртка одинакова в обоих транспортах.
# --------------------------------------------------------------------------- #

V2_SEED = "https://parity.test/"
V2_A = "https://parity.test/a"
V2_LOC1 = "https://parity.test/loc1"
V2_LOC2 = "https://parity.test/loc2"
V2_LOC1_CHILD = "https://parity.test/loc1-child"
V2_SITEMAP = "https://parity.test/sitemap.xml"
V2_FEED = "https://feed.test/rss"
V2_E1 = "https://feed.test/e1"
V2_E2 = "https://feed.test/e2"
V2_E1_CHILD = "https://feed.test/e1-child"


def _site_directory_and_sitemap() -> dict[str, bytes]:
    return {
        V2_SEED: b'<html><a href="/a">a</a></html>',
        V2_A: b"<html>a</html>",
        V2_LOC1: b'<html><a href="/loc1-child">c</a></html>',
        V2_LOC2: b"<html>loc2</html>",
        V2_LOC1_CHILD: b"<html>loc1 child</html>",
        V2_E1: b'<html><a href="/e1-child">c</a></html>',
        V2_E2: b"<html>e2</html>",
        V2_E1_CHILD: b"<html>e1 child</html>",
    }


def _directory_sources() -> CrawlSourcesConfig:
    return CrawlSourcesConfig(
        products={"p1": CrawlSource(engine="http", kind="web")},
        sites={"p1": {"feed.test": CrawlSource(kind="feed", engine="http")}},
    )


def test_parity_local_vs_split_directory_and_sitemap(tmp_path: Path) -> None:
    """§5: feed-развёртка и sitemap-seeds одного корня — идентичны local ‖ split."""
    pages = _site_directory_and_sitemap()
    urls = {"p1": [V2_SEED, V2_FEED]}
    crawl_defaults = CrawlRunConfig(max_depth=1, max_documents=2)
    domain_profiles = {"parity.test": DomainProfile(sitemap=V2_SITEMAP)}

    def _pollers() -> dict[str, _FakePoller]:
        return {
            "feed": _FakePoller([FeedEntry(url=V2_E1), FeedEntry(url=V2_E2)]),
            "sitemap": _FakePoller([FeedEntry(url=V2_LOC1), FeedEntry(url=V2_LOC2)]),
        }

    local_result, local, local_items = _run_local(
        tmp_path / "local",
        pages=pages,
        crawl_defaults=crawl_defaults,
        urls=urls,
        domain_profiles=domain_profiles,
        crawl_sources=_directory_sources(),
        pollers=_pollers(),
    )
    _split_result, split, split_items, executor = _run_split(
        tmp_path / "split",
        pages=pages,
        crawl_defaults=crawl_defaults,
        urls=urls,
        domain_profiles=domain_profiles,
        crawl_sources=_directory_sources(),
        pollers=_pollers(),
    )

    # --- Санитария: каталог развёрнут (записи — свои корни), sitemap-seeds прикреплены. ---
    roots = {rec["root"] for rec in _build_of(local_result).truncation}
    assert V2_E1 in roots and V2_E2 in roots  # записи feed — отдельные seed-roots
    assert V2_SEED in roots  # корень seed оператора (sitemap-seeds делят его фронтир)
    assert V2_FEED not in roots
    assert V2_LOC1 not in roots  # sitemap-seed НЕ отдельный корень — делит фронтир V2_SEED
    assert V2_A not in roots  # обнаруженный URL не становится корнем
    assert V2_LOC1 in {d.url for d in _build_of(local_result).docs}
    assert V2_LOC2 not in {d.url for d in _build_of(local_result).docs}  # кэп=2 съеден
    assert local_items[("p1", V2_FEED)].raw_path is None  # каталог не контент (I19)
    assert local_items[("p1", V2_SITEMAP)].raw_path is None  # sitemap не контент (I19)
    # Каждая запись каталога дренится своим корнем/раундом (раскладка §5).
    assert [w.url for wave in executor.submits for w in wave].count(V2_E1) == 1
    assert [w.url for wave in executor.submits for w in wave].count(V2_E2) == 1
    assert V2_FEED not in {w.url for wave in executor.submits for w in wave}

    # --- Паритет. ---
    _assert_parity(local, split)
    assert split_items[("p1", V2_E1)].status is local_items[("p1", V2_E1)].status


# --------------------------------------------------------------------------- #
# Pacing (§11.6): непустой ``rate_limit`` + no-sleep лимитер — контент-нейтрально,
# отличается только wall-clock.
# --------------------------------------------------------------------------- #


def test_parity_pacing_content_neutral(tmp_path: Path) -> None:
    """§11.4/§11.6: ``rate_limit`` не меняет хэши/``index_build_id``/усечение."""
    pages, final_url = _site_full_bfs()
    urls = {"p1": [P_ROOT]}
    paced_defaults = CrawlRunConfig(
        max_depth=2,
        max_documents=5,
        exclude_paths=["/blocked/*"],
        rate_limit=RateLimitSpec(min_s=0.5, max_s=0.5),
    )

    sleeps: list[float] = []
    now = [0.0]

    def _fake_sleep(seconds: float) -> None:
        # Часы двигаются на проспанное время (как ``time.sleep``): последовательные
        # резервы не накапливаются, сон равен сэмплированной задержке.
        sleeps.append(seconds)
        now[0] += seconds

    limiter = RateLimiter(sleep=_fake_sleep, rng=random.Random(0), clock=lambda: now[0])

    plain_local_result, plain_local, _ = _run_local(
        tmp_path / "plain",
        pages=pages,
        final_url=final_url,
        crawl_defaults=FULL_BFS_DEFAULTS,
        urls=urls,
    )
    paced_local_result, paced_local, _ = _run_local(
        tmp_path / "paced-local",
        pages=pages,
        final_url=final_url,
        crawl_defaults=paced_defaults,
        urls=urls,
        rate_limiter=limiter,
    )
    _paced_split_result, paced_split, _, _ = _run_split(
        tmp_path / "paced-split",
        pages=pages,
        final_url=final_url,
        crawl_defaults=paced_defaults,
        urls=urls,
    )

    # Пейсинг реально исполнялся локально (по одному сну на живой фетч), но без задержек.
    assert sleeps and all(delay == 0.5 for delay in sleeps)

    # Контент-нейтральность: paced local ≡ plain local (только тайминг отличается).
    _assert_parity(paced_local, plain_local)
    assert _build_of(paced_local_result).index_build_id == _build_of(
        plain_local_result
    ).index_build_id

    # Паритет транспортов сохраняется и при непустом rate_limit.
    _assert_parity(paced_local, paced_split)
