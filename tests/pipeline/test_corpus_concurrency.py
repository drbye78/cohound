"""Тесты D4 ``max_workers``: ограниченный пул ТОЛЬКО на сетевой fetch локального пути.

Порты подменяются детерминированными стабами; пул инжектится через аддитивный kwarg
``build_corpus(pool_factory=...)``. Проверяются замороженные решения P1-gate (A1):

1. дефолт байт-идентичен (``max_workers`` не задан и ``=1`` ⇒ пул не создаётся);
2. граница конкурентности (записывающий пул, мульти-хостовая волна);
3. детерминизм FIFO-коммита (retained-set/``index_build_id``/победитель дедупа);
4. жёсткий стоп ``max_documents`` соблюдён при ``K>1``;
5. browser-элементы не пулятся (один in-flight);
6. resume-элементы минуют пул (живого фетча нет).

Нейтральные данные (§1.3); конкретные адаптеры не импортируются (только контракт портов).
"""

from __future__ import annotations

import datetime as _dt
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.ports.state import UrlStateRow
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash

RUN_ID = "run-concurrency"
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


def _make_run(store: SqliteStore, run_id: str = RUN_ID) -> None:
    store.create_run(
        run_id,
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
    """Краулер ``url -> bytes``; опциональный ``gate_to`` держит слоты для проверки K.

    ``gate_to`` (``K>1``) заставляет каждый ``fetch`` ждать, пока одновременно активных
    вызовов не станет ``K`` (или таймаута ``gate_timeout``) — так детерминированно
    наблюдается ``max_active == K`` для полной волны, без проверки *последовательностей*
    задержек (перемешиваемых под потоками).
    """

    version = "crawler-1"
    engine = "http"
    supports_browser = True

    def __init__(
        self,
        pages: dict[str, bytes],
        *,
        raise_urls: set[str] | None = None,
        gate_to: int | None = None,
        gate_timeout: float = 0.5,
    ) -> None:
        self.pages = dict(pages)
        self.raise_urls = set(raise_urls or ())
        self.calls: list[str] = []
        self._gate_to = gate_to
        self._gate_timeout = gate_timeout
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        with self._lock:
            self.calls.append(url)
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            if self._gate_to is not None and self._gate_to > 1:
                deadline = time.monotonic() + self._gate_timeout
                while time.monotonic() < deadline:
                    with self._lock:
                        if self._active >= self._gate_to:
                            break
                    time.sleep(0.001)
            if url in self.raise_urls:
                raise RuntimeError(f"crawl failed: {url}")
            return RawPage(
                url=url,
                content=self.pages[url],
                fetched_at=TS,
                fetch_mode=plan.fetch,
                lang="en",
            )
        finally:
            with self._lock:
                self._active -= 1


class _BombCrawler(_Crawler):
    """Краулер, обязанный НЕ вызываться: любой ``fetch`` — провал (resume)."""

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        raise AssertionError("краулер вызван при наличии retained-байтов (resume)")


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


class _RecordingPool(ThreadPoolExecutor):
    """``ThreadPoolExecutor``, считающий число задач «в полёте» (liveness/граница волны)."""

    def __init__(self, max_workers: int) -> None:
        super().__init__(max_workers=max_workers)
        self.max_workers_arg = max_workers
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0
        self.submitted = 0

    def submit(self, fn, *args, **kwargs):  # noqa: ANN001, ANN201 — как у ``Executor.submit``
        def _run():  # noqa: ANN202
            with self._lock:
                self._active += 1
                self.max_active = max(self.max_active, self._active)
            try:
                return fn(*args, **kwargs)
            finally:
                with self._lock:
                    self._active -= 1

        with self._lock:
            self.submitted += 1
        return super().submit(_run)


def _recording_factory(created: list[_RecordingPool]):
    """Фабрика, записывающая созданный пул (для ассертов K/конкурентности)."""

    def factory(n: int) -> _RecordingPool:
        pool = _RecordingPool(n)
        created.append(pool)
        return pool

    return factory


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


def _docs(build: IndexBuild) -> list[tuple[str, str]]:
    """Retained-set (``doc_id``, ``url``) в детерминированном порядке."""
    return sorted((d.doc_id, d.url) for d in build.docs)


def _multi_host_pages(n: int = 4, body: str = "page") -> dict[str, bytes]:
    links = "".join(
        f'<a href="https://h{i}.docs.test/{i}">{i}</a>' for i in range(n)
    )
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    for i in range(n):
        pages[f"https://h{i}.docs.test/{i}"] = f"<html>{body}-{i}</html>".encode()
    return pages


# --------------------------------------------------------------------------- #
# (1) Дефолт неизменен: None/1 ⇒ пул не создаётся, выводы идентичны
# --------------------------------------------------------------------------- #


def test_default_and_single_worker_are_identical_and_create_no_pool(
    tmp_path: Path,
) -> None:
    pages = _multi_host_pages()
    created: list[_RecordingPool] = []
    factory = _recording_factory(created)

    default = _build(tmp_path / "default", pages=pages, pool_factory=factory)
    one = _build(
        tmp_path / "one",
        pages=pages,
        crawl_defaults=CrawlRunConfig(max_workers=1),
        pool_factory=factory,
    )

    # Самый важный guardrail: при max_workers in (None, 1) пул НЕ создаётся.
    assert created == []
    b_default, b_one = _build_of(default), _build_of(one)
    assert b_default.index_build_id == b_one.index_build_id
    assert _docs(b_default) == _docs(b_one)
    assert default.errors == one.errors == []


# --------------------------------------------------------------------------- #
# (2) Граница конкурентности: max_inflight == max_workers и > 1, без превышения
# --------------------------------------------------------------------------- #


def test_bounded_concurrency_never_exceeds_max_workers(tmp_path: Path) -> None:
    pages = _multi_host_pages(4)
    created: list[_RecordingPool] = []
    crawler = _Crawler(pages, gate_to=3)

    _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        pool_factory=_recording_factory(created),
        crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=3),
    )

    assert len(created) == 1
    pool = created[0]
    assert pool.max_workers_arg == 3
    # Достигает K на полной волне и никогда не превышает её.
    assert pool.max_active == 3
    assert pool.max_active > 1
    assert pool.max_active <= 3
    # Волна 1 — seed (1), волна 2 — 3 ссылки, волна 3 — остаток (1): всего 5 отправок.
    assert pool.submitted == 5


# --------------------------------------------------------------------------- #
# (3) Детерминизм: K=4, мульти-хост ⇒ идентичные retained-set/id/победитель дедупа
# --------------------------------------------------------------------------- #


def test_two_k4_runs_are_deterministic_including_dedup_winner(tmp_path: Path) -> None:
    dup = b"<html>same</html>"
    links = "".join(
        f'<a href="https://h{i}.docs.test/{i}">{i}</a>' for i in range(4)
    )
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    pages["https://h0.docs.test/0"] = dup
    pages["https://h1.docs.test/1"] = dup  # дубликат h0 (I17: один документ на контент)
    pages["https://h2.docs.test/2"] = b"<html>two</html>"
    pages["https://h3.docs.test/3"] = b"<html>three</html>"

    def run(sub: str) -> Any:
        return _build(
            tmp_path / sub,
            pages=pages,
            crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=4),
        )

    a, b = _build_of(run("a")), _build_of(run("b"))
    assert a.index_build_id == b.index_build_id
    assert _docs(a) == _docs(b)

    # Победитель дедупа детерминирован FIFO-коммитом: первый в HTML-порядке (h0).
    dup_hash = content_hash(dup)
    winners = [d for d in a.docs if d.content_hash == dup_hash]
    assert len(winners) == 1
    assert winners[0].url == "https://h0.docs.test/0"


# --------------------------------------------------------------------------- #
# (4) Ёмкость: max_documents соблюдён при K>1, усечение честно
# --------------------------------------------------------------------------- #


def test_max_documents_respected_under_concurrency(tmp_path: Path) -> None:
    pages = _multi_host_pages(6)
    created: list[_RecordingPool] = []
    result = _build(
        tmp_path,
        pages=pages,
        pool_factory=_recording_factory(created),
        crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=3, max_documents=2),
    )
    build = _build_of(result)

    assert len(build.docs) == 2  # нет пере-удержания сверх ёмкости
    assert build.truncated is True
    assert build.truncation[0]["truncated_reason"] == "max_documents"
    # Остаток ёмкости ограничивает волну: после seed остаётся 1 слот.
    assert created[0].max_active == 1


# --------------------------------------------------------------------------- #
# (5) Browser-элементы не пулятся: один in-flight (inline на главном потоке)
# --------------------------------------------------------------------------- #


def test_browser_items_are_not_pooled(tmp_path: Path) -> None:
    pages = _multi_host_pages(3)
    explicit = {ROOT: FetchMode.BROWSER}
    for i in range(3):
        explicit[f"https://h{i}.docs.test/{i}"] = FetchMode.BROWSER
    created: list[_RecordingPool] = []
    crawler = _Crawler(pages)  # supports_browser=True ⇒ browser-фетч легитимен, но не пулится

    result = _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        explicit_fetch=explicit,
        pool_factory=_recording_factory(created),
        crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=3),
    )

    assert result.status == "ok"
    assert result.errors == []
    assert len(created) == 1
    assert created[0].submitted == 0  # ни один browser-элемент не ушёл в пул
    assert crawler.max_active == 1  # строго один in-flight (последовательный inline)


# --------------------------------------------------------------------------- #
# (6) Resume-элементы минуют пул: живого фетча нет
# --------------------------------------------------------------------------- #


def test_resume_items_bypass_pool(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    _make_run(store)
    url = "https://docs.test/seed"
    try:
        first = _build(
            tmp_path,
            pages={url: b"retained bytes"},
            urls={"p1": [url]},
            store=store,
            run_id=RUN_ID,
        )
        assert _build_of(first).chunk_count == 1

        created: list[_RecordingPool] = []
        second = _build(
            tmp_path,
            pages={},
            urls={"p1": [url]},
            crawler=_BombCrawler({}),
            store=store,
            run_id=RUN_ID,
            pool_factory=_recording_factory(created),
            crawl_defaults=CrawlRunConfig(max_workers=3),
        )
        build = _build_of(second)
        assert second.errors == []
        assert build.index_build_id == _build_of(first).index_build_id
        assert build.docs[0].content_hash == content_hash(b"retained bytes")
        # Пул мог быть создан (K=3), но resume в него не отправляется; краулер не вызван.
        assert all(p.submitted == 0 for p in created)
    finally:
        store.close()


def test_pool_transport_revalidates_all_known_urls_in_bounded_batches(
    tmp_path: Path,
) -> None:
    """M2: pool-ветка — все известные URL ревалидируются, batch ≤ ``max_workers``.

    ``max_documents=1`` оставляет нулевой discovery-room; новая формула обязана
    впускать непрерывный revalidation-блок (не более половины волны), а не по одному
    URL. Иначе цикл делает ``N`` волн, хотя ни один URL не теряется.
    """
    known = [f"https://docs.test/known/{i}" for i in range(8)]
    with SqliteStateStore(":memory:") as state:
        for url in known:
            state.upsert_url_state(
                UrlStateRow(product_id="p1", url=url, normalized_url=url)
            )
        pages = {ROOT: b"root", **{url: b"text" for url in known}}
        created: list[_RecordingPool] = []
        crawler = _Crawler(pages)
        result = _build(
            tmp_path,
            pages=pages,
            crawler=crawler,
            state=state,
            product_modes={"p1": "incremental"},
            pool_factory=_recording_factory(created),
            crawl_defaults=CrawlRunConfig(max_workers=4, max_documents=1),
        )
        build = _build_of(result)
        assert result.errors == []
        assert set(known) <= set(crawler.calls)
        assert len(created) == 1
        pool = created[0]
        # Все 8 известных + корневой seed; за один batch — не более max_workers,
        # а при нулевом remaining — не более половины (reval_n = max_workers // 2).
        assert pool.submitted == len(known) + 1
        assert pool.max_active <= 2
        assert build.truncation[0]["revalidation"] == {
            "selected": len(known),
            "fetched": len(known),
        }
