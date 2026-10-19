"""P4 (§10) — параллельная экстракция: окно/детерминизм/отказы (детерминированные фейки).

Пул экстракции инжектится аддитивным kwarg ``build_corpus(extraction_pool=...)``
(:class:`~core.pipeline.corpus.ExtractionPool`: фабрика исполнителя + рабочий вызов), а ось
``extract_workers`` — через ``crawl_defaults=CrawlRunConfig(...)``. Проверяются замороженные
решения P4-гейта:

1. дефолт ``None``/``1`` ⇒ пул не создаётся, вывод байт-в-байт;
2. паритет sequential ‖ pool (FIFO-доводка; ``errors`` — мультимножество);
3. граница окна: ``max_inflight ≤ extract_workers``, доводка строго FIFO;
4. честная ёмкость с окном: ровно ``max_documents`` свеже-добытых, усечение;
5. отказ воркера (``error`` / исключение) — явный ``FAILED parser_failed`` + ``PARTIAL``;
6. краш воркера (``BrokenProcessPool``) — in-flight ``FAILED``, пул пересобирается (bounded);
7. таймаут зависшего воркера — ``FAILED`` + ``terminate_workers`` + продолжение;
8. payload без байтов (только ``raw_path`` + метаданные), воркер без ``store``/``session``;
9. ось ``>1`` без пула — явная запись в ``errors`` + inline (не тихо);
10. резолв цепочкой app < run < site, дайджест/манифест.

Данные категорийно-нейтральны (§1.3); адаптеры (docling) не импортируются — только порты.
Item 11 (реальный docling) — отдельный gated-тест в конце.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any

import pytest

from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlRunConfig,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    _ExtractRequest,
    ExtractionPool,
    IndexBuild,
    build_corpus,
)
from core.pipeline.crawl_frontier import resolve_crawl_limits
from core.pipeline.crawl_session import default_session_id
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-extract-pool"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"
ROOT = "https://docs.test/"


# --------------------------------------------------------------------------- #
# Харнесс (план, прогон, порты-заглушки)
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


class _Crawler:
    version = "crawler-1"
    engine = "http"
    supports_browser = True

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
        )


class _Extractor:
    """Последовательный экстрактор: ``text = decode(content)``, ``hash = content_hash``."""

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


class _RaisingExtractor:
    """Inline-экстрактор, падающий на каждой странице (эмуляция ошибки извлечения)."""

    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        raise RuntimeError("boom")


# --- детерминированные воркеры пула (совпадают с ``_Extractor``) ------------- #


def _reconstruct_worker(request: _ExtractRequest) -> ExtractedDoc:
    """Детерминированный воркер: читает retained-файл (I19) и повторяет ``_Extractor``."""
    content = Path(request.raw_path).read_bytes()
    return ExtractedDoc(
        text=content.decode("utf-8", errors="replace"),
        content_hash=content_hash(content),
        extractor_version="extractor-1",
        page_count=1,
    )


def _error_worker(request: _ExtractRequest) -> ExtractedDoc:
    return ExtractedDoc(
        text="", content_hash="", extractor_version="extractor-1", error="nope"
    )


def _raise_worker(request: _ExtractRequest) -> ExtractedDoc:
    raise RuntimeError("boom")


# --- фейковый исполнитель (запись порядка/границ, эмуляция отказов) ---------- #


class _FakeFuture:
    def __init__(self, pool: "_FakePool", fn: Any, request: _ExtractRequest) -> None:
        self._pool = pool
        self._fn = fn
        self._request = request
        self._done = False
        self._value: Any = None
        self._error: BaseException | None = None

    def result(self, timeout: float | None = None) -> Any:
        pool = self._pool
        if pool._hang and not self._done:
            raise TimeoutError()
        pool._result_calls += 1
        if pool._crash_on is not None and pool._result_calls == pool._crash_on:
            pool._open -= 1
            raise BrokenProcessPool("worker died")
        if not self._done:
            try:
                self._value = self._fn(self._request)
            except BaseException as exc:  # noqa: BLE001 — эмуляция отказа воркера
                self._error = exc
            self._done = True
            pool._open -= 1
            pool.completions.append(self._request.url)
        if self._error is not None:
            raise self._error
        return self._value


class _FakePool:
    """Записывающий исполнитель: порядок submit/result, границы in-flight, отказы."""

    def __init__(
        self, size: int, *, hang: bool = False, crash_on: int | None = None
    ) -> None:
        self.size = size
        self._hang = hang
        self._crash_on = crash_on
        self._open = 0
        self._result_calls = 0
        self.max_inflight = 0
        self.submitted_requests: list[_ExtractRequest] = []
        self.completions: list[str] = []
        self.terminate_calls = 0
        self.shutdown_calls = 0

    def submit(self, fn: Any, request: _ExtractRequest) -> _FakeFuture:
        self.submitted_requests.append(request)
        self._open += 1
        self.max_inflight = max(self.max_inflight, self._open)
        return _FakeFuture(self, fn, request)

    def shutdown(self, wait: bool = True) -> None:
        self.shutdown_calls += 1

    def terminate_workers(self) -> None:
        self.terminate_calls += 1


def _recording_factory(created: list[_FakePool], **kw: Any):
    # P5: фабрика получает (размер, уровень) — уровень здесь несущественен (фейк).
    def factory(n: int, level: Any = None) -> _FakePool:
        pool = _FakePool(n, **kw)
        created.append(pool)
        return pool

    return factory


def _build(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    urls: dict[str, list[str]] | None = None,
    store: SqliteStore | None = None,
    run_id: str | None = None,
    crawl_defaults: CrawlRunConfig | None = None,
    extraction_pool: ExtractionPool | None = None,
    extract_task_timeout_s: float | None = None,
    extractor: Any = None,
):
    return build_corpus(
        _plan(),
        _Crawler(pages),
        _Extractor() if extractor is None else extractor,
        _Chunker(),
        store,
        urls=urls or {"p1": [ROOT]},
        raw_root=tmp_path,
        run_id=run_id,
        crawl_defaults=crawl_defaults,
        extraction_pool=extraction_pool,
        extract_task_timeout_s=extract_task_timeout_s,
    )


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _snapshot(result: Any, store: SqliteStore | None) -> dict[str, Any]:
    build = _build_of(result)
    docs = sorted((d.product_id, d.url, d.content_hash) for d in build.docs)
    entries = sorted(
        (e.product_id, e.content_digest, e.stats_json) for e in build.entries
    )
    session = store.get_crawl_session(SESSION_ID) if store is not None else None
    return {
        "index_build_id": build.index_build_id,
        "chunk_count": build.chunk_count,
        "docs": docs,
        "entries": entries,
        "truncated": build.truncated,
        "truncation": [
            (r["product_id"], r["root"], r["pages_fetched"], r["truncated_reason"])
            for r in sorted(build.truncation, key=lambda r: (r["product_id"], r["root"]))
        ],
        "errors_sorted": sorted(result.errors),
        "session_status": session.status.value if session is not None else None,
    }


def _session_items(store: SqliteStore) -> dict[tuple[str, str], Any]:
    return {(i.product_id, i.url): i for i in store.iter_crawl_session_items(SESSION_ID)}


def _site_dup() -> dict[str, bytes]:
    """Root + два дубликата (FIFO-доводка определяет победителя дедупа) + два разных."""
    dup = b"<html>same</html>"
    return {
        ROOT: (
            b'<html><a href="/a">a</a><a href="/b">b</a>'
            b'<a href="/c">c</a><a href="/d">d</a></html>'
        ),
        "https://docs.test/a": dup,
        "https://docs.test/b": dup,  # дубликат a (I17: один документ на контент)
        "https://docs.test/c": b"<html>three</html>",
        "https://docs.test/d": b"<html>four</html>",
    }


BFS1 = CrawlRunConfig(max_depth=1)


# --------------------------------------------------------------------------- #
# (1) Дефолт неизменен: None/1 ⇒ пул не создаётся, выводы идентичны
# --------------------------------------------------------------------------- #


def test_extract_workers_default_is_byte_identical(tmp_path: Path) -> None:
    pages = _site_dup()

    baseline = _build(tmp_path / "base", pages=pages, crawl_defaults=BFS1)

    created: list[_FakePool] = []
    factory = _recording_factory(created)
    pool = ExtractionPool(pool_factory=factory, worker=_reconstruct_worker)

    default = _build(
        tmp_path / "default",
        pages=pages,
        crawl_defaults=BFS1,
        extraction_pool=pool,  # ось не задана (None) ⇒ пул не создаётся
    )
    one = _build(
        tmp_path / "one",
        pages=pages,
        crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=1),
        extraction_pool=pool,
    )

    # Самый важный guardrail: ось None/1 ⇒ фабрика пула НЕ вызвана.
    assert created == []
    b_base, b_default, b_one = (
        _build_of(baseline),
        _build_of(default),
        _build_of(one),
    )
    assert b_base.index_build_id == b_default.index_build_id == b_one.index_build_id
    assert sorted((d.doc_id, d.url) for d in b_base.docs) == sorted(
        (d.doc_id, d.url) for d in b_default.docs
    ) == sorted((d.doc_id, d.url) for d in b_one.docs)
    assert baseline.errors == default.errors == one.errors == []


# --------------------------------------------------------------------------- #
# (1b) Рециклинг воркеров (OOM): ось включает пул при W=1; None — прежнее поведение
# --------------------------------------------------------------------------- #


def test_extract_max_tasks_per_child_none_keeps_inline(tmp_path: Path) -> None:
    """Ось не задана ⇒ даже recycling-пул не создаётся при ``extract_workers=1``."""
    pages = _site_dup()
    created: list[_FakePool] = []
    pool = ExtractionPool(
        pool_factory=_recording_factory(created),
        worker=_reconstruct_worker,
        supports_worker_recycling=True,
    )
    _build(
        tmp_path,
        pages=pages,
        crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=1),
        extraction_pool=pool,
    )
    assert created == []


def test_extract_max_tasks_per_child_w1_uses_pool(tmp_path: Path) -> None:
    """P4/OOM: ``N`` задан ⇒ пул даже при W=1, окно 1, ``N`` доходит до фабрики."""
    pages = _site_dup()
    baseline = _build(tmp_path / "base", pages=pages, crawl_defaults=BFS1)

    created: list[_FakePool] = []

    def factory(n: int, level: Any = None, *, max_tasks_per_child: int | None = None):
        fake = _FakePool(n)
        fake.max_tasks_per_child = max_tasks_per_child
        created.append(fake)
        return fake

    pool = ExtractionPool(
        pool_factory=factory,
        worker=_reconstruct_worker,
        supports_worker_recycling=True,
    )
    recycled = _build(
        tmp_path / "recycle",
        pages=pages,
        crawl_defaults=CrawlRunConfig(
            max_depth=1, extract_workers=1, extract_max_tasks_per_child=3
        ),
        extraction_pool=pool,
    )

    assert len(created) == 1
    assert created[0].size == 1
    assert created[0].max_tasks_per_child == 3
    # Корпус идентичен inline-эталону: рециклинг меняет только ресурсы, не вывод.
    assert _build_of(recycled).index_build_id == _build_of(baseline).index_build_id
    assert recycled.errors == []


def test_extract_max_tasks_per_child_with_w2(tmp_path: Path) -> None:
    """``N`` вместе с ``W>1``: окно = W, счётчик рециклинга едет в фабрику."""
    pages = _site_dup()
    created: list[_FakePool] = []

    def factory(n: int, level: Any = None, *, max_tasks_per_child: int | None = None):
        fake = _FakePool(n)
        fake.max_tasks_per_child = max_tasks_per_child
        created.append(fake)
        return fake

    pool = ExtractionPool(
        pool_factory=factory,
        worker=_reconstruct_worker,
        supports_worker_recycling=True,
    )
    _build(
        tmp_path,
        pages=pages,
        crawl_defaults=CrawlRunConfig(
            max_depth=1, extract_workers=2, extract_max_tasks_per_child=7
        ),
        extraction_pool=pool,
    )
    assert len(created) == 1
    assert created[0].size == 2
    assert created[0].max_tasks_per_child == 7


def test_extract_max_tasks_per_child_not_supported_is_visible(tmp_path: Path) -> None:
    """``split``-пул (без рециклинга): ось не молчит — запись в ``errors`` + inline."""
    pages = _site_dup()
    created: list[_FakePool] = []
    pool = ExtractionPool(
        pool_factory=_recording_factory(created),
        worker=_reconstruct_worker,
        supports_worker_recycling=False,
    )
    result = _build(
        tmp_path,
        pages=pages,
        crawl_defaults=CrawlRunConfig(
            max_depth=1, extract_workers=1, extract_max_tasks_per_child=5
        ),
        extraction_pool=pool,
    )
    assert created == []
    assert any("extract_max_tasks_per_child" in e and "local" in e for e in result.errors)


# --------------------------------------------------------------------------- #
# (2) Паритет sequential ‖ pool (+ mutation-check: FIFO-доводка несущая)
# --------------------------------------------------------------------------- #


def test_extract_pool_parity_sequential_vs_pool(tmp_path: Path) -> None:
    pages = _site_dup()
    store_a, store_b = SqliteStore(":memory:"), SqliteStore(":memory:")
    try:
        _make_run(store_a)
        _make_run(store_b)

        sequential = _build(
            tmp_path / "seq", pages=pages, store=store_a, run_id=RUN_ID,
            crawl_defaults=BFS1,
        )

        created: list[_FakePool] = []
        pool = ExtractionPool(
            pool_factory=_recording_factory(created),
            worker=_reconstruct_worker,
        )
        pooled = _build(
            tmp_path / "pool",
            pages=pages,
            store=store_b,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=2),
            extraction_pool=pool,
        )

        seq_snap = _snapshot(sequential, store_a)
        pool_snap = _snapshot(pooled, store_b)
        for key in seq_snap:
            if key == "errors_sorted":
                # R1: интерливинг ``errors`` может отличаться — сравнение мультимножеством.
                assert sorted(seq_snap[key]) == sorted(pool_snap[key]), key
            else:
                assert seq_snap[key] == pool_snap[key], (
                    f"паритет нарушен в {key!r}:\n seq={seq_snap[key]!r}\n"
                    f" pool={pool_snap[key]!r}"
                )

        # Победитель дедупа — первый по FIFO (``a``), одинаков в обоих прогонах.
        dup_hash = content_hash(pages["https://docs.test/a"])
        winners = [d for d in _build_of(pooled).docs if d.content_hash == dup_hash]
        assert len(winners) == 1
        assert winners[0].url == "https://docs.test/a"
        assert _session_items(store_b)[("p1", "https://docs.test/a")].status is (
            CrawlItemStatus.FETCHED
        )
    finally:
        store_a.close()
        store_b.close()


# --------------------------------------------------------------------------- #
# (3) Граница окна: max_inflight ≤ extract_workers, доводка строго FIFO
# --------------------------------------------------------------------------- #


def test_extract_window_bounds_inflight(tmp_path: Path) -> None:
    pages = _site_dup()
    created: list[_FakePool] = []
    pool = ExtractionPool(
        pool_factory=_recording_factory(created),
        worker=_reconstruct_worker,
    )
    _build(
        tmp_path,
        pages=pages,
        crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=2),
        extraction_pool=pool,
    )

    assert len(created) == 1
    fake = created[0]
    assert fake.size == 2
    assert 1 < fake.max_inflight <= 2  # окно реально используется и не превышено
    # Доводка строго FIFO: порядок ``result()`` = порядок submit.
    assert [r.url for r in fake.submitted_requests] == fake.completions


# --------------------------------------------------------------------------- #
# (4) Ёмкость честна с окном: ровно max_documents, усечение, PARTIAL
# --------------------------------------------------------------------------- #


def test_extract_capacity_honest_with_window(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        pages = _site_dup()
        created: list[_FakePool] = []
        pool = ExtractionPool(
            pool_factory=_recording_factory(created),
            worker=_reconstruct_worker,
        )
        result = _build(
            tmp_path,
            pages=pages,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=4, max_documents=2),
            extraction_pool=pool,
        )
        build = _build_of(result)
        # Ровно 2 свеже-добытых и ровно 2 задания экстракции (нет пере-удержания).
        assert len(build.docs) == 2
        assert created[0].submitted_requests and len(created[0].submitted_requests) == 2
        assert build.truncated is True
        assert build.truncation[0]["truncated_reason"] == "max_documents"
        assert store.get_crawl_session(SESSION_ID).status.value == "partial"
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) Отказ воркера честен: ``error`` и исключение ⇒ FAILED parser_failed + PARTIAL
# --------------------------------------------------------------------------- #


def test_extract_worker_error_is_honest(tmp_path: Path) -> None:
    for worker in (_error_worker, _raise_worker):
        store = SqliteStore(":memory:")
        try:
            _make_run(store)
            page = b"<html>hello</html>"
            created: list[_FakePool] = []
            pool = ExtractionPool(
                pool_factory=_recording_factory(created), worker=worker
            )
            result = _build(
                tmp_path / worker.__name__,
                pages={ROOT: page},
                store=store,
                run_id=RUN_ID,
                crawl_defaults=CrawlRunConfig(extract_workers=2),
                extraction_pool=pool,
            )
            build = _build_of(result)
            assert result.status == "ok"
            assert build.docs == []
            item = _session_items(store)[("p1", ROOT)]
            assert item.status is CrawlItemStatus.FAILED
            assert item.error_kind == "parser_failed"
            assert store.get_crawl_session(SESSION_ID).status.value == "partial"
            assert any(ROOT in e and "extract" in e for e in result.errors)
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# (5b) D1-регресс: краш/таймаут воркера не теряет исходящие ссылки упавшей
#      страницы (доводка через ``_commit_outcome``, §11.6/§12.5)
# --------------------------------------------------------------------------- #


def _crawled_urls(store: SqliteStore) -> set[str]:
    """Множество URL, доведённых до терминального элемента сеанса (FETCHED/FAILED)."""
    return {item.url for item in store.iter_crawl_session_items(SESSION_ID)}


def _pages_fetched(result: Any) -> int:
    """Свеже-добытые страницы корня (``frontier.fetched`` через ``IndexBuild.truncation``)."""
    return sum(int(rec["pages_fetched"]) for rec in _build_of(result).truncation)


def test_extract_failure_keeps_out_links_like_inline(tmp_path: Path) -> None:
    """D1: краш/таймаут воркера не теряет исходящие ссылки упавшей страницы.

    Регресс: ``_fail_extract`` в обход ``_commit_outcome`` терял BFS-поддерево упавшей
    страницы (``crawled = {root}`` вместо ``{root, /a, /b}``). После фикса отказ доводится
    тем же упорядоченным путём, что inline-путь (``note_fetched`` + ``offer`` + ``errors`` +
    ``_mark``), а ``note_fetched`` срабатывает ровно один раз на свеже-добытую страницу.
    """
    pages = {
        ROOT: b'<html><a href="/a">a</a><a href="/b">b</a></html>',
        "https://docs.test/a": b"<html>a</html>",
        "https://docs.test/b": b"<html>b</html>",
    }
    expected = {ROOT, "https://docs.test/a", "https://docs.test/b"}

    def run(
        name: str,
        *,
        store: SqliteStore,
        crawl_defaults: CrawlRunConfig,
        extraction_pool: ExtractionPool | None,
        extractor: Any = None,
    ):
        return _build(
            tmp_path / name,
            pages=pages,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=crawl_defaults,
            extraction_pool=extraction_pool,
            extractor=extractor,
        )

    pool_defaults = CrawlRunConfig(max_depth=1, extract_workers=2)
    store_inline, store_raise, store_crash = (
        SqliteStore(":memory:"),
        SqliteStore(":memory:"),
        SqliteStore(":memory:"),
    )
    try:
        for s in (store_inline, store_raise, store_crash):
            _make_run(s)

        # (a) inline: экстрактор бросает на каждой странице — ссылки всё равно предлагаются.
        inline = run(
            "inline",
            store=store_inline,
            crawl_defaults=BFS1,
            extraction_pool=None,
            extractor=_RaisingExtractor(),
        )
        # (b) пул, воркер бросает исключение (этот путь уже шёл через ``_commit_outcome``).
        raised = run(
            "raise",
            store=store_raise,
            crawl_defaults=pool_defaults,
            extraction_pool=ExtractionPool(
                pool_factory=_recording_factory([]), worker=_raise_worker
            ),
        )
        # (c) пул, воркер умер (``BrokenProcessPool`` на root) — регресс-путь ``_fail_extract``.
        created: list[_FakePool] = []

        def crash_factory(n: int, level: Any = None) -> _FakePool:
            # 1-й пул: падает на 1-м ``result()``; 2-й (rebuild): рабочий.
            fake = _FakePool(n, crash_on=1 if not created else None)
            created.append(fake)
            return fake

        crashed = run(
            "crash",
            store=store_crash,
            crawl_defaults=pool_defaults,
            extraction_pool=ExtractionPool(
                pool_factory=crash_factory, worker=_reconstruct_worker
            ),
        )
        assert created and created[0]._crash_on == 1  # краш действительно случился

        # Все три пути видят одно подмножество — поддерево упавшей страницы не потеряно.
        assert _crawled_urls(store_inline) == expected
        assert _crawled_urls(store_raise) == expected
        assert _crawled_urls(store_crash) == expected

        # ``note_fetched`` — ровно один раз на свеже-добытую страницу (3 ⇒ нет двойного учёта).
        assert _pages_fetched(inline) == 3
        assert _pages_fetched(raised) == 3
        assert _pages_fetched(crashed) == 3

        # Сеанс честно ``PARTIAL`` (есть ``FAILED``); ошибка честно в ``errors``.
        assert store_crash.get_crawl_session(SESSION_ID).status.value == "partial"
        assert any("extraction worker died" in e for e in crashed.errors)
    finally:
        store_inline.close()
        store_raise.close()
        store_crash.close()


# --------------------------------------------------------------------------- #
# (6) Краш воркера: in-flight → FAILED, пул пересобирается (bounded)
# --------------------------------------------------------------------------- #


def test_extract_worker_crash_reaps_and_degrades(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        pages = _site_dup()
        created: list[_FakePool] = []

        def factory(n: int, level: Any = None) -> _FakePool:
            # 1-й пул: падает на 2-м ``result()``; 2-й (rebuild): рабочий.
            crash_on = 2 if not created else None
            fake = _FakePool(n, crash_on=crash_on)
            created.append(fake)
            return fake

        pool = ExtractionPool(pool_factory=factory, worker=_reconstruct_worker)
        result = _build(
            tmp_path,
            pages=pages,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=2),
            extraction_pool=pool,
        )
        build = _build_of(result)
        items = _session_items(store)

        # Краш: сопутствующие in-flight помечены FAILED parser_failed (не тихо).
        failed = [u for (p, u), i in items.items() if i.status is CrawlItemStatus.FAILED]
        assert failed, items
        assert all(items[("p1", u)].error_kind == "parser_failed" for u in failed)
        # Пул пересобран (bounded rebuild) и доводка продолжилась.
        assert len(created) >= 2
        assert build.docs, "после пересборки пула экстракция обязана продолжиться"
        assert store.get_crawl_session(SESSION_ID).status.value == "partial"
        assert any("extraction worker died" in e for e in result.errors)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (7) Таймаут зависшего воркера: FAILED + terminate_workers + продолжение
# --------------------------------------------------------------------------- #


def test_extract_timeout_marks_failed_and_terminates(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        created: list[_FakePool] = []
        pool = ExtractionPool(
            pool_factory=_recording_factory(created, hang=True),
            worker=_reconstruct_worker,
        )
        result = _build(
            tmp_path,
            pages={ROOT: b"<html>slow</html>"},
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(extract_workers=2),
            extraction_pool=pool,
            extract_task_timeout_s=0.01,
        )
        item = _session_items(store)[("p1", ROOT)]
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == "parser_failed"
        assert "timeout" in (item.error or "")
        assert created[0].terminate_calls >= 1  # reap зависшего пула
        assert result.status == "ok"  # прогон продолжается, не падает
        assert store.get_crawl_session(SESSION_ID).status.value == "partial"
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (8) Payload без байтов; воркер без store/session (структурно)
# --------------------------------------------------------------------------- #


def test_extract_request_carries_no_bytes(tmp_path: Path) -> None:
    names = {f.name for f in dataclasses.fields(_ExtractRequest)}
    assert names == {
        "raw_path",
        "url",
        "fetched_at",
        "fetch_mode",
        "lang",
        "status_code",
        "content_type",
        "engine",
        "route",
        # P5: уровень извлечения (аддитивное поле payload; примитив StrEnum).
        "level",
        # Ф1.2: корень и бюджет межпрогонного кэша экстракции — примитивы, не объект
        # кэша: воркер строит свой экземпляр (иначе счётчики были бы копией родителя).
        "cache_root",
        "cache_max_bytes",
    }
    # Байты не пиклятся: у payload нет поля content/bytes (только путь к retained-файлу).
    assert "content" not in names and "bytes" not in names and "raw_bytes" not in names
    # Кэш не пересекает границу процесса как объект — только примитивы конфигурации.
    assert "cache" not in names and "extract_cache" not in names

    # Воркер получает ровно один аргумент (payload) и не несёт ссылок на store/session.
    from core.pipeline.corpus import _extract_worker

    assert list(_extract_worker.__code__.co_varnames)[:1] == ["request"]
    assert "store" not in _extract_worker.__code__.co_names
    assert "session" not in _extract_worker.__code__.co_names

    # Сквозная фиксация: пул получает payload без байтов (структурная проверка полей).
    created: list[_FakePool] = []
    pool = ExtractionPool(
        pool_factory=_recording_factory(created),
        worker=_reconstruct_worker,
    )
    _build(
        tmp_path,
        pages={ROOT: b"<html>bytes-net</html>"},
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=pool,
    )
    req = created[0].submitted_requests[0]
    assert isinstance(req.raw_path, str) and req.raw_path
    assert not hasattr(req, "content")


# --------------------------------------------------------------------------- #
# (9) Ось >1 без пула: явная запись + inline (не тихо, не краш)
# --------------------------------------------------------------------------- #


def test_extract_pool_missing_factory_falls_back_inline(tmp_path: Path) -> None:
    pages = _site_dup()
    baseline = _build(tmp_path / "base", pages=pages, crawl_defaults=BFS1)
    result = _build(
        tmp_path / "nopool",
        pages=pages,
        crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=3),
        extraction_pool=None,
    )
    assert result.status == "ok"
    assert any("extract_workers" in e and "inline" in e for e in result.errors)
    # Inline-экстракция: вывод идентичен последовательному эталону.
    assert _build_of(result).index_build_id == _build_of(baseline).index_build_id
    assert sorted((d.doc_id, d.url) for d in _build_of(result).docs) == sorted(
        (d.doc_id, d.url) for d in _build_of(baseline).docs
    )


# --------------------------------------------------------------------------- #
# (10) Резолв цепочкой app < run < site; дайджест/манифест
# --------------------------------------------------------------------------- #


def test_extract_workers_resolved_through_chain() -> None:
    app = CrawlConfig(extract_workers=2)
    run = CrawlRunConfig(extract_workers=3)
    site = DomainProfile(extract_workers=5)

    # site > run > app.
    limits = resolve_crawl_limits(
        "https://example.com/x", app=app, run=run, profiles={"example.com": site}
    )
    assert limits.extract_workers == 5
    # run > app (нет per-site).
    limits = resolve_crawl_limits("https://example.com/x", app=app, run=run, profiles={})
    assert limits.extract_workers == 3
    # app (run не задан).
    limits = resolve_crawl_limits("https://example.com/x", app=app, run=None, profiles={})
    assert limits.extract_workers == 2
    # Не задано нигде → None (поведение неизменно).
    assert resolve_crawl_limits(
        "https://example.com/x", app=CrawlConfig(), run=None, profiles={}
    ).extract_workers is None

    # Дайджест разрешённой конфигурации и поле манифеста.
    from types import SimpleNamespace

    from core.pipeline.report import EXECUTION_FIELDS, build_manifest

    assert "crawl_extract_workers" in EXECUTION_FIELDS
    cfg = RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort-1",
        products=["p1"],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    inputs = SimpleNamespace(run=cfg, package=None)
    plan = SimpleNamespace(run_id="run-x", anchor="p1", modules=[], cohort_ref="cohort-1")
    base = build_manifest(inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z")  # type: ignore[arg-type]

    cfg2 = cfg.model_copy(update={"crawl": CrawlRunConfig(extract_workers=4)})
    inputs2 = SimpleNamespace(run=cfg2, package=None)
    changed = build_manifest(inputs=inputs2, plan=plan, created_at="2026-01-01T00:00:00Z")  # type: ignore[arg-type]
    assert changed.crawl_extract_workers == 4
    assert changed.crawl_config_digest != base.crawl_config_digest


# --------------------------------------------------------------------------- #
# (10b) Ось рециклинга: цепочка app < run < site, дайджест/манифест, ge=1
# --------------------------------------------------------------------------- #


def test_extract_max_tasks_per_child_resolved_through_chain() -> None:
    app = CrawlConfig(extract_max_tasks_per_child=10)
    run = CrawlRunConfig(extract_max_tasks_per_child=20)
    site = DomainProfile(extract_max_tasks_per_child=30)

    # site > run > app.
    limits = resolve_crawl_limits(
        "https://example.com/x", app=app, run=run, profiles={"example.com": site}
    )
    assert limits.extract_max_tasks_per_child == 30
    limits = resolve_crawl_limits("https://example.com/x", app=app, run=run, profiles={})
    assert limits.extract_max_tasks_per_child == 20
    limits = resolve_crawl_limits("https://example.com/x", app=app, run=None, profiles={})
    assert limits.extract_max_tasks_per_child == 10
    # Не задано нигде → None (поведение неизменно).
    assert (
        resolve_crawl_limits(
            "https://example.com/x", app=CrawlConfig(), run=None, profiles={}
        ).extract_max_tasks_per_child
        is None
    )

    # Валидация: ``ge=1`` на всех трёх слоях.
    for factory in (
        lambda: CrawlConfig(extract_max_tasks_per_child=0),
        lambda: CrawlRunConfig(extract_max_tasks_per_child=0),
        lambda: DomainProfile(extract_max_tasks_per_child=0),
    ):
        with pytest.raises(ValueError):
            factory()

    # Манифест/дайджест: смена оси — граница ``execution_change`` (§13).
    from types import SimpleNamespace

    from core.pipeline.report import EXECUTION_FIELDS, build_manifest

    assert "crawl_extract_max_tasks_per_child" in EXECUTION_FIELDS
    cfg = RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort-1",
        products=["p1"],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    inputs = SimpleNamespace(run=cfg, package=None)
    plan = SimpleNamespace(run_id="run-x", anchor="p1", modules=[], cohort_ref="cohort-1")
    base = build_manifest(inputs=inputs, plan=plan, created_at="2026-01-01T00:00:00Z")  # type: ignore[arg-type]
    assert base.crawl_extract_max_tasks_per_child is None

    cfg2 = cfg.model_copy(
        update={"crawl": CrawlRunConfig(extract_max_tasks_per_child=25)}
    )
    changed = build_manifest(
        inputs=SimpleNamespace(run=cfg2, package=None),
        plan=plan,
        created_at="2026-01-01T00:00:00Z",  # type: ignore[arg-type]
    )
    assert changed.crawl_extract_max_tasks_per_child == 25
    assert changed.crawl_config_digest != base.crawl_config_digest


# --------------------------------------------------------------------------- #
# (11) Gated/offline: реальный docling — байт-идентичность markdown
#      между процессами и в ProcessPoolExecutor (§5 / §10 item 11)
# --------------------------------------------------------------------------- #

#: Малый cached PDF из §5 (201 104 B, ≈36 с/экстракция) — offline-прогон остаётся терпимым.
_SMALL_PDF = (
    "corpus_raw/jatoba/"
    "d1af57a1a2591c6c4a14bb77046637ecd161ceb9feeb8cf3f06e14546f46d6a8.bin"
)

#: Дочерняя программа (отдельный процесс): печатает hash+text markdown как JSON.
_CHILD_EXTRACT_SCRIPT = """
import json, sys
from pathlib import Path
from core.adapters.extract_document import DoclingDocumentExtractor
content = Path(sys.argv[1]).read_bytes()
doc = DoclingDocumentExtractor().extract_bytes(content, "document.pdf")
print(json.dumps({"hash": doc.content_hash, "text": doc.text}))
"""

#: per-worker конвертер для ProcessPoolExecutor: строится инициализатором в ребёнке.
_DOCLING_WORKER: Any = None


def _docling_worker_init() -> None:
    """Инициализатор воркера: один ``DocumentConverter`` (внутри ``DoclingDocumentExtractor``)."""
    global _DOCLING_WORKER
    from core.adapters.extract_document import DoclingDocumentExtractor

    _DOCLING_WORKER = DoclingDocumentExtractor()


def _docling_worker(pdf_path: str) -> dict[str, Any]:
    """Module-level (picklable by reference): перечитывает PDF и извлекает тем же конвертером."""
    from pathlib import Path

    content = Path(pdf_path).read_bytes()
    doc = _DOCLING_WORKER.extract_bytes(content, "document.pdf")
    return {"hash": doc.content_hash, "text": doc.text}


def _cached_small_pdf(max_bytes: int = 400_000) -> Path | None:
    """Первый доступный cached PDF (малый из §5 — приоритетно; иначе любой ≤ ``max_bytes``)."""
    preferred = Path(_SMALL_PDF)
    if preferred.exists() and preferred.read_bytes()[:4] == b"%PDF":
        return preferred
    for candidate in sorted(Path("corpus_raw").glob("*/*.bin")):
        try:
            if (
                candidate.stat().st_size <= max_bytes
                and candidate.read_bytes()[:4] == b"%PDF"
            ):
                return candidate
        except OSError:  # pragma: no cover — гонка/права на файл
            continue
    return None


@pytest.mark.slow
def test_docling_determinism_across_processes() -> None:
    """Реальный docling: markdown байт-идентичен (a) между процессами, (b) в пуле (§5/§10.11).

    CI по умолчанию — **skip** (тяжело/офлайн, ≈2 GiB RSS на процесс). Включается явной
    опт-ин-переменной ``COHOUND_RUN_DOCLING_DETERMINISM=1`` при наличии кэшированного PDF и
    весов. Сравниваются ``content_hash`` и ``text`` markdown: (a) два **genuinely отдельных
    процесса** (``subprocess`` — независимая загрузка весов), (b) ``ProcessPoolExecutor(2)``
    с per-worker конвертером (``initializer``).
    """
    import multiprocessing
    import os
    import subprocess
    import sys
    from concurrent.futures import ProcessPoolExecutor

    if os.environ.get("COHOUND_RUN_DOCLING_DETERMINISM") != "1":
        pytest.skip("тяжёлый offline-тест: нужен COHOUND_RUN_DOCLING_DETERMINISM=1")
    try:
        from core.adapters.extract_document import DoclingDocumentExtractor
    except Exception as exc:  # noqa: BLE001 — зависимость недоступна
        pytest.skip(f"docling недоступен: {exc}")

    extractor = DoclingDocumentExtractor()
    if not extractor.available:
        pytest.skip("docling/веса недоступны (offline)")

    pdf = _cached_small_pdf()
    if pdf is None:
        pytest.skip("нет cached PDF в corpus_raw/*/*.bin")

    inline = extractor.extract_bytes(pdf.read_bytes(), "document.pdf")
    assert inline.error is None, inline.error

    # (a) Два genuinely отдельных процесса — независимая загрузка весов и конвертация.
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    env.setdefault("PYTHONPATH", str(root))
    for _ in range(2):
        proc = subprocess.run(
            [sys.executable, "-c", _CHILD_EXTRACT_SCRIPT, str(pdf)],
            cwd=str(root),
            env=env,
            capture_output=True,
            text=True,
            timeout=1800,
        )
        assert proc.returncode == 0, proc.stderr
        child = json.loads(proc.stdout.strip().splitlines()[-1])
        assert child["hash"] == inline.content_hash
        assert child["text"] == inline.text

    # (b) ProcessPoolExecutor(2) с per-worker конвертером (initializer), 2 задания.
    try:
        context = multiprocessing.get_context("forkserver")
    except ValueError:  # pragma: no cover — платформо-зависимо
        pytest.skip("метод multiprocessing 'forkserver' недоступен")
    with ProcessPoolExecutor(
        max_workers=2, mp_context=context, initializer=_docling_worker_init
    ) as pool:
        futures = [pool.submit(_docling_worker, str(pdf)) for _ in range(2)]
        for future in futures:
            result = future.result(timeout=1800)
            assert result["hash"] == inline.content_hash
            assert result["text"] == inline.text
