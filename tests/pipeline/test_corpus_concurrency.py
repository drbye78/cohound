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
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest

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
from core.pipeline import RunPlan, corpus_drain, corpus_driver
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.corpus_ingest import LocalFetch
from core.pipeline.crawl_session import default_session_id, read_provenance
from core.ports.state import UrlStateRow
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
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
        delays: dict[str, float] | None = None,
        gate_urls: set[str] | None = None,
    ) -> None:
        self.pages = dict(pages)
        self.raise_urls = set(raise_urls or ())
        self.calls: list[str] = []
        #: Порядок **завершения** фетчей (F3, T3/T4): не порядок отправки. Именно он
        #: проверяет, что артефакты корпуса от него не зависят.
        self.completed: list[str] = []
        self._gate_to = gate_to
        self._gate_timeout = gate_timeout
        #: Гейт только на эти URL (``None`` — на все). Нужен, когда гейт изображает
        #: встречу **корней**, а не любых страниц: иначе волны детей платят таймаут.
        self._gate_urls = set(gate_urls) if gate_urls is not None else None
        #: Управляемые задержки по URL: переставляют порядок завершения страниц.
        self._delays = dict(delays or {})
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        with self._lock:
            self.calls.append(url)
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            if (
                self._gate_to is not None
                and self._gate_to > 1
                and (self._gate_urls is None or url in self._gate_urls)
            ):
                deadline = time.monotonic() + self._gate_timeout
                while time.monotonic() < deadline:
                    with self._lock:
                        if self._active >= self._gate_to:
                            break
                    time.sleep(0.001)
            if url in self.raise_urls:
                raise RuntimeError(f"crawl failed: {url}")
            delay = self._delays.get(url)
            if delay:
                time.sleep(delay)
            page = RawPage(
                url=url,
                content=self.pages[url],
                fetched_at=TS,
                fetch_mode=plan.fetch,
                lang="en",
            )
            with self._lock:
                self.completed.append(url)
            return page
        finally:
            with self._lock:
                self._active -= 1


class _BombCrawler(_Crawler):
    """Краулер, обязанный НЕ вызываться: любой ``fetch`` — провал (resume)."""

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        raise AssertionError("краулер вызван при наличии retained-байтов (resume)")


class _TimedCrawler(_Crawler):
    """Краулер с отметками начала фетча и пиком активности **по host** (T1/T2).

    T1 спрашивает: два корня одного host не идут одновременно и интервал между их
    запросами не меньше объявленного минимума. T2 — что разные host'ы идут
    одновременно. Оба факта наблюдаемы только по времени/активности, поэтому
    ``_Crawler`` дополняется, а не подменяется: гейт ``gate_to`` остаётся тем же
    (он и делает конкурентность детерминированно наблюдаемой).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.started_at: list[tuple[str, float]] = []
        self.host_active: dict[str, int] = {}
        self.host_max_active: dict[str, int] = {}

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        host = (urlsplit(url).hostname or "").lower()
        with self._lock:
            self.started_at.append((url, time.monotonic()))
            self.host_active[host] = self.host_active.get(host, 0) + 1
            self.host_max_active[host] = max(
                self.host_max_active.get(host, 0), self.host_active[host]
            )
        try:
            return super().fetch(url, plan)
        finally:
            with self._lock:
                self.host_active[host] -= 1


class _ThreadGuardStore:
    """Стор с thread-affinity: мутация из чужого потока — провал теста (T5).

    Инвариант 17 §6 (F4b): состояние фазы мутирует только на потоке драйвера
    (главном), а пулы исполняют ровно ``local_fetch``. Проверка сделана падением, а
    не сверкой лога: нарушение иначе видно только как редкая гонка под нагрузкой.
    """

    #: Префиксы **мутирующих** методов ``StorePort``: чтения разрешены с любого
    #: потока (они и так наблюдаются), записи — только с потока владельца.
    _MUTATING = (
        "add_",
        "create_",
        "delete_",
        "record_",
        "register_",
        "replace_",
        "save_",
        "set_",
        "touch_",
        "update_",
        "upsert_",
        "write_",
    )

    def __init__(self, inner: SqliteStore) -> None:
        self._inner = inner
        self.owner = threading.get_ident()
        self.writes = 0
        self.write_threads: set[int] = set()
        self.read_threads: set[int] = set()
        #: Вызовы, пришедшие не с потока владельца (мутирующие — сразу падение).
        self.foreign: list[str] = []

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr
        mutating = name.startswith(self._MUTATING)

        def call(*args: Any, **kwargs: Any) -> Any:
            ident = threading.get_ident()
            if ident != self.owner:
                self.foreign.append(f"{name}@{threading.current_thread().name}")
                if mutating:
                    raise AssertionError(
                        f"состояние фазы пишется из чужого потока: {name} "
                        f"({threading.current_thread().name})"
                    )
                self.read_threads.add(ident)
            elif mutating:
                self.writes += 1
                self.write_threads.add(ident)
            return attr(*args, **kwargs)

        return call


class _Extractor:
    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        return ExtractedDoc(
            text=_page_bytes(page).decode("utf-8", errors="replace"),
            content_hash=content_hash(_page_bytes(page)),
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


def _docs(store: SqliteStore) -> list[tuple[str, str]]:
    """Retained-set (``doc_id``, ``url``) из стора, в детерминированном порядке.

    ``IndexBuild`` документы больше не несёт (Д-2/A4): носитель — ``corpus_docs``.
    """
    return sorted((d.doc_id, d.url) for d in store.iter_corpus_docs(RUN_ID))


def _multi_host_pages(n: int = 4, body: str = "page") -> dict[str, bytes]:
    links = "".join(
        f'<a href="https://h{i}.docs.test/{i}">{i}</a>' for i in range(n)
    )
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    for i in range(n):
        pages[f"https://h{i}.docs.test/{i}"] = f"<html>{body}-{i}</html>".encode()
    return pages

def _declared_multi_host_profiles(n: int = 4) -> dict[str, DomainProfile]:
    """Объявляет мульти-хостовые поддомены фикстуры (C, ``execution_change`` §13).

    Scope корня — **объявленные** хосты: необъявленный поддомен в scope не входит, поэтому
    «волна независимых URL» на разных хостах обязана эти хосты объявить (ключ
    ``domain_profiles.yaml`` и есть объявление). Пустой ``DomainProfile()`` лимитов корня не
    меняет: они резолвятся по URL корня, а не по обнаруженному URL.
    """
    return {f"h{i}.docs.test": DomainProfile() for i in range(n)}


# --------------------------------------------------------------------------- #
# (1) Дефолт неизменен: None/1 ⇒ пул не создаётся, выводы идентичны
# --------------------------------------------------------------------------- #


def test_default_and_single_worker_are_identical_and_create_no_pool(
    tmp_path: Path,
) -> None:
    pages = _multi_host_pages()
    created: list[_RecordingPool] = []
    factory = _recording_factory(created)

    default = _build(
        tmp_path / "default",
        pages=pages,
        domain_profiles=_declared_multi_host_profiles(),
        pool_factory=factory,
    )
    one = _build(
        tmp_path / "one",
        pages=pages,
        domain_profiles=_declared_multi_host_profiles(),
        crawl_defaults=CrawlRunConfig(max_workers=1),
        pool_factory=factory,
    )

    # Самый важный guardrail: при max_workers in (None, 1) пул НЕ создаётся.
    assert created == []
    b_default, b_one = _build_of(default), _build_of(one)
    assert b_default.index_build_id == b_one.index_build_id
    assert b_default.doc_count == b_one.doc_count
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
        domain_profiles=_declared_multi_host_profiles(4),
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

    def run(sub: str) -> tuple[SqliteStore, Any]:
        store = SqliteStore(":memory:")
        _make_run(store)
        result = _build(
            tmp_path / sub,
            pages=pages,
            domain_profiles=_declared_multi_host_profiles(4),
            crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=4),
            store=store,
            run_id=RUN_ID,
        )
        return store, result

    store_a, result_a = run("a")
    store_b, result_b = run("b")
    try:
        a, b = _build_of(result_a), _build_of(result_b)
        assert a.index_build_id == b.index_build_id
        assert _docs(store_a) == _docs(store_b)

        # Победитель дедупа детерминирован FIFO-коммитом: первый в HTML-порядке (h0).
        dup_hash = content_hash(dup)
        winners = [
            d
            for d in store_a.iter_corpus_docs(RUN_ID)
            if d.content_hash == dup_hash
        ]
        assert len(winners) == 1
        assert winners[0].url == "https://h0.docs.test/0"
    finally:
        store_a.close()
        store_b.close()


# --------------------------------------------------------------------------- #
# (4) Ёмкость: max_documents соблюдён при K>1, усечение честно
# --------------------------------------------------------------------------- #


def test_max_documents_respected_under_concurrency(tmp_path: Path) -> None:
    pages = _multi_host_pages(6)
    created: list[_RecordingPool] = []
    result = _build(
        tmp_path,
        pages=pages,
        domain_profiles=_declared_multi_host_profiles(6),
        pool_factory=_recording_factory(created),
        crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=3, max_documents=2),
    )
    build = _build_of(result)

    assert build.doc_count == 2  # нет пере-удержания сверх ёмкости
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
        domain_profiles=_declared_multi_host_profiles(3),
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
        assert next(iter(store.iter_corpus_docs(RUN_ID))).content_hash == content_hash(
            b"retained bytes"
        )
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


# --------------------------------------------------------------------------- #
# F3/T3–T4 (план 17): артефакты не зависят от порядка завершения страниц
# --------------------------------------------------------------------------- #

#: Четыре страницы с **переставленными** задержками: порядок завершения фетчей второй
#: группы — обратный первой (шаг 0.02 с заметно больше дрожания планировщика).
_DELAYS_FORWARD = {
    "https://h0.docs.test/0": 0.02,
    "https://h1.docs.test/1": 0.04,
    "https://h2.docs.test/2": 0.06,
    "https://h3.docs.test/3": 0.08,
}
_DELAYS_REVERSED = {url: round(0.10 - delay, 2) for url, delay in _DELAYS_FORWARD.items()}


def _corpus_rows(store: SqliteStore) -> list[tuple[str, str, str, str]]:
    """``(product_id, doc_id, url, content_hash)`` из ``corpus_docs``, детерминированно.

    Именно этот кортеж — durable-факт корпуса (I19): он и обязан совпадать при любом
    порядке завершения страниц.
    """
    return sorted(
        (doc.product_id, doc.doc_id, doc.url, doc.content_hash)
        for doc in store.iter_corpus_docs(RUN_ID)
    )


def _dedup_winner(store: SqliteStore, content: bytes) -> str:
    """URL единственного документа с данным ``content_hash`` (I17: один на контент)."""
    winners = [
        doc.url
        for doc in store.iter_corpus_docs(RUN_ID)
        if doc.content_hash == content_hash(content)
    ]
    assert len(winners) == 1, winners
    return winners[0]


def _dedup_pages() -> dict[str, bytes]:
    """Страницы T3: два URL с одинаковыми байтами (дубликат) + два разных."""
    dup = b"<html>same</html>"
    links = "".join(
        f'<a href="https://h{i}.docs.test/{i}">{i}</a>' for i in range(4)
    )
    return {
        ROOT: f"<html>{links}</html>".encode(),
        "https://h0.docs.test/0": dup,
        "https://h1.docs.test/1": dup,
        "https://h2.docs.test/2": b"<html>two</html>",
        "https://h3.docs.test/3": b"<html>three</html>",
    }


def test_corpus_is_identical_for_any_concurrency(tmp_path: Path) -> None:
    """T3: конкурентность 1/2/4 при переставленных задержках даёт один и тот же корпус.

    Порядок завершения фетчей наблюдаемо разный (``_Crawler.completed``), а набор
    ``(product_id, doc_id, url, content_hash)`` и победитель контент-дедупа — одни и те
    же. Проверяется именно независимость от **момента готовности**: порядок обхода задан
    ключом ``(depth, url)`` (F3/A1), поэтому задержки сети его не переставляют.

    Победитель дедупа здесь утверждается как «тот же во всех прогонах», а не как
    ``min(url)``: выбор канонического URL — предмет F3B; этот тест доказывает лишь, что
    он не зависит от порядка прихода.

    Вторая половина (F4b, план 17 §6) — та же независимость по оси
    ``crawl.host_concurrency``: три корня на разных host'ах, ``host_concurrency``
    1/2/3. Один корень полос не создаёт, поэтому ось бессмысленно проверять на
    одиночном корне.
    """
    pages = _dedup_pages()
    runs: dict[str, Any] = {}
    stores: list[SqliteStore] = []
    try:
        for label, workers, delays in (
            ("k1", 1, _DELAYS_FORWARD),
            ("k2", 2, _DELAYS_REVERSED),
            ("k4", 4, _DELAYS_FORWARD),
        ):
            store = SqliteStore(":memory:")
            stores.append(store)
            _make_run(store)
            crawler = _Crawler(pages, delays=delays)
            runs[label] = _build(
                tmp_path / label,
                pages=pages,
                crawler=crawler,
                domain_profiles=_declared_multi_host_profiles(4),
                crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=workers),
                store=store,
                run_id=RUN_ID,
            )
            runs[f"{label}_crawler"] = crawler

        # Предусловие теста: порядок **завершения** действительно перемешан, иначе
        # совпадение корпуса ничего не доказывало бы.
        assert runs["k2_crawler"].completed != runs["k4_crawler"].completed
        assert runs["k4_crawler"].completed[1:] == [
            "https://h0.docs.test/0",
            "https://h1.docs.test/1",
            "https://h2.docs.test/2",
            "https://h3.docs.test/3",
        ]
        # K=2 идёт двумя подволнами, поэтому порядок завершения внутри подволны
        # обратный задержке: главное — он **не** совпадает с порядком отправки.
        assert runs["k2_crawler"].completed[1:] != runs["k4_crawler"].completed[1:]
        assert set(runs["k2_crawler"].completed) == set(runs["k4_crawler"].completed)

        k1, k2, k4 = (_corpus_rows(store) for store in stores)
        assert k1 == k2 == k4
        assert len(k1) == 4  # seed + 3 документа: два URL с одинаковыми байтами дали один
        assert runs["k1"].errors == runs["k2"].errors == runs["k4"].errors == []

        winners = {_dedup_winner(store, b"<html>same</html>") for store in stores}
        assert len(winners) == 1  # победитель дедупа не зависит от порядка прихода
    finally:
        for store in stores:
            store.close()

    # --- F4b (план 17 §6): та же независимость по оси ``host_concurrency`` ------ #
    # Один корень полос не создаёт (носитель состояния — корень), поэтому ось
    # проверяется на **нескольких** корнях продукта: три host'а, у каждого свои дети,
    # а ``r0/a`` и ``r1/a`` — один и тот же текст (дедуп **через** корни, не внутри).
    lane_roots = [f"https://r{i}.docs.test/" for i in range(3)]
    lane_dup = b"<html>dup</html>"
    lane_pages: dict[str, bytes] = {
        lane_roots[0]: b'<html><a href="/a">a</a><a href="/b">b</a></html>',
        lane_roots[1]: b'<html><a href="/a">a</a><a href="/c">c</a></html>',
        lane_roots[2]: b'<html><a href="/d">d</a><a href="/e">e</a></html>',
        "https://r0.docs.test/a": lane_dup,
        "https://r1.docs.test/a": lane_dup,
        "https://r0.docs.test/b": b"<html>b</html>",
        "https://r1.docs.test/c": b"<html>c</html>",
        "https://r2.docs.test/d": b"<html>d</html>",
        "https://r2.docs.test/e": b"<html>e</html>",
    }
    lane_stores: list[SqliteStore] = []
    lane_runs: dict[str, Any] = {}
    lane_crawlers: dict[str, _TimedCrawler] = {}
    try:
        for label, lanes in (("hc1", 1), ("hc2", 2), ("hc3", 3)):
            store = SqliteStore(":memory:")
            lane_stores.append(store)
            _make_run(store)
            # Гейт на 3: в hc=1 одновременно не бывает трёх фетчей (полоса одна,
            # пул шириной 2), поэтому гейт открывается только при чередовании.
            # Гейтятся только seed-корни: встреча корней — предмет проверки, а
            # волны детей платили бы таймаут впустую.
            crawler = _TimedCrawler(
                lane_pages,
                gate_to=3,
                gate_timeout=0.25,
                gate_urls=set(lane_roots),
            )
            lane_crawlers[label] = crawler
            lane_runs[label] = _build(
                tmp_path / label,
                pages=lane_pages,
                crawler=crawler,
                urls={"p1": lane_roots},
                crawl_defaults=CrawlRunConfig(
                    max_depth=1, max_workers=2, host_concurrency=lanes
                ),
                store=store,
                run_id=RUN_ID,
            )

        # Предусловие: ось действительно работает. Один корень даёт не более 2
        # одновременных фетчей (ширина пула), три полосы — три host'а сразу.
        assert lane_crawlers["hc1"].max_active <= 2
        assert lane_crawlers["hc3"].max_active >= 3

        lane_rows = [_corpus_rows(store) for store in lane_stores]
        assert lane_rows[0] == lane_rows[1] == lane_rows[2]
        assert len(lane_rows[0]) == 8  # 3 корня + 6 детей − 1 дубликат
        assert all(run.errors == [] for run in lane_runs.values())

        lane_winners = {_dedup_winner(store, lane_dup) for store in lane_stores}
        assert len(lane_winners) == 1
        assert lane_winners.pop() == "https://r0.docs.test/a"  # ``min(url)``, F3

        # Провенанс корней — в **авторском** порядке при любой полосности: слот
        # занят при планировании (F4b), а не при завершении корня.
        for run in lane_runs.values():
            records = _build_of(run).truncation
            assert [rec["root"] for rec in records] == lane_roots
            assert all(rec.get("planned") is not True for rec in records)
    finally:
        for store in lane_stores:
            store.close()


def test_truncation_is_independent_of_arrival_order(tmp_path: Path) -> None:
    """T4: при ``max_documents`` меньше достижимого набор добытых URL одинаков.

    Бюджет решается **на изъятии** (F3/A2), а изъятие идёт по ключу ``(depth, url)``,
    поэтому при переставленных задержках добываются одни и те же страницы: минимальные
    ``(depth, url)``. Усечение видно (``truncated``/``queue_remaining``), а не тихо.

    Оговорка: пул фетча коммитит исходы в порядке **отправки**, поэтому данная
    реализация уже не делает порядко-зависимости; тест пинит свойство, на которое
    опираются T3 и будущие F4-полосы (коммит по завершению), а не только сегодняшний
    код.
    """
    links = [f"https://h{i}.docs.test/{i}" for i in range(6)]
    pages: dict[str, bytes] = {
        ROOT: "".join(f'<a href="{url}">{url}</a>' for url in links).encode(),
        **{url: f"<html>page-{url}</html>".encode() for url in links},
    }
    delays = {url: 0.02 + 0.01 * index for index, url in enumerate(links)}

    fetched: list[set[str]] = []
    remaining: list[int] = []
    stores: list[SqliteStore] = []
    try:
        for label, delay_map in (("forward", delays), ("reversed", {u: round(0.10 - d, 2) for u, d in delays.items()})):
            store = SqliteStore(":memory:")
            stores.append(store)
            _make_run(store)
            result = _build(
                tmp_path / label,
                pages=pages,
                crawler=_Crawler(pages, delays=delay_map),
                domain_profiles=_declared_multi_host_profiles(6),
                crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=4, max_documents=3),
                store=store,
                run_id=RUN_ID,
            )
            build = _build_of(result)
            assert build.truncated is True
            assert build.truncation[0]["truncated_reason"] == "max_documents"
            assert result.errors == []
            fetched.append({url for _pid, _doc, url, _hash in _corpus_rows(store)})
            remaining.append(int(build.truncation[0]["queue_remaining"]))

        assert fetched[0] == fetched[1]
        # Ровно потолок и ровно минимальные по ``(depth, url)``: seed, затем h0/h1.
        assert fetched[0] == {ROOT, links[0], links[1]}
        # «Осталось N» опубликовано числом и тоже не зависит от порядка прихода.
        assert remaining[0] == remaining[1] == 4
    finally:
        for store in stores:
            store.close()


# --------------------------------------------------------------------------- #
# F4b (план 17 §6): полосы — право на host, а не объект
# --------------------------------------------------------------------------- #


def _same_host_roots(n: int = 2) -> list[str]:
    return [f"https://same.docs.test/root{i}" for i in range(n)]


def _cross_host_roots(n: int = 3) -> list[str]:
    return [f"https://h{i}.docs.test/" for i in range(n)]


def test_lane_serializes_within_host(tmp_path: Path) -> None:
    """T1: два корня **одного** host не идут одновременно и держат минимум интервала.

    ``host_concurrency=2`` объявлен, но host один: право на него выдаётся волне, а
    волна живёт до коммита, поэтому второй корень припаркован. Проверяется не «код
    вызвал claim», а наблюдаемое: пик одновременных фетчей этого host'а = 1 и
    фактический интервал между запросами не меньше объявленного минимума
    (``rate_limit``). Гейт ``gate_to=2`` делает нарушение детерминированным: при
    параллельных корнях двух фетчей хватило бы, чтобы гейт открылся сразу.
    """
    roots = _same_host_roots(2)
    pages = {root: f"<html>root-{i}</html>".encode() for i, root in enumerate(roots)}
    crawler = _TimedCrawler(pages, gate_to=2, gate_timeout=0.2)

    result = _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        urls={"p1": roots},
        crawl_defaults=CrawlRunConfig(
            max_depth=0,
            max_workers=2,
            host_concurrency=2,
            rate_limit=RateLimitSpec(min_s=0.06, max_s=0.06),
        ),
    )

    assert result.errors == []
    assert result.status == "ok"
    assert crawler.host_max_active["same.docs.test"] == 1
    assert crawler.max_active == 1
    assert len(crawler.started_at) == 2
    starts = sorted(moment for _url, moment in crawler.started_at)
    # Минимум интервала — объявленный ``rate_limit.min_s`` (0.06 с); допуск 0.01 с
    # покрывает планировщик, но не «почти одновременно».
    assert starts[1] - starts[0] >= 0.05


def test_lanes_run_concurrently_across_hosts(tmp_path: Path) -> None:
    """T2: три host'а идут полосами — критический путь, а не сумма.

    Три корня на разных host'ах, ``host_concurrency=3``: все три волны допущены
    сразу (claim'ы не пересекаются). Наблюдаемое — пик 3 одновременных фетча и
    укладывание в критический путь: гейт ``gate_to=3`` отпускает только когда все
    три в полёте, поэтому при последовательной работе прогон стоил бы
    ``3 × gate_timeout`` (1.2 с), а при полосах — один гейт.
    """
    roots = _cross_host_roots(3)
    pages = {root: f"<html>root-{i}</html>".encode() for i, root in enumerate(roots)}
    crawler = _TimedCrawler(pages, gate_to=3, gate_timeout=0.4)

    started = time.monotonic()
    result = _build(
        tmp_path,
        pages=pages,
        crawler=crawler,
        urls={"p1": roots},
        crawl_defaults=CrawlRunConfig(
            max_depth=0,
            max_workers=2,
            host_concurrency=3,
            rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0),
        ),
    )
    elapsed = time.monotonic() - started

    assert result.errors == []
    assert result.status == "ok"
    # Все три полосы были в сети одновременно и каждая — одна волна на host.
    assert crawler.max_active == 3
    assert set(crawler.host_max_active) == {"h0.docs.test", "h1.docs.test", "h2.docs.test"}
    assert all(peak == 1 for peak in crawler.host_max_active.values())
    # Критический путь ≈ один гейт, а не сумма: последовательный путь — 3 × 0.4 с.
    assert elapsed < 1.0


def test_state_is_written_only_by_driver(tmp_path: Path) -> None:
    """T5: состояние фазы пишет только поток драйвера (thread-affinity — инвариант).

    Стор обёрнут ``_ThreadGuardStore``: мутирующий вызов из чужого потока падает
    ``AssertionError``. Сначала доказывается, что обёртка **жива** (запись из чужого
    потока действительно отвергается), иначе «нарушений не было» ничего не значило
    бы. Затем полосами (3 host'а, ``max_workers=2``) собирается корпус, и проверяется,
    что все записи прошли на потоке владельца, а чтения с чужих потоков отсутствуют.
    """
    roots = _cross_host_roots(3)
    pages: dict[str, bytes] = {}
    for i, root in enumerate(roots):
        child = f"https://h{i}.docs.test/a"
        pages[root] = b'<html><a href="/a">a</a></html>'
        pages[child] = f"<html>child-{i}</html>".encode()

    store = SqliteStore(":memory:")
    _make_run(store)
    guarded = _ThreadGuardStore(store)
    try:
        # Предусловие: обёртка падает на мутации из чужого потока.
        foreign_errors: list[BaseException] = []

        def _foreign_write() -> None:
            try:
                guarded.upsert_corpus_doc(None)
            except BaseException as exc:  # noqa: BLE001 — проверяется именно отказ
                foreign_errors.append(exc)

        worker = threading.Thread(target=_foreign_write, name="foreign-writer")
        worker.start()
        worker.join()
        assert len(foreign_errors) == 1
        assert isinstance(foreign_errors[0], AssertionError)

        result = _build(
            tmp_path,
            pages=pages,
            crawler=_TimedCrawler(pages),
            urls={"p1": roots},
            store=guarded,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(
                max_depth=1, max_workers=2, host_concurrency=3
            ),
        )

        assert result.errors == []
        assert result.status == "ok"
        assert guarded.writes > 0  # записи были — проверка не вакуумная
        assert guarded.write_threads == {guarded.owner}
        assert guarded.read_threads == set()
        # Единственный «чужой» вызов — предусловие выше; в прогоне их нет.
        assert guarded.foreign == ["upsert_corpus_doc@foreign-writer"]
    finally:
        store.close()


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело или retained-файл (потребитель очереди не держит тело в RAM).

    До F5 конверсия шла внутри краула, и ``RawPage.content`` страницы был непуст. Теперь
    документ конвертируется из строки очереди: ``content`` пуст, тело лежит по
    ``source_path`` (I19, ``corpus_raw/{hash}.bin``) — ровно так его получает воркер пула.
    Фейковый экстрактор обязан читать тот же носитель, что и адаптер, иначе он проверял бы
    не контракт порта, а прежний способ доставки байтов.
    """
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""


# --------------------------------------------------------------------------- #
# (7) Прибор темпа: внутрихостовая конкурентность видна числом, а не молчанием
# --------------------------------------------------------------------------- #


def test_within_host_parallel_fetches_are_seen_by_pacing_counter(
    tmp_path: Path,
) -> None:
    """Один корень, два ребёнка на том же host'е, ``K=2``, без паузы ⇒ пик = 2.

    Это пин внутрихостовой конкурентности: пул шириной 2 обязан отправить двух детей
    одного host'а одновременно. До прибора понижение до одного не покраснело бы ни
    один тест — корпус, ``index_build_id`` и порядок коммита от этого не меняются, а
    темп падает вдвое. Пик читается из **durable**-провенанса сеанса: то же число
    публикуют ``hound crawl status --per-host`` и ``metrics.json.resources.crawl``.
    """
    children = [f"{ROOT}{name}" for name in ("a", "b")]
    pages: dict[str, bytes] = {
        ROOT: "".join(f'<a href="{url}">{url}</a>' for url in children).encode()
    }
    pages.update({url: f"<html>{url}</html>".encode() for url in children})
    # Гейт только на детей: волна 1 — корень (один), волна 2 — оба ребёнка. Без гейта
    # перекрытие зависело бы от планировщика, и тест мог бы «не увидеть» пик случайно.
    crawler = _TimedCrawler(pages, gate_to=2, gate_timeout=0.5, gate_urls=set(children))
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages=pages,
            crawler=crawler,
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(
                max_depth=1,
                max_workers=2,
                rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0),
            ),
        )
        assert result.errors == []
        # Предпосылка теста: волна действительно была шире одного запроса.
        assert crawler.host_max_active["docs.test"] == 2

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        pacing = read_provenance(session)["pacing"]
        host = pacing["hosts"]["docs.test"]
        assert host["requests"] == 3  # корень + два ребёнка
        assert host["in_flight_max"] == 2
        assert host["requests_per_s"] is not None and host["requests_per_s"] > 0.0
        # Паузы не объявлено — ожидания не было (а не «не измеряли»).
        assert host["limiter_waits"] == 0
        assert pacing["total"]["in_flight_max"] == 2
        assert pacing["products"]["p1"]["requests"] == 3
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# F9 (план 17): когорта — продукт как граница **коммита**, а не взаимного исключения
# --------------------------------------------------------------------------- #

#: Два продукта когорты: по одному корню с одним ребёнком. Корни — на разных host'ах
#: (волны могут идти вместе) или на одном (право на host держится).
_COHORT_ROOTS_SHARED = ("https://same.docs.test/one", "https://same.docs.test/two")
_COHORT_ROOTS_DISTINCT = ("https://p1.docs.test/", "https://p2.docs.test/")


def _cohort_pages(roots: tuple[str, str]) -> dict[str, bytes]:
    """Страницы двух продуктов: корень со ссылкой на ребёнка **внутри** своего корня."""
    pages: dict[str, bytes] = {}
    for index, root in enumerate(roots):
        child = f"{root.rstrip('/')}/child{index}"
        pages[root] = f'<html><a href="{child}">c</a></html>'.encode()
        pages[child] = f"<html>child-{index}</html>".encode()
    return pages


def _cohort_build(
    tmp_path: Path,
    *,
    roots: tuple[str, str],
    crawler: _TimedCrawler,
    host_concurrency: int | None,
) -> Any:
    """Сборка корпуса когорты из двух продуктов (план и строка прогона — из ``test_corpus``).

    Идентификатор прогона берётся **тот же**, что у плана: строка ``runs`` обязана
    существовать до открытия сеанса (FK), иначе фаза сообщит ``IntegrityError`` вместо
    корпуса — и тест проверял бы не то, что назвал.
    """
    from test_corpus import RUN_ID as RUN_ID_TWO
    from test_corpus import _build as _build_two
    from test_corpus import _make_run as _make_run_two

    store = SqliteStore(":memory:")
    _make_run_two(store, RUN_ID_TWO)
    try:
        result = _build_two(
            tmp_path,
            pages=_cohort_pages(roots),
            crawler=crawler,
            urls={"p1": [roots[0]], "p2": [roots[1]]},
            store=store,
            run_id=RUN_ID_TWO,
            crawl_defaults=CrawlRunConfig(
                max_depth=1, max_workers=2, host_concurrency=host_concurrency
            ),
        )
    finally:
        store.close()
    return result


def _product_events(result: Any) -> list[tuple[str, str]]:
    """Границы продуктов в порядке появления: ``(event, product_id)``."""
    return [
        (str(entry["event"]), str(entry["product_id"]))
        for entry in result.log_entries
        if isinstance(entry, dict)
        and entry.get("event") in {"product_start", "product_end"}
    ]


def test_cohort_waves_of_two_products_overlap_on_distinct_hosts(tmp_path: Path) -> None:
    """F9: два продукта идут полосами **одновременно**, если их host'ы разные.

    Это и есть цель когортного планирования: до F9 ``drive_roots`` вызывался внутри цикла
    продуктов, поэтому волна не могла содержать корень другого продукта и окна
    ``fetch_started_at`` продуктов не пересекались ни при какой оси. Здесь гейт отпускает
    фетч только при **двух** одновременных, и гейтованы ровно корни разных продуктов —
    значит корни двух продуктов были в сети вместе.

    Второй половиной проверяется новый контракт границ: ``start``/``end`` продукта больше
    не обязаны быть вложенными парами — продукт ``p2`` начинается, пока ``p1`` не закрыт
    (закрытие — граница коммита, а не запрет на работу соседа).
    """
    roots = _COHORT_ROOTS_DISTINCT
    crawler = _TimedCrawler(
        _cohort_pages(roots), gate_to=2, gate_timeout=0.25, gate_urls=set(roots)
    )

    result = _cohort_build(
        tmp_path, roots=roots, crawler=crawler, host_concurrency=2
    )

    assert result.errors == []
    assert result.status == "ok"
    # Предусловие теста: оба корня действительно фетчились (иначе «пересечение» ложно).
    assert set(roots) <= set(crawler.calls)
    # Пик одновременных фетчей ≥ 2 при гейте на двух корнях разных продуктов ⇒
    # волны обоих продуктов были в полёте одновременно.
    assert crawler.max_active >= 2

    events = _product_events(result)
    assert ("product_start", "p2") in events and ("product_end", "p1") in events
    # Новый контракт: p2 открыт до того, как p1 закрыт.
    assert events.index(("product_start", "p2")) < events.index(("product_end", "p1"))
    # При этом у каждого продукта его собственный start предшествует его end.
    for product_id in ("p1", "p2"):
        assert events.index(("product_start", product_id)) < events.index(
            ("product_end", product_id)
        )


def test_cohort_keeps_host_claim_across_products(tmp_path: Path) -> None:
    """F9: общий host у двух продуктов — право на host держится и между продуктами.

    Ось объявлена (``host_concurrency=2``), но host один: волна второго продукта
    припаркована до коммита первой. Гейт на двух одновременных фетчах при этом не
    открывается вовсе, поэтому наблюдённый пик — 1. Без общего реестра права (если бы
    каждый продукт вёл свой) гейт открылся бы сразу: это и есть проверяемый дефект.
    """
    roots = _COHORT_ROOTS_SHARED
    crawler = _TimedCrawler(
        _cohort_pages(roots), gate_to=2, gate_timeout=0.2, gate_urls=set(roots)
    )

    result = _cohort_build(
        tmp_path, roots=roots, crawler=crawler, host_concurrency=2
    )

    assert result.errors == []
    assert result.status == "ok"
    assert set(roots) <= set(crawler.calls)
    assert crawler.host_max_active["same.docs.test"] == 1
    assert crawler.max_active == 1


def test_cohort_without_axis_stays_sequential(tmp_path: Path) -> None:
    """F9: ``crawl.host_concurrency`` не объявлена ⇒ последовательность прежняя.

    Предохранитель когортного планирования: без оси один вызов драйвера обязан вести себя
    как прежний цикл — продукт целиком, затем следующий. Проверяется наблюдаемое: границы
    продуктов идут парами, корни не встречаются, пик фетчей — 1.
    """
    roots = _COHORT_ROOTS_DISTINCT
    crawler = _TimedCrawler(
        _cohort_pages(roots), gate_to=2, gate_timeout=0.2, gate_urls=set(roots)
    )

    result = _cohort_build(tmp_path, roots=roots, crawler=crawler, host_concurrency=None)

    assert result.errors == []
    assert result.status == "ok"
    assert crawler.max_active == 1
    assert _product_events(result) == [
        ("product_start", "p1"),
        ("product_end", "p1"),
        ("product_start", "p2"),
        ("product_end", "p2"),
    ]


# --------------------------------------------------------------------------- #
# F9: потолок полос — продуктовый, когортный потолок не выдаётся «за компанию»
# --------------------------------------------------------------------------- #

#: Два продукта когорты разной ширины: узкому объявлена одна полоса, широкому — три.
#: Когортный потолок (три) принадлежит **когорте**; продуктовый приходит из
#: ``lane_limit_for``, потому что ось объявлена на корень (``app < run < site``).
_COHORT_NARROW = "p-narrow"
_COHORT_WIDE = "p-wide"


class _ProductGate:
    """``local_fetch`` с гейтом: держит вызовы до ``to`` одновременных и считает пик по продукту.

    Гейт делает одновременность наблюдаемой без сверки последовательностей задержек:
    вызов не возвращается, пока активных не станет ``to`` (или не истечёт ``timeout`` —
    тогда одинокая полоса просто едет дальше), и держится ещё ``hold`` секунд, чтобы
    третья волна успела войти **до** выхода второй: иначе пик мерил бы порядок
    планировщика потоков, а не допуск драйвера. Пик считается **раздельно по**
    ``desc.pid``: вопрос теста — «сколько полос держал продукт», а не «сколько было в
    когорте», и общий пик это второй, отдельный факт.
    """

    def __init__(self, *, to: int, timeout: float, hold: float = 0.05) -> None:
        self._to = to
        self._timeout = timeout
        self._hold = hold
        self._lock = threading.Lock()
        self._active = 0
        self._active_by_pid: dict[str, int] = {}
        self.max_active = 0
        self.max_active_by_pid: dict[str, int] = {}
        self.calls: list[str] = []

    def __call__(self, desc: Any) -> RawPage:
        pid = str(desc.pid)
        with self._lock:
            self.calls.append(desc.wu)
            self._active += 1
            self._active_by_pid[pid] = self._active_by_pid.get(pid, 0) + 1
            self.max_active = max(self.max_active, self._active)
            self.max_active_by_pid[pid] = max(
                self.max_active_by_pid.get(pid, 0), self._active_by_pid[pid]
            )
        try:
            deadline = time.monotonic() + self._timeout
            while time.monotonic() < deadline:
                with self._lock:
                    if self._active >= self._to:
                        break
                time.sleep(0.001)
            # Держим окно открытым: пик по продукту — это «сколько волн допущено
            # одновременно», и мгновенный выход сделал бы его функцией планировщика.
            time.sleep(self._hold)
            return RawPage(
                url=f"https://{desc.host}/",
                content=b"x",
                fetched_at=TS,
                fetch_mode=desc.fetch_plan.fetch,
                lang="en",
            )
        finally:
            with self._lock:
                self._active -= 1
                self._active_by_pid[pid] -= 1


def _lane_plan(root_url: str, product_id: str) -> corpus_drain.RootPlan:
    """План корня для драйвера: цель и sitemap не нужны — волну строит фейковый ``begin``."""
    return corpus_drain.RootPlan(
        root_url=root_url, target=None, sitemap_seeds=False, product_id=product_id
    )


def _one_host_begin(
    hosts_by_root: dict[str, str],
) -> Any:
    """``begin=`` драйвера: корень — **одна** волна из одного фетча, затем конец.

    Настоящий ``root_lifecycle`` здесь не нужен: драйвер спрашивается только о том,
    сколько волн **продукта** он допустил одновременно, а не о том, что сделал корень.
    """

    def begin(plan: Any, *, fetch_pool: Any = None, browser_pool: Any = None) -> Any:
        host = hosts_by_root[plan.root_url]
        url = f"https://{host}/"
        entry = corpus_drain._WaveEntry(
            pid=str(plan.product_id),
            wu=url,
            depth=0,
            kind="fetch",
            desc=LocalFetch(
                pid=str(plan.product_id),
                wu=url,
                depth=0,
                fetch_plan=FetchPlan(),
                active=None,
                engine_value=None,
                prior_item=None,
                resume=None,
                browser_blocked=False,
                poolable=True,
                host=host,
                scope_host=None,
                enforce_redirect_scope=False,
            ),
        )
        yield corpus_drain.WavePlan(entries=[entry], hosts=frozenset({host}))
        return (None, [])

    return begin


def test_cohort_product_lane_limits_are_not_shared() -> None:
    """F9: потолок полос — **продуктовый**; когортный потолок не выдаётся «за компанию».

    Ось объявлена на корень, поэтому у продукта со строгим объявлением полос меньше, чем
    у когорты; ``lane_limit_for`` обязан вернуть потолок **этого** продукта. Проверяются
    обе стороны одной ошибки: узкий продукт не расширяется до когортного потолка
    (иначе лимит продукта игнорируется) и широкий не душится до узкого (иначе
    продуктовый потолок стал бы общим). Наблюдаемое — пик одновременных фетчей **по
    продукту**; общий пик — отдельное утверждение о потолке когорты.
    """
    narrow_roots = ("https://n1.test/", "https://n2.test/")
    wide_roots = ("https://w1.test/", "https://w2.test/", "https://w3.test/")
    hosts = {
        narrow_roots[0]: "n1.test",
        narrow_roots[1]: "n2.test",
        wide_roots[0]: "w1.test",
        wide_roots[1]: "w2.test",
        wide_roots[2]: "w3.test",
    }
    # Узкие корни идут **подряд и первыми**: это ставит их в одну волну допуска, поэтому
    # испорченный лимит («всем потолок когорты») сразу даёт узкому продукту пик 2. Широкие
    # идут следом: их пик достижим, только если продуктовый лимит не стал общим.
    plans = [
        _lane_plan(narrow_roots[0], _COHORT_NARROW),
        _lane_plan(narrow_roots[1], _COHORT_NARROW),
        _lane_plan(wide_roots[0], _COHORT_WIDE),
        _lane_plan(wide_roots[1], _COHORT_WIDE),
        _lane_plan(wide_roots[2], _COHORT_WIDE),
    ]
    gate = _ProductGate(to=2, timeout=0.25)
    timings: dict[str, Any] = {}
    errors: list[str] = []

    outcomes = corpus_driver.drive_roots(
        plans,
        host_concurrency=3,
        begin=_one_host_begin(hosts),
        lane_width=lambda plan: 2,
        local_fetch=gate,
        timings_for=lambda pid: timings.setdefault(str(pid), _CorpusStageTimings()),
        errors=errors,
        lane_limit_for=lambda plan: 1 if plan.product_id == _COHORT_NARROW else 3,
        label=" теста",
    )

    # Предусловие: работа дошла до конца — иначе пики объяснялись бы незавершённостью.
    assert errors == []
    assert len(outcomes) == len(plans)
    assert len(gate.calls) == len(plans)
    # Узкий продукт держал ровно одну полосу из трёх возможных у когорты.
    assert gate.max_active_by_pid[_COHORT_NARROW] == 1
    # Широкий не душится до узкого значения: две полосы наблюдались одновременно.
    assert gate.max_active_by_pid[_COHORT_WIDE] >= 2
    # И потолок когорты при этом не превышен: он — третье, отдельное число.
    assert gate.max_active <= 3


# --------------------------------------------------------------------------- #
# F4b: цикл ожидания не крутится на уже готовых future'ах
# --------------------------------------------------------------------------- #


def test_wait_receives_only_pending_futures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Регресс: в ``wait`` не попадают уже завершённые future'ы.

    ``concurrent.futures.wait(..., FIRST_COMPLETED)`` возвращается немедленно, если в
    наборе есть готовый future, а волна сбрасывается только когда готовы **все** её
    записи. Пока одна запись волны завершилась, а другая ещё в полёте, набор оставался
    непустым и «готовым», и драйвер крутился в цикле без сна. Профиль py-spy на живом
    прогоне 02.11.2026: 97 % одного ядра внутри этого цикла при темпе, который задавал
    пейсинг (0.9 с CPU на документ при работе, которая стоит миллисекунды).

    Проверяется наблюдаемое самого цикла: набор, переданный в ``wait``, обязан состоять
    только из незавершённых future'ов, а число вызовов — не расти с длиной окна
    рассинхронизации записей волны.
    """
    hosts = {"https://slow.test/": "slow.test"}

    def begin(plan: Any, *, fetch_pool: Any = None, browser_pool: Any = None) -> Any:
        host = hosts[plan.root_url]
        entries = [
            corpus_drain._WaveEntry(
                pid="p1",
                wu=f"https://{host}/{kind}",
                depth=0,
                kind="fetch",
                desc=LocalFetch(
                    pid="p1",
                    wu=f"https://{host}/{kind}",
                    depth=0,
                    fetch_plan=FetchPlan(),
                    active=None,
                    engine_value=None,
                    prior_item=None,
                    resume=None,
                    browser_blocked=False,
                    poolable=True,
                    host=host,
                    scope_host=None,
                    enforce_redirect_scope=False,
                ),
            )
            for kind in ("fast", "slow")
        ]
        yield corpus_drain.WavePlan(entries=entries, hosts=frozenset({host}))
        return (None, [])

    window = 0.05

    def fetch(desc: Any) -> RawPage:
        # Рассинхронизация внутри одной волны: первая запись готова сразу, вторая —
        # через ``window``. Именно этот зазор старый цикл прожигал.
        if desc.wu.endswith("/slow"):
            time.sleep(window)
        return RawPage(url=desc.wu, content=b"x", fetched_at=TS)

    real_wait = corpus_driver.wait
    passed_done: list[int] = []

    def spy(futures_to_wait: Any, **kwargs: Any) -> Any:
        items = list(futures_to_wait)
        passed_done.append(sum(1 for future in items if future.done()))
        return real_wait(items, **kwargs)

    monkeypatch.setattr(corpus_driver, "wait", spy)

    errors: list[str] = []
    timings: dict[str, Any] = {}
    outcomes = corpus_driver.drive_roots(
        [_lane_plan("https://slow.test/", "p1")],
        host_concurrency=1,
        begin=begin,
        lane_width=lambda plan: 2,
        local_fetch=fetch,
        timings_for=lambda pid: timings.setdefault(str(pid), _CorpusStageTimings()),
        errors=errors,
        label=" теста",
    )

    assert errors == []
    assert len(outcomes) == 1
    # Ни одного вызова с готовым future'ом: иначе ожидание — горячий простой.
    assert passed_done, "драйвер ни разу не ждал — сценарий не воспроизвёл окно"
    assert passed_done == [0] * len(passed_done), passed_done
    # И число вызовов не растёт с длиной окна (при 5 с — сотни итераций).
    assert len(passed_done) <= 5, len(passed_done)
