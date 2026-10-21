"""Тесты сборки корпуса Ш5 (W7/T7.4): retention, dedup, I17/I19, детерминизм, персистенция.

Порты подменяются детерминированными in-memory заглушками (без сети/I/O, кроме ``tmp_path``
для retention). Конкретные адаптеры (``core/adapters/*``) не импортируются — тестируется
только контракт портов (A1). Нейтральные фикстуры: ``p1/p2``, ``param_a``, ``mod_a``.
"""

from __future__ import annotations

import datetime as _dt
import inspect
import json
import random
from pathlib import Path
from typing import Any

import pytest

from core.adapters import index_format
from core.adapters.feed_poller import LocalFeedPoller
from core.adapters.index_npy import NpyIndexBuilder
from core.config.crawl_sources import CrawlSource, CrawlSourcesConfig
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    BLOCKING_FAILURE_KINDS,
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CorpusDoc,
    CrawlItemStatus,
    CrawlRunConfig,
    DocumentLevel,
    CrawlSessionItem,
    CrawlSessionStatus,
    CrawlStrategy,
    FailureKind,
    RateLimitSpec,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    IndexBuild,
    _SUBMIT_MAX_ATTEMPTS,
    _derive_partition_build_id,
    _product_content_token,
    _work_set_idempotency_key,
    build_corpus,
    derive_corpus_coverage,
)
from core.pipeline.crawl_frontier import CrawlLimits
from core.pipeline.crawl_session import default_session_id, truncation_summary
from core.pipeline.extract_cache import ExtractionCache
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
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.ports.state import UrlStateRow
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.feed import FeedEntry
from core.util.hashing import content_hash
from core.util.rate_limit import RateLimiter

try:  # необязательная зависимость (extra ``retrieval``)
    import numpy as _np  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - зависит от окружения
    _np = None  # type: ignore[assignment]

requires_numpy = pytest.mark.skipif(_np is None, reason="numpy не установлен (extra retrieval)")


class ImmutableBuildError(RuntimeError):
    """Локальное зеркало имени адаптерного типа: проверяем строку ошибки, а не класс (K2)."""

RUN_ID = "run-fixture"
#: Идентификатор сеанса краула по умолчанию (L1a): ``f"crawl:{run_id}"``.
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"

URL_P1_A = "https://docs.test/p1/index"
URL_P1_B = "https://docs.test/p1/print"
URL_P2_A = "https://docs.test/p2/index"


# --------------------------------------------------------------------------- #
# Нейтральные фикстуры/заглушки портов
# --------------------------------------------------------------------------- #


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan(products: tuple[str, ...] = ("p1", "p2")) -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
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
                ),
                CohortEntry(
                    product_id="p2",
                    role_overlap=0.5,
                    selection_reason="analog",
                    anchor_relation=AnchorRelation.ANALOG,
                ),
            ],
        ),
        golden=[],
    )


def _make_run(store: SqliteStore, run_id: str = RUN_ID) -> None:
    """Создаёт строку прогона (нужна для FK ``crawl_sessions.run_id`` при L1a-resume)."""
    store.create_run(
        run_id,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1", "p2"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


class _Crawler:
    """Краулер: ``url -> bytes``; опционально URL, бросающие исключение.

    ``engine`` — duck-typed имя транспорта (Phase 2.1/S1): ``None`` (дефолт) повторяет
    поведение адаптера без объявленного имени — провенанс запишет запрошенный
    ``target.engine``; заданное имя приоритетнее запрошенного.
    """

    version = "crawler-1"
    engine: str | None = None

    def __init__(
        self,
        pages: dict[str, bytes],
        *,
        raising: set[str] | None = None,
        engine: str | None = None,
    ) -> None:
        self.pages = dict(pages)
        self.raising = set(raising or ())
        self.calls: list[tuple[str, FetchPlan]] = []
        self.engine = engine

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append((url, plan))
        if url in self.raising:
            raise RuntimeError(f"crawl failed: {url}")
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
        )


class _NotFoundError(RuntimeError):
    """Сбой с машинным классом ``not_found`` (duck-typed контракт адаптеров).

    Тест pipeline не импортирует адаптеры: ``corpus._commit_local`` читает атрибут
    ``failure_kind`` через ``getattr`` — ровно как ``refusal_reason`` у robots-отказа.
    """

    failure_kind = FailureKind.NOT_FOUND.value


class _NotFoundCrawler(_Crawler):
    """Краулер, на каждом URL поднимающий ``not_found`` (HTTP 404/410)."""

    def __init__(self) -> None:
        super().__init__({})

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append((url, plan))
        raise _NotFoundError(f"HTTP 404: {url}")


class _RefusedError(RuntimeError):
    """Robots-отказ: ``refusal_reason`` вместо ``failure_kind`` (D1/§10)."""

    refusal_reason = "robots_disallow"


class _RefusedCrawler(_Crawler):
    """Краулер, на каждом URL поднимающий robots-отказ."""

    def __init__(self) -> None:
        super().__init__({})

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append((url, plan))
        raise _RefusedError(f"robots.txt запрещает краул {url!r}")


class _Extractor:
    """Экстрактор: текст — декодированные байты; ``error_urls`` → грациозная ошибка."""

    version = "extractor-1"

    def __init__(self, *, error_urls: set[str] | None = None) -> None:
        self.error_urls = set(error_urls or ())
        #: Ф1.2: сколько раз реально вызывался экстрактор (кэш-хит его не вызывает).
        self.calls = 0

    def extract(self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO) -> ExtractedDoc:
        self.calls += 1
        if page.url in self.error_urls:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(page.content),
                extractor_version=self.version,
                error="extraction failed",
            )
        text = page.content.decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(page.content),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    """Чанкер: один чанк на документ; ``empty_hash`` имитирует I19-нарушение."""

    version = "chunker-1"

    def __init__(self, *, empty_hash: bool = False) -> None:
        self.empty_hash = empty_hash

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="h1",
                context_prefix="ctx",
                content_hash="" if self.empty_hash else doc.content_hash,
            )
        ]


class _Embedder:
    """Эмбеддер: фиксированные вектора заданной размерности."""

    def __init__(self, model_id: str = "embed-x", dim: int = 3) -> None:
        self.model_id = model_id
        self.dim = dim
        self.texts: list[str] = []

    def embed(self, texts):  # noqa: ANN001 — структурная заглушка порта
        texts = list(texts)
        self.texts.extend(texts)
        return [[0.0, 0.1 * i, 0.2] for i in range(len(texts))]


class _IndexBuilder:
    """Сборщик индекса: фиксирует вызовы build/commit (единственный писатель, K3)."""

    lexical_engine_version = "lex-1"
    vector_store = "npy"

    def __init__(self) -> None:
        self.built: list[tuple[int, str]] = []
        self.commits = 0

    def build(self, chunks, vectors, meta):  # noqa: ANN001 — структурная заглушка порта
        self.built.append((len(list(chunks)), meta.product_id))
        return "builder-id"

    def commit(self) -> None:
        self.commits += 1


def _build(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    urls: dict[str, list[str]],
    store=None,  # noqa: ANN001
    crawler: _Crawler | None = None,
    extractor: _Extractor | None = None,
    chunker: _Chunker | None = None,
    **kwargs,
):
    return build_corpus(
        _plan(),
        crawler or _Crawler(pages),
        extractor or _Extractor(),
        chunker or _Chunker(),
        store,
        urls=urls,
        raw_root=tmp_path,
        **kwargs,
    )


def _build_of(result) -> IndexBuild:  # noqa: ANN001
    """Извлекает ``IndexBuild`` из ``PhaseResult.log_entries``."""
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


# --------------------------------------------------------------------------- #
# Сквозная сборка, retention, dedup
# --------------------------------------------------------------------------- #


def test_end_to_end_two_products(tmp_path: Path) -> None:
    pages = {URL_P1_A: b"page one", URL_P1_B: b"page one", URL_P2_A: b"page two"}
    result = _build(
        tmp_path,
        pages=pages,
        urls={"p1": [URL_P1_A, URL_P1_B], "p2": [URL_P2_A]},
    )
    assert result.status == "ok" and result.phase == "corpus"
    assert result.cells == []
    build = _build_of(result)
    # p1: две перепечатки → один документ; p2: один документ.
    assert build.chunk_count == 2
    assert {e.product_id for e in build.entries} == {"p1", "p2"}
    assert len(build.docs) == 2
    assert result.errors == []


def test_retention_file_written(tmp_path: Path) -> None:
    content = "retained bytes".encode()
    result = _build(tmp_path, pages={URL_P1_A: content}, urls={"p1": [URL_P1_A]})
    build = _build_of(result)
    doc = build.docs[0]
    expected = tmp_path / "p1" / f"{content_hash(content)}.bin"
    assert expected.exists()
    assert expected.read_bytes() == content
    assert doc.raw_path == str(expected)
    assert doc.content_hash == content_hash(content)
    assert doc.extractor_version == "extractor-1"


def test_dedup_same_content_two_urls_indexed_once(tmp_path: Path) -> None:
    crawler = _Crawler({URL_P1_A: b"same text", URL_P1_B: b"same text"})
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [URL_P1_A, URL_P1_B]},
        crawler=crawler,
    )
    build = _build_of(result)
    # Обе страницы скачаны (retention по каждой), но проиндексирован один документ.
    assert len(crawler.calls) == 2
    assert len(build.docs) == 1
    assert build.chunk_count == 1
    assert {e.product_id for e in build.entries} == {"p1"}


def test_dedup_is_product_scoped(tmp_path: Path) -> None:
    # Идентичный текст у разных продуктов обязан остаться в обеих партициях (I17).
    result = _build(
        tmp_path,
        pages={URL_P1_A: b"shared", URL_P2_A: b"shared"},
        urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
    )
    build = _build_of(result)
    assert build.chunk_count == 2
    assert {d.product_id for d in build.docs} == {"p1", "p2"}
    assert {e.product_id for e in build.entries} == {"p1", "p2"}


# --------------------------------------------------------------------------- #
# Детерминизм идентификаторов
# --------------------------------------------------------------------------- #


def test_chunk_id_is_deterministic(tmp_path: Path) -> None:
    pages = {URL_P1_A: b"chunk me", URL_P2_A: b"other"}
    urls = {"p1": [URL_P1_A], "p2": [URL_P2_A]}

    def _ids(root: Path) -> list[str]:
        store = SqliteStore(":memory:")
        try:
            _build(root, pages=pages, urls=urls, store=store)
            return sorted(
                row["chunk_id"]
                for row in store.conn.execute("SELECT chunk_id FROM corpus_chunks")
            )
        finally:
            store.close()

    first = _ids(tmp_path / "a")
    second = _ids(tmp_path / "b")
    assert first == second
    # Формат: product:doc_hash:chunk_no — глобально уникальный.
    doc_hash = content_hash(b"chunk me")
    assert f"p1:{doc_hash}:0" in first


def test_chunk_content_hash_from_text_and_build_id(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"hash me"},
            urls={"p1": [URL_P1_A]},
            store=store,
        )
        build = _build_of(result)
        rows = list(
            store.conn.execute(
                "SELECT chunk_id, text, content_hash, extractor_version, build_id"
                " FROM corpus_chunks"
            )
        )
        assert rows, "чанки обязаны быть записаны"
        for row in rows:
            assert row["content_hash"] == content_hash(row["text"])
            assert row["content_hash"]  # I19
            assert row["build_id"] == build.entries[0].partition_build_id
            # P2: версия экстрактора чанка персистится (re-extract-провенанс, §3.2).
            assert row["extractor_version"] == "extractor-1"
    finally:
        store.close()


def test_index_build_id_same_content_deterministic(tmp_path: Path) -> None:
    """Те же входы и то же содержимое → один id (детерминизм, §B.1/§13)."""
    pages = {URL_P1_A: b"doc", URL_P2_A: b"doc-two"}
    urls = {"p1": [URL_P1_A], "p2": [URL_P2_A]}
    base = _build_of(_build(tmp_path / "a", pages=pages, urls=urls)).index_build_id
    again = _build_of(_build(tmp_path / "b", pages=pages, urls=urls)).index_build_id
    assert base == again


def test_index_build_id_collision_regression(tmp_path: Path) -> None:
    """P2: одинаковые декларированные входы + разный контент ⇒ РАЗНЫЙ id (коллизия)."""
    urls = {"p1": [URL_P1_A]}
    first = _build_of(
        _build(tmp_path / "a", pages={URL_P1_A: b"content-a"}, urls=urls)
    ).index_build_id
    same = _build_of(
        _build(tmp_path / "b", pages={URL_P1_A: b"content-a"}, urls=urls)
    ).index_build_id
    other = _build_of(
        _build(tmp_path / "c", pages={URL_P1_A: b"content-b"}, urls=urls)
    ).index_build_id
    assert first == same
    assert first != other


def test_index_build_id_sensitive_to_declared_inputs(tmp_path: Path) -> None:
    """P2: id меняется при смене chunker/embedder/extractor/lexical/vector/snapshot."""
    pages = {URL_P1_A: b"doc"}
    urls = {"p1": [URL_P1_A]}
    base = _build_of(_build(tmp_path / "a", pages=pages, urls=urls)).index_build_id

    other_chunker = _build_of(
        _build(tmp_path / "b", pages=pages, urls=urls, chunker_version="chunker-2")
    ).index_build_id
    # 09 §8.1: snapshot_date — провенанс, не содержимое; id композиции не меняется.
    other_snapshot = _build_of(
        _build(tmp_path / "c", pages=pages, urls=urls, snapshot_date="2026-02-01")
    ).index_build_id
    assert other_snapshot == base
    other_embedder = _build_of(
        _build(tmp_path / "d", pages=pages, urls=urls, embedder=_Embedder(model_id="embed-y"))
    ).index_build_id

    extractor = _Extractor()
    extractor.version = "extractor-2"
    other_extractor = _build_of(
        _build(tmp_path / "e", pages=pages, urls=urls, extractor=extractor)
    ).index_build_id

    builder = _IndexBuilder()
    builder.lexical_engine_version = "lex-2"
    other_lexical = _build_of(
        _build(tmp_path / "f", pages=pages, urls=urls, index_builder=builder)
    ).index_build_id

    builder2 = _IndexBuilder()
    builder2.vector_store = "store-2"
    other_vector = _build_of(
        _build(tmp_path / "g", pages=pages, urls=urls, index_builder=builder2)
    ).index_build_id

    ids = {
        base,
        other_chunker,
        other_embedder,
        other_extractor,
        other_lexical,
        other_vector,
    }
    assert len(ids) == 6


def test_partition_build_id_ignores_retrieval_and_provenance() -> None:
    """09 §8.1: ``retrieval_*``/``params_token``/``snapshot_date`` не входят в ``pb``."""
    params = set(inspect.signature(_derive_partition_build_id).parameters)
    assert not {name for name in params if name.startswith("retrieval")}
    assert not {"params_token", "snapshot_date"} & params
    assert {
        "package_id",
        "product_id",
        "chunker_version",
        "embed_model_id",
        "extractor_version",
        "lexical_engine_version",
        "vector_store",
        "product_content_token",
    } <= params


def test_partition_build_id_deterministic_and_sensitive() -> None:
    """``pb`` детерминирован, чувствителен к каждому входу и не зависит от провенанса."""
    base = dict(
        package_id="pkg",
        product_id="p1",
        chunker_version="chunker-1",
        embed_model_id="embed-1",
        extractor_version="extractor-1",
        lexical_engine_version="lex-1",
        vector_store="npy-fp16",
        product_content_token="ct",
    )
    value = _derive_partition_build_id(**base)
    assert value.startswith("pb_") and len(value) == len("pb_") + 24
    assert _derive_partition_build_id(**base) == value
    variants = [
        {"package_id": "other"},
        {"product_id": "p2"},
        {"chunker_version": "chunker-2"},
        {"embed_model_id": "embed-2"},
        {"extractor_version": "extractor-2"},
        {"lexical_engine_version": "lex-2"},
        {"vector_store": "other"},
        {"product_content_token": "ct2"},
    ]
    assert {_derive_partition_build_id(**(base | variant)) for variant in variants}.isdisjoint(
        {value}
    )


def test_product_content_token_order_independent() -> None:
    """Отпечаток партиции — sorted ``(chunk_id, content_hash)``: порядок не значим."""
    def _chunk(chunk_id: str, digest: str) -> Any:
        return type("C", (), {"chunk_id": chunk_id, "content_hash": digest})()

    a = _product_content_token([_chunk("a", "1"), _chunk("b", "2")])
    b = _product_content_token([_chunk("b", "2"), _chunk("a", "1")])
    assert a == b
    assert a != _product_content_token([_chunk("a", "1"), _chunk("b", "3")])


# --------------------------------------------------------------------------- #
# I17: одна запись манифеста на продукт-партицию
# --------------------------------------------------------------------------- #


def test_one_manifest_entry_per_product(tmp_path: Path) -> None:
    result = _build(
        tmp_path,
        pages={URL_P1_A: b"a", URL_P2_A: b"b"},
        urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
    )
    build = _build_of(result)
    assert [e.product_id for e in build.entries] == ["p1", "p2"]
    # Одна композиция на когорту, но у каждого продукта — своя партиция (09 §8.1).
    assert len({e.index_build_id for e in build.entries}) == 1
    assert len({e.partition_build_id for e in build.entries}) == 2
    for entry in build.entries:
        assert entry.index_build_id == build.index_build_id
        assert entry.partition_key == f"{entry.index_build_id}:{entry.product_id}"
        assert entry.store_path == (
            f"index_store/{entry.package_id}/{entry.partition_build_id}/"
            f"{entry.product_id}/"
        )


# --------------------------------------------------------------------------- #
# I19: чанк без хеша отклоняется
# --------------------------------------------------------------------------- #


def test_i19_chunk_without_hash_is_rejected(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"no hash"},
            urls={"p1": [URL_P1_A]},
            chunker=_Chunker(empty_hash=True),
            store=store,
        )
        build = _build_of(result)
        assert build.chunk_count == 0
        assert build.entries == []
        assert any("I19" in err for err in result.errors)
        assert list(store.conn.execute("SELECT 1 FROM corpus_chunks")) == []
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Изоляция ошибок
# --------------------------------------------------------------------------- #


def test_error_isolation_crawler_and_extractor(tmp_path: Path) -> None:
    bad_url = "https://docs.test/p1/broken"
    result = _build(
        tmp_path,
        pages={URL_P1_A: b"ok", bad_url: b"ignored", URL_P2_A: b"extract error"},
        urls={"p1": [URL_P1_A, bad_url], "p2": [URL_P2_A]},
        crawler=_Crawler(
            {URL_P1_A: b"ok", bad_url: b"ignored", URL_P2_A: b"extract error"},
            raising={bad_url},
        ),
        extractor=_Extractor(error_urls={URL_P2_A}),
    )
    build = _build_of(result)
    # Сбой одного URL/документа не отменяет остальные.
    assert build.chunk_count == 1
    assert {e.product_id for e in build.entries} == {"p1"}
    assert any("crawl" in err or "RuntimeError" in err for err in result.errors)
    assert any("extract" in err for err in result.errors)


# --------------------------------------------------------------------------- #
# D2: URL-скоупедированная проверка исполнимости browser-рендера
# --------------------------------------------------------------------------- #


def test_browser_plan_on_http_only_crawler_skips_url_scoped(tmp_path: Path) -> None:
    """HTTP-краулер + browser-URL: пофURL-ошибка, фаза ``ok``, http-URL обработан (D2)."""
    browser_url = "https://browser.test/landing"
    pages = {URL_P1_A: b"http ok", browser_url: b"browser bytes"}
    crawler = _Crawler(pages)
    crawler.supports_browser = False  # type: ignore[attr-defined]

    result = _build(
        tmp_path,
        pages=pages,
        urls={"p1": [URL_P1_A, browser_url]},
        crawler=crawler,
        domain_profiles={"browser.test": DomainProfile(fetch=FetchMode.BROWSER)},
    )
    build = _build_of(result)

    assert result.status == "ok"
    assert build.chunk_count == 1
    # Browser-URL пропущен ДО фетча, с понятной пофURL-ошибкой.
    assert all(url != browser_url for url, _ in crawler.calls)
    assert any(
        browser_url in err and "browser" in err and "scrapy" in err
        for err in result.errors
    )
    # HTTP-URL обработан, без ошибки для него.
    assert [d.url for d in build.docs] == [URL_P1_A]
    assert not any(URL_P1_A in err for err in result.errors)


def test_explicit_http_override_avoids_browser_support_error(tmp_path: Path) -> None:
    """Явный override на ``http`` снимает browser-ошибку: проверяется резолв-план (D2)."""
    browser_url = "https://browser.test/landing"
    pages = {browser_url: b"served as http"}
    crawler = _Crawler(pages)
    crawler.supports_browser = False  # type: ignore[attr-defined]

    result = _build(
        tmp_path,
        pages=pages,
        urls={"p1": [browser_url]},
        crawler=crawler,
        domain_profiles={"browser.test": DomainProfile(fetch=FetchMode.BROWSER)},
        explicit_fetch={browser_url: FetchMode.HTTP},
    )
    build = _build_of(result)

    assert result.status == "ok"
    assert result.errors == []
    assert build.chunk_count == 1
    assert [url for url, _ in crawler.calls] == [browser_url]


def test_domain_profile_browser_routes_to_browser_crawler(tmp_path: Path) -> None:
    """Пакет без ``crawl_sources``: ``fetch: browser`` профиля домена → ``crawlers['browser']``.

    Локальная паритетность edge ``scheduler._resolve_engine`` и
    ``wiring._referenced_crawl_engines``: при пустом ``wtarget.engine`` ключ движка
    выводится из оси ``fetch``. Регрессия на ошибку «краулер HttpCrawler не поддерживает
    browser» для браузерных URL, чей движок задан только профилем домена.
    """
    browser_url = "https://browser.test/landing"
    http_fake = _Crawler({})
    http_fake.supports_browser = False  # type: ignore[attr-defined]
    browser_fake = _Crawler({browser_url: b"rendered"})

    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [browser_url]},
        crawler=http_fake,
        crawlers={"http": http_fake, "browser": browser_fake},
        domain_profiles={"browser.test": DomainProfile(fetch=FetchMode.BROWSER)},
    )
    build = _build_of(result)

    assert result.errors == []
    assert [url for url, _ in browser_fake.calls] == [browser_url]
    assert http_fake.calls == []
    assert build.chunk_count == 1


def test_domain_profile_browser_without_browser_crawler_keeps_error(
    tmp_path: Path,
) -> None:
    """Карта без ``browser`` — прежний честный URL-скоупедированный отказ (без тихой подмены)."""
    browser_url = "https://browser.test/landing"
    http_fake = _Crawler({browser_url: b"x"})
    http_fake.supports_browser = False  # type: ignore[attr-defined]

    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [browser_url]},
        crawler=http_fake,
        crawlers={"http": http_fake},
        domain_profiles={"browser.test": DomainProfile(fetch=FetchMode.BROWSER)},
    )

    assert http_fake.calls == []
    assert any(
        browser_url in err and "browser" in err and "scrapy" in err
        for err in result.errors
    )


# --------------------------------------------------------------------------- #
# Персистенция через StorePort
# --------------------------------------------------------------------------- #


def test_store_persistence_round_trip(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"persist me", URL_P2_A: b"persist two"},
            urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
            store=store,
        )
        build = _build_of(result)
        docs = list(store.conn.execute("SELECT doc_id, content_hash, raw_path FROM corpus_docs ORDER BY doc_id"))
        chunks = list(store.conn.execute("SELECT chunk_id, doc_id, content_hash, build_id FROM corpus_chunks ORDER BY chunk_id"))
        manifest = list(store.conn.execute("SELECT index_build_id, product_id FROM index_manifest ORDER BY product_id"))
        assert len(docs) == 2
        assert len(chunks) == 2
        assert [m["product_id"] for m in manifest] == ["p1", "p2"]
        assert {m["index_build_id"] for m in manifest} == {build.index_build_id}
        # README-контракт: get_chunk отдаёт порт-тип Chunk с непустым hash (I19).
        for chunk in chunks:
            port_chunk = store.get_chunk(chunk["chunk_id"])
            assert port_chunk is not None
            assert port_chunk.content_hash == chunk["content_hash"]
            assert port_chunk.content_hash
    finally:
        store.close()


class _AbortCrawl(BaseException):
    """Жёсткое прерывание прогона (аналог OOM/SIGKILL): ``Exception``-изоляция его не ловит."""


class _AbortOnUrlCrawler(_Crawler):
    """Краулер, «убивающий» прогон на выбранном URL (после коммита предыдущего)."""

    def __init__(self, pages: dict[str, bytes], abort_url: str) -> None:
        super().__init__(pages)
        self.abort_url = abort_url

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        if url == self.abort_url:
            raise _AbortCrawl(url)
        return super().fetch(url, plan)


class _FlakyDocStore(SqliteStore):
    """Store, отказывающий в ``upsert_corpus_doc`` для выбранных ``doc_id``."""

    def __init__(self, fail_doc_ids: set[str]) -> None:
        super().__init__(":memory:")
        self.fail_doc_ids = set(fail_doc_ids)
        self.doc_attempts: list[str] = []

    def upsert_corpus_doc(self, doc: CorpusDoc) -> None:
        self.doc_attempts.append(doc.doc_id)
        if doc.doc_id in self.fail_doc_ids:
            raise RuntimeError("store unavailable")
        super().upsert_corpus_doc(doc)


def test_docs_persisted_mid_product_survive_abort(tmp_path: Path) -> None:
    """Жёсткое прерывание посреди продукта не теряет уже закоммиченные ``corpus_docs``.

    Первый seed-root продукта коммитится и персистится сразу; на втором краулер
    «убивает» прогон. Строка первого документа обязана уже быть в БД вместе с полным
    retention-провенансом — иначе resume после OOM повторно извлекал бы весь продукт.
    """
    store = SqliteStore(":memory:")
    payload = b"committed before abort"
    crawler = _AbortOnUrlCrawler({URL_P1_A: payload}, abort_url=URL_P1_B)
    try:
        with pytest.raises(_AbortCrawl):
            _build(
                tmp_path,
                pages={},
                urls={"p1": [URL_P1_A, URL_P1_B]},
                store=store,
                crawler=crawler,
            )

        rows = list(store.conn.execute("SELECT * FROM corpus_docs"))
        assert len(rows) == 1
        row = rows[0]
        expected_id = f"p1:{content_hash(payload)}"
        assert row["doc_id"] == expected_id
        assert row["product_id"] == "p1"
        assert row["url"] == URL_P1_A
        assert row["fetched_at"] == TS
        assert row["content_hash"] == content_hash(payload)
        assert row["extractor_version"] == "extractor-1"
        assert row["render_mode"] == "http"
        assert row["lang"] == "en"
        raw_path = Path(row["raw_path"])
        assert raw_path.is_file()
        assert raw_path.read_bytes() == payload
        # Второй корень не обработан — прерывание действительно посреди продукта.
        assert [url for url, _ in crawler.calls] == [URL_P1_A]
    finally:
        store.close()


def test_resume_upsert_is_idempotent(tmp_path: Path) -> None:
    """Повторный коммит того же документа (resume) даёт ровно одну строку ``corpus_docs``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    payload = b"same bytes"
    doc_id = f"p1:{content_hash(payload)}"
    try:
        first = _build(
            tmp_path,
            pages={URL_P1_A: payload},
            urls={"p1": [URL_P1_A]},
            store=store,
            run_id=RUN_ID,
        )
        assert _build_of(first).chunk_count == 1

        # Resume из retained raw: краулер не должен вызываться вовсе (``_BombCrawler``).
        second = _build(
            tmp_path,
            pages={URL_P1_A: payload},
            urls={"p1": [URL_P1_A]},
            store=store,
            run_id=RUN_ID,
            crawler=_BombCrawler({URL_P1_A: payload}),
        )
        assert _build_of(second).chunk_count == 1

        rows = list(
            store.conn.execute(
                "SELECT doc_id, raw_path FROM corpus_docs WHERE doc_id = ?", (doc_id,)
            )
        )
        assert len(rows) == 1
        assert Path(rows[0]["raw_path"]).read_bytes() == payload
    finally:
        store.close()


def test_upsert_failure_is_isolated_and_others_persist(tmp_path: Path) -> None:
    """Сбой ``upsert_corpus_doc`` одного документа → ``errors``; остальные записаны."""
    bad = b"bad doc"
    good = b"good doc"
    bad_id = f"p1:{content_hash(bad)}"
    good_id = f"p1:{content_hash(good)}"
    store = _FlakyDocStore(fail_doc_ids={bad_id})
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: bad, URL_P1_B: good},
            urls={"p1": [URL_P1_A, URL_P1_B]},
            store=store,
        )

        assert any(
            "upsert_corpus_doc" in err and bad_id in err for err in result.errors
        )
        rows = list(store.conn.execute("SELECT doc_id FROM corpus_docs"))
        assert [row["doc_id"] for row in rows] == [good_id]
        # Каждый документ прошёл через upsert ровно один раз: финального дубля-дампа нет.
        assert store.doc_attempts == [bad_id, good_id]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Опциональное подключение embedder/index_builder (W8) и структура портов
# --------------------------------------------------------------------------- #


def test_optional_embed_and_index_build(tmp_path: Path) -> None:
    embedder = _Embedder()
    builder = _IndexBuilder()
    result = _build(
        tmp_path,
        pages={URL_P1_A: b"one", URL_P2_A: b"two"},
        urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
        embedder=embedder,
        index_builder=builder,
    )
    build = _build_of(result)
    assert sorted(builder.built) == [(1, "p1"), (1, "p2")]
    assert builder.commits == 1
    assert len(embedder.texts) == 2
    for entry in build.entries:
        assert entry.embed_model_id == "embed-x"
        assert entry.embed_dim == 3
        assert entry.lexical_engine_version == "lex-1"
        assert entry.vector_store == "npy"


def test_no_urls_returns_empty_ok(tmp_path: Path) -> None:
    result = build_corpus(_plan(), _Crawler({}), _Extractor(), _Chunker(), urls=None)
    build = _build_of(result)
    assert result.status == "ok"
    assert build.chunk_count == 0
    assert build.entries == []
    assert result.errors == []


def test_urls_must_be_mapping(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        build_corpus(_plan(), _Crawler({}), _Extractor(), _Chunker(), urls=[URL_P1_A])  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# «Disk first, DB second»: физическая сборка авторитетна (K2/§13)
# --------------------------------------------------------------------------- #


class _ImmutableBuilder:
    """Сборщик, отказывающий во втором ``build`` того же ключа с другим содержимым.

    Имитирует :class:`~core.adapters.index_npy.ImmutableBuildError` по имени класса:
    сигнатура ``(chunk_id, content_hash)`` меняется вместе с контентом, поэтому повторная
    сборка по тому же ``(package, build_id, product_id)`` поднимает ошибку.
    """

    lexical_engine_version = "lex-1"
    vector_store = "npy"

    def __init__(self) -> None:
        self._signatures: dict[tuple[str, str, str], tuple] = {}
        self.commits = 0

    def build(self, chunks, vectors, meta):  # noqa: ANN001 — структурная заглушка порта
        chunks = list(chunks)
        signature = tuple((c.chunk_id, c.content_hash) for c in chunks)
        key = (
            meta.package,
            (meta.extra or {}).get("partition_build_id", ""),
            meta.product_id,
        )
        previous = self._signatures.get(key)
        if previous is not None and previous != signature:
            raise ImmutableBuildError(
                "NpyIndexBuilder: сборка уже существует с другим содержимым (§13)"
            )
        self._signatures[key] = signature
        return key[1]

    def commit(self) -> None:
        self.commits += 1


class _RefusingBuilder:
    """Сборщик, отказывающий в сборке заданных продуктов (прочие строит штатно)."""

    lexical_engine_version = "lex-1"
    vector_store = "npy"

    def __init__(self, refuse: set[str] | None = None) -> None:
        self.refuse = set(refuse or ())
        self.built: list[tuple[int, str]] = []
        self.commits = 0

    def build(self, chunks, vectors, meta):  # noqa: ANN001 — структурная заглушка порта
        self.built.append((len(list(chunks)), meta.product_id))
        if meta.product_id in self.refuse:
            raise ImmutableBuildError(
                "NpyIndexBuilder: партиция уже зафиксирована на диске с другим содержимым (§13)"
            )
        return "builder-id"

    def commit(self) -> None:
        self.commits += 1


def _manifest_row(store: SqliteStore, build_id: str, product_id: str):  # noqa: ANN202
    return store.conn.execute(
        "SELECT stats_json FROM index_manifest WHERE index_build_id = ? AND product_id = ?",
        (build_id, product_id),
    ).fetchone()


def _chunk_count(store: SqliteStore, build_id: str) -> int:
    row = store.conn.execute(
        "SELECT COUNT(*) AS n FROM corpus_chunks WHERE build_id = ?", (build_id,)
    ).fetchone()
    return int(row["n"])


def test_new_content_mints_new_build_leaves_previous_untouched(tmp_path: Path) -> None:
    # P2: изменённый контент при тех же декларированных входах даёт **новую** сборку, а
    # прошлая продукт-партиция не мутируется (append-only, K2/§13). Это заменяет прежний
    # сценарий same-id/different-content: теперь он структурно невозможен.
    store = SqliteStore(":memory:")
    builder = _ImmutableBuilder()
    try:
        first = _build(
            tmp_path / "first",
            pages={URL_P1_A: b"first"},
            urls={"p1": [URL_P1_A]},
            store=store,
            embedder=_Embedder(),
            index_builder=builder,
        )
        build = _build_of(first)
        build_id = build.index_build_id
        partition_id = build.entries[0].partition_build_id or ""
        manifest_before = _manifest_row(store, build_id, "p1")
        assert manifest_before is not None
        assert _chunk_count(store, partition_id) == 1

        second = _build(
            tmp_path / "second",
            pages={URL_P1_A: b"second-a", URL_P1_B: b"second-b"},
            urls={"p1": [URL_P1_A, URL_P1_B]},
            store=store,
            embedder=_Embedder(),
            index_builder=builder,
        )
        build2 = _build_of(second)
        build_id_2 = build2.index_build_id
        partition_id_2 = build2.entries[0].partition_build_id or ""
        # Контент другой ⇒ РАЗНЫЙ pb и, как следствие, РАЗНЫЙ cid.
        assert partition_id_2 != partition_id
        assert build_id_2 != build_id
        assert second.errors == []

        # Прошлая сборка не изменилась: БД согласована с иммутабельной партицией на диске.
        assert _manifest_row(store, build_id, "p1") == manifest_before
        assert _chunk_count(store, partition_id) == 1
        # Новая сборка зафиксирована собственной партицией.
        assert _manifest_row(store, build_id_2, "p1") is not None
        assert _chunk_count(store, partition_id_2) == 2
    finally:
        store.close()


@requires_numpy
def test_successful_build_writes_manifest_matching_disk(tmp_path: Path) -> None:
    index_root = tmp_path / "index_store"
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path / "raw",
            pages={URL_P1_A: b"physical"},
            urls={"p1": [URL_P1_A]},
            store=store,
            embedder=_Embedder(),
            index_builder=NpyIndexBuilder(index_root),
        )
        build = _build_of(result)
        assert result.errors == []
        package_id = build.entries[0].package_id or ""
        partition_id = build.entries[0].partition_build_id or ""
        partition = index_format.partition_dir(
            index_root, package_id, partition_id, "p1"
        )
        physical = json.loads((partition / "manifest.json").read_text(encoding="utf-8"))
        persisted = _manifest_row(store, build.index_build_id, "p1")
        assert persisted is not None
        stats = json.loads(persisted["stats_json"])
        assert stats["chunks"] == physical["stats"]["chunks"]
        physical_lines = [
            line
            for line in (partition / "chunk_ids.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert _chunk_count(store, partition_id) == len(physical_lines)
    finally:
        store.close()


def test_docs_and_chunks_without_index_builder(tmp_path: Path) -> None:
    # Легитимный путь «логический манифест без физического индекса»: без embedder/builder
    # docs, chunks и index_manifest всё равно персистятся (обратная совместимость).
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"logical"},
            urls={"p1": [URL_P1_A]},
            store=store,
        )
        build = _build_of(result)
        assert result.errors == []
        docs = list(store.conn.execute("SELECT doc_id FROM corpus_docs"))
        chunks = list(store.conn.execute("SELECT chunk_id FROM corpus_chunks"))
        manifest = list(store.conn.execute("SELECT product_id FROM index_manifest"))
        assert len(docs) == 1
        assert len(chunks) == 1
        assert [m["product_id"] for m in manifest] == ["p1"]
        assert _chunk_count(store, build.entries[0].partition_build_id or "") == 1
    finally:
        store.close()


def test_partial_product_failure(tmp_path: Path) -> None:
    # p1 собирается физически, p2 отказан: p1 — чанки+манифест; p2 — только docs, без манифеста.
    store = SqliteStore(":memory:")
    builder = _RefusingBuilder(refuse={"p2"})
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"ok-one", URL_P2_A: b"ok-two"},
            urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
            store=store,
            embedder=_Embedder(),
            index_builder=builder,
        )
        build = _build_of(result)

        man_p1 = _manifest_row(store, build.index_build_id, "p1")
        man_p2 = _manifest_row(store, build.index_build_id, "p2")
        assert man_p1 is not None
        assert man_p2 is None

        p1_chunks = list(
            store.conn.execute("SELECT chunk_id FROM corpus_chunks WHERE product_id = 'p1'")
        )
        p2_chunks = list(
            store.conn.execute("SELECT chunk_id FROM corpus_chunks WHERE product_id = 'p2'")
        )
        assert len(p1_chunks) == 1
        assert p2_chunks == []

        p2_docs = list(
            store.conn.execute("SELECT doc_id FROM corpus_docs WHERE product_id = 'p2'")
        )
        assert len(p2_docs) == 1

        assert any("p2" in err and "не зафиксирована" in err for err in result.errors)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# L1a: durable-краул — сеанс краула (crawl_sessions/crawl_session_items)
# --------------------------------------------------------------------------- #


class _BombCrawler(_Crawler):
    """Краулер, обязанный НЕ вызываться: любой ``fetch`` — провал теста (resume)."""

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        raise AssertionError("краулер вызван при наличии retained-байтов (L1a resume)")


def _items(store: SqliteStore) -> list[CrawlSessionItem]:
    """Элементы сеанса краула прогона (порядок ``product_id, url``)."""
    return list(store.iter_crawl_session_items(SESSION_ID))


def test_resume_from_retained_raw_skips_refetch(tmp_path: Path) -> None:
    """URL с prior ``FETCHED`` + retained raw НЕ фетчится повторно и ре-извлекается (L1a)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        first = _build(
            tmp_path, pages={URL_P1_A: b"retained bytes"},
            urls={"p1": [URL_P1_A]}, store=store, run_id=RUN_ID,
        )
        assert _build_of(first).chunk_count == 1
        prior = next(iter(_items(store)))
        assert prior.status is CrawlItemStatus.FETCHED
        assert prior.raw_path and Path(prior.raw_path).is_file()

        # Второй прогон с «бомбой»-краулером: фетча быть не должно.
        second = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}), store=store, run_id=RUN_ID,
        )
        build = _build_of(second)
        assert second.errors == []
        assert build.chunk_count == 1
        assert build.docs[0].content_hash == content_hash(b"retained bytes")
        # Повторный resume идемпотентен: один и тот же id сборки, статус FETCHED.
        assert build.index_build_id == _build_of(first).index_build_id
        assert next(iter(_items(store))).status is CrawlItemStatus.FETCHED
        # Сеанс финализирован без сбоев → ``COMPLETED``.
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.COMPLETED
    finally:
        store.close()


def test_crawl_failure_records_failed_source_unavailable(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    _make_run(store)
    bad = "https://docs.test/p1/broken"
    try:
        result = _build(
            tmp_path, pages={}, urls={"p1": [bad]},
            crawler=_Crawler({}, raising={bad}), store=store, run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.status is CrawlItemStatus.FAILED
        assert row.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert row.error
        assert any("crawl" in err for err in result.errors)
        assert _build_of(result).crawl_failed == 1
        # Есть сбойный элемент → сеанс ``PARTIAL``.
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_not_found_is_terminal_and_not_retried_on_resume(tmp_path: Path) -> None:
    """404/410 — определённый ответ: один фетч, без повторов и ``attempts_exhausted``.

    Регрессия расточительности: ``not_found`` раньше записывался как
    ``source_unavailable``, а FAILED-элемент без retained-байтов пере-фетчивался на
    каждом resume (и при ``max_attempts`` превращался в ``attempts_exhausted``).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _NotFoundCrawler()
    try:
        _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=crawler, store=store, run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.status is CrawlItemStatus.FAILED
        assert row.error_kind == FailureKind.NOT_FOUND.value
        assert row.attempts == 1
        assert len(crawler.calls) == 1

        # Resume без потолка: определённый ответ не повторяется.
        _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=crawler, store=store, run_id=RUN_ID,
        )
        assert len(crawler.calls) == 1
        row = next(iter(_items(store)))
        assert row.error_kind == FailureKind.NOT_FOUND.value
        assert row.attempts == 1

        # ``max_attempts`` не переклассифицирует терминальный not_found.
        result = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=crawler, store=store, run_id=RUN_ID, max_attempts=1,
        )
        assert len(crawler.calls) == 1
        row = next(iter(_items(store)))
        assert row.error_kind == FailureKind.NOT_FOUND.value
        assert row.error_kind != "attempts_exhausted"
        assert row.attempts == 1
        assert not any("max_attempts" in err for err in result.errors)
    finally:
        store.close()


def test_single_url_origin_not_found_is_incomplete(tmp_path: Path) -> None:
    """404 на единственном URL: наблюдений ноль — отсутствие не доказуемо (2.3.0).

    ``not_found`` — наблюдение отсутствия конкретного URL, а не доказательство, что
    у продукта нет возможности. Корпус без единого ``FETCHED`` неполон; ``reason``
    остаётся ``None`` (объясняют счётчики и ``blocked``).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=_NotFoundCrawler(), store=store, run_id=RUN_ID,
        )
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.unreadable_origins == []
        assert coverage.partial_blocks == {}
        assert coverage.blocked == {FailureKind.NOT_FOUND.value: 1}
        assert coverage.expected_sources == 1
        assert coverage.observed_sources == 0
        assert coverage.reason is None
    finally:
        store.close()


def test_all_not_found_origin_is_not_complete_and_absence_not_provable(
    tmp_path: Path,
) -> None:
    """Origin, где всё 404: ноль наблюдений — ``complete=False``, ❌ не доказуем.

    Сайт ответил «ресурса нет» на каждый URL, но ни одного документа не прочитано:
    отсутствие параметра на исчезнувшем сайте нельзя публиковать как машинный ``❌``
    без человеческой аттестации (2.3.0).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    gone = ["https://gone.test/a", "https://gone.test/b"]
    try:
        _build(
            tmp_path, pages={}, urls={"p1": gone},
            crawler=_NotFoundCrawler(), store=store, run_id=RUN_ID,
        )
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.unreadable_origins == []
        assert coverage.partial_blocks == {}
        assert coverage.blocked == {FailureKind.NOT_FOUND.value: 2}
        assert coverage.expected_sources == 2
        assert coverage.observed_sources == 0
        assert coverage.reason is None
    finally:
        store.close()


def test_robots_refusal_still_yields_robots_disallow(tmp_path: Path) -> None:
    """Robots-отказ не задет: ``refusal_reason`` по-прежнему побеждает дефолт."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _RefusedCrawler()
    try:
        _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=crawler, store=store, run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.status is CrawlItemStatus.FAILED
        assert row.error_kind == FailureKind.ROBOTS_DISALLOW.value
        assert len(crawler.calls) == 1
    finally:
        store.close()


def test_max_attempts_skips_exhausted_url_and_marks_failed(tmp_path: Path) -> None:
    """Phase 4: URL с ``attempts >= max_attempts`` не фетчится → терминальный ``FAILED``.

    Первый прогон изолированно фиксирует сбой (``attempts=1``); повторный с ``max_attempts=1``
    URL не фетчит (не тихий дроп), пишет ``error_kind="attempts_exhausted"`` и финализирует
    сеанс ``PARTIAL``.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    failing = _Crawler({URL_P1_A: b"one"}, raising={URL_P1_A})
    try:
        _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=failing, store=store, run_id=RUN_ID,
        )
        first = next(iter(_items(store)))
        assert first.status is CrawlItemStatus.FAILED and first.attempts == 1

        working = _Crawler({URL_P1_A: b"one"})
        result = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=working, store=store, run_id=RUN_ID, max_attempts=1,
        )
        assert working.calls == []  # попытки исчерпаны — живого фетча нет
        assert any("max_attempts" in err for err in result.errors)
        row = next(iter(_items(store)))
        assert row.error_kind == "attempts_exhausted"
        assert row.attempts == 1
        assert _build_of(result).crawl_failed == 1
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_progress_callback_receives_session_counts(tmp_path: Path) -> None:
    """Phase 4: колбэк ``progress`` получает снимки ``crawl_session_counts`` по ходу фазы."""
    store = SqliteStore(":memory:")
    _make_run(store)
    snapshots: list[dict[str, int]] = []
    try:
        _build(
            tmp_path, pages={URL_P1_A: b"one"}, urls={"p1": [URL_P1_A]},
            crawler=_Crawler({URL_P1_A: b"one"}), store=store, run_id=RUN_ID,
            progress=snapshots.append,
        )
        assert snapshots, "колбэк прогресса обязан вызываться"
        last = snapshots[-1]
        assert set(last) == {"pending", "running", "fetched", "failed", "cancelled"}
        assert last["fetched"] == 1
    finally:
        store.close()


def test_extraction_failure_records_failed_parser_and_retains_raw(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path, pages={URL_P1_A: b"bytes"},
            urls={"p1": [URL_P1_A]},
            extractor=_Extractor(error_urls={URL_P1_A}),
            store=store, run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.status is CrawlItemStatus.FAILED
        assert row.error_kind == FailureKind.PARSER_FAILED.value
        # raw_path сохранён: поздний resume сможет ре-извлечь без ре-фетча (I19).
        assert row.raw_path and Path(row.raw_path).is_file()
        assert any("extract" in err for err in result.errors)
    finally:
        store.close()


def test_no_run_id_degrades_to_no_session_rows(tmp_path: Path) -> None:
    """Без ``run_id`` durable-краул выключен: обычный фетч, строк сеанса нет, счётчики — 0."""
    store = SqliteStore(":memory:")
    try:
        crawler = _Crawler({URL_P1_A: b"x"})
        result = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]}, crawler=crawler, store=store
        )
        assert len(crawler.calls) == 1
        assert list(store.iter_crawl_session_items(SESSION_ID)) == []
        build = _build_of(result)
        assert (
            build.crawl_pending,
            build.crawl_fetched,
            build.crawl_done,
            build.crawl_failed,
        ) == (0, 0, 0, 0)
    finally:
        store.close()


def test_index_build_crawl_counters(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"one", URL_P2_A: b"two"},
            urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
            store=store, run_id=RUN_ID,
        )
        build = _build_of(result)
        # Элемент ``FETCHED`` = «ready»: ``crawl_done`` — зеркало ``crawl_fetched`` (L1a).
        assert (build.crawl_done, build.crawl_failed) == (2, 0)
        assert (build.crawl_pending, build.crawl_fetched) == (0, 2)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# L1a (must-fix): per-(product,url) resume, stale-raw, lang, counters
# --------------------------------------------------------------------------- #


def test_cross_product_same_url_resume_is_per_product(tmp_path: Path) -> None:
    """Один URL под двумя продуктами: элементы независимы (PK ``(session_id, product_id, url)``).

    ``prior`` обязан ключеваться ``(product_id, url)``: иначе элемент одного продукта
    затирает элемент другого и p1 (FETCHED+raw) был бы ошибочно ре-фетчен (I17/§4.4).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    url = "https://docs.test/shared"
    try:
        first = _build(
            tmp_path, pages={url: b"shared"}, urls={"p1": [url], "p2": [url]},
            store=store, run_id=RUN_ID,
        )
        assert _build_of(first).crawl_done == 2
        rows = _items(store)
        assert {(r.product_id, r.url) for r in rows} == {("p1", url), ("p2", url)}

        # p1 — валидный FETCHED-элемент (raw сохранён). p2 — FAILED без raw.
        p1_row = next(r for r in rows if r.product_id == "p1")
        store.upsert_crawl_session_item(p1_row)
        store.upsert_crawl_session_item(
            CrawlSessionItem(
                session_id=SESSION_ID, product_id="p2", url=url,
                status=CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            )
        )

        crawler = _Crawler({url: b"shared"})
        second = _build(
            tmp_path, pages={}, urls={"p1": [url], "p2": [url]},
            crawler=crawler, store=store, run_id=RUN_ID,
        )
        # p1 резюмит без фетча, p2 фетчит → ровно один вызов краулера (только для p2).
        assert [called for called, _ in crawler.calls] == [url]
        assert _build_of(second).chunk_count == 2
        assert second.errors == []
    finally:
        store.close()


def test_stale_raw_mismatch_triggers_refetch(tmp_path: Path) -> None:
    """Подменённый retained-файл (hash ≠ audited ``content_hash``) → обычный фетч, не resume."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path, pages={URL_P1_A: b"original"},
            urls={"p1": [URL_P1_A]}, store=store, run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.raw_path is not None and Path(row.raw_path).is_file()
        Path(row.raw_path).write_bytes(b"corrupted-on-disk")

        crawler = _Crawler({URL_P1_A: b"refetched"})
        second = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=crawler, store=store, run_id=RUN_ID,
        )
        assert len(crawler.calls) == 1  # расхождение хеша → снова фетч
        doc = _build_of(second).docs[0]
        assert doc.content_hash == content_hash(b"refetched")
    finally:
        store.close()


def test_store_none_with_run_id_degrades_cleanly(tmp_path: Path) -> None:
    """``store=None`` при заданном ``run_id``: durable-краул выключен, фаза штатна (L1a)."""
    crawler = _Crawler({URL_P1_A: b"x"})
    result = _build(
        tmp_path, pages={}, urls={"p1": [URL_P1_A]},
        crawler=crawler, store=None, run_id=RUN_ID,
    )
    assert len(crawler.calls) == 1
    assert result.errors == []
    assert _build_of(result).crawl_done == 0


def test_resume_from_failed_parser_failed_reextracts(tmp_path: Path) -> None:
    """``FAILED``/``parser_failed`` с сохранённым raw → resume ре-извлекает без ре-фетча."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path, pages={URL_P1_A: b"bytes"},
            urls={"p1": [URL_P1_A]},
            extractor=_Extractor(error_urls={URL_P1_A}),
            store=store, run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.status is CrawlItemStatus.FAILED
        assert row.error_kind == FailureKind.PARSER_FAILED.value
        assert row.raw_path and Path(row.raw_path).is_file()

        # Рабочий экстрактор + «бомба»-краулер: raw уже есть, фетча быть не должно.
        second = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}), store=store, run_id=RUN_ID,
        )
        assert second.errors == []
        assert _build_of(second).chunk_count == 1
        assert next(iter(_items(store))).status is CrawlItemStatus.FETCHED
    finally:
        store.close()


def test_browser_unsupported_url_with_session_marks_failed_and_partial(tmp_path: Path) -> None:
    """B2: пропущенный (browser без поддержки) URL при активном сеансе → ``FAILED``-элемент.

    Раньше пропуск не писал элемент и сеанс ложно финализировался ``COMPLETED``. Теперь
    фиксируется ``FAILED`` (``source_unavailable``), сеанс становится ``PARTIAL`` (URL видим
    и возобновляем), а счётчики берутся из элементов сеанса (``crawl_pending == 0``).
    """
    browser_url = "https://browser.test/landing"
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _Crawler({browser_url: b"x"})
    crawler.supports_browser = False  # type: ignore[attr-defined]
    try:
        result = _build(
            tmp_path, pages={}, urls={"p1": [browser_url]}, crawler=crawler,
            store=store, run_id=RUN_ID,
            domain_profiles={"browser.test": DomainProfile(fetch=FetchMode.BROWSER)},
        )
        assert result.status == "ok"
        assert any("browser" in err for err in result.errors)
        # Пропуск оставляет durable-элемент ``FAILED`` (не «молчаливый pending»).
        row = next(iter(_items(store)))
        assert row.status is CrawlItemStatus.FAILED
        assert row.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert row.error and "browser" in row.error
        build = _build_of(result)
        assert (build.crawl_pending, build.crawl_fetched, build.crawl_failed) == (0, 0, 1)
        # Сеанс не «ложно завершён»: есть неразрешённый URL → ``PARTIAL``.
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_browser_unsupported_url_without_session_writes_no_rows(tmp_path: Path) -> None:
    """B2: без сеанса (нет ``store``/``run_id``) пропущенный URL строк не пишет, счётчики — 0."""
    browser_url = "https://browser.test/landing"
    crawler = _Crawler({browser_url: b"x"})
    crawler.supports_browser = False  # type: ignore[attr-defined]

    result = _build(
        tmp_path, pages={}, urls={"p1": [browser_url]}, crawler=crawler,
        domain_profiles={"browser.test": DomainProfile(fetch=FetchMode.BROWSER)},
    )
    build = _build_of(result)
    assert (build.crawl_pending, build.crawl_fetched, build.crawl_failed) == (0, 0, 0)
    assert any("browser" in err for err in result.errors)


def test_retention_write_failure_marks_failed(tmp_path: Path) -> None:
    """Сбой записи retention помечает URL ``FAILED`` (source_unavailable), не ``pending`` (L1a)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    # Файл вместо каталога продукта → ``mkdir(parents=True, exist_ok=True)`` падает.
    (blocked / "p1").write_bytes(b"not a directory")
    try:
        result = build_corpus(
            _plan(),
            _Crawler({URL_P1_A: b"x"}),
            _Extractor(),
            _Chunker(),
            store,
            urls={"p1": [URL_P1_A]},
            raw_root=blocked,
            run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.status is CrawlItemStatus.FAILED
        assert row.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("retention" in err for err in result.errors)
        assert _build_of(result).crawl_failed == 1
    finally:
        store.close()


def test_lang_preserved_across_resume(tmp_path: Path) -> None:
    """``lang`` персистится в элементе сеанса и восстанавливается при resume (нет расхождения)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path, pages={URL_P1_A: b"bytes"},
            urls={"p1": [URL_P1_A]}, store=store, run_id=RUN_ID,
        )
        row = next(iter(_items(store)))
        assert row.lang == "en"  # из _Crawler (RawPage.lang)

        second = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}), store=store, run_id=RUN_ID,
        )
        assert _build_of(second).docs[0].lang == "en"
    finally:
        store.close()


def test_crawl_defaults_resolved_into_fetch_plan(tmp_path: Path) -> None:
    """``crawl_defaults`` (run.yaml ``crawl``) доезжает до ``FetchPlan`` краулера (D4)."""
    crawler = _Crawler({URL_P1_A: b"page"})
    defaults = CrawlRunConfig(
        strategy=CrawlStrategy.HONEST,
        rate_limit=RateLimitSpec(min_s=1.0, max_s=3.0),
    )
    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [URL_P1_A]},
        crawler=crawler,
        crawl_defaults=defaults,
        domain_profiles={"docs.test": DomainProfile(fetch=FetchMode.BROWSER)},
    )
    assert result.status == "ok"
    (_, plan) = crawler.calls[0]
    assert plan.strategy is CrawlStrategy.HONEST
    assert plan.rate_limit == RateLimitSpec(min_s=1.0, max_s=3.0)


@pytest.mark.parametrize("strategy", [CrawlStrategy.HONEST, CrawlStrategy.HYBRID])
def test_live_fetch_persists_crawl_strategy(tmp_path: Path, strategy: CrawlStrategy) -> None:
    """v2.4/L1a: непустой ``FetchPlan.strategy`` при живом фетче доезжает в элемент сеанса."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path,
            pages={URL_P1_A: b"page"},
            urls={"p1": [URL_P1_A]},
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(strategy=strategy),
            domain_profiles={"docs.test": DomainProfile(fetch=FetchMode.BROWSER)},
        )
        row = next(iter(_items(store)))
        assert row.strategy == strategy.value
    finally:
        store.close()


def test_crawl_defaults_strategy_normalized_on_http_url(tmp_path: Path) -> None:
    """HTTP-URL нормализует стратегию в ``AUTO`` (инвариант осей); rate_limit сохраняется."""
    crawler = _Crawler({URL_P1_A: b"page"})
    defaults = CrawlRunConfig(
        strategy=CrawlStrategy.HONEST,
        rate_limit=RateLimitSpec(min_s=1.0, max_s=3.0),
    )
    _build(
        tmp_path, pages={}, urls={"p1": [URL_P1_A]}, crawler=crawler,
        crawl_defaults=defaults,
    )
    (_, plan) = crawler.calls[0]
    assert plan.strategy is CrawlStrategy.AUTO
    assert plan.rate_limit == RateLimitSpec(min_s=1.0, max_s=3.0)


def test_crawl_defaults_absent_keeps_auto_strategy(tmp_path: Path) -> None:
    """Без ``crawl_defaults`` план остаётся ``auto`` без rate-limit (поведение неизменно)."""
    crawler = _Crawler({URL_P1_A: b"page"})
    _build(tmp_path, pages={}, urls={"p1": [URL_P1_A]}, crawler=crawler)
    (_, plan) = crawler.calls[0]
    assert plan.strategy is CrawlStrategy.AUTO
    assert plan.rate_limit is None


# --------------------------------------------------------------------------- #
# L1a: per-engine выбор краулера и финализация сеанса
# --------------------------------------------------------------------------- #


def test_engine_selected_crawler_used_and_recorded(tmp_path: Path) -> None:
    """``crawl_sources`` → ``engine``: активный краулер берётся из ``crawlers[engine]`` (L1a).

    Проверяется и выбор краулера (``selected`` вызывается, ``default`` — нет), и провенанс
    элемента сеанса (``engine``/``kind``).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    default = _Crawler({})
    selected = _Crawler({URL_P1_A: b"engine-bytes"})
    sources = CrawlSourcesConfig(products={"p1": CrawlSource(engine="fake", kind="web")})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=default,
            crawlers={"fake": selected},
            crawl_sources=sources,
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert [called for called, _ in selected.calls] == [URL_P1_A]
        assert default.calls == []
        item = next(iter(_items(store)))
        assert item.status is CrawlItemStatus.FETCHED
        assert item.engine == "fake"
        assert item.kind == "web"
    finally:
        store.close()


def test_applied_engine_recorded_without_requested_engine(tmp_path: Path) -> None:
    """Phase 2.1/S1: без ``crawl_sources.engine`` провенанс несёт транспорт адаптера.

    Резолвер возвращает ``engine=None`` (ни один слой не назвал движок), но фактически
    применимый движок известен адаптеру (duck-typed ``engine``) — он и попадает в элемент.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=_Crawler({URL_P1_A: b"x"}, engine="http"),
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        item = next(iter(_items(store)))
        assert item.status is CrawlItemStatus.FETCHED
        assert item.engine == "http"
        assert item.kind is None

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        provenance = json.loads(session.provenance_json or "{}")
        assert provenance["engines"] == ["http"]
    finally:
        store.close()


def test_session_aggregates_applied_engines_from_items(tmp_path: Path) -> None:
    """Phase 2.1/S1: агрегат движков сеанса — из персистенных элементов (смешанные движки)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    sources = CrawlSourcesConfig(
        sites={
            "p1": {"docs.test": CrawlSource(engine="http", kind="web")},
            "p2": {"docs.test": CrawlSource(engine="browser", kind="web")},
        }
    )
    http_crawler = _Crawler({URL_P1_A: b"a"}, engine="http")
    browser_crawler = _Crawler({URL_P2_A: b"b"}, engine="browser")
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
            crawler=_Crawler({}),
            crawlers={"http": http_crawler, "browser": browser_crawler},
            crawl_sources=sources,
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        provenance = json.loads(session.provenance_json or "{}")
        assert provenance["engines"] == ["browser", "http"]
        assert provenance["kinds"] == ["web"]
    finally:
        store.close()


def test_session_finalized_completed_when_all_fetched(tmp_path: Path) -> None:
    """Сеанс без сбоев финализируется ``COMPLETED`` с ``finished_at`` (L1a)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path,
            pages={URL_P1_A: b"x", URL_P2_A: b"y"},
            urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
            store=store,
            run_id=RUN_ID,
        )
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.COMPLETED
        assert session.finished_at
        assert (session.fetched, session.failed) == (2, 0)
    finally:
        store.close()


def test_session_finalized_partial_on_failure(tmp_path: Path) -> None:
    """Сеанс с хотя бы одним сбойным элементом финализируется ``PARTIAL`` (L1a)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    bad = "https://docs.test/p1/broken"
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [bad]},
            crawler=_Crawler({}, raising={bad}),
            store=store,
            run_id=RUN_ID,
        )
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.PARTIAL
        assert session.finished_at
        assert (session.fetched, session.failed) == (0, 1)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# P2-3: централизованный politeness-пейсинг — единственный владелец corpus
# --------------------------------------------------------------------------- #


class _RecordingLimiter(RateLimiter):
    """Пейсинг-шпион: пишет ``(host, spec)`` в ``wait``; ``slept`` — фактические задержки.

    ``sleep`` двигает фейковые часы на проспанное время (как реальный ``time.sleep``),
    поэтому последовательные резервы не накапливаются и сон равен сэмплированной
    задержке — без wall-clock.
    """

    def __init__(self, *, rng: random.Random | None = None) -> None:
        self.slept: list[float] = []
        self._now = 0.0
        super().__init__(sleep=self._fake_sleep, rng=rng, clock=lambda: self._now)
        self.calls: list[tuple[str, RateLimitSpec | None]] = []

    def _fake_sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._now += seconds

    def wait(self, host: str, spec: RateLimitSpec | None) -> float:
        self.calls.append((host, spec))
        return super().wait(host, spec)


def test_corpus_paces_live_fetch_per_url(tmp_path: Path) -> None:
    """Перед каждым живым фетчем — ``wait(host, plan.rate_limit)``; сон в границах spec."""
    spec = RateLimitSpec(min_s=1.0, max_s=2.0)
    defaults = CrawlRunConfig(strategy=CrawlStrategy.AUTO, rate_limit=spec)
    limiter = _RecordingLimiter(rng=random.Random(7))
    crawler = _Crawler({URL_P1_A: b"a", URL_P1_B: b"b"})

    _build(
        tmp_path,
        pages={},
        urls={"p1": [URL_P1_A, URL_P1_B]},
        crawler=crawler,
        crawl_defaults=defaults,
        rate_limiter=limiter,
    )

    # Один wait на каждый живой фетч; host нормализован (lower), spec — из FetchPlan.
    assert len(limiter.calls) == 2
    assert [host for host, _ in limiter.calls] == ["docs.test", "docs.test"]
    assert [s for _, s in limiter.calls] == [spec, spec]
    # Сон реально происходил в границах spec (детерминизм через seeded rng).
    assert len(limiter.slept) == 2
    assert all(1.0 <= delay <= 2.0 for delay in limiter.slept)


def test_corpus_pacing_no_sleep_when_rate_limit_absent(tmp_path: Path) -> None:
    """``plan.rate_limit is None`` → ``wait`` вызван, но сна нет (0.0)."""
    limiter = _RecordingLimiter()
    crawler = _Crawler({URL_P1_A: b"x"})

    _build(tmp_path, pages={}, urls={"p1": [URL_P1_A]}, crawler=crawler, rate_limiter=limiter)

    assert limiter.calls == [("docs.test", None)]
    assert limiter.slept == []


def test_corpus_pacing_not_applied_on_resume(tmp_path: Path) -> None:
    """Путь ``resume`` (retained raw) не пейсится: сетевого вызова нет (P2-3)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        # Первый прогон: живой фетч → пейсинг был.
        first_limiter = _RecordingLimiter()
        _build(
            tmp_path,
            pages={URL_P1_A: b"retained"},
            urls={"p1": [URL_P1_A]},
            store=store,
            run_id=RUN_ID,
            rate_limiter=first_limiter,
        )
        assert len(first_limiter.calls) == 1

        # Второй прогон: re-извлечение из retained raw (краулер-«бомба») → пейсинга нет.
        second_limiter = _RecordingLimiter()
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}),
            store=store,
            run_id=RUN_ID,
            rate_limiter=second_limiter,
        )
        assert second_limiter.calls == []
        assert second_limiter.slept == []
    finally:
        store.close()


def test_browser_guard_error_suggests_browser_crawler(tmp_path: Path) -> None:
    """Browser-guard подсказывает и ``scrapy``, и ``browser`` (P2-3)."""
    browser_url = "https://browser.test/landing"
    crawler = _Crawler({browser_url: b"x"})
    crawler.supports_browser = False  # type: ignore[attr-defined]

    result = _build(
        tmp_path,
        pages={},
        urls={"p1": [browser_url]},
        crawler=crawler,
        domain_profiles={"browser.test": DomainProfile(fetch=FetchMode.BROWSER)},
    )

    assert result.status == "ok"
    assert any("scrapy или browser" in err for err in result.errors)


# --------------------------------------------------------------------------- #
# Phase 3.3: executor-ветка (CrawlExecutorPort) — ingest через исполнителя
# --------------------------------------------------------------------------- #


class _ExecutorError(RuntimeError):
    """Локальная ошибка фейкового исполнителя (тест не импортирует адаптеры, A1)."""


class _FakeExecutor:
    """Фейковый ``CrawlExecutorPort``: отдаёт заранее заданные байты/ошибки по URL.

    ``statuses`` задаёт последовательность статусов опроса (последний повторяется);
    ``pages``/``errors``/``engines`` — исход по URL; ``final_url`` — пост-редиректный URL
    результата (P3-A2/A2a; ``None`` ⇒ ядро трактует как запрошенный ``url``). Журналирует
    submit/status/collect; ``submits`` — наборы работ **по раундам** (P3-A2).
    """

    version = "fake-executor-1"
    profile = "split"

    def __init__(
        self,
        *,
        pages: dict[str, bytes] | None = None,
        errors: dict[str, str] | None = None,
        engines: dict[str, str] | None = None,
        statuses: list[str] | None = None,
        collect_error: str | None = None,
        extra_items: list[CrawlResultItem] | None = None,
        accept_sequence: list[bool] | None = None,
        retry_after_s: float | None = None,
        final_url: dict[str, str] | None = None,
    ) -> None:
        self.pages = dict(pages or {})
        self.errors = dict(errors or {})
        self.engines = dict(engines or {})
        self.final_url = dict(final_url or {})
        self.statuses = list(statuses or ["completed"])
        self.collect_error = collect_error
        self.extra_items = list(extra_items or [])
        self.submits: list[list[CrawlWorkItem]] = []
        self.submit_kwargs: list[dict[str, Any]] = []
        self.status_calls: list[str] = []
        self.collect_cursors: list[str | None] = []
        self.accept_sequence = accept_sequence
        self.retry_after_s = retry_after_s
        self._status_i = 0
        self._accept_i = 0

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
        self.submit_kwargs.append(
            {
                "session_id": session_id,
                "run_id": run_id,
                "profile": profile,
                "idempotency_key": idempotency_key,
            }
        )
        if self.accept_sequence is None:
            accepted = True
        else:
            pos = min(self._accept_i, len(self.accept_sequence) - 1)
            accepted = self.accept_sequence[pos]
            self._accept_i += 1
        return CrawlSubmitResult(
            job_id="job-1" if accepted else "",
            accepted=accepted,
            retry_after_s=self.retry_after_s if not accepted else None,
        )

    def status(self, handle: str) -> CrawlProgress:
        value = self.statuses[min(self._status_i, len(self.statuses) - 1)]
        self._status_i += 1
        self.status_calls.append(value)
        return CrawlProgress(job_id=handle, status=value)

    def cancel(self, handle: str) -> CrawlProgress:
        return CrawlProgress(job_id=handle, status="cancelled")

    def collect(
        self, handle: str, *, cursor: str | None = None, limit: int | None = None
    ) -> CrawlBatch:
        self.collect_cursors.append(cursor)
        if self.collect_error is not None:
            raise _ExecutorError(self.collect_error)
        items = [self._result(work) for work in self.submits[-1]]
        items.extend(self.extra_items)
        return CrawlBatch(items=items, next_cursor=None, done=True)

    def _result(self, work: CrawlWorkItem) -> CrawlResultItem:
        engine = self.engines.get(work.url, work.engine)
        if work.url in self.errors:
            return CrawlResultItem(
                product_id=work.product_id,
                url=work.url,
                status="failed",
                engine=engine,
                kind=work.kind,
                error=self.errors[work.url],
            )
        content = self.pages.get(work.url, b"")
        return CrawlResultItem(
            product_id=work.product_id,
            url=work.url,
            status="fetched",
            engine=engine or "http",
            kind=work.kind,
            content_hash=content_hash(content),
            raw_bytes=content,
            final_url=self.final_url.get(work.url),
        )


def test_executor_ingests_marks_and_finalizes(tmp_path: Path) -> None:
    """Executor-ветка: ingest через исполнителя, сеанс помечен/финализирован, счётчики."""
    store = SqliteStore(":memory:")
    _make_run(store)
    executor = _FakeExecutor(pages={URL_P1_A: b"executor page"}, engines={URL_P1_A: "scrapy"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=_BombCrawler({}),  # локальный краулер НЕ должен вызываться
            executor=executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        build = _build_of(result)
        assert build.chunk_count == 1
        assert build.docs[0].content_hash == content_hash(b"executor page")
        item = next(iter(_items(store)))
        assert item.status is CrawlItemStatus.FETCHED
        assert item.engine == "scrapy"  # S1: применённый движок из результата шва
        assert item.raw_path and Path(item.raw_path).is_file()
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.COMPLETED
        assert (build.crawl_fetched, build.crawl_failed) == (1, 0)
        # Исполнитель получил ровно одну работу, с корректными метаданными подачи.
        assert len(executor.submits) == 1
        (work,) = executor.submits[0]
        assert (work.product_id, work.url) == ("p1", URL_P1_A)
        kwargs = executor.submit_kwargs[0]
        assert kwargs["session_id"] == SESSION_ID
        assert kwargs["run_id"] == RUN_ID
        assert kwargs["profile"] == "split"
        # B2: ключ идемпотентности — по набору работ, НЕ по сеансу.
        assert kwargs["idempotency_key"] != SESSION_ID
        assert kwargs["idempotency_key"] == _work_set_idempotency_key(
            executor.submits[0]
        )
    finally:
        store.close()


def test_executor_records_resolved_axes_in_work_items(tmp_path: Path) -> None:
    """Оси работы (engine/kind/fetch/extract/strategy) берутся из ``resolve_crawl_target``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    sources = CrawlSourcesConfig(
        sites={"p1": {"docs.test": CrawlSource(engine="scrapy", kind="web")}}
    )
    executor = _FakeExecutor(pages={URL_P1_A: b"x"})
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            crawl_sources=sources,
            store=store,
            run_id=RUN_ID,
        )
        (work,) = executor.submits[0]
        assert work.engine == "scrapy"
        assert work.kind == "web"
        assert work.fetch is FetchMode.HTTP
        assert work.extract is ExtractionRoute.AUTO
        assert work.strategy is CrawlStrategy.AUTO
    finally:
        store.close()


def test_executor_resume_skips_submission(tmp_path: Path) -> None:
    """Resume: URL с retained raw не подаётся — исполнитель не вызывается (как локально)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        first = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=_FakeExecutor(pages={URL_P1_A: b"retained"}),
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert _build_of(first).chunk_count == 1

        # Второй прогон: submit не должен вызываться (всё resum'ится из retained raw).
        second_executor = _FakeExecutor(pages={})
        second = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=second_executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert second.errors == []
        assert second_executor.submits == []
        assert _build_of(second).chunk_count == 1
        assert _build_of(second).index_build_id == _build_of(first).index_build_id
    finally:
        store.close()


def test_executor_max_attempts_excludes_url_from_work_set(tmp_path: Path) -> None:
    """Phase 4: исчерпавший попытки URL не подаётся исполнителю (work-set его исключает)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            executor=_FakeExecutor(pages={}, errors={URL_P1_A: "http_503"}),
            executor_poll_interval=0.0, executor_sleep=lambda _s: None,
            store=store, run_id=RUN_ID,
        )
        assert next(iter(_items(store))).attempts == 1

        second = _FakeExecutor(pages={URL_P1_A: b"x"})
        result = _build(
            tmp_path, pages={}, urls={"p1": [URL_P1_A]},
            executor=second, executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store, run_id=RUN_ID, max_attempts=1,
        )
        assert second.submits == []
        assert any("max_attempts" in err for err in result.errors)
        assert next(iter(_items(store))).error_kind == "attempts_exhausted"
    finally:
        store.close()


def test_executor_failure_marks_source_unavailable_and_partial(tmp_path: Path) -> None:
    """Сбойный результат исполнителя → элемент ``FAILED``, сеанс ``PARTIAL``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    executor = _FakeExecutor(pages={}, errors={URL_P1_A: "http_503"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        item = next(iter(_items(store)))
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("http_503" in err for err in result.errors)
        assert _build_of(result).crawl_failed == 1
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_executor_missing_result_marks_failed(tmp_path: Path) -> None:
    """Работа без результата (исполнитель не вернул item) → ``FAILED``."""
    store = SqliteStore(":memory:")
    _make_run(store)

    class _EmptyCollect(_FakeExecutor):
        def collect(self, handle, *, cursor=None, limit=None):  # type: ignore[override]
            self.collect_cursors.append(cursor)
            return CrawlBatch(items=[], next_cursor=None, done=True)

    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=_EmptyCollect(),
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        item = next(iter(_items(store)))
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("результат не получен" in err for err in result.errors)
    finally:
        store.close()


def test_executor_collect_error_is_isolated(tmp_path: Path) -> None:
    """Сбой ``collect`` изолируется: работа → ``FAILED``, фаза не падает."""
    store = SqliteStore(":memory:")
    _make_run(store)
    executor = _FakeExecutor(collect_error="edge down")
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert result.status == "ok"
        assert any("executor.collect" in err for err in result.errors)
        item = next(iter(_items(store)))
        assert item.status is CrawlItemStatus.FAILED
    finally:
        store.close()


def test_executor_submit_error_is_isolated(tmp_path: Path) -> None:
    """Сбой ``submit`` изолируется: работа → ``FAILED``, фаза не падает."""
    store = SqliteStore(":memory:")
    _make_run(store)

    class _BadSubmit(_FakeExecutor):
        def submit(self, items, *, session_id, run_id, profile, idempotency_key):  # type: ignore[override]
            raise _ExecutorError("submit boom")

    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=_BadSubmit(),
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert result.status == "ok"
        assert any("submit boom" in err for err in result.errors)
        assert next(iter(_items(store))).status is CrawlItemStatus.FAILED
    finally:
        store.close()


def test_executor_polls_until_terminal(tmp_path: Path) -> None:
    """Опрос продолжается, пока статус не-терминальный; пауза — через инжектированный sleep."""
    store = SqliteStore(":memory:")
    _make_run(store)
    slept: list[float] = []
    executor = _FakeExecutor(
        pages={URL_P1_A: b"x"},
        statuses=["pending", "running", "completed"],
    )
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            executor_poll_interval=0.5,
            executor_sleep=slept.append,
            store=store,
            run_id=RUN_ID,
        )
        assert executor.status_calls == ["pending", "running", "completed"]
        # Две паузы: перед вторым и третьим опросом (терминальный не пейсится).
        assert slept == [0.5, 0.5]
    finally:
        store.close()


def test_executor_collects_only_once_when_terminal(tmp_path: Path) -> None:
    """Уже терминальное задание не пейсится; сбор — одна страница (``done``)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    slept: list[float] = []
    executor = _FakeExecutor(pages={URL_P1_A: b"x"})
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            executor_poll_interval=0.5,
            executor_sleep=slept.append,
            store=store,
            run_id=RUN_ID,
        )
        assert slept == []
        assert executor.status_calls == ["completed"]
        assert executor.collect_cursors == [None]
    finally:
        store.close()


def test_executor_ignores_local_crawler_even_if_available(tmp_path: Path) -> None:
    """При наличии ``executor`` локальные ``crawler``/``crawlers`` не используются."""
    store = SqliteStore(":memory:")
    _make_run(store)
    local = _Crawler({URL_P1_A: b"local should not be used"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=local,
            crawlers={"http": local},
            executor=_FakeExecutor(pages={URL_P1_A: b"from executor"}),
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert local.calls == []
        assert _build_of(result).docs[0].content_hash == content_hash(b"from executor")
    finally:
        store.close()


def test_work_set_idempotency_key_is_content_based() -> None:
    """B2: ключ детерминирован по содержимому набора работ (порядко-независим, чувствителен)."""
    a = CrawlWorkItem(product_id="p1", url=URL_P1_A)
    b = CrawlWorkItem(product_id="p1", url=URL_P1_B)
    # Порядко-независимость: один и тот же набор → один ключ.
    assert _work_set_idempotency_key([a, b]) == _work_set_idempotency_key([b, a])
    # Чувствительность к содержимому набора: изменился набор → другой ключ.
    assert _work_set_idempotency_key([a, b]) != _work_set_idempotency_key([a])
    # Повторная подача идентичного набора идемпотентна (тот же ключ).
    assert _work_set_idempotency_key([a]) == _work_set_idempotency_key(
        [CrawlWorkItem(product_id="p1", url=URL_P1_A)]
    )


def test_executor_retry_after_partial_resume_new_key(tmp_path: Path) -> None:
    """B2: после частичного успеха повторная подача несёт только сбойные URL и новый ключ.

    Первый прогон: ``URL_P1_A`` добыт (retained raw), ``URL_P1_B`` сбойный (без raw). Второй
    прогон в том же сеансе: ``A`` resum'ится (не подаётся), ``B`` подаётся заново — набор
    работ изменился, значит и ``idempotency_key`` другой (edge создаст НОВОЕ задание, а не
    вернёт старое терминальное). Без фикса (ключ == session_id) ``B`` остался бы не добыт.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    first_executor = _FakeExecutor(pages={URL_P1_A: b"ok-a"}, errors={URL_P1_B: "boom"})
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A, URL_P1_B]},
            executor=first_executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        by_url = {i.url: i for i in _items(store)}
        assert by_url[URL_P1_A].status is CrawlItemStatus.FETCHED
        assert by_url[URL_P1_B].status is CrawlItemStatus.FAILED
        first_key = first_executor.submit_kwargs[0]["idempotency_key"]

        second_executor = _FakeExecutor(pages={URL_P1_B: b"ok-b"})
        _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A, URL_P1_B]},
            executor=second_executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert len(second_executor.submits) == 1
        (work,) = second_executor.submits[0]
        assert work.url == URL_P1_B  # только сбойный URL (A резюмирован из retained raw)
        second_key = second_executor.submit_kwargs[0]["idempotency_key"]
        assert second_key != first_key  # набор работ изменился → новое задание
        assert second_key == _work_set_idempotency_key(second_executor.submits[0])
        assert {i.url: i for i in _items(store)}[URL_P1_B].status is CrawlItemStatus.FETCHED
    finally:
        store.close()


def test_executor_retries_submit_on_backpressure(tmp_path: Path) -> None:
    """C2: ``accepted=false`` → ядро ждёт ``retry_after`` и повторяет подачу, пока не примут."""
    store = SqliteStore(":memory:")
    _make_run(store)
    slept: list[float] = []
    executor = _FakeExecutor(
        pages={URL_P1_A: b"x"},
        accept_sequence=[False, True],
        retry_after_s=0.25,
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            executor_poll_interval=0.0,
            executor_sleep=slept.append,
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert len(executor.submits) == 2  # отклонена, затем принята
        assert slept == [0.25]  # пауза по retry_after_s
        assert next(iter(_items(store))).status is CrawlItemStatus.FETCHED
    finally:
        store.close()


def test_executor_backpressure_exhausted_marks_failed(tmp_path: Path) -> None:
    """C2: persistent ``accepted=false`` → ограниченные попытки затем ясный ``FAILED``.

    Не тихий провал всего прогона: фаза завершается ``ok`` с изолированной ошибкой, а работы
    помечаются ``source_unavailable`` (результатов нет).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    executor = _FakeExecutor(pages={}, accept_sequence=[False])  # всегда отклоняет
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            executor_poll_interval=0.0,
            executor_sleep=lambda _s: None,
            store=store,
            run_id=RUN_ID,
        )
        assert result.status == "ok"
        assert len(executor.submits) == _SUBMIT_MAX_ATTEMPTS
        assert any("back-pressure" in err for err in result.errors)
        item = next(iter(_items(store)))
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Phase 5.2: источники-каталоги (feed/channel) — локальный fan-out
# --------------------------------------------------------------------------- #

FEED_URL = "https://docs.test/p1/feed"
ENTRY_1 = "https://docs.test/p1/entries/1"
ENTRY_2 = "https://docs.test/p1/entries/2"


class _FakePoller:
    """Детерминированный ``PollPort``-двойник: заданные записи/сбои/проблемы ``params``."""

    version = "poller-1"

    def __init__(
        self,
        entries: list[FeedEntry] | None = None,
        *,
        params_problems: list[str] | None = None,
        raising: Exception | None = None,
    ) -> None:
        self.entries = list(entries or ())
        self.params_problems = list(params_problems or ())
        self.raising = raising
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.validated: list[tuple[str, dict[str, Any]]] = []

    def validate_params(self, kind: str, params: dict[str, Any]) -> list[str]:
        self.validated.append((kind, dict(params)))
        return list(self.params_problems)

    def poll(
        self, url: str, *, kind: str, params: dict[str, Any], plan: FetchPlan
    ) -> list[FeedEntry]:
        self.calls.append((url, kind, dict(params)))
        if self.raising is not None:
            raise self.raising
        return list(self.entries)


def _feed_sources() -> CrawlSourcesConfig:
    return CrawlSourcesConfig(products={"p1": CrawlSource(engine="http", kind="feed")})


def test_directory_feed_fanout_fetches_retains_ingests(tmp_path: Path) -> None:
    """feed-каталог → записи фетчатся/ретейнятся/индексируются; сам каталог — не контент."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=ENTRY_1), FeedEntry(url=ENTRY_2)])
    crawler = _Crawler({ENTRY_1: b"entry one", ENTRY_2: b"entry two"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=crawler,
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        # Каталог поллится ровно один раз и краулером НЕ фетчится.
        assert [c[0] for c in poller.calls] == [FEED_URL]
        assert [u for u, _ in crawler.calls] == [ENTRY_1, ENTRY_2]
        build = _build_of(result)
        # Индексируются только две записи (каталог не даёт документа).
        assert build.chunk_count == 2
        assert len(build.docs) == 2
        assert {d.product_id for d in build.docs} == {"p1"}

        items = {i.url: i for i in _items(store)}
        feed_item = items[FEED_URL]
        assert feed_item.status is CrawlItemStatus.FETCHED
        assert not feed_item.raw_path  # каталог не ретейнится (не evidence)
        assert feed_item.kind == "feed"
        for entry_url in (ENTRY_1, ENTRY_2):
            it = items[entry_url]
            assert it.status is CrawlItemStatus.FETCHED
            assert it.raw_path and Path(it.raw_path).is_file()
            assert it.kind == "feed"
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.COMPLETED
    finally:
        store.close()


def test_directory_feed_url_not_retained(tmp_path: Path) -> None:
    """URL каталога не даёт ``CorpusDoc``/retention: только записи становятся контентом (I19)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=ENTRY_1)])
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=_Crawler({ENTRY_1: b"only entry"}),
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        build = _build_of(result)
        assert len(build.docs) == 1
        assert build.docs[0].content_hash == content_hash(b"only entry")
        # Ни один ретейнед-файл не соответствует байтам каталога.
        assert not (tmp_path / "p1" / f"{content_hash(b'feed bytes')}.bin").exists()
    finally:
        store.close()


def test_directory_poll_failure_marks_source_unavailable(tmp_path: Path) -> None:
    """Сбой поллинга → терминальный ``FAILED source_unavailable`` (не «тихо пусто»), ``PARTIAL``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller(raising=RuntimeError("feed down"))
    crawler = _Crawler({})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=crawler,
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert crawler.calls == []  # записей нет — фетча нет
        feed_item = {i.url: i for i in _items(store)}[FEED_URL]
        assert feed_item.status is CrawlItemStatus.FAILED
        assert feed_item.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert feed_item.raw_path in (None, "")
        assert any("poll" in err and "feed down" in err for err in result.errors)
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_directory_bad_params_marks_source_unavailable(tmp_path: Path) -> None:
    """Некорректные ``params`` (``validate_params``) → ``FAILED`` без вызова ``poll``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=ENTRY_1)], params_problems=["limit должен быть > 0"])
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=_Crawler({ENTRY_1: b"x"}),
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert poller.calls == []  # до поллинга дело не дошло
        feed_item = {i.url: i for i in _items(store)}[FEED_URL]
        assert feed_item.status is CrawlItemStatus.FAILED
        assert feed_item.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("limit должен быть > 0" in err for err in result.errors)
    finally:
        store.close()


def test_directory_missing_poller_marks_source_unavailable(tmp_path: Path) -> None:
    """kind-каталог без сконфигурированного поллера → ``FAILED source_unavailable``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=_Crawler({}),
            crawl_sources=_feed_sources(),
            pollers=None,
            store=store,
            run_id=RUN_ID,
        )
        feed_item = {i.url: i for i in _items(store)}[FEED_URL]
        assert feed_item.status is CrawlItemStatus.FAILED
        assert feed_item.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("поллер не сконфигурирован" in err for err in result.errors)
    finally:
        store.close()


def test_directory_entry_ssrf_dropped(tmp_path: Path) -> None:
    """Запись на приватный/метаданные-IP отбрасывается SSRF-политикой; безопасная — добывается."""
    store = SqliteStore(":memory:")
    _make_run(store)
    evil = "http://169.254.169.254/latest/meta-data"
    poller = _FakePoller([FeedEntry(url=evil), FeedEntry(url=ENTRY_1)])
    crawler = _Crawler({ENTRY_1: b"safe"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=crawler,
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert [u for u, _ in crawler.calls] == [ENTRY_1]
        assert any("SSRF" in err and evil in err for err in result.errors)
        urls = {i.url for i in _items(store)}
        assert evil not in urls
        assert ENTRY_1 in urls
    finally:
        store.close()


def test_directory_duplicate_entries_deduped(tmp_path: Path) -> None:
    """Дубли записей каталога дедуплицируются с сохранением порядка → один фетч."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller(
        [FeedEntry(url=ENTRY_1), FeedEntry(url=ENTRY_1), FeedEntry(url=ENTRY_2)]
    )
    crawler = _Crawler({ENTRY_1: b"one", ENTRY_2: b"two"})
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=crawler,
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert [u for u, _ in crawler.calls] == [ENTRY_1, ENTRY_2]
    finally:
        store.close()


def test_directory_entries_not_re_expanded(tmp_path: Path) -> None:
    """Один уровень развёртки: записи каталога повторно не расширяются (поллер зовётся раз)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=ENTRY_1)])
    crawler = _Crawler({ENTRY_1: b"one"})
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=crawler,
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        # Записи унаследовали kind="feed", но повторно не поллились.
        assert [c[0] for c in poller.calls] == [FEED_URL]
        assert [i.kind for i in _items(store) if i.url == ENTRY_1] == ["feed"]
    finally:
        store.close()


def test_non_directory_kind_unchanged(tmp_path: Path) -> None:
    """kind="web" (не каталог) обрабатывается как обычная страница; поллер не вызывается."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=ENTRY_1)])
    crawler = _Crawler({URL_P1_A: b"page"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            crawl_sources=CrawlSourcesConfig(
                products={"p1": CrawlSource(engine="http", kind="web")}
            ),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert poller.calls == []
        assert poller.validated == []
        assert [u for u, _ in crawler.calls] == [URL_P1_A]
        assert result.errors == []
    finally:
        store.close()


def test_pollers_none_web_unchanged(tmp_path: Path) -> None:
    """``pollers=None`` + kind="web" — прежний путь без обращения к поллерам."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _Crawler({URL_P1_A: b"page"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            crawl_sources=CrawlSourcesConfig(
                products={"p1": CrawlSource(engine="http", kind="web")}
            ),
            pollers=None,
            store=store,
            run_id=RUN_ID,
        )
        assert [u for u, _ in crawler.calls] == [URL_P1_A]
        assert result.errors == []
    finally:
        store.close()


def test_directory_max_attempts_exhausted_skips_poll(tmp_path: Path) -> None:
    """``max_attempts`` исчерпан на URL каталога → поллинг не выполняется, ``attempts_exhausted``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    failing = _FakePoller(raising=RuntimeError("down"))
    try:
        _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=_Crawler({}),
            crawl_sources=_feed_sources(),
            pollers={"feed": failing},
            store=store,
            run_id=RUN_ID,
        )
        prior = {i.url: i for i in _items(store)}[FEED_URL]
        assert prior.status is CrawlItemStatus.FAILED and prior.attempts == 1

        working = _FakePoller([FeedEntry(url=ENTRY_1)])
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=_Crawler({ENTRY_1: b"x"}),
            crawl_sources=_feed_sources(),
            pollers={"feed": working},
            store=store,
            run_id=RUN_ID,
            max_attempts=1,
        )
        assert working.calls == []  # исчерпано — поллинга нет
        assert any("max_attempts" in err for err in result.errors)
        row = {i.url: i for i in _items(store)}[FEED_URL]
        assert row.error_kind == "attempts_exhausted"
        assert row.attempts == 1
    finally:
        store.close()


def test_directory_entry_equal_to_catalog_url_skipped(tmp_path: Path) -> None:
    """N2: запись с URL == URL каталога (self-listing фид) не фетчится/не ретейнится повторно."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=FEED_URL), FeedEntry(url=ENTRY_1)])
    crawler = _Crawler({ENTRY_1: b"entry one"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [FEED_URL]},
            crawler=crawler,
            crawl_sources=_feed_sources(),
            pollers={"feed": poller},
            store=store,
            run_id=RUN_ID,
        )
        # Каталог-URL не подставляется как работа повторно: фетчится только запись.
        assert [u for u, _ in crawler.calls] == [ENTRY_1]
        items = {i.url: i for i in _items(store)}
        feed_item = items[FEED_URL]
        assert feed_item.status is CrawlItemStatus.FETCHED
        assert not feed_item.raw_path  # каталог не ретейнится
        assert items[ENTRY_1].status is CrawlItemStatus.FETCHED
        assert len(_build_of(result).docs) == 1
    finally:
        store.close()


def test_real_poller_catalog_bytes_not_retained_entry_retained(tmp_path: Path) -> None:
    """N3 (I19): реальный ``LocalFeedPoller`` — байты каталога НЕ ретейнятся, байты записи — да."""
    store = SqliteStore(":memory:")
    _make_run(store)
    rss = (
        b'<?xml version="1.0"?><rss version="2.0"><channel>'
        b"<item><title>E</title><link>https://docs.test/p1/entry-1</link></item>"
        b"</channel></rss>"
    )
    entry_url = "https://docs.test/p1/entry-1"
    entry_bytes = b"entry raw bytes"
    feed_url = "https://docs.test/p1/feed"
    crawler = _Crawler({feed_url: rss, entry_url: entry_bytes})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [feed_url]},
            crawler=crawler,
            crawl_sources=_feed_sources(),
            pollers={"feed": LocalFeedPoller(crawler)},
            store=store,
            run_id=RUN_ID,
        )
        items = {i.url: i for i in _items(store)}
        feed_item = items[feed_url]
        assert feed_item.status is CrawlItemStatus.FETCHED
        assert feed_item.raw_path is None  # каталог — не контент/evidence (I19)
        # Байты каталога отсутствуют под corpus_raw/<product>/.
        assert not (tmp_path / "p1" / f"{content_hash(rss)}.bin").exists()
        # Байты записи присутствуют и ретейнятся (I19).
        entry_bin = tmp_path / "p1" / f"{content_hash(entry_bytes)}.bin"
        assert entry_bin.exists() and entry_bin.read_bytes() == entry_bytes
        assert items[entry_url].raw_path and Path(items[entry_url].raw_path).is_file()
        build = _build_of(result)
        assert len(build.docs) == 1
        assert build.docs[0].content_hash == content_hash(entry_bytes)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Phase 5.3: executor-ветка (split) — развёртка каталогов волнами
# --------------------------------------------------------------------------- #

EXEC_FEED_URL = "https://feed.test/p1/rss"
EXEC_CHANNEL_URL = "https://feed.test/p1/list"
EXEC_E1 = "https://feed.test/p1/e1"
EXEC_E2 = "https://feed.test/p1/e2"


def _mixed_sources(kind: str) -> CrawlSourcesConfig:
    """Продукт — ``web``; хост ``feed.test`` — каталог ``kind`` (site-слой)."""
    return CrawlSourcesConfig(
        products={"p1": CrawlSource(engine="http", kind="web")},
        sites={"p1": {"feed.test": CrawlSource(kind=kind, engine="http")}},
    )


def _run_executor_build(tmp_path: Path, store, executor, **kwargs):  # noqa: ANN001
    """Вызов ``_build`` в executor-режиме с общими дефолтами (локальный краулер — «бомба»)."""
    return _build(
        tmp_path,
        pages={},
        crawler=_BombCrawler({}),
        executor=executor,
        executor_poll_interval=0.0,
        executor_sleep=lambda _s: None,
        store=store,
        run_id=RUN_ID,
        **kwargs,
    )


def test_executor_directory_feed_entries_each_own_root(tmp_path: Path) -> None:
    """P3-A2: feed-каталог → каждая запись — свой seed-root (свой раунд/``submit``)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=EXEC_E1), FeedEntry(url=EXEC_E2)])
    executor = _FakeExecutor(
        pages={URL_P1_A: b"top", EXEC_E1: b"one", EXEC_E2: b"two"}
    )
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        assert result.errors == []
        # Каждый корень дренится своим раундом (в порядке ``url_map``); записи каталога —
        # отдельные depth-0 корни. Сам каталог как страница НЕ подаётся.
        assert [[w.url for w in s] for s in executor.submits] == [
            [URL_P1_A],
            [EXEC_E1],
            [EXEC_E2],
        ]
        assert EXEC_FEED_URL not in {
            w.url for s in executor.submits for w in s
        }
        keys = [k["idempotency_key"] for k in executor.submit_kwargs]
        assert len(set(keys)) == 3  # разные наборы работ ⇒ разные ключи
        assert all(k["session_id"] == SESSION_ID for k in executor.submit_kwargs)

        items = {i.url: i for i in _items(store)}
        catalog = items[EXEC_FEED_URL]
        assert catalog.status is CrawlItemStatus.FETCHED
        assert not catalog.raw_path  # каталог не ретейнится (I19)
        assert catalog.kind == "feed"
        for eu in (EXEC_E1, EXEC_E2):
            it = items[eu]
            assert it.status is CrawlItemStatus.FETCHED
            assert it.raw_path and Path(it.raw_path).is_file()
            assert it.kind == "feed"  # провенанс записи = kind каталога
        # Записи добыты исполнителем и сохранены (I19), каталог — нет.
        assert (tmp_path / "p1" / f"{content_hash(b'one')}.bin").exists()
        assert (tmp_path / "p1" / f"{content_hash(b'two')}.bin").exists()
        assert len(_build_of(result).docs) == 3
        assert [c[0] for c in poller.calls] == [EXEC_FEED_URL]
    finally:
        store.close()


def test_executor_directory_channel_two_waves(tmp_path: Path) -> None:
    """Phase 5.3: ``channel``-каталог — та же волновая развёртка (kind провенанса = channel)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=EXEC_E1)])
    executor = _FakeExecutor(pages={URL_P1_A: b"top", EXEC_E1: b"ch"})
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_CHANNEL_URL]},
            crawl_sources=_mixed_sources("channel"),
            pollers={"channel": poller},
        )
        assert result.errors == []
        assert len(executor.submits) == 2
        assert [w.url for w in executor.submits[1]] == [EXEC_E1]
        items = {i.url: i for i in _items(store)}
        assert items[EXEC_CHANNEL_URL].status is CrawlItemStatus.FETCHED
        assert not items[EXEC_CHANNEL_URL].raw_path
        assert items[EXEC_E1].kind == "channel"
    finally:
        store.close()


def test_executor_directory_poll_failure_partial(tmp_path: Path) -> None:
    """Сбой опроса каталога → каталог ``FAILED source_unavailable``, без волны 2, ``PARTIAL``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller(raising=RuntimeError("feed down"))
    executor = _FakeExecutor(pages={URL_P1_A: b"top"})
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        # Волна 2 пуста → второй submit не выполняется.
        assert len(executor.submits) == 1
        assert [w.url for w in executor.submits[0]] == [URL_P1_A]
        catalog = {i.url: i for i in _items(store)}[EXEC_FEED_URL]
        assert catalog.status is CrawlItemStatus.FAILED
        assert catalog.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("poll" in e and "feed down" in e for e in result.errors)
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_executor_directory_missing_poller_partial(tmp_path: Path) -> None:
    """Каталог без сконфигурированного поллера → ``FAILED source_unavailable``, ``PARTIAL``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    executor = _FakeExecutor(pages={URL_P1_A: b"top"})
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers=None,
        )
        assert len(executor.submits) == 1
        catalog = {i.url: i for i in _items(store)}[EXEC_FEED_URL]
        assert catalog.status is CrawlItemStatus.FAILED
        assert catalog.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("поллер не сконфигурирован" in e for e in result.errors)
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_executor_directory_bad_params_partial(tmp_path: Path) -> None:
    """Некорректные ``params`` каталога → ``FAILED source_unavailable``, без опроса/волны 2."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=EXEC_E1)], params_problems=["limit недопустим"])
    executor = _FakeExecutor(pages={URL_P1_A: b"top"})
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        assert poller.calls == []  # до поллинга не дошло
        assert len(executor.submits) == 1
        catalog = {i.url: i for i in _items(store)}[EXEC_FEED_URL]
        assert catalog.status is CrawlItemStatus.FAILED
        assert any("limit недопустим" in e for e in result.errors)
    finally:
        store.close()


def test_executor_directory_entry_ssrf_dropped(tmp_path: Path) -> None:
    """Запись-каталога с приватным URL отбрасывается SSRF-политикой (не подаётся в волну 2)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    evil = "http://169.254.169.254/latest/meta-data"
    poller = _FakePoller([FeedEntry(url=evil), FeedEntry(url=EXEC_E1)])
    executor = _FakeExecutor(pages={URL_P1_A: b"top", EXEC_E1: b"safe"})
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        assert [w.url for w in executor.submits[1]] == [EXEC_E1]
        assert any("SSRF" in e and evil in e for e in result.errors)
        assert evil not in {i.url for i in _items(store)}
    finally:
        store.close()


def test_executor_directory_self_referential_entry_skipped(tmp_path: Path) -> None:
    """N2: запись с URL == URL каталога не подаётся в волну 2 (self-listing фид)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=EXEC_FEED_URL), FeedEntry(url=EXEC_E1)])
    executor = _FakeExecutor(pages={URL_P1_A: b"top", EXEC_E1: b"one"})
    try:
        _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        assert [w.url for w in executor.submits[1]] == [EXEC_E1]
        catalog = {i.url: i for i in _items(store)}[EXEC_FEED_URL]
        assert catalog.status is CrawlItemStatus.FETCHED
        assert not catalog.raw_path
    finally:
        store.close()


def test_executor_directory_entry_resume_not_submitted(tmp_path: Path) -> None:
    """Запись с retained raw (prior ``FETCHED``) не подаётся: ре-ингест из raw (I19)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=EXEC_E1)])
    try:
        first = _run_executor_build(
            tmp_path,
            store,
            _FakeExecutor(pages={EXEC_E1: b"entry bytes"}),
            urls={"p1": [EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        assert _build_of(first).chunk_count == 1
        assert {i.url: i for i in _items(store)}[EXEC_E1].raw_path

        # Повторный прогон: запись resum'ится, поллер всё ещё вызывается, submit'ов нет.
        second_poller = _FakePoller([FeedEntry(url=EXEC_E1)])
        second_executor = _FakeExecutor(pages={})
        second = _run_executor_build(
            tmp_path,
            store,
            second_executor,
            urls={"p1": [EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": second_poller},
        )
        assert second.errors == []
        assert second_executor.submits == []  # волна 1 пуста, запись resum'ится
        assert [c[0] for c in second_poller.calls] == [EXEC_FEED_URL]
        assert _build_of(second).chunk_count == 1
        assert _build_of(second).index_build_id == _build_of(first).index_build_id
    finally:
        store.close()


def test_executor_directory_entry_max_attempts_not_submitted(tmp_path: Path) -> None:
    """Phase 4: исчерпавшая попытки запись каталога не подаётся — терминальный ``FAILED``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=EXEC_E1)])
    try:
        first = _run_executor_build(
            tmp_path,
            store,
            _FakeExecutor(pages={EXEC_E1: b"entry"}),
            urls={"p1": [EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        assert _build_of(first).chunk_count == 1

        # Имитируем запись, исчерпавшую попытки (2 >= max_attempts; raw убран → не resume).
        prior = {i.url: i for i in _items(store)}[EXEC_E1]
        store.upsert_crawl_session_item(
            prior.model_copy(
                update={
                    "status": CrawlItemStatus.FAILED,
                    "attempts": 2,
                    "raw_path": None,
                    "error_kind": FailureKind.SOURCE_UNAVAILABLE.value,
                }
            )
        )

        second_executor = _FakeExecutor(pages={EXEC_E1: b"x"})
        result = _run_executor_build(
            tmp_path,
            store,
            second_executor,
            urls={"p1": [EXEC_FEED_URL]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": _FakePoller([FeedEntry(url=EXEC_E1)])},
            max_attempts=2,
        )
        assert second_executor.submits == []  # исчерпано — волна 2 пуста
        assert any("max_attempts" in e for e in result.errors)
        assert {i.url: i for i in _items(store)}[EXEC_E1].error_kind == "attempts_exhausted"
    finally:
        store.close()


def test_executor_top_level_max_attempts_exhausted_not_submitted(tmp_path: Path) -> None:
    """Phase 4: исчерпавший попытки top-level URL не попадает в волну 1 (не подаётся)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        _run_executor_build(
            tmp_path,
            store,
            _FakeExecutor(pages={}, errors={URL_P1_A: "http_503"}),
            urls={"p1": [URL_P1_A]},
        )
        assert next(iter(_items(store))).attempts == 1

        second_executor = _FakeExecutor(pages={URL_P1_A: b"x"})
        result = _run_executor_build(
            tmp_path,
            store,
            second_executor,
            urls={"p1": [URL_P1_A]},
            max_attempts=1,
        )
        assert second_executor.submits == []
        assert any("max_attempts" in e for e in result.errors)
        assert next(iter(_items(store))).error_kind == "attempts_exhausted"
    finally:
        store.close()


def test_executor_root_order_matches_local(tmp_path: Path) -> None:
    """P3-A2: корни дренируются в порядке ``url_map`` — ingest-порядок = локальный.

    URL сортируются лексикографически; ``zzz.test`` идёт после ``feed.test``, поэтому
    записи каталога обязаны встать МЕЖДУ top-level URL (в месте каталога). Однопутевой
    product-цикл делает это по построению — трюк с interleaving больше не нужен.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    zzz = "https://zzz.test/p1/page"
    poller = _FakePoller([FeedEntry(url=EXEC_E1), FeedEntry(url=EXEC_E2)])
    executor = _FakeExecutor(
        pages={URL_P1_A: b"top", zzz: b"zzz", EXEC_E1: b"one", EXEC_E2: b"two"}
    )
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A, EXEC_FEED_URL, zzz]},
            crawl_sources=_mixed_sources("feed"),
            pollers={"feed": poller},
        )
        assert result.errors == []
        # Каждый корень — свой раунд, в порядке ``url_map`` (docs < feed < zzz); записи
        # каталога разворачиваются на месте каталога.
        assert [[w.url for w in s] for s in executor.submits] == [
            [URL_P1_A],
            [EXEC_E1],
            [EXEC_E2],
            [zzz],
        ]
        # Порядок документов = порядок ingest: top, e1, e2, zzz (записи на месте каталога).
        order = [d.content_hash for d in _build_of(result).docs]
        assert order == [
            content_hash(b"top"),
            content_hash(b"one"),
            content_hash(b"two"),
            content_hash(b"zzz"),
        ]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# D3: per-site sitemap (urlset/gzip/sitemapindex; seeds; блэклист/бюджет/SSRF)
# --------------------------------------------------------------------------- #

SITEMAP_URL = "https://docs.test/p1/sitemap.xml"
SITEMAP_CHILD = "https://docs.test/p1/sm-child.xml"
SITEMAP_LOC_1 = "https://docs.test/p1/loc-1"
SITEMAP_LOC_2 = "https://docs.test/p1/loc-2"

#: Seed-корень **каталогом** (``/p1/``): его path-scope — ``/p1``, поэтому ``<loc>``-соседи
#: ``/p1/loc-*`` в scope. Раньше здесь стоял страничный seed ``/p1/index``; после фикса
#: дефекта B (``<loc>`` подчинены scope корня, а не задают его) такой seed законно отсекал
#: соседей — страница не является каталогом. Все реальные seeds пакета — каталоги (``/``).
SITEMAP_ROOT = "https://docs.test/p1/"


def _sitemap_profile(**overrides: Any) -> dict[str, DomainProfile]:
    """Per-site профиль ``docs.test`` с заданным sitemap (и опциональными осями D1/D2)."""
    return {"docs.test": DomainProfile(sitemap=SITEMAP_URL, **overrides)}


def _urlset(*locs: str) -> bytes:
    body = b"".join(f"<url><loc>{loc}</loc></url>".encode() for loc in locs)
    return b"<urlset>" + body + b"</urlset>"


def test_sitemap_locs_become_depth0_seeds_and_sitemap_not_retained(tmp_path: Path) -> None:
    """``<loc>`` — дополнительные depth-0 seeds; сам sitemap не ретейнится/не ингестится."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1), FeedEntry(url=SITEMAP_LOC_2)])
    crawler = _Crawler(
        {
            SITEMAP_ROOT: b"seed page",
            SITEMAP_LOC_1: b"loc one",
            SITEMAP_LOC_2: b"loc two",
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        # sitemap опрошен один раз; страницы (seed + locs) — краулером.
        assert [c[0] for c in poller.calls] == [SITEMAP_URL]
        assert [u for u, _ in crawler.calls] == [SITEMAP_ROOT, SITEMAP_LOC_1, SITEMAP_LOC_2]
        build = _build_of(result)
        assert build.chunk_count == 3
        assert {d.url for d in build.docs} == {SITEMAP_ROOT, SITEMAP_LOC_1, SITEMAP_LOC_2}

        items = {i.url: i for i in _items(store)}
        sitemap_item = items[SITEMAP_URL]
        assert sitemap_item.status is CrawlItemStatus.FETCHED
        assert not sitemap_item.raw_path  # не страница → нет retained байтов (I19)
        assert sitemap_item.kind == "sitemap"
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.COMPLETED
    finally:
        store.close()


def test_sitemap_gzip_via_local_poller(tmp_path: Path) -> None:
    """gzip ``sitemap.xml.gz`` распознаётся по magic-байтам (реальный ``LocalFeedPoller``)."""
    import gzip as _gzip

    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _Crawler(
        {
            SITEMAP_ROOT: b"seed",
            SITEMAP_URL: _gzip.compress(_urlset(SITEMAP_LOC_1)),
            SITEMAP_LOC_1: b"loc",
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": LocalFeedPoller(crawler)},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert {d.url for d in _build_of(result).docs} == {SITEMAP_ROOT, SITEMAP_LOC_1}
    finally:
        store.close()


def test_sitemap_index_one_level_via_local_poller(tmp_path: Path) -> None:
    """``sitemapindex`` разворачивается на один уровень; вложенный индекс — отказ (изолирован)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    index = (
        b"<sitemapindex><sitemap><loc>" + SITEMAP_CHILD.encode() + b"</loc></sitemap></sitemapindex>"
    )
    crawler = _Crawler(
        {
            SITEMAP_ROOT: b"seed",
            SITEMAP_URL: index,
            SITEMAP_CHILD: index,  # дочерний — сам индекс → глубже одного уровня
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": LocalFeedPoller(crawler)},
            store=store,
            run_id=RUN_ID,
        )
        assert any("вложенный" in e for e in result.errors)
        items = {i.url: i for i in _items(store)}
        assert items[SITEMAP_URL].status is CrawlItemStatus.FAILED
        # seed оператора всё равно добыт (аугментация, не замена).
        assert SITEMAP_ROOT in {d.url for d in _build_of(result).docs}
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_sitemap_poller_entry_cap_enforced(tmp_path: Path) -> None:
    """Потолок ``max_entries`` поллера ограничивает число ``<loc>``-seeds."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _Crawler(
        {
            SITEMAP_ROOT: b"seed",
            SITEMAP_URL: _urlset(SITEMAP_LOC_1, SITEMAP_LOC_2),
            SITEMAP_LOC_1: b"a",
            SITEMAP_LOC_2: b"b",
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": LocalFeedPoller(crawler, max_entries=1)},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert {d.url for d in _build_of(result).docs} == {SITEMAP_ROOT, SITEMAP_LOC_1}
    finally:
        store.close()


def test_sitemap_loc_ssrf_rejected(tmp_path: Path) -> None:
    """Небезопасный ``<loc>`` (приватный IP) отбрасывается до становления seed."""
    store = SqliteStore(":memory:")
    _make_run(store)
    unsafe = "http://127.0.0.1/secret"
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1), FeedEntry(url=unsafe)])
    crawler = _Crawler({SITEMAP_ROOT: b"seed", SITEMAP_LOC_1: b"ok"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert any("sitemap <loc>" in e for e in result.errors)
        assert unsafe not in [u for u, _ in crawler.calls]
        assert {d.url for d in _build_of(result).docs} == {SITEMAP_ROOT, SITEMAP_LOC_1}
    finally:
        store.close()


def test_sitemap_loc_obeys_blacklist(tmp_path: Path) -> None:
    """``<loc>`` подчиняется блэклисту профиля (``exclude_documents``) как обычный seed."""
    store = SqliteStore(":memory:")
    _make_run(store)
    pdf = "https://docs.test/p1/private/report.pdf"
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1), FeedEntry(url=pdf)])
    crawler = _Crawler({SITEMAP_ROOT: b"seed", SITEMAP_LOC_1: b"ok"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(exclude_documents=["*.pdf"]),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert any("блэклист" in e for e in result.errors)
        assert pdf not in [u for u, _ in crawler.calls]
        assert {d.url for d in _build_of(result).docs} == {SITEMAP_ROOT, SITEMAP_LOC_1}
    finally:
        store.close()


def test_sitemap_seeds_obey_max_documents(tmp_path: Path) -> None:
    """``max_documents`` — жёсткий потолок и для sitemap-seeds (тихая штатная остановка)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1), FeedEntry(url=SITEMAP_LOC_2)])
    crawler = _Crawler(
        {SITEMAP_ROOT: b"seed", SITEMAP_LOC_1: b"a", SITEMAP_LOC_2: b"b"}
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(max_documents=1),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        build = _build_of(result)
        assert {d.url for d in build.docs} == {SITEMAP_ROOT}
        assert build.truncated is True
        assert any(rec["truncated_reason"] == "max_documents" for rec in build.truncation)
    finally:
        store.close()


#: F13: host-корень и вложенный path-корень — общий durable-URL лежит в scope обоих.
HOST_SCOPE_ROOT = "https://docs.test/"
PATH_SCOPE_ROOT = "https://docs.test/p1/"
SHARED_DURABLE = "https://docs.test/p1/shared"


def test_sitemap_multi_root_resume_budget_counts_shared_url_once(tmp_path: Path) -> None:
    """F13 (multi-root, секция sitemap/бюджет): URL в scope двух корней заряжает
    ``max_documents`` **один раз** (продукт).

    Раньше ``consumed`` считался по каждому корню отдельно: URL, лежащий в scope host-корня
    и вложенного path-корня, заряжался обоим, и на resume бюджет вложенного корня
    исчерпывался досрочно (ложный ``truncated``). Теперь первый корень-претендент помечает
    URL, последующие его не пересчитывают; fresh-путь (``note_fetched``) не изменён.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        # Первый прогон: host-корень обходит ссылку /p1/shared → durable-элементы обоих.
        _build(
            tmp_path,
            pages={},
            urls={"p1": [HOST_SCOPE_ROOT]},
            crawler=_Crawler(
                {
                    HOST_SCOPE_ROOT: f'<a href="{SHARED_DURABLE}">x</a>'.encode(),
                    SHARED_DURABLE: b"shared",
                }
            ),
            domain_profiles={"docs.test": DomainProfile(max_depth=1)},
            store=store,
            run_id=RUN_ID,
        )
        assert {i.url for i in _items(store)} >= {HOST_SCOPE_ROOT, SHARED_DURABLE}

        # Второй прогон: host-корень идёт первым (сортировка URL) и забирает оба durable-URL;
        # вложенному корню бюджет не заряжается повторно.
        crawler = _Crawler({PATH_SCOPE_ROOT: b"path seed"})
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [PATH_SCOPE_ROOT, HOST_SCOPE_ROOT]},
            crawler=crawler,
            domain_profiles={
                "docs.test": DomainProfile(max_documents=5),
                "docs.test/p1": DomainProfile(max_documents=2),
            },
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        records = {rec["root"]: rec for rec in _build_of(result).truncation}
        # Host-корень заряжен обоими durable-URL (2 < 5) — не усечён.
        assert records[HOST_SCOPE_ROOT]["pages_fetched"] == 2
        assert records[HOST_SCOPE_ROOT]["truncated"] is False
        # Вложенный корень: 0 заряженных (URL уже учтён продуктом) + 1 свежий seed.
        assert records[PATH_SCOPE_ROOT]["pages_fetched"] == 1
        assert records[PATH_SCOPE_ROOT]["truncated"] is False
        assert [u for u, _ in crawler.calls] == [PATH_SCOPE_ROOT]
        assert PATH_SCOPE_ROOT in {d.url for d in _build_of(result).docs}
    finally:
        store.close()


def test_sitemap_absent_behaviour_unchanged(tmp_path: Path) -> None:
    """Без ``sitemap`` в профиле поллер не вызывается и ошибок нет (поведение неизменно)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller(raising=RuntimeError("не должен вызываться"))
    try:
        result = _build(
            tmp_path,
            pages={SITEMAP_ROOT: b"only seed"},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=_Crawler({SITEMAP_ROOT: b"only seed"}),
            domain_profiles={"docs.test": DomainProfile()},
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert poller.calls == []
        assert {d.url for d in _build_of(result).docs} == {SITEMAP_ROOT}
    finally:
        store.close()


def test_sitemap_without_poller_records_error(tmp_path: Path) -> None:
    """``sitemap`` задан, но поллер не сконфигурирован → ``FAILED source_unavailable``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [SITEMAP_ROOT]},
            crawler=_Crawler({SITEMAP_ROOT: b"seed"}),
            domain_profiles=_sitemap_profile(),
            pollers=None,
            store=store,
            run_id=RUN_ID,
        )
        assert any("поллер sitemap не сконфигурирован" in e for e in result.errors)
        items = {i.url: i for i in _items(store)}
        assert items[SITEMAP_URL].status is CrawlItemStatus.FAILED
        assert SITEMAP_ROOT in {d.url for d in _build_of(result).docs}
    finally:
        store.close()


def test_sitemap_domain_profile_rejects_unknown_field() -> None:
    """``extra="forbid"`` сохранён: неизвестное поле профиля (и ``sitemap``-опечатки) — отказ."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        DomainProfile(sitemaps="https://docs.test/sitemap.xml")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        DomainProfile(sitemap="docs.test/sitemap.xml")  # нет схемы http(s)
    assert DomainProfile(sitemap="https://docs.test/sitemap.xml").sitemap == (
        "https://docs.test/sitemap.xml"
    )


def test_executor_sitemap_locs_share_root_frontier(tmp_path: Path) -> None:
    """P3-A2: ``<loc>`` sitemap — ``extra_seeds`` того же корня (один раунд, общий бюджет)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1)])
    executor = _FakeExecutor(pages={SITEMAP_ROOT: b"top", SITEMAP_LOC_1: b"loc"})
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SITEMAP_ROOT]},
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": poller},
        )
        assert result.errors == []
        # Seed оператора и ``<loc>`` делят один фронтир → один раунд (как в local).
        assert [[w.url for w in s] for s in executor.submits] == [
            [SITEMAP_ROOT, SITEMAP_LOC_1]
        ]
        assert {d.url for d in _build_of(result).docs} == {SITEMAP_ROOT, SITEMAP_LOC_1}
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# P3-A2: протокол раундов executor-транспорта (единый drain-алгоритм local/split)
# --------------------------------------------------------------------------- #

SPLIT_ROOT = "https://split.test/"
SPLIT_D1A = "https://split.test/a"
SPLIT_D1B = "https://split.test/b"
SPLIT_D1C = "https://split.test/c"
SPLIT_D2A = "https://split.test/a/1"
SPLIT_D2B = "https://split.test/b/1"
SPLIT_BLOCKED = "https://split.test/blocked"
SPLIT_REDIR = "https://split.test/redir"
SPLIT_OFFSITE = "https://other.test/x"


def _split_profile(**overrides: Any) -> dict[str, DomainProfile]:
    """Per-site профиль ``split.test`` с заданными осями контроля краула (D1/D2)."""
    return {"split.test": DomainProfile(**overrides)}


def test_split_bfs_rounds_until_frontier_drained(tmp_path: Path) -> None:
    """P3-A2 (якорь): сайт глубины 2 — обнаруженные URL в отдельных раундах, терминация.

    Раунд 1 — seed (depth 0); раунд 2 — depth-1; раунд 3 — depth-2; затем фронтир пуст.
    Падает на старом коде (отказ ``max_depth>0``) и на наивной «третьей статической волне»
    (depth-2 фетчились бы в раунде depth-1).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    pages = {
        SPLIT_ROOT: b'<a href="/a">a</a><a href="/b">b</a>',
        SPLIT_D1A: b'<a href="/a/1">a1</a>',
        SPLIT_D1B: b'<a href="/b/1">b1</a>',
        SPLIT_D2A: b"leaf a1",
        SPLIT_D2B: b"leaf b1",
    }
    executor = _FakeExecutor(pages=pages)
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=2),
        )
        assert result.errors == []
        assert [[w.url for w in s] for s in executor.submits] == [
            [SPLIT_ROOT],
            [SPLIT_D1A, SPLIT_D1B],
            [SPLIT_D2A, SPLIT_D2B],
        ]
        assert {d.url for d in _build_of(result).docs} == {
            SPLIT_ROOT,
            SPLIT_D1A,
            SPLIT_D1B,
            SPLIT_D2A,
            SPLIT_D2B,
        }
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.COMPLETED
    finally:
        store.close()


def test_split_round_respects_max_documents_across_rounds(tmp_path: Path) -> None:
    """P3-A2/§2: ``max_documents`` зажимает размер раунда; пере-удержание невозможно."""
    store = SqliteStore(":memory:")
    _make_run(store)
    pages = {
        SPLIT_ROOT: (
            b'<a href="/a">a</a><a href="/b">b</a><a href="/c">c</a>'
        ),
        SPLIT_D1A: b"leaf a",
        SPLIT_D1B: b"leaf b",
        SPLIT_D1C: b"leaf c",
    }
    executor = _FakeExecutor(pages=pages)
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=1, max_documents=2),
        )
        # Ровно два фетча: seed + один depth-1; дальше ёмкость исчерпана (тихая остановка).
        assert [[w.url for w in s] for s in executor.submits] == [
            [SPLIT_ROOT],
            [SPLIT_D1A],
        ]
        build = _build_of(result)
        assert len(build.docs) == 2
        assert build.truncated is True
        assert any(
            rec["truncated_reason"] == "max_documents" for rec in build.truncation
        )
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_split_round_budget_valve_truncates(tmp_path: Path, monkeypatch: Any) -> None:
    """P3-A2/§1(iii): предохранитель раундов усекает ``round_budget``, не молчит."""
    monkeypatch.setattr(
        "core.pipeline.corpus._EXECUTOR_MAX_WAVES_PER_ROOT", 1
    )
    store = SqliteStore(":memory:")
    _make_run(store)
    pages = {SPLIT_ROOT: b'<a href="/a">a</a>', SPLIT_D1A: b"leaf a"}
    executor = _FakeExecutor(pages=pages)
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=1),
        )
        assert len(executor.submits) == 1  # второй раунд остановлен клапаном
        build = _build_of(result)
        assert build.truncated is True
        assert any(
            rec["truncated_reason"] == "round_budget" for rec in build.truncation
        )
        assert any("round_budget" in e for e in result.errors)
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_split_resume_charges_budget_once(tmp_path: Path) -> None:
    """P3-A2/B2: resume-элементы заряжают бюджет корня; повторного ``submit`` нет."""
    store = SqliteStore(":memory:")
    _make_run(store)
    profile = _split_profile(max_depth=1, max_documents=2)
    pages = {
        SPLIT_ROOT: (
            b'<a href="/a">a</a><a href="/b">b</a><a href="/c">c</a>'
        ),
        SPLIT_D1A: b"leaf a",
        SPLIT_D1B: b"leaf b",
        SPLIT_D1C: b"leaf c",
    }
    try:
        first = _run_executor_build(
            tmp_path,
            store,
            _FakeExecutor(pages=pages),
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=profile,
        )
        assert len(_build_of(first).docs) == 2  # бюджет исчерпан (root + /a)
        first_items = {i.url: i for i in _items(store)}
        assert first_items[SPLIT_ROOT].raw_path
        assert first_items[SPLIT_D1A].raw_path

        second_executor = _FakeExecutor(pages=pages)
        second = _run_executor_build(
            tmp_path,
            store,
            second_executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=profile,
        )
        assert second_executor.submits == []  # бюджет заряжен resume-элементами
        assert any("max_documents" in e for e in second.errors)
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_split_blacklist_and_scope_enforced(tmp_path: Path) -> None:
    """P3-A2/§5: блэклист и scope применяются к обнаруженным URL (не только в local)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    pages = {
        SPLIT_ROOT: (
            b'<a href="/blocked">b</a><a href="/a">a</a>'
            b'<a href="https://other.test/x">x</a>'
        ),
        SPLIT_D1A: b"leaf a",
    }
    executor = _FakeExecutor(pages=pages)
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=1, exclude_paths=["/blocked"]),
        )
        assert result.errors == []
        # Заблэклистенный и вне-scope URL не подаются; только seed и легитимная ссылка.
        assert [[w.url for w in s] for s in executor.submits] == [
            [SPLIT_ROOT],
            [SPLIT_D1A],
        ]
        assert {d.url for d in _build_of(result).docs} == {SPLIT_ROOT, SPLIT_D1A}
        record = _build_of(result).truncation[0]
        assert SPLIT_BLOCKED in record["skipped_blacklisted"]
    finally:
        store.close()


def test_split_redirect_scope_uses_final_url(tmp_path: Path) -> None:
    """P3-A2/A2a/§4: off-host ``final_url`` → ``FAILED redirect_out_of_scope``, без ретейна."""
    store = SqliteStore(":memory:")
    _make_run(store)
    pages = {
        SPLIT_ROOT: b'<a href="/redir">r</a>',
        SPLIT_REDIR: b'<a href="/should-not-fetch">x</a>',
    }
    executor = _FakeExecutor(
        pages=pages, final_url={SPLIT_REDIR: SPLIT_OFFSITE}
    )
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=1),
        )
        assert [[w.url for w in s] for s in executor.submits] == [
            [SPLIT_ROOT],
            [SPLIT_REDIR],
        ]
        # Финальный URL ушёл из scope → страница не ретейнится и ссылки не расширяются.
        assert {d.url for d in _build_of(result).docs} == {SPLIT_ROOT}
        item = {i.url: i for i in _items(store)}[SPLIT_REDIR]
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == "redirect_out_of_scope"
        assert any("redirect вышел из scope" in e for e in result.errors)
    finally:
        store.close()


def test_split_round_idempotency_keys_distinct_per_round_and_session_stable(
    tmp_path: Path,
) -> None:
    """P3-A2/B2: ключ идемпотентности — по набору работ раунда; ``session_id`` стабилен."""
    store = SqliteStore(":memory:")
    _make_run(store)
    pages = {
        SPLIT_ROOT: b'<a href="/a">a</a><a href="/b">b</a>',
        SPLIT_D1A: b'<a href="/a/1">a1</a>',
        SPLIT_D1B: b'<a href="/b/1">b1</a>',
        SPLIT_D2A: b"leaf a1",
        SPLIT_D2B: b"leaf b1",
    }
    executor = _FakeExecutor(pages=pages)
    try:
        _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=2),
        )
        keys = [k["idempotency_key"] for k in executor.submit_kwargs]
        assert len(set(keys)) == len(keys)  # разные наборы работ ⇒ разные ключи
        for work, kwargs in zip(executor.submits, executor.submit_kwargs):
            assert kwargs["idempotency_key"] == _work_set_idempotency_key(work)
            assert kwargs["session_id"] == SESSION_ID
    finally:
        store.close()


def test_split_backpressure_bisects_and_singleton_fails_honestly(
    tmp_path: Path,
) -> None:
    """P3-A2/§3: отвергнутая волна делится пополам; отвергнутый одиночка — честный FAILED."""
    store = SqliteStore(":memory:")
    _make_run(store)

    class _RejectLargeWaves(_FakeExecutor):
        """Принимает одиночные работы; волну из >1 — отклоняет (back-pressure)."""

        def submit(  # type: ignore[override]
            self, items, *, session_id, run_id, profile, idempotency_key
        ) -> CrawlSubmitResult:
            self.submits.append(list(items))
            self.submit_kwargs.append(
                {
                    "session_id": session_id,
                    "run_id": run_id,
                    "profile": profile,
                    "idempotency_key": idempotency_key,
                }
            )
            if len(items) > 1:
                return CrawlSubmitResult(job_id="", accepted=False, retry_after_s=None)
            return CrawlSubmitResult(job_id="job-1", accepted=True, retry_after_s=None)

    pages = {
        SPLIT_ROOT: b'<a href="/a">a</a><a href="/b">b</a>',
        SPLIT_D1A: b"leaf a",
        SPLIT_D1B: b"leaf b",
    }
    executor = _RejectLargeWaves(pages=pages)
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=1),
        )
        assert any("бисекция" in e for e in result.errors)
        # Обе половины добыты: волна из двух отвергнута, затем принята по частям.
        assert {d.url for d in _build_of(result).docs} == {
            SPLIT_ROOT,
            SPLIT_D1A,
            SPLIT_D1B,
        }
        items = {i.url: i for i in _items(store)}
        assert items[SPLIT_D1A].status is CrawlItemStatus.FETCHED
        assert items[SPLIT_D1B].status is CrawlItemStatus.FETCHED
    finally:
        store.close()

    # Синглтон, отвергнутый после всех ретраев → per-URL FAILED, сеанс PARTIAL (не тихо).
    store2 = SqliteStore(":memory:")
    _make_run(store2)
    try:
        result = _run_executor_build(
            tmp_path,
            store2,
            _FakeExecutor(pages={}, accept_sequence=[False]),
            urls={"p1": [SPLIT_ROOT]},
        )
        item = next(iter(_items(store2)))
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("back-pressure" in e for e in result.errors)
        session = store2.get_crawl_session(SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store2.close()


def test_split_missing_result_marks_failed(tmp_path: Path) -> None:
    """P3-A2/§4: работа без результата в раунде → ``FAILED source_unavailable`` (не тихо)."""
    store = SqliteStore(":memory:")
    _make_run(store)

    class _PartialCollect(_FakeExecutor):
        """Отдаёт результаты только для части работ волны (``/b`` пропадает)."""

        def collect(self, handle, *, cursor=None, limit=None):  # type: ignore[override]
            self.collect_cursors.append(cursor)
            items = [
                self._result(w)
                for w in self.submits[-1]
                if w.url != SPLIT_D1B
            ]
            return CrawlBatch(items=items, next_cursor=None, done=True)

    pages = {
        SPLIT_ROOT: b'<a href="/a">a</a><a href="/b">b</a>',
        SPLIT_D1A: b"leaf a",
        SPLIT_D1B: b"leaf b",
    }
    executor = _PartialCollect(pages=pages)
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=1),
        )
        items = {i.url: i for i in _items(store)}
        assert items[SPLIT_D1A].status is CrawlItemStatus.FETCHED
        assert items[SPLIT_D1B].status is CrawlItemStatus.FAILED
        assert items[SPLIT_D1B].error_kind == FailureKind.SOURCE_UNAVAILABLE.value
        assert any("результат не получен" in e for e in result.errors)
        assert {d.url for d in _build_of(result).docs} == {SPLIT_ROOT, SPLIT_D1A}
    finally:
        store.close()


def test_split_polls_until_terminal(tmp_path: Path) -> None:
    """P3-A2/§1: каждый раунд поллится до терминального статуса; пауза — инжектированный sleep."""
    store = SqliteStore(":memory:")
    _make_run(store)
    slept: list[float] = []
    pages = {SPLIT_ROOT: b'<a href="/a">a</a>', SPLIT_D1A: b"leaf a"}
    executor = _FakeExecutor(pages=pages, statuses=["pending", "completed"])
    try:
        _build(
            tmp_path,
            pages={},
            crawler=_BombCrawler({}),
            executor=executor,
            executor_poll_interval=0.5,
            executor_sleep=slept.append,
            urls={"p1": [SPLIT_ROOT]},
            domain_profiles=_split_profile(max_depth=1),
            store=store,
            run_id=RUN_ID,
        )
        # Раунд 1: pending → пауза → completed. Раунд 2: уже терминально (без паузы).
        assert executor.status_calls == ["pending", "completed", "completed"]
        assert slept == [0.5]
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Ф0: покрытие корпуса продукта
# ---------------------------------------------------------------------------


def _session_item(
    url: str,
    status: CrawlItemStatus,
    *,
    error_kind: str | None = None,
) -> CrawlSessionItem:
    """Элемент сеанса для чистого теста деривации покрытия."""
    return CrawlSessionItem(
        session_id="crawl:run",
        product_id="p1",
        url=url,
        status=status,
        error_kind=error_kind,
    )


def test_derive_corpus_coverage_counts_blocked_sources() -> None:
    """Ф0: failed-исходы считаются по каноническому ``error_kind``, fetched — observed.

    С версии методологии 2.2.0 частичный ``robots_disallow`` на читаемом origin'е —
    **блокирующий** in-scope сбой: корпус неполон, хотя ``unreadable_origins`` пуст.
    """
    items = [
        _session_item("https://a.test/1", CrawlItemStatus.FETCHED),
        _session_item("https://a.test/2", CrawlItemStatus.FETCHED),
        _session_item("https://a.test/3", CrawlItemStatus.FETCHED),
        _session_item(
            "https://a.test/4",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.ROBOTS_DISALLOW.value,
        ),
        _session_item(
            "https://a.test/5",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.ROBOTS_DISALLOW.value,
        ),
        _session_item("https://a.test/6", CrawlItemStatus.CANCELLED),
    ]
    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is False
    assert coverage.unreadable_origins == []
    assert coverage.expected_sources == 6
    assert coverage.observed_sources == 3
    assert coverage.blocked == {"robots_disallow": 2}
    assert coverage.blocking_failures == {"robots_disallow": 2}
    assert coverage.partial_blocks == {"a.test": {"robots_disallow": 2}}
    assert coverage.truncated is False


def test_truncation_forces_incomplete_even_without_failures() -> None:
    """Ф0: политическое усечение делает корпус неполным при нуле сбоев."""
    items = [
        _session_item("https://a.test/1", CrawlItemStatus.FETCHED),
        _session_item("https://a.test/2", CrawlItemStatus.FETCHED),
    ]
    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=True,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.blocked == {}
    assert coverage.observed_sources == coverage.expected_sources == 2
    assert coverage.truncated is True
    assert coverage.complete is False


def test_partially_blocked_origin_is_incomplete() -> None:
    """Ф0: блокирующий сбой на читаемом origin'е делает корпус неполным (2.2.0).

    Измеренный случай (pangolin): origin отдал 60 документов, 50 URL отказаны
    ``robots_disallow``; второй origin прочитан целиком. Под новой методологией
    ненаблюдённые 50 in-scope URL — причина неполноты, а не «тихий» аудит.
    ``source_unavailable`` при этом остаётся неблокирующим.

    Под правилом 2.2.0 неполны все три измеренных продукта ru-pg-dbms: jatoba —
    2 ``parser_failed``, pangolin — 50 ``robots_disallow``, postgrespro — тёмный origin.
    """
    items = [
        _session_item(f"https://docs.example/{i}", CrawlItemStatus.FETCHED)
        for i in range(60)
    ]
    items += [
        _session_item(
            f"https://docs.example/q{i}?facet=1",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.ROBOTS_DISALLOW.value,
        )
        for i in range(50)
    ]
    items.append(
        _session_item(
            "https://docs.example/gone",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
        )
    )
    items += [
        _session_item("https://portal.example/a", CrawlItemStatus.FETCHED),
        _session_item("https://portal.example/b", CrawlItemStatus.FETCHED),
    ]

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is False
    assert coverage.unreadable_origins == []
    assert coverage.expected_sources == 113
    assert coverage.observed_sources == 62
    assert coverage.blocked == {"robots_disallow": 50, "source_unavailable": 1}
    assert coverage.blocking_failures == {"robots_disallow": 50}
    assert coverage.partial_blocks == {
        "docs.example": {"robots_disallow": 50, "source_unavailable": 1}
    }


def test_blocking_failure_kinds_are_canonical() -> None:
    """Ф0: набор блокирующих классов фиксирован и не включает разрешённые наблюдения."""
    assert {kind.value for kind in BLOCKING_FAILURE_KINDS} == {
        "robots_disallow",
        "parser_failed",
        "extraction_timeout",
        "attempts_exhausted",
        "dlp_blocked",
        "quarantined",
    }
    assert FailureKind.NOT_FOUND not in BLOCKING_FAILURE_KINDS
    assert FailureKind.SOURCE_UNAVAILABLE not in BLOCKING_FAILURE_KINDS
    assert FailureKind.REDIRECT_OUT_OF_SCOPE not in BLOCKING_FAILURE_KINDS
    assert FailureKind.EVALUATION_FAILED not in BLOCKING_FAILURE_KINDS
    assert FailureKind.VALIDATION_FAILED not in BLOCKING_FAILURE_KINDS
    assert FailureKind.SEARCH_FAILED not in BLOCKING_FAILURE_KINDS


def test_parser_failure_makes_corpus_incomplete() -> None:
    """Ф0: ``parser_failed`` — блокирующий in-scope сбой, даже при читаемом origin'е."""
    items = [
        _session_item(f"https://docs.example/{i}", CrawlItemStatus.FETCHED)
        for i in range(10)
    ]
    items.append(
        _session_item(
            "https://docs.example/broken.pdf",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.PARSER_FAILED.value,
        )
    )

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is False
    assert coverage.unreadable_origins == []
    assert coverage.blocking_failures == {"parser_failed": 1}


def test_source_unavailable_on_readable_origin_stays_complete() -> None:
    """Ф0: ``source_unavailable`` транзиентен и не блокирует читаемый origin."""
    items = [
        _session_item("https://docs.example/1", CrawlItemStatus.FETCHED),
        _session_item(
            "https://docs.example/flaky",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
        ),
    ]

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is True
    assert coverage.unreadable_origins == []
    assert coverage.blocked == {"source_unavailable": 1}
    assert coverage.blocking_failures == {}


def test_wholly_dark_origin_from_source_unavailable_is_incomplete() -> None:
    """Ф0: полностью тёмный origin ловится правилом нечитаемости, а не классом сбоя."""
    items = [
        _session_item("https://docs.example/1", CrawlItemStatus.FETCHED),
        _session_item(
            "https://dark.example/a",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
        ),
        _session_item(
            "https://dark.example/b",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
        ),
    ]

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is False
    assert coverage.unreadable_origins == ["dark.example"]
    assert coverage.blocking_failures == {}


def test_fully_closed_origin_makes_corpus_incomplete() -> None:
    """Ф0: origin с нулём ``FETCHED`` нечитаем; отказы других origin'ов сюда не входят.

    Измеренный случай: основной origin частично отказал (404/robots), но читаем;
    отдельный origin отказал целиком — именно он блокирует ``❌``.
    """
    items = [
        _session_item(f"https://docs.example/{i}", CrawlItemStatus.FETCHED)
        for i in range(84)
    ]
    items += [
        _session_item(
            f"https://docs.example/gone/{i}",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
        )
        for i in range(21)
    ]
    items += [
        _session_item(
            f"https://docs.example/account/{i}",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.ROBOTS_DISALLOW.value,
        )
        for i in range(9)
    ]
    items += [
        _session_item(f"https://edu.example/{i}", CrawlItemStatus.FETCHED)
        for i in range(95)
    ]
    items.append(
        _session_item(
            "https://closed.example/",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.ROBOTS_DISALLOW.value,
        )
    )

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is False
    assert coverage.unreadable_origins == ["closed.example"]
    assert coverage.expected_sources == 210
    assert coverage.observed_sources == 179
    assert coverage.partial_blocks == {
        "closed.example": {"robots_disallow": 1},
        "docs.example": {"robots_disallow": 9, "source_unavailable": 21},
    }


def test_not_found_only_origin_is_readable() -> None:
    """Ф0: 404/410 — разрешённое наблюдение отсутствия, а не нечитаемость origin'а."""
    items = [
        _session_item("https://a.example/1", CrawlItemStatus.FETCHED),
        _session_item("https://a.example/2", CrawlItemStatus.FETCHED),
        _session_item(
            "https://a.example/gone-1",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.NOT_FOUND.value,
        ),
        _session_item(
            "https://gone.example/x",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.NOT_FOUND.value,
        ),
        _session_item(
            "https://gone.example/y",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.NOT_FOUND.value,
        ),
    ]

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is True
    assert coverage.unreadable_origins == []
    assert coverage.blocked == {"not_found": 3}
    assert coverage.partial_blocks == {}


def test_one_fetched_plus_not_found_stays_complete() -> None:
    """2.3.0: одного ``FETCHED`` достаточно — новое условие не пере-срабатывает."""
    items = [
        _session_item("https://a.example/1", CrawlItemStatus.FETCHED),
        _session_item(
            "https://gone.example/x",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.NOT_FOUND.value,
        ),
    ]

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is True
    assert coverage.expected_sources == 2
    assert coverage.observed_sources == 1
    assert coverage.unreadable_origins == []
    assert coverage.blocked == {"not_found": 1}
    assert coverage.reason is None


def test_all_pending_items_are_incomplete() -> None:
    """2.3.0: сеанс без терминальных исходов — ноль наблюдений, ``complete=False``."""
    items = [
        _session_item("https://a.example/1", CrawlItemStatus.PENDING),
        _session_item("https://a.example/2", CrawlItemStatus.RUNNING),
    ]

    coverage = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )

    assert coverage.complete is False
    assert coverage.expected_sources == 2
    assert coverage.observed_sources == 0
    assert coverage.blocked == {}
    assert coverage.reason is None


def test_derive_corpus_coverage_is_deterministic() -> None:
    """Ф0: порядок элементов не влияет на вывод — байт-идентичный JSON."""
    items = [
        _session_item("https://b.example/1", CrawlItemStatus.FETCHED),
        _session_item(
            "https://a.example/1",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.ROBOTS_DISALLOW.value,
        ),
        _session_item(
            "https://b.example/2",
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
        ),
    ]
    first = derive_corpus_coverage(
        product_id="p1",
        items=items,
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )
    second = derive_corpus_coverage(
        product_id="p1",
        items=list(reversed(items)),
        truncated=False,
        computed_at="2026-01-01T00:00:00+00:00",
    )
    assert first.model_dump_json() == second.model_dump_json()


def test_build_corpus_persists_product_coverage(tmp_path: Path) -> None:
    """Ф0: фаза корпуса персистит покрытие по персистентным элементам сеанса."""
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            store=store,
            run_id=RUN_ID,
        )
        assert result.status == "ok"
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is True
        assert coverage.expected_sources == coverage.observed_sources == 1
        assert coverage.blocked == {}
        assert coverage.truncated is False
    finally:
        store.close()


def test_extract_cache_skips_second_extraction_of_identical_bytes(
    tmp_path: Path,
) -> None:
    """Ф1.2: кэш экстракции переживает прогон и снимает вызов экстрактора.

    Это регрессия на «кэш объявлен, но не подключён»: недельный цикл открывает
    **новую** per-run БД, поэтому без межпрогонного кэша docling гоняется заново
    на байт-идентичных документах. Ключ — хеш ретенированных байтов (I19), а не
    URL: второй прогон с другим URL и теми же байтами тоже обязан попасть в кэш.
    """
    cache = ExtractionCache(tmp_path / "extract_cache")
    page = b"identical bytes"

    first = _Extractor()
    _build(
        tmp_path / "run1",
        pages={URL_P1_A: page},
        urls={"p1": [URL_P1_A]},
        extractor=first,
        extract_cache=cache,
    )
    assert first.calls == 1, "первый прогон обязан извлечь документ"

    second = _Extractor()
    _build(
        tmp_path / "run2",
        pages={URL_P1_A: page},
        urls={"p1": [URL_P1_A]},
        extractor=second,
        extract_cache=cache,
    )
    assert second.calls == 0, "байт-идентичный документ обязан взять из кэша"

    third = _Extractor()
    _build(
        tmp_path / "run3",
        pages={URL_P1_A: b"changed bytes"},
        urls={"p1": [URL_P1_A]},
        extractor=third,
        extract_cache=cache,
    )
    assert third.calls == 1, "изменившиеся байты обязаны извлечься заново"


def test_extract_cache_none_preserves_byte_identical_behaviour(
    tmp_path: Path,
) -> None:
    """Ф1.2: без кэша поведение прежнее — экстрактор вызывается на каждый документ."""
    extractor = _Extractor()
    _build(
        tmp_path,
        pages={URL_P1_A: b"page one"},
        urls={"p1": [URL_P1_A]},
        extractor=extractor,
    )
    assert extractor.calls == 1





# --------------------------------------------------------------------------- #
# MVP-3b: durable-субстрат (09 §6.3/§9) — url_state/document_version/chunk_store
# --------------------------------------------------------------------------- #

STATE_ETAG = 'W/"v1"'
STATE_LAST_MODIFIED = "Wed, 21 Oct 2026 07:28:00 GMT"


class _StatefulCrawler(_Crawler):
    """Краулер с условным GET: журналирует валидаторы и умеет отвечать ``304``.

    ``not_modified`` — множество URL, на которые сервер отвечает ``304`` **только** при
    совпавшем ``If-None-Match`` (иначе — обычные байты). ``etag``/``last_modified``
    отдаются в ``RawPage``: без них ``url_state`` нечем заполнить.
    """

    def __init__(
        self,
        pages: dict[str, bytes],
        *,
        not_modified: set[str] | None = None,
        etag: str | None = STATE_ETAG,
        last_modified: str | None = STATE_LAST_MODIFIED,
    ) -> None:
        super().__init__(pages)
        self.not_modified = set(not_modified or ())
        self.etag = etag
        self.last_modified = last_modified

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append((url, plan))
        confirmed = (
            url in self.not_modified
            and self.etag is not None
            and plan.if_none_match == self.etag
        )
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
            status_code=304 if confirmed else 200,
            etag=self.etag,
            last_modified=self.last_modified,
            not_modified=confirmed,
        )


class _CountingChunker(_Chunker):
    """Чанкер со счётчиком вызовов: переиспользование обязано его не вызывать."""

    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        self.calls += 1
        return super().chunk(doc, document_id)


def _validators_sent(crawler: _Crawler, url: str) -> tuple[str | None, str | None]:
    """Валидаторы из ``FetchPlan`` последнего фетча URL (``(etag, last_modified)``)."""
    plans = [plan for u, plan in crawler.calls if u == url]
    assert plans, f"{url} не фетчился"
    return plans[-1].if_none_match, plans[-1].if_modified_since


def _state_run(
    root: Path,
    state: SqliteStateStore,
    *,
    pages: dict[str, bytes],
    urls: dict[str, list[str]],
    crawler: _Crawler | None = None,
    extractor: _Extractor | None = None,
    chunker: _Chunker | None = None,
    mode: str = "incremental",
    **kwargs,
):
    """Прогон корпуса с durable-субстратом и явным режимом продукта."""
    modes = {pid: mode for pid in urls}
    return _build(
        root,
        pages=pages,
        urls=urls,
        crawler=crawler,
        extractor=extractor,
        chunker=chunker,
        state=state,
        product_modes=modes,
        **kwargs,
    )


def test_state_first_run_fills_substrate_and_counts_new_docs(tmp_path: Path) -> None:
    """Первый прогон с ``state``: url_state/document_version/chunk_store/chunk_meta + new."""
    with SqliteStateStore(":memory:") as state:
        crawler = _StatefulCrawler({URL_P1_A: b"page one"})
        result = _state_run(
            tmp_path,
            state,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
        )
        build = _build_of(result)
        assert result.errors == []
        assert build.doc_counters == {"p1": {"total": 1, "new": 1, "reused": 0}}

        row = state.get_url_state("p1", URL_P1_A)
        assert row is not None
        assert row.doc_version_id
        assert row.etag == STATE_ETAG
        assert row.last_modified == STATE_LAST_MODIFIED
        assert row.reuse_kind == "live"
        assert row.last_http_status == 200
        assert row.consecutive_failures == 0

        version = state.get_document_version(row.doc_version_id)
        assert version is not None
        assert version.status == "active"
        assert version.first_seen_run_id == ""  # без run_id прогона токен пуст
        assert version.chunker_version == "chunker-1"
        assert version.chunk_ids_digest
        assert version.extractor_version == "extractor-1"

        chunks = state.iter_doc_chunks(version.doc_version_id)
        assert len(chunks) == 1
        text = state.get_chunk_text(chunks[0].content_hash)
        assert text == "page one"
        assert chunks[0].content_hash == content_hash(text)
        # Run-level ``chunk_id`` не смешивается с durable-ключом chunk_meta.
        assert chunks[0].chunk_id != f"p1:{content_hash(b'page one')}:0"


def test_state_revalidated_304_reuses_document_without_extract(tmp_path: Path) -> None:
    """304: валидаторы уехали в FetchPlan, экстрактор/чанкер не вызваны, chunk_id те же."""
    with SqliteStateStore(":memory:") as state:
        first = _state_run(
            tmp_path / "r1",
            state,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"page one"}),
        )
        build1 = _build_of(first)
        first_ids = sorted(
            row["chunk_id"]
            for row in state.conn.execute(
                "SELECT chunk_id FROM chunk_meta ORDER BY chunk_id"
            )
        )
        first_version = state.get_url_state("p1", URL_P1_A).doc_version_id

        crawler2 = _StatefulCrawler(
            {URL_P1_A: b"page one"}, not_modified={URL_P1_A}
        )
        extractor2 = _Extractor()
        chunker2 = _CountingChunker()
        # Отдельный прогон (свой store/run_id): resume прошлого сеанса не должен
        # маскировать живой условный GET — проверяем именно путь ревалидации.
        run_store = SqliteStore(":memory:")
        try:
            _make_run(run_store, "run-2")
            second = _state_run(
                tmp_path / "r2",
                state,
                pages={URL_P1_A: b"page one"},
                urls={"p1": [URL_P1_A]},
                crawler=crawler2,
                extractor=extractor2,
                chunker=chunker2,
                store=run_store,
                run_id="run-2",
            )
            coverage = run_store.read_corpus_coverage("run-2", "p1")
        finally:
            run_store.close()
        build2 = _build_of(second)

        assert _validators_sent(crawler2, URL_P1_A) == (
            STATE_ETAG,
            STATE_LAST_MODIFIED,
        )
        assert extractor2.calls == 0, "304 не должен вызывать экстрактор"
        assert chunker2.calls == 0, "304 не должен вызывать чанкер"
        # Идентификаторы индекса не меняются: тот же контент — та же сборка.
        assert build2.index_build_id == build1.index_build_id
        second_ids = sorted(
            row["chunk_id"]
            for row in state.conn.execute(
                "SELECT chunk_id FROM chunk_meta ORDER BY chunk_id"
            )
        )
        assert second_ids == first_ids
        assert build2.chunk_count == build1.chunk_count == 1

        assert build2.doc_counters == {"p1": {"total": 1, "new": 0, "reused": 1}}
        row = state.get_url_state("p1", URL_P1_A)
        assert row.doc_version_id == first_version
        assert row.reuse_kind == "revalidated"
        assert row.last_http_status == 304
        # 304 — наблюдение, а не пропуск: покрытие то же (expected/observed = 1/1).
        assert coverage is not None
        assert coverage.expected_sources == 1
        assert coverage.observed_sources == 1
        assert coverage.complete is True


def test_state_changed_content_creates_new_version_and_keeps_old_active(
    tmp_path: Path,
) -> None:
    """200 с другим ``content_hash`` ⇒ новая версия; прежняя остаётся ``active``."""
    with SqliteStateStore(":memory:") as state:
        _state_run(
            tmp_path / "r1",
            state,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"page one"}),
        )
        old_version = state.get_url_state("p1", URL_P1_A).doc_version_id

        extractor2 = _Extractor()
        second = _state_run(
            tmp_path / "r2",
            state,
            pages={URL_P1_A: b"page two"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"page two"}),
            extractor=extractor2,
        )
        build2 = _build_of(second)
        new_version = state.get_url_state("p1", URL_P1_A).doc_version_id

        assert new_version != old_version
        assert extractor2.calls == 1
        assert build2.doc_counters == {"p1": {"total": 1, "new": 1, "reused": 0}}
        # Superseded — только на 404; при изменении контента обе версии активны.
        assert state.get_document_version(old_version).status == "active"
        assert state.get_document_version(new_version).status == "active"


def test_state_refresh_policy_sends_no_validators(tmp_path: Path) -> None:
    """Политика ``refresh`` — безусловный GET: валидаторы не отправляются."""
    with SqliteStateStore(":memory:") as state:
        _state_run(
            tmp_path / "r1",
            state,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"page one"}),
        )
        crawler2 = _StatefulCrawler(
            {URL_P1_A: b"page one"}, not_modified={URL_P1_A}
        )
        extractor2 = _Extractor()
        _state_run(
            tmp_path / "r2",
            state,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=crawler2,
            extractor=extractor2,
            refresh_policy="refresh",
        )
        assert _validators_sent(crawler2, URL_P1_A) == (None, None)
        assert extractor2.calls == 1, "refresh обязан получить и разобрать свежий ответ"


def test_state_not_found_clears_url_version_and_supersedes_document(
    tmp_path: Path,
) -> None:
    """404: ``url_state.doc_version_id`` очищен, прежняя версия — ``superseded``."""
    with SqliteStateStore(":memory:") as state:
        _state_run(
            tmp_path / "r1",
            state,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"page one"}),
        )
        old_version = state.get_url_state("p1", URL_P1_A).doc_version_id

        result = _state_run(
            tmp_path / "r2",
            state,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=_NotFoundCrawler(),
        )
        row = state.get_url_state("p1", URL_P1_A)
        assert row is not None
        assert row.doc_version_id is None
        assert row.last_error_kind == "not_found"
        assert row.reuse_kind == "live"
        assert state.get_document_version(old_version).status == "superseded"
        assert any("404" in err or "not_found" in err for err in result.errors)


def test_state_corrupt_chunk_text_falls_back_with_visible_error(
    tmp_path: Path,
) -> None:
    """Испорченный ``chunk_store`` не переиспользуется: fallback + видимая ошибка (I19)."""
    with SqliteStateStore(":memory:") as state:
        _state_run(
            tmp_path / "r1",
            state,
            pages={URL_P1_A: b"page one"},
            urls={"p1": [URL_P1_A]},
            crawler=_StatefulCrawler({URL_P1_A: b"page one"}),
        )
        with state.conn:
            state.conn.execute("UPDATE chunk_store SET text = ?", ("tampered",))

        crawler2 = _StatefulCrawler(
            {URL_P1_A: b"page one"}, not_modified={URL_P1_A}
        )
        extractor2 = _Extractor()
        chunker2 = _CountingChunker()
        run_store = SqliteStore(":memory:")
        try:
            second = _state_run(
                tmp_path / "r2",
                state,
                pages={URL_P1_A: b"page one"},
                urls={"p1": [URL_P1_A]},
                crawler=crawler2,
                extractor=extractor2,
                chunker=chunker2,
                store=run_store,
            )
            # Корпус собран из свежих байтов, а не из испорченной строки.
            texts = [
                row["text"]
                for row in run_store.conn.execute("SELECT text FROM corpus_chunks")
            ]
            assert texts == ["page one"]
        finally:
            run_store.close()
        build2 = _build_of(second)

        assert extractor2.calls == 1, "неверифицированный текст не переиспользуется"
        assert chunker2.calls == 1
        assert any("I19" in err and "reuse" in err for err in second.errors)
        assert build2.doc_counters == {"p1": {"total": 1, "new": 1, "reused": 0}}
        # ``chunk_store`` иммутабелен по ``content_hash`` (порт не даёт перезаписи текста),
        # поэтому испорченная строка остаётся ядовитой навсегда — но и **никогда** не
        # используется: каждый следующий прогон снова отвергает её по I19 и пере-нарезает.
        still_bad = [
            state.get_chunk_text(row["content_hash"])
            for row in state.conn.execute("SELECT content_hash FROM chunk_meta")
        ]
        assert still_bad and all(text == "tampered" for text in still_bad)


def test_state_none_is_byte_identical_and_writes_nothing(tmp_path: Path) -> None:
    """``state=None``: тот же ``index_build_id``/chunk_id, ноль записей в субстрат."""
    pages = {URL_P1_A: b"page one"}
    urls = {"p1": [URL_P1_A]}

    plain = _build_of(_build(tmp_path / "a", pages=pages, urls=urls))
    with SqliteStateStore(":memory:") as state:
        # Режим/политика при ``state=None`` игнорируются целиком.
        result = _build(
            tmp_path / "b",
            pages=pages,
            urls=urls,
            state=None,
            product_modes={"p1": "incremental"},
            refresh_policy="revalidate",
        )
        build = _build_of(result)
        assert build.index_build_id == plain.index_build_id
        assert build.chunk_count == plain.chunk_count
        assert build.doc_counters == {}
        assert list(state.iter_url_state("p1")) == []
        assert list(state.iter_document_versions("p1")) == []
        assert state.conn.execute("SELECT COUNT(*) AS n FROM chunk_store").fetchone()[
            "n"
        ] == 0


def test_state_executor_preserves_validators_and_304(tmp_path: Path) -> None:
    """Исполнитель: ``etag``/``last_modified``/``not_modified`` доходят до ядра."""
    with SqliteStateStore(":memory:") as state:
        executor = _FakeExecutor(
            pages={URL_P1_A: b"page one"},
            extra_items=[
                CrawlResultItem(
                    product_id="p1",
                    url=URL_P1_A,
                    status="fetched",
                    engine="http",
                    raw_bytes=b"page one",
                    http_status=200,
                    etag=STATE_ETAG,
                    last_modified=STATE_LAST_MODIFIED,
                )
            ],
        )
        first = _build(
            tmp_path / "r1",
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=executor,
            state=state,
            product_modes={"p1": "incremental"},
        )
        assert first.status == "ok"
        row = state.get_url_state("p1", URL_P1_A)
        assert row is not None and row.doc_version_id
        assert row.etag == STATE_ETAG
        assert row.last_modified == STATE_LAST_MODIFIED
        version = state.get_document_version(row.doc_version_id)

        extractor2 = _Extractor()
        chunker2 = _CountingChunker()
        second_executor = _FakeExecutor(
            pages={URL_P1_A: b"page one"},
            extra_items=[
                CrawlResultItem(
                    product_id="p1",
                    url=URL_P1_A,
                    status="fetched",
                    engine="http",
                    raw_bytes=b"page one",
                    http_status=304,
                    etag=STATE_ETAG,
                    last_modified=STATE_LAST_MODIFIED,
                    not_modified=True,
                )
            ],
        )
        second = _build(
            tmp_path / "r2",
            pages={},
            urls={"p1": [URL_P1_A]},
            executor=second_executor,
            extractor=extractor2,
            chunker=chunker2,
            state=state,
            product_modes={"p1": "incremental"},
        )
        build2 = _build_of(second)
        # Работа несла валидаторы (иначе edge не смог бы ответить 304).
        sent = second_executor.submits[-1][0]
        assert sent.if_none_match == STATE_ETAG
        assert sent.if_modified_since == STATE_LAST_MODIFIED
        assert extractor2.calls == 0
        assert chunker2.calls == 0
        assert build2.doc_counters == {"p1": {"total": 1, "new": 0, "reused": 1}}
        assert state.get_url_state("p1", URL_P1_A).doc_version_id == version.doc_version_id


def test_state_offline_reports_explicit_error_and_still_crawls(tmp_path: Path) -> None:
    """``offline``: reuse не реализован — видимая ошибка на продукт, известный URL краулится."""
    with SqliteStateStore(":memory:") as state:
        state.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url=URL_P1_B,
                normalized_url=URL_P1_B,
                doc_version_id="dv_known",
                etag=STATE_ETAG,
            )
        )
        crawler = _StatefulCrawler(
            {URL_P1_A: b"page one", URL_P1_B: b"page two"}
        )
        result = _state_run(
            tmp_path,
            state,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            refresh_policy="offline",
        )
        assert "offline reuse не реализован в corpus: p1" in result.errors
        # Элемент обработан как revalidate: сеть тронута, а не молча пропущена.
        assert [u for u, _plan in crawler.calls] == [URL_P1_A, URL_P1_B]
        assert _validators_sent(crawler, URL_P1_B) == (None, None)


def test_state_incremental_grows_corpus_via_existing_bfs(tmp_path: Path) -> None:
    """Инкрементальный план не отменяет BFS: новые ссылки со страницы по-прежнему добываются."""
    root = "https://docs.test/p1/"
    linked_url = "https://docs.test/p1/new"
    with SqliteStateStore(":memory:") as state:
        _state_run(
            tmp_path / "r1",
            state,
            pages={root: b"page one"},
            urls={"p1": [root]},
            crawler=_StatefulCrawler({root: b"page one"}),
        )
        linked = (
            b'<html><body><a href="https://docs.test/p1/new">new</a></body></html>'
        )
        crawler2 = _StatefulCrawler({root: linked, linked_url: b"fresh page"})
        second = _state_run(
            tmp_path / "r2",
            state,
            pages={},
            urls={"p1": [root]},
            crawler=crawler2,
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )
        assert linked_url in [u for u, _p in crawler2.calls]
        build2 = _build_of(second)
        assert build2.doc_counters["p1"]["total"] == 2
        assert state.get_url_state("p1", linked_url) is not None


def test_state_304_without_known_version_is_visible_not_silent(tmp_path: Path) -> None:
    """``304`` без известной версии: обычная экстракция + видимая запись (не тихо)."""
    with SqliteStateStore(":memory:") as state:
        # Валидатор известен, а версии документа в субстрате нет (например, вытеснена):
        # сервер подтверждает «не изменилось», переиспользовать нечего.
        state.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url=URL_P1_A,
                normalized_url=URL_P1_A,
                doc_version_id="dv_missing",
                etag=STATE_ETAG,
            )
        )
        crawler = _StatefulCrawler(
            {URL_P1_A: b"page one"}, not_modified={URL_P1_A}
        )
        extractor = _Extractor()
        result = _state_run(
            tmp_path,
            state,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            extractor=extractor,
        )
        # Отправлен ровно тот валидатор, что известен состоянию (Last-Modified не задан).
        assert _validators_sent(crawler, URL_P1_A) == (STATE_ETAG, None)
        assert extractor.calls == 1, "переиспользовать нечего — обычная экстракция"
        assert any("304 без переиспользуемой версии" in err for err in result.errors)
        assert _build_of(result).doc_counters == {
            "p1": {"total": 1, "new": 1, "reused": 0}
        }


# --------------------------------------------------------------------------- #
# MVP-4 шаг 2: переиспользование партиции, композиция, цепочка идентификаторов
# --------------------------------------------------------------------------- #


def _composition_dir(tmp_path: Path, package_id: str) -> Path:
    return tmp_path / "index_store" / package_id / "_compositions"


@requires_numpy
def test_partition_reuse_calls_embedder_zero_times(tmp_path: Path) -> None:
    """09 §8.5: неизменённый продукт переиспользует партицию — ноль эмбеддингов."""
    index_root = tmp_path / "index_store"
    pages = {URL_P1_A: b"stable page"}
    urls = {"p1": [URL_P1_A]}
    store = SqliteStore(":memory:")
    try:
        first_embedder = _Embedder()
        first = _build(
            tmp_path / "raw1",
            pages=pages,
            urls=urls,
            store=store,
            index_root=index_root,
            embedder=first_embedder,
            index_builder=NpyIndexBuilder(index_root),
        )
        first_build = _build_of(first)
        assert first_embedder.texts == ["stable page"]
        assert first_build.partitions_reused == 0
        partition_id = first_build.entries[0].partition_build_id or ""
        assert partition_id.startswith("pb_")

        second_embedder = _Embedder()
        second = _build(
            tmp_path / "raw2",
            pages=pages,
            urls=urls,
            store=store,
            index_root=index_root,
            embedder=second_embedder,
            index_builder=NpyIndexBuilder(index_root),
        )
        second_build = _build_of(second)
        # Ноль вызовов эмбеддера — реальный CPU-win, не «пересобрали, но дёшево».
        assert second_embedder.texts == []
        assert second_build.partitions_reused == 1
        assert second_build.entries[0].partition_build_id == partition_id
        assert second.errors == []

        rows = list(
            store.conn.execute("SELECT build_id FROM corpus_chunks ORDER BY chunk_id")
        )
        assert rows and {row["build_id"] for row in rows} == {partition_id}
        # Композиция идемпотентна: тот же состав → тот же cid, файл не перезаписан.
        assert second_build.index_build_id == first_build.index_build_id
        assert (_composition_dir(tmp_path, first_build.entries[0].package_id or "")
                / f"{second_build.index_build_id}.json").is_file()
    finally:
        store.close()


@requires_numpy
def test_changed_content_changes_partition_and_reembeds(tmp_path: Path) -> None:
    """Изменение одного чанка меняет ``pb`` и заставляет эмбеддить снова (09 §8.1)."""
    index_root = tmp_path / "index_store"
    store = SqliteStore(":memory:")
    try:
        first = _build(
            tmp_path / "a",
            pages={URL_P1_A: b"one"},
            urls={"p1": [URL_P1_A]},
            store=store,
            index_root=index_root,
            embedder=_Embedder(),
            index_builder=NpyIndexBuilder(index_root),
        )
        second_embedder = _Embedder()
        second = _build(
            tmp_path / "b",
            pages={URL_P1_A: b"two"},
            urls={"p1": [URL_P1_A]},
            store=store,
            index_root=index_root,
            embedder=second_embedder,
            index_builder=NpyIndexBuilder(index_root),
        )
        assert _build_of(first).entries[0].partition_build_id != (
            _build_of(second).entries[0].partition_build_id
        )
        assert second_embedder.texts == ["two"]
        assert _build_of(second).partitions_reused == 0
    finally:
        store.close()


def test_reuse_refused_when_manifest_digest_differs(tmp_path: Path) -> None:
    """Второй, физический контроль: расхождение digest ⇒ пересборка с видимой причиной."""
    index_root = tmp_path / "index_store"
    pages = {URL_P1_A: b"page"}
    urls = {"p1": [URL_P1_A]}
    first = _build(
        tmp_path / "a",
        pages=pages,
        urls=urls,
        index_root=index_root,
        embedder=_Embedder(),
        index_builder=_IndexBuilder(),
    )
    build = _build_of(first)
    package_id = build.entries[0].package_id or ""
    partition_id = build.entries[0].partition_build_id or ""
    # Кладём «зафиксированную» партицию с чужим digest (id при этом тот же).
    index_format.write_partition(
        index_root, package_id, partition_id, "p1",
        [[0.0, 0.1, 0.2]], ["p1:fake:0"],
        {
            "dim": 3,
            "content_digest": "bogus",
            "chunker_version": "chunker-1",
            "embed_model_id": "embed-x",
            "extractor_version": "extractor-1",
            "lexical_engine_version": "lex-1",
            "vector_store": "npy",
        },
    )
    embedder = _Embedder()
    second = _build(
        tmp_path / "b",
        pages=pages,
        urls=urls,
        index_root=index_root,
        embedder=embedder,
        index_builder=_IndexBuilder(),
    )
    second_build = _build_of(second)
    assert second_build.partitions_reused == 0
    assert embedder.texts == ["page"]
    assert any("не переиспользована" in err and "content_digest" in err
               for err in second.errors)


def test_reuse_refused_when_manifest_version_differs(tmp_path: Path) -> None:
    """Версия в манифесте расходится с прогоном ⇒ пересборка с видимой причиной."""
    index_root = tmp_path / "index_store"
    pages = {URL_P1_A: b"page"}
    urls = {"p1": [URL_P1_A]}
    first = _build(
        tmp_path / "a",
        pages=pages,
        urls=urls,
        index_root=index_root,
        embedder=_Embedder(),
        index_builder=_IndexBuilder(),
    )
    build = _build_of(first)
    package_id = build.entries[0].package_id or ""
    partition_id = build.entries[0].partition_build_id or ""
    index_format.write_partition(
        index_root, package_id, partition_id, "p1",
        [[0.0, 0.1, 0.2]], ["p1:fake:0"],
        {
            "dim": 3,
            "content_digest": build.entries[0].content_digest,
            "chunker_version": "chunker-old",
            "embed_model_id": "embed-x",
            "extractor_version": "extractor-1",
            "lexical_engine_version": "lex-1",
            "vector_store": "npy",
        },
    )
    embedder = _Embedder()
    second = _build(
        tmp_path / "b",
        pages=pages,
        urls=urls,
        index_root=index_root,
        embedder=embedder,
        index_builder=_IndexBuilder(),
    )
    assert _build_of(second).partitions_reused == 0
    assert embedder.texts == ["page"]
    assert any(
        "не переиспользована" in err and "chunker_version" in err
        for err in second.errors
    )


def test_reuse_refused_when_partition_incomplete(tmp_path: Path) -> None:
    """Неполный каталог партиции — видимая причина и обычная сборка (09 §8.5)."""
    index_root = tmp_path / "index_store"
    pages = {URL_P1_A: b"page"}
    urls = {"p1": [URL_P1_A]}
    first = _build(
        tmp_path / "a",
        pages=pages,
        urls=urls,
        index_root=index_root,
        embedder=_Embedder(),
        index_builder=_IndexBuilder(),
    )
    build = _build_of(first)
    package_id = build.entries[0].package_id or ""
    partition_id = build.entries[0].partition_build_id or ""
    incomplete = index_root / package_id / partition_id / "p1"
    incomplete.mkdir(parents=True)
    (incomplete / "manifest.json").write_text("{}", encoding="utf-8")

    embedder = _Embedder()
    second = _build(
        tmp_path / "b",
        pages=pages,
        urls=urls,
        index_root=index_root,
        embedder=embedder,
        index_builder=_IndexBuilder(),
    )
    assert _build_of(second).partitions_reused == 0
    assert embedder.texts == ["page"]
    assert any("не переиспользована" in err for err in second.errors)


def test_composition_written_only_after_all_members_committed(tmp_path: Path) -> None:
    """Частичный сбой члена ⇒ композиции нет; state-реестр тоже не пишется."""
    index_root = tmp_path / "index_store"
    store = SqliteStore(":memory:")
    try:
        with SqliteStateStore(":memory:") as state:
            result = _build(
                tmp_path,
                pages={URL_P1_A: b"ok-one", URL_P2_A: b"ok-two"},
                urls={"p1": [URL_P1_A], "p2": [URL_P2_A]},
                store=store,
                state=state,
                index_root=index_root,
                embedder=_Embedder(),
                index_builder=_RefusingBuilder(refuse={"p2"}),
            )
            build = _build_of(result)
            package_id = build.entries[0].package_id or ""
            assert not list(_composition_dir(tmp_path, package_id).glob("*.json"))
            assert state.get_index_composition(build.index_build_id) is None
            assert list(state.iter_index_partitions(package_id)) == []
    finally:
        store.close()


@requires_numpy
def test_identity_chain_partition_vs_composition(tmp_path: Path) -> None:
    """Сквозная цепочка id: pb — чанки/партиции/evidence; cid — manifest/cell/runs."""
    from core.domain import LLMCellOutput, LLMEvidence, Status
    from core.pipeline.composition import CompositionView
    from core.pipeline.evaluate import _provenance
    from core.pipeline.validate import validate_output
    from core.adapters.vector_npy import NpyVectorIndex
    from core.ports.vector_index import RetrievedChunk, RetrievalResult

    index_root = tmp_path / "index_store"
    store = SqliteStore(":memory:")
    try:
        with SqliteStateStore(":memory:") as state:
            result = _build(
                tmp_path,
                pages={URL_P1_A: b"identity page"},
                urls={"p1": [URL_P1_A]},
                store=store,
                state=state,
                index_root=index_root,
                embedder=_Embedder(),
                index_builder=NpyIndexBuilder(index_root),
                run_id=RUN_ID,
            )
            build = _build_of(result)
            cid = build.index_build_id
            entry = build.entries[0]
            package_id = entry.package_id or ""
            pb = entry.partition_build_id or ""
            assert cid.startswith("cb_") and pb.startswith("pb_")

            # index_manifest: cid + pb; corpus_chunks: pb.
            row = store.conn.execute(
                "SELECT index_build_id, partition_build_id FROM index_manifest"
                " WHERE product_id = 'p1'"
            ).fetchone()
            assert row["index_build_id"] == cid
            assert row["partition_build_id"] == pb
            chunk = store.conn.execute(
                "SELECT chunk_id, build_id FROM corpus_chunks WHERE product_id = 'p1'"
            ).fetchone()
            assert chunk["build_id"] == pb

            # Dense retrieval: RetrievedChunk.build_id = pb; индекс = cid.
            view = CompositionView(composition_id=cid, members={"p1": pb})
            vindex = NpyVectorIndex(
                index_root, package_id, cid, 3, store.get_chunk, view=view
            )
            from core.ports.vector_index import VectorQuery

            hits = vindex.search(
                VectorQuery(product_ids=["p1"], top_k=5, vector=[0.0, 0.1, 0.2])
            )
            assert hits and hits[0].build_id == pb

            # Cell provenance = cid; evidence.index_build_id = cid, partition = pb.
            provenance = _provenance(
                RetrievalResult(
                    chunks=hits, profile="R1", cutoff_K=10, build_id=cid
                )
            )
            assert provenance[0] == cid
            validation = validate_output(
                output=LLMCellOutput(
                    status=Status.OK,
                    justification="ok",
                    evidence=[LLMEvidence(source_id=chunk["chunk_id"], quote="identity page")],
                ),
                chunks={chunk["chunk_id"]: hits[0]},
                product_id="p1",
                param_id=_plan().params[0].param_id,
                run_id=RUN_ID,
                resolver=store,
                composition_id=provenance[0],
            )
            assert validation.resolved_evidence
            evidence = validation.resolved_evidence[0]
            assert evidence.index_build_id == cid
            assert evidence.partition_build_id == pb
            store.add_evidence(evidence)
            ev_row = store.conn.execute(
                "SELECT index_build_id, partition_build_id FROM cell_evidence"
                " WHERE evidence_id = ?",
                (evidence.evidence_id,),
            ).fetchone()
            assert ev_row["index_build_id"] == cid
            assert ev_row["partition_build_id"] == pb

            # State: композиция и партиция зарегистрированы.
            composition = state.get_index_composition(cid)
            assert composition is not None
            assert composition.members == ((entry.product_id, pb, entry.content_digest),)
            partitions = list(state.iter_index_partitions(package_id))
            assert [p.partition_build_id for p in partitions] == [pb]
            assert partitions[0].last_used_run_id == RUN_ID
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# 09 §6.3: группировка ревалидации известных URL по авторским корням
# --------------------------------------------------------------------------- #

RG_HOST_ROOT = "https://corp.test/"
RG_PATH_ROOT = "https://corp.test/docs/"
RG_BLOG_A = "https://corp.test/blog/a"
RG_BLOG_B = "https://corp.test/blog/b"
RG_DOCS_A = "https://corp.test/docs/a"
RG_DOCS_B = "https://corp.test/docs/b"
RG_DOCS_C = "https://corp.test/docs/c"


def _seed_known_urls(state: SqliteStateStore, urls: list[str]) -> None:
    """Кладёт известные URL продукта p1 в ``url_state`` (без версии документа)."""
    for url in urls:
        state.upsert_url_state(
            UrlStateRow(product_id="p1", url=url, normalized_url=url)
        )


def _rg_pages(*urls: str, body: bytes = b"text") -> dict[str, bytes]:
    return {url: body for url in urls}


def test_revalidation_groups_by_most_specific_authored_root(tmp_path: Path) -> None:
    """Два корня (host + path key): 5 известных URL → по одной записи усечения на корень.

    Самый специфичный ``root_path_prefix`` побеждает: ``/docs/*`` не должны попасть в
    host-корень. Проверяем по порядку фетчей (дренаж авторских корней — авторский порядок).
    """
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A, RG_DOCS_B, RG_DOCS_C]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(
            RG_HOST_ROOT, RG_PATH_ROOT, *known, body=b"text without links"
        )
        crawler = _StatefulCrawler(pages)
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT, RG_PATH_ROOT]},
            crawler=crawler,
        )
        build = _build_of(result)
        assert len(build.truncation) == 2
        assert {record["root"] for record in build.truncation} == {
            RG_HOST_ROOT,
            RG_PATH_ROOT,
        }
        assert not ({record["root"] for record in build.truncation} & set(known))
        calls = [url for url, _plan in crawler.calls]
        assert calls.count(RG_DOCS_A) == 1
        assert calls.count(RG_DOCS_B) == 1
        assert calls.count(RG_DOCS_C) == 1
        # path-key URL дренируется вместе с path-корнем, а не с host-корнем.
        path_root_at = calls.index(RG_PATH_ROOT)
        assert all(calls.index(url) > path_root_at for url in (RG_DOCS_A, RG_DOCS_B, RG_DOCS_C))
        host_root_at = calls.index(RG_HOST_ROOT)
        assert all(host_root_at < calls.index(url) < path_root_at for url in (RG_BLOG_A, RG_BLOG_B))
        assert build.truncated is False


def test_revalidation_does_not_charge_max_documents(tmp_path: Path) -> None:
    """``max_documents`` — потолок discovery: 30 известных URL не усекают продукт."""
    known = [f"https://corp.test/k/{i}" for i in range(30)]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        crawler = _StatefulCrawler(pages)
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=crawler,
            crawl_defaults=CrawlRunConfig(max_documents=5),
        )
        build = _build_of(result)
        record = build.truncation[0]
        assert record["truncated"] is False
        assert build.truncated is False
        # pages_fetched — только документы, которые добавил обход (авторский seed).
        assert record["pages_fetched"] == 1
        assert record["limits"]["max_documents"] == 5
        assert {url for url, _p in crawler.calls} == {RG_HOST_ROOT, *known}


def test_revalidated_page_is_leaf_and_does_not_expand_discovery(
    tmp_path: Path,
) -> None:
    """Ревалидированная страница — depth-0 лист: её ссылки не расширяют BFS."""
    known = "https://corp.test/known"
    links = [f"https://corp.test/from-known/{i}" for i in range(3)]
    html = (
        "<html><body>"
        + "".join(f'<a href="{url}">x</a>' for url in links)
        + "</body></html>"
    ).encode()
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, [known])
        pages = {
            RG_HOST_ROOT: b"root without links",
            known: html,
            **{url: b"should not be fetched" for url in links},
        }
        crawler = _StatefulCrawler(pages)
        store = SqliteStore(":memory:")
        _make_run(store)
        try:
            result = _state_run(
                tmp_path,
                state,
                pages=pages,
                urls={"p1": [RG_HOST_ROOT]},
                crawler=crawler,
                store=store,
                run_id=RUN_ID,
                crawl_defaults=CrawlRunConfig(max_depth=1),
            )
            calls = {url for url, _p in crawler.calls}
            assert calls == {RG_HOST_ROOT, known}
            assert not calls & set(links)
            assert len(_items(store)) == 2, "лист не добавил ни одного элемента сеанса"
            assert _build_of(result).revalidation_orphans == {}
        finally:
            store.close()


def test_revalidation_orphans_are_observation_only(tmp_path: Path) -> None:
    """Известный URL вне всех авторских корней: один origin-бакет, без truncation/errors."""
    orphan = "https://other.test/legacy/page"
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, [orphan])
        pages = _rg_pages(RG_PATH_ROOT, orphan, body=b"orphan page")
        crawler = _StatefulCrawler(pages)
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_PATH_ROOT]},
            crawler=crawler,
        )
        build = _build_of(result)
        assert build.revalidation_orphans == {
            "p1:https://other.test": {"selected": 1, "fetched": 1}
        }
        # Никакой synthetic-записи усечения: запись ровно одна — авторский корень.
        assert len(build.truncation) == 1
        assert build.truncation[0]["root"] == RG_PATH_ROOT
        assert build.truncated is False
        assert orphan in {url for url, _p in crawler.calls}
        assert "https://other.test" not in {url for url, _p in crawler.calls}
        assert not any("orphan" in err for err in result.errors)
        notes = [entry for entry in result.log_entries if isinstance(entry, str)]
        assert any("author-scope drift" in note for note in notes)


def test_revalidation_groups_survive_resume_without_truncation(
    tmp_path: Path,
) -> None:
    """Повторный прогон того же сеанса: те же 2 записи, ``truncated`` не поднимается."""
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A, RG_DOCS_B, RG_DOCS_C]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        store = SqliteStore(":memory:")
        _make_run(store)
        pages = _rg_pages(RG_HOST_ROOT, RG_PATH_ROOT, *known, body=b"resume text")
        try:
            first = _state_run(
                tmp_path / "r1",
                state,
                pages=pages,
                urls={"p1": [RG_HOST_ROOT, RG_PATH_ROOT]},
                crawler=_StatefulCrawler(pages),
                store=store,
                run_id=RUN_ID,
            )
            second = _state_run(
                tmp_path / "r2",
                state,
                pages=pages,
                urls={"p1": [RG_HOST_ROOT, RG_PATH_ROOT]},
                crawler=_StatefulCrawler(pages),
                store=store,
                run_id=RUN_ID,
            )
            for result in (first, second):
                build = _build_of(result)
                assert len(build.truncation) == 2
                assert {record["root"] for record in build.truncation} == {
                    RG_HOST_ROOT,
                    RG_PATH_ROOT,
                }
                assert build.truncated is False
            session = store.get_crawl_session(SESSION_ID)
            assert session is not None and session.status is CrawlSessionStatus.COMPLETED
        finally:
            store.close()


def test_blacklisted_root_still_revalidates_known_urls(tmp_path: Path) -> None:
    """Заблокированный seed не теряет группу: известные URL ревалидируются и видны."""
    root = "https://corp.test/docs/start"
    known = [f"{root}/page1", f"{root}/page2"]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(*known, body=b"revalidated")
        crawler = _StatefulCrawler(pages)
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [root]},
            crawler=crawler,
            crawl_defaults=CrawlRunConfig(exclude_paths=["/docs/start"]),
        )
        build = _build_of(result)
        record = build.truncation[0]
        assert record["root"] == root
        assert record["skipped_blacklisted"] == [root]
        assert build.truncated is False
        assert [url for url, _p in crawler.calls] == known
        assert any("seed зачёркнут блэклистом" in err for err in result.errors)


def test_state_none_keeps_previous_truncation_shape(tmp_path: Path) -> None:
    """``state is None``: прежняя форма — запись на авторский корень, без orphan-ключа."""
    pages = _rg_pages(RG_HOST_ROOT, RG_PATH_ROOT, body=b"legacy")
    result = _build(
        tmp_path,
        pages=pages,
        urls={"p1": [RG_HOST_ROOT, RG_PATH_ROOT]},
    )
    build = _build_of(result)
    assert len(build.truncation) == 2
    assert {record["root"] for record in build.truncation} == {
        RG_HOST_ROOT,
        RG_PATH_ROOT,
    }
    assert build.revalidation_orphans == {}
    assert not [entry for entry in result.log_entries if isinstance(entry, str)]


# --------------------------------------------------------------------------- #
# Ops-потолок ревалидации (crawl.revalidate_max_documents): default-off, видимый defer
# --------------------------------------------------------------------------- #


def _revalidation_counts(build: IndexBuild) -> tuple[int, int]:
    """Суммы ``selected``/``deferred`` по корневым записям и orphan-бакетам.

    Продуктовый потолок обязан считаться по продукту целиком — поэтому суммы
    проверяются вместе, а не по каждой записи отдельно.
    """
    selected = deferred = 0
    for record in build.truncation:
        info = record.get("revalidation") or {}
        selected += int(info.get("selected", 0))
        deferred += int(info.get("deferred", 0))
    for info in build.revalidation_orphans.values():
        selected += int(info.get("selected", 0))
        deferred += int(info.get("deferred", 0))
    return selected, deferred


def _revalidation_accounting(build: IndexBuild) -> tuple[int, int, int]:
    """``(selected, deferred, skipped)`` по корневым записям и orphan-бакетам."""
    selected = deferred = skipped = 0
    for record in build.truncation:
        info = record.get("revalidation") or {}
        selected += int(info.get("selected", 0))
        deferred += int(info.get("deferred", 0))
        skipped += int(info.get("skipped", 0))
    for info in build.revalidation_orphans.values():
        selected += int(info.get("selected", 0))
        deferred += int(info.get("deferred", 0))
        skipped += int(info.get("skipped", 0))
    return selected, deferred, skipped


def test_revalidate_cap_axis_absent_is_default_off(tmp_path: Path) -> None:
    """Ось не задана: все известные URL ревалидируются, ``stale`` не пишется.

    ``CrawlLimits.as_dict()`` не добавляет ключ, поэтому дайджест прогонов без оси
    байт-идентичен прежнему (проверяем форму напрямую).
    """
    assert "revalidate_max_documents" not in CrawlLimits().as_dict()
    assert CrawlLimits(revalidate_max_documents=2).as_dict()[
        "revalidate_max_documents"
    ] == 2
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A, RG_DOCS_B, RG_DOCS_C]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        crawler = _StatefulCrawler(pages)
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=crawler,
            crawl_defaults=CrawlRunConfig(),
        )
        build = _build_of(result)
        assert {url for url, _plan in crawler.calls} == {RG_HOST_ROOT, *known}
        assert build.truncated is False
        assert build.truncation[0]["revalidation"] == {
            "selected": len(known),
            "fetched": len(known),
        }
        assert "deferred" not in build.truncation[0]["revalidation"]
        assert all(
            state.get_url_state("p1", url).reuse_kind != "stale" for url in known
        )


def test_revalidate_cap_defers_tail_with_stale_and_visible_truncation(
    tmp_path: Path,
) -> None:
    """``revalidate_max_documents=2`` из 5: 2 ревалидированы, 3 отложены и ``stale``."""
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A, RG_DOCS_B, RG_DOCS_C]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        crawler = _StatefulCrawler(pages)
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=crawler,
            crawl_defaults=CrawlRunConfig(revalidate_max_documents=2),
        )
        build = _build_of(result)
        fetched_known = {url for url, _plan in crawler.calls if url in known}
        assert fetched_known == {RG_BLOG_A, RG_BLOG_B}
        assert state.get_url_state("p1", RG_DOCS_A).reuse_kind == "stale"
        assert state.get_url_state("p1", RG_DOCS_B).reuse_kind == "stale"
        assert state.get_url_state("p1", RG_DOCS_C).reuse_kind == "stale"
        assert build.truncated is True
        assert build.truncation[0]["truncated"] is True
        assert build.truncation[0]["truncated_reason"] == "revalidate_max_documents"
        assert build.truncation[0]["revalidation"] == {
            "selected": 2,
            "fetched": 2,
            "deferred": 3,
        }
        assert _revalidation_counts(build) == (2, 3)
        summary = truncation_summary({"effective_limits": {"roots": build.truncation}})
        assert summary["truncated_reason"] == "revalidate_max_documents"


def test_revalidate_cap_rotates_oldest_first(tmp_path: Path) -> None:
    """Второй прогон выбирает другой пары: отложенные (старые) идут первыми."""
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A, RG_DOCS_B, RG_DOCS_C]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        first = _state_run(
            tmp_path / "r1",
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=_StatefulCrawler(pages),
            crawl_defaults=CrawlRunConfig(revalidate_max_documents=2),
        )
        assert _build_of(first).truncation[0]["revalidation"] == {
            "selected": 2,
            "fetched": 2,
            "deferred": 3,
        }
        second_crawler = _StatefulCrawler(pages)
        second = _state_run(
            tmp_path / "r2",
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=second_crawler,
            crawl_defaults=CrawlRunConfig(revalidate_max_documents=2),
        )
        fetched_known = {
            url for url, _plan in second_crawler.calls if url in known
        }
        # Первые два (blog/a, blog/b) теперь имеют свежий last_checked_at; хвост — старый.
        assert fetched_known == {RG_DOCS_A, RG_DOCS_B}
        assert state.get_url_state("p1", RG_BLOG_A).reuse_kind == "stale"
        assert state.get_url_state("p1", RG_BLOG_B).reuse_kind == "stale"
        assert state.get_url_state("p1", RG_DOCS_C).reuse_kind == "stale"
        assert _revalidation_counts(_build_of(second)) == (2, 3)


def test_revalidate_cap_precedence_keeps_crawl_ceiling_reason(tmp_path: Path) -> None:
    """``max_documents`` и capped-ревалидация: ``truncation_summary`` сообщает crawl-потолок."""
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A, RG_DOCS_B, RG_DOCS_C]
    discoverable = "https://corp.test/new"
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = {
            RG_HOST_ROOT: (
                f'<a href="{discoverable}">new</a>'.encode()
            ),
            discoverable: b"new",
            **{url: b"text" for url in known},
        }
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=_StatefulCrawler(pages),
            crawl_defaults=CrawlRunConfig(
                max_depth=1,
                max_documents=1,
                revalidate_max_documents=2,
            ),
        )
        build = _build_of(result)
        record = build.truncation[0]
        assert record["truncated_reason"] == "max_documents"
        assert record["revalidation"]["selected"] == 2
        assert record["revalidation"]["deferred"] == 3
        summary = truncation_summary({"effective_limits": {"roots": build.truncation}})
        assert summary["truncated_reason"] == "max_documents"


def test_revalidate_cap_is_product_wide_across_roots_and_orphans(
    tmp_path: Path,
) -> None:
    """Cap делит N между корнями и orphan-бакетами; deferred виден в обеих записях."""
    orphan_a = "https://other.test/legacy/a"
    orphan_b = "https://other.test/legacy/b"
    known = [
        RG_BLOG_A,
        RG_BLOG_B,
        RG_DOCS_A,
        RG_DOCS_B,
        RG_DOCS_C,
        orphan_a,
        orphan_b,
    ]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(
            RG_HOST_ROOT, RG_PATH_ROOT, *known, body=b"text"
        )
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT, RG_PATH_ROOT]},
            crawler=_StatefulCrawler(pages),
            crawl_defaults=CrawlRunConfig(revalidate_max_documents=3),
        )
        build = _build_of(result)
        by_root = {record["root"]: record for record in build.truncation}
        assert by_root[RG_HOST_ROOT]["revalidation"] == {
            "selected": 2,
            "fetched": 2,
            "deferred": 0,
        }
        assert by_root[RG_PATH_ROOT]["revalidation"] == {
            "selected": 1,
            "fetched": 1,
            "deferred": 2,
        }
        assert build.revalidation_orphans == {
            "p1:https://other.test": {
                "selected": 0,
                "fetched": 0,
                "deferred": 2,
            }
        }
        assert _revalidation_counts(build) == (3, 4)
        assert build.truncated is True


def test_revalidate_cap_split_transport_is_visibly_unsupported(
    tmp_path: Path,
) -> None:
    """``split`` не исполняет локальную политику отбора — ось явно игнорируется."""
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        store = SqliteStore(":memory:")
        _make_run(store)
        try:
            executor = _FakeExecutor(
                pages=_rg_pages(RG_HOST_ROOT, *known, body=b"text")
            )
            result = _run_executor_build(
                tmp_path,
                store,
                executor,
                urls={"p1": [RG_HOST_ROOT]},
                state=state,
                product_modes={"p1": "incremental"},
                crawl_defaults=CrawlRunConfig(revalidate_max_documents=1),
            )
            assert any(
                "revalidate_max_documents" in error and "split" in error
                for error in result.errors
            )
            submitted = {work.url for wave in executor.submits for work in wave}
            assert set(known) <= submitted
            build = _build_of(result)
            assert build.truncated is False
            assert all(
                state.get_url_state("p1", url).reuse_kind != "stale" for url in known
            )
            assert all(
                "deferred" not in (record.get("revalidation") or {})
                for record in build.truncation
            )
        finally:
            store.close()


def test_revalidate_cap_prefilters_ineligible_and_rotates(tmp_path: Path) -> None:
    """M1: блэклист/pre-fetch-пропуск не занимают слот потолка навсегда.

    ``cap=1``, два неисполнимых известных URL (``exclude_paths`` и ``.pdf`` при
    ``document_level=off``) и два исполнимых: каждый цикл обязан ревалидировать
    **другой** исполнимый URL, а ``selected+deferred+skipped == len(known)``.
    """
    pdf = "https://corp.test/blog/scan.pdf"
    known = [RG_BLOG_A, pdf, RG_DOCS_A, RG_DOCS_B]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        defaults = CrawlRunConfig(
            exclude_paths=["/blog/a"],
            document_level=DocumentLevel.OFF,
            revalidate_max_documents=1,
        )
        first_crawler = _StatefulCrawler(pages)
        first = _state_run(
            tmp_path / "r1",
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=first_crawler,
            crawl_defaults=defaults,
        )
        first_build = _build_of(first)
        first_fetched = {url for url, _plan in first_crawler.calls if url in known}
        assert first_fetched == {RG_DOCS_A}
        assert first_build.truncation[0]["revalidation"] == {
            "selected": 1,
            "fetched": 1,
            "deferred": 1,
            "skipped": 2,
        }
        assert _revalidation_accounting(first_build) == (1, 1, 2)
        assert state.get_url_state("p1", RG_DOCS_B).reuse_kind == "stale"

        second_crawler = _StatefulCrawler(pages)
        second = _state_run(
            tmp_path / "r2",
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=second_crawler,
            crawl_defaults=defaults,
        )
        second_build = _build_of(second)
        second_fetched = {url for url, _plan in second_crawler.calls if url in known}
        assert second_fetched == {RG_DOCS_B}
        assert _revalidation_accounting(second_build) == (1, 1, 2)


def test_executor_revalidation_waves_do_not_trip_breaker(tmp_path: Path) -> None:
    """M2: 1005 известных URL, ``max_documents=2`` — все отправлены, без round_budget."""
    known = [f"https://corp.test/known/{i:04d}" for i in range(1005)]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        store = SqliteStore(":memory:")
        _make_run(store)
        try:
            pages = {RG_HOST_ROOT: b"root", **{url: b"text" for url in known}}
            executor = _FakeExecutor(pages=pages)
            result = _run_executor_build(
                tmp_path,
                store,
                executor,
                urls={"p1": [RG_HOST_ROOT]},
                state=state,
                product_modes={"p1": "incremental"},
                crawl_defaults=CrawlRunConfig(max_documents=2),
                executor_wave_items=64,
            )
            submitted = [work.url for wave in executor.submits for work in wave]
            assert set(known) <= set(submitted)
            assert all(len(wave) <= 64 for wave in executor.submits)
            assert not any("предохранитель" in error for error in result.errors)
            build = _build_of(result)
            assert all(
                record.get("truncated_reason") != "round_budget"
                for record in build.truncation
            )
        finally:
            store.close()


def test_revalidate_cap_leftover_catalog_bucket_is_visible(tmp_path: Path) -> None:
    """S2: группа известных URL под каталогом — видимый бакет ``via: catalog``."""
    known = [f"{FEED_URL}/legacy/a", f"{FEED_URL}/legacy/b"]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        store = SqliteStore(":memory:")
        _make_run(store)
        try:
            pages = {ENTRY_1: b"entry", **{url: b"text" for url in known}}
            result = _state_run(
                tmp_path,
                state,
                pages=pages,
                urls={"p1": [FEED_URL]},
                crawler=_StatefulCrawler(pages),
                store=store,
                run_id=RUN_ID,
                crawl_sources=_feed_sources(),
                pollers={"feed": _FakePoller([FeedEntry(url=ENTRY_1)])},
                crawl_defaults=CrawlRunConfig(revalidate_max_documents=1),
            )
            build = _build_of(result)
            info = build.revalidation_orphans[f"p1:catalog:{FEED_URL}"]
            assert info == {
                "selected": 1,
                "fetched": 1,
                "deferred": 1,
                "via": "catalog",
            }
            assert _revalidation_accounting(build) == (1, 1, 0)
        finally:
            store.close()


def test_revalidation_orphans_one_bucket_per_origin(tmp_path: Path) -> None:
    """M1/S2: вне scope — ровно один synthetic-бакет на ``(product, origin)``."""
    known = [
        "https://other.test/a",
        "https://other.test/b",
        "https://third.test/a",
    ]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=_StatefulCrawler(pages),
        )
        build = _build_of(result)
        assert set(build.revalidation_orphans) == {
            "p1:https://other.test",
            "p1:https://third.test",
        }
        assert build.revalidation_orphans["p1:https://other.test"] == {
            "selected": 2,
            "fetched": 2,
        }
        assert build.revalidation_orphans["p1:https://third.test"] == {
            "selected": 1,
            "fetched": 1,
        }


def test_revalidate_cap_state_read_failure_is_visible_and_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M1: сбой ``iter_url_state`` в cap — явная ошибка, ось игнорируется целиком."""
    known = [RG_BLOG_A, RG_DOCS_A]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        original = state.iter_url_state
        calls = 0

        def _maybe_raise(product_id: str):  # noqa: ANN202 — тестовая обёртка
            nonlocal calls
            calls += 1
            if calls > 1:
                raise RuntimeError("state unavailable")
            return original(product_id)

        monkeypatch.setattr(state, "iter_url_state", _maybe_raise)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        crawler = _StatefulCrawler(pages)
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=crawler,
            crawl_defaults=CrawlRunConfig(revalidate_max_documents=1),
        )
        fetched = {url for url, _plan in crawler.calls if url in known}
        assert fetched == set(known)
        errors = [
            error for error in result.errors if "state.iter_url_state" in error
        ]
        assert len(errors) == 1
        build = _build_of(result)
        assert build.truncation[0]["revalidation"] == {
            "selected": 2,
            "fetched": 2,
        }
        assert all(
            state.get_url_state("p1", url).reuse_kind != "stale" for url in known
        )


def test_revalidation_fetched_is_not_pages_fetched(tmp_path: Path) -> None:
    """S1: ``pages_fetched`` остаётся discovery-only, ревалидация — в ``fetched``."""
    known = [RG_BLOG_A, RG_BLOG_B, RG_DOCS_A]
    with SqliteStateStore(":memory:") as state:
        _seed_known_urls(state, known)
        pages = _rg_pages(RG_HOST_ROOT, *known, body=b"text")
        result = _state_run(
            tmp_path,
            state,
            pages=pages,
            urls={"p1": [RG_HOST_ROOT]},
            crawler=_StatefulCrawler(pages),
        )
        record = _build_of(result).truncation[0]
        assert record["pages_fetched"] == 1
        assert record["revalidation"] == {"selected": 3, "fetched": 3}


def test_revalidate_cap_split_no_false_error_without_cappable_items(
    tmp_path: Path,
) -> None:
    """S3: split-ошибка не возникает, если потолку нечего ограничивать."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        executor = _FakeExecutor(pages={RG_HOST_ROOT: b"text"})
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [RG_HOST_ROOT]},
            crawl_defaults=CrawlRunConfig(revalidate_max_documents=1),
        )
        assert not any(
            "revalidate_max_documents" in error and "split" in error
            for error in result.errors
        )
    finally:
        store.close()
