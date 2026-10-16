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
from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlRunConfig,
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
    _derive_index_build_id,
    _work_set_idempotency_key,
    build_corpus,
)
from core.pipeline.crawl_session import default_session_id
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
from core.store.sqlite_store import SqliteStore
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


class _Extractor:
    """Экстрактор: текст — декодированные байты; ``error_urls`` → грациозная ошибка."""

    version = "extractor-1"

    def __init__(self, *, error_urls: set[str] | None = None) -> None:
        self.error_urls = set(error_urls or ())

    def extract(self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO) -> ExtractedDoc:
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
            assert row["build_id"] == build.index_build_id
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
    other_snapshot = _build_of(
        _build(tmp_path / "c", pages=pages, urls=urls, snapshot_date="2026-02-01")
    ).index_build_id
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
        other_snapshot,
        other_embedder,
        other_extractor,
        other_lexical,
        other_vector,
    }
    assert len(ids) == 7


def test_index_build_id_ignores_retrieval_config() -> None:
    """P2: ``retrieval_*`` не входят в формулу id (ретривал не меняет чанкинг/векторы)."""
    params = set(inspect.signature(_derive_index_build_id).parameters)
    assert not {name for name in params if name.startswith("retrieval")}
    assert {
        "chunker_version",
        "embed_model_id",
        "extractor_version",
        "lexical_engine_version",
        "vector_store",
        "content_token",
    } <= params


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
    assert len({e.index_build_id for e in build.entries}) == 1
    for entry in build.entries:
        assert entry.partition_key == f"{entry.index_build_id}:{entry.product_id}"
        assert entry.store_path == (
            f"index_store/{entry.package_id}/{entry.index_build_id}/{entry.product_id}/"
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
        key = (meta.package, (meta.extra or {}).get("index_build_id", ""), meta.product_id)
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
        build_id = _build_of(first).index_build_id
        manifest_before = _manifest_row(store, build_id, "p1")
        assert manifest_before is not None
        assert _chunk_count(store, build_id) == 1

        second = _build(
            tmp_path / "second",
            pages={URL_P1_A: b"second-a", URL_P1_B: b"second-b"},
            urls={"p1": [URL_P1_A, URL_P1_B]},
            store=store,
            embedder=_Embedder(),
            index_builder=builder,
        )
        build_id_2 = _build_of(second).index_build_id
        # Контент другой ⇒ РАЗНЫЙ id (контент-адресуемость P2).
        assert build_id_2 != build_id
        assert second.errors == []

        # Прошлая сборка не изменилась: БД согласована с иммутабельной партицией на диске.
        assert _manifest_row(store, build_id, "p1") == manifest_before
        assert _chunk_count(store, build_id) == 1
        # Новая сборка зафиксирована собственной партицией.
        assert _manifest_row(store, build_id_2, "p1") is not None
        assert _chunk_count(store, build_id_2) == 2
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
        partition = index_format.partition_dir(
            index_root, package_id, build.index_build_id, "p1"
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
        assert _chunk_count(store, build.index_build_id) == len(physical_lines)
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
        assert _chunk_count(store, build.index_build_id) == 1
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
    """Пейсинг-шпион: пишет ``(host, spec)`` в ``wait``; ``slept`` — фактические задержки."""

    def __init__(self, *, rng: random.Random | None = None) -> None:
        self.slept: list[float] = []
        super().__init__(sleep=self.slept.append, rng=rng)
        self.calls: list[tuple[str, RateLimitSpec | None]] = []

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
    ``pages``/``errors``/``engines`` — исход по URL. Журналирует submit/status/collect.
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
    ) -> None:
        self.pages = dict(pages or {})
        self.errors = dict(errors or {})
        self.engines = dict(engines or {})
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


def test_executor_directory_feed_two_waves(tmp_path: Path) -> None:
    """Phase 5.3: feed-каталог → волна 1 (top-level) + волна 2 (записи), два submit."""
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
        # Две волны → два submit с разными ключами идемпотентности (каталог не подан).
        assert len(executor.submits) == 2
        wave1 = [w.url for w in executor.submits[0]]
        wave2 = [w.url for w in executor.submits[1]]
        assert wave1 == [URL_P1_A]
        assert wave2 == [EXEC_E1, EXEC_E2]
        assert EXEC_FEED_URL not in wave1 + wave2
        keys = [k["idempotency_key"] for k in executor.submit_kwargs]
        assert keys[0] != keys[1]
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


def test_executor_work_order_mirrors_local_interleaving(tmp_path: Path) -> None:
    """Порядок ingest в Pass A зеркалит локальный: записи каталога — на месте каталога."""
    store = SqliteStore(":memory:")
    _make_run(store)
    # URL сортируются лексикографически (``url_map``); ``zzz.test`` идёт после ``feed.test``,
    # поэтому записи каталога обязаны встать МЕЖДУ top-level URL (в месте каталога).
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
        # Волна 1 — top-level в порядке url_map; волна 2 — записи в порядке поллера.
        assert [w.url for w in executor.submits[0]] == [URL_P1_A, zzz]
        assert [w.url for w in executor.submits[1]] == [EXEC_E1, EXEC_E2]
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
            URL_P1_A: b"seed page",
            SITEMAP_LOC_1: b"loc one",
            SITEMAP_LOC_2: b"loc two",
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        # sitemap опрошен один раз; страницы (seed + locs) — краулером.
        assert [c[0] for c in poller.calls] == [SITEMAP_URL]
        assert [u for u, _ in crawler.calls] == [URL_P1_A, SITEMAP_LOC_1, SITEMAP_LOC_2]
        build = _build_of(result)
        assert build.chunk_count == 3
        assert {d.url for d in build.docs} == {URL_P1_A, SITEMAP_LOC_1, SITEMAP_LOC_2}

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
            URL_P1_A: b"seed",
            SITEMAP_URL: _gzip.compress(_urlset(SITEMAP_LOC_1)),
            SITEMAP_LOC_1: b"loc",
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": LocalFeedPoller(crawler)},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert {d.url for d in _build_of(result).docs} == {URL_P1_A, SITEMAP_LOC_1}
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
            URL_P1_A: b"seed",
            SITEMAP_URL: index,
            SITEMAP_CHILD: index,  # дочерний — сам индекс → глубже одного уровня
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
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
        assert URL_P1_A in {d.url for d in _build_of(result).docs}
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
            URL_P1_A: b"seed",
            SITEMAP_URL: _urlset(SITEMAP_LOC_1, SITEMAP_LOC_2),
            SITEMAP_LOC_1: b"a",
            SITEMAP_LOC_2: b"b",
        }
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": LocalFeedPoller(crawler, max_entries=1)},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert {d.url for d in _build_of(result).docs} == {URL_P1_A, SITEMAP_LOC_1}
    finally:
        store.close()


def test_sitemap_loc_ssrf_rejected(tmp_path: Path) -> None:
    """Небезопасный ``<loc>`` (приватный IP) отбрасывается до становления seed."""
    store = SqliteStore(":memory:")
    _make_run(store)
    unsafe = "http://127.0.0.1/secret"
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1), FeedEntry(url=unsafe)])
    crawler = _Crawler({URL_P1_A: b"seed", SITEMAP_LOC_1: b"ok"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert any("sitemap <loc>" in e for e in result.errors)
        assert unsafe not in [u for u, _ in crawler.calls]
        assert {d.url for d in _build_of(result).docs} == {URL_P1_A, SITEMAP_LOC_1}
    finally:
        store.close()


def test_sitemap_loc_obeys_blacklist(tmp_path: Path) -> None:
    """``<loc>`` подчиняется блэклисту профиля (``exclude_documents``) как обычный seed."""
    store = SqliteStore(":memory:")
    _make_run(store)
    pdf = "https://docs.test/p1/private/report.pdf"
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1), FeedEntry(url=pdf)])
    crawler = _Crawler({URL_P1_A: b"seed", SITEMAP_LOC_1: b"ok"})
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(exclude_documents=["*.pdf"]),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert any("блэклист" in e for e in result.errors)
        assert pdf not in [u for u, _ in crawler.calls]
        assert {d.url for d in _build_of(result).docs} == {URL_P1_A, SITEMAP_LOC_1}
    finally:
        store.close()


def test_sitemap_seeds_obey_max_documents(tmp_path: Path) -> None:
    """``max_documents`` — жёсткий потолок и для sitemap-seeds (тихая штатная остановка)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1), FeedEntry(url=SITEMAP_LOC_2)])
    crawler = _Crawler(
        {URL_P1_A: b"seed", SITEMAP_LOC_1: b"a", SITEMAP_LOC_2: b"b"}
    )
    try:
        result = _build(
            tmp_path,
            pages={},
            urls={"p1": [URL_P1_A]},
            crawler=crawler,
            domain_profiles=_sitemap_profile(max_documents=1),
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        build = _build_of(result)
        assert {d.url for d in build.docs} == {URL_P1_A}
        assert build.truncated is True
        assert any(rec["truncated_reason"] == "max_documents" for rec in build.truncation)
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
            pages={URL_P1_A: b"only seed"},
            urls={"p1": [URL_P1_A]},
            crawler=_Crawler({URL_P1_A: b"only seed"}),
            domain_profiles={"docs.test": DomainProfile()},
            pollers={"sitemap": poller},
            store=store,
            run_id=RUN_ID,
        )
        assert result.errors == []
        assert poller.calls == []
        assert {d.url for d in _build_of(result).docs} == {URL_P1_A}
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
            urls={"p1": [URL_P1_A]},
            crawler=_Crawler({URL_P1_A: b"seed"}),
            domain_profiles=_sitemap_profile(),
            pollers=None,
            store=store,
            run_id=RUN_ID,
        )
        assert any("поллер sitemap не сконфигурирован" in e for e in result.errors)
        items = {i.url: i for i in _items(store)}
        assert items[SITEMAP_URL].status is CrawlItemStatus.FAILED
        assert URL_P1_A in {d.url for d in _build_of(result).docs}
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


def test_executor_sitemap_locs_go_to_wave2(tmp_path: Path) -> None:
    """``executor``/split: ``<loc>`` sitemap подаются волной 2 (как записи каталога)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    poller = _FakePoller([FeedEntry(url=SITEMAP_LOC_1)])
    executor = _FakeExecutor(pages={URL_P1_A: b"top", SITEMAP_LOC_1: b"loc"})
    try:
        result = _run_executor_build(
            tmp_path,
            store,
            executor,
            urls={"p1": [URL_P1_A]},
            domain_profiles=_sitemap_profile(),
            pollers={"sitemap": poller},
        )
        assert result.errors == []
        assert [w.url for w in executor.submits[0]] == [URL_P1_A]
        assert [w.url for w in executor.submits[1]] == [SITEMAP_LOC_1]
        assert {d.url for d in _build_of(result).docs} == {URL_P1_A, SITEMAP_LOC_1}
    finally:
        store.close()


