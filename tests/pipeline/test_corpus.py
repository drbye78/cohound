"""Тесты сборки корпуса Ш5 (W7/T7.4): retention, dedup, I17/I19, детерминизм, персистенция.

Порты подменяются детерминированными in-memory заглушками (без сети/I/O, кроме ``tmp_path``
для retention). Конкретные адаптеры (``core/adapters/*``) не импортируются — тестируется
только контракт портов (A1). Нейтральные фикстуры: ``p1/p2``, ``param_a``, ``mod_a``.
"""

from __future__ import annotations

import datetime as _dt
import inspect
import json
from pathlib import Path

import pytest

from core.adapters import index_format
from core.adapters.index_npy import NpyIndexBuilder
from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, _derive_index_build_id, build_corpus
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

try:  # необязательная зависимость (extra ``retrieval``)
    import numpy as _np  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - зависит от окружения
    _np = None  # type: ignore[assignment]

requires_numpy = pytest.mark.skipif(_np is None, reason="numpy не установлен (extra retrieval)")


class ImmutableBuildError(RuntimeError):
    """Локальное зеркало имени адаптерного типа: проверяем строку ошибки, а не класс (K2)."""

RUN_ID = "run-fixture"
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


class _Crawler:
    """Краулер: ``url -> bytes``; опционально URL, бросающие исключение."""

    version = "crawler-1"

    def __init__(
        self,
        pages: dict[str, bytes],
        *,
        raising: set[str] | None = None,
    ) -> None:
        self.pages = dict(pages)
        self.raising = set(raising or ())
        self.calls: list[tuple[str, FetchPlan]] = []

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
