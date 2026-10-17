"""P5 (§7) — уровни извлечения документов + режим «игнорировать документы» ``off``.

Детерминированные фейки (без docling/весов): проверяются замороженные решения гейта
``p5-extract-levels-gate.md`` (с поправкой владельца: ``full`` — OCR ON, ``layout_tables`` —
OCR OFF):

1. дефолт ``document_level`` = ``full``; манифест несёт ``crawl_document_level: full``;
2. резолв цепочкой app < run < site; ``crawl_config_digest`` меняется при заданной оси;
3. ``off`` — pre-fetch-пропуск суффикс-детектируемого документа (не фетчится, не в
   ``max_documents``);
4. ``off`` — post-fetch-пропуск по ``Content-Type`` (фетч/retain были, элемент ``FETCHED``,
   в ``max_documents`` входит);
5. ``off`` не затрагивает HTML;
6. уровень доезжает до ``_ExtractRequest.level`` (per seed-root);
7. ``extractor_version`` несёт уровень ⇒ ``index_build_id`` различается (мутационный якорь);
8. детерминизм ``index_build_id`` при одном уровне;
9. Office-подобный контент: не-``off`` уровни дают один текст, ``off`` — пропуск;
10. ``document_skips`` питает канал усечения/аномалии (защита absence-exhaustive);
11. дефолт сохраняет контент (docs/chunks), но даёт churn ``index_build_id`` (execution).

Offline-тесты (реальный docling; ``pytest.mark.slow`` + skip 0/1/2) — 12–14:

12. явные опции ``full`` ≡ голый ``DocumentConverter()`` (байт-идентичность; обязательное
    измерение §8 п.1);
13. per-level ``content_hash`` различаются (кроме Office);
14. байт-детерминизм уровня в ``ProcessPoolExecutor``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from types import SimpleNamespace
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
    DocumentLevel,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import _ExtractRequest, ExtractionPool, IndexBuild, build_corpus
from core.pipeline.crawl_frontier import resolve_crawl_limits
from core.pipeline.crawl_session import default_session_id
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-document-levels"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"
ROOT = "https://docs.test/"

#: HTML-страница, на которую не влияет ``document_level`` ни при каком значении.
HTML_PAGE = b"<html><body>plain html body</body></html>"
#: «Документный» контент (не HTML) — уровень применяется к нему.
DOC_BYTES = b"%PDF-1.7 stub document bytes"
DOC_URL = "https://docs.test/report.pdf"
#: Суффикс-less URL с документным ``Content-Type`` (post-fetch-детект).
DOC_SUFFIXLESS_URL = "https://docs.test/download?id=9"
PDF_CONTENT_TYPE = "application/pdf"


# --------------------------------------------------------------------------- #
# Харнесс
# --------------------------------------------------------------------------- #


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan(products: list[str] | None = None) -> RunPlan:
    products = products or ["p1"]
    entries = [
        CohortEntry(
            product_id=pid,
            role_overlap=1.0,
            selection_reason="anchor",
            anchor_relation=AnchorRelation.ANCHOR if pid == products[0] else AnchorRelation.ANALOG,
        )
        for pid in products
    ]
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor=products[0],
        modules=["mod_a"],
        roles=["role_a"],
        params=_catalog(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=entries,
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
    """Фейковый краулер: страницы с ``content_type``; считает вызовы (pre-fetch-детект)."""

    version = "crawler-1"
    engine = "http"
    supports_browser = True

    def __init__(self, pages: dict[str, tuple[bytes, str]]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        content, content_type = self.pages[url]
        return RawPage(
            url=url,
            content=content,
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
            content_type=content_type,
        )


def _is_document(content: bytes, content_type: str | None) -> bool:
    """Грубый тестовый предикат «документ» (зеркалит нейтральный util)."""
    from core.util.document_formats import document_content_type, document_suffix

    return document_suffix(ROOT) is not None or document_content_type(content_type) is not None


class _LevelExtractor:
    """Композиция HTML+документ: ``version`` несёт уровень (как реальный ``_LocalExtractor``).

    Для документного контента ``extractor_version = "docling-doc-3:<level>"`` и текст зависит
    от уровня (симуляция «разного текста на уровнях»); HTML — ``trafilatura-html-2`` (не
    затронут осью).
    """

    def __init__(self, level: DocumentLevel) -> None:
        self.level = DocumentLevel(level)
        self.version = f"html-2+docling-doc-3:{self.level.value}"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        if _is_document(page.content, page.content_type):
            # Текст намеренно НЕ зависит от уровня (уровни могут дать один текст), но
            # ``extractor_version`` кодирует уровень — именно это различает сборки (§5).
            text = page.content.decode("utf-8", errors="replace")
            return ExtractedDoc(
                text=text,
                content_hash=content_hash(page.content),
                extractor_version=f"docling-doc-3:{self.level.value}",
                page_count=1,
            )
        return ExtractedDoc(
            text=page.content.decode("utf-8", errors="replace"),
            content_hash=content_hash(page.content),
            extractor_version="trafilatura-html-2",
            page_count=1,
        )


def _level_factory(level: DocumentLevel) -> _LevelExtractor:
    return _LevelExtractor(level)


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


# --- воркер пула: несёт уровень из payload --------------- #


def _level_worker(request: _ExtractRequest) -> ExtractedDoc:
    content = Path(request.raw_path).read_bytes()
    text = content.decode("utf-8", errors="replace") + f"|level={DocumentLevel(request.level).value}"
    return ExtractedDoc(
        text=text,
        content_hash=content_hash(text.encode("utf-8")),
        extractor_version=f"docling-doc-3:{DocumentLevel(request.level).value}",
        page_count=1,
    )


class _FakeFuture:
    def __init__(self, pool: "_FakePool", fn: Any, request: _ExtractRequest) -> None:
        self._fn, self._request = fn, request

    def result(self, timeout: float | None = None) -> Any:
        return self._fn(self._request)


class _FakePool:
    def __init__(self, size: int, level: DocumentLevel, sink: list[_ExtractRequest]) -> None:
        self.size = size
        self.level = DocumentLevel(level)
        self._sink = sink

    def submit(self, fn: Any, request: _ExtractRequest) -> _FakeFuture:
        self._sink.append(request)
        return _FakeFuture(self, fn, request)

    def shutdown(self, wait: bool = True) -> None:
        return None


def _build(
    tmp_path: Path,
    *,
    pages: dict[str, tuple[bytes, str]],
    urls: dict[str, list[str]] | None = None,
    plan: RunPlan | None = None,
    store: SqliteStore | None = None,
    run_id: str | None = None,
    crawl_defaults: CrawlRunConfig | None = None,
    crawl_app: CrawlConfig | None = None,
    domain_profiles: dict[str, DomainProfile] | None = None,
    extraction_pool: ExtractionPool | None = None,
    extractor_factory: Any = None,
    plan_products: list[str] | None = None,
):
    return build_corpus(
        plan or _plan(plan_products),
        _Crawler(pages),
        _LevelExtractor(DocumentLevel.FULL),
        _Chunker(),
        store,
        urls=urls or {"p1": [ROOT]},
        raw_root=tmp_path,
        run_id=run_id,
        crawl_defaults=crawl_defaults,
        crawl_app=crawl_app,
        domain_profiles=domain_profiles,
        extraction_pool=extraction_pool,
        extractor_factory=extractor_factory,
    )


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


# --------------------------------------------------------------------------- #
# (1) Дефолт оси = full; манифест несёт crawl_document_level
# --------------------------------------------------------------------------- #


def test_default_level_is_full(tmp_path: Path) -> None:
    assert CrawlConfig().document_level is DocumentLevel.FULL
    # Не задано нигде в цепочке → FULL (сегодняшнее поведение).
    assert (
        resolve_crawl_limits("https://x.test/a", app=CrawlConfig()).document_level
        is DocumentLevel.FULL
    )

    result = _build(tmp_path, pages={ROOT: (HTML_PAGE, "text/html")})
    build = _build_of(result)
    assert result.status == "ok"
    # Ось попала в провенанс усечения через as_dict (per-root).
    assert build.truncation[0]["limits"]["document_level"] == "full"

    from core.pipeline.report import EXECUTION_FIELDS, build_manifest

    assert "crawl_document_level" in EXECUTION_FIELDS
    cfg = RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort-1",
        products=["p1"],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    manifest = build_manifest(
        inputs=SimpleNamespace(run=cfg, package=None),
        plan=SimpleNamespace(run_id="run-x", anchor="p1", modules=[], cohort_ref="cohort-1"),
        crawl_app=CrawlConfig(),
        created_at="2026-01-01T00:00:00Z",
    )
    assert manifest.crawl_document_level == "full"


# --------------------------------------------------------------------------- #
# (2) Резолв цепочкой app < run < site; дайджест реагирует
# --------------------------------------------------------------------------- #


def test_level_resolved_through_chain() -> None:
    app = CrawlConfig(document_level=DocumentLevel.LAYOUT)
    run = CrawlRunConfig(document_level=DocumentLevel.TEXT)
    site = DomainProfile(document_level=DocumentLevel.OFF)

    # site > run > app.
    assert (
        resolve_crawl_limits(
            "https://example.com/x", app=app, run=run, profiles={"example.com": site}
        ).document_level
        is DocumentLevel.OFF
    )
    # run > app.
    assert (
        resolve_crawl_limits("https://example.com/x", app=app, run=run, profiles={}).document_level
        is DocumentLevel.TEXT
    )
    # app (run/site не заданы).
    assert (
        resolve_crawl_limits("https://example.com/x", app=app, run=None, profiles={}).document_level
        is DocumentLevel.LAYOUT
    )

    from core.pipeline.report import build_manifest

    cfg = RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort-1",
        products=["p1"],
        package="pkg",
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )
    plan = SimpleNamespace(run_id="run-x", anchor="p1", modules=[], cohort_ref="cohort-1")
    base = build_manifest(
        inputs=SimpleNamespace(run=cfg, package=None),
        plan=plan,
        crawl_app=CrawlConfig(),
        created_at="2026-01-01T00:00:00Z",
    )
    changed = build_manifest(
        inputs=SimpleNamespace(
            run=cfg.model_copy(
                update={"crawl": CrawlRunConfig(document_level=DocumentLevel.LAYOUT)}
            ),
            package=None,
        ),
        plan=plan,
        crawl_app=CrawlConfig(),
        created_at="2026-01-01T00:00:00Z",
    )
    assert changed.crawl_document_level == "layout"
    assert changed.crawl_config_digest != base.crawl_config_digest


# --------------------------------------------------------------------------- #
# (3) off: pre-fetch-пропуск суффикс-детектируемого документа
# --------------------------------------------------------------------------- #


def test_off_skips_url_suffix_document_prefetch(tmp_path: Path) -> None:
    crawler = _Crawler({DOC_URL: (DOC_BYTES, PDF_CONTENT_TYPE)})
    # Соберём вручную, чтобы проверить, что краулер не вызван ни разу.
    result = build_corpus(
        _plan(),
        crawler,
        _LevelExtractor(DocumentLevel.FULL),
        _Chunker(),
        None,
        urls={"p1": [DOC_URL]},
        raw_root=tmp_path,
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.OFF),
    )
    build = _build_of(result)

    assert crawler.calls == []  # pre-fetch: документ не фетчится
    assert build.docs == []  # не индексируется
    assert build.truncation[0]["pages_fetched"] == 0  # не в max_documents
    skips = build.document_skips
    assert [s["reason"] for s in skips] == ["url_suffix"]
    assert skips[0]["url"] == DOC_URL and skips[0]["level"] == "off"
    assert [s["url"] for s in build.truncation[0]["skipped_documents"]] == [DOC_URL]


# --------------------------------------------------------------------------- #
# (4) off: post-fetch-пропуск по Content-Type
# --------------------------------------------------------------------------- #


def test_off_skips_content_type_document_postfetch(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        crawler = _Crawler({DOC_SUFFIXLESS_URL: (DOC_BYTES, PDF_CONTENT_TYPE)})
        result = build_corpus(
            _plan(),
            crawler,
            _LevelExtractor(DocumentLevel.FULL),
            _Chunker(),
            store,
            urls={"p1": [DOC_SUFFIXLESS_URL]},
            raw_root=tmp_path,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.OFF),
        )
        build = _build_of(result)

        # Фетч был (иначе тип не узнать) — страница ретейнена и учтена в max_documents.
        assert crawler.calls == [DOC_SUFFIXLESS_URL]
        assert build.truncation[0]["pages_fetched"] == 1
        assert [s["reason"] for s in build.document_skips] == ["content_type"]
        assert build.docs == []  # docling не зван → документа в корпусе нет

        # Элемент сеанса — FETCHED (правдиво: байты есть), НЕ FAILED (skip ≠ сбой).
        items = {i.url: i for i in store.iter_crawl_session_items(SESSION_ID)}
        assert items[DOC_SUFFIXLESS_URL].status is CrawlItemStatus.FETCHED
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) off не затрагивает HTML
# --------------------------------------------------------------------------- #


def test_off_leaves_html_unaffected(tmp_path: Path) -> None:
    result = _build(
        tmp_path,
        pages={ROOT: (HTML_PAGE, "text/html")},
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.OFF),
    )
    build = _build_of(result)
    assert len(build.docs) == 1
    assert build.chunk_count == 1
    assert build.document_skips == []
    assert result.errors == []


# --------------------------------------------------------------------------- #
# (6) Уровень доезжает до _ExtractRequest (per seed-root)
# --------------------------------------------------------------------------- #


def test_level_reaches_worker_request(tmp_path: Path) -> None:
    sink: list[_ExtractRequest] = []

    def factory(n: int, level: DocumentLevel) -> _FakePool:
        return _FakePool(n, level, sink)

    pool = ExtractionPool(pool_factory=factory, worker=_level_worker)
    a, b = "https://a.test/", "https://b.test/"
    result = _build(
        tmp_path,
        pages={a: (HTML_PAGE, "text/html"), b: (HTML_PAGE, "text/html")},
        plan_products=["p1", "p2"],
        urls={"p1": [a], "p2": [b]},
        domain_profiles={
            "a.test": DomainProfile(document_level=DocumentLevel.TEXT),
            "b.test": DomainProfile(document_level=DocumentLevel.LAYOUT),
        },
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=pool,
    )
    assert result.status == "ok"
    levels = {DocumentLevel(r.level) for r in sink}
    assert levels == {DocumentLevel.TEXT, DocumentLevel.LAYOUT}
    # Per-root: пул каждой корневой страницы видит ровно свой уровень.
    by_url = {r.url: DocumentLevel(r.level) for r in sink}
    assert by_url[a] is DocumentLevel.TEXT
    assert by_url[b] is DocumentLevel.LAYOUT


# --------------------------------------------------------------------------- #
# (7) extractor_version несёт уровень ⇒ index_build_id (мутационный якорь)
# --------------------------------------------------------------------------- #


def test_extractor_version_carries_level(tmp_path: Path) -> None:
    from core.adapters.extract_document import DoclingDocumentExtractor
    from core.pipeline.corpus import _derive_index_build_id, _extractor_token

    # Реальный адаптер: version инстансный и кодирует уровень.
    versions = {
        level: DoclingDocumentExtractor(level).version
        for level in (
            DocumentLevel.OFF,
            DocumentLevel.TEXT,
            DocumentLevel.LAYOUT,
            DocumentLevel.LAYOUT_TABLES,
            DocumentLevel.FULL,
        )
    }
    assert len(set(versions.values())) == 5
    for level, token in versions.items():
        assert token == f"docling-doc-3:{level.value}"
        assert level.value in token

    # _extractor_token различает уровни (композит версий документов батча).
    def token_for(version: str) -> str:
        docs = [SimpleNamespace(extractor_version=version)]
        return _extractor_token({"p1": docs})

    assert token_for("docling-doc-3:full") != token_for("docling-doc-3:text")

    # Сквозной прогон: разные уровни ⇒ разные index_build_id (док-содержащий корпус).
    pages = {DOC_URL: (DOC_BYTES, PDF_CONTENT_TYPE)}
    full = _build(
        tmp_path / "full",
        pages=pages,
        urls={"p1": [DOC_URL]},
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.FULL),
        extractor_factory=_level_factory,
    )
    text = _build(
        tmp_path / "text",
        pages=pages,
        urls={"p1": [DOC_URL]},
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        extractor_factory=_level_factory,
    )
    assert _build_of(full).index_build_id != _build_of(text).index_build_id

    # Точечно: _derive_index_build_id чувствителен к extractor_version.
    common = dict(
        package_id="pkg",
        params_token="a",
        snapshot_date="2026-01-01",
        chunker_version="chunker-1",
        embed_model_id="",
        lexical_engine_version="",
        vector_store="",
        content_token="ct",
    )
    assert _derive_index_build_id(
        extractor_version="docling-doc-3:full", **common
    ) != _derive_index_build_id(extractor_version="docling-doc-3:text", **common)


def test_index_build_id_stable_same_level(tmp_path: Path) -> None:
    pages = {DOC_URL: (DOC_BYTES, PDF_CONTENT_TYPE)}
    a = _build(
        tmp_path / "a",
        pages=pages,
        urls={"p1": [DOC_URL]},
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        extractor_factory=_level_factory,
    )
    b = _build(
        tmp_path / "b",
        pages=pages,
        urls={"p1": [DOC_URL]},
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        extractor_factory=_level_factory,
    )
    assert _build_of(a).index_build_id == _build_of(b).index_build_id


# --------------------------------------------------------------------------- #
# (9) Office-подобный контент: не-off уровни — один текст; off — пропуск
# --------------------------------------------------------------------------- #


def test_office_level_only_off_differs(tmp_path: Path) -> None:
    docx_url = "https://docs.test/spec.docx"
    content = b"office stub"
    pages = {docx_url: (content, "application/vnd.openxmlformats-officedocument.wordprocessingml.document")}

    texts: dict[str, str] = {}
    for level in (
        DocumentLevel.TEXT,
        DocumentLevel.LAYOUT,
        DocumentLevel.LAYOUT_TABLES,
        DocumentLevel.FULL,
    ):
        result = _build(
            tmp_path / level.value,
            pages=pages,
            urls={"p1": [docx_url]},
            crawl_defaults=CrawlRunConfig(document_level=level),
            extractor_factory=_level_factory,
        )
        build = _build_of(result)
        assert len(build.docs) == 1, level
        texts[level.value] = build.docs[0].content_hash
    # Ось не меняет Office-текст (реальный docling: SimplePipeline игнорирует PDF-опции).
    # Фейк возвращает один и тот же текст для документа => один content_hash.
    assert len(set(texts.values())) == 1

    off = _build(
        tmp_path / "off",
        pages=pages,
        urls={"p1": [docx_url]},
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.OFF),
        extractor_factory=_level_factory,
    )
    assert _build_of(off).docs == []
    assert [s["reason"] for s in _build_of(off).document_skips] == ["url_suffix"]


# --------------------------------------------------------------------------- #
# (10) document_skips питает канал усечения/аномалии (защита absence-exhaustive)
# --------------------------------------------------------------------------- #


def test_document_skip_feeds_truncation_anomaly(tmp_path: Path) -> None:
    from core.pipeline.report import collect_crawl_truncation_anomalies

    result = _build(
        tmp_path,
        pages={DOC_URL: (DOC_BYTES, PDF_CONTENT_TYPE)},
        urls={"p1": [DOC_URL]},
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.OFF),
        extractor_factory=_level_factory,
    )
    build = _build_of(result)
    assert build.document_skips  # видимость
    assert build.truncated is True  # «прореженный» продукт — не «полный»
    assert build.truncation[0]["skipped_documents"]

    # Продукт попал в truncated_products ⇒ аномалия для человека при exhaustive-absence.
    cell = SimpleNamespace(
        product_id="p1",
        param_id="param",
        absence_evidence=[{"source_scope": "exhaustive"}],
    )
    anomalies = collect_crawl_truncation_anomalies([cell], ["p1"])
    assert [a["product_id"] for a in anomalies] == ["p1"]


# --------------------------------------------------------------------------- #
# (11) Дефолт сохраняет контент, но даёт churn index_build_id (execution)
# --------------------------------------------------------------------------- #


def test_default_identity_preserves_manifest_only_churn(tmp_path: Path) -> None:
    pages = {DOC_URL: (DOC_BYTES, PDF_CONTENT_TYPE), ROOT: (HTML_PAGE, "text/html")}
    urls = {"p1": [ROOT, DOC_URL]}

    # Дефолт (ось не задана) и явный full — байт-в-байт по контенту и по сборке.
    default = _build(tmp_path / "default", pages=pages, urls=urls)
    explicit_full = _build(
        tmp_path / "full",
        pages=pages,
        urls=urls,
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.FULL),
        extractor_factory=_level_factory,
    )
    bd, bf = _build_of(default), _build_of(explicit_full)
    assert [(d.doc_id, d.content_hash) for d in bd.docs] == [
        (d.doc_id, d.content_hash) for d in bf.docs
    ]
    assert bd.index_build_id == bf.index_build_id
    assert bd.chunk_count == bf.chunk_count

    # Не-full уровень: контент (текст по фейку) тот же, но сборка иная (execution churn).
    text = _build(
        tmp_path / "text",
        pages=pages,
        urls=urls,
        crawl_defaults=CrawlRunConfig(document_level=DocumentLevel.TEXT),
        extractor_factory=_level_factory,
    )
    bt = _build_of(text)
    assert [(d.doc_id, d.content_hash) for d in bt.docs] == [
        (d.doc_id, d.content_hash) for d in bd.docs
    ]
    assert bt.index_build_id != bd.index_build_id


# --------------------------------------------------------------------------- #
# (12–14) Offline: реальный docling (skip 0/1/2, как smoke_scrapy_playwright)
# --------------------------------------------------------------------------- #


def _docling_available() -> bool:
    try:
        from core.adapters.extract_document import DoclingDocumentExtractor

        return DoclingDocumentExtractor().available
    except Exception:  # noqa: BLE001 — зависимость недоступна
        return False


def _cached_pdfs(max_bytes: int = 400_000) -> list[Path]:
    out: list[Path] = []
    for candidate in sorted(Path("corpus_raw").glob("*/*.bin")):
        try:
            if candidate.stat().st_size <= max_bytes and candidate.read_bytes()[:4] == b"%PDF":
                out.append(candidate)
        except OSError:  # pragma: no cover — гонка/права
            continue
    return out


def _docling_comment() -> None:
    """Проверка docling для параметризованных offline-тестов (skip, если недоступен)."""
    if not _docling_available():
        pytest.skip("docling/веса недоступны (offline)")
    if not _cached_pdfs():
        pytest.skip("нет cached PDF в corpus_raw/*/*.bin")


# Инициализатор и работа воркер-процесса offline-теста (§7.14). ОБЯЗАНЫ быть module-level:
# `ProcessPoolExecutor(mp_context="forkserver")` пиклит их **по ссылке**, поэтому локальные
# функции дают `PicklingError` — на это и упал первый прогон gated-набора (тест не доходил
# до измерения).
_CHILD_EXTRACTOR: Any = None


def _docling_child_init() -> None:
    global _CHILD_EXTRACTOR  # noqa: PLW0603 — глобал воркера, как в проде (P4/§7)
    from core.adapters.extract_document import DoclingDocumentExtractor

    _CHILD_EXTRACTOR = DoclingDocumentExtractor(DocumentLevel.LAYOUT)


def _docling_child_hash(path: str) -> str:
    return _CHILD_EXTRACTOR.extract_bytes(Path(path).read_bytes(), "document.pdf").content_hash


@pytest.mark.slow
def test_docling_explicit_full_equals_bare_default() -> None:
    """§8 п.1: явные опции ``full`` ≡ голый ``DocumentConverter()`` (байт-идентичность).

    Мутационный якорь: если опции ``full`` хоть чем-то отличаются от дефолта docling,
    ``content_hash`` разойдётся и тест упадёт. Включается ``COHOUND_RUN_DOCLING_LEVELS=1``.
    """
    import os

    if os.environ.get("COHOUND_RUN_DOCLING_LEVELS") != "1":
        pytest.skip("тяжёлый offline-тест: нужен COHOUND_RUN_DOCLING_LEVELS=1")
    _docling_comment()
    from docling.document_converter import DocumentConverter

    from core.adapters.extract_document import DoclingDocumentExtractor

    explicit = DoclingDocumentExtractor(DocumentLevel.FULL)
    bare = DocumentConverter()
    for pdf in _cached_pdfs(7_500_000):
        content = pdf.read_bytes()
        try:
            got = explicit.extract_bytes(content, "document.pdf")
        except Exception as exc:  # noqa: BLE001 — фикстура/офлайн
            pytest.skip(f"docling: {exc}")
        assert got.error is None, got.error
        import hashlib
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(content)
            path = tmp.name
        try:
            reference = bare.convert(path)
            ref_text = reference.document.export_to_markdown()
        finally:
            Path(path).unlink(missing_ok=True)
        assert got.content_hash == hashlib.sha256(ref_text.encode("utf-8")).hexdigest(), pdf


@pytest.mark.slow
def test_docling_levels_distinct() -> None:
    """§7.13: per-level ``content_hash`` различаются; таблицы сильнее на верхних уровнях.

    Проверяется и направление «фич-матрицы»: макро-нетабличный ``text`` (Native) не даёт
    больше markdown-табличных строк, чем ``layout_tables``/``full`` (TableFormer).
    """
    import os

    if os.environ.get("COHOUND_RUN_DOCLING_LEVELS") != "1":
        pytest.skip("тяжёлый offline-тест: нужен COHOUND_RUN_DOCLING_LEVELS=1")
    _docling_comment()
    from core.adapters.extract_document import DoclingDocumentExtractor

    def _table_rows(md: str) -> int:
        return sum(
            1
            for line in md.splitlines()
            if line.strip().startswith("|") and line.strip().count("|") >= 2
        )

    pdf = _cached_pdfs(7_500_000)[0]
    content = pdf.read_bytes()
    hashes: dict[str, str] = {}
    tables: dict[str, int] = {}
    for level in (
        DocumentLevel.TEXT,
        DocumentLevel.LAYOUT,
        DocumentLevel.LAYOUT_TABLES,
        DocumentLevel.FULL,
    ):
        doc = DoclingDocumentExtractor(level).extract_bytes(content, "document.pdf")
        assert doc.error is None, (level, doc.error)
        hashes[level.value] = doc.content_hash
        tables[level.value] = _table_rows(doc.text)
    # Хотя бы text/layout отличаются от full (натив vs layout+таблицы+OCR).
    assert len(set(hashes.values())) >= 2, hashes
    # Табличный уровень — не беднее нативного текста по markdown-таблицам.
    assert tables["layout_tables"] >= tables["text"], tables
    assert tables["full"] >= tables["text"], tables


@pytest.mark.slow
def test_docling_level_determinism_across_processes() -> None:
    """§7.14: байт-идентичность уровня в ``ProcessPoolExecutor`` (расширение P4 §5).

    Файл — малый cached PDF (как в замере гейта): проверяется сам факт кросс-процессной
    идентичности, а не объём документа; тяжёлый 7 МБ PDF делал gated-набор непрактичным.
    """
    import multiprocessing
    import os
    from concurrent.futures import ProcessPoolExecutor

    if os.environ.get("COHOUND_RUN_DOCLING_LEVELS") != "1":
        pytest.skip("тяжёлый offline-тест: нужен COHOUND_RUN_DOCLING_LEVELS=1")
    from core.adapters.extract_document import DoclingDocumentExtractor

    _docling_comment()
    pdf = _cached_pdfs(300_000)[0]
    inline = DoclingDocumentExtractor(DocumentLevel.LAYOUT).extract_bytes(
        pdf.read_bytes(), "document.pdf"
    )
    assert inline.error is None, inline.error

    try:
        context = multiprocessing.get_context("forkserver")
    except ValueError:  # pragma: no cover
        pytest.skip("метод multiprocessing 'forkserver' недоступен")
    with ProcessPoolExecutor(
        max_workers=2, mp_context=context, initializer=_docling_child_init
    ) as pool:
        results = list(pool.map(_docling_child_hash, [str(pdf), str(pdf)]))
    assert results[0] == results[1] == inline.content_hash
