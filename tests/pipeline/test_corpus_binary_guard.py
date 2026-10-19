"""P0: границы исполнения корпуса — ``payload_too_large`` и ``unsupported_binary``.

Проверяются: size-check раньше sniff (OVA → payload_too_large), бинарный артефакт
(``.bin`` с NUL) не ретейнится и не извлекается, PDF/OOXML/UTF-8 HTML проходят,
``text/plain`` с NUL отвергается, retained-бинарник на resume терминален, а оба класса
не блокируют coverage (осознанные границы, как ``exclude_documents``).
Данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlRunConfig,
    FailureKind,
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
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-binary-guard"
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
    version = "crawler-1"
    engine = "http"

    def __init__(
        self,
        pages: dict[str, bytes],
        *,
        content_types: dict[str, str] | None = None,
    ) -> None:
        self.pages = dict(pages)
        self.content_types = dict(content_types or {})
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            content_type=self.content_types.get(url),
            lang="en",
        )


class _Extractor:
    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
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
    content_types: dict[str, str] | None = None,
    store: SqliteStore | None = None,
    run_id: str | None = None,
    crawl_defaults: CrawlRunConfig | None = None,
    extractor: Any = None,
    root: str = ROOT,
):
    return build_corpus(
        _plan(),
        _Crawler(pages, content_types=content_types),
        _Extractor() if extractor is None else extractor,
        _Chunker(),
        store,
        urls={"p1": [root]},
        raw_root=tmp_path,
        run_id=run_id,
        crawl_defaults=crawl_defaults,
    )


def _items(store: SqliteStore) -> dict[str, Any]:
    return {
        i.url: i for i in store.iter_crawl_session_items(SESSION_ID)
    }


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def test_binary_bin_is_terminal_without_retention_or_extraction(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        extractor = _Extractor()
        result = _build(
            tmp_path,
            pages={ROOT: b"\x00\x01\x02binary"},
            store=store,
            run_id=RUN_ID,
            extractor=extractor,
        )
        item = _items(store)[ROOT]
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.UNSUPPORTED_BINARY.value
        assert item.raw_path is None
        assert extractor.calls == 0
        assert list((tmp_path / "p1").glob("*.bin")) == []
        assert _build_of(result).docs == []
        assert any("unsupported_binary" in e for e in result.errors)
    finally:
        store.close()


def test_pdf_magic_passes_without_suffix(tmp_path: Path) -> None:
    extractor = _Extractor()
    result = _build(
        tmp_path,
        pages={ROOT: b"%PDF-1.7\nbody"},
        extractor=extractor,
    )
    assert extractor.calls == 1
    assert _build_of(result).docs


def test_ooxml_docx_passes(tmp_path: Path) -> None:
    extractor = _Extractor()
    result = _build(
        tmp_path,
        pages={"https://docs.test/file.docx": b"PK\x03\x04zip-data"},
        extractor=extractor,
        root="https://docs.test/file.docx",
    )
    assert extractor.calls == 1
    assert _build_of(result).docs


def test_utf8_html_passes(tmp_path: Path) -> None:
    extractor = _Extractor()
    result = _build(
        tmp_path,
        pages={ROOT: "<html><body>Привет</body></html>".encode()},
        extractor=extractor,
    )
    assert extractor.calls == 1
    assert _build_of(result).docs


def test_text_plain_with_nul_is_binary(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        extractor = _Extractor()
        _build(
            tmp_path,
            pages={"https://docs.test/data.dat": b"ok\x00bad"},
            content_types={"https://docs.test/data.dat": "text/plain; charset=utf-8"},
            store=store,
            run_id=RUN_ID,
            extractor=extractor,
            root="https://docs.test/data.dat",
        )
        item = _items(store)["https://docs.test/data.dat"]
        assert item.error_kind == FailureKind.UNSUPPORTED_BINARY.value
        assert extractor.calls == 0
    finally:
        store.close()


def test_oversize_is_checked_before_binary_sniff(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            pages={"https://docs.test/DEV1.ova": b"\x00" * 128},
            store=store,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(max_response_bytes=64),
            root="https://docs.test/DEV1.ova",
        )
        item = _items(store)["https://docs.test/DEV1.ova"]
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.PAYLOAD_TOO_LARGE.value
        assert any("payload_too_large" in e for e in result.errors)
    finally:
        store.close()


def test_retained_binary_on_resume_is_terminal(tmp_path: Path) -> None:
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        _build(
            tmp_path,
            pages={ROOT: b"plain text"},
            store=store,
            run_id=RUN_ID,
        )
        item = _items(store)[ROOT]
        assert item.raw_path is not None
        binary = b"\x00binary-retained"
        Path(item.raw_path).write_bytes(binary)
        updated = item.model_copy(
            update={
                "content_hash": content_hash(binary),
                "content_type": None,
            }
        )
        store.upsert_crawl_session_item(updated)

        extractor = _Extractor()
        crawler = _Crawler({ROOT: b"plain text"})
        result = build_corpus(
            _plan(),
            crawler,
            extractor,
            _Chunker(),
            store,
            urls={"p1": [ROOT]},
            raw_root=tmp_path,
            run_id=RUN_ID,
        )
        resumed = _items(store)[ROOT]
        assert resumed.error_kind == FailureKind.UNSUPPORTED_BINARY.value
        assert resumed.raw_path is None
        assert extractor.calls == 0
        assert crawler.calls == []  # терминально: без повторного фетча
        assert any("unsupported_binary" in e for e in result.errors)
    finally:
        store.close()


def test_guarded_failure_kinds_do_not_block_coverage() -> None:
    from core.domain import BLOCKING_FAILURE_KINDS

    assert FailureKind.PAYLOAD_TOO_LARGE not in BLOCKING_FAILURE_KINDS
    assert FailureKind.UNSUPPORTED_BINARY not in BLOCKING_FAILURE_KINDS
